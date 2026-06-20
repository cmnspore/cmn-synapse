pub mod admin;
pub mod cmn;
pub(crate) mod common;
pub mod mycelium;
#[cfg(feature = "nostr")]
pub mod nostr;
pub mod pulse;
pub mod response;
#[cfg(feature = "ruvector")]
pub mod search;
pub mod spore;

use crate::config::AppConfig;
use crate::storage::{LineageNode, Storage, StorageBackend};
use axum::extract::DefaultBodyLimit;
use axum::extract::State;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::IntoResponse;
use axum::{routing::get, routing::post, Router};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[cfg(feature = "nostr")]
use crate::relay::RelayState;

pub struct AppState {
    pub config: Arc<AppConfig>,
    pub storage: StorageBackend,
    pub crawler: crate::crawler::Crawler,
    pub cmn_cache: Arc<crate::cmn_cache::CmnCache>,
    #[cfg(feature = "nostr")]
    pub nostr_relay: Option<Arc<RelayState>>,
    #[cfg(feature = "ruvector")]
    pub search: Option<Arc<crate::search::SearchBackend>>,
    #[cfg(feature = "ruvector")]
    pub graph: Option<Arc<crate::search::SporeGraph>>,
    /// Packed rate-limit state: upper 32 bits = epoch second, lower 32 bits = counter.
    /// Atomically updated via CAS to avoid TOCTOU races.
    pub rate_state: AtomicU64,
}

pub fn create_router(state: Arc<AppState>) -> Router<()> {
    let mut router = Router::new()
        // Health check
        .route("/health", get(health))
        // CMN endpoints (domain-level)
        .route("/synapse/cmn/{domain}", get(cmn::get_cmn))
        .route("/synapse/cmn/{domain}/tastes", get(cmn::get_cmn_tastes))
        // Mycelium endpoints (hash-based)
        .route(
            "/synapse/mycelium/{hash}",
            get(mycelium::get_mycelium_by_hash),
        )
        // Taste endpoint (hash-based)
        .route("/synapse/taste/{hash}", get(spore::get_taste))
        // Spore endpoints
        .route("/synapse/spore/{hash}", get(spore::get_spore))
        .route("/synapse/spore/{hash}/bonds", get(spore::get_spore_bonds))
        .route("/synapse/spore/{hash}/tastes", get(spore::get_spore_tastes))
        // Pulse endpoint
        .route("/synapse/pulse", post(pulse::handle_pulse))
        // Admin endpoint
        .route("/synapse/admin/status", get(admin::admin_status));

    // Nostr relay WebSocket endpoint
    #[cfg(feature = "nostr")]
    if state.nostr_relay.is_some() {
        router = router.route("/synapse/nostr", get(nostr::nostr_relay_handler));
    }

    // Search endpoint
    #[cfg(feature = "ruvector")]
    if state.search.is_some() {
        router = router.route("/synapse/search", get(search::handle_search));
    }

    let max_body = state.config.pulse.max_body_bytes;
    let max_concurrent = state.config.rate_limit.max_concurrent_requests;
    let rps = state.config.rate_limit.requests_per_second;

    let mut router = router
        .layer(DefaultBodyLimit::max(max_body))
        .layer(middleware::from_fn_with_state(state.clone(), access_log));

    // Apply per-second rate limiting if configured (0 = unlimited)
    if rps > 0 {
        router = router.layer(middleware::from_fn_with_state(state.clone(), rate_limit));
    }

    // Apply concurrency limiting if configured (0 = unlimited)
    if max_concurrent > 0 {
        router
            .layer(tower::limit::ConcurrencyLimitLayer::new(
                max_concurrent as usize,
            ))
            .with_state(state)
    } else {
        router.with_state(state)
    }
}

// Embed spore.core.json at compile time
const SPORE_CORE_JSON: &str = include_str!("../../spore.core.json");

async fn health(State(state): State<Arc<AppState>>) -> response::ApiResult {
    let config_bonds = &state.config.bonds;

    let bonds = if config_bonds.is_empty() {
        // Parse embedded spore.core.json and extract bonds
        serde_json::from_str::<serde_json::Value>(SPORE_CORE_JSON)
            .ok()
            .and_then(|v| v.get("bonds").cloned())
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default()
    } else {
        config_bonds.clone()
    };

    response::success(serde_json::json!({"bonds": bonds}))
}

async fn access_log(
    State(state): State<Arc<AppState>>,
    req: axum::extract::Request,
    next: Next,
) -> impl IntoResponse {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let start = crate::time::now_epoch_ms();

    let res = next.run(req).await;

    let status_code = res.status().as_u16();
    let duration_ms = crate::time::now_epoch_ms().saturating_sub(start);

    // Build fields
    let mut fields = serde_json::json!({
        "method": method,
        "path": path,
        "status_code": status_code,
    });

    // Build trace with duration + handler trace fields
    let mut trace = serde_json::json!({
        "duration_ms": duration_ms,
    });

    // Extract log metadata from response extension (set by ApiResponse::into_response)
    if let Some(log_meta) = res.extensions().get::<response::LogMeta>() {
        if let Some(result) = &log_meta.result {
            fields["result"] = result.clone();
        }
        if let Some(error) = &log_meta.error {
            fields["error"] = error.clone();
        }
        if let Some(handler_trace) = &log_meta.trace {
            if let (Some(target), Some(source)) = (trace.as_object_mut(), handler_trace.as_object())
            {
                for (k, v) in source {
                    target.insert(k.clone(), v.clone());
                }
            }
        }
    }

    let mut log = agent_first_data::build_json("request", fields, Some(trace));
    agent_first_data::redact_secrets_in_place(&mut log);

    let log_format = state.config.log_format.as_str();

    let formatted = match log_format {
        "plain" => agent_first_data::output_plain(&log),
        "yaml" => agent_first_data::output_yaml(&log),
        _ => agent_first_data::output_json(&log),
    };

    {
        use std::io::Write;
        let mut out = std::io::stdout().lock();
        let _ = out.write_all(formatted.as_bytes());
        let _ = out.write_all(b"\n");
    }

    res
}

/// Atomic CAS rate limiter.
///
/// Packs the current epoch-second window (upper 32 bits) and request count
/// (lower 32 bits) into a single `AtomicU64`. Uses `compare_exchange_weak`
/// to atomically check-and-increment, eliminating TOCTOU races between
/// separate window and counter atomics.
fn rate_limit_check(atom: &AtomicU64, rps: u64, now_s: u64) -> bool {
    let now_s_u32 = now_s as u32;
    loop {
        let current = atom.load(Ordering::Relaxed);
        let window = (current >> 32) as u32;
        let count = (current & 0xFFFF_FFFF) as u32;

        let (new_window, new_count) = if now_s_u32 != window {
            (now_s_u32, 1u32)
        } else if (count as u64) < rps {
            (window, count + 1)
        } else {
            return false;
        };

        let new_value = ((new_window as u64) << 32) | (new_count as u64);
        if atom
            .compare_exchange_weak(current, new_value, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return true;
        }
    }
}

/// Simple per-second rate limiter middleware.
async fn rate_limit(
    State(state): State<Arc<AppState>>,
    req: axum::extract::Request,
    next: Next,
) -> impl IntoResponse {
    let rps = state.config.rate_limit.requests_per_second;
    let now_s = crate::time::now_epoch_ms() / 1000;

    if !rate_limit_check(&state.rate_state, rps, now_s) {
        return response::error(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "Too many requests",
        )
        .into_response();
    }

    next.run(req).await.into_response()
}

/// Extract spore summary from stored JSON via substrate::Spore.
fn extract_summary(data: &[u8]) -> (String, String, String, Vec<String>) {
    if let Ok(spore) = serde_json::from_slice::<substrate::Spore>(data) {
        let core = &spore.capsule.core;
        (
            core.name.clone(),
            core.synopsis.clone(),
            core.license.clone(),
            core.intent.clone(),
        )
    } else {
        (String::new(), String::new(), String::new(), Vec::new())
    }
}

/// Format lineage nodes with spore summaries looked up from storage.
///
/// Uses batch fetch to avoid N+1 queries — one round-trip for all spore summaries.
pub async fn format_nodes(nodes: &[LineageNode], storage: &StorageBackend) -> Vec<Value> {
    use std::collections::HashMap;

    // Parse URIs and collect unique hashes for batch fetch
    let parsed: Vec<(String, String)> = nodes
        .iter()
        .map(|node| match substrate::parse_uri(&node.uri) {
            Ok(p) => (p.domain, p.hash.unwrap_or_default()),
            Err(_) => (String::new(), String::new()),
        })
        .collect();

    let unique_hashes: Vec<&str> = parsed
        .iter()
        .map(|(_, h)| h.as_str())
        .filter(|h| !h.is_empty())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    // Batch fetch all spore data in a single query
    let spore_map: HashMap<String, (String, String, String, Vec<String>)> =
        match storage.get_spores_batch(&unique_hashes).await {
            Ok(batch) => batch
                .into_iter()
                .filter_map(|(hash, data)| data.map(|d| (hash, extract_summary(&d))))
                .collect(),
            Err(_) => HashMap::new(),
        };

    let mut results = Vec::with_capacity(nodes.len());
    for (i, node) in nodes.iter().enumerate() {
        let (domain, hash) = &parsed[i];

        // Use the first ref's relation as the primary relation for this node.
        // Raw string — no enum parsing, forward-compatible with new relation types.
        let relation = node
            .refs
            .first()
            .map(|r| r.relation.as_str())
            .unwrap_or("unknown");

        let (name, synopsis, license, intent) = spore_map.get(hash).cloned().unwrap_or_default();

        results.push(serde_json::json!({
            "uri": node.uri,
            "domain": domain,
            "name": name,
            "synopsis": synopsis,
            "license": license,
            "intent": intent,
            "relation": relation,
        }));
    }
    results
}
