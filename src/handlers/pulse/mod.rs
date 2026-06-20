use super::response::{self, ApiResult};
use super::AppState;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, info_span, warn, Instrument};

/// Per-domain cooldown for cmn.json pulses to prevent abuse.
/// Tracks the last accepted pulse timestamp (epoch ms) per domain.
static CMN_PULSE_COOLDOWN: std::sync::LazyLock<RwLock<HashMap<String, u64>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));

/// Minimum interval between cmn.json pulses for the same domain (60 seconds).
const CMN_PULSE_COOLDOWN_MS: u64 = 60_000;

use crate::ingest::{self, StoreOptions};

/// Handle a pulse: the body is a complete signed CMN document.
///
/// Dispatches by `$schema`:
/// - cmn.json → notification: invalidate cache, fetch live version from domain
/// - spore/mycelium/taste → unified ingest pipeline (validate_and_verify + store)
pub async fn handle_pulse(State(state): State<Arc<AppState>>, body: Bytes) -> ApiResult {
    let payload: Value =
        match substrate::client::json_from_slice_reject_duplicates(&body, "synapse pulse") {
            Ok(payload) => payload,
            Err(e) => {
                warn!(error = %e, "Pulse JSON parse failed");
                return response::error(StatusCode::BAD_REQUEST, "invalid_capsule", &e.to_string());
            }
        };

    // Detect type early to handle cmn.json separately
    let schema_type = match substrate::validate_schema(&payload) {
        Ok(t) => t,
        Err(e) => {
            warn!(error = %e, "Pulse schema validation failed");
            return response::error(StatusCode::BAD_REQUEST, "invalid_capsule", &e.to_string());
        }
    };

    if schema_type == substrate::SchemaType::Cmn {
        return handle_cmn_pulse(&state, &payload).await;
    }

    // Standard ingest pipeline for spore/mycelium/taste
    let max_clock_skew_ms = state.config.pulse.max_clock_skew_s * 1000;
    let max_taste_clock_skew_ms = state.config.pulse.max_taste_clock_skew_s * 1000;

    let pulse = match ingest::validate_and_verify(
        &payload,
        &state.cmn_cache,
        &state.storage,
        max_clock_skew_ms,
        max_taste_clock_skew_ms,
        state.config.debug,
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "Pulse rejected");
            return response::error(
                StatusCode::from_u16(e.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                e.code(),
                &e.to_string(),
            );
        }
    };

    let domain = pulse.domain.clone();
    let content_type = pulse.content_type;

    let result = ingest::store(
        &state,
        &pulse,
        &StoreOptions {
            forward_nostr: true,
        },
    )
    .instrument(info_span!("pulse", domain = %domain, content_type))
    .await;

    match result {
        Ok(action) => response::success_trace(
            serde_json::json!({ "action": action }),
            serde_json::json!({
                "pulse": {
                    "type": content_type,
                    "domain": domain,
                    "uri": pulse.uri,
                }
            }),
        ),
        Err(e) => {
            warn!(error = %e, "Pulse storage failed");
            response::error(
                StatusCode::from_u16(e.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                e.code(),
                &e.to_string(),
            )
        }
    }
}

/// Handle a cmn.json pulse: notification that a domain's cmn.json has changed.
///
/// The pulse payload is NOT trusted as data — it's a notification.
/// We extract the domain, invalidate the cache, and fetch the live version
/// from the domain's server to verify and cache.
async fn handle_cmn_pulse(state: &AppState, payload: &Value) -> ApiResult {
    // Extract domain from capsules[0].uri
    let domain = match payload
        .get("capsules")
        .and_then(|c| c.as_array())
        .and_then(|arr| arr.first())
        .and_then(|entry| entry.get("uri"))
        .and_then(|u| u.as_str())
    {
        Some(uri) => match substrate::parse_uri(uri) {
            Ok(parsed) => parsed.domain,
            Err(e) => {
                return response::error(
                    StatusCode::BAD_REQUEST,
                    "uri_invalid",
                    &format!("Invalid capsules[0].uri: {}", e),
                );
            }
        },
        None => {
            return response::error(
                StatusCode::BAD_REQUEST,
                "field_missing",
                "Missing capsules[0].uri",
            );
        }
    };

    // Per-domain cooldown: reject if we processed a cmn.json pulse for this domain recently.
    // Use write lock for both check and update to avoid TOCTOU race.
    let now_ms = crate::time::now_epoch_ms();
    {
        let mut guard = CMN_PULSE_COOLDOWN.write().await;
        if let Some(&last_ms) = guard.get(&domain) {
            if now_ms.saturating_sub(last_ms) < CMN_PULSE_COOLDOWN_MS {
                return response::error(
                    StatusCode::TOO_MANY_REQUESTS,
                    "rate_limited",
                    &format!(
                        "cmn.json pulse for {} was processed recently, retry after cooldown",
                        domain
                    ),
                );
            }
        }
        guard.insert(domain.clone(), now_ms);
    }

    info!(domain = %domain, "cmn.json pulse received, refreshing");

    // Invalidate cache and fetch live version from the domain
    state.cmn_cache.invalidate(&domain).await;

    match state.cmn_cache.get_entry(&domain).await {
        Ok(_entry) => {
            info!(domain = %domain, "cmn.json refreshed from live server");

            // Trigger a crawl in background to pick up any mycelium changes
            let crawler = state.crawler.clone();
            let crawl_domain = domain.clone();
            tokio::spawn(async move {
                if let Err(e) = crawler.crawl_domain(&crawl_domain).await {
                    warn!(error = %e, domain = %crawl_domain, "Post-cmn.json crawl failed");
                }
            });

            response::success_trace(
                serde_json::json!({ "action": "indexed" }),
                serde_json::json!({
                    "pulse": {
                        "type": "cmn",
                        "domain": domain,
                    }
                }),
            )
        }
        Err(e) => {
            warn!(error = %e, domain = %domain, "Failed to fetch live cmn.json");
            response::error(
                StatusCode::BAD_GATEWAY,
                "cmn_fetch_failed",
                &format!("Failed to verify cmn.json from {}: {}", domain, e),
            )
        }
    }
}
