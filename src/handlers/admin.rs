use super::AppState;
use super::response::{self, ApiResult};
use axum::extract::State;
use std::sync::Arc;
use tracing::warn;

use crate::storage::Storage;

/// GET /synapse/admin/status
///
/// Returns operational status of this Synapse node: indexed domains,
/// spore/taste counts, retry queue, enabled features, and graph stats.
pub async fn admin_status(State(state): State<Arc<AppState>>) -> ApiResult {
    // Domains (from mycelium index)
    let domains = match state.storage.get_all_myceliums().await {
        Ok(rows) => {
            let total = rows.len();
            let items: Vec<serde_json::Value> = rows
                .iter()
                .map(|row| {
                    serde_json::json!({
                        "domain": row.domain,
                        "hash": row.hash,
                        "updated_at_epoch_ms": row.updated_at_epoch_ms,
                    })
                })
                .collect();
            serde_json::json!({ "total": total, "items": items })
        }
        Err(e) => {
            warn!(error = %e, "Admin: failed to get myceliums");
            serde_json::json!({
                "available": false,
                "error_code": "storage_query_failed",
            })
        }
    };

    // Spore count (single query instead of N+1)
    let spore_count = match state.storage.get_total_spore_count().await {
        Ok(count) => serde_json::json!(count),
        Err(_) => serde_json::json!(null),
    };

    // Retry queue
    let retry_pending = match state.storage.get_domains_for_retry(i32::MAX).await {
        Ok(domains) => serde_json::json!(domains.len()),
        Err(_) => serde_json::json!(null),
    };

    // Features
    let mut features = serde_json::Map::new();

    #[cfg(feature = "ruvector")]
    {
        features.insert(
            "search".to_string(),
            serde_json::json!(state.search.is_some()),
        );
        features.insert(
            "graph".to_string(),
            serde_json::json!(state.graph.is_some()),
        );
    }
    #[cfg(not(feature = "ruvector"))]
    {
        features.insert("search".to_string(), serde_json::json!(false));
        features.insert("graph".to_string(), serde_json::json!(false));
    }

    #[cfg(feature = "nostr")]
    {
        features.insert(
            "nostr".to_string(),
            serde_json::json!(state.nostr_relay.is_some()),
        );
    }
    #[cfg(not(feature = "nostr"))]
    {
        features.insert("nostr".to_string(), serde_json::json!(false));
    }

    // Graph stats (if available)
    let graph_stats = {
        #[cfg(feature = "ruvector")]
        {
            state
                .graph
                .as_ref()
                .and_then(|g| g.stats().ok())
                .map(|s| serde_json::json!(s))
                .unwrap_or(serde_json::json!(null))
        }
        #[cfg(not(feature = "ruvector"))]
        {
            serde_json::json!(null)
        }
    };

    let mut result = serde_json::json!({
        "domains": domains,
        "spores": { "total": spore_count },
        "retry_queue": { "pending": retry_pending },
        "features": features,
    });

    if !graph_stats.is_null() {
        result["graph"] = graph_stats;
    }

    response::success(result)
}
