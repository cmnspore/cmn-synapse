use super::AppState;
use super::common::mirror_replicates;
use super::response::{self, ApiResult};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use std::sync::Arc;
use substrate::Mycelium;
use tracing::warn;

use crate::storage::Storage;

/// GET /synapse/mycelium/{hash}
///
/// Returns a mycelium shard looked up by its content hash.
pub async fn get_mycelium_by_hash(
    Path(hash): Path<String>,
    State(state): State<Arc<AppState>>,
) -> ApiResult {
    if substrate::parse_hash(&hash).is_err() {
        return response::error(
            StatusCode::BAD_REQUEST,
            "hash_invalid",
            "Invalid hash format",
        );
    }

    // Find domains hosting this hash via reverse index
    let matching = match state.storage.get_mycelium_by_hash(&hash).await {
        Ok(rows) => rows,
        Err(e) => {
            warn!(error = %e, hash = %hash, "Failed to look up mycelium by hash");
            return response::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                &e.to_string(),
            );
        }
    };
    let matching: Vec<_> = matching.iter().collect();

    if matching.is_empty() {
        return response::error(
            StatusCode::NOT_FOUND,
            "not_found",
            &format!("Mycelium not found for hash {}", hash),
        );
    }

    let hosts: Vec<String> = matching.iter().map(|row| row.domain.clone()).collect();

    // Try each hosting domain until one succeeds
    for row in &matching {
        match state.storage.get_mycelium(&row.domain).await {
            Ok(Some(data)) => match serde_json::from_slice::<Mycelium>(&data) {
                Ok(mycelium) => {
                    let origin_domain = mycelium.capsule.core.domain.clone();
                    match serde_json::to_value(mycelium) {
                        Ok(v) => {
                            let replicates =
                                mirror_replicates(&hosts, Some(origin_domain.as_str()));
                            return response::success(serde_json::json!({
                                "query": { "hash": hash },
                                "mycelium": v,
                                "replicates": replicates
                            }));
                        }
                        Err(e) => {
                            warn!(error = %e, hash = %hash, domain = %row.domain, "Failed to serialize mycelium, trying next host");
                        }
                    }
                }
                Err(e) => {
                    warn!(error = %e, hash = %hash, domain = %row.domain, "Failed to deserialize mycelium, trying next host");
                }
            },
            Ok(None) => {
                warn!(hash = %hash, domain = %row.domain, "Mycelium not found for domain, trying next host");
            }
            Err(e) => {
                warn!(error = %e, hash = %hash, domain = %row.domain, "Failed to get mycelium, trying next host");
            }
        }
    }

    response::error(
        StatusCode::NOT_FOUND,
        "not_found",
        "Mycelium data unavailable from all hosting domains",
    )
}
