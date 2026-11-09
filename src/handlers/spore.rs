use super::common::{build_taste_response, mirror_replicates, taste_limit};
use super::response::{self, ApiResult};
use super::{AppState, format_nodes};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use std::sync::Arc;
use substrate::Spore;
use tracing::warn;

use crate::storage::Storage;

// GET /synapse/spore/:hash - Get spore by hash
pub async fn get_spore(Path(hash): Path<String>, State(state): State<Arc<AppState>>) -> ApiResult {
    if substrate::parse_hash(&hash).is_err() {
        return response::error(
            StatusCode::BAD_REQUEST,
            "hash_invalid",
            "Invalid hash format",
        );
    }

    match state.storage.get_spore(&hash).await {
        Ok(Some(data)) => match serde_json::from_slice::<Spore>(&data) {
            Ok(spore) => {
                let origin_domain = spore.capsule.core.domain.clone();
                match serde_json::to_value(spore) {
                    Ok(v) => {
                        let hosts = state
                            .storage
                            .get_spore_hosts(&hash)
                            .await
                            .unwrap_or_default();
                        let replicates = mirror_replicates(&hosts, Some(origin_domain.as_str()));
                        response::success(serde_json::json!({
                            "query": { "hash": hash },
                            "spore": v,
                            "replicates": replicates
                        }))
                    }
                    Err(e) => {
                        warn!(error = %e, hash = %hash, "Failed to serialize spore");
                        response::error(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "internal_error",
                            &e.to_string(),
                        )
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, hash = %hash, "Failed to deserialize spore");
                response::error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    &e.to_string(),
                )
            }
        },
        Ok(None) => response::error(StatusCode::NOT_FOUND, "not_found", "Spore not found"),
        Err(e) => {
            warn!(error = %e, hash = %hash, "Failed to get spore");
            response::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                &e.to_string(),
            )
        }
    }
}

/// Shared implementation for spore-hash-based taste lookups.
/// Used by both `GET /synapse/spore/:hash/tastes` and `GET /synapse/taste/:hash`.
async fn get_tastes_for_spore_hash(hash: &str, limit: usize, state: &AppState) -> ApiResult {
    match state.storage.get_spore_tastes(hash).await {
        Ok(taste_data) => {
            let hosts = state
                .storage
                .get_spore_hosts(hash)
                .await
                .unwrap_or_default();
            let origin_domain = match state.storage.get_spore(hash).await {
                Ok(Some(data)) => match serde_json::from_slice::<Spore>(&data) {
                    Ok(spore) => Some(spore.capsule.core.domain),
                    Err(_) => None,
                },
                _ => None,
            };
            let replicates = mirror_replicates(&hosts, origin_domain.as_deref());
            let mut resp = build_taste_response(taste_data, limit, &replicates);
            resp["query"] = serde_json::json!({ "hash": hash, "limit": limit });
            response::success(resp)
        }
        Err(e) => {
            warn!(error = %e, hash = %hash, "Failed to get taste reports");
            response::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                &e.to_string(),
            )
        }
    }
}

// GET /synapse/spore/:hash/tastes - Get aggregated taste reports for a spore
// Query params: ?limit=20 (default: 20)
pub async fn get_spore_tastes(
    Path(hash): Path<String>,
    State(state): State<Arc<AppState>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> ApiResult {
    let limit = taste_limit(&params);
    get_tastes_for_spore_hash(&hash, limit, &state).await
}

/// GET /synapse/taste/{hash} - Get a single taste report by its content hash.
pub async fn get_taste(Path(hash): Path<String>, State(state): State<Arc<AppState>>) -> ApiResult {
    match state.storage.get_taste_by_hash(&hash).await {
        Ok(Some(data)) => match serde_json::from_slice::<serde_json::Value>(&data) {
            Ok(taste) => {
                // Taste reports are indexed by content hash only — no per-host tracking yet,
                // so replicates is always empty. Future: track which domains submitted this taste.
                let replicates: Vec<String> = Vec::new();
                response::success(serde_json::json!({
                    "query": { "hash": hash },
                    "taste": taste,
                    "replicates": replicates
                }))
            }
            Err(e) => {
                warn!(error = %e, hash = %hash, "Failed to deserialize taste");
                response::error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    &e.to_string(),
                )
            }
        },
        Ok(None) => response::error(StatusCode::NOT_FOUND, "not_found", "Taste not found"),
        Err(e) => {
            warn!(error = %e, hash = %hash, "Failed to look up taste by hash");
            response::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                &e.to_string(),
            )
        }
    }
}

// GET /synapse/spore/:hash/bonds - Get bonds of a spore
// Query params:
//   direction (required): "inbound" or "outbound"
//   relation (optional): filter by relation type
//   max_depth (optional): max traversal depth, default 1
//   limit (optional): max items to return, default 100
pub async fn get_spore_bonds(
    Path(hash): Path<String>,
    State(state): State<Arc<AppState>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> ApiResult {
    let direction = match params.get("direction").map(|s| s.as_str()) {
        Some("inbound") => "inbound",
        Some("outbound") => "outbound",
        Some(_) => {
            return response::error(
                StatusCode::BAD_REQUEST,
                "field_missing",
                "direction must be 'inbound' or 'outbound'",
            );
        }
        None => {
            return response::error(
                StatusCode::BAD_REQUEST,
                "field_missing",
                "direction parameter is required",
            );
        }
    };

    let relation_filter = params.get("relation").map(|s| s.as_str());
    let server_max = state.config.lineage.max_depth;
    let max_depth = params
        .get("max_depth")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1)
        .min(server_max);
    let limit = params
        .get("limit")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(super::common::DEFAULT_BONDS_LIMIT)
        .min(super::common::MAX_BONDS_LIMIT);

    // Choose storage call based on direction
    let storage_result = if direction == "inbound" {
        // Inbound = who points to me = descendants
        // Try graph first
        #[cfg(feature = "ruvector")]
        if let Some(ref graph) = state.graph {
            let (nodes, reached) = graph.get_descendants(&hash, max_depth).unwrap_or_default();
            if !nodes.is_empty() {
                Ok(crate::storage::LineageResult {
                    nodes,
                    max_depth_reached: reached,
                })
            } else {
                state.storage.get_spore_lineage(&hash, max_depth).await
            }
        } else {
            state.storage.get_spore_lineage(&hash, max_depth).await
        }
        #[cfg(not(feature = "ruvector"))]
        state.storage.get_spore_lineage(&hash, max_depth).await
    } else {
        // Outbound = who do I point to = ancestors
        #[cfg(feature = "ruvector")]
        if let Some(ref graph) = state.graph {
            let (nodes, reached) = graph.get_ancestors(&hash, max_depth).unwrap_or_default();
            if !nodes.is_empty() {
                Ok(crate::storage::LineageResult {
                    nodes,
                    max_depth_reached: reached,
                })
            } else {
                state.storage.get_spore_ancestors(&hash, max_depth).await
            }
        } else {
            state.storage.get_spore_ancestors(&hash, max_depth).await
        }
        #[cfg(not(feature = "ruvector"))]
        state.storage.get_spore_ancestors(&hash, max_depth).await
    };

    match storage_result {
        Ok(lineage) => {
            // Apply relation filter before format_nodes to avoid unnecessary storage lookups
            let nodes: Vec<_> = if let Some(rel) = relation_filter {
                lineage
                    .nodes
                    .into_iter()
                    .filter(|n| n.refs.iter().any(|r| r.relation.as_str() == rel))
                    .collect()
            } else {
                lineage.nodes
            };

            let total = nodes.len();
            let limited: Vec<_> = nodes.into_iter().take(limit).collect();
            let bonds = format_nodes(&limited, &state.storage).await;

            let mut query = serde_json::json!({
                "hash": hash,
                "direction": direction,
                "max_depth": max_depth,
            });
            if let Some(rel) = relation_filter {
                query["relation"] = serde_json::json!(rel);
            }

            response::success_trace(
                serde_json::json!({
                    "query": query,
                    "bonds": bonds,
                    "total": total,
                }),
                serde_json::json!({
                    "max_depth_reached": lineage.max_depth_reached
                }),
            )
        }
        Err(e) => {
            warn!(error = %e, hash = %hash, "Failed to get spore bonds");
            response::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                &e.to_string(),
            )
        }
    }
}
