use super::AppState;
use super::response::{self, ApiResult};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use serde::Deserialize;
use std::sync::Arc;
use tracing::warn;

use crate::search::{BondFilter, Search, SearchQuery};
use crate::storage::Storage;

#[derive(Deserialize)]
pub struct SearchParams {
    /// Search query text
    pub q: String,
    /// Filter by domain
    pub domain: Option<String>,
    /// Filter by license
    pub license: Option<String>,
    /// Filter by strain ID (e.g. `strain-chat-llm`).
    /// Only return spores that follow the given strain.
    pub strain: Option<String>,
    /// Filter by bond. Format: `{relation}:{uri}`.
    /// Multiple filters use comma separation (AND logic).
    pub bonds: Option<String>,
    /// Only return spores updated after this timestamp (epoch ms, inclusive)
    pub updated_after: Option<u64>,
    /// Only return spores updated before this timestamp (epoch ms, inclusive)
    pub updated_before: Option<u64>,
    /// Max results (default 20)
    pub limit: Option<usize>,
    /// Opaque cursor for pagination
    pub cursor: Option<String>,
}

/// Legacy offset cursor support (`cursor=123`).
fn decode_offset_cursor(cursor: &str) -> Option<usize> {
    cursor.parse().ok()
}

/// Encode an offset as an opaque base64 cursor token.
fn encode_opaque_cursor(offset: usize) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(offset.to_string().as_bytes())
}

/// Decode an opaque base64 cursor token back to an offset.
fn decode_opaque_cursor(cursor: &str) -> Option<usize> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .ok()?;
    let s = std::str::from_utf8(&bytes).ok()?;
    s.parse().ok()
}

pub async fn handle_search(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchParams>,
) -> ApiResult {
    let search = match state.search {
        Some(ref s) => s,
        None => {
            return response::error(
                StatusCode::SERVICE_UNAVAILABLE,
                "search_unavailable",
                "Search engine not configured",
            );
        }
    };

    // Parse bond filters (comma-separated: "relation1:uri1,relation2:uri2")
    let bond_filters: Vec<BondFilter> = params
        .bonds
        .as_deref()
        .map(|s| s.split(',').filter_map(BondFilter::parse).collect())
        .unwrap_or_default();

    let limit = params
        .limit
        .unwrap_or(super::common::DEFAULT_PAGE_LIMIT)
        .min(super::common::MAX_SEARCH_LIMIT);

    let offset = match params.cursor.as_deref() {
        None => 0,
        Some(cursor) => {
            // Try opaque base64 cursor first, then legacy numeric offset
            if let Some(n) = decode_opaque_cursor(cursor) {
                n
            } else if let Some(n) = decode_offset_cursor(cursor) {
                n
            } else {
                return response::error(
                    StatusCode::BAD_REQUEST,
                    "cursor_invalid",
                    "Invalid cursor token",
                );
            }
        }
    };

    let query = SearchQuery {
        text: params.q.clone(),
        domain: params.domain.clone(),
        license: params.license.clone(),
        strain: params.strain.clone(),
        bonds: bond_filters,
        updated_after: params.updated_after,
        updated_before: params.updated_before,
        limit,
        offset,
    };
    if query.checked_fetch_count().is_none() {
        return response::error(
            StatusCode::BAD_REQUEST,
            "cursor_invalid",
            "Cursor offset is too large for the requested page",
        );
    }

    match search.search(&query).await {
        Ok(results) => {
            // Backend returns limit+1 results; if we got more than limit, there are more pages
            let has_more = results.len() > limit;
            let page: Vec<_> = results.into_iter().take(limit).collect();

            // Batch-fetch verdicts for all spores in a single query
            let hash_refs: Vec<&str> = page.iter().map(|r| r.hash.as_str()).collect();
            let verdicts_map = build_verdicts_batch(&hash_refs, &state.storage).await;

            let mut spores = Vec::with_capacity(page.len());
            for result in &page {
                let verdicts = verdicts_map
                    .get(&result.hash)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                let mut entry = serde_json::json!({
                    "uri": result.uri,
                    "domain": result.domain,
                    "name": result.name,
                    "synopsis": result.synopsis,
                    "license": result.license,
                    "intent": result.intent,
                    "relevance": result.score,
                    "verdicts": verdicts,
                });
                // Remove null verdicts (no taste data)
                if verdicts.is_null()
                    && let Some(obj) = entry.as_object_mut()
                {
                    obj.remove("verdicts");
                }
                spores.push(entry);
            }

            let total = offset + page.len() + if has_more { 1 } else { 0 };

            let query_json = serde_json::json!({
                "text": params.q,
                "domain": params.domain,
                "license": params.license,
                "strain": params.strain,
                "bonds": params.bonds,
                "updated_after": params.updated_after,
                "updated_before": params.updated_before,
                "limit": limit,
            });

            let mut result = serde_json::json!({
                "query": query_json,
                "spores": spores,
                "total": total,
            });
            if has_more {
                let next_offset = offset + page.len();
                result["cursor"] = serde_json::json!(encode_opaque_cursor(next_offset));
            }

            response::success(result)
        }
        Err(e) => {
            warn!(error = %e, "Search query failed");
            response::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "search_failed",
                &e.to_string(),
            )
        }
    }
}

/// Maximum taste reports to load per spore for verdict aggregation.
/// Prevents unbounded memory usage when a spore has many taste reports.
const MAX_TASTES_PER_VERDICT: usize = 1000;

/// Build verdict counts for multiple spore hashes in a single batch query.
/// Returns a map from hash to verdict JSON (omits hashes with no taste data).
async fn build_verdicts_batch(
    hashes: &[&str],
    storage: &crate::storage::StorageBackend,
) -> std::collections::HashMap<String, serde_json::Value> {
    let batch = match storage.get_spore_tastes_batch(hashes).await {
        Ok(b) => b,
        Err(_) => return std::collections::HashMap::new(),
    };

    batch
        .into_iter()
        .filter_map(|(hash, taste_data)| {
            if taste_data.is_empty() {
                return None;
            }
            let tastes: Vec<substrate::Taste> = taste_data
                .iter()
                .take(MAX_TASTES_PER_VERDICT)
                .filter_map(|data| serde_json::from_slice(data).ok())
                .collect();
            let summary = substrate::VerdictSummary::from_tastes(&tastes);
            Some((hash, serde_json::Value::Object(summary.to_json_map())))
        })
        .collect()
}
