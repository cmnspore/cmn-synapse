use super::common::{build_taste_response, taste_limit};
use super::response::{self, ApiResult};
use super::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use std::sync::Arc;
use tracing::warn;

use crate::storage::Storage;

/// GET /synapse/cmn/{domain}
///
/// Returns the cmn.json document for a domain, fetched via the cmn_cache.
pub async fn get_cmn(Path(domain): Path<String>, State(state): State<Arc<AppState>>) -> ApiResult {
    if let Err(e) = substrate::validate_domain(&domain) {
        return response::error(StatusCode::BAD_REQUEST, "domain_invalid", &e.to_string());
    }

    match state.cmn_cache.get_entry(&domain).await {
        Ok(entry) => match serde_json::to_value(&entry) {
            Ok(v) => response::success(serde_json::json!({
                "query": { "domain": domain },
                "cmn": v
            })),
            Err(e) => {
                warn!(error = %e, domain = %domain, "Failed to serialize cmn entry");
                response::error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    &e.to_string(),
                )
            }
        },
        Err(e) => {
            warn!(error = %e, domain = %domain, "Failed to get cmn entry");
            response::error(
                StatusCode::NOT_FOUND,
                "not_found",
                &format!("CMN entry not found for domain {}", domain),
            )
        }
    }
}

/// GET /synapse/cmn/{domain}/tastes
///
/// Return aggregated taste reputation for a domain, computed from the taste
/// verdicts of all spores published by the domain.
pub async fn get_cmn_tastes(
    Path(domain): Path<String>,
    State(state): State<Arc<AppState>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> ApiResult {
    if let Err(e) = substrate::validate_domain(&domain) {
        return response::error(StatusCode::BAD_REQUEST, "domain_invalid", &e.to_string());
    }

    let limit = taste_limit(&params);

    // Get all spore hashes for this domain
    let spore_hashes = match state.storage.get_domain_spores(&domain).await {
        Ok(hashes) => hashes,
        Err(e) => {
            warn!(error = %e, domain = %domain, "Failed to get domain spores for taste aggregation");
            return response::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                &e.to_string(),
            );
        }
    };

    // Collect taste reports across spores published by this domain (capped to prevent OOM)
    const MAX_DOMAIN_TASTES: usize = 10_000;
    let hash_refs: Vec<&str> = spore_hashes.iter().map(|s| s.as_str()).collect();
    let batch = match state.storage.get_spore_tastes_batch(&hash_refs).await {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, domain = %domain, "Failed to batch-get tastes for domain");
            Vec::new()
        }
    };
    let mut all_taste_data: Vec<Vec<u8>> = Vec::new();
    for (_hash, mut tastes) in batch {
        if all_taste_data.len() >= MAX_DOMAIN_TASTES {
            break;
        }
        let remaining = MAX_DOMAIN_TASTES.saturating_sub(all_taste_data.len());
        tastes.truncate(remaining);
        all_taste_data.append(&mut tastes);
    }

    let no_replicates: Vec<String> = Vec::new();
    let mut resp = build_taste_response(all_taste_data, limit, &no_replicates);
    resp["query"] = serde_json::json!({ "domain": domain, "limit": limit });

    response::success(resp)
}
