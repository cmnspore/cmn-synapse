use substrate::{Taste, VerdictSummary};

/// Default page size for taste and search result listings.
pub const DEFAULT_PAGE_LIMIT: usize = 20;
/// Maximum page size for search results.
pub const MAX_SEARCH_LIMIT: usize = 100;
/// Default page size for bond traversal results.
pub const DEFAULT_BONDS_LIMIT: usize = 100;
/// Maximum number of bond traversal results per request.
pub const MAX_BONDS_LIMIT: usize = 1000;

pub fn mirror_replicates(hosts: &[String], origin_domain: Option<&str>) -> Vec<String> {
    let Some(origin_domain) = origin_domain else {
        return Vec::new();
    };
    let mut replicates: Vec<String> = hosts
        .iter()
        .filter(|host| host.as_str() != origin_domain)
        .cloned()
        .collect();
    replicates.sort();
    replicates.dedup();
    replicates
}

pub fn taste_limit(params: &std::collections::HashMap<String, String>) -> usize {
    params
        .get("limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PAGE_LIMIT)
}

pub fn build_taste_response(
    taste_data: Vec<Vec<u8>>,
    limit: usize,
    replicates: &[String],
) -> serde_json::Value {
    let all_tastes: Vec<Taste> = taste_data
        .iter()
        .filter_map(|data| serde_json::from_slice::<Taste>(data).ok())
        .collect();

    let summary = VerdictSummary::from_tastes(&all_tastes);
    let total = summary.total;
    let verdicts = summary.to_json_map();

    let mut flat_tastes: Vec<serde_json::Value> = all_tastes
        .iter()
        .map(|taste| {
            serde_json::json!({
                "uri": taste.capsule.uri,
                "domain": taste.capsule.core.domain,
                "target_uri": taste.capsule.core.target_uri,
                "verdict": taste.capsule.core.verdict.as_str(),
                "notes": taste.capsule.core.notes,
                "tasted_at_epoch_ms": taste.capsule.core.tasted_at_epoch_ms,
                "replicates": replicates,
            })
        })
        .collect();

    flat_tastes.sort_by(|a, b| {
        let ts_a = a
            .get("tasted_at_epoch_ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let ts_b = b
            .get("tasted_at_epoch_ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        ts_b.cmp(&ts_a)
    });
    flat_tastes.truncate(limit);

    serde_json::json!({
        "verdicts": verdicts,
        "tastes": flat_tastes,
        "total": total
    })
}
