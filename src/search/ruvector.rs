use std::collections::HashMap;

use super::{EmbedClient, Search, SearchQuery, SearchResult};
use crate::config::SearchConfig;
use anyhow::Result;
use async_trait::async_trait;
use ruvector_core::types::{DbOptions, DistanceMetric, SearchQuery as RvSearchQuery, VectorEntry};
use ruvector_core::VectorDB;
use serde_json::Value;

pub struct RuvectorSearch {
    db: VectorDB,
    embedder: EmbedClient,
}

impl RuvectorSearch {
    pub fn new(config: &SearchConfig) -> Result<Self> {
        let db = VectorDB::new(DbOptions {
            dimensions: config.embedding.dimensions,
            distance_metric: DistanceMetric::Cosine,
            storage_path: config.index_path.clone(),
            hnsw_config: None,
            quantization: None,
        })?;
        let embedder = EmbedClient::new(&config.embedding);
        Ok(Self { db, embedder })
    }
}

/// Build a searchable text string from the spore JSON.
/// Uses structured prefixes so the embedding model understands field roles.
fn format_spore_text(spore: &Value) -> String {
    let core = &spore["capsule"]["core"];
    let mut parts = Vec::new();

    if let Some(s) = core["name"].as_str().filter(|s| !s.is_empty()) {
        parts.push(format!("Name: {}.", s));
    }
    if let Some(s) = core["synopsis"].as_str().filter(|s| !s.is_empty()) {
        parts.push(format!("Synopsis: {}.", s.trim_end_matches('.')));
    }
    if let Some(s) = core["description"].as_str().filter(|s| !s.is_empty()) {
        parts.push(format!("Description: {}.", s.trim_end_matches('.')));
    }
    if let Some(s) = core["domain"].as_str().filter(|s| !s.is_empty()) {
        parts.push(format!("Domain: {}.", s));
    }
    if let Some(s) = core["license"].as_str().filter(|s| !s.is_empty()) {
        parts.push(format!("License: {}.", s));
    }

    // String-array fields with labeled prefixes
    for (field, label) in &[
        ("keywords", "Keywords"),
        ("intent", "Intent"),
        ("mutations", "Mutations"),
    ] {
        if let Some(arr) = core[*field].as_array() {
            let items: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
            if !items.is_empty() {
                parts.push(format!("{}: {}.", label, items.join(". ")));
            }
        }
    }

    parts.join(" ")
}

/// Build metadata filter HashMap from SearchQuery constraints.
fn build_filter(query: &SearchQuery) -> Option<HashMap<String, Value>> {
    let mut filter = HashMap::new();

    if let Some(ref domain) = query.domain {
        filter.insert("domain".to_string(), Value::String(domain.clone()));
    }
    if let Some(ref license) = query.license {
        filter.insert("license".to_string(), Value::String(license.clone()));
    }

    if filter.is_empty() {
        None
    } else {
        Some(filter)
    }
}

#[async_trait]
impl Search for RuvectorSearch {
    async fn index_spore(&self, domain: &str, hash: &str, spore_json: &Value) -> Result<()> {
        // format_spore_text needs raw JSON for optional fields (description, keywords)
        // not present in substrate::SporeCore
        let text = format_spore_text(spore_json);
        let embeddings = self.embedder.embed(&[&text]).await?;
        let embedding = embeddings
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("No embedding returned"))?;

        // Deserialize via substrate for typed field access
        let spore: substrate::Spore = serde_json::from_value(spore_json.clone())
            .map_err(|e| anyhow::anyhow!("Failed to deserialize spore: {}", e))?;
        let core = &spore.capsule.core;

        // Use substrate's followed_strain_uris() for strain filter
        let strains: Vec<String> = spore
            .followed_strain_uris()
            .into_iter()
            .map(String::from)
            .collect();

        // Serialize bonds back to JSON for storage in vector metadata (used for bond filter queries)
        let bonds_json = serde_json::to_value(&core.bonds).unwrap_or_default();

        let mut metadata = HashMap::new();
        metadata.insert("domain".to_string(), Value::String(domain.to_string()));
        metadata.insert("name".to_string(), Value::String(core.name.clone()));
        metadata.insert("synopsis".to_string(), Value::String(core.synopsis.clone()));
        metadata.insert("license".to_string(), Value::String(core.license.clone()));
        metadata.insert("intent".to_string(), serde_json::json!(core.intent));
        metadata.insert("bonds".to_string(), bonds_json);
        metadata.insert("strains".to_string(), serde_json::json!(strains));
        metadata.insert("text".to_string(), Value::String(text));
        metadata.insert(
            "updated_at_epoch_ms".to_string(),
            serde_json::json!(core.updated_at_epoch_ms),
        );

        self.db.insert(VectorEntry {
            id: Some(hash.to_string()),
            vector: embedding,
            metadata: Some(metadata),
        })?;

        Ok(())
    }

    fn remove_spore(&self, hash: &str) {
        let _ = self.db.delete(hash);
    }

    async fn search(&self, query: &SearchQuery) -> Result<Vec<SearchResult>> {
        let query_embeddings = self.embedder.embed(&[&query.text]).await?;
        let query_vec = query_embeddings
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("No embedding returned for query"))?;

        // Over-fetch to cover offset + pagination probe (+1) + post-filtering
        let base = query.offset + query.limit + 1;
        let needs_post_filter = !query.bonds.is_empty()
            || query.strain.is_some()
            || query.updated_after.is_some()
            || query.updated_before.is_some();
        let fetch_limit = if needs_post_filter {
            (base * 10).min(500)
        } else {
            base
        };

        let results = self.db.search(RvSearchQuery {
            vector: query_vec,
            k: fetch_limit,
            filter: build_filter(query),
            ef_search: None,
        })?;

        let mapped: Vec<SearchResult> = results
            .into_iter()
            .filter_map(|r| {
                let meta = r.metadata.as_ref();

                // Apply strain filter: spore must follow the given strain
                if let Some(ref strain) = query.strain {
                    let has_strain = meta
                        .and_then(|m| m.get("strains"))
                        .and_then(|v| v.as_array())
                        .map(|arr| arr.iter().any(|s| s.as_str() == Some(strain.as_str())))
                        .unwrap_or(false);
                    if !has_strain {
                        return None;
                    }
                }

                // Apply bond filters: every filter must match at least one bond
                if !query.bonds.is_empty() {
                    let bonds: Vec<substrate::SporeBond> = meta
                        .and_then(|m| m.get("bonds"))
                        .and_then(|v| serde_json::from_value(v.clone()).ok())
                        .unwrap_or_default();
                    let filters: Vec<_> = query
                        .bonds
                        .iter()
                        .map(|rf| (rf.relation.clone(), rf.uri.clone()))
                        .collect();
                    if !substrate::bonds_match_all(&bonds, &filters) {
                        return None;
                    }
                }

                // Apply time range filter on spore updated_at_epoch_ms
                if query.updated_after.is_some() || query.updated_before.is_some() {
                    let updated_ms = meta
                        .and_then(|m| m.get("updated_at_epoch_ms"))
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    if let Some(after_ms) = query.updated_after {
                        if updated_ms < after_ms {
                            return None;
                        }
                    }
                    if let Some(before_ms) = query.updated_before {
                        if updated_ms > before_ms {
                            return None;
                        }
                    }
                }

                let get_str = |key: &str| -> String {
                    meta.and_then(|m| m.get(key))
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string()
                };
                let domain = get_str("domain");
                let hash = r.id.clone();
                let uri = format!("cmn://{}/{}", domain, hash);
                let intent: Vec<String> = meta
                    .and_then(|m| m.get("intent"))
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                Some(SearchResult {
                    uri,
                    domain,
                    hash,
                    name: get_str("name"),
                    synopsis: get_str("synopsis"),
                    license: get_str("license"),
                    intent,
                    score: r.score,
                })
            })
            .skip(query.offset)
            .take(query.limit + 1) // +1 to detect has_more
            .collect();

        Ok(mapped)
    }
}
