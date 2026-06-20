#[cfg(feature = "ruvector")]
mod graph;
#[cfg(feature = "ruvector")]
mod ruvector;

#[cfg(feature = "ruvector")]
pub use self::graph::SporeGraph;
#[cfg(feature = "ruvector")]
pub use self::ruvector::RuvectorSearch;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::config::EmbeddingConfig;

/// Search result returned by any search engine.
#[derive(Debug, Serialize)]
pub struct SearchResult {
    pub uri: String,
    pub domain: String,
    pub hash: String,
    pub name: String,
    pub synopsis: String,
    pub license: String,
    pub intent: Vec<String>,
    pub score: f32,
}

/// A parsed bond filter from the `bond` query parameter.
/// Format: `{relation}:{uri}` (e.g., `spawned_from:cmn://alice.dev/b3.xxx`).
#[derive(Debug, Clone)]
pub struct BondFilter {
    pub relation: substrate::BondRelation,
    pub uri: String,
}

impl BondFilter {
    /// Parse a `bond` parameter value like `"spawned_from:cmn://alice.dev/b3.xxx"`.
    /// Returns `None` if the format is invalid.
    pub fn parse(s: &str) -> Option<Self> {
        let (relation, uri) = s.split_once(':')?;
        if relation.is_empty() || uri.is_empty() {
            return None;
        }
        Some(Self {
            relation: relation.parse().ok()?,
            uri: uri.to_string(),
        })
    }
}

/// Search query parameters.
#[derive(Debug, Deserialize)]
pub struct SearchQuery {
    pub text: String,
    pub domain: Option<String>,
    pub license: Option<String>,
    /// Filter by strain ID (e.g. `strain-chat-llm`). Only returns spores that follow this strain.
    #[serde(default)]
    pub strain: Option<String>,
    #[serde(skip)]
    pub bonds: Vec<BondFilter>,
    /// Only return spores updated after this timestamp (epoch ms, inclusive).
    pub updated_after: Option<u64>,
    /// Only return spores updated before this timestamp (epoch ms, inclusive).
    pub updated_before: Option<u64>,
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
}

/// Trait for search engine implementations.
#[async_trait]
pub trait Search: Send + Sync {
    async fn index_spore(
        &self,
        domain: &str,
        hash: &str,
        spore_json: &serde_json::Value,
    ) -> Result<()>;
    /// Remove a spore from the search index (e.g., superseded by a newer version).
    fn remove_spore(&self, hash: &str);
    async fn search(&self, query: &SearchQuery) -> Result<Vec<SearchResult>>;
}

/// SearchBackend enum (mirrors StorageBackend pattern).
pub enum SearchBackend {
    #[cfg(feature = "ruvector")]
    Ruvector(RuvectorSearch),
}

#[async_trait]
impl Search for SearchBackend {
    async fn index_spore(
        &self,
        domain: &str,
        hash: &str,
        spore_json: &serde_json::Value,
    ) -> Result<()> {
        match self {
            #[cfg(feature = "ruvector")]
            SearchBackend::Ruvector(engine) => engine.index_spore(domain, hash, spore_json).await,
        }
    }

    fn remove_spore(&self, hash: &str) {
        match self {
            #[cfg(feature = "ruvector")]
            SearchBackend::Ruvector(engine) => engine.remove_spore(hash),
        }
    }

    async fn search(&self, query: &SearchQuery) -> Result<Vec<SearchResult>> {
        match self {
            #[cfg(feature = "ruvector")]
            SearchBackend::Ruvector(engine) => engine.search(query).await,
        }
    }
}

/// Embedding client — calls external API (Ollama or OpenAI) via reqwest.
pub struct EmbedClient {
    http: reqwest::Client,
    config: EmbeddingConfig,
}

impl EmbedClient {
    pub fn new(config: &EmbeddingConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            config: config.clone(),
        }
    }

    pub async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        match self.config.provider.as_str() {
            "ollama" => self.embed_ollama(texts).await,
            "openai" => self.embed_openai(texts).await,
            other => anyhow::bail!("Unknown embedding provider: {}", other),
        }
    }

    async fn embed_ollama(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        let body = serde_json::json!({
            "model": self.config.model,
            "input": texts,
        });

        let resp = self
            .http
            .post(format!("{}/api/embed", self.config.url))
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = substrate::client::text_from_response(
                resp,
                "ollama /api/embed",
                Some(8 * 1024 * 1024),
            )
            .await
            .unwrap_or_default();
            anyhow::bail!("Ollama embedding failed ({}): {}", status, text);
        }

        let json: serde_json::Value =
            substrate::client::json_from_response(resp, "ollama /api/embed", Some(8 * 1024 * 1024))
                .await?;
        let embeddings = json
            .get("embeddings")
            .and_then(|e| e.as_array())
            .ok_or_else(|| anyhow::anyhow!("Missing 'embeddings' in Ollama response"))?;

        embeddings
            .iter()
            .map(|emb| {
                emb.as_array()
                    .ok_or_else(|| anyhow::anyhow!("Invalid embedding array"))
                    .and_then(|arr| {
                        arr.iter()
                            .map(|v| {
                                v.as_f64()
                                    .map(|f| f as f32)
                                    .ok_or_else(|| anyhow::anyhow!("Invalid embedding value"))
                            })
                            .collect()
                    })
            })
            .collect()
    }

    async fn embed_openai(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        let body = serde_json::json!({
            "model": self.config.model,
            "input": texts,
        });

        let mut req = self
            .http
            .post(format!("{}/v1/embeddings", self.config.url))
            .json(&body);

        if let Some(ref api_key_secret) = self.config.api_key_secret {
            req = req.header("Authorization", format!("Bearer {}", api_key_secret));
        }

        let resp = req.send().await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = substrate::client::text_from_response(
                resp,
                "openai /v1/embeddings",
                Some(8 * 1024 * 1024),
            )
            .await
            .unwrap_or_default();
            anyhow::bail!("OpenAI embedding failed ({}): {}", status, text);
        }

        let json: serde_json::Value = substrate::client::json_from_response(
            resp,
            "openai /v1/embeddings",
            Some(8 * 1024 * 1024),
        )
        .await?;
        let data = json
            .get("data")
            .and_then(|d| d.as_array())
            .ok_or_else(|| anyhow::anyhow!("Missing 'data' in OpenAI response"))?;

        let mut results: Vec<(usize, Vec<f32>)> = Vec::with_capacity(data.len());
        for item in data {
            let index = item
                .get("index")
                .and_then(|i| i.as_u64())
                .unwrap_or(results.len() as u64) as usize;
            let embedding = item
                .get("embedding")
                .and_then(|e| e.as_array())
                .ok_or_else(|| anyhow::anyhow!("Missing embedding in OpenAI response item"))?
                .iter()
                .map(|v| {
                    v.as_f64()
                        .map(|f| f as f32)
                        .ok_or_else(|| anyhow::anyhow!("Invalid embedding value"))
                })
                .collect::<Result<Vec<f32>>>()?;
            results.push((index, embedding));
        }

        results.sort_by_key(|(i, _)| *i);
        Ok(results.into_iter().map(|(_, v)| v).collect())
    }
}
