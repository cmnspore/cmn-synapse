use crate::storage::{Storage, StorageBackend};
use anyhow::Result;
use substrate::{CmnCapsuleEntry, Mycelium, Spore, parse_uri};
use tracing::{Instrument, error, info, info_span, warn};

use std::sync::Arc;

#[derive(Debug)]
struct PermanentContentFailure(anyhow::Error);

impl std::fmt::Display for PermanentContentFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for PermanentContentFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

fn content_error(error: anyhow::Error) -> anyhow::Error {
    if is_retryable_crawl_error(&error) {
        error
    } else {
        PermanentContentFailure(error).into()
    }
}

/// Preserve typed HTTP errors so retry decisions never parse human error messages.
pub(crate) async fn fetch_json(
    client: &reqwest::Client,
    url: &str,
    max_bytes: usize,
) -> Result<serde_json::Value> {
    let result = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let mut response = client.get(url).send().await?.error_for_status()?;
        anyhow::ensure!(
            response
                .content_length()
                .is_none_or(|length| length <= max_bytes as u64),
            "Manifest exceeds body limit"
        );
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            anyhow::ensure!(
                bytes.len().saturating_add(chunk.len()) <= max_bytes,
                "Manifest exceeds body limit"
            );
            bytes.extend_from_slice(&chunk);
        }
        substrate::client::json_from_slice_reject_duplicates(&bytes, "CMN endpoint")
    })
    .await?;
    result.map_err(content_error)
}

pub fn is_retryable_crawl_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<tokio::time::error::Elapsed>()
            .is_some()
            || cause.downcast_ref::<reqwest::Error>().is_some_and(|error| {
                error.is_timeout()
                    || error.is_connect()
                    || error.is_body()
                    || (error.is_request()
                        && error.status().is_none()
                        && !error.is_builder()
                        && !error.is_redirect()
                        && !error.is_decode())
                    || error
                        .status()
                        .is_some_and(|status| status.is_server_error())
            })
    })
}

#[derive(Clone)]
pub struct Crawler {
    storage: StorageBackend,
    cmn_cache: Arc<crate::cmn_cache::CmnCache>,
    http: reqwest::Client,
    max_mycelium_bytes: usize,
    max_spore_bytes: usize,
    max_clock_skew_ms: u64,
    #[cfg(feature = "ruvector")]
    search: Option<Arc<crate::search::SearchBackend>>,
    #[cfg(feature = "ruvector")]
    graph: Option<Arc<crate::search::SporeGraph>>,
}

impl Crawler {
    pub fn new(
        storage: StorageBackend,
        cmn_cache: Arc<crate::cmn_cache::CmnCache>,
        http: reqwest::Client,
        cache_config: &crate::config::CacheConfig,
        pulse_config: &crate::config::PulseConfig,
        #[cfg(feature = "ruvector")] search: Option<Arc<crate::search::SearchBackend>>,
        #[cfg(feature = "ruvector")] graph: Option<Arc<crate::search::SporeGraph>>,
    ) -> Self {
        Self {
            storage,
            cmn_cache,
            http,
            max_mycelium_bytes: cache_config.max_mycelium_bytes,
            max_spore_bytes: cache_config.max_spore_bytes,
            max_clock_skew_ms: pulse_config.max_clock_skew_s * 1000,
            #[cfg(feature = "ruvector")]
            search,
            #[cfg(feature = "ruvector")]
            graph,
        }
    }

    pub async fn crawl_domain(&self, domain: &str) -> Result<()> {
        let result = self
            .crawl_domain_inner(domain)
            .instrument(info_span!("crawl", domain = %domain))
            .await;
        self.finish_sync(domain, result).await
    }

    async fn crawl_domain_inner(&self, domain: &str) -> Result<()> {
        info!("Crawling domain");

        // 1. Fetch cmn.json
        let entry = self
            .cmn_cache
            .get_entry(domain)
            .await
            .map_err(content_error)?;
        let capsule = entry.primary_capsule().map_err(content_error)?;
        let public_key = &capsule.key;
        let primary_hash = capsule
            .mycelium_hash()
            .ok_or_else(|| content_error(anyhow::anyhow!("No mycelium hash found in cmn.json")))?;
        let overflow_hashes = capsule.mycelium_hashes();
        let hashes_key = if overflow_hashes.is_empty() {
            primary_hash.to_string()
        } else {
            format!("{},{}", primary_hash, overflow_hashes.join(","))
        };

        info!(public_key = %public_key, hashes_key = %hashes_key, "Fetched cmn.json");

        // 3. Check if mycelium has changed (compare full hashes array)
        if let Some(cached_hashes) = self.storage.get_mycelium_hash(domain).await? {
            if cached_hashes == hashes_key {
                info!(hashes = %hashes_key, "Mycelium unchanged, skipping");
                return self.crawl_missing_spores_inner(domain).await;
            } else {
                info!(old_hashes = %cached_hashes, new_hashes = %hashes_key, "Mycelium changed");
            }
        }

        // 5. Download all mycelium shards (merged into one manifest)
        info!("Fetching mycelium shards");
        let mut mycelium_value = fetch_json(
            &self.http,
            &capsule.mycelium_url(primary_hash).map_err(content_error)?,
            self.max_mycelium_bytes,
        )
        .await?;
        let primary: Mycelium = serde_json::from_value(mycelium_value.clone())
            .map_err(|error| content_error(error.into()))?;
        primary
            .verify_uri_hash(primary_hash)
            .map_err(content_error)?;
        for hash in capsule.mycelium_hashes() {
            let shard = fetch_json(
                &self.http,
                &capsule.mycelium_url(hash).map_err(content_error)?,
                self.max_mycelium_bytes,
            )
            .await?;
            let manifest: Mycelium = serde_json::from_value(shard.clone())
                .map_err(|error| content_error(error.into()))?;
            manifest.verify_uri_hash(hash).map_err(content_error)?;
            substrate::client::merge_mycelium_spores(&mut mycelium_value, &shard);
        }
        let mycelium: Mycelium =
            serde_json::from_value(mycelium_value).map_err(|error| content_error(error.into()))?;

        // 6. Verify signatures; core.key may be a retired domain key.
        let mycelium_author_key = mycelium.embedded_core_key().unwrap_or(public_key);
        if capsule
            .key_confirmation_at(mycelium_author_key, mycelium.timestamp_ms())
            .is_none()
        {
            return Err(content_error(anyhow::anyhow!(
                "Mycelium core key is not confirmed by its domain"
            )));
        }
        mycelium
            .verify_signatures(public_key, mycelium_author_key)
            .map_err(content_error)?;

        // 6b. Clock skew validation
        let now_ms = crate::time::now_epoch_ms();
        if let Err(e) = substrate::validate_timestamp_not_future(
            mycelium.capsule.core.updated_at_epoch_ms,
            now_ms,
            self.max_clock_skew_ms,
        ) {
            warn!(domain = %domain, error = %e, "Crawled mycelium failed clock skew check");
            return Err(content_error(e));
        }

        // 6c. Version check: only accept strictly newer timestamps.
        // Same semantics as ingest::store_mycelium, but silently skips instead of 409.
        if let Some(cached_data) = self.storage.get_mycelium(domain).await? {
            let cached: Mycelium = serde_json::from_slice(&cached_data)?;
            let incoming_ts = mycelium.capsule.core.updated_at_epoch_ms;
            let cached_ts = cached.capsule.core.updated_at_epoch_ms;
            if substrate::compare_version_timestamps(incoming_ts, cached_ts)
                != substrate::VersionOrder::Newer
            {
                info!(domain = %domain, incoming_updated_at_epoch_ms = incoming_ts, cached_updated_at_epoch_ms = cached_ts, "Crawled mycelium not newer than cached, skipping");
                return self.crawl_missing_spores_inner(domain).await;
            }
        }

        // 7. Persistence (store merged mycelium with primary shard hash)
        let data = serde_json::to_vec(&mycelium)?;
        self.storage
            .save_mycelium(domain, &data, public_key, &hashes_key)
            .await?;

        self.crawl_missing_spores_inner(domain).await
    }

    /// Crawl only spores from a mycelium that are not already in storage.
    /// Called after a mycelium pulse to index newly-referenced spores.
    pub async fn crawl_missing_spores(&self, domain: &str) -> Result<()> {
        let result = self.crawl_missing_spores_inner(domain).await;
        self.finish_sync(domain, result).await
    }

    async fn finish_sync(&self, domain: &str, result: Result<()>) -> Result<()> {
        match result {
            Ok(()) => self.storage.mark_mycelium_synced(domain).await,
            Err(error) if is_retryable_crawl_error(&error) => {
                self.storage
                    .mark_mycelium_failed(domain, "Temporary content fetch failure")
                    .await?;
                Err(error)
            }
            Err(error) if error.downcast_ref::<PermanentContentFailure>().is_some() => {
                warn!(error = %error, domain = %domain, "Permanent content fetch failure; skipping retry");
                self.storage.mark_mycelium_synced(domain).await
            }
            Err(error) => Err(error),
        }
    }

    async fn crawl_missing_spores_inner(&self, domain: &str) -> Result<()> {
        // 1. Load mycelium from storage (already saved by pulse handler)
        let mycelium_data = match self.storage.get_mycelium(domain).await? {
            Some(d) => d,
            None => {
                anyhow::bail!("No mycelium in storage for {}", domain);
            }
        };
        let mycelium: Mycelium = serde_json::from_slice(&mycelium_data)?;

        // 2. Fetch host key and endpoints from cmn.json
        let entry = self
            .cmn_cache
            .get_entry(domain)
            .await
            .map_err(content_error)?;
        let capsule = entry.primary_capsule().map_err(content_error)?;
        let host_key = &capsule.key;

        // 3. Only crawl spores not already in storage
        let mut crawled = 0u32;
        let mut retry_error = None;
        for item in &mycelium.capsule.core.spores {
            if self.storage.get_spore(&item.hash).await?.is_some() {
                continue;
            }
            let spore_uri = format!("cmn://{}/{}", domain, item.hash);
            info!(name = %item.name, uri = %spore_uri, "Fetching missing spore");
            if let Err(e) = self
                .crawl_spore(&spore_uri, host_key, host_key, capsule)
                .await
            {
                error!(uri = %spore_uri, error = %e, "Failed to crawl spore");
                if is_retryable_crawl_error(&e) {
                    retry_error = Some(e);
                } else if e.downcast_ref::<PermanentContentFailure>().is_none() {
                    return Err(e);
                }
            } else {
                crawled += 1;
            }
        }

        if crawled > 0 {
            info!(
                crawled_count = crawled,
                "Indexed missing spores after mycelium pulse"
            );
        }

        // 4. Remove superseded spores from search index.
        // Current hashes = what's in the mycelium. Anything else for this domain is old.
        #[cfg(feature = "ruvector")]
        if let Some(ref search) = self.search {
            use crate::search::Search;
            use std::collections::HashSet;

            let current: HashSet<&str> = mycelium
                .capsule
                .core
                .spores
                .iter()
                .map(|s| s.hash.as_str())
                .collect();

            if let Ok(all_hashes) = self.storage.get_domain_spores(domain).await {
                let mut removed = 0u32;
                for hash in &all_hashes {
                    if !current.contains(hash.as_str()) {
                        search.remove_spore(hash);
                        removed += 1;
                    }
                }
                if removed > 0 {
                    info!(
                        removed_count = removed,
                        "Removed superseded spores from search index"
                    );
                }
            }
        }

        match retry_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub async fn crawl_spore(
        &self,
        uri: &str,
        host_key: &str,
        fallback_author_key: &str,
        capsule: &CmnCapsuleEntry,
    ) -> Result<()> {
        // Parse URI: cmn://{domain}/{hash}
        let parsed = parse_uri(uri).map_err(content_error)?;
        let domain = &parsed.domain;
        let hash = parsed
            .hash
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Spore URI missing hash: {}", uri))?;

        // 1. Fetch Spore Manifest using endpoint template
        let spore_value = fetch_json(
            &self.http,
            &capsule.spore_url(hash).map_err(content_error)?,
            self.max_spore_bytes,
        )
        .await?;
        let spore: Spore =
            serde_json::from_value(spore_value).map_err(|error| content_error(error.into()))?;

        // 2. Verify Signature (two-layer: capsule with host key, core with author key)
        let author_key = if let Some(core_key) = spore.embedded_core_key() {
            if spore.author_domain() == domain {
                if capsule
                    .key_confirmation_at(core_key, spore.timestamp_ms())
                    .is_none()
                {
                    return Err(content_error(anyhow::anyhow!(
                        "Spore core key is not confirmed by its domain"
                    )));
                }
                core_key.to_string()
            } else {
                self.cmn_cache
                    .resolve_confirmed_key(
                        spore.author_domain(),
                        core_key,
                        spore.timestamp_ms(),
                        &self.storage,
                    )
                    .await
                    .map_err(|error| {
                        if error
                            .downcast_ref::<crate::cmn_cache::DomainVerificationFailure>()
                            .is_some()
                        {
                            content_error(error)
                        } else {
                            error
                        }
                    })?
                    .key
            }
        } else {
            fallback_author_key.to_string()
        };
        self.verify_spore_signature(&spore, host_key, &author_key)
            .map_err(content_error)?;
        if spore.uri() != uri {
            return Err(content_error(anyhow::anyhow!(
                "Spore URI does not match requested content"
            )));
        }

        // 3. Persistence
        let data = serde_json::to_vec(&spore)?;
        let bonds = spore.extract_bonds();
        self.storage.save_spore(hash, domain, &data, &bonds).await?;

        #[cfg(feature = "ruvector")]
        if let Some(ref search) = self.search {
            use crate::search::Search;
            if let Ok(json) = serde_json::to_value(&spore)
                && let Err(e) = search.index_spore(domain, hash, &json).await
            {
                tracing::warn!(error = %e, "Search indexing failed for spore");
            }
            // Note: superseded spore cleanup happens in crawl_missing_spores (mycelium-based)
        }

        #[cfg(feature = "ruvector")]
        if let Some(ref graph) = self.graph {
            if let Err(e) = graph.add_spore(hash, domain) {
                tracing::warn!(error = %e, "Graph add_spore failed");
            }
            if !bonds.is_empty() {
                // Spawn graph relationship indexing in background to avoid blocking the crawl
                // loop when the embedding API is slow.
                let graph = graph.clone();
                let hash = hash.to_string();
                let bonds = bonds.clone();
                tokio::spawn(async move {
                    if let Err(e) = graph.add_relationship(&hash, &bonds).await {
                        tracing::warn!(error = %e, "Graph indexing failed for spore");
                    }
                });
            }
        }

        info!(uri = %uri, "Successfully indexed spore");

        // 4. Recursive reference crawling could go here (with depth limit)

        Ok(())
    }

    /// Verify spore signatures via substrate's unified two-key verification.
    fn verify_spore_signature(
        &self,
        spore: &Spore,
        host_key: &str,
        author_key: &str,
    ) -> Result<()> {
        spore
            .verify_signatures(host_key, author_key)
            .map_err(|e| anyhow::anyhow!("Spore signature verification failed: {}", e))
    }
}
