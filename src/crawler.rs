use crate::storage::{Storage, StorageBackend};
use anyhow::Result;
use substrate::{parse_uri, CmnCapsuleEntry, Mycelium, Spore};
use tracing::{error, info, info_span, warn, Instrument};

use std::sync::Arc;

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
        self.crawl_domain_inner(domain)
            .instrument(info_span!("crawl", domain = %domain))
            .await
    }

    async fn crawl_domain_inner(&self, domain: &str) -> Result<()> {
        info!("Crawling domain");

        // 1. Fetch cmn.json
        let entry = self.cmn_cache.get_entry(domain).await?;
        let capsule = entry
            .primary_capsule()
            .map_err(|e| anyhow::anyhow!("Invalid cmn.json from {}: {}", domain, e))?;
        let public_key = &capsule.key;
        let primary_hash = capsule
            .mycelium_hash()
            .ok_or_else(|| anyhow::anyhow!("No mycelium hash found in cmn.json for {}", domain))?;
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
                return Ok(());
            } else {
                info!(old_hashes = %cached_hashes, new_hashes = %hashes_key, "Mycelium changed");
            }
        }

        // 5. Download all mycelium shards (merged into one manifest)
        info!("Fetching mycelium shards");
        let mycelium_opts =
            substrate::client::FetchOptions::with_max_bytes(self.max_mycelium_bytes);
        let mycelium_value =
            substrate::client::fetch_mycelium(&self.http, capsule, mycelium_opts).await?;
        let mycelium: Mycelium = serde_json::from_value(mycelium_value)?;

        // 6. Verify signatures; core.key may be a retired domain key.
        let mycelium_author_key = mycelium.embedded_core_key().unwrap_or(public_key);
        if capsule
            .key_confirmation_at(mycelium_author_key, mycelium.timestamp_ms())
            .is_none()
        {
            anyhow::bail!(
                "Mycelium core key {} is not confirmed by {} at timestamp {}",
                mycelium_author_key,
                domain,
                mycelium.timestamp_ms()
            );
        }
        mycelium
            .verify_signatures(public_key, mycelium_author_key)
            .map_err(|e| anyhow::anyhow!("Mycelium signature verification failed: {}", e))?;

        // 6b. Clock skew validation
        let now_ms = crate::time::now_epoch_ms();
        if let Err(e) = substrate::validate_timestamp_not_future(
            mycelium.capsule.core.updated_at_epoch_ms,
            now_ms,
            self.max_clock_skew_ms,
        ) {
            warn!(domain = %domain, error = %e, "Crawled mycelium failed clock skew check");
            anyhow::bail!("Mycelium clock skew: {}", e);
        }

        // 6c. Version check: only accept strictly newer timestamps.
        // Same semantics as ingest::store_mycelium, but silently skips instead of 409.
        if let Ok(Some(cached_data)) = self.storage.get_mycelium(domain).await {
            if let Ok(cached) = serde_json::from_slice::<Mycelium>(&cached_data) {
                let incoming_ts = mycelium.capsule.core.updated_at_epoch_ms;
                let cached_ts = cached.capsule.core.updated_at_epoch_ms;
                if substrate::compare_version_timestamps(incoming_ts, cached_ts)
                    != substrate::VersionOrder::Newer
                {
                    info!(
                        domain = %domain,
                        incoming_updated_at_epoch_ms = incoming_ts,
                        cached_updated_at_epoch_ms = cached_ts,
                        "Crawled mycelium not newer than cached, skipping"
                    );
                    return Ok(());
                }
            }
        }

        // 7. Persistence (store merged mycelium with primary shard hash)
        let data = serde_json::to_vec(&mycelium)?;
        self.storage
            .save_mycelium(domain, &data, public_key, &hashes_key)
            .await?;

        // 8. Spore Ingestion (using endpoints from cmn.json)
        for item in &mycelium.capsule.core.spores {
            // Construct URI from domain and hash
            let spore_uri = format!("cmn://{}/{}", domain, item.hash);
            info!(name = %item.name, uri = %spore_uri, "Discovered spore");
            if let Err(e) = self
                .crawl_spore(&spore_uri, public_key, public_key, capsule)
                .await
            {
                error!(uri = %spore_uri, error = %e, "Failed to crawl spore");
            }
        }

        Ok(())
    }

    /// Crawl only spores from a mycelium that are not already in storage.
    /// Called after a mycelium pulse to index newly-referenced spores.
    pub async fn crawl_missing_spores(&self, domain: &str) -> Result<()> {
        // 1. Load mycelium from storage (already saved by pulse handler)
        let mycelium_data = match self.storage.get_mycelium(domain).await? {
            Some(d) => d,
            None => {
                anyhow::bail!("No mycelium in storage for {}", domain);
            }
        };
        let mycelium: Mycelium = serde_json::from_slice(&mycelium_data)?;

        // 2. Fetch host key and endpoints from cmn.json
        let entry = self.cmn_cache.get_entry(domain).await?;
        let capsule = entry
            .primary_capsule()
            .map_err(|e| anyhow::anyhow!("Invalid cmn.json from {}: {}", domain, e))?;
        let host_key = &capsule.key;

        // 3. Only crawl spores not already in storage
        let mut crawled = 0u32;
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

        Ok(())
    }

    pub async fn crawl_spore(
        &self,
        uri: &str,
        host_key: &str,
        fallback_author_key: &str,
        capsule: &CmnCapsuleEntry,
    ) -> Result<()> {
        // Parse URI: cmn://{domain}/{hash}
        let parsed = parse_uri(uri)?;
        let domain = &parsed.domain;
        let hash = parsed
            .hash
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Spore URI missing hash: {}", uri))?;

        // 1. Fetch Spore Manifest using endpoint template
        let opts = substrate::client::FetchOptions::with_max_bytes(self.max_spore_bytes);
        let spore_value =
            substrate::client::fetch_spore_manifest(&self.http, capsule, hash, opts).await?;
        let spore: Spore = serde_json::from_value(spore_value)?;

        // 2. Verify Signature (two-layer: capsule with host key, core with author key)
        let author_key = if let Some(core_key) = spore.embedded_core_key() {
            if spore.author_domain() == domain {
                if capsule
                    .key_confirmation_at(core_key, spore.timestamp_ms())
                    .is_none()
                {
                    anyhow::bail!(
                        "Spore core key {} is not confirmed by {} at timestamp {}",
                        core_key,
                        domain,
                        spore.timestamp_ms()
                    );
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
                    .await?
                    .key
            }
        } else {
            fallback_author_key.to_string()
        };
        self.verify_spore_signature(&spore, host_key, &author_key)?;

        // 3. Persistence
        let data = serde_json::to_vec(&spore)?;
        let bonds = spore.extract_bonds();
        self.storage.save_spore(hash, domain, &data, &bonds).await?;

        #[cfg(feature = "ruvector")]
        if let Some(ref search) = self.search {
            use crate::search::Search;
            if let Ok(json) = serde_json::to_value(&spore) {
                if let Err(e) = search.index_spore(domain, hash, &json).await {
                    tracing::warn!(error = %e, "Search indexing failed for spore");
                }
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
