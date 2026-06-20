//! Storage module for Synapse.
//!
//! Provides a trait-based storage abstraction with implementations for:
//! - PostgreSQL (production)
//! - Redb (embedded, development)

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "redb")]
mod redb;

#[cfg(feature = "postgres")]
pub use self::postgres::PostgresStorage;
#[cfg(feature = "redb")]
pub use self::redb::RedbStorage;

use anyhow::Result;
use async_trait::async_trait;

/// Bond with relation information — re-export of substrate's `BondProjection`.
pub type SporeBond = substrate::BondProjection;

/// Result of saving a spore — indicates what action was taken.
#[derive(Debug, Clone, PartialEq)]
pub enum SaveSporeResult {
    /// New content stored (hash never seen before)
    Indexed,
    /// Content already known, new host domain registered (replicate)
    Replicated,
    /// Same domain and hash already stored (no-op upsert)
    Duplicate,
}

/// A single bond reference attached to a lineage node.
#[derive(Debug, Clone)]
pub struct LineageRef {
    /// Raw relation type string (e.g., "spawned_from", "absorbed_from").
    /// Passed through as-is — no enum parsing — so future relation types work without upgrades.
    pub relation: String,
    /// Full CMN URI of the referenced spore.
    #[allow(dead_code)]
    pub uri: String,
}

/// Lineage node with relationship information.
#[derive(Debug, Clone)]
pub struct LineageNode {
    /// Full CMN URI of this spore (e.g., "cmn://example.com/b3.abc123")
    pub uri: String,
    /// All bond references for this node, with raw relation strings.
    pub refs: Vec<LineageRef>,
}

/// Result of a lineage traversal query.
#[derive(Debug, Clone)]
pub struct LineageResult {
    pub nodes: Vec<LineageNode>,
    pub max_depth_reached: bool,
}

/// Mycelium row for listing all myceliums.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MyceliumRow {
    pub domain: String,
    pub hash: String,
    pub updated_at_epoch_ms: u64,
}

/// Storage trait for Synapse data persistence.
#[async_trait]
pub trait Storage: Send + Sync + Clone {
    /// Save a spore to storage.
    /// Returns what action was taken: new content, mirror registration, or duplicate.
    async fn save_spore(
        &self,
        hash: &str,
        domain: &str,
        data: &[u8],
        bonds: &[SporeBond],
    ) -> Result<SaveSporeResult>;

    /// Get a spore by hash.
    async fn get_spore(&self, hash: &str) -> Result<Option<Vec<u8>>>;

    /// Save a mycelium to storage (successful sync).
    async fn save_mycelium(
        &self,
        domain: &str,
        data: &[u8],
        public_key: &str,
        hash: &str,
    ) -> Result<()>;

    /// Mark mycelium sync as failed.
    async fn mark_mycelium_failed(&self, domain: &str, error: &str) -> Result<()>;

    /// Get a mycelium by domain.
    async fn get_mycelium(&self, domain: &str) -> Result<Option<Vec<u8>>>;

    /// Get mycelium hash by domain.
    async fn get_mycelium_hash(&self, domain: &str) -> Result<Option<String>>;

    /// Get all myceliums with their hashes.
    /// Returns: Vec of (domain, hash, updated_at_epoch_ms)
    async fn get_all_myceliums(&self) -> Result<Vec<MyceliumRow>>;

    /// Get domains that need retry (failed with retry_count < max_retries).
    async fn get_domains_for_retry(&self, max_retries: i32) -> Result<Vec<String>>;

    /// Get all spore hashes for a domain.
    async fn get_domain_spores(&self, domain: &str) -> Result<Vec<String>>;

    /// Get lineage of a spore (all spores that bond to this one - descendants only).
    /// Uses BFS to traverse the bond graph.
    /// max_depth: Maximum depth to traverse (0 = unlimited, recommended: 10)
    async fn get_spore_lineage(&self, hash: &str, max_depth: usize) -> Result<LineageResult>;

    /// Get all domains (hosts) that have published this spore hash.
    /// Returns: Vec of domain names
    async fn get_spore_hosts(&self, hash: &str) -> Result<Vec<String>>;

    /// Get ancestors of a spore (all spores that this one bonds to).
    /// max_depth: Maximum depth to traverse (0 = unlimited, recommended: 10)
    async fn get_spore_ancestors(&self, hash: &str, max_depth: usize) -> Result<LineageResult>;

    /// Save a TLS certificate.
    async fn save_cert(
        &self,
        domains: &[String],
        cert_pem: &str,
        key_pem: &str,
        expires_at: chrono::DateTime<chrono::Utc>,
        self_signed: bool,
    ) -> Result<()>;

    /// Get a TLS certificate for domains.
    async fn get_cert(&self, domains: &[String]) -> Result<Option<TlsCert>>;

    /// Save a settings value by key.
    async fn save_setting(&self, key: &str, value: &str) -> Result<()>;

    /// Get a settings value by key.
    async fn get_setting(&self, key: &str) -> Result<Option<String>>;

    /// Save a Nostr event to persistent storage (deduplicated by event_id).
    async fn save_nostr_event(&self, event_id: &str, event_json: &[u8]) -> Result<()>;

    /// Load all stored Nostr events.
    async fn get_nostr_events(&self) -> Result<Vec<Vec<u8>>>;

    /// Save a taste report for a target URI.
    async fn save_taste(&self, target_uri: &str, domain: &str, data: &[u8]) -> Result<()>;

    /// Get all taste reports for an exact target URI.
    #[allow(dead_code)]
    async fn get_tastes(&self, target_uri: &str) -> Result<Vec<Vec<u8>>>;

    /// Get all taste reports for a spore hash across domains.
    async fn get_spore_tastes(&self, spore_hash: &str) -> Result<Vec<Vec<u8>>>;

    /// Batch-get spores by hash. Default loops over `get_spore`.
    async fn get_spores_batch(&self, hashes: &[&str]) -> Result<Vec<(String, Option<Vec<u8>>)>> {
        let mut results = Vec::with_capacity(hashes.len());
        for hash in hashes {
            let data = self.get_spore(hash).await?;
            results.push((hash.to_string(), data));
        }
        Ok(results)
    }

    /// Batch-get taste reports for multiple spore hashes. Default loops over `get_spore_tastes`.
    async fn get_spore_tastes_batch(&self, hashes: &[&str]) -> Result<Vec<(String, Vec<Vec<u8>>)>> {
        let mut results = Vec::with_capacity(hashes.len());
        for hash in hashes {
            let tastes = self.get_spore_tastes(hash).await?;
            results.push((hash.to_string(), tastes));
        }
        Ok(results)
    }

    /// Get the cached public key for a domain (from the last successful mycelium sync).
    async fn get_domain_public_key(&self, domain: &str) -> Result<Option<String>>;

    /// Look up a single taste report by its own content hash.
    /// Default: not indexed, returns None.
    async fn get_taste_by_hash(&self, _hash: &str) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    /// Save a taste hash → data index entry.
    /// Called alongside `save_taste` when the taste hash is known.
    async fn save_taste_index(&self, _taste_hash: &str, _data: &[u8]) -> Result<()> {
        Ok(())
    }

    /// Get total spore count across all domains (single query).
    async fn get_total_spore_count(&self) -> Result<u64> {
        // Default: fallback to iterating domains
        let myceliums = self.get_all_myceliums().await?;
        let mut total = 0u64;
        for row in &myceliums {
            if let Ok(hashes) = self.get_domain_spores(&row.domain).await {
                total += hashes.len() as u64;
            }
        }
        Ok(total)
    }

    /// Look up mycelium rows by hash (reverse index).
    /// Returns all domains hosting a mycelium with the given hash.
    async fn get_mycelium_by_hash(&self, hash: &str) -> Result<Vec<MyceliumRow>> {
        // Default: fall back to full scan
        let all = self.get_all_myceliums().await?;
        Ok(all
            .into_iter()
            .filter(|row| row.hash == hash || row.hash.split(',').any(|h| h == hash))
            .collect())
    }

    /// Batch save multiple spores in a single transaction.
    /// Default implementation calls save_spore in a loop.
    #[allow(dead_code)]
    async fn save_spores_batch(
        &self,
        spores: &[(&str, &str, &[u8], &[SporeBond])],
    ) -> Result<Vec<SaveSporeResult>> {
        let mut results = Vec::with_capacity(spores.len());
        for (hash, domain, data, bonds) in spores {
            results.push(self.save_spore(hash, domain, data, bonds).await?);
        }
        Ok(results)
    }
}

/// TLS certificate record.
#[derive(Debug, Clone)]
pub struct TlsCert {
    pub cert_pem: String,
    pub key_pem_secret: String,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    pub self_signed: bool,
}

impl TlsCert {
    pub fn needs_renewal(&self, days_before: i64) -> bool {
        let renewal_threshold = chrono::Utc::now() + chrono::Duration::days(days_before);
        self.expires_at <= renewal_threshold
    }
}

/// Enum wrapper for dynamic dispatch.
#[derive(Clone)]
pub enum StorageBackend {
    #[cfg(feature = "postgres")]
    Postgres(PostgresStorage),
    #[cfg(feature = "redb")]
    Redb(RedbStorage),
}

#[async_trait]
impl Storage for StorageBackend {
    async fn save_spore(
        &self,
        hash: &str,
        domain: &str,
        data: &[u8],
        bonds: &[SporeBond],
    ) -> Result<SaveSporeResult> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.save_spore(hash, domain, data, bonds).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.save_spore(hash, domain, data, bonds).await,
        }
    }

    async fn get_spore(&self, hash: &str) -> Result<Option<Vec<u8>>> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.get_spore(hash).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.get_spore(hash).await,
        }
    }

    async fn save_mycelium(
        &self,
        domain: &str,
        data: &[u8],
        public_key: &str,
        hash: &str,
    ) -> Result<()> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.save_mycelium(domain, data, public_key, hash).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.save_mycelium(domain, data, public_key, hash).await,
        }
    }

    async fn mark_mycelium_failed(&self, domain: &str, error: &str) -> Result<()> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.mark_mycelium_failed(domain, error).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.mark_mycelium_failed(domain, error).await,
        }
    }

    async fn get_mycelium(&self, domain: &str) -> Result<Option<Vec<u8>>> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.get_mycelium(domain).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.get_mycelium(domain).await,
        }
    }

    async fn get_domains_for_retry(&self, max_retries: i32) -> Result<Vec<String>> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.get_domains_for_retry(max_retries).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.get_domains_for_retry(max_retries).await,
        }
    }

    async fn get_domain_spores(&self, domain: &str) -> Result<Vec<String>> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.get_domain_spores(domain).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.get_domain_spores(domain).await,
        }
    }

    async fn get_spore_lineage(&self, hash: &str, max_depth: usize) -> Result<LineageResult> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.get_spore_lineage(hash, max_depth).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.get_spore_lineage(hash, max_depth).await,
        }
    }

    async fn get_spore_hosts(&self, hash: &str) -> Result<Vec<String>> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.get_spore_hosts(hash).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.get_spore_hosts(hash).await,
        }
    }

    async fn get_spore_ancestors(&self, hash: &str, max_depth: usize) -> Result<LineageResult> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.get_spore_ancestors(hash, max_depth).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.get_spore_ancestors(hash, max_depth).await,
        }
    }

    async fn save_cert(
        &self,
        domains: &[String],
        cert_pem: &str,
        key_pem: &str,
        expires_at: chrono::DateTime<chrono::Utc>,
        self_signed: bool,
    ) -> Result<()> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => {
                s.save_cert(domains, cert_pem, key_pem, expires_at, self_signed)
                    .await
            }
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => {
                s.save_cert(domains, cert_pem, key_pem, expires_at, self_signed)
                    .await
            }
        }
    }

    async fn get_cert(&self, domains: &[String]) -> Result<Option<TlsCert>> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.get_cert(domains).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.get_cert(domains).await,
        }
    }

    async fn get_mycelium_hash(&self, domain: &str) -> Result<Option<String>> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.get_mycelium_hash(domain).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.get_mycelium_hash(domain).await,
        }
    }

    async fn get_all_myceliums(&self) -> Result<Vec<MyceliumRow>> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.get_all_myceliums().await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.get_all_myceliums().await,
        }
    }

    async fn save_setting(&self, key: &str, value: &str) -> Result<()> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.save_setting(key, value).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.save_setting(key, value).await,
        }
    }

    async fn get_setting(&self, key: &str) -> Result<Option<String>> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.get_setting(key).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.get_setting(key).await,
        }
    }

    async fn save_nostr_event(&self, event_id: &str, event_json: &[u8]) -> Result<()> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.save_nostr_event(event_id, event_json).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.save_nostr_event(event_id, event_json).await,
        }
    }

    async fn get_nostr_events(&self) -> Result<Vec<Vec<u8>>> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.get_nostr_events().await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.get_nostr_events().await,
        }
    }

    async fn save_taste(&self, target_uri: &str, domain: &str, data: &[u8]) -> Result<()> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.save_taste(target_uri, domain, data).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.save_taste(target_uri, domain, data).await,
        }
    }

    async fn get_tastes(&self, target_uri: &str) -> Result<Vec<Vec<u8>>> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.get_tastes(target_uri).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.get_tastes(target_uri).await,
        }
    }

    async fn get_spore_tastes(&self, spore_hash: &str) -> Result<Vec<Vec<u8>>> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.get_spore_tastes(spore_hash).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.get_spore_tastes(spore_hash).await,
        }
    }

    async fn get_domain_public_key(&self, domain: &str) -> Result<Option<String>> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.get_domain_public_key(domain).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.get_domain_public_key(domain).await,
        }
    }

    async fn get_spores_batch(&self, hashes: &[&str]) -> Result<Vec<(String, Option<Vec<u8>>)>> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.get_spores_batch(hashes).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.get_spores_batch(hashes).await,
        }
    }

    async fn get_spore_tastes_batch(&self, hashes: &[&str]) -> Result<Vec<(String, Vec<Vec<u8>>)>> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.get_spore_tastes_batch(hashes).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.get_spore_tastes_batch(hashes).await,
        }
    }

    async fn save_spores_batch(
        &self,
        spores: &[(&str, &str, &[u8], &[SporeBond])],
    ) -> Result<Vec<SaveSporeResult>> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.save_spores_batch(spores).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.save_spores_batch(spores).await,
        }
    }

    async fn get_taste_by_hash(&self, hash: &str) -> Result<Option<Vec<u8>>> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.get_taste_by_hash(hash).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.get_taste_by_hash(hash).await,
        }
    }

    async fn save_taste_index(&self, taste_hash: &str, data: &[u8]) -> Result<()> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.save_taste_index(taste_hash, data).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.save_taste_index(taste_hash, data).await,
        }
    }

    async fn get_mycelium_by_hash(&self, hash: &str) -> Result<Vec<MyceliumRow>> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.get_mycelium_by_hash(hash).await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => s.get_mycelium_by_hash(hash).await,
        }
    }

    async fn get_total_spore_count(&self) -> Result<u64> {
        match self {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.get_total_spore_count().await,
            #[cfg(feature = "redb")]
            StorageBackend::Redb(s) => {
                // Redb uses default fallback
                <RedbStorage as Storage>::get_total_spore_count(s).await
            }
        }
    }
}
