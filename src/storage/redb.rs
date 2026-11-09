//! Redb (embedded) storage implementation.

use super::{
    LineageNode, LineageRef, LineageResult, MyceliumRow, SaveSporeResult, SporeBond, Storage,
    TlsCert,
};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use std::path::Path;
use std::sync::Arc;

/// Parse CMN spore URI to extract domain and hash
/// e.g., "cmn://example.com/b3.abc123" -> ("example.com", "b3.abc123")
/// Only parses spore URIs (with hash), returns None for domain-only URIs
fn parse_cmn_uri(uri: &str) -> Option<(String, String)> {
    let parsed = substrate::parse_uri(uri).ok()?;
    if parsed.kind != substrate::CmnUriKind::Spore {
        return None;
    }
    Some((parsed.domain, parsed.hash?))
}

// Table Definitions
// SPORES: key format "domain:hash" to represent (domain, hash) composite key
// Note: table names use plural form (SQL convention), Rust constants match.
const SPORES: TableDefinition<&str, &[u8]> = TableDefinition::new("spores");
const MYCELIA: TableDefinition<&str, &[u8]> = TableDefinition::new("mycelia");
const INBOUND: TableDefinition<&str, Vec<&str>> = TableDefinition::new("inbound");
const DOMAINS: TableDefinition<&str, Vec<&str>> = TableDefinition::new("domains");
const TLS_CERTS: TableDefinition<&str, &[u8]> = TableDefinition::new("tls_certs");
// HASH_INDEX: hash -> list of domain:hash keys (for content-addressable lookup)
const HASH_INDEX: TableDefinition<&str, Vec<&str>> = TableDefinition::new("hash_index");
// SPORE_REFS: spore_hash -> JSON array of references [{uri, type}]
const SPORE_REFS: TableDefinition<&str, &[u8]> = TableDefinition::new("spore_refs");
const SETTINGS: TableDefinition<&str, &str> = TableDefinition::new("settings");
// NOSTR_EVENTS: event_id (hex) -> event JSON bytes
const NOSTR_EVENTS: TableDefinition<&str, &[u8]> = TableDefinition::new("nostr_events");
// TASTE_REPORTS: "{target_uri}|{domain}" -> taste report JSON bytes
const TASTE_REPORTS: TableDefinition<&str, &[u8]> = TableDefinition::new("taste_reports");
// TASTE_BY_HASH: taste content hash -> taste report JSON bytes
const TASTE_BY_HASH: TableDefinition<&str, &[u8]> = TableDefinition::new("taste_by_hash");
// TASTE_SPORE_INDEX: spore_hash -> Vec<"target_uri|domain"> keys
const TASTE_SPORE_INDEX: TableDefinition<&str, Vec<&str>> =
    TableDefinition::new("taste_spore_index");
// MYCELIA_HASH_INDEX: hash -> Vec<domain> (reverse index for hash-based lookup)
const MYCELIA_HASH_INDEX: TableDefinition<&str, Vec<&str>> =
    TableDefinition::new("mycelium_hash_index"); // keep on-disk name for backward compat

#[derive(Clone)]
pub struct RedbStorage {
    db: Arc<Database>,
}

/// Serialized TLS certificate for redb storage.
#[derive(serde::Serialize, serde::Deserialize)]
struct SerializedCert {
    domains: String,
    cert_pem: String,
    key_pem_secret: String,
    expires_at_epoch_s: i64,
    self_signed: bool,
}

/// Serialized mycelium record for redb storage.
#[derive(serde::Serialize, serde::Deserialize)]
struct MyceliumRecord {
    data: Vec<u8>,
    public_key: String,
    hash: String,
    updated_at_epoch_ms: u64,
    status: String,
    retry_count: i32,
    last_error: Option<String>,
    next_retry_at_epoch_s: Option<i64>,
}

impl RedbStorage {
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn remove_test_table(&self, name: &str) -> Result<()> {
        let transaction = self.db.begin_write()?;
        match name {
            "spores" => {
                transaction.delete_table(SPORES)?;
            }
            "domains" => {
                transaction.delete_table(DOMAINS)?;
            }
            "mycelia" => {
                transaction.delete_table(MYCELIA)?;
            }
            _ => anyhow::bail!("Unknown test table"),
        }
        transaction.commit()?;
        Ok(())
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn retry_record(&self, domain: &str) -> Result<Option<(String, i32, bool, bool)>> {
        let transaction = self.db.begin_read()?;
        let table = transaction.open_table(MYCELIA)?;
        table
            .get(domain)?
            .map(|value| {
                let record: MyceliumRecord = serde_json::from_slice(value.value())?;
                Ok((
                    record.status,
                    record.retry_count,
                    record.last_error.is_some(),
                    record.next_retry_at_epoch_s.is_some(),
                ))
            })
            .transpose()
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::create(path)?;

        // Ensure tables exist
        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(SPORES)?;
            let _ = write_txn.open_table(MYCELIA)?;
            let _ = write_txn.open_table(INBOUND)?;
            let _ = write_txn.open_table(DOMAINS)?;
            let _ = write_txn.open_table(TLS_CERTS)?;
            let _ = write_txn.open_table(HASH_INDEX)?;
            let _ = write_txn.open_table(SPORE_REFS)?;
            let _ = write_txn.open_table(NOSTR_EVENTS)?;
            let _ = write_txn.open_table(TASTE_REPORTS)?;
            let _ = write_txn.open_table(TASTE_BY_HASH)?;
            let _ = write_txn.open_table(TASTE_SPORE_INDEX)?;
            let _ = write_txn.open_table(MYCELIA_HASH_INDEX)?;
            let _ = write_txn.open_table(SETTINGS)?;
        }
        write_txn.commit()?;

        let storage = Self { db: Arc::new(db) };
        storage.run_migrations()?;
        Ok(storage)
    }

    // --- Schema migration system ---

    fn get_schema_version(&self) -> Result<u32> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(SETTINGS)?;
        match table.get("schema_version")? {
            Some(v) => Ok(v.value().parse().unwrap_or(0)),
            None => Ok(0),
        }
    }

    fn set_schema_version(&self, version: u32) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(SETTINGS)?;
            table.insert("schema_version", version.to_string().as_str())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn run_migrations(&self) -> Result<()> {
        let version = self.get_schema_version()?;

        if version < 1 {
            tracing::info!("Running redb migration v1: Agent-First Data field renames");
            self.migrate_v1_afd_renames()?;
            self.set_schema_version(1)?;
        }

        Ok(())
    }

    /// v1: Rename JSON fields to Agent-First Data convention.
    /// - MyceliumRecord: updated_at → updated_at_epoch_ms, next_retry_at → next_retry_at_epoch_s
    /// - SerializedCert: key_pem → key_pem_secret, expires_at → expires_at_epoch_s
    fn migrate_v1_afd_renames(&self) -> Result<()> {
        // Migrate MYCELIA entries
        {
            let read_txn = self.db.begin_read()?;
            let table = read_txn.open_table(MYCELIA)?;
            let mut updates = Vec::new();
            for entry in table.iter()? {
                let (key, value) = entry?;
                let mut json: serde_json::Value = serde_json::from_slice(value.value())?;
                let mut changed = false;
                if let Some(obj) = json.as_object_mut() {
                    if let Some(v) = obj.remove("updated_at") {
                        obj.insert("updated_at_epoch_ms".to_string(), v);
                        changed = true;
                    }
                    if let Some(v) = obj.remove("next_retry_at") {
                        obj.insert("next_retry_at_epoch_s".to_string(), v);
                        changed = true;
                    }
                }
                if changed {
                    updates.push((key.value().to_string(), serde_json::to_vec(&json)?));
                }
            }
            drop(table);
            drop(read_txn);

            if !updates.is_empty() {
                let write_txn = self.db.begin_write()?;
                {
                    let mut table = write_txn.open_table(MYCELIA)?;
                    for (key, data) in &updates {
                        table.insert(key.as_str(), data.as_slice())?;
                    }
                }
                write_txn.commit()?;
            }
        }

        // Migrate TLS_CERTS entries
        {
            let read_txn = self.db.begin_read()?;
            let table = read_txn.open_table(TLS_CERTS)?;
            let mut updates = Vec::new();
            for entry in table.iter()? {
                let (key, value) = entry?;
                let mut json: serde_json::Value = serde_json::from_slice(value.value())?;
                let mut changed = false;
                if let Some(obj) = json.as_object_mut() {
                    if let Some(v) = obj.remove("key_pem") {
                        obj.insert("key_pem_secret".to_string(), v);
                        changed = true;
                    }
                    if let Some(v) = obj.remove("expires_at") {
                        obj.insert("expires_at_epoch_s".to_string(), v);
                        changed = true;
                    }
                }
                if changed {
                    updates.push((key.value().to_string(), serde_json::to_vec(&json)?));
                }
            }
            drop(table);
            drop(read_txn);

            if !updates.is_empty() {
                let write_txn = self.db.begin_write()?;
                {
                    let mut table = write_txn.open_table(TLS_CERTS)?;
                    for (key, data) in &updates {
                        table.insert(key.as_str(), data.as_slice())?;
                    }
                }
                write_txn.commit()?;
            }
        }

        Ok(())
    }

    /// Unified BFS traversal for both lineage (inbound) and ancestors (outbound).
    fn bfs_traverse(
        &self,
        hash: &str,
        max_depth: usize,
        direction: BfsDirection,
    ) -> Result<LineageResult> {
        use std::collections::{HashMap, HashSet, VecDeque};

        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();
        let mut depth_tracker = HashMap::new();

        queue.push_back((hash.to_string(), 0usize));
        depth_tracker.insert(hash.to_string(), 0usize);

        let read_txn = self.db.begin_read()?;
        let hash_index = read_txn.open_table(HASH_INDEX)?;
        let spore_refs_table = read_txn.open_table(SPORE_REFS)?;

        // For inbound traversal, we also need the INBOUND table
        let inbound_table = if matches!(direction, BfsDirection::Inbound) {
            Some(read_txn.open_table(INBOUND)?)
        } else {
            None
        };

        while let Some((current, depth)) = queue.pop_front() {
            if visited.contains(&current) {
                continue;
            }
            visited.insert(current.clone());

            if max_depth > 0 && depth >= max_depth {
                continue;
            }

            match direction {
                BfsDirection::Inbound => {
                    // Inbound: who points to me (children from INBOUND table)
                    if let Some(ref table) = inbound_table
                        && let Some(children_value) = table.get(current.as_str())?
                    {
                        for child in children_value.value() {
                            let child_hash = child.to_string();
                            if !visited.contains(&child_hash) {
                                queue.push_back((child_hash.clone(), depth + 1));
                                depth_tracker.insert(child_hash, depth + 1);
                            }
                        }
                    }
                }
                BfsDirection::Outbound => {
                    // Outbound: who do I point to (refs from SPORE_REFS table)
                    if let Some(refs_data) = spore_refs_table.get(current.as_str())?
                        && let Ok(refs) =
                            serde_json::from_slice::<Vec<serde_json::Value>>(refs_data.value())
                    {
                        for ref_obj in &refs {
                            if let Some(ref_uri) = ref_obj.get("uri").and_then(|u| u.as_str())
                                && let Some((_, ref_hash)) = parse_cmn_uri(ref_uri)
                                && !visited.contains(&ref_hash)
                            {
                                queue.push_back((ref_hash.clone(), depth + 1));
                                depth_tracker.insert(ref_hash, depth + 1);
                            }
                        }
                    }
                }
            }
        }

        // Build result nodes
        let mut nodes = Vec::new();
        for visited_hash in &visited {
            if visited_hash == hash {
                continue;
            }
            if let Some(keys_value) = hash_index.get(visited_hash.as_str())? {
                let keys = keys_value.value();
                if let Some(key) = keys.first() {
                    let parts: Vec<&str> = key.split(':').collect();
                    let domain = if parts.len() >= 2 {
                        parts[0]
                    } else {
                        "unknown"
                    };
                    let uri = format!("cmn://{}/{}", domain, visited_hash);

                    let mut refs = Vec::new();
                    if let Some(refs_data) = spore_refs_table.get(visited_hash.as_str())?
                        && let Ok(raw_refs) =
                            serde_json::from_slice::<Vec<serde_json::Value>>(refs_data.value())
                    {
                        for ref_obj in &raw_refs {
                            let ref_uri = ref_obj.get("uri").and_then(|u| u.as_str());
                            let ref_type = ref_obj.get("relation").and_then(|t| t.as_str());
                            if let (Some(u), Some(r)) = (ref_uri, ref_type) {
                                refs.push(LineageRef {
                                    relation: r.to_string(),
                                    uri: u.to_string(),
                                });
                            }
                        }
                    }

                    nodes.push(LineageNode { uri, refs });
                }
            }
        }

        let max_depth_reached = max_depth > 0 && depth_tracker.values().any(|&d| d >= max_depth);

        Ok(LineageResult {
            nodes,
            max_depth_reached,
        })
    }
}

enum BfsDirection {
    Inbound,
    Outbound,
}

#[async_trait]
impl Storage for RedbStorage {
    async fn save_spore(
        &self,
        hash: &str,
        domain: &str,
        data: &[u8],
        bonds: &[SporeBond],
    ) -> Result<SaveSporeResult> {
        let write_txn = self.db.begin_write()?;
        let result;
        {
            // Use "domain:hash" as composite key
            let spore_key = format!("{}:{}", domain, hash);

            let mut spore_table = write_txn.open_table(SPORES)?;

            // Maintain hash index for content-addressable lookup
            let mut hash_index = write_txn.open_table(HASH_INDEX)?;
            let mut keys: Vec<String> = hash_index
                .get(hash)?
                .map(|v| v.value().iter().map(|s| s.to_string()).collect())
                .unwrap_or_default();

            if keys.contains(&spore_key) {
                // Same domain, same hash — duplicate
                result = SaveSporeResult::Duplicate;
            } else if keys.is_empty() {
                // Hash never seen — new content
                result = SaveSporeResult::Indexed;
            } else {
                // Hash exists from other domain(s) — replicate
                result = SaveSporeResult::Replicated;
            }

            spore_table.insert(spore_key.as_str(), data)?;

            if !keys.contains(&spore_key) {
                keys.push(spore_key.clone());
                let keys_ref: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
                hash_index.insert(hash, keys_ref)?;
            }

            let mut domain_table = write_txn.open_table(DOMAINS)?;
            let mut domain_hashes: Vec<String> = domain_table
                .get(domain)?
                .map(|v| v.value().iter().map(|s| s.to_string()).collect())
                .unwrap_or_default();

            if !domain_hashes.contains(&hash.to_string()) {
                domain_hashes.push(hash.to_string());
                let hashes_ref: Vec<&str> = domain_hashes.iter().map(|s| s.as_str()).collect();
                domain_table.insert(domain, hashes_ref)?;
            }

            // Store bonds with types in SPORE_REFS table
            let mut spore_refs_table = write_txn.open_table(SPORE_REFS)?;
            let refs_json: Vec<serde_json::Value> = bonds
                .iter()
                .map(|r| {
                    serde_json::json!({
                        "uri": r.uri,
                        "relation": r.relation
                    })
                })
                .collect();
            let refs_bytes = serde_json::to_vec(&refs_json)?;
            spore_refs_table.insert(hash, refs_bytes.as_slice())?;

            // Update INBOUND table for graph traversal
            let mut inbound_table = write_txn.open_table(INBOUND)?;
            for bond in bonds {
                // Parse URI to extract hash (domain not needed for INBOUND table)
                if let Some((_ref_domain, ref_hash)) = parse_cmn_uri(&bond.uri) {
                    let mut children: Vec<String> = inbound_table
                        .get(ref_hash.as_str())?
                        .map(|v| v.value().iter().map(|s| s.to_string()).collect())
                        .unwrap_or_default();

                    if !children.contains(&hash.to_string()) {
                        children.push(hash.to_string());
                        let children_ref: Vec<&str> = children.iter().map(|s| s.as_str()).collect();
                        inbound_table.insert(ref_hash.as_str(), children_ref)?;
                    }
                }
            }
        }
        write_txn.commit()?;
        Ok(result)
    }

    async fn get_spore(&self, hash: &str) -> Result<Option<Vec<u8>>> {
        let read_txn = self.db.begin_read()?;

        // Use hash index to find spore keys
        let hash_index = read_txn.open_table(HASH_INDEX)?;
        let keys = match hash_index.get(hash)? {
            Some(v) => v.value().iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            None => return Ok(None),
        };

        // Return the first matching spore (content-addressable)
        if let Some(key) = keys.first() {
            let spore_table = read_txn.open_table(SPORES)?;
            Ok(spore_table.get(key.as_str())?.map(|v| v.value().to_vec()))
        } else {
            Ok(None)
        }
    }

    async fn get_spore_on_host(&self, domain: &str, hash: &str) -> Result<Option<Vec<u8>>> {
        let transaction = self.db.begin_read()?;
        let table = transaction.open_table(SPORES)?;
        let key = format!("{domain}:{hash}");
        Ok(table.get(key.as_str())?.map(|value| value.value().to_vec()))
    }

    async fn save_mycelium(
        &self,
        domain: &str,
        data: &[u8],
        public_key: &str,
        hash: &str,
    ) -> Result<()> {
        // Extract updated_at from mycelium data
        let mycelium: serde_json::Value = serde_json::from_slice(data)?;
        let updated_at = mycelium
            .get("capsule")
            .and_then(|c| c.get("core"))
            .and_then(|m| m.get("updated_at_epoch_ms"))
            .and_then(|u| u.as_u64())
            .unwrap_or(0);

        let record = MyceliumRecord {
            data: data.to_vec(),
            public_key: public_key.to_string(),
            hash: hash.to_string(),
            updated_at_epoch_ms: updated_at,
            status: "synced".to_string(),
            retry_count: 0,
            last_error: None,
            next_retry_at_epoch_s: None,
        };
        let record_bytes = serde_json::to_vec(&record)?;

        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(MYCELIA)?;
            let old_record: Option<MyceliumRecord> = table
                .get(domain)?
                .map(|v| serde_json::from_slice(v.value()))
                .transpose()?;

            table.insert(domain, record_bytes.as_slice())?;

            // Maintain mycelium hash reverse index
            let mut hash_idx = write_txn.open_table(MYCELIA_HASH_INDEX)?;

            // Remove domain from old hash entries if old record had a different hash
            if let Some(old) = old_record
                && old.hash != hash
            {
                let mut old_hashes: Vec<&str> = old.hash.split(',').collect();
                if old_hashes.len() > 1 {
                    old_hashes.push(&old.hash);
                }
                for old_h in old_hashes {
                    let maybe_domains: Option<Vec<String>> = hash_idx
                        .get(old_h)?
                        .map(|v| v.value().iter().map(|s| s.to_string()).collect());
                    if let Some(mut domains) = maybe_domains
                        && let Some(pos) = domains.iter().position(|d| d == domain)
                    {
                        domains.remove(pos);
                        if domains.is_empty() {
                            hash_idx.remove(old_h)?;
                        } else {
                            let refs: Vec<&str> = domains.iter().map(|s| s.as_str()).collect();
                            hash_idx.insert(old_h, refs)?;
                        }
                    }
                }
            }

            // Index each shard hash individually (comma-separated) plus the full key
            let shard_hashes: Vec<&str> = hash.split(',').collect();
            for shard_hash in &shard_hashes {
                let mut domains: Vec<String> = hash_idx
                    .get(*shard_hash)?
                    .map(|v| v.value().iter().map(|s| s.to_string()).collect())
                    .unwrap_or_default();
                if !domains.contains(&domain.to_string()) {
                    domains.push(domain.to_string());
                    let refs: Vec<&str> = domains.iter().map(|s| s.as_str()).collect();
                    hash_idx.insert(*shard_hash, refs)?;
                }
            }
            // Also index the full composite key if multi-shard
            if shard_hashes.len() > 1 {
                let mut domains: Vec<String> = hash_idx
                    .get(hash)?
                    .map(|v| v.value().iter().map(|s| s.to_string()).collect())
                    .unwrap_or_default();
                if !domains.contains(&domain.to_string()) {
                    domains.push(domain.to_string());
                    let refs: Vec<&str> = domains.iter().map(|s| s.as_str()).collect();
                    hash_idx.insert(hash, refs)?;
                }
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    async fn mark_mycelium_failed(&self, domain: &str, error: &str) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(MYCELIA)?;

            // Get existing record or create new
            let mut record: MyceliumRecord = table
                .get(domain)?
                .map(|v| serde_json::from_slice(v.value()))
                .transpose()?
                .unwrap_or_else(|| MyceliumRecord {
                    data: vec![],
                    public_key: String::new(),
                    hash: String::new(),
                    updated_at_epoch_ms: 0,
                    status: "failed".to_string(),
                    retry_count: 0,
                    last_error: None,
                    next_retry_at_epoch_s: None,
                });

            record.status = "failed".to_string();
            record.retry_count += 1;
            record.last_error = Some(error.to_string());

            // Calculate next retry time (exponential backoff)
            let now = chrono::Utc::now();
            let next_retry = match record.retry_count {
                1 => now + chrono::Duration::minutes(1),
                2 => now + chrono::Duration::minutes(5),
                3 => now + chrono::Duration::minutes(15),
                4 => now + chrono::Duration::hours(1),
                5 => now + chrono::Duration::hours(6),
                _ => now + chrono::Duration::hours(24), // Permanent daily retry
            };
            record.next_retry_at_epoch_s = Some(next_retry.timestamp());

            let record_bytes = serde_json::to_vec(&record)?;
            table.insert(domain, record_bytes.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    async fn get_mycelium(&self, domain: &str) -> Result<Option<Vec<u8>>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(MYCELIA)?;

        match table.get(domain)? {
            Some(v) => {
                let record: MyceliumRecord = serde_json::from_slice(v.value())?;
                if !record.data.is_empty() {
                    Ok(Some(record.data))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    async fn get_domains_for_retry(&self, _max_retries: i32) -> Result<Vec<String>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(MYCELIA)?;

        let now = chrono::Utc::now().timestamp();
        let mut domains = Vec::new();
        let iter = table.iter()?;
        for entry in iter {
            let (key, value) = entry?;
            let record: MyceliumRecord = serde_json::from_slice(value.value())?;
            if record.status == "failed" {
                // Check if retry time has been reached
                if record.next_retry_at_epoch_s.is_none_or(|t| t <= now) {
                    domains.push(key.value().to_string());
                }
            }
        }
        Ok(domains)
    }

    async fn get_domain_spores(&self, domain: &str) -> Result<Vec<String>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(DOMAINS)?;
        Ok(table
            .get(domain)?
            .map(|v| {
                v.value()
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default())
    }

    async fn get_spore_hosts(&self, hash: &str) -> Result<Vec<String>> {
        // Get all domains that have published this spore hash
        // Use HASH_INDEX to find all domain:hash keys
        let read_txn = self.db.begin_read()?;
        let hash_index = read_txn.open_table(HASH_INDEX)?;

        let keys = match hash_index.get(hash)? {
            Some(v) => v.value().iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            None => return Ok(vec![]),
        };

        // Extract domains from "domain:hash" keys
        let mut domains = Vec::new();
        for key in keys {
            let parts: Vec<&str> = key.split(':').collect();
            if parts.len() >= 2 {
                domains.push(parts[0].to_string());
            }
        }

        Ok(domains)
    }

    async fn get_spore_ancestors(&self, hash: &str, max_depth: usize) -> Result<LineageResult> {
        self.bfs_traverse(hash, max_depth, BfsDirection::Outbound)
    }

    async fn get_spore_lineage(&self, hash: &str, max_depth: usize) -> Result<LineageResult> {
        self.bfs_traverse(hash, max_depth, BfsDirection::Inbound)
    }

    async fn save_cert(
        &self,
        domains: &[String],
        cert_pem: &str,
        key_pem: &str,
        expires_at: DateTime<Utc>,
        self_signed: bool,
    ) -> Result<()> {
        let mut sorted_domains = domains.to_vec();
        sorted_domains.sort();
        let domains_key = sorted_domains.join(",");

        let cert = SerializedCert {
            domains: domains_key.clone(),
            cert_pem: cert_pem.to_string(),
            key_pem_secret: key_pem.to_string(),
            expires_at_epoch_s: expires_at.timestamp(),
            self_signed,
        };

        let data = serde_json::to_vec(&cert)?;

        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(TLS_CERTS)?;
            table.insert(domains_key.as_str(), data.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    async fn get_cert(&self, domains: &[String]) -> Result<Option<TlsCert>> {
        let mut sorted_domains = domains.to_vec();
        sorted_domains.sort();
        let domains_key = sorted_domains.join(",");

        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(TLS_CERTS)?;

        match table.get(domains_key.as_str())? {
            Some(data) => {
                let cert: SerializedCert = serde_json::from_slice(data.value())?;
                Ok(Some(TlsCert {
                    cert_pem: cert.cert_pem,
                    key_pem_secret: cert.key_pem_secret,
                    expires_at: DateTime::from_timestamp(cert.expires_at_epoch_s, 0)
                        .unwrap_or_else(Utc::now),
                    self_signed: cert.self_signed,
                }))
            }
            None => Ok(None),
        }
    }

    async fn get_mycelium_hash(&self, domain: &str) -> Result<Option<String>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(MYCELIA)?;

        match table.get(domain)? {
            Some(data) => {
                let record: MyceliumRecord = serde_json::from_slice(data.value())?;
                if !record.data.is_empty() && !record.hash.is_empty() {
                    Ok(Some(record.hash))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    async fn get_all_myceliums(&self) -> Result<Vec<MyceliumRow>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(MYCELIA)?;

        let mut result = Vec::new();
        let iter = table.iter()?;
        for item in iter {
            let (domain, data) = item?;
            let record: MyceliumRecord = serde_json::from_slice(data.value())?;

            // Keep verified manifests visible while missing content is retried.
            if !record.data.is_empty() && !record.hash.is_empty() {
                result.push(MyceliumRow {
                    domain: domain.value().to_string(),
                    hash: record.hash,
                    updated_at_epoch_ms: record.updated_at_epoch_ms,
                });
            }
        }

        // Sort by updated_at_epoch_ms descending (newest first)
        result.sort_by_key(|row| std::cmp::Reverse(row.updated_at_epoch_ms));

        Ok(result)
    }

    async fn save_setting(&self, key: &str, value: &str) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(SETTINGS)?;
            table.insert(key, value)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    async fn get_setting(&self, key: &str) -> Result<Option<String>> {
        let read_txn = self.db.begin_read()?;
        let table = match read_txn.open_table(SETTINGS) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        match table.get(key)? {
            Some(v) => Ok(Some(v.value().to_string())),
            None => Ok(None),
        }
    }

    async fn save_nostr_event(&self, event_id: &str, event_json: &[u8]) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(NOSTR_EVENTS)?;
            table.insert(event_id, event_json)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    async fn get_nostr_events(&self) -> Result<Vec<Vec<u8>>> {
        let read_txn = self.db.begin_read()?;
        let table = match read_txn.open_table(NOSTR_EVENTS) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(vec![]),
            Err(e) => return Err(e.into()),
        };
        let mut events = Vec::new();
        for entry in table.iter()? {
            let (_, value) = entry?;
            events.push(value.value().to_vec());
        }
        Ok(events)
    }

    async fn get_nostr_event_records(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let transaction = self.db.begin_read()?;
        let table = match transaction.open_table(NOSTR_EVENTS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(vec![]),
            Err(error) => return Err(error.into()),
        };
        table
            .iter()?
            .map(|entry| {
                let (id, bytes) = entry?;
                Ok((id.value().to_string(), bytes.value().to_vec()))
            })
            .collect()
    }

    async fn mutate_nostr_events(
        &self,
        event: Option<(&str, &[u8])>,
        removed_ids: &[String],
    ) -> Result<()> {
        let transaction = self.db.begin_write()?;
        {
            let mut table = transaction.open_table(NOSTR_EVENTS)?;
            for id in removed_ids {
                table.remove(id.as_str())?;
            }
            if let Some((id, json)) = event {
                table.insert(id, json)?;
            }
        }
        transaction.commit()?;
        Ok(())
    }

    async fn trim_nostr_events(&self, max_events: usize) -> Result<()> {
        anyhow::ensure!(max_events > 0, "Nostr capacity must be greater than zero");
        let transaction = self.db.begin_write()?;
        {
            let mut table = transaction.open_table(NOSTR_EVENTS)?;
            let excess = table.len()?.saturating_sub(max_events as u64);
            let ids = table
                .iter()?
                .take(excess as usize)
                .map(|entry| entry.map(|(id, _)| id.value().to_string()))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            for id in ids {
                table.remove(id.as_str())?;
            }
        }
        transaction.commit()?;
        Ok(())
    }

    async fn mark_mycelium_synced(&self, domain: &str) -> Result<()> {
        let transaction = self.db.begin_write()?;
        {
            let mut table = transaction.open_table(MYCELIA)?;
            let bytes = table.get(domain)?.map(|value| value.value().to_vec());
            if let Some(bytes) = bytes {
                let mut record: MyceliumRecord = serde_json::from_slice(&bytes)?;
                record.status = "synced".to_string();
                record.retry_count = 0;
                record.last_error = None;
                record.next_retry_at_epoch_s = None;
                if record.data.is_empty() {
                    table.remove(domain)?;
                } else {
                    table.insert(domain, serde_json::to_vec(&record)?.as_slice())?;
                }
            }
        }
        transaction.commit()?;
        Ok(())
    }

    async fn save_taste(&self, target_uri: &str, domain: &str, data: &[u8]) -> Result<()> {
        let target_uri = substrate::normalize_taste_target_uri(target_uri)?;
        let key = format!("{}|{}", target_uri, domain);

        // Extract spore hash from target_uri for the spore taste index
        let spore_hash = substrate::parse_uri(&target_uri)
            .ok()
            .filter(|p| p.kind == substrate::CmnUriKind::Spore)
            .and_then(|p| p.hash);

        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(TASTE_REPORTS)?;
            table.insert(key.as_str(), data)?;

            // Maintain spore taste index
            if let Some(ref hash) = spore_hash {
                let normalized_hash = substrate::parse_hash(hash)
                    .map(|h| substrate::format_hash(h.algorithm, &h.bytes))
                    .unwrap_or_else(|_| hash.clone());
                let mut idx = write_txn.open_table(TASTE_SPORE_INDEX)?;
                let mut keys: Vec<String> = idx
                    .get(normalized_hash.as_str())?
                    .map(|v| v.value().iter().map(|s| s.to_string()).collect())
                    .unwrap_or_default();
                if !keys.contains(&key) {
                    keys.push(key.clone());
                    let refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
                    idx.insert(normalized_hash.as_str(), refs)?;
                }
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    async fn get_tastes(&self, target_uri: &str) -> Result<Vec<Vec<u8>>> {
        let target_uri = substrate::normalize_taste_target_uri(target_uri)?;
        let read_txn = self.db.begin_read()?;
        let table = match read_txn.open_table(TASTE_REPORTS) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(vec![]),
            Err(e) => return Err(e.into()),
        };

        // Use range query instead of full table scan.
        // Keys are "{target_uri}|{domain}"; scan from "uri|" to "uri}" ('|'+1 = '}')
        let range_start = format!("{}|", target_uri);
        let range_end = format!("{}}}", target_uri);
        let mut tastes = Vec::new();
        for entry in table.range(range_start.as_str()..range_end.as_str())? {
            let (_key, value) = entry?;
            tastes.push(value.value().to_vec());
        }
        Ok(tastes)
    }

    async fn get_spore_tastes(&self, spore_hash: &str) -> Result<Vec<Vec<u8>>> {
        let spore_hash = substrate::parse_hash(spore_hash)
            .map(|hash| substrate::format_hash(hash.algorithm, &hash.bytes))?;
        let read_txn = self.db.begin_read()?;

        // Try the spore taste index first (O(log N) lookup)
        if let Ok(idx) = read_txn.open_table(TASTE_SPORE_INDEX)
            && let Some(keys_value) = idx.get(spore_hash.as_str())?
        {
            let keys: Vec<String> = keys_value.value().iter().map(|s| s.to_string()).collect();
            if !keys.is_empty() {
                let table = match read_txn.open_table(TASTE_REPORTS) {
                    Ok(t) => t,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(vec![]),
                    Err(e) => return Err(e.into()),
                };
                let mut tastes = Vec::new();
                for key in &keys {
                    if let Some(value) = table.get(key.as_str())? {
                        tastes.push(value.value().to_vec());
                    }
                }
                return Ok(tastes);
            }
        }

        // Fall back to full scan for data written before the index was added
        let table = match read_txn.open_table(TASTE_REPORTS) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(vec![]),
            Err(e) => return Err(e.into()),
        };

        let mut tastes = Vec::new();
        for entry in table.iter()? {
            let (key, value) = entry?;
            let Some((target_uri, _)) = key.value().split_once('|') else {
                continue;
            };
            let Ok(parsed) = substrate::parse_uri(target_uri) else {
                continue;
            };
            if parsed.kind != substrate::CmnUriKind::Spore {
                continue;
            }
            if parsed.hash.as_deref() == Some(spore_hash.as_str()) {
                tastes.push(value.value().to_vec());
            }
        }
        Ok(tastes)
    }

    async fn get_domain_public_key(&self, domain: &str) -> Result<Option<String>> {
        let read_txn = self.db.begin_read()?;
        let table = match read_txn.open_table(MYCELIA) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        match table.get(domain)? {
            Some(data) => {
                let record: MyceliumRecord = serde_json::from_slice(data.value())?;
                if record.status == "synced" && !record.public_key.is_empty() {
                    Ok(Some(record.public_key))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    async fn get_taste_by_hash(&self, hash: &str) -> Result<Option<Vec<u8>>> {
        let read_txn = self.db.begin_read()?;
        let table = match read_txn.open_table(TASTE_BY_HASH) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        Ok(table.get(hash)?.map(|v| v.value().to_vec()))
    }

    async fn save_taste_index(&self, taste_hash: &str, data: &[u8]) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(TASTE_BY_HASH)?;
            table.insert(taste_hash, data)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    async fn get_mycelium_by_hash(&self, hash: &str) -> Result<Vec<MyceliumRow>> {
        let read_txn = self.db.begin_read()?;

        // Try the reverse index first
        if let Ok(idx) = read_txn.open_table(MYCELIA_HASH_INDEX)
            && let Some(domains_value) = idx.get(hash)?
        {
            let domains: Vec<String> = domains_value
                .value()
                .iter()
                .map(|s| s.to_string())
                .collect();
            if !domains.is_empty() {
                let mycelia_table = read_txn.open_table(MYCELIA)?;
                let mut result = Vec::new();
                for domain in &domains {
                    if let Some(data) = mycelia_table.get(domain.as_str())? {
                        let record: MyceliumRecord = serde_json::from_slice(data.value())?;
                        if record.status == "synced"
                            && (record.hash == hash || record.hash.split(',').any(|h| h == hash))
                        {
                            result.push(MyceliumRow {
                                domain: domain.clone(),
                                hash: record.hash,
                                updated_at_epoch_ms: record.updated_at_epoch_ms,
                            });
                        }
                    }
                }
                return Ok(result);
            }
        }

        // Fall back to full scan for data written before the index
        let all = self.get_all_myceliums().await?;
        Ok(all
            .into_iter()
            .filter(|row| row.hash == hash || row.hash.split(',').any(|h| h == hash))
            .collect())
    }
}
