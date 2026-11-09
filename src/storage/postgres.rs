//! PostgreSQL storage implementation.

use super::{
    LineageNode, LineageRef, LineageResult, MyceliumRow, SaveSporeResult, SporeBond, Storage,
    TlsCert,
};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::FromRow;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::collections::HashMap;

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

#[derive(Clone)]
pub struct PostgresStorage {
    pool: PgPool,
}

#[derive(Debug, FromRow)]
struct DbTlsCert {
    #[allow(dead_code)]
    domains: String, // needed by FromRow mapping from SELECT
    cert_pem: String,
    key_pem_secret: String,
    expires_at: DateTime<Utc>,
    self_signed: bool,
}

impl PostgresStorage {
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) async fn retry_record(
        &self,
        domain: &str,
    ) -> Result<Option<(String, i32, bool, bool)>> {
        Ok(sqlx::query_as("SELECT status, retry_count, last_error IS NOT NULL, next_retry_at IS NOT NULL FROM mycelia WHERE domain = $1")
            .bind(domain).fetch_optional(&self.pool).await?)
    }

    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await?;

        let storage = Self { pool };
        storage.init_schema().await?;
        storage.run_migrations().await?;
        Ok(storage)
    }

    async fn init_schema(&self) -> Result<()> {
        // Spores table - using JSONB for efficient JSON storage and querying
        // Primary key: (domain, hash) - a spore belongs to a specific domain
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS spores (
                domain TEXT NOT NULL,
                hash TEXT NOT NULL,
                data JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (domain, hash)
            )"#,
        )
        .execute(&self.pool)
        .await?;

        // Index on hash for content-addressable lookup across domains
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_spores_hash ON spores(hash)")
            .execute(&self.pool)
            .await?;

        // GIN index for JSONB queries (e.g., search by spore.name)
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_spores_data ON spores USING GIN (data)")
            .execute(&self.pool)
            .await?;

        // Mycelia table - using JSONB
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS mycelia (
                domain TEXT PRIMARY KEY,
                data JSONB,
                public_key TEXT,
                hash TEXT,
                updated_at_epoch_ms BIGINT NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending',
                retry_count INT NOT NULL DEFAULT 0,
                last_error TEXT,
                last_attempt_at TIMESTAMPTZ,
                next_retry_at TIMESTAMPTZ,
                synced_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )"#,
        )
        .execute(&self.pool)
        .await?;

        // Migration: add hash and updated_at_epoch_ms columns if missing
        sqlx::query("ALTER TABLE mycelia ADD COLUMN IF NOT EXISTS hash TEXT")
            .execute(&self.pool)
            .await?;

        // Index for finding domains that need retry (by next_retry_at)
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_mycelia_retry ON mycelia(status, next_retry_at)",
        )
        .execute(&self.pool)
        .await?;

        // GIN index for JSONB queries
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_mycelia_data ON mycelia USING GIN (data)")
            .execute(&self.pool)
            .await?;

        // Spore references (for lineage tracking)
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS spore_references (
                spore_hash TEXT NOT NULL,
                reference_domain TEXT NOT NULL,
                reference_hash TEXT NOT NULL,
                reference_type TEXT NOT NULL,
                PRIMARY KEY (spore_hash, reference_domain, reference_hash)
            )"#,
        )
        .execute(&self.pool)
        .await?;

        // Index on reference_hash for fast lineage lookup (inbound references)
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_spore_references_hash ON spore_references(reference_hash)",
        )
        .execute(&self.pool)
        .await?;

        // Settings table (key-value store for app settings like Nostr keys)
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )"#,
        )
        .execute(&self.pool)
        .await?;

        // TLS certificates table
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS tls_certs (
                domains TEXT PRIMARY KEY,
                cert_pem TEXT NOT NULL,
                key_pem_secret TEXT NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                self_signed BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )"#,
        )
        .execute(&self.pool)
        .await?;

        // Nostr events table (for persistent relay storage)
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS nostr_events (
                event_id TEXT PRIMARY KEY,
                event_json JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )"#,
        )
        .execute(&self.pool)
        .await?;

        // Taste reports table
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS taste_reports (
                target_uri TEXT NOT NULL,
                target_kind TEXT NOT NULL,
                target_hash TEXT,
                domain TEXT NOT NULL,
                data JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (target_uri, domain)
            )"#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_taste_reports_kind_hash ON taste_reports(target_kind, target_hash)",
        )
        .execute(&self.pool)
        .await?;

        // Taste by hash index table
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS taste_by_hash (
                taste_hash TEXT PRIMARY KEY,
                data JSONB NOT NULL
            )"#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // --- Schema migration system ---

    async fn get_schema_version(&self) -> Result<u32> {
        let result: Option<(String,)> =
            sqlx::query_as("SELECT value FROM settings WHERE key = 'schema_version'")
                .fetch_optional(&self.pool)
                .await?;
        Ok(result.and_then(|(v,)| v.parse().ok()).unwrap_or(0))
    }

    async fn set_schema_version(&self, version: u32) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO settings (key, value, updated_at)
               VALUES ('schema_version', $1, NOW())
               ON CONFLICT (key) DO UPDATE SET
                   value = EXCLUDED.value,
                   updated_at = NOW()"#,
        )
        .bind(version.to_string())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn run_migrations(&self) -> Result<()> {
        let version = self.get_schema_version().await?;

        if version < 1 {
            tracing::info!("Running PostgreSQL migration v1: Agent-First Data column renames");
            self.migrate_v1_afd_renames().await?;
            self.set_schema_version(1).await?;
        }

        if version < 2 {
            tracing::info!("Running PostgreSQL migration v2: taste target_uri schema");
            self.migrate_v2_taste_target_uri().await?;
            self.set_schema_version(2).await?;
        }

        Ok(())
    }

    /// v1: Rename columns to Agent-First Data convention.
    /// - mycelia.updated_at → updated_at_epoch_ms
    /// - tls_certs.key_pem → key_pem_secret
    async fn migrate_v1_afd_renames(&self) -> Result<()> {
        // Rename mycelia.updated_at → updated_at_epoch_ms (if old column exists)
        let has_old_updated_at: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'mycelia' AND column_name = 'updated_at')",
        )
        .fetch_one(&self.pool)
        .await?;

        if has_old_updated_at {
            sqlx::query("ALTER TABLE mycelia RENAME COLUMN updated_at TO updated_at_epoch_ms")
                .execute(&self.pool)
                .await?;
        }

        // Rename tls_certs.key_pem → key_pem_secret (if old column exists)
        let has_old_key_pem: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'tls_certs' AND column_name = 'key_pem')",
        )
        .fetch_one(&self.pool)
        .await?;

        if has_old_key_pem {
            sqlx::query("ALTER TABLE tls_certs RENAME COLUMN key_pem TO key_pem_secret")
                .execute(&self.pool)
                .await?;
        }

        Ok(())
    }

    /// v2: Switch taste_reports from spore_hash keying to target_uri keying.
    ///
    /// This migration is intentionally destructive for old taste rows because
    /// old rows only keyed by spore hash cannot be reliably mapped to full URI.
    async fn migrate_v2_taste_target_uri(&self) -> Result<()> {
        let has_target_uri: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'taste_reports' AND column_name = 'target_uri')",
        )
        .fetch_one(&self.pool)
        .await?;

        if has_target_uri {
            return Ok(());
        }

        sqlx::query("DROP TABLE IF EXISTS taste_reports")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS taste_reports (
                target_uri TEXT NOT NULL,
                target_kind TEXT NOT NULL,
                target_hash TEXT,
                domain TEXT NOT NULL,
                data JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (target_uri, domain)
            )"#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_taste_reports_kind_hash ON taste_reports(target_kind, target_hash)",
        )
        .execute(&self.pool)
        .await?;

        // Taste by hash index table
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS taste_by_hash (
                taste_hash TEXT PRIMARY KEY,
                data JSONB NOT NULL
            )"#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Build LineageResult from CTE results, batch-fetching all references in one query
    /// instead of N+1 per-node queries.
    async fn build_lineage_nodes(
        &self,
        results: &[(String, String, i32)],
        max_depth: usize,
        depth_limit: i32,
    ) -> Result<LineageResult> {
        let max_depth_reached = if max_depth > 0 && !results.is_empty() {
            let max_result_depth = results.iter().map(|(_, _, d)| *d).max().unwrap_or(0);
            max_result_depth >= depth_limit
        } else {
            false
        };

        if results.is_empty() {
            return Ok(LineageResult {
                nodes: Vec::new(),
                max_depth_reached: false,
            });
        }

        // Batch-fetch all references for all hashes in one query
        let hashes: Vec<&str> = results.iter().map(|(_, h, _)| h.as_str()).collect();
        let all_refs: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT spore_hash, reference_domain, reference_hash, reference_type FROM spore_references WHERE spore_hash = ANY($1)",
        )
        .bind(&hashes)
        .fetch_all(&self.pool)
        .await?;

        // Group references by spore_hash
        let mut refs_by_hash: HashMap<String, Vec<(String, String, String)>> = HashMap::new();
        for (spore_hash, ref_domain, ref_hash, ref_type) in all_refs {
            refs_by_hash
                .entry(spore_hash)
                .or_default()
                .push((ref_domain, ref_hash, ref_type));
        }

        let mut nodes = Vec::new();
        for (domain, spore_hash, _) in results {
            let uri = format!("cmn://{}/{}", domain, spore_hash);
            let refs = if let Some(raw_refs) = refs_by_hash.get(spore_hash) {
                raw_refs
                    .iter()
                    .map(|(ref_domain, ref_hash, ref_type)| LineageRef {
                        relation: ref_type.clone(),
                        uri: format!("cmn://{}/{}", ref_domain, ref_hash),
                    })
                    .collect()
            } else {
                Vec::new()
            };
            nodes.push(LineageNode { uri, refs });
        }

        Ok(LineageResult {
            nodes,
            max_depth_reached,
        })
    }
}

#[async_trait]
impl Storage for PostgresStorage {
    async fn save_spore(
        &self,
        hash: &str,
        domain: &str,
        data: &[u8],
        bonds: &[SporeBond],
    ) -> Result<SaveSporeResult> {
        // Parse data as JSON for JSONB storage
        let json_data: JsonValue = serde_json::from_slice(data)?;

        // Use a transaction to ensure atomicity of spore + references
        let mut tx = self.pool.begin().await?;

        // Check existing state to determine action
        let existing: Option<(String,)> =
            sqlx::query_as("SELECT domain FROM spores WHERE hash = $1 LIMIT 1")
                .bind(hash)
                .fetch_optional(&mut *tx)
                .await?;

        let has_this_domain: bool = if existing.is_some() {
            sqlx::query_as::<_, (i64,)>(
                "SELECT COUNT(*) FROM spores WHERE hash = $1 AND domain = $2",
            )
            .bind(hash)
            .bind(domain)
            .fetch_one(&mut *tx)
            .await
            .map(|(c,)| c > 0)
            .unwrap_or(false)
        } else {
            false
        };

        let result = match (existing.is_some(), has_this_domain) {
            (false, _) => SaveSporeResult::Indexed,
            (true, true) => SaveSporeResult::Duplicate,
            (true, false) => SaveSporeResult::Replicated,
        };

        // Insert or update spore
        sqlx::query(
            r#"INSERT INTO spores (domain, hash, data)
               VALUES ($1, $2, $3)
               ON CONFLICT (domain, hash) DO UPDATE SET
                   data = EXCLUDED.data,
                   created_at = NOW()"#,
        )
        .bind(domain)
        .bind(hash)
        .bind(&json_data)
        .execute(&mut *tx)
        .await?;

        // Insert bonds with type
        for bond in bonds {
            // Parse URI to extract domain and hash
            if let Some((ref_domain, ref_hash)) = parse_cmn_uri(&bond.uri) {
                sqlx::query(
                    r#"INSERT INTO spore_references (spore_hash, reference_domain, reference_hash, reference_type)
                       VALUES ($1, $2, $3, $4)
                       ON CONFLICT (spore_hash, reference_domain, reference_hash)
                       DO UPDATE SET reference_type = EXCLUDED.reference_type"#,
                )
                .bind(hash)
                .bind(&ref_domain)
                .bind(&ref_hash)
                .bind(bond.relation.as_str())
                .execute(&mut *tx)
                .await?;
            }
        }

        tx.commit().await?;

        Ok(result)
    }

    async fn get_spore(&self, hash: &str) -> Result<Option<Vec<u8>>> {
        // Content-addressable lookup: find any spore with this hash
        // If multiple domains have the same hash, returns the first one (arbitrary)
        let result: Option<(JsonValue,)> =
            sqlx::query_as("SELECT data FROM spores WHERE hash = $1 LIMIT 1")
                .bind(hash)
                .fetch_optional(&self.pool)
                .await?;

        match result {
            Some((json_data,)) => Ok(Some(serde_json::to_vec(&json_data)?)),
            None => Ok(None),
        }
    }

    async fn get_spore_on_host(&self, domain: &str, hash: &str) -> Result<Option<Vec<u8>>> {
        let row: Option<(JsonValue,)> =
            sqlx::query_as("SELECT data FROM spores WHERE domain = $1 AND hash = $2")
                .bind(domain)
                .bind(hash)
                .fetch_optional(&self.pool)
                .await?;
        row.map(|(value,)| serde_json::to_vec(&value).map_err(Into::into))
            .transpose()
    }

    async fn save_mycelium(
        &self,
        domain: &str,
        data: &[u8],
        public_key: &str,
        hash: &str,
    ) -> Result<()> {
        // Parse data as JSON for JSONB storage
        let json_data: JsonValue = serde_json::from_slice(data)?;

        // Extract updated_at_epoch_ms from capsule.core.updated_at_epoch_ms
        let updated_at = json_data
            .get("capsule")
            .and_then(|c| c.get("core"))
            .and_then(|c| c.get("updated_at_epoch_ms"))
            .and_then(|u| u.as_u64())
            .unwrap_or(0) as i64;

        sqlx::query(
            r#"INSERT INTO mycelia (domain, data, public_key, hash, updated_at_epoch_ms, status, synced_at, last_attempt_at, retry_count)
               VALUES ($1, $2, $3, $4, $5, 'synced', NOW(), NOW(), 0)
               ON CONFLICT (domain) DO UPDATE SET
                   data = EXCLUDED.data,
                   public_key = EXCLUDED.public_key,
                   hash = EXCLUDED.hash,
                   updated_at_epoch_ms = EXCLUDED.updated_at_epoch_ms,
                   status = 'synced',
                   synced_at = NOW(),
                   last_attempt_at = NOW(),
                   last_error = NULL,
                   retry_count = 0"#,
        )
        .bind(domain)
        .bind(&json_data)
        .bind(public_key)
        .bind(hash)
        .bind(updated_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn mark_mycelium_failed(&self, domain: &str, error: &str) -> Result<()> {
        // Calculate next retry time (exponential backoff)
        // retry_count=0: 1 minute later
        // retry_count=1: 5 minutes later
        // retry_count=2: 15 minutes later
        // retry_count=3: 1 hour later
        // retry_count=4: 6 hours later
        // retry_count>=5: 24 hours later (permanent daily retry)
        sqlx::query(
            r#"INSERT INTO mycelia (domain, status, last_error, last_attempt_at, retry_count, next_retry_at)
               VALUES ($1, 'failed', $2, NOW(), 1,
                       NOW() + INTERVAL '1 minute')
               ON CONFLICT (domain) DO UPDATE SET
                   status = 'failed',
                   last_error = EXCLUDED.last_error,
                   last_attempt_at = NOW(),
                   retry_count = mycelia.retry_count + 1,
                   next_retry_at = CASE
                       WHEN mycelia.retry_count = 0 THEN NOW() + INTERVAL '1 minute'
                       WHEN mycelia.retry_count = 1 THEN NOW() + INTERVAL '5 minutes'
                       WHEN mycelia.retry_count = 2 THEN NOW() + INTERVAL '15 minutes'
                       WHEN mycelia.retry_count = 3 THEN NOW() + INTERVAL '1 hour'
                       WHEN mycelia.retry_count = 4 THEN NOW() + INTERVAL '6 hours'
                       ELSE NOW() + INTERVAL '24 hours'
                   END"#,
        )
        .bind(domain)
        .bind(error)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_mycelium(&self, domain: &str) -> Result<Option<Vec<u8>>> {
        let result: Option<(Option<JsonValue>,)> =
            sqlx::query_as("SELECT data FROM mycelia WHERE domain = $1 AND data IS NOT NULL")
                .bind(domain)
                .fetch_optional(&self.pool)
                .await?;

        match result {
            Some((Some(json_data),)) => Ok(Some(serde_json::to_vec(&json_data)?)),
            _ => Ok(None),
        }
    }

    async fn get_domains_for_retry(&self, _max_retries: i32) -> Result<Vec<String>> {
        // Get all failed domains that have reached next_retry_at
        // No retry limit - permanent retry (max once per day)
        let results: Vec<(String,)> = sqlx::query_as(
            r#"SELECT domain FROM mycelia
               WHERE status = 'failed'
               AND (next_retry_at IS NULL OR next_retry_at <= NOW())
               ORDER BY next_retry_at NULLS FIRST, last_attempt_at"#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(results.into_iter().map(|(d,)| d).collect())
    }

    async fn get_domain_spores(&self, domain: &str) -> Result<Vec<String>> {
        let results: Vec<(String,)> = sqlx::query_as("SELECT hash FROM spores WHERE domain = $1")
            .bind(domain)
            .fetch_all(&self.pool)
            .await?;

        Ok(results.into_iter().map(|(hash,)| hash).collect())
    }

    async fn get_spore_hosts(&self, hash: &str) -> Result<Vec<String>> {
        // Get all domains that have published this spore hash
        let results: Vec<(String,)> = sqlx::query_as("SELECT domain FROM spores WHERE hash = $1")
            .bind(hash)
            .fetch_all(&self.pool)
            .await?;

        Ok(results.into_iter().map(|(domain,)| domain).collect())
    }

    async fn get_spore_ancestors(&self, hash: &str, max_depth: usize) -> Result<LineageResult> {
        // Get all spores that this one references (ancestors)
        // Uses recursive CTE to traverse backwards through references
        let depth_limit = if max_depth > 0 {
            max_depth as i32
        } else {
            1000 // Reasonable upper bound
        };

        // Get ancestor spores with their relationships from spore_references table
        let results: Vec<(String, String, i32)> = sqlx::query_as(
            r#"
            WITH RECURSIVE ancestors AS (
                -- Base case: find direct parents (spores that this one references)
                SELECT reference_domain, reference_hash, 1 as depth
                FROM spore_references
                WHERE spore_hash = $1

                UNION

                -- Recursive case: find grandparents
                SELECT r.reference_domain, r.reference_hash, a.depth + 1
                FROM spore_references r
                INNER JOIN ancestors a ON r.spore_hash = a.reference_hash
                WHERE a.depth < $2
            )
            SELECT reference_domain, reference_hash, MIN(depth) as depth
            FROM ancestors
            GROUP BY reference_domain, reference_hash
            ORDER BY depth, reference_domain, reference_hash
            "#,
        )
        .bind(hash)
        .bind(depth_limit)
        .fetch_all(&self.pool)
        .await?;

        self.build_lineage_nodes(&results, max_depth, depth_limit)
            .await
    }

    async fn get_spore_lineage(&self, hash: &str, max_depth: usize) -> Result<LineageResult> {
        // Use PostgreSQL recursive CTE for efficient lineage tracking
        // This queries all descendants in a single database roundtrip
        let depth_limit = if max_depth > 0 {
            max_depth as i32
        } else {
            1000 // Reasonable upper bound to prevent infinite loops
        };

        let results: Vec<(String, String, i32)> = sqlx::query_as(
            r#"
            WITH RECURSIVE lineage AS (
                -- Base case: find spores that directly reference this hash
                SELECT spore_hash, 1 as depth
                FROM spore_references
                WHERE reference_hash = $1

                UNION

                -- Recursive case: find spores that reference the children
                SELECT r.spore_hash, l.depth + 1
                FROM spore_references r
                INNER JOIN lineage l ON r.reference_hash = l.spore_hash
                WHERE l.depth < $2
            )
            SELECT s.domain, l.spore_hash, MIN(l.depth) as depth
            FROM lineage l
            INNER JOIN spores s ON s.hash = l.spore_hash
            GROUP BY s.domain, l.spore_hash
            ORDER BY depth, s.domain, l.spore_hash
            "#,
        )
        .bind(hash)
        .bind(depth_limit)
        .fetch_all(&self.pool)
        .await?;

        self.build_lineage_nodes(&results, max_depth, depth_limit)
            .await
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

        sqlx::query(
            r#"INSERT INTO tls_certs (domains, cert_pem, key_pem_secret, expires_at, self_signed)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (domains) DO UPDATE SET
                   cert_pem = EXCLUDED.cert_pem,
                   key_pem_secret = EXCLUDED.key_pem_secret,
                   expires_at = EXCLUDED.expires_at,
                   self_signed = EXCLUDED.self_signed,
                   updated_at = NOW()"#,
        )
        .bind(&domains_key)
        .bind(cert_pem)
        .bind(key_pem)
        .bind(expires_at)
        .bind(self_signed)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_cert(&self, domains: &[String]) -> Result<Option<TlsCert>> {
        let mut sorted_domains = domains.to_vec();
        sorted_domains.sort();
        let domains_key = sorted_domains.join(",");

        let result: Option<DbTlsCert> =
            sqlx::query_as("SELECT domains, cert_pem, key_pem_secret, expires_at, self_signed FROM tls_certs WHERE domains = $1")
                .bind(&domains_key)
                .fetch_optional(&self.pool)
                .await?;

        Ok(result.map(|r| TlsCert {
            cert_pem: r.cert_pem,
            key_pem_secret: r.key_pem_secret,
            expires_at: r.expires_at,
            self_signed: r.self_signed,
        }))
    }

    async fn get_mycelium_hash(&self, domain: &str) -> Result<Option<String>> {
        let result: Option<(Option<String>,)> =
            sqlx::query_as("SELECT hash FROM mycelia WHERE domain = $1 AND data IS NOT NULL")
                .bind(domain)
                .fetch_optional(&self.pool)
                .await?;

        Ok(result.and_then(|(hash,)| hash))
    }

    async fn get_all_myceliums(&self) -> Result<Vec<MyceliumRow>> {
        #[derive(FromRow)]
        struct DbMyceliumRow {
            domain: String,
            hash: String,
            updated_at_epoch_ms: i64,
        }

        let rows: Vec<DbMyceliumRow> = sqlx::query_as(
            "SELECT domain, hash, updated_at_epoch_ms FROM mycelia WHERE data IS NOT NULL ORDER BY updated_at_epoch_ms DESC"
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| MyceliumRow {
                domain: r.domain,
                hash: r.hash,
                updated_at_epoch_ms: r.updated_at_epoch_ms as u64,
            })
            .collect())
    }

    async fn save_setting(&self, key: &str, value: &str) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO settings (key, value, updated_at)
               VALUES ($1, $2, NOW())
               ON CONFLICT (key) DO UPDATE SET
                   value = EXCLUDED.value,
                   updated_at = NOW()"#,
        )
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_setting(&self, key: &str) -> Result<Option<String>> {
        let result: Option<(String,)> = sqlx::query_as("SELECT value FROM settings WHERE key = $1")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
        Ok(result.map(|(v,)| v))
    }

    async fn save_nostr_event(&self, event_id: &str, event_json: &[u8]) -> Result<()> {
        let json_value: JsonValue = serde_json::from_slice(event_json)?;
        sqlx::query(
            r#"INSERT INTO nostr_events (event_id, event_json)
               VALUES ($1, $2)
               ON CONFLICT (event_id) DO NOTHING"#,
        )
        .bind(event_id)
        .bind(json_value)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_nostr_events(&self) -> Result<Vec<Vec<u8>>> {
        let rows: Vec<(JsonValue,)> =
            sqlx::query_as("SELECT event_json FROM nostr_events ORDER BY created_at, event_id")
                .fetch_all(&self.pool)
                .await?;
        let mut events = Vec::new();
        for (json_value,) in rows {
            events.push(serde_json::to_vec(&json_value)?);
        }
        Ok(events)
    }

    async fn get_nostr_event_records(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let rows: Vec<(String, JsonValue)> = sqlx::query_as(
            "SELECT event_id, event_json FROM nostr_events ORDER BY created_at, event_id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(id, value)| Ok((id, serde_json::to_vec(&value)?)))
            .collect()
    }

    async fn mutate_nostr_events(
        &self,
        event: Option<(&str, &[u8])>,
        removed_ids: &[String],
    ) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("DELETE FROM nostr_events WHERE event_id = ANY($1)")
            .bind(removed_ids)
            .execute(&mut *transaction)
            .await?;
        if let Some((id, json)) = event {
            let value: JsonValue = serde_json::from_slice(json)?;
            sqlx::query("INSERT INTO nostr_events (event_id, event_json) VALUES ($1, $2) ON CONFLICT (event_id) DO NOTHING")
                .bind(id).bind(value).execute(&mut *transaction).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn trim_nostr_events(&self, max_events: usize) -> Result<()> {
        anyhow::ensure!(max_events > 0, "Nostr capacity must be greater than zero");
        sqlx::query("DELETE FROM nostr_events WHERE event_id IN (SELECT event_id FROM nostr_events ORDER BY created_at DESC, event_id DESC OFFSET $1)")
            .bind(i64::try_from(max_events)?).execute(&self.pool).await?;
        Ok(())
    }

    async fn mark_mycelium_synced(&self, domain: &str) -> Result<()> {
        sqlx::query("UPDATE mycelia SET status = 'synced', retry_count = 0, last_error = NULL, next_retry_at = NULL WHERE domain = $1 AND data IS NOT NULL")
            .bind(domain).execute(&self.pool).await?;
        sqlx::query("DELETE FROM mycelia WHERE domain = $1 AND data IS NULL")
            .bind(domain)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn save_taste(&self, target_uri: &str, domain: &str, data: &[u8]) -> Result<()> {
        let target_uri = substrate::normalize_taste_target_uri(target_uri)?;
        let parsed = substrate::parse_uri(&target_uri)?;
        let (target_kind, target_hash) = match parsed.kind {
            substrate::CmnUriKind::Domain => ("domain", None),
            substrate::CmnUriKind::Spore => ("spore", parsed.hash),
            substrate::CmnUriKind::Mycelium => ("mycelium", parsed.hash),
            substrate::CmnUriKind::Taste => {
                return Err(anyhow::anyhow!("Taste URI cannot be used as taste target"));
            }
        };

        let json_data: JsonValue = serde_json::from_slice(data)?;
        sqlx::query(
            r#"INSERT INTO taste_reports (target_uri, target_kind, target_hash, domain, data)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (target_uri, domain) DO UPDATE SET
                   data = EXCLUDED.data,
                   created_at = NOW()"#,
        )
        .bind(&target_uri)
        .bind(target_kind)
        .bind(target_hash)
        .bind(domain)
        .bind(&json_data)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_tastes(&self, target_uri: &str) -> Result<Vec<Vec<u8>>> {
        let target_uri = substrate::normalize_taste_target_uri(target_uri)?;
        let rows: Vec<(JsonValue,)> = sqlx::query_as(
            "SELECT data FROM taste_reports WHERE target_uri = $1 ORDER BY created_at",
        )
        .bind(target_uri)
        .fetch_all(&self.pool)
        .await?;
        let mut tastes = Vec::new();
        for (json_value,) in rows {
            tastes.push(serde_json::to_vec(&json_value)?);
        }
        Ok(tastes)
    }

    async fn get_spore_tastes(&self, spore_hash: &str) -> Result<Vec<Vec<u8>>> {
        let spore_hash = substrate::parse_hash(spore_hash)
            .map(|hash| substrate::format_hash(hash.algorithm, &hash.bytes))?;
        let rows: Vec<(JsonValue,)> = sqlx::query_as(
            "SELECT data FROM taste_reports WHERE target_kind = 'spore' AND target_hash = $1 ORDER BY created_at",
        )
        .bind(spore_hash)
        .fetch_all(&self.pool)
        .await?;
        let mut tastes = Vec::new();
        for (json_value,) in rows {
            tastes.push(serde_json::to_vec(&json_value)?);
        }
        Ok(tastes)
    }

    async fn get_spores_batch(&self, hashes: &[&str]) -> Result<Vec<(String, Option<Vec<u8>>)>> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        let rows: Vec<(String, serde_json::Value)> =
            sqlx::query_as("SELECT DISTINCT ON (hash) hash, data FROM spores WHERE hash = ANY($1)")
                .bind(hashes)
                .fetch_all(&self.pool)
                .await?;

        let mut map: std::collections::HashMap<String, Vec<u8>> =
            std::collections::HashMap::with_capacity(rows.len());
        for (hash, data) in rows {
            map.insert(hash, serde_json::to_vec(&data)?);
        }

        Ok(hashes
            .iter()
            .map(|h| (h.to_string(), map.remove(*h)))
            .collect())
    }

    async fn get_spore_tastes_batch(&self, hashes: &[&str]) -> Result<Vec<(String, Vec<Vec<u8>>)>> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        // Normalize hashes
        let normalized: Vec<String> = hashes
            .iter()
            .filter_map(|h| {
                substrate::parse_hash(h)
                    .map(|hash| substrate::format_hash(hash.algorithm, &hash.bytes))
                    .ok()
            })
            .collect();
        let norm_refs: Vec<&str> = normalized.iter().map(|s| s.as_str()).collect();

        let rows: Vec<(String, serde_json::Value)> = sqlx::query_as(
            "SELECT target_hash, data FROM taste_reports WHERE target_kind = 'spore' AND target_hash = ANY($1) ORDER BY created_at",
        )
        .bind(&norm_refs)
        .fetch_all(&self.pool)
        .await?;

        let mut map: std::collections::HashMap<String, Vec<Vec<u8>>> =
            std::collections::HashMap::new();
        for (hash, data) in rows {
            map.entry(hash)
                .or_default()
                .push(serde_json::to_vec(&data)?);
        }

        Ok(hashes
            .iter()
            .map(|h| {
                let key = substrate::parse_hash(h)
                    .map(|hash| substrate::format_hash(hash.algorithm, &hash.bytes))
                    .unwrap_or_else(|_| h.to_string());
                let tastes = map.remove(&key).unwrap_or_default();
                (h.to_string(), tastes)
            })
            .collect())
    }

    async fn get_domain_public_key(&self, domain: &str) -> Result<Option<String>> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT public_key FROM mycelia WHERE domain = $1 AND status = 'synced'",
        )
        .bind(domain)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(pk,)| pk))
    }

    async fn get_total_spore_count(&self) -> Result<u64> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM spores")
            .fetch_one(&self.pool)
            .await?;
        Ok(count as u64)
    }

    async fn get_taste_by_hash(&self, hash: &str) -> Result<Option<Vec<u8>>> {
        let row: Option<(JsonValue,)> =
            sqlx::query_as("SELECT data FROM taste_by_hash WHERE taste_hash = $1")
                .bind(hash)
                .fetch_optional(&self.pool)
                .await?;
        Ok(match row {
            Some((val,)) => Some(serde_json::to_vec(&val)?),
            None => None,
        })
    }

    async fn save_taste_index(&self, taste_hash: &str, data: &[u8]) -> Result<()> {
        let json_value: JsonValue = serde_json::from_slice(data)?;
        sqlx::query(
            r#"INSERT INTO taste_by_hash (taste_hash, data)
               VALUES ($1, $2)
               ON CONFLICT (taste_hash) DO UPDATE SET data = EXCLUDED.data"#,
        )
        .bind(taste_hash)
        .bind(json_value)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_mycelium_by_hash(&self, hash: &str) -> Result<Vec<MyceliumRow>> {
        #[derive(FromRow)]
        struct DbMyceliumRow {
            domain: String,
            hash: String,
            updated_at_epoch_ms: i64,
        }

        let rows: Vec<DbMyceliumRow> = sqlx::query_as(
            "SELECT domain, hash, updated_at_epoch_ms FROM mycelia WHERE status = 'synced' AND ($1 = ANY(string_to_array(hash, ',')) OR hash = $1) ORDER BY updated_at_epoch_ms DESC"
        )
        .bind(hash)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| MyceliumRow {
                domain: r.domain,
                hash: r.hash,
                updated_at_epoch_ms: r.updated_at_epoch_ms as u64,
            })
            .collect())
    }
}
