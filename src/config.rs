//! Configuration module for Synapse.

use config::{Config, ConfigError, File};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AppConfig {
    pub debug: bool,
    /// Log output format: "json" (default) or "plain"
    #[serde(default = "default_log_format")]
    pub log_format: String,
    /// Service bonds override — when non-empty, replaces embedded spore.core.json bonds on /health
    #[serde(default)]
    pub bonds: Vec<serde_json::Value>,
    pub storage: StorageConfig,
    #[serde(default)]
    pub http: Option<HttpConfig>,
    #[serde(default)]
    pub https: Option<HttpsConfig>,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub pulse: PulseConfig,
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
    #[serde(default)]
    pub retry: RetryConfig,
    #[serde(default)]
    pub lineage: LineageConfig,
    #[cfg(feature = "nostr")]
    #[serde(default)]
    pub nostr: Option<NostrConfig>,
    #[cfg(feature = "ruvector")]
    #[serde(default)]
    pub search: Option<SearchConfig>,
}

fn default_log_format() -> String {
    "json".to_string()
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct StorageConfig {
    /// Storage backend: "postgres" or "redb"
    pub backend: String,
    /// PostgreSQL connection URL (required if backend is "postgres")
    #[serde(default)]
    pub postgres_url_secret: Option<String>,
    /// Redb file path (required if backend is "redb")
    #[serde(default)]
    pub redb_path: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct HttpConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    pub address: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct HttpsConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    pub address: String,
    #[serde(default)]
    pub acme: AcmeConfig,
    #[serde(default)]
    pub domains: Vec<String>,
    #[serde(default = "default_self_signed_days")]
    pub self_signed_validity_days: i64,
}

fn default_self_signed_days() -> i64 {
    365
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct AcmeConfig {
    #[serde(default)]
    pub contact: String,
    #[serde(default = "default_acme_mode")]
    pub mode: String,
    #[serde(default = "default_renewal_days")]
    pub renewal_before_days: i64,
}

fn default_renewal_days() -> i64 {
    30
}

fn default_acme_mode() -> String {
    "self".to_string()
}

impl AcmeConfig {
    pub fn is_staging(&self) -> bool {
        self.mode == "staging"
    }
}

#[cfg(feature = "nostr")]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NostrConfig {
    /// Enable Nostr integration
    #[serde(default)]
    pub enabled: bool,
    /// Nostr relay URLs to connect to (e.g., ["wss://relay.damus.io"])
    #[serde(default)]
    pub relays: Vec<String>,
    /// Where to persist the Nostr identity key (nsec).
    /// - "file:<path>" (default: "file:nostr.key") — plain file, chmod 0600
    /// - "storage" — store in the configured database (redb/postgres)
    ///   Auto-generated on first run if no key exists.
    #[serde(default = "default_key_store")]
    pub key_store: String,
    /// Subscribe to external relays for incoming CMN events
    #[serde(default)]
    pub subscribe: bool,
    /// Forward HTTP pulses to Nostr relays
    #[serde(default)]
    pub forward_pulse: bool,
    /// Maximum Nostr events to keep in memory (default: 10_000, oldest evicted first)
    #[serde(default = "default_max_events")]
    pub max_events: usize,
    /// Maximum WebSocket message size in bytes (default: 256KB)
    #[serde(default = "default_max_ws_message_bytes")]
    pub max_ws_message_bytes: usize,
}

#[cfg(feature = "nostr")]
fn default_key_store() -> String {
    "storage".to_string()
}

#[cfg(feature = "nostr")]
fn default_max_events() -> usize {
    10_000
}

#[cfg(feature = "nostr")]
fn default_max_ws_message_bytes() -> usize {
    262_144 // 256 KB
}

#[cfg(feature = "ruvector")]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SearchConfig {
    /// Enable search indexing
    #[serde(default)]
    pub enabled: bool,
    /// Path for the search index data
    #[serde(default = "default_index_path")]
    pub index_path: String,
    /// Embedding provider configuration
    pub embedding: EmbeddingConfig,
    /// Graph acceleration layer configuration
    #[serde(default)]
    pub graph: GraphConfig,
}

#[cfg(feature = "ruvector")]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct GraphConfig {
    /// Enable the in-memory evolution graph
    #[serde(default)]
    pub enabled: bool,
    /// Rebuild graph from storage on startup
    #[serde(default = "default_true")]
    pub rebuild_on_startup: bool,
    /// Generate embeddings for relationship edges (costs API calls)
    #[serde(default)]
    pub embed_relationships: bool,
    /// Batch size for startup rebuild
    #[serde(default = "default_rebuild_batch_size")]
    pub rebuild_batch_size: usize,
}

#[cfg(feature = "ruvector")]
impl Default for GraphConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            rebuild_on_startup: true,
            embed_relationships: false,
            rebuild_batch_size: 100,
        }
    }
}

#[cfg(feature = "ruvector")]
fn default_true() -> bool {
    true
}

#[cfg(feature = "ruvector")]
fn default_rebuild_batch_size() -> usize {
    100
}

#[cfg(feature = "ruvector")]
fn default_index_path() -> String {
    "synapse-search".to_string()
}

#[cfg(feature = "ruvector")]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct EmbeddingConfig {
    /// Provider: "ollama" or "openai"
    pub provider: String,
    /// API base URL
    pub url: String,
    /// Model name
    pub model: String,
    /// API key (required for OpenAI)
    #[serde(default)]
    pub api_key_secret: Option<String>,
    /// Embedding vector dimensions
    pub dimensions: usize,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct CacheConfig {
    /// cmn.json cache TTL in seconds (default: 300 = 5 minutes)
    pub cmn_ttl_s: u64,
    /// Maximum size for cmn.json payloads in bytes (default: 256 KB)
    pub max_cmn_bytes: usize,
    /// Maximum size for mycelium.json payloads in bytes (default: 4 MB)
    pub max_mycelium_bytes: usize,
    /// Maximum size for spore.json payloads in bytes (default: 1 MB)
    pub max_spore_bytes: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            cmn_ttl_s: 300,
            max_cmn_bytes: 256 * 1024,           // 256 KB
            max_mycelium_bytes: 4 * 1024 * 1024, // 4 MB
            max_spore_bytes: 1024 * 1024,        // 1 MB
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct PulseConfig {
    /// Maximum allowed clock skew in seconds for spore/mycelium (default: 300 = 5 minutes)
    pub max_clock_skew_s: u64,
    /// Maximum allowed clock skew in seconds for taste reports (default: 60 = 1 minute).
    /// Taste reports are security-sensitive, so a stricter tolerance prevents
    /// attackers from injecting backdated verdicts within a wider window.
    pub max_taste_clock_skew_s: u64,
    /// Maximum request body size in bytes (default: 1MB)
    pub max_body_bytes: usize,
}

impl Default for PulseConfig {
    fn default() -> Self {
        Self {
            max_clock_skew_s: 300,
            max_taste_clock_skew_s: 60,
            max_body_bytes: 1_048_576,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct RateLimitConfig {
    /// Maximum concurrent requests across all endpoints (0 = unlimited, default: 50)
    pub max_concurrent_requests: u64,
    /// Maximum requests per second (0 = unlimited, default: 100)
    pub requests_per_second: u64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_concurrent_requests: 50,
            requests_per_second: 100,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct RetryConfig {
    /// Base retry interval in seconds (default: 60)
    pub base_interval_s: u64,
    /// Maximum interval in seconds after backoff (default: 3600)
    pub max_interval_s: u64,
    /// Backoff multiplier applied on each failure (default: 2.0, clamped to 1.0..=5.0)
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            base_interval_s: 60,
            max_interval_s: 3600,
            backoff_multiplier: 2.0,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct LineageConfig {
    /// Maximum BFS traversal depth for lineage/ancestry queries (default: 50)
    pub max_depth: usize,
}

impl Default for LineageConfig {
    fn default() -> Self {
        Self { max_depth: 50 }
    }
}

fn default_enabled() -> bool {
    true
}

impl AppConfig {
    pub fn load(config_file: Option<&str>) -> Result<Self, ConfigError> {
        let config_path = config_file.unwrap_or("config.yml");
        let config_name = config_path
            .strip_suffix(".yml")
            .or_else(|| config_path.strip_suffix(".yaml"))
            .unwrap_or(config_path);

        let s = Config::builder()
            .add_source(File::with_name(config_name).required(true))
            .build()?;

        let mut config: Self = s.try_deserialize()?;
        config.retry.backoff_multiplier = config.retry.backoff_multiplier.clamp(1.0, 5.0);
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        // Validate storage config
        match self.storage.backend.as_str() {
            #[cfg(feature = "postgres")]
            "postgres" => {
                if self.storage.postgres_url_secret.is_none() {
                    return Err(ConfigError::Message(
                        "storage.postgres_url_secret is required when backend is 'postgres'"
                            .to_string(),
                    ));
                }
            }
            #[cfg(not(feature = "postgres"))]
            "postgres" => {
                return Err(ConfigError::Message(
                    "PostgreSQL support not compiled. Enable the 'postgres' feature.".to_string(),
                ));
            }
            #[cfg(feature = "redb")]
            "redb" => {
                if self.storage.redb_path.is_none() {
                    return Err(ConfigError::Message(
                        "storage.redb_path is required when backend is 'redb'".to_string(),
                    ));
                }
            }
            #[cfg(not(feature = "redb"))]
            "redb" => {
                return Err(ConfigError::Message(
                    "Redb support not compiled. Enable the 'redb' feature.".to_string(),
                ));
            }
            other => {
                return Err(ConfigError::Message(format!(
                    "Invalid storage backend '{}'. Must be 'postgres' or 'redb'",
                    other
                )));
            }
        }

        // Validate HTTPS config if enabled
        if let Some(ref https) = self.https {
            if https.enabled && https.address.is_empty() {
                return Err(ConfigError::Message(
                    "https.address is required when https is enabled".to_string(),
                ));
            }
        }

        // Validate HTTP config if enabled
        if let Some(ref http) = self.http {
            if http.enabled && http.address.is_empty() {
                return Err(ConfigError::Message(
                    "http.address is required when http is enabled".to_string(),
                ));
            }
        }

        // Validate ACME mode: production required when debug=false
        if let Some(ref https) = self.https {
            if https.enabled && !self.debug {
                let mode = &https.acme.mode;
                if mode != "production" {
                    return Err(ConfigError::Message(format!(
                        "ACME mode '{}' is not allowed when debug=false. Use 'production' for real certificates.",
                        mode
                    )));
                }
            }
        }

        // Ensure at least one server is enabled
        let http_enabled = self.http.as_ref().is_some_and(|h| h.enabled);
        let https_enabled = self.https.as_ref().is_some_and(|h| h.enabled);
        if !http_enabled && !https_enabled {
            return Err(ConfigError::Message(
                "At least one of http or https must be enabled".to_string(),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {

    use super::*;

    // ── CacheConfig ──

    #[test]
    fn test_cache_config_defaults() {
        let cfg = CacheConfig::default();
        assert_eq!(cfg.cmn_ttl_s, 300);
        assert_eq!(cfg.max_cmn_bytes, 256 * 1024);
    }

    #[test]
    fn test_cache_config_deserialize_partial() {
        let json = r#"{"cmn_ttl_s": 60}"#;
        let cfg: CacheConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.cmn_ttl_s, 60);
        assert_eq!(cfg.max_cmn_bytes, 256 * 1024);
    }

    #[test]
    fn test_cache_config_deserialize_empty() {
        let cfg: CacheConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(cfg.cmn_ttl_s, 300);
        assert_eq!(cfg.max_cmn_bytes, 256 * 1024);
    }

    #[test]
    fn test_cache_config_deserialize_custom() {
        let json = r#"{"cmn_ttl_s": 10, "max_cmn_bytes": 131072, "max_mycelium_bytes": 2097152, "max_spore_bytes": 524288}"#;
        let cfg: CacheConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.cmn_ttl_s, 10);
        assert_eq!(cfg.max_cmn_bytes, 131_072);
        assert_eq!(cfg.max_mycelium_bytes, 2_097_152);
        assert_eq!(cfg.max_spore_bytes, 524_288);
    }

    // ── PulseConfig ──

    #[test]
    fn test_pulse_config_defaults() {
        let cfg = PulseConfig::default();
        assert_eq!(cfg.max_clock_skew_s, 300);
    }

    #[test]
    fn test_pulse_config_deserialize_custom() {
        let json = r#"{"max_clock_skew_s": 60}"#;
        let cfg: PulseConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.max_clock_skew_s, 60);
    }

    #[test]
    fn test_pulse_config_deserialize_empty() {
        let cfg: PulseConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(cfg.max_clock_skew_s, 300);
    }

    // ── HttpsConfig new fields ──

    #[test]
    fn test_https_config_self_signed_days_default() {
        let json = r#"{"enabled": false, "address": "0.0.0.0:443"}"#;
        let cfg: HttpsConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.self_signed_validity_days, 365);
    }

    #[test]
    fn test_https_config_self_signed_days_custom() {
        let json =
            r#"{"enabled": false, "address": "0.0.0.0:443", "self_signed_validity_days": 90}"#;
        let cfg: HttpsConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.self_signed_validity_days, 90);
    }

    // ── AcmeConfig new fields ──

    #[test]
    fn test_acme_config_renewal_days_default() {
        let json = r#"{"contact": "admin@example.com", "mode": "self"}"#;
        let cfg: AcmeConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.renewal_before_days, 30);
    }

    #[test]
    fn test_acme_config_renewal_days_custom() {
        let json =
            r#"{"contact": "admin@example.com", "mode": "production", "renewal_before_days": 14}"#;
        let cfg: AcmeConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.renewal_before_days, 14);
    }

    // ── Full AppConfig with new sections ──

    #[test]
    fn test_app_config_new_sections_from_yaml_file() {
        // Load from the actual config.yml shipped with the crate
        let config = AppConfig::load(Some("config.yml")).unwrap();
        assert_eq!(config.cache.cmn_ttl_s, 300);
        assert_eq!(config.pulse.max_clock_skew_s, 300);
    }

    #[test]
    fn test_app_config_https_fields_from_yaml_file() {
        let config = AppConfig::load(Some("config.yml")).unwrap();
        let https = config.https.unwrap();
        assert_eq!(https.self_signed_validity_days, 365);
        assert_eq!(https.acme.renewal_before_days, 30);
    }

    #[test]
    fn test_cache_config_serialization_roundtrip() {
        let cfg = CacheConfig {
            cmn_ttl_s: 60,
            max_cmn_bytes: 131_072,
            max_mycelium_bytes: 2_097_152,
            max_spore_bytes: 524_288,
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let deserialized: CacheConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.cmn_ttl_s, 60);
        assert_eq!(deserialized.max_cmn_bytes, 131_072);
        assert_eq!(deserialized.max_mycelium_bytes, 2_097_152);
        assert_eq!(deserialized.max_spore_bytes, 524_288);
    }

    #[test]
    fn test_pulse_config_serialization_roundtrip() {
        let cfg = PulseConfig {
            max_clock_skew_s: 120,
            max_taste_clock_skew_s: 30,
            max_body_bytes: 512_000,
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let deserialized: PulseConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.max_clock_skew_s, 120);
        assert_eq!(deserialized.max_body_bytes, 512_000);
    }

    #[test]
    fn test_cache_config_zero_values() {
        let json = r#"{"cmn_ttl_s": 0, "max_cmn_bytes": 0, "max_mycelium_bytes": 0, "max_spore_bytes": 0}"#;
        let cfg: CacheConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.cmn_ttl_s, 0);
        assert_eq!(cfg.max_cmn_bytes, 0);
    }

    #[test]
    fn test_lineage_config_defaults() {
        let cfg = LineageConfig::default();
        assert_eq!(cfg.max_depth, 50);
    }

    #[test]
    fn test_lineage_config_deserialize_custom() {
        let json = r#"{"max_depth": 100}"#;
        let cfg: LineageConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.max_depth, 100);
    }

    #[test]
    fn test_pulse_config_zero_skew() {
        let json = r#"{"max_clock_skew_s": 0}"#;
        let cfg: PulseConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.max_clock_skew_s, 0);
    }
}
