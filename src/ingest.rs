//! Unified CMN payload ingestion pipeline.
//!
//! Single entry point for validating, verifying, and storing CMN payloads
//! regardless of transport (HTTP pulse, Nostr event, or future sources).
//!
//! ```text
//! HTTP pulse ──┐
//!              ├──→ validate_and_verify() ──→ VerifiedPulse ──→ store()
//! Nostr event ─┘
//! ```

use crate::cmn_cache::{CmnCache, KeySource, ResolvedKey};
use crate::handlers::AppState;
use crate::storage::{SporeBond, Storage, StorageBackend};
use anyhow::Result;
use serde_json::Value;
use tracing::{info, info_span, warn, Instrument};

/// The result of a successful validation + verification pass.
#[allow(dead_code)]
pub struct VerifiedPulse {
    pub payload: Value,
    pub data: Vec<u8>,
    pub content_type: &'static str,
    pub domain: String,
    pub author_domain: String,
    pub uri: String,
    pub hash: Option<String>,
    pub timestamp: u64,
    pub host_key: String,
    pub host_key_source: KeySource,
    pub host_key_hint: Option<String>,
    pub author_key: Option<String>,
    pub author_key_source: Option<KeySource>,
    pub author_key_hint: Option<String>,
    pub spore_bonds: Vec<SporeBond>,
    pub taste_target_uri: Option<String>,
}

enum ParsedPayload {
    Spore(substrate::Spore),
    Mycelium(substrate::Mycelium),
    Taste(substrate::Taste),
}

impl ParsedPayload {
    fn content_type(&self) -> &'static str {
        match self {
            Self::Spore(_) => "spore",
            Self::Mycelium(_) => "mycelium",
            Self::Taste(_) => "taste",
        }
    }

    fn uri(&self) -> &str {
        match self {
            Self::Spore(spore) => spore.uri(),
            Self::Mycelium(mycelium) => mycelium.uri(),
            Self::Taste(taste) => taste.uri(),
        }
    }

    fn author_domain(&self) -> &str {
        match self {
            Self::Spore(spore) => spore.author_domain(),
            Self::Mycelium(mycelium) => mycelium.author_domain(),
            Self::Taste(taste) => taste.author_domain(),
        }
    }

    fn timestamp_ms(&self) -> u64 {
        match self {
            Self::Spore(spore) => spore.timestamp_ms(),
            Self::Mycelium(mycelium) => mycelium.timestamp_ms(),
            Self::Taste(taste) => taste.timestamp_ms(),
        }
    }

    fn embedded_core_key(&self) -> Option<&str> {
        match self {
            Self::Spore(spore) => spore.embedded_core_key(),
            Self::Mycelium(mycelium) => mycelium.embedded_core_key(),
            Self::Taste(taste) => taste.embedded_core_key(),
        }
    }

    fn spore_bonds(&self) -> Vec<SporeBond> {
        match self {
            Self::Spore(spore) => spore.extract_bonds(),
            Self::Mycelium(_) | Self::Taste(_) => Vec::new(),
        }
    }

    fn taste_target_uri(&self) -> Result<Option<String>, IngestError> {
        match self {
            Self::Taste(taste) => substrate::normalize_taste_target_uri(taste.target_uri())
                .map(Some)
                .map_err(|e| IngestError::UriInvalid(e.to_string())),
            Self::Spore(_) | Self::Mycelium(_) => Ok(None),
        }
    }

    fn verify_signatures(&self, host_key: &str, author_key: &str) -> anyhow::Result<()> {
        match self {
            Self::Spore(spore) => spore.verify_signatures(host_key, author_key),
            Self::Mycelium(mycelium) => mycelium.verify_signatures(host_key, author_key),
            Self::Taste(taste) => taste.verify_signatures(host_key, author_key),
        }
    }

    fn verified_uri_hash(&self, expected_hash: Option<&str>) -> anyhow::Result<Option<String>> {
        match self {
            Self::Spore(_) => Ok(expected_hash.map(str::to_string)),
            Self::Mycelium(mycelium) => match expected_hash {
                Some(hash) => {
                    mycelium.verify_uri_hash(hash)?;
                    Ok(Some(hash.to_string()))
                }
                None => mycelium.computed_uri_hash().map(Some),
            },
            Self::Taste(taste) => match expected_hash {
                Some(hash) => {
                    taste.verify_uri_hash(hash)?;
                    Ok(Some(hash.to_string()))
                }
                None => taste.computed_uri_hash().map(Some),
            },
        }
    }
}

#[derive(Debug)]
pub enum StoreError {
    VersionNotNewer {
        incoming_timestamp_epoch_ms: u64,
        cached_timestamp_epoch_ms: u64,
    },
    Internal(anyhow::Error),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::VersionNotNewer {
                incoming_timestamp_epoch_ms,
                cached_timestamp_epoch_ms,
            } => write!(
                f,
                "Mycelium timestamp {} is not newer than cached version {}",
                incoming_timestamp_epoch_ms, cached_timestamp_epoch_ms
            ),
            Self::Internal(err) => write!(f, "{}", err),
        }
    }
}

impl std::error::Error for StoreError {}

impl StoreError {
    pub fn status_code(&self) -> u16 {
        match self {
            Self::VersionNotNewer { .. } => 409,
            Self::Internal(_) => 500,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::VersionNotNewer { .. } => "version_not_newer",
            Self::Internal(_) => "internal_error",
        }
    }
}

impl From<anyhow::Error> for StoreError {
    fn from(err: anyhow::Error) -> Self {
        Self::Internal(err)
    }
}

/// Typed ingestion errors with enough context for both HTTP (→ status code)
/// and Nostr (→ anyhow) callers to produce good messages.
#[derive(Debug)]
pub enum IngestError {
    SchemaInvalid(String),
    SchemaUnknown(String),
    UriInvalid(String),
    HashInvalid(String),
    TimestampFuture(String),
    KeyFetchFailed {
        domain: String,
        error: String,
    },
    SignatureInvalid {
        error: String,
        fallback_hint: Option<String>,
    },
    KeyMismatch {
        core_key: String,
        expected_key: String,
    },
    Serialization(String),
}

impl std::fmt::Display for IngestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SchemaInvalid(e) => write!(f, "Schema validation failed: {}", e),
            Self::SchemaUnknown(e) => write!(f, "Unknown schema: {}", e),
            Self::UriInvalid(e) => write!(f, "URI invalid: {}", e),
            Self::HashInvalid(e) => write!(f, "Hash invalid: {}", e),
            Self::TimestampFuture(e) => write!(f, "Timestamp too far in future: {}", e),
            Self::KeyFetchFailed { domain, error } => {
                write!(f, "Failed to resolve key for {}: {}", domain, error)
            }
            Self::SignatureInvalid {
                error,
                fallback_hint,
            } => match fallback_hint {
                Some(hint) => write!(f, "Signature verification failed: {}. {}", error, hint),
                None => write!(f, "Signature verification failed: {}", error),
            },
            Self::KeyMismatch {
                core_key,
                expected_key,
            } => {
                write!(
                    f,
                    "core.key ({}) does not match expected key ({})",
                    core_key, expected_key
                )
            }
            Self::Serialization(e) => write!(f, "Serialization error: {}", e),
        }
    }
}

impl std::error::Error for IngestError {}

/// HTTP status code mapping for IngestError.
impl IngestError {
    pub fn status_code(&self) -> u16 {
        match self {
            Self::SchemaInvalid(_) | Self::SchemaUnknown(_) => 400,
            Self::UriInvalid(_) | Self::HashInvalid(_) | Self::TimestampFuture(_) => 422,
            Self::SignatureInvalid { .. } | Self::KeyMismatch { .. } => 422,
            Self::KeyFetchFailed { .. } => 502,
            Self::Serialization(_) => 500,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::SchemaInvalid(_) | Self::SchemaUnknown(_) => "invalid_capsule",
            Self::UriInvalid(_) => "uri_invalid",
            Self::HashInvalid(_) => "hash_invalid",
            Self::TimestampFuture(_) => "timestamp_future",
            Self::KeyFetchFailed { .. } => "key_fetch_failed",
            Self::SignatureInvalid { .. } | Self::KeyMismatch { .. } => "invalid_signature",
            Self::Serialization(_) => "internal_error",
        }
    }
}

/// Validate schema, detect type, extract fields, verify signatures, check core.key.
///
/// This is the **single source of truth** for CMN payload verification.
/// Both HTTP pulse and Nostr event processing call this function.
pub async fn validate_and_verify(
    payload: &Value,
    cmn_cache: &CmnCache,
    storage: &StorageBackend,
    max_clock_skew_ms: u64,
    max_taste_clock_skew_ms: u64,
    debug: bool,
) -> Result<VerifiedPulse, IngestError> {
    // 1. Schema validation
    let schema_type = substrate::validate_schema(payload)
        .map_err(|e| IngestError::SchemaInvalid(e.to_string()))?;
    let now_epoch_ms = crate::time::now_epoch_ms();

    let parsed_payload = match schema_type {
        substrate::SchemaType::Spore => {
            let spore = substrate::decode_spore(payload)
                .map_err(|e| IngestError::SchemaInvalid(format!("Typed decode failed: {}", e)))?;
            substrate::validate_timestamp_not_future(
                spore.timestamp_ms(),
                now_epoch_ms,
                max_clock_skew_ms,
            )
            .map_err(|e| IngestError::TimestampFuture(e.to_string()))?;
            ParsedPayload::Spore(spore)
        }
        substrate::SchemaType::Mycelium => {
            let mycelium = substrate::decode_mycelium(payload)
                .map_err(|e| IngestError::SchemaInvalid(format!("Typed decode failed: {}", e)))?;
            substrate::validate_timestamp_not_future(
                mycelium.timestamp_ms(),
                now_epoch_ms,
                max_clock_skew_ms,
            )
            .map_err(|e| IngestError::TimestampFuture(e.to_string()))?;
            ParsedPayload::Mycelium(mycelium)
        }
        substrate::SchemaType::Taste => {
            let taste = substrate::decode_taste(payload)
                .map_err(|e| IngestError::SchemaInvalid(format!("Typed decode failed: {}", e)))?;
            substrate::validate_timestamp_not_future(
                taste.timestamp_ms(),
                now_epoch_ms,
                max_taste_clock_skew_ms,
            )
            .map_err(|e| IngestError::TimestampFuture(e.to_string()))?;
            ParsedPayload::Taste(taste)
        }
        substrate::SchemaType::Cmn | substrate::SchemaType::SporeCore => {
            return Err(IngestError::SchemaUnknown(
                "Unsupported content type".into(),
            ))
        }
    };
    let content_type = parsed_payload.content_type();
    let parsed_uri = substrate::parse_uri(parsed_payload.uri())
        .map_err(|e| IngestError::UriInvalid(e.to_string()))?;
    let uri = parsed_payload.uri().to_string();
    let domain = parsed_uri.domain;
    let hash = parsed_payload
        .verified_uri_hash(parsed_uri.hash.as_deref())
        .map_err(|e| IngestError::HashInvalid(e.to_string()))?;
    let author_domain = parsed_payload.author_domain().to_string();
    let timestamp = parsed_payload.timestamp_ms();
    let embedded_core_key = parsed_payload.embedded_core_key().map(str::to_string);
    let spore_bonds = parsed_payload.spore_bonds();
    let taste_target_uri = parsed_payload.taste_target_uri()?;

    let (host_key, host_key_source, host_key_hint) =
        resolve_key_with_override(&domain, cmn_cache, storage, debug).await?;

    let (author_key, author_key_source, author_key_hint) =
        if let Some(core_key) = embedded_core_key.as_deref() {
            let (k, s, h) = resolve_confirmed_key_with_override(
                &author_domain,
                core_key,
                timestamp,
                cmn_cache,
                storage,
                debug,
            )
            .await?;
            (Some(k), Some(s), h)
        } else if author_domain != domain {
            let (k, s, h) =
                resolve_key_with_override(&author_domain, cmn_cache, storage, debug).await?;
            (Some(k), Some(s), h)
        } else {
            (None, None, None)
        };

    let verify_result =
        parsed_payload.verify_signatures(&host_key, author_key.as_deref().unwrap_or(&host_key));
    if let Err(e) = verify_result {
        let used_fallback = host_key_source.is_fallback()
            || author_key_source.map(|s| s.is_fallback()).unwrap_or(false);
        let fallback_hint = if used_fallback {
            let mut parts = Vec::new();
            if let Some(h) = host_key_hint.as_ref() {
                parts.push(format!("host key fallback: {}", h));
            }
            if let Some(h) = author_key_hint.as_ref() {
                parts.push(format!("author key fallback: {}", h));
            }
            Some(if parts.is_empty() {
                "cached key fallback was used; domain may have rotated keys".into()
            } else {
                format!(
                    "cached key fallback was used; domain may have rotated keys ({})",
                    parts.join("; ")
                )
            })
        } else {
            None
        };
        return Err(IngestError::SignatureInvalid {
            error: e.to_string(),
            fallback_hint,
        });
    }

    // 8. core.key consistency check
    if let Some(core_key) = embedded_core_key.as_deref() {
        let expected = author_key.as_deref().unwrap_or(host_key.as_str());
        if core_key != expected {
            return Err(IngestError::KeyMismatch {
                core_key: core_key.to_string(),
                expected_key: expected.to_string(),
            });
        }
    }

    // 9. Serialize for storage
    let data =
        serde_json::to_vec(payload).map_err(|e| IngestError::Serialization(e.to_string()))?;

    Ok(VerifiedPulse {
        payload: payload.clone(),
        data,
        content_type,
        domain,
        author_domain,
        uri,
        hash,
        timestamp,
        host_key,
        host_key_source,
        host_key_hint,
        author_key,
        author_key_source,
        author_key_hint,
        spore_bonds,
        taste_target_uri,
    })
}

/// Resolve a domain key, respecting CMN_PUBLIC_KEY_OVERRIDE in debug mode.
async fn resolve_key_with_override(
    domain: &str,
    cmn_cache: &CmnCache,
    storage: &StorageBackend,
    debug: bool,
) -> Result<(String, KeySource, Option<String>), IngestError> {
    if debug {
        if let Ok(pk) = std::env::var("CMN_PUBLIC_KEY_OVERRIDE") {
            warn!("Using CMN_PUBLIC_KEY_OVERRIDE (debug mode)");
            return Ok((pk, KeySource::Override, None));
        }
    } else if std::env::var("CMN_PUBLIC_KEY_OVERRIDE").is_ok() {
        warn!("CMN_PUBLIC_KEY_OVERRIDE is set but ignored (debug=false)");
    }

    let ResolvedKey { key, source, hint } =
        cmn_cache
            .resolve_key(domain, storage)
            .await
            .map_err(|e| IngestError::KeyFetchFailed {
                domain: domain.to_string(),
                error: e.to_string(),
            })?;

    Ok((key, source, hint))
}

/// Confirm an embedded core key against its author domain.
async fn resolve_confirmed_key_with_override(
    domain: &str,
    key: &str,
    signed_at_epoch_ms: u64,
    cmn_cache: &CmnCache,
    storage: &StorageBackend,
    debug: bool,
) -> Result<(String, KeySource, Option<String>), IngestError> {
    if debug {
        if let Ok(pk) = std::env::var("CMN_PUBLIC_KEY_OVERRIDE") {
            warn!("Using CMN_PUBLIC_KEY_OVERRIDE (debug mode)");
            if pk == key {
                return Ok((pk, KeySource::Override, None));
            }
            return Err(IngestError::KeyMismatch {
                core_key: key.to_string(),
                expected_key: pk,
            });
        }
    } else if std::env::var("CMN_PUBLIC_KEY_OVERRIDE").is_ok() {
        warn!("CMN_PUBLIC_KEY_OVERRIDE is set but ignored (debug=false)");
    }

    let ResolvedKey { key, source, hint } = cmn_cache
        .resolve_confirmed_key(domain, key, signed_at_epoch_ms, storage)
        .await
        .map_err(|e| IngestError::KeyFetchFailed {
            domain: domain.to_string(),
            error: e.to_string(),
        })?;

    Ok((key, source, hint))
}

/// Options controlling post-storage side effects.
pub struct StoreOptions {
    /// Forward to Nostr relays (false when source IS Nostr, to prevent loops).
    pub forward_nostr: bool,
}

/// Store a verified pulse payload by content type.
///
/// Handles spore bond extraction, mycelium hash verification + version check,
/// taste normalization, and triggers side effects (Nostr forward, search index, graph update).
pub async fn store(
    state: &AppState,
    pulse: &VerifiedPulse,
    opts: &StoreOptions,
) -> std::result::Result<&'static str, StoreError> {
    match pulse.content_type {
        "spore" => store_spore(state, pulse, opts).await,
        "mycelium" => store_mycelium(state, pulse, opts).await,
        "taste" => store_taste(state, pulse).await,
        _ => Err(StoreError::Internal(anyhow::anyhow!(
            "Unknown content type: {}",
            pulse.content_type
        ))),
    }
}

async fn store_spore(
    state: &AppState,
    pulse: &VerifiedPulse,
    opts: &StoreOptions,
) -> std::result::Result<&'static str, StoreError> {
    let hash = pulse
        .hash
        .as_deref()
        .ok_or_else(|| StoreError::Internal(anyhow::anyhow!("Spore hash is required")))?;

    let bonds = pulse.spore_bonds.clone();

    let save_result = state
        .storage
        .save_spore(hash, &pulse.domain, &pulse.data, &bonds)
        .await?;

    let action = match save_result {
        crate::storage::SaveSporeResult::Indexed => "indexed",
        crate::storage::SaveSporeResult::Replicated => "replicated",
        // Duplicate is a success (not an error): content already indexed, skip all
        // side effects (Nostr forwarding, search indexing, graph updates).
        crate::storage::SaveSporeResult::Duplicate => return Ok("duplicate"),
    };

    // Side effects (only for new content)
    #[cfg(feature = "nostr")]
    if opts.forward_nostr {
        if let Some(ref relay_state) = state.nostr_relay {
            if let Err(e) = relay_state.bridge.forward_pulse(&pulse.payload).await {
                warn!(error = %e, "Failed to forward spore pulse to Nostr");
            }
        }
    }

    #[cfg(feature = "ruvector")]
    if let Some(ref search) = state.search {
        use crate::search::Search;
        if let Err(e) = search
            .index_spore(&pulse.domain, hash, &pulse.payload)
            .await
        {
            warn!(error = %e, "Search indexing failed for spore");
        }
    }

    #[cfg(feature = "ruvector")]
    if let Some(ref graph) = state.graph {
        if let Err(e) = graph.add_spore(hash, &pulse.domain) {
            warn!(error = %e, "Graph add_spore failed");
        }
        if !bonds.is_empty() {
            // Spawn graph relationship indexing in background to avoid blocking the pulse
            // response when the embedding API is slow.
            let graph = graph.clone();
            let hash = hash.to_string();
            let bonds = bonds.clone();
            tokio::spawn(async move {
                if let Err(e) = graph.add_relationship(&hash, &bonds).await {
                    warn!(error = %e, "Graph indexing failed for spore");
                }
            });
        }
    }

    // Suppress unused variable warnings when features are disabled
    let _ = opts;

    info!(uri = %pulse.uri, action = %action, "Ingested spore");
    Ok(action)
}

async fn store_mycelium(
    state: &AppState,
    pulse: &VerifiedPulse,
    opts: &StoreOptions,
) -> std::result::Result<&'static str, StoreError> {
    let hash = pulse
        .hash
        .clone()
        .ok_or_else(|| anyhow::anyhow!("Expected verified mycelium URI hash"))?;

    // Version check: only accept strictly newer timestamps.
    // Same semantics as crawler::crawl_domain_inner, but returns 409 instead of silent skip.
    let is_update = if let Ok(Some(cached_data)) = state.storage.get_mycelium(&pulse.domain).await {
        if let Ok(cached) = serde_json::from_slice::<substrate::Mycelium>(&cached_data) {
            let cached_ts = cached.capsule.core.updated_at_epoch_ms;
            if substrate::compare_version_timestamps(pulse.timestamp, cached_ts)
                != substrate::VersionOrder::Newer
            {
                return Err(StoreError::VersionNotNewer {
                    incoming_timestamp_epoch_ms: pulse.timestamp,
                    cached_timestamp_epoch_ms: cached_ts,
                });
            }
            true
        } else {
            false
        }
    } else {
        false
    };

    state
        .storage
        .save_mycelium(&pulse.domain, &pulse.data, &pulse.host_key, &hash)
        .await?;

    #[cfg(feature = "nostr")]
    if opts.forward_nostr {
        if let Some(ref relay_state) = state.nostr_relay {
            if let Err(e) = relay_state.bridge.forward_pulse(&pulse.payload).await {
                warn!(error = %e, "Failed to forward mycelium pulse to Nostr");
            }
        }
    }

    // Suppress unused variable warnings when features are disabled
    let _ = opts;

    // Crawl missing spores in background
    let crawl_state_crawler = state.crawler.clone();
    let crawl_domain = pulse.domain.clone();
    tokio::spawn(async move {
        let span = info_span!("post_pulse_crawl", domain = %crawl_domain);
        async {
            if let Err(e) = crawl_state_crawler
                .crawl_missing_spores(&crawl_domain)
                .await
            {
                warn!(error = %e, "Post-pulse spore crawl failed");
            }
        }
        .instrument(span)
        .await;
    });

    let action = if is_update { "updated" } else { "indexed" };
    info!(uri = %pulse.uri, action = %action, "Ingested mycelium");
    Ok(action)
}

async fn store_taste(
    state: &AppState,
    pulse: &VerifiedPulse,
) -> std::result::Result<&'static str, StoreError> {
    let target_uri = pulse
        .taste_target_uri
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Expected normalized taste target_uri"))?;

    state
        .storage
        .save_taste(target_uri, &pulse.author_domain, &pulse.data)
        .await?;

    // Index taste by its own content hash for GET /synapse/taste/:hash lookups
    if let Some(ref taste_hash) = pulse.hash {
        if let Err(e) = state
            .storage
            .save_taste_index(taste_hash, &pulse.data)
            .await
        {
            warn!(error = %e, "Failed to index taste by hash");
        }
    }

    info!(uri = %pulse.uri, target_uri = %target_uri, "Ingested taste");
    Ok("indexed")
}
