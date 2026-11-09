//! In-memory cache for cmn.json entries and key resolution.
//!
//! Provides TTL-based caching with stale fallback and storage-backed key fallback.

use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::warn;

use crate::storage::{Storage, StorageBackend};
use substrate::CmnEntry;

const DOMAIN_STATE_JUMP_THRESHOLD: u64 = 1000;

/// A fetched identity was present but failed verification; retrying transport cannot fix it.
#[derive(Debug)]
pub(crate) struct DomainVerificationFailure(pub anyhow::Error);

impl std::fmt::Display for DomainVerificationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for DomainVerificationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

fn verification_failure(error: anyhow::Error) -> anyhow::Error {
    DomainVerificationFailure(error).into()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeySource {
    Override,
    MemoryFresh,
    Network,
    MemoryStale,
    StorageFallback,
}

impl KeySource {
    pub fn is_fallback(self) -> bool {
        matches!(self, Self::MemoryStale | Self::StorageFallback)
    }
}

fn verify_cmn_entry(domain: &str, entry: &CmnEntry) -> Result<()> {
    let capsule = entry
        .primary_capsule()
        .map_err(|e| anyhow!("Invalid cmn.json for {}: {}", domain, e))?;
    let expected_uri = substrate::build_domain_uri(domain);
    if capsule.uri != expected_uri {
        return Err(anyhow!(
            "cmn.json primary capsule uri {} does not match domain {}",
            capsule.uri,
            domain
        ));
    }
    entry.verify_signature(&capsule.key).map_err(|e| {
        anyhow!(
            "cmn.json signature verification failed for {}: {}",
            domain,
            e
        )
    })
}

fn domain_state_pin_from_entry(domain: &str, entry: &CmnEntry) -> Result<DomainStatePin> {
    verify_cmn_entry(domain, entry)?;
    let capsule = entry
        .primary_capsule()
        .map_err(|e| anyhow!("Invalid cmn.json for {}: {}", domain, e))?;
    let capsules_digest = entry
        .capsules_digest()
        .map_err(|e| anyhow!("Failed to digest cmn.json capsules for {}: {}", domain, e))?;
    Ok(DomainStatePin {
        serial: capsule.serial,
        capsules_digest,
        current_key: capsule.key.clone(),
    })
}

fn validate_domain_state_transition(
    domain: &str,
    pin: &DomainStatePin,
    next: &DomainStatePin,
    entry: &CmnEntry,
) -> Result<()> {
    if next.serial < pin.serial {
        return Err(anyhow!(
            "domain_state_rollback: cmn.json serial {} is lower than pinned serial {} for {}",
            next.serial,
            pin.serial,
            domain
        ));
    }
    if next.serial == pin.serial && next.capsules_digest != pin.capsules_digest {
        return Err(anyhow!(
            "domain_state_equivocation: cmn.json serial {} for {} has a different capsules digest than the local pin",
            next.serial,
            domain
        ));
    }
    if next.serial.saturating_sub(pin.serial) > DOMAIN_STATE_JUMP_THRESHOLD {
        return Err(anyhow!(
            "domain_state_jump: cmn.json serial jumped from {} to {} for {}",
            pin.serial,
            next.serial,
            domain
        ));
    }
    if next.current_key != pin.current_key {
        entry
            .primary_capsule()
            .map_err(|e| anyhow!("Invalid cmn.json for {}: {}", domain, e))?
            .verify_rotation_chain_from(&pin.current_key)
            .map_err(|e| {
                anyhow!(
                    "domain_key_rotation_unproven: cmn.json key changed for {} without a valid rotation chain: {}",
                    domain,
                    e
                )
            })?;
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn keypair(seed: u8) -> ([u8; 32], String) {
        let private_key = [seed; 32];
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&private_key);
        let public_key = substrate::format_key(
            substrate::KeyAlgorithm::Ed25519,
            &signing_key.verifying_key().to_bytes(),
        );
        (private_key, public_key)
    }

    fn cmn_entry(domain: &str, serial: u64, key: &str, private_key: &[u8; 32]) -> CmnEntry {
        let capsules = vec![substrate::CmnCapsuleEntry {
            uri: substrate::build_domain_uri(domain),
            serial,
            key: key.to_string(),
            history: vec![],
            endpoints: vec![],
        }];
        let capsule_signature = substrate::compute_signature(
            &capsules,
            substrate::SignatureAlgorithm::Ed25519,
            private_key,
        )
        .unwrap();
        CmnEntry {
            schema: substrate::CMN_SCHEMA.to_string(),
            capsules,
            capsule_signature,
        }
    }

    fn rotation_entry(
        domain: &str,
        old_private: &[u8; 32],
        old_key: &str,
        new_key: &str,
        serial: u64,
    ) -> substrate::KeyHistoryEntry {
        let retired_at_epoch_ms = 1_710_000_000_000;
        let statement = substrate::build_key_rotation_statement(
            domain,
            old_key,
            new_key,
            serial,
            retired_at_epoch_ms,
        );
        let rotation_signature = substrate::compute_signature(
            &statement,
            substrate::SignatureAlgorithm::Ed25519,
            old_private,
        )
        .unwrap();
        substrate::KeyHistoryEntry {
            key: old_key.to_string(),
            status: substrate::KeyHistoryStatus::Retired,
            retired_at_epoch_ms,
            replaced_by: Some(new_key.to_string()),
            effective_serial: Some(serial),
            rotation_signature: Some(rotation_signature),
            revoked_at_epoch_ms: None,
        }
    }

    #[test]
    fn verify_cmn_entry_rejects_tampered_signature_scope() {
        let (private_key, public_key) = keypair(1);
        let mut entry = cmn_entry("example.com", 1, &public_key, &private_key);
        entry.capsules[0].serial = 2;

        let err = verify_cmn_entry("example.com", &entry).unwrap_err();
        assert!(err.to_string().contains("signature verification failed"));
    }

    #[test]
    fn domain_state_transition_rejects_rollback_and_equivocation() {
        let (private_key, public_key) = keypair(2);
        let entry = cmn_entry("example.com", 2, &public_key, &private_key);
        let pin = domain_state_pin_from_entry("example.com", &entry).unwrap();

        let rollback = cmn_entry("example.com", 1, &public_key, &private_key);
        let rollback_pin = domain_state_pin_from_entry("example.com", &rollback).unwrap();
        assert!(
            validate_domain_state_transition("example.com", &pin, &rollback_pin, &rollback)
                .unwrap_err()
                .to_string()
                .contains("domain_state_rollback")
        );

        let mut equivocation = cmn_entry("example.com", 2, &public_key, &private_key);
        equivocation.capsules[0]
            .endpoints
            .push(substrate::CmnEndpoint {
                kind: "spore".to_string(),
                url: "https://example.com/cmn/spore/{hash}.json".to_string(),
                hash: String::new(),
                hashes: vec![],
                format: None,
                delta_url: None,
            });
        equivocation.capsule_signature = substrate::compute_signature(
            &equivocation.capsules,
            substrate::SignatureAlgorithm::Ed25519,
            &private_key,
        )
        .unwrap();
        let equivocation_pin = domain_state_pin_from_entry("example.com", &equivocation).unwrap();
        assert!(
            validate_domain_state_transition("example.com", &pin, &equivocation_pin, &equivocation)
                .unwrap_err()
                .to_string()
                .contains("domain_state_equivocation")
        );
    }

    #[test]
    fn domain_state_transition_requires_rotation_proof_on_key_change() {
        let (old_private, old_key) = keypair(3);
        let (new_private, new_key) = keypair(4);
        let old_entry = cmn_entry("example.com", 1, &old_key, &old_private);
        let old_pin = domain_state_pin_from_entry("example.com", &old_entry).unwrap();

        let unproven = cmn_entry("example.com", 2, &new_key, &new_private);
        let unproven_pin = domain_state_pin_from_entry("example.com", &unproven).unwrap();
        assert!(
            validate_domain_state_transition("example.com", &old_pin, &unproven_pin, &unproven)
                .unwrap_err()
                .to_string()
                .contains("domain_key_rotation_unproven")
        );

        let mut proven = cmn_entry("example.com", 2, &new_key, &new_private);
        proven.capsules[0].history = vec![rotation_entry(
            "example.com",
            &old_private,
            &old_key,
            &new_key,
            2,
        )];
        proven.capsule_signature = substrate::compute_signature(
            &proven.capsules,
            substrate::SignatureAlgorithm::Ed25519,
            &new_private,
        )
        .unwrap();
        let proven_pin = domain_state_pin_from_entry("example.com", &proven).unwrap();
        validate_domain_state_transition("example.com", &old_pin, &proven_pin, &proven).unwrap();
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedKey {
    pub key: String,
    pub source: KeySource,
    /// Optional reason from the failed refresh/network fetch when fallback was used.
    pub hint: Option<String>,
}

#[derive(Clone)]
pub struct CmnCache {
    ttl: Duration,
    max_cmn_bytes: usize,
    entries: Arc<RwLock<HashMap<String, CachedEntry>>>,
    pins: Arc<RwLock<HashMap<String, DomainStatePin>>>,
    http: reqwest::Client,
}

#[derive(Clone)]
struct CachedEntry {
    entry: CmnEntry,
    fetched_at: Instant,
}

#[derive(Clone)]
struct DomainStatePin {
    serial: u64,
    capsules_digest: String,
    current_key: String,
}

impl CmnCache {
    pub fn new(ttl_s: u64, max_cmn_bytes: usize, http: reqwest::Client) -> Self {
        Self {
            ttl: Duration::from_secs(ttl_s),
            max_cmn_bytes,
            entries: Arc::new(RwLock::new(HashMap::new())),
            pins: Arc::new(RwLock::new(HashMap::new())),
            http,
        }
    }

    /// Invalidate the cache for a domain, forcing a fresh fetch on next access.
    pub async fn invalidate(&self, domain: &str) {
        let mut guard = self.entries.write().await;
        guard.remove(domain);
    }

    /// Get a CMN entry with TTL cache and stale fallback.
    pub async fn get_entry(&self, domain: &str) -> Result<CmnEntry> {
        let (entry, _, _) = self.get_entry_with_source(domain).await?;
        Ok(entry)
    }

    /// Resolve domain key with cache + stale fallback + storage fallback.
    pub async fn resolve_key(&self, domain: &str, storage: &StorageBackend) -> Result<ResolvedKey> {
        match self.get_entry_with_source(domain).await {
            Ok((entry, source, hint)) => {
                let key = entry
                    .primary_capsule()
                    .map_err(|e| anyhow!("Invalid cmn.json for {}: {}", domain, e))?
                    .key
                    .clone();
                Ok(ResolvedKey { key, source, hint })
            }
            Err(fetch_err)
                if fetch_err
                    .downcast_ref::<DomainVerificationFailure>()
                    .is_some() =>
            {
                Err(fetch_err)
            }
            Err(fetch_err) => match storage.get_domain_public_key(domain).await {
                Ok(Some(key)) => Ok(ResolvedKey {
                    key,
                    source: KeySource::StorageFallback,
                    hint: Some(fetch_err.to_string()),
                }),
                Ok(None) => Err(fetch_err),
                Err(storage_err) => Err(storage_err.context(
                    "Failed to read the stored domain key after an identity fetch failure",
                )),
            },
        }
    }

    /// Resolve and confirm a specific key for a domain at a signed timestamp.
    pub async fn resolve_confirmed_key(
        &self,
        domain: &str,
        key: &str,
        signed_at_epoch_ms: u64,
        storage: &StorageBackend,
    ) -> Result<ResolvedKey> {
        match self.get_entry_with_source(domain).await {
            Ok((entry, source, hint)) => {
                let capsule = entry
                    .primary_capsule()
                    .map_err(|e| anyhow!("Invalid cmn.json for {}: {}", domain, e))?;
                if capsule
                    .key_confirmation_at(key, signed_at_epoch_ms)
                    .is_some()
                {
                    Ok(ResolvedKey {
                        key: key.to_string(),
                        source,
                        hint,
                    })
                } else {
                    Err(verification_failure(anyhow!(
                        "Key {} is not confirmed by {} at timestamp {}",
                        key,
                        domain,
                        signed_at_epoch_ms
                    )))
                }
            }
            Err(fetch_err)
                if fetch_err
                    .downcast_ref::<DomainVerificationFailure>()
                    .is_some() =>
            {
                Err(fetch_err)
            }
            Err(fetch_err) => match storage.get_domain_public_key(domain).await {
                Ok(Some(stored_key)) if stored_key == key => Ok(ResolvedKey {
                    key: key.to_string(),
                    source: KeySource::StorageFallback,
                    hint: Some(fetch_err.to_string()),
                }),
                Ok(Some(_)) | Ok(None) => Err(fetch_err),
                Err(storage_err) => Err(storage_err.context(
                    "Failed to read the stored domain key after an identity fetch failure",
                )),
            },
        }
    }

    async fn get_entry_with_source(
        &self,
        domain: &str,
    ) -> Result<(CmnEntry, KeySource, Option<String>)> {
        let now = Instant::now();
        let mut stale_entry: Option<CachedEntry> = None;

        {
            let guard = self.entries.read().await;
            if let Some(cached) = guard.get(domain) {
                if now.duration_since(cached.fetched_at) <= self.ttl {
                    return Ok((cached.entry.clone(), KeySource::MemoryFresh, None));
                }
                stale_entry = Some(cached.clone());
            }
        }

        let fetched_entry = match self.fetch_entry(domain).await {
            Ok(entry) => match self.validate_and_pin_entry(domain, &entry).await {
                Ok(()) => Ok(entry),
                Err(e) => Err(verification_failure(e)),
            },
            Err(e) => Err(e),
        };

        match fetched_entry {
            Ok(entry) => {
                let mut guard = self.entries.write().await;
                guard.insert(
                    domain.to_string(),
                    CachedEntry {
                        entry: entry.clone(),
                        fetched_at: Instant::now(),
                    },
                );
                Ok((entry, KeySource::Network, None))
            }
            Err(e) => {
                if e.downcast_ref::<DomainVerificationFailure>().is_some() {
                    return Err(e);
                }
                if let Some(cached) = stale_entry {
                    warn!(domain = %domain, error = %e, "Using stale cmn.json cache entry");
                    Ok((cached.entry, KeySource::MemoryStale, Some(e.to_string())))
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn fetch_entry(&self, domain: &str) -> Result<CmnEntry> {
        let payload = crate::crawler::fetch_json(
            &self.http,
            &substrate::cmn_entry_url(domain),
            self.max_cmn_bytes,
        )
        .await
        .map_err(|error| {
            let transport_failure = crate::crawler::is_retryable_crawl_error(&error)
                || error.chain().any(|cause| {
                    cause
                        .downcast_ref::<reqwest::Error>()
                        .is_some_and(|error| error.status().is_some())
                });
            if transport_failure {
                error
            } else {
                verification_failure(error)
            }
        })?;
        if substrate::validate_schema(&payload).map_err(verification_failure)?
            != substrate::SchemaType::Cmn
        {
            return Err(verification_failure(anyhow!("Expected cmn.json")));
        }
        let entry: CmnEntry =
            serde_json::from_value(payload).map_err(|error| verification_failure(error.into()))?;
        verify_cmn_entry(domain, &entry).map_err(verification_failure)?;
        Ok(entry)
    }

    async fn validate_and_pin_entry(&self, domain: &str, entry: &CmnEntry) -> Result<()> {
        let next = domain_state_pin_from_entry(domain, entry)?;
        let mut pins = self.pins.write().await;
        if let Some(pin) = pins.get(domain) {
            validate_domain_state_transition(domain, pin, &next, entry)?;
        }
        pins.insert(domain.to_string(), next);
        Ok(())
    }
}
