//! Nostr bridge for CMN pulse relay.
//!
//! Provides:
//! - Subscribing to external Nostr relays for CMN spore/mycelium/taste events
//! - Forwarding HTTP pulses as Nostr events to configured relays
//! - Processing incoming Nostr events into CMN storage

use crate::config::NostrConfig;
use crate::storage::{Storage, StorageBackend};
use anyhow::{anyhow, Result};
use nostr_sdk::prelude::*;
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Settings key for storing the Nostr nsec in the database.
const NOSTR_KEY_SETTING: &str = "nostr_nsec";

/// CMN-specific Nostr event kind (NIP-78: Application-specific data)
const CMN_EVENT_KIND: u16 = 30078;

/// Nostr bridge managing relay connections for CMN event distribution.
pub struct NostrBridge {
    keys: Keys,
    relays: Vec<String>,
    forward_pulse: bool,
    max_events: usize,
    max_body_bytes: usize,
    /// Cached events for relay functionality (also persisted to storage).
    /// Uses VecDeque for O(1) eviction of oldest events, with HashMap for O(1) dedup.
    events: Arc<RwLock<VecDeque<Event>>>,
    event_ids: Arc<RwLock<HashMap<EventId, ()>>>,
    /// Storage backend for persisting Nostr events
    storage: StorageBackend,
    /// Long-lived Nostr client for relay connections (avoids reconnect per pulse)
    forward_client: Option<Arc<Client>>,
}

impl NostrBridge {
    /// Create a new NostrBridge from configuration.
    ///
    /// Key storage is determined by `config.key_store`:
    /// - `"storage"` — persist nsec in the database via `StorageBackend`
    /// - `"file:<path>"` — persist nsec in a plain file (chmod 0600)
    ///
    /// Auto-generates a new keypair on first run if no key exists.
    pub async fn from_config(
        config: &NostrConfig,
        storage: &StorageBackend,
        pulse_max_body_bytes: usize,
    ) -> Result<Self> {
        let keys = if config.key_store == "storage" {
            // Database-backed key storage
            Self::load_or_generate_key_db(storage).await?
        } else if let Some(path) = config.key_store.strip_prefix("file:") {
            // File-backed key storage
            Self::load_or_generate_key_file(path)?
        } else {
            return Err(anyhow!(
                "Invalid key_store value '{}'. Use 'storage' or 'file:<path>'",
                config.key_store
            ));
        };

        // Load persisted events from storage
        let persisted = storage.get_nostr_events().await.unwrap_or_default();
        let mut events = VecDeque::new();
        let mut event_ids = HashMap::new();
        for event_json in &persisted {
            match Event::from_json(std::str::from_utf8(event_json).unwrap_or("")) {
                Ok(event) => {
                    event_ids.insert(event.id, ());
                    events.push_back(event);
                }
                Err(e) => warn!(error = %e, "Failed to parse persisted Nostr event"),
            }
        }

        // Build long-lived forward client if relays are configured and forwarding is enabled
        let forward_client = if config.forward_pulse && !config.relays.is_empty() {
            let client = Client::builder().signer(keys.clone()).build();
            for relay_url in &config.relays {
                if let Err(e) = client.add_relay(relay_url).await {
                    warn!(relay_url = %relay_url, error = %e, "Failed to add relay");
                }
            }
            client.connect().await;
            info!(
                relay_count = config.relays.len(),
                "Nostr forward client connected"
            );
            Some(Arc::new(client))
        } else {
            None
        };

        info!(
            npub = %keys.public_key().to_bech32().unwrap_or_default(),
            relay_count = config.relays.len(),
            key_store = %config.key_store,
            persisted_events = events.len(),
            "Nostr bridge initialized"
        );

        Ok(Self {
            keys,
            relays: config.relays.clone(),
            forward_pulse: config.forward_pulse,
            max_events: config.max_events,
            max_body_bytes: pulse_max_body_bytes,
            events: Arc::new(RwLock::new(events)),
            event_ids: Arc::new(RwLock::new(event_ids)),
            storage: storage.clone(),
            forward_client,
        })
    }

    /// Load key from database or generate and save a new one.
    async fn load_or_generate_key_db(storage: &StorageBackend) -> Result<Keys> {
        if let Some(nsec) = storage.get_setting(NOSTR_KEY_SETTING).await? {
            let keys =
                Keys::parse(nsec.trim()).map_err(|e| anyhow!("Invalid nsec in database: {}", e))?;
            info!(
                npub = %keys.public_key().to_bech32().unwrap_or_default(),
                "Loaded Nostr key from database"
            );
            Ok(keys)
        } else {
            let keys = Keys::generate();
            let nsec = keys
                .secret_key()
                .to_bech32()
                .map_err(|e| anyhow!("Failed to encode nsec: {}", e))?;
            storage.save_setting(NOSTR_KEY_SETTING, &nsec).await?;
            info!(
                npub = %keys.public_key().to_bech32().unwrap_or_default(),
                "Generated new Nostr key and saved to database"
            );
            Ok(keys)
        }
    }

    /// Load key from file or generate and save a new one.
    fn load_or_generate_key_file(path: &str) -> Result<Keys> {
        use std::path::Path;
        let key_path = Path::new(path);

        // Reject path traversal via .. components
        if key_path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(anyhow!(
                "Key file path '{}' must not contain '..' components",
                path
            ));
        }

        if key_path.exists() {
            // After resolving symlinks, verify the real path matches what we expect.
            // For relative paths, ensure resolution stays under the working directory.
            let canonical = key_path
                .canonicalize()
                .map_err(|e| anyhow!("Failed to resolve key file path '{}': {}", path, e))?;
            if !key_path.is_absolute() {
                let cwd = std::env::current_dir()
                    .map_err(|e| anyhow!("Failed to get working directory: {}", e))?;
                if !canonical.starts_with(&cwd) {
                    return Err(anyhow!(
                        "Key file '{}' resolves outside working directory (symlink escape)",
                        path
                    ));
                }
            }

            let nsec = std::fs::read_to_string(&canonical)
                .map_err(|e| anyhow!("Failed to read key file '{}': {}", path, e))?;
            let keys = Keys::parse(nsec.trim())
                .map_err(|e| anyhow!("Invalid nsec in '{}': {}", path, e))?;
            info!(
                npub = %keys.public_key().to_bech32().unwrap_or_default(),
                key_file = %path,
                "Loaded Nostr key from file"
            );
            Ok(keys)
        } else {
            let keys = Keys::generate();
            let nsec = keys
                .secret_key()
                .to_bech32()
                .map_err(|e| anyhow!("Failed to encode nsec: {}", e))?;

            std::fs::write(key_path, nsec.as_bytes())
                .map_err(|e| anyhow!("Failed to write key file '{}': {}", path, e))?;

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let perms = std::fs::Permissions::from_mode(0o600);
                std::fs::set_permissions(key_path, perms)
                    .map_err(|e| anyhow!("Failed to set key file permissions: {}", e))?;
            }

            info!(
                npub = %keys.public_key().to_bech32().unwrap_or_default(),
                key_file = %path,
                "Generated new Nostr key and saved to file"
            );
            Ok(keys)
        }
    }

    /// Get stored events (for relay functionality).
    pub fn events(&self) -> Arc<RwLock<VecDeque<Event>>> {
        self.events.clone()
    }

    /// Store an event in the in-memory ring buffer and persist to storage.
    pub(crate) async fn store_event(&self, event: &Event) {
        // O(1) dedup check via HashMap
        {
            let ids = self.event_ids.read().await;
            if ids.contains_key(&event.id) {
                return;
            }
        }

        let mut events = self.events.write().await;
        let mut ids = self.event_ids.write().await;

        // Double-check under write lock
        if ids.contains_key(&event.id) {
            return;
        }

        while events.len() >= self.max_events {
            if let Some(evicted) = events.pop_front() {
                ids.remove(&evicted.id);
            }
        }
        ids.insert(event.id, ());
        events.push_back(event.clone());
        drop(ids);
        drop(events);

        if let Err(e) = self
            .storage
            .save_nostr_event(&event.id.to_hex(), event.as_json().as_bytes())
            .await
        {
            warn!(error = %e, "Failed to persist Nostr event to storage");
        }
    }

    /// Build a CMN Nostr event from a raw manifest (pulse payload).
    pub fn build_event(&self, pulse_json: &Value) -> Result<Event> {
        let uri = pulse_json
            .get("capsule")
            .and_then(|c| c.get("uri"))
            .and_then(|u| u.as_str())
            .ok_or_else(|| anyhow!("Pulse missing 'capsule.uri' field"))?;

        // Parse URI using substrate's parser
        let parsed =
            substrate::parse_uri(uri).map_err(|e| anyhow!("Invalid CMN URI '{}': {}", uri, e))?;

        let domain = &parsed.domain;
        let hash = parsed.hash.as_deref().unwrap_or("");

        let content = serde_json::to_string(pulse_json)
            .map_err(|e| anyhow!("Failed to serialize pulse: {}", e))?;

        // Determine tag value from URI kind
        let t_value = match parsed.kind {
            substrate::CmnUriKind::Domain => "cmn-mycelium",
            substrate::CmnUriKind::Mycelium => "cmn-mycelium",
            substrate::CmnUriKind::Spore => "cmn-spore",
            substrate::CmnUriKind::Taste => "cmn-taste",
        };

        let event = EventBuilder::new(Kind::Custom(CMN_EVENT_KIND), &content)
            .tag(Tag::identifier(uri))
            .tag(Tag::hashtag(t_value))
            .tag(Tag::custom(TagKind::custom("domain"), [domain.as_str()]))
            .tag(Tag::custom(TagKind::custom("uri"), [uri]))
            .tag(Tag::custom(TagKind::custom("hash"), [hash]))
            .sign_with_keys(&self.keys)
            .map_err(|e| anyhow!("Failed to sign Nostr event: {}", e))?;

        Ok(event)
    }

    /// Forward an HTTP pulse as a Nostr event to configured relays.
    /// Uses the long-lived client to avoid reconnection overhead.
    pub async fn forward_pulse(&self, pulse_json: &Value) -> Result<()> {
        if !self.forward_pulse || self.relays.is_empty() {
            return Ok(());
        }

        let event = self.build_event(pulse_json)?;

        // Store locally for relay serving and persist to database
        self.store_event(&event).await;

        // Publish via the long-lived client
        if let Some(ref client) = self.forward_client {
            match client.send_event(&event).await {
                Ok(output) => {
                    info!(
                        event_id = %event.id.to_hex(),
                        success_count = output.success.len(),
                        failed_count = output.failed.len(),
                        "Forwarded pulse to Nostr"
                    );
                }
                Err(e) => {
                    warn!(error = %e, "Failed to forward pulse to Nostr");
                }
            }
        }

        Ok(())
    }

    /// Run the subscription listener loop (long-running task).
    ///
    /// Uses `Client::subscribe` (persistent) + `client.notifications()` instead of
    /// `stream_events` (which auto-closes on EOSE).
    pub async fn run_listener(
        &self,
        state: Arc<crate::handlers::AppState>,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ) -> Result<()> {
        if self.relays.is_empty() {
            info!("No Nostr relays configured, listener not started");
            return Ok(());
        }

        info!(
            relay_count = self.relays.len(),
            "Starting Nostr subscription listener"
        );

        let client = Client::builder().signer(self.keys.clone()).build();

        for relay_url in &self.relays {
            if let Err(e) = client.add_relay(relay_url).await {
                warn!(relay_url = %relay_url, error = %e, "Failed to add relay for subscription");
            }
        }
        client.connect().await;

        // Subscribe to CMN events (persistent, not auto-closing)
        let filter = Filter::new().kind(Kind::Custom(CMN_EVENT_KIND)).hashtags([
            "cmn-spore",
            "cmn-mycelium",
            "cmn-taste",
        ]);

        let mut notifications = client.notifications();

        match client.subscribe(filter, None).await {
            Ok(output) => {
                info!(
                    subscription_id = %output.val,
                    success_count = output.success.len(),
                    failed_count = output.failed.len(),
                    "Nostr subscription active, listening for CMN events"
                );
            }
            Err(e) => {
                return Err(anyhow!("Failed to subscribe to CMN events: {}", e));
            }
        }

        loop {
            tokio::select! {
                result = notifications.recv() => {
                    match result {
                        Ok(RelayPoolNotification::Event { event, .. }) => {
                            // Store the raw event for relay serving and persist
                            self.store_event(&event).await;

                            // Process the CMN content (unified ingest pipeline)
                            if let Err(e) = process_cmn_event(&event, &state, self.max_body_bytes, state.config.pulse.max_clock_skew_s * 1000, state.config.pulse.max_taste_clock_skew_s * 1000).await {
                                warn!(
                                    event_id = %event.id.to_hex(),
                                    error = %e,
                                    "Failed to process CMN event"
                                );
                            }
                        }
                        Ok(_) => {
                            // Ignore non-event notifications (Message, Shutdown, etc.)
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!(
                                missed_count = n,
                                "Notification channel lagged, some events may be missed"
                            );
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            warn!("Notification channel closed");
                            break;
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Nostr listener received shutdown signal");
                    break;
                }
            }
        }

        client.disconnect().await;
        info!("Nostr listener stopped");
        Ok(())
    }
}

/// Process a CMN Nostr event from the relay module.
/// Uses the unified ingest pipeline (same as HTTP pulse).
pub async fn process_cmn_event_from_relay(
    event: &Event,
    state: &crate::handlers::AppState,
    max_body_bytes: usize,
    max_clock_skew_ms: u64,
    max_taste_clock_skew_ms: u64,
) -> Result<()> {
    process_cmn_event(
        event,
        state,
        max_body_bytes,
        max_clock_skew_ms,
        max_taste_clock_skew_ms,
    )
    .await
}

/// Process a CMN Nostr event via the unified ingest pipeline.
///
/// Parses event content as JSON, then delegates to `ingest::validate_and_verify`
/// and `ingest::store` — the exact same code path as HTTP pulse.
/// `forward_nostr: false` prevents relay loops.
async fn process_cmn_event(
    event: &Event,
    state: &crate::handlers::AppState,
    max_body_bytes: usize,
    max_clock_skew_ms: u64,
    max_taste_clock_skew_ms: u64,
) -> Result<()> {
    // Reject non-CMN events (NIP-78 is shared by all apps)
    let is_cmn = event.tags.iter().any(|tag| {
        tag.as_slice().first().map(|s| s.as_str()) == Some("t")
            && matches!(
                tag.as_slice().get(1).map(|s| s.as_str()),
                Some("cmn-spore" | "cmn-mycelium" | "cmn-taste")
            )
    });
    if !is_cmn {
        return Ok(());
    }

    // Reject oversized event content (same limit as HTTP body)
    if event.content.len() > max_body_bytes {
        return Err(anyhow!(
            "Nostr event content too large: {} bytes (max {})",
            event.content.len(),
            max_body_bytes
        ));
    }

    // Parse content as CMN manifest JSON.
    let payload: Value =
        substrate::client::json_from_slice_reject_duplicates(event.content.as_bytes(), "nostr")
            .map_err(|e| anyhow!("Invalid JSON in Nostr event content: {}", e))?;

    // Unified verification pipeline (same code as HTTP pulse)
    let pulse = crate::ingest::validate_and_verify(
        &payload,
        &state.cmn_cache,
        &state.storage,
        max_clock_skew_ms,
        max_taste_clock_skew_ms,
        state.config.debug,
    )
    .await
    .map_err(|e| anyhow!("{}", e))?;

    // Unified storage pipeline (forward_nostr=false to prevent loops)
    crate::ingest::store(
        state,
        &pulse,
        &crate::ingest::StoreOptions {
            forward_nostr: false,
        },
    )
    .await?;

    Ok(())
}
