#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
//! Nostr integration tests for Synapse.
//!
//! Tests:
//! 1. build_event: correct tags for spore vs mycelium
//! 2. forward_pulse: stores event locally + persists to storage
//! 3. Relay NIP-01: REQ → EVENT + EOSE
//! 4. Relay EVENT write-in: client submits EVENT → relay stores + broadcasts
//! 5. Relay CLOSE: unsubscribe stops delivery
//! 6. Relay rejects non-CMN events (wrong kind / missing tag)
//! 7. Event persistence: events survive bridge restart
//! 8. Mycelium event sync (not only spore)
//! 9. Key persistence: file-based key reload
//! 10. Full two-node relay sync
//! 11. process_cmn_event: spore round-trip (event → parse → verify → store)
//! 12. process_cmn_event: mycelium round-trip
//! 13. Spore bonds preserved through nostr path
//! 14. Relay live broadcast: subscriber receives EVENT in real-time
//! 15. Event deduplication: same event_id not stored twice
//! 16. Data integrity: nostr-stored data matches original pulse
//! 17. Full relay chain: A forward_pulse → relay → B subscribe + process → storage

#![cfg(feature = "nostr")]

use ed25519_dalek::{Signer, SigningKey};
use futures::{SinkExt, StreamExt};
use nostr_sdk::JsonUtil;
use serde_json::Value;
use serial_test::serial;
use std::sync::Arc;
use synapse::config::NostrConfig;
use synapse::nostr::NostrBridge;
use synapse::relay::RelayState;
use synapse::storage::{Storage, StorageBackend};
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite;

fn now_epoch_ms() -> u64 {
    use std::time::SystemTime;

    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

// --- Test helpers ---

fn generate_test_keypair() -> (SigningKey, String) {
    let secret_bytes: [u8; 32] = [
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
        26, 27, 28, 29, 30, 31, 32,
    ];
    let signing_key = SigningKey::from_bytes(&secret_bytes);
    let public_key_b58 = bs58::encode(signing_key.verifying_key().to_bytes()).into_string();
    (signing_key, public_key_b58)
}

fn sign_jcs(signing_key: &SigningKey, data: &Value) -> String {
    let canonical = serde_jcs::to_string(data).unwrap();
    let signature = signing_key.sign(canonical.as_bytes());
    format!(
        "ed25519.{}",
        bs58::encode(signature.to_bytes()).into_string()
    )
}

fn create_signed_spore(signing_key: &SigningKey, domain: &str, hash: &str) -> Value {
    create_signed_spore_with_refs(signing_key, domain, hash, vec![])
}

fn create_signed_spore_with_refs(
    signing_key: &SigningKey,
    domain: &str,
    hash: &str,
    bonds: Vec<Value>,
) -> Value {
    let public_key = format!(
        "ed25519.{}",
        bs58::encode(signing_key.verifying_key().to_bytes()).into_string()
    );

    // Parse bonds from JSON values into typed SporeBond structs
    let typed_bonds: Vec<substrate::SporeBond> = bonds
        .iter()
        .map(|b| serde_json::from_value(b.clone()).unwrap())
        .collect();

    // Build via typed structs to ensure JCS-canonicalized bytes match verification
    let core = substrate::SporeCore {
        id: "test-spore".into(),
        name: "Test Spore".into(),
        version: String::new(),
        domain: domain.into(),
        key: public_key.clone(),
        synopsis: "A test spore".into(),
        intent: vec!["Initial release".into()],
        license: "MIT".into(),
        mutations: Vec::new(),
        size_bytes: 0,
        updated_at_epoch_ms: now_epoch_ms(),
        bonds: typed_bonds,
        tree: substrate::SporeTree {
            algorithm: "blob_tree_blake3_nfc".into(),
            exclude_names: Vec::new(),
            follow_rules: Vec::new(),
        },
    };
    let core_value = serde_json::to_value(&core).unwrap();
    let core_signature = sign_jcs(signing_key, &core_value);

    let mut payload = substrate::Spore {
        schema: substrate::SPORE_SCHEMA.to_string(),
        capsule: substrate::SporeCapsule {
            uri: format!("cmn://{}/{}", domain, hash),
            core,
            core_signature,
            dist: vec![substrate::SporeDist {
                kind: substrate::DistKind::Archive,
                filename: Some("test.tar.zst".into()),
                url: None,
                git_ref: None,
                cid: None,
                extra: Default::default(),
            }],
        },
        capsule_signature: String::new(),
    };
    let capsule_value = serde_json::to_value(&payload.capsule).unwrap();
    payload.capsule_signature = sign_jcs(signing_key, &capsule_value);

    serde_json::to_value(&payload).unwrap()
}

fn create_signed_mycelium(signing_key: &SigningKey, domain: &str, _hash: &str) -> Value {
    let now = now_epoch_ms();
    let public_key = format!(
        "ed25519.{}",
        bs58::encode(signing_key.verifying_key().to_bytes()).into_string()
    );

    // Build typed struct first, serialize, then sign — ensures JCS matches
    let core = substrate::MyceliumCore {
        name: "Test Mycelium".into(),
        synopsis: "A test mycelium".into(),
        domain: domain.into(),
        key: public_key.clone(),
        bio: String::new(),
        nutrients: Vec::new(),
        updated_at_epoch_ms: now,
        spores: Vec::new(),
        tastes: Vec::new(),
    };
    let core_value = serde_json::to_value(&core).unwrap();
    let core_signature = sign_jcs(signing_key, &core_value);

    let mut payload = substrate::Mycelium {
        schema: substrate::MYCELIUM_SCHEMA.to_string(),
        capsule: substrate::MyceliumCapsule {
            uri: String::new(),
            core,
            core_signature,
        },
        capsule_signature: String::new(),
    };
    let computed_hash = payload.computed_uri_hash().unwrap();
    payload.capsule.uri = format!("cmn://{}/mycelium/{}", domain, computed_hash);
    let capsule_value = serde_json::to_value(&payload.capsule).unwrap();
    payload.capsule_signature = sign_jcs(signing_key, &capsule_value);
    serde_json::to_value(&payload).unwrap()
}

fn create_test_storage() -> StorageBackend {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    std::mem::forget(temp_dir);
    let storage = synapse::RedbStorage::open(db_path).unwrap();
    StorageBackend::Redb(storage)
}

#[allow(dead_code)]
fn test_config() -> Arc<synapse::config::AppConfig> {
    Arc::new(synapse::config::AppConfig {
        debug: true,
        log_format: "plain".to_string(),
        bonds: vec![],
        storage: synapse::config::StorageConfig {
            backend: "redb".to_string(),
            postgres_url_secret: None,
            redb_path: Some("test.db".to_string()),
        },
        http: Some(synapse::config::HttpConfig {
            enabled: true,
            address: "127.0.0.1:0".to_string(),
        }),
        https: None,
        cache: synapse::config::CacheConfig::default(),
        pulse: synapse::config::PulseConfig::default(),
        rate_limit: synapse::config::RateLimitConfig::default(),
        retry: synapse::config::RetryConfig::default(),
        lineage: synapse::config::LineageConfig::default(),
        #[cfg(feature = "nostr")]
        nostr: None,
        search: None,
    })
}

fn create_test_app_state(storage: StorageBackend) -> Arc<synapse::AppState> {
    let config = test_config();
    let cmn_cache = Arc::new(synapse::cmn_cache::CmnCache::new(
        300,
        256 * 1024,
        reqwest::Client::new(),
    ));
    let cache_config = synapse::config::CacheConfig::default();
    let crawler = synapse::crawler::Crawler::new(
        storage.clone(),
        cmn_cache.clone(),
        reqwest::Client::new(),
        &cache_config,
        &config.pulse,
        #[cfg(feature = "ruvector")]
        None,
        #[cfg(feature = "ruvector")]
        None,
    );

    Arc::new(synapse::AppState {
        config,
        storage,
        crawler,
        cmn_cache,
        rate_state: std::sync::atomic::AtomicU64::new(0),
        #[cfg(feature = "nostr")]
        nostr_relay: None,
        #[cfg(feature = "ruvector")]
        search: None,
        #[cfg(feature = "ruvector")]
        graph: None,
    })
}

async fn process_event_into_storage(
    event: &nostr_sdk::Event,
    storage: &StorageBackend,
) -> anyhow::Result<()> {
    let state = create_test_app_state(storage.clone());
    synapse::nostr::process_cmn_event_from_relay(
        event,
        state.as_ref(),
        1_048_576,
        state.config.pulse.max_clock_skew_s * 1000,
        state.config.pulse.max_taste_clock_skew_s * 1000,
    )
    .await
}

fn create_nostr_config(
    key_dir: &std::path::Path,
    relays: Vec<String>,
    forward_pulse: bool,
) -> NostrConfig {
    NostrConfig {
        enabled: true,
        relays,
        key_store: format!("file:{}", key_dir.join("nostr.key").to_string_lossy()),
        subscribe: true,
        forward_pulse,
        max_events: 10_000,
        max_ws_message_bytes: 262_144,
    }
}

/// Start a relay server with the given bridge and storage, return (addr, relay_state, handle).
async fn start_relay_with_state(
    bridge: Arc<NostrBridge>,
    _storage: StorageBackend,
) -> (
    std::net::SocketAddr,
    Arc<RelayState>,
    tokio::task::JoinHandle<Result<(), std::io::Error>>,
) {
    let (event_tx, _) = broadcast::channel::<nostr_sdk::Event>(256);
    let relay_state = Arc::new(RelayState {
        event_tx,
        bridge,
        max_body_bytes: 1_048_576,
        app_state: std::sync::OnceLock::new(),
    });

    let state = relay_state.clone();
    let app = axum::Router::new().route(
        "/",
        axum::routing::get(move |ws: axum::extract::ws::WebSocketUpgrade| {
            let s = state.clone();
            async move { ws.on_upgrade(move |socket| synapse::relay::handle_connection(socket, s)) }
        }),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move { axum::serve(listener, app).await });
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    (addr, relay_state, handle)
}

async fn start_relay(
    bridge: Arc<NostrBridge>,
    storage: StorageBackend,
) -> (
    std::net::SocketAddr,
    tokio::task::JoinHandle<Result<(), std::io::Error>>,
) {
    let (addr, _, handle) = start_relay_with_state(bridge, storage).await;
    (addr, handle)
}

/// Build Nostr events from signed pulse payloads and store in bridge.
async fn store_events(bridge: &NostrBridge, pulses: &[Value]) {
    let events_lock = bridge.events();
    let mut events = events_lock.write().await;
    for pulse in pulses {
        events.push_back(bridge.build_event(pulse).unwrap());
    }
}

/// Collect WebSocket messages until EOSE or timeout.
async fn collect_until_eose(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    timeout_secs: u64,
) -> (Vec<Value>, bool) {
    let mut received_events = Vec::new();
    let mut got_eose = false;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(timeout_secs);

    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Ok(Some(Ok(tungstenite::Message::Text(text)))) => {
                let msg: Value = serde_json::from_str(&text).unwrap();
                let arr = msg.as_array().unwrap();
                match arr[0].as_str().unwrap() {
                    "EVENT" => received_events.push(arr[2].clone()),
                    "EOSE" => {
                        got_eose = true;
                        break;
                    }
                    _ => {}
                }
            }
            _ => break,
        }
    }
    (received_events, got_eose)
}

// =============================================================================
// 1. build_event tag correctness
// =============================================================================

#[tokio::test]
async fn test_build_event_spore_tags() {
    let (signing_key, _) = generate_test_keypair();
    let key_dir = tempfile::tempdir().unwrap();
    let config = create_nostr_config(key_dir.path(), vec![], false);
    let storage = create_test_storage();
    let bridge = NostrBridge::from_config(&config, &storage, 1_048_576)
        .await
        .unwrap();

    let pulse = create_signed_spore(&signing_key, "example.com", "b3.aaa");
    let event = bridge.build_event(&pulse).unwrap();

    // kind must be 30078
    assert_eq!(event.kind, nostr_sdk::Kind::Custom(30078));

    // Must have #t = cmn-spore
    let tags: Vec<Vec<String>> = event.tags.iter().map(|t| t.clone().to_vec()).collect();
    let has_spore_tag = tags
        .iter()
        .any(|t| t.len() >= 2 && t[0] == "t" && t[1] == "cmn-spore");
    assert!(has_spore_tag, "Missing #t cmn-spore tag, got: {:?}", tags);

    // Must have domain tag
    let has_domain = tags
        .iter()
        .any(|t| t.len() >= 2 && t[0] == "domain" && t[1] == "example.com");
    assert!(has_domain, "Missing domain tag, got: {:?}", tags);

    // Content must be valid JSON containing our URI in capsule.uri
    let content: Value = serde_json::from_str(&event.content).unwrap();
    assert_eq!(content["capsule"]["uri"], "cmn://example.com/b3.aaa");
}

#[tokio::test]
async fn test_build_event_mycelium_tags() {
    let (signing_key, _) = generate_test_keypair();
    let key_dir = tempfile::tempdir().unwrap();
    let config = create_nostr_config(key_dir.path(), vec![], false);
    let storage = create_test_storage();
    let bridge = NostrBridge::from_config(&config, &storage, 1_048_576)
        .await
        .unwrap();

    let pulse = create_signed_mycelium(&signing_key, "example.com", "b3.myc");
    let event = bridge.build_event(&pulse).unwrap();

    assert_eq!(event.kind, nostr_sdk::Kind::Custom(30078));

    let tags: Vec<Vec<String>> = event.tags.iter().map(|t| t.clone().to_vec()).collect();
    let has_mycelium_tag = tags
        .iter()
        .any(|t| t.len() >= 2 && t[0] == "t" && t[1] == "cmn-mycelium");
    assert!(
        has_mycelium_tag,
        "Missing #t cmn-mycelium tag, got: {:?}",
        tags
    );
}

// =============================================================================
// 2. forward_pulse local storage
// =============================================================================

#[tokio::test]
async fn test_forward_pulse_stores_locally() {
    let (signing_key, _) = generate_test_keypair();
    let key_dir = tempfile::tempdir().unwrap();
    // forward_pulse=true but no relays → skips network send, but build_event still works
    let config = create_nostr_config(key_dir.path(), vec![], true);
    let storage = create_test_storage();
    let bridge = NostrBridge::from_config(&config, &storage, 1_048_576)
        .await
        .unwrap();

    // No relays → forward_pulse returns Ok immediately (self.relays.is_empty())
    // but we can still test build_event + manual local store
    let pulse = create_signed_spore(&signing_key, "fw.local", "b3.fw1");
    let event = bridge.build_event(&pulse).unwrap();

    // Simulate what forward_pulse does: store locally
    {
        let events_lock = bridge.events();
        let mut events = events_lock.write().await;
        events.push_back(event.clone());
    }
    storage
        .save_nostr_event(&event.id.to_hex(), event.as_json().as_bytes())
        .await
        .unwrap();

    // Verify in-memory
    let events_lock = bridge.events();
    let events = events_lock.read().await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].id, event.id);

    // Verify persisted in storage
    let persisted = storage.get_nostr_events().await.unwrap();
    assert_eq!(persisted.len(), 1);
}

// =============================================================================
// 3. Relay NIP-01: REQ → EVENT + EOSE (existing test, kept)
// =============================================================================

#[tokio::test]
async fn test_relay_delivers_stored_events() {
    let (signing_key, _) = generate_test_keypair();
    let domain = "relay-test.local";
    let hash = "b3.raw111";

    let key_dir = tempfile::tempdir().unwrap();
    let config = create_nostr_config(key_dir.path(), vec![], false);
    let storage = create_test_storage();
    let bridge = Arc::new(
        NostrBridge::from_config(&config, &storage, 1_048_576)
            .await
            .unwrap(),
    );

    let pulse = create_signed_spore(&signing_key, domain, hash);
    store_events(&bridge, &[pulse]).await;
    let (addr, server) = start_relay(bridge, storage).await;

    let url = format!("ws://127.0.0.1:{}", addr.port());
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    let req = serde_json::json!(["REQ", "test-sub", {"kinds": [30078]}]);
    ws.send(tungstenite::Message::Text(req.to_string().into()))
        .await
        .unwrap();

    let (received_events, got_eose) = collect_until_eose(&mut ws, 3).await;

    let _ = ws.close(None).await;
    server.abort();

    assert!(got_eose, "Relay did not send EOSE");
    assert_eq!(received_events.len(), 1, "Expected 1 stored event");

    let content: Value =
        serde_json::from_str(received_events[0]["content"].as_str().unwrap()).unwrap();
    assert_eq!(
        content["capsule"]["uri"],
        format!("cmn://{}/{}", domain, hash)
    );
}

// =============================================================================
// 4. Relay EVENT write-in: client submits EVENT → relay stores + broadcasts
// =============================================================================

#[tokio::test]
async fn test_relay_accepts_event_submission() {
    let key_dir = tempfile::tempdir().unwrap();
    let config = create_nostr_config(key_dir.path(), vec![], false);
    let storage = create_test_storage();
    let bridge = Arc::new(
        NostrBridge::from_config(&config, &storage, 1_048_576)
            .await
            .unwrap(),
    );

    // Build a valid CMN event to submit
    let (signing_key, _) = generate_test_keypair();
    let pulse = create_signed_spore(&signing_key, "submit.local", "b3.sub1");
    let event = bridge.build_event(&pulse).unwrap();

    let (addr, relay_state, server) = start_relay_with_state(bridge, storage.clone()).await;

    // --- Client 1: subscriber ---
    let url = format!("ws://127.0.0.1:{}", addr.port());
    let (mut ws_sub, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let req = serde_json::json!(["REQ", "sub1", {"kinds": [30078]}]);
    ws_sub
        .send(tungstenite::Message::Text(req.to_string().into()))
        .await
        .unwrap();
    // Drain EOSE for stored events (none yet)
    let _ = collect_until_eose(&mut ws_sub, 2).await;

    // --- Client 2: submitter ---
    let (mut ws_pub, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let event_json: Value = serde_json::from_str(&event.as_json()).unwrap();
    let event_msg = serde_json::json!(["EVENT", event_json]);
    ws_pub
        .send(tungstenite::Message::Text(event_msg.to_string().into()))
        .await
        .unwrap();

    // Wait for relay to process
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // --- Verify: event stored in bridge memory ---
    let events_lock = relay_state.bridge.events();
    let events = events_lock.read().await;
    assert!(
        events.iter().any(|e| e.id == event.id),
        "Submitted event not found in relay bridge"
    );
    drop(events);

    // --- Verify: event persisted to storage ---
    let persisted = storage.get_nostr_events().await.unwrap();
    assert!(
        !persisted.is_empty(),
        "Submitted event not persisted to storage"
    );

    let _ = ws_sub.close(None).await;
    let _ = ws_pub.close(None).await;
    server.abort();
}

// =============================================================================
// 5. Relay CLOSE: unsubscribe stops delivery
// =============================================================================

#[tokio::test]
async fn test_relay_close_stops_delivery() {
    let key_dir = tempfile::tempdir().unwrap();
    let config = create_nostr_config(key_dir.path(), vec![], false);
    let storage = create_test_storage();
    let bridge = Arc::new(
        NostrBridge::from_config(&config, &storage, 1_048_576)
            .await
            .unwrap(),
    );

    let (addr, _, server) = start_relay_with_state(bridge.clone(), storage).await;
    let url = format!("ws://127.0.0.1:{}", addr.port());

    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    // Subscribe
    let req = serde_json::json!(["REQ", "mysub", {"kinds": [30078]}]);
    ws.send(tungstenite::Message::Text(req.to_string().into()))
        .await
        .unwrap();
    let _ = collect_until_eose(&mut ws, 2).await;

    // Close subscription
    let close_msg = serde_json::json!(["CLOSE", "mysub"]);
    ws.send(tungstenite::Message::Text(close_msg.to_string().into()))
        .await
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Submit a new event via a second connection — subscriber should NOT receive it
    let (signing_key, _) = generate_test_keypair();
    let pulse = create_signed_spore(&signing_key, "close.local", "b3.ca1");
    let event = bridge.build_event(&pulse).unwrap();

    let (mut ws2, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let event_json: Value = serde_json::from_str(&event.as_json()).unwrap();
    let event_msg = serde_json::json!(["EVENT", event_json]);
    ws2.send(tungstenite::Message::Text(event_msg.to_string().into()))
        .await
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Try to read from original subscriber — should get nothing (timeout)
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_millis(300);
    let mut got_event = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Ok(Some(Ok(tungstenite::Message::Text(text)))) => {
                let msg: Value = serde_json::from_str(&text).unwrap();
                if let Some(arr) = msg.as_array() {
                    if arr[0].as_str() == Some("EVENT") {
                        got_event = true;
                    }
                }
            }
            _ => break,
        }
    }

    assert!(
        !got_event,
        "Received event after CLOSE — subscription was not removed"
    );

    let _ = ws.close(None).await;
    let _ = ws2.close(None).await;
    server.abort();
}

// =============================================================================
// 6. Relay rejects non-CMN events
// =============================================================================

#[tokio::test]
async fn test_relay_rejects_wrong_kind() {
    let key_dir = tempfile::tempdir().unwrap();
    let config = create_nostr_config(key_dir.path(), vec![], false);
    let storage = create_test_storage();
    let bridge = Arc::new(
        NostrBridge::from_config(&config, &storage, 1_048_576)
            .await
            .unwrap(),
    );

    let (addr, _, server) = start_relay_with_state(bridge.clone(), storage).await;
    let url = format!("ws://127.0.0.1:{}", addr.port());

    // Build a non-CMN event (kind 1 = text note)
    let nostr_keys = nostr_sdk::Keys::generate();
    let wrong_event = nostr_sdk::EventBuilder::new(nostr_sdk::Kind::TextNote, "hello world")
        .sign_with_keys(&nostr_keys)
        .unwrap();

    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let event_json: Value = serde_json::from_str(&wrong_event.as_json()).unwrap();
    let msg = serde_json::json!(["EVENT", event_json]);
    ws.send(tungstenite::Message::Text(msg.to_string().into()))
        .await
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Bridge should NOT have stored it
    let events_lock = bridge.events();
    let events = events_lock.read().await;
    assert!(
        events.is_empty(),
        "Wrong-kind event should not be stored, but found {} events",
        events.len()
    );

    let _ = ws.close(None).await;
    server.abort();
}

#[tokio::test]
async fn test_relay_rejects_missing_cmn_tag() {
    let key_dir = tempfile::tempdir().unwrap();
    let config = create_nostr_config(key_dir.path(), vec![], false);
    let storage = create_test_storage();
    let bridge = Arc::new(
        NostrBridge::from_config(&config, &storage, 1_048_576)
            .await
            .unwrap(),
    );

    let (addr, _, server) = start_relay_with_state(bridge.clone(), storage).await;
    let url = format!("ws://127.0.0.1:{}", addr.port());

    // Build kind 30078 event but without cmn-spore/cmn-mycelium tag
    let nostr_keys = nostr_sdk::Keys::generate();
    let bad_event = nostr_sdk::EventBuilder::new(nostr_sdk::Kind::Custom(30078), "no cmn tag here")
        .sign_with_keys(&nostr_keys)
        .unwrap();

    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let event_json: Value = serde_json::from_str(&bad_event.as_json()).unwrap();
    let msg = serde_json::json!(["EVENT", event_json]);
    ws.send(tungstenite::Message::Text(msg.to_string().into()))
        .await
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let events_lock = bridge.events();
    let events = events_lock.read().await;
    assert!(
        events.is_empty(),
        "Event without CMN tag should not be stored, but found {} events",
        events.len()
    );

    let _ = ws.close(None).await;
    server.abort();
}

// =============================================================================
// 7. Event persistence: events survive bridge restart
// =============================================================================

#[tokio::test]
async fn test_event_persistence_across_restart() {
    let (signing_key, _) = generate_test_keypair();
    let key_dir = tempfile::tempdir().unwrap();
    let config = create_nostr_config(key_dir.path(), vec![], false);
    let storage = create_test_storage();

    // First bridge: create and persist an event
    {
        let bridge = NostrBridge::from_config(&config, &storage, 1_048_576)
            .await
            .unwrap();
        let pulse = create_signed_spore(&signing_key, "persist.local", "b3.p1");
        let event = bridge.build_event(&pulse).unwrap();

        // Persist to storage (like forward_pulse does)
        storage
            .save_nostr_event(&event.id.to_hex(), event.as_json().as_bytes())
            .await
            .unwrap();
    }

    // Second bridge: should load the persisted event
    let bridge2 = NostrBridge::from_config(&config, &storage, 1_048_576)
        .await
        .unwrap();
    let events_lock = bridge2.events();
    let events = events_lock.read().await;
    assert_eq!(
        events.len(),
        1,
        "Persisted event not loaded on bridge restart"
    );

    let content: Value = serde_json::from_str(&events[0].content).unwrap();
    assert_eq!(content["capsule"]["uri"], "cmn://persist.local/b3.p1");
}

// =============================================================================
// 8. Mycelium event sync via relay
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_relay_sync_mycelium() {
    let (signing_key, public_key_b58) = generate_test_keypair();
    let domain = "myc-sync.local";
    let hash = "b3.myc1";

    unsafe {
        std::env::set_var(
            "CMN_PUBLIC_KEY_OVERRIDE",
            format!("ed25519.{}", public_key_b58),
        )
    };

    let key_dir = tempfile::tempdir().unwrap();
    let config = create_nostr_config(key_dir.path(), vec![], false);
    let storage_a = create_test_storage();
    let bridge = Arc::new(
        NostrBridge::from_config(&config, &storage_a, 1_048_576)
            .await
            .unwrap(),
    );

    // Store a mycelium event
    let pulse = create_signed_mycelium(&signing_key, domain, hash);
    store_events(&bridge, &[pulse]).await;

    let (addr, server) = start_relay(bridge, storage_a).await;

    // Synapse B connects and fetches
    let storage_b = create_test_storage();
    let relay_url = format!("ws://127.0.0.1:{}", addr.port());
    let client = nostr_sdk::Client::default();
    client.add_relay(&relay_url).await.unwrap();
    client.connect().await;
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let filter = nostr_sdk::Filter::new().kind(nostr_sdk::Kind::Custom(30078));
    let events = client
        .fetch_events(filter, tokio::time::Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(events.len(), 1, "Should receive 1 mycelium event");

    for event in events.into_iter() {
        process_event_into_storage(&event, &storage_b)
            .await
            .unwrap();
    }

    client.disconnect().await;
    server.abort();

    // Verify mycelium was stored in B
    let myc = storage_b.get_mycelium(domain).await.unwrap();
    unsafe { std::env::remove_var("CMN_PUBLIC_KEY_OVERRIDE") };
    assert!(myc.is_some(), "Mycelium not synced to B");
}

// =============================================================================
// 9. Key persistence: file-based key reload produces same npub
// =============================================================================

#[tokio::test]
async fn test_key_file_persistence() {
    let key_dir = tempfile::tempdir().unwrap();
    let config = create_nostr_config(key_dir.path(), vec![], false);
    let storage = create_test_storage();

    // First init: generates key
    let bridge1 = NostrBridge::from_config(&config, &storage, 1_048_576)
        .await
        .unwrap();
    let event1 = bridge1
        .build_event(&serde_json::json!({
            "$schema": "https://cmn.dev/schemas/v1/spore.json",
            "capsule": { "uri": "cmn://key.local/b3.k1" }
        }))
        .unwrap();
    let pubkey1 = event1.pubkey;

    // Second init: should load same key
    let bridge2 = NostrBridge::from_config(&config, &storage, 1_048_576)
        .await
        .unwrap();
    let event2 = bridge2
        .build_event(&serde_json::json!({
            "$schema": "https://cmn.dev/schemas/v1/spore.json",
            "capsule": { "uri": "cmn://key.local/b3.k2" }
        }))
        .unwrap();
    let pubkey2 = event2.pubkey;

    assert_eq!(pubkey1, pubkey2, "Key not persisted — npub changed");
}

#[tokio::test]
async fn test_key_storage_persistence() {
    let storage = create_test_storage();
    let config = NostrConfig {
        enabled: true,
        relays: vec![],
        key_store: "storage".to_string(),
        subscribe: false,
        forward_pulse: false,
        max_events: 10_000,
        max_ws_message_bytes: 262_144,
    };

    let bridge1 = NostrBridge::from_config(&config, &storage, 1_048_576)
        .await
        .unwrap();
    let event1 = bridge1
        .build_event(&serde_json::json!({
            "$schema": "https://cmn.dev/schemas/v1/spore.json",
            "capsule": { "uri": "cmn://dbkey.local/b3.db1" }
        }))
        .unwrap();
    let pubkey1 = event1.pubkey;

    let bridge2 = NostrBridge::from_config(&config, &storage, 1_048_576)
        .await
        .unwrap();
    let event2 = bridge2
        .build_event(&serde_json::json!({
            "$schema": "https://cmn.dev/schemas/v1/spore.json",
            "capsule": { "uri": "cmn://dbkey.local/b3.db2" }
        }))
        .unwrap();
    let pubkey2 = event2.pubkey;

    assert_eq!(
        pubkey1, pubkey2,
        "DB-backed key not persisted — npub changed"
    );
}

// =============================================================================
// 10. Full two-node relay sync (existing test, kept)
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_two_synapse_relay_sync() {
    let (signing_key, public_key_b58) = generate_test_keypair();
    let domain = "sync-test.local";
    let hash1 = "b3.aaa111";
    let hash2 = "b3.bbb222";

    unsafe {
        std::env::set_var(
            "CMN_PUBLIC_KEY_OVERRIDE",
            format!("ed25519.{}", public_key_b58),
        )
    };

    // --- Synapse A: build events, start relay ---
    let storage_a = create_test_storage();
    let config_a = NostrConfig {
        enabled: true,
        relays: vec![],
        key_store: "storage".to_string(),
        subscribe: false,
        forward_pulse: true,
        max_events: 10_000,
        max_ws_message_bytes: 262_144,
    };
    let bridge_a = Arc::new(
        NostrBridge::from_config(&config_a, &storage_a, 1_048_576)
            .await
            .unwrap(),
    );

    let pulse1 = create_signed_spore(&signing_key, domain, hash1);
    let pulse2 = create_signed_spore(&signing_key, domain, hash2);
    store_events(&bridge_a, &[pulse1, pulse2]).await;

    let (addr, server) = start_relay(bridge_a, storage_a).await;

    // --- Synapse B: connect to A, fetch events, process into storage ---
    let storage_b = create_test_storage();

    let relay_url = format!("ws://127.0.0.1:{}", addr.port());
    let client = nostr_sdk::Client::default();
    client.add_relay(&relay_url).await.unwrap();
    client.connect().await;
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let filter = nostr_sdk::Filter::new().kind(nostr_sdk::Kind::Custom(30078));
    let events = client
        .fetch_events(filter, tokio::time::Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(events.len(), 2, "B should receive 2 events from A");

    for event in events.into_iter() {
        process_event_into_storage(&event, &storage_b)
            .await
            .unwrap();
    }

    client.disconnect().await;
    server.abort();

    // --- Verify: B has both spores ---
    let s1 = storage_b.get_spore(hash1).await.unwrap();
    let s2 = storage_b.get_spore(hash2).await.unwrap();

    unsafe { std::env::remove_var("CMN_PUBLIC_KEY_OVERRIDE") };

    assert!(s1.is_some(), "Spore {} not synced to B", hash1);
    assert!(s2.is_some(), "Spore {} not synced to B", hash2);
}

// =============================================================================
// 11. process_cmn_event: spore round-trip (event → parse → verify → store)
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_process_cmn_event_spore_roundtrip() {
    let (signing_key, public_key_b58) = generate_test_keypair();
    let domain = "proc-spore.local";
    let hash = "b3.procsp1";

    unsafe {
        std::env::set_var(
            "CMN_PUBLIC_KEY_OVERRIDE",
            format!("ed25519.{}", public_key_b58),
        )
    };

    let key_dir = tempfile::tempdir().unwrap();
    let config = create_nostr_config(key_dir.path(), vec![], false);
    let storage = create_test_storage();
    let bridge = NostrBridge::from_config(&config, &storage, 1_048_576)
        .await
        .unwrap();

    // Build a signed spore and convert to nostr event
    let pulse = create_signed_spore(&signing_key, domain, hash);
    let event = bridge.build_event(&pulse).unwrap();

    // Process the nostr event back into storage (simulates receiving from relay)
    let target_storage = create_test_storage();
    process_event_into_storage(&event, &target_storage)
        .await
        .unwrap();

    unsafe { std::env::remove_var("CMN_PUBLIC_KEY_OVERRIDE") };

    // Verify spore was stored
    let stored = target_storage.get_spore(hash).await.unwrap();
    assert!(stored.is_some(), "Spore not stored after process_cmn_event");

    // Verify stored data is valid JSON matching original pulse
    let stored_data: Value = serde_json::from_slice(&stored.unwrap()).unwrap();
    assert_eq!(
        stored_data["capsule"]["uri"],
        format!("cmn://{}/{}", domain, hash)
    );
    assert_eq!(
        stored_data["$schema"],
        "https://cmn.dev/schemas/v1/spore.json"
    );
}

// =============================================================================
// 12. process_cmn_event: mycelium round-trip
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_process_cmn_event_mycelium_roundtrip() {
    let (signing_key, public_key_b58) = generate_test_keypair();
    let domain = "proc-myc.local";
    let hash = "b3.procmyc1";

    unsafe {
        std::env::set_var(
            "CMN_PUBLIC_KEY_OVERRIDE",
            format!("ed25519.{}", public_key_b58),
        )
    };

    let key_dir = tempfile::tempdir().unwrap();
    let config = create_nostr_config(key_dir.path(), vec![], false);
    let storage = create_test_storage();
    let bridge = NostrBridge::from_config(&config, &storage, 1_048_576)
        .await
        .unwrap();

    let pulse = create_signed_mycelium(&signing_key, domain, hash);
    let event = bridge.build_event(&pulse).unwrap();

    let target_storage = create_test_storage();
    process_event_into_storage(&event, &target_storage)
        .await
        .unwrap();

    unsafe { std::env::remove_var("CMN_PUBLIC_KEY_OVERRIDE") };

    // Verify mycelium was stored
    let stored = target_storage.get_mycelium(domain).await.unwrap();
    assert!(
        stored.is_some(),
        "Mycelium not stored after process_cmn_event"
    );

    let stored_data: Value = serde_json::from_slice(&stored.unwrap()).unwrap();
    assert!(stored_data["capsule"]["uri"]
        .as_str()
        .unwrap_or_default()
        .starts_with(&format!("cmn://{}/mycelium/", domain)));
    assert_eq!(
        stored_data["$schema"],
        "https://cmn.dev/schemas/v1/mycelium.json"
    );
}

// =============================================================================
// 13. Spore bonds preserved through nostr path
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_spore_bonds_preserved_through_nostr() {
    let (signing_key, public_key_b58) = generate_test_keypair();
    let domain = "refs.local";
    let hash = "b3.refs1";
    let parent_hash = "b3.parent1";
    let dep_hash = "b3.dep1";

    unsafe {
        std::env::set_var(
            "CMN_PUBLIC_KEY_OVERRIDE",
            format!("ed25519.{}", public_key_b58),
        )
    };

    let bonds = vec![
        serde_json::json!({
            "uri": format!("cmn://{}/{}", domain, parent_hash),
            "relation": "spawned_from"
        }),
        serde_json::json!({
            "uri": format!("cmn://{}/{}", domain, dep_hash),
            "relation": "depends_on"
        }),
    ];

    let key_dir = tempfile::tempdir().unwrap();
    let config = create_nostr_config(key_dir.path(), vec![], false);
    let storage = create_test_storage();
    let bridge = NostrBridge::from_config(&config, &storage, 1_048_576)
        .await
        .unwrap();

    let pulse = create_signed_spore_with_refs(&signing_key, domain, hash, bonds);
    let event = bridge.build_event(&pulse).unwrap();

    // Process into target storage
    let target_storage = create_test_storage();
    process_event_into_storage(&event, &target_storage)
        .await
        .unwrap();

    unsafe { std::env::remove_var("CMN_PUBLIC_KEY_OVERRIDE") };

    // Verify spore stored
    let stored = target_storage.get_spore(hash).await.unwrap();
    assert!(stored.is_some(), "Spore with refs not stored");

    // Verify bonds are in stored data
    let stored_data: Value = serde_json::from_slice(&stored.unwrap()).unwrap();
    let stored_refs = stored_data["capsule"]["core"]["bonds"]
        .as_array()
        .expect("bonds should be array");
    assert_eq!(stored_refs.len(), 2, "Expected 2 bonds");

    let ref_uris: Vec<&str> = stored_refs
        .iter()
        .filter_map(|r| r["uri"].as_str())
        .collect();
    assert!(
        ref_uris.contains(&format!("cmn://{}/{}", domain, parent_hash).as_str()),
        "Missing spawned_from ref"
    );
    assert!(
        ref_uris.contains(&format!("cmn://{}/{}", domain, dep_hash).as_str()),
        "Missing depends_on ref"
    );

    // Verify lineage via storage API (spore should have ancestors)
    let hosts = target_storage.get_spore_hosts(hash).await.unwrap();
    assert!(
        hosts.contains(&domain.to_string()),
        "Domain not in spore hosts"
    );
}

// =============================================================================
// 14. Relay live broadcast: subscriber receives EVENT in real-time
// =============================================================================

#[tokio::test]
async fn test_relay_live_broadcast_to_subscriber() {
    let key_dir = tempfile::tempdir().unwrap();
    let config = create_nostr_config(key_dir.path(), vec![], false);
    let storage = create_test_storage();
    let bridge = Arc::new(
        NostrBridge::from_config(&config, &storage, 1_048_576)
            .await
            .unwrap(),
    );

    let (addr, relay_state, server) = start_relay_with_state(bridge.clone(), storage.clone()).await;
    let url = format!("ws://127.0.0.1:{}", addr.port());

    // Subscriber connects and subscribes
    let (mut ws_sub, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let req = serde_json::json!(["REQ", "live-sub", {"kinds": [30078]}]);
    ws_sub
        .send(tungstenite::Message::Text(req.to_string().into()))
        .await
        .unwrap();
    // Drain EOSE (no events yet)
    let (initial_events, got_eose) = collect_until_eose(&mut ws_sub, 2).await;
    assert!(got_eose, "No EOSE received");
    assert_eq!(initial_events.len(), 0, "Should have no initial events");

    // Publisher submits an event AFTER subscriber is connected
    let (signing_key, _) = generate_test_keypair();
    let pulse = create_signed_spore(&signing_key, "live.local", "b3.nive1");
    let event = bridge.build_event(&pulse).unwrap();

    let (mut ws_pub, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let event_json: Value = serde_json::from_str(&event.as_json()).unwrap();
    let event_msg = serde_json::json!(["EVENT", event_json]);
    ws_pub
        .send(tungstenite::Message::Text(event_msg.to_string().into()))
        .await
        .unwrap();

    // Subscriber should receive the event as a live broadcast
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(3);
    let mut received_live = false;
    let mut received_event_id = String::new();

    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, ws_sub.next()).await {
            Ok(Some(Ok(tungstenite::Message::Text(text)))) => {
                let msg: Value = serde_json::from_str(&text).unwrap();
                if let Some(arr) = msg.as_array() {
                    if arr[0].as_str() == Some("EVENT") && arr[1].as_str() == Some("live-sub") {
                        received_live = true;
                        received_event_id = arr[2]["id"].as_str().unwrap_or("").to_string();
                        break;
                    }
                }
            }
            _ => break,
        }
    }

    assert!(
        received_live,
        "Subscriber did not receive live broadcast EVENT"
    );
    assert_eq!(
        received_event_id,
        event.id.to_hex(),
        "Received event ID doesn't match submitted event"
    );

    let _ = ws_sub.close(None).await;
    let _ = ws_pub.close(None).await;
    drop(relay_state);
    server.abort();
}

// =============================================================================
// 15. Event deduplication: same event_id not stored twice
// =============================================================================

#[tokio::test]
async fn test_nostr_event_dedup_in_relay() {
    let key_dir = tempfile::tempdir().unwrap();
    let config = create_nostr_config(key_dir.path(), vec![], false);
    let storage = create_test_storage();
    let bridge = Arc::new(
        NostrBridge::from_config(&config, &storage, 1_048_576)
            .await
            .unwrap(),
    );

    let (signing_key, _) = generate_test_keypair();
    let pulse = create_signed_spore(&signing_key, "dedup.local", "b3.dup1");
    let event = bridge.build_event(&pulse).unwrap();

    let (addr, relay_state, server) = start_relay_with_state(bridge.clone(), storage.clone()).await;
    let url = format!("ws://127.0.0.1:{}", addr.port());

    // Submit same event twice
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let event_json: Value = serde_json::from_str(&event.as_json()).unwrap();
    let event_msg = serde_json::json!(["EVENT", event_json]);

    ws.send(tungstenite::Message::Text(event_msg.to_string().into()))
        .await
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    ws.send(tungstenite::Message::Text(event_msg.to_string().into()))
        .await
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Verify only one copy in bridge memory
    let events_lock = relay_state.bridge.events();
    let events = events_lock.read().await;
    let matching = events.iter().filter(|e| e.id == event.id).count();
    assert_eq!(
        matching, 1,
        "Expected 1 event in bridge memory, found {}",
        matching
    );
    drop(events);

    // Verify subscriber receives only 1 event via REQ
    let (mut ws2, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let req = serde_json::json!(["REQ", "dedup-check", {"kinds": [30078]}]);
    ws2.send(tungstenite::Message::Text(req.to_string().into()))
        .await
        .unwrap();
    let (received_events, _) = collect_until_eose(&mut ws2, 3).await;
    assert_eq!(
        received_events.len(),
        1,
        "REQ should return 1 event after dedup, got {}",
        received_events.len()
    );

    let _ = ws.close(None).await;
    let _ = ws2.close(None).await;
    drop(relay_state);
    server.abort();
}

// =============================================================================
// 16. Data integrity: nostr-stored data matches original pulse
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_data_integrity_nostr_vs_original() {
    let (signing_key, public_key_b58) = generate_test_keypair();
    let domain = "integrity.local";
    let hash = "b3.integ1";

    unsafe {
        std::env::set_var(
            "CMN_PUBLIC_KEY_OVERRIDE",
            format!("ed25519.{}", public_key_b58),
        )
    };

    let key_dir = tempfile::tempdir().unwrap();
    let config = create_nostr_config(key_dir.path(), vec![], false);
    let storage = create_test_storage();
    let bridge = NostrBridge::from_config(&config, &storage, 1_048_576)
        .await
        .unwrap();

    let pulse = create_signed_spore(&signing_key, domain, hash);

    // Path A: store directly (simulates HTTP pulse handler)
    let direct_storage = create_test_storage();
    let data_direct = serde_json::to_vec(&pulse).unwrap();
    direct_storage
        .save_spore(hash, domain, &data_direct, &[])
        .await
        .unwrap();

    // Path B: go through nostr event → process_cmn_event
    let event = bridge.build_event(&pulse).unwrap();
    let nostr_storage = create_test_storage();
    process_event_into_storage(&event, &nostr_storage)
        .await
        .unwrap();

    unsafe { std::env::remove_var("CMN_PUBLIC_KEY_OVERRIDE") };

    // Compare stored data
    let direct_data = direct_storage.get_spore(hash).await.unwrap().unwrap();
    let nostr_data = nostr_storage.get_spore(hash).await.unwrap().unwrap();

    let direct_json: Value = serde_json::from_slice(&direct_data).unwrap();
    let nostr_json: Value = serde_json::from_slice(&nostr_data).unwrap();

    // Both should contain the same capsule
    assert_eq!(
        direct_json["capsule"], nostr_json["capsule"],
        "Capsule data differs between direct and nostr paths"
    );
    assert_eq!(
        direct_json["capsule_signature"], nostr_json["capsule_signature"],
        "Capsule signature differs between direct and nostr paths"
    );
    assert_eq!(
        direct_json["$schema"], nostr_json["$schema"],
        "Schema differs between direct and nostr paths"
    );

    // Verify domain spores list is correct for both
    let direct_spores = direct_storage.get_domain_spores(domain).await.unwrap();
    let nostr_spores = nostr_storage.get_domain_spores(domain).await.unwrap();
    assert_eq!(direct_spores, nostr_spores, "Domain spore lists differ");
}

// =============================================================================
// 17. Full relay chain: A forward_pulse → relay → B fetch + process → storage
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_full_relay_chain_forward_fetch_process() {
    let (signing_key, public_key_b58) = generate_test_keypair();
    let domain = "chain.local";
    let hash_spore = "b3.chainsp1";
    let hash_mycelium = "b3.chainmyc1";

    unsafe {
        std::env::set_var(
            "CMN_PUBLIC_KEY_OVERRIDE",
            format!("ed25519.{}", public_key_b58),
        )
    };

    // --- Node A: build events from both spore and mycelium pulses, start relay ---
    let storage_a = create_test_storage();
    let config_a = NostrConfig {
        enabled: true,
        relays: vec![],
        key_store: "storage".to_string(),
        subscribe: false,
        forward_pulse: true,
        max_events: 10_000,
        max_ws_message_bytes: 262_144,
    };
    let bridge_a = Arc::new(
        NostrBridge::from_config(&config_a, &storage_a, 1_048_576)
            .await
            .unwrap(),
    );

    let pulse_spore = create_signed_spore(&signing_key, domain, hash_spore);
    let pulse_mycelium = create_signed_mycelium(&signing_key, domain, hash_mycelium);
    store_events(&bridge_a, &[pulse_spore, pulse_mycelium]).await;

    let (addr, server) = start_relay(bridge_a, storage_a).await;

    // --- Node B: connect to A's relay, fetch all events, process into storage ---
    let storage_b = create_test_storage();
    let relay_url = format!("ws://127.0.0.1:{}", addr.port());
    let client = nostr_sdk::Client::default();
    client.add_relay(&relay_url).await.unwrap();
    client.connect().await;
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let filter = nostr_sdk::Filter::new().kind(nostr_sdk::Kind::Custom(30078));
    let events = client
        .fetch_events(filter, tokio::time::Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(
        events.len(),
        2,
        "B should receive 2 events (spore + mycelium)"
    );

    for event in events.into_iter() {
        process_event_into_storage(&event, &storage_b)
            .await
            .unwrap();
    }

    client.disconnect().await;
    server.abort();

    // --- Verify both spore and mycelium stored in B ---
    let spore = storage_b.get_spore(hash_spore).await.unwrap();
    assert!(spore.is_some(), "Spore not synced to B via relay chain");

    let myc = storage_b.get_mycelium(domain).await.unwrap();
    assert!(myc.is_some(), "Mycelium not synced to B via relay chain");

    // Verify stored data integrity
    let spore_data: Value = serde_json::from_slice(&spore.unwrap()).unwrap();
    assert_eq!(
        spore_data["capsule"]["uri"],
        format!("cmn://{}/{}", domain, hash_spore)
    );

    let myc_data: Value = serde_json::from_slice(&myc.unwrap()).unwrap();
    assert!(myc_data["capsule"]["uri"]
        .as_str()
        .unwrap_or_default()
        .starts_with(&format!("cmn://{}/mycelium/", domain)));

    // Verify domain spores includes our hash
    let domain_spores = storage_b.get_domain_spores(domain).await.unwrap();
    assert!(
        domain_spores.contains(&hash_spore.to_string()),
        "Spore hash not in domain spores list"
    );

    unsafe { std::env::remove_var("CMN_PUBLIC_KEY_OVERRIDE") };
}
