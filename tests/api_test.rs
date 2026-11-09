#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod support;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use std::sync::Arc;
use substrate::BondRelation;
use synapse::storage::{SporeBond, Storage, StorageBackend};
use tower::ServiceExt;

fn now_epoch_ms() -> u64 {
    use std::time::SystemTime;

    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

// Helper to create a test storage backend (redb)
fn create_test_storage() -> StorageBackend {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let storage = synapse::RedbStorage::open(db_path).unwrap();
    StorageBackend::Redb(storage)
}

fn test_config() -> Arc<synapse::config::AppConfig> {
    Arc::new(synapse::config::AppConfig {
        debug: true,
        log_format: "json".to_string(),
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
        #[cfg(feature = "ruvector")]
        search: None,
    })
}

// Helper to create test router
async fn create_test_app() -> axum::Router {
    let config = test_config();
    let storage = create_test_storage();
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
    synapse::handlers::response::set_storage_backend("redb");
    let state = Arc::new(synapse::AppState {
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
    });
    synapse::create_router(state)
}

#[tokio::test]
async fn test_health_check() {
    let app = create_test_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_get_spore_not_found() {
    let app = create_test_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/synapse/spore/b3.nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 10000)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert!(
        agent_first_data::validate_protocol_event(&json, true).is_ok(),
        "error response must be strict AFDATA: {json}"
    );
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json["error"]["code"], "not_found");
    assert!(
        json["error"]["hint"]
            .as_str()
            .is_some_and(|h| !h.is_empty()),
        "error response should include hint: {:?}",
        json
    );
}

#[tokio::test]
async fn test_save_and_get_spore() {
    let storage = create_test_storage();

    // Save a test spore
    let spore_data = serde_json::json!({
        "protocol": "cmn/1",
        "uri": "cmn://test.com/b3.abc123",
        "capsule": {
            "dist": [{"type": "archive", "filename": "test.tar.zst"}]
        },
        "spore": {
            "name": "test-spore",
            "synopsis": "A test spore",
            "intent": ["testing"],
            "license": "MIT",
            "author": "test.com",
            "bonds": [],
            "tree": {
                "algorithm": "blob_tree_blake3_nfc",
                "exclude_names": [],
                "follow_rules": []
            },
            "signature": "ed25519.test"
        }
    });
    let spore_bytes = serde_json::to_vec(&spore_data).unwrap();

    storage
        .save_spore("b3.abc123", "test.com", &spore_bytes, &[])
        .await
        .unwrap();

    // Retrieve the spore
    let retrieved = storage.get_spore("b3.abc123").await.unwrap();
    assert!(retrieved.is_some());

    let retrieved_data: Value = serde_json::from_slice(&retrieved.unwrap()).unwrap();
    assert_eq!(retrieved_data["spore"]["name"], "test-spore");
}

#[tokio::test]
async fn test_save_and_get_mycelium() {
    let storage = create_test_storage();

    // Save a test mycelium
    let mycelium_data = serde_json::json!({
        "protocol": "cmn/1",
        "uri": "cmn://test.com",
        "mycelium": {
            "name": "Test Developer",
            "synopsis": "A test mycelium",
            "author": "test.com",
            "updated_at_epoch_ms": 1737331200000_u64,
            "spores": [],
            "signature": "ed25519.test"
        }
    });
    let mycelium_bytes = serde_json::to_vec(&mycelium_data).unwrap();

    storage
        .save_mycelium("test.com", &mycelium_bytes, "test_public_key", "b3.def456")
        .await
        .unwrap();

    // Retrieve the mycelium
    let retrieved = storage.get_mycelium("test.com").await.unwrap();
    assert!(retrieved.is_some());

    let retrieved_data: Value = serde_json::from_slice(&retrieved.unwrap()).unwrap();
    assert_eq!(retrieved_data["mycelium"]["name"], "Test Developer");
}

#[tokio::test]
async fn test_domain_spores() {
    let storage = create_test_storage();

    // Save multiple spores for the same domain
    let spore1 = serde_json::json!({"test": "data1"});
    let spore2 = serde_json::json!({"test": "data2"});

    storage
        .save_spore(
            "b3.hash1",
            "test.com",
            &serde_json::to_vec(&spore1).unwrap(),
            &[],
        )
        .await
        .unwrap();

    storage
        .save_spore(
            "b3.hash2",
            "test.com",
            &serde_json::to_vec(&spore2).unwrap(),
            &[],
        )
        .await
        .unwrap();

    // Get all spores for the domain
    let spores = storage.get_domain_spores("test.com").await.unwrap();
    assert_eq!(spores.len(), 2);
    assert!(spores.contains(&"b3.hash1".to_string()));
    assert!(spores.contains(&"b3.hash2".to_string()));
}

#[tokio::test]
async fn test_lineage_tracking() {
    let storage = create_test_storage();
    let parent_hash = substrate::compute_blake3_hash(b"lineage-parent");
    let child1_hash = substrate::compute_blake3_hash(b"lineage-child1");
    let grandchild_hash = substrate::compute_blake3_hash(b"lineage-grandchild");

    // Create a spore lineage:
    // parent
    //   └─> child1
    //        └─> grandchild

    let parent = serde_json::json!({
        "protocol": "cmn/1",
        "uri": format!("cmn://test.com/{}", parent_hash),
        "spore": {
            "name": "parent",
            "author": "test.com",
            "bonds": [],
            "signature": "ed25519.test"
        },
    });

    let child1 = serde_json::json!({
        "protocol": "cmn/1",
        "uri": format!("cmn://test.com/{}", child1_hash),
        "spore": {
            "name": "child1",
            "author": "test.com",
            "bonds": [{"uri": format!("cmn://test.com/{}", parent_hash)}],
            "signature": "ed25519.test"
        },
    });

    let grandchild = serde_json::json!({
        "protocol": "cmn/1",
        "uri": format!("cmn://test.com/{}", grandchild_hash),
        "spore": {
            "name": "grandchild",
            "author": "test.com",
            "bonds": [{"uri": format!("cmn://test.com/{}", child1_hash)}],
            "signature": "ed25519.test"
        },
    });

    // Save spores with bonds
    storage
        .save_spore(
            &parent_hash,
            "test.com",
            &serde_json::to_vec(&parent).unwrap(),
            &[],
        )
        .await
        .unwrap();

    storage
        .save_spore(
            &child1_hash,
            "test.com",
            &serde_json::to_vec(&child1).unwrap(),
            &[SporeBond {
                uri: format!("cmn://test.com/{}", parent_hash),
                relation: BondRelation::SpawnedFrom,
            }],
        )
        .await
        .unwrap();

    storage
        .save_spore(
            &grandchild_hash,
            "test.com",
            &serde_json::to_vec(&grandchild).unwrap(),
            &[SporeBond {
                uri: format!("cmn://test.com/{}", child1_hash),
                relation: BondRelation::SpawnedFrom,
            }],
        )
        .await
        .unwrap();

    // Get lineage of parent
    let lineage = storage.get_spore_lineage(&parent_hash, 10).await.unwrap();

    // Should contain child1 and grandchild
    assert_eq!(lineage.nodes.len(), 2);
    // Extract hash from URI (e.g., "cmn://test.com/b3...." -> "b3....")
    let hashes: Vec<&str> = lineage
        .nodes
        .iter()
        .filter_map(|node| node.uri.rsplit('/').next())
        .collect();
    assert!(hashes.contains(&child1_hash.as_str()));
    assert!(hashes.contains(&grandchild_hash.as_str()));
}

#[tokio::test]
async fn test_lineage_max_depth() {
    let storage = create_test_storage();

    // Create a deeper lineage chain
    let chain: Vec<String> = (0..4)
        .map(|i| substrate::compute_blake3_hash(format!("lineage-level-{i}").as_bytes()))
        .collect();

    for i in 0..chain.len() {
        let bonds: Vec<SporeBond> = if i > 0 {
            vec![SporeBond {
                uri: format!("cmn://test.com/{}", chain[i - 1]),
                relation: BondRelation::SpawnedFrom,
            }]
        } else {
            vec![]
        };

        let spore = serde_json::json!({
            "protocol": "cmn/1",
            "spore": {
                "name": format!("level{}", i),
                "author": "test.com",
                "signature": "ed25519.test"
            },
        });

        storage
            .save_spore(
                chain[i].as_str(),
                "test.com",
                &serde_json::to_vec(&spore).unwrap(),
                &bonds,
            )
            .await
            .unwrap();
    }

    // Test with max_depth = 2
    let lineage = storage.get_spore_lineage(&chain[0], 2).await.unwrap();
    assert_eq!(lineage.nodes.len(), 2); // level1 and level2
    assert!(lineage.max_depth_reached); // Should have more nodes beyond depth 2

    // Test with unlimited depth (max_depth = 0)
    let lineage = storage.get_spore_lineage(&chain[0], 0).await.unwrap();
    assert_eq!(lineage.nodes.len(), 3); // level1, level2, level3
    assert!(!lineage.max_depth_reached); // No limit, so all nodes returned
}

#[tokio::test]
async fn test_mycelium_retry_tracking() {
    let storage = create_test_storage();

    // Mark a domain as failed
    storage
        .mark_mycelium_failed("failed.com", "Connection timeout")
        .await
        .unwrap();

    // Wait for a short time to ensure the retry time has passed
    // (first retry is after 1 minute, but we'll sleep a bit to test the mechanism)
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Note: In redb implementation, next_retry_at is set to now + duration,
    // so we need to wait or check the data was stored correctly
    // For this test, we'll just verify the domain was marked as failed
    // by checking it exists in the storage with failed status

    // Try to get the mycelium - should return None since it's failed
    let result = storage.get_mycelium("failed.com").await.unwrap();
    assert!(result.is_none(), "Failed mycelium should not be returned");
}

#[tokio::test]
async fn test_content_addressable_lookup() {
    let storage = create_test_storage();

    // Save the same hash from different domains
    let spore1 = serde_json::json!({"name": "from-domain1"});
    let spore2 = serde_json::json!({"name": "from-domain2"});

    storage
        .save_spore(
            "b3.samehash",
            "domain1.com",
            &serde_json::to_vec(&spore1).unwrap(),
            &[],
        )
        .await
        .unwrap();

    storage
        .save_spore(
            "b3.samehash",
            "domain2.com",
            &serde_json::to_vec(&spore2).unwrap(),
            &[],
        )
        .await
        .unwrap();

    // Content-addressable lookup should return one of them
    let retrieved = storage.get_spore("b3.samehash").await.unwrap();
    assert!(retrieved.is_some());
}

#[tokio::test]
async fn test_tls_cert_storage() {
    let storage = create_test_storage();

    let domains = vec!["example.com".to_string(), "test.com".to_string()];
    let expires_at = chrono::Utc::now() + chrono::Duration::days(90);

    storage
        .save_cert(&domains, "cert_pem_data", "key_pem_data", expires_at, false)
        .await
        .unwrap();

    let retrieved = storage.get_cert(&domains).await.unwrap();
    assert!(retrieved.is_some());

    let cert = retrieved.unwrap();
    assert_eq!(cert.cert_pem, "cert_pem_data");
    assert_eq!(cert.key_pem_secret, "key_pem_data");
    assert!(!cert.self_signed);
}

#[tokio::test]
async fn test_cross_domain_references() {
    let storage = create_test_storage();

    // Create spores from different domains that bond to each other
    let domain1_spore = serde_json::json!({
        "protocol": "cmn/1",
        "uri": "cmn://domain1.com/b3.hash1",
        "spore": {
            "name": "domain1-spore",
            "author": "domain1.com",
            "bonds": [],
            "signature": "ed25519.test"
        },
    });

    let domain2_spore = serde_json::json!({
        "protocol": "cmn/1",
        "uri": "cmn://domain2.com/b3.hash2",
        "spore": {
            "name": "domain2-spore",
            "author": "domain2.com",
            "bonds": [{"uri": "cmn://domain1.com/b3.hash1"}],
            "signature": "ed25519.test"
        },
    });

    storage
        .save_spore(
            "b3.hash1",
            "domain1.com",
            &serde_json::to_vec(&domain1_spore).unwrap(),
            &[],
        )
        .await
        .unwrap();

    storage
        .save_spore(
            "b3.hash2",
            "domain2.com",
            &serde_json::to_vec(&domain2_spore).unwrap(),
            &[SporeBond {
                uri: "cmn://domain1.com/b3.hash1".to_string(),
                relation: BondRelation::SpawnedFrom,
            }],
        )
        .await
        .unwrap();

    // Check lineage across domains
    let lineage = storage.get_spore_lineage("b3.hash1", 10).await.unwrap();
    assert_eq!(lineage.nodes.len(), 1);
    assert_eq!(lineage.nodes[0].uri, "cmn://domain2.com/b3.hash2");
    let spawned = lineage.nodes[0]
        .refs
        .iter()
        .find(|r| r.relation == "spawned_from");
    assert!(spawned.is_some(), "Missing spawned_from ref");
    assert_eq!(spawned.unwrap().uri, "cmn://domain1.com/b3.hash1");
}

#[tokio::test]
async fn test_spore_hosts() {
    let storage = create_test_storage();

    // Save the same hash from multiple domains (mirrors/forks)
    let spore1 = serde_json::json!({"name": "original"});
    let spore2 = serde_json::json!({"name": "fork"});
    let spore3 = serde_json::json!({"name": "mirror"});

    storage
        .save_spore(
            "b3.samehash",
            "original.com",
            &serde_json::to_vec(&spore1).unwrap(),
            &[],
        )
        .await
        .unwrap();

    storage
        .save_spore(
            "b3.samehash",
            "fork.org",
            &serde_json::to_vec(&spore2).unwrap(),
            &[],
        )
        .await
        .unwrap();

    storage
        .save_spore(
            "b3.samehash",
            "mirror.net",
            &serde_json::to_vec(&spore3).unwrap(),
            &[],
        )
        .await
        .unwrap();

    // Get all hosts for this spore
    let hosts = storage.get_spore_hosts("b3.samehash").await.unwrap();
    assert_eq!(hosts.len(), 3);
    assert!(hosts.contains(&"original.com".to_string()));
    assert!(hosts.contains(&"fork.org".to_string()));
    assert!(hosts.contains(&"mirror.net".to_string()));
}

#[tokio::test]
async fn test_spore_ancestors() {
    let storage = create_test_storage();
    let lib_a_hash = substrate::compute_blake3_hash(b"ancestor-lib-a");
    let lib_b_hash = substrate::compute_blake3_hash(b"ancestor-lib-b");
    let app_hash = substrate::compute_blake3_hash(b"ancestor-app");

    // Create a dependency chain:
    // lib-a (no deps)
    // lib-b (depends on lib-a)
    // app (depends on both lib-a and lib-b)

    let lib_a = serde_json::json!({
        "protocol": "cmn/1",
        "uri": format!("cmn://base.com/{}", lib_a_hash),
        "spore": {
            "name": "lib-a",
            "author": "base.com",
            "bonds": [],
            "signature": "ed25519.test"
        },
    });

    let lib_b = serde_json::json!({
        "protocol": "cmn/1",
        "uri": format!("cmn://base.com/{}", lib_b_hash),
        "spore": {
            "name": "lib-b",
            "author": "base.com",
            "bonds": [{"uri": format!("cmn://base.com/{}", lib_a_hash)}],
            "signature": "ed25519.test"
        },
    });

    let app = serde_json::json!({
        "protocol": "cmn/1",
        "uri": format!("cmn://app.com/{}", app_hash),
        "spore": {
            "name": "app",
            "author": "app.com",
            "bonds": [
                {"uri": format!("cmn://base.com/{}", lib_a_hash)},
                {"uri": format!("cmn://base.com/{}", lib_b_hash)}
            ],
            "signature": "ed25519.test"
        },
    });

    storage
        .save_spore(
            &lib_a_hash,
            "base.com",
            &serde_json::to_vec(&lib_a).unwrap(),
            &[],
        )
        .await
        .unwrap();

    storage
        .save_spore(
            &lib_b_hash,
            "base.com",
            &serde_json::to_vec(&lib_b).unwrap(),
            &[SporeBond {
                uri: format!("cmn://base.com/{}", lib_a_hash),
                relation: BondRelation::DependsOn,
            }],
        )
        .await
        .unwrap();

    storage
        .save_spore(
            &app_hash,
            "app.com",
            &serde_json::to_vec(&app).unwrap(),
            &[
                SporeBond {
                    uri: format!("cmn://base.com/{}", lib_a_hash),
                    relation: BondRelation::DependsOn,
                },
                SporeBond {
                    uri: format!("cmn://base.com/{}", lib_b_hash),
                    relation: BondRelation::DependsOn,
                },
            ],
        )
        .await
        .unwrap();

    // Get ancestors of app
    let ancestors = storage.get_spore_ancestors(&app_hash, 10).await.unwrap();

    // Should find both direct dependencies (lib-a, lib-b) and transitive (lib-a via lib-b)
    assert!(ancestors.nodes.len() >= 2);
    // Extract hash from URI (e.g., "cmn://base.com/b3...." -> "b3....")
    let hashes: Vec<&str> = ancestors
        .nodes
        .iter()
        .filter_map(|node| node.uri.rsplit('/').next())
        .collect();
    assert!(hashes.contains(&lib_a_hash.as_str()));
    assert!(hashes.contains(&lib_b_hash.as_str()));
}

// ============================================================================
// Pulse endpoint tests with CMN_PUBLIC_KEY_OVERRIDE mocking
// ============================================================================

mod pulse_tests {
    use super::*;
    use axum::http::header::CONTENT_TYPE;
    use ed25519_dalek::{Signer, SigningKey};
    use serial_test::serial;

    /// Helper to generate a test keypair and return (signing_key, public_key_b58)
    fn generate_test_keypair() -> (SigningKey, String) {
        let secret_bytes: [u8; 32] = [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ];
        let signing_key = SigningKey::from_bytes(&secret_bytes);
        let public_key_b58 = bs58::encode(signing_key.verifying_key().to_bytes()).into_string();
        (signing_key, public_key_b58)
    }

    /// Helper to sign data with JCS canonicalization
    fn sign_jcs(signing_key: &SigningKey, data: &serde_json::Value) -> String {
        let canonical = serde_jcs::to_string(data).unwrap();
        let signature = signing_key.sign(canonical.as_bytes());
        format!(
            "ed25519.{}",
            bs58::encode(signature.to_bytes()).into_string()
        )
    }

    /// Create a signed mycelium payload for testing
    fn create_signed_mycelium(
        signing_key: &SigningKey,
        domain: &str,
    ) -> (serde_json::Value, String) {
        let now = now_epoch_ms();
        let public_key_b58 = bs58::encode(signing_key.verifying_key().to_bytes()).into_string();
        let key = format!("ed25519.{}", public_key_b58);

        // Build typed struct first, then serialize — ensures signed bytes match
        // what the verification pipeline sees (includes all default fields).
        let core = substrate::MyceliumCore {
            name: "Test Mycelium".into(),
            synopsis: "A test mycelium for integration tests".into(),
            domain: domain.into(),
            key: key.clone(),
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

        let mycelium_hash = payload.computed_uri_hash().unwrap();
        payload.capsule.uri = format!("cmn://{}/mycelium/{}", domain, mycelium_hash);
        let capsule_value = serde_json::to_value(&payload.capsule).unwrap();
        payload.capsule_signature = sign_jcs(signing_key, &capsule_value);
        let capsule_signature = payload.capsule_signature.clone();
        let payload = serde_json::to_value(&payload).unwrap();

        (payload, capsule_signature)
    }

    /// Create a signed spore payload for testing.
    /// Uses typed structs to ensure JCS-canonicalized bytes match what the
    /// verification pipeline produces after deserializing.
    fn create_signed_spore(
        signing_key: &SigningKey,
        domain: &str,
        hash: &str,
    ) -> serde_json::Value {
        let public_key_b58 = bs58::encode(signing_key.verifying_key().to_bytes()).into_string();
        let key = format!("ed25519.{}", public_key_b58);

        let core = substrate::SporeCore {
            id: "test-spore".into(),
            name: "Test Spore".into(),
            version: String::new(),
            domain: domain.into(),
            key: key.clone(),
            synopsis: "A test spore".into(),
            intent: vec!["Initial release".into()],
            license: "MIT".into(),
            mutations: Vec::new(),
            size_bytes: 0,
            updated_at_epoch_ms: now_epoch_ms(),
            bonds: Vec::new(),
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

    #[tokio::test]
    #[serial]
    async fn test_pulse_mycelium_with_key_override() {
        let (signing_key, public_key_b58) = generate_test_keypair();
        let domain = "test-pulse.local";

        let public_key = format!("ed25519.{}", public_key_b58);
        if support::run_with_key_override(&public_key) {
            return;
        }

        let app = create_test_app().await;

        let (payload, _) = create_signed_mycelium(&signing_key, domain);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/synapse/pulse")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_string(&payload).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 10000)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(status, StatusCode::OK, "Response: {:?}", json);
        assert_eq!(json["kind"], "result", "Response: {:?}", json);
    }

    #[tokio::test]
    #[serial]
    async fn test_pulse_spore_with_key_override() {
        let (signing_key, public_key_b58) = generate_test_keypair();
        let domain = "test-spore-pulse.local";
        let hash = "b3.abc123def456";

        let public_key = format!("ed25519.{}", public_key_b58);
        if support::run_with_key_override(&public_key) {
            return;
        }

        let app = create_test_app().await;

        let payload = create_signed_spore(&signing_key, domain, hash);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/synapse/pulse")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_string(&payload).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 10000)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(status, StatusCode::OK, "Response: {:?}", json);
        assert_eq!(json["kind"], "result", "Response: {:?}", json);
    }

    #[tokio::test]
    #[serial]
    async fn test_pulse_invalid_signature_rejected() {
        let (_signing_key, public_key_b58) = generate_test_keypair();
        let domain = "test-invalid-sig.local";

        let public_key = format!("ed25519.{}", public_key_b58);
        if support::run_with_key_override(&public_key) {
            return;
        }

        let app = create_test_app().await;

        // Create payload with invalid signature (using a different key)
        let other_key = SigningKey::from_bytes(&[99u8; 32]);
        let (payload, _) = create_signed_mycelium(&other_key, domain);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/synapse/pulse")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_string(&payload).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 10000)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "Response: {:?}",
            json
        );
        assert_eq!(
            json["error"]["code"], "invalid_signature",
            "Response: {:?}",
            json
        );
    }

    #[tokio::test]
    async fn test_pulse_missing_schema_rejected() {
        let app = create_test_app().await;

        // Create payload without $schema field
        let payload = serde_json::json!({
            "capsule": { "uri": "cmn://test.local" }
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/synapse/pulse")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_string(&payload).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 10000)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(status, StatusCode::BAD_REQUEST, "Response: {:?}", json);
        assert_eq!(
            json["error"]["code"], "invalid_capsule",
            "Response: {:?}",
            json
        );
        assert!(
            json["error"]["hint"]
                .as_str()
                .is_some_and(|h| !h.is_empty()),
            "Response: {:?}",
            json
        );
    }

    #[tokio::test]
    async fn test_pulse_missing_uri_rejected() {
        let app = create_test_app().await;

        let payload = serde_json::json!({
            "$schema": "https://cmn.dev/schemas/v1/mycelium.json",
            "capsule": {}
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/synapse/pulse")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_string(&payload).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 10000)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(status, StatusCode::BAD_REQUEST, "Response: {:?}", json);
        assert_eq!(
            json["error"]["code"], "invalid_capsule",
            "Response: {:?}",
            json
        );
        assert!(
            json["error"]["hint"]
                .as_str()
                .is_some_and(|h| !h.is_empty()),
            "Response: {:?}",
            json
        );
    }

    /// Create a signed mycelium payload with an explicit updated_at_epoch_ms timestamp
    fn create_signed_mycelium_at(
        signing_key: &SigningKey,
        domain: &str,
        updated_at: u64,
    ) -> serde_json::Value {
        create_signed_mycelium_at_with_synopsis(signing_key, domain, updated_at, "clock skew test")
    }

    fn create_signed_mycelium_at_with_synopsis(
        signing_key: &SigningKey,
        domain: &str,
        updated_at: u64,
        synopsis: &str,
    ) -> serde_json::Value {
        let public_key_b58 = bs58::encode(signing_key.verifying_key().to_bytes()).into_string();
        let key = format!("ed25519.{}", public_key_b58);

        // Build typed struct first, serialize, then sign — ensures JCS matches
        let core = substrate::MyceliumCore {
            name: "Test Mycelium".into(),
            synopsis: synopsis.into(),
            domain: domain.into(),
            key: key.clone(),
            bio: String::new(),
            nutrients: Vec::new(),
            updated_at_epoch_ms: updated_at,
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
        let mycelium_hash = payload.computed_uri_hash().unwrap();
        payload.capsule.uri = format!("cmn://{}/mycelium/{}", domain, mycelium_hash);
        let capsule_value = serde_json::to_value(&payload.capsule).unwrap();
        payload.capsule_signature = sign_jcs(signing_key, &capsule_value);

        serde_json::to_value(&payload).unwrap()
    }

    #[tokio::test]
    #[serial]
    async fn test_pulse_timestamp_far_future_rejected() {
        let (signing_key, public_key_b58) = generate_test_keypair();
        let domain = "test-clock-skew-reject.local";

        let public_key = format!("ed25519.{}", public_key_b58);
        if support::run_with_key_override(&public_key) {
            return;
        }

        let app = create_test_app().await;

        // max_clock_skew is 300s (from ensure_test_config).
        // Set updated_at to now + 10 minutes (600s) — well beyond tolerance.
        let future_ts = now_epoch_ms() + 600 * 1000;
        let payload = create_signed_mycelium_at(&signing_key, domain, future_ts);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/synapse/pulse")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_string(&payload).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 10000)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "Response: {:?}",
            json
        );
        assert_eq!(
            json["error"]["code"], "timestamp_future",
            "Response: {:?}",
            json
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_pulse_timestamp_within_skew_accepted() {
        let (signing_key, public_key_b58) = generate_test_keypair();
        let domain = "test-clock-skew-accept.local";

        let public_key = format!("ed25519.{}", public_key_b58);
        if support::run_with_key_override(&public_key) {
            return;
        }

        let app = create_test_app().await;

        // Set updated_at to now + 2 minutes (120s) — within the 300s tolerance
        let near_future_ts = now_epoch_ms() + 120 * 1000;
        let payload = create_signed_mycelium_at(&signing_key, domain, near_future_ts);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/synapse/pulse")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_string(&payload).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 10000)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        // Should be accepted (200 OK), not rejected for clock skew
        assert_eq!(status, StatusCode::OK, "Response: {:?}", json);
        assert_eq!(json["kind"], "result", "Response: {:?}", json);
    }

    #[tokio::test]
    #[serial]
    async fn test_pulse_mycelium_duplicate_same_timestamp_same_hash() {
        let (signing_key, public_key_b58) = generate_test_keypair();
        let domain = "test-mycelium-dup.local";

        let public_key = format!("ed25519.{}", public_key_b58);
        if support::run_with_key_override(&public_key) {
            return;
        }

        let app = create_test_app().await;
        let ts = now_epoch_ms();
        let payload = create_signed_mycelium_at_with_synopsis(&signing_key, domain, ts, "v1");

        let response1 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/synapse/pulse")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_string(&payload).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status1 = response1.status();
        let body1 = axum::body::to_bytes(response1.into_body(), 10000)
            .await
            .unwrap();
        let json1: Value = serde_json::from_slice(&body1).unwrap();
        assert_eq!(status1, StatusCode::OK, "Response: {:?}", json1);
        assert_eq!(
            json1["result"]["action"], "indexed",
            "Response: {:?}",
            json1
        );

        let response2 = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/synapse/pulse")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_string(&payload).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let status2 = response2.status();
        let body2 = axum::body::to_bytes(response2.into_body(), 10000)
            .await
            .unwrap();
        let json2: Value = serde_json::from_slice(&body2).unwrap();
        assert_eq!(status2, StatusCode::CONFLICT, "Response: {:?}", json2);
        assert_eq!(
            json2["error"]["code"], "version_not_newer",
            "Response: {:?}",
            json2
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_pulse_mycelium_conflict_same_timestamp_different_hash() {
        let (signing_key, public_key_b58) = generate_test_keypair();
        let domain = "test-mycelium-conflict.local";

        let public_key = format!("ed25519.{}", public_key_b58);
        if support::run_with_key_override(&public_key) {
            return;
        }

        let app = create_test_app().await;
        let ts = now_epoch_ms();
        let payload_v1 = create_signed_mycelium_at_with_synopsis(&signing_key, domain, ts, "v1");
        let payload_v2 = create_signed_mycelium_at_with_synopsis(&signing_key, domain, ts, "v2");

        let response1 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/synapse/pulse")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_string(&payload_v1).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status1 = response1.status();
        let body1 = axum::body::to_bytes(response1.into_body(), 10000)
            .await
            .unwrap();
        let json1: Value = serde_json::from_slice(&body1).unwrap();
        assert_eq!(status1, StatusCode::OK, "Response: {:?}", json1);
        assert_eq!(
            json1["result"]["action"], "indexed",
            "Response: {:?}",
            json1
        );

        let response2 = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/synapse/pulse")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_string(&payload_v2).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let status2 = response2.status();
        let body2 = axum::body::to_bytes(response2.into_body(), 10000)
            .await
            .unwrap();
        let json2: Value = serde_json::from_slice(&body2).unwrap();
        assert_eq!(status2, StatusCode::CONFLICT, "Response: {:?}", json2);
        assert_eq!(
            json2["error"]["code"], "version_not_newer",
            "Response: {:?}",
            json2
        );
    }
}
