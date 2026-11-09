#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use crate::config::{CacheConfig, NostrConfig, PulseConfig};
use crate::crawler::Crawler;
use crate::nostr::{NostrBridge, prune_unbacked_nostr_events};
use crate::storage::{Storage, StorageBackend};
use nostr_sdk::prelude::{Event, EventBuilder, FinalizeEvent, Keys, Kind, Tag};
use std::sync::Arc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

enum Fixture {
    Redb(tempfile::TempDir),
    #[cfg(feature = "postgres")]
    Postgres {
        admin: sqlx::PgPool,
        schema: String,
        url_secret: String,
    },
}

impl Fixture {
    fn redb() -> (Self, StorageBackend) {
        let directory = tempfile::tempdir().unwrap();
        let storage =
            crate::storage::RedbStorage::open(directory.path().join("data.redb")).unwrap();
        (Self::Redb(directory), StorageBackend::Redb(storage))
    }

    #[cfg(feature = "postgres")]
    async fn postgres() -> (Self, StorageBackend) {
        let url_secret = std::env::var("SYNAPSE_TEST_POSTGRES_URL_SECRET")
            .expect("use scripts/test-postgres.sh to provide a real PostgreSQL fixture");
        let admin = sqlx::PgPool::connect(&url_secret).await.unwrap();
        let schema = format!("synapse_test_{}", crate::time::now_epoch_ms());
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let mut url = reqwest::Url::parse(&url_secret).unwrap();
        url.query_pairs_mut()
            .append_pair("options", &format!("-c search_path={schema}"));
        let url_secret = url.to_string();
        let storage = crate::storage::PostgresStorage::new(&url_secret)
            .await
            .unwrap();
        (
            Self::Postgres {
                admin,
                schema,
                url_secret,
            },
            StorageBackend::Postgres(storage),
        )
    }

    async fn restart(&self, storage: StorageBackend) -> StorageBackend {
        drop(storage);
        match self {
            Self::Redb(directory) => StorageBackend::Redb(
                crate::storage::RedbStorage::open(directory.path().join("data.redb")).unwrap(),
            ),
            #[cfg(feature = "postgres")]
            Self::Postgres { url_secret, .. } => StorageBackend::Postgres(
                crate::storage::PostgresStorage::new(url_secret)
                    .await
                    .unwrap(),
            ),
        }
    }

    async fn finish(self) {
        #[cfg(feature = "postgres")]
        if let Self::Postgres { admin, schema, .. } = self {
            sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
                .execute(&admin)
                .await
                .unwrap();
            admin.close().await;
        }
    }
}

fn config(capacity: usize) -> NostrConfig {
    NostrConfig {
        enabled: true,
        relays: vec![],
        key_store: "storage".into(),
        subscribe: false,
        forward_pulse: false,
        max_events: capacity,
        max_ws_message_bytes: 262_144,
    }
}

fn event(content: serde_json::Value) -> Event {
    EventBuilder::new(Kind::Custom(30078), content.to_string())
        .tag(Tag::hashtag("cmn-spore"))
        .finalize(&Keys::generate())
        .unwrap()
}

async fn capacity_cleanup_contract(fixture: &Fixture, storage: StorageBackend) {
    assert!(
        NostrBridge::from_config(&config(0), &storage, 1_048_576)
            .await
            .is_err()
    );
    let bridge = NostrBridge::from_config(&config(3), &storage, 1_048_576)
        .await
        .unwrap();
    for sequence in 0..9 {
        assert!(
            bridge
                .store_event(&event(serde_json::json!({"sequence":sequence})))
                .await
                .unwrap()
        );
    }
    let persisted = storage.get_nostr_events().await.unwrap();
    assert_eq!(persisted.len(), 3);
    let ids: std::collections::HashSet<_> = bridge
        .events()
        .read()
        .await
        .iter()
        .map(|e| e.id.to_hex())
        .collect();
    assert_eq!(
        ids,
        persisted
            .iter()
            .map(|bytes| Event::from_json(bytes).unwrap().id.to_hex())
            .collect()
    );
    drop(bridge);
    let storage = fixture.restart(storage).await;
    let bridge = NostrBridge::from_config(&config(3), &storage, 1_048_576)
        .await
        .unwrap();
    assert_eq!(
        ids,
        bridge
            .events()
            .read()
            .await
            .iter()
            .map(|e| e.id.to_hex())
            .collect()
    );
    drop(bridge);

    // A legacy oversized window is trimmed before either memory or serving loads it.
    for sequence in 10..19 {
        let event = event(serde_json::json!({"sequence":sequence}));
        storage
            .save_nostr_event(&event.id.to_hex(), event.as_json().as_bytes())
            .await
            .unwrap();
    }
    let bridge = NostrBridge::from_config(&config(3), &storage, 1_048_576)
        .await
        .unwrap();
    assert_eq!(bridge.events().read().await.len(), 3);
    assert_eq!(storage.get_nostr_events().await.unwrap().len(), 3);
    let retained: std::collections::HashSet<_> =
        bridge.events().read().await.iter().map(|e| e.id).collect();
    drop(bridge);
    let storage = fixture.restart(storage).await;
    let bridge = NostrBridge::from_config(&config(3), &storage, 1_048_576)
        .await
        .unwrap();
    assert_eq!(
        retained,
        bridge.events().read().await.iter().map(|e| e.id).collect()
    );
    drop(bridge);

    // Replicas have different host capsules. Cleanup must compare the correct host.
    for domain in ["first.test", "second.test"] {
        let content =
            serde_json::json!({"capsule":{"uri":format!("cmn://{domain}/b3.same"),"host":domain}});
        storage
            .save_spore(
                "b3.same",
                domain,
                &serde_json::to_vec(&content).unwrap(),
                &[],
            )
            .await
            .unwrap();
        let event = event(content);
        storage
            .save_nostr_event(&event.id.to_hex(), event.as_json().as_bytes())
            .await
            .unwrap();
    }
    storage.save_nostr_event("malformed", b"{}").await.unwrap();
    let (retained_count, removed_count) = prune_unbacked_nostr_events(&storage).await.unwrap();
    assert_eq!(retained_count, 2);
    assert_eq!(removed_count, 4);
    let storage = fixture.restart(storage).await;
    let bridge = NostrBridge::from_config(&config(10), &storage, 1_048_576)
        .await
        .unwrap();
    assert_eq!(bridge.events().read().await.len(), 2);
    assert_eq!(prune_unbacked_nostr_events(&storage).await.unwrap(), (2, 0));
}

#[tokio::test]
async fn redb_capacity_cleanup_restart() {
    let (fixture, storage) = Fixture::redb();
    capacity_cleanup_contract(&fixture, storage).await;
    fixture.finish().await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn postgres_contract_capacity_cleanup_restart() {
    let (fixture, storage) = Fixture::postgres().await;
    capacity_cleanup_contract(&fixture, storage).await;
    fixture.finish().await;
}

fn keypair() -> ([u8; 32], String) {
    let private = [37; 32];
    let key = ed25519_dalek::SigningKey::from_bytes(&private);
    (
        private,
        substrate::format_key(
            substrate::KeyAlgorithm::Ed25519,
            &key.verifying_key().to_bytes(),
        ),
    )
}

struct PartialSearch;

#[async_trait::async_trait]
impl crate::search::Search for PartialSearch {
    async fn index_spore(
        &self,
        _domain: &str,
        hash: &str,
        _value: &serde_json::Value,
    ) -> anyhow::Result<()> {
        if hash == "b3.fail" {
            anyhow::bail!("embedding fixture failed");
        }
        Ok(())
    }
    fn remove_spore(&self, _hash: &str) {}
    async fn search(
        &self,
        _query: &crate::search::SearchQuery,
    ) -> anyhow::Result<Vec<crate::search::SearchResult>> {
        Ok(vec![])
    }
}

#[tokio::test]
async fn rebuild_counts_parse_embedding_and_storage_failures() {
    let (fixture, storage) = Fixture::redb();
    storage
        .save_mycelium("search.test", b"{}", "key", "hash")
        .await
        .unwrap();
    for hash in ["b3.good", "b3.fail"] {
        storage
            .save_spore(hash, "search.test", b"{}", &[])
            .await
            .unwrap();
    }
    storage
        .save_spore("b3.badjson", "search.test", b"broken JSON", &[])
        .await
        .unwrap();
    assert_eq!(
        crate::search::rebuild_search(&storage, &PartialSearch).await,
        crate::search::RebuildStatistics {
            indexed_count: 1,
            failed_count: 2
        }
    );
    let StorageBackend::Redb(redb) = &storage else {
        unreachable!()
    };
    redb.remove_test_table("spores").unwrap();
    assert_eq!(
        crate::search::rebuild_search(&storage, &PartialSearch).await,
        crate::search::RebuildStatistics {
            indexed_count: 0,
            failed_count: 3
        }
    );
    redb.remove_test_table("domains").unwrap();
    assert_eq!(
        crate::search::rebuild_search(&storage, &PartialSearch)
            .await
            .failed_count,
        1
    );
    redb.remove_test_table("mycelia").unwrap();
    assert_eq!(
        crate::search::rebuild_search(&storage, &PartialSearch)
            .await
            .failed_count,
        1
    );
    drop(storage);
    fixture.finish().await;
}

fn sign<T: serde::Serialize>(value: &T, private: &[u8; 32]) -> String {
    substrate::compute_signature(value, substrate::SignatureAlgorithm::Ed25519, private).unwrap()
}

#[tokio::test]
async fn invalid_domain_identity_does_not_fall_back_to_cached_storage_key() {
    let (fixture, storage) = Fixture::redb();
    let server = MockServer::start().await;
    let domain = "identity.test.onion";
    let (private, key) = keypair();
    storage
        .save_mycelium(domain, b"{}", &key, "b3.cached")
        .await
        .unwrap();
    let capsules = vec![substrate::CmnCapsuleEntry {
        uri: substrate::build_domain_uri(domain),
        serial: 1,
        key: key.clone(),
        history: vec![],
        endpoints: vec![],
    }];
    let invalid = substrate::CmnEntry {
        schema: substrate::CMN_SCHEMA.into(),
        capsule_signature: sign(&capsules, &[99; 32]),
        capsules: capsules.clone(),
    };
    Mock::given(path("/.well-known/cmn.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(invalid))
        .mount(&server)
        .await;
    let http = reqwest::Client::builder()
        .no_proxy()
        .resolve(domain, *server.address())
        .build()
        .unwrap();
    let cache = crate::cmn_cache::CmnCache::new(300, 256 * 1024, http);
    assert!(
        cache
            .resolve_key(domain, &storage)
            .await
            .unwrap_err()
            .downcast_ref::<crate::cmn_cache::DomainVerificationFailure>()
            .is_some()
    );
    server.reset().await;
    let valid = substrate::CmnEntry {
        schema: substrate::CMN_SCHEMA.into(),
        capsule_signature: sign(&capsules, &private),
        capsules,
    };
    Mock::given(path("/.well-known/cmn.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(valid))
        .mount(&server)
        .await;
    assert!(
        cache
            .resolve_confirmed_key(
                domain,
                "ed25519.unconfirmed",
                crate::time::now_epoch_ms(),
                &storage
            )
            .await
            .unwrap_err()
            .downcast_ref::<crate::cmn_cache::DomainVerificationFailure>()
            .is_some()
    );

    // A valid host signature does not authorize an unconfirmed embedded author key.
    // Exercise the ingestion boundary so this cannot become a retryable fetch error.
    let mut unconfirmed = spore(domain, "b3.missing");
    let author = ed25519_dalek::SigningKey::from_bytes(&[99; 32]);
    unconfirmed.capsule.core.key = substrate::format_key(
        substrate::KeyAlgorithm::Ed25519,
        &author.verifying_key().to_bytes(),
    );
    unconfirmed.capsule.core_signature = sign(&unconfirmed.capsule.core, &[99; 32]);
    unconfirmed.capsule.dist.push(substrate::SporeDist {
        kind: substrate::DistKind::Archive,
        filename: Some("fixture.tar.zst".into()),
        url: None,
        git_ref: None,
        cid: None,
        extra: Default::default(),
    });
    unconfirmed.capsule_signature = sign(&unconfirmed.capsule, &private);
    let payload = serde_json::to_value(unconfirmed).unwrap();
    let error =
        crate::ingest::validate_and_verify(&payload, &cache, &storage, 60_000, 60_000, false)
            .await
            .err()
            .unwrap();
    assert!(
        matches!(
            &error,
            crate::ingest::IngestError::SignatureInvalid {
                fallback_hint: None,
                ..
            }
        ),
        "expected an invalid identity rather than a retryable fetch failure: {error:?}"
    );
    drop(storage);
    fixture.finish().await;
}

fn mycelium(domain: &str, hashes: &[&str], timestamp: u64) -> substrate::Mycelium {
    let (private, key) = keypair();
    let mut manifest = substrate::Mycelium::new(domain, "Fixture", "Crawler regression", timestamp);
    manifest.capsule.core.key = key;
    for hash in hashes {
        manifest.add_spore(hash, hash, hash, None, timestamp);
    }
    manifest.capsule.core_signature = sign(&manifest.capsule.core, &private);
    manifest.capsule.uri =
        substrate::build_mycelium_uri(domain, &manifest.computed_uri_hash().unwrap());
    manifest.capsule_signature = sign(&manifest.capsule, &private);
    manifest
}

fn spore(domain: &str, hash: &str) -> substrate::Spore {
    let (private, key) = keypair();
    let core = substrate::SporeCore {
        id: hash.into(),
        name: hash.into(),
        version: String::new(),
        domain: domain.into(),
        key,
        synopsis: "Fixture".into(),
        intent: vec![],
        license: "MIT".into(),
        mutations: vec![],
        size_bytes: 0,
        updated_at_epoch_ms: crate::time::now_epoch_ms(),
        bonds: vec![],
        tree: substrate::SporeTree {
            algorithm: "blob_tree_blake3_nfc".into(),
            exclude_names: vec![],
            follow_rules: vec![],
        },
    };
    let core_signature = sign(&core, &private);
    let capsule = substrate::SporeCapsule {
        uri: substrate::build_spore_uri(domain, hash),
        core,
        core_signature,
        dist: vec![],
    };
    let capsule_signature = sign(&capsule, &private);
    substrate::Spore {
        schema: substrate::SPORE_SCHEMA.into(),
        capsule,
        capsule_signature,
    }
}

async fn mount_entry(server: &MockServer, domain: &str, manifest: &substrate::Mycelium) {
    let (private, key) = keypair();
    let capsules = vec![substrate::CmnCapsuleEntry {
        uri: substrate::build_domain_uri(domain),
        serial: 1,
        key,
        history: vec![],
        endpoints: vec![
            substrate::CmnEndpoint {
                kind: "mycelium".into(),
                url: format!("http://{domain}/mycelium/{{hash}}"),
                hash: manifest.computed_uri_hash().unwrap(),
                hashes: vec![],
                format: None,
                delta_url: None,
            },
            substrate::CmnEndpoint {
                kind: "spore".into(),
                url: format!("http://{domain}/spore/{{hash}}"),
                hash: String::new(),
                hashes: vec![],
                format: None,
                delta_url: None,
            },
        ],
    }];
    let capsule_signature = sign(&capsules, &private);
    let entry = substrate::CmnEntry {
        schema: substrate::CMN_SCHEMA.into(),
        capsules,
        capsule_signature,
    };
    Mock::given(method("GET"))
        .and(path("/.well-known/cmn.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&entry))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!(
            "/mycelium/{}",
            manifest.computed_uri_hash().unwrap()
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(manifest))
        .mount(server)
        .await;
}

fn make_crawler(storage: &StorageBackend, server: &MockServer, domain: &str) -> Crawler {
    let http = reqwest::Client::builder()
        .no_proxy()
        .resolve(domain, *server.address())
        .build()
        .unwrap();
    let cache = Arc::new(crate::cmn_cache::CmnCache::new(
        300,
        256 * 1024,
        http.clone(),
    ));
    Crawler::new(
        storage.clone(),
        cache,
        http,
        &CacheConfig::default(),
        &PulseConfig::default(),
        None,
        None,
    )
}

fn make_app_state(
    storage: StorageBackend,
    server: &MockServer,
    domain: &str,
) -> Arc<crate::handlers::AppState> {
    let config: crate::config::AppConfig = serde_json::from_value(serde_json::json!({
        "debug":false,"storage":{"backend":"redb","redb_path":"unused"},
        "http":{"enabled":true,"address":"127.0.0.1:0"}
    }))
    .unwrap();
    let http = reqwest::Client::builder()
        .no_proxy()
        .resolve(domain, *server.address())
        .build()
        .unwrap();
    let cmn_cache = Arc::new(crate::cmn_cache::CmnCache::new(
        300,
        256 * 1024,
        http.clone(),
    ));
    let crawler = Crawler::new(
        storage.clone(),
        cmn_cache.clone(),
        http,
        &config.cache,
        &config.pulse,
        None,
        None,
    );
    Arc::new(crate::handlers::AppState {
        config: Arc::new(config),
        storage,
        crawler,
        cmn_cache,
        rate_state: std::sync::atomic::AtomicU64::new(0),
        nostr_relay: None,
        search: None,
        graph: None,
    })
}

async fn retry_record(storage: &StorageBackend, domain: &str) -> Option<(String, i32, bool, bool)> {
    match storage {
        StorageBackend::Redb(storage) => storage.retry_record(domain).unwrap(),
        #[cfg(feature = "postgres")]
        StorageBackend::Postgres(storage) => storage.retry_record(domain).await.unwrap(),
    }
}

async fn crawler_contract(fixture: &Fixture, storage: StorageBackend) {
    let domain = "crawler.test.onion";
    let server = MockServer::start().await;
    let manifest = mycelium(
        domain,
        &["b3.retry", "b3.gone", "b3.bad", "b3.wrong"],
        crate::time::now_epoch_ms(),
    );
    mount_entry(&server, domain, &manifest).await;
    Mock::given(path("/spore/b3.retry"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    Mock::given(path("/spore/b3.gone"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    let mut invalid = spore(domain, "b3.bad");
    invalid.capsule.core.synopsis = "tampered".into();
    Mock::given(path("/spore/b3.bad"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&invalid))
        .mount(&server)
        .await;
    Mock::given(path("/spore/b3.wrong"))
        .respond_with(ResponseTemplate::new(200).set_body_json(spore(domain, "b3.another")))
        .mount(&server)
        .await;
    let crawler = make_crawler(&storage, &server, domain);
    assert!(crawler.crawl_domain(domain).await.is_err());
    assert_eq!(
        retry_record(&storage, domain).await,
        Some(("failed".into(), 1, true, true))
    );
    assert!(storage.get_mycelium(domain).await.unwrap().is_some());
    assert_eq!(
        storage.get_mycelium_hash(domain).await.unwrap(),
        Some(manifest.computed_uri_hash().unwrap())
    );
    drop(crawler);
    let storage = fixture.restart(storage).await;
    assert_eq!(
        retry_record(&storage, domain).await,
        Some(("failed".into(), 1, true, true))
    );

    server.reset().await;
    mount_entry(&server, domain, &manifest).await;
    Mock::given(path("/spore/b3.retry"))
        .respond_with(ResponseTemplate::new(200).set_body_json(spore(domain, "b3.retry")))
        .mount(&server)
        .await;
    let crawler = make_crawler(&storage, &server, domain);
    // An unchanged manifest must complete missing content, without re-fetching its manifest.
    crawler.crawl_domain(domain).await.unwrap();
    assert!(storage.get_spore("b3.retry").await.unwrap().is_some());
    assert!(storage.get_spore("b3.gone").await.unwrap().is_none());
    assert!(storage.get_spore("b3.bad").await.unwrap().is_none());
    assert!(storage.get_spore("b3.wrong").await.unwrap().is_none());
    assert_eq!(
        retry_record(&storage, domain).await,
        Some(("synced".into(), 0, false, false))
    );
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| !request.url.path().starts_with("/mycelium/"))
    );
    drop(crawler);
    let storage = fixture.restart(storage).await;
    assert_eq!(
        retry_record(&storage, domain).await,
        Some(("synced".into(), 0, false, false))
    );

    // Exercise the actual ingestion spawn, rather than calling the crawler manually.
    let pulse_manifest = mycelium(domain, &["b3.post"], crate::time::now_epoch_ms() + 1);
    Mock::given(path("/spore/b3.post"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let state = make_app_state(storage.clone(), &server, domain);
    let payload = serde_json::to_value(&pulse_manifest).unwrap();
    let verified = crate::ingest::validate_and_verify(
        &payload,
        &state.cmn_cache,
        &state.storage,
        60_000,
        60_000,
        false,
    )
    .await
    .unwrap();
    assert_eq!(
        crate::ingest::store(
            &state,
            &verified,
            &crate::ingest::StoreOptions {
                forward_nostr: false
            }
        )
        .await
        .unwrap(),
        "updated"
    );
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if retry_record(&storage, domain).await == Some(("failed".into(), 1, true, true)) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    drop(state);
    let storage = fixture.restart(storage).await;
    assert_eq!(
        retry_record(&storage, domain).await,
        Some(("failed".into(), 1, true, true))
    );
    server.reset().await;
    mount_entry(&server, domain, &manifest).await;
    Mock::given(path("/spore/b3.post"))
        .respond_with(ResponseTemplate::new(200).set_body_json(spore(domain, "b3.post")))
        .mount(&server)
        .await;
    // The entry still advertises an older manifest. Recover the newer local inventory.
    make_crawler(&storage, &server, domain)
        .crawl_domain(domain)
        .await
        .unwrap();
    assert!(storage.get_spore("b3.post").await.unwrap().is_some());
    assert_eq!(
        storage.get_mycelium_hash(domain).await.unwrap(),
        Some(pulse_manifest.computed_uri_hash().unwrap())
    );
    assert_eq!(
        retry_record(&storage, domain).await,
        Some(("synced".into(), 0, false, false))
    );
}

#[tokio::test]
async fn redb_crawler_retry_recovery_restart() {
    let (fixture, storage) = Fixture::redb();
    crawler_contract(&fixture, storage).await;
    fixture.finish().await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn postgres_contract_crawler_retry_recovery_restart() {
    let (fixture, storage) = Fixture::postgres().await;
    crawler_contract(&fixture, storage).await;
    fixture.finish().await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn postgres_contract_storage_failure_keeps_memory_and_fails_startup() {
    let (fixture, storage) = Fixture::postgres().await;
    let bridge = NostrBridge::from_config(&config(3), &storage, 1_048_576)
        .await
        .unwrap();
    let Fixture::Postgres { url_secret, .. } = &fixture else {
        unreachable!()
    };
    let pool = sqlx::PgPool::connect(url_secret).await.unwrap();
    sqlx::query("DROP TABLE nostr_events")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        bridge
            .store_event(&event(serde_json::json!({"test":"write failure"})))
            .await
            .is_err()
    );
    assert!(bridge.events().read().await.is_empty());
    assert!(
        NostrBridge::from_config(&config(3), &storage, 1_048_576)
            .await
            .is_err()
    );
    // A failed database fallback must retain its infrastructure cause rather than
    // being converted into a permanent content or identity verification failure.
    sqlx::query("DROP TABLE mycelia")
        .execute(&pool)
        .await
        .unwrap();
    let server = MockServer::start().await;
    Mock::given(path("/.well-known/cmn.json"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let domain = "fallback.test.onion";
    let http = reqwest::Client::builder()
        .no_proxy()
        .resolve(domain, *server.address())
        .build()
        .unwrap();
    let cache = crate::cmn_cache::CmnCache::new(300, 256 * 1024, http);
    let error = cache.resolve_key(domain, &storage).await.unwrap_err();
    assert!(error.downcast_ref::<sqlx::Error>().is_some());
    let error = cache
        .resolve_confirmed_key(domain, &keypair().1, crate::time::now_epoch_ms(), &storage)
        .await
        .unwrap_err();
    assert!(error.downcast_ref::<sqlx::Error>().is_some());
    assert!(
        error
            .downcast_ref::<crate::cmn_cache::DomainVerificationFailure>()
            .is_none()
    );
    pool.close().await;
    drop(bridge);
    drop(storage);
    fixture.finish().await;
}
