#![cfg(feature = "ruvector")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use axum::body::Body;
use axum::http::Request;
use serde_json::Value;
use std::sync::Arc;
use substrate::BondRelation;
use synapse::config::{EmbeddingConfig, SearchConfig};
use synapse::search::{Search, SearchBackend, SearchQuery};
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Fake embedding responder — deterministic vectors from input text
// ---------------------------------------------------------------------------

/// Generate a deterministic unit-norm embedding from text.
/// Texts with overlapping words will have higher cosine similarity.
fn fake_embed(text: &str, dims: usize) -> Vec<f32> {
    let bytes = text.as_bytes();
    let mut vec = vec![0.0f32; dims];
    for (i, &b) in bytes.iter().enumerate() {
        vec[i % dims] += b as f32;
    }
    let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for v in &mut vec {
            *v /= norm;
        }
    }
    vec
}

/// Wiremock responder that mimics Ollama's POST /api/embed endpoint.
struct OllamaEmbedResponder {
    dimensions: usize,
}

impl wiremock::Respond for OllamaEmbedResponder {
    fn respond(&self, request: &wiremock::Request) -> ResponseTemplate {
        let body: Value = serde_json::from_slice(&request.body).unwrap_or_default();
        let inputs = body["input"].as_array().cloned().unwrap_or_default();
        let embeddings: Vec<Vec<f32>> = inputs
            .iter()
            .map(|input| {
                let text = input.as_str().unwrap_or("");
                fake_embed(text, self.dimensions)
            })
            .collect();

        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "embeddings": embeddings,
        }))
    }
}

// ---------------------------------------------------------------------------
// Test data helpers
// ---------------------------------------------------------------------------

const DIMS: usize = 32;

fn make_spore_json(name: &str, description: &str, domain: &str, license: &str) -> Value {
    make_spore_json_with_refs(name, description, domain, license, vec![])
}

fn make_spore_json_with_refs(
    name: &str,
    description: &str,
    domain: &str,
    license: &str,
    bonds: Vec<Value>,
) -> Value {
    serde_json::json!({
        "$schema": "https://cmn.dev/schemas/v1/spore.json",
        "capsule": {
            "uri": format!("cmn://{}/b3.{}", domain, name),
            "core": {
                "name": name,
                "description": description,
                "domain": domain,
                "license": license,
                "synopsis": description,
                "intent": ["testing"],
                "bonds": bonds,
                "tree": { "algorithm": "blob_tree_blake3_nfc" }
            },
            "core_signature": "ed25519.test",
            "dist": [{"type": "archive", "filename": "test.tar.zst"}]
        },
        "capsule_signature": "ed25519.test"
    })
}

async fn create_search_engine(mock_url: &str) -> synapse::search::RuvectorSearch {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = SearchConfig {
        enabled: true,
        index_path: temp_dir
            .path()
            .join("search-idx")
            .to_string_lossy()
            .to_string(),
        graph: Default::default(),
        embedding: EmbeddingConfig {
            provider: "ollama".to_string(),
            url: mock_url.to_string(),
            model: "test-embed".to_string(),
            api_key_secret: None,
            dimensions: DIMS,
        },
    };
    synapse::search::RuvectorSearch::new(&config).unwrap()
}

/// Five sample spores spanning two domains and two licenses.
fn sample_spores() -> Vec<(
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
)> {
    // (hash, name, description, domain, license)
    vec![
        (
            "b3.aaa",
            "reqwest-lite",
            "Lightweight HTTP client for Rust with async support",
            "cmn.dev",
            "MIT",
        ),
        (
            "b3.bbb",
            "tokio-retry",
            "Retry middleware for Tokio async tasks with backoff",
            "cmn.dev",
            "Apache-2.0",
        ),
        (
            "b3.ccc",
            "serde-csv",
            "Fast CSV serializer and deserializer using serde",
            "tools.rs",
            "MIT",
        ),
        (
            "b3.ddd",
            "axum-guard",
            "Authorization guard middleware for Axum HTTP server",
            "tools.rs",
            "Apache-2.0",
        ),
        (
            "b3.eee",
            "hyper-client",
            "Low-level HTTP client built on Hyper with connection pooling",
            "cmn.dev",
            "MIT",
        ),
    ]
}

// ---------------------------------------------------------------------------
// Tests — search engine layer
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_index_and_search_returns_results() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/embed"))
        .respond_with(OllamaEmbedResponder { dimensions: DIMS })
        .mount(&mock_server)
        .await;

    let engine = create_search_engine(&mock_server.uri()).await;

    // Index all sample spores
    for (hash, name, desc, domain, license) in sample_spores() {
        let json = make_spore_json(name, desc, domain, license);
        engine.index_spore(domain, hash, &json).await.unwrap();
    }

    // Search for "HTTP client" — should return results
    let results = engine
        .search(&SearchQuery {
            offset: 0,
            text: "HTTP client".to_string(),
            domain: None,
            license: None,
            bonds: vec![],
            strain: None,
            updated_after: None,
            updated_before: None,
            limit: 10,
        })
        .await
        .unwrap();

    assert!(!results.is_empty(), "Search should return results");
    assert!(results.len() <= 5, "Should not exceed indexed count");

    // Every result should have non-empty fields
    for r in &results {
        assert!(!r.uri.is_empty());
        assert!(!r.domain.is_empty());
        assert!(!r.name.is_empty());
    }
}

#[tokio::test]
async fn test_search_domain_filter() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/embed"))
        .respond_with(OllamaEmbedResponder { dimensions: DIMS })
        .mount(&mock_server)
        .await;

    let engine = create_search_engine(&mock_server.uri()).await;

    for (hash, name, desc, domain, license) in sample_spores() {
        let json = make_spore_json(name, desc, domain, license);
        engine.index_spore(domain, hash, &json).await.unwrap();
    }

    // Filter by domain "tools.rs"
    let results = engine
        .search(&SearchQuery {
            offset: 0,
            text: "middleware".to_string(),
            domain: Some("tools.rs".to_string()),
            license: None,
            bonds: vec![],
            strain: None,
            updated_after: None,
            updated_before: None,
            limit: 10,
        })
        .await
        .unwrap();

    for r in &results {
        assert_eq!(
            r.domain, "tools.rs",
            "Domain filter should restrict results"
        );
    }
}

#[tokio::test]
async fn test_search_license_filter() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/embed"))
        .respond_with(OllamaEmbedResponder { dimensions: DIMS })
        .mount(&mock_server)
        .await;

    let engine = create_search_engine(&mock_server.uri()).await;

    for (hash, name, desc, domain, license) in sample_spores() {
        let json = make_spore_json(name, desc, domain, license);
        engine.index_spore(domain, hash, &json).await.unwrap();
    }

    // Filter by license "Apache-2.0"
    let results = engine
        .search(&SearchQuery {
            offset: 0,
            text: "async".to_string(),
            license: Some("Apache-2.0".to_string()),
            domain: None,
            bonds: vec![],
            strain: None,
            updated_after: None,
            updated_before: None,
            limit: 10,
        })
        .await
        .unwrap();

    for r in &results {
        // Verify by checking the original data
        let hash = r.uri.rsplit('/').next().unwrap_or("");
        let spore = sample_spores()
            .into_iter()
            .find(|(h, ..)| *h == hash)
            .unwrap();
        assert_eq!(
            spore.4, "Apache-2.0",
            "License filter should restrict results"
        );
    }
}

#[tokio::test]
async fn test_search_combined_filters() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/embed"))
        .respond_with(OllamaEmbedResponder { dimensions: DIMS })
        .mount(&mock_server)
        .await;

    let engine = create_search_engine(&mock_server.uri()).await;

    for (hash, name, desc, domain, license) in sample_spores() {
        let json = make_spore_json(name, desc, domain, license);
        engine.index_spore(domain, hash, &json).await.unwrap();
    }

    // Filter: domain=cmn.dev AND license=MIT
    let results = engine
        .search(&SearchQuery {
            offset: 0,
            text: "client".to_string(),
            domain: Some("cmn.dev".to_string()),
            license: Some("MIT".to_string()),
            bonds: vec![],
            strain: None,
            updated_after: None,
            updated_before: None,
            limit: 10,
        })
        .await
        .unwrap();

    // cmn.dev + MIT = reqwest-lite and hyper-client
    for r in &results {
        assert_eq!(r.domain, "cmn.dev");
        let hash = r.uri.rsplit('/').next().unwrap_or("");
        let spore = sample_spores()
            .into_iter()
            .find(|(h, ..)| *h == hash)
            .unwrap();
        assert_eq!(spore.4, "MIT");
    }
}

#[tokio::test]
async fn test_search_limit() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/embed"))
        .respond_with(OllamaEmbedResponder { dimensions: DIMS })
        .mount(&mock_server)
        .await;

    let engine = create_search_engine(&mock_server.uri()).await;

    for (hash, name, desc, domain, license) in sample_spores() {
        let json = make_spore_json(name, desc, domain, license);
        engine.index_spore(domain, hash, &json).await.unwrap();
    }

    let results = engine
        .search(&SearchQuery {
            offset: 0,
            text: "rust".to_string(),
            domain: None,
            license: None,
            bonds: vec![],
            strain: None,
            updated_after: None,
            updated_before: None,
            limit: 2,
        })
        .await
        .unwrap();

    // Engine over-fetches by 1 for pagination probe; handler trims to exact limit
    assert!(
        results.len() <= 3,
        "Limit should cap result count (engine returns limit+1)"
    );
}

#[tokio::test]
async fn test_search_empty_index() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/embed"))
        .respond_with(OllamaEmbedResponder { dimensions: DIMS })
        .mount(&mock_server)
        .await;

    let engine = create_search_engine(&mock_server.uri()).await;

    // Search on empty index
    let results = engine
        .search(&SearchQuery {
            offset: 0,
            text: "anything".to_string(),
            domain: None,
            license: None,
            bonds: vec![],
            strain: None,
            updated_after: None,
            updated_before: None,
            limit: 10,
        })
        .await
        .unwrap();

    assert!(results.is_empty(), "Empty index should return no results");
}

#[tokio::test]
async fn test_search_scores_are_finite() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/embed"))
        .respond_with(OllamaEmbedResponder { dimensions: DIMS })
        .mount(&mock_server)
        .await;

    let engine = create_search_engine(&mock_server.uri()).await;

    for (hash, name, desc, domain, license) in sample_spores() {
        let json = make_spore_json(name, desc, domain, license);
        engine.index_spore(domain, hash, &json).await.unwrap();
    }

    let results = engine
        .search(&SearchQuery {
            offset: 0,
            text: "serde".to_string(),
            domain: None,
            license: None,
            bonds: vec![],
            strain: None,
            updated_after: None,
            updated_before: None,
            limit: 10,
        })
        .await
        .unwrap();

    for r in &results {
        assert!(r.score.is_finite(), "Score should be a finite number");
    }
}

// ---------------------------------------------------------------------------
// Tests — SearchBackend enum dispatch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_search_backend_dispatch() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/embed"))
        .respond_with(OllamaEmbedResponder { dimensions: DIMS })
        .mount(&mock_server)
        .await;

    let engine = create_search_engine(&mock_server.uri()).await;
    let backend = SearchBackend::Ruvector(engine);

    let json = make_spore_json("test-pkg", "A test package", "example.com", "MIT");
    backend
        .index_spore("example.com", "b3.test", &json)
        .await
        .unwrap();

    let results = backend
        .search(&SearchQuery {
            offset: 0,
            text: "test package".to_string(),
            domain: None,
            license: None,
            bonds: vec![],
            strain: None,
            updated_after: None,
            updated_before: None,
            limit: 5,
        })
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].uri, "cmn://example.com/b3.test");
    assert_eq!(results[0].domain, "example.com");
    assert_eq!(results[0].name, "test-pkg");
}

// ---------------------------------------------------------------------------
// Tests — HTTP handler via axum router
// ---------------------------------------------------------------------------

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

fn create_test_storage() -> synapse::storage::StorageBackend {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let storage = synapse::RedbStorage::open(db_path).unwrap();
    synapse::storage::StorageBackend::Redb(storage)
}

#[tokio::test]
async fn test_http_search_endpoint() {
    let config = test_config();

    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/embed"))
        .respond_with(OllamaEmbedResponder { dimensions: DIMS })
        .mount(&mock_server)
        .await;

    let engine = create_search_engine(&mock_server.uri()).await;
    let search = Arc::new(SearchBackend::Ruvector(engine));

    // Index some spores via the search engine directly
    for (hash, name, desc, domain, license) in sample_spores() {
        let json = make_spore_json(name, desc, domain, license);
        search.index_spore(domain, hash, &json).await.unwrap();
    }

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
        Some(search.clone()),
        None,
    );
    synapse::handlers::response::set_storage_backend("redb");
    let state = Arc::new(synapse::AppState {
        config: config.clone(),
        storage,
        crawler,
        cmn_cache,
        rate_state: std::sync::atomic::AtomicU64::new(0),
        #[cfg(feature = "nostr")]
        nostr_relay: None,
        search: Some(search),
        graph: None,
    });

    let app = synapse::create_router(state);

    // GET /v1/search?q=HTTP+client&limit=3
    let response = app
        .oneshot(
            Request::builder()
                .uri("/synapse/search?q=HTTP+client&limit=3")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body = axum::body::to_bytes(response.into_body(), 10000)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["kind"], "result");
    assert_eq!(json["result"]["query"]["text"], "HTTP client");
    assert_eq!(json["result"]["query"]["limit"], 3);

    let results = json["result"]["spores"].as_array().unwrap();
    assert!(!results.is_empty());
    assert!(results.len() <= 3);

    // Each result should have expected fields
    for r in results {
        assert!(r["uri"].is_string());
        assert!(r["domain"].is_string());
        assert!(r["name"].is_string());
        assert!(r["relevance"].is_number());
    }
}

#[tokio::test]
async fn test_http_search_with_filters() {
    let config = test_config();

    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/embed"))
        .respond_with(OllamaEmbedResponder { dimensions: DIMS })
        .mount(&mock_server)
        .await;

    let engine = create_search_engine(&mock_server.uri()).await;
    let search = Arc::new(SearchBackend::Ruvector(engine));

    for (hash, name, desc, domain, license) in sample_spores() {
        let json = make_spore_json(name, desc, domain, license);
        search.index_spore(domain, hash, &json).await.unwrap();
    }

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
        Some(search.clone()),
        None,
    );
    synapse::handlers::response::set_storage_backend("redb");
    let state = Arc::new(synapse::AppState {
        config: config.clone(),
        storage,
        crawler,
        cmn_cache,
        rate_state: std::sync::atomic::AtomicU64::new(0),
        #[cfg(feature = "nostr")]
        nostr_relay: None,
        search: Some(search),
        graph: None,
    });

    let app = synapse::create_router(state);

    // Search with domain filter
    let response = app
        .oneshot(
            Request::builder()
                .uri("/synapse/search?q=async&domain=cmn.dev")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body = axum::body::to_bytes(response.into_body(), 10000)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["kind"], "result");
    assert_eq!(json["result"]["query"]["domain"], "cmn.dev");

    let results = json["result"]["spores"].as_array().unwrap();
    for r in results {
        assert_eq!(r["domain"], "cmn.dev");
    }
}

#[tokio::test]
async fn test_http_search_unavailable_without_engine() {
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
        None,
        None,
    );
    let state = Arc::new(synapse::AppState {
        config,
        storage,
        crawler,
        cmn_cache,
        rate_state: std::sync::atomic::AtomicU64::new(0),
        #[cfg(feature = "nostr")]
        nostr_relay: None,
        search: None,
        graph: None,
    });

    let app = synapse::create_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/synapse/search?q=test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // When search is None, the route isn't registered, so 404
    // (the route is only added when state.search.is_some())
    assert_eq!(response.status(), 404);
}

// ---------------------------------------------------------------------------
// Tests — OpenAI embedding provider
// ---------------------------------------------------------------------------

/// Wiremock responder that mimics OpenAI's POST /v1/embeddings endpoint.
struct OpenAIEmbedResponder {
    dimensions: usize,
}

impl wiremock::Respond for OpenAIEmbedResponder {
    fn respond(&self, request: &wiremock::Request) -> ResponseTemplate {
        let body: Value = serde_json::from_slice(&request.body).unwrap_or_default();
        let inputs = body["input"].as_array().cloned().unwrap_or_default();
        let data: Vec<Value> = inputs
            .iter()
            .enumerate()
            .map(|(i, input)| {
                let text = input.as_str().unwrap_or("");
                let embedding = fake_embed(text, self.dimensions);
                serde_json::json!({
                    "index": i,
                    "embedding": embedding,
                    "object": "embedding",
                })
            })
            .collect();

        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": data,
            "model": "text-embedding-test",
            "usage": { "prompt_tokens": 10, "total_tokens": 10 },
        }))
    }
}

#[tokio::test]
async fn test_openai_embedding_provider() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/embeddings"))
        .respond_with(OpenAIEmbedResponder { dimensions: DIMS })
        .mount(&mock_server)
        .await;

    let temp_dir = tempfile::tempdir().unwrap();
    let config = SearchConfig {
        enabled: true,
        index_path: temp_dir
            .path()
            .join("search-idx")
            .to_string_lossy()
            .to_string(),
        graph: Default::default(),
        embedding: EmbeddingConfig {
            provider: "openai".to_string(),
            url: mock_server.uri(),
            model: "text-embedding-test".to_string(),
            api_key_secret: Some("sk-test-key".to_string()),
            dimensions: DIMS,
        },
    };

    let engine = synapse::search::RuvectorSearch::new(&config).unwrap();

    let json = make_spore_json("openai-test", "Testing OpenAI embeddings", "api.dev", "MIT");
    engine
        .index_spore("api.dev", "b3.openai", &json)
        .await
        .unwrap();

    let results = engine
        .search(&SearchQuery {
            offset: 0,
            text: "OpenAI embeddings".to_string(),
            domain: None,
            license: None,
            bonds: vec![],
            strain: None,
            updated_after: None,
            updated_before: None,
            limit: 5,
        })
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].uri, "cmn://api.dev/b3.openai");
    assert_eq!(results[0].name, "openai-test");
}

// ---------------------------------------------------------------------------
// Tests — bond filter
// ---------------------------------------------------------------------------

use synapse::search::BondFilter;

#[tokio::test]
async fn test_search_bond_filter() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/embed"))
        .respond_with(OllamaEmbedResponder { dimensions: DIMS })
        .mount(&mock_server)
        .await;

    let engine = create_search_engine(&mock_server.uri()).await;

    let parent_uri = "cmn://original.dev/b3.parent";

    // Spore WITH spawned_from reference
    let json_with_ref = make_spore_json_with_refs(
        "forked-lib",
        "A forked HTTP library with extras",
        "fork.dev",
        "MIT",
        vec![serde_json::json!({
            "uri": parent_uri,
            "relation": "spawned_from"
        })],
    );
    engine
        .index_spore("fork.dev", "b3.forked", &json_with_ref)
        .await
        .unwrap();

    // Spore WITHOUT reference
    let json_no_ref = make_spore_json("plain-lib", "A plain HTTP library", "plain.dev", "MIT");
    engine
        .index_spore("plain.dev", "b3.plain", &json_no_ref)
        .await
        .unwrap();

    // Search WITHOUT bond filter — both should appear
    let results = engine
        .search(&SearchQuery {
            offset: 0,
            text: "HTTP library".to_string(),
            domain: None,
            license: None,
            bonds: vec![],
            strain: None,
            updated_after: None,
            updated_before: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(
        results.len(),
        2,
        "Both spores should appear without bond filter"
    );

    // Search WITH bond filter — only the forked one
    let results = engine
        .search(&SearchQuery {
            offset: 0,
            text: "HTTP library".to_string(),
            domain: None,
            license: None,
            strain: None,
            updated_after: None,
            updated_before: None,
            bonds: vec![BondFilter {
                relation: BondRelation::SpawnedFrom,
                uri: parent_uri.to_string(),
            }],
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(
        results.len(),
        1,
        "Only forked spore should match bond filter"
    );
    assert_eq!(results[0].domain, "fork.dev");
}

#[tokio::test]
async fn test_search_bond_filter_multiple_and() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/embed"))
        .respond_with(OllamaEmbedResponder { dimensions: DIMS })
        .mount(&mock_server)
        .await;

    let engine = create_search_engine(&mock_server.uri()).await;

    let parent_uri = "cmn://original.dev/b3.parent";
    let profile_uri = "cmn://cmn.dev/b3.profile";

    // Spore with BOTH bonds
    let json_both = make_spore_json_with_refs(
        "full-lib",
        "A complete library with lineage and profile",
        "full.dev",
        "MIT",
        vec![
            serde_json::json!({ "uri": parent_uri, "relation": "spawned_from" }),
            serde_json::json!({ "uri": profile_uri, "relation": "follows" }),
        ],
    );
    engine
        .index_spore("full.dev", "b3.full", &json_both)
        .await
        .unwrap();

    // Spore with only spawned_from
    let json_one = make_spore_json_with_refs(
        "partial-lib",
        "A library with only lineage",
        "partial.dev",
        "MIT",
        vec![serde_json::json!({ "uri": parent_uri, "relation": "spawned_from" })],
    );
    engine
        .index_spore("partial.dev", "b3.partial", &json_one)
        .await
        .unwrap();

    // Filter with both bonds (AND) — only full-lib should match
    let results = engine
        .search(&SearchQuery {
            offset: 0,
            text: "library".to_string(),
            domain: None,
            license: None,
            strain: None,
            updated_after: None,
            updated_before: None,
            bonds: vec![
                BondFilter {
                    relation: BondRelation::SpawnedFrom,
                    uri: parent_uri.to_string(),
                },
                BondFilter {
                    relation: BondRelation::Follows,
                    uri: profile_uri.to_string(),
                },
            ],
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(
        results.len(),
        1,
        "Only spore with both bonds should match AND filter"
    );
    assert_eq!(results[0].domain, "full.dev");
}

#[tokio::test]
async fn test_search_bond_filter_no_match() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/embed"))
        .respond_with(OllamaEmbedResponder { dimensions: DIMS })
        .mount(&mock_server)
        .await;

    let engine = create_search_engine(&mock_server.uri()).await;

    let json = make_spore_json("no-ref-lib", "A library without bonds", "test.dev", "MIT");
    engine
        .index_spore("test.dev", "b3.noref", &json)
        .await
        .unwrap();

    // Filter with a bond that no spore has
    let results = engine
        .search(&SearchQuery {
            offset: 0,
            text: "library".to_string(),
            domain: None,
            license: None,
            strain: None,
            updated_after: None,
            updated_before: None,
            bonds: vec![BondFilter {
                relation: BondRelation::SpawnedFrom,
                uri: "cmn://nonexistent.dev/b3.xxx".to_string(),
            }],
            limit: 10,
        })
        .await
        .unwrap();
    assert!(
        results.is_empty(),
        "No results should match nonexistent bond"
    );
}

#[tokio::test]
async fn test_http_search_with_bond_filter() {
    let config = test_config();

    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/embed"))
        .respond_with(OllamaEmbedResponder { dimensions: DIMS })
        .mount(&mock_server)
        .await;

    let engine = create_search_engine(&mock_server.uri()).await;
    let search = Arc::new(SearchBackend::Ruvector(engine));

    let parent_uri = "cmn://original.dev/b3.parent";

    let json_with_ref = make_spore_json_with_refs(
        "ref-test",
        "A library for ref filter HTTP test",
        "ref.dev",
        "MIT",
        vec![serde_json::json!({ "uri": parent_uri, "relation": "spawned_from" })],
    );
    search
        .index_spore("ref.dev", "b3.reftest", &json_with_ref)
        .await
        .unwrap();

    let json_no_ref = make_spore_json(
        "noref-test",
        "Another library without refs",
        "noref.dev",
        "MIT",
    );
    search
        .index_spore("noref.dev", "b3.noreftest", &json_no_ref)
        .await
        .unwrap();

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
        Some(search.clone()),
        None,
    );
    synapse::handlers::response::set_storage_backend("redb");
    let state = Arc::new(synapse::AppState {
        config: config.clone(),
        storage,
        crawler,
        cmn_cache,
        rate_state: std::sync::atomic::AtomicU64::new(0),
        #[cfg(feature = "nostr")]
        nostr_relay: None,
        search: Some(search),
        graph: None,
    });

    let app = synapse::create_router(state);

    // URL-encode the bonds parameter value
    let bond_value = format!("spawned_from:{}", parent_uri);
    let uri = format!(
        "/synapse/search?q=library&bonds={}",
        urlencoding::encode(&bond_value)
    );

    let response = app
        .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body = axum::body::to_bytes(response.into_body(), 10000)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["kind"], "result");

    let results = json["result"]["spores"].as_array().unwrap();
    assert_eq!(
        results.len(),
        1,
        "Only the spore with matching bond should be returned"
    );
    assert_eq!(results[0]["domain"], "ref.dev");

    // Verify bonds is echoed in query
    assert!(
        json["result"]["query"]["bonds"].is_string(),
        "bonds should be echoed as string"
    );
}

// ---------------------------------------------------------------------------
// Tests — embedding error handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_embedding_api_error() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/embed"))
        .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
        .mount(&mock_server)
        .await;

    let engine = create_search_engine(&mock_server.uri()).await;

    let json = make_spore_json("fail-test", "Should fail", "test.dev", "MIT");
    let result = engine.index_spore("test.dev", "b3.fail", &json).await;

    assert!(result.is_err(), "Should propagate embedding API errors");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("500"),
        "Error should mention HTTP status: {}",
        err_msg
    );
}
