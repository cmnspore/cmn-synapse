#![cfg(feature = "ruvector")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stderr,
    clippy::print_stdout
)]
//! Live search integration tests using a real Ollama embedding service.
//!
//! These tests require:
//!   - Feature flag: `ruvector`
//!   - Environment variable: `OLLAMA_URL` (e.g. "http://192.168.20.100:11434")
//!   - A running Ollama instance with `bge-large` model loaded
//!
//! Run with:
//!   OLLAMA_URL=http://192.168.20.100:11434 cargo test -p synapse \
//!       --features ruvector --test search_live_test -- --nocapture

use serde_json::Value;
use synapse::config::{EmbeddingConfig, SearchConfig};
use synapse::search::{Search, SearchQuery};

const MODEL: &str = "bge-large";
const DIMS: usize = 1024;

fn ollama_url() -> Option<String> {
    std::env::var("OLLAMA_URL").ok()
}

fn log_skip(reason: &str) {
    let line = agent_first_data::output_json(&agent_first_data::build_json(
        "log",
        serde_json::json!({"event": "skip", "reason": reason}),
        None,
    ));
    println!("{line}");
}

fn make_spore(name: &str, desc: &str, domain: &str, license: &str) -> Value {
    serde_json::json!({
        "$schema": "https://cmn.dev/schemas/v1/spore.json",
        "capsule": {
            "uri": format!("cmn://{}/b3.{}", domain, name),
            "core": {
                "name": name,
                "description": desc,
                "domain": domain,
                "license": license,
                "synopsis": desc,
                "intent": ["release"],
                "keywords": [],
                "bonds": [],
                "tree": { "algorithm": "blob_tree_blake3_nfc", "exclude_names": [], "follow_rules": [] }
            },
            "core_signature": "ed25519.test",
            "dist": [{"type": "archive", "filename": "test.tar.zst"}]
        },
        "capsule_signature": "ed25519.test"
    })
}

async fn create_engine(ollama_url: &str) -> synapse::search::RuvectorSearch {
    let dir = tempfile::tempdir().unwrap();
    let cfg = SearchConfig {
        enabled: true,
        index_path: dir.path().join("idx").to_string_lossy().into_owned(),
        graph: Default::default(),
        embedding: EmbeddingConfig {
            provider: "ollama".to_string(),
            url: ollama_url.to_string(),
            model: MODEL.to_string(),
            api_key_secret: None,
            dimensions: DIMS,
        },
    };
    // leak the tempdir so it lives for the duration of the test
    std::mem::forget(dir);
    synapse::search::RuvectorSearch::new(&cfg).unwrap()
}

/// Corpus of 10 spores covering distinct topics:
///   - HTTP / networking (3)
///   - Serialization / data formats (2)
///   - Database / storage (2)
///   - Crypto / security (2)
///   - CLI / tooling (1)
struct Spore {
    hash: &'static str,
    name: &'static str,
    desc: &'static str,
    domain: &'static str,
    license: &'static str,
}

fn corpus() -> Vec<Spore> {
    vec![
        Spore {
            hash: "b3.http1",
            name: "reqwest-lite",
            desc: "Lightweight async HTTP client for Rust with automatic retries and connection pooling",
            domain: "cmn.dev",
            license: "MIT",
        },
        Spore {
            hash: "b3.http2",
            name: "hyper-client",
            desc: "Low-level HTTP/1.1 and HTTP/2 client built on top of Hyper with streaming support",
            domain: "cmn.dev",
            license: "Apache-2.0",
        },
        Spore {
            hash: "b3.http3",
            name: "axum-guard",
            desc: "Authentication and authorization middleware for Axum web framework with JWT support",
            domain: "tools.rs",
            license: "MIT",
        },
        Spore {
            hash: "b3.ser1",
            name: "serde-csv",
            desc: "Fast CSV serializer and deserializer using serde with streaming parser",
            domain: "tools.rs",
            license: "MIT",
        },
        Spore {
            hash: "b3.ser2",
            name: "json-patch",
            desc: "RFC 6902 JSON Patch implementation for applying and generating patches on JSON documents",
            domain: "cmn.dev",
            license: "MIT",
        },
        Spore {
            hash: "b3.db1",
            name: "redb-cache",
            desc: "LRU caching layer on top of redb embedded key-value database for Rust applications",
            domain: "tools.rs",
            license: "Apache-2.0",
        },
        Spore {
            hash: "b3.db2",
            name: "pg-migrate",
            desc: "PostgreSQL schema migration tool with version tracking and rollback support",
            domain: "cmn.dev",
            license: "MIT",
        },
        Spore {
            hash: "b3.crypto1",
            name: "ed25519-verify",
            desc: "Fast Ed25519 signature verification library optimized for batch verification",
            domain: "cmn.dev",
            license: "Apache-2.0",
        },
        Spore {
            hash: "b3.crypto2",
            name: "tls-config",
            desc: "TLS certificate management and configuration helper for rustls with ACME integration",
            domain: "tools.rs",
            license: "MIT",
        },
        Spore {
            hash: "b3.cli1",
            name: "clap-wizard",
            desc: "Interactive CLI wizard builder that generates guided prompts from clap argument definitions",
            domain: "tools.rs",
            license: "Apache-2.0",
        },
    ]
}

/// Index the full corpus into the search engine.
async fn index_corpus(engine: &synapse::search::RuvectorSearch) {
    for s in corpus() {
        let json = make_spore(s.name, s.desc, s.domain, s.license);
        engine.index_spore(s.domain, s.hash, &json).await.unwrap();
    }
}

// ─── Semantic relevance tests ───────────────────────────────────────────────

/// "HTTP client" should rank the two HTTP client spores at the top.
#[tokio::test]
async fn test_live_search_http_client() {
    let Some(url) = ollama_url() else {
        log_skip("OLLAMA_URL not set, skipping live search test");
        return;
    };
    let engine = create_engine(&url).await;
    index_corpus(&engine).await;

    let results = engine
        .search(&SearchQuery {
            offset: 0,
            text: "HTTP client library".to_string(),
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

    println!("\n=== Query: \"HTTP client library\" (top 5) ===");
    for r in &results {
        println!("  {:.4}  {:20} [{}] {}", r.score, r.name, r.domain, r.uri);
    }

    // The top-2 should include reqwest-lite and hyper-client
    let top2_names: Vec<&str> = results.iter().take(2).map(|r| r.name.as_str()).collect();
    assert!(
        top2_names.contains(&"reqwest-lite") || top2_names.contains(&"hyper-client"),
        "Expected an HTTP client spore in top 2, got: {:?}",
        top2_names
    );
}

/// "database migration" should rank pg-migrate highly.
#[tokio::test]
async fn test_live_search_database() {
    let Some(url) = ollama_url() else {
        log_skip("OLLAMA_URL not set, skipping live search test");
        return;
    };
    let engine = create_engine(&url).await;
    index_corpus(&engine).await;

    let results = engine
        .search(&SearchQuery {
            offset: 0,
            text: "database migration tool".to_string(),
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

    println!("\n=== Query: \"database migration tool\" (top 5) ===");
    for r in &results {
        println!("  {:.4}  {:20} [{}] {}", r.score, r.name, r.domain, r.uri);
    }

    let top3_names: Vec<&str> = results.iter().take(3).map(|r| r.name.as_str()).collect();
    assert!(
        top3_names.contains(&"pg-migrate"),
        "Expected pg-migrate in top 3, got: {:?}",
        top3_names
    );
}

/// "cryptographic signatures" should rank ed25519-verify highly.
#[tokio::test]
async fn test_live_search_crypto() {
    let Some(url) = ollama_url() else {
        log_skip("OLLAMA_URL not set, skipping live search test");
        return;
    };
    let engine = create_engine(&url).await;
    index_corpus(&engine).await;

    let results = engine
        .search(&SearchQuery {
            offset: 0,
            text: "cryptographic signature verification".to_string(),
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

    println!("\n=== Query: \"cryptographic signature verification\" (top 5) ===");
    for r in &results {
        println!("  {:.4}  {:20} [{}] {}", r.score, r.name, r.domain, r.uri);
    }

    let top3_names: Vec<&str> = results.iter().take(3).map(|r| r.name.as_str()).collect();
    assert!(
        top3_names.contains(&"ed25519-verify"),
        "Expected ed25519-verify in top 3, got: {:?}",
        top3_names
    );
}

/// "JSON serialization" should rank json-patch and serde-csv above unrelated results.
#[tokio::test]
async fn test_live_search_serialization() {
    let Some(url) = ollama_url() else {
        log_skip("OLLAMA_URL not set, skipping live search test");
        return;
    };
    let engine = create_engine(&url).await;
    index_corpus(&engine).await;

    let results = engine
        .search(&SearchQuery {
            offset: 0,
            text: "JSON serialization and data formats".to_string(),
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

    println!("\n=== Query: \"JSON serialization and data formats\" (top 5) ===");
    for r in &results {
        println!("  {:.4}  {:20} [{}] {}", r.score, r.name, r.domain, r.uri);
    }

    let top3_names: Vec<&str> = results.iter().take(3).map(|r| r.name.as_str()).collect();
    assert!(
        top3_names.contains(&"json-patch") || top3_names.contains(&"serde-csv"),
        "Expected a serialization spore in top 3, got: {:?}",
        top3_names
    );
}

/// "command line interface" should rank clap-wizard at the top.
#[tokio::test]
async fn test_live_search_cli() {
    let Some(url) = ollama_url() else {
        log_skip("OLLAMA_URL not set, skipping live search test");
        return;
    };
    let engine = create_engine(&url).await;
    index_corpus(&engine).await;

    let results = engine
        .search(&SearchQuery {
            offset: 0,
            text: "command line interface builder".to_string(),
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

    println!("\n=== Query: \"command line interface builder\" (top 5) ===");
    for r in &results {
        println!("  {:.4}  {:20} [{}] {}", r.score, r.name, r.domain, r.uri);
    }

    assert_eq!(
        results[0].name, "clap-wizard",
        "Expected clap-wizard as #1, got: {}",
        results[0].name
    );
}

// ─── Domain filter + semantic combo ─────────────────────────────────────────

/// "HTTP" filtered to tools.rs should return axum-guard (the only HTTP-related one on tools.rs).
#[tokio::test]
async fn test_live_search_domain_filter() {
    let Some(url) = ollama_url() else {
        log_skip("OLLAMA_URL not set, skipping live search test");
        return;
    };
    let engine = create_engine(&url).await;
    index_corpus(&engine).await;

    let results = engine
        .search(&SearchQuery {
            offset: 0,
            text: "web framework middleware".to_string(),
            domain: Some("tools.rs".to_string()),
            license: None,
            bonds: vec![],
            strain: None,
            updated_after: None,
            updated_before: None,
            limit: 5,
        })
        .await
        .unwrap();

    println!("\n=== Query: \"web framework middleware\" domain=tools.rs (top 5) ===");
    for r in &results {
        println!("  {:.4}  {:20} [{}]", r.score, r.name, r.domain);
    }

    // All results should be from tools.rs
    for r in &results {
        assert_eq!(r.domain, "tools.rs", "Domain filter violated: {}", r.domain);
    }
    // axum-guard should be in results
    let names: Vec<&str> = results.iter().map(|r| r.name.as_str()).collect();
    assert!(
        names.contains(&"axum-guard"),
        "Expected axum-guard in results, got: {:?}",
        names
    );
}

/// License filter: only Apache-2.0 results for "security".
#[tokio::test]
async fn test_live_search_license_filter() {
    let Some(url) = ollama_url() else {
        log_skip("OLLAMA_URL not set, skipping live search test");
        return;
    };
    let engine = create_engine(&url).await;
    index_corpus(&engine).await;

    let results = engine
        .search(&SearchQuery {
            offset: 0,
            text: "security".to_string(),
            domain: None,
            license: Some("Apache-2.0".to_string()),
            bonds: vec![],
            strain: None,
            updated_after: None,
            updated_before: None,
            limit: 10,
        })
        .await
        .unwrap();

    println!("\n=== Query: \"security\" license=Apache-2.0 ===");
    for r in &results {
        println!("  {:.4}  {:20} [{}]", r.score, r.name, r.domain);
    }

    let corpus_map: std::collections::HashMap<&str, &str> =
        corpus().iter().map(|s| (s.hash, s.license)).collect();
    for r in &results {
        let hash = r.uri.rsplit('/').next().unwrap_or("");
        let expected_license = corpus_map.get(hash).unwrap_or(&"unknown");
        assert_eq!(
            *expected_license, "Apache-2.0",
            "License filter violated for {}: got {}",
            r.name, expected_license
        );
    }
}

// ─── Chinese query (BGE models handle multilingual) ─────────────────────────

/// 用中文搜索 "数据库" 应该命中 pg-migrate 或 redb-cache.
#[tokio::test]
async fn test_live_search_chinese_query() {
    let Some(url) = ollama_url() else {
        log_skip("OLLAMA_URL not set, skipping live search test");
        return;
    };
    let engine = create_engine(&url).await;
    index_corpus(&engine).await;

    let results = engine
        .search(&SearchQuery {
            offset: 0,
            text: "数据库存储".to_string(),
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

    println!("\n=== Query: \"数据库存储\" (top 5) ===");
    for r in &results {
        println!("  {:.4}  {:20} [{}]", r.score, r.name, r.domain);
    }

    let top3_names: Vec<&str> = results.iter().take(3).map(|r| r.name.as_str()).collect();
    assert!(
        top3_names.contains(&"pg-migrate") || top3_names.contains(&"redb-cache"),
        "Expected a database spore in top 3 for Chinese query, got: {:?}",
        top3_names
    );
}
