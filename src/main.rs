mod cmn_cache;
mod config;
mod crawler;
mod handlers;
mod http;
mod https;
mod ingest;
#[cfg(feature = "nostr")]
mod nostr;
#[cfg(feature = "nostr")]
mod relay;
#[cfg(feature = "ruvector")]
mod search;
mod storage;
mod time;

use clap::{CommandFactory, Parser};
use std::net::SocketAddr;
use std::sync::Arc;
#[cfg(feature = "postgres")]
use storage::PostgresStorage;
#[cfg(feature = "redb")]
use storage::RedbStorage;
use storage::{Storage, StorageBackend};
use tracing::{error, info, info_span, warn, Instrument};
use tracing_subscriber::EnvFilter;

use handlers::AppState;

#[derive(Parser, Debug)]
#[command(
    name = "synapse",
    version,
    about = "Synapse - CMN Federated Indexer",
    disable_help_subcommand = true
)]
struct Args {
    /// Configuration file path
    #[arg(long, default_value = "config.yml")]
    config: String,

    /// Rebuild the search vector index from storage, then exit.
    /// Requires the ruvector feature and a running embedding server (Ollama).
    /// Stop the running synapse process before using this flag.
    #[arg(long)]
    rebuild_search: bool,

    /// Redirect stdout to this file
    #[arg(long, value_name = "PATH")]
    stdout_file: Option<std::path::PathBuf>,

    /// Redirect stderr to this file
    #[arg(long, value_name = "PATH")]
    stderr_file: Option<std::path::PathBuf>,
}

fn emit_cli_event(event: agent_first_data::Event) {
    let stdout = std::io::stdout();
    let mut emitter =
        agent_first_data::CliEmitter::new(stdout.lock(), agent_first_data::OutputFormat::Json);
    let _ = emitter.emit(event);
}

fn emit_cli_value(value: serde_json::Value) {
    let stdout = std::io::stdout();
    let mut emitter =
        agent_first_data::CliEmitter::new(stdout.lock(), agent_first_data::OutputFormat::Json);
    let _ = emitter.emit_validated_value(value);
}

fn write_stdout(text: &str) {
    let mut stdout = std::io::stdout().lock();
    let _ = std::io::Write::write_all(&mut stdout, text.as_bytes());
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let raw: Vec<String> = std::env::args().collect();
    let _stream_redirect =
        match agent_first_data::stream_redirect::install_from_raw_args(raw.clone()) {
            Ok(installed) => installed,
            Err(err) => {
                emit_cli_event(agent_first_data::build_cli_error(&err.to_string(), None));
                std::process::exit(2);
            }
        };

    let build = match env!("GIT_SHA") {
        "unknown" => None,
        sha => Some(sha),
    };
    match agent_first_data::cli_handle_version_or_continue(
        &raw,
        &Args::command(),
        "synapse",
        Some(env!("DISPLAY_NAME")),
        env!("CARGO_PKG_VERSION"),
        build,
    ) {
        Ok(Some(version)) => {
            write_stdout(&version);
            return Ok(());
        }
        Ok(None) => {}
        Err(err) => {
            emit_cli_event(err);
            std::process::exit(2);
        }
    }

    match agent_first_data::cli_handle_help_or_continue(
        &raw,
        &Args::command(),
        &agent_first_data::HelpConfig::human_cli_default(),
    ) {
        Ok(Some(help)) => {
            write_stdout(&help);
            return Ok(());
        }
        Ok(None) => {}
        Err(err) => {
            emit_cli_value(err);
            std::process::exit(2);
        }
    }

    let args = Args::try_parse().unwrap_or_else(|e| {
        if matches!(
            e.kind(),
            clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
        ) {
            write_stdout(&e.to_string());
            std::process::exit(0);
        }
        emit_cli_event(agent_first_data::build_cli_error(
            &e.to_string(),
            Some("run synapse --help and retry with a valid configuration path"),
        ));
        std::process::exit(2);
    });
    let _ = (&args.stdout_file, &args.stderr_file);

    // Install ring as the default crypto provider for rustls.
    if rustls::crypto::ring::default_provider()
        .install_default()
        .is_err()
    {
        return Err("Failed to install ring crypto provider".into());
    }

    // Load config
    let cfg = Arc::new(config::AppConfig::load(Some(&args.config))?);
    let http_client = substrate::client::http_client(30)?;

    let cmn_cache = Arc::new(cmn_cache::CmnCache::new(
        cfg.cache.cmn_ttl_s,
        cfg.cache.max_cmn_bytes,
        http_client.clone(),
    ));

    // Initialize tracing
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        if cfg.debug {
            EnvFilter::new("debug,hyper=info,h2=info,tower=info,sqlx=warn")
        } else {
            EnvFilter::new("info")
        }
    });

    // Configure log output format (JSON by default for network services)
    let log_format = match cfg.log_format.as_str() {
        "plain" => agent_first_data::afdata_tracing::LogFormat::Plain,
        "yaml" => agent_first_data::afdata_tracing::LogFormat::Yaml,
        _ => agent_first_data::afdata_tracing::LogFormat::Json,
    };
    agent_first_data::afdata_tracing::try_init(
        filter,
        log_format,
        agent_first_data::Redactor::new(),
    )?;

    // Startup config as nested JSON — bypasses tracing (which can't nest).
    {
        let raw_yaml = std::fs::read_to_string(&args.config)?;
        let raw_config: serde_json::Value = serde_yaml::from_str(&raw_yaml)?;
        log_startup_config(raw_config, &args.config, &cfg.log_format);
    }

    // Initialize storage based on config
    let storage = match cfg.storage.backend.as_str() {
        #[cfg(feature = "postgres")]
        "postgres" => {
            let url = cfg
                .storage
                .postgres_url_secret
                .as_ref()
                .ok_or("postgres_url_secret required")?;
            info!("Connecting to PostgreSQL...");
            let pg = PostgresStorage::new(url).await?;
            info!("PostgreSQL connected");
            StorageBackend::Postgres(pg)
        }
        #[cfg(not(feature = "postgres"))]
        "postgres" => {
            return Err("PostgreSQL support not compiled. Enable the 'postgres' feature.".into());
        }
        #[cfg(feature = "redb")]
        "redb" => {
            let path = cfg.storage.redb_path.as_ref().ok_or("redb_path required")?;
            info!(path = %path, "Opening redb database...");
            let db = RedbStorage::open(path)?;
            info!("Redb opened");
            StorageBackend::Redb(db)
        }
        #[cfg(not(feature = "redb"))]
        "redb" => {
            return Err("Redb support not compiled. Enable the 'redb' feature.".into());
        }
        other => {
            return Err(format!("Unknown storage backend: {}", other).into());
        }
    };

    // Crawler is created after search init so it can receive the search engine
    // (moved below)

    // Initialize Nostr bridge if configured
    #[cfg(feature = "nostr")]
    let nostr_relay = {
        if let Some(ref nostr_cfg) = cfg.nostr {
            if nostr_cfg.enabled {
                match nostr::NostrBridge::from_config(nostr_cfg, &storage, cfg.pulse.max_body_bytes)
                    .await
                {
                    Ok(bridge) => {
                        let bridge = Arc::new(bridge);
                        let (event_tx, _) =
                            tokio::sync::broadcast::channel::<nostr_sdk::Event>(256);
                        let relay_state = Arc::new(relay::RelayState {
                            event_tx,
                            bridge,
                            max_body_bytes: cfg.pulse.max_body_bytes,
                            app_state: std::sync::OnceLock::new(),
                        });
                        Some(relay_state)
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to initialize Nostr bridge");
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        }
    };

    // Initialize search engine if configured
    #[cfg(feature = "ruvector")]
    let search = {
        if let Some(ref search_cfg) = cfg.search {
            if search_cfg.enabled {
                match search::RuvectorSearch::new(search_cfg) {
                    Ok(engine) => {
                        info!("Search engine initialized (ruvector)");
                        Some(Arc::new(search::SearchBackend::Ruvector(engine)))
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to initialize search engine");
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        }
    };

    // Initialize graph engine if configured
    #[cfg(feature = "ruvector")]
    let graph = {
        if let Some(ref search_cfg) = cfg.search {
            if search_cfg.graph.enabled {
                let embedder = if search_cfg.graph.embed_relationships {
                    Some(search::EmbedClient::new(&search_cfg.embedding))
                } else {
                    None
                };
                let graph = Arc::new(search::SporeGraph::new(embedder));

                if search_cfg.graph.rebuild_on_startup {
                    info!("Rebuilding spore evolution graph in background...");
                    let bg_storage = storage.clone();
                    let bg_graph = graph.clone();
                    tokio::spawn(async move {
                        rebuild_graph(&bg_storage, &bg_graph).await;
                        match bg_graph.stats() {
                            Ok(stats) => info!(
                                nodes = stats.total_nodes,
                                edges = stats.total_edges,
                                "Graph rebuild complete"
                            ),
                            Err(e) => warn!(error = %e, "Failed to read graph stats after rebuild"),
                        }
                    });
                }
                Some(graph)
            } else {
                None
            }
        } else {
            None
        }
    };

    // --rebuild-search: re-embed all spores from storage, then exit
    #[cfg(feature = "ruvector")]
    if args.rebuild_search {
        match &search {
            Some(search_engine) => {
                rebuild_search(&storage, search_engine).await;
                info!("Search rebuild complete, exiting");
                return Ok(());
            }
            None => {
                error!("--rebuild-search requires search to be enabled in config");
                return Err("Search not enabled".into());
            }
        }
    }
    #[cfg(not(feature = "ruvector"))]
    if args.rebuild_search {
        error!("--rebuild-search requires the ruvector feature");
        return Err("ruvector feature not enabled".into());
    }

    let crawler = crawler::Crawler::new(
        storage.clone(),
        cmn_cache.clone(),
        http_client.clone(),
        &cfg.cache,
        &cfg.pulse,
        #[cfg(feature = "ruvector")]
        search.clone(),
        #[cfg(feature = "ruvector")]
        graph.clone(),
    );

    // Set storage backend name for response traces
    handlers::response::set_storage_backend(&cfg.storage.backend);

    let state = Arc::new(AppState {
        config: cfg.clone(),
        storage,
        crawler,
        cmn_cache,
        #[cfg(feature = "nostr")]
        nostr_relay,
        #[cfg(feature = "ruvector")]
        search,
        #[cfg(feature = "ruvector")]
        graph,
        rate_state: std::sync::atomic::AtomicU64::new(0),
    });

    // Set AppState on Nostr relay for unified ingest pipeline
    #[cfg(feature = "nostr")]
    if let Some(ref relay_state) = state.nostr_relay {
        let _ = relay_state.app_state.set(state.clone());
    }

    // Create API router (includes /nostr WebSocket endpoint if configured)
    let app = handlers::create_router(state.clone());

    // Determine which servers to start
    let http_enabled = cfg.http.as_ref().is_some_and(|h| h.enabled);
    let https_enabled = cfg.https.as_ref().is_some_and(|h| h.enabled);

    if !http_enabled && !https_enabled {
        error!("No servers enabled. Enable http or https in config.");
        return Err("No servers enabled".into());
    }

    info!(
        http = http_enabled,
        https = https_enabled,
        debug = cfg.debug,
        storage = %cfg.storage.backend,
        "starting servers"
    );

    // Create shutdown channel for graceful shutdown
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    // Spawn servers
    let mut handles = Vec::new();

    // HTTP server
    if let Some(ref http_cfg) = cfg.http {
        if http_cfg.enabled {
            let http_addr: SocketAddr = http_cfg.address.parse()?;
            let http_shutdown_rx = shutdown_tx.subscribe();

            if https_enabled {
                // HTTP redirects to HTTPS
                handles.push(tokio::spawn(async move {
                    let redirect_app = http::create_redirect_router();
                    http::run_server(http_addr, redirect_app, http_shutdown_rx).await
                }));
            } else {
                // HTTP serves API directly (debug mode)
                let http_app = app.clone();
                handles.push(tokio::spawn(async move {
                    http::run_server(http_addr, http_app, http_shutdown_rx).await
                }));
            }
        }
    }

    // HTTPS server
    if let Some(ref https_cfg) = cfg.https {
        if https_cfg.enabled {
            let https_addr: SocketAddr = https_cfg.address.parse()?;
            let domains = https_cfg.domains.clone();
            let https_app = app.clone();
            let https_storage = state.storage.clone();
            let acme_mode = https_cfg.acme.mode.clone();
            let https_shutdown_rx = shutdown_tx.subscribe();

            let cert_domains: Vec<String> = if domains.is_empty() {
                vec!["localhost".to_string()]
            } else {
                domains.clone()
            };

            if acme_mode == "self" {
                let self_signed_days = https_cfg.self_signed_validity_days;
                handles.push(tokio::spawn(async move {
                    https::run_server_self_signed(
                        https_addr,
                        &https_storage,
                        &cert_domains,
                        self_signed_days,
                        https_app,
                        https_shutdown_rx,
                    )
                    .await
                }));
            } else {
                let acme_cfg = config::AcmeConfig {
                    contact: https_cfg.acme.contact.clone(),
                    mode: https_cfg.acme.mode.clone(),
                    renewal_before_days: https_cfg.acme.renewal_before_days,
                };
                let renewal_days = https_cfg.acme.renewal_before_days;
                handles.push(tokio::spawn(async move {
                    https::run_server_acme(
                        https_addr,
                        https_storage,
                        &cert_domains,
                        &acme_cfg,
                        renewal_days,
                        https_app,
                        https_shutdown_rx,
                    )
                    .await
                }));
            }
        }
    }

    // Spawn Nostr subscription listener if configured
    #[cfg(feature = "nostr")]
    if let Some(ref relay_state) = state.nostr_relay {
        if cfg.nostr.as_ref().is_some_and(|n| n.subscribe) {
            let bridge = relay_state.bridge.clone();
            let nostr_state = state.clone();
            let nostr_shutdown_rx = shutdown_tx.subscribe();
            handles.push(tokio::spawn(async move {
                bridge
                    .run_listener(nostr_state, nostr_shutdown_rx)
                    .await
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })
            }));
            info!("Nostr subscription listener spawned");
        }
    }

    // Start background retry task
    let retry_state = state.clone();
    let retry_shutdown_rx = shutdown_tx.subscribe();
    let retry_handle =
        tokio::spawn(async move { mycelium_retry_loop(retry_state, retry_shutdown_rx).await });
    handles.push(retry_handle);

    info!("Synapse Aggregator running");
    info!("Press Ctrl+C to shutdown gracefully");

    // Wait for shutdown signal
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal, stopping gracefully...");
            let _ = shutdown_tx.send(());
        }
    }

    // Wait for all tasks to complete
    for handle in handles {
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), handle).await {
            Ok(Ok(Ok(()))) => {
                info!("Task completed successfully");
            }
            Ok(Ok(Err(e))) => {
                error!(error = %e, "Task error");
            }
            Ok(Err(e)) => {
                error!(error = %e, "Task panicked");
            }
            Err(_) => {
                warn!("Task did not complete within timeout, forcing shutdown");
            }
        }
    }

    info!("Synapse shutdown complete");
    Ok(())
}

/// Rebuild the in-memory graph from storage on startup.
#[cfg(feature = "ruvector")]
async fn rebuild_graph(storage: &StorageBackend, graph: &search::SporeGraph) {
    // Get all domains
    let myceliums = match storage.get_all_myceliums().await {
        Ok(m) => m,
        Err(e) => {
            error!(error = %e, "Failed to get myceliums for graph rebuild");
            return;
        }
    };

    let mut total_spores = 0usize;
    let mut total_refs = 0usize;

    for mycelium in &myceliums {
        let domain = &mycelium.domain;
        let hashes = match storage.get_domain_spores(domain).await {
            Ok(h) => h,
            Err(e) => {
                warn!(error = %e, domain = %domain, "Failed to get spores for domain");
                continue;
            }
        };

        for hash in &hashes {
            // Register the spore node
            if let Err(e) = graph.add_spore(hash, domain) {
                tracing::warn!(error = %e, hash, "Failed to add spore to graph");
            }

            // Load spore data to extract bonds
            if let Ok(Some(data)) = storage.get_spore(hash).await {
                let refs = parse_spore_references(&data);
                if !refs.is_empty() {
                    if let Err(e) = graph.add_relationship(hash, &refs).await {
                        warn!(error = %e, hash = %hash, "Failed to add graph relationship");
                    } else {
                        total_refs += refs.len();
                    }
                }
            }
            total_spores += 1;
        }
    }

    info!(
        spores = total_spores,
        relationships = total_refs,
        domains = myceliums.len(),
        "Graph rebuild indexed"
    );
}

/// Rebuild the search vector index from storage.
/// Iterates all spores in all domains and re-embeds them.
#[cfg(feature = "ruvector")]
async fn rebuild_search(storage: &StorageBackend, search: &search::SearchBackend) {
    use search::Search;

    let myceliums = match storage.get_all_myceliums().await {
        Ok(m) => m,
        Err(e) => {
            error!(error = %e, "Failed to get myceliums for search rebuild");
            return;
        }
    };

    let mut indexed = 0usize;
    let mut failed = 0usize;

    for mycelium in &myceliums {
        let domain = &mycelium.domain;
        let hashes = match storage.get_domain_spores(domain).await {
            Ok(h) => h,
            Err(e) => {
                warn!(error = %e, domain = %domain, "Failed to get spores for domain");
                continue;
            }
        };

        for hash in &hashes {
            if let Ok(Some(data)) = storage.get_spore(hash).await {
                if let Ok(spore) = serde_json::from_slice::<serde_json::Value>(&data) {
                    match search.index_spore(domain, hash, &spore).await {
                        Ok(()) => indexed += 1,
                        Err(e) => {
                            warn!(error = %e, hash = %hash, "Failed to index spore");
                            failed += 1;
                        }
                    }
                }
            }
        }
    }

    info!(
        indexed = indexed,
        failed = failed,
        domains = myceliums.len(),
        "Search rebuild complete"
    );
}

/// Extract SporeBonds from stored spore data via substrate::Spore.
#[cfg(feature = "ruvector")]
fn parse_spore_references(data: &[u8]) -> Vec<storage::SporeBond> {
    match serde_json::from_slice::<substrate::Spore>(data) {
        Ok(spore) => spore.extract_bonds(),
        Err(_) => Vec::new(),
    }
}

/// Background task to retry failed mycelium syncs with exponential backoff.
async fn mycelium_retry_loop(
    state: Arc<AppState>,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let retry_cfg = &state.config.retry;
    let base_interval = retry_cfg.base_interval_s;
    let max_interval = retry_cfg.max_interval_s;
    let multiplier = retry_cfg.backoff_multiplier;

    // Per-domain retry state: (retry_count, last_attempt_time)
    let mut retry_state: std::collections::HashMap<String, (u32, tokio::time::Instant)> =
        std::collections::HashMap::new();

    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(base_interval));

    info!(
        base_interval_s = base_interval,
        max_interval_s = max_interval,
        backoff_multiplier = multiplier,
        "Mycelium retry task started"
    );

    loop {
        tokio::select! {
            _ = interval.tick() => {
                match state.storage.get_domains_for_retry(i32::MAX).await {
                    Ok(domains) => {
                        if !domains.is_empty() {
                            info!("Found {} domains to retry", domains.len());
                        }

                        for domain in domains {
                            let retries = retry_state.get(&domain).map(|(r, _)| *r).unwrap_or(0);

                            // Exponential backoff with jitter
                            let delay_s = (base_interval as f64
                                * multiplier.powi(retries as i32))
                                .min(max_interval as f64) as u64;

                            // Add deterministic jitter (±25%) to prevent thundering herd
                            let effective_delay = {
                                use std::hash::{Hash, Hasher};
                                let mut hasher =
                                    std::collections::hash_map::DefaultHasher::new();
                                domain.hash(&mut hasher);
                                retries.hash(&mut hasher);
                                let h = hasher.finish();
                                let jitter_range = delay_s / 4;
                                let jitter = if jitter_range > 0 {
                                    (h % (jitter_range * 2)) as i64 - jitter_range as i64
                                } else {
                                    0
                                };
                                (delay_s as i64 + jitter).max(base_interval as i64) as u64
                            };

                            // Skip if not enough real time has elapsed since last attempt
                            if let Some((_, last_attempt)) = retry_state.get(&domain) {
                                if last_attempt.elapsed().as_secs() < effective_delay {
                                    continue;
                                }
                            }

                            let span = info_span!("retry", domain = %domain, retries = retries, delay_s = effective_delay);
                            let success = async {
                                info!("Retrying domain");
                                match state.crawler.crawl_domain(&domain).await {
                                    Ok(_) => {
                                        info!("Successfully synced");
                                        true
                                    }
                                    Err(e) => {
                                        warn!(error = %e, "Failed to sync");
                                        if let Err(mark_err) = state
                                            .storage
                                            .mark_mycelium_failed(&domain, &e.to_string())
                                            .await
                                        {
                                            error!(error = %mark_err, "Failed to mark as failed");
                                        }
                                        false
                                    }
                                }
                            }
                            .instrument(span)
                            .await;

                            if success {
                                retry_state.remove(&domain);
                            } else {
                                let prev = retry_state.get(&domain).map(|(r, _)| *r).unwrap_or(0);
                                retry_state.insert(domain, (prev.saturating_add(1), tokio::time::Instant::now()));
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to get domains for retry: {}", e);
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                info!("Mycelium retry task received shutdown signal");
                break;
            }
        }
    }

    info!("Mycelium retry task stopped gracefully");
    Ok(())
}

/// Emit startup config as a single nested-JSON line to stdout.
/// Bypasses tracing because tracing flattens all fields (no nested objects/arrays).
fn log_startup_config(raw_config: serde_json::Value, config_path: &str, log_format: &str) {
    let event = agent_first_data::json_log(serde_json::json!({
        "event": "startup",
        "config": raw_config,
        "args": {"config_path": config_path},
        "env": {"RUST_LOG": std::env::var("RUST_LOG").ok()}
    }))
    .build();
    let format = match log_format {
        "plain" => agent_first_data::OutputFormat::Plain,
        "yaml" => agent_first_data::OutputFormat::Yaml,
        _ => agent_first_data::OutputFormat::Json,
    };
    let stdout = std::io::stdout();
    let mut emitter = agent_first_data::CliEmitter::new(stdout.lock(), format);
    let _ = emitter.emit(event);
}
