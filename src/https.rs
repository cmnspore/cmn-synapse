//! HTTPS server module with TLS certificate management.
//!
//! Supports:
//! - Self-signed certificates (development)
//! - ACME (Let's Encrypt) certificates (production)

use crate::config::AcmeConfig;
use crate::storage::{Storage, StorageBackend};
use chrono::{Duration, Utc};
use rcgen::generate_simple_self_signed;
use rustls::ServerConfig;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

/// Generate a self-signed certificate for development
pub fn generate_self_signed(domains: &[String]) -> Result<(String, String), rcgen::Error> {
    info!(domains = %domains.join(","), "Generating self-signed certificate");
    let certified_key = generate_simple_self_signed(domains.to_vec())?;
    let cert_pem = certified_key.cert.pem();
    let key_pem = certified_key.signing_key.serialize_pem();
    Ok((cert_pem, key_pem))
}

/// Load or create certificate from storage
pub async fn load_or_create_cert<S: Storage>(
    storage: &S,
    domains: &[String],
    debug: bool,
    self_signed_validity_days: i64,
) -> Result<(String, String), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(cert) = storage.get_cert(domains).await? {
        if !cert.needs_renewal(0) {
            info!(
                domains = %domains.join(","),
                expires_at_rfc3339 = %cert.expires_at.to_rfc3339(),
                self_signed = cert.self_signed,
                "Using existing certificate from storage"
            );
            return Ok((cert.cert_pem, cert.key_pem_secret));
        }

        if cert.self_signed {
            info!(domains = %domains.join(","), "Self-signed certificate expired, regenerating");
        } else {
            warn!(domains = %domains.join(","), "Let's Encrypt certificate expired");
        }
    }

    if debug {
        let (cert_pem, key_pem) = generate_self_signed(domains)?;
        let expires_at = Utc::now() + Duration::days(self_signed_validity_days);

        storage
            .save_cert(domains, &cert_pem, &key_pem, expires_at, true)
            .await?;

        info!(
            domains = %domains.join(","),
            expires_at_rfc3339 = %expires_at.to_rfc3339(),
            "Generated and saved self-signed certificate"
        );

        Ok((cert_pem, key_pem))
    } else {
        Err("No valid certificate found. Configure ACME for production.".into())
    }
}

/// Parse PEM certificate and key into rustls types
pub fn parse_pem(
    cert_pem: &str,
    key_pem: &str,
) -> Result<
    (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let certs: Vec<CertificateDer<'static>> =
        CertificateDer::pem_slice_iter(cert_pem.as_bytes()).collect::<Result<Vec<_>, _>>()?;

    if certs.is_empty() {
        return Err("No certificates found in PEM".into());
    }

    let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes())?;

    Ok((certs, key))
}

/// Create rustls ServerConfig from PEM strings
pub fn create_rustls_config(
    cert_pem: &str,
    key_pem: &str,
) -> Result<Arc<ServerConfig>, Box<dyn std::error::Error + Send + Sync>> {
    let (certs, key) = parse_pem(cert_pem, key_pem)?;

    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    // Enable HTTP/2 ALPN
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    Ok(Arc::new(config))
}

/// Create TLS acceptor from storage certificate
pub async fn create_tls_acceptor<S: Storage>(
    storage: &S,
    domains: &[String],
    debug: bool,
    self_signed_validity_days: i64,
) -> Result<tokio_rustls::TlsAcceptor, Box<dyn std::error::Error + Send + Sync>> {
    let (cert_pem, key_pem) =
        load_or_create_cert(storage, domains, debug, self_signed_validity_days).await?;
    let config = create_rustls_config(&cert_pem, &key_pem)?;
    Ok(tokio_rustls::TlsAcceptor::from(config))
}

/// Run HTTPS server with self-signed certificate.
pub async fn run_server_self_signed(
    addr: SocketAddr,
    storage: &StorageBackend,
    domains: &[String],
    self_signed_validity_days: i64,
    app: axum::Router,
    shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(addr).await?;
    info!(listen_addr = %addr, domains = %domains.join(","), "HTTPS server (self-signed)");

    let acceptor = create_tls_acceptor(storage, domains, true, self_signed_validity_days).await?;
    run_with_acceptor(listener, acceptor, app, shutdown_rx).await
}

/// Run HTTPS server with ACME (Let's Encrypt).
pub async fn run_server_acme(
    addr: SocketAddr,
    storage: StorageBackend,
    domains: &[String],
    acme_cfg: &AcmeConfig,
    renewal_days: i64,
    app: axum::Router,
    shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(addr).await?;
    let is_staging = acme_cfg.is_staging();
    info!(listen_addr = %addr, domains = %domains.join(","), staging = is_staging, "HTTPS server (Let's Encrypt)");

    // Check if we have a valid certificate in storage
    if let Some(cert) = storage.get_cert(domains).await? {
        if !cert.needs_renewal(renewal_days) {
            info!(
                domains = %domains.join(","),
                expires_at_rfc3339 = %cert.expires_at.to_rfc3339(),
                "Using existing Let's Encrypt certificate from storage"
            );

            let config = create_rustls_config(&cert.cert_pem, &cert.key_pem_secret)?;
            let acceptor = tokio_rustls::TlsAcceptor::from(config);

            // Start background renewal check
            let renewal_storage = storage.clone();
            let renewal_domains = domains.to_vec();
            tokio::spawn(async move {
                certificate_renewal_loop(renewal_storage, renewal_domains, renewal_days).await;
            });

            return run_with_acceptor(listener, acceptor, app, shutdown_rx).await;
        }
        info!(domains = %domains.join(","), "Certificate needs renewal");
    }

    info!(domains = %domains.join(","), "Starting ACME certificate acquisition");
    run_acme_with_storage(listener, storage, domains, acme_cfg, app, shutdown_rx).await
}

/// Run ACME and store certificate in storage
async fn run_acme_with_storage(
    listener: TcpListener,
    storage: StorageBackend,
    domains: &[String],
    acme_cfg: &AcmeConfig,
    app: axum::Router,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use futures::StreamExt;
    use tokio_rustls_acme::AcmeConfig as TokioAcmeConfig;
    use tokio_rustls_acme::caches::DirCache;

    let domains = domains.to_vec();
    let contact = format!("mailto:{}", acme_cfg.contact);

    // Create temporary directory for ACME
    let temp_dir: &'static str = Box::leak(
        std::env::temp_dir()
            .join("synapse_acme")
            .to_string_lossy()
            .into_owned()
            .into_boxed_str(),
    );
    std::fs::create_dir_all(temp_dir)?;

    let mut acme_config = TokioAcmeConfig::new(domains.clone())
        .contact([contact])
        .cache(DirCache::new(temp_dir));

    if acme_cfg.is_staging() {
        warn!("Using Let's Encrypt STAGING environment");
        acme_config = acme_config.directory_lets_encrypt(false);
    } else {
        info!("Using Let's Encrypt PRODUCTION environment");
        acme_config = acme_config.directory_lets_encrypt(true);
    }

    let mut state = acme_config.state();
    let mut rustls_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_cert_resolver(state.resolver());
    rustls_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let acceptor = state.axum_acceptor(Arc::new(rustls_config));

    // Spawn task to handle ACME events and save to storage
    let save_storage = storage.clone();
    let save_domains = domains.clone();
    let temp_dir_path = std::path::PathBuf::from(temp_dir);
    tokio::spawn(async move {
        loop {
            match state.next().await {
                Some(Ok(ok)) => {
                    info!("ACME: {:?}", ok);
                    if let Err(e) =
                        sync_cert_to_storage(&save_storage, &save_domains, &temp_dir_path).await
                    {
                        error!("Failed to save certificate to storage: {}", e);
                    }
                }
                Some(Err(err)) => error!("ACME error: {:?}", err),
                None => break,
            }
        }
    });

    let addr = listener.local_addr()?;
    info!(listen_addr = %addr, "ACME HTTPS server listening");

    let server = axum_server::from_tcp(listener.into_std()?)?
        .acceptor(acceptor)
        .serve(app.into_make_service_with_connect_info::<SocketAddr>());

    tokio::select! {
        result = server => {
            result?;
        }
        _ = shutdown_rx.recv() => {
            info!("ACME HTTPS server received shutdown signal");
        }
    }

    info!("ACME HTTPS server stopped gracefully");
    Ok(())
}

/// Sync certificate from temp directory to storage
async fn sync_cert_to_storage(
    storage: &StorageBackend,
    domains: &[String],
    cache_dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for entry in std::fs::read_dir(cache_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file()
            && let Some(name) = path.file_name().and_then(|n| n.to_str())
            && name.starts_with("cached_cert")
        {
            let data = std::fs::read(&path)?;
            if let Ok(content) = String::from_utf8(data)
                && let Some((cert_pem, key_pem)) = extract_cert_and_key(&content)
                && let Ok((certs, _)) = parse_pem(&cert_pem, &key_pem)
                && let Some(cert_der) = certs.first()
                && let Ok(expires_at) = get_cert_expiry(cert_der)
            {
                storage
                    .save_cert(domains, &cert_pem, &key_pem, expires_at, false)
                    .await?;
                info!(domains = %domains.join(","), expires_at_rfc3339 = %expires_at.to_rfc3339(),
                                              "Saved Let's Encrypt certificate to storage");
                return Ok(());
            }
        }
    }

    tracing::debug!("Certificate not yet in cache directory");
    Ok(())
}

/// Extract certificate chain and key from combined PEM content
fn extract_cert_and_key(content: &str) -> Option<(String, String)> {
    let mut cert_pem = String::new();
    let mut search_start = 0;
    while let Some(cert_start) = content[search_start..].find("-----BEGIN CERTIFICATE-----") {
        let abs_start = search_start + cert_start;
        if let Some(cert_end) = content[abs_start..].find("-----END CERTIFICATE-----") {
            let abs_end = abs_start + cert_end + "-----END CERTIFICATE-----".len();
            cert_pem.push_str(&content[abs_start..abs_end]);
            cert_pem.push('\n');
            search_start = abs_end;
        } else {
            break;
        }
    }

    if cert_pem.is_empty() {
        return None;
    }

    let key_markers = [
        ("-----BEGIN PRIVATE KEY-----", "-----END PRIVATE KEY-----"),
        (
            "-----BEGIN EC PRIVATE KEY-----",
            "-----END EC PRIVATE KEY-----",
        ),
        (
            "-----BEGIN RSA PRIVATE KEY-----",
            "-----END RSA PRIVATE KEY-----",
        ),
    ];

    for (start_marker, end_marker) in key_markers {
        if let (Some(key_start), Some(key_end)) =
            (content.find(start_marker), content.find(end_marker))
        {
            let key_pem = content[key_start..key_end + end_marker.len()].to_string();
            return Some((cert_pem, key_pem));
        }
    }

    None
}

/// Get certificate expiry date from DER
fn get_cert_expiry(
    cert_der: &CertificateDer,
) -> Result<chrono::DateTime<chrono::Utc>, Box<dyn std::error::Error + Send + Sync>> {
    use x509_parser::prelude::*;

    let (_, cert) = X509Certificate::from_der(cert_der.as_ref())?;
    let not_after = cert.validity().not_after;
    let timestamp = not_after.timestamp();

    Ok(chrono::DateTime::from_timestamp(timestamp, 0).ok_or("Invalid timestamp")?)
}

/// Background task to check certificate renewal
async fn certificate_renewal_loop(
    storage: StorageBackend,
    domains: Vec<String>,
    renewal_days: i64,
) {
    use tokio::time::{Duration, interval};

    let mut interval = interval(Duration::from_secs(12 * 60 * 60));

    loop {
        interval.tick().await;

        match storage.get_cert(&domains).await {
            Ok(Some(cert)) => {
                if cert.needs_renewal(renewal_days) {
                    info!(
                        domains = %domains.join(","),
                        expires_at_rfc3339 = %cert.expires_at.to_rfc3339(),
                        "Certificate needs renewal"
                    );
                    warn!("Certificate renewal needed. Restart server to renew.");
                }
            }
            Ok(None) => {
                warn!(domains = %domains.join(","), "No certificate found for renewal check");
            }
            Err(e) => {
                error!(error = %e, "Failed to check certificate for renewal");
            }
        }
    }
}

/// Run HTTPS server with a pre-configured TLS acceptor
pub async fn run_with_acceptor(
    listener: TcpListener,
    acceptor: tokio_rustls::TlsAcceptor,
    app: axum::Router,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use tower::Service;

    let addr = listener.local_addr()?;
    info!(listen_addr = %addr, "HTTPS server listening");

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, peer_addr)) => {
                        let acceptor = acceptor.clone();
                        let app = app.clone();

                        tokio::spawn(async move {
                            match acceptor.accept(stream).await {
                                Ok(tls_stream) => {
                                    let io = hyper_util::rt::TokioIo::new(tls_stream);
                                    let mut make_service = app.into_make_service_with_connect_info::<SocketAddr>();

                                    let service = match make_service.call(peer_addr).await {
                                        Ok(s) => s,
                                        Err(_) => return,
                                    };
                                    let hyper_service = hyper_util::service::TowerToHyperService::new(service);
                                    let _ = hyper_util::server::conn::auto::Builder::new(
                                        hyper_util::rt::TokioExecutor::new(),
                                    )
                                    .serve_connection_with_upgrades(io, hyper_service)
                                    .await;
                                }
                                Err(e) => {
                                    tracing::debug!(error = %e, "TLS handshake failed");
                                }
                            }
                        });
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                info!("HTTPS server received shutdown signal");
                break;
            }
        }
    }

    info!("HTTPS server stopped gracefully");
    Ok(())
}
