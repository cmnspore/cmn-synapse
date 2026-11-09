//! HTTP server module.
//!
//! Handles plain HTTP connections with h2c (HTTP/2 cleartext) support:
//! - Redirect to HTTPS (production with HTTPS enabled)
//! - Serve API directly with h2c (when HTTPS disabled, e.g., behind Caddy)

use std::net::SocketAddr;

use axum::{
    Router,
    body::Body,
    extract::Request,
    http::{Response, StatusCode, header},
};
use tokio::net::TcpListener;
use tower::Service;
use tracing::info;

/// Create HTTP redirect router.
/// Redirects all requests to HTTPS.
pub fn create_redirect_router() -> Router {
    Router::new().fallback(|req: Request| async move {
        let host = match req.headers().get(header::HOST) {
            Some(h) => match h.to_str() {
                Ok(s) => s.split(':').next().unwrap_or(""),
                Err(_) => {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from("Invalid Host header"))
                        .unwrap_or_else(|_| Response::new(Body::from("Bad Request")));
                }
            },
            None => {
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from("Missing Host header"))
                    .unwrap_or_else(|_| Response::new(Body::from("Bad Request")));
            }
        };

        let path = req
            .uri()
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/");

        Response::builder()
            .status(StatusCode::MOVED_PERMANENTLY)
            .header(header::LOCATION, format!("https://{}{}", host, path))
            .body(Body::empty())
            .unwrap_or_else(|_| Response::new(Body::empty()))
    })
}

/// Run HTTP server with h2c support.
///
/// Uses hyper's auto connection builder to support both HTTP/1.1 and HTTP/2 (h2c).
/// This enables HTTP/2 features like WebSocket upgrades when behind a reverse proxy.
pub async fn run_server(
    addr: SocketAddr,
    app: Router,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(addr).await?;
    info!(listen_addr = %addr, "HTTP server listening (h2c enabled)");

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, peer_addr) = result?;
                let app = app.clone();

                tokio::spawn(async move {
                    let io = hyper_util::rt::TokioIo::new(stream);
                    let mut make_service = app.into_make_service_with_connect_info::<SocketAddr>();

                    let service = match make_service.call(peer_addr).await {
                        Ok(s) => s,
                        Err(_) => return, // Infallible
                    };
                    let hyper_service = hyper_util::service::TowerToHyperService::new(service);

                    // Auto builder supports both HTTP/1.1 and h2c (HTTP/2 cleartext)
                    let _ = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new()
                    )
                    .serve_connection_with_upgrades(io, hyper_service)
                    .await;
                });
            }
            _ = shutdown_rx.recv() => {
                info!("HTTP server received shutdown signal");
                break;
            }
        }
    }

    info!("HTTP server stopped gracefully");
    Ok(())
}
