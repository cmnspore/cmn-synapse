use super::response;
use super::AppState;
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use std::sync::Arc;

/// WebSocket upgrade handler for the Nostr relay endpoint.
pub async fn nostr_relay_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    match state.nostr_relay {
        Some(ref relay_state) => {
            tracing::info!("Nostr relay: WebSocket upgrade accepted");
            let relay_state = relay_state.clone();

            // Apply WebSocket message size limit from config
            let max_ws_bytes = state
                .config
                .nostr
                .as_ref()
                .map(|n| n.max_ws_message_bytes)
                .unwrap_or(262_144);

            ws.max_message_size(max_ws_bytes)
                .on_upgrade(move |socket| crate::relay::handle_connection(socket, relay_state))
        }
        None => {
            tracing::warn!("Nostr relay: not configured, returning 503");
            response::error(
                StatusCode::SERVICE_UNAVAILABLE,
                "relay_unavailable",
                "Nostr relay is not configured on this node",
            )
            .into_response()
        }
    }
}
