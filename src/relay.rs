//! CMN-only Nostr relay (NIP-01 minimal implementation).
//!
//! Accepts WebSocket connections and serves CMN events (kind 30078).
//! Only stores/forwards events tagged with "cmn-spore", "cmn-mycelium", or "cmn-taste".
//!
//! Implements the minimum NIP-01 protocol:
//! - EVENT: Store event (if valid CMN event) + broadcast to subscribers
//! - REQ: Subscribe with filters + send matching stored events + EOSE
//! - CLOSE: Remove subscription

use crate::nostr::NostrBridge;
use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use nostr_sdk::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{info, warn};

/// CMN event kind for filtering
const CMN_EVENT_KIND: u16 = 30078;

/// Relay state shared across WebSocket connections.
pub struct RelayState {
    /// Broadcast channel for new events
    pub event_tx: broadcast::Sender<Event>,
    /// Reference to the NostrBridge (for shared event storage)
    pub bridge: Arc<NostrBridge>,
    /// Maximum body bytes for pulse processing
    pub max_body_bytes: usize,
    /// Lazy reference to AppState (set after AppState is constructed)
    pub app_state: std::sync::OnceLock<Arc<crate::handlers::AppState>>,
}

/// Extended filter that wraps nostr-sdk's Filter with multi-letter tag support.
/// NIP-01 only defines single-letter tag filters (#d, #t, #e, #p, etc.), but CMN
/// events carry multi-letter tags (domain, uri, hash) that clients may want to filter.
#[derive(Clone)]
struct CmnFilter {
    inner: Filter,
    /// Multi-letter tag filters (e.g. "domain" -> ["example.com"])
    multi_letter_tags: HashMap<String, Vec<String>>,
}

/// Handle a single WebSocket connection as a NIP-01 relay.
pub async fn handle_connection(ws: WebSocket, state: Arc<RelayState>) {
    info!("Relay: new WebSocket connection");
    let (mut ws_tx, mut ws_rx) = ws.split();

    // mpsc channel so both the main loop and the broadcast forwarder
    // can send messages to the client through a single writer task.
    let (send_tx, mut send_rx) = mpsc::unbounded_channel::<String>();

    // Writer task: drains the mpsc channel into the WebSocket sink.
    let writer_handle = tokio::spawn(async move {
        while let Some(text) = send_rx.recv().await {
            if ws_tx.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
    });

    // Per-connection subscriptions: subscription_id -> CmnFilter
    let subscriptions: Arc<RwLock<HashMap<String, CmnFilter>>> =
        Arc::new(RwLock::new(HashMap::new()));

    // Subscribe to broadcast channel for new events
    let mut event_rx = state.event_tx.subscribe();

    // Spawn task to forward broadcast events to matching subscriptions
    let subs_clone = subscriptions.clone();
    let send_tx_broadcast = send_tx.clone();
    let forward_handle = tokio::spawn(async move {
        while let Ok(event) = event_rx.recv().await {
            let subs = subs_clone.read().await;
            for (sub_id, filter) in subs.iter() {
                if event_matches_filter(&event, filter) {
                    let msg = serde_json::json!([
                        "EVENT",
                        sub_id,
                        serde_json::from_str::<Value>(&event.as_json()).unwrap_or_default()
                    ]);
                    if send_tx_broadcast.send(msg.to_string()).is_err() {
                        return;
                    }
                }
            }
        }
    });

    // Process incoming messages
    while let Some(Ok(msg)) = ws_rx.next().await {
        let text = match msg {
            Message::Text(t) => t.to_string(),
            Message::Close(_) => {
                info!("Relay: client sent Close");
                break;
            }
            _ => continue,
        };

        let parsed: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "Relay: invalid JSON from client");
                let notice = serde_json::json!(["NOTICE", "Invalid JSON"]);
                let _ = send_tx.send(notice.to_string());
                continue;
            }
        };

        let arr = match parsed.as_array() {
            Some(a) if !a.is_empty() => a,
            _ => continue,
        };

        let msg_type = match arr[0].as_str() {
            Some(t) => t,
            None => continue,
        };

        match msg_type {
            "EVENT" => {
                if arr.len() < 2 {
                    continue;
                }
                handle_event(&arr[1], &state).await;
            }
            "REQ" => {
                if arr.len() < 3 {
                    continue;
                }
                let sub_id = match arr[1].as_str() {
                    Some(s) => s.to_string(),
                    None => continue,
                };

                info!(subscription_id = %sub_id, "Relay: REQ received");

                // Parse filter
                let filter = parse_filter(&arr[2]);

                // Register subscription FIRST so broadcast events are also matched
                subscriptions
                    .write()
                    .await
                    .insert(sub_id.clone(), filter.clone());

                // Send stored matching events directly to this client
                let events_lock = state.bridge.events();
                let events = events_lock.read().await;
                let mut sent_count = 0usize;
                let send_limit = filter.inner.limit.unwrap_or(usize::MAX);
                for event in events.iter() {
                    if sent_count >= send_limit {
                        break;
                    }
                    if event_matches_filter(event, &filter) {
                        let msg = serde_json::json!([
                            "EVENT",
                            &sub_id,
                            serde_json::from_str::<Value>(&event.as_json()).unwrap_or_default()
                        ]);
                        if send_tx.send(msg.to_string()).is_err() {
                            warn!(subscription_id = %sub_id, "Relay: send channel closed while sending stored events");
                            break;
                        }
                        sent_count += 1;
                    }
                }

                // Send EOSE (End of Stored Events)
                let eose = serde_json::json!(["EOSE", &sub_id]);
                let _ = send_tx.send(eose.to_string());
                info!(
                    subscription_id = %sub_id,
                    sent_count,
                    "Relay: EOSE sent"
                );
            }
            "CLOSE" => {
                if arr.len() < 2 {
                    continue;
                }
                if let Some(sub_id) = arr[1].as_str() {
                    info!(subscription_id = %sub_id, "Relay: CLOSE received");
                    subscriptions.write().await.remove(sub_id);
                }
            }
            _ => {}
        }
    }

    forward_handle.abort();
    writer_handle.abort();
}

/// Handle an incoming EVENT message.
async fn handle_event(event_json: &Value, state: &RelayState) {
    // Parse the event
    let event_str = event_json.to_string();
    let event = match Event::from_json(&event_str) {
        Ok(e) => e,
        Err(e) => {
            warn!(error = %e, "Invalid Nostr event");
            return;
        }
    };

    // Validate: must be kind 30078
    if event.kind != Kind::Custom(CMN_EVENT_KIND) {
        warn!(kind = ?event.kind, "Rejected non-CMN event");
        return;
    }

    // Validate: must have a CMN content tag
    let has_cmn_tag = event.tags.iter().any(|tag| {
        let tag_vec: Vec<String> = tag.clone().to_vec();
        tag_vec.len() >= 2
            && tag_vec[0] == "t"
            && (tag_vec[1] == "cmn-spore"
                || tag_vec[1] == "cmn-mycelium"
                || tag_vec[1] == "cmn-taste")
    });

    if !has_cmn_tag {
        warn!("Rejected event without CMN tag");
        return;
    }

    // Verify event signature
    if event.verify().is_err() {
        warn!("Rejected event with invalid signature");
        return;
    }

    // Store using shared bridge method (dedup + cap + persist)
    state.bridge.store_event(&event).await;
    info!(event_id = %event.id.to_hex(), "Stored CMN event via relay");

    // Process CMN content (unified ingest pipeline)
    if let Some(app_state) = state.app_state.get() {
        if let Err(e) = crate::nostr::process_cmn_event_from_relay(
            &event,
            app_state,
            state.max_body_bytes,
            app_state.config.pulse.max_clock_skew_s * 1000,
            app_state.config.pulse.max_taste_clock_skew_s * 1000,
        )
        .await
        {
            warn!(error = %e, "Failed to process CMN event content");
        }
    } else {
        warn!("AppState not yet initialized, skipping CMN event processing");
    }

    // Broadcast to subscribers
    let _ = state.event_tx.send(event);
}

/// Parse a NIP-01 filter from JSON.
///
/// Handles standard single-letter tag filters (#d, #t, #e, #p) via nostr-sdk's Filter,
/// and CMN-specific multi-letter tag filters (#domain, #uri, #hash) via CmnFilter.
fn parse_filter(filter_json: &Value) -> CmnFilter {
    let mut filter = Filter::new();
    let mut multi_letter_tags: HashMap<String, Vec<String>> = HashMap::new();

    if let Some(kinds) = filter_json.get("kinds").and_then(|k| k.as_array()) {
        for kind in kinds {
            if let Some(k) = kind.as_u64() {
                filter = filter.kind(Kind::Custom(k as u16));
            }
        }
    }

    // Handle generic #<tag> filters
    if let Some(obj) = filter_json.as_object() {
        for (key, values) in obj {
            if let Some(tag_name) = key.strip_prefix('#') {
                let str_values: Vec<String> = values
                    .as_array()
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();

                if str_values.is_empty() {
                    continue;
                }

                // Single-character tags go into nostr-sdk Filter (NIP-01 standard)
                if tag_name.len() == 1 {
                    if let Some(ch) = tag_name.chars().next() {
                        if let Ok(slt) = SingleLetterTag::from_char(ch) {
                            filter = filter.custom_tags(slt, str_values);
                        }
                    }
                } else {
                    // Multi-letter tags (domain, uri, hash) — CMN extension
                    multi_letter_tags.insert(tag_name.to_string(), str_values);
                }
            }
        }
    }

    // Handle since/until
    if let Some(since) = filter_json.get("since").and_then(|s| s.as_u64()) {
        filter = filter.since(Timestamp::from(since));
    }
    if let Some(until) = filter_json.get("until").and_then(|u| u.as_u64()) {
        filter = filter.until(Timestamp::from(until));
    }

    // Handle limit
    if let Some(limit) = filter_json.get("limit").and_then(|l| l.as_u64()) {
        filter = filter.limit(limit as usize);
    }

    CmnFilter {
        inner: filter,
        multi_letter_tags,
    }
}

/// Check if an event matches a CmnFilter.
fn event_matches_filter(event: &Event, filter: &CmnFilter) -> bool {
    // Kind check
    if let Some(kinds) = &filter.inner.kinds {
        if !kinds.contains(&event.kind) {
            return false;
        }
    }

    // Timestamp checks (NIP-01: since/until)
    if let Some(since) = filter.inner.since {
        if event.created_at < since {
            return false;
        }
    }
    if let Some(until) = filter.inner.until {
        if event.created_at > until {
            return false;
        }
    }

    // Single-letter tag check (NIP-01 standard: #t, #d, #e, #p, etc.)
    for (tag_key, tag_values) in &filter.inner.generic_tags {
        let key_str: String = tag_key.to_string();
        let event_has_match = event.tags.iter().any(|etag| {
            let tv: Vec<String> = etag.clone().to_vec();
            tv.len() >= 2 && tv[0] == key_str && tag_values.iter().any(|v: &String| tv[1] == *v)
        });
        if !event_has_match {
            return false;
        }
    }

    // Multi-letter tag check (CMN extension: #domain, #uri, #hash)
    for (tag_name, tag_values) in &filter.multi_letter_tags {
        let event_has_match = event.tags.iter().any(|etag| {
            let tv: Vec<String> = etag.clone().to_vec();
            tv.len() >= 2 && tv[0] == *tag_name && tag_values.iter().any(|v| tv[1] == *v)
        });
        if !event_has_match {
            return false;
        }
    }

    true
}
