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
use tokio::sync::{RwLock, broadcast, mpsc};
use tracing::warn;

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

/// Bound each connection independently; history sends wait only after releasing storage locks.
pub const DEFAULT_HISTORY_LIMIT: usize = 256;
pub const MAX_HISTORY_LIMIT: usize = 1024;
pub const MAX_SUBSCRIPTIONS: usize = 32;
const SEND_QUEUE_CAPACITY: usize = 64;
const SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

async fn send_message(sender: &mpsc::Sender<String>, message: Value) -> bool {
    matches!(
        tokio::time::timeout(SEND_TIMEOUT, sender.send(message.to_string())).await,
        Ok(Ok(()))
    )
}

async fn forward_events(
    mut events: broadcast::Receiver<Event>,
    subscriptions: Arc<RwLock<HashMap<String, CmnFilter>>>,
    sender: mpsc::Sender<String>,
    stop: tokio::sync::watch::Sender<bool>,
) {
    loop {
        match events.recv().await {
            Ok(event) => {
                let matching: Vec<String> = {
                    let subscriptions = subscriptions.read().await;
                    subscriptions
                        .iter()
                        .filter(|(_, filter)| event_matches_filter(&event, filter))
                        .map(|(id, _)| id.clone())
                        .collect()
                };
                for id in matching {
                    if sender
                        .try_send(serde_json::json!(["EVENT", id, &event]).to_string())
                        .is_err()
                    {
                        let _ = stop.send(true);
                        return;
                    }
                }
            }
            Err(broadcast::error::RecvError::Lagged(missed_count)) => {
                warn!(
                    missed_count,
                    "Closing lagged relay connection; client must resubscribe"
                );
                let _ = stop.send(true);
                return;
            }
            Err(broadcast::error::RecvError::Closed) => {
                let _ = stop.send(true);
                return;
            }
        }
    }
}

/// Handle a single WebSocket connection as a NIP-01 relay.
pub async fn handle_connection(ws: WebSocket, state: Arc<RelayState>) {
    let (mut ws_tx, mut ws_rx) = ws.split();
    let (send_tx, mut send_rx) = mpsc::channel::<String>(SEND_QUEUE_CAPACITY);
    let (stop_tx, mut writer_stop) = tokio::sync::watch::channel(false);
    let mut connection_stop = stop_tx.subscribe();
    let writer_signal = stop_tx.clone();
    let mut writer_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = writer_stop.changed() => {
                    let close = axum::extract::ws::CloseFrame { code: 1013, reason: "relay stream interrupted; resubscribe".into() };
                    let _ = tokio::time::timeout(SEND_TIMEOUT, ws_tx.send(Message::Close(Some(close)))).await;
                    break;
                }
                message = send_rx.recv() => {
                    let Some(text) = message else { break; };
                    if !matches!(tokio::time::timeout(SEND_TIMEOUT, ws_tx.send(Message::Text(text.into()))).await, Ok(Ok(()))) {
                        let _ = writer_signal.send(true);
                        break;
                    }
                }
            }
        }
    });
    let subscriptions: Arc<RwLock<HashMap<String, CmnFilter>>> =
        Arc::new(RwLock::new(HashMap::new()));
    let forward_handle = tokio::spawn(forward_events(
        state.event_tx.subscribe(),
        subscriptions.clone(),
        send_tx.clone(),
        stop_tx.clone(),
    ));
    loop {
        let message = tokio::select! {
            _ = connection_stop.changed() => break,
            message = ws_rx.next() => message,
        };
        let Some(Ok(message)) = message else {
            break;
        };
        let text = match message {
            Message::Text(text) => text,
            Message::Close(_) => break,
            _ => continue,
        };
        let parsed: Value = match serde_json::from_str(&text) {
            Ok(value) => value,
            Err(_) => {
                if !send_message(&send_tx, serde_json::json!(["NOTICE", "Invalid JSON"])).await {
                    break;
                }
                continue;
            }
        };
        let Some(array) = parsed.as_array() else {
            continue;
        };
        match array.first().and_then(Value::as_str) {
            Some("EVENT") if array.len() >= 2 => {
                let acknowledgement = handle_event(&array[1], &state).await;
                if !send_message(&send_tx, acknowledgement).await {
                    break;
                }
            }
            Some("REQ") if array.len() >= 3 => {
                let Some(id) = array[1].as_str() else {
                    continue;
                };
                if id.len() > 128 {
                    if !send_message(
                        &send_tx,
                        serde_json::json!(["CLOSED", id, "invalid: subscription id too long"]),
                    )
                    .await
                    {
                        break;
                    }
                    continue;
                }
                let filter = parse_filter(&array[2]);
                let accepted = {
                    let mut subscriptions = subscriptions.write().await;
                    if subscriptions.contains_key(id) || subscriptions.len() < MAX_SUBSCRIPTIONS {
                        subscriptions.insert(id.to_string(), filter.clone());
                        true
                    } else {
                        false
                    }
                };
                if !accepted {
                    if !send_message(
                        &send_tx,
                        serde_json::json!(["CLOSED", id, "restricted: too many subscriptions"]),
                    )
                    .await
                    {
                        break;
                    }
                    continue;
                }
                let events_lock = state.bridge.events();
                let history: Vec<Event> = {
                    let events = events_lock.read().await;
                    events
                        .iter()
                        .filter(|event| event_matches_filter(event, &filter))
                        .take(
                            filter
                                .inner
                                .limit
                                .unwrap_or(DEFAULT_HISTORY_LIMIT)
                                .min(MAX_HISTORY_LIMIT),
                        )
                        .cloned()
                        .collect()
                };
                let mut sent = true;
                for event in history {
                    if !send_message(&send_tx, serde_json::json!(["EVENT", id, event])).await {
                        sent = false;
                        break;
                    }
                }
                if !sent || !send_message(&send_tx, serde_json::json!(["EOSE", id])).await {
                    break;
                }
            }
            Some("CLOSE") if array.len() >= 2 => {
                if let Some(id) = array[1].as_str() {
                    subscriptions.write().await.remove(id);
                }
            }
            _ => {}
        }
    }
    let _ = stop_tx.send(true);
    forward_handle.abort();
    if tokio::time::timeout(SEND_TIMEOUT, &mut writer_handle)
        .await
        .is_err()
    {
        writer_handle.abort();
    }
}

/// Return a NIP-01 acknowledgement for every event, without exposing verifier error text.
async fn handle_event(event_json: &Value, state: &RelayState) -> Value {
    let id = event_json.get("id").and_then(Value::as_str).unwrap_or("");
    let acknowledgement = |accepted, reason| serde_json::json!(["OK", id, accepted, reason]);
    let event = match Event::from_json(event_json.to_string()) {
        Ok(event) => event,
        Err(_) => return acknowledgement(false, "invalid: malformed Nostr event"),
    };
    let Some(app_state) = state.app_state.get() else {
        return acknowledgement(false, "error: CMN verifier unavailable; retry later");
    };
    if let Err(error) = crate::nostr::process_cmn_event_from_relay(
        &event,
        app_state,
        state.max_body_bytes,
        app_state.config.pulse.max_clock_skew_s * 1000,
        app_state.config.pulse.max_taste_clock_skew_s * 1000,
    )
    .await
    {
        warn!(error = %error, "Rejected relay event before caching");
        let reason = if error
            .downcast_ref::<crate::ingest::IngestError>()
            .is_some_and(|error| {
                matches!(
                    error,
                    crate::ingest::IngestError::KeyFetchFailed { .. }
                        | crate::ingest::IngestError::SignatureInvalid {
                            fallback_hint: Some(_),
                            ..
                        }
                )
            }) {
            "error: domain verification unavailable; retry later"
        } else if error.downcast_ref::<crate::ingest::StoreError>().is_some() {
            "error: CMN storage unavailable; retry later"
        } else {
            "invalid: CMN verification failed"
        };
        return acknowledgement(false, reason);
    }
    match state.bridge.store_event(&event).await {
        Ok(true) => {
            let _ = state.event_tx.send(event);
            acknowledgement(true, "")
        }
        Ok(false) => acknowledgement(true, "duplicate: already stored"),
        Err(error) => {
            warn!(error = %error, "Failed to persist verified relay event");
            acknowledgement(false, "error: relay storage unavailable; retry later")
        }
    }
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
                    if let Some(ch) = tag_name.chars().next()
                        && let Ok(slt) = SingleLetterTag::from_char(ch)
                    {
                        filter = filter.custom_tags(slt, str_values);
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
    if let Some(kinds) = &filter.inner.kinds
        && !kinds.contains(&event.kind)
    {
        return false;
    }

    // Timestamp checks (NIP-01: since/until)
    if let Some(since) = filter.inner.since
        && event.created_at < since
    {
        return false;
    }
    if let Some(until) = filter.inner.until
        && event.created_at > until
    {
        return false;
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod queue_tests {
    use super::*;

    fn event(sequence: usize) -> Event {
        EventBuilder::new(Kind::Custom(30078), sequence.to_string())
            .finalize(&Keys::generate())
            .unwrap()
    }

    fn subscriptions() -> Arc<RwLock<HashMap<String, CmnFilter>>> {
        Arc::new(RwLock::new(HashMap::from([(
            "live".into(),
            parse_filter(&serde_json::json!({})),
        )])))
    }

    #[tokio::test]
    async fn lagged_stream_signals_connection_close() {
        let subscriptions = subscriptions();
        let guard = subscriptions.write().await;
        let (events, receiver) = broadcast::channel(2);
        let (sender, _outgoing) = mpsc::channel(64);
        let (stop, mut signal) = tokio::sync::watch::channel(false);
        let forward = tokio::spawn(forward_events(
            receiver,
            subscriptions.clone(),
            sender,
            stop,
        ));
        events.send(event(0)).unwrap();
        tokio::task::yield_now().await;
        for sequence in 1..8 {
            events.send(event(sequence)).unwrap();
        }
        drop(guard);
        tokio::time::timeout(std::time::Duration::from_secs(1), signal.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(*signal.borrow());
        forward.await.unwrap();
    }

    #[tokio::test]
    async fn full_client_queue_signals_close_without_waiting_for_reader() {
        let (events, receiver) = broadcast::channel(16);
        let (sender, _outgoing) = mpsc::channel(1);
        let (stop, mut signal) = tokio::sync::watch::channel(false);
        let forward = tokio::spawn(forward_events(receiver, subscriptions(), sender, stop));
        events.send(event(0)).unwrap();
        tokio::task::yield_now().await;
        events.send(event(1)).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), signal.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(*signal.borrow());
        forward.await.unwrap();
    }
}
