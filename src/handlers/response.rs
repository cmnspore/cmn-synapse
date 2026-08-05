use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::Value;
use std::sync::OnceLock;

/// Log metadata carried via response extensions, read by the access_log middleware.
/// Avoids reading and re-parsing the response body for logging.
#[derive(Clone, Default)]
pub struct LogMeta {
    pub result: Option<Value>,
    pub error: Option<Value>,
    pub trace: Option<Value>,
}

/// API response that serializes JSON and attaches [`LogMeta`] as a response extension.
pub struct ApiResponse {
    status: StatusCode,
    body: Value,
}

pub type ApiResult = ApiResponse;

impl IntoResponse for ApiResponse {
    fn into_response(self) -> axum::response::Response {
        let log_meta = LogMeta {
            result: self.body.get("result").cloned(),
            error: self.body.get("error").cloned(),
            trace: self.body.get("trace").cloned(),
        };
        let mut resp = (self.status, Json(self.body)).into_response();
        resp.extensions_mut().insert(log_meta);
        resp
    }
}

/// Storage backend name for trace metadata. Set once at startup.
static STORAGE_BACKEND: OnceLock<String> = OnceLock::new();

fn event_value(result: Result<agent_first_data::Event, agent_first_data::BuildError>) -> Value {
    match result {
        Ok(event) => event.into_value(),
        Err(_) => serde_json::json!({
            "kind": "error",
            "error": {
                "code": "internal_error",
                "message": "Failed to construct the response event",
                "hint": "retry later; if it persists, contact the synapse operator",
                "retryable": false,
            },
            "trace": {},
        }),
    }
}

/// Scrub CMN v1's protocol-defined bare `url` fields without renaming them.
fn redact_embedded_cmn_urls(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            for (name, field) in fields {
                if name == "url" {
                    if let Value::String(raw_url) = field {
                        *raw_url = agent_first_data::redact_url_secrets(raw_url);
                    }
                }
                redact_embedded_cmn_urls(field);
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_embedded_cmn_urls(item);
            }
        }
        _ => {}
    }
}

/// Scrub scheme-prefixed URLs before diagnostic prose enters an event.
fn redact_urls_in_text(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut rendered = String::with_capacity(text.len());
    let mut copied_until = 0;
    let mut search_from = 0;

    while let Some(relative) = text[search_from..].find("://") {
        let separator = search_from + relative;
        let mut start = separator;
        while start > 0
            && bytes[start - 1].is_ascii()
            && (bytes[start - 1].is_ascii_alphanumeric()
                || matches!(bytes[start - 1], b'+' | b'-' | b'.'))
        {
            start -= 1;
        }
        if start == separator || !bytes[start].is_ascii_alphabetic() {
            search_from = separator + 3;
            continue;
        }

        let mut end = separator + 3;
        while end < bytes.len()
            && !bytes[end].is_ascii_whitespace()
            && !matches!(
                bytes[end],
                b'"' | b'\'' | b'<' | b'>' | b'{' | b'}' | b'\\' | b'|'
            )
        {
            end += 1;
        }
        while end > separator + 3
            && matches!(
                bytes[end - 1],
                b'.' | b',' | b';' | b':' | b'!' | b')' | b']'
            )
        {
            end -= 1;
        }

        rendered.push_str(&text[copied_until..start]);
        rendered.push_str(&agent_first_data::redact_url_secrets(&text[start..end]));
        copied_until = end;
        search_from = end.max(separator + 3);
    }

    rendered.push_str(&text[copied_until..]);
    rendered
}

fn redact_boundary_value(mut value: Value) -> Value {
    redact_embedded_cmn_urls(&mut value);
    agent_first_data::Redactor::new().redact_in_place(&mut value);
    value
}

/// Set the storage backend name for response traces. Call once at startup.
pub fn set_storage_backend(backend: &str) {
    let _ = STORAGE_BACKEND.set(backend.to_string());
}

/// Build the base trace object with node operational state.
/// Always present in every response so visitors know the node's configuration.
fn base_trace() -> serde_json::Map<String, Value> {
    let mut map = serde_json::Map::new();
    if let Some(backend) = STORAGE_BACKEND.get() {
        map.insert("storage".to_string(), Value::String(backend.clone()));
    }
    map
}

/// Success with result only (trace contains base node state)
pub fn success(result: Value) -> ApiResult {
    let trace = Value::Object(base_trace());
    let event = agent_first_data::json_result(result).trace(trace).build();
    let resp = redact_boundary_value(event.into_value());
    ApiResponse {
        status: StatusCode::OK,
        body: resp,
    }
}

/// Success with extra trace metadata (merges handler fields into base node state)
pub fn success_trace(result: Value, trace: Value) -> ApiResult {
    let mut base = base_trace();
    if let Value::Object(extra) = trace {
        for (k, v) in extra {
            base.insert(k, v);
        }
    }
    let event = agent_first_data::json_result(result)
        .trace(Value::Object(base))
        .build();
    let resp = redact_boundary_value(event.into_value());
    ApiResponse {
        status: StatusCode::OK,
        body: resp,
    }
}

/// Error encoded as an AFDATA protocol-v1 error event.
///
/// `diagnostic_detail` is accepted for source compatibility but is never
/// returned to clients. Callers log it separately; the public response uses
/// the stable code-owned message below so parser, storage, network, and
/// user-controlled errors cannot leak data through protocol prose.
pub fn error(status: StatusCode, code: &str, _diagnostic_detail: &str) -> ApiResult {
    error_hint(
        status,
        code,
        default_error_message(code),
        Some(default_error_hint(code)),
    )
}

fn error_hint(status: StatusCode, code: &str, message: &str, hint: Option<&str>) -> ApiResult {
    let hint = hint
        .filter(|hint| !hint.trim().is_empty())
        .unwrap_or_else(|| default_error_hint(code));
    let message = redact_urls_in_text(message);
    let hint = redact_urls_in_text(hint);
    let resp = redact_boundary_value(event_value(
        agent_first_data::json_error(code, &message)
            .hint(&hint)
            .trace(Value::Object(base_trace()))
            .build(),
    ));
    ApiResponse { status, body: resp }
}

fn default_error_message(code: &str) -> &'static str {
    match code {
        "domain_invalid" => "The domain is invalid",
        "hash_invalid" => "The content hash is invalid",
        "uri_invalid" => "The CMN URI is invalid",
        "field_missing" => "A required request field is missing or invalid",
        "invalid_capsule" | "schema_invalid" => "The submitted CMN capsule is invalid",
        "timestamp_future" => "The submitted timestamp is outside the accepted clock window",
        "key_fetch_failed" => "The signing key could not be resolved",
        "invalid_signature" | "signature_invalid" => {
            "The submitted signature could not be verified"
        }
        "version_not_newer" => "The submitted version is not newer than the stored version",
        "not_found" => "The requested resource was not found",
        "unauthorized" => "The request is not authorized",
        "rate_limited" => "Too many requests",
        "cmn_fetch_failed" => "The live CMN document could not be fetched or verified",
        "cursor_invalid" => "The pagination cursor is invalid",
        "search_unavailable" => "Search is not available on this node",
        "search_failed" => "The search request could not be completed",
        "relay_unavailable" => "The Nostr relay is not available on this node",
        "internal_error" => "The request could not be completed",
        _ => "The request failed",
    }
}

fn default_error_hint(code: &str) -> &'static str {
    match code {
        "domain_invalid" => "use a valid DNS domain from a CMN URI or cmn.json",
        "hash_invalid" => "use a valid b3 content hash from a CMN spore URI",
        "uri_invalid" => "use a CMN URI in the form cmn://domain or cmn://domain/b3.hash",
        "field_missing" => "provide every required request field with the documented type",
        "invalid_capsule" | "schema_invalid" => {
            "submit a signed CMN capsule that matches the CMN schema"
        }
        "timestamp_future" => "synchronize the sender clock and submit the current signed version",
        "key_fetch_failed" | "cmn_fetch_failed" => {
            "verify the domain is reachable and publishes a valid signed cmn.json"
        }
        "version_not_newer" => "submit a version with a later signed timestamp",
        "not_found" => "verify the domain/hash and wait for indexing before retrying",
        "unauthorized" | "invalid_signature" | "signature_invalid" => {
            "verify the signing key, token, and signature"
        }
        "rate_limited" => "wait before retrying or reduce request rate",
        "cursor_invalid" => "omit the cursor or use the opaque cursor from the preceding response",
        "search_unavailable" => "enable search on this node or query another Synapse",
        "search_failed" => "retry later; if it persists, contact the synapse operator",
        "relay_unavailable" => "enable the Nostr relay or connect to another Synapse",
        "internal_error" => "retry later; if it persists, contact the synapse operator",
        _ => "read the error field, correct the request, and retry",
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn response_boundary_redacts_canonical_and_embedded_cmn_fields() {
        let response = success(serde_json::json!({
            "token_secret": "field-canary",
            "capsule": {
                "dist": [{
                    "url": "https://user:password@example.com/archive?token_secret=query-canary"
                }]
            }
        }));
        let rendered = serde_json::to_string(&response.body).unwrap();

        for canary in ["field-canary", "password", "query-canary"] {
            assert!(!rendered.contains(canary), "leaked {canary}: {rendered}");
        }
        assert!(agent_first_data::validate_protocol_event(&response.body, true).is_ok());
    }

    #[test]
    fn error_boundary_does_not_echo_untrusted_diagnostic_prose() {
        let response = error(
            StatusCode::BAD_GATEWAY,
            "upstream_failed",
            "request to (https://user:password@example.com/api?token_secret=query-canary) failed",
        );
        let rendered = serde_json::to_string(&response.body).unwrap();

        assert!(!rendered.contains("password"));
        assert!(!rendered.contains("query-canary"));
        assert_eq!(response.body["error"]["message"], "The request failed");
        assert!(agent_first_data::validate_protocol_event(&response.body, true).is_ok());
    }

    #[test]
    fn diagnostic_url_scanner_scrubs_url_credentials_and_secret_queries() {
        let rendered = redact_urls_in_text(
            "request to https://user:password@example.com/api?token_secret=query-canary failed",
        );

        assert!(!rendered.contains("password"));
        assert!(!rendered.contains("query-canary"));
    }
}
