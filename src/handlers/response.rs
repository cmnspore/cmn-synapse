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
        Err(err) => serde_json::json!({
            "kind": "error",
            "error": {
                "code": "internal_error",
                "message": err.to_string(),
                "retryable": false,
            },
            "trace": {},
        }),
    }
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
    let resp = agent_first_data::redacted_value(event.as_value());
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
    let resp = agent_first_data::redacted_value(event.as_value());
    ApiResponse {
        status: StatusCode::OK,
        body: resp,
    }
}

/// Error encoded as an AFDATA protocol-v1 error event.
pub fn error(status: StatusCode, code: &str, message: &str) -> ApiResult {
    error_hint(status, code, message, Some(default_error_hint(code)))
}

/// Like [`error`] but with an actionable hint for remediation.
pub fn error_hint(status: StatusCode, code: &str, message: &str, hint: Option<&str>) -> ApiResult {
    let hint = hint
        .filter(|hint| !hint.trim().is_empty())
        .unwrap_or_else(|| default_error_hint(code));
    let resp = agent_first_data::redacted_value(&event_value(
        agent_first_data::json_error(code, message)
            .hint(hint)
            .trace(Value::Object(base_trace()))
            .build(),
    ));
    ApiResponse { status, body: resp }
}

fn default_error_hint(code: &str) -> &'static str {
    match code {
        "domain_invalid" => "use a valid DNS domain from a CMN URI or cmn.json",
        "hash_invalid" => "use a valid b3 content hash from a CMN spore URI",
        "invalid_capsule" | "schema_invalid" => {
            "submit a signed CMN capsule that matches the CMN schema"
        }
        "not_found" => "verify the domain/hash and wait for indexing before retrying",
        "unauthorized" | "signature_invalid" => "verify the signing key, token, and signature",
        "rate_limited" => "wait before retrying or reduce request rate",
        "internal_error" => "retry later; if it persists, contact the synapse operator",
        _ => "read the error field, correct the request, and retry",
    }
}
