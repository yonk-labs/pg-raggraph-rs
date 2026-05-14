//! Shared HTTP client for LLM providers.
//!
//! Sync (uses `reqwest::blocking`) to match the bg worker model: one job per
//! worker process, no tokio runtime. SC-001 (`OpenAI`) and SC-002 (Anthropic /
//! Ollama) build on this in T11–T13. `RetryingProvider` (T10) layers retry
//! and wall-clock budget on top of `HttpClassification`.
//!
//! What this module owns:
//! - `HttpClassification::from_status` — the table that decides retryable vs
//!   permanent for the retry wrapper.
//! - `HttpClient` — a `reqwest::blocking::Client` with a configured timeout
//!   and a stable error surface (`CoreError::Http`).
//!
//! What this module does NOT do:
//! - Provider-specific JSON parsing (lives in `openai.rs` / `anthropic.rs` /
//!   `ollama.rs` in T11–T13).
//! - Retry loops (lives in `retry.rs` in T10).

use std::time::Duration;

use crate::error::{CoreError, CoreResult};

/// Default total request timeout (per attempt; `RetryingProvider` adds a
/// wall-clock budget cap on top across retries in T10).
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// How `RetryingProvider` (T10) should treat a response status code.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum HttpClassification {
    /// 2xx — provider call succeeded; caller parses the body.
    Ok,
    /// 429 or 5xx — back off and retry per the configured policy.
    Retryable,
    /// Other 4xx (auth, bad request, schema mismatch) — fail fast.
    Permanent,
}

impl HttpClassification {
    /// Map an HTTP status code to a retry classification.
    ///
    /// Table:
    /// - 200..=299 → `Ok`
    /// - 429 or 500..=599 → `Retryable`
    /// - everything else → `Permanent`
    #[must_use]
    pub const fn from_status(code: u16) -> Self {
        match code {
            200..=299 => Self::Ok,
            429 | 500..=599 => Self::Retryable,
            _ => Self::Permanent,
        }
    }
}

/// Thin wrapper around `reqwest::blocking::Client` with a configured timeout
/// and consistent error mapping to `CoreError::Http`.
#[derive(Debug, Clone)]
pub struct HttpClient {
    inner: reqwest::blocking::Client,
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpClient {
    /// Build a client with `DEFAULT_TIMEOUT`.
    #[must_use]
    pub fn new() -> Self {
        Self::with_timeout(DEFAULT_TIMEOUT)
    }

    /// Build a client with the given per-request timeout.
    ///
    /// # Panics
    /// Panics if `reqwest` cannot build a `blocking::Client`. The builder
    /// only fails on TLS init for invalid root certs, which would mean the
    /// host system is broken — there is no recoverable path.
    #[must_use]
    pub fn with_timeout(t: Duration) -> Self {
        let inner = reqwest::blocking::Client::builder()
            .timeout(t)
            .user_agent("pg-raggraph/0.1")
            .build()
            .expect("reqwest blocking client build");
        Self { inner }
    }

    /// GET a URL. Returns the response body as a string on 2xx; otherwise
    /// returns `CoreError::Http` with the status code.
    ///
    /// Used by `ollama.rs` (T13) for `/api/tags` and similar discovery
    /// endpoints. `RetryingProvider` (T10) does not currently route GETs
    /// through classification — non-2xx GET is reported as a flat error.
    ///
    /// # Errors
    /// Returns `CoreError::Http` if the request fails to send, the body
    /// can't be read, or the status is non-2xx.
    pub fn get(&self, url: &str) -> CoreResult<String> {
        let resp = self
            .inner
            .get(url)
            .send()
            .map_err(|e| CoreError::Http(format!("send: {e}")))?;
        if resp.status().is_success() {
            resp.text()
                .map_err(|e| CoreError::Http(format!("read body: {e}")))
        } else {
            Err(CoreError::Http(format!("status {}", resp.status())))
        }
    }

    /// POST a JSON body with arbitrary headers. Returns `(status_code, response_body)`.
    ///
    /// This is the general form: all provider-specific header shapes (Bearer auth,
    /// `x-api-key`, `anthropic-version`, etc.) go through here so every POST
    /// inherits the configured timeout and User-Agent from the shared client.
    ///
    /// Returning the raw `(status, body)` pair (instead of `Result<String>`)
    /// is deliberate: the retry wrapper in T10 needs the status code to
    /// decide whether to back off or fail fast, and the body may contain a
    /// provider-specific error JSON we want to preserve for logging.
    ///
    /// # Errors
    /// Returns `CoreError::Http` only on transport-level failures (DNS,
    /// connect, send, read body). Non-2xx responses return `Ok` here and
    /// are classified by the caller.
    pub fn post_json_with_headers(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: &serde_json::Value,
    ) -> CoreResult<(u16, String)> {
        let mut req = self.inner.post(url).json(body);
        for (name, value) in headers {
            req = req.header(*name, *value);
        }
        let resp = req
            .send()
            .map_err(|e| CoreError::Http(format!("send: {e}")))?;
        let status = resp.status().as_u16();
        let body = resp
            .text()
            .map_err(|e| CoreError::Http(format!("read body: {e}")))?;
        Ok((status, body))
    }

    /// POST a JSON body with optional Bearer auth. Thin wrapper over
    /// [`Self::post_json_with_headers`] preserved for `OpenAI`'s auth shape.
    ///
    /// # Errors
    /// Returns `CoreError::Http` on transport-level failures.
    pub fn post_json(
        &self,
        url: &str,
        bearer: Option<&str>,
        body: &serde_json::Value,
    ) -> CoreResult<(u16, String)> {
        let bearer_value = bearer.map(|t| format!("Bearer {t}"));
        let headers: Vec<(&str, &str)> = bearer_value
            .as_deref()
            .map(|v| vec![("authorization", v)])
            .unwrap_or_default();
        self.post_json_with_headers(url, &headers, body)
    }
}
