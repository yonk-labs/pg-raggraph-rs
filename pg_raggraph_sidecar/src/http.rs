//! Axum HTTP server for the `POST /v1/ask` endpoint (Plan 5 Slice 4).
//!
//! Mirrors Plan 4's pgrx `pgrg.ask` (`pg_raggraph/src/ask.rs`) data flow
//! exactly — only the SPI layer is swapped for tokio-postgres (SC-005,
//! SC-016). The retrieval SQL, prompt construction, token-budget rule, and
//! the `{answer, citations, signals, mode_used}` JSON shape are all parity
//! with the in-extension `pgrg.ask`.
//!
//! ## The one unavoidable SPI-vs-tokio-postgres delta
//!
//! `build_query_sql(Mode::Hybrid)` embeds `pgrg.embed($1)` in its `q_emb`
//! CTE. `pgrg.embed` is a pgrx `#[pg_extern]` (`pgrg._embed_text`) loaded
//! into Postgres by the extension `.so`. It does **not** exist in the
//! stock/managed Postgres the sidecar targets — the SQL migration files
//! never create it. So the sidecar embeds the question in Rust with the
//! same `EmbeddingBackend` loaded once at startup and substitutes the
//! `pgrg.embed($1)` call with an inline `'[..]'::vector` literal built by
//! the byte-identical `vector_literal` algorithm (the same swap
//! `pg_client.rs` already makes on the ingest write path — DC-001). Every
//! other byte of `build_query_sql` runs verbatim, and `$1` is still bound
//! as the question text (the `bm` lane's `plainto_tsquery('english', $1)`
//! still uses it).
//!
//! ## Error envelopes (SC-013, Task 16)
//!
//! All error responses use a uniform JSON shape:
//! `{"error": "<short, sanitized message>", "code": "<variant>"}`.
//!
//! | Situation | HTTP | `code` |
//! |---|---|---|
//! | Malformed / invalid JSON body | 400 | `bad_request` |
//! | Requested namespace not in `pgrg.namespaces` | 404 | `unknown_namespace` |
//! | Any DB / embedding / provider / internal failure | 500 | `internal` |
//!
//! The HTTP 500 body is always the generic `"internal error"` string. The
//! real cause is logged server-side at `warn` level with connection strings
//! run through `db::redact_conn_string`. No stack trace, no credential, no
//! `postgres://` URL, no master-key path ever reaches the response body.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::async_trait;
use axum::extract::rejection::JsonRejection;
use axum::extract::{FromRequest, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use pg_raggraph_core::embedding::EmbeddingBackend;
use pg_raggraph_core::llm::ask::{AskRequest, ask as core_ask};
use pg_raggraph_core::llm::prompt::PromptChunk;
use pg_raggraph_core::retrieval::Mode;
use pg_raggraph_core::retrieval::query_sql::build_query_sql;
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::config::SidecarConfig;
use crate::{db, embedder, provider_factory};

const fn default_top_k() -> i32 {
    10
}

fn default_namespace() -> String {
    "default".to_string()
}

const fn default_hops() -> i32 {
    1
}

/// `POST /v1/ask` request body. Defaults mirror the pgrx `pgrg.ask`
/// argument defaults (`top_k=10`, `namespace='default'`, `hops=1`).
#[derive(Debug, Deserialize)]
struct AskBody {
    q: String,
    #[serde(default)]
    filter: Option<serde_json::Value>,
    #[serde(default = "default_top_k")]
    top_k: i32,
    #[serde(default = "default_namespace")]
    namespace: String,
    #[serde(default = "default_hops")]
    hops: i32,
    #[serde(default)]
    llm_provider: Option<String>,
}

/// Shared handler state: the parsed config (for `master_key_path`) and the
/// embedding backend loaded once at startup (SC-014) — needed because the
/// sidecar embeds the query in Rust (see module docs).
#[derive(Clone)]
struct AppState {
    cfg: Arc<SidecarConfig>,
    embedder: Arc<dyn EmbeddingBackend>,
}

/// Build a pgvector text literal `[v1,v2,...]`. Byte-identical to
/// `pg_client::vector_literal` / `SpiPgClient::vector_literal` (DC-001) so
/// the query embedding is encoded exactly as ingested embeddings are.
fn vector_literal(v: &[f32]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(v.len() * 8 + 2);
    s.push('[');
    let mut first = true;
    for x in v {
        if !first {
            s.push(',');
        }
        first = false;
        let _ = write!(s, "{x}");
    }
    s.push(']');
    s
}

/// Sanitized error envelope for `POST /v1/ask` (SC-013, Task 16).
///
/// Each variant maps to a distinct HTTP status code and `code` field in the
/// response body. The response body NEVER contains a stack trace, a credential,
/// a `postgres://` URL, the master-key path, or any `CoreError` Display text.
/// Real causes are logged server-side at `warn`/`error` level, with any
/// connection string run through `db::redact_conn_string`.
enum AskError {
    /// Malformed or invalid JSON body → 400 `bad_request`.
    BadRequest(String),
    /// The requested namespace does not exist in `pgrg.namespaces` → 404
    /// `unknown_namespace`.
    UnknownNamespace,
    /// Any DB / embedding / provider / core failure → 500 `internal`.
    /// The wrapped `anyhow::Error` is logged but never sent to the client.
    Internal(anyhow::Error),
}

impl IntoResponse for AskError {
    fn into_response(self) -> Response {
        match self {
            Self::BadRequest(msg) => (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": msg, "code": "bad_request" })),
            )
                .into_response(),

            Self::UnknownNamespace => (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "unknown namespace", "code": "unknown_namespace" })),
            )
                .into_response(),

            Self::Internal(e) => {
                // Log the real cause server-side only. redact_conn_string
                // defends the (unlikely) case the error carries the DB URL.
                tracing::warn!(
                    error = %db::redact_conn_string(&format!("{e:#}")),
                    "POST /v1/ask internal error"
                );
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "internal error", "code": "internal" })),
                )
                    .into_response()
            }
        }
    }
}

/// Blanket conversion from any `anyhow`-compatible error into `Internal`.
/// Used by `?` in the handler for DB / embedding / provider failures.
impl<E: Into<anyhow::Error>> From<E> for AskError {
    fn from(e: E) -> Self {
        Self::Internal(e.into())
    }
}

/// A custom extractor that wraps `axum::Json<T>` and converts `JsonRejection`
/// (malformed body, wrong content-type, …) into `AskError::BadRequest` so
/// the handler returns our sanitized envelope instead of axum's default
/// plaintext / HTML rejection body.
struct AskJson<T>(T);

#[async_trait]
impl<T, S> FromRequest<S> for AskJson<T>
where
    T: serde::de::DeserializeOwned + Send,
    S: Send + Sync,
{
    type Rejection = AskError;

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(Json(v)) => Ok(AskJson(v)),
            Err(rejection) => {
                let msg = match &rejection {
                    JsonRejection::JsonDataError(_) | JsonRejection::JsonSyntaxError(_) => {
                        "invalid JSON body".to_string()
                    }
                    JsonRejection::MissingJsonContentType(_) => {
                        "content-type must be application/json".to_string()
                    }
                    _ => "bad request".to_string(),
                };
                Err(AskError::BadRequest(msg))
            }
        }
    }
}

/// Build the request-scoped retrieval SQL: the verbatim
/// `build_query_sql(Mode::Hybrid)` with its single `pgrg.embed($1)` call
/// (absent in stock Postgres) replaced by the Rust-computed query
/// embedding as an inline `'[..]'::vector` literal, then wrapped exactly
/// like Plan 4's `pgrg.ask` wraps `pgrg.query` — joined to `pgrg.chunks`
/// for `ord` / `token_count`, ordered by score descending.
fn retrieval_sql(query_embedding: &[f32]) -> anyhow::Result<String> {
    let base = build_query_sql(Mode::Hybrid);
    let lit = format!("'{}'::vector", vector_literal(query_embedding));
    // Exactly one occurrence in build_query_sql's `q_emb` CTE.
    let needle = "pgrg.embed($1)";
    if !base.contains(needle) {
        anyhow::bail!("retrieval SQL template changed: `pgrg.embed($1)` not found");
    }
    let inner = base.replacen(needle, &lit, 1);
    // Mirror pgrx ask.rs: SELECT q.chunk_id, q.document_id, ch.ord, q.text,
    // ch.token_count FROM (<query>) q JOIN pgrg.chunks ch ON ch.id =
    // q.chunk_id ORDER BY q.score DESC. `build_query_sql` projects
    // (id, document_id, text, score, sigs) as (c.id, c.document_id,
    // c.text, f.score, f.sigs) — column 1 is the chunk id.
    Ok(format!(
        "SELECT q.chunk_id, q.document_id, ch.ord, q.text, ch.token_count \
         FROM ({inner}) AS q(chunk_id, document_id, text, score, signals) \
         JOIN pgrg.chunks ch ON ch.id = q.chunk_id \
         ORDER BY q.score DESC"
    ))
}

/// Verify the namespace exists in `pgrg.namespaces`; return `UnknownNamespace`
/// if absent. Extracted to keep `ask_handler` within the line budget.
async fn check_namespace(client: &tokio_postgres::Client, namespace: &str) -> Result<(), AskError> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pgrg.namespaces WHERE name = $1)",
            &[&namespace],
        )
        .await
        .map_err(|e| AskError::Internal(e.into()))?
        .get(0);
    if exists {
        Ok(())
    } else {
        Err(AskError::UnknownNamespace)
    }
}

/// Run the retrieval SQL and map rows into `PromptChunk`s.
async fn retrieve_chunks(
    client: &tokio_postgres::Client,
    body: &AskBody,
    query_embedding: &[f32],
) -> Result<Vec<PromptChunk>, AskError> {
    let sql = retrieval_sql(query_embedding)?;
    // build_query_sql binds: $1 q, $2 filter jsonb, $3 top_k, $4 namespace,
    // $5 hops, $6 vec_weight, $7 bm25_weight, $8 graph_weight. `$3` is
    // `LIMIT $3` (bigint in PG); widen top_k from i32 → i64 for the bind.
    let top_k_i64 = i64::from(body.top_k);
    let rows = client
        .query(
            &sql,
            &[
                &body.q,
                &body.filter,
                &top_k_i64,
                &body.namespace,
                &body.hops,
                &1.0_f64,
                &1.0_f64,
                &1.0_f64,
            ],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| PromptChunk {
            chunk_id: r.try_get::<_, Uuid>(0).unwrap_or_else(|_| Uuid::nil()),
            document_id: r.try_get::<_, Uuid>(1).unwrap_or_else(|_| Uuid::nil()),
            ord: r.try_get::<_, i32>(2).unwrap_or(0),
            text: r.try_get::<_, String>(3).unwrap_or_default(),
            token_count: r.try_get::<_, i32>(4).unwrap_or(0),
        })
        .collect())
}

/// `POST /v1/ask` — parity with pgrx `pgrg.ask`.
///
/// Uses the custom `AskJson` extractor so malformed bodies return a sanitized
/// 400 JSON envelope rather than axum's default plaintext rejection.
async fn ask_handler(
    State(state): State<AppState>,
    AskJson(body): AskJson<AskBody>,
) -> Result<Json<serde_json::Value>, AskError> {
    let cfg = &state.cfg;
    let master_key_path = cfg.master_key_path.as_deref();

    // Request-scoped connection.
    let client = db::connect(&cfg.database_url).await?;

    // Namespace existence check → 404 if absent.
    check_namespace(&client, &body.namespace).await?;

    // 1) Resolve provider: explicit -> namespace default -> first LLM match.
    //    Reuse Task 14's provider_factory unchanged.
    let provider = match body.llm_provider.as_deref() {
        Some(name) => provider_factory::build_provider_impl(&client, name, master_key_path).await?,
        None => {
            provider_factory::resolve_or_default_provider(&client, &body.namespace, master_key_path)
                .await
        }
    };

    // Provider name + model for `signals.llm.*` attribution.
    let provider_name = resolve_provider_name(&client, &body).await;
    let provider_model: String = client
        .query_opt(
            "SELECT model FROM pgrg.providers WHERE name = $1",
            &[&provider_name],
        )
        .await
        .ok()
        .flatten()
        .and_then(|r| r.try_get::<_, Option<String>>(0).ok().flatten())
        .unwrap_or_default();

    // 2) Embed the question in Rust (SPI-vs-tokio-postgres delta — see module
    //    docs) and run the parity retrieval SQL.
    let q_emb = state.embedder.embed(&body.q)?;
    let chunks = retrieve_chunks(&client, &body, &q_emb).await?;

    if chunks.is_empty() {
        return Ok(Json(json!({
            "answer": "No relevant context found.",
            "citations": [],
            "signals": { "retrieval": { "chunks_in_prompt": 0, "dropped_for_budget": 0 } },
            "mode_used": "hybrid",
        })));
    }

    // 3) Token budget from namespace settings (default 4000) — SC-012.
    let budget: i32 = client
        .query_opt(
            "SELECT (settings->>'ask_token_budget')::int FROM pgrg.namespaces WHERE name = $1",
            &[&body.namespace],
        )
        .await
        .ok()
        .flatten()
        .and_then(|r| r.try_get::<_, Option<i32>>(0).ok().flatten())
        .unwrap_or(4000);

    // 4) Orchestrate via _core::llm::ask::ask().
    let req = AskRequest {
        question: body.q.clone(),
        chunks,
        provider: provider_name,
        model: provider_model,
        token_budget: budget,
    };
    let out = core_ask(&req, provider.as_ref())?;

    // 5) Parity shape: citations = [{chunk_id, document_id, ord}].
    let citations: Vec<serde_json::Value> = out
        .citations
        .iter()
        .map(|c| {
            json!({
                "chunk_id": c.chunk_id.to_string(),
                "document_id": c.document_id.to_string(),
                "ord": c.ord,
            })
        })
        .collect();

    Ok(Json(json!({
        "answer": out.answer,
        "citations": citations,
        "signals": out.signals,
        "mode_used": out.mode_used,
    })))
}

/// Re-resolve the provider name the factory would pick (explicit ->
/// namespace default -> first LLM). Used only for `signals.llm.provider`
/// attribution — the actual provider impl is built by the factory. On any
/// lookup failure we fall back to an empty string (attribution-only;
/// matches the `MockProvider` fallback the factory uses).
async fn resolve_provider_name(client: &tokio_postgres::Client, body: &AskBody) -> String {
    use pg_raggraph_core::llm::resolve::{ProviderRef, resolve_provider};

    let ns_default: Option<String> = client
        .query_opt(
            "SELECT llm_provider FROM pgrg.namespaces WHERE name = $1",
            &[&body.namespace],
        )
        .await
        .ok()
        .flatten()
        .and_then(|r| r.try_get::<_, Option<String>>(0).ok().flatten());

    let available: Vec<ProviderRef> = match client
        .query("SELECT name, kind FROM pgrg.providers", &[])
        .await
    {
        Ok(rows) => rows
            .iter()
            .map(|r| ProviderRef {
                name: r.try_get::<_, String>(0).unwrap_or_default(),
                kind: r.try_get::<_, String>(1).unwrap_or_default(),
            })
            .collect(),
        Err(_) => Vec::new(),
    };

    resolve_provider(
        body.llm_provider.as_deref(),
        ns_default.as_deref(),
        &available,
    )
    .unwrap_or_default()
}

/// Build the axum router. The embedding backend is loaded once here (same
/// lifecycle as the worker pool) and shared with every request.
///
/// # Errors
/// Returns an error if the embedding backend fails to load.
pub fn router(cfg: Arc<SidecarConfig>) -> anyhow::Result<Router> {
    let embedder = embedder::build_embedder(cfg.embed_dim, cfg.embed_model_path.as_deref())?;
    let state = AppState { cfg, embedder };
    Ok(Router::new()
        .route("/v1/ask", post(ask_handler))
        .with_state(state))
}

/// Bind `cfg.http_bind` and serve the router until the process exits.
///
/// # Errors
/// Returns an error if the address cannot be bound or the server task fails.
pub async fn serve(cfg: Arc<SidecarConfig>) -> anyhow::Result<()> {
    let app = router(Arc::clone(&cfg))?;
    let listener = tokio::net::TcpListener::bind(&cfg.http_bind).await?;
    tracing::info!(http_bind = %cfg.http_bind, "HTTP server listening");
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use axum::response::IntoResponse as _;

    use super::AskError;

    /// Unit test (no DB required): `AskError::Internal` always produces HTTP
    /// 500 with the sanitized `{error:"internal error",code:"internal"}` body.
    /// Asserts the body does NOT contain `postgres://`, `password`, the
    /// master-key path, or any `CoreError` Display text (SC-013, DC-002).
    #[tokio::test]
    async fn internal_error_500_envelope_is_sanitized() {
        // Simulate an internal failure whose Display contains sensitive text.
        let sensitive = "postgres://user:s3cr3t@localhost:5443/db: connection refused";
        let err = AskError::Internal(anyhow::anyhow!("{sensitive}"));
        let response = err.into_response();

        assert_eq!(
            response.status().as_u16(),
            500,
            "AskError::Internal must yield HTTP 500"
        );

        let body_bytes = to_bytes(response.into_body(), 4096)
            .await
            .expect("read body bytes");
        let body_str = std::str::from_utf8(&body_bytes).expect("body is UTF-8");
        let v: serde_json::Value = serde_json::from_str(body_str).expect("body is valid JSON");

        assert_eq!(
            v["code"].as_str(),
            Some("internal"),
            "code must be 'internal': {body_str}"
        );
        assert_eq!(
            v["error"].as_str(),
            Some("internal error"),
            "error field must be the generic sentinel, not the real cause: {body_str}"
        );

        // The sensitive text MUST NOT appear in the response body (DC-002).
        assert!(
            !body_str.contains("postgres://"),
            "response must not contain 'postgres://': {body_str}"
        );
        assert!(
            !body_str.contains("password"),
            "response must not contain 'password': {body_str}"
        );
        assert!(
            !body_str.contains("s3cr3t"),
            "response must not contain the test password: {body_str}"
        );
        assert!(
            !body_str.contains("connection refused"),
            "response must not contain the real error cause: {body_str}"
        );
    }
}
