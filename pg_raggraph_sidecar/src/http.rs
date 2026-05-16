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
//! Errors here return a single sanitized HTTP 500 `{error, code:"internal"}`
//! — no stack trace, no credential, no connection string. The rich
//! 400/404/500 envelope matrix is Task 16.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::State;
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

/// The sanitized error envelope. The `Display` of the source error is logged
/// server-side at warn level and never returned to the client (no stack
/// trace, no credential, no connection string). Task 16 expands this into a
/// 400/404/500 matrix; Task 15 only needs the happy path + a single 500.
struct AskError(anyhow::Error);

impl<E: Into<anyhow::Error>> From<E> for AskError {
    fn from(e: E) -> Self {
        Self(e.into())
    }
}

impl IntoResponse for AskError {
    fn into_response(self) -> Response {
        // Log the real cause server-side only. redact_conn_string defends
        // the (unlikely) case the error text carries the DB URL.
        tracing::warn!(
            error = %db::redact_conn_string(&format!("{:#}", self.0)),
            "POST /v1/ask failed"
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "internal error", "code": "internal" })),
        )
            .into_response()
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

/// `POST /v1/ask` — parity with pgrx `pgrg.ask`.
async fn ask_handler(
    State(state): State<AppState>,
    Json(body): Json<AskBody>,
) -> Result<Json<serde_json::Value>, AskError> {
    let cfg = &state.cfg;
    let master_key_path = cfg.master_key_path.as_deref();

    // Request-scoped connection.
    let client = db::connect(&cfg.database_url).await?;

    // 1) Resolve provider: explicit -> namespace default -> first LLM match.
    //    Reuse Task 14's provider_factory unchanged.
    let provider = match body.llm_provider.as_deref() {
        Some(name) => provider_factory::build_provider_impl(&client, name, master_key_path).await?,
        None => {
            provider_factory::resolve_or_default_provider(&client, &body.namespace, master_key_path)
                .await
        }
    };

    // Provider name + model for `signals.llm.*` attribution. Mirrors pgrx
    // ask.rs which reads the resolved name + `provider_model_for`. We
    // re-resolve the name the same way the factory does (explicit ->
    // namespace default -> first LLM) so attribution is faithful even
    // through the MockProvider fallback path.
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

    // 2) Retrieval. Embed the question in Rust (the SPI-vs-tokio-postgres
    //    delta — see module docs), then run the parity retrieval SQL.
    let q_emb = state.embedder.embed(&body.q)?;
    let sql = retrieval_sql(&q_emb)?;

    // build_query_sql binds: $1 q, $2 filter jsonb, $3 top_k, $4 namespace,
    // $5 hops, $6 vec_weight, $7 bm25_weight, $8 graph_weight. Weights are
    // the parity defaults (all 1.0 — RrfWeights::default()). `$3` is used
    // as `LIMIT $3`, which Postgres types as `bigint`; SPI (pgrx) coerces
    // i32 transparently but tokio-postgres binds by exact type, so `top_k`
    // is widened to i64 for the bind.
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

    let chunks: Vec<PromptChunk> = rows
        .iter()
        .map(|r| PromptChunk {
            chunk_id: r.try_get::<_, Uuid>(0).unwrap_or_else(|_| Uuid::nil()),
            document_id: r.try_get::<_, Uuid>(1).unwrap_or_else(|_| Uuid::nil()),
            ord: r.try_get::<_, i32>(2).unwrap_or(0),
            text: r.try_get::<_, String>(3).unwrap_or_default(),
            token_count: r.try_get::<_, i32>(4).unwrap_or(0),
        })
        .collect();

    if chunks.is_empty() {
        // Same empty-context shape pgrx ask.rs returns.
        return Ok(Json(json!({
            "answer": "No relevant context found.",
            "citations": [],
            "signals": { "retrieval": { "chunks_in_prompt": 0, "dropped_for_budget": 0 } },
            "mode_used": "hybrid",
        })));
    }

    // 3) Token budget from namespace settings (default 4000) — SC-012,
    //    EXACT pgrx ask.rs query.
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

    // 4) Orchestrate via _core::llm::ask::ask() — UNCHANGED.
    let req = AskRequest {
        question: body.q.clone(),
        chunks,
        provider: provider_name,
        model: provider_model,
        token_budget: budget,
    };
    let out = core_ask(&req, provider.as_ref())?;

    // 5) Serialize with the SAME shape pgrx ask.rs returns: citations =
    //    array of {chunk_id, document_id, ord} (the `n` field is dropped).
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
