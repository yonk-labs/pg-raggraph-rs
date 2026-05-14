//! `pgrg.ask` SQL function — retrieves chunks via `pgrg.query`, builds a
//! grounded answer with `[N]` citations resolved back to `chunk_id`s.
//!
//! SC-009 / SC-010 / SC-011 / SC-012 / SC-016 / SC-018.
//!
//! Security: decrypts the provider's credential at use site using
//! `pg_raggraph.master_key_path` (if set). Plaintext credentials are never
//! logged or returned in error messages.
//!
//! Reuses `pgrg.query` for retrieval (SC-016) — this module does not
//! construct retrieval SQL.

use pg_raggraph_core::llm::ask::{AskRequest, ask as core_ask};
use pg_raggraph_core::llm::prompt::PromptChunk;
use pg_raggraph_core::llm::resolve::{ProviderRef, resolve_provider};
use pgrx::prelude::*;

use crate::provider_factory;

/// `pgrg.ask(q, filter, top_k, namespace, hops, llm_provider)`
///
/// Returns one row: `(answer, citations, signals, mode_used)`.
///
/// - `citations` is a JSONB array of `{chunk_id, document_id, ord}` objects
///   (subset of retrieved chunks — SC-010 enforced upstream).
/// - `signals` carries `retrieval` + `llm` attribution (SC-018).
/// - `mode_used` is `"hybrid"` (Plan 4 has no smart-mode escalation yet).
#[pg_extern]
fn ask(
    q: &str,
    filter: default!(Option<pgrx::JsonB>, "NULL"),
    top_k: default!(i32, "10"),
    namespace: default!(&str, "'default'"),
    hops: default!(i32, "1"),
    llm_provider: default!(Option<&str>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(answer, String),
        name!(citations, pgrx::JsonB),
        name!(signals, pgrx::JsonB),
        name!(mode_used, String),
    ),
> {
    // 1) Resolve provider: explicit -> namespace.llm_provider -> first LLM match.
    let ns_default: Option<String> = Spi::get_one_with_args(
        "SELECT llm_provider FROM pgrg.namespaces WHERE name = $1",
        &[namespace.into()],
    )
    .ok()
    .flatten();

    let available: Vec<ProviderRef> = Spi::connect(|client| {
        client
            .select("SELECT name, kind FROM pgrg.providers", None, &[])
            .expect("pgrg.ask: providers select")
            .map(|r| ProviderRef {
                name: r.get::<String>(1).ok().flatten().unwrap_or_default(),
                kind: r.get::<String>(2).ok().flatten().unwrap_or_default(),
            })
            .collect()
    });

    let provider_name = match resolve_provider(llm_provider, ns_default.as_deref(), &available) {
        Ok(n) => n,
        Err(e) => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
                format!("pgrg.ask: {e}")
            );
        }
    };

    // 2) Fetch retrieval results via pgrg.query (SC-016 — reuse, don't fork).
    //    pgrg.query returns: (chunk_id uuid, document_id uuid, text text,
    //                         score float8, signals jsonb).
    //    Join to pgrg.chunks to pick up `ord` and `token_count` for prompt
    //    construction. pgrg.query takes 7 args: q, filter, top_k, namespace,
    //    hops, weights (NULL = defaults), mode ('hybrid' by default).
    let rows: Vec<PromptChunk> = Spi::connect(|client| {
        client
            .select(
                "SELECT q.chunk_id, q.document_id, ch.ord, q.text, ch.token_count \
                 FROM pgrg.query($1, $2, $3, $4, $5, NULL::jsonb, 'hybrid') AS q \
                 JOIN pgrg.chunks ch ON ch.id = q.chunk_id \
                 ORDER BY q.score DESC",
                None,
                &[
                    q.into(),
                    filter.into(),
                    top_k.into(),
                    namespace.into(),
                    hops.into(),
                ],
            )
            .expect("pgrg.ask: pgrg.query select")
            .map(|r| PromptChunk {
                chunk_id: uuid_from_row(r.get::<pgrx::Uuid>(1).ok().flatten()),
                document_id: uuid_from_row(r.get::<pgrx::Uuid>(2).ok().flatten()),
                ord: r.get::<i32>(3).ok().flatten().unwrap_or(0),
                text: r.get::<String>(4).ok().flatten().unwrap_or_default(),
                token_count: r.get::<i32>(5).ok().flatten().unwrap_or(0),
            })
            .collect()
    });

    if rows.is_empty() {
        return TableIterator::once((
            "No relevant context found.".into(),
            pgrx::JsonB(serde_json::json!([])),
            pgrx::JsonB(serde_json::json!({
                "retrieval": {"chunks_in_prompt": 0, "dropped_for_budget": 0}
            })),
            "hybrid".into(),
        ));
    }

    // 3) Token budget from namespace settings (default 4000) — SC-012.
    let budget: Option<i32> = Spi::get_one_with_args(
        "SELECT (settings->>'ask_token_budget')::int FROM pgrg.namespaces WHERE name = $1",
        &[namespace.into()],
    )
    .ok()
    .flatten();
    let budget = budget.unwrap_or(4000);

    // 4) Build provider impl (decrypts credential at use site).
    let provider_impl = provider_factory::build_provider_impl(&provider_name);
    let provider_model = provider_factory::provider_model_for(&provider_name);

    // 5) Orchestrate via _core::llm::ask::ask().
    let req = AskRequest {
        question: q.to_string(),
        chunks: rows,
        provider: provider_name.clone(),
        model: provider_model,
        token_budget: budget,
    };

    let out = match core_ask(&req, &*provider_impl) {
        Ok(o) => o,
        Err(e) => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("pgrg.ask: {e}")
            );
        }
    };

    let citations_json: Vec<serde_json::Value> = out
        .citations
        .iter()
        .map(|c| {
            serde_json::json!({
                "chunk_id":    c.chunk_id.to_string(),
                "document_id": c.document_id.to_string(),
                "ord":         c.ord,
            })
        })
        .collect();

    TableIterator::once((
        out.answer,
        pgrx::JsonB(serde_json::Value::Array(citations_json)),
        pgrx::JsonB(out.signals),
        out.mode_used,
    ))
}

/// Convert `Option<pgrx::Uuid>` to `uuid::Uuid` for `PromptChunk`. Defaults
/// to `Uuid::nil()` for NULL rows (pgrg.query enforces NOT NULL upstream).
fn uuid_from_row(o: Option<pgrx::Uuid>) -> uuid::Uuid {
    o.and_then(|u| uuid::Uuid::parse_str(&u.to_string()).ok())
        .unwrap_or(uuid::Uuid::nil())
}
