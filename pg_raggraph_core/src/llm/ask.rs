//! `ask` flow — retrieval results in, grounded answer + citations + signals out.
//!
//! Pure function. The pgrx adapter (`pg_raggraph/src/ask.rs` in T17) fetches
//! chunks via `pgrg.query` (SC-016: shares the retrieval builder), then calls
//! this function with the rows already marshalled into `PromptChunk`s.
//!
//! SC-009: returns answer text + structured citations.
//! SC-010: citations are subset of retrieved `chunk_ids` (forged `[N]` outside
//!   the `id_map` are silently dropped — enforced by `extract_citations` from
//!   the prompt module).
//! SC-018: `signals.llm` carries provider/model/latency/token attribution.

use std::time::Instant;

use serde_json::json;

use crate::error::CoreResult;
use crate::llm::LlmProvider;
use crate::llm::prompt::{Citation, PromptChunk, build_ask_prompt, extract_citations};

#[derive(Debug, Clone)]
pub struct AskRequest {
    pub question: String,
    pub chunks: Vec<PromptChunk>,
    pub provider: String, // attribution-only (for signals.llm.provider)
    pub model: String,    // attribution-only (for signals.llm.model)
    pub token_budget: i32,
}

#[derive(Debug, Clone)]
pub struct AskResult {
    pub answer: String,
    pub citations: Vec<Citation>,
    pub signals: serde_json::Value,
    pub mode_used: String,
}

/// Orchestrate the ask flow: build prompt, call `provider.complete()`, post-map
/// numbered citations back to `chunk_ids`.
///
/// # Errors
/// - `CoreError::InvalidConfig` if budget invalid or `chunks` empty (propagated
///   from [`build_ask_prompt`]).
/// - `CoreError::Http` / `CoreError::Llm` from the underlying provider.
pub fn ask(req: &AskRequest, provider: &dyn LlmProvider) -> CoreResult<AskResult> {
    let built = build_ask_prompt(&req.question, &req.chunks, req.token_budget)?;
    let started = Instant::now();
    let completion = provider.complete(&built.prompt_text)?;
    let latency_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let citations = extract_citations(&completion.text, &built);

    let signals = json!({
        "retrieval": {
            "chunks_in_prompt": built.id_map.len(),
            "dropped_for_budget": built.dropped_count,
        },
        "llm": {
            "provider": req.provider,
            "model": req.model,
            "latency_ms": latency_ms,
            "prompt_tokens": completion.prompt_tokens,
            "completion_tokens": completion.completion_tokens,
        },
    });
    Ok(AskResult {
        answer: completion.text,
        citations,
        signals,
        mode_used: "hybrid".into(),
    })
}
