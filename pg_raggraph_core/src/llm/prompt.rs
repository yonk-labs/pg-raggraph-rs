//! Citation-required prompt builder for `pgrg.ask`.
//!
//! SC-010: LLM sees numbered context blocks `[1]`, `[2]`, ... but never raw
//! `chunk_ids`. After the LLM returns text with `[N]` citations, the caller
//! maps N -> `chunk_id` using `BuiltPrompt::id_map`. Citation forgery becomes
//! impossible by construction.
//!
//! SC-012: token budget enforced greedily in retrieval order. Chunks past
//! the budget are dropped (counted in `dropped_count` for the signals jsonb).

use uuid::Uuid;

use crate::error::{CoreError, CoreResult};

#[derive(Debug, Clone)]
pub struct PromptChunk {
    pub chunk_id: Uuid,
    pub document_id: Uuid,
    pub ord: i32,
    pub text: String,
    pub token_count: i32,
}

#[derive(Debug)]
pub struct BuiltPrompt {
    pub prompt_text: String,
    /// `id_map[i]` is the `chunk_id` for the `[i+1]` citation marker.
    pub id_map: Vec<Uuid>,
    /// `doc_map[i]` is the `document_id` for `[i+1]` (parallel to `id_map`).
    pub doc_map: Vec<Uuid>,
    pub ord_map: Vec<i32>,
    pub prompt_tokens: i32,
    pub dropped_count: usize,
}

const SYSTEM_PROMPT: &str = "\
You are a careful assistant that answers questions using ONLY the provided context blocks.

For every factual claim, cite the supporting block as `[N]` where N is the block number.
Do not invent citations. Do not cite blocks not provided. If the answer is not in the context, say so.";

/// Build a citation-required prompt for `pgrg.ask`.
///
/// # Errors
/// Returns `CoreError::InvalidConfig` if `budget <= 0`, the chunks slice is
/// empty, or the very first chunk exceeds `budget` (caller should retry with
/// a smaller or non-existent context).
pub fn build_ask_prompt(
    question: &str,
    chunks: &[PromptChunk],
    budget: i32,
) -> CoreResult<BuiltPrompt> {
    if budget <= 0 {
        return Err(CoreError::InvalidConfig(format!("invalid budget {budget}")));
    }
    if chunks.is_empty() {
        return Err(CoreError::InvalidConfig("ask: no context chunks".into()));
    }
    if chunks[0].token_count > budget {
        return Err(CoreError::InvalidConfig(format!(
            "first chunk ({} tokens) exceeds budget ({budget})",
            chunks[0].token_count
        )));
    }
    let mut running = 0i32;
    let mut included: Vec<&PromptChunk> = Vec::new();
    let mut dropped = 0usize;
    for c in chunks {
        if running + c.token_count <= budget {
            running += c.token_count;
            included.push(c);
        } else {
            dropped += 1;
        }
    }

    let mut prompt =
        String::with_capacity(included.iter().map(|c| c.text.len() + 32).sum::<usize>() + 256);
    prompt.push_str(SYSTEM_PROMPT);
    prompt.push_str("\n\n## Context\n");
    for (i, c) in included.iter().enumerate() {
        use std::fmt::Write as _;
        let _ = write!(prompt, "[{n}] {}\n\n", c.text, n = i + 1);
    }
    prompt.push_str("## Question\n");
    prompt.push_str(question);
    prompt.push_str("\n\n## Answer\n");

    Ok(BuiltPrompt {
        prompt_text: prompt,
        id_map: included.iter().map(|c| c.chunk_id).collect(),
        doc_map: included.iter().map(|c| c.document_id).collect(),
        ord_map: included.iter().map(|c| c.ord).collect(),
        prompt_tokens: running,
        dropped_count: dropped,
    })
}

/// One extracted citation from an LLM answer.
#[derive(Debug, Clone)]
pub struct Citation {
    pub n: usize,
    pub chunk_id: Uuid,
    pub document_id: Uuid,
    pub ord: i32,
}

/// Extract `[1]`, `[2]`, ... citations from `answer_text`. Returns a sorted
/// dedup'd list of (`citation_number`, `chunk_id`, `document_id`, `ord`) for inclusion
/// in the `citations` jsonb of `pgrg.ask`.
///
/// Citations referencing N outside `built.id_map` (i.e., forged or
/// out-of-bound numbers like `[99]`) are SILENTLY DROPPED — they cannot
/// resolve to a real `chunk_id` by construction.
#[must_use]
pub fn extract_citations(answer_text: &str, built: &BuiltPrompt) -> Vec<Citation> {
    let mut seen = std::collections::BTreeSet::new();
    let re = regex_lite::Regex::new(r"\[(\d+)\]").expect("citation regex");
    let mut out = Vec::new();
    for cap in re.captures_iter(answer_text) {
        if let Some(s) = cap.get(1)
            && let Ok(n) = s.as_str().parse::<usize>()
            && n >= 1
            && n <= built.id_map.len()
            && seen.insert(n)
        {
            out.push(Citation {
                n,
                chunk_id: built.id_map[n - 1],
                document_id: built.doc_map[n - 1],
                ord: built.ord_map[n - 1],
            });
        }
    }
    out
}
