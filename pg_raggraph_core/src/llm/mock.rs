//! `MockProvider` — Plan 3 no-op extractor + Plan 4 stub completer.
//!
//! Returns empty `Extraction` from `extract()`; configurable stub answer
//! from `complete()` via `with_stub_answer(...)`. CI runs without any LLM
//! credentials (SC-017).

use crate::error::CoreResult;
use crate::llm::{Completion, Extraction, LlmProvider};

/// Mock LLM provider. `extract()` always returns empty. `complete()` returns
/// the configured `stub_answer` (empty by default).
#[derive(Debug, Default, Clone)]
pub struct MockProvider {
    stub_answer: String,
}

impl MockProvider {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            stub_answer: String::new(),
        }
    }

    /// Configure the answer returned by `complete()`. Useful for tests that
    /// need deterministic LLM output with hand-set `[N]` citations.
    #[must_use]
    pub fn with_stub_answer(mut self, a: impl Into<String>) -> Self {
        self.stub_answer = a.into();
        self
    }
}

impl LlmProvider for MockProvider {
    fn extract(&self, _chunk_text: &str, _namespace: &str) -> CoreResult<Extraction> {
        Ok(Extraction::default())
    }

    fn complete(&self, _prompt: &str) -> CoreResult<Completion> {
        Ok(Completion {
            text: self.stub_answer.clone(),
            prompt_tokens: 0,
            completion_tokens: 0,
        })
    }
}
