//! `MockProvider` — Plan 3 no-op extractor + Plan 4 stub completer/extractor.
//!
//! Returns empty `Extraction` by default; configurable via `with_stub_answer`
//! (for `complete()` tests, T15a) and `with_stub_extraction` (for `extract()`
//! tests, T24 mock-extractor pgrx flow). CI runs without LLM credentials
//! (SC-017).

use crate::error::CoreResult;
use crate::llm::{Completion, Extraction, LlmProvider};

/// Mock LLM provider. `extract()` returns the configured `stub_extraction`
/// (empty by default). `complete()` returns the configured `stub_answer`
/// (empty by default).
#[derive(Debug, Default, Clone)]
pub struct MockProvider {
    stub_answer: String,
    stub_extraction: Option<Extraction>,
}

impl MockProvider {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            stub_answer: String::new(),
            stub_extraction: None,
        }
    }

    /// Configure the answer returned by `complete()`. Useful for tests that
    /// need deterministic LLM output with hand-set `[N]` citations.
    #[must_use]
    pub fn with_stub_answer(mut self, a: impl Into<String>) -> Self {
        self.stub_answer = a.into();
        self
    }

    /// Configure the structured `Extraction` returned by `extract()`. Used by
    /// the pgrx-side "mock-extractor" provider kind (T24) to inject
    /// deterministic entity/relationship lists without LLM network calls.
    #[must_use]
    pub fn with_stub_extraction(mut self, ex: Extraction) -> Self {
        self.stub_extraction = Some(ex);
        self
    }
}

impl LlmProvider for MockProvider {
    fn extract(&self, _chunk_text: &str, _namespace: &str) -> CoreResult<Extraction> {
        Ok(self.stub_extraction.clone().unwrap_or_default())
    }

    fn complete(&self, _prompt: &str) -> CoreResult<Completion> {
        Ok(Completion {
            text: self.stub_answer.clone(),
            prompt_tokens: 0,
            completion_tokens: 0,
        })
    }
}
