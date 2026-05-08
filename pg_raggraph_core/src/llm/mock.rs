//! `MockProvider` — Plan 3 no-op extractor.
//!
//! Returns empty `Extraction`; satisfies `LlmProvider` so the bg worker
//! can run the full ingest happy path without network calls. Plan 4 ships
//! `OpenAiProvider`, `AnthropicProvider`, `OllamaProvider`.

use crate::error::CoreResult;
use crate::llm::{Extraction, LlmProvider};

/// No-op extractor. Always returns `Extraction::default()`.
#[derive(Debug, Default, Clone, Copy)]
pub struct MockProvider;

impl MockProvider {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl LlmProvider for MockProvider {
    fn extract(&self, _chunk_text: &str, _namespace: &str) -> CoreResult<Extraction> {
        Ok(Extraction::default())
    }
}
