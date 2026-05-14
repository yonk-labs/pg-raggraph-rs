//! `LlmProvider` trait surface — Plan 3 ships the trait and a no-op `MockProvider`.
//!
//! Plan 4 plugs in concrete impls (`OpenAiProvider`, `AnthropicProvider`,
//! `OllamaProvider`) and a `RetryingProvider` wrapper. Per spec §7 line 357,
//! the trait shape matches the `pg_agents` precedent.
//!
//! Mission brief SC-015: trait surface consumable, `MockProvider` available,
//! no real network. Constraint Never: real LLM extraction does not run here.
//!
//! Async note: the trait is currently sync (no Future returns) because
//! Plan 3's only impl is `MockProvider`, which returns synchronously. Plan 4
//! will introduce an async variant or wrap blocking calls inside the bg
//! worker's tokio runtime. Trait shape changes between plans require
//! Constraint Ask First (signal in the Plan 4 brief).

pub mod anthropic;
pub mod ask;
pub mod http;
pub mod mock;
pub mod ollama;
pub mod openai;
pub mod prompt;
pub mod resolve;
pub mod retry;

pub use mock::MockProvider;
pub use prompt::Citation;

use crate::error::CoreResult;
use serde::{Deserialize, Serialize};

/// One extracted entity from a chunk. Lightweight DTO; resolution and
/// upsert happen in `_core::ingest` after the provider returns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractedEntity {
    pub name: String,
    pub kind: Option<String>,
    pub description: Option<String>,
    pub confidence: f32,
}

/// One extracted relationship. `src_name` and `dst_name` reference entity
/// names within the same extraction call; the resolver in `_core::ingest`
/// turns them into UUIDs after entity persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractedRelationship {
    pub src_name: String,
    pub dst_name: String,
    pub kind: String,
    pub weight: f32,
    pub confidence: f32,
}

/// What an `LlmProvider` returns from one `extract()` call.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Extraction {
    pub entities: Vec<ExtractedEntity>,
    pub relationships: Vec<ExtractedRelationship>,
}

/// Free-text completion response. Used by `ask()` for grounded answers.
/// `prompt_tokens` / `completion_tokens` are 0 when the provider doesn't
/// report usage (e.g., `MockProvider`, or providers with limited telemetry).
#[derive(Debug, Clone, Default)]
pub struct Completion {
    pub text: String,
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
}

/// LLM provider trait. Plan 3 defines the surface; Plan 4 ships impls.
///
/// Trait is object-safe (no generics) so the bg worker can hold a
/// `Box<dyn LlmProvider>` configured at namespace lookup time.
pub trait LlmProvider: Send + Sync + 'static {
    /// Extract entities and relationships from `chunk_text` in `namespace`.
    ///
    /// `MockProvider` returns `Extraction::default()`. Plan 4 impls call
    /// network APIs (`OpenAI`, `Anthropic`, `Ollama`).
    ///
    /// # Errors
    /// Returns `CoreError` if the underlying provider call fails. `MockProvider`
    /// never errors.
    fn extract(&self, chunk_text: &str, namespace: &str) -> CoreResult<Extraction>;

    /// Generate a free-text completion for `prompt`. Used by `ask()` for
    /// grounded answers.
    ///
    /// The default implementation returns an error. Providers that support
    /// free-text generation should override (real `OpenAi`/`Anthropic`/`Ollama`
    /// providers do so in Task 15b). `MockProvider` overrides for tests that
    /// don't need real LLM behavior.
    ///
    /// # Errors
    /// Default impl returns `CoreError::Llm("complete() not implemented for this provider")`.
    /// Real impls return `CoreError::Http` on transient transport errors and
    /// `CoreError::Llm` on permanent errors (matching `extract()` mapping).
    fn complete(&self, _prompt: &str) -> CoreResult<Completion> {
        Err(crate::error::CoreError::Llm(
            "complete() not implemented for this provider".into(),
        ))
    }
}
