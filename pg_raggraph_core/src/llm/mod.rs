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

pub mod mock;

pub use mock::MockProvider;

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
}
