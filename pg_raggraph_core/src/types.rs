use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A validated namespace identifier.
///
/// **Validation contract:** the inner string is trusted by all consumers of
/// this type. Validation (length, character whitelist, reserved-name checks)
/// happens at the pgrx admin function boundary in the extension crate
/// (`pgrg.namespace_create`, etc., introduced in Task 8). Internal callers
/// constructing this type are responsible for ensuring the string has been
/// validated upstream.
///
/// A typed constructor (e.g., `NamespaceName::new(&str) -> CoreResult<Self>`)
/// will be added when validation logic crystallizes (likely Plan 2 or 3 when
/// retrieval starts taking namespace parameters from untrusted call sites).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct NamespaceName(pub String);

/// What a provider produces. Determines which trait it must implement
/// (`LlmProvider` vs `EmbeddingProvider`, both introduced in Plan 4).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ProviderKind {
    /// LLM provider: takes prompts, returns generated text.
    Llm,
    /// Embedding provider: takes text, returns dense vectors.
    Embedding,
}

impl ProviderKind {
    /// Stable string identifier for serialization and database storage.
    ///
    /// Round-trips with [`ProviderKind::parse`]. Values must remain stable
    /// across releases — a rename would silently break stored data.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            ProviderKind::Llm => "llm",
            ProviderKind::Embedding => "embedding",
        }
    }

    /// Parse a provider kind from a string. Returns `None` for unknown kinds.
    ///
    /// Named `parse` rather than `from_str` to avoid shadowing
    /// [`std::str::FromStr::from_str`] (we return `Option`, not `Result`,
    /// because there's no useful error data to surface).
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "llm" => Some(ProviderKind::Llm),
            "embedding" => Some(ProviderKind::Embedding),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DocumentId(pub Uuid);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ChunkId(pub Uuid);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct EntityId(pub Uuid);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct JobId(pub Uuid);
