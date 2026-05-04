use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NamespaceName(pub String);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ProviderKind {
    Llm,
    Embedding,
}

impl ProviderKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentId(pub Uuid);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkId(pub Uuid);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityId(pub Uuid);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobId(pub Uuid);
