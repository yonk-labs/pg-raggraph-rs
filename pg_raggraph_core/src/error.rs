use thiserror::Error;

/// Top-level error type for the `pg_raggraph_core` crate.
///
/// A single error type covers the foundation crate. Per-module error
/// types may be introduced when the crate grows ≥3 distinct domains
/// with their own failure modes.
#[derive(Debug, Error)]
pub enum CoreError {
    /// Operation referenced a namespace that doesn't exist in the registry.
    #[error("namespace `{0}` not found")]
    NamespaceNotFound(String),

    /// Operation referenced a provider that hasn't been registered.
    #[error("provider `{0}` not found")]
    ProviderNotFound(String),

    /// Configuration validation failed (length, character set, semantics).
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// JSON (de)serialization failure. Wraps `serde_json::Error` for `?`
    /// ergonomics in code that handles `metadata` / `properties` columns.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error("crypto error: {0}")]
    Crypto(String),
}

pub type CoreResult<T> = Result<T, CoreError>;
