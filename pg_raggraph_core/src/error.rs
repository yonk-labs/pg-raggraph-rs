use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("namespace `{0}` not found")]
    NamespaceNotFound(String),

    #[error("provider `{0}` not found")]
    ProviderNotFound(String),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub type CoreResult<T> = Result<T, CoreError>;
