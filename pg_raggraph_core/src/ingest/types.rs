//! Ingest request / job DTOs — PG-agnostic.
//!
//! `IngestRequest` is the wire form of an ingest call (what the SQL functions
//! enqueue, what the bg worker dequeues). `IngestSource` is one of three
//! variants matching the SQL surface (`pgrg.ingest`, `pgrg.ingest_text`,
//! `pgrg.ingest_bytes`).

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum IngestSource {
    /// `pgrg.ingest(path)` — file path on the PG host filesystem.
    Path(String),
    /// `pgrg.ingest_text(name, content)` — inline text payload.
    Text { name: String, content: String },
    /// `pgrg.ingest_bytes(name, bytes)` — inline binary payload.
    Bytes { name: String, bytes: Vec<u8> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestRequest {
    pub source: IngestSource,
    pub namespace: String,
    pub chunk_strategy: String,
}

impl IngestRequest {
    /// Convenience constructor for path-shaped requests with defaults.
    #[must_use]
    pub fn new_path(path: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            source: IngestSource::Path(path.into()),
            namespace: namespace.into(),
            chunk_strategy: "auto".into(),
        }
    }
}

/// One queue entry in flight. Wraps the request with bookkeeping the worker
/// needs (job id for status updates, `attempt_count` for the reaper).
#[derive(Debug, Clone)]
pub struct IngestJob {
    pub id: Uuid,
    pub request: IngestRequest,
    pub attempt_count: i32,
}
