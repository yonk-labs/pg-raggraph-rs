//! `PgClient` injection trait — lets `_core::ingest::run_job` run unit-tested
//! without a real `PostgreSQL`.
//!
//! The pgrx-side adapter (Task 11) wraps `pgrx::Spi`. The `FakePgClient`
//! impl below is for `cargo test` and lives next to the trait so unit tests
//! never need to depend on the pgrx crate.
//!
//! Mission brief SC-011 (per-doc transaction atomicity) and SC-017 (cargo
//! test-able without PG). The trait surface is intentionally minimal in
//! Plan 3 — Plan 4 will extend it with entity/relationship persistence
//! once the `LlmProvider` impls land.

use crate::error::CoreResult;
use uuid::Uuid;

/// One persisted document — the row written into `pgrg.documents`.
#[derive(Debug, Clone)]
pub struct DocRow {
    pub id: Uuid,
    pub namespace: String,
    pub source: String,
    pub content_hash: String,
    pub title: Option<String>,
}

/// One persisted chunk — the row written into `pgrg.chunks`.
#[derive(Debug, Clone)]
pub struct ChunkRow {
    pub id: Uuid,
    pub document_id: Uuid,
    pub namespace: String,
    pub ord: i32,
    pub text: String,
    pub token_count: i32,
    pub embedding: Vec<f32>,
}

/// Trait the `run_job` pipeline uses to persist into PG.
///
/// Methods are sync — the pgrx adapter performs SPI calls inside
/// `BackgroundWorker::transaction` (Task 11). Errors are returned through
/// `CoreResult` so the `run_job` can roll back deterministically.
pub trait PgClient {
    /// Returns `true` if a document with `content_hash` already exists in `namespace`.
    fn document_exists_by_hash(&mut self, namespace: &str, content_hash: &str) -> CoreResult<bool>;

    /// Insert a document row. Caller must check `document_exists_by_hash` first.
    fn insert_document(&mut self, doc: &DocRow) -> CoreResult<()>;

    /// Insert one chunk row.
    fn insert_chunk(&mut self, chunk: &ChunkRow) -> CoreResult<()>;

    /// Discard everything written in the current logical transaction.
    ///
    /// In the pgrx adapter (Task 11), this is a no-op because
    /// `BackgroundWorker::transaction` rolls back on `Err` return; in the
    /// fake, we drop the buffered writes.
    fn rollback(&mut self) -> CoreResult<()>;

    /// Commit (no-op in pgrx adapter; flushes the fake's buffer into the
    /// canonical state).
    fn commit(&mut self) -> CoreResult<()>;
}

/// Test-only in-memory `PgClient`. Buffers writes; rolls back by clearing.
///
/// Writes go to `buffered_documents` / `buffered_chunks` first; `commit`
/// moves them into `documents` / `chunks` (the canonical state inspected
/// by tests); `rollback` clears the buffers without touching the canonical
/// state.
#[derive(Debug, Default)]
pub struct FakePgClient {
    pub documents: Vec<DocRow>,
    pub chunks: Vec<ChunkRow>,
    /// Plan 4 will populate these via real `LlmProvider`; Plan 3 always empty.
    pub entities: Vec<()>,
    pub relationships: Vec<()>,
    /// If `Some(n)`, the n-th chunk insert (0-indexed) returns `Err`.
    chunk_fail_at: Option<usize>,
    chunk_inserts: usize,
    /// Buffer that becomes the canonical state on commit.
    buffered_documents: Vec<DocRow>,
    buffered_chunks: Vec<ChunkRow>,
}

impl FakePgClient {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Configure the n-th chunk insert (0-indexed) to fail, simulating a
    /// per-row write error from the underlying store.
    #[must_use]
    pub fn with_chunk_write_failure_at(mut self, n: usize) -> Self {
        self.chunk_fail_at = Some(n);
        self
    }
}

impl PgClient for FakePgClient {
    fn document_exists_by_hash(&mut self, namespace: &str, content_hash: &str) -> CoreResult<bool> {
        Ok(self
            .documents
            .iter()
            .any(|d| d.namespace == namespace && d.content_hash == content_hash))
    }

    fn insert_document(&mut self, doc: &DocRow) -> CoreResult<()> {
        self.buffered_documents.push(doc.clone());
        Ok(())
    }

    fn insert_chunk(&mut self, chunk: &ChunkRow) -> CoreResult<()> {
        if Some(self.chunk_inserts) == self.chunk_fail_at {
            self.chunk_inserts += 1;
            return Err(crate::error::CoreError::InvalidConfig(
                "synthetic chunk write failure".into(),
            ));
        }
        self.chunk_inserts += 1;
        self.buffered_chunks.push(chunk.clone());
        Ok(())
    }

    fn rollback(&mut self) -> CoreResult<()> {
        self.buffered_documents.clear();
        self.buffered_chunks.clear();
        self.chunk_inserts = 0;
        Ok(())
    }

    fn commit(&mut self) -> CoreResult<()> {
        self.documents.append(&mut self.buffered_documents);
        self.chunks.append(&mut self.buffered_chunks);
        self.chunk_inserts = 0;
        Ok(())
    }
}
