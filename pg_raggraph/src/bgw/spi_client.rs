//! `SpiPgClient` — pgrx-side adapter implementing `_core::ingest::pg_client::PgClient`.
//!
//! Used by the bg worker to bridge `_core::ingest::run_job` to real `PostgreSQL`.
//! The whole pipeline runs inside `BackgroundWorker::transaction(...)` so the
//! commit/rollback semantics are inherited from pgrx's transaction wrapper.
//!
//! Mission brief SC-004 (chunks land with non-NULL embeddings of correct dim),
//! SC-009 (embedder injected; client carries no per-job state), SC-011
//! (per-doc atomicity via the wrapping `BackgroundWorker::transaction`).

use pg_raggraph_core::error::{CoreError, CoreResult};
use pg_raggraph_core::ingest::pg_client::{ChunkRow, DocRow, PgClient};
use pgrx::prelude::*;

/// Build a pgvector text literal of the form `[v1,v2,...]`.
///
/// pgrx 0.17 does not ship a native pgvector binding, so the inserts cast
/// from a string literal to `vector` in SQL (matches `embedding.rs` and
/// `ingest_extracted.rs`).
fn vector_literal(v: &[f32]) -> String {
    let mut s = String::with_capacity(v.len() * 8 + 2);
    s.push('[');
    let mut first = true;
    for x in v {
        if !first {
            s.push(',');
        }
        first = false;
        s.push_str(&format!("{x}"));
    }
    s.push(']');
    s
}

/// SPI adapter. Stateless — every method is a one-shot SPI call, intended to
/// be invoked from inside a surrounding `BackgroundWorker::transaction` so
/// that commit / rollback are driven by the wrapper.
pub(crate) struct SpiPgClient;

impl PgClient for SpiPgClient {
    fn document_exists_by_hash(&mut self, namespace: &str, content_hash: &str) -> CoreResult<bool> {
        let exists: Option<bool> = Spi::get_one_with_args(
            "SELECT EXISTS(SELECT 1 FROM pgrg.documents \
             WHERE namespace = $1 AND content_hash = $2)",
            &[namespace.into(), content_hash.into()],
        )
        .map_err(|e| CoreError::InvalidConfig(format!("spi document_exists: {e}")))?;
        Ok(exists.unwrap_or(false))
    }

    fn insert_document(&mut self, doc: &DocRow) -> CoreResult<()> {
        let id = pgrx::Uuid::from_bytes(*doc.id.as_bytes());
        Spi::connect_mut(|client| {
            client
                .update(
                    "INSERT INTO pgrg.documents (id, namespace, source, content_hash, title) \
                     VALUES ($1, $2, $3, $4, $5) ON CONFLICT (content_hash) DO NOTHING",
                    None,
                    &[
                        id.into(),
                        doc.namespace.as_str().into(),
                        doc.source.as_str().into(),
                        doc.content_hash.as_str().into(),
                        doc.title.as_deref().into(),
                    ],
                )
                .map_err(|e| CoreError::InvalidConfig(format!("spi insert document: {e}")))?;
            Ok(())
        })
    }

    fn insert_chunk(&mut self, chunk: &ChunkRow) -> CoreResult<()> {
        let lit = vector_literal(&chunk.embedding);
        let sql = format!(
            "INSERT INTO pgrg.chunks (id, namespace, document_id, ord, text, token_count, embedding) \
             VALUES ($1, $2, $3, $4, $5, $6, '{lit}'::vector) \
             ON CONFLICT (document_id, ord) DO NOTHING"
        );
        let id = pgrx::Uuid::from_bytes(*chunk.id.as_bytes());
        let document_id = pgrx::Uuid::from_bytes(*chunk.document_id.as_bytes());
        Spi::connect_mut(|client| {
            client
                .update(
                    &sql,
                    None,
                    &[
                        id.into(),
                        chunk.namespace.as_str().into(),
                        document_id.into(),
                        chunk.ord.into(),
                        chunk.text.as_str().into(),
                        chunk.token_count.into(),
                    ],
                )
                .map_err(|e| CoreError::InvalidConfig(format!("spi insert chunk: {e}")))?;
            Ok(())
        })
    }

    fn rollback(&mut self) -> CoreResult<()> {
        // No-op: the wrapping `BackgroundWorker::transaction` rolls back when
        // the closure returns Err. Plan 3 Task 11 keeps `_core::ingest::run_job`
        // PG-agnostic by routing rollback through the trait surface even when
        // the pgrx adapter has nothing to do.
        Ok(())
    }

    fn commit(&mut self) -> CoreResult<()> {
        // No-op: pgrx commits when the closure returns Ok.
        Ok(())
    }
}
