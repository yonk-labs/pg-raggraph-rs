//! `TokioPgClient` — implements `pg_raggraph_core::ingest::pg_client::PgClient`
//! over `tokio-postgres`. SQL is IDENTICAL to `SpiPgClient` (DC-001 parity);
//! only the execution mechanism differs (`tokio-postgres` vs SPI). Sync trait
//! bridged to async driver via a runtime `Handle` + `block_on`; the job loop
//! (Task 10) guarantees we run inside `spawn_blocking`.
//!
//! Transaction boundary note (DC-006): `SpiPgClient` relies on pgrx's
//! `BackgroundWorker::transaction` wrapper for atomicity; the sidecar's
//! equivalent wrapper is `jobloop::process_one`'s explicit
//! `BEGIN`/`COMMIT`/`ROLLBACK`. So `commit()`/`rollback()` here are intentional
//! no-ops matching `SpiPgClient`'s own no-op contract — mechanism differs, SQL
//! semantics do not.

use pg_raggraph_core::ingest::pg_client::{
    ChunkEntityRow, ChunkRow, DocRow, EntityCandidate, EntityRow, PgClient, RelRow,
};
use pg_raggraph_core::{CoreError, CoreResult};
use tokio::runtime::Handle;
use tokio_postgres::Client;

/// Build a pgvector text literal of the form `[v1,v2,...]`.
///
/// Byte-identical to `SpiPgClient::vector_literal` (DC-001). pgvector inserts
/// inline the vector as a `'[..]'::vector` string literal rather than binding
/// it as a typed parameter, so this helper feeds the same SQL shape SPI uses.
fn vector_literal(v: &[f32]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(v.len() * 8 + 2);
    s.push('[');
    let mut first = true;
    for x in v {
        if !first {
            s.push(',');
        }
        first = false;
        // Byte-identical to SpiPgClient's `s.push_str(&format!("{x}"))`:
        // write! to a String is infallible, so the result is the same bytes.
        let _ = write!(s, "{x}");
    }
    s.push(']');
    s
}

/// tokio-postgres adapter. Stateless w.r.t. the ingest pipeline — every method
/// is a one-shot query. Intended to be invoked from inside the job loop's
/// explicit `BEGIN`/`COMMIT`/`ROLLBACK` (Task 10) so commit / rollback here are
/// no-ops, matching `SpiPgClient`'s contract.
pub struct TokioPgClient {
    client: Client,
    handle: Handle,
}

impl TokioPgClient {
    #[must_use]
    pub fn new(client: Client, handle: Handle) -> Self {
        Self { client, handle }
    }

    /// Wrap a driver error into the same `CoreError` variant `SpiPgClient`
    /// uses (`CoreError::InvalidConfig`). tokio-postgres errors do not carry
    /// connection strings, but keep it credential-free regardless.
    fn map_err<E: std::fmt::Display>(ctx: &str, e: E) -> CoreError {
        CoreError::InvalidConfig(format!("{ctx}: {e}"))
    }
}

impl PgClient for TokioPgClient {
    fn document_exists_by_hash(&mut self, namespace: &str, content_hash: &str) -> CoreResult<bool> {
        self.handle.block_on(async {
            let row = self
                .client
                .query_opt(
                    "SELECT EXISTS(SELECT 1 FROM pgrg.documents \
                     WHERE namespace = $1 AND content_hash = $2)",
                    &[&namespace, &content_hash],
                )
                .await
                .map_err(|e| Self::map_err("spi document_exists", e))?;
            let exists: Option<bool> = match row {
                Some(r) => r
                    .try_get::<_, bool>(0)
                    .map_err(|e| Self::map_err("spi document_exists", e))?
                    .into(),
                None => None,
            };
            Ok(exists.unwrap_or(false))
        })
    }

    fn insert_document(&mut self, doc: &DocRow) -> CoreResult<()> {
        self.handle.block_on(async {
            self.client
                .execute(
                    "INSERT INTO pgrg.documents (id, namespace, source, content_hash, title) \
                     VALUES ($1, $2, $3, $4, $5) ON CONFLICT (content_hash) DO NOTHING",
                    &[
                        &doc.id,
                        &doc.namespace,
                        &doc.source,
                        &doc.content_hash,
                        &doc.title,
                    ],
                )
                .await
                .map_err(|e| Self::map_err("spi insert document", e))?;
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
        self.handle.block_on(async {
            self.client
                .execute(
                    &sql,
                    &[
                        &chunk.id,
                        &chunk.namespace,
                        &chunk.document_id,
                        &chunk.ord,
                        &chunk.text,
                        &chunk.token_count,
                    ],
                )
                .await
                .map_err(|e| Self::map_err("spi insert chunk", e))?;
            Ok(())
        })
    }

    fn insert_entity(&mut self, _row: &EntityRow) -> CoreResult<()> {
        // SAFETY: Task 9b replaces this before any caller (jobloop) exists; no
        // runtime path reaches it in T9.
        unimplemented!("Task 9b: insert_entity")
    }

    fn insert_relationship(&mut self, _row: &RelRow) -> CoreResult<()> {
        // SAFETY: Task 9b replaces this before any caller (jobloop) exists; no
        // runtime path reaches it in T9.
        unimplemented!("Task 9b: insert_relationship")
    }

    fn insert_chunk_entity(&mut self, _row: &ChunkEntityRow) -> CoreResult<()> {
        // SAFETY: Task 9b replaces this before any caller (jobloop) exists; no
        // runtime path reaches it in T9.
        unimplemented!("Task 9b: insert_chunk_entity")
    }

    fn fuzzy_match_entity(
        &mut self,
        _namespace: &str,
        _name: &str,
        _limit: usize,
    ) -> CoreResult<Vec<EntityCandidate>> {
        // SAFETY: Task 9b replaces this before any caller (jobloop) exists; no
        // runtime path reaches it in T9.
        unimplemented!("Task 9b: fuzzy_match_entity")
    }

    fn rollback(&mut self) -> CoreResult<()> {
        // No-op (DC-006): the job loop's explicit ROLLBACK drives discard,
        // mirroring SpiPgClient's reliance on pgrx's transaction wrapper.
        Ok(())
    }

    fn commit(&mut self) -> CoreResult<()> {
        // No-op (DC-006): the job loop's explicit COMMIT drives durability,
        // mirroring SpiPgClient's reliance on pgrx's transaction wrapper.
        Ok(())
    }
}
