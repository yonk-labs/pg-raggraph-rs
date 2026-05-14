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
use pg_raggraph_core::ingest::pg_client::{
    ChunkEntityRow, ChunkRow, DocRow, EntityCandidate, EntityRow, PgClient, RelRow,
};
use pgrx::prelude::*;
use uuid::Uuid;

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

/// Parse a pgvector text literal of the form `[v1,v2,...]` back into a
/// `Vec<f32>`. Returns `None` on malformed input.
fn parse_vector_literal(s: &str) -> Option<Vec<f32>> {
    let t = s.trim();
    let inner = t.strip_prefix('[').and_then(|x| x.strip_suffix(']'))?;
    if inner.is_empty() {
        return Some(Vec::new());
    }
    let mut out = Vec::new();
    for part in inner.split(',') {
        out.push(part.trim().parse::<f32>().ok()?);
    }
    Some(out)
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

    fn insert_entity(&mut self, row: &EntityRow) -> CoreResult<()> {
        let id = pgrx::Uuid::from_bytes(*row.id.as_bytes());
        // Build the SQL with optional embedding inlined as pgvector literal,
        // matching the pattern used in `insert_chunk` (pgrx 0.17 has no native
        // pgvector binding).
        let emb_sql = match &row.name_emb {
            Some(v) => format!("'{}'::vector", vector_literal(v)),
            None => "NULL".to_string(),
        };
        let sql = format!(
            "INSERT INTO pgrg.entities (id, namespace, name, kind, name_emb, description) \
             VALUES ($1, $2, $3, $4, {emb_sql}, $5) \
             ON CONFLICT (namespace, name, kind) DO NOTHING"
        );
        Spi::connect_mut(|client| {
            client
                .update(
                    &sql,
                    None,
                    &[
                        id.into(),
                        row.namespace.as_str().into(),
                        row.name.as_str().into(),
                        row.kind.as_deref().into(),
                        row.description.as_deref().into(),
                    ],
                )
                .map_err(|e| CoreError::InvalidConfig(format!("spi insert entity: {e}")))?;
            Ok(())
        })
    }

    fn insert_relationship(&mut self, row: &RelRow) -> CoreResult<()> {
        let id = pgrx::Uuid::from_bytes(*row.id.as_bytes());
        let src_id = pgrx::Uuid::from_bytes(*row.src_id.as_bytes());
        let dst_id = pgrx::Uuid::from_bytes(*row.dst_id.as_bytes());
        Spi::connect_mut(|client| {
            client
                .update(
                    "INSERT INTO pgrg.relationships \
                       (id, namespace, src_id, dst_id, kind, weight, description) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7) \
                     ON CONFLICT (namespace, src_id, dst_id, kind) DO NOTHING",
                    None,
                    &[
                        id.into(),
                        row.namespace.as_str().into(),
                        src_id.into(),
                        dst_id.into(),
                        row.kind.as_str().into(),
                        f64::from(row.weight).into(),
                        row.description.as_deref().into(),
                    ],
                )
                .map_err(|e| CoreError::InvalidConfig(format!("spi insert relationship: {e}")))?;
            Ok(())
        })
    }

    fn insert_chunk_entity(&mut self, row: &ChunkEntityRow) -> CoreResult<()> {
        let chunk_id = pgrx::Uuid::from_bytes(*row.chunk_id.as_bytes());
        let entity_id = pgrx::Uuid::from_bytes(*row.entity_id.as_bytes());
        Spi::connect_mut(|client| {
            client
                .update(
                    "INSERT INTO pgrg.chunk_entities (chunk_id, entity_id, confidence) \
                     VALUES ($1, $2, $3) \
                     ON CONFLICT (chunk_id, entity_id) DO NOTHING",
                    None,
                    &[
                        chunk_id.into(),
                        entity_id.into(),
                        f64::from(row.confidence).into(),
                    ],
                )
                .map_err(|e| CoreError::InvalidConfig(format!("spi insert chunk_entity: {e}")))?;
            Ok(())
        })
    }

    fn fuzzy_match_entity(
        &mut self,
        namespace: &str,
        name: &str,
        limit: usize,
    ) -> CoreResult<Vec<EntityCandidate>> {
        // pg_trgm `similarity()` returns float4 in [0, 1]. We deliberately
        // do NOT filter by GUC `pg_trgm.similarity_threshold` here — T21's
        // resolver applies its own cosine threshold on `name_emb`.
        let lim = i64::try_from(limit)
            .map_err(|_| CoreError::InvalidConfig("fuzzy_match_entity: limit too large".into()))?;
        let mut out: Vec<EntityCandidate> = Vec::new();
        Spi::connect(|client| -> CoreResult<()> {
            let tup = client
                .select(
                    "SELECT id, name, similarity(name, $2) AS sim, name_emb::text \
                     FROM pgrg.entities \
                     WHERE namespace = $1 \
                     ORDER BY sim DESC \
                     LIMIT $3",
                    Some(lim),
                    &[namespace.into(), name.into(), lim.into()],
                )
                .map_err(|e| CoreError::InvalidConfig(format!("spi fuzzy_match_entity: {e}")))?;
            for row in tup {
                let id: pgrx::Uuid = row
                    .get::<pgrx::Uuid>(1)
                    .map_err(|e| CoreError::InvalidConfig(format!("spi fuzzy id: {e}")))?
                    .ok_or_else(|| CoreError::InvalidConfig("spi fuzzy id null".into()))?;
                let nm: String = row
                    .get::<String>(2)
                    .map_err(|e| CoreError::InvalidConfig(format!("spi fuzzy name: {e}")))?
                    .ok_or_else(|| CoreError::InvalidConfig("spi fuzzy name null".into()))?;
                let sim: f32 = row
                    .get::<f32>(3)
                    .map_err(|e| CoreError::InvalidConfig(format!("spi fuzzy sim: {e}")))?
                    .unwrap_or(0.0);
                let emb_text: Option<String> = row
                    .get::<String>(4)
                    .map_err(|e| CoreError::InvalidConfig(format!("spi fuzzy emb: {e}")))?;
                let name_emb = emb_text.and_then(|t| parse_vector_literal(&t));
                out.push(EntityCandidate {
                    id: Uuid::from_bytes(*id.as_bytes()),
                    name: nm,
                    trgm_similarity: sim,
                    name_emb,
                });
            }
            Ok(())
        })?;
        Ok(out)
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
