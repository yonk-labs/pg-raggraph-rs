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
use uuid::Uuid;

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

/// Parse a pgvector text literal of the form `[v1,v2,...]` back into a
/// `Vec<f32>`. Returns `None` on malformed input.
///
/// Byte-identical to `SpiPgClient::parse_vector_literal` (DC-001). Used by
/// `fuzzy_match_entity` to round-trip `name_emb::text` back into a `Vec<f32>`.
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

    /// Consume the client, returning the owned tokio-postgres `Client` and the
    /// runtime `Handle`. Used by `jobloop::process_one` to drive the explicit
    /// transaction boundary (`BEGIN`/`COMMIT`/`ROLLBACK`) and the queue status
    /// `UPDATE` on the SAME connection `run_job`'s INSERTs used — a Postgres
    /// transaction is per-connection, and [`TokioPgClient::commit`]/[`TokioPgClient::rollback`]
    /// are intentional DC-006 no-ops (the real boundary lives in the job loop,
    /// mirroring how `SpiPgClient` relies on pgrx's transaction wrapper).
    /// Without this, the per-job transaction cannot be committed on the
    /// connection that wrote the rows (SC-004 atomicity).
    #[must_use]
    pub fn into_parts(self) -> (Client, Handle) {
        (self.client, self.handle)
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

    fn insert_entity(&mut self, row: &EntityRow) -> CoreResult<()> {
        // Build the SQL with optional embedding inlined as a pgvector literal,
        // byte-identical to SpiPgClient (DC-001): pgvector has no native
        // tokio-postgres binding, so cast from a `'[..]'::vector` string.
        let emb_sql = match &row.name_emb {
            Some(v) => format!("'{}'::vector", vector_literal(v)),
            None => "NULL".to_string(),
        };
        let sql = format!(
            "INSERT INTO pgrg.entities (id, namespace, name, kind, name_emb, description) \
             VALUES ($1, $2, $3, $4, {emb_sql}, $5) \
             ON CONFLICT (namespace, name, kind) DO NOTHING"
        );
        self.handle.block_on(async {
            self.client
                .execute(
                    &sql,
                    &[
                        &row.id,
                        &row.namespace,
                        &row.name,
                        &row.kind,
                        &row.description,
                    ],
                )
                .await
                .map_err(|e| Self::map_err("spi insert entity", e))?;
            Ok(())
        })
    }

    fn insert_relationship(&mut self, row: &RelRow) -> CoreResult<()> {
        let weight = f64::from(row.weight);
        self.handle.block_on(async {
            self.client
                .execute(
                    "INSERT INTO pgrg.relationships \
                       (id, namespace, src_id, dst_id, kind, weight, description) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7) \
                     ON CONFLICT (namespace, src_id, dst_id, kind) DO NOTHING",
                    &[
                        &row.id,
                        &row.namespace,
                        &row.src_id,
                        &row.dst_id,
                        &row.kind,
                        &weight,
                        &row.description,
                    ],
                )
                .await
                .map_err(|e| Self::map_err("spi insert relationship", e))?;
            Ok(())
        })
    }

    fn insert_chunk_entity(&mut self, row: &ChunkEntityRow) -> CoreResult<()> {
        let confidence = f64::from(row.confidence);
        self.handle.block_on(async {
            self.client
                .execute(
                    "INSERT INTO pgrg.chunk_entities (chunk_id, entity_id, confidence) \
                     VALUES ($1, $2, $3) \
                     ON CONFLICT (chunk_id, entity_id) DO NOTHING",
                    &[&row.chunk_id, &row.entity_id, &confidence],
                )
                .await
                .map_err(|e| Self::map_err("spi insert chunk_entity", e))?;
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
        self.handle.block_on(async {
            let rows = self
                .client
                .query(
                    "SELECT id, name, similarity(name, $2) AS sim, name_emb::text \
                     FROM pgrg.entities \
                     WHERE namespace = $1 \
                     ORDER BY sim DESC \
                     LIMIT $3",
                    &[&namespace, &name, &lim],
                )
                .await
                .map_err(|e| Self::map_err("spi fuzzy_match_entity", e))?;
            let mut out: Vec<EntityCandidate> = Vec::with_capacity(rows.len());
            for r in rows {
                let id: Uuid = r
                    .try_get::<_, Uuid>(0)
                    .map_err(|e| Self::map_err("spi fuzzy id", e))?;
                let nm: String = r
                    .try_get::<_, String>(1)
                    .map_err(|e| Self::map_err("spi fuzzy name", e))?;
                let sim: f32 = r
                    .try_get::<_, Option<f32>>(2)
                    .map_err(|e| Self::map_err("spi fuzzy sim", e))?
                    .unwrap_or(0.0);
                let emb_text: Option<String> = r
                    .try_get::<_, Option<String>>(3)
                    .map_err(|e| Self::map_err("spi fuzzy emb", e))?;
                let name_emb = emb_text.and_then(|t| parse_vector_literal(&t));
                out.push(EntityCandidate {
                    id,
                    name: nm,
                    trgm_similarity: sim,
                    name_emb,
                });
            }
            Ok(out)
        })
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
