//! `PgClient` injection trait — lets `_core::ingest::run_job` run unit-tested
//! without a real `PostgreSQL`.
//!
//! The pgrx-side adapter (Task 11) wraps `pgrx::Spi`. The `FakePgClient`
//! impl below is for `cargo test` and lives next to the trait so unit tests
//! never need to depend on the pgrx crate.
//!
//! Mission brief SC-011 (per-doc transaction atomicity) and SC-017 (cargo
//! test-able without PG). Plan 4 T20 extended the surface with entity /
//! relationship / `chunk_entity` persistence + a fuzzy-match query so the
//! resolver (T21) and bg worker (T22/T23) can persist real extraction
//! output (SC-013, SC-014).

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

/// One entity row to insert. The pgrx adapter materializes this into a
/// `pgrg.entities` row. Resolution (merge vs insert) happens at the caller
/// via `_core::ingest::resolve::resolve_or_insert_entity` (T21).
#[derive(Debug, Clone)]
pub struct EntityRow {
    pub id: Uuid,
    pub namespace: String,
    pub name: String,
    pub kind: Option<String>,
    pub name_emb: Option<Vec<f32>>,
    pub description: Option<String>,
}

/// One relationship row to insert. The pgrx adapter materializes into
/// `pgrg.relationships`. `src_id` and `dst_id` are already-resolved entity
/// UUIDs (T21's resolver mints them).
#[derive(Debug, Clone)]
pub struct RelRow {
    pub id: Uuid,
    pub namespace: String,
    pub src_id: Uuid,
    pub dst_id: Uuid,
    pub kind: String,
    pub weight: f32,
    pub description: Option<String>,
}

/// One `chunk_entity` link to insert. Tracks which chunk mentioned which
/// resolved entity, with confidence carried from the LLM extraction.
#[derive(Debug, Clone)]
pub struct ChunkEntityRow {
    pub chunk_id: Uuid,
    pub entity_id: Uuid,
    pub confidence: f32,
}

/// Fuzzy-match candidate returned from `pg_trgm` lookup. The caller refines
/// with cosine on `name_emb` (T21's resolver applies the threshold).
#[derive(Debug, Clone)]
pub struct EntityCandidate {
    pub id: Uuid,
    pub name: String,
    pub trgm_similarity: f32,
    pub name_emb: Option<Vec<f32>>,
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

    /// Insert an entity row. Caller is responsible for resolution decisions
    /// (i.e., calling `fuzzy_match_entity` first and only inserting when the
    /// resolver picks "new" rather than "merge").
    fn insert_entity(&mut self, row: &EntityRow) -> CoreResult<()>;

    /// Insert a relationship row. Both `src_id` and `dst_id` must already
    /// exist in `pgrg.entities`.
    fn insert_relationship(&mut self, row: &RelRow) -> CoreResult<()>;

    /// Insert a `chunk_entity` link.
    fn insert_chunk_entity(&mut self, row: &ChunkEntityRow) -> CoreResult<()>;

    /// Fuzzy-match an entity name within `namespace`. Returns up to `limit`
    /// candidates ordered by trigram similarity desc. The caller refines
    /// with cosine on `name_emb` (SC-014's two-step resolution).
    fn fuzzy_match_entity(
        &mut self,
        namespace: &str,
        name: &str,
        limit: usize,
    ) -> CoreResult<Vec<EntityCandidate>>;

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

/// Trigram-set Jaccard similarity. Simple in-memory analog of `pg_trgm`.
/// The pgrx adapter (T23/`SpiPgClient`) delegates to real `pg_trgm`.
pub(crate) fn trgm_sim(a: &str, b: &str) -> f32 {
    fn trigrams(s: &str) -> std::collections::HashSet<String> {
        let s = format!("  {}  ", s.to_lowercase());
        let chars: Vec<char> = s.chars().collect();
        let mut set = std::collections::HashSet::new();
        for w in chars.windows(3) {
            set.insert(w.iter().collect::<String>());
        }
        set
    }
    let ta = trigrams(a);
    let tb = trigrams(b);
    #[allow(clippy::cast_precision_loss)]
    let inter = ta.intersection(&tb).count() as f32;
    #[allow(clippy::cast_precision_loss)]
    let union = ta.union(&tb).count() as f32;
    if union == 0.0 { 0.0 } else { inter / union }
}

/// Test-only in-memory `PgClient`. Buffers writes; rolls back by clearing.
///
/// Writes go to `buffered_*` first; `commit` moves them into the canonical
/// vecs (inspected by tests); `rollback` clears the buffers without
/// touching canonical state.
#[derive(Debug, Default)]
pub struct FakePgClient {
    pub documents: Vec<DocRow>,
    pub chunks: Vec<ChunkRow>,
    pub entities: Vec<EntityRow>,
    pub relationships: Vec<RelRow>,
    pub chunk_entities: Vec<ChunkEntityRow>,
    /// If `Some(n)`, the n-th chunk insert (0-indexed) returns `Err`.
    chunk_fail_at: Option<usize>,
    chunk_inserts: usize,
    /// Buffers that become canonical state on commit.
    buffered_documents: Vec<DocRow>,
    buffered_chunks: Vec<ChunkRow>,
    pub buffered_entities: Vec<EntityRow>,
    buffered_relationships: Vec<RelRow>,
    buffered_chunk_entities: Vec<ChunkEntityRow>,
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

    fn insert_entity(&mut self, row: &EntityRow) -> CoreResult<()> {
        self.buffered_entities.push(row.clone());
        Ok(())
    }

    fn insert_relationship(&mut self, row: &RelRow) -> CoreResult<()> {
        self.buffered_relationships.push(row.clone());
        Ok(())
    }

    fn insert_chunk_entity(&mut self, row: &ChunkEntityRow) -> CoreResult<()> {
        self.buffered_chunk_entities.push(row.clone());
        Ok(())
    }

    fn fuzzy_match_entity(
        &mut self,
        namespace: &str,
        name: &str,
        limit: usize,
    ) -> CoreResult<Vec<EntityCandidate>> {
        let mut hits: Vec<EntityCandidate> = self
            .entities
            .iter()
            .filter(|e| e.namespace == namespace)
            .map(|e| EntityCandidate {
                id: e.id,
                name: e.name.clone(),
                trgm_similarity: trgm_sim(&e.name, name),
                name_emb: e.name_emb.clone(),
            })
            .collect();
        hits.sort_by(|a, b| {
            b.trgm_similarity
                .partial_cmp(&a.trgm_similarity)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        hits.truncate(limit);
        Ok(hits)
    }

    fn rollback(&mut self) -> CoreResult<()> {
        self.buffered_documents.clear();
        self.buffered_chunks.clear();
        self.buffered_entities.clear();
        self.buffered_relationships.clear();
        self.buffered_chunk_entities.clear();
        self.chunk_inserts = 0;
        Ok(())
    }

    fn commit(&mut self) -> CoreResult<()> {
        self.documents.append(&mut self.buffered_documents);
        self.chunks.append(&mut self.buffered_chunks);
        self.entities.append(&mut self.buffered_entities);
        self.relationships.append(&mut self.buffered_relationships);
        self.chunk_entities
            .append(&mut self.buffered_chunk_entities);
        self.chunk_inserts = 0;
        Ok(())
    }
}
