//! `run_job` — per-document transaction pipeline (PG-agnostic).
//!
//! Spec §3 lines 68–74. Mission brief SC-005 (text source -> doc + chunks),
//! SC-007 (content-hash skip), SC-011 (per-doc transaction atomicity),
//! SC-017 (cargo test-able without PG).
//!
//! Sequence:
//!   1. Read source bytes.
//!   2. Compute `content_hash`.
//!   3. If hash already exists in namespace -> `SkippedDuplicate` (SC-007).
//!   4. Chunk via `chunkshop`.
//!   5. Embed each chunk via the injected `EmbeddingBackend`.
//!   6. Call `LlmProvider::extract` (`MockProvider` yields empty in Plan 3).
//!   7. Persist document + chunks in one logical transaction.
//!      Plan 4 will add entities / relationships / `chunk_entities`.
//!   8. Commit; return `Completed`.
//!
//! Errors at any step trigger rollback. SC-011: atomicity is enforced by
//! the `PgClient` trait — the pgrx adapter (Task 11) returns `Err` to
//! `BackgroundWorker::transaction`, which rolls back the SPI session.

use uuid::Uuid;

use crate::chunking::{ChunkStrategy, Chunker};
use crate::embedding::EmbeddingBackend;
use crate::error::{CoreError, CoreResult};
use crate::ingest::content_hash::content_hash;
use crate::ingest::pg_client::{ChunkRow, DocRow, PgClient};
use crate::ingest::types::{IngestRequest, IngestSource};
use crate::llm::LlmProvider;

/// Outcome of one `run_job` call.
#[derive(Debug, Clone)]
pub enum RunJobOutcome {
    /// Document persisted with N chunks.
    Completed {
        document_id: Uuid,
        chunk_count: usize,
    },
    /// Document with this `content_hash` already existed; nothing written.
    SkippedDuplicate { existing_hash: String },
}

/// Per-document transaction pipeline.
///
/// `client` is the `PgClient` adapter (pgrx Spi or `FakePgClient` in tests).
/// `embedder` and `provider` are loaded by the worker once at startup
/// (SC-009) and reused across jobs.
pub fn run_job(
    client: &mut dyn PgClient,
    req: &IngestRequest,
    embedder: &dyn EmbeddingBackend,
    provider: &dyn LlmProvider,
) -> CoreResult<RunJobOutcome> {
    // 1+2: read source bytes and compute hash.
    let (source_name, bytes) = read_source(&req.source)?;
    let hash = content_hash(&bytes);

    // 3: incremental skip (SC-007).
    if client.document_exists_by_hash(&req.namespace, &hash)? {
        return Ok(RunJobOutcome::SkippedDuplicate {
            existing_hash: hash,
        });
    }

    // 4: chunk via chunkshop.
    let strategy = ChunkStrategy::parse(&req.chunk_strategy).unwrap_or_default();
    let chunker = Chunker::new(strategy);
    let text = std::str::from_utf8(&bytes)
        .map_err(|e| CoreError::InvalidConfig(format!("source not valid utf-8: {e}")))?;
    let chunks = chunker.chunk(text)?;
    if chunks.is_empty() {
        return Err(CoreError::InvalidConfig(
            "chunkshop produced 0 chunks for non-empty source".into(),
        ));
    }

    // 5: embed each chunk.
    let chunk_texts: Vec<&str> = chunks.iter().map(|c| c.text.as_str()).collect();
    let embeddings = embedder.embed_batch(&chunk_texts)?;
    if embeddings.len() != chunks.len() {
        return Err(CoreError::InvalidConfig(format!(
            "embedder returned {} vectors for {} chunks",
            embeddings.len(),
            chunks.len()
        )));
    }

    // 6: extraction (Plan 3: `MockProvider` returns empty).
    for c in &chunks {
        let _ = provider.extract(&c.text, &req.namespace)?;
    }

    // 7: persist in a single logical transaction.
    let doc_id = Uuid::new_v4();
    let doc = DocRow {
        id: doc_id,
        namespace: req.namespace.clone(),
        source: source_name,
        content_hash: hash.clone(),
        title: None,
    };
    let persist_result: CoreResult<usize> = (|| {
        client.insert_document(&doc)?;
        for (chunk, embedding) in chunks.iter().zip(embeddings.iter()) {
            let row = ChunkRow {
                id: Uuid::new_v4(),
                document_id: doc_id,
                namespace: req.namespace.clone(),
                ord: chunk.ord,
                text: chunk.text.clone(),
                token_count: chunk.token_count,
                embedding: embedding.clone(),
            };
            client.insert_chunk(&row)?;
        }
        Ok(chunks.len())
    })();

    match persist_result {
        Ok(n) => {
            client.commit()?;
            Ok(RunJobOutcome::Completed {
                document_id: doc_id,
                chunk_count: n,
            })
        }
        Err(e) => {
            // SC-011: atomicity. Roll back and propagate.
            let _ = client.rollback();
            Err(e)
        }
    }
}

/// Read source bytes from the `IngestSource` variant.
///
/// `Path` reads from the host filesystem (must be readable by the postgres
/// OS user — spec §3 line 69). `Text` and `Bytes` carry payload inline.
fn read_source(source: &IngestSource) -> CoreResult<(String, Vec<u8>)> {
    match source {
        IngestSource::Path(p) => {
            let bytes =
                std::fs::read(p).map_err(|e| CoreError::InvalidConfig(format!("read {p}: {e}")))?;
            Ok((p.clone(), bytes))
        }
        IngestSource::Text { name, content } => Ok((name.clone(), content.as_bytes().to_vec())),
        IngestSource::Bytes { name, bytes } => Ok((name.clone(), bytes.clone())),
    }
}
