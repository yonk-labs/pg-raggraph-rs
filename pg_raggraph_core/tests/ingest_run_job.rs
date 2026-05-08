//! Task 10 — `_core::ingest::run_job` per-document transaction tests.
//!
//! Mission brief SC-005 (text source -> doc + chunks), SC-007 (content-hash
//! skip), SC-011 (per-doc transaction atomicity), SC-017 (cargo test-able
//! without PG). Drives the `PgClient` injection trait via `FakePgClient`
//! and the no-op `MockProvider`.

use pg_raggraph_core::chunking::ChunkStrategy;
use pg_raggraph_core::embedding::DeterministicEmbedder;
use pg_raggraph_core::ingest::pg_client::FakePgClient;
use pg_raggraph_core::ingest::run::{RunJobOutcome, run_job};
use pg_raggraph_core::ingest::{IngestRequest, IngestSource};
use pg_raggraph_core::llm::MockProvider;

#[test]
fn run_job_writes_document_and_chunks_for_text_source() {
    let mut client = FakePgClient::new();
    let req = IngestRequest {
        source: IngestSource::Text {
            name: "doc1".into(),
            content: "hello world".into(),
        },
        namespace: "default".into(),
        chunk_strategy: ChunkStrategy::Auto.as_str().into(),
    };
    let embedder = DeterministicEmbedder::new(384);
    let provider = MockProvider::new();
    let outcome = run_job(&mut client, &req, &embedder, &provider).expect("run_job ok");
    assert!(matches!(outcome, RunJobOutcome::Completed { .. }));
    assert_eq!(client.documents.len(), 1);
    assert!(!client.chunks.is_empty());
}

#[test]
fn run_job_skips_when_content_hash_already_exists() {
    let mut client = FakePgClient::new();
    let req = IngestRequest {
        source: IngestSource::Text {
            name: "doc1".into(),
            content: "hello world".into(),
        },
        namespace: "default".into(),
        chunk_strategy: ChunkStrategy::Auto.as_str().into(),
    };
    let embedder = DeterministicEmbedder::new(384);
    let provider = MockProvider::new();
    let _ = run_job(&mut client, &req, &embedder, &provider).expect("first ok");
    let outcome = run_job(&mut client, &req, &embedder, &provider).expect("second ok");
    assert!(matches!(outcome, RunJobOutcome::SkippedDuplicate { .. }));
    assert_eq!(client.documents.len(), 1, "no second doc row");
}

#[test]
fn run_job_rolls_back_on_chunk_write_failure() {
    // Fail on the first chunk insert (index 0): deterministic regardless of
    // how many chunks the configured chunker produces for the given input.
    let mut client = FakePgClient::new().with_chunk_write_failure_at(0);
    let req = IngestRequest {
        source: IngestSource::Text {
            name: "doc_fail".into(),
            content: "the quick brown fox jumps over the lazy dog. it was a dark and stormy night."
                .into(),
        },
        namespace: "default".into(),
        chunk_strategy: ChunkStrategy::Auto.as_str().into(),
    };
    let embedder = DeterministicEmbedder::new(384);
    let provider = MockProvider::new();
    let outcome = run_job(&mut client, &req, &embedder, &provider);
    assert!(outcome.is_err(), "must surface chunk-write failure");
    assert!(client.documents.is_empty(), "rollback: no document row");
    assert!(client.chunks.is_empty(), "rollback: no chunk rows");
}

#[test]
fn run_job_uses_mock_provider_no_network() {
    let mut client = FakePgClient::new();
    let req = IngestRequest {
        source: IngestSource::Text {
            name: "doc1".into(),
            content: "hello".into(),
        },
        namespace: "default".into(),
        chunk_strategy: ChunkStrategy::Auto.as_str().into(),
    };
    let embedder = DeterministicEmbedder::new(384);
    let provider = MockProvider::new();
    run_job(&mut client, &req, &embedder, &provider).expect("mock-driven ok");
    assert!(
        client.entities.is_empty(),
        "MockProvider must yield no entities"
    );
    assert!(
        client.relationships.is_empty(),
        "MockProvider must yield no relationships"
    );
}
