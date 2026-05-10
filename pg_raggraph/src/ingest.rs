//! `pgrg.ingest`, `pgrg.ingest_text`, `pgrg.ingest_bytes` — async ingest entry points.
//!
//! Mission brief Constraint Never: never block the SQL caller. These functions
//! are queue inserts; the bg worker (`crate::bgw::worker`) drains the queue.
//! SC-003: <50ms return time. SC-005: ingest_text. SC-006: ingest_bytes.

use pgrx::prelude::*;

/// `pgrg.ingest(path, namespace, chunk_strategy)` — enqueue a path-shaped job.
#[pg_extern]
fn ingest(
    path: &str,
    namespace: default!(&str, "'default'"),
    chunk_strategy: default!(&str, "'auto'"),
) -> pgrx::Uuid {
    enqueue_path(path, namespace, chunk_strategy)
}

/// `pgrg.ingest_text(name, content, namespace, chunk_strategy)` — enqueue inline text.
#[pg_extern]
fn ingest_text(
    name: &str,
    content: &str,
    namespace: default!(&str, "'default'"),
    chunk_strategy: default!(&str, "'auto'"),
) -> pgrx::Uuid {
    enqueue_payload(name, content.as_bytes(), namespace, chunk_strategy)
}

/// `pgrg.ingest_bytes(name, bytes, namespace, chunk_strategy)` — enqueue inline binary.
#[pg_extern]
fn ingest_bytes(
    name: &str,
    bytes: &[u8],
    namespace: default!(&str, "'default'"),
    chunk_strategy: default!(&str, "'auto'"),
) -> pgrx::Uuid {
    enqueue_payload(name, bytes, namespace, chunk_strategy)
}

/// Common enqueue-with-payload path (text/bytes share this).
fn enqueue_payload(name: &str, bytes: &[u8], namespace: &str, chunk_strategy: &str) -> pgrx::Uuid {
    let id = uuid::Uuid::new_v4();
    let pgrx_id = pgrx::Uuid::from_bytes(*id.as_bytes());
    Spi::connect_mut(|client| {
        client
            .update(
                "INSERT INTO pgrg.ingest_jobs \
                     (id, status, source, namespace, chunk_strategy, payload, enqueued_at, updated_at) \
                 VALUES ($1, 'queued', $2, $3, $4, $5, now(), now())",
                None,
                &[
                    pgrx_id.into(),
                    name.into(),
                    namespace.into(),
                    chunk_strategy.into(),
                    bytes.into(),
                ],
            )
            .expect("pgrg.ingest_*: enqueue failed");
    });
    pgrx_id
}

/// Path-shaped enqueue (no payload).
fn enqueue_path(path: &str, namespace: &str, chunk_strategy: &str) -> pgrx::Uuid {
    let id = uuid::Uuid::new_v4();
    let pgrx_id = pgrx::Uuid::from_bytes(*id.as_bytes());
    Spi::connect_mut(|client| {
        client
            .update(
                "INSERT INTO pgrg.ingest_jobs \
                     (id, status, source, namespace, chunk_strategy, enqueued_at, updated_at) \
                 VALUES ($1, 'queued', $2, $3, $4, now(), now())",
                None,
                &[
                    pgrx_id.into(),
                    path.into(),
                    namespace.into(),
                    chunk_strategy.into(),
                ],
            )
            .expect("pgrg.ingest: enqueue failed");
    });
    pgrx_id
}
