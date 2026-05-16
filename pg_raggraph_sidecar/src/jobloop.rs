//! Background job-queue polling loop (Plan 5 Slice 2).
//!
//! DC-001 parity core. This module replicates the pgrx background worker's
//! claim + `IngestRequest` reconstruction + `run_job` dispatch over a
//! tokio-postgres (libpq) connection instead of SPI, so the sidecar produces
//! byte-identical queue behaviour to the in-process worker.
//!
//! Provenance (replicated verbatim, do NOT re-derive):
//!   - claim SQL: `pg_raggraph/src/bgw/queue.rs::claim_next_job` (lines 52-72)
//!   - row → `IngestSource`: `pg_raggraph/src/bgw/worker.rs::build_request`
//!     (lines 167-191): `payload` NULL -> `IngestSource::Path`; `payload`
//!     non-NULL + utf-8 -> `IngestSource::Text`; `payload` non-NULL +
//!     non-utf-8 -> `IngestSource::Bytes`. Defaults: `chunk_strategy` ->
//!     `"auto"` when NULL, `source` -> `"(unnamed)"` when NULL, `namespace`
//!     -> `"default"` when NULL.
//!   - completion SQL: `queue.rs::complete_job` (lines 95-107)
//!   - failure SQL:    `queue.rs::fail_job` (lines 110-122) — the worker sets
//!     `status='failed'` (NOT `'queued'`); the reaper is a separate sweep.
//!     Sidecar parity (DC-006) requires matching the worker exactly.

use std::sync::Arc;
use std::time::Duration;

use pg_raggraph_core::embedding::EmbeddingBackend;
use pg_raggraph_core::ingest::run::run_job;
use pg_raggraph_core::ingest::types::{IngestRequest, IngestSource};
use pg_raggraph_core::llm::MockProvider;
use tokio_postgres::Client;
use uuid::Uuid;

use crate::db;
use crate::pg_client::TokioPgClient;

/// Bounded poll backoff for an empty queue (SC-010). `next_delay()` yields
/// 1s, 5s, then doubles up to a 30s cap; `reset()` (on a successful claim)
/// returns to 1s. Pure — no DB, no time source; the caller sleeps.
#[derive(Debug)]
pub struct Backoff {
    current_secs: u64,
}

impl Backoff {
    /// Create a new backoff starting at 1s.
    #[must_use]
    pub fn new() -> Self {
        Self { current_secs: 1 }
    }

    /// Get the next delay duration and advance the backoff state.
    /// Sequence: 1, 5, 10, 20, 30, 30, 30, …
    pub fn next_delay(&mut self) -> Duration {
        let delay = Duration::from_secs(self.current_secs);
        // Advance: 1 → 5, then double up to 30s cap
        self.current_secs = match self.current_secs {
            1 => 5,
            5 => 10,
            _ => (self.current_secs * 2).min(30),
        };
        delay
    }

    /// Reset backoff to 1s. Called when a job is successfully claimed.
    pub fn reset(&mut self) {
        self.current_secs = 1;
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new()
    }
}

/// One claimed job — the sidecar mirror of pgrx `queue::ClaimedJob`.
#[derive(Debug, Clone)]
pub struct ClaimedJob {
    pub id: Uuid,
    pub source: Option<String>,
    pub namespace: String,
    pub chunk_strategy: Option<String>,
    pub payload: Option<Vec<u8>>,
    pub attempt_count: i32,
}

/// Outcome of one `process_one` poll cycle.
#[derive(Debug)]
pub enum ProcessOutcome {
    /// No queued job was available this cycle.
    Idle,
    /// Job completed and marked `completed`.
    Completed(Uuid),
    /// `run_job` returned an error; job released with the error recorded.
    Failed { id: Uuid, error: String },
}

/// Translate a claimed row's `source` + `payload` into an `IngestSource`.
///
/// Byte-for-byte the same disambiguation as pgrx
/// `worker::build_request` (lines 172-184): non-lossy UTF-8 check, any
/// `Utf8Error` falls through to `Bytes`.
#[must_use]
pub fn row_to_ingest_source(source: &str, payload: Option<Vec<u8>>) -> IngestSource {
    match payload {
        Some(bytes) => match std::str::from_utf8(&bytes) {
            Ok(text) => IngestSource::Text {
                name: source.to_string(),
                content: text.to_string(),
            },
            Err(_) => IngestSource::Bytes {
                name: source.to_string(),
                bytes,
            },
        },
        None => IngestSource::Path(source.to_string()),
    }
}

/// Build an `IngestRequest` from a claimed job, applying the worker's
/// defaults (`build_request`, lines 167-191): `chunk_strategy` → `"auto"`,
/// `source` → `"(unnamed)"` when NULL. `namespace` is already defaulted to
/// `"default"` at claim time (mirrors queue.rs:86).
fn build_request(job: &ClaimedJob) -> IngestRequest {
    let chunk_strategy = job.chunk_strategy.clone().unwrap_or_else(|| "auto".into());
    let namespace = job.namespace.clone();
    let source_name = job.source.clone().unwrap_or_else(|| "(unnamed)".into());
    let source = row_to_ingest_source(&source_name, job.payload.clone());
    IngestRequest {
        source,
        namespace,
        chunk_strategy,
    }
}

/// Claim the next queued job over libpq.
///
/// Uses the **verbatim** claim SQL from pgrx `queue.rs::claim_next_job`
/// (lines 54-68), including `ORDER BY enqueued_at ASC` then `LIMIT 1` then
/// `FOR UPDATE SKIP LOCKED`. `namespace` is defaulted to `"default"` here, the
/// same way queue.rs:86 does.
///
/// # Errors
/// Returns the tokio-postgres query error.
pub async fn claim_next_job(client: &Client) -> anyhow::Result<Option<ClaimedJob>> {
    let row = client
        .query_opt(
            "WITH next_job AS ( \
                 SELECT id FROM pgrg.ingest_jobs \
                 WHERE status = 'queued' \
                 ORDER BY enqueued_at ASC \
                 LIMIT 1 \
                 FOR UPDATE SKIP LOCKED \
             ) \
             UPDATE pgrg.ingest_jobs ij \
             SET status = 'running', \
                 started_at = COALESCE(ij.started_at, now()), \
                 updated_at = now(), \
                 attempt_count = ij.attempt_count + 1 \
             FROM next_job \
             WHERE ij.id = next_job.id \
             RETURNING ij.id, ij.source, ij.namespace, ij.chunk_strategy, ij.payload, ij.attempt_count",
            &[],
        )
        .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let id: Uuid = row.get(0);
    let source: Option<String> = row.get(1);
    let namespace: Option<String> = row.get(2);
    let chunk_strategy: Option<String> = row.get(3);
    let payload: Option<Vec<u8>> = row.get(4);
    let attempt_count: i32 = row.get(5);

    Ok(Some(ClaimedJob {
        id,
        source,
        namespace: namespace.unwrap_or_else(|| "default".into()),
        chunk_strategy,
        payload,
        attempt_count,
    }))
}

/// Poll once: claim a job, run the ingest pipeline inside a single-connection
/// transaction, and finalize the queue row.
///
/// The job transaction runs on the **same** control connection as the claim —
/// one connection, one transaction (DC-006). `run_job` (and every
/// `TokioPgClient` method) calls `Handle::block_on` internally, so the
/// dispatch MUST run inside `spawn_blocking`; calling `block_on` on a runtime
/// worker thread would panic.
///
/// # Errors
/// Returns an error if the control connection, claim query, or queue
/// finalisation fails. `run_job` failures are captured as
/// `ProcessOutcome::Failed` (not an `Err`).
pub async fn process_one(
    database_url: &str,
    embedder: &Arc<dyn EmbeddingBackend>,
    handle: tokio::runtime::Handle,
) -> anyhow::Result<ProcessOutcome> {
    // 1: control connection (tokio-postgres `Client` methods take `&self`).
    let ctrl = db::connect(database_url).await?;

    // 2: claim a job (NULL → idle).
    let Some(job) = claim_next_job(&ctrl).await? else {
        return Ok(ProcessOutcome::Idle);
    };
    let job_id = job.id;

    // 3: reconstruct the request (worker parity).
    let req = build_request(&job);

    // 4: open the job transaction on the SAME connection, then hand the
    //    connection to TokioPgClient for the duration of run_job.
    ctrl.batch_execute("BEGIN").await?;
    let mut tpc = TokioPgClient::new(ctrl, handle.clone());
    let embedder = Arc::clone(embedder);

    // 5: dispatch run_job on a blocking thread (TokioPgClient methods +
    //    run_job do Handle::block_on internally — never on a worker thread).
    //    The closure owns `tpc` and returns it so the async context can
    //    recover the connection via `into_parts`.
    let (result, tpc) = tokio::task::spawn_blocking(move || {
        // Task 14 swaps in provider_factory::resolve_or_default_provider(&req.namespace)
        let provider = MockProvider::new();
        let r = run_job(&mut tpc, &req, embedder.as_ref(), &provider);
        (r.map(|_| ()).map_err(|e| format!("{e:?}")), tpc)
    })
    .await?;

    // 6: recover the connection and finalize the queue row.
    let (ctrl, _h) = tpc.into_parts();
    match result {
        Ok(()) => {
            ctrl.batch_execute("COMMIT").await?;
            // queue.rs::complete_job (lines 99-101), byte-identical.
            ctrl.execute(
                "UPDATE pgrg.ingest_jobs \
                 SET status = 'completed', finished_at = now(), updated_at = now(), error = NULL \
                 WHERE id = $1",
                &[&job_id],
            )
            .await?;
            Ok(ProcessOutcome::Completed(job_id))
        }
        Err(error) => {
            ctrl.batch_execute("ROLLBACK").await.ok();
            // queue.rs::fail_job (lines 114-116), byte-identical.
            ctrl.execute(
                "UPDATE pgrg.ingest_jobs \
                 SET status = 'failed', finished_at = now(), updated_at = now(), error = $2 \
                 WHERE id = $1",
                &[&job_id, &error],
            )
            .await
            .ok();
            Ok(ProcessOutcome::Failed { id: job_id, error })
        }
    }
}
