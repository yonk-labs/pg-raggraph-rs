//! SPI queue operations for `pgrg.ingest_jobs`.
//!
//! All functions are internal (`pub(crate)`) and operate via SPI.
//! Mission brief Desired Outcome: `FOR UPDATE SKIP LOCKED LIMIT 1` claim,
//! status transition to `running`, error text on failure.

use pgrx::prelude::*;

/// One claimed job — what the worker dispatches into `_core::ingest::run_job`.
#[derive(Debug)]
pub(crate) struct ClaimedJob {
    pub id: pgrx::Uuid,
    #[allow(dead_code)] // Consumed by Task 10's run_job pipeline.
    pub source: Option<String>,
    #[allow(dead_code)] // Consumed by Task 10's run_job pipeline.
    pub namespace: String,
    #[allow(dead_code)] // Consumed by Task 10's run_job pipeline.
    pub chunk_strategy: Option<String>,
    #[allow(dead_code)] // Consumed by Task 10's run_job pipeline.
    pub payload: Option<Vec<u8>>,
    #[allow(dead_code)] // Consumed by Task 10's run_job pipeline.
    pub attempt_count: i32,
}

/// Claim the next queued job using `FOR UPDATE SKIP LOCKED`.
/// Returns `None` if no jobs are available.
///
/// `_PG_init` registers workers globally (one set per cluster) but each worker
/// connects to a single database via `connect_worker_to_spi`. If the worker is
/// attached to a database that does not have `pg_raggraph` installed, the
/// `pgrg.ingest_jobs` table is absent — we return `None` instead of letting the
/// SPI error propagate and kill the worker.
pub(crate) fn claim_next_job() -> Option<ClaimedJob> {
    Spi::connect_mut(|client| {
        // Bail out cleanly if pgrg.ingest_jobs is not present in this DB.
        let installed: Option<bool> = client
            .select(
                "SELECT to_regclass('pgrg.ingest_jobs') IS NOT NULL",
                Some(1),
                &[],
            )
            .ok()
            .and_then(|t| t.first().get::<bool>(1).ok().flatten());
        if installed != Some(true) {
            // installed == Some(false): extension not installed in this DB — graceful idle, no log spam.
            // installed == None: SPI error path — log once so we don't silently starve workers.
            if installed.is_none() {
                pgrx::log!(
                    "pg_raggraph: claim_next_job: to_regclass check failed unexpectedly; idling this cycle"
                );
            }
            return None;
        }

        let table = client
            .update(
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
                Some(1),
                &[],
            )
            .ok()?;
        if table.is_empty() {
            return None;
        }
        let row = table.first();
        let id: pgrx::Uuid = row.get(1).ok().flatten()?;
        let source: Option<String> = row.get(2).ok().flatten();
        let namespace: Option<String> = row.get(3).ok().flatten();
        let chunk_strategy: Option<String> = row.get(4).ok().flatten();
        let payload: Option<Vec<u8>> = row.get(5).ok().flatten();
        let attempt_count: i32 = row.get(6).ok().flatten().unwrap_or(0);
        Some(ClaimedJob {
            id,
            source,
            namespace: namespace.unwrap_or_else(|| "default".into()),
            chunk_strategy,
            payload,
            attempt_count,
        })
    })
}

/// Mark a job completed.
pub(crate) fn complete_job(job_id: &pgrx::Uuid) {
    Spi::connect_mut(|client| {
        client
            .update(
                "UPDATE pgrg.ingest_jobs \
                 SET status = 'completed', finished_at = now(), updated_at = now(), error = NULL \
                 WHERE id = $1",
                None,
                &[(*job_id).into()],
            )
            .expect("complete_job: update failed");
    });
}

/// Mark a job failed with an error message.
/// Used by Task 10's run_job error path; kept here so the queue module exposes
/// the full lifecycle in one place.
#[allow(dead_code)]
pub(crate) fn fail_job(job_id: &pgrx::Uuid, error: &str) {
    Spi::connect_mut(|client| {
        client
            .update(
                "UPDATE pgrg.ingest_jobs \
                 SET status = 'failed', finished_at = now(), updated_at = now(), error = $2 \
                 WHERE id = $1",
                None,
                &[(*job_id).into(), error.into()],
            )
            .expect("fail_job: update failed");
    });
}
