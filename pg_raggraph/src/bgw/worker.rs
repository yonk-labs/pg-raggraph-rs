//! Worker pool background workers for `pg_raggraph`.
//!
//! Plan 3 Task 8 ships the worker pool as registration + idle shell.
//! Task 9 added `claim_next_job` / `complete_job` / `fail_job`.
//! Task 11 wires the worker main loop to `_core::ingest::run_job` via the
//! `SpiPgClient` adapter and a once-per-worker embedder cache.
//!
//! Each worker polls the queue every 1s. Workers auto-restart on crash via
//! `set_restart_time`.
//!
//! Mission brief Constraint Always: bg-worker code that touches PG goes
//! through pgrx SPI helpers, never raw libpq.

use pg_raggraph_core::embedding::EmbeddingBackend;
use pg_raggraph_core::ingest::types::{IngestRequest, IngestSource};
use pg_raggraph_core::ingest::{RunJobOutcome, run_job};
use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use crate::bgw::{embedder_cache, queue, spi_client};
use crate::gucs;
use crate::provider_factory;

/// Register `bgw_workers` static BGWs (called from `_PG_init`).
pub fn register_workers() {
    let n = gucs::BGW_WORKERS.get();
    for i in 0..n {
        BackgroundWorkerBuilder::new(&format!("pg_raggraph w{i}"))
            .set_function("pg_raggraph_worker_main")
            .set_library("pg_raggraph")
            .enable_spi_access()
            .set_restart_time(Some(Duration::from_secs(1)))
            .set_argument(i.into_datum())
            .load();
    }
}

/// Worker main function — must match the name passed to `set_function`.
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn pg_raggraph_worker_main(arg: pgrx::pg_sys::Datum) {
    // Extract worker index from argument.
    let worker_idx: i32 =
        unsafe { i32::from_polymorphic_datum(arg, false, pgrx::pg_sys::INT4OID) }.unwrap_or(0);
    let worker_name = format!("pg_raggraph w{worker_idx}");

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    pgrx::log!("{worker_name}: started");

    // SC-009: load the embedder ONCE per worker process, before entering the
    // poll loop. The `Arc<dyn EmbeddingBackend>` is shared by reference
    // across every job iteration.
    let embedder: Arc<dyn EmbeddingBackend> = embedder_cache::build_backend();
    pgrx::log!("{worker_name}: embedder loaded (dim={})", embedder.dim());

    // T23 (Plan 4): the provider is resolved PER JOB (inside the same
    // BackgroundWorker::transaction as `run_job`) — different jobs may run
    // in different namespaces, each with its own `pgrg.namespaces.llm_provider`.
    // Falls back to a no-op MockProvider if no provider is configured, which
    // preserves Plan 3 bg-worker test compatibility (queue/launcher tests).

    let poll = Duration::from_secs(1);
    while BackgroundWorker::wait_latch(Some(poll)) {
        // Drain SIGHUP — PG reloads GUCs automatically; nothing else to do
        // until Task 15 reads `extract_concurrency` per-cycle.
        let _ = BackgroundWorker::sighup_received();

        let claimed = BackgroundWorker::transaction(queue::claim_next_job);
        let Some(job) = claimed else {
            continue;
        };

        let req = match build_request(&job) {
            Ok(r) => r,
            Err(e) => {
                pgrx::warning!("{worker_name}: malformed job {}: {e}", job.id);
                let job_id = job.id;
                let err_msg = e.clone();
                BackgroundWorker::transaction(move || queue::fail_job(&job_id, &err_msg));
                continue;
            }
        };

        // SC-011 + SC-013: resolve the namespace's LLM provider, then run
        // ingest. Both happen inside one `BackgroundWorker::transaction` so
        // an Err return rolls the whole document back atomically.
        //
        // `dyn EmbeddingBackend` is not `RefUnwindSafe` and `CoreError` (which
        // wraps `serde_json::Error`) is not `UnwindSafe`, so we use
        // `AssertUnwindSafe` to opt in. Both types are still safe to use across
        // a panic boundary in practice — the embedder is stateless after
        // construction and CoreError is plain data.
        let job_namespace = req.namespace.clone();
        let outcome: JobOutcome = BackgroundWorker::transaction(AssertUnwindSafe(|| {
            let provider = provider_factory::resolve_or_default_provider(&job_namespace);
            let mut client = spi_client::SpiPgClient;
            match run_job(&mut client, &req, &*embedder, &*provider) {
                Ok(RunJobOutcome::Completed {
                    document_id,
                    chunk_count,
                }) => JobOutcome::Completed {
                    document_id: document_id.to_string(),
                    chunk_count,
                },
                Ok(RunJobOutcome::SkippedDuplicate { existing_hash }) => {
                    JobOutcome::Skipped { existing_hash }
                }
                Err(e) => JobOutcome::Failed {
                    message: format!("{e:?}"),
                },
            }
        }));

        let job_id = job.id;
        match outcome {
            JobOutcome::Completed {
                document_id,
                chunk_count,
            } => {
                pgrx::log!(
                    "{worker_name}: job {job_id} completed (doc={document_id}, chunks={chunk_count})"
                );
                BackgroundWorker::transaction(move || queue::complete_job(&job_id));
            }
            JobOutcome::Skipped { existing_hash } => {
                pgrx::log!("{worker_name}: job {job_id} skipped (duplicate hash {existing_hash})");
                BackgroundWorker::transaction(move || queue::complete_job(&job_id));
            }
            JobOutcome::Failed { message } => {
                pgrx::warning!("{worker_name}: job {job_id} failed: {message}");
                BackgroundWorker::transaction(move || queue::fail_job(&job_id, &message));
            }
        }
    }

    pgrx::log!("{worker_name}: shutting down");
}

/// Unwind-safe summary of one `run_job` call. We project `RunJobOutcome` /
/// `CoreError` (neither of which is `UnwindSafe`) into plain `String` data
/// so the post-transaction logging and queue update can use ordinary moves
/// without `AssertUnwindSafe` gymnastics.
enum JobOutcome {
    Completed {
        document_id: String,
        chunk_count: usize,
    },
    Skipped {
        existing_hash: String,
    },
    Failed {
        message: String,
    },
}

/// Translate a `ClaimedJob` into a PG-agnostic `IngestRequest`.
///
/// Source disambiguation:
///   - `payload` non-NULL + utf-8     -> `IngestSource::Text`
///   - `payload` non-NULL + non-utf-8 -> `IngestSource::Bytes`
///   - `payload` NULL                 -> `IngestSource::Path` (filesystem)
fn build_request(job: &queue::ClaimedJob) -> Result<IngestRequest, String> {
    let chunk_strategy = job.chunk_strategy.clone().unwrap_or_else(|| "auto".into());
    let namespace = job.namespace.clone();
    let source_name = job.source.clone().unwrap_or_else(|| "(unnamed)".into());

    let source = match &job.payload {
        Some(bytes) => match std::str::from_utf8(bytes) {
            Ok(text) => IngestSource::Text {
                name: source_name,
                content: text.to_string(),
            },
            Err(_) => IngestSource::Bytes {
                name: source_name,
                bytes: bytes.clone(),
            },
        },
        None => IngestSource::Path(source_name),
    };

    Ok(IngestRequest {
        source,
        namespace,
        chunk_strategy,
    })
}
