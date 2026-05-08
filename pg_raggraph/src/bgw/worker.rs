//! Worker pool background workers for pg_raggraph.
//!
//! Plan 3 Task 8 ships the worker pool as registration + idle shell.
//! Task 9 adds claim_next_job; Task 10 adds run_job dispatch.
//! Each worker polls the queue every 1s. Workers auto-restart on crash.

use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::time::Duration;

use crate::gucs;

/// Register `bgw_workers` static BGWs (called from `_PG_init`).
pub fn register_workers() {
    let n = gucs::BGW_WORKERS.get();
    for i in 0..n {
        BackgroundWorkerBuilder::new(&format!("pg_raggraph w{}", i))
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

    pgrx::log!("{}: started", worker_name);

    // Main loop — 1-second poll cycle.
    // Task 9 wires claim_next_job + complete_job; Task 10 will replace the
    // immediate-complete short-circuit with a real run_job dispatch.
    while BackgroundWorker::wait_latch(Some(Duration::from_secs(1))) {
        // Drain the SIGHUP flag — PG reloads GUCs automatically; no per-worker action needed yet.
        let _ = BackgroundWorker::sighup_received();

        let claimed = BackgroundWorker::transaction(crate::bgw::queue::claim_next_job);
        if let Some(job) = claimed {
            // Task 10 fills in: dispatch to _core::ingest::run_job.
            // Plan 9 marks completed immediately so the queue drains and SC-016 holds.
            let id = job.id;
            BackgroundWorker::transaction(|| crate::bgw::queue::complete_job(&id));
        }
    }

    pgrx::log!("{}: shutting down", worker_name);
}
