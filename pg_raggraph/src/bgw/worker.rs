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

    // Main loop — 1-second poll cycle. Currently a no-op shell.
    // Task 9 adds claim_next_job; Task 10 adds run_job dispatch.
    while BackgroundWorker::wait_latch(Some(Duration::from_secs(1))) {
        // Drain the SIGHUP flag — PG reloads GUCs automatically; no per-worker action needed yet.
        let _ = BackgroundWorker::sighup_received();
        // Job claim + dispatch lands in Tasks 9–11.
    }

    pgrx::log!("{}: shutting down", worker_name);
}
