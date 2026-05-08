//! Launcher background worker for pg_raggraph.
//!
//! Plan 3 Task 8 ships the launcher as a registration + idle shell.
//! Task 16 will add the reaper sweep that re-queues stuck `running` jobs.
//! Runs on a 30-second latch cycle.

use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::time::Duration;

/// Register the launcher BGW (called from `_PG_init`).
pub fn register_launcher() {
    BackgroundWorkerBuilder::new("pg_raggraph launcher")
        .set_function("pg_raggraph_launcher_main")
        .set_library("pg_raggraph")
        .enable_spi_access()
        .set_restart_time(Some(Duration::from_secs(5)))
        .load();
}

/// Launcher main function — must match the name passed to `set_function`.
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn pg_raggraph_launcher_main(_arg: pgrx::pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    pgrx::log!("pg_raggraph launcher started");

    // Main loop — 30-second latch cycle. Currently a no-op shell.
    // Task 16 will add the stuck-job reaper here.
    while BackgroundWorker::wait_latch(Some(Duration::from_secs(30))) {
        if BackgroundWorker::sighup_received() {
            // GUCs reloaded automatically by PG on SIGHUP.
        }
        // Reaper sweep lands in Task 16.
    }

    pgrx::log!("pg_raggraph launcher shutting down");
}
