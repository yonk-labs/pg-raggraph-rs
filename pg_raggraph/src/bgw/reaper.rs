//! Reaper sweep — re-queue stuck `running` jobs, fail at attempt cap (SC-012).

use pgrx::prelude::*;

use crate::gucs;

/// One reaper pass. Called from the launcher loop and (for tests) exposed as
/// `pgrg._reaper_sweep()` SQL function.
pub(crate) fn run_reaper_sweep() {
    let interval = gucs::JOB_REAPER_INTERVAL_SECS.get();
    Spi::connect_mut(|client| {
        client
            .update(
                "UPDATE pgrg.ingest_jobs \
                 SET status = 'queued', updated_at = now() \
                 WHERE status = 'running' \
                   AND updated_at < now() - make_interval(secs := $1::float8) \
                   AND attempt_count < 3",
                None,
                &[interval.into()],
            )
            .expect("reaper requeue update failed");
        client
            .update(
                "UPDATE pgrg.ingest_jobs \
                 SET status = 'failed', \
                     error = COALESCE(error, '') || ' (reaper: max attempts reached)', \
                     finished_at = now(), \
                     updated_at = now() \
                 WHERE status = 'running' \
                   AND updated_at < now() - make_interval(secs := $1::float8) \
                   AND attempt_count >= 3",
                None,
                &[interval.into()],
            )
            .expect("reaper fail-cap update failed");
    });
}

/// SQL surface for tests and manual triggering. Internal helper — not part of
/// the public extension API. Underscore prefix per project convention.
#[pg_extern]
fn _reaper_sweep() {
    run_reaper_sweep();
}
