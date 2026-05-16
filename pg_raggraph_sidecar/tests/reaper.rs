//! SC-009: reaper recovers crashed-sidecar jobs (stuck `status='running'` with
//! stale `updated_at`). Requeue under the attempt cap; terminalize at/over it.
//! MUST NOT touch `completed` or fresh (non-stale) `running` rows.
//! Gated by `PGRG_TEST_DATABASE_URL` (docker fixture, port 5443).

use pg_raggraph_sidecar::jobloop::{ReapOutcome, reap_stale_jobs};
use pg_raggraph_sidecar::{bootstrap, db};
use uuid::Uuid;

fn test_db_url() -> Option<String> {
    std::env::var("PGRG_TEST_DATABASE_URL").ok()
}

#[tokio::test]
async fn reaper_recovers_crashed_sidecar_jobs_only() {
    let Some(url) = test_db_url() else {
        eprintln!("SKIP: PGRG_TEST_DATABASE_URL unset");
        return;
    };
    let mut c = db::connect(&url).await.expect("connect");

    // Clean slate, then bootstrap the full pgrg.* schema.
    c.batch_execute("DROP SCHEMA IF EXISTS pgrg CASCADE;")
        .await
        .expect("reset");
    bootstrap::run_migrations(&mut c)
        .await
        .expect("bootstrap schema");

    // Distinct ids so we can re-fetch each row's post-state.
    let stale_under_cap = Uuid::new_v4(); // running, stale, attempt_count=1 (< 3)  -> queued
    let stale_at_cap = Uuid::new_v4(); // running, stale, attempt_count=3 (>= 3) -> failed
    let completed = Uuid::new_v4(); // completed, stale updated_at        -> UNCHANGED
    let fresh_running = Uuid::new_v4(); // running, updated_at = now()      -> UNCHANGED

    // Seed rows directly. Every NOT-NULL column the schema requires is set:
    // status, source, namespace, chunk_strategy, attempt_count,
    // enqueued_at, updated_at (id explicit; started/finished/error nullable).
    c.execute(
        "INSERT INTO pgrg.ingest_jobs \
             (id, status, source, namespace, chunk_strategy, attempt_count, enqueued_at, updated_at) \
         VALUES \
             ($1, 'running',   'doc-a', 'default', 'auto', 1, now() - interval '2 hours', now() - interval '1 hour'), \
             ($2, 'running',   'doc-b', 'default', 'auto', 3, now() - interval '2 hours', now() - interval '1 hour'), \
             ($3, 'completed', 'doc-c', 'default', 'auto', 1, now() - interval '2 hours', now() - interval '1 hour'), \
             ($4, 'running',   'doc-d', 'default', 'auto', 0, now(),                       now())",
        &[&stale_under_cap, &stale_at_cap, &completed, &fresh_running],
    )
    .await
    .expect("seed ingest_jobs");

    // Interval 300s: the 1-hour-stale rows exceed it; the fresh row does not.
    let outcome = reap_stale_jobs(&c, 300).await.expect("reap");
    assert_eq!(
        outcome,
        ReapOutcome {
            requeued: 1,
            failed: 1
        },
        "exactly one requeued + one failed"
    );

    let status = |id: Uuid| {
        let c = &c;
        async move {
            c.query_one("SELECT status FROM pgrg.ingest_jobs WHERE id = $1", &[&id])
                .await
                .expect("fetch status")
                .get::<_, String>(0)
        }
    };

    assert_eq!(
        status(stale_under_cap).await,
        "queued",
        "stale running, attempt_count < 3 -> requeued"
    );
    assert_eq!(
        status(stale_at_cap).await,
        "failed",
        "stale running, attempt_count >= 3 -> failed (terminal)"
    );
    assert_eq!(
        status(completed).await,
        "completed",
        "completed row must be untouched by the reaper"
    );
    assert_eq!(
        status(fresh_running).await,
        "running",
        "fresh (non-stale) running row must be untouched"
    );

    // The fail path appends Plan 3's verbatim reaper message.
    let failed_err: Option<String> = c
        .query_one(
            "SELECT error FROM pgrg.ingest_jobs WHERE id = $1",
            &[&stale_at_cap],
        )
        .await
        .expect("fetch error")
        .get(0);
    assert_eq!(
        failed_err.as_deref(),
        Some(" (reaper: max attempts reached)"),
        "fail path appends the verbatim reaper message"
    );
}
