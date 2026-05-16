//! SC-004: 2 concurrent worker pools + 50 queued jobs → each processed
//! exactly once (FOR UPDATE SKIP LOCKED). Final: 50 documents, 0 failed,
//! 0 duplicate `content_hash`. Gated by `PGRG_TEST_DATABASE_URL` (port 5443).
//!
//! Two in-test concurrent driver tasks against the same DB prove the
//! `FOR UPDATE SKIP LOCKED` claim coordination identically to two OS
//! processes — without the flakiness of spawning real binaries.

use std::sync::Arc;
use std::time::{Duration, Instant};

use pg_raggraph_core::embedding::EmbeddingBackend;
use pg_raggraph_sidecar::embedder::build_embedder;
use pg_raggraph_sidecar::jobloop::{ProcessOutcome, process_one};
use pg_raggraph_sidecar::{bootstrap, db};
use tokio_postgres::Client;

fn test_db_url() -> Option<String> {
    std::env::var("PGRG_TEST_DATABASE_URL").ok()
}

const JOB_COUNT: i64 = 50;
/// Consecutive Idle observations after which a driver concludes the queue is
/// drained. >1 so a driver doesn't quit while the peer still holds locks.
const IDLE_QUIT_STREAK: u32 = 5;
/// Overall safety bound so a hung pipeline fails the test instead of hanging.
const OVERALL_TIMEOUT: Duration = Duration::from_secs(60);

/// Clean slate + full pgrg.* bootstrap (seeds the 'default' namespace via
/// `001_tables.sql`), then enqueue 50 jobs with UNIQUE content so each yields a
/// distinct document. payload non-NULL + utf-8 → `IngestSource::Text`.
async fn reset_and_enqueue(c: &mut Client) {
    c.batch_execute("DROP SCHEMA IF EXISTS pgrg CASCADE;")
        .await
        .expect("reset");
    bootstrap::run_migrations(c)
        .await
        .expect("bootstrap schema");

    for i in 0..JOB_COUNT {
        let source = format!("doc-{i}");
        let payload = format!("content number {i}").into_bytes();
        c.execute(
            "INSERT INTO pgrg.ingest_jobs \
                 (status, source, namespace, chunk_strategy, attempt_count, payload, enqueued_at, updated_at) \
             VALUES ('queued', $1, 'default', 'auto', 0, $2, now(), now())",
            &[&source, &payload],
        )
        .await
        .expect("enqueue job");
    }

    let queued = count(
        c,
        "SELECT count(*) FROM pgrg.ingest_jobs WHERE status = 'queued'",
    )
    .await;
    assert_eq!(queued, JOB_COUNT, "all 50 jobs enqueued");
}

async fn count(c: &Client, sql: &str) -> i64 {
    c.query_one(sql, &[]).await.expect("count query").get(0)
}

/// One concurrent driver pool: drain via `process_one` until it observes
/// `IDLE_QUIT_STREAK` consecutive Idles, bounded by `OVERALL_TIMEOUT`.
/// Returns how many jobs this pool processed.
async fn run_pool(pool_id: i32, url: String, embedder: Arc<dyn EmbeddingBackend>) -> u32 {
    let start = Instant::now();
    let mut idle_streak = 0u32;
    let mut processed = 0u32;
    while idle_streak < IDLE_QUIT_STREAK {
        assert!(
            start.elapsed() <= OVERALL_TIMEOUT,
            "pool {pool_id} timed out after {OVERALL_TIMEOUT:?}"
        );
        match process_one(&url, &embedder, tokio::runtime::Handle::current()).await {
            Ok(ProcessOutcome::Idle) => {
                idle_streak += 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok(ProcessOutcome::Completed(_)) => {
                idle_streak = 0;
                processed += 1;
            }
            Ok(ProcessOutcome::Failed { id, error }) => {
                idle_streak = 0;
                processed += 1;
                eprintln!("pool {pool_id}: job {id} FAILED: {error}");
            }
            Err(e) => panic!("pool {pool_id}: process_one error: {e:?}"),
        }
    }
    processed
}

/// Assert every SC-004 final invariant against the DB.
async fn assert_invariants(c: &Client) {
    assert_eq!(
        count(c, "SELECT count(*) FROM pgrg.documents").await,
        JOB_COUNT,
        "exactly 50 documents persisted"
    );
    assert_eq!(
        count(
            c,
            "SELECT count(*) FROM pgrg.ingest_jobs WHERE status = 'failed'"
        )
        .await,
        0,
        "no jobs failed"
    );
    assert_eq!(
        count(
            c,
            "SELECT count(*) FROM pgrg.ingest_jobs WHERE status = 'completed'"
        )
        .await,
        JOB_COUNT,
        "exactly 50 jobs marked completed"
    );
    assert_eq!(
        count(
            c,
            "SELECT count(*) - count(DISTINCT content_hash) FROM pgrg.documents"
        )
        .await,
        0,
        "no duplicate content_hash (exactly-once)"
    );
    assert_eq!(
        count(
            c,
            "SELECT count(*) FROM pgrg.ingest_jobs WHERE status IN ('queued', 'running')"
        )
        .await,
        0,
        "no job left queued or running"
    );
}

#[tokio::test]
async fn fifty_jobs_two_pools_each_processed_exactly_once() {
    let Some(url) = test_db_url() else {
        eprintln!("SKIP: PGRG_TEST_DATABASE_URL unset");
        return;
    };

    let mut c = db::connect(&url).await.expect("connect");
    reset_and_enqueue(&mut c).await;

    // Embedder built once, shared across both driver pools (Arc clone).
    let embedder = build_embedder(384, None).expect("embedder");

    // Two concurrent driver tasks racing the same queue.
    let pool_a = tokio::spawn(run_pool(0, url.clone(), Arc::clone(&embedder)));
    let pool_b = tokio::spawn(run_pool(1, url.clone(), Arc::clone(&embedder)));
    let (a, b) = tokio::join!(pool_a, pool_b);
    let processed_a = a.expect("pool 0 join");
    let processed_b = b.expect("pool 1 join");

    // Every queued job was claimed by exactly one pool (SKIP LOCKED): the
    // pools' processed counts sum to exactly JOB_COUNT.
    assert_eq!(
        i64::from(processed_a + processed_b),
        JOB_COUNT,
        "pools processed {processed_a} + {processed_b}; expected exactly {JOB_COUNT} \
         (no job claimed twice, none skipped)"
    );

    assert_invariants(&c).await;
}
