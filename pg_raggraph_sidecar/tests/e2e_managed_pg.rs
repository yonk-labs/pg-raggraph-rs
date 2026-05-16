//! SC-012 / SC-015: the headline thesis proof. The sidecar performs a full
//! ingest + grounded ask against **stock** managed-style Postgres
//! (`pgvector/pgvector:pg17`, the `pg-stock` docker fixture) — NO
//! `shared_preload_libraries`, NO `CREATE EXTENSION pg_raggraph`. The fixture's
//! docker-init applies only `CREATE EXTENSION vector; CREATE EXTENSION
//! pg_trgm;` (the sole privileged setup a managed-PG user can do); the sidecar
//! bootstrap creates every `pgrg.*` object itself.
//!
//! End-to-end (spec §8 4-statement managed-PG demo):
//!   1. clean-slate bootstrap (`DROP SCHEMA pgrg CASCADE` → `run_migrations`)
//!   2. `default` namespace seeded by `001_tables.sql`
//!   3. register a `mock` LLM provider (stub answer with `[1]` → one citation)
//!   4. enqueue a text ingest job, drive `jobloop::process_one` until it is
//!      `completed` — the real claim → `run_job` → commit path on stock PG
//!   5. assert one `pgrg.documents` row + ≥1 `pgrg.chunks` row
//!   6. `POST /v1/ask` against the axum server → 200, non-empty answer,
//!      citation `chunk_id` ∈ `pgrg.chunks`, `mode_used == "hybrid"`
//!
//! DB-gated: requires `PGRG_TEST_DATABASE_URL` (port 5443, the no-preload
//! `pg-stock` fixture) and SKIPs cleanly when unset so CI without a database
//! stays green.

use std::sync::Arc;
use std::time::{Duration, Instant};

use pg_raggraph_core::embedding::EmbeddingBackend;
use pg_raggraph_sidecar::config::SidecarConfig;
use pg_raggraph_sidecar::embedder::build_embedder;
use pg_raggraph_sidecar::jobloop::{ProcessOutcome, process_one};
use pg_raggraph_sidecar::{bootstrap, db, http};
use tokio_postgres::Client;
use uuid::Uuid;

fn test_db_url() -> Option<String> {
    std::env::var("PGRG_TEST_DATABASE_URL").ok()
}

/// Overall safety bound: a hung ingest must fail the test, not hang CI.
const INGEST_TIMEOUT: Duration = Duration::from_secs(30);

async fn count(c: &Client, sql: &str) -> i64 {
    c.query_one(sql, &[]).await.expect("count query").get(0)
}

/// Clean slate + full `pgrg.*` bootstrap against stock PG. `run_migrations`
/// does `CREATE SCHEMA IF NOT EXISTS pgrg` then applies the SAME embedded SQL
/// the in-extension build ships — proving the sidecar needs no privileged
/// preload. `001_tables.sql` seeds the `default` namespace.
async fn reset_bootstrap_and_register_provider(c: &mut Client) {
    c.batch_execute("DROP SCHEMA IF EXISTS pgrg CASCADE;")
        .await
        .expect("reset schema");
    bootstrap::run_migrations(c)
        .await
        .expect("bootstrap schema on stock PG (no preload)");

    // `default` namespace must exist — seeded by 001_tables.sql line 108.
    let ns: i64 = count(
        c,
        "SELECT count(*) FROM pgrg.namespaces WHERE name = 'default'",
    )
    .await;
    assert_eq!(ns, 1, "bootstrap must seed the 'default' namespace");

    // Mock LLM provider: build_provider_impl maps `provider='mock'` to
    // MockProvider::with_stub_answer(credential), so the credential IS the
    // canned answer. `[1]` makes _core::llm::ask emit one citation to the
    // first prompt chunk.
    c.execute(
        "INSERT INTO pgrg.providers (name, kind, provider, model, credential) \
         VALUES ('mock', 'llm', 'mock', 'mock-model', $1)",
        &[&"According to the source, the fox is quick [1]."],
    )
    .await
    .expect("register mock provider");

    // Point the default namespace at the mock provider so the
    // resolve-by-namespace path works without an explicit llm_provider.
    c.execute(
        "UPDATE pgrg.namespaces SET llm_provider = 'mock' WHERE name = 'default'",
        &[],
    )
    .await
    .expect("set namespace default provider");
}

/// Drive the REAL claim → `run_job` → commit path via `process_one` in a
/// bounded loop until `job_id` is `completed` (the pgrx bg-worker queue path
/// is precluded in tests by MVCC — Plan 5 T13 precedent). Panics on timeout
/// or job failure.
async fn drive_ingest_to_completion(url: &str, embedder: &Arc<dyn EmbeddingBackend>, job_id: Uuid) {
    let start = Instant::now();
    loop {
        assert!(
            start.elapsed() <= INGEST_TIMEOUT,
            "ingest did not complete within {INGEST_TIMEOUT:?}"
        );
        match process_one(url, embedder, tokio::runtime::Handle::current())
            .await
            .expect("process_one must not error on stock PG")
        {
            ProcessOutcome::Completed(id) => {
                assert_eq!(id, job_id, "completed job must be the one we enqueued");
                return;
            }
            ProcessOutcome::Failed { id, error } => {
                panic!("ingest job {id} FAILED on stock PG: {error}");
            }
            ProcessOutcome::Idle => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// Spawn the axum server (ephemeral port) against `url`, `POST /v1/ask`, and
/// assert: 200, non-empty answer, `mode_used == "hybrid"`, ≥1 citation whose
/// `chunk_id` exists in `pgrg.chunks` (grounded-answer guarantee, SC-012).
async fn assert_grounded_ask(url: &str, setup: &Client) {
    let cfg = Arc::new(SidecarConfig::parse_from([
        "pg-raggraph-sidecar",
        "--database-url",
        url,
        "--http-bind",
        "127.0.0.1:0",
    ]));
    let app = http::router(Arc::clone(&cfg)).expect("build router");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/ask"))
        .json(&serde_json::json!({
            "q": "what is the fox?",
            "namespace": "default",
            "top_k": 10
        }))
        .send()
        .await
        .expect("POST /v1/ask");

    assert_eq!(
        resp.status().as_u16(),
        200,
        "POST /v1/ask must return 200 on stock PG"
    );
    let body: serde_json::Value = resp.json().await.expect("parse JSON body");

    let answer = body["answer"].as_str().expect("answer is a string");
    assert!(!answer.is_empty(), "answer must be non-empty: {body}");

    assert_eq!(
        body["mode_used"].as_str(),
        Some("hybrid"),
        "mode_used must be 'hybrid': {body}"
    );

    let citations = body["citations"].as_array().expect("citations is an array");
    assert!(
        !citations.is_empty(),
        "expected at least one citation from the `[1]` stub: {body}"
    );

    // Every cited chunk_id must exist in the pgrg.chunks the pipeline wrote
    // on stock PG (grounded-answer guarantee, SC-012).
    for cit in citations {
        let cid = cit["chunk_id"].as_str().expect("citation.chunk_id string");
        let uuid: Uuid = cid.parse().expect("citation.chunk_id is a UUID");
        let exists: bool = setup
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pgrg.chunks WHERE id = $1)",
                &[&uuid],
            )
            .await
            .expect("chunk existence query")
            .get(0);
        assert!(exists, "cited chunk {cid} must exist in pgrg.chunks");
    }

    server.abort();
}

/// SC-012: stock PG (no preload) + sidecar → full ingest + grounded ask.
/// SC-015: this runs under `cargo test -p pg_raggraph_sidecar` against the
/// docker fixture in CI.
#[tokio::test]
async fn managed_pg_full_ingest_then_grounded_ask() {
    let Some(url) = test_db_url() else {
        eprintln!("SKIP: PGRG_TEST_DATABASE_URL unset");
        return;
    };

    // Embedder built exactly as the handler / worker build it (dim 384,
    // deterministic — no model baked into the image, SC-012).
    let embedder: Arc<dyn EmbeddingBackend> = build_embedder(384, None).expect("build embedder");

    let mut setup = db::connect(&url).await.expect("connect for setup");
    reset_bootstrap_and_register_provider(&mut setup).await;

    // (4) Enqueue ONE text ingest job. payload non-NULL + utf-8 →
    // IngestSource::Text (jobloop::row_to_ingest_source). Columns match the
    // real pgrg.ingest_jobs DDL (001_tables.sql:78-91).
    let payload = b"the quick brown fox".to_vec();
    let job_id: Uuid = setup
        .query_one(
            "INSERT INTO pgrg.ingest_jobs \
                 (status, source, namespace, chunk_strategy, attempt_count, \
                  payload, enqueued_at, updated_at) \
             VALUES ('queued', 'e2e-doc', 'default', 'auto', 0, $1, now(), now()) \
             RETURNING id",
            &[&payload],
        )
        .await
        .expect("enqueue ingest job")
        .get(0);

    drive_ingest_to_completion(&url, &embedder, job_id).await;

    let status: String = setup
        .query_one(
            "SELECT status FROM pgrg.ingest_jobs WHERE id = $1",
            &[&job_id],
        )
        .await
        .expect("job status query")
        .get(0);
    assert_eq!(status, "completed", "job must be completed");

    // (5) Exactly one document for our source, ≥1 chunk written by the
    // pipeline running entirely on stock (no-preload) PG.
    let docs = count(
        &setup,
        "SELECT count(*) FROM pgrg.documents WHERE source = 'e2e-doc'",
    )
    .await;
    assert_eq!(docs, 1, "exactly one document ingested from 'e2e-doc'");

    let chunks = count(&setup, "SELECT count(*) FROM pgrg.chunks").await;
    assert!(chunks >= 1, "at least one chunk written, got {chunks}");

    // (6) Grounded ask over HTTP against the same stock DB.
    assert_grounded_ask(&url, &setup).await;
}
