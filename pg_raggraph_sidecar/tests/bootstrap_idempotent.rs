//! SC-002: first-connect bootstrap creates all pgrg.* tables on a stock
//! managed-PG-like server (only vector + `pg_trgm` pre-installed).
//! SC-003: a second run is a no-op (no error, no duplicate migration rows).
//!
//! Gated by `PGRG_TEST_DATABASE_URL` (CI sets it from docker-compose.test.yml,
//! host port 5443).

use pg_raggraph_sidecar::{bootstrap, db};

fn test_db_url() -> Option<String> {
    std::env::var("PGRG_TEST_DATABASE_URL").ok()
}

#[tokio::test]
async fn bootstrap_creates_schema_then_is_idempotent() {
    let Some(url) = test_db_url() else {
        eprintln!("SKIP: PGRG_TEST_DATABASE_URL unset");
        return;
    };
    let mut c = db::connect(&url).await.expect("connect");

    // Clean slate. CREATE SCHEMA IF NOT EXISTS in run_migrations recreates it.
    c.batch_execute("DROP SCHEMA IF EXISTS pgrg CASCADE;")
        .await
        .expect("reset");

    // First run. run_migrations counts the base bootstrap (versions 0-3,
    // which includes 003's `INSERT INTO pgrg.migrations (version) VALUES (1)`)
    // as ONE unit, then each incremental migration (004, 005) as one more.
    // Fresh-DB return value = 3 (1 base unit + 004 + 005), NOT 6.
    let n1 = bootstrap::run_migrations(&mut c)
        .await
        .expect("first bootstrap");
    assert_eq!(n1, 3, "first run = 1 base-bootstrap unit + 004 + 005");

    // The canonical pgrg.* tables Plan 1 produces in-extension.
    let tables: Vec<String> = c
        .query(
            "SELECT tablename FROM pg_tables WHERE schemaname='pgrg' ORDER BY tablename",
            &[],
        )
        .await
        .expect("introspect")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    for expected in [
        "chunk_entities",
        "chunks",
        "documents",
        "entities",
        "ingest_jobs",
        "migrations",
        "namespaces",
        "providers",
        "relationships",
    ] {
        assert!(
            tables.contains(&expected.to_string()),
            "missing pgrg.{expected}; got {tables:?}"
        );
    }

    // Second run: idempotent — zero newly applied, no error.
    let n2 = bootstrap::run_migrations(&mut c)
        .await
        .expect("second bootstrap");
    assert_eq!(n2, 0, "second run must apply nothing (idempotent)");

    // pgrg.migrations holds DB-side version rows, NOT one row per embedded
    // file. 003 inserts VALUES (1); incrementals 004/005 insert their own
    // versions. So the table has exactly 3 rows: versions {1, 4, 5}.
    let mig_count: i64 = c
        .query_one("SELECT count(*) FROM pgrg.migrations", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(mig_count, 3, "migrations rows = versions {{1,4,5}}");

    let versions: Vec<i32> = c
        .query("SELECT version FROM pgrg.migrations ORDER BY version", &[])
        .await
        .expect("versions")
        .iter()
        .map(|r| r.get::<_, i32>(0))
        .collect();
    assert_eq!(
        versions,
        vec![1, 4, 5],
        "expected baseline v1 + incrementals v4,v5"
    );
}
