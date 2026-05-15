//! Embedded SQL bootstrap. The sidecar runs the SAME migration files the
//! in-extension build ships (`pg_raggraph/sql/*`), bundled via `include_str!`,
//! applied idempotently against `pgrg.migrations`. DC-003: byte-identical.
//!
//! Why this is not the plan's naive double-execute design:
//!
//! - `000_schema.sql` does **not** create the `pgrg` schema. In-extension,
//!   pgrx's `.control` file (`schema = 'pgrg'`) creates it. The sidecar has no
//!   `.control` mechanism, so it must `CREATE SCHEMA IF NOT EXISTS pgrg`
//!   itself. That statement is sidecar bootstrap glue — it is NOT one of the
//!   embedded files, so DC-003 byte-identity is unaffected.
//! - `003_migrations_table.sql` is a bare, non-idempotent
//!   `CREATE TABLE pgrg.migrations (version int PRIMARY KEY, applied_at ...)`
//!   followed by a bare `INSERT ... VALUES (1)`. There is **no `name` column**.
//!   Re-executing it would error ("relation already exists"), and an
//!   `INSERT (version, name, applied_at)` would error ("column name").
//!
//! Correct design: the base bootstrap (`000`-`003`) runs **exactly once**,
//! guarded by `to_regclass('pgrg.migrations') IS NULL`. After that, only
//! incremental migration files (`004`, `005`, ...) are applied, recorded with
//! `INSERT (version) ON CONFLICT DO NOTHING` against the real 2-column
//! `pgrg.migrations` schema. The whole thing runs in one transaction so a
//! mid-bootstrap failure rolls back cleanly and a retry sees a clean DB.

use tokio_postgres::Client;

/// One embedded migration file, bundled byte-identical to `pg_raggraph/sql/`.
pub struct Migration {
    /// Logical version. `0`-`3` are the run-once base bootstrap; `4`+ are
    /// incremental migrations recorded individually in `pgrg.migrations`.
    pub version: i32,
    /// Stable name; matched byte-for-byte by the DC-003 guard test.
    pub name: &'static str,
    /// Verbatim file contents (`include_str!` of the shared SQL).
    pub sql: &'static str,
}

// Paths relative to pg_raggraph_sidecar/src/. The `../../pg_raggraph/sql` hop
// reaches the SAME source files the extension's extension_sql_file! uses.
pub static EMBEDDED_MIGRATIONS: &[Migration] = &[
    Migration {
        version: 0,
        name: "000_schema",
        sql: include_str!("../../pg_raggraph/sql/000_schema.sql"),
    },
    Migration {
        version: 1,
        name: "001_tables",
        sql: include_str!("../../pg_raggraph/sql/001_tables.sql"),
    },
    Migration {
        version: 2,
        name: "002_indexes",
        sql: include_str!("../../pg_raggraph/sql/002_indexes.sql"),
    },
    Migration {
        version: 3,
        name: "003_migrations_table",
        sql: include_str!("../../pg_raggraph/sql/003_migrations_table.sql"),
    },
    Migration {
        version: 4,
        name: "004_retrieval_indexes",
        sql: include_str!("../../pg_raggraph/sql/migrations/004_retrieval_indexes.sql"),
    },
    Migration {
        version: 5,
        name: "005_status_check_atomicity",
        sql: include_str!("../../pg_raggraph/sql/migrations/005_status_check_atomicity.sql"),
    },
];

/// Versions `0`-`3` form the run-once base bootstrap (schema + tables +
/// indexes + the `pgrg.migrations` table itself). They are not idempotent and
/// are applied as a unit only when `pgrg.migrations` does not yet exist.
const BASE_BOOTSTRAP_MAX_VERSION: i32 = 3;

/// Bring the database schema up to date. Idempotent across runs.
///
/// Single transaction:
/// 1. `CREATE SCHEMA IF NOT EXISTS pgrg` (sidecar glue — replaces the
///    `.control` schema directive the in-extension build relies on).
/// 2. If `pgrg.migrations` does not exist, run the base bootstrap
///    (`000`-`003`) once, in order. `003` itself creates the table and
///    inserts its baseline row, so nothing else is recorded for `0`-`3`.
/// 3. Read applied versions from `pgrg.migrations`.
/// 4. Apply each incremental migration (`version > 3`) not yet recorded, in
///    version order, each followed by
///    `INSERT (version) ... ON CONFLICT DO NOTHING`.
/// 5. `COMMIT`.
///
/// Returns the count of newly applied migrations (base bootstrap counts as one
/// when it runs).
///
/// # Errors
/// Returns an error if any embedded statement fails or the transaction cannot
/// commit; the transaction is rolled back so a retry sees a clean database.
pub async fn run_migrations(client: &mut Client) -> anyhow::Result<usize> {
    let tx = client.transaction().await?;

    // (1) Schema. The shared 000_schema.sql deliberately does NOT create this
    // (pgrx's .control does, in-extension). Sidecar must.
    tx.batch_execute("CREATE SCHEMA IF NOT EXISTS pgrg").await?;

    let mut newly = 0usize;

    // (2) Run-once base bootstrap, gated on the migrations table's absence.
    let migrations_table = tx
        .query_one("SELECT to_regclass('pgrg.migrations') IS NULL", &[])
        .await?;
    let needs_base: bool = migrations_table.get(0);
    if needs_base {
        for m in EMBEDDED_MIGRATIONS
            .iter()
            .filter(|m| m.version <= BASE_BOOTSTRAP_MAX_VERSION)
        {
            tx.batch_execute(m.sql)
                .await
                .map_err(|e| anyhow::anyhow!("base bootstrap {}: {e}", m.name))?;
        }
        newly += 1;
    }

    // (3) Applied versions (the table exists now, either pre-existing or
    // just created by 003).
    let applied: std::collections::HashSet<i32> = tx
        .query("SELECT version FROM pgrg.migrations", &[])
        .await?
        .iter()
        .map(|r| r.get::<_, i32>(0))
        .collect();

    // (4) Incremental migrations only. Base versions (0-3) are never
    // re-executed; 003 already recorded the baseline row.
    for m in EMBEDDED_MIGRATIONS
        .iter()
        .filter(|m| m.version > BASE_BOOTSTRAP_MAX_VERSION)
    {
        if applied.contains(&m.version) {
            continue;
        }
        tx.batch_execute(m.sql)
            .await
            .map_err(|e| anyhow::anyhow!("migration {}: {e}", m.name))?;
        tx.execute(
            "INSERT INTO pgrg.migrations (version) \
             VALUES ($1) ON CONFLICT (version) DO NOTHING",
            &[&m.version],
        )
        .await?;
        newly += 1;
    }

    // (5)
    tx.commit().await?;
    Ok(newly)
}
