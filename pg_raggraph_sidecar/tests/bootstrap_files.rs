//! DC-003: the SQL the sidecar bundles MUST be byte-identical to what the
//! in-extension build ships in `pg_raggraph/sql/`. `include_str!` the SAME
//! files.

use pg_raggraph_sidecar::bootstrap::EMBEDDED_MIGRATIONS;
use std::fs;

#[test]
fn embedded_sql_is_byte_identical_to_extension_sql() {
    // (name, relative path under pg_raggraph/sql)
    let map: &[(&str, &str)] = &[
        ("000_schema", "000_schema.sql"),
        ("001_tables", "001_tables.sql"),
        ("002_indexes", "002_indexes.sql"),
        ("003_migrations_table", "003_migrations_table.sql"),
        (
            "004_retrieval_indexes",
            "migrations/004_retrieval_indexes.sql",
        ),
        (
            "005_status_check_atomicity",
            "migrations/005_status_check_atomicity.sql",
        ),
    ];
    let base = concat!(env!("CARGO_MANIFEST_DIR"), "/../pg_raggraph/sql/");
    for (name, rel) in map {
        let on_disk = fs::read_to_string(format!("{base}{rel}"))
            .unwrap_or_else(|e| panic!("read {rel}: {e}"));
        let embedded = EMBEDDED_MIGRATIONS
            .iter()
            .find(|m| m.name == *name)
            .unwrap_or_else(|| panic!("embedded migration {name} missing"));
        assert_eq!(
            embedded.sql, on_disk,
            "DC-003 drift: embedded {name} != pg_raggraph/sql/{rel}"
        );
    }
    assert_eq!(
        EMBEDDED_MIGRATIONS.len(),
        map.len(),
        "migration count mismatch"
    );
}
