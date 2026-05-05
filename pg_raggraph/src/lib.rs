//! pg_raggraph — PostgreSQL-native GraphRAG extension.
//!
//! See `docs/superpowers/specs/2026-05-03-pg-raggraph-rs-extension-design.md`.

use pgrx::prelude::*;

::pgrx::pg_module_magic!(name, version);

mod admin;

::pgrx::extension_sql_file!(
    "../sql/000_schema.sql",
    name = "bootstrap_schema",
    bootstrap
);
::pgrx::extension_sql_file!(
    "../sql/001_tables.sql",
    name = "create_tables",
    requires = ["bootstrap_schema"]
);
::pgrx::extension_sql_file!(
    "../sql/002_indexes.sql",
    name = "create_indexes",
    requires = ["create_tables"]
);
::pgrx::extension_sql_file!(
    "../sql/003_migrations_table.sql",
    name = "migrations_table",
    requires = ["create_tables"]
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn extension_loads() {
        assert_eq!(Spi::get_one::<bool>("SELECT true").unwrap(), Some(true));
    }

    #[pg_test]
    fn schema_tables_exist() {
        let tables: Vec<String> = Spi::connect(|client| {
            let rows = client
                .select(
                    "SELECT tablename::text FROM pg_tables WHERE schemaname = 'pgrg' ORDER BY tablename",
                    None,
                    &[],
                )
                .unwrap();
            rows.map(|r| r.get::<String>(1).unwrap().unwrap()).collect()
        });

        let expected: Vec<&str> = vec![
            "chunk_entities",
            "chunks",
            "documents",
            "entities",
            "ingest_jobs",
            "migrations",
            "namespaces",
            "providers",
            "relationships",
        ];
        let actual: Vec<&str> = tables.iter().map(String::as_str).collect();
        assert_eq!(actual, expected, "expected pgrg.* tables present");
    }

    #[pg_test]
    fn migrations_seeded() {
        let v: Option<i32> = Spi::get_one("SELECT max(version) FROM pgrg.migrations").unwrap();
        assert_eq!(v, Some(1));
    }

    #[pg_test]
    fn default_namespace_present() {
        let n: Option<i64> =
            Spi::get_one("SELECT count(*) FROM pgrg.namespaces WHERE name = 'default'").unwrap();
        assert_eq!(n, Some(1));
    }

    #[pg_test]
    fn namespace_create_inserts_row() {
        Spi::run("SELECT pgrg.namespace_create('test_ns')").unwrap();
        let n: Option<i64> =
            Spi::get_one("SELECT count(*) FROM pgrg.namespaces WHERE name = 'test_ns'").unwrap();
        assert_eq!(n, Some(1));
    }

    #[pg_test]
    fn namespace_drop_removes_row() {
        Spi::run("SELECT pgrg.namespace_create('drop_me')").unwrap();
        Spi::run("SELECT pgrg.namespace_drop('drop_me', false)").unwrap();
        let n: Option<i64> =
            Spi::get_one("SELECT count(*) FROM pgrg.namespaces WHERE name = 'drop_me'").unwrap();
        assert_eq!(n, Some(0));
    }

    #[pg_test]
    fn provider_create_then_list() {
        Spi::run(
            "SELECT pgrg.provider_create('p1', 'llm', 'openai', \
                                          'https://api.openai.com', 'gpt-4o-mini', \
                                          'sk-test-secret-1234567890', '{}')",
        )
        .unwrap();

        let json: pgrx::JsonB = Spi::get_one("SELECT pgrg.provider_list()")
            .unwrap()
            .expect("provider_list returned NULL");
        let arr = json.0.as_array().expect("provider_list returns array");
        assert_eq!(arr.len(), 1);
        let obj = &arr[0];
        assert_eq!(obj["name"], "p1");
        assert_eq!(obj["kind"], "llm");
        assert_eq!(obj["provider"], "openai");
        let cred = obj["credential"].as_str().unwrap();
        assert!(
            cred.starts_with("sk-"),
            "credential should still show prefix"
        );
        assert!(cred.contains("***"), "credential should be redacted");
        assert!(
            !cred.contains("1234567890"),
            "credential should not include the secret"
        );
    }

    #[pg_test]
    fn provider_drop_removes_row() {
        Spi::run(
            "SELECT pgrg.provider_create('p2', 'embedding', 'openai', \
                                          'https://api.openai.com', 'text-embedding-3-small', \
                                          'sk-also-secret', '{}')",
        )
        .unwrap();
        Spi::run("SELECT pgrg.provider_drop('p2')").unwrap();
        let n: Option<i64> =
            Spi::get_one("SELECT count(*) FROM pgrg.providers WHERE name = 'p2'").unwrap();
        assert_eq!(n, Some(0));
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![
            "shared_preload_libraries='pg_raggraph'",
            "pg_raggraph.bgw_workers=2",
        ]
    }
}
