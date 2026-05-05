//! pg_raggraph — PostgreSQL-native GraphRAG extension.
//!
//! See `docs/superpowers/specs/2026-05-03-pg-raggraph-rs-extension-design.md`.

use pgrx::prelude::*;

::pgrx::pg_module_magic!(name, version);

mod admin;
mod embedding;
mod gucs;
mod ingest_extracted;
mod retrieval;

/// Called by PostgreSQL when the extension shared library is loaded.
/// Registers GUCs so they are available before CREATE EXTENSION runs.
#[allow(non_snake_case)]
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    gucs::register();
}

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
::pgrx::extension_sql_file!(
    "../sql/migrations/004_retrieval_indexes.sql",
    name = "retrieval_indexes",
    requires = ["create_indexes"]
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

    #[pg_test]
    fn health_returns_expected_keys() {
        let json: pgrx::JsonB = Spi::get_one("SELECT pgrg.health()")
            .unwrap()
            .expect("health() returned NULL");
        let obj = json.0.as_object().expect("health() returns object");
        for k in ["version", "schema_version", "queue_depth", "bgw_workers"] {
            assert!(obj.contains_key(k), "health() missing key `{k}`");
        }
        assert_eq!(obj["bgw_workers"], 2);
        assert_eq!(obj["queue_depth"], 0);
        let v = obj["version"].as_str().unwrap();
        assert!(
            v.starts_with("0.1.0"),
            "version should start with 0.1.0, got {v}"
        );
    }

    #[pg_test]
    fn status_summary_has_zero_jobs() {
        let json: pgrx::JsonB = Spi::get_one("SELECT pgrg.status()")
            .unwrap()
            .expect("status() returned NULL");
        let obj = json.0.as_object().unwrap();
        assert_eq!(obj["queued"], 0);
        assert_eq!(obj["running"], 0);
        assert_eq!(obj["completed"], 0);
        assert_eq!(obj["failed"], 0);
    }

    #[pg_test]
    fn status_unknown_job_returns_null() {
        let json: Option<pgrx::JsonB> =
            Spi::get_one("SELECT pgrg.status('00000000-0000-0000-0000-000000000000'::uuid)")
                .unwrap();
        assert!(json.is_none(), "unknown job_id should return NULL");
    }

    #[pg_test]
    fn delete_document_removes_chunks_via_cascade() {
        Spi::run("SELECT pgrg.namespace_create('del_doc_ns')").unwrap();
        Spi::run(
            "INSERT INTO pgrg.documents (id, namespace, source, content_hash) \
             VALUES ('11111111-1111-1111-1111-111111111111', 'del_doc_ns', 'a.md', 'hash1')",
        )
        .unwrap();
        Spi::run(
            "INSERT INTO pgrg.chunks (namespace, document_id, ord, text, token_count) \
             VALUES ('del_doc_ns', '11111111-1111-1111-1111-111111111111', 0, 'hi', 1)",
        )
        .unwrap();

        Spi::run("SELECT pgrg.delete_document('11111111-1111-1111-1111-111111111111'::uuid)")
            .unwrap();

        let docs: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.documents \
             WHERE id = '11111111-1111-1111-1111-111111111111'",
        )
        .unwrap();
        assert_eq!(docs, Some(0));

        let chunks: Option<i64> =
            Spi::get_one("SELECT count(*) FROM pgrg.chunks WHERE namespace = 'del_doc_ns'")
                .unwrap();
        assert_eq!(chunks, Some(0), "chunks must cascade");
    }

    #[pg_test]
    fn delete_namespace_without_cascade_blocks_when_docs_exist() {
        Spi::run("SELECT pgrg.namespace_create('blocked_ns')").unwrap();
        Spi::run(
            "INSERT INTO pgrg.documents (namespace, source, content_hash) \
             VALUES ('blocked_ns', 'b.md', 'hashB')",
        )
        .unwrap();

        let res = std::panic::catch_unwind(|| {
            Spi::run("SELECT pgrg.namespace_drop('blocked_ns', false)").unwrap();
        });
        assert!(res.is_err(), "namespace_drop without cascade must error");
    }

    #[pg_test]
    fn parity_mode_creates_ivfflat_indexes() {
        // SC-009: parity_mode at namespace_create swaps HNSW -> IVFFlat
        Spi::run("SET pg_raggraph.parity_mode = true").unwrap();
        Spi::run("SELECT pgrg.namespace_create('parity_ns')").unwrap();

        let chunk_idx_def: Option<String> = Spi::get_one(
            "SELECT indexdef FROM pg_indexes \
             WHERE schemaname = 'pgrg' AND indexname = 'chunks_embedding_hnsw'",
        )
        .unwrap();

        let def = chunk_idx_def.expect("chunks_embedding_hnsw must exist");
        assert!(
            def.contains("USING ivfflat"),
            "expected IVFFlat under parity_mode, got: {def}"
        );

        let entity_idx_def: Option<String> = Spi::get_one(
            "SELECT indexdef FROM pg_indexes \
             WHERE schemaname = 'pgrg' AND indexname = 'entities_name_emb_hnsw'",
        )
        .unwrap();
        let edef = entity_idx_def.expect("entities_name_emb_hnsw must exist");
        assert!(
            edef.contains("USING ivfflat"),
            "expected IVFFlat under parity_mode, got: {edef}"
        );

        Spi::run("SET pg_raggraph.parity_mode = false").unwrap();
    }

    #[pg_test]
    fn default_mode_keeps_hnsw_indexes() {
        // Counterpart: default install must remain HNSW.
        let chunk_idx_def: Option<String> = Spi::get_one(
            "SELECT indexdef FROM pg_indexes \
             WHERE schemaname = 'pgrg' AND indexname = 'chunks_embedding_hnsw'",
        )
        .unwrap();
        let def = chunk_idx_def.expect("chunks_embedding_hnsw must exist");
        assert!(
            def.contains("USING hnsw"),
            "default install must use HNSW, got: {def}"
        );
    }

    #[pg_test]
    fn embed_returns_correct_dim_vector() {
        // SC-002: pgrg.embed returns a vector(N) where N = pg_raggraph.embed_dim.
        // pgvector returns vectors as text in the form '[v1,v2,...]'; the dim
        // is verifiable by parsing the comma count. Use vector_dims() from
        // pgvector to assert without parsing strings.
        let dim: Option<i32> =
            Spi::get_one("SELECT vector_dims(pgrg.embed('hello world'))").unwrap();
        assert_eq!(dim, Some(384));
    }

    #[pg_test]
    fn embed_is_deterministic() {
        // SC-002: two consecutive calls on the same input return byte-identical vectors.
        let same: Option<bool> = Spi::get_one(
            "SELECT pgrg.embed('hello world')::text = pgrg.embed('hello world')::text",
        )
        .unwrap();
        assert_eq!(same, Some(true));
    }

    #[pg_test]
    fn embed_works_without_providers_table_rows() {
        // SC-011: fresh DB with no providers rows — pgrg.embed must succeed.
        let n: Option<i64> = Spi::get_one("SELECT count(*) FROM pgrg.providers").unwrap();
        assert_eq!(n, Some(0), "test precondition: no providers rows");
        // If this errors, SC-011 fails.
        let _: Option<i32> = Spi::get_one("SELECT vector_dims(pgrg.embed('q'))").unwrap();
    }

    #[pg_test]
    fn ingest_extracted_loads_fixture_into_tables() {
        // SC-003: load fixture, verify all four tables populated, verify
        // ingest_jobs is NOT touched.
        Spi::run("SELECT pgrg.namespace_create('fix_ns')").unwrap();

        // Build a 384-dim fixture so embeddings match the GUC dim.
        let dim: i32 = Spi::get_one::<i32>("SELECT current_setting('pg_raggraph.embed_dim')::int")
            .unwrap()
            .unwrap();
        let mut emb = String::from("[");
        for i in 0..dim {
            if i > 0 {
                emb.push(',');
            }
            emb.push_str(&format!("{}", (i as f32) * 0.0001));
        }
        emb.push(']');
        let emb_chunk = format!(
            r#"{{"kind":"chunk","id":"c0000000-0000-0000-0000-000000000003","namespace":"fix_ns","document_id":"a0000000-0000-0000-0000-000000000001","ord":2,"text":"epsilon zeta","token_count":2,"embedding":{emb}}}"#
        );
        let path = "/tmp/pgrg_fix_test.jsonl";
        std::fs::write(
            path,
            format!(
                "{}\n{}\n",
                r#"{"kind":"document","id":"a0000000-0000-0000-0000-000000000001","namespace":"fix_ns","source":"d.md","content_hash":"h-fix-1"}"#,
                emb_chunk,
            ),
        )
        .expect("write fixture");

        Spi::run("SELECT pgrg.ingest_extracted('/tmp/pgrg_fix_test.jsonl', 'fix_ns')").unwrap();

        let docs: Option<i64> =
            Spi::get_one("SELECT count(*) FROM pgrg.documents WHERE namespace = 'fix_ns'").unwrap();
        assert_eq!(docs, Some(1));

        let chunks: Option<i64> =
            Spi::get_one("SELECT count(*) FROM pgrg.chunks WHERE namespace = 'fix_ns'").unwrap();
        assert_eq!(chunks, Some(1));

        // SC-003: ingest_jobs MUST be unchanged.
        let jobs: Option<i64> =
            Spi::get_one("SELECT count(*) FROM pgrg.ingest_jobs WHERE namespace = 'fix_ns'")
                .unwrap();
        assert_eq!(jobs, Some(0), "ingest_extracted must NOT enqueue jobs");
    }

    fn load_minimal_fixture_for_query(ns: &str) {
        // Helper used by query tests: load 3 chunks (alpha/beta/gamma), 1 entity, 1 chunk_entity.
        Spi::run(&format!("SELECT pgrg.namespace_create('{ns}')")).unwrap();
        let dim: i32 = Spi::get_one::<i32>("SELECT current_setting('pg_raggraph.embed_dim')::int")
            .unwrap()
            .unwrap();
        let mk_emb = |seed: f32| {
            let mut s = String::from("[");
            for i in 0..dim {
                if i > 0 {
                    s.push(',');
                }
                s.push_str(&format!("{}", seed + (i as f32) * 0.0001));
            }
            s.push(']');
            s
        };
        let path = format!("/tmp/pgrg_q_{ns}.jsonl");
        std::fs::write(
            &path,
            format!(
                concat!(
                    r#"{{"kind":"document","id":"a0000000-0000-0000-0000-000000000010","namespace":"{ns}","source":"d.md","content_hash":"h-q-{ns}"}}"#, "\n",
                    r#"{{"kind":"chunk","id":"c0000000-0000-0000-0000-000000000011","namespace":"{ns}","document_id":"a0000000-0000-0000-0000-000000000010","ord":0,"text":"alpha auth module","token_count":3,"embedding":{e1}}}"#, "\n",
                    r#"{{"kind":"chunk","id":"c0000000-0000-0000-0000-000000000012","namespace":"{ns}","document_id":"a0000000-0000-0000-0000-000000000010","ord":1,"text":"beta gamma","token_count":2,"embedding":{e2}}}"#, "\n",
                    r#"{{"kind":"entity","id":"e0000000-0000-0000-0000-000000000020","namespace":"{ns}","name":"alpha","kind_label":"module","name_emb":{e3}}}"#, "\n",
                    r#"{{"kind":"chunk_entity","chunk_id":"c0000000-0000-0000-0000-000000000011","entity_id":"e0000000-0000-0000-0000-000000000020","confidence":0.9,"classification":"extracted"}}"#, "\n",
                ),
                ns = ns,
                e1 = mk_emb(0.1),
                e2 = mk_emb(0.5),
                e3 = mk_emb(0.1),
            ),
        )
        .expect("fixture write");
        Spi::run(&format!("SELECT pgrg.ingest_extracted('{path}', '{ns}')")).unwrap();
    }

    #[pg_test]
    fn query_hybrid_returns_documented_columns() {
        // SC-001: column shape (chunk_id, document_id, text, score, signals) in descending score order.
        load_minimal_fixture_for_query("q_hybrid_ns");
        let json: pgrx::JsonB = Spi::get_one(
            "SELECT to_jsonb(t) FROM pgrg.query('alpha', NULL, 5, 'q_hybrid_ns', 1, NULL, 'hybrid') t LIMIT 1",
        )
        .unwrap()
        .expect("query returned no rows");
        let obj = json.0.as_object().unwrap();
        for k in ["chunk_id", "document_id", "text", "score", "signals"] {
            assert!(obj.contains_key(k), "result missing key {k}");
        }
        let signals = obj["signals"].as_array().expect("signals is array");
        assert!(!signals.is_empty(), "signals must be populated");
    }

    #[pg_test]
    fn query_hybrid_descending_score_order() {
        load_minimal_fixture_for_query("q_order_ns");
        let scores: Vec<f64> = Spi::connect(|client| {
            client
                .select(
                    "SELECT score FROM pgrg.query('alpha auth', NULL, 5, 'q_order_ns', 1, NULL, 'hybrid')",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| r.get::<f64>(1).unwrap().unwrap_or(0.0))
                .collect()
        });
        for w in scores.windows(2) {
            assert!(
                w[0] >= w[1],
                "results must be descending by score, got {scores:?}"
            );
        }
    }

    #[pg_test]
    fn gucs_have_expected_defaults() {
        let workers: Option<i32> =
            Spi::get_one("SELECT current_setting('pg_raggraph.bgw_workers')::int").unwrap();
        assert_eq!(workers, Some(2));

        let dim: Option<i32> =
            Spi::get_one("SELECT current_setting('pg_raggraph.embed_dim')::int").unwrap();
        assert_eq!(dim, Some(384));

        let extract_conc: Option<i32> =
            Spi::get_one("SELECT current_setting('pg_raggraph.extract_concurrency')::int").unwrap();
        assert_eq!(extract_conc, Some(4));
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
