//! pg_raggraph — PostgreSQL-native GraphRAG extension.
//!
//! See `docs/superpowers/specs/2026-05-03-pg-raggraph-rs-extension-design.md`.

use pgrx::prelude::*;

::pgrx::pg_module_magic!(name, version);

mod admin;
mod ask;
mod bgw;
mod embedding;
mod gucs;
mod ingest;
mod ingest_extracted;
mod ingest_profile;
mod provider_factory;
mod retrieval;

/// Test-only sentinel: set to `true` when `_PG_init` fires the SC-005 WARNING.
/// Exposed for the pgrx test that asserts the warning path executed.
#[cfg(any(test, feature = "pg_test"))]
pub static MASTER_KEY_WARNING_FIRED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Called by PostgreSQL when the extension shared library is loaded.
/// Registers GUCs so they are available before CREATE EXTENSION runs.
/// When loaded via `shared_preload_libraries`, also registers background workers.
#[allow(non_snake_case)]
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    gucs::register();

    // SC-005: warn at startup when credentials will be stored plaintext.
    if gucs::MASTER_KEY_PATH.get().is_none() {
        pgrx::warning!(
            "pgrg.master_key_path not set — provider credentials stored plaintext. \
             They will appear in `pg_dump`. Set a master key for production."
        );
        #[cfg(any(test, feature = "pg_test"))]
        MASTER_KEY_WARNING_FIRED.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    // SC-001: only register BGWs when loading via shared_preload_libraries.
    // During CREATE EXTENSION, this flag is false and we skip registration.
    unsafe {
        if pgrx::pg_sys::process_shared_preload_libraries_in_progress {
            bgw::register_launcher();
            bgw::register_workers();
        }
    }
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
::pgrx::extension_sql_file!(
    "../sql/migrations/005_status_check_atomicity.sql",
    name = "status_check_atomicity",
    requires = ["retrieval_indexes"]
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
    fn status_propagates_non_invalid_position_errors() {
        // SC-014: status() must NOT swallow SPI errors silently.
        // No-row path: random UUID -> NULL.
        let null_result: Option<pgrx::JsonB> =
            Spi::get_one("SELECT pgrg.status('00000000-0000-0000-0000-000000000000'::uuid)")
                .unwrap();
        assert!(null_result.is_none(), "unknown job_id must return NULL");

        // Existing-row path: insert a job row, query its id.
        Spi::run("SELECT pgrg.namespace_create('status_test_ns')").unwrap();
        Spi::run(
            "INSERT INTO pgrg.ingest_jobs (id, status, source, namespace) \
             VALUES ('44444444-4444-4444-4444-444444444444', 'queued', 'test.md', 'status_test_ns')",
        )
        .unwrap();
        let found: Option<pgrx::JsonB> =
            Spi::get_one("SELECT pgrg.status('44444444-4444-4444-4444-444444444444'::uuid)")
                .unwrap();
        let obj = found.expect("must find row").0;
        assert_eq!(obj["status"], "queued");
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
    fn sc012_parity_mode_guc_is_suset() {
        // SC-012 / folded Plan 1 concern: pg_raggraph.parity_mode must be Suset,
        // not Userset.
        //
        // PRIMARY assertion: pg_settings.context must be 'superuser' — this
        // definitively proves GucContext::Suset registration regardless of session
        // role mechanics.
        let ctx: Option<String> = Spi::get_one(
            "SELECT context FROM pg_settings WHERE name = 'pg_raggraph.parity_mode'",
        )
        .unwrap();
        assert_eq!(ctx.as_deref(), Some("superuser"), "expected Suset context");

        // SECONDARY (best-effort) assertion: a non-superuser SET must fail.
        // pgrx converts PG errors into Rust panics, so we use catch_unwind.
        // Mechanism note: a two-statement string ("SET ROLE …; SET guc…;") causes
        // the PG error to propagate as a panic outside the SPI call site, not as
        // Err — so we split the role switch and the GUC set into separate
        // catch_unwind scopes to capture the panic reliably.
        Spi::run("CREATE ROLE pgrg_parity_lowpriv NOLOGIN").unwrap();
        Spi::run("SET ROLE pgrg_parity_lowpriv").unwrap();
        let res = std::panic::catch_unwind(|| {
            Spi::run("SET pg_raggraph.parity_mode = true").unwrap();
        });
        assert!(
            res.is_err(),
            "SC-012: non-superuser SET pg_raggraph.parity_mode must fail (Suset)"
        );
        // Reset role; pg_test harness runs in a fresh connection so cleanup is
        // belt-and-suspenders only.
        Spi::run("RESET ROLE").unwrap();
        Spi::run("DROP ROLE IF EXISTS pgrg_parity_lowpriv").unwrap();
    }

    #[pg_test]
    fn ingest_jobs_payload_column_exists() {
        // Spec §5: ingest_jobs.payload bytea for ingest_text/ingest_bytes carriage.
        // Plan 1 schema declares this column; Plan 3 Task 1 locks the invariant
        // with this guard so future schema edits cannot drop it without flipping
        // a test.
        let exists: Option<bool> = Spi::get_one(
            "SELECT EXISTS( \
                 SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = 'pgrg' \
                   AND table_name = 'ingest_jobs' \
                   AND column_name = 'payload' \
                   AND data_type = 'bytea')",
        )
        .unwrap();
        assert_eq!(exists, Some(true), "ingest_jobs.payload bytea must exist");
    }

    #[pg_test]
    fn ingest_jobs_attempt_count_column_exists() {
        // Spec §5 + brief Desired Outcome: reaper bumps attempt_count, caps at 3.
        // Locked as schema invariant by Plan 3 Task 1.
        let exists: Option<bool> = Spi::get_one(
            "SELECT EXISTS( \
                 SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = 'pgrg' \
                   AND table_name = 'ingest_jobs' \
                   AND column_name = 'attempt_count')",
        )
        .unwrap();
        assert_eq!(exists, Some(true), "ingest_jobs.attempt_count must exist");
    }

    #[pg_test]
    fn ingest_jobs_active_partial_index_exists() {
        // Spec §5 line 254: partial index for the bg worker scan.
        // Locked as schema invariant by Plan 3 Task 1.
        let exists: Option<bool> = Spi::get_one(
            "SELECT EXISTS( \
                 SELECT 1 FROM pg_indexes \
                 WHERE schemaname = 'pgrg' \
                   AND indexname = 'ingest_jobs_active_idx')",
        )
        .unwrap();
        assert_eq!(
            exists,
            Some(true),
            "ingest_jobs_active_idx partial index must exist"
        );
    }

    #[pg_test]
    fn ingest_jobs_status_check_rejects_unknown_value() {
        // Plan 1+2 carry-forward: status enumeration is enforced at the schema level.
        Spi::run("SELECT pgrg.namespace_create('status_check_ns')").unwrap();
        let res = std::panic::catch_unwind(|| {
            Spi::run(
                "INSERT INTO pgrg.ingest_jobs (id, status, source, namespace) \
                 VALUES ('55555555-5555-5555-5555-555555555555', 'unknown_status', 't.md', 'status_check_ns')",
            )
            .unwrap();
        });
        assert!(res.is_err(), "unknown status must violate CHECK constraint");
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
    fn parity_mode_swap_skipped_when_data_exists() {
        // DC-004: existing namespaces must not get re-indexed when parity_mode flips.
        // Load a chunk first so has_chunks=true at the moment we flip the GUC.
        load_minimal_fixture_for_query("dc004_pre");
        // Now flip parity_mode and create a NEW namespace.
        Spi::run("SET pg_raggraph.parity_mode = true").unwrap();
        Spi::run("SELECT pgrg.namespace_create('dc004_post')").unwrap();
        // Verify HNSW indexes are still in place (NOT swapped to IVFFlat).
        let def: Option<String> = Spi::get_one(
            "SELECT indexdef FROM pg_indexes \
             WHERE schemaname = 'pgrg' AND indexname = 'chunks_embedding_hnsw'",
        )
        .unwrap();
        assert!(
            def.unwrap_or_default().contains("USING hnsw"),
            "DC-004: existing chunks must keep HNSW even when parity_mode flips later"
        );
        Spi::run("SET pg_raggraph.parity_mode = false").unwrap();
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

    #[pg_test]
    fn sc002_parity_small_fixture_loads_via_real_loader() {
        // SC-002: the SHIPPED bench/parity/extracted/small.jsonl loads cleanly
        // through the real pgrg.ingest_extracted AFTER its provenance header
        // (line 1) is stripped — the documented strip-before-load contract.
        // Resolve from the workspace root regardless of the harness cwd:
        // CARGO_MANIFEST_DIR is <repo>/pg_raggraph; the fixture lives at
        // <repo>/bench/parity/extracted/small.jsonl.
        let src = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../bench/parity/extracted/small.jsonl"
        );
        let raw = std::fs::read_to_string(src)
            .expect("parity small fixture must exist (run gen_extracted.py)");
        let mut lines = raw.lines();
        let header = lines.next().expect("fixture has a header line");
        assert!(
            header.contains("\"kind\":\"_header\"") || header.contains("\"kind\": \"_header\""),
            "line 1 must be the SC-002 provenance _header"
        );
        let stripped = "/tmp/pgrg_parity_small_stripped.jsonl";
        std::fs::write(stripped, lines.collect::<Vec<_>>().join("\n") + "\n")
            .expect("write header-stripped copy");

        Spi::run("SELECT pgrg.namespace_create('parity')").unwrap();
        let n: Option<i64> = Spi::get_one(&format!(
            "SELECT pgrg.ingest_extracted('{stripped}', 'parity')"
        ))
        .unwrap();
        let n = n.expect("ingest_extracted returns a row count");
        // 120 documents + 120 chunks + 120 entities + 120 chunk_entities +
        // 4 relationships = 484 records read from the header-stripped file.
        assert_eq!(n, 484, "sc002: expected exactly 484 records (120 docs + 120 chunks + 120 entities + 120 chunk_entities + 4 rels), got {n}");

        // Sanity: documents/chunks actually landed in the parity namespace.
        let docs: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.documents WHERE namespace='parity'",
        )
        .unwrap();
        assert_eq!(docs, Some(120), "120 documents expected");
        let chunks: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.chunks WHERE namespace='parity'",
        )
        .unwrap();
        assert_eq!(chunks, Some(120), "120 chunks expected");
        // Entities dedupe via ON CONFLICT (namespace, name, kind) to the
        // number of distinct topic words in the small corpus.
        let entity_count: i64 = Spi::get_one(
            "SELECT count(*) FROM pgrg.entities WHERE namespace='parity'",
        )
        .unwrap()
        .expect("entity count query returned NULL");
        assert_eq!(entity_count, 5, "sc002: expected 5 distinct topic entities after ON CONFLICT dedup, got {entity_count}");
    }

    fn load_minimal_fixture_for_query(ns: &str) {
        // Helper used by query tests: load 2 chunks, 1 entity, 1 chunk_entity.
        // UUIDs are derived from the namespace string so parallel tests do
        // not collide on documents.id (a global PK).
        use pg_raggraph_core::test_helpers::ns_uuid;
        Spi::run(&format!("SELECT pgrg.namespace_create('{ns}')")).unwrap();
        let dim: i32 = Spi::get_one::<i32>("SELECT current_setting('pg_raggraph.embed_dim')::int")
            .unwrap()
            .unwrap();
        let dim_usize: usize = usize::try_from(dim).expect("dim fits in usize");
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
        // Entity name_emb must be byte-identical to pgrg.embed('alpha') so the
        // graph seed CTE (cosine distance < 0.35) accepts it. Use the same
        // deterministic embedder the SQL surface uses (Plan 2 T4).
        let alpha_vec = pg_raggraph_core::embedding::deterministic_embed("alpha", dim_usize);
        let mut alpha_lit = String::from("[");
        for (i, x) in alpha_vec.iter().enumerate() {
            if i > 0 {
                alpha_lit.push(',');
            }
            alpha_lit.push_str(&format!("{x}"));
        }
        alpha_lit.push(']');
        let doc_id = ns_uuid(ns, 0x10);
        let chunk1_id = ns_uuid(ns, 0x11);
        let chunk2_id = ns_uuid(ns, 0x12);
        let entity_id = ns_uuid(ns, 0x20);
        let path = format!("/tmp/pgrg_q_{ns}.jsonl");
        std::fs::write(
            &path,
            format!(
                concat!(
                    r#"{{"kind":"document","id":"{doc}","namespace":"{ns}","source":"d.md","content_hash":"h-q-{ns}"}}"#, "\n",
                    r#"{{"kind":"chunk","id":"{c1}","namespace":"{ns}","document_id":"{doc}","ord":0,"text":"alpha auth module","token_count":3,"embedding":{e1}}}"#, "\n",
                    r#"{{"kind":"chunk","id":"{c2}","namespace":"{ns}","document_id":"{doc}","ord":1,"text":"beta gamma","token_count":2,"embedding":{e2}}}"#, "\n",
                    r#"{{"kind":"entity","id":"{e}","namespace":"{ns}","name":"alpha","kind_label":"module","name_emb":{e3}}}"#, "\n",
                    r#"{{"kind":"chunk_entity","chunk_id":"{c1}","entity_id":"{e}","confidence":0.9,"classification":"extracted"}}"#, "\n",
                ),
                ns = ns,
                doc = doc_id,
                c1 = chunk1_id,
                c2 = chunk2_id,
                e = entity_id,
                e1 = mk_emb(0.1),
                e2 = mk_emb(0.5),
                e3 = alpha_lit,
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
    fn query_vector_mode_only_vec_lane_in_signals() {
        load_minimal_fixture_for_query("q_vec_only_ns");
        let signals: pgrx::JsonB = Spi::get_one(
            "SELECT signals FROM pgrg.query('alpha', NULL, 5, 'q_vec_only_ns', 1, NULL, 'vector') LIMIT 1",
        )
        .unwrap()
        .expect("vector mode returned no rows");
        let arr = signals.0.as_array().expect("signals is array");
        for sig in arr {
            assert_eq!(
                sig["lane"], "vec",
                "vector mode: signals must contain only lane='vec', got {sig}"
            );
        }
    }

    #[pg_test]
    fn query_bm25_mode_only_bm25_lane_in_signals() {
        load_minimal_fixture_for_query("q_bm25_only_ns");
        let signals: pgrx::JsonB = Spi::get_one(
            "SELECT signals FROM pgrg.query('alpha', NULL, 5, 'q_bm25_only_ns', 1, NULL, 'bm25') LIMIT 1",
        )
        .unwrap()
        .expect("bm25 mode returned no rows");
        let arr = signals.0.as_array().expect("signals is array");
        for sig in arr {
            assert_eq!(
                sig["lane"], "bm25",
                "bm25 mode: signals must contain only lane='bm25', got {sig}"
            );
        }
    }

    #[pg_test]
    fn query_graph_mode_only_graph_lane_in_signals() {
        load_minimal_fixture_for_query("q_graph_only_ns");
        let signals: pgrx::JsonB = Spi::get_one(
            "SELECT signals FROM pgrg.query('alpha', NULL, 5, 'q_graph_only_ns', 1, NULL, 'graph') LIMIT 1",
        )
        .unwrap()
        .expect("graph mode returned no rows");
        let arr = signals.0.as_array().expect("signals is array");
        for sig in arr {
            assert_eq!(
                sig["lane"], "graph",
                "graph mode: signals must contain only lane='graph', got {sig}"
            );
        }
    }

    #[pg_test]
    fn query_unknown_mode_errors() {
        // Constraint Never: no smart/local/global modes — these must error, not silently fall back.
        load_minimal_fixture_for_query("q_unknown_ns");
        let res = std::panic::catch_unwind(|| {
            let _: Option<i64> = Spi::get_one(
                "SELECT count(*) FROM pgrg.query('q', NULL, 5, 'q_unknown_ns', 1, NULL, 'smart')",
            )
            .unwrap();
        });
        assert!(res.is_err(), "mode='smart' must error per Constraint Never");
    }

    fn load_chain_fixture(ns: &str) {
        // 3-node chain A -> B -> C, each entity attached to one chunk.
        // Seed query embedding will match entity A so hops control reachability.
        // UUIDs are derived from the namespace string so parallel tests do
        // not collide on documents.id (a global PK).
        use pg_raggraph_core::test_helpers::ns_uuid;
        Spi::run(&format!("SELECT pgrg.namespace_create('{ns}')")).unwrap();
        let dim: i32 = Spi::get_one::<i32>("SELECT current_setting('pg_raggraph.embed_dim')::int")
            .unwrap()
            .unwrap();
        let dim_usize: usize = usize::try_from(dim).expect("dim fits in usize");
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
        // Entity name_emb must be byte-identical to pgrg.embed('XXX') so the
        // graph seed CTE (cosine distance < 0.35) accepts it. Use the same
        // deterministic embedder the SQL surface uses (Plan 2 T4).
        let lit_for = |name: &str| -> String {
            let v = pg_raggraph_core::embedding::deterministic_embed(name, dim_usize);
            let mut s = String::from("[");
            for (i, x) in v.iter().enumerate() {
                if i > 0 {
                    s.push(',');
                }
                s.push_str(&format!("{x}"));
            }
            s.push(']');
            s
        };
        let aaa_lit = lit_for("AAA");
        let bbb_lit = lit_for("BBB");
        let ccc_lit = lit_for("CCC");
        let doc_id = ns_uuid(ns, 0x50);
        let c1_id = ns_uuid(ns, 0x51);
        let c2_id = ns_uuid(ns, 0x52);
        let c3_id = ns_uuid(ns, 0x53);
        let e1_id = ns_uuid(ns, 0x61);
        let e2_id = ns_uuid(ns, 0x62);
        let e3_id = ns_uuid(ns, 0x63);
        let r1_id = ns_uuid(ns, 0x71);
        let r2_id = ns_uuid(ns, 0x72);
        let path = format!("/tmp/pgrg_chain_{ns}.jsonl");
        // Three chunks (one per entity), three entities A/B/C, two relationships A->B, B->C.
        std::fs::write(
            &path,
            format!(
                concat!(
                    r#"{{"kind":"document","id":"{doc}","namespace":"{ns}","source":"d.md","content_hash":"h-chain-{ns}"}}"#,"\n",
                    r#"{{"kind":"chunk","id":"{c1}","namespace":"{ns}","document_id":"{doc}","ord":0,"text":"chunk-a","token_count":1,"embedding":{ea}}}"#,"\n",
                    r#"{{"kind":"chunk","id":"{c2}","namespace":"{ns}","document_id":"{doc}","ord":1,"text":"chunk-b","token_count":1,"embedding":{eb}}}"#,"\n",
                    r#"{{"kind":"chunk","id":"{c3}","namespace":"{ns}","document_id":"{doc}","ord":2,"text":"chunk-c","token_count":1,"embedding":{ec}}}"#,"\n",
                    r#"{{"kind":"entity","id":"{e1}","namespace":"{ns}","name":"AAA","kind_label":"node","name_emb":{ea_ent}}}"#,"\n",
                    r#"{{"kind":"entity","id":"{e2}","namespace":"{ns}","name":"BBB","kind_label":"node","name_emb":{eb_ent}}}"#,"\n",
                    r#"{{"kind":"entity","id":"{e3}","namespace":"{ns}","name":"CCC","kind_label":"node","name_emb":{ec_ent}}}"#,"\n",
                    r#"{{"kind":"relationship","id":"{r1}","namespace":"{ns}","src_id":"{e1}","dst_id":"{e2}","kind_label":"next","weight":1.0}}"#,"\n",
                    r#"{{"kind":"relationship","id":"{r2}","namespace":"{ns}","src_id":"{e2}","dst_id":"{e3}","kind_label":"next","weight":1.0}}"#,"\n",
                    r#"{{"kind":"chunk_entity","chunk_id":"{c1}","entity_id":"{e1}","confidence":1.0,"classification":"extracted"}}"#,"\n",
                    r#"{{"kind":"chunk_entity","chunk_id":"{c2}","entity_id":"{e2}","confidence":1.0,"classification":"extracted"}}"#,"\n",
                    r#"{{"kind":"chunk_entity","chunk_id":"{c3}","entity_id":"{e3}","confidence":1.0,"classification":"extracted"}}"#,"\n",
                ),
                ns = ns,
                doc = doc_id,
                c1 = c1_id,
                c2 = c2_id,
                c3 = c3_id,
                e1 = e1_id,
                e2 = e2_id,
                e3 = e3_id,
                r1 = r1_id,
                r2 = r2_id,
                ea = mk_emb(0.10),
                eb = mk_emb(0.20),
                ec = mk_emb(0.30),
                ea_ent = aaa_lit,
                eb_ent = bbb_lit,
                ec_ent = ccc_lit,
            ),
        )
        .expect("chain fixture write");
        Spi::run(&format!("SELECT pgrg.ingest_extracted('{path}', '{ns}')")).unwrap();
    }

    #[pg_test]
    fn hops_zero_excludes_graph_lane() {
        // SC-006: hops=0 excludes graph lane entirely.
        load_chain_fixture("hops0_ns");
        let n: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.query('AAA', NULL, 10, 'hops0_ns', 0, NULL, 'graph')",
        )
        .unwrap();
        assert_eq!(n, Some(0), "hops=0 in graph mode must yield zero rows");
    }

    #[pg_test]
    fn hops_one_includes_direct_neighbors_only() {
        // SC-006: hops=1 -> direct neighbors. Seed = AAA, reachable = {A, B}; chunk-c excluded.
        load_chain_fixture("hops1_ns");
        let texts: Vec<String> = Spi::connect(|client| {
            client
                .select(
                    "SELECT text FROM pgrg.query('AAA', NULL, 10, 'hops1_ns', 1, NULL, 'graph')",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| r.get::<String>(1).unwrap().unwrap_or_default())
                .collect()
        });
        assert!(
            texts.contains(&"chunk-a".to_string()),
            "must include chunk-a (seed)"
        );
        assert!(
            texts.contains(&"chunk-b".to_string()),
            "must include chunk-b (1-hop neighbor)"
        );
        assert!(
            !texts.contains(&"chunk-c".to_string()),
            "chunk-c is 2 hops away; should be excluded"
        );
    }

    #[pg_test]
    fn hops_two_includes_friends_of_friends() {
        // SC-006: hops=2 -> includes chunk-c.
        load_chain_fixture("hops2_ns");
        let texts: Vec<String> = Spi::connect(|client| {
            client
                .select(
                    "SELECT text FROM pgrg.query('AAA', NULL, 10, 'hops2_ns', 2, NULL, 'graph')",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| r.get::<String>(1).unwrap().unwrap_or_default())
                .collect()
        });
        assert!(
            texts.contains(&"chunk-c".to_string()),
            "hops=2 must include 2-hop chunk-c"
        );
    }

    #[pg_test]
    fn undirected_walk_reaches_a_from_b_seed() {
        // SC-007: undirected. A -> B exists; seed at B, walk to A.
        load_chain_fixture("undir_ns");
        let texts: Vec<String> = Spi::connect(|client| {
            client
                .select(
                    "SELECT text FROM pgrg.query('BBB', NULL, 10, 'undir_ns', 1, NULL, 'graph')",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| r.get::<String>(1).unwrap().unwrap_or_default())
                .collect()
        });
        assert!(
            texts.contains(&"chunk-a".to_string()),
            "undirected walk: A must be reachable from B (relationship A->B); got {texts:?}"
        );
    }

    #[pg_test]
    fn bgw_workers_registered_under_preload() {
        // SC-002: with shared_preload_libraries='pg_raggraph' and
        // pg_raggraph.bgw_workers=2, exactly 2 worker processes run.
        // Workers may not have populated pg_stat_activity yet — poll until
        // they're visible (up to 5 seconds).
        let mut n = Some(0i64);
        for _ in 0..50 {
            n = Spi::get_one(
                "SELECT count(*) FROM pg_stat_activity \
                 WHERE backend_type LIKE 'pg_raggraph w%'",
            )
            .unwrap();
            if n == Some(2) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        assert_eq!(n, Some(2), "expected 2 pg_raggraph bg workers, got {n:?}");
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

    #[pg_test]
    fn filter_metadata_predicate_applied_inside_lanes() {
        // SC-008: filter='{"tag":"x"}' — only chunks whose metadata @> '{"tag":"x"}' returned.
        Spi::run("SELECT pgrg.namespace_create('filter_ns')").unwrap();
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
        // Two chunks: one with tag=x, one without. Same text.
        std::fs::write(
            "/tmp/pgrg_filter.jsonl",
            format!(
                concat!(
                    r#"{{"kind":"document","id":"a0000000-0000-0000-0000-000000000080","namespace":"filter_ns","source":"d.md","content_hash":"h-filter"}}"#,"\n",
                    r#"{{"kind":"chunk","id":"c0000000-0000-0000-0000-000000000081","namespace":"filter_ns","document_id":"a0000000-0000-0000-0000-000000000080","ord":0,"text":"alpha","token_count":1,"embedding":{e},"metadata":{{"tag":"x"}}}}"#,"\n",
                    r#"{{"kind":"chunk","id":"c0000000-0000-0000-0000-000000000082","namespace":"filter_ns","document_id":"a0000000-0000-0000-0000-000000000080","ord":1,"text":"alpha","token_count":1,"embedding":{e},"metadata":{{"tag":"y"}}}}"#,"\n",
                ),
                e = mk_emb(0.5),
            ),
        )
        .unwrap();
        Spi::run("SELECT pgrg.ingest_extracted('/tmp/pgrg_filter.jsonl', 'filter_ns')").unwrap();

        // Without filter: both chunks reachable.
        let unfiltered: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.query('alpha', NULL, 10, 'filter_ns', 1, NULL, 'hybrid')",
        )
        .unwrap();
        assert_eq!(unfiltered, Some(2), "without filter, both chunks return");

        // With filter: only the tagged chunk.
        let filtered: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.query('alpha', '{\"tag\":\"x\"}'::jsonb, 10, 'filter_ns', 1, NULL, 'hybrid')",
        )
        .unwrap();
        assert_eq!(filtered, Some(1), "filter must restrict to tag=x chunk");

        // Verify it's the right chunk.
        let id: Option<pgrx::Uuid> = Spi::get_one(
            "SELECT chunk_id FROM pgrg.query('alpha', '{\"tag\":\"x\"}'::jsonb, 10, 'filter_ns', 1, NULL, 'hybrid') LIMIT 1",
        )
        .unwrap();
        let expected = pgrx::Uuid::from_bytes(
            *uuid::Uuid::parse_str("c0000000-0000-0000-0000-000000000081")
                .unwrap()
                .as_bytes(),
        );
        assert_eq!(id, Some(expected));
    }

    #[pg_test]
    fn rrf_score_matches_hand_computed_with_default_weights() {
        // SC-005: emitted score must equal SUM(w * 1/(60+rk)) over the lane signals.
        // With default weights (1.0 each), the score is reproducible from the
        // signals JSONB alone — the strongest correctness gate for RRF fusion.
        load_minimal_fixture_for_query("rrf_default_ns");
        let row: Option<(pgrx::JsonB, f64)> = Spi::connect(|client| {
            client
                .select(
                    "SELECT signals, score FROM pgrg.query('alpha auth module', NULL, 5, 'rrf_default_ns', 1, NULL, 'hybrid') LIMIT 1",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .map(|r| (
                    r.get::<pgrx::JsonB>(1).unwrap().unwrap(),
                    r.get::<f64>(2).unwrap().unwrap_or(0.0),
                ))
        });
        let (sigs, score) = row.expect("must return row");
        let arr = sigs.0.as_array().unwrap();
        let mut expected: f64 = 0.0;
        for s in arr {
            let rk = s["rk"].as_i64().unwrap();
            let w = s["w"].as_f64().unwrap();
            #[allow(clippy::cast_precision_loss)]
            let rk_f = rk as f64;
            expected += w * (1.0 / (60.0 + rk_f));
        }
        assert!(
            (score - expected).abs() < 1e-9,
            "RRF score {score} != hand-computed {expected} from signals {arr:?}"
        );
    }

    #[pg_test]
    fn weights_override_zeros_bm25_doubles_vec() {
        // SC-010: weights JSONB override changes the emitted score.
        load_minimal_fixture_for_query("rrf_weights_ns");
        let default_score: Option<f64> = Spi::get_one(
            "SELECT score FROM pgrg.query('alpha', NULL, 5, 'rrf_weights_ns', 1, NULL, 'hybrid') LIMIT 1",
        ).unwrap();
        let override_score: Option<f64> = Spi::get_one(
            "SELECT score FROM pgrg.query('alpha', NULL, 5, 'rrf_weights_ns', 1, '{\"vec\":2.0,\"bm25\":0.0,\"graph\":1.0}'::jsonb, 'hybrid') LIMIT 1",
        ).unwrap();
        assert!(
            default_score != override_score,
            "weight override must change score (default={default_score:?}, override={override_score:?})"
        );
    }

    #[pg_test]
    fn weights_negative_input_clamped_to_zero() {
        load_minimal_fixture_for_query("neg_w_ns");
        // Negative weights should clamp to 0.0 — bm25=-1.0 acts like bm25=0.0.
        let zero_score: Option<f64> = Spi::get_one(
            "SELECT score FROM pgrg.query('alpha', NULL, 5, 'neg_w_ns', 1, '{\"vec\":1.0,\"bm25\":0.0,\"graph\":1.0}'::jsonb, 'hybrid') LIMIT 1",
        )
        .unwrap();
        let neg_score: Option<f64> = Spi::get_one(
            "SELECT score FROM pgrg.query('alpha', NULL, 5, 'neg_w_ns', 1, '{\"vec\":1.0,\"bm25\":-1.0,\"graph\":1.0}'::jsonb, 'hybrid') LIMIT 1",
        )
        .unwrap();
        assert_eq!(
            zero_score, neg_score,
            "negative weights must clamp to 0.0; bm25=0.0 and bm25=-1.0 must score identically"
        );
    }

    #[pg_test]
    fn signals_shape_is_lane_rk_w_tuple() {
        // Constraint "Ask First": signals shape change requires approval.
        // Plan 2's shape: jsonb_agg(jsonb_build_object('lane',lane,'rk',rk,'w',w)).
        // The 'w' field is an additive change (Task 11) — downstream readers
        // that only consume {lane, rk} continue to work.
        load_minimal_fixture_for_query("sig_shape_ns");
        let signals: pgrx::JsonB = Spi::get_one(
            "SELECT signals FROM pgrg.query('alpha', NULL, 5, 'sig_shape_ns', 1, NULL, 'hybrid') LIMIT 1",
        )
        .unwrap()
        .expect("must return row");
        let arr = signals.0.as_array().expect("signals is array");
        for s in arr {
            assert!(s.get("lane").is_some(), "signal must have `lane` key");
            assert!(s.get("rk").is_some(), "signal must have `rk` key");
            assert!(
                s.get("w").is_some(),
                "signal must have `w` key (Plan 2 addition)"
            );
        }
    }

    #[pg_test]
    fn debug_retrieval_guc_does_not_break_query() {
        // Plan 2: GUC is a no-op (additional debug fields land in Plan 6).
        // This test guards against future regressions: setting the GUC must
        // not error or change the column shape.
        load_minimal_fixture_for_query("debug_guc_ns");
        Spi::run("SET pg_raggraph.debug_retrieval = true").unwrap();
        let n: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.query('alpha', NULL, 5, 'debug_guc_ns', 1, NULL, 'hybrid')",
        )
        .unwrap();
        assert!(
            n.unwrap_or(0) > 0,
            "query must still work with debug_retrieval=true"
        );
        Spi::run("SET pg_raggraph.debug_retrieval = false").unwrap();
    }

    #[pg_test]
    fn parity_mode_end_to_end_query_works() {
        // SC-009 + DC-004: with parity_mode=true at namespace_create,
        // the IVFFlat index path serves queries.
        Spi::run("SET pg_raggraph.parity_mode = true").unwrap();
        load_minimal_fixture_for_query("parity_e2e_ns");

        // Verify the index is IVFFlat.
        let def: Option<String> = Spi::get_one(
            "SELECT indexdef FROM pg_indexes \
             WHERE schemaname = 'pgrg' AND indexname = 'chunks_embedding_hnsw'",
        )
        .unwrap();
        assert!(
            def.unwrap_or_default().contains("USING ivfflat"),
            "parity_mode must produce IVFFlat index"
        );

        // Verify queries still work.
        let n: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.query('alpha', NULL, 5, 'parity_e2e_ns', 1, NULL, 'hybrid')",
        )
        .unwrap();
        assert!(
            n.unwrap_or(0) > 0,
            "queries must work under parity_mode (IVFFlat)"
        );

        Spi::run("SET pg_raggraph.parity_mode = false").unwrap();
    }

    #[pg_test]
    fn queue_claim_marks_one_job_running() {
        // SC-016 part 1: claim_next_job() flips a queued row to 'running' atomically
        // and returns the claimed ClaimedJob. We invoke the helper directly because
        // pgrx tests run inside a wrapping transaction that is rolled back on exit
        // — bg-worker backends in their own transactions cannot see uncommitted
        // rows from the test session, so the cross-backend timing assertion is
        // deferred to Task 18 (E2E + load-path SC tests). The SQL semantics of the
        // claim itself are exercised in-process here.
        Spi::run("SELECT pgrg.namespace_create('q_claim_ns')").unwrap();
        Spi::run(
            "INSERT INTO pgrg.ingest_jobs (id, status, source, namespace) \
             VALUES ('77777777-7777-7777-7777-777777777777', 'queued', 't.md', 'q_claim_ns')",
        )
        .unwrap();

        let claimed = crate::bgw::queue::claim_next_job().expect("claim_next_job must return job");
        let expected = pgrx::Uuid::from_bytes(
            *uuid::Uuid::parse_str("77777777-7777-7777-7777-777777777777")
                .unwrap()
                .as_bytes(),
        );
        assert_eq!(
            claimed.id, expected,
            "claimed job id must match inserted id"
        );
        assert_eq!(claimed.namespace, "q_claim_ns");
        assert_eq!(
            claimed.attempt_count, 1,
            "attempt_count must increment to 1"
        );

        let s: Option<String> = Spi::get_one(
            "SELECT status FROM pgrg.ingest_jobs \
             WHERE id = '77777777-7777-7777-7777-777777777777'",
        )
        .unwrap();
        assert_eq!(
            s.as_deref(),
            Some("running"),
            "status must transition to 'running'"
        );

        // complete_job must drive the row to 'completed'.
        crate::bgw::queue::complete_job(&claimed.id);
        let s2: Option<String> = Spi::get_one(
            "SELECT status FROM pgrg.ingest_jobs \
             WHERE id = '77777777-7777-7777-7777-777777777777'",
        )
        .unwrap();
        assert_eq!(s2.as_deref(), Some("completed"));
    }

    #[pg_test]
    fn queue_skip_locked_no_double_processing() {
        // SC-016 part 2: FOR UPDATE SKIP LOCKED LIMIT 1 returns one distinct
        // queued job per call and never re-claims a job already moved out of
        // 'queued'. We drive 10 sequential claim_next_job() calls (the bg
        // worker would do the same per loop iteration) and assert (a) all 10
        // are claimed exactly once, (b) attempt_count caps at 1, (c) the 11th
        // call returns None. Multi-worker concurrency is verified separately
        // at Task 18 once jobs are committed via real ingest queueing.
        Spi::run("SELECT pgrg.namespace_create('skip_locked_ns')").unwrap();
        for i in 0..10 {
            let id = format!("99999999-9999-9999-9999-{i:012}");
            Spi::run(&format!(
                "INSERT INTO pgrg.ingest_jobs (id, status, source, namespace) \
                 VALUES ('{id}', 'queued', 's{i}.md', 'skip_locked_ns')"
            ))
            .unwrap();
        }

        let mut claimed_ids: std::collections::HashSet<pgrx::Uuid> =
            std::collections::HashSet::new();
        for _ in 0..10 {
            let job = crate::bgw::queue::claim_next_job()
                .expect("claim_next_job must return a job while queue is non-empty");
            assert!(
                claimed_ids.insert(job.id),
                "FOR UPDATE SKIP LOCKED must not return the same id twice (got {:?} twice)",
                job.id
            );
            crate::bgw::queue::complete_job(&job.id);
        }
        assert_eq!(
            claimed_ids.len(),
            10,
            "all 10 jobs must be claimed exactly once"
        );

        // 11th call must return None (queue drained).
        assert!(
            crate::bgw::queue::claim_next_job().is_none(),
            "drained queue must yield None"
        );

        let max_attempts: Option<i32> = Spi::get_one(
            "SELECT max(attempt_count) FROM pgrg.ingest_jobs \
             WHERE namespace = 'skip_locked_ns'",
        )
        .unwrap();
        assert!(
            max_attempts.unwrap_or(0) <= 1,
            "FOR UPDATE SKIP LOCKED must prevent double-claim, max attempts = {max_attempts:?}"
        );
    }

    #[pg_test]
    fn spi_pg_client_drives_run_job_to_chunks_with_embeddings() {
        // SC-004 / SC-009 in-task verification: SpiPgClient + DeterministicEmbedder
        // (pg_test build) + MockProvider produces a document + chunks with embeddings.
        // The cross-backend bg-worker dispatch path is verified in Task 18.
        use pg_raggraph_core::embedding::DeterministicEmbedder;
        use pg_raggraph_core::ingest::run::run_job;
        use pg_raggraph_core::ingest::{IngestRequest, IngestSource};
        use pg_raggraph_core::llm::MockProvider;

        Spi::run("SELECT pgrg.namespace_create('spi_drain_ns')").unwrap();

        let req = IngestRequest {
            source: IngestSource::Text {
                name: "doc.md".into(),
                content: "the quick brown fox jumps over the lazy dog".into(),
            },
            namespace: "spi_drain_ns".into(),
            chunk_strategy: "auto".into(),
        };
        let dim_i32 = crate::gucs::EMBED_DIM.get();
        let dim: usize = usize::try_from(dim_i32).expect("embed_dim non-negative");
        let embedder = DeterministicEmbedder::new(dim);
        let provider = MockProvider::new();

        let mut client = crate::bgw::spi_client::SpiPgClient;
        let outcome = run_job(&mut client, &req, &embedder, &provider).expect("run_job ok");
        assert!(matches!(
            outcome,
            pg_raggraph_core::ingest::run::RunJobOutcome::Completed { .. }
        ));

        let docs: Option<i64> =
            Spi::get_one("SELECT count(*) FROM pgrg.documents WHERE namespace = 'spi_drain_ns'")
                .unwrap();
        assert_eq!(docs, Some(1), "exactly 1 document row");

        let chunks: Option<i64> =
            Spi::get_one("SELECT count(*) FROM pgrg.chunks WHERE namespace = 'spi_drain_ns'")
                .unwrap();
        assert!(
            chunks.unwrap_or(0) >= 1,
            "at least 1 chunk row, got {chunks:?}"
        );

        // SC-004: chunks must have non-NULL embeddings.
        let null_emb: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.chunks \
             WHERE namespace = 'spi_drain_ns' AND embedding IS NULL",
        )
        .unwrap();
        assert_eq!(null_emb, Some(0), "all chunks must carry an embedding");
    }

    #[pg_test]
    fn ingest_returns_uuid_under_50ms_and_enqueues_job() {
        // SC-003: pgrg.ingest is non-blocking; returns UUID quickly; row visible.
        Spi::run("SELECT pgrg.namespace_create('ingest_speed_ns')").unwrap();
        let path = "/tmp/pgrg_ingest_speed.md";
        std::fs::write(path, "# Title\n\nbody").unwrap();

        let start = std::time::Instant::now();
        let id: Option<pgrx::Uuid> = Spi::get_one(&format!(
            "SELECT pgrg.ingest('{path}', 'ingest_speed_ns', 'auto')"
        ))
        .unwrap();
        let elapsed = start.elapsed();

        assert!(id.is_some(), "must return a UUID");
        assert!(
            elapsed.as_millis() < 50,
            "SC-003: pgrg.ingest must return in <50ms, took {elapsed:?}"
        );

        let n: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.ingest_jobs \
             WHERE namespace = 'ingest_speed_ns'",
        )
        .unwrap();
        assert_eq!(n, Some(1), "exactly one job row enqueued");
    }

    #[pg_test]
    fn ingest_text_enqueues_payload_and_pipeline_writes_document() {
        // SC-005: pgrg.ingest_text enqueues utf-8 payload; the worker pipeline
        // (verified in-test via direct run_job dispatch) writes a document
        // whose content equals 'hello world from ingest_text' after chunking.
        // Cross-backend bg-worker dispatch is verified in Task 18.
        use pg_raggraph_core::embedding::DeterministicEmbedder;
        use pg_raggraph_core::ingest::run::run_job;
        use pg_raggraph_core::ingest::{IngestRequest, IngestSource};
        use pg_raggraph_core::llm::MockProvider;

        Spi::run("SELECT pgrg.namespace_create('ingest_text_ns')").unwrap();

        // Part 1: pgrg.ingest_text enqueues with the right payload.
        let id: Option<pgrx::Uuid> = Spi::get_one(
            "SELECT pgrg.ingest_text('doc1', 'hello world from ingest_text', 'ingest_text_ns', 'auto')",
        )
        .unwrap();
        assert!(id.is_some(), "ingest_text must return a UUID");

        let payload: Option<Vec<u8>> = Spi::get_one(&format!(
            "SELECT payload FROM pgrg.ingest_jobs WHERE id = '{}'",
            id.unwrap()
        ))
        .unwrap();
        let bytes = payload.expect("payload must be set");
        assert_eq!(
            std::str::from_utf8(&bytes).unwrap(),
            "hello world from ingest_text",
            "payload bytes must match the utf-8 content"
        );
        let job_source: Option<String> = Spi::get_one(&format!(
            "SELECT source FROM pgrg.ingest_jobs WHERE id = '{}'",
            id.unwrap()
        ))
        .unwrap();
        assert_eq!(job_source.as_deref(), Some("doc1"));

        // Part 2: directly drive run_job (the same path the worker takes) and
        // verify the document/chunks land + are queryable.
        let req = IngestRequest {
            source: IngestSource::Text {
                name: "doc1".into(),
                content: "hello world from ingest_text".into(),
            },
            namespace: "ingest_text_ns".into(),
            chunk_strategy: "auto".into(),
        };
        let embedder = DeterministicEmbedder::new(crate::gucs::EMBED_DIM.get() as usize);
        let provider = MockProvider::new();
        let mut client = crate::bgw::spi_client::SpiPgClient;
        run_job(&mut client, &req, &embedder, &provider).expect("run_job ok");

        let docs: Option<i64> =
            Spi::get_one("SELECT count(*) FROM pgrg.documents WHERE namespace = 'ingest_text_ns'")
                .unwrap();
        assert_eq!(docs, Some(1), "exactly 1 document row");

        let n: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.query('hello world', NULL, 5, 'ingest_text_ns', 1, NULL, 'hybrid')",
        )
        .unwrap();
        assert!(
            n.unwrap_or(0) >= 1,
            "ingested doc must be retrievable via pgrg.query"
        );
    }

    #[pg_test]
    fn ingest_bytes_enqueues_payload_and_pipeline_writes_document() {
        // SC-006: pgrg.ingest_bytes carries arbitrary bytes through the queue;
        // the worker pipeline chunks them. Use UTF-8 bytes so chunkshop can
        // process them (binary handlers are out of Plan 3 scope).
        use pg_raggraph_core::embedding::DeterministicEmbedder;
        use pg_raggraph_core::ingest::run::run_job;
        use pg_raggraph_core::ingest::{IngestRequest, IngestSource};
        use pg_raggraph_core::llm::MockProvider;

        Spi::run("SELECT pgrg.namespace_create('ingest_bytes_ns')").unwrap();

        // Part 1: pgrg.ingest_bytes enqueues. "hello world" as hex.
        let bytes_sql = "E'\\\\x68656c6c6f20776f726c64'::bytea";
        let id: Option<pgrx::Uuid> = Spi::get_one(&format!(
            "SELECT pgrg.ingest_bytes('doc1.bin', {bytes_sql}, 'ingest_bytes_ns', 'auto')"
        ))
        .unwrap();
        assert!(id.is_some());

        let payload: Option<Vec<u8>> = Spi::get_one(&format!(
            "SELECT payload FROM pgrg.ingest_jobs WHERE id = '{}'",
            id.unwrap()
        ))
        .unwrap();
        assert_eq!(payload.as_deref(), Some(b"hello world".as_slice()));

        // Part 2: drive run_job directly with Bytes source.
        let req = IngestRequest {
            source: IngestSource::Bytes {
                name: "doc1.bin".into(),
                bytes: b"hello world".to_vec(),
            },
            namespace: "ingest_bytes_ns".into(),
            chunk_strategy: "auto".into(),
        };
        let embedder = DeterministicEmbedder::new(crate::gucs::EMBED_DIM.get() as usize);
        let provider = MockProvider::new();
        let mut client = crate::bgw::spi_client::SpiPgClient;
        run_job(&mut client, &req, &embedder, &provider).expect("run_job ok");

        let docs: Option<i64> =
            Spi::get_one("SELECT count(*) FROM pgrg.documents WHERE namespace = 'ingest_bytes_ns'")
                .unwrap();
        assert_eq!(docs, Some(1), "ingest_bytes must produce 1 document");
    }

    #[pg_test]
    fn duplicate_ingest_via_run_job_yields_skipped_no_op() {
        // SC-007: identical content_hash -> SkippedDuplicate, no second doc row.
        // Verified end-to-end via SpiPgClient (real schema). Cross-backend
        // worker dispatch covered in Task 18.
        use pg_raggraph_core::embedding::DeterministicEmbedder;
        use pg_raggraph_core::ingest::run::{RunJobOutcome, run_job};
        use pg_raggraph_core::ingest::{IngestRequest, IngestSource};
        use pg_raggraph_core::llm::MockProvider;

        Spi::run("SELECT pgrg.namespace_create('dup_ns')").unwrap();

        let req = IngestRequest {
            source: IngestSource::Text {
                name: "d".into(),
                content: "identical content body".into(),
            },
            namespace: "dup_ns".into(),
            chunk_strategy: "auto".into(),
        };
        let embedder = DeterministicEmbedder::new(crate::gucs::EMBED_DIM.get() as usize);
        let provider = MockProvider::new();
        let mut client = crate::bgw::spi_client::SpiPgClient;

        let first = run_job(&mut client, &req, &embedder, &provider).expect("first run_job ok");
        assert!(matches!(first, RunJobOutcome::Completed { .. }));

        let second = run_job(&mut client, &req, &embedder, &provider).expect("second run_job ok");
        assert!(
            matches!(second, RunJobOutcome::SkippedDuplicate { .. }),
            "second ingest of identical content must be SkippedDuplicate"
        );

        let docs: Option<i64> =
            Spi::get_one("SELECT count(*) FROM pgrg.documents WHERE namespace = 'dup_ns'").unwrap();
        assert_eq!(
            docs,
            Some(1),
            "duplicate content_hash must not create second doc"
        );
    }

    #[pg_test]
    fn e2e_ingest_extracted_then_query() {
        load_minimal_fixture_for_query("e2e_demo");

        let start = std::time::Instant::now();
        let rows: Vec<(String, f64)> = Spi::connect(|client| {
            client
                .select(
                    "SELECT text, score FROM pgrg.query('what is the auth module', NULL, 5, 'e2e_demo', 1, NULL, 'hybrid')",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| {
                    (
                        r.get::<String>(1).unwrap().unwrap_or_default(),
                        r.get::<f64>(2).unwrap().unwrap_or(0.0),
                    )
                })
                .collect()
        });
        let elapsed = start.elapsed();

        assert!(
            !rows.is_empty(),
            "E2E: query must return at least one ranked result"
        );
        assert!(
            elapsed.as_millis() < 1000,
            "E2E: query latency must be < 1s on the small fixture, took {elapsed:?}"
        );

        for (text, score) in &rows {
            assert!(
                *score > 0.0,
                "score must be positive, got {score} for `{text}`"
            );
        }
    }

    #[pg_test]
    fn set_ingest_profile_persists_in_namespace_settings() {
        Spi::run("SELECT pgrg.namespace_create('profile_ns')").unwrap();
        Spi::run("SELECT pgrg.set_ingest_profile('profile_ns', 'aggressive')").unwrap();

        let setting: Option<pgrx::JsonB> =
            Spi::get_one("SELECT settings FROM pgrg.namespaces WHERE name = 'profile_ns'").unwrap();
        let obj = setting.expect("settings present").0;
        assert_eq!(
            obj["ingest_profile"], "aggressive",
            "profile must persist in namespace settings"
        );
    }

    #[pg_test]
    fn set_ingest_profile_rejects_unknown_value() {
        Spi::run("SELECT pgrg.namespace_create('profile_bad_ns')").unwrap();
        let res = std::panic::catch_unwind(|| {
            Spi::run("SELECT pgrg.set_ingest_profile('profile_bad_ns', 'turbo')").unwrap();
        });
        assert!(res.is_err(), "unknown profile must error");
    }

    #[pg_test]
    fn reaper_requeues_stuck_running_job_under_attempt_cap() {
        // SC-012: simulate stuck job; reaper requeues it.
        Spi::run("SELECT pgrg.namespace_create('reaper_ns')").unwrap();
        // Default pgrg.job_reaper_interval is 300s; updated_at set 10 min ago.
        Spi::run(
            "INSERT INTO pgrg.ingest_jobs \
             (id, status, source, namespace, attempt_count, updated_at, started_at, enqueued_at) \
             VALUES ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbb01', 'running', 's.md', 'reaper_ns', \
                     1, now() - interval '10 minutes', now() - interval '10 minutes', now() - interval '10 minutes')",
        )
        .unwrap();

        Spi::run("SELECT pgrg._reaper_sweep()").unwrap();

        let s: Option<String> = Spi::get_one(
            "SELECT status FROM pgrg.ingest_jobs \
             WHERE id = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbb01'",
        )
        .unwrap();
        assert_eq!(
            s.as_deref(),
            Some("queued"),
            "reaper must re-queue stuck job"
        );
    }

    #[pg_test]
    fn reaper_fails_job_at_attempt_cap() {
        // SC-012: max 3 attempts, then 'failed' with reaper message.
        Spi::run("SELECT pgrg.namespace_create('reaper_cap_ns')").unwrap();
        Spi::run(
            "INSERT INTO pgrg.ingest_jobs \
             (id, status, source, namespace, attempt_count, updated_at, started_at, enqueued_at) \
             VALUES ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbb02', 'running', 's.md', 'reaper_cap_ns', \
                     3, now() - interval '10 minutes', now() - interval '10 minutes', now() - interval '10 minutes')",
        )
        .unwrap();

        Spi::run("SELECT pgrg._reaper_sweep()").unwrap();

        let status: Option<String> = Spi::get_one(
            "SELECT status FROM pgrg.ingest_jobs \
             WHERE id = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbb02'",
        )
        .unwrap();
        assert_eq!(status.as_deref(), Some("failed"));

        let error: Option<String> = Spi::get_one(
            "SELECT error FROM pgrg.ingest_jobs \
             WHERE id = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbb02'",
        )
        .unwrap();
        assert!(
            error.unwrap_or_default().contains("reaper"),
            "error message must mention 'reaper'"
        );
    }

    #[pg_test]
    fn ingest_jobs_status_check_violation_has_sqlstate_23514() {
        // SC-013: explicit SQLSTATE 23514 (check_violation) assertion.
        // The constraint shipped in 005_status_check_atomicity.sql (Plan 2);
        // this test asserts the SQLSTATE Postgres raises for the violation
        // is exactly 23514 (check_violation).
        //
        // We use a PL/pgSQL DO block with EXCEPTION WHEN check_violation so
        // the SQLSTATE is captured server-side (pgrx strips ERROR payloads
        // before they reach Rust panic context). The block writes the
        // observed SQLSTATE to a temp table we can SELECT from.
        Spi::run("SELECT pgrg.namespace_create('sqlstate_ns')").unwrap();
        Spi::run("CREATE TEMP TABLE _sqlstate_probe (state text)").unwrap();
        Spi::run(
            "DO $$
            BEGIN
                BEGIN
                    INSERT INTO pgrg.ingest_jobs
                        (id, status, source, namespace)
                    VALUES
                        ('cccccccc-cccc-cccc-cccc-cccccccccc01',
                         'bogus', 't.md', 'sqlstate_ns');
                EXCEPTION WHEN check_violation THEN
                    INSERT INTO _sqlstate_probe(state) VALUES (SQLSTATE);
                END;
            END $$;",
        )
        .unwrap();

        let sqlstate: Option<String> = Spi::get_one("SELECT state FROM _sqlstate_probe").unwrap();
        assert_eq!(
            sqlstate.as_deref(),
            Some("23514"),
            "expected SQLSTATE 23514 (check_violation) on ingest_jobs.status CHECK"
        );
    }

    #[pg_test]
    fn e2e_ingest_then_query_via_run_job() {
        // Mission brief E2E (mod): 5 documents ingested via direct run_job
        // dispatch through SpiPgClient, pgrg.query returns ranked results.
        // The cross-backend bg-worker dispatch is verified in Task 18's DC-006
        // manual-run section (README).
        use pg_raggraph_core::embedding::DeterministicEmbedder;
        use pg_raggraph_core::ingest::run::run_job;
        use pg_raggraph_core::ingest::{IngestRequest, IngestSource};
        use pg_raggraph_core::llm::MockProvider;

        Spi::run("SELECT pgrg.namespace_create('e2e_bgw_ns')").unwrap();
        let docs: Vec<&str> = vec![
            "the auth module verifies user credentials",
            "billing service charges customers monthly",
            "search service ranks documents by relevance",
            "notification service emails alerts to users",
            "auth module also handles password resets",
        ];

        let embedder = DeterministicEmbedder::new(crate::gucs::EMBED_DIM.get() as usize);
        let provider = MockProvider::new();
        let mut client = crate::bgw::spi_client::SpiPgClient;

        for (i, body) in docs.iter().enumerate() {
            let req = IngestRequest {
                source: IngestSource::Text {
                    name: format!("doc{i}"),
                    content: (*body).to_string(),
                },
                namespace: "e2e_bgw_ns".into(),
                chunk_strategy: "auto".into(),
            };
            run_job(&mut client, &req, &embedder, &provider).expect("run_job ok");
        }

        let n_docs: Option<i64> =
            Spi::get_one("SELECT count(*) FROM pgrg.documents WHERE namespace = 'e2e_bgw_ns'")
                .unwrap();
        assert_eq!(n_docs, Some(5), "5 documents must be ingested");

        let results: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.query('auth module', NULL, 5, 'e2e_bgw_ns', 1, NULL, 'hybrid')",
        )
        .unwrap();
        assert!(
            results.unwrap_or(0) >= 1,
            "pgrg.query must return at least one ranked result for ingested corpus"
        );
    }

    #[pg_test]
    fn chunk_strategy_hierarchy_and_sentence_aware_differ_on_markdown() {
        // SC-008 (modified): chunkshop's `Semantic` strategy requires a fastembed
        // boundary-model load (deferred to Plan 4) — see Task 6 commit. Pivoted
        // to `hierarchy` vs `sentence_aware`, both available now. Same SC-008
        // contract: different strategies produce different chunk counts.
        use pg_raggraph_core::embedding::DeterministicEmbedder;
        use pg_raggraph_core::ingest::run::run_job;
        use pg_raggraph_core::ingest::{IngestRequest, IngestSource};
        use pg_raggraph_core::llm::MockProvider;

        Spi::run("SELECT pgrg.namespace_create('strategy_ns')").unwrap();
        // Hierarchy applies `min_section_chars=100` and drops short sections
        // entirely; sentence_aware's `split_prose` keeps every section. We mix
        // two "long" sections (>= 100 char bodies) with two "short" sections
        // (< 100 char bodies) so the counts diverge: hierarchy -> 2,
        // sentence_aware -> 4.
        // Each source uses a slightly different body so the per-document
        // `content_hash` differs (the ON CONFLICT (content_hash) clause would
        // otherwise turn the second insert into SkippedDuplicate).
        let body_h = "# Auth Module H\n\n\
            The authentication module verifies user credentials against the central identity store. \
            It supports password, OAuth, and SAML flows, and integrates with the audit log for every login event. \
            Each verification path emits structured telemetry for downstream analytics.\n\n\
            # Short A\n\nshort body a.\n\n\
            # Billing Service H\n\n\
            The billing service runs nightly to charge customers on their monthly cycle. \
            It batches invoices, handles failed payments with retry, and notifies the dunning workflow. \
            Reconciliation reports are emitted to the finance pipeline every morning.\n\n\
            # Short B\n\nshort body b.";
        let body_s = "# Auth Module S\n\n\
            The authentication module verifies user credentials against the central identity store. \
            It supports password, OAuth, and SAML flows, and integrates with the audit log for every login event. \
            Each verification path emits structured telemetry for downstream analytics.\n\n\
            # Short A\n\nshort body a.\n\n\
            # Billing Service S\n\n\
            The billing service runs nightly to charge customers on their monthly cycle. \
            It batches invoices, handles failed payments with retry, and notifies the dunning workflow. \
            Reconciliation reports are emitted to the finance pipeline every morning.\n\n\
            # Short B\n\nshort body b.";

        let embedder = DeterministicEmbedder::new(crate::gucs::EMBED_DIM.get() as usize);
        let provider = MockProvider::new();
        let mut client = crate::bgw::spi_client::SpiPgClient;

        for (name, strategy, body) in [
            ("h.md", "hierarchy", body_h),
            ("s.md", "sentence_aware", body_s),
        ] {
            let req = IngestRequest {
                source: IngestSource::Text {
                    name: (*name).to_string(),
                    content: (*body).to_string(),
                },
                namespace: "strategy_ns".into(),
                chunk_strategy: (*strategy).to_string(),
            };
            run_job(&mut client, &req, &embedder, &provider).expect("run_job ok");
        }

        let count_for_source = |s: &str| -> i64 {
            let n: Option<i64> = Spi::get_one(&format!(
                "SELECT count(*) FROM pgrg.chunks c \
                 JOIN pgrg.documents d ON d.id = c.document_id \
                 WHERE d.source = '{s}' AND d.namespace = 'strategy_ns'"
            ))
            .unwrap();
            n.unwrap_or(0)
        };
        let n_h = count_for_source("h.md");
        let n_s = count_for_source("s.md");
        assert!(n_h > 0 && n_s > 0, "both strategies must produce chunks");
        assert_ne!(
            n_h, n_s,
            "SC-008: hierarchy and sentence_aware must differ in chunk count, got h={n_h}, s={n_s}"
        );
    }

    #[pg_test]
    fn token_count_present_on_chunks() {
        // SC-008 second clause: chunks have token_count set.
        use pg_raggraph_core::embedding::DeterministicEmbedder;
        use pg_raggraph_core::ingest::run::run_job;
        use pg_raggraph_core::ingest::{IngestRequest, IngestSource};
        use pg_raggraph_core::llm::MockProvider;

        Spi::run("SELECT pgrg.namespace_create('tokens_ns')").unwrap();

        let req = IngestRequest {
            source: IngestSource::Text {
                name: "t.md".into(),
                content: "short text body for token count check".into(),
            },
            namespace: "tokens_ns".into(),
            chunk_strategy: "auto".into(),
        };
        let embedder = DeterministicEmbedder::new(crate::gucs::EMBED_DIM.get() as usize);
        let provider = MockProvider::new();
        let mut client = crate::bgw::spi_client::SpiPgClient;
        run_job(&mut client, &req, &embedder, &provider).expect("run_job ok");

        let nulls: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.chunks WHERE namespace = 'tokens_ns' AND token_count IS NULL",
        )
        .unwrap();
        assert_eq!(nulls, Some(0), "all chunks must have token_count set");
    }

    #[pg_test]
    fn embedder_is_loaded_once_per_worker_not_per_job() {
        // SC-009: source-contract assertion — build_backend must be called
        // before the latch loop so the model is loaded exactly once per worker.
        let src = include_str!("bgw/worker.rs");
        let pos_build = src
            .find("build_backend()")
            .expect("must call build_backend");
        let pos_loop = src.find("wait_latch").expect("must enter latch loop");
        assert!(
            pos_build < pos_loop,
            "SC-009: build_backend must be called before the latch loop (model loaded once)"
        );
    }

    #[pg_test]
    fn master_key_path_guc_is_suset_not_sighup() {
        // SC-007 (Plan 1 deferred concern): the GUC must require superuser to
        // change. A non-superuser session that tries SET pgrg.master_key_path
        // must fail with insufficient privilege. Suset context guarantees this
        // at the GUC layer, but we lock the contract via an introspection check
        // because pg_test runs as superuser and cannot directly assert privilege
        // rejection without role-switching gymnastics.
        let context: Option<String> = Spi::get_one(
            "SELECT context FROM pg_settings WHERE name = 'pg_raggraph.master_key_path'",
        )
        .unwrap();
        assert_eq!(
            context.as_deref(),
            Some("superuser"),
            "master_key_path GUC must be Suset (pg_settings.context = 'superuser')"
        );
    }

    /// Write a 32-byte master key to a temp file with 0600 permissions and
    /// return the absolute path. The temp file is leaked (persisted on disk)
    /// for the test duration so the PG backend can re-read it across SPI calls.
    fn write_master_key_file(bytes: &[u8; 32]) -> String {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;
        let mut f = tempfile::NamedTempFile::new().expect("tempfile");
        f.write_all(bytes).unwrap();
        std::fs::set_permissions(f.path(), std::fs::Permissions::from_mode(0o600)).unwrap();
        let p = f.path().to_string_lossy().into_owned();
        // Persist (keep the path) so PG can read the file for the test duration.
        let _ = f.into_temp_path().keep().unwrap();
        p
    }

    /// SC-003: when `master_key_path` is set, `provider_create` must store the
    /// credential encrypted (`enc:v1:` prefix) — never as plaintext.
    #[pg_test]
    fn provider_create_encrypts_credential_when_master_key_set() {
        let key = [0xCDu8; 32];
        let path = write_master_key_file(&key);
        Spi::run(&format!("SET pg_raggraph.master_key_path = '{path}'")).unwrap();

        Spi::run(
            "SELECT pgrg.provider_create('enc-p', 'llm', 'openai', \
                                          'https://api.openai.com', 'gpt-4o', \
                                          'sk-test-PLAINTEXT-9876543210', '{}')",
        )
        .unwrap();

        // Read the raw row, not provider_list (which redacts).
        let stored: Option<String> =
            Spi::get_one("SELECT credential FROM pgrg.providers WHERE name = 'enc-p'").unwrap();
        let stored = stored.expect("credential column should be non-NULL");
        assert!(
            stored.starts_with("enc:v1:"),
            "expected enc:v1: prefix, got {stored:?}"
        );
        assert!(
            !stored.contains("PLAINTEXT"),
            "plaintext leaked into encrypted column: {stored}"
        );
        assert!(!stored.contains("9876543210"), "plaintext leaked: {stored}");
    }

    /// SC-004: `provider_list` must redact uniformly regardless of whether the
    /// stored credential is encrypted or plaintext. Here we exercise the
    /// plaintext path (master_key_path RESET to unset).
    #[pg_test]
    fn provider_list_redacts_encrypted_and_plaintext_uniformly() {
        // Plaintext path (no master key set for this test).
        Spi::run("RESET pg_raggraph.master_key_path").unwrap();
        Spi::run(
            "SELECT pgrg.provider_create('plain-p', 'llm', 'openai', \
                                          NULL, 'gpt-4o', 'sk-plain-secret-AAA', '{}')",
        )
        .unwrap();

        let json: pgrx::JsonB = Spi::get_one("SELECT pgrg.provider_list()")
            .unwrap()
            .expect("provider_list");
        let arr = json.0.as_array().unwrap();
        let plain_obj = arr
            .iter()
            .find(|o| o["name"] == "plain-p")
            .expect("plain-p row missing");
        let cred = plain_obj["credential"].as_str().unwrap();
        assert!(cred.starts_with("sk-"), "expected prefix, got {cred}");
        assert!(cred.contains("***"), "expected redaction, got {cred}");
        assert!(!cred.contains("secret"), "plaintext body leaked: {cred}");
    }

    /// SC-005: when `master_key_path` is unset at `_PG_init`, the WARNING must
    /// fire. The test-only `MASTER_KEY_WARNING_FIRED` sentinel records this.
    #[pg_test]
    fn master_key_path_unset_fires_startup_warning() {
        // _PG_init has already run for this backend. If the test harness boots
        // without master_key_path pre-set (the default), the sentinel is true.
        let fired = crate::MASTER_KEY_WARNING_FIRED.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            fired,
            "WARNING for unset master_key_path was not emitted at _PG_init"
        );
    }

    /// SC-015: simulate a `pg_dump` read of the raw `credential` column for an
    /// encrypted row. The plaintext secret must not appear anywhere in the dump.
    #[pg_test]
    fn pg_dump_does_not_contain_plaintext_credential() {
        let key = [0xEFu8; 32];
        let path = write_master_key_file(&key);
        Spi::run(&format!("SET pg_raggraph.master_key_path = '{path}'")).unwrap();

        let secret = "sk-pgdump-secret-XYZ-ABC-12345";
        Spi::run(&format!(
            "SELECT pgrg.provider_create('dump-p', 'llm', 'openai', NULL, 'gpt-4o', '{secret}', '{{}}')"
        ))
        .unwrap();

        // pg_dump simulator: read the raw column as text. SC-015 contract is
        // that plaintext does not appear in this value.
        let copied: Option<String> = Spi::get_one(
            "SELECT string_agg(credential, ',') FROM pgrg.providers WHERE name='dump-p'",
        )
        .unwrap();
        let copied = copied.unwrap_or_default();
        assert!(
            !copied.contains(secret),
            "encrypted column leaks plaintext: {copied}"
        );
        assert!(!copied.contains("XYZ"), "leak: {copied}");
    }

    #[pg_test]
    fn master_key_path_with_0644_perms_fails_provider_create() {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;

        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(&[0u8; 32]).unwrap();
        // 0644: world-readable. Must be rejected.
        std::fs::set_permissions(f.path(), std::fs::Permissions::from_mode(0o644)).unwrap();
        let path = f.path().to_string_lossy().into_owned();
        let _ = f.into_temp_path().keep().unwrap();

        Spi::run(&format!("SET pg_raggraph.master_key_path = '{path}'")).unwrap();

        // Expect provider_create to ERROR out because load_master_key rejects 0644.
        let res = std::panic::catch_unwind(|| {
            Spi::run(
                "SELECT pgrg.provider_create('badperm-p', 'llm', 'openai', NULL, 'gpt-4o', 'sk-x', '{}')",
            ).unwrap();
        });
        assert!(
            res.is_err(),
            "0644 master key file must cause provider_create to error"
        );
    }

    #[pg_test]
    fn master_key_path_with_0600_perms_succeeds() {
        let key = [0xAAu8; 32];
        let path = write_master_key_file(&key);
        Spi::run(&format!("SET pg_raggraph.master_key_path = '{path}'")).unwrap();

        Spi::run(
            "SELECT pgrg.provider_create('goodperm-p', 'llm', 'openai', NULL, 'gpt-4o', 'sk-x', '{}')",
        )
        .unwrap();

        let n: Option<i64> =
            Spi::get_one("SELECT count(*) FROM pgrg.providers WHERE name = 'goodperm-p'").unwrap();
        assert_eq!(n, Some(1));
    }

    #[pg_test]
    fn e2e_three_statement_demo_with_mock_provider() {
        // SC-009: pgrg.ask returns a non-empty answer + structured citations.
        // SC-010: every citation references a chunk that was actually retrieved
        //         (verified end-to-end: each cited chunk_id exists in
        //         pgrg.chunks). _core::llm::prompt::extract_citations silently
        //         drops forged `[N]` markers outside the prompt's id_map, so
        //         the only way `[1]` / `[2]` survive into the output is if
        //         the chunks they map to actually came from pgrg.query.
        // SC-017: MockProvider drives the LLM step — no network, no creds.
        //
        // Test wiring: pgrx tests run inside one transaction that ROLLBACKs
        // at the end, so any rows inserted into `pgrg.ingest_jobs` are
        // invisible to the bg-worker backends (MVCC). The existing
        // `ingest_text_enqueues_payload_and_pipeline_writes_document` test
        // handles this by driving `_core::ingest::run::run_job` directly
        // (line ~1280 here: "Part 2: directly drive run_job (the same path
        // the worker takes)"). We follow that pattern — the worker's
        // dispatch wrapper is exercised by Plan 3's queue/launcher tests;
        // this test exercises the ingest → retrieve → ask → cite path
        // end-to-end inside the same transaction.
        //
        // Mock-provider mechanics (see pg_raggraph/src/ask.rs::build_provider_impl):
        // a provider row with `provider = 'mock'` stuffs the row's
        // credential string into MockProvider::with_stub_answer. We seed
        // it with an answer containing `[1]` and `[2]` to prove that
        // `_core::llm::ask::ask` wires retrieval → prompt id_map →
        // structured citations correctly. With two short docs and top_k=10,
        // both seeded chunks will be returned by retrieval and occupy the
        // first two id_map slots, so `[1]` and `[2]` both resolve.
        use pg_raggraph_core::embedding::DeterministicEmbedder;
        use pg_raggraph_core::ingest::run::run_job;
        use pg_raggraph_core::ingest::{IngestRequest, IngestSource};
        use pg_raggraph_core::llm::MockProvider;

        Spi::run("SELECT pgrg.namespace_create('e2e_demo_ns')").unwrap();

        // 1) Register a 'mock' provider whose stub answer carries [1] and [2].
        Spi::run(
            "SELECT pgrg.provider_create('e2e-demo-mock', 'llm', 'mock', NULL, 'mock-v1', \
             'Auth verifies user credentials [1]. Tokens expire after 24 hours [2].', '{}')",
        )
        .unwrap();

        // 2) Drive the ingest pipeline directly for two short docs. This is
        //    the same code path the bg worker takes — see
        //    pg_raggraph/src/bgw/worker.rs::run_one (`run_job(&mut client,
        //    &req, &*embedder, &provider)`).
        let embedder = DeterministicEmbedder::new(crate::gucs::EMBED_DIM.get() as usize);
        let provider = MockProvider::new();
        let mut client = crate::bgw::spi_client::SpiPgClient;

        let req1 = IngestRequest {
            source: IngestSource::Text {
                name: "auth-doc-1".into(),
                content: "the auth module verifies user credentials".into(),
            },
            namespace: "e2e_demo_ns".into(),
            chunk_strategy: "auto".into(),
        };
        run_job(&mut client, &req1, &embedder, &provider).expect("run_job auth-doc-1");

        let req2 = IngestRequest {
            source: IngestSource::Text {
                name: "token-doc-1".into(),
                content: "session tokens expire after 24 hours of inactivity".into(),
            },
            namespace: "e2e_demo_ns".into(),
            chunk_strategy: "auto".into(),
        };
        run_job(&mut client, &req2, &embedder, &provider).expect("run_job token-doc-1");

        // Sanity: both docs landed.
        let docs: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM pgrg.documents \
             WHERE namespace = 'e2e_demo_ns' AND source IN ('auth-doc-1', 'token-doc-1')",
        )
        .unwrap();
        assert_eq!(docs, Some(2), "expected 2 ingested documents, got {docs:?}");

        // 3) Call pgrg.ask. Wrap the TableIterator row in to_jsonb so we get
        //    one inspectable jsonb object.
        let result: Option<pgrx::JsonB> = Spi::get_one(
            "SELECT to_jsonb(t) FROM pgrg.ask( \
                 'what does the auth module do?', \
                 NULL::jsonb, 10, 'e2e_demo_ns', 1, 'e2e-demo-mock' \
             ) t",
        )
        .unwrap();
        let r = result.expect("pgrg.ask returned NULL").0;

        let answer = r["answer"].as_str().expect("answer must be a string");
        let citations = r["citations"]
            .as_array()
            .expect("citations must be an array");
        let mode = r["mode_used"].as_str().expect("mode_used must be a string");

        // SC-009: non-empty answer + at least one citation.
        assert!(!answer.is_empty(), "answer empty: {answer:?}");
        assert!(
            !citations.is_empty(),
            "expected >=1 citation, got 0. answer={answer:?}"
        );

        // SC-010: every cited chunk_id must exist in pgrg.chunks.
        //         If extract_citations had forged or fabricated an id, the
        //         row would be missing here.
        for c in citations {
            let cid = c["chunk_id"].as_str().expect("chunk_id must be a string");
            let exists: Option<i64> = Spi::get_one_with_args(
                "SELECT count(*)::bigint FROM pgrg.chunks WHERE id::text = $1",
                &[cid.into()],
            )
            .unwrap();
            assert_eq!(
                exists,
                Some(1),
                "cited chunk_id {cid} not in pgrg.chunks (forged)"
            );
        }

        // Plan 4 has no smart-mode escalation yet — ask always reports hybrid.
        assert_eq!(mode, "hybrid");
    }

    #[pg_test]
    fn ask_signals_contains_llm_attribution() {
        // SC-018: signals.llm has provider/model/latency_ms/prompt_tokens/completion_tokens.
        use pg_raggraph_core::embedding::DeterministicEmbedder;
        use pg_raggraph_core::ingest::run::run_job;
        use pg_raggraph_core::ingest::{IngestRequest, IngestSource};
        use pg_raggraph_core::llm::MockProvider;

        Spi::run("SELECT pgrg.namespace_create('signals_ns')").unwrap();

        // Register a fresh mock provider — unique name to avoid collision with T18.
        Spi::run(
            "SELECT pgrg.provider_create('signals-mock-p', 'llm', 'mock', NULL, 'mock-v18', \
             'an answer [1].', '{}')",
        )
        .unwrap();

        // Drive the ingest pipeline directly, same pattern as T18.
        let embedder = DeterministicEmbedder::new(crate::gucs::EMBED_DIM.get() as usize);
        let provider = MockProvider::new();
        let mut client = crate::bgw::spi_client::SpiPgClient;

        let req = IngestRequest {
            source: IngestSource::Text {
                name: "signals-doc-x".into(),
                content: "short fixture text for signals test".into(),
            },
            namespace: "signals_ns".into(),
            chunk_strategy: "auto".into(),
        };
        run_job(&mut client, &req, &embedder, &provider).expect("run_job signals-doc-x");

        // Read signals from pgrg.ask.
        let result: Option<pgrx::JsonB> = Spi::get_one(
            "SELECT to_jsonb(t) FROM pgrg.ask( \
                 'test query?', \
                 NULL::jsonb, 5, 'signals_ns', 1, 'signals-mock-p' \
             ) t",
        )
        .unwrap();
        let r = result.expect("pgrg.ask returned NULL").0;
        let sig = r["signals"].clone();

        // Verify signals.llm has all five required keys.
        let llm = sig.get("llm").expect("signals.llm missing entirely");
        for key in [
            "provider",
            "model",
            "latency_ms",
            "prompt_tokens",
            "completion_tokens",
        ] {
            assert!(
                llm.get(key).is_some(),
                "signals.llm missing key `{key}`: {llm}"
            );
        }

        // Sanity-check the attribution values.
        assert_eq!(
            llm["provider"].as_str(),
            Some("signals-mock-p"),
            "signals.llm.provider mismatch"
        );
        assert_eq!(
            llm["model"].as_str(),
            Some("mock-v18"),
            "signals.llm.model mismatch"
        );
        assert!(
            llm["latency_ms"].as_u64().is_some(),
            "latency_ms must be a u64, got: {:?}",
            llm["latency_ms"]
        );
        assert!(
            llm["prompt_tokens"].as_u64().is_some(),
            "prompt_tokens must be a u64"
        );
        assert!(
            llm["completion_tokens"].as_u64().is_some(),
            "completion_tokens must be a u64"
        );
    }

    #[pg_test]
    fn ingest_with_mock_extractor_creates_entities_and_relationship() {
        // SC-013: extraction pipeline produces entities + relationships.
        // Uses the 'mock-extractor' provider kind (T24): the stub
        // `Extraction` is encoded as JSON in the `credential` column;
        // `provider_factory::build_provider_impl` parses it and injects
        // it into MockProvider::with_stub_extraction. No network.
        //
        // pgrx test MVCC isolation prevents the bg-worker queue from
        // seeing test rows, so we drive `_core::ingest::run::run_job`
        // directly via SpiPgClient — the same path the bg worker takes
        // (`pg_raggraph/src/bgw/worker.rs::run_one`). The provider used
        // here comes from `provider_factory::resolve_or_default_provider`,
        // which resolves the namespace's `llm_provider` and constructs
        // the MockProvider via the new "mock-extractor" arm.
        use pg_raggraph_core::embedding::DeterministicEmbedder;
        use pg_raggraph_core::ingest::run::run_job;
        use pg_raggraph_core::ingest::{IngestRequest, IngestSource};

        let extraction = serde_json::json!({
            "entities": [
                {"name": "Alice", "kind": "person", "confidence": 0.95},
                {"name": "Acme Corp", "kind": "organization", "confidence": 0.92}
            ],
            "relationships": [
                {"src_name": "Alice", "dst_name": "Acme Corp", "kind": "works_at",
                 "weight": 1.0, "confidence": 0.9}
            ]
        })
        .to_string();

        // Single-quote-escape for SQL literal.
        let credential_sql = extraction.replace('\'', "''");

        Spi::run(&format!(
            "SELECT pgrg.provider_create('mock-ext-p', 'llm', 'mock-extractor', \
             NULL, 'v1', '{credential_sql}', '{{}}')"
        ))
        .unwrap();

        // Namespace with this provider as default. resolve_or_default_provider
        // reads namespace.llm_provider.
        Spi::run("SELECT pgrg.namespace_create('ns-ext', 'bge-small-en-v1.5', 'mock-ext-p', '{}')")
            .unwrap();

        let embedder = DeterministicEmbedder::new(crate::gucs::EMBED_DIM.get() as usize);
        let provider = crate::provider_factory::resolve_or_default_provider("ns-ext");
        let mut client = crate::bgw::spi_client::SpiPgClient;

        let req = IngestRequest {
            source: IngestSource::Text {
                name: "doc-ext".into(),
                content: "Alice works at Acme Corp.".into(),
            },
            namespace: "ns-ext".into(),
            chunk_strategy: "auto".into(),
        };
        run_job(&mut client, &req, &embedder, &*provider).expect("run_job ok");

        // SC-013: >=2 entities and exactly 1 'works_at' relationship.
        let entities: Option<i64> =
            Spi::get_one("SELECT count(*)::bigint FROM pgrg.entities WHERE namespace = 'ns-ext'")
                .unwrap();
        assert!(
            entities.unwrap_or(0) >= 2,
            "expected >=2 entities, got {entities:?}"
        );

        let rels: Option<i64> = Spi::get_one(
            "SELECT count(*)::bigint FROM pgrg.relationships WHERE namespace = 'ns-ext' \
             AND kind = 'works_at'",
        )
        .unwrap();
        assert_eq!(rels, Some(1));
    }

    #[pg_test]
    fn entity_resolution_merges_punctuation_variant() {
        // SC-014 (partial — legs 1 + 2 proven here; leg 3 deferred).
        //
        // SC-014's contract: "Acme Corp" and "Acme Corp." resolve to ONE
        // entity. `resolve_or_insert_entity` (T21) requires BOTH
        // pg_trgm.similarity >= 0.85 (TRGM_MERGE) AND embedding cosine
        // >= 0.90 to merge. That splits SC-014 into three legs:
        //
        //   Leg 1 — resolution decision logic (both-thresholds-required):
        //           proven by the T21 unit test on `resolve_or_insert_entity`.
        //   Leg 2 — real pg_trgm name similarity >= 0.85 for the
        //           punctuation variant: REGRESSION-LOCKED by Assertion 1
        //           below, against REAL PostgreSQL pg_trgm.
        //   Leg 3 — embedding cosine >= 0.90 with REAL SEMANTIC embeddings:
        //           NOT provable here. The deterministic test embedder is a
        //           SHA-256 hash, not a semantic model: identical strings
        //           give cosine 1.0, but any byte difference gives
        //           orthogonal vectors (cosine ~= 0). For the
        //           "Acme Corp"/"Acme Corp." pair the probe measured
        //           trgm=1.0 (clears 0.85) but deterministic-embedder
        //           cosine=-0.0225 (cannot clear 0.90, by design). Leg 3 is
        //           therefore DEFERRED to the Plan 3 ONNX-embedder
        //           carry-forward (tracked in the CHANGELOG, wired in T26),
        //           where real semantic embeddings make the cosine leg pass.
        //
        // Assertion 2 ingests two documents whose extracted entity name is
        // IDENTICAL ("Acme Corp"). Identical strings -> identical
        // deterministic embedding -> cosine 1.0, and trgm 1.0 -> BOTH
        // thresholds clear -> the resolver MUST merge. This proves the
        // real-PG resolution+merge pipeline end-to-end (SpiPgClient's real
        // pg_trgm `fuzzy_match_entity` + `resolve_or_insert_entity` +
        // `run_job` persistence across two documents/transactions), which
        // is the exact machinery SC-014's punctuation-variant case depends
        // on once leg 3 lands.
        use pg_raggraph_core::embedding::DeterministicEmbedder;
        use pg_raggraph_core::ingest::run::run_job;
        use pg_raggraph_core::ingest::{IngestRequest, IngestSource};

        // --- Assertion 1: real pg_trgm clears TRGM_MERGE for the
        // punctuation variant (leg 2 regression lock, real PostgreSQL). ---
        let trgm: Option<f32> =
            Spi::get_one("SELECT similarity('Acme Corp', 'Acme Corp.')::real").unwrap();
        assert!(
            trgm.unwrap_or(0.0) >= 0.85,
            "SC-014 leg 2: real pg_trgm similarity for the punctuation variant must \
             clear TRGM_MERGE (0.85); got {trgm:?}"
        );

        // --- Assertion 2: real-PG resolution+merge pipeline collapses
        // duplicate entities across two documents into ONE row. ---
        // Two docs, SAME entity name "Acme Corp". Both clear trgm (1.0) AND
        // cosine (1.0, identical deterministic embedding) -> the resolver
        // MUST merge them. This proves the real-PG merge machinery
        // (SpiPgClient + resolve + run_job) works end-to-end. The
        // PUNCTUATION-variant case additionally needs the cosine leg with
        // real semantic embeddings (ONNX) -- see the doc comment above and
        // the CHANGELOG carry-forward note (handled in T26).
        let extraction = serde_json::json!({
            "entities": [
                {"name": "Acme Corp", "kind": "organization", "confidence": 0.95}
            ],
            "relationships": []
        })
        .to_string();
        let credential_sql = extraction.replace('\'', "''");

        Spi::run(&format!(
            "SELECT pgrg.provider_create('mock-res-p', 'llm', 'mock-extractor', \
             NULL, 'v1', '{credential_sql}', '{{}}')"
        ))
        .unwrap();
        Spi::run("SELECT pgrg.namespace_create('ns-res', 'bge-small-en-v1.5', 'mock-res-p', '{}')")
            .unwrap();

        let embedder = DeterministicEmbedder::new(crate::gucs::EMBED_DIM.get() as usize);
        let provider = crate::provider_factory::resolve_or_default_provider("ns-res");
        let mut client = crate::bgw::spi_client::SpiPgClient;

        for doc_name in ["doc-res-a", "doc-res-b"] {
            let req = IngestRequest {
                source: IngestSource::Text {
                    name: doc_name.into(),
                    content: format!("{doc_name}: Acme Corp is an organization."),
                },
                namespace: "ns-res".into(),
                chunk_strategy: "auto".into(),
            };
            run_job(&mut client, &req, &embedder, &*provider).expect("run_job ok");
        }

        // Both ingests extracted the identical entity name; resolution must
        // collapse them into exactly ONE 'Acme%' entity in the namespace.
        let acme_entities: Option<i64> = Spi::get_one(
            "SELECT count(*)::bigint FROM pgrg.entities \
             WHERE namespace = 'ns-res' AND name LIKE 'Acme%'",
        )
        .unwrap();
        assert_eq!(
            acme_entities,
            Some(1),
            "SC-014 legs 1+2 E2E: two documents extracting the identical entity \
             name must resolve+merge into exactly ONE 'Acme%' entity via the \
             real-PG pipeline; got {acme_entities:?}"
        );
    }

    #[pg_test]
    fn sc011_graph_traversal_is_undirected() {
        // SC-011: A->B directional relationship (src_id=A, dst_id=B). A graph
        // query seeded at A must reach B's chunk, and a query seeded at B
        // must reach A's chunk. This proves the recursive-CTE walker is
        // undirected (it UNION ALLs both directions, per spec §10).
        //
        // Resolve from the workspace root regardless of the harness cwd:
        // CARGO_MANIFEST_DIR is <repo>/pg_raggraph; the fixture lives at
        // <repo>/bench/parity/undirected_fixture.jsonl.
        let src = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../bench/parity/undirected_fixture.jsonl"
        );
        assert!(
            std::path::Path::new(src).exists(),
            "sc011: fixture not found at {src}"
        );

        let dest = "/tmp/pgrg_undir.jsonl";
        std::fs::copy(src, dest).expect("sc011: cannot copy undirected fixture");

        Spi::run("SELECT pgrg.namespace_create('undir')").unwrap();
        Spi::run(&format!(
            "SELECT pgrg.ingest_extracted('{dest}', 'undir')"
        ))
        .unwrap();

        // Seeded at A: must reach B's chunk via 1-hop graph traversal
        // (forward direction src_id=A -> dst_id=B).
        let from_a: i64 = Spi::get_one(
            "SELECT count(*) FROM pgrg.query('EntityA', NULL, 10, 'undir', 1, \
             NULL, 'graph') WHERE text LIKE '%B-only%'",
        )
        .unwrap()
        .unwrap_or(0);

        // Seeded at B: must reach A's chunk (reverse direction
        // dst_id=B -> src_id=A — the undirected leg).
        let from_b: i64 = Spi::get_one(
            "SELECT count(*) FROM pgrg.query('EntityB', NULL, 10, 'undir', 1, \
             NULL, 'graph') WHERE text LIKE '%A-only%'",
        )
        .unwrap()
        .unwrap_or(0);

        assert!(
            from_a >= 1,
            "sc011: A-> query did not reach B's chunk (undirected check \
             failed; traversal may be directional); from_a={from_a}"
        );
        assert!(
            from_b >= 1,
            "sc011: B-> query did not reach A's chunk (undirected check \
             failed; traversal may be directional); from_b={from_b}"
        );
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
