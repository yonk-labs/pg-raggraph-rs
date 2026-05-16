//! SC-006: the managed-PG client shim `pgrg.ask` (`PL/pgSQL`, `pg_net` ->
//! sidecar `POST /v1/ask`) must present the SAME call surface and the SAME
//! `TABLE(answer text, citations jsonb, signals jsonb, mode_used text)`
//! return shape as the in-extension pgrx `pgrg.ask`
//! (`pg_raggraph/src/ask.rs`).
//!
//! This needs a Postgres that ships `pg_net` — the stock pgvector image
//! does NOT. It is gated on `PGRG_SUPABASE_TEST_DATABASE_URL` (the
//! `pg-supabase` compose service on port 5444), distinct from the stock
//! `PGRG_TEST_DATABASE_URL` (port 5443). When unset, the test SKIPs so CI
//! without the Supabase fixture stays green.
//!
//! `pg_net`'s background worker issues the HTTP request from INSIDE the
//! container, so it cannot reach a host `127.0.0.1` bind. The sidecar is
//! therefore bound on `0.0.0.0:<ephemeral>` and `pg_net` is pointed at
//! `http://<PGRG_SIDECAR_HOST_FOR_PGNET>:<port>` (default
//! `host.docker.internal`, wired via the compose service's
//! `extra_hosts: host-gateway`).
//!
//! DC-004: the sidecar bootstrap installs only the `pgrg.*` tables; the
//! pgrx callables (`pgrg.ask`/`pgrg.query`/`pgrg.ingest*`) are absent in
//! sidecar mode. This test executes `client.sql` to install `pgrg.ask`,
//! proving the shim restores the `PL/pgSQL` entry point users need.

use std::sync::Arc;

use pg_raggraph_core::embedding::EmbeddingBackend;
use pg_raggraph_sidecar::config::SidecarConfig;
use pg_raggraph_sidecar::embedder::build_embedder;
use pg_raggraph_sidecar::{bootstrap, db, http};
use tokio_postgres::Client;

fn supabase_db_url() -> Option<String> {
    std::env::var("PGRG_SUPABASE_TEST_DATABASE_URL").ok()
}

/// Hostname `pg_net` (inside the container) uses to reach the host-bound
/// sidecar. Default works with the compose service's host-gateway.
fn pgnet_host() -> String {
    std::env::var("PGRG_SIDECAR_HOST_FOR_PGNET")
        .unwrap_or_else(|_| "host.docker.internal".to_string())
}

/// Byte-identical to the handler's pgvector literal so the seeded chunk
/// embedding round-trips against the Rust-computed query embedding.
fn vector_literal(v: &[f32]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(v.len() * 8 + 2);
    s.push('[');
    let mut first = true;
    for x in v {
        if !first {
            s.push(',');
        }
        first = false;
        let _ = write!(s, "{x}");
    }
    s.push(']');
    s
}

async fn reset_and_seed(c: &mut Client, embedder: &dyn EmbeddingBackend) {
    c.batch_execute("DROP SCHEMA IF EXISTS pgrg CASCADE;")
        .await
        .expect("reset schema");
    bootstrap::run_migrations(c)
        .await
        .expect("bootstrap schema");

    c.execute(
        "INSERT INTO pgrg.providers (name, kind, provider, model, credential) \
         VALUES ('mock', 'llm', 'mock', 'mock-model', $1)",
        &[&"According to the source, the build passes [1]."],
    )
    .await
    .expect("register mock provider");

    c.execute(
        "UPDATE pgrg.namespaces SET llm_provider = 'mock' WHERE name = 'default'",
        &[],
    )
    .await
    .expect("set namespace default provider");

    let doc_id: uuid::Uuid = c
        .query_one(
            "INSERT INTO pgrg.documents (namespace, source, content_hash, title) \
             VALUES ('default', 'pgnet-client-test', 'hash-pgnet-1', 'SC-006 fixture') \
             RETURNING id",
            &[],
        )
        .await
        .expect("insert document")
        .get(0);

    let chunk_text = "The sidecar exposes POST /v1/ask and returns grounded answers.";
    let emb = embedder.embed(chunk_text).expect("embed chunk");
    let lit = vector_literal(&emb);
    c.execute(
        &format!(
            "INSERT INTO pgrg.chunks \
                 (namespace, document_id, ord, text, token_count, embedding) \
             VALUES ('default', $1, 0, $2, $3, '{lit}'::vector)"
        ),
        &[&doc_id, &chunk_text, &12_i32],
    )
    .await
    .expect("insert chunk");
}

/// SC-006 + DC-004: install `client.sql` on a pg_net-capable Postgres,
/// point `pgrg.sidecar_url` at an in-test sidecar, call `pgrg.ask` over
/// SQL, and assert the 4-column parity shape comes back populated.
#[tokio::test]
async fn pgrg_ask_via_pg_net_returns_parity_shape() {
    let Some(url) = supabase_db_url() else {
        eprintln!(
            "SKIP: PGRG_SUPABASE_TEST_DATABASE_URL unset (pg_net needs supabase/postgres; \
             stock pgvector image does not ship it)"
        );
        return;
    };

    let embedder: Arc<dyn EmbeddingBackend> = build_embedder(384, None).expect("build embedder");

    let mut setup = db::connect(&url).await.expect("connect for setup");
    reset_and_seed(&mut setup, embedder.as_ref()).await;

    // pg_net is the prerequisite the managed-PG DBA installs once.
    setup
        .batch_execute("CREATE EXTENSION IF NOT EXISTS pg_net;")
        .await
        .expect("create pg_net extension");

    // DC-004: the bootstrap above did NOT install pgrg.ask (pgrx-only in
    // extension mode). Prove its absence, then install the shim.
    let ask_exists_before: bool = setup
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_proc p \
               JOIN pg_namespace n ON n.oid = p.pronamespace \
              WHERE n.nspname = 'pgrg' AND p.proname = 'ask')",
            &[],
        )
        .await
        .expect("probe pgrg.ask presence")
        .get(0);
    assert!(
        !ask_exists_before,
        "DC-004: sidecar bootstrap must NOT install pgrg.ask (pgrx-only); \
         client.sql is what restores it"
    );

    let client_sql = include_str!("../sql/client.sql");
    setup
        .batch_execute(client_sql)
        .await
        .expect("install client.sql (pgrg.ask shim)");

    let ask_exists_after: bool = setup
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_proc p \
               JOIN pg_namespace n ON n.oid = p.pronamespace \
              WHERE n.nspname = 'pgrg' AND p.proname = 'ask')",
            &[],
        )
        .await
        .expect("probe pgrg.ask presence (post)")
        .get(0);
    assert!(ask_exists_after, "client.sql must install pgrg.ask");

    // Bind the sidecar on 0.0.0.0 so pg_net (in the container) can reach
    // it via host-gateway. Ephemeral port; discover the actual one.
    let cfg = Arc::new(SidecarConfig::parse_from([
        "pg-raggraph-sidecar",
        "--database-url",
        &url,
        "--http-bind",
        "0.0.0.0:0",
    ]));
    let app = http::router(Arc::clone(&cfg)).expect("build router");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    // Point pg_net at the host-reachable sidecar URL.
    let sidecar_url = format!("http://{}:{}", pgnet_host(), port);
    setup
        .execute(&format!("SET pgrg.sidecar_url = '{sidecar_url}'"), &[])
        .await
        .expect("set pgrg.sidecar_url");

    // The SQL call surface a managed-PG PL/pgSQL user actually uses.
    let row = setup
        .query_one(
            "SELECT answer, citations, signals, mode_used \
             FROM pgrg.ask('what does the sidecar expose?')",
            &[],
        )
        .await
        .expect("SELECT * FROM pgrg.ask(...)");

    let answer: String = row.get("answer");
    let citations: serde_json::Value = row.get("citations");
    let signals: serde_json::Value = row.get("signals");
    let mode_used: String = row.get("mode_used");

    assert!(!answer.is_empty(), "answer must be non-empty: {answer:?}");
    assert!(
        citations.is_array(),
        "citations must be a JSON array: {citations}"
    );
    assert!(
        !citations.as_array().unwrap().is_empty(),
        "expected >=1 citation from the `[1]` stub: {citations}"
    );
    assert!(
        signals.is_object(),
        "signals must be a JSON object: {signals}"
    );
    assert_eq!(mode_used, "hybrid", "mode_used parity with pgrx ask.rs");

    server.abort();
}
