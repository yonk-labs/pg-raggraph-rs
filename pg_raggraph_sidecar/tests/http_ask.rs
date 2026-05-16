//! SC-005 / SC-016 / SC-013: `POST /v1/ask` end-to-end tests against a real
//! managed-style Postgres. Covers:
//!
//! - Happy path (Task 15): 200 with parity JSON shape.
//! - Error envelopes (Task 16 / SC-013):
//!   - Malformed body → 400 `bad_request` (sanitized; no credentials).
//!   - Unknown namespace → 404 `unknown_namespace`.
//!   - 500 envelope shape (unit test; no DB required).
//!
//! DB-gated tests require `PGRG_TEST_DATABASE_URL` (port 5443) and skip
//! cleanly when unset so CI without a database is green.

use std::sync::Arc;

use pg_raggraph_core::embedding::EmbeddingBackend;
use pg_raggraph_sidecar::config::SidecarConfig;
use pg_raggraph_sidecar::embedder::build_embedder;
use pg_raggraph_sidecar::{bootstrap, db, http};
use tokio_postgres::Client;

fn test_db_url() -> Option<String> {
    std::env::var("PGRG_TEST_DATABASE_URL").ok()
}

/// Build a pgvector text literal `[v1,v2,...]` — byte-identical to the
/// handler's / `pg_client`'s `vector_literal` so the seeded chunk embedding
/// round-trips against the Rust-computed query embedding.
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

/// Clean slate + full pgrg.* bootstrap (seeds the `default` namespace via
/// `001_tables.sql`), then register the mock provider and seed one
/// document + chunk so retrieval returns exactly one row.
async fn reset_and_seed(c: &mut Client, embedder: &dyn EmbeddingBackend) {
    c.batch_execute("DROP SCHEMA IF EXISTS pgrg CASCADE;")
        .await
        .expect("reset schema");
    bootstrap::run_migrations(c)
        .await
        .expect("bootstrap schema");

    // Mock LLM provider. build_provider_impl maps `provider='mock'` to
    // MockProvider::with_stub_answer(credential), so the credential IS the
    // canned answer. The `[1]` makes _core::llm::ask emit one citation to
    // the first prompt chunk.
    c.execute(
        "INSERT INTO pgrg.providers (name, kind, provider, model, credential) \
         VALUES ('mock', 'llm', 'mock', 'mock-model', $1)",
        &[&"According to the source, the build passes [1]."],
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

    // One document + one chunk. The chunk text intentionally contains the
    // query terms so the BM25 lane also matches; the vector lane matches
    // regardless via the deterministic embedding.
    let doc_id: uuid::Uuid = c
        .query_one(
            "INSERT INTO pgrg.documents (namespace, source, content_hash, title) \
             VALUES ('default', 'http-ask-test', 'hash-http-ask-1', 'T15 fixture') \
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

#[tokio::test]
async fn post_v1_ask_happy_path() {
    let Some(url) = test_db_url() else {
        eprintln!("SKIP: PGRG_TEST_DATABASE_URL unset");
        return;
    };

    // Embedder built exactly as the handler builds it (dim 384, deterministic).
    let embedder: Arc<dyn EmbeddingBackend> = build_embedder(384, None).expect("build embedder");

    let mut setup = db::connect(&url).await.expect("connect for setup");
    reset_and_seed(&mut setup, embedder.as_ref()).await;

    // Config pointing at the test DB; http_bind is unused here because we
    // bind our own ephemeral listener and drive `router()` directly.
    let cfg = Arc::new(SidecarConfig::parse_from([
        "pg-raggraph-sidecar",
        "--database-url",
        &url,
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

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/v1/ask"))
        .json(&serde_json::json!({
            "q": "what does the sidecar expose?",
            "namespace": "default",
            "top_k": 10
        }))
        .send()
        .await
        .expect("POST /v1/ask");

    assert_eq!(resp.status().as_u16(), 200, "expected HTTP 200");
    let body: serde_json::Value = resp.json().await.expect("parse JSON body");

    // Parity shape: {answer, citations, signals, mode_used}.
    let answer = body["answer"].as_str().expect("answer is a string");
    assert!(!answer.is_empty(), "answer must be non-empty: {body}");

    assert_eq!(
        body["mode_used"].as_str(),
        Some("hybrid"),
        "mode_used must be hybrid: {body}"
    );

    let citations = body["citations"].as_array().expect("citations is an array");
    assert!(
        !citations.is_empty(),
        "expected at least one citation from the `[1]` stub: {body}"
    );

    // Every cited chunk_id must exist in pgrg.chunks (SC-010 spirit).
    for cit in citations {
        let cid = cit["chunk_id"].as_str().expect("citation.chunk_id string");
        let uuid: uuid::Uuid = cid.parse().expect("citation.chunk_id is a UUID");
        let exists: bool = setup
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pgrg.chunks WHERE id = $1)",
                &[&uuid],
            )
            .await
            .expect("chunk existence query")
            .get(0);
        assert!(exists, "cited chunk {cid} must exist in pgrg.chunks");
        assert!(
            cit["document_id"].as_str().is_some(),
            "citation must carry document_id"
        );
        assert!(cit["ord"].is_number(), "citation must carry ord");
    }

    // signals carries retrieval attribution (parity with pgrx ask.rs).
    assert!(
        body["signals"].is_object(),
        "signals must be a JSON object: {body}"
    );

    server.abort();
}

// ── Error-envelope tests (SC-013, Task 16) ──────────────────────────────────

/// Helper: spin up the router bound to an ephemeral port and return the
/// address. The caller holds the `JoinHandle` and should `.abort()` it when
/// done.
async fn spawn_server(url: &str) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
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
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    (addr, handle)
}

/// SC-013: A malformed JSON body must return HTTP 400 with
/// `code == "bad_request"`. The response body must contain NEITHER
/// `postgres://` NOR `password` (sanitization guarantee, DC-002).
#[tokio::test]
async fn malformed_body_returns_400() {
    let Some(url) = test_db_url() else {
        eprintln!("SKIP: PGRG_TEST_DATABASE_URL unset");
        return;
    };

    let (addr, server) = spawn_server(&url).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/ask"))
        .header("Content-Type", "application/json")
        .body("{ not json")
        .send()
        .await
        .expect("POST /v1/ask with malformed body");

    assert_eq!(
        resp.status().as_u16(),
        400,
        "expected HTTP 400 for malformed body"
    );

    let body_str = resp.text().await.expect("read body");
    let v: serde_json::Value =
        serde_json::from_str(&body_str).expect("400 body must be valid JSON");

    assert_eq!(
        v["code"].as_str(),
        Some("bad_request"),
        "code must be 'bad_request': {body_str}"
    );

    // Sanitization guarantee (DC-002): no credentials in the response body.
    assert!(
        !body_str.contains("postgres://"),
        "400 body must not contain 'postgres://': {body_str}"
    );
    assert!(
        !body_str.contains("password"),
        "400 body must not contain 'password': {body_str}"
    );

    server.abort();
}

/// SC-013: A request for a namespace that does not exist in `pgrg.namespaces`
/// must return HTTP 404 with `code == "unknown_namespace"`.
///
/// Uses an idempotent bootstrap (no DROP) to avoid conflicting with parallel
/// tests that also bootstrap the schema.
#[tokio::test]
async fn unknown_namespace_returns_404() {
    let Some(url) = test_db_url() else {
        eprintln!("SKIP: PGRG_TEST_DATABASE_URL unset");
        return;
    };

    // Idempotent bootstrap — no DROP to avoid racing with the happy-path test
    // that may be resetting the schema in parallel. If migrations fail (e.g.,
    // the happy-path test dropped and is recreating the schema concurrently),
    // we wait briefly and retry once; by then the schema should be stable.
    let mut setup = db::connect(&url).await.expect("connect for setup");
    if bootstrap::run_migrations(&mut setup).await.is_err() {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        // Reconnect after the brief pause (connection may have been closed).
        let mut setup2 = db::connect(&url).await.expect("reconnect for setup");
        bootstrap::run_migrations(&mut setup2)
            .await
            .expect("bootstrap schema (retry)");
    }

    let (addr, server) = spawn_server(&url).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/ask"))
        .json(&serde_json::json!({
            "q": "does this namespace exist?",
            "namespace": "does-not-exist-xyz"
        }))
        .send()
        .await
        .expect("POST /v1/ask with unknown namespace");

    assert_eq!(
        resp.status().as_u16(),
        404,
        "expected HTTP 404 for unknown namespace"
    );

    let body_str = resp.text().await.expect("read body");
    let v: serde_json::Value =
        serde_json::from_str(&body_str).expect("404 body must be valid JSON");

    assert_eq!(
        v["code"].as_str(),
        Some("unknown_namespace"),
        "code must be 'unknown_namespace': {body_str}"
    );
    assert_eq!(
        v["error"].as_str(),
        Some("unknown namespace"),
        "error must be 'unknown namespace': {body_str}"
    );

    server.abort();
}
