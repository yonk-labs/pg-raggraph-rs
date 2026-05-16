//! SC-005 / SC-016: `POST /v1/ask` happy path end-to-end against a real
//! managed-style Postgres. Bootstraps the schema, registers a `mock` LLM
//! provider whose stub answer contains a `[1]` marker (so `_core::llm::ask`
//! resolves one citation), seeds one document + one chunk with a
//! Rust-computed embedding (the same `EmbeddingBackend` the handler uses for
//! the query, so the vector lane matches), starts the axum router on an
//! ephemeral port, and asserts the parity JSON shape.
//!
//! Gated by `PGRG_TEST_DATABASE_URL` (port 5443). SKIPs cleanly when unset
//! so CI without a database is green.
//!
//! Mirrors `pg_raggraph` Plan 4's `e2e_three_statement_demo` assertions in
//! spirit: 200, non-empty `answer`, `citations` ⊆ `pgrg.chunks`,
//! `mode_used == "hybrid"`.

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
