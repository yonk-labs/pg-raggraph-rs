//! Shared HTTP client behavior: status classification, error mapping, timeout.
//!
//! Foundation for SC-001 (`OpenAI`) and SC-002 (Anthropic / Ollama). The retry
//! policy itself lives in `RetryingProvider` (T10); here we only verify the
//! classification table and basic transport.

use pg_raggraph_core::llm::http::{HttpClassification, HttpClient};

#[test]
fn classify_429_as_retryable() {
    assert_eq!(
        HttpClassification::from_status(429),
        HttpClassification::Retryable
    );
}

#[test]
fn classify_500_502_503_504_as_retryable() {
    for s in [500, 502, 503, 504] {
        assert_eq!(
            HttpClassification::from_status(s),
            HttpClassification::Retryable,
            "status {s} should be retryable"
        );
    }
}

#[test]
fn classify_4xx_as_permanent_except_429() {
    for s in [400, 401, 403, 404, 422] {
        assert_eq!(
            HttpClassification::from_status(s),
            HttpClassification::Permanent,
            "status {s} should be permanent"
        );
    }
}

#[test]
fn classify_2xx_as_ok() {
    for s in [200, 201, 204] {
        assert_eq!(
            HttpClassification::from_status(s),
            HttpClassification::Ok,
            "status {s} should be ok"
        );
    }
}

#[test]
fn http_client_respects_request_timeout() {
    // Bind a TCP listener that accepts but never responds. The blocking
    // reqwest client must surface a timeout error (transport-level), not
    // succeed.
    //
    // We use a plain TCP listener instead of `mockito` for the timeout case
    // because mockito 1.x doesn't expose a "delay-the-response" knob that
    // works across versions, and the SC ("timeout-shaped error from a
    // slow/silent server within the configured budget") is identical either
    // way.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local_addr");
    // Spawn a thread that accepts and HOLDS connections without responding.
    // We must not drop the accepted stream — dropping it closes the socket
    // and reqwest will see "connection closed" instead of a read timeout.
    // The thread keeps streams alive in a Vec for the lifetime of the test.
    std::thread::spawn(move || {
        let mut held = Vec::new();
        loop {
            if let Ok((stream, _)) = listener.accept() {
                held.push(stream);
            }
        }
    });

    let timeout = std::time::Duration::from_millis(150);
    let client = HttpClient::with_timeout(timeout);
    let url = format!("http://{addr}/slow");
    let start = std::time::Instant::now();
    let err = client.get(&url).expect_err("must time out");
    let elapsed = start.elapsed();

    // The error must surface as a `CoreError::Http` (transport-level).
    // reqwest's top-level Display doesn't always contain the string
    // "timeout" — the timeout signal lives in the error source chain.
    // What we actually care about for the SC is:
    //   1. It's an `Http` variant (not e.g. `Json` or a panic).
    //   2. It bailed within the configured timeout window plus slack,
    //      not after the OS default connect/read timeout (60s+).
    assert!(
        matches!(err, pg_raggraph_core::error::CoreError::Http(_)),
        "expected CoreError::Http, got: {err:?}"
    );
    let upper_bound = timeout + std::time::Duration::from_millis(850);
    assert!(
        elapsed < upper_bound,
        "expected to bail out within ~{upper_bound:?}, took {elapsed:?}"
    );
}
