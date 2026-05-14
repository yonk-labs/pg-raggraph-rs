//! SC-002: `RetryingProvider` retries on transient (Retryable) errors up to a
//! bounded count; permanent errors fail fast; total wall-clock is bounded.

use pg_raggraph_core::llm::retry::RetryingProvider;
use pg_raggraph_core::llm::{Extraction, LlmProvider};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct CountingProvider {
    calls: AtomicUsize,
    behavior: Vec<Behavior>, // index = call number
}

#[derive(Clone)]
enum Behavior {
    Transient,
    Permanent,
    Ok,
}

impl CountingProvider {
    fn new(behaviors: Vec<Behavior>) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            behavior: behaviors,
        })
    }
}

impl LlmProvider for CountingProvider {
    fn extract(
        &self,
        _chunk_text: &str,
        _namespace: &str,
    ) -> pg_raggraph_core::CoreResult<Extraction> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        match self.behavior.get(n).unwrap_or(&Behavior::Ok) {
            Behavior::Transient => Err(pg_raggraph_core::CoreError::Http("503".into())),
            Behavior::Permanent => Err(pg_raggraph_core::CoreError::Llm("400 bad request".into())),
            Behavior::Ok => Ok(Extraction::default()),
        }
    }
}

#[test]
fn retries_transient_up_to_max_then_succeeds() {
    let inner = CountingProvider::new(vec![Behavior::Transient, Behavior::Transient, Behavior::Ok]);
    let wrapped = RetryingProvider::new(inner.clone())
        .with_max_attempts(3)
        .with_backoff_ms(&[10, 20]); // shortened for test speed
    let out = wrapped.extract("x", "ns").unwrap();
    assert_eq!(out.entities.len(), 0);
    assert_eq!(inner.calls.load(Ordering::SeqCst), 3);
}

#[test]
fn fails_fast_on_permanent_error() {
    let inner = CountingProvider::new(vec![Behavior::Permanent]);
    let wrapped = RetryingProvider::new(inner.clone())
        .with_max_attempts(3)
        .with_backoff_ms(&[10, 20]);
    let err = wrapped.extract("x", "ns").expect_err("permanent must fail");
    assert!(format!("{err}").contains("bad request"));
    assert_eq!(
        inner.calls.load(Ordering::SeqCst),
        1,
        "no retry on permanent"
    );
}

#[test]
fn gives_up_after_max_attempts_on_transient() {
    let inner = CountingProvider::new(vec![
        Behavior::Transient,
        Behavior::Transient,
        Behavior::Transient,
        Behavior::Transient,
    ]);
    let wrapped = RetryingProvider::new(inner.clone())
        .with_max_attempts(3)
        .with_backoff_ms(&[1, 2]);
    let err = wrapped.extract("x", "ns").expect_err("exhausted");
    assert!(format!("{err}").contains("503") || format!("{err}").contains("transient"));
    assert_eq!(inner.calls.load(Ordering::SeqCst), 3);
}

// ---------------------------------------------------------------------------
// Test scaffolding for complete() retry behavior. Reuses the existing
// CountingProvider pattern but with a complete()-returning behavior list.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct CompletingCountingProvider {
    calls: AtomicUsize,
    behavior: Vec<Behavior>,
}

impl CompletingCountingProvider {
    fn new(behaviors: Vec<Behavior>) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            behavior: behaviors,
        })
    }
}

impl LlmProvider for CompletingCountingProvider {
    fn extract(
        &self,
        _chunk_text: &str,
        _namespace: &str,
    ) -> pg_raggraph_core::CoreResult<Extraction> {
        unreachable!("CompletingCountingProvider only exercises complete()")
    }

    fn complete(
        &self,
        _prompt: &str,
    ) -> pg_raggraph_core::CoreResult<pg_raggraph_core::llm::Completion> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        match self.behavior.get(n).unwrap_or(&Behavior::Ok) {
            Behavior::Transient => Err(pg_raggraph_core::CoreError::Http("503".into())),
            Behavior::Permanent => Err(pg_raggraph_core::CoreError::Llm("400 bad request".into())),
            Behavior::Ok => Ok(pg_raggraph_core::llm::Completion {
                text: "ok".into(),
                prompt_tokens: 0,
                completion_tokens: 0,
            }),
        }
    }
}

#[test]
fn complete_retries_transient_then_succeeds() {
    let inner = CompletingCountingProvider::new(vec![Behavior::Transient, Behavior::Ok]);
    let wrapped = RetryingProvider::new(inner.clone())
        .with_max_attempts(3)
        .with_backoff_ms(&[10, 20]);
    let out = wrapped.complete("hello").unwrap();
    assert_eq!(out.text, "ok");
    assert_eq!(inner.calls.load(Ordering::SeqCst), 2);
}

#[test]
fn complete_fails_fast_on_permanent() {
    let inner = CompletingCountingProvider::new(vec![Behavior::Permanent]);
    let wrapped = RetryingProvider::new(inner.clone())
        .with_max_attempts(3)
        .with_backoff_ms(&[10, 20]);
    let err = wrapped.complete("hello").expect_err("permanent");
    assert!(format!("{err}").contains("bad request"));
    assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn respects_total_wall_clock_cap() {
    let inner = CountingProvider::new(vec![Behavior::Transient; 10]);
    let start = std::time::Instant::now();
    let wrapped = RetryingProvider::new(inner.clone())
        .with_max_attempts(10)
        .with_backoff_ms(&[100, 200, 400, 800, 1600])
        .with_total_cap_ms(500);
    let _ = wrapped.extract("x", "ns");
    let elapsed = start.elapsed();
    assert!(
        elapsed.as_millis() < 700,
        "total cap (500ms) breached: elapsed = {elapsed:?}"
    );
}
