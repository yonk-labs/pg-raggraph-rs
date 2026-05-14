//! `RetryingProvider<P>` — wraps an `LlmProvider` with bounded retry.
//!
//! Retries only `CoreError::Http(...)`. Other errors (including `CoreError::Llm`)
//! fail fast (SC-002). Total wall-clock cap honors the brief's "must not hang
//! for more than ~30s" constraint.
//!
//! Default policy (production, pre-approved in session):
//! - max 3 total attempts
//! - 1s/2s/4s backoff between retries
//! - 10s total wall-clock cap

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::error::{CoreError, CoreResult};
use crate::llm::{Extraction, LlmProvider};

/// Fallback backoff (ms) used if `backoff_ms` is empty or attempt index exceeds it.
const FALLBACK_BACKOFF_MS: u64 = 1000;

pub struct RetryingProvider<P: LlmProvider> {
    inner: Arc<P>,
    max_attempts: usize,
    backoff_ms: Vec<u64>,
    total_cap_ms: u64,
}

impl<P: LlmProvider> RetryingProvider<P> {
    #[must_use]
    pub fn new(inner: Arc<P>) -> Self {
        Self {
            inner,
            max_attempts: 3,
            backoff_ms: vec![1000, 2000, 4000],
            total_cap_ms: 10_000,
        }
    }

    #[must_use]
    pub fn with_max_attempts(mut self, n: usize) -> Self {
        self.max_attempts = n.max(1);
        self
    }

    #[must_use]
    pub fn with_backoff_ms(mut self, b: &[u64]) -> Self {
        self.backoff_ms = b.to_vec();
        self
    }

    #[must_use]
    pub fn with_total_cap_ms(mut self, c: u64) -> Self {
        self.total_cap_ms = c;
        self
    }

    fn is_retryable(err: &CoreError) -> bool {
        matches!(err, CoreError::Http(_))
    }
}

impl<P: LlmProvider> LlmProvider for RetryingProvider<P> {
    fn extract(&self, chunk_text: &str, namespace: &str) -> CoreResult<Extraction> {
        let start = Instant::now();
        let cap = Duration::from_millis(self.total_cap_ms);
        let mut last_err: Option<CoreError> = None;
        for attempt in 0..self.max_attempts {
            if start.elapsed() >= cap {
                break;
            }
            match self.inner.extract(chunk_text, namespace) {
                Ok(v) => return Ok(v),
                Err(e) if Self::is_retryable(&e) => {
                    last_err = Some(e);
                    if attempt + 1 < self.max_attempts {
                        let sleep_ms = self.backoff_ms.get(attempt).copied().unwrap_or_else(|| {
                            self.backoff_ms
                                .last()
                                .copied()
                                .unwrap_or(FALLBACK_BACKOFF_MS)
                        });
                        let remaining = cap.saturating_sub(start.elapsed());
                        let sleep = Duration::from_millis(sleep_ms).min(remaining);
                        if sleep.is_zero() {
                            break;
                        }
                        std::thread::sleep(sleep);
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Err(last_err.unwrap_or_else(|| CoreError::Llm("retries exhausted".into())))
    }
}
