//! Ingest pipeline: per-document transaction, profile knobs, source DTOs.
//!
//! Lives outside the pgrx crate so unit tests run with plain `cargo test`.
//! Per mission brief Constraint Always: bg worker code that touches PG goes
//! through pgrx SPI / connection helpers; `_core` stays PG-agnostic and uses
//! an injected `PgClient`-like trait so it can be unit-tested without a server.

pub mod content_hash;
pub mod pg_client;
pub mod profile;
pub mod run;
pub mod types;

pub use profile::IngestProfile;
pub use run::{RunJobOutcome, run_job};
pub use types::{IngestJob, IngestRequest, IngestSource};
