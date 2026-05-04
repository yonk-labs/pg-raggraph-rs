//! `pg_raggraph_core` — provider-agnostic logic for the `pg_raggraph` extension.
//!
//! Has no pgrx dependency; testable with plain `cargo test`. Used by both the
//! extension crate (linked into the .so) and the sidecar binary.

pub mod error;
pub mod types;

pub use error::{CoreError, CoreResult};
pub use types::*;
