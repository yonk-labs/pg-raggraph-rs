//! Retrieval engine: SQL builder, RRF fusion, undirected graph walk.
//!
//! Lives outside the pgrx crate so unit tests run with plain `cargo test`.
//! Per mission brief Constraint Always: all retrieval logic that is not
//! strictly pgrx FFI lives here.

pub mod mode;

pub use mode::Mode;
