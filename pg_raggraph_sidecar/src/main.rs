//! pg-raggraph-sidecar — standalone binary for cloud-managed PostgreSQL.
//!
//! Real implementation lands in Plan 5. This crate exists in Plan 1 only so
//! the workspace builds end-to-end.

fn main() {
    eprintln!("pg-raggraph-sidecar v{}: not yet implemented (Plan 5)", env!("CARGO_PKG_VERSION"));
    std::process::exit(64); // EX_USAGE
}
