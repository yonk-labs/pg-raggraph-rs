//! pg-raggraph-sidecar — standalone binary for cloud-managed `PostgreSQL`.
//!
//! Real startup wiring lands in Plan 5 Task 4. This entry point exists so the
//! binary compiles and `--help` is functional.

use clap::Parser;
use pg_raggraph_sidecar::config::SidecarConfig;

fn main() {
    let _config = SidecarConfig::parse();
    eprintln!(
        "pg-raggraph-sidecar v{}: not yet implemented (Plan 5 Task 4)",
        env!("CARGO_PKG_VERSION")
    );
    std::process::exit(64); // EX_USAGE — not yet implemented
}
