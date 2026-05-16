//! SC-008: the sidecar must have ZERO pgrx in its dependency tree, or it
//! cannot deploy on managed PG. This test shells out to `cargo tree` and
//! asserts no pgrx-family crate is reachable from `pg_raggraph_sidecar`.

use std::process::Command;

#[test]
fn sidecar_dependency_tree_has_no_pgrx() {
    let out = Command::new(env!("CARGO"))
        .args([
            "tree",
            "-p",
            "pg_raggraph_sidecar",
            "--edges",
            "normal",
            "--prefix",
            "none",
        ])
        .output()
        .expect("cargo tree");
    assert!(
        out.status.success(),
        "cargo tree failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let tree = String::from_utf8_lossy(&out.stdout);
    let offending: Vec<&str> = tree
        .lines()
        .filter(|l| {
            let name = l.split_whitespace().next().unwrap_or("");
            name == "pgrx" || name.starts_with("pgrx-") || name == "pgrx_embed"
        })
        .collect();
    assert!(
        offending.is_empty(),
        "pgrx reachable from sidecar (breaks managed-PG deploy): {offending:?}"
    );
}

#[test]
fn sidecar_depends_on_pg_raggraph_core() {
    // SC-016: the sidecar runs the SAME _core code, not a fork.
    let out = Command::new(env!("CARGO"))
        .args(["tree", "-p", "pg_raggraph_sidecar", "--prefix", "none"])
        .output()
        .expect("cargo tree");
    let tree = String::from_utf8_lossy(&out.stdout);
    assert!(
        tree.lines()
            .any(|l| l.split_whitespace().next() == Some("pg_raggraph_core")),
        "sidecar must depend on pg_raggraph_core"
    );
}

#[test]
fn release_binary_has_no_pg_backend_symbols() {
    use std::process::Command;
    // Ensure the release binary exists (build it; cheap if already built).
    let build = Command::new(env!("CARGO"))
        .args(["build", "--release", "-p", "pg_raggraph_sidecar"])
        .output()
        .expect("cargo build --release");
    assert!(
        build.status.success(),
        "release build failed: {}",
        String::from_utf8_lossy(&build.stderr)
    );

    // Locate the binary relative to CARGO_MANIFEST_DIR (workspace target/).
    let bin = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../target/release/pg-raggraph-sidecar"
    );
    if !std::path::Path::new(bin).exists() {
        eprintln!("SKIP: release binary not found at {bin}");
        return;
    }
    // `nm` may be unavailable in minimal environments — SKIP if so (the
    // T1 cargo-tree guard is the always-on pgrx-free assertion).
    let nm = Command::new("nm").arg(bin).output();
    let Ok(nm) = nm else {
        eprintln!("SKIP: `nm` unavailable; cargo-tree guard (other test) still enforces pgrx-free");
        return;
    };
    if !nm.status.success() {
        eprintln!(
            "SKIP: nm exited non-zero (stripped binary?); cargo-tree guard still enforces pgrx-free"
        );
        return;
    }
    let syms = String::from_utf8_lossy(&nm.stdout);
    let offenders: Vec<&str> = syms
        .lines()
        .filter(|l| {
            let u = l.to_ascii_lowercase();
            u.contains("_pg_init")
                || u.contains("spi_connect")
                || u.contains("spi_execute")
                || u.contains("pgrx")
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "release binary references PG backend internals (breaks managed-PG deploy): {offenders:?}"
    );
}
