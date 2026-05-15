//! SC-008: the sidecar must have ZERO pgrx in its dependency tree, or it
//! cannot deploy on managed PG. This test shells out to `cargo tree` and
//! asserts no pgrx-family crate is reachable from pg_raggraph_sidecar.

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
