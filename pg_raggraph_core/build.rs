//! Compile-time bridge for the shared cross-impl resolution constants.
//! Reads ../bench/parity/resolution_constants.yaml (flat `key: value`),
//! emits OUT_DIR/parity_constants_gen.rs. A value that disagrees with the
//! canonical Rust literals below is a BUILD ERROR — that IS the spec §10
//! "drift between the two files is a build-time error" guarantee.

use std::{env, fs, path::Path};

// Canonical literals. Must equal resolve.rs. If you change resolution
// behavior, change BOTH these and the YAML in the same commit.
const CANON_TRGM: f32 = 0.85;
const CANON_COSINE: f32 = 0.90;

fn scan(yaml: &str, key: &str) -> f32 {
    for line in yaml.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            if k.trim() == key {
                return v
                    .trim()
                    .parse::<f32>()
                    .unwrap_or_else(|_| panic!("parity yaml: `{key}` not a float"));
            }
        }
    }
    panic!("parity yaml: key `{key}` not found");
}

fn main() {
    let yaml_path = "../bench/parity/resolution_constants.yaml";
    println!("cargo:rerun-if-changed={yaml_path}");
    println!("cargo:rerun-if-changed=src/ingest/resolve.rs");

    let yaml = fs::read_to_string(yaml_path)
        .unwrap_or_else(|e| panic!("cannot read {yaml_path}: {e}"));

    let trgm = scan(&yaml, "trgm_merge");
    let cosine = scan(&yaml, "cosine_merge");

    if trgm.to_bits() != CANON_TRGM.to_bits() {
        panic!(
            "DRIFT: resolution_constants.yaml trgm_merge={trgm} != canonical \
             {CANON_TRGM}. Reconcile resolve.rs and the YAML in one commit."
        );
    }
    if cosine.to_bits() != CANON_COSINE.to_bits() {
        panic!(
            "DRIFT: resolution_constants.yaml cosine_merge={cosine} != canonical \
             {CANON_COSINE}. Reconcile resolve.rs and the YAML in one commit."
        );
    }

    let out = Path::new(&env::var("OUT_DIR").unwrap()).join("parity_constants_gen.rs");
    fs::write(
        &out,
        format!(
            "pub const TRGM_MERGE: f32 = {trgm}_f32;\n\
             pub const COSINE_MERGE: f32 = {cosine}_f32;\n"
        ),
    )
    .unwrap();
}
