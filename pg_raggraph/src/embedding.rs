//! `pgrg.embed` SQL function — thin pgrx wrapper over `pg_raggraph_core::embedding`.
//!
//! Returns a pgvector `Vector` of dimension `pg_raggraph.embed_dim`. Plan 2
//! uses the deterministic hash-derived embedder; Plan 3 swaps the real
//! ONNX model in behind this same SQL surface.
//!
//! pgrx 0.17 has no native pgvector type binding, so the Rust function
//! returns a `text` literal of the form `[v1,v2,...]` and a thin SQL
//! wrapper (`pgrg.embed`) casts it to `public.vector`. The Rust-side
//! function is exposed as `pgrg._embed_text` (Plan 1 underscore convention
//! for internal helpers) and is an internal detail.

use pgrx::prelude::*;

/// Build a pgvector text literal of the form `[v1,v2,...]` from an f32 slice.
/// Returning the text and casting in SQL avoids depending on a pgvector pgrx
/// type binding (none ships in pgrx 0.17).
fn vector_literal(v: &[f32]) -> String {
    let mut s = String::with_capacity(v.len() * 8 + 2);
    s.push('[');
    let mut first = true;
    for x in v {
        if !first {
            s.push(',');
        }
        first = false;
        // {x} on f32 gives a round-trip representation; pgvector's parser accepts it.
        s.push_str(&format!("{x}"));
    }
    s.push(']');
    s
}

/// `pgrg._embed_text(text, namespace)` — internal text form of the embedder.
///
/// Returns the embedding as a `[v1,v2,...]` literal that pgvector parses.
/// User-facing callers should use `pgrg.embed(...)` which casts to `vector`.
/// Leading underscore follows Plan 1 convention for internal SQL helpers
/// (e.g., `pgrg._maybe_apply_parity_indexes`).
#[pg_extern]
fn _embed_text(text: &str, _namespace: default!(&str, "'default'")) -> String {
    // EMBED_DIM is i32 GUC bounded [64, 4096] (see gucs.rs); always non-negative.
    let dim_i32 = crate::gucs::EMBED_DIM.get();
    let dim: usize = usize::try_from(dim_i32).unwrap_or(384);
    let v = pg_raggraph_core::embedding::deterministic_embed(text, dim);
    vector_literal(&v)
}

// SQL-only wrapper: `pgrg.embed(text, namespace) RETURNS public.vector`.
//
// Casts the text literal returned by `pgrg._embed_text` to `public.vector`.
// Keeps `pgrg.embed` as the user-facing surface (mission brief SC-002).
::pgrx::extension_sql!(
    r#"
    CREATE FUNCTION pgrg.embed(
        "text" text,
        "namespace" text DEFAULT 'default'
    ) RETURNS public.vector
    LANGUAGE sql
    IMMUTABLE STRICT
    AS $$
        SELECT pgrg._embed_text("text", "namespace")::public.vector
    $$;
    "#,
    name = "embed_vector_wrapper",
    requires = [_embed_text]
);
