//! Operator-level GUCs registered at extension startup.
//!
//! See design-spec Section 7. Per-tenant settings live in `pgrg.providers` /
//! `pgrg.namespaces`, NOT in GUCs.

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CString;

pub static BGW_WORKERS: GucSetting<i32> = GucSetting::<i32>::new(2);
pub static EXTRACT_CONCURRENCY: GucSetting<i32> = GucSetting::<i32>::new(4);
pub static EMBED_DIM: GucSetting<i32> = GucSetting::<i32>::new(384);
pub static DEBUG_RETRIEVAL: GucSetting<bool> = GucSetting::<bool>::new(false);
pub static JOB_REAPER_INTERVAL_SECS: GucSetting<i32> = GucSetting::<i32>::new(300);
pub static PARITY_MODE: GucSetting<bool> = GucSetting::<bool>::new(false);
pub static MASTER_KEY_PATH: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
pub static EMBED_MODEL_PATH: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);

pub fn register() {
    GucRegistry::define_int_guc(
        c"pg_raggraph.bgw_workers",
        c"Number of pg_raggraph background worker processes",
        c"Set in postgresql.conf and restart. Per design-spec §7.",
        &BGW_WORKERS,
        1,
        16,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_raggraph.extract_concurrency",
        c"Concurrent LLM extraction calls per worker",
        c"",
        &EXTRACT_CONCURRENCY,
        1,
        64,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_raggraph.embed_dim",
        c"DB-wide vector dimension; must be set before CREATE EXTENSION",
        c"Default 384 matches BAAI/bge-small-en-v1.5.",
        &EMBED_DIM,
        64,
        4096,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"pg_raggraph.debug_retrieval",
        c"Populate signals jsonb in pgrg.query results",
        c"",
        &DEBUG_RETRIEVAL,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_raggraph.job_reaper_interval",
        c"Seconds between reaper sweeps for stuck running jobs",
        c"",
        &JOB_REAPER_INTERVAL_SECS,
        10,
        86_400,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"pg_raggraph.parity_mode",
        c"Use IVFFlat instead of HNSW for deterministic parity benchmarks",
        c"",
        &PARITY_MODE,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"pg_raggraph.master_key_path",
        c"File path to AES-GCM master key for credential encryption",
        c"",
        &MASTER_KEY_PATH,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"pg_raggraph.embed_model_path",
        c"Override embedding model location for offline installs",
        c"",
        &EMBED_MODEL_PATH,
        GucContext::Sighup,
        GucFlags::default(),
    );
}
