//! Sidecar configuration: env-var-first, CLI flags mirror env names.
//!
//! Mirrors the in-extension GUC names so operators carry their mental model
//! over. No HTTP auth and no TLS in v1 — trusted private network behind a
//! reverse proxy (documented in `--help`).

use clap::Parser;

#[derive(Parser)]
#[command(
    name = "pg-raggraph-sidecar",
    about = "pg-raggraph sidecar for cloud-managed PostgreSQL (RDS/Cloud SQL/Supabase/Neon).",
    long_about = "Runs the pg-raggraph ingest + ask pipeline as a standalone process \
                  against managed PostgreSQL where shared_preload_libraries cannot be set. \
                  No HTTP auth and no TLS in v1 — run on a trusted private network behind \
                  a reverse proxy."
)]
pub struct SidecarConfig {
    /// `PostgreSQL` connection string. (`PGRG_DATABASE_URL`)
    #[arg(long, env = "PGRG_DATABASE_URL")]
    pub database_url: String,

    /// HTTP bind address for `POST /v1/ask`. (`PGRG_HTTP_BIND`)
    #[arg(long, env = "PGRG_HTTP_BIND", default_value = "0.0.0.0:8410")]
    pub http_bind: String,

    /// Number of sidecar ingest worker tasks. (`PGRG_BGW_WORKERS`)
    #[arg(long, env = "PGRG_BGW_WORKERS", default_value_t = 2)]
    pub bgw_workers: i32,

    /// Concurrent LLM extraction calls per worker. (`PGRG_EXTRACT_CONCURRENCY`)
    #[arg(long, env = "PGRG_EXTRACT_CONCURRENCY", default_value_t = 4)]
    pub extract_concurrency: i32,

    /// DB-wide embedding vector dimension. (`PGRG_EMBED_DIM`)
    #[arg(long, env = "PGRG_EMBED_DIM", default_value_t = 384)]
    pub embed_dim: i32,

    /// Embedding model path override for offline installs. (`PGRG_EMBED_MODEL_PATH`)
    #[arg(long, env = "PGRG_EMBED_MODEL_PATH")]
    pub embed_model_path: Option<String>,

    /// AES-GCM master key file for credential encryption. (`PGRG_MASTER_KEY_PATH`)
    #[arg(long, env = "PGRG_MASTER_KEY_PATH")]
    pub master_key_path: Option<String>,

    /// Seconds before a stuck running job is re-queued. (`PGRG_JOB_REAPER_INTERVAL`)
    #[arg(long, env = "PGRG_JOB_REAPER_INTERVAL", default_value_t = 300)]
    pub job_reaper_interval_secs: i64,

    /// Use `IVFFlat` instead of `HNSW` for deterministic parity. (`PGRG_PARITY_MODE`)
    #[arg(long, env = "PGRG_PARITY_MODE", default_value_t = false)]
    pub parity_mode: bool,
}

impl SidecarConfig {
    /// Parse config from an explicit iterator of arguments (used in tests).
    #[must_use]
    pub fn parse_from<I, T>(it: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        <Self as Parser>::parse_from(it)
    }
}

impl std::fmt::Debug for SidecarConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never leak the connection-string password.
        f.debug_struct("SidecarConfig")
            .field("database_url", &"<redacted>")
            .field("http_bind", &self.http_bind)
            .field("bgw_workers", &self.bgw_workers)
            .field("extract_concurrency", &self.extract_concurrency)
            .field("embed_dim", &self.embed_dim)
            .field("embed_model_path", &self.embed_model_path)
            .field(
                "master_key_path",
                &self.master_key_path.as_ref().map(|_| "<set>"),
            )
            .field("job_reaper_interval_secs", &self.job_reaper_interval_secs)
            .field("parity_mode", &self.parity_mode)
            .finish()
    }
}
