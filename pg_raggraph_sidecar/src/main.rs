//! pg-raggraph-sidecar — standalone binary for cloud-managed `PostgreSQL`.

use std::sync::Arc;
use std::time::Duration;

use pg_raggraph_core::embedding::EmbeddingBackend;
use pg_raggraph_sidecar::config::SidecarConfig;
use pg_raggraph_sidecar::db::redact_conn_string;
use pg_raggraph_sidecar::jobloop::{Backoff, ProcessOutcome, process_one, spawn_reaper};
use pg_raggraph_sidecar::{bootstrap, db, embedder};

/// One ingest worker task: poll `process_one` forever. An empty queue backs
/// off (1→5→…→30s); a processed job resets the backoff; a transient error is
/// logged and retried after a short fixed delay (the loop must never die).
async fn run_worker(
    worker_id: i32,
    database_url: String,
    embedder: Arc<dyn EmbeddingBackend>,
) -> ! {
    let mut backoff = Backoff::new();
    loop {
        match process_one(&database_url, &embedder, tokio::runtime::Handle::current()).await {
            Ok(ProcessOutcome::Idle) => {
                tokio::time::sleep(backoff.next_delay()).await;
            }
            Ok(ProcessOutcome::Completed(id)) => {
                backoff.reset();
                tracing::debug!(worker = worker_id, %id, "job completed");
            }
            Ok(ProcessOutcome::Failed { id, error }) => {
                backoff.reset();
                tracing::warn!(worker = worker_id, %id, error, "job failed");
            }
            Err(e) => {
                tracing::error!(worker = worker_id, "process_one error: {e:?}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = SidecarConfig::parse_from(std::env::args_os());
    tracing::info!(
        db = %redact_conn_string(&cfg.database_url),
        http_bind = %cfg.http_bind,
        workers = cfg.bgw_workers,
        "pg-raggraph-sidecar starting"
    );

    // Bootstrap the pgrg.* schema ONCE before any worker claims a job.
    let mut boot = db::connect(&cfg.database_url).await?;
    bootstrap::run_migrations(&mut boot).await?;

    // Embedding backend loaded once, shared across all workers via Arc.
    let embedder = embedder::build_embedder(cfg.embed_dim, cfg.embed_model_path.as_deref())?;

    // Spawn the ingest worker pool. Each worker owns its own backoff and opens
    // its own control connection per `process_one` call (established design).
    for worker_id in 0..cfg.bgw_workers {
        let url = cfg.database_url.clone();
        let emb = Arc::clone(&embedder);
        tokio::spawn(run_worker(worker_id, url, emb));
    }

    // Crashed-job reaper (SC-009).
    let _reaper = spawn_reaper(cfg.database_url.clone(), cfg.job_reaper_interval_secs);

    // Workers + reaper are detached tasks; process exit stops them. v1 is a
    // clean ctrl_c exit — no drain protocol (out of scope).
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown");
    Ok(())
}
