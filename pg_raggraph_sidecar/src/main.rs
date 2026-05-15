//! pg-raggraph-sidecar — standalone binary for cloud-managed `PostgreSQL`.

use pg_raggraph_sidecar::config::SidecarConfig;
use pg_raggraph_sidecar::db::redact_conn_string;

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

    // Slice 2+ fills in: bootstrap().await?; then tokio::join!(jobloop, http_server).
    // For Slice 1 the binary validates config + logs + exits 0 so SC-001 is met.
    Ok(())
}
