//! Background worker registration (called from `_PG_init`).
//!
//! Mission brief SC-001: registration only when
//! `process_shared_preload_libraries_in_progress`.
//! Mission brief SC-002: `pg_raggraph.bgw_workers` GUC controls worker count.
//! Mission brief SC-009: `embedder_cache` loads the embedding backend once
//! per worker process; the `spi_client` adapter bridges
//! `_core::ingest::run_job` to real PG via pgrx SPI.

pub(crate) mod embedder_cache;
pub mod launcher;
pub(crate) mod queue;
pub(crate) mod spi_client;
pub mod worker;

pub use launcher::register_launcher;
pub use worker::register_workers;
