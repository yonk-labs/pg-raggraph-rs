//! Background worker registration (called from `_PG_init`).
//!
//! Mission brief SC-001: registration only when
//! `process_shared_preload_libraries_in_progress`.
//! Mission brief SC-002: `pg_raggraph.bgw_workers` GUC controls worker count.

pub mod launcher;
pub mod worker;

pub use launcher::register_launcher;
pub use worker::register_workers;
