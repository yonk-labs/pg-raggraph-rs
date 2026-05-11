//! Profile -> concurrency resolution.
//!
//! SC-014: profile resolution. The resolver prefers the profile (if any) and
//! falls back to the GUC default.

use crate::ingest::IngestProfile;

/// Resolve the effective `extract_concurrency` for a job.
///
/// Returns the profile's value when `profile` is `Some`, otherwise the
/// `guc_default` (which the caller passes as `pgrg.extract_concurrency`).
#[must_use]
pub fn resolve_concurrency(profile: Option<IngestProfile>, guc_default: u32) -> u32 {
    profile.map_or(guc_default, IngestProfile::extract_concurrency)
}
