//! Provider resolution chain: explicit -> namespace -> first match -> error.
//!
//! SC-011. Inputs come from the pgrx adapter:
//! - `explicit`: passed by the SQL caller (`pgrg.ask(..., llm_provider := 'name')`)
//! - `namespace_default`: read from `pgrg.namespaces.llm_provider` for the active namespace
//! - `available`: read from `pgrg.providers` (all rows, filtered to `kind = 'llm'` by this fn)

use crate::error::{CoreError, CoreResult};

/// One row's worth of metadata about a configured provider. Built by the
/// pgrx adapter from `pgrg.providers` rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRef {
    pub name: String,
    pub kind: String, // "llm" | "embedding"
}

/// Pick the provider name to use for an `ask` call. Returns the resolved
/// provider name (caller looks up the full row separately).
///
/// # Errors
/// Returns `CoreError::InvalidConfig` when there is no LLM-kind provider
/// configured AND no explicit/namespace override.
pub fn resolve_provider(
    explicit: Option<&str>,
    namespace_default: Option<&str>,
    available: &[ProviderRef],
) -> CoreResult<String> {
    if let Some(e) = explicit {
        return Ok(e.to_string());
    }
    if let Some(ns) = namespace_default {
        return Ok(ns.to_string());
    }
    available
        .iter()
        .find(|p| p.kind == "llm")
        .map(|p| p.name.clone())
        .ok_or_else(|| CoreError::InvalidConfig("no LLM provider configured".into()))
}
