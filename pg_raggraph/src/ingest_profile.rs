//! `pgrg.set_ingest_profile(namespace, profile)` — write `ingest_profile`
//! into `pgrg.namespaces.settings`.
//!
//! Profile is read from `pgrg.namespaces.settings->>'ingest_profile'` by the
//! bg worker (Plan 4 wires it into real `extract_concurrency` for LLM calls;
//! Plan 3 ships the surface).

use pg_raggraph_core::ingest::IngestProfile;
use pgrx::prelude::*;

#[pg_extern]
fn set_ingest_profile(namespace: &str, profile: &str) {
    if IngestProfile::parse(profile).is_none() {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            format!(
                "pgrg.set_ingest_profile: unknown profile '{profile}'; \
                 valid: conservative|balanced|aggressive|max"
            )
        );
    }
    Spi::connect_mut(|client| {
        client
            .update(
                "UPDATE pgrg.namespaces \
                 SET settings = COALESCE(settings, '{}'::jsonb) \
                              || jsonb_build_object('ingest_profile', $2) \
                 WHERE name = $1",
                None,
                &[namespace.into(), profile.into()],
            )
            .expect("pgrg.set_ingest_profile: update failed");
    });
}
