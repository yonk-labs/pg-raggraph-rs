//! Admin SQL functions: namespaces, providers, operational endpoints.

use pgrx::prelude::*;

#[pg_extern]
fn namespace_create(
    name: &str,
    embedding_model: default!(&str, "'bge-small-en-v1.5'"),
    llm_provider: default!(Option<&str>, "NULL"),
    settings: default!(pgrx::JsonB, "'{}'::jsonb"),
) {
    Spi::connect_mut(|client| {
        client
            .update(
                "INSERT INTO pgrg.namespaces (name, embedding_model, llm_provider, settings) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (name) DO UPDATE SET \
                     embedding_model = EXCLUDED.embedding_model, \
                     llm_provider    = EXCLUDED.llm_provider, \
                     settings        = EXCLUDED.settings",
                None,
                &[
                    name.into(),
                    embedding_model.into(),
                    llm_provider.into(),
                    settings.into(),
                ],
            )
            .expect("namespace_create insert failed");
    });
}

#[pg_extern]
fn namespace_drop(name: &str, cascade: default!(bool, "false")) {
    if cascade {
        Spi::connect_mut(|client| {
            client
                .update(
                    "DELETE FROM pgrg.namespaces WHERE name = $1",
                    None,
                    &[name.into()],
                )
                .expect("namespace_drop cascade failed");
        });
        return;
    }

    let has_docs: Option<bool> = Spi::get_one_with_args(
        "SELECT EXISTS(SELECT 1 FROM pgrg.documents WHERE namespace = $1)",
        &[name.into()],
    )
    .expect("namespace_drop: existence check failed");

    if has_docs.unwrap_or(false) {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_FOREIGN_KEY_VIOLATION,
            format!("namespace `{name}` has documents; pass cascade := true to delete")
        );
    }

    Spi::connect_mut(|client| {
        client
            .update(
                "DELETE FROM pgrg.namespaces WHERE name = $1",
                None,
                &[name.into()],
            )
            .expect("namespace_drop failed");
    });
}

#[pg_extern]
fn provider_create(
    name: &str,
    kind: &str,
    provider: &str,
    base_url: Option<&str>,
    model: Option<&str>,
    credential: Option<&str>,
    config: default!(pgrx::JsonB, "'{}'::jsonb"),
) {
    if !matches!(kind, "llm" | "embedding") {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            format!("provider kind must be 'llm' or 'embedding', got `{kind}`")
        );
    }
    Spi::connect_mut(|client| {
        client
            .update(
                "INSERT INTO pgrg.providers \
                   (name, kind, provider, base_url, model, credential, config) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7) \
                 ON CONFLICT (name) DO UPDATE SET \
                   kind       = EXCLUDED.kind, \
                   provider   = EXCLUDED.provider, \
                   base_url   = EXCLUDED.base_url, \
                   model      = EXCLUDED.model, \
                   credential = EXCLUDED.credential, \
                   config     = EXCLUDED.config",
                None,
                &[
                    name.into(),
                    kind.into(),
                    provider.into(),
                    base_url.into(),
                    model.into(),
                    credential.into(),
                    config.into(),
                ],
            )
            .expect("provider_create insert failed");
    });
}

#[pg_extern]
fn provider_drop(name: &str) {
    Spi::connect_mut(|client| {
        client
            .update(
                "DELETE FROM pgrg.providers WHERE name = $1",
                None,
                &[name.into()],
            )
            .expect("provider_drop failed");
    });
}

#[pg_extern]
fn provider_list() -> pgrx::JsonB {
    let rows: Vec<serde_json::Value> = Spi::connect(|client| {
        client
            .select(
                "SELECT name, kind, provider, base_url, model, credential, config \
                 FROM pgrg.providers ORDER BY name",
                None,
                &[],
            )
            .expect("provider_list select")
            .map(|r| {
                let credential_redacted = r
                    .get::<String>(6)
                    .ok()
                    .flatten()
                    .map(|c| pg_raggraph_core::credentials::redact(&c));
                serde_json::json!({
                    "name":       r.get::<String>(1).ok().flatten(),
                    "kind":       r.get::<String>(2).ok().flatten(),
                    "provider":   r.get::<String>(3).ok().flatten(),
                    "base_url":   r.get::<String>(4).ok().flatten(),
                    "model":      r.get::<String>(5).ok().flatten(),
                    "credential": credential_redacted,
                    "config":     r
                        .get::<pgrx::JsonB>(7)
                        .ok()
                        .flatten()
                        .map(|j| j.0)
                        .unwrap_or_else(|| serde_json::json!({})),
                })
            })
            .collect()
    });
    pgrx::JsonB(serde_json::Value::Array(rows))
}
