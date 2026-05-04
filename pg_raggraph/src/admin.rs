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
