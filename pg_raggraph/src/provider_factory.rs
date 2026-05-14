//! Shared LLM provider construction — used by both the `pgrg.ask` SQL
//! surface (`ask.rs`) and the background worker job loop (`bgw/worker.rs`).
//!
//! T23 (Plan 4): extracted from `ask.rs` so the bg worker can build the
//! same real-provider `Box<dyn LlmProvider>` per job as the synchronous
//! ask path does. Single source of truth for:
//!   - reading the `pgrg.providers` row,
//!   - decrypting the credential at use-site (`enc:v1:` -> plaintext),
//!   - dispatching on the `provider` kind column.
//!
//! Security: the decrypted credential lives inside the returned
//! `Box<dyn LlmProvider>` for the duration of one call (ask or one bg job)
//! and drops at function return. It is never logged or returned in error
//! messages.

use pg_raggraph_core::llm::LlmProvider;
use pg_raggraph_core::llm::resolve::{ProviderRef, resolve_provider};
use pgrx::prelude::*;

/// Look up the provider's model name from `pgrg.providers`. Used for
/// `signals.llm.model` attribution by `pgrg.ask`.
pub(crate) fn provider_model_for(name: &str) -> String {
    Spi::get_one_with_args(
        "SELECT model FROM pgrg.providers WHERE name = $1",
        &[name.into()],
    )
    .ok()
    .flatten()
    .unwrap_or_default()
}

/// Build the right `LlmProvider` impl from a `pgrg.providers` row,
/// decrypting the credential if it is in `enc:v1:` form.
/// `ereport!(ERROR)` on any malformed config — fail closed.
pub(crate) fn build_provider_impl(name: &str) -> Box<dyn LlmProvider> {
    // Fetch the row from pgrg.providers. We need `provider` (kind-of-LLM:
    // 'openai'/'anthropic'/'ollama'/'mock'), `model`, `base_url`,
    // `credential`. The `kind` column ('llm' vs 'embedding') was already
    // checked in resolve_provider.
    let row: Vec<(String, String, Option<String>, Option<String>)> = Spi::connect(|client| {
        client
            .select(
                "SELECT provider, COALESCE(model, ''), base_url, credential \
                 FROM pgrg.providers WHERE name = $1",
                None,
                &[name.into()],
            )
            .expect("provider_factory: provider row select")
            .map(|r| {
                (
                    r.get::<String>(1).ok().flatten().unwrap_or_default(),
                    r.get::<String>(2).ok().flatten().unwrap_or_default(),
                    r.get::<String>(3).ok().flatten(),
                    r.get::<String>(4).ok().flatten(),
                )
            })
            .collect()
    });

    let (provider_kind, model, base_url, credential_opt) = match row.into_iter().next() {
        Some(t) => t,
        None => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_NO_DATA_FOUND,
                format!("provider_factory: provider `{name}` not found")
            );
        }
    };

    // Decrypt credential if present and encrypted; otherwise keep as-is.
    let plaintext_cred: String = match credential_opt {
        None => String::new(),
        Some(c) if pg_raggraph_core::credentials::is_encrypted(&c) => {
            let path = match crate::gucs::MASTER_KEY_PATH.get() {
                Some(p) => p,
                None => {
                    ereport!(
                        ERROR,
                        PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
                        "provider_factory: credential is encrypted but pg_raggraph.master_key_path is unset"
                    );
                }
            };
            let path_str = path.to_string_lossy().into_owned();
            let key = match pg_raggraph_core::credentials::MasterKey::load_from_path(&path_str) {
                Ok(k) => k,
                Err(e) => {
                    ereport!(
                        ERROR,
                        PgSqlErrorCode::ERRCODE_CONFIG_FILE_ERROR,
                        format!("provider_factory: master key load failed: {e}")
                    );
                }
            };
            match pg_raggraph_core::credentials::decrypt_v1(&c, key.as_bytes()) {
                Ok(p) => p,
                Err(e) => {
                    ereport!(
                        ERROR,
                        PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                        format!("provider_factory: credential decrypt failed: {e}")
                    );
                }
            }
        }
        Some(c) => c,
    };

    let base = base_url.unwrap_or_else(|| match provider_kind.as_str() {
        "openai" => "https://api.openai.com".into(),
        "anthropic" => "https://api.anthropic.com".into(),
        "ollama" => "http://localhost:11434".into(),
        _ => String::new(),
    });

    match provider_kind.as_str() {
        "openai" => Box::new(pg_raggraph_core::llm::openai::OpenAiProvider::new(
            plaintext_cred,
            model,
            base,
        )),
        "anthropic" => Box::new(pg_raggraph_core::llm::anthropic::AnthropicProvider::new(
            plaintext_cred,
            model,
            base,
        )),
        "ollama" => Box::new(pg_raggraph_core::llm::ollama::OllamaProvider::new(
            model, base,
        )),
        "mock" => Box::new(
            pg_raggraph_core::llm::MockProvider::default().with_stub_answer(plaintext_cred),
        ),
        other => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
                format!("provider_factory: unknown provider kind `{other}`")
            );
        }
    }
}

/// Resolve the LLM provider for a job in `namespace`. Falls back to a
/// no-op `MockProvider` if no provider is configured — preserves Plan 3
/// bg-worker test compatibility (queue/launcher tests that don't seed a
/// real provider row).
///
/// Resolution order matches `pgrg.ask` minus the explicit override:
///   1. `pgrg.namespaces.llm_provider` (the namespace default), then
///   2. first LLM-kind row in `pgrg.providers` (via `resolve_provider`).
///
/// On failure, fall back to MockProvider instead of erroring — the bg
/// worker should not crash just because no provider is configured.
pub(crate) fn resolve_or_default_provider(namespace: &str) -> Box<dyn LlmProvider> {
    let ns_default: Option<String> = Spi::get_one_with_args(
        "SELECT llm_provider FROM pgrg.namespaces WHERE name = $1",
        &[namespace.into()],
    )
    .ok()
    .flatten();

    let available: Vec<ProviderRef> = Spi::connect(|client| {
        client
            .select("SELECT name, kind FROM pgrg.providers", None, &[])
            .expect("provider_factory: providers select")
            .map(|r| ProviderRef {
                name: r.get::<String>(1).ok().flatten().unwrap_or_default(),
                kind: r.get::<String>(2).ok().flatten().unwrap_or_default(),
            })
            .collect()
    });

    match resolve_provider(None, ns_default.as_deref(), &available) {
        Ok(name) => build_provider_impl(&name),
        Err(_) => Box::new(pg_raggraph_core::llm::MockProvider::new()),
    }
}
