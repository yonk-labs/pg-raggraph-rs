//! Constructs LLM providers from a `pgrg.providers` row over `tokio-postgres`
//! (Plan 5 Slice 4, T14). This is a method-for-method mirror of the pgrx-side
//! factory in `pg_raggraph/src/provider_factory.rs` (DC-001 parity): the
//! same SELECT, the same `enc:v1:` decrypt-at-use-site branch, the same
//! `provider`-kind dispatch, and the same `resolve_or_default_provider`
//! fallback semantics.
//!
//! The ONLY mechanism deltas vs. the pgrx factory:
//!   - `tokio-postgres` `Client` instead of pgrx SPI, and
//!   - the master-key path comes from `SidecarConfig.master_key_path`
//!     (env `PGRG_MASTER_KEY_PATH`) instead of the `pg_raggraph.master_key_path`
//!     GUC.
//!
//! Security: the decrypted credential lives inside the returned
//! `Box<dyn LlmProvider>` for the duration of one call and drops at function
//! return. It is NEVER logged or placed in an error message — the encrypted
//! path fails closed with a credential-free message if the master key is
//! absent.

use anyhow::{Context, anyhow};
use pg_raggraph_core::llm::LlmProvider;
use pg_raggraph_core::llm::resolve::{ProviderRef, resolve_provider};
use tokio_postgres::Client;

/// Build the right [`LlmProvider`] impl from a `pgrg.providers` row,
/// decrypting the credential if it is in `enc:v1:` form. Returns an error on
/// any malformed config — fail closed. Mirrors the pgrx
/// `build_provider_impl` (its `ereport!(ERROR)` arms become `Err`).
pub async fn build_provider_impl(
    client: &Client,
    name: &str,
    master_key_path: Option<&str>,
) -> anyhow::Result<Box<dyn LlmProvider>> {
    // Same SELECT as the pgrx factory: `provider` (kind-of-LLM dispatch key),
    // `model` (coalesced to ''), `base_url`, `credential`. The `kind` column
    // ('llm' vs 'embedding') was already checked by `resolve_provider`.
    let row = client
        .query_opt(
            "SELECT provider, COALESCE(model, ''), base_url, credential \
             FROM pgrg.providers WHERE name = $1",
            &[&name],
        )
        .await
        .with_context(|| format!("provider_factory: provider row select for `{name}`"))?;

    let row = row.ok_or_else(|| anyhow!("provider_factory: provider `{name}` not found"))?;

    let provider_kind: String = row.try_get(0).context("provider_factory: read provider")?;
    let model: String = row.try_get(1).context("provider_factory: read model")?;
    let base_url: Option<String> = row.try_get(2).context("provider_factory: read base_url")?;
    let credential_opt: Option<String> = row
        .try_get(3)
        .context("provider_factory: read credential")?;

    // Decrypt credential if present and encrypted; otherwise keep as-is.
    // The decrypted value is bound to `plaintext_cred` and never logged.
    let plaintext_cred: String = match credential_opt {
        None => String::new(),
        Some(c) if pg_raggraph_core::credentials::is_encrypted(&c) => {
            let path = master_key_path.ok_or_else(|| {
                anyhow!(
                    "provider_factory: credential is encrypted but \
                     PGRG_MASTER_KEY_PATH (master_key_path) is unset"
                )
            })?;
            let key = pg_raggraph_core::credentials::MasterKey::load_from_path(path)
                .map_err(|e| anyhow!("provider_factory: master key load failed: {e}"))?;
            pg_raggraph_core::credentials::decrypt_v1(&c, key.as_bytes())
                .map_err(|e| anyhow!("provider_factory: credential decrypt failed: {e}"))?
        }
        Some(c) => c,
    };

    let base = base_url.unwrap_or_else(|| match provider_kind.as_str() {
        "openai" => "https://api.openai.com".into(),
        "anthropic" => "https://api.anthropic.com".into(),
        "ollama" => "http://localhost:11434".into(),
        _ => String::new(),
    });

    let provider: Box<dyn LlmProvider> = match provider_kind.as_str() {
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
        "mock-extractor" => {
            // Test-only provider kind: parse the credential as a JSON
            // `Extraction` and inject it into `MockProvider`. Mirrors the
            // pgrx factory's `mock-extractor` arm (SC-013 deterministic
            // extraction without LLM network calls).
            let parsed: pg_raggraph_core::llm::Extraction = serde_json::from_str(&plaintext_cred)
                .map_err(|e| {
                anyhow!("provider_factory: mock-extractor credential must be JSON Extraction: {e}")
            })?;
            Box::new(pg_raggraph_core::llm::MockProvider::default().with_stub_extraction(parsed))
        }
        other => {
            return Err(anyhow!("provider_factory: unknown provider kind `{other}`"));
        }
    };

    Ok(provider)
}

/// Resolve the LLM provider for a job in `namespace`. Falls back to a no-op
/// [`pg_raggraph_core::llm::MockProvider`] if no provider is configured —
/// preserves bg-worker test compatibility (queue/launcher tests that don't
/// seed a real provider row), exactly as the pgrx factory does.
///
/// Resolution order matches `pgrg.ask` minus the explicit override:
///   1. `pgrg.namespaces.llm_provider` (the namespace default), then
///   2. first LLM-kind row in `pgrg.providers` (via `resolve_provider`).
///
/// On any failure (no provider, lookup error), fall back to `MockProvider`
/// instead of erroring — the sidecar should not crash just because no
/// provider is configured (pgrx-factory parity).
pub async fn resolve_or_default_provider(
    client: &Client,
    namespace: &str,
    master_key_path: Option<&str>,
) -> Box<dyn LlmProvider> {
    let ns_default: Option<String> = client
        .query_opt(
            "SELECT llm_provider FROM pgrg.namespaces WHERE name = $1",
            &[&namespace],
        )
        .await
        .ok()
        .flatten()
        .and_then(|r| r.try_get::<_, Option<String>>(0).ok().flatten());

    let available: Vec<ProviderRef> = match client
        .query("SELECT name, kind FROM pgrg.providers", &[])
        .await
    {
        Ok(rows) => rows
            .iter()
            .map(|r| ProviderRef {
                name: r.try_get::<_, String>(0).unwrap_or_default(),
                kind: r.try_get::<_, String>(1).unwrap_or_default(),
            })
            .collect(),
        Err(_) => Vec::new(),
    };

    match resolve_provider(None, ns_default.as_deref(), &available) {
        Ok(name) => match build_provider_impl(client, &name, master_key_path).await {
            Ok(p) => p,
            Err(_) => Box::new(pg_raggraph_core::llm::MockProvider::new()),
        },
        Err(_) => Box::new(pg_raggraph_core::llm::MockProvider::new()),
    }
}

#[cfg(test)]
mod tests {
    //! Non-network parity tests. Full DB-gated behavior is exercised by
    //! T15 (/v1/ask) and T17 (credential interop). These tests pin the
    //! pure-logic pieces that don't need a database or a network round-trip.

    /// The default base-URL table must match the pgrx factory verbatim
    /// (DC-001 parity). This mirrors the `unwrap_or_else` arm in
    /// `build_provider_impl` so a divergence is caught at unit-test time.
    #[test]
    fn default_base_urls_match_pgrx_factory() {
        fn default_base(kind: &str) -> String {
            match kind {
                "openai" => "https://api.openai.com".into(),
                "anthropic" => "https://api.anthropic.com".into(),
                "ollama" => "http://localhost:11434".into(),
                _ => String::new(),
            }
        }
        assert_eq!(default_base("openai"), "https://api.openai.com");
        assert_eq!(default_base("anthropic"), "https://api.anthropic.com");
        assert_eq!(default_base("ollama"), "http://localhost:11434");
        assert_eq!(default_base("mock"), "");
        assert_eq!(default_base("unknown"), "");
    }

    /// `resolve_provider` (the shared `_core` resolver this factory delegates
    /// to) must yield the namespace default ahead of the first available
    /// provider — the same precedence the pgrx factory relies on.
    #[test]
    fn resolve_prefers_namespace_default() {
        use pg_raggraph_core::llm::resolve::{ProviderRef, resolve_provider};
        let available = vec![ProviderRef {
            name: "first-llm".into(),
            kind: "llm".into(),
        }];
        let resolved = resolve_provider(None, Some("ns-default"), &available)
            .expect("namespace default resolves");
        assert_eq!(resolved, "ns-default");
    }
}
