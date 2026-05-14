//! SC-011: provider resolution chain
//!   explicit param → `namespace.llm_provider` → first matching `pgrg.providers` → error.

use pg_raggraph_core::llm::resolve::{ProviderRef, resolve_provider};

#[test]
fn resolves_to_explicit_param_when_present() {
    let providers = vec![ProviderRef {
        name: "default-p".into(),
        kind: "llm".into(),
    }];
    let result = resolve_provider(Some("explicit-p"), None, &providers).unwrap();
    assert_eq!(result, "explicit-p");
}

#[test]
fn falls_through_to_namespace_when_no_explicit() {
    let providers = vec![ProviderRef {
        name: "default-p".into(),
        kind: "llm".into(),
    }];
    let result = resolve_provider(None, Some("ns-p"), &providers).unwrap();
    assert_eq!(result, "ns-p");
}

#[test]
fn falls_through_to_first_llm_provider_when_no_explicit_or_namespace() {
    let providers = vec![
        ProviderRef {
            name: "embed-p".into(),
            kind: "embedding".into(),
        },
        ProviderRef {
            name: "llm-p".into(),
            kind: "llm".into(),
        },
        ProviderRef {
            name: "other-llm".into(),
            kind: "llm".into(),
        },
    ];
    let result = resolve_provider(None, None, &providers).unwrap();
    assert_eq!(result, "llm-p");
}

#[test]
fn errors_when_no_llm_provider_at_all() {
    let providers = vec![ProviderRef {
        name: "embed-p".into(),
        kind: "embedding".into(),
    }];
    let err = resolve_provider(None, None, &providers).expect_err("none");
    assert!(format!("{err}").contains("no LLM provider"));
}
