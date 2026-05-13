//! Display redaction for credentials. Keeps a short prefix (sk-, key-) visible
//! while hiding the secret body. Character-aware indexing so multi-byte
//! UTF-8 inputs do not panic.

/// Redact a credential string for display. Returns "***" for inputs of 3 chars
/// or fewer; otherwise "<first 3 chars>***".
#[must_use]
pub fn redact(credential: &str) -> String {
    if credential.chars().count() <= 3 {
        return "***".to_string();
    }
    let cutoff = credential
        .char_indices()
        .nth(3)
        .map_or(credential.len(), |(i, _)| i);
    format!("{}***", &credential[..cutoff])
}
