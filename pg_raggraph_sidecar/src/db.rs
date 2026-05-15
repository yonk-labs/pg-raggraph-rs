//! tokio-postgres connection helpers + connection-string redaction.

/// Redact the password component of a libpq URI for safe logging.
/// `scheme://user:PASS@host/db` → `scheme://user:***@host/db`.
#[must_use]
pub fn redact_conn_string(s: &str) -> String {
    // Only the `user:pass@` form carries an inline password.
    let Some(scheme_end) = s.find("://") else {
        return s.to_string();
    };
    let (scheme, rest) = s.split_at(scheme_end + 3);
    let Some(at) = rest.find('@') else {
        return s.to_string();
    };
    let (authority, tail) = rest.split_at(at);
    match authority.split_once(':') {
        Some((user, _pw)) => format!("{scheme}{user}:***{tail}"),
        None => s.to_string(),
    }
}
