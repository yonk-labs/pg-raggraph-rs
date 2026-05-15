use pg_raggraph_sidecar::db::redact_conn_string;

#[test]
fn redacts_password_in_uri() {
    assert_eq!(
        redact_conn_string("postgres://user:hunter2@host:5432/db"),
        "postgres://user:***@host:5432/db"
    );
    assert_eq!(redact_conn_string("postgresql://h/db"), "postgresql://h/db");
}
