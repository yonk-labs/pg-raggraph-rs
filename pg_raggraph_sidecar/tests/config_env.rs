use pg_raggraph_sidecar::config::SidecarConfig;

#[test]
fn config_parses_from_explicit_args() {
    let c = SidecarConfig::parse_from([
        "pg-raggraph-sidecar",
        "--database-url",
        "postgres://u:p@h/db",
        "--http-bind",
        "0.0.0.0:8410",
        "--bgw-workers",
        "4",
        "--embed-dim",
        "384",
    ]);
    assert_eq!(c.database_url, "postgres://u:p@h/db");
    assert_eq!(c.http_bind, "0.0.0.0:8410");
    assert_eq!(c.bgw_workers, 4);
    assert_eq!(c.embed_dim, 384);
    assert_eq!(c.job_reaper_interval_secs, 300); // default
}

#[test]
fn config_redacts_database_url_in_debug() {
    let c = SidecarConfig::parse_from([
        "pg-raggraph-sidecar",
        "--database-url",
        "postgres://user:SECRETPW@host/db",
    ]);
    let dbg = format!("{c:?}");
    assert!(
        !dbg.contains("SECRETPW"),
        "conn string password leaked in Debug: {dbg}"
    );
}
