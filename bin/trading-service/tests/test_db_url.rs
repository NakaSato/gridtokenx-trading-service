//! Unit coverage for the integration-test DSN rewrite (`tests/common`).
//!
//! Lives in its own binary so these run exactly once. Left inside
//! `common/mod.rs` they would be compiled into all eight test binaries that
//! include the module, running 8× and reporting one failure as eight.
//!
//! Needs no database.

mod common;

use common::with_test_database;

#[test]
fn renames_only_the_database() {
    assert_eq!(
        with_test_database("postgresql://u:p@localhost:7001/gridtokenx_trading"),
        "postgresql://u:p@localhost:7001/gridtokenx_trading_test"
    );
}

#[test]
fn preserves_query_parameters() {
    assert_eq!(
        with_test_database("postgres://u:p@db:5432/trading?sslmode=require"),
        "postgres://u:p@db:5432/trading_test?sslmode=require"
    );
}

/// Idempotent, so an already-test URL is not turned into `_test_test`.
#[test]
fn already_a_test_database_is_untouched() {
    let url = "postgresql://u:p@localhost:7001/gridtokenx_trading_test";
    assert_eq!(with_test_database(url), url);
}

/// A URL naming no database must still yield an isolated target. Splitting on
/// the last `/` (the obvious implementation) lands on the scheme's own `//`
/// here and produces `postgresql://u:p@localhost:7001_test` — a mangled host,
/// not a database.
#[test]
fn url_without_a_database_falls_back_to_the_conventional_name() {
    assert_eq!(
        with_test_database("postgresql://u:p@localhost:7001"),
        "postgresql://u:p@localhost:7001/gridtokenx_trading_test"
    );
}

/// A trailing slash is a path, not a database name.
#[test]
fn trailing_slash_is_not_a_database_name() {
    assert_eq!(
        with_test_database("postgresql://u:p@localhost:7001/"),
        "postgresql://u:p@localhost:7001/gridtokenx_trading_test"
    );
}

/// The invariant that matters: whatever goes in, the result never addresses the
/// live database a service is attached to.
#[test]
fn never_returns_the_live_database() {
    for url in [
        "postgresql://u:p@localhost:7001/gridtokenx_trading",
        "postgresql://u:p@localhost:7001/gridtokenx_trading/",
        "postgresql://u:p@localhost:7001",
        "postgres://u:p@h/gridtokenx?x=1",
        "postgres://h/gridtokenx_trading",
    ] {
        let out = with_test_database(url);
        let database = out
            .rsplit('/')
            .next()
            .and_then(|tail| tail.split('?').next())
            .expect("a database name");
        assert!(
            database.ends_with("_test"),
            "{url} rewrote to {out}, whose database {database:?} is not an isolated one"
        );
    }
}
