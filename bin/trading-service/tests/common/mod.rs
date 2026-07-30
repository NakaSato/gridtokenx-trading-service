//! Shared helpers for the DB-backed integration tests.
//!
//! Included per test binary with `mod common;` — this file is not a test target
//! of its own.

#![allow(dead_code)] // not every including binary uses every helper

/// Connection URL for the integration-test database.
///
/// **These tests must never run against a database a live service is attached
/// to.** `gridtokenx_trading` is served by the running trading-service, whose
/// SettlementWorker claims and resets settlement rows every 10s and whose
/// MatcherWorker fills resting orders. That worker mutating a fixture mid-test
/// is not hypothetical: it made `settlement_cas_retry_test` fail intermittently
/// (~1 run in 4) by bumping `retry_count` underneath the assertion, and it can
/// fill an order a test expects to stay open.
///
/// So the database name is always suffixed `_test`, even when `DATABASE_URL`
/// names the live one — the host, port and credentials are reused, only the
/// database differs. Precedence:
///
/// 1. `TRADING_TEST_DATABASE_URL` — used verbatim, the full override.
/// 2. `DATABASE_URL` / `TRADING_DATABASE_URL` with the database renamed to
///    `<name>_test`.
/// 3. The localnet default, `gridtokenx_trading_test`.
///
/// Create the database with `scripts/setup-test-db.sh` (idempotent; clones the
/// schema from the service's own database).
pub fn test_db_url() -> String {
    if let Ok(explicit) = std::env::var("TRADING_TEST_DATABASE_URL") {
        return explicit;
    }
    let base = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("TRADING_DATABASE_URL"))
        .unwrap_or_else(|_| {
            "postgresql://gridtokenx_user:gridtokenx_password@localhost:7001/gridtokenx_trading"
                .to_string()
        });
    with_test_database(&base)
}

/// Rewrite a Postgres URL's database name to `<name>_test`, preserving
/// everything else (credentials, host, port, query parameters). Idempotent: a
/// name that already ends in `_test` is left alone.
///
/// Parses by locating the authority (`scheme://host:port`) and then the first
/// `/` after it. Splitting on the *last* `/` instead looks equivalent but is
/// not: for a URL naming no database it lands on the scheme's own `//` and
/// mangles the host into a database name.
pub fn with_test_database(url: &str) -> String {
    let (head, query) = match url.split_once('?') {
        Some((h, q)) => (h, Some(q)),
        None => (url, None),
    };
    let head = head.trim_end_matches('/');

    let authority_start = head.find("://").map_or(0, |i| i + 3);
    let (prefix, name) = match head[authority_start..].find('/') {
        // `+ 1` keeps the separator out of both halves.
        Some(rel) => {
            let abs = authority_start + rel;
            (&head[..abs], &head[abs + 1..])
        }
        // No path at all: no database is named.
        None => (head, ""),
    };

    let name = if name.is_empty() {
        "gridtokenx_trading"
    } else {
        name
    };
    let renamed = if name.ends_with("_test") {
        name.to_string()
    } else {
        format!("{name}_test")
    };
    match query {
        Some(q) => format!("{prefix}/{renamed}?{q}"),
        None => format!("{prefix}/{renamed}"),
    }
}
