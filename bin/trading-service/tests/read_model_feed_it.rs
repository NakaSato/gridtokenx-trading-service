//! Gated DB integration tests for the read-model feed projection
//! (DB-per-service migration, pre-cutover checklist §8).
//!
//! These exercise `PgWalletReadModelRepository` / `PgMeterReadModelRepository`
//! against a REAL Postgres that already has the Phase-1 migration applied
//! (`gridtokenx_trading`, migration `20260715000001_trading_phase1_local_models`).
//! They cover the invariants the projection relies on:
//!   * wallet upsert idempotency (same event twice → one row),
//!   * last-writer-wins (an older `updated_at` cannot clobber a newer row),
//!   * primary sibling-demote (promoting B demotes A; partial unique index holds),
//!   * meter upsert,
//!   * boot backfill parity (seed source rows → backfill → counts match).
//!
//! GATED: every test early-returns unless `RUN_DB_IT` is set, so a host
//! `cargo test` (no infra) stays green and `cargo test --no-run` still compiles
//! them. Runtime SQLx only (no compile-time macros / DATABASE_URL at build),
//! mirroring `crates/trading-persistence/src/repositories/vpp.rs`.
//!
//! Run: `RUN_DB_IT=1 TRADING_DATABASE_URL=postgres://…/gridtokenx_trading \
//!       cargo test -p trading-service --test read_model_feed_it -- --nocapture`

#![allow(clippy::unwrap_used)] // unwrap is idiomatic in integration tests

use sqlx::PgPool;
use uuid::Uuid;

use trading_core::traits::{
    MeterReadModelRecord, MeterReadModelRepository, WalletReadModelRecord,
    WalletReadModelRepository,
};
use trading_persistence::repositories::read_model::{
    PgMeterReadModelRepository, PgWalletReadModelRepository,
};

mod common;

/// Returns `false` (and prints a skip line) unless `RUN_DB_IT` is set. Call at
/// the top of every `#[tokio::test]` so host `cargo test` skips the DB work.
fn db_it_enabled() -> bool {
    if std::env::var("RUN_DB_IT").is_err() {
        eprintln!("skip: set RUN_DB_IT=1 (+ a live gridtokenx_trading Postgres) to run read_model_feed_it");
        return false;
    }
    true
}

async fn connect() -> PgPool {
    let db_url = common::test_db_url();
    PgPool::connect(&db_url)
        .await
        .expect("connect to gridtokenx_trading")
}

fn wallet_rec(user_id: Uuid, addr: &str, is_primary: bool) -> WalletReadModelRecord {
    WalletReadModelRecord {
        user_id,
        wallet_address: addr.to_string(),
        is_primary,
        blockchain_registered: true,
        user_account_pda: Some(format!("pda-{addr}")),
        shard_id: Some(1),
    }
}

/// Remove any read-model rows we seeded for `user_id` (both tables).
async fn cleanup_user(pool: &PgPool, user_id: Uuid) {
    sqlx::query("DELETE FROM iam_wallet_read_model WHERE user_id = $1")
        .bind(user_id)
        .execute(pool)
        .await
        .ok();
    sqlx::query("DELETE FROM meter_read_model WHERE user_id = $1")
        .bind(user_id)
        .execute(pool)
        .await
        .ok();
}

// ── Wallet upsert idempotency ────────────────────────────────────────────────

#[tokio::test]
async fn wallet_upsert_is_idempotent() {
    if !db_it_enabled() {
        return;
    }
    let pool = connect().await;
    let repo = PgWalletReadModelRepository::new(pool.clone());
    let user_id = Uuid::new_v4();
    let rec = wallet_rec(user_id, "wallet-idem-1", true);

    // Replaying the same event twice must leave exactly one row.
    repo.upsert_wallet(&rec).await.expect("first upsert");
    repo.upsert_wallet(&rec).await.expect("replay upsert");

    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM iam_wallet_read_model WHERE user_id = $1")
            .bind(user_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 1, "same event replayed must not duplicate the row");

    cleanup_user(&pool, user_id).await;
}

// ── Last-writer-wins guard ───────────────────────────────────────────────────

#[tokio::test]
async fn wallet_upsert_older_write_does_not_clobber_newer() {
    if !db_it_enabled() {
        return;
    }
    let pool = connect().await;
    let repo = PgWalletReadModelRepository::new(pool.clone());
    let user_id = Uuid::new_v4();

    // Seed a row whose updated_at is in the FUTURE (a "newer" state), directly
    // so we control the timestamp. blockchain_registered=false is our marker.
    sqlx::query(
        r#"INSERT INTO iam_wallet_read_model
               (user_id, wallet_address, is_primary, blockchain_registered,
                user_account_pda, shard_id, updated_at)
           VALUES ($1, $2, false, false, 'pda-new', 1, now() + interval '1 hour')"#,
    )
    .bind(user_id)
    .bind("wallet-lww")
    .execute(&pool)
    .await
    .unwrap();

    // An event upsert stamps updated_at = now() (OLDER than the seeded row);
    // the ON CONFLICT ... WHERE stored.updated_at <= EXCLUDED.updated_at guard
    // must reject it, so blockchain_registered stays false.
    let older = wallet_rec(user_id, "wallet-lww", true); // is_primary=true, registered=true
    repo.upsert_wallet(&older).await.expect("older upsert runs");

    let registered: bool = sqlx::query_scalar(
        "SELECT blockchain_registered FROM iam_wallet_read_model WHERE user_id = $1 AND wallet_address = $2",
    )
    .bind(user_id)
    .bind("wallet-lww")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        !registered,
        "older write must NOT clobber the newer row's state"
    );

    cleanup_user(&pool, user_id).await;
}

// ── Primary sibling-demote ───────────────────────────────────────────────────

#[tokio::test]
async fn promoting_second_wallet_demotes_first() {
    if !db_it_enabled() {
        return;
    }
    let pool = connect().await;
    let repo = PgWalletReadModelRepository::new(pool.clone());
    let user_id = Uuid::new_v4();

    // A is primary.
    repo.upsert_wallet(&wallet_rec(user_id, "wallet-A", true))
        .await
        .expect("upsert A primary");
    // Promote B — must demote A in the same transaction (partial unique index
    // (user_id) WHERE is_primary would otherwise reject two primaries).
    repo.upsert_wallet(&wallet_rec(user_id, "wallet-B", true))
        .await
        .expect("upsert B primary (must not violate the partial unique index)");

    let primaries: Vec<String> = sqlx::query_scalar(
        "SELECT wallet_address FROM iam_wallet_read_model WHERE user_id = $1 AND is_primary",
    )
    .bind(user_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(primaries, vec!["wallet-B".to_string()], "only B stays primary");

    cleanup_user(&pool, user_id).await;
}

#[tokio::test]
async fn set_wallet_primary_flips_and_demotes() {
    if !db_it_enabled() {
        return;
    }
    let pool = connect().await;
    let repo = PgWalletReadModelRepository::new(pool.clone());
    let user_id = Uuid::new_v4();

    repo.upsert_wallet(&wallet_rec(user_id, "wallet-P1", true))
        .await
        .expect("seed P1 primary");
    repo.upsert_wallet(&wallet_rec(user_id, "wallet-P2", false))
        .await
        .expect("seed P2 non-primary");

    // Promote P2 via the dedicated primary-changed path (UPDATE, not upsert).
    repo.set_wallet_primary(user_id, "wallet-P2", true)
        .await
        .expect("set P2 primary");

    let primaries: Vec<String> = sqlx::query_scalar(
        "SELECT wallet_address FROM iam_wallet_read_model WHERE user_id = $1 AND is_primary",
    )
    .bind(user_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(primaries, vec!["wallet-P2".to_string()]);

    cleanup_user(&pool, user_id).await;
}

// ── Meter upsert ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn meter_upsert_inserts_then_updates() {
    if !db_it_enabled() {
        return;
    }
    let pool = connect().await;
    let repo = PgMeterReadModelRepository::new(pool.clone());
    let user_id = Uuid::new_v4();
    let serial = format!("SN-{}", &user_id.to_string()[..8]);

    let rec = MeterReadModelRecord {
        serial_number: serial.clone(),
        meter_id: Uuid::new_v4(),
        user_id,
        zone_id: Some(2),
        status: Some("active".to_string()),
    };
    repo.upsert_meter(&rec).await.expect("insert meter");

    // Same serial, changed status → update in place (conflict on serial_number).
    let updated = MeterReadModelRecord {
        status: Some("suspended".to_string()),
        ..rec.clone()
    };
    repo.upsert_meter(&updated).await.expect("update meter");

    let (count, status): (i64, Option<String>) = {
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM meter_read_model WHERE serial_number = $1")
                .bind(&serial)
                .fetch_one(&pool)
                .await
                .unwrap();
        let status: Option<String> =
            sqlx::query_scalar("SELECT status FROM meter_read_model WHERE serial_number = $1")
                .bind(&serial)
                .fetch_one(&pool)
                .await
                .unwrap();
        (count, status)
    };
    assert_eq!(count, 1, "conflict on serial must update, not duplicate");
    assert_eq!(status.as_deref(), Some("suspended"));

    cleanup_user(&pool, user_id).await;
}

// ── Backfill parity ──────────────────────────────────────────────────────────

#[tokio::test]
async fn backfill_wallets_mirrors_source_rows() {
    if !db_it_enabled() {
        return;
    }
    let pool = connect().await;
    let repo = PgWalletReadModelRepository::new(pool.clone());

    // Seed a user + 3 source wallets in IAM `user_wallets` (still reachable on
    // the same pool pre-cutover). One primary, two secondary.
    let user_id = Uuid::new_v4();
    seed_user(&pool, user_id).await;
    for (i, primary) in [true, false, false].iter().enumerate() {
        sqlx::query(
            r#"INSERT INTO user_wallets
                   (user_id, wallet_address, is_primary, blockchain_registered)
               VALUES ($1, $2, $3, true)
               ON CONFLICT DO NOTHING"#,
        )
        .bind(user_id)
        .bind(format!("bf-wallet-{i}"))
        .bind(primary)
        .execute(&pool)
        .await
        .unwrap();
    }
    let source_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM user_wallets WHERE user_id = $1")
            .bind(user_id)
            .fetch_one(&pool)
            .await
            .unwrap();

    repo.backfill_wallets().await.expect("backfill wallets");

    let mirrored: i64 =
        sqlx::query_scalar("SELECT count(*) FROM iam_wallet_read_model WHERE user_id = $1")
            .bind(user_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        mirrored, source_count,
        "read-model must mirror every seeded source wallet for the user"
    );

    cleanup_user(&pool, user_id).await;
    cleanup_source(&pool, user_id).await;
}

/// The lazy single-user self-heal: a user whose wallet row never reached the
/// read-model (e.g. a dropped event) must be recoverable on demand —
/// `backfill_wallet_for` reconciles just that user from the source and returns
/// their primary wallet, without a full boot backfill.
#[tokio::test]
async fn backfill_wallet_for_reconciles_missing_row_and_returns_primary() {
    if !db_it_enabled() {
        return;
    }
    let pool = connect().await;
    let repo = PgWalletReadModelRepository::new(pool.clone());

    // Seed a user with a primary + a secondary source wallet, but DO NOT project
    // them into the read-model — simulating the dropped-event gap.
    let user_id = Uuid::new_v4();
    seed_user(&pool, user_id).await;
    for (addr, primary) in [("heal-primary", true), ("heal-secondary", false)] {
        sqlx::query(
            r#"INSERT INTO user_wallets
                   (user_id, wallet_address, is_primary, blockchain_registered)
               VALUES ($1, $2, $3, true)
               ON CONFLICT DO NOTHING"#,
        )
        .bind(user_id)
        .bind(addr)
        .bind(primary)
        .execute(&pool)
        .await
        .unwrap();
    }

    // Precondition: the read-model has nothing for this user yet.
    let before: i64 =
        sqlx::query_scalar("SELECT count(*) FROM iam_wallet_read_model WHERE user_id = $1")
            .bind(user_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(before, 0, "read-model must start empty for the user");

    // Self-heal: reconcile just this user and return the primary.
    let primary = repo
        .backfill_wallet_for(user_id)
        .await
        .expect("lazy reconcile runs");
    assert_eq!(
        primary.as_deref(),
        Some("heal-primary"),
        "must return the source primary wallet"
    );

    // Both source wallets are now mirrored, exactly one flagged primary.
    let mirrored: i64 =
        sqlx::query_scalar("SELECT count(*) FROM iam_wallet_read_model WHERE user_id = $1")
            .bind(user_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(mirrored, 2, "both source wallets projected");
    let primaries: Vec<String> = sqlx::query_scalar(
        "SELECT wallet_address FROM iam_wallet_read_model WHERE user_id = $1 AND is_primary",
    )
    .bind(user_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(primaries, vec!["heal-primary".to_string()]);

    // Idempotent: a second call is a harmless no-op returning the same primary.
    let again = repo
        .backfill_wallet_for(user_id)
        .await
        .expect("second reconcile runs");
    assert_eq!(again.as_deref(), Some("heal-primary"));

    cleanup_user(&pool, user_id).await;
    cleanup_source(&pool, user_id).await;
}

#[tokio::test]
async fn backfill_wallet_for_unknown_user_returns_none() {
    if !db_it_enabled() {
        return;
    }
    let pool = connect().await;
    let repo = PgWalletReadModelRepository::new(pool.clone());
    // A user with no source wallet yields None (fail-closed, nothing projected).
    let user_id = Uuid::new_v4();
    let primary = repo
        .backfill_wallet_for(user_id)
        .await
        .expect("reconcile runs for unknown user");
    assert!(primary.is_none(), "unknown user must resolve to None");
}

#[tokio::test]
async fn backfill_meters_mirrors_source_rows() {
    if !db_it_enabled() {
        return;
    }
    let pool = connect().await;
    let repo = PgMeterReadModelRepository::new(pool.clone());

    let user_id = Uuid::new_v4();
    seed_user(&pool, user_id).await;
    let mut serials = Vec::new();
    for i in 0..2 {
        let serial = format!("bf-sn-{}-{i}", &user_id.to_string()[..8]);
        serials.push(serial.clone());
        sqlx::query(
            r#"INSERT INTO meters (id, serial_number, user_id, zone_id, status)
               VALUES ($1, $2, $3, 1, 'active')
               ON CONFLICT DO NOTHING"#,
        )
        .bind(Uuid::new_v4())
        .bind(&serial)
        .bind(user_id)
        .execute(&pool)
        .await
        .unwrap();
    }
    let source_count: i64 = sqlx::query_scalar("SELECT count(*) FROM meters WHERE user_id = $1")
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .unwrap();

    repo.backfill_meters().await.expect("backfill meters");

    let mirrored: i64 =
        sqlx::query_scalar("SELECT count(*) FROM meter_read_model WHERE user_id = $1")
            .bind(user_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(mirrored, source_count, "read-model must mirror seeded meters");

    cleanup_user(&pool, user_id).await;
    cleanup_source(&pool, user_id).await;
}

/// Insert the minimal IAM `users` row that `user_wallets` / `meters` FK to.
async fn seed_user(pool: &PgPool, user_id: Uuid) {
    sqlx::query(
        "INSERT INTO users (id, email, username, password_hash, wallet_address)
         VALUES ($1, $2, $3, 'mock_hash', $4) ON CONFLICT DO NOTHING",
    )
    .bind(user_id)
    .bind(format!("rm-it-{user_id}@gridtokenx.com"))
    .bind(format!("rm_it_{user_id}"))
    .bind(format!("Wallet_{}", &user_id.to_string()[..24]))
    .execute(pool)
    .await
    .expect("seed user");
}

async fn cleanup_source(pool: &PgPool, user_id: Uuid) {
    sqlx::query("DELETE FROM user_wallets WHERE user_id = $1")
        .bind(user_id)
        .execute(pool)
        .await
        .ok();
    sqlx::query("DELETE FROM meters WHERE user_id = $1")
        .bind(user_id)
        .execute(pool)
        .await
        .ok();
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(pool)
        .await
        .ok();
}
