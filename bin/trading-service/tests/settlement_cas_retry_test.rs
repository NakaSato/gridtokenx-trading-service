#![allow(clippy::unwrap_used)] // unwrap is idiomatic in integration tests

//! Integration coverage for the settlement claim-CAS / retry / stale-reaper
//! financial-safety paths (`PostgresSettlementRepository`), exercised against a
//! live Postgres. These guard the "mint-once" invariant: a settlement must never
//! be claimed twice (→ double mint) and must never be stranded in `processing`.
//!
//! Mirrors the production constants in `trading-logic::settlement`
//! (`MAX_SETTLEMENT_RETRIES = 5`, `STALE_PROCESSING_SECS = 300`). Kept in sync by
//! hand — they are private there.
//!
//! Requires a migrated Postgres (`just db-up` / superproject `just migrate`).
//! Skips cleanly (passes) when no DB is reachable so the suite stays green in
//! environments without infra.

use chrono::Utc;
use rust_decimal_macros::dec;
use sqlx::PgPool;
use trading_core::models::{Settlement, SettlementStatus};
use trading_core::traits::{OrderRepository, SettlementRepository};
use trading_core::types::{OrderSide, OrderStatus, OrderType, TimeInForce};
use trading_core::models::TradingOrder;
use trading_persistence::repositories::{PostgresOrderRepository, PostgresSettlementRepository};
use uuid::Uuid;

const MAX_SETTLEMENT_RETRIES: i32 = 5;
const STALE_PROCESSING_SECS: i64 = 300;

/// Connect to the dev/test Postgres, or return `None` if unreachable so the test
/// can skip instead of fail in infra-less environments.
async fn try_pool() -> Option<PgPool> {
    let db_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("TRADING_DATABASE_URL"))
        .unwrap_or_else(|_| {
            "postgresql://gridtokenx_user:gridtokenx_password@localhost:7001/gridtokenx_trading"
                .to_string()
        });
    PgPool::connect(&db_url).await.ok()
}

/// Seed the FK chain a settlement requires (user → epoch → buy/sell orders) and
/// return their ids. Caller deletes the user + epoch at the end (cascades).
async fn seed_fk_chain(pool: &PgPool) -> (Uuid, Uuid, Uuid, Uuid) {
    // No `users` rows are seeded or cleaned up here: migration 20260728000000
    // (the DB-per-service split) dropped the cross-domain FK to IAM `users`, and
    // `gridtokenx_trading` has no such table at all — identities live in
    // `gridtokenx_iam`. Verified: the only FK on `trading_orders`/`settlements` is
    // to `market_epochs(id)`. The leftover INSERT/DELETE failed with
    // `relation "users" does not exist`.
    let user_id = Uuid::new_v4();

    let epoch_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO market_epochs (id, epoch_number, start_time, end_time, status) \
         VALUES ($1, $2, $3, $4, 'pending')",
    )
    .bind(epoch_id)
    // Unique-id-derived, not wall-clock — avoids parallel-test collisions on
    // market_epochs_epoch_number_key.
    .bind((epoch_id.as_u128() as u64 >> 1) as i64)
    .bind(Utc::now())
    .bind(Utc::now() + chrono::Duration::minutes(15))
    .execute(pool)
    .await
    .expect("insert epoch");

    let order_repo = PostgresOrderRepository::new(pool.clone());
    let buy_order_id = Uuid::new_v4();
    let sell_order_id = Uuid::new_v4();
    for (oid, side, idx, pda) in [
        (buy_order_id, OrderSide::Buy, 1, "BuyPDA1111111111111111111111111111111111"),
        (sell_order_id, OrderSide::Sell, 2, "SellPDA111111111111111111111111111111111"),
    ] {
        let order = TradingOrder {
            id: oid,
            user_id,
            order_type: OrderType::Limit,
            side,
            energy_amount: dec!(10.0),
            price_per_kwh: dec!(4.0),
            filled_amount: dec!(0.0),
            status: OrderStatus::Active,
            expires_at: None,
            created_at: Some(Utc::now()),
            filled_at: None,
            epoch_id: Some(epoch_id),
            zone_id: Some(1),
            meter_id: None,
            refund_tx_signature: None,
            order_pda: Some(pda.to_string()),
            order_index: Some(idx),
            session_token: None,
            blockchain_status: None,
            blockchain_tx_hash: None,
            blockchain_error: None,
            retry_count: 0,
            time_in_force: TimeInForce::Gtc,
            market_segment: trading_core::types::MarketSegment::Realtime,
        };
        order_repo.insert_order(&order).await.expect("insert order");
    }

    (user_id, epoch_id, buy_order_id, sell_order_id)
}

/// Build a fresh `pending` settlement over the seeded FK chain.
fn pending_settlement(
    epoch_id: Uuid,
    user_id: Uuid,
    buy_order_id: Uuid,
    sell_order_id: Uuid,
) -> Settlement {
    Settlement {
        id: Uuid::new_v4(),
        trade_id: None,
        epoch_id,
        buyer_id: user_id,
        seller_id: user_id,
        buy_order_id,
        sell_order_id,
        energy_amount: dec!(5.0),
        price: dec!(4.0),
        total_amount: dec!(20.0),
        fee_amount: dec!(0.0),
        net_amount: dec!(20.0),
        status: SettlementStatus::Pending,
        blockchain_tx: None,
        created_at: Utc::now(),
        confirmed_at: None,
        wheeling_charge: Some(dec!(0.1)),
        loss_factor: Some(dec!(1.0)),
        loss_cost: Some(dec!(0.0)),
        effective_energy: Some(dec!(5.0)),
        buyer_zone_id: Some(1),
        seller_zone_id: Some(1),
        buyer_session_token: None,
        seller_session_token: None,
        erc_certificate_id: None,
        erc_transfer_tx: None,
        retry_count: 0,
        error_message: None,
    }
}

async fn status_of(pool: &PgPool, id: Uuid) -> String {
    sqlx::query_scalar::<_, String>("SELECT status FROM settlements WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("fetch status")
}

async fn retry_count_of(pool: &PgPool, id: Uuid) -> i32 {
    sqlx::query_scalar::<_, i32>("SELECT retry_count FROM settlements WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("fetch retry_count")
}

async fn cleanup(pool: &PgPool, user_id: Uuid, epoch_id: Uuid) {
    // Orders first: the DB-per-service split dropped the cross-domain FK to IAM
    // `users` (migration 20260728000000), so deleting the owner no longer
    // cascades them away.
    sqlx::query("DELETE FROM trading_orders WHERE user_id = $1").bind(user_id).execute(pool).await.ok();
    sqlx::query("DELETE FROM market_epochs WHERE id = $1").bind(epoch_id).execute(pool).await.ok();
}

/// claim_settlements_for_processing is a single-statement CAS: claiming the same
/// id set twice yields the row exactly once. This is the core mint-once guard
/// against a worker tick + an RPC batch racing on the same pending settlements.
#[tokio::test]
async fn claim_is_mint_once_under_double_claim() {
    let Some(pool) = try_pool().await else {
        eprintln!("SKIP: no Postgres reachable");
        return;
    };
    let (user_id, epoch_id, buy, sell) = seed_fk_chain(&pool).await;
    let repo = PostgresSettlementRepository::new(pool.clone());

    let s = pending_settlement(epoch_id, user_id, buy, sell);
    repo.insert_settlement(&s).await.expect("insert settlement");
    let ids = [s.id];

    // First claimer wins the row.
    let first = repo.claim_settlements_for_processing(&ids).await.expect("first claim");
    assert_eq!(first.len(), 1, "first claim must capture the pending row");
    assert_eq!(first[0].id, s.id);

    // Second claimer over the SAME id set gets nothing — row is already
    // `processing`, so it can never be minted twice.
    let second = repo.claim_settlements_for_processing(&ids).await.expect("second claim");
    assert!(second.is_empty(), "second claim must be empty (no double-claim)");

    assert_eq!(status_of(&pool, s.id).await, "processing");

    cleanup(&pool, user_id, epoch_id).await;
}

/// reset_settlements_for_retry bumps retry_count and releases `processing` rows
/// back to `pending`, until the retry budget is exhausted, after which the row
/// becomes terminal `permanently_failed` (bounds retry churn).
#[tokio::test]
async fn reset_increments_then_parks_permanently_failed() {
    let Some(pool) = try_pool().await else {
        eprintln!("SKIP: no Postgres reachable");
        return;
    };
    let (user_id, epoch_id, buy, sell) = seed_fk_chain(&pool).await;
    let repo = PostgresSettlementRepository::new(pool.clone());

    let s = pending_settlement(epoch_id, user_id, buy, sell);
    repo.insert_settlement(&s).await.expect("insert settlement");
    let ids = [s.id];

    // Drive the retry budget to exhaustion. Each cycle: claim (pending →
    // processing) then reset (processing → pending, retry_count++), except the
    // final reset which tips it over the cap into permanently_failed.
    for expected_retry in 1..=MAX_SETTLEMENT_RETRIES {
        let claimed = repo.claim_settlements_for_processing(&ids).await.expect("claim");
        assert_eq!(claimed.len(), 1, "row must be re-claimable until terminal");

        let affected = repo
            .reset_settlements_for_retry(&ids, MAX_SETTLEMENT_RETRIES, Some("simulated chain failure"))
            .await
            .expect("reset");
        assert_eq!(affected, 1, "reset must touch the processing row");
        assert_eq!(retry_count_of(&pool, s.id).await, expected_retry);

        if expected_retry < MAX_SETTLEMENT_RETRIES {
            assert_eq!(status_of(&pool, s.id).await, "pending", "released for retry");
        } else {
            assert_eq!(
                status_of(&pool, s.id).await,
                "permanently_failed",
                "retry budget exhausted → terminal"
            );
        }
    }

    // Once permanently_failed it is no longer pending, so it can never be
    // re-claimed and re-minted.
    let post = repo.claim_settlements_for_processing(&ids).await.expect("post claim");
    assert!(post.is_empty(), "permanently_failed row must not be claimable");

    cleanup(&pool, user_id, epoch_id).await;
}

/// reclaim_stale_processing recovers settlements orphaned in `processing` by a
/// crash/partial failure between claim and finalize: rows whose updated_at is
/// older than the staleness window are bumped back to a retryable state.
#[tokio::test]
async fn reclaim_releases_stale_processing_rows() {
    let Some(pool) = try_pool().await else {
        eprintln!("SKIP: no Postgres reachable");
        return;
    };
    let (user_id, epoch_id, buy, sell) = seed_fk_chain(&pool).await;
    let repo = PostgresSettlementRepository::new(pool.clone());

    let s = pending_settlement(epoch_id, user_id, buy, sell);
    repo.insert_settlement(&s).await.expect("insert settlement");
    let ids = [s.id];

    repo.claim_settlements_for_processing(&ids).await.expect("claim");
    assert_eq!(status_of(&pool, s.id).await, "processing");

    // A fresh claim is NOT stale — reaper leaves it alone.
    let none = repo
        .reclaim_stale_processing(STALE_PROCESSING_SECS, MAX_SETTLEMENT_RETRIES)
        .await
        .expect("reclaim fresh");
    assert_eq!(none, 0, "freshly-claimed row is not stale");
    assert_eq!(status_of(&pool, s.id).await, "processing");

    // Backdate updated_at past the staleness window to simulate an orphan.
    // A `BEFORE UPDATE` trigger (`update_settlements_updated_at`) normally forces
    // updated_at = NOW() on every UPDATE, which would clobber the backdate — so
    // we disable user triggers for this one connection via
    // `session_replication_role = replica` (superuser, connection-scoped: does
    // not affect other tests' connections).
    {
        let mut conn = pool.acquire().await.expect("acquire conn");
        sqlx::query("SET session_replication_role = replica")
            .execute(&mut *conn)
            .await
            .expect("disable triggers");
        sqlx::query("UPDATE settlements SET updated_at = NOW() - make_interval(secs => $2) WHERE id = $1")
            .bind(s.id)
            .bind((STALE_PROCESSING_SECS + 60) as f64)
            .execute(&mut *conn)
            .await
            .expect("backdate updated_at");
        sqlx::query("SET session_replication_role = DEFAULT")
            .execute(&mut *conn)
            .await
            .expect("re-enable triggers");
    }

    let reclaimed = repo
        .reclaim_stale_processing(STALE_PROCESSING_SECS, MAX_SETTLEMENT_RETRIES)
        .await
        .expect("reclaim stale");
    assert_eq!(reclaimed, 1, "stale processing row must be reclaimed");
    assert_eq!(status_of(&pool, s.id).await, "pending", "reclaimed → retryable");
    assert_eq!(retry_count_of(&pool, s.id).await, 1, "reclaim counts as a retry");

    cleanup(&pool, user_id, epoch_id).await;
}
