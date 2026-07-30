#![allow(clippy::unwrap_used)] // unwrap is idiomatic in integration tests

//! Live-Postgres coverage for `SettlementRepository::persist_matched_trade` —
//! the atomic per-trade persistence path that fixes the batch-non-atomicity bug
//! (settlement + order_matches + both incremental fills + outbox events commit
//! together, or the whole trade rolls back). Needs a migrated database; run with
//! `cargo test -p trading-service --test settlement_atomic_trade_test`.

use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sqlx::{PgPool, Row};
use trading_core::events::{Event, OrderMatchedPayload};
use trading_core::models::{OrderMatch, Settlement, SettlementStatus};
use trading_core::traits::{SettlementRepository, TradeFill};
use trading_core::types::{OrderSide, OrderStatus, OrderType, TimeInForce};
use trading_persistence::repositories::PostgresSettlementRepository;
use uuid::Uuid;

async fn connect() -> PgPool {
    let db_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("TRADING_DATABASE_URL"))
        .unwrap_or_else(|_| {
            "postgresql://gridtokenx_user:gridtokenx_password@localhost:7001/gridtokenx_trading"
                .to_string()
        });
    PgPool::connect(&db_url)
        .await
        .expect("Failed to connect to postgres")
}

async fn insert_epoch(pool: &PgPool) -> Uuid {
    let epoch_id = Uuid::new_v4();
    // Derive epoch_number from the unique id (not wall-clock) so parallel tests
    // never collide on the same nanosecond and trip a unique constraint.
    let epoch_number = (epoch_id.as_u128() as u64 >> 1) as i64;
    sqlx::query(
        "INSERT INTO market_epochs (id, epoch_number, start_time, end_time, status) VALUES ($1, $2, $3, $4, 'pending')",
    )
    .bind(epoch_id)
    .bind(epoch_number)
    .bind(Utc::now())
    .bind(Utc::now() + Duration::minutes(15))
    .execute(pool)
    .await
    .expect("insert epoch");
    epoch_id
}

// User ids here are throwaway uuids with nothing to seed. The `insert_user` /
// `seed_settlement_users` helpers this file used to carry seeded IAM `users`
// rows for FKs that no longer exist: migration 20260728000000 (the
// DB-per-service split) dropped the cross-domain FKs, and `gridtokenx_trading`
// has no `users` table at all — identities live in `gridtokenx_iam`. Verified:
// the only FK on `trading_orders` and on `settlements` is to `market_epochs(id)`.
// The leftover INSERTs failed with `relation "users" does not exist`.

async fn insert_order(
    pool: &PgPool,
    epoch_id: Uuid,
    user_id: Uuid,
    side: OrderSide,
    energy: Decimal,
    status: OrderStatus,
) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO trading_orders (id, user_id, order_type, side, energy_amount, \
         price_per_kwh, filled_amount, status, time_in_force, zone_id, epoch_id) \
         VALUES ($1, $2, $3, $4, $5, $6, 0, $7, $8, $9, $10)",
    )
    .bind(id)
    .bind(user_id)
    .bind(OrderType::Limit)
    .bind(side)
    .bind(energy)
    .bind(dec!(1.0))
    .bind(status)
    .bind(TimeInForce::Gtc)
    .bind(Some(1_i32))
    .bind(epoch_id)
    .execute(pool)
    .await
    .expect("insert order");
    id
}

fn settlement(epoch_id: Uuid, buy_order: Uuid, sell_order: Uuid, amount: Decimal) -> Settlement {
    Settlement {
        id: Uuid::new_v4(),
        trade_id: Some(Uuid::new_v4()),
        epoch_id,
        buyer_id: Uuid::new_v4(),
        seller_id: Uuid::new_v4(),
        buy_order_id: buy_order,
        sell_order_id: sell_order,
        energy_amount: amount,
        price: dec!(1.0),
        total_amount: amount,
        fee_amount: dec!(0),
        net_amount: amount,
        status: SettlementStatus::Pending,
        blockchain_tx: None,
        created_at: Utc::now(),
        confirmed_at: None,
        wheeling_charge: Some(dec!(0)),
        loss_factor: Some(dec!(0)),
        loss_cost: Some(dec!(0)),
        effective_energy: Some(amount),
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

fn order_match(epoch_id: Uuid, buy_order: Uuid, sell_order: Uuid, amount: Decimal) -> OrderMatch {
    OrderMatch {
        id: Uuid::new_v4(),
        epoch_id,
        buy_order_id: buy_order,
        sell_order_id: sell_order,
        matched_amount: amount,
        match_price: dec!(1.0),
        match_time: Utc::now(),
        status: "pending".to_string(),
    }
}

fn matched_event(m: &OrderMatch) -> Event {
    Event::OrderMatched(OrderMatchedPayload {
        match_id: m.id,
        epoch_id: m.epoch_id,
        buy_order_id: m.buy_order_id,
        sell_order_id: m.sell_order_id,
        amount: m.matched_amount,
        price: m.match_price,
        buyer_id: Uuid::new_v4(),
        seller_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        zone_id: Some(1),
    })
}

async fn order_status(pool: &PgPool, id: Uuid) -> (Decimal, String) {
    let row = sqlx::query(
        "SELECT COALESCE(filled_amount, 0) AS f, status::text AS s FROM trading_orders WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .unwrap();
    (row.get::<Decimal, _>("f"), row.get::<String, _>("s"))
}

async fn count_rows(pool: &PgPool, sql: &str, id: Uuid) -> i64 {
    sqlx::query(sql)
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
        .get::<i64, _>("n")
}

/// Happy path + incremental accumulation: two trades against the same buy order
/// fill it 10 then 20/20 (Filled), each committing a settlement, a ledger row,
/// and outbox events atomically.
#[tokio::test]
async fn persist_matched_trade_commits_and_accumulates() {
    let pool = connect().await;
    let epoch = insert_epoch(&pool).await;
    let buyer_uid = Uuid::new_v4();
    let buy = insert_order(&pool, epoch, buyer_uid, OrderSide::Buy, dec!(20.0), OrderStatus::Active).await;
    let sell1 = insert_order(&pool, epoch, Uuid::new_v4(), OrderSide::Sell, dec!(10.0), OrderStatus::Active).await;
    let sell2 = insert_order(&pool, epoch, Uuid::new_v4(), OrderSide::Sell, dec!(10.0), OrderStatus::Active).await;

    let repo = PostgresSettlementRepository::new(pool.clone());

    // Trade 1: buy x sell1, 10 kWh.
    let s1 = settlement(epoch, buy, sell1, dec!(10.0));
    let m1 = order_match(epoch, buy, sell1, dec!(10.0));
    let committed = repo
        .persist_matched_trade(
            &s1,
            &m1,
            &matched_event(&m1),
            Some(1),
            &TradeFill { order_id: buy, user_id: Some(buyer_uid), delta: dec!(10.0), zone_id: Some(1) },
            &TradeFill { order_id: sell1, user_id: Some(Uuid::new_v4()), delta: dec!(10.0), zone_id: Some(1) },
        )
        .await
        .expect("persist trade 1");
    assert!(committed, "trade 1 commits");

    let (filled, status) = order_status(&pool, buy).await;
    assert_eq!(filled, dec!(10.0), "buy filled 10 after trade 1");
    assert_eq!(status, "partially_filled", "buy partially filled (10/20)");
    let (_, s1_status) = order_status(&pool, sell1).await;
    assert_eq!(s1_status, "filled", "sell1 fully filled (10/10)");

    // Trade 2: buy x sell2, 10 kWh → buy reaches 20/20.
    let s2 = settlement(epoch, buy, sell2, dec!(10.0));
    let m2 = order_match(epoch, buy, sell2, dec!(10.0));
    let committed = repo
        .persist_matched_trade(
            &s2,
            &m2,
            &matched_event(&m2),
            Some(1),
            &TradeFill { order_id: buy, user_id: Some(buyer_uid), delta: dec!(10.0), zone_id: Some(1) },
            &TradeFill { order_id: sell2, user_id: Some(Uuid::new_v4()), delta: dec!(10.0), zone_id: Some(1) },
        )
        .await
        .expect("persist trade 2");
    assert!(committed, "trade 2 commits");

    let (filled, status) = order_status(&pool, buy).await;
    assert_eq!(filled, dec!(20.0), "buy filled 20 after trade 2 (incremental accumulation)");
    assert_eq!(status, "filled", "buy fully filled (20/20)");

    // Both settlements + both ledger rows persisted.
    assert_eq!(count_rows(&pool, "SELECT COUNT(*) AS n FROM settlements WHERE epoch_id = $1", epoch).await, 2);
    assert_eq!(count_rows(&pool, "SELECT COUNT(*) AS n FROM order_matches WHERE epoch_id = $1", epoch).await, 2);
    // OrderMatched event for each trade + OrderUpdate for the buy on each trade.
    assert!(count_rows(&pool, "SELECT COUNT(*) AS n FROM outbox_events WHERE event_type = 'OrderMatched' AND payload::text LIKE '%' || $1 || '%'", m1.id).await >= 1);
    assert!(count_rows(&pool, "SELECT COUNT(*) AS n FROM outbox_events WHERE event_type = 'OrderUpdate' AND payload::text LIKE '%' || $1 || '%'", buy).await >= 2);

    // Cleanup.
    sqlx::query("DELETE FROM order_matches WHERE epoch_id = $1").bind(epoch).execute(&pool).await.ok();
    sqlx::query("DELETE FROM settlements WHERE epoch_id = $1").bind(epoch).execute(&pool).await.ok();
    sqlx::query("DELETE FROM trading_orders WHERE epoch_id = $1").bind(epoch).execute(&pool).await.ok();
    sqlx::query("DELETE FROM market_epochs WHERE id = $1").bind(epoch).execute(&pool).await.ok();
}

/// Atomicity: if either order is already terminal (the reaper expired it), the
/// guarded fill matches 0 rows and the WHOLE trade rolls back — no settlement,
/// no ledger row, no fill, no events — and `persist_matched_trade` returns false.
#[tokio::test]
async fn persist_matched_trade_rolls_back_when_a_side_is_terminal() {
    let pool = connect().await;
    let epoch = insert_epoch(&pool).await;
    let buy = insert_order(&pool, epoch, Uuid::new_v4(), OrderSide::Buy, dec!(10.0), OrderStatus::Active).await;
    // Seller already expired (reaper won the race).
    let sell = insert_order(&pool, epoch, Uuid::new_v4(), OrderSide::Sell, dec!(10.0), OrderStatus::Expired).await;

    let repo = PostgresSettlementRepository::new(pool.clone());
    let s = settlement(epoch, buy, sell, dec!(10.0));
    let m = order_match(epoch, buy, sell, dec!(10.0));

    let committed = repo
        .persist_matched_trade(
            &s,
            &m,
            &matched_event(&m),
            Some(1),
            &TradeFill { order_id: buy, user_id: Some(Uuid::new_v4()), delta: dec!(10.0), zone_id: Some(1) },
            &TradeFill { order_id: sell, user_id: Some(Uuid::new_v4()), delta: dec!(10.0), zone_id: Some(1) },
        )
        .await
        .expect("persist call itself succeeds");
    assert!(!committed, "trade with a terminal side does NOT commit");

    // NOTHING was written — the settlement (inserted before the fills) rolled back too.
    assert_eq!(count_rows(&pool, "SELECT COUNT(*) AS n FROM settlements WHERE id = $1", s.id).await, 0, "settlement rolled back — not orphaned");
    assert_eq!(count_rows(&pool, "SELECT COUNT(*) AS n FROM order_matches WHERE id = $1", m.id).await, 0, "ledger row rolled back");
    // The buy order was NOT resurrected/filled — it stays live for the next cycle.
    let (filled, status) = order_status(&pool, buy).await;
    assert_eq!(filled, dec!(0.0), "buy not filled");
    assert_eq!(status, "active", "buy still live");
    // No OrderMatched event leaked.
    assert_eq!(count_rows(&pool, "SELECT COUNT(*) AS n FROM outbox_events WHERE payload::text LIKE '%' || $1 || '%'", m.id).await, 0, "no events for the rolled-back trade");

    sqlx::query("DELETE FROM trading_orders WHERE epoch_id = $1").bind(epoch).execute(&pool).await.ok();
    sqlx::query("DELETE FROM market_epochs WHERE id = $1").bind(epoch).execute(&pool).await.ok();
}
