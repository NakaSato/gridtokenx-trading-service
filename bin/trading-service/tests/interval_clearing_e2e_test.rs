#![allow(clippy::unwrap_used)] // unwrap is idiomatic in integration tests

//! Chained end-to-end coverage of the interval (uniform-price) track against a
//! live Postgres: place crossing Interval orders bound to an epoch, elapse the
//! epoch, run the clearing worker's step, and assert the orders cleared at the
//! uniform price, settlements were booked, and the epoch was closed. This is the
//! seam the per-stage tests don't cover on their own.
//!
//! Run: `cargo test -p trading-service --test interval_clearing_e2e_test`

use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;
use sqlx::PgPool;
use trading_core::fast_price::FastPrice;
use trading_core::models::TradingOrder;
use trading_core::traits::{OrderRepository, SettlementRepository};
use trading_core::types::{MarketSegment, OrderSide, OrderStatus, OrderType, TimeInForce};
use trading_engine::engine::TopologySnapshot;
use trading_logic::ClearingService;
use trading_persistence::repositories::{PostgresOrderRepository, PostgresSettlementRepository};
use uuid::Uuid;

mod common;

/// Zero wheeling, unit loss → uniform price is the clean bid/ask midpoint.
struct NoFee;
impl TopologySnapshot for NoFee {
    fn can_accommodate_flow(&self, _f: Option<i32>, _t: Option<i32>, _a: Decimal) -> bool {
        true
    }
    fn calculate_wheeling_charge(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice {
        FastPrice::from(dec!(0))
    }
    fn calculate_loss_factor(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice {
        FastPrice::from(dec!(1.0))
    }
}

fn interval_order(
    id: Uuid,
    user: Uuid,
    epoch: Uuid,
    side: OrderSide,
    price: Decimal,
) -> TradingOrder {
    TradingOrder {
        id,
        user_id: user,
        order_type: OrderType::Limit,
        side,
        energy_amount: dec!(10.0),
        price_per_kwh: price,
        filled_amount: dec!(0.0),
        status: OrderStatus::Active,
        expires_at: None,
        created_at: Some(Utc::now()),
        filled_at: None,
        epoch_id: Some(epoch),
        zone_id: Some(1),
        meter_id: None,
        refund_tx_signature: None,
        order_pda: None,
        order_index: None,
        session_token: None,
        blockchain_status: None,
        blockchain_tx_hash: None,
        blockchain_error: None,
        retry_count: 0,
        time_in_force: TimeInForce::Gtc,
        market_segment: MarketSegment::Interval,
    }
}

#[tokio::test]
async fn test_interval_order_clears_end_to_end() {
    let db_url = common::test_db_url();
    let pool = PgPool::connect(&db_url).await.expect("connect");

    // Buyer + seller ids. No `users` rows are seeded: the FK they were inserted
    // for does not exist here. After the DB-per-service split this service owns
    // `gridtokenx_trading`, which has no `users` table (identities live in
    // `gridtokenx_iam`), and `trading_orders`'s only FK is
    // `epoch_id -> market_epochs(id)`. The old INSERT targeted the pre-split
    // shared `gridtokenx` DB and failed with `relation "users" does not exist`.
    let buyer = Uuid::new_v4();
    let seller = Uuid::new_v4();

    // An epoch whose window has already elapsed → due for clearing this tick.
    let epoch = Uuid::new_v4();
    let epoch_num = (epoch.as_u128() & 0x7FFF_FFFF_FFFF_FFFF) as i64;
    sqlx::query(
        "INSERT INTO market_epochs (id, epoch_number, start_time, end_time, status) \
         VALUES ($1, $2, $3, $4, 'active'::epoch_status)",
    )
    .bind(epoch)
    .bind(epoch_num)
    .bind(Utc::now() - Duration::minutes(20))
    .bind(Utc::now() - Duration::minutes(5))
    .execute(&pool)
    .await
    .expect("insert elapsed epoch");

    let order_repo = Arc::new(PostgresOrderRepository::new(pool.clone()));
    let settlement_repo = Arc::new(PostgresSettlementRepository::new(pool.clone()));

    // Crossing Interval orders in the same zone: bid 1.0, ask 0.6 → p* = 0.8.
    let (buy_id, sell_id) = (Uuid::new_v4(), Uuid::new_v4());
    order_repo
        .insert_order(&interval_order(buy_id, buyer, epoch, OrderSide::Buy, dec!(1.0)))
        .await
        .expect("insert buy");
    order_repo
        .insert_order(&interval_order(sell_id, seller, epoch, OrderSide::Sell, dec!(0.6)))
        .await
        .expect("insert sell");

    // Run the clearing worker's step.
    let clearing = ClearingService::new(
        order_repo.clone(),
        settlement_repo.clone(),
        Arc::new(NoFee),
        // Zero rates keep this test's assertions on gross amounts valid; the
        // charge arithmetic is covered by `trading_core::charges`.
        Arc::new(trading_core::charges::StaticChargeRates::ZERO),
    );
    let summaries = clearing.clear_due_epochs().await.expect("clear");

    let mine = summaries
        .into_iter()
        .find(|s| s.epoch_id == epoch)
        .expect("our epoch was cleared");
    assert_eq!(mine.matches, 1, "the crossing pair cleared");
    assert_eq!(mine.total_volume, dec!(10.0));
    assert_eq!(mine.zone_prices, vec![(Some(1), dec!(0.8))], "uniform midpoint price");

    // Epoch closed with the single-zone price stamped.
    let (status, price): (String, Option<Decimal>) =
        sqlx::query_as("SELECT status::text, clearing_price FROM market_epochs WHERE id = $1")
            .bind(epoch)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "cleared");
    assert_eq!(price, Some(dec!(0.8)));

    // Both orders filled.
    let b = order_repo.get_order(buy_id).await.unwrap().unwrap();
    let s = order_repo.get_order(sell_id).await.unwrap().unwrap();
    assert_eq!(b.status, OrderStatus::Filled, "buy filled");
    assert_eq!(s.status, OrderStatus::Filled, "sell filled");
    assert_eq!(b.filled_amount, dec!(10.0));

    // A settlement was booked at the uniform price.
    let (settle_count, settle_price): (i64, Option<Decimal>) = sqlx::query_as(
        "SELECT COUNT(*), MAX(price_per_kwh) FROM settlements WHERE buy_order_id = $1 AND sell_order_id = $2",
    )
    .bind(buy_id)
    .bind(sell_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(settle_count, 1, "one settlement for the pair");
    assert_eq!(settle_price, Some(dec!(0.8)));

    // Cleanup — the cross-domain FKs to IAM `users` were dropped by the
    // DB-per-service split (migration 20260728000000), so orders no longer
    // cascade away with their owner. Delete them explicitly or they linger as
    // orphaned rows in the shared dev order book.
    for id in [buyer, seller] {
        sqlx::query("DELETE FROM trading_orders WHERE user_id = $1").bind(id).execute(&pool).await.ok();
    }
    sqlx::query("DELETE FROM market_epochs WHERE id = $1").bind(epoch).execute(&pool).await.ok();
}
