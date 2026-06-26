#![allow(clippy::unwrap_used)] // unwrap is idiomatic in integration tests

//! Live-Postgres coverage for `OrderRepository::expire_stale_orders`: the bulk
//! status flip to Expired, the per-row outbox event, and the open-status /
//! expires_at predicate. Needs a migrated database (see CLAUDE.md); run with
//! `cargo test -p trading-service --test order_expiry_integration_test`.

use chrono::{Duration, Utc};
use rust_decimal_macros::dec;
use sqlx::{PgPool, Row};
use trading_core::traits::OrderRepository;
use trading_core::types::{OrderSide, OrderStatus, OrderType, TimeInForce};
use trading_persistence::repositories::PostgresOrderRepository;
use uuid::Uuid;

#[tokio::test]
async fn test_expire_stale_orders_e2e() {
    let db_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("TRADING_DATABASE_URL"))
        .unwrap_or_else(|_| {
            "postgresql://gridtokenx_user:gridtokenx_password@localhost:7001/gridtokenx".to_string()
        });
    let pool = PgPool::connect(&db_url)
        .await
        .expect("Failed to connect to postgres");

    // Foreign keys: an order needs a user and a market epoch.
    let user_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO users (id, email, username, password_hash, wallet_address) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(user_id)
    .bind(format!("expiry-{user_id}@gridtokenx.com"))
    .bind(format!("expiry_user_{user_id}"))
    .bind("mock_hash")
    .bind(format!("Wallet_{}", &user_id.to_string()[..32]))
    .execute(&pool)
    .await
    .expect("insert user");

    let epoch_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO market_epochs (id, epoch_number, start_time, end_time, status) VALUES ($1, $2, $3, $4, 'pending')",
    )
    .bind(epoch_id)
    .bind(Utc::now().timestamp_nanos_opt().unwrap_or(0))
    .bind(Utc::now())
    .bind(Utc::now() + Duration::minutes(15))
    .execute(&pool)
    .await
    .expect("insert epoch");

    let now = Utc::now();
    let past = now - Duration::hours(1);
    let future = now + Duration::hours(1);

    // Raw insert so we control status / expires_at / filled_amount (insert_order
    // omits those columns). Returns the new id.
    let insert = |status: OrderStatus,
                  side: OrderSide,
                  filled: rust_decimal::Decimal,
                  expires: Option<chrono::DateTime<Utc>>| {
        let pool = pool.clone();
        async move {
            let id = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO trading_orders (id, user_id, order_type, side, energy_amount, \
                 price_per_kwh, filled_amount, status, time_in_force, zone_id, epoch_id, expires_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
            )
            .bind(id)
            .bind(user_id)
            .bind(OrderType::Limit)
            .bind(side)
            .bind(dec!(10.0))
            .bind(dec!(1.0))
            .bind(filled)
            .bind(status)
            .bind(TimeInForce::Gtc)
            .bind(Some(1_i32))
            .bind(epoch_id)
            .bind(expires)
            .execute(&pool)
            .await
            .expect("insert order");
            id
        }
    };

    // A,D: open + already expired → must be reaped.
    let a = insert(OrderStatus::Active, OrderSide::Buy, dec!(0.0), Some(past)).await;
    let d = insert(OrderStatus::PartiallyFilled, OrderSide::Sell, dec!(4.0), Some(past)).await;
    // B: open but not yet expired. C: open, never expires. E: terminal (filled),
    // expired — its status is outside the open set, so the reaper ignores it.
    let b = insert(OrderStatus::Active, OrderSide::Buy, dec!(0.0), Some(future)).await;
    let c = insert(OrderStatus::Active, OrderSide::Sell, dec!(0.0), None).await;
    let e = insert(OrderStatus::Filled, OrderSide::Buy, dec!(10.0), Some(past)).await;

    let repo = PostgresOrderRepository::new(pool.clone());
    let reaped = repo
        .expire_stale_orders(now)
        .await
        .expect("expire_stale_orders");

    // Only the open + expired orders come back.
    let reaped: std::collections::HashSet<Uuid> = reaped.into_iter().collect();
    assert_eq!(
        reaped,
        [a, d].into_iter().collect(),
        "only open + expired orders are reaped"
    );

    // DB status reflects the reap; untouched orders keep their status.
    let status_of = |id: Uuid| {
        let pool = pool.clone();
        async move {
            sqlx::query("SELECT status::text FROM trading_orders WHERE id = $1")
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap()
                .get::<String, _>("status")
        }
    };
    assert_eq!(status_of(a).await, "expired", "A reaped");
    assert_eq!(status_of(d).await, "expired", "D reaped (partially_filled)");
    assert_eq!(status_of(b).await, "active", "B not yet expired");
    assert_eq!(status_of(c).await, "active", "C never expires");
    assert_eq!(status_of(e).await, "filled", "E terminal, untouched");

    // Each reaped order got its OrderUpdate outbox event in the same transaction.
    for id in [a, d] {
        let count: i64 = sqlx::query(
            "SELECT COUNT(*) AS n FROM outbox_events \
             WHERE event_type = 'OrderUpdate' AND payload::text LIKE $1",
        )
        .bind(format!("%{id}%"))
        .fetch_one(&pool)
        .await
        .unwrap()
        .get("n");
        assert!(count >= 1, "reaped order {id} must have an OrderUpdate event");
    }

    // Cleanup — cascades delete the orders.
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(&pool)
        .await
        .ok();
    sqlx::query("DELETE FROM market_epochs WHERE id = $1")
        .bind(epoch_id)
        .execute(&pool)
        .await
        .ok();
}
