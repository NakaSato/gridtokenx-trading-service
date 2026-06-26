#![allow(clippy::unwrap_used)] // unwrap is idiomatic in integration tests

//! Live-Postgres coverage for the Phase-4 epoch-clearing SQL:
//! `OrderRepository::get_epochs_due_for_clearing` (the elapsed-window predicate)
//! and `mark_epoch_cleared` (the `active`-guarded close + summary write-back, and
//! its idempotency). Needs a migrated database (see CLAUDE.md); run with
//! `cargo test -p trading-service --test epoch_clearing_integration_test`.

use chrono::{Duration, Utc};
use rust_decimal_macros::dec;
use sqlx::{PgPool, Row};
use std::collections::HashSet;
use trading_core::traits::OrderRepository;
use trading_persistence::repositories::PostgresOrderRepository;
use uuid::Uuid;

#[tokio::test]
async fn test_epoch_clearing_lifecycle_e2e() {
    let db_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("TRADING_DATABASE_URL"))
        .unwrap_or_else(|_| {
            "postgresql://gridtokenx_user:gridtokenx_password@localhost:7001/gridtokenx".to_string()
        });
    let pool = PgPool::connect(&db_url)
        .await
        .expect("Failed to connect to postgres");

    let now = Utc::now();
    let past = now - Duration::minutes(20);
    let future = now + Duration::minutes(20);
    // Unique epoch_number base (column is UNIQUE) — offset per row.
    let base = now.timestamp_nanos_opt().unwrap_or(0);

    let insert = |id: Uuid, num_off: i64, status: &'static str, end: chrono::DateTime<Utc>| {
        let pool = pool.clone();
        async move {
            sqlx::query(
                "INSERT INTO market_epochs (id, epoch_number, start_time, end_time, status) \
                 VALUES ($1, $2, $3, $4, $5::epoch_status)",
            )
            .bind(id)
            .bind(base + num_off)
            .bind(end - Duration::minutes(15))
            .bind(end)
            .bind(status)
            .execute(&pool)
            .await
            .expect("insert epoch");
        }
    };

    // E1: active + window elapsed → due. E2: active but future → not due.
    // E3: already cleared → not due.
    let (e1, e2, e3) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    insert(e1, 0, "active", past).await;
    insert(e2, 1, "active", future).await;
    insert(e3, 2, "cleared", past).await;

    let repo = PostgresOrderRepository::new(pool.clone());

    // Only the elapsed, still-active epoch is due.
    let due: HashSet<Uuid> = repo
        .get_epochs_due_for_clearing()
        .await
        .expect("get_epochs_due_for_clearing")
        .into_iter()
        .collect();
    assert!(due.contains(&e1), "elapsed active epoch is due");
    assert!(!due.contains(&e2), "future epoch is not due");
    assert!(!due.contains(&e3), "already-cleared epoch is not due");

    // Close E1 with a summary.
    repo.mark_epoch_cleared(e1, Some(dec!(0.8)), dec!(10.0), 1)
        .await
        .expect("mark_epoch_cleared");

    let row = sqlx::query(
        "SELECT status::text, clearing_price, total_volume, matched_orders \
         FROM market_epochs WHERE id = $1",
    )
    .bind(e1)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("status"), "cleared");
    assert_eq!(row.get::<rust_decimal::Decimal, _>("clearing_price"), dec!(0.8));
    assert_eq!(row.get::<rust_decimal::Decimal, _>("total_volume"), dec!(10.0));
    assert_eq!(row.get::<i64, _>("matched_orders"), 1);

    // Idempotency: a second call finds no `active` row to update, so the stamped
    // summary is unchanged (the WHERE status='active' guard held).
    repo.mark_epoch_cleared(e1, Some(dec!(9.9)), dec!(99.0), 9)
        .await
        .expect("idempotent mark_epoch_cleared");
    let price: rust_decimal::Decimal =
        sqlx::query("SELECT clearing_price FROM market_epochs WHERE id = $1")
            .bind(e1)
            .fetch_one(&pool)
            .await
            .unwrap()
            .get("clearing_price");
    assert_eq!(price, dec!(0.8), "second close is a no-op; summary unchanged");

    // E1 no longer due after clearing.
    let due_after: HashSet<Uuid> = repo
        .get_epochs_due_for_clearing()
        .await
        .unwrap()
        .into_iter()
        .collect();
    assert!(!due_after.contains(&e1), "cleared epoch drops out of the work-list");

    // Cleanup.
    for id in [e1, e2, e3] {
        sqlx::query("DELETE FROM market_epochs WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await
            .ok();
    }
}

#[tokio::test]
async fn test_list_recent_cleared_epochs_e2e() {
    let db_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("TRADING_DATABASE_URL"))
        .unwrap_or_else(|_| {
            "postgresql://gridtokenx_user:gridtokenx_password@localhost:7001/gridtokenx".to_string()
        });
    let pool = PgPool::connect(&db_url).await.expect("connect");

    let now = Utc::now();
    let base = now.timestamp_nanos_opt().unwrap_or(0);

    let insert = |id: Uuid, off: i64, end: chrono::DateTime<Utc>| {
        let pool = pool.clone();
        async move {
            sqlx::query(
                "INSERT INTO market_epochs (id, epoch_number, start_time, end_time, status) \
                 VALUES ($1, $2, $3, $4, 'active'::epoch_status)",
            )
            .bind(id)
            .bind(base + off)
            .bind(end - Duration::minutes(15))
            .bind(end)
            .execute(&pool)
            .await
            .expect("insert epoch");
        }
    };

    // c1: elapsed, then closed with a summary → must be listed. a1: still active
    // (future) → must NOT be listed.
    let (c1, a1) = (Uuid::new_v4(), Uuid::new_v4());
    insert(c1, 0, now - Duration::minutes(20)).await;
    insert(a1, 1, now + Duration::minutes(20)).await;

    let repo = PostgresOrderRepository::new(pool.clone());
    repo.mark_epoch_cleared(c1, Some(dec!(1.25)), dec!(42.0), 3)
        .await
        .expect("close c1");

    let listed = repo.list_recent_cleared_epochs(100).await.expect("list");
    let found = listed.iter().find(|e| e.id == c1).expect("cleared epoch listed");
    assert_eq!(found.clearing_price, Some(dec!(1.25)));
    assert_eq!(found.total_volume, Some(dec!(42.0)));
    assert_eq!(found.matched_orders, Some(3));
    assert!(!listed.iter().any(|e| e.id == a1), "active epoch is not a clearing result");

    for id in [c1, a1] {
        sqlx::query("DELETE FROM market_epochs WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await
            .ok();
    }
}
