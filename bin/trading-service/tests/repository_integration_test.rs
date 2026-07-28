#![allow(clippy::unwrap_used)] // unwrap is idiomatic in integration tests

use sqlx::PgPool;
use uuid::Uuid;
use chrono::Utc;
use rust_decimal_macros::dec;
use trading_core::traits::{MeterRepository, OrderRepository, SettlementRepository};
use trading_core::models::{TradingOrder, Settlement, SettlementStatus};
use trading_core::types::{OrderSide, OrderStatus, OrderType, TimeInForce};
use trading_persistence::repositories::{PostgresMeterRepository, PostgresOrderRepository, PostgresSettlementRepository};

#[tokio::test]
async fn test_postgres_order_repository_e2e() {
    // 1. Establish central database connection
    let db_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("TRADING_DATABASE_URL"))
        .unwrap_or_else(|_| "postgresql://gridtokenx_user:gridtokenx_password@localhost:7001/gridtokenx_trading".to_string());

    let pool = PgPool::connect(&db_url).await.expect("Failed to connect to postgres");

    // 2. Insert test user to satisfy user foreign key constraint
    let user_id = Uuid::new_v4();
    let email = format!("test-user-{}@gridtokenx.com", user_id);
    let username = format!("test_user_{}", user_id);
    let wallet = format!("Wallet_{}", user_id.to_string()[..32].to_string());

    sqlx::query(
        "INSERT INTO users (id, email, username, password_hash, wallet_address) VALUES ($1, $2, $3, $4, $5)"
    )
    .bind(user_id)
    .bind(&email)
    .bind(&username)
    .bind("mock_hash")
    .bind(&wallet)
    .execute(&pool)
    .await
    .expect("Failed to insert test user");

    // 3. Insert test epoch to satisfy epoch foreign key constraint
    let epoch_id = Uuid::new_v4();
    // Unique-id-derived, not wall-clock — avoids parallel-test collisions on
    // market_epochs_epoch_number_key.
    let epoch_number = (epoch_id.as_u128() as u64 >> 1) as i64;
    let start_time = Utc::now();
    let end_time = Utc::now() + chrono::Duration::minutes(15);

    sqlx::query(
        "INSERT INTO market_epochs (id, epoch_number, start_time, end_time, status) VALUES ($1, $2, $3, $4, 'pending')"
    )
    .bind(epoch_id)
    .bind(epoch_number)
    .bind(start_time)
    .bind(end_time)
    .execute(&pool)
    .await
    .expect("Failed to insert test epoch");

    let order_repo = PostgresOrderRepository::new(pool.clone());
    let settlement_repo = PostgresSettlementRepository::new(pool.clone());

    // 4. Prepare test entities with unique IDs
    let order_id = Uuid::new_v4();
    let order = TradingOrder {
        id: order_id,
        user_id,
        order_type: OrderType::Limit,
        side: OrderSide::Buy,
        energy_amount: dec!(15.5),
        price_per_kwh: dec!(4.25),
        filled_amount: dec!(0.0),
        status: OrderStatus::Active,
        expires_at: None,
        created_at: Some(Utc::now()),
        filled_at: None,
        epoch_id: Some(epoch_id),
        zone_id: Some(1),
        meter_id: None,
        refund_tx_signature: None,
        order_pda: Some("OrderPDA111111111111111111111111111111111".to_string()),
        order_index: Some(42),
        session_token: None,
        blockchain_status: None,
        blockchain_tx_hash: None,
        blockchain_error: None,
        retry_count: 0,
        time_in_force: TimeInForce::Gtc,
        market_segment: trading_core::types::MarketSegment::Realtime,
    };

    // 5. Test insert and fetch
    order_repo.insert_order(&order).await.expect("Failed to insert order");

    let fetched = order_repo.get_order(order_id).await.expect("Failed to get order").expect("Order not found");
    assert_eq!(fetched.id, order_id);
    assert_eq!(fetched.user_id, user_id);
    assert_eq!(fetched.energy_amount, dec!(15.5));
    assert_eq!(fetched.price_per_kwh, dec!(4.25));
    assert_eq!(fetched.order_index, Some(42));

    // 6. Test status and fill updates
    order_repo.update_filled_amount(order_id, dec!(5.5), OrderStatus::PartiallyFilled).await.expect("Failed to update filled amount");
    let fetched_after_update = order_repo.get_order(order_id).await.unwrap().unwrap();
    assert_eq!(fetched_after_update.filled_amount, dec!(5.5));
    assert_eq!(fetched_after_update.status, OrderStatus::PartiallyFilled);

    // 7. Test settlement operations
    let settlement_id = Uuid::new_v4();
    let buy_order_id = order_id;
    let sell_order_id = Uuid::new_v4();

    // To satisfy sell_order_id foreign key constraint, we must also insert a sell order!
    let sell_order = TradingOrder {
        id: sell_order_id,
        user_id,
        order_type: OrderType::Limit,
        side: OrderSide::Sell,
        energy_amount: dec!(15.5),
        price_per_kwh: dec!(4.25),
        filled_amount: dec!(0.0),
        status: OrderStatus::Active,
        expires_at: None,
        created_at: Some(Utc::now()),
        filled_at: None,
        epoch_id: Some(epoch_id),
        zone_id: Some(1),
        meter_id: None,
        refund_tx_signature: None,
        order_pda: Some("OrderPDA222222222222222222222222222222222".to_string()),
        order_index: Some(43),
        session_token: None,
        blockchain_status: None,
        blockchain_tx_hash: None,
        blockchain_error: None,
        retry_count: 0,
        time_in_force: TimeInForce::Gtc,
        market_segment: trading_core::types::MarketSegment::Realtime,
    };
    order_repo.insert_order(&sell_order).await.expect("Failed to insert sell order");

    let settlement = Settlement {
        id: settlement_id,
        trade_id: None,
        epoch_id,
        buyer_id: user_id,
        seller_id: user_id, // buy and sell user can be same for this schema test
        buy_order_id,
        sell_order_id,
        energy_amount: dec!(5.5),
        price: dec!(4.25),
        total_amount: dec!(23.375),
        fee_amount: dec!(0.0),
        net_amount: dec!(23.375),
        status: SettlementStatus::Pending,
        blockchain_tx: None,
        created_at: Utc::now(),
        confirmed_at: None,
        wheeling_charge: Some(dec!(0.1)),
        loss_factor: Some(dec!(1.02)),
        loss_cost: Some(dec!(0.05)),
        effective_energy: Some(dec!(5.5)),
        buyer_zone_id: Some(1),
        seller_zone_id: Some(1),
        buyer_session_token: None,
        seller_session_token: None,
        erc_certificate_id: None,
        erc_transfer_tx: None,
        retry_count: 0,
        error_message: None,
    };

    settlement_repo.insert_settlement(&settlement).await.expect("Failed to insert settlement");
    let fetched_settlement = settlement_repo.get_settlement(settlement_id).await.unwrap().unwrap();
    assert_eq!(fetched_settlement.id, settlement_id);
    assert_eq!(fetched_settlement.energy_amount, dec!(5.5));

    // 8. Cleanup — delete the rows this test created, explicitly.
    // These used to disappear via `trading_orders_user_id_fkey ON DELETE CASCADE`,
    // but the DB-per-service split dropped every cross-domain FK to IAM `users`
    // (migration 20260728000000). Without an explicit delete the fixture orders
    // survive teardown, and `market_epochs` deletion only NULLs their epoch
    // (ON DELETE SET NULL) — leaving orphaned active orders in the shared dev
    // order book that the live matcher then tries, and fails, to settle.
    sqlx::query("DELETE FROM settlements WHERE id = $1").bind(settlement_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM trading_orders WHERE user_id = $1").bind(user_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM market_epochs WHERE id = $1").bind(epoch_id).execute(&pool).await.ok();
}

/// db-split guard: meter identity lookups must read the Trading-owned
/// `meter_read_model`, NOT the metering `meters` table (which no longer lives in
/// the trading DB). A regression here 500s both active-order-meters endpoints —
/// `get_serials_for_ids` runs on every populated response to translate
/// `meter_id` into the map's `meter_serial` id space.
#[tokio::test]
async fn test_meter_repository_reads_read_model() {
    let db_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("TRADING_DATABASE_URL"))
        .unwrap_or_else(|_| "postgresql://gridtokenx_user:gridtokenx_password@localhost:7001/gridtokenx_trading".to_string());
    let pool = PgPool::connect(&db_url).await.expect("connect");

    let meter_id = Uuid::new_v4();
    let user_id = Uuid::new_v4();
    let serial = format!("SN-REPO-{}", &meter_id.to_string()[..8]);

    // Read model is fed by meter NATS events; here we seed one row directly.
    // updated_at defaults to now(); user_id has no FK (read-model table).
    sqlx::query(
        "INSERT INTO meter_read_model (meter_id, serial_number, user_id, zone_id) VALUES ($1, $2, $3, $4)"
    )
    .bind(meter_id)
    .bind(&serial)
    .bind(user_id)
    .bind(7_i32)
    .execute(&pool)
    .await
    .expect("Failed to insert meter_read_model row");

    let repo = PostgresMeterRepository::new(pool.clone());

    // meter_id -> serial (the response-shaping path)
    let serials = repo.get_serials_for_ids(&[meter_id]).await.expect("get_serials_for_ids");
    assert_eq!(
        serials.get(&meter_id).map(String::as_str),
        Some(serial.as_str()),
        "meter_id must resolve to its serial via meter_read_model"
    );

    // serial -> meter_id (the order-submission path)
    let resolved = repo.resolve_id_by_serial(&serial).await.expect("resolve_id_by_serial");
    assert_eq!(resolved, Some(meter_id), "serial must resolve back to the meter_id");

    // Empty input short-circuits without touching the DB.
    let empty = repo.get_serials_for_ids(&[]).await.expect("empty ids");
    assert!(empty.is_empty());

    // Unknown serial yields None, not an error.
    let missing = repo.resolve_id_by_serial("SN-DOES-NOT-EXIST").await.expect("missing serial");
    assert_eq!(missing, None);

    sqlx::query("DELETE FROM meter_read_model WHERE meter_id = $1").bind(meter_id).execute(&pool).await.ok();
}

/// Phase 0: the market_segment column round-trips. An Interval order inserted
/// via the repository must read back as Interval — not silently default to
/// Realtime (which would route it to the CDA matcher instead of the clearing
/// worker).
#[tokio::test]
async fn test_market_segment_round_trips() {
    let db_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("TRADING_DATABASE_URL"))
        .unwrap_or_else(|_| "postgresql://gridtokenx_user:gridtokenx_password@localhost:7001/gridtokenx_trading".to_string());
    let pool = PgPool::connect(&db_url).await.expect("connect");

    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, username, password_hash, wallet_address) VALUES ($1,$2,$3,$4,$5)")
        .bind(user_id)
        .bind(format!("seg-{user_id}@gridtokenx.com"))
        .bind(format!("seg_user_{user_id}"))
        .bind("mock_hash")
        .bind(format!("Wallet_{}", &user_id.to_string()[..32]))
        .execute(&pool).await.expect("insert user");

    let epoch_id = Uuid::new_v4();
    // Derive epoch_number from the epoch UUID (epoch_number is UNIQUE) — a
    // timestamp-based number would collide with the sibling test running in
    // parallel at the same nanosecond.
    let epoch_num = (epoch_id.as_u128() & 0x7FFF_FFFF_FFFF_FFFF) as i64;
    sqlx::query("INSERT INTO market_epochs (id, epoch_number, start_time, end_time, status) VALUES ($1,$2,$3,$4,'active'::epoch_status)")
        .bind(epoch_id)
        .bind(epoch_num)
        .bind(Utc::now())
        .bind(Utc::now() + chrono::Duration::minutes(15))
        .execute(&pool).await.expect("insert epoch");

    let repo = PostgresOrderRepository::new(pool.clone());
    let order_id = Uuid::new_v4();
    let mut order = TradingOrder {
        id: order_id,
        user_id,
        order_type: OrderType::Limit,
        side: OrderSide::Buy,
        energy_amount: dec!(5.0),
        price_per_kwh: dec!(2.0),
        filled_amount: dec!(0.0),
        status: OrderStatus::Active,
        expires_at: None,
        created_at: Some(Utc::now()),
        filled_at: None,
        epoch_id: Some(epoch_id),
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
        market_segment: trading_core::types::MarketSegment::Interval,
    };
    repo.insert_order(&order).await.expect("insert interval order");
    let fetched = repo.get_order(order_id).await.unwrap().unwrap();
    assert_eq!(
        fetched.market_segment,
        trading_core::types::MarketSegment::Interval,
        "interval segment must survive the DB round-trip"
    );

    // A defaulted (realtime) order also round-trips.
    order.id = Uuid::new_v4();
    order.market_segment = trading_core::types::MarketSegment::Realtime;
    repo.insert_order(&order).await.expect("insert realtime order");
    let rt = repo.get_order(order.id).await.unwrap().unwrap();
    assert_eq!(rt.market_segment, trading_core::types::MarketSegment::Realtime);

    // Explicit order cleanup — no cross-domain FK cascade to rely on any more
    // (see the teardown note in the repository round-trip test above).
    sqlx::query("DELETE FROM trading_orders WHERE user_id = $1").bind(user_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM market_epochs WHERE id = $1").bind(epoch_id).execute(&pool).await.ok();
}
