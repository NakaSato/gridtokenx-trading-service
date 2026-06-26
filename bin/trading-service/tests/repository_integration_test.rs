#![allow(clippy::unwrap_used)] // unwrap is idiomatic in integration tests

use sqlx::PgPool;
use uuid::Uuid;
use chrono::Utc;
use rust_decimal_macros::dec;
use trading_core::traits::{OrderRepository, SettlementRepository};
use trading_core::models::{TradingOrder, Settlement, SettlementStatus};
use trading_core::types::{OrderSide, OrderStatus, OrderType, TimeInForce};
use trading_persistence::repositories::{PostgresOrderRepository, PostgresSettlementRepository};

#[tokio::test]
async fn test_postgres_order_repository_e2e() {
    // 1. Establish central database connection
    let db_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("TRADING_DATABASE_URL"))
        .unwrap_or_else(|_| "postgresql://gridtokenx_user:gridtokenx_password@localhost:7001/gridtokenx".to_string());

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
    let epoch_number = Utc::now().timestamp_nanos_opt().unwrap_or(0);
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

    // 8. Cleanup (Cascades delete order and settlement automatically when deleting user/epoch)
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM market_epochs WHERE id = $1").bind(epoch_id).execute(&pool).await.ok();
}
