use gridtokenx_trading_service::domain::events::Event;
use gridtokenx_trading_service::domain::trading::engine::rehydration::StateRehydrator;
use gridtokenx_trading_service::domain::trading::models::TradingOrderDb;
use gridtokenx_trading_service::infra::db::schema::types::{OrderSide, OrderStatus, OrderType, TimeInForce};
use gridtokenx_trading_service::infra::events::kafka_consumer::KafkaConsumer;
use rust_decimal::Decimal;
use std::collections::HashMap;
use uuid::Uuid;

#[tokio::test]
async fn test_rehydration_logic_simulation() {
    // Setup rehydrator - Note: we won't use the consumer for this unit-level check
    // but we need it to satisfy the constructor. We'll use a dummy bootstrap.
    let topics = vec!["orders_created".to_string(), "orders_updated".to_string()];
    let consumer = KafkaConsumer::new("localhost:9001", topics, Some("test-group".to_string())).unwrap();
    let rehydrator = StateRehydrator::new(consumer);
    
    let mut orders: HashMap<Uuid, TradingOrderDb> = HashMap::new();
    
    // 1. Create an order
    let order_id = Uuid::new_v4();
    let user_id = Uuid::new_v4();
    let order = TradingOrderDb {
        id: order_id,
        user_id,
        order_type: OrderType::Limit,
        side: OrderSide::Buy,
        energy_amount: Decimal::new(100, 0),
        price_per_kwh: Decimal::new(5, 1),
        filled_amount: Some(Decimal::ZERO),
        status: OrderStatus::Active,
        expires_at: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
        created_at: Some(chrono::Utc::now()),
        filled_at: None,
        epoch_id: Some(Uuid::new_v4()),
        zone_id: Some(1),
        meter_id: None,
        refund_tx_signature: None,
        order_pda: None,
        order_index: None,
        session_token: None,
        trigger_price: None,
        trigger_type: None,
        trigger_status: None,
        trailing_offset: None,
        triggered_at: None,
        last_peak_price: None,
        blockchain_status: None,
        blockchain_tx_hash: None,
        blockchain_error: None,
        retry_count: 0,
        time_in_force: TimeInForce::Gtc,
    };
    
    rehydrator.handle_event(&mut orders, Event::OrderCreated(order.clone()));
    assert_eq!(orders.len(), 1);
    assert!(orders.contains_key(&order_id));
    
    // 2. Partially fill the order
    rehydrator.handle_event(&mut orders, Event::OrderUpdate {
        id: order_id,
        filled_amount: Decimal::new(40, 0),
        status: "partially_filled".to_string(),
    });
    assert_eq!(orders.len(), 1);
    assert_eq!(orders.get(&order_id).unwrap().filled_amount, Some(Decimal::new(40, 0)));
    assert_eq!(orders.get(&order_id).unwrap().status, OrderStatus::PartiallyFilled);
    
    // 3. Fully fill (should remove from rehydrated active set)
    rehydrator.handle_event(&mut orders, Event::OrderUpdate {
        id: order_id,
        filled_amount: Decimal::new(100, 0),
        status: "filled".to_string(),
    });
    assert_eq!(orders.len(), 0, "Filled order should be removed from active rehydrated state");
}
