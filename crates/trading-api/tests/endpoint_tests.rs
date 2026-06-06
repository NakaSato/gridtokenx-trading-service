use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use ed25519_dalek::{SigningKey, Signer};
use gridtokenx_blockchain_core::auth::{GATEWAY_SECRET_HEADER, INTERNAL_ROLE_HEADER};
use rust_decimal::Decimal;
use serde_json::json;
use std::sync::{Arc, Mutex};
use tower::ServiceExt;
use uuid::Uuid;
use trading_core::fast_price::FastPrice;

use trading_api::auth::USER_ID_HEADER;
use trading_api::startup::build_router;
use trading_api::state::AppState;
use trading_core::events::Event;
use trading_core::models::*;
use trading_core::traits::*;
use trading_core::types::*;

// ── Manual Stubs ────────────────────────────────────────────────────────────

#[derive(Default)]
struct MockSystem {
    pub orders: Mutex<Vec<TradingOrder>>,
    pub settlements: Mutex<Vec<Settlement>>,
    pub futures_products: Mutex<Vec<FuturesProduct>>,
    pub futures_orders: Mutex<Vec<FuturesOrder>>,
    pub futures_positions: Mutex<Vec<FuturesPosition>>,
    pub carbon_balances: Mutex<std::collections::HashMap<Uuid, Decimal>>,
    pub published_events: Mutex<Vec<Event>>,
}

#[async_trait]
impl OrderRepository for MockSystem {
    async fn insert_order(&self, order: &TradingOrder) -> TraitResult<()> {
        self.orders.lock().unwrap().push(order.clone());
        Ok(())
    }
    async fn get_or_create_active_epoch(&self) -> TraitResult<Uuid> {
        Ok(Uuid::nil())
    }
    async fn get_order(&self, id: Uuid) -> TraitResult<Option<TradingOrder>> {
        Ok(self.orders.lock().unwrap().iter().find(|o| o.id == id).cloned())
    }
    async fn get_orders_by_user(&self, user_id: Uuid, _limit: i64, _offset: i64) -> TraitResult<Vec<TradingOrder>> {
        Ok(self.orders.lock().unwrap().iter().filter(|o| o.user_id == user_id).cloned().collect())
    }
    async fn get_active_orders_by_zone(&self, _zone_id: i32) -> TraitResult<Vec<OrderBookEntry>> { Ok(vec![]) }
    async fn update_order_status(&self, _id: Uuid, _status: OrderStatus) -> TraitResult<()> { Ok(()) }
    async fn update_filled_amount(&self, _id: Uuid, _filled_amount: Decimal, _status: OrderStatus) -> TraitResult<()> { Ok(()) }
    async fn get_active_buy_orders(&self) -> TraitResult<Vec<TradingOrder>> { Ok(vec![]) }
    async fn get_active_sell_orders(&self) -> TraitResult<Vec<TradingOrder>> { Ok(vec![]) }
    async fn cancel_order(&self, _id: Uuid, _user_id: Uuid) -> TraitResult<bool> { Ok(true) }
    async fn bootstrap_active_orders(&self) -> TraitResult<Vec<TradingOrder>> { Ok(vec![]) }
}

#[async_trait]
impl SettlementRepository for MockSystem {
    async fn insert_settlement(&self, settlement: &Settlement) -> TraitResult<()> {
        self.settlements.lock().unwrap().push(settlement.clone());
        Ok(())
    }
    async fn get_settlement(&self, id: Uuid) -> TraitResult<Option<Settlement>> {
        Ok(self.settlements.lock().unwrap().iter().find(|s| s.id == id).cloned())
    }
    async fn insert_match(&self, _m: &trading_core::models::OrderMatch, _settlement_id: Option<Uuid>, _zone_id: Option<i32>) -> TraitResult<()> { Ok(()) }
    async fn get_pending_settlements(&self, _limit: i64) -> TraitResult<Vec<Settlement>> { Ok(vec![]) }
    async fn update_settlement_status(&self, _id: Uuid, _status: &str, _tx_hash: Option<&str>, _error: Option<&str>) -> TraitResult<()> { Ok(()) }
}

#[async_trait]
impl FuturesRepository for MockSystem {
    async fn get_products(&self) -> TraitResult<Vec<FuturesProduct>> { Ok(self.futures_products.lock().unwrap().clone()) }
    async fn get_product(&self, id: Uuid) -> TraitResult<Option<FuturesProduct>> { Ok(self.futures_products.lock().unwrap().iter().find(|p| p.id == id).cloned()) }
    async fn insert_order(&self, order: &FuturesOrder) -> TraitResult<()> { self.futures_orders.lock().unwrap().push(order.clone()); Ok(()) }
    async fn get_orders_by_user(&self, user_id: Uuid) -> TraitResult<Vec<FuturesOrder>> { Ok(self.futures_orders.lock().unwrap().iter().filter(|o| o.user_id == user_id).cloned().collect()) }
    async fn get_positions_by_user(&self, user_id: Uuid) -> TraitResult<Vec<FuturesPosition>> { Ok(self.futures_positions.lock().unwrap().iter().filter(|p| p.user_id == user_id).cloned().collect()) }
    async fn close_position(&self, id: Uuid) -> TraitResult<()> { self.futures_positions.lock().unwrap().retain(|p| p.id != id); Ok(()) }
}

#[async_trait]
impl CarbonRepository for MockSystem {
    async fn get_balance(&self, user_id: Uuid) -> TraitResult<Decimal> { Ok(*self.carbon_balances.lock().unwrap().get(&user_id).unwrap_or(&Decimal::ZERO)) }
    async fn get_history(&self, _user_id: Uuid) -> TraitResult<Vec<CarbonCredit>> { Ok(vec![]) }
    async fn get_transactions(&self, _user_id: Uuid) -> TraitResult<Vec<CarbonTransaction>> { Ok(vec![]) }
    async fn insert_transaction(&self, _tx: &CarbonTransaction) -> TraitResult<()> { Ok(()) }
}

#[async_trait]
impl AnalyticsRepository for MockSystem {
    async fn get_user_stats(&self, _user_id: Uuid) -> TraitResult<UserAnalytics> {
        Ok(UserAnalytics {
            total_traded_kwh: Decimal::ZERO,
            total_spent_grid: Decimal::ZERO,
            total_earned_grid: Decimal::ZERO,
            carbon_offset_tons: Decimal::ZERO,
            reliability_score: 0.98,
        })
    }
    async fn get_user_transactions(&self, _user_id: Uuid) -> TraitResult<Vec<TransactionData>> { Ok(vec![]) }
}

#[async_trait]
impl EventPublisher for MockSystem {
    async fn publish(&self, event: Event) -> TraitResult<()> { self.published_events.lock().unwrap().push(event); Ok(()) }
    async fn publish_to_topic(&self, _topic: &str, event: Event) -> TraitResult<()> { self.publish(event).await }
    async fn create_consumer_group(&self, _group_name: &str) -> TraitResult<()> { Ok(()) }
    async fn consume_events(&self, _g: &str, _c: &str, _h: Arc<dyn Fn(Event) -> std::pin::Pin<Box<dyn std::future::Future<Output = TraitResult<()>> + Send>> + Send + Sync>) -> TraitResult<()> { Ok(()) }
}

#[async_trait]
impl BlockchainGateway for MockSystem {
    async fn is_user_registered(&self, _u: Uuid) -> TraitResult<bool> { Ok(true) }
    async fn get_user_wallet(&self, _u: Uuid) -> TraitResult<Option<String>> { Ok(Some("wallet_address".to_string())) }
    async fn get_token_balance(&self, _w: &str) -> TraitResult<u64> { Ok(1000000000) }
    async fn get_zone_config(&self, zone_id: i32) -> TraitResult<ZoneConfig> {
        Ok(ZoneConfig {
            zone_id,
            incentive_multiplier: Decimal::ONE,
            wheeling_charge: Decimal::ZERO,
            maintenance_mode: false,
            last_updated: chrono::Utc::now(),
        })
    }
    async fn execute_settlement(&self, _s: &Settlement) -> TraitResult<SettlementTransaction> {
        Ok(SettlementTransaction {
            settlement_id: Uuid::new_v4(),
            signature: "sig".to_string(),
            slot: 1,
            confirmation_status: "finalized".to_string(),
        })
    }
    async fn execute_batched_settlements(&self, _s: Vec<Settlement>) -> TraitResult<Vec<SettlementTransaction>> { Ok(vec![]) }
    async fn issue_erc(&self, _u: Uuid, _m: &str, _a: Decimal) -> TraitResult<String> { Ok("erc_sig".to_string()) }
    async fn execute_generation_mint(&self, _w: &str, _a: Decimal, _t: i64) -> TraitResult<String> { Ok("mint_sig".to_string()) }
    async fn sync_total_supply(&self) -> TraitResult<String> { Ok("sync_sig".to_string()) }
    async fn execute_create_order(&self, _u: Uuid, _m: &str, _a: u64, _p: u64, _s: &str, _e: Option<&str>, _z: u32) -> TraitResult<(String, String, u64)> {
        Ok(("sig".to_string(), "pda".to_string(), 1))
    }
}

#[async_trait]
impl IdentityGateway for MockSystem {
    async fn sign_message(&self, _u: Uuid, _w: Option<String>, m: Vec<u8>) -> TraitResult<Vec<u8>> { Ok(m) }
}

#[async_trait]
impl AuditLog for MockSystem {
    async fn log_action(&self, _u: Uuid, _a: &str, _d: &str) -> TraitResult<()> { Ok(()) }
}

#[async_trait]
impl VppRepository for MockSystem {
    async fn get_cluster_by_id(&self, cluster_id: &str) -> TraitResult<Option<VppCluster>> {
        Ok(Some(VppCluster {
            id: Uuid::new_v4(),
            cluster_id: cluster_id.to_string(),
            zone_id: Some(1),
            total_capacity_kwh: 100.0,
            current_stored_kwh: 50.0,
            soc_percentage: 50.0,
            target_soc_percentage: 80.0,
            flex_up_kw: 10.0,
            flex_down_kw: 10.0,
            health_score: 1.0,
            resource_count: 5,
            dispatch_mode: "idle".to_string(),
            last_update: Some(chrono::Utc::now()),
            created_at: Some(chrono::Utc::now()),
        }))
    }
    async fn get_all_clusters(&self) -> TraitResult<Vec<VppCluster>> { Ok(vec![]) }
    async fn get_member_association(&self, _m: &str) -> TraitResult<Option<VppMember>> { Ok(None) }
    async fn get_cluster_members(&self, _c: &str) -> TraitResult<Vec<VppMember>> { Ok(vec![]) }
    async fn update_cluster_metrics(&self, _c: &str, _s: f64, _soc: f64, _fu: f64, _fd: f64) -> TraitResult<()> { Ok(()) }
}

struct TopologyStub;
impl trading_engine::engine::TopologySnapshot for TopologyStub {
    fn can_accommodate_flow(&self, _f: Option<i32>, _t: Option<i32>, _a: Decimal) -> bool { true }
    fn calculate_wheeling_charge(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice { FastPrice::from_raw(1000) }
    fn calculate_loss_factor(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice { FastPrice::from_raw(1000000) }
}

// ── Test Setup ──────────────────────────────────────────────────────────────

fn setup_test_state(oracle_pub_key: String) -> AppState {
    let mock = Arc::new(MockSystem::default());
    let config_json = json!({
        "environment": "test",
        "database_url": "postgres://localhost/test",
        "redis_url": "redis://localhost/0",
        "solana_rpc_url": "http://localhost:8899",
        "chain_bridge_url": "http://localhost:5040",
        "solana_ws_url": "ws://localhost:8900",
        "solana_cluster": "localnet",
        "energy_token_mint": "Mint111111111111111111111111111111111111111",
        "max_connections": 10,
        "log_level": "debug",
        "tokenization": {
            "kwh_to_token_ratio": 1.0,
            "decimals": 9,
            "max_reading_kwh": 100.0,
            "reading_max_age_days": 7,
            "auto_mint_enabled": true,
            "polling_interval_secs": 60,
            "batch_size": 50,
            "max_retry_attempts": 3,
            "initial_retry_delay_secs": 300,
            "retry_backoff_multiplier": 2.0,
            "max_retry_delay_secs": 3600,
            "transaction_timeout_secs": 60,
            "max_transactions_per_batch": 20,
            "enable_real_blockchain": false,
            "use_onchain_balance_for_escrow": false
        },
        "solana_programs": {
            "registry_program_id": "5xdQsDuGa1AaLVnddGhevvf2bngCvSob4dAepETS7oaJ",
            "oracle_program_id": "D5MCbSHxhxZTRFyUMdTHcQvjzwjx5Lb8jg9PQ2LTja8S",
            "energy_token_program_id": "EzXnJoHSjS6VR7eBwHTkHHAJGqVfRsEvyksqz7uJCBpe",
            "trading_program_id": "DA9TdkcToi5r7oS7X5CddoMBiGNF3sAGqwPQph1CfLwd",
            "governance_program_id": "BRQEyx7DHX1Ljx1eNTHUve52aHHwkWckBXGeL9FZPEgZ",
            "trading_market_id": "mqiBmZcWMc3mor3B8fnSE2xrKThqHW7HzjuhhGKtv9u"
        },
        "encryption_secret": "secret",
        "iam_service_url": "http://localhost:5010",
        "internal_api_key": "gridtokenx-gateway-secret-2025",
        "kafka_enabled": false,
        "kafka_bootstrap_servers": "localhost:9092",
        "kafka_topic_prefix": "trading",
        "role": "api",
        "platform_user_id": Uuid::nil(),
        "oracle_feed_in_tariff": "0.10",
        "oracle_bridge_public_key": oracle_pub_key
    });

    let config: trading_core::config::Config = serde_json::from_value(config_json).unwrap();
    let config_arc = Arc::new(config);

    let matcher = Arc::new(trading_logic::MatcherService::new(
        mock.clone(),
        mock.clone(),
        mock.clone(),
        Arc::new(TopologyStub),
    ));

    let settlement = Arc::new(trading_logic::SettlementService::new(
        mock.clone(),
        mock.clone(),
        mock.clone(),
        mock.clone(),
        Uuid::nil(),
        Decimal::new(45, 1),
        oracle_pub_key,
    ));

    let vpp = Arc::new(trading_logic::vpp::VppService::new(
        mock.clone(),
        mock.clone(),
        mock.clone(),
        Arc::new(trading_logic::forecasting::ForecastingService::new()),
    ));

    AppState {
        config: config_arc,
        order_repo: mock.clone(),
        settlement_repo: mock.clone(),
        futures_repo: mock.clone(),
        carbon_repo: mock.clone(),
        analytics_repo: mock.clone(),
        events: mock.clone(),
        blockchain: mock.clone(),
        identity: mock.clone(),
        audit: mock.clone(),
        matcher,
        settlement,
        vpp,
    }
}

async fn request(
    app: axum::Router,
    method: &str,
    uri: &str,
    user_id: Uuid,
    body: Body,
) -> axum::response::Response {
    app.oneshot(
        Request::builder()
            .method(method)
            .uri(uri)
            .header(INTERNAL_ROLE_HEADER, "admin")
            .header(GATEWAY_SECRET_HEADER, "gridtokenx-gateway-secret-2025")
            .header(USER_ID_HEADER, user_id.to_string())
            .header("Content-Type", "application/json")
            .body(body)
            .unwrap(),
    )
    .await
    .unwrap()
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_all_endpoints() {
    let secret_bytes = [0u8; 32];
    let signing_key = SigningKey::from_bytes(&secret_bytes);
    let verifying_key = signing_key.verifying_key();
    let oracle_pub_key = bs58::encode(verifying_key.to_bytes()).into_string();

    let state = setup_test_state(oracle_pub_key);
    let app = build_router(state);
    let user_id = Uuid::new_v4();

    // 1. Submit Order
    let res = request(app.clone(), "POST", "/api/v1/orders", user_id, Body::from(json!({
        "side": "buy",
        "order_type": "limit",
        "energy_amount_kwh": "100.5",
        "price_per_kwh": "4.5",
        "zone_id": 1
    }).to_string())).await;
    assert_eq!(res.status(), StatusCode::OK);

    // 2. Get Order by ID
    let res = request(app.clone(), "GET", &format!("/api/v1/orders/{}", Uuid::new_v4()), user_id, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::OK);

    // 3. List Orders
    let res = request(app.clone(), "GET", "/api/v1/orders", user_id, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::OK);

    // 4. Cancel Order
    let res = request(app.clone(), "DELETE", &format!("/api/v1/orders/{}", Uuid::new_v4()), user_id, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::OK);

    // 5. Create Quote
    let res = request(app.clone(), "POST", "/api/v1/quotes", user_id, Body::from(json!({
        "buyer_zone_id": 1,
        "seller_zone_id": 2,
        "energy_amount_kwh": "100",
        "agreed_price": "4.5"
    }).to_string())).await;
    assert_eq!(res.status(), StatusCode::OK);

    // 6. Order Book
    let res = request(app.clone(), "GET", "/api/v1/zones/1/book", user_id, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::OK);

    // 7. Market Stats
    let res = request(app.clone(), "GET", "/api/v1/stats", user_id, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::OK);

    // 8. Futures Products
    let res = request(app.clone(), "GET", "/api/v1/futures/products", user_id, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::OK);

    // 9. Futures Order
    let res = request(app.clone(), "POST", "/api/v1/futures/orders", user_id, Body::from(json!({
        "product_id": Uuid::new_v4().to_string(),
        "side": "buy",
        "order_type": "market",
        "quantity": 1.5,
        "price": 50000.0,
        "leverage": 10
    }).to_string())).await;
    assert_eq!(res.status(), StatusCode::OK);

    // 10. Futures Positions
    let res = request(app.clone(), "GET", "/api/v1/futures/positions", user_id, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::OK);

    // 11. Futures Orders (List)
    let res = request(app.clone(), "GET", "/api/v1/futures/orders", user_id, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::OK);

    // 12. Close Futures Position
    let res = request(app.clone(), "DELETE", &format!("/api/v1/futures/positions/{}", Uuid::new_v4()), user_id, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::OK);

    // 13. Wallet Balance
    let res = request(app.clone(), "GET", "/api/v1/wallets/addr123/balance", user_id, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::OK);

    // 14. Analytics Stats
    let res = request(app.clone(), "GET", "/api/v1/analytics/stats", user_id, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::OK);

    // 15. Transaction History
    let res = request(app.clone(), "GET", "/api/v1/transactions", user_id, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::OK);

    // 16. Carbon Balance
    let res = request(app.clone(), "GET", "/api/v1/carbon/balance", user_id, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::OK);

    // 17. Carbon History
    let res = request(app.clone(), "GET", "/api/v1/carbon/history", user_id, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::OK);

    // 18. Carbon Transactions
    let res = request(app.clone(), "GET", "/api/v1/carbon/transactions", user_id, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::OK);

    // 19. Carbon Transfers
    let res = request(app.clone(), "POST", "/api/v1/carbon/transfers", user_id, Body::from(json!({
        "to_user_id": Uuid::new_v4(),
        "amount": "10.0"
    }).to_string())).await;
    assert_eq!(res.status(), StatusCode::OK);

    // 20. Settlement Mint
    let amount = Decimal::new(100, 0);
    let start_time = 1700000000;
    let end_time = 1700003600;
    let meter_serial = "METER-001";
    let message = format!("{}:{}:{}:{}:{}", user_id, meter_serial, amount, start_time, end_time);
    let signature = signing_key.sign(message.as_bytes());
    let signature_str = bs58::encode(signature.to_bytes()).into_string();

    let res = request(app.clone(), "POST", "/api/v1/settlement/mint", user_id, Body::from(json!({
        "user_id": user_id,
        "meter_serial": meter_serial,
        "energy_generated_kwh": amount,
        "start_time": start_time,
        "end_time": end_time,
        "signature": signature_str
    }).to_string())).await;
    assert_eq!(res.status(), StatusCode::OK);

    // 21. Batch Settlement Mint
    let res = request(app.clone(), "POST", "/api/v1/settlement/generation-mint/batch", user_id, Body::from(json!({
        "requests": [
            {
                "user_id": user_id,
                "meter_serial": meter_serial,
                "energy_generated_kwh": amount,
                "start_time": start_time,
                "end_time": end_time,
                "signature": signature_str
            }
        ]
    }).to_string())).await;
    assert_eq!(res.status(), StatusCode::OK);

    // 22. Health
    let res = app.clone().oneshot(Request::builder().method("GET").uri("/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}
