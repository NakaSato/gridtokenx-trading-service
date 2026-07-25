#![allow(clippy::unwrap_used)] // unwrap is idiomatic in integration tests

use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use ed25519_dalek::SigningKey;
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

// gRPC (ConnectRPC) handler test surface
use buffa::view::OwnedView;
use connectrpc::Context;
use trading_api::handlers::TradingGrpcService;
use trading_protocol::trading_proto::{
    BatchExecuteSettlementsRequest, CancelOrderRequest, DispatchVppRequest, GetOrderRequest,
    ListOrdersRequest, SubmitOrderRequest, TradingService,
};

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
    pub price_alerts: Mutex<Vec<PriceAlert>>,
    pub recurring: Mutex<Vec<RecurringOrder>>,
}

#[async_trait]
impl OrderRepository for MockSystem {
    async fn insert_order(&self, order: &TradingOrder) -> TraitResult<()> {
        self.orders.lock().unwrap().push(order.clone());
        Ok(())
    }
    async fn insert_order_with_event(
        &self,
        order: &TradingOrder,
        _event: &trading_core::events::Event,
    ) -> TraitResult<()> {
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
    async fn get_active_orders_by_zone(&self, zone_id: i32) -> TraitResult<Vec<OrderBookEntry>> {
        Ok(self
            .orders
            .lock()
            .unwrap()
            .iter()
            .filter(|o| is_active(o.status) && o.zone_id == Some(zone_id))
            .map(to_book_entry)
            .collect())
    }
    async fn get_all_active_orders(&self) -> TraitResult<Vec<OrderBookEntry>> {
        Ok(self
            .orders
            .lock()
            .unwrap()
            .iter()
            .filter(|o| is_active(o.status))
            .map(to_book_entry)
            .collect())
    }
    async fn update_order_status(&self, _id: Uuid, _status: OrderStatus) -> TraitResult<()> { Ok(()) }
    async fn update_order_pda(&self, _id: Uuid, _p: &str, _i: i64) -> TraitResult<()> { Ok(()) }
    async fn update_filled_amount(&self, _id: Uuid, _filled_amount: Decimal, _status: OrderStatus) -> TraitResult<()> { Ok(()) }
    async fn update_filled_amount_with_event(&self, _id: Uuid, _filled_amount: Decimal, _status: OrderStatus, _event: &Event) -> TraitResult<()> { Ok(()) }
    async fn get_active_buy_orders(&self) -> TraitResult<Vec<TradingOrder>> {
        Ok(self.orders.lock().unwrap().iter().filter(|o| is_active(o.status) && o.side == OrderSide::Buy).cloned().collect())
    }
    async fn get_active_sell_orders(&self) -> TraitResult<Vec<TradingOrder>> {
        Ok(self.orders.lock().unwrap().iter().filter(|o| is_active(o.status) && o.side == OrderSide::Sell).cloned().collect())
    }
    async fn cancel_order(&self, _id: Uuid, _user_id: Uuid) -> TraitResult<bool> { Ok(true) }
    async fn bootstrap_active_orders(&self) -> TraitResult<Vec<TradingOrder>> { Ok(vec![]) }
}

#[async_trait]
impl SettlementRepository for MockSystem {
    async fn insert_settlement(&self, settlement: &Settlement) -> TraitResult<()> {
        self.settlements.lock().unwrap().push(settlement.clone());
        Ok(())
    }
    async fn insert_settlement_with_event(&self, settlement: &Settlement, _event: &Event) -> TraitResult<()> {
        self.settlements.lock().unwrap().push(settlement.clone());
        Ok(())
    }
    async fn get_settlement(&self, id: Uuid) -> TraitResult<Option<Settlement>> {
        Ok(self.settlements.lock().unwrap().iter().find(|s| s.id == id).cloned())
    }
    async fn get_or_create_active_epoch(&self) -> TraitResult<Uuid> { Ok(Uuid::nil()) }
    async fn insert_match(&self, _m: &trading_core::models::OrderMatch, _settlement_id: Option<Uuid>, _zone_id: Option<i32>) -> TraitResult<()> { Ok(()) }
    async fn insert_match_with_event(&self, _m: &trading_core::models::OrderMatch, _settlement_id: Option<Uuid>, _zone_id: Option<i32>, _event: &Event) -> TraitResult<()> { Ok(()) }
    async fn persist_matched_trade(&self, _settlement: &trading_core::models::Settlement, _order_match: &trading_core::models::OrderMatch, _matched_event: &Event, _match_zone_id: Option<i32>, _buyer: &TradeFill, _seller: &TradeFill) -> TraitResult<bool> { Ok(true) }
    async fn get_pending_settlements(&self, _limit: i64) -> TraitResult<Vec<Settlement>> { Ok(vec![]) }
    async fn claim_settlements_for_processing(&self, _ids: &[Uuid]) -> TraitResult<Vec<Settlement>> { Ok(vec![]) }
    async fn reset_settlements_for_retry(&self, _ids: &[Uuid], _max_retries: i32, _error: Option<&str>) -> TraitResult<u64> { Ok(0) }
    async fn reclaim_stale_processing(&self, _stale_after_secs: i64, _max_retries: i32) -> TraitResult<u64> { Ok(0) }
    async fn list_settlements_for_user(&self, user_id: Uuid, limit: i64, offset: i64) -> TraitResult<(Vec<Settlement>, i64)> {
        let s = self.settlements.lock().unwrap();
        let mut matched: Vec<Settlement> = s
            .iter()
            .filter(|x| x.buyer_id == user_id || x.seller_id == user_id)
            .cloned()
            .collect();
        matched.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        let total = matched.len() as i64;
        let page = matched
            .into_iter()
            .skip(offset as usize)
            .take(limit as usize)
            .collect();
        Ok((page, total))
    }
    async fn get_settlement_stats(&self) -> TraitResult<trading_core::models::SettlementStats> {
        let s = self.settlements.lock().unwrap();
        let count = |st: SettlementStatus| s.iter().filter(|x| x.status == st).count() as i64;
        let total_settled_value = s
            .iter()
            .filter(|x| x.status == SettlementStatus::Completed)
            .map(|x| x.total_amount)
            .sum();
        Ok(trading_core::models::SettlementStats {
            pending_count: count(SettlementStatus::Pending),
            processing_count: count(SettlementStatus::Processing),
            confirmed_count: count(SettlementStatus::Completed),
            failed_count: count(SettlementStatus::Failed),
            total_settled_value,
        })
    }
    async fn get_market_price(&self, window_hours: i64) -> TraitResult<trading_core::models::MarketPrice> {
        use rust_decimal::Decimal;
        let s = self.settlements.lock().unwrap();
        let done: Vec<_> = s.iter().filter(|x| x.status == SettlementStatus::Completed).collect();
        let volume: Decimal = done.iter().map(|x| x.energy_amount).sum();
        let notional: Decimal = done.iter().map(|x| x.price * x.energy_amount).sum();
        let vwap = if volume.is_zero() { Decimal::ZERO } else { notional / volume };
        let high = done.iter().map(|x| x.price).max().unwrap_or(Decimal::ZERO);
        let low = done.iter().map(|x| x.price).min().unwrap_or(Decimal::ZERO);
        let last_price = done.last().map(|x| x.price).unwrap_or(Decimal::ZERO);
        Ok(trading_core::models::MarketPrice {
            vwap,
            last_price,
            high,
            low,
            volume_kwh: volume,
            trade_count: done.len() as i64,
            window_hours,
            as_of: gridtokenx_telemetry::time::now(),
        })
    }
    async fn count_active_traders(&self, _window_hours: i64) -> TraitResult<i64> {
        use std::collections::HashSet;
        let s = self.settlements.lock().unwrap();
        let mut ids: HashSet<Uuid> = HashSet::new();
        for x in s.iter().filter(|x| x.status == SettlementStatus::Completed) {
            ids.insert(x.buyer_id);
            ids.insert(x.seller_id);
        }
        Ok(ids.len() as i64)
    }
    async fn update_settlement_status(&self, _id: Uuid, _status: &str, _tx_hash: Option<&str>, _error: Option<&str>) -> TraitResult<()> { Ok(()) }
    async fn update_settlement_status_with_event(&self, _id: Uuid, _status: &str, _tx_hash: Option<&str>, _error: Option<&str>, _event: &Event) -> TraitResult<()> { Ok(()) }
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
    async fn execute_batched_settlements(&self, _s: Vec<Settlement>) -> TraitResult<Vec<SettlementTransaction>> { Ok(vec![]) }
    async fn issue_erc(&self, _u: Uuid, _m: &str, _a: Decimal) -> TraitResult<String> { Ok("erc_sig".to_string()) }
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

#[async_trait]
impl PriceAlertRepository for MockSystem {
    async fn create_price_alert(&self, input: NewPriceAlert) -> TraitResult<PriceAlert> {
        let alert = PriceAlert {
            id: Uuid::new_v4(),
            user_id: input.user_id,
            target_price: input.target_price,
            condition: input.condition,
            status: AlertStatus::Active,
            triggered_at: None,
            triggered_price: None,
            repeat: false,
            note: input.note,
            created_at: chrono::Utc::now(),
            updated_at: Some(chrono::Utc::now()),
        };
        self.price_alerts.lock().unwrap().push(alert.clone());
        Ok(alert)
    }
    async fn list_price_alerts_for_user(&self, user_id: Uuid) -> TraitResult<Vec<PriceAlert>> {
        let mut rows: Vec<PriceAlert> = self
            .price_alerts
            .lock()
            .unwrap()
            .iter()
            .filter(|a| a.user_id == user_id)
            .cloned()
            .collect();
        rows.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(rows)
    }
    async fn delete_price_alert(&self, id: Uuid, user_id: Uuid) -> TraitResult<bool> {
        let mut alerts = self.price_alerts.lock().unwrap();
        let before = alerts.len();
        alerts.retain(|a| !(a.id == id && a.user_id == user_id));
        Ok(alerts.len() < before)
    }
    async fn get_active_alerts(&self) -> TraitResult<Vec<PriceAlert>> {
        Ok(self
            .price_alerts
            .lock()
            .unwrap()
            .iter()
            .filter(|a| a.status == AlertStatus::Active)
            .cloned()
            .collect())
    }
    async fn mark_triggered(
        &self,
        id: Uuid,
        triggered_price: rust_decimal::Decimal,
    ) -> TraitResult<()> {
        let mut alerts = self.price_alerts.lock().unwrap();
        if let Some(a) = alerts.iter_mut().find(|a| a.id == id) {
            a.triggered_price = Some(triggered_price);
            a.triggered_at = Some(chrono::Utc::now());
            if !a.repeat {
                a.status = AlertStatus::Triggered;
            }
        }
        Ok(())
    }
    async fn mark_triggered_with_event(
        &self,
        id: Uuid,
        triggered_price: rust_decimal::Decimal,
        _event: &Event,
    ) -> TraitResult<()> {
        self.mark_triggered(id, triggered_price).await
    }
}

#[async_trait]
impl RecurringOrderRepository for MockSystem {
    async fn get_due_recurring_orders(
        &self,
        _now: chrono::DateTime<chrono::Utc>,
    ) -> TraitResult<Vec<RecurringOrder>> {
        Ok(vec![])
    }
    async fn update_after_execution(
        &self,
        _id: Uuid,
        _next_execution: chrono::DateTime<chrono::Utc>,
        _total_executions: i32,
    ) -> TraitResult<()> {
        Ok(())
    }
    async fn create_recurring_order(&self, input: NewRecurringOrder) -> TraitResult<RecurringOrder> {
        let order = RecurringOrder {
            id: Uuid::new_v4(),
            user_id: input.user_id,
            side: input.side,
            energy_amount: input.energy_amount,
            max_price_per_kwh: input.max_price_per_kwh,
            min_price_per_kwh: input.min_price_per_kwh,
            interval_type: input.interval_type,
            interval_value: input.interval_value,
            next_execution_at: input.next_execution_at,
            last_executed_at: None,
            status: RecurringStatus::Active,
            total_executions: 0,
            max_executions: input.max_executions,
            name: input.name,
            description: input.description,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        self.recurring.lock().unwrap().push(order.clone());
        Ok(order)
    }
    async fn list_recurring_orders_for_user(
        &self,
        user_id: Uuid,
    ) -> TraitResult<Vec<RecurringOrder>> {
        let mut rows: Vec<RecurringOrder> = self
            .recurring
            .lock()
            .unwrap()
            .iter()
            .filter(|o| o.user_id == user_id)
            .cloned()
            .collect();
        rows.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(rows)
    }
    async fn get_recurring_order(
        &self,
        id: Uuid,
        user_id: Uuid,
    ) -> TraitResult<Option<RecurringOrder>> {
        Ok(self
            .recurring
            .lock()
            .unwrap()
            .iter()
            .find(|o| o.id == id && o.user_id == user_id)
            .cloned())
    }
    async fn delete_recurring_order(&self, id: Uuid, user_id: Uuid) -> TraitResult<bool> {
        let mut rows = self.recurring.lock().unwrap();
        let before = rows.len();
        rows.retain(|o| !(o.id == id && o.user_id == user_id));
        Ok(rows.len() < before)
    }
    async fn set_recurring_status(
        &self,
        id: Uuid,
        user_id: Uuid,
        status: RecurringStatus,
    ) -> TraitResult<bool> {
        let mut rows = self.recurring.lock().unwrap();
        if let Some(o) = rows.iter_mut().find(|o| o.id == id && o.user_id == user_id) {
            o.status = status;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

struct TopologyStub;
impl trading_engine::engine::TopologySnapshot for TopologyStub {
    fn can_accommodate_flow(&self, _f: Option<i32>, _t: Option<i32>, _a: Decimal) -> bool { true }
    fn calculate_wheeling_charge(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice { FastPrice::from_raw(1000) }
    fn calculate_loss_factor(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice { FastPrice::from_raw(1000000) }
}

// ── Mock helpers / factories ──────────────────────────────────────────────────

fn is_active(s: OrderStatus) -> bool {
    matches!(
        s,
        OrderStatus::Pending | OrderStatus::Active | OrderStatus::PartiallyFilled
    )
}

fn to_book_entry(o: &TradingOrder) -> OrderBookEntry {
    OrderBookEntry {
        order_id: o.id,
        user_id: o.user_id,
        side: o.side,
        energy_amount: o.energy_amount - o.filled_amount,
        original_amount: o.energy_amount,
        price_per_kwh: o.price_per_kwh,
        created_at: o.created_at.unwrap_or_else(chrono::Utc::now),
        zone_id: o.zone_id,
        session_token: o.session_token.clone(),
        signature: None,
        payload_bytes: None,
        time_in_force: o.time_in_force,
    }
}

fn mk_order(side: OrderSide, price: i64, amount: i64) -> TradingOrder {
    TradingOrder {
        id: Uuid::new_v4(),
        user_id: Uuid::new_v4(),
        order_type: OrderType::Limit,
        side,
        energy_amount: Decimal::new(amount, 0),
        price_per_kwh: Decimal::new(price, 2),
        filled_amount: Decimal::ZERO,
        status: OrderStatus::Pending,
        expires_at: None,
        created_at: Some(chrono::Utc::now()),
        filled_at: None,
        epoch_id: None,
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
        market_segment: trading_core::types::MarketSegment::Realtime,
    }
}

fn mk_settlement(status: SettlementStatus, total: i64) -> Settlement {
    Settlement {
        id: Uuid::new_v4(),
        trade_id: None,
        epoch_id: Uuid::nil(),
        buyer_id: Uuid::new_v4(),
        seller_id: Uuid::new_v4(),
        buy_order_id: Uuid::new_v4(),
        sell_order_id: Uuid::new_v4(),
        energy_amount: Decimal::new(total, 0),
        price: Decimal::ONE,
        total_amount: Decimal::new(total, 0),
        fee_amount: Decimal::ZERO,
        net_amount: Decimal::new(total, 0),
        status,
        blockchain_tx: None,
        created_at: chrono::Utc::now(),
        confirmed_at: None,
        wheeling_charge: None,
        loss_factor: None,
        loss_cost: None,
        effective_energy: None,
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

async fn get_json(app: axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let res = request(app, "GET", uri, Uuid::new_v4(), Body::empty()).await;
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, json)
}

// ── Test Setup ──────────────────────────────────────────────────────────────

fn setup_test_state(oracle_pub_key: String) -> AppState {
    setup_test_state_with_mock(oracle_pub_key).0
}

fn setup_test_state_with_mock(oracle_pub_key: String) -> (AppState, Arc<MockSystem>) {
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
            "treasury_program_id": "FfxSQYKUmx9NGdCC9TDPmZSYjWYE1h4ruu3JatzHN5Tn",
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
        "aggregator_bridge_public_key": oracle_pub_key,
        "trade_settlement_enabled": false
    });

    let config: trading_core::config::Config = serde_json::from_value(config_json).unwrap();
    let config_arc = Arc::new(config);

    let matcher = Arc::new(trading_logic::MatcherService::new(
        mock.clone(),
        mock.clone(),
        Arc::new(TopologyStub),
    ));

    let settlement = Arc::new(trading_logic::SettlementService::new(
        mock.clone(),
        mock.clone(),
        mock.clone(),
        Uuid::nil(),
    ));

    let vpp = Arc::new(trading_logic::vpp::VppService::new(
        mock.clone(),
        mock.clone(),
        mock.clone(),
    ));

    let state = AppState {
        config: config_arc,
        order_repo: mock.clone(),
        settlement_repo: mock.clone(),
        futures_repo: mock.clone(),
        carbon_repo: mock.clone(),
        analytics_repo: mock.clone(),
        price_alert_repo: mock.clone(),
        recurring_repo: mock.clone(),
        events: mock.clone(),
        blockchain: mock.clone(),
        identity: mock.clone(),
        audit: mock.clone(),
        matcher,
        settlement,
        vpp,
    };
    (state, mock)
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
    let body = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let created_id = created["id"].as_str().expect("submit response has id");

    // 2. Get Order by ID (use the order created in step 1, not a random id)
    let res = request(app.clone(), "GET", &format!("/api/v1/orders/{}", created_id), user_id, Body::empty()).await;
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

    // 20. Health
    let res = app.clone().oneshot(Request::builder().method("GET").uri("/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

// ── Phase 1 + 2 Markets endpoints (HTTP integration) ──────────────────────────

fn test_oracle_key() -> String {
    let signing_key = SigningKey::from_bytes(&[0u8; 32]);
    bs58::encode(signing_key.verifying_key().to_bytes()).into_string()
}

#[tokio::test]
async fn test_markets_config_endpoint() {
    let app = build_router(setup_test_state(test_oracle_key()));
    let (status, body) = get_json(app, "/api/v1/markets/config").await;
    assert_eq!(status, StatusCode::OK);
    // all 6 contract fields present
    for k in [
        "base_price_thb_kwh",
        "grid_import_price_thb_kwh",
        "grid_export_price_thb_kwh",
        "transaction_fee_bps",
        "min_price_per_kwh",
        "max_price_per_kwh",
    ] {
        assert!(body.get(k).is_some(), "missing field {k}");
    }
    // defaults
    assert_eq!(body["base_price_thb_kwh"], 4.5);
    assert_eq!(body["transaction_fee_bps"], 50);
    assert_eq!(body["max_price_per_kwh"], 20.0);
}

#[tokio::test]
async fn test_p2p_market_prices_endpoint() {
    let app = build_router(setup_test_state(test_oracle_key()));
    let (status, body) = get_json(app, "/api/v1/markets/p2p/market-prices").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["loss_allocation_model"], "proportional");
    assert_eq!(body["wheeling_charges"]["intra_zone"], 0.0);
    assert_eq!(body["wheeling_charges"]["cross_zone"], 0.02);
    assert_eq!(body["loss_factors"]["intra_zone"], 1.01);
    assert_eq!(body["loss_factors"]["cross_zone"], 1.03);
}

#[tokio::test]
async fn test_matching_status_empty_endpoint() {
    let app = build_router(setup_test_state(test_oracle_key()));
    let (status, body) = get_json(app, "/api/v1/markets/matching-status").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["pending_buy_orders"], 0);
    assert_eq!(body["pending_sell_orders"], 0);
    assert_eq!(body["can_match"], false);
    assert_eq!(body["match_reason"], "no orders");
}

#[tokio::test]
async fn test_matching_status_crossing_endpoint() {
    let (state, mock) = setup_test_state_with_mock(test_oracle_key());
    {
        let mut o = mock.orders.lock().unwrap();
        o.push(mk_order(OrderSide::Buy, 500, 10)); // buy max 5.00
        o.push(mk_order(OrderSide::Sell, 400, 8)); // sell min 4.00 → crosses
    }
    let app = build_router(state);
    let (status, body) = get_json(app, "/api/v1/markets/matching-status").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["pending_buy_orders"], 1);
    assert_eq!(body["pending_sell_orders"], 1);
    assert_eq!(body["can_match"], true);
    assert_eq!(body["pending_matches"], 1);
    assert_eq!(body["match_reason"], "orders crossing");
}

#[tokio::test]
async fn test_settlement_stats_empty_endpoint() {
    let app = build_router(setup_test_state(test_oracle_key()));
    let (status, body) = get_json(app, "/api/v1/markets/settlement-stats").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["pending_count"], 0);
    assert_eq!(body["processing_count"], 0);
    assert_eq!(body["confirmed_count"], 0);
    assert_eq!(body["failed_count"], 0);
    assert_eq!(body["total_settled_value"], 0.0);
}

#[tokio::test]
async fn test_settlement_stats_endpoint() {
    let (state, mock) = setup_test_state_with_mock(test_oracle_key());
    {
        let mut s = mock.settlements.lock().unwrap();
        s.push(mk_settlement(SettlementStatus::Pending, 5));
        s.push(mk_settlement(SettlementStatus::Pending, 5));
        s.push(mk_settlement(SettlementStatus::Processing, 7));
        s.push(mk_settlement(SettlementStatus::Completed, 10));
        s.push(mk_settlement(SettlementStatus::Completed, 20));
        s.push(mk_settlement(SettlementStatus::Completed, 30));
        s.push(mk_settlement(SettlementStatus::Failed, 9));
    }
    let app = build_router(state);
    let (status, body) = get_json(app, "/api/v1/markets/settlement-stats").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["pending_count"], 2);
    assert_eq!(body["processing_count"], 1);
    assert_eq!(body["confirmed_count"], 3); // Completed → confirmed
    assert_eq!(body["failed_count"], 1);
    assert_eq!(body["total_settled_value"], 60.0); // sum of completed only
}

#[tokio::test]
async fn test_orderbook_empty_endpoint() {
    let app = build_router(setup_test_state(test_oracle_key()));
    let (status, body) = get_json(app, "/api/v1/markets/orderbook").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["asks"].as_array().unwrap().len(), 0);
    assert_eq!(body["bids"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_orderbook_endpoint() {
    let (state, mock) = setup_test_state_with_mock(test_oracle_key());
    {
        let mut o = mock.orders.lock().unwrap();
        o.push(mk_order(OrderSide::Buy, 450, 10));
        o.push(mk_order(OrderSide::Buy, 500, 5));
        o.push(mk_order(OrderSide::Buy, 500, 15)); // same level → aggregates with above
        o.push(mk_order(OrderSide::Sell, 600, 8));
        o.push(mk_order(OrderSide::Sell, 550, 3));
    }
    let app = build_router(state);
    let (status, body) = get_json(app, "/api/v1/markets/orderbook").await;
    assert_eq!(status, StatusCode::OK);
    let bids = body["bids"].as_array().unwrap();
    let asks = body["asks"].as_array().unwrap();
    // bids descending: 5.00 (aggregated 20) then 4.50 (10)
    assert_eq!(bids[0][0], "5.00");
    assert_eq!(bids[0][1], "20");
    assert_eq!(bids[1][0], "4.50");
    // asks ascending: 5.50 then 6.00
    assert_eq!(asks[0][0], "5.50");
    assert_eq!(asks[1][0], "6.00");
}

// ── Trades (Phase 3) ──────────────────────────────────────────────────────────

async fn get_json_as(
    app: axum::Router,
    uri: &str,
    user_id: Uuid,
) -> (StatusCode, serde_json::Value) {
    let res = request(app, "GET", uri, user_id, Body::empty()).await;
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, json)
}

fn mk_settlement_between(
    buyer: Uuid,
    seller: Uuid,
    status: SettlementStatus,
    total: i64,
) -> Settlement {
    let mut s = mk_settlement(status, total);
    s.buyer_id = buyer;
    s.seller_id = seller;
    s
}

#[tokio::test]
async fn test_trades_empty_endpoint() {
    let app = build_router(setup_test_state(test_oracle_key()));
    let (status, body) = get_json(app, "/api/v1/trades").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["trades"].as_array().unwrap().len(), 0);
    assert_eq!(body["total"], 0);
    assert_eq!(body["total_count"], 0);
}

#[tokio::test]
async fn test_trades_user_scoped_and_role() {
    let (state, mock) = setup_test_state_with_mock(test_oracle_key());
    let me = Uuid::new_v4();
    let other = Uuid::new_v4();
    {
        let mut s = mock.settlements.lock().unwrap();
        s.push(mk_settlement_between(me, other, SettlementStatus::Completed, 10)); // me=buyer
        s.push(mk_settlement_between(other, me, SettlementStatus::Completed, 20)); // me=seller
        s.push(mk_settlement_between(other, other, SettlementStatus::Completed, 30)); // not mine
    }
    let app = build_router(state);
    let (status, body) = get_json_as(app, "/api/v1/trades", me).await;
    assert_eq!(status, StatusCode::OK);
    let trades = body["trades"].as_array().unwrap();
    assert_eq!(trades.len(), 2); // only mine
    assert_eq!(body["total"], 2);
    assert_eq!(body["total_count"], 2);
    // every row carries a role relative to me + counterparty = other
    for t in trades {
        let role = t["role"].as_str().unwrap();
        assert!(role == "buyer" || role == "seller");
        assert_eq!(t["counterparty_id"].as_str().unwrap(), other.to_string());
    }
}

#[tokio::test]
async fn test_trades_pagination() {
    let (state, mock) = setup_test_state_with_mock(test_oracle_key());
    let me = Uuid::new_v4();
    {
        let mut s = mock.settlements.lock().unwrap();
        for i in 0..5 {
            s.push(mk_settlement_between(me, Uuid::new_v4(), SettlementStatus::Completed, i));
        }
    }
    let app = build_router(state);
    let (status, body) = get_json_as(app, "/api/v1/trades?limit=2&offset=1", me).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["trades"].as_array().unwrap().len(), 2);
    assert_eq!(body["total"], 5); // total ignores paging
    assert_eq!(body["total_count"], 5);
}

#[tokio::test]
async fn test_trades_export_csv() {
    let (state, mock) = setup_test_state_with_mock(test_oracle_key());
    let me = Uuid::new_v4();
    {
        let mut s = mock.settlements.lock().unwrap();
        s.push(mk_settlement_between(me, Uuid::new_v4(), SettlementStatus::Completed, 10));
        s.push(mk_settlement_between(me, Uuid::new_v4(), SettlementStatus::Completed, 20));
    }
    let app = build_router(state);
    let res = request(app, "GET", "/api/v1/trades/export", me, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::OK);
    let ct = res.headers().get("content-type").unwrap().to_str().unwrap().to_string();
    let cd = res.headers().get("content-disposition").unwrap().to_str().unwrap().to_string();
    assert!(ct.starts_with("text/csv"));
    assert!(cd.contains("trades.csv"));
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let csv = String::from_utf8(bytes.to_vec()).unwrap();
    let lines: Vec<&str> = csv.lines().collect();
    assert!(lines[0].starts_with("id,executed_at,role,"));
    assert_eq!(lines.len(), 3); // header + 2 rows
}

#[tokio::test]
async fn test_trades_export_json_format() {
    let (state, mock) = setup_test_state_with_mock(test_oracle_key());
    let me = Uuid::new_v4();
    {
        let mut s = mock.settlements.lock().unwrap();
        s.push(mk_settlement_between(me, Uuid::new_v4(), SettlementStatus::Completed, 10));
    }
    let app = build_router(state);
    let (status, body) = get_json_as(app, "/api/v1/trades/export?format=json", me).await;
    assert_eq!(status, StatusCode::OK);
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["role"], "buyer");
}

// ── Price alerts (Phase 4) ──────────────────────────────────────────────────

async fn post_json_as(
    app: axum::Router,
    uri: &str,
    user_id: Uuid,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let res = request(app, "POST", uri, user_id, Body::from(body.to_string())).await;
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, json)
}

#[tokio::test]
async fn test_price_alert_create_maps_symbol_and_active() {
    let app = build_router(setup_test_state(test_oracle_key()));
    let me = Uuid::new_v4();
    let (status, body) = post_json_as(
        app,
        "/api/v1/price-alerts",
        me,
        json!({ "symbol": "GRID", "target_price": "12.50", "condition": "above" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["symbol"], "GRID"); // symbol round-trips via note
    assert_eq!(body["target_price"], "12.50"); // decimal preserved as sent
    assert_eq!(body["condition"], "above");
    assert_eq!(body["is_active"], true);
    assert_eq!(body["user_id"].as_str().unwrap(), me.to_string());
}

#[tokio::test]
async fn test_price_alert_create_then_list_newest_first() {
    let (state, _mock) = setup_test_state_with_mock(test_oracle_key());
    let app = build_router(state);
    let me = Uuid::new_v4();
    for (sym, cond) in [("A", "above"), ("B", "below")] {
        let (s, _) = post_json_as(
            app.clone(),
            "/api/v1/price-alerts",
            me,
            json!({ "symbol": sym, "target_price": "1.0", "condition": cond }),
        )
        .await;
        assert_eq!(s, StatusCode::OK);
    }
    let (status, body) = get_json_as(app, "/api/v1/price-alerts", me).await;
    assert_eq!(status, StatusCode::OK);
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["symbol"], "B"); // newest first
    assert_eq!(arr[1]["symbol"], "A");
}

#[tokio::test]
async fn test_price_alert_list_user_scoped() {
    let (state, _mock) = setup_test_state_with_mock(test_oracle_key());
    let app = build_router(state);
    let me = Uuid::new_v4();
    let other = Uuid::new_v4();
    let (s, _) = post_json_as(app.clone(), "/api/v1/price-alerts", other,
        json!({ "symbol": "X", "target_price": "1.0", "condition": "above" })).await;
    assert_eq!(s, StatusCode::OK);
    let (status, body) = get_json_as(app, "/api/v1/price-alerts", me).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_array().unwrap().len(), 0); // other's alert not visible
}

#[tokio::test]
async fn test_price_alert_delete_roundtrip() {
    let (state, _mock) = setup_test_state_with_mock(test_oracle_key());
    let app = build_router(state);
    let me = Uuid::new_v4();
    let (_, created) = post_json_as(app.clone(), "/api/v1/price-alerts", me,
        json!({ "symbol": "GRID", "target_price": "2.0", "condition": "below" })).await;
    let id = created["id"].as_str().unwrap().to_string();

    let res = request(app.clone(), "DELETE", &format!("/api/v1/price-alerts/{id}"), me, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::OK);

    let (status, body) = get_json_as(app, "/api/v1/price-alerts", me).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_array().unwrap().len(), 0); // gone
}

#[tokio::test]
async fn test_price_alert_delete_foreign_404() {
    let app = build_router(setup_test_state(test_oracle_key()));
    let me = Uuid::new_v4();
    let res = request(app, "DELETE", &format!("/api/v1/price-alerts/{}", Uuid::new_v4()), me, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_price_alert_bad_condition_400() {
    let app = build_router(setup_test_state(test_oracle_key()));
    let me = Uuid::new_v4();
    // 400 body is plain text, not JSON — use raw request and assert status only.
    let res = request(app, "POST", "/api/v1/price-alerts", me,
        Body::from(json!({ "symbol": "GRID", "target_price": "1.0", "condition": "sideways" }).to_string())).await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

// ── Recurring orders (Phase 5) ──────────────────────────────────────────────

#[tokio::test]
async fn test_recurring_create_maps_fields() {
    let app = build_router(setup_test_state(test_oracle_key()));
    let me = Uuid::new_v4();
    let (status, body) = post_json_as(
        app,
        "/api/v1/orders/recurring",
        me,
        json!({
            "side": "buy",
            "energy_amount": "10.50",
            "max_price_per_kwh": "0.20",
            "interval_type": "daily",
            "interval_value": 2,
            "max_executions": 5,
            "name": "dca"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["side"], "buy");
    assert_eq!(body["energy_amount"], "10.50"); // decimal preserved as string
    assert_eq!(body["max_price_per_kwh"], "0.20");
    assert_eq!(body["interval_type"], "daily");
    assert_eq!(body["interval_value"], 2);
    assert_eq!(body["status"], "active"); // new orders start active
    assert_eq!(body["user_id"].as_str().unwrap(), me.to_string());
    // next_execution_at is set (non-null)
    assert!(body["next_execution_at"].is_string());
}

#[tokio::test]
async fn test_recurring_create_then_list_newest_first() {
    let (state, _mock) = setup_test_state_with_mock(test_oracle_key());
    let app = build_router(state);
    let me = Uuid::new_v4();
    for name in ["first", "second"] {
        let (s, _) = post_json_as(
            app.clone(),
            "/api/v1/orders/recurring",
            me,
            json!({ "side": "sell", "energy_amount": "1.0", "interval_type": "hourly", "name": name }),
        )
        .await;
        assert_eq!(s, StatusCode::OK);
    }
    let (status, body) = get_json_as(app, "/api/v1/orders/recurring", me).await;
    assert_eq!(status, StatusCode::OK);
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["name"], "second"); // newest first
    assert_eq!(arr[1]["name"], "first");
}

#[tokio::test]
async fn test_recurring_list_user_scoped() {
    let (state, _mock) = setup_test_state_with_mock(test_oracle_key());
    let app = build_router(state);
    let me = Uuid::new_v4();
    let other = Uuid::new_v4();
    let (s, _) = post_json_as(app.clone(), "/api/v1/orders/recurring", other,
        json!({ "side": "buy", "energy_amount": "1.0", "interval_type": "daily" })).await;
    assert_eq!(s, StatusCode::OK);
    let (status, body) = get_json_as(app, "/api/v1/orders/recurring", me).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_array().unwrap().len(), 0); // other's order not visible
}

#[tokio::test]
async fn test_recurring_get_roundtrip_and_foreign_404() {
    let (state, _mock) = setup_test_state_with_mock(test_oracle_key());
    let app = build_router(state);
    let me = Uuid::new_v4();
    let (_, created) = post_json_as(app.clone(), "/api/v1/orders/recurring", me,
        json!({ "side": "buy", "energy_amount": "3.0", "interval_type": "weekly" })).await;
    let id = created["id"].as_str().unwrap().to_string();

    let (status, body) = get_json_as(app.clone(), &format!("/api/v1/orders/recurring/{id}"), me).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["id"].as_str().unwrap(), id);

    // foreign user cannot read it
    let other = Uuid::new_v4();
    let res = request(app, "GET", &format!("/api/v1/orders/recurring/{id}"), other, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_recurring_pause_resume_flips_status() {
    let (state, _mock) = setup_test_state_with_mock(test_oracle_key());
    let app = build_router(state);
    let me = Uuid::new_v4();
    let (_, created) = post_json_as(app.clone(), "/api/v1/orders/recurring", me,
        json!({ "side": "buy", "energy_amount": "1.0", "interval_type": "daily" })).await;
    let id = created["id"].as_str().unwrap().to_string();

    let res = request(app.clone(), "POST", &format!("/api/v1/orders/recurring/{id}/pause"), me, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::OK);
    let (_, body) = get_json_as(app.clone(), &format!("/api/v1/orders/recurring/{id}"), me).await;
    assert_eq!(body["status"], "paused");

    let res = request(app.clone(), "POST", &format!("/api/v1/orders/recurring/{id}/resume"), me, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::OK);
    let (_, body) = get_json_as(app, &format!("/api/v1/orders/recurring/{id}"), me).await;
    assert_eq!(body["status"], "active");
}

#[tokio::test]
async fn test_recurring_delete_roundtrip() {
    let (state, _mock) = setup_test_state_with_mock(test_oracle_key());
    let app = build_router(state);
    let me = Uuid::new_v4();
    let (_, created) = post_json_as(app.clone(), "/api/v1/orders/recurring", me,
        json!({ "side": "buy", "energy_amount": "1.0", "interval_type": "daily" })).await;
    let id = created["id"].as_str().unwrap().to_string();

    let res = request(app.clone(), "DELETE", &format!("/api/v1/orders/recurring/{id}"), me, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::OK);

    let (status, body) = get_json_as(app, "/api/v1/orders/recurring", me).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_array().unwrap().len(), 0); // gone
}

#[tokio::test]
async fn test_recurring_pause_foreign_404() {
    let app = build_router(setup_test_state(test_oracle_key()));
    let me = Uuid::new_v4();
    let res = request(app, "POST", &format!("/api/v1/orders/recurring/{}/pause", Uuid::new_v4()), me, Body::empty()).await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_recurring_bad_interval_400() {
    let app = build_router(setup_test_state(test_oracle_key()));
    let me = Uuid::new_v4();
    // 400 body is plain text — raw request, status-only assert.
    let res = request(app, "POST", "/api/v1/orders/recurring", me,
        Body::from(json!({ "side": "buy", "energy_amount": "1.0", "interval_type": "yearly" }).to_string())).await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

// ── Full route coverage ───────────────────────────────────────────────────────
//
// Exercises EVERY route wired in `build_router` (startup.rs) against the mock
// AppState. The contract here is reachability + dispatch, NOT business output:
// each route must NOT return 404 (route absent / bad path param) or 405 (wrong
// method) — that would mean the wiring drifted from the handler set. Routes that
// the dedicated tests above already assert on output are re-checked here only
// for "is it still wired". The five endpoints with no dedicated test
// (futures/candles, futures/book, analytics/history, health/ready, /metrics)
// get an explicit `OK` assertion so they have real coverage.

/// Every (method, path, body, lookup) tuple in `build_router`. `lookup` = true
/// when the path targets a specific resource id that won't exist in the mock,
/// so a 404 is a legitimate NotFound (not a missing route) and is excluded from
/// the no-404 assertion. Keep in lockstep with `trading_api::startup::build_router`.
fn all_routes(uid: Uuid) -> Vec<(&'static str, String, Body, bool)> {
    let new_id = Uuid::new_v4();
    vec![
        // Spot / core orders
        ("POST", "/api/v1/orders".into(), Body::from(json!({
            "side": "buy", "order_type": "limit",
            "energy_amount_kwh": "1.0", "price_per_kwh": "4.5", "zone_id": 1
        }).to_string()), false),
        ("GET", "/api/v1/orders".into(), Body::empty(), false),
        ("GET", format!("/api/v1/orders/{new_id}"), Body::empty(), true),
        ("DELETE", format!("/api/v1/orders/{new_id}"), Body::empty(), true),
        ("POST", "/api/v1/quotes".into(), Body::from(json!({
            "buyer_zone_id": 1, "seller_zone_id": 2,
            "energy_amount_kwh": "1.0", "agreed_price": "4.5"
        }).to_string()), false),
        ("GET", "/api/v1/zones/1/book".into(), Body::empty(), false),
        ("GET", "/api/v1/stats".into(), Body::empty(), false),
        // Markets (read-only)
        ("GET", "/api/v1/markets/config".into(), Body::empty(), false),
        ("GET", "/api/v1/markets/p2p/market-prices".into(), Body::empty(), false),
        ("GET", "/api/v1/markets/matching-status".into(), Body::empty(), false),
        ("GET", "/api/v1/markets/settlement-stats".into(), Body::empty(), false),
        ("GET", "/api/v1/markets/orderbook".into(), Body::empty(), false),
        // Trades
        ("GET", "/api/v1/trades".into(), Body::empty(), false),
        ("GET", "/api/v1/trades/export".into(), Body::empty(), false),
        // Price alerts
        ("POST", "/api/v1/price-alerts".into(), Body::from(json!({
            "symbol": "GRID/GRX", "condition": "above", "target_price": "5.0"
        }).to_string()), false),
        ("GET", "/api/v1/price-alerts".into(), Body::empty(), false),
        ("DELETE", format!("/api/v1/price-alerts/{new_id}"), Body::empty(), true),
        // Recurring orders
        ("POST", "/api/v1/orders/recurring".into(), Body::from(json!({
            "side": "buy", "energy_amount": "1.0", "interval_type": "daily"
        }).to_string()), false),
        ("GET", "/api/v1/orders/recurring".into(), Body::empty(), false),
        ("GET", format!("/api/v1/orders/recurring/{new_id}"), Body::empty(), true),
        ("DELETE", format!("/api/v1/orders/recurring/{new_id}"), Body::empty(), true),
        ("POST", format!("/api/v1/orders/recurring/{new_id}/pause"), Body::empty(), true),
        ("POST", format!("/api/v1/orders/recurring/{new_id}/resume"), Body::empty(), true),
        // Futures
        ("GET", "/api/v1/futures/products".into(), Body::empty(), false),
        ("GET", "/api/v1/futures/candles".into(), Body::empty(), false),
        ("GET", "/api/v1/futures/book".into(), Body::empty(), false),
        ("POST", "/api/v1/futures/orders".into(), Body::from(json!({
            "product_id": new_id.to_string(), "side": "buy", "order_type": "market",
            "quantity": 1.0, "price": 100.0, "leverage": 1
        }).to_string()), false),
        ("GET", "/api/v1/futures/orders".into(), Body::empty(), false),
        ("GET", "/api/v1/futures/positions".into(), Body::empty(), false),
        ("DELETE", format!("/api/v1/futures/positions/{new_id}"), Body::empty(), true),
        // User data & analytics
        ("GET", "/api/v1/wallets/addr123/balance".into(), Body::empty(), false),
        ("GET", "/api/v1/analytics/stats".into(), Body::empty(), false),
        ("GET", "/api/v1/analytics/history".into(), Body::empty(), false),
        ("GET", "/api/v1/transactions".into(), Body::empty(), false),
        // Carbon / ESG
        ("GET", "/api/v1/carbon/balance".into(), Body::empty(), false),
        ("GET", "/api/v1/carbon/history".into(), Body::empty(), false),
        ("GET", "/api/v1/carbon/transactions".into(), Body::empty(), false),
        ("POST", "/api/v1/carbon/transfers".into(), Body::from(json!({
            "to_user_id": uid, "amount": "1.0"
        }).to_string()), false),
        // Ops
        ("GET", "/health".into(), Body::empty(), false),
        ("GET", "/health/ready".into(), Body::empty(), false),
        ("GET", "/metrics".into(), Body::empty(), false),
    ]
}

#[tokio::test]
async fn test_every_route_reachable() {
    let app = build_router(setup_test_state(test_oracle_key()));
    let me = Uuid::new_v4();

    for (method, uri, body, lookup) in all_routes(me) {
        let res = request(app.clone(), method, &uri, me, body).await;
        let status = res.status();
        // 405 always means the wiring drifted — wrong method bound for the path.
        assert_ne!(
            status, StatusCode::METHOD_NOT_ALLOWED,
            "{method} {uri} returned 405 — wrong method wired for this path?"
        );
        // 404 means an absent route — UNLESS the path looks up a resource id
        // that legitimately doesn't exist in the mock (handler NotFound).
        if !lookup {
            assert_ne!(
                status, StatusCode::NOT_FOUND,
                "{method} {uri} returned 404 — route not wired in build_router?"
            );
        }
    }
}

#[tokio::test]
async fn test_untested_endpoints_return_ok() {
    // The five routes with no dedicated test above — assert real 200s, not just
    // "reachable", so they carry actual coverage.
    let app = build_router(setup_test_state(test_oracle_key()));
    let me = Uuid::new_v4();

    for uri in [
        "/api/v1/futures/candles",
        "/api/v1/futures/book",
        "/api/v1/analytics/history",
        "/health/ready",
        "/metrics",
    ] {
        let res = request(app.clone(), "GET", uri, me, Body::empty()).await;
        assert_eq!(res.status(), StatusCode::OK, "GET {uri} expected 200");
    }
}

// ── ConnectRPC gRPC handler coverage ──────────────────────────────────────────
//
// Exercises the real-logic `TradingService` methods (submit/get/list/cancel,
// dispatch_vpp, batch settle empty-path) directly against `TradingGrpcService`
// over the mock `AppState`. Request views are built with `OwnedView::from_owned`
// (encode→decode roundtrip from a programmatically-constructed owned message).
// The many handler methods that just return `Default::default()` are NOT covered
// here — they carry no behavior to assert.

fn grpc_service() -> TradingGrpcService {
    TradingGrpcService::new(setup_test_state(test_oracle_key()))
}

fn ctx() -> Context {
    Context::new(axum::http::HeaderMap::new())
}

/// Build a zero-copy request view from an owned protobuf message.
fn owned_view<V>(owned: &V::Owned) -> OwnedView<V>
where
    V: buffa::view::MessageView<'static>,
{
    OwnedView::from_owned(owned).expect("owned message -> view roundtrip")
}

#[tokio::test]
async fn test_grpc_submit_get_list_cancel_roundtrip() {
    let svc = grpc_service();
    let uid = Uuid::new_v4();

    // submit_order — inserts into the mock order store, returns id.
    let submit = SubmitOrderRequest {
        user_id: uid.to_string(),
        side: "buy".into(),
        order_type: "limit".into(),
        energy_amount: 100.0,
        price_per_kwh: 4.5,
        zone_id: Some(1),
        ..Default::default()
    };
    let (res, _) = svc.submit_order(ctx(), owned_view(&submit)).await.expect("submit ok");
    assert!(res.success);
    let order_id = res.id.expect("submit returns id");

    // get_order — the order just inserted round-trips back.
    let get = GetOrderRequest { order_id: order_id.clone(), ..Default::default() };
    let (got, _) = svc.get_order(ctx(), owned_view(&get)).await.expect("get ok");
    assert_eq!(got.id, order_id);
    assert_eq!(got.user_id, uid.to_string());
    assert_eq!(got.side, "buy");

    // list_orders — scoped to the user; exactly the one we created.
    let list = ListOrdersRequest { user_id: uid.to_string(), ..Default::default() };
    let (listed, _) = svc.list_orders(ctx(), owned_view(&list)).await.expect("list ok");
    assert_eq!(listed.orders.len(), 1);
    assert_eq!(listed.orders[0].id, order_id);

    // cancel_order — mock cancel always succeeds.
    let cancel = CancelOrderRequest {
        order_id: order_id.clone(),
        user_id: uid.to_string(),
        ..Default::default()
    };
    let (cancelled, _) = svc.cancel_order(ctx(), owned_view(&cancel)).await.expect("cancel ok");
    assert!(cancelled.success);
}

#[tokio::test]
async fn test_grpc_submit_order_invalid_side_rejected() {
    let svc = grpc_service();
    let submit = SubmitOrderRequest {
        user_id: Uuid::new_v4().to_string(),
        side: "hold".into(), // not buy/sell
        order_type: "limit".into(),
        energy_amount: 1.0,
        price_per_kwh: 1.0,
        zone_id: Some(1),
        ..Default::default()
    };
    assert!(svc.submit_order(ctx(), owned_view(&submit)).await.is_err());
}

#[tokio::test]
async fn test_grpc_submit_order_invalid_user_rejected() {
    let svc = grpc_service();
    let submit = SubmitOrderRequest {
        user_id: "not-a-uuid".into(),
        side: "buy".into(),
        order_type: "limit".into(),
        energy_amount: 1.0,
        price_per_kwh: 1.0,
        zone_id: Some(1),
        ..Default::default()
    };
    assert!(svc.submit_order(ctx(), owned_view(&submit)).await.is_err());
}

#[tokio::test]
async fn test_grpc_get_order_not_found() {
    let svc = grpc_service();
    let get = GetOrderRequest { order_id: Uuid::new_v4().to_string(), ..Default::default() };
    assert!(svc.get_order(ctx(), owned_view(&get)).await.is_err());
}

#[tokio::test]
async fn test_grpc_dispatch_vpp_succeeds() {
    let svc = grpc_service();
    // Mock cluster exists with 0 members -> optimize returns empty -> success.
    let req = DispatchVppRequest {
        cluster_id: "vpp-1".into(),
        dispatch_mode: "discharge".into(),
        target_kw: 10.0,
        ..Default::default()
    };
    let (res, _) = svc.dispatch_vpp(ctx(), owned_view(&req)).await.expect("dispatch ok");
    assert!(res.success);
}

#[tokio::test]
async fn test_grpc_batch_execute_settlements_empty() {
    let svc = grpc_service();
    let req = BatchExecuteSettlementsRequest::default(); // no settlements
    let (res, _) = svc
        .batch_execute_settlements(ctx(), owned_view(&req))
        .await
        .expect("batch ok");
    // Empty input short-circuits to the default response: success=false, no ids.
    assert!(!res.success);
    assert!(res.settlement_ids.is_empty());
}

/// #1 wiring: the REST submit endpoint must parse `time_in_force` and
/// `market_segment` from the request and persist them — previously both were
/// hardcoded Gtc/Realtime, leaving IOC and the interval (uniform-auction)
/// segment unreachable from the API.
#[tokio::test]
async fn test_submit_order_parses_tif_and_segment() {
    let (state, mock) = setup_test_state_with_mock(test_oracle_key());
    let app = build_router(state);
    let user_id = Uuid::new_v4();

    // Realtime + IOC flows through (interval+ioc is rejected — see guard below).
    let res = request(
        app.clone(),
        "POST",
        "/api/v1/orders",
        user_id,
        Body::from(
            json!({
                "side": "sell", "order_type": "limit",
                "energy_amount_kwh": "10", "price_per_kwh": "4.0", "zone_id": 1,
                "time_in_force": "ioc", "market_segment": "realtime"
            })
            .to_string(),
        ),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    {
        let orders = mock.orders.lock().unwrap();
        let o = orders.last().expect("order captured");
        assert_eq!(o.time_in_force, TimeInForce::Ioc);
        assert_eq!(o.market_segment, trading_core::types::MarketSegment::Realtime);
    }

    // Interval + GTC is the valid interval combination.
    let res = request(
        app.clone(),
        "POST",
        "/api/v1/orders",
        user_id,
        Body::from(
            json!({
                "side": "sell", "order_type": "limit",
                "energy_amount_kwh": "10", "price_per_kwh": "4.0", "zone_id": 1,
                "market_segment": "interval"
            })
            .to_string(),
        ),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    {
        let orders = mock.orders.lock().unwrap();
        let o = orders.last().expect("order captured");
        assert_eq!(o.time_in_force, TimeInForce::Gtc);
        assert_eq!(o.market_segment, trading_core::types::MarketSegment::Interval);
    }

    // Guard: interval + ioc is rejected — IOC has no meaning in batch clearing,
    // and the CDA IOC sweep never sees interval orders.
    let res = request(
        app.clone(),
        "POST",
        "/api/v1/orders",
        user_id,
        Body::from(
            json!({
                "side": "sell", "order_type": "limit",
                "energy_amount_kwh": "10", "price_per_kwh": "4.0", "zone_id": 1,
                "time_in_force": "ioc", "market_segment": "interval"
            })
            .to_string(),
        ),
    )
    .await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    // Omitted → defaults Gtc / Realtime.
    let res = request(
        app.clone(),
        "POST",
        "/api/v1/orders",
        user_id,
        Body::from(
            json!({
                "side": "buy", "order_type": "limit",
                "energy_amount_kwh": "10", "price_per_kwh": "4.0", "zone_id": 1
            })
            .to_string(),
        ),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    {
        let orders = mock.orders.lock().unwrap();
        let o = orders.last().unwrap();
        assert_eq!(o.time_in_force, TimeInForce::Gtc);
        assert_eq!(o.market_segment, trading_core::types::MarketSegment::Realtime);
    }

    // A market order with no explicit TIF auto-maps to IOC (not GTC).
    let res = request(
        app.clone(),
        "POST",
        "/api/v1/orders",
        user_id,
        Body::from(
            json!({
                "side": "buy", "order_type": "market",
                "energy_amount_kwh": "10", "price_per_kwh": "4.0", "zone_id": 1
            })
            .to_string(),
        ),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    {
        let orders = mock.orders.lock().unwrap();
        let o = orders.last().unwrap();
        assert_eq!(o.time_in_force, TimeInForce::Ioc, "market defaults to IOC");
    }

    // Unknown value → 400, not a silent default.
    let res = request(
        app,
        "POST",
        "/api/v1/orders",
        user_id,
        Body::from(
            json!({
                "side": "buy", "order_type": "limit",
                "energy_amount_kwh": "10", "price_per_kwh": "4.0", "zone_id": 1,
                "time_in_force": "bogus"
            })
            .to_string(),
        ),
    )
    .await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

/// Phase 5: the clearing-results endpoint is wired and returns a JSON array
/// (empty here — the mock has no cleared epochs). Guards routing, auth, and the
/// response shape.
#[tokio::test]
async fn test_clearing_epochs_endpoint() {
    let app = build_router(setup_test_state(test_oracle_key()));
    let (status, body) = get_json(app, "/api/v1/markets/clearing-epochs?limit=5").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_array(), "clearing-epochs returns a JSON array");
}

/// True market orders: a market BUY needs no price (crosses at the resting ask,
/// gets the ceiling bid + IOC); a market SELL is rejected; market+GTC rejected;
/// a limit order still requires a price.
#[tokio::test]
async fn test_market_order_semantics() {
    let (state, mock) = setup_test_state_with_mock(test_oracle_key());
    let app = build_router(state);
    let user_id = Uuid::new_v4();

    let post = |app: axum::Router, body: serde_json::Value| async move {
        request(app, "POST", "/api/v1/orders", user_id, Body::from(body.to_string())).await
    };

    // Market BUY, no price → OK; stored at the ceiling bid, IOC.
    let res = post(app.clone(), json!({
        "side": "buy", "order_type": "market",
        "energy_amount_kwh": "10", "zone_id": 1
    })).await;
    assert_eq!(res.status(), StatusCode::OK, "market buy needs no price");
    {
        let orders = mock.orders.lock().unwrap();
        let o = orders.last().unwrap();
        assert_eq!(o.price_per_kwh, rust_decimal::Decimal::new(1_000_000, 0), "ceiling bid");
        assert_eq!(o.time_in_force, TimeInForce::Ioc);
        assert_eq!(o.order_type, OrderType::Market);
    }

    // Market SELL → 400 (unsupported by the maker/sell-priced matcher).
    let res = post(app.clone(), json!({
        "side": "sell", "order_type": "market",
        "energy_amount_kwh": "10", "zone_id": 1
    })).await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST, "market sell rejected");

    // Market + explicit GTC → 400 (market must be immediate).
    let res = post(app.clone(), json!({
        "side": "buy", "order_type": "market", "time_in_force": "gtc",
        "energy_amount_kwh": "10", "zone_id": 1
    })).await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST, "market gtc rejected");

    // Limit without a price → 400.
    let res = post(app, json!({
        "side": "buy", "order_type": "limit",
        "energy_amount_kwh": "10", "zone_id": 1
    })).await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST, "limit needs a price");
}

// ── Quote computation ────────────────────────────────────────────────────────

async fn post_quote_json(app: axum::Router, body: serde_json::Value) -> (StatusCode, serde_json::Value) {
    let res = request(app, "POST", "/api/v1/quotes", Uuid::new_v4(), Body::from(body.to_string())).await;
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    // 400s return a bare string body, not JSON — tolerate that.
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

#[tokio::test]
async fn create_quote_computes_cross_zone_breakdown_from_config() {
    let app = build_router(setup_test_state(test_oracle_key()));
    // 100 kWh, Zone 1 -> Zone 2 (cross-zone), agreed ฿4.50.
    // Default MarketConfig: cross wheeling 0.02/kWh, cross loss factor 1.03.
    //   energy_cost = 100 * 4.50           = 450.00
    //   wheeling    = 100 * 0.02           =   2.00
    //   loss_cost   = 450.00 * 0.03        =  13.50
    //   total       = 450 + 2 + 13.50      = 465.50
    //   effective   = 100 * (1 - 0.03)     =  97.0000
    //   distance    = |1 - 2| * 10         =  10.0
    let (status, body) = post_quote_json(app, json!({
        "buyer_zone_id": 1,
        "seller_zone_id": 2,
        "energy_amount_kwh": "100",
        "agreed_price": "4.50"
    })).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["breakdown"]["energy_cost"], "450.00");
    assert_eq!(body["breakdown"]["wheeling_charge"], "2.00");
    assert_eq!(body["breakdown"]["loss_cost"], "13.50");
    assert_eq!(body["breakdown"]["total_cost"], "465.50");
    assert_eq!(body["grid_metrics"]["effective_energy_kwh"], "97.0000");
    assert_eq!(body["grid_metrics"]["loss_factor"], "0.0300");
    assert_eq!(body["grid_metrics"]["zone_distance_km"], "10.0");
    assert_eq!(body["grid_metrics"]["is_grid_compliant"], true);
}

#[tokio::test]
async fn create_quote_same_zone_has_no_wheeling_and_low_loss() {
    let app = build_router(setup_test_state(test_oracle_key()));
    // Same zone: intra wheeling 0.00, intra loss factor 1.01 (0.01 fraction).
    //   energy_cost = 10 * 5.00 = 50.00 ; wheeling = 0 ; loss = 50 * 0.01 = 0.50
    let (status, body) = post_quote_json(app, json!({
        "buyer_zone_id": 3,
        "seller_zone_id": 3,
        "energy_amount_kwh": "10",
        "agreed_price": "5.00"
    })).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["breakdown"]["energy_cost"], "50.00");
    assert_eq!(body["breakdown"]["wheeling_charge"], "0.00");
    assert_eq!(body["breakdown"]["loss_cost"], "0.50");
    assert_eq!(body["breakdown"]["total_cost"], "50.50");
    assert_eq!(body["grid_metrics"]["zone_distance_km"], "0.0");
}

#[tokio::test]
async fn create_quote_defaults_price_to_base_when_zero() {
    let app = build_router(setup_test_state(test_oracle_key()));
    // agreed_price "0.00" -> falls back to base_price 4.50 -> energy_cost 10*4.50=45.00
    let (status, body) = post_quote_json(app, json!({
        "buyer_zone_id": 1,
        "seller_zone_id": 1,
        "energy_amount_kwh": "10",
        "agreed_price": "0.00"
    })).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["breakdown"]["energy_cost"], "45.00");
}

#[tokio::test]
async fn create_quote_rejects_non_positive_energy() {
    let app = build_router(setup_test_state(test_oracle_key()));
    let (status, _) = post_quote_json(app.clone(), json!({
        "buyer_zone_id": 1, "seller_zone_id": 2,
        "energy_amount_kwh": "0", "agreed_price": "4.50"
    })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "zero energy rejected");

    let (status, _) = post_quote_json(app, json!({
        "buyer_zone_id": 1, "seller_zone_id": 2,
        "energy_amount_kwh": "abc", "agreed_price": "4.50"
    })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "non-numeric energy rejected");
}
