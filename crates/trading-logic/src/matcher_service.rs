use std::sync::Arc;
use tracing::info;
use trading_core::fast_price::FastPrice;
use trading_core::traits::{OrderRepository, SettlementRepository, TraitResult};
use trading_engine::engine::{MatchingEngine, TopologySnapshot};

/// Async orchestrator for the matching engine
pub struct MatcherService {
    order_repo: Arc<dyn OrderRepository>,
    settlement_repo: Arc<dyn SettlementRepository>,
    topology: Arc<dyn TopologySnapshot>,
}

impl MatcherService {
    pub fn new(
        order_repo: Arc<dyn OrderRepository>,
        settlement_repo: Arc<dyn SettlementRepository>,
        topology: Arc<dyn TopologySnapshot>,
    ) -> Self {
        Self {
            order_repo,
            settlement_repo,
            topology,
        }
    }

    /// Run a matching cycle for all active orders
    pub async fn run_matching_cycle(&self) -> TraitResult<usize> {
        // 1. Fetch active orders, then keep only Realtime-segment orders. Interval
        // orders are settled by the uniform-price clearing path, not the CDA matcher;
        // routing them here would let the two mechanisms double-fill the same order.
        let buy_orders: Vec<_> = self
            .order_repo
            .get_active_buy_orders()
            .await?
            .into_iter()
            .filter(|o| o.market_segment == trading_core::types::MarketSegment::Realtime)
            .collect();
        let sell_orders: Vec<_> = self
            .order_repo
            .get_active_sell_orders()
            .await?
            .into_iter()
            .filter(|o| o.market_segment == trading_core::types::MarketSegment::Realtime)
            .collect();

        if buy_orders.is_empty() || sell_orders.is_empty() {
            return Ok(0);
        }

        // 2. Convert to FastOrder + metadata sidecar (shared helper).
        // No caller-side pre-sort: MatchingEngine::match_cycle segments sells into
        // a price-time-keyed BTreeMap and re-sorts buys internally, so output is
        // independent of slice order.
        let (mut fast_buys, buy_metadata) = crate::clearing_support::to_fast_orders(&buy_orders);
        let (mut fast_sells, sell_metadata) = crate::clearing_support::to_fast_orders(&sell_orders);

        // 3. Execute pure matching logic
        let now_ns = gridtokenx_telemetry::time::now().timestamp_nanos_opt().unwrap_or(0);
        let (matches, stats) = MatchingEngine::match_cycle(
            &mut fast_buys,
            &mut fast_sells,
            &buy_metadata,
            &sell_metadata,
            self.topology.as_ref(),
            FastPrice::from(rust_decimal_macros::dec!(1.0)), // Default multiplier
            now_ns,
        );

        if matches.is_empty() {
            return Ok(0);
        }

        info!(
            "Matching cycle completed: {} matches, total volume: {}",
            matches.len(),
            stats.total_volume
        );

        // 4. Batch-persist fills + settlements (shared with the clearing engine).
        let totals = crate::clearing_support::order_totals(&buy_orders, &sell_orders);
        crate::clearing_support::persist_matches(
            self.order_repo.as_ref(),
            self.settlement_repo.as_ref(),
            &matches,
            &buy_metadata,
            &sell_metadata,
            &totals,
        )
        .await?;

        Ok(matches.len())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // unwrap is idiomatic in tests
    use super::*;
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::sync::Mutex;
    use uuid::Uuid;
    use trading_core::events::Event;
    use trading_core::models::{
        OrderBookEntry, OrderMatch, Settlement, SettlementStats, TradingOrder,
    };
    use trading_core::types::{MarketSegment, OrderSide, OrderStatus, OrderType, TimeInForce};

    /// Permissive topology: every flow accommodated, zero wheeling/loss — so the
    /// test exercises the match→persist orchestration, not grid routing.
    struct PermissiveTopology;
    impl TopologySnapshot for PermissiveTopology {
        fn can_accommodate_flow(&self, _f: Option<i32>, _t: Option<i32>, _a: Decimal) -> bool {
            true
        }
        fn calculate_wheeling_charge(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice {
            FastPrice::from(dec!(0))
        }
        fn calculate_loss_factor(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice {
            FastPrice::from(dec!(0))
        }
    }

    fn order(side: OrderSide, price: Decimal, amount: Decimal, secs_ago: i64) -> TradingOrder {
        TradingOrder {
            id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            order_type: OrderType::Limit,
            side,
            energy_amount: amount,
            price_per_kwh: price,
            filled_amount: Decimal::ZERO,
            status: OrderStatus::Active,
            expires_at: None,
            created_at: Some(Utc::now() - chrono::Duration::seconds(secs_ago)),
            filled_at: None,
            epoch_id: Some(Uuid::new_v4()),
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
            market_segment: MarketSegment::Realtime,
        }
    }

    #[derive(Default)]
    struct MockOrderRepo {
        buys: Vec<TradingOrder>,
        sells: Vec<TradingOrder>,
        fills: Mutex<Vec<(Uuid, Decimal, OrderStatus)>>,
    }

    #[async_trait]
    impl OrderRepository for MockOrderRepo {
        async fn get_active_buy_orders(&self) -> TraitResult<Vec<TradingOrder>> {
            Ok(self.buys.clone())
        }
        async fn get_active_sell_orders(&self) -> TraitResult<Vec<TradingOrder>> {
            Ok(self.sells.clone())
        }
        async fn update_filled_amount_with_event(&self, id: Uuid, filled: Decimal, status: OrderStatus, _e: &Event) -> TraitResult<()> {
            self.fills.lock().unwrap().push((id, filled, status));
            Ok(())
        }
        async fn insert_order(&self, _o: &TradingOrder) -> TraitResult<()> { unimplemented!() }
        async fn insert_order_with_event(&self, _o: &TradingOrder, _e: &Event) -> TraitResult<()> { unimplemented!() }
        async fn get_or_create_active_epoch(&self) -> TraitResult<Uuid> { unimplemented!() }
        async fn get_order(&self, _id: Uuid) -> TraitResult<Option<TradingOrder>> { unimplemented!() }
        async fn get_orders_by_user(&self, _u: Uuid, _l: i64, _o: i64) -> TraitResult<Vec<TradingOrder>> { unimplemented!() }
        async fn get_active_orders_by_zone(&self, _z: i32) -> TraitResult<Vec<OrderBookEntry>> { unimplemented!() }
        async fn get_all_active_orders(&self) -> TraitResult<Vec<OrderBookEntry>> { unimplemented!() }
        async fn update_order_status(&self, _id: Uuid, _s: OrderStatus) -> TraitResult<()> { unimplemented!() }
        async fn update_filled_amount(&self, _id: Uuid, _f: Decimal, _s: OrderStatus) -> TraitResult<()> { unimplemented!() }
        async fn cancel_order(&self, _id: Uuid, _u: Uuid) -> TraitResult<bool> { unimplemented!() }
        async fn bootstrap_active_orders(&self) -> TraitResult<Vec<TradingOrder>> { unimplemented!() }
    }

    #[derive(Default)]
    struct MockSettlementRepo {
        settlements: Mutex<Vec<Settlement>>,
        matches: Mutex<Vec<OrderMatch>>,
    }

    #[async_trait]
    impl SettlementRepository for MockSettlementRepo {
        async fn insert_settlement(&self, s: &Settlement) -> TraitResult<()> {
            self.settlements.lock().unwrap().push(s.clone());
            Ok(())
        }
        async fn insert_match_with_event(&self, m: &OrderMatch, _sid: Option<Uuid>, _z: Option<i32>, _e: &Event) -> TraitResult<()> {
            self.matches.lock().unwrap().push(m.clone());
            Ok(())
        }
        async fn insert_settlement_with_event(&self, _s: &Settlement, _e: &Event) -> TraitResult<()> { unimplemented!() }
        async fn get_or_create_active_epoch(&self) -> TraitResult<Uuid> { unimplemented!() }
        async fn insert_match(&self, _m: &OrderMatch, _sid: Option<Uuid>, _z: Option<i32>) -> TraitResult<()> { unimplemented!() }
        async fn get_settlement(&self, _id: Uuid) -> TraitResult<Option<Settlement>> { unimplemented!() }
        async fn get_pending_settlements(&self, _l: i64) -> TraitResult<Vec<Settlement>> { unimplemented!() }
        async fn claim_settlements_for_processing(&self, _ids: &[Uuid]) -> TraitResult<Vec<Settlement>> { unimplemented!() }
        async fn reset_settlements_for_retry(&self, _ids: &[Uuid], _m: i32, _e: Option<&str>) -> TraitResult<u64> { unimplemented!() }
        async fn reclaim_stale_processing(&self, _s: i64, _m: i32) -> TraitResult<u64> { unimplemented!() }
        async fn list_settlements_for_user(&self, _u: Uuid, _l: i64, _o: i64) -> TraitResult<(Vec<Settlement>, i64)> { unimplemented!() }
        async fn get_settlement_stats(&self) -> TraitResult<SettlementStats> { unimplemented!() }
        async fn update_settlement_status(&self, _id: Uuid, _s: &str, _t: Option<&str>, _e: Option<&str>) -> TraitResult<()> { unimplemented!() }
        async fn update_settlement_status_with_event(&self, _id: Uuid, _s: &str, _t: Option<&str>, _e: Option<&str>, _ev: &Event) -> TraitResult<()> { unimplemented!() }
    }

    /// A crossing buy (5.0) and sell (4.0) for the same 10 kWh fully match: one
    /// settlement + one match-ledger row persisted, and both orders marked Filled.
    #[tokio::test]
    async fn run_matching_cycle_matches_and_persists() {
        let buy = order(OrderSide::Buy, dec!(5.0), dec!(10.0), 10);
        let sell = order(OrderSide::Sell, dec!(4.0), dec!(10.0), 20);
        let (buy_id, sell_id) = (buy.id, sell.id);

        let orders = Arc::new(MockOrderRepo {
            buys: vec![buy],
            sells: vec![sell],
            fills: Mutex::new(Vec::new()),
        });
        let settlements = Arc::new(MockSettlementRepo::default());
        let matcher = MatcherService::new(orders.clone(), settlements.clone(), Arc::new(PermissiveTopology));

        let matched = matcher.run_matching_cycle().await.unwrap();

        assert_eq!(matched, 1, "one match");
        assert_eq!(settlements.settlements.lock().unwrap().len(), 1, "settlement persisted");
        assert_eq!(settlements.matches.lock().unwrap().len(), 1, "match-ledger row persisted");

        let s = &settlements.settlements.lock().unwrap()[0];
        assert_eq!(s.energy_amount, dec!(10.0));
        assert!(s.trade_id.is_some(), "trade settlement carries a trade_id");

        // Both orders fully filled (10/10) → Filled.
        let fills = orders.fills.lock().unwrap();
        assert_eq!(fills.len(), 2);
        assert!(fills.iter().any(|(id, amt, st)| *id == buy_id && *amt == dec!(10.0) && *st == OrderStatus::Filled));
        assert!(fills.iter().any(|(id, amt, st)| *id == sell_id && *amt == dec!(10.0) && *st == OrderStatus::Filled));
    }

    /// No sell orders → nothing to match → no persistence, returns 0.
    #[tokio::test]
    async fn run_matching_cycle_noop_without_sells() {
        let orders = Arc::new(MockOrderRepo {
            buys: vec![order(OrderSide::Buy, dec!(5.0), dec!(10.0), 10)],
            sells: vec![],
            fills: Mutex::new(Vec::new()),
        });
        let settlements = Arc::new(MockSettlementRepo::default());
        let matcher = MatcherService::new(orders.clone(), settlements.clone(), Arc::new(PermissiveTopology));

        let matched = matcher.run_matching_cycle().await.unwrap();

        assert_eq!(matched, 0);
        assert!(settlements.settlements.lock().unwrap().is_empty());
        assert!(orders.fills.lock().unwrap().is_empty());
    }

    /// Phase 1: the CDA matcher must ignore Interval-segment orders (they belong
    /// to the uniform-price clearing path). A crossing Interval buy/sell pair must
    /// NOT match here.
    #[tokio::test]
    async fn run_matching_cycle_ignores_interval_segment() {
        let mut buy = order(OrderSide::Buy, dec!(5.0), dec!(10.0), 10);
        let mut sell = order(OrderSide::Sell, dec!(4.0), dec!(10.0), 20);
        buy.market_segment = MarketSegment::Interval;
        sell.market_segment = MarketSegment::Interval;

        let orders = Arc::new(MockOrderRepo {
            buys: vec![buy],
            sells: vec![sell],
            fills: Mutex::new(Vec::new()),
        });
        let settlements = Arc::new(MockSettlementRepo::default());
        let matcher =
            MatcherService::new(orders.clone(), settlements.clone(), Arc::new(PermissiveTopology));

        let matched = matcher.run_matching_cycle().await.unwrap();

        assert_eq!(matched, 0, "interval orders are not matched by CDA");
        assert!(settlements.settlements.lock().unwrap().is_empty());
        assert!(orders.fills.lock().unwrap().is_empty());
    }

    /// A Realtime buy and an Interval sell at a crossing price must NOT match —
    /// the Interval sell is filtered out, leaving no counterparty.
    #[tokio::test]
    async fn run_matching_cycle_does_not_cross_segments() {
        let buy = order(OrderSide::Buy, dec!(5.0), dec!(10.0), 10); // Realtime (default)
        let mut sell = order(OrderSide::Sell, dec!(4.0), dec!(10.0), 20);
        sell.market_segment = MarketSegment::Interval;

        let orders = Arc::new(MockOrderRepo {
            buys: vec![buy],
            sells: vec![sell],
            fills: Mutex::new(Vec::new()),
        });
        let settlements = Arc::new(MockSettlementRepo::default());
        let matcher =
            MatcherService::new(orders.clone(), settlements.clone(), Arc::new(PermissiveTopology));

        let matched = matcher.run_matching_cycle().await.unwrap();

        assert_eq!(matched, 0, "segments must not cross-match");
        assert!(orders.fills.lock().unwrap().is_empty());
    }
}
