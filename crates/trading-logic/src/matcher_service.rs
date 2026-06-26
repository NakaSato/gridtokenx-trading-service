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
        // Expiry reaping is owned by the dedicated `ReaperWorker` (runs in every
        // role, not just where the matcher runs). The engine still skips any
        // expired order it's handed (`is_expired`), so a not-yet-reaped expired
        // order can never match here.

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

        // Run the engine whenever either side has orders — not just when both do.
        // An IOC buy with no sells (or vice-versa) still has to be swept and
        // cancelled, which only happens once the engine reports it.
        if buy_orders.is_empty() && sell_orders.is_empty() {
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

        // 4. Batch-persist fills + settlements (shared with the clearing engine).
        if !matches.is_empty() {
            info!(
                "Matching cycle completed: {} matches, total volume: {}",
                matches.len(),
                stats.total_volume
            );
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
        }

        // 5. Cancel IOC remainders LAST, so Cancelled is the terminal status even
        // for a partially-filled IOC (persist_matches above wrote PartiallyFilled
        // for the part that crossed; this supersedes it). An IOC order fills only
        // what crossed this cycle — its leftover never rests in the book. The
        // filled amount is preserved (read from the engine-mutated FastOrder) and
        // the OrderUpdate event is emitted atomically through the outbox.
        self.cancel_ioc_remainders(&stats, &fast_buys, &fast_sells)
            .await?;

        Ok(matches.len())
    }

    /// Mark every IOC order the engine flagged as `Cancelled`, preserving the
    /// amount it filled this cycle. Each write goes through
    /// `update_filled_amount_with_event` so the cancellation and its
    /// `OrderUpdate` event land atomically (outbox). Filled amount and zone are
    /// read from the engine-mutated `FastOrder`s, so a partially-filled IOC keeps
    /// its fill and only the remainder is dropped.
    async fn cancel_ioc_remainders(
        &self,
        stats: &trading_engine::types::CycleStats,
        fast_buys: &[trading_engine::types::FastOrder],
        fast_sells: &[trading_engine::types::FastOrder],
    ) -> TraitResult<()> {
        if stats.ioc_cancellations.is_empty() {
            return Ok(());
        }
        let cancel: std::collections::HashSet<_> =
            stats.ioc_cancellations.iter().copied().collect();
        for fo in fast_buys.iter().chain(fast_sells.iter()) {
            if !cancel.contains(&fo.id) {
                continue;
            }
            let event = trading_core::events::Event::OrderUpdate {
                id: fo.id,
                filled_amount: fo.filled_amount,
                status: trading_core::types::OrderStatus::Cancelled.to_string(),
                zone_id: fo.zone_id,
            };
            self.order_repo
                .update_filled_amount_with_event(
                    fo.id,
                    fo.filled_amount,
                    trading_core::types::OrderStatus::Cancelled,
                    &event,
                )
                .await?;
        }
        Ok(())
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
        /// Ids the reaper returns (preset per test).
        expire_returns: Vec<Uuid>,
        /// How many times the matcher invoked the reaper.
        expire_calls: Mutex<usize>,
    }

    #[async_trait]
    impl OrderRepository for MockOrderRepo {
        async fn expire_stale_orders(&self, _now: DateTime<Utc>) -> TraitResult<Vec<Uuid>> {
            *self.expire_calls.lock().unwrap() += 1;
            Ok(self.expire_returns.clone())
        }
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
            ..Default::default()
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
            ..Default::default()
        });
        let settlements = Arc::new(MockSettlementRepo::default());
        let matcher = MatcherService::new(orders.clone(), settlements.clone(), Arc::new(PermissiveTopology));

        let matched = matcher.run_matching_cycle().await.unwrap();

        assert_eq!(matched, 0);
        assert!(settlements.settlements.lock().unwrap().is_empty());
        assert!(orders.fills.lock().unwrap().is_empty());
    }

    /// Reaping is the ReaperWorker's job, NOT the matcher's: a matching cycle
    /// must not call expire_stale_orders (it would reap in only the matcher role).
    #[tokio::test]
    async fn run_matching_cycle_does_not_reap() {
        let buy = order(OrderSide::Buy, dec!(5.0), dec!(10.0), 10);
        let sell = order(OrderSide::Sell, dec!(4.0), dec!(10.0), 20);
        let orders = Arc::new(MockOrderRepo {
            buys: vec![buy],
            sells: vec![sell],
            ..Default::default()
        });
        let settlements = Arc::new(MockSettlementRepo::default());
        let matcher =
            MatcherService::new(orders.clone(), settlements.clone(), Arc::new(PermissiveTopology));

        let matched = matcher.run_matching_cycle().await.unwrap();

        assert_eq!(
            *orders.expire_calls.lock().unwrap(),
            0,
            "matcher must not reap; that's the ReaperWorker's job"
        );
        assert_eq!(matched, 1, "matching still works");
    }

    /// An unfilled IOC order with no counterparty is cancelled, not left Active:
    /// the engine reports it and the matcher writes a Cancelled status (filled 0).
    /// Guards that the empty-sells path still runs the engine + sweep.
    #[tokio::test]
    async fn run_matching_cycle_cancels_unfilled_ioc() {
        let mut buy = order(OrderSide::Buy, dec!(5.0), dec!(10.0), 10);
        buy.time_in_force = TimeInForce::Ioc;
        let buy_id = buy.id;

        let orders = Arc::new(MockOrderRepo {
            buys: vec![buy],
            sells: vec![],
            fills: Mutex::new(Vec::new()),
            ..Default::default()
        });
        let settlements = Arc::new(MockSettlementRepo::default());
        let matcher =
            MatcherService::new(orders.clone(), settlements.clone(), Arc::new(PermissiveTopology));

        let matched = matcher.run_matching_cycle().await.unwrap();

        assert_eq!(matched, 0, "no liquidity → no match");
        let fills = orders.fills.lock().unwrap();
        assert_eq!(
            *fills,
            vec![(buy_id, dec!(0.0), OrderStatus::Cancelled)],
            "unfilled IOC must be cancelled (filled 0), not rested"
        );
    }

    /// A partially-filled IOC: it fills what crosses, then its remainder is
    /// cancelled. The final write for the buy is Cancelled, preserving the 40 kWh
    /// it actually filled.
    #[tokio::test]
    async fn run_matching_cycle_cancels_ioc_remainder_after_partial_fill() {
        let mut buy = order(OrderSide::Buy, dec!(5.0), dec!(100.0), 10);
        buy.time_in_force = TimeInForce::Ioc;
        let buy_id = buy.id;
        let sell = order(OrderSide::Sell, dec!(4.0), dec!(40.0), 20);

        let orders = Arc::new(MockOrderRepo {
            buys: vec![buy],
            sells: vec![sell],
            fills: Mutex::new(Vec::new()),
            ..Default::default()
        });
        let settlements = Arc::new(MockSettlementRepo::default());
        let matcher =
            MatcherService::new(orders.clone(), settlements.clone(), Arc::new(PermissiveTopology));

        let matched = matcher.run_matching_cycle().await.unwrap();

        assert_eq!(matched, 1, "IOC fills the 40 that crossed");
        let fills = orders.fills.lock().unwrap();
        // Last write for the buy is the cancellation, keeping its 40 kWh fill.
        assert_eq!(
            *fills.last().unwrap(),
            (buy_id, dec!(40.0), OrderStatus::Cancelled),
            "IOC remainder cancelled, filled amount preserved"
        );
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
            ..Default::default()
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
            ..Default::default()
        });
        let settlements = Arc::new(MockSettlementRepo::default());
        let matcher =
            MatcherService::new(orders.clone(), settlements.clone(), Arc::new(PermissiveTopology));

        let matched = matcher.run_matching_cycle().await.unwrap();

        assert_eq!(matched, 0, "segments must not cross-match");
        assert!(orders.fills.lock().unwrap().is_empty());
    }
}
