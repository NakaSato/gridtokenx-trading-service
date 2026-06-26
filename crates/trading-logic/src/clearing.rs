//! Uniform-price auction clearing orchestrator.
//!
//! The async counterpart to [`crate::matcher_service::MatcherService`] for the
//! second trading mechanism (proposal: `docs/design-docs/dual-mechanism-trading.md`).
//! Where the matcher runs continuously, this clears a whole epoch's worth of
//! `Interval`-segment orders at a single uniform price per zone via the pure
//! [`UniformAuction`] engine, then persists the resulting fills + settlements
//! through the same path the matcher uses.
//!
//! The `MarketEpoch` lifecycle (open / close / write back clearing price) is
//! owned by the clearing **worker** (Phase 4), not this service — keeping the
//! service a pure "clear one epoch, persist trades" unit.

use rust_decimal::Decimal;
use std::sync::Arc;
use tracing::info;
use trading_core::traits::{OrderRepository, SettlementRepository, TraitResult};
use trading_core::types::MarketSegment;
use trading_engine::engine::TopologySnapshot;
use trading_engine::types::MatchResult;
use trading_engine::uniform_auction::UniformAuction;
use uuid::Uuid;

/// Outcome of clearing one epoch: per-zone uniform prices plus aggregate volume.
#[derive(Debug, Clone)]
pub struct ClearingSummary {
    pub epoch_id: Uuid,
    pub zones_cleared: usize,
    pub matches: usize,
    pub total_volume: Decimal,
    /// `(zone_id, clearing_price)` for each zone that cleared.
    pub zone_prices: Vec<(Option<i32>, Decimal)>,
}

impl ClearingSummary {
    fn empty(epoch_id: Uuid) -> Self {
        Self {
            epoch_id,
            zones_cleared: 0,
            matches: 0,
            total_volume: Decimal::ZERO,
            zone_prices: Vec::new(),
        }
    }
}

/// Async orchestrator for the uniform-price auction engine.
pub struct ClearingService {
    order_repo: Arc<dyn OrderRepository>,
    settlement_repo: Arc<dyn SettlementRepository>,
    topology: Arc<dyn TopologySnapshot>,
}

impl ClearingService {
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

    /// Clear all `Interval`-segment orders belonging to `epoch_id` at a uniform
    /// price per zone, persist the trades, and return a summary.
    pub async fn run_epoch_clearing(&self, epoch_id: Uuid) -> TraitResult<ClearingSummary> {
        // 1. Fetch this epoch's Interval orders. (Reuses the active-order queries
        // and filters in-memory; a dedicated by-epoch+segment query is a later
        // optimisation.)
        let buy_orders: Vec<_> = self
            .order_repo
            .get_active_buy_orders()
            .await?
            .into_iter()
            .filter(|o| o.market_segment == MarketSegment::Interval && o.epoch_id == Some(epoch_id))
            .collect();
        let sell_orders: Vec<_> = self
            .order_repo
            .get_active_sell_orders()
            .await?
            .into_iter()
            .filter(|o| o.market_segment == MarketSegment::Interval && o.epoch_id == Some(epoch_id))
            .collect();

        if buy_orders.is_empty() || sell_orders.is_empty() {
            return Ok(ClearingSummary::empty(epoch_id));
        }

        // 2. Convert + clear (pure engine).
        let (mut fast_buys, buy_metadata) = crate::clearing_support::to_fast_orders(&buy_orders);
        let (mut fast_sells, sell_metadata) = crate::clearing_support::to_fast_orders(&sell_orders);
        let now_ns = gridtokenx_telemetry::time::now()
            .timestamp_nanos_opt()
            .unwrap_or(0);
        let (result, stats) = UniformAuction::clear_cycle(
            &mut fast_buys,
            &mut fast_sells,
            &buy_metadata,
            &sell_metadata,
            self.topology.as_ref(),
            now_ns,
        );

        // 3. Flatten per-zone matches and persist (shared with the matcher).
        let all_matches: Vec<MatchResult> = result
            .zones
            .iter()
            .flat_map(|z| z.matches.iter().cloned())
            .collect();
        if all_matches.is_empty() {
            return Ok(ClearingSummary::empty(epoch_id));
        }

        let totals = crate::clearing_support::order_totals(&buy_orders, &sell_orders);
        crate::clearing_support::persist_matches(
            self.order_repo.as_ref(),
            self.settlement_repo.as_ref(),
            &all_matches,
            &buy_metadata,
            &sell_metadata,
            &totals,
        )
        .await?;

        let zone_prices = result
            .zones
            .iter()
            .map(|z| (z.zone_id, z.clearing_price))
            .collect();
        info!(
            "Epoch clearing {epoch_id}: {} zone(s), {} matches, volume {}",
            result.zones.len(),
            stats.matches_created,
            stats.total_volume
        );
        Ok(ClearingSummary {
            epoch_id,
            zones_cleared: result.zones.len(),
            matches: stats.matches_created,
            total_volume: stats.total_volume,
            zone_prices,
        })
    }

    /// Clear every epoch whose window has elapsed, then close it. For each due
    /// epoch: run the uniform-price clearing, then mark it `cleared` (stamping the
    /// summary) — even when nothing matched, so the epoch doesn't linger `active`
    /// and get re-selected forever. The `mark_epoch_cleared` guard makes the close
    /// idempotent. Returns one summary per epoch processed.
    pub async fn clear_due_epochs(&self) -> TraitResult<Vec<ClearingSummary>> {
        let due = self.order_repo.get_epochs_due_for_clearing().await?;
        let mut summaries = Vec::with_capacity(due.len());
        for epoch_id in due {
            let summary = self.run_epoch_clearing(epoch_id).await?;

            // The epoch column holds a single price; only a single-zone clear has
            // an unambiguous one. Multi-zone (or empty) clears leave it NULL — the
            // per-zone prices live on the settlements/order_matches rows.
            let clearing_price = match summary.zone_prices.as_slice() {
                [(_, price)] => Some(*price),
                _ => None,
            };
            self.order_repo
                .mark_epoch_cleared(
                    epoch_id,
                    clearing_price,
                    summary.total_volume,
                    i64::try_from(summary.matches).unwrap_or(i64::MAX),
                )
                .await?;
            summaries.push(summary);
        }
        Ok(summaries)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // unwrap is idiomatic in tests
    use super::*;
    use async_trait::async_trait;
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use std::sync::Mutex;
    use trading_core::events::Event;
    use trading_core::models::{
        OrderBookEntry, OrderMatch, Settlement, SettlementStats, TradingOrder,
    };
    use trading_core::traits::{OrderRepository, SettlementRepository};
    use trading_core::types::{OrderSide, OrderStatus, OrderType, TimeInForce};
    use trading_engine::engine::TopologySnapshot;
    use trading_core::fast_price::FastPrice;

    struct NoFeeTopology;
    impl TopologySnapshot for NoFeeTopology {
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

    #[allow(clippy::too_many_arguments)]
    fn order(
        side: OrderSide,
        price: Decimal,
        amount: Decimal,
        zone: i32,
        epoch: Uuid,
        segment: MarketSegment,
    ) -> TradingOrder {
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
            created_at: Some(Utc::now()),
            filled_at: None,
            epoch_id: Some(epoch),
            zone_id: Some(zone),
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
            market_segment: segment,
        }
    }

    #[derive(Default)]
    struct MockOrderRepo {
        buys: Vec<TradingOrder>,
        sells: Vec<TradingOrder>,
        fills: Mutex<Vec<(Uuid, Decimal, OrderStatus)>>,
        /// Epoch ids the worker should treat as due (preset per test).
        due_epochs: Vec<Uuid>,
        /// Captured `mark_epoch_cleared` calls: (epoch, price, volume, matched).
        cleared: Mutex<Vec<(Uuid, Option<Decimal>, Decimal, i64)>>,
    }

    #[async_trait]
    impl OrderRepository for MockOrderRepo {
        async fn get_epochs_due_for_clearing(&self) -> TraitResult<Vec<Uuid>> {
            Ok(self.due_epochs.clone())
        }
        async fn mark_epoch_cleared(
            &self,
            epoch_id: Uuid,
            clearing_price: Option<Decimal>,
            total_volume: Decimal,
            matched_orders: i64,
        ) -> TraitResult<()> {
            self.cleared
                .lock()
                .unwrap()
                .push((epoch_id, clearing_price, total_volume, matched_orders));
            Ok(())
        }
        async fn get_active_buy_orders(&self) -> TraitResult<Vec<TradingOrder>> {
            Ok(self.buys.clone())
        }
        async fn get_active_sell_orders(&self) -> TraitResult<Vec<TradingOrder>> {
            Ok(self.sells.clone())
        }
        async fn update_filled_amount_with_event(
            &self,
            id: Uuid,
            filled: Decimal,
            status: OrderStatus,
            _e: &Event,
        ) -> TraitResult<()> {
            self.fills.lock().unwrap().push((id, filled, status));
            Ok(())
        }
        async fn insert_order(&self, _o: &TradingOrder) -> TraitResult<()> {
            unimplemented!()
        }
        async fn insert_order_with_event(&self, _o: &TradingOrder, _e: &Event) -> TraitResult<()> {
            unimplemented!()
        }
        async fn get_or_create_active_epoch(&self) -> TraitResult<Uuid> {
            unimplemented!()
        }
        async fn get_order(&self, _id: Uuid) -> TraitResult<Option<TradingOrder>> {
            unimplemented!()
        }
        async fn get_orders_by_user(
            &self,
            _u: Uuid,
            _l: i64,
            _o: i64,
        ) -> TraitResult<Vec<TradingOrder>> {
            unimplemented!()
        }
        async fn get_active_orders_by_zone(&self, _z: i32) -> TraitResult<Vec<OrderBookEntry>> {
            unimplemented!()
        }
        async fn get_all_active_orders(&self) -> TraitResult<Vec<OrderBookEntry>> {
            unimplemented!()
        }
        async fn update_order_status(&self, _id: Uuid, _s: OrderStatus) -> TraitResult<()> {
            unimplemented!()
        }
        async fn update_filled_amount(
            &self,
            _id: Uuid,
            _f: Decimal,
            _s: OrderStatus,
        ) -> TraitResult<()> {
            unimplemented!()
        }
        async fn cancel_order(&self, _id: Uuid, _u: Uuid) -> TraitResult<bool> {
            unimplemented!()
        }
        async fn bootstrap_active_orders(&self) -> TraitResult<Vec<TradingOrder>> {
            unimplemented!()
        }
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
        async fn insert_match_with_event(
            &self,
            m: &OrderMatch,
            _settlement_id: Option<Uuid>,
            _zone_id: Option<i32>,
            _e: &Event,
        ) -> TraitResult<()> {
            self.matches.lock().unwrap().push(m.clone());
            Ok(())
        }
        async fn insert_settlement_with_event(
            &self,
            _s: &Settlement,
            _e: &Event,
        ) -> TraitResult<()> {
            unimplemented!()
        }
        async fn get_or_create_active_epoch(&self) -> TraitResult<Uuid> {
            unimplemented!()
        }
        async fn insert_match(
            &self,
            _m: &OrderMatch,
            _s: Option<Uuid>,
            _z: Option<i32>,
        ) -> TraitResult<()> {
            unimplemented!()
        }
        async fn get_settlement(&self, _id: Uuid) -> TraitResult<Option<Settlement>> {
            unimplemented!()
        }
        async fn get_pending_settlements(&self, _l: i64) -> TraitResult<Vec<Settlement>> {
            unimplemented!()
        }
        async fn claim_settlements_for_processing(
            &self,
            _ids: &[Uuid],
        ) -> TraitResult<Vec<Settlement>> {
            unimplemented!()
        }
        async fn reset_settlements_for_retry(
            &self,
            _ids: &[Uuid],
            _m: i32,
            _e: Option<&str>,
        ) -> TraitResult<u64> {
            unimplemented!()
        }
        async fn reclaim_stale_processing(&self, _s: i64, _m: i32) -> TraitResult<u64> {
            unimplemented!()
        }
        async fn list_settlements_for_user(
            &self,
            _u: Uuid,
            _l: i64,
            _o: i64,
        ) -> TraitResult<(Vec<Settlement>, i64)> {
            unimplemented!()
        }
        async fn get_settlement_stats(&self) -> TraitResult<SettlementStats> {
            unimplemented!()
        }
        async fn update_settlement_status(
            &self,
            _id: Uuid,
            _s: &str,
            _t: Option<&str>,
            _e: Option<&str>,
        ) -> TraitResult<()> {
            unimplemented!()
        }
        async fn update_settlement_status_with_event(
            &self,
            _id: Uuid,
            _s: &str,
            _t: Option<&str>,
            _e: Option<&str>,
            _ev: &Event,
        ) -> TraitResult<()> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn clears_interval_epoch_at_uniform_price() {
        let epoch = Uuid::new_v4();
        // bid 1.0 / ask 0.6, zone 1 → p* = midpoint 0.8, 10 units.
        let buy = order(OrderSide::Buy, dec!(1.0), dec!(10.0), 1, epoch, MarketSegment::Interval);
        let sell = order(
            OrderSide::Sell,
            dec!(0.6),
            dec!(10.0),
            1,
            epoch,
            MarketSegment::Interval,
        );
        let orders = Arc::new(MockOrderRepo {
            buys: vec![buy],
            sells: vec![sell],
            fills: Mutex::new(Vec::new()),
            ..Default::default()
        });
        let settlements = Arc::new(MockSettlementRepo::default());
        let svc = ClearingService::new(
            orders.clone(),
            settlements.clone(),
            Arc::new(NoFeeTopology),
        );

        let summary = svc.run_epoch_clearing(epoch).await.unwrap();

        assert_eq!(summary.zones_cleared, 1);
        assert_eq!(summary.matches, 1);
        assert_eq!(summary.total_volume, dec!(10.0));
        assert_eq!(summary.zone_prices, vec![(Some(1), dec!(0.8))]);
        assert_eq!(settlements.settlements.lock().unwrap().len(), 1);
        assert_eq!(settlements.matches.lock().unwrap().len(), 1);
        let s = &settlements.settlements.lock().unwrap()[0];
        assert_eq!(s.price, dec!(0.8), "settlement booked at the uniform price");
        assert_eq!(orders.fills.lock().unwrap().len(), 2, "both orders filled");
    }

    #[tokio::test]
    async fn ignores_realtime_segment_orders() {
        let epoch = Uuid::new_v4();
        let buy = order(OrderSide::Buy, dec!(1.0), dec!(10.0), 1, epoch, MarketSegment::Realtime);
        let sell = order(
            OrderSide::Sell,
            dec!(0.6),
            dec!(10.0),
            1,
            epoch,
            MarketSegment::Realtime,
        );
        let orders = Arc::new(MockOrderRepo {
            buys: vec![buy],
            sells: vec![sell],
            fills: Mutex::new(Vec::new()),
            ..Default::default()
        });
        let settlements = Arc::new(MockSettlementRepo::default());
        let svc = ClearingService::new(orders.clone(), settlements.clone(), Arc::new(NoFeeTopology));

        let summary = svc.run_epoch_clearing(epoch).await.unwrap();

        assert_eq!(summary.zones_cleared, 0, "Realtime orders belong to the CDA matcher");
        assert!(settlements.settlements.lock().unwrap().is_empty());
        assert!(orders.fills.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn ignores_other_epoch_orders() {
        let epoch = Uuid::new_v4();
        let other = Uuid::new_v4();
        let buy = order(OrderSide::Buy, dec!(1.0), dec!(10.0), 1, epoch, MarketSegment::Interval);
        // Sell belongs to a DIFFERENT epoch → no counterparty for `epoch`.
        let sell = order(
            OrderSide::Sell,
            dec!(0.6),
            dec!(10.0),
            1,
            other,
            MarketSegment::Interval,
        );
        let orders = Arc::new(MockOrderRepo {
            buys: vec![buy],
            sells: vec![sell],
            fills: Mutex::new(Vec::new()),
            ..Default::default()
        });
        let settlements = Arc::new(MockSettlementRepo::default());
        let svc = ClearingService::new(orders.clone(), settlements.clone(), Arc::new(NoFeeTopology));

        let summary = svc.run_epoch_clearing(epoch).await.unwrap();

        assert_eq!(summary.zones_cleared, 0);
        assert!(orders.fills.lock().unwrap().is_empty());
    }

    /// `clear_due_epochs` clears each due epoch and closes it with the
    /// single-zone price + summary stamped via `mark_epoch_cleared`.
    #[tokio::test]
    async fn clears_and_closes_due_epochs() {
        let epoch = Uuid::new_v4();
        let buy = order(OrderSide::Buy, dec!(1.0), dec!(10.0), 1, epoch, MarketSegment::Interval);
        let sell = order(
            OrderSide::Sell,
            dec!(0.6),
            dec!(10.0),
            1,
            epoch,
            MarketSegment::Interval,
        );
        let orders = Arc::new(MockOrderRepo {
            buys: vec![buy],
            sells: vec![sell],
            fills: Mutex::new(Vec::new()),
            due_epochs: vec![epoch],
            ..Default::default()
        });
        let settlements = Arc::new(MockSettlementRepo::default());
        let svc = ClearingService::new(orders.clone(), settlements.clone(), Arc::new(NoFeeTopology));

        let summaries = svc.clear_due_epochs().await.unwrap();

        assert_eq!(summaries.len(), 1);
        let cleared = orders.cleared.lock().unwrap();
        assert_eq!(
            *cleared,
            vec![(epoch, Some(dec!(0.8)), dec!(10.0), 1)],
            "epoch closed with single-zone price, volume, matched count"
        );
    }

    /// An empty due epoch (no matchable orders) is still closed — otherwise it
    /// lingers `active` and is re-selected every tick. Price is NULL.
    #[tokio::test]
    async fn closes_empty_due_epoch() {
        let epoch = Uuid::new_v4();
        let orders = Arc::new(MockOrderRepo {
            due_epochs: vec![epoch],
            ..Default::default()
        });
        let settlements = Arc::new(MockSettlementRepo::default());
        let svc = ClearingService::new(orders.clone(), settlements.clone(), Arc::new(NoFeeTopology));

        let summaries = svc.clear_due_epochs().await.unwrap();

        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].matches, 0);
        let cleared = orders.cleared.lock().unwrap();
        assert_eq!(
            *cleared,
            vec![(epoch, None, Decimal::ZERO, 0)],
            "empty epoch still closed, price NULL"
        );
    }
}
