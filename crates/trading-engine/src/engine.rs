//! Pure synchronous CDA matching engine.

use rust_decimal::Decimal;
use trading_core::fast_price::FastPrice;
use trading_core::types::TimeInForce;
use crate::types::{FastOrder, OrderMetadata, MatchResult, CycleStats};

/// Minimum amount of energy to allow a trade.
pub const MIN_TRADE_AMOUNT: Decimal = rust_decimal_macros::dec!(0.001);

/// Discount applied to intra-zone trades to encourage local balancing.
pub const INTRA_ZONE_DISCOUNT: Decimal = rust_decimal_macros::dec!(0.05);

/// Trait for the grid topology snapshot.
/// Allows the matching engine to remain pure and testable.
pub trait TopologySnapshot: Send + Sync {
    fn can_accommodate_flow(&self, from_zone: Option<i32>, to_zone: Option<i32>, amount: Decimal) -> bool;
    fn calculate_wheeling_charge(&self, from_zone: Option<i32>, to_zone: Option<i32>) -> FastPrice;
    fn calculate_loss_factor(&self, from_zone: Option<i32>, to_zone: Option<i32>) -> FastPrice;
}

/// The pure matching engine.
pub struct MatchingEngine;

impl MatchingEngine {
    /// Execute a matching cycle on a set of orders.
    /// This function is pure and synchronous.
    pub fn match_cycle(
        buy_orders: &mut [FastOrder],
        sell_orders: &mut [FastOrder],
        buy_metadata: &[OrderMetadata],
        sell_metadata: &[OrderMetadata],
        topology: &dyn TopologySnapshot,
        dynamic_multiplier: FastPrice,
        now_ns: i64,
    ) -> (Vec<MatchResult>, CycleStats) {
        let mut results = Vec::new();
        let mut stats = CycleStats::default();

        // NOTE: Inputs are expected to be pre-sorted by MatcherService.
        // Buy orders: Creation time (FIFO)
        // Sell orders: Price then Creation time (Price-Time Priority)

        for buy in buy_orders.iter_mut() {
            if buy.remaining_amount() < MIN_TRADE_AMOUNT || buy.is_expired(now_ns) {
                continue;
            }

            // Find candidates for this buy order
            let mut candidates = Vec::with_capacity(sell_orders.len());
            for sell in sell_orders.iter() {
                if sell.remaining_amount() < MIN_TRADE_AMOUNT || sell.is_expired(now_ns) || buy.user_id == sell.user_id {
                    continue;
                }

                if !topology.can_accommodate_flow(sell.zone_id, buy.zone_id, sell.remaining_amount().min(buy.remaining_amount())) {
                    continue;
                }

                let wheeling_fp = topology.calculate_wheeling_charge(sell.zone_id, buy.zone_id);
                let loss_fp = topology.calculate_loss_factor(sell.zone_id, buy.zone_id);
                
                // Optimized arithmetic using unchecked where safe
                // (loss_factor - 1.0)
                let extra_loss_raw = loss_fp.raw().saturating_sub(FastPrice::FACTOR); 
                let loss_cost_extra_raw = (sell.price.raw() as i128 * extra_loss_raw as i128 / FastPrice::FACTOR as i128) as i64;
                let mut landed_cost = FastPrice::from_raw(sell.price.raw() + wheeling_fp.raw() + loss_cost_extra_raw);

                // Apply dynamic multiplier (ToU)
                if dynamic_multiplier != FastPrice::from_raw(FastPrice::FACTOR) {
                    landed_cost = landed_cost.unchecked_mul(dynamic_multiplier);
                }

                // Apply Intra-zone discount
                if sell.zone_id == buy.zone_id {
                    const DISCOUNT_FP_RAW: i64 = (FastPrice::FACTOR as f64 * (1.0 - 0.05)) as i64; // 0.95 at FACTOR scale
                    landed_cost = landed_cost.unchecked_mul(FastPrice::from_raw(DISCOUNT_FP_RAW));
                }

                if landed_cost <= buy.price {
                    candidates.push((sell.id, landed_cost, wheeling_fp, loss_fp, FastPrice::from_raw(loss_cost_extra_raw)));
                }
            }

            // Sort candidates by landed cost (cheapest first)
            candidates.sort_unstable_by(|a, b| a.1.cmp(&b.1));

            // FOK (Fill-or-Kill) handling
            if buy.time_in_force == TimeInForce::Fok {
                let mut total_available = Decimal::ZERO;
                for (sell_id, _, _, _, _) in &candidates {
                    if let Some(sell) = sell_orders.iter().find(|s| s.id == *sell_id) {
                        total_available += sell.remaining_amount();
                    }
                }
                if total_available < buy.remaining_amount() {
                    continue;
                }
            }

            for (sell_id, landed_cost_fp, wheeling_fp, loss_fp, loss_cost_fp) in candidates {
                if buy.remaining_amount() < MIN_TRADE_AMOUNT {
                    break;
                }

                if let Some(sell) = sell_orders.iter_mut().find(|s| s.id == sell_id) {
                    if sell.remaining_amount() < MIN_TRADE_AMOUNT {
                        continue;
                    }

                    let match_amount = buy.remaining_amount().min(sell.remaining_amount());

                    let buy_meta = &buy_metadata[buy.metadata_index];
                    let sell_meta = &sell_metadata[sell.metadata_index];

                    results.push(MatchResult {
                        buy_order_id: buy.id,
                        sell_order_id: sell.id,
                        match_amount,
                        match_price: landed_cost_fp.to_decimal(),
                        total_energy_cost: match_amount * landed_cost_fp.to_decimal(),
                        wheeling_charge: match_amount * wheeling_fp.to_decimal(),
                        loss_factor: loss_fp.to_decimal(),
                        loss_cost: match_amount * loss_cost_fp.to_decimal(),
                        buyer_zone_id: buy.zone_id,
                        seller_zone_id: sell.zone_id,
                        buyer_id: buy.user_id,
                        seller_id: sell.user_id,
                        epoch_id: buy_meta.epoch_id.unwrap_or_default(),
                        buyer_session_token: buy_meta.session_token.clone(),
                        seller_session_token: sell_meta.session_token.clone(),
                        buyer_order_pda: buy_meta.order_pda.clone(),
                        seller_order_pda: sell_meta.order_pda.clone(),
                    });

                    buy.filled_amount += match_amount;
                    sell.filled_amount += match_amount;
                    stats.matches_created += 1;
                    stats.total_volume += match_amount;
                }
            }
        }

        (results, stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use uuid::Uuid;
    use trading_core::fast_price::FastPrice;

    struct MockTopology;
    impl TopologySnapshot for MockTopology {
        fn can_accommodate_flow(&self, _f: Option<i32>, _t: Option<i32>, _a: Decimal) -> bool { true }
        fn calculate_wheeling_charge(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice { FastPrice::from(dec!(0.01)) }
        fn calculate_loss_factor(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice { FastPrice::from(dec!(1.02)) }
    }

    #[test]
    fn test_basic_match() {
        let mut buys = vec![
            FastOrder {
                id: Uuid::new_v4(),
                user_id: Uuid::new_v4(),
                price: FastPrice::from(dec!(1.0)),
                energy_amount: dec!(100.0),
                filled_amount: dec!(0.0),
                zone_id: Some(1),
                created_at_ns: 100,
                expires_at_ns: None,
                time_in_force: TimeInForce::Gtc,
                metadata_index: 0,
            }
        ];
        let buy_meta = vec![OrderMetadata { epoch_id: None, order_pda: None, session_token: None }];

        let mut sells = vec![
            FastOrder {
                id: Uuid::new_v4(),
                user_id: Uuid::new_v4(),
                price: FastPrice::from(dec!(0.5)),
                energy_amount: dec!(50.0),
                filled_amount: dec!(0.0),
                zone_id: Some(2),
                created_at_ns: 100,
                expires_at_ns: None,
                time_in_force: TimeInForce::Gtc,
                metadata_index: 0,
            }
        ];
        let sell_meta = vec![OrderMetadata { epoch_id: None, order_pda: None, session_token: None }];

        let topo = MockTopology;
        let (matches, stats) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &buy_meta,
            &sell_meta,
            &topo,
            FastPrice::from(dec!(1.0)),
            200
        );

        assert_eq!(stats.matches_created, 1);
        assert_eq!(stats.total_volume, dec!(50.0));
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].match_amount, dec!(50.0));
    }
}
