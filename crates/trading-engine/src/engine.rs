//! Pure synchronous CDA matching engine.

use crate::types::{CycleStats, FastOrder, MatchResult, OrderMetadata};
use rust_decimal::Decimal;
use trading_core::fast_price::FastPrice;
use trading_core::types::TimeInForce;

/// Minimum amount of energy to allow a trade.
pub const MIN_TRADE_AMOUNT: Decimal = rust_decimal_macros::dec!(0.001);

/// Discount applied to intra-zone trades to encourage local balancing.
pub const INTRA_ZONE_DISCOUNT: Decimal = rust_decimal_macros::dec!(0.05);

/// Trait for the grid topology snapshot.
/// Allows the matching engine to remain pure and testable.
pub trait TopologySnapshot: Send + Sync {
    fn can_accommodate_flow(
        &self,
        from_zone: Option<i32>,
        to_zone: Option<i32>,
        amount: Decimal,
    ) -> bool;
    fn calculate_wheeling_charge(&self, from_zone: Option<i32>, to_zone: Option<i32>) -> FastPrice;
    fn calculate_loss_factor(&self, from_zone: Option<i32>, to_zone: Option<i32>) -> FastPrice;
}

use std::collections::{BTreeMap, HashMap};

/// The pure matching engine.
pub struct MatchingEngine;

impl MatchingEngine {
    /// Execute a matching cycle on a set of orders.
    /// Optimized for grid-scale throughput using zone-segmented order books.
    pub fn match_cycle(
        buy_orders: &mut [FastOrder],
        sell_orders: &mut [FastOrder],
        buy_metadata: &[OrderMetadata],
        _sell_metadata: &[OrderMetadata],
        topology: &dyn TopologySnapshot,
        dynamic_multiplier: FastPrice,
        now_ns: i64,
    ) -> (Vec<MatchResult>, CycleStats) {
        let mut results = Vec::new();
        let mut stats = CycleStats::default();

        // 1. Segment active sell orders by Zone and Price-Time priority
        // Map: ZoneId -> BTreeMap<(Price, CreatedAt, Id), Index>
        let mut zone_books: HashMap<Option<i32>, BTreeMap<(FastPrice, i64, uuid::Uuid), usize>> =
            HashMap::new();
        for (idx, sell) in sell_orders.iter().enumerate() {
            if sell.remaining_amount() >= MIN_TRADE_AMOUNT && !sell.is_expired(now_ns) {
                zone_books
                    .entry(sell.zone_id)
                    .or_default()
                    .insert((sell.price, sell.created_at_ns, sell.id), idx);
            }
        }

        if zone_books.is_empty() {
            return (results, stats);
        }

        // 2. Iterate buys and match against reachable zone books
        for buy in buy_orders.iter_mut() {
            if buy.remaining_amount() < MIN_TRADE_AMOUNT || buy.is_expired(now_ns) {
                continue;
            }

            let mut candidates = Vec::new();

            // Optimization: Only iterate through zones that can reach the buyer's zone
            for (&sell_zone_id, book) in &zone_books {
                // Topology pre-filtering: can the grid move energy from sell_zone to buy_zone?
                // We check with MIN_TRADE_AMOUNT as a baseline.
                if !topology.can_accommodate_flow(sell_zone_id, buy.zone_id, MIN_TRADE_AMOUNT) {
                    continue;
                }

                // Range query: all sells in this zone with price <= buy.price
                for ((sell_price, _, _), &sell_idx) in
                    book.range(..=(buy.price, i64::MAX, uuid::Uuid::max()))
                {
                    let sell = &sell_orders[sell_idx];

                    if buy.user_id == sell.user_id {
                        continue;
                    }

                    // Strict topology check for actual amount
                    let potential_amount = buy.remaining_amount().min(sell.remaining_amount());
                    if !topology.can_accommodate_flow(sell_zone_id, buy.zone_id, potential_amount) {
                        continue;
                    }

                    let wheeling_fp = topology.calculate_wheeling_charge(sell_zone_id, buy.zone_id);
                    let loss_fp = topology.calculate_loss_factor(sell_zone_id, buy.zone_id);

                    let extra_loss_raw = loss_fp.raw().saturating_sub(FastPrice::FACTOR);
                    let loss_cost_extra_raw = (sell_price.raw() as i128 * extra_loss_raw as i128
                        / FastPrice::FACTOR as i128)
                        as i64;
                    let mut landed_cost = FastPrice::from_raw(
                        sell_price.raw() + wheeling_fp.raw() + loss_cost_extra_raw,
                    );

                    if dynamic_multiplier != FastPrice::from_raw(FastPrice::FACTOR) {
                        landed_cost = landed_cost.unchecked_mul(dynamic_multiplier);
                    }

                    if sell_zone_id == buy.zone_id {
                        const DISCOUNT_FP_RAW: i64 =
                            (FastPrice::FACTOR as f64 * (1.0 - 0.05)) as i64;
                        landed_cost =
                            landed_cost.unchecked_mul(FastPrice::from_raw(DISCOUNT_FP_RAW));
                    }

                    if landed_cost <= buy.price {
                        candidates.push((
                            sell_idx,
                            landed_cost,
                            wheeling_fp,
                            loss_fp,
                            FastPrice::from_raw(loss_cost_extra_raw),
                        ));
                    }
                }
            }

            // Sort consolidated candidates from all reachable zones by landed cost
            candidates.sort_unstable_by(|a, b| a.1.cmp(&b.1));

            // FOK handling
            if buy.time_in_force == TimeInForce::Fok {
                let total_available: Decimal = candidates
                    .iter()
                    .map(|c| sell_orders[c.0].remaining_amount())
                    .sum();
                if total_available < buy.remaining_amount() {
                    continue;
                }
            }

            for (sell_idx, landed_cost_fp, wheeling_fp, loss_fp, loss_cost_fp) in candidates {
                if buy.remaining_amount() < MIN_TRADE_AMOUNT {
                    break;
                }

                let sell = &mut sell_orders[sell_idx];
                if sell.remaining_amount() < MIN_TRADE_AMOUNT {
                    continue;
                }

                let match_amount = buy.remaining_amount().min(sell.remaining_amount());
                let buy_meta_idx = buy.metadata_index;
                let sell_meta_idx = sell.metadata_index;

                // Optimization: Match Consolidation
                // If the last result was the same buyer/seller pair at the same price, merge them.
                if let Some(last) = results.last_mut() {
                    if last.buy_order_id == buy.id
                        && last.sell_order_id == sell.id
                        && last.match_price == landed_cost_fp.to_decimal()
                    {
                        last.match_amount += match_amount;
                        last.total_energy_cost += match_amount * landed_cost_fp.to_decimal();
                        last.wheeling_charge += match_amount * wheeling_fp.to_decimal();
                        last.loss_cost += match_amount * loss_cost_fp.to_decimal();

                        buy.filled_amount += match_amount;
                        sell.filled_amount += match_amount;
                        stats.total_volume += match_amount;

                        if sell.remaining_amount() < MIN_TRADE_AMOUNT {
                            zone_books.get_mut(&sell.zone_id).unwrap().remove(&(
                                sell.price,
                                sell.created_at_ns,
                                sell.id,
                            ));
                        }
                        continue;
                    }
                }

                let buy_meta = &buy_metadata[buy_meta_idx];

                results.push(MatchResult {
                    buy_order_id: buy.id,
                    sell_order_id: sell.id,
                    buy_metadata_index: buy_meta_idx,
                    sell_metadata_index: sell_meta_idx,
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
                });

                buy.filled_amount += match_amount;
                sell.filled_amount += match_amount;
                stats.matches_created += 1;
                stats.total_volume += match_amount;

                // Cleanup books if sell is depleted
                if sell.remaining_amount() < MIN_TRADE_AMOUNT {
                    if let Some(book) = zone_books.get_mut(&sell.zone_id) {
                        book.remove(&(sell.price, sell.created_at_ns, sell.id));
                    }
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
    use trading_core::fast_price::FastPrice;
    use uuid::Uuid;

    struct MockTopology;
    impl TopologySnapshot for MockTopology {
        fn can_accommodate_flow(&self, _f: Option<i32>, _t: Option<i32>, _a: Decimal) -> bool {
            true
        }
        fn calculate_wheeling_charge(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice {
            FastPrice::from(dec!(0.01))
        }
        fn calculate_loss_factor(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice {
            FastPrice::from(dec!(1.02))
        }
    }

    #[test]
    fn test_basic_match() {
        let mut buys = vec![FastOrder {
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
        }];
        let buy_meta = vec![OrderMetadata {
            epoch_id: None,
            order_pda: None,
            session_token: None,
        }];

        let mut sells = vec![FastOrder {
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
        }];
        let sell_meta = vec![OrderMetadata {
            epoch_id: None,
            order_pda: None,
            session_token: None,
        }];

        let topo = MockTopology;
        let (matches, stats) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &buy_meta,
            &sell_meta,
            &topo,
            FastPrice::from(dec!(1.0)),
            200,
        );

        assert_eq!(stats.matches_created, 1);
        assert_eq!(stats.total_volume, dec!(50.0));
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].match_amount, dec!(50.0));
    }
}
