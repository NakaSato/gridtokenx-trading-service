//! Pure synchronous uniform-price auction clearing engine.
//!
//! The second of the two trading mechanisms (see
//! `docs/design-docs/dual-mechanism-trading.md`). Where the CDA matcher
//! (`engine.rs`) clears continuously and each trade prices independently, this
//! engine clears a whole batch of orders at a **single uniform price per zone**
//! — the intended mechanism for 15-minute interval meter trading (prosumer /
//! consumer).
//!
//! Like the CDA engine it is **pure**: zero async, zero I/O, zero DB. It mutates
//! `filled_amount` on the order slices in place and returns the resulting fills.
//!
//! ## Algorithm (per zone)
//! 1. Demand curve = eligible buys sorted by price **descending** (most
//!    aggressive first); supply curve = eligible sells sorted **ascending**.
//! 2. Walk both with a two-pointer sweep while `buy.price >= sell.price`,
//!    trading `min(remaining)` each step. This yields the clearing quantity and
//!    the marginal accepted bid `b_m` and ask `s_m`.
//! 3. The single clearing price is `p* = midpoint(b_m, s_m)`, which lies in
//!    `[s_m, b_m]` so every matched buyer pays `<= bid` and every matched seller
//!    receives `>= ask` (individually rational).
//! 4. All fills in the zone settle at `p*`.
//!
//! **Marginal proration** is resolved by **price-time priority** (the marginal
//! order is partially filled with whatever residual remains), consistent with
//! the CDA engine. The pro-rata-by-size alternative is an open design decision
//! (proposal §5) and intentionally not implemented here.
//!
//! Zones are cleared independently — there is no cross-zone trading in this
//! mechanism (proposal §5, open: cross-zone handling).

use crate::engine::{TopologySnapshot, MIN_TRADE_AMOUNT};
use crate::types::{CycleStats, FastOrder, MatchResult, OrderMetadata};
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use trading_core::fast_price::FastPrice;

/// Per-zone clearing outcome.
#[derive(Debug, Clone)]
pub struct ZoneClearing {
    pub zone_id: Option<i32>,
    /// The single uniform clearing price `p*` paid by every matched order.
    pub clearing_price: Decimal,
    pub cleared_volume: Decimal,
    pub matches: Vec<MatchResult>,
}

/// Result of one clearing cycle: one [`ZoneClearing`] per zone that cleared.
#[derive(Debug, Clone, Default)]
pub struct ClearingResult {
    pub zones: Vec<ZoneClearing>,
}

/// The pure uniform-price auction engine.
pub struct UniformAuction;

impl UniformAuction {
    /// Clear a batch of orders, one uniform price per zone.
    ///
    /// `buy_orders` / `sell_orders` are mutated in place (`filled_amount`).
    pub fn clear_cycle(
        buy_orders: &mut [FastOrder],
        sell_orders: &mut [FastOrder],
        buy_metadata: &[OrderMetadata],
        _sell_metadata: &[OrderMetadata],
        topology: &dyn TopologySnapshot,
        now_ns: i64,
    ) -> (ClearingResult, CycleStats) {
        let mut result = ClearingResult::default();
        let mut stats = CycleStats::default();

        // Partition eligible order indices by zone. BTreeMap → deterministic
        // zone order. Eligible = tradeable size remaining and not expired.
        let buys_by_zone = Self::partition_by_zone(buy_orders, now_ns);
        let sells_by_zone = Self::partition_by_zone(sell_orders, now_ns);

        for (&zone, buy_ix) in &buys_by_zone {
            let Some(sell_ix) = sells_by_zone.get(&zone) else {
                continue; // no supply in this zone
            };
            if let Some(zc) = Self::clear_zone(
                zone,
                buy_ix,
                sell_ix,
                buy_orders,
                sell_orders,
                buy_metadata,
                topology,
            ) {
                stats.matches_created += zc.matches.len();
                stats.total_volume += zc.cleared_volume;
                result.zones.push(zc);
            }
        }

        (result, stats)
    }

    fn partition_by_zone(orders: &[FastOrder], now_ns: i64) -> BTreeMap<Option<i32>, Vec<usize>> {
        let mut map: BTreeMap<Option<i32>, Vec<usize>> = BTreeMap::new();
        for (idx, o) in orders.iter().enumerate() {
            if o.remaining_amount() >= MIN_TRADE_AMOUNT && !o.is_expired(now_ns) {
                map.entry(o.zone_id).or_default().push(idx);
            }
        }
        map
    }

    fn clear_zone(
        zone: Option<i32>,
        buy_ix: &[usize],
        sell_ix: &[usize],
        buy_orders: &mut [FastOrder],
        sell_orders: &mut [FastOrder],
        buy_metadata: &[OrderMetadata],
        topology: &dyn TopologySnapshot,
    ) -> Option<ZoneClearing> {
        // Demand: price desc, then time, then id. Supply: price asc, then time, id.
        let mut buys: Vec<usize> = buy_ix.to_vec();
        let mut sells: Vec<usize> = sell_ix.to_vec();
        buys.sort_unstable_by(|&a, &b| {
            buy_orders[b]
                .price
                .cmp(&buy_orders[a].price)
                .then_with(|| buy_orders[a].created_at_ns.cmp(&buy_orders[b].created_at_ns))
                .then_with(|| buy_orders[a].id.cmp(&buy_orders[b].id))
        });
        sells.sort_unstable_by(|&a, &b| {
            sell_orders[a]
                .price
                .cmp(&sell_orders[b].price)
                .then_with(|| sell_orders[a].created_at_ns.cmp(&sell_orders[b].created_at_ns))
                .then_with(|| sell_orders[a].id.cmp(&sell_orders[b].id))
        });

        // Two-pointer sweep. Record (buy_idx, sell_idx, amount); track marginals.
        let mut fills: Vec<(usize, usize, Decimal)> = Vec::new();
        let mut marginal_bid: Option<FastPrice> = None;
        let mut marginal_ask: Option<FastPrice> = None;
        let (mut i, mut j) = (0usize, 0usize);

        while i < buys.len() && j < sells.len() {
            let bi = buys[i];
            let sj = sells[j];
            let buy_price = buy_orders[bi].price;
            let sell_price = sell_orders[sj].price;

            // Demand desc / supply asc: once the top bid can't cover the cheapest
            // remaining ask, nothing further can cross.
            if buy_price < sell_price {
                break;
            }
            // Self-trade: advance supply to try the next seller for this buyer.
            if buy_orders[bi].user_id == sell_orders[sj].user_id {
                j += 1;
                continue;
            }

            let buy_rem = buy_orders[bi].remaining_amount();
            let sell_rem = sell_orders[sj].remaining_amount();
            if buy_rem < MIN_TRADE_AMOUNT {
                i += 1;
                continue;
            }
            if sell_rem < MIN_TRADE_AMOUNT {
                j += 1;
                continue;
            }

            let amount = buy_rem.min(sell_rem);
            fills.push((bi, sj, amount));
            buy_orders[bi].filled_amount += amount;
            sell_orders[sj].filled_amount += amount;
            marginal_bid = Some(buy_price);
            marginal_ask = Some(sell_price);

            if buy_orders[bi].remaining_amount() < MIN_TRADE_AMOUNT {
                i += 1;
            }
            if sell_orders[sj].remaining_amount() < MIN_TRADE_AMOUNT {
                j += 1;
            }
        }

        let (Some(b_m), Some(s_m)) = (marginal_bid, marginal_ask) else {
            return None; // nothing cleared in this zone
        };
        if fills.is_empty() {
            return None;
        }

        // Uniform clearing price = midpoint of the marginal bid/ask, in [s_m, b_m].
        // i64::midpoint avoids overflow on the raw sum.
        let p_star_fp = FastPrice::from_raw(i64::midpoint(b_m.raw(), s_m.raw()));
        Some(Self::settle_fills(
            zone,
            &fills,
            p_star_fp,
            buy_orders,
            sell_orders,
            buy_metadata,
            topology,
        ))
    }

    /// Build the per-fill [`MatchResult`]s, all priced at the uniform `p*`.
    fn settle_fills(
        zone: Option<i32>,
        fills: &[(usize, usize, Decimal)],
        p_star_fp: FastPrice,
        buy_orders: &[FastOrder],
        sell_orders: &[FastOrder],
        buy_metadata: &[OrderMetadata],
        topology: &dyn TopologySnapshot,
    ) -> ZoneClearing {
        let p_star = p_star_fp.to_decimal();

        // Same-zone wheeling / loss (constant for the zone). Loss floored at 0 —
        // a sub-unit loss factor would be a physical gain.
        let wheeling_fp = topology.calculate_wheeling_charge(zone, zone);
        let loss_fp = topology.calculate_loss_factor(zone, zone);
        let extra_loss_raw = loss_fp.raw().saturating_sub(FastPrice::FACTOR).max(0);
        let loss_cost_unit_raw = i64::try_from(
            i128::from(p_star_fp.raw()) * i128::from(extra_loss_raw) / i128::from(FastPrice::FACTOR),
        )
        .unwrap_or(i64::MAX);
        let loss_cost_unit = FastPrice::from_raw(loss_cost_unit_raw).to_decimal();
        let wheeling_unit = wheeling_fp.to_decimal();
        let loss_factor = loss_fp.to_decimal();

        let mut matches = Vec::with_capacity(fills.len());
        let mut cleared_volume = Decimal::ZERO;
        for &(bi, sj, amount) in fills {
            let buy = &buy_orders[bi];
            let sell = &sell_orders[sj];
            let buy_meta = &buy_metadata[buy.metadata_index];
            cleared_volume += amount;
            matches.push(MatchResult {
                buy_order_id: buy.id,
                sell_order_id: sell.id,
                buy_metadata_index: buy.metadata_index,
                sell_metadata_index: sell.metadata_index,
                match_amount: amount,
                match_price: p_star,
                total_energy_cost: amount * p_star,
                wheeling_charge: amount * wheeling_unit,
                loss_factor,
                loss_cost: amount * loss_cost_unit,
                buyer_zone_id: buy.zone_id,
                seller_zone_id: sell.zone_id,
                buyer_id: buy.user_id,
                seller_id: sell.user_id,
                epoch_id: buy_meta.epoch_id.unwrap_or_default(),
            });
        }

        ZoneClearing {
            zone_id: zone,
            clearing_price: p_star,
            cleared_volume,
            matches,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::TopologySnapshot;
    use rust_decimal_macros::dec;
    use trading_core::types::TimeInForce;
    use uuid::Uuid;

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

    fn order(
        user: Uuid,
        price: Decimal,
        amount: Decimal,
        zone: i32,
        t: i64,
        mi: usize,
    ) -> FastOrder {
        FastOrder {
            id: Uuid::new_v4(),
            user_id: user,
            price: FastPrice::from(price),
            energy_amount: amount,
            filled_amount: dec!(0.0),
            zone_id: Some(zone),
            created_at_ns: t,
            expires_at_ns: None,
            time_in_force: TimeInForce::Gtc,
            metadata_index: mi,
        }
    }

    fn meta(n: usize) -> Vec<OrderMetadata> {
        (0..n)
            .map(|_| OrderMetadata {
                epoch_id: None,
                order_pda: None,
                session_token: None,
            })
            .collect()
    }

    #[test]
    fn single_crossing_clears_at_midpoint() {
        // bid 1.0 vs ask 0.6 → p* = midpoint = 0.8, full 10 units cleared.
        let mut buys = vec![order(Uuid::new_v4(), dec!(1.0), dec!(10.0), 1, 100, 0)];
        let mut sells = vec![order(Uuid::new_v4(), dec!(0.6), dec!(10.0), 1, 100, 0)];
        let (res, stats) = UniformAuction::clear_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            200,
        );
        assert_eq!(res.zones.len(), 1);
        assert_eq!(res.zones[0].clearing_price, dec!(0.8));
        assert_eq!(res.zones[0].cleared_volume, dec!(10.0));
        assert_eq!(stats.matches_created, 1);
        assert_eq!(stats.total_volume, dec!(10.0));
        assert_eq!(res.zones[0].matches[0].match_price, dec!(0.8));
    }

    #[test]
    fn no_cross_returns_empty() {
        // bid below ask → nothing clears.
        let mut buys = vec![order(Uuid::new_v4(), dec!(0.5), dec!(10.0), 1, 100, 0)];
        let mut sells = vec![order(Uuid::new_v4(), dec!(0.9), dec!(10.0), 1, 100, 0)];
        let (res, stats) = UniformAuction::clear_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            200,
        );
        assert!(res.zones.is_empty());
        assert_eq!(stats.matches_created, 0);
    }

    #[test]
    fn flat_marginal_prorates_by_time_priority() {
        // Demand 15 @ 1.0. Supply: two sells 10 @ 0.5 each, earlier one first.
        // Clearing fills the earlier sell fully (10) and the later one partially
        // (5) — time-priority proration of the marginal order.
        let buyer = Uuid::new_v4();
        let s_early = Uuid::new_v4();
        let s_late = Uuid::new_v4();
        let mut buys = vec![order(buyer, dec!(1.0), dec!(15.0), 1, 100, 0)];
        let early = order(s_early, dec!(0.5), dec!(10.0), 1, 100, 0);
        let late = order(s_late, dec!(0.5), dec!(10.0), 1, 200, 1);
        let early_id = early.id;
        let late_id = late.id;
        let mut sells = vec![late, early]; // unsorted on input
        let (res, stats) = UniformAuction::clear_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(2),
            &NoFeeTopology,
            300,
        );
        assert_eq!(res.zones.len(), 1);
        assert_eq!(res.zones[0].cleared_volume, dec!(15.0));
        assert_eq!(stats.total_volume, dec!(15.0));
        // p* = midpoint(1.0, 0.5) = 0.75
        assert_eq!(res.zones[0].clearing_price, dec!(0.75));
        let filled: std::collections::HashMap<Uuid, Decimal> = res.zones[0]
            .matches
            .iter()
            .map(|m| (m.sell_order_id, m.match_amount))
            .collect();
        assert_eq!(filled.get(&early_id), Some(&dec!(10.0)), "earlier sell full");
        assert_eq!(filled.get(&late_id), Some(&dec!(5.0)), "later sell partial");
    }

    #[test]
    fn zones_clear_independently() {
        // Buyer in zone 1, seller in zone 2 → no cross-zone trade.
        let mut buys = vec![order(Uuid::new_v4(), dec!(1.0), dec!(10.0), 1, 100, 0)];
        let mut sells = vec![order(Uuid::new_v4(), dec!(0.5), dec!(10.0), 2, 100, 0)];
        let (res, _) = UniformAuction::clear_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            200,
        );
        assert!(res.zones.is_empty(), "different zones must not clear together");
    }

    #[test]
    fn two_zones_each_get_own_price() {
        // Zone 1: bid 1.0 / ask 0.6 → 0.8. Zone 2: bid 2.0 / ask 1.0 → 1.5.
        let mut buys = vec![
            order(Uuid::new_v4(), dec!(1.0), dec!(10.0), 1, 100, 0),
            order(Uuid::new_v4(), dec!(2.0), dec!(10.0), 2, 100, 1),
        ];
        let mut sells = vec![
            order(Uuid::new_v4(), dec!(0.6), dec!(10.0), 1, 100, 0),
            order(Uuid::new_v4(), dec!(1.0), dec!(10.0), 2, 100, 1),
        ];
        let (res, _) = UniformAuction::clear_cycle(
            &mut buys,
            &mut sells,
            &meta(2),
            &meta(2),
            &NoFeeTopology,
            200,
        );
        assert_eq!(res.zones.len(), 2);
        let by_zone: std::collections::HashMap<Option<i32>, Decimal> = res
            .zones
            .iter()
            .map(|z| (z.zone_id, z.clearing_price))
            .collect();
        assert_eq!(by_zone.get(&Some(1)), Some(&dec!(0.8)));
        assert_eq!(by_zone.get(&Some(2)), Some(&dec!(1.5)));
    }

    #[test]
    fn tie_at_clearing_price_is_deterministic() {
        // Two sells at the same ask; earlier one fills first, stable across runs.
        let buyer = Uuid::new_v4();
        let s1 = order(Uuid::new_v4(), dec!(0.5), dec!(6.0), 1, 100, 0);
        let s2 = order(Uuid::new_v4(), dec!(0.5), dec!(6.0), 1, 200, 1);
        let first_id = s1.id;
        for _ in 0..5 {
            let mut buys = vec![order(buyer, dec!(1.0), dec!(6.0), 1, 100, 0)];
            let mut sells = vec![s2, s1];
            let (res, _) = UniformAuction::clear_cycle(
                &mut buys,
                &mut sells,
                &meta(1),
                &meta(2),
                &NoFeeTopology,
                300,
            );
            assert_eq!(res.zones[0].matches.len(), 1);
            assert_eq!(
                res.zones[0].matches[0].sell_order_id, first_id,
                "earlier sell must win the tie every run"
            );
        }
    }

    #[test]
    fn self_trade_is_skipped() {
        // Buyer == self-seller; a distinct seller exists at a slightly higher ask.
        let me = Uuid::new_v4();
        let other = Uuid::new_v4();
        let mut buys = vec![order(me, dec!(1.0), dec!(10.0), 1, 100, 0)];
        let self_sell = order(me, dec!(0.5), dec!(10.0), 1, 100, 0);
        let other_sell = order(other, dec!(0.6), dec!(10.0), 1, 100, 1);
        let other_id = other_sell.id;
        let mut sells = vec![self_sell, other_sell];
        let (res, _) = UniformAuction::clear_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(2),
            &NoFeeTopology,
            200,
        );
        assert_eq!(res.zones.len(), 1);
        assert_eq!(res.zones[0].matches.len(), 1);
        assert_eq!(
            res.zones[0].matches[0].seller_id, other,
            "must trade with the other party, not self"
        );
        assert_eq!(res.zones[0].matches[0].sell_order_id, other_id);
    }

    #[test]
    fn expired_orders_are_skipped() {
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        let mut buys = vec![order(buyer, dec!(1.0), dec!(10.0), 1, 100, 0)];
        let mut sells = vec![order(seller, dec!(0.5), dec!(10.0), 1, 100, 0)];
        sells[0].expires_at_ns = Some(150); // expires before now_ns = 200
        let (res, _) = UniformAuction::clear_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            200,
        );
        assert!(res.zones.is_empty(), "expired sell must not clear");
    }
}
