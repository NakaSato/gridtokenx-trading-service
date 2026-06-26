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

use std::collections::BTreeMap;

/// The pure matching engine.
pub struct MatchingEngine;

/// A priced match candidate ready to fill, carrying its own price-time sort key
/// so the comparator never has to re-index the sell slice.
struct Candidate {
    sell_idx: usize,
    landed: FastPrice,
    wheeling: FastPrice,
    loss: FastPrice,
    loss_cost: FastPrice,
    sort_price: FastPrice,
    sort_time: i64,
    sort_id: uuid::Uuid,
}

/// Wheeling + loss for a (sell_zone, buy_zone) pair — constant across every sell
/// in one zone book for a given buy, so computed once per zone, not per sell.
#[derive(Clone, Copy)]
struct ZoneFees {
    wheeling: FastPrice,
    loss: FastPrice,
}

impl MatchingEngine {
    /// Price one sell against a buy. Returns a [`Candidate`] when the trade is
    /// viable: not a self-trade, the grid can carry the flow, and the landed
    /// cost crosses the bid. All raw fixed-point arithmetic is saturating /
    /// checked — overflow clamps to `i64::MAX` rather than wrapping, so an
    /// adversarial price can never wrap the accumulator into a false (negative)
    /// cross.
    fn priced_candidate(
        buy: &FastOrder,
        sell_idx: usize,
        sell: &FastOrder,
        topology: &dyn TopologySnapshot,
        fees: ZoneFees,
        dynamic_multiplier: FastPrice,
        intra_zone_mult: FastPrice,
    ) -> Option<Candidate> {
        if buy.user_id == sell.user_id {
            return None;
        }
        let sell_zone_id = sell.zone_id;

        // Strict topology check for the actual tradeable amount.
        let potential_amount = buy.remaining_amount().min(sell.remaining_amount());
        if !topology.can_accommodate_flow(sell_zone_id, buy.zone_id, potential_amount) {
            return None;
        }

        // wheeling/loss are zone-constant — passed in, not recomputed per sell.
        let wheeling_fp = fees.wheeling;
        let loss_fp = fees.loss;

        // Line loss can only ADD cost: a loss factor below 1.0 would be a physical
        // gain (impossible), so floor the excess at 0 rather than booking a negative
        // loss_cost into the settlement ledger.
        let extra_loss_raw = loss_fp.raw().saturating_sub(FastPrice::FACTOR).max(0);
        // i128 intermediate then clamp into i64 — never wraps on large prices.
        let loss_cost_extra_raw = i64::try_from(
            i128::from(sell.price.raw()) * i128::from(extra_loss_raw) / i128::from(FastPrice::FACTOR),
        )
        .unwrap_or(i64::MAX);

        let landed_raw = sell
            .price
            .raw()
            .saturating_add(wheeling_fp.raw())
            .saturating_add(loss_cost_extra_raw);
        let mut landed_cost = FastPrice::from_raw(landed_raw);

        // checked_mul clamps to i64::MAX on overflow. An over-large landed cost
        // stays huge (> any sane bid) so it simply fails to cross — never wraps
        // negative and books a spurious match.
        if dynamic_multiplier != FastPrice::from_raw(FastPrice::FACTOR) {
            landed_cost = landed_cost
                .checked_mul(dynamic_multiplier)
                .unwrap_or_else(|| FastPrice::from_raw(i64::MAX));
        }
        if sell_zone_id == buy.zone_id {
            landed_cost = landed_cost
                .checked_mul(intra_zone_mult)
                .unwrap_or_else(|| FastPrice::from_raw(i64::MAX));
        }

        if landed_cost <= buy.price {
            Some(Candidate {
                sell_idx,
                landed: landed_cost,
                wheeling: wheeling_fp,
                loss: loss_fp,
                loss_cost: FastPrice::from_raw(loss_cost_extra_raw),
                sort_price: sell.price,
                sort_time: sell.created_at_ns,
                sort_id: sell.id,
            })
        } else {
            None
        }
    }

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
        let mut results: Vec<MatchResult> = Vec::new();
        let mut stats = CycleStats::default();

        // 0. Enforce CDA buy-side priority here, in the engine, so correctness does
        // not depend on how the caller pre-sorted: highest bid first (most
        // aggressive buyer wins scarce liquidity), ties broken by earliest arrival
        // (price-time priority), then id for determinism.
        buy_orders.sort_unstable_by(|a, b| {
            b.price
                .cmp(&a.price)
                .then_with(|| a.created_at_ns.cmp(&b.created_at_ns))
                .then_with(|| a.id.cmp(&b.id))
        });

        // Intra-zone discount multiplier, derived once from the INTRA_ZONE_DISCOUNT
        // const (1 - discount). Integer/decimal math, no float, no drift.
        let intra_zone_mult = FastPrice::from(Decimal::ONE - INTRA_ZONE_DISCOUNT);

        // 1. Segment active sell orders by Zone and Price-Time priority.
        // BTreeMap (not HashMap) so zone iteration order is deterministic.
        // Map: ZoneId -> BTreeMap<(Price, CreatedAt, Id), Index>
        let mut zone_books: BTreeMap<Option<i32>, BTreeMap<(FastPrice, i64, uuid::Uuid), usize>> =
            BTreeMap::new();
        for (idx, sell) in sell_orders.iter().enumerate() {
            if sell.remaining_amount() >= MIN_TRADE_AMOUNT && !sell.is_expired(now_ns) {
                zone_books
                    .entry(sell.zone_id)
                    .or_default()
                    .insert((sell.price, sell.created_at_ns, sell.id), idx);
            }
        }

        // No early return on empty zone_books: the buy loop is a no-op without
        // sells, but flow must still reach the IOC sweep below so an IOC order
        // with no counterparty this cycle is reported for cancellation.

        // 2. Iterate buys and match against reachable zone books.
        // One scratch `candidates` buffer, reused across every buy (`clear()` keeps
        // the allocation, `drain()` empties it on consume) — avoids a fresh Vec +
        // grow per buy on the hot path.
        let mut candidates: Vec<Candidate> = Vec::new();
        for buy in buy_orders.iter_mut() {
            if buy.remaining_amount() < MIN_TRADE_AMOUNT || buy.is_expired(now_ns) {
                continue;
            }

            candidates.clear();

            // Optimization: Only iterate through zones that can reach the buyer's zone
            for (&sell_zone_id, book) in &zone_books {
                // Topology pre-filtering: can the grid move energy from sell_zone to buy_zone?
                // We check with MIN_TRADE_AMOUNT as a baseline.
                if !topology.can_accommodate_flow(sell_zone_id, buy.zone_id, MIN_TRADE_AMOUNT) {
                    continue;
                }

                // wheeling + loss depend only on (sell_zone, buy_zone) — compute once
                // per zone book, not once per sell.
                let fees = ZoneFees {
                    wheeling: topology.calculate_wheeling_charge(sell_zone_id, buy.zone_id),
                    loss: topology.calculate_loss_factor(sell_zone_id, buy.zone_id),
                };

                // Range query: all sells in this zone with raw price <= upper bound.
                // landed_cost = raw_cost * dynamic_multiplier (all zones) * intra-zone
                // discount (buyer's own zone only). Whenever that combined factor scales
                // cost DOWN (< 1.0), a sell priced above the raw bid can still cross, so
                // the prune bound must widen to bid / factor — otherwise the range query
                // drops it before the discount/multiplier is applied. `priced_candidate`
                // still does the final landed <= bid check, so widening only over-admits.
                let mut factor_raw = i128::from(dynamic_multiplier.raw());
                if sell_zone_id == buy.zone_id {
                    factor_raw =
                        factor_raw * i128::from(intra_zone_mult.raw()) / i128::from(FastPrice::FACTOR);
                }
                let upper_price = if factor_raw <= 0 {
                    // zero / misconfigured factor → cost collapses to 0, every sell crosses.
                    FastPrice::from_raw(i64::MAX)
                } else if factor_raw < i128::from(FastPrice::FACTOR) {
                    // cost scaled down → widen to bid / factor. CEIL (inclusive bound) so a
                    // sell landing exactly at the bid is kept, not dropped one ULP short.
                    let num = i128::from(buy.price.raw()) * i128::from(FastPrice::FACTOR);
                    let raw = (num + factor_raw - 1) / factor_raw;
                    FastPrice::from_raw(i64::try_from(raw).unwrap_or(i64::MAX))
                } else {
                    // factor >= 1.0 → cost only rises, raw bid is already a safe upper bound.
                    buy.price
                };
                for (_, &sell_idx) in book.range(..=(upper_price, i64::MAX, uuid::Uuid::max())) {
                    if let Some(c) = Self::priced_candidate(
                        buy,
                        sell_idx,
                        &sell_orders[sell_idx],
                        topology,
                        fees,
                        dynamic_multiplier,
                        intra_zone_mult,
                    ) {
                        candidates.push(c);
                    }
                }
            }

            // Sort consolidated candidates from all reachable zones by landed cost.
            // Ties broken by raw sell price, then arrival, then id so the chosen
            // fill order is deterministic (price-time priority) and not subject to
            // zone-book traversal order.
            candidates.sort_unstable_by(|a, b| {
                a.landed
                    .cmp(&b.landed)
                    .then_with(|| a.sort_price.cmp(&b.sort_price))
                    .then_with(|| a.sort_time.cmp(&b.sort_time))
                    .then_with(|| a.sort_id.cmp(&b.sort_id))
            });

            // FOK handling — all-or-nothing. Simulate the real drain loop below
            // (sells under MIN_TRADE_AMOUNT are skipped; the loop stops once the
            // buy's remainder drops below MIN) and only proceed if the order fills
            // completely. A plain `sum >= size` check is not enough: granular fills
            // can leave a sub-MIN dust remainder that the real loop's MIN break
            // strands, which the immediate sweep then cancels — a partial fill that
            // violates fill-or-kill. If the simulation leaves anything unfilled,
            // kill the order (skip matching; the sweep reports it for cancellation).
            if buy.time_in_force == TimeInForce::Fok {
                let mut need = buy.remaining_amount();
                for c in candidates.iter() {
                    let avail = sell_orders[c.sell_idx].remaining_amount();
                    if avail < MIN_TRADE_AMOUNT {
                        continue;
                    }
                    need -= need.min(avail);
                    if need < MIN_TRADE_AMOUNT {
                        break;
                    }
                }
                if need > Decimal::ZERO {
                    continue;
                }
            }

            // drain(..) yields owned Candidates and leaves the buffer empty but
            // allocated for the next buy. On an early `break`, drain's Drop still
            // clears the remaining range — `clear()` next iteration is idempotent.
            for c in candidates.drain(..) {
                if buy.remaining_amount() < MIN_TRADE_AMOUNT {
                    break;
                }
                let Candidate {
                    sell_idx,
                    landed: landed_cost_fp,
                    wheeling: wheeling_fp,
                    loss: loss_fp,
                    loss_cost: loss_cost_fp,
                    ..
                } = c;

                let sell = &mut sell_orders[sell_idx];
                if sell.remaining_amount() < MIN_TRADE_AMOUNT {
                    continue;
                }

                let match_amount = buy.remaining_amount().min(sell.remaining_amount());
                let buy_meta_idx = buy.metadata_index;
                let sell_meta_idx = sell.metadata_index;

                // No match-consolidation step: each sell lives in exactly one zone
                // book and so appears at most once in `candidates`, and `buy` is
                // fixed across this loop — so a given (buy, sell) pair can produce
                // at most one result per cycle. There is never an adjacent same-pair
                // result to merge into; emit each match directly.
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

        // 3. Immediate-TIF sweep. An immediate order (IOC or FOK) fills only what
        // crosses in THIS pass; its leftover must not rest in the book for a later
        // cycle. This covers two cases: an IOC that partially filled, and an FOK
        // whose all-or-nothing liquidity check failed above (the `continue` at the
        // FOK branch) and so filled nothing — without this, that FOK would rest at
        // its bid ceiling (e.g. a market buy's 1,000,000 ceiling) and cross any
        // later ask up to that ceiling. Any immediate order (buy or sell) with ANY
        // unfilled energy is reported for cancellation — including a
        // sub-MIN_TRADE_AMOUNT dust remainder, which can never trade again (below
        // the min) and so would otherwise rest as a permanent PartiallyFilled
        // order. Expired orders are skipped here: they are reaped to `Expired` by
        // the ReaperWorker (`OrderRepository::expire_stale_orders`), so cancelling
        // them too would race that terminal status.
        for o in buy_orders.iter().chain(sell_orders.iter()) {
            if o.time_in_force.is_immediate()
                && !o.is_expired(now_ns)
                && o.remaining_amount() > Decimal::ZERO
            {
                stats.ioc_cancellations.push(o.id);
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

    /// CDA price priority on the BUY side.
    ///
    /// Two buyers compete for a single cheap sell that can fill only one of
    /// them. Buyer HIGH bids more but arrived later; buyer LOW bids less but
    /// arrived earlier. Buys are handed to the engine in FIFO order `[LOW, HIGH]`
    /// (the order the orchestrator would pass them).
    ///
    /// Textbook CDA awards scarce liquidity to the most aggressive (highest) bid
    /// first. `match_cycle` enforces this internally (it re-sorts buys by
    /// price-desc, then time — see step 0), so HIGH wins despite arriving later.
    /// Guards that buy-side priority is the engine's responsibility, not the
    /// caller's.
    #[test]
    fn cda_buy_price_priority_highest_bid_wins() {
        let seller = Uuid::new_v4();
        let buyer_low = Uuid::new_v4();
        let buyer_high = Uuid::new_v4();

        // One sell, only enough to fill ONE buyer.
        let mut sells = vec![FastOrder {
            id: Uuid::new_v4(),
            user_id: seller,
            price: FastPrice::from(dec!(0.5)),
            energy_amount: dec!(50.0),
            filled_amount: dec!(0.0),
            zone_id: Some(1),
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

        // Buys handed to the engine in FIFO order (what the orchestrator does):
        // LOW bid (earlier) first, HIGH bid (later) second.
        let mut buys = vec![
            FastOrder {
                id: buyer_low,
                user_id: buyer_low,
                price: FastPrice::from(dec!(0.60)),
                energy_amount: dec!(50.0),
                filled_amount: dec!(0.0),
                zone_id: Some(1),
                created_at_ns: 100,
                expires_at_ns: None,
                time_in_force: TimeInForce::Gtc,
                metadata_index: 0,
            },
            FastOrder {
                id: buyer_high,
                user_id: buyer_high,
                price: FastPrice::from(dec!(1.00)),
                energy_amount: dec!(50.0),
                filled_amount: dec!(0.0),
                zone_id: Some(1),
                created_at_ns: 200,
                expires_at_ns: None,
                time_in_force: TimeInForce::Gtc,
                metadata_index: 1,
            },
        ];
        let buy_meta = vec![
            OrderMetadata {
                epoch_id: None,
                order_pda: None,
                session_token: None,
            },
            OrderMetadata {
                epoch_id: None,
                order_pda: None,
                session_token: None,
            },
        ];

        let topo = MockTopology;
        let (matches, _stats) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &buy_meta,
            &sell_meta,
            &topo,
            FastPrice::from(dec!(1.0)),
            300,
        );

        assert_eq!(matches.len(), 1, "only one buyer can be filled");
        assert_eq!(
            matches[0].buyer_id, buyer_high,
            "CDA price priority: the higher bid (1.00) must win over the \
             earlier lower bid (0.60), even though it arrived later"
        );
    }

    // ── Edge-case coverage ───────────────────────────────────────────────────

    /// Zero wheeling, unit loss factor → landed cost == sell price. Lets the
    /// discount / tiebreak / FOK tests reason about exact numbers.
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
        tif: TimeInForce,
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
            time_in_force: tif,
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

    fn unit() -> FastPrice {
        FastPrice::from(dec!(1.0))
    }

    /// Loss factor below 1.0 (physically a gain) — used to prove the engine floors
    /// the loss component at 0 instead of crediting a negative cost.
    struct SubUnitLossTopology;
    impl TopologySnapshot for SubUnitLossTopology {
        fn can_accommodate_flow(&self, _f: Option<i32>, _t: Option<i32>, _a: Decimal) -> bool {
            true
        }
        fn calculate_wheeling_charge(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice {
            FastPrice::from(dec!(0))
        }
        fn calculate_loss_factor(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice {
            FastPrice::from(dec!(0.9))
        }
    }

    #[test]
    fn sub_unit_loss_factor_does_not_credit_negative_cost() {
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        // Cross-zone (no intra discount) isolates the loss term. sell=1.0, bid=1.5.
        let mut buys = vec![order(buyer, dec!(1.5), dec!(10.0), 1, 100, TimeInForce::Gtc, 0)];
        let mut sells = vec![order(seller, dec!(1.0), dec!(10.0), 2, 100, TimeInForce::Gtc, 0)];
        let (matches, _) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &SubUnitLossTopology,
            unit(),
            200,
        );
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].loss_cost, dec!(0), "loss floored at 0, not negative");
        assert_eq!(
            matches[0].match_price,
            dec!(1.0),
            "no phantom discount from sub-unit loss"
        );
    }

    #[test]
    fn self_trade_is_skipped() {
        let u = Uuid::new_v4();
        let mut buys = vec![order(u, dec!(1.0), dec!(10.0), 1, 100, TimeInForce::Gtc, 0)];
        let mut sells = vec![order(u, dec!(0.5), dec!(10.0), 1, 100, TimeInForce::Gtc, 0)];
        let (matches, stats) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            unit(),
            200,
        );
        assert!(matches.is_empty(), "same user must not trade with itself");
        assert_eq!(stats.matches_created, 0);
    }

    #[test]
    fn expired_sell_is_skipped() {
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        let mut buys = vec![order(buyer, dec!(1.0), dec!(10.0), 1, 100, TimeInForce::Gtc, 0)];
        let mut sells = vec![order(seller, dec!(0.5), dec!(10.0), 1, 100, TimeInForce::Gtc, 0)];
        sells[0].expires_at_ns = Some(150); // expires before now_ns = 200
        let (matches, _) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            unit(),
            200,
        );
        assert!(matches.is_empty(), "expired sell must not match");
    }

    #[test]
    fn intra_zone_discount_applied() {
        // Same zone → 5% discount. NoFee landed = sell price 1.0 → discounted 0.95.
        // Bid 1.10 is above raw price so the range query admits the sell, and the
        // recorded clearing price is the discounted 0.95.
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        let mut buys = vec![order(buyer, dec!(1.10), dec!(10.0), 1, 100, TimeInForce::Gtc, 0)];
        let mut sells = vec![order(seller, dec!(1.0), dec!(10.0), 1, 100, TimeInForce::Gtc, 0)];
        let (matches, _) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            unit(),
            200,
        );
        assert_eq!(matches.len(), 1, "same-zone trade should clear");
        assert_eq!(
            matches[0].match_price,
            dec!(0.95),
            "intra-zone price = 1.0 * (1 - 0.05)"
        );
    }

    /// The zone-book range query is discount-aware for the buyer's own zone: a
    /// local sell priced ABOVE the bid still crosses when the 5% discount pulls
    /// its landed cost under the bid. Here sell=1.0, bid=0.96, discounted
    /// landed=0.95 ≤ 0.96 → it must match and clear at 0.95.
    #[test]
    fn intra_zone_discount_rescues_above_bid_sell() {
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        let mut buys = vec![order(buyer, dec!(0.96), dec!(10.0), 1, 100, TimeInForce::Gtc, 0)];
        let mut sells = vec![order(seller, dec!(1.0), dec!(10.0), 1, 100, TimeInForce::Gtc, 0)];
        let (matches, _) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            unit(),
            200,
        );
        assert_eq!(matches.len(), 1, "discount must rescue the above-bid local sell");
        assert_eq!(matches[0].match_price, dec!(0.95));
    }

    /// Cross-zone has NO discount, so an above-bid sell in another zone stays
    /// excluded (wheeling/loss only add cost). Guards the widening from leaking
    /// across zones.
    #[test]
    fn cross_zone_above_bid_sell_still_excluded() {
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        let mut buys = vec![order(buyer, dec!(0.96), dec!(10.0), 1, 100, TimeInForce::Gtc, 0)];
        let mut sells = vec![order(seller, dec!(1.0), dec!(10.0), 2, 100, TimeInForce::Gtc, 0)];
        let (matches, _) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            unit(),
            200,
        );
        assert!(matches.is_empty(), "no discount across zones → no rescue");
    }

    /// Regression for the CEIL-division fix at the intra-zone range bound.
    /// Raw-constructed so `bid / (1 - discount)` is INEXACT: bid = 0.950000001,
    /// sell = 1.000000002 (FACTOR = 1e9, discount mult = 0.95).
    /// landed = floor(1.000000002 * 0.95) = 0.950000001 == bid → it crosses.
    /// floor(bid/0.95) = 1.000000001 would exclude the sell from the range;
    /// ceil(bid/0.95) = 1.000000002 keeps it. With the ceil fix it must match.
    #[test]
    fn intra_zone_range_bound_ceils_at_inexact_boundary() {
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        let mut buys = vec![FastOrder {
            id: Uuid::new_v4(),
            user_id: buyer,
            price: FastPrice::from_raw(950_000_001),
            energy_amount: dec!(10.0),
            filled_amount: dec!(0.0),
            zone_id: Some(1),
            created_at_ns: 100,
            expires_at_ns: None,
            time_in_force: TimeInForce::Gtc,
            metadata_index: 0,
        }];
        let mut sells = vec![FastOrder {
            id: Uuid::new_v4(),
            user_id: seller,
            price: FastPrice::from_raw(1_000_000_002),
            energy_amount: dec!(10.0),
            filled_amount: dec!(0.0),
            zone_id: Some(1),
            created_at_ns: 100,
            expires_at_ns: None,
            time_in_force: TimeInForce::Gtc,
            metadata_index: 0,
        }];
        let (matches, _) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            unit(),
            200,
        );
        assert_eq!(
            matches.len(),
            1,
            "ceil bound must keep the exact-boundary intra-zone sell"
        );
    }

    /// Overflow must SATURATE, not wrap. Sell price 5e9 (in range of bid 5e9) times
    /// a 5e9 dynamic multiplier overflows i64 in the landed-cost product. With the
    /// old `unchecked_mul` this wrapped negative and passed `landed <= bid`, booking
    /// a spurious match at a garbage price. `checked_mul` clamps to i64::MAX → no cross.
    #[test]
    fn landed_cost_overflow_does_not_wrap_into_false_match() {
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        let big = FastPrice::from_raw(5_000_000_000_000_000_000); // 5e9
        let mut buys = vec![FastOrder {
            id: Uuid::new_v4(),
            user_id: buyer,
            price: big,
            energy_amount: dec!(10.0),
            filled_amount: dec!(0.0),
            zone_id: Some(1),
            created_at_ns: 100,
            expires_at_ns: None,
            time_in_force: TimeInForce::Gtc,
            metadata_index: 0,
        }];
        let mut sells = vec![FastOrder {
            id: Uuid::new_v4(),
            user_id: seller,
            price: big,
            energy_amount: dec!(10.0),
            filled_amount: dec!(0.0),
            zone_id: Some(2), // cross-zone → no discount widening, plain bid bound
            created_at_ns: 100,
            expires_at_ns: None,
            time_in_force: TimeInForce::Gtc,
            metadata_index: 0,
        }];
        let (matches, _) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            big, // adversarial dynamic_multiplier
            200,
        );
        assert!(
            matches.is_empty(),
            "overflowed landed cost must clamp high, not wrap into a false match"
        );
    }

    /// A dynamic_multiplier below 1.0 lowers landed cost for ALL zones, so the
    /// range bound must widen by 1/multiplier — otherwise an above-bid sell that
    /// crosses post-multiplier is pruned. Cross-zone (no intra discount) isolates
    /// the multiplier's effect: sell 1.5, bid 0.96, multiplier 0.5 → landed 0.75.
    #[test]
    fn dynamic_multiplier_below_one_widens_bound() {
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        let mut buys = vec![order(buyer, dec!(0.96), dec!(10.0), 1, 100, TimeInForce::Gtc, 0)];
        let mut sells = vec![order(seller, dec!(1.5), dec!(10.0), 2, 100, TimeInForce::Gtc, 0)];
        let (matches, _) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            FastPrice::from(dec!(0.5)),
            200,
        );
        assert_eq!(matches.len(), 1, "multiplier <1 must widen the prune bound");
        assert_eq!(matches[0].match_price, dec!(0.75), "1.5 * 0.5");
    }

    #[test]
    fn fok_kills_when_insufficient_liquidity() {
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        // FOK wants 100, only 50 available → nothing fills.
        let mut buys = vec![order(buyer, dec!(1.0), dec!(100.0), 1, 100, TimeInForce::Fok, 0)];
        let mut sells = vec![order(seller, dec!(0.5), dec!(50.0), 1, 100, TimeInForce::Gtc, 0)];
        let (matches, _) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            unit(),
            200,
        );
        assert!(matches.is_empty(), "FOK must be all-or-nothing");
        assert_eq!(buys[0].filled_amount, dec!(0.0), "buy untouched after kill");
    }

    #[test]
    fn fok_fills_when_sufficient_liquidity() {
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        let mut buys = vec![order(buyer, dec!(1.0), dec!(50.0), 1, 100, TimeInForce::Fok, 0)];
        let mut sells = vec![order(seller, dec!(0.5), dec!(50.0), 1, 100, TimeInForce::Gtc, 0)];
        let (matches, stats) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            unit(),
            200,
        );
        assert_eq!(matches.len(), 1);
        assert_eq!(stats.total_volume, dec!(50.0));
    }

    #[test]
    fn partial_fill_carries_across_cycles() {
        let buyer = Uuid::new_v4();
        let mut buys = vec![order(buyer, dec!(1.0), dec!(100.0), 1, 100, TimeInForce::Gtc, 0)];
        let bmeta = meta(1);

        // Cycle 1: only 40 of liquidity.
        let mut sells1 = vec![order(
            Uuid::new_v4(),
            dec!(0.5),
            dec!(40.0),
            1,
            100,
            TimeInForce::Gtc,
            0,
        )];
        let (m1, _) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells1,
            &bmeta,
            &meta(1),
            &NoFeeTopology,
            unit(),
            200,
        );
        assert_eq!(m1.len(), 1);
        assert_eq!(buys[0].filled_amount, dec!(40.0));
        assert_eq!(buys[0].remaining_amount(), dec!(60.0));

        // Cycle 2: same buy (filled state carried), new 60 of liquidity → full.
        let mut sells2 = vec![order(
            Uuid::new_v4(),
            dec!(0.5),
            dec!(60.0),
            1,
            100,
            TimeInForce::Gtc,
            0,
        )];
        let (m2, _) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells2,
            &bmeta,
            &meta(1),
            &NoFeeTopology,
            unit(),
            300,
        );
        assert_eq!(m2.len(), 1);
        assert_eq!(buys[0].filled_amount, dec!(100.0));
        assert!(buys[0].remaining_amount() < MIN_TRADE_AMOUNT);
    }

    // ── IOC (Immediate-or-Cancel) sweep ──────────────────────────────────────

    /// IOC buy that only partially fills: the engine reports its id so the
    /// orchestrator cancels the remainder instead of letting it rest in the book.
    #[test]
    fn ioc_partial_fill_remainder_is_reported() {
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        let mut buys = vec![order(buyer, dec!(1.0), dec!(100.0), 1, 100, TimeInForce::Ioc, 0)];
        let mut sells = vec![order(seller, dec!(0.5), dec!(40.0), 1, 100, TimeInForce::Gtc, 0)];
        let (matches, stats) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            unit(),
            200,
        );
        assert_eq!(matches.len(), 1, "IOC fills what it can");
        assert_eq!(buys[0].filled_amount, dec!(40.0));
        assert_eq!(
            stats.ioc_cancellations,
            vec![buys[0].id],
            "60 kWh remainder must be reported for cancellation"
        );
    }

    /// IOC order with no counterparty this cycle is reported in full — the
    /// zone_books-empty path must still reach the sweep.
    #[test]
    fn ioc_with_no_liquidity_is_reported() {
        let buyer = Uuid::new_v4();
        let mut buys = vec![order(buyer, dec!(1.0), dec!(10.0), 1, 100, TimeInForce::Ioc, 0)];
        let mut sells: Vec<FastOrder> = vec![];
        let (matches, stats) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(0),
            &NoFeeTopology,
            unit(),
            200,
        );
        assert!(matches.is_empty());
        assert_eq!(
            stats.ioc_cancellations,
            vec![buys[0].id],
            "unfilled IOC with no sells must be cancelled, not rested"
        );
    }

    /// A fully-filled IOC has no remainder, so it is NOT reported.
    #[test]
    fn ioc_fully_filled_is_not_reported() {
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        let mut buys = vec![order(buyer, dec!(1.0), dec!(50.0), 1, 100, TimeInForce::Ioc, 0)];
        let mut sells = vec![order(seller, dec!(0.5), dec!(50.0), 1, 100, TimeInForce::Gtc, 0)];
        let (matches, stats) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            unit(),
            200,
        );
        assert_eq!(matches.len(), 1);
        assert!(
            stats.ioc_cancellations.is_empty(),
            "fully-filled IOC has nothing to cancel"
        );
    }

    /// A GTC order's unfilled remainder rests in the book — it must NOT be swept
    /// as an IOC cancellation.
    #[test]
    fn gtc_remainder_is_not_swept() {
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        let mut buys = vec![order(buyer, dec!(1.0), dec!(100.0), 1, 100, TimeInForce::Gtc, 0)];
        let mut sells = vec![order(seller, dec!(0.5), dec!(40.0), 1, 100, TimeInForce::Gtc, 0)];
        let (_m, stats) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            unit(),
            200,
        );
        assert!(
            stats.ioc_cancellations.is_empty(),
            "GTC remainder rests in the book, not cancelled"
        );
    }

    /// An FOK buy whose all-or-nothing liquidity check fails fills nothing and
    /// must be swept (cancelled), not left resting at its (possibly ceiling) bid.
    /// Regression: the sweep previously matched only IOC, so a market+FOK buy rested
    /// at the 1,000,000 ceiling and crossed later asks.
    #[test]
    fn fok_insufficient_liquidity_is_reported() {
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        // Buyer wants 100 kWh FOK; only 40 kWh resting → all-or-nothing fails.
        let mut buys = vec![order(buyer, dec!(1.0), dec!(100.0), 1, 100, TimeInForce::Fok, 0)];
        let mut sells = vec![order(seller, dec!(0.5), dec!(40.0), 1, 100, TimeInForce::Gtc, 0)];
        let (matches, stats) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            unit(),
            200,
        );
        assert!(matches.is_empty(), "FOK fills nothing when it can't fill fully");
        assert_eq!(buys[0].filled_amount, dec!(0.0));
        assert_eq!(
            stats.ioc_cancellations,
            vec![buys[0].id],
            "unfilled FOK must be swept, not left resting"
        );
    }

    /// An FOK with ample total liquidity that nonetheless can't fill completely
    /// (granular fill strands sub-MIN dust) must be killed whole, not partially
    /// filled then cancelled. buy 10.0005 vs sells 10.0 + 1.0: the first sell
    /// fills 10.0, leaving 0.0005 (< MIN) which the drain loop strands — so the
    /// order must fill NOTHING and be swept.
    #[test]
    fn fok_strands_dust_is_killed_whole() {
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        let mut buys = vec![order(buyer, dec!(1.0), dec!(10.0005), 1, 100, TimeInForce::Fok, 0)];
        let mut sells = vec![
            order(seller, dec!(0.5), dec!(10.0), 1, 100, TimeInForce::Gtc, 0),
            order(seller, dec!(0.5), dec!(1.0), 1, 101, TimeInForce::Gtc, 0),
        ];
        let (matches, stats) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(2),
            &NoFeeTopology,
            unit(),
            200,
        );
        assert!(matches.is_empty(), "FOK that can't fully fill must not partially fill");
        assert_eq!(buys[0].filled_amount, dec!(0.0));
        assert_eq!(
            stats.ioc_cancellations,
            vec![buys[0].id],
            "killed FOK must be swept for cancellation"
        );
    }

    /// A fully-fillable FOK has no remainder, so it is NOT reported.
    #[test]
    fn fok_fully_filled_is_not_reported() {
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        let mut buys = vec![order(buyer, dec!(1.0), dec!(50.0), 1, 100, TimeInForce::Fok, 0)];
        let mut sells = vec![order(seller, dec!(0.5), dec!(50.0), 1, 100, TimeInForce::Gtc, 0)];
        let (matches, stats) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            unit(),
            200,
        );
        assert_eq!(matches.len(), 1);
        assert!(
            stats.ioc_cancellations.is_empty(),
            "fully-filled FOK has nothing to cancel"
        );
    }

    /// A sub-MIN_TRADE_AMOUNT IOC dust remainder is reported: it can never trade
    /// again (below the min), so for IOC it must be cancelled, not left resting.
    /// buy 10.0005 fills 10.0 against the sell → 0.0005 dust (< MIN 0.001).
    #[test]
    fn ioc_sub_min_dust_remainder_is_reported() {
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        // Cross-zone (no intra discount) so landed cost = sell price, clean cross.
        let mut buys = vec![order(buyer, dec!(1.0), dec!(10.0005), 1, 100, TimeInForce::Ioc, 0)];
        let mut sells = vec![order(seller, dec!(0.5), dec!(10.0), 2, 100, TimeInForce::Gtc, 0)];
        let (matches, stats) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            unit(),
            200,
        );
        assert_eq!(matches.len(), 1, "fills the 10.0 that crossed");
        assert!(buys[0].remaining_amount() < MIN_TRADE_AMOUNT, "leftover is dust");
        assert_eq!(
            stats.ioc_cancellations,
            vec![buys[0].id],
            "sub-MIN IOC dust must still be reported for cancellation"
        );
    }

    /// The sweep is symmetric: an IOC SELL remainder is reported too, not just buys.
    #[test]
    fn ioc_sell_remainder_is_reported() {
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        let mut buys = vec![order(buyer, dec!(1.0), dec!(40.0), 1, 100, TimeInForce::Gtc, 0)];
        let mut sells = vec![order(seller, dec!(0.5), dec!(100.0), 1, 100, TimeInForce::Ioc, 0)];
        let (matches, stats) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            unit(),
            200,
        );
        assert_eq!(matches.len(), 1);
        assert_eq!(
            stats.ioc_cancellations,
            vec![sells[0].id],
            "IOC sell with 60 kWh left must be reported"
        );
    }

    #[test]
    fn multi_zone_equal_cost_tiebreak_is_deterministic() {
        // Two sells, different zones, identical price; buyer in a third zone so
        // neither gets the intra-zone discount → equal landed cost. Tiebreak
        // resolves to the smaller order id, stably across repeated runs.
        let buyer = Uuid::new_v4();
        let s1 = order(Uuid::new_v4(), dec!(0.5), dec!(30.0), 1, 100, TimeInForce::Gtc, 0);
        let s2 = order(Uuid::new_v4(), dec!(0.5), dec!(30.0), 2, 100, TimeInForce::Gtc, 1);
        let expected_winner = if s1.id < s2.id { s1.user_id } else { s2.user_id };

        for run in 0..5 {
            let mut buys = vec![order(buyer, dec!(1.0), dec!(30.0), 3, 100, TimeInForce::Gtc, 0)];
            let mut sells = vec![s1.clone(), s2.clone()];
            let (matches, _) = MatchingEngine::match_cycle(
                &mut buys,
                &mut sells,
                &meta(1),
                &meta(2),
                &NoFeeTopology,
                unit(),
                200,
            );
            assert_eq!(matches.len(), 1, "buyer only fills 30 = one sell");
            assert_eq!(
                matches[0].seller_id, expected_winner,
                "run {run}: tiebreak must pick the same (lowest-id) sell every time"
            );
        }
    }
}
