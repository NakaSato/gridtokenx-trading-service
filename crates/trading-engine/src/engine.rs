//! Pure synchronous CDA matching engine.

use crate::types::{CycleStats, FastOrder, MatchResult, OrderMetadata};
use rust_decimal::Decimal;
use trading_core::fast_price::FastPrice;
use trading_core::types::TimeInForce;

/// Minimum amount of energy to allow a trade.
pub const MIN_TRADE_AMOUNT: Decimal = rust_decimal_macros::dec!(0.001);

/// One fill step's decision, shared by the FOK pre-check simulation and the real
/// drain loop so the per-sell fill rule cannot drift between them. Given the
/// buy's remaining `need` and a candidate sell's `avail`, returns the amount to
/// match, or `None` if the sell is below the tradeable minimum (skip it).
/// Callers independently stop once `need` itself drops below `MIN_TRADE_AMOUNT`.
#[inline]
fn fill_take(need: Decimal, avail: Decimal) -> Option<Decimal> {
    if avail < MIN_TRADE_AMOUNT {
        None
    } else {
        Some(need.min(avail))
    }
}

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

use std::collections::{BTreeMap, BinaryHeap};

/// The pure matching engine.
pub struct MatchingEngine;

/// Zone-segmented sell books: `ZoneId -> (price, created_at, id) -> sell index`.
/// A `BTreeMap` outer for deterministic zone iteration, `BTreeMap` inner for
/// price-time priority within a zone.
type ZoneBooks = BTreeMap<Option<i32>, BTreeMap<(FastPrice, i64, uuid::Uuid), usize>>;

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

/// One reachable zone book's head in the k-way merge: a priced candidate plus the
/// index of the cursor it came from (so that cursor can be advanced after a pop).
/// Ordered so a `BinaryHeap` (a max-heap) yields the CHEAPEST candidate first —
/// same key as the exhaustive sort (landed, raw ask, arrival, id), so the merge
/// reproduces that total order lazily. Sell ids are unique, so the key is a strict
/// total order with no ties.
struct HeapHead {
    cand: Candidate,
    cursor: usize,
}
impl HeapHead {
    fn key(&self) -> (FastPrice, FastPrice, i64, uuid::Uuid) {
        (
            self.cand.landed,
            self.cand.sort_price,
            self.cand.sort_time,
            self.cand.sort_id,
        )
    }
}
impl Ord for HeapHead {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reversed: the smallest key must be the heap's max so `pop` returns it.
        other.key().cmp(&self.key())
    }
}
impl PartialOrd for HeapHead {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for HeapHead {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}
impl Eq for HeapHead {}

impl MatchingEngine {
    /// Price one sell against a buy. Returns a [`Candidate`] when the trade is
    /// viable: not a self-trade, the grid can carry the flow, and the landed
    /// cost crosses the bid. All raw fixed-point arithmetic is saturating /
    /// checked — overflow clamps to `i64::MAX` rather than wrapping, so an
    /// adversarial price can never wrap the accumulator into a false (negative)
    /// cross.
    #[allow(clippy::too_many_arguments)]
    fn priced_candidate(
        buy: &FastOrder,
        buy_remaining: Decimal,
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

        // Settlement invariant (option-a, Case 1): the seller is always paid their
        // full ask, funded from the buyer's escrow, which the buyer posts at their
        // bid. So the bid MUST cover the ask. A match may clear below the ask only via
        // the platform-subsidised intra-zone discount (or a sub-unit dynamic
        // multiplier), which reduces the buyer's payment WITHIN a crossing match — it
        // must never rescue a sell whose ask exceeds the bid. Such a sell can't settle
        // without an on-chain platform top-up (buyer escrow < ask, and the on-chain
        // guard rejects price < ask), so never match it here.
        if sell.price > buy.price {
            return None;
        }
        let sell_zone_id = sell.zone_id;

        // Strict topology check for the actual tradeable amount. `buy_remaining` is
        // passed in (not read live off `buy`) so a rebuild after a partial fill uses
        // the buy's start-of-cycle size — pricing a sell identically to a single pass.
        let potential_amount = buy_remaining.min(sell.remaining_amount());
        if !topology.can_accommodate_flow(sell_zone_id, buy.zone_id, potential_amount) {
            return None;
        }

        // wheeling/loss are zone-constant — passed in, not recomputed per sell.
        // Floor wheeling at 0, symmetric with the loss floor below: wheeling is a
        // transmission CHARGE (it can only ADD cost), so a negative value — a
        // misconfigured topology encoding a "subsidy" as negative wheeling — must not
        // pull landed cost below the seller's ask and book a negative charge into the
        // settlement ledger. The live GridAwareTopology never emits negative wheeling;
        // this is defensive.
        let wheeling_fp = FastPrice::from_raw(fees.wheeling.raw().max(0));
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

    /// Refill `candidates` for one buy, in landed-cost order, by k-way-merging the
    /// reachable zone books. Each zone book is already `(price,time,id)`-ordered,
    /// which — fees being zone-constant — is landed order for that zone; a min-heap
    /// over the per-zone heads yields the global landed order without materialising
    /// and sorting every crossing sell. Returns `true` (truncated) when `early_stop`
    /// let it stop with resting energy still unscanned — the only case a later
    /// grid-capacity under-fill can be rescued by a full rebuild.
    ///
    /// `need_at_start` is the buy's size at the start of its cycle-iteration; passing
    /// it (rather than reading `buy` live) makes a rebuild after a partial fill price
    /// each sell exactly as a single full pass would.
    #[allow(clippy::too_many_arguments)]
    fn gather_candidates(
        candidates: &mut Vec<Candidate>,
        zone_books: &ZoneBooks,
        sell_orders: &[FastOrder],
        buy: &FastOrder,
        need_at_start: Decimal,
        topology: &dyn TopologySnapshot,
        dynamic_multiplier: FastPrice,
        intra_zone_mult: FastPrice,
        early_stop: bool,
    ) -> bool {
        candidates.clear();

        // Fast path — zero or one zone book (the common deployment). Its `range` is
        // already landed-ordered, so drain it directly with no per-buy cursor Vec or
        // heap allocation. (`match_cycle` is called once per matching tick, so this
        // per-buy allocation saving is on the hot path.)
        if zone_books.len() <= 1 {
            let Some((&sell_zone_id, book)) = zone_books.iter().next() else {
                return false;
            };
            if !topology.can_accommodate_flow(sell_zone_id, buy.zone_id, MIN_TRADE_AMOUNT) {
                return false;
            }
            let fees = ZoneFees {
                wheeling: topology.calculate_wheeling_charge(sell_zone_id, buy.zone_id),
                loss: topology.calculate_loss_factor(sell_zone_id, buy.zone_id),
            };
            let mut gathered = Decimal::ZERO;
            for (_, &sell_idx) in book.range(..=(buy.price, i64::MAX, uuid::Uuid::max())) {
                if let Some(c) = Self::priced_candidate(
                    buy,
                    need_at_start,
                    sell_idx,
                    &sell_orders[sell_idx],
                    topology,
                    fees,
                    dynamic_multiplier,
                    intra_zone_mult,
                ) {
                    gathered += sell_orders[sell_idx].remaining_amount();
                    candidates.push(c);
                    if early_stop && gathered >= need_at_start {
                        return true;
                    }
                }
            }
            return false;
        }

        // Multi-zone: k-way merge. One cursor per reachable zone — a landed-ordered
        // iterator over that zone's crossing sells (raw ask <= bid) plus its fees.
        struct ZoneCursor<'a> {
            fees: ZoneFees,
            range: std::collections::btree_map::Range<'a, (FastPrice, i64, uuid::Uuid), usize>,
        }
        let mut cursors: Vec<ZoneCursor> = Vec::new();
        for (&sell_zone_id, book) in zone_books {
            // Topology pre-filter: can the grid move energy from this zone to the
            // buyer's zone at all? Checked with MIN_TRADE_AMOUNT as a baseline.
            if !topology.can_accommodate_flow(sell_zone_id, buy.zone_id, MIN_TRADE_AMOUNT) {
                continue;
            }
            // wheeling + loss depend only on (sell_zone, buy_zone) — computed once per
            // zone, not per sell.
            let fees = ZoneFees {
                wheeling: topology.calculate_wheeling_charge(sell_zone_id, buy.zone_id),
                loss: topology.calculate_loss_factor(sell_zone_id, buy.zone_id),
            };
            // Range: all sells in this zone with raw ask <= the buyer's bid.
            // `priced_candidate` rejects any sell whose ask exceeds the bid, so the
            // bid is the exact, tight upper bound (the discount / dynamic multiplier
            // only ever reduce the buyer's payment within a crossing match, never
            // rescue an above-bid sell).
            cursors.push(ZoneCursor {
                fees,
                range: book.range(..=(buy.price, i64::MAX, uuid::Uuid::max())),
            });
        }

        // Advance one cursor to its next priced (crossing) candidate, skipping sells
        // that price out (self-trade, capacity, landed > bid).
        let next_priced = |cur: &mut ZoneCursor| -> Option<Candidate> {
            for (_, &sell_idx) in cur.range.by_ref() {
                if let Some(c) = Self::priced_candidate(
                    buy,
                    need_at_start,
                    sell_idx,
                    &sell_orders[sell_idx],
                    topology,
                    cur.fees,
                    dynamic_multiplier,
                    intra_zone_mult,
                ) {
                    return Some(c);
                }
            }
            None
        };

        // Single reachable zone (the common case): its cursor is already in landed
        // order, so drain it directly — no heap, no per-candidate `HeapHead` move.
        if cursors.len() == 1 {
            let mut gathered = Decimal::ZERO;
            while let Some(cand) = next_priced(&mut cursors[0]) {
                gathered += sell_orders[cand.sell_idx].remaining_amount();
                candidates.push(cand);
                if early_stop && gathered >= need_at_start {
                    return true;
                }
            }
            return false;
        }

        let mut heap: BinaryHeap<HeapHead> = BinaryHeap::with_capacity(cursors.len());
        for idx in 0..cursors.len() {
            if let Some(cand) = next_priced(&mut cursors[idx]) {
                heap.push(HeapHead { cand, cursor: idx });
            }
        }

        let mut gathered = Decimal::ZERO;
        let mut truncated = false;
        while let Some(head) = heap.pop() {
            let idx = head.cursor;
            gathered += sell_orders[head.cand.sell_idx].remaining_amount();
            candidates.push(head.cand);
            // Landed order == fill order, so once gathered resting energy covers the
            // buy the cheaper sells are all in hand and any dearer ones would never
            // fill — stop (unless a full scan was requested, e.g. for FOK).
            if early_stop && gathered >= need_at_start {
                truncated = true;
                break;
            }
            if let Some(cand) = next_priced(&mut cursors[idx]) {
                heap.push(HeapHead { cand, cursor: idx });
            }
        }
        truncated
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

        // Guard a non-positive dynamic_multiplier. `priced_candidate` multiplies the
        // landed cost by this factor with `checked_mul`, which does NOT overflow on
        // zero/negative — it just returns 0 (or a negative), collapsing every landed
        // cost so all sells cross and settle at match_price 0 (sellers deliver energy
        // for zero payment). A multiplier <= 0 is only ever a misconfigured oracle/
        // config, never a real price signal, so fall back to 1.0 (no adjustment)
        // rather than zeroing out clearing prices. The caller is expected to pass a
        // positive multiplier; this is the last line of defence in the pure engine.
        let dynamic_multiplier = if dynamic_multiplier.raw() <= 0 {
            FastPrice::from_raw(FastPrice::FACTOR)
        } else {
            dynamic_multiplier
        };

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
        let mut zone_books: ZoneBooks =
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

        // Per-cycle grid-flow ledger: running committed energy per (sell_zone,
        // buy_zone) link this cycle. `can_accommodate_flow` is a per-trade check
        // against an immutable snapshot, so without this, N buyers each passing the
        // per-trade check independently can oversell one link N-fold. Every committed
        // match adds to its link's total; each fill is re-checked against the running
        // total so cumulative flow can never exceed the link rating.
        let mut committed_flow: BTreeMap<(Option<i32>, Option<i32>), Decimal> = BTreeMap::new();
        for buy in buy_orders.iter_mut() {
            if buy.remaining_amount() < MIN_TRADE_AMOUNT || buy.is_expired(now_ns) {
                continue;
            }

            // The buy's fillable size, fixed for this cycle-iteration. Passed to
            // `gather_candidates` on every (re)build so a rebuild after a partial fill
            // prices each sell exactly as a single full pass would, and the target the
            // k-way merge gathers up to before it stops.
            let need_at_start = buy.remaining_amount();

            // FOK never early-stops — its all-or-nothing check must weigh every
            // reachable sell. Everything else starts optimistic (early-stop on); a
            // grid-capacity skip that leaves the buy under-filled flips `full_scan`
            // and rebuilds once — correctness first, the fast path for the common
            // no-skip case.
            let mut full_scan = buy.time_in_force == TimeInForce::Fok;

            loop {
                // K-way-merge the reachable zone books into `candidates` in landed
                // order (no separate sort). `truncated` = the merge stopped early with
                // resting energy still unscanned, the only case a capacity-skip
                // under-fill can be rescued by a full rebuild.
                let truncated = Self::gather_candidates(
                    &mut candidates,
                    &zone_books,
                    sell_orders,
                    buy,
                    need_at_start,
                    topology,
                    dynamic_multiplier,
                    intra_zone_mult,
                    !full_scan,
                );

                // FOK handling — all-or-nothing. Simulate the real drain loop below
                // (sells under MIN_TRADE_AMOUNT are skipped; the loop stops once the
                // buy's remainder drops below MIN) and only proceed if the order fills
                // completely. A plain `sum >= size` check is not enough: granular fills
                // can leave a sub-MIN dust remainder that the real loop's MIN break
                // strands, which the immediate sweep then cancels — a partial fill that
                // violates fill-or-kill. The simulation must also mirror the drain's
                // grid-capacity skip (against a COPY of committed_flow so the real ledger
                // is untouched) — otherwise a FOK buy could pass a capacity-blind
                // simulation, then partial-fill in the real drain when a link is full and
                // get its remainder swept: exactly the partial-fill FOK violates. FOK
                // always runs a full scan, so `candidates` holds every reachable sell
                // here. If the simulation leaves anything unfilled, kill the order (skip
                // matching; the sweep reports it for cancellation).
                if buy.time_in_force == TimeInForce::Fok {
                    let mut need = buy.remaining_amount();
                    let mut sim_flow = committed_flow.clone();
                    for c in candidates.iter() {
                        let sim_sell = &sell_orders[c.sell_idx];
                        if let Some(take) = fill_take(need, sim_sell.remaining_amount()) {
                            let key = (sim_sell.zone_id, buy.zone_id);
                            let already = sim_flow.get(&key).copied().unwrap_or(Decimal::ZERO);
                            if !topology.can_accommodate_flow(sim_sell.zone_id, buy.zone_id, already + take)
                            {
                                continue;
                            }
                            *sim_flow.entry(key).or_insert(Decimal::ZERO) += take;
                            need -= take;
                            if need < MIN_TRADE_AMOUNT {
                                break;
                            }
                        }
                    }
                    if need > Decimal::ZERO {
                        break;
                    }
                }

                // drain(..) yields owned Candidates and leaves the buffer empty but
                // allocated for the next buy. On an early `break`, drain's Drop still
                // clears the remaining range — `clear()` next iteration is idempotent.
                for c in candidates.drain(..) {
                    // Stable within this iteration: `buy.filled_amount` is only bumped
                    // at the end, so compute the remaining need once and reuse it for
                    // both the MIN break guard and the fill amount.
                    let need = buy.remaining_amount();
                    if need < MIN_TRADE_AMOUNT {
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
                    let Some(match_amount) = fill_take(need, sell.remaining_amount()) else {
                        continue;
                    };

                    // Grid-capacity gate (see `committed_flow`): re-check the link with the
                    // running committed total for this (sell_zone, buy_zone) pair PLUS this
                    // fill. If the grid can't carry the aggregate, skip this fill — the sell
                    // stays available for another pair / a later cycle rather than being
                    // over-committed now. Conservative: the topology API is boolean (no
                    // remaining-headroom readout), so a fill that would only partially fit is
                    // skipped whole, not trimmed to the remaining capacity.
                    let flow_key = (sell.zone_id, buy.zone_id);
                    let already = committed_flow.get(&flow_key).copied().unwrap_or(Decimal::ZERO);
                    if !topology.can_accommodate_flow(sell.zone_id, buy.zone_id, already + match_amount)
                    {
                        continue;
                    }

                    let buy_meta_idx = buy.metadata_index;
                    let sell_meta_idx = sell.metadata_index;

                    // No match-consolidation step: each sell lives in exactly one zone
                    // book and so appears at most once in `candidates`, and `buy` is
                    // fixed across this loop — so a given (buy, sell) pair can produce
                    // at most one result per cycle. A full-scan rebuild after a partial
                    // fill re-prices only sells still resting in the book (depleted ones
                    // were removed below), so it cannot re-emit an already-filled pair.
                    // There is never an adjacent same-pair result to merge into; emit
                    // each match directly.
                    let buy_meta = &buy_metadata[buy_meta_idx];

                    results.push(MatchResult {
                        buy_order_id: buy.id,
                        sell_order_id: sell.id,
                        buy_metadata_index: buy_meta_idx,
                        sell_metadata_index: sell_meta_idx,
                        match_amount,
                        match_price: landed_cost_fp.to_decimal(),
                        // CDA settles at the seller's ask (see settle_price docs).
                        settle_price: sell.price.to_decimal(),
                        seller_price: sell.price.to_decimal(),
                        buyer_price: buy.price.to_decimal(),
                        total_energy_cost: match_amount * landed_cost_fp.to_decimal(),
                        wheeling_charge: match_amount * wheeling_fp.to_decimal(),
                        loss_factor: loss_fp.to_decimal(),
                        loss_cost: match_amount * loss_cost_fp.to_decimal(),
                        buyer_zone_id: buy.zone_id,
                        seller_zone_id: sell.zone_id,
                        buyer_id: buy.user_id,
                        seller_id: sell.user_id,
                        epoch_id: buy_meta.epoch_id,
                    });

                    buy.filled_amount += match_amount;
                    sell.filled_amount += match_amount;
                    *committed_flow.entry(flow_key).or_insert(Decimal::ZERO) += match_amount;
                    stats.matches_created += 1;
                    stats.total_volume += match_amount;

                    // Cleanup books if sell is depleted
                    if sell.remaining_amount() < MIN_TRADE_AMOUNT {
                        if let Some(book) = zone_books.get_mut(&sell.zone_id) {
                            book.remove(&(sell.price, sell.created_at_ns, sell.id));
                        }
                    }
                }

                // The merge stopped early; if a grid-capacity skip left the buy short
                // there may be farther, unmerged sells that can still fill it. Rebuild
                // this buy once with a full scan. Otherwise (already full, buy filled,
                // or the merge drained without truncation) done.
                if full_scan || buy.remaining_amount() < MIN_TRADE_AMOUNT || !truncated {
                    break;
                }
                full_scan = true;
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
            epoch_id: Uuid::new_v4(),
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
            epoch_id: Uuid::new_v4(),
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
            epoch_id: Uuid::new_v4(),
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
                epoch_id: Uuid::new_v4(),
                order_pda: None,
                session_token: None,
            },
            OrderMetadata {
                epoch_id: Uuid::new_v4(),
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
                epoch_id: Uuid::new_v4(),
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

    /// Negative wheeling (a misconfigured "subsidy") must be floored at 0, symmetric
    /// with the loss floor — it must not pull landed cost below the seller's ask or
    /// book a negative charge. Cross-zone (no discount) isolates the wheeling term.
    struct NegativeWheelingTopology;
    impl TopologySnapshot for NegativeWheelingTopology {
        fn can_accommodate_flow(&self, _f: Option<i32>, _t: Option<i32>, _a: Decimal) -> bool {
            true
        }
        fn calculate_wheeling_charge(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice {
            FastPrice::from(dec!(-0.1))
        }
        fn calculate_loss_factor(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice {
            FastPrice::from(dec!(1.0))
        }
    }

    #[test]
    fn negative_wheeling_is_floored_at_zero() {
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        // ask == bid so the trade crosses; cross-zone so no discount. Without the floor,
        // wheeling -0.1 would drop landed to 0.9 (< ask) and book a -0.1 charge.
        let mut buys = vec![order(buyer, dec!(1.0), dec!(10.0), 1, 100, TimeInForce::Gtc, 0)];
        let mut sells = vec![order(seller, dec!(1.0), dec!(10.0), 2, 100, TimeInForce::Gtc, 0)];
        let (matches, _) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NegativeWheelingTopology,
            unit(),
            200,
        );
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].match_price, dec!(1.0), "wheeling floored at 0 → landed == ask");
        assert_eq!(matches[0].wheeling_charge, dec!(0), "no negative wheeling charge booked");
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

    /// Settlement invariant (option-a, Case 1): the intra-zone discount must NOT
    /// rescue a local sell whose ask exceeds the bid. Here sell=1.0 > bid=0.96 in the
    /// same zone; the discounted landed would be 0.95 ≤ 0.96, but rescuing it would
    /// clear below the seller's ask with the buyer's escrow (posted at 0.96) unable to
    /// pay the 1.0 ask — unsettleable without an on-chain platform top-up. So it must
    /// NOT match. The discount only reduces the buyer's payment within an ask≤bid
    /// crossing match (see intra_zone_discount_applied).
    #[test]
    fn intra_zone_discount_does_not_rescue_above_bid_sell() {
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
        assert!(
            matches.is_empty(),
            "above-bid sell must not be rescued — the seller's ask can't be paid from the buyer's escrow"
        );
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

    /// A sub-unit dynamic_multiplier must NOT rescue an above-bid sell either. Even
    /// though multiplier 0.5 would pull sell=1.5 down to landed 0.75 ≤ bid 0.96, the
    /// seller's 1.5 ask can't be paid from a 0.96 escrow, so the ask>bid guard rejects
    /// it before the multiplier is applied — the multiplier only reduces the buyer's
    /// payment within an ask≤bid crossing match.
    #[test]
    fn dynamic_multiplier_below_one_does_not_rescue_above_bid_sell() {
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
        assert!(
            matches.is_empty(),
            "sub-unit multiplier must not rescue an above-bid sell (unsettleable)"
        );
    }

    /// A zero dynamic_multiplier must NOT collapse clearing prices to 0. Without the
    /// guard, `landed.checked_mul(0)` returns 0 for every sell (checked_mul doesn't
    /// overflow on zero), so all sells cross and settle at match_price 0 — sellers
    /// deliver energy for zero payment. The guard falls back to 1.0, so the trade
    /// clears at the real ask.
    #[test]
    fn zero_dynamic_multiplier_does_not_zero_clearing_price() {
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        // Cross-zone (no intra discount) so landed cost == sell price at multiplier 1.0.
        let mut buys = vec![order(buyer, dec!(1.0), dec!(10.0), 1, 100, TimeInForce::Gtc, 0)];
        let mut sells = vec![order(seller, dec!(0.5), dec!(10.0), 2, 100, TimeInForce::Gtc, 0)];
        let (matches, _) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            FastPrice::from(dec!(0)), // adversarial: zero multiplier
            200,
        );
        assert_eq!(matches.len(), 1, "trade must still clear");
        assert_eq!(
            matches[0].match_price,
            dec!(0.5),
            "zero multiplier must fall back to 1.0, clearing at the real ask — not 0"
        );
        assert!(
            matches[0].total_energy_cost > dec!(0),
            "seller must be paid, not zeroed out"
        );
    }

    /// A negative dynamic_multiplier is likewise clamped to 1.0 rather than producing
    /// negative clearing prices.
    #[test]
    fn negative_dynamic_multiplier_is_clamped() {
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        let mut buys = vec![order(buyer, dec!(1.0), dec!(10.0), 1, 100, TimeInForce::Gtc, 0)];
        let mut sells = vec![order(seller, dec!(0.5), dec!(10.0), 2, 100, TimeInForce::Gtc, 0)];
        let (matches, _) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &NoFeeTopology,
            FastPrice::from_raw(-1_000_000_000), // adversarial: negative multiplier
            200,
        );
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].match_price, dec!(0.5), "negative multiplier clamped to 1.0");
    }

    /// Enforces a hard per-link flow cap so the per-cycle capacity ledger can be
    /// exercised: `can_accommodate_flow` returns true only while `amount <= cap`.
    struct CapTopology {
        cap: Decimal,
    }
    impl TopologySnapshot for CapTopology {
        fn can_accommodate_flow(&self, _f: Option<i32>, _t: Option<i32>, amount: Decimal) -> bool {
            amount <= self.cap
        }
        fn calculate_wheeling_charge(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice {
            FastPrice::from(dec!(0))
        }
        fn calculate_loss_factor(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice {
            FastPrice::from(dec!(1.0))
        }
    }

    /// Grid capacity is enforced cumulatively across a cycle. Two buyers each want the
    /// full 100 kWh link capacity from one large sell, but the link carries only 100 kWh
    /// total. Without the committed_flow ledger each buyer passes the per-trade
    /// can_accommodate_flow independently and the engine oversells the link to 200 kWh.
    /// With it, the higher-priority buyer fills 100 and the second is capacity-blocked.
    #[test]
    fn per_cycle_capacity_is_not_oversold() {
        let seller = Uuid::new_v4();
        let buyer_a = Uuid::new_v4();
        let buyer_b = Uuid::new_v4();
        // One big cross-zone sell (zone 1) — liquidity is NOT the binding constraint.
        let mut sells = vec![order(seller, dec!(0.5), dec!(200.0), 1, 100, TimeInForce::Gtc, 0)];
        // Two zone-2 buyers, each wanting 100 kWh. Same bid; A earlier → higher priority.
        let mut buys = vec![
            order(buyer_a, dec!(1.0), dec!(100.0), 2, 100, TimeInForce::Gtc, 0),
            order(buyer_b, dec!(1.0), dec!(100.0), 2, 200, TimeInForce::Gtc, 1),
        ];
        let (matches, stats) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(2),
            &meta(1),
            &CapTopology { cap: dec!(100.0) },
            unit(),
            300,
        );
        assert_eq!(matches.len(), 1, "link carries only one 100 kWh fill, not two");
        assert_eq!(
            matches[0].buyer_id, buyer_a,
            "higher-priority buyer wins the scarce link capacity"
        );
        assert_eq!(matches[0].match_amount, dec!(100.0));
        assert_eq!(
            stats.total_volume,
            dec!(100.0),
            "cumulative flow must not exceed the 100 kWh link rating"
        );
    }

    /// A FOK buy that would need more than a link can carry must be killed whole, not
    /// partially filled. Buyer wants 150 kWh FOK across a 100 kWh-capped link with ample
    /// liquidity (200 kWh sell). The capacity-aware FOK simulation sees only 100 kWh is
    /// reachable → kills the order; nothing fills.
    #[test]
    fn fok_buy_blocked_by_capacity_is_killed_whole() {
        let seller = Uuid::new_v4();
        let buyer = Uuid::new_v4();
        let mut sells = vec![order(seller, dec!(0.5), dec!(200.0), 1, 100, TimeInForce::Gtc, 0)];
        let mut buys = vec![order(buyer, dec!(1.0), dec!(150.0), 2, 100, TimeInForce::Fok, 0)];
        let (matches, stats) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(1),
            &CapTopology { cap: dec!(100.0) },
            unit(),
            200,
        );
        assert!(matches.is_empty(), "FOK buy exceeding link capacity must fill nothing");
        assert_eq!(buys[0].filled_amount, dec!(0.0));
        assert_eq!(
            stats.ioc_cancellations,
            vec![buys[0].id],
            "killed FOK must be swept for cancellation"
        );
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

    /// Single-zone fast path (early-stop build, no per-buy sort) must still match
    /// EVERY crossing pair — a completeness guard against the early `break`
    /// dropping fills. 300 buys × 300 sells, all crossing, 1:1 by size.
    #[test]
    #[allow(clippy::cast_possible_wrap)] // loop index → created_at_ns, always small
    fn single_zone_fast_path_matches_all_crossing() {
        let n = 300usize;
        let mut buys: Vec<FastOrder> = (0..n)
            .map(|i| order(Uuid::new_v4(), dec!(1.0), dec!(100.0), 1, i as i64, TimeInForce::Gtc, i))
            .collect();
        let mut sells: Vec<FastOrder> = (0..n)
            .map(|i| order(Uuid::new_v4(), dec!(0.9), dec!(100.0), 1, i as i64, TimeInForce::Gtc, i))
            .collect();
        let (matches, stats) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(n),
            &meta(n),
            &NoFeeTopology,
            unit(),
            (n * 2) as i64,
        );
        assert_eq!(stats.matches_created, n, "every crossing pair fills");
        assert_eq!(matches.len(), n);
        assert_eq!(stats.total_volume, dec!(100.0) * Decimal::from(n));
    }

    /// Early-stop must not disturb price-time priority: a buy stops building once it
    /// has gathered enough resting energy, but the sells it gathered (from the
    /// landed-ordered range) are the CHEAPEST ones. Buyer needs 100; the 0.50 ask
    /// (not the 0.70 / 0.90) must fill it.
    #[test]
    fn single_zone_early_stop_fills_cheapest() {
        let cheap = Uuid::new_v4();
        let mut sells = vec![
            order(cheap, dec!(0.50), dec!(100.0), 1, 100, TimeInForce::Gtc, 0),
            order(Uuid::new_v4(), dec!(0.70), dec!(100.0), 1, 101, TimeInForce::Gtc, 1),
            order(Uuid::new_v4(), dec!(0.90), dec!(100.0), 1, 102, TimeInForce::Gtc, 2),
        ];
        let mut buys = vec![order(Uuid::new_v4(), dec!(1.0), dec!(100.0), 1, 100, TimeInForce::Gtc, 0)];
        let (matches, stats) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(3),
            &NoFeeTopology,
            unit(),
            100,
        );
        assert_eq!(matches.len(), 1, "buyer fills entirely from the cheapest sell");
        assert_eq!(matches[0].seller_id, cheap);
        assert_eq!(stats.total_volume, dec!(100.0));
    }

    /// Early-stop must gather ACROSS sells until the buy is covered, still cheapest
    /// first. Buyer needs 250; fills 100+100+50 from the 0.50 / 0.60 / 0.70 asks and
    /// never touches the 0.80 ask.
    #[test]
    fn single_zone_early_stop_gathers_across_sells() {
        let (u1, u2, u3, u4) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let mut sells = vec![
            order(u1, dec!(0.50), dec!(100.0), 1, 100, TimeInForce::Gtc, 0),
            order(u2, dec!(0.60), dec!(100.0), 1, 101, TimeInForce::Gtc, 1),
            order(u3, dec!(0.70), dec!(100.0), 1, 102, TimeInForce::Gtc, 2),
            order(u4, dec!(0.80), dec!(100.0), 1, 103, TimeInForce::Gtc, 3),
        ];
        let mut buys = vec![order(Uuid::new_v4(), dec!(1.0), dec!(250.0), 1, 100, TimeInForce::Gtc, 0)];
        let (matches, stats) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(4),
            &NoFeeTopology,
            unit(),
            100,
        );
        assert_eq!(matches.len(), 3, "100 + 100 + 50 across the three cheapest");
        assert_eq!(stats.total_volume, dec!(250.0));
        let sellers: Vec<Uuid> = matches.iter().map(|m| m.seller_id).collect();
        assert!(sellers.contains(&u1) && sellers.contains(&u2) && sellers.contains(&u3));
        assert!(!sellers.contains(&u4), "the dearest 0.80 ask must stay untouched");
        assert_eq!(matches[2].match_amount, dec!(50.0), "third fill is the 50 remainder");
    }

    /// Grid capacity that caps one link at 150 while the buy wants 300. The
    /// single-zone early-stop under-fills (capacity skip) and triggers the full-scan
    /// rebuild; the rebuild must respect the running `committed_flow` cap (no
    /// over-fill, no double-count) and leave the buy partially filled — exactly a
    /// single full pass would.
    #[test]
    #[allow(clippy::cast_possible_wrap)] // loop index → created_at_ns, always small
    fn single_zone_capacity_cap_triggers_retry_without_overfill() {
        struct CapTopology;
        impl TopologySnapshot for CapTopology {
            fn can_accommodate_flow(&self, _f: Option<i32>, _t: Option<i32>, a: Decimal) -> bool {
                a <= dec!(150.0)
            }
            fn calculate_wheeling_charge(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice {
                FastPrice::from(dec!(0))
            }
            fn calculate_loss_factor(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice {
                FastPrice::from(dec!(1.0))
            }
        }
        let mut sells: Vec<FastOrder> = (0..4)
            .map(|i| order(Uuid::new_v4(), dec!(0.5), dec!(100.0), 1, 100 + i as i64, TimeInForce::Gtc, i))
            .collect();
        let mut buys = vec![order(Uuid::new_v4(), dec!(1.0), dec!(300.0), 1, 100, TimeInForce::Gtc, 0)];
        let (matches, stats) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(4),
            &CapTopology,
            unit(),
            100,
        );
        // First fill of 100 commits the link to 100; the next fill would push it to
        // 200 > 150 and is skipped, as are the rest — so only 100 settles.
        assert_eq!(stats.total_volume, dec!(100.0), "capacity gate caps the fill at one 100 lot");
        assert_eq!(matches.len(), 1, "no double-count across the retry rebuild");
        assert_eq!(buys[0].remaining_amount(), dec!(200.0), "buy left partially filled");
    }

    /// Multi-zone k-way merge: every crossing pair across 8 zones must still match
    /// (completeness), guarding the heap merge + early-stop against dropped fills.
    /// One buyer per zone, one seller per zone, all crossing 1:1.
    #[test]
    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)] // small loop indices
    fn multi_zone_merge_matches_all_crossing() {
        let z = 8usize;
        let mut buys: Vec<FastOrder> = (0..z)
            .map(|i| order(Uuid::new_v4(), dec!(1.0), dec!(100.0), (i % z) as i32, i as i64, TimeInForce::Gtc, i))
            .collect();
        let mut sells: Vec<FastOrder> = (0..z)
            .map(|i| order(Uuid::new_v4(), dec!(0.9), dec!(100.0), (i % z) as i32, i as i64, TimeInForce::Gtc, i))
            .collect();
        let (matches, stats) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(z),
            &meta(z),
            &NoFeeTopology,
            unit(),
            (z * 2) as i64,
        );
        assert_eq!(matches.len(), z, "each zone's buyer fills from its seller");
        assert_eq!(stats.total_volume, dec!(100.0) * Decimal::from(z));
    }

    /// Multi-zone merge must pick the globally cheapest sell first, across zones,
    /// even though each zone book is only locally sorted. The buyer sits in a zone
    /// with no local sell (so every sell is inter-zone, no intra-zone discount to
    /// muddy the landed order); sells in zones 1/2/3 priced 0.90/0.50/0.70 — the
    /// 0.50 (zone 2) must win.
    #[test]
    fn multi_zone_merge_fills_globally_cheapest_first() {
        let cheap = Uuid::new_v4();
        let mut sells = vec![
            order(Uuid::new_v4(), dec!(0.90), dec!(100.0), 1, 100, TimeInForce::Gtc, 0),
            order(cheap, dec!(0.50), dec!(100.0), 2, 100, TimeInForce::Gtc, 1),
            order(Uuid::new_v4(), dec!(0.70), dec!(100.0), 3, 100, TimeInForce::Gtc, 2),
        ];
        let mut buys = vec![order(Uuid::new_v4(), dec!(1.0), dec!(100.0), 99, 100, TimeInForce::Gtc, 0)];
        let (matches, stats) = MatchingEngine::match_cycle(
            &mut buys,
            &mut sells,
            &meta(1),
            &meta(3),
            &NoFeeTopology,
            unit(),
            100,
        );
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].seller_id, cheap, "cheapest across zones wins the merge");
        assert_eq!(stats.total_volume, dec!(100.0));
    }
}
