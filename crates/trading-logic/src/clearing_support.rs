//! Shared order-conversion and match-persistence helpers used by both trading
//! mechanisms — the continuous CDA matcher (`MatcherService`) and the
//! uniform-price auction clearing (`ClearingService`).
//!
//! Both produce the same `trading_engine::types::MatchResult` and persist it the
//! same way (settlement row first, then the `order_matches` ledger row + its
//! outbox event, then aggregated order fills), so that logic lives here once.

use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use trading_core::fast_price::FastPrice;
use trading_core::models::TradingOrder;
use trading_core::traits::{OrderRepository, SettlementRepository, TraitResult};
use trading_core::types::OrderStatus;
use trading_engine::types::{FastOrder, MatchResult, OrderMetadata};
use uuid::Uuid;

/// Convert domain orders into the engine's `FastOrder` hot-path representation
/// plus the parallel `OrderMetadata` sidecar (indexed by `metadata_index`).
pub(crate) fn to_fast_orders(orders: &[TradingOrder]) -> (Vec<FastOrder>, Vec<OrderMetadata>) {
    let mut metadata = Vec::with_capacity(orders.len());
    let fast = orders
        .iter()
        .enumerate()
        .map(|(i, o)| {
            metadata.push(OrderMetadata {
                epoch_id: o.epoch_id,
                order_pda: o.order_pda.as_ref().map(|s| Arc::from(s.as_str())),
                session_token: o.session_token.as_ref().map(|s| Arc::from(s.as_str())),
            });
            FastOrder {
                id: o.id,
                user_id: o.user_id,
                price: FastPrice::from(o.price_per_kwh),
                energy_amount: o.energy_amount,
                filled_amount: o.filled_amount,
                created_at_ns: o
                    .created_at
                    .map(|t| t.timestamp_nanos_opt().unwrap_or(0))
                    .unwrap_or(0),
                expires_at_ns: o.expires_at.map(|t| t.timestamp_nanos_opt().unwrap_or(0)),
                zone_id: o.zone_id,
                time_in_force: o.time_in_force,
                metadata_index: i,
            }
        })
        .collect();
    (fast, metadata)
}

/// `(energy_amount, prior_filled_amount)` keyed by order id, so the fill-apply
/// step can mark an order Filled once its full energy is covered and accumulate
/// onto prior fills (fills are written absolutely, not incrementally).
pub(crate) fn order_totals(
    a: &[TradingOrder],
    b: &[TradingOrder],
) -> HashMap<Uuid, (Decimal, Decimal)> {
    let mut totals = HashMap::new();
    for o in a.iter().chain(b.iter()) {
        totals.insert(o.id, (o.energy_amount, o.filled_amount));
    }
    totals
}

/// Persist a batch of matches: per match, the settlement row (first, so the
/// `order_matches.settlement_id` FK can point at it), then the ledger row + its
/// `OrderMatched` outbox event, then the aggregated per-order fill updates
/// (each with its `OrderUpdate` event). Atomic at each repo call via the
/// `_with_event` methods so an event can never be lost relative to its write.
pub(crate) async fn persist_matches(
    order_repo: &dyn OrderRepository,
    settlement_repo: &dyn SettlementRepository,
    matches: &[MatchResult],
    buy_metadata: &[OrderMetadata],
    sell_metadata: &[OrderMetadata],
    totals: &HashMap<Uuid, (Decimal, Decimal)>,
) -> TraitResult<()> {
    let mut order_deltas: HashMap<Uuid, (Decimal, OrderStatus, Option<i32>)> = HashMap::new();

    for m in matches {
        let (buy_amt, _, buy_zone) = order_deltas
            .entry(m.buy_order_id)
            .or_insert((Decimal::ZERO, OrderStatus::PartiallyFilled, m.buyer_zone_id));
        *buy_amt += m.match_amount;
        *buy_zone = m.buyer_zone_id;

        let (sell_amt, _, sell_zone) = order_deltas
            .entry(m.sell_order_id)
            .or_insert((Decimal::ZERO, OrderStatus::PartiallyFilled, m.seller_zone_id));
        *sell_amt += m.match_amount;
        *sell_zone = m.seller_zone_id;

        let buy_meta = &buy_metadata[m.buy_metadata_index];
        let sell_meta = &sell_metadata[m.sell_metadata_index];

        // Shared ids so the ledger row, its settlement, and the event reconcile.
        let match_id = Uuid::new_v4();
        let settlement_id = Uuid::new_v4();

        // The price the trade settles/records at, chosen by the producing engine
        // (`MatchResult::settle_price`): the seller's ask for CDA, the uniform
        // clearing price `p_star` for the interval auction. Both are
        // `>= sell_order.price_per_kwh`, so the on-chain
        // `execute_atomic_settlement` guard (`price >= ask`, Custom 6024
        // SlippageExceeded otherwise) holds. The CDA intra-zone discount lowers the
        // buyer's landed `match_price` below the ask to help a trade cross, but it
        // is a crossing incentive only — never what settles.
        let settle_price = m.settle_price;

        // The matching engine guarantees the seller's ask never exceeds the buyer's
        // bid (the Case-1 settlement invariant enforced in `priced_candidate`): a sell
        // whose ask is above the bid is no longer rescued by the intra-zone discount,
        // because it could not be settled on-chain (buyer escrow < ask, and the swap
        // guard rejects price < ask). So every emitted match is settleable at the ask
        // — there is no rescued (ask > bid) case to skip. Settle at the ask; the
        // buyer's escrow, posted at the bid (>= ask), covers it.
        let settlement_link = {
            // Settlement first (order_matches.settlement_id FKs to it). On failure,
            // record the match with a NULL link rather than dropping the ledger row.
            let total_at_ask = m.match_amount * settle_price;
            match settlement_repo
                .insert_settlement(&trading_core::models::Settlement {
                    id: settlement_id,
                    trade_id: Some(Uuid::new_v4()),
                    epoch_id: m.epoch_id,
                    buyer_id: m.buyer_id,
                    seller_id: m.seller_id,
                    buy_order_id: m.buy_order_id,
                    sell_order_id: m.sell_order_id,
                    energy_amount: m.match_amount,
                    price: settle_price,
                    total_amount: total_at_ask,
                    fee_amount: rust_decimal_macros::dec!(0),
                    net_amount: total_at_ask,
                    status: trading_core::models::SettlementStatus::Pending,
                    blockchain_tx: None,
                    created_at: gridtokenx_telemetry::time::now(),
                    confirmed_at: None,
                    wheeling_charge: Some(m.wheeling_charge),
                    loss_factor: Some(m.loss_factor),
                    loss_cost: Some(m.loss_cost),
                    effective_energy: Some(m.match_amount),
                    buyer_zone_id: m.buyer_zone_id,
                    seller_zone_id: m.seller_zone_id,
                    buyer_session_token: buy_meta.session_token.as_ref().map(|s| s.to_string()),
                    seller_session_token: sell_meta.session_token.as_ref().map(|s| s.to_string()),
                    erc_certificate_id: None,
                    erc_transfer_tx: None,
                    retry_count: 0,
                    error_message: None,
                })
                .await
            {
                Ok(()) => Some(settlement_id),
                Err(e) => {
                    tracing::warn!(error = %e, buy_order_id = %m.buy_order_id, sell_order_id = %m.sell_order_id, "Failed to persist settlement row; recording order_matches without settlement link");
                    None
                }
            }
        };

        // Record the price that actually settles on-chain (the seller's ask), NOT the
        // discounted CDA landed `match_price`. The intra-zone discount is a crossing
        // incentive only: the buyer is charged the ask (the pooled escrow moves
        // `amount * ask` to the seller), so publishing `match_price` here made the
        // OrderMatched event and order_matches ledger disagree with the wallet — the
        // explorer showed 0.95 while 1.0 moved. Record `settle_price` so ledger,
        // event, and on-chain transfer reconcile. (Delivering the discount to the
        // buyer as a platform subsidy is a separate, unbuilt feature — see the
        // custodial-accounting investigation; until then the discount does not reduce
        // what the buyer pays.)
        let matched_event =
            trading_core::events::Event::OrderMatched(trading_core::events::OrderMatchedPayload {
                match_id,
                epoch_id: m.epoch_id,
                buy_order_id: m.buy_order_id,
                sell_order_id: m.sell_order_id,
                amount: m.match_amount,
                price: settle_price,
                buyer_id: m.buyer_id,
                seller_id: m.seller_id,
                timestamp: gridtokenx_telemetry::time::now(),
                zone_id: m.buyer_zone_id,
            });
        if let Err(e) = settlement_repo
            .insert_match_with_event(
                &trading_core::models::OrderMatch {
                    id: match_id,
                    epoch_id: m.epoch_id,
                    buy_order_id: m.buy_order_id,
                    sell_order_id: m.sell_order_id,
                    matched_amount: m.match_amount,
                    match_price: settle_price,
                    match_time: gridtokenx_telemetry::time::now(),
                    status: "pending".to_string(),
                },
                settlement_link,
                m.buyer_zone_id,
                &matched_event,
            )
            .await
        {
            tracing::warn!(error = %e, buy_order_id = %m.buy_order_id, sell_order_id = %m.sell_order_id, "Failed to persist order_matches row");
        }
    }

    // Apply aggregated fills. filled_amount is absolute: prior + this batch.
    for (order_id, (amount, _status, zone_id)) in order_deltas {
        let (energy_amount, prior_filled) =
            totals.get(&order_id).copied().unwrap_or((Decimal::ZERO, Decimal::ZERO));
        let cumulative = prior_filled + amount;
        let status = if energy_amount > Decimal::ZERO && cumulative >= energy_amount {
            OrderStatus::Filled
        } else {
            OrderStatus::PartiallyFilled
        };

        let update_event = trading_core::events::Event::OrderUpdate {
            id: order_id,
            filled_amount: cumulative,
            status: status.to_string(),
            zone_id,
        };
        order_repo
            .update_filled_amount_with_event(order_id, cumulative, status, &update_event)
            .await?;
    }

    Ok(())
}
