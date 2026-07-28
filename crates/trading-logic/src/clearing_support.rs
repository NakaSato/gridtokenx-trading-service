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
use trading_core::traits::{SettlementRepository, TradeFill, TraitResult};
use trading_engine::types::{FastOrder, MatchResult, OrderMetadata};
use uuid::Uuid;

/// Convert domain orders into the engine's `FastOrder` hot-path representation
/// plus the parallel `OrderMetadata` sidecar (indexed by `metadata_index`).
///
/// Orders with no `epoch_id` are excluded: `settlements.epoch_id` and
/// `order_matches.epoch_id` are NOT NULL and FK to `market_epochs`, so a match
/// involving one can never be persisted. Letting them into a book produced a
/// match that failed its atomic persist and re-matched every cycle — an endless
/// retry loop that never drained. Placement stamps every order with the active
/// epoch (`rest.rs` `submit_order`), so a NULL here means orphaned data, not a
/// live market participant. See `trading_persistence::repositories::epoch`.
pub(crate) fn to_fast_orders(orders: &[TradingOrder]) -> (Vec<FastOrder>, Vec<OrderMetadata>) {
    let epoch_less = orders.iter().filter(|o| o.epoch_id.is_none()).count();
    if epoch_less > 0 {
        tracing::warn!(
            epoch_less,
            "orders with no market epoch excluded from matching (unsettleable — orphaned rows?)"
        );
    }

    let mut metadata = Vec::with_capacity(orders.len());
    let fast = orders
        .iter()
        .filter_map(|o| o.epoch_id.map(|epoch_id| (o, epoch_id)))
        .enumerate()
        .map(|(i, (o, epoch_id))| {
            metadata.push(OrderMetadata {
                epoch_id,
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
) -> HashMap<Uuid, (Decimal, Decimal, Uuid)> {
    let mut totals = HashMap::new();
    for o in a.iter().chain(b.iter()) {
        // The owner rides along so `persist_matches` can stamp it on the
        // `OrderUpdate` event without a second lookup.
        totals.insert(o.id, (o.energy_amount, o.filled_amount, o.user_id));
    }
    totals
}

/// Persist a batch of matches. Each match is persisted **atomically** through
/// [`SettlementRepository::persist_matched_trade`]: its settlement row, the
/// `order_matches` ledger row, both orders' incremental fills, and every outbox
/// event commit together or not at all. A trade whose settlement/ledger write
/// fails, or whose order raced to a terminal status, rolls back entirely and is
/// simply left for the next cycle — orders are re-read from the DB each cycle
/// (`get_active_*`), so nothing is double-counted, orphaned, or filled-without-
/// payment. `totals` supplies each order's owner for the `OrderUpdate` events.
pub(crate) async fn persist_matches(
    settlement_repo: &dyn SettlementRepository,
    matches: &[MatchResult],
    buy_metadata: &[OrderMetadata],
    sell_metadata: &[OrderMetadata],
    totals: &HashMap<Uuid, (Decimal, Decimal, Uuid)>,
    rates: &dyn trading_core::charges::ChargeRates,
) -> TraitResult<()> {
    let owner_of = |order_id: Uuid| totals.get(&order_id).map(|(_, _, u)| *u);

    for m in matches {
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
        let total_at_ask = m.match_amount * settle_price;

        // What the chain will actually deduct from the buyer's escrow before the
        // seller is paid. Previously this row recorded the matching engine's own
        // numbers — fee hardcoded to zero, net set to the gross, and wheeling/loss
        // taken from the landed-cost calculation that decides whether a pair
        // crosses. Those are not the tariff the chain applies: on a measured
        // 1 kWh @ THB1.00 trade the seller was credited 0.897 while this row
        // claimed net 1.00 / fee 0.00 / wheeling 0.00 / loss 0.01.
        //
        // `m.wheeling_charge` / `m.loss_cost` remain correct for their own purpose
        // (pricing the buyer's landed cost during matching) — they simply are not
        // what gets charged, so only the tariff-derived values are persisted here.
        let charges = trading_core::charges::compute_charges(total_at_ask, m.match_amount, rates);

        let settlement = trading_core::models::Settlement {
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
            fee_amount: charges.fee,
            net_amount: charges.net,
            status: trading_core::models::SettlementStatus::Pending,
            blockchain_tx: None,
            created_at: gridtokenx_telemetry::time::now(),
            confirmed_at: None,
            wheeling_charge: Some(charges.wheeling),
            loss_factor: Some(m.loss_factor),
            loss_cost: Some(charges.loss),
            effective_energy: Some(m.match_amount),
            buyer_zone_id: m.buyer_zone_id,
            seller_zone_id: m.seller_zone_id,
            buyer_session_token: buy_meta.session_token.as_ref().map(|s| s.to_string()),
            seller_session_token: sell_meta.session_token.as_ref().map(|s| s.to_string()),
            erc_certificate_id: None,
            erc_transfer_tx: None,
            retry_count: 0,
            error_message: None,
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
        let order_match = trading_core::models::OrderMatch {
            id: match_id,
            epoch_id: m.epoch_id,
            buy_order_id: m.buy_order_id,
            sell_order_id: m.sell_order_id,
            matched_amount: m.match_amount,
            match_price: settle_price,
            match_time: gridtokenx_telemetry::time::now(),
            status: "pending".to_string(),
        };

        let buyer = TradeFill {
            order_id: m.buy_order_id,
            user_id: owner_of(m.buy_order_id),
            delta: m.match_amount,
            zone_id: m.buyer_zone_id,
        };
        let seller = TradeFill {
            order_id: m.sell_order_id,
            user_id: owner_of(m.sell_order_id),
            delta: m.match_amount,
            zone_id: m.seller_zone_id,
        };

        match settlement_repo
            .persist_matched_trade(&settlement, &order_match, &matched_event, m.buyer_zone_id, &buyer, &seller)
            .await
        {
            Ok(true) => {}
            Ok(false) => tracing::warn!(
                buy_order_id = %m.buy_order_id,
                sell_order_id = %m.sell_order_id,
                "A side went terminal mid-cycle; trade rolled back atomically, orders re-match next cycle"
            ),
            Err(e) => tracing::warn!(
                error = %e,
                buy_order_id = %m.buy_order_id,
                sell_order_id = %m.sell_order_id,
                "Atomic trade persist failed; trade skipped (nothing written), orders re-match next cycle"
            ),
        }
    }

    Ok(())
}
