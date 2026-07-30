use crate::state::AppState;
use axum::{
    extract::{Path, Query, State},
    http::header,
    response::{IntoResponse, Response},
    Json,
};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use tracing::info;
use utoipa::{IntoParams, ToSchema};
use trading_core::models::{
    MarketPrice, NewPriceAlert, NewRecurringOrder, OrderBookEntry, PriceAlert, RecurringOrder,
    Settlement, SettlementStats, TradingOrder,
};
use trading_core::recurring::next_execution_at;
use trading_core::types::{
    AlertCondition, AlertStatus, IntervalType, OrderSide, OrderStatus, OrderType, RecurringStatus,
    TimeInForce,
};
use uuid::Uuid;

#[derive(Debug, Deserialize, ToSchema)]
pub struct SubmitOrderRequest {
    pub side: String,
    pub order_type: String,
    pub energy_amount_kwh: String,
    /// Required for limit orders. For a market buy it is optional and, if given,
    /// acts as the maximum acceptable price (slippage cap); absent → a wide
    /// default ceiling. A market buy still fills at the resting ask, not this value.
    pub price_per_kwh: Option<String>,
    pub zone_id: i32,
    /// The metering `meters.id`. Callers holding a `serial_number` (the grid
    /// map's node id) must send `meter_serial` instead — the two id spaces are
    /// not interchangeable and `trading_orders.meter_id` FKs to `meters.id`.
    pub meter_id: Option<Uuid>,
    /// The meter's `serial_number`, resolved server-side to `meters.id`.
    /// Takes precedence over `meter_id` when both are sent.
    pub meter_serial: Option<String>,
    pub custodial_sign: Option<bool>,
    /// Time-in-force: "gtc" (default), "ioc", or "fok".
    pub time_in_force: Option<String>,
    /// Market segment: "realtime" (default, CDA matcher) or "interval"
    /// (15-min uniform-price clearing).
    pub market_segment: Option<String>,

    // ── Order lifetime (both optional; send at most one) ─────────────────────
    //
    // Omitted → `ORDER_DEFAULT_TTL_SECS` (15m, one interval-clearing window) from
    // now. A resting order past its expiry is skipped
    // by the matcher immediately and flipped to `Expired` by the ReaperWorker.
    // Inert for ioc/fok, which never rest.
    /// Absolute expiry (RFC 3339, e.g. `2026-07-30T12:00:00Z`). Use when the
    /// deadline is an instant that matters — an epoch boundary, a delivery window.
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Expiry as a lifetime in seconds from now. Preferred by default: unlike
    /// `expires_at` it cannot be thrown off by client clock skew.
    pub expires_in_secs: Option<i64>,

    // ── Per-user escrow settlement (config `per_user_escrow_settlement`) ──────
    //
    // Required only when that flag is on. The three fields below are inputs to
    // the signed payload, so the client must send exactly what it signed; the
    // server re-derives the message from them and verifies the signature before
    // accepting the order (see `crate::order_signature`).
    /// Order UUID. The client generates it because the id is part of the signed
    /// payload — the server cannot mint one after the fact and still verify.
    pub order_id: Option<Uuid>,
    /// Base58 Ed25519 signature over the canonical payload
    /// ([`trading_core::offchain_payload`]), produced by the user's wallet.
    /// Distinct from the HMAC `signature` on `CreateOrderRequest`.
    pub wallet_signature: Option<String>,
    /// The exact `expires_at` (unix seconds) that was signed. Sent explicitly
    /// because the server's own default TTL would not match the signed bytes.
    pub signed_expires_at: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SubmitOrderResponse {
    pub id: Uuid,
    pub status: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OrderBookResponse {
    pub zone_id: i32,
    /// Sequence the `/ws/trading` stream had reached for this zone when the
    /// snapshot was read.
    ///
    /// **A staleness hint, not a splice point.** This book is read from
    /// Postgres while the sequence counts Kafka frames, and events reach
    /// Postgres before they reach Kafka (the outbox relay,
    /// `trading-infra/src/events/outbox_worker.rs`). So the snapshot can
    /// already contain an order this sequence has not counted yet. A client
    /// that resumed deltas from exactly `last_update_id + 1` would re-apply
    /// that order.
    ///
    /// Apply frames keyed by `order_id`/`match_id` so a repeat is a no-op, and
    /// use this only to notice you have fallen behind. Note it also **resets on
    /// gateway restart** — the WS pump replays Kafka from earliest with a fresh
    /// consumer group — so treat a decrease as "resync", not as an error.
    pub last_update_id: u64,
    pub asks: Vec<[String; 2]>,
    pub bids: Vec<[String; 2]>,
}

/// One meter that currently has at least one resting order, and on which sides.
///
/// Deliberately minimal: no user id, amount or price. It answers "is this meter
/// trading right now, and which way" — enough for the map to filter markers —
/// without turning a map read into an order-flow feed.
#[derive(Debug, Serialize, ToSchema)]
pub struct ActiveOrderMeter {
    /// The metering `meters.id` the order was placed against.
    pub meter_id: Uuid,
    /// The same meter's `serial_number` — the id the grid map keys its nodes on.
    /// Clients matching against map nodes must use this, not `meter_id`.
    pub meter_serial: String,
    pub zone_id: i32,
    pub has_open_buy: bool,
    pub has_open_sell: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ActiveOrderMetersResponse {
    pub data: Vec<ActiveOrderMeter>,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct ListOrdersParams {
    pub status: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListOrdersResponse {
    pub data: Vec<OrderData>,
    pub pagination: Pagination,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OrderData {
    pub id: Uuid,
    pub zone_id: i32,
    pub side: String,
    pub order_type: String,
    pub status: String,
    pub energy_amount_kwh: String,
    /// `None` for market orders: their stored `price_per_kwh` is a synthetic bid
    /// (ceiling or slippage cap), not the price the order executed at — surfacing
    /// it would mislead. Limit orders carry the user's real price.
    pub price_per_kwh: Option<String>,
    pub filled_amount_kwh: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl OrderData {
    fn from_order(o: &TradingOrder) -> Self {
        // Market orders price at the resting ask; the row's price_per_kwh is a
        // synthetic bid the user never set, so don't expose it as the price.
        let price_per_kwh = match o.order_type {
            OrderType::Market => None,
            _ => Some(o.price_per_kwh.to_string()),
        };
        OrderData {
            id: o.id,
            zone_id: o.zone_id.unwrap_or(0),
            side: o.side.to_string().to_lowercase(),
            order_type: o.order_type.to_string().to_lowercase(),
            status: o.status.to_string().to_lowercase(),
            energy_amount_kwh: o.energy_amount.to_string(),
            price_per_kwh,
            filled_amount_kwh: o.filled_amount.to_string(),
            created_at: o.created_at.unwrap_or_else(gridtokenx_telemetry::time::now),
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Pagination {
    pub total: usize,
    pub limit: usize,
    pub offset: usize,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct QuoteRequest {
    pub buyer_zone_id: i32,
    pub seller_zone_id: i32,
    pub energy_amount_kwh: String,
    pub agreed_price: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct QuoteResponse {
    pub quote_id: String,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    pub breakdown: QuoteBreakdown,
    pub grid_metrics: GridMetrics,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct QuoteBreakdown {
    pub energy_cost: String,
    pub wheeling_charge: String,
    pub loss_cost: String,
    pub total_cost: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct GridMetrics {
    pub effective_energy_kwh: String,
    pub loss_factor: String,
    pub zone_distance_km: String,
    pub is_grid_compliant: bool,
}

use crate::auth::UserContext;
use gridtokenx_blockchain_auth::ServiceRole;

/// Submit a spot order (limit or market) into the CDA or interval market.
#[utoipa::path(
    post,
    path = "/api/v1/orders",
    tag = "orders",
    request_body = SubmitOrderRequest,
    responses(
        (status = 200, description = "Order accepted (status `open`)", body = SubmitOrderResponse),
        (status = 400, description = "Invalid side/type/amount/price/TIF/segment combination", body = String),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database or epoch resolution error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]

// ---------------------------------------------------------------------------
// Handlers live in per-domain submodules (split out of this file). They are
// re-exported so every existing `crate::rest::<name>` path — router wiring and
// openapi.rs — keeps resolving exactly as before.
// ---------------------------------------------------------------------------
mod orders;
mod futures;
mod portfolio;
mod market;
mod trades;
mod alerts;
mod recurring;

pub use orders::*;
pub use futures::*;
pub use portfolio::*;
pub use market::*;
pub use trades::*;
pub use alerts::*;
pub use recurring::*;

#[cfg(test)]
mod tests {
    use super::*;

    fn order(side: OrderSide, price: i64) -> TradingOrder {
        TradingOrder {
            id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            order_type: OrderType::Limit,
            side,
            energy_amount: Decimal::new(100, 0),
            price_per_kwh: Decimal::new(price, 2),
            filled_amount: Decimal::ZERO,
            status: OrderStatus::Pending,
            expires_at: None,
            created_at: None,
            filled_at: None,
            epoch_id: None,
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
            market_segment: trading_core::types::MarketSegment::Realtime,
        }
    }

    fn price_alert(status: AlertStatus, note: Option<&str>) -> PriceAlert {
        PriceAlert {
            id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            target_price: Decimal::new(125, 1), // 12.5
            condition: AlertCondition::Above,
            status,
            triggered_at: None,
            triggered_price: None,
            repeat: false,
            note: note.map(str::to_string),
            created_at: gridtokenx_telemetry::time::now(),
            updated_at: None,
        }
    }

    #[test]
    fn price_alert_active_maps_symbol_and_decimal() {
        let r = build_price_alert_response(&price_alert(AlertStatus::Active, Some("GRID")));
        assert_eq!(r.symbol, "GRID"); // note -> symbol
        assert_eq!(r.target_price, "12.5");
        assert_eq!(r.condition, "above");
        assert!(r.is_active);
    }

    #[test]
    fn price_alert_triggered_inactive_empty_symbol() {
        let r = build_price_alert_response(&price_alert(AlertStatus::Triggered, None));
        assert_eq!(r.symbol, ""); // null note -> ""
        assert!(!r.is_active);
    }

    fn recurring(status: RecurringStatus) -> RecurringOrder {
        RecurringOrder {
            id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            side: OrderSide::Buy,
            energy_amount: Decimal::new(1050, 2), // 10.50
            max_price_per_kwh: Some(Decimal::new(20, 2)), // 0.20
            min_price_per_kwh: None,
            interval_type: IntervalType::Daily,
            interval_value: 2,
            next_execution_at: gridtokenx_telemetry::time::now(),
            last_executed_at: None,
            status,
            total_executions: 0,
            max_executions: Some(5),
            name: Some("dca".to_string()),
            description: None,
            created_at: gridtokenx_telemetry::time::now(),
            updated_at: gridtokenx_telemetry::time::now(),
        }
    }

    #[test]
    fn recurring_response_maps_decimals_and_enums() {
        let r = build_recurring_response(&recurring(RecurringStatus::Active));
        assert_eq!(r.side, "buy");
        assert_eq!(r.energy_amount, "10.50"); // decimal preserved as string
        assert_eq!(r.max_price_per_kwh.as_deref(), Some("0.20"));
        assert_eq!(r.min_price_per_kwh, None);
        assert_eq!(r.interval_type, "daily");
        assert_eq!(r.status, "active");
        assert_eq!(r.interval_value, 2);
    }

    #[test]
    fn recurring_response_paused_status() {
        let r = build_recurring_response(&recurring(RecurringStatus::Paused));
        assert_eq!(r.status, "paused");
    }

    #[test]
    fn matching_status_empty() {
        let r = build_matching_status(&[], &[], gridtokenx_telemetry::time::now());
        assert_eq!(r.pending_buy_orders, 0);
        assert_eq!(r.pending_sell_orders, 0);
        assert_eq!(r.pending_matches, 0);
        assert_eq!(r.buy_price_range.min, 0.0);
        assert_eq!(r.sell_price_range.max, 0.0);
        assert!(!r.can_match);
        assert_eq!(r.match_reason, "no orders");
    }

    #[test]
    fn matching_status_buys_only() {
        let r = build_matching_status(&[order(OrderSide::Buy, 500)], &[], gridtokenx_telemetry::time::now());
        assert_eq!(r.pending_buy_orders, 1);
        assert_eq!(r.pending_sell_orders, 0);
        assert!(!r.can_match);
        assert_eq!(r.pending_matches, 0);
        assert_eq!(r.match_reason, "no sell liquidity");
    }

    #[test]
    fn matching_status_sells_only() {
        let r = build_matching_status(&[], &[order(OrderSide::Sell, 400)], gridtokenx_telemetry::time::now());
        assert_eq!(r.pending_sell_orders, 1);
        assert!(!r.can_match);
        assert_eq!(r.match_reason, "no buy liquidity");
    }

    #[test]
    fn matching_status_crossing() {
        // buy_max 500 >= sell_min 400 → crossing
        let buys = vec![order(OrderSide::Buy, 450), order(OrderSide::Buy, 500)];
        let sells = vec![order(OrderSide::Sell, 400), order(OrderSide::Sell, 600)];
        let r = build_matching_status(&buys, &sells, gridtokenx_telemetry::time::now());
        assert!(r.can_match);
        assert!(r.pending_matches > 0);
        assert_eq!(r.buy_price_range.min, 4.5);
        assert_eq!(r.buy_price_range.max, 5.0);
        assert_eq!(r.sell_price_range.min, 4.0);
        assert_eq!(r.sell_price_range.max, 6.0);
        assert_eq!(r.match_reason, "orders crossing");
    }

    #[test]
    fn matching_status_non_crossing() {
        // buy_max 300 < sell_min 400 → spread too wide
        let buys = vec![order(OrderSide::Buy, 300)];
        let sells = vec![order(OrderSide::Sell, 400)];
        let r = build_matching_status(&buys, &sells, gridtokenx_telemetry::time::now());
        assert!(!r.can_match);
        assert_eq!(r.pending_matches, 0);
        assert_eq!(r.match_reason, "spread too wide");
    }

    /// Expired-but-unreaped orders must not count as liquidity: the DB query no
    /// longer filters them, so build_matching_status filters by expires_at itself.
    #[test]
    fn matching_status_excludes_expired_orders() {
        let now = gridtokenx_telemetry::time::now();
        let mut expired_sell = order(OrderSide::Sell, 400);
        expired_sell.expires_at = Some(now - chrono::Duration::seconds(1));
        // Crossing buy (500) vs an expired sell (400): without filtering this would
        // report can_match=true; the expired sell must be dropped → no liquidity.
        let r = build_matching_status(&[order(OrderSide::Buy, 500)], &[expired_sell], now);
        assert_eq!(r.pending_sell_orders, 0, "expired sell excluded");
        assert!(!r.can_match, "no live counterparty → cannot match");
        assert_eq!(r.pending_matches, 0);
        assert_eq!(r.match_reason, "no sell liquidity");
    }

    #[test]
    fn matching_status_pending_matches_min_side() {
        // 3 crossable buys, 1 crossable sell → min = 1
        let buys = vec![
            order(OrderSide::Buy, 500),
            order(OrderSide::Buy, 510),
            order(OrderSide::Buy, 520),
        ];
        let sells = vec![order(OrderSide::Sell, 400)];
        let r = build_matching_status(&buys, &sells, gridtokenx_telemetry::time::now());
        assert_eq!(r.pending_matches, 1);
    }

    fn book_entry(side: OrderSide, price: i64, amount: i64) -> OrderBookEntry {
        OrderBookEntry {
            order_id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            side,
            energy_amount: Decimal::new(amount, 0),
            original_amount: Decimal::new(amount, 0),
            price_per_kwh: Decimal::new(price, 2),
            created_at: gridtokenx_telemetry::time::now(),
            zone_id: Some(1),
            session_token: None,
            signature: None,
            payload_bytes: None,
            time_in_force: TimeInForce::Gtc,
        }
    }

    #[test]
    fn settlement_stats_mapping() {
        let s = SettlementStats {
            pending_count: 3,
            processing_count: 1,
            confirmed_count: 5,
            failed_count: 2,
            total_settled_value: Decimal::new(12550, 2),
        };
        let r = build_settlement_stats_response(&s);
        assert_eq!(r.pending_count, 3);
        assert_eq!(r.processing_count, 1);
        assert_eq!(r.confirmed_count, 5);
        assert_eq!(r.failed_count, 2);
        assert_eq!(r.total_settled_value, 125.5);
    }

    #[test]
    fn orderbook_empty() {
        let r = build_p2p_orderbook(&[]);
        assert!(r.asks.is_empty());
        assert!(r.bids.is_empty());
    }

    #[test]
    fn orderbook_split_and_sort() {
        let entries = vec![
            book_entry(OrderSide::Buy, 450, 10),
            book_entry(OrderSide::Buy, 500, 5),
            book_entry(OrderSide::Sell, 600, 8),
            book_entry(OrderSide::Sell, 550, 3),
        ];
        let r = build_p2p_orderbook(&entries);
        // bids descending by price
        assert_eq!(r.bids[0][0], "5.00");
        assert_eq!(r.bids[1][0], "4.50");
        // asks ascending by price
        assert_eq!(r.asks[0][0], "5.50");
        assert_eq!(r.asks[1][0], "6.00");
    }

    #[test]
    fn orderbook_aggregates_price_level() {
        // two buys at same price → summed amount
        let entries = vec![
            book_entry(OrderSide::Buy, 500, 10),
            book_entry(OrderSide::Buy, 500, 15),
        ];
        let r = build_p2p_orderbook(&entries);
        assert_eq!(r.bids.len(), 1);
        assert_eq!(r.bids[0][0], "5.00");
        assert_eq!(r.bids[0][1], "25");
    }

    // ── Trades (Phase 3) ────────────────────────────────────────────────────

    fn settlement(buyer: Uuid, seller: Uuid) -> trading_core::models::Settlement {
        use trading_core::models::SettlementStatus;
        trading_core::models::Settlement {
            id: Uuid::new_v4(),
            trade_id: None,
            epoch_id: Uuid::nil(),
            buyer_id: buyer,
            seller_id: seller,
            buy_order_id: Uuid::new_v4(),
            sell_order_id: Uuid::new_v4(),
            energy_amount: Decimal::new(105, 1), // 10.5
            price: Decimal::new(12, 1),          // 1.2
            total_amount: Decimal::new(126, 1),  // 12.6
            fee_amount: Decimal::new(5, 2),      // 0.05
            net_amount: Decimal::new(1255, 2),
            status: SettlementStatus::Completed,
            blockchain_tx: Some("sig123".to_string()),
            created_at: gridtokenx_telemetry::time::now(),
            confirmed_at: None,
            wheeling_charge: Some(Decimal::new(2, 2)),
            loss_factor: None,
            loss_cost: Some(Decimal::new(1, 2)),
            effective_energy: Some(Decimal::new(104, 1)),
            buyer_zone_id: Some(1),
            seller_zone_id: Some(2),
            buyer_session_token: None,
            seller_session_token: None,
            erc_certificate_id: None,
            erc_transfer_tx: None,
            retry_count: 0,
            error_message: None,
        }
    }

    #[test]
    fn trade_record_role_buyer() {
        let me = Uuid::new_v4();
        let other = Uuid::new_v4();
        let r = build_trade_record(&settlement(me, other), me);
        assert_eq!(r.role, "buyer");
        assert_eq!(r.counterparty_id, other);
        assert_eq!(r.quantity, "10.5");
        assert_eq!(r.price, "1.2");
        assert_eq!(r.total_value, "12.6");
        assert_eq!(r.status, "completed");
        assert_eq!(r.transaction_hash.as_deref(), Some("sig123"));
    }

    /// A parked settlement must carry its diagnostics to the client — the
    /// UI renders `error_message`/`retry_count` on `permanently_failed` rows.
    #[test]
    fn trade_record_surfaces_permanent_failure_diagnostics() {
        use trading_core::models::SettlementStatus;
        let me = Uuid::new_v4();
        let other = Uuid::new_v4();
        let mut s = settlement(me, other);
        s.status = SettlementStatus::PermanentlyFailed;
        s.retry_count = 5;
        s.error_message = Some("chain bridge: blockhash expired".to_string());

        let r = build_trade_record(&s, me);

        assert_eq!(r.status, "permanently_failed");
        assert_eq!(r.retry_count, 5);
        assert_eq!(
            r.error_message.as_deref(),
            Some("chain bridge: blockhash expired")
        );
    }

    /// A healthy settlement reports no failure reason.
    #[test]
    fn trade_record_healthy_has_no_error_message() {
        let me = Uuid::new_v4();
        let r = build_trade_record(&settlement(me, Uuid::new_v4()), me);
        assert_eq!(r.retry_count, 0);
        assert!(r.error_message.is_none());
    }

    #[test]
    fn trade_record_role_seller() {
        let me = Uuid::new_v4();
        let other = Uuid::new_v4();
        let r = build_trade_record(&settlement(other, me), me);
        assert_eq!(r.role, "seller");
        assert_eq!(r.counterparty_id, other);
    }

    #[test]
    fn trade_record_null_optionals_zero() {
        let me = Uuid::new_v4();
        let mut s = settlement(me, Uuid::new_v4());
        s.wheeling_charge = None;
        s.loss_cost = None;
        s.effective_energy = None;
        let r = build_trade_record(&s, me);
        assert_eq!(r.wheeling_charge, "0");
        assert_eq!(r.loss_cost, "0");
        assert_eq!(r.effective_energy, "0");
    }

    #[test]
    fn trades_response_dual_totals() {
        let me = Uuid::new_v4();
        let resp = build_trades_response(&[settlement(me, Uuid::new_v4())], 7, me);
        assert_eq!(resp.trades.len(), 1);
        assert_eq!(resp.total, 7);
        assert_eq!(resp.total_count, 7);
    }

    #[test]
    fn csv_field_escapes() {
        assert_eq!(csv_field("plain"), "plain");
        assert_eq!(csv_field("a,b"), "\"a,b\"");
        assert_eq!(csv_field("a\"b"), "\"a\"\"b\"");
        assert_eq!(csv_field("a\nb"), "\"a\nb\"");
    }

    #[test]
    fn csv_has_header_and_row() {
        let me = Uuid::new_v4();
        let rec = build_trade_record(&settlement(me, Uuid::new_v4()), me);
        let csv = trades_to_csv(&[rec]);
        let mut lines = csv.lines();
        assert!(lines
            .next()
            .expect("csv header line")
            .starts_with("id,executed_at,role,"));
        let row = lines.next().expect("csv data row");
        assert!(row.contains("buyer"));
        assert!(row.contains("12.6"));
        // exactly header + 1 data row
        assert_eq!(csv.lines().count(), 2);
    }
}
