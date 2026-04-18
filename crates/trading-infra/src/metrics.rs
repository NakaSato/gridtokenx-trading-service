//! Trading service metrics instrumentation
//! 
//! This module provides metrics for:
//! - Order operations (submit, cancel, match)
//! - Settlement operations
//! - ERC (Energy Renewable Certificate) operations
//! - Market data and order book metrics
//! - Blockchain operations

use metrics::{counter, gauge, histogram};

use std::time::Instant;

/// Records order submission metrics
pub fn record_order_submission(order_type: &str, side: &str, success: bool, duration_ms: f64) {
    counter!("trading_orders_submitted_total",
        "type" => order_type.to_string(),
        "side" => side.to_string(),
        "success" => success.to_string()
    ).increment(1);

    histogram!("trading_order_submission_duration_ms",
        "type" => order_type.to_string(),
        "side" => side.to_string()
    ).record(duration_ms);
}

/// Records order cancellation metrics
pub fn record_order_cancellation(success: bool, duration_ms: f64) {
    counter!("trading_orders_cancelled_total",
        "success" => success.to_string()
    ).increment(1);

    histogram!("trading_order_cancellation_duration_ms").record(duration_ms);
}

/// Records order matching metrics
pub fn record_order_match(
    order_type: &str,
    quantity: f64,
    price: f64,
    zone_id: &str,
) {
    counter!("trading_orders_matched_total",
        "type" => order_type.to_string(),
        "zone" => zone_id.to_string()
    ).increment(1);

    histogram!("trading_order_match_quantity").record(quantity);
    histogram!("trading_order_match_price").record(price);
}

/// Records settlement operation metrics
pub fn record_settlement(operation: &str, success: bool, duration_ms: f64) {
    counter!("trading_settlements_total",
        "operation" => operation.to_string(),
        "success" => success.to_string()
    ).increment(1);

    histogram!("trading_settlement_duration_ms",
        "operation" => operation.to_string()
    ).record(duration_ms);

    if !success {
        counter!("trading_settlement_failures_total",
            "operation" => operation.to_string()
        ).increment(1);
    }
}

/// Records ERC (Energy Renewable Certificate) operation metrics
pub fn record_erc_operation(operation: &str, success: bool, duration_ms: f64) {
    counter!("trading_erc_operations_total",
        "operation" => operation.to_string(),
        "success" => success.to_string()
    ).increment(1);

    histogram!("trading_erc_operation_duration_ms",
        "operation" => operation.to_string()
    ).record(duration_ms);
}

/// Records ERC issuance metrics
pub fn record_erc_issuance(amount: f64, success: bool, duration_ms: f64) {
    counter!("trading_erc_issued_total",
        "success" => success.to_string()
    ).increment(1);

    if success {
        histogram!("trading_erc_issued_amount").record(amount);
    }

    histogram!("trading_erc_issuance_duration_ms").record(duration_ms);
}

/// Records ERC transfer metrics
pub fn record_erc_transfer(amount: f64, success: bool, duration_ms: f64) {
    counter!("trading_erc_transferred_total",
        "success" => success.to_string()
    ).increment(1);

    if success {
        histogram!("trading_erc_transferred_amount").record(amount);
    }

    histogram!("trading_erc_transfer_duration_ms").record(duration_ms);
}

/// Records ERC retirement metrics
pub fn record_erc_retirement(amount: f64, success: bool, duration_ms: f64) {
    counter!("trading_erc_retired_total",
        "success" => success.to_string()
    ).increment(1);

    if success {
        histogram!("trading_erc_retired_amount").record(amount);
    }

    histogram!("trading_erc_retirement_duration_ms").record(duration_ms);
}

/// Records order book depth metrics
pub fn record_order_book_depth(zone_id: &str, bids: u64, asks: u64) {
    gauge!("trading_orderbook_bids_depth", "zone" => zone_id.to_string()).set(bids as f64);
    gauge!("trading_orderbook_asks_depth", "zone" => zone_id.to_string()).set(asks as f64);
}

/// Records spread metrics
pub fn record_spread(zone_id: &str, spread_bps: f64) {
    gauge!("trading_spread_bps", "zone" => zone_id.to_string()).set(spread_bps);
}

/// Records matching engine cycle metrics
pub fn record_matching_cycle(duration_ms: f64, orders_processed: u64, matches: u64) {
    histogram!("trading_matching_cycle_duration_ms").record(duration_ms);
    histogram!("trading_matching_orders_per_cycle").record(orders_processed as f64);
    histogram!("trading_matching_matches_per_cycle").record(matches as f64);
}


/// Records blockchain settlement metrics
pub fn record_blockchain_settlement(operation: &str, success: bool, duration_ms: f64) {
    counter!("trading_blockchain_settlements_total",
        "operation" => operation.to_string(),
        "success" => success.to_string()
    ).increment(1);

    histogram!("trading_blockchain_settlement_duration_ms",
        "operation" => operation.to_string()
    ).record(duration_ms);

    if !success {
        counter!("trading_blockchain_settlement_failures_total",
            "operation" => operation.to_string()
        ).increment(1);
    }
}


/// Records gRPC request metrics for trading service
pub struct GrpcMetricsTimer {
    start: Instant,
    method: String,
}

impl GrpcMetricsTimer {
    pub fn new(method: &str) -> Self {
        let start = Instant::now();
        gauge!("trading_grpc_requests_in_flight", "method" => method.to_string()).increment(1.0);
        Self { start, method: method.to_string() }
    }

    pub fn finish(self, success: bool) {
        let duration = self.start.elapsed();
        let duration_secs = duration.as_secs_f64();
        
        gauge!("trading_grpc_requests_in_flight", "method" => self.method.clone()).decrement(1.0);
        
        counter!("trading_grpc_requests_total",
            "method" => self.method.clone(),
            "success" => success.to_string()
        ).increment(1);

        histogram!("trading_grpc_request_duration_seconds",
            "method" => self.method.clone(),
            "success" => success.to_string()
        ).record(duration_secs);
    }
}

/// Records active connections gauge
pub fn record_active_connections(count: u64) {
    gauge!("trading_active_connections").set(count as f64);
}

/// Records event bus metrics
pub fn record_event_published(event_type: &str) {
    counter!("trading_events_published_total",
        "type" => event_type.to_string()
    ).increment(1);
}

/// Records event consumption metrics
pub fn record_event_consumed(event_type: &str, success: bool, duration_ms: f64) {
    counter!("trading_events_consumed_total",
        "type" => event_type.to_string(),
        "success" => success.to_string()
    ).increment(1);

    histogram!("trading_event_consumption_duration_ms",
        "type" => event_type.to_string()
    ).record(duration_ms);
}

/// Records market data update metrics
pub fn record_market_data_update(zone_id: &str, price: f64) {
    counter!("trading_market_data_updates_total",
        "zone" => zone_id.to_string()
    ).increment(1);

    gauge!("trading_market_price", "zone" => zone_id.to_string()).set(price);
}

/// Records VPP aggregation metrics
pub fn record_vpp_aggregation(cluster_id: &str, duration_ms: f64, success: bool) {
    counter!("trading_vpp_aggregation_total",
        "cluster" => cluster_id.to_string(),
        "success" => success.to_string()
    ).increment(1);

    histogram!("trading_vpp_aggregation_duration_ms",
        "cluster" => cluster_id.to_string()
    ).record(duration_ms);
}

/// Records VPP cluster SOC
pub fn record_vpp_cluster_soc(cluster_id: &str, soc: f64) {
    gauge!("trading_vpp_cluster_soc", "cluster" => cluster_id.to_string()).set(soc);
}

/// Records DCA (Recurring Order) evaluation metrics
pub fn record_dca_evaluation(duration_ms: f64, orders_evaluated: u64, orders_promoted: u64) {
    histogram!("trading_dca_evaluation_duration_ms").record(duration_ms);
    counter!("trading_dca_orders_evaluated_total").increment(orders_evaluated);
    counter!("trading_dca_orders_promoted_total").increment(orders_promoted);
}

/// Records Market Data (OHLC) upsert metrics
pub fn record_market_candle_update(zone_id: i32, duration_ms: f64) {
    histogram!("trading_market_candle_upsert_duration_ms",
        "zone" => zone_id.to_string()
    ).record(duration_ms);
}

