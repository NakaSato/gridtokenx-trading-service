// Order Book Depth Snapshots
//
// Aggregates bid/ask levels into a compact depth snapshot
// for WebSocket broadcasting and caching.

use rust_decimal::Decimal;
use serde::Serialize;
use tracing::info;
use chrono::{Utc, DateTime};

use super::MarketClearingService;

/// A single aggregated price level in the depth snapshot
#[derive(Debug, Clone, Serialize)]
pub struct DepthLevel {
    /// Price per kWh at this level
    pub price: Decimal,
    /// Aggregated volume (sum of all orders at this price)
    pub volume: Decimal,
    /// Number of orders contributing to this level
    pub order_count: i64,
}

/// Complete order book depth snapshot
#[derive(Debug, Clone, Serialize)]
pub struct OrderBookDepthSnapshot {
    /// Top N bid (buy) levels sorted by price descending
    pub bids: Vec<DepthLevel>,
    /// Top N ask (sell) levels sorted by price ascending
    pub asks: Vec<DepthLevel>,
    /// Timestamp of the snapshot
    pub timestamp: DateTime<Utc>,
    /// Best bid price
    pub best_bid: Option<Decimal>,
    /// Best ask price
    pub best_ask: Option<Decimal>,
    /// Spread percentage ((ask - bid) / bid * 100)
    pub spread_pct: Option<Decimal>,
    /// Total buy volume across all levels
    pub total_bid_volume: Decimal,
    /// Total sell volume across all levels
    pub total_ask_volume: Decimal,
    /// Mid-market price (average of best bid and ask)
    pub mid_price: Option<Decimal>,
}

impl MarketClearingService {
    /// Capture an order book depth snapshot aggregated by price level.
    ///
    /// Groups active orders by `price_per_kwh`, sums remaining volume,
    /// returns top `depth` levels for each side.
    pub async fn capture_depth_snapshot(&self, depth: usize) -> anyhow::Result<OrderBookDepthSnapshot> {
        // Aggregate bids (buy side): price DESC, sum remaining volume
        let bid_rows = sqlx::query_as::<_, (Decimal, Decimal, i64)>(
            r#"
            SELECT
                price_per_kwh,
                SUM(energy_amount - COALESCE(filled_amount, 0)) as volume,
                COUNT(*) as order_count
            FROM trading_orders
            WHERE side = 'buy'::order_side
              AND status IN ('pending', 'active', 'partially_filled')
              AND (energy_amount - COALESCE(filled_amount, 0)) > 0
            GROUP BY price_per_kwh
            ORDER BY price_per_kwh DESC
            LIMIT $1
            "#,
        )
        .bind(depth as i64)
        .fetch_all(&self.db)
        .await?;

        let bids: Vec<DepthLevel> = bid_rows
            .into_iter()
            .map(|(price, volume, count)| DepthLevel {
                price,
                volume,
                order_count: count,
            })
            .collect();

        // Aggregate asks (sell side): price ASC, sum remaining volume
        let ask_rows = sqlx::query_as::<_, (Decimal, Decimal, i64)>(
            r#"
            SELECT
                price_per_kwh,
                SUM(energy_amount - COALESCE(filled_amount, 0)) as volume,
                COUNT(*) as order_count
            FROM trading_orders
            WHERE side = 'sell'::order_side
              AND status IN ('pending', 'active', 'partially_filled')
              AND (energy_amount - COALESCE(filled_amount, 0)) > 0
            GROUP BY price_per_kwh
            ORDER BY price_per_kwh ASC
            LIMIT $1
            "#,
        )
        .bind(depth as i64)
        .fetch_all(&self.db)
        .await?;

        let asks: Vec<DepthLevel> = ask_rows
            .into_iter()
            .map(|(price, volume, count)| DepthLevel {
                price,
                volume,
                order_count: count,
            })
            .collect();

        // Derive aggregate statistics
        let best_bid = bids.first().map(|l| l.price);
        let best_ask = asks.first().map(|l| l.price);
        let total_bid_volume: Decimal = bids.iter().map(|l| l.volume).sum();
        let total_ask_volume: Decimal = asks.iter().map(|l| l.volume).sum();

        let mid_price = match (best_bid, best_ask) {
            (Some(bid), Some(ask)) => Some((bid + ask) / Decimal::from(2)),
            _ => None,
        };

        let spread_pct = match (best_bid, best_ask) {
            (Some(bid), Some(ask)) if bid > Decimal::ZERO => {
                Some(((ask - bid) / bid) * Decimal::from(100))
            }
            _ => None,
        };

        let snapshot = OrderBookDepthSnapshot {
            bids,
            asks,
            timestamp: Utc::now(),
            best_bid,
            best_ask,
            spread_pct,
            total_bid_volume,
            total_ask_volume,
            mid_price,
        };

        info!(
            "📊 Depth snapshot: {} bid levels, {} ask levels, spread={:?}%",
            snapshot.bids.len(),
            snapshot.asks.len(),
            snapshot.spread_pct,
        );

        Ok(snapshot)
    }

    /// Capture and broadcast a depth snapshot via the existing WebSocket infrastructure.
    pub async fn broadcast_depth_snapshot_full(&self) -> anyhow::Result<()> {
        let snapshot = self.capture_depth_snapshot(20).await?;

        // Convert to the WebSocket PriceLevel format
        let ws_bids: Vec<(String, String)> = snapshot.bids.iter()
            .map(|l| (l.price.to_string(), l.volume.to_string()))
            .collect();
        let ws_asks: Vec<(String, String)> = snapshot.asks.iter()
            .map(|l| (l.price.to_string(), l.volume.to_string()))
            .collect();

        self.websocket_service.broadcast_order_book_snapshot(
            ws_bids,
            ws_asks,
            snapshot.best_bid.map(|p| p.to_string()),
            snapshot.best_ask.map(|p| p.to_string()),
            snapshot.mid_price.map(|p| p.to_string()),
            snapshot.spread_pct.map(|p| format!("{:.2}", p)),
        ).await;

        info!("📡 Full depth snapshot broadcast to all clients");
        Ok(())
    }
}
