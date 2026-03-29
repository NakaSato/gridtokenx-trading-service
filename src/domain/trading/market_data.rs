//! Market Data Sync Service
//!
//! Domain service that aggregates market depth from the database
//! and synchronizes it with the Solana blockchain.

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use solana_sdk::pubkey::Pubkey;
use sqlx::{PgPool, Row};
use std::str::FromStr;
use std::sync::Arc;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info};

use crate::infra::blockchain::BlockchainService;
use crate::infra::db::schema::types::OrderSide;

/// Market data sync configuration
#[derive(Debug, Clone)]
pub struct MarketDataConfig {
    /// How often to sync market depth (in seconds)
    pub sync_interval_secs: u64,
    /// Whether the sync is enabled
    pub enabled: bool,
    /// Trading Program ID
    pub trading_program_id: String,
}

impl Default for MarketDataConfig {
    fn default() -> Self {
        Self {
            sync_interval_secs: 30, // Sync every 30s by default
            enabled: true,
            trading_program_id: "69dGpKu9a8EZiZ7orgfTH6CoGj9DeQHHkHBF2exSr8na".to_string(),
        }
    }
}

/// Market data service (Domain Layer)
#[derive(Clone)]
pub struct MarketDataManager {
    db: PgPool,
    blockchain: BlockchainService,
    pub config: MarketDataConfig,
    market_pubkey: Pubkey,
}

impl MarketDataManager {
    pub fn new(db: PgPool, blockchain: BlockchainService, config: MarketDataConfig) -> Self {
        let program_id = Pubkey::from_str(&config.trading_program_id).unwrap_or_default();
        let (market_pubkey, _) = Pubkey::find_program_address(&[b"market"], &program_id);

        Self {
            db,
            blockchain,
            config,
            market_pubkey,
        }
    }

    /// Start the market data sync loop
    pub async fn start(self: Arc<Self>) {
        if !self.config.enabled {
            info!("Market data sync is disabled");
            return;
        }

        info!(
            "Starting market data sync with {}s interval",
            self.config.sync_interval_secs
        );

        let mut sync_interval = interval(Duration::from_secs(self.config.sync_interval_secs));

        loop {
            sync_interval.tick().await;

            if let Err(e) = self.sync_market_depth().await {
                error!("Market depth sync error: {}", e);
            }
        }
    }

    /// Aggregate market depth from DB and sync to blockchain
    pub async fn sync_market_depth(&self) -> anyhow::Result<()> {
        if !self.config.enabled {
            debug!("Market data sync is disabled, skipping depth sync");
            return Ok(());
        }

        // Find all active zones
        let zones_rows =
            sqlx::query("SELECT DISTINCT zone_id FROM trading_orders WHERE zone_id IS NOT NULL")
                .fetch_all(&self.db)
                .await?;

        for row in zones_rows {
            let zone_id: i32 = row.get(0);
            if let Err(e) = self.sync_zone_depth(zone_id as u32).await {
                error!("Error syncing depth for zone {}: {}", zone_id, e);
            }
        }

        Ok(())
    }

    async fn sync_zone_depth(&self, zone_id: u32) -> anyhow::Result<()> {
        debug!("Syncing market depth for zone {}", zone_id);

        let buy_depth = self.get_aggregated_depth(zone_id, OrderSide::Buy).await?;
        let sell_depth = self.get_aggregated_depth(zone_id, OrderSide::Sell).await?;

        let buy_prices: Vec<u64> = buy_depth.iter().map(|(p, _)| *p).collect();
        let buy_amounts: Vec<u64> = buy_depth.iter().map(|(_, a)| *a).collect();
        let sell_prices: Vec<u64> = sell_depth.iter().map(|(p, _)| *p).collect();
        let sell_amounts: Vec<u64> = sell_depth.iter().map(|(_, a)| *a).collect();

        if buy_prices.is_empty() && sell_prices.is_empty() {
            return Ok(());
        }

        info!(
            "Updating on-chain depth for zone {}: {} bid levels, {} ask levels",
            zone_id,
            buy_prices.len(),
            sell_prices.len()
        );

        self.blockchain
            .execute_update_depth(
                &self.market_pubkey,
                zone_id,
                buy_prices,
                buy_amounts,
                sell_prices,
                sell_amounts,
            )
            .await?;

        Ok(())
    }

    async fn get_aggregated_depth(
        &self,
        zone_id: u32,
        side: OrderSide,
    ) -> anyhow::Result<Vec<(u64, u64)>> {
        let rows = sqlx::query(
            r#"
            SELECT price_per_kwh, SUM(energy_amount - COALESCE(filled_amount, 0)) as remaining_amount
            FROM trading_orders
            WHERE status IN ('active', 'partially_filled')
              AND side = $1
              AND zone_id = $2
            GROUP BY price_per_kwh
            ORDER BY price_per_kwh DESC
            LIMIT 20
            "#
        )
        .bind(side)
        .bind(zone_id as i32)
        .fetch_all(&self.db)
        .await?;

        let mut depth = Vec::new();
        for row in rows {
            let price_dec: Decimal = row.get("price_per_kwh");
            let amount_dec: Decimal = row.get("remaining_amount");

            let price = (price_dec * Decimal::from(1_000_000_000u64))
                .to_u64()
                .unwrap_or(0);
            let amount = (amount_dec * Decimal::from(1_000_000_000u64))
                .to_u64()
                .unwrap_or(0);

            if amount > 0 {
                depth.push((price, amount));
            }
        }

        if side == OrderSide::Sell {
            depth.sort_by_key(|&(p, _)| p);
        }

        Ok(depth)
    }

    /// Explicitly sync price history after a trade
    pub async fn sync_price_history(&self, price: Decimal, amount: Decimal) -> anyhow::Result<()> {
        if !self.config.enabled {
            debug!("Market data sync is disabled, skipping price history sync");
            return Ok(());
        }

        let price_u64 = (price * Decimal::from(1_000_000_000u64))
            .to_u64()
            .unwrap_or(0);
        let amount_u64 = (amount * Decimal::from(1_000_000_000u64))
            .to_u64()
            .unwrap_or(0);

        info!(
            "Syncing price history on-chain: price={}, volume={}",
            price, amount
        );

        self.blockchain
            .execute_update_price_history(&self.market_pubkey, price_u64, amount_u64)
            .await?;

        Ok(())
    }
}
