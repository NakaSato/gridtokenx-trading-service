pub mod blockchain;
pub mod depth;
pub mod epoch;
pub mod escrow;
pub mod matching;
pub mod orders;
pub mod revenue;
pub mod types;

use crate::core::config::Config;
use crate::infra::db::DatabasePool;
use chrono::{Timelike, Utc};
use rust_decimal::Decimal;
use std::sync::Arc;

pub use types::*;

use crate::infra::blockchain::{BlockchainService, WalletService};
use crate::services::erc::ErcService;

#[derive(Clone, Debug)]
pub struct MarketClearingService {
    db: DatabasePool,
    config: Arc<Config>,
    blockchain_service: Arc<BlockchainService>,
    #[allow(dead_code)]
    wallet_service: WalletService,
    erc_service: Arc<ErcService>,
    settlement_service: Option<crate::services::SettlementService>,
    websocket_service: WebSocketService,
}

#[derive(Clone, Debug)]
pub struct WebSocketService;
impl WebSocketService {
    pub async fn broadcast_order_created(
        &self,
        _id: String,
        _amt: Decimal,
        _price: Decimal,
        _type: Option<String>,
        _user: String,
    ) {
    }
    pub async fn broadcast_order_book_snapshot(
        &self,
        _bids: Vec<(String, String)>,
        _asks: Vec<(String, String)>,
        _best_bid: Option<String>,
        _best_ask: Option<String>,
        _mid_price: Option<String>,
        _spread: Option<String>,
    ) {
    }
}

impl MarketClearingService {
    pub fn new(
        db: DatabasePool,
        config: Arc<Config>,
        blockchain_service: Arc<BlockchainService>,
        wallet_service: WalletService,
        erc_service: Arc<ErcService>,
    ) -> Self {
        Self {
            db,
            config,
            blockchain_service,
            wallet_service,
            erc_service,
            settlement_service: None,
            websocket_service: WebSocketService,
        }
    }

    /// Set the settlement service for processing matched trades
    pub fn with_settlement(mut self, settlement: crate::services::SettlementService) -> Self {
        self.settlement_service = Some(settlement);
        self
    }

    /// Set the P2P config service (Deprecated in microservice)
    pub fn with_p2p_config(self) -> Self {
        self
    }

    /// Get a Time-of-Use (TOU) pricing multiplier based on the current hour.
    ///
    /// This now also incorporates the dynamic `pricing.market_multiplier` from config.
    pub async fn get_tou_multiplier(&self) -> (Decimal, &'static str) {
        let hour = Utc::now().hour();
        let (base_mult, period) = Self::get_tou_multiplier_for_hour(hour);

        // Placeholder for market multiplier (previously from P2PConfigService)
        let market_mult = Decimal::ONE;

        (base_mult * market_mult, period)
    }

    /// Internal helper for TOU multiplier calculation (facilitates testing)
    pub fn get_tou_multiplier_for_hour(hour: u32) -> (Decimal, &'static str) {
        match hour {
            9..=13 | 18..=20 => (
                Decimal::from_str_exact("1.15").unwrap_or(Decimal::ONE),
                "peak",
            ),
            7..=8 | 14..=17 => (
                Decimal::from_str_exact("1.05").unwrap_or(Decimal::ONE),
                "shoulder",
            ),
            _ => (
                Decimal::from_str_exact("0.90").unwrap_or(Decimal::ONE),
                "off_peak",
            ),
        }
    }

    /// Calculate market clearing price from order book
    /// Uses midpoint of bid-ask spread where supply meets demand
    pub fn calculate_clearing_price(
        buy_orders: &[OrderBookEntry],
        sell_orders: &[OrderBookEntry],
    ) -> Option<ClearingPrice> {
        if buy_orders.is_empty() || sell_orders.is_empty() {
            return None;
        }

        // Get best bid (highest buy price) and best ask (lowest sell price)
        let best_bid = buy_orders.iter().map(|o| o.price_per_kwh).max()?;
        let best_ask = sell_orders.iter().map(|o| o.price_per_kwh).min()?;

        // No clearing price if bid < ask (no overlap)
        if best_bid < best_ask {
            return None;
        }

        // Calculate clearing price as midpoint
        let clearing_price = (best_bid + best_ask) / Decimal::from(2);

        // Calculate clearable volume (sum of orders that can trade)
        let buy_volume: Decimal = buy_orders
            .iter()
            .filter(|o| o.price_per_kwh >= best_ask)
            .map(|o| o.energy_amount)
            .sum();
        let sell_volume: Decimal = sell_orders
            .iter()
            .filter(|o| o.price_per_kwh <= best_bid)
            .map(|o| o.energy_amount)
            .sum();
        let clearable_volume = buy_volume.min(sell_volume);

        Some(ClearingPrice {
            price: clearing_price,
            volume: clearable_volume,
            buy_orders_count: buy_orders.len(),
            sell_orders_count: sell_orders.len(),
            best_bid,
            best_ask,
        })
    }

    /// Fetch latest market data and broadcast depth update (Stubbed for microservice)
    pub async fn broadcast_depth_update(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
