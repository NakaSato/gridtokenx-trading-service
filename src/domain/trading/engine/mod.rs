pub mod fast_decimal;
pub mod rehydration;
pub mod types;

use anyhow::Result;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use sqlx::{PgPool, Row};
use tracing::{debug, error, info, warn};

fn track_order_matched(_strategy: &str, _amount: f64) {}
use crate::domain::trading::engine::fast_decimal::FastPrice;
use crate::metrics::record_matching_cycle;
use solana_sdk::pubkey::Pubkey;
use uuid::Uuid;

use chrono::{Timelike, Utc};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use ulid::Ulid;
use tokio_util::sync::CancellationToken;

use crate::utils::numeric::to_u64_atomic;

use crate::{
    domain::trading::market_data::MarketDataManager as MarketDataService,
    domain::trading::{
        clearing::MarketClearingService,
        models::{TradingOrderDb, TriggerStatus, TriggerType},
        settlement::Settlement,
    },
    domain::{
        energy::GridTopologyService,
        events::{Event, OrderMatchedPayload},
    },
    infra::db::schema::types::{OrderSide, OrderStatus},
    infra::{blockchain::BlockchainService, events::EventBus},
    services::p2p_config::P2PConfigService,
    services::settlement::SettlementService,
};

use dashmap::DashMap;

/// Events to be persisted to the database asynchronously
pub struct ShardedMatchingWorker {
    shard_id: u32,
    num_shards: u32,
    db: PgPool,
    settlement: Option<SettlementService>,
    market_clearing: Option<MarketClearingService>,
    blockchain_service: Option<BlockchainService>,
    grid_topology: Arc<GridTopologyService>,
    market_data_service: Option<MarketDataService>,
    event_bus: Option<EventBus>,
    match_interval_secs: u64,
    p2p_config: Option<Arc<P2PConfigService>>,
    buy_orders: Arc<DashMap<Uuid, TradingOrderDb>>,
    sell_orders: Arc<DashMap<Uuid, TradingOrderDb>>,
    trigger_orders: Arc<DashMap<Uuid, TradingOrderDb>>,
    zone_prices: Arc<DashMap<i32, Decimal>>,
    ohlc_aggregator: Option<Arc<crate::services::market_data::MarketDataService>>,
}
}

impl ShardedMatchingWorker {
    pub fn new(
        shard_id: u32,
        num_shards: u32,
        db: PgPool,
        match_interval_secs: u64,
        grid_topology: Arc<GridTopologyService>,
    ) -> Self {
        Self {
            shard_id,
            num_shards,
            db,
            match_interval_secs,
            grid_topology,
            market_data_service: None,
            settlement: None,
            market_clearing: None,
            blockchain_service: None,
            event_bus: None,
            p2p_config: None,
            buy_orders: Arc::new(DashMap::new()),
            sell_orders: Arc::new(DashMap::new()),
            trigger_orders: Arc::new(DashMap::new()),
            zone_prices: Arc::new(DashMap::new()),
            market_data_aggregator: None,
        }
    }

    pub fn with_ohlc_aggregator(mut self, aggregator: Arc<crate::services::market_data::MarketDataService>) -> Self {
        self.ohlc_aggregator = Some(aggregator);
        self
    }

    async fn bootstrap_orders(&self) -> Result<()> {
        // Using local type aliases if needed

        info!(
            "Shard {} bootstrapping in-memory order book...",
            self.shard_id
        );

        let active_orders_rows = sqlx::query(
            r#"
            SELECT 
                id, user_id, energy_amount, price_per_kwh, filled_amount,
                epoch_id, zone_id, order_type, side, status,
                expires_at, created_at, filled_at, meter_id,
                refund_tx_signature, order_pda, order_index, session_token,
                trigger_price, trigger_type, trigger_status,
                trailing_offset, triggered_at, last_peak_price,
                blockchain_status, blockchain_tx_hash, blockchain_error, retry_count,
                time_in_force
            FROM trading_orders
            WHERE status IN ('pending', 'active', 'partially_filled')
            AND (COALESCE(zone_id, 0) % $1) = $2
            "#,
        )
        .bind(self.num_shards as i32)
        .bind(self.shard_id as i32)
        .fetch_all(&self.db)
        .await?;

        for row in active_orders_rows {
            let order = TradingOrderDb {
                id: row.get("id"),
                user_id: row.get("user_id"),
                energy_amount: row.get("energy_amount"),
                price_per_kwh: row.get("price_per_kwh"),
                filled_amount: row.get("filled_amount"),
                epoch_id: row.get("epoch_id"),
                zone_id: row.get("zone_id"),
                order_type: row.get("order_type"),
                side: row.get("side"),
                status: row.get("status"),
                expires_at: row.get("expires_at"),
                created_at: row.get("created_at"),
                filled_at: row.get("filled_at"),
                meter_id: row.get("meter_id"),
                refund_tx_signature: row.get("refund_tx_signature"),
                order_pda: row.get("order_pda"),
                order_index: row.get("order_index"),
                session_token: row.get("session_token"),
                trigger_price: row.get("trigger_price"),
                trigger_type: row.get("trigger_type"),
                trigger_status: row.get("trigger_status"),
                trailing_offset: row.get("trailing_offset"),
                triggered_at: row.get("triggered_at"),
                last_peak_price: row.get("last_peak_price"),
                blockchain_status: row.get("blockchain_status"),
                blockchain_tx_hash: row.get("blockchain_tx_hash"),
                blockchain_error: row.get("blockchain_error"),
                retry_count: row.get("retry_count"),
                time_in_force: row.get("time_in_force"),
            };

            if (order.trigger_price.is_some() || order.trigger_type.is_some())
                && order.trigger_status == Some(TriggerStatus::Pending)
            {
                self.trigger_orders.insert(order.id, order);
            } else if order.side == OrderSide::Buy {
                self.buy_orders.insert(order.id, order);
            } else {
                self.sell_orders.insert(order.id, order);
            }
        }

        info!(
            "Shard {} bootstrapped {} buy, {} sell, and {} trigger orders",
            self.shard_id,
            self.buy_orders.len(),
            self.sell_orders.len(),
            self.trigger_orders.len()
        );
        Ok(())
    }

    /// Seed the worker with rehydrated orders from Kafka.
    pub fn seed_orders(&self, orders: Vec<TradingOrderDb>) {
        let mut b = 0;
        let mut s = 0;
        let mut t = 0;

        for order in orders {
            if (order.trigger_price.is_some() || order.trigger_type.is_some())
                && order.trigger_status == Some(TriggerStatus::Pending)
            {
                self.trigger_orders.insert(order.id, order);
                t += 1;
            } else if order.side == OrderSide::Buy {
                self.buy_orders.insert(order.id, order);
                b += 1;
            } else {
                self.sell_orders.insert(order.id, order);
                s += 1;
            }
        }

        if b > 0 || s > 0 || t > 0 {
            info!(
                "Shard {} seeded with {} buy, {} sell, and {} trigger orders from rehydration",
                self.shard_id, b, s, t
            );
        }
    }

    pub async fn run(
        &self,
        running: Arc<RwLock<bool>>,
        token: CancellationToken,
        mut notify_rx: tokio::sync::mpsc::Receiver<Option<TradingOrderDb>>,
    ) {
        info!(
            "Worker Shard {} started (Managing zones % {} == {})",
            self.shard_id, self.num_shards, self.shard_id
        );

        // Bootstrap in-memory state
        if let Err(e) = self.bootstrap_orders().await {
            error!("Shard {} failed to bootstrap orders: {}", self.shard_id, e);
        }

        let mut interval = tokio::time::interval(Duration::from_secs(self.match_interval_secs));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Periodic match
                }
                Some(order_opt) = notify_rx.recv() => {
                    // Reactive match triggered by new order
                    if let Some(order) = order_opt {
                        debug!("Worker Shard {} received new order reactive injection: {}", self.shard_id, order.id);
                        if order.trigger_price.is_some() || order.trigger_type.is_some() {
                             self.trigger_orders.insert(order.id, order);
                        } else if order.side == OrderSide::Buy {
                            self.buy_orders.insert(order.id, order);
                        } else {
                            self.sell_orders.insert(order.id, order);
                        }
                    } else {
                        debug!("Worker Shard {} triggered by manual notification", self.shard_id);
                    }
                }
                _ = token.cancelled() => {
                    info!("🔄 Worker Shard {} shutting down...", self.shard_id);
                    break;
                }
                else => break,
            }

            {
                let is_running = running.read().await;
                if !*is_running {
                    break;
                }
            }

            if let Err(e) = self.match_shard_cycle().await {
                error!("Error in worker shard {}: {}", self.shard_id, e);
            }
        }
    }

    async fn check_triggers(&self) -> Result<()> {
        let mut triggered_orders = Vec::new();

        for entry in self.trigger_orders.iter() {
            let order = entry.value();
            let zone_id = order.zone_id.unwrap_or(0);

            if let Some(last_price) = self.zone_prices.get(&zone_id) {
                let last_price = *last_price;
                let trigger_price = order.trigger_price.unwrap_or(Decimal::ZERO);

                let is_triggered = match order.trigger_type.as_ref() {
                    Some(TriggerType::StopLoss) => match order.side {
                        OrderSide::Sell => {
                            let triggered = last_price <= trigger_price;
                            debug!(
                                "Checking Sell StopLoss: price {} <= trigger {}? {}",
                                last_price, trigger_price, triggered
                            );
                            triggered
                        }
                        OrderSide::Buy => {
                            let triggered = last_price >= trigger_price;
                            debug!(
                                "Checking Buy StopLoss: price {} >= trigger {}? {}",
                                last_price, trigger_price, triggered
                            );
                            triggered
                        }
                    },
                    Some(TriggerType::TakeProfit) => match order.side {
                        OrderSide::Sell => {
                            let triggered = last_price >= trigger_price;
                            debug!(
                                "Checking Sell TakeProfit: price {} >= trigger {}? {}",
                                last_price, trigger_price, triggered
                            );
                            triggered
                        }
                        OrderSide::Buy => {
                            let triggered = last_price <= trigger_price;
                            debug!(
                                "Checking Buy TakeProfit: price {} <= trigger {}? {}",
                                last_price, trigger_price, triggered
                            );
                            triggered
                        }
                    },
                    Some(TriggerType::TrailingStop) => {
                        let mut peak = order.last_peak_price.unwrap_or(last_price);
                        let offset = order.trailing_offset.unwrap_or(Decimal::ZERO);
                        let mut peak_updated = false;

                        match order.side {
                            OrderSide::Sell => {
                                if last_price > peak || order.last_peak_price.is_none() {
                                    peak = last_price;
                                    peak_updated = true;
                                }
                                let effective_trigger = peak - offset;
                                debug!("Checking Sell TrailingStop: price {} <= trigger {} (Peak: {})? {}", 
                                    last_price, effective_trigger, peak, last_price <= effective_trigger);

                                if peak_updated {
                                    if let Some(event_bus) = &self.event_bus {
                                        let event = Event::PeakPriceUpdate {
                                            id: order.id,
                                            peak_price: peak,
                                        };
                                        let bus = event_bus.clone();
                                        tokio::spawn(async move {
                                            let _ = bus.publish(&event).await;
                                        });
                                    }
                                }

                                last_price <= effective_trigger
                            }
                            OrderSide::Buy => {
                                if last_price < peak || order.last_peak_price.is_none() {
                                    peak = last_price;
                                    peak_updated = true;
                                }
                                let effective_trigger = peak + offset;
                                debug!("Checking Buy TrailingStop: price {} >= trigger {} (Peak: {})? {}", 
                                    last_price, effective_trigger, peak, last_price >= effective_trigger);

                                if peak_updated {
                                    if let Some(event_bus) = &self.event_bus {
                                        let event = Event::PeakPriceUpdate {
                                            id: order.id,
                                            peak_price: peak,
                                        };
                                        let bus = event_bus.clone();
                                        tokio::spawn(async move {
                                            let _ = bus.publish(&event).await;
                                        });
                                    }
                                }

                                last_price >= effective_trigger
                            }
                        }
                    }
                    _ => false,
                };

                if is_triggered {
                    triggered_orders.push(order.id);
                }
            }
        }

        // Apply peak updates to memory
        /*
                for (order_id, peak) in peak_updates {
                    if let Some(mut order) = self.trigger_orders.get_mut(&order_id) {
                        order.last_peak_price = Some(peak);
                    }
                }
        */

        for order_id in triggered_orders {
            if let Some((_, mut order)) = self.trigger_orders.remove(&order_id) {
                info!(
                    "🚀 Triggered order {}: type={:?}, price={}",
                    order_id,
                    order.trigger_type,
                    order.trigger_price.unwrap_or_default()
                );

                order.status = OrderStatus::Active;
                order.trigger_status = Some(TriggerStatus::Triggered);
                order.triggered_at = Some(chrono::Utc::now());

                // Inject into matching pools
                if order.side == OrderSide::Buy {
                    self.buy_orders.insert(order.id, order.clone());
                } else {
                    self.sell_orders.insert(order.id, order.clone());
                }

                // Async DB Update (via EventBus)
                if let Some(event_bus) = &self.event_bus {
                    if let Some(triggered_ts) = order.triggered_at {
                        let event = Event::TriggerExecution {
                            id: order.id,
                            triggered_at: triggered_ts,
                        };
                        let bus = event_bus.clone();
                        tokio::spawn(async move {
                            let _ = bus.publish(&event).await;
                        });
                    }
                }
            }
        }

        Ok(())
    }

    async fn get_dynamic_multiplier(&self) -> Decimal {
        let hour = Utc::now().hour();
        let (base_mult, _) = MarketClearingService::get_tou_multiplier_for_hour(hour);

        let market_mult = if let Some(p2p) = &self.p2p_config {
            Decimal::from_f64(p2p.get_f64("pricing.market_multiplier").await.unwrap_or(1.0))
                .unwrap_or(Decimal::ONE)
        } else {
            Decimal::ONE
        };

        base_mult * market_mult
    }

    async fn match_shard_cycle(&self) -> Result<usize> {
        let cycle_start = std::time::Instant::now();
        let buy_count = self.buy_orders.len();
        let sell_count = self.sell_orders.len();
        info!(
            "Shard {} matching cycle started (Pools: {} buy, {} sell)",
            self.shard_id, buy_count, sell_count
        );

        // [B4] Event batch collector to avoid individual tokio::spawn overhead
        let mut match_events: Vec<crate::domain::events::Event> = Vec::new();

        // Evaluate triggers before matching
        if let Err(e) = self.check_triggers().await {
            error!("Shard {} trigger check failed: {}", self.shard_id, e);
        }

        // 1. Reset grid flows and sync config for this cycle
        if self.shard_id == 0 {
            self.grid_topology.reset_flows().await;
        }

        // [B2] Take a single topology snapshot for the entire cycle (lock-free after this)
        let topo_snapshot = self.grid_topology.snapshot().await;

        // [B6] Hoist dynamic multiplier outside sell-candidate loop
        let dynamic_mult_dec = self.get_dynamic_multiplier().await;
        // [PHASE 3] Convert to FastPrice (i128) once per cycle
        let dynamic_mult = FastPrice::from(dynamic_mult_dec);

        // [B1+B5] Collect lightweight sort keys instead of cloning full TradingOrderDb structs.
        let mut buy_orders: Vec<FastOrder> = Vec::with_capacity(buy_count);
        let now_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        for entry in self.buy_orders.iter() {
            let v = entry.value();
            
            // [PHASE 3] Filter out expired orders
            if let Some(expires_at) = v.expires_at {
                if expires_at.timestamp_nanos_opt().unwrap_or(0) <= now_ns {
                    continue;
                }
            }

            buy_orders.push(FastOrder {
                id: *entry.key(),
                price: FastPrice::from(v.price_per_kwh),
                zone_id: v.zone_id,
                created_at_ns: v.created_at.map(|t| t.timestamp_nanos_opt().unwrap_or(0)).unwrap_or(0),
                expires_at_ns: v.expires_at.map(|t| t.timestamp_nanos_opt().unwrap_or(0)),
                user_id: v.user_id,
                energy_amount: v.energy_amount,
                filled_amount: v.filled_amount.unwrap_or(Decimal::ZERO),
                time_in_force: v.time_in_force,
                epoch_id: v.epoch_id,
                order_pda: v.order_pda.clone(),
                session_token: v.session_token.clone(),
            });
        }
        buy_orders.sort_unstable_by(|a, b| a.created_at_ns.cmp(&b.created_at_ns));

        if buy_orders.is_empty() {
            return Ok(0);
        }

        // Sell orders: sorted by (price ASC, created_at ASC) — cheapest first
        let sell_count = self.sell_orders.len();
        let mut sell_orders: Vec<FastOrder> = Vec::with_capacity(sell_count);
        for entry in self.sell_orders.iter() {
            let v = entry.value();

            // [PHASE 3] Filter out expired orders
            if let Some(expires_at) = v.expires_at {
                if expires_at.timestamp_nanos_opt().unwrap_or(0) <= now_ns {
                    continue;
                }
            }

            sell_orders.push(FastOrder {
                id: *entry.key(),
                price: FastPrice::from(v.price_per_kwh),
                zone_id: v.zone_id,
                created_at_ns: v.created_at.map(|t| t.timestamp_nanos_opt().unwrap_or(0)).unwrap_or(0),
                expires_at_ns: v.expires_at.map(|t| t.timestamp_nanos_opt().unwrap_or(0)),
                user_id: v.user_id,
                energy_amount: v.energy_amount,
                filled_amount: v.filled_amount.unwrap_or(Decimal::ZERO),
                time_in_force: v.time_in_force,
                epoch_id: v.epoch_id,
                order_pda: v.order_pda.clone(),
                session_token: v.session_token.clone(),
            });
        }
        sell_orders.sort_unstable_by(|a, b| a.price.cmp(&b.price).then_with(|| a.created_at_ns.cmp(&b.created_at_ns)));

        if sell_orders.is_empty() {
            return Ok(0);
        }

        let mut matches_created = 0;

        struct Candidate {
            id: Uuid,
            landed_cost: FastPrice,
            wheeling_charge_per_kwh: Decimal,
            loss_factor: Decimal,
            loss_cost_per_kwh: Decimal,
            user_id: Uuid,
            zone_id: Option<i32>,
            epoch_id: Uuid,
            order_pda: Option<String>,
            session_token: Option<String>,
        }

        let mut candidates: Vec<Candidate> = Vec::with_capacity(256);
        let mut geometry_cache: std::collections::HashMap<Option<i32>, (Decimal, Decimal, bool)> = std::collections::HashMap::with_capacity(16);

        // Try to match each buy order — access fields directly without full clone
        for buy in &buy_orders {
            let mut remaining_buy_amount = buy.energy_amount - buy.filled_amount;
            let buy_initial_filled = buy.filled_amount;
            let mut buy_filled_current = buy.filled_amount;

            if remaining_buy_amount < OrderMatchingEngine::MIN_TRADE_AMOUNT {
                continue;
            }

            candidates.clear();
            geometry_cache.clear();

            for sell in &sell_orders {
                if buy.user_id == sell.user_id {
                    continue;
                }

                let remaining_sell = sell.energy_amount - sell.filled_amount;

                if remaining_sell < OrderMatchingEngine::MIN_TRADE_AMOUNT {
                    continue;
                }

                // [PHASE 2 - GEOMETRY HOISTING]
                // Use the geometry_cache to avoid redundant DB/Arc calls for the same zone pair
                let (wheeling, loss, can_flow) = if sell.zone_id == buy.zone_id {
                    (Decimal::ZERO, Decimal::ONE, true)
                } else if let Some(&cached) = geometry_cache.get(&sell.zone_id) {
                    cached
                } else {
                    let flow_allowed = topo_snapshot.can_accommodate_flow(
                        sell.zone_id,
                        buy.zone_id,
                        remaining_sell.min(remaining_buy_amount),
                    );
                    
                    if !flow_allowed {
                        geometry_cache.insert(sell.zone_id, (Decimal::ZERO, Decimal::ZERO, false));
                        (Decimal::ZERO, Decimal::ZERO, false)
                    } else {
                        let w = topo_snapshot.calculate_wheeling_charge(sell.zone_id, buy.zone_id);
                        let l = topo_snapshot.calculate_loss_factor(sell.zone_id, buy.zone_id);
                        geometry_cache.insert(sell.zone_id, (w, l, true));
                        (w, l, true)
                    }
                };

                if !can_flow {
                    continue;
                }

                let wheeling_fp = FastPrice::from(wheeling);
                let loss_fp = FastPrice::from(loss);
                
                // [PHASE 3 - ARITHMETIC OPTIMIZATION]
                // Using fixed-point i64 (FastPrice) for zero-allocation arithmetic
                let extra_loss_raw = loss_fp.raw().saturating_sub(1_000_000_000); // (loss_factor - 1.0)
                let loss_cost_extra_raw = (sell.price.raw() as i128 * extra_loss_raw as i128 / 1_000_000_000) as i64;
                let mut landed_cost = FastPrice::from_raw(sell.price.raw() + wheeling_fp.raw() + loss_cost_extra_raw);

                // Apply dynamic multiplier (ToU)
                landed_cost = landed_cost.checked_mul(dynamic_mult).unwrap_or(landed_cost);

                // Apply Intra-zone discount
                if sell.zone_id == buy.zone_id {
                    let discount = FastPrice::from(Decimal::ONE - OrderMatchingEngine::INTRA_ZONE_DISCOUNT);
                    landed_cost = landed_cost.checked_mul(discount).unwrap_or(landed_cost);
                }

                if landed_cost <= buy.price {
                    candidates.push(Candidate {
                        id: sell.id,
                        landed_cost,
                        wheeling_charge_per_kwh: wheeling,
                        loss_factor: loss,
                        loss_cost_per_kwh: FastPrice::from_raw(loss_cost_extra_raw).to_decimal(),
                        user_id: sell.user_id,
                        zone_id: sell.zone_id,
                        epoch_id: sell.epoch_id.unwrap_or_default(),
                        order_pda: sell.order_pda.clone(),
                        session_token: sell.session_token.clone(),
                    });
                }
            }

            candidates.sort_by(|a, b| a.landed_cost.cmp(&b.landed_cost));

            // [ADVANCED] Handle Time-In-Force Instructions
            use crate::infra::db::schema::types::{TimeInForce, OrderStatus};
            
            if buy.time_in_force == TimeInForce::Fok {
                let mut total_available = Decimal::ZERO;
                for cand in &candidates {
                    if let Some(sell_entry) = self.sell_orders.get(&cand.id) {
                        let filled = sell_entry.filled_amount.unwrap_or(Decimal::ZERO);
                        total_available += sell_entry.energy_amount - filled;
                    }
                }
                
                if total_available < remaining_buy_amount {
                    debug!("    x FOK REJECTED: Insufficient aggregate liquidity (Available: {} < Requested: {})", total_available, remaining_buy_amount);
                    // Cancel the order
                    self.buy_orders.remove(&buy.id);
                    if let Some(event_bus) = &self.event_bus {
                        match_events.push(Event::OrderUpdate {
                            id: buy.id,
                            filled_amount: buy.filled_amount,
                            status: OrderStatus::Cancelled.to_string(),
                        });
                    }
                    continue;
                }
            }

            for candidate in &candidates {
                if remaining_buy_amount <= Decimal::ZERO {
                    break;
                }

                // Get the latest sell order state from DashMap
                let sell_order_entry = match self.sell_orders.get_mut(&candidate.id) {
                    Some(entry) => entry,
                    None => continue,
                };

                let sell_filled = sell_order_entry.filled_amount.unwrap_or(Decimal::ZERO);
                let remaining_sell = sell_order_entry.energy_amount - sell_filled;

                if remaining_sell < OrderMatchingEngine::MIN_TRADE_AMOUNT {
                    continue;
                }

                let match_amount = remaining_buy_amount.min(remaining_sell);
                let match_price = candidate.landed_cost.to_decimal();

                let total_energy_cost = match_amount * match_price;
                let total_wheeling = match_amount * candidate.wheeling_charge_per_kwh;
                let total_loss_cost = match_amount * candidate.loss_cost_per_kwh;

                let sell_order_id = sell_order_entry.id;
                let sell_order_user_id = sell_order_entry.user_id;
                let sell_order_pda = sell_order_entry.order_pda.clone();
                let sell_order_session_token = sell_order_entry.session_token.clone();
                let sell_order_zone_id = sell_order_entry.zone_id;
                let epoch_id = buy.epoch_id.unwrap_or_default(); // Use buy order's epoch (primary for matching)

                // Drop symbols before async to satisfy Send + Sync constraints and prevent deadlocks
                drop(sell_order_entry);

                match self
                    .create_order_match(
                        epoch_id,
                        buy.id,
                        sell_order_id,
                        buy.user_id,
                        sell_order_user_id,
                        match_amount,
                        match_price,
                        buy.order_pda.as_deref(),
                        sell_order_pda.as_deref(),
                        buy.zone_id,
                    )
                    .await
                {
                    Ok(match_id) => {
                        matches_created += 1;
                        track_order_matched("p2p", match_amount.to_f64().unwrap_or(0.0));

                        if let Some(market_data) = &self.market_data_service {
                            let market_data = market_data.clone();
                            let match_price_captured = match_price;
                            let match_amount_captured = match_amount;
                            tokio::spawn(async move {
                                let _ = market_data
                                    .sync_price_history(match_price_captured, match_amount_captured)
                                    .await;
                            });
                        }

                        // Update local zone price for triggers
                        let b_zone = buy.zone_id.unwrap_or(0);
                        let s_zone = sell_order_zone_id.unwrap_or(0);
                        self.zone_prices.insert(b_zone, match_price);
                        self.zone_prices.insert(s_zone, match_price);

                        // [PHASE 6] Database OHLC Aggregation
                        if let Some(aggregator) = &self.ohlc_aggregator {
                            let agg = aggregator.clone();
                            let zone_id = s_zone; // Using seller zone as trade location anchor
                            let match_price_captured = match_price;
                            let match_amount_captured = match_amount;
                            tokio::spawn(async move {
                                if let Err(e) = agg.on_order_matched(zone_id, match_price_captured, match_amount_captured, Utc::now()).await {
                                    error!("Failed to aggregate market data for zone {}: {}", zone_id, e);
                                }
                            });
                        }

                        self.trigger_settlement(
                            match_id,
                            buy.id,
                            sell_order_id,
                            buy.user_id,
                            sell_order_user_id,
                            match_amount,
                            match_price,
                            total_energy_cost,
                            epoch_id,
                            (
                                total_wheeling,
                                candidate.loss_factor,
                                total_loss_cost,
                                buy.zone_id,
                                sell_order_zone_id,
                            ),
                            buy.session_token.clone(),
                            sell_order_session_token.clone(),
                        )
                        .await;

                        self.grid_topology
                            .record_flow(sell_order_zone_id, buy.zone_id, match_amount)
                            .await;

                        // Update states safely
                        if let Some(mut sell_order_entry) = self.sell_orders.get_mut(&candidate.id) {
                            let sell_filled = sell_order_entry.filled_amount.unwrap_or(Decimal::ZERO);
                            let new_sell_filled = sell_filled + match_amount;
                            sell_order_entry.filled_amount = Some(new_sell_filled);
                            
                            buy_filled_current += match_amount;
                            remaining_buy_amount -= match_amount;

                            let new_sell_status = if new_sell_filled >= sell_order_entry.energy_amount {
                                OrderStatus::Filled
                            } else {
                                OrderStatus::PartiallyFilled
                            };
                            sell_order_entry.status = new_sell_status.clone();

                            if let Some(_) = &self.event_bus {
                                match_events.push(Event::OrderUpdate {
                                    id: sell_order_id,
                                    filled_amount: new_sell_filled,
                                    status: new_sell_status.to_string(),
                                });
                            }

                            if new_sell_status == OrderStatus::Filled {
                                drop(sell_order_entry);
                                self.sell_orders.remove(&candidate.id);
                            }
                        }
                    }
                    Err(e) => error!("Failed to create match: {}", e),
                }
            }

            let new_buy_status = if buy_filled_current >= buy.energy_amount {
                OrderStatus::Filled
            } else if buy_filled_current > Decimal::ZERO {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::Active
            };

            if let Some(_) = &self.event_bus {
                if buy_filled_current > buy_initial_filled {
                    match_events.push(Event::OrderUpdate {
                        id: buy.id,
                        filled_amount: buy_filled_current,
                        status: new_buy_status.to_string(),
                    });
                }
            }

            if new_buy_status == OrderStatus::Filled {
                self.buy_orders.remove(&buy.id);
            } else if buy.time_in_force == TimeInForce::Ioc {
                self.buy_orders.remove(&buy.id);
                if let Some(_) = &self.event_bus {
                    match_events.push(Event::OrderUpdate {
                        id: buy.id,
                        filled_amount: buy_filled_current,
                        status: OrderStatus::Cancelled.to_string(),
                    });
                }
            } else {
                if let Some(mut entry) = self.buy_orders.get_mut(&buy.id) {
                    entry.filled_amount = Some(buy_filled_current);
                    entry.status = new_buy_status;
                }
            }
        }

        // [B4] Publish all collected events in a single batch call
        if !match_events.is_empty() {
            if let Some(event_bus) = &self.event_bus {
                let _ = event_bus.publish_batch(&match_events).await;
            }
        }

        let duration_ms = cycle_start.elapsed().as_secs_f64() * 1000.0;
        record_matching_cycle(duration_ms, (buy_count + sell_count) as u64, matches_created as u64);
        crate::metrics::record_matching_cycle_high_fidelity(duration_ms);

        info!(
            "Shard {} matching cycle completed in {:.2}ms (Pools: {} -> {} buy, {} -> {} sell). Matches created: {}",
            self.shard_id,
            duration_ms,
            buy_count,
            self.buy_orders.len(),
            sell_count,
            self.sell_orders.len(),
            matches_created
        );

        Ok(matches_created)
    }

    async fn create_order_match(
        &self,
        epoch_id: Uuid,
        buy_order_id: Uuid,
        sell_order_id: Uuid,
        buyer_id: Uuid,
        seller_id: Uuid,
        energy_amount: Decimal,
        price_per_kwh: Decimal,
        buy_order_pda: Option<&str>,
        sell_order_pda: Option<&str>,
        zone_id: Option<i32>,
    ) -> Result<Uuid> {
        let match_id = Uuid::new_v4();

        // Extract trace context for distributed tracing
        use opentelemetry::propagation::TextMapPropagator;
        let mut trace_context = std::collections::HashMap::new();
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&opentelemetry::Context::current(), &mut trace_context);
        });
        let trace_context = if trace_context.is_empty() { None } else { Some(trace_context) };

        // Emit OrderMatched event via EventBus if available
        if let Some(event_bus) = &self.event_bus {
            let event = Event::OrderMatched(OrderMatchedPayload {
                match_id,
                epoch_id,
                buy_order_id,
                sell_order_id,
                amount: energy_amount,
                price: price_per_kwh,
                buyer_id,
                seller_id,
                timestamp: chrono::Utc::now(),
                zone_id,
                otel_trace_context: trace_context,
            });
            let bus = event_bus.clone();
            tokio::spawn(async move {
                let _ = bus.publish(&event).await;
            });
        }

        if let Some(blockchain) = &self.blockchain_service {
            let blockchain = blockchain.clone();
            let b_pda_owned = buy_order_pda.map(|s| s.to_string());
            let s_pda_owned = sell_order_pda.map(|s| s.to_string());
            let zone_num = zone_id.unwrap_or(1) as u32;

            tokio::spawn(async move {
                if let Ok(authority) = blockchain.get_authority_keypair().await {
                    let market_pda = Pubkey::find_program_address(
                        &[b"market"],
                        &blockchain.trading_program_id().unwrap_or_default(),
                    )
                    .0;
                    if let (Some(b_pda), Some(s_pda)) = (b_pda_owned, s_pda_owned) {
                        match to_u64_atomic(energy_amount, 9, "match_amount") {
                            Ok(match_u64) => {
                                if let Err(e) = blockchain
                                    .execute_match_orders(
                                        &authority,
                                        &market_pda.to_string(),
                                        &b_pda,
                                        &s_pda,
                                        match_u64,
                                        zone_num,
                                    )
                                    .await 
                                {
                                    error!("❌ On-chain matching failed for match {}: {:#}", match_id, e);
                                }
                            },
                            Err(e) => {
                                error!("❌ Numeric conversion failed for match {}: {}", match_id, e);
                            }
                        }
                    }
                }
            });
        }
        Ok(match_id)
    }

    async fn trigger_settlement(
        &self,
        _match_id: Uuid,
        buy_order_id: Uuid,
        sell_order_id: Uuid,
        buyer_id: Uuid,
        seller_id: Uuid,
        amount: Decimal,
        price: Decimal,
        cost: Decimal,
        epoch_id: Uuid,
        costs: (Decimal, Decimal, Decimal, Option<i32>, Option<i32>),
        buyer_token: Option<String>,
        seller_token: Option<String>,
    ) {
        // Calculate settlement record (replicated from manager logic for batching)
        let total_value = cost;
        let fee_rate = Decimal::from_str("0.01").unwrap_or_default();

        let loss_cost = costs.2;
        let wheeling_charge = costs.0;

        let seller_base_price_total = total_value - wheeling_charge - loss_cost;
        let fee_amount = seller_base_price_total * fee_rate;
        let net_amount = seller_base_price_total - fee_amount;

        let effective_energy = amount * (Decimal::ONE - costs.1);

        let settlement_ulid = Ulid::new();
        let settlement_id = Uuid::from_bytes(settlement_ulid.to_bytes());

        // Extract trace context for propagation
        use opentelemetry::propagation::TextMapPropagator;
        let mut trace_context = std::collections::HashMap::new();
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&opentelemetry::Context::current(), &mut trace_context);
        });
        let trace_context = if trace_context.is_empty() { None } else { Some(trace_context) };

        let settlement = Settlement {
            id: settlement_id,
            trade_id: Uuid::new_v4(), // In-memory reference ID
            buyer_id,
            seller_id,
            buy_order_id,
            sell_order_id,
            energy_amount: amount,
            price,
            total_value,
            fee_amount,
            net_amount,
            status: crate::domain::trading::settlement::SettlementStatus::Pending,
            blockchain_tx: None,
            created_at: Utc::now(),
            confirmed_at: None,
            buyer_zone_id: costs.3,
            seller_zone_id: costs.4,
            wheeling_charge: Some(wheeling_charge),
            loss_factor: Some(costs.1),
            loss_cost: Some(loss_cost),
            effective_energy: Some(effective_energy),
            buyer_session_token: buyer_token,
            seller_session_token: seller_token,
            erc_certificate_id: None,
            erc_transfer_tx: None,
            epoch_id,
            trace_context,
        };

        // Emit SettlementRequested event via EventBus if available
        if let Some(event_bus) = &self.event_bus {
            let event = Event::SettlementRequested(settlement);
            let bus = event_bus.clone();
            tokio::spawn(async move {
                let _ = bus.publish(&event).await;
            });
        }
    }
}

/// Pool of matching workers distributed by zone consistent hashing
pub struct MatchingWorkerPool {
    senders: Vec<tokio::sync::mpsc::Sender<Option<TradingOrderDb>>>,
    num_shards: u32,
}

impl MatchingWorkerPool {
    pub fn notify_zone(&self, zone_id: Option<i32>, order: Option<TradingOrderDb>) {
        let shard_id = zone_id.unwrap_or(0).unsigned_abs() % self.num_shards;
        if let Some(sender) = self.senders.get(shard_id as usize) {
            let _ = sender.try_send(order);
        }
    }
}

/// Lightweight order representation for fast matching
#[derive(Debug, Clone)]
pub struct FastOrder {
    pub id: Uuid,
    pub price: FastPrice,
    pub zone_id: Option<i32>,
    pub created_at_ns: i64,
    pub expires_at_ns: Option<i64>,
    pub user_id: Uuid,
    pub energy_amount: Decimal,
    pub filled_amount: Decimal,
    pub time_in_force: crate::infra::db::schema::types::TimeInForce,
    pub epoch_id: Option<Uuid>,
    pub order_pda: Option<String>,
    pub session_token: Option<String>,
}

/// Background service that automatically matches orders with offers
#[derive(Clone)]
pub struct OrderMatchingEngine {
    db: PgPool,
    running: Arc<RwLock<bool>>,
    match_interval_secs: u64,
    worker_pool: Arc<RwLock<Option<MatchingWorkerPool>>>,
    settlement: Option<SettlementService>,
    market_clearing: Option<MarketClearingService>,
    blockchain_service: Option<BlockchainService>,
    grid_topology: Arc<GridTopologyService>,
    market_data_service: Option<MarketDataService>,
    ohlc_aggregator: Option<Arc<crate::services::market_data::MarketDataService>>,
    event_bus: Option<EventBus>,
    p2p_config: Option<Arc<P2PConfigService>>,
}

impl OrderMatchingEngine {
    pub fn new(db: PgPool) -> Self {
        let num_shards = std::env::var("MATCHING_SHARDS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(4);

        // Read interval from environment variable, default to 5 seconds
        let match_interval_secs = std::env::var("MATCHING_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(1);

        if match_interval_secs != 1 {
            info!(
                "Order matching interval set to {} seconds (Shards: {})",
                match_interval_secs, num_shards
            );
        }

        Self {
            db,
            running: Arc::new(RwLock::new(false)),
            match_interval_secs,
            worker_pool: Arc::new(RwLock::new(None)),
            settlement: None,
            market_clearing: None,
            blockchain_service: None,
            grid_topology: Arc::new(GridTopologyService::new()),
            market_data_service: None,
            ohlc_aggregator: None,
            event_bus: None,
            p2p_config: None,
        }
    }

    /// Set the matching interval
    pub fn with_interval(mut self, interval_secs: u64) -> Self {
        self.match_interval_secs = interval_secs;
        self
    }

    /// Set the Grid Topology service
    pub fn with_topology(mut self, grid_topology: Arc<GridTopologyService>) -> Self {
        self.grid_topology = grid_topology;
        self
    }

    /// Set the OHLC Aggregator service for price charting
    pub fn with_ohlc_aggregator(mut self, aggregator: Arc<crate::services::market_data::MarketDataService>) -> Self {
        self.ohlc_aggregator = Some(aggregator);
        self
    }

    /// Set the Market Clearing service for processing escrow refunds
    pub fn with_market_clearing(mut self, market_clearing: MarketClearingService) -> Self {
        self.market_clearing = Some(market_clearing);
        self
    }

    /// Set the Settlement service for processing matched trades
    pub fn with_settlement(mut self, settlement: SettlementService) -> Self {
        self.settlement = Some(settlement);
        self
    }

    /// Set the Blockchain service for on-chain matching
    pub fn with_blockchain(mut self, blockchain_service: BlockchainService) -> Self {
        self.blockchain_service = Some(blockchain_service);
        self
    }

    /// Set the EventBus for real-time event emission
    pub fn with_event_bus(mut self, event_bus: EventBus) -> Self {
        self.event_bus = Some(event_bus);
        self
    }

    /// Set the P2P config service
    pub fn with_p2p_config(mut self, p2p_config: Arc<P2PConfigService>) -> Self {
        self.p2p_config = Some(p2p_config);
        self
    }

    /// Start the background matching engine, optionally seeding with rehydrated state.
    pub async fn start(&self, token: CancellationToken, seed_state: Option<std::collections::HashMap<Uuid, TradingOrderDb>>) {
        let mut running = self.running.write().await;
        if *running {
            warn!("Order matching engine is already running");
            return;
        }
        *running = true;
        drop(running);

        let num_shards = std::env::var("MATCHING_SHARDS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(4);

        info!(
            "🚀 Starting sharded order matching engine (shards: {}, interval: {}s)",
            num_shards, self.match_interval_secs
        );

        // Initialize and start workers
        let mut senders = Vec::new();
        for i in 0..num_shards {
            let (tx, rx) = tokio::sync::mpsc::channel(1000);
            senders.push(tx);

            let mut worker = ShardedMatchingWorker::new(
                i,
                num_shards,
                self.db.clone(),
                self.match_interval_secs,
                self.grid_topology.clone(),
            );

            // Wire up dependencies
            worker.settlement = self.settlement.clone();
            worker.market_clearing = self.market_clearing.clone();
            worker.blockchain_service = self.blockchain_service.clone();
            worker.market_data_service = self.market_data_service.clone();
            worker.ohlc_aggregator = self.ohlc_aggregator.clone();
            worker.event_bus = self.event_bus.clone();
            worker.p2p_config = self.p2p_config.clone();

            // Seed with rehydrated orders if available
            if let Some(state) = &seed_state {
                let shard_orders: Vec<TradingOrderDb> = state.values()
                    .filter(|o| (o.zone_id.unwrap_or(0) as u32 % num_shards) == i)
                    .cloned()
                    .collect();
                
                if !shard_orders.is_empty() {
                    worker.seed_orders(shard_orders);
                }
            }

            let worker = Arc::new(worker);
            let running = self.running.clone();
            let shard_token = token.clone();

            tokio::spawn(async move {
                worker.run(running, shard_token, rx).await;
            });
        }

        let mut pool = self.worker_pool.write().await;
        *pool = Some(MatchingWorkerPool {
            senders,
            num_shards,
        });
        drop(pool);

        // Keep the main loop for global maintenance (like expiry)
        let engine = self.clone();
        let maintenance_token = token.clone();
        tokio::spawn(async move {
            engine.run_maintenance_loop(maintenance_token).await;
        });
    }

    /// Global maintenance loop (Expiry, Stats aggregation)
    async fn run_maintenance_loop(&self, token: CancellationToken) {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(60)) => {
                    if let Err(e) = self.expire_stale_orders().await {
                        error!("❌ Error expiring stale orders: {}", e);
                    }

                    // Sync P2P config to GridTopology pricing cache
                    if let (Some(p2p), topology) = (&self.p2p_config, &self.grid_topology) {
                        if let Ok(active_configs) = p2p.get_all_active().await {
                            topology.load_zone_pair_overrides(&active_configs).await;
                        }
                    }
                }
                _ = token.cancelled() => {
                    info!("🔄 Matching engine maintenance loop shutting down...");
                    break;
                }
            }

            {
                let running = self.running.read().await;
                if !*running {
                    break;
                }
            }
        }
    }

    /// Stop the background matching engine
    pub async fn stop(&self) {
        let mut running = self.running.write().await;
        *running = false;
        info!("⏹️  Stopped automated order matching engine");
    }

    /// Minimum trade amount in kWh to avoid dust
    pub const MIN_TRADE_AMOUNT: Decimal = Decimal::from_parts(100000000, 0, 0, false, 9); // 0.100000000

    /// Intra-zone discount applied to landed cost when buyer and seller are in the same zone.
    /// This incentivizes local energy trading by making same-zone trades 0.5% cheaper.
    pub const INTRA_ZONE_DISCOUNT: Decimal = Decimal::from_parts(5, 0, 0, false, 3); // 0.005 = 0.5%

    /// Notify the matching engine that a new order has been created in a specific zone
    pub async fn notify_new_order(&self, zone_id: Option<i32>, order: Option<TradingOrderDb>) {
        let pool = self.worker_pool.read().await;
        if let Some(pool) = pool.as_ref() {
            pool.notify_zone(zone_id, order);
        }
    }

    /// Expire orders that have passed their expiration time
    pub async fn expire_stale_orders(&self) -> Result<u64> {
        let now = chrono::Utc::now();

        // Fetch stale orders that need expiry
        let stale_orders_rows = sqlx::query(
            r#"
            SELECT 
                id, user_id, order_type, side, 
                energy_amount, price_per_kwh, filled_amount, status, 
                expires_at, created_at, filled_at, epoch_id, zone_id, meter_id, refund_tx_signature, order_pda,
                order_index,
                trigger_price, trigger_type, trigger_status, trailing_offset, session_token, triggered_at, last_peak_price,
                blockchain_status, blockchain_tx_hash, blockchain_error, retry_count,
                time_in_force
            FROM trading_orders 
            WHERE status IN ('active', 'pending', 'partially_filled') 
            AND expires_at < $1
            "#,
        )
        .bind(now)
        .fetch_all(&self.db)
        .await?;

        // use crate::api::handlers::websocket::broadcaster::broadcast_p2p_order_update;
        let stale_orders: Vec<TradingOrderDb> = stale_orders_rows
            .into_iter()
            .map(|row| TradingOrderDb {
                id: row.get("id"),
                user_id: row.get("user_id"),
                order_type: row.get("order_type"),
                side: row.get("side"),
                energy_amount: row.get("energy_amount"),
                price_per_kwh: row.get("price_per_kwh"),
                filled_amount: row.get("filled_amount"),
                status: row.get("status"),
                expires_at: row.get("expires_at"),
                created_at: row.get("created_at"),
                filled_at: row.get("filled_at"),
                epoch_id: row.get("epoch_id"),
                zone_id: row.get("zone_id"),
                meter_id: row.get("meter_id"),
                refund_tx_signature: row.get("refund_tx_signature"),
                order_pda: row.get("order_pda"),
                order_index: row.get("order_index"),
                session_token: row.get("session_token"),
                trigger_price: row.get("trigger_price"),
                trigger_type: row.get("trigger_type"),
                trigger_status: row.get("trigger_status"),
                trailing_offset: row.get("trailing_offset"),
                triggered_at: row.get("triggered_at"),
                last_peak_price: row.get("last_peak_price"),
                blockchain_status: row.get("blockchain_status"),
                blockchain_tx_hash: row.get("blockchain_tx_hash"),
                blockchain_error: row.get("blockchain_error"),
                retry_count: row.get("retry_count"),
                time_in_force: row.get("time_in_force"),
            })
            .collect();

        let mut expired_count = 0;
        for order in stale_orders {
            debug!(
                "🕒 Expiring order {}: type={}, side={}, amount={}, status={}",
                order.id,
                order.order_type.as_str(),
                order.side.as_str(),
                order.energy_amount,
                order.status.as_str()
            );

            // 1. Update status to expired
            sqlx::query(
                "UPDATE trading_orders SET status = 'expired', updated_at = NOW() WHERE id = $1",
            )
            .bind(order.id)
            .execute(&self.db)
            .await?;

            // 2. Process Refund/Unlock
            if let Some(market_clearing) = &self.market_clearing {
                let remaining_amount =
                    order.energy_amount - order.filled_amount.unwrap_or(Decimal::ZERO);

                if remaining_amount > Decimal::ZERO {
                    match order.side {
                        OrderSide::Buy => {
                            let refund_value = remaining_amount * order.price_per_kwh;
                            // The provided snippet for `receiver_wallet_addr` and `receiver_wallet` is incomplete and refers to an undefined `db_user`.
                            // Assuming it was meant to be part of a larger, separate change or a placeholder, it's omitted to maintain syntactic correctness.
                            if let Err(e) = market_clearing
                                .unlock_funds(
                                    order.user_id,
                                    order.id,
                                    refund_value,
                                    "Order Expired",
                                )
                                .await
                            {
                                error!(
                                    "Failed to refund funds for expired order {}: {}",
                                    order.id, e
                                );
                            } else {
                                debug!(
                                    "💰 Refunded {} for expired buy order {}",
                                    refund_value, order.id
                                );
                            }
                        }
                        OrderSide::Sell => {
                            if let Err(e) = market_clearing
                                .unlock_energy(
                                    order.user_id,
                                    order.id,
                                    remaining_amount,
                                    "Order Expired",
                                )
                                .await
                            {
                                error!(
                                    "Failed to unlock energy for expired order {}: {}",
                                    order.id, e
                                );
                            } else {
                                debug!(
                                    "⚡ Unlocked {} energy for expired sell order {}",
                                    remaining_amount, order.id
                                );
                            }
                        }
                    }
                }
            }

            expired_count += 1;
        }

        if expired_count > 0 {
            info!("🧹 Expired {} stale orders totaling", expired_count);
        }

        Ok(expired_count)
    }

    /// Manually trigger a matching cycle (for testing or API endpoints)
    /// Returns a summary of matched orders and total volume.
    pub async fn trigger_matching(
        &self,
    ) -> Result<crate::domain::trading::models::MatchingSummary> {
        info!("Manual matching trigger requested for all shards");

        let market_clearing = self
            .market_clearing
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("MarketClearingService not initialized"))?;

        // 1. Get current market epoch
        let epoch = market_clearing
            .get_current_epoch()
            .await?
            .ok_or_else(|| anyhow::anyhow!("No active market epoch found"))?;

        // 2. Execute matching using the optimized clearing service
        let matches = market_clearing.run_order_matching(epoch.id).await?;

        let matched_count = matches.len();
        let total_volume: Decimal = matches.iter().map(|m| m.matched_amount).sum();
        let match_ids = matches.iter().map(|m| m.id).collect();

        // 3. Notify workers to refresh their in-memory state after DB changes
        let pool = self.worker_pool.read().await;
        if let Some(pool) = pool.as_ref() {
            for i in 0..pool.num_shards {
                pool.notify_zone(Some(i as i32), None);
            }
        }

        Ok(crate::domain::trading::models::MatchingSummary {
            matched_count,
            total_volume,
            matches: match_ids,
        })
    }
}
