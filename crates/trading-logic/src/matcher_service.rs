use chrono::Utc;
use rust_decimal::Decimal;
use std::sync::Arc;
use tracing::info;
use trading_core::fast_price::FastPrice;
use trading_core::traits::{EventPublisher, OrderRepository, SettlementRepository, TraitResult};
use trading_engine::engine::{MatchingEngine, TopologySnapshot};
use trading_engine::types::FastOrder;
use uuid::Uuid;

/// Async orchestrator for the matching engine
pub struct MatcherService {
    order_repo: Arc<dyn OrderRepository>,
    settlement_repo: Arc<dyn SettlementRepository>,
    events: Arc<dyn EventPublisher>,
    topology: Arc<dyn TopologySnapshot>,
}

impl MatcherService {
    pub fn new(
        order_repo: Arc<dyn OrderRepository>,
        settlement_repo: Arc<dyn SettlementRepository>,
        events: Arc<dyn EventPublisher>,
        topology: Arc<dyn TopologySnapshot>,
    ) -> Self {
        Self {
            order_repo,
            settlement_repo,
            events,
            topology,
        }
    }

    /// Run a matching cycle for all active orders
    pub async fn run_matching_cycle(&self) -> TraitResult<usize> {
        // 1. Fetch active orders
        let buy_orders = self.order_repo.get_active_buy_orders().await?;
        let sell_orders = self.order_repo.get_active_sell_orders().await?;

        if buy_orders.is_empty() || sell_orders.is_empty() {
            return Ok(0);
        }

        // 2. Convert to FastOrder and Metadata
        let mut buy_metadata = Vec::with_capacity(buy_orders.len());
        let mut fast_buys: Vec<FastOrder> = buy_orders
            .iter()
            .enumerate()
            .map(|(i, o)| {
                buy_metadata.push(trading_engine::types::OrderMetadata {
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

        let mut sell_metadata = Vec::with_capacity(sell_orders.len());
        let mut fast_sells: Vec<FastOrder> = sell_orders
            .iter()
            .enumerate()
            .map(|(i, o)| {
                sell_metadata.push(trading_engine::types::OrderMetadata {
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

        // Sort for matching engine expectations (FIFO for buys, Price-Time for sells)
        fast_buys.sort_unstable_by(|a, b| a.created_at_ns.cmp(&b.created_at_ns));
        fast_sells.sort_unstable_by(|a, b| {
            a.price
                .cmp(&b.price)
                .then_with(|| a.created_at_ns.cmp(&b.created_at_ns))
        });

        // 3. Execute pure matching logic
        let now_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let (matches, stats) = MatchingEngine::match_cycle(
            &mut fast_buys,
            &mut fast_sells,
            &buy_metadata,
            &sell_metadata,
            self.topology.as_ref(),
            FastPrice::from(rust_decimal_macros::dec!(1.0)), // Default multiplier
            now_ns,
        );

        if matches.is_empty() {
            return Ok(0);
        }

        info!(
            "Matching cycle completed: {} matches, total volume: {}",
            matches.len(),
            stats.total_volume
        );

        // 4. Batch Persist fills and settlements
        use std::collections::HashMap;
        use trading_core::types::OrderStatus;

        let mut order_deltas: HashMap<Uuid, (Decimal, OrderStatus, Option<i32>)> = HashMap::new();

        for m in &matches {
            // Aggregate fill amounts for buyer
            let (buy_amt, _, buy_zone) = order_deltas
                .entry(m.buy_order_id)
                .or_insert((Decimal::ZERO, OrderStatus::PartiallyFilled, m.buyer_zone_id));
            *buy_amt += m.match_amount;
            *buy_zone = m.buyer_zone_id;

            // Aggregate fill amounts for seller
            let (sell_amt, _, sell_zone) = order_deltas
                .entry(m.sell_order_id)
                .or_insert((Decimal::ZERO, OrderStatus::PartiallyFilled, m.seller_zone_id));
            *sell_amt += m.match_amount;
            *sell_zone = m.seller_zone_id;

            // Reconstruct metadata for settlement
            let buy_meta = &buy_metadata[m.buy_metadata_index];
            let sell_meta = &sell_metadata[m.sell_metadata_index];

            // Create settlement record
            let _ = self
                .settlement_repo
                .insert_settlement(&trading_core::models::Settlement {
                    id: Uuid::new_v4(),
                    trade_id: Some(Uuid::new_v4()),
                    epoch_id: m.epoch_id,
                    buyer_id: m.buyer_id,
                    seller_id: m.seller_id,
                    buy_order_id: m.buy_order_id,
                    sell_order_id: m.sell_order_id,
                    energy_amount: m.match_amount,
                    price: m.match_price,
                    total_amount: m.total_energy_cost,
                    fee_amount: rust_decimal_macros::dec!(0),
                    net_amount: m.total_energy_cost,
                    status: trading_core::models::SettlementStatus::Pending,
                    blockchain_tx: None,
                    created_at: Utc::now(),
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
                .await;

            // Publish OrderMatched Event
            let matched_event = trading_core::events::Event::OrderMatched(trading_core::events::OrderMatchedPayload {
                match_id: Uuid::new_v4(), 
                epoch_id: m.epoch_id,
                buy_order_id: m.buy_order_id,
                sell_order_id: m.sell_order_id,
                amount: m.match_amount,
                price: m.match_price,
                buyer_id: m.buyer_id,
                seller_id: m.seller_id,
                timestamp: Utc::now(),
                zone_id: m.buyer_zone_id,
            });
            let _ = self.events.publish(matched_event).await;
        }

        // Apply aggregated order updates (Batching logic for OrderRepository)
        for (order_id, (amount, status, zone_id)) in order_deltas {
            self.order_repo
                .update_filled_amount(order_id, amount, status)
                .await?;

            // Publish OrderUpdate Event for UI/Real-time updates
            let update_event = trading_core::events::Event::OrderUpdate {
                id: order_id,
                filled_amount: amount,
                status: status.to_string(),
                zone_id,
            };
            let _ = self.events.publish(update_event).await;
        }

        Ok(matches.len())
    }
}
