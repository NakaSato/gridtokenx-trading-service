use anyhow::{Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use sqlx::Row;
use std::sync::Arc;
use tracing::{error, info, debug};
use ulid::Ulid;
use uuid::Uuid;
use crate::infra::db::schema::types::TimeInForce;
use tokio_util::sync::CancellationToken;

use super::types::{OrderMatch, Settlement};
use super::MarketClearingService;
use crate::infra::db::schema::types::OrderStatus;
use solana_sdk::pubkey::Pubkey;

pub struct MatchingResult {
    pub matches: Vec<OrderMatch>,
    pub settlements: Vec<Settlement>,
    pub order_updates: Vec<(Uuid, Decimal, OrderStatus)>,
    pub websocket_events: Vec<MarketEvent>,
    pub total_volume: Decimal,
}

#[derive(Clone, Debug)]
pub enum MarketEvent {
    P2POrderUpdate {
        order_id: Uuid,
        user_id: Uuid,
        side: String,
        status: String,
        original_amount: String,
        filled_amount: String,
        remaining_amount: String,
        price_per_kwh: String,
        timestamp: chrono::DateTime<Utc>,
    },
    TradeExecuted {
        trade_id: String,
        buy_order_id: String,
        sell_order_id: String,
        buyer_id: String,
        seller_id: String,
        quantity: String,
        price: String,
        total_value: String,
        executed_at: String,
    },
}

impl MarketClearingService {
    pub async fn run_order_matching(&self, epoch_id: Uuid) -> Result<Vec<OrderMatch>> {
        info!(
            "🚀 Starting Batch Optimized order matching for epoch: {}",
            epoch_id
        );

        // 1. Data Pre-fetching (O(1) pop_front ready)
        let (mut buy_orders, mut sell_orders) = self.get_order_book(epoch_id)
            .await
            .context("Failed to fetch order book for matching")?;
        
        if buy_orders.is_empty() || sell_orders.is_empty() {
            info!("No orders to match in epoch: {}", epoch_id);
            return Ok(vec![]);
        }

        let user_ids: std::collections::HashSet<Uuid> = buy_orders
            .iter()
            .map(|o| o.user_id)
            .chain(sell_orders.iter().map(|o| o.user_id))
            .collect();
        let decrypted_wallets = Arc::new(
            self.fetch_and_decrypt_wallets_batch(user_ids.into_iter().collect())
                .await
                .context("Failed to fetch and decrypt wallets for matching")?,
        );
        let (tou_multiplier, _) = self.get_tou_multiplier().await;

        // 2. Core Matching Engine (Memory-Only)
        let result =
            Self::perform_matching(epoch_id, &mut buy_orders, &mut sell_orders, tou_multiplier, &self.token)
                .await;

        if result.matches.is_empty() {
            return Ok(vec![]);
        }

        // 3. Batch Persistence
        self.persist_matching_results(
            epoch_id,
            &result.matches,
            &result.settlements,
            &result.order_updates,
            result.total_volume,
        )
        .await
        .context("Failed to persist matching results to database")?;

        // 4. Post-Commit Processing
        self.handle_post_matching_actions(
            &result.settlements,
            decrypted_wallets,
            result.websocket_events,
        )
        .await;

        info!(
            "🏆 MATCHING COMPLETE: {} trades, {} kWh total volume",
            result.matches.len(),
            result.total_volume
        );
        Ok(result.matches)
    }

    async fn perform_matching(
        epoch_id: Uuid,
        buy_orders: &mut std::collections::VecDeque<
            crate::domain::trading::clearing::types::OrderBookEntry,
        >,
        sell_orders: &mut std::collections::VecDeque<
            crate::domain::trading::clearing::types::OrderBookEntry,
        >,
        tou_multiplier: Decimal,
        token: &CancellationToken,
    ) -> MatchingResult {
        let mut matches = Vec::new();
        let mut settlements = Vec::new();
        let mut order_updates = Vec::new();
        let mut websocket_events = Vec::new();
        let mut total_volume = Decimal::ZERO;
        let mut zone_cost_cache = std::collections::HashMap::new();

        while let Some(buy_order) = buy_orders.front_mut() {
            if token.is_cancelled() {
                info!("MATCH_LOOP_INTERRUPTED: Shutdown signal received during batch matching for epoch {}", epoch_id);
                break;
            }

            // [ADVANCED] Handle Fill-or-Kill (FOK)
            if buy_order.time_in_force == TimeInForce::Fok {
                let mut total_available = Decimal::ZERO;
                for sell_order in sell_orders.iter() {
                    if sell_order.price_per_kwh <= buy_order.price_per_kwh {
                        total_available += sell_order.energy_amount;
                    } else {
                        break;
                    }
                }

                if total_available < buy_order.energy_amount {
                    debug!("    x FOK REJECTED in Batch: Insufficient liquidity (Available: {} < Requested: {})", total_available, buy_order.energy_amount);
                    order_updates.push((buy_order.order_id, buy_order.original_amount - buy_order.energy_amount, OrderStatus::Cancelled));
                    buy_orders.pop_front();
                    continue;
                }
            }

            let mut matched_in_this_pass = false;

            while let Some(sell_order) = sell_orders.front_mut() {
                if buy_order.price_per_kwh < sell_order.price_per_kwh {
                    break;
                }

                let base_match_price =
                    (buy_order.price_per_kwh + sell_order.price_per_kwh) / Decimal::from(2);
                let match_price = base_match_price * tou_multiplier;
                let match_amount = buy_order.energy_amount.min(sell_order.energy_amount);

                if match_amount > Decimal::ZERO {
                    matched_in_this_pass = true;
                    let match_id = Uuid::from_bytes(Ulid::new().to_bytes());
                    let settlement_id = Uuid::from_bytes(Ulid::new().to_bytes());

                    let order_match = OrderMatch {
                        id: match_id,
                        epoch_id,
                        buy_order_id: buy_order.order_id,
                        sell_order_id: sell_order.order_id,
                        matched_amount: match_amount,
                        match_price,
                        match_time: Utc::now(),
                        status: "pending".to_string(),
                    };
                    matches.push(order_match);

                    let settlement = Self::prepare_settlement_memory(
                        matches.last().unwrap(),
                        buy_order,
                        sell_order,
                        settlement_id,
                        &mut zone_cost_cache,
                    )
                    .await;
                    settlements.push(settlement);

                    buy_order.energy_amount -= match_amount;
                    sell_order.energy_amount -= match_amount;
                    total_volume += match_amount;

                    // Track Order Updates & Events
                    let buy_filled = buy_order.original_amount - buy_order.energy_amount;
                    let buy_status = if buy_order.energy_amount <= Decimal::ZERO {
                        OrderStatus::Filled
                    } else {
                        OrderStatus::PartiallyFilled
                    };
                    order_updates.push((buy_order.order_id, buy_filled, buy_status.clone()));

                    websocket_events.push(MarketEvent::P2POrderUpdate {
                        order_id: buy_order.order_id,
                        user_id: buy_order.user_id,
                        side: "buy".to_string(),
                        status: buy_status.to_string(),
                        original_amount: buy_order.original_amount.to_string(),
                        filled_amount: buy_filled.to_string(),
                        remaining_amount: buy_order.energy_amount.to_string(),
                        price_per_kwh: buy_order.price_per_kwh.to_string(),
                        timestamp: Utc::now(),
                    });

                    let sell_filled = sell_order.original_amount - sell_order.energy_amount;
                    let sell_status = if sell_order.energy_amount <= Decimal::ZERO {
                        OrderStatus::Filled
                    } else {
                        OrderStatus::PartiallyFilled
                    };
                    order_updates.push((sell_order.order_id, sell_filled, sell_status.clone()));

                    websocket_events.push(MarketEvent::P2POrderUpdate {
                        order_id: sell_order.order_id,
                        user_id: sell_order.user_id,
                        side: "sell".to_string(),
                        status: sell_status.to_string(),
                        original_amount: sell_order.original_amount.to_string(),
                        filled_amount: sell_filled.to_string(),
                        remaining_amount: sell_order.energy_amount.to_string(),
                        price_per_kwh: sell_order.price_per_kwh.to_string(),
                        timestamp: Utc::now(),
                    });

                    if sell_order.energy_amount <= Decimal::ZERO {
                        sell_orders.pop_front();
                    }

                    if buy_order.energy_amount <= Decimal::ZERO {
                        break;
                    }
                } else {
                    break;
                }
            }

            // [ADVANCED] Handle IOC and Order Completion
            if buy_order.energy_amount <= Decimal::ZERO {
                buy_orders.pop_front();
            } else if buy_order.time_in_force == TimeInForce::Ioc {
                debug!("    ! IOC REMAINDER CANCELLED in Batch: {} kWh remaining", buy_order.energy_amount);
                order_updates.push((buy_order.order_id, buy_order.original_amount - buy_order.energy_amount, OrderStatus::Cancelled));
                buy_orders.pop_front();
            } else if !matched_in_this_pass {
                // No more compatible sell orders in this epoch for this GTC order
                break;
            }
        }

        MatchingResult {
            matches,
            settlements,
            order_updates,
            websocket_events,
            total_volume,
        }
    }


    async fn persist_matching_results(
        &self,
        epoch_id: Uuid,
        matches: &[OrderMatch],
        settlements: &[Settlement],
        order_updates: &[(Uuid, Decimal, OrderStatus)],
        total_volume: Decimal,
    ) -> Result<()> {
        info!(
            "💾 Batch persisting {} matches and {} settlements",
            matches.len(),
            settlements.len()
        );
        let mut tx = self.db.begin()
            .await
            .context("Failed to start database transaction for matching persistence")?;

        Self::persist_orders_batch(&mut tx, order_updates)
            .await
            .context("Failed to persist order updates in matching batch")?;
        
        Self::persist_matches_batch(&mut tx, epoch_id, matches, settlements)
            .await
            .context("Failed to persist match records in matching batch")?;
        
        Self::persist_settlements_batch(&mut tx, epoch_id, settlements)
            .await
            .context("Failed to persist settlement records in matching batch")?;
        
        Self::update_epoch_statistics(
            &mut tx,
            epoch_id,
            total_volume,
            matches.len() as i32,
            settlements,
        )
        .await
        .context("Failed to update epoch statistics in matching batch")?;

        let _: () = tx.commit()
            .await
            .context("Failed to commit matching persistence transaction")?;
        
        Ok(())
    }

    async fn persist_orders_batch(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        order_updates: &[(Uuid, Decimal, OrderStatus)],
    ) -> Result<()> {
        if order_updates.is_empty() {
            return Ok(());
        }
        let (ids, filled, statuses): (Vec<_>, Vec<_>, Vec<_>) = order_updates
            .iter()
            .map(|(id, f, s)| (*id, *f, s.to_string()))
            .fold((vec![], vec![], vec![]), |mut acc, (id, f, s)| {
                acc.0.push(id);
                acc.1.push(f);
                acc.2.push(s);
                acc
            });

        sqlx::query(r#"UPDATE trading_orders AS t SET filled_amount = u.filled, status = u.status::order_status, updated_at = NOW() FROM UNNEST($1::uuid[], $2::numeric[], $3::text[]) AS u(id, filled, status) WHERE t.id = u.id"#)
            .bind(&ids).bind(&filled).bind(&statuses).execute(&mut **tx).await
            .context("Failed to execute batch order update in trading engine")?;
        Ok(())
    }

    async fn persist_matches_batch(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        epoch_id: Uuid,
        matches: &[OrderMatch],
        settlements: &[Settlement],
    ) -> Result<()> {
        if matches.is_empty() {
            return Ok(());
        }
        let m_ids: Vec<Uuid> = matches.iter().map(|m| m.id).collect();
        let m_buys: Vec<Uuid> = matches.iter().map(|m| m.buy_order_id).collect();
        let m_sells: Vec<Uuid> = matches.iter().map(|m| m.sell_order_id).collect();
        let m_amounts: Vec<Decimal> = matches.iter().map(|m| m.matched_amount).collect();
        let m_prices: Vec<Decimal> = matches.iter().map(|m| m.match_price).collect();
        let m_settlements: Vec<Uuid> = settlements.iter().map(|s| s.id).collect();

        sqlx::query(r#"INSERT INTO order_matches (id, epoch_id, buy_order_id, sell_order_id, matched_amount, match_price, settlement_id, match_time, status) SELECT * FROM UNNEST($1::uuid[], $2::uuid[], $3::uuid[], $4::uuid[], $5::numeric[], $6::numeric[], $7::uuid[], $8::timestamptz[], $9::text[])"#)
            .bind(&m_ids).bind(vec![epoch_id; matches.len()]).bind(&m_buys).bind(&m_sells).bind(&m_amounts).bind(&m_prices).bind(&m_settlements)
            .bind(vec![Utc::now(); matches.len()]).bind(vec!["settled".to_string(); matches.len()])
            .execute(&mut **tx).await
            .context("Failed to execute batch match insertion in trading engine")?;
        Ok(())
    }

    async fn persist_settlements_batch(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        epoch_id: Uuid,
        settlements: &[Settlement],
    ) -> Result<()> {
        if settlements.is_empty() {
            return Ok(());
        }
        let s_ids: Vec<Uuid> = settlements.iter().map(|s| s.id).collect();
        let s_buyers: Vec<Uuid> = settlements.iter().map(|s| s.buyer_id).collect();
        let s_sellers: Vec<Uuid> = settlements.iter().map(|s| s.seller_id).collect();
        let s_buy_orders: Vec<Uuid> = settlements.iter().map(|s| s.buy_order_id).collect();
        let s_sell_orders: Vec<Uuid> = settlements.iter().map(|s| s.sell_order_id).collect();
        let s_amounts: Vec<Decimal> = settlements.iter().map(|s| s.energy_amount).collect();
        let s_prices: Vec<Decimal> = settlements.iter().map(|s| s.price_per_kwh).collect();
        let s_totals: Vec<Decimal> = settlements.iter().map(|s| s.total_amount).collect();
        let s_fees: Vec<Decimal> = settlements.iter().map(|s| s.fee_amount).collect();
        let s_wh: Vec<Decimal> = settlements.iter().map(|s| s.wheeling_charge).collect();
        let s_lf: Vec<Decimal> = settlements.iter().map(|s| s.loss_factor).collect();
        let s_lc: Vec<Decimal> = settlements.iter().map(|s| s.loss_cost).collect();
        let s_ee: Vec<Decimal> = settlements.iter().map(|s| s.effective_energy).collect();
        let s_bz: Vec<Option<i32>> = settlements.iter().map(|s| s.buyer_zone_id).collect();
        let s_sz: Vec<Option<i32>> = settlements.iter().map(|s| s.seller_zone_id).collect();
        let s_net: Vec<Decimal> = settlements.iter().map(|s| s.net_amount).collect();
        let s_buy_sigs: Vec<Option<String>> = settlements
            .iter()
            .map(|s| s.buy_signature.clone())
            .collect();
        let s_sell_sigs: Vec<Option<String>> = settlements
            .iter()
            .map(|s| s.sell_signature.clone())
            .collect();
        let s_buy_payloads: Vec<Option<Vec<u8>>> =
            settlements.iter().map(|s| s.buy_payload.clone()).collect();
        let s_sell_payloads: Vec<Option<Vec<u8>>> =
            settlements.iter().map(|s| s.sell_payload.clone()).collect();

        sqlx::query(r#"INSERT INTO settlements (id, epoch_id, buyer_id, seller_id, buy_order_id, sell_order_id, energy_amount, price_per_kwh, total_amount, fee_amount, wheeling_charge, loss_factor, loss_cost, effective_energy, buyer_zone_id, seller_zone_id, net_amount, status, buy_signature, sell_signature, buy_payload, sell_payload, processed_at, updated_at) SELECT * FROM UNNEST($1::uuid[], $2::uuid[], $3::uuid[], $4::uuid[], $5::uuid[], $6::uuid[], $7::numeric[], $8::numeric[], $9::numeric[], $10::numeric[], $11::numeric[], $12::numeric[], $13::numeric[], $14::numeric[], $15::int4[], $16::int4[], $17::numeric[], $18::text[], $19::text[], $20::text[], $21::bytea[], $22::bytea[], $23::timestamptz[], $24::timestamptz[])"#)
            .bind(&s_ids).bind(vec![epoch_id; settlements.len()]).bind(&s_buyers).bind(&s_sellers).bind(&s_buy_orders).bind(&s_sell_orders).bind(&s_amounts).bind(&s_prices).bind(&s_totals).bind(&s_fees).bind(&s_wh).bind(&s_lf).bind(&s_lc).bind(&s_ee).bind(&s_bz).bind(&s_sz).bind(&s_net).bind(vec!["completed".to_string(); settlements.len()]).bind(&s_buy_sigs).bind(&s_sell_sigs).bind(&s_buy_payloads).bind(&s_sell_payloads).bind(vec![Utc::now(); settlements.len()]).bind(vec![Utc::now(); settlements.len()])
            .execute(&mut **tx).await
            .context("Failed to execute batch settlement insertion in trading engine")?;
        Ok(())
    }

    async fn update_epoch_statistics(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        epoch_id: Uuid,
        total_volume: Decimal,
        matched_count: i32,
        settlements: &[Settlement],
    ) -> Result<()> {
        let clearing_price = if total_volume > Decimal::ZERO {
            settlements.iter().map(|s| s.total_amount).sum::<Decimal>() / total_volume
        } else {
            Decimal::ZERO
        };
        sqlx::query("UPDATE market_epochs SET total_volume = COALESCE(total_volume, 0) + $1, matched_count = COALESCE(matched_count, 0) + $2, clearing_price = $3 WHERE id = $4")
            .bind(total_volume).bind(matched_count).bind(clearing_price).bind(epoch_id)
            .execute(&mut **tx).await
            .context("Failed to update epoch statistics in trading engine")?;
        Ok(())
    }

    async fn handle_post_matching_actions(
        &self,
        settlements: &[Settlement],
        _decrypted_wallets: Arc<std::collections::HashMap<Uuid, solana_sdk::signature::Keypair>>,
        websocket_events: Vec<MarketEvent>,
    ) {
        info!(
            "🤝 Post-commit processing for {} settlements",
            settlements.len()
        );
        for settlement in settlements {
            let mut _events = websocket_events.clone(); // Note: Simplified for now
            _events.push(MarketEvent::TradeExecuted {
                trade_id: settlement.id.to_string(),
                buy_order_id: settlement.buy_order_id.to_string(),
                sell_order_id: settlement.sell_order_id.to_string(),
                buyer_id: settlement.buyer_id.to_string(),
                seller_id: settlement.seller_id.to_string(),
                quantity: settlement.energy_amount.to_string(),
                price: settlement.price_per_kwh.to_string(),
                total_value: settlement.total_amount.to_string(),
                executed_at: Utc::now().to_rfc3339(),
            });

            self.trigger_rec_issuance_async(settlement).await;

            let service = self.clone();
            let settlement = settlement.clone();
            tokio::spawn(async move {
                if let (Some(b_sig), Some(s_sig), Some(b_payload), Some(s_payload)) = (
                    settlement.buy_signature.clone(),
                    settlement.sell_signature.clone(),
                    settlement.buy_payload.clone(),
                    settlement.sell_payload.clone(),
                ) {
                    let trading_program_id = match service.blockchain_service.trading_program_id() {
                        Ok(id) => id,
                        Err(e) => {
                            error!("❌ Critical: Failed to get trading program ID for trade {}: {}", settlement.id, e);
                            return;
                        }
                    };
                    let (market_pda, _) = Pubkey::find_program_address(&[b"market"], &trading_program_id);

                    match service
                        .execute_offchain_settlement(
                            &market_pda,
                            settlement.buyer_id,
                            settlement.seller_id,
                            &b_sig,
                            &b_payload,
                            &s_sig,
                            &s_payload,
                            settlement.energy_amount,
                            settlement.price_per_kwh,
                            settlement.wheeling_charge,
                            settlement.loss_cost,
                        )
                        .await
                    {
                        Ok(sig) => info!(
                            "✅ On-chain settlement successful for trade {}: {}",
                            settlement.id, sig
                        ),
                        Err(e) => error!(
                            "❌ On-chain settlement failed for trade {}: {:?}",
                            settlement.id, e
                        ),
                    }
                } else {
                    // Mock Escrow Releases
                    if let Err(e) = service
                        .execute_escrow_release(
                            settlement.seller_id,
                            settlement.net_amount,
                            "currency",
                        )
                        .await
                    {
                        error!("❌ Failed to release currency escrow for trade {}: {:?}", settlement.id, e);
                    }
                    
                    if let Err(e) = service
                        .execute_escrow_release(
                            settlement.buyer_id,
                            settlement.effective_energy,
                            "energy",
                        )
                        .await
                    {
                        error!("❌ Failed to release energy escrow for trade {}: {:?}", settlement.id, e);
                    }
                }
            });
        }
        let _ = self.broadcast_depth_update().await;
    }

    /// Pure memory helper for settlement preparation
    async fn prepare_settlement_memory(
        order_match: &OrderMatch,
        buy_order: &crate::domain::trading::clearing::types::OrderBookEntry,
        sell_order: &crate::domain::trading::clearing::types::OrderBookEntry,
        settlement_id: Uuid,
        zone_cost_cache: &mut std::collections::HashMap<
            (i32, i32),
            (Decimal, Decimal, Decimal, Decimal),
        >,
    ) -> Settlement {
        let total_amount = order_match.matched_amount * order_match.match_price;
        let fee_rate = Decimal::from_parts(1, 0, 0, false, 2); // 0.01 (1%)
        let fee_amount = total_amount * fee_rate;

        // Physical Grid Logic (Wheeling + Losses)
        let mut wheeling_charge = Decimal::ZERO;
        let mut loss_factor = Decimal::ZERO;
        let mut loss_cost = Decimal::ZERO;
        let mut effective_energy = order_match.matched_amount;

        if let (Some(b_zone), Some(s_zone)) = (buy_order.zone_id, sell_order.zone_id) {
            let zone_pair = (b_zone, s_zone);

            if let Some((cached_wh, cached_lf, cached_lc, cached_ee)) =
                zone_cost_cache.get(&zone_pair)
            {
                wheeling_charge = *cached_wh;
                loss_factor = *cached_lf;
                loss_cost = *cached_lc;
                effective_energy = *cached_ee;
            } else {
                // Safe Grid Physics Calculation (using Decimal to avoid f64 precision loss)
                let distance = Decimal::from((b_zone - s_zone).unsigned_abs());
                
                // Wheeling Charge: 0.02 + (0.01 * distance)
                let wh_base = Decimal::from_parts(2, 0, 0, false, 2); // 0.02
                let wh_dist_rate = Decimal::from_parts(1, 0, 0, false, 2); // 0.01
                let total_wh_rate = wh_base + (wh_dist_rate * distance);
                wheeling_charge = order_match.matched_amount * total_wh_rate;

                // Loss Factor: 0.01 + (0.005 * distance)
                let lf_base = Decimal::from_parts(1, 0, 0, false, 2); // 0.01
                let lf_dist_rate = Decimal::from_parts(5, 0, 0, false, 3); // 0.005
                loss_factor = lf_base + (lf_dist_rate * distance);

                // Loss Cost: Matched Amount * Match Price * Loss Factor
                loss_cost = order_match.matched_amount * order_match.match_price * loss_factor;
                
                // Effective Energy: Amount * (1 - Loss Factor)
                effective_energy = order_match.matched_amount * (Decimal::ONE - loss_factor);

                zone_cost_cache.insert(
                    zone_pair,
                    (wheeling_charge, loss_factor, loss_cost, effective_energy),
                );
            }
        }

        let net_amount = total_amount - fee_amount - wheeling_charge;

        Settlement {
            id: settlement_id,
            epoch_id: order_match.epoch_id,
            buyer_id: buy_order.user_id,
            seller_id: sell_order.user_id,
            buy_order_id: order_match.buy_order_id,
            sell_order_id: order_match.sell_order_id,
            energy_amount: order_match.matched_amount,
            price_per_kwh: order_match.match_price,
            total_amount,
            fee_amount,
            wheeling_charge,
            loss_factor,
            loss_cost,
            effective_energy,
            buyer_zone_id: buy_order.zone_id,
            seller_zone_id: sell_order.zone_id,
            net_amount,
            status: "completed".to_string(),
            buyer_session_token: buy_order.session_token.clone(),
            seller_session_token: sell_order.session_token.clone(),
            buy_signature: buy_order.signature.clone(),
            sell_signature: sell_order.signature.clone(),
            buy_payload: buy_order.payload_bytes.clone(),
            sell_payload: sell_order.payload_bytes.clone(),
            retry_count: 0,
            error_message: None,
            otel_trace_context: None, // Will be populated by caller if needed
        }
    }

    /// Internal helper for async REC issuance
    async fn trigger_rec_issuance_async(&self, settlement: &Settlement) {
        let db = self.db.clone();
        let erc_service = self.erc_service.clone();
        let settlement_id = settlement.id;
        let seller_id = settlement.seller_id;
        let energy_amount = settlement.energy_amount;

        tokio::spawn(async move {
            let seller_row = sqlx::query("SELECT wallet_address FROM users WHERE id = $1")
                .bind(seller_id)
                .fetch_optional(&db)
                .await;
            
            let wallet_addr = match seller_row {
                Ok(Some(row)) => row.get::<Option<String>, _>("wallet_address").unwrap_or_default(),
                Ok(None) => {
                    error!("❌ REC Issuance Error: Seller {} not found in database", seller_id);
                    return;
                }
                Err(e) => {
                    error!("❌ REC Issuance Error: Failed to fetch seller {} from database: {:?}", seller_id, e);
                    return;
                }
            };

            let cert_request = crate::services::erc::IssueErcRequest {
                wallet_address: wallet_addr,
                meter_id: None,
                kwh_amount: energy_amount,
                expiry_date: Some(Utc::now() + chrono::Duration::days(365)),
                metadata: Some(
                    serde_json::json!({ "renewable_source": "Solar", "settlement_id": settlement_id }),
                ),
            };

            match erc_service
                .issue_certificate(
                    seller_id,
                    "PlatformAuthority",
                    cert_request,
                    Some(settlement_id),
                )
                .await {
                    Ok(_) => info!("✅ REC certificate issued for settlement {}", settlement_id),
                    Err(e) => error!("❌ Failed to issue REC certificate for settlement {}: {:?}", settlement_id, e),
                }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::trading::clearing::types::OrderBookEntry;
    use crate::infra::db::schema::types::OrderSide;
    use rust_decimal_macros::dec;
    use std::collections::VecDeque;

    fn create_test_order(
        id: u64,
        user_id: u64,
        amount: Decimal,
        price: Decimal,
        side: OrderSide,
    ) -> OrderBookEntry {
        OrderBookEntry {
            order_id: Uuid::from_u128(id as u128),
            user_id: Uuid::from_u128(user_id as u128),
            side,
            energy_amount: amount,
            original_amount: amount,
            price_per_kwh: price,
            created_at: Utc::now(),
            zone_id: Some(1),
            session_token: None,
            signature: None,
            payload_bytes: None,
            time_in_force: TimeInForce::Gtc,
        }
    }

    #[tokio::test]
    async fn test_perform_matching_empty() {
        let mut buy_orders = VecDeque::new();
        let mut sell_orders = VecDeque::new();

        let result = MarketClearingService::perform_matching(
            Uuid::new_v4(),
            &mut buy_orders,
            &mut sell_orders,
            dec!(1.0),
            &CancellationToken::new(),
        )
        .await;

        assert!(result.matches.is_empty());
        assert!(result.settlements.is_empty());
        assert_eq!(result.total_volume, Decimal::ZERO);
    }

    #[tokio::test]
    async fn test_perform_matching_no_overlap() {
        let mut buy_orders =
            VecDeque::from([create_test_order(1, 1, dec!(10), dec!(1.0), OrderSide::Buy)]);
        let mut sell_orders = VecDeque::from([create_test_order(
            2,
            2,
            dec!(10),
            dec!(2.0),
            OrderSide::Sell,
        )]);

        let result = MarketClearingService::perform_matching(
            Uuid::new_v4(),
            &mut buy_orders,
            &mut sell_orders,
            dec!(1.0),
            &CancellationToken::new(),
        )
        .await;

        assert!(result.matches.is_empty());
    }

    #[tokio::test]
    async fn test_perform_matching_perfect_match() {
        let mut buy_orders =
            VecDeque::from([create_test_order(1, 1, dec!(10), dec!(2.0), OrderSide::Buy)]);
        let mut sell_orders = VecDeque::from([create_test_order(
            2,
            2,
            dec!(10),
            dec!(1.0),
            OrderSide::Sell,
        )]);

        let result = MarketClearingService::perform_matching(
            Uuid::new_v4(),
            &mut buy_orders,
            &mut sell_orders,
            dec!(1.0),
            &CancellationToken::new(),
        )
        .await;

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.settlements.len(), 1);
        assert_eq!(result.total_volume, dec!(10));
        assert_eq!(result.matches[0].match_price, dec!(1.5));
        assert_eq!(result.matches[0].matched_amount, dec!(10));
        assert!(buy_orders.is_empty());
        assert!(sell_orders.is_empty());
    }

    #[tokio::test]
    async fn test_perform_matching_partial_buy() {
        let mut buy_orders =
            VecDeque::from([create_test_order(1, 1, dec!(15), dec!(2.0), OrderSide::Buy)]);
        let mut sell_orders = VecDeque::from([create_test_order(
            2,
            2,
            dec!(10),
            dec!(1.0),
            OrderSide::Sell,
        )]);

        let result = MarketClearingService::perform_matching(
            Uuid::new_v4(),
            &mut buy_orders,
            &mut sell_orders,
            dec!(1.0),
            &CancellationToken::new(),
        )
        .await;

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].matched_amount, dec!(10));
        assert_eq!(buy_orders.len(), 1);
        assert_eq!(buy_orders[0].energy_amount, dec!(5));
        assert!(sell_orders.is_empty());
    }

    #[tokio::test]
    async fn test_perform_matching_multiple_sells() {
        let mut buy_orders =
            VecDeque::from([create_test_order(1, 1, dec!(20), dec!(3.0), OrderSide::Buy)]);
        let mut sell_orders = VecDeque::from([
            create_test_order(2, 2, dec!(10), dec!(1.0), OrderSide::Sell),
            create_test_order(3, 3, dec!(10), dec!(2.0), OrderSide::Sell),
        ]);

        let result = MarketClearingService::perform_matching(
            Uuid::new_v4(),
            &mut buy_orders,
            &mut sell_orders,
            dec!(1.0),
            &CancellationToken::new(),
        )
        .await;

        assert_eq!(result.matches.len(), 2);
        assert_eq!(result.total_volume, dec!(20));
        assert!(buy_orders.is_empty());
        assert!(sell_orders.is_empty());
    }

    #[tokio::test]
    async fn test_perform_matching_tou_multiplier() {
        let mut buy_orders =
            VecDeque::from([create_test_order(1, 1, dec!(10), dec!(2.0), OrderSide::Buy)]);
        let mut sell_orders = VecDeque::from([create_test_order(
            2,
            2,
            dec!(10),
            dec!(2.0),
            OrderSide::Sell,
        )]);

        let result = MarketClearingService::perform_matching(
            Uuid::new_v4(),
            &mut buy_orders,
            &mut sell_orders,
            dec!(1.2),
            &CancellationToken::new(),
        )
        .await;

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].match_price, dec!(2.4));
    }
    #[tokio::test]
    async fn test_perform_matching_zero_quantity() {
        let mut buy_orders =
            VecDeque::from([create_test_order(1, 1, dec!(0), dec!(2.0), OrderSide::Buy)]);
        let mut sell_orders = VecDeque::from([create_test_order(
            2,
            2,
            dec!(10),
            dec!(1.0),
            OrderSide::Sell,
        )]);

        let result = MarketClearingService::perform_matching(
            Uuid::new_v4(),
            &mut buy_orders,
            &mut sell_orders,
            dec!(1.0),
            &CancellationToken::new(),
        )
        .await;

        assert!(result.matches.is_empty());
    }
}
