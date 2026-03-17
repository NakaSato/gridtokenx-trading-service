use anyhow::Result;
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal::prelude::{ToPrimitive, FromPrimitive};

use sqlx::Row;
use ulid::Ulid;
use uuid::Uuid;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{error, info};

use crate::infra::db::schema::types::OrderStatus;
use crate::services::erc::IssueErcRequest;
use solana_sdk::pubkey::Pubkey;
use super::MarketClearingService;
use super::types::{OrderMatch, Settlement};

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
    /// Run order matching algorithm for an epoch (P1 Optimization: Batch Persistence O(B+S+DB))
    pub async fn run_order_matching(&self, epoch_id: Uuid) -> Result<Vec<OrderMatch>> {
        info!("🚀 Starting Batch Optimized order matching for epoch: {}", epoch_id);

        // Get current order book (O(1) pop_front ready, with metadata pre-fetched)
        let (mut buy_orders, mut sell_orders) = self.get_order_book(epoch_id).await?;

        if buy_orders.is_empty() || sell_orders.is_empty() {
            info!("No orders to match in epoch: {}", epoch_id);
            return Ok(vec![]);
        }

        // P2 Optimization: Pre-fetch and Decrypt all necessary wallets in parallel (O(1) pass)
        let mut user_ids: std::collections::HashSet<Uuid> = buy_orders.iter().map(|o| o.user_id).collect();
        user_ids.extend(sell_orders.iter().map(|o| o.user_id));
        let decrypted_wallets = self.fetch_and_decrypt_wallets_batch(user_ids.into_iter().collect()).await?;
        let decrypted_wallets = Arc::new(decrypted_wallets); // Share with background tasks

        let mut matches = Vec::new();
        let mut settlements = Vec::new();
        let mut total_volume = Decimal::ZERO;
        let mut total_match_count = 0;
        
        // Batch tracking
        let mut order_updates: Vec<(Uuid, Decimal, OrderStatus)> = Vec::new();
        let mut websocket_events: Vec<MarketEvent> = Vec::new();
        let mut zone_cost_cache = std::collections::HashMap::new();

        // P2 Optimization: Pre-fetch TOU Multiplier once (O(1) instead of O(N))
        let (tou_multiplier, _tou_period) = self.get_tou_multiplier().await;
        info!("🕒 Applied TOU Multiplier: {} for epoch {}", tou_multiplier, epoch_id);

        // Step 1: Matching Loop (Pure Memory - O(B + S))
        while let Some(buy_order) = buy_orders.front_mut() {
            if let Some(sell_order) = sell_orders.front_mut() {
                // Check if orders can be matched (bid >= ask)
                if buy_order.price_per_kwh >= sell_order.price_per_kwh {
                    let base_match_price = (buy_order.price_per_kwh + sell_order.price_per_kwh) / Decimal::from(2);
                    let match_price = base_match_price * tou_multiplier;

                    let match_amount = buy_order.energy_amount.min(sell_order.energy_amount);

                    if match_amount > Decimal::ZERO {
                        let match_id = Uuid::from_bytes(Ulid::new().to_bytes());
                        let settlement_id = Uuid::from_bytes(Ulid::new().to_bytes());

                        // 1.1 Record Match (In Memory)
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

                        // 1.2 Prepare Settlement Record (In Memory - No DB calls!)
                        let settlement = self.prepare_settlement_memory(
                            &matches.last().unwrap(),
                            buy_order,
                            sell_order,
                            settlement_id,
                            &mut zone_cost_cache
                        ).await;
                        settlements.push(settlement);

                        // Update in-memory amounts
                        buy_order.energy_amount -= match_amount;
                        sell_order.energy_amount -= match_amount;
                        total_volume += match_amount;
                        total_match_count += 1;

                        // Track Buy Order Updates
                        let buy_filled = buy_order.original_amount - buy_order.energy_amount;
                        let buy_status = if buy_order.energy_amount <= Decimal::ZERO { OrderStatus::Filled } else { OrderStatus::PartiallyFilled };
                        
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

                        if buy_status == OrderStatus::Filled { buy_orders.pop_front(); }

                        // Track Sell Order Updates
                        let sell_filled = sell_order.original_amount - sell_order.energy_amount;
                        let sell_status = if sell_order.energy_amount <= Decimal::ZERO { OrderStatus::Filled } else { OrderStatus::PartiallyFilled };
                        
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

                        if sell_status == OrderStatus::Filled { sell_orders.pop_front(); }
                    }
                } else { break; }
            } else { break; }
        }

        if matches.is_empty() { return Ok(vec![]); }

        // Step 2: Batch Persistence (Inside a single transaction - O(1) DB roundtrips)
        // ... (Persistence logic remains same)
        info!("💾 Batch persisting {} matches and {} settlements", matches.len(), settlements.len());
        let mut tx = self.db.begin().await?;

        // 2.1 Batch Update Orders using UNNEST (Postgres specific Optimization)
        let order_ids: Vec<Uuid> = order_updates.iter().map(|u| u.0).collect();
        let order_filled: Vec<Decimal> = order_updates.iter().map(|u| u.1).collect();
        let order_statuses: Vec<String> = order_updates.iter().map(|u| u.2.to_string()).collect();

        sqlx::query!(
            r#"
            UPDATE trading_orders AS t
            SET filled_amount = u.filled, status = u.status::order_status, updated_at = NOW()
            FROM UNNEST($1::uuid[], $2::numeric[], $3::text[]) AS u(id, filled, status)
            WHERE t.id = u.id
            "#
        )
        .bind(&order_ids)
        .bind(&order_filled)
        .bind(&order_statuses)
        .execute(&mut *tx)
        .await?;

        // 2.2 Batch Insert Match Records
        let match_ids: Vec<Uuid> = matches.iter().map(|m| m.id).collect();
        let match_buy_ids: Vec<Uuid> = matches.iter().map(|m| m.buy_order_id).collect();
        let match_sell_ids: Vec<Uuid> = matches.iter().map(|m| m.sell_order_id).collect();
        let match_amounts: Vec<Decimal> = matches.iter().map(|m| m.matched_amount).collect();
        let match_prices: Vec<Decimal> = matches.iter().map(|m| m.match_price).collect();
        let match_settlement_ids: Vec<Uuid> = settlements.iter().map(|s| s.id).collect();

        sqlx::query!(
            r#"
            INSERT INTO order_matches (id, epoch_id, buy_order_id, sell_order_id, matched_amount, match_price, settlement_id, match_time, status)
            SELECT * FROM UNNEST($1::uuid[], $2::uuid[], $3::uuid[], $4::uuid[], $5::numeric[], $6::numeric[], $7::uuid[], $8::timestamptz[], $9::text[])
            "#
        )
        .bind(&match_ids)
        .bind(&vec![epoch_id; matches.len()])
        .bind(&match_buy_ids)
        .bind(&match_sell_ids)
        .bind(&match_amounts)
        .bind(&match_prices)
        .bind(&match_settlement_ids)
        .bind(&vec![Utc::now(); matches.len()])
        .bind(&vec!["completed".to_string(); matches.len()])
        .execute(&mut *tx)
        .await?;

        // 2.3 Batch Insert Settlements
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
        let s_buy_sigs: Vec<Option<String>> = settlements.iter().map(|s| s.buy_signature.clone()).collect();
        let s_sell_sigs: Vec<Option<String>> = settlements.iter().map(|s| s.sell_signature.clone()).collect();
        let s_buy_payloads: Vec<Option<Vec<u8>>> = settlements.iter().map(|s| s.buy_payload.clone()).collect();
        let s_sell_payloads: Vec<Option<Vec<u8>>> = settlements.iter().map(|s| s.sell_payload.clone()).collect();

        sqlx::query!(
            r#"
            INSERT INTO settlements (
                id, epoch_id, buyer_id, seller_id, buy_order_id, sell_order_id, energy_amount, 
                price_per_kwh, total_amount, fee_amount, wheeling_charge, loss_factor, loss_cost,
                effective_energy, buyer_zone_id, seller_zone_id, net_amount, status, 
                buy_signature, sell_signature, buy_payload, sell_payload,
                processed_at, updated_at
            )
            SELECT * FROM UNNEST($1::uuid[], $2::uuid[], $3::uuid[], $4::uuid[], $5::uuid[], $6::uuid[], $7::numeric[], $8::numeric[], $9::numeric[], $10::numeric[], $11::numeric[], $12::numeric[], $13::numeric[], $14::numeric[], $15::int4[], $16::int4[], $17::numeric[], $18::text[], $19::text[], $20::text[], $21::bytea[], $22::bytea[], $23::timestamptz[], $24::timestamptz[])
            "#
        )
        .bind(&s_ids)
        .bind(&vec![epoch_id; settlements.len()])
        .bind(&s_buyers)
        .bind(&s_sellers)
        .bind(&s_buy_orders)
        .bind(&s_sell_orders)
        .bind(&s_amounts)
        .bind(&s_prices)
        .bind(&s_totals)
        .bind(&s_fees)
        .bind(&s_wh)
        .bind(&s_lf)
        .bind(&s_lc)
        .bind(&s_ee)
        .bind(&s_bz)
        .bind(&s_sz)
        .bind(&s_net)
        .bind(&vec!["completed".to_string(); settlements.len()])
        .bind(&s_buy_sigs)
        .bind(&s_sell_sigs)
        .bind(&s_buy_payloads)
        .bind(&s_sell_payloads)
        .bind(&vec![Utc::now(); settlements.len()])
        .bind(&vec![Utc::now(); settlements.len()])
        .execute(&mut *tx)
        .await?;

        // 2.4 Update Epoch Statistics
        sqlx::query!(
            "UPDATE market_epochs SET total_volume = COALESCE(total_volume, 0) + $1, matched_count = COALESCE(matched_count, 0) + $2, clearing_price = $3 WHERE id = $4",
        )
        .bind(total_volume)
        .bind(total_match_count as i32)
        .bind(if total_volume > Decimal::ZERO { settlements.iter().map(|s| s.total_amount).sum::<Decimal>() / total_volume } else { Decimal::ZERO })
        .bind(epoch_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        // Step 3: Post-Commit Processing (WebSockets, On-Chain Settlement, Notifications)
        info!("🤝 Post-commit processing for {} settlements", settlements.len());
        for settlement in &settlements {
             // 3.1 Collect Batch WebSocket Event
             websocket_events.push(MarketEvent::TradeExecuted {
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
            
            // 3.2 Automated REC Issuance (Async)
            self.trigger_rec_issuance_async(settlement).await;

            // 3.3 Execute On-Chain Settlement or Escrow Release (Background Task - O(1) Latency)
            let service_clone = self.clone();
            let settlement_clone = settlement.clone();
            let _wallets_clone = decrypted_wallets.clone();
            
            tokio::spawn(async move {
                if let (Some(b_sig), Some(s_sig), Some(b_payload), Some(s_payload)) = 
                    (settlement_clone.buy_signature, settlement_clone.sell_signature, settlement_clone.buy_payload, settlement_clone.sell_payload) 
                {
                    let market_pubkey = service_clone.blockchain_service.trading_program_id().unwrap_or_default();
                    let (market_pda, _) = Pubkey::find_program_address(&[b"market"], &market_pubkey);

                    match service_clone.execute_offchain_settlement(
                        &market_pda, settlement_clone.buyer_id, settlement_clone.seller_id,
                        &b_sig, &b_payload, &s_sig, &s_payload,
                        settlement_clone.energy_amount, settlement_clone.price_per_kwh, 
                        settlement_clone.wheeling_charge, settlement_clone.loss_cost,
                    ).await {
                        Ok(sig) => info!("✅ On-chain settlement successful for trade {}: {}", settlement_clone.id, sig),
                        Err(e) => error!("❌ On-chain settlement failed for trade {}: {}", settlement_clone.id, e),
                    }
                } else {
                     // Mock Escrow Releases (Note: These would also benefit from pre-decrypted keys in real implementation)
                     let _ = service_clone.execute_escrow_release(settlement_clone.seller_id, settlement_clone.net_amount, "currency").await;
                     let _ = service_clone.execute_escrow_release(settlement_clone.buyer_id, settlement_clone.effective_energy, "energy").await;
                }
            });
        }

        // 3.4 Batch Broadcast all events (Stubbed)
        // self.websocket_service.broadcast_batch(websocket_events).await;

        let _ = self.broadcast_depth_update().await;
        info!("🏆 MATCHING COMPLETE: {} trades, {} kWh total volume", matches.len(), total_volume);
        Ok(matches)
    }

    /// Pure memory helper for settlement preparation
    async fn prepare_settlement_memory(
        &self, 
        order_match: &OrderMatch,
        buy_order: &crate::domain::trading::clearing::types::OrderBookEntry,
        sell_order: &crate::domain::trading::clearing::types::OrderBookEntry,
        settlement_id: Uuid,
        zone_cost_cache: &mut std::collections::HashMap<(i32, i32), (Decimal, Decimal, Decimal, Decimal)>,
    ) -> Settlement {
        let total_amount = order_match.matched_amount * order_match.match_price;
        let fee_rate = Decimal::from_str("0.01").unwrap_or_else(|_| Decimal::from_parts(1, 0, 0, false, 2)); 
        let fee_amount = total_amount * fee_rate;

        // Physical Grid Logic (Wheeling + Losses)
        let mut wheeling_charge = Decimal::ZERO;
        let mut loss_factor = Decimal::ZERO;
        let mut loss_cost = Decimal::ZERO;
        let mut effective_energy = order_match.matched_amount;

        if let (Some(b_zone), Some(s_zone)) = (buy_order.zone_id, sell_order.zone_id) {
            let zone_pair = (b_zone, s_zone);
            
            if let Some((cached_wh, cached_lf, cached_lc, cached_ee)) = zone_cost_cache.get(&zone_pair) {
                wheeling_charge = *cached_wh;
                loss_factor = *cached_lf;
                loss_cost = *cached_lc;
                effective_energy = *cached_ee;
            } else {
                // Fallback Logic (Simulation of Grid Physics based on zone distance)
                let distance = (b_zone - s_zone).unsigned_abs() as f64;
                let wh_rate = 0.02 + (0.01 * distance);
                let matched_f64 = order_match.matched_amount.to_f64().unwrap_or(0.0);
                let price_f64 = order_match.match_price.to_f64().unwrap_or(0.0);
                
                wheeling_charge = Decimal::from_f64(wh_rate * matched_f64).unwrap_or(Decimal::ZERO);
                loss_factor = Decimal::from_f64(0.01 + (0.005 * distance)).unwrap_or(Decimal::ZERO);
                loss_cost = Decimal::from_f64(matched_f64 * price_f64 * (0.01 + 0.005 * distance)).unwrap_or(Decimal::ZERO);
                effective_energy = order_match.matched_amount * (Decimal::ONE - loss_factor);
                
                zone_cost_cache.insert(zone_pair, (wheeling_charge, loss_factor, loss_cost, effective_energy));
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
            let seller_row = sqlx::query!("SELECT wallet_address FROM users WHERE id = $1").bind(seller_id).fetch_optional(&db).await;
            let wallet_addr = seller_row.ok().flatten().and_then(|r| r.get::<Option<String>, _>("wallet_address")).unwrap_or_default();
            
            let cert_request = crate::services::erc::IssueErcRequest {
                wallet_address: wallet_addr,
                meter_id: None,
                kwh_amount: energy_amount,
                expiry_date: Some(Utc::now() + chrono::Duration::days(365)),
                metadata: Some(serde_json::json!({ "renewable_source": "Solar", "settlement_id": settlement_id })),
            };

            let _ = erc_service.issue_certificate(seller_id, "PlatformAuthority", cert_request, Some(settlement_id)).await;
        });
    }
}
