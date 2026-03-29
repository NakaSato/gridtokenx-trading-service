use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use sqlx::Row;
use std::collections::VecDeque;
use tracing::{error, info};
use ulid::Ulid;
use uuid::Uuid;

use super::types::{OrderBookEntry, Settlement};
use super::MarketClearingService;
use crate::core::error::ApiError;
use crate::infra::blockchain::BlockchainService;
use crate::infra::db::schema::types::{OrderSide, OrderStatus, OrderType};

impl MarketClearingService {
    /// Get current order book for an epoch (P1 Optimization: Returns VecDeque for O(1) pop_front)
    pub async fn get_order_book(
        &self,
        epoch_id: Uuid,
    ) -> Result<(VecDeque<OrderBookEntry>, VecDeque<OrderBookEntry>)> {
        info!("Getting order book for epoch: {}", epoch_id);

        // Get pending buy orders (sorted by price descending, then time ascending)
        // energy_amount in the query is the remaining amount (original - filled)
        let buy_orders: Vec<OrderBookEntry> = sqlx::query_as::<_, OrderBookEntry>(
            r#"
            SELECT 
                id as order_id, user_id, side, 
                (energy_amount - COALESCE(filled_amount, 0)) as energy_amount,
                energy_amount as original_amount,
                price_per_kwh, created_at, zone_id,
                session_token, signature, payload_bytes
            FROM trading_orders 
            WHERE status IN ('pending', 'active', 'partially_filled') AND side = 'buy' AND epoch_id = $1 AND price_per_kwh IS NOT NULL
            ORDER BY price_per_kwh DESC, created_at ASC
            "#
        )
        .bind(epoch_id)
        .fetch_all(&self.db)
        .await?;

        let buy_orders: VecDeque<OrderBookEntry> = buy_orders.into();

        info!(
            "Found {} buy orders for epoch {}",
            buy_orders.len(),
            epoch_id
        );

        // Get pending sell orders (sorted by price ascending, then time ascending)
        let sell_orders: Vec<OrderBookEntry> = sqlx::query_as::<_, OrderBookEntry>(
            r#"
            SELECT 
                id as order_id, user_id, side, 
                (energy_amount - COALESCE(filled_amount, 0)) as energy_amount,
                energy_amount as original_amount,
                price_per_kwh, created_at, zone_id,
                session_token, signature, payload_bytes
            FROM trading_orders 
            WHERE status IN ('pending', 'active', 'partially_filled') AND side = 'sell' AND epoch_id = $1 AND price_per_kwh IS NOT NULL
            ORDER BY price_per_kwh ASC, created_at ASC
            "#
        )
        .bind(epoch_id)
        .fetch_all(&self.db)
        .await?;

        let sell_orders: VecDeque<OrderBookEntry> = sell_orders.into();

        info!(
            "Found {} sell orders for epoch {}",
            sell_orders.len(),
            epoch_id
        );

        Ok((buy_orders, sell_orders))
    }

    /// Create a new trading order (DB and On-Chain)
    pub async fn create_order(
        &self,
        user_id: Uuid,
        side: OrderSide,
        order_type: OrderType,
        energy_amount: Decimal,
        price_per_kwh: Option<Decimal>,
        expiry_time: Option<DateTime<Utc>>,
        zone_id: Option<i32>,
        meter_id: Option<Uuid>,
        session_token: Option<&str>,
    ) -> Result<crate::domain::trading::models::TradingOrderDb> {
        info!(
            "Creating order in MarketClearingService for user: {}, meter: {:?}",
            user_id, meter_id
        );

        if energy_amount <= Decimal::ZERO {
            return Err(anyhow::anyhow!("Energy amount must be positive"));
        }

        let price_per_kwh_val = match order_type {
            OrderType::Limit => {
                let price = price_per_kwh
                    .ok_or_else(|| anyhow::anyhow!("Price per kWh is required for Limit orders"))?;
                if price <= Decimal::ZERO {
                    return Err(anyhow::anyhow!("Price per kWh must be positive"));
                }
                price
            }
            OrderType::Market => Decimal::ZERO,
        };

        // Generate ULID for time-ordered, lexicographically sortable IDs
        // This prevents index fragmentation in the B-tree compared to random UUIDs
        let order_ulid = Ulid::new();
        let order_id = Uuid::from_bytes(order_ulid.to_bytes());
        let now = Utc::now();
        let expires_at = expiry_time.unwrap_or_else(|| now + Duration::days(1));

        // Get or create current epoch
        let epoch = self.get_or_create_epoch(now).await?;

        // 1. Start transaction
        let mut tx = self.db.begin().await?;

        // 2. Insert order into DB (Must process first to satisfy FK for escrow_records)
        sqlx::query(
            r#"
            INSERT INTO trading_orders (
                id, user_id, order_type, side, energy_amount, price_per_kwh,
                filled_amount, status, expires_at, created_at, epoch_id, zone_id, meter_id,
                is_confidential
            ) VALUES ($1, $2, $3::order_type, $4::order_side, $5, $6, $7, $8::order_status, $9, $10, $11, $12, $13, $14)
            "#
        )
        .bind(order_id)
        .bind(user_id)
        .bind(order_type.to_string().to_lowercase())
        .bind(side.to_string().to_lowercase())
        .bind(energy_amount)
        .bind(price_per_kwh_val)
        .bind(Decimal::ZERO)
        .bind("pending")
        .bind(expires_at)
        .bind(now)
        .bind(epoch.id)
        .bind(zone_id)
        .bind(meter_id)
        .bind(false)
        .execute(&mut *tx)
        .await?;

        // 3. Handle Escrow (Lock Funds/Energy)
        match side {
            OrderSide::Buy => {
                // Buffer to cover network charges (wheeling + loss)
                // Max wheeling: 2.0, Max loss: 15%
                let buffer_per_kwh: Decimal = <Decimal as FromPrimitive>::from_f64(2.0)
                    .unwrap_or_default()
                    + (price_per_kwh_val
                        * <Decimal as FromPrimitive>::from_f64(0.15).unwrap_or_default());
                let total_escrow_amount: Decimal =
                    energy_amount * (price_per_kwh_val + buffer_per_kwh);

                // On-chain balance check (if enabled)
                let use_onchain_balance = self.config.tokenization.use_onchain_balance_for_escrow;
                if use_onchain_balance {
                    if let Ok(wallet_addr) = sqlx::query_scalar::<_, Option<String>>(
                        "SELECT wallet_address FROM users WHERE id = $1",
                    )
                    .bind(user_id)
                    .fetch_one(&self.db)
                    .await
                    {
                        if let Some(wallet) = wallet_addr {
                            if let Ok(wallet_pubkey) = BlockchainService::parse_pubkey(&wallet) {
                                let currency_mint_str =
                                    std::env::var("CURRENCY_TOKEN_MINT").unwrap_or_default();
                                if let Ok(mint_pubkey) =
                                    BlockchainService::parse_pubkey(&currency_mint_str)
                                {
                                    match self
                                        .blockchain_service
                                        .get_token_balance(&wallet_pubkey, &mint_pubkey)
                                        .await
                                    {
                                        Ok(onchain_balance) => {
                                            // Convert total_escrow_amount to atomic units for comparison
                                            let required_decimal: rust_decimal::Decimal =
                                                total_escrow_amount
                                                    * rust_decimal::Decimal::from(1_000_000i64);
                                            let required_atomic: u64 =
                                                required_decimal.trunc().to_u64().unwrap_or(0);
                                            if onchain_balance < required_atomic {
                                                return Err(anyhow::anyhow!(
                                                    "Insufficient on-chain balance for escrow. Required: {} (atomic), Available: {} (atomic)",
                                                    required_atomic, onchain_balance
                                                ));
                                            }
                                            info!("✅ On-chain balance check passed for user {}: {} >= {}", user_id, onchain_balance, required_atomic);
                                        }
                                        Err(e) => {
                                            tracing::warn!("On-chain balance check failed, falling back to DB check: {}", e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // DB balance check (always performed as authoritative source)
                let user = sqlx::query("SELECT balance FROM users WHERE id = $1 FOR UPDATE")
                    .bind(user_id)
                    .fetch_one(&mut *tx)
                    .await?;

                let balance: Decimal = user
                    .get::<Option<Decimal>, _>("balance")
                    .unwrap_or(Decimal::ZERO);
                if balance < total_escrow_amount {
                    return Err(anyhow::anyhow!(
                        "Insufficient balance for escrow. Required: {}, Available: {}",
                        total_escrow_amount,
                        balance
                    ));
                }

                // Update user balance and locked_amount
                sqlx::query("UPDATE users SET balance = balance - $1, locked_amount = locked_amount + $1 WHERE id = $2")
                    .bind(total_escrow_amount)
                    .bind(user_id)
                    .execute(&mut *tx)
                    .await?;

                // Create escrow record
                sqlx::query(
                    r#"
                    INSERT INTO escrow_records (
                        user_id, order_id, amount, asset_type, escrow_type, status, description
                    ) VALUES ($1, $2, $3, 'currency', 'buy_lock', 'locked', $4)
                    "#,
                )
                .bind(user_id)
                .bind(order_id)
                .bind(total_escrow_amount)
                .bind(format!("Buy order {} escrow", order_id))
                .execute(&mut *tx)
                .await?;
            }
            OrderSide::Sell => {
                // Lock energy in DB
                sqlx::query("UPDATE users SET locked_energy = locked_energy + $1 WHERE id = $2")
                    .bind(energy_amount)
                    .bind(user_id)
                    .execute(&mut *tx)
                    .await?;

                sqlx::query(
                    r#"
                    INSERT INTO escrow_records (
                        user_id, order_id, amount, asset_type, escrow_type, status, description
                    ) VALUES ($1, $2, $3, 'energy', 'sell_lock', 'locked', $4)
                    "#,
                )
                .bind(user_id)
                .bind(order_id)
                .bind(energy_amount)
                .bind(format!("Sell order {} energy lock", order_id))
                .execute(&mut *tx)
                .await?;
            }
        }

        tx.commit().await?;

        info!(
            "Created order {} for user {} with assets escrowed",
            order_id, user_id
        );

        // Broadcast order created event
        self.websocket_service
            .broadcast_order_created(
                order_id.to_string(),
                energy_amount,
                price_per_kwh_val,
                match side {
                    OrderSide::Buy => None,
                    OrderSide::Sell => Some("solar".to_string()), // Simplified assumption
                },
                user_id.to_string(),
            )
            .await;

        // 2. Audit Log (Stubbed)
        // self.audit_logger.log_async(crate::infra::logging::audit::AuditEvent::OrderCreated {
        //     user_id,
        //     order_id,
        //     order_type: format!("{:?}", side),
        //     amount: energy_amount.to_string(),
        //     price: price_per_kwh_val.to_string(),
        // });

        // 3. On-Chain Order Creation
        self.execute_on_chain_order_creation(
            user_id,
            order_id,
            side,
            energy_amount,
            price_per_kwh_val,
            session_token,
            zone_id,
        )
        .await?;

        // 4. Real-Time Market Depth Update
        let _ = self.broadcast_depth_update().await;

        let order = crate::domain::trading::models::TradingOrderDb {
            id: order_id,
            user_id,
            energy_amount,
            price_per_kwh: price_per_kwh_val,
            filled_amount: Some(Decimal::ZERO),
            epoch_id: Some(epoch.id),
            zone_id,
            order_type,
            side,
            status: OrderStatus::Pending,
            expires_at: Some(expires_at),
            created_at: Some(now),
            filled_at: None,
            meter_id,
            refund_tx_signature: None,
            order_pda: None,
            order_index: None,
            session_token: session_token.map(|s| s.to_string()),
            trigger_price: None,
            trigger_type: None,
            trigger_status: None,
            trailing_offset: None,
            triggered_at: None,
            last_peak_price: None,
        };

        Ok(order)
    }

    pub async fn relay_order(
        &self,
        user_id: Uuid,
        order_id: Uuid,
        side: OrderSide,
        energy_amount: Decimal,
        price_per_kwh: Decimal,
        zone_id: i32,
        signature: String,
        payload_bytes: Vec<u8>,
    ) -> Result<()> {
        info!(
            "Relaying order in MarketClearingService for user: {}, order: {}",
            user_id, order_id
        );

        let now = Utc::now();
        let expires_at = now + Duration::days(1); // Default expiry for relayed orders if not specified

        // Get or create current epoch
        let epoch = self.get_or_create_epoch(now).await?;

        // 1. Start transaction
        let mut tx = self.db.begin().await?;

        // 2. Insert relayed order into DB
        sqlx::query(
            r#"
            INSERT INTO trading_orders (
                id, user_id, order_type, side, energy_amount, price_per_kwh,
                filled_amount, status, expires_at, created_at, epoch_id, zone_id, 
                signature, payload_bytes, is_confidential
            ) VALUES ($1, $2, $3::order_type, $4::order_side, $5, $6, $7, $8::order_status, $9, $10, $11, $12, $13, $14, $15)
            "#
        )
        .bind(order_id)
        .bind(user_id)
        .bind("market") 
        .bind(side.to_string().to_lowercase())
        .bind(energy_amount)
        .bind(price_per_kwh)
        .bind(Decimal::ZERO)
        .bind("pending")
        .bind(expires_at)
        .bind(now)
        .bind(epoch.id)
        .bind(zone_id)
        .bind(signature)
        .bind(payload_bytes)
        .bind(false)
        .execute(&mut *tx)
        .await?;

        // 3. Handle Escrow (Lock Funds/Energy) - Same logic as create_order
        match side {
            OrderSide::Buy => {
                let buffer_per_kwh: Decimal = <Decimal as FromPrimitive>::from_f64(2.0)
                    .unwrap_or_default()
                    + (price_per_kwh
                        * <Decimal as FromPrimitive>::from_f64(0.15).unwrap_or_default());
                let total_escrow_amount: Decimal = energy_amount * (price_per_kwh + buffer_per_kwh);

                let user = sqlx::query("SELECT balance FROM users WHERE id = $1 FOR UPDATE")
                    .bind(user_id)
                    .fetch_one(&mut *tx)
                    .await?;

                let user_balance: Decimal = user
                    .get::<Option<Decimal>, _>("balance")
                    .unwrap_or(Decimal::ZERO);
                if user_balance < total_escrow_amount {
                    return Err(anyhow::anyhow!(
                        "Insufficient balance for escrow. Required: {}, Available: {}",
                        total_escrow_amount,
                        user_balance
                    ));
                }

                sqlx::query("UPDATE users SET balance = balance - $1, locked_amount = locked_amount + $1 WHERE id = $2")
                    .bind(total_escrow_amount)
                    .bind(user_id)
                    .execute(&mut *tx)
                    .await?;

                sqlx::query(
                    r#"
                    INSERT INTO escrow_records (
                        user_id, order_id, amount, asset_type, escrow_type, status, description
                    ) VALUES ($1, $2, $3, 'currency', 'buy_lock', 'locked', $4)
                    "#,
                )
                .bind(user_id)
                .bind(order_id)
                .bind(total_escrow_amount)
                .bind(format!("Relayed Buy order {} escrow", order_id))
                .execute(&mut *tx)
                .await?;
            }
            OrderSide::Sell => {
                sqlx::query("UPDATE users SET locked_energy = locked_energy + $1 WHERE id = $2")
                    .bind(energy_amount)
                    .bind(user_id)
                    .execute(&mut *tx)
                    .await?;

                sqlx::query(
                    r#"
                    INSERT INTO escrow_records (
                        user_id, order_id, amount, asset_type, escrow_type, status, description
                    ) VALUES ($1, $2, $3, 'energy', 'sell_lock', 'locked', $4)
                    "#,
                )
                .bind(user_id)
                .bind(order_id)
                .bind(energy_amount)
                .bind(format!("Relayed Sell order {} energy lock", order_id))
                .execute(&mut *tx)
                .await?;
            }
        }

        tx.commit().await?;

        // broadcast and audit
        self.websocket_service
            .broadcast_order_created(
                order_id.to_string(),
                energy_amount,
                price_per_kwh,
                match side {
                    OrderSide::Buy => None,
                    OrderSide::Sell => Some("solar".to_string()),
                },
                user_id.to_string(),
            )
            .await;

        // self.audit_logger.log_async(crate::infra::logging::audit::AuditEvent::OrderCreated {
        //     user_id,
        //     order_id,
        //     order_type: format!("Relayed {:?}", side),
        //     amount: energy_amount.to_string(),
        //     price: price_per_kwh.to_string(),
        // });

        // Real-Time Market Depth Update
        let _ = self.broadcast_depth_update().await;

        Ok(())
    }

    /// Cancel an order and refund the unfilled escrow amount
    pub async fn cancel_order(&self, order_id: Uuid, user_id: Uuid) -> Result<()> {
        // use crate::api::handlers::websocket::broadcaster::broadcast_p2p_order_update;

        // Get full order details including filled amount
        let order = sqlx::query(
            r#"
            SELECT user_id, side as side, status as status, 
                   energy_amount, filled_amount, price_per_kwh as price_per_kwh
            FROM trading_orders 
            WHERE id = $1
            "#,
        )
        .bind(order_id)
        .fetch_optional(&self.db)
        .await?;

        if let Some(order) = order {
            let order_user_id: Uuid = order.get("user_id");
            if order_user_id != user_id {
                return Err(
                    ApiError::Forbidden("Order does not belong to user".to_string()).into(),
                );
            }

            let status: OrderStatus = order.get("status");
            if status != OrderStatus::Pending
                && status != OrderStatus::Active
                && status != OrderStatus::PartiallyFilled
            {
                return Err(anyhow::anyhow!("Cannot cancel order in status: {}", status));
            }

            // Calculate unfilled amount that needs to be refunded
            let filled_amount: Decimal = order.get("filled_amount");
            let energy_amount: Decimal = order.get("energy_amount");
            let refund_amount = energy_amount - filled_amount;
            let side: OrderSide = order.get("side");
            let price_per_kwh: Decimal = order.get("price_per_kwh");

            if refund_amount <= Decimal::ZERO {
                return Err(ApiError::BadRequest(
                    "Order is fully filled and cannot be cancelled".to_string(),
                )
                .into());
            }

            // Start transaction for atomicity
            let mut tx = self.db.begin().await?;

            // Refund based on order side
            match side {
                OrderSide::Buy => {
                    // Return locked funds for unfilled portion
                    let total_refund = refund_amount * price_per_kwh;
                    sqlx::query("UPDATE users SET balance = balance + $1, locked_amount = locked_amount - $1 WHERE id = $2")
                        .bind(total_refund)
                        .bind(user_id)
                        .execute(&mut *tx)
                        .await?;

                    info!(
                        "Refunded {} to user {} for cancelled buy order {} (unfilled: {} kWh @ {})",
                        total_refund, user_id, order_id, refund_amount, price_per_kwh
                    );
                }
                OrderSide::Sell => {
                    // Return locked energy for unfilled portion
                    sqlx::query(
                        "UPDATE users SET locked_energy = locked_energy - $1 WHERE id = $2",
                    )
                    .bind(refund_amount)
                    .bind(user_id)
                    .execute(&mut *tx)
                    .await?;

                    info!(
                        "Unlocked {} kWh energy for user {} from cancelled sell order {}",
                        refund_amount, user_id, order_id
                    );
                }
            }

            // Update escrow record status
            sqlx::query("UPDATE escrow_records SET status = 'released', description = $1, updated_at = NOW() WHERE order_id = $2 AND status = 'locked'")
                .bind(format!("Order cancelled - refunded unfilled portion: {}", refund_amount))
                .bind(order_id)
                .execute(&mut *tx)
                .await?;

            // Update order status to cancelled
            sqlx::query(
                "UPDATE trading_orders SET status = 'cancelled', updated_at = NOW() WHERE id = $1",
            )
            .bind(order_id)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;

            info!(
                "Cancelled order {} for user {} and refunded unfilled portion",
                order_id, user_id
            );
            // Broadcast cancellation via WebSocket
            /*
            let _ = broadcast_p2p_order_update(
                order_id,
                user_id,
                match order.side {
                    OrderSide::Buy => "buy".to_string(),
                    OrderSide::Sell => "sell".to_string(),
                },
                "cancelled".to_string(),
                original.to_string(),
                filled.to_string(),
                "0".to_string(), // remaining is 0 after cancel
                price.to_string(),
            ).await;
            */

            info!(
                "Order {} cancelled by user {} (filled: {}, refunded: {})",
                order_id, user_id, filled_amount, refund_amount
            );

            // Execute On-Chain Refund
            // Buy Order -> Refund Currency (unfilled * price)
            // Sell Order -> Refund Energy (unfilled)
            let (asset_type, refund_amount_val) = match side {
                OrderSide::Buy => ("currency", refund_amount * price_per_kwh),
                OrderSide::Sell => ("energy", refund_amount),
            };

            if refund_amount_val > Decimal::ZERO {
                match self
                    .execute_escrow_refund(user_id, refund_amount_val, asset_type)
                    .await
                {
                    Ok(sig) => {
                        info!(
                            "On-chain escrow refund executed for order {}: {}",
                            order_id, sig
                        );
                    }
                    Err(e) => {
                        // Check for existing order in DB if needed (e.g. idempotence)
                        // ...
                        // Critical error if DB refunded but Chain failed.
                        // For now, we log it. In a real system, this needs a reconciliation queue.
                        error!(
                            "Failed to execute on-chain refund for order {}: {}",
                            order_id, e
                        );
                    }
                }
            }

            // Real-Time Market Depth Update
            let _ = self.broadcast_depth_update().await;
        } else {
            return Err(ApiError::NotFound("Order not found".to_string()).into());
        }

        Ok(())
    }

    /// Get trading history for a user
    pub async fn get_trading_history(
        &self,
        user_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<Settlement>> {
        let settlements = sqlx::query_as::<_, Settlement>(
            r#"
            SELECT 
                id, epoch_id, buyer_id, seller_id, 
                buy_order_id, sell_order_id,
                energy_amount, price_per_kwh, 
                total_amount, fee_amount, 
                wheeling_charge, loss_factor, 
                loss_cost, effective_energy, 
                buyer_zone_id, seller_zone_id, 
                net_amount, status,
                buyer_session_token, seller_session_token,
                buy_signature, sell_signature,
                buy_payload, sell_payload,
                retry_count, error_message
            FROM settlements 
            WHERE buyer_id = $1 OR seller_id = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(user_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.db)
        .await?;

        Ok(settlements)
    }
}
