use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use std::str::FromStr;

use tracing::info;
use ulid::Ulid;
use uuid::Uuid;

use crate::core::error::{ApiError, Result};
use crate::domain::trading::clearing::TradeMatch;

/// Settlement status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SettlementStatus {
    Pending,
    Processing,
    Completed,
    Failed,
    PermanentlyFailed,
}

impl std::fmt::Display for SettlementStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Processing => write!(f, "processing"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
            Self::PermanentlyFailed => write!(f, "permanently_failed"),
        }
    }
}

/// Settlement record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settlement {
    pub id: Uuid,
    pub trade_id: Uuid,
    pub buyer_id: Uuid,
    pub seller_id: Uuid,
    pub buy_order_id: Uuid,
    pub sell_order_id: Uuid,
    pub energy_amount: Decimal,
    pub price: Decimal,
    pub total_value: Decimal,
    pub fee_amount: Decimal,
    pub net_amount: Decimal,
    pub status: SettlementStatus,
    pub blockchain_tx: Option<String>,
    pub created_at: DateTime<Utc>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub buyer_zone_id: Option<i32>,
    pub seller_zone_id: Option<i32>,
    pub wheeling_charge: Option<Decimal>,
    pub loss_cost: Option<Decimal>,
    pub loss_factor: Option<Decimal>,
    pub effective_energy: Option<Decimal>,
    pub buyer_session_token: Option<String>,
    pub seller_session_token: Option<String>,
    pub erc_certificate_id: Option<String>,
    pub erc_transfer_tx: Option<String>,
    pub epoch_id: Uuid,
}

/// Settlement transaction result
#[derive(Debug, Clone, Serialize)]
pub struct SettlementTransaction {
    pub settlement_id: Uuid,
    pub signature: String,
    pub slot: u64,
    pub confirmation_status: String,
}

/// Context for batching a settlement on-chain
#[derive(Debug, Clone)]
pub struct SettlementBatchContext {
    pub settlement: Settlement,
    pub buyer_wallet: Pubkey,
    pub seller_wallet: Pubkey,
    pub buy_order_pda: Pubkey,
    pub sell_order_pda: Pubkey,
    pub buy_order_index: Option<u64>,
    pub sell_order_index: Option<u64>,
}

use solana_sdk::pubkey::Pubkey;

/// Settlement service configuration
#[derive(Debug, Clone)]
pub struct SettlementConfig {
    pub fee_rate: Decimal,
    pub min_confirmation_blocks: u64,
    pub retry_attempts: u32,
    pub retry_delay_secs: u64,
    pub enable_real_blockchain: bool,
    pub max_batch_size: usize,
    pub priority_fee_micro_lamports: u64,
}

impl Default for SettlementConfig {
    fn default() -> Self {
        Self {
            fee_rate: Decimal::from_str("0.01").expect("valid hardcoded decimal 0.01"),
            min_confirmation_blocks: 32,
            retry_attempts: 3,
            retry_delay_secs: 5,
            enable_real_blockchain: true,
            max_batch_size: 10,
            priority_fee_micro_lamports: 5000,
        }
    }
}

impl SettlementConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();
        if let Ok(val) = std::env::var("SETTLEMENT_FEE_RATE") {
            if let Ok(rate) = Decimal::from_str(&val) {
                config.fee_rate = rate;
            }
        }
        if let Ok(val) = std::env::var("TOKENIZATION_ENABLE_REAL_BLOCKCHAIN") {
            if let Ok(enabled) = val.parse::<bool>() {
                config.enable_real_blockchain = enabled;
            }
        }
        if let Ok(val) = std::env::var("SETTLEMENT_MAX_BATCH_SIZE") {
            if let Ok(size) = val.parse::<usize>() {
                config.max_batch_size = size;
            }
        }
        if let Ok(val) = std::env::var("SETTLEMENT_PRIORITY_FEE") {
            if let Ok(fee) = val.parse::<u64>() {
                config.priority_fee_micro_lamports = fee;
            }
        }
        config
    }
}

/// Settlement statistics
#[derive(Debug, Clone, Serialize)]
pub struct SettlementStats {
    pub pending_count: i64,
    pub processing_count: i64,
    pub confirmed_count: i64,
    pub failed_count: i64,
    pub total_settled_value: Decimal,
}

/// Domain service for settlement business logic
#[derive(Debug)]
pub struct SettlementManager {
    db: PgPool,
    pub config: SettlementConfig,
}

impl SettlementManager {
    pub fn new(db: PgPool, config: SettlementConfig) -> Self {
        Self { db, config }
    }

    /// Expose the database pool for cross-domain queries (e.g., meter ownership lookup)
    pub fn db_pool(&self) -> PgPool {
        self.db.clone()
    }

    /// Create a settlement record from a trade match (Business Logic)
    pub async fn create_settlement_record(&self, trade: &TradeMatch) -> Result<Settlement> {
        info!(
            "Creating settlement record for trade match: {} (Match: {})",
            trade.id, trade.match_id
        );

        let total_value = trade.total_value;
        let fee_rate = self.config.fee_rate;

        let loss_cost = trade.loss_cost;
        let wheeling_charge = trade.wheeling_charge;

        let seller_base_price_total = total_value - wheeling_charge - loss_cost;
        let fee_amount = seller_base_price_total * fee_rate;
        let net_amount = seller_base_price_total - fee_amount;

        if net_amount < Decimal::ZERO {
            return Err(ApiError::BadRequest(format!(
                "Settlement would result in negative seller balance: net={}",
                net_amount
            )));
        }

        let effective_energy = trade.quantity * (Decimal::ONE - trade.loss_factor);

        let settlement_ulid = Ulid::new();
        let settlement_id = Uuid::from_bytes(settlement_ulid.to_bytes());

        let settlement = Settlement {
            id: settlement_id,
            trade_id: trade.id,
            buyer_id: trade.buyer_id,
            seller_id: trade.seller_id,
            buy_order_id: trade.buy_order_id,
            sell_order_id: trade.sell_order_id,
            energy_amount: trade.quantity,
            price: trade.price,
            total_value,
            fee_amount,
            net_amount,
            status: SettlementStatus::Pending,
            blockchain_tx: None,
            created_at: Utc::now(),
            confirmed_at: None,
            buyer_zone_id: trade.buyer_zone_id,
            seller_zone_id: trade.seller_zone_id,
            wheeling_charge: Some(wheeling_charge),
            loss_factor: Some(trade.loss_factor),
            loss_cost: Some(loss_cost),
            effective_energy: Some(effective_energy),
            buyer_session_token: trade.buyer_session_token.clone(),
            seller_session_token: trade.seller_session_token.clone(),
            erc_certificate_id: None,
            erc_transfer_tx: None,
            epoch_id: trade.epoch_id,
        };

        sqlx::query(
            r#"
            INSERT INTO settlements (
                id, buyer_id, seller_id, buy_order_id, sell_order_id,
                energy_amount, price_per_kwh, total_amount, fee_amount, net_amount, status, created_at,
                wheeling_charge, loss_factor, loss_cost, effective_energy, buyer_zone_id, seller_zone_id, epoch_id,
                buyer_session_token, seller_session_token
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
            "#,
        )
        .bind(settlement.id)
        .bind(settlement.buyer_id)
        .bind(settlement.seller_id)
        .bind(settlement.buy_order_id)
        .bind(settlement.sell_order_id)
        .bind(settlement.energy_amount)
        .bind(settlement.price)
        .bind(settlement.total_value)
        .bind(settlement.fee_amount)
        .bind(settlement.net_amount)
        .bind(settlement.status.to_string())
        .bind(settlement.created_at)
        .bind(settlement.wheeling_charge)
        .bind(settlement.loss_factor)
        .bind(settlement.loss_cost)
        .bind(settlement.effective_energy)
        .bind(settlement.buyer_zone_id)
        .bind(settlement.seller_zone_id)
        .bind(trade.epoch_id)
        .bind(&settlement.buyer_session_token)
        .bind(&settlement.seller_session_token)
        .execute(&self.db)
        .await?;

        Ok(settlement)
    }

    pub async fn get_settlement(&self, id: Uuid) -> Result<Settlement> {
        let row = sqlx::query(
            r#"
            SELECT
                id, buyer_id, seller_id, buy_order_id, sell_order_id, energy_amount,
                price_per_kwh, total_amount, fee_amount, net_amount,
                status, transaction_hash, created_at, processed_at,
                wheeling_charge, loss_factor, loss_cost, effective_energy, buyer_zone_id, seller_zone_id,
                buyer_session_token, seller_session_token, erc_certificate_id, erc_transfer_tx, epoch_id
            FROM settlements
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_one(&self.db)
        .await
        .map_err(ApiError::Database)?;

        let status_str: String = row.get("status");
        let status = match status_str.as_str() {
            "processing" => SettlementStatus::Processing,
            "completed" | "confirmed" => SettlementStatus::Completed,
            "failed" => SettlementStatus::Failed,
            "permanently_failed" => SettlementStatus::PermanentlyFailed,
            _ => SettlementStatus::Pending,
        };

        Ok(Settlement {
            id: row.get("id"),
            trade_id: Uuid::nil(), // Not directly stored
            buyer_id: row.get("buyer_id"),
            seller_id: row.get("seller_id"),
            buy_order_id: row.get("buy_order_id"),
            sell_order_id: row.get("sell_order_id"),
            energy_amount: row.get("energy_amount"),
            price: row.get("price_per_kwh"),
            total_value: row.get("total_amount"),
            fee_amount: row.get("fee_amount"),
            net_amount: row.get("net_amount"),
            status,
            blockchain_tx: row.get("transaction_hash"),
            created_at: row.get("created_at"),
            confirmed_at: row.get("processed_at"),
            buyer_zone_id: row.get("buyer_zone_id"),
            seller_zone_id: row.get("seller_zone_id"),
            wheeling_charge: row.get("wheeling_charge"),
            loss_factor: row.get("loss_factor"),
            loss_cost: row.get("loss_cost"),
            effective_energy: row.get("effective_energy"),
            buyer_session_token: row.get("buyer_session_token"),
            seller_session_token: row.get("seller_session_token"),
            erc_certificate_id: row.get("erc_certificate_id"),
            erc_transfer_tx: row.get("erc_transfer_tx"),
            epoch_id: row.get("epoch_id"),
        })
    }

    pub async fn get_pending_settlements(&self) -> Result<Vec<Uuid>> {
        let rows = sqlx::query(
            r#"
            SELECT id FROM settlements 
            WHERE status = 'pending' 
            AND (next_retry_at IS NULL OR next_retry_at <= NOW())
            ORDER BY created_at ASC 
            LIMIT 100
            "#,
        )
        .fetch_all(&self.db)
        .await
        .map_err(ApiError::Database)?;

        Ok(rows.into_iter().map(|r| r.get("id")).collect())
    }

    pub async fn update_settlement_status(&self, id: Uuid, status: SettlementStatus) -> Result<()> {
        sqlx::query("UPDATE settlements SET status = $1, updated_at = NOW() WHERE id = $2")
            .bind(status.to_string())
            .bind(id)
            .execute(&self.db)
            .await
            .map_err(ApiError::Database)?;
        Ok(())
    }

    /// Update status for a batch of settlements (single DB call)
    pub async fn update_batch_status(&self, ids: &[Uuid], status: SettlementStatus) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        sqlx::query("UPDATE settlements SET status = $1, updated_at = NOW() WHERE id = ANY($2)")
            .bind(status.to_string())
            .bind(ids)
            .execute(&self.db)
            .await
            .map_err(ApiError::Database)?;
        Ok(())
    }

    /// Mark a settlement as failed, increment retry count with exponential backoff
    pub async fn mark_settlement_failed(&self, id: Uuid, error_message: &str) -> Result<()> {
        let max_retries = self.config.retry_attempts as i32;
        let base_delay = self.config.retry_delay_secs as i32;

        let result = sqlx::query(
            r#"
            UPDATE settlements
            SET 
                retry_count = COALESCE(retry_count, 0) + 1,
                status = CASE 
                    WHEN COALESCE(retry_count, 0) + 1 >= $1 THEN 'failed'
                    ELSE 'pending'
                END,
                next_retry_at = CASE 
                    WHEN COALESCE(retry_count, 0) + 1 < $1 THEN 
                        NOW() + (power(2, COALESCE(retry_count, 0)) * $2 * interval '1 second')
                    ELSE NULL
                END,
                error_message = $3,
                updated_at = NOW()
            WHERE id = $4
            RETURNING status, retry_count, next_retry_at
            "#,
        )
        .bind(max_retries)
        .bind(base_delay)
        .bind(error_message)
        .bind(id)
        .fetch_optional(&self.db)
        .await
        .map_err(ApiError::Database)?;

        if let Some(row) = result {
            let status: String = row.get("status");
            let retry_count: i32 = row.get("retry_count");
            if status == "failed" {
                tracing::error!(
                    "❌ Settlement {} PERMANENTLY FAILED after {} attempts: {}",
                    id,
                    retry_count,
                    error_message
                );
            } else {
                let next_retry: Option<DateTime<Utc>> = row.get("next_retry_at");
                tracing::warn!(
                    "⚠️ Settlement {} failed (Attempt {}). Retrying at {:?}: {}",
                    id,
                    retry_count,
                    next_retry,
                    error_message
                );
            }
        }
        Ok(())
    }

    /// Fetch full context for a batch of settlements, including on-chain addresses
    pub async fn get_batch_context(&self, ids: &[Uuid]) -> Result<Vec<SettlementBatchContext>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let rows = sqlx::query(
            r#"
            SELECT
                s.id as s_id, s.buyer_id, s.seller_id, s.buy_order_id, s.sell_order_id, s.energy_amount,
                s.price_per_kwh, s.total_amount, s.fee_amount, s.net_amount,
                s.status, s.transaction_hash, s.created_at, s.processed_at,
                s.wheeling_charge, s.loss_factor, s.loss_cost, s.effective_energy, s.buyer_zone_id, s.seller_zone_id,
                s.buyer_session_token, s.seller_session_token, s.erc_certificate_id, s.erc_transfer_tx, s.epoch_id,
                u_b.wallet_address as buyer_wallet,
                u_s.wallet_address as seller_wallet,
                o_b.order_pda as buy_order_pda,
                o_s.order_pda as sell_order_pda,
                o_b.order_index as buy_order_index,
                o_s.order_index as sell_order_index
            FROM settlements s
            JOIN users u_b ON s.buyer_id = u_b.id
            JOIN users u_s ON s.seller_id = u_s.id
            JOIN trading_orders o_b ON s.buy_order_id = o_b.id
            JOIN trading_orders o_s ON s.sell_order_id = o_s.id
            WHERE s.id = ANY($1)
            "#
        )
        .bind(ids)
        .fetch_all(&self.db)
        .await
        .map_err(ApiError::Database)?;

        let mut contexts = Vec::new();
        for row in rows {
            let status_str: String = row.get("status");
            let status = match status_str.as_str() {
                "processing" => SettlementStatus::Processing,
                "completed" | "confirmed" => SettlementStatus::Completed,
                "failed" => SettlementStatus::Failed,
                "permanently_failed" => SettlementStatus::PermanentlyFailed,
                _ => SettlementStatus::Pending,
            };

            let settlement = Settlement {
                id: row.get("s_id"),
                trade_id: Uuid::nil(),
                buyer_id: row.get("buyer_id"),
                seller_id: row.get("seller_id"),
                buy_order_id: row.get("buy_order_id"),
                sell_order_id: row.get("sell_order_id"),
                energy_amount: row.get("energy_amount"),
                price: row.get("price_per_kwh"),
                total_value: row.get("total_amount"),
                fee_amount: row.get("fee_amount"),
                net_amount: row.get("net_amount"),
                status,
                blockchain_tx: row.get("transaction_hash"),
                created_at: row.get("created_at"),
                confirmed_at: row.get("processed_at"),
                buyer_zone_id: row.get("buyer_zone_id"),
                seller_zone_id: row.get("seller_zone_id"),
                wheeling_charge: row.get("wheeling_charge"),
                loss_factor: row.get("loss_factor"),
                loss_cost: row.get("loss_cost"),
                effective_energy: row.get("effective_energy"),
                buyer_session_token: row.get("buyer_session_token"),
                seller_session_token: row.get("seller_session_token"),
                erc_certificate_id: row.get("erc_certificate_id"),
                erc_transfer_tx: row.get("erc_transfer_tx"),
                epoch_id: row.get("epoch_id"),
            };

            let buyer_wallet_str: String = row.get("buyer_wallet");
            let seller_wallet_str: String = row.get("seller_wallet");
            let buy_order_pda_str: Option<String> = row.get("buy_order_pda");
            let sell_order_pda_str: Option<String> = row.get("sell_order_pda");
            let buy_order_index: Option<i64> = row.get("buy_order_index");
            let sell_order_index: Option<i64> = row.get("sell_order_index");

            // PDA fallback derivation logic if missing in DB
            let mut buy_order_pda = buy_order_pda_str
                .and_then(|s| Pubkey::from_str(&s).ok())
                .unwrap_or_default();
            let mut sell_order_pda = sell_order_pda_str
                .and_then(|s| Pubkey::from_str(&s).ok())
                .unwrap_or_default();

            let trading_program_id = Pubkey::from_str(
                &std::env::var("NEXT_PUBLIC_TRADING_PROGRAM_ID").unwrap_or_else(|_| "HHAG2cG6sGHTWFwiEh1HBgfqZJWBbnsYzv4f5KtHavUr".to_string()),
            )
            .unwrap_or_default();

            if buy_order_pda == Pubkey::default() {
                if let (Some(idx), Ok(buyer_wallet)) =
                    (buy_order_index, Pubkey::from_str(&buyer_wallet_str))
                {
                    let (pda, _) = Pubkey::find_program_address(
                        &[b"order", buyer_wallet.as_ref(), &(idx as u64).to_le_bytes()],
                        &trading_program_id,
                    );
                    buy_order_pda = pda;
                    info!(
                        "Re-derived Buy Order PDA: {} for order {}",
                        buy_order_pda, settlement.buy_order_id
                    );
                }
            }

            if sell_order_pda == Pubkey::default() {
                if let (Some(idx), Ok(seller_wallet)) =
                    (sell_order_index, Pubkey::from_str(&seller_wallet_str))
                {
                    let (pda, _) = Pubkey::find_program_address(
                        &[
                            b"order",
                            seller_wallet.as_ref(),
                            &(idx as u64).to_le_bytes(),
                        ],
                        &trading_program_id,
                    );
                    sell_order_pda = pda;
                    info!(
                        "Re-derived Sell Order PDA: {} for order {}",
                        sell_order_pda, settlement.sell_order_id
                    );
                }
            }

            contexts.push(SettlementBatchContext {
                settlement,
                buyer_wallet: Pubkey::from_str(&buyer_wallet_str).unwrap_or_default(),
                seller_wallet: Pubkey::from_str(&seller_wallet_str).unwrap_or_default(),
                buy_order_pda,
                sell_order_pda,
                buy_order_index: buy_order_index.map(|idx| idx as u64),
                sell_order_index: sell_order_index.map(|idx| idx as u64),
            });
        }
        Ok(contexts)
    }

    pub async fn update_settlement_confirmed(
        &self,
        id: Uuid,
        tx_signature: &str,
        status: SettlementStatus,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE settlements SET status = $1, transaction_hash = $2, processed_at = NOW(), updated_at = NOW() WHERE id = $3"
        )
        .bind(status.to_string())
        .bind(tx_signature)
        .bind(id)
        .execute(&self.db)
        .await
        .map_err(ApiError::Database)?;
        Ok(())
    }

    /// Update confirmation status for a batch of settlements sharing the same Solana signature
    pub async fn update_batch_confirmed(
        &self,
        ids: &[Uuid],
        tx_signature: &str,
        status: SettlementStatus,
    ) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        sqlx::query(
            "UPDATE settlements SET status = $1, transaction_hash = $2, processed_at = NOW(), updated_at = NOW() WHERE id = ANY($3)"
        )
        .bind(status.to_string())
        .bind(tx_signature)
        .bind(ids)
        .execute(&self.db)
        .await
        .map_err(ApiError::Database)?;
        Ok(())
    }

    pub async fn update_settlement_erc(
        &self,
        id: Uuid,
        erc_certificate_id: &str,
        erc_transfer_tx: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE settlements SET erc_certificate_id = $1, erc_transfer_tx = $2 WHERE id = $3",
        )
        .bind(erc_certificate_id)
        .bind(erc_transfer_tx)
        .bind(id)
        .execute(&self.db)
        .await
        .map_err(ApiError::Database)?;
        Ok(())
    }

    pub async fn finalize_escrow(&self, settlement: &Settlement) -> Result<()> {
        self.finalize_batch_escrow(&[settlement.clone()]).await
    }

    /// Finalize multiple escrows in a single database transaction
    pub async fn finalize_batch_escrow(&self, settlements: &[Settlement]) -> Result<()> {
        if settlements.is_empty() {
            return Ok(());
        }

        let mut tx = self.db.begin().await.map_err(ApiError::Database)?;

        for settlement in settlements {
            sqlx::query(
                "UPDATE users SET locked_energy = GREATEST(0, locked_energy - $1) WHERE id = $2",
            )
            .bind(settlement.energy_amount)
            .bind(settlement.seller_id)
            .execute(&mut *tx)
            .await
            .map_err(ApiError::Database)?;

            sqlx::query(
                "UPDATE users SET locked_amount = GREATEST(0, locked_amount - $1) WHERE id = $2",
            )
            .bind(settlement.total_value)
            .bind(settlement.buyer_id)
            .execute(&mut *tx)
            .await
            .map_err(ApiError::Database)?;

            sqlx::query("UPDATE users SET balance = balance + $1 WHERE id = $2")
                .bind(settlement.net_amount)
                .bind(settlement.seller_id)
                .execute(&mut *tx)
                .await
                .map_err(ApiError::Database)?;

            if settlement.fee_amount > Decimal::ZERO {
                sqlx::query("INSERT INTO platform_revenue (settlement_id, amount, revenue_type, description) VALUES ($1, $2, 'platform_fee', $3)")
                    .bind(settlement.id)
                    .bind(settlement.fee_amount)
                    .bind(format!("Platform fee for settlement {}", settlement.id))
                    .execute(&mut *tx).await.map_err(ApiError::Database)?;
            }

            sqlx::query("UPDATE escrow_records SET status = 'released', updated_at = NOW() WHERE order_id IN ($1, $2) AND status = 'locked'")
                .bind(settlement.buy_order_id)
                .bind(settlement.sell_order_id)
                .execute(&mut *tx).await.map_err(ApiError::Database)?;
        }

        tx.commit().await.map_err(ApiError::Database)?;
        Ok(())
    }

    pub async fn get_settlement_stats(&self) -> Result<SettlementStats> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE status = 'pending') as pending_count,
                COUNT(*) FILTER (WHERE status = 'processing') as processing_count,
                COUNT(*) FILTER (WHERE status = 'completed') as confirmed_count,
                COUNT(*) FILTER (WHERE status = 'failed') as failed_count,
                COALESCE(SUM(CASE WHEN status = 'completed' THEN total_amount ELSE 0 END), 0) as total_settled_value
            FROM settlements
            WHERE created_at > NOW() - INTERVAL '24 hours'
            "#
        )
        .fetch_one(&self.db)
        .await
        .map_err(ApiError::Database)?;

        Ok(SettlementStats {
            pending_count: row.get::<i64, _>("pending_count"),
            processing_count: row.get::<i64, _>("processing_count"),
            confirmed_count: row.get::<i64, _>("confirmed_count"),
            failed_count: row.get::<i64, _>("failed_count"),
            total_settled_value: row.get("total_settled_value"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use crate::domain::trading::clearing::TradeMatch;

    #[test]
    fn test_settlement_calculations() {
        let config = SettlementConfig {
            fee_rate: dec!(0.02), // 2% fee
            ..Default::default()
        };
        
        // Mock a TradeMatch
        // total_value = 100
        // wheeling = 5
        // loss = 5
        // base = 100 - 5 - 5 = 90
        // fee = 90 * 0.02 = 1.8
        // net = 90 - 1.8 = 88.2
        let trade = TradeMatch {
            id: Uuid::new_v4(),
            match_id: Uuid::new_v4(),
            buyer_id: Uuid::new_v4(),
            seller_id: Uuid::new_v4(),
            buy_order_id: Uuid::new_v4(),
            sell_order_id: Uuid::new_v4(),
            quantity: dec!(100),
            price: dec!(1.0),
            total_value: dec!(100),
            wheeling_charge: dec!(5),
            loss_factor: dec!(0.05),
            loss_cost: dec!(5),
            buyer_zone_id: Some(1),
            seller_zone_id: Some(2),
            matched_at: Utc::now(),
            buyer_session_token: None,
            seller_session_token: None,
            epoch_id: Uuid::new_v4(),
            otel_trace_context: None,
        };

        // We can't easily call create_settlement_record due to DB INSERT,
        // but we can verify the Settlement struct logic if we had a pure function.
        // For now, let's verify the math matches our expectations.
        
        let total_value = trade.total_value;
        let fee_rate = config.fee_rate;
        let loss_cost = trade.loss_cost;
        let wheeling_charge = trade.wheeling_charge;

        let seller_base_price_total = total_value - wheeling_charge - loss_cost;
        let fee_amount = seller_base_price_total * fee_rate;
        let net_amount = seller_base_price_total - fee_amount;
        let effective_energy = trade.quantity * (Decimal::ONE - trade.loss_factor);

        assert_eq!(fee_amount, dec!(1.8));
        assert_eq!(net_amount, dec!(88.2));
        assert_eq!(effective_energy, dec!(95.0));
    }

    #[test]
    fn test_settlement_config_from_env() {
        unsafe {
            std::env::set_var("SETTLEMENT_FEE_RATE", "0.05");
            std::env::set_var("SETTLEMENT_MAX_BATCH_SIZE", "50");
        }
        
        let config = SettlementConfig::from_env();
        assert_eq!(config.fee_rate, dec!(0.05));
        assert_eq!(config.max_batch_size, 50);
    }
}
