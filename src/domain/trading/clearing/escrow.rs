use anyhow::Result;
use rust_decimal::Decimal;
use uuid::Uuid;
use super::MarketClearingService;

impl MarketClearingService {
    pub async fn lock_funds(&self, user_id: Uuid, order_id: Uuid, amount: Decimal) -> Result<()> {
        let mut tx = self.db.begin().await?;

        // Check balance
        let user = sqlx::query!("SELECT balance FROM users WHERE id = $1 FOR UPDATE", user_id)
            .fetch_one(&mut *tx)
            .await?;

        if user.balance.unwrap_or(Decimal::ZERO) < amount {
            return Err(anyhow::anyhow!("Insufficient balance for escrow. Required: {}, Available: {}", amount, user.balance.unwrap_or(Decimal::ZERO)));
        }

        // Update user balance and locked_amount
        sqlx::query!(
            "UPDATE users SET balance = balance - $1, locked_amount = locked_amount + $1 WHERE id = $2",
            amount,
            user_id
        )
        .execute(&mut *tx)
        .await?;

        // Create escrow record
        sqlx::query!(
            r#"
            INSERT INTO escrow_records (
                user_id, order_id, amount, asset_type, escrow_type, status, description
            ) VALUES ($1, $2, $3, 'currency', 'buy_lock', 'locked', $4)
            "#,
            user_id,
            order_id,
            amount,
            format!("Buy order {} escrow", order_id)
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn lock_energy(&self, user_id: Uuid, order_id: Uuid, amount: Decimal) -> Result<()> {
        let mut tx = self.db.begin().await?;
        
        sqlx::query!(
            "UPDATE users SET locked_energy = locked_energy + $1 WHERE id = $2",
            amount,
            user_id
        )
        .execute(&mut *tx)
        .await?;

        sqlx::query!(
            r#"
            INSERT INTO escrow_records (
                user_id, order_id, amount, asset_type, escrow_type, status, description
            ) VALUES ($1, $2, $3, 'energy', 'sell_lock', 'locked', $4)
            "#,
            user_id,
            order_id,
            amount,
            format!("Sell order {} energy lock", order_id)
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn unlock_funds(&self, user_id: Uuid, order_id: Uuid, amount: Decimal, reason: &str) -> Result<()> {
        let mut tx = self.db.begin().await?;

        sqlx::query!(
            "UPDATE users SET balance = balance + $1, locked_amount = locked_amount - $1 WHERE id = $2",
            amount,
            user_id
        )
        .execute(&mut *tx)
        .await?;

        sqlx::query!(
            "UPDATE escrow_records SET status = 'released', description = $1, updated_at = NOW() WHERE user_id = $2 AND order_id = $3 AND asset_type = 'currency'",
            format!("Unlock: {}", reason),
            user_id,
            order_id
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn unlock_energy(&self, user_id: Uuid, order_id: Uuid, amount: Decimal, reason: &str) -> Result<()> {
        let mut tx = self.db.begin().await?;

        sqlx::query!(
            "UPDATE users SET locked_energy = locked_energy - $1 WHERE id = $2",
            amount,
            user_id
        )
        .execute(&mut *tx)
        .await?;

        sqlx::query!(
            "UPDATE escrow_records SET status = 'released', description = $1, updated_at = NOW() WHERE user_id = $2 AND order_id = $3 AND asset_type = 'energy'",
            format!("Unlock: {}", reason),
            user_id,
            order_id
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }
}
