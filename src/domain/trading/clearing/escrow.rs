use super::MarketClearingService;
use anyhow::Result;
use rust_decimal::Decimal;
use sqlx::Row;
use uuid::Uuid;

impl MarketClearingService {
    pub async fn lock_funds(&self, user_id: Uuid, order_id: Uuid, amount: Decimal) -> Result<()> {
        let mut tx = self.db.begin().await?;

        // Check balance
        let user = sqlx::query("SELECT balance FROM users WHERE id = $1 FOR UPDATE")
            .bind(user_id)
            .fetch_one(&mut *tx)
            .await?;

        let balance: Decimal = user
            .get::<Option<Decimal>, _>("balance")
            .unwrap_or(Decimal::ZERO);
        if balance < amount {
            return Err(anyhow::anyhow!(
                "Insufficient balance for escrow. Required: {}, Available: {}",
                amount,
                balance
            ));
        }

        // Update user balance and locked_amount
        sqlx::query("UPDATE users SET balance = balance - $1, locked_amount = locked_amount + $1 WHERE id = $2")
            .bind(amount)
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
        .bind(amount)
        .bind(format!("Buy order {} escrow", order_id))
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn lock_energy(&self, user_id: Uuid, order_id: Uuid, amount: Decimal) -> Result<()> {
        let mut tx = self.db.begin().await?;

        sqlx::query("UPDATE users SET locked_energy = locked_energy + $1 WHERE id = $2")
            .bind(amount)
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
        .bind(amount)
        .bind(format!("Sell order {} energy lock", order_id))
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn unlock_funds(
        &self,
        user_id: Uuid,
        order_id: Uuid,
        amount: Decimal,
        reason: &str,
    ) -> Result<()> {
        let mut tx = self.db.begin().await?;

        sqlx::query("UPDATE users SET balance = balance + $1, locked_amount = locked_amount - $1 WHERE id = $2")
            .bind(amount)
            .bind(user_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query("UPDATE escrow_records SET status = 'released', description = $1, updated_at = NOW() WHERE user_id = $2 AND order_id = $3 AND asset_type = 'currency'")
            .bind(format!("Unlock: {}", reason))
            .bind(user_id)
            .bind(order_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn unlock_energy(
        &self,
        user_id: Uuid,
        order_id: Uuid,
        amount: Decimal,
        reason: &str,
    ) -> Result<()> {
        let mut tx = self.db.begin().await?;

        sqlx::query("UPDATE users SET locked_energy = locked_energy - $1 WHERE id = $2")
            .bind(amount)
            .bind(user_id)
            .execute(&mut *tx)
            .await?;

        sqlx::query("UPDATE escrow_records SET status = 'released', description = $1, updated_at = NOW() WHERE user_id = $2 AND order_id = $3 AND asset_type = 'energy'")
            .bind(format!("Unlock: {}", reason))
            .bind(user_id)
            .bind(order_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }
}
