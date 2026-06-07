//! Order repository implementation.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use trading_core::events::Event;
use trading_core::models::{OrderBookEntry, TradingOrder};
use trading_core::traits::{OrderRepository, TraitResult};
use trading_core::types::{
    OrderSide, OrderStatus, OrderType, TimeInForce, TriggerStatus, TriggerType,
};

/// Database-specific order model with SQLx metadata.
#[derive(Debug, Clone, FromRow)]
pub struct TradingOrderDb {
    pub id: Uuid,
    pub user_id: Uuid,
    pub order_type: OrderType,
    pub side: OrderSide,
    pub energy_amount: Decimal,
    pub price_per_kwh: Decimal,
    pub filled_amount: Option<Decimal>,
    pub status: OrderStatus,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: Option<DateTime<Utc>>,
    pub filled_at: Option<DateTime<Utc>>,
    pub epoch_id: Option<Uuid>,
    pub zone_id: Option<i32>,
    pub meter_id: Option<Uuid>,
    pub refund_tx_signature: Option<String>,
    pub order_pda: Option<String>,
    pub order_index: Option<i64>,
    pub session_token: Option<String>,
    // Conditional order fields
    pub trigger_price: Option<Decimal>,
    pub trigger_type: Option<TriggerType>,
    pub trigger_status: Option<TriggerStatus>,
    pub trailing_offset: Option<Decimal>,
    pub triggered_at: Option<DateTime<Utc>>,
    pub last_peak_price: Option<Decimal>,
    pub limit_price: Option<Decimal>,
    // Blockchain sync fields
    pub blockchain_status: Option<String>,
    pub blockchain_tx_hash: Option<String>,
    pub blockchain_error: Option<String>,
    pub retry_count: i32,
    pub time_in_force: TimeInForce,
}

impl From<TradingOrderDb> for TradingOrder {
    fn from(db: TradingOrderDb) -> Self {
        Self {
            id: db.id,
            user_id: db.user_id,
            order_type: db.order_type,
            side: db.side,
            energy_amount: db.energy_amount,
            price_per_kwh: db.price_per_kwh,
            filled_amount: db.filled_amount.unwrap_or(Decimal::ZERO),
            status: db.status,
            expires_at: db.expires_at,
            created_at: db.created_at,
            filled_at: db.filled_at,
            epoch_id: db.epoch_id,
            zone_id: db.zone_id,
            meter_id: db.meter_id,
            refund_tx_signature: db.refund_tx_signature,
            order_pda: db.order_pda,
            order_index: db.order_index,
            session_token: db.session_token,
            blockchain_status: db.blockchain_status,
            blockchain_tx_hash: db.blockchain_tx_hash,
            blockchain_error: db.blockchain_error,
            retry_count: db.retry_count,
            time_in_force: db.time_in_force,
        }
    }
}

pub struct PostgresOrderRepository {
    pool: PgPool,
}

impl PostgresOrderRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl OrderRepository for PostgresOrderRepository {
    async fn insert_order(&self, order: &TradingOrder) -> TraitResult<()> {
        sqlx::query(
            r#"
            INSERT INTO trading_orders (
                id, user_id, order_type, side, energy_amount, price_per_kwh,
                status, time_in_force, zone_id, meter_id, session_token,
                order_pda, order_index, blockchain_tx_hash, blockchain_status,
                epoch_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            "#,
        )
        .bind(order.id)
        .bind(order.user_id)
        .bind(order.order_type)
        .bind(order.side)
        .bind(order.energy_amount)
        .bind(order.price_per_kwh)
        .bind(order.status)
        .bind(order.time_in_force)
        .bind(order.zone_id)
        .bind(order.meter_id)
        .bind(&order.session_token)
        .bind(&order.order_pda)
        .bind(order.order_index)
        .bind(&order.blockchain_tx_hash)
        .bind(&order.blockchain_status)
        .bind(order.epoch_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn insert_order_with_event(
        &self,
        order: &TradingOrder,
        event: &Event,
    ) -> TraitResult<()> {
        // Order row + outbox row committed in one transaction: the event can
        // never be lost relative to the state change. Mirrors the inserts in
        // `insert_order` and `PostgresOutboxRepository::insert_event`.
        let payload = serde_json::to_value(event).map_err(|e| {
            trading_core::error::ApiError::Internal(format!("Failed to serialize event: {}", e))
        })?;

        let mut tx = self.pool.begin().await?;

        sqlx::query(
            r#"
            INSERT INTO trading_orders (
                id, user_id, order_type, side, energy_amount, price_per_kwh,
                status, time_in_force, zone_id, meter_id, session_token,
                order_pda, order_index, blockchain_tx_hash, blockchain_status,
                epoch_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            "#,
        )
        .bind(order.id)
        .bind(order.user_id)
        .bind(order.order_type)
        .bind(order.side)
        .bind(order.energy_amount)
        .bind(order.price_per_kwh)
        .bind(order.status)
        .bind(order.time_in_force)
        .bind(order.zone_id)
        .bind(order.meter_id)
        .bind(&order.session_token)
        .bind(&order.order_pda)
        .bind(order.order_index)
        .bind(&order.blockchain_tx_hash)
        .bind(&order.blockchain_status)
        .bind(order.epoch_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query("INSERT INTO outbox_events (event_type, payload) VALUES ($1, $2)")
            .bind(event.outbox_event_type())
            .bind(payload)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;

        Ok(())
    }

    async fn get_order(&self, id: Uuid) -> TraitResult<Option<TradingOrder>> {
        let order =
            sqlx::query_as::<_, TradingOrderDb>("SELECT * FROM trading_orders WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;

        Ok(order.map(Into::into))
    }

    async fn get_orders_by_user(
        &self,
        user_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> TraitResult<Vec<TradingOrder>> {
        let orders = sqlx::query_as::<_, TradingOrderDb>(
            "SELECT * FROM trading_orders WHERE user_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3"
        )
        .bind(user_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(orders.into_iter().map(Into::into).collect())
    }

    async fn get_active_orders_by_zone(&self, zone_id: i32) -> TraitResult<Vec<OrderBookEntry>> {
        let orders = sqlx::query_as::<_, TradingOrderDb>(
            r#"
            SELECT * FROM trading_orders 
            WHERE zone_id = $1 
            AND status IN ('pending', 'active', 'partially_filled')
            "#,
        )
        .bind(zone_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(orders
            .into_iter()
            .map(|o| OrderBookEntry {
                order_id: o.id,
                user_id: o.user_id,
                side: o.side,
                energy_amount: o.energy_amount - o.filled_amount.unwrap_or(Decimal::ZERO),
                original_amount: o.energy_amount,
                price_per_kwh: o.price_per_kwh,
                created_at: o.created_at.unwrap_or_else(Utc::now),
                zone_id: o.zone_id,
                session_token: o.session_token,
                signature: None, // Logic for signatures will be handled in logic crate
                payload_bytes: None,
                time_in_force: o.time_in_force,
            })
            .collect())
    }

    async fn get_all_active_orders(&self) -> TraitResult<Vec<OrderBookEntry>> {
        let orders = sqlx::query_as::<_, TradingOrderDb>(
            r#"
            SELECT * FROM trading_orders
            WHERE status IN ('pending', 'active', 'partially_filled')
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(orders
            .into_iter()
            .map(|o| OrderBookEntry {
                order_id: o.id,
                user_id: o.user_id,
                side: o.side,
                energy_amount: o.energy_amount - o.filled_amount.unwrap_or(Decimal::ZERO),
                original_amount: o.energy_amount,
                price_per_kwh: o.price_per_kwh,
                created_at: o.created_at.unwrap_or_else(Utc::now),
                zone_id: o.zone_id,
                session_token: o.session_token,
                signature: None,
                payload_bytes: None,
                time_in_force: o.time_in_force,
            })
            .collect())
    }

    async fn update_order_status(&self, id: Uuid, status: OrderStatus) -> TraitResult<()> {
        sqlx::query("UPDATE trading_orders SET status = $1, updated_at = NOW() WHERE id = $2")
            .bind(status)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn update_filled_amount(
        &self,
        id: Uuid,
        filled_amount: Decimal,
        status: OrderStatus,
    ) -> TraitResult<()> {
        sqlx::query(
            "UPDATE trading_orders SET filled_amount = $1, status = $2, filled_at = CASE WHEN $2 = 'filled' THEN NOW() ELSE filled_at END WHERE id = $3"
        )
        .bind(filled_amount)
        .bind(status)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn cancel_order(&self, id: Uuid, user_id: Uuid) -> TraitResult<bool> {
        let result = sqlx::query(
            "UPDATE trading_orders SET status = 'cancelled' WHERE id = $1 AND user_id = $2 AND status IN ('pending', 'active', 'partially_filled')"
        )
        .bind(id)
        .bind(user_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn bootstrap_active_orders(&self) -> TraitResult<Vec<TradingOrder>> {
        let orders = sqlx::query_as::<_, TradingOrderDb>(
            "SELECT * FROM trading_orders WHERE status IN ('pending', 'active', 'partially_filled')"
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(orders.into_iter().map(Into::into).collect())
    }

    async fn get_active_buy_orders(&self) -> TraitResult<Vec<TradingOrder>> {
        let orders = sqlx::query_as::<_, TradingOrderDb>(
            "SELECT * FROM trading_orders WHERE side = 'buy' AND status IN ('pending', 'active', 'partially_filled')"
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(orders.into_iter().map(Into::into).collect())
    }

    async fn get_active_sell_orders(&self) -> TraitResult<Vec<TradingOrder>> {
        let orders = sqlx::query_as::<_, TradingOrderDb>(
            "SELECT * FROM trading_orders WHERE side = 'sell' AND status IN ('pending', 'active', 'partially_filled')"
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(orders.into_iter().map(Into::into).collect())
    }

    async fn get_or_create_active_epoch(&self) -> TraitResult<Uuid> {
        crate::repositories::epoch::get_or_create_active_epoch(&self.pool).await
    }
}
