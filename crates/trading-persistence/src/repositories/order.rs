//! Order repository implementation.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::{FromRow, PgPool, Row};
use uuid::Uuid;

use trading_core::events::Event;
use trading_core::models::{OrderBookEntry, TradingOrder};
use trading_core::traits::{OrderRepository, TraitResult};
use trading_core::types::{
    MarketSegment, OrderSide, OrderStatus, OrderType, TimeInForce, TriggerStatus, TriggerType,
};

/// Database-specific order model with `SQLx` metadata.
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
    pub market_segment: MarketSegment,
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
            market_segment: db.market_segment,
        }
    }
}

pub struct PostgresOrderRepository {
    pool: PgPool,
}

impl PostgresOrderRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl OrderRepository for PostgresOrderRepository {
    async fn insert_order(&self, order: &TradingOrder) -> TraitResult<()> {
        sqlx::query(
            r"
            INSERT INTO trading_orders (
                id, user_id, order_type, side, energy_amount, price_per_kwh,
                status, time_in_force, zone_id, meter_id, session_token,
                order_pda, order_index, blockchain_tx_hash, blockchain_status,
                epoch_id, market_segment, expires_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
            ",
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
        .bind(order.market_segment)
        // `expires_at` was missing from this column list, so it was silently
        // dropped on every insert: orders landed with NULL and the ReaperWorker
        // (which matches on `expires_at < now()`) had nothing to reap, making
        // order expiry inert for everything created through the API.
        .bind(order.expires_at)
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
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            r"
            INSERT INTO trading_orders (
                id, user_id, order_type, side, energy_amount, price_per_kwh,
                status, time_in_force, zone_id, meter_id, session_token,
                order_pda, order_index, blockchain_tx_hash, blockchain_status,
                epoch_id, market_segment, expires_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
            ",
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
        .bind(order.market_segment)
        // `expires_at` was missing from this column list, so it was silently
        // dropped on every insert: orders landed with NULL and the ReaperWorker
        // (which matches on `expires_at < now()`) had nothing to reap, making
        // order expiry inert for everything created through the API.
        .bind(order.expires_at)
        .execute(&mut *tx)
        .await?;

        crate::repositories::outbox::insert_event_in_tx(&mut tx, event).await?;

        tx.commit().await?;

        Ok(())
    }

    async fn set_wallet_signature(&self, order_id: Uuid, signature: &str) -> TraitResult<()> {
        // Targeted UPDATE rather than an extra INSERT column: `TradingOrder` does
        // not map `signature`, and widening the model would touch every order
        // literal in the suite for a field only the settlement builder reads.
        sqlx::query("UPDATE trading_orders SET signature = $1 WHERE id = $2")
            .bind(signature)
            .bind(order_id)
            .execute(&self.pool)
            .await?;
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
            r"
            SELECT * FROM trading_orders 
            WHERE zone_id = $1 
            AND status IN ('pending', 'active', 'partially_filled')
            ",
        )
        .bind(zone_id)
        .fetch_all(&self.pool)
        .await?;

        // Drop rows already past `expires_at`. Expiry is reaped asynchronously
        // (ReaperWorker, 10s), so a lapsed order keeps its active status until the
        // next tick — and `OrderBookEntry` carries no expiry, so no caller can
        // filter it downstream. Every consumer of this method builds a *live book*
        // view (REST/gRPC order book, price-alert best bid/ask), and the matcher
        // would never fill such an order, so reporting it is phantom depth.
        // Filtered in Rust against the telemetry (NTP) clock rather than a SQL
        // `now()` so this compares against the same clock `expires_at` was
        // written with — see `get_active_buy_orders`.
        let now = gridtokenx_telemetry::time::now();
        let orders = orders
            .into_iter()
            .filter(|o| o.expires_at.is_none_or(|e| e > now));

        Ok(orders
            .map(|o| OrderBookEntry {
                order_id: o.id,
                user_id: o.user_id,
                side: o.side,
                energy_amount: o.energy_amount - o.filled_amount.unwrap_or(Decimal::ZERO),
                original_amount: o.energy_amount,
                price_per_kwh: o.price_per_kwh,
                created_at: o.created_at.unwrap_or_else(gridtokenx_telemetry::time::now),
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
            r"
            SELECT * FROM trading_orders
            WHERE status IN ('pending', 'active', 'partially_filled')
            ",
        )
        .fetch_all(&self.pool)
        .await?;

        // Drop rows already past `expires_at`. Expiry is reaped asynchronously
        // (ReaperWorker, 10s), so a lapsed order keeps its active status until the
        // next tick — and `OrderBookEntry` carries no expiry, so no caller can
        // filter it downstream. Every consumer of this method builds a *live book*
        // view (REST/gRPC order book, price-alert best bid/ask), and the matcher
        // would never fill such an order, so reporting it is phantom depth.
        // Filtered in Rust against the telemetry (NTP) clock rather than a SQL
        // `now()` so this compares against the same clock `expires_at` was
        // written with — see `get_active_buy_orders`.
        let now = gridtokenx_telemetry::time::now();
        let orders = orders
            .into_iter()
            .filter(|o| o.expires_at.is_none_or(|e| e > now));

        Ok(orders
            .map(|o| OrderBookEntry {
                order_id: o.id,
                user_id: o.user_id,
                side: o.side,
                energy_amount: o.energy_amount - o.filled_amount.unwrap_or(Decimal::ZERO),
                original_amount: o.energy_amount,
                price_per_kwh: o.price_per_kwh,
                created_at: o.created_at.unwrap_or_else(gridtokenx_telemetry::time::now),
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

    async fn update_order_pda(&self, id: Uuid, order_pda: &str, order_index: i64) -> TraitResult<()> {
        sqlx::query(
            "UPDATE trading_orders SET order_pda = $1, order_index = $2, updated_at = NOW() WHERE id = $3",
        )
        .bind(order_pda)
        .bind(order_index)
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

    async fn update_filled_amount_with_event(
        &self,
        id: Uuid,
        filled_amount: Decimal,
        status: OrderStatus,
        event: &Event,
    ) -> TraitResult<()> {
        // Fill update + outbox row committed in one transaction (see
        // `insert_order_with_event`).
        let mut tx = self.pool.begin().await?;

        // Guard on a live source status, exactly like `expire_stale_orders` and
        // `cancel_order`. Without it this write would unconditionally overwrite a
        // row the reaper (or a cancel) had already moved to a terminal state —
        // resurrecting an expired/cancelled order to `filled` after its escrow
        // was refunded, i.e. an escrow double-process. The order book is
        // in-memory, so the matcher can still hold an order the reaper expired
        // between the book snapshot and this write; this predicate is the
        // authoritative check.
        let result = sqlx::query(
            "UPDATE trading_orders SET filled_amount = $1, status = $2, filled_at = CASE WHEN $2 = 'filled' THEN NOW() ELSE filled_at END \
             WHERE id = $3 AND status IN ('pending', 'active', 'partially_filled')"
        )
        .bind(filled_amount)
        .bind(status)
        .bind(id)
        .execute(&mut *tx)
        .await?;

        // Row already terminal (expired/cancelled/filled elsewhere): the fill did
        // not apply, so its `OrderUpdate` event must not fire either. Emitting it
        // would publish a phantom fill for an order the trader was told was gone.
        if result.rows_affected() == 0 {
            tx.rollback().await?;
            return Ok(());
        }

        crate::repositories::outbox::insert_event_in_tx(&mut tx, event).await?;

        tx.commit().await?;
        Ok(())
    }

    async fn expire_stale_orders(&self, now: DateTime<Utc>) -> TraitResult<Vec<Uuid>> {
        // Reap + outbox in one transaction: the status flip to Expired and its
        // OrderUpdate event commit together, so the event can never be lost
        // relative to the state change (mirrors `update_filled_amount_with_event`).
        let mut tx = self.pool.begin().await?;

        let rows = sqlx::query(
            "UPDATE trading_orders \
             SET status = 'expired', updated_at = NOW() \
             WHERE status IN ('pending', 'active', 'partially_filled') \
               AND expires_at IS NOT NULL AND expires_at <= $1 \
             RETURNING id, user_id, filled_amount, zone_id",
        )
        .bind(now)
        .fetch_all(&mut *tx)
        .await?;

        let mut ids = Vec::with_capacity(rows.len());
        for row in &rows {
            let id: Uuid = row.try_get("id")?;
            let user_id: Uuid = row.try_get("user_id")?;
            let filled_amount: Decimal = row.try_get("filled_amount")?;
            let zone_id: Option<i32> = row.try_get("zone_id")?;
            let event = Event::OrderUpdate {
                id,
                user_id: Some(user_id),
                filled_amount,
                status: OrderStatus::Expired.to_string(),
                zone_id,
            };
            crate::repositories::outbox::insert_event_in_tx(&mut tx, &event).await?;
            ids.push(id);
        }

        tx.commit().await?;
        Ok(ids)
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
            // No `expires_at` predicate: expiry is gated by the telemetry (NTP)
            // clock — the ReaperWorker (which flips expired → Expired, so they
            // drop from this status filter) and the engine's `is_expired` skip.
            // A DB `NOW()` predicate here would compare against a *different*
            // clock than the one `expires_at` was written with.
            "SELECT * FROM trading_orders WHERE side = 'buy' AND status IN ('pending', 'active', 'partially_filled')"
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(orders.into_iter().map(Into::into).collect())
    }

    async fn get_active_sell_orders(&self) -> TraitResult<Vec<TradingOrder>> {
        let orders = sqlx::query_as::<_, TradingOrderDb>(
            // See get_active_buy_orders: expiry is telemetry-clock-gated, not a
            // DB NOW() predicate (which would mix clocks vs how expires_at is set).
            "SELECT * FROM trading_orders WHERE side = 'sell' AND status IN ('pending', 'active', 'partially_filled')"
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(orders.into_iter().map(Into::into).collect())
    }

    async fn get_or_create_active_epoch(&self) -> TraitResult<Uuid> {
        crate::repositories::epoch::get_or_create_active_epoch(&self.pool).await
    }

    async fn get_epochs_due_for_clearing(&self) -> TraitResult<Vec<Uuid>> {
        crate::repositories::epoch::get_epochs_due_for_clearing(&self.pool).await
    }

    async fn mark_epoch_cleared(
        &self,
        epoch_id: Uuid,
        clearing_price: Option<Decimal>,
        total_volume: Decimal,
        matched_orders: i64,
    ) -> TraitResult<()> {
        crate::repositories::epoch::mark_epoch_cleared(
            &self.pool,
            epoch_id,
            clearing_price,
            total_volume,
            matched_orders,
        )
        .await
    }

    async fn list_recent_cleared_epochs(
        &self,
        limit: i64,
    ) -> TraitResult<Vec<trading_core::models::MarketEpoch>> {
        crate::repositories::epoch::list_recent_cleared_epochs(&self.pool, limit).await
    }
}
