//! Shared market-epoch resolution.
//!
//! `market_epochs` is the single FK target for both `trading_orders.epoch_id`
//! and `settlements.epoch_id`/`order_matches.epoch_id`. Every write path that
//! stamps an epoch (order placement, trade settlement, generation-mint
//! settlement) must reference a row that exists here, so they all funnel through
//! this routine instead of inventing nil/hardcoded epoch UUIDs.

use rust_decimal::Decimal;
use sqlx::{PgPool, Row};
use trading_core::models::MarketEpoch;
use trading_core::traits::TraitResult;
use uuid::Uuid;

/// Return the id of the open market epoch whose 15-minute window still covers
/// now, creating the next one (`epoch_number = max + 1`) if none is open.
///
/// Serialized with a transaction-scoped advisory lock: `market_epochs
/// .epoch_number` is UNIQUE, so two callers that both find no active epoch would
/// otherwise race on INSERT. The lock releases on commit/rollback. The 15-minute
/// window matches the oracle's aggregation window.
pub async fn get_or_create_active_epoch(pool: &PgPool) -> TraitResult<Uuid> {
    const EPOCH_LOCK_KEY: i64 = 0x6772_6964; // "grid"

    let mut tx = pool.begin().await?;

    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(EPOCH_LOCK_KEY)
        .execute(&mut *tx)
        .await?;

    let existing: Option<(Uuid,)> = sqlx::query_as(
        "SELECT id FROM market_epochs \
         WHERE status = 'active' AND end_time > NOW() \
         ORDER BY end_time DESC LIMIT 1",
    )
    .fetch_optional(&mut *tx)
    .await?;

    if let Some((id,)) = existing {
        tx.commit().await?;
        return Ok(id);
    }

    let (id,): (Uuid,) = sqlx::query_as(
        "INSERT INTO market_epochs (epoch_number, start_time, end_time, status) \
         VALUES ( \
             (SELECT COALESCE(MAX(epoch_number), 0) + 1 FROM market_epochs), \
             NOW(), NOW() + INTERVAL '15 minutes', 'active' \
         ) RETURNING id",
    )
    .fetch_one(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(id)
}

/// Ids of epochs whose window has elapsed (`end_time <= NOW()`) but are still
/// `active` — the uniform-price clearing worker's work-list. Oldest first so a
/// backlog clears in order.
pub async fn get_epochs_due_for_clearing(pool: &PgPool) -> TraitResult<Vec<Uuid>> {
    let rows: Vec<(Uuid,)> = sqlx::query_as(
        "SELECT id FROM market_epochs \
         WHERE status = 'active' AND end_time <= NOW() \
         ORDER BY end_time ASC",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(id,)| id).collect())
}

/// Close an epoch after clearing: `active` → `cleared`, stamping the summary
/// columns. The `WHERE status = 'active'` guard makes it idempotent — a second
/// call (or a racing worker) updates zero rows.
pub async fn mark_epoch_cleared(
    pool: &PgPool,
    epoch_id: Uuid,
    clearing_price: Option<Decimal>,
    total_volume: Decimal,
    matched_orders: i64,
) -> TraitResult<()> {
    sqlx::query(
        "UPDATE market_epochs \
         SET status = 'cleared', clearing_price = $2, total_volume = $3, matched_orders = $4 \
         WHERE id = $1 AND status = 'active'",
    )
    .bind(epoch_id)
    .bind(clearing_price)
    .bind(total_volume)
    .bind(matched_orders)
    .execute(pool)
    .await?;
    Ok(())
}

/// Most-recently-cleared (or settled) epochs, newest first, capped at `limit` —
/// the clearing-results read endpoint. `settled` is included because a settled
/// epoch still carries the clearing summary.
pub async fn list_recent_cleared_epochs(pool: &PgPool, limit: i64) -> TraitResult<Vec<MarketEpoch>> {
    let rows = sqlx::query(
        "SELECT id, epoch_number, start_time, end_time, status, clearing_price, \
                total_volume, total_orders, matched_orders \
         FROM market_epochs \
         WHERE status IN ('cleared', 'settled') \
         ORDER BY end_time DESC LIMIT $1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let mut epochs = Vec::with_capacity(rows.len());
    for r in &rows {
        epochs.push(MarketEpoch {
            id: r.try_get("id")?,
            epoch_number: r.try_get("epoch_number")?,
            start_time: r.try_get("start_time")?,
            end_time: r.try_get("end_time")?,
            status: r.try_get("status")?,
            clearing_price: r.try_get("clearing_price")?,
            total_volume: r.try_get("total_volume")?,
            total_orders: r.try_get("total_orders")?,
            matched_orders: r.try_get("matched_orders")?,
        });
    }
    Ok(epochs)
}
