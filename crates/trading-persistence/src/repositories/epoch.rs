//! Shared market-epoch resolution.
//!
//! `market_epochs` is the single FK target for both `trading_orders.epoch_id`
//! and `settlements.epoch_id`/`order_matches.epoch_id`. Every write path that
//! stamps an epoch (order placement, trade settlement, generation-mint
//! settlement) must reference a row that exists here, so they all funnel through
//! this routine instead of inventing nil/hardcoded epoch UUIDs.

use sqlx::PgPool;
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
    const EPOCH_LOCK_KEY: i64 = 0x677269_64; // "grid"

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
