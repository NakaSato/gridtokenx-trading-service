//! Settlement repository implementation.

use async_trait::async_trait;
use rust_decimal::Decimal;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use chrono::{DateTime, Utc};
use trading_core::events::Event;
use trading_core::models::{
    MarketPrice, OrderMatch, Settlement, SettlementStats, SettlementStatus,
};
use trading_core::traits::{SettlementRepository, TraitResult};

/// Bind every settlement column onto an INSERT query — shared by the plain and
/// transactional insert paths so the column list cannot drift between them.
fn bind_settlement<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    s: &'q Settlement,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    query
        .bind(s.id)
        .bind(s.trade_id)
        .bind(s.epoch_id)
        .bind(s.buyer_id)
        .bind(s.seller_id)
        .bind(s.buy_order_id)
        .bind(s.sell_order_id)
        .bind(s.energy_amount)
        .bind(s.price)
        .bind(s.total_amount)
        .bind(s.fee_amount)
        .bind(s.wheeling_charge)
        .bind(s.loss_factor)
        .bind(s.loss_cost)
        .bind(s.effective_energy)
        .bind(s.buyer_zone_id)
        .bind(s.seller_zone_id)
        .bind(s.net_amount)
        .bind(s.status.to_string())
        .bind(&s.buyer_session_token)
        .bind(&s.seller_session_token)
        .bind(&s.blockchain_tx)
        .bind(s.created_at)
        .bind(s.confirmed_at)
        .bind(&s.erc_certificate_id)
        .bind(&s.erc_transfer_tx)
}

const INSERT_SETTLEMENT_SQL: &str = r#"
    INSERT INTO settlements (
        id, trade_id, epoch_id, buyer_id, seller_id, buy_order_id, sell_order_id,
        energy_amount, price_per_kwh, total_amount, fee_amount,
        wheeling_charge, loss_factor, loss_cost, effective_energy,
        buyer_zone_id, seller_zone_id, net_amount, status,
        buyer_session_token, seller_session_token, transaction_hash,
        created_at, processed_at, erc_certificate_id, erc_transfer_tx
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26)
"#;

const INSERT_MATCH_SQL: &str = r#"
    INSERT INTO order_matches (
        id, epoch_id, buy_order_id, sell_order_id,
        matched_amount, match_price, match_time, status,
        settlement_id, zone_id
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
"#;

/// Bind every order-match column — shared by the plain and transactional
/// insert paths.
fn bind_match<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    m: &'q OrderMatch,
    settlement_id: Option<Uuid>,
    zone_id: Option<i32>,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    query
        .bind(m.id)
        .bind(m.epoch_id)
        .bind(m.buy_order_id)
        .bind(m.sell_order_id)
        .bind(m.matched_amount)
        .bind(m.match_price)
        .bind(m.match_time)
        .bind(&m.status)
        .bind(settlement_id)
        .bind(zone_id)
}

const UPDATE_SETTLEMENT_STATUS_SQL: &str =
    "UPDATE settlements SET status = $1, transaction_hash = $2, error_message = $3, updated_at = NOW() WHERE id = $4";

#[derive(Debug, Clone, FromRow)]
struct SettlementStatsRow {
    pending: i64,
    processing: i64,
    confirmed: i64,
    failed: i64,
    total_settled_value: Decimal,
}

/// Aggregates over completed settlements in the trailing window. Every field is
/// nullable at the SQL level (no rows → all NULL), coalesced to zero in code.
#[derive(Debug, Clone, FromRow)]
struct MarketPriceRow {
    vwap: Option<Decimal>,
    last_price: Option<Decimal>,
    high: Option<Decimal>,
    low: Option<Decimal>,
    volume_kwh: Option<Decimal>,
    trade_count: i64,
}

#[derive(Debug, Clone, FromRow)]
pub struct SettlementDb {
    pub id: Uuid,
    pub trade_id: Option<Uuid>,
    pub epoch_id: Uuid,
    pub buyer_id: Uuid,
    pub seller_id: Uuid,
    pub buy_order_id: Uuid,
    pub sell_order_id: Uuid,
    pub energy_amount: Decimal,
    pub price_per_kwh: Decimal,
    pub total_amount: Decimal,
    pub fee_amount: Decimal,
    pub wheeling_charge: Option<Decimal>,
    pub loss_factor: Option<Decimal>,
    pub loss_cost: Option<Decimal>,
    pub effective_energy: Option<Decimal>,
    pub buyer_zone_id: Option<i32>,
    pub seller_zone_id: Option<i32>,
    pub net_amount: Decimal,
    pub status: String,
    pub transaction_hash: Option<String>,
    pub created_at: DateTime<Utc>,
    pub processed_at: Option<DateTime<Utc>>,
    pub buyer_session_token: Option<String>,
    pub seller_session_token: Option<String>,
    pub erc_certificate_id: Option<String>,
    pub erc_transfer_tx: Option<String>,
    pub retry_count: i32,
    pub error_message: Option<String>,
}

impl From<SettlementDb> for Settlement {
    fn from(db: SettlementDb) -> Self {
        let status = match db.status.as_str() {
            "processing" => SettlementStatus::Processing,
            "completed" | "confirmed" => SettlementStatus::Completed,
            "failed" => SettlementStatus::Failed,
            "permanently_failed" => SettlementStatus::PermanentlyFailed,
            _ => SettlementStatus::Pending,
        };

        Self {
            id: db.id,
            trade_id: db.trade_id,
            epoch_id: db.epoch_id,
            buyer_id: db.buyer_id,
            seller_id: db.seller_id,
            buy_order_id: db.buy_order_id,
            sell_order_id: db.sell_order_id,
            energy_amount: db.energy_amount,
            price: db.price_per_kwh,
            total_amount: db.total_amount,
            fee_amount: db.fee_amount,
            wheeling_charge: db.wheeling_charge,
            loss_factor: db.loss_factor,
            loss_cost: db.loss_cost,
            effective_energy: db.effective_energy,
            buyer_zone_id: db.buyer_zone_id,
            seller_zone_id: db.seller_zone_id,
            net_amount: db.net_amount,
            status,
            blockchain_tx: db.transaction_hash,
            created_at: db.created_at,
            confirmed_at: db.processed_at,
            buyer_session_token: db.buyer_session_token,
            seller_session_token: db.seller_session_token,
            erc_certificate_id: db.erc_certificate_id,
            erc_transfer_tx: db.erc_transfer_tx,
            retry_count: db.retry_count,
            error_message: db.error_message,
        }
    }
}

pub struct PostgresSettlementRepository {
    pool: PgPool,
}

impl PostgresSettlementRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl SettlementRepository for PostgresSettlementRepository {
    async fn insert_settlement(&self, s: &Settlement) -> TraitResult<()> {
        bind_settlement(sqlx::query(INSERT_SETTLEMENT_SQL), s)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn insert_settlement_with_event(
        &self,
        s: &Settlement,
        event: &Event,
    ) -> TraitResult<()> {
        // Settlement row + outbox row committed in one transaction: the event
        // can never be lost relative to the state change.
        let mut tx = self.pool.begin().await?;

        bind_settlement(sqlx::query(INSERT_SETTLEMENT_SQL), s)
            .execute(&mut *tx)
            .await?;

        crate::repositories::outbox::insert_event_in_tx(&mut tx, event).await?;

        tx.commit().await?;

        Ok(())
    }

    async fn get_or_create_active_epoch(&self) -> TraitResult<Uuid> {
        crate::repositories::epoch::get_or_create_active_epoch(&self.pool).await
    }

    async fn insert_match(
        &self,
        m: &OrderMatch,
        settlement_id: Option<Uuid>,
        zone_id: Option<i32>,
    ) -> TraitResult<()> {
        bind_match(sqlx::query(INSERT_MATCH_SQL), m, settlement_id, zone_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn insert_match_with_event(
        &self,
        m: &OrderMatch,
        settlement_id: Option<Uuid>,
        zone_id: Option<i32>,
        event: &Event,
    ) -> TraitResult<()> {
        // Match-ledger row + outbox row committed in one transaction: the
        // event can never be lost relative to the state change.
        let mut tx = self.pool.begin().await?;

        bind_match(sqlx::query(INSERT_MATCH_SQL), m, settlement_id, zone_id)
            .execute(&mut *tx)
            .await?;

        crate::repositories::outbox::insert_event_in_tx(&mut tx, event).await?;

        tx.commit().await?;

        Ok(())
    }

    async fn get_settlement(&self, id: Uuid) -> TraitResult<Option<Settlement>> {
        let s = sqlx::query_as::<_, SettlementDb>("SELECT * FROM settlements WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(s.map(Into::into))
    }

    async fn get_pending_settlements(&self, limit: i64) -> TraitResult<Vec<Settlement>> {
        let settlements = sqlx::query_as::<_, SettlementDb>(
            "SELECT * FROM settlements WHERE status = 'pending' LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(settlements.into_iter().map(Into::into).collect())
    }

    async fn claim_settlements_for_processing(
        &self,
        ids: &[Uuid],
    ) -> TraitResult<Vec<Settlement>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        // Single-statement compare-and-set: only rows still `pending` flip to
        // `processing` and are returned. A concurrent claimer sees them already
        // `processing` and gets nothing back, so the same settlement can never
        // be minted twice.
        let claimed = sqlx::query_as::<_, SettlementDb>(
            "UPDATE settlements SET status = 'processing', updated_at = NOW() \
             WHERE id = ANY($1) AND status = 'pending' RETURNING *",
        )
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(claimed.into_iter().map(Into::into).collect())
    }

    async fn reset_settlements_for_retry(
        &self,
        ids: &[Uuid],
        max_retries: i32,
        error: Option<&str>,
    ) -> TraitResult<u64> {
        if ids.is_empty() {
            return Ok(0);
        }

        // Bump retry_count; rows that have now exhausted their budget become
        // terminal `permanently_failed`, the rest go back to `pending` so the
        // worker/claim path (which select only `pending`) retries them. Guarded
        // on `status = 'processing'` so a row already finalized elsewhere is
        // untouched.
        let result = sqlx::query(
            "UPDATE settlements \
             SET retry_count = retry_count + 1, \
                 error_message = $2, \
                 status = CASE WHEN retry_count + 1 >= $3 THEN 'permanently_failed' ELSE 'pending' END, \
                 updated_at = NOW() \
             WHERE id = ANY($1) AND status = 'processing'",
        )
        .bind(ids)
        .bind(error)
        .bind(max_retries)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    async fn reclaim_stale_processing(
        &self,
        stale_after_secs: i64,
        max_retries: i32,
    ) -> TraitResult<u64> {
        // Same retry accounting as reset_settlements_for_retry, but selects rows
        // by staleness instead of id — recovers settlements orphaned in
        // `processing` by a crash or partial failure between claim and finalize.
        let result = sqlx::query(
            "UPDATE settlements \
             SET retry_count = retry_count + 1, \
                 status = CASE WHEN retry_count + 1 >= $2 THEN 'permanently_failed' ELSE 'pending' END, \
                 updated_at = NOW() \
             WHERE status = 'processing' \
               AND updated_at < NOW() - make_interval(secs => $1::double precision)",
        )
        .bind(stale_after_secs as f64)
        .bind(max_retries)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    async fn list_settlements_for_user(
        &self,
        user_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> TraitResult<(Vec<Settlement>, i64)> {
        let rows = sqlx::query_as::<_, SettlementDb>(
            r#"
            SELECT * FROM settlements
            WHERE buyer_id = $1 OR seller_id = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(user_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM settlements WHERE buyer_id = $1 OR seller_id = $1",
        )
        .bind(user_id)
        .fetch_one(&self.pool)
        .await?;

        Ok((rows.into_iter().map(Into::into).collect(), total))
    }

    async fn get_settlement_stats(&self) -> TraitResult<SettlementStats> {
        let row = sqlx::query_as::<_, SettlementStatsRow>(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE status = 'pending')    AS pending,
                COUNT(*) FILTER (WHERE status = 'processing') AS processing,
                COUNT(*) FILTER (WHERE status = 'completed')  AS confirmed,
                COUNT(*) FILTER (WHERE status = 'failed')     AS failed,
                COALESCE(SUM(total_amount) FILTER (WHERE status = 'completed'), 0) AS total_settled_value
            FROM settlements
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(SettlementStats {
            pending_count: row.pending,
            processing_count: row.processing,
            confirmed_count: row.confirmed,
            failed_count: row.failed,
            total_settled_value: row.total_settled_value,
        })
    }

    async fn get_market_price(&self, window_hours: i64) -> TraitResult<MarketPrice> {
        // VWAP weights price by settled energy; high/low/volume/count are plain
        // aggregates. `last_price` uses a correlated subselect ordering by the
        // settlement's effective time (confirmed_at, falling back to created_at)
        // so it is the genuinely most-recent completed fill, not an aggregate.
        //
        // `window_hours <= 0` means "all-time": the `$2::bool` all-time flag
        // short-circuits the time predicate, so widening to the full history
        // needs no magic large window value.
        let all_time = window_hours <= 0;
        let hours = i32::try_from(window_hours.max(0)).unwrap_or(24);
        let row = sqlx::query_as::<_, MarketPriceRow>(
            r#"
            SELECT
                SUM(price * energy_amount) / NULLIF(SUM(energy_amount), 0) AS vwap,
                MAX(price)              AS high,
                MIN(price)              AS low,
                SUM(energy_amount)      AS volume_kwh,
                COUNT(*)                AS trade_count,
                (
                    SELECT s2.price
                    FROM settlements s2
                    WHERE s2.status = 'completed'
                      AND ($2::bool OR s2.created_at >= NOW() - make_interval(hours => $1::int))
                    ORDER BY COALESCE(s2.confirmed_at, s2.created_at) DESC
                    LIMIT 1
                ) AS last_price
            FROM settlements
            WHERE status = 'completed'
              AND ($2::bool OR created_at >= NOW() - make_interval(hours => $1::int))
            "#,
        )
        .bind(hours)
        .bind(all_time)
        .fetch_one(&self.pool)
        .await?;

        let z = Decimal::ZERO;
        Ok(MarketPrice {
            vwap: row.vwap.unwrap_or(z),
            last_price: row.last_price.unwrap_or(z),
            high: row.high.unwrap_or(z),
            low: row.low.unwrap_or(z),
            volume_kwh: row.volume_kwh.unwrap_or(z),
            trade_count: row.trade_count,
            window_hours,
            as_of: gridtokenx_telemetry::time::now(),
        })
    }

    async fn count_active_traders(&self, window_hours: i64) -> TraitResult<i64> {
        let all_time = window_hours <= 0;
        let hours = i32::try_from(window_hours.max(0)).unwrap_or(24);
        // Distinct users across both sides: UNION the buyer and seller id sets,
        // then count the deduped rows.
        let row: (i64,) = sqlx::query_as(
            r#"
            SELECT COUNT(*) FROM (
                SELECT buyer_id AS uid FROM settlements
                WHERE status = 'completed'
                  AND ($2::bool OR created_at >= NOW() - make_interval(hours => $1::int))
                UNION
                SELECT seller_id AS uid FROM settlements
                WHERE status = 'completed'
                  AND ($2::bool OR created_at >= NOW() - make_interval(hours => $1::int))
            ) AS traders
            "#,
        )
        .bind(hours)
        .bind(all_time)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.0)
    }

    async fn update_settlement_status(
        &self,
        id: Uuid,
        status: &str,
        tx_hash: Option<&str>,
        error: Option<&str>,
    ) -> TraitResult<()> {
        sqlx::query(UPDATE_SETTLEMENT_STATUS_SQL)
            .bind(status)
            .bind(tx_hash)
            .bind(error)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn update_settlement_status_with_event(
        &self,
        id: Uuid,
        status: &str,
        tx_hash: Option<&str>,
        error: Option<&str>,
        event: &Event,
    ) -> TraitResult<()> {
        // Status update + outbox row committed in one transaction: the event
        // can never be lost relative to the state change.
        let mut tx = self.pool.begin().await?;

        sqlx::query(UPDATE_SETTLEMENT_STATUS_SQL)
            .bind(status)
            .bind(tx_hash)
            .bind(error)
            .bind(id)
            .execute(&mut *tx)
            .await?;

        crate::repositories::outbox::insert_event_in_tx(&mut tx, event).await?;

        tx.commit().await?;
        Ok(())
    }
}
