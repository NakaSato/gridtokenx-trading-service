//! Read-model repository implementations (DB-per-service Phase 1).
//!
//! These populate the Trading-owned local mirrors of two cross-domain tables:
//!   * `iam_wallet_read_model`  ← IAM `user_wallets`
//!   * `meter_read_model`       ← metering `meters`
//!
//! They are fed by two paths: a one-shot boot **backfill** (snapshot the source
//! table, which is still reachable on the same pool pre-cutover) and the live
//! NATS/Kafka event stream (see `trading-logic` `ReadModelFeedWorker`). At the
//! later cutover the two cross-domain reads
//! (`rpc/service.rs::get_user_primary_wallet`, `vpp.rs` meters JOIN) swap to
//! these tables. Runtime SQLx (no compile-time macros), per this workspace's
//! convention.

use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;

use trading_core::traits::{
    MeterReadModelRecord, MeterReadModelRepository, TraitResult, WalletReadModelRecord,
    WalletReadModelRepository,
};

// ── Wallet read-model ────────────────────────────────────────────────────────

pub struct PgWalletReadModelRepository {
    pool: PgPool,
}

impl PgWalletReadModelRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// Last-writer-wins upsert. `updated_at` is stamped `now()` on every write; the
/// `ON CONFLICT` guard only overwrites when the incoming write is at least as
/// new as the stored row, so an out-of-order redelivery can never regress state.
const WALLET_UPSERT: &str = r#"
    INSERT INTO iam_wallet_read_model
        (user_id, wallet_address, is_primary, blockchain_registered,
         user_account_pda, shard_id, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, now())
    ON CONFLICT (user_id, wallet_address) DO UPDATE SET
        is_primary            = EXCLUDED.is_primary,
        blockchain_registered = EXCLUDED.blockchain_registered,
        user_account_pda      = EXCLUDED.user_account_pda,
        shard_id              = EXCLUDED.shard_id,
        updated_at            = EXCLUDED.updated_at
    WHERE iam_wallet_read_model.updated_at <= EXCLUDED.updated_at
"#;

/// Demote every other primary wallet of a user (keeps the partial unique index
/// `(user_id) WHERE is_primary` satisfiable when a new primary is set).
const WALLET_DEMOTE_SIBLINGS: &str = r#"
    UPDATE iam_wallet_read_model
       SET is_primary = false, updated_at = now()
     WHERE user_id = $1 AND is_primary AND wallet_address <> $2
"#;

#[async_trait]
impl WalletReadModelRepository for PgWalletReadModelRepository {
    async fn upsert_wallet(&self, rec: &WalletReadModelRecord) -> TraitResult<()> {
        if rec.is_primary {
            // Promoting to primary: demote siblings first, then upsert, atomically.
            let mut tx = self.pool.begin().await?;
            sqlx::query(WALLET_DEMOTE_SIBLINGS)
                .bind(rec.user_id)
                .bind(&rec.wallet_address)
                .execute(&mut *tx)
                .await?;
            sqlx::query(WALLET_UPSERT)
                .bind(rec.user_id)
                .bind(&rec.wallet_address)
                .bind(rec.is_primary)
                .bind(rec.blockchain_registered)
                .bind(rec.user_account_pda.as_deref())
                .bind(rec.shard_id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
        } else {
            sqlx::query(WALLET_UPSERT)
                .bind(rec.user_id)
                .bind(&rec.wallet_address)
                .bind(rec.is_primary)
                .bind(rec.blockchain_registered)
                .bind(rec.user_account_pda.as_deref())
                .bind(rec.shard_id)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    async fn set_wallet_primary(
        &self,
        user_id: Uuid,
        wallet_address: &str,
        is_primary: bool,
    ) -> TraitResult<()> {
        let mut tx = self.pool.begin().await?;
        if is_primary {
            sqlx::query(WALLET_DEMOTE_SIBLINGS)
                .bind(user_id)
                .bind(wallet_address)
                .execute(&mut *tx)
                .await?;
        }
        // UPDATE only — never insert — so a stray primary-changed event cannot
        // clobber blockchain_registered / user_account_pda / shard_id. If the
        // row does not exist yet, this is a harmless no-op; a later link event
        // or the boot backfill will create it.
        sqlx::query(
            r#"UPDATE iam_wallet_read_model
                  SET is_primary = $3, updated_at = now()
                WHERE user_id = $1 AND wallet_address = $2"#,
        )
        .bind(user_id)
        .bind(wallet_address)
        .bind(is_primary)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn backfill_wallets(&self) -> TraitResult<u64> {
        // Source `is_primary` / `blockchain_registered` are nullable on
        // `user_wallets`; COALESCE to the read-model's NOT NULL columns.
        // Assumes the source holds at most one primary wallet per user (IAM
        // enforces this) — otherwise the partial unique index would reject it.
        let res = sqlx::query(
            r#"INSERT INTO iam_wallet_read_model
                   (user_id, wallet_address, is_primary, blockchain_registered,
                    user_account_pda, shard_id, updated_at)
               SELECT user_id, wallet_address,
                      COALESCE(is_primary, false),
                      COALESCE(blockchain_registered, false),
                      user_account_pda, shard_id, now()
                 FROM user_wallets
               ON CONFLICT (user_id, wallet_address) DO NOTHING"#,
        )
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected())
    }
}

// ── Meter read-model ─────────────────────────────────────────────────────────

pub struct PgMeterReadModelRepository {
    pool: PgPool,
}

impl PgMeterReadModelRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// Last-writer-wins upsert. `rated_power_kw` / `rated_capacity_kwh` are omitted
/// (NULL on insert, untouched on update) — the metering `meters` table carries
/// no such columns, so there is no event/backfill source for them.
const METER_UPSERT: &str = r#"
    INSERT INTO meter_read_model
        (serial_number, meter_id, user_id, zone_id, status, updated_at)
    VALUES ($1, $2, $3, $4, $5, now())
    ON CONFLICT (serial_number) DO UPDATE SET
        meter_id   = EXCLUDED.meter_id,
        user_id    = EXCLUDED.user_id,
        zone_id    = EXCLUDED.zone_id,
        status     = EXCLUDED.status,
        updated_at = EXCLUDED.updated_at
    WHERE meter_read_model.updated_at <= EXCLUDED.updated_at
"#;

#[async_trait]
impl MeterReadModelRepository for PgMeterReadModelRepository {
    async fn upsert_meter(&self, rec: &MeterReadModelRecord) -> TraitResult<()> {
        sqlx::query(METER_UPSERT)
            .bind(&rec.serial_number)
            .bind(rec.meter_id)
            .bind(rec.user_id)
            .bind(rec.zone_id)
            .bind(rec.status.as_deref())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn backfill_meters(&self) -> TraitResult<u64> {
        // `meters.id` is the meter_id; rated_power/capacity have no source column.
        let res = sqlx::query(
            r#"INSERT INTO meter_read_model
                   (serial_number, meter_id, user_id, zone_id, status, updated_at)
               SELECT serial_number, id, user_id, zone_id, status, now()
                 FROM meters
               ON CONFLICT (serial_number) DO NOTHING"#,
        )
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected())
    }
}
