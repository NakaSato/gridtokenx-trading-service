//! Read-model repository implementations (DB-per-service Phase 1).
//!
//! These populate the Trading-owned local mirrors of two cross-domain tables:
//!   * `iam_wallet_read_model`  ← IAM `user_wallets`
//!   * `meter_read_model`       ← metering `meters`
//!
//! They are fed by two paths: a one-shot boot **backfill** (snapshot of the
//! source table) and the live NATS/Kafka event stream (see `trading-logic`
//! `ReadModelFeedWorker`). The two cross-domain reads
//! (`rpc/service.rs::get_user_primary_wallet`, `vpp.rs` meters JOIN) read from
//! these tables. Runtime SQLx (no compile-time macros), per this workspace's
//! convention.
//!
//! Post DB-split the source tables (`user_wallets`, `meters`) live in the IAM
//! and metering databases, not the trading one, so the backfill needs a
//! dedicated read-only source pool (`with_source_pool`). Without one it falls
//! back to querying the local pool — correct only pre-cutover on the shared DB.

use async_trait::async_trait;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use trading_core::traits::{
    MeterReadModelRecord, MeterReadModelRepository, TraitResult, WalletReadModelRecord,
    WalletReadModelRepository,
};

/// Does `table` exist in the pool's current database/search-path?
///
/// Used by the boot backfill's no-source-pool branch to distinguish a genuine
/// pre-cutover shared DB (source table present locally → backfill it) from a
/// post-split boot where the source pool wasn't available (source table absent
/// locally → skip silently; the event feed seeds the read-model). `to_regclass`
/// returns NULL for an unknown relation instead of raising, so this never logs
/// a "relation does not exist" ERROR.
async fn local_source_table_exists(pool: &PgPool, table: &str) -> TraitResult<bool> {
    let present: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
        .bind(table)
        .fetch_one(pool)
        .await?;
    Ok(present.is_some())
}

// ── Wallet read-model ────────────────────────────────────────────────────────

pub struct PgWalletReadModelRepository {
    pool: PgPool,
    /// Read-only pool on the IAM database; used by `backfill_wallets` (boot
    /// snapshot) and `backfill_wallet_for` (lazy single-user self-heal).
    source_pool: Option<PgPool>,
}

impl PgWalletReadModelRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool, source_pool: None }
    }

    #[must_use]
    pub fn with_source_pool(mut self, source_pool: PgPool) -> Self {
        self.source_pool = Some(source_pool);
        self
    }
}

/// Upsert one wallet row.
///
/// ORDERING INVARIANT — read carefully: the `ON CONFLICT … WHERE updated_at <=
/// EXCLUDED.updated_at` clause is **NOT** out-of-order protection. `updated_at` is
/// stamped `now()` on every write (`VALUES (…, now())`, and `EXCLUDED.updated_at`
/// is that same `now()`), so the comparison is `now()`-vs-`now()` and always
/// passes — it can never reject a stale redelivery. Correctness rests entirely on
/// IAM keying every wallet event by `user_id` (`iam .../event_bus/kafka.rs`
/// `.key(user_id)`) ⇒ one partition per user ⇒ per-user delivery order is
/// preserved, including on the boot `earliest` replay (a partition re-delivers a
/// user's events in produced order, so the FINAL state converges). A transient
/// wrong-primary window can exist mid-replay, but it self-corrects as the later
/// events re-apply, and recipient resolution is fail-closed on a missing primary
/// (`rpc/service.rs::get_user_primary_wallet`). If IAM's per-user keying ever
/// changes, this needs a real event-time/version guard here AND on
/// `WALLET_DEMOTE_SIBLINGS` (which is currently unguarded).
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

    async fn delete_wallet(&self, user_id: Uuid, wallet_address: &str) -> TraitResult<()> {
        // Revocation propagation (IAM UserWalletUnlinked). Idempotent — deleting
        // a row that is already absent affects zero rows and is not an error.
        sqlx::query(
            r#"DELETE FROM iam_wallet_read_model
                WHERE user_id = $1 AND wallet_address = $2"#,
        )
        .bind(user_id)
        .bind(wallet_address)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn backfill_wallets(&self) -> TraitResult<u64> {
        // Source `is_primary` / `blockchain_registered` are nullable on
        // `user_wallets`; COALESCE to the read-model's NOT NULL columns.
        // Assumes the source holds at most one primary wallet per user (IAM
        // enforces this) — otherwise the partial unique index would reject it.
        let Some(source) = &self.source_pool else {
            // No source pool: either genuine pre-cutover (the `user_wallets` source
            // lives on the local shared DB) or a post-split boot where connecting the
            // source pool failed (transient DB/pgdog unreadiness). Only backfill from
            // the local pool when the source table is actually present there — post
            // DB-split it isn't, and querying it just logs a spurious "relation does
            // not exist" ERROR. The live event feed seeds the read-model regardless,
            // so skip silently instead of erroring.
            if !local_source_table_exists(&self.pool, "user_wallets").await? {
                tracing::info!(
                    "read-model wallet backfill: no source pool and no local `user_wallets` \
                     (post-split); skipping snapshot, event feed will seed"
                );
                return Ok(0);
            }
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
            return Ok(res.rows_affected());
        };

        let rows = sqlx::query(
            r#"SELECT user_id, wallet_address,
                      COALESCE(is_primary, false)            AS is_primary,
                      COALESCE(blockchain_registered, false) AS blockchain_registered,
                      user_account_pda, shard_id
                 FROM user_wallets"#,
        )
        .fetch_all(source)
        .await?;

        let mut inserted = 0u64;
        for row in rows {
            // DO NOTHING (not upsert): the backfill is a snapshot seed and must
            // never overwrite fresher state already applied by the event feed.
            let res = sqlx::query(
                r#"INSERT INTO iam_wallet_read_model
                       (user_id, wallet_address, is_primary, blockchain_registered,
                        user_account_pda, shard_id, updated_at)
                   VALUES ($1, $2, $3, $4, $5, $6, now())
                   ON CONFLICT (user_id, wallet_address) DO NOTHING"#,
            )
            .bind(row.get::<Uuid, _>("user_id"))
            .bind(row.get::<String, _>("wallet_address"))
            .bind(row.get::<bool, _>("is_primary"))
            .bind(row.get::<bool, _>("blockchain_registered"))
            .bind(row.get::<Option<String>, _>("user_account_pda"))
            .bind(row.get::<Option<i16>, _>("shard_id"))
            .execute(&self.pool)
            .await?;
            inserted += res.rows_affected();
        }
        Ok(inserted)
    }

    async fn backfill_wallet_for(&self, user_id: Uuid) -> TraitResult<Option<String>> {
        // Single-user reconcile: pull this user's wallet rows from the source
        // (post-split IAM DB, or the local pool pre-cutover) and upsert each via
        // `upsert_wallet`, so primary-demotion + last-writer-wins match the event
        // path exactly. Then return the resulting primary wallet_address.
        const SELECT_ONE_USER: &str = r#"
            SELECT user_id, wallet_address,
                   COALESCE(is_primary, false)            AS is_primary,
                   COALESCE(blockchain_registered, false) AS blockchain_registered,
                   user_account_pda, shard_id
              FROM user_wallets
             WHERE user_id = $1"#;

        let rows = match &self.source_pool {
            Some(source) => sqlx::query(SELECT_ONE_USER)
                .bind(user_id)
                .fetch_all(source)
                .await?,
            None => {
                // No source pool: only the pre-cutover shared DB can answer.
                // Post-split the table is absent locally — skip rather than error
                // (mirrors the `backfill_wallets` no-source branch).
                if !local_source_table_exists(&self.pool, "user_wallets").await? {
                    return Ok(None);
                }
                sqlx::query(SELECT_ONE_USER)
                    .bind(user_id)
                    .fetch_all(&self.pool)
                    .await?
            }
        };

        for row in &rows {
            let rec = WalletReadModelRecord {
                user_id: row.get::<Uuid, _>("user_id"),
                wallet_address: row.get::<String, _>("wallet_address"),
                is_primary: row.get::<bool, _>("is_primary"),
                blockchain_registered: row.get::<bool, _>("blockchain_registered"),
                user_account_pda: row.get::<Option<String>, _>("user_account_pda"),
                shard_id: row.get::<Option<i16>, _>("shard_id"),
            };
            self.upsert_wallet(&rec).await?;
        }

        // Read the primary back from the freshly-reconciled read-model rather
        // than trusting the source snapshot's flag directly.
        let primary: Option<String> = sqlx::query_scalar(
            "SELECT wallet_address FROM iam_wallet_read_model WHERE user_id = $1 AND is_primary = true",
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(primary)
    }
}

// ── Meter read-model ─────────────────────────────────────────────────────────

pub struct PgMeterReadModelRepository {
    pool: PgPool,
    /// Read-only pool on the metering database; used only by `backfill_meters`.
    source_pool: Option<PgPool>,
}

impl PgMeterReadModelRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool, source_pool: None }
    }

    #[must_use]
    pub fn with_source_pool(mut self, source_pool: PgPool) -> Self {
        self.source_pool = Some(source_pool);
        self
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
        let Some(source) = &self.source_pool else {
            // No source pool: pre-cutover (the `meters` source lives on the local
            // shared DB) or a post-split boot where the source pool connect failed.
            // Only backfill from the local pool when `meters` is actually present
            // there — post DB-split it isn't, and querying it just logs a spurious
            // "relation does not exist" ERROR. The event feed seeds the read-model
            // regardless, so skip silently instead of erroring.
            if !local_source_table_exists(&self.pool, "meters").await? {
                tracing::info!(
                    "read-model meter backfill: no source pool and no local `meters` \
                     (post-split); skipping snapshot, event feed will seed"
                );
                return Ok(0);
            }
            let res = sqlx::query(
                r#"INSERT INTO meter_read_model
                       (serial_number, meter_id, user_id, zone_id, status, updated_at)
                   SELECT serial_number, id, user_id, zone_id, status, now()
                     FROM meters
                   ON CONFLICT (serial_number) DO NOTHING"#,
            )
            .execute(&self.pool)
            .await?;
            return Ok(res.rows_affected());
        };

        let rows = sqlx::query(
            r#"SELECT serial_number, id, user_id, zone_id, status FROM meters"#,
        )
        .fetch_all(source)
        .await?;

        let mut inserted = 0u64;
        for row in rows {
            let res = sqlx::query(
                r#"INSERT INTO meter_read_model
                       (serial_number, meter_id, user_id, zone_id, status, updated_at)
                   VALUES ($1, $2, $3, $4, $5, now())
                   ON CONFLICT (serial_number) DO NOTHING"#,
            )
            .bind(row.get::<String, _>("serial_number"))
            .bind(row.get::<Uuid, _>("id"))
            .bind(row.get::<Uuid, _>("user_id"))
            .bind(row.get::<Option<i32>, _>("zone_id"))
            .bind(row.get::<Option<String>, _>("status"))
            .execute(&self.pool)
            .await?;
            inserted += res.rows_affected();
        }
        Ok(inserted)
    }
}
