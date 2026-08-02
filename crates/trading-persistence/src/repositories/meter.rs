//! Meter identity lookups (`meter_read_model.meter_id` ↔ `meter_read_model.serial_number`).

use async_trait::async_trait;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use uuid::Uuid;

use trading_core::traits::{MeterIdentity, MeterRepository, TraitResult};

pub struct PostgresMeterRepository {
    pool: PgPool,
}

impl PostgresMeterRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl MeterRepository for PostgresMeterRepository {
    async fn resolve_id_by_serial(&self, serial: &str) -> TraitResult<Option<Uuid>> {
        // db-split cutover: read the Trading-owned `meter_read_model` (fed by meter
        // NATS events + backfill), which carries the serial_number → meter_id
        // mapping. The metering `meters` table no longer lives in this DB.
        // See migrations/20260715000001_trading_phase1_local_models.sql + docs/db-split-phase1.md.
        let row = sqlx::query("SELECT meter_id FROM meter_read_model WHERE serial_number = $1")
            .bind(serial)
            .fetch_optional(&self.pool)
            .await?;

        Ok(match row {
            Some(r) => Some(r.try_get("meter_id")?),
            None => None,
        })
    }

    async fn get_serials_for_ids(&self, ids: &[Uuid]) -> TraitResult<HashMap<Uuid, String>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        // db-split cutover: see resolve_id_by_serial — Trading-owned `meter_read_model`.
        let rows =
            sqlx::query("SELECT meter_id, serial_number FROM meter_read_model WHERE meter_id = ANY($1)")
                .bind(ids)
                .fetch_all(&self.pool)
                .await?;

        let mut out = HashMap::with_capacity(rows.len());
        for row in &rows {
            let id: Uuid = row.try_get("meter_id")?;
            let serial: String = row.try_get("serial_number")?;
            out.insert(id, serial);
        }
        Ok(out)
    }

    async fn lookup_by_serial(&self, serial: &str) -> TraitResult<Option<MeterIdentity>> {
        let row = sqlx::query(
            "SELECT meter_id, user_id, is_verified FROM meter_read_model WHERE serial_number = $1",
        )
        .bind(serial)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|r| {
            Ok(MeterIdentity {
                meter_id: r.try_get("meter_id")?,
                user_id: r.try_get("user_id")?,
                is_verified: r.try_get("is_verified")?,
            })
        })
        .transpose()
    }

    async fn lookup_by_id(&self, meter_id: Uuid) -> TraitResult<Option<MeterIdentity>> {
        let row = sqlx::query(
            "SELECT meter_id, user_id, is_verified FROM meter_read_model WHERE meter_id = $1",
        )
        .bind(meter_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|r| {
            Ok(MeterIdentity {
                meter_id: r.try_get("meter_id")?,
                user_id: r.try_get("user_id")?,
                is_verified: r.try_get("is_verified")?,
            })
        })
        .transpose()
    }

    async fn has_verified_meter(&self, user_id: Uuid) -> TraitResult<bool> {
        // EXISTS, not COUNT: the gate only asks "at least one", and a seller with
        // a large fleet should not pay to count it on every order. Served by the
        // partial index `idx_meter_read_model_verified_user`.
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM meter_read_model WHERE user_id = $1 AND is_verified
             )",
        )
        .bind(user_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(exists)
    }
}
