//! Meter identity lookups (`meters.id` ↔ `meters.serial_number`).

use async_trait::async_trait;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use uuid::Uuid;

use trading_core::traits::{MeterRepository, TraitResult};

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
        // TODO(db-split): cross-domain read of metering `meters`. Phase 1 keeps this SQL;
        // at cutover replace `FROM meters` with the Trading-owned `meter_read_model`
        // (fed by meter NATS events + backfill), which carries the same
        // serial_number → meter_id mapping.
        // See migrations/20260715000001_trading_phase1_local_models.sql + docs/db-split-phase1.md.
        let row = sqlx::query("SELECT id FROM meters WHERE serial_number = $1")
            .bind(serial)
            .fetch_optional(&self.pool)
            .await?;

        Ok(match row {
            Some(r) => Some(r.try_get("id")?),
            None => None,
        })
    }

    async fn get_serials_for_ids(&self, ids: &[Uuid]) -> TraitResult<HashMap<Uuid, String>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        // TODO(db-split): see resolve_id_by_serial — same `meters` → `meter_read_model` flip.
        let rows = sqlx::query("SELECT id, serial_number FROM meters WHERE id = ANY($1)")
            .bind(ids)
            .fetch_all(&self.pool)
            .await?;

        let mut out = HashMap::with_capacity(rows.len());
        for row in &rows {
            let id: Uuid = row.try_get("id")?;
            let serial: String = row.try_get("serial_number")?;
            out.insert(id, serial);
        }
        Ok(out)
    }
}
