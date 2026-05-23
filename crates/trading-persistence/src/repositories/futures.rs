//! Futures repository implementation.

use async_trait::async_trait;
use rust_decimal::Decimal;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use trading_core::models::{FuturesOrder, FuturesPosition, FuturesProduct};
use trading_core::traits::{FuturesRepository, TraitResult};

#[derive(Debug, Clone, FromRow)]
struct FuturesProductDb {
    id: Uuid,
    symbol: String,
    base_asset: String,
    quote_asset: String,
    contract_size: Decimal,
    expiration_date: chrono::DateTime<chrono::Utc>,
    current_price: Decimal,
    is_active: bool,
}

impl From<FuturesProductDb> for FuturesProduct {
    fn from(db: FuturesProductDb) -> Self {
        Self {
            id: db.id,
            symbol: db.symbol,
            base_asset: db.base_asset,
            quote_asset: db.quote_asset,
            contract_size: db.contract_size,
            expiration_date: db.expiration_date,
            current_price: db.current_price,
            is_active: db.is_active,
        }
    }
}

pub struct PostgresFuturesRepository {
    pool: PgPool,
}

impl PostgresFuturesRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl FuturesRepository for PostgresFuturesRepository {
    async fn get_products(&self) -> TraitResult<Vec<FuturesProduct>> {
        let products = sqlx::query_as::<_, FuturesProductDb>(
            "SELECT * FROM futures_products WHERE is_active = true",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(products.into_iter().map(Into::into).collect())
    }

    async fn get_product(&self, id: Uuid) -> TraitResult<Option<FuturesProduct>> {
        let product =
            sqlx::query_as::<_, FuturesProductDb>("SELECT * FROM futures_products WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;

        Ok(product.map(Into::into))
    }

    async fn insert_order(&self, order: &FuturesOrder) -> TraitResult<()> {
        sqlx::query(
            r#"
            INSERT INTO futures_orders (
                id, user_id, product_id, side, order_type, quantity, price, leverage, status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(order.id)
        .bind(order.user_id)
        .bind(order.product_id)
        .bind(order.side)
        .bind(order.order_type)
        .bind(order.quantity)
        .bind(order.price)
        .bind(order.leverage)
        .bind(order.status)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_orders_by_user(&self, _user_id: Uuid) -> TraitResult<Vec<FuturesOrder>> {
        // Implement real query here
        Ok(vec![])
    }

    async fn get_positions_by_user(&self, _user_id: Uuid) -> TraitResult<Vec<FuturesPosition>> {
        // Implement real query here
        Ok(vec![])
    }

    async fn close_position(&self, position_id: Uuid) -> TraitResult<()> {
        sqlx::query("DELETE FROM futures_positions WHERE id = $1")
            .bind(position_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
