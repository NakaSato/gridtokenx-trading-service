use chrono::{DateTime, Utc, Timelike};
use rust_decimal::Decimal;
use sqlx::PgPool;
use tracing::{error, debug};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum CandleResolution {
    #[serde(rename = "1m")]
    Min1,
    #[serde(rename = "5m")]
    Min5,
    #[serde(rename = "15m")]
    Min15,
    #[serde(rename = "1h")]
    Hour1,
    #[serde(rename = "1d")]
    Day1,
}

impl CandleResolution {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Min1 => "1m",
            Self::Min5 => "5m",
            Self::Min15 => "15m",
            Self::Hour1 => "1h",
            Self::Day1 => "1d",
        }
    }

    pub fn all() -> Vec<Self> {
        vec![Self::Min1, Self::Min5, Self::Min15, Self::Hour1, Self::Day1]
    }

    pub fn round_time(&self, dt: DateTime<Utc>) -> DateTime<Utc> {
        match self {
            Self::Min1 => dt.with_nanosecond(0).unwrap().with_second(0).unwrap(),
            Self::Min5 => {
                let minute = (dt.minute() / 5) * 5;
                dt.with_nanosecond(0).unwrap().with_second(0).unwrap().with_minute(minute).unwrap()
            }
            Self::Min15 => {
                let minute = (dt.minute() / 15) * 15;
                dt.with_nanosecond(0).unwrap().with_second(0).unwrap().with_minute(minute).unwrap()
            }
            Self::Hour1 => dt.with_nanosecond(0).unwrap().with_second(0).unwrap().with_minute(0).unwrap(),
            Self::Day1 => dt.with_nanosecond(0).unwrap().with_second(0).unwrap().with_minute(0).unwrap().with_hour(0).unwrap(),
        }
    }
}

pub struct MarketDataService {
    db: PgPool,
}

impl MarketDataService {
    pub fn new(db: PgPool) -> Self {
        Self { db }
    }

    /// Primary entry point for new trade matches
    pub async fn on_order_matched(
        &self,
        zone_id: i32,
        price: Decimal,
        amount: Decimal,
        match_time: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let start = std::time::Instant::now();
        debug!("📊 Aggregating trade into OHLC: zone={}, price={}, amount={}", zone_id, price, amount);

        for resolution in CandleResolution::all() {
            let start_time = resolution.round_time(match_time);
            let res_str = resolution.as_str();

            // Perform UPSERT for each resolution
            if let Err(e) = sqlx::query(
                r#"
                INSERT INTO market_candles (
                    zone_id, resolution, start_time, 
                    open_price, high_price, low_price, close_price, 
                    volume, trades_count
                )
                VALUES ($1, $2::candle_resolution, $3, $4, $4, $4, $4, $5, 1)
                ON CONFLICT (zone_id, resolution, start_time) DO UPDATE
                SET high_price = GREATEST(market_candles.high_price, EXCLUDED.high_price),
                    low_price = LEAST(market_candles.low_price, EXCLUDED.low_price),
                    close_price = EXCLUDED.close_price,
                    volume = market_candles.volume + EXCLUDED.volume,
                    trades_count = market_candles.trades_count + 1,
                    updated_at = NOW()
                "#
            )
            .bind(zone_id)
            .bind(res_str)
            .bind(start_time)
            .bind(price)
            .bind(amount)
            .execute(&self.db)
            .await {
                error!("❌ Failed to update OHLC candle for resolution {}: {}", res_str, e);
            }
        }

        let duration = start.elapsed().as_secs_f64() * 1000.0;
        crate::metrics::record_market_candle_update(zone_id, duration);

        Ok(())
    }
}
