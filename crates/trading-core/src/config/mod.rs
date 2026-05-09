//! Service configuration for the GridTokenX trading service.

pub mod tokenization;
pub use tokenization::{TokenizationConfig, ValidationError, ConfigError};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::env;
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub environment: String,
    pub database_url: String,
    pub redis_url: String,
    pub solana_rpc_url: String,
    pub chain_bridge_url: String,
    pub solana_ws_url: String,
    pub energy_token_mint: String,
    pub max_connections: u32,
    pub log_level: String,
    pub tokenization: TokenizationConfig,
    pub solana_programs: SolanaProgramsConfig,
    pub encryption_secret: String,
    pub iam_service_url: String,
    /// Enable Kafka-backed event sourcing (falls back to Redis Streams if false)
    pub kafka_enabled: bool,
    /// Kafka bootstrap servers (e.g., "localhost:9001")
    pub kafka_bootstrap_servers: String,
    /// Topic prefix for trading events (default: "trading")
    pub kafka_topic_prefix: String,
    /// Service role (api or matcher)
    pub role: String,
    /// Platform user ID for automated settlements (surplus buying)
    pub platform_user_id: uuid::Uuid,
    /// Feed-in tariff price per kWh for oracle settlements
    pub oracle_feed_in_tariff: rust_decimal::Decimal,
    pub oracle_bridge_public_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaProgramsConfig {
    pub registry_program_id: String,
    pub oracle_program_id: String,
    pub energy_token_program_id: String,
    pub trading_program_id: String,
    pub governance_program_id: String,
}

impl Default for SolanaProgramsConfig {
    fn default() -> Self {
        Self {
            registry_program_id: "C8RT8L5pZCVDrf9v94CNNk3XPBKZU5p4o4aPnAVQGiTu".to_string(),
            oracle_program_id: "9XqNt1FqeKyhh4jBaagBSDUpJSMJhEy5gi8E5xx2RaeY".to_string(),
            energy_token_program_id: "FC28Av9roMDjx5PHH7GkSQQB6qo1vi4jsXR4ymiaV4CW".to_string(),
            trading_program_id: "HHAG2cG6sGHTWFwiEh1HBgfqZJWBbnsYzv4f5KtHavUr".to_string(),
            governance_program_id: "Czz3aK3CmJfTVJJYDkuu3DcCGfWmuBruC4gbKTqDeq9x".to_string(),
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        info!("CWD: {:?}", std::env::current_dir().unwrap_or_default());
        info!("FEE_COLLECTOR_WALLET: {:?}", std::env::var("FEE_COLLECTOR_WALLET"));
        info!("CURRENCY_TOKEN_MINT: {:?}", std::env::var("CURRENCY_TOKEN_MINT"));
        info!("TOKENIZATION_ENABLE_REAL_BLOCKCHAIN: {:?}", std::env::var("TOKENIZATION_ENABLE_REAL_BLOCKCHAIN"));

        Ok(Config {
            environment: env::var("ENVIRONMENT").unwrap_or_else(|_| "development".to_string()),
            database_url: env::var("TRADING_DATABASE_URL")
                .or_else(|_| env::var("DATABASE_URL"))
                .map_err(|_| anyhow::anyhow!("DATABASE_URL or TRADING_DATABASE_URL environment variable is required"))?,
            redis_url: env::var("REDIS_URL")
                .map_err(|_| anyhow::anyhow!("REDIS_URL environment variable is required"))?,
            solana_rpc_url: env::var("SOLANA_RPC_URL")
                .map_err(|_| anyhow::anyhow!("SOLANA_RPC_URL environment variable is required"))?,
            chain_bridge_url: env::var("CHAIN_BRIDGE_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:5040".to_string()),
            solana_ws_url: env::var("SOLANA_WS_URL")
                .map_err(|_| anyhow::anyhow!("SOLANA_WS_URL environment variable is required"))?,
            energy_token_mint: env::var("ENERGY_TOKEN_MINT").map_err(|_| {
                anyhow::anyhow!("ENERGY_TOKEN_MINT environment variable is required")
            })?,
            max_connections: env::var("MAX_CONNECTIONS")
                .unwrap_or_else(|_| "50".to_string())
                .parse()?,
            log_level: env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string()),
            tokenization: TokenizationConfig::from_env()?,
            solana_programs: SolanaProgramsConfig {
                registry_program_id: env::var("SOLANA_REGISTRY_PROGRAM_ID")
                    .unwrap_or_else(|_| "C8RT8L5pZCVDrf9v94CNNk3XPBKZU5p4o4aPnAVQGiTu".to_string()),
                oracle_program_id: env::var("SOLANA_ORACLE_PROGRAM_ID")
                    .unwrap_or_else(|_| "9XqNt1FqeKyhh4jBaagBSDUpJSMJhEy5gi8E5xx2RaeY".to_string()),
                energy_token_program_id: env::var("SOLANA_ENERGY_TOKEN_PROGRAM_ID")
                    .unwrap_or_else(|_| "FC28Av9roMDjx5PHH7GkSQQB6qo1vi4jsXR4ymiaV4CW".to_string()),
                trading_program_id: env::var("SOLANA_TRADING_PROGRAM_ID")
                    .unwrap_or_else(|_| "HHAG2cG6sGHTWFwiEh1HBgfqZJWBbnsYzv4f5KtHavUr".to_string()),
                governance_program_id: env::var("SOLANA_GOVERNANCE_PROGRAM_ID")
                    .unwrap_or_else(|_| "Czz3aK3CmJfTVJJYDkuu3DcCGfWmuBruC4gbKTqDeq9x".to_string()),
            },
            encryption_secret: env::var("ENCRYPTION_SECRET").unwrap_or_default(),
            iam_service_url: env::var("IAM_SERVICE_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:5010".to_string()),
            kafka_enabled: env::var("KAFKA_EVENTS_ENABLED")
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .unwrap_or(false),
            kafka_bootstrap_servers: env::var("KAFKA_BOOTSTRAP_SERVERS")
                .unwrap_or_else(|_| "localhost:9001".to_string()),
            kafka_topic_prefix: env::var("KAFKA_TOPIC_PREFIX")
                .unwrap_or_else(|_| "trading".to_string()),
            role: env::var("TRADING_ROLE").unwrap_or_else(|_| "api".to_string()),
            platform_user_id: env::var("PLATFORM_USER_ID")
                .unwrap_or_else(|_| "9d27181d-ab85-4a30-86f9-a9cf4701eb5b".to_string())
                .parse()?,
            oracle_feed_in_tariff: env::var("ORACLE_FEED_IN_TARIFF")
                .unwrap_or_else(|_| "0.10".to_string())
                .parse()?,
            oracle_bridge_public_key: env::var("ORACLE_BRIDGE_PUBLIC_KEY")
                .unwrap_or_else(|_| "45S9aX7vNq9Ea9qVb8J5G7W9z9P9z9P9z9P9z9P9z9P9".to_string()),
        })
    }
}
