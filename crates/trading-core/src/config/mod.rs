//! Service configuration for the GridTokenX trading service.

pub mod tokenization;
pub use tokenization::{ConfigError, TokenizationConfig, ValidationError};

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
    pub solana_cluster: String,
    pub energy_token_mint: String,
    pub max_connections: u32,
    pub log_level: String,
    pub tokenization: TokenizationConfig,
    pub solana_programs: SolanaProgramsConfig,
    pub encryption_secret: String,
    pub iam_service_url: String,
    pub internal_api_key: String,
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
    pub trading_market_id: String,
}

impl Default for SolanaProgramsConfig {
    fn default() -> Self {
        Self {
            registry_program_id: "5xdQsDuGa1AaLVnddGhevvf2bngCvSob4dAepETS7oaJ".to_string(),
            oracle_program_id: "D5MCbSHxhxZTRFyUMdTHcQvjzwjx5Lb8jg9PQ2LTja8S".to_string(),
            energy_token_program_id: "EzXnJoHSjS6VR7eBwHTkHHAJGqVfRsEvyksqz7uJCBpe".to_string(),
            trading_program_id: "DA9TdkcToi5r7oS7X5CddoMBiGNF3sAGqwPQph1CfLwd".to_string(),
            governance_program_id: "BRQEyx7DHX1Ljx1eNTHUve52aHHwkWckBXGeL9FZPEgZ".to_string(),
            trading_market_id: "mqiBmZcWMc3mor3B8fnSE2xrKThqHW7HzjuhhGKtv9u".to_string(),
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        info!("CWD: {:?}", std::env::current_dir().unwrap_or_default());
        info!(
            "FEE_COLLECTOR_WALLET: {:?}",
            std::env::var("FEE_COLLECTOR_WALLET")
        );
        info!(
            "CURRENCY_TOKEN_MINT: {:?}",
            std::env::var("CURRENCY_TOKEN_MINT")
        );
        info!(
            "TOKENIZATION_ENABLE_REAL_BLOCKCHAIN: {:?}",
            std::env::var("TOKENIZATION_ENABLE_REAL_BLOCKCHAIN")
        );

        Ok(Config {
            environment: env::var("ENVIRONMENT").unwrap_or_else(|_| "development".to_string()),
            database_url: env::var("TRADING_DATABASE_URL")
                .or_else(|_| env::var("DATABASE_URL"))
                .map_err(|_| {
                    anyhow::anyhow!(
                        "DATABASE_URL or TRADING_DATABASE_URL environment variable is required"
                    )
                })?,
            redis_url: env::var("REDIS_URL")
                .map_err(|_| anyhow::anyhow!("REDIS_URL environment variable is required"))?,
            solana_rpc_url: env::var("SOLANA_RPC_URL")
                .map_err(|_| anyhow::anyhow!("SOLANA_RPC_URL environment variable is required"))?,
            chain_bridge_url: env::var("CHAIN_BRIDGE_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:5040".to_string()),
            solana_ws_url: env::var("SOLANA_WS_URL")
                .map_err(|_| anyhow::anyhow!("SOLANA_WS_URL environment variable is required"))?,
            solana_cluster: env::var("SOLANA_CLUSTER").unwrap_or_else(|_| "localnet".to_string()),
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
                    .unwrap_or_else(|_| "5xdQsDuGa1AaLVnddGhevvf2bngCvSob4dAepETS7oaJ".to_string()),
                oracle_program_id: env::var("SOLANA_ORACLE_PROGRAM_ID")
                    .unwrap_or_else(|_| "D5MCbSHxhxZTRFyUMdTHcQvjzwjx5Lb8jg9PQ2LTja8S".to_string()),
                energy_token_program_id: env::var("SOLANA_ENERGY_TOKEN_PROGRAM_ID")
                    .unwrap_or_else(|_| "EzXnJoHSjS6VR7eBwHTkHHAJGqVfRsEvyksqz7uJCBpe".to_string()),
                trading_program_id: env::var("SOLANA_TRADING_PROGRAM_ID")
                    .unwrap_or_else(|_| "DA9TdkcToi5r7oS7X5CddoMBiGNF3sAGqwPQph1CfLwd".to_string()),
                governance_program_id: env::var("SOLANA_GOVERNANCE_PROGRAM_ID")
                    .unwrap_or_else(|_| "BRQEyx7DHX1Ljx1eNTHUve52aHHwkWckBXGeL9FZPEgZ".to_string()),
                trading_market_id: env::var("SOLANA_TRADING_MARKET_ID")
                    .unwrap_or_else(|_| "mqiBmZcWMc3mor3B8fnSE2xrKThqHW7HzjuhhGKtv9u".to_string()),
            },
            encryption_secret: env::var("ENCRYPTION_SECRET").unwrap_or_default(),
            iam_service_url: env::var("IAM_GRPC_URL")
                .or_else(|_| env::var("IAM_SERVICE_URL"))
                .unwrap_or_else(|_| "http://127.0.0.1:5010".to_string()),
            internal_api_key: env::var("INTERNAL_API_KEY").unwrap_or_default(),
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
