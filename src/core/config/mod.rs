use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::env;

pub mod tokenization;
pub use tokenization::TokenizationConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub environment: String,
    pub database_url: String,
    pub redis_url: String,
    pub solana_rpc_url: String,
    pub solana_ws_url: String,
    pub energy_token_mint: String,
    pub max_connections: u32,
    pub log_level: String,
    pub tokenization: TokenizationConfig,
    pub solana_programs: SolanaProgramsConfig,
    pub encryption_secret: String,
    pub iam_service_url: String,
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
            registry_program_id: "DVoD5K5YRuXXF54a3b6r282jRD8RmtVHGfpw55DHFVDe".to_string(),
            oracle_program_id: "Ad5crRxCcvKFAShAMYtRAD9XKak1cwH1FCE6TrpUA9i2".to_string(),
            energy_token_program_id: "ExZKhghptUk675rjxgHPjJZjczgWWRRwzUTQnqjPTLno".to_string(),
            trading_program_id: "3iFReh5tvdWkLt7eJcvGKsST7wcwZsSHk3z3xCfUwHLw".to_string(),
            governance_program_id: "GzEcWzkb73zcgvgoNRxEiuuT7CEAbzbHcAgjNV25pbLV".to_string(), // Typical default from IAM or similar
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        Ok(Config {
            environment: env::var("ENVIRONMENT").unwrap_or_else(|_| "development".to_string()),
            database_url: env::var("DATABASE_URL")
                .map_err(|_| anyhow::anyhow!("DATABASE_URL environment variable is required"))?,
            redis_url: env::var("REDIS_URL")
                .map_err(|_| anyhow::anyhow!("REDIS_URL environment variable is required"))?,
            solana_rpc_url: env::var("SOLANA_RPC_URL")
                .map_err(|_| anyhow::anyhow!("SOLANA_RPC_URL environment variable is required"))?,
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
                    .unwrap_or_else(|_| "EmiSgo85FVUYWXPtScCMQZBpq9ecZ4jhveg7E7T7F75z".to_string()),
                oracle_program_id: env::var("SOLANA_ORACLE_PROGRAM_ID")
                    .unwrap_or_else(|_| "BRctXUydec2wrP4k2NpqZZT2sVnMfGqpv9bmWn5mTWh9".to_string()),
                energy_token_program_id: env::var("SOLANA_ENERGY_TOKEN_PROGRAM_ID")
                    .unwrap_or_else(|_| "GzEcWzkb73zcgvgoNRxEiuuT7CEAbzbHcAgjNV25pbLV".to_string()),
                trading_program_id: env::var("SOLANA_TRADING_PROGRAM_ID")
                    .unwrap_or_else(|_| "3LXbBJ7sWYYrveHvLoLtwuVYbYd27HPcbpF1DQ8rK1Bo".to_string()),
                governance_program_id: env::var("SOLANA_GOVERNANCE_PROGRAM_ID")
                    .unwrap_or_else(|_| "GzEcWzkb73zcgvgoNRxEiuuT7CEAbzbHcAgjNV25pbLV".to_string()),
            },
            encryption_secret: env::var("ENCRYPTION_SECRET").unwrap_or_default(),
            iam_service_url: env::var("IAM_SERVICE_URL").unwrap_or_else(|_| "http://127.0.0.1:8090".to_string()),
        })
    }
}
