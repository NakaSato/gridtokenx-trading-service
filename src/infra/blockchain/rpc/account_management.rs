use crate::infra::blockchain::rpc::transactions::TransactionHandler;
use crate::infra::blockchain::rpc::utils::BlockchainUtils;
use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature};
use solana_transaction_status;
use std::str::FromStr;
// use crate::api::middleware::metrics::track_wallet_balance_check;
fn track_wallet_balance_check(_wallet: &str, _balance: f64) {}
use tracing::info;
/// Manages Solana accounts and keypairs
#[derive(Clone, Debug)]
pub struct AccountManager {
    transaction_handler: TransactionHandler,
}

impl AccountManager {
    pub fn new(transaction_handler: TransactionHandler) -> Self {
        Self {
            transaction_handler,
        }
    }

    /// Load keypair from a JSON file
    pub fn load_keypair_from_file(filepath: &str) -> Result<Keypair> {
        BlockchainUtils::load_keypair_from_file(filepath)
    }

    /// Get authority keypair
    pub async fn get_authority_keypair(&self) -> Result<Keypair> {
        // Try loading from environment variable first (preferred for production)
        if let Ok(keypair) = BlockchainUtils::load_keypair_from_env("SOLANA_AUTHORITY_SECRET_KEY") {
            info!("Successfully loaded authority keypair from SOLANA_AUTHORITY_SECRET_KEY");
            return Ok(keypair);
        }

        // Fallback to file-based loading (for development)
        let wallet_path = std::env::var("AUTHORITY_WALLET_PATH")
            .unwrap_or_else(|_| "dev-wallet.json".to_string());

        info!("Loading authority keypair from file: {}", wallet_path);
        Self::load_keypair_from_file(&wallet_path)
    }

    /// Get account balance in lamports
    pub async fn get_balance(&self, pubkey: &Pubkey, force_refresh: bool) -> Result<u64> {
        self.transaction_handler.get_balance(pubkey, force_refresh).await
    }

    /// Get account balance in SOL
    pub async fn get_balance_sol(&self, pubkey: &Pubkey, force_refresh: bool) -> Result<f64> {
        let balance = self.transaction_handler.get_balance_sol(pubkey, force_refresh).await?;
        track_wallet_balance_check("authority", balance);
        Ok(balance)
    }

    /// Check if account exists
    pub async fn account_exists(&self, pubkey: &Pubkey) -> Result<bool> {
        self.transaction_handler.account_exists(pubkey).await
    }

    /// Get account data
    pub async fn get_account_data(&self, pubkey: &Pubkey) -> Result<Vec<u8>> {
        self.transaction_handler.get_account_data(pubkey).await
    }

    /// Parse Pubkey from string
    pub fn parse_pubkey(pubkey_str: &str) -> Result<Pubkey> {
        BlockchainUtils::parse_pubkey(pubkey_str)
    }

    /// Calculate the Associated Token Account address
    pub fn calculate_ata_address(&self, user_wallet: &Pubkey, mint: &Pubkey) -> Result<Pubkey> {
        // Use the same token program ID as used for minting
        let token_program_id = BlockchainUtils::get_token_program_id()?;

        let ata_address =
            spl_associated_token_account::get_associated_token_address_with_program_id(
                user_wallet,
                mint,
                &token_program_id,
            );
        Ok(ata_address)
    }

    /// Get transaction account keys
    pub async fn get_transaction_account_keys(&self, signature: &str) -> Result<Vec<Pubkey>> {
        let sig =
            Signature::from_str(signature).map_err(|e| anyhow!("Invalid signature: {}", e))?;

        let tx = self
            .transaction_handler
            .client()
            .get_transaction(&sig, solana_transaction_status::UiTransactionEncoding::Json)?;

        let transaction = tx.transaction.transaction;
        match transaction {
            solana_transaction_status::EncodedTransaction::Json(ui_tx) => match ui_tx.message {
                solana_transaction_status::UiMessage::Parsed(msg) => {
                    let mut keys = Vec::new();
                    for k in &msg.account_keys {
                        let pubkey = Pubkey::from_str(&k.pubkey)
                            .map_err(|e| anyhow!("Invalid pubkey in transaction: {}", e))?;
                        keys.push(pubkey);
                    }
                    Ok(keys)
                }
                solana_transaction_status::UiMessage::Raw(msg) => {
                    let mut keys = Vec::new();
                    for k in &msg.account_keys {
                        let pubkey = Pubkey::from_str(k)
                            .map_err(|e| anyhow!("Invalid pubkey in transaction: {}", e))?;
                        keys.push(pubkey);
                    }
                    Ok(keys)
                }
            },
            _ => Err(anyhow!("Unsupported transaction encoding")),
        }
    }
}
