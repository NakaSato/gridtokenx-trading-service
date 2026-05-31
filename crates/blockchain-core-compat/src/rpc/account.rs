use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

use super::chain_v1::{GetAccountDataRequest, GetBalanceRequest};
use super::transaction::TransactionHandler;

/// Manages Solana accounts and keypairs
#[derive(Clone)]
pub struct AccountManager {
    transaction_handler: TransactionHandler,
}

impl std::fmt::Debug for AccountManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccountManager").finish()
    }
}

impl AccountManager {
    pub fn new(transaction_handler: TransactionHandler) -> Self {
        Self {
            transaction_handler,
        }
    }

    pub async fn get_balance(&self, pubkey: &Pubkey, force_refresh: bool) -> Result<u64> {
        let response = self
            .transaction_handler
            .get_balance(GetBalanceRequest {
                pubkey: pubkey.to_string(),
                force_refresh,
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow!("Failed to get balance via gRPC: {}", e))?;

        Ok(response.lamports)
    }

    pub async fn account_exists(&self, pubkey: &Pubkey) -> Result<bool> {
        let response = self
            .transaction_handler
            .get_account_data(GetAccountDataRequest {
                pubkey: pubkey.to_string(),
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow!("Failed to check account existence via gRPC: {}", e))?;

        Ok(response.exists)
    }

    pub async fn get_account_data(&self, pubkey: &Pubkey) -> Result<Vec<u8>> {
        let response = self
            .transaction_handler
            .get_account_data(GetAccountDataRequest {
                pubkey: pubkey.to_string(),
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow!("Failed to get account data via gRPC: {}", e))?;

        if !response.exists {
            return Err(anyhow!("Account not found: {}", pubkey));
        }

        Ok(response.data)
    }

    pub fn calculate_ata_address(&self, user_wallet: &Pubkey, mint: &Pubkey) -> Result<Pubkey> {
        // Shared logic: assume Token-2022 for GridTokenX mints
        let token_program_id = Pubkey::from_str("TokenzQdBNbLqP5VEhdkThp9Dz9L33itf29V7D3fR65")?;
        Ok(
            spl_associated_token_account::get_associated_token_address_with_program_id(
                user_wallet,
                mint,
                &token_program_id,
            ),
        )
    }

    pub async fn get_transaction_account_keys(&self, signature: &str) -> Result<Vec<Pubkey>> {
        let response = self
            .transaction_handler
            .get_transaction_details(signature)
            .await?;

        if !response.found {
            return Err(anyhow!("Transaction not found: {}", signature));
        }

        if !response.error.is_empty() {
            return Err(anyhow!("Transaction failed with error: {}", response.error));
        }

        let keys = response
            .account_keys
            .iter()
            .map(|k| {
                Pubkey::from_str(k).map_err(|e| anyhow!("Invalid pubkey in transaction: {}", e))
            })
            .collect::<Result<Vec<Pubkey>>>()?;

        Ok(keys)
    }
}
