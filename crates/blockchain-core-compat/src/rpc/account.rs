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
            })
            .await
            .map_err(|e| anyhow!("Failed to get account data via gRPC: {}", e))?;

        if !response.exists {
            return Err(anyhow!("Account not found: {}", pubkey));
        }

        Ok(response.data)
    }

    pub fn calculate_ata_address(&self, user_wallet: &Pubkey, mint: &Pubkey) -> Result<Pubkey> {
        // GridTokenX energy mints are Token-2022. Use the program id directly
        // (matches TokenManager) instead of re-parsing a hardcoded base58 string.
        Ok(
            spl_associated_token_account::get_associated_token_address_with_program_id(
                user_wallet,
                mint,
                &spl_token_2022::id(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::metrics::NoopMetrics;
    use crate::rpc::transaction::{MockChainBridgeProvider, TransactionHandler};
    use std::sync::Arc;

    fn manager() -> AccountManager {
        let handler = TransactionHandler::new(
            Arc::new(MockChainBridgeProvider),
            Arc::new(NoopMetrics {}),
        );
        AccountManager::new(handler)
    }

    // Lock the energy-mint ATA to Token-2022. The register path
    // (BlockchainService::register_user_on_chain), TokenManager, and this helper
    // must all derive the SAME address; classic spl_token here regresses to an
    // orphaned account + silent "0 balance".
    #[test]
    fn calculate_ata_uses_token_2022() {
        let wallet = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let mgr = manager();

        let got = mgr
            .calculate_ata_address(&wallet, &mint)
            .expect("ata derivation should succeed");

        let expected_2022 =
            spl_associated_token_account::get_associated_token_address_with_program_id(
                &wallet,
                &mint,
                &spl_token_2022::id(),
            );
        let classic = spl_associated_token_account::get_associated_token_address_with_program_id(
            &wallet,
            &mint,
            &spl_token::id(),
        );

        assert_eq!(got, expected_2022, "ATA must be derived under Token-2022");
        assert_ne!(got, classic, "ATA must NOT match the classic SPL derivation");
    }
}
