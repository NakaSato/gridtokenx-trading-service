use anyhow::{anyhow, Result};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature, Signer};
use tracing::info;

use super::account::AccountManager;
use super::chain_v1::GetTokenAccountBalanceRequest;
use super::transaction::TransactionHandler;

/// Manages Token operations (mint, burn, transfer)
#[derive(Clone)]
pub struct TokenManager {
    transaction_handler: TransactionHandler,
    account_manager: AccountManager,
}

impl std::fmt::Debug for TokenManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenManager").finish()
    }
}

impl TokenManager {
    pub fn new(transaction_handler: TransactionHandler, account_manager: AccountManager) -> Self {
        Self {
            transaction_handler,
            account_manager,
        }
    }

    pub async fn get_token_balance(&self, owner: &Pubkey, mint: &Pubkey) -> Result<u64> {
        let ata_address = self.account_manager.calculate_ata_address(owner, mint)?;
        if !self.account_manager.account_exists(&ata_address).await? {
            return Ok(0);
        }

        let response = self
            .transaction_handler
            .get_token_account_balance(GetTokenAccountBalanceRequest {
                pubkey: ata_address.to_string(),
            })
            .await
            .map_err(|e| anyhow!("Failed to get token balance via gRPC: {}", e))?;

        // Propagate parse failures rather than masking a malformed amount as 0 —
        // this is a balance on a financial path.
        response
            .amount
            .parse::<u64>()
            .map_err(|e| anyhow!("Invalid token balance amount '{}': {}", response.amount, e))
    }

    pub async fn ensure_token_account_exists(
        &self,
        authority: &Keypair,
        user_wallet: &Pubkey,
        mint: &Pubkey,
    ) -> Result<Pubkey> {
        self.ensure_token_account_exists_with_signer(authority, user_wallet, mint)
            .await
    }

    pub async fn ensure_token_account_exists_with_signer(
        &self,
        authority: &(dyn Signer + Send + Sync),
        user_wallet: &Pubkey,
        mint: &Pubkey,
    ) -> Result<Pubkey> {
        let token_program_id = spl_token_2022::id();
        let ata_address =
            spl_associated_token_account::get_associated_token_address_with_program_id(
                user_wallet,
                mint,
                &token_program_id,
            );

        if self.account_manager.account_exists(&ata_address).await? {
            return Ok(ata_address);
        }

        info!("Creating ATA {} for user {}...", ata_address, user_wallet);
        let create_ata_ix =
            spl_associated_token_account::instruction::create_associated_token_account(
                &authority.pubkey(),
                user_wallet,
                mint,
                &token_program_id,
            );

        let mut transaction = solana_sdk::transaction::Transaction::new_with_payer(
            &[create_ata_ix],
            Some(&authority.pubkey()),
        );
        let bh = self.transaction_handler.get_latest_blockhash().await?;
        transaction.sign(&[authority], bh);

        self.transaction_handler
            .submit_transaction(transaction)
            .await?;

        Ok(ata_address)
    }

    pub async fn transfer_tokens(
        &self,
        authority: &Keypair,
        from_token_account: &Pubkey,
        to_token_account: &Pubkey,
        mint: &Pubkey,
        amount: u64,
        decimals: u8,
    ) -> Result<Signature> {
        self.transfer_tokens_with_signer(
            authority,
            from_token_account,
            to_token_account,
            mint,
            amount,
            decimals,
        )
        .await
    }

    pub async fn transfer_tokens_with_signer(
        &self,
        authority: &(dyn Signer + Send + Sync),
        from_token_account: &Pubkey,
        to_token_account: &Pubkey,
        mint: &Pubkey,
        amount: u64,
        decimals: u8,
    ) -> Result<Signature> {
        let token_program_id = spl_token_2022::id();
        let ix = spl_token::instruction::transfer_checked(
            &token_program_id,
            from_token_account,
            mint,
            to_token_account,
            &authority.pubkey(),
            &[],
            amount,
            decimals,
        )?;

        let mut transaction =
            solana_sdk::transaction::Transaction::new_with_payer(&[ix], Some(&authority.pubkey()));
        let bh = self.transaction_handler.get_latest_blockhash().await?;
        transaction.sign(&[authority], bh);

        self.transaction_handler
            .submit_transaction(transaction)
            .await
    }

    pub async fn mint_energy_tokens(
        &self,
        authority: &Keypair,
        token_account: &Pubkey,
        _user_wallet: &Pubkey,
        mint: &Pubkey,
        amount: Decimal,
    ) -> Result<Signature> {
        self.mint_energy_tokens_with_signer(authority, token_account, _user_wallet, mint, amount)
            .await
    }

    pub async fn mint_energy_tokens_with_signer(
        &self,
        authority: &(dyn Signer + Send + Sync),
        token_account: &Pubkey,
        _user_wallet: &Pubkey,
        mint: &Pubkey,
        amount: Decimal,
    ) -> Result<Signature> {
        let token_program_id = spl_token_2022::id();

        let amount_u64 = (amount * Decimal::from(1_000_000_000))
            .to_u64()
            .unwrap_or(0);

        let ix = spl_token::instruction::mint_to(
            &token_program_id,
            mint,
            token_account,
            &authority.pubkey(),
            &[],
            amount_u64,
        )?;

        let mut transaction =
            solana_sdk::transaction::Transaction::new_with_payer(&[ix], Some(&authority.pubkey()));
        let bh = self.transaction_handler.get_latest_blockhash().await?;
        transaction.sign(&[authority], bh);

        self.transaction_handler
            .submit_transaction(transaction)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::chain_v1::*;
    use crate::rpc::metrics::NoopMetrics;
    use crate::rpc::transaction::ChainBridgeProvider;
    use async_trait::async_trait;
    use std::sync::Arc;

    /// Drives both `account_exists` (via `get_account_data`) and the balance
    /// lookup from one provider. `exists` gates the early `Ok(0)`; `amount` is
    /// the raw string the bridge would return for the token account balance.
    #[derive(Debug)]
    struct BalanceProvider {
        exists: bool,
        amount: String,
    }

    #[async_trait]
    impl ChainBridgeProvider for BalanceProvider {
        async fn get_account_data(
            &self,
            _r: GetAccountDataRequest,
        ) -> Result<GetAccountDataResponse> {
            Ok(GetAccountDataResponse {
                exists: self.exists,
                data: vec![],
            })
        }
        async fn get_token_account_balance(
            &self,
            _r: GetTokenAccountBalanceRequest,
        ) -> Result<GetTokenAccountBalanceResponse> {
            Ok(GetTokenAccountBalanceResponse {
                amount: self.amount.clone(),
                decimals: 9,
                ui_amount: 0.0,
            })
        }
        async fn submit_transaction(
            &self,
            _r: SubmitTransactionRequest,
        ) -> Result<SubmitTransactionResponse> {
            Ok(SubmitTransactionResponse::default())
        }
        async fn get_latest_blockhash(
            &self,
            _r: GetLatestBlockhashRequest,
        ) -> Result<GetLatestBlockhashResponse> {
            Ok(GetLatestBlockhashResponse::default())
        }
        async fn get_recent_prioritization_fees(
            &self,
            _r: GetRecentPrioritizationFeesRequest,
        ) -> Result<GetRecentPrioritizationFeesResponse> {
            Ok(GetRecentPrioritizationFeesResponse::default())
        }
        async fn get_signature_status(
            &self,
            _r: GetSignatureStatusRequest,
        ) -> Result<GetSignatureStatusResponse> {
            Ok(GetSignatureStatusResponse::default())
        }
        async fn get_slot(&self, _r: GetSlotRequest) -> Result<GetSlotResponse> {
            Ok(GetSlotResponse::default())
        }
        async fn get_balance(&self, _r: GetBalanceRequest) -> Result<GetBalanceResponse> {
            Ok(GetBalanceResponse::default())
        }
        async fn simulate_transaction(
            &self,
            _r: SimulateTransactionRequest,
        ) -> Result<SimulateTransactionResponse> {
            Ok(SimulateTransactionResponse::default())
        }
        async fn request_airdrop(
            &self,
            _r: RequestAirdropRequest,
        ) -> Result<RequestAirdropResponse> {
            Ok(RequestAirdropResponse::default())
        }
        async fn get_transaction_details(
            &self,
            _r: GetTransactionDetailsRequest,
        ) -> Result<GetTransactionDetailsResponse> {
            Ok(GetTransactionDetailsResponse::default())
        }
    }

    fn token_manager(exists: bool, amount: &str) -> TokenManager {
        let provider = Arc::new(BalanceProvider {
            exists,
            amount: amount.to_string(),
        });
        let handler = TransactionHandler::new(provider, Arc::new(NoopMetrics {}));
        let accounts = AccountManager::new(handler.clone());
        TokenManager::new(handler, accounts)
    }

    // P3: a malformed balance string must surface as an error, not silently
    // collapse to 0 — masking a parse failure as a zero balance on a financial
    // path could read a funded account as empty.
    #[tokio::test]
    async fn get_token_balance_propagates_parse_error() {
        let mgr = token_manager(true, "not_a_number");
        let err = mgr
            .get_token_balance(&Pubkey::new_unique(), &Pubkey::new_unique())
            .await
            .expect_err("a malformed amount must error, not return 0");
        assert!(
            err.to_string().contains("Invalid token balance amount"),
            "error should name the bad amount, got: {err}"
        );
    }

    #[tokio::test]
    async fn get_token_balance_parses_valid_amount() {
        let mgr = token_manager(true, "4200");
        let bal = mgr
            .get_token_balance(&Pubkey::new_unique(), &Pubkey::new_unique())
            .await
            .expect("a numeric amount should parse");
        assert_eq!(bal, 4200);
    }

    // Absent ATA is the one legitimate zero — short-circuits before the parse.
    #[tokio::test]
    async fn get_token_balance_zero_when_account_absent() {
        let mgr = token_manager(false, "ignored");
        let bal = mgr
            .get_token_balance(&Pubkey::new_unique(), &Pubkey::new_unique())
            .await
            .expect("absent account is a clean zero");
        assert_eq!(bal, 0);
    }
}
