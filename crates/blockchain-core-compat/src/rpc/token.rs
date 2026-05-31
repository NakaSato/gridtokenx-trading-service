use anyhow::{anyhow, Result};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature, Signer};
use std::str::FromStr;
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
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow!("Failed to get token balance via gRPC: {}", e))?;

        Ok(response.amount.parse().unwrap_or(0))
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
        let token_program_id =
            Pubkey::try_from("TokenzQdBNbLqP5VEhdkThp9Dz9L33itf29V7D3fR65").unwrap();
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
        let token_program_id =
            Pubkey::try_from("TokenzQdBNbLqP5VEhdkThp9Dz9L33itf29V7D3fR65").unwrap();
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
        let token_program_id =
            Pubkey::from_str("TokenzQdBNbLqP5VEhdkThp9Dz9L33itf29V7D3fR65").unwrap();

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
