use anyhow::Result;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature, Signer};
use std::str::FromStr;
use tracing::info;

use crate::infra::blockchain::rpc::account_management::AccountManager; // Dependency
use crate::infra::blockchain::rpc::transactions::TransactionHandler;
use crate::infra::blockchain::rpc::utils::BlockchainUtils;

/// Manages Token operations (mint, burn, transfer)
#[derive(Clone, Debug)]
pub struct TokenManager {
    transaction_handler: TransactionHandler,
    account_manager: AccountManager,
}

impl TokenManager {
    pub fn new(transaction_handler: TransactionHandler, account_manager: AccountManager) -> Self {
        Self {
            transaction_handler,
            account_manager,
        }
    }

    /// Get SPL token balance for a user
    pub async fn get_token_balance(&self, owner: &Pubkey, mint: &Pubkey) -> Result<u64> {
        let ata_address = self.account_manager.calculate_ata_address(owner, mint)?;

        if !self.account_manager.account_exists(&ata_address).await? {
            return Ok(0);
        }

        self.transaction_handler
            .get_token_account_balance(&ata_address)
            .await
    }

    /// Ensures user has an Associated Token Account for the token mint
    pub async fn ensure_token_account_exists(
        &self,
        authority: &Keypair,
        user_wallet: &Pubkey,
        mint: &Pubkey,
    ) -> Result<Pubkey> {
        let token_program_id = self
            .transaction_handler
            .get_token_program_for_mint(mint)
            .await?;
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

        self.transaction_handler
            .build_and_send_transaction_with_priority(
                vec![create_ata_ix],
                &[authority],
                "token_transaction",
            )
            .await?;

        Ok(ata_address)
    }

    /// Mint energy tokens directly to a user's token account via Anchor program
    /// The mint authority is the token_info PDA, so we must use the Anchor program CPI
    pub async fn mint_energy_tokens(
        &self,
        authority: &Keypair,
        _user_token_account: &Pubkey, // Not used directly - we derive from wallet
        user_wallet: &Pubkey,
        _mint: &Pubkey, // Not used directly - we derive from program
        amount_kwh: Decimal,
    ) -> Result<Signature> {
        let token_program_id = BlockchainUtils::get_token_program_id()?;

        // Derive the mint PDA from energy_token program
        let energy_token_program_id = std::env::var("SOLANA_ENERGY_TOKEN_PROGRAM_ID")
            .unwrap_or_else(|_| "GzEcWzkb73zcgvgoNRxEiuuT7CEAbzbHcAgjNV25pbLV".to_string());
        let energy_token_program_id = Pubkey::from_str(&energy_token_program_id)?;

        let (mint_pda, _) = Pubkey::find_program_address(&[b"mint_2022"], &energy_token_program_id);

        // Calculate ATA for the user
        let user_token_account =
            spl_associated_token_account::get_associated_token_address_with_program_id(
                user_wallet,
                &mint_pda,
                &token_program_id,
            );

        // Build instructions
        let mut instructions = Vec::new();

        // 1. Create ATA if it doesn't exist (idempotent)
        let create_ata_ix =
            spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                &authority.pubkey(),
                user_wallet,
                &mint_pda,
                &token_program_id,
            );
        instructions.push(create_ata_ix);

        // 2. Build the Anchor mint_tokens_direct instruction via BlockchainUtils
        let mint_instruction = BlockchainUtils::create_mint_instruction(
            authority,
            &user_token_account,
            user_wallet,
            &mint_pda,
            amount_kwh,
        )?;
        instructions.push(mint_instruction);

        let signers = vec![authority];
        self.transaction_handler
            .build_and_send_transaction_with_priority(instructions, &signers, "token_transaction")
            .await
    }

    /// Generic SPL token minting (used by faucet)
    pub async fn mint_spl_tokens(
        &self,
        authority: &Keypair,
        user_wallet: &Pubkey,
        mint: &Pubkey,
        amount: Decimal,
    ) -> Result<Signature> {
        let token_program_id = self
            .transaction_handler
            .get_token_program_for_mint(mint)
            .await?;
        let ata_address = self
            .ensure_token_account_exists(authority, user_wallet, mint)
            .await?;

        // Convert to raw amount (assuming 9 decimals for energy tokens)
        let amount_u64 = (amount.abs() * Decimal::from(1_000_000_000))
            .to_u64()
            .unwrap_or(0);

        let ix = if amount >= Decimal::ZERO {
            info!("Minting {} tokens to {}...", amount, user_wallet);
            spl_token::instruction::mint_to(
                &token_program_id,
                mint,
                &ata_address,
                &authority.pubkey(),
                &[],
                amount_u64,
            )?
        } else {
            info!("Burning {} tokens from {}...", amount.abs(), user_wallet);
            spl_token::instruction::burn(
                &token_program_id,
                &ata_address,
                mint,
                &authority.pubkey(),
                &[],
                amount_u64,
            )?
        };

        self.transaction_handler
            .build_and_send_transaction_with_priority(vec![ix], &[authority], "token_transaction")
            .await
    }

    /// Transfer SPL tokens from one account to another
    pub async fn transfer_tokens(
        &self,
        authority: &Keypair,
        from_token_account: &Pubkey,
        to_token_account: &Pubkey,
        mint: &Pubkey,
        amount: u64,
        decimals: u8,
    ) -> Result<Signature> {
        let token_program_id = self
            .transaction_handler
            .get_token_program_for_mint(mint)
            .await?;

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

        self.transaction_handler
            .build_and_send_transaction_with_priority(vec![ix], &[authority], "token_transaction")
            .await
    }

    /// Burn energy tokens from a user's token account (Compatibility)
    pub async fn burn_energy_tokens(
        &self,
        authority: &Keypair,
        user_token_account: &Pubkey,
        mint: &Pubkey,
        amount_kwh: Decimal,
    ) -> Result<Signature> {
        let burn_instruction = BlockchainUtils::create_burn_instruction(
            authority,
            user_token_account,
            mint,
            amount_kwh,
        )?;

        self.transaction_handler
            .build_and_send_transaction_with_priority(
                vec![burn_instruction],
                &[authority],
                "token_transaction",
            )
            .await
    }

    /// Transfer energy tokens between accounts (Compatibility)
    pub async fn transfer_energy_tokens(
        &self,
        authority: &Keypair,
        from_token_account: &Pubkey,
        to_token_account: &Pubkey,
        mint: &Pubkey,
        amount_kwh: Decimal,
    ) -> Result<Signature> {
        let amount_u64 = (amount_kwh.abs() * Decimal::from(1_000_000_000))
            .to_u64()
            .unwrap_or(0);

        let transfer_instruction = BlockchainUtils::create_transfer_instruction(
            authority,
            from_token_account,
            to_token_account,
            mint,
            amount_u64,
            9, // Decimals
        )?;

        self.transaction_handler
            .build_and_send_transaction_with_priority(
                vec![transfer_instruction],
                &[authority],
                "token_transaction",
            )
            .await
    }
}
