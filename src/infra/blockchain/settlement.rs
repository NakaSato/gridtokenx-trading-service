use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use std::sync::Arc;
use tracing::info;

use crate::core::error::ApiError;
use crate::domain::trading::settlement::{Settlement, SettlementTransaction};
use crate::infra::blockchain::BlockchainService;

#[derive(Debug)]
pub struct BlockchainSettlementProvider {
    blockchain: Arc<BlockchainService>,
}

impl BlockchainSettlementProvider {
    pub fn new(blockchain: Arc<BlockchainService>) -> Self {
        Self { blockchain }
    }

    #[tracing::instrument(skip(self, settlement), fields(settlement_id = %settlement.id))]
    pub async fn execute_atomic_settlement(
        &self,
        settlement: &Settlement,
        buy_order_pda: &Pubkey,
        sell_order_pda: &Pubkey,
        buyer_pubkey: &Pubkey,
        seller_pubkey: &Pubkey,
    ) -> crate::core::error::Result<SettlementTransaction> {
        let trading_program_id = self
            .blockchain
            .trading_program_id()
            .map_err(|e| ApiError::Internal(format!("Trading program ID error: {}", e)))?;
        let (market_pda, _) = Pubkey::find_program_address(&[b"market"], &trading_program_id);

        let platform_authority = self
            .blockchain
            .get_authority_keypair()
            .await
            .map_err(|e| ApiError::Internal(format!("Failed to get authority: {}", e)))?;

        // Mints from env
        let energy_mint_str = std::env::var("ENERGY_TOKEN_MINT").unwrap_or_default(); info!("DEBUG ENERGY_MINT: '{}'", energy_mint_str);
        let currency_mint_str = std::env::var("CURRENCY_TOKEN_MINT").unwrap_or_default(); info!("DEBUG CURRENCY_MINT: '{}'", currency_mint_str);
        let energy_mint = BlockchainService::parse_pubkey(&energy_mint_str)?;
        let currency_mint = BlockchainService::parse_pubkey(&currency_mint_str)?;

        // ATAs
        let buyer_currency_ata = self
            .blockchain
            .calculate_ata_address(buyer_pubkey, &currency_mint)?;
        let seller_energy_ata = self
            .blockchain
            .calculate_ata_address(seller_pubkey, &energy_mint)?;
        let seller_currency_ata = self
            .blockchain
            .calculate_ata_address(seller_pubkey, &currency_mint)?;
        let buyer_energy_ata = self
            .blockchain
            .calculate_ata_address(buyer_pubkey, &energy_mint)?;

        // Collectors
        let fee_collector = BlockchainService::parse_pubkey(
            &{ let s = std::env::var("FEE_COLLECTOR_WALLET").unwrap_or_default(); info!("DEBUG FEE_COL: '{}'", s); s }
        )?;
        let wheeling_collector = BlockchainService::parse_pubkey(
            &{ let s = std::env::var("WHEELING_COLLECTOR_WALLET").unwrap_or_default(); info!("DEBUG WHEEL_COL: '{}'", s); s }
        )?;
        let loss_collector = BlockchainService::parse_pubkey(
            &{ let s = std::env::var("LOSS_COLLECTOR_WALLET").unwrap_or_default(); info!("DEBUG LOSS_COL: '{}'", s); s }
        )?;

        let fee_collector_ata = self
            .blockchain
            .calculate_ata_address(&fee_collector, &currency_mint)?;
        let wheeling_collector_ata = self
            .blockchain
            .calculate_ata_address(&wheeling_collector, &currency_mint)?;
        let loss_collector_ata = self
            .blockchain
            .calculate_ata_address(&loss_collector, &currency_mint)?;

        // Amounts in atomic units - using direct conversion (more efficient than to_string().parse())
        let amount_atomic = ToPrimitive::to_u64(
            &(settlement.energy_amount * Decimal::from(1_000_000_000i64)).trunc(),
        )
        .unwrap_or(0);
        let price_atomic =
            ToPrimitive::to_u64(&(settlement.price * Decimal::from(1_000_000i64)).trunc())
                .unwrap_or(0);
        let wheeling_val = ToPrimitive::to_u64(
            &(settlement.wheeling_charge.unwrap_or(Decimal::ZERO) * Decimal::from(1_000_000i64))
                .trunc(),
        )
        .unwrap_or(0);
        let loss_val = ToPrimitive::to_u64(
            &(settlement.loss_cost.unwrap_or(Decimal::ZERO) * Decimal::from(1_000_000i64)).trunc(),
        )
        .unwrap_or(0);

        let signature = self
            .blockchain
            .execute_atomic_settlement(
                &platform_authority,
                &platform_authority,
                &market_pda,
                buy_order_pda,
                sell_order_pda,
                &buyer_currency_ata,
                &seller_energy_ata,
                &seller_currency_ata,
                &buyer_energy_ata,
                &fee_collector_ata,
                &wheeling_collector_ata,
                &loss_collector_ata,
                &energy_mint,
                &currency_mint,
                amount_atomic,
                price_atomic,
                wheeling_val,
                loss_val,
            )
            .await?;

        let slot = self.blockchain.get_slot().await?;

        Ok(SettlementTransaction {
            settlement_id: settlement.id,
            signature: signature.to_string(),
            slot,
            confirmation_status: "confirmed".to_string(),
        })
    }

    /// Execute direct energy token transfer (Secondary/Fallback)
    #[tracing::instrument(skip(self, settlement, seller_keypair), fields(settlement_id = %settlement.id))]
    pub async fn execute_energy_transfer(
        &self,
        settlement: &Settlement,
        seller_keypair: &Keypair,
        buyer_pubkey: &Pubkey,
    ) -> crate::core::error::Result<SettlementTransaction> {
        let mint_str = std::env::var("ENERGY_TOKEN_MINT").unwrap_or_default();
        let mint = BlockchainService::parse_pubkey(&mint_str)?;
        let seller_pubkey = seller_keypair.pubkey();

        let seller_token_account = self
            .blockchain
            .calculate_ata_address(&seller_pubkey, &mint)?;
        let buyer_token_account = self.blockchain.calculate_ata_address(buyer_pubkey, &mint)?;

        let effective_energy = settlement
            .effective_energy
            .unwrap_or(settlement.energy_amount);
        let transfer_amount =
            ToPrimitive::to_u64(&(effective_energy * Decimal::from(1_000_000_000i64)).trunc())
                .unwrap_or(0);

        let signature = self
            .blockchain
            .transfer_tokens(
                seller_keypair,
                &seller_token_account,
                &buyer_token_account,
                &mint,
                transfer_amount,
                9,
            )
            .await?;

        let slot = self.blockchain.get_slot().await?;

        Ok(SettlementTransaction {
            settlement_id: settlement.id,
            signature: signature.to_string(),
            slot,
            confirmation_status: "confirmed".to_string(),
        })
    }

    #[tracing::instrument(skip(self, inputs), fields(batch_count = inputs.len()))]
    pub async fn execute_batched_settlements(
        &self,
        inputs: Vec<(
            &crate::domain::trading::settlement::Settlement,
            Pubkey,
            Pubkey,
            Pubkey,
            Pubkey,
        )>,
        priority_fee: u64,
    ) -> crate::core::error::Result<Vec<SettlementTransaction>> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let trading_program_id = self
            .blockchain
            .trading_program_id()
            .map_err(|e| ApiError::Internal(format!("Trading program ID error: {}", e)))?;
        let (market_pda, _) = Pubkey::find_program_address(&[b"market"], &trading_program_id);

        let platform_authority = self
            .blockchain
            .get_authority_keypair()
            .await
            .map_err(|e| ApiError::Internal(format!("Failed to get authority: {}", e)))?;

        // Mints from env
        let energy_mint_str = std::env::var("ENERGY_TOKEN_MINT").unwrap_or_default(); info!("DEBUG ENERGY_MINT: '{}'", energy_mint_str);
        let currency_mint_str = std::env::var("CURRENCY_TOKEN_MINT").unwrap_or_default(); info!("DEBUG CURRENCY_MINT: '{}'", currency_mint_str);
        let energy_mint = BlockchainService::parse_pubkey(&energy_mint_str)?;
        let currency_mint = BlockchainService::parse_pubkey(&currency_mint_str)?;

        // Collectors
        let fee_collector = BlockchainService::parse_pubkey(
            &{ let s = std::env::var("FEE_COLLECTOR_WALLET").unwrap_or_default(); info!("DEBUG FEE_COL: '{}'", s); s }
        )?;
        let wheeling_collector = BlockchainService::parse_pubkey(
            &{ let s = std::env::var("WHEELING_COLLECTOR_WALLET").unwrap_or_default(); info!("DEBUG WHEEL_COL: '{}'", s); s }
        )?;
        let loss_collector = BlockchainService::parse_pubkey(
            &{ let s = std::env::var("LOSS_COLLECTOR_WALLET").unwrap_or_default(); info!("DEBUG LOSS_COL: '{}'", s); s }
        )?;

        let fee_collector_ata = self
            .blockchain
            .calculate_ata_address(&fee_collector, &currency_mint)?;
        let wheeling_collector_ata = self
            .blockchain
            .calculate_ata_address(&wheeling_collector, &currency_mint)?;
        let loss_collector_ata = self
            .blockchain
            .calculate_ata_address(&loss_collector, &currency_mint)?;

        let mut instructions = Vec::new();
        let mut settlement_ids = Vec::new();

        for (settlement, buy_order_pda, sell_order_pda, buyer_pubkey, seller_pubkey) in inputs
        {
            // ATAs
            let buyer_currency_ata = self
                .blockchain
                .calculate_ata_address(&buyer_pubkey, &currency_mint)?;
            let seller_energy_ata = self
                .blockchain
                .calculate_ata_address(&seller_pubkey, &energy_mint)?;
            let seller_currency_ata = self
                .blockchain
                .calculate_ata_address(&seller_pubkey, &currency_mint)?;
            let buyer_energy_ata = self
                .blockchain
                .calculate_ata_address(&buyer_pubkey, &energy_mint)?;

            // Amounts in atomic units - using direct conversion
            let amount_atomic = (settlement.energy_amount * Decimal::from(1_000_000_000i64))
                .trunc()
                .to_u64()
                .unwrap_or(0);
            let price_atomic = (settlement.price * Decimal::from(1_000_000i64))
                .trunc()
                .to_u64()
                .unwrap_or(0);
            let wheeling_val = (settlement.wheeling_charge.unwrap_or(Decimal::ZERO)
                * Decimal::from(1_000_000i64))
            .trunc()
            .to_u64()
            .unwrap_or(0);
            let loss_val = (settlement.loss_cost.unwrap_or(Decimal::ZERO)
                * Decimal::from(1_000_000i64))
            .trunc()
            .to_u64()
            .unwrap_or(0);

            let instruction = self
                .blockchain
                .build_atomic_settlement_instruction(
                    &market_pda,
                    &buy_order_pda,
                    &sell_order_pda,
                    &buyer_currency_ata,
                    &seller_energy_ata,
                    &seller_currency_ata,
                    &buyer_energy_ata,
                    &fee_collector_ata,
                    &wheeling_collector_ata,
                    &loss_collector_ata,
                    &energy_mint,
                    &currency_mint,
                    &platform_authority.pubkey(),
                    &platform_authority.pubkey(),
                    amount_atomic,
                    price_atomic,
                    wheeling_val,
                    loss_val,
                )
                .map_err(|e| ApiError::Internal(format!("Failed to build instruction: {}", e)))?;

            instructions.push(instruction);
            settlement_ids.push(settlement.id);
        }

        // Add priority fee if provided
        if priority_fee > 0 {
            self.blockchain.add_priority_fee_to_instructions(&mut instructions, "settlement")?;
        }

        let signature = self
            .blockchain
            .execute_batched_instructions(&platform_authority, instructions)
            .await
            .map_err(|e| ApiError::Internal(format!("Batch settlement execution failed: {}", e)))?;

        let slot = self.blockchain.get_slot().await.unwrap_or(0);

        let tx_results = settlement_ids
            .into_iter()
            .map(|id| SettlementTransaction {
                settlement_id: id,
                signature: signature.to_string(),
                slot,
                confirmation_status: "confirmed".to_string(),
            })
            .collect();

        Ok(tx_results)
    }

    /// Execute on-chain generation mint (directly minting GRX to prosumer wallet)
    /// Called by the Oracle Bridge after a 15-minute billing window closes.
    #[tracing::instrument(skip(self), fields(wallet = %user_wallet, amount = %amount_kwh))]
    pub async fn execute_generation_mint(
        &self,
        user_wallet: &Pubkey,
        amount_kwh: Decimal,
        _timestamp: i64,
    ) -> crate::core::error::Result<String> {
        let platform_authority = self
            .blockchain
            .get_authority_keypair()
            .await
            .map_err(|e| ApiError::Internal(format!("Failed to get authority: {}", e)))?;

        let energy_mint_str = std::env::var("ENERGY_TOKEN_MINT").unwrap_or_default();
        let energy_mint = BlockchainService::parse_pubkey(&energy_mint_str)?;

        // Calculate atomic units for energy (9 decimal places for GRX)
        let amount_atomic = ToPrimitive::to_u64(&(amount_kwh * Decimal::from(1_000_000_000i64)).trunc())
            .unwrap_or(0);

        // Calculate User ATA
        let user_ata = self.blockchain.calculate_ata_address(user_wallet, &energy_mint)?;

        // 1. Build Instruction (Using Energy Token Program)
        let instruction = self.blockchain.instruction_builder().build_mint_instruction(
            &user_ata.to_string(),
            amount_atomic
        ).map_err(|e| ApiError::Internal(format!("Failed to build mint instruction: {}", e)))?;

        // 2. Execute Transaction
        let signature = self.blockchain.build_and_send_transaction_with_priority(
            vec![instruction],
            &[&platform_authority],
            "generation_mint",
        ).await.map_err(|e| ApiError::Internal(format!("Blockchain mint failed: {}", e)))?;

        Ok(signature.to_string())
    }
}
