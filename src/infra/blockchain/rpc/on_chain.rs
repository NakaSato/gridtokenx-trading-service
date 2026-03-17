use anyhow::{anyhow, Result}; // Added anyhow macro
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature};
use solana_sdk::transaction::Transaction;
use std::str::FromStr;
// Removed Arc usage if not needed (but struct uses it implicitely? No, struct uses Sync objects?)
// OnChainManager struct definition uses TransactionHandler which is cheap to clone (Arc internal).
// Wait, OnChainManager definition:
// pub struct OnChainManager { transaction_handler: TransactionHandler, ... }
// Does it use Arc explicitly?
// No.
// Let's remove std::sync::Arc.

use crate::core::config::SolanaProgramsConfig;
use crate::infra::blockchain::rpc::instructions::InstructionBuilder;
use crate::infra::blockchain::rpc::transactions::TransactionHandler;

/// Manages On-Chain transactions and program interactions
#[derive(Clone, Debug)]
pub struct OnChainManager {
    transaction_handler: TransactionHandler,
    instruction_builder: InstructionBuilder,
    program_ids: SolanaProgramsConfig,
}

impl OnChainManager {
    pub fn new(
        transaction_handler: TransactionHandler,
        instruction_builder: InstructionBuilder,
        program_ids: SolanaProgramsConfig,
    ) -> Self {
        Self {
            transaction_handler,
            instruction_builder,
            program_ids,
        }
    }

    /// Submit raw transaction
    pub async fn submit_transaction(&self, transaction: Transaction) -> Result<Signature> {
        self.transaction_handler
            .submit_transaction(transaction)
            .await
    }

    /// Confirm transaction
    pub async fn confirm_transaction(&self, signature: &str) -> Result<bool> {
        self.transaction_handler
            .confirm_transaction(signature)
            .await
    }

    /// Build and send transaction
    pub async fn build_and_send_transaction(
        &self,
        instructions: Vec<Instruction>,
        signers: &[&Keypair],
    ) -> Result<Signature> {
        self.transaction_handler
            .build_and_send_transaction(instructions, signers)
            .await
    }

    /// Build and send transaction with priority
    pub async fn build_and_send_transaction_with_priority(
        &self,
        instructions: Vec<Instruction>,
        signers: &[&Keypair],
        transaction_type: &'static str,
    ) -> Result<Signature> {
        self.transaction_handler
            .build_and_send_transaction_with_priority(instructions, signers, transaction_type)
            .await
    }

    /// Get account data
    pub async fn get_account_data(&self, pubkey: &Pubkey) -> Result<Vec<u8>> {
        self.transaction_handler.get_account_data(pubkey).await
    }

    /// Get transaction handler
    pub fn transaction_handler(&self) -> &TransactionHandler {
        &self.transaction_handler
    }

    // Program ID getters
    pub fn trading_program_id(&self) -> Result<Pubkey> {
        Pubkey::from_str(&self.program_ids.trading_program_id)
            .map_err(|e| anyhow!("Invalid Trading ID: {}", e))
    }

    // Proxy methods for InstructionBuilder would go here or be accessed directly via getter
    pub fn instruction_builder(&self) -> &InstructionBuilder {
        &self.instruction_builder
    }
}
