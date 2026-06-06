use anyhow::Result;
use solana_sdk::{
    instruction::Instruction,
    signature::{Keypair, Signature, Signer},
    transaction::Transaction,
};

use super::instructions::InstructionBuilder;
use super::priority_fee::PriorityLevel;
use super::transaction::TransactionHandler;
use crate::config::SolanaProgramsConfig;

/// Manages On-Chain transactions and program interactions
#[derive(Clone)]
pub struct OnChainManager {
    transaction_handler: TransactionHandler,
    instruction_builder: InstructionBuilder,
    program_ids: SolanaProgramsConfig,
}

impl std::fmt::Debug for OnChainManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OnChainManager")
            .field("program_ids", &self.program_ids)
            .finish()
    }
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

    pub async fn submit_transaction(&self, transaction: Transaction) -> Result<Signature> {
        self.transaction_handler
            .submit_transaction(transaction)
            .await
    }

    pub async fn build_and_send_transaction(
        &self,
        instructions: Vec<Instruction>,
        signers: &[&Keypair],
    ) -> Result<Signature> {
        // Use the first signer as payer, or fallback to the builder's default payer (for bridge signing)
        let payer = signers
            .first()
            .map(|k| k.pubkey())
            .unwrap_or_else(|| self.instruction_builder.payer());

        let mut transaction = Transaction::new_with_payer(&instructions, Some(&payer));
        let bh = self.transaction_handler.get_latest_blockhash().await?;

        if !signers.is_empty() {
            transaction.sign(signers, bh);
        } else {
            // Unsigned transaction (bridge will sign) still needs the recent blockhash
            transaction.message.recent_blockhash = bh;
        }

        self.transaction_handler
            .submit_transaction(transaction)
            .await
    }

    pub async fn build_and_send_transaction_with_signers(
        &self,
        instructions: Vec<Instruction>,
        signers: &[&(dyn Signer + Send + Sync)],
    ) -> Result<Signature> {
        // Use the first signer as payer, or fallback to the builder's default payer
        let payer = signers
            .first()
            .map(|k| k.pubkey())
            .unwrap_or_else(|| self.instruction_builder.payer());

        let mut transaction = Transaction::new_with_payer(&instructions, Some(&payer));
        let bh = self.transaction_handler.get_latest_blockhash().await?;

        if !signers.is_empty() {
            transaction.sign(signers, bh);
        } else {
            transaction.message.recent_blockhash = bh;
        }

        self.transaction_handler
            .submit_transaction(transaction)
            .await
    }

    pub async fn build_and_send_transaction_with_priority(
        &self,
        instructions: Vec<Instruction>,
        signers: &[&Keypair],
        transaction_type: &'static str,
        priority: Option<PriorityLevel>,
    ) -> Result<Signature> {
        let mut instructions = instructions;
        self.transaction_handler
            .add_priority_fee_to_instructions(&mut instructions, transaction_type, priority)
            .await?;

        self.build_and_send_transaction(instructions, signers).await
    }

    pub async fn build_and_send_transaction_with_priority_and_signers(
        &self,
        instructions: Vec<Instruction>,
        signers: &[&(dyn Signer + Send + Sync)],
        transaction_type: &'static str,
        priority: Option<PriorityLevel>,
    ) -> Result<Signature> {
        let mut instructions = instructions;
        self.transaction_handler
            .add_priority_fee_to_instructions(&mut instructions, transaction_type, priority)
            .await?;

        self.build_and_send_transaction_with_signers(instructions, signers)
            .await
    }

    pub fn instruction_builder(&self) -> &InstructionBuilder {
        &self.instruction_builder
    }
}
