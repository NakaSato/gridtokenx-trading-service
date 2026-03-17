use super::account_management::AccountManager;
use super::instructions::InstructionBuilder;
use super::on_chain::OnChainManager;
use super::token_management::TokenManager;
use super::transactions::TransactionHandler;
use super::utils::BlockchainUtils;
use crate::core::config::SolanaProgramsConfig;
use anyhow::{anyhow, Result};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    transaction::Transaction,
};
use std::str::FromStr;
use rust_decimal::Decimal;
use std::sync::Arc;
// use crate::api::middleware::metrics::track_blockchain_operation;
fn track_blockchain_operation(_op: &str, _duration: f64, _success: bool) {}
use tracing::{debug, info, warn};

/// Blockchain service for interacting with Solana programs
#[derive(Clone)]
pub struct BlockchainService {
    pub transaction_handler: TransactionHandler,
    instruction_builder: InstructionBuilder,
    rpc_client: Arc<RpcClient>,
    cluster: String,
    program_ids: SolanaProgramsConfig,

    // Sub-services
    pub account_manager: AccountManager,
    pub token_manager: TokenManager,
    pub on_chain_manager: OnChainManager,
}

impl std::fmt::Debug for BlockchainService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockchainService")
            .field("cluster", &self.cluster)
            .field("program_ids", &self.program_ids)
            .finish()
    }
}

impl BlockchainService {
    /// Create a new blockchain service with program IDs from config
    pub fn new(
        rpc_url: String,
        cluster: String,
        program_ids: SolanaProgramsConfig,
    ) -> Result<Self> {
        info!("Initializing blockchain service for cluster: {}", cluster);

        let rpc_client = Arc::new(RpcClient::new(rpc_url));
        let transaction_handler = TransactionHandler::new(Arc::clone(&rpc_client));

        // Load authority keypair to get the payer pubkey
        let authority_path = std::env::var("AUTHORITY_WALLET_PATH")
            .unwrap_or_else(|_| "dev-wallet.json".to_string());

        let payer = match BlockchainUtils::load_keypair_from_file(&authority_path) {
            Ok(keypair) => {
                info!("Loaded authority keypair: {}", keypair.pubkey());
                keypair.pubkey()
            }
            Err(e) => {
                warn!(
                    "Failed to load authority keypair: {}. Using placeholder.",
                    e
                );
                "11111111111111111111111111111112".parse().unwrap_or_else(|_| Pubkey::default())
            }
        };

        let instruction_builder = InstructionBuilder::new(payer);

        // Initialize sub-managers
        let account_manager = AccountManager::new(transaction_handler.clone());
        let token_manager = TokenManager::new(transaction_handler.clone(), account_manager.clone());
        let on_chain_manager = OnChainManager::new(
            transaction_handler.clone(),
            instruction_builder.clone(),
            program_ids.clone(),
        );

        Ok(Self {
            transaction_handler,
            instruction_builder,
            rpc_client,
            cluster,
            program_ids,
            account_manager,
            token_manager,
            on_chain_manager,
        })
    }

    /// Get the RPC client
    pub fn client(&self) -> &RpcClient {
        &self.rpc_client
    }

    /// Get the cluster name
    pub fn cluster(&self) -> &str {
        &self.cluster
    }

    /// Get the payer pubkey
    pub fn payer_pubkey(&self) -> Pubkey {
        self.instruction_builder.payer()
    }

    /// Get the instruction builder
    pub fn instruction_builder(&self) -> &InstructionBuilder {
        &self.instruction_builder
    }

    /// Submit transaction to blockchain
    pub async fn submit_transaction(&self, transaction: Transaction) -> Result<Signature> {
        self.on_chain_manager.submit_transaction(transaction).await
    }

    /// Add priority fee to instruction list
    pub fn add_priority_fee_to_instructions(
        &self,
        instructions: &mut Vec<Instruction>,
        tx_type: &'static str,
    ) -> Result<()> {
        self.transaction_handler
            .add_priority_fee_to_instructions(instructions, tx_type)
    }

    /// Confirm transaction status
    pub async fn confirm_transaction(&self, signature: &str) -> Result<bool> {
        self.on_chain_manager.confirm_transaction(signature).await
    }

    // DISABLED - uses models module
    // /// Get trade record from blockchain
    // pub async fn get_trade_record(
    //     &self,
    //     signature: &str,
    // ) -> Result<crate::domain::trading::models::TradeRecord> {
    //     self.transaction_handler.get_trade_record(signature).await
    // }

    /// Check if the service is healthy
    pub async fn health_check(&self) -> Result<bool> {
        self.transaction_handler.health_check().await
    }

    /// Request airdrop (devnet/localnet only)
    pub async fn request_airdrop(&self, pubkey: &Pubkey, lamports: u64) -> Result<Signature> {
        self.transaction_handler
            .request_airdrop(pubkey, lamports)
            .await
    }

    /// Get account balance in lamports
    pub async fn get_balance(&self, pubkey: &Pubkey) -> Result<u64> {
        self.account_manager.get_balance(pubkey).await
    }

    /// Get account balance in SOL
    pub async fn get_balance_sol(&self, pubkey: &Pubkey) -> Result<f64> {
        self.account_manager.get_balance_sol(pubkey).await
    }

    /// Get SPL token balance for a user
    pub async fn get_token_balance(&self, owner: &Pubkey, mint: &Pubkey) -> Result<u64> {
        self.token_manager.get_token_balance(owner, mint).await
    }

    /// Send and confirm a transaction
    pub async fn send_and_confirm_transaction(
        &self,
        transaction: &Transaction,
    ) -> Result<Signature> {
        self.transaction_handler
            .send_and_confirm_transaction(transaction)
            .await
    }

    /// Get transaction status
    pub async fn get_signature_status(&self, signature: &Signature) -> Result<Option<bool>> {
        self.transaction_handler
            .get_signature_status(signature)
            .await
    }

    /// Get recent blockhash
    pub async fn get_latest_blockhash(&self) -> Result<solana_sdk::hash::Hash> {
        self.transaction_handler.get_latest_blockhash().await
    }

    /// Get slot height
    pub async fn get_slot(&self) -> Result<u64> {
        self.transaction_handler.get_slot().await
    }

    /// Get account data
    pub async fn get_account_data(&self, pubkey: &Pubkey) -> Result<Vec<u8>> {
        self.account_manager.get_account_data(pubkey).await
    }

    /// Initialize the registry on-chain (localnet bootstrapping)
    pub async fn initialize_registry(&self, authority: &Keypair) -> Result<Signature> {
        info!("Initializing Registry on-chain...");
        let start = std::time::Instant::now();
        let instruction = self.instruction_builder.build_initialize_registry_instruction()?;
        let res = self.build_and_send_transaction(vec![instruction], &[authority]).await;
        track_blockchain_operation("initialize_registry", start.elapsed().as_millis() as f64, res.is_ok());
        res
    }

    /// Initialize a registry shard on-chain
    pub async fn initialize_registry_shard(&self, authority: &Keypair, shard_id: u8) -> Result<Signature> {
        info!("Initializing Registry Shard {} on-chain...", shard_id);
        let start = std::time::Instant::now();
        let instruction = self.instruction_builder.build_initialize_shard_instruction(shard_id)?;
        let res = self.build_and_send_transaction(vec![instruction], &[authority]).await;
        track_blockchain_operation("initialize_shard", start.elapsed().as_millis() as f64, res.is_ok());
        res
    }

    /// Initialize the oracle on-chain (localnet bootstrapping)
    pub async fn initialize_oracle(&self, authority: &Keypair, api_gateway: &Pubkey) -> Result<Signature> {
        info!("Initializing Oracle on-chain with API Gateway: {}...", api_gateway);
        let start = std::time::Instant::now();
        let instruction = self.instruction_builder.build_initialize_oracle_instruction(api_gateway)?;
        let res = self.build_and_send_transaction(vec![instruction], &[authority]).await;
        track_blockchain_operation("initialize_oracle", start.elapsed().as_millis() as f64, res.is_ok());
        res
    }

    /// Set the oracle authority in the Registry program (admin only)
    pub async fn set_oracle_authority(&self, authority: &Keypair, oracle: &Pubkey) -> Result<Signature> {
        info!("Setting oracle authority in Registry to: {}...", oracle);
        let instruction = BlockchainUtils::create_set_oracle_authority_instruction(authority, oracle)?;
        self.build_and_send_transaction(vec![instruction], &[authority]).await
    }

    /// Initialize the governance (PoA) on-chain (localnet bootstrapping)
    pub async fn initialize_governance(&self, authority: &Keypair) -> Result<Signature> {
        info!("Initializing Governance (PoA) on-chain...");
        let instruction = self.instruction_builder.build_initialize_governance_instruction()?;
        self.build_and_send_transaction(vec![instruction], &[authority]).await
    }

    /// Initialize the Energy Token program mint on-chain (localnet bootstrapping)
    pub async fn initialize_energy_token(&self, authority: &Keypair) -> Result<Signature> {
        info!("Initializing Energy Token on-chain with Authority: {}", authority.pubkey());
        let instruction = self.instruction_builder.build_initialize_energy_token_instruction(authority.pubkey())?;
        
        for (i, acc) in instruction.accounts.iter().enumerate() {
            info!("  Account {}: {} (signer: {}, writable: {})", i, acc.pubkey, acc.is_signer, acc.is_writable);
        }

        self.build_and_send_transaction(vec![instruction], &[authority]).await
    }

    /// Initialize the Trading Market on-chain
    pub async fn initialize_trading_market(&self, authority: &Keypair, num_shards: u8) -> Result<Signature> {
        info!("Initializing Trading Market on-chain with Authority: {}, shards: {}", authority.pubkey(), num_shards);
        let instruction = self.instruction_builder.build_initialize_market_instruction(authority.pubkey(), num_shards)?;
        
        self.build_and_send_transaction(vec![instruction], &[authority]).await
    }

    /// Initialize a Zone Market on-chain
    pub async fn initialize_zone_market(
        &self,
        authority: &Keypair,
        zone_id: u32,
        num_shards: u8,
    ) -> Result<Signature> {
        let market_pubkey = self.instruction_builder.get_market_pda()?;
        info!("Initializing Zone Market {} on-chain (market: {})", zone_id, market_pubkey);
        
        let instruction = self.instruction_builder.build_initialize_zone_market_instruction(
            market_pubkey,
            authority.pubkey(),
            zone_id,
            num_shards,
        )?;
        
        self.build_and_send_transaction(vec![instruction], &[authority]).await
    }

    /// Issue an ERC certificate on-chain
    pub async fn issue_erc(
        &self,
        certificate_id: &str,
        user_wallet: &Pubkey,
        meter_account: &Pubkey,
        energy_amount: u64,
        renewable_source: &str,
        validation_data: &str,
        authority: &Keypair,
    ) -> Result<Signature> {
        info!("Issuing ERC {} on-chain for {} kWh", certificate_id, energy_amount);
        let start = std::time::Instant::now();
        let instruction = self.instruction_builder.build_issue_erc_instruction(
            certificate_id,
            user_wallet,
            meter_account,
            energy_amount,
            renewable_source,
            validation_data,
        )?;
        let res = self.build_and_send_transaction(vec![instruction], &[authority]).await;
        track_blockchain_operation("issue_erc", start.elapsed().as_millis() as f64, res.is_ok());
        res
    }

    /// Transfer an ERC certificate on-chain
    pub async fn transfer_erc(
        &self,
        certificate_id: &str,
        owner: &Keypair,
        new_owner: &Pubkey,
    ) -> Result<Signature> {
        info!("Transferring ERC {} on-chain to {}", certificate_id, new_owner);
        let instruction = self.instruction_builder.build_transfer_erc_instruction(
            certificate_id,
            &owner.pubkey(),
            new_owner,
        )?;
        self.build_and_send_transaction(vec![instruction], &[owner]).await
    }

    /// Revoke (retire) an ERC certificate on-chain
    pub async fn revoke_erc(
        &self,
        certificate_id: &str,
        reason: &str,
        authority: &Keypair,
    ) -> Result<Signature> {
        info!("Revoking ERC {} on-chain (Reason: {})", certificate_id, reason);
        let instruction = self.instruction_builder.build_revoke_erc_instruction(
            certificate_id,
            reason,
        )?;
        self.build_and_send_transaction(vec![instruction], &[authority]).await
    }

    /// Check if an account exists
    pub async fn account_exists(&self, pubkey: &Pubkey) -> Result<bool> {
        self.account_manager.account_exists(pubkey).await
    }

    /// Get transaction account keys
    pub async fn get_transaction_account_keys(&self, signature: &str) -> Result<Vec<Pubkey>> {
        self.account_manager
            .get_transaction_account_keys(signature)
            .await
    }

    /// Parse Pubkey from string
    pub fn parse_pubkey(pubkey_str: &str) -> Result<Pubkey> {
        AccountManager::parse_pubkey(pubkey_str)
    }

    /// Get Registry program ID from config
    pub fn registry_program_id(&self) -> Result<Pubkey> {
        Pubkey::from_str(&self.program_ids.registry_program_id).map_err(|e| {
            anyhow!(
                "Invalid Registry Program ID '{}': {}",
                self.program_ids.registry_program_id,
                e
            )
        })
    }

    /// Get Oracle program ID from config
    pub fn oracle_program_id(&self) -> Result<Pubkey> {
        Pubkey::from_str(&self.program_ids.oracle_program_id).map_err(|e| {
            anyhow!(
                "Invalid Oracle Program ID '{}': {}",
                self.program_ids.oracle_program_id,
                e
            )
        })
    }

    /// Get Governance program ID from config
    pub fn governance_program_id(&self) -> Result<Pubkey> {
        Pubkey::from_str(&self.program_ids.governance_program_id).map_err(|e| {
            anyhow!(
                "Invalid Governance Program ID '{}': {}",
                self.program_ids.governance_program_id,
                e
            )
        })
    }

    /// Get Energy Token program ID from config
    pub fn energy_token_program_id(&self) -> Result<Pubkey> {
        Pubkey::from_str(&self.program_ids.energy_token_program_id).map_err(|e| {
            anyhow!(
                "Invalid Energy Token Program ID '{}': {}",
                self.program_ids.energy_token_program_id,
                e
            )
        })
    }

    /// Get Trading program ID from config
    pub fn trading_program_id(&self) -> Result<Pubkey> {
        Pubkey::from_str(&self.program_ids.trading_program_id).map_err(|e| {
            anyhow!(
                "Invalid Trading Program ID '{}': {}",
                self.program_ids.trading_program_id,
                e
            )
        })
    }

    // ====================================================================
    // Instruction Building Methods (delegated to InstructionBuilder)
    // ====================================================================

    /// Get active orders count from ZoneMarket
    async fn get_zone_market_active_orders(&self, zone_market_pubkey: &Pubkey) -> Result<u32> {
        let client = Arc::clone(&self.rpc_client);
        let zone_market_pubkey = *zone_market_pubkey;

        let active_orders = tokio::task::spawn_blocking(move || {
            let account = client.get_account(&zone_market_pubkey)?;
            // Offset for ZoneMarket.active_orders (offset 56, u32)
            // Layout: Disc(8) + Market(32) + ZoneId(4) + NumShards(1) + Pad(3) + Vol(8) = 56
            if account.data.len() < 60 {
                return Err(anyhow!("ZoneMarket account data too small (expected at least 60 bytes, got {})", account.data.len()));
            }
            let active_orders_bytes: [u8; 4] = account.data[56..60]
                .try_into()
                .expect("slice length already verified to be 4 bytes");
            Ok(u32::from_le_bytes(active_orders_bytes))
        })
        .await??;

        Ok(active_orders)
    }

    /// Derive order PDA
    pub fn derive_order_pda(
        &self,
        authority: &Pubkey,
        index: u64,
    ) -> Result<Pubkey> {
        let (pda, _) = Pubkey::find_program_address(
            &[b"order", authority.as_ref(), &index.to_le_bytes()],
            &self.trading_program_id()?,
        );
        Ok(pda)
    }

    /// Execute on-chain match_orders
    pub async fn execute_match_orders(
        &self,
        authority: &Keypair,
        market_pubkey: &str,
        buy_order_pubkey: &str,
        sell_order_pubkey: &str,
        match_amount: u64,
        zone_id: u32,
    ) -> Result<Signature> {
        // Parse order pubkeys
        let buy_order = Pubkey::from_str(buy_order_pubkey)?;
        let sell_order = Pubkey::from_str(sell_order_pubkey)?;

        // Derive trade_record PDA (must match on-chain seeds)
        let (trade_record_pda, _bump) = Pubkey::find_program_address(
            &[b"trade", buy_order.as_ref(), sell_order.as_ref()],
            &self.trading_program_id()?,
        );

        let instruction = self.instruction_builder.build_match_orders_instruction(
            market_pubkey,
            buy_order_pubkey,
            sell_order_pubkey,
            match_amount,
            trade_record_pda,
            zone_id,
        )?;

        // Only authority signs (trade_record is a PDA, not a signer)
        let start = std::time::Instant::now();
        let signers = vec![authority];
        let res = self.build_and_send_transaction_with_priority(
            vec![instruction],
            &signers,
            "token_transaction",
        )
        .await;

        track_blockchain_operation("execute_match_orders", start.elapsed().as_millis() as f64, res.is_ok());
        res
    }

    /// Execute off-chain matched settlement on-chain
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_settle_offchain_match(
        &self,
        market_pubkey: &Pubkey,
        buyer_payload: &super::instructions::OffchainOrderPayload,
        seller_payload: &super::instructions::OffchainOrderPayload,
        match_amount: u64,
        match_price: u64,
        wheeling_charge: u64,
        loss_cost: u64,
        // Accounts
        buyer_currency_ata: &Pubkey,
        seller_currency_ata: &Pubkey,
        seller_energy_ata: &Pubkey,
        buyer_energy_ata: &Pubkey,
        fee_collector_ata: &Pubkey,
        wheeling_collector_ata: &Pubkey,
        loss_collector_ata: &Pubkey,
        currency_mint: &Pubkey,
        energy_mint: &Pubkey,
    ) -> Result<Signature> {
        let authority = self.get_authority_keypair().await?;
        
        let instruction = self.instruction_builder.build_settle_offchain_match_instruction(
            market_pubkey,
            buyer_payload,
            seller_payload,
            match_amount,
            match_price,
            wheeling_charge,
            loss_cost,
            buyer_currency_ata,
            seller_currency_ata,
            seller_energy_ata,
            buyer_energy_ata,
            fee_collector_ata,
            wheeling_collector_ata,
            loss_collector_ata,
            currency_mint,
            energy_mint,
        )?;

        let start = std::time::Instant::now();
        let signers = vec![&authority];
        let res = self.build_and_send_transaction_with_priority(
            vec![instruction],
            &signers,
            "token_transaction",
        )
        .await;

        track_blockchain_operation("execute_settle_offchain_match", start.elapsed().as_millis() as f64, res.is_ok());
        res
    }

    /// Build the instruction for atomic settlement without executing it
    #[allow(clippy::too_many_arguments)]
    pub fn build_atomic_settlement_instruction(
        &self,
        market_pubkey: &Pubkey,
        buy_order_pubkey: &Pubkey,
        sell_order_pubkey: &Pubkey,
        buyer_currency_escrow: &Pubkey,
        seller_energy_escrow: &Pubkey,
        seller_currency_account: &Pubkey,
        buyer_energy_account: &Pubkey,
        fee_collector: &Pubkey,
        wheeling_collector: &Pubkey,
        loss_collector: &Pubkey,
        energy_mint: &Pubkey,
        currency_mint: &Pubkey,
        escrow_authority: &Pubkey,
        market_authority: &Pubkey,
        amount: u64,
        price: u64,
        wheeling_charge: u64,
        loss_cost: u64,
    ) -> Result<Instruction> {
        self.instruction_builder.build_execute_atomic_settlement_instruction(
            *market_pubkey,
            *buy_order_pubkey,
            *sell_order_pubkey,
            *buyer_currency_escrow,
            *seller_energy_escrow,
            *seller_currency_account,
            *buyer_energy_account,
            *fee_collector,
            *wheeling_collector,
            *loss_collector,
            *energy_mint,
            *currency_mint,
            *escrow_authority,
            *market_authority,
            amount,
            price,
            wheeling_charge,
            loss_cost,
        )
    }

    /// Execute on-chain atomic settlement (energy-for-currency swap)
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_atomic_settlement(
        &self,
        escrow_authority: &Keypair,
        market_authority: &Keypair,
        market_pubkey: &Pubkey,
        buy_order_pubkey: &Pubkey,
        sell_order_pubkey: &Pubkey,
        buyer_currency_escrow: &Pubkey,
        seller_energy_escrow: &Pubkey,
        seller_currency_account: &Pubkey,
        buyer_energy_account: &Pubkey,
        fee_collector: &Pubkey,
        wheeling_collector: &Pubkey,
        loss_collector: &Pubkey,
        energy_mint: &Pubkey,
        currency_mint: &Pubkey,
        amount: u64,
        price: u64,
        wheeling_charge: u64,
        loss_cost: u64,
    ) -> Result<Signature> {
        let instruction = self.build_atomic_settlement_instruction(
            market_pubkey,
            buy_order_pubkey,
            sell_order_pubkey,
            buyer_currency_escrow,
            seller_energy_escrow,
            seller_currency_account,
            buyer_energy_account,
            fee_collector,
            wheeling_collector,
            loss_collector,
            energy_mint,
            currency_mint,
            &escrow_authority.pubkey(),
            &market_authority.pubkey(),
            amount,
            price,
            wheeling_charge,
            loss_cost,
        )?;

        let signers: Vec<&Keypair> = if escrow_authority.pubkey() == market_authority.pubkey() {
            vec![escrow_authority]
        } else {
            vec![escrow_authority, market_authority]
        };

        let start = std::time::Instant::now();
        let res = self.build_and_send_transaction_with_priority(
            vec![instruction],
            &signers,
            "token_transaction",
        )
        .await;

        track_blockchain_operation("execute_atomic_settlement", start.elapsed().as_millis() as f64, res.is_ok());
        res
    }

    /// Execute multiple instructions in a single atomic transaction
    pub async fn execute_batched_instructions(
        &self,
        authority: &Keypair,
        instructions: Vec<Instruction>,
    ) -> Result<Signature> {
        let signers = vec![authority];
        let start = std::time::Instant::now();
        
        let res = self.build_and_send_transaction_with_priority(
            instructions,
            &signers,
            "batched_transaction",
        )
        .await;

        track_blockchain_operation("batched_transaction", start.elapsed().as_millis() as f64, res.is_ok());
        res
    }

    /// Execute on-chain create_order
    pub async fn execute_create_order(
        &self,
        authority: &Keypair,
        market_pubkey: &str,
        energy_amount: u64,
        price_per_kwh: u64,
        order_type: &str,
        erc_certificate_id: Option<&str>,
        zone_id: u32,
    ) -> Result<(Signature, String, u64)> {
        let market =
            Pubkey::from_str(market_pubkey).map_err(|e| anyhow!("Invalid market pubkey: {}", e))?;
 
        let start = std::time::Instant::now();
        let build_res = self
            .build_create_order_instruction(
                &market,
                authority.pubkey(),
                energy_amount,
                price_per_kwh,
                order_type,
                erc_certificate_id,
                zone_id,
            )
            .await;
 
        match build_res {
            Ok((instruction, order_pda, index)) => {
                let signers = vec![authority];
                let res = self.build_and_send_transaction_with_priority(
                    vec![instruction],
                    &signers,
                    "token_transaction",
                ).await;
                
                track_blockchain_operation("execute_create_order", start.elapsed().as_millis() as f64, res.is_ok());
                
                Ok((res?, order_pda.to_string(), index))
            },
            Err(e) => {
                track_blockchain_operation("execute_create_order_build", start.elapsed().as_millis() as f64, false);
                Err(e)
            }
        }
    }

    /// Update market depth on-chain
    pub async fn execute_update_depth(
        &self,
        market_pubkey: &Pubkey,
        zone_id: u32,
        buy_prices: Vec<u64>,
        buy_amounts: Vec<u64>,
        sell_prices: Vec<u64>,
        sell_amounts: Vec<u64>,
    ) -> Result<Signature> {
        let authority = self.get_authority_keypair().await?;
        let instruction = self.instruction_builder.build_update_depth_instruction(
            market_pubkey,
            zone_id,
            buy_prices,
            buy_amounts,
            sell_prices,
            sell_amounts,
        )?;

        let start = std::time::Instant::now();
        let signers = vec![&authority];
        let res = self.build_and_send_transaction_with_priority(
            vec![instruction],
            &signers,
            "token_transaction",
        ).await;

        track_blockchain_operation("execute_update_depth", start.elapsed().as_millis() as f64, res.is_ok());
        res
    }

    /// Update market price history on-chain
    pub async fn execute_update_price_history(
        &self,
        market_pubkey: &Pubkey,
        trade_price: u64,
        trade_volume: u64,
    ) -> Result<Signature> {
        let authority = self.get_authority_keypair().await?;
        let instruction = self.instruction_builder.build_update_price_history_instruction(
            market_pubkey,
            trade_price,
            trade_volume,
        )?;

        let start = std::time::Instant::now();
        let signers = vec![&authority];
        let res = self.build_and_send_transaction_with_priority(
            vec![instruction],
            &signers,
            "token_transaction",
        ).await;

        track_blockchain_operation("execute_update_price_history", start.elapsed().as_millis() as f64, res.is_ok());
        res
    }

    /// Build instruction for creating energy trade order
    /// Returns (Instruction, Order PDA)
    pub async fn build_create_order_instruction(
        &self,
        market_pubkey: &Pubkey,
        authority: Pubkey,
        energy_amount: u64,
        price_per_kwh: u64,
        order_type: &str,
        erc_certificate_id: Option<&str>,
        zone_id: u32,
    ) -> Result<(Instruction, Pubkey, u64)> {
        let market = *market_pubkey;
 
        // Derive zone_market PDA
        let (zone_market_pda, _) = Pubkey::find_program_address(
            &[b"zone_market", market.as_ref(), &zone_id.to_le_bytes()],
            &self.trading_program_id()?,
        );

        // Get active orders count from ZoneMarket (since Anchor increments zone_market.active_orders)
        let active_orders = self.get_zone_market_active_orders(&zone_market_pda).await.unwrap_or(0);

        // Derive order PDA using 8-byte u64 for index
        let (order_pda, _) = Pubkey::find_program_address(
            &[b"order", authority.as_ref(), &(active_orders as u64).to_le_bytes()],
            &self.trading_program_id()?,
        );
 
        let instruction = self.instruction_builder.build_create_order_instruction(
            market_pubkey,
            &authority,
            order_pda,
            active_orders as u64,
            energy_amount,
            price_per_kwh,
            order_type,
            erc_certificate_id,
            authority,
            zone_id,
        )?;
 
        Ok((instruction, order_pda, active_orders as u64))
    }

    /// Build instruction for matching orders
    pub fn build_match_orders_instruction(
        &self,
        market_pubkey: &str,
        buy_order_pubkey: &str,
        sell_order_pubkey: &str,
        match_amount: u64,
        trade_record_pubkey: Pubkey,
        zone_id: u32,
    ) -> Result<Instruction> {
        self.instruction_builder.build_match_orders_instruction(
            market_pubkey,
            buy_order_pubkey,
            sell_order_pubkey,
            match_amount,
            trade_record_pubkey,
            zone_id,
        )
    }

    /// Build instruction for minting tokens
    pub fn build_mint_instruction(&self, recipient: &str, amount: u64) -> Result<Instruction> {
        self.instruction_builder
            .build_mint_instruction(recipient, amount)
    }

    /// Build instruction for transferring tokens
    pub fn build_transfer_instruction(
        &self,
        from: &str,
        to: &str,
        amount: u64,
        token_mint: &str,
    ) -> Result<Instruction> {
        self.instruction_builder
            .build_transfer_instruction(from, to, amount, token_mint)
    }

    /// Build instruction for casting a governance vote
    pub fn build_vote_instruction(&self, proposal_id: u64, vote: bool) -> Result<Instruction> {
        self.instruction_builder
            .build_vote_instruction(proposal_id, vote)
    }

    /// Build instruction for updating oracle price
    pub fn build_update_price_instruction(
        &self,
        price_feed_id: &str,
        price: u64,
        confidence: u64,
    ) -> Result<Instruction> {
        self.instruction_builder
            .build_update_price_instruction(price_feed_id, price, confidence)
    }

    /// Build instruction for updating registry
    pub fn build_update_registry_instruction(
        &self,
        participant_id: &str,
        update_data: &serde_json::Value,
    ) -> Result<Instruction> {
        self.instruction_builder
            .build_update_registry_instruction(participant_id, update_data)
    }

    // ====================================================================
    // Transaction Building & Signing (Phase 4) - delegated to TransactionHandler
    // ====================================================================

    /// Priority 4: Build, sign, and send a transaction with automatic priority fees
    /// Returns transaction signature with enhanced performance monitoring
    // ====================================================================
    // Transaction Building & Signing (Phase 4) - delegated to OnChainManager
    // ====================================================================

    /// Priority 4: Build, sign, and send a transaction with automatic priority fees
    pub async fn build_and_send_transaction(
        &self,
        instructions: Vec<Instruction>,
        signers: &[&Keypair],
    ) -> Result<Signature> {
        self.on_chain_manager
            .build_and_send_transaction(instructions, signers)
            .await
    }

    /// Build, sign, and send a transaction with specified priority level
    pub async fn build_and_send_transaction_with_priority(
        &self,
        instructions: Vec<Instruction>,
        signers: &[&Keypair],
        transaction_type: &'static str,
    ) -> Result<Signature> {
        self.on_chain_manager
            .build_and_send_transaction_with_priority(instructions, signers, transaction_type)
            .await
    }

    /// Simulate a transaction before sending
    pub async fn simulate_transaction(&self, transaction: &Transaction) -> Result<bool> {
        self.transaction_handler
            .simulate_transaction(transaction)
            .await?;
        Ok(true)
    }

    /// Wait for transaction confirmation with timeout
    pub async fn wait_for_confirmation(
        &self,
        signature: &Signature,
        timeout_secs: u64,
    ) -> Result<bool> {
        self.transaction_handler
            .wait_for_confirmation(signature, timeout_secs)
            .await
    }

    /// Send transaction with retry logic
    pub async fn send_transaction_with_retry(
        &self,
        instructions: Vec<Instruction>,
        signers: &[&Keypair],
        max_retries: u32,
    ) -> Result<Signature> {
        self.transaction_handler
            .send_transaction_with_retry(instructions, signers, max_retries)
            .await
    }

    /// Build a transaction without sending
    pub async fn build_transaction(
        &self,
        instructions: Vec<Instruction>,
        payer: &Pubkey,
    ) -> Result<Transaction> {
        self.transaction_handler
            .build_transaction(instructions, payer)
            .await
    }

    // ====================================================================
    // Utility Methods - delegated to BlockchainUtils
    // ====================================================================

    /// Load keypair from a JSON file
    pub fn load_keypair_from_file(filepath: &str) -> Result<Keypair> {
        AccountManager::load_keypair_from_file(filepath)
    }

    /// Get authority keypair (for settlement service)
    pub async fn get_authority_keypair(&self) -> Result<Keypair> {
        self.account_manager.get_authority_keypair().await
    }

    /// Mint energy tokens directly to a user's token account
    /// Mint (or Burn) energy tokens based on reading amount
    /// Positive amount = Mint
    /// Negative amount = Burn
    pub async fn mint_energy_tokens(
        &self,
        authority: &Keypair,
        user_token_account: &Pubkey,
        user_wallet: &Pubkey,
        mint: &Pubkey,
        amount_kwh: Decimal,
    ) -> Result<Signature> {
        if amount_kwh > Decimal::ZERO {
            info!("Minting {} kWh tokens for wallet {}", amount_kwh, user_wallet);
            self.token_manager
                .mint_energy_tokens(authority, user_token_account, user_wallet, mint, amount_kwh)
                .await
        } else if amount_kwh < Decimal::ZERO {
            let burn_amount = amount_kwh.abs();
            info!("Burning {} kWh tokens from wallet {}", burn_amount, user_wallet);
            self.token_manager
                .burn_energy_tokens(authority, user_token_account, mint, burn_amount)
                .await
        } else {
            // Zero reading, no-op but return successful "signature" placeholder?
            // Or technically this shouldn't happen if validation works.
            // Let's just return a log and skip. 
            // We need to return a signature though.
            // Returning an error might fail the flow, but zero tokens is valid state.
            // We can return the last signature or a dummy one if we had one.
            // For now, let's treat it as a warning.
            Err(anyhow!("Cannot mint/burn zero tokens"))
        }
    }

    /// Mint SPL tokens using standard spl-token CLI (for testing with standard SPL tokens)
    pub async fn mint_spl_tokens(
        &self,
        authority: &Keypair,
        user_wallet: &Pubkey,
        mint: &Pubkey,
        amount_kwh: Decimal,
    ) -> Result<Signature> {
        info!("Minting {} SPL tokens for wallet {} using CLI", amount_kwh, user_wallet);
        self.token_manager
            .mint_spl_tokens(authority, user_wallet, mint, amount_kwh)
            .await
    }

    /// Burn energy tokens from a user's token account
    pub async fn burn_energy_tokens(
        &self,
        authority: &Keypair,
        user_token_account: &Pubkey,
        mint: &Pubkey,
        amount_kwh: Decimal,
    ) -> Result<Signature> {
        self.token_manager
            .burn_energy_tokens(authority, user_token_account, mint, amount_kwh)
            .await
    }

    /// Transfer energy tokens between accounts
    pub async fn transfer_energy_tokens(
        &self,
        authority: &Keypair,
        from_token_account: &Pubkey,
        to_token_account: &Pubkey,
        mint: &Pubkey,
        amount_kwh: Decimal,
    ) -> Result<Signature> {
        self.token_manager
            .transfer_energy_tokens(
                authority,
                from_token_account,
                to_token_account,
                mint,
                amount_kwh,
            )
            .await
    }

    /// Ensures user has an Associated Token Account for the token mint
    pub async fn ensure_token_account_exists(
        &self,
        authority: &Keypair,
        user_wallet: &Pubkey,
        mint: &Pubkey,
    ) -> Result<Pubkey> {
        self.token_manager
            .ensure_token_account_exists(authority, user_wallet, mint)
            .await
    }

    /// Calculate the Associated Token Account address for a user and mint
    pub fn calculate_ata_address(&self, user_wallet: &Pubkey, mint: &Pubkey) -> Result<Pubkey> {
        self.account_manager
            .calculate_ata_address(user_wallet, mint)
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
        self.token_manager
            .transfer_tokens(
                authority,
                from_token_account,
                to_token_account,
                mint,
                amount,
                decimals,
            )
            .await
    }

    /// Register a user on-chain
    pub async fn register_user_on_chain(
        &self,
        authority: &Keypair,
        user_type: u8,
        lat_e7: i32,
        long_e7: i32,
        h3_index: u64,
        // Optional for airdrop
        energy_token_program: Option<Pubkey>,
        mint: Option<Pubkey>,
    ) -> Result<Signature> {
        let registry = self.registry_program_id()?;
        let (registry_pda, _) = Pubkey::find_program_address(&[b"registry"], &registry);

        let register_instruction = self.instruction_builder.build_register_user_instruction(
            &authority.pubkey(),
            &registry_pda,
            user_type,
            lat_e7,
            long_e7,
            h3_index,
            energy_token_program,
            mint,
        )?;

        self.build_and_send_transaction(vec![register_instruction], &[authority])
            .await
    }

    /// Register a meter on-chain
    pub async fn register_meter_on_chain(
        &self,
        authority: &Keypair,
        meter_id: &str,
        meter_type: u8,
    ) -> Result<Signature> {
        let registry_id = self.registry_program_id()?;
        let (registry_pda, _) = Pubkey::find_program_address(&[b"registry"], &registry_id);

        let register_instruction = self.instruction_builder.build_register_meter_instruction(
            &authority.pubkey(),
            &registry_pda,
            meter_id,
            meter_type,
        )?;

        self.build_and_send_transaction(vec![register_instruction], &[authority])
            .await
    }

    /// Submit meter reading on-chain (via Oracle)
    pub async fn submit_meter_reading_on_chain(
        &self,
        authority: &Keypair,
        owner: &Pubkey,
        meter_id: &str,
        produced: u64,
        consumed: u64,
        timestamp: i64,
    ) -> Result<Signature> {
        let submit_instruction = BlockchainUtils::create_submit_meter_reading_instruction(
            authority, owner, meter_id, produced, consumed, timestamp,
        )?;

        self.build_and_send_transaction(vec![submit_instruction], &[authority])
            .await
    }

    /// Update meter reading on-chain via Registry program
    /// The oracle_authority must be the configured oracle on the Registry program
    /// Call `set_oracle_authority` on Registry first to authorize the oracle
    pub async fn update_meter_reading_on_chain(
        &self,
        oracle_authority: &Keypair,
        owner: &Pubkey,
        meter_id: &str,
        energy_generated_wh: u64,
        energy_consumed_wh: u64,
        reading_timestamp: i64,
    ) -> Result<Signature> {
        info!(
            "Updating meter {} on-chain: gen={} Wh, cons={} Wh",
            meter_id, energy_generated_wh, energy_consumed_wh
        );

        let update_instruction = BlockchainUtils::create_update_meter_reading_instruction(
            oracle_authority,
            owner,
            meter_id,
            energy_generated_wh,
            energy_consumed_wh,
            reading_timestamp,
        )?;

        self.build_and_send_transaction(vec![update_instruction], &[oracle_authority])
            .await
    }

    /// Derive escrow PDA
    pub fn derive_escrow_pda(order_id: &[u8; 32], program_id: &Pubkey) -> (Pubkey, u8) {
        TransactionHandler::derive_escrow_pda(order_id, program_id)
    }

    /// Lock tokens to escrow
    pub async fn lock_tokens_to_escrow(
        &self,
        buyer_authority: &Keypair,
        buyer_ata: &Pubkey,
        escrow_ata: &Pubkey,
        token_mint: &Pubkey,
        amount: u64,
        decimals: u8,
    ) -> Result<Signature> {
        self.transaction_handler
            .lock_tokens_to_escrow(buyer_authority, buyer_ata, escrow_ata, token_mint, amount, decimals)
            .await
    }

    /// Release escrow to seller
    pub async fn release_escrow_to_seller(
        &self,
        escrow_authority: &Keypair,
        escrow_ata: &Pubkey,
        seller_ata: &Pubkey,
        token_mint: &Pubkey,
        amount: u64,
        decimals: u8,
    ) -> Result<Signature> {
        self.transaction_handler
            .release_escrow_to_seller(escrow_authority, escrow_ata, seller_ata, token_mint, amount, decimals)
            .await
    }

    /// Refund escrow to buyer
    pub async fn refund_escrow_to_buyer(
        &self,
        escrow_authority: &Keypair,
        escrow_ata: &Pubkey,
        buyer_ata: &Pubkey,
        token_mint: &Pubkey,
        amount: u64,
        decimals: u8,
    ) -> Result<Signature> {
        self.transaction_handler
            .refund_escrow_to_buyer(escrow_authority, escrow_ata, buyer_ata, token_mint, amount, decimals)
            .await
    }

    /// Mint tokens directly to a user's wallet using the Anchor energy_token program
    pub async fn mint_tokens_direct(&self, user_wallet: &Pubkey, amount: u64) -> Result<Signature> {
        info!(
            "mint_tokens_direct called for wallet: {}, amount: {}",
            user_wallet, amount
        );

        // Get authority keypair
        let authority = self.account_manager.get_authority_keypair().await?;

        // Get configured mint
        let mint_str = std::env::var("ENERGY_TOKEN_MINT")
            .unwrap_or_else(|_| "2XLTgMue7MHSjZ7A25zmV9xF6ZeBz2LouZt6Y92AtN2H".to_string());
        let mint = Pubkey::from_str(&mint_str)
            .map_err(|e| anyhow!("Invalid ENERGY_TOKEN_MINT: {}", e))?;

        // Convert atomic amount to UI amount (assuming 9 decimals)
        let amount_kwh = Decimal::from(amount) / Decimal::from(1_000_000_000);
        
        // Ensure ATA exists explicitly
        let user_token_account = self.ensure_token_account_exists(&authority, user_wallet, &mint).await?;

        // Use mint_energy_tokens which properly calls the Anchor program via CPI
        // (mint authority is the program's PDA, not the wallet)
        self.mint_energy_tokens(&authority, &user_token_account, user_wallet, &mint, amount_kwh).await
    }

    pub async fn validate_erc_on_chain(
        &self,
        certificate_id: &str,
        authority: &Keypair,
    ) -> Result<Signature> {
        info!("Validating ERC {} on-chain for trading", certificate_id);
        let instruction = self.instruction_builder.build_validate_erc_instruction(
            certificate_id,
        )?;
        self.build_and_send_transaction(vec![instruction], &[authority]).await
    }

    /// Update governance configuration on-chain
    pub async fn update_governance_config(
        &self,
        erc_validation_enabled: bool,
        allow_certificate_transfers: bool,
        authority: &Keypair,
    ) -> Result<Signature> {
        info!("Updating governance config on-chain: validation={}, transfers={}", 
            erc_validation_enabled, allow_certificate_transfers);
        let instruction = self.instruction_builder.build_update_governance_config_instruction(
            erc_validation_enabled,
            allow_certificate_transfers,
        )?;
        self.build_and_send_transaction(vec![instruction], &[authority]).await
    }

    /// Check if an ERC certificate is already validated for trading
    pub async fn is_erc_validated(&self, certificate_id: &str) -> Result<bool> {
        let pda = self.instruction_builder.get_erc_certificate_pubkey(certificate_id)?;
        
        // Use result from get_account_data
        let data: Vec<u8> = self.on_chain_manager.get_account_data(&pda).await?;
        
        // Offset 486 is validated_for_trading bool in ErcCertificate
        // Account structure: Discriminator(8) + cert_id(64) + id_len(1) + 
        // authority(32) + owner(32) + amount(8) + source(64) + source_len(1) +
        // validation_data(256) + data_len(2) + issued_at(8) + expires_at(9) + status(1) + validated(1)
        // 8+64+1+32+32+8+64+1+256+2+8+9+1 = 486
        
        if data.len() <= 486 {
            debug!("Account data too short for ERC certificate or account empty: {} bytes", data.len());
            return Ok(false);
        }
        
        Ok(data[486] != 0)
    }

    pub fn transaction_handler(&self) -> &TransactionHandler {
        self.on_chain_manager.transaction_handler()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::SolanaProgramsConfig;

    fn test_config() -> SolanaProgramsConfig {
        SolanaProgramsConfig {
            registry_program_id: "2XPQmFYMdXjP7ffoBB3mXeCdboSFg5Yeb6QmTSGbW8a7".to_string(),
            oracle_program_id: "DvdtU4quEbuxUY2FckmvcXwTpC9qp4HLJKb1PMLaqAoE".to_string(),
            governance_program_id: "4DY97YYBt4bxvG7xaSmWy3MhYhmA6HoMajBHVqhySvXe".to_string(),
            energy_token_program_id: "94G1r674LmRDmLN2UPjDFD8Eh7zT8JaSaxv9v68GyEur".to_string(),
            trading_program_id: "9t3s8sCgVUG9kAgVPsozj8mDpJp9cy6SF5HwRK5nvAHb".to_string(),
        }
    }

    #[test]
    fn test_parse_program_ids() {
        let service = BlockchainService::new(
            "http://localhost:8899".to_string(),
            "localnet".to_string(),
            test_config(),
        )
        .unwrap();
        assert!(service.registry_program_id().is_ok());
        assert!(service.oracle_program_id().is_ok());
        assert!(service.governance_program_id().is_ok());
        assert!(service.energy_token_program_id().is_ok());
        assert!(service.trading_program_id().is_ok());
    }

    #[test]
    fn test_parse_invalid_pubkey() {
        assert!(BlockchainService::parse_pubkey("invalid").is_err());
    }
}
