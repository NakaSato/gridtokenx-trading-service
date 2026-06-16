use anyhow::Result;
use gridtokenx_blockchain_core::rpc::{instructions::OffchainOrderPayload, BlockchainUtils};
use solana_sdk::{
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    transaction::Transaction,
};
use std::sync::Arc;
use tracing::info;

/// Blockchain service for interacting with Solana programs via the Chain Bridge
#[derive(Clone)]
pub struct BlockchainService {
    pub core: Arc<gridtokenx_blockchain_core::BlockchainService>,
    pub db: Option<sqlx::PgPool>,
    pub identity_gateway: Option<Arc<dyn trading_core::traits::IdentityGateway>>,
    /// When false, the oracle/surplus generation-mint path (path B) is disabled
    /// so trading no longer mints metered generation (issuance owned elsewhere).
    pub oracle_mint_enabled: bool,
    /// When false, on-chain atomic-swap settlement of matching-engine trades is
    /// disabled (trade settlements are left for retry, never sent on-chain).
    pub trade_settlement_enabled: bool,
}

impl std::fmt::Debug for BlockchainService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockchainService").finish()
    }
}

impl BlockchainService {
    /// Create a new blockchain service wrapping the core gRPC client
    pub async fn new(
        chain_bridge_url: String,
        cluster: String,
        program_ids: trading_core::config::SolanaProgramsConfig,
        db: Option<sqlx::PgPool>,
        _metrics_registry: Option<
            Arc<dyn gridtokenx_blockchain_core::rpc::metrics::BlockchainMetrics>,
        >, // Kept for compatibility
    ) -> Result<Self> {
        info!(
            "Initializing gRPC Blockchain Service via: {}",
            chain_bridge_url
        );

        // Convert trading-service config to core config
        let core_config = gridtokenx_blockchain_core::config::SolanaProgramsConfig {
            registry_program_id: program_ids.registry_program_id,
            oracle_program_id: program_ids.oracle_program_id,
            governance_program_id: program_ids.governance_program_id,
            energy_token_program_id: program_ids.energy_token_program_id,
            trading_program_id: program_ids.trading_program_id,
            trading_market_id: program_ids.trading_market_id,
        };

        let metrics = Arc::new(gridtokenx_blockchain_core::rpc::NoopMetrics);
        let core = gridtokenx_blockchain_core::BlockchainService::new(
            chain_bridge_url,
            cluster,
            core_config,
            metrics,
        )
        .await?;

        Ok(Self {
            core: Arc::new(core),
            db,
            identity_gateway: None,
            oracle_mint_enabled: true,
            trade_settlement_enabled: false,
        })
    }

    /// Set the identity gateway for custodial signing
    pub fn with_identity_gateway(mut self, gateway: Arc<dyn trading_core::traits::IdentityGateway>) -> Self {
        self.identity_gateway = Some(gateway);
        self
    }

    /// Enable/disable trading's oracle generation-mint path (path B).
    pub fn with_oracle_mint_enabled(mut self, enabled: bool) -> Self {
        self.oracle_mint_enabled = enabled;
        self
    }

    /// Enable/disable on-chain atomic-swap settlement of matching-engine trades.
    pub fn with_trade_settlement_enabled(mut self, enabled: bool) -> Self {
        self.trade_settlement_enabled = enabled;
        self
    }

    /// Resolve a trading order's on-chain PDA from its database id. Returns
    /// `None` when the order does not exist or has no on-chain PDA recorded
    /// (e.g. an order that never reached the chain).
    pub async fn get_order_pda(&self, order_id: &uuid::Uuid) -> Result<Option<Pubkey>> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Database pool not available in BlockchainService"))?;

        let pda: Option<String> =
            sqlx::query_scalar::<_, Option<String>>("SELECT order_pda FROM trading_orders WHERE id = $1")
                .bind(order_id)
                .fetch_optional(db)
                .await?
                .flatten();

        match pda {
            Some(p) => Ok(Some(Self::parse_pubkey(&p)?)),
            None => Ok(None),
        }
    }

    /// Load the authority keypair from the environment/config
    pub async fn get_authority_keypair(&self) -> Result<Keypair> {
        let wallet_path = std::env::var("AUTHORITY_WALLET_PATH")
            .unwrap_or_else(|_| "dev-wallet.json".to_string());
        BlockchainUtils::load_keypair_from_file(&wallet_path)
    }

    /// Submit transaction to blockchain
    pub async fn submit_transaction(&self, transaction: Transaction) -> Result<Signature> {
        self.core.submit_transaction(transaction).await
    }

    /// Confirm transaction status
    pub async fn confirm_transaction(&self, signature: &str) -> Result<bool> {
        self.core
            .transaction_handler
            .confirm_transaction(signature)
            .await
    }

    /// Wait for transaction confirmation
    pub async fn wait_for_confirmation(
        &self,
        signature: &Signature,
        _timeout_secs: u64,
    ) -> Result<bool> {
        self.core
            .transaction_handler
            .confirm_transaction(&signature.to_string())
            .await
    }

    /// Check if an account exists
    pub async fn account_exists(&self, pubkey: &Pubkey) -> Result<bool> {
        self.core.account_manager.account_exists(pubkey).await
    }

    /// Check if the service is healthy
    pub async fn health_check(&self) -> Result<bool> {
        self.core.health_check().await
    }

    pub fn trading_program_id(&self) -> Result<Pubkey> {
        use std::str::FromStr;
        Pubkey::from_str(&self.core.program_ids.trading_program_id).map_err(Into::into)
    }

    pub fn registry_program_id(&self) -> Result<Pubkey> {
        use std::str::FromStr;
        Pubkey::from_str(&self.core.program_ids.registry_program_id).map_err(Into::into)
    }

    pub fn oracle_program_id(&self) -> Result<Pubkey> {
        use std::str::FromStr;
        Pubkey::from_str(&self.core.program_ids.oracle_program_id).map_err(Into::into)
    }

    pub fn governance_program_id(&self) -> Result<Pubkey> {
        use std::str::FromStr;
        Pubkey::from_str(&self.core.program_ids.governance_program_id).map_err(Into::into)
    }

    /// Request airdrop (devnet/localnet only)
    pub async fn request_airdrop(&self, pubkey: &Pubkey, lamports: u64) -> Result<Signature> {
        self.core
            .request_airdrop(pubkey, lamports)
            .await
            .map(|_| Signature::default())
    }

    /// Get account balance in lamports
    pub async fn get_balance(&self, pubkey: &Pubkey, force_refresh: bool) -> Result<u64> {
        self.core
            .account_manager
            .get_balance(pubkey, force_refresh)
            .await
    }

    /// Get account balance in SOL
    pub async fn get_balance_sol(&self, pubkey: &Pubkey, force_refresh: bool) -> Result<f64> {
        let balance = self
            .core
            .account_manager
            .get_balance(pubkey, force_refresh)
            .await?;
        Ok(balance as f64 / 1_000_000_000.0)
    }

    /// Get SPL token balance for a user
    pub async fn get_token_balance(&self, owner: &Pubkey, mint: &Pubkey) -> Result<u64> {
        self.core.token_manager.get_token_balance(owner, mint).await
    }

    /// Send and confirm a transaction
    pub async fn send_and_confirm_transaction(
        &self,
        transaction: &Transaction,
    ) -> Result<Signature> {
        self.core
            .transaction_handler
            .send_and_confirm_transaction(transaction)
            .await
    }

    /// Get transaction status
    pub async fn get_signature_status(&self, signature: &Signature) -> Result<Option<bool>> {
        self.core
            .transaction_handler
            .get_signature_status(signature)
            .await
    }

    /// Get recent blockhash
    pub async fn get_latest_blockhash(&self) -> Result<solana_sdk::hash::Hash> {
        self.core.transaction_handler.get_latest_blockhash().await
    }

    /// Get slot height
    pub async fn get_slot(&self) -> Result<u64> {
        self.core.transaction_handler.get_slot().await
    }

    /// Get account data
    pub async fn get_account_data(&self, pubkey: &Pubkey) -> Result<Vec<u8>> {
        self.core.account_manager.get_account_data(pubkey).await
    }

    /// Calculate ATA address
    pub fn calculate_ata_address(&self, owner: &Pubkey, mint: &Pubkey) -> Result<Pubkey> {
        self.core.account_manager.calculate_ata_address(owner, mint)
    }

    /// Ensure token account exists
    pub async fn ensure_token_account_exists(
        &self,
        authority: &(dyn Signer + Send + Sync),
        user_wallet: &Pubkey,
        mint: &Pubkey,
    ) -> Result<Pubkey> {
        self.core
            .token_manager
            .ensure_token_account_exists_with_signer(authority, user_wallet, mint)
            .await
    }

    /// Parse Pubkey from string
    pub fn parse_pubkey(pubkey_str: &str) -> Result<Pubkey> {
        use std::str::FromStr;
        Pubkey::from_str(pubkey_str).map_err(|e| anyhow::anyhow!("Invalid pubkey: {}", e))
    }

    /// Get the instruction builder
    pub fn instruction_builder(
        &self,
    ) -> &gridtokenx_blockchain_core::rpc::instructions::InstructionBuilder {
        &self.core.instruction_builder
    }

    /// Derive order PDA
    pub fn derive_order_pda(&self, authority: &Pubkey, index: u64) -> Result<Pubkey> {
        let program_id = std::str::FromStr::from_str(&self.core.program_ids.trading_program_id)?;
        let (pda, _) = Pubkey::find_program_address(
            &[b"order", authority.as_ref(), &index.to_le_bytes()],
            &program_id,
        );
        Ok(pda)
    }

    /// Execute on-chain create_order
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_create_order(
        &self,
        authority: &(dyn Signer + Send + Sync),
        market_pubkey: &str,
        amount: u64,
        price: u64,
        order_type: &str,
        erc_id: Option<&str>,
        zone_id: u32,
    ) -> Result<(Signature, String, u64)> {
        use std::str::FromStr;
        let market = Pubkey::from_str(market_pubkey)?;

        // Get next index (simplified for thin wrapper, real logic should be more robust)
        let index = 0; // In production this would fetch from account state
        let order_pda = self.derive_order_pda(&authority.pubkey(), index)?;

        let instruction = self
            .core
            .instruction_builder
            .build_create_order_instruction(
                &market,
                &authority.pubkey(),
                order_pda,
                index,
                amount,
                price,
                order_type,
                erc_id,
                authority.pubkey(),
                zone_id,
            )?;

        let sig = self
            .core
            .on_chain_manager
            .build_and_send_transaction_with_signers(vec![instruction], &[authority as &(dyn Signer + Send + Sync)])
            .await?;
        Ok((sig, order_pda.to_string(), index))
    }

    /// Execute on-chain match_orders
    pub async fn execute_match_orders(
        &self,
        authority: &(dyn Signer + Send + Sync),
        market_pubkey: &str,
        buy_order_pubkey: &str,
        sell_order_pubkey: &str,
        match_amount: u64,
        zone_id: u32,
    ) -> Result<Signature> {
        let instruction = self
            .core
            .instruction_builder
            .build_match_orders_instruction(
                market_pubkey,
                buy_order_pubkey,
                sell_order_pubkey,
                match_amount,
                Pubkey::default(), // trade_record_pda handled in core if needed
                zone_id,
            )?;

        self.core
            .on_chain_manager
            .build_and_send_transaction_with_signers(vec![instruction], &[authority as &(dyn Signer + Send + Sync)])
            .await
    }

    /// Lock tokens to escrow (transfer)
    pub async fn lock_tokens_to_escrow(
        &self,
        user: &(dyn Signer + Send + Sync),
        from_ata: &Pubkey,
        to_ata: &Pubkey,
        mint: &Pubkey,
        amount: u64,
        decimals: u8,
    ) -> Result<Signature> {
        self.core
            .token_manager
            .transfer_tokens_with_signer(user, from_ata, to_ata, mint, amount, decimals)
            .await
    }

    /// Release escrow to seller
    pub async fn release_escrow_to_seller(
        &self,
        authority: &(dyn Signer + Send + Sync),
        escrow_ata: &Pubkey,
        receiver_ata: &Pubkey,
        mint: &Pubkey,
        amount: u64,
        decimals: u8,
    ) -> Result<Signature> {
        self.core
            .token_manager
            .transfer_tokens_with_signer(authority, escrow_ata, receiver_ata, mint, amount, decimals)
            .await
    }

    /// Refund escrow to buyer
    pub async fn refund_escrow_to_buyer(
        &self,
        authority: &(dyn Signer + Send + Sync),
        escrow_ata: &Pubkey,
        user_ata: &Pubkey,
        mint: &Pubkey,
        amount: u64,
        decimals: u8,
    ) -> Result<Signature> {
        self.core
            .token_manager
            .transfer_tokens_with_signer(authority, escrow_ata, user_ata, mint, amount, decimals)
            .await
    }

    /// Settle off-chain match
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_settle_offchain_match(
        &self,
        market_pubkey: &Pubkey,
        buyer_payload: &OffchainOrderPayload,
        seller_payload: &OffchainOrderPayload,
        match_amount: u64,
        match_price: u64,
        wheeling_charge: u64,
        loss_cost: u64,
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

        let instruction = self
            .core
            .instruction_builder
            .build_settle_offchain_match_instruction(
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

        self.core
            .on_chain_manager
            .build_and_send_transaction(vec![instruction], &[&authority])
            .await
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
        authority: &(dyn Signer + Send + Sync),
    ) -> Result<Signature> {
        self.core
            .issue_erc_with_signer(
                certificate_id,
                user_wallet,
                meter_account,
                energy_amount,
                renewable_source,
                validation_data,
                authority,
            )
            .await
    }

    /// Transfer an ERC certificate on-chain
    pub async fn transfer_erc(
        &self,
        certificate_id: &str,
        owner: &(dyn Signer + Send + Sync),
        new_owner: &Pubkey,
    ) -> Result<Signature> {
        self.core
            .transfer_erc_with_signer(certificate_id, owner, new_owner)
            .await
    }

    /// Revoke (retire) an ERC certificate on-chain
    pub async fn revoke_erc(
        &self,
        certificate_id: &str,
        reason: &str,
        authority: &(dyn Signer + Send + Sync),
    ) -> Result<Signature> {
        self.core
            .revoke_erc_with_signer(certificate_id, reason, authority)
            .await
    }

    /// Add priority fee to instructions
    pub async fn add_priority_fee_to_instructions(
        &self,
        instructions: &mut Vec<Instruction>,
        tx_type: &'static str,
    ) -> Result<()> {
        self.core
            .add_priority_fee_to_instructions(instructions, tx_type)
            .await
    }

    pub async fn build_and_send_transaction(
        &self,
        instructions: Vec<Instruction>,
        signers: &[&Keypair],
    ) -> Result<Signature> {
        self.core
            .on_chain_manager
            .build_and_send_transaction(instructions, signers)
            .await
    }

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
        self.core
            .execute_atomic_settlement(
                escrow_authority,
                market_authority,
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
                amount,
                price,
                wheeling_charge,
                loss_cost,
                None, // Default priority
            )
            .await
    }

    pub async fn transfer_tokens(
        &self,
        user: &(dyn Signer + Send + Sync),
        from_ata: &Pubkey,
        to_ata: &Pubkey,
        mint: &Pubkey,
        amount: u64,
        decimals: u8,
    ) -> Result<Signature> {
        self.core
            .token_manager
            .transfer_tokens_with_signer(user, from_ata, to_ata, mint, amount, decimals)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build_atomic_settlement_instruction(
        &self,
        market: Pubkey,
        buy_order: Pubkey,
        sell_order: Pubkey,
        buyer_currency_escrow: Pubkey,
        seller_energy_escrow: Pubkey,
        seller_currency_account: Pubkey,
        buyer_energy_account: Pubkey,
        fee_collector: Pubkey,
        wheeling_collector: Pubkey,
        loss_collector: Pubkey,
        energy_mint: Pubkey,
        currency_mint: Pubkey,
        escrow_authority: Pubkey,
        market_authority: Pubkey,
        amount: u64,
        price: u64,
        wheeling_charge: u64,
        loss_cost: u64,
    ) -> Result<Instruction> {
        self.core
            .instruction_builder
            .build_execute_atomic_settlement_instruction(
                market,
                buy_order,
                sell_order,
                buyer_currency_escrow,
                seller_energy_escrow,
                seller_currency_account,
                buyer_energy_account,
                fee_collector,
                wheeling_collector,
                loss_collector,
                energy_mint,
                currency_mint,
                escrow_authority,
                market_authority,
                amount,
                price,
                wheeling_charge,
                loss_cost,
            )
    }

    pub async fn execute_batched_instructions(
        &self,
        signers: &[&Keypair],
        instructions: Vec<Instruction>,
    ) -> Result<Signature> {
        self.core
            .on_chain_manager
            .build_and_send_transaction(instructions, signers)
            .await
    }

    /// Update price history on-chain
    pub async fn execute_update_price_history(
        &self,
        market_pubkey: &Pubkey,
        price: u64,
        amount: u64,
    ) -> Result<Signature> {
        let instruction = self
            .core
            .instruction_builder
            .build_submit_meter_reading_instruction(
                &market_pubkey.to_string(),
                price,
                amount,
                gridtokenx_telemetry::time::now().timestamp(),
                0,
            )?;

        let authority = self.get_authority_keypair().await?;
        self.build_and_send_transaction(vec![instruction], &[&authority])
            .await
    }

    /// Update market depth on-chain (batched)
    pub async fn execute_update_depth(
        &self,
        market_pubkey: &Pubkey,
        _zone_id: u32,
        buy_prices: Vec<u64>,
        buy_amounts: Vec<u64>,
        sell_prices: Vec<u64>,
        sell_amounts: Vec<u64>,
    ) -> Result<Signature> {
        let mut instructions = Vec::new();

        // Add bid levels
        for (price, amount) in buy_prices.into_iter().zip(buy_amounts.into_iter()) {
            instructions.push(
                self.core
                    .instruction_builder
                    .build_submit_meter_reading_instruction(
                        &market_pubkey.to_string(),
                        price,
                        amount,
                        0,
                        0, // Side 0 for Buy
                    )?,
            );
        }

        // Add ask levels
        for (price, amount) in sell_prices.into_iter().zip(sell_amounts.into_iter()) {
            instructions.push(
                self.core
                    .instruction_builder
                    .build_submit_meter_reading_instruction(
                        &market_pubkey.to_string(),
                        price,
                        amount,
                        0,
                        1, // Side 1 for Sell
                    )?,
            );
        }

        if instructions.is_empty() {
            return Ok(Signature::default());
        }

        let authority = self.get_authority_keypair().await?;
        self.build_and_send_transaction(instructions, &[&authority])
            .await
    }

    /// Register user on-chain
    pub async fn register_user_on_chain(
        &self,
        authority: &Keypair,
        user_type: u8,
        lat_e7: i32,
        long_e7: i32,
        h3_index: u64,
        shard_id: u8,
    ) -> Result<Signature> {
        let user_type_enum = match user_type {
            0 => gridtokenx_blockchain_core::rpc::instructions::UserType::Prosumer,
            _ => gridtokenx_blockchain_core::rpc::instructions::UserType::Consumer,
        };

        self.core
            .register_user_on_chain(
                authority.pubkey(),
                user_type_enum,
                lat_e7,
                long_e7,
                h3_index,
                shard_id,
            )
            .await
    }

    /// SHIM: Build and send transaction with priority (compatibility with legacy calls)
    pub async fn build_and_send_transaction_with_priority(
        &self,
        mut instructions: Vec<Instruction>,
        signers: &[&Keypair],
        priority_label: &'static str,
    ) -> Result<Signature> {
        self.add_priority_fee_to_instructions(&mut instructions, priority_label)
            .await?;
        self.build_and_send_transaction(instructions, signers).await
    }

    /// Check if ERC is validated on-chain (Registry Program)
    pub async fn is_erc_validated(&self, certificate_id: &str) -> Result<bool> {
        // Placeholder check via account manager
        let program_id = self.oracle_program_id()?;
        let (pda, _) = Pubkey::find_program_address(
            &[b"erc_verification", certificate_id.as_bytes()],
            &program_id,
        );
        self.account_exists(&pda).await
    }

    /// Validate ERC on-chain
    pub async fn validate_erc_on_chain(
        &self,
        certificate_id: &str,
        authority: &Keypair,
    ) -> Result<Signature> {
        let instruction = self
            .core
            .instruction_builder
            .build_submit_meter_reading_instruction(certificate_id, 0, 0, 0, 0)?; // Placeholder instruction

        self.build_and_send_transaction(vec![instruction], &[authority])
            .await
    }

    /// Internal: Resolve primary wallet for a user
    pub async fn get_user_primary_wallet(&self, user_id: &uuid::Uuid) -> Result<Option<Pubkey>> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Database pool not available in BlockchainService"))?;

        let wallet_address: Option<String> = sqlx::query_scalar(
            "SELECT wallet_address FROM user_wallets WHERE user_id = $1 AND is_primary = true",
        )
        .bind(user_id)
        .fetch_optional(db)
        .await?;

        match wallet_address {
            Some(addr) => Ok(Some(Self::parse_pubkey(&addr)?)),
            None => Ok(None),
        }
    }

    /// Get a custodial signer for a user
    pub async fn get_custodial_signer(&self, user_id: uuid::Uuid) -> Result<crate::identity::CustodialSigner> {
        let gateway = self.identity_gateway.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Identity gateway not configured for custodial signing"))?;
        
        let wallet = self.get_user_primary_wallet(&user_id).await?
            .ok_or_else(|| anyhow::anyhow!("User has no primary wallet for signing"))?;
            
        Ok(crate::identity::CustodialSigner::new(user_id, wallet, gateway.clone()))
    }
}
