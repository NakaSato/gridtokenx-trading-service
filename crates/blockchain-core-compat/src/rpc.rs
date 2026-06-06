pub mod account;
pub mod instructions;
pub mod metrics;
pub mod nats_provider;
pub mod nats_schema;
pub mod on_chain;
pub mod priority_fee;
pub mod token;
pub mod transaction;
pub mod utils;

use anyhow::Result;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Signature, Signer};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

// Dev fallback fee-payer. MUST match the key Chain Bridge signs with when
// CHAIN_BRIDGE_INSECURE=true (its InsecureKeypairProvider dev keypair, and the
// Registry authority hardcoded at chain-bridge api.rs). On-chain register_user
// makes the payer the sole transaction signer, so a drift here yields
// "Transaction did not pass signature verification". Override per-env with
// SOLANA_PAYER_KEY (the real Vault platform_admin pubkey in prod).
const DEFAULT_PAYER: Pubkey =
    Pubkey::from_str_const("EzudwoHvNPAc4dpPi5ndU8MEZVHVzq3Pj3Thm9ooKmiJ");

/// Deterministic 16-way shard for a key — must mirror the Registry program's
/// `shard_for` (`key.to_bytes()[0] % 16`) exactly, or register_user/register_meter
/// fail with InvalidShardId (0x177c). See superproject CLAUDE.md invariant #3.
fn shard_for(key: &Pubkey) -> u8 {
    key.to_bytes()[0] % 16
}

pub mod chain_v1 {
    tonic::include_proto!("gridtokenx.chain.v1");
}

pub use chain_v1::chain_bridge_service_client::ChainBridgeServiceClient;

pub use account::AccountManager;
pub use instructions::InstructionBuilder;
pub use metrics::{BlockchainMetrics, NoopMetrics};
pub use on_chain::OnChainManager;
pub use priority_fee::{PriorityFeeService, PriorityLevel, TransactionType};
pub use token::TokenManager;
pub use transaction::{SignatureState, TransactionHandler};
pub use utils::BlockchainUtils;

use crate::config::SolanaProgramsConfig;

/// Unified Blockchain Service for GridTokenX Platform
#[derive(Clone)]
pub struct BlockchainService {
    pub transaction_handler: TransactionHandler,
    pub instruction_builder: InstructionBuilder,
    pub account_manager: AccountManager,
    pub token_manager: TokenManager,
    pub on_chain_manager: OnChainManager,
    pub wallet_service: crate::wallet::WalletService,
    pub cluster: String,
    pub program_ids: SolanaProgramsConfig,
}

impl std::fmt::Debug for BlockchainService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockchainService").finish()
    }
}

impl BlockchainService {
    pub async fn new(
        bridge_url: String,
        cluster: String,
        program_ids: SolanaProgramsConfig,
        metrics: Arc<dyn BlockchainMetrics>,
    ) -> Result<Self> {
        let insecure_mode = std::env::var("CHAIN_BRIDGE_INSECURE")
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(false);

        let channel = if insecure_mode && bridge_url.starts_with("http://") {
            warn!("⚠️ Connecting to Chain Bridge in INSECURE mode (no TLS)");
            tonic::transport::Channel::from_shared(bridge_url)?.connect_lazy()
        } else {
            let ca_pem = tokio::fs::read(
                std::env::var("CHAIN_BRIDGE_CA_CERT")
                    .unwrap_or_else(|_| "infra/certs/ca.crt".to_string()),
            )
            .await?;
            let cert_pem = tokio::fs::read(
                std::env::var("CHAIN_BRIDGE_CLIENT_CERT")
                    .unwrap_or_else(|_| "infra/certs/client.crt".to_string()),
            )
            .await?;
            let key_pem = tokio::fs::read(
                std::env::var("CHAIN_BRIDGE_CLIENT_KEY")
                    .unwrap_or_else(|_| "infra/certs/client.key".to_string()),
            )
            .await?;

            let tls = tonic::transport::ClientTlsConfig::new()
                .ca_certificate(tonic::transport::Certificate::from_pem(&ca_pem))
                .identity(tonic::transport::Identity::from_pem(&cert_pem, &key_pem))
                .domain_name("localhost");

            tonic::transport::Channel::from_shared(bridge_url)?
                .tls_config(tls)?
                .connect()
                .await?
        };
        let bridge_client = ChainBridgeServiceClient::new(channel);
        let real_provider = Arc::new(transaction::RealChainBridgeProvider::new(bridge_client));

        let provider: Arc<dyn transaction::ChainBridgeProvider> =
            if let Ok(nats_url) = std::env::var("NATS_URL") {
                if let Ok(nats_client) = async_nats::connect(&nats_url).await {
                    info!(
                        "✅ Connected to NATS for transaction submission: {}",
                        nats_url
                    );
                    let service_identity = std::env::var("CHAIN_BRIDGE_SERVICE_IDENTITY")
                        .unwrap_or_else(|_| "spiffe://gridtokenx.th/prod/unknown".to_string());

                    Arc::new(nats_provider::NatsChainBridgeProvider::new(
                        nats_client,
                        service_identity,
                        real_provider,
                        metrics.clone(),
                        Duration::from_millis(
                            std::env::var("CHAIN_BRIDGE_TX_TIMEOUT_MS")
                                .unwrap_or_else(|_| "30000".to_string())
                                .parse()
                                .unwrap_or(30000),
                        ),
                    ))
                } else {
                    warn!(
                        "⚠️ Failed to connect to NATS at {}. Falling back to gRPC-only provider.",
                        nats_url
                    );
                    real_provider
                }
            } else {
                real_provider
            };

        let transaction_handler = TransactionHandler::new(provider, metrics);

        // Payer is handled per-transaction or via authority in specific services.
        // Payer strategy:
        // 1. SOLANA_PAYER_KEY env (Base58)
        // 2. infra/solana/dev-wallet.json
        // 3. Fallback to hardcoded dev-wallet pubkey
        let payer = if let Ok(key_b58) = std::env::var("SOLANA_PAYER_KEY") {
            Pubkey::from_str(&key_b58).unwrap_or(DEFAULT_PAYER)
        } else if let Ok(kp) =
            utils::BlockchainUtils::load_keypair_from_file("infra/solana/dev-wallet.json")
        {
            kp.pubkey()
        } else {
            DEFAULT_PAYER
        };

        info!("🔑 Using blockchain payer: {}", payer);
        let instruction_builder = InstructionBuilder::new(payer, program_ids.clone());

        let account_manager = AccountManager::new(transaction_handler.clone());
        let token_manager = TokenManager::new(transaction_handler.clone(), account_manager.clone());
        let on_chain_manager = OnChainManager::new(
            transaction_handler.clone(),
            instruction_builder.clone(),
            program_ids.clone(),
        );

        let wallet_service = crate::wallet::WalletService::new(transaction_handler.clone());

        Ok(Self {
            transaction_handler,
            instruction_builder,
            account_manager,
            token_manager,
            on_chain_manager,
            wallet_service,
            cluster,
            program_ids,
        })
    }

    pub async fn health_check(&self) -> Result<bool> {
        // Simple health check could be GetLatestBlockhash
        self.transaction_handler
            .get_latest_blockhash()
            .await
            .map(|_| true)
    }

    /// Submit transaction to blockchain
    pub async fn submit_transaction(
        &self,
        transaction: solana_sdk::transaction::Transaction,
    ) -> Result<Signature> {
        self.on_chain_manager.submit_transaction(transaction).await
    }

    /// Add priority fee to instruction list
    pub async fn add_priority_fee_to_instructions(
        &self,
        instructions: &mut Vec<solana_sdk::instruction::Instruction>,
        tx_type: &'static str,
    ) -> Result<()> {
        self.transaction_handler
            .add_priority_fee_to_instructions(instructions, tx_type, None)
            .await
    }

    /// Execute on-chain atomic settlement (energy-for-currency swap)
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_atomic_settlement(
        &self,
        escrow_authority: &solana_sdk::signature::Keypair,
        market_authority: &solana_sdk::signature::Keypair,
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
        priority: Option<PriorityLevel>,
    ) -> Result<Signature> {
        self.execute_atomic_settlement_with_signers(
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
            priority,
        )
        .await
    }

    /// Execute on-chain atomic settlement with custom signers
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_atomic_settlement_with_signers(
        &self,
        escrow_authority: &(dyn Signer + Send + Sync),
        market_authority: &(dyn Signer + Send + Sync),
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
        priority: Option<PriorityLevel>,
    ) -> Result<Signature> {
        let instruction = self
            .instruction_builder
            .build_execute_atomic_settlement_instruction(
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
                escrow_authority.pubkey(),
                market_authority.pubkey(),
                amount,
                price,
                wheeling_charge,
                loss_cost,
            )?;

        let signers: Vec<&(dyn Signer + Send + Sync)> =
            if escrow_authority.pubkey() == market_authority.pubkey() {
                vec![escrow_authority]
            } else {
                vec![escrow_authority, market_authority]
            };

        self.on_chain_manager
            .build_and_send_transaction_with_priority_and_signers(
                vec![instruction],
                &signers,
                "settlement",
                priority,
            )
            .await
    }

    /// Execute on-chain batch atomic settlement
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_batch_settle_offchain_match_with_signers(
        &self,
        payer_authority: &(dyn Signer + Send + Sync),
        market_pubkey: &Pubkey,
        matches: &[instructions::BatchMatchPair],
        currency_mint: &Pubkey,
        energy_mint: &Pubkey,
        fee_collector_ata: &Pubkey,
        wheeling_collector_ata: &Pubkey,
        loss_collector_ata: &Pubkey,
        priority: Option<PriorityLevel>,
    ) -> Result<Signature> {
        let instruction = self
            .instruction_builder
            .build_batch_settle_offchain_match_instruction(
                market_pubkey,
                matches,
                currency_mint,
                energy_mint,
                fee_collector_ata,
                wheeling_collector_ata,
                loss_collector_ata,
            )?;

        self.on_chain_manager
            .build_and_send_transaction_with_priority_and_signers(
                vec![instruction],
                &[payer_authority],
                "batch_settlement",
                priority,
            )
            .await
    }

    /// Execute multiple instructions in a single atomic transaction
    pub async fn execute_batched_instructions(
        &self,
        authority: &solana_sdk::signature::Keypair,
        instructions: Vec<solana_sdk::instruction::Instruction>,
    ) -> Result<Signature> {
        self.on_chain_manager
            .build_and_send_transaction(instructions, &[authority])
            .await
    }

    /// Execute multiple instructions with custom signer
    pub async fn execute_batched_instructions_with_signer(
        &self,
        authority: &(dyn Signer + Send + Sync),
        instructions: Vec<solana_sdk::instruction::Instruction>,
    ) -> Result<Signature> {
        self.on_chain_manager
            .build_and_send_transaction_with_signers(instructions, &[authority])
            .await
    }

    /// Issue an ERC certificate on-chain
    #[allow(clippy::too_many_arguments)]
    pub async fn issue_erc(
        &self,
        certificate_id: &str,
        user_wallet: &Pubkey,
        meter_account: &Pubkey,
        energy_amount: u64,
        renewable_source: &str,
        validation_data: &str,
        authority: &solana_sdk::signature::Keypair,
    ) -> Result<Signature> {
        self.issue_erc_with_signer(
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

    /// Issue an ERC certificate on-chain with custom signer
    #[allow(clippy::too_many_arguments)]
    pub async fn issue_erc_with_signer(
        &self,
        certificate_id: &str,
        user_wallet: &Pubkey,
        meter_account: &Pubkey,
        energy_amount: u64,
        renewable_source: &str,
        validation_data: &str,
        authority: &(dyn Signer + Send + Sync),
    ) -> Result<Signature> {
        let instruction = self.instruction_builder.build_issue_erc_instruction(
            certificate_id,
            user_wallet,
            meter_account,
            energy_amount,
            renewable_source,
            validation_data,
        )?;

        self.on_chain_manager
            .build_and_send_transaction_with_signers(vec![instruction], &[authority])
            .await
    }

    /// Transfer an ERC certificate on-chain
    pub async fn transfer_erc(
        &self,
        certificate_id: &str,
        owner: &solana_sdk::signature::Keypair,
        new_owner: &Pubkey,
    ) -> Result<Signature> {
        self.transfer_erc_with_signer(certificate_id, owner, new_owner)
            .await
    }

    /// Transfer an ERC certificate on-chain with custom signer
    pub async fn transfer_erc_with_signer(
        &self,
        certificate_id: &str,
        owner: &(dyn Signer + Send + Sync),
        new_owner: &Pubkey,
    ) -> Result<Signature> {
        let instruction = self.instruction_builder.build_transfer_erc_instruction(
            certificate_id,
            &owner.pubkey(),
            new_owner,
        )?;

        self.on_chain_manager
            .build_and_send_transaction_with_signers(vec![instruction], &[owner])
            .await
    }

    /// Mint energy tokens directly to a user's wallet using the Anchor energy_token program
    pub async fn mint_tokens_direct(
        &self,
        authority: &solana_sdk::signature::Keypair,
        user_wallet: &Pubkey,
        amount: u64,
    ) -> Result<Signature> {
        self.mint_tokens_direct_with_signer(authority, user_wallet, amount)
            .await
    }

    /// Mint energy tokens directly with custom signer
    pub async fn mint_tokens_direct_with_signer(
        &self,
        authority: &(dyn Signer + Send + Sync),
        user_wallet: &Pubkey,
        amount: u64,
    ) -> Result<Signature> {
        // Convert atomic amount to UI amount (assuming 9 decimals)
        let amount_kwh =
            rust_decimal::Decimal::from(amount) / rust_decimal::Decimal::from(1_000_000_000);

        let mint = self.instruction_builder.get_mint_pda()?;

        let user_token_account = self
            .token_manager
            .ensure_token_account_exists_with_signer(authority, user_wallet, &mint)
            .await?;

        self.token_manager
            .mint_energy_tokens_with_signer(
                authority,
                &user_token_account,
                user_wallet,
                &mint,
                amount_kwh,
            )
            .await
    }

    /// Sync total supply from SPL Mint to TokenInfo PDA
    pub async fn sync_total_supply(
        &self,
        authority: &(dyn Signer + Send + Sync),
    ) -> Result<Signature> {
        let mint = self.instruction_builder.get_mint_pda()?;
        let token_info = self.instruction_builder.get_token_info_pda()?;

        let instruction = self
            .instruction_builder
            .build_sync_total_supply_instruction(mint, token_info, authority.pubkey())?;

        self.on_chain_manager
            .build_and_send_transaction_with_signers(vec![instruction], &[authority])
            .await
    }

    /// Revoke (retire) an ERC certificate on-chain with custom signer
    pub async fn revoke_erc_with_signer(
        &self,
        certificate_id: &str,
        reason: &str,
        authority: &(dyn Signer + Send + Sync),
    ) -> Result<Signature> {
        let instruction = self
            .instruction_builder
            .build_revoke_erc_instruction(certificate_id, reason)?;
        self.on_chain_manager
            .build_and_send_transaction_with_signers(vec![instruction], &[authority])
            .await
    }

    /// Parse pubkey from string
    pub fn parse_pubkey(s: &str) -> Result<Pubkey> {
        Pubkey::from_str(s).map_err(|e| anyhow::anyhow!("Invalid pubkey: {}", e))
    }

    /// Register user on-chain (Registry Program)
    pub async fn register_user_on_chain(
        &self,
        authority: Pubkey,
        user_type: instructions::UserType,
        lat_e7: i32,
        long_e7: i32,
        h3_index: u64,
        _shard_id: u8,
    ) -> Result<Signature> {
        info!("🌐 Registering user {} on-chain", authority);

        // The Registry program enforces `shard_id == authority.to_bytes()[0] % 16`
        // (registry::shard_for / superproject CLAUDE.md invariant #3). Callers can't
        // be trusted to compute this — IAM defaults it to 0 — so derive it here as
        // the single source of truth; a stale caller value yields on-chain error
        // 0x177c (InvalidShardId).
        let shard_id = shard_for(&authority);

        let mut instructions = Vec::new();

        // 1. Initialize ATA (Idempotent)
        // The energy mint (seed `mint_2022`) is owned by Token-2022, so its ATA
        // MUST be derived under spl_token_2022. Classic spl_token here produced a
        // different ATA address than the mint/balance paths (TokenManager and
        // AccountManager both use Token-2022), leaving an orphaned account and a
        // silent "0 balance".
        let mint_pda = self.instruction_builder.get_mint_pda()?;
        let ata_instruction =
            spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                &self.instruction_builder.payer(),
                &authority,
                &mint_pda,
                &spl_token_2022::id(),
            );
        instructions.push(ata_instruction);

        // 2. Register User
        let register_instruction = self.instruction_builder.build_register_user_instruction(
            authority, user_type, lat_e7, long_e7, h3_index, shard_id,
        )?;
        instructions.push(register_instruction);

        // Sovereignty: The bridge handles signing via its internal platform_admin key.
        // We pass an empty Signers list if the bridge is expected to sign.
        self.on_chain_manager
            .build_and_send_transaction(instructions, &[])
            .await
    }

    /// Register meter on-chain (Registry Program)
    pub async fn register_meter_on_chain(
        &self,
        owner: Pubkey,
        meter_id: String,
        meter_type: instructions::MeterType,
        _shard_id: u8,
        _priority: Option<PriorityLevel>,
    ) -> Result<Signature> {
        info!(
            "🌐 Registering meter {} for owner {} on-chain",
            meter_id, owner
        );

        // Same shard invariant as register_user: the program requires
        // `shard_id == owner.to_bytes()[0] % 16`. Derive it from the owner key
        // rather than trusting the caller-supplied value (error 0x177c otherwise).
        let shard_id = shard_for(&owner);

        let mut instructions = Vec::new();

        let register_instruction = self
            .instruction_builder
            .build_register_meter_instruction(owner, meter_id, meter_type, shard_id)?;
        instructions.push(register_instruction);

        self.on_chain_manager
            .build_and_send_transaction(instructions, &[])
            .await
    }

    /// Request airdrop (Devnet/Localnet only)
    pub async fn request_airdrop(&self, pubkey: &Pubkey, lamports: u64) -> Result<()> {
        info!(
            "💸 Requesting airdrop of {} lamports for {}",
            lamports, pubkey
        );
        // Route through Chain Bridge rather than silently succeeding. The bridge
        // rejects this outside dev clusters, surfacing a real error to the caller.
        self.transaction_handler
            .request_airdrop(pubkey, lamports)
            .await
            .map(|_sig| ())
    }
}

#[cfg(test)]
mod shard_tests {
    use super::shard_for;
    use solana_sdk::pubkey::Pubkey;

    // Lock the shard formula to the Registry program's `shard_for`
    // (key.to_bytes()[0] % 16). Drift here reintroduces InvalidShardId (0x177c)
    // on register_user / register_meter.
    #[test]
    fn shard_for_matches_registry_first_byte_mod_16() {
        for _ in 0..256 {
            let pk = Pubkey::new_unique();
            assert_eq!(shard_for(&pk), pk.to_bytes()[0] % 16);
            assert!(shard_for(&pk) < 16);
        }
    }
}
