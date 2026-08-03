use anyhow::Result;
use gridtokenx_blockchain_core::rpc::BlockchainUtils;
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
    /// When false, on-chain atomic-swap settlement of matching-engine trades is
    /// disabled (trade settlements are left for retry, never sent on-chain).
    pub trade_settlement_enabled: bool,
    /// Settle from the parties' own escrow PDAs rather than the platform's pooled
    /// ATAs. Off = today's custodial behaviour (see `Config`).
    pub per_user_escrow_settlement: bool,
    /// Wallet read-model repo used to self-heal a missing wallet row on demand.
    /// When `get_user_primary_wallet` misses the local `iam_wallet_read_model`
    /// (e.g. a wallet event dropped in a boot-window Kafka wedge before it was
    /// projected), this reconciles that one user from the IAM source instead of
    /// failing closed until the next restart's boot backfill. `None` disables
    /// the fallback (read-model feed off / no source pool wired).
    pub wallet_read_model: Option<Arc<dyn trading_core::traits::WalletReadModelRepository>>,
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
            treasury_program_id: program_ids.treasury_program_id,
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
            trade_settlement_enabled: false,
            per_user_escrow_settlement: false,
            wallet_read_model: None,
        })
    }

    /// Set the identity gateway for custodial signing
    #[must_use]
    pub fn with_identity_gateway(mut self, gateway: Arc<dyn trading_core::traits::IdentityGateway>) -> Self {
        self.identity_gateway = Some(gateway);
        self
    }

    /// Wire the wallet read-model repo enabling on-demand self-heal of a missing
    /// wallet row in `get_user_primary_wallet` (see the field doc).
    #[must_use]
    pub fn with_wallet_read_model(
        mut self,
        repo: Arc<dyn trading_core::traits::WalletReadModelRepository>,
    ) -> Self {
        self.wallet_read_model = Some(repo);
        self
    }

    /// Enable/disable on-chain atomic-swap settlement of matching-engine trades.
    #[must_use]
    pub fn with_trade_settlement_enabled(mut self, enabled: bool) -> Self {
        self.trade_settlement_enabled = enabled;
        self
    }

    /// Settle from the parties' own escrow PDAs instead of the platform's pooled
    /// ATAs. See `Config::per_user_escrow_settlement`.
    #[must_use]
    pub fn with_per_user_escrow_settlement(mut self, enabled: bool) -> Self {
        self.per_user_escrow_settlement = enabled;
        self
    }

    /// Read the settlement charge rates from chain: the market fee, and the
    /// wheeling/loss tariff.
    ///
    /// These decide what the buyer's escrow is actually debited before the seller
    /// is paid, and they are the only correct source for the `settlements` ledger.
    /// In particular do NOT use `Config::transaction_fee_bps` — it defaults to 50
    /// while the deployed market reads 25, so it would book double the real fee.
    ///
    /// Both accounts are `#[account(zero_copy)]`, so they are parsed by byte offset
    /// (Borsh cannot decode them) — see `market_accounts`.
    pub async fn read_charge_rates(&self) -> Result<trading_core::charges::StaticChargeRates> {
        use gridtokenx_blockchain_core::rpc::instructions::market_accounts;

        let trading_program_id = self.trading_program_id()?;
        let (market_pda, _) = Pubkey::find_program_address(&[b"market"], &trading_program_id);
        let (tariff_pda, _) =
            Pubkey::find_program_address(&[b"tariff_config"], &trading_program_id);

        let market = self.get_account_data(&market_pda).await?;
        let tariff = self.get_account_data(&tariff_pda).await?;

        Ok(trading_core::charges::StaticChargeRates {
            fee_bps: market_accounts::parse_market_fee_bps(&market)?,
            wheeling_rate_per_kwh: market_accounts::parse_tariff_wheeling_rate(&tariff)?,
            loss_bps: market_accounts::parse_tariff_loss_bps(&tariff)?,
        })
    }

    /// Load one side of a match in the form `settle_offchain_match` needs: the
    /// terms the wallet signed, plus the signature.
    ///
    /// Returns `None` when the order carries no wallet signature — i.e. it was
    /// placed before per-user escrow settlement was enabled. Such orders can only
    /// settle on the pooled path; the caller must skip them rather than attempt a
    /// settlement the on-chain Ed25519 check would reject.
    ///
    /// Amounts are converted with the same truncating rule the browser used when
    /// signing (`trading_core::offchain_payload`); any divergence would rebuild a
    /// different message and fail verification on-chain.
    pub async fn get_signed_order_side(
        &self,
        order_id: &uuid::Uuid,
    ) -> Result<Option<crate::blockchain::settlement::SignedOrderSide>> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Database pool not available in BlockchainService"))?;

        // (user_id, side, energy, price, zone, expires_at, meter_serial)
        #[allow(clippy::items_after_statements)] // alias belongs beside its query
        type OrderRow = (
            uuid::Uuid,
            String,
            rust_decimal::Decimal,
            rust_decimal::Decimal,
            Option<i32>,
            Option<chrono::DateTime<chrono::Utc>>,
            Option<String>,
        );
        let row: Option<OrderRow> =
            sqlx::query_as(
                "SELECT user_id, side::text, energy_amount, price_per_kwh, zone_id, expires_at, signature \
                 FROM trading_orders WHERE id = $1",
            )
            .bind(order_id)
            .fetch_optional(db)
            .await?;

        let Some((user_id, side, energy, price, zone_id, expires_at, signature)) = row else {
            return Ok(None);
        };
        let Some(signature) = signature else {
            return Ok(None); // unsigned legacy order — pooled path only
        };

        let Some(wallet) = self.get_user_primary_wallet(&user_id).await? else {
            return Ok(None);
        };

        let energy_amount = trading_core::offchain_payload::energy_to_base_units(energy)
            .ok_or_else(|| anyhow::anyhow!("order {order_id}: energy {energy} out of range"))?;
        let price_per_kwh = trading_core::offchain_payload::currency_to_base_units(price)
            .ok_or_else(|| anyhow::anyhow!("order {order_id}: price {price} out of range"))?;

        Ok(Some(crate::blockchain::settlement::SignedOrderSide {
            order_id: *order_id.as_bytes(),
            user: wallet,
            energy_amount,
            price_per_kwh,
            side: if side.eq_ignore_ascii_case("sell") {
                trading_core::offchain_payload::SIDE_SELL
            } else {
                trading_core::offchain_payload::SIDE_BUY
            },
            zone_id: u32::try_from(zone_id.unwrap_or(0)).unwrap_or(0), // non-negative by schema
            expires_at: expires_at.map_or(0, |t| t.timestamp()),
            signature,
        }))
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
    #[allow(clippy::unused_async)] // API symmetry with the Vault-backed variant
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

    /// Wait for transaction confirmation, polling until `timeout_secs` elapses.
    ///
    /// Distinguishes "landed but failed on-chain" (terminal `Err`, e.g. the
    /// settlement's escrow/PDA constraint checks) from "not confirmed yet"
    /// (`Ok(false)`, safe to retry) — a bridge-reported execution error is NOT
    /// success just because the transaction was included in a block. Reusing
    /// the old `confirm_transaction` (a single lossy `Ok(bool)` check that
    /// discards the error) let a failed on-chain settlement be recorded as
    /// `Completed` in the DB with a real-looking signature.
    pub async fn wait_for_confirmation(
        &self,
        signature: &Signature,
        timeout_secs: u64,
    ) -> Result<bool> {
        use gridtokenx_blockchain_core::SignatureState;

        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(timeout_secs);
        let poll_interval = std::time::Duration::from_millis(500);

        loop {
            match self
                .core
                .transaction_handler
                .signature_state(signature)
                .await
            {
                Ok(SignatureState::Confirmed) => return Ok(true),
                Ok(SignatureState::Failed(err)) => {
                    return Err(anyhow::anyhow!(
                        "transaction {signature} failed on-chain: {err}"
                    ))
                }
                Ok(SignatureState::Pending) => {}
                Err(e) => {
                    tracing::warn!("signature_state check failed for {signature}: {e}; retrying");
                }
            }
            if start.elapsed() >= timeout {
                return Ok(false);
            }
            tokio::time::sleep(poll_interval).await;
        }
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
            .map(|()| Signature::default())
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
        #[allow(clippy::cast_precision_loss)] // display-path SOL conversion
        Ok(balance as f64 / 1_000_000_000.0)
    }

    /// Get SPL token balance for a user. Assumes the mint's ATA is under
    /// Token-2022 — true for energy, NOT for the classic-SPL currency mint. For
    /// that, use [`Self::get_token_balance_with_program`].
    pub async fn get_token_balance(&self, owner: &Pubkey, mint: &Pubkey) -> Result<u64> {
        self.core.token_manager.get_token_balance(owner, mint).await
    }

    /// Balance for a mint whose owning token program is not Token-2022.
    pub async fn get_token_balance_with_program(
        &self,
        owner: &Pubkey,
        mint: &Pubkey,
        token_program: &Pubkey,
    ) -> Result<u64> {
        self.core
            .token_manager
            .get_token_balance_with_program(owner, mint, token_program)
            .await
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

    /// Calculate ATA address (assumes Token-2022 — correct for the energy mint only).
    pub fn calculate_ata_address(&self, owner: &Pubkey, mint: &Pubkey) -> Result<Pubkey> {
        self.core.account_manager.calculate_ata_address(owner, mint)
    }

    /// Calculate ATA address under an explicit token program. Use for mints that
    /// are not Token-2022 — notably the classic-SPL currency mint, whose ATAs must
    /// be derived under `spl_token::id()`.
    pub fn calculate_ata_address_with_program(
        &self,
        owner: &Pubkey,
        mint: &Pubkey,
        token_program: &Pubkey,
    ) -> Result<Pubkey> {
        self.core
            .account_manager
            .calculate_ata_address_with_program(owner, mint, token_program)
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
        Pubkey::from_str(pubkey_str).map_err(|e| anyhow::anyhow!("Invalid pubkey: {e}"))
    }

    /// Get the instruction builder
    #[must_use]
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

    /// Custodial order placement (Option A): record the order PDA, platform-signed
    /// (no user signature). Returns (signature, `order_pda`).
    ///
    /// This deliberately does NOT fund `[b"escrow", user, mint]`. It used to, and the
    /// transfer was unspendable in both settlement modes:
    ///
    /// - **Custodial mode** (`per_user_escrow_settlement = false`, the only mode that
    ///   reaches this function from REST): `execute_atomic_settlement` is handed the
    ///   platform's pooled ATA as `buyer_currency_escrow`/`seller_energy_escrow`
    ///   (`blockchain/settlement.rs:214-220`, which even says
    ///   `// party ATAs unused for this model`). The per-user escrow PDA is never
    ///   read, so every placement moved platform tokens into an account settlement
    ///   ignores — and `withdraw_escrow` lets that user take them
    ///   (`trading/src/instructions/escrow.rs:166-196`). Nothing refunded them either:
    ///   `refund_escrow_to_buyer` has no callers. Measured 2026-08-01: 8 THBC parked in
    ///   one test user's escrow across two buys, one of them cancelled.
    /// - **Per-user mode**: each party funds their own escrow by wallet-signed
    ///   `deposit_escrow`, which is the entire point of the flag. Platform-funding it
    ///   here would defeat that.
    ///
    /// So the funding leg is dropped, not made conditional — there is no mode that
    /// wants it. The pooled ATA still pays the seller in custodial mode; that is the
    /// model's known economics, not this function's business.
    // The on-chain placement genuinely takes this many independent params.
    #[allow(clippy::too_many_arguments)]
    pub async fn place_order_on_chain(
        &self,
        user_wallet: &Pubkey,
        order_id: u64,
        is_buy: bool,
        energy_amount_atomic: u64,
        price_atomic: u64,
        zone_id: u32,
        expires_at_unix: i64,
    ) -> Result<(Signature, Pubkey)> {
        use std::str::FromStr;
        let authority = self.get_authority_keypair().await?;
        let funder = authority.pubkey();
        let trading_program_id = Pubkey::from_str(&self.core.program_ids.trading_program_id)?;
        let (market_pda, _) = Pubkey::find_program_address(&[b"market"], &trading_program_id);
        let order_pda = self.derive_order_pda(user_wallet, order_id)?;

        let record_ix = self.core.instruction_builder.build_record_order_custodial_instruction(
            &market_pda,
            order_pda,
            order_id,
            user_wallet,
            is_buy,
            energy_amount_atomic,
            price_atomic,
            funder,
            zone_id,
            expires_at_unix,
        )?;

        let signature = self
            .execute_batched_instructions(&[&authority], vec![record_ix])
            .await?;

        // Submission only proves the bridge accepted the tx for broadcast, not that
        // record_order_custodial actually ran — the caller (rest.rs) stamps
        // blockchain_status="confirmed" off this Ok, so an unconfirmed/failed tx
        // must not be reported as success (the order_pda would then point at an
        // account that was never created on-chain).
        let confirm_timeout = std::env::var("ORDER_CONFIRM_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30);
        match self.wait_for_confirmation(&signature, confirm_timeout).await {
            Ok(true) => Ok((signature, order_pda)),
            Ok(false) => Err(anyhow::anyhow!(
                "order placement {signature} not confirmed within {confirm_timeout}s"
            )),
            Err(e) => Err(anyhow::anyhow!(
                "order placement {signature} confirmation failed: {e}"
            )),
        }
    }

    /// Execute on-chain `create_order`
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
        expires_at_unix: i64,
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
                expires_at_unix,
            )?;

        let sig = self
            .core
            .on_chain_manager
            .build_and_send_transaction_with_signers(vec![instruction], &[authority as &(dyn Signer + Send + Sync)])
            .await?;
        Ok((sig, order_pda.to_string(), index))
    }

    /// Execute on-chain `match_orders`
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

    /// Issue an ERC certificate on-chain via the sovereign Chain Bridge path.
    ///
    /// Trading is custodial: it holds only the meter owner's `Pubkey` (the key
    /// lives in IAM/Vault), so it cannot sign locally. Submit with no local
    /// signer; Chain Bridge signs server-side.
    pub async fn issue_erc(
        &self,
        certificate_id: &str,
        user_wallet: &Pubkey,
        meter_account: &Pubkey,
        energy_amount: u64,
        renewable_source: &str,
        validation_data: &str,
    ) -> Result<Signature> {
        self.core
            .issue_erc_sovereign(
                certificate_id,
                user_wallet,
                meter_account,
                energy_amount,
                renewable_source,
                validation_data,
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
        // Per-match id (settlement row UUID) → on-chain TradeNullifier replay guard (F3c).
        trade_id: [u8; 16],
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
                trade_id,
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
        // Per-match id (settlement row UUID) → on-chain TradeNullifier replay guard (F3c).
        trade_id: [u8; 16],
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
                trade_id,
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
        for (price, amount) in buy_prices.into_iter().zip(buy_amounts) {
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
        for (price, amount) in sell_prices.into_iter().zip(sell_amounts) {
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

        // db-split cutover: read from the Trading-owned local read-model
        // `iam_wallet_read_model` (fed by IAM `user.wallet.*` Kafka events + boot
        // backfill, TRADING_READMODEL_FEED) instead of the cross-domain IAM
        // `user_wallets`. The read-model mirrors the same (user_id, is_primary,
        // wallet_address) triple. See migrations/20260715000001_trading_phase1_local_models.sql.
        let wallet_address: Option<String> = sqlx::query_scalar(
            "SELECT wallet_address FROM iam_wallet_read_model WHERE user_id = $1 AND is_primary = true",
        )
        .bind(user_id)
        .fetch_optional(db)
        .await?;

        if let Some(addr) = wallet_address {
            return Ok(Some(Self::parse_pubkey(&addr)?));
        }

        // Read-model miss. Before failing closed, try a lazy single-user
        // reconcile from the IAM source: the wallet row may simply never have
        // been projected (e.g. a `user.wallet.*` event dropped in a boot-window
        // Kafka producer wedge). This self-heals resolution on demand instead of
        // waiting for the next restart's boot backfill.
        if let Some(repo) = &self.wallet_read_model {
            if let Some(addr) = repo.backfill_wallet_for(*user_id).await.map_err(|e| {
                anyhow::anyhow!("lazy wallet read-model reconcile failed for {user_id}: {e}")
            })? {
                tracing::info!(
                    %user_id,
                    "wallet read-model miss self-healed via lazy IAM-source reconcile"
                );
                return Ok(Some(Self::parse_pubkey(&addr)?));
            }
        }
        Ok(None)
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
