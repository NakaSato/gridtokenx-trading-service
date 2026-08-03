use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signer;
use std::sync::Arc;

use crate::blockchain::BlockchainService;
use trading_core::error::ApiError;
use trading_core::models::{Settlement, SettlementTransaction};

/// One side of an off-chain match, as `settle_offchain_match` needs it: the terms
/// the user's wallet signed, plus that signature.
///
/// Amounts are on-chain base units (energy 9-dec, currency 6-dec) — the same
/// truncating conversion the browser used when it signed, or the reconstructed
/// message will not match and the on-chain Ed25519 check rejects the trade.
#[derive(Debug, Clone)]
pub struct SignedOrderSide {
    /// Order UUID bytes — part of the signed payload.
    pub order_id: [u8; 16],
    /// The signer's wallet (also the escrow PDA seed).
    pub user: Pubkey,
    pub energy_amount: u64,
    pub price_per_kwh: u64,
    /// 0 = Buy, 1 = Sell — the on-chain discriminants.
    pub side: u8,
    pub zone_id: u32,
    pub expires_at: i64,
    /// Base58 Ed25519 signature over the canonical payload.
    pub signature: String,
}

/// The four party ATAs a trade settlement moves tokens between. Each is derived
/// under its mint's owning token program — energy (GRID/GRX) is Token-2022,
/// currency (THBC) is classic SPL Token. Deriving a classic-mint ATA under
/// Token-2022 (or vice versa) yields a different, unfunded address, so the program
/// must be chosen per mint and never assumed.
#[allow(clippy::struct_field_names)] // the shared `_ata` suffix IS the meaning
struct TradeAtas {
    buyer_currency_ata: Pubkey,
    seller_energy_ata: Pubkey,
    seller_currency_ata: Pubkey,
    buyer_energy_ata: Pubkey,
}

/// Map our side struct onto the payload type the instruction builder expects.
/// The field set is identical — the two types exist either side of a crate
/// boundary, not because they differ.
fn to_core_payload(
    s: &SignedOrderSide,
) -> gridtokenx_blockchain_core::rpc::instructions::OffchainOrderPayload {
    gridtokenx_blockchain_core::rpc::instructions::OffchainOrderPayload {
        order_id: s.order_id,
        user: s.user,
        energy_amount: s.energy_amount,
        price_per_kwh: s.price_per_kwh,
        side: s.side,
        zone_id: s.zone_id,
        expires_at: s.expires_at,
    }
}

/// Scale a `Decimal` to on-chain base units, rejecting overflow instead of
/// silently settling zero (`Decimal`'s `*` panics on overflow, so use checked).
fn decimal_to_atomic(v: Option<Decimal>, scale: i64) -> trading_core::error::Result<u64> {
    let v = v.unwrap_or(Decimal::ZERO);
    v.checked_mul(Decimal::from(scale))
        .and_then(|x| ToPrimitive::to_u64(&x.trunc()))
        .ok_or_else(|| ApiError::Validation(format!("amount {v} out of range for settlement")))
}

fn settlement_amount_atomic(s: &Settlement) -> trading_core::error::Result<u64> {
    decimal_to_atomic(Some(s.energy_amount), 1_000_000_000i64)
}

fn settlement_price_atomic(s: &Settlement) -> trading_core::error::Result<u64> {
    decimal_to_atomic(Some(s.price), 1_000_000i64)
}

#[derive(Debug)]
pub struct BlockchainSettlementProvider {
    blockchain: Arc<BlockchainService>,
}

impl BlockchainSettlementProvider {
    #[must_use]
    pub fn new(blockchain: Arc<BlockchainService>) -> Self {
        Self { blockchain }
    }

    /// Derive the four trade-settlement party ATAs, each under its mint's owning
    /// token program (currency = classic SPL Token, energy = Token-2022). Shared
    /// by the single and batched settlement paths so the program selection cannot
    /// drift between them.
    fn derive_trade_atas(
        &self,
        energy_mint: &Pubkey,
        currency_mint: &Pubkey,
        buyer_pubkey: &Pubkey,
        seller_pubkey: &Pubkey,
    ) -> trading_core::error::Result<TradeAtas> {
        let energy_tp = spl_token_2022::id();
        let currency_tp = crate::blockchain::currency_token_program();
        Ok(TradeAtas {
            buyer_currency_ata: self.blockchain.calculate_ata_address_with_program(
                buyer_pubkey,
                currency_mint,
                &currency_tp,
            )?,
            seller_energy_ata: self.blockchain.calculate_ata_address_with_program(
                seller_pubkey,
                energy_mint,
                &energy_tp,
            )?,
            seller_currency_ata: self.blockchain.calculate_ata_address_with_program(
                seller_pubkey,
                currency_mint,
                &currency_tp,
            )?,
            buyer_energy_ata: self.blockchain.calculate_ata_address_with_program(
                buyer_pubkey,
                energy_mint,
                &energy_tp,
            )?,
        })
    }

    #[tracing::instrument(skip(self, settlement), fields(settlement_id = %settlement.id))]
    // One deliberate on-chain sequence; splitting scatters the invariants.
    #[allow(clippy::too_many_lines)]
    pub async fn execute_atomic_settlement(
        &self,
        settlement: &Settlement,
        buy_order_pda: &Pubkey,
        sell_order_pda: &Pubkey,
        buyer_pubkey: &Pubkey,
        seller_pubkey: &Pubkey,
    ) -> trading_core::error::Result<SettlementTransaction> {
        let trading_program_id = self
            .blockchain
            .trading_program_id()
            .map_err(|e| ApiError::Internal(format!("Trading program ID error: {e}")))?;
        let (market_pda, _) = Pubkey::find_program_address(&[b"market"], &trading_program_id);

        let platform_authority = self
            .blockchain
            .get_authority_keypair()
            .await
            .map_err(|e| ApiError::Internal(format!("Failed to get authority: {e}")))?;

        // Mints: Energy Token is derived, Currency from env
        let energy_mint = self
            .blockchain
            .instruction_builder()
            .get_mint_pda()
            .map_err(|e| ApiError::Internal(format!("Failed to derive energy mint PDA: {e}")))?;
        let currency_mint_str = std::env::var("CURRENCY_TOKEN_MINT").unwrap_or_default();
        let currency_mint = BlockchainService::parse_pubkey(&currency_mint_str)?;

        // Party ATAs (currency = classic SPL Token, energy = Token-2022).
        let TradeAtas {
            buyer_currency_ata,
            seller_energy_ata,
            seller_currency_ata,
            buyer_energy_ata,
        } = self.derive_trade_atas(&energy_mint, &currency_mint, buyer_pubkey, seller_pubkey)?;

        // Collectors (currency mint → classic SPL Token program).
        let currency_tp = crate::blockchain::currency_token_program();
        let fee_collector = BlockchainService::parse_pubkey(
            &std::env::var("FEE_COLLECTOR_WALLET").unwrap_or_default(),
        )?;
        let wheeling_collector = BlockchainService::parse_pubkey(
            &std::env::var("WHEELING_COLLECTOR_WALLET").unwrap_or_default(),
        )?;
        let loss_collector = BlockchainService::parse_pubkey(
            &std::env::var("LOSS_COLLECTOR_WALLET").unwrap_or_default(),
        )?;

        let fee_collector_ata = self.blockchain.calculate_ata_address_with_program(
            &fee_collector,
            &currency_mint,
            &currency_tp,
        )?;
        let wheeling_collector_ata = self.blockchain.calculate_ata_address_with_program(
            &wheeling_collector,
            &currency_mint,
            &currency_tp,
        )?;
        let loss_collector_ata = self.blockchain.calculate_ata_address_with_program(
            &loss_collector,
            &currency_mint,
            &currency_tp,
        )?;

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

        // `execute_atomic_settlement` sources funds from escrow accounts owned by
        // `escrow_authority` (= the platform key the bridge signs as, which also ==
        // market.authority). So the escrows are the PLATFORM's pooled ATAs, not the
        // parties' ATAs. (Per-user `fund_escrow_custodial` PDAs target the separate
        // settle_offchain model — not this instruction.) The receiver ATAs (seller
        // currency, buyer energy) must exist; custodial parties have none, so create
        // them idempotently in the same tx. escrow_authority == market_authority ==
        // platform → one bridge signature satisfies both signer slots.
        let platform = platform_authority.pubkey();
        let energy_tp = spl_token_2022::id();
        let buyer_currency_escrow = self.blockchain.calculate_ata_address_with_program(
            &platform,
            &currency_mint,
            &currency_tp,
        )?;
        let seller_energy_escrow = self.blockchain.calculate_ata_address_with_program(
            &platform,
            &energy_mint,
            &energy_tp,
        )?;
        let _ = (buyer_currency_ata, seller_energy_ata); // party ATAs unused for this model

        let create_seller_currency =
            spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                &platform,
                seller_pubkey,
                &currency_mint,
                &currency_tp,
            );
        let create_buyer_energy =
            spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                &platform,
                buyer_pubkey,
                &energy_mint,
                &energy_tp,
            );

        let settle_ix = self
            .blockchain
            .instruction_builder()
            .build_execute_atomic_settlement_instruction(
                market_pda,
                *buy_order_pda,
                *sell_order_pda,
                buyer_currency_escrow,
                seller_energy_escrow,
                seller_currency_ata,
                buyer_energy_ata,
                fee_collector_ata,
                wheeling_collector_ata,
                loss_collector_ata,
                energy_mint,
                currency_mint,
                platform,
                platform,
                amount_atomic,
                price_atomic,
                wheeling_val,
                loss_val,
                // Per-match id (settlement row UUID) → on-chain TradeNullifier replay guard (F3c).
                *settlement.id.as_bytes(),
            )
            .map_err(|e| ApiError::Internal(format!("build settle ix: {e}")))?;

        let signature = self
            .blockchain
            .execute_batched_instructions(
                &[&platform_authority],
                vec![create_seller_currency, create_buyer_energy, settle_ix],
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

    /// Settle a match from the parties' **own** escrow PDAs
    /// (`[b"escrow", user, mint]`) via `settle_offchain_match`.
    ///
    /// This is the conserving path. `execute_atomic_settlement` above spends the
    /// PLATFORM's pooled ATAs, so a seller's own GRX is never debited and the pool
    /// drains by the traded amount on every match; here the tokens come from the
    /// parties, funded by their wallet-signed `deposit_escrow`.
    ///
    /// It cannot reuse `execute_atomic_settlement`: that instruction takes
    /// `escrow_authority: Signer` (a keypair), and the escrow PDAs are owned by the
    /// `market_authority` PDA, which only the program can sign for.
    ///
    /// Transaction layout is fixed by the on-chain handler, which reads the
    /// instructions sysvar at indices 0 and 1
    /// (`settle_offchain.rs:705-714`):
    ///
    ///   `[ed25519(buyer), ed25519(seller), settle_offchain_match]`
    ///
    /// Enabled by `per_user_escrow_settlement`; both orders must carry a valid
    /// wallet signature or the on-chain verification rejects the trade.
    #[tracing::instrument(skip(self, settlement, buyer, seller), fields(settlement_id = %settlement.id))]
    // One deliberate on-chain sequence; splitting scatters the invariants.
    #[allow(clippy::too_many_lines)]
    pub async fn execute_offchain_settlement(
        &self,
        settlement: &Settlement,
        buyer: &SignedOrderSide,
        seller: &SignedOrderSide,
    ) -> trading_core::error::Result<SettlementTransaction> {
        use gridtokenx_blockchain_core::rpc::instructions::{
            build_ed25519_verify_instruction, market_accounts,
        };

        let trading_program_id = self
            .blockchain
            .trading_program_id()
            .map_err(|e| ApiError::Internal(format!("Trading program ID error: {e}")))?;
        let (market_pda, _) = Pubkey::find_program_address(&[b"market"], &trading_program_id);

        let platform_authority = self
            .blockchain
            .get_authority_keypair()
            .await
            .map_err(|e| ApiError::Internal(format!("Failed to get authority: {e}")))?;

        let energy_mint = self
            .blockchain
            .instruction_builder()
            .get_mint_pda()
            .map_err(|e| ApiError::Internal(format!("Failed to derive energy mint PDA: {e}")))?;
        let currency_mint = BlockchainService::parse_pubkey(
            &std::env::var("CURRENCY_TOKEN_MINT").unwrap_or_default(),
        )?;

        // Shard counts must be the values the market was initialized with — the
        // shard PDA is `payer[0] % num_shards`, so a guess derives an account that
        // does not exist. Read them rather than assuming 16 (this deployment is 4).
        let market_data = self
            .blockchain
            .get_account_data(&market_pda)
            .await
            .map_err(|e| ApiError::Internal(format!("read market account: {e}")))?;
        let market_num_shards = market_accounts::parse_market_num_shards(&market_data)
            .map_err(|e| ApiError::Internal(format!("market num_shards: {e}")))?;

        let (zone_market_pda, _) = Pubkey::find_program_address(
            &[
                b"zone_market",
                market_pda.as_ref(),
                &buyer.zone_id.to_le_bytes(),
            ],
            &trading_program_id,
        );
        let zone_data = self
            .blockchain
            .get_account_data(&zone_market_pda)
            .await
            .map_err(|e| ApiError::Internal(format!("read zone_market account: {e}")))?;
        let zone_num_shards = market_accounts::parse_zone_market_num_shards(&zone_data)
            .map_err(|e| ApiError::Internal(format!("zone num_shards: {e}")))?;

        // Rebuild each signed message from the stored terms and pair it with the
        // stored signature. The program recomputes the same bytes from the payload
        // it receives, so any drift between what was signed and what we persisted
        // surfaces here as a rejected trade rather than a wrong transfer.
        let ed25519_ix = |s: &SignedOrderSide| -> trading_core::error::Result<Instruction> {
            let msg = trading_core::offchain_payload::message_for(
                &s.order_id,
                &s.user.to_bytes(),
                s.energy_amount,
                s.price_per_kwh,
                s.side,
                s.zone_id,
                s.expires_at,
            );
            let sig_bytes: [u8; 64] = bs58::decode(&s.signature)
                .into_vec()
                .ok()
                .and_then(|v| v.try_into().ok())
                .ok_or_else(|| {
                    ApiError::Validation(format!(
                        "order {} has no valid Base58 Ed25519 signature",
                        uuid::Uuid::from_bytes(s.order_id)
                    ))
                })?;
            build_ed25519_verify_instruction(&s.user, &msg, &sig_bytes)
                .map_err(|e| ApiError::Internal(format!("build ed25519 ix: {e}")))
        };

        let buyer_payload = to_core_payload(buyer);
        let seller_payload = to_core_payload(seller);

        let settle_ix = self
            .blockchain
            .instruction_builder()
            .build_settle_offchain_match_instruction(
                &market_pda,
                &buyer_payload,
                &seller_payload,
                &currency_mint,
                &energy_mint,
                settlement_amount_atomic(settlement)?,
                settlement_price_atomic(settlement)?,
                decimal_to_atomic(settlement.wheeling_charge, 1_000_000i64)?,
                decimal_to_atomic(settlement.loss_cost, 1_000_000i64)?,
                *settlement.id.as_bytes(),
                market_num_shards,
                zone_num_shards,
                false, // REC leg is opt-in and requires both parties' REC escrows funded
            )
            .map_err(|e| ApiError::Internal(format!("build settle_offchain ix: {e}")))?;

        // Order is load-bearing: the handler reads buyer at sysvar index 0 and
        // seller at index 1.
        let signature = self
            .blockchain
            .execute_batched_instructions(
                &[&platform_authority],
                vec![ed25519_ix(buyer)?, ed25519_ix(seller)?, settle_ix],
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
    #[tracing::instrument(skip(self, settlement, seller_signer), fields(settlement_id = %settlement.id))]
    pub async fn execute_energy_transfer(
        &self,
        settlement: &Settlement,
        seller_signer: &(dyn Signer + Send + Sync),
        buyer_pubkey: &Pubkey,
    ) -> trading_core::error::Result<SettlementTransaction> {
        let mint_str = std::env::var("ENERGY_TOKEN_MINT").unwrap_or_default();
        let mint = BlockchainService::parse_pubkey(&mint_str)?;
        let seller_pubkey = seller_signer.pubkey();

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
                seller_signer,
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
    // One deliberate on-chain sequence; splitting scatters the invariants.
    #[allow(clippy::too_many_lines)]
    pub async fn execute_batched_settlements(
        &self,
        inputs: Vec<(
            &trading_core::models::Settlement,
            Pubkey,
            Pubkey,
            Pubkey,
            Pubkey,
        )>,
        priority_fee: u64,
    ) -> trading_core::error::Result<Vec<SettlementTransaction>> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let trading_program_id = self
            .blockchain
            .trading_program_id()
            .map_err(|e| ApiError::Internal(format!("Trading program ID error: {e}")))?;
        let (market_pda, _) = Pubkey::find_program_address(&[b"market"], &trading_program_id);

        let platform_authority = self
            .blockchain
            .get_authority_keypair()
            .await
            .map_err(|e| ApiError::Internal(format!("Failed to get authority: {e}")))?;

        // Mints: Energy Token is derived, Currency from env
        let energy_mint = self
            .blockchain
            .instruction_builder()
            .get_mint_pda()
            .map_err(|e| ApiError::Internal(format!("Failed to derive energy mint PDA: {e}")))?;
        let currency_mint_str = std::env::var("CURRENCY_TOKEN_MINT").unwrap_or_default();
        let currency_mint = BlockchainService::parse_pubkey(&currency_mint_str)?;

        // Collectors
        let fee_collector = BlockchainService::parse_pubkey(
            &std::env::var("FEE_COLLECTOR_WALLET").unwrap_or_default(),
        )?;
        let wheeling_collector = BlockchainService::parse_pubkey(
            &std::env::var("WHEELING_COLLECTOR_WALLET").unwrap_or_default(),
        )?;
        let loss_collector = BlockchainService::parse_pubkey(
            &std::env::var("LOSS_COLLECTOR_WALLET").unwrap_or_default(),
        )?;

        // Collectors hold currency (GRX) → classic SPL Token program. Party ATAs
        // are derived per-trade below via `derive_trade_atas`.
        let currency_tp = crate::blockchain::currency_token_program();

        let fee_collector_ata = self.blockchain.calculate_ata_address_with_program(
            &fee_collector,
            &currency_mint,
            &currency_tp,
        )?;
        let wheeling_collector_ata = self.blockchain.calculate_ata_address_with_program(
            &wheeling_collector,
            &currency_mint,
            &currency_tp,
        )?;
        let loss_collector_ata = self.blockchain.calculate_ata_address_with_program(
            &loss_collector,
            &currency_mint,
            &currency_tp,
        )?;

        // `execute_atomic_settlement` sources funds from escrows owned by
        // `escrow_authority` (= the platform key the bridge signs as, also ==
        // market.authority). So escrows are the PLATFORM's pooled ATAs, not the
        // parties'. escrow_authority == market_authority == platform → one bridge
        // signature satisfies both signer slots.
        let platform = platform_authority.pubkey();
        let energy_tp = spl_token_2022::id();
        let buyer_currency_escrow = self.blockchain.calculate_ata_address_with_program(
            &platform,
            &currency_mint,
            &currency_tp,
        )?;
        let seller_energy_escrow = self.blockchain.calculate_ata_address_with_program(
            &platform,
            &energy_mint,
            &energy_tp,
        )?;

        let mut groups = Vec::new();

        for (settlement, buy_order_pda, sell_order_pda, buyer_pubkey, seller_pubkey) in inputs {
            let mut instructions: Vec<Instruction> = Vec::with_capacity(3);
            // Receiver ATAs (seller gets currency, buyer gets energy). Custodial
            // parties may have none, so create them idempotently in the same tx.
            let TradeAtas {
                buyer_currency_ata: _,
                seller_energy_ata: _,
                seller_currency_ata,
                buyer_energy_ata,
            } = self.derive_trade_atas(
                &energy_mint,
                &currency_mint,
                &buyer_pubkey,
                &seller_pubkey,
            )?;
            instructions.push(
                spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                    &platform, &seller_pubkey, &currency_mint, &currency_tp,
                ),
            );
            instructions.push(
                spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                    &platform, &buyer_pubkey, &energy_mint, &energy_tp,
                ),
            );

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
                    market_pda,
                    buy_order_pda,
                    sell_order_pda,
                    buyer_currency_escrow,
                    seller_energy_escrow,
                    seller_currency_ata,
                    buyer_energy_ata,
                    fee_collector_ata,
                    wheeling_collector_ata,
                    loss_collector_ata,
                    energy_mint,
                    currency_mint,
                    platform_authority.pubkey(),
                    platform_authority.pubkey(),
                    amount_atomic,
                    price_atomic,
                    wheeling_val,
                    loss_val,
                    // Per-match id = the settlement row UUID. Stable across re-signed retries,
                    // so the on-chain TradeNullifier rejects a replay that already landed (F3c).
                    *settlement.id.as_bytes(),
                )
                .map_err(|e| ApiError::Internal(format!("Failed to build instruction: {e}")))?;

            instructions.push(instruction);
            groups.push((instructions, settlement.id));
        }

        // F3a: gate completion on FINALITY, not the bridge's RPC-accept. A generic submit
        // returns as soon as Chain Bridge accepts the tx (fire-and-forget, slot 0); the tx
        // may never finalize. Retry is safe: the on-chain TradeNullifier (F3c) no-ops a
        // replay of a settle that already landed, so a lost-reply retry cannot double-settle.
        let confirm_timeout = std::env::var("SETTLEMENT_CONFIRM_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30);

        // Each settlement is 3 instructions (~460 raw bytes); a Solana tx caps at 1232 raw
        // bytes, so packing the whole worker batch into one tx overflows (-32602 "too large").
        // Chunk settlements across txs — default 2 (~960B/tx); tune via SETTLEMENT_TX_CHUNK.
        let chunk = std::env::var("SETTLEMENT_TX_CHUNK")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n >= 1)
            .unwrap_or(2);

        let mut tx_results = Vec::new();
        for group in groups.chunks(chunk) {
            let mut instructions: Vec<Instruction> = group
                .iter()
                .flat_map(|(ix, _)| ix.iter().cloned())
                .collect();

            // Add priority fee if provided
            if priority_fee > 0 {
                self.blockchain
                    .add_priority_fee_to_instructions(&mut instructions, "settlement")
                    .await?;
            }

            let signature = self
                .blockchain
                .execute_batched_instructions(&[&platform_authority], instructions)
                .await
                .map_err(|e| {
                    ApiError::Internal(format!("Batch settlement execution failed: {e}"))
                })?;

            // Poll until confirmed — if it doesn't land, return Err so the caller resets the
            // batch for retry instead of marking it Completed (silent loss).
            match self
                .blockchain
                .wait_for_confirmation(&signature, confirm_timeout)
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    return Err(ApiError::Internal(format!(
                        "settlement batch {signature} not confirmed within {confirm_timeout}s; releasing for retry"
                    )))
                }
                Err(e) => {
                    return Err(ApiError::Internal(format!(
                        "settlement batch {signature} confirmation check failed: {e}; releasing for retry"
                    )))
                }
            }

            let slot = self.blockchain.get_slot().await.unwrap_or(0);
            for (_, id) in group {
                tx_results.push(SettlementTransaction {
                    settlement_id: *id,
                    signature: signature.to_string(),
                    slot,
                    confirmation_status: "confirmed".to_string(),
                });
            }
        }

        Ok(tx_results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trading_core::config::SolanaProgramsConfig;

    fn programs() -> SolanaProgramsConfig {
        SolanaProgramsConfig {
            registry_program_id: "Registry111111111111111111111111111111111".to_string(),
            oracle_program_id: "Oracle1111111111111111111111111111111111".to_string(),
            energy_token_program_id: "Energy1111111111111111111111111111111".to_string(),
            governance_program_id: "Gov11111111111111111111111111111111111".to_string(),
            treasury_program_id: "Treasury11111111111111111111111111111111".to_string(),
            trading_program_id: "Trade1111111111111111111111111111111111".to_string(),
            trading_market_id: "Market1111111111111111111111111111111111".to_string(),
        }
    }

    fn classic_ata(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
        spl_associated_token_account::get_associated_token_address_with_program_id(
            owner,
            mint,
            &spl_token::id(),
        )
    }

    fn token_2022_ata(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
        spl_associated_token_account::get_associated_token_address_with_program_id(
            owner,
            mint,
            &spl_token_2022::id(),
        )
    }

    /// Regression guard for the currency-ATA bug: `derive_trade_atas` must derive
    /// currency (GRX) ATAs under the **classic** SPL Token program and energy
    /// (GRID) ATAs under **Token-2022**. Deriving the classic currency mint under
    /// Token-2022 (the original bug) produced unfunded addresses that broke the
    /// atomic swap once `TRADE_SETTLEMENT_ENABLED=true`.
    #[tokio::test]
    async fn derive_trade_atas_picks_token_program_per_mint() {
        std::env::set_var("CHAIN_BRIDGE_INSECURE", "true");

        let blockchain = Arc::new(
            BlockchainService::new(
                "http://localhost:8899".to_string(),
                "localnet".to_string(),
                programs(),
                None,
                None,
            )
            .await
            .expect("construct BlockchainService offline"),
        );
        let provider = BlockchainSettlementProvider::new(blockchain);

        let energy_mint = Pubkey::new_unique();
        let currency_mint = Pubkey::new_unique();
        let buyer = Pubkey::new_unique();
        let seller = Pubkey::new_unique();

        let atas = provider
            .derive_trade_atas(&energy_mint, &currency_mint, &buyer, &seller)
            .expect("derive trade ATAs");

        // Currency ATAs use the classic SPL Token program.
        assert_eq!(atas.buyer_currency_ata, classic_ata(&buyer, &currency_mint));
        assert_eq!(
            atas.seller_currency_ata,
            classic_ata(&seller, &currency_mint)
        );
        // Energy ATAs use the Token-2022 program.
        assert_eq!(
            atas.seller_energy_ata,
            token_2022_ata(&seller, &energy_mint)
        );
        assert_eq!(atas.buyer_energy_ata, token_2022_ata(&buyer, &energy_mint));

        // The bug: currency ATAs must NOT match the Token-2022 derivation.
        assert_ne!(
            atas.buyer_currency_ata,
            token_2022_ata(&buyer, &currency_mint),
            "currency ATA must not regress to Token-2022 derivation"
        );
        assert_ne!(
            atas.seller_currency_ata,
            token_2022_ata(&seller, &currency_mint),
            "currency ATA must not regress to Token-2022 derivation"
        );
    }
}
