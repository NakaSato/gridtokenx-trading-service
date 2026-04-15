use anyhow::{anyhow, Result};
use solana_sdk::sysvar::{clock, instructions as instructions_sysvar};
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};
use std::str::FromStr;

// System program ID constant
const SYSTEM_PROGRAM_ID: &str = "11111111111111111111111111111111";

/// Program IDs (localnet) — keep in sync with `gridtokenx-anchor/Anchor.toml`
pub const REGISTRY_PROGRAM_ID: &str = "7JsfJPuJvhkY376RAzQExbdFbZMgdGc2cWLic25SE1tq";
pub const ORACLE_PROGRAM_ID: &str = "9XqNt1FqeKyhh4jBaagBSDUpJSMJhEy5gi8E5xx2RaeY";
pub const GOVERNANCE_PROGRAM_ID: &str = "Czz3aK3CmJfTVJJYDkuu3DcCGfWmuBruC4gbKTqDeq9x";
pub const ENERGY_TOKEN_PROGRAM_ID: &str = "FC28Av9roMDjx5PHH7GkSQQB6qo1vi4jsXR4ymiaV4CW";
pub const TRADING_PROGRAM_ID: &str = "HHAG2cG6sGHTWFwiEh1HBgfqZJWBbnsYzv4f5KtHavUr";
pub const TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
pub const ASSOCIATED_TOKEN_PROGRAM_ID: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";

/// Payload for off-chain orders
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct OffchainOrderPayload {
    pub order_id: [u8; 16], // UUID bytes
    pub user: Pubkey,
    pub energy_amount: u64,
    pub price_per_kwh: u64,
    pub side: u8, // 0 = Buy, 1 = Sell
    pub zone_id: u32,
    pub expires_at: i64,
}

/// Instruction builder for Solana programs
#[derive(Clone, Debug)]
pub struct InstructionBuilder {
    payer: Pubkey,
}

impl InstructionBuilder {
    pub fn new(payer: Pubkey) -> Self {
        Self { payer }
    }

    pub fn payer(&self) -> Pubkey {
        self.payer
    }

    /// Get market PDA
    pub fn get_market_pda(&self) -> Result<Pubkey> {
        let program_id = Pubkey::from_str(TRADING_PROGRAM_ID)?;
        let (market_pda, _) = Pubkey::find_program_address(&[b"market"], &program_id);
        Ok(market_pda)
    }

    /// Get registry PDA
    pub fn get_registry_pda(&self) -> Result<Pubkey> {
        let program_id = Pubkey::from_str(REGISTRY_PROGRAM_ID)?;
        let (registry_pda, _) = Pubkey::find_program_address(&[b"registry"], &program_id);
        Ok(registry_pda)
    }

    /// Build instruction for creating energy trade order
    pub fn build_create_order_instruction(
        &self,
        market_pubkey: &Pubkey,
        _authority: &Pubkey,
        order_pda: Pubkey,
        order_id_val: u64,
        energy_amount: u64,
        price_per_kwh: u64,
        order_type: &str,
        erc_certificate_id: Option<&str>,
        payer: Pubkey,
        zone_id: u32,
    ) -> Result<Instruction> {
        // Parse program and market pubkeys
        let program_id = Pubkey::from_str(TRADING_PROGRAM_ID)?;
        // market_pubkey is already Pubkey

        // Find ERC certificate account if provided
        let erc_certificate = if let Some(cert_id) = erc_certificate_id {
            Some(self.get_erc_certificate_pubkey(cert_id)?)
        } else {
            None
        };

        // Build accounts array
        let system_program = Pubkey::from_str(SYSTEM_PROGRAM_ID)?;

        // Derive zone_market PDA
        let (zone_market_pda, _) = Pubkey::find_program_address(
            &[
                b"zone_market",
                market_pubkey.as_ref(),
                &zone_id.to_le_bytes(),
            ],
            &program_id,
        );

        // Get governance config PDA
        let governance_config = self.get_poa_config_pubkey()?;

        let accounts = if order_type == "sell" {
            // Sell orders have an optional ERC certificate account at index 3
            let erc_key = erc_certificate.unwrap_or(program_id);
            vec![
                AccountMeta::new_readonly(*market_pubkey, false),
                AccountMeta::new(zone_market_pda, false),
                AccountMeta::new(order_pda, false),
                AccountMeta::new_readonly(erc_key, false),
                AccountMeta::new(payer, true), // Payer must be writable to pay for rent
                AccountMeta::new_readonly(system_program, false),
                AccountMeta::new_readonly(governance_config, false),
            ]
        } else {
            // Buy orders do NOT have the ERC certificate account
            // IDL: market, zoneMarket, order, authority, systemProgram, governanceConfig
            vec![
                AccountMeta::new_readonly(*market_pubkey, false),
                AccountMeta::new(zone_market_pda, false),
                AccountMeta::new(order_pda, false),
                AccountMeta::new(payer, true), // Payer must be writable to pay for rent
                AccountMeta::new_readonly(system_program, false),
                AccountMeta::new_readonly(governance_config, false),
            ]
        };

        // Build instruction data
        let mut data = Vec::new();

        // Add instruction discriminator based on order type (Anchor uses 8-byte sha256("global:<name>"))
        if order_type == "sell" {
            // createSellOrder discriminator: [53, 52, 255, 44, 191, 74, 171, 225]
            data.extend_from_slice(&[53, 52, 255, 44, 191, 74, 171, 225]);
        } else {
            // createBuyOrder discriminator: [182, 87, 0, 160, 192, 66, 151, 130]
            data.extend_from_slice(&[182, 87, 0, 160, 192, 66, 151, 130]);
        }

        // Add parameters (Anchor IDL order: order_id_val, energy_amount, price_per_kwh)
        data.extend_from_slice(&order_id_val.to_le_bytes());
        data.extend_from_slice(&energy_amount.to_le_bytes());
        data.extend_from_slice(&price_per_kwh.to_le_bytes());

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
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
        // Parse pubkeys
        let program_id = Pubkey::from_str(TRADING_PROGRAM_ID)?;
        let market = Pubkey::from_str(market_pubkey)?;
        let buy_order = Pubkey::from_str(buy_order_pubkey)?;
        let sell_order = Pubkey::from_str(sell_order_pubkey)?;

        // Derive zone_market PDA
        let (zone_market_pda, _) = Pubkey::find_program_address(
            &[b"zone_market", market.as_ref(), &zone_id.to_le_bytes()],
            &program_id,
        );

        // Build accounts array
        let accounts = vec![
            AccountMeta::new(market, false),
            AccountMeta::new(zone_market_pda, false),
            AccountMeta::new(buy_order, false),
            AccountMeta::new(sell_order, false),
            AccountMeta::new(trade_record_pubkey, false), // PDA doesn't sign, Anchor verifies seeds
            AccountMeta::new(self.payer, true), // Changed to mut - payer pays for trade_record init
            AccountMeta::new_readonly(Pubkey::from_str(SYSTEM_PROGRAM_ID)?, false),
        ];

        // Build instruction data
        let mut data = Vec::new();
        // MatchOrders discriminator: [17, 1, 201, 93, 7, 51, 251, 134]
        data.extend_from_slice(&[17, 1, 201, 93, 7, 51, 251, 134]);
        data.extend_from_slice(&match_amount.to_le_bytes());

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Build instruction for minting GRX tokens to a wallet (Anchor: mint_to_wallet)
    pub fn build_mint_instruction(&self, recipient: &str, amount: u64) -> Result<Instruction> {
        let program_id = Pubkey::from_str(ENERGY_TOKEN_PROGRAM_ID)?;
        // recipient is the wallet owner, we need the ATA or use the helper in the contract
        // In the updated contract, mint_to_wallet handles ATA derivation or use-if-exists
        let recipient_pubkey = Pubkey::from_str(recipient)?;
        let mint_pubkey = self.get_token_mint_pubkey()?;
        let token_info = self.get_token_info_pda()?; // Helper to get PDA
        
        let token_program = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
        let associated_token_program = Pubkey::from_str("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL").unwrap();
        let system_program = Pubkey::from_str("11111111111111111111111111111111").unwrap();

        // We need the destination ATA. For simplicity here, we assume it's provided or derived
        // In Anchor 1.0.0, we use the account list defined in energy_token.json
        let (destination_ata, _) = Pubkey::find_program_address(
            &[recipient_pubkey.as_ref(), token_program.as_ref(), mint_pubkey.as_ref()],
            &associated_token_program
        );

        let accounts = vec![
            AccountMeta::new(mint_pubkey, false),
            AccountMeta::new_readonly(token_info, false),
            AccountMeta::new(destination_ata, false),
            AccountMeta::new_readonly(recipient_pubkey, false),
            AccountMeta::new_readonly(self.payer, true), // authority
            AccountMeta::new(self.payer, true), // payer
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new_readonly(associated_token_program, false),
            AccountMeta::new_readonly(system_program, false),
        ];

        let mut data = Vec::new();
        // mint_to_wallet discriminator: [17, 40, 71, 107, 142, 232, 163, 100]
        data.extend_from_slice(&[17, 40, 71, 107, 142, 232, 163, 100]);
        data.extend_from_slice(&amount.to_le_bytes());

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Build instruction for transferring tokens (Anchor: transfer_tokens)
    pub fn build_transfer_instruction(
        &self,
        from_ata: &str,
        to_ata: &str,
        amount: u64,
        token_mint: &str,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(ENERGY_TOKEN_PROGRAM_ID)?;
        let from_pubkey = Pubkey::from_str(from_ata)?;
        let to_pubkey = Pubkey::from_str(to_ata)?;
        let mint_pubkey = Pubkey::from_str(token_mint)?;
        let token_program = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();

        let accounts = vec![
            AccountMeta::new(from_pubkey, false),
            AccountMeta::new(to_pubkey, false),
            AccountMeta::new_readonly(mint_pubkey, false),
            AccountMeta::new_readonly(self.payer, true), // from_authority
            AccountMeta::new_readonly(token_program, false),
        ];

        let mut data = Vec::new();
        // transfer_tokens discriminator: [54, 180, 238, 175, 74, 85, 126, 188]
        data.extend_from_slice(&[54, 180, 238, 175, 74, 85, 126, 188]);
        data.extend_from_slice(&amount.to_le_bytes());

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    // Legacy/Unused instructions removed during Anchor 1.0.0 synchronization
    // build_vote_instruction, build_update_price_instruction, build_update_registry_instruction
    // are no longer compatible with the 1.0.0 smart contracts and were found to be dead code.

    /// Build instruction for initializing the registry
    pub fn build_initialize_registry_instruction(&self) -> Result<Instruction> {
        let program_id = Pubkey::from_str(REGISTRY_PROGRAM_ID)?;
        let system_program = Pubkey::from_str(SYSTEM_PROGRAM_ID)?;

        // Find registry PDA: seeds = ["registry"]
        let (registry_pda, _bump) = Pubkey::find_program_address(&[b"registry"], &program_id);

        let accounts = vec![
            AccountMeta::new(registry_pda, false),
            AccountMeta::new(self.payer, true),
            AccountMeta::new_readonly(system_program, false),
        ];

        // initialize discriminator: [175, 175, 109, 31, 13, 152, 155, 237]
        let mut data = Vec::new();
        data.extend_from_slice(&[175, 175, 109, 31, 13, 152, 155, 237]);

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Build instruction for initializing the oracle
    pub fn build_initialize_oracle_instruction(&self, api_gateway: &Pubkey) -> Result<Instruction> {
        let program_id = Pubkey::from_str(ORACLE_PROGRAM_ID)?;
        let system_program = Pubkey::from_str(SYSTEM_PROGRAM_ID)?;

        // Find oracle_data PDA: seeds = ["oracle_data"]
        let (oracle_data_pda, _bump) = Pubkey::find_program_address(&[b"oracle_data"], &program_id);

        let accounts = vec![
            AccountMeta::new(oracle_data_pda, false),
            AccountMeta::new(self.payer, true),
            AccountMeta::new_readonly(system_program, false),
        ];

        // initialize discriminator: [175, 175, 109, 31, 13, 152, 155, 237]
        let mut data = Vec::new();
        data.extend_from_slice(&[175, 175, 109, 31, 13, 152, 155, 237]);
        data.extend_from_slice(api_gateway.as_ref());

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Build instruction for initializing the governance (PoA)
    pub fn build_initialize_governance_instruction(&self) -> Result<Instruction> {
        let program_id = Pubkey::from_str(GOVERNANCE_PROGRAM_ID)?;
        let system_program = Pubkey::from_str(SYSTEM_PROGRAM_ID)?;

        // Find poa_config PDA: seeds = ["poa_config"]
        let (poa_config_pda, _bump) = Pubkey::find_program_address(&[b"poa_config"], &program_id);

        let accounts = vec![
            AccountMeta::new(poa_config_pda, false),
            AccountMeta::new(self.payer, true),
            AccountMeta::new_readonly(system_program, false),
        ];

        // initialize_poa discriminator: [98, 199, 82, 10, 244, 161, 157, 46]
        let mut data = Vec::new();
        data.extend_from_slice(&[98, 199, 82, 10, 244, 161, 157, 46]);

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Build instruction for issuing an ERC certificate
    pub fn build_issue_erc_instruction(
        &self,
        certificate_id: &str,
        _user_wallet: &Pubkey,
        meter_account: &Pubkey,
        energy_amount: u64,
        renewable_source: &str,
        validation_data: &str,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(GOVERNANCE_PROGRAM_ID)?;
        let system_program = Pubkey::from_str(SYSTEM_PROGRAM_ID)?;

        // Find poa_config PDA: seeds = ["poa_config"]
        let (poa_config_pda, _) = Pubkey::find_program_address(&[b"poa_config"], &program_id);

        // Find erc_certificate PDA: seeds = ["erc_certificate", certificate_id]
        let (erc_certificate_pda, _) = Pubkey::find_program_address(
            &[b"erc_certificate", certificate_id.as_bytes()],
            &program_id,
        );

        let accounts = vec![
            AccountMeta::new(poa_config_pda, false),
            AccountMeta::new(erc_certificate_pda, false),
            AccountMeta::new(*meter_account, false),
            AccountMeta::new(self.payer, true),
            AccountMeta::new_readonly(system_program, false),
        ];

        // issue_erc discriminator: [174, 248, 149, 107, 155, 4, 196, 8]
        let mut data = Vec::new();
        data.extend_from_slice(&[174, 248, 149, 107, 155, 4, 196, 8]);

        // Args: certificate_id (String), energy_amount (u64), renewable_source (String), validation_data (String)
        let write_string = |d: &mut Vec<u8>, s: &str| {
            let bytes = s.as_bytes();
            d.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            d.extend_from_slice(bytes);
        };

        write_string(&mut data, certificate_id);
        data.extend_from_slice(&energy_amount.to_le_bytes());
        write_string(&mut data, renewable_source);
        write_string(&mut data, validation_data);

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Build instruction for transferring an ERC certificate
    pub fn build_transfer_erc_instruction(
        &self,
        certificate_id: &str,
        owner: &Pubkey,
        new_owner: &Pubkey,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(GOVERNANCE_PROGRAM_ID)?;

        // Find poa_config PDA
        let (poa_config_pda, _) = Pubkey::find_program_address(&[b"poa_config"], &program_id);

        // Find erc_certificate PDA
        let (erc_certificate_pda, _) = Pubkey::find_program_address(
            &[b"erc_certificate", certificate_id.as_bytes()],
            &program_id,
        );

        let accounts = vec![
            AccountMeta::new(poa_config_pda, false),
            AccountMeta::new(erc_certificate_pda, false),
            AccountMeta::new(*owner, true),
            AccountMeta::new_readonly(*new_owner, false),
        ];

        // transfer_erc discriminator: [200, 15, 16, 13, 13, 143, 11, 11]
        let mut data = Vec::new();
        data.extend_from_slice(&[200, 15, 16, 13, 13, 143, 11, 11]);

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Build instruction for validating an ERC certificate for trading
    pub fn build_validate_erc_instruction(&self, certificate_id: &str) -> Result<Instruction> {
        let program_id = Pubkey::from_str(GOVERNANCE_PROGRAM_ID)?;

        // Find poa_config PDA
        let (poa_config_pda, _) = Pubkey::find_program_address(&[b"poa_config"], &program_id);

        // Find erc_certificate PDA
        let (erc_certificate_pda, _) = Pubkey::find_program_address(
            &[b"erc_certificate", certificate_id.as_bytes()],
            &program_id,
        );

        let accounts = vec![
            AccountMeta::new(poa_config_pda, false),
            AccountMeta::new(erc_certificate_pda, false),
            AccountMeta::new(self.payer, true),
        ];

        // validate_erc_for_trading discriminator: [9, 215, 176, 63, 247, 150, 72, 139]
        let mut data = Vec::new();
        data.extend_from_slice(&[9, 215, 176, 63, 247, 150, 72, 139]);

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Build instruction for updating governance configuration
    pub fn build_update_governance_config_instruction(
        &self,
        erc_validation_enabled: bool,
        allow_certificate_transfers: bool,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(GOVERNANCE_PROGRAM_ID)?;

        // Find poa_config PDA
        let (poa_config_pda, _) = Pubkey::find_program_address(&[b"poa_config"], &program_id);

        let accounts = vec![
            AccountMeta::new(poa_config_pda, false),
            AccountMeta::new(self.payer, true),
        ];

        // update_governance_config discriminator: [140, 45, 181, 17, 77, 67, 157, 248]
        let mut data = Vec::new();
        data.extend_from_slice(&[140, 45, 181, 17, 77, 67, 157, 248]);
        data.push(erc_validation_enabled as u8);
        data.push(allow_certificate_transfers as u8);

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Build instruction for revoking (retiring) an ERC certificate
    pub fn build_revoke_erc_instruction(
        &self,
        certificate_id: &str,
        reason: &str,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(GOVERNANCE_PROGRAM_ID)?;

        // Find poa_config PDA
        let (poa_config_pda, _) = Pubkey::find_program_address(&[b"poa_config"], &program_id);

        // Find erc_certificate PDA
        let (erc_certificate_pda, _) = Pubkey::find_program_address(
            &[b"erc_certificate", certificate_id.as_bytes()],
            &program_id,
        );

        let accounts = vec![
            AccountMeta::new(poa_config_pda, false),
            AccountMeta::new(erc_certificate_pda, false),
            AccountMeta::new(self.payer, true),
        ];

        // revoke_erc discriminator: [16, 48, 113, 85, 118, 70, 185, 150]
        let mut data = Vec::new();
        data.extend_from_slice(&[16, 48, 113, 85, 118, 70, 185, 150]);

        // Arg: reason (String)
        let bytes = reason.as_bytes();
        data.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        data.extend_from_slice(bytes);

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Build instruction for updating market depth on-chain
    pub fn build_update_depth_instruction(
        &self,
        market_pubkey: &Pubkey,
        zone_id: u32,
        buy_prices: Vec<u64>,
        buy_amounts: Vec<u64>,
        sell_prices: Vec<u64>,
        sell_amounts: Vec<u64>,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(TRADING_PROGRAM_ID)?;
        let gov_program_id = Pubkey::from_str(GOVERNANCE_PROGRAM_ID)?;

        let (zone_market_pda, _) = Pubkey::find_program_address(
            &[
                b"zone_market",
                market_pubkey.as_ref(),
                &zone_id.to_le_bytes(),
            ],
            &program_id,
        );

        let (poa_config_pda, _) = Pubkey::find_program_address(&[b"poa_config"], &gov_program_id);

        let accounts = vec![
            AccountMeta::new(*market_pubkey, false),
            AccountMeta::new(zone_market_pda, false),
            AccountMeta::new(self.payer, true),
            AccountMeta::new_readonly(poa_config_pda, false),
        ];

        let mut data = Vec::new();
        // UpdateDepth discriminator: [181, 172, 219, 119, 18, 167, 28, 168]
        data.extend_from_slice(&[181, 172, 219, 119, 18, 167, 28, 168]);

        // Borsh Vec serialization
        let write_vec_u64 = |d: &mut Vec<u8>, v: &[u64]| {
            d.extend_from_slice(&(v.len() as u32).to_le_bytes());
            for val in v {
                d.extend_from_slice(&val.to_le_bytes());
            }
        };

        write_vec_u64(&mut data, &buy_prices);
        write_vec_u64(&mut data, &buy_amounts);
        write_vec_u64(&mut data, &sell_prices);
        write_vec_u64(&mut data, &sell_amounts);

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Build instruction for updating market price history on-chain
    pub fn build_update_price_history_instruction(
        &self,
        market_pubkey: &Pubkey,
        trade_price: u64,
        trade_volume: u64,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(TRADING_PROGRAM_ID)?;
        let gov_program_id = Pubkey::from_str(GOVERNANCE_PROGRAM_ID)?;

        let (poa_config_pda, _) = Pubkey::find_program_address(&[b"poa_config"], &gov_program_id);

        let accounts = vec![
            AccountMeta::new(*market_pubkey, false),
            AccountMeta::new(self.payer, true),
            AccountMeta::new_readonly(poa_config_pda, false),
        ];

        let mut data = Vec::new();
        // UpdatePriceHistory discriminator: [23, 154, 255, 160, 30, 165, 151, 70]
        data.extend_from_slice(&[23, 154, 255, 160, 30, 165, 151, 70]);
        data.extend_from_slice(&trade_price.to_le_bytes());
        data.extend_from_slice(&trade_volume.to_le_bytes());

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    // Helper methods

    /// Get ERC certificate pubkey from certificate ID
    pub fn get_erc_certificate_pubkey(&self, certificate_id: &str) -> Result<Pubkey> {
        let (certificate_pubkey, _) = Pubkey::find_program_address(
            &[b"erc_certificate", certificate_id.as_bytes()],
            &Pubkey::from_str(GOVERNANCE_PROGRAM_ID)?,
        );

        Ok(certificate_pubkey)
    }

    /// Get token mint pubkey
    fn get_token_mint_pubkey(&self) -> Result<Pubkey> {
        let mint_str = std::env::var("ENERGY_TOKEN_MINT")
            .map_err(|e| anyhow!("ENERGY_TOKEN_MINT not set: {}", e))?;

        Pubkey::from_str(&mint_str).map_err(|e| anyhow!("Failed to parse token mint pubkey: {}", e))
    }

    /// Get proposal account pubkey from proposal ID
    fn get_proposal_account_pubkey(&self, proposal_id: u64) -> Result<Pubkey> {
        let (proposal_pubkey, _) = Pubkey::find_program_address(
            &[b"proposal", &proposal_id.to_le_bytes()],
            &Pubkey::from_str(GOVERNANCE_PROGRAM_ID)?,
        );

        Ok(proposal_pubkey)
    }

    /// Get price feed account pubkey from price feed ID
    fn get_price_feed_account_pubkey(&self, price_feed_id: &str) -> Result<Pubkey> {
        let (price_feed_pubkey, _) = Pubkey::find_program_address(
            &[b"price_feed", price_feed_id.as_bytes()],
            &Pubkey::from_str(ORACLE_PROGRAM_ID)?,
        );

        Ok(price_feed_pubkey)
    }

    /// Get participant account pubkey from participant ID
    fn get_participant_account_pubkey(&self, participant_id: &str) -> Result<Pubkey> {
        let (participant_pubkey, _) = Pubkey::find_program_address(
            &[b"participant", participant_id.as_bytes()],
            &Pubkey::from_str(REGISTRY_PROGRAM_ID)?,
        );

        Ok(participant_pubkey)
    }

    /// Build instruction for registering a user in the registry program
    /// This creates an on-chain PDA account at ["user", user_authority]
    pub fn build_register_user_instruction(
        &self,
        user_authority: &Pubkey,
        registry: &Pubkey,
        user_type: u8,
        lat_e7: i32,
        long_e7: i32,
        h3_index: u64,
        // Optional airdrop accounts
        energy_token_program: Option<Pubkey>,
        mint: Option<Pubkey>,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(REGISTRY_PROGRAM_ID)?;
        let system_program = Pubkey::from_str(SYSTEM_PROGRAM_ID)?;
        let token_program = Pubkey::from_str(TOKEN_2022_PROGRAM_ID)?;
        let associated_token_program = Pubkey::from_str(ASSOCIATED_TOKEN_PROGRAM_ID)?;

        // Find user account PDA: seeds = ["user", user_authority]
        let (user_account_pda, _bump) =
            Pubkey::find_program_address(&[b"user", user_authority.as_ref()], &program_id);

        // Find optional airdrop PDAs
        // Sentinel matching Program Logic (lib.rs:645)
        let energy_program = energy_token_program.unwrap_or(program_id);

        let (token_mint, token_info_pda, user_token_account) = if energy_token_program.is_some() {
            let mint_key = mint.unwrap_or_default();
            let (info_pda, _) =
                Pubkey::find_program_address(&[b"token_info_2022"], &energy_program);
            let (ata_pda, _) = Pubkey::find_program_address(
                &[
                    user_authority.as_ref(),
                    token_program.as_ref(), // token-2022
                    mint_key.as_ref(),
                ],
                &associated_token_program,
            );
            (mint_key, info_pda, ata_pda)
        } else {
            // For optional mut accounts, use registry PDA as dummy to satisfy Anchor's mut constraint
            // since it is already marked as mut in the accounts vector.
            (*registry, *registry, *registry)
        };

        // Find registry shard PDA: seeds = [b"registry_shard", [shard_id]]
        let shard_id = 0; // Default shard for now
        let (shard_pda, _) = Pubkey::find_program_address(&[b"registry_shard", &[shard_id]], &program_id);

        // Build accounts array matching RegisterUser struct
        let accounts = vec![
            AccountMeta::new(user_account_pda, false), // user_account (init, mut)
            AccountMeta::new(shard_pda, false),        // registry_shard (mut)
            AccountMeta::new(*registry, false),        // registry (mut)
            AccountMeta::new(*user_authority, false),  // authority (NOT a signer in 1.0.0 Registry)
            AccountMeta::new(payer, true),             // payer (signer, mut)
            AccountMeta::new_readonly(energy_program, false), // energy_token_program
            AccountMeta::new(token_mint, false),       // mint (mut)
            AccountMeta::new(user_token_account, false), // user_token_account (mut)
            AccountMeta::new_readonly(token_info_pda, false), // token_info
            AccountMeta::new_readonly(token_program, false), // token_program
            AccountMeta::new_readonly(system_program, false), // system_program
        ];

        // Build instruction data
        let mut data = Vec::new();

        // register_user discriminator: [2, 241, 150, 223, 99, 214, 116, 97]
        data.extend_from_slice(&[2, 241, 150, 223, 99, 214, 116, 97]);

        // Args
        data.push(user_type);
        data.extend_from_slice(&lat_e7.to_le_bytes());
        data.extend_from_slice(&long_e7.to_le_bytes());
        data.extend_from_slice(&h3_index.to_le_bytes());
        data.push(0); // shard_id (u8)

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Build instruction for registering a meter
    pub fn build_register_meter_instruction(
        &self,
        owner: &Pubkey,
        registry: &Pubkey,
        meter_id: &str,
        meter_type: u8,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(REGISTRY_PROGRAM_ID)?;
        let system_program = Pubkey::from_str(SYSTEM_PROGRAM_ID)?;

        // Find user account PDA
        let (user_account_pda, _) =
            Pubkey::find_program_address(&[b"user", owner.as_ref()], &program_id);

        // Find meter account PDA: seeds = [b"meter", owner, meter_id]
        let (meter_account_pda, _) = Pubkey::find_program_address(
            &[b"meter", owner.as_ref(), meter_id.as_bytes()],
            &program_id,
        );

        // Find registry shard PDA
        let shard_id = 0; // Default shard
        let (shard_pda, _) = Pubkey::find_program_address(&[b"registry_shard", &[shard_id]], &program_id);

        let accounts = vec![
            AccountMeta::new(meter_account_pda, false),
            AccountMeta::new(user_account_pda, false),
            AccountMeta::new(shard_pda, false),
            AccountMeta::new(*registry, false),
            AccountMeta::new(*owner, true),
            AccountMeta::new_readonly(system_program, false),
        ];

        // register_meter discriminator: [49, 106, 87, 72, 138, 214, 224, 125]
        let mut data = Vec::new();
        data.extend_from_slice(&[49, 106, 87, 72, 138, 214, 224, 125]);

        // String arg: (u32 len) + bytes
        let meter_id_bytes = meter_id.as_bytes();
        data.extend_from_slice(&(meter_id_bytes.len() as u32).to_le_bytes());
        data.extend_from_slice(meter_id_bytes);

        // u8 arg
        data.push(meter_type);

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Get user account PDA from user authority
    pub fn get_user_account_pda(&self, user_authority: &Pubkey) -> Result<Pubkey> {
        let program_id = Pubkey::from_str(REGISTRY_PROGRAM_ID)?;
        let (user_account_pda, _) =
            Pubkey::find_program_address(&[b"user", user_authority.as_ref()], &program_id);
        Ok(user_account_pda)
    }

    /// Build instruction for initializing the Energy Token program
    pub fn build_initialize_energy_token_instruction(
        &self,
        authority: Pubkey,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(ENERGY_TOKEN_PROGRAM_ID)?;
        let system_program = Pubkey::from_str(SYSTEM_PROGRAM_ID)?;
        let token_program = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")?; // Token-2022
        let rent = solana_sdk::sysvar::rent::ID;

        // PDAs
        let (token_info_pda, _) = Pubkey::find_program_address(&[b"token_info_2022"], &program_id);
        let (mint_pda, _) = Pubkey::find_program_address(&[b"mint_2022"], &program_id);

        let accounts = vec![
            AccountMeta::new(token_info_pda, false),
            AccountMeta::new(mint_pda, false),
            AccountMeta::new(authority, true), // authority used as payer
            AccountMeta::new_readonly(system_program, false),
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new_readonly(rent, false),
        ];

        // Discriminator for "initialize_token" (from IDL: [38, 209, 150, 50, 190, 117, 16, 54])
        let data = vec![38, 209, 150, 50, 190, 117, 16, 54];

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Build instruction for settling an off-chain matched trade
    pub fn build_settle_offchain_match_instruction(
        &self,
        market_pubkey: &Pubkey,
        buyer_payload: &OffchainOrderPayload,
        seller_payload: &OffchainOrderPayload,
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
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(TRADING_PROGRAM_ID)?;
        let system_program = Pubkey::from_str(SYSTEM_PROGRAM_ID)?;
        let token_program = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")?; // SPL Token
        let token_2022_program = Pubkey::from_str(TOKEN_2022_PROGRAM_ID)?;

        // Derive PDAs
        let (zone_market_pda, _) = Pubkey::find_program_address(
            &[
                b"zone_market",
                market_pubkey.as_ref(),
                &buyer_payload.zone_id.to_le_bytes(),
            ],
            &program_id,
        );

        let (buyer_nullifier_pda, _) = Pubkey::find_program_address(
            &[
                b"nullifier",
                buyer_payload.user.as_ref(),
                &buyer_payload.order_id,
            ],
            &program_id,
        );

        let (seller_nullifier_pda, _) = Pubkey::find_program_address(
            &[
                b"nullifier",
                seller_payload.user.as_ref(),
                &seller_payload.order_id,
            ],
            &program_id,
        );

        let (market_authority_pda, _) =
            Pubkey::find_program_address(&[b"market_authority"], &program_id);

        // Build account array (Order must match Anchor's SettleOffchainMatchContext)
        let accounts = vec![
            AccountMeta::new(*market_pubkey, false),
            AccountMeta::new(zone_market_pda, false),
            AccountMeta::new(buyer_nullifier_pda, false),
            AccountMeta::new(seller_nullifier_pda, false),
            AccountMeta::new(*buyer_currency_ata, false),
            AccountMeta::new(*seller_currency_ata, false),
            AccountMeta::new(*seller_energy_ata, false),
            AccountMeta::new(*buyer_energy_ata, false),
            AccountMeta::new(*fee_collector_ata, false),
            AccountMeta::new(*wheeling_collector_ata, false),
            AccountMeta::new(*loss_collector_ata, false),
            AccountMeta::new_readonly(*currency_mint, false),
            AccountMeta::new_readonly(*energy_mint, false),
            AccountMeta::new_readonly(market_authority_pda, false),
            AccountMeta::new(self.payer, true),
            AccountMeta::new_readonly(instructions_sysvar::id(), false),
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new_readonly(token_2022_program, false),
            AccountMeta::new_readonly(system_program, false),
        ];

        // Build data
        let mut data = Vec::new();
        // SettleOffchainMatch discriminator: [140, 170, 63, 151, 81, 62, 212, 11]
        data.extend_from_slice(&[140, 170, 63, 151, 81, 62, 212, 11]);

        // Helper to serialize payload
        let serialize_payload = |p: &OffchainOrderPayload| {
            let mut buf = Vec::new();
            buf.extend_from_slice(&p.order_id);
            buf.extend_from_slice(p.user.as_ref());
            buf.extend_from_slice(&p.energy_amount.to_le_bytes());
            buf.extend_from_slice(&p.price_per_kwh.to_le_bytes());
            buf.push(p.side);
            buf.extend_from_slice(&p.zone_id.to_le_bytes());
            buf.extend_from_slice(&p.expires_at.to_le_bytes());
            buf
        };

        data.extend(serialize_payload(buyer_payload));
        data.extend(serialize_payload(seller_payload));
        data.extend_from_slice(&match_amount.to_le_bytes());
        data.extend_from_slice(&match_price.to_le_bytes());
        data.extend_from_slice(&wheeling_charge.to_le_bytes());
        data.extend_from_slice(&loss_cost.to_le_bytes());

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Build instruction for initializing the Trading Market
    pub fn build_initialize_market_instruction(
        &self,
        authority: Pubkey,
        num_shards: u8,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(TRADING_PROGRAM_ID)?;
        let system_program = Pubkey::from_str(SYSTEM_PROGRAM_ID)?;

        // Find market PDA: seeds = ["market"]
        let (market_pda, _) = Pubkey::find_program_address(&[b"market"], &program_id);

        let accounts = vec![
            AccountMeta::new(market_pda, false),
            AccountMeta::new(authority, true),
            AccountMeta::new_readonly(system_program, false),
        ];

        // discriminator: [35, 35, 189, 193, 155, 48, 170, 203]
        let mut data = vec![35, 35, 189, 193, 155, 48, 170, 203];
        data.push(num_shards);

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Build instruction for initializing a Zone Market
    pub fn build_initialize_zone_market_instruction(
        &self,
        market: Pubkey,
        authority: Pubkey,
        zone_id: u32,
        num_shards: u8,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(TRADING_PROGRAM_ID)?;
        let system_program = Pubkey::from_str(SYSTEM_PROGRAM_ID)?;

        // Derive zone_market PDA
        let (zone_market_pda, _) = Pubkey::find_program_address(
            &[b"zone_market", market.as_ref(), &zone_id.to_le_bytes()],
            &program_id,
        );

        let accounts = vec![
            AccountMeta::new_readonly(market, false),
            AccountMeta::new(zone_market_pda, false),
            AccountMeta::new(authority, true),
            AccountMeta::new_readonly(system_program, false),
        ];

        // discriminator: [185, 121, 4, 197, 200, 69, 32, 201]
        let mut data = vec![185, 121, 4, 197, 200, 69, 32, 201];
        data.extend_from_slice(&zone_id.to_le_bytes());
        data.push(num_shards);

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Build instruction for executing atomic settlement
    pub fn build_execute_atomic_settlement_instruction(
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
        let program_id = Pubkey::from_str(TRADING_PROGRAM_ID)?;
        let system_program = Pubkey::from_str(SYSTEM_PROGRAM_ID)?;
        let token_program = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")?; // Token-2022
        let secondary_token_program =
            Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")?; // SPL Token
        let governance_config = self.get_poa_config_pubkey()?;

        let accounts = vec![
            AccountMeta::new(market, false),
            AccountMeta::new(buy_order, false),
            AccountMeta::new(sell_order, false),
            AccountMeta::new(buyer_currency_escrow, false),
            AccountMeta::new(seller_energy_escrow, false),
            AccountMeta::new(seller_currency_account, false),
            AccountMeta::new(buyer_energy_account, false),
            AccountMeta::new(fee_collector, false),
            AccountMeta::new(wheeling_collector, false),
            AccountMeta::new(loss_collector, false),
            AccountMeta::new_readonly(energy_mint, false),
            AccountMeta::new_readonly(currency_mint, false),
            AccountMeta::new_readonly(escrow_authority, true),
            AccountMeta::new_readonly(market_authority, true),
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new_readonly(system_program, false),
            AccountMeta::new_readonly(secondary_token_program, false),
            AccountMeta::new_readonly(governance_config, false),
        ];

        let mut data = Vec::new();
        // discriminator: [86, 216, 13, 114, 76, 114, 212, 11]
        data.extend_from_slice(&[86, 216, 13, 114, 76, 114, 212, 11]);
        data.extend_from_slice(&amount.to_le_bytes());
        data.extend_from_slice(&price.to_le_bytes());
        data.extend_from_slice(&wheeling_charge.to_le_bytes());
        data.extend_from_slice(&loss_cost.to_le_bytes());

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Build instruction for initializing a registry shard
    pub fn build_initialize_shard_instruction(&self, shard_id: u8) -> Result<Instruction> {
        let program_id = Pubkey::from_str(REGISTRY_PROGRAM_ID)?;
        let system_program = Pubkey::from_str(SYSTEM_PROGRAM_ID)?;

        // Find shard PDA: seeds = ["registry_shard", [shard_id]]
        let (shard_pda, _bump) =
            Pubkey::find_program_address(&[b"registry_shard", &[shard_id]], &program_id);

        let accounts = vec![
            AccountMeta::new(shard_pda, false),
            AccountMeta::new(self.payer, true),
            AccountMeta::new_readonly(system_program, false),
        ];

        // initialize_shard discriminator: [100, 96, 88, 58, 225, 178, 9, 147]
        let mut data = Vec::new();
        data.extend_from_slice(&[100, 96, 88, 58, 225, 178, 9, 147]);
        data.push(shard_id);

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Get token_info PDA
    fn get_token_info_pda(&self) -> Result<Pubkey> {
        let program_id = Pubkey::from_str(ENERGY_TOKEN_PROGRAM_ID)?;
        let (token_info_pda, _) = Pubkey::find_program_address(&[b"token_info"], &program_id);
        Ok(token_info_pda)
    }

    /// Get poa_config PDA
    fn get_poa_config_pubkey(&self) -> Result<Pubkey> {
        let (poa_config_pda, _) = Pubkey::find_program_address(
            &[b"poa_config"],
            &Pubkey::from_str(GOVERNANCE_PROGRAM_ID)?,
        );
        Ok(poa_config_pda)
    }
}

/// Program ID utilities
pub mod program_ids {
    use super::*;
    use anyhow::Result;

    /// Get Registry program ID
    pub fn registry_program_id() -> Result<Pubkey> {
        Pubkey::from_str(REGISTRY_PROGRAM_ID)
            .map_err(|e| anyhow!("Failed to parse registry program ID: {}", e))
    }

    /// Get Oracle program ID
    pub fn oracle_program_id() -> Result<Pubkey> {
        Pubkey::from_str(ORACLE_PROGRAM_ID)
            .map_err(|e| anyhow!("Failed to parse oracle program ID: {}", e))
    }

    /// Get Governance program ID
    pub fn governance_program_id() -> Result<Pubkey> {
        Pubkey::from_str(GOVERNANCE_PROGRAM_ID)
            .map_err(|e| anyhow!("Failed to parse governance program ID: {}", e))
    }

    /// Get Energy Token program ID
    pub fn energy_token_program_id() -> Result<Pubkey> {
        Pubkey::from_str(ENERGY_TOKEN_PROGRAM_ID)
            .map_err(|e| anyhow!("Failed to parse energy token program ID: {}", e))
    }

    /// Get Trading program ID
    pub fn trading_program_id() -> Result<Pubkey> {
        Pubkey::from_str(TRADING_PROGRAM_ID)
            .map_err(|e| anyhow!("Failed to parse trading program ID: {}", e))
    }
}
