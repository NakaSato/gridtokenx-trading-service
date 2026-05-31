use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};
use std::str::FromStr;

use crate::config::SolanaProgramsConfig;

// System program ID constant
pub const SYSTEM_PROGRAM_ID: &str = "11111111111111111111111111111111";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u8)]
pub enum UserType {
    Prosumer = 0,
    Consumer = 1,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u8)]
pub enum MeterType {
    Solar = 0,
    Wind = 1,
    Grid = 2,
    Storage = 3,
    Battery = 4,
    Hydro = 5,
    Biomass = 6,
    Geothermal = 7,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u8)]
pub enum UserStatus {
    Active = 0,
    Suspended = 1,
    Inactive = 2,
}

/// Payload for off-chain orders
#[derive(Clone, Debug, Serialize, Deserialize)]
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
    config: SolanaProgramsConfig,
}

impl InstructionBuilder {
    pub fn new(payer: Pubkey, config: SolanaProgramsConfig) -> Self {
        Self { payer, config }
    }

    pub fn payer(&self) -> Pubkey {
        self.payer
    }

    /// Get market PDA
    pub fn get_market_pda(&self) -> Result<Pubkey> {
        let program_id = Pubkey::from_str(&self.config.trading_program_id)
            .context("Invalid Trading program ID")?;
        let (market_pda, _) = Pubkey::find_program_address(&[b"market"], &program_id);
        Ok(market_pda)
    }

    /// Get registry PDA
    pub fn get_registry_pda(&self) -> Result<Pubkey> {
        let program_id = Pubkey::from_str(&self.config.registry_program_id)
            .context("Invalid Registry program ID")?;
        let (registry_pda, _) = Pubkey::find_program_address(&[b"registry"], &program_id);
        Ok(registry_pda)
    }

    /// Get user account PDA
    pub fn get_user_account_pda(&self, wallet: &Pubkey) -> Result<Pubkey> {
        let program_id = Pubkey::from_str(&self.config.registry_program_id)
            .context("Invalid Registry program ID")?;
        let (pda, _) = Pubkey::find_program_address(&[b"user", wallet.as_ref()], &program_id);
        Ok(pda)
    }

    /// Get registry shard PDA
    pub fn get_registry_shard_pda(&self, shard_id: u8) -> Result<Pubkey> {
        let program_id = Pubkey::from_str(&self.config.registry_program_id)
            .context("Invalid Registry program ID")?;
        let (shard_pda, _) =
            Pubkey::find_program_address(&[b"registry_shard", &[shard_id]], &program_id);
        Ok(shard_pda)
    }

    /// Get Energy Token Mint PDA
    pub fn get_mint_pda(&self) -> Result<Pubkey> {
        let program_id = Pubkey::from_str(&self.config.energy_token_program_id)
            .context("Invalid Energy Token program ID")?;
        let (mint_pda, _) = Pubkey::find_program_address(&[b"mint_2022"], &program_id);
        Ok(mint_pda)
    }

    /// Get Energy Token Info PDA
    pub fn get_token_info_pda(&self) -> Result<Pubkey> {
        let program_id = Pubkey::from_str(&self.config.energy_token_program_id)
            .context("Invalid Energy Token program ID")?;
        let (info_pda, _) = Pubkey::find_program_address(&[b"token_info_2022"], &program_id);
        Ok(info_pda)
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
        let program_id = Pubkey::from_str(&self.config.trading_program_id)
            .context("Invalid Trading program ID")?;
        let system_program =
            Pubkey::from_str(SYSTEM_PROGRAM_ID).context("Invalid System program ID")?;

        let (zone_market_pda, _) = Pubkey::find_program_address(
            &[
                b"zone_market",
                market_pubkey.as_ref(),
                &zone_id.to_le_bytes(),
            ],
            &program_id,
        );

        let governance_config = self.get_poa_config_pubkey()?;

        let accounts = if order_type == "sell" {
            let erc_key = if let Some(cert_id) = erc_certificate_id {
                self.get_erc_certificate_pubkey(cert_id)
                    .context("Failed to get ERC certificate pubkey")?
            } else {
                program_id
            };
            vec![
                AccountMeta::new_readonly(*market_pubkey, false),
                AccountMeta::new(zone_market_pda, false),
                AccountMeta::new(order_pda, false),
                AccountMeta::new_readonly(erc_key, false),
                AccountMeta::new(payer, true),
                AccountMeta::new_readonly(system_program, false),
                AccountMeta::new_readonly(governance_config, false),
            ]
        } else {
            vec![
                AccountMeta::new_readonly(*market_pubkey, false),
                AccountMeta::new(zone_market_pda, false),
                AccountMeta::new(order_pda, false),
                AccountMeta::new(payer, true),
                AccountMeta::new_readonly(system_program, false),
                AccountMeta::new_readonly(governance_config, false),
            ]
        };

        let mut data = Vec::new();
        if order_type == "sell" {
            data.extend_from_slice(&[53, 52, 255, 44, 191, 74, 171, 225]);
        } else {
            data.extend_from_slice(&[182, 87, 0, 160, 192, 66, 151, 130]);
        }

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
        let program_id = Pubkey::from_str(&self.config.trading_program_id)
            .context("Invalid Trading program ID")?;
        let market = Pubkey::from_str(market_pubkey).context("Invalid market pubkey")?;
        let buy_order = Pubkey::from_str(buy_order_pubkey).context("Invalid buy order pubkey")?;
        let sell_order =
            Pubkey::from_str(sell_order_pubkey).context("Invalid sell order pubkey")?;

        let (zone_market_pda, _) = Pubkey::find_program_address(
            &[b"zone_market", market.as_ref(), &zone_id.to_le_bytes()],
            &program_id,
        );

        let system_program = Pubkey::from_str(SYSTEM_PROGRAM_ID)?;
        let poa_config = self.get_poa_config_pubkey()?;

        let accounts = vec![
            AccountMeta::new(market, false),
            AccountMeta::new(zone_market_pda, false),
            AccountMeta::new(buy_order, false),
            AccountMeta::new(sell_order, false),
            AccountMeta::new(trade_record_pubkey, false),
            AccountMeta::new(self.payer, true),
            AccountMeta::new_readonly(system_program, false),
            AccountMeta::new_readonly(poa_config, false),
        ];

        let mut data = Vec::new();
        data.extend_from_slice(&[17, 1, 201, 93, 7, 51, 251, 134]);
        data.extend_from_slice(&match_amount.to_le_bytes());

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Build instruction for submitting meter reading
    pub fn build_submit_meter_reading_instruction(
        &self,
        meter_id: &str,
        energy_produced: u64,
        energy_consumed: u64,
        reading_timestamp: i64,
        zone_id: i32,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(&self.config.oracle_program_id)?;
        let system_program = Pubkey::from_str(SYSTEM_PROGRAM_ID)?;

        let (oracle_data_pda, _) = Pubkey::find_program_address(&[b"oracle_data"], &program_id);
        let (meter_state_pda, _) =
            Pubkey::find_program_address(&[b"meter", meter_id.as_bytes()], &program_id);

        let accounts = vec![
            AccountMeta::new_readonly(oracle_data_pda, false),
            AccountMeta::new(meter_state_pda, false),
            AccountMeta::new(self.payer, true),
            AccountMeta::new_readonly(system_program, false),
        ];

        // Discriminator for submit_meter_reading: [181, 247, 196, 139, 78, 88, 192, 206]
        let mut data = vec![181, 247, 196, 139, 78, 88, 192, 206];
        let id_bytes = meter_id.as_bytes();
        data.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
        data.extend_from_slice(id_bytes);
        data.extend_from_slice(&energy_produced.to_le_bytes());
        data.extend_from_slice(&energy_consumed.to_le_bytes());
        data.extend_from_slice(&reading_timestamp.to_le_bytes());
        data.extend_from_slice(&zone_id.to_le_bytes());

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    // PDA Helpers
    pub fn get_erc_certificate_pubkey(&self, certificate_id: &str) -> Result<Pubkey> {
        let program_id = Pubkey::from_str(&self.config.governance_program_id)?;
        let (pda, _) = Pubkey::find_program_address(
            &[b"erc_certificate", certificate_id.as_bytes()],
            &program_id,
        );
        Ok(pda)
    }

    pub fn get_poa_config_pubkey(&self) -> Result<Pubkey> {
        let program_id = Pubkey::from_str(&self.config.governance_program_id)?;
        let (pda, _) = Pubkey::find_program_address(&[b"poa_config"], &program_id);
        Ok(pda)
    }

    pub fn build_initialize_registry_instruction(&self) -> Result<Instruction> {
        let program_id = Pubkey::from_str(&self.config.registry_program_id)?;
        let (registry_pda, _) = Pubkey::find_program_address(&[b"registry"], &program_id);
        let system_program = Pubkey::from_str(SYSTEM_PROGRAM_ID)?;

        Ok(Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(registry_pda, false),
                AccountMeta::new(self.payer, true),
                AccountMeta::new_readonly(system_program, false),
            ],
            data: vec![175, 175, 109, 31, 13, 152, 155, 237],
        })
    }

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
        let program_id = Pubkey::from_str(&self.config.trading_program_id)?;
        let system_program = Pubkey::from_str(SYSTEM_PROGRAM_ID)?;

        let (zone_market_pda, _) = Pubkey::find_program_address(
            &[b"zone_market", market.as_ref(), &0_u32.to_le_bytes()], // Default zone 0 for atomic
            &program_id,
        );

        let accounts = vec![
            AccountMeta::new(market, false),
            AccountMeta::new(zone_market_pda, false),
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
            AccountMeta::new_readonly(system_program, false),
            AccountMeta::new_readonly(self.config.energy_token_program_id.parse().unwrap(), false), // Secondary program for settlement
            AccountMeta::new_readonly(self.get_poa_config_pubkey().unwrap(), false), // Governance
        ];

        let mut data = Vec::new();
        // Discriminator for execute_atomic_settlement: [86, 216, 13, 114, 76, 114, 212, 11]
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

    pub fn build_settle_offchain_match_instruction(
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
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(&self.config.trading_program_id)?;
        let system_program = Pubkey::from_str(SYSTEM_PROGRAM_ID)?;
        let token_program = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")?;
        let token_2022_program = Pubkey::from_str("TokenzQdBNbLqP5VEhdkThp9Dz9L33itf29V7D3fR65")?;

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

        let accounts = vec![
            AccountMeta::new_readonly(*market_pubkey, false), // Market is read-only in IDL for settlement
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
            AccountMeta::new_readonly(solana_sdk::sysvar::instructions::id(), false),
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new_readonly(token_2022_program, false),
            AccountMeta::new_readonly(system_program, false),
        ];

        let mut data = Vec::new();
        // SettleOffchainMatch discriminator: [140, 170, 63, 151, 81, 62, 212, 11]
        data.extend_from_slice(&[140, 170, 63, 151, 81, 62, 212, 11]);
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

    pub fn build_issue_erc_instruction(
        &self,
        certificate_id: &str,
        _user_wallet: &Pubkey,
        meter_account: &Pubkey,
        energy_amount: u64,
        renewable_source: &str,
        validation_data: &str,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(&self.config.governance_program_id)?;
        let system_program = Pubkey::from_str(SYSTEM_PROGRAM_ID)?;

        let (poa_config_pda, _) = Pubkey::find_program_address(&[b"poa_config"], &program_id);
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

        let mut data = Vec::new();
        // issue_erc discriminator: [174, 248, 149, 107, 155, 4, 196, 8]
        data.extend_from_slice(&[174, 248, 149, 107, 155, 4, 196, 8]);

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

    pub fn build_revoke_erc_instruction(
        &self,
        certificate_id: &str,
        reason: &str,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(&self.config.governance_program_id)?;

        let (poa_config_pda, _) = Pubkey::find_program_address(&[b"poa_config"], &program_id);
        let (erc_certificate_pda, _) = Pubkey::find_program_address(
            &[b"erc_certificate", certificate_id.as_bytes()],
            &program_id,
        );

        let accounts = vec![
            AccountMeta::new(poa_config_pda, false),
            AccountMeta::new(erc_certificate_pda, false),
            AccountMeta::new(self.payer, true),
        ];

        let mut data = Vec::new();
        // revoke_erc discriminator: [16, 48, 113, 85, 118, 70, 185, 150]
        data.extend_from_slice(&[16, 48, 113, 85, 118, 70, 185, 150]);

        let bytes = reason.as_bytes();
        data.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        data.extend_from_slice(bytes);

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    pub fn build_transfer_erc_instruction(
        &self,
        certificate_id: &str,
        owner: &Pubkey,
        new_owner: &Pubkey,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(&self.config.governance_program_id)?;

        let (poa_config_pda, _) = Pubkey::find_program_address(&[b"poa_config"], &program_id);
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

        let mut data = Vec::new();
        // transfer_erc discriminator: [200, 15, 16, 13, 13, 143, 11, 11]
        data.extend_from_slice(&[200, 15, 16, 13, 13, 143, 11, 11]);

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    pub fn build_register_user_instruction(
        &self,
        authority: Pubkey,
        user_type: UserType,
        lat_e7: i32,
        long_e7: i32,
        h3_index: u64,
        shard_id: u8,
    ) -> Result<Instruction> {
        let registry_program = Pubkey::from_str(&self.config.registry_program_id)
            .context("Invalid Registry program ID")?;
        let energy_token_program = Pubkey::from_str(&self.config.energy_token_program_id)
            .context("Invalid Energy Token program ID")?;
        let system_program = Pubkey::from_str(SYSTEM_PROGRAM_ID)?;

        let registry_pda = self.get_registry_pda()?;
        let user_account_pda = self.get_user_account_pda(&authority)?;
        let shard_pda = self.get_registry_shard_pda(shard_id)?;

        let mint_pda = self.get_mint_pda()?;
        let token_info_pda = self.get_token_info_pda()?;
        let token_program = spl_token_2022::id();

        // Get user token account (ATA)
        let user_token_account =
            spl_associated_token_account::get_associated_token_address_with_program_id(
                &authority,
                &mint_pda,
                &token_program,
            );

        let accounts = vec![
            AccountMeta::new(user_account_pda, false),
            AccountMeta::new(shard_pda, false),
            AccountMeta::new(registry_pda, false),
            AccountMeta::new_readonly(authority, false),
            AccountMeta::new(self.payer, true),
            AccountMeta::new_readonly(energy_token_program, false),
            AccountMeta::new(mint_pda, false),
            AccountMeta::new(user_token_account, false),
            AccountMeta::new_readonly(token_info_pda, false),
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new_readonly(system_program, false),
        ];

        let mut data = Vec::new();
        // RegisterUser discriminator: [2, 241, 150, 223, 99, 214, 116, 97]
        data.extend_from_slice(&[2, 241, 150, 223, 99, 214, 116, 97]);
        data.push(user_type as u8);
        data.extend_from_slice(&lat_e7.to_le_bytes());
        data.extend_from_slice(&long_e7.to_le_bytes());
        data.extend_from_slice(&h3_index.to_le_bytes());
        data.push(shard_id);

        Ok(Instruction {
            program_id: registry_program,
            accounts,
            data,
        })
    }

    /// Build instruction for minting GRX tokens to a wallet
    pub fn build_mint_to_wallet_instruction(
        &self,
        mint: Pubkey,
        token_info: Pubkey,
        destination: Pubkey,
        destination_owner: Pubkey,
        authority: Pubkey,
        amount: u64,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(&self.config.energy_token_program_id)?;
        let token_program = spl_token_2022::id();
        let associated_token_program =
            Pubkey::from_str("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL").unwrap();
        let system_program = Pubkey::from_str(SYSTEM_PROGRAM_ID)?;

        let accounts = vec![
            AccountMeta::new(mint, false),
            AccountMeta::new_readonly(token_info, false),
            AccountMeta::new(destination, false),
            AccountMeta::new_readonly(destination_owner, false),
            AccountMeta::new_readonly(authority, true), // authority (signer)
            AccountMeta::new(self.payer, true),         // payer (signer)
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

    /// Build instruction for aggregating meter readings into global counters
    pub fn build_aggregate_readings_instruction(
        &self,
        total_produced: u64,
        total_consumed: u64,
        valid_count: u64,
        rejected_count: u64,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(&self.config.oracle_program_id)?;
        let oracle_data_pda = self.get_oracle_data_pda()?;

        let accounts = vec![
            AccountMeta::new(oracle_data_pda, false),
            AccountMeta::new_readonly(self.payer, true),
        ];

        let mut data = Vec::new();
        // aggregate_readings discriminator: [238, 40, 45, 142, 54, 56, 83, 253]
        data.extend_from_slice(&[238, 40, 45, 142, 54, 56, 83, 253]);
        data.extend_from_slice(&total_produced.to_le_bytes());
        data.extend_from_slice(&total_consumed.to_le_bytes());
        data.extend_from_slice(&valid_count.to_le_bytes());
        data.extend_from_slice(&rejected_count.to_le_bytes());

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Build instruction for triggering market clearing
    pub fn build_trigger_market_clearing_instruction(
        &self,
        epoch_timestamp: i64,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(&self.config.oracle_program_id)?;
        let oracle_data_pda = self.get_oracle_data_pda()?;

        let accounts = vec![
            AccountMeta::new(oracle_data_pda, false),
            AccountMeta::new_readonly(self.payer, true),
        ];

        let mut data = Vec::new();
        // trigger_market_clearing discriminator: [180, 116, 162, 167, 37, 28, 78, 159]
        data.extend_from_slice(&[180, 116, 162, 167, 37, 28, 78, 159]);
        data.extend_from_slice(&epoch_timestamp.to_le_bytes());

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Build instruction for updating API Gateway address
    pub fn build_update_api_gateway_instruction(
        &self,
        new_api_gateway: Pubkey,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(&self.config.oracle_program_id)?;
        let oracle_data_pda = self.get_oracle_data_pda()?;

        let accounts = vec![
            AccountMeta::new(oracle_data_pda, false),
            AccountMeta::new_readonly(self.payer, true),
        ];

        let mut data = Vec::new();
        // update_api_gateway discriminator: [66, 69, 252, 242, 127, 168, 42, 112]
        data.extend_from_slice(&[66, 69, 252, 242, 127, 168, 42, 112]);
        data.extend_from_slice(new_api_gateway.as_ref());

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// Helper to get Oracle Data PDA
    pub fn get_oracle_data_pda(&self) -> Result<Pubkey> {
        let program_id = Pubkey::from_str(&self.config.oracle_program_id)
            .context("Invalid Oracle program ID")?;
        let (pda, _) = Pubkey::find_program_address(&[b"oracle_data"], &program_id);
        Ok(pda)
    }

    /// Get meter account PDA
    pub fn get_meter_account_pda(&self, serial_number: &str) -> Result<Pubkey> {
        let program_id = Pubkey::from_str(&self.config.registry_program_id)
            .context("Invalid Registry program ID")?;
        let (pda, _) = Pubkey::find_program_address(
            &[b"meter", serial_number.as_bytes()],
            &program_id,
        );
        Ok(pda)
    }

    /// Build instruction for meter registration (Registry Program)
    pub fn build_register_meter_instruction(
        &self,
        owner: Pubkey,
        meter_id: String,
        meter_type: MeterType,
        shard_id: u8,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(&self.config.registry_program_id)
            .context("Invalid Registry program ID")?;
        let system_program = Pubkey::from_str(SYSTEM_PROGRAM_ID)?;

        let user_account_pda = self.get_user_account_pda(&owner)?;
        let registry_pda = self.get_registry_pda()?;
        let shard_pda = self.get_registry_shard_pda(shard_id)?;
        let meter_account_pda = self.get_meter_account_pda(&meter_id)?;

        let accounts = vec![
            AccountMeta::new(meter_account_pda, false),
            AccountMeta::new(user_account_pda, false),
            AccountMeta::new(shard_pda, false),
            AccountMeta::new(registry_pda, false),
            AccountMeta::new(owner, true), // owner IS a signer in the program
            AccountMeta::new_readonly(system_program, false),
        ];

        let mut data = Vec::new();
        // register_meter discriminator: [156, 172, 12, 102, 116, 219, 137, 203]
        data.extend_from_slice(&[156, 172, 12, 102, 116, 219, 137, 203]);
        
        // Serialize Serial Number (String)
        let bytes = meter_id.as_bytes();
        data.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        data.extend_from_slice(bytes);
        
        data.push(meter_type as u8);
        data.push(shard_id);

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_pda_derivation() {
        let program_id = Pubkey::from_str("C8RT8L5pZCVDrf9v94CNNk3XPBKZU5p4o4aPnAVQGiTu").unwrap();
        let shard_id: u8 = 0;
        let (shard_pda, _) =
            Pubkey::find_program_address(&[b"registry_shard", &[shard_id]], &program_id);
        println!("EXPECTED_SHARD_0_PDA: {}", shard_pda);

        let (registry_pda, _) = Pubkey::find_program_address(&[b"registry"], &program_id);
        println!("EXPECTED_REGISTRY_PDA: {}", registry_pda);
    }
}
