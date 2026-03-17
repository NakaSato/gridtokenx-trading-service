use anyhow::{anyhow, Result};
use solana_sdk::{
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use super::instructions::{
    ENERGY_TOKEN_PROGRAM_ID, GOVERNANCE_PROGRAM_ID, ORACLE_PROGRAM_ID, REGISTRY_PROGRAM_ID,
    TRADING_PROGRAM_ID,
};
use std::str::FromStr;
use tracing::info;

// Token Program IDs
#[allow(dead_code)]
const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
#[allow(dead_code)]
const TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkThp9Dz9L33itf29V7D3fR65";

/// Utility functions for Solana blockchain operations
pub struct BlockchainUtils;

impl BlockchainUtils {
    /// Parse Pubkey from string
    pub fn parse_pubkey(pubkey_str: &str) -> Result<Pubkey> {
        Pubkey::from_str(pubkey_str)
            .map_err(|e| anyhow!("Invalid public key '{}': {}", pubkey_str, e))
    }

    /// Load keypair from a JSON file
    /// The file should contain an array of 64 bytes representing the keypair
    pub fn load_keypair_from_file(filepath: &str) -> Result<Keypair> {
        use std::fs;

        info!("Loading keypair from file: {}", filepath);

        // Read the file contents
        let file_contents = fs::read_to_string(filepath)
            .map_err(|e| anyhow!("Failed to read keypair file '{}': {}", filepath, e))?;

        Self::load_keypair_from_string(&file_contents)
    }

    /// Load keypair from an environment variable
    pub fn load_keypair_from_env(var_name: &str) -> Result<Keypair> {
        let val = std::env::var(var_name)
            .map_err(|e| anyhow!("Environment variable '{}' not set: {}", var_name, e))?;
        
        Self::load_keypair_from_string(&val)
    }

    /// Load keypair from a string (JSON array or base64)
    pub fn load_keypair_from_string(s: &str) -> Result<Keypair> {
        let s = s.trim();

        // Try parsing as JSON array first
        if s.starts_with('[') {
            let bytes: Vec<u8> = serde_json::from_str(s)
                .map_err(|e| anyhow!("Failed to parse keypair JSON: {}", e))?;
            
            return Self::keypair_from_bytes(&bytes);
        }

        // Try parsing as base64
        use base64::{engine::general_purpose, Engine as _};
        let bytes = general_purpose::STANDARD.decode(s)
            .map_err(|e| anyhow!("Failed to decode base64 keypair: {}", e))?;
        
        Self::keypair_from_bytes(&bytes)
    }

    /// Internal helper to create keypair from bytes
    fn keypair_from_bytes(bytes: &[u8]) -> Result<Keypair> {
        if bytes.len() != 64 {
            return Err(anyhow!(
                "Invalid keypair: expected 64 bytes, got {}",
                bytes.len()
            ));
        }

        let mut keypair_bytes = [0u8; 64];
        keypair_bytes.copy_from_slice(bytes);

        let mut secret_key = [0u8; 32];
        secret_key.copy_from_slice(&keypair_bytes[0..32]);
        let keypair = Keypair::new_from_array(secret_key);

        info!("Successfully loaded keypair: {}", keypair.pubkey());

        Ok(keypair)
    }

    /// Mint energy tokens directly to a user's token account (Anchor CPI)
    pub fn create_mint_instruction(
        authority: &Keypair,
        user_token_account: &Pubkey,
        _user_wallet: &Pubkey,
        mint: &Pubkey,
        amount_kwh: Decimal,
    ) -> Result<Instruction> {
        info!(
            "Creating mint instruction (Anchor) for {} kWh to {}",
            amount_kwh, user_token_account
        );

        // Convert kWh to token amount (with 9 decimals)
        let amount_lamports = ToPrimitive::to_u64(&(amount_kwh * Decimal::from(1_000_000_000))).unwrap_or(0);

        let energy_token_program_id = Self::energy_token_program_id()?;
        let token_program_id = Self::get_token_program_id()?;

        // Derive token_info PDA
        let (token_info_pda, _) =
            Pubkey::find_program_address(&[b"token_info_2022"], &energy_token_program_id);

        // Build instruction data
        let mut instruction_data = Vec::new();

        // Discriminator for "mint_tokens_direct": [13, 246, 31, 237, 99, 19, 88, 226]
        // Calculated via sha256("global:mint_tokens_direct")[:8]
        instruction_data.extend_from_slice(&[13, 246, 31, 237, 99, 19, 88, 226]);

        // Arguments
        instruction_data.extend_from_slice(&amount_lamports.to_le_bytes());

        // Accounts required by MintTokensDirect context:
        // 0. token_info (mut)
        // 1. mint (mut)
        // 2. user_token_account (mut)
        // 3. authority (signer)
        // 4. registry_authority (readonly) - often same as authority
        // 5. token_program
        
        use solana_sdk::instruction::{AccountMeta, Instruction};

        let accounts = vec![
            AccountMeta::new(token_info_pda, false),
            AccountMeta::new(*mint, false),
            AccountMeta::new(*user_token_account, false),
            AccountMeta::new(authority.pubkey(), true),
            AccountMeta::new_readonly(authority.pubkey(), false),
            AccountMeta::new_readonly(token_program_id, false),
        ];

        let mint_instruction = Instruction {
            program_id: energy_token_program_id,
            accounts,
            data: instruction_data,
        };

        Ok(mint_instruction)
    }

    /// Mint SPL tokens directly using standard Token Program (for minimal build)
    /// This bypasses the Anchor program and uses raw SPL token minting
    pub fn create_spl_mint_instruction(
        authority: &Keypair,
        user_token_account: &Pubkey,
        mint: &Pubkey,
        amount_kwh: Decimal,
    ) -> Result<Instruction> {
        info!(
            "Creating SPL mint instruction for {} kWh to {}",
            amount_kwh, user_token_account
        );

        // Convert kWh to token amount (with 9 decimals)
        let amount_lamports = ToPrimitive::to_u64(&(amount_kwh * Decimal::from(1_000_000_000))).unwrap_or(0);

        let token_program_id = Self::get_token_program_id()?;

        // Use the proper spl_token instruction builder
        let instruction = spl_token::instruction::mint_to(
            &token_program_id,
            mint,
            user_token_account,
            &authority.pubkey(),
            &[],  // No multisig signers
            amount_lamports,
        )?;

        Ok(instruction)
    }

    /// Ensures user has an Associated Token Account for the token mint
    /// Creates ATA if it doesn't exist (idempotent - won't fail if already exists)
    pub fn create_ata_instruction_idempotent(
        authority: &Keypair,
        user_wallet: &Pubkey,
        mint: &Pubkey,
    ) -> Result<Instruction> {
        info!("Creating idempotent ATA instruction for user: {}", user_wallet);

        let token_program_id = Self::get_token_program_id()?;

        // Use idempotent version - doesn't fail if account already exists
        let instruction = spl_associated_token_account::instruction::create_associated_token_account_idempotent(
            &authority.pubkey(),  // Payer
            user_wallet,          // Owner of the ATA
            mint,                 // Token mint
            &token_program_id,    // Token program
        );

        Ok(instruction)
    }

    /// Transfer SPL tokens from one account to another
    /// Used for settlement transfers: seller → buyer
    /// Uses transfer_checked for Token-2022 compatibility
    pub fn create_transfer_instruction(
        authority: &Keypair,
        from_token_account: &Pubkey,
        to_token_account: &Pubkey,
        mint: &Pubkey,
        amount: u64,
        decimals: u8,
    ) -> Result<Instruction> {
        info!(
            "Creating transfer_checked instruction for {} tokens from {} to {}",
            amount, from_token_account, to_token_account
        );

        let token_program_id = Self::get_token_program_id()?;

        // Use transfer_checked for Token-2022 compatibility (validates mint and decimals)
        let instruction = spl_token::instruction::transfer_checked(
            &token_program_id,
            from_token_account,
            mint,
            to_token_account,
            &authority.pubkey(),
            &[],  // No multisig signers
            amount,
            decimals,
        )?;

        Ok(instruction)
    }

    /// Register a user on-chain
    pub fn create_register_user_instruction(
        authority: &Keypair,
        user_type: u8,
        lat_e7: i32,
        long_e7: i32,
        h3_index: u64,
    ) -> Result<Instruction> {
        info!(
            "Creating register user instruction for: {}",
            authority.pubkey()
        );

        let registry_program_id = Self::registry_program_id()?;

        // Derive PDAs
        let (registry_pda, _) = Pubkey::find_program_address(&[b"registry"], &registry_program_id);
        let (user_account_pda, _) = Pubkey::find_program_address(
            &[b"user", authority.pubkey().as_ref()],
            &registry_program_id,
        );

        // Build instruction data
        let mut instruction_data = Vec::new();

        // Discriminator for "register_user"
        instruction_data.extend_from_slice(&[2, 241, 150, 223, 99, 214, 116, 97]);

        // Arguments
        instruction_data.push(user_type);
        instruction_data.extend_from_slice(&lat_e7.to_le_bytes());
        instruction_data.extend_from_slice(&long_e7.to_le_bytes());
        instruction_data.extend_from_slice(&h3_index.to_le_bytes());

        // Accounts (Simple version without optional airdrop accounts for Utils)
        let accounts = vec![
            solana_sdk::instruction::AccountMeta::new(user_account_pda, false),
            solana_sdk::instruction::AccountMeta::new(registry_pda, false),
            solana_sdk::instruction::AccountMeta::new(authority.pubkey(), true),
            solana_sdk::instruction::AccountMeta::new_readonly(
                solana_sdk::pubkey!("11111111111111111111111111111111"),
                false,
            ),
        ];

        Ok(Instruction::new_with_bytes(
            registry_program_id,
            &instruction_data,
            accounts,
        ))
    }

    /// Register a meter on-chain
    pub fn create_register_meter_instruction(
        authority: &Keypair,
        meter_id: &str,
        meter_type: u8,
    ) -> Result<Instruction> {
        info!("Creating register meter instruction for: {}", meter_id);

        let registry_program_id = Self::registry_program_id()?;

        // Derive PDAs
        let (registry_pda, _) = Pubkey::find_program_address(&[b"registry"], &registry_program_id);
        let (user_account_pda, _) = Pubkey::find_program_address(
            &[b"user", authority.pubkey().as_ref()],
            &registry_program_id,
        );
        // Correct seeds: [b"meter", owner, meter_id]
        let (meter_account_pda, _) =
            Pubkey::find_program_address(
                &[b"meter", authority.pubkey().as_ref(), meter_id.as_bytes()], 
                &registry_program_id
            );

        // Build instruction data
        let mut instruction_data = Vec::new();

        // register_meter discriminator: [49, 106, 87, 72, 138, 214, 224, 125]
        instruction_data.extend_from_slice(&[49, 106, 87, 72, 138, 214, 224, 125]);

        // Arguments: String (u32 len + bytes)
        let meter_id_bytes = meter_id.as_bytes();
        instruction_data.extend_from_slice(&(meter_id_bytes.len() as u32).to_le_bytes());
        instruction_data.extend_from_slice(meter_id_bytes);
        instruction_data.push(meter_type);

        // Accounts
        let accounts = vec![
            solana_sdk::instruction::AccountMeta::new(meter_account_pda, false),
            solana_sdk::instruction::AccountMeta::new(user_account_pda, false),
            solana_sdk::instruction::AccountMeta::new(registry_pda, false),
            solana_sdk::instruction::AccountMeta::new(authority.pubkey(), true),
            solana_sdk::instruction::AccountMeta::new_readonly(
                solana_sdk::pubkey!("11111111111111111111111111111111"),
                false,
            ),
        ];

        Ok(Instruction::new_with_bytes(
            registry_program_id,
            &instruction_data,
            accounts,
        ))
    }

    /// Submit meter reading on-chain (via Oracle)
    pub fn create_submit_meter_reading_instruction(
        authority: &Keypair, // Must be API Gateway authority
        owner: &Pubkey,      // Owner of the meter
        meter_id: &str,
        produced: u64,
        consumed: u64,
        timestamp: i64,
    ) -> Result<Instruction> {
        info!(
            "Creating submit meter reading instruction for: {}",
            meter_id
        );

        let oracle_program_id = Self::oracle_program_id()?;
        let registry_program_id = Self::registry_program_id()?;

        // Derive PDAs
        let (oracle_data_pda, _) =
            Pubkey::find_program_address(&[b"oracle_data"], &oracle_program_id);
        let (_meter_account_pda, _) =
            Pubkey::find_program_address(
                &[b"meter", owner.as_ref(), meter_id.as_bytes()], 
                &registry_program_id
            );

        // Build instruction data
        let mut instruction_data = Vec::new();

        // Use discriminator from IDL for submit_meter_reading: [181, 247, 196, 139, 78, 88, 192, 206]
        instruction_data.extend_from_slice(&[181, 247, 196, 139, 78, 88, 192, 206]);

        // Arguments
        instruction_data.extend_from_slice(&(meter_id.len() as u32).to_le_bytes());
        instruction_data.extend_from_slice(meter_id.as_bytes());
        instruction_data.extend_from_slice(&produced.to_le_bytes());
        instruction_data.extend_from_slice(&consumed.to_le_bytes());
        instruction_data.extend_from_slice(&timestamp.to_le_bytes());

        // Accounts - matching IDL (oracle_data, authority)
        let accounts = vec![
            solana_sdk::instruction::AccountMeta::new(oracle_data_pda, false),
            solana_sdk::instruction::AccountMeta::new_readonly(authority.pubkey(), true),
        ];

        Ok(Instruction::new_with_bytes(
            oracle_program_id,
            &instruction_data,
            accounts,
        ))
    }

    /// Update meter reading on-chain via Registry program (oracle authorization required)
    /// This calls the Registry program's `update_meter_reading` instruction
    /// which requires the caller to be the registered oracle authority
    pub fn create_update_meter_reading_instruction(
        oracle_authority: &Keypair, // Must be the configured oracle authority on Registry
        owner: &Pubkey,             // Owner of the meter
        meter_id: &str,
        energy_generated: u64,  // in Wh (watt-hours)
        energy_consumed: u64,   // in Wh (watt-hours)
        reading_timestamp: i64, // Unix timestamp
    ) -> Result<Instruction> {
        info!(
            "Creating Registry update_meter_reading instruction for meter: {} (gen: {} Wh, cons: {} Wh)",
            meter_id, energy_generated, energy_consumed
        );

        let registry_program_id = Self::registry_program_id()?;

        // Derive PDAs
        let (registry_pda, _) = Pubkey::find_program_address(&[b"registry"], &registry_program_id);
        let (meter_account_pda, _) =
            Pubkey::find_program_address(
                &[b"meter", owner.as_ref(), meter_id.as_bytes()], 
                &registry_program_id
            );

        // Build instruction data
        let mut instruction_data = Vec::new();

        // Discriminator for "update_meter_reading"
        // Calculated via sha256("global:update_meter_reading")[:8]
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(b"global:update_meter_reading");
        let hash = hasher.finalize();
        instruction_data.extend_from_slice(&hash[0..8]);

        // Arguments (must match Anchor instruction signature)
        // update_meter_reading(energy_generated: u64, energy_consumed: u64, reading_timestamp: i64)
        instruction_data.extend_from_slice(&energy_generated.to_le_bytes());
        instruction_data.extend_from_slice(&energy_consumed.to_le_bytes());
        instruction_data.extend_from_slice(&reading_timestamp.to_le_bytes());

        // Accounts - matching UpdateMeterReading context in Registry program:
        // 0. registry: Account<Registry>
        // 1. meter_account: Account<MeterAccount> (mut)
        // 2. oracle_authority: Signer
        let accounts = vec![
            solana_sdk::instruction::AccountMeta::new_readonly(registry_pda, false),
            solana_sdk::instruction::AccountMeta::new(meter_account_pda, false),
            solana_sdk::instruction::AccountMeta::new_readonly(oracle_authority.pubkey(), true),
        ];

        Ok(Instruction::new_with_bytes(
            registry_program_id,
            &instruction_data,
            accounts,
        ))
    }

    /// Set the oracle authority in the Registry program (admin only)
    pub fn create_set_oracle_authority_instruction(
        authority: &Keypair, // Must be the Registry authority
        oracle: &Pubkey,      // New oracle authority to set
    ) -> Result<Instruction> {
        info!("Creating set_oracle_authority instruction for: {}", oracle);

        let registry_program_id = Self::registry_program_id()?;

        // Derive PDAs
        let (registry_pda, _) = Pubkey::find_program_address(&[b"registry"], &registry_program_id);

        // Build instruction data
        let mut instruction_data = Vec::new();

        // Discriminator for "set_oracle_authority": [39, 155, 66, 106, 213, 226, 114, 174]
        instruction_data.extend_from_slice(&[39, 155, 66, 106, 213, 226, 114, 174]);

        // Arguments: oracle (Pubkey)
        instruction_data.extend_from_slice(oracle.as_ref());

        // Accounts
        let accounts = vec![
            solana_sdk::instruction::AccountMeta::new(registry_pda, false),
            solana_sdk::instruction::AccountMeta::new_readonly(authority.pubkey(), true),
        ];

        Ok(Instruction::new_with_bytes(
            registry_program_id,
            &instruction_data,
            accounts,
        ))
    }

    /// Burn energy tokens (for energy consumption)
    pub fn create_burn_instruction(
        authority: &Keypair,
        user_token_account: &Pubkey,
        mint: &Pubkey,
        amount_kwh: Decimal,
    ) -> Result<Instruction> {
        info!(
            "Creating burn instruction for {} kWh from {}",
            amount_kwh, user_token_account
        );

        // Convert kWh to token amount (with 9 decimals)
        let amount_lamports = ToPrimitive::to_u64(&(amount_kwh.abs() * Decimal::from(1_000_000_000))).unwrap_or(0);

        let energy_token_program_id = Self::energy_token_program_id()?;

        // Derive token_info PDA
        let (token_info_pda, _) =
            Pubkey::find_program_address(&[b"token_info_2022"], &energy_token_program_id);

        // Build instruction data
        let mut instruction_data = Vec::new();

        // Discriminator for "burn_tokens"
        // global:burn_tokens = 9b4d08130831626e
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(b"global:burn_tokens");
        let hash = hasher.finalize();
        instruction_data.extend_from_slice(&hash[0..8]);

        // Arguments
        instruction_data.extend_from_slice(&amount_lamports.to_le_bytes());

        // Accounts
        let accounts = vec![
            solana_sdk::instruction::AccountMeta::new(token_info_pda, false),
            solana_sdk::instruction::AccountMeta::new(*mint, false),
            solana_sdk::instruction::AccountMeta::new(*user_token_account, false),
            solana_sdk::instruction::AccountMeta::new_readonly(authority.pubkey(), true),
            solana_sdk::instruction::AccountMeta::new_readonly(
                Self::get_token_program_id()?,
                false,
            ), // Token program
        ];

        Ok(Instruction::new_with_bytes(
            energy_token_program_id,
            &instruction_data,
            accounts,
        ))
    }

    // Helper methods for program IDs

    /// Get Registry program ID
    fn registry_program_id() -> Result<Pubkey> {
        let program_id = std::env::var("SOLANA_REGISTRY_PROGRAM_ID")
            .unwrap_or_else(|_| REGISTRY_PROGRAM_ID.to_string());

        program_id
            .parse()
            .map_err(|e| anyhow!("Failed to parse registry program ID: {}", e))
    }

    /// Get Oracle program ID
    fn oracle_program_id() -> Result<Pubkey> {
        let program_id = std::env::var("SOLANA_ORACLE_PROGRAM_ID")
            .unwrap_or_else(|_| ORACLE_PROGRAM_ID.to_string());

        program_id
            .parse()
            .map_err(|e| anyhow!("Failed to parse oracle program ID: {}", e))
    }

    /// Get Governance program ID
    #[allow(dead_code)]
    fn governance_program_id() -> Result<Pubkey> {
        let program_id = std::env::var("SOLANA_GOVERNANCE_PROGRAM_ID")
            .unwrap_or_else(|_| GOVERNANCE_PROGRAM_ID.to_string());

        program_id
            .parse()
            .map_err(|e| anyhow!("Failed to parse governance program ID: {}", e))
    }

    /// Get Energy Token program ID
    fn energy_token_program_id() -> Result<Pubkey> {
        let program_id = std::env::var("SOLANA_ENERGY_TOKEN_PROGRAM_ID")
            .unwrap_or_else(|_| ENERGY_TOKEN_PROGRAM_ID.to_string());

        program_id
            .parse()
            .map_err(|e| anyhow!("Failed to parse energy token program ID: {}", e))
    }

    /// Get Trading program ID
    #[allow(dead_code)]
    fn trading_program_id() -> Result<Pubkey> {
        let program_id = std::env::var("SOLANA_TRADING_PROGRAM_ID")
            .unwrap_or_else(|_| TRADING_PROGRAM_ID.to_string());

        program_id
            .parse()
            .map_err(|e| anyhow!("Failed to parse trading program ID: {}", e))
    }

    /// Get the correct Token Program ID
    /// We use Token-2022 since our mint is deployed with Token-2022 (--program-2022)
    pub fn get_token_program_id() -> Result<Pubkey> {
        // Use Token-2022 Program ID since ENERGY_TOKEN_MINT is created with Token-2022
        Pubkey::from_str(TOKEN_2022_PROGRAM_ID)
            .map_err(|e| anyhow!("Failed to parse token program ID: {}", e))
    }
}

/// Helper functions for transaction building
pub mod transaction_utils {
    use super::*;
    use solana_sdk::hash::Hash;
    use solana_sdk::instruction::Instruction;
    use solana_sdk::signature::Keypair;
    use solana_sdk::transaction::Transaction;

    /// Build a transaction from instructions
    pub fn build_transaction(
        instructions: Vec<Instruction>,
        payer: &Pubkey,
        _recent_blockhash: Hash,
    ) -> Transaction {
        Transaction::new_with_payer(&instructions, Some(payer))
    }

    /// Sign a transaction
    pub fn sign_transaction(
        transaction: &mut Transaction,
        signers: &[&Keypair],
        recent_blockhash: Hash,
    ) -> Result<()> {
        transaction
            .try_sign(signers, recent_blockhash)
            .map_err(|e| anyhow!("Failed to sign transaction: {}", e))?;
        Ok(())
    }

    /// Data structure for batch minting operations
    #[derive(Debug, Clone)]
    pub struct MintBatchData {
        pub user_wallet: Pubkey,
        pub user_token_account: Pubkey,
        pub amount_kwh: Decimal,
        pub tokens_to_mint: u64,
    }

    /// Result of a batch minting operation
    #[derive(Debug, Clone)]
    pub struct MintBatchResult {
        pub user_wallet: Pubkey,
        pub success: bool,
        pub error: Option<String>,
        pub tx_signature: Option<String>,
    }

    /// Helper method to create MintBatchData from user wallet and kWh amount
    pub fn create_mint_batch_data(
        user_wallet: Pubkey,
        kwh_amount: Decimal,
        kwh_to_token_ratio: Decimal,
        decimals: u8,
    ) -> Result<MintBatchData> {
        // Calculate tokens to mint
        let tokens_to_mint = ToPrimitive::to_u64(&(kwh_amount * kwh_to_token_ratio * Decimal::from(10_u64.pow(decimals as u32)))).unwrap_or(0);

        // Get or create associated token account
        let token_program_id = BlockchainUtils::get_token_program_id()?;

        let ata_program_id = Pubkey::from_str("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")
            .map_err(|e| anyhow!("Invalid ATA program ID: {}", e))?;

        // Get energy token mint
        let mint = Pubkey::from_str(
            &std::env::var("ENERGY_TOKEN_MINT")
                .map_err(|e| anyhow!("ENERGY_TOKEN_MINT not set: {}", e))?,
        )?;

        // Calculate ATA address
        let (user_token_account, _bump) = Pubkey::find_program_address(
            &[
                user_wallet.as_ref(),
                token_program_id.as_ref(),
                mint.as_ref(),
            ],
            &ata_program_id,
        );

        Ok(MintBatchData {
            user_wallet,
            user_token_account,
            amount_kwh: kwh_amount,
            tokens_to_mint,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keypair_serialization_consistency() {
        let kp = Keypair::new();
        let pubkey_orig = kp.pubkey();
        let bytes = kp.to_bytes();
        
        let mut seed = [0u8; 32];
        seed.copy_from_slice(&bytes[0..32]);
        let kp_der = Keypair::new_from_array(seed);
        
        assert_eq!(pubkey_orig, kp_der.pubkey(), "Keypair derived from first 32 bytes of to_bytes() should match original!");
    }

    #[test]
    fn test_load_keypair_from_string_json() {
        let kp = Keypair::new();
        let bytes = kp.to_bytes();
        let json = serde_json::to_string(&bytes.to_vec()).unwrap();
        
        let loaded_kp = BlockchainUtils::load_keypair_from_string(&json).unwrap();
        assert_eq!(kp.pubkey(), loaded_kp.pubkey());
    }

    #[test]
    fn test_load_keypair_from_string_base64() {
        use base64::{engine::general_purpose, Engine as _};
        let kp = Keypair::new();
        let bytes = kp.to_bytes();
        let b64 = general_purpose::STANDARD.encode(&bytes);
        
        let loaded_kp = BlockchainUtils::load_keypair_from_string(&b64).unwrap();
        assert_eq!(kp.pubkey(), loaded_kp.pubkey());
    }
}
