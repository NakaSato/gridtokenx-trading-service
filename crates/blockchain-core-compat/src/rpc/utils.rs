use anyhow::{anyhow, Result};
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use std::str::FromStr;
use tracing::info;

pub struct BlockchainUtils;

impl BlockchainUtils {
    pub fn parse_pubkey(pubkey_str: &str) -> Result<Pubkey> {
        Pubkey::from_str(pubkey_str)
            .map_err(|e| anyhow!("Invalid public key '{}': {}", pubkey_str, e))
    }

    pub fn load_keypair_from_file(filepath: &str) -> Result<Keypair> {
        use std::fs;
        let file_contents = fs::read_to_string(filepath)
            .map_err(|e| anyhow!("Failed to read keypair file '{}': {}", filepath, e))?;
        Self::load_keypair_from_string(&file_contents)
    }

    pub fn load_keypair_from_string(s: &str) -> Result<Keypair> {
        let s = s.trim();
        if s.starts_with('[') {
            let bytes: Vec<u8> = serde_json::from_str(s)
                .map_err(|e| anyhow!("Failed to parse keypair JSON: {}", e))?;
            return Self::keypair_from_bytes(&bytes);
        }

        // Try Base58 first (Solana CLI standard)
        if let Ok(bytes) = bs58::decode(s).into_vec() {
            if bytes.len() == 64 {
                return Self::keypair_from_bytes(&bytes);
            }
        }

        use base64::{engine::general_purpose, Engine as _};
        let bytes = general_purpose::STANDARD
            .decode(s)
            .map_err(|e| anyhow!("Failed to decode base64 keypair: {}", e))?;
        Self::keypair_from_bytes(&bytes)
    }

    pub fn load_keypair_from_base58(s: &str) -> Result<Keypair> {
        let bytes = bs58::decode(s.trim())
            .into_vec()
            .map_err(|e| anyhow!("Failed to decode base58 keypair: {}", e))?;
        Self::keypair_from_bytes(&bytes)
    }

    fn keypair_from_bytes(bytes: &[u8]) -> Result<Keypair> {
        if bytes.len() != 64 {
            return Err(anyhow!(
                "Invalid keypair: expected 64 bytes, got {}",
                bytes.len()
            ));
        }
        let secret_key: [u8; 32] = bytes[0..32]
            .try_into()
            .map_err(|_| anyhow!("Failed to extract secret"))?;
        let keypair = Keypair::new_from_array(secret_key);
        info!("Successfully loaded keypair: {}", keypair.pubkey());
        Ok(keypair)
    }
}
