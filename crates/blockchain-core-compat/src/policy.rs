use crate::auth::SpiffeIdentity;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::Transaction;
use std::str::FromStr;
use tracing::{info, warn};

pub struct PolicyEngine;

impl PolicyEngine {
    /// Validates that every instruction in the transaction is calling an explicitly allowlisted program ID
    /// for the given SPIFFE identity.
    pub fn validate_transaction(
        identity: &SpiffeIdentity,
        transaction: &Transaction,
    ) -> anyhow::Result<()> {
        let uri = &identity.0;

        // Dev mode / Admin override
        if uri.starts_with("spiffe://gridtokenx.th/prod/admin")
            || std::env::var("CHAIN_BRIDGE_INSECURE")
                .map(|v| v.to_lowercase() == "true")
                .unwrap_or(false)
        {
            info!("🛡️ PolicyEngine: Bypassing program ID check for Admin/Insecure mode");
            return Ok(());
        }

        let system_program =
            Pubkey::from_str("11111111111111111111111111111111").expect("Valid pubkey");

        let mut allowed_programs = vec![system_program];

        if uri.starts_with("spiffe://gridtokenx.th/prod/trading-service") {
            // Trading Matcher/API can invoke Trading, Registry, and Energy Token
            allowed_programs.push(
                Pubkey::from_str("DXxHdUar3pUUKRnt4XAMA8rdYRpAsNY1xk3Zo4crShvY")
                    .expect("Valid pubkey"),
            );
            allowed_programs.push(
                Pubkey::from_str("HZR6b8GhzhDowyL6dX58qBjdSDNtFyJHU5dPF3kXDcTS")
                    .expect("Valid pubkey"),
            );
            allowed_programs.push(
                Pubkey::from_str("GjSjmPt8VSHr49ti4BijWZSu7rwb8o32pod7gNBnTY4U")
                    .expect("Valid pubkey"),
            );
        } else if uri.starts_with("spiffe://gridtokenx.th/prod/oracle-bridge") {
            // Oracle Bridge can only invoke Oracle program
            allowed_programs.push(
                Pubkey::from_str("AiWcoPDEk3G4iKrDXj1wCN1ffWxQDEsgtJZKcjauoFJr")
                    .expect("Valid pubkey"),
            );
        } else if uri.starts_with("spiffe://gridtokenx.th/prod/iam-service") {
            // IAM Service might interact with Registry or Governance
            allowed_programs.push(
                Pubkey::from_str("HZR6b8GhzhDowyL6dX58qBjdSDNtFyJHU5dPF3kXDcTS")
                    .expect("Valid pubkey"),
            );
        } else {
            warn!("🛡️ PolicyEngine: Unknown or unsupported SPIFFE ID: {}", uri);
            return Err(anyhow::anyhow!(
                "PolicyEngine: Unauthorized SPIFFE identity"
            ));
        }

        // Validate each instruction
        for (i, instruction) in transaction.message.instructions.iter().enumerate() {
            let program_id =
                transaction.message.account_keys[instruction.program_id_index as usize];
            if !allowed_programs.contains(&program_id) {
                warn!(
                    "🚨 PolicyEngine BLOCK: Identity {} attempted to invoke unauthorized program {} at instruction index {}",
                    uri, program_id, i
                );
                return Err(anyhow::anyhow!(
                    "PolicyEngine: Unauthorized program ID {}",
                    program_id
                ));
            }
        }

        info!("🛡️ PolicyEngine: Transaction passed allowlist for {}", uri);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::instruction::Instruction;
    use solana_sdk::message::Message;

    fn create_mock_tx(program_id: Pubkey) -> Transaction {
        let instruction = Instruction {
            program_id,
            accounts: vec![],
            data: vec![],
        };
        let message = Message::new(&[instruction], None);
        Transaction::new_unsigned(message)
    }

    #[test]
    fn test_trading_service_allowed() {
        let identity = SpiffeIdentity("spiffe://gridtokenx.th/prod/trading-service/matcher".to_string());
        let trading_prog = Pubkey::from_str("DXxHdUar3pUUKRnt4XAMA8rdYRpAsNY1xk3Zo4crShvY").unwrap();
        let tx = create_mock_tx(trading_prog);
        assert!(PolicyEngine::validate_transaction(&identity, &tx).is_ok());
    }

    #[test]
    fn test_trading_service_denied_oracle() {
        let identity = SpiffeIdentity("spiffe://gridtokenx.th/prod/trading-service/matcher".to_string());
        let oracle_prog = Pubkey::from_str("AiWcoPDEk3G4iKrDXj1wCN1ffWxQDEsgtJZKcjauoFJr").unwrap();
        let tx = create_mock_tx(oracle_prog);
        assert!(PolicyEngine::validate_transaction(&identity, &tx).is_err());
    }

    #[test]
    fn test_system_program_allowed_globally() {
        let identity = SpiffeIdentity("spiffe://gridtokenx.th/prod/oracle-bridge".to_string());
        let sys_prog = Pubkey::from_str("11111111111111111111111111111111").unwrap();
        let tx = create_mock_tx(sys_prog);
        assert!(PolicyEngine::validate_transaction(&identity, &tx).is_ok());
    }
}
