use crate::auth::{ServiceRole, SpiffeIdentity};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::Transaction;
use tracing::{info, warn};

const SYSTEM_PROGRAM: Pubkey = Pubkey::from_str_const("11111111111111111111111111111111");
const TRADING_PROGRAM: Pubkey =
    Pubkey::from_str_const("DXxHdUar3pUUKRnt4XAMA8rdYRpAsNY1xk3Zo4crShvY");
const REGISTRY_PROGRAM: Pubkey =
    Pubkey::from_str_const("HZR6b8GhzhDowyL6dX58qBjdSDNtFyJHU5dPF3kXDcTS");
const ENERGY_TOKEN_PROGRAM: Pubkey =
    Pubkey::from_str_const("GjSjmPt8VSHr49ti4BijWZSu7rwb8o32pod7gNBnTY4U");
const ORACLE_PROGRAM: Pubkey =
    Pubkey::from_str_const("AiWcoPDEk3G4iKrDXj1wCN1ffWxQDEsgtJZKcjauoFJr");

pub struct PolicyEngine;

impl PolicyEngine {
    /// Validates that every instruction in the transaction is calling an explicitly allowlisted program ID
    /// for the given SPIFFE identity.
    pub fn validate_transaction(
        identity: &SpiffeIdentity,
        transaction: &Transaction,
    ) -> anyhow::Result<()> {
        let uri = &identity.0;
        // Resolve via the hardened segment-boundary matcher (auth::ServiceRole)
        // rather than loose `starts_with`, so a look-alike identity like
        // `…/iam-service-evil` can't borrow another service's allowlist.
        let role = ServiceRole::from(identity);

        // Dev mode / Admin override
        if role == ServiceRole::Admin
            || std::env::var("CHAIN_BRIDGE_INSECURE")
                .map(|v| v.to_lowercase() == "true")
                .unwrap_or(false)
        {
            info!("🛡️ PolicyEngine: Bypassing program ID check for Admin/Insecure mode");
            return Ok(());
        }

        let mut allowed_programs = vec![SYSTEM_PROGRAM];

        match role {
            // Trading Matcher/API can invoke Trading, Registry, and Energy Token
            ServiceRole::TradingApi | ServiceRole::TradingMatcher => {
                allowed_programs.push(TRADING_PROGRAM);
                allowed_programs.push(REGISTRY_PROGRAM);
                allowed_programs.push(ENERGY_TOKEN_PROGRAM);
            }
            // Oracle Bridge can only invoke Oracle program
            ServiceRole::OracleBridge => {
                allowed_programs.push(ORACLE_PROGRAM);
            }
            // IAM Service might interact with Registry or Governance
            ServiceRole::IamService => {
                allowed_programs.push(REGISTRY_PROGRAM);
            }
            _ => {
                warn!("🛡️ PolicyEngine: Unknown or unsupported SPIFFE ID: {}", uri);
                return Err(anyhow::anyhow!("PolicyEngine: Unauthorized SPIFFE identity"));
            }
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
        let identity =
            SpiffeIdentity("spiffe://gridtokenx.th/prod/trading-service/matcher".to_string());
        let tx = create_mock_tx(TRADING_PROGRAM);
        assert!(PolicyEngine::validate_transaction(&identity, &tx).is_ok());
    }

    #[test]
    fn test_trading_service_denied_oracle() {
        let identity =
            SpiffeIdentity("spiffe://gridtokenx.th/prod/trading-service/matcher".to_string());
        let tx = create_mock_tx(ORACLE_PROGRAM);
        assert!(PolicyEngine::validate_transaction(&identity, &tx).is_err());
    }

    #[test]
    fn test_system_program_allowed_globally() {
        let identity = SpiffeIdentity("spiffe://gridtokenx.th/prod/oracle-bridge".to_string());
        let tx = create_mock_tx(SYSTEM_PROGRAM);
        assert!(PolicyEngine::validate_transaction(&identity, &tx).is_ok());
    }

    #[test]
    fn test_lookalike_identity_denied() {
        // A look-alike of iam-service must NOT inherit its Registry allowlist.
        let identity =
            SpiffeIdentity("spiffe://gridtokenx.th/prod/iam-service-evil".to_string());
        let tx = create_mock_tx(REGISTRY_PROGRAM);
        assert!(PolicyEngine::validate_transaction(&identity, &tx).is_err());
    }

    #[test]
    fn test_iam_service_subpath_allowed() {
        // A legitimate workload sub-path still resolves to IamService.
        let identity =
            SpiffeIdentity("spiffe://gridtokenx.th/prod/iam-service/workload-1".to_string());
        let tx = create_mock_tx(REGISTRY_PROGRAM);
        assert!(PolicyEngine::validate_transaction(&identity, &tx).is_ok());
    }
}
