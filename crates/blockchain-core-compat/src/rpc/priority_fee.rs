use anyhow::Result;
use solana_sdk::instruction::Instruction;
use tracing::{debug, info};

/// Types of transactions for priority fee recommendations
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TransactionType {
    TokenMinting,
    Settlement,
    OrderCreation,
    ERCIssuance,
    WalletConnection,
    UserRegistration,
    MeterRegistration,
    MeterReading,
    TokenTransfer,
}

impl TransactionType {
    pub fn description(&self) -> &'static str {
        match self {
            TransactionType::TokenMinting => "Energy token minting",
            TransactionType::Settlement => "Trade settlement transfer",
            TransactionType::OrderCreation => "Trading order creation",
            TransactionType::ERCIssuance => "ERC certificate issuance",
            TransactionType::WalletConnection => "Wallet connection",
            TransactionType::UserRegistration => "User registration on blockchain",
            TransactionType::MeterRegistration => "Smart meter registration",
            TransactionType::MeterReading => "Meter reading submission",
            TransactionType::TokenTransfer => "Token transfer between accounts",
        }
    }

    pub fn should_use_priority_fees(&self) -> bool {
        match self {
            TransactionType::WalletConnection => false,
            TransactionType::MeterReading => false,
            _ => true,
        }
    }
}

/// Priority fee levels for Solana transactions
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PriorityLevel {
    Low,
    Medium,
    High,
}

impl PriorityLevel {
    pub fn micro_lamports_per_cu(&self) -> u64 {
        match self {
            PriorityLevel::Low => 1_000,
            PriorityLevel::Medium => 10_000,
            PriorityLevel::High => 50_000,
        }
    }

    pub fn description(&self) -> &'static str {
        match self {
            PriorityLevel::Low => "Low priority - slower confirmation",
            PriorityLevel::Medium => "Medium priority - balanced speed/cost",
            PriorityLevel::High => "High priority - fastest confirmation",
        }
    }

    pub fn estimated_slots(&self) -> u64 {
        match self {
            PriorityLevel::Low => 10,
            PriorityLevel::Medium => 5,
            PriorityLevel::High => 2,
        }
    }
}

pub struct PriorityFeeService;

impl PriorityFeeService {
    pub fn add_priority_fee(
        instructions: &mut Vec<Instruction>,
        priority_level: PriorityLevel,
        compute_limit: Option<u64>,
    ) -> Result<()> {
        let micro_lamports = priority_level.micro_lamports_per_cu();
        Self::add_priority_fee_raw(
            instructions,
            micro_lamports,
            compute_limit,
            Some(priority_level.description()),
        )
    }

    pub fn add_priority_fee_raw(
        instructions: &mut Vec<Instruction>,
        micro_lamports: u64,
        compute_limit: Option<u64>,
        priority_desc: Option<&str>,
    ) -> Result<()> {
        let compute_limit = compute_limit.unwrap_or(200_000);

        debug!(
            "Adding priority fee: {} micro-lamports per CU, compute limit: {}",
            micro_lamports, compute_limit
        );

        use solana_sdk::compute_budget::ComputeBudgetInstruction;
        instructions.insert(
            0,
            ComputeBudgetInstruction::set_compute_unit_limit(compute_limit as u32),
        );
        instructions.insert(
            1,
            ComputeBudgetInstruction::set_compute_unit_price(micro_lamports),
        );

        info!(
            "Priority fee added: {} ({} micro-lamports/CU)",
            priority_desc.unwrap_or("Dynamic"),
            micro_lamports
        );

        Ok(())
    }

    pub fn recommend_priority_level(transaction_type: TransactionType) -> PriorityLevel {
        match transaction_type {
            TransactionType::TokenMinting => PriorityLevel::Medium,
            TransactionType::Settlement => PriorityLevel::High,
            TransactionType::OrderCreation => PriorityLevel::Low,
            TransactionType::ERCIssuance => PriorityLevel::Medium,
            TransactionType::WalletConnection => PriorityLevel::Low,
            TransactionType::UserRegistration => PriorityLevel::Medium,
            TransactionType::MeterRegistration => PriorityLevel::Medium,
            TransactionType::MeterReading => PriorityLevel::Low,
            TransactionType::TokenTransfer => PriorityLevel::Medium,
        }
    }

    pub fn recommend_compute_limit(transaction_type: TransactionType) -> u64 {
        match transaction_type {
            TransactionType::TokenMinting => 150_000,
            TransactionType::Settlement => 300_000,
            TransactionType::OrderCreation => 100_000,
            TransactionType::ERCIssuance => 250_000,
            TransactionType::WalletConnection => 50_000,
            TransactionType::UserRegistration => 100_000,
            TransactionType::MeterRegistration => 120_000,
            TransactionType::MeterReading => 80_000,
            TransactionType::TokenTransfer => 100_000,
        }
    }

    pub fn calculate_adaptive_fee(
        recent_fees: &[PrioritizationFeeResult],
        priority_level: PriorityLevel,
        _tx_type: TransactionType,
    ) -> u64 {
        if recent_fees.is_empty() {
            return priority_level.micro_lamports_per_cu();
        }
        let mut fees: Vec<u64> = recent_fees.iter().map(|f| f.prioritization_fee).collect();
        fees.sort_unstable();
        let len = fees.len();
        let index = match priority_level {
            PriorityLevel::Low => len / 4,
            PriorityLevel::Medium => len / 2,
            PriorityLevel::High => (len * 3) / 4,
        };
        let calculated_fee = fees.get(index).cloned().unwrap_or(0);
        let floor = priority_level.micro_lamports_per_cu();
        let final_fee = std::cmp::max(calculated_fee, floor);
        std::cmp::min(final_fee, 500_000)
    }
}

/// Local replacement for solana_client::rpc_response::RpcPrioritizationFee
#[derive(Debug, Clone, Copy)]
pub struct PrioritizationFeeResult {
    pub slot: u64,
    pub prioritization_fee: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recommend_priority_level() {
        assert_eq!(
            PriorityFeeService::recommend_priority_level(TransactionType::Settlement),
            PriorityLevel::High
        );
        assert_eq!(
            PriorityFeeService::recommend_priority_level(TransactionType::OrderCreation),
            PriorityLevel::Low
        );
    }

    #[test]
    fn test_recommend_compute_limit() {
        assert_eq!(
            PriorityFeeService::recommend_compute_limit(TransactionType::Settlement),
            300_000
        );
        assert_eq!(
            PriorityFeeService::recommend_compute_limit(TransactionType::TokenMinting),
            150_000
        );
    }

    #[test]
    fn test_calculate_adaptive_fee_empty() {
        let fee = PriorityFeeService::calculate_adaptive_fee(
            &[],
            PriorityLevel::Medium,
            TransactionType::TokenTransfer,
        );
        assert_eq!(fee, PriorityLevel::Medium.micro_lamports_per_cu());
    }

    #[test]
    fn test_calculate_adaptive_fee_with_data() {
        let recent_fees = vec![
            PrioritizationFeeResult {
                slot: 1,
                prioritization_fee: 100,
            },
            PrioritizationFeeResult {
                slot: 2,
                prioritization_fee: 200,
            },
            PrioritizationFeeResult {
                slot: 3,
                prioritization_fee: 300,
            },
            PrioritizationFeeResult {
                slot: 4,
                prioritization_fee: 400,
            },
        ];

        // Low -> index 1 -> 200. Floor is 1000. So 1000.
        let fee_low = PriorityFeeService::calculate_adaptive_fee(
            &recent_fees,
            PriorityLevel::Low,
            TransactionType::TokenTransfer,
        );
        assert_eq!(fee_low, 1_000);

        // High -> index 3 -> 400. Floor is 50000. So 50000.
        let fee_high = PriorityFeeService::calculate_adaptive_fee(
            &recent_fees,
            PriorityLevel::High,
            TransactionType::TokenTransfer,
        );
        assert_eq!(fee_high, 50_000);

        // If market fees are higher than floor
        let expensive_fees = vec![
            PrioritizationFeeResult {
                slot: 1,
                prioritization_fee: 100_000,
            },
            PrioritizationFeeResult {
                slot: 2,
                prioritization_fee: 200_000,
            },
        ];
        // Medium -> index 1 -> 200000. Floor is 10000. So 200000.
        let fee_med = PriorityFeeService::calculate_adaptive_fee(
            &expensive_fees,
            PriorityLevel::Medium,
            TransactionType::TokenTransfer,
        );
        assert_eq!(fee_med, 200_000);
    }

    #[test]
    fn test_add_priority_fee_instructions() {
        let mut ixs = vec![];
        PriorityFeeService::add_priority_fee(&mut ixs, PriorityLevel::High, Some(400_000)).unwrap();

        assert_eq!(ixs.len(), 2);
        // Correct order: limit then price
        // Note: We'd need to inspect the data but for now we just check the length
    }
}
