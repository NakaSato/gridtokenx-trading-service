use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaProgramsConfig {
    pub registry_program_id: String,
    pub oracle_program_id: String,
    pub governance_program_id: String,
    pub energy_token_program_id: String,
    pub trading_program_id: String,
    pub trading_market_id: String,
}

impl Default for SolanaProgramsConfig {
    fn default() -> Self {
        Self {
            registry_program_id: "C8RT8L5pZCVDrf9v94CNNk3XPBKZU5p4o4aPnAVQGiTu".to_string(),
            oracle_program_id: "DdeZQdfv7qtnhHktPt8CevKrW6BvjbgKknkD7c63C9hP".to_string(),
            governance_program_id: "AMowMcC3gVkEvZ3vaskGC4L9uTsBvTxcD4ewEA1TyrK4".to_string(),
            energy_token_program_id: "6ZoMJypt2vufxeUarFJRZxAvRfUsf7gRHZ1pRQTYerNp".to_string(),
            trading_program_id: "ctBDmdW3VHqqQF7HyEKwoMWszyNcKBNNFsofem3JEup".to_string(),
            trading_market_id: "".to_string(),
        }
    }
}
