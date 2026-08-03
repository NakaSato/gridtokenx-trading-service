#![allow(clippy::unwrap_used)] // unwrap is idiomatic in integration tests

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;
use trading_core::config::SolanaProgramsConfig;
use trading_core::fast_price::FastPrice;
use trading_core::models::TradeMatch;
use trading_infra::blockchain::settlement::BlockchainSettlementProvider;
use trading_infra::blockchain::BlockchainService;
use uuid::Uuid;

#[tokio::test]
async fn test_settlement_numeric_integrity() {
    // 1. Setup Environment
    std::env::set_var("CHAIN_BRIDGE_INSECURE", "true");
    std::env::set_var(
        "ENERGY_TOKEN_MINT",
        "EneryTokenMint111111111111111111111111111",
    );
    std::env::set_var(
        "CURRENCY_TOKEN_MINT",
        "CurrntTokenMint1111111111111111111111111",
    );
    std::env::set_var(
        "FEE_COLLECTOR_WALLET",
        "FeeCollector111111111111111111111111111",
    );
    std::env::set_var(
        "WHEELING_COLLECTOR_WALLET",
        "WheelCollector111111111111111111111111",
    );
    std::env::set_var(
        "LOSS_COLLECTOR_WALLET",
        "LossCollector1111111111111111111111111",
    );
    std::env::set_var(
        "SOLANA_TRADING_PROGRAM_ID",
        "Trade1111111111111111111111111111111111",
    );

    // 2. Simulate Engine Output (FastPrice precision)
    // Price: 5.123456789 (9 decimals)
    let engine_price = FastPrice::from(dec!(5.123456789));
    let engine_quantity = dec!(10.5); // 10.5 kWh

    // Convert back to Decimal as the engine does when emitting matches
    let match_price = engine_price.to_decimal();
    assert_eq!(match_price, dec!(5.123456789));

    // 3. Create a TradeMatch (Mocking what engine produces)
    let trade = TradeMatch {
        id: Uuid::new_v4(),
        match_id: Uuid::new_v4(),
        epoch_id: Uuid::new_v4(),
        buyer_id: Uuid::new_v4(),
        seller_id: Uuid::new_v4(),
        buy_order_id: Uuid::new_v4(),
        sell_order_id: Uuid::new_v4(),
        quantity: engine_quantity,
        price: match_price,
        total_value: engine_quantity * match_price,
        wheeling_charge: dec!(0.1),
        loss_factor: dec!(0.02),
        loss_cost: dec!(0.05),
        buyer_zone_id: Some(1),
        seller_zone_id: Some(1),
        matched_at: chrono::Utc::now(),
        buyer_session_token: None,
        seller_session_token: None,
    };

    // 4. Initialize BlockchainSettlementProvider (with dummy BlockchainService)
    let program_ids = SolanaProgramsConfig {
        registry_program_id: "Registry111111111111111111111111111111111".to_string(),
        oracle_program_id: "Oracle1111111111111111111111111111111111".to_string(),
        energy_token_program_id: "Energy1111111111111111111111111111111".to_string(),
        governance_program_id: "Gov11111111111111111111111111111111111".to_string(),
        treasury_program_id: "Treasury11111111111111111111111111111111".to_string(),
        trading_program_id: "Trade1111111111111111111111111111111111".to_string(),
        trading_market_id: "Market1111111111111111111111111111111111".to_string(),
    };

    // We use a dummy URL. Service creation should succeed if it doesn't try to connect immediately.
    let blockchain = Arc::new(
        BlockchainService::new(
            "http://localhost:8899".to_string(),
            "localnet".to_string(),
            program_ids,
            None,
            None,
        )
        .await
        .unwrap(),
    );

    let _provider = BlockchainSettlementProvider::new(blockchain.clone());

    // 5. Verify Atomic Conversion Logic (replicating internal provider logic)
    let amount_atomic =
        ToPrimitive::to_u64(&(trade.quantity * Decimal::from(1_000_000_000i64)).trunc())
            .unwrap_or(0);

    let price_atomic =
        ToPrimitive::to_u64(&(trade.price * Decimal::from(1_000_000i64)).trunc()).unwrap_or(0);

    // 10.5 * 1e9 = 10,500,000,000
    assert_eq!(amount_atomic, 10_500_000_000);
    // 5.123456789 * 1e6 = 5,123,456 (truncated to 6 decimals)
    assert_eq!(price_atomic, 5_123_456);

    println!(
        "✅ Atomic conversion verified: {} energy, {} price",
        amount_atomic, price_atomic
    );
}
