# GridTokenX: Settlement Orchestration

The Settlement Orchestration layer ensures that virtual matches made in the trading engine are settled as atomic, immutable transactions on the blockchain.

## 1. Orchestration: `SettlementWorker`
The `SettlementWorker` is a background task that monitors the database for `Pending` settlement records.

*   **Frequency:** Continuous or batch-based (e.g., every 10 seconds).
*   **Responsibility:**
    *   Poll for settlements with `status = 'pending'`.
    *   Coordinate with the `BlockchainGateway` to execute transactions.
    *   Update settlement status based on on-chain confirmation.

## 2. Atomic Execution (On-chain)
GridTokenX uses a **single multi-instruction transaction** on the Solana blockchain to ensure that a trade either succeeds completely or fails entirely.

```rust
// crates/trading-infra/src/blockchain/settlement.rs

pub async fn execute_atomic_settlement(
    &self,
    settlement: &Settlement,
    // ... public keys ...
) -> Result<SettlementTransaction> {
    // 1. Build atomic instruction
    let instruction = self.blockchain.build_atomic_settlement_instruction(
        market_pda,
        buy_order_pda,
        sell_order_pda,
        buyer_currency_ata,
        seller_energy_ata,
        seller_currency_ata,
        buyer_energy_ata,
        fee_collector_ata,
        wheeling_collector_ata,
        loss_collector_ata,
        energy_mint,
        currency_mint,
        // ...
    )?;

    // 2. Execute on Solana
    let signature = self.blockchain.execute_batched_instructions(
        &[&platform_authority], 
        vec![instruction]
    ).await?;
    
    // ...
}
```

## 3. Reliability & Finality
The `SettlementWorker` implements a robust lifecycle to handle blockchain asynchronicity.

```rust
// crates/trading-logic/src/settlement.rs

pub async fn process_settlement(&self, settlement_id: Uuid) -> TraitResult<()> {
    // 1. Mark as Processing
    self.repo.update_settlement_status(settlement_id, "processing", ...).await?;

    // 2. Execute on-chain
    match self.blockchain.execute_settlement(&settlement).await {
        Ok(tx) => {
            // 3. Mark as Completed
            self.repo.update_settlement_status(settlement_id, "completed", Some(&tx.signature), None).await?;
        }
        Err(e) => {
            // 4. Mark as Failed for retry or manual intervention
            self.repo.update_settlement_status(settlement_id, "failed", None, Some(&e.to_string())).await?;
        }
    }
}
```

## 4. Oracle Integration (Direct Settlement)
Verified surplus energy triggers the **immediate minting** of new Energy Tokens.

```rust
// crates/trading-infra/src/blockchain/settlement.rs

pub async fn execute_generation_mint(
    &self,
    user_wallet: &Pubkey,
    amount_kwh: Decimal,
) -> Result<String> {
    // Build mint instruction (Atomic units: 9 decimals for GRX)
    let amount_atomic = (amount_kwh * dec!(1_000_000_000)).to_u64().unwrap();
    
    let instruction = self.blockchain.instruction_builder()
        .build_mint_to_wallet_instruction(
            mint_pda, user_ata, *user_wallet, ... amount_atomic
        )?;

    // Send transaction
    let signature = self.blockchain.build_and_send_transaction(
        vec![instruction], &[&platform_authority]
    ).await?;

    Ok(signature.to_string())
}
```
