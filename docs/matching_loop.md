# GridTokenX: The Matching Loop & CDA Logic

The Matching Loop is the heart of the GridTokenX trading service. It utilizes a **Continuous Double Auction (CDA)** model optimized for high-performance energy trading with physical grid awareness.

## 1. Orchestration: `MatcherWorker`
The `MatcherWorker` is a background task that triggers a matching cycle every ~1 second.

*   **Frequency:** ~1Hz (configurable).
*   **Responsibility:** 
    *   Fetch active `TradingOrder`s from the database.
    *   Transform domain models into optimized `FastOrder` structures.
    *   Execute the `MatchingEngine`.
    *   Persist resulting matches into the `SettlementRepository`.

## 2. Pure Logic Engine: `MatchingEngine`
The engine itself is **side-effect-free**. It does not perform I/O, database calls, or network requests. This ensures determinism and allows for high-speed unit testing.

```rust
// crates/trading-engine/src/engine.rs

pub fn match_cycle(
    buy_orders: &mut [FastOrder],
    sell_orders: &mut [FastOrder],
    // ... metadata ...
    topology: &dyn TopologySnapshot,
    now_ns: i64,
) -> (Vec<MatchResult>, CycleStats) {
    for buy in buy_orders.iter_mut() {
        // 1. Find candidates satisfying constraints
        let mut candidates = Vec::new();
        for sell in sell_orders.iter() {
            // Check grid flow capacity
            if !topology.can_accommodate_flow(sell.zone_id, buy.zone_id, amount) {
                continue;
            }
            // ... calculate landed cost ...
        }
    }
}
```

*   **Fixed-Point Math:** Uses `FastPrice` (scaled `i64`) to prevent floating-point non-determinism while maintaining nanosecond/sub-cent precision.
*   **Priority Rules:** 
    *   **Buyers:** FIFO (First-In, First-Out) based on creation timestamp.
    *   **Sellers:** Price-Time Priority (Lowest price first, then oldest timestamp).

## 3. Landed Cost Model
Unlike standard financial markets, energy trading must account for the physical cost of moving electrons. The engine calculates a **Landed Cost** for every potential match:

```rust
// crates/trading-engine/src/engine.rs

let wheeling_fp = topology.calculate_wheeling_charge(sell.zone_id, buy.zone_id);
let loss_fp = topology.calculate_loss_factor(sell.zone_id, buy.zone_id);

// Landed Cost = Base + Wheeling + LossCost
let extra_loss_raw = loss_fp.raw().saturating_sub(FastPrice::FACTOR); 
let loss_cost_extra_raw = (sell.price.raw() as i128 * extra_loss_raw as i128 / FastPrice::FACTOR as i128) as i64;

let mut landed_cost = FastPrice::from_raw(
    sell.price.raw() + wheeling_fp.raw() + loss_cost_extra_raw
);
```

*   **Wheeling Charge:** A flat fee per kWh for using grid infrastructure.
*   **Loss Factor:** A percentage of energy lost during transmission (e.g., 1.03 = 3% loss).
*   **Intra-zone Reward:** A 5% discount is automatically applied if the Buyer and Seller are in the same `zone_id`.

## 4. Topology & Grid Enforcement
The matching engine is "Grid-Aware." Before confirming a match, it queries a `TopologySnapshot` to ensure the trade is physically possible.

```rust
// crates/trading-logic/src/energy.rs

impl TopologySnapshot for GridAwareTopology {
    fn can_accommodate_flow(&self, from_zone: Option<i32>, to_zone: Option<i32>, amount: Decimal) -> bool {
        if let Some(target_zone) = to_zone {
            if let Some(config) = IslandRegistry::get_island_config(target_zone) {
                let committed = self.committed_island_flow.read().get(&config.id).cloned().unwrap_or_default();
                let new_flow_mw = amount / dec!(1000.0);

                // Check against submarine cable capacity (e.g. 15MW for Ko Tao)
                if (committed + new_flow_mw) > Decimal::from_f64(config.submarine_cable_capacity_mw).unwrap() {
                    return false;
                }
                // ... update committed flow ...
            }
        }
        true
    }
}
```

## 5. Output: `MatchResult` & `Settlement`
If a match satisfies both price and physical constraints:

1.  **MatchResult:** The engine generates a result containing the price, amount, and specific grid charges (wheeling/losses).
2.  **Order Update:** The involved orders are updated to `Filled` or `PartiallyFilled` status.
3.  **Settlement Creation:** A new `Settlement` record is created in the `Pending` state. This record serves as the input for the on-chain settlement worker.
