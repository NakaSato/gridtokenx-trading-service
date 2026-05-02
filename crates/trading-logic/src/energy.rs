use trading_engine::engine::TopologySnapshot;
use rust_decimal::Decimal;
use gridtokenx_blockchain_core::island::IslandRegistry;
use std::collections::HashMap;
use parking_lot::RwLock;

/// Grid-Aware Topology that enforces island microgrid constraints.
pub struct GridAwareTopology {
    /// Track committed flow per island (MW) for the current cycle
    committed_island_flow: RwLock<HashMap<i32, Decimal>>,
}

impl GridAwareTopology {
    pub fn new() -> Self {
        Self {
            committed_island_flow: RwLock::new(HashMap::new()),
        }
    }
}

impl TopologySnapshot for GridAwareTopology {
    fn can_accommodate_flow(&self, from_zone: Option<i32>, to_zone: Option<i32>, amount: Decimal) -> bool {
        // If the flow is going to an island, check the submarine cable capacity
        if let Some(target_zone) = to_zone {
            if let Some(config) = IslandRegistry::get_island_config(target_zone) {
                // If it's an intra-island trade, it's fine (no cable usage)
                if from_zone == to_zone {
                    return true;
                }

                // If it's an import from mainland (or another island), check cable capacity
                let committed = {
                    let flow = self.committed_island_flow.read();
                    *flow.get(&config.id).unwrap_or(&Decimal::ZERO)
                };

                let new_flow_mw = amount / rust_decimal_macros::dec!(1000.0);
                if (committed + new_flow_mw) > Decimal::from_f64_retain(config.submarine_cable_capacity_mw).unwrap_or(Decimal::MAX) {
                    return false;
                }

                // Update committed flow
                // NOTE: In a multi-threaded CDA match, we might need a more sophisticated commit/rollback,
                // but for the synchronous cycle, we can commit optimisticlly during the `can_accommodate_flow` check
                // or after the match. Since match_cycle calls this repeatedly, we should commit here.
                let mut flow = self.committed_island_flow.write();
                let entry = flow.entry(config.id).or_default();
                *entry += new_flow_mw;
            }
        }
        true
    }

    fn calculate_wheeling_charge(&self, from_zone: Option<i32>, to_zone: Option<i32>) -> trading_core::fast_price::FastPrice {
        if from_zone == to_zone {
            trading_core::fast_price::FastPrice::ZERO
        } else {
            // Apply higher wheeling charge for island imports to reflect submarine cable costs
            if let Some(target) = to_zone {
                if IslandRegistry::get_island_config(target).is_some() {
                    return trading_core::fast_price::FastPrice::from(rust_decimal_macros::dec!(0.05));
                }
            }
            trading_core::fast_price::FastPrice::from(rust_decimal_macros::dec!(0.02))
        }
    }

    fn calculate_loss_factor(&self, from_zone: Option<i32>, to_zone: Option<i32>) -> trading_core::fast_price::FastPrice {
        if from_zone == to_zone {
            trading_core::fast_price::FastPrice::from(rust_decimal_macros::dec!(1.01))
        } else {
            // Submarine cables have slightly higher loss factors
            if let Some(target) = to_zone {
                if IslandRegistry::get_island_config(target).is_some() {
                    return trading_core::fast_price::FastPrice::from(rust_decimal_macros::dec!(1.05));
                }
            }
            trading_core::fast_price::FastPrice::from(rust_decimal_macros::dec!(1.03))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_island_bottleneck_enforcement() {
        let topology = GridAwareTopology::new();
        
        // Ko Tao Capacity: 15 MW
        let zone_tao = 301;
        let zone_mainland = 1;

        // 1. Intra-island trade (10 MW) - Should be allowed regardless of cable
        assert!(topology.can_accommodate_flow(Some(zone_tao), Some(zone_tao), dec!(10000.0)));
        
        // 2. Import from mainland (10 MW) - Should be allowed (10 MW < 15 MW)
        assert!(topology.can_accommodate_flow(Some(zone_mainland), Some(zone_tao), dec!(10000.0)));
        
        // 3. Another import from mainland (6 MW) - Should be blocked (10 + 6 = 16 MW > 15 MW)
        assert!(!topology.can_accommodate_flow(Some(zone_mainland), Some(zone_tao), dec!(6000.0)));
        
        // 4. Smaller import (4 MW) - Should be allowed (10 + 4 = 14 MW < 15 MW)
        assert!(topology.can_accommodate_flow(Some(zone_mainland), Some(zone_tao), dec!(4000.0)));
    }
}
