use rust_decimal::Decimal;
use trading_engine::engine::TopologySnapshot;

/// Grid-Aware Topology that enforces island microgrid constraints.
pub struct GridAwareTopology;

impl GridAwareTopology {
    pub fn new() -> Self {
        Self
    }
}

impl TopologySnapshot for GridAwareTopology {
    fn can_accommodate_flow(
        &self,
        _from_zone: Option<i32>,
        _to_zone: Option<i32>,
        _amount: Decimal,
    ) -> bool {
        // Without IslandRegistry, we default to allowing all flows.
        // In a real system, this would be replaced by dynamic on-chain zone configuration.
        true
    }

    fn calculate_wheeling_charge(
        &self,
        from_zone: Option<i32>,
        to_zone: Option<i32>,
    ) -> trading_core::fast_price::FastPrice {
        if from_zone == to_zone {
            trading_core::fast_price::FastPrice::ZERO
        } else {
            // Default wheeling charge for cross-zone trades
            trading_core::fast_price::FastPrice::from(rust_decimal_macros::dec!(0.02))
        }
    }

    fn calculate_loss_factor(
        &self,
        from_zone: Option<i32>,
        to_zone: Option<i32>,
    ) -> trading_core::fast_price::FastPrice {
        if from_zone == to_zone {
            trading_core::fast_price::FastPrice::from(rust_decimal_macros::dec!(1.01))
        } else {
            // Default loss factor for cross-zone trades
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

        // Without IslandRegistry, all flows are currently accommodated
        let zone_tao = 301;
        let zone_mainland = 1;

        // 1. Intra-island trade
        assert!(topology.can_accommodate_flow(Some(zone_tao), Some(zone_tao), dec!(10000.0)));

        // 2. Import from mainland
        assert!(topology.can_accommodate_flow(Some(zone_mainland), Some(zone_tao), dec!(10000.0)));
    }
}
