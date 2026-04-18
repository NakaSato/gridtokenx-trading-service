use trading_engine::engine::TopologySnapshot;
use rust_decimal::Decimal;

/// Simple implementation of TopologySnapshot for the modular monolith scaffold.
pub struct StaticTopology;

impl TopologySnapshot for StaticTopology {
    fn can_accommodate_flow(&self, _from_zone: Option<i32>, _to_zone: Option<i32>, _amount: Decimal) -> bool {
        // In a real system, this would check grid capacity constraints.
        true
    }

    fn calculate_wheeling_charge(&self, from_zone: Option<i32>, to_zone: Option<i32>) -> trading_core::fast_price::FastPrice {
        if from_zone == to_zone {
            trading_core::fast_price::FastPrice::ZERO
        } else {
            trading_core::fast_price::FastPrice::from(rust_decimal_macros::dec!(0.02)) // Flat 0.02 charge for inter-zone
        }
    }

    fn calculate_loss_factor(&self, from_zone: Option<i32>, to_zone: Option<i32>) -> trading_core::fast_price::FastPrice {
        if from_zone == to_zone {
            trading_core::fast_price::FastPrice::from(rust_decimal_macros::dec!(1.01)) // 1% loss intra-zone (1.01 factor)
        } else {
            trading_core::fast_price::FastPrice::from(rust_decimal_macros::dec!(1.03)) // 3% loss inter-zone (1.03 factor)
        }
    }
}
