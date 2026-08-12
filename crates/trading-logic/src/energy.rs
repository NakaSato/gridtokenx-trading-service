use rust_decimal::Decimal;
use std::sync::Arc;
use trading_core::charges::ChargeRates;
use trading_engine::engine::TopologySnapshot;

/// Grid-Aware Topology that enforces island microgrid constraints.
///
/// # Wheeling and loss come from the chain, not from here
///
/// This type used to hardcode its own schedule — wheeling `0` intra-zone /
/// `0.02` cross-zone, loss `1.01` / `1.03` — while the chain charged a flat
/// `TariffConfig.wheeling_rate_per_kwh` (0.10 THB/kWh, no zone dimension) and
/// `loss_bps` (5 = 0.05%). Those numbers fed the landed cost that decides
/// whether a pair crosses, so the buyer was filtered against one tariff and the
/// seller billed another, and the gap fell entirely on the seller: an intra-zone
/// match crossed with wheeling priced at **zero**, then settled with 0.10/kWh
/// deducted from the seller's proceeds. On a 4.00 THB/kWh ask that is 2.5% of
/// the seller's revenue, on every trade, invisible to both sides at match time.
///
/// Both hooks now read the same `ChargeRates` the settlement ledger uses, so a
/// pair crosses only when the bid actually covers the ask plus the charges the
/// chain will levy. The units line up exactly: wheeling is a flat per-kWh
/// currency amount (what `calculate_wheeling_charge` returns) and loss is a
/// factor `1 + loss_bps/10_000` (what `calculate_loss_factor` returns).
///
/// Note this does **not** make wheeling a pass-through to the buyer. The trade
/// settles at the seller's ask (`MatchResult::settle_price`), so the buyer pays
/// `q * ask` and the charges still come out of the seller's side. What changes
/// is that the crossing test no longer understates them.
pub struct GridAwareTopology {
    rates: Arc<dyn ChargeRates>,
}

impl GridAwareTopology {
    #[must_use]
    pub fn new(rates: Arc<dyn ChargeRates>) -> Self {
        Self { rates }
    }
}

impl TopologySnapshot for GridAwareTopology {
    /// **No grid constraint is applied off-chain — every flow is admitted.**
    ///
    /// This is not a stub awaiting a small patch; it currently mirrors the chain.
    /// The on-chain throttle is itself inert, because it is gated on a capacity
    /// nobody sets: `settle_offchain_match` only consults `ZoneCapacity` when
    /// `zone_market.capacity > 0`, and **every** initialization path creates zone
    /// markets with capacity `0` (= uncapped) — `bootstrap.ts` and
    /// `init-zone-markets.ts` (what `scripts/init-zones.sh` runs) both pass `0`
    /// explicitly. Only litesvm fixtures and throughput benches set a real number.
    ///
    /// So there is no ceiling anywhere in the deployed system for this method to
    /// mirror, and returning `true` is the accurate answer rather than a missing
    /// one. Making it enforce would mean inventing a limit the chain does not have,
    /// and would reject trades that settle perfectly well today.
    ///
    /// Enforcing here becomes worth doing the moment zones are given finite
    /// capacities — and at that point it is required, not optional: the engine
    /// commits cross-zone flow with no ceiling awareness, so a capped zone would
    /// match trades the chain then refuses `CapacityExceeded`, which settlement now
    /// parks permanently (see the deterministic-rejection path in
    /// `SettlementService`). The missing piece is a read path for
    /// `ZoneMarket.capacity` / `ZoneCapacity.committed_flow`, which
    /// `BlockchainGateway` does not expose today — `get_zone_config` carries the
    /// multiplier, wheeling and maintenance flag, but no capacity.
    fn can_accommodate_flow(
        &self,
        _from_zone: Option<i32>,
        _to_zone: Option<i32>,
        _amount: Decimal,
    ) -> bool {
        true
    }

    /// Zone-independent on purpose: the chain's wheeling charge has no zone
    /// dimension, so pricing one here would reintroduce the mismatch this reads
    /// the tariff to avoid. A grid constraint belongs in `can_accommodate_flow`
    /// — which, note, currently returns `true` for everything, so no zone
    /// constraint is applied off-chain at all.
    fn calculate_wheeling_charge(
        &self,
        _from_zone: Option<i32>,
        _to_zone: Option<i32>,
    ) -> trading_core::fast_price::FastPrice {
        trading_core::fast_price::FastPrice::from(trading_core::charges::wheeling_per_kwh(
            self.rates.as_ref(),
        ))
    }

    /// Also zone-independent — `TariffConfig.loss_bps` is a single rate applied to
    /// every settled match.
    fn calculate_loss_factor(
        &self,
        _from_zone: Option<i32>,
        _to_zone: Option<i32>,
    ) -> trading_core::fast_price::FastPrice {
        trading_core::fast_price::FastPrice::from(trading_core::charges::loss_factor(
            self.rates.as_ref(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use trading_core::charges::StaticChargeRates;

    /// The rates deployed on the dev validator (`bootstrap.ts`): 0.10 THB/kWh
    /// wheeling, 5 bps loss, 25 bps market fee.
    fn live_rates() -> Arc<dyn ChargeRates> {
        Arc::new(StaticChargeRates {
            fee_bps: 25,
            wheeling_rate_per_kwh: 100_000,
            loss_bps: 5,
        })
    }

    #[test]
    fn test_island_bottleneck_enforcement() {
        let topology = GridAwareTopology::new(live_rates());

        // Without IslandRegistry, all flows are currently accommodated
        let zone_tao = 301;
        let zone_mainland = 1;

        // 1. Intra-island trade
        assert!(topology.can_accommodate_flow(Some(zone_tao), Some(zone_tao), dec!(10000.0)));

        // 2. Import from mainland
        assert!(topology.can_accommodate_flow(Some(zone_mainland), Some(zone_tao), dec!(10000.0)));
    }

    /// The regression this type was rewritten for: an intra-zone pair used to be
    /// priced with wheeling **zero**, then settled with the chain's flat
    /// 0.10/kWh taken out of the seller. The landed cost must now carry the same
    /// charge the chain will bill, in every zone.
    #[test]
    fn wheeling_is_the_on_chain_rate_in_every_zone() {
        let topology = GridAwareTopology::new(live_rates());
        let intra = topology.calculate_wheeling_charge(Some(7), Some(7));
        let cross = topology.calculate_wheeling_charge(Some(7), Some(9));

        assert_eq!(intra.to_decimal(), dec!(0.100000), "intra-zone is NOT free");
        assert_eq!(intra, cross, "the chain's wheeling has no zone dimension");
    }

    /// Same for loss: 5 bps on-chain is a factor of 1.0005, not the 1.01/1.03
    /// this used to invent — which overstated the buyer's landed cost by ~20x
    /// while the chain billed the seller a twentieth of it.
    #[test]
    fn loss_factor_is_the_on_chain_rate_in_every_zone() {
        let topology = GridAwareTopology::new(live_rates());
        let intra = topology.calculate_loss_factor(Some(7), Some(7));
        let cross = topology.calculate_loss_factor(Some(7), Some(9));

        assert_eq!(intra.to_decimal(), dec!(1.000500));
        assert_eq!(intra, cross, "the chain's loss_bps has no zone dimension");
    }

    /// A tariff-free market must not have a crossing bar bolted onto it: with
    /// zero rates the landed cost has to collapse back to the bare ask.
    #[test]
    fn zero_rates_add_nothing_to_the_landed_cost() {
        let topology = GridAwareTopology::new(Arc::new(StaticChargeRates::ZERO));

        assert_eq!(
            topology.calculate_wheeling_charge(Some(1), Some(2)),
            trading_core::fast_price::FastPrice::ZERO
        );
        assert_eq!(
            topology
                .calculate_loss_factor(Some(1), Some(2))
                .to_decimal(),
            dec!(1.000000),
            "unit factor — no loss uplift"
        );
    }
}
