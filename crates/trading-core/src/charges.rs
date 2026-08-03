//! What a settled trade actually costs, and who is charged.
//!
//! # Why this exists
//!
//! The `settlements` row used to record the matching engine's own estimates:
//! `fee_amount` hardcoded to zero, `net_amount` set to the gross, and
//! wheeling/loss taken from the engine's landed-cost calculation. None of that is
//! what the chain charges. Measured on a live 1 kWh @ THB1.00 trade the seller
//! received **0.897** while the database claimed net 1.00, fee 0.00, wheeling 0.00
//! and loss 0.01 — every field wrong, and `loss` wrong in the *opposite* direction
//! (20x too high) from `wheeling` (entirely missing).
//!
//! The engine's wheeling/loss numbers are not a bug in themselves: they price the
//! *landed cost* that decides whether a pair crosses. They simply are not the
//! tariff the chain applies at settlement, and only the latter belongs in the
//! ledger.
//!
//! # The authoritative rates
//!
//! Three rates across two on-chain accounts, read via
//! `gridtokenx_blockchain_core::rpc::instructions::market_accounts`:
//!
//! | Rate | Account | Live value |
//! |---|---|---|
//! | `fee_bps` | `Market.market_fee_bps` | 25 (0.25%) |
//! | `wheeling_rate_per_kwh` | `TariffConfig.wheeling_rate_per_kwh` | 100000 = THB0.10/kWh |
//! | `loss_bps` | `TariffConfig.loss_bps` | 5 (0.05%) |
//!
//! Do **not** substitute `Config::transaction_fee_bps` — it defaults to 50 and the
//! deployed market reads 25, so it would book double what was charged.

use rust_decimal::Decimal;

/// Basis-point denominator.
const BPS: i64 = 10_000;
/// Currency mint decimals (THBC is 6-dec), used to scale `wheeling_rate_per_kwh`
/// from base units to a whole-currency amount.
const CURRENCY_SCALE: i64 = 1_000_000;

/// The on-chain settlement charge rates. Implemented in the infra layer by
/// reading the market and tariff accounts; injected like `TopologySnapshot` so
/// the pure clearing logic stays free of I/O.
pub trait ChargeRates: Send + Sync + std::fmt::Debug {
    /// `Market.market_fee_bps` — platform fee, in basis points of trade value.
    fn fee_bps(&self) -> u16;
    /// `TariffConfig.wheeling_rate_per_kwh` — currency **base units** (6-dec) per
    /// whole kWh. A flat per-energy charge, not a percentage.
    fn wheeling_rate_per_kwh(&self) -> u64;
    /// `TariffConfig.loss_bps` — transmission loss, in basis points of trade value.
    fn loss_bps(&self) -> u16;
}

/// Rates captured once, at startup, from the on-chain accounts.
///
/// They are governance-controlled and change rarely (`set_params` on the market,
/// `initialize_tariff_config` / the wheeling and loss authorities on the tariff),
/// so re-reading two accounts on every match would be pure overhead. The
/// trade-off is explicit: **a rate changed on-chain is not picked up until the
/// service restarts**, and until then settlements are booked at the old rate while
/// the chain charges the new one. If rates ever become dynamic, replace this with
/// a refreshing implementation behind the same trait rather than reading inline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StaticChargeRates {
    pub fee_bps: u16,
    pub wheeling_rate_per_kwh: u64,
    pub loss_bps: u16,
}

impl StaticChargeRates {
    /// All-zero rates. Used only when the on-chain values cannot be read at boot:
    /// booking zero is visibly wrong and pairs with a loud error, whereas guessing
    /// a rate would be quietly wrong — the exact failure this module exists to end.
    pub const ZERO: Self = Self {
        fee_bps: 0,
        wheeling_rate_per_kwh: 0,
        loss_bps: 0,
    };
}

impl ChargeRates for StaticChargeRates {
    fn fee_bps(&self) -> u16 {
        self.fee_bps
    }
    fn wheeling_rate_per_kwh(&self) -> u64 {
        self.wheeling_rate_per_kwh
    }
    fn loss_bps(&self) -> u16 {
        self.loss_bps
    }
}

/// The charge breakdown for one settled trade. `net` is what the seller actually
/// receives, and is the value that must reconcile with their on-chain credit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementCharges {
    pub fee: Decimal,
    pub wheeling: Decimal,
    pub loss: Decimal,
    pub net: Decimal,
}

/// Split a trade's gross value into the charges the chain applies.
///
/// `total` is the gross trade value (energy x settle price) and `kwh` the energy
/// traded — wheeling is charged per kWh, the other two per unit of value.
///
/// `net` can go negative on a trade small enough that the flat wheeling charge
/// exceeds its value (at THB0.10/kWh, anything under ~THB0.10/kWh of price). That
/// is a real economic outcome, not an error, so it is reported rather than clamped
/// — clamping would silently misreport the ledger, which is the very bug this
/// module exists to fix.
pub fn compute_charges(
    total: Decimal,
    kwh: Decimal,
    rates: &dyn ChargeRates,
) -> SettlementCharges {
    let fee = total * Decimal::from(i64::from(rates.fee_bps())) / Decimal::from(BPS);
    let wheeling =
        kwh * Decimal::from(rates.wheeling_rate_per_kwh()) / Decimal::from(CURRENCY_SCALE);
    let loss = total * Decimal::from(i64::from(rates.loss_bps())) / Decimal::from(BPS);
    let net = total - fee - wheeling - loss;
    SettlementCharges {
        fee,
        wheeling,
        loss,
        net,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    /// The rates deployed on the dev validator.
    #[derive(Debug)]
    struct LiveRates;
    impl ChargeRates for LiveRates {
        fn fee_bps(&self) -> u16 {
            25
        }
        fn wheeling_rate_per_kwh(&self) -> u64 {
            100_000
        }
        fn loss_bps(&self) -> u16 {
            5
        }
    }

    /// The measurement this module was built from: a 1 kWh @ THB1.00 trade in
    /// which the seller was credited exactly 0.897 on-chain.
    #[test]
    fn reproduces_the_measured_settlement() {
        let c = compute_charges(dec!(1.00), dec!(1.0), &LiveRates);
        assert_eq!(c.fee, dec!(0.0025));
        assert_eq!(c.wheeling, dec!(0.100000));
        assert_eq!(c.loss, dec!(0.000500));
        assert_eq!(c.net, dec!(0.897), "must equal the seller's on-chain credit");
    }

    /// The second measured trade: 3 kWh @ THB4.00, seller credited 11.66.
    #[test]
    fn reproduces_the_three_kwh_trade() {
        let c = compute_charges(dec!(12.00), dec!(3.0), &LiveRates);
        assert_eq!(c.fee, dec!(0.03)); // 0.25% of 12
        assert_eq!(c.wheeling, dec!(0.300000)); // 3 kWh x 0.10
        assert_eq!(c.loss, dec!(0.006)); // 0.05% of 12
        assert_eq!(c.net, dec!(11.664));
    }

    #[test]
    fn charges_always_sum_back_to_the_gross() {
        let c = compute_charges(dec!(17.50), dec!(5.0), &LiveRates);
        assert_eq!(c.fee + c.wheeling + c.loss + c.net, dec!(17.50));
    }

    /// Wheeling is per-kWh and flat, so a cheap trade can owe more than it is
    /// worth. Report it; do not clamp.
    #[test]
    fn net_can_go_negative_when_flat_wheeling_exceeds_value() {
        let c = compute_charges(dec!(0.01), dec!(1.0), &LiveRates);
        assert!(c.net.is_sign_negative(), "0.01 value vs 0.10 wheeling");
    }

    #[test]
    fn zero_rates_leave_the_seller_whole() {
        #[derive(Debug)]
        struct Free;
        impl ChargeRates for Free {
            fn fee_bps(&self) -> u16 {
                0
            }
            fn wheeling_rate_per_kwh(&self) -> u64 {
                0
            }
            fn loss_bps(&self) -> u16 {
                0
            }
        }
        let c = compute_charges(dec!(10), dec!(2), &Free);
        assert_eq!(c.net, dec!(10));
        assert_eq!(c.fee + c.wheeling + c.loss, Decimal::ZERO);
    }
}
