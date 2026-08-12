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

use rust_decimal::{Decimal, RoundingStrategy};

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

/// The flat wheeling charge as a whole-currency amount **per kWh**.
///
/// `TariffConfig.wheeling_rate_per_kwh` is 6-decimal currency base units per kWh
/// and carries **no zone dimension** — `settle_offchain_match` applies it to every
/// match, intra-zone included (`settle_offchain.rs`, the `wheeling_charge_val`
/// computation has no zone condition). Anything that prices wheeling — the
/// settlement ledger *and* the matcher's landed cost — must go through here, or
/// the two disagree and the difference silently lands on the seller.
pub fn wheeling_per_kwh(rates: &dyn ChargeRates) -> Decimal {
    Decimal::from(rates.wheeling_rate_per_kwh()) / Decimal::from(CURRENCY_SCALE)
}

/// The transmission loss expressed as the multiplicative factor the matcher wants,
/// `1 + loss_bps/10_000`.
///
/// `compute_charges` bills loss as `total * loss_bps / 10_000`; this is the same
/// rate in the form `TopologySnapshot::calculate_loss_factor` returns, so a
/// landed cost built from it recovers exactly the loss the chain will charge
/// (pinned by `loss_factor_recovers_the_billed_loss`).
pub fn loss_factor(rates: &dyn ChargeRates) -> Decimal {
    Decimal::ONE + Decimal::from(i64::from(rates.loss_bps())) / Decimal::from(BPS)
}

/// A [`ChargeRates`] whose values can be replaced while the service runs.
///
/// # Why the boot-time snapshot stopped being enough
///
/// [`StaticChargeRates`] is read once at startup, and its doc below explains the
/// trade-off that was acceptable when these rates only decided what the
/// `settlements` ledger *recorded*. They now decide four things: the landed cost
/// the matcher crosses on, the ledger, the minimum settleable ask the submit edges
/// refuse below, and the quote shown to customers. A governance rate change
/// therefore desynchronises all four from the chain until someone restarts the
/// service — and the `ZERO` fallback taken when the boot read fails leaves the
/// price floor disabled for just as long.
///
/// This type closes both: a refresher (`ChargeRatesWorker`) re-reads the accounts
/// on a cadence and [`store`](Self::store)s the result, so a rate change converges
/// and a failed boot read heals on the next successful poll instead of persisting
/// until restart.
///
/// A failed *refresh* deliberately keeps the previous values rather than reverting
/// to `ZERO`: stale-but-real rates are closer to the truth than zeros, and zeroing
/// would silently switch the sell-price floor off on an RPC blip.
#[derive(Debug)]
pub struct RefreshingChargeRates {
    current: std::sync::RwLock<StaticChargeRates>,
}

impl RefreshingChargeRates {
    /// Start from `initial` — normally the boot read, or [`StaticChargeRates::ZERO`]
    /// when that read failed.
    #[must_use]
    pub fn new(initial: StaticChargeRates) -> Self {
        Self {
            current: std::sync::RwLock::new(initial),
        }
    }

    /// Replace the live rates. Returns the previous value so a caller can log the
    /// transition — a rate change is a governance event worth seeing in the log.
    pub fn store(&self, next: StaticChargeRates) -> StaticChargeRates {
        let mut guard = self.current.write().unwrap_or_else(|e| e.into_inner());
        std::mem::replace(&mut *guard, next)
    }

    /// The rates currently in force.
    #[must_use]
    pub fn snapshot(&self) -> StaticChargeRates {
        *self.current.read().unwrap_or_else(|e| e.into_inner())
    }
}

// Lock poisoning is recovered from rather than propagated: this guards a plain
// value with no invariant a panicking writer could have broken, and these getters
// are infallible by trait signature. Refusing to serve a rate would take the
// matcher down over a poisoned mutex.
impl ChargeRates for RefreshingChargeRates {
    fn fee_bps(&self) -> u16 {
        self.snapshot().fee_bps
    }
    fn wheeling_rate_per_kwh(&self) -> u64 {
        self.snapshot().wheeling_rate_per_kwh
    }
    fn loss_bps(&self) -> u16 {
        self.snapshot().loss_bps
    }
}

/// Mirror of the trading program's `MAX_NETWORK_CHARGE_BPS`
/// (`programs/trading/src/state/tariff_config.rs`): the hard ceiling, in basis
/// points of a trade's value, on combined wheeling + loss. `net_seller_after_charges`
/// rejects a settlement that breaches it with `ChargesExceedCap`.
///
/// **This is a compile-time program constant, not account data**, so — unlike the
/// three rates in [`ChargeRates`] — it cannot be read from chain and has to be
/// mirrored. Change it here and in the program together; a stale copy here makes
/// [`min_settleable_price_per_kwh`] admit orders the chain then refuses forever,
/// which is the failure this whole module exists to prevent.
pub const MAX_NETWORK_CHARGE_BPS: i64 = 2_000;

/// The lowest ask at which a trade can settle **at all**, or `None` when the live
/// tariff admits no price whatsoever.
///
/// # Why a floor exists
///
/// Wheeling is a *flat* per-kWh charge while the cap that bounds it is a
/// *fraction of trade value*, so the cheaper the energy, the larger a share the
/// same charge consumes. Below some price the two cross and the chain rejects
/// every settlement with `ChargesExceedCap`:
///
/// ```text
///   wheeling + loss                  <= value * MAX_NETWORK_CHARGE_BPS / 10_000
///   q*w + q*p*loss_bps/10_000        <= q*p*cap_bps/10_000
///   p                                >= w * 10_000 / (cap_bps - loss_bps)
/// ```
///
/// Note `q` cancels: this is a pure price floor, independent of trade size. At the
/// deployed rates (0.10 THB/kWh wheeling, 5 bps loss) it lands at ~0.5013 THB/kWh
/// — **above** the 0.50 default of `MarketConfig::min_price_per_kwh`, so the
/// market's own configured minimum used to admit asks that could never settle.
///
/// The result is rounded UP to currency precision so the off-chain gate is
/// strictly more conservative than the on-chain check, whose integer truncation
/// is slightly more forgiving. Erring the other way would re-open the hole.
///
/// Returns `None` when `loss_bps` alone meets or exceeds the cap, leaving no
/// headroom for any wheeling charge at any price — a misconfigured tariff under
/// which the chain refuses every settlement, which the caller must surface rather
/// than paper over with a floor of zero.
///
/// A zero wheeling rate yields a floor of zero: with no flat component there is
/// nothing for the proportional cap to outrun.
pub fn min_settleable_price_per_kwh(rates: &dyn ChargeRates) -> Option<Decimal> {
    let wheeling = wheeling_per_kwh(rates);
    if wheeling.is_zero() {
        return Some(Decimal::ZERO);
    }
    let headroom_bps = Decimal::from(MAX_NETWORK_CHARGE_BPS - i64::from(rates.loss_bps()));
    if headroom_bps <= Decimal::ZERO {
        return None;
    }
    Some(
        (wheeling * Decimal::from(BPS) / headroom_bps)
            .round_dp_with_strategy(6, RoundingStrategy::ToPositiveInfinity),
    )
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
pub fn compute_charges(total: Decimal, kwh: Decimal, rates: &dyn ChargeRates) -> SettlementCharges {
    let fee = total * Decimal::from(i64::from(rates.fee_bps())) / Decimal::from(BPS);
    let wheeling = kwh * wheeling_per_kwh(rates);
    let loss = total * Decimal::from(i64::from(rates.loss_bps())) / Decimal::from(BPS);
    let net = total - fee - wheeling - loss;
    SettlementCharges {
        fee,
        wheeling,
        loss,
        net,
    }
}

/// A way a settlement row disagrees with what the chain would have done.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LedgerDiscrepancy {
    /// `fee + wheeling + loss + net` does not equal the gross. **Always a bug** —
    /// it is arithmetic on one row, independent of any tariff, so no rate change
    /// or timing can explain it. Every satang leaving the buyer's escrow is either
    /// a charge or the seller's payout; a row that does not add up describes a
    /// settlement that cannot have happened as recorded.
    DoesNotSumToGross { accounted: Decimal, gross: Decimal },
    /// A charge column differs from what the live tariff produces for this row.
    ///
    /// Unlike the above this is **not automatically a bug**: a row booked before a
    /// governance rate change legitimately differs from the current schedule, and
    /// nothing on the row records which schedule it was booked under. Read it as
    /// drift to explain, not a defect to fix — a rate change shows up as every row
    /// after some instant differing by the same amount, whereas a code fault shows
    /// up as rows differing arbitrarily or all rows differing since forever.
    ChargeOffTariff {
        field: &'static str,
        booked: Decimal,
        expected: Decimal,
    },
}

/// Check one settlement row against the accounting identity and the live tariff.
///
/// # Why this exists
///
/// [`compute_charges`] states that `net` "must reconcile with their on-chain
/// credit", and that claim was established by **measuring two trades by hand**.
/// Nothing has ever checked it since: no worker compares the `settlements` ledger
/// to the schedule the chain bills at, which is how a row could claim
/// `net = gross, fee = 0, wheeling = 0` while the seller was actually credited
/// 0.897 of every 1.00 — undetected until someone looked.
///
/// Deliberately arithmetic-only. A per-settlement check against the chain would
/// need the settle transaction, and this validator prunes history fast enough
/// (`--limit-ledger-size`) that old signatures return "not found" — so a
/// history-based reconciler would report false alarms that grow with age. The
/// identity below needs no chain read and cannot produce one.
pub fn reconcile_charges(
    gross: Decimal,
    kwh: Decimal,
    fee: Decimal,
    wheeling: Decimal,
    loss: Decimal,
    net: Decimal,
    rates: &dyn ChargeRates,
) -> Vec<LedgerDiscrepancy> {
    let mut found = Vec::new();

    let accounted = fee + wheeling + loss + net;
    if accounted != gross {
        found.push(LedgerDiscrepancy::DoesNotSumToGross { accounted, gross });
    }

    let expected = compute_charges(gross, kwh, rates);
    for (field, booked, want) in [
        ("fee_amount", fee, expected.fee),
        ("wheeling_charge", wheeling, expected.wheeling),
        ("loss_cost", loss, expected.loss),
        ("net_amount", net, expected.net),
    ] {
        if booked != want {
            found.push(LedgerDiscrepancy::ChargeOffTariff {
                field,
                booked,
                expected: want,
            });
        }
    }

    found
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
        assert_eq!(
            c.net,
            dec!(0.897),
            "must equal the seller's on-chain credit"
        );
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

    /// The matcher prices its landed cost from `wheeling_per_kwh`; settlement bills
    /// `compute_charges`. If these two ever diverge the buyer is filtered on one
    /// number and the seller is charged another — the exact defect that let a
    /// zone-conditional 0/0.02 filter coexist with a flat 0.10/kWh charge.
    #[test]
    fn wheeling_per_kwh_recovers_the_billed_wheeling() {
        assert_eq!(wheeling_per_kwh(&LiveRates), dec!(0.100000));
        let c = compute_charges(dec!(12.00), dec!(3.0), &LiveRates);
        assert_eq!(c.wheeling, dec!(3.0) * wheeling_per_kwh(&LiveRates));
    }

    /// Same contract for loss, in the multiplicative form the topology returns:
    /// the excess over 1.0, applied to the gross, must be what settlement bills.
    #[test]
    fn loss_factor_recovers_the_billed_loss() {
        assert_eq!(loss_factor(&LiveRates), dec!(1.0005));
        let gross = dec!(12.00);
        let c = compute_charges(gross, dec!(3.0), &LiveRates);
        assert_eq!(c.loss, gross * (loss_factor(&LiveRates) - Decimal::ONE));
    }

    // === settleability floor ===

    /// Reproduce the on-chain `net_seller_after_charges` cap check for a trade, so the
    /// floor is verified against the rule it mirrors rather than against itself.
    fn chain_accepts(price: Decimal, kwh: Decimal, rates: &dyn ChargeRates) -> bool {
        let c = compute_charges(price * kwh, kwh, rates);
        let cap = (price * kwh) * Decimal::from(MAX_NETWORK_CHARGE_BPS) / Decimal::from(BPS);
        c.wheeling + c.loss <= cap
    }

    #[test]
    fn floor_is_where_the_flat_wheeling_meets_the_proportional_cap() {
        let floor = min_settleable_price_per_kwh(&LiveRates).expect("live rates admit a price");
        assert_eq!(
            floor,
            dec!(0.501254),
            "0.10 * 10000 / (2000 - 5), rounded up"
        );
    }

    /// The headline consequence: the market's own configured minimum is BELOW the
    /// floor, so a compliant 0.50 ask was accepted and could never settle.
    #[test]
    fn the_default_market_minimum_is_itself_unsettleable() {
        let floor = min_settleable_price_per_kwh(&LiveRates).expect("live rates admit a price");
        let default_market_min = dec!(0.50);
        assert!(default_market_min < floor);
        assert!(!chain_accepts(default_market_min, dec!(1.0), &LiveRates));
    }

    #[test]
    fn at_and_above_the_floor_the_chain_accepts() {
        let floor = min_settleable_price_per_kwh(&LiveRates).expect("live rates admit a price");
        for kwh in [dec!(0.5), dec!(1.0), dec!(37.25), dec!(1000)] {
            assert!(
                chain_accepts(floor, kwh, &LiveRates),
                "floor must settle at {kwh} kWh"
            );
            assert!(chain_accepts(floor * dec!(2), kwh, &LiveRates));
        }
    }

    /// The floor must be conservative: everything the gate admits must settle. A
    /// floor rounded DOWN would admit a price the chain refuses.
    #[test]
    fn just_below_the_floor_the_chain_refuses() {
        let floor = min_settleable_price_per_kwh(&LiveRates).expect("live rates admit a price");
        assert!(!chain_accepts(floor - dec!(0.001), dec!(1.0), &LiveRates));
    }

    /// No flat component → nothing for the proportional cap to outrun.
    #[test]
    fn zero_wheeling_admits_any_price() {
        #[derive(Debug)]
        struct NoWheeling;
        impl ChargeRates for NoWheeling {
            fn fee_bps(&self) -> u16 {
                25
            }
            fn wheeling_rate_per_kwh(&self) -> u64 {
                0
            }
            fn loss_bps(&self) -> u16 {
                5
            }
        }
        assert_eq!(
            min_settleable_price_per_kwh(&NoWheeling),
            Some(Decimal::ZERO)
        );
    }

    /// Loss alone eating the whole cap leaves no headroom at any price. Reported as
    /// `None`, not as a floor of zero — the chain refuses every settlement and the
    /// caller has to say so.
    #[test]
    fn a_tariff_with_no_headroom_admits_no_price() {
        #[derive(Debug)]
        struct AllLoss;
        impl ChargeRates for AllLoss {
            fn fee_bps(&self) -> u16 {
                25
            }
            fn wheeling_rate_per_kwh(&self) -> u64 {
                100_000
            }
            fn loss_bps(&self) -> u16 {
                2_000 // == MAX_NETWORK_CHARGE_BPS
            }
        }
        assert_eq!(min_settleable_price_per_kwh(&AllLoss), None);
    }

    /// The ZERO fallback (on-chain rates unreadable at boot) must not manufacture a
    /// floor out of rates it does not know — it fails open, like every other
    /// admission gate here.
    #[test]
    fn unknown_rates_impose_no_floor() {
        assert_eq!(
            min_settleable_price_per_kwh(&StaticChargeRates::ZERO),
            Some(Decimal::ZERO)
        );
    }

    // === ledger reconciliation ===

    /// A row produced by `compute_charges` itself must reconcile cleanly — if this
    /// ever fails, the checker and the biller disagree and every other case here is
    /// meaningless.
    #[test]
    fn a_correctly_booked_row_has_no_discrepancies() {
        let gross = dec!(12.00);
        let kwh = dec!(3.0);
        let c = compute_charges(gross, kwh, &LiveRates);
        assert_eq!(
            reconcile_charges(gross, kwh, c.fee, c.wheeling, c.loss, c.net, &LiveRates),
            vec![]
        );
    }

    /// The measured production bug, replayed: the ledger claimed the seller kept
    /// the whole gross while the chain deducted 0.103 of it. Both checks must fire
    /// — the row neither adds up nor matches the tariff.
    #[test]
    fn the_measured_bug_is_caught() {
        let found = reconcile_charges(
            dec!(1.00),
            dec!(1.0),
            Decimal::ZERO, // fee booked as 0
            Decimal::ZERO, // wheeling booked as 0
            dec!(0.01),    // loss booked 20x too high
            dec!(1.00),    // net booked as the gross
            &LiveRates,
        );
        assert!(found
            .iter()
            .any(|d| matches!(d, LedgerDiscrepancy::DoesNotSumToGross { .. })));
        assert!(found.iter().any(|d| matches!(
            d,
            LedgerDiscrepancy::ChargeOffTariff {
                field: "wheeling_charge",
                ..
            }
        )));
    }

    /// The sum identity is tariff-independent: a row can match every rate and still
    /// not add up (a bad `net`), and that is unambiguously a bug.
    #[test]
    fn a_row_that_does_not_add_up_is_caught_even_with_right_rates() {
        let gross = dec!(12.00);
        let kwh = dec!(3.0);
        let c = compute_charges(gross, kwh, &LiveRates);
        let found = reconcile_charges(
            gross,
            kwh,
            c.fee,
            c.wheeling,
            c.loss,
            c.net + dec!(0.01), // seller over-credited by a satang
            &LiveRates,
        );
        assert!(found
            .iter()
            .any(|d| matches!(d, LedgerDiscrepancy::DoesNotSumToGross { .. })));
    }

    /// A row booked under an older schedule still SUMS correctly — it is internally
    /// consistent, just priced differently. Reporting only the tariff drift (and not
    /// a phantom arithmetic fault) is what keeps a governance rate change from
    /// looking like corruption.
    #[test]
    fn a_row_from_an_older_tariff_drifts_without_breaking_the_identity() {
        #[derive(Debug)]
        struct OldRates;
        impl ChargeRates for OldRates {
            fn fee_bps(&self) -> u16 {
                25
            }
            fn wheeling_rate_per_kwh(&self) -> u64 {
                50_000 // half the current rate
            }
            fn loss_bps(&self) -> u16 {
                5
            }
        }
        let gross = dec!(12.00);
        let kwh = dec!(3.0);
        let old = compute_charges(gross, kwh, &OldRates);

        let found = reconcile_charges(
            gross,
            kwh,
            old.fee,
            old.wheeling,
            old.loss,
            old.net,
            &LiveRates,
        );

        assert!(
            !found
                .iter()
                .any(|d| matches!(d, LedgerDiscrepancy::DoesNotSumToGross { .. })),
            "an old-tariff row is internally consistent — do not cry corruption"
        );
        assert!(found.iter().any(|d| matches!(
            d,
            LedgerDiscrepancy::ChargeOffTariff {
                field: "wheeling_charge",
                ..
            }
        )));
    }

    /// With rates unreadable the whole ledger would "drift" against zeros, burying
    /// any real fault in noise. The worker must skip the tariff comparison then —
    /// pinned here so the reason survives.
    #[test]
    fn zero_rates_make_every_real_row_look_wrong() {
        let gross = dec!(12.00);
        let kwh = dec!(3.0);
        let c = compute_charges(gross, kwh, &LiveRates);
        let found = reconcile_charges(
            gross,
            kwh,
            c.fee,
            c.wheeling,
            c.loss,
            c.net,
            &StaticChargeRates::ZERO,
        );
        assert!(
            found
                .iter()
                .any(|d| matches!(d, LedgerDiscrepancy::ChargeOffTariff { .. })),
            "justifies the worker's skip when rates are unknown"
        );
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

/// Ledger-side and chain-side totals for the three collector accounts, in whole
/// currency units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ChargeTotals {
    pub fee: Decimal,
    pub wheeling: Decimal,
    pub loss: Decimal,
}

impl ChargeTotals {
    /// Component-wise difference, `self - other`.
    #[must_use]
    pub fn minus(self, other: Self) -> Self {
        Self {
            fee: self.fee - other.fee,
            wheeling: self.wheeling - other.wheeling,
            loss: self.loss - other.loss,
        }
    }

    /// Each component paired with its name, for uniform reporting.
    #[must_use]
    pub fn labelled(self) -> [(&'static str, Decimal); 3] {
        [
            ("fee", self.fee),
            ("wheeling", self.wheeling),
            ("loss", self.loss),
        ]
    }
}
