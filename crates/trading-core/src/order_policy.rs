//! Order price-admission policy — shared by the REST and gRPC submit edges.
//!
//! The two API edges parse their own raw input (REST: a `String`, gRPC: an
//! `f64`) and render their own error types, but the *policy* that turns an
//! (`order_type`, side, `time_in_force`, optional price) into a matching price is
//! identical. Keep it here, once, as pure sync logic so the edges can't drift.

use rust_decimal::Decimal;
use uuid::Uuid;

use crate::traits::MeterIdentity;
use crate::types::{OrderSide, OrderType, TimeInForce};

/// Ceiling bid for a market BUY with no slippage cap: above any realistic energy
/// ask, well below fixed-point overflow. A market buy will not cross an ask above
/// this. Used as the bid so the buyer fills at the resting ask, not this value.
pub const MARKET_BUY_CEILING_BID: Decimal = Decimal::from_parts(1_000_000, 0, 0, false, 0);

/// Why an order's price could not be resolved. Each API edge maps these to its
/// own transport error (HTTP 400 / Connect `InvalidArgument`) with a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderPriceError {
    /// Market SELL is unsupported — the matcher would clear it at its OWN ask,
    /// not the best bid, so it can't be priced correctly.
    MarketSellUnsupported,
    /// Fill-or-kill SELL is unsupported — the buy-driven matcher fills a resting
    /// sell incrementally across a cycle, so a FOK sell would partially fill and
    /// then have its remainder cancelled, violating all-or-nothing.
    FokSellUnsupported,
    /// A market order must be immediate (IOC/FOK); GTC would rest a max-price bid.
    MarketGtc,
    /// A market-buy slippage cap was supplied but is not positive.
    MarketCapNonPositive,
    /// A limit order requires an explicit price.
    LimitMissingPrice,
    /// A limit order's price was supplied but is not positive (a zero/negative
    /// bid or ask would mis-cross the CDA book).
    LimitNonPositive,
}

impl OrderPriceError {
    /// Client-facing message, shared verbatim by the REST and gRPC edges.
    #[must_use]
    pub fn message(self) -> &'static str {
        match self {
            Self::MarketSellUnsupported => {
                "market sell orders are not supported (the matcher prices at the resting ask); submit a limit sell"
            }
            Self::FokSellUnsupported => {
                "fill-or-kill sell orders are not supported (the matcher fills a resting sell incrementally); use gtc or ioc"
            }
            Self::MarketGtc => {
                "market orders are immediate — use ioc or fok (gtc would rest a max-price bid)"
            }
            Self::MarketCapNonPositive => {
                "price_per_kwh (market buy slippage cap) must be positive"
            }
            Self::LimitMissingPrice => "price_per_kwh is required for limit orders",
            Self::LimitNonPositive => "price_per_kwh must be positive",
        }
    }
}

/// Resolve the matching price for a submitted order.
///
/// `price_input` is the caller's parsed `price_per_kwh`, if any. Both edges map a
/// parsed value of exactly zero to `None` (absent) so the two transports agree;
/// a present value is therefore always non-zero here.
/// - **Limit**: it is the required order price (absent → [`OrderPriceError::LimitMissingPrice`];
///   negative → [`OrderPriceError::LimitNonPositive`]).
/// - **Market BUY**: it is an *optional* maximum acceptable price (slippage cap)
///   used as the bid; absent → [`MARKET_BUY_CEILING_BID`]. A negative cap is
///   rejected. Market SELL and market GTC are rejected outright.
///
/// # Errors
/// Returns [`OrderPriceError`] when the (type, side, tif, price) combination is
/// not admissible; see its variants.
pub fn resolve_order_price(
    order_type: OrderType,
    side: OrderSide,
    time_in_force: TimeInForce,
    price_input: Option<Decimal>,
) -> Result<Decimal, OrderPriceError> {
    // Fill-or-kill SELL is unsupported on either order type: the buy-driven CDA
    // matcher fills a resting sell incrementally across a cycle, so a FOK sell
    // would partially fill and then have its remainder swept for cancellation —
    // the opposite of all-or-nothing. The engine enforces FOK on the buy side
    // only, so reject FOK sells at entry rather than mis-executing them.
    if matches!(side, OrderSide::Sell) && time_in_force == TimeInForce::Fok {
        return Err(OrderPriceError::FokSellUnsupported);
    }
    match order_type {
        OrderType::Market => {
            if matches!(side, OrderSide::Sell) {
                return Err(OrderPriceError::MarketSellUnsupported);
            }
            if time_in_force == TimeInForce::Gtc {
                return Err(OrderPriceError::MarketGtc);
            }
            match price_input {
                Some(cap) if cap <= Decimal::ZERO => Err(OrderPriceError::MarketCapNonPositive),
                Some(cap) => Ok(cap),
                None => Ok(MARKET_BUY_CEILING_BID),
            }
        }
        OrderType::Limit => match price_input {
            None => Err(OrderPriceError::LimitMissingPrice),
            Some(p) if p <= Decimal::ZERO => Err(OrderPriceError::LimitNonPositive),
            Some(p) => Ok(p),
        },
    }
}

/// Why an order's expiry could not be resolved. Mapped by each edge to HTTP 400 /
/// Connect `InvalidArgument`, like [`OrderPriceError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderExpiryError {
    /// Both the absolute and relative forms were sent — they can disagree, so
    /// there is no safe way to pick one.
    BothForms,
    /// An expiry was sent alongside `signed_expires_at`. The stored expiry must be
    /// the signed one byte-for-byte (settlement re-derives the signed payload from
    /// it), so a second, possibly-different value is refused rather than silently
    /// dropped.
    SignedConflict,
    /// The expiry is not in the future. Such an order can never match — the engine
    /// skips expired orders — so it would rest as dead weight until reaped.
    NotInFuture,
    /// The expiry is further out than `max_ttl_secs` allows. Unbounded lifetimes
    /// let the active book grow without limit, and the matcher re-reads that whole
    /// book every cycle.
    BeyondMaxTtl,
}

impl OrderExpiryError {
    /// Client-facing message, shared verbatim by the REST and gRPC edges.
    #[must_use]
    pub fn message(self) -> &'static str {
        match self {
            Self::BothForms => "send either expires_at or expires_in_secs, not both",
            Self::SignedConflict => {
                "expires_at/expires_in_secs must not be sent with signed_expires_at — the signed expiry is authoritative"
            }
            Self::NotInFuture => "expiry must be in the future",
            Self::BeyondMaxTtl => "expiry exceeds the maximum order lifetime (ORDER_MAX_TTL_SECS)",
        }
    }
}

/// Resolve the `expires_at` to store for a submitted order.
///
/// Precedence: `signed_expires_at` (authoritative under per-user-escrow
/// settlement) → an explicit client expiry → `default_ttl_secs` from now.
///
/// The two client forms are equivalent but serve different callers: `expires_at`
/// pins an absolute instant (aligning to an epoch boundary, say), while
/// `expires_in_secs` is immune to client clock skew. Sending both is an error.
///
/// `max_ttl_secs` is applied to caller-supplied values only, never to
/// `default_ttl_secs` — a config where the default exceeds the max would
/// otherwise reject *every* order. (`Config::from_env` rejects that combination
/// up front; this is the second line of defence.)
///
/// IOC/FOK orders never rest, so their expiry is inert — accepted, not rejected,
/// so a client can send one uniform payload shape for every time-in-force.
///
/// # Errors
/// Returns [`OrderExpiryError`] when the requested expiry is contradictory, in
/// the past, or beyond the maximum lifetime.
pub fn resolve_expires_at(
    now: chrono::DateTime<chrono::Utc>,
    requested_at: Option<chrono::DateTime<chrono::Utc>>,
    requested_ttl_secs: Option<i64>,
    signed_expires_at: Option<i64>,
    default_ttl_secs: u64,
    max_ttl_secs: u64,
) -> Result<chrono::DateTime<chrono::Utc>, OrderExpiryError> {
    let client_supplied = requested_at.is_some() || requested_ttl_secs.is_some();

    if let Some(signed) = signed_expires_at {
        if client_supplied {
            return Err(OrderExpiryError::SignedConflict);
        }
        // A signed expiry is still checked: signing a past (or absurdly distant)
        // instant is a client bug that would otherwise rest an unmatchable order.
        let signed_at =
            chrono::DateTime::from_timestamp(signed, 0).ok_or(OrderExpiryError::NotInFuture)?;
        return check_window(now, signed_at, max_ttl_secs);
    }

    if requested_at.is_some() && requested_ttl_secs.is_some() {
        return Err(OrderExpiryError::BothForms);
    }

    if let Some(at) = requested_at {
        return check_window(now, at, max_ttl_secs);
    }

    if let Some(ttl) = requested_ttl_secs {
        // Reject non-positive TTLs here rather than letting `now + ttl` land in
        // the past — the message ("must be in the future") is the same, and this
        // avoids a duration overflow on an extreme negative value.
        if ttl <= 0 {
            return Err(OrderExpiryError::NotInFuture);
        }
        // `Duration::seconds` PANICS outside its representable range, so an
        // absurd `expires_in_secs` would take the handler down rather than 400.
        // `try_seconds` returns None there instead.
        let at = chrono::Duration::try_seconds(ttl)
            .and_then(|d| now.checked_add_signed(d))
            .ok_or(OrderExpiryError::BeyondMaxTtl)?;
        return check_window(now, at, max_ttl_secs);
    }

    // Nothing supplied: the configured default, unchecked against max_ttl_secs
    // for the reason given above.
    chrono::Duration::try_seconds(i64::try_from(default_ttl_secs).unwrap_or(i64::MAX))
        .and_then(|d| now.checked_add_signed(d))
        .ok_or(OrderExpiryError::BeyondMaxTtl)
}

/// Shared window check for every caller-supplied expiry.
fn check_window(
    now: chrono::DateTime<chrono::Utc>,
    at: chrono::DateTime<chrono::Utc>,
    max_ttl_secs: u64,
) -> Result<chrono::DateTime<chrono::Utc>, OrderExpiryError> {
    if at <= now {
        return Err(OrderExpiryError::NotInFuture);
    }
    let horizon = chrono::Duration::try_seconds(i64::try_from(max_ttl_secs).unwrap_or(i64::MAX))
        .and_then(|d| now.checked_add_signed(d))
        .ok_or(OrderExpiryError::BeyondMaxTtl)?;
    if at > horizon {
        return Err(OrderExpiryError::BeyondMaxTtl);
    }
    Ok(at)
}

// ── Sell-side meter-verification gate ────────────────────────────────────────

/// Why a sell order was refused on meter grounds. Each edge maps these to its
/// own transport error, like [`OrderPriceError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SellEligibilityError {
    /// The order named a meter the seller does not own. Deliberately not
    /// distinguished from "no such meter": a distinct error would turn order
    /// submission into an oracle for which serials exist and who holds them.
    MeterNotOwned,
    /// The seller owns the named meter, but metering has not verified possession
    /// of the device. This is the headline rule.
    MeterUnverified,
    /// The order named no meter and the seller owns no verified meter at all.
    /// Without this case, omitting `meter_serial` would bypass the entire gate.
    NoVerifiedMeter,
}

impl SellEligibilityError {
    /// Client-facing message, shared verbatim by the REST and gRPC edges.
    #[must_use]
    pub fn message(self) -> &'static str {
        match self {
            Self::MeterNotOwned => "the meter named on this order is not registered to you",
            Self::MeterUnverified => {
                "this meter is not verified — verify it (POST /api/v1/me/meters/{serial}/verify) before opening a sell order"
            }
            Self::NoVerifiedMeter => {
                "you have no verified meter — register a meter and verify it (POST /api/v1/me/meters/{serial}/verify) before selling energy"
            }
        }
    }
}

/// Whether the edge must look up "does this seller own any verified meter?".
///
/// Exists so the extra query is taken **only** on the path that needs it — a buy
/// order, or a sell that already names a meter, must not pay for it. Pairing it
/// with [`check_sell_eligibility`] keeps the decision of *when to ask* in the
/// same shared place as the decision of *what the answer means*, so the REST and
/// gRPC edges cannot drift on either half.
#[must_use]
pub fn needs_any_verified_meter_lookup(side: OrderSide, meter: Option<&MeterIdentity>) -> bool {
    matches!(side, OrderSide::Sell) && meter.is_none()
}

/// Decide whether `seller` may open this sell order.
///
/// Selling energy is a claim to have produced it, and the meter is the only
/// thing that substantiates the claim. Registration alone does not: it records
/// an unproven assertion that a serial belongs to you. So a sell is admitted
/// only when a meter the seller owns has been verified by metering.
///
/// Buy orders are unaffected — a consumer needs no meter to bid.
///
/// `seller_has_verified_meter` is only consulted when no meter is named; pass
/// `false` when [`needs_any_verified_meter_lookup`] says the lookup is not
/// needed, since it is then unused.
///
/// # Errors
/// Returns [`SellEligibilityError`] when the seller has not substantiated the
/// claim: the named meter is not theirs, is unverified, or they own no verified
/// meter at all.
pub fn check_sell_eligibility(
    seller: Uuid,
    side: OrderSide,
    meter: Option<&MeterIdentity>,
    seller_has_verified_meter: bool,
) -> Result<(), SellEligibilityError> {
    if !matches!(side, OrderSide::Sell) {
        return Ok(());
    }

    let Some(meter) = meter else {
        return if seller_has_verified_meter {
            Ok(())
        } else {
            Err(SellEligibilityError::NoVerifiedMeter)
        };
    };

    // Ownership first: an unverified meter belonging to someone else must report
    // "not yours", never "not verified" — the latter would confirm the serial is
    // registered and tell the caller what state it is in.
    if meter.user_id != seller {
        return Err(SellEligibilityError::MeterNotOwned);
    }
    if !meter.is_verified {
        return Err(SellEligibilityError::MeterUnverified);
    }
    Ok(())
}

// ── Buy-side funding gate ────────────────────────────────────────────────────

/// Atomic units per whole currency token. The currency leg is 6-decimal
/// (THBC-style) everywhere — the on-chain placement path scales prices by the
/// same factor — so the funding gate must agree or it checks the wrong number.
pub const CURRENCY_ATOMIC_PER_UNIT: u32 = 1_000_000;

/// What the buy-side funding gate requires of the buyer's currency balance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuyFundingRequirement {
    /// No lookup needed (sell orders — a seller spends energy, not currency).
    None,
    /// A market buy with no slippage cap: its spend is unbounded by
    /// construction, so the only meaningful submit-time check is that the buyer
    /// holds *any* currency at all.
    NonZero,
    /// The order's maximum spend in currency atomic units (6-dec):
    /// `matching_price × amount`, rounded up so truncation can never admit a
    /// buyer who is short by less than one atomic unit.
    Atomic(u64),
}

/// Why a buy order was refused on funding grounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuyFundingError {
    /// The buyer has no on-chain wallet to pay from.
    NoWallet,
    /// The buyer's currency balance cannot cover the order's maximum spend.
    InsufficientFunds {
        /// Maximum spend of this order, currency atomic units (6-dec).
        required_atomic: u64,
        /// The buyer's current balance, currency atomic units (6-dec).
        available_atomic: u64,
    },
}

impl BuyFundingError {
    /// Client-facing message, shared verbatim by the REST and gRPC edges.
    #[must_use]
    pub fn message(self) -> String {
        match self {
            Self::NoWallet => {
                "you have no on-chain wallet — link a wallet before buying energy".to_string()
            }
            Self::InsufficientFunds {
                required_atomic,
                available_atomic,
            } => {
                let scale = Decimal::from(CURRENCY_ATOMIC_PER_UNIT);
                format!(
                    "insufficient currency balance: this order can spend up to {} but the wallet holds {}",
                    Decimal::from(required_atomic) / scale,
                    Decimal::from(available_atomic) / scale,
                )
            }
        }
    }
}

/// Compute what the funding gate must verify for this order.
///
/// `explicit_price` is the price the CLIENT sent (limit price or market-buy
/// slippage cap), `matching_price` the output of [`resolve_order_price`]. The
/// two are distinguished because a market buy with no cap resolves to
/// [`MARKET_BUY_CEILING_BID`] — a sentinel, not a spend the buyer could ever
/// owe — so that case degrades to [`BuyFundingRequirement::NonZero`].
///
/// A cost too large for `u64` maps to `u64::MAX` (fail closed: no real wallet
/// holds it), and a non-positive amount contributes zero cost — amount
/// validation is the edges' job, not this gate's.
#[must_use]
pub fn required_buy_funding(
    side: OrderSide,
    order_type: OrderType,
    explicit_price: Option<Decimal>,
    matching_price: Decimal,
    amount: Decimal,
) -> BuyFundingRequirement {
    use rust_decimal::prelude::ToPrimitive;

    if !matches!(side, OrderSide::Buy) {
        return BuyFundingRequirement::None;
    }
    if matches!(order_type, OrderType::Market) && explicit_price.is_none() {
        return BuyFundingRequirement::NonZero;
    }
    // checked_mul, not `*`: Decimal multiplication PANICS on overflow, and this
    // runs on request input — an absurd price × amount must refuse, not crash.
    let cost_atomic = matching_price
        .checked_mul(amount)
        .and_then(|c| c.checked_mul(Decimal::from(CURRENCY_ATOMIC_PER_UNIT)))
        .map_or(u64::MAX, |c| c.ceil().to_u64().unwrap_or(u64::MAX));
    BuyFundingRequirement::Atomic(cost_atomic)
}

/// Decide whether the buyer's balance admits the order.
///
/// `available_atomic` is the buyer's currency balance in atomic units, read by
/// the edge only when the requirement is not [`BuyFundingRequirement::None`].
///
/// This is a submit-time admission check, not a reservation: nothing stops the
/// balance being spent between admission and settlement, where the atomic swap
/// remains the real enforcement. Its job is to keep knowably-unfundable bids
/// out of the book.
///
/// # Errors
/// Returns [`BuyFundingError::InsufficientFunds`] when the balance cannot cover
/// the requirement.
pub fn check_buy_funding(
    requirement: BuyFundingRequirement,
    available_atomic: u64,
) -> Result<(), BuyFundingError> {
    match requirement {
        BuyFundingRequirement::None => Ok(()),
        BuyFundingRequirement::NonZero => {
            if available_atomic > 0 {
                Ok(())
            } else {
                Err(BuyFundingError::InsufficientFunds {
                    required_atomic: 1,
                    available_atomic,
                })
            }
        }
        BuyFundingRequirement::Atomic(required_atomic) => {
            if available_atomic >= required_atomic {
                Ok(())
            } else {
                Err(BuyFundingError::InsufficientFunds {
                    required_atomic,
                    available_atomic,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    // ── Sell-side meter-verification gate ────────────────────────────────────

    fn meter(owner: Uuid, is_verified: bool) -> MeterIdentity {
        MeterIdentity {
            meter_id: Uuid::new_v4(),
            user_id: owner,
            is_verified,
        }
    }

    #[test]
    fn sell_with_own_verified_meter_is_admitted() {
        let seller = Uuid::new_v4();
        assert_eq!(
            check_sell_eligibility(seller, OrderSide::Sell, Some(&meter(seller, true)), false),
            Ok(())
        );
    }

    #[test]
    fn sell_with_own_unverified_meter_is_refused() {
        // The headline rule: registration is a claim, verification is the proof.
        let seller = Uuid::new_v4();
        assert_eq!(
            check_sell_eligibility(seller, OrderSide::Sell, Some(&meter(seller, false)), false),
            Err(SellEligibilityError::MeterUnverified)
        );
    }

    #[test]
    fn owning_another_verified_meter_does_not_excuse_the_named_one() {
        // `seller_has_verified_meter = true` must NOT rescue an order that names
        // a specific unverified meter — otherwise one verified meter would
        // launder every other meter the seller owns.
        let seller = Uuid::new_v4();
        assert_eq!(
            check_sell_eligibility(seller, OrderSide::Sell, Some(&meter(seller, false)), true),
            Err(SellEligibilityError::MeterUnverified)
        );
    }

    #[test]
    fn sell_naming_someone_elses_meter_is_refused_as_not_owned() {
        let seller = Uuid::new_v4();
        let stranger = Uuid::new_v4();
        // Verified or not, another user's meter reports the same error — the
        // response must not disclose the meter's verification state.
        for verified in [true, false] {
            assert_eq!(
                check_sell_eligibility(
                    seller,
                    OrderSide::Sell,
                    Some(&meter(stranger, verified)),
                    true
                ),
                Err(SellEligibilityError::MeterNotOwned)
            );
        }
    }

    #[test]
    fn sell_without_a_meter_requires_any_verified_meter() {
        // Closes the bypass: omitting meter_serial must not skip the gate.
        let seller = Uuid::new_v4();
        assert_eq!(
            check_sell_eligibility(seller, OrderSide::Sell, None, false),
            Err(SellEligibilityError::NoVerifiedMeter)
        );
        assert_eq!(
            check_sell_eligibility(seller, OrderSide::Sell, None, true),
            Ok(())
        );
    }

    #[test]
    fn buy_orders_are_never_gated() {
        // A consumer needs no meter to bid, with or without one named.
        let buyer = Uuid::new_v4();
        let stranger = Uuid::new_v4();
        assert_eq!(
            check_sell_eligibility(buyer, OrderSide::Buy, None, false),
            Ok(())
        );
        assert_eq!(
            check_sell_eligibility(buyer, OrderSide::Buy, Some(&meter(stranger, false)), false),
            Ok(())
        );
    }

    #[test]
    fn any_verified_lookup_is_taken_only_for_a_meterless_sell() {
        let owner = Uuid::new_v4();
        let m = meter(owner, true);
        assert!(needs_any_verified_meter_lookup(OrderSide::Sell, None));
        assert!(
            !needs_any_verified_meter_lookup(OrderSide::Sell, Some(&m)),
            "a named meter already answers the question — don't pay for the query"
        );
        assert!(!needs_any_verified_meter_lookup(OrderSide::Buy, None));
        assert!(!needs_any_verified_meter_lookup(OrderSide::Buy, Some(&m)));
    }

    // ── Buy-side funding gate ─────────────────────────────────────────────────

    #[test]
    fn sell_orders_need_no_funding_lookup() {
        assert_eq!(
            required_buy_funding(OrderSide::Sell, OrderType::Limit, None, dec!(5), dec!(10)),
            BuyFundingRequirement::None
        );
        assert_eq!(check_buy_funding(BuyFundingRequirement::None, 0), Ok(()));
    }

    #[test]
    fn limit_buy_requires_price_times_amount_in_atomic() {
        // 5.5 THB/kWh × 10 kWh = 55 currency = 55_000_000 atomic (6-dec).
        assert_eq!(
            required_buy_funding(
                OrderSide::Buy,
                OrderType::Limit,
                Some(dec!(5.5)),
                dec!(5.5),
                dec!(10)
            ),
            BuyFundingRequirement::Atomic(55_000_000)
        );
    }

    #[test]
    fn funding_cost_rounds_up_never_down() {
        // 0.0000001 × 1 kWh = 0.1 atomic — truncation would admit a zero-balance
        // buyer; ceiling demands 1 atomic unit.
        assert_eq!(
            required_buy_funding(
                OrderSide::Buy,
                OrderType::Limit,
                Some(dec!(0.0000001)),
                dec!(0.0000001),
                dec!(1)
            ),
            BuyFundingRequirement::Atomic(1)
        );
    }

    #[test]
    fn capped_market_buy_requires_cap_times_amount() {
        assert_eq!(
            required_buy_funding(
                OrderSide::Buy,
                OrderType::Market,
                Some(dec!(2)),
                dec!(2),
                dec!(3)
            ),
            BuyFundingRequirement::Atomic(6_000_000)
        );
    }

    #[test]
    fn uncapped_market_buy_requires_nonzero_balance_only() {
        // The resolved price is the ceiling sentinel, not a real spend — the
        // gate must not demand a million-currency balance for it.
        assert_eq!(
            required_buy_funding(
                OrderSide::Buy,
                OrderType::Market,
                None,
                MARKET_BUY_CEILING_BID,
                dec!(1)
            ),
            BuyFundingRequirement::NonZero
        );
        assert_eq!(check_buy_funding(BuyFundingRequirement::NonZero, 1), Ok(()));
        assert_eq!(
            check_buy_funding(BuyFundingRequirement::NonZero, 0),
            Err(BuyFundingError::InsufficientFunds {
                required_atomic: 1,
                available_atomic: 0
            })
        );
    }

    #[test]
    fn exact_balance_is_admitted_one_atomic_short_is_refused() {
        let req = BuyFundingRequirement::Atomic(55_000_000);
        assert_eq!(check_buy_funding(req, 55_000_000), Ok(()));
        assert_eq!(
            check_buy_funding(req, 54_999_999),
            Err(BuyFundingError::InsufficientFunds {
                required_atomic: 55_000_000,
                available_atomic: 54_999_999
            })
        );
    }

    #[test]
    fn overflowing_cost_fails_closed() {
        // A cost beyond u64 maps to u64::MAX — no wallet passes it.
        assert_eq!(
            required_buy_funding(
                OrderSide::Buy,
                OrderType::Limit,
                Some(Decimal::MAX),
                Decimal::MAX,
                dec!(1000000)
            ),
            BuyFundingRequirement::Atomic(u64::MAX)
        );
    }

    #[test]
    fn insufficient_funds_message_reports_whole_currency_units() {
        let msg = BuyFundingError::InsufficientFunds {
            required_atomic: 55_000_000,
            available_atomic: 1_500_000,
        }
        .message();
        assert!(msg.contains("55"), "required in whole units: {msg}");
        assert!(msg.contains("1.5"), "available in whole units: {msg}");
    }

    #[test]
    fn market_buy_no_cap_uses_ceiling() {
        let p = resolve_order_price(OrderType::Market, OrderSide::Buy, TimeInForce::Ioc, None);
        assert_eq!(p, Ok(MARKET_BUY_CEILING_BID));
        assert_eq!(MARKET_BUY_CEILING_BID, dec!(1000000));
    }

    #[test]
    fn market_buy_cap_used_as_bid() {
        let p = resolve_order_price(
            OrderType::Market,
            OrderSide::Buy,
            TimeInForce::Fok,
            Some(dec!(5.5)),
        );
        assert_eq!(p, Ok(dec!(5.5)));
    }

    #[test]
    fn market_buy_non_positive_cap_rejected() {
        for bad in [dec!(0), dec!(-1)] {
            let p = resolve_order_price(
                OrderType::Market,
                OrderSide::Buy,
                TimeInForce::Ioc,
                Some(bad),
            );
            assert_eq!(p, Err(OrderPriceError::MarketCapNonPositive));
        }
    }

    #[test]
    fn market_sell_rejected() {
        let p = resolve_order_price(OrderType::Market, OrderSide::Sell, TimeInForce::Ioc, None);
        assert_eq!(p, Err(OrderPriceError::MarketSellUnsupported));
    }

    #[test]
    fn fok_sell_rejected_limit_and_market() {
        // Limit FOK sell — rejected regardless of whether a price is supplied.
        for price in [None, Some(dec!(4.5))] {
            let p = resolve_order_price(OrderType::Limit, OrderSide::Sell, TimeInForce::Fok, price);
            assert_eq!(p, Err(OrderPriceError::FokSellUnsupported));
        }
        // Market FOK sell — the FOK-sell guard fires before the market-sell arm.
        let p = resolve_order_price(OrderType::Market, OrderSide::Sell, TimeInForce::Fok, None);
        assert_eq!(p, Err(OrderPriceError::FokSellUnsupported));
    }

    #[test]
    fn fok_buy_and_ioc_sell_still_ok() {
        // FOK BUY is fine (engine enforces buy-side all-or-nothing).
        let fok_buy = resolve_order_price(
            OrderType::Limit,
            OrderSide::Buy,
            TimeInForce::Fok,
            Some(dec!(1.0)),
        );
        assert_eq!(fok_buy, Ok(dec!(1.0)));
        // IOC SELL is fine (partial fill + cancel remainder is IOC's contract).
        let ioc_sell = resolve_order_price(
            OrderType::Limit,
            OrderSide::Sell,
            TimeInForce::Ioc,
            Some(dec!(0.5)),
        );
        assert_eq!(ioc_sell, Ok(dec!(0.5)));
    }

    #[test]
    fn market_gtc_rejected() {
        let p = resolve_order_price(OrderType::Market, OrderSide::Buy, TimeInForce::Gtc, None);
        assert_eq!(p, Err(OrderPriceError::MarketGtc));
    }

    #[test]
    fn limit_requires_price() {
        let missing = resolve_order_price(OrderType::Limit, OrderSide::Buy, TimeInForce::Gtc, None);
        assert_eq!(missing, Err(OrderPriceError::LimitMissingPrice));
        let ok = resolve_order_price(
            OrderType::Limit,
            OrderSide::Buy,
            TimeInForce::Gtc,
            Some(dec!(4.5)),
        );
        assert_eq!(ok, Ok(dec!(4.5)));
    }

    #[test]
    fn limit_non_positive_price_rejected() {
        // Negative reaches the policy as Some(neg) (edges map only exact 0 -> None);
        // a zero/negative limit price would mis-cross the CDA book.
        for (side, bad) in [
            (OrderSide::Buy, dec!(-5)),
            (OrderSide::Sell, dec!(-0.01)),
            (OrderSide::Buy, dec!(0)),
        ] {
            let p = resolve_order_price(OrderType::Limit, side, TimeInForce::Gtc, Some(bad));
            assert_eq!(p, Err(OrderPriceError::LimitNonPositive));
        }
    }

    // ── resolve_expires_at ───────────────────────────────────────────────────

    /// Fixed "now" so every expiry case is deterministic.
    fn now() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp(1_800_000_000, 0).expect("valid timestamp")
    }

    /// Mirrors `OrderExpiryConfig::default()` so the fixture doesn't imply a TTL
    /// the service no longer stamps. The assertions derive from the const, so the
    /// resolver is still proven to use the value passed in, not a hardcoded one.
    const DEFAULT_TTL: u64 = 15 * 60;
    const MAX_TTL: u64 = 7 * 24 * 60 * 60;

    fn resolve(
        at: Option<chrono::DateTime<chrono::Utc>>,
        ttl: Option<i64>,
        signed: Option<i64>,
    ) -> Result<chrono::DateTime<chrono::Utc>, OrderExpiryError> {
        resolve_expires_at(now(), at, ttl, signed, DEFAULT_TTL, MAX_TTL)
    }

    /// Omitting an expiry falls back to the configured default TTL — the same
    /// value at both edges, whatever `ORDER_DEFAULT_TTL_SECS` is set to.
    #[test]
    fn absent_expiry_falls_back_to_the_configured_default() {
        let got = resolve(None, None, None).expect("default is admissible");
        assert_eq!(got, now() + chrono::Duration::seconds(DEFAULT_TTL as i64));
    }

    /// Both client forms name the same instant when they agree.
    #[test]
    fn absolute_and_relative_forms_agree() {
        let ttl = 3_600;
        let absolute = now() + chrono::Duration::seconds(ttl);
        assert_eq!(resolve(Some(absolute), None, None), Ok(absolute));
        assert_eq!(resolve(None, Some(ttl), None), Ok(absolute));
    }

    /// Sending both is refused rather than silently picking one — they can
    /// disagree, and guessing would store an expiry the client did not ask for.
    #[test]
    fn sending_both_forms_is_rejected() {
        let at = now() + chrono::Duration::seconds(60);
        assert_eq!(
            resolve(Some(at), Some(60), None),
            Err(OrderExpiryError::BothForms)
        );
    }

    /// A past or now expiry can never match (the engine skips expired orders), so
    /// it is a 400 instead of an order that rests as dead weight until reaped.
    #[test]
    fn past_or_present_expiry_is_rejected() {
        assert_eq!(
            resolve(Some(now()), None, None),
            Err(OrderExpiryError::NotInFuture)
        );
        assert_eq!(
            resolve(Some(now() - chrono::Duration::seconds(1)), None, None),
            Err(OrderExpiryError::NotInFuture)
        );
        for ttl in [0, -1, -86_400, i64::MIN] {
            assert_eq!(
                resolve(None, Some(ttl), None),
                Err(OrderExpiryError::NotInFuture),
                "ttl {ttl} must be rejected without overflowing"
            );
        }
    }

    /// The horizon is inclusive: exactly max_ttl is fine, one second past is not.
    /// Unbounded lifetimes would grow the active book the matcher re-reads every
    /// cycle.
    #[test]
    fn client_expiry_is_capped_at_max_ttl() {
        let at_limit = now() + chrono::Duration::seconds(MAX_TTL as i64);
        assert_eq!(resolve(Some(at_limit), None, None), Ok(at_limit));
        assert_eq!(
            resolve(Some(at_limit + chrono::Duration::seconds(1)), None, None),
            Err(OrderExpiryError::BeyondMaxTtl)
        );
        assert_eq!(resolve(None, Some(MAX_TTL as i64), None), Ok(at_limit));
        assert_eq!(
            resolve(None, Some(MAX_TTL as i64 + 1), None),
            Err(OrderExpiryError::BeyondMaxTtl)
        );
        // An extreme TTL must report the cap, not panic on duration overflow.
        assert_eq!(
            resolve(None, Some(i64::MAX), None),
            Err(OrderExpiryError::BeyondMaxTtl)
        );
    }

    /// A signed expiry is authoritative: settlement re-derives the signed payload
    /// from the stored value, so it must be kept byte-for-byte.
    #[test]
    fn signed_expiry_wins_and_is_still_validated() {
        let signed = now() + chrono::Duration::seconds(600);
        assert_eq!(resolve(None, None, Some(signed.timestamp())), Ok(signed));

        // Signing a past instant is a client bug, not a licence to rest an
        // unmatchable order.
        assert_eq!(
            resolve(
                None,
                None,
                Some((now() - chrono::Duration::seconds(1)).timestamp())
            ),
            Err(OrderExpiryError::NotInFuture)
        );
        assert_eq!(
            resolve(
                None,
                None,
                Some((now() + chrono::Duration::days(30)).timestamp())
            ),
            Err(OrderExpiryError::BeyondMaxTtl)
        );
    }

    /// A client expiry alongside a signed one is refused: storing either would be
    /// wrong (the signed bytes must match) or surprising (silently dropped).
    #[test]
    fn client_expiry_with_a_signed_expiry_is_rejected() {
        let signed = (now() + chrono::Duration::seconds(600)).timestamp();
        let at = now() + chrono::Duration::seconds(60);
        assert_eq!(
            resolve(Some(at), None, Some(signed)),
            Err(OrderExpiryError::SignedConflict)
        );
        assert_eq!(
            resolve(None, Some(60), Some(signed)),
            Err(OrderExpiryError::SignedConflict)
        );
    }

    /// The default is exempt from the max check on purpose: a config with
    /// default > max would otherwise 400 every order that omits an expiry.
    /// `Config::from_env` rejects that combination, so this is defence in depth.
    #[test]
    fn default_is_not_capped_by_max_ttl() {
        let long_default = 10 * 24 * 60 * 60;
        let got = resolve_expires_at(now(), None, None, None, long_default, MAX_TTL)
            .expect("default must never be rejected");
        assert_eq!(got, now() + chrono::Duration::seconds(long_default as i64));
    }
}
