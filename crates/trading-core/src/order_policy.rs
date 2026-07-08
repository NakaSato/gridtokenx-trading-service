//! Order price-admission policy — shared by the REST and gRPC submit edges.
//!
//! The two API edges parse their own raw input (REST: a `String`, gRPC: an
//! `f64`) and render their own error types, but the *policy* that turns an
//! (order_type, side, time_in_force, optional price) into a matching price is
//! identical. Keep it here, once, as pure sync logic so the edges can't drift.

use rust_decimal::Decimal;

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
        _ => match price_input {
            None => Err(OrderPriceError::LimitMissingPrice),
            Some(p) if p <= Decimal::ZERO => Err(OrderPriceError::LimitNonPositive),
            Some(p) => Ok(p),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

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
            let p =
                resolve_order_price(OrderType::Market, OrderSide::Buy, TimeInForce::Ioc, Some(bad));
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
        let fok_buy =
            resolve_order_price(OrderType::Limit, OrderSide::Buy, TimeInForce::Fok, Some(dec!(1.0)));
        assert_eq!(fok_buy, Ok(dec!(1.0)));
        // IOC SELL is fine (partial fill + cancel remainder is IOC's contract).
        let ioc_sell =
            resolve_order_price(OrderType::Limit, OrderSide::Sell, TimeInForce::Ioc, Some(dec!(0.5)));
        assert_eq!(ioc_sell, Ok(dec!(0.5)));
    }

    #[test]
    fn market_gtc_rejected() {
        let p = resolve_order_price(OrderType::Market, OrderSide::Buy, TimeInForce::Gtc, None);
        assert_eq!(p, Err(OrderPriceError::MarketGtc));
    }

    #[test]
    fn limit_requires_price() {
        let missing =
            resolve_order_price(OrderType::Limit, OrderSide::Buy, TimeInForce::Gtc, None);
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
}
