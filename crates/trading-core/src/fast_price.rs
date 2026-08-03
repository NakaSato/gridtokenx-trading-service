//! Fast fixed-point price representation for hot-path comparison.
//!
//! `rust_decimal::Decimal` comparison involves internal normalization overhead.
//! For sorted order book iteration where comparison dominates, this pre-normalized
//! i128 representation provides ~10x faster comparison.
//!
//! All prices are stored at 9 decimal places precision (matching Solana SPL token decimals).

use rust_decimal::Decimal;
use std::cmp::Ordering;

/// Pre-normalized price for fast comparison in matching loops.
/// Stores price as fixed-point i64 with 9 decimal places.
///
/// # Example
/// ```
/// use trading_core::fast_price::FastPrice;
/// use rust_decimal_macros::dec;
/// let price = FastPrice::from(dec!(1.234567890));
/// assert_eq!(price.to_decimal(), dec!(1.234567890));
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FastPrice(i64);

impl FastPrice {
    /// The number of decimal places (matches Solana SPL token decimals)
    pub const SCALE: u32 = 9;

    /// Scale factor for conversion
    pub const FACTOR: i64 = 1_000_000_000; // 10^9

    /// Zero value
    pub const ZERO: Self = Self(0);

    /// Create from raw mantissa value
    #[inline]
    #[must_use]
    pub const fn from_raw(mantissa: i64) -> Self {
        Self(mantissa)
    }

    /// Create from Decimal (optimized)
    #[inline]
    #[must_use]
    pub fn from_decimal(d: Decimal) -> Self {
        // Multiply by 10^9 to bring 9 decimal places into the integer part
        let scaled = d * Decimal::from(Self::FACTOR);
        let mantissa = scaled.round_dp(0).mantissa();

        if mantissa > i128::from(i64::MAX) {
            Self(i64::MAX)
        } else if mantissa < i128::from(i64::MIN) {
            Self(i64::MIN)
        } else {
            // Guarded above: mantissa is within [i64::MIN, i64::MAX].
            #[allow(clippy::cast_possible_truncation)]
            FastPrice(mantissa as i64)
        }
    }

    /// Get the raw mantissa value
    #[inline]
    #[must_use]
    pub const fn raw(self) -> i64 {
        self.0
    }

    /// Convert back to Decimal
    #[inline]
    #[must_use]
    pub fn to_decimal(self) -> Decimal {
        Decimal::from_i128_with_scale(i128::from(self.0), Self::SCALE)
    }

    /// Multiply two `FastPrices` (returns result at same scale)
    #[inline]
    #[must_use]
    pub fn checked_mul(self, rhs: Self) -> Option<Self> {
        let product = i128::from(self.0).checked_mul(i128::from(rhs.0))?;
        let result = product / i128::from(Self::FACTOR);

        if result > i128::from(i64::MAX) || result < i128::from(i64::MIN) {
            None
        } else {
            // Guarded above: result is within [i64::MIN, i64::MAX].
            #[allow(clippy::cast_possible_truncation)]
            Some(Self(result as i64))
        }
    }

    /// Fast multiplication without overflow checks (use only when bounds are known)
    #[inline]
    #[must_use]
    pub fn unchecked_mul(self, rhs: Self) -> Self {
        // The contract IS "bounds are known" (see doc); the caller vouches.
        #[allow(clippy::cast_possible_truncation)]
        Self(((i128::from(self.0) * i128::from(rhs.0)) / i128::from(Self::FACTOR)) as i64)
    }

    /// Add two `FastPrices`
    #[inline]
    #[must_use]
    pub fn checked_add(self, rhs: Self) -> Option<Self> {
        Some(Self(self.0.checked_add(rhs.0)?))
    }

    /// Fast addition without overflow checks
    #[inline]
    #[must_use]
    pub fn unchecked_add(self, rhs: Self) -> Self {
        Self(self.0 + rhs.0)
    }

    /// Subtract two `FastPrices`
    #[inline]
    #[must_use]
    pub fn checked_sub(self, rhs: Self) -> Option<Self> {
        Some(Self(self.0.checked_sub(rhs.0)?))
    }

    /// Fast subtraction without overflow checks
    #[inline]
    #[must_use]
    pub fn unchecked_sub(self, rhs: Self) -> Self {
        Self(self.0 - rhs.0)
    }
}

impl From<Decimal> for FastPrice {
    #[inline]
    fn from(d: Decimal) -> Self {
        // Multiply by 10^9 to bring 9 decimal places into the integer part
        let scaled = d * Decimal::from(Self::FACTOR);
        // Round to 0 decimal places to get the clean integer representation at our fixed scale
        let normalized = scaled.round_dp(0);
        let mantissa = normalized.mantissa();

        // Truncate/clamp to i64
        if mantissa > i128::from(i64::MAX) {
            Self(i64::MAX)
        } else if mantissa < i128::from(i64::MIN) {
            Self(i64::MIN)
        } else {
            // Guarded above: mantissa is within [i64::MIN, i64::MAX].
            #[allow(clippy::cast_possible_truncation)]
            FastPrice(mantissa as i64)
        }
    }
}

impl PartialOrd for FastPrice {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FastPrice {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl std::fmt::Display for FastPrice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_decimal())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // unwrap is idiomatic in test assertions

    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_roundtrip_conversion() {
        let prices = vec![
            dec!(0.0),
            dec!(1.0),
            dec!(0.123456789),
            dec!(999999.999999999),
            dec!(0.000000001),
        ];

        for price in prices {
            let fast = FastPrice::from(price);
            let back = fast.to_decimal();
            assert_eq!(price.round_dp(9), back, "Roundtrip failed for {}", price);
        }
    }

    #[test]
    fn test_ordering() {
        let a = FastPrice::from(dec!(1.50));
        let b = FastPrice::from(dec!(2.00));
        let c = FastPrice::from(dec!(1.50));

        assert!(a < b);
        assert!(b > a);
        assert_eq!(a, c);
    }

    #[test]
    fn test_arithmetic() {
        let a = FastPrice::from(dec!(1.5));
        let b = FastPrice::from(dec!(0.5));

        assert_eq!(
            a.checked_add(b).unwrap().to_decimal(),
            dec!(1.5).round_dp(9) + dec!(0.5).round_dp(9)
        );
        assert_eq!(
            a.checked_sub(b).unwrap().to_decimal(),
            dec!(1.0).round_dp(9)
        );
        assert_eq!(
            a.checked_mul(b).unwrap().to_decimal(),
            dec!(0.75).round_dp(9)
        );
    }

    #[test]
    fn test_arithmetic_overflow() {
        let max = FastPrice::from_raw(i64::MAX);
        let bit = FastPrice::from_raw(1);

        assert!(max.checked_add(bit).is_none());
    }

    #[test]
    fn test_sort_performance_equivalence() {
        let decimals = vec![dec!(3.14), dec!(1.00), dec!(2.71), dec!(0.50), dec!(1.00)];

        let mut sorted_decimals = decimals.clone();
        sorted_decimals.sort();

        let mut fast_prices: Vec<FastPrice> =
            decimals.iter().map(|d| FastPrice::from(*d)).collect();
        fast_prices.sort();
        let sorted_via_fast: Vec<Decimal> = fast_prices.iter().map(|f| f.to_decimal()).collect();

        assert_eq!(
            sorted_decimals
                .iter()
                .map(|d| d.round_dp(9))
                .collect::<Vec<_>>(),
            sorted_via_fast
        );
    }
}
