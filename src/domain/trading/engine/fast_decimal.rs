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
/// use gridtokenx_trading_service::domain::trading::engine::fast_decimal::FastPrice;
/// use rust_decimal_macros::dec;
/// let price = FastPrice::from(dec!(1.234567890));
/// assert_eq!(price.to_decimal(), dec!(1.234567890));
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash)]
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
    pub const fn from_raw(mantissa: i64) -> Self {
        Self(mantissa)
    }

    /// Get the raw mantissa value
    #[inline]
    pub const fn raw(self) -> i64 {
        self.0
    }

    /// Convert back to Decimal
    #[inline]
    pub fn to_decimal(self) -> Decimal {
        Decimal::from_i128_with_scale(self.0 as i128, Self::SCALE)
    }

    /// Multiply two FastPrices (returns result at same scale)
    #[inline]
    pub fn checked_mul(self, rhs: Self) -> Option<Self> {
        // (a * 10^9) * (b * 10^9) / 10^9 = (a * b) * 10^9
        // Use i128 for intermediate product to prevent overflow
        let product = (self.0 as i128).checked_mul(rhs.0 as i128)?;
        let result = product / (Self::FACTOR as i128);
        
        if result > i64::MAX as i128 || result < i64::MIN as i128 {
            None
        } else {
            Some(Self(result as i64))
        }
    }

    /// Add two FastPrices
    #[inline]
    pub fn checked_add(self, rhs: Self) -> Option<Self> {
        Some(Self(self.0.checked_add(rhs.0)?))
    }

    /// Subtract two FastPrices
    #[inline]
    pub fn checked_sub(self, rhs: Self) -> Option<Self> {
        Some(Self(self.0.checked_sub(rhs.0)?))
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
        if mantissa > i64::MAX as i128 {
            Self(i64::MAX)
        } else if mantissa < i64::MIN as i128 {
            Self(i64::MIN)
        } else {
            FastPrice(mantissa as i64)
        }
    }
}

impl PartialEq for FastPrice {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
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
            assert_eq!(
                price.round_dp(9),
                back,
                "Roundtrip failed for {}",
                price
            );
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

        assert_eq!(a.checked_add(b).unwrap().to_decimal(), dec!(1.5).round_dp(9) + dec!(0.5).round_dp(9));
        assert_eq!(a.checked_sub(b).unwrap().to_decimal(), dec!(1.0).round_dp(9));
        
        // Multiplication: 1.5 * 0.5 = 0.75
        assert_eq!(a.checked_mul(b).unwrap().to_decimal(), dec!(0.75).round_dp(9));
    }

    #[test]
    fn test_arithmetic_overflow() {
        let max = FastPrice::from_raw(i64::MAX);
        let bit = FastPrice::from_raw(1);
        
        assert!(max.checked_add(bit).is_none());
        
        let large = FastPrice::from(dec!(1000000000)); // 10^9
        assert!(large.checked_mul(large).is_none()); // (10^9 * 10^9) / 10^9 = 10^9. Wait, 10^9 * 10^9 is 10^18. i64::MAX is ~9e18. 
        // 10^9 in FastPrice is 10^18 raw. 
        // (10^18 * 10^18) / 10^9 = 10^27. Definitely overflows i64 (and i128 intermediate is fine as 10^36 < 2^128 ~ 3.4e38).
    }

    #[test]
    fn test_extreme_conversions() {
        // Very small positive
        let small = dec!(0.0000000001); // 10 decimal places, should be 0 in FastPrice (9 scale)
        assert_eq!(FastPrice::from(small).to_decimal(), dec!(0));
        
        let smallest = dec!(0.000000001); // 9 decimal places
        assert_eq!(FastPrice::from(smallest).to_decimal(), dec!(0.000000001));

        // Clamping
        let too_big = dec!(9223372036.854775807); // ~i64::MAX / 10^9
        // i64::MAX is 9,223,372,036,854,775,807
        // So 9,223,372,036.854775807 * 10^9 = i64::MAX.
        assert_eq!(FastPrice::from(too_big).raw(), i64::MAX);
    }

    #[test]
    fn test_sort_performance_equivalence() {
        // Verify that sorting FastPrice produces same order as sorting Decimal
        let decimals = vec![
            dec!(3.14), dec!(1.00), dec!(2.71), dec!(0.50), dec!(1.00),
        ];

        let mut sorted_decimals = decimals.clone();
        sorted_decimals.sort();

        let mut fast_prices: Vec<FastPrice> = decimals.iter().map(|d| FastPrice::from(*d)).collect();
        fast_prices.sort();
        let sorted_via_fast: Vec<Decimal> = fast_prices.iter().map(|f| f.to_decimal()).collect();

        assert_eq!(
            sorted_decimals.iter().map(|d| d.round_dp(9)).collect::<Vec<_>>(),
            sorted_via_fast
        );
    }
}
