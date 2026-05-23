//! Robust numeric conversion and safe arithmetic for the Trading Service.
//!
//! This module provides safe helpers for:
//! - Converting Decimal to Atomic Unit (u64) for blockchain
//! - Checked division to prevent division-by-zero or NaN in matching
//! - Safe casting between primitive types with context-enriched errors

use anyhow::{anyhow, Context, Result};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

/// Converts a Decimal value to a u64 in atomic units (e.g., 9 decimals for SOL).
///
/// Returns an error if:
/// - The value would overflow a u64
/// - The value is negative (energy/amounts must be positive)
/// - Precision loss would occur (input has more than `decimals` significant fractional digits)
pub fn to_u64_atomic(val: Decimal, decimals: u32, label: &str) -> Result<u64> {
    if val.is_sign_negative() {
        return Err(anyhow!("Negative value for {}: {}", label, val));
    }

    let multiplier = Decimal::from(10u64.pow(decimals));
    let atomic_val = val * multiplier;

    // Ensure it's an integer (no fractional parts after scaling)
    if atomic_val.fract() != Decimal::ZERO {
        return Err(anyhow!(
            "Precision loss for {}: {} with {} decimals would lose fractional digits",
            label,
            val,
            decimals
        ));
    }

    atomic_val.to_u64().context(format!(
        "Value too large for u64 after scaling {}: {} (scaled: {})",
        label, val, atomic_val
    ))
}

/// Safely divides two Decimal values, returning an error instead of panic/NaN for zero denominators.
pub fn safe_div(numerator: Decimal, denominator: Decimal, label: &str) -> Result<Decimal> {
    if denominator.is_zero() {
        return Err(anyhow!(
            "Division by zero for {}: {} / {}",
            label,
            numerator,
            denominator
        ));
    }
    Ok(numerator / denominator)
}

/// Safely casts an i32 (common for DB zone_id) to u32
pub fn to_u32_safe(val: i32, label: &str) -> Result<u32> {
    if val < 0 {
        return Err(anyhow!("Negative value for {}: {}", label, val));
    }
    u32::try_from(val).context(format!("Failed to cast {} ({}) to u32", label, val))
}

/// Safely casts u32 to i64 (for DB order_index)
pub fn to_i64_safe(val: u32, label: &str) -> Result<i64> {
    i64::try_from(val).context(format!("Failed to cast {} ({}) to i64", label, val))
}
