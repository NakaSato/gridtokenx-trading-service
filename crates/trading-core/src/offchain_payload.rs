//! Canonical encoding of the off-chain order payload that users sign with their
//! wallet, and that `trading::settle_offchain_match` verifies on-chain.
//!
//! # Why this lives here
//!
//! Settlement via `settle_offchain_match` embeds a native Ed25519 verify
//! instruction carrying `(user_pubkey, message, signature)`. The on-chain handler
//! recomputes the message from the payload it receives as an instruction argument
//! and rejects the trade unless the signed message matches
//! (`settle_offchain.rs:705-714`). So THREE implementations must agree byte for
//! byte: the browser that signs, this service that builds the Ed25519
//! instruction, and the program that verifies.
//!
//! # Why the service recomputes instead of trusting `payload_bytes`
//!
//! The client sends the bytes it signed. Replaying those verbatim would let a
//! client sign one price and submit another: the signature would verify against
//! its own bytes while the order book carried different terms. [`message_for`]
//! derives the message from the values the service actually stored, so a mismatch
//! is caught at placement instead of settling on terms nobody agreed to.
//!
//! Layout — 77 bytes, all integers little-endian, mirroring
//! `OffchainOrderPayload::get_message()`:
//!
//! ```text
//! order_id[16] ‖ user[32] ‖ energy_amount u64 ‖ price_per_kwh u64
//!              ‖ side u8 ‖ zone_id u32 ‖ expires_at i64
//! ```

/// Total signed-message length. Asserted in tests so a field added on either side
/// of the boundary fails loudly here rather than as an opaque on-chain error.
pub const OFFCHAIN_MESSAGE_LEN: usize = 16 + 32 + 8 + 8 + 1 + 4 + 8;

/// Side discriminants as encoded in the signed message. These are the on-chain
/// values (`settle_offchain.rs:394`), not this crate's `OrderSide` repr — do not
/// substitute one for the other.
pub const SIDE_BUY: u8 = 0;
pub const SIDE_SELL: u8 = 1;

/// Energy is a 9-decimal mint; currency (THBC) is 6-decimal.
/// See `docs/blockchain-tokens.md` §1.
pub const ENERGY_SCALE: i64 = 1_000_000_000;
pub const CURRENCY_SCALE: i64 = 1_000_000;

/// kWh → 9-decimal base units, truncating (never rounding up — that would sign
/// for more energy than the order carries).
///
/// The browser, this service, and the settlement builder must all convert the
/// same way or the signature verifies against different numbers than the order
/// stores. Mirrors the existing conversions at
/// `trading-infra/src/blockchain/mod.rs:91` and `blockchain/settlement.rs:131`.
/// Returns `None` on overflow or a negative amount rather than silently yielding
/// zero.
#[must_use]
pub fn energy_to_base_units(kwh: rust_decimal::Decimal) -> Option<u64> {
    to_base_units(kwh, ENERGY_SCALE)
}

/// Currency → 6-decimal base units, truncating. See [`energy_to_base_units`].
#[must_use]
pub fn currency_to_base_units(amount: rust_decimal::Decimal) -> Option<u64> {
    to_base_units(amount, CURRENCY_SCALE)
}

fn to_base_units(value: rust_decimal::Decimal, scale: i64) -> Option<u64> {
    use rust_decimal::prelude::ToPrimitive;
    if value.is_sign_negative() {
        return None;
    }
    // checked_mul, not `*`: Decimal's multiply PANICS on overflow, which would
    // take down the request handler on an absurd amount instead of rejecting it.
    value
        .checked_mul(rust_decimal::Decimal::from(scale))?
        .trunc()
        .to_u64()
}

/// The exact bytes a user's wallet must sign for an off-chain order.
///
/// `order_id` is the order UUID's 16 raw bytes; `user` the signer's Solana public
/// key bytes. `energy_amount` and `price_per_kwh` are in on-chain base units (9-dec
/// energy, 6-dec currency) — NOT decimals — because that is what the program
/// compares against.
#[must_use]
pub fn message_for(
    order_id: &[u8; 16],
    user: &[u8; 32],
    energy_amount: u64,
    price_per_kwh: u64,
    side: u8,
    zone_id: u32,
    expires_at: i64,
) -> Vec<u8> {
    let mut msg = Vec::with_capacity(OFFCHAIN_MESSAGE_LEN);
    msg.extend_from_slice(order_id);
    msg.extend_from_slice(user);
    msg.extend_from_slice(&energy_amount.to_le_bytes());
    msg.extend_from_slice(&price_per_kwh.to_le_bytes());
    msg.push(side);
    msg.extend_from_slice(&zone_id.to_le_bytes());
    msg.extend_from_slice(&expires_at.to_le_bytes());
    msg
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixture shared with the frontend encoder. `lib/__tests__/order-signing.test.ts`
    /// asserts the identical hex for the identical inputs; if you change one side,
    /// change both or settlement breaks on-chain with no useful error.
    const FIXTURE_ORDER_ID: [u8; 16] = [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10,
    ];
    const FIXTURE_USER: [u8; 32] = [0xab; 32];

    fn fixture() -> Vec<u8> {
        message_for(
            &FIXTURE_ORDER_ID,
            &FIXTURE_USER,
            5_000_000_000, // 5 kWh at 9 decimals
            2_500_000,     // 2.50 currency at 6 decimals
            SIDE_SELL,
            1,
            1_800_000_000,
        )
    }

    #[test]
    fn message_is_exactly_77_bytes() {
        assert_eq!(fixture().len(), OFFCHAIN_MESSAGE_LEN);
        assert_eq!(OFFCHAIN_MESSAGE_LEN, 77);
    }

    /// Field offsets in the signed message. Named so an accidental reordering
    /// shows up as a failing assertion rather than silent on-chain rejection.
    const OFF_ORDER_ID: usize = 0;
    const OFF_USER: usize = 16;
    const OFF_ENERGY: usize = 48;
    const OFF_PRICE: usize = 56;
    const OFF_SIDE: usize = 64;
    const OFF_ZONE: usize = 65;
    const OFF_EXPIRES: usize = 69;

    #[test]
    fn message_matches_pinned_fixture() {
        // Pinned byte-for-byte — this is the cross-language contract. The TS test
        // in the frontend asserts this exact string for the same inputs.
        let expected = concat!(
            "0102030405060708090a0b0c0d0e0f10", // order_id
            "abababababababababababababababababababababababababababababababab", // user (32B)
            "00f2052a01000000",                 // 5_000_000_000 LE
            "a025260000000000",                 // 2_500_000 LE
            "01",                               // side = sell
            "01000000",                         // zone_id = 1
            "00d2496b00000000",                 // 1_800_000_000 LE
        );
        assert_eq!(hex(&fixture()), expected);
    }

    #[test]
    fn field_offsets_and_endianness_are_stable() {
        let buy = message_for(&FIXTURE_ORDER_ID, &FIXTURE_USER, 7, 9, SIDE_BUY, 3, 0);
        assert_eq!(&buy[OFF_ORDER_ID..OFF_USER], &FIXTURE_ORDER_ID);
        assert_eq!(&buy[OFF_USER..OFF_ENERGY], &FIXTURE_USER);
        assert_eq!(&buy[OFF_ENERGY..OFF_PRICE], &7u64.to_le_bytes());
        assert_eq!(&buy[OFF_PRICE..OFF_SIDE], &9u64.to_le_bytes());
        assert_eq!(buy[OFF_SIDE], SIDE_BUY);
        assert_eq!(&buy[OFF_ZONE..OFF_EXPIRES], &3u32.to_le_bytes());
    }

    #[test]
    fn side_changes_the_signed_message() {
        let buy = message_for(&FIXTURE_ORDER_ID, &FIXTURE_USER, 1, 1, SIDE_BUY, 0, 0);
        let sell = message_for(&FIXTURE_ORDER_ID, &FIXTURE_USER, 1, 1, SIDE_SELL, 0, 0);
        assert_ne!(buy, sell, "a buy signature must not validate a sell");
    }

    /// A negative `expires_at` must round-trip as two's-complement LE, matching
    /// Rust's `i64::to_le_bytes` and the TS `DataView.setBigInt64(.., true)`.
    #[test]
    fn negative_expiry_encodes_as_twos_complement() {
        let m = message_for(&FIXTURE_ORDER_ID, &FIXTURE_USER, 1, 1, SIDE_BUY, 0, -1);
        assert_eq!(&m[OFF_EXPIRES..], &[0xff; 8]);
    }

    #[test]
    fn base_unit_conversion_matches_the_onchain_scales() {
        use rust_decimal_macros::dec;
        assert_eq!(energy_to_base_units(dec!(5.0)), Some(5_000_000_000));
        assert_eq!(currency_to_base_units(dec!(2.50)), Some(2_500_000));
        // Sub-unit dust truncates down, never up.
        assert_eq!(energy_to_base_units(dec!(0.0000000004)), Some(0));
        assert_eq!(currency_to_base_units(dec!(1.9999999)), Some(1_999_999));
    }

    #[test]
    fn base_unit_conversion_rejects_negative_and_overflow() {
        use rust_decimal_macros::dec;
        assert_eq!(energy_to_base_units(dec!(-1)), None);
        // u64::MAX kWh scaled by 1e9 cannot fit — must be None, not a wrapped value.
        assert_eq!(energy_to_base_units(rust_decimal::Decimal::MAX), None);
    }

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }
}
