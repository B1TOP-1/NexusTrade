// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Native (in-process) Lighter transaction signer.
//!
//! The signing math (`goldilocks`, `goldilocks_quintic`, `curve`, `poseidon2`,
//! `scalar`, `schnorr`) is vendored verbatim from the production-proven
//! `lighter_rust_signer` package. The C ABI shell of that package is dropped in
//! favour of a safe, instance-based Rust API so the NautilusTrader execution
//! client can sign create/cancel transactions in-process without any FFI,
//! Python interop, or `cdylib` build step.
//!
//! Field ordering, timing, and `tx_info` JSON layout are byte-identical to the
//! upstream signer; only the transport (extern "C") wrapper is replaced.

// Vendored verbatim from `lighter_rust_signer`; keep upstream naming/lints intact.
#[allow(
    dead_code,
    non_snake_case,
    unreachable_pub,
    missing_debug_implementations,
    clippy::all,
    clippy::pedantic,
    clippy::nursery
)]
mod curve;
#[allow(
    dead_code,
    non_snake_case,
    unreachable_pub,
    missing_debug_implementations,
    clippy::all,
    clippy::pedantic,
    clippy::nursery
)]
mod goldilocks;
#[allow(
    dead_code,
    non_snake_case,
    unreachable_pub,
    missing_debug_implementations,
    clippy::all,
    clippy::pedantic,
    clippy::nursery
)]
mod goldilocks_quintic;
#[allow(
    dead_code,
    non_snake_case,
    unreachable_pub,
    missing_debug_implementations,
    clippy::all,
    clippy::pedantic,
    clippy::nursery
)]
mod poseidon2;
#[allow(
    dead_code,
    non_snake_case,
    unreachable_pub,
    missing_debug_implementations,
    clippy::all,
    clippy::pedantic,
    clippy::nursery
)]
mod scalar;
#[allow(
    dead_code,
    non_snake_case,
    unreachable_pub,
    missing_debug_implementations,
    clippy::all,
    clippy::pedantic,
    clippy::nursery
)]
mod schnorr;

use std::time::{SystemTime, UNIX_EPOCH};

use goldilocks::GoldilocksField;
use goldilocks_quintic::QuinticElement;
use scalar::Scalar;

/// Lighter L2 transaction type for a create-order transaction.
pub const TX_TYPE_L2_CREATE_ORDER: u8 = 14;
/// Lighter L2 transaction type for a cancel-order transaction.
pub const TX_TYPE_L2_CANCEL_ORDER: u8 = 15;

/// Validity window applied to the `ExpiredAt` field of every signed tx.
const DEFAULT_EXPIRE_MS: i64 = 599_000;
/// Default order lifetime used when `order_expiry == -1`.
const DEFAULT_ORDER_EXPIRY_MS: i64 = 28 * 24 * 60 * 60 * 1000;

/// A signed Lighter transaction ready to be posted to `/api/v1/sendTx`.
#[derive(Clone, PartialEq, Eq)]
pub struct LighterSignedPayload {
    /// Lighter L2 transaction type (14 = create order, 15 = cancel order).
    pub tx_type: u8,
    /// The signed `tx_info` JSON string posted to `sendTx`.
    pub tx_info: String,
    /// Hex-encoded message hash (matches the upstream `txHash`).
    pub tx_hash: String,
}

impl std::fmt::Debug for LighterSignedPayload {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LighterSignedPayload")
            .field("tx_type", &self.tx_type)
            .field("tx_info", &"<redacted>")
            .field("tx_hash", &self.tx_hash)
            .finish()
    }
}

/// An in-process Lighter signer bound to a single API key / account.
///
/// Holds the parsed private key plus the static identifiers required to build
/// the Poseidon message hash. Equivalent to the upstream `CreateClient` state,
/// but instance-scoped instead of a process-global singleton.
#[derive(Clone)]
pub struct LighterSigner {
    private_key: Scalar,
    chain_id: u32,
    api_key_index: u8,
    account_index: i64,
}

impl std::fmt::Debug for LighterSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LighterSigner")
            .field("chain_id", &self.chain_id)
            .field("api_key_index", &self.api_key_index)
            .field("account_index", &self.account_index)
            .field("private_key", &"<redacted>")
            .finish()
    }
}

impl LighterSigner {
    /// Creates a signer from a hex-encoded private key and account identifiers.
    ///
    /// # Errors
    ///
    /// Returns an error if the private key is not valid hex or not a valid scalar.
    pub fn new(
        private_key_hex: &str,
        chain_id: u32,
        api_key_index: u8,
        account_index: i64,
    ) -> Result<Self, String> {
        let bytes = parse_hex_bytes(private_key_hex)?;
        let private_key = Scalar::from_le_bytes(&bytes).map_err(str::to_string)?;
        Ok(Self {
            private_key,
            chain_id,
            api_key_index,
            account_index,
        })
    }

    #[must_use]
    pub const fn api_key_index(&self) -> u8 {
        self.api_key_index
    }

    #[must_use]
    pub const fn account_index(&self) -> i64 {
        self.account_index
    }

    /// Signs a create-order transaction.
    ///
    /// `nonce` is supplied explicitly by the caller (matching the upstream
    /// signer's explicit-nonce hot path). When `order_expiry == -1` the default
    /// 28-day lifetime is applied.
    ///
    /// # Errors
    ///
    /// Returns an error if the Schnorr signing step fails (e.g. RNG failure).
    #[allow(clippy::too_many_arguments)]
    pub fn sign_create_order(
        &self,
        market_index: i16,
        client_order_index: i64,
        base_amount: i64,
        price: u32,
        is_ask: u8,
        order_type: u8,
        time_in_force: u8,
        reduce_only: u8,
        trigger_price: u32,
        order_expiry: i64,
        nonce: i64,
    ) -> Result<LighterSignedPayload, String> {
        let timing = create_order_timing(order_expiry, now_ms());
        let hash = create_order_message_hash(
            self.chain_id,
            nonce,
            timing.expired_at,
            self.account_index,
            self.api_key_index,
            market_index,
            client_order_index,
            base_amount,
            price,
            is_ask,
            order_type,
            time_in_force,
            reduce_only,
            trigger_price,
            timing.order_expiry,
        );
        let signature = schnorr::sign_hashed_message(hash, self.private_key)?.to_bytes();
        let tx_info = create_order_tx_info_json(
            self.account_index,
            self.api_key_index,
            market_index,
            client_order_index,
            base_amount,
            price,
            is_ask,
            order_type,
            time_in_force,
            reduce_only,
            trigger_price,
            timing.order_expiry,
            timing.expired_at,
            nonce,
            &signature,
        );
        Ok(LighterSignedPayload {
            tx_type: TX_TYPE_L2_CREATE_ORDER,
            tx_info,
            tx_hash: hex_encode(&hash.to_le_bytes()),
        })
    }

    /// Signs a cancel-order transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if the Schnorr signing step fails (e.g. RNG failure).
    pub fn sign_cancel_order(
        &self,
        market_index: i16,
        order_index: i64,
        nonce: i64,
    ) -> Result<LighterSignedPayload, String> {
        let expired_at = now_ms() + DEFAULT_EXPIRE_MS;
        let hash = cancel_order_message_hash(
            self.chain_id,
            nonce,
            expired_at,
            self.account_index,
            self.api_key_index,
            market_index,
            order_index,
        );
        let signature = schnorr::sign_hashed_message(hash, self.private_key)?.to_bytes();
        let tx_info = cancel_order_tx_info_json(
            self.account_index,
            self.api_key_index,
            market_index,
            order_index,
            expired_at,
            nonce,
            &signature,
        );
        Ok(LighterSignedPayload {
            tx_type: TX_TYPE_L2_CANCEL_ORDER,
            tx_info,
            tx_hash: hex_encode(&hash.to_le_bytes()),
        })
    }

    /// Builds a Lighter WebSocket auth token valid until `deadline_secs`
    /// (absolute Unix time in seconds).
    ///
    /// Mirrors the official Go signer `ConstructAuthToken`: the ASCII message
    /// `"{deadline}:{account_index}:{api_key_index}"` is packed into goldilocks
    /// field elements (8-byte little-endian chunks, zero padded), Poseidon2
    /// hashed, and Schnorr signed. The returned token is `"{message}:{sig_hex}"`.
    ///
    /// # Errors
    ///
    /// Returns an error if the Schnorr signing step fails (e.g. RNG failure).
    pub fn create_auth_token(&self, deadline_secs: i64) -> Result<String, String> {
        let message = format!(
            "{deadline_secs}:{}:{}",
            self.account_index, self.api_key_index
        );
        let hash = auth_message_hash(&message);
        let signature = schnorr::sign_hashed_message(hash, self.private_key)?.to_bytes();
        Ok(format!("{message}:{}", hex_encode(&signature)))
    }
}

/// Packs ASCII/UTF-8 bytes into goldilocks field elements using 8-byte
/// little-endian chunks (the trailing chunk is zero-padded to 8 bytes).
///
/// Mirrors `goldilocks.ArrayFromCanonicalLittleEndianBytes` from the upstream
/// `poseidon_crypto` package.
fn pack_le8_fields(bytes: &[u8]) -> Vec<GoldilocksField> {
    let mut fields = Vec::with_capacity(bytes.len().div_ceil(8));
    let mut i = 0;
    while i < bytes.len() {
        let mut chunk = [0u8; 8];
        let end = (i + 8).min(bytes.len());
        chunk[..end - i].copy_from_slice(&bytes[i..end]);
        fields.push(GoldilocksField::from_le_bytes(chunk));
        i += 8;
    }
    fields
}

/// Poseidon2 hash of the auth-token message string (see [`LighterSigner::create_auth_token`]).
fn auth_message_hash(message: &str) -> QuinticElement {
    poseidon2::hash_to_quintic_extension(&pack_le8_fields(message.as_bytes()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CreateOrderTiming {
    order_expiry: i64,
    expired_at: i64,
}

fn create_order_timing(order_expiry: i64, now_ms: i64) -> CreateOrderTiming {
    CreateOrderTiming {
        order_expiry: if order_expiry == -1 {
            now_ms + DEFAULT_ORDER_EXPIRY_MS
        } else {
            order_expiry
        },
        expired_at: now_ms + DEFAULT_EXPIRE_MS,
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as i64)
}

fn field_from_i64(value: i64) -> GoldilocksField {
    GoldilocksField::from_i64(value)
}

#[allow(clippy::too_many_arguments)]
fn create_order_message_hash(
    chain_id: u32,
    nonce: i64,
    expired_at: i64,
    account_index: i64,
    api_key_index: u8,
    market_index: i16,
    client_order_index: i64,
    base_amount: i64,
    price: u32,
    is_ask: u8,
    order_type: u8,
    time_in_force: u8,
    reduce_only: u8,
    trigger_price: u32,
    order_expiry: i64,
) -> QuinticElement {
    poseidon2::hash_to_quintic_extension(&[
        GoldilocksField::from_u32(chain_id),
        GoldilocksField::from_u32(TX_TYPE_L2_CREATE_ORDER as u32),
        field_from_i64(nonce),
        field_from_i64(expired_at),
        field_from_i64(account_index),
        GoldilocksField::from_u32(api_key_index as u32),
        GoldilocksField::from_u32(market_index as u32),
        field_from_i64(client_order_index),
        field_from_i64(base_amount),
        GoldilocksField::from_u32(price),
        GoldilocksField::from_u32(is_ask as u32),
        GoldilocksField::from_u32(order_type as u32),
        GoldilocksField::from_u32(time_in_force as u32),
        GoldilocksField::from_u32(reduce_only as u32),
        GoldilocksField::from_u32(trigger_price),
        field_from_i64(order_expiry),
    ])
}

fn cancel_order_message_hash(
    chain_id: u32,
    nonce: i64,
    expired_at: i64,
    account_index: i64,
    api_key_index: u8,
    market_index: i16,
    order_index: i64,
) -> QuinticElement {
    poseidon2::hash_to_quintic_extension(&[
        GoldilocksField::from_u32(chain_id),
        GoldilocksField::from_u32(TX_TYPE_L2_CANCEL_ORDER as u32),
        field_from_i64(nonce),
        field_from_i64(expired_at),
        field_from_i64(account_index),
        GoldilocksField::from_u32(api_key_index as u32),
        GoldilocksField::from_u32(market_index as u32),
        field_from_i64(order_index),
    ])
}

#[allow(clippy::too_many_arguments)]
fn create_order_tx_info_json(
    account_index: i64,
    api_key_index: u8,
    market_index: i16,
    client_order_index: i64,
    base_amount: i64,
    price: u32,
    is_ask: u8,
    order_type: u8,
    time_in_force: u8,
    reduce_only: u8,
    trigger_price: u32,
    order_expiry: i64,
    expired_at: i64,
    nonce: i64,
    signature: &[u8],
) -> String {
    format!(
        "{{\"AccountIndex\":{},\"ApiKeyIndex\":{},\"MarketIndex\":{},\"ClientOrderIndex\":{},\"BaseAmount\":{},\"Price\":{},\"IsAsk\":{},\"Type\":{},\"TimeInForce\":{},\"ReduceOnly\":{},\"TriggerPrice\":{},\"OrderExpiry\":{},\"ExpiredAt\":{},\"Nonce\":{},\"Sig\":\"{}\"}}",
        account_index,
        api_key_index,
        market_index,
        client_order_index,
        base_amount,
        price,
        is_ask,
        order_type,
        time_in_force,
        reduce_only,
        trigger_price,
        order_expiry,
        expired_at,
        nonce,
        base64_encode(signature)
    )
}

fn cancel_order_tx_info_json(
    account_index: i64,
    api_key_index: u8,
    market_index: i16,
    order_index: i64,
    expired_at: i64,
    nonce: i64,
    signature: &[u8],
) -> String {
    format!(
        "{{\"AccountIndex\":{},\"ApiKeyIndex\":{},\"MarketIndex\":{},\"Index\":{},\"ExpiredAt\":{},\"Nonce\":{},\"Sig\":\"{}\"}}",
        account_index,
        api_key_index,
        market_index,
        order_index,
        expired_at,
        nonce,
        base64_encode(signature)
    )
}

fn parse_hex_bytes(value: &str) -> Result<Vec<u8>, String> {
    let hex = value.strip_prefix("0x").unwrap_or(value);
    if !hex.len().is_multiple_of(2) {
        return Err("hex length must be even".to_string());
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    for index in (0..hex.len()).step_by(2) {
        let high = hex_nibble(bytes[index]).ok_or_else(|| "invalid hex".to_string())?;
        let low = hex_nibble(bytes[index + 1]).ok_or_else(|| "invalid hex".to_string())?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    let mut index = 0;
    while index < bytes.len() {
        let b0 = bytes[index];
        let b1 = if index + 1 < bytes.len() {
            bytes[index + 1]
        } else {
            0
        };
        let b2 = if index + 2 < bytes.len() {
            bytes[index + 2]
        } else {
            0
        };

        out.push(TABLE[(b0 >> 2) as usize] as char);
        out.push(TABLE[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        if index + 1 < bytes.len() {
            out.push(TABLE[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if index + 2 < bytes.len() {
            out.push(TABLE[(b2 & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }

        index += 3;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // A throwaway, well-formed scalar private key (LE value = 2; 40 bytes / 80 hex).
    const TEST_KEY_HEX: &str = "0200000000000000000000000000000000000000\
                                0000000000000000000000000000000000000000";

    fn test_signer() -> LighterSigner {
        LighterSigner::new(TEST_KEY_HEX, 304, 1, 42).expect("valid test key")
    }

    #[test]
    fn create_order_timing_uses_single_now_for_expiry_fields() {
        let timing = create_order_timing(-1, 1_700_000_000_000);
        assert_eq!(
            timing.order_expiry,
            1_700_000_000_000 + DEFAULT_ORDER_EXPIRY_MS
        );
        assert_eq!(timing.expired_at, 1_700_000_000_000 + DEFAULT_EXPIRE_MS);
    }

    #[test]
    fn create_order_timing_preserves_explicit_order_expiry() {
        let timing = create_order_timing(1_700_111_222_333, 1_700_000_000_000);
        assert_eq!(timing.order_expiry, 1_700_111_222_333);
        assert_eq!(timing.expired_at, 1_700_000_000_000 + DEFAULT_EXPIRE_MS);
    }

    #[test]
    fn create_order_tx_info_has_expected_field_order_and_sig() {
        let json = create_order_tx_info_json(
            42,
            1,
            1,
            100,
            5,
            1000,
            0,
            0,
            0,
            0,
            0,
            -1,
            999,
            7,
            &[0xAB, 0xCD],
        );
        assert!(json.starts_with(
            "{\"AccountIndex\":42,\"ApiKeyIndex\":1,\"MarketIndex\":1,\"ClientOrderIndex\":100,"
        ));
        assert!(json.contains("\"Nonce\":7,"));
        assert!(json.ends_with(&format!("\"Sig\":\"{}\"}}", base64_encode(&[0xAB, 0xCD]))));
    }

    #[test]
    fn sign_create_order_is_deterministic_in_hash_and_well_formed() {
        let signer = test_signer();
        let a = signer
            .sign_create_order(1, 100, 1_000, 50_000, 0, 0, 1, 0, 0, -1, 5)
            .expect("sign create");
        assert_eq!(a.tx_type, TX_TYPE_L2_CREATE_ORDER);
        // 80-byte signature -> base64; 40-byte hash -> 80 hex chars.
        assert_eq!(a.tx_hash.len(), 80);
        assert!(a.tx_info.contains("\"Nonce\":5,"));
        assert!(a.tx_info.contains("\"MarketIndex\":1,"));
    }

    #[test]
    fn sign_cancel_order_is_well_formed() {
        let signer = test_signer();
        let c = signer.sign_cancel_order(1, 123, 6).expect("sign cancel");
        assert_eq!(c.tx_type, TX_TYPE_L2_CANCEL_ORDER);
        assert_eq!(c.tx_hash.len(), 80);
        assert!(c.tx_info.contains("\"Index\":123,"));
        assert!(c.tx_info.contains("\"Nonce\":6,"));
    }

    #[test]
    fn base64_matches_known_vector() {
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
    }

    #[test]
    fn create_auth_token_has_expected_shape() {
        let signer = test_signer(); // chain_id 304, api_key_index 1, account_index 42
        let token = signer.create_auth_token(1_900_000_000).expect("auth token");
        let parts: Vec<&str> = token.split(':').collect();
        assert_eq!(parts.len(), 4, "token = deadline:account:apikey:sig");
        assert_eq!(parts[0], "1900000000");
        assert_eq!(parts[1], "42");
        assert_eq!(parts[2], "1");
        // 80-byte Schnorr signature -> 160 lowercase hex chars.
        assert_eq!(parts[3].len(), 160);
        assert!(parts[3].bytes().all(|b| b.is_ascii_hexdigit()));
    }

    // Regression guard: the auth-token message hash construction must match the
    // official Lighter Go signer. Verified against an offline vector captured
    // from the bundled `lighter-signer` .dylib (CreateAuthToken) — no runtime
    // dependency on the .dylib. If this fails, our construction diverged from
    // `types.ConstructAuthToken` (lighter-go) and WS auth would be rejected.
    #[test]
    fn auth_message_hash_matches_official_signer_vector() {
        use super::curve::mul_generator;
        use super::goldilocks::GoldilocksField as F;
        use super::scalar::Scalar;

        // Offline vector from lighter-signer-darwin-arm64.dylib CreateAuthToken.
        // Throwaway key generated by the .dylib's GenerateAPIKey (not a real account).
        let priv_hex =
            "0xc89d22df8df76acee9f31bd35bdc15afde6324378e760ba8d4feaa233c6292318ad4849dc4285a50";
        let sig_hex = "eb94febe5e0097b00a32e80260018e2bea0bb61aa91de45eb1a2934f20627df24d77d7ac67f452517877ce2f24f510872ae0d746dec68b2d55e97dbfb66e9d3c686ffc28587599bc9646f1f903b3a302";
        let deadline: i64 = 1_900_000_000;
        let account_index: i64 = 42;
        let api_key_index: u8 = 1;

        let sk = Scalar::from_le_bytes(&parse_hex_bytes(priv_hex).unwrap()).unwrap();
        let sig = parse_hex_bytes(sig_hex).unwrap();
        let s = Scalar::from_le_bytes(&sig[..40]).unwrap();
        let e = Scalar::from_le_bytes(&sig[40..]).unwrap();
        let pub_point = mul_generator(&sk);

        // Reconstruct our auth message hash and verify the official signature
        // against it: R = s*G + e*P, then e' = H(R.encode() || m) must equal e.
        let message = format!("{deadline}:{account_index}:{api_key_index}");
        let m = auth_message_hash(&message);
        let r = mul_generator(&s).add(pub_point.mul(&e));
        let mut preimage = [F::zero(); 10];
        preimage[..5].copy_from_slice(&r.encode().0);
        preimage[5..].copy_from_slice(&m.0);
        let e_prime = Scalar::from_quintic(poseidon2::hash_to_quintic_extension(&preimage));
        assert_eq!(
            e_prime, e,
            "auth message hash construction diverged from official Lighter signer",
        );
    }

    #[test]
    fn pack_le8_fields_pads_trailing_chunk() {
        // "1900000000:42:1" is 15 bytes -> two 8-byte chunks (second zero-padded).
        let fields = pack_le8_fields(b"1900000000:42:1");
        assert_eq!(fields.len(), 2);
        // Exact multiple of 8 yields no extra padding chunk.
        assert_eq!(pack_le8_fields(b"abcdefgh").len(), 1);
        assert_eq!(pack_le8_fields(b"").len(), 0);
    }
}
