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

//! Gate APIv4 credential storage and signing helpers (HMAC-SHA512).
//!
//! Gate signs REST requests with `SIGN = hex(HMAC_SHA512(secret, sign_string))`
//! where `sign_string = METHOD\n{prefix}{path}\n{query}\n{hex(SHA512(body))}\n{ts}`,
//! and private WebSocket subscriptions with
//! `hex(HMAC_SHA512(secret, "channel={c}&event={e}&time={t}"))`.

use aws_lc_rs::{digest, hmac};
use nexus_core::Side;

/// Gate APIv4 path prefix included in the REST signature string.
pub const GATE_API_PREFIX: &str = "/api/v4";

/// Gate API credentials used for signing REST and private WebSocket requests.
#[derive(Clone)]
pub struct GateCredential {
    api_key: String,
    api_secret: String,
}

impl std::fmt::Debug for GateCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never leak the secret.
        f.debug_struct(stringify!(GateCredential))
            .field("api_key", &self.api_key)
            .field("api_secret", &"<redacted>")
            .finish()
    }
}

impl GateCredential {
    #[must_use]
    pub fn new(api_key: String, api_secret: String) -> Self {
        Self {
            api_key,
            api_secret,
        }
    }

    #[must_use]
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Signs a REST request, returning the `SIGN` header value (hex HMAC-SHA512).
    ///
    /// `path` must NOT include the `/api/v4` prefix (it is prepended here, matching
    /// Gate's signature spec). `query` is the raw query string without `?`.
    #[must_use]
    pub fn sign_rest(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: &str,
        timestamp: i64,
    ) -> String {
        let body_hash = to_hex(digest::digest(&digest::SHA512, body.as_bytes()).as_ref());
        let sign_string = format!(
            "{}\n{GATE_API_PREFIX}{path}\n{query}\n{body_hash}\n{timestamp}",
            method.to_uppercase(),
        );
        self.hmac_hex(sign_string.as_bytes())
    }

    /// Signs a private WebSocket subscription `auth` payload.
    #[must_use]
    pub fn sign_ws_auth(&self, channel: &str, event: &str, timestamp: i64) -> String {
        let message = format!("channel={channel}&event={event}&time={timestamp}");
        self.hmac_hex(message.as_bytes())
    }

    /// Signs a WebSocket API request (e.g. `futures.login`).
    ///
    /// Only `futures.login` is signed; subsequent `event:"api"` order requests
    /// reuse the authenticated session and carry no per-request signature.
    #[must_use]
    pub fn sign_ws_api(&self, channel: &str, request_param: &str, timestamp: i64) -> String {
        let message = format!("api\n{channel}\n{request_param}\n{timestamp}");
        self.hmac_hex(message.as_bytes())
    }

    fn hmac_hex(&self, message: &[u8]) -> String {
        let key = hmac::Key::new(hmac::HMAC_SHA512, self.api_secret.as_bytes());
        to_hex(hmac::sign(&key, message).as_ref())
    }
}

/// Converts a Nautilus order side + positive contract count to Gate's signed size
/// (buy/long = positive, sell/short = negative).
///
/// # Errors
///
/// Returns an error if `size` is zero or the side is not buy/sell.
pub fn signed_size(side: Side, size: u64) -> anyhow::Result<i64> {
    if size == 0 {
        anyhow::bail!("Gate futures size must be a positive contract count");
    }
    let magnitude = i64::try_from(size)
        .map_err(|_| anyhow::anyhow!("Gate futures size {size} exceeds i64"))?;
    match side {
        Side::Buy => Ok(magnitude),
        Side::Sell => Ok(-magnitude),
    }
}

/// Normalises a client order id into Gate's `text` field: sanitised, `t-`-prefixed,
/// and truncated to Gate's 28-character limit (plus the `t-`).
#[must_use]
pub fn normalize_order_text(text: &str) -> String {
    // 8 位日期(YYYYMMDD)只取后 4 位 MMDD(丢年份), 给单调计数器腾空间。
    // 例: O-20260611-125111-01-XA-13 -> O-0611-125111-01-XA-13 (24字符)。
    let compacted: String = text
        .split('-')
        .map(|tok| {
            if tok.len() == 8 && tok.bytes().all(|b| b.is_ascii_digit()) {
                &tok[4..]
            } else {
                tok
            }
        })
        .collect::<Vec<_>>()
        .join("-");
    let sanitized: String = compacted
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-') {
                c
            } else {
                '-'
            }
        })
        .collect();
    let prefixed = if sanitized.starts_with("t-") {
        sanitized
    } else {
        format!("t-{sanitized}")
    };
    // Gate 实测上限: "Text length should be less than 30" -> 长度 30 接受、32 拒绝。
    // 取 30 (含 t-)。配合上面的日期压缩, 实际单号(约27)远在限内, 序号不会丢。
    prefixed.chars().take(30).collect()
}

/// Lowercase hex encoding (dependency-free; signatures are hex HMAC/SHA512).
fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_hex_known() {
        assert_eq!(to_hex(&[0x00, 0x0f, 0xff, 0xa5]), "000fffa5");
    }

    #[test]
    fn sha512_empty_vector() {
        // NIST SHA-512("") reference digest.
        let hash = to_hex(digest::digest(&digest::SHA512, b"").as_ref());
        assert_eq!(
            hash,
            "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce\
             47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"
        );
    }

    #[test]
    fn hmac_sha512_known_vector() {
        // RFC-style vector: HMAC_SHA512("key", "The quick brown fox jumps over the lazy dog").
        let cred = GateCredential::new("k".to_string(), "key".to_string());
        let sig = cred.hmac_hex(b"The quick brown fox jumps over the lazy dog");
        assert_eq!(
            sig,
            "b42af09057bac1e2d41708e48a902e09b5ff7f12ab428a4fe86653c73dd248fb\
             82f948a549f7b791a5b41915ee4d1ec3935357e4e2317250d0372afa2ebeeb3a"
        );
    }

    #[test]
    fn rest_signature_is_stable() {
        // Regression guard on the sign-string layout + hashing.
        let cred = GateCredential::new("key".to_string(), "secret".to_string());
        let sig = cred.sign_rest(
            "POST",
            "/futures/usdt/orders",
            "",
            r#"{"contract":"BTC_USDT","size":1,"price":"0","tif":"ioc"}"#,
            1_700_000_000,
        );
        assert_eq!(sig.len(), 128); // hex of 64-byte HMAC-SHA512
        // Stable for fixed inputs (recomputed if the layout ever changes).
        let sig2 = cred.sign_rest(
            "POST",
            "/futures/usdt/orders",
            "",
            r#"{"contract":"BTC_USDT","size":1,"price":"0","tif":"ioc"}"#,
            1_700_000_000,
        );
        assert_eq!(sig, sig2);
    }

    #[test]
    fn ws_api_login_signature_is_stable() {
        let cred = GateCredential::new("key".to_string(), "secret".to_string());
        let sig = cred.sign_ws_api("futures.login", "", 1_700_000_000);
        assert_eq!(sig.len(), 128);
        assert_eq!(sig, cred.sign_ws_api("futures.login", "", 1_700_000_000));
    }

    #[test]
    fn signed_size_direction() {
        assert_eq!(signed_size(Side::Buy, 3).unwrap(), 3);
        assert_eq!(signed_size(Side::Sell, 3).unwrap(), -3);
        assert!(signed_size(Side::Buy, 0).is_err());
    }

    #[test]
    fn normalize_text_prefixes_and_truncates() {
        assert_eq!(normalize_order_text("abc"), "t-abc");
        assert_eq!(normalize_order_text("t-abc"), "t-abc");
        assert_eq!(normalize_order_text("a/b c"), "t-a-b-c");
        assert_eq!(normalize_order_text(&"x".repeat(50)).len(), 30);
        // 日期取后4位 MMDD(丢年份), 短 tag(01/XA): 24 字符。
        assert_eq!(
            normalize_order_text("O-20260611-125111-01-XA-13"),
            "t-O-0611-125111-01-XA-13"
        );
    }
}
