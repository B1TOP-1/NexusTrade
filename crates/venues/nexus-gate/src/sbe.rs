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

//! Gate SBE (Simple Binary Encoding) decoder for the `userTrade` push.
//!
//! On the `/sbe` endpoint, JSON arrives as text frames (subscribe acks, system)
//! and SBE arrives as binary frames. Each SBE frame is `MessageHeader` (8 bytes:
//! blockLength/templateId/schemaId/version, little-endian) + body. Decimals are
//! `mantissa * 10^exponent`; variable strings are length-prefixed (u8). This
//! module decodes the `userTrade` message (templateId 6) for the fills feed.
//!
//! Ported from the venue reference decoder (`bybot/market/gate_sbe_decoder.py`).

use rust_decimal::Decimal;

const SBE_HEADER_SIZE: usize = 8;
const USER_TRADE_TEMPLATE_ID: u16 = 6;

/// A fill decoded from an SBE `userTrade` frame (mirrors the JSON usertrades push).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateSbeUserTrade {
    pub trade_id: String,
    pub order_id: String,
    pub size: String,
    pub price: String,
    pub fee: String,
    pub role: String,
    pub contract: String,
    pub text: String,
}

/// Decodes an SBE binary frame, returning the `userTrade` fills it carries.
///
/// Returns an empty vec for non-`userTrade` templates (other channels' frames).
///
/// # Errors
///
/// Returns an error if the frame is truncated or malformed.
pub fn decode_user_trades(frame: &[u8]) -> anyhow::Result<Vec<GateSbeUserTrade>> {
    let mut r = SbeReader::new(frame);
    let (_block_length, template_id, _schema_id, _version) = r.read_header()?;
    if template_id != USER_TRADE_TEMPLATE_ID {
        return Ok(Vec::new());
    }

    // Root block: time + per-field decimal exponents.
    let _time_us = r.read_i64()?;
    let _event_code = r.read_i8()?;
    let px_exponent = r.read_i8()?;
    let sz_exponent = r.read_i8()?;
    let fee_exponent = r.read_i8()?;
    let _point_exponent = r.read_i8()?;

    let (entry_block_length, count) = r.read_group_header()?;
    let mut trades = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let entry_start = r.pos;
        let trade_id = r.read_u64()?;
        let order_id = r.read_u64()?;
        let _trade_time_us = r.read_i64()?;
        let size_mantissa = r.read_i64()?;
        let price_mantissa = r.read_i64()?;
        let fee_mantissa = r.read_i64()?;
        let _point_fee_mantissa = r.read_i64()?;
        let _close_size_mantissa = r.read_i64()?;
        let role_code = r.read_i8()?;
        // Skip any trailing fixed fields up to the group's block length.
        r.skip_to(entry_start + entry_block_length as usize)?;
        let contract = r.read_var_string()?;
        let text = r.read_var_string()?;
        let _amend_text = r.read_var_string()?;
        let _biz_info = r.read_var_string()?;
        trades.push(GateSbeUserTrade {
            trade_id: trade_id.to_string(),
            order_id: order_id.to_string(),
            size: mantissa_to_string(size_mantissa, sz_exponent),
            price: mantissa_to_string(price_mantissa, px_exponent),
            fee: mantissa_to_string(fee_mantissa, fee_exponent),
            role: role_name(role_code),
            contract,
            text,
        });
    }
    Ok(trades)
}

/// `mantissa * 10^exponent` rendered as a decimal string.
fn mantissa_to_string(mantissa: i64, exponent: i8) -> String {
    let value = if exponent >= 0 {
        Decimal::from(mantissa) * Decimal::from(10i64.pow(exponent as u32))
    } else {
        // Decimal::new(m, scale) == m * 10^-scale.
        Decimal::new(mantissa, u32::from(exponent.unsigned_abs()))
    };
    value.normalize().to_string()
}

fn role_name(code: i8) -> String {
    match code {
        0 => "maker".to_string(),
        1 => "taker".to_string(),
        other => other.to_string(),
    }
}

/// Little-endian cursor over an SBE frame.
struct SbeReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> SbeReader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn require(&self, size: usize) -> anyhow::Result<()> {
        if self.pos + size > self.buf.len() {
            anyhow::bail!("SBE frame truncated at offset {}", self.pos);
        }
        Ok(())
    }

    fn read_i8(&mut self) -> anyhow::Result<i8> {
        self.require(1)?;
        let v = self.buf[self.pos] as i8;
        self.pos += 1;
        Ok(v)
    }

    fn read_u8(&mut self) -> anyhow::Result<u8> {
        self.require(1)?;
        let v = self.buf[self.pos];
        self.pos += 1;
        Ok(v)
    }

    fn read_u16(&mut self) -> anyhow::Result<u16> {
        self.require(2)?;
        let v = u16::from_le_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    fn read_i64(&mut self) -> anyhow::Result<i64> {
        self.require(8)?;
        let mut b = [0u8; 8];
        b.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
        self.pos += 8;
        Ok(i64::from_le_bytes(b))
    }

    fn read_u64(&mut self) -> anyhow::Result<u64> {
        self.require(8)?;
        let mut b = [0u8; 8];
        b.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
        self.pos += 8;
        Ok(u64::from_le_bytes(b))
    }

    fn read_header(&mut self) -> anyhow::Result<(u16, u16, u16, u16)> {
        self.require(SBE_HEADER_SIZE)?;
        let block_length = self.read_u16()?;
        let template_id = self.read_u16()?;
        let schema_id = self.read_u16()?;
        let version = self.read_u16()?;
        Ok((block_length, template_id, schema_id, version))
    }

    fn read_group_header(&mut self) -> anyhow::Result<(u16, u16)> {
        let block_length = self.read_u16()?;
        let count = self.read_u16()?;
        Ok((block_length, count))
    }

    fn read_var_string(&mut self) -> anyhow::Result<String> {
        let len = self.read_u8()? as usize;
        self.require(len)?;
        let s = String::from_utf8_lossy(&self.buf[self.pos..self.pos + len]).into_owned();
        self.pos += len;
        Ok(s)
    }

    fn skip_to(&mut self, absolute_offset: usize) -> anyhow::Result<()> {
        if absolute_offset < self.pos {
            return Ok(());
        }
        if absolute_offset > self.buf.len() {
            anyhow::bail!("SBE group block exceeds frame size");
        }
        self.pos = absolute_offset;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a synthetic `userTrade` frame and round-trips it through the decoder
    /// to verify the byte offsets/layout (mirrors the reference decoder).
    #[test]
    fn decode_user_trade_roundtrip() {
        let mut f: Vec<u8> = Vec::new();
        // MessageHeader: block_length, template_id=6, schema_id=1, version=0.
        f.extend_from_slice(&13u16.to_le_bytes());
        f.extend_from_slice(&6u16.to_le_bytes());
        f.extend_from_slice(&1u16.to_le_bytes());
        f.extend_from_slice(&0u16.to_le_bytes());
        // Root: time_us, event=2, px_exp=-1, sz_exp=0, fee_exp=-8, point_exp=-8.
        f.extend_from_slice(&1_770_000_000_000_000i64.to_le_bytes());
        f.push(2i8 as u8);
        f.push((-1i8) as u8);
        f.push(0i8 as u8);
        f.push((-8i8) as u8);
        f.push((-8i8) as u8);
        // Group header: block_length=65, count=1.
        f.extend_from_slice(&65u16.to_le_bytes());
        f.extend_from_slice(&1u16.to_le_bytes());
        // Entry fixed (65 bytes): ids + mantissas + role.
        f.extend_from_slice(&768_085_496u64.to_le_bytes());
        f.extend_from_slice(&36_028_831_052_842_960u64.to_le_bytes());
        f.extend_from_slice(&1_770_000_000_000_000i64.to_le_bytes());
        f.extend_from_slice(&1i64.to_le_bytes()); // size mantissa, sz_exp=0 -> "1"
        f.extend_from_slice(&626_484i64.to_le_bytes()); // price, px_exp=-1 -> 62648.4
        f.extend_from_slice(&313_242i64.to_le_bytes()); // fee, fee_exp=-8 -> 0.00313242
        f.extend_from_slice(&0i64.to_le_bytes()); // point_fee
        f.extend_from_slice(&0i64.to_le_bytes()); // close_size
        f.push(1i8 as u8); // role=taker
        // (entry_block_length=65 == bytes written above; no skip needed)
        // Var strings: contract, text, amend_text, biz_info.
        for s in ["BTC_USDT", "t-abc", "-", "-"] {
            f.push(s.len() as u8);
            f.extend_from_slice(s.as_bytes());
        }

        let trades = decode_user_trades(&f).unwrap();
        assert_eq!(trades.len(), 1);
        let t = &trades[0];
        assert_eq!(t.trade_id, "768085496");
        assert_eq!(t.order_id, "36028831052842960");
        assert_eq!(t.size, "1");
        assert_eq!(t.price, "62648.4");
        assert_eq!(t.fee, "0.00313242");
        assert_eq!(t.role, "taker");
        assert_eq!(t.contract, "BTC_USDT");
    }

    #[test]
    fn non_user_trade_template_returns_empty() {
        let mut f: Vec<u8> = Vec::new();
        f.extend_from_slice(&0u16.to_le_bytes());
        f.extend_from_slice(&4u16.to_le_bytes()); // orderBook template
        f.extend_from_slice(&1u16.to_le_bytes());
        f.extend_from_slice(&0u16.to_le_bytes());
        assert!(decode_user_trades(&f).unwrap().is_empty());
    }

    #[test]
    fn mantissa_string() {
        assert_eq!(mantissa_to_string(626_484, -1), "62648.4");
        assert_eq!(mantissa_to_string(313_242, -8), "0.00313242");
        assert_eq!(mantissa_to_string(1, 0), "1");
        assert_eq!(mantissa_to_string(5, 2), "500");
    }
}
