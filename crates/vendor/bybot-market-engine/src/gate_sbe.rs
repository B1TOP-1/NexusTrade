use crate::model::Level;

pub const ORDER_BOOK_UPDATE_TEMPLATE_ID: u16 = 5;
pub const OBU_TEMPLATE_ID: u16 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateBookUpdate {
    pub full: bool,
    pub first_id: u64,
    pub last_id: u64,
    pub symbol: String,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub time_us: i64,
    pub exchange_time_us: i64,
}

pub fn decode_order_book_update(frame: &[u8]) -> Result<GateBookUpdate, String> {
    let mut reader = SbeReader::new(frame);
    let _block_length = reader.read_u16()?;
    let template_id = reader.read_u16()?;
    let _schema_id = reader.read_u16()?;
    let _version = reader.read_u16()?;
    if template_id == OBU_TEMPLATE_ID {
        return decode_obu_update(reader);
    }
    if template_id != ORDER_BOOK_UPDATE_TEMPLATE_ID {
        return Err(format!("unsupported Gate SBE template: {template_id}"));
    }

    let time_us = reader.read_i64()?;
    let _event = reader.read_u8()?;
    let exchange_time_us = reader.read_i64()?;
    let first_id = reader.read_i64()?;
    let last_id = reader.read_i64()?;
    let px_exponent = reader.read_i8()?;
    let sz_exponent = reader.read_i8()?;
    let _level = reader.read_u8()?;
    let asks = reader.read_level_group(px_exponent, sz_exponent)?;
    let bids = reader.read_level_group(px_exponent, sz_exponent)?;
    let _channel = reader.read_var_string()?;
    let symbol = reader.read_var_string()?;

    if first_id < 0 || last_id < 0 {
        return Err("negative order book update id".to_string());
    }
    Ok(GateBookUpdate {
        full: false,
        first_id: first_id as u64,
        last_id: last_id as u64,
        symbol,
        bids,
        asks,
        time_us,
        exchange_time_us,
    })
}

fn decode_obu_update(mut reader: SbeReader<'_>) -> Result<GateBookUpdate, String> {
    let time_us = reader.read_i64()?;
    let _event = reader.read_u8()?;
    let exchange_time_us = reader.read_i64()?;
    let full = reader.read_u8()? != 0;
    let first_id = reader.read_i64()?;
    let last_id = reader.read_i64()?;
    let px_exponent = reader.read_i8()?;
    let sz_exponent = reader.read_i8()?;
    let bids = reader.read_level_group(px_exponent, sz_exponent)?;
    let asks = reader.read_level_group(px_exponent, sz_exponent)?;
    let _channel = reader.read_var_string()?;
    let symbol = reader.read_var_string()?;

    if first_id < 0 || last_id < 0 {
        return Err("negative order book update id".to_string());
    }
    Ok(GateBookUpdate {
        full,
        first_id: first_id as u64,
        last_id: last_id as u64,
        symbol,
        bids,
        asks,
        time_us,
        exchange_time_us,
    })
}

struct SbeReader<'a> {
    frame: &'a [u8],
    offset: usize,
}

impl<'a> SbeReader<'a> {
    fn new(frame: &'a [u8]) -> Self {
        Self { frame, offset: 0 }
    }

    fn require(&self, len: usize) -> Result<(), String> {
        if self.offset + len > self.frame.len() {
            Err("SBE frame truncated".to_string())
        } else {
            Ok(())
        }
    }

    fn read_u8(&mut self) -> Result<u8, String> {
        self.require(1)?;
        let value = self.frame[self.offset];
        self.offset += 1;
        Ok(value)
    }

    fn read_i8(&mut self) -> Result<i8, String> {
        Ok(self.read_u8()? as i8)
    }

    fn read_u16(&mut self) -> Result<u16, String> {
        self.require(2)?;
        let value = u16::from_le_bytes([self.frame[self.offset], self.frame[self.offset + 1]]);
        self.offset += 2;
        Ok(value)
    }

    fn read_i64(&mut self) -> Result<i64, String> {
        self.require(8)?;
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&self.frame[self.offset..self.offset + 8]);
        self.offset += 8;
        Ok(i64::from_le_bytes(bytes))
    }

    fn read_var_string(&mut self) -> Result<String, String> {
        let len = self.read_u8()? as usize;
        self.require(len)?;
        let raw = &self.frame[self.offset..self.offset + len];
        self.offset += len;
        std::str::from_utf8(raw)
            .map(|value| value.to_string())
            .map_err(|err| err.to_string())
    }

    fn read_level_group(&mut self, px_exponent: i8, sz_exponent: i8) -> Result<Vec<Level>, String> {
        let block_length = self.read_u16()? as usize;
        let count = self.read_u16()? as usize;
        let mut levels = Vec::with_capacity(count);
        for _ in 0..count {
            let entry_start = self.offset;
            let price_mantissa = self.read_i64()?;
            let size_mantissa = self.read_i64()?;
            levels.push(Level {
                price: mantissa_to_scaled(price_mantissa, px_exponent)?,
                size: mantissa_to_scaled(size_mantissa, sz_exponent)?,
            });
            let entry_end = entry_start + block_length;
            if entry_end < self.offset || entry_end > self.frame.len() {
                return Err("SBE group block exceeds frame size".to_string());
            }
            self.offset = entry_end;
        }
        Ok(levels)
    }
}

fn mantissa_to_scaled(mantissa: i64, exponent: i8) -> Result<i64, String> {
    let scale_power = 8 + i32::from(exponent);
    if scale_power >= 0 {
        let factor = 10i64
            .checked_pow(scale_power as u32)
            .ok_or_else(|| "scale factor overflow".to_string())?;
        mantissa
            .checked_mul(factor)
            .ok_or_else(|| "scaled mantissa overflow".to_string())
    } else {
        let divisor = 10i64
            .checked_pow((-scale_power) as u32)
            .ok_or_else(|| "scale divisor overflow".to_string())?;
        Ok(mantissa / divisor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(template_id: u16, block_length: u16, body: Vec<u8>) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&block_length.to_le_bytes());
        out.extend_from_slice(&template_id.to_le_bytes());
        out.extend_from_slice(&1u16.to_le_bytes());
        out.extend_from_slice(&1u16.to_le_bytes());
        out.extend_from_slice(&body);
        out
    }

    fn var_string(value: &str) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(value.len() as u8);
        out.extend_from_slice(value.as_bytes());
        out
    }

    fn book_update_frame(first_id: i64, last_id: i64) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&1_770_000_000_000_001i64.to_le_bytes());
        body.push(2u8);
        body.extend_from_slice(&1_770_000_000_000_000i64.to_le_bytes());
        body.extend_from_slice(&first_id.to_le_bytes());
        body.extend_from_slice(&last_id.to_le_bytes());
        body.push((-1i8) as u8);
        body.push((-4i8) as u8);
        body.push(20u8);
        body.extend_from_slice(&16u16.to_le_bytes());
        body.extend_from_slice(&1u16.to_le_bytes());
        body.extend_from_slice(&809450i64.to_le_bytes());
        body.extend_from_slice(&23i64.to_le_bytes());
        body.extend_from_slice(&16u16.to_le_bytes());
        body.extend_from_slice(&1u16.to_le_bytes());
        body.extend_from_slice(&809445i64.to_le_bytes());
        body.extend_from_slice(&17i64.to_le_bytes());
        body.extend_from_slice(&var_string("futures.order_book_update"));
        body.extend_from_slice(&var_string("BTC_USDT"));
        frame(ORDER_BOOK_UPDATE_TEMPLATE_ID, 36, body)
    }

    fn obu_frame(first_id: i64, last_id: i64) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&1_770_000_000_000_001i64.to_le_bytes());
        body.push(2u8);
        body.extend_from_slice(&1_770_000_000_000_000i64.to_le_bytes());
        body.push(0u8);
        body.extend_from_slice(&first_id.to_le_bytes());
        body.extend_from_slice(&last_id.to_le_bytes());
        body.push((-1i8) as u8);
        body.push((-4i8) as u8);
        body.extend_from_slice(&16u16.to_le_bytes());
        body.extend_from_slice(&1u16.to_le_bytes());
        body.extend_from_slice(&809445i64.to_le_bytes());
        body.extend_from_slice(&17i64.to_le_bytes());
        body.extend_from_slice(&16u16.to_le_bytes());
        body.extend_from_slice(&1u16.to_le_bytes());
        body.extend_from_slice(&809450i64.to_le_bytes());
        body.extend_from_slice(&23i64.to_le_bytes());
        body.extend_from_slice(&var_string("futures.obu"));
        body.extend_from_slice(&var_string("BTC_USDT"));
        frame(OBU_TEMPLATE_ID, 36, body)
    }

    #[test]
    fn decodes_order_book_update_frame() {
        let update = decode_order_book_update(&book_update_frame(101, 102)).unwrap();

        assert_eq!(update.first_id, 101);
        assert_eq!(update.last_id, 102);
        assert_eq!(update.symbol, "BTC_USDT");
        assert_eq!(update.asks[0], Level { price: 80945_00000000, size: 23_0000 });
        assert_eq!(update.bids[0], Level { price: 80944_50000000, size: 17_0000 });
    }

    #[test]
    fn decodes_obu_frame() {
        let update = decode_order_book_update(&obu_frame(101, 102)).unwrap();

        assert!(!update.full);
        assert_eq!(update.first_id, 101);
        assert_eq!(update.last_id, 102);
        assert_eq!(update.symbol, "BTC_USDT");
        assert_eq!(update.asks[0], Level { price: 80945_00000000, size: 23_0000 });
        assert_eq!(update.bids[0], Level { price: 80944_50000000, size: 17_0000 });
    }

    #[test]
    fn rejects_truncated_frame() {
        assert!(decode_order_book_update(&[1, 2, 3]).is_err());
    }

    #[test]
    fn decodes_obu_full_flag() {
        let mut frame = obu_frame(0, 102);
        frame[8 + 8 + 1 + 8] = 1;

        let update = decode_order_book_update(&frame).unwrap();

        assert!(update.full);
        assert_eq!(update.first_id, 0);
        assert_eq!(update.last_id, 102);
    }
}
