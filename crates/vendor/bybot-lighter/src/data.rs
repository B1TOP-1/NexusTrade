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

use std::str::FromStr;

use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LighterBookMessageKind {
    Snapshot,
    Update,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LighterBookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterPriceLevel {
    pub price: Decimal,
    pub size: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterOrderBookMessage {
    pub kind: LighterBookMessageKind,
    pub channel: Option<String>,
    pub begin_nonce: Option<u64>,
    pub nonce: u64,
    pub bids: Vec<LighterPriceLevel>,
    pub asks: Vec<LighterPriceLevel>,
    pub ts_event_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterMarketSpec {
    pub symbol: String,
    pub market_id: u64,
    pub min_base_amount: Decimal,
    pub supported_size_decimals: u8,
    pub supported_price_decimals: u8,
    pub size_multiplier: u64,
    pub price_multiplier: u64,
}

pub fn parse_lighter_market_specs(payload: &str) -> Result<Vec<LighterMarketSpec>> {
    let root: Value = serde_json::from_str(payload).context("invalid Lighter markets JSON")?;
    let markets = root
        .get("order_books")
        .and_then(Value::as_array)
        .context("Lighter markets response missing order_books")?;
    markets
        .iter()
        .filter(|market| {
            string_field(market, &["status"]).as_deref() == Some("active")
                && string_field(market, &["market_type"]).as_deref() == Some("perp")
        })
        .map(parse_market_spec)
        .collect()
}

pub fn parse_order_book_message(payload: &str) -> Result<Option<LighterOrderBookMessage>> {
    let root: Value = serde_json::from_str(payload).context("invalid Lighter WebSocket JSON")?;
    let message_type = string_field(&root, &["type"]);
    let kind = match message_type.as_deref() {
        Some("subscribed/order_book") => LighterBookMessageKind::Snapshot,
        Some("update/order_book") => LighterBookMessageKind::Update,
        _ => return Ok(None),
    };
    let book = root
        .get("order_book")
        .or_else(|| root.get("data").and_then(|data| data.get("order_book")))
        .context("Lighter order-book message missing order_book body")?;
    let nonce = u64_field(book, &["nonce", "last_nonce", "offset"])?
        .context("Lighter order-book message missing nonce")?;
    let begin_nonce = u64_field(book, &["begin_nonce"])?;
    if kind == LighterBookMessageKind::Update && begin_nonce.is_none() {
        bail!("Lighter order-book update missing begin_nonce");
    }

    Ok(Some(LighterOrderBookMessage {
        kind,
        channel: string_field(&root, &["channel"]),
        begin_nonce,
        nonce,
        bids: parse_levels(book.get("bids"), LighterBookSide::Bid)?,
        asks: parse_levels(book.get("asks"), LighterBookSide::Ask)?,
        ts_event_ms: u64_field(book, &["timestamp", "transaction_time", "ts", "time"])?
            .or(u64_field(
                &root,
                &["timestamp", "transaction_time", "ts", "time"],
            )?)
            .map(normalize_epoch_millis),
    }))
}

fn parse_market_spec(value: &Value) -> Result<LighterMarketSpec> {
    let symbol = string_field(value, &["symbol"])
        .context("Lighter market missing symbol")?
        .trim()
        .to_uppercase();
    if symbol.is_empty() {
        bail!("Lighter market symbol must not be empty");
    }
    let market_id =
        u64_field(value, &["market_id"])?.context("Lighter market missing market_id")?;
    let size_decimals = u8::try_from(
        u64_field(value, &["supported_size_decimals"])?
            .context("Lighter market missing supported_size_decimals")?,
    )
    .context("Lighter size decimals exceed u8")?;
    let price_decimals = u8::try_from(
        u64_field(value, &["supported_price_decimals"])?
            .context("Lighter market missing supported_price_decimals")?,
    )
    .context("Lighter price decimals exceed u8")?;
    let min_base_amount = decimal_value(value.get("min_base_amount"), "min_base_amount")?;
    if min_base_amount <= Decimal::ZERO {
        bail!("Lighter market min_base_amount must be positive");
    }
    Ok(LighterMarketSpec {
        symbol,
        market_id,
        min_base_amount,
        supported_size_decimals: size_decimals,
        supported_price_decimals: price_decimals,
        size_multiplier: checked_pow10(size_decimals, "size")?,
        price_multiplier: checked_pow10(price_decimals, "price")?,
    })
}

fn checked_pow10(exponent: u8, field: &str) -> Result<u64> {
    10_u64
        .checked_pow(u32::from(exponent))
        .ok_or_else(|| anyhow::anyhow!("Lighter {field} multiplier overflow"))
}

fn parse_levels(value: Option<&Value>, side: LighterBookSide) -> Result<Vec<LighterPriceLevel>> {
    let values = value
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("Lighter {side:?} levels must be an array"))?;
    values
        .iter()
        .map(|value| parse_level(value, side))
        .collect()
}

fn parse_level(value: &Value, side: LighterBookSide) -> Result<LighterPriceLevel> {
    let (price, size) = if let Some(items) = value.as_array() {
        (items.first(), items.get(1))
    } else {
        (
            value.get("p").or_else(|| value.get("price")),
            value.get("s").or_else(|| value.get("size")),
        )
    };
    let price = decimal_value(price, "price")?;
    let size = decimal_value(size, "size")?;
    if price <= Decimal::ZERO {
        bail!("Lighter {side:?} level price must be positive");
    }
    if size < Decimal::ZERO {
        bail!("Lighter {side:?} level size must not be negative");
    }
    Ok(LighterPriceLevel { price, size })
}

fn decimal_value(value: Option<&Value>, field: &str) -> Result<Decimal> {
    let value = value.ok_or_else(|| anyhow::anyhow!("missing Lighter level {field}"))?;
    let text = match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        _ => bail!("invalid Lighter level {field}"),
    };
    Decimal::from_str(&text).with_context(|| format!("invalid Lighter level {field}: {text}"))
}

fn string_field(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        value.get(key).and_then(|field| match field {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        })
    })
}

fn u64_field(value: &Value, keys: &[&str]) -> Result<Option<u64>> {
    keys.iter()
        .find_map(|key| value.get(key).map(parse_u64))
        .transpose()
}

fn parse_u64(value: &Value) -> Result<u64> {
    match value {
        Value::Number(number) => number
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("invalid unsigned Lighter value: {number}")),
        Value::String(text) => text
            .parse::<u64>()
            .with_context(|| format!("invalid unsigned Lighter value: {text}")),
        _ => bail!("invalid unsigned Lighter field type"),
    }
}

fn normalize_epoch_millis(value: u64) -> u64 {
    const MICROSECOND_EPOCH: u64 = 1_000_000_000_000_000;
    const NANOSECOND_EPOCH: u64 = 1_000_000_000_000_000_000;
    if value >= NANOSECOND_EPOCH {
        value / 1_000_000
    } else if value >= MICROSECOND_EPOCH {
        value / 1_000
    } else {
        value
    }
}
