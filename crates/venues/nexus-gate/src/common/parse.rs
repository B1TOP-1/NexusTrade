use anyhow::Context;
use nautilus_core::UnixNanos;
use nautilus_model::types::{Price, Quantity};
use std::str::FromStr;

pub fn parse_millis_timestamp(value: i64, label: &str) -> anyhow::Result<UnixNanos> {
    let millis = u64::try_from(value).with_context(|| format!("negative {label} timestamp"))?;
    Ok(UnixNanos::from(millis * 1_000_000))
}

pub fn parse_price(value: &str, precision: u8, label: &str) -> anyhow::Result<Price> {
    let price = Price::from_str(value)
        .map_err(|e| anyhow::anyhow!("invalid {label} price: {value}: {e}"))?;
    Price::from_decimal_dp(price.as_decimal(), precision)
        .with_context(|| format!("invalid {label} price precision: {value}"))
}

pub fn parse_quantity(value: &str, precision: u8, label: &str) -> anyhow::Result<Quantity> {
    let quantity = Quantity::from_str(value)
        .map_err(|e| anyhow::anyhow!("invalid {label} amount: {value}: {e}"))?;
    Quantity::from_decimal_dp(quantity.as_decimal(), precision)
        .with_context(|| format!("invalid {label} amount precision: {value}"))
}

pub fn parse_level(
    level: &[String],
    price_precision: u8,
    size_precision: u8,
    label: &str,
) -> anyhow::Result<(Price, Quantity)> {
    let price = level
        .first()
        .ok_or_else(|| anyhow::anyhow!("missing price component in {label} level"))?;
    let amount = level
        .get(1)
        .ok_or_else(|| anyhow::anyhow!("missing amount component in {label} level"))?;
    Ok((
        parse_price(price, price_precision, label)?,
        parse_quantity(amount, size_precision, label)?,
    ))
}
