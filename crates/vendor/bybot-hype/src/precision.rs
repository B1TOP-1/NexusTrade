use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketPrecision {
    size_decimals: u32,
}

impl MarketPrecision {
    pub fn new(size_decimals: i64) -> Result<Self> {
        let size_decimals = u32::try_from(size_decimals).context("negative size decimals")?;
        if size_decimals > 18 {
            bail!("size decimals exceed Decimal precision: {size_decimals}");
        }
        Ok(Self { size_decimals })
    }

    #[must_use]
    pub fn size_step(self) -> Decimal {
        Decimal::new(1, self.size_decimals)
    }

    pub fn minimum_size(self, price: Decimal, minimum_notional: Decimal) -> Result<Decimal> {
        if price <= Decimal::ZERO {
            bail!("price must be positive");
        }
        if minimum_notional <= Decimal::ZERO {
            bail!("minimum notional must be positive");
        }
        let step = self.size_step();
        let units = (minimum_notional / price / step).ceil();
        Ok(units.max(Decimal::ONE) * step)
    }

    pub fn floor_size(self, size: Decimal) -> Result<Decimal> {
        if size < Decimal::ZERO {
            bail!("size cannot be negative");
        }
        let step = self.size_step();
        Ok((size / step).floor() * step)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_floor_size_uses_market_step() {
        let precision = MarketPrecision::new(3).unwrap();
        assert_eq!(
            precision.floor_size(Decimal::new(12_345, 4)).unwrap(),
            Decimal::new(1_234, 3)
        );
    }

    #[test]
    fn test_precision_rejects_invalid_inputs() {
        assert!(MarketPrecision::new(-1).is_err());
        let precision = MarketPrecision::new(2).unwrap();
        assert!(precision.minimum_size(Decimal::ZERO, Decimal::ONE).is_err());
    }
}
