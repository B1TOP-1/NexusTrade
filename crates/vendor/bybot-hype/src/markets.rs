use std::collections::HashMap;

use anyhow::{anyhow, Result};
use hypersdk::hypercore::{self, types::Side, PerpMarket};
use rust_decimal::Decimal;
use tokio::sync::OnceCell;

use crate::orders::OrderSide;
use crate::precision::MarketPrecision;

#[derive(Debug, Clone)]
pub struct MarketDescriptor {
    market: PerpMarket,
    dex: Option<String>,
    precision: MarketPrecision,
}

impl MarketDescriptor {
    pub fn new(market: PerpMarket, dex: Option<String>) -> Result<Self> {
        let precision = MarketPrecision::new(market.sz_decimals)?;
        Ok(Self {
            market,
            dex,
            precision,
        })
    }

    #[must_use]
    pub fn market(&self) -> &PerpMarket {
        &self.market
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.market.name
    }

    #[must_use]
    pub fn dex(&self) -> Option<&str> {
        self.dex.as_deref()
    }

    #[must_use]
    pub fn precision(&self) -> MarketPrecision {
        self.precision
    }

    pub fn aggressive_price(
        &self,
        reference: Decimal,
        is_buy: bool,
        slippage_bps: i64,
    ) -> Result<Decimal> {
        let adjustment = Decimal::from(slippage_bps) / Decimal::from(10_000_i64);
        let raw = if is_buy {
            reference * (Decimal::ONE + adjustment)
        } else {
            reference * (Decimal::ONE - adjustment)
        };
        self.market
            .round_by_side(if is_buy { Side::Bid } else { Side::Ask }, raw, false)
            .ok_or_else(|| anyhow!("unable to round price for {}", self.symbol()))
    }

    pub fn maker_price(&self, reference: Decimal, side: OrderSide) -> Result<Decimal> {
        self.market
            .round_by_side(
                if side == OrderSide::Buy {
                    Side::Bid
                } else {
                    Side::Ask
                },
                reference,
                true,
            )
            .ok_or_else(|| anyhow!("unable to round maker price for {}", self.symbol()))
    }
}

#[derive(Debug, Clone)]
pub struct MarketCatalog {
    markets: HashMap<String, MarketDescriptor>,
}

#[derive(Debug, Clone)]
pub struct HyperliquidSymbolCatalog {
    symbols: Vec<String>,
}

static MAINNET_SYMBOL_CATALOG: OnceCell<HyperliquidSymbolCatalog> = OnceCell::const_new();

impl HyperliquidSymbolCatalog {
    pub async fn load_mainnet() -> Result<Self> {
        let payload = hypercore::mainnet().all_perp_metas().await?;
        Self::from_all_perp_metas(&payload)
    }

    pub async fn mainnet_cached() -> Result<&'static Self> {
        MAINNET_SYMBOL_CATALOG
            .get_or_try_init(Self::load_mainnet)
            .await
    }

    pub fn from_all_perp_metas(payload: &serde_json::Value) -> Result<Self> {
        let metas = payload
            .as_array()
            .ok_or_else(|| anyhow!("allPerpMetas response must be an array"))?;
        let mut symbols = Vec::new();
        for meta in metas {
            let universe = meta
                .get("universe")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| anyhow!("allPerpMetas entry is missing universe"))?;
            for market in universe {
                let symbol = market
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| anyhow!("allPerpMetas market is missing name"))?;
                symbols.push(normalize_hyperliquid_symbol(symbol)?);
            }
        }
        symbols.sort();
        symbols.dedup();
        Ok(Self { symbols })
    }

    pub fn resolve(&self, requested: &str) -> Result<String> {
        let requested = normalize_hyperliquid_symbol(requested)?;
        if let Some(symbol) = self
            .symbols
            .iter()
            .find(|symbol| symbol.eq_ignore_ascii_case(&requested))
        {
            return Ok(symbol.clone());
        }
        let matches = self
            .symbols
            .iter()
            .filter(|symbol| {
                symbol
                    .rsplit_once(':')
                    .is_some_and(|(_, suffix)| suffix.eq_ignore_ascii_case(&requested))
            })
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [symbol] => Ok((*symbol).clone()),
            [] => Err(anyhow!("Hyperliquid market not found: {requested}")),
            _ => Err(anyhow!(
                "Hyperliquid market is ambiguous: {requested}; candidates: {}",
                matches
                    .iter()
                    .map(|symbol| symbol.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        }
    }
}

fn normalize_hyperliquid_symbol(symbol: &str) -> Result<String> {
    let symbol = symbol.trim();
    if symbol.is_empty() {
        return Err(anyhow!("Hyperliquid market symbol cannot be empty"));
    }
    if let Some((dex, asset)) = symbol.split_once(':') {
        if dex.trim().is_empty() || asset.trim().is_empty() {
            return Err(anyhow!("invalid Hyperliquid HIP-3 market symbol: {symbol}"));
        }
        return Ok(format!(
            "{}:{}",
            dex.trim().to_lowercase(),
            asset.trim().to_uppercase()
        ));
    }
    Ok(symbol.to_uppercase())
}

impl MarketCatalog {
    pub async fn load(client: &hypercore::HttpClient) -> Result<Self> {
        let dexes = client.perp_dexes().await?;
        let names = dexes.iter().map(|dex| dex.name()).collect::<Vec<_>>();
        Self::load_selected(client, &names).await
    }

    pub async fn load_selected(client: &hypercore::HttpClient, dex_names: &[&str]) -> Result<Self> {
        let mut markets = HashMap::new();
        let (main_markets, dexes) = tokio::try_join!(client.perps(), client.perp_dexes())?;
        for market in main_markets {
            let descriptor = MarketDescriptor::new(market, None)?;
            markets.insert(descriptor.symbol().to_string(), descriptor);
        }
        for requested_name in dex_names {
            let dex = dexes
                .iter()
                .find(|dex| dex.name().eq_ignore_ascii_case(requested_name))
                .cloned()
                .ok_or_else(|| anyhow!("perp dex not found: {requested_name}"))?;
            let dex_name = dex.name().to_string();
            for market in client.perps_from(dex).await? {
                let descriptor = MarketDescriptor::new(market, Some(dex_name.clone()))?;
                markets.insert(descriptor.symbol().to_string(), descriptor);
            }
        }
        Ok(Self { markets })
    }

    #[must_use]
    pub fn get(&self, symbol: &str) -> Option<&MarketDescriptor> {
        self.markets.get(symbol).or_else(|| {
            self.markets.values().find(|market| {
                market
                    .symbol()
                    .rsplit(':')
                    .next()
                    .is_some_and(|suffix| suffix == symbol)
            })
        })
    }

    #[must_use]
    pub fn symbols(&self) -> Vec<&str> {
        let mut symbols = self.markets.keys().map(String::as_str).collect::<Vec<_>>();
        symbols.sort_unstable();
        symbols
    }

    pub async fn mids(
        &self,
        client: &hypercore::HttpClient,
        dex: Option<&str>,
    ) -> Result<HashMap<String, Decimal>> {
        client.all_mids(dex.map(str::to_string)).await
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::HyperliquidSymbolCatalog;

    #[test]
    fn test_symbol_catalog_resolves_unique_hip3_suffix() {
        let payload = json!([
            {"universe": [{"name": "BTC"}, {"name": "ETH"}]},
            {"universe": [{"name": "xyz:CL"}, {"name": "xyz:SPCX"}]}
        ]);
        let catalog = HyperliquidSymbolCatalog::from_all_perp_metas(&payload).unwrap();

        assert_eq!(catalog.resolve("CL").unwrap(), "xyz:CL");
    }

    #[test]
    fn test_symbol_catalog_prefers_exact_main_market() {
        let payload = json!([
            {"universe": [{"name": "BTC"}]},
            {"universe": [{"name": "xyz:BTC"}]}
        ]);
        let catalog = HyperliquidSymbolCatalog::from_all_perp_metas(&payload).unwrap();

        assert_eq!(catalog.resolve("btc").unwrap(), "BTC");
    }

    #[test]
    fn test_symbol_catalog_normalizes_explicit_hip3_market() {
        let payload = json!([
            {"universe": [{"name": "BTC"}]},
            {"universe": [{"name": "xyz:CL"}]}
        ]);
        let catalog = HyperliquidSymbolCatalog::from_all_perp_metas(&payload).unwrap();

        assert_eq!(catalog.resolve("XYZ:cl").unwrap(), "xyz:CL");
    }

    #[test]
    fn test_symbol_catalog_reports_ambiguous_hip3_candidates() {
        let payload = json!([
            {"universe": [{"name": "BTC"}]},
            {"universe": [{"name": "abc:CL"}]},
            {"universe": [{"name": "xyz:CL"}]}
        ]);
        let catalog = HyperliquidSymbolCatalog::from_all_perp_metas(&payload).unwrap();

        let error = catalog.resolve("CL").unwrap_err().to_string();
        assert!(error.contains("abc:CL"));
        assert!(error.contains("xyz:CL"));
    }
}
