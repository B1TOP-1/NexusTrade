use std::time::Instant;

use bybot_hype::markets::MarketCatalog;
use hypersdk::hypercore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let started = Instant::now();
    let client = hypercore::mainnet();
    let catalog = MarketCatalog::load_selected(&client, &["xyz"]).await?;
    let btc = catalog
        .get("BTC")
        .ok_or_else(|| anyhow::anyhow!("BTC missing"))?;
    let spcx = catalog
        .get("xyz:SPCX")
        .ok_or_else(|| anyhow::anyhow!("SPCX missing"))?;
    println!(
        "markets_ready elapsed_ms={} btc_asset={} spcx_asset={} spcx_step={}",
        started.elapsed().as_millis(),
        btc.market().index,
        spcx.market().index,
        spcx.precision().size_step()
    );
    Ok(())
}
