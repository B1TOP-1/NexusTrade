use std::{env, time::Duration};

use bybot_hype::public_ws::{fixed_to_string, monitor_books, MonitorConfig};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("Hype live book test failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let seconds = env::args()
        .nth(1)
        .map(|value| value.parse::<u64>().map_err(|error| error.to_string()))
        .transpose()?
        .unwrap_or(60);
    let symbols = vec!["BTC".to_string(), "xyz:SPCX".to_string()];
    let mut config = match env::args().nth(2).as_deref() {
        Some("fast") => MonitorConfig::mainnet_fast(symbols, Duration::from_secs(seconds)),
        _ => MonitorConfig::mainnet(symbols, Duration::from_secs(seconds)),
    };
    if seconds >= 300 {
        config.report_interval = Duration::from_secs(300);
    }
    let report = monitor_books(config).await?;

    println!(
        "[HypeBookFinal] elapsed_ms={} connections={} disconnections={} pongs={}",
        report.elapsed.as_millis(),
        report.connections,
        report.disconnections,
        report.application_pongs,
    );
    for market in report.markets {
        let bid = market
            .best_bid
            .map(|level| fixed_to_string(level.price()))
            .unwrap_or_else(|| "-".to_string());
        let ask = market
            .best_ask
            .map(|level| fixed_to_string(level.price()))
            .unwrap_or_else(|| "-".to_string());
        println!(
            "[HypeBookFinal] symbol={} state={:?} updates={} rejected={} latency_ms[min={:?},avg={:?},max={:?}] bid={} ask={} depth={}/{}",
            market.symbol,
            market.state,
            market.updates,
            market.rejected,
            market.min_latency_ms,
            market.average_latency_ms,
            market.max_latency_ms,
            bid,
            ask,
            market.bid_levels,
            market.ask_levels,
        );
    }
    Ok(())
}
