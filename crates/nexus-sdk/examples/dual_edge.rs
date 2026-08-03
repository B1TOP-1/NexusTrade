//! 双所对称 Edge 演示：Hyperliquid + Lighter 行情，无需私钥。
//!
//! 用法（symbol 默认 BTC）：
//! `cargo run --example dual_edge -p nexus-sdk -- ETH`
//!
//! 每秒打印 vwap(2000 USD) 口径的对称 Edge，Ctrl-C 退出：
//! - long_edge  = 200*(L_bid − H_ask)/(L_bid + H_ask)（HYPE 买入、LIGHTER 卖出）
//! - short_edge = 200*(H_bid − L_ask)/(H_bid + L_ask)（LIGHTER 买入、HYPE 卖出）

use std::sync::Arc;
use std::time::Duration;

use nexus_sdk::{BookOptions, Decimal, HypeMarket, LighterMarket, Nexus, Symbol, VenueId};

/// 对称 Edge：200*(卖侧 bid − 买侧 ask)/(卖侧 bid + 买侧 ask)。
fn edge(sell_bid: Decimal, buy_ask: Decimal) -> Decimal {
    let denom = sell_bid + buy_ask;
    if denom <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    (Decimal::from(200) * (sell_bid - buy_ask) / denom).round_dp(4)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = std::env::args().nth(1).unwrap_or_else(|| "BTC".to_string());
    let sym = Symbol::new(base.clone(), "USD", base.clone());
    let notional = Decimal::from(2000);
    let opts = BookOptions {
        vwap_notional: Some(notional),
        ..Default::default()
    };

    let nexus = Nexus::builder()
        .market(VenueId::HYPE, Arc::new(HypeMarket::connect_mainnet().await?))
        .market(VenueId::LIGHTER, Arc::new(LighterMarket::connect_mainnet().await?))
        .build();

    let hype = nexus.book(VenueId::HYPE, &sym, opts).await?;
    let lighter = nexus.book(VenueId::LIGHTER, &sym, opts).await?;
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    println!("dual edge on {base} (vwap {notional} USD), Ctrl-C to exit");
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = tick.tick() => match (hype.vwap(notional), lighter.vwap(notional)) {
                (Some((h_bid, h_ask)), Some((l_bid, l_ask))) => println!(
                    "[{base}] long_edge={} short_edge={}",
                    edge(l_bid, h_ask),
                    edge(h_bid, l_ask),
                ),
                _ => println!("[{base}] book not ready or depth insufficient, skip"),
            },
        }
    }
    Ok(())
}
