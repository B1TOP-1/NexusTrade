//! Binance Futures 本地订单簿集成测试。
//!
//! 用法：
//! ```bash
//! cargo run --example live_book -- -p nexus-binance             # BTCUSDT, 60s
//! cargo run --example live_book -- -p nexus-binance -- ETHUSDT 300   # ETHUSDT, 5min
//! cargo run --example live_book -- -p nexus-binance -- --testnet BTCUSDT 30
//! ```
//!
//! 每秒打印 Best Bid/Ask + Spread + 更新计数，Ctrl-C 或超时后输出最终深度和统计。

use std::io::{self, Write};
use std::time::{Duration, Instant};

use nexus_binance::BinanceMarket;
use nexus_core::{BookOptions, Decimal, MarketVenue, Symbol};

fn parse_args() -> (String, u64, bool) {
    let args: Vec<String> = std::env::args().collect();

    let symbol = args
        .iter()
        .position(|a| a == "--")
        .and_then(|i| args.get(i + 1).cloned())
        .or_else(|| {
            args.iter()
                .find(|a| a.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()))
                .cloned()
        })
        .unwrap_or_else(|| "BTCUSDT".to_string());

    let duration = args
        .iter()
        .filter(|a| a.chars().all(|c| c.is_ascii_digit()))
        .last()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(300); // 5 minutes default

    let testnet = args.iter().any(|a| a == "--testnet");

    (symbol, duration, testnet)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (symbol_str, duration_secs, testnet) = parse_args();

    println!("{}", "=".repeat(64));
    println!("  Binance Futures Local Order Book (Rust)");
    println!("  Symbol:  {symbol_str}");
    println!("  Network: {}", if testnet { "TESTNET" } else { "MAINNET" });
    println!("  Runtime: {duration_secs}s");
    println!("  Stream:  @depth@100ms (fastest official)");
    println!("{}", "=".repeat(64));

    // 连接
    let market = if testnet {
        BinanceMarket::connect_testnet().await?
    } else {
        BinanceMarket::connect_mainnet().await?
    };

    let symbol = Symbol {
        base: symbol_str.replace("USDT", ""),
        quote: "USDT".to_string(),
        venue_native: symbol_str.clone(),
    };

    println!("\nConnecting to Binance WebSocket...");

    // 订阅（fastest=true → 100ms）
    let opts = BookOptions {
        fastest: true,
        ..Default::default()
    };
    let book = market.subscribe_book(&symbol, opts).await?;

    // 等待簿就绪
    println!("Waiting for order book snapshot...");
    let t0 = Instant::now();
    loop {
        if book.top().is_some() {
            let (bid, ask) = (book.top().unwrap().bid, book.top().unwrap().ask);
            println!("  Book ready! bid={bid} ask={ask}");
            break;
        }
        if t0.elapsed() > Duration::from_secs(30) {
            eprintln!("Timeout waiting for snapshot.");
            return Err("snapshot timeout".into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    println!();

    // 主循环
    let start = Instant::now();
    let mut last_seq = book.seq();
    let mut last_update_count: u64 = 0;

    loop {
        let elapsed = start.elapsed();
        if elapsed >= Duration::from_secs(duration_secs) {
            break;
        }

        if let Some(top) = book.top() {
            let spread = top.ask - top.bid;
            let spread_pct = if top.bid > Decimal::ZERO {
                (spread / top.bid * Decimal::from(100))
                    .round_dp(4)
            } else {
                Decimal::ZERO
            };
            let cur_seq = book.seq();
            let updates = cur_seq.saturating_sub(last_seq);
            last_seq = cur_seq;
            last_update_count += updates;

            let ts = format!(
                "{}:{}:{}",
                elapsed.as_secs() / 60,
                (elapsed.as_secs() % 60) / 10,
                elapsed.as_secs() % 10
            );

            // 事件时间戳：E = 网关吐出, T = 撮合；local-E = 本地收 - E（纯网络+本地）
            let view = book.depth(1);
            let e_ms = view.gateway_ts_ms;
            let t_ms = view.venue_ts_ms;
            let local_now = nexus_core::now_ms();
            // 不 clamp：负数即表示本地时钟超前交易所网关，暴露时钟偏差。
            let local_e = if e_ms > 0 { local_now - e_ms } else { 0 };
            let t_minus_e = if e_ms > 0 && t_ms > 0 {
                e_ms - t_ms
            } else {
                0
            };

            print!(
                "\r[{ts}] Bid: {bid:>10.4} x {bid_qty:<8.4} | Ask: {ask:>10.4} x {ask_qty:<8.4} | Spread: {spread:.4} ({spread_pct}%) | Updates: {updates:>5} | E={e_ms} T={t_ms} local-E={local_e}ms T→E={t_minus_e}ms     ",
                bid = top.bid,
                bid_qty = top.bid_qty,
                ask = top.ask,
                ask_qty = top.ask_qty,
            );
            let _ = io::stdout().flush();
        } else {
            print!("\r[{elapsed:?}] Book not ready, waiting...                              ");
            let _ = io::stdout().flush();
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // 最终输出
    println!("\n");
    let view = book.depth(5);
    println!("{}", "─".repeat(56));
    println!("  Final Depth (5 levels):");
    println!("  {:>22}  {:<22}", "Bids", "Asks");
    println!("  {:─>22}  {:<22}", "", "");
    for i in 0..5 {
        let bid_str = if i < view.bids.len() {
            format!("{:>10.4}  x {:<8.4}", view.bids[i].0, view.bids[i].1)
        } else {
            " ".repeat(24)
        };
        let ask_str = if i < view.asks.len() {
            format!("{:>10.4}  x {:<8.4}", view.asks[i].0, view.asks[i].1)
        } else {
            " ".repeat(24)
        };
        println!("  {bid_str}  {ask_str}");
    }
    println!("{}", "─".repeat(56));
    println!(
        "\nTotal: {elapsed:.1}s, updates={updates}, staleness={staleness:?}",
        elapsed = start.elapsed().as_secs_f64(),
        updates = last_update_count,
        staleness = book.staleness(),
    );
    println!("Done.");

    Ok(())
}
