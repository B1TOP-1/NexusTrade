//! depth@0ms vs depth@100ms 对比工具。
//!
//! 同时订阅 BTCUSDT 的 `@depth@0ms` 和 `@depth@100ms` 两个流，对比：
//!   1. 推送频率（事件/秒）
//!   2. 同一采样时刻的 BBO（0ms 的买一/卖一是否 = 100ms 的买一/卖一）
//!   3. 深度一致性（前 5 档价格是否对齐）
//!
//! 用法：
//!   cargo run -p nexus-binance --example depth_speed_compare -- --rounds 3
//!   cargo run -p nexus-binance --example depth_speed_compare -- --duration 30
//!   cargo run -p nexus-binance --example depth_speed_compare -- --symbol ETHUSDT

use std::time::Duration;

use nexus_binance::{BinanceMarket, BinanceMarketConfig};
use nexus_core::{BookOptions, Decimal, MarketVenue, Symbol};

struct Args {
    symbol: String,
    rounds: usize,
    testnet: bool,
}

fn parse_args() -> Args {
    let mut args = Args {
        symbol: "BTCUSDT".to_string(),
        rounds: 3,
        testnet: false,
    };
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
            "--symbol" => {
                if i + 1 < raw.len() {
                    args.symbol = raw[i + 1].to_uppercase();
                    i += 1;
                }
            }
            "--rounds" => {
                if i + 1 < raw.len() {
                    args.rounds = raw[i + 1].parse().unwrap_or(args.rounds);
                    i += 1;
                }
            }
            "--testnet" => args.testnet = true,
            _ => {}
        }
        i += 1;
    }
    args
}

fn fmt_price(p: &Decimal) -> String {
    format!("{p:.2}")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();
    println!("{}", "=".repeat(78));
    println!(
        "  depth@0ms vs depth@100ms 对比  ({})",
        if args.testnet { "TESTNET" } else { "MAINNET" }
    );
    println!("  Symbol: {}  Rounds: {}", args.symbol, args.rounds);
    println!("{}", "=".repeat(78));

    let sym = Symbol::new(
        args.symbol.replace("USDT", ""),
        "USDT",
        args.symbol.clone(),
    );

    // 两个 market 实例：0ms 和 100ms
    let mut cfg_0ms = if args.testnet {
        BinanceMarketConfig::testnet()
    } else {
        BinanceMarketConfig::default()
    };
    cfg_0ms.depth_speed = "0ms".to_string();
    let market_0ms = BinanceMarket::connect(cfg_0ms).await?;
    let book_0ms = market_0ms
        .subscribe_book(&sym, BookOptions::default())
        .await?;

    let mut cfg_100ms = if args.testnet {
        BinanceMarketConfig::testnet()
    } else {
        BinanceMarketConfig::default()
    };
    cfg_100ms.depth_speed = "100ms".to_string();
    let market_100ms = BinanceMarket::connect(cfg_100ms).await?;
    let book_100ms = market_100ms
        .subscribe_book(&sym, BookOptions::default())
        .await?;

    // 等两个簿都就绪
    println!("\n等待订单簿就绪...");
    let ready_deadline = Duration::from_secs(15);
    let start = std::time::Instant::now();
    loop {
        if book_0ms.top().is_some() && book_100ms.top().is_some() {
            println!("  两个簿都已就绪 ✓");
            break;
        }
        if start.elapsed() > ready_deadline {
            println!("  超时等待就绪");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // 采样对比
    println!("\n开始采样对比（每 1s 采样一次）...\n");
    let mut total_0ms = 0u64;
    let mut total_100ms = 0u64;
    let mut match_count = 0u64;
    let mut mismatch_count = 0u64;

    for round in 0..args.rounds {
        // 采样 1 秒的增量事件数（seq 差 = 该秒事件数）
        let last_seq_0 = book_0ms.seq();
        let last_seq_100 = book_100ms.seq();
        tokio::time::sleep(Duration::from_secs(1)).await;
        let ev_0ms = book_0ms.seq().saturating_sub(last_seq_0);
        let ev_100ms = book_100ms.seq().saturating_sub(last_seq_100);
        total_0ms += ev_0ms;
        total_100ms += ev_100ms;

        // 取两边 BBO
        let top_0 = book_0ms.top();
        let top_100 = book_100ms.top();

        match (top_0, top_100) {
            (Some(t0), Some(t100)) => {
                let bid_match = t0.bid == t100.bid;
                let ask_match = t0.ask == t100.ask;
                let bbo_match = bid_match && ask_match;
                if bbo_match {
                    match_count += 1;
                } else {
                    mismatch_count += 1;
                }

                println!(
                    "[轮{}] 0ms: {}/s | 100ms: {}/s ({}x) | BBO匹配={}",
                    round,
                    ev_0ms,
                    ev_100ms,
                    if ev_100ms > 0 { ev_0ms as f64 / ev_100ms as f64 } else { 0.0 },
                    if bbo_match { "✅" } else { "❌" }
                );
                println!(
                    "       0ms   bid={} ask={}  seq={}",
                    fmt_price(&t0.bid),
                    fmt_price(&t0.ask),
                    book_0ms.seq()
                );
                println!(
                    "       100ms bid={} ask={}  seq={}",
                    fmt_price(&t100.bid),
                    fmt_price(&t100.ask),
                    book_100ms.seq()
                );
                if !bbo_match {
                    println!(
                        "       差异: bid差={} ask差={}",
                        (t0.bid - t100.bid),
                        (t0.ask - t100.ask)
                    );
                }
            }
            _ => println!("[轮{}] 簿未就绪", round),
        }

        // 深度对比（前 5 档）
        let d0 = book_0ms.depth(5);
        let d100 = book_100ms.depth(5);
        let bid_aligned = d0.bids.len() == d100.bids.len()
            && d0.bids.iter().zip(&d100.bids).all(|(a, b)| a.0 == b.0);
        let ask_aligned = d0.asks.len() == d100.asks.len()
            && d0.asks.iter().zip(&d100.asks).all(|(a, b)| a.0 == b.0);
        println!(
            "       深度5档: 0ms bids={}/asks={} | 100ms bids={}/asks={} | 对齐={}",
            d0.bids.len(),
            d0.asks.len(),
            d100.bids.len(),
            d100.asks.len(),
            if bid_aligned && ask_aligned { "✅" } else { "❌" }
        );
        println!();
    }

    // 汇总
    println!("{}", "=".repeat(78));
    println!("  汇总 ({} 轮, 每轮 1s)", args.rounds);
    println!(
        "  推送频率:  0ms = {}/s, 100ms = {}/s, 倍数 = {:.1}x",
        total_0ms / args.rounds as u64,
        total_100ms / args.rounds as u64,
        if total_100ms > 0 {
            total_0ms as f64 / total_100ms as f64
        } else {
            0.0
        }
    );
    println!(
        "  BBO 匹配:  {}/{} 轮完全一致",
        match_count,
        match_count + mismatch_count
    );
    println!("  （深度对齐见每轮输出）");
    println!("{}", "=".repeat(78));

    Ok(())
}
