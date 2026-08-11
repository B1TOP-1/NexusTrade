//! REST vs WS 下单延迟对比（自研实现）。
//!
//! 对比两条下单通道的真实延迟：
//!   1. REST  : BinanceVenue::place/cancel (POST /fapi/v1/order)
//!   2. WS    : WsFapiClient::place/cancel (wss://ws-fapi.binance.com)
//!
//! 每项 N 轮取 min/avg/max，只统计成功请求。post-only 远离盘口，不成交。
//!
//! 用法：
//!   cargo run -p nexus-binance --example bench_exec -- --rounds 5
//!   cargo run -p nexus-binance --example bench_exec -- --symbol ETHUSDT
//!   cargo run -p nexus-binance --example bench_exec -- --testnet

use std::time::Instant;

use nexus_binance::{BinanceVenue, BinanceVenueConfig, WsFapiClient};
use nexus_core::{
    ClientOrderId, ExecutionVenue, NewOrder, OrderRef, Side, Symbol,
};
use rust_decimal_macros::dec;

fn load_dotenv() {
    for path in [".env", "../.env", "../../.env"] {
        let Ok(content) = std::fs::read_to_string(path) else {
            continue;
        };
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((k, v)) = line.split_once('=') {
                if std::env::var(k.trim()).is_err() {
                    std::env::set_var(k.trim(), v.trim());
                }
            }
        }
        break;
    }
}

struct Args {
    rounds: usize,
    symbol: String,
    testnet: bool,
}

fn parse_args() -> Args {
    let mut args = Args {
        rounds: 5,
        symbol: "BTCUSDT".to_string(),
        testnet: false,
    };
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
            "--rounds" => {
                if i + 1 < raw.len() {
                    args.rounds = raw[i + 1].parse().unwrap_or(args.rounds);
                    i += 1;
                }
            }
            "--symbol" => {
                if i + 1 < raw.len() {
                    args.symbol = raw[i + 1].to_uppercase();
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

fn summarize(samples: &[f64]) -> (f64, f64, f64) {
    if samples.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    let min = samples.iter().cloned().fold(f64::MAX, f64::min);
    let max = samples.iter().cloned().fold(0.0, f64::max);
    let avg = samples.iter().sum::<f64>() / samples.len() as f64;
    (min, avg, max)
}

fn report(label: &str, samples: &[f64]) {
    if samples.is_empty() {
        println!("  {label:<24} 无样本（全部失败）");
        return;
    }
    let (min, avg, max) = summarize(samples);
    println!(
        "  {label:<24} min={min:>7.2}ms avg={avg:>7.2}ms max={max:>7.2}ms  (n={})",
        samples.len()
    );
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    load_dotenv();
    let args = parse_args();

    let (key, secret) = if args.testnet {
        (
            std::env::var("BINANCE_TESTNET_KEY").unwrap_or_default(),
            std::env::var("BINANCE_TESTNET_SECRET").unwrap_or_default(),
        )
    } else {
        (
            std::env::var("BINANCE_API_KEY").unwrap_or_default(),
            std::env::var("BINANCE_API_SECRET").unwrap_or_default(),
        )
    };

    if key.is_empty() || secret.is_empty() {
        println!("⚠ 未找到 API Key，请创建 .env");
        return Ok(());
    }

    println!("{}", "=".repeat(64));
    println!("  REST vs WS 下单延迟对比（post-only 不成交）");
    println!(
        "  Symbol: {}  Rounds: {}  Network: {}",
        args.symbol,
        args.rounds,
        if args.testnet { "TESTNET" } else { "MAINNET" }
    );
    println!("{}", "=".repeat(64));

    // 下单价格（远离盘口，post-only 不会成交）
    let price = dec!(63600);
    let sym = Symbol::new(
        args.symbol.replace("USDT", ""),
        "USDT",
        args.symbol.clone(),
    );

    // ── REST 通道 ──
    let config = if args.testnet {
        BinanceVenueConfig::testnet(key.clone(), secret.clone())
    } else {
        BinanceVenueConfig::mainnet(key.clone(), secret.clone())
    };
    println!("\n[1] REST 通道 (BinanceVenue::place)");
    let venue = BinanceVenue::connect(config).await?;

    let mut rest_place = Vec::new();
    let mut rest_cancel = Vec::new();
    for i in 0..args.rounds {
        let order = NewOrder::limit(
            sym.clone(),
            Side::Buy,
            price,
            dec!(0.001),
            ClientOrderId(format!("nxr{}", i)),
        )
        .post_only();

        let t0 = Instant::now();
        match venue.place(order).await {
            Ok(ack) => {
                rest_place.push(t0.elapsed().as_secs_f64() * 1000.0);
                let ref_ = OrderRef {
                    symbol: sym.clone(),
                    client_id: ClientOrderId(format!("nxr{}", i)),
                    venue_order_id: ack.venue_order_id,
                };
                let t1 = Instant::now();
                match venue.cancel(&ref_).await {
                    Ok(()) => rest_cancel.push(t1.elapsed().as_secs_f64() * 1000.0),
                    Err(e) => println!("    REST 撤单#{i} FAILED: {e}"),
                }
            }
            Err(e) => println!("    REST 下单#{i} FAILED: {e}"),
        }
    }
    report("REST 下单", &rest_place);
    report("REST 撤单", &rest_cancel);

    // ── WS 通道 ──
    println!("\n[2] WS 通道 (WsFapiClient::place)");
    let ws_client = WsFapiClient::connect(key, secret, args.testnet).await?;

    let mut ws_place = Vec::new();
    let mut ws_cancel = Vec::new();
    for i in 0..args.rounds {
        let order = NewOrder::limit(
            sym.clone(),
            Side::Buy,
            price,
            dec!(0.001),
            ClientOrderId(format!("nxw{}", i)),
        )
        .post_only();

        let t0 = Instant::now();
        match ws_client.place(&order).await {
            Ok(order_id) => {
                ws_place.push(t0.elapsed().as_secs_f64() * 1000.0);
                let t1 = Instant::now();
                match ws_client.cancel(&sym, order_id).await {
                    Ok(()) => ws_cancel.push(t1.elapsed().as_secs_f64() * 1000.0),
                    Err(e) => println!("    WS 撤单#{i} FAILED: {e}"),
                }
            }
            Err(e) => println!("    WS 下单#{i} FAILED: {e}"),
        }
    }
    report("WS 下单", &ws_place);
    report("WS 撤单", &ws_cancel);

    // ── 结论 ──
    println!("\n{}", "=".repeat(64));
    let rest = rest_place.iter().cloned().fold(f64::MAX, f64::min);
    let ws = ws_place.iter().cloned().fold(f64::MAX, f64::min);
    if !rest_place.is_empty() && !ws_place.is_empty() {
        println!("  下单 min:  REST={rest:.2}ms  WS={ws:.2}ms");
        if ws < rest {
            println!("  → WS 快 {:.2}ms ({:.1}%)", rest - ws, (rest - ws) / rest * 100.0);
        } else {
            println!("  → REST 快 {:.2}ms ({:.1}%)", ws - rest, (ws - rest) / ws * 100.0);
        }
    }
    println!("{}", "=".repeat(64));

    Ok(())
}
