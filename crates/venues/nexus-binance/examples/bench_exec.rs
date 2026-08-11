//! 交易实现性能对比：自研 execution.rs vs binance-futures-rs。
//!
//! 对比维度（真实请求，主网）：
//!   1. 下单延迟（post-only 远离盘口，不成交）
//!   2. 撤单延迟
//!   3. 账户查询延迟
//!
//! 每项跑 N 次取 最小/平均/最大 延迟，用数据决定哪套实现更优。
//!
//! 用法：
//!   cp .env.example .env   # 填 BINANCE_API_KEY / BINANCE_API_SECRET
//!   cargo run -p nexus-binance --example bench_exec                    # 3 轮默认
//!   cargo run -p nexus-binance --example bench_exec -- --rounds 5 --symbol BTCUSDT
//!   cargo run -p nexus-binance --example bench_exec -- --testnet        # 测试网
//!
//! 安全：post-only(GTX) + 价格偏离盘口 0.5% → 永不成交；下单后立即撤单。

use binance_futures_rs::{
    BinanceClient, CancelOrderRequest, Credentials, NewOrderRequest, OrderSide, OrderType,
    TimeInForce,
};
use nexus_binance::{BinanceVenue, BinanceVenueConfig};
use nexus_core::{
    ClientOrderId, Decimal, ExecutionVenue, NewOrder, OrderRef, PrivateVenue, Side, Symbol,
};
use rust_decimal_macros::dec;

// ── .env 加载 ──

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

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

// ── 计时工具 ──

struct Latency {
    min_ms: f64,
    avg_ms: f64,
    max_ms: f64,
}

fn summarize(samples: &[f64]) -> Latency {
    let min = samples.iter().cloned().fold(f64::MAX, f64::min);
    let max = samples.iter().cloned().fold(0.0, f64::max);
    let avg = samples.iter().sum::<f64>() / samples.len() as f64;
    Latency { min_ms: min, avg_ms: avg, max_ms: max }
}

fn report(label: &str, samples: &[f64]) {
    if samples.is_empty() {
        println!("  → {label}: 无样本（全部失败）");
        return;
    }
    let s = summarize(samples);
    println!(
        "  → {label}:  min={:.1}ms avg={:.1}ms max={:.1}ms  (n={})",
        s.min_ms,
        s.avg_ms,
        s.max_ms,
        samples.len()
    );
}

// ── 参考价：post-only 价格计算 ──

/// 从库的 price_ticker 取参考价，计算 post-only 不成交的价格。
async fn fetch_reference_prices(
    client: &BinanceClient,
    symbol: &str,
) -> Option<(f64, f64)> {
    match client.market().price_ticker(Some(symbol)).await {
        Ok(tickers) if !tickers.is_empty() => {
            let price = tickers[0].price.parse::<f64>().unwrap_or(0.0);
            Some((price * 0.995, price * 1.005)) // buy 在下方 0.5%，sell 在上方 0.5%
        }
        _ => {
            eprintln!("无法获取 {symbol} 参考价");
            None
        }
    }
}

// ── 1. 自研 execution.rs ──

async fn bench_self_made(
    venue: &BinanceVenue,
    symbol: &str,
    rounds: usize,
    buy_price: Decimal,
) {
    let sym = Symbol::new(symbol.replace("USDT", ""), "USDT", symbol.to_string());

    let mut place_samples = Vec::new();
    let mut cancel_samples = Vec::new();

    for i in 0..rounds {
        let client_id = ClientOrderId(format!("nxb-{}", i));
        let order = NewOrder::limit(sym.clone(), Side::Buy, buy_price, dec!(0.001), client_id.clone())
            .post_only();

        let t0 = std::time::Instant::now();
        let ack = venue.place(order).await;
        let elapsed = t0.elapsed().as_secs_f64() * 1000.0;

        match ack {
            Ok(a) => {
                place_samples.push(elapsed);

                let ref_ = OrderRef {
                    symbol: sym.clone(),
                    client_id,
                    venue_order_id: a.venue_order_id,
                };
                let t1 = std::time::Instant::now();
                match venue.cancel(&ref_).await {
                    Ok(()) => cancel_samples.push(t1.elapsed().as_secs_f64() * 1000.0),
                    Err(e) => eprintln!("    [自研] 撤单#{i} FAILED: {e}"),
                }
            }
            Err(e) => eprintln!("    [自研] 下单#{i} FAILED: {e}"),
        }
    }

    report("自研下单", &place_samples);
    report("自研撤单", &cancel_samples);
}

// ── 2. binance-futures-rs ──

async fn bench_library(
    client: &BinanceClient,
    symbol: &str,
    rounds: usize,
    buy_price: f64,
) {
    let mut place_samples = Vec::new();
    let mut cancel_samples = Vec::new();

    for i in 0..rounds {
        let client_id = format!("nxl-{i}");
        let order_req = NewOrderRequest::new(symbol.to_string(), OrderSide::Buy, OrderType::Limit)
            .quantity("0.001".to_string())
            .price(format!("{buy_price:.2}"))
            .time_in_force(TimeInForce::Gtx)
            .client_order_id(client_id.clone());

        let t0 = std::time::Instant::now();
        let order = client.trading().new_order(order_req).await;
        let elapsed = t0.elapsed().as_secs_f64() * 1000.0;

        match order {
            Ok(o) => {
                place_samples.push(elapsed);

                let cancel_req = CancelOrderRequest::new(symbol.to_string())
                    .order_id(o.order_id);
                let t1 = std::time::Instant::now();
                match client.trading().cancel_order(cancel_req).await {
                    Ok(_) => cancel_samples.push(t1.elapsed().as_secs_f64() * 1000.0),
                    Err(e) => eprintln!("    [库] 撤单#{i} FAILED: {e}"),
                }
            }
            Err(e) => eprintln!("    [库] 下单#{i} FAILED: {e}"),
        }
    }

    report("库下单", &place_samples);
    report("库撤单", &cancel_samples);
}

// ── 账户查询对比 ──

async fn bench_account(venue: &BinanceVenue, client: &BinanceClient) {
    let mut self_samples = Vec::new();
    let mut lib_samples = Vec::new();

    for _ in 0..3 {
        let t0 = std::time::Instant::now();
        let _ = client.account().balance().await;
        lib_samples.push(t0.elapsed().as_secs_f64() * 1000.0);

        let t1 = std::time::Instant::now();
        let _ = venue.snapshot().await;
        self_samples.push(t1.elapsed().as_secs_f64() * 1000.0);
    }

    report("自研账户查询", &self_samples);
    report("库账户查询", &lib_samples);
}

// ── main ──

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    load_dotenv();

    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut rounds = 3usize;
    let mut symbol = "BTCUSDT".to_string();
    let mut testnet = false;
    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
            "--rounds" => {
                if i + 1 < raw.len() {
                    rounds = raw[i + 1].parse().unwrap_or(rounds);
                    i += 1;
                }
            }
            "--symbol" => {
                if i + 1 < raw.len() {
                    symbol = raw[i + 1].to_uppercase();
                    i += 1;
                }
            }
            "--testnet" => testnet = true,
            _ => {}
        }
        i += 1;
    }

    let (key, secret) = if testnet {
        (env_or("BINANCE_TESTNET_KEY", ""), env_or("BINANCE_TESTNET_SECRET", ""))
    } else {
        (env_or("BINANCE_API_KEY", ""), env_or("BINANCE_API_SECRET", ""))
    };

    if key.is_empty() || secret.is_empty() {
        println!("⚠ 未找到 API Key。请创建 .env（cp .env.example .env）并填入 BINANCE_API_KEY / BINANCE_API_SECRET");
        return Ok(());
    }

    println!("{}", "=".repeat(64));
    println!("  交易实现性能对比: 自研 execution.rs  vs  binance-futures-rs");
    println!(
        "  Symbol: {symbol}  Rounds: {rounds}  Network: {}",
        if testnet { "TESTNET" } else { "MAINNET" }
    );
    println!("{}", "=".repeat(64));

    // 库 client（先建，用于取参考价）
    let client = if testnet {
        BinanceClient::testnet_with_credentials(Credentials::new(key.clone(), secret.clone()))
    } else {
        BinanceClient::new_with_credentials(Credentials::new(key.clone(), secret.clone()))
    };

    // 取参考价
    let Some((buy_price_f, _sell)) = fetch_reference_prices(&client, &symbol).await else {
        println!("无法获取参考价，退出。");
        return Ok(());
    };
    let buy_price_dec = Decimal::from_f64_retain(buy_price_f).unwrap_or(dec!(1));
    println!(
        "  参考买价(post-only 0.5% 下方): {buy_price_dec} ({symbol})"
    );

    // 自研 venue
    let config = if testnet {
        BinanceVenueConfig::testnet(key.clone(), secret.clone())
    } else {
        BinanceVenueConfig::mainnet(key.clone(), secret.clone())
    };
    println!("\n连接自研 venue (需 listenKey)...");
    let venue = match BinanceVenue::connect(config).await {
        Ok(v) => Some(v),
        Err(e) => {
            println!("  自研 venue 连接失败: {e}");
            println!("  → 跳过自研测试，只测库。");
            None
        }
    };

    if let Some(venue) = &venue {
        println!("\n────────── 1. 账户查询延迟 (3 轮) ──────────");
        bench_account(venue, &client).await;

        println!("\n────────── 2. 下单 + 撤单延迟 ──────────");
        println!("  [自研 execution.rs]");
        bench_self_made(venue, &symbol, rounds, buy_price_dec).await;
    } else {
        println!("\n────────── 1. 账户查询延迟 (库) ──────────");
        let mut lib_samples = Vec::new();
        for _ in 0..3 {
            let t0 = std::time::Instant::now();
            let _ = client.account().balance().await;
            lib_samples.push(t0.elapsed().as_secs_f64() * 1000.0);
        }
        report("库账户查询", &lib_samples);
    }

    println!("  [binance-futures-rs]");
    bench_library(&client, &symbol, rounds, buy_price_f).await;

    println!("\n{}", "=".repeat(64));
    println!("  结论: 对比上方 min/avg 延迟，取更优者作为交易主实现。");
    println!("{}", "=".repeat(64));

    Ok(())
}
