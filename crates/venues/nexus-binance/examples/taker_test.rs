//! Taker 市价单全链路测试（0ms 本地订单簿 + ws-fapi 市价单 + 用户流确认）。
//!
//! 设计：
//!   - **用户流提前建立后台读取**（下单前启动，持续收 ORDER_TRADE_UPDATE 存 map）
//!   - **交替开/平仓**：偶数轮买开仓，奇数轮卖 reduceOnly 平仓
//!   - **滑点容忍**：市价单成交价 vs 本地簿 BBO，容忍 0.1% 滑点
//!
//! 测量：
//!   1. 订单簿延迟：0ms 模式 local-E / local-T
//!   2. 市价单全链路：下单→ACK→用户流 FILLED
//!   3. 滑点：成交价 vs 本地簿 BBO 的差
//!
//! ⚠ 真实成交，消耗手续费（默认 0.0001 BTC ≈ 6.4 USDT）。
//! 用法：
//!   cargo run -p nexus-binance --example taker_test -- --qty 0.0001 --rounds 4
//!   cargo run -p nexus-binance --example taker_test -- --testnet

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt;
use nexus_binance::{BinanceMarket, BinanceMarketConfig, WsFapiClient};
use nexus_core::{BookOptions, ClientOrderId, Decimal, MarketVenue, NewOrder, Side, Symbol};
use rust_decimal_macros::dec;

struct Args {
    symbol: String,
    qty: Decimal,
    rounds: usize,
    testnet: bool,
}

fn parse_args() -> Args {
    let mut args = Args {
        symbol: "BTCUSDT".to_string(),
        qty: dec!(0.0001),
        rounds: 4,
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
            "--qty" => {
                if i + 1 < raw.len() {
                    args.qty = Decimal::from_str(&raw[i + 1]).unwrap_or(args.qty);
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

fn load_dotenv() {
    for p in [".env", "../.env", "../../.env"] {
        let Ok(c) = std::fs::read_to_string(p) else { continue };
        for l in c.lines() {
            let l = l.trim();
            if l.is_empty() || l.starts_with('#') { continue; }
            if let Some((k, v)) = l.split_once('=') {
                if std::env::var(k.trim()).is_err() { std::env::set_var(k.trim(), v.trim()); }
            }
        }
        break;
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 用户流裸连接。返回 (write, read)。
async fn connect_user_stream(
    api_key: &str,
    rest_url: &str,
) -> Result<
    (
        futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
            tokio_tungstenite::tungstenite::Message,
        >,
        futures_util::stream::SplitStream<
            tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
        >,
    ),
    String,
> {
    let http = reqwest::Client::new();
    let resp: serde_json::Value = http
        .post(format!("{rest_url}/fapi/v1/listenKey"))
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    let lk = resp["listenKey"].as_str().ok_or("listenKey missing")?.to_string();
    let url = if rest_url.contains("testnet") {
        format!("wss://stream.binancefuture.com/private/ws/{lk}")
    } else {
        format!("wss://fstream.binance.com/private/ws/{lk}")
    };
    let (ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .map_err(|e| format!("user stream connect: {e}"))?;
    Ok(ws.split())
}

/// 用户流订单更新（存 map）。
#[derive(Clone)]
struct StreamEntry {
    status: String,
    avg_price: String,
    exec_qty: String,
    gateway_ms: i64,
    trade_ms: i64,
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

    println!("{}", "=".repeat(72));
    println!("  Taker 市价单全链路测试（0ms 簿 + ws-fapi + 用户流后台读取）");
    println!(
        "  Symbol: {}  Qty: {}  Rounds: {}（偶数轮买开仓 / 奇数轮卖平仓）",
        args.symbol, args.qty, args.rounds
    );
    println!("  ⚠ 真实成交，消耗手续费!");
    println!("{}", "=".repeat(72));

    let rest_url = if args.testnet {
        "https://testnet.binancefuture.com"
    } else {
        "https://fapi.binance.com"
    };
    let sym = Symbol::new(
        args.symbol.replace("USDT", ""),
        "USDT",
        args.symbol.clone(),
    );

    // 1. 连 0ms 本地簿
    println!("\n[1] 连接 0ms 本地订单簿...");
    let mut cfg = if args.testnet {
        BinanceMarketConfig::testnet()
    } else {
        BinanceMarketConfig::default()
    };
    cfg.depth_speed = "0ms".to_string();
    let market = BinanceMarket::connect(cfg).await?;
    let book = market
        .subscribe_book(&sym, BookOptions::default())
        .await?;
    let ready_start = std::time::Instant::now();
    loop {
        if book.top().is_some() {
            break;
        }
        if ready_start.elapsed() > Duration::from_secs(15) {
            println!("  超时等待簿就绪");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    println!("    簿就绪 ✓");

    // 2. 连 ws-fapi
    println!("[2] 连接 ws-fapi...");
    let fapi = WsFapiClient::connect(key.clone(), secret.clone(), args.testnet).await?;
    println!("    连接成功 ✓");

    // 3. 连用户流 + 提前建立后台读取 task
    println!("[3] 连接用户流 + 提前建立后台读取...");
    let (_user_write, user_read) = connect_user_stream(&key, rest_url).await?;
    println!("    连接成功 ✓");

    // 共享 map：cid → 最新 OTU 条目
    let updates: Arc<Mutex<HashMap<String, StreamEntry>>> = Arc::new(Mutex::new(HashMap::new()));
    let notify = Arc::new(tokio::sync::Notify::new());
    let updates_reader = Arc::clone(&updates);
    let notify_reader = Arc::clone(&notify);
    tokio::spawn(async move {
        let mut read = user_read;
        loop {
            match tokio::time::timeout(Duration::from_secs(30), read.next()).await {
                Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                    let Ok(v) = serde_json::from_str::<serde_json::Value>(&text.to_string()) else {
                        continue;
                    };
                    if v["e"] != "ORDER_TRADE_UPDATE" {
                        continue;
                    }
                    let o = &v["o"];
                    let cid = o["c"].as_str().unwrap_or("").to_string();
                    if cid.is_empty() {
                        continue;
                    }
                    let entry = StreamEntry {
                        status: o["X"].as_str().unwrap_or("").to_string(),
                        avg_price: o["ap"].as_str().unwrap_or("0").to_string(),
                        exec_qty: o["z"].as_str().unwrap_or("0").to_string(),
                        gateway_ms: v["E"].as_i64().unwrap_or(0),
                        trade_ms: v["T"].as_i64().unwrap_or(0),
                    };
                    updates_reader.lock().unwrap().insert(cid, entry);
                    notify_reader.notify_one(); // 事件驱动：通知主循环
                }
                _ => continue,
            }
        }
    });
    println!("    用户流后台读取已启动 ✓");

    // 4. 逐轮测试（交替开/平仓）
    println!("\n[4] 开始 taker 市价测试（开/平交替）...\n");
    let mut book_local_e: Vec<f64> = Vec::new();
    let mut book_local_t: Vec<f64> = Vec::new();
    let mut fill_latency: Vec<f64> = Vec::new();
    let mut ack_latency: Vec<f64> = Vec::new();
    let mut slippages: Vec<f64> = Vec::new();
    let mut succeeded = 0u32;

    for i in 0..args.rounds {
        // 开/平交替：偶数轮买开仓，奇数轮卖 reduceOnly 平仓
        let is_open = i % 2 == 0;
        let side = if is_open { Side::Buy } else { Side::Sell };
        let side_str = if is_open { "买开仓" } else { "卖平仓" };

        // 下单前的本地簿 BBO + 簿延迟
        let pre_top = book.top().unwrap();
        let view = book.depth(1);
        let now = now_ms();
        let book_e = view.gateway_ts_ms;
        let book_t = view.venue_ts_ms;
        let local_e = if book_e > 0 { now - book_e } else { 0 };
        let local_t = if book_t > 0 { now - book_t } else { 0 };
        book_local_e.push(local_e as f64);
        book_local_t.push(local_t as f64);

        let book_side_price = if is_open { pre_top.ask } else { pre_top.bid };
        println!(
            "── 轮 {i} ── {side_str} | 簿BBO: bid={} ask={} | 簿local-E={}ms local-T={}ms",
            pre_top.bid, pre_top.ask, local_e, local_t,
        );

        // ── 下单前精度校验（SymbolMeta::validate）──
        // 检查 qty 是否满足 LOT_SIZE/minQty，名义额是否 ≥ MIN_NOTIONAL
        let meta = market.symbol_meta(&sym)?;
        let mut qty = args.qty;
        // 自动量化到 lot 并抬高到 min_qty
        if qty < meta.min_qty {
            println!(
                "    ⚠ qty {} < minQty {}, 自动抬到 {}",
                qty, meta.min_qty, meta.min_qty
            );
            qty = meta.min_qty;
        }
        qty = meta.quantize_qty(qty);
        if qty * pre_top.bid < meta.min_notional {
            println!(
                "    ⚠ 名义额 {} < minNotional {}，需 qty ≥ {:.4}",
                qty * pre_top.bid,
                meta.min_notional,
                meta.min_notional / pre_top.bid
            );
        }

        // 构造市价单（平仓用 reduceOnly）
        let cid = format!("nxtaker{}", i);
        let mut order = NewOrder::market(
            sym.clone(),
            side,
            qty,
            ClientOrderId(cid.clone()),
        );
        if !is_open {
            order = order.reduce_only();
        }

        // 下单计时
        let t_start = std::time::Instant::now();
        let result = fapi.place(&order).await;
        let ack_us = t_start.elapsed().as_micros() as f64;
        ack_latency.push(ack_us);

        match result {
            Ok(order_id) => {
                println!("  市价单已下: orderId={order_id}  ACK={:.2}ms", ack_us / 1000.0);

                // 事件驱动：等待用户流通知（后台 task 收 OTU 时 notify）
                let mut confirmed = false;
                let confirm_deadline = std::time::Instant::now() + Duration::from_secs(10);
                loop {
                    // 先查 map（可能已到达）
                    let entry = updates.lock().unwrap().get(&cid).cloned();
                    if let Some(e) = entry {
                        if e.status == "FILLED" || e.status == "PARTIALLY_FILLED" {
                            let fill_us = t_start.elapsed().as_micros() as f64;
                            let now = now_ms();
                            let order_local_e = now - e.gateway_ms;
                            let order_local_t = now - e.trade_ms;
                            fill_latency.push(fill_us);
                            succeeded += 1;

                            // 滑点：成交价 vs 本地簿 BBO
                            let fp = Decimal::from_str(&e.avg_price).unwrap_or(Decimal::ZERO);
                            let slippage_pct = if book_side_price > Decimal::ZERO {
                                ((fp - book_side_price) / book_side_price * dec!(100))
                                    .abs()
                            } else {
                                Decimal::ZERO
                            };
                            slippages.push(
                                slippage_pct
                                    .to_string()
                                    .parse::<f64>()
                                    .unwrap_or(0.0),
                            );

                            println!(
                                "  用户流确认: status={} 下单→FILLED={:.2}ms local-E={}ms local-T={}ms",
                                e.status,
                                fill_us / 1000.0,
                                order_local_e,
                                order_local_t,
                            );
                            println!(
                                "  成交: avgPrice={} 量={} 滑点={:.3}% (簿参考 {})",
                                e.avg_price, e.exec_qty, slippage_pct, book_side_price,
                            );
                            confirmed = true;
                            break;
                        }
                    }
                    // 等用户流通知（事件驱动，不固定 sleep）。超时兜底。
                    let notified = tokio::time::timeout(
                        Duration::from_millis(100),
                        notify.notified(),
                    )
                    .await;
                    if notified.is_err() && std::time::Instant::now() > confirm_deadline {
                        break;
                    }
                }
                if !confirmed {
                    println!("  用户流确认: 超时（查 order.status 兜底）");
                    if let Ok(status) = fapi.query(&sym, order_id).await {
                        let st = status["result"]["status"].as_str().unwrap_or("?").to_string();
                        let ap = status["result"]["avgPrice"].as_str().unwrap_or("?").to_string();
                        let z = status["result"]["executedQty"].as_str().unwrap_or("?").to_string();
                        println!("  兜底查询: status={st} avgPrice={ap} executedQty={z}");
                    }
                }
            }
            Err(e) => println!("  下单失败: {e}"),
        }
        println!();
    }

    // 汇总
    println!("{}", "=".repeat(72));
    println!("  汇总 ({succeeded} 笔确认成交)");
    let sum = |v: &[f64]| -> (f64, f64, f64) {
        if v.is_empty() { return (0.0, 0.0, 0.0); }
        let min = v.iter().cloned().fold(f64::MAX, f64::min);
        let max = v.iter().cloned().fold(0.0, f64::max);
        let avg = v.iter().sum::<f64>() / v.len() as f64;
        (min, avg, max)
    };
    let (a_min, a_avg, a_max) = sum(&ack_latency);
    let (f_min, f_avg, f_max) = sum(&fill_latency);
    let (e_min, e_avg, e_max) = sum(&book_local_e);
    let (t_min, t_avg, t_max) = sum(&book_local_t);
    let (s_min, s_avg, s_max) = sum(&slippages);
    println!("  市价单 ACK:  min={:.2}ms avg={:.2}ms max={:.2}ms", a_min / 1000.0, a_avg / 1000.0, a_max / 1000.0);
    if !fill_latency.is_empty() {
        println!("  下单→FILLED: min={:.2}ms avg={:.2}ms max={:.2}ms", f_min / 1000.0, f_avg / 1000.0, f_max / 1000.0);
    }
    println!("  0ms簿 local-E: min={:.2}ms avg={:.2}ms max={:.2}ms", e_min, e_avg, e_max);
    println!("  0ms簿 local-T: min={:.2}ms avg={:.2}ms max={:.2}ms", t_min, t_avg, t_max);
    if !slippages.is_empty() {
        println!("  滑点: min={:.3}% avg={:.3}% max={:.3}%", s_min, s_avg, s_max);
    }
    println!("{}", "=".repeat(72));

    Ok(())
}
