//! Taker 市价单全链路测试（0ms 本地订单簿 + ws-fapi 市价单 + 用户流确认）。
//!
//! 同时测量：
//!   1. **订单簿延迟**：0ms 模式下的 local-E（本地收−E）和 local-T（本地收−T）
//!   2. **市价单全链路**：下单→ACK→用户流 FILLED 确认延迟
//!   3. **价格一致性**：成交价 vs 本地簿 BBO
//!
//! ⚠ 真实成交，消耗手续费。默认 0.001 BTC。
//! 用法：
//!   cargo run -p nexus-binance --example taker_test -- --qty 0.001
//!   cargo run -p nexus-binance --example taker_test -- --qty 0.001 --side sell
//!   cargo run -p nexus-binance --example taker_test -- --rounds 3

use std::str::FromStr;
use std::time::Duration;

use futures_util::StreamExt;
use nexus_binance::{BinanceMarket, BinanceMarketConfig, WsFapiClient};
use nexus_core::{BookOptions, ClientOrderId, Decimal, MarketVenue, NewOrder, Side, Symbol};
use rust_decimal_macros::dec;

struct Args {
    symbol: String,
    qty: Decimal,
    side: Side,
    rounds: usize,
    testnet: bool,
}

fn parse_args() -> Args {
    let mut args = Args {
        symbol: "BTCUSDT".to_string(),
        qty: dec!(0.001),
        side: Side::Buy,
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
            "--qty" => {
                if i + 1 < raw.len() {
                    args.qty = Decimal::from_str(&raw[i + 1]).unwrap_or(args.qty);
                    i += 1;
                }
            }
            "--side" => {
                if i + 1 < raw.len() {
                    args.side = if raw[i + 1].to_lowercase() == "sell" {
                        Side::Sell
                    } else {
                        Side::Buy
                    };
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

/// 用户流裸连接，等待指定 client_order_id 的状态确认。
/// 返回 (local_E, local_T, 状态)。超时返回 None。
async fn wait_fill(
    user_read: &mut futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    >,
    cid: &str,
    statuses: &[&str],
) -> Option<(i64, i64, String)> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        let msg = tokio::time::timeout(Duration::from_millis(500), user_read.next()).await;
        let Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) = msg else {
            continue;
        };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&text.to_string()) else {
            continue;
        };
        if v["e"] != "ORDER_TRADE_UPDATE" {
            continue;
        }
        let o = &v["o"];
        if o["c"].as_str() == Some(cid) {
            let st = o["X"].as_str().unwrap_or("").to_string();
            // 完整成交明细打印
            println!(
                "    [WS成交] 事件e={} E={} T={}",
                v["e"].as_str().unwrap_or("?"),
                v["E"].as_i64().unwrap_or(0),
                v["T"].as_i64().unwrap_or(0)
            );
            println!(
                "            订单 s={} i={} c={} S={} o={} X={}",
                o["s"].as_str().unwrap_or("?"),
                o["i"].as_i64().unwrap_or(0),
                o["c"].as_str().unwrap_or("?"),
                o["S"].as_str().unwrap_or("?"),
                o["o"].as_str().unwrap_or("?"),
                o["X"].as_str().unwrap_or("?"),
            );
            println!(
                "            价格 p={} 原始量 q={} 已成交 z={} 均价 ap={}",
                o["p"].as_str().unwrap_or("?"),
                o["q"].as_str().unwrap_or("?"),
                o["z"].as_str().unwrap_or("?"),
                o["ap"].as_str().unwrap_or("?"),
            );
            println!(
                "            最新成交价 L={} 最新成交量 l={} 手续费 n={} 币种 N={}",
                o["L"].as_str().unwrap_or("?"),
                o["l"].as_str().unwrap_or("?"),
                o["n"].as_str().unwrap_or("?"),
                o["N"].as_str().unwrap_or("?"),
            );
            println!(
                "            tif={} 被动成交 m={} reduceOnly={}",
                o["f"].as_str().unwrap_or("?"),
                o["m"].as_bool().unwrap_or(false),
                o["R"].as_str().unwrap_or("?"),
            );
            if statuses.contains(&st.as_str()) {
                return Some((
                    v["E"].as_i64().unwrap_or(0),
                    v["T"].as_i64().unwrap_or(0),
                    st,
                ));
            }
        }
    }
    None
}

/// 连接用户流（裸连接）。
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

    let side_str = if args.side == Side::Buy { "BUY" } else { "SELL" };
    println!("{}", "=".repeat(72));
    println!("  Taker 市价单全链路测试（0ms 订单簿 + ws-fapi + 用户流）");
    println!(
        "  Symbol: {}  Qty: {}  Side: {side_str}  Rounds: {}",
        args.symbol, args.qty, args.rounds
    );
    println!("  ⚠ 市价单会真实成交，消耗手续费!");
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

    // 3. 连用户流（裸连接）
    println!("[3] 连接用户流 (listenKey)...");
    let (_user_write, mut user_read) = connect_user_stream(&key, rest_url).await?;
    println!("    连接成功 ✓");

    // 4. 逐轮测试
    println!("\n[4] 开始 taker 市价测试...\n");
    let mut book_local_e: Vec<f64> = Vec::new();
    let mut book_local_t: Vec<f64> = Vec::new();
    let mut fill_latency: Vec<f64> = Vec::new();
    let mut ack_latency: Vec<f64> = Vec::new();
    let mut price_matches = 0u32;
    let mut total_rounds = 0u32;

    for i in 0..args.rounds {
        // 下单前的本地簿 BBO + 簿延迟（0ms 的 local-E/local-T）
        let pre_top = book.top().unwrap();
        let view = book.depth(1);
        let now = now_ms();
        let book_e = view.gateway_ts_ms;
        let book_t = view.venue_ts_ms;
        let local_e = if book_e > 0 { now - book_e } else { 0 };
        let local_t = if book_t > 0 { now - book_t } else { 0 };
        book_local_e.push(local_e as f64);
        book_local_t.push(local_t as f64);

        let book_side_price = if args.side == Side::Buy {
            pre_top.ask
        } else {
            pre_top.bid
        };
        println!(
            "── 轮 {i} ── 簿BBO: bid={} ask={} | 簿local-E={}ms local-T={}ms",
            pre_top.bid, pre_top.ask, local_e, local_t,
        );

        // 构造市价单
        let cid = format!("nxtaker{}", i);
        let order = NewOrder::market(
            sym.clone(),
            args.side,
            args.qty,
            ClientOrderId(cid.clone()),
        );

        // 下单计时
        let t_start = std::time::Instant::now();
        let result = fapi.place(&order).await;
        let ack_us = t_start.elapsed().as_micros() as f64;
        ack_latency.push(ack_us);

        match result {
            Ok(order_id) => {
                println!(
                    "  市价单已下: orderId={order_id}  ACK={:.2}ms",
                    ack_us / 1000.0
                );

                // 等用户流 FILLED 确认（全链路）
                if let Some((e_ms, t_ms, st)) =
                    wait_fill(&mut user_read, &cid, &["FILLED", "PARTIALLY_FILLED"]).await
                {
                    let fill_us = t_start.elapsed().as_micros() as f64;
                    let now = now_ms();
                    let order_local_e = now - e_ms;
                    let order_local_t = now - t_ms;
                    fill_latency.push(fill_us);
                    println!(
                        "  用户流确认: status={st} 下单→FILLED={:.2}ms local-E={}ms local-T={}ms",
                        fill_us / 1000.0,
                        order_local_e,
                        order_local_t,
                    );
                } else {
                    println!("  用户流确认: 超时");
                }

                // 查成交价
                match fapi.query(&sym, order_id).await {
                    Ok(status) => {
                        let fill_price = status["result"]["avgPrice"]
                            .as_str()
                            .unwrap_or("?")
                            .to_string();
                        let exec_qty = status["result"]["executedQty"]
                            .as_str()
                            .unwrap_or("?")
                            .to_string();
                        let st = status["result"]["status"].as_str().unwrap_or("?").to_string();
                        let fp = Decimal::from_str(&fill_price).unwrap_or(Decimal::ZERO);
                        // 价格一致性：成交价 ≈ 本地簿参考价（±1 USDT）
                        let price_match =
                            fp >= book_side_price - dec!(1) && fp <= book_side_price + dec!(1);
                        if price_match { price_matches += 1; }
                        total_rounds += 1;
                        println!(
                            "  成交: status={st} avgPrice={fill_price} executedQty={exec_qty} 价格一致性={}",
                            if price_match { "✅" } else { "❌" }
                        );
                    }
                    Err(e) => println!("  查询订单失败: {e}"),
                }
            }
            Err(e) => println!("  下单失败: {e}"),
        }
        println!();
    }

    // 汇总
    println!("{}", "=".repeat(72));
    println!("  汇总 ({total_rounds} 笔真实成交)");
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
    println!(
        "  市价单 ACK:   min={:.2}ms avg={:.2}ms max={:.2}ms",
        a_min / 1000.0, a_avg / 1000.0, a_max / 1000.0
    );
    if !fill_latency.is_empty() {
        println!(
            "  下单→FILLED:  min={:.2}ms avg={:.2}ms max={:.2}ms",
            f_min / 1000.0, f_avg / 1000.0, f_max / 1000.0
        );
    }
    println!(
        "  0ms簿 local-E: min={:.2}ms avg={:.2}ms max={:.2}ms",
        e_min, e_avg, e_max
    );
    println!(
        "  0ms簿 local-T: min={:.2}ms avg={:.2}ms max={:.2}ms",
        t_min, t_avg, t_max
    );
    println!(
        "  价格一致性: {price_matches}/{total_rounds} 笔成交价≈本地簿BBO"
    );
    println!("{}", "=".repeat(72));

    Ok(())
}
