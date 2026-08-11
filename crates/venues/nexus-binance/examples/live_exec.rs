//! binance-futures-rs 集成示例：交易 + 账户管理 + WebSocket 流。
//!
//! 覆盖三大能力（走 binance-futures-rs 库）：
//!   1. 账户管理  — account_info / balance / position_risk
//!   2. 行情      — REST depth + WebSocket depth 流
//!   3. 交易      — REST 下单（GTX post-only 远离盘口，不成交）→ 查询 → 撤单
//!
//! 凭据从环境变量读取（严禁硬编码）。优先从 `.env` 文件加载（自动查找工作目录向上两级）：
//!   cp .env.example .env   # 填入 BINANCE_API_KEY / BINANCE_API_SECRET
//!
//! 用法：
//!   cargo run -p nexus-binance --example live_exec                       # 只读：账户 + 行情
//!   cargo run -p nexus-binance --example live_exec -- --place            # + post-only 下单 + 撤单
//!   cargo run -p nexus-binance --example live_exec -- --symbol ETHUSDT --qty 0.01 --side buy
//!   cargo run -p nexus-binance --example live_exec -- --testnet          # 测试网
//!
//! 安全设计：
//!   - 默认只读，仅 `--place` 才下真实单
//!   - 下单用 GTX(post-only) + 价格偏离盘口 --offset-pct（默认 0.5%）→ 永不成交
//!   - 下单后自动查询确认 + 撤单，不留挂单

use std::time::Duration;

use binance_futures_rs::websocket::{StreamBuilder, WebSocketClient, WebSocketMessage};
use binance_futures_rs::{
    BinanceClient, CancelOrderRequest, Credentials, NewOrderRequest, OrderSide, OrderType,
    QueryOrderRequest, TimeInForce,
};
use futures_util::StreamExt;

// ── .env 加载（极简实现，不引 dotenv 依赖）──

fn load_dotenv() {
    // cargo run 的工作目录是 workspace 根；向上两级兜底从 crate 目录执行的情况。
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
                let key = k.trim();
                let val = v.trim();
                // 已设置的环境变量优先，不覆盖（shell export 优先于 .env）。
                if std::env::var(key).is_err() {
                    std::env::set_var(key, val);
                }
            }
        }
        eprintln!("[live_exec] loaded {path}");
        break;
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

// ── 解析参数 ──

struct Args {
    place: bool,
    symbol: String,
    qty: f64,
    side: OrderSide,
    offset_pct: f64,
    testnet: bool,
}

fn parse_args() -> Args {
    let mut args = Args {
        place: false,
        symbol: "BTCUSDT".to_string(),
        qty: 0.001,
        side: OrderSide::Sell,
        offset_pct: 0.5,
        testnet: false,
    };
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
            "--place" => args.place = true,
            "--testnet" => args.testnet = true,
            "--symbol" => {
                if i + 1 < raw.len() {
                    args.symbol = raw[i + 1].to_uppercase();
                    i += 1;
                }
            }
            "--qty" => {
                if i + 1 < raw.len() {
                    args.qty = raw[i + 1].parse().unwrap_or(args.qty);
                    i += 1;
                }
            }
            "--side" => {
                if i + 1 < raw.len() {
                    args.side = match raw[i + 1].to_lowercase().as_str() {
                        "buy" => OrderSide::Buy,
                        _ => OrderSide::Sell,
                    };
                    i += 1;
                }
            }
            "--offset-pct" => {
                if i + 1 < raw.len() {
                    args.offset_pct = raw[i + 1].parse().unwrap_or(args.offset_pct);
                    i += 1;
                }
            }
            "--help" | "-h" => {
                println!(
                    "Usage: live_exec [--place] [--symbol BTCUSDT] [--qty 0.001] \
                     [--side buy|sell] [--offset-pct 0.5] [--testnet]"
                );
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }
    args
}

// ── 只读：账户管理 ──

async fn print_account(client: &BinanceClient, symbol: &str) {
    println!("\n────────── 账户管理 (Account) ──────────");
    match client.account().account_info().await {
        Ok(info) => {
            println!(
                "  钱包余额: {:.2} USDT | 可用: {:.2} | 未实现盈亏: {:.2}",
                info.total_wallet_balance.parse::<f64>().unwrap_or(0.0),
                info.available_balance.parse::<f64>().unwrap_or(0.0),
                info.total_unrealized_pnl.parse::<f64>().unwrap_or(0.0),
            );
        }
        Err(e) => println!("  账户信息获取失败: {e}"),
    }

    match client.account().balance().await {
        Ok(balances) => {
            let usdt: Vec<_> = balances
                .iter()
                .filter(|b| b.asset == "USDT")
                .collect();
            for b in usdt {
                println!(
                    "  余额[{:>4}] 总额: {:<12.4} 可用: {:<12.4}",
                    b.asset,
                    b.balance.parse::<f64>().unwrap_or(0.0),
                    b.available_balance.parse::<f64>().unwrap_or(0.0),
                );
            }
        }
        Err(e) => println!("  余额获取失败: {e}"),
    }

    match client.account().position_risk(Some(symbol)).await {
        Ok(positions) => {
            let active: Vec<_> = positions
                .iter()
                .filter(|p| p.position_amt.parse::<f64>().unwrap_or(0.0) != 0.0)
                .collect();
            if active.is_empty() {
                println!("  {symbol} 无持仓");
            } else {
                for p in active {
                    println!(
                        "  持仓[{symbol}] 数量: {} 开仓价: {} 未实现盈亏: {} 杠杆: {}x",
                        p.position_amt, p.entry_price, p.un_realized_pnl, p.leverage
                    );
                }
            }
        }
        Err(e) => println!("  持仓获取失败: {e}"),
    }
}

// ── 行情：REST + WebSocket ──

async fn print_depth_rest(client: &BinanceClient, symbol: &str) -> (f64, f64) {
    println!("\n────────── 行情 (Market Data) ──────────");
    let mut best_bid = 0.0;
    let mut best_ask = 0.0;
    match client.market().depth(symbol, Some(5)).await {
        Ok(book) => {
            if let Some(b) = book.bids.first() {
                best_bid = b[0].parse::<f64>().unwrap_or(0.0);
                println!("  REST 盘口: bid {} x {}", b[0], b[1]);
            }
            if let Some(a) = book.asks.first() {
                best_ask = a[0].parse::<f64>().unwrap_or(0.0);
                println!("  REST 盘口: ask {} x {}", a[0], a[1]);
            }
        }
        Err(e) => println!("  REST 深度获取失败: {e}"),
    }
    (best_bid, best_ask)
}

async fn ws_depth_demo(symbol: &str) {
    println!("\n────────── 行情 (WebSocket 流, 5 秒演示) ──────────");
    let ws = match tokio::time::timeout(
        Duration::from_secs(8),
        StreamBuilder::new().depth(symbol, Some(5)).connect(),
    )
    .await
    {
        Ok(Ok(ws)) => ws,
        Ok(Err(e)) => {
            println!("  WS 连接失败（VPS 直连可用；本地需走代理则跳过）: {e}");
            return;
        }
        Err(_) => {
            println!("  WS 连接超时，跳过");
            return;
        }
    };

    let mut ws = ws;
    let mut count = 0usize;
    while count < 5 {
        let msg = tokio::time::timeout(Duration::from_secs(3), ws.next()).await;
        let Ok(Some(Ok(text))) = msg else { break };
        let text = match text.into_text() {
            Ok(t) => t,
            Err(_) => continue,
        };
        match WebSocketClient::parse_message(&text.to_string()) {
            Ok(WebSocketMessage::DepthUpdate(depth)) => {
                let best_bid = depth.bids.first().map(|b| b[0].as_str()).unwrap_or("-");
                let best_ask = depth.asks.first().map(|a| a[0].as_str()).unwrap_or("-");
                println!("  [WS] {symbol} bid={best_bid} ask={best_ask}  U={}", depth.first_update_id);
                count += 1;
            }
            _ => continue,
        }
    }
    println!("  WS 行情演示结束（{count} 帧）");
}

// ── 交易：REST 下单（post-only 不成交）→ 查询 → 撤单 ──

async fn place_and_cancel(
    client: &BinanceClient,
    symbol: &str,
    qty: f64,
    side: OrderSide,
    offset_pct: f64,
    best_bid: f64,
    best_ask: f64,
) {
    println!("\n────────── 交易 (REST 下单, post-only) ──────────");
    let reference = if matches!(side, OrderSide::Buy) {
        best_ask
    } else {
        best_bid
    };
    if reference <= 0.0 {
        println!("  ⚠ 无法获取参考价，跳过下单（先跑只读模式确认行情）。");
        return;
    }

    // 远离盘口的价格：buy 挂在 ask 下方 --offset%，sell 挂在 bid 上方 +offset%。
    let price = if matches!(side, OrderSide::Buy) {
        reference * (1.0 - offset_pct / 100.0)
    } else {
        reference * (1.0 + offset_pct / 100.0)
    };
    // 量化到 tick（0.10），避免 -1111 精度超限。
    let price = (price / 0.10).round() * 0.10;
    let price_str = format!("{price:.2}");
    let qty_str = format!("{qty}");

    let client_id = format!("nx-liveexec-{}", chrono::Utc::now().timestamp_millis());

    println!(
        "  ⚠ 下 {symbol} {} 限价单 {qty} @ {price_str}（GTX post-only，偏离 {offset_pct}%，不会成交）",
        if matches!(side, OrderSide::Buy) { "BUY" } else { "SELL" }
    );

    let order_req = NewOrderRequest::new(symbol.to_string(), side, OrderType::Limit)
        .quantity(qty_str.clone())
        .price(price_str)
        .time_in_force(TimeInForce::Gtx)
        .client_order_id(client_id.clone());

    let order = match client.trading().new_order(order_req).await {
        Ok(o) => o,
        Err(e) => {
            println!("  ✗ 下单失败: {e}");
            return;
        }
    };
    println!(
        "  ✓ 已下单: order_id={} client_id={} status={:?}",
        order.order_id, order.client_order_id, order.status
    );

    // 查询确认
    let query_req = QueryOrderRequest::new(symbol.to_string())
        .order_id(order.order_id);
    match client.trading().query_order(query_req).await {
        Ok(q) => {
            println!(
                "  ✓ 查询确认: status={:?} 成交量={}/{} 价格={}",
                q.status, q.executed_qty, q.orig_qty, q.price
            );
        }
        Err(e) => println!("  ✗ 查询失败: {e}"),
    }

    // 撤单（不留挂单）
    let cancel_req = CancelOrderRequest::new(symbol.to_string())
        .order_id(order.order_id);
    match client.trading().cancel_order(cancel_req).await {
        Ok(c) => println!("  ✓ 已撤单: status={:?}", c.status),
        Err(e) => println!("  ✗ 撤单失败: {e}"),
    }
}

// ── main ──

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    load_dotenv();
    let args = parse_args();

    let mode = if args.testnet { "TESTNET" } else { "MAINNET" };
    println!("{}", "=".repeat(60));
    println!("  binance-futures-rs 集成示例");
    println!("  Symbol: {}  Network: {mode}  Side: {:?}", args.symbol, args.side);
    println!("{}", "=".repeat(60));

    // 凭据：测试网 / 主网
    let (key, secret) = if args.testnet {
        (
            env_or("BINANCE_TESTNET_KEY", ""),
            env_or("BINANCE_TESTNET_SECRET", ""),
        )
    } else {
        (
            env_or("BINANCE_API_KEY", ""),
            env_or("BINANCE_API_SECRET", ""),
        )
    };

    if key.is_empty() || secret.is_empty() {
        println!("\n⚠ 未找到 API Key。请创建 .env：\n    cp .env.example .env\n    填入 BINANCE_API_KEY / BINANCE_API_SECRET");
        return Ok(());
    }

    let client = if args.testnet {
        BinanceClient::testnet_with_credentials(Credentials::new(key, secret))
    } else {
        BinanceClient::new_with_credentials(Credentials::new(key, secret))
    };

    // 1. 账户
    print_account(&client, &args.symbol).await;

    // 2. 行情 REST
    let (best_bid, best_ask) = print_depth_rest(&client, &args.symbol).await;

    // 3. 交易（--place 才真实下单）
    if args.place {
        place_and_cancel(
            &client,
            &args.symbol,
            args.qty,
            args.side,
            args.offset_pct,
            best_bid,
            best_ask,
        )
        .await;
    } else {
        println!("\n────────── 交易 ──────────");
        println!("  （只读模式，未下单。加 --place 执行 post-only 下单演示）");
    }

    // 4. 行情 WS 演示
    ws_depth_demo(&args.symbol).await;

    println!("\nDone.");
    Ok(())
}
