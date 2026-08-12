//! Gate 集成冒烟测试：三大 trait 组装。
//!
//! 用 Nexus builder 注册 Gate（行情+交易+私有流），验证本地订单簿和连接。
//! 需 GATE_API_KEY / GATE_API_SECRET（.env 或环境变量）。
//!
//! 用法：
//!   cargo run --example gate_smoke --features gate -- --symbol BTC_USDT

use std::sync::Arc;
use std::time::Duration;

use nexus_sdk::{BookOptions, GateMarket, GateMarketConfig, MarketVenue, PrivateVenue};

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    load_dotenv();
    let symbol = std::env::args().nth(1).unwrap_or_else(|| "BTC_USDT".to_string());

    println!("{}", "=".repeat(60));
    println!("  Gate 集成冒烟测试");
    println!("  Symbol: {symbol}");
    println!("{}", "=".repeat(60));

    // 行情 venue
    let market = Arc::new(GateMarket::new(GateMarketConfig::default()));
    println!("[1] GateMarket 创建 ✓ (20ms 订单簿)");

    // 订阅本地订单簿
    let sym = nexus_sdk::Symbol::new("BTC", "USDT", symbol.clone());
    println!("[2] 订阅本地订单簿 {symbol}...");
    let book = market
        .subscribe_book(&sym, BookOptions::default())
        .await?;

    // 等簿就绪
    let t0 = std::time::Instant::now();
    loop {
        if book.top().is_some() {
            break;
        }
        if t0.elapsed() > Duration::from_secs(15) {
            println!("  超时等待簿就绪（需公网可连 Gate）");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // 打印 BBO
    let top = book.top().unwrap();
    println!("[3] 簿就绪 ✓ BBO: bid={} x {} | ask={} x {}", top.bid, top.bid_qty, top.ask, top.ask_qty);

    // 私有流（需 API Key）
    let key = std::env::var("GATE_API_KEY").unwrap_or_default();
    let secret = std::env::var("GATE_API_SECRET").unwrap_or_default();
    if !key.is_empty() && !secret.is_empty() {
        println!("[4] 连接 Gate 私有流...");
        let exec = nexus_sdk::GateVenue::new(nexus_sdk::GateExecConfig {
            ws_url: "wss://fx-ws.gateio.ws/v4/ws/usdt".to_string(),
            api_key: key.clone(),
            api_secret: secret.clone(),
        });
        let private = Arc::new(nexus_sdk::GatePrivate::new(
            nexus_sdk::GatePrivateConfig {
                ws_url: "wss://fx-ws.gateio.ws/v4/ws/usdt".to_string(),
                api_key: key,
                api_secret: secret,
                reconnect_delay: Duration::from_millis(500),
            },
            exec,
        ));
        let mut stream = private.subscribe().await?;
        println!("  私有流已连接 ✓ (收事件验证)");

        // 收 2 个事件或超时
        for _ in 0..2 {
            match tokio::time::timeout(Duration::from_secs(5), stream.recv()).await {
                Ok(Some(ev)) => println!("  收到事件: {ev:?}"),
                Ok(None) => break,
                Err(_) => {
                    println!("  5s 无事件（无订单活动，正常）");
                    break;
                }
            }
        }
    } else {
        println!("[4] 跳过私有流（无 GATE_API_KEY/GATE_API_SECRET）");
    }

    println!("Done.");
    Ok(())
}
