//! ws-fapi WebSocket 下单验证。
//!
//! 架构底线①：下单/撤单走 WS。用 WsFapiClient 真实下单（post-only 远离盘口不成交）。
//!
//! 用法：
//!   cargo run -p nexus-binance --example ws_place                      # post-only 下单+撤单
//!   cargo run -p nexus-binance --example ws_place -- --symbol ETHUSDT   # 换币种
//!   cargo run -p nexus-binance --example ws_place -- --testnet          # 测试网
//!
//! 凭据：.env → BINANCE_API_KEY / BINANCE_API_SECRET

use std::time::Instant;

use nexus_binance::WsFapiClient;
use nexus_core::{ClientOrderId, NewOrder, Side, Symbol};
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    load_dotenv();

    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut symbol = "BTCUSDT".to_string();
    let mut testnet = false;
    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
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

    println!("{}", "=".repeat(60));
    println!("  ws-fapi WebSocket 下单验证");
    println!(
        "  Symbol: {symbol}  Network: {}",
        if testnet { "TESTNET" } else { "MAINNET" }
    );
    println!("{}", "=".repeat(60));

    // 连接 ws-fapi
    println!("连接 ws-fapi...");
    let client = WsFapiClient::connect(key, secret, testnet).await?;
    println!("连接成功 ✓");

    // 下单（post-only 远离盘口，不成交）
    let sym = Symbol::new(symbol.replace("USDT", ""), "USDT", symbol.clone());
    let order = NewOrder::limit(
        sym.clone(),
        Side::Buy,
        dec!(63600), // 远离盘口，post-only 不会成交
        dec!(0.001),
        ClientOrderId(format!("nxws{}", chrono::Utc::now().timestamp_millis())),
    )
    .post_only();

    let t0 = Instant::now();
    match client.place(&order).await {
        Ok(order_id) => {
            let ms = t0.elapsed().as_secs_f64() * 1000.0;
            println!("下单成功 ✓ orderId={order_id}  {ms:.1}ms");

            // 撤单
            let t1 = Instant::now();
            match client.cancel(&sym, order_id).await {
                Ok(()) => {
                    let cancel_ms = t1.elapsed().as_secs_f64() * 1000.0;
                    println!("撤单成功 ✓  {cancel_ms:.1}ms");
                }
                Err(e) => println!("撤单失败: {e}"),
            }
        }
        Err(e) => println!("下单失败: {e}"),
    }

    Ok(())
}
