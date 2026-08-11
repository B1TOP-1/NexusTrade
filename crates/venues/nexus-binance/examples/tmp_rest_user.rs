//! 临时最小测试：REST 下单 + 用户流确认。
//! 只测一件事——REST 下单后用户流能否收到 NEW。

use std::time::{Duration, Instant};

use nexus_binance::auth::sign;
use nexus_binance::ws;
use nexus_core::Decimal;
use rust_decimal_macros::dec;

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
async fn main() {
    load_dotenv();
    let key = std::env::var("BINANCE_API_KEY").unwrap_or_default();
    let secret = std::env::var("BINANCE_API_SECRET").unwrap_or_default();
    let http = reqwest::Client::new();
    let rest_url = "https://fapi.binance.com";

    // 1. 拿 listenKey
    let resp: serde_json::Value = http.post(format!("{rest_url}/fapi/v1/listenKey"))
        .header("X-MBX-APIKEY", &key).send().await.unwrap().json().await.unwrap();
    let lk = resp["listenKey"].as_str().unwrap().to_string();
    println!("[1] listenKey: {}...", &lk[..12]);

    // 2. 连用户流
    let ws_url = format!("wss://fstream.binance.com/private/ws/{lk}");
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let (_stx, srx) = tokio::sync::watch::channel(false);
    let (session, write_tx) = ws::spawn_reader(&ws_url, tx, srx, Duration::from_millis(500)).await.unwrap();
    println!("[2] 用户流 spawn_reader 返回 (连接后台进行)");
    let _keep = (session, write_tx);

    // 3. 等用户流连接建立
    tokio::time::sleep(Duration::from_secs(3)).await;
    println!("[3] 已等 3s");

    // 4. REST 下单
    let ts = chrono::Utc::now().timestamp_millis();
    let cid = format!("nxrest{}", ts % 100000);
    let mut params = vec![
        ("symbol", "BTCUSDT".to_string()),
        ("side", "BUY".to_string()),
        ("type", "LIMIT".to_string()),
        ("timeInForce", "GTX".to_string()),
        ("quantity", "0.001".to_string()),
        ("price", "63600.0".to_string()),
        ("newClientOrderId", cid.clone()),
        ("timestamp", ts.to_string()),
    ];
    params.sort_by(|a, b| a.0.cmp(&b.0));
    let query = params.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
    let sig = sign(&query, &secret);
    let full = format!("{query}&signature={sig}");
    let t0 = Instant::now();
    let resp2 = http.post(format!("{rest_url}/fapi/v1/order"))
        .header("X-MBX-APIKEY", &key)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(full)
        .send().await.unwrap();
    let body = resp2.text().await.unwrap();
    let v: serde_json::Value = serde_json::from_str(&body).unwrap_or(serde_json::json!({}));
    println!("[4] REST下单: {:?} {}ms", v.get("orderId").map(|x| x.as_u64().unwrap_or(0)), t0.elapsed().as_millis());
    println!("    cid={} resp={}", cid, &body[..body.len().min(200)]);

    // 5. 等用户流 NEW（20s）
    println!("[5] 等用户流 NEW (20s)...");
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), rx.recv()).await {
            Ok(Some(msg)) => {
                println!("    [user] 收到: {}", &msg[..msg.len().min(150)]);
                if msg.contains("ORDER_TRADE_UPDATE") && msg.contains(&cid) {
                    println!("    ★ 匹配到我们的订单!");
                    break;
                }
            }
            Ok(None) => { println!("    channel closed"); break; }
            Err(_) => println!("    1s 无消息..."),
        }
    }
    println!("done");
}
