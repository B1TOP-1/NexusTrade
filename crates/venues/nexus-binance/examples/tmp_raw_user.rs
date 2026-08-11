//! 临时：裸连接用户流 + REST 下单 + 收 NEW（排除 spawn_reader 时序）。
use std::time::Duration;
use futures_util::{SinkExt, StreamExt};
use nexus_binance::auth::sign;

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

    // 1. listenKey
    let resp: serde_json::Value = http.post(format!("{rest_url}/fapi/v1/listenKey"))
        .header("X-MBX-APIKEY", &key).send().await.unwrap().json().await.unwrap();
    let lk = resp["listenKey"].as_str().unwrap().to_string();
    let url = format!("wss://fstream.binance.com/private/ws/{lk}");
    println!("listenKey: {}...", &lk[..12]);

    // 2. 裸连接（同步等连接完成）
    println!("裸连接...");
    let (ws, _) = tokio_tungstenite::connect_async(&url).await.expect("connect fail");
    let (mut write, mut read) = ws.split();
    println!("裸连接成功 ✓");

    // 3. REST 下单
    let ts = chrono::Utc::now().timestamp_millis();
    let cid = format!("nxraw{}", ts % 100000);
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
    let resp2 = http.post(format!("{rest_url}/fapi/v1/order"))
        .header("X-MBX-APIKEY", &key)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(full)
        .send().await.unwrap();
    let body = resp2.text().await.unwrap();
    let v: serde_json::Value = serde_json::from_str(&body).unwrap_or(serde_json::json!({}));
    println!("REST下单: orderId={:?} cid={cid}", v.get("orderId").map(|x| x.as_u64().unwrap_or(0)));

    // 4. 等用户流 NEW（裸连接直接收）
    println!("等用户流 NEW (15s)...");
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut found = false;
    while std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(2), read.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                let s = text.to_string();
                println!("收到: {}", &s[..s.len().min(120)]);
                if s.contains("ORDER_TRADE_UPDATE") && s.contains(&cid) {
                    println!("★ 匹配到订单 {cid}!");
                    found = true;
                    break;
                }
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(e))) => { println!("err: {e}"); break; }
            Ok(None) => { println!("closed"); break; }
            Err(_) => println!("2s 无消息..."),
        }
    }
    if !found { println!("✗ 未收到订单确认"); }

    // 5. 撤单
    if let Some(oid) = v.get("orderId").and_then(|x| x.as_u64()) {
        let ts2 = chrono::Utc::now().timestamp_millis();
        let mut cp = vec![
            ("symbol", "BTCUSDT".to_string()),
            ("orderId", oid.to_string()),
            ("timestamp", ts2.to_string()),
        ];
        cp.sort_by(|a, b| a.0.cmp(&b.0));
        let cq = cp.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
        let csig = sign(&cq, &secret);
        let cfull = format!("{cq}&signature={csig}");
        let _ = http.delete(format!("{rest_url}/fapi/v1/order"))
            .header("X-MBX-APIKEY", &key)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(cfull).send().await;
        println!("已发撤单 orderId={oid}");
        // 等 CANCELED
        let d2 = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < d2 {
            match tokio::time::timeout(Duration::from_secs(2), read.next()).await {
                Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                    let s = text.to_string();
                    println!("收到: {}", &s[..s.len().min(120)]);
                    if s.contains("CANCELED") && s.contains(&cid) {
                        println!("★ CANCELED 确认!");
                        break;
                    }
                }
                _ => {}
            }
        }
    }
    let _ = write.send(tokio_tungstenite::tungstenite::Message::Close(None)).await;
    println!("done");
}
