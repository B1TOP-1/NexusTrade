//! 临时：用 tokio-tungstenite 裸连接用户流，对比 spawn_reader。
use std::time::Duration;
use futures_util::StreamExt;

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
    let http = reqwest::Client::new();
    let resp: serde_json::Value = http.post("https://fapi.binance.com/fapi/v1/listenKey")
        .header("X-MBX-APIKEY", &key).send().await.unwrap().json().await.unwrap();
    let lk = resp["listenKey"].as_str().unwrap().to_string();
    let url = format!("wss://fstream.binance.com/private/ws/{lk}");
    println!("listenKey: {}...", &lk[..12]);

    // tokio-tungstenite 裸连接（直连优先，无代理）
    println!("裸连接 {url}...");
    match tokio_tungstenite::connect_async(&url).await {
        Ok((ws, _)) => {
            println!("裸连接成功 ✓");
            let (_, mut read) = ws.split();
            let start = std::time::Instant::now();
            while start.elapsed() < Duration::from_secs(10) {
                match tokio::time::timeout(Duration::from_secs(2), read.next()).await {
                    Ok(Some(Ok(msg))) => {
                        println!("[{:.0}s] 收到: {}", start.elapsed().as_secs(), &msg.to_string()[..msg.to_string().len().min(100)]);
                    }
                    Ok(Some(Err(e))) => { println!("err: {e}"); break; }
                    Ok(None) => { println!("closed"); break; }
                    Err(_) => println!("[{:.0}s] 2s 无消息", start.elapsed().as_secs()),
                }
            }
        }
        Err(e) => println!("裸连接失败: {e}"),
    }
    println!("done");
}
