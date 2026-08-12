// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Gate 撤单测试器: 验证「按 text 撤单」与「按 venue 单号撤单」都被 Gate 接受,
//! 并用 get_open_orders 查询确认挂单/已撤。
//!
//! 流程: 连接->登录 -> 挂远价 post-only 单A -> 查在挂 -> 按 **text** 撤A -> 查没了
//!       -> 挂单B -> 查在挂 -> 按 **venue 单号** 撤B -> 查没了。
//!
//! ```text
//! cargo run --example gate-cancel-tester --package nautilus-gate -- --contract BTC_USDT --price 50000 --armed
//! ```
//! 凭证 .env: GATE_API_KEY, GATE_API_SECRET。`--armed` 才真实挂/撤单。

use std::{
    collections::HashMap,
    env, fs,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::StreamExt;
use nexus_gate::{
    common::consts::GATE_WS_SIZE_DECIMAL_HEADER,
    config::GateExecutionClientConfig,
    http::client::GateHttpClient,
    websocket::{client::GateWebSocketClient, messages::GateWsEventMessage},
};
use nautilus_model::identifiers::{AccountId, TraderId};

struct StderrLogger;
impl log::Log for StderrLogger {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }
    fn log(&self, record: &log::Record) {
        eprintln!("[{}] {}", record.level(), record.args());
    }
    fn flush(&self) {}
}
static LOGGER: StderrLogger = StderrLogger;

fn load_env_file(path: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let Ok(content) = fs::read_to_string(path) else {
        return map;
    };
    for line in content.lines() {
        let line = line.trim().strip_prefix("export ").unwrap_or(line.trim());
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            map.insert(
                k.trim().to_string(),
                v.trim().trim_matches('"').trim_matches('\'').to_string(),
            );
        }
    }
    map
}

fn unix_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// 查询并打印当前挂单, 返回匹配 `text` 的订单 venue id(若在挂)。
async fn query_open(
    http: &GateHttpClient,
    settle: &str,
    contract: &str,
    credential: &nexus_gate::common::credential::GateCredential,
    want_text: &str,
    label: &str,
) -> Option<String> {
    match http.get_open_orders(settle, contract, credential).await {
        Ok(orders) => {
            println!("[查询挂单·{label}] 共 {} 个挂单", orders.len());
            let mut found = None;
            for o in &orders {
                let id = o
                    .get("id")
                    .map(|v| v.to_string())
                    .unwrap_or_default()
                    .trim_matches('"')
                    .to_string();
                let text = o.get("text").and_then(|v| v.as_str()).unwrap_or("");
                let price = o.get("price").map(std::string::ToString::to_string).unwrap_or_default();
                let left = o.get("left").map(std::string::ToString::to_string).unwrap_or_default();
                println!("    - id={id} text={text} price={price} left={left}");
                if text == want_text {
                    found = Some(id);
                }
            }
            match &found {
                Some(id) => println!("[查询挂单·{label}] 目标 text={want_text} 在挂 (venue id={id})"),
                None => println!("[查询挂单·{label}] 目标 text={want_text} 不在挂单中"),
            }
            found
        }
        Err(e) => {
            println!("[查询挂单·{label}] get_open_orders 失败: {e}");
            None
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = log::set_logger(&LOGGER);
    log::set_max_level(log::LevelFilter::Info);

    let argv: Vec<String> = env::args().collect();
    let mut contract = "BTC_USDT".to_string();
    let mut price = "50000".to_string(); // 远低于市价的 post-only 买单, 必挂不成交
    let mut armed = false;
    let mut probe = false;
    let mut env_file = ".env".to_string();
    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--contract" => {
                contract = argv[i + 1].clone();
                i += 2;
            }
            "--price" => {
                price = argv[i + 1].clone();
                i += 2;
            }
            "--env-file" => {
                env_file = argv[i + 1].clone();
                i += 2;
            }
            "--armed" => {
                armed = true;
                i += 1;
            }
            "--probe" => {
                probe = true;
                armed = true;
                i += 1;
            }
            other => anyhow::bail!("unknown arg: {other}"),
        }
    }

    let env_map = load_env_file(&env_file);
    let get = |k: &str| -> anyhow::Result<String> {
        env_map
            .get(k)
            .cloned()
            .or_else(|| env::var(k).ok())
            .ok_or_else(|| anyhow::anyhow!("missing env key {k}"))
    };
    let api_key = get("GATE_API_KEY")?;
    let api_secret = get("GATE_API_SECRET")?;

    let config = GateExecutionClientConfig {
        trader_id: TraderId::from("GATE-CXL-001"),
        account_id: AccountId::from("GATE-001"),
        api_key: Some(api_key),
        api_secret: Some(api_secret),
        contracts: vec![contract.clone()],
        ..Default::default()
    };
    let credential = config
        .credential()
        .ok_or_else(|| anyhow::anyhow!("missing credential"))?;
    let settle = config.settle.clone();

    let http = GateHttpClient::new(Some(config.http_url()), Some(10), config.proxy_url.clone())?;

    if !armed {
        println!("未 armed: 仅查询当前挂单, 不挂/撤单。加 --armed 执行完整测试。");
        query_open(&http, &settle, &contract, &credential, "", "只读").await;
        return Ok(());
    }

    let mut ws = GateWebSocketClient::new(
        config.ws_url(),
        config.heartbeat_interval_secs,
        config.transport_backend,
        config.proxy_url.clone(),
    )
    .with_header(GATE_WS_SIZE_DECIMAL_HEADER.0, GATE_WS_SIZE_DECIMAL_HEADER.1);
    ws.connect().await?;

    let ts = unix_seconds();
    let signature = credential.sign_ws_api("futures.login", "", ts);
    ws.login(credential.api_key(), &signature, "login-1", ts)
        .await?;

    // 读取流并打印所有 WS-API 响应(供观察 order_place/order_cancel 结果)。
    let buf = Arc::new(Mutex::new(Vec::<String>::new()));
    let buf2 = buf.clone();
    let mut stream = Box::pin(ws.stream());
    tokio::spawn(async move {
        while let Some(msg) = stream.next().await {
            if let GateWsEventMessage::Raw(t) = msg {
                println!("[WS响应] {t}");
                buf2.lock().expect("lock").push(t);
            }
        }
    });
    tokio::time::sleep(Duration::from_secs(2)).await;
    println!("登录完成。");

    // text 长度探测: 挂不同长度的 text, 看 Gate 接受到多长(在挂=接受)。
    if probe {
        println!("\n===== text 长度探测 =====");
        let lens = [28usize, 30, 32, 34, 36, 38, 40, 42, 44, 48];
        for len in lens {
            let mut text = format!("t-L{len}-");
            while text.len() < len {
                text.push('x');
            }
            text.truncate(len);
            let p = serde_json::json!({
                "contract": contract, "size": 1, "price": price, "tif": "poc", "text": text
            });
            ws.send_api("futures.order_place", &format!("probe-{len}"), &p, unix_seconds())
                .await?;
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
        println!("--- 被 Gate 接受(在挂)的 text 长度 ---");
        let orders = http
            .get_open_orders(&settle, &contract, &credential)
            .await
            .unwrap_or_default();
        let mut max_len = 0usize;
        for o in &orders {
            let text = o.get("text").and_then(|v| v.as_str()).unwrap_or("");
            println!("  接受: 长度={} text={text}", text.len());
            max_len = max_len.max(text.len());
        }
        println!(
            "探测 {} 个长度, {} 个被接受, 实测最大可用 text 长度 = {max_len}",
            lens.len(),
            orders.len()
        );
        for o in &orders {
            if let Some(id) = o.get("id") {
                let vid = id.to_string().trim_matches('"').to_string();
                ws.send_api(
                    "futures.order_cancel",
                    "cleanup",
                    &serde_json::json!({ "order_id": vid }),
                    unix_seconds(),
                )
                .await?;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
        ws.close().await?;
        println!("探测完成, 已清理挂单。");
        return Ok(());
    }

    println!("开始撤单测试 (contract={contract} price={price})");

    let ts_tag = unix_seconds();
    let text_a = format!("t-cxla-{ts_tag}");
    let text_b = format!("t-cxlb-{ts_tag}");

    // ---- A: 挂单 -> 查在挂 -> 按 text 撤 -> 查没了 ----
    println!("\n===== 测试1: 按 text 撤单 =====");
    let place_a = serde_json::json!({
        "contract": contract, "size": 1, "price": price, "tif": "poc", "text": text_a
    });
    ws.send_api("futures.order_place", "place-a", &place_a, unix_seconds())
        .await?;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    query_open(&http, &settle, &contract, &credential, &text_a, "挂单A后").await;

    let cancel_a = serde_json::json!({ "order_id": text_a });
    println!("[撤单·按text] order_id={text_a}");
    ws.send_api("futures.order_cancel", "cancel-a", &cancel_a, unix_seconds())
        .await?;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let still_a = query_open(&http, &settle, &contract, &credential, &text_a, "撤单A后").await;
    println!(
        "[结论1] 按 text 撤单 {}",
        if still_a.is_none() { "成功 ✓" } else { "失败 ✗(仍在挂)" }
    );

    // ---- B: 挂单 -> 查在挂(取 venue id) -> 按 venue id 撤 -> 查没了 ----
    println!("\n===== 测试2: 按 venue 单号撤单 =====");
    let place_b = serde_json::json!({
        "contract": contract, "size": 1, "price": price, "tif": "poc", "text": text_b
    });
    ws.send_api("futures.order_place", "place-b", &place_b, unix_seconds())
        .await?;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let venue_b = query_open(&http, &settle, &contract, &credential, &text_b, "挂单B后").await;

    if let Some(vid) = venue_b {
        let cancel_b = serde_json::json!({ "order_id": vid });
        println!("[撤单·按venue] order_id={vid}");
        ws.send_api("futures.order_cancel", "cancel-b", &cancel_b, unix_seconds())
            .await?;
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let still_b = query_open(&http, &settle, &contract, &credential, &text_b, "撤单B后").await;
        println!(
            "[结论2] 按 venue 单号撤单 {}",
            if still_b.is_none() { "成功 ✓" } else { "失败 ✗(仍在挂)" }
        );
    } else {
        println!("[结论2] B 未查到 venue 单号, 跳过 venue 撤单(检查挂单是否成功)");
    }

    // ---- C: 下单后【立即】按 text 撤(不等任何确认), 探测「venue未反应」竞速 ----
    println!("\n===== 测试3: 下单后立即按text撤(探测下单未确认竞速) =====");
    let text_c = format!("t-cxlc-{ts_tag}");
    let place_c = serde_json::json!({
        "contract": contract, "size": 1, "price": price, "tif": "poc", "text": text_c
    });
    let now = unix_seconds();
    ws.send_api("futures.order_place", "place-c", &place_c, now)
        .await?;
    // 不等待, 背靠背立即按 text 撤(同一 WS 连接, Gate 按序处理 place->cancel)。
    ws.send_api(
        "futures.order_cancel",
        "cancel-c",
        &serde_json::json!({ "order_id": text_c }),
        now,
    )
    .await?;
    println!("[测试3] 已背靠背发送 place-c + cancel-c(按text), 看下方 cancel-c 响应");
    tokio::time::sleep(Duration::from_millis(2000)).await;
    let still_c = query_open(&http, &settle, &contract, &credential, &text_c, "测试3后").await;
    println!(
        "[结论3] 下单后立即按 text 撤: {} (若 cancel-c 响应是 cancelled 即无竞速问题)",
        if still_c.is_none() { "撤掉了 ✓" } else { "未撤 ✗" }
    );
    // 清理: 若 C 仍在挂, 按 venue 单号补撤。
    if let Some(vid) = still_c {
        println!("[清理] C 仍在挂, 按 venue 补撤 order_id={vid}");
        ws.send_api(
            "futures.order_cancel",
            "cleanup-c",
            &serde_json::json!({ "order_id": vid }),
            unix_seconds(),
        )
        .await?;
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }

    ws.close().await?;
    println!("\n撤单测试完成。");
    Ok(())
}
