//! 下单全链路延迟明细（ws-fapi + 用户数据流）。
//!
//! 测量策略发指令到网卡推出的完整链路，含 WS 状态推送延迟：
//!
//!   下单链路:
//!     [策略发指令] → [写入WS队列] → [网卡推出] → [币安ACK] → [用户流NEW确认]
//!   撤单链路:
//!     [发起撤单] → [网卡推出] → [币安ACK] → [用户流CANCELED确认]
//!
//! 输出每个阶段的延迟（ms），含本地路径 + 网络 RTT + 交易所内部 + 用户流推送。
//!
//! 用法：
//!   cargo run -p nexus-binance --example latency_detail -- --rounds 5
//!   cargo run -p nexus-binance --example latency_detail -- --testnet

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nexus_binance::auth::sign;
use nexus_core::{ClientOrderId, NewOrder, Side, Symbol};
use tokio::sync::{mpsc, oneshot};
use rust_decimal_macros::dec;

// 复用 nexus-binance 内部 API
use nexus_binance::ws;

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

struct Args {
    rounds: usize,
    symbol: String,
    testnet: bool,
}

fn parse_args() -> Args {
    let mut args = Args {
        rounds: 5,
        symbol: "BTCUSDT".to_string(),
        testnet: false,
    };
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
            "--rounds" => {
                if i + 1 < raw.len() {
                    args.rounds = raw[i + 1].parse().unwrap_or(args.rounds);
                    i += 1;
                }
            }
            "--symbol" => {
                if i + 1 < raw.len() {
                    args.symbol = raw[i + 1].to_uppercase();
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

/// 统计单轮延迟样本。
struct Stats {
    samples: Vec<f64>,
}

impl Stats {
    fn new() -> Self {
        Self { samples: Vec::new() }
    }
    fn push(&mut self, ms: f64) {
        self.samples.push(ms);
    }
    fn report(&self, label: &str) {
        if self.samples.is_empty() {
            println!("  {label:<28} 无样本");
            return;
        }
        let min = self.samples.iter().cloned().fold(f64::MAX, f64::min);
        let max = self.samples.iter().cloned().fold(0.0, f64::max);
        let avg = self.samples.iter().sum::<f64>() / self.samples.len() as f64;
        println!(
            "  {label:<28} min={min:>8.2}  avg={avg:>8.2}  max={max:>8.2}  (n={})",
            self.samples.len()
        );
    }
}

/// 往统计表 push 一个样本。
fn push(s: &mut HashMap<&str, Stats>, label: &str, value: f64) {
    if let Some(stats) = s.get_mut(label) {
        stats.push(value);
    }
}

/// ws-fapi 客户端（带网卡 hook 打点 + 详细阶段计时）。
struct FapiTracer {
    write_tx: mpsc::UnboundedSender<String>,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<serde_json::Value>>>>,
    api_key: String,
    api_secret: String,
    id_counter: AtomicU64,
    /// 网卡推出时刻（hook 写入，本地 epoch ms）
    wire_ts: Arc<Mutex<i64>>,
}

impl FapiTracer {
    async fn connect(api_key: String, api_secret: String, testnet: bool) -> Result<Self, String> {
        let url = if testnet {
            "wss://testnet.binancefuture.com/ws-fapi/v1"
        } else {
            "wss://ws-fapi.binance.com/ws-fapi/v1"
        };

        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let wire_ts: Arc<Mutex<i64>> = Arc::new(Mutex::new(0));

        // 网卡 hook：记录每次发送的真实时刻
        let wire_ts_hook = Arc::clone(&wire_ts);
        let hook: ws::WireHook = Arc::new(move |_msg| {
            *wire_ts_hook.lock().unwrap() = now_ms();
        });

        let (session, write_tx) = ws::spawn_reader_with_hook(
            &url,
            tx,
            shutdown_rx,
            Duration::from_millis(500),
            Some(hook),
        )
        .await
        .map_err(|e| e.to_string())?;

        let pending: Arc<Mutex<HashMap<String, oneshot::Sender<serde_json::Value>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // 后台：保活 + 路由响应
        let pending_reader = Arc::clone(&pending);
        tokio::spawn(async move {
            let _keep = (shutdown_tx, session);
            while let Some(msg) = rx.recv().await {
                let Ok(v) = serde_json::from_str::<serde_json::Value>(&msg) else {
                    continue;
                };
                let id = v["id"].as_str().unwrap_or("");
                let sender = pending_reader.lock().unwrap().remove(id);
                if let Some(s) = sender {
                    let _ = s.send(v);
                }
            }
        });

        Ok(Self {
            write_tx,
            pending,
            api_key,
            api_secret,
            id_counter: AtomicU64::new(1),
            wire_ts,
        })
    }

    /// 签名请求。返回 (响应, 网卡推出时刻)。
    async fn request(
        &self,
        method: &str,
        params: Vec<(String, String)>,
    ) -> Result<(serde_json::Value, i64), String> {
        let req_id = format!("trc-{}", self.id_counter.fetch_add(1, Ordering::Relaxed));

        let mut all = params;
        all.push((
            "timestamp".to_string(),
            chrono::Utc::now().timestamp_millis().to_string(),
        ));
        all.push(("apiKey".to_string(), self.api_key.clone()));
        all.sort_by(|a, b| a.0.cmp(&b.0));
        let query = all
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        let sig = sign(&query, &self.api_secret);

        let mut params_map = serde_json::Map::new();
        for (k, v) in all {
            params_map.insert(k, serde_json::Value::String(v));
        }
        params_map.insert("signature".to_string(), serde_json::Value::String(sig));
        let payload = serde_json::json!({"id": req_id, "method": method, "params": params_map});

        // 清零网卡时刻，标记发送前
        *self.wire_ts.lock().unwrap() = 0;
        self.write_tx.send(payload.to_string()).map_err(|e| e.to_string())?;

        let (resp_tx, resp_rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(req_id, resp_tx);
        let resp = resp_rx.await.map_err(|_| "response dropped".to_string())?;
        let wire = *self.wire_ts.lock().unwrap();
        Ok((resp, wire))
    }

    async fn place(&self, order: &NewOrder) -> Result<(serde_json::Value, i64), String> {
        let mut params = vec![
            ("symbol".to_string(), order.symbol.venue_native.clone()),
            ("side".to_string(), format!("{:?}", order.side).to_uppercase()),
            ("type".to_string(), "LIMIT".to_string()),
            ("quantity".to_string(), order.qty.to_string()),
            ("newClientOrderId".to_string(), order.client_id.0.clone()),
        ];
        // post-only → GTX
        params.push(("timeInForce".to_string(), "GTX".to_string()));
        if let Some(price) = order.price() {
            params.push(("price".to_string(), price.to_string()));
        }
        self.request("order.place", params).await
    }

    async fn cancel(&self, symbol: &str, order_id: u64) -> Result<(serde_json::Value, i64), String> {
        let params = vec![
            ("symbol".to_string(), symbol.to_string()),
            ("orderId".to_string(), order_id.to_string()),
        ];
        self.request("order.cancel", params).await
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 用户数据流：连接 listenKey 通道，接收 ORDER_TRADE_UPDATE。
/// 返回 (session, 消息流)。消息为原始 JSON 字符串。
async fn connect_user_stream(
    api_key: &str,
    rest_url: &str,
) -> Result<(ws::WsSession, mpsc::UnboundedReceiver<String>), String> {
    // 拿 listenKey（POST /fapi/v1/listenKey，带 API key header）
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
    let listen_key = resp["listenKey"]
        .as_str()
        .ok_or("listenKey missing")?
        .to_string();

    let ws_url = if rest_url.contains("testnet") {
        format!("wss://stream.binancefuture.com/private/ws/{listen_key}")
    } else {
        format!("wss://fstream.binance.com/private/ws/{listen_key}")
    };

    let (tx, rx) = mpsc::unbounded_channel::<String>();
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (session, _write) = ws::spawn_reader(&ws_url, tx, shutdown_rx, Duration::from_millis(500))
        .await
        .map_err(|e| e.to_string())?;
    Ok((session, rx))
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

    let rest_url = if args.testnet {
        "https://testnet.binancefuture.com"
    } else {
        "https://fapi.binance.com"
    };

    println!("{}", "=".repeat(72));
    println!("  下单全链路延迟明细 (ws-fapi + 用户数据流)");
    println!(
        "  Symbol: {}  Rounds: {}  Network: {}",
        args.symbol,
        args.rounds,
        if args.testnet { "TESTNET" } else { "MAINNET" }
    );
    println!("{}", "=".repeat(72));

    // 连接 ws-fapi（带网卡打点）
    println!("\n[1] 连接 ws-fapi...");
    let fapi = FapiTracer::connect(key.clone(), secret.clone(), args.testnet)
        .await
        .map_err(|e| format!("ws-fapi connect: {e}"))?;
    println!("    连接成功 ✓");

    // 连接用户数据流
    println!("[2] 连接用户数据流 (listenKey)...");
    let (user_session, mut user_rx) = connect_user_stream(&key, rest_url).await?;
    println!("    连接成功 ✓");
    let _keep_user = user_session;
    // 给用户流 WS 留出连接建立时间（直连4s超时→代理CONNECT→TLS，需较长时间）
    tokio::time::sleep(Duration::from_millis(5000)).await;

    let sym = Symbol::new(
        args.symbol.replace("USDT", ""),
        "USDT",
        args.symbol.clone(),
    );
    let price = dec!(63600);

    // 统计容器
    let mut s = HashMap::<&str, Stats>::new();
    for k in [
        "策略→网卡",
        "网卡→ACK",
        "策略→ACK (RTT)",
        "ACK→NEW(用户流)",
        "策略→NEW",
        "撤单发起→网卡",
        "网卡→撤单ACK",
        "撤单ACK→CANCELED",
        "撤单发起→CANCELED",
    ] {
        s.insert(k, Stats::new());
    }

    for i in 0..args.rounds {
        println!("\n────────── 轮次 {i} ──────────");
        let client_order_id = ClientOrderId(format!("nx-d-{}-{}", args.symbol, i));
        let order = NewOrder::limit(
            sym.clone(),
            Side::Buy,
            price,
            dec!(0.001),
            client_order_id.clone(),
        )
        .post_only();

        // ── 下单链路 ──
        let t_strategy = Instant::now();
        let t0 = now_ms();

        let (resp, wire_ts) = fapi.place(&order).await?;
        let t_ack = now_ms();
        let t_ack_wall = t_strategy.elapsed().as_secs_f64() * 1000.0;

        // 策略→网卡 = 网卡时刻 - 策略时刻
        let t_strategy_ms = t0;
        let local_to_wire = if wire_ts > 0 { wire_ts - t_strategy_ms } else { 0 };
        // 网卡→ACK = ACK时刻 - 网卡时刻
        let wire_to_ack = if wire_ts > 0 { t_ack - wire_ts } else { 0 };

        let status = resp["status"].as_i64().unwrap_or(-1);
        let resp_cid = resp["result"]["clientOrderId"].as_str().unwrap_or("?");
        println!(
            "  下单: status={status} 策略→网卡={local_to_wire}ms 网卡→ACK={wire_to_ack}ms 策略→ACK={t_ack_wall:.2}ms cid_sent={} cid_resp={}",
            client_order_id.0, resp_cid,
        );

        if status != 200 {
            println!("    下单失败: {}", resp["error"]);
            continue;
        }
        let order_id = resp["result"]["orderId"].as_u64().unwrap_or(0);

        push(&mut s, "策略→网卡", local_to_wire as f64);
        push(&mut s, "网卡→ACK", wire_to_ack as f64);
        push(&mut s, "策略→ACK (RTT)", t_ack_wall);

        // 等用户流 NEW 确认（匹配 clientOrderId）
        let t_new = wait_for_order_status(&mut user_rx, &client_order_id.0, &["NEW", "PARTIALLY_FILLED"])
            .await;
        if let Some(t_new) = t_new {
            let ack_to_new = t_new - t_ack;
            let strategy_to_new = t_new - t0;
            println!(
                "  NEW(用户流): ACK→NEW={ack_to_new}ms 策略→NEW={strategy_to_new}ms  (orderId={order_id})"
            );
            push(&mut s, "ACK→NEW(用户流)", ack_to_new as f64);
            push(&mut s, "策略→NEW", strategy_to_new as f64);
        } else {
            println!("  NEW(用户流): 超时未收到");
        }

        // ── 撤单链路 ──
        let t_c0 = now_ms();
        let t_cancel_start = Instant::now();
        let (cresp, c_wire) = fapi.cancel(&args.symbol, order_id).await?;
        let t_cack = now_ms();
        let t_cack_wall = t_cancel_start.elapsed().as_secs_f64() * 1000.0;

        let c_local = if c_wire > 0 { c_wire - t_c0 } else { 0 };
        let c_wire_to_ack = if c_wire > 0 { t_cack - c_wire } else { 0 };
        let cstatus = cresp["status"].as_i64().unwrap_or(-1);
        println!(
            "  撤单: status={cstatus} 发起→网卡={c_local}ms 网卡→ACK={c_wire_to_ack}ms 发起→ACK={t_cack_wall:.2}ms"
        );

        push(&mut s, "撤单发起→网卡", c_local as f64);
        push(&mut s, "网卡→撤单ACK", c_wire_to_ack as f64);

        if cstatus == 200 {
            let t_canceled = wait_for_order_status(&mut user_rx, &client_order_id.0, &["CANCELED"])
                .await;
            if let Some(t_canc) = t_canceled {
                let ack_to_canc = t_canc - t_cack;
                let c0_to_canc = t_canc - t_c0;
                println!(
                    "  CANCELED(用户流): ACK→CANCELED={ack_to_canc}ms 发起→CANCELED={c0_to_canc}ms"
                );
                push(&mut s, "撤单ACK→CANCELED", ack_to_canc as f64);
                push(&mut s, "撤单发起→CANCELED", c0_to_canc as f64);
            } else {
                println!("  CANCELED(用户流): 超时未收到");
            }
        }
    }

    // ── 汇总 ──
    println!("\n{}", "=".repeat(72));
    println!("  延迟汇总 (ms, 只统计成功)");
    println!("{}", "=".repeat(72));
    println!("  ── 下单链路 ──");
    s["策略→网卡"].report("策略发指令→网卡推出 (本地)");
    s["网卡→ACK"].report("网卡→币安ACK (网络)");
    s["策略→ACK (RTT)"].report("策略→币安ACK (总RTT)");
    s["ACK→NEW(用户流)"].report("币安ACK→用户流NEW");
    s["策略→NEW"].report("策略→用户流NEW (全链路)");
    println!("  ── 撤单链路 ──");
    s["撤单发起→网卡"].report("撤单发起→网卡推出 (本地)");
    s["网卡→撤单ACK"].report("网卡→撤单ACK (网络)");
    s["撤单ACK→CANCELED"].report("撤单ACK→用户流CANCELED");
    s["撤单发起→CANCELED"].report("撤单发起→CANCELED (全链路)");
    println!("{}", "=".repeat(72));

    Ok(())
}

/// 从用户数据流等待指定状态的订单更新。返回收到时刻 (本地 ms)。
/// 超时 10s 返回 None。
async fn wait_for_order_status(
    user_rx: &mut mpsc::UnboundedReceiver<String>,
    client_order_id: &str,
    statuses: &[&str],
) -> Option<i64> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let msg = tokio::time::timeout(Duration::from_millis(500), user_rx.recv()).await;
        let Ok(Some(msg)) = msg else {
            continue; // 超时或通道关闭则继续等（不提前返回）
        };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&msg) else {
            continue;
        };
        let evt = v["e"].as_str().unwrap_or("?");
        if evt != "ORDER_TRADE_UPDATE" {
            eprintln!("[user-stream] 非订单事件: e={evt}");
            continue;
        }
        let o = &v["o"];
        eprintln!(
            "[user-stream] OTU c={} X={} target={}",
            o["c"].as_str().unwrap_or("?"),
            o["X"].as_str().unwrap_or("?"),
            client_order_id
        );
        if o["c"].as_str() == Some(client_order_id) {
            let st = o["X"].as_str().unwrap_or("");
            if statuses.contains(&st) {
                return Some(now_ms());
            }
        }
    }
    None
}
