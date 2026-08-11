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
use nexus_core::{ClientOrderId, Decimal, NewOrder, Side, Symbol};
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
    fn push_us(&mut self, us: f64) {
        self.samples.push(us);
    }
    /// 智能显示：>=1ms 显示 ms，<1ms 显示 us。
    fn fmt(val: f64) -> String {
        if val >= 1000.0 {
            format!("{:.2}ms", val / 1000.0)
        } else {
            format!("{:.1}us", val)
        }
    }
    fn report(&self, label: &str) {
        if self.samples.is_empty() {
            println!("  {label:<30} 无样本");
            return;
        }
        let min = self.samples.iter().cloned().fold(f64::MAX, f64::min);
        let max = self.samples.iter().cloned().fold(0.0, f64::max);
        let avg = self.samples.iter().sum::<f64>() / self.samples.len() as f64;
        println!(
            "  {label:<30} min={:<10} avg={:<10} max={:<10}  (n={})",
            Self::fmt(min),
            Self::fmt(avg),
            Self::fmt(max),
            self.samples.len()
        );
    }
}

/// 往统计表 push 一个 us 样本。
fn push(s: &mut HashMap<&str, Stats>, label: &str, value_us: f64) {
    if let Some(stats) = s.get_mut(label) {
        stats.push_us(value_us);
    }
}

/// ws-fapi 客户端（带网卡 hook 打点 + 详细阶段计时）。
struct FapiTracer {
    write_tx: mpsc::UnboundedSender<String>,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<serde_json::Value>>>>,
    api_key: String,
    api_secret: String,
    id_counter: AtomicU64,
    /// 网卡推出时刻（hook 写入，monotonic Instant，us 精度）
    wire_ts: Arc<Mutex<Option<Instant>>>,
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
        let wire_ts: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));

        // 网卡 hook：记录每次发送的真实时刻（Instant，us 精度）
        let wire_ts_hook = Arc::clone(&wire_ts);
        let hook: ws::WireHook = Arc::new(move |_msg| {
            *wire_ts_hook.lock().unwrap() = Some(Instant::now());
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

    /// 签名请求。返回 (响应, 网卡推出时刻 Instant)。
    async fn request(
        &self,
        method: &str,
        params: Vec<(String, String)>,
    ) -> Result<(serde_json::Value, Option<Instant>), String> {
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
        *self.wire_ts.lock().unwrap() = None;
        self.write_tx.send(payload.to_string()).map_err(|e| e.to_string())?;

        let (resp_tx, resp_rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(req_id, resp_tx);
        let resp = resp_rx.await.map_err(|_| "response dropped".to_string())?;
        let wire = *self.wire_ts.lock().unwrap();
        Ok((resp, wire))
    }

    async fn place(
        &self,
        order: &NewOrder,
    ) -> Result<(serde_json::Value, Option<Instant>), String> {
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

    async fn cancel(
        &self,
        symbol: &str,
        order_id: u64,
    ) -> Result<(serde_json::Value, Option<Instant>), String> {
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
/// 返回 (session, write_tx, 消息流)。write_tx 必须保活，否则阅读器死亡。
/// 裸连接用户流（已验证通：tmp_raw_user 裸连接能收到 NEW）。
/// 不用 spawn_reader（其 connect_with_proxy 在 VPS 上收不到消息）。
/// 返回 (ws 写端, ws 读端)。
async fn connect_user_stream_raw(
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
    use futures_util::StreamExt;

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

    // 裸连接（直连，不走 proxy 逻辑）
    let (ws, _) = tokio_tungstenite::connect_async(&ws_url)
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

    // 连接用户数据流（裸连接，已验证通）
    println!("[2] 连接用户数据流 (listenKey)...");
    let (_user_write, mut user_read) = connect_user_stream_raw(&key, rest_url).await?;
    println!("    连接成功 ✓");
    println!("    用户流自检: 裸连接建立 ✓ (等待订单事件推送)");

    // REST 通道：纯 reqwest 签名下单（不 acquire listenKey，避免抢占用户流）
    println!("[3] 准备 REST 通道 (reqwest 签名)...");
    let http = reqwest::Client::new();
    println!("    准备完成 ✓");

    let sym = Symbol::new(
        args.symbol.replace("USDT", ""),
        "USDT",
        args.symbol.clone(),
    );
    let price = dec!(63600);

    // 统计容器（REST + WS 各一套）
    let mut s = HashMap::<&str, Stats>::new();
    for k in [
        // 下单链路
        "WS策略→网卡",
        "WS网卡→ACK",
        "WS策略→ACK",
        "WS策略→NEW",
        "WS local-E",
        "WS local-T",
        "REST策略→ACK",
        "REST策略→NEW",
        "REST local-E",
        "REST local-T",
        // 撤单链路
        "WS撤单→网卡",
        "WS网卡→撤单ACK",
        "WS发起→CANCELED",
        "REST撤单→ACK",
        "REST发起→CANCELED",
    ] {
        s.insert(k, Stats::new());
    }

    for i in 0..args.rounds {
        println!("\n────────── 轮次 {i} ──────────");

        // ═══ WS 通道 ═══
        println!("  ── WS 通道 ──");
        let ws_cid = ClientOrderId(format!("nxws{}", i));
        let ws_order = NewOrder::limit(
            sym.clone(),
            Side::Buy,
            price,
            dec!(0.001),
            ws_cid.clone(),
        )
        .post_only();

        let t_ws_start = Instant::now();
        let (ws_resp, ws_wire) = fapi.place(&ws_order).await?;
        let t_ws_ack = Instant::now();
        let ws_ack_wall_us = t_ws_start.elapsed().as_micros() as f64;

        // us 精度：策略→网卡 和 网卡→ACK
        let ws_local_us = match ws_wire {
            Some(w) => w.duration_since(t_ws_start).as_micros() as f64,
            None => 0.0,
        };
        let ws_wire_ack_us = match ws_wire {
            Some(w) => t_ws_ack.duration_since(w).as_micros() as f64,
            None => 0.0,
        };
        let ws_status = ws_resp["status"].as_i64().unwrap_or(-1);
        println!(
            "  下单: status={ws_status} 策略→网卡={ws_local_us:.0}us 网卡→ACK={ws_wire_ack_us:.0}us 策略→ACK={ws_ack_wall_us:.0}us"
        );
        if ws_status != 200 {
            println!("    下单失败: {}", ws_resp["error"]);
        } else {
            push(&mut s, "WS策略→网卡", ws_local_us);
            push(&mut s, "WS网卡→ACK", ws_wire_ack_us);
            push(&mut s, "WS策略→ACK", ws_ack_wall_us);

            let ws_order_id = ws_resp["result"]["orderId"].as_u64().unwrap_or(0);
            // 用户流 NEW 确认 + local-E/T（local_recv 时刻用 Instant）
            if let Some(c) = wait_for_order_status(&mut user_read, &ws_cid.0, &["NEW", "PARTIALLY_FILLED"]).await {
                let ws_to_new_us = t_ws_start.elapsed().as_micros() as f64;
                let local_e_us = (now_ms() - c.gateway_ms) as f64;
                let local_t_us = (now_ms() - c.trade_ms) as f64;
                println!(
                    "  NEW(用户流): 策略→NEW={ws_to_new_us:.0}us local-E={local_e_us:.0}us local-T={local_t_us:.0}us (status={})",
                    c.status
                );
                push(&mut s, "WS策略→NEW", ws_to_new_us);
                push(&mut s, "WS local-E", local_e_us);
                push(&mut s, "WS local-T", local_t_us);
            } else {
                println!("  NEW(用户流): 超时未收到");
            }

            // 撤单
            let t_cs = Instant::now();
            let (cresp, c_wire) = fapi.cancel(&args.symbol, ws_order_id).await?;
            let t_cack = Instant::now();
            let c_local_us = match c_wire {
                Some(w) => w.duration_since(t_cs).as_micros() as f64,
                None => 0.0,
            };
            let c_wack_us = match c_wire {
                Some(w) => t_cack.duration_since(w).as_micros() as f64,
                None => 0.0,
            };
            let c_wall_us = t_cs.elapsed().as_micros() as f64;
            let cstatus = cresp["status"].as_i64().unwrap_or(-1);
            println!(
                "  撤单: status={cstatus} 发起→网卡={c_local_us:.0}us 网卡→ACK={c_wack_us:.0}us 发起→ACK={c_wall_us:.0}us"
            );
            if cstatus == 200 {
                push(&mut s, "WS撤单→网卡", c_local_us);
                push(&mut s, "WS网卡→撤单ACK", c_wack_us);
                if let Some(_cc) = wait_for_order_status(&mut user_read, &ws_cid.0, &["CANCELED"]).await {
                    let c0_to_c_us = t_cs.elapsed().as_micros() as f64;
                    println!("  CANCELED(用户流): 发起→CANCELED={c0_to_c_us:.0}us");
                    push(&mut s, "WS发起→CANCELED", c0_to_c_us);
                } else {
                    println!("  CANCELED(用户流): 超时未收到");
                }
            }
        }

        // ═══ REST 通道 ═══
        println!("  ── REST 通道 ──");
        let rest_cid = ClientOrderId(format!("nxr{}", i));

        // REST 下单（纯 reqwest 签名，不 acquire listenKey）
        let t_r_start = Instant::now();
        let rest_result = rest_place(
            &http,
            rest_url,
            &key,
            &secret,
            &args.symbol,
            &rest_cid.0,
            price,
        )
        .await;
        let rest_ack_wall_us = t_r_start.elapsed().as_micros() as f64;

        match rest_result {
            Ok(rest_oid) => {
                push(&mut s, "REST策略→ACK", rest_ack_wall_us);
                println!(
                    "  下单: status=200 策略→ACK={rest_ack_wall_us:.0}us orderId={rest_oid}"
                );
                // 用户流 NEW 确认 + local-E/T
                if let Some(c) = wait_for_order_status(&mut user_read, &rest_cid.0, &["NEW", "PARTIALLY_FILLED"]).await {
                    let r_to_new_us = t_r_start.elapsed().as_micros() as f64;
                    let local_e_us = (now_ms() - c.gateway_ms) as f64;
                    let local_t_us = (now_ms() - c.trade_ms) as f64;
                    println!(
                        "  NEW(用户流): 策略→NEW={r_to_new_us:.0}us local-E={local_e_us:.0}us local-T={local_t_us:.0}us (status={})",
                        c.status
                    );
                    push(&mut s, "REST策略→NEW", r_to_new_us);
                    push(&mut s, "REST local-E", local_e_us);
                    push(&mut s, "REST local-T", local_t_us);
                } else {
                    println!("  NEW(用户流): 超时未收到");
                }

                // 撤单
                let t_crs = Instant::now();
                let cancel_r = rest_cancel(&http, rest_url, &key, &secret, &args.symbol, rest_oid).await;
                let cr_wall_us = t_crs.elapsed().as_micros() as f64;
                let cr_status = if cancel_r.is_ok() { 200 } else { -1 };
                println!("  撤单: status={cr_status} 发起→ACK={cr_wall_us:.0}us");
                if cancel_r.is_ok() {
                    push(&mut s, "REST撤单→ACK", cr_wall_us);
                    if let Some(_cc) = wait_for_order_status(&mut user_read, &rest_cid.0, &["CANCELED"]).await {
                        let c0_to_c_us = t_crs.elapsed().as_micros() as f64;
                        println!("  CANCELED(用户流): 发起→CANCELED={c0_to_c_us:.0}us");
                        push(&mut s, "REST发起→CANCELED", c0_to_c_us);
                    } else {
                        println!("  CANCELED(用户流): 超时未收到");
                    }
                }
            }
            Err(e) => println!("  下单失败: {e}"),
        }
    }

    // ── 汇总 ──
    println!("\n{}", "=".repeat(72));
    println!("  延迟汇总 (us/ms 自动切换, 只统计成功)");
    println!("{}", "=".repeat(72));
    println!("  ── WS 下单链路 ──");
    s["WS策略→网卡"].report("WS 策略→网卡 (本地)");
    s["WS网卡→ACK"].report("WS 网卡→币安ACK (网络)");
    s["WS策略→ACK"].report("WS 策略→ACK (发出)");
    s["WS策略→NEW"].report("WS 策略→用户流NEW (挂单确认)");
    s["WS local-E"].report("WS local-E (本地收-E)");
    s["WS local-T"].report("WS local-T (本地收-T)");
    println!("  ── REST 下单链路 ──");
    s["REST策略→ACK"].report("REST 策略→ACK (发出)");
    s["REST策略→NEW"].report("REST 策略→用户流NEW (挂单确认)");
    s["REST local-E"].report("REST local-E (本地收-E)");
    s["REST local-T"].report("REST local-T (本地收-T)");
    println!("  ── WS 撤单链路 ──");
    s["WS撤单→网卡"].report("WS 撤单发起→网卡 (本地)");
    s["WS网卡→撤单ACK"].report("WS 网卡→撤单ACK (网络)");
    s["WS发起→CANCELED"].report("WS 发起→用户流CANCELED");
    println!("  ── REST 撤单链路 ──");
    s["REST撤单→ACK"].report("REST 撤单发起→ACK");
    s["REST发起→CANCELED"].report("REST 发起→用户流CANCELED");
    println!("{}", "=".repeat(72));

    Ok(())
}

/// REST 下单（纯 reqwest 签名，不 acquire listenKey）。返回 orderId。
async fn rest_place(
    http: &reqwest::Client,
    rest_url: &str,
    api_key: &str,
    api_secret: &str,
    symbol: &str,
    client_order_id: &str,
    price: Decimal,
) -> Result<u64, String> {
    let ts = chrono::Utc::now().timestamp_millis();
    let mut params = vec![
        ("symbol", symbol.to_string()),
        ("side", "BUY".to_string()),
        ("type", "LIMIT".to_string()),
        ("timeInForce", "GTX".to_string()),
        ("quantity", "0.001".to_string()),
        ("price", price.to_string()),
        ("newClientOrderId", client_order_id.to_string()),
        ("timestamp", ts.to_string()),
    ];
    params.sort_by(|a, b| a.0.cmp(&b.0));
    let query = params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let sig = sign(&query, api_secret);
    let full = format!("{query}&signature={sig}");

    let resp = http
        .post(format!("{rest_url}/fapi/v1/order"))
        .header("X-MBX-APIKEY", api_key)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(full)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let v: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;
    if v["orderId"].is_null() {
        return Err(format!("REST order failed: {v}"));
    }
    v["orderId"].as_u64().ok_or_else(|| "missing orderId".to_string())
}

/// REST 撤单。返回 orderId。
async fn rest_cancel(
    http: &reqwest::Client,
    rest_url: &str,
    api_key: &str,
    api_secret: &str,
    symbol: &str,
    order_id: u64,
) -> Result<(), String> {
    let ts = chrono::Utc::now().timestamp_millis();
    let mut params = vec![
        ("symbol", symbol.to_string()),
        ("orderId", order_id.to_string()),
        ("timestamp", ts.to_string()),
    ];
    params.sort_by(|a, b| a.0.cmp(&b.0));
    let query = params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let sig = sign(&query, api_secret);
    let full = format!("{query}&signature={sig}");

    let resp = http
        .delete(format!("{rest_url}/fapi/v1/order"))
        .header("X-MBX-APIKEY", api_key)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(full)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let v: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;
    if v["status"].as_str().is_none() {
        return Err(format!("REST cancel failed: {v}"));
    }
    Ok(())
}

/// 用户流订单确认结果：含事件时间戳。
struct StreamConfirm {
    /// 交易所事件时间 E（用户流推送时刻）。
    gateway_ms: i64,
    /// 交易所交易时间 T（撮合时刻）。
    trade_ms: i64,
    /// 订单状态（NEW/CANCELED 等）。
    status: String,
}

/// 从用户数据流等待指定状态的订单更新。
/// 返回确认信息（含 E/T 时间戳用于 local-E / local-T 计算）。超时返回 None。
async fn wait_for_order_status(
    user_read: &mut futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    >,
    client_order_id: &str,
    statuses: &[&str],
) -> Option<StreamConfirm> {
    use futures_util::StreamExt;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let msg = tokio::time::timeout(Duration::from_millis(500), user_read.next()).await;
        let Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) = msg else {
            continue; // 超时或非文本则继续等
        };
        let msg = text.to_string();
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&msg) else {
            continue;
        };
        let evt = v["e"].as_str().unwrap_or("?");
        if evt != "ORDER_TRADE_UPDATE" {
            continue;
        }
        let o = &v["o"];
        if o["c"].as_str() == Some(client_order_id) {
            let st = o["X"].as_str().unwrap_or("");
            if statuses.contains(&st) {
                return Some(StreamConfirm {
                    gateway_ms: v["E"].as_i64().unwrap_or(0),
                    trade_ms: v["T"].as_i64().unwrap_or(0),
                    status: st.to_string(),
                });
            }
        }
    }
    None
}
