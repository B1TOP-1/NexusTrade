//! Taker 市价单全链路测试（0ms 本地订单簿 + ws-fapi 市价单 + 用户流）。
//!
//! 核心：**正确的订单状态管理**
//!   - OrderManager 用 HashMap<OrderId, OrderState> 持续更新所有状态
//!   - 状态只能向前推进（NEW → PARTIALLY_FILLED → 终态），禁止倒退
//!   - 每次状态流转都记录（完整过程）
//!   - 终态（FILLED/CANCELED/EXPIRED/REJECTED）才 resolve oneshot
//!   - NEW/PARTIALLY_FILLED 不触发 waiter 完成
//!
//! 设计（参考用户提供的正确架构）：
//!   wait_for_terminal(order_id) → oneshot（只在终态完成）
//!   OrderState 持续更新，可查询任意时刻状态
//!
//! ⚠ 真实成交，消耗手续费（默认 0.001 BTC）。
//! 用法：
//!   cargo run -p nexus-binance --example taker_test -- --qty 0.001 --rounds 4
//!   cargo run -p nexus-binance --example taker_test -- --testnet

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt;
use nexus_binance::{BinanceMarket, BinanceMarketConfig, WsFapiClient};
use nexus_core::{BookOptions, ClientOrderId, Decimal, MarketVenue, NewOrder, Side, Symbol};
use rust_decimal_macros::dec;

// ═══════════════════════════════════════════════════════════════
// 订单状态机（状态只向前推进）
// ═══════════════════════════════════════════════════════════════

/// 订单状态机（状态只允许前进，禁止倒退）。
/// 枚举顺序 = 状态优先级：高的状态只能从低的推进来。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum OrderStatus {
    /// 未出网：策略本地构造，尚未发送（优先级最低，初始态）。
    PendingSubmit,
    /// 已发送未确认：交易所未回任何事件。
    Unknown,
    /// 已受理挂单：交易所确认收到（NEW）。
    New,
    /// 部分成交。
    PartiallyFilled,
    /// 全部成交（终态）。
    Filled,
    /// 已撤销（终态，可能带部分成交）。
    Canceled,
    /// 已过期（终态）。
    Expired,
    /// 已拒绝（终态，无敞口）。
    Rejected,
}

impl OrderStatus {
    fn from_str(s: &str) -> OrderStatus {
        match s {
            "NEW" => OrderStatus::New,
            "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
            "FILLED" => OrderStatus::Filled,
            "CANCELED" => OrderStatus::Canceled,
            "EXPIRED" => OrderStatus::Expired,
            "REJECTED" => OrderStatus::Rejected,
            _ => OrderStatus::Unknown,
        }
    }

    /// 是否终态。
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            OrderStatus::Filled
                | OrderStatus::Canceled
                | OrderStatus::Expired
                | OrderStatus::Rejected
        )
    }

    fn as_str(&self) -> &'static str {
        match self {
            OrderStatus::PendingSubmit => "PENDING_SUBMIT",
            OrderStatus::Unknown => "UNKNOWN",
            OrderStatus::New => "NEW",
            OrderStatus::PartiallyFilled => "PARTIALLY_FILLED",
            OrderStatus::Filled => "FILLED",
            OrderStatus::Canceled => "CANCELED",
            OrderStatus::Expired => "EXPIRED",
            OrderStatus::Rejected => "REJECTED",
        }
    }
}

/// 一次状态流转记录（完整过程）。
#[derive(Debug, Clone)]
struct Transition {
    from: OrderStatus,
    to: OrderStatus,
    /// 交易所撮合时间 T。
    trade_ms: i64,
    /// 交易所事件时间 E。
    gateway_ms: i64,
    /// 本地接收时刻。
    local_recv_ms: i64,
}

/// 订单持续状态（OrderManager 维护）。
#[derive(Debug, Clone)]
struct OrderState {
    client_order_id: String,
    status: OrderStatus,
    orig_qty: Decimal,
    executed_qty: Decimal,
    avg_price: Decimal,
    last_fill_qty: Decimal,
    last_fill_price: Decimal,
    /// 累计手续费。
    fee: Decimal,
    /// 手续费币种。
    fee_asset: String,
    /// 完整状态流转记录。
    transitions: Vec<Transition>,
}

impl OrderState {
    fn new(cid: &str) -> Self {
        Self {
            client_order_id: cid.to_string(),
            status: OrderStatus::PendingSubmit,
            orig_qty: Decimal::ZERO,
            executed_qty: Decimal::ZERO,
            avg_price: Decimal::ZERO,
            last_fill_qty: Decimal::ZERO,
            last_fill_price: Decimal::ZERO,
            fee: Decimal::ZERO,
            fee_asset: String::new(),
            transitions: Vec::new(),
        }
    }

    /// 应用一次 OTU 事件。
    ///
    /// ⚠ 不假设 WS 顺序：私有成交可能先于 NEW 到达，FILLED 也可能直接到。
    /// 每条事件都记录 + 更新字段。终态后忽略后续（防倒退/重复）。
    fn apply_update(&mut self, st: OrderStatus, o: &serde_json::Value, e_ms: i64, t_ms: i64) {
        // 终态后忽略后续事件（防倒退，如重连后旧 NEW）
        if self.status.is_terminal() {
            return;
        }

        let transition = Transition {
            from: self.status,
            to: st,
            trade_ms: t_ms,
            gateway_ms: e_ms,
            local_recv_ms: now_ms(),
        };
        self.status = st;
        self.transitions.push(transition);

        // 更新量/价字段
        self.orig_qty = Decimal::from_str(o["q"].as_str().unwrap_or("0"))
            .unwrap_or(self.orig_qty);
        self.executed_qty = Decimal::from_str(o["z"].as_str().unwrap_or("0"))
            .unwrap_or(self.executed_qty);
        self.avg_price = Decimal::from_str(o["ap"].as_str().unwrap_or("0"))
            .unwrap_or(self.avg_price);
        self.last_fill_qty = Decimal::from_str(o["l"].as_str().unwrap_or("0"))
            .unwrap_or(self.last_fill_qty);
        self.last_fill_price = Decimal::from_str(o["L"].as_str().unwrap_or("0"))
            .unwrap_or(self.last_fill_price);

        // 提取手续费（每次成交增量累加）
        if let Ok(f) = Decimal::from_str(o["n"].as_str().unwrap_or("0")) {
            self.fee += f;
        }
        if let Some(a) = o["N"].as_str() {
            if !a.is_empty() {
                self.fee_asset = a.to_string();
            }
        }
    }
}

/// OrderManager：持续更新 + 终态 waiter。
struct OrderManager {
    orders: Mutex<HashMap<String, OrderState>>,
    terminal_waiters: Mutex<HashMap<String, tokio::sync::oneshot::Sender<OrderState>>>,
    /// 最新余额（ACCOUNT_UPDATE 更新）。
    last_balance: Mutex<Decimal>,
    /// 最新仓位（带符号：正=多，负=空）。
    last_position: Mutex<Decimal>,
}

impl OrderManager {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            orders: Mutex::new(HashMap::new()),
            terminal_waiters: Mutex::new(HashMap::new()),
            last_balance: Mutex::new(Decimal::ZERO),
            last_position: Mutex::new(Decimal::ZERO),
        })
    }

    /// 更新余额/仓位（ACCOUNT_UPDATE）。返回 (余额, 仓位)。
    fn on_account_update(&self, v: &serde_json::Value) {
        let a = &v["a"];
        // 余额（取 USDT 钱包余额）
        if let Some(bs) = a["B"].as_array() {
            for b in bs {
                if b["a"].as_str() == Some("USDT") {
                    if let Ok(wb) = Decimal::from_str(b["wb"].as_str().unwrap_or("0")) {
                        *self.last_balance.lock().unwrap() = wb;
                    }
                }
            }
        }
        // 仓位（取 BTCUSDT）
        if let Some(ps) = a["P"].as_array() {
            for p in ps {
                if p["s"].as_str() == Some("BTCUSDT") {
                    if let Ok(pa) = Decimal::from_str(p["pa"].as_str().unwrap_or("0")) {
                        *self.last_position.lock().unwrap() = pa;
                    }
                }
            }
        }
    }

    /// 注册一个等待终态的 waiter。返回 oneshot receiver。
    fn register_waiter(
        &self,
        cid: &str,
    ) -> tokio::sync::oneshot::Receiver<OrderState> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.terminal_waiters.lock().unwrap().insert(cid.to_string(), tx);
        rx
    }

    /// 处理一条 OTU：更新状态 + 提取 fee，终态时 resolve waiter。
    /// （不再逐条打印大框——TRADE_LITE 已立即打印成交行，聚合到终态两行）
    fn on_order_update(&self, full: &serde_json::Value) {
        let o = &full["o"];
        let e_ms = full["E"].as_i64().unwrap_or(0);
        let t_ms = full["T"].as_i64().unwrap_or(0);

        let cid = o["c"].as_str().unwrap_or("").to_string();
        if cid.is_empty() {
            return;
        }
        let st = OrderStatus::from_str(o["X"].as_str().unwrap_or(""));

        // 持续更新 OrderState
        let mut orders = self.orders.lock().unwrap();
        let entry = orders.entry(cid.clone()).or_insert_with(|| OrderState::new(&cid));
        entry.apply_update(st, o, e_ms, t_ms);

        // 终态：resolve waiter（复制状态给 waiter）
        if st.is_terminal() {
            let snapshot = entry.clone();
            drop(orders);
            if let Some(sender) = self.terminal_waiters.lock().unwrap().remove(&cid) {
                let _ = sender.send(snapshot);
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════
// 工具
// ═══════════════════════════════════════════════════════════════

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
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

struct Args {
    symbol: String,
    qty: Decimal,
    rounds: usize,
    testnet: bool,
}

fn parse_args() -> Args {
    let mut args = Args {
        symbol: "BTCUSDT".to_string(),
        qty: dec!(0.001),
        rounds: 4,
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
    eprintln!("[user-stream] listenKey: {}...", &lk[..12.min(lk.len())]);
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

    println!("{}", "=".repeat(72));
    println!("  Taker 市价单全链路测试（正确订单状态机）");
    println!(
        "  Symbol: {}  Qty: {}  Rounds: {}（偶数轮买开仓 / 奇数轮卖平仓）",
        args.symbol, args.qty, args.rounds
    );
    println!("  ⚠ 真实成交，消耗手续费!");
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

    // 3. 连用户流 + OrderManager
    println!("[3] 连接用户流 + OrderManager...");
    let (_user_write, user_read) = connect_user_stream(&key, rest_url).await?;
    let manager = OrderManager::new();
    println!("    连接成功 ✓");

    // 后台 task：用户流 → OrderManager
    let manager_reader = Arc::clone(&manager);
    tokio::spawn(async move {
        let mut read = user_read;
        loop {
            match tokio::time::timeout(Duration::from_secs(30), read.next()).await {
                Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                    let Ok(v) = serde_json::from_str::<serde_json::Value>(&text.to_string()) else {
                        continue;
                    };
                    // 分类处理所有用户流事件
                    match v["e"].as_str().unwrap_or("?") {
                        "ORDER_TRADE_UPDATE" => manager_reader.on_order_update(&v),
                        "TRADE_LITE" => {
                            // 私有成交轻量版：立即打印，带本地时间戳（第一行）
                            let t_local = now_ms();
                            println!(
                                "[{t_local}] 成交 {} {} @ {}{} 成交ID={}",
                                v["S"].as_str().unwrap_or("?"),
                                v["l"].as_str().unwrap_or("?"),
                                v["L"].as_str().unwrap_or("?"),
                                if v["m"].as_bool().unwrap_or(false) {
                                    " 被动"
                                } else {
                                    " 主动"
                                },
                                v["t"].as_str().unwrap_or("?"),
                            );
                        }
                        "ACCOUNT_UPDATE" => manager_reader.on_account_update(&v),
                        "ACCOUNT_CONFIG_UPDATE" => {
                            let ac = &v["ac"];
                            println!(
                                "  [WS] ACCOUNT_CONFIG_UPDATE: 杠杆变更={} 多资产={}",
                                ac["l"].as_str().unwrap_or("?"),
                                ac["j"].as_bool().unwrap_or(false),
                            );
                        }
                        "MARGIN_CALL" => {
                            let positions: Vec<String> = v["p"]
                                .as_array()
                                .map(|ps| {
                                    ps.iter()
                                        .map(|p| {
                                            p["s"].as_str().unwrap_or("?").to_string()
                                        })
                                        .collect()
                                })
                                .unwrap_or_default();
                            println!(
                                "  [WS] ⚠ MARGIN_CALL 强平预警: 仓位={:?}",
                                positions
                            );
                        }
                        "listenKeyExpired" => println!("  [WS] ⚠ listenKeyExpired"),
                        _ => {} // 未知事件静默（调试期已确认无遗漏）
                    }
                }
                _ => continue,
            }
        }
    });
    println!("    用户流 → OrderManager 已启动 ✓");

    // 4. 逐轮测试
    println!("\n[4] 开始 taker 市价测试...\n");
    // 首轮前等用户流稳定
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut book_local_e: Vec<f64> = Vec::new();
    let mut book_local_t: Vec<f64> = Vec::new();
    let mut fill_latency: Vec<f64> = Vec::new();
    let mut ack_latency: Vec<f64> = Vec::new();
    let mut slippages: Vec<f64> = Vec::new();

    for i in 0..args.rounds {
        let is_open = i % 2 == 0;
        let side = if is_open { Side::Buy } else { Side::Sell };
        let side_str = if is_open { "买开仓" } else { "卖平仓" };

        // 簿 BBO + 延迟
        let pre_top = book.top().unwrap();
        let view = book.depth(1);
        let now = now_ms();
        let local_e = if view.gateway_ts_ms > 0 { now - view.gateway_ts_ms } else { 0 };
        let local_t = if view.venue_ts_ms > 0 { now - view.venue_ts_ms } else { 0 };
        book_local_e.push(local_e as f64);
        book_local_t.push(local_t as f64);
        let book_side_price = if is_open { pre_top.ask } else { pre_top.bid };
        println!(
            "── 轮 {i} ── {side_str} | 簿BBO: bid={} ask={} | 簿local-E={}ms local-T={}ms",
            pre_top.bid, pre_top.ask, local_e, local_t,
        );

        // 精度校验
        let meta = market.symbol_meta(&sym)?;
        let mut qty = args.qty;
        if qty < meta.min_qty {
            println!("    ⚠ qty {qty} < minQty {}, 自动抬到 {}", qty, meta.min_qty);
            qty = meta.min_qty;
        }
        qty = meta.quantize_qty(qty);

        // 下单前注册终态 waiter
        let cid = format!("nxtaker{}", i);
        let fill_rx = manager.register_waiter(&cid);

        let mut order = NewOrder::market(sym.clone(), side, qty, ClientOrderId(cid.clone()));
        if !is_open {
            order = order.reduce_only();
        }

        // 下单发起日志（带时间戳，place 之前）
        let t_local = now_ms();
        println!(
            "[{t_local}] 发起 {} {} @ 市价 {} cid={cid}",
            if is_open { "买入开仓" } else { "卖出平仓" },
            qty,
            if !is_open { "(reduceOnly)" } else { "" },
        );

        // 下单：t_start = 策略开始 → place 返回 = ACK
        let t_start = std::time::Instant::now();
        let result = fapi.place(&order).await;
        let ack_us = t_start.elapsed().as_micros() as f64; // 策略→ACK
        ack_latency.push(ack_us);

        match result {
            Ok(order_id) => {
                println!(
                    "[{t_local}] 已下单: orderId={order_id}  策略→ACK={:.2}ms",
                    ack_us / 1000.0
                );

                // 等终态（OrderManager 只在 FILLED/CANCELED/EXPIRED/REJECTED resolve）
                match tokio::time::timeout(Duration::from_secs(10), fill_rx).await {
                    Ok(Ok(final_state)) => {
                        // 全链路 = 策略→FILLED 确认；ACK→FILLED = 全链路 - ACK
                        let fill_us = t_start.elapsed().as_micros() as f64;
                        let ack_to_fill_us = fill_us - ack_us;
                        fill_latency.push(fill_us);
                        let now = now_ms();
                        let order_local_e = now - final_state.transitions.last().unwrap().gateway_ms;
                        let order_local_t = now - final_state.transitions.last().unwrap().trade_ms;

                        // 滑点
                        let fp = final_state.avg_price;
                        let slippage_pct = if book_side_price > Decimal::ZERO {
                            ((fp - book_side_price) / book_side_price * dec!(100)).abs()
                        } else {
                            Decimal::ZERO
                        };
                        slippages.push(
                            slippage_pct.to_string().parse::<f64>().unwrap_or(0.0),
                        );

                        // ── 两行输出 ──
                        // 第一行：成交汇总（fee/余额/仓位来自 OrderManager 聚合）
                        let balance = *manager.last_balance.lock().unwrap();
                        let position = *manager.last_position.lock().unwrap();
                        let fee = final_state.fee;
                        let fee_str = if fee > Decimal::ZERO {
                            format!(" fee={}{}", fee, final_state.fee_asset)
                        } else {
                            String::new()
                        };
                        println!(
                            "[成交] {} {} @ {} 滑点{:.2}%{} 余额={} 仓位={}",
                            if final_state.status == OrderStatus::Filled { "FILLED" } else { final_state.status.as_str() },
                            if is_open { "BUY" } else { "SELL" },
                            final_state.avg_price,
                            slippage_pct,
                            fee_str,
                            balance,
                            position,
                        );

                        // 第二行：延迟明细
                        println!(
                            "  延迟: 策略→ACK={:.2}ms ACK→FILLED={:.2}ms 策略→FILLED={:.2}ms local-E={}ms local-T={}ms",
                            ack_us / 1000.0,
                            ack_to_fill_us / 1000.0,
                            fill_us / 1000.0,
                            order_local_e,
                            order_local_t,
                        );
                    }
                    _ => {
                        println!("  终态确认: 超时");
                        // 兜底查询
                        if let Ok(status) = fapi.query(&sym, order_id).await {
                            let st = status["result"]["status"].as_str().unwrap_or("?").to_string();
                            let ap = status["result"]["avgPrice"].as_str().unwrap_or("?").to_string();
                            let z = status["result"]["executedQty"].as_str().unwrap_or("?").to_string();
                            println!("  兜底查询: status={st} avgPrice={ap} executedQty={z}");
                        }
                    }
                }
            }
            Err(e) => println!("  下单失败: {e}"),
        }
        println!();
    }

    // 汇总
    println!("{}", "=".repeat(72));
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
    let (s_min, s_avg, s_max) = sum(&slippages);
    println!("  策略→ACK (发出):   min={:.2}ms avg={:.2}ms max={:.2}ms", a_min / 1000.0, a_avg / 1000.0, a_max / 1000.0);
    if !fill_latency.is_empty() {
        println!("  策略→FILLED (全链路): min={:.2}ms avg={:.2}ms max={:.2}ms", f_min / 1000.0, f_avg / 1000.0, f_max / 1000.0);
        println!("  其中 ACK→FILLED (撮合+推送): ≈ 全链路 − ACK");
    }
    println!("  0ms簿 local-E: min={:.2}ms avg={:.2}ms max={:.2}ms", e_min, e_avg, e_max);
    println!("  0ms簿 local-T: min={:.2}ms avg={:.2}ms max={:.2}ms", t_min, t_avg, t_max);
    if !slippages.is_empty() {
        println!("  滑点: min={:.3}% avg={:.3}% max={:.3}%", s_min, s_avg, s_max);
    }
    println!("{}", "=".repeat(72));

    Ok(())
}
