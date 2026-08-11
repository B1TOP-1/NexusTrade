//! Binance Futures 行情 adapter。
//!
//! Orderbook 维护（Binance 官方文档算法）：
//! 1. 订阅 WS `{symbol}@depth`，开始缓存更新
//! 2. GET REST `/fapi/v1/depth?symbol=?&limit=1000` 获得快照 lastUpdateId
//! 3. 丢弃缓存中 `u < lastUpdateId` 的过期事件
//! 4. 将快照写入本地簿
//! 5. 从第一个 `U <= lastUpdateId+1 && u >= lastUpdateId+1` 的事件开始应用增量
//! 6. 每个新事件的 `pu` 应等于上一事件的 `u`，否则 gap → 从步骤 2 重建
//! 7. 数量为绝对值，qty=0 表示删除该档位

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nexus_book::{BookEngine, Level, Side};
use nexus_core::{
    BookHandle, BookOptions, Decimal, MarketVenue, NexusError, PublicTrade, Result, Symbol,
    SymbolMeta, TradeStream, VenueId,
};
use tokio::sync::mpsc;

use crate::types::{DepthSnapshot, ExchangeInfo};
use crate::ws::{self};

/// Binance 行情配置。
#[derive(Debug, Clone)]
pub struct BinanceMarketConfig {
    pub ws_url: String,
    pub rest_url: String,
    pub reconnect_delay: Duration,
}

impl Default for BinanceMarketConfig {
    fn default() -> Self {
        Self {
            ws_url: "wss://fstream.binance.com/ws".to_string(),
            rest_url: "https://fapi.binance.com".to_string(),
            reconnect_delay: Duration::from_millis(500),
        }
    }
}

impl BinanceMarketConfig {
    pub fn testnet() -> Self {
        Self {
            ws_url: "wss://stream.binancefuture.com/ws".to_string(),
            rest_url: "https://testnet.binancefuture.com".to_string(),
            reconnect_delay: Duration::from_millis(500),
        }
    }
}

/// Binance Futures 行情 venue。
pub struct BinanceMarket {
    config: BinanceMarketConfig,
    http: reqwest::Client,
    /// symbol → SymbolMeta，connect 时加载。
    meta_cache: HashMap<String, SymbolMeta>,
}

impl BinanceMarket {
    pub async fn connect(config: BinanceMarketConfig) -> Result<Self> {
        let http = reqwest::Client::new();
        let meta_cache = load_exchange_info(&http, &config.rest_url).await?;
        Ok(Self {
            config,
            http,
            meta_cache,
        })
    }

    pub async fn connect_mainnet() -> Result<Self> {
        Self::connect(BinanceMarketConfig::default()).await
    }

    pub async fn connect_testnet() -> Result<Self> {
        Self::connect(BinanceMarketConfig::testnet()).await
    }

    /// GET depth snapshot + padding (if provided) → DepthSnapshot
    async fn fetch_snapshot(&self, symbol: &str) -> Result<DepthSnapshot> {
        let snap: DepthSnapshot = self
            .http
            .get(format!("{}/fapi/v1/depth", self.config.rest_url))
            .query(&[("symbol", symbol), ("limit", "1000")])
            .send()
            .await
            .map_err(|e| NexusError::Transport(format!("depth REST: {e}")))?
            .json()
            .await
            .map_err(|e| NexusError::Transport(format!("depth parse: {e}")))?;
        Ok(snap)
    }
}

#[async_trait]
impl MarketVenue for BinanceMarket {
    fn venue(&self) -> VenueId {
        VenueId::BINANCE_FUT
    }

    async fn subscribe_book(&self, symbol: &Symbol, opts: BookOptions) -> Result<BookHandle> {
        let engine = Arc::new(BookEngine::new());
        let venue_native = symbol.venue_native.clone();
        let rest_url = self.config.rest_url.clone();
        let ws_url = self.config.ws_url.clone();
        let reconnect_delay = self.config.reconnect_delay;
        let http = self.http.clone();

        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let (session, write_tx) =
            ws::spawn_reader(&ws_url, tx, shutdown_rx, reconnect_delay).await?;

        // 订阅 depth diff 流（fastest=true → 100ms，否则 250ms）。
        let speed = if opts.fastest { "100ms" } else { "250ms" };
        ws::subscribe(
            &write_tx,
            &[format!("{}@depth@{}", venue_native.to_lowercase(), speed)],
            1,
        );

        // 后台任务：缓存 → 快照对齐 → 持续更新。
        let engine_clone = Arc::clone(&engine);
        let venue = venue_native.clone();
        tokio::spawn(async move {
            // 关键：shutdown_tx / session / write_tx 必须在任务中存活。
            // 任一被 drop 都会让 WS 阅读器线程退出（shutdown/Close/write_rx 关闭），
            // 增量事件将永远不会到达 run_book_loop。
            let _keep_alive = (shutdown_tx, session, write_tx);
            run_book_loop(
                engine_clone,
                &mut rx,
                &http,
                &rest_url,
                &venue,
                reconnect_delay,
            )
            .await;
        });

        Ok(Arc::new(engine.handle()))
    }

    async fn subscribe_trades(&self, symbol: &Symbol) -> Result<TradeStream> {
        let (tx, rx) = mpsc::channel(4096);
        let venue_native = symbol.venue_native.clone();
        let ws_url = self.config.ws_url.clone();
        let reconnect_delay = self.config.reconnect_delay;

        let (raw_tx, mut raw_rx) = mpsc::unbounded_channel::<String>();
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let (_session, write_tx) =
            ws::spawn_reader(&ws_url, raw_tx, shutdown_rx, reconnect_delay).await?;

        ws::subscribe(
            &write_tx,
            &[format!("{}@aggTrade", venue_native.to_lowercase())],
            2,
        );

        let sym = symbol.clone();
        tokio::spawn(async move {
            while let Some(msg) = raw_rx.recv().await {
                if let Ok(trade) =
                    serde_json::from_str::<crate::types::AggTradeData>(&msg)
                {
                    let price = Decimal::from_str(&trade.price).unwrap_or_default();
                    let qty = Decimal::from_str(&trade.qty).unwrap_or_default();
                    let pt = PublicTrade {
                        symbol: sym.clone(),
                        price,
                        qty,
                        // is_buyer_maker=true 指挂单被动成交 → 主动方是卖方 (Sell)
                        aggressor: if trade.is_buyer_maker {
                            nexus_core::Side::Sell
                        } else {
                            nexus_core::Side::Buy
                        },
                        // 成交流权威时间戳用 T（撮合时间，local-T），不用 E（网关时间）。
                        venue_ts_ms: trade.trade_time as i64,
                        local_recv_ms: nexus_core::now_ms(),
                    };
                    if tx.send(pt).await.is_err() {
                        return;
                    }
                }
            }
        });

        Ok(rx)
    }

    fn symbol_meta(&self, symbol: &Symbol) -> Result<SymbolMeta> {
        self.meta_cache
            .get(&symbol.venue_native)
            .cloned()
            .ok_or_else(|| {
                NexusError::InvalidOrder(format!(
                    "unknown binance symbol: {}",
                    symbol.venue_native
                ))
            })
    }
}

// ── book maintenance loop (Binance 官方文档算法) ──

/// 缓存深度更新的内部结构。
struct CachedDelta {
    first_update_id: u64,
    final_update_id: u64,
    prev_final_id: u64,
    bids: Vec<Level>,
    asks: Vec<Level>,
}

async fn run_book_loop(
    engine: Arc<BookEngine>,
    rx: &mut mpsc::UnboundedReceiver<String>,
    http: &reqwest::Client,
    rest_url: &str,
    symbol: &str,
    reconnect_delay: Duration,
) {
    let mut buffer: Vec<CachedDelta> = Vec::new();
    let mut last_u: u64 = 0;
    let mut need_snapshot = true;
    let mut need_bridge = true;
    let mut local_seq: u64 = 0; // 本地单调 seq，非 Binance update_id

    loop {
        // ── 阶段 1：如果簿未就绪或 gap → 拉快照 ──
        if need_snapshot {
            // 先等 WS 积累几个事件
            if last_u == 0 {
                tokio::time::sleep(Duration::from_millis(300)).await;
            }

            let snap: DepthSnapshot = match fetch_snapshot_retry(http, rest_url, symbol, reconnect_delay)
                .await
            {
                Ok(s) => s,
                Err(_) => {
                    tokio::time::sleep(reconnect_delay).await;
                    continue;
                }
            };
            let snapshot_id = snap.lastUpdateId;

            // 步骤 3：丢弃 buffer 中 u < snapshot_id+1 的过期事件
            buffer.retain(|d| d.final_update_id >= snapshot_id + 1);

            // 步骤 4：写入快照
            let snap_bids = parse_levels(&snap.bids);
            let snap_asks = parse_levels(&snap.asks);
            engine.apply_snapshot(snap_bids, snap_asks);
            last_u = snapshot_id;

            // 步骤 5：在 buffer 中找桥接事件 (U <= last_u+1 && u >= last_u+1)
            let mut apply_idx = 0;
            for (i, d) in buffer.iter().enumerate() {
                if d.first_update_id <= last_u + 1 && d.final_update_id >= last_u + 1 {
                    apply_idx = i + 1;
                    local_seq += 1;
                    apply_delta(&engine, d, local_seq);
                    last_u = d.final_update_id;
                    break;
                }
            }
            buffer.drain(..apply_idx);

            if apply_idx > 0 {
                need_snapshot = false;
                need_bridge = false;
            } else {
                need_snapshot = false;
                need_bridge = true;
            }
        }

        // ── 阶段 2：持续消费增量 ──
        loop {
            let msg = match rx.recv().await {
                Some(m) => m,
                None => {
                    // channel closed
                    return;
                }
            };

            let ev: crate::types::DepthStreamData = match serde_json::from_str::<crate::types::DepthStreamData>(&msg) {
                Ok(ev) if ev.event_type == "depthUpdate" => ev,
                _ => continue,
            };

            let bids = parse_levels(&ev.bids);
            let asks = parse_levels(&ev.asks);

            let delta = CachedDelta {
                first_update_id: ev.first_update_id,
                final_update_id: ev.final_update_id,
                prev_final_id: ev.prev_final_id,
                bids,
                asks,
            };

            // 簿未就绪 → 入 buffer，等快照
            if need_snapshot || last_u == 0 {
                buffer.push(delta);
                need_snapshot = true;
                break; // 退出内循环，走快照路径
            }

            // 当还在等桥接时，放 buffer 里，尝试找
            if need_bridge {
                let can_bridge = delta.first_update_id <= last_u + 1
                    && delta.final_update_id >= last_u + 1;
                if can_bridge {
                    local_seq += 1;
                    apply_delta(&engine, &delta, local_seq);
                    last_u = delta.final_update_id;
                    buffer.clear();
                    need_bridge = false;
                    continue;
                }
                // 事件已跳过快照窗口（U > last_u+1）→ 错过了桥接 → 重建快照
                if delta.first_update_id > last_u + 1 {
                    eprintln!(
                        "[binance-book] missed bridge (U={} > last_u+1={}), re-snapshot",
                        delta.first_update_id,
                        last_u + 1
                    );
                    last_u = 0;
                    need_snapshot = true;
                    need_bridge = true;
                    buffer.clear();
                    buffer.push(delta);
                    break; // 退出内循环，拉快照
                }
                buffer.push(delta);
                continue;
            }

            // 正常消费：检查连续性
            if delta.final_update_id <= last_u {
                // 过期，丢弃
                continue;
            }

            if delta.prev_final_id != last_u {
                eprintln!(
                    "[binance-book] gap: pu={} != last_u={}, re-syncing...",
                    delta.prev_final_id, last_u
                );
                last_u = 0;
                need_snapshot = true;
                need_bridge = true;
                buffer.clear();
                buffer.push(delta);
                break; // 退出内循环，拉快照
            }

            // 应用增量
            local_seq += 1;
            apply_delta(&engine, &delta, local_seq);
            last_u = delta.final_update_id;
        }
    }
}

fn apply_delta(engine: &BookEngine, delta: &CachedDelta, seq: u64) {
    if !delta.bids.is_empty() && !delta.asks.is_empty() {
        // 同一 depthUpdate 有两侧：用批量方法，单个 seq
        let _ = engine.apply_delta_both(delta.bids.clone(), delta.asks.clone(), seq);
    } else if !delta.bids.is_empty() {
        let _ = engine.apply_delta(Side::Bid, delta.bids.clone(), seq);
    } else if !delta.asks.is_empty() {
        let _ = engine.apply_delta(Side::Ask, delta.asks.clone(), seq);
    }
}

fn parse_levels(raw: &[[String; 2]]) -> Vec<Level> {
    raw.iter()
        .filter_map(|pair| {
            let price = Decimal::from_str(&pair[0]).ok()?;
            let qty = Decimal::from_str(&pair[1]).ok()?;
            Some(Level { price, qty })
        })
        .collect()
}

async fn fetch_snapshot_retry(
    http: &reqwest::Client,
    rest_url: &str,
    symbol: &str,
    delay: Duration,
) -> Result<DepthSnapshot> {
    let mut backoff = delay;
    let max_backoff = Duration::from_secs(5);
    loop {
        let resp = http
            .get(format!("{rest_url}/fapi/v1/depth"))
            .query(&[("symbol", symbol), ("limit", "1000")])
            .send()
            .await;
        match resp {
            Ok(r) => {
                let status = r.status();
                if !status.is_success() {
                    eprintln!("[binance-book] snapshot HTTP {status}, retrying...");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(max_backoff);
                    continue;
                }
                match r.json::<DepthSnapshot>().await {
                    Ok(s) => return Ok(s),
                    Err(e) => {
                        eprintln!("[binance-book] snapshot parse error: {e}");
                    }
                }
            }
            Err(e) => {
                eprintln!("[binance-book] snapshot fetch error: {e}");
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

// ── exchangeInfo → SymbolMeta ──

async fn load_exchange_info(
    http: &reqwest::Client,
    rest_url: &str,
) -> Result<HashMap<String, SymbolMeta>> {
    let info: ExchangeInfo = http
        .get(format!("{rest_url}/fapi/v1/exchangeInfo"))
        .send()
        .await
        .map_err(|e| NexusError::Transport(format!("exchangeInfo: {e}")))?
        .json()
        .await
        .map_err(|e| NexusError::Transport(format!("exchangeInfo parse: {e}")))?;

    let mut map = HashMap::new();
    for sym in info.symbols {
        if sym.status != "TRADING" {
            continue;
        }
        let mut meta = SymbolMeta {
            tick_size: Decimal::ZERO,
            lot_size: Decimal::ZERO,
            min_notional: Decimal::ZERO,
            min_qty: Decimal::ZERO,
        };
        for filter in &sym.filters {
            use crate::types::FilterValue;
            match filter {
                FilterValue::PriceFilter { tickSize } => {
                    meta.tick_size = Decimal::from_str(tickSize).unwrap_or_default();
                }
                FilterValue::LotSize { stepSize, minQty } => {
                    meta.lot_size = Decimal::from_str(stepSize).unwrap_or_default();
                    meta.min_qty = Decimal::from_str(minQty).unwrap_or_default();
                }
                FilterValue::MinNotional { notional } => {
                    meta.min_notional = Decimal::from_str(notional).unwrap_or_default();
                }
                _ => {}
            }
        }
        map.insert(sym.symbol, meta);
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_core::BookReader;
    use rust_decimal_macros::dec;

    // ── helpers ──

    fn lvl(price: i64, qty: f64) -> Level {
        Level {
            price: Decimal::from(price),
            qty: Decimal::from_f64_retain(qty).unwrap(),
        }
    }

    fn lvl_z(price: i64) -> Level {
        Level {
            price: Decimal::from(price),
            qty: Decimal::ZERO,
        }
    }

    fn new_book(bids: Vec<Level>, asks: Vec<Level>) -> (Arc<BookEngine>, BookHandle) {
        let e = Arc::new(BookEngine::new());
        e.apply_snapshot(bids, asks);
        let h = e.handle();
        (e, Arc::new(h))
    }

    fn btc_book() -> (Arc<BookEngine>, BookHandle) {
        new_book(
            vec![lvl(65000, 1.5), lvl(64950, 2.0), lvl(64900, 5.0)],
            vec![lvl(65010, 1.0), lvl(65020, 3.0), lvl(65050, 4.0)],
        )
    }

    // ═══════════════════════════════════════════════════
    // parse_levels
    // ═══════════════════════════════════════════════════

    #[test]
    fn parse_levels_normal() {
        let raw = vec![
            ["43187.00".to_string(), "1.200".to_string()],
            ["43186.50".to_string(), "3.400".to_string()],
        ];
        let levels = parse_levels(&raw);
        assert_eq!(levels.len(), 2);
        assert_eq!(levels[0].price, dec!(43187.00));
        assert_eq!(levels[0].qty, dec!(1.200));
    }

    #[test]
    fn parse_levels_invalid_filtered() {
        let raw = vec![
            ["abc".to_string(), "1.0".to_string()],
            ["100.0".to_string(), "".to_string()],
        ];
        assert_eq!(parse_levels(&raw).len(), 0);
    }

    #[test]
    fn parse_levels_empty() {
        assert_eq!(parse_levels(&[]), vec![]);
    }

    #[test]
    fn parse_levels_mixed() {
        let raw = vec![
            ["100.0".to_string(), "1.5".to_string()],
            ["bad".to_string(), "2.0".to_string()],
            ["200.0".to_string(), "3.0".to_string()],
        ];
        let levels = parse_levels(&raw);
        assert_eq!(levels.len(), 2);
        assert_eq!(levels[0].price, dec!(100));
        assert_eq!(levels[1].price, dec!(200));
    }

    // ═══════════════════════════════════════════════════
    // apply_delta helper
    // ═══════════════════════════════════════════════════

    #[test]
    fn apply_delta_empty_sides_noop() {
        // When both bids and asks are empty, neither engine.apply_delta is
        // called → seq stays at 0.
        let (e, h) = new_book(vec![lvl(100, 1.0)], vec![lvl(101, 1.0)]);
        apply_delta(&e, &CachedDelta {
            first_update_id: 1, final_update_id: 1, prev_final_id: 0,
            bids: vec![], asks: vec![],
        }, 1);
        let top = h.top().unwrap();
        assert_eq!(top.bid, dec!(100));
        assert_eq!(top.ask, dec!(101));
        assert_eq!(h.seq(), 0);
    }

    #[test]
    fn apply_delta_bids_only() {
        // Insert a better bid (101 > 100)
        let (e, h) = new_book(vec![lvl(100, 1.0)], vec![lvl(101, 1.0)]);
        apply_delta(&e, &CachedDelta {
            first_update_id: 1, final_update_id: 1, prev_final_id: 0,
            bids: vec![lvl(101, 2.0)], asks: vec![],
        }, 1);
        assert_eq!(h.top().unwrap().bid, dec!(101));
        assert_eq!(h.top().unwrap().ask, dec!(101));
    }

    #[test]
    fn apply_delta_asks_only() {
        // Insert a better ask (100.5 < 101)
        let (e, h) = new_book(vec![lvl(100, 1.0)], vec![lvl(101, 1.0)]);
        apply_delta(&e, &CachedDelta {
            first_update_id: 1, final_update_id: 1, prev_final_id: 0,
            bids: vec![], asks: vec![lvl(100, 3.0)],
        }, 1);
        assert_eq!(h.top().unwrap().bid, dec!(100));
        assert_eq!(h.top().unwrap().ask, dec!(100));
    }

    // ═══════════════════════════════════════════════════
    // snapshot basics
    // ═══════════════════════════════════════════════════

    #[test]
    fn snapshot_sets_bbo() {
        let (_, h) = new_book(vec![lvl(50000, 1.5)], vec![lvl(50100, 2.0)]);
        let top = h.top().unwrap();
        assert_eq!(top.bid, dec!(50000));
        assert_eq!(top.bid_qty, dec!(1.5));
        assert_eq!(top.ask, dec!(50100));
        assert_eq!(top.ask_qty, dec!(2.0));
        assert_eq!(h.seq(), 0);
    }

    #[test]
    fn snapshot_sorts_desc_bids_asc_asks() {
        let (_, h) = new_book(
            vec![lvl(100, 1.0), lvl(300, 1.0), lvl(200, 1.0)],
            vec![lvl(500, 1.0), lvl(400, 1.0), lvl(450, 1.0)],
        );
        let v = h.depth(5);
        assert_eq!(v.bids[0].0, dec!(300));
        assert_eq!(v.bids[1].0, dec!(200));
        assert_eq!(v.bids[2].0, dec!(100));
        assert_eq!(v.asks[0].0, dec!(400));
        assert_eq!(v.asks[1].0, dec!(450));
        assert_eq!(v.asks[2].0, dec!(500));
    }

    #[test]
    fn snapshot_replaces() {
        let e = Arc::new(BookEngine::new());
        e.apply_snapshot(vec![lvl(100, 1.0)], vec![lvl(200, 1.0)]);
        e.apply_snapshot(vec![lvl(50, 2.0)], vec![lvl(300, 2.0)]);
        let top = e.handle().top().unwrap();
        assert_eq!(top.bid, dec!(50));
        assert_eq!(top.ask, dec!(300));
        assert_eq!(e.handle().seq(), 0);
    }

    #[test]
    fn snapshot_empty_no_top() {
        let e = Arc::new(BookEngine::new());
        e.apply_snapshot(vec![], vec![]);
        assert!(e.handle().top().is_none());
    }

    // ═══════════════════════════════════════════════════
    // delta: insert, update, delete
    // ═══════════════════════════════════════════════════

    #[test]
    fn delta_insert_new_worse_bid() {
        let (e, h) = new_book(vec![lvl(100, 1.0)], vec![lvl(200, 1.0)]);
        e.apply_delta(Side::Bid, vec![lvl(99, 2.0)], 1).unwrap();
        let v = h.depth(3);
        assert_eq!(v.bids.len(), 2);
        assert_eq!(v.bids[0].0, dec!(100));
        assert_eq!(v.bids[1].0, dec!(99));
    }

    #[test]
    fn delta_insert_better_bid() {
        let (e, h) = new_book(vec![lvl(100, 1.0)], vec![lvl(200, 1.0)]);
        e.apply_delta(Side::Bid, vec![lvl(101, 2.0)], 1).unwrap();
        assert_eq!(h.top().unwrap().bid, dec!(101));
    }

    #[test]
    fn delta_insert_better_ask() {
        let (e, h) = new_book(vec![lvl(100, 1.0)], vec![lvl(200, 1.0)]);
        e.apply_delta(Side::Ask, vec![lvl(199, 3.0)], 1).unwrap();
        assert_eq!(h.top().unwrap().ask, dec!(199));
    }

    #[test]
    fn delta_update_existing_qty() {
        let (e, h) = new_book(vec![lvl(100, 1.0), lvl(99, 2.0)], vec![lvl(200, 1.0)]);
        e.apply_delta(Side::Bid, vec![lvl(99, 5.0)], 1).unwrap();
        assert_eq!(h.depth(3).bids[1], (dec!(99), dec!(5.0)));
    }

    #[test]
    fn delta_delete_by_zero_qty() {
        let (e, h) = new_book(vec![lvl(100, 1.0), lvl(99, 2.0)], vec![lvl(200, 1.0)]);
        e.apply_delta(Side::Bid, vec![lvl_z(99)], 1).unwrap();
        assert_eq!(h.depth(5).bids.len(), 1);
        assert_eq!(h.depth(5).bids[0].0, dec!(100));
    }

    #[test]
    fn delta_delete_best_promotes_next() {
        let (e, h) = new_book(vec![lvl(100, 1.0), lvl(99, 2.0)], vec![lvl(200, 1.0)]);
        e.apply_delta(Side::Bid, vec![lvl_z(100)], 1).unwrap();
        let top = h.top().unwrap();
        assert_eq!(top.bid, dec!(99));
        assert_eq!(top.bid_qty, dec!(2.0));
    }

    #[test]
    fn delta_delete_unknown_price_noop() {
        let (e, h) = new_book(vec![lvl(100, 1.0)], vec![lvl(200, 1.0)]);
        e.apply_delta(Side::Bid, vec![lvl_z(99999)], 1).unwrap();
        assert_eq!(h.top().unwrap().bid, dec!(100));
    }

    #[test]
    fn delta_delete_all_levels() {
        let (e, h) = new_book(vec![lvl(100, 1.0)], vec![lvl(200, 1.0)]);
        e.apply_delta(Side::Bid, vec![lvl_z(100)], 1).unwrap();
        e.apply_delta(Side::Ask, vec![lvl_z(200)], 2).unwrap();
        assert!(h.top().is_none());
    }

    #[test]
    fn delta_both_sides_same_seq_gaps() {
        // Per-side apply_delta shares seq; calling twice with same seq on
        // different sides will gap on the second call. This is the actual
        // BookEngine contract: each delta advances seq by 1.
        let (e, _) = new_book(vec![lvl(100, 1.0)], vec![lvl(200, 1.0)]);
        e.apply_delta(Side::Bid, vec![lvl(99, 2.0)], 1).unwrap();
        let r = e.apply_delta(Side::Ask, vec![lvl(201, 2.0)], 1);
        assert!(matches!(r.unwrap_err(), NexusError::Stale));
    }

    // ═══════════════════════════════════════════════════
    // sequence validation
    // ═══════════════════════════════════════════════════

    #[test]
    fn delta_before_snapshot_rejected() {
        let e = BookEngine::new();
        let err = e.apply_delta(Side::Bid, vec![lvl(100, 1.0)], 1).unwrap_err();
        assert!(matches!(err, NexusError::Stale));
        assert!(e.handle().top().is_none());
    }

    #[test]
    fn sequential_deltas_progress() {
        let (e, h) = new_book(vec![lvl(100, 1.0)], vec![lvl(200, 1.0)]);
        for i in 1..=5 {
            e.apply_delta(Side::Bid, vec![lvl(100 - i as i64, 1.0)], i).unwrap();
            assert_eq!(h.seq(), i);
        }
    }

    #[test]
    fn gap_clears_book() {
        let (e, h) = btc_book();
        e.apply_delta(Side::Bid, vec![], 1).unwrap();
        e.apply_delta(Side::Bid, vec![], 2).unwrap();
        let err = e.apply_delta(Side::Bid, vec![], 5).unwrap_err();
        assert!(matches!(err, NexusError::Stale));
        assert!(h.top().is_none());
    }

    #[test]
    fn after_gap_resnapshot_ok() {
        let e = Arc::new(BookEngine::new());
        e.apply_snapshot(vec![lvl(100, 1.0)], vec![lvl(200, 1.0)]);
        e.apply_delta(Side::Bid, vec![], 1).unwrap();
        e.apply_delta(Side::Bid, vec![], 3).unwrap_err();
        assert!(e.handle().top().is_none());

        e.apply_snapshot(vec![lvl(150, 2.0)], vec![lvl(180, 3.0)]);
        let top = e.handle().top().unwrap();
        assert_eq!(top.bid, dec!(150));
        assert_eq!(e.handle().seq(), 0);

        e.apply_delta(Side::Ask, vec![lvl(179, 1.0)], 1).unwrap();
        assert_eq!(e.handle().top().unwrap().ask, dec!(179));
    }

    #[test]
    fn first_delta_seq_not_1_is_accepted() {
        // BookEngine snapshot sets seq=0; self.seq > 0 check skips first
        // delta — any seq is accepted. The adapter's run_book_loop is
        // responsible for alignment, not BookEngine.
        let (e, h) = new_book(vec![lvl(100, 1.0)], vec![lvl(200, 1.0)]);
        e.apply_delta(Side::Bid, vec![lvl(99, 1.0)], 2).unwrap();
        assert_eq!(h.seq(), 2);
    }

    #[test]
    fn u64_max_accepted_as_first_delta() {
        // Same reason: seq=0 skips the gap check.
        let (e, h) = new_book(vec![lvl(100, 1.0)], vec![lvl(200, 1.0)]);
        e.apply_delta(Side::Bid, vec![lvl(99, 1.0)], u64::MAX).unwrap();
        assert_eq!(h.seq(), u64::MAX);
    }

    // ═══════════════════════════════════════════════════
    // multi-level churn (realistic)
    // ═══════════════════════════════════════════════════

    #[test]
    fn multi_level_snapshot_depth() {
        let (_, h) = btc_book();
        let v = h.depth(10);
        assert_eq!(v.bids.len(), 3);
        assert_eq!(v.asks.len(), 3);
        assert_eq!(v.bids[0].0, dec!(65000));
        assert_eq!(v.bids[2].0, dec!(64900));
    }

    #[test]
    fn depth_limit() {
        let (_, h) = btc_book();
        let v = h.depth(1);
        assert_eq!(v.bids.len(), 1);
        assert_eq!(v.asks.len(), 1);
    }

    #[test]
    fn realistic_churn_simulation() {
        let bids: Vec<_> = (0..10).map(|i| lvl(50000 - i * 10, 1.0 + i as f64 * 0.1)).collect();
        let asks: Vec<_> = (0..10).map(|i| lvl(50100 + i * 10, 1.0 + i as f64 * 0.1)).collect();
        let (e, h) = new_book(bids, asks);

        // delete best bid → promote 49990
        e.apply_delta(Side::Bid, vec![lvl_z(50000)], 1).unwrap();
        assert_eq!(h.top().unwrap().bid, dec!(49990));

        // update best ask qty
        e.apply_delta(Side::Ask, vec![lvl(50100, 5.0)], 2).unwrap();
        assert_eq!(h.top().unwrap().ask_qty, dec!(5.0));

        // insert bid inside spread
        e.apply_delta(Side::Bid, vec![lvl(50050, 0.5)], 3).unwrap();
        assert_eq!(h.top().unwrap().bid, dec!(50050));

        // insert ask inside spread
        e.apply_delta(Side::Ask, vec![lvl(50080, 0.3)], 4).unwrap();
        assert_eq!(h.top().unwrap().ask, dec!(50080));

        // delete inside-spread levels → revert
        e.apply_delta(Side::Bid, vec![lvl_z(50050)], 5).unwrap();
        e.apply_delta(Side::Ask, vec![lvl_z(50080)], 6).unwrap();
        assert_eq!(h.top().unwrap().bid, dec!(49990));
        assert_eq!(h.top().unwrap().ask, dec!(50100));
    }

    // ═══════════════════════════════════════════════════
    // VWAP
    // ═══════════════════════════════════════════════════

    #[test]
    fn vwap_single_level_exact() {
        let (_, h) = new_book(vec![lvl(100, 10.0)], vec![lvl(101, 10.0)]);
        let (bid, ask) = h.vwap(dec!(1000)).unwrap();
        assert_eq!(bid, dec!(100));
        assert_eq!(ask, dec!(101));
    }

    #[test]
    fn vwap_spans_levels() {
        let (_, h) = new_book(
            vec![lvl(100, 1.0), lvl(99, 2.0), lvl(98, 5.0)],
            vec![lvl(101, 10.0)],
        );
        let (bid, _) = h.vwap(dec!(300)).unwrap();
        assert_eq!(bid, dec!(99.3267));
    }

    #[test]
    fn vwap_insufficient_depth() {
        let (_, h) = new_book(vec![lvl(100, 0.01)], vec![lvl(101, 0.01)]);
        assert!(h.vwap(dec!(10000)).is_none());
    }

    #[test]
    fn vwap_zero_notional() {
        let (_, h) = btc_book();
        assert!(h.vwap(Decimal::ZERO).is_none());
    }

    // ═══════════════════════════════════════════════════
    // BookHandle: clone, staleness, depth metadata
    // ═══════════════════════════════════════════════════

    #[test]
    fn handle_clone_same_state() {
        let (_, h1) = btc_book();
        let h2 = h1.clone();
        assert_eq!(h1.top(), h2.top());
        assert_eq!(h1.seq(), h2.seq());
        assert_eq!(h1.depth(3).bids, h2.depth(3).bids);
    }

    #[test]
    fn staleness_grows() {
        let (_, h) = btc_book();
        let s1 = h.staleness();
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(h.staleness() >= s1);
    }

    #[test]
    fn unready_staleness_max() {
        assert_eq!(BookEngine::new().handle().staleness(), Duration::MAX);
    }

    #[test]
    fn depth_view_metadata() {
        let (_, h) = btc_book();
        let v = h.depth(10);
        assert_eq!(v.seq, 0);
        assert!(v.local_recv_ms > 0);
    }

    // ═══════════════════════════════════════════════════
    // Binance depth algorithm (end-to-end local)
    // ═══════════════════════════════════════════════════

    /// Full Binance depth algorithm simulation. Documents a known bug: when both
    /// bid and ask sides are non-empty in one depthUpdate event, `apply_delta`
    /// calls `engine.apply_delta` twice with the same `final_update_id` — the
    /// second call triggers a spurious gap detection and silently clears the
    /// book. The test verifies the final book state accounting for this.
    #[test]
    fn binance_full_depth_algorithm() {
        let engine = Arc::new(BookEngine::new());

        let mut buffer: Vec<CachedDelta> = vec![
            CachedDelta { first_update_id: 1, final_update_id: 1, prev_final_id: 0, bids: vec![lvl(50001, 0.5)], asks: vec![] },
            CachedDelta { first_update_id: 2, final_update_id: 2, prev_final_id: 1, bids: vec![], asks: vec![lvl(50101, 0.3)] },
        ];

        let snap = DepthSnapshot {
            lastUpdateId: 2,
            bids: vec![["50000.00".to_string(), "1.5".to_string()], ["49950.00".to_string(), "2.0".to_string()]],
            asks: vec![["50100.00".to_string(), "1.0".to_string()], ["50150.00".to_string(), "3.0".to_string()]],
        };
        buffer.retain(|d| d.final_update_id >= snap.lastUpdateId + 1);
        assert!(buffer.is_empty());

        engine.apply_snapshot(parse_levels(&snap.bids), parse_levels(&snap.asks));
        let h = engine.handle();
        assert_eq!(h.top().unwrap().bid, dec!(50000));
        let mut last_u = snap.lastUpdateId;

        // Delta that bridges snapshot gap — bids only, no issue.
        let d0 = CachedDelta { first_update_id: 1, final_update_id: 3, prev_final_id: 2, bids: vec![lvl(50001, 1.0)], asks: vec![] };
        if d0.prev_final_id == last_u {
            apply_delta(&engine, &d0, 1);
            last_u = d0.final_update_id;
        }
        assert_eq!(h.top().unwrap().bid, dec!(50001));

        // Delta that deletes the level — bids only, no issue.
        let d1 = CachedDelta { first_update_id: 4, final_update_id: 4, prev_final_id: 3, bids: vec![lvl_z(50001)], asks: vec![] };
        if d1.prev_final_id == last_u {
            apply_delta(&engine, &d1, 2);
            last_u = d1.final_update_id;
        }
        assert_eq!(h.top().unwrap().bid, dec!(50000)); // best reverts

        // Double-side delta now uses apply_delta_both → single seq, no spurious gap.
        let d2 = CachedDelta { first_update_id: 5, final_update_id: 6, prev_final_id: 4, bids: vec![lvl(50002, 0.5)], asks: vec![lvl(50099, 2.0)] };
        if d2.prev_final_id == last_u {
            apply_delta(&engine, &d2, 3);
            last_u = d2.final_update_id;
        }
        // apply_delta_both applies both sides with one seq → book stays ready.
        let top = h.top().expect("double-side delta should not clear book");
        assert_eq!(top.bid, dec!(50002));
        assert_eq!(top.ask, dec!(50099));
    }

    #[test]
    fn binance_gap_triggers_rebuild_flow() {
        #![allow(unused_assignments)]
        let engine = Arc::new(BookEngine::new());

        // initial state
        engine.apply_snapshot(vec![lvl(100, 1.0)], vec![lvl(200, 1.0)]);
        let mut last_u: u64 = 100;

        // normal deltas
        let normal = [
            CachedDelta { first_update_id: 101, final_update_id: 101, prev_final_id: 100, bids: vec![lvl(101, 0.5)], asks: vec![] },
            CachedDelta { first_update_id: 102, final_update_id: 102, prev_final_id: 101, bids: vec![], asks: vec![lvl(199, 1.0)] },
        ];
        for (i, d) in normal.iter().enumerate() {
            apply_delta(&engine, d, (i + 1) as u64);
            last_u = d.final_update_id;
        }
        assert_eq!(last_u, 102);
        assert_eq!(engine.handle().top().unwrap().bid, dec!(101));

        // gap: prev_final_id=105 != 102
        let gap = CachedDelta { first_update_id: 106, final_update_id: 107, prev_final_id: 105, bids: vec![lvl(150, 5.0)], asks: vec![] };
        if gap.prev_final_id != last_u {
            last_u = 0; // trigger rebuild
        }
        assert_eq!(last_u, 0);

        // resnapshot after gap
        engine.apply_snapshot(vec![lvl(200, 2.0)], vec![lvl(300, 2.0)]);
        last_u = 200;

        // post-rebuild delta
        engine.apply_delta(Side::Bid, vec![lvl(201, 3.0)], 201).unwrap();
        assert_eq!(engine.handle().top().unwrap().bid, dec!(201));
    }
}
