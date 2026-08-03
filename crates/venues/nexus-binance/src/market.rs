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
    BookHandle, BookOptions, Decimal, MarketVenue, NexusError, PublicTrade, Result,
    Symbol, SymbolMeta, TradeStream, VenueId,
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
            ws_url: "wss://fstream.binance.com/public/ws".to_string(),
            rest_url: "https://fapi.binance.com".to_string(),
            reconnect_delay: Duration::from_millis(500),
        }
    }
}

impl BinanceMarketConfig {
    pub fn testnet() -> Self {
        Self {
            ws_url: "wss://stream.binancefuture.com/public/ws".to_string(),
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

    async fn subscribe_book(&self, symbol: &Symbol, _opts: BookOptions) -> Result<BookHandle> {
        let engine = Arc::new(BookEngine::new());
        let venue_native = symbol.venue_native.clone();
        let rest_url = self.config.rest_url.clone();
        let ws_url = self.config.ws_url.clone();
        let reconnect_delay = self.config.reconnect_delay;
        let http = self.http.clone();

        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let (_session, write_tx) =
            ws::spawn_reader(&ws_url, tx, shutdown_rx, reconnect_delay).await?;

        // 订阅 depth diff 流。
        ws::subscribe(
            &write_tx,
            &[format!("{}@depth", venue_native.to_lowercase())],
            1,
        );

        // 后台任务：缓存 → 快照对齐 → 持续更新。
        let engine_clone = Arc::clone(&engine);
        let venue = venue_native.clone();
        tokio::spawn(async move {
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
                        venue_ts_ms: trade.event_time as i64,
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
    let mut last_u: u64 = 0; // 上一个已成功应用的 final_update_id

    loop {
        // 步骤 1-2：已订阅 WS，持续接收缓存。
        while let Some(msg) = rx.recv().await {
            // 尝试解析 depthUpdate。
            let ev: crate::types::DepthStreamData = match serde_json::from_str::<crate::types::DepthStreamData>(&msg) {
                Ok(ev) if ev.event_type == "depthUpdate" => ev,
                _ => continue,
            };

            // 转换 levels。
            let bids = parse_levels(&ev.bids);
            let asks = parse_levels(&ev.asks);

            let delta = CachedDelta {
                first_update_id: ev.first_update_id,
                final_update_id: ev.final_update_id,
                prev_final_id: ev.prev_final_id,
                bids,
                asks,
            };

            buffer.push(delta);

            // 如果簿尚未初始化（last_u == 0），回到步骤 3：拉 REST 快照。
            if last_u == 0 {
                break;
            }

            // 已初始化：检查事件连续性（步骤 6：pu == 上一事件的 u）。
            // buffer 末尾即当前事件。
            let cur = buffer.last().unwrap();
            if cur.prev_final_id != last_u {
                // 丢包 → 重建。
                last_u = 0;
                break;
            }

            // 应用增量（步骤 5 & 7：绝对量，qty=0 删除）。
            apply_delta(&engine, cur);
            last_u = cur.final_update_id;
        }

        // 步骤 3：拉 REST 快照。
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

        // 步骤 3：丢弃 buffer 中 u < snapshot_id 的过期事件。
        buffer.retain(|d| d.final_update_id >= snapshot_id + 1);

        // 步骤 4：将快照写入本地簿。
        let snap_bids = parse_levels(&snap.bids);
        let snap_asks = parse_levels(&snap.asks);
        engine.apply_snapshot(snap_bids, snap_asks);
        last_u = snapshot_id;

        // 步骤 5：对齐 —— 找第一个 U <= last_u+1 && u >= last_u+1 的事件。
        let mut apply_idx = 0;
        for (i, d) in buffer.iter().enumerate() {
            if d.first_update_id <= last_u + 1 && d.final_update_id >= last_u + 1 {
                apply_idx = i + 1;
                // 应用此事件中 >= last_u+1 的部分。
                // 简化：整个 delta 应用（BookEngine 按价格 upsert，幂等）。
                apply_delta(&engine, d);
                last_u = d.final_update_id;
                break;
            }
        }
        // 丢弃已消费的事件。
        buffer.drain(..apply_idx);

        // 回到步骤 5 循环：继续收事件并检查连续性。
    }
}

fn apply_delta(engine: &BookEngine, delta: &CachedDelta) {
    if !delta.bids.is_empty() {
        let _ = engine.apply_delta(Side::Bid, delta.bids.clone(), delta.final_update_id);
    }
    if !delta.asks.is_empty() {
        let _ = engine.apply_delta(Side::Ask, delta.asks.clone(), delta.final_update_id);
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
    loop {
        let resp = http
            .get(format!("{rest_url}/fapi/v1/depth"))
            .query(&[("symbol", symbol), ("limit", "1000")])
            .send()
            .await;
        match resp {
            Ok(r) => match r.json::<DepthSnapshot>().await {
                Ok(s) => return Ok(s),
                Err(e) => {
                    eprintln!("[binance-book] snapshot parse error: {e}");
                }
            },
            Err(e) => {
                eprintln!("[binance-book] snapshot fetch error: {e}");
            }
        }
        tokio::time::sleep(delay).await;
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

    #[test]
    fn parse_partial_snapshot_levels() {
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
    fn parse_levels_filters_invalid() {
        let raw = vec![
            ["abc".to_string(), "1.0".to_string()],
            ["100.0".to_string(), "".to_string()],
        ];
        assert_eq!(parse_levels(&raw).len(), 0);
    }

    /// 模拟完整的 snapshot → delta 对齐 → qty=0 删除流程。
    #[test]
    fn book_snapshot_then_delta_remove_zero_qty() {
        let engine = Arc::new(BookEngine::new());

        // 步骤 4：写入快照（模拟 REST 返回）。
        let snap_bids = vec![Level {
            price: dec!(50000),
            qty: dec!(1.5),
        }];
        let snap_asks = vec![Level {
            price: dec!(50100),
            qty: dec!(2.0),
        }];
        engine.apply_snapshot(snap_bids, snap_asks);
        let h = engine.handle();
        assert_eq!(h.top().unwrap().bid, dec!(50000));
        // 快照后 seq=0，准备接收第一个 delta。

        // 步骤 5-7：应用增量 —— 同一价位 qty=0 删除，新增价位是绝对量。
        engine
            .apply_delta(
                Side::Bid,
                vec![
                    Level {
                        price: dec!(50000),
                        qty: Decimal::ZERO,
                    }, // 删除
                    Level {
                        price: dec!(49900),
                        qty: dec!(3.0),
                    }, // 新增
                ],
                1,
            )
            .expect("delta ok");
        let top = h.top().unwrap();
        assert_eq!(top.bid, dec!(49900)); // 原 50000 被删除，最佳变为 49900
        assert_eq!(top.bid_qty, dec!(3.0));
    }

    /// 模拟 gap → 重建流程。
    #[test]
    fn gap_detection_triggers_rebuild() {
        let engine = Arc::new(BookEngine::new());

        // 快照后。
        engine.apply_snapshot(
            vec![Level {
                price: dec!(100),
                qty: dec!(1),
            }],
            vec![Level {
                price: dec!(101),
                qty: dec!(1),
            }],
        );

        // Deltas seq 1, 2 ok.
        engine
            .apply_delta(
                Side::Bid,
                vec![Level {
                    price: dec!(99),
                    qty: dec!(1),
                }],
                1,
            )
            .unwrap();
        engine
            .apply_delta(
                Side::Ask,
                vec![Level {
                    price: dec!(102),
                    qty: dec!(1),
                }],
                2,
            )
            .unwrap();
        assert_eq!(engine.handle().seq(), 2);

        // 跳号：seq 5（缺 3,4）→ 清簿 + Stale。
        let err = engine
            .apply_delta(
                Side::Bid,
                vec![Level {
                    price: dec!(98),
                    qty: dec!(1),
                }],
                5,
            )
            .unwrap_err();
        assert!(matches!(err, NexusError::Stale));
        assert!(engine.handle().top().is_none(), "book cleared on gap");
    }
}
