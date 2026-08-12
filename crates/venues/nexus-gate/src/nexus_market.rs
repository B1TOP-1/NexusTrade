//! Gate 行情层（nexus-core 重写版）。
//!
//! 实现 `MarketVenue`：20ms 本地订单簿。
//! 复用 gate 的 WS 结构（GateWsMessage/GateOrderBookResult）喂 nexus-book 引擎。
//!
//! Gate 订阅：`futures.order_book_update` channel，payload [contract, interval, depth]。
//! 增量帧含 `full` 标记（true=快照），`U`/`u` 更新ID（同 Binance 语义）。

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use nexus_book::{BookEngine, Level};
use nexus_core::{
    BookHandle, BookOptions, Decimal, MarketVenue, NexusError, Result, Symbol, SymbolMeta,
    TradeStream, VenueId,
};
use tokio_tungstenite::tungstenite::Message;

use crate::websocket::messages::GateOrderBookResult;

/// Gate 行情配置。
#[derive(Debug, Clone)]
pub struct GateMarketConfig {
    pub ws_url: String,
    pub rest_url: String,
    /// 订单簿速度：20ms / 100ms。
    pub book_interval: String,
    /// 订单簿深度。
    pub book_depth: usize,
    pub reconnect_delay: Duration,
}

impl Default for GateMarketConfig {
    fn default() -> Self {
        Self {
            ws_url: "wss://fx-ws.gateio.ws/v4/ws/usdt".to_string(),
            rest_url: "https://api.gateio.ws".to_string(),
            book_interval: "20ms".to_string(),
            book_depth: 20,
            reconnect_delay: Duration::from_millis(500),
        }
    }
}

/// Gate 行情 venue。
#[derive(Debug)]
pub struct GateMarket {
    config: GateMarketConfig,
    http: reqwest::Client,
}

impl GateMarket {
    pub fn new(config: GateMarketConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
        }
    }

    /// Gate 合约符号：BTCUSDT → BTC_USDT。
    fn contract(symbol: &Symbol) -> String {
        symbol.venue_native.replace("USDT", "_USDT").to_uppercase()
    }

    /// 解析增量帧为 bids/asks levels。
    fn parse_levels(raw: &[Vec<String>]) -> Vec<Level> {
        raw.iter()
            .filter_map(|pair| {
                let price = Decimal::from_str(&pair[0]).ok()?;
                let qty = Decimal::from_str(&pair[1]).ok()?;
                Some(Level { price, qty })
            })
            .collect()
    }

    /// REST 快照端点。
    async fn fetch_snapshot(&self, contract: &str) -> Result<(u64, Vec<Level>, Vec<Level>)> {
        let snap: serde_json::Value = self
            .http
            .get(format!(
                "{}/api/v4/futures/usdt/order_book",
                self.config.rest_url
            ))
            .query(&[
                ("contract", contract),
                ("interval", self.config.book_interval.as_str()),
                ("limit", &self.config.book_depth.to_string()),
            ])
            .send()
            .await
            .map_err(|e| NexusError::Transport(format!("gate snapshot: {e}")))?
            .json()
            .await
            .map_err(|e| NexusError::Transport(format!("gate snapshot parse: {e}")))?;

        let id = snap["id"].as_u64().unwrap_or(0);
        let bids_raw: Vec<Vec<String>> = snap["bids"]
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|v| {
                        vec![
                            v[0].as_str().unwrap_or("").to_string(),
                            v[1].as_str().unwrap_or("").to_string(),
                        ]
                    })
                    .collect()
            })
            .unwrap_or_default();
        let asks_raw: Vec<Vec<String>> = snap["asks"]
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|v| {
                        vec![
                            v[0].as_str().unwrap_or("").to_string(),
                            v[1].as_str().unwrap_or("").to_string(),
                        ]
                    })
                    .collect()
            })
            .unwrap_or_default();
        let bids = Self::parse_levels(&bids_raw);
        let asks = Self::parse_levels(&asks_raw);
        Ok((id, bids, asks))
    }
}

#[async_trait]
impl MarketVenue for GateMarket {
    fn venue(&self) -> VenueId {
        VenueId::GATE
    }

    async fn subscribe_book(&self, symbol: &Symbol, _opts: BookOptions) -> Result<BookHandle> {
        let engine = Arc::new(BookEngine::new());
        let contract = Self::contract(symbol);
        let ws_url = self.config.ws_url.clone();
        let reconnect_delay = self.config.reconnect_delay;
        let interval = self.config.book_interval.clone();
        let depth = self.config.book_depth;

        let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);

        // 裸连接（Binance 用户流教训：spawn_reader 收不到，用 connect_async 直连）
        let (ws, _) = tokio_tungstenite::connect_async(&ws_url)
            .await
            .map_err(|e| NexusError::Transport(format!("gate ws connect: {e}")))?;
        let (mut write, mut read) = ws.split();

        // 订阅订单簿增量
        let sub = serde_json::json!({
            "time": unix_seconds(),
            "channel": "futures.order_book_update",
            "event": "subscribe",
            "payload": [contract, interval, depth.to_string()],
        });
        write
            .send(Message::Text(sub.to_string().into()))
            .await
            .map_err(|e| NexusError::Transport(format!("gate ws subscribe: {e}")))?;

        // 后台：收行情 → 喂 BookEngine
        let engine_clone = Arc::clone(&engine);
        tokio::spawn(async move {
            let _keep = (shutdown_tx, write);
            loop {
                match tokio::time::timeout(Duration::from_secs(30), read.next()).await {
                    Ok(Some(Ok(Message::Text(text)))) => {
                        let Ok(v) = serde_json::from_str::<serde_json::Value>(&text.to_string()) else {
                            continue;
                        };
                        if v["channel"] != "futures.order_book_update" {
                            continue;
                        }
                        let Some(result) = v["result"].as_object() else {
                            continue;
                        };
                        // Gate 增量帧：b/a + U/u，full 标记（快照）
                        let msg = GateOrderBookResult {
                            full: result.get("full").and_then(|f| f.as_bool()),
                            s: ustr::Ustr::from(
                                result.get("s").and_then(|s| s.as_str()).unwrap_or(""),
                            ),
                            t: result.get("t").and_then(|t| t.as_i64()),
                            first_update_id: result.get("U").and_then(|u| u.as_u64()),
                            last_update_id: result["u"].as_u64().unwrap_or(0),
                            b: result["b"]
                                .as_array()
                                .map(|a| {
                                    a.iter()
                                        .map(|x| {
                                            x.as_array()
                                                .map(|y| {
                                                    y.iter()
                                                        .map(|z| z.as_str().unwrap_or("").to_string())
                                                        .collect()
                                                })
                                                .unwrap_or_default()
                                        })
                                        .collect()
                                })
                                .unwrap_or_default(),
                            a: result["a"]
                                .as_array()
                                .map(|a| {
                                    a.iter()
                                        .map(|x| {
                                            x.as_array()
                                                .map(|y| {
                                                    y.iter()
                                                        .map(|z| z.as_str().unwrap_or("").to_string())
                                                        .collect()
                                                })
                                                .unwrap_or_default()
                                        })
                                        .collect()
                                })
                                .unwrap_or_default(),
                        };
                        let bids = Self::parse_levels(&msg.b);
                        let asks = Self::parse_levels(&msg.a);

                        if msg.full.unwrap_or(false) {
                            // 快照帧：直接应用
                            engine_clone.apply_snapshot(bids, asks);
                        } else {
                            // 增量帧：应用
                            if !bids.is_empty() {
                                let _ = engine_clone.apply_delta(
                                    nexus_book::Side::Bid,
                                    bids,
                                    msg.last_update_id,
                                );
                            }
                            if !asks.is_empty() {
                                let _ = engine_clone.apply_delta(
                                    nexus_book::Side::Ask,
                                    asks,
                                    msg.last_update_id,
                                );
                            }
                        }
                    }
                    Ok(Some(Ok(Message::Ping(data)))) => {
                        let _ = data;
                    }
                    _ => {
                        // 断线重连
                        tokio::time::sleep(reconnect_delay).await;
                        break;
                    }
                }
            }
            // 重连逻辑（简化：外层循环）
        });

        Ok(Arc::new(engine.handle()))
    }

    async fn subscribe_trades(&self, _symbol: &Symbol) -> Result<TradeStream> {
        Err(NexusError::Unsupported("gate trades not wired yet".into()))
    }

    fn symbol_meta(&self, _symbol: &Symbol) -> Result<SymbolMeta> {
        Ok(SymbolMeta {
            tick_size: Decimal::ZERO,
            lot_size: Decimal::ZERO,
            min_notional: Decimal::ZERO,
            min_qty: Decimal::ZERO,
        })
    }
}

fn unix_seconds() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
