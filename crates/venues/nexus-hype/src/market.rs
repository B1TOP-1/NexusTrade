//! Hyperliquid 行情 adapter：包装 bybot-hype 的 spawn_live_book。
//!
//! 薄包装：底层任务负责 WS、快照校验、staleness 判死、重连；
//! 本层只做类型转换与 BookReader 投影。

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bybot_hype::markets::MarketCatalog;
use bybot_hype::public_ws::{spawn_live_book, LiveBookConfig, LiveBookUpdate, MAINNET_WS_URL};
use hypersdk::hypercore;
use nexus_core::{
    now_ms, BookHandle, BookOptions, BookReader, BookView, Decimal, MarketVenue, NexusError,
    Result, Symbol, SymbolMeta, TopOfBook, TradeStream, VenueId,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// Hyperliquid 行情配置。默认主网。
#[derive(Debug, Clone)]
pub struct HypeMarketConfig {
    pub ws_url: String,
    /// 订阅 fast 档增量。
    pub fastest: bool,
    pub stale_after_ms: u64,
    pub reconnect_delay: Duration,
    pub heartbeat_interval: Duration,
    /// 未在 BookOptions 指定时的默认 VWAP 名义额（USD）。
    pub default_vwap_notional: Decimal,
    /// Hyperliquid 全站最小订单名义额（未在 API 元数据中显式给出）。
    pub min_notional: Decimal,
}

impl Default for HypeMarketConfig {
    fn default() -> Self {
        Self {
            ws_url: MAINNET_WS_URL.to_string(),
            fastest: true,
            stale_after_ms: 3_000,
            reconnect_delay: Duration::from_millis(500),
            heartbeat_interval: Duration::from_secs(20),
            default_vwap_notional: Decimal::from(2_000),
            min_notional: Decimal::from(10),
        }
    }
}

/// Hyperliquid 行情 venue。
pub struct HypeMarket {
    config: HypeMarketConfig,
    catalog: MarketCatalog,
}

impl HypeMarket {
    /// 连接主网并加载市场目录（精度来源）。
    pub async fn connect_mainnet() -> Result<Self> {
        Self::connect(HypeMarketConfig::default()).await
    }

    pub async fn connect(config: HypeMarketConfig) -> Result<Self> {
        let client = hypercore::mainnet();
        let catalog = MarketCatalog::load(&client)
            .await
            .map_err(|e| NexusError::Transport(format!("hype market catalog: {e}")))?;
        Ok(Self { config, catalog })
    }
}

#[async_trait]
impl MarketVenue for HypeMarket {
    fn venue(&self) -> VenueId {
        VenueId::HYPE
    }

    async fn subscribe_book(&self, symbol: &Symbol, opts: BookOptions) -> Result<BookHandle> {
        let vwap_notional = opts
            .vwap_notional
            .unwrap_or(self.config.default_vwap_notional);

        let (rx, task) = spawn_live_book(LiveBookConfig {
            symbol: symbol.venue_native.clone(),
            ws_url: self.config.ws_url.clone(),
            fast: opts.fastest && self.config.fastest,
            stale_after_ms: self.config.stale_after_ms,
            reconnect_delay: self.config.reconnect_delay,
            heartbeat_interval: self.config.heartbeat_interval,
            depth_notional_usd: vwap_notional,
        });

        Ok(Arc::new(HypeBookHandle {
            rx,
            vwap_notional,
            _task: AbortOnDrop(task),
        }))
    }

    async fn subscribe_trades(&self, _symbol: &Symbol) -> Result<TradeStream> {
        // bybot-hype 只接 l2Book；成交确认一律走私有回报（架构 §2）。
        Err(NexusError::Unsupported(
            "hype public trade stream not wired in M1".into(),
        ))
    }

    fn symbol_meta(&self, symbol: &Symbol) -> Result<SymbolMeta> {
        let descriptor = self.catalog.get(&symbol.venue_native).ok_or_else(|| {
            NexusError::InvalidOrder(format!("unknown hype symbol: {}", symbol.venue_native))
        })?;
        Ok(SymbolMeta {
            // Hyperliquid 无显式 tick：价格舍入由 gateway 的 round_by_side 内部处理。
            tick_size: Decimal::ZERO,
            lot_size: descriptor.precision().size_step(),
            min_notional: self.config.min_notional,
            min_qty: Decimal::ZERO,
        })
    }
}

/// watch 通道投影为 BookReader。
struct HypeBookHandle {
    rx: watch::Receiver<Option<LiveBookUpdate>>,
    vwap_notional: Decimal,
    _task: AbortOnDrop,
}

impl HypeBookHandle {
    fn latest(&self) -> Option<LiveBookUpdate> {
        self.rx.borrow().clone()
    }
}

impl BookReader for HypeBookHandle {
    fn top(&self) -> Option<TopOfBook> {
        let u = self.latest()?;
        let bid = Decimal::from_str(&u.bid).ok()?;
        let ask = Decimal::from_str(&u.ask).ok()?;
        Some(TopOfBook {
            bid,
            bid_qty: Decimal::ZERO, // 上游只回传价格
            ask,
            ask_qty: Decimal::ZERO,
        })
    }

    fn vwap(&self, notional: Decimal) -> Option<(Decimal, Decimal)> {
        if notional != self.vwap_notional {
            return None; // 只承诺订阅时配置的名义额，绝不返回劣化值。
        }
        let u = self.latest()?;
        let wb = Decimal::from_str(u.weighted_bid.as_deref()?).ok()?;
        let wa = Decimal::from_str(u.weighted_ask.as_deref()?).ok()?;
        Some((wb, wa))
    }

    fn depth(&self, _levels: usize) -> BookView {
        let (bids, asks, seq, ts) = match self.latest() {
            Some(u) => {
                let bid = Decimal::from_str(&u.bid).ok();
                let ask = Decimal::from_str(&u.ask).ok();
                (
                    bid.map(|p| vec![(p, Decimal::ZERO)]).unwrap_or_default(),
                    ask.map(|p| vec![(p, Decimal::ZERO)]).unwrap_or_default(),
                    u.exchange_time_ms,
                    u.received_time_ms as i64,
                )
            }
            None => (Vec::new(), Vec::new(), 0, 0),
        };
        BookView {
            bids,
            asks,
            seq,
            local_recv_ms: ts,
            gateway_ts_ms: 0,
            venue_ts_ms: 0,
        }
    }

    fn staleness(&self) -> Duration {
        match self.latest() {
            Some(u) => {
                let age = now_ms().saturating_sub(u.received_time_ms as i64);
                Duration::from_millis(age.max(0) as u64)
            }
            None => Duration::MAX, // 断线/未就绪：无限陈旧，fail-closed。
        }
    }

    fn seq(&self) -> u64 {
        // Hyperliquid 无显式序列号，用交易所时间戳单调性充当。
        self.latest().map(|u| u.exchange_time_ms).unwrap_or(0)
    }
}

/// JoinHandle 守护：句柄 Drop 即 abort 底层 WS 任务。
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
