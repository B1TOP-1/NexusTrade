//! Lighter 行情 adapter：包装 bybot-market-engine 的 spawn_live_lighter。
//!
//! 薄包装：底层任务负责 WS、nonce 连续性、重连；本层只做类型转换与
//! BookReader 投影。vwap 仅在名义额与订阅时配置一致时返回（见 BookOptions）。

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bybot_lighter::data::LighterMarketSpec;
use bybot_lighter::http::LighterHttpClient;
use bybot_market_engine::live_lighter::{
    resolve_lighter_market, spawn_live_lighter, LighterBookUpdate, LiveLighterConfig,
    MAINNET_WS_URL,
};
use nexus_core::{
    BookHandle, BookOptions, BookReader, BookView, Decimal, MarketVenue, NexusError, Result,
    Symbol, SymbolMeta, TopOfBook, TradeStream, VenueId,
};
use rust_decimal::prelude::ToPrimitive;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::now_ms;

/// Lighter 行情配置。默认主网。
#[derive(Debug, Clone)]
pub struct LighterMarketConfig {
    pub markets_url: String,
    pub ws_url: String,
    pub http_url: String,
    pub reconnect_delay_ms: u64,
    pub heartbeat_interval_ms: u64,
    /// 未在 BookOptions 指定时的默认 VWAP 名义额（USD）。
    pub default_vwap_notional: Decimal,
}

impl Default for LighterMarketConfig {
    fn default() -> Self {
        Self {
            markets_url: bybot_market_engine::live_lighter::MAINNET_API_URL.to_string(),
            ws_url: MAINNET_WS_URL.to_string(),
            http_url: "https://mainnet.zklighter.elliot.ai".to_string(),
            reconnect_delay_ms: 500,
            heartbeat_interval_ms: 20_000,
            default_vwap_notional: Decimal::from(2_000),
        }
    }
}

/// Lighter 行情 venue。
pub struct LighterMarket {
    config: LighterMarketConfig,
    specs: Vec<LighterMarketSpec>,
}

impl LighterMarket {
    /// 连接并加载市场元数据（精度/lot/min_qty 来源）。
    pub async fn connect(config: LighterMarketConfig) -> Result<Self> {
        let http = LighterHttpClient::new(&config.http_url)
            .map_err(|e| NexusError::Transport(format!("lighter http client: {e}")))?;
        let specs = http
            .market_specs()
            .await
            .map_err(|e| NexusError::Transport(format!("lighter market specs: {e}")))?;
        Ok(Self { config, specs })
    }

    pub async fn connect_mainnet() -> Result<Self> {
        Self::connect(LighterMarketConfig::default()).await
    }

    fn spec(&self, symbol: &Symbol) -> Result<&LighterMarketSpec> {
        self.specs
            .iter()
            .find(|s| s.symbol == symbol.venue_native)
            .ok_or_else(|| {
                NexusError::InvalidOrder(format!("unknown lighter symbol: {}", symbol.venue_native))
            })
    }
}

#[async_trait]
impl MarketVenue for LighterMarket {
    fn venue(&self) -> VenueId {
        VenueId::LIGHTER
    }

    async fn subscribe_book(&self, symbol: &Symbol, opts: BookOptions) -> Result<BookHandle> {
        let market = resolve_lighter_market(&symbol.venue_native, &self.config.markets_url)
            .await
            .map_err(NexusError::Transport)?;

        let vwap_notional = opts
            .vwap_notional
            .unwrap_or(self.config.default_vwap_notional);
        let depth_notional_usd = vwap_notional.to_f64().ok_or_else(|| {
            NexusError::InvalidOrder(format!("vwap notional not representable: {vwap_notional}"))
        })?;

        let (rx, task) = spawn_live_lighter(LiveLighterConfig {
            ticker: symbol.venue_native.clone(),
            market_id: market.market_id,
            ws_url: self.config.ws_url.clone(),
            reconnect_delay_ms: self.config.reconnect_delay_ms,
            heartbeat_interval_ms: self.config.heartbeat_interval_ms,
            depth_notional_usd,
        });

        Ok(Arc::new(LighterBookHandle {
            rx,
            vwap_notional,
            _task: AbortOnDrop(task),
        }))
    }

    async fn subscribe_trades(&self, _symbol: &Symbol) -> Result<TradeStream> {
        // bybot-market-engine 未暴露公开成交流；成交确认一律走私有回报（架构 §2）。
        Err(NexusError::Unsupported(
            "lighter public trade stream not wired in M1".into(),
        ))
    }

    fn symbol_meta(&self, symbol: &Symbol) -> Result<SymbolMeta> {
        let spec = self.spec(symbol)?;
        let tick = pow10_neg(spec.supported_price_decimals);
        let lot = pow10_neg(spec.supported_size_decimals);
        Ok(SymbolMeta {
            tick_size: tick,
            lot_size: lot,
            min_notional: Decimal::ZERO,
            min_qty: spec.min_base_amount,
        })
    }
}

fn pow10_neg(decimals: u8) -> Decimal {
    Decimal::new(1, u32::from(decimals))
}

/// watch 通道投影为 BookReader。
struct LighterBookHandle {
    rx: watch::Receiver<Option<LighterBookUpdate>>,
    vwap_notional: Decimal,
    _task: AbortOnDrop,
}

impl LighterBookHandle {
    fn latest(&self) -> Option<LighterBookUpdate> {
        self.rx.borrow().clone()
    }
}

impl BookReader for LighterBookHandle {
    fn top(&self) -> Option<TopOfBook> {
        let u = self.latest()?;
        let bid = Decimal::from_str(&u.bid).ok()?;
        let ask = Decimal::from_str(&u.ask).ok()?;
        Some(TopOfBook {
            bid,
            // 上游只回传价格；数量在薄包装层不可得，置 0（不可作深度判断依据）。
            bid_qty: Decimal::ZERO,
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
        // 薄包装仅有一档投影。
        let (bids, asks, seq, ts) = match self.latest() {
            Some(u) => {
                let bid = Decimal::from_str(&u.bid).ok();
                let ask = Decimal::from_str(&u.ask).ok();
                (
                    bid.map(|p| vec![(p, Decimal::ZERO)]).unwrap_or_default(),
                    ask.map(|p| vec![(p, Decimal::ZERO)]).unwrap_or_default(),
                    u.nonce.unwrap_or(0),
                    (u.timestamp_ns / 1_000_000) as i64,
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
                let update_ms = (u.timestamp_ns / 1_000_000) as i64;
                let age = now_ms().saturating_sub(update_ms);
                Duration::from_millis(age.max(0) as u64)
            }
            None => Duration::MAX, // 断线/未就绪：无限陈旧，fail-closed。
        }
    }

    fn seq(&self) -> u64 {
        self.latest().and_then(|u| u.nonce).unwrap_or(0)
    }
}

/// JoinHandle 守护：句柄 Drop 即 abort 底层 WS 任务。
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
