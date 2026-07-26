//! 三大 trait：每个交易所 adapter 必须完整实现后方可标记 ready。
//!
//! 接入清单见 docs/architecture.md §9。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use crate::capabilities::VenueCapabilities;
use crate::events::{AccountEvent, AccountSnapshot, BookView, PublicTrade, TopOfBook};
use crate::types::{ClientOrderId, NewOrder, OrderRef, Symbol, SymbolMeta, VenueId};
use crate::Result;

/// 订单簿订阅选项。
#[derive(Debug, Clone, Copy)]
pub struct BookOptions {
    /// 双轨订阅（P7）：两条 WS 按序列号去重互补。
    pub dual_feed: bool,
    /// 请求交易所最快增量档位（如 Binance futures depth@0ms）。
    pub fastest: bool,
}

impl Default for BookOptions {
    fn default() -> Self {
        Self {
            dual_feed: false,
            fastest: true,
        }
    }
}

/// 本地订单簿只读接口。写侧由 nexus-book 引擎独占，读侧无锁快照。
pub trait BookReader: Send + Sync {
    /// 盘口一档。簿未就绪或已判死返回 None。
    fn top(&self) -> Option<TopOfBook>;

    /// 指定名义额深度 VWAP（Edge 计算标配）。
    /// 深度不足名义额时返回 None（不可交易），不返回劣化值。
    fn vwap(&self, notional: Decimal) -> Option<(Decimal, Decimal)>;

    /// 多档视图快照。
    fn depth(&self, levels: usize) -> BookView;

    /// 距最后一次更新的时长（local-E 口径）。P3 watchdog 的数据源。
    fn staleness(&self) -> Duration;

    /// 当前序列号/nonce。
    fn seq(&self) -> u64;
}

/// 订单簿句柄：策略侧持有的只读引用。
pub type BookHandle = Arc<dyn BookReader>;

/// 公开成交流（仅特征输入）。
pub type TradeStream = mpsc::Receiver<PublicTrade>;

/// 私有账户事件流。
pub type AccountStream = mpsc::Receiver<AccountEvent>;

/// 下单确认。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderAck {
    pub client_id: ClientOrderId,
    pub venue_order_id: Option<String>,
}

/// 行情：本地订单簿 + 公开成交。
#[async_trait]
pub trait MarketVenue: Send + Sync {
    fn venue(&self) -> VenueId;

    /// 订阅并维护本地订单簿，返回只读句柄。
    /// 内部负责：快照+增量、序列校验、断裂重建、重连重订阅。
    async fn subscribe_book(&self, symbol: &Symbol, opts: BookOptions) -> Result<BookHandle>;

    /// 订阅公开成交流。
    async fn subscribe_trades(&self, symbol: &Symbol) -> Result<TradeStream>;

    /// 交易对精度元数据（初始化时拉取并缓存）。
    fn symbol_meta(&self, symbol: &Symbol) -> Result<SymbolMeta>;
}

/// 执行：下单/撤单，WS 优先（P1）。
#[async_trait]
pub trait ExecutionVenue: Send + Sync {
    fn venue(&self) -> VenueId;

    fn capabilities(&self) -> VenueCapabilities;

    /// 私有通道就绪且可下单。
    fn is_ready(&self) -> bool;

    async fn place(&self, order: NewOrder) -> Result<OrderAck>;

    /// 批量下单：本地攒单一次 flush（交易所支持时单帧发出）。
    async fn place_batch(&self, orders: Vec<NewOrder>) -> Result<Vec<Result<OrderAck>>>;

    async fn cancel(&self, order: &OrderRef) -> Result<()>;

    async fn cancel_batch(&self, orders: &[OrderRef]) -> Result<Vec<Result<()>>>;

    /// 一键撤单（kill-switch 快速通道，必须实现）。
    /// `symbol = None` 撤全部。无原生支持时 SDK 逐单模拟。
    async fn cancel_all(&self, symbol: Option<&Symbol>) -> Result<()>;
}

/// 私有状态：订单回报、成交、仓位、余额。
#[async_trait]
pub trait PrivateVenue: Send + Sync {
    fn venue(&self) -> VenueId;

    /// 订阅统一账户事件流。断线自动重连并发出 ConnectionState 事件。
    async fn subscribe(&self) -> Result<AccountStream>;

    /// REST 兜底快照（Unknown 态对账用）。
    async fn snapshot(&self) -> Result<AccountSnapshot>;
}
