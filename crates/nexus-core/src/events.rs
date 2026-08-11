//! 统一事件与行情读取类型。
//!
//! 时间口径：`local_recv_ms` 一律为本地接收时刻（local-E），毫秒 epoch；
//! `venue_ts_ms` 为交易所侧时间戳（local-T 类），仅供参考，不作延迟基准。

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::order_state::OrderState;
use crate::types::{ClientOrderId, OrderRef, Side, Symbol};

/// 盘口一档。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopOfBook {
    pub bid: Decimal,
    pub bid_qty: Decimal,
    pub ask: Decimal,
    pub ask_qty: Decimal,
}

/// 订单簿多档视图（读侧快照，价格降序 bids / 升序 asks）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookView {
    pub bids: Vec<(Decimal, Decimal)>,
    pub asks: Vec<(Decimal, Decimal)>,
    pub seq: u64,
    pub local_recv_ms: i64,
    /// 最近事件的 E（交易所网关吐出时间，local-E）。0 = 未提供。
    #[serde(default)]
    pub gateway_ts_ms: i64,
    /// 最近事件的 T（交易所撮合时间，local-T）。0 = 未提供。
    #[serde(default)]
    pub venue_ts_ms: i64,
}

/// 公开成交。仅作特征输入（主动方向/量），不作成交确认与延迟基准。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicTrade {
    pub symbol: Symbol,
    pub price: Decimal,
    pub qty: Decimal,
    /// 主动方（taker）方向。
    pub aggressor: Side,
    pub venue_ts_ms: i64,
    pub local_recv_ms: i64,
}

/// 私有成交回报：唯一合法的成交确认来源。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fill {
    pub order: OrderRef,
    pub side: Side,
    pub price: Decimal,
    pub qty: Decimal,
    pub fee: Decimal,
    pub fee_currency: String,
    pub is_maker: bool,
    pub venue_ts_ms: i64,
    pub local_recv_ms: i64,
}

/// 订单状态机流转事件（SDK 保证状态序列一致，绝不跳变）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderUpdate {
    pub client_id: ClientOrderId,
    pub symbol: Symbol,
    pub state: OrderState,
    pub filled_qty: Decimal,
    pub reason: Option<String>,
    pub local_recv_ms: i64,
}

/// 仓位。`qty` 带符号：正=多，负=空。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub symbol: Symbol,
    pub qty: Decimal,
    pub entry_price: Option<Decimal>,
    pub local_recv_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Balance {
    pub asset: String,
    pub total: Decimal,
    pub available: Decimal,
    pub local_recv_ms: i64,
}

/// 连接状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnState {
    Connected,
    Reconnecting,
    Down,
}

/// 私有流统一事件。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccountEvent {
    OrderUpdate(OrderUpdate),
    Fill(Fill),
    PositionUpdate(Position),
    BalanceUpdate(Balance),
    ConnectionState(ConnState),
}

/// REST 兜底对账快照。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountSnapshot {
    pub positions: Vec<Position>,
    pub balances: Vec<Balance>,
    pub open_orders: Vec<OrderRef>,
    pub local_recv_ms: i64,
}
