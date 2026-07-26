//! 统一基础类型：交易所标识、交易对、订单、精度元数据。
//!
//! 原则 P10：资金相关数值全程 `rust_decimal::Decimal`，禁止浮点。

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::error::NexusError;

/// 交易所标识。静态字符串，可自由扩展新所。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct VenueId(pub &'static str);

impl VenueId {
    pub const HYPE: VenueId = VenueId("HYPE");
    pub const LIGHTER: VenueId = VenueId("LIGHTER");
    pub const BINANCE_FUT: VenueId = VenueId("BINANCE_FUT");
    pub const BINANCE_SPOT: VenueId = VenueId("BINANCE_SPOT");
    pub const OKX: VenueId = VenueId("OKX");
    pub const BYBIT: VenueId = VenueId("BYBIT");
    pub const GATE: VenueId = VenueId("GATE");
    pub const BITGET: VenueId = VenueId("BITGET");

    pub fn as_str(&self) -> &'static str {
        self.0
    }
}

impl fmt::Display for VenueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

/// 统一交易对。`venue_native` 保存交易所原生符号（如 "BTCUSDT" / "BTC-USDT-SWAP"）。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Symbol {
    pub base: String,
    pub quote: String,
    pub venue_native: String,
}

impl Symbol {
    pub fn new(
        base: impl Into<String>,
        quote: impl Into<String>,
        venue_native: impl Into<String>,
    ) -> Self {
        Self {
            base: base.into(),
            quote: quote.into(),
            venue_native: venue_native.into(),
        }
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.base, self.quote)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn opposite(&self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

/// Time in force。PostOnly 单独成档：交易所语义上它与 GTC 互斥。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Tif {
    Gtc,
    Ioc,
    Fok,
    PostOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderKind {
    Limit { price: Decimal },
    Market,
}

/// SDK 生成的全局唯一客户端订单号，全链路幂等键。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClientOrderId(pub String);

impl fmt::Display for ClientOrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// 客户端订单号生成器：`{prefix}-{seed}-{counter}`。
///
/// 进程内计数器保证唯一；`seed` 由调用方传入（建议启动时间戳），保证跨进程重启唯一。
pub struct ClientIdGen {
    prefix: String,
    seed: u64,
    counter: AtomicU64,
}

impl ClientIdGen {
    pub fn new(prefix: impl Into<String>, seed: u64) -> Self {
        Self {
            prefix: prefix.into(),
            seed,
            counter: AtomicU64::new(0),
        }
    }

    pub fn next(&self) -> ClientOrderId {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        ClientOrderId(format!("{}-{}-{}", self.prefix, self.seed, n))
    }
}

/// 订单引用：撤单/查询用。`venue_order_id` 在 ack 后回填。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderRef {
    pub symbol: Symbol,
    pub client_id: ClientOrderId,
    pub venue_order_id: Option<String>,
}

/// 统一新订单。通过构造器 + 链式方法创建，杜绝字段遗漏。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewOrder {
    pub symbol: Symbol,
    pub side: Side,
    pub kind: OrderKind,
    pub qty: Decimal,
    pub tif: Tif,
    pub reduce_only: bool,
    pub client_id: ClientOrderId,
}

impl NewOrder {
    pub fn limit(
        symbol: Symbol,
        side: Side,
        price: Decimal,
        qty: Decimal,
        client_id: ClientOrderId,
    ) -> Self {
        Self {
            symbol,
            side,
            kind: OrderKind::Limit { price },
            qty,
            tif: Tif::Gtc,
            reduce_only: false,
            client_id,
        }
    }

    pub fn market(symbol: Symbol, side: Side, qty: Decimal, client_id: ClientOrderId) -> Self {
        Self {
            symbol,
            side,
            kind: OrderKind::Market,
            qty,
            tif: Tif::Ioc,
            reduce_only: false,
            client_id,
        }
    }

    pub fn ioc(mut self) -> Self {
        self.tif = Tif::Ioc;
        self
    }

    pub fn fok(mut self) -> Self {
        self.tif = Tif::Fok;
        self
    }

    pub fn post_only(mut self) -> Self {
        self.tif = Tif::PostOnly;
        self
    }

    pub fn reduce_only(mut self) -> Self {
        self.reduce_only = true;
        self
    }

    /// 限价单价格；市价单返回 None。
    pub fn price(&self) -> Option<Decimal> {
        match &self.kind {
            OrderKind::Limit { price } => Some(*price),
            OrderKind::Market => None,
        }
    }
}

/// 交易对精度元数据。由 adapter 初始化时从交易所拉取。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SymbolMeta {
    pub tick_size: Decimal,
    pub lot_size: Decimal,
    pub min_notional: Decimal,
}

impl SymbolMeta {
    /// 价格向下对齐 tick。策略不自己算精度。
    pub fn quantize_price(&self, price: Decimal) -> Decimal {
        quantize_floor(price, self.tick_size)
    }

    /// 数量向下对齐 lot。
    pub fn quantize_qty(&self, qty: Decimal) -> Decimal {
        quantize_floor(qty, self.lot_size)
    }

    /// 下单前校验：对齐 + 最小名义额。返回违规原因。
    pub fn validate(&self, order: &NewOrder) -> Result<(), NexusError> {
        if order.qty <= Decimal::ZERO {
            return Err(NexusError::InvalidOrder("qty must be positive".into()));
        }
        if self.quantize_qty(order.qty) != order.qty {
            return Err(NexusError::InvalidOrder(format!(
                "qty {} not aligned to lot {}",
                order.qty, self.lot_size
            )));
        }
        if let Some(price) = order.price() {
            if price <= Decimal::ZERO {
                return Err(NexusError::InvalidOrder("price must be positive".into()));
            }
            if self.quantize_price(price) != price {
                return Err(NexusError::InvalidOrder(format!(
                    "price {} not aligned to tick {}",
                    price, self.tick_size
                )));
            }
            if price * order.qty < self.min_notional {
                return Err(NexusError::InvalidOrder(format!(
                    "notional {} below minimum {}",
                    price * order.qty,
                    self.min_notional
                )));
            }
        }
        Ok(())
    }
}

/// 向下对齐到 step 的整数倍。step <= 0 时原样返回（视为无约束）。
fn quantize_floor(value: Decimal, step: Decimal) -> Decimal {
    if step <= Decimal::ZERO {
        return value;
    }
    (value / step).floor() * step
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn meta() -> SymbolMeta {
        SymbolMeta {
            tick_size: dec!(0.5),
            lot_size: dec!(0.001),
            min_notional: dec!(10),
        }
    }

    fn btc() -> Symbol {
        Symbol::new("BTC", "USDT", "BTCUSDT")
    }

    #[test]
    fn quantize_price_floors_to_tick() {
        assert_eq!(meta().quantize_price(dec!(65000.7)), dec!(65000.5));
        assert_eq!(meta().quantize_price(dec!(65000.5)), dec!(65000.5));
    }

    #[test]
    fn quantize_qty_floors_to_lot() {
        assert_eq!(meta().quantize_qty(dec!(0.0019)), dec!(0.001));
        assert_eq!(meta().quantize_qty(dec!(0.010)), dec!(0.010));
    }

    #[test]
    fn validate_rejects_misaligned_and_small_orders() {
        let m = meta();
        let id = ClientOrderId("t-1".into());

        let misaligned_qty =
            NewOrder::limit(btc(), Side::Buy, dec!(65000.5), dec!(0.0015), id.clone());
        assert!(m.validate(&misaligned_qty).is_err());

        let misaligned_price =
            NewOrder::limit(btc(), Side::Buy, dec!(65000.7), dec!(0.001), id.clone());
        assert!(m.validate(&misaligned_price).is_err());

        let below_notional =
            NewOrder::limit(btc(), Side::Buy, dec!(100.5), dec!(0.001), id.clone());
        assert!(m.validate(&below_notional).is_err());

        let ok = NewOrder::limit(btc(), Side::Buy, dec!(65000.5), dec!(0.001), id);
        assert!(m.validate(&ok).is_ok());
    }

    #[test]
    fn client_id_gen_is_unique_and_prefixed() {
        let g = ClientIdGen::new("nx", 42);
        let a = g.next();
        let b = g.next();
        assert_ne!(a, b);
        assert!(a.0.starts_with("nx-42-"));
    }

    #[test]
    fn order_builder_chains() {
        let o = NewOrder::limit(
            btc(),
            Side::Sell,
            dec!(65000.5),
            dec!(0.001),
            ClientOrderId("t-2".into()),
        )
        .ioc()
        .reduce_only();
        assert_eq!(o.tif, Tif::Ioc);
        assert!(o.reduce_only);
        assert_eq!(o.price(), Some(dec!(65000.5)));
    }
}
