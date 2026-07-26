//! nexus-core：NexusTrade L0 层。
//!
//! 统一类型 + 三大 trait + 订单状态机。零 IO、零交易所依赖。
//! 行为规范见 docs/architecture.md（唯一真理来源）。

pub mod capabilities;
pub mod error;
pub mod events;
pub mod order_state;
pub mod traits;
pub mod types;

pub use capabilities::VenueCapabilities;
pub use error::NexusError;
pub use events::{
    AccountEvent, AccountSnapshot, Balance, BookView, ConnState, Fill, OrderUpdate, Position,
    PublicTrade, TopOfBook,
};
pub use order_state::{OrderEvent, OrderState, OrderTracker, ReconcileOutcome, StateError};
pub use traits::{
    AccountStream, BookHandle, BookOptions, BookReader, ExecutionVenue, MarketVenue, OrderAck,
    PrivateVenue, TradeStream,
};
pub use types::{
    ClientIdGen, ClientOrderId, NewOrder, OrderKind, OrderRef, Side, Symbol, SymbolMeta, Tif,
    VenueId,
};

pub use rust_decimal::Decimal;

/// 统一 Result 别名。
pub type Result<T> = std::result::Result<T, NexusError>;
