//! nexus-lighter：Lighter venue adapter。
//!
//! 铁律（architecture.md §10 M1）：只包装 `bybot-lighter`（执行/私有流）与
//! `bybot-market-engine`（行情），不重构其内部逻辑。

pub mod execution;
pub mod market;

pub use execution::{LighterVenue, LighterVenueConfig};
pub use market::{LighterMarket, LighterMarketConfig};

pub(crate) use nexus_core::now_ms;
