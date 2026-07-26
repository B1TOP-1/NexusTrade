//! nexus-hype：Hyperliquid venue adapter。
//!
//! 铁律（architecture.md §10 M1）：只包装 `bybot-hype`，不重构其内部逻辑。
//! adapter 层职责：类型转换 + trait 实现 + 生命周期管理。

pub mod execution;
pub mod market;

pub use execution::HypeVenue;
pub use market::{HypeMarket, HypeMarketConfig};
