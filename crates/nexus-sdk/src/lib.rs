//! nexus-sdk：NexusTrade L2 门面 crate，策略工程的唯一依赖。
//!
//! ```toml
//! nexus-sdk = { features = ["hype", "lighter"] }
//! ```
//!
//! 策略侧只 `use nexus_sdk::*`；新增交易所零改动（docs/architecture.md §1）。

mod nexus;

pub use nexus::{Nexus, NexusBuilder};

// L0 常用类型全量 re-export。
pub use nexus_core::*;

// L1 基建。
pub use nexus_book::{BookEngine, DualFeedMerger, Level, Side as BookSide};
pub use nexus_net::{LatencyRecorder, Percentiles, WeightedTokenBucket};
pub use nexus_risk::{KillSwitch, RiskEvent, StalenessWatchdog, WatchdogConfig, WatchdogHandle};

// venue 便捷构造（feature 对应）。
#[cfg(feature = "hype")]
pub use nexus_hype::{HypeMarket, HypeMarketConfig, HypeVenue};

#[cfg(feature = "lighter")]
pub use nexus_lighter::{LighterMarket, LighterMarketConfig, LighterVenue, LighterVenueConfig};

#[cfg(feature = "binance")]
pub use nexus_binance::{BinanceMarket, BinanceMarketConfig, BinanceVenue, BinanceVenueConfig};

#[cfg(feature = "gate")]
pub use nexus_gate::{
    GateExecConfig, GateMarket, GateMarketConfig, GatePrivate, GatePrivateConfig, GateVenue,
};
