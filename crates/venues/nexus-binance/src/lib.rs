//! nexus-binance：Binance Futures (USDⓈ-M) venue adapter。
//!
//! M4 首个从零构建的 adapter（无 vendor crate）。自主实现 WS 客户端、
//! depth 增量流合并、HMAC-SHA256 签名、listenKey 生命周期管理。
//!
//! 裁切范围（明确不做）：
//! - Spot / COIN-M 期货（仅 USDⓈ-M）
//! - Combined streams 合流（每 symbol 独立 WS 连接）
//! - SBE 行情（仅 JSON）

pub mod auth;
pub mod execution;
pub mod market;
pub mod types;
pub mod ws;
pub mod ws_exec;

pub use execution::{BinanceVenue, BinanceVenueConfig};
pub use market::{BinanceMarket, BinanceMarketConfig};
pub use ws_exec::WsFapiClient;
pub use ws::{spawn_reader_with_hook, WireHook};

pub use types::DepthStreamData;
