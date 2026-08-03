//! nexus-book：NexusTrade L1 本地订单簿引擎。
//!
//! 行为规范见 docs/architecture.md §6：
//! - 首帧完整快照 + 增量 diff；序列号/nonce 连续性校验，断裂即清簿重建。
//! - 每次更新打本地时间戳（local-E 口径）；`staleness()` 随时可查。
//! - 读侧 `arc-swap` 快照，策略读无锁、无等待。
//! - 双轨模式（P7）：同 symbol 两条 WS，按序列号合并去重，任一条断流零感知切换。
//!
//! 读 API：`top()` / `vwap(notional)` / `depth(n)` / `staleness()` / `seq()`。

mod book;
mod dual;

pub use book::{BookEngine, Level, Side};
pub use dual::DualFeedMerger;
