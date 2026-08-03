//! nexus-net：NexusTrade L1 连接基建。
//!
//! M2 落地范围（docs/architecture.md §7）：
//! - `WeightedTokenBucket`：权重感知令牌桶（P5），触发限流返回 `RateLimited` 而不排队。
//! - `LatencyRecorder`：HDR 直方图延迟仪表（P6），按 label 分流，p95/p99/p99.9 开箱可读。
//!
//! WS 管理/重连/心跳当前由 vendor 层已实盘验证实现承担（M1 铁律），待新所接入时抽取。

mod latency;
mod rate_limit;

pub use latency::{LatencyRecorder, Percentiles};
pub use rate_limit::WeightedTokenBucket;
