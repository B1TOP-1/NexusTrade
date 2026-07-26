//! 统一错误分类。
//!
//! 原则 P9（fail-closed）：`Unknown` 类错误必须走对账路径，SDK 绝不静默猜测。

use std::time::Duration;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum NexusError {
    /// 网络/传输层错误，可安全重连（不可盲目重发订单）。
    #[error("transport: {0}")]
    Transport(String),

    /// 签名/鉴权错误，不可重试，直接暴露。
    #[error("auth: {0}")]
    Auth(String),

    /// 触发限流。`retry_after` 来自交易所响应头（如有）。
    #[error("rate limited (retry_after={retry_after:?})")]
    RateLimited { retry_after: Option<Duration> },

    /// 交易所明确拒绝（确定性失败，无敞口）。
    #[error("venue reject [{code}]: {msg}")]
    VenueReject { code: String, msg: String },

    /// 本地校验拒绝（精度/最小名义额/参数非法），未出网。
    #[error("invalid order: {0}")]
    InvalidOrder(String),

    /// 数据陈旧，fail-closed 拒绝服务。
    #[error("stale data")]
    Stale,

    /// Kill switch 已触发，拒绝新单。
    #[error("kill switch tripped")]
    KillSwitch,

    /// 该交易所不支持请求的能力（查 VenueCapabilities）。
    #[error("capability not supported: {0}")]
    Unsupported(String),

    /// 歧义结果：订单状态未知，必须走对账路径。
    #[error("unknown outcome: {0}")]
    Unknown(String),
}
