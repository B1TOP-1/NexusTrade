//! nexus-risk：NexusTrade L1 风控原语。
//!
//! 行为规范见 docs/architecture.md §8：
//! - `StalenessWatchdog`：监控所有已订阅簿，超阈值发 `RiskEvent::Stale`，可配自动撤单（P3）。
//! - `KillSwitch`：与策略解耦的一键闸，全 venue 并发撤单 + 禁新单（P4）。
//!
//! Kill switch 是 SDK 内唯一允许"未经策略同意就动订单"的模块。

mod kill_switch;
mod watchdog;

use std::sync::Arc;

use nexus_core::{ExecutionVenue, NexusError, VenueId};
use tokio::task::JoinSet;

pub use kill_switch::KillSwitch;
pub use watchdog::{StalenessWatchdog, WatchdogConfig, WatchdogHandle};

/// 风控事件。通过 `tokio::sync::broadcast` 分发，策略侧可多路订阅。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RiskEvent {
    /// `label` 对应的订单簿 staleness 超阈值。宁可不报价，绝不用残影定价（P3）。
    Stale { label: String },
    /// `label` 对应的订单簿恢复新鲜。
    Recovered { label: String },
    /// 自动 cancel_all 在某 venue 上失败（P9：暴露，不静默）。
    AutoCancelFailed { venue: String, error: String },
}

/// 对一组 venue 并发执行 `cancel_all(None)`，收集失败项。
///
/// 失败不中断其他 venue 的撤单（fail-closed：尽力撤，逐项报错）。
pub(crate) async fn cancel_all_venues(
    venues: &[Arc<dyn ExecutionVenue>],
) -> Vec<(VenueId, NexusError)> {
    let mut set = JoinSet::new();
    for venue in venues {
        let venue = Arc::clone(venue);
        set.spawn(async move {
            let id = venue.venue();
            (id, venue.cancel_all(None).await)
        });
    }
    let mut failures = Vec::new();
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((_, Ok(()))) => {}
            Ok((id, Err(e))) => failures.push((id, e)),
            Err(join_err) => failures.push((
                VenueId("UNKNOWN"),
                NexusError::Unknown(format!("cancel_all task panicked: {join_err}")),
            )),
        }
    }
    failures
}
