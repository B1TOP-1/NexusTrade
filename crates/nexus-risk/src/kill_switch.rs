//! Kill switch（P4）：与策略完全解耦的一键闸。
//!
//! `trip()`：原子置位 → 全 venue 并发 `cancel_all(None)` → 后续 `guard()` 一律拒绝。
//! `reset()` 需显式调用，绝不自动恢复。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use nexus_core::{ExecutionVenue, NexusError, Result, VenueId};

use crate::cancel_all_venues;

/// 一键闸。内部为原子布尔门，`guard()` 供下单入口强制检查。
#[derive(Default)]
pub struct KillSwitch {
    tripped: AtomicBool,
    venues: RwLock<Vec<Arc<dyn ExecutionVenue>>>,
}

impl KillSwitch {
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册 trip 时参与并发撤单的执行 venue。
    pub fn register(&self, venue: Arc<dyn ExecutionVenue>) {
        self.venues
            .write()
            .expect("kill switch venue lock poisoned")
            .push(venue);
    }

    /// 是否已触发。
    pub fn is_tripped(&self) -> bool {
        self.tripped.load(Ordering::SeqCst)
    }

    /// 下单前置检查：已触发返回 `NexusError::KillSwitch`。
    pub fn guard(&self) -> Result<()> {
        if self.is_tripped() {
            Err(NexusError::KillSwitch)
        } else {
            Ok(())
        }
    }

    /// 触发：先置位（立即禁新单），再对全部已注册 venue 并发 `cancel_all(None)`。
    /// 返回撤单失败项（P9：暴露给上层，不静默）。空表示全部撤单成功。
    pub async fn trip(&self) -> Vec<(VenueId, NexusError)> {
        self.tripped.store(true, Ordering::SeqCst);
        let venues = self
            .venues
            .read()
            .expect("kill switch venue lock poisoned")
            .clone();
        cancel_all_venues(&venues).await
    }

    /// 显式复位。仅解除禁单，不做任何补单动作。
    pub fn reset(&self) {
        self.tripped.store(false, Ordering::SeqCst);
    }
}
