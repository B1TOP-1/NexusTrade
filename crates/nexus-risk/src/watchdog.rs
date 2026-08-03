//! Staleness watchdog（P3）：每条行情流带本地时间戳，超阈值熔断。
//!
//! 注册若干 (label, BookHandle)，后台任务周期检查 `BookReader::staleness()`：
//! - 超阈值：先（可选）并发全 venue `cancel_all(None)`，再广播 `RiskEvent::Stale`；
//!   事件送达即代表自动撤单动作已完成（便于策略侧同步推理）。
//! - 恢复：广播 `RiskEvent::Recovered`。
//! 每个 label 只在状态翻转沿发一次事件，不重复轰炸。

use std::sync::Arc;
use std::time::Duration;

use nexus_core::{BookHandle, ExecutionVenue};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::{cancel_all_venues, RiskEvent};

/// watchdog 配置。
#[derive(Debug, Clone)]
pub struct WatchdogConfig {
    /// staleness 熔断阈值。默认 500ms（architecture.md P3）。
    pub threshold: Duration,
    /// 后台检查周期。默认 100ms。
    pub check_interval: Duration,
    /// 触发熔断时是否对所有已注册 venue 并发 `cancel_all(None)`。默认 false。
    pub auto_cancel_all: bool,
}

impl Default for WatchdogConfig {
    fn default() -> Self {
        Self {
            threshold: Duration::from_millis(500),
            check_interval: Duration::from_millis(100),
            auto_cancel_all: false,
        }
    }
}

/// Staleness watchdog。注册完成后 `spawn()` 启动后台监控任务。
pub struct StalenessWatchdog {
    config: WatchdogConfig,
    books: Vec<(String, BookHandle)>,
    venues: Vec<Arc<dyn ExecutionVenue>>,
    events: broadcast::Sender<RiskEvent>,
}

impl StalenessWatchdog {
    pub fn new(config: WatchdogConfig) -> Self {
        let (events, _) = broadcast::channel(64);
        Self {
            config,
            books: Vec::new(),
            venues: Vec::new(),
            events,
        }
    }

    /// 注册一条待监控的订单簿。`label` 用于事件标识（如 "HYPE/BTC"）。
    pub fn register_book(&mut self, label: impl Into<String>, book: BookHandle) {
        self.books.push((label.into(), book));
    }

    /// 注册熔断时参与 `cancel_all` 的执行 venue（仅 `auto_cancel_all = true` 时生效）。
    pub fn register_execution(&mut self, venue: Arc<dyn ExecutionVenue>) {
        self.venues.push(venue);
    }

    /// 订阅风控事件流。可在 `spawn()` 前后任意时刻调用（句柄侧同样可订阅）。
    pub fn subscribe(&self) -> broadcast::Receiver<RiskEvent> {
        self.events.subscribe()
    }

    /// 启动后台监控任务。返回句柄；句柄 Drop 即停止监控。
    pub fn spawn(self) -> WatchdogHandle {
        let events = self.events.clone();
        let task = tokio::spawn(run(self));
        WatchdogHandle { events, task }
    }
}

/// watchdog 后台任务句柄。Drop 即 abort 监控任务。
pub struct WatchdogHandle {
    events: broadcast::Sender<RiskEvent>,
    task: JoinHandle<()>,
}

impl WatchdogHandle {
    pub fn subscribe(&self) -> broadcast::Receiver<RiskEvent> {
        self.events.subscribe()
    }
}

impl Drop for WatchdogHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// 监控主循环。
async fn run(watchdog: StalenessWatchdog) {
    let StalenessWatchdog {
        config,
        books,
        venues,
        events,
    } = watchdog;

    let mut stale_flags = vec![false; books.len()];
    let mut interval = tokio::time::interval(config.check_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;
        for (i, (label, book)) in books.iter().enumerate() {
            let is_stale = book.staleness() > config.threshold;
            if is_stale && !stale_flags[i] {
                stale_flags[i] = true;
                if config.auto_cancel_all {
                    for (venue, error) in cancel_all_venues(&venues).await {
                        let _ = events.send(RiskEvent::AutoCancelFailed {
                            venue: venue.as_str().to_string(),
                            error: error.to_string(),
                        });
                    }
                }
                let _ = events.send(RiskEvent::Stale {
                    label: label.clone(),
                });
            } else if !is_stale && stale_flags[i] {
                stale_flags[i] = false;
                let _ = events.send(RiskEvent::Recovered {
                    label: label.clone(),
                });
            }
        }
    }
}
