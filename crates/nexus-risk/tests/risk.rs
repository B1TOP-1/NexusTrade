//! nexus-risk 集成测试：mock BookReader（可控 staleness）+ mock ExecutionVenue（记录撤单次数）。

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nexus_core::{
    BookReader, BookView, Decimal, ExecutionVenue, NewOrder, NexusError, OrderAck, OrderRef,
    Result, Symbol, TopOfBook, VenueCapabilities, VenueId,
};
use nexus_risk::{KillSwitch, RiskEvent, StalenessWatchdog, WatchdogConfig};
use tokio::time::timeout;

/// staleness 可外部控制的 mock 订单簿。
struct MockBook {
    staleness_ms: AtomicU64,
}

impl MockBook {
    fn new(initial_ms: u64) -> Arc<Self> {
        Arc::new(Self {
            staleness_ms: AtomicU64::new(initial_ms),
        })
    }

    fn set_staleness_ms(&self, ms: u64) {
        self.staleness_ms.store(ms, Ordering::SeqCst);
    }
}

impl BookReader for MockBook {
    fn top(&self) -> Option<TopOfBook> {
        None
    }

    fn vwap(&self, _notional: Decimal) -> Option<(Decimal, Decimal)> {
        None
    }

    fn depth(&self, _levels: usize) -> BookView {
        BookView {
            bids: Vec::new(),
            asks: Vec::new(),
            seq: 0,
            local_recv_ms: 0,
            gateway_ts_ms: 0,
            venue_ts_ms: 0,
        }
    }

    fn staleness(&self) -> Duration {
        Duration::from_millis(self.staleness_ms.load(Ordering::SeqCst))
    }

    fn seq(&self) -> u64 {
        0
    }
}

/// 记录 cancel_all 调用次数的 mock 执行 venue。
struct MockExec {
    id: VenueId,
    cancel_all_calls: AtomicUsize,
    fail_cancel: bool,
}

impl MockExec {
    fn new(id: VenueId) -> Arc<Self> {
        Arc::new(Self {
            id,
            cancel_all_calls: AtomicUsize::new(0),
            fail_cancel: false,
        })
    }

    fn failing(id: VenueId) -> Arc<Self> {
        Arc::new(Self {
            id,
            cancel_all_calls: AtomicUsize::new(0),
            fail_cancel: true,
        })
    }

    fn cancel_all_count(&self) -> usize {
        self.cancel_all_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ExecutionVenue for MockExec {
    fn venue(&self) -> VenueId {
        self.id
    }

    fn capabilities(&self) -> VenueCapabilities {
        VenueCapabilities::default()
    }

    fn is_ready(&self) -> bool {
        true
    }

    async fn place(&self, _order: NewOrder) -> Result<OrderAck> {
        Err(NexusError::Unsupported("mock".into()))
    }

    async fn place_batch(&self, _orders: Vec<NewOrder>) -> Result<Vec<Result<OrderAck>>> {
        Err(NexusError::Unsupported("mock".into()))
    }

    async fn cancel(&self, _order: &OrderRef) -> Result<()> {
        Err(NexusError::Unsupported("mock".into()))
    }

    async fn cancel_batch(&self, _orders: &[OrderRef]) -> Result<Vec<Result<()>>> {
        Err(NexusError::Unsupported("mock".into()))
    }

    async fn cancel_all(&self, _symbol: Option<&Symbol>) -> Result<()> {
        self.cancel_all_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_cancel {
            Err(NexusError::Transport("mock cancel_all down".into()))
        } else {
            Ok(())
        }
    }
}

fn fast_config(auto_cancel_all: bool) -> WatchdogConfig {
    WatchdogConfig {
        threshold: Duration::from_millis(50),
        check_interval: Duration::from_millis(10),
        auto_cancel_all,
    }
}

async fn expect_event(rx: &mut tokio::sync::broadcast::Receiver<RiskEvent>) -> RiskEvent {
    timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timed out waiting for risk event")
        .expect("risk event channel closed")
}

#[tokio::test]
async fn watchdog_emits_stale_and_cancels_then_recovers() {
    let book = MockBook::new(0);
    let exec_a = MockExec::new(VenueId::HYPE);
    let exec_b = MockExec::new(VenueId::LIGHTER);

    let mut dog = StalenessWatchdog::new(fast_config(true));
    dog.register_book("HYPE/BTC", book.clone());
    dog.register_execution(exec_a.clone());
    dog.register_execution(exec_b.clone());
    let mut rx = dog.subscribe();
    let _handle = dog.spawn();

    // 超阈值 → Stale 事件 + 两个 venue 各撤单一次（事件送达即撤单完成）。
    book.set_staleness_ms(10_000);
    assert_eq!(
        expect_event(&mut rx).await,
        RiskEvent::Stale {
            label: "HYPE/BTC".into()
        }
    );
    assert_eq!(exec_a.cancel_all_count(), 1);
    assert_eq!(exec_b.cancel_all_count(), 1);

    // 恢复 → Recovered 事件，且不重复撤单。
    book.set_staleness_ms(0);
    assert_eq!(
        expect_event(&mut rx).await,
        RiskEvent::Recovered {
            label: "HYPE/BTC".into()
        }
    );
    assert_eq!(exec_a.cancel_all_count(), 1);
}

#[tokio::test]
async fn watchdog_without_auto_cancel_only_emits_events() {
    let book = MockBook::new(10_000);
    let exec = MockExec::new(VenueId::HYPE);

    let mut dog = StalenessWatchdog::new(fast_config(false));
    dog.register_book("HYPE/ETH", book.clone());
    dog.register_execution(exec.clone());
    let mut rx = dog.subscribe();
    let _handle = dog.spawn();

    assert_eq!(
        expect_event(&mut rx).await,
        RiskEvent::Stale {
            label: "HYPE/ETH".into()
        }
    );
    assert_eq!(exec.cancel_all_count(), 0);
}

#[tokio::test]
async fn watchdog_reports_auto_cancel_failure() {
    let book = MockBook::new(10_000);
    let exec = MockExec::failing(VenueId::LIGHTER);

    let mut dog = StalenessWatchdog::new(fast_config(true));
    dog.register_book("LIGHTER/BTC", book.clone());
    dog.register_execution(exec.clone());
    let mut rx = dog.subscribe();
    let _handle = dog.spawn();

    // 失败先报 AutoCancelFailed，再报 Stale（撤单动作先于事件）。
    match expect_event(&mut rx).await {
        RiskEvent::AutoCancelFailed { venue, error } => {
            assert_eq!(venue, "LIGHTER");
            assert!(error.contains("mock cancel_all down"));
        }
        other => panic!("expected AutoCancelFailed, got {other:?}"),
    }
    assert_eq!(
        expect_event(&mut rx).await,
        RiskEvent::Stale {
            label: "LIGHTER/BTC".into()
        }
    );
    assert_eq!(exec.cancel_all_count(), 1);
}

#[tokio::test]
async fn kill_switch_trips_cancels_and_guards() {
    let exec_a = MockExec::new(VenueId::HYPE);
    let exec_b = MockExec::new(VenueId::LIGHTER);

    let ks = KillSwitch::new();
    ks.register(exec_a.clone());
    ks.register(exec_b.clone());

    assert!(!ks.is_tripped());
    assert!(ks.guard().is_ok());

    let failures = ks.trip().await;
    assert!(failures.is_empty());
    assert!(ks.is_tripped());
    assert_eq!(exec_a.cancel_all_count(), 1);
    assert_eq!(exec_b.cancel_all_count(), 1);
    assert!(matches!(ks.guard(), Err(NexusError::KillSwitch)));

    ks.reset();
    assert!(!ks.is_tripped());
    assert!(ks.guard().is_ok());
}

#[tokio::test]
async fn kill_switch_trip_collects_failures_but_cancels_everywhere() {
    let good = MockExec::new(VenueId::HYPE);
    let bad = MockExec::failing(VenueId::LIGHTER);

    let ks = KillSwitch::new();
    ks.register(good.clone());
    ks.register(bad.clone());

    let failures = ks.trip().await;
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].0, VenueId::LIGHTER);
    // 单 venue 失败不影响其他 venue 撤单，且闸门保持触发。
    assert_eq!(good.cancel_all_count(), 1);
    assert_eq!(bad.cancel_all_count(), 1);
    assert!(ks.is_tripped());
}
