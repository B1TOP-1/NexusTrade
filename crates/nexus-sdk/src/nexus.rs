//! Nexus 门面：venue 注册表 + 内置 kill switch / 延迟仪表。
//!
//! M3 边界：`exec()` 直通返回 venue 句柄，kill switch 由策略侧经 `kill_switch().guard()`
//! 自查；下单入口自动 guard 的包装层后置（见 docs/architecture.md §8 P4）。

use std::collections::HashMap;
use std::sync::Arc;

use nexus_core::{
    BookHandle, BookOptions, ExecutionVenue, MarketVenue, NexusError, PrivateVenue, Result,
    Symbol, VenueId,
};
use nexus_net::LatencyRecorder;
use nexus_risk::KillSwitch;

/// Nexus 构造器。按 venue 注册三大 trait 实现，`build()` 收口。
#[derive(Default)]
pub struct NexusBuilder {
    markets: HashMap<VenueId, Arc<dyn MarketVenue>>,
    executions: HashMap<VenueId, Arc<dyn ExecutionVenue>>,
    privates: HashMap<VenueId, Arc<dyn PrivateVenue>>,
}

impl NexusBuilder {
    /// 注册行情 venue。同一 VenueId 重复注册以后者为准。
    pub fn market(mut self, venue: VenueId, market: Arc<dyn MarketVenue>) -> Self {
        self.markets.insert(venue, market);
        self
    }

    /// 注册执行 venue。build 时自动纳入 kill switch 撤单清单。
    pub fn execution(mut self, venue: VenueId, execution: Arc<dyn ExecutionVenue>) -> Self {
        self.executions.insert(venue, execution);
        self
    }

    /// 注册私有状态 venue。
    pub fn private(mut self, venue: VenueId, private: Arc<dyn PrivateVenue>) -> Self {
        self.privates.insert(venue, private);
        self
    }

    /// 收口：创建内置 kill switch 并注册全部执行 venue。
    pub fn build(self) -> Nexus {
        let kill_switch = KillSwitch::new();
        for execution in self.executions.values() {
            kill_switch.register(Arc::clone(execution));
        }
        Nexus {
            markets: self.markets,
            executions: self.executions,
            privates: self.privates,
            kill_switch,
            metrics: LatencyRecorder::new(),
        }
    }
}

/// 策略侧的全平台入口。
pub struct Nexus {
    markets: HashMap<VenueId, Arc<dyn MarketVenue>>,
    executions: HashMap<VenueId, Arc<dyn ExecutionVenue>>,
    privates: HashMap<VenueId, Arc<dyn PrivateVenue>>,
    kill_switch: KillSwitch,
    metrics: LatencyRecorder,
}

impl Nexus {
    pub fn builder() -> NexusBuilder {
        NexusBuilder::default()
    }

    /// 订阅并维护本地订单簿，返回只读句柄（转发 `MarketVenue::subscribe_book`）。
    pub async fn book(
        &self,
        venue: VenueId,
        symbol: &Symbol,
        opts: BookOptions,
    ) -> Result<BookHandle> {
        self.market(venue)?.subscribe_book(symbol, opts).await
    }

    /// 行情 venue 句柄。
    pub fn market(&self, venue: VenueId) -> Result<Arc<dyn MarketVenue>> {
        self.markets
            .get(&venue)
            .cloned()
            .ok_or_else(|| not_registered("market", venue))
    }

    /// 执行 venue 句柄（M3 直通；下单前请自行 `kill_switch().guard()`）。
    pub fn exec(&self, venue: VenueId) -> Result<Arc<dyn ExecutionVenue>> {
        self.executions
            .get(&venue)
            .cloned()
            .ok_or_else(|| not_registered("execution", venue))
    }

    /// 私有状态 venue 句柄。
    pub fn private(&self, venue: VenueId) -> Result<Arc<dyn PrivateVenue>> {
        self.privates
            .get(&venue)
            .cloned()
            .ok_or_else(|| not_registered("private", venue))
    }

    /// 内置一键闸：已注册全部执行 venue（P4）。
    pub fn kill_switch(&self) -> &KillSwitch {
        &self.kill_switch
    }

    /// 内置延迟仪表（P6）。
    pub fn metrics(&self) -> &LatencyRecorder {
        &self.metrics
    }
}

fn not_registered(kind: &str, venue: VenueId) -> NexusError {
    NexusError::Unsupported(format!("{kind} venue not registered: {venue}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use nexus_core::{
        BookReader, BookView, Decimal, NewOrder, OrderAck, OrderRef, SymbolMeta, TopOfBook,
        TradeStream, VenueCapabilities,
    };

    struct StubBook;

    impl BookReader for StubBook {
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
                seq: 7,
                local_recv_ms: 0,
            gateway_ts_ms: 0,
            venue_ts_ms: 0,
            }
        }
        fn staleness(&self) -> Duration {
            Duration::ZERO
        }
        fn seq(&self) -> u64 {
            7
        }
    }

    struct StubMarket;

    #[async_trait]
    impl MarketVenue for StubMarket {
        fn venue(&self) -> VenueId {
            VenueId::HYPE
        }
        async fn subscribe_book(&self, _symbol: &Symbol, _opts: BookOptions) -> Result<BookHandle> {
            Ok(Arc::new(StubBook))
        }
        async fn subscribe_trades(&self, _symbol: &Symbol) -> Result<TradeStream> {
            Err(NexusError::Unsupported("stub".into()))
        }
        fn symbol_meta(&self, _symbol: &Symbol) -> Result<SymbolMeta> {
            Err(NexusError::Unsupported("stub".into()))
        }
    }

    struct StubExec {
        cancel_all_calls: AtomicUsize,
    }

    #[async_trait]
    impl ExecutionVenue for StubExec {
        fn venue(&self) -> VenueId {
            VenueId::HYPE
        }
        fn capabilities(&self) -> VenueCapabilities {
            VenueCapabilities::default()
        }
        fn is_ready(&self) -> bool {
            true
        }
        async fn place(&self, _order: NewOrder) -> Result<OrderAck> {
            Err(NexusError::Unsupported("stub".into()))
        }
        async fn place_batch(&self, _orders: Vec<NewOrder>) -> Result<Vec<Result<OrderAck>>> {
            Err(NexusError::Unsupported("stub".into()))
        }
        async fn cancel(&self, _order: &OrderRef) -> Result<()> {
            Err(NexusError::Unsupported("stub".into()))
        }
        async fn cancel_batch(&self, _orders: &[OrderRef]) -> Result<Vec<Result<()>>> {
            Err(NexusError::Unsupported("stub".into()))
        }
        async fn cancel_all(&self, _symbol: Option<&Symbol>) -> Result<()> {
            self.cancel_all_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn sym() -> Symbol {
        Symbol::new("BTC", "USD", "BTC")
    }

    #[tokio::test]
    async fn builder_registers_and_book_forwards() {
        let nexus = Nexus::builder()
            .market(VenueId::HYPE, Arc::new(StubMarket))
            .build();

        assert!(nexus.market(VenueId::HYPE).is_ok());
        let book = nexus
            .book(VenueId::HYPE, &sym(), BookOptions::default())
            .await
            .expect("registered market forwards subscribe_book");
        assert_eq!(book.seq(), 7);
    }

    #[tokio::test]
    async fn unregistered_venue_is_rejected() {
        let nexus = Nexus::builder().build();

        assert!(matches!(
            nexus.book(VenueId::HYPE, &sym(), BookOptions::default()).await,
            Err(NexusError::Unsupported(_))
        ));
        assert!(matches!(
            nexus.exec(VenueId::LIGHTER),
            Err(NexusError::Unsupported(_))
        ));
        assert!(matches!(
            nexus.private(VenueId::OKX),
            Err(NexusError::Unsupported(_))
        ));
    }

    #[tokio::test]
    async fn kill_switch_covers_registered_executions() {
        let exec = Arc::new(StubExec {
            cancel_all_calls: AtomicUsize::new(0),
        });
        let nexus = Nexus::builder()
            .execution(VenueId::HYPE, exec.clone())
            .build();

        assert!(nexus.kill_switch().guard().is_ok());
        let failures = nexus.kill_switch().trip().await;
        assert!(failures.is_empty());
        assert_eq!(exec.cancel_all_calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            nexus.kill_switch().guard(),
            Err(NexusError::KillSwitch)
        ));
        assert!(nexus.exec(VenueId::HYPE).is_ok(), "M3 exec 直通不拦截");

        nexus.kill_switch().reset();
        assert!(nexus.kill_switch().guard().is_ok());
    }

    #[test]
    fn metrics_is_wired() {
        let nexus = Nexus::builder().build();
        nexus.metrics().record("ws.book", 120);
        assert!(nexus.metrics().percentiles("ws.book").is_some());
    }
}
