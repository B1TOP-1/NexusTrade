//! 本地订单簿引擎：快照+增量，序列校验，arc-swap 无锁读，VWAP。
//!
//! 写侧单任务独占，读侧 `arc-swap` 快照——策略读取零锁竞争。

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use nexus_core::{now_ms, BookReader, BookView, Decimal, NexusError, Result, TopOfBook};

/// 单档价格-数量对。bids 降序，asks 升序。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level {
    pub price: Decimal,
    pub qty: Decimal,
}

/// 方向（内部使用）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}

impl Side {
    pub(crate) fn is_bid(&self) -> bool {
        matches!(self, Side::Bid)
    }
}

/// 订单簿内部状态。价格以 `Decimal` 存储，精确无浮点。
#[derive(Clone)]
pub(crate) struct BookInner {
    pub(crate) bids: Vec<Level>,
    pub(crate) asks: Vec<Level>,
    pub(crate) seq: u64,
    pub(crate) last_update_ms: i64,
    pub(crate) ready: bool,
}

impl BookInner {
    fn new() -> Self {
        Self {
            bids: Vec::new(),
            asks: Vec::new(),
            seq: 0,
            last_update_ms: 0,
            ready: false,
        }
    }

    pub(crate) fn apply_snapshot(&mut self, bids: Vec<Level>, asks: Vec<Level>) {
        self.bids = dedup_and_sort(bids, false);
        self.asks = dedup_and_sort(asks, true);
        self.seq = 0;
        self.last_update_ms = now_ms();
        self.ready = true;
    }

    pub(crate) fn apply_delta(&mut self, side: Side, entries: Vec<Level>, seq: u64) -> Result<()> {
        if !self.ready {
            return Ok(());
        }
        if self.seq > 0 && seq != self.seq + 1 {
            self.ready = false;
            self.bids.clear();
            self.asks.clear();
            return Err(NexusError::Stale);
        }
        self.seq = seq;
        for entry in entries {
            if entry.qty.is_zero() {
                remove_level(
                    if side.is_bid() {
                        &mut self.bids
                    } else {
                        &mut self.asks
                    },
                    entry.price,
                );
            } else {
                upsert_level(
                    if side.is_bid() {
                        &mut self.bids
                    } else {
                        &mut self.asks
                    },
                    entry.price,
                    entry.qty,
                    side.is_bid(),
                );
            }
        }
        self.last_update_ms = now_ms();
        Ok(())
    }

    fn top(&self) -> Option<TopOfBook> {
        if !self.ready {
            return None;
        }
        let bid = self.bids.first()?;
        let ask = self.asks.first()?;
        Some(TopOfBook {
            bid: bid.price,
            bid_qty: bid.qty,
            ask: ask.price,
            ask_qty: ask.qty,
        })
    }

    fn vwap(&self, notional: Decimal) -> Option<(Decimal, Decimal)> {
        if !self.ready || notional <= Decimal::ZERO {
            return None;
        }
        let bid = side_vwap(&self.bids, notional)?;
        let ask = side_vwap(&self.asks, notional)?;
        Some((bid, ask))
    }

    fn depth(&self, levels: usize) -> BookView {
        let to_pairs =
            |side: &[Level]| side.iter().take(levels).map(|l| (l.price, l.qty)).collect();
        BookView {
            bids: to_pairs(&self.bids),
            asks: to_pairs(&self.asks),
            seq: self.seq,
            local_recv_ms: self.last_update_ms,
        }
    }

    fn staleness(&self, now: i64) -> Duration {
        if !self.ready || self.last_update_ms == 0 {
            return Duration::MAX;
        }
        Duration::from_millis((now - self.last_update_ms).max(0) as u64)
    }
}

/// 本地订单簿引擎：单写多读，`ArcSwap` 驱动的无锁快照。
pub struct BookEngine {
    pub(crate) inner: Arc<ArcSwap<BookInner>>,
}

impl BookEngine {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ArcSwap::new(Arc::new(BookInner::new()))),
        }
    }

    pub fn apply_snapshot(&self, bids: Vec<Level>, asks: Vec<Level>) {
        let old = self.inner.load_full();
        let mut inner = (*old).clone();
        inner.apply_snapshot(bids, asks);
        self.inner.store(Arc::new(inner));
    }

    pub fn apply_delta(&self, side: Side, entries: Vec<Level>, seq: u64) -> Result<()> {
        let old = self.inner.load_full();
        let mut inner = (*old).clone();
        if !inner.ready {
            return Err(NexusError::Stale);
        }
        match inner.apply_delta(side, entries, seq) {
            Ok(()) => {
                self.inner.store(Arc::new(inner));
                Ok(())
            }
            Err(e) => {
                self.inner.store(Arc::new(inner));
                Err(e)
            }
        }
    }

    pub fn handle(&self) -> BookHandle {
        BookHandle {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Default for BookEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// 订单簿只读句柄。clone 轻量（Arc 引用计数加一）。
#[derive(Clone)]
pub struct BookHandle {
    inner: Arc<ArcSwap<BookInner>>,
}

impl BookHandle {
    fn load(&self) -> Arc<BookInner> {
        self.inner.load_full()
    }
}

impl BookReader for BookHandle {
    fn top(&self) -> Option<TopOfBook> {
        self.load().top()
    }

    fn vwap(&self, notional: Decimal) -> Option<(Decimal, Decimal)> {
        self.load().vwap(notional)
    }

    fn depth(&self, levels: usize) -> BookView {
        self.load().depth(levels)
    }

    fn staleness(&self) -> Duration {
        self.load().staleness(now_ms())
    }

    fn seq(&self) -> u64 {
        self.load().seq
    }
}

// ── 内部辅助 ──

fn dedup_and_sort(levels: Vec<Level>, ascending: bool) -> Vec<Level> {
    let mut out: Vec<Level> = Vec::with_capacity(levels.len());
    for l in levels {
        if l.price <= Decimal::ZERO || l.qty <= Decimal::ZERO {
            continue;
        }
        if out.iter().any(|x| x.price == l.price) {
            continue;
        }
        out.push(l);
    }
    out.sort_unstable_by(|a, b| {
        if ascending {
            a.price.cmp(&b.price)
        } else {
            b.price.cmp(&a.price)
        }
    });
    out
}

pub(crate) fn upsert_level(side: &mut Vec<Level>, price: Decimal, qty: Decimal, is_bid: bool) {
    match side.binary_search_by(|l| {
        if is_bid {
            l.price.cmp(&price).reverse()
        } else {
            l.price.cmp(&price)
        }
    }) {
        Ok(idx) => {
            side[idx].qty = qty;
        }
        Err(idx) => {
            side.insert(idx, Level { price, qty });
        }
    }
}

pub(crate) fn remove_level(side: &mut Vec<Level>, price: Decimal) {
    side.retain(|l| l.price != price);
}

fn side_vwap(levels: &[Level], target_notional: Decimal) -> Option<Decimal> {
    let mut acc_notional = Decimal::ZERO;
    let mut acc_cost = Decimal::ZERO;
    for l in levels {
        let level_notional = l.price * l.qty;
        let remaining = target_notional - acc_notional;
        if remaining <= Decimal::ZERO {
            break;
        }
        if level_notional <= remaining {
            acc_notional += level_notional;
            acc_cost += l.price * level_notional;
        } else {
            let partial_qty = remaining / l.price;
            acc_notional += remaining;
            acc_cost += l.price * (l.price * partial_qty);
            break;
        }
    }
    if acc_notional < target_notional {
        return None;
    }
    Some((acc_cost / acc_notional).round_dp(4))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn btc_snapshot() -> (Vec<Level>, Vec<Level>) {
        let bids = vec![
            Level {
                price: dec!(65000.0),
                qty: dec!(1.5),
            },
            Level {
                price: dec!(64950.0),
                qty: dec!(2.0),
            },
        ];
        let asks = vec![
            Level {
                price: dec!(65010.0),
                qty: dec!(1.0),
            },
            Level {
                price: dec!(65020.0),
                qty: dec!(3.0),
            },
        ];
        (bids, asks)
    }

    #[test]
    fn snapshot_sets_bbo_and_ready() {
        let engine = BookEngine::new();
        let (bids, asks) = btc_snapshot();
        engine.apply_snapshot(bids, asks);
        let h = engine.handle();

        let top = h.top().expect("ready book has BBO");
        assert_eq!(top.bid, dec!(65000.0));
        assert_eq!(top.ask, dec!(65010.0));
        assert!(h.staleness() < Duration::from_millis(100));
    }

    #[test]
    fn empty_book_returns_none_top() {
        let h = BookEngine::new().handle();
        assert!(h.top().is_none());
    }

    #[test]
    fn vwap_exact_notional() {
        let engine = BookEngine::new();
        let bids = vec![
            Level {
                price: dec!(65000),
                qty: dec!(1.0),
            },
            Level {
                price: dec!(64900),
                qty: dec!(1.0),
            },
        ];
        let asks = vec![Level {
            price: dec!(65100),
            qty: dec!(2.0),
        }];
        engine.apply_snapshot(bids, asks);
        let h = engine.handle();

        let (bid_vwap, ask_vwap) = h.vwap(dec!(65000)).expect("depth sufficient");
        assert_eq!(bid_vwap, dec!(65000));
        assert_eq!(ask_vwap, dec!(65100));
    }

    #[test]
    fn vwap_partial_level() {
        let engine = BookEngine::new();
        let bids = vec![Level {
            price: dec!(100.0),
            qty: dec!(10.0),
        }];
        let asks = vec![Level {
            price: dec!(101.0),
            qty: dec!(10.0),
        }];
        engine.apply_snapshot(bids, asks);
        let h = engine.handle();

        let (bid_vwap, _) = h.vwap(dec!(500)).expect("depth sufficient");
        assert_eq!(bid_vwap, dec!(100));
    }

    #[test]
    fn vwap_insufficient_depth_returns_none() {
        let engine = BookEngine::new();
        let bids = vec![Level {
            price: dec!(100),
            qty: dec!(0.01),
        }];
        let asks = vec![Level {
            price: dec!(101),
            qty: dec!(0.01),
        }];
        engine.apply_snapshot(bids, asks);
        let h = engine.handle();
        assert!(h.vwap(dec!(10000)).is_none());
    }

    #[test]
    fn delta_updates_and_sequence_validation() {
        let engine = BookEngine::new();
        let (bids, asks) = btc_snapshot();
        engine.apply_snapshot(bids, asks.clone());

        engine
            .apply_delta(
                Side::Bid,
                vec![
                    Level {
                        price: dec!(65010.0),
                        qty: dec!(0.5),
                    },
                    Level {
                        price: dec!(64950.0),
                        qty: dec!(0.0),
                    },
                ],
                1,
            )
            .expect("sequential delta ok");

        let h = engine.handle();
        let top = h.top().unwrap();
        assert_eq!(top.bid, dec!(65010.0));
        assert_eq!(top.bid_qty, dec!(0.5));
        assert_eq!(h.seq(), 1);

        engine
            .apply_delta(
                Side::Ask,
                vec![Level {
                    price: dec!(65010.0),
                    qty: dec!(2.0),
                }],
                2,
            )
            .expect("sequential delta ok");
        let top = engine.handle().top().unwrap();
        assert_eq!(top.ask, dec!(65010.0));
        assert_eq!(top.ask_qty, dec!(2.0));
    }

    #[test]
    fn non_sequential_delta_clears_book_and_returns_stale() {
        let engine = BookEngine::new();
        let (bids, asks) = btc_snapshot();
        engine.apply_snapshot(bids, asks);
        engine
            .apply_delta(Side::Bid, vec![], 1)
            .expect("first delta ok");
        assert_eq!(engine.handle().seq(), 1);

        let err = engine
            .apply_delta(Side::Bid, vec![], 5)
            .expect_err("gap should return Stale");
        assert!(matches!(err, NexusError::Stale));
        assert!(engine.handle().top().is_none(), "book cleared on gap");
        assert_eq!(
            engine.handle().staleness(),
            Duration::MAX,
            "stale since book is unready"
        );
    }

    #[test]
    fn delta_before_snapshot_is_rejected() {
        let engine = BookEngine::new();
        let err = engine
            .apply_delta(
                Side::Bid,
                vec![Level {
                    price: dec!(1),
                    qty: dec!(1),
                }],
                1,
            )
            .expect_err("delta before snapshot rejected");
        assert!(matches!(err, NexusError::Stale));
        assert!(engine.handle().top().is_none());
    }

    #[test]
    fn dedup_keeps_first_occurrence() {
        let engine = BookEngine::new();
        let bids = vec![
            Level {
                price: dec!(100),
                qty: dec!(1.0),
            },
            Level {
                price: dec!(100),
                qty: dec!(2.0),
            },
        ];
        let asks = vec![Level {
            price: dec!(200),
            qty: dec!(1.0),
        }];
        engine.apply_snapshot(bids, asks);
        let top = engine.handle().top().unwrap();
        assert_eq!(top.bid_qty, dec!(1.0));
    }

    #[test]
    fn handle_clone_is_cheap_and_sees_same_state() {
        let engine = BookEngine::new();
        let (bids, asks) = btc_snapshot();
        engine.apply_snapshot(bids.clone(), asks.clone());

        let h1 = engine.handle();
        let h2 = h1.clone();
        assert_eq!(h1.top(), h2.top());

        engine
            .apply_delta(
                Side::Ask,
                vec![Level {
                    price: dec!(65010.0),
                    qty: Decimal::ZERO,
                }],
                1,
            )
            .expect("sequential");
        assert_eq!(h1.top().unwrap().ask, dec!(65020.0));
        assert_eq!(h1.top(), h2.top());
    }

    #[test]
    fn snapshot_sorts_bids_desc_asks_asc() {
        let engine = BookEngine::new();
        let bids = vec![
            Level {
                price: dec!(100),
                qty: dec!(1.0),
            },
            Level {
                price: dec!(200),
                qty: dec!(1.0),
            },
        ];
        let asks = vec![
            Level {
                price: dec!(300),
                qty: dec!(1.0),
            },
            Level {
                price: dec!(250),
                qty: dec!(1.0),
            },
        ];
        engine.apply_snapshot(bids, asks);
        let view = engine.handle().depth(5);
        assert_eq!(view.bids[0].0, dec!(200));
        assert_eq!(view.bids[1].0, dec!(100));
        assert_eq!(view.asks[0].0, dec!(250));
        assert_eq!(view.asks[1].0, dec!(300));
    }
}
