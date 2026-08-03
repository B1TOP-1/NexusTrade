//! 双轨合并器（P7）：同 symbol 两条 WS，按序列号合并去重。
//!
//! 任一条断流不影响另一条；序列号冲突时优先采纳序号更新者。
//! 两条流共享同一输出 `BookEngine`，引擎端无感是单轨还是双轨。

use std::sync::Arc;

use nexus_core::{now_ms, NexusError, Result};

use crate::book::{remove_level, upsert_level, BookEngine, Level, Side};

/// 双轨合并状态。
pub struct DualFeedMerger {
    engine: Arc<BookEngine>,
    tracks: [TrackState; 2],
}

struct TrackState {
    last_seen_seq: u64,
}

impl DualFeedMerger {
    /// 创建合并器，共享底层 BookEngine。
    pub fn new(engine: Arc<BookEngine>) -> Self {
        Self {
            engine,
            tracks: [
                TrackState { last_seen_seq: 0 },
                TrackState { last_seen_seq: 0 },
            ],
        }
    }

    /// 应用全量快照，重置双轨序列号到快照起点。
    pub fn apply_snapshot(&self, bids: Vec<Level>, asks: Vec<Level>) {
        self.engine.apply_snapshot(bids, asks);
    }

    /// 从指定 track 应用增量（0 或 1）。
    ///
    /// - 同轨 seq 必须严格递增；重复或回跳静默丢弃。
    /// - 异轨 seq 间不做连续性要求。
    /// - 写入 engine 跳过序列断裂检查（双轨由 merge 保证 freshness）。
    pub fn apply_delta(
        &mut self,
        track: usize,
        side: Side,
        entries: Vec<Level>,
        seq: u64,
    ) -> Result<()> {
        assert!(track <= 1, "dual feed track must be 0 or 1");
        let t = &mut self.tracks[track];

        if seq <= t.last_seen_seq {
            return Ok(());
        }
        t.last_seen_seq = seq;

        let old = self.engine.inner.load_full();
        let mut inner = (*old).clone();
        if !inner.ready {
            return Err(NexusError::Stale);
        }
        for entry in entries {
            if entry.qty.is_zero() {
                remove_level(
                    if side.is_bid() {
                        &mut inner.bids
                    } else {
                        &mut inner.asks
                    },
                    entry.price,
                );
            } else {
                upsert_level(
                    if side.is_bid() {
                        &mut inner.bids
                    } else {
                        &mut inner.asks
                    },
                    entry.price,
                    entry.qty,
                    side.is_bid(),
                );
            }
        }
        if seq > inner.seq {
            inner.seq = seq;
        }
        inner.last_update_ms = now_ms();
        self.engine.inner.store(Arc::new(inner));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_core::BookReader;
    use rust_decimal_macros::dec;

    fn snapshot() -> (Vec<Level>, Vec<Level>) {
        let bids = vec![Level {
            price: dec!(100),
            qty: dec!(1.0),
        }];
        let asks = vec![Level {
            price: dec!(101),
            qty: dec!(1.0),
        }];
        (bids, asks)
    }

    #[test]
    fn dual_feed_merges_without_sequence_conflict() {
        let engine = Arc::new(BookEngine::new());
        let (bids, asks) = snapshot();
        let mut merger = DualFeedMerger::new(engine.clone());
        merger.apply_snapshot(bids, asks);

        merger
            .apply_delta(
                0,
                Side::Bid,
                vec![Level {
                    price: dec!(99),
                    qty: dec!(1.0),
                }],
                5,
            )
            .expect("track0 delta ok");
        assert_eq!(engine.handle().top().unwrap().bid, dec!(100));

        merger
            .apply_delta(
                1,
                Side::Ask,
                vec![Level {
                    price: dec!(100.5),
                    qty: dec!(2.0),
                }],
                3,
            )
            .expect("track1 delta ok");
        let top = engine.handle().top().unwrap();
        assert_eq!(top.ask, dec!(100.5));
        assert_eq!(engine.handle().seq(), 5);
    }

    #[test]
    fn dual_feed_duplicate_seq_is_ignored() {
        let engine = Arc::new(BookEngine::new());
        let (bids, asks) = snapshot();
        let mut merger = DualFeedMerger::new(engine.clone());
        merger.apply_snapshot(bids, asks);

        merger
            .apply_delta(
                0,
                Side::Bid,
                vec![Level {
                    price: dec!(99),
                    qty: dec!(1.0),
                }],
                2,
            )
            .expect("first ok");
        merger
            .apply_delta(
                0,
                Side::Bid,
                vec![Level {
                    price: dec!(98),
                    qty: dec!(1.0),
                }],
                2,
            )
            .expect("duplicate ignored without error");
        assert_eq!(engine.handle().seq(), 2);
    }

    #[test]
    fn dual_feed_rejects_before_snapshot() {
        let engine = Arc::new(BookEngine::new());
        let mut merger = DualFeedMerger::new(engine.clone());
        let err = merger
            .apply_delta(
                0,
                Side::Bid,
                vec![Level {
                    price: dec!(1),
                    qty: dec!(1),
                }],
                1,
            )
            .expect_err("delta before snapshot");
        assert!(matches!(err, nexus_core::NexusError::Stale));
    }
}
