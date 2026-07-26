use crate::book::LocalBook;
use crate::model::{BookStatus, EngineConfig, SignalRow};
use crate::sliding_median::SlidingMedian;

pub struct SignalEngine {
    config: EngineConfig,
    long_history: SlidingMedian,
    short_history: SlidingMedian,
}

impl SignalEngine {
    pub fn new(config: EngineConfig) -> Self {
        Self {
            long_history: SlidingMedian::new(config.window_size),
            short_history: SlidingMedian::new(config.window_size),
            config,
        }
    }

    pub fn sample(&mut self, gate: &LocalBook, lighter: &LocalBook) -> bool {
        let Some(values) = Self::book_values(gate, lighter) else {
            return false;
        };
        self.long_history.push(values.long_spread);
        self.short_history.push(values.short_spread);
        true
    }

    pub fn evaluate(
        &self,
        sequence: u64,
        timestamp_ns: u64,
        source: &str,
        gate: &LocalBook,
        lighter: &LocalBook,
    ) -> Option<SignalRow> {
        let values = Self::book_values(gate, lighter)?;
        let sample_count = self.long_history.len().min(self.short_history.len());
        let ready = sample_count >= self.config.min_samples;
        let mid_price =
            (values.lighter_bid + values.lighter_ask + values.gate_bid + values.gate_ask) / 4.0;
        let basis = mid_price * self.config.threshold_bps / 10000.0;

        let mut long_median = 0.0;
        let mut short_median = 0.0;
        let mut long_threshold = 0.0;
        let mut short_threshold = 0.0;
        let mut long_ok = false;
        let mut short_ok = false;
        if ready {
            long_median = self.long_history.median();
            short_median = self.short_history.median();
            long_threshold = long_median - basis;
            short_threshold = short_median + basis;
            long_ok = values.long_spread <= long_threshold;
            short_ok = values.short_spread >= short_threshold;
        }

        Some(SignalRow {
            sequence,
            timestamp_ns,
            source: source.to_string(),
            ticker: self.config.ticker.clone(),
            gate_contract: self.config.gate_contract.clone(),
            lighter_market_id: self.config.lighter_market_id,
            ready,
            sample_count,
            lighter_bid: values.lighter_bid,
            lighter_bid_size: values.lighter_bid_size,
            lighter_ask: values.lighter_ask,
            lighter_ask_size: values.lighter_ask_size,
            gate_bid: values.gate_bid,
            gate_bid_size: values.gate_bid_size,
            gate_ask: values.gate_ask,
            gate_ask_size: values.gate_ask_size,
            long_spread: values.long_spread,
            short_spread: values.short_spread,
            long_median,
            short_median,
            long_threshold,
            short_threshold,
            basis,
            long_ok,
            short_ok,
            gate_book_status: gate.status(),
            lighter_book_status: lighter.status(),
            depth: None,
        })
    }

    pub fn maybe_signal(
        &mut self,
        sequence: u64,
        timestamp_ns: u64,
        source: &str,
        gate: &LocalBook,
        lighter: &LocalBook,
    ) -> Option<SignalRow> {
        if !self.sample(gate, lighter) {
            return None;
        }
        self.evaluate(sequence, timestamp_ns, source, gate, lighter)
    }

    fn book_values(gate: &LocalBook, lighter: &LocalBook) -> Option<BookValues> {
        if gate.status() != BookStatus::Ready || lighter.status() != BookStatus::Ready {
            return None;
        }

        let (gate_bid, gate_bid_size) = gate.best_bid()?;
        let (gate_ask, gate_ask_size) = gate.best_ask()?;
        let (lighter_bid, lighter_bid_size) = lighter.best_bid()?;
        let (lighter_ask, lighter_ask_size) = lighter.best_ask()?;
        let long_spread = gate_ask - lighter_bid;
        let short_spread = gate_bid - lighter_ask;

        Some(BookValues {
            gate_bid,
            gate_bid_size,
            gate_ask,
            gate_ask_size,
            lighter_bid,
            lighter_bid_size,
            lighter_ask,
            lighter_ask_size,
            long_spread,
            short_spread,
        })
    }
}

struct BookValues {
    gate_bid: f64,
    gate_bid_size: f64,
    gate_ask: f64,
    gate_ask_size: f64,
    lighter_bid: f64,
    lighter_bid_size: f64,
    lighter_ask: f64,
    lighter_ask_size: f64,
    long_spread: f64,
    short_spread: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{BookStatus, Level};

    fn config(min_samples: usize) -> EngineConfig {
        EngineConfig {
            window_size: 3,
            min_samples,
            threshold_bps: 1.5,
            ticker: "BTC".to_string(),
            gate_contract: "BTC_USDT".to_string(),
            lighter_market_id: 1,
        }
    }

    fn ready_books() -> (LocalBook, LocalBook) {
        let mut gate = LocalBook::new();
        let mut lighter = LocalBook::new();
        gate.apply_snapshot(
            &[Level {
                price: 100_00000000,
                size: 3_00000000,
            }],
            &[Level {
                price: 100_10000000,
                size: 2_50000000,
            }],
            Some(10),
        );
        lighter.apply_snapshot(
            &[Level {
                price: 99_80000000,
                size: 1_00000000,
            }],
            &[Level {
                price: 99_90000000,
                size: 1_50000000,
            }],
            Some(20),
        );
        (gate, lighter)
    }

    #[test]
    fn generates_ready_signal_after_min_samples() {
        let (gate, lighter) = ready_books();
        let mut engine = SignalEngine::new(config(2));

        let first = engine
            .maybe_signal(2, 1_000_001_000, "lighter_snapshot", &gate, &lighter)
            .unwrap();
        assert!(!first.ready);
        assert_eq!(first.sample_count, 1);
        assert_close(first.long_spread, 0.3);
        assert_close(first.short_spread, 0.1);

        let second = engine
            .maybe_signal(3, 1_000_002_000, "gate_update", &gate, &lighter)
            .unwrap();
        assert!(second.ready);
        assert_eq!(second.sample_count, 2);
        assert_close(second.long_median, 0.3);
        assert_close(second.short_median, 0.1);
        assert_eq!(second.gate_book_status, BookStatus::Ready);
        assert_eq!(second.lighter_book_status, BookStatus::Ready);
    }

    #[test]
    fn rolling_window_median_threshold_and_short_signal_match_fixture() {
        let mut gate = LocalBook::new();
        let mut lighter = LocalBook::new();
        let mut engine = SignalEngine::new(config(3));

        gate.apply_snapshot(
            &[
                Level {
                    price: 100_00000000,
                    size: 3_00000000,
                },
                Level {
                    price: 99_90000000,
                    size: 2_00000000,
                },
            ],
            &[
                Level {
                    price: 100_10000000,
                    size: 2_50000000,
                },
                Level {
                    price: 100_20000000,
                    size: 4_00000000,
                },
            ],
            Some(10),
        );
        lighter.apply_snapshot(
            &[
                Level {
                    price: 99_80000000,
                    size: 1_00000000,
                },
                Level {
                    price: 99_70000000,
                    size: 2_00000000,
                },
            ],
            &[
                Level {
                    price: 99_90000000,
                    size: 1_50000000,
                },
                Level {
                    price: 100_00000000,
                    size: 3_00000000,
                },
            ],
            Some(20),
        );
        engine.maybe_signal(2, 1, "lighter_snapshot", &gate, &lighter);
        gate.apply_update(
            &[Level {
                price: 100_00000000,
                size: 3_50000000,
            }],
            &[],
            Some(11),
            Some(11),
        );
        engine.maybe_signal(3, 2, "gate_update", &gate, &lighter);
        lighter.apply_update(
            &[Level {
                price: 99_85000000,
                size: 1_20000000,
            }],
            &[],
            None,
            None,
        );
        engine.maybe_signal(4, 3, "lighter_update", &gate, &lighter);
        gate.apply_update(
            &[],
            &[Level {
                price: 100_10000000,
                size: 0,
            }],
            Some(12),
            Some(12),
        );
        engine.maybe_signal(5, 4, "gate_update", &gate, &lighter);
        lighter.apply_update(
            &[],
            &[Level {
                price: 99_95000000,
                size: 1_10000000,
            }],
            None,
            None,
        );
        engine.maybe_signal(6, 5, "lighter_update", &gate, &lighter);
        gate.apply_update(
            &[Level {
                price: 100_05000000,
                size: 1_00000000,
            }],
            &[Level {
                price: 100_15000000,
                size: 2_00000000,
            }],
            Some(13),
            Some(13),
        );
        let row = engine
            .maybe_signal(7, 6, "gate_update", &gate, &lighter)
            .unwrap();
        assert!(row.ready);
        assert_eq!(row.sample_count, 3);
        assert_eq!(format!("{:.8}", row.long_spread), "0.30000000");
        assert_eq!(format!("{:.8}", row.short_spread), "0.15000000");
        assert!(row.long_ok);
        assert!(row.short_ok);

        lighter.apply_update(
            &[Level {
                price: 100_20000000,
                size: 1_00000000,
            }],
            &[Level {
                price: 100_30000000,
                size: 1_00000000,
            }],
            None,
            None,
        );
        assert!(engine
            .maybe_signal(8, 7, "lighter_update", &gate, &lighter)
            .is_none());
    }

    #[test]
    fn directional_spread_medians_roll_with_window() {
        let mut gate = LocalBook::new();
        let mut lighter = LocalBook::new();
        let mut engine = SignalEngine::new(EngineConfig {
            window_size: 3,
            min_samples: 3,
            threshold_bps: 0.0,
            ticker: "BTC".to_string(),
            gate_contract: "BTC_USDT".to_string(),
            lighter_market_id: 1,
        });

        apply_top(&mut lighter, 100.0, 105.0);
        let mut row = None;
        for (sequence, long_spread, short_spread) in
            [(1, 8.0, -2.0), (2, 10.0, 0.0), (3, 12.0, 2.0)]
        {
            apply_top(&mut gate, 105.0 + short_spread, 100.0 + long_spread);
            row = engine.maybe_signal(sequence, sequence, "test", &gate, &lighter);
        }

        let ready = row.unwrap();
        assert!(ready.ready);
        assert_close(ready.long_median, 10.0);
        assert_close(ready.short_median, 0.0);

        apply_top(&mut gate, 111.0, 130.0);
        let rolled = engine.maybe_signal(4, 4, "test", &gate, &lighter).unwrap();
        assert_eq!(rolled.sample_count, 3);
        assert_close(rolled.long_spread, 30.0);
        assert_close(rolled.short_spread, 6.0);
        assert_close(rolled.long_median, 12.0);
        assert_close(rolled.short_median, 2.0);
    }

    #[test]
    fn evaluate_reads_median_without_adding_samples() {
        let mut gate = LocalBook::new();
        let mut lighter = LocalBook::new();
        let mut engine = SignalEngine::new(EngineConfig {
            window_size: 3600,
            min_samples: 1,
            threshold_bps: 0.0,
            ticker: "BTC".to_string(),
            gate_contract: "BTC_USDT".to_string(),
            lighter_market_id: 1,
        });

        apply_top(&mut gate, 100.0, 101.0);
        apply_top(&mut lighter, 100.0, 101.0);
        engine.sample(&gate, &lighter);

        let mut event_gate = LocalBook::new();
        let mut event_lighter = LocalBook::new();
        apply_top(&mut event_gate, 98.0, 99.0);
        apply_top(&mut event_lighter, 100.0, 101.0);

        let first_event = engine
            .evaluate(1, 1, "rust_live", &event_gate, &event_lighter)
            .unwrap();
        let second_event = engine
            .evaluate(2, 2, "rust_live", &event_gate, &event_lighter)
            .unwrap();

        assert!(first_event.ready);
        assert!(first_event.long_ok);
        assert_eq!(first_event.sample_count, 1);
        assert_eq!(second_event.sample_count, 1);
        assert_close(first_event.long_median, 1.0);
        assert_close(second_event.long_median, 1.0);
    }

    fn assert_close(left: f64, right: f64) {
        assert!((left - right).abs() <= 1e-9, "left={left} right={right}");
    }

    fn apply_top(book: &mut LocalBook, bid: f64, ask: f64) {
        book.apply_snapshot(
            &[Level {
                price: (bid * 100_000_000.0).round() as i64,
                size: 1_00000000,
            }],
            &[Level {
                price: (ask * 100_000_000.0).round() as i64,
                size: 1_00000000,
            }],
            Some(1),
        );
    }

    #[test]
    fn crossed_book_does_not_generate_signal() {
        let mut gate = LocalBook::new();
        let mut lighter = LocalBook::new();
        let mut engine = SignalEngine::new(config(1));

        gate.apply_snapshot(
            &[Level {
                price: 100_20000000,
                size: 1_00000000,
            }],
            &[Level {
                price: 100_10000000,
                size: 1_00000000,
            }],
            Some(10),
        );
        lighter.apply_snapshot(
            &[Level {
                price: 99_80000000,
                size: 1_00000000,
            }],
            &[Level {
                price: 99_90000000,
                size: 1_50000000,
            }],
            Some(20),
        );

        assert_eq!(gate.status(), BookStatus::Stale);
        assert!(engine
            .maybe_signal(1, 1, "gate_snapshot", &gate, &lighter)
            .is_none());
    }

    #[test]
    fn empty_book_side_after_update_does_not_generate_signal() {
        let (mut gate, lighter) = ready_books();
        let mut engine = SignalEngine::new(config(1));

        gate.apply_update(
            &[Level {
                price: 100_00000000,
                size: 0,
            }],
            &[],
            Some(11),
            Some(11),
        );

        assert_eq!(gate.status(), BookStatus::Stale);
        assert!(engine
            .maybe_signal(1, 1, "gate_update", &gate, &lighter)
            .is_none());
    }

    #[test]
    fn gate_gap_suppresses_signal_after_lighter_update() {
        let (mut gate, mut lighter) = ready_books();
        let mut engine = SignalEngine::new(config(1));

        let before_gap = engine
            .maybe_signal(1, 1, "lighter_snapshot", &gate, &lighter)
            .unwrap();
        assert!(before_gap.ready);

        gate.apply_update(
            &[Level {
                price: 100_20000000,
                size: 1_00000000,
            }],
            &[],
            Some(12),
            Some(12),
        );
        assert_eq!(gate.status(), BookStatus::Stale);

        lighter.apply_update(
            &[Level {
                price: 100_30000000,
                size: 1_00000000,
            }],
            &[Level {
                price: 100_40000000,
                size: 1_00000000,
            }],
            None,
            None,
        );

        assert!(engine
            .maybe_signal(2, 2, "lighter_update", &gate, &lighter)
            .is_none());
    }
}
