use crate::model::{BookSide, BookStatus, DecimalLevel, FillMetadata, Level, SCALE};

#[derive(Debug, Clone)]
pub struct LocalBook {
    bids: BookSide,
    asks: BookSide,
    last_id: Option<u64>,
    stale: bool,
}

impl LocalBook {
    pub fn new() -> Self {
        Self {
            bids: BookSide::new(),
            asks: BookSide::new(),
            last_id: None,
            stale: true,
        }
    }

    pub fn apply_snapshot(&mut self, bids: &[Level], asks: &[Level], book_id: Option<u64>) {
        self.bids.clear();
        self.asks.clear();
        apply_side(&mut self.bids, bids);
        apply_side(&mut self.asks, asks);
        self.last_id = book_id;
        self.stale = !self.has_bbo();
    }

    pub fn apply_update(
        &mut self,
        bids: &[Level],
        asks: &[Level],
        first_id: Option<u64>,
        last_id: Option<u64>,
    ) {
        if let (Some(current), Some(first), Some(last)) = (self.last_id, first_id, last_id) {
            let expected = current + 1;
            if last < expected {
                return;
            }
            if first != expected {
                self.stale = true;
                return;
            }
        }

        apply_side(&mut self.bids, bids);
        apply_side(&mut self.asks, asks);
        if let Some(last) = last_id {
            self.last_id = Some(last);
        }
        self.stale = !self.has_bbo();
    }

    pub fn status(&self) -> BookStatus {
        if !self.stale && self.has_bbo() {
            BookStatus::Ready
        } else {
            BookStatus::Stale
        }
    }

    pub fn last_id(&self) -> Option<u64> {
        self.last_id
    }

    pub fn best_bid(&self) -> Option<(f64, f64)> {
        self.bids
            .iter()
            .next_back()
            .map(|(price, size)| (to_decimal(*price), to_decimal(*size)))
    }

    pub fn best_bid_raw(&self) -> Option<(i64, i64)> {
        self.bids
            .iter()
            .next_back()
            .map(|(price, size)| (*price, *size))
    }

    pub fn best_ask(&self) -> Option<(f64, f64)> {
        self.asks
            .iter()
            .next()
            .map(|(price, size)| (to_decimal(*price), to_decimal(*size)))
    }

    pub fn best_ask_raw(&self) -> Option<(i64, i64)> {
        self.asks.iter().next().map(|(price, size)| (*price, *size))
    }

    pub fn bid_depth(&self) -> usize {
        self.bids.len()
    }

    pub fn ask_depth(&self) -> usize {
        self.asks.len()
    }

    pub fn bid_levels(&self, limit: usize) -> Vec<DecimalLevel> {
        self.bids
            .iter()
            .rev()
            .take(limit)
            .map(|(price, size)| DecimalLevel {
                price: to_decimal(*price),
                size: to_decimal(*size),
            })
            .collect()
    }

    pub fn ask_levels(&self, limit: usize) -> Vec<DecimalLevel> {
        self.asks
            .iter()
            .take(limit)
            .map(|(price, size)| DecimalLevel {
                price: to_decimal(*price),
                size: to_decimal(*size),
            })
            .collect()
    }

    pub fn weighted_fill_by_quote(&self, side: FillSide, quote: f64) -> Option<WeightedFill> {
        if quote <= 0.0 {
            return None;
        }
        let mut remaining = quote;
        let mut filled_quantity = 0.0;
        let mut notional = 0.0;
        let mut levels_used = 0usize;

        let iter: Box<dyn Iterator<Item = (&i64, &i64)>> = match side {
            FillSide::Buy => Box::new(self.asks.iter()),
            FillSide::Sell => Box::new(self.bids.iter().rev()),
        };

        for (raw_price, raw_size) in iter {
            let price = to_decimal(*raw_price);
            let size = to_decimal(*raw_size);
            if price <= 0.0 || size <= 0.0 {
                continue;
            }
            let level_notional = price * size;
            let take_notional = remaining.min(level_notional);
            let take_quantity = take_notional / price;
            filled_quantity += take_quantity;
            notional += take_notional;
            remaining -= take_notional;
            levels_used += 1;
            if remaining <= 0.00000001 {
                remaining = 0.0;
                break;
            }
        }

        if filled_quantity <= 0.0 {
            return None;
        }
        Some(WeightedFill {
            avg_price: notional / filled_quantity,
            filled_quantity,
            remaining_quote: remaining,
            is_complete: remaining <= 0.00000001,
            levels_used,
        })
    }

    fn has_bbo(&self) -> bool {
        match (self.bids.keys().next_back(), self.asks.keys().next()) {
            (Some(best_bid), Some(best_ask)) => best_bid < best_ask,
            _ => false,
        }
    }
}

impl WeightedFill {
    pub fn metadata(self) -> FillMetadata {
        FillMetadata {
            avg_price: self.avg_price,
            filled_quantity: self.filled_quantity,
            levels_used: self.levels_used,
            remaining_quote: self.remaining_quote,
            is_complete: self.is_complete,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WeightedFill {
    pub avg_price: f64,
    pub filled_quantity: f64,
    pub remaining_quote: f64,
    pub is_complete: bool,
    pub levels_used: usize,
}

impl Default for LocalBook {
    fn default() -> Self {
        Self::new()
    }
}

fn to_decimal(value: i64) -> f64 {
    value as f64 / SCALE as f64
}

fn apply_side(side: &mut BookSide, levels: &[Level]) {
    for level in levels {
        if level.size == 0 {
            side.remove(&level.price);
        } else if level.size > 0 {
            side.insert(level.price, level.size);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_sets_best_bid_ask_and_ready_status() {
        let mut book = LocalBook::new();

        book.apply_snapshot(
            &[
                Level {
                    price: 99_90000000,
                    size: 2_00000000,
                },
                Level {
                    price: 100_00000000,
                    size: 3_00000000,
                },
            ],
            &[
                Level {
                    price: 100_20000000,
                    size: 4_00000000,
                },
                Level {
                    price: 100_10000000,
                    size: 2_50000000,
                },
            ],
            Some(10),
        );

        assert_eq!(book.status(), BookStatus::Ready);
        assert_eq!(book.best_bid(), Some((100.0, 3.0)));
        assert_eq!(book.best_ask(), Some((100.1, 2.5)));
    }

    #[test]
    fn update_replaces_and_removes_levels() {
        let mut book = LocalBook::new();
        book.apply_snapshot(
            &[Level {
                price: 100_00000000,
                size: 3_00000000,
            }],
            &[Level {
                price: 100_10000000,
                size: 2_00000000,
            }],
            Some(10),
        );

        book.apply_update(
            &[Level {
                price: 100_05000000,
                size: 1_00000000,
            }],
            &[Level {
                price: 100_10000000,
                size: 0,
            }],
            Some(11),
            Some(11),
        );

        assert_eq!(book.best_bid(), Some((100.05, 1.0)));
        assert_eq!(book.best_ask(), None);
        assert_eq!(book.status(), BookStatus::Stale);
    }

    #[test]
    fn old_gate_update_is_ignored_and_gap_marks_book_stale() {
        let mut book = LocalBook::new();
        book.apply_snapshot(
            &[Level {
                price: 100_00000000,
                size: 3_00000000,
            }],
            &[Level {
                price: 100_10000000,
                size: 2_00000000,
            }],
            Some(10),
        );

        book.apply_update(
            &[Level {
                price: 101_00000000,
                size: 1_00000000,
            }],
            &[],
            Some(9),
            Some(9),
        );
        assert_eq!(book.best_bid(), Some((100.0, 3.0)));
        assert_eq!(book.status(), BookStatus::Ready);

        book.apply_update(&[], &[], Some(12), Some(12));
        assert_eq!(book.status(), BookStatus::Stale);
    }

    #[test]
    fn crossed_book_is_stale() {
        let mut book = LocalBook::new();

        book.apply_snapshot(
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

        assert_eq!(book.best_bid(), Some((100.2, 1.0)));
        assert_eq!(book.best_ask(), Some((100.1, 1.0)));
        assert_eq!(book.status(), BookStatus::Stale);
    }

    #[test]
    fn empty_bids_or_asks_after_update_marks_book_stale() {
        let mut book = LocalBook::new();
        book.apply_snapshot(
            &[Level {
                price: 100_00000000,
                size: 3_00000000,
            }],
            &[Level {
                price: 100_10000000,
                size: 2_00000000,
            }],
            Some(10),
        );

        book.apply_update(
            &[Level {
                price: 100_00000000,
                size: 0,
            }],
            &[],
            Some(11),
            Some(11),
        );

        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), Some((100.1, 2.0)));
        assert_eq!(book.status(), BookStatus::Stale);

        book.apply_snapshot(
            &[Level {
                price: 100_00000000,
                size: 3_00000000,
            }],
            &[Level {
                price: 100_10000000,
                size: 2_00000000,
            }],
            Some(20),
        );
        book.apply_update(
            &[],
            &[Level {
                price: 100_10000000,
                size: 0,
            }],
            Some(21),
            Some(21),
        );

        assert_eq!(book.best_bid(), Some((100.0, 3.0)));
        assert_eq!(book.best_ask(), None);
        assert_eq!(book.status(), BookStatus::Stale);
    }

    #[test]
    fn old_gate_update_with_last_before_expected_is_ignored_without_changing_bbo() {
        let mut book = LocalBook::new();
        book.apply_snapshot(
            &[Level {
                price: 100_00000000,
                size: 3_00000000,
            }],
            &[Level {
                price: 100_10000000,
                size: 2_00000000,
            }],
            Some(10),
        );

        book.apply_update(
            &[Level {
                price: 101_00000000,
                size: 1_00000000,
            }],
            &[Level {
                price: 99_00000000,
                size: 1_00000000,
            }],
            Some(8),
            Some(9),
        );

        assert_eq!(book.best_bid(), Some((100.0, 3.0)));
        assert_eq!(book.best_ask(), Some((100.1, 2.0)));
        assert_eq!(book.status(), BookStatus::Ready);
    }

    #[test]
    fn weighted_fill_by_quote_uses_depth_levels() {
        let mut book = LocalBook::new();
        book.apply_snapshot(
            &[
                Level {
                    price: 99_00000000,
                    size: 10_00000000,
                },
                Level {
                    price: 100_00000000,
                    size: 10_00000000,
                },
            ],
            &[
                Level {
                    price: 101_00000000,
                    size: 1_00000000,
                },
                Level {
                    price: 102_00000000,
                    size: 2_00000000,
                },
            ],
            Some(1),
        );

        let fill = book
            .weighted_fill_by_quote(FillSide::Buy, 202.0)
            .expect("fill should exist");

        assert_eq!(fill.levels_used, 2);
        assert!(fill.is_complete);
        assert!((fill.avg_price - 101.49753695).abs() < 0.00000001);
    }
}
