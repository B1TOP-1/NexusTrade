use std::{cmp::Reverse, collections::HashSet};

use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BookLevel {
    price: i64,
    size: i64,
    orders: u32,
}

impl BookLevel {
    #[must_use]
    pub const fn new(price: i64, size: i64, orders: u32) -> Self {
        Self {
            price,
            size,
            orders,
        }
    }

    #[must_use]
    pub const fn price(self) -> i64 {
        self.price
    }

    #[must_use]
    pub const fn size(self) -> i64 {
        self.size
    }

    #[must_use]
    pub const fn orders(self) -> u32 {
        self.orders
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInput {
    exchange_time_ms: u64,
    received_time_ms: u64,
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
}

impl SnapshotInput {
    #[must_use]
    pub fn new(
        exchange_time_ms: u64,
        received_time_ms: u64,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
    ) -> Self {
        Self {
            exchange_time_ms,
            received_time_ms,
            bids,
            asks,
        }
    }

    #[must_use]
    pub const fn exchange_time_ms(&self) -> u64 {
        self.exchange_time_ms
    }

    #[must_use]
    pub const fn received_time_ms(&self) -> u64 {
        self.received_time_ms
    }

    #[must_use]
    pub fn bids(&self) -> &[BookLevel] {
        &self.bids
    }

    #[must_use]
    pub fn asks(&self) -> &[BookLevel] {
        &self.asks
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleReason {
    Timeout,
    InvalidSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookState {
    Disconnected,
    WaitingSnapshot,
    Ready,
    Stale(StaleReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BookError {
    EmptyBids,
    EmptyAsks,
    InvalidPrice { side: BookSide, index: usize },
    InvalidSize { side: BookSide, index: usize },
    DuplicatePrice { side: BookSide, price: i64 },
    CrossedBook,
    NonIncreasingExchangeTime { current: u64, incoming: u64 },
    BookNotReady,
    InvalidRequestedSize,
    ArithmeticOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FillEstimate {
    requested_size: i64,
    filled_size: i64,
    remaining_size: i64,
    average_price: i64,
    worst_price: i64,
    levels_used: usize,
    complete: bool,
}

impl FillEstimate {
    #[must_use]
    pub const fn requested_size(self) -> i64 {
        self.requested_size
    }

    #[must_use]
    pub const fn filled_size(self) -> i64 {
        self.filled_size
    }

    #[must_use]
    pub const fn remaining_size(self) -> i64 {
        self.remaining_size
    }

    #[must_use]
    pub const fn average_price(self) -> i64 {
        self.average_price
    }

    #[must_use]
    pub const fn worst_price(self) -> i64 {
        self.worst_price
    }

    #[must_use]
    pub const fn levels_used(self) -> usize {
        self.levels_used
    }

    #[must_use]
    pub const fn is_complete(self) -> bool {
        self.complete
    }

    pub fn slippage_bps(self, reference_price: i64, is_buy: bool) -> Result<i64, BookError> {
        if reference_price <= 0 {
            return Err(BookError::InvalidRequestedSize);
        }

        let difference = if is_buy {
            self.average_price - reference_price
        } else {
            reference_price - self.average_price
        };
        let numerator = i128::from(difference)
            .checked_mul(10_000)
            .ok_or(BookError::ArithmeticOverflow)?;
        let result = numerator / i128::from(reference_price);
        i64::try_from(result).map_err(|_| BookError::ArithmeticOverflow)
    }
}

#[derive(Debug, Clone)]
pub struct HypeOrderBook {
    symbol: String,
    stale_after_ms: u64,
    state: BookState,
    exchange_time_ms: Option<u64>,
    received_time_ms: Option<u64>,
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
}

impl HypeOrderBook {
    #[must_use]
    pub fn new(symbol: impl Into<String>, stale_after_ms: u64) -> Self {
        Self {
            symbol: symbol.into(),
            stale_after_ms,
            state: BookState::WaitingSnapshot,
            exchange_time_ms: None,
            received_time_ms: None,
            bids: Vec::new(),
            asks: Vec::new(),
        }
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub const fn state(&self) -> BookState {
        self.state
    }

    #[must_use]
    pub const fn exchange_time_ms(&self) -> Option<u64> {
        self.exchange_time_ms
    }

    #[must_use]
    pub const fn received_time_ms(&self) -> Option<u64> {
        self.received_time_ms
    }

    #[must_use]
    pub fn bids(&self) -> &[BookLevel] {
        &self.bids
    }

    #[must_use]
    pub fn asks(&self) -> &[BookLevel] {
        &self.asks
    }

    #[must_use]
    pub fn best_bid(&self) -> Option<BookLevel> {
        self.bids.first().copied()
    }

    #[must_use]
    pub fn best_ask(&self) -> Option<BookLevel> {
        self.asks.first().copied()
    }

    pub fn apply_snapshot(&mut self, input: SnapshotInput) -> Result<(), BookError> {
        if let Some(current) = self.exchange_time_ms {
            if input.exchange_time_ms <= current {
                return Err(BookError::NonIncreasingExchangeTime {
                    current,
                    incoming: input.exchange_time_ms,
                });
            }
        }

        let result = Self::validate_and_sort(input.bids, input.asks);
        let (bids, asks) = match result {
            Ok(levels) => levels,
            Err(error) => {
                if self.exchange_time_ms.is_some() {
                    self.state = BookState::Stale(StaleReason::InvalidSnapshot);
                }
                return Err(error);
            }
        };

        self.bids = bids;
        self.asks = asks;
        self.exchange_time_ms = Some(input.exchange_time_ms);
        self.received_time_ms = Some(input.received_time_ms);
        self.state = BookState::Ready;
        Ok(())
    }

    pub fn mark_disconnected(&mut self) {
        self.state = BookState::Disconnected;
    }

    pub fn mark_connected(&mut self) {
        self.state = BookState::WaitingSnapshot;
    }

    pub fn mark_stale(&mut self, reason: StaleReason) {
        self.state = BookState::Stale(reason);
    }

    pub fn is_tradeable(&mut self, now_ms: u64) -> bool {
        if self.state != BookState::Ready {
            return false;
        }

        let Some(received_time_ms) = self.received_time_ms else {
            self.state = BookState::Stale(StaleReason::Timeout);
            return false;
        };

        if now_ms.saturating_sub(received_time_ms) > self.stale_after_ms {
            self.state = BookState::Stale(StaleReason::Timeout);
            return false;
        }

        true
    }

    pub fn estimate_buy(&self, requested_size: i64) -> Result<FillEstimate, BookError> {
        self.estimate(&self.asks, requested_size)
    }

    pub fn estimate_sell(&self, requested_size: i64) -> Result<FillEstimate, BookError> {
        self.estimate(&self.bids, requested_size)
    }

    #[must_use]
    pub fn vwap_for_quote_notional(
        &self,
        side: BookSide,
        quote_notional: Decimal,
    ) -> Option<Decimal> {
        self.quote_vwap(side, quote_notional, false)
    }

    #[must_use]
    pub fn reference_vwap_for_quote_notional(
        &self,
        side: BookSide,
        quote_notional: Decimal,
    ) -> Option<Decimal> {
        self.quote_vwap(side, quote_notional, true)
    }

    fn quote_vwap(
        &self,
        side: BookSide,
        quote_notional: Decimal,
        allow_partial: bool,
    ) -> Option<Decimal> {
        if self.state != BookState::Ready || quote_notional <= Decimal::ZERO {
            return None;
        }
        let levels = match side {
            BookSide::Bid => &self.bids,
            BookSide::Ask => &self.asks,
        };
        let mut remaining = quote_notional;
        let mut total_base = Decimal::ZERO;
        let mut total_quote = Decimal::ZERO;
        for level in levels {
            let price = Decimal::new(level.price, 8);
            let size = Decimal::new(level.size, 8);
            let level_quote = price * size;
            let take_quote = level_quote.min(remaining);
            total_quote += take_quote;
            total_base += take_quote / price;
            remaining -= take_quote;
            if remaining <= Decimal::ZERO {
                break;
            }
        }
        if (!allow_partial && remaining > Decimal::ZERO) || total_base <= Decimal::ZERO {
            None
        } else {
            Some(total_quote / total_base)
        }
    }

    fn validate_and_sort(
        mut bids: Vec<BookLevel>,
        mut asks: Vec<BookLevel>,
    ) -> Result<(Vec<BookLevel>, Vec<BookLevel>), BookError> {
        if bids.is_empty() {
            return Err(BookError::EmptyBids);
        }
        if asks.is_empty() {
            return Err(BookError::EmptyAsks);
        }

        Self::validate_levels(&bids, BookSide::Bid)?;
        Self::validate_levels(&asks, BookSide::Ask)?;
        bids.sort_unstable_by_key(|level| Reverse(level.price));
        asks.sort_unstable_by_key(|level| level.price);

        if bids[0].price >= asks[0].price {
            return Err(BookError::CrossedBook);
        }

        Ok((bids, asks))
    }

    fn validate_levels(levels: &[BookLevel], side: BookSide) -> Result<(), BookError> {
        let mut prices = HashSet::with_capacity(levels.len());
        for (index, level) in levels.iter().enumerate() {
            if level.price <= 0 {
                return Err(BookError::InvalidPrice { side, index });
            }
            if level.size <= 0 {
                return Err(BookError::InvalidSize { side, index });
            }
            if !prices.insert(level.price) {
                return Err(BookError::DuplicatePrice {
                    side,
                    price: level.price,
                });
            }
        }
        Ok(())
    }

    fn estimate(
        &self,
        levels: &[BookLevel],
        requested_size: i64,
    ) -> Result<FillEstimate, BookError> {
        if self.state != BookState::Ready {
            return Err(BookError::BookNotReady);
        }
        if requested_size <= 0 {
            return Err(BookError::InvalidRequestedSize);
        }

        let mut remaining_size = requested_size;
        let mut filled_size = 0_i64;
        let mut quote_notional = 0_i128;
        let mut worst_price = 0_i64;
        let mut levels_used = 0_usize;

        for level in levels {
            if remaining_size == 0 {
                break;
            }
            let size = remaining_size.min(level.size);
            let notional = i128::from(level.price)
                .checked_mul(i128::from(size))
                .ok_or(BookError::ArithmeticOverflow)?;
            quote_notional = quote_notional
                .checked_add(notional)
                .ok_or(BookError::ArithmeticOverflow)?;
            filled_size = filled_size
                .checked_add(size)
                .ok_or(BookError::ArithmeticOverflow)?;
            remaining_size -= size;
            worst_price = level.price;
            levels_used += 1;
        }

        if filled_size == 0 {
            return Err(BookError::BookNotReady);
        }

        let average_price = quote_notional / i128::from(filled_size);
        Ok(FillEstimate {
            requested_size,
            filled_size,
            remaining_size,
            average_price: i64::try_from(average_price)
                .map_err(|_| BookError::ArithmeticOverflow)?,
            worst_price,
            levels_used,
            complete: remaining_size == 0,
        })
    }
}
