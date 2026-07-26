use std::{collections::HashMap, error::Error, fmt};

use rust_decimal::Decimal;

use crate::orderbook::{
    BookError, BookLevel, BookSide, BookState, FillEstimate, HypeOrderBook, SnapshotInput,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalBookConfig {
    stale_after_ms: u64,
}

impl LocalBookConfig {
    #[must_use]
    pub const fn new(stale_after_ms: u64) -> Self {
        Self { stale_after_ms }
    }

    #[must_use]
    pub const fn stale_after_ms(self) -> u64 {
        self.stale_after_ms
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalBookError {
    NoSymbols,
    InvalidStaleAfter,
    EmptySymbol,
    DuplicateSymbol(String),
    UnknownSymbol(String),
    NotTradeable { symbol: String, state: BookState },
    Book { symbol: String, error: BookError },
}

impl fmt::Display for LocalBookError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoSymbols => formatter.write_str("at least one symbol is required"),
            Self::InvalidStaleAfter => {
                formatter.write_str("stale timeout must be greater than zero")
            }
            Self::EmptySymbol => formatter.write_str("symbol cannot be empty"),
            Self::DuplicateSymbol(symbol) => write!(formatter, "duplicate symbol: {symbol}"),
            Self::UnknownSymbol(symbol) => write!(formatter, "unknown symbol: {symbol}"),
            Self::NotTradeable { symbol, state } => {
                write!(
                    formatter,
                    "order book is not tradeable: {symbol} state={state:?}"
                )
            }
            Self::Book { symbol, error } => {
                write!(
                    formatter,
                    "order book rejected update: {symbol} error={error:?}"
                )
            }
        }
    }
}

impl Error for LocalBookError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopOfBook {
    exchange_time_ms: u64,
    received_time_ms: u64,
    best_bid: BookLevel,
    best_ask: BookLevel,
}

impl TopOfBook {
    #[must_use]
    pub const fn exchange_time_ms(self) -> u64 {
        self.exchange_time_ms
    }

    #[must_use]
    pub const fn received_time_ms(self) -> u64 {
        self.received_time_ms
    }

    #[must_use]
    pub const fn best_bid(self) -> BookLevel {
        self.best_bid
    }

    #[must_use]
    pub const fn best_ask(self) -> BookLevel {
        self.best_ask
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalBookSnapshot {
    symbol: String,
    state: BookState,
    exchange_time_ms: Option<u64>,
    received_time_ms: Option<u64>,
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
}

impl LocalBookSnapshot {
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
}

#[derive(Debug)]
pub struct LocalOrderBookModule {
    books: HashMap<String, HypeOrderBook>,
}

impl LocalOrderBookModule {
    pub fn new<I, S>(symbols: I, config: LocalBookConfig) -> Result<Self, LocalBookError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        if config.stale_after_ms == 0 {
            return Err(LocalBookError::InvalidStaleAfter);
        }

        let mut books = HashMap::new();
        for symbol in symbols {
            let symbol = symbol.into();
            if symbol.trim().is_empty() {
                return Err(LocalBookError::EmptySymbol);
            }
            if books.contains_key(&symbol) {
                return Err(LocalBookError::DuplicateSymbol(symbol));
            }
            books.insert(
                symbol.clone(),
                HypeOrderBook::new(symbol, config.stale_after_ms),
            );
        }
        if books.is_empty() {
            return Err(LocalBookError::NoSymbols);
        }
        Ok(Self { books })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.books.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.books.is_empty()
    }

    #[must_use]
    pub fn symbols(&self) -> Vec<&str> {
        let mut symbols = self.books.keys().map(String::as_str).collect::<Vec<_>>();
        symbols.sort_unstable();
        symbols
    }

    pub fn mark_connected(&mut self) {
        for book in self.books.values_mut() {
            book.mark_connected();
        }
    }

    pub fn mark_disconnected(&mut self) {
        for book in self.books.values_mut() {
            book.mark_disconnected();
        }
    }

    pub fn apply_snapshot(
        &mut self,
        symbol: &str,
        snapshot: SnapshotInput,
    ) -> Result<(), LocalBookError> {
        let book = self.book_mut(symbol)?;
        book.apply_snapshot(snapshot)
            .map_err(|error| LocalBookError::Book {
                symbol: symbol.to_string(),
                error,
            })
    }

    pub fn top_of_book(&mut self, symbol: &str, now_ms: u64) -> Result<TopOfBook, LocalBookError> {
        let book = self.book_mut(symbol)?;
        if !book.is_tradeable(now_ms) {
            return Err(LocalBookError::NotTradeable {
                symbol: symbol.to_string(),
                state: book.state(),
            });
        }
        let Some(exchange_time_ms) = book.exchange_time_ms() else {
            return Err(LocalBookError::NotTradeable {
                symbol: symbol.to_string(),
                state: book.state(),
            });
        };
        let Some(received_time_ms) = book.received_time_ms() else {
            return Err(LocalBookError::NotTradeable {
                symbol: symbol.to_string(),
                state: book.state(),
            });
        };
        let Some(best_bid) = book.best_bid() else {
            return Err(LocalBookError::NotTradeable {
                symbol: symbol.to_string(),
                state: book.state(),
            });
        };
        let Some(best_ask) = book.best_ask() else {
            return Err(LocalBookError::NotTradeable {
                symbol: symbol.to_string(),
                state: book.state(),
            });
        };
        Ok(TopOfBook {
            exchange_time_ms,
            received_time_ms,
            best_bid,
            best_ask,
        })
    }

    pub fn snapshot(&self, symbol: &str) -> Result<LocalBookSnapshot, LocalBookError> {
        let book = self.book(symbol)?;
        Ok(LocalBookSnapshot {
            symbol: book.symbol().to_string(),
            state: book.state(),
            exchange_time_ms: book.exchange_time_ms(),
            received_time_ms: book.received_time_ms(),
            bids: book.bids().to_vec(),
            asks: book.asks().to_vec(),
        })
    }

    pub fn estimate_buy(
        &mut self,
        symbol: &str,
        requested_size: i64,
        now_ms: u64,
    ) -> Result<FillEstimate, LocalBookError> {
        self.estimate(symbol, requested_size, now_ms, true)
    }

    pub fn estimate_sell(
        &mut self,
        symbol: &str,
        requested_size: i64,
        now_ms: u64,
    ) -> Result<FillEstimate, LocalBookError> {
        self.estimate(symbol, requested_size, now_ms, false)
    }

    pub fn vwap_for_quote_notional(
        &mut self,
        symbol: &str,
        side: BookSide,
        quote_notional: Decimal,
        now_ms: u64,
    ) -> Result<Option<Decimal>, LocalBookError> {
        let book = self.book_mut(symbol)?;
        if !book.is_tradeable(now_ms) {
            return Err(LocalBookError::NotTradeable {
                symbol: symbol.to_string(),
                state: book.state(),
            });
        }
        Ok(book.reference_vwap_for_quote_notional(side, quote_notional))
    }

    fn estimate(
        &mut self,
        symbol: &str,
        requested_size: i64,
        now_ms: u64,
        is_buy: bool,
    ) -> Result<FillEstimate, LocalBookError> {
        let book = self.book_mut(symbol)?;
        if !book.is_tradeable(now_ms) {
            return Err(LocalBookError::NotTradeable {
                symbol: symbol.to_string(),
                state: book.state(),
            });
        }
        let result = if is_buy {
            book.estimate_buy(requested_size)
        } else {
            book.estimate_sell(requested_size)
        };
        result.map_err(|error| LocalBookError::Book {
            symbol: symbol.to_string(),
            error,
        })
    }

    fn book(&self, symbol: &str) -> Result<&HypeOrderBook, LocalBookError> {
        self.books
            .get(symbol)
            .ok_or_else(|| LocalBookError::UnknownSymbol(symbol.to_string()))
    }

    fn book_mut(&mut self, symbol: &str) -> Result<&mut HypeOrderBook, LocalBookError> {
        self.books
            .get_mut(symbol)
            .ok_or_else(|| LocalBookError::UnknownSymbol(symbol.to_string()))
    }
}
