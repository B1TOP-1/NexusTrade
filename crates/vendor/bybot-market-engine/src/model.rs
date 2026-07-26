use std::collections::BTreeMap;

pub const SCALE: i64 = 100_000_000;

pub type BookSide = BTreeMap<i64, i64>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookStatus {
    Ready,
    Stale,
}

impl BookStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            BookStatus::Ready => "ready",
            BookStatus::Stale => "stale",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level {
    pub price: i64,
    pub size: i64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DecimalLevel {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FillMetadata {
    pub avg_price: f64,
    pub filled_quantity: f64,
    pub levels_used: usize,
    pub remaining_quote: f64,
    pub is_complete: bool,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SignalDepthMetadata {
    pub gate_bid_levels: Vec<DecimalLevel>,
    pub gate_ask_levels: Vec<DecimalLevel>,
    pub lighter_bid_levels: Vec<DecimalLevel>,
    pub lighter_ask_levels: Vec<DecimalLevel>,
    pub gate_bid_fill: Option<FillMetadata>,
    pub gate_ask_fill: Option<FillMetadata>,
    pub lighter_bid_fill: Option<FillMetadata>,
    pub lighter_ask_fill: Option<FillMetadata>,
}

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub window_size: usize,
    pub min_samples: usize,
    pub threshold_bps: f64,
    pub ticker: String,
    pub gate_contract: String,
    pub lighter_market_id: u64,
}

#[derive(Debug, Clone)]
pub struct SignalRow {
    pub sequence: u64,
    pub timestamp_ns: u64,
    pub source: String,
    pub ticker: String,
    pub gate_contract: String,
    pub lighter_market_id: u64,
    pub ready: bool,
    pub sample_count: usize,
    pub lighter_bid: f64,
    pub lighter_bid_size: f64,
    pub lighter_ask: f64,
    pub lighter_ask_size: f64,
    pub gate_bid: f64,
    pub gate_bid_size: f64,
    pub gate_ask: f64,
    pub gate_ask_size: f64,
    pub long_spread: f64,
    pub short_spread: f64,
    pub long_median: f64,
    pub short_median: f64,
    pub long_threshold: f64,
    pub short_threshold: f64,
    pub basis: f64,
    pub long_ok: bool,
    pub short_ok: bool,
    pub gate_book_status: BookStatus,
    pub lighter_book_status: BookStatus,
    pub depth: Option<SignalDepthMetadata>,
}
