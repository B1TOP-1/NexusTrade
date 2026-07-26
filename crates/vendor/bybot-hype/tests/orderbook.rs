use bybot_hype::orderbook::{
    BookError, BookLevel, BookSide, BookState, HypeOrderBook, SnapshotInput, StaleReason,
};
use rust_decimal::Decimal;

fn level(price: i64, size: i64, orders: u32) -> BookLevel {
    BookLevel::new(price, size, orders)
}

fn snapshot(exchange_time_ms: u64, received_time_ms: u64) -> SnapshotInput {
    SnapshotInput::new(
        exchange_time_ms,
        received_time_ms,
        vec![
            level(100_00000000, 2_00000000, 2),
            level(99_90000000, 3_00000000, 1),
        ],
        vec![
            level(100_10000000, 1_50000000, 1),
            level(100_20000000, 4_00000000, 3),
        ],
    )
}

#[test]
fn full_snapshot_replaces_previous_levels_and_marks_ready() {
    let mut book = HypeOrderBook::new("HYPE", 1_000);
    book.apply_snapshot(snapshot(100, 110)).unwrap();

    let replacement = SnapshotInput::new(
        101,
        120,
        vec![level(101_00000000, 5_00000000, 1)],
        vec![level(101_10000000, 6_00000000, 1)],
    );
    book.apply_snapshot(replacement).unwrap();

    assert_eq!(book.state(), BookState::Ready);
    assert_eq!(book.best_bid().unwrap().price(), 101_00000000);
    assert_eq!(book.best_ask().unwrap().price(), 101_10000000);
    assert_eq!(book.bids().len(), 1);
    assert_eq!(book.asks().len(), 1);
}

#[test]
fn older_or_duplicate_snapshot_is_rejected_without_overwriting_book() {
    let mut book = HypeOrderBook::new("HYPE", 1_000);
    book.apply_snapshot(snapshot(100, 110)).unwrap();

    let result = book.apply_snapshot(snapshot(100, 120));

    assert_eq!(
        result,
        Err(BookError::NonIncreasingExchangeTime {
            current: 100,
            incoming: 100
        })
    );
    assert_eq!(book.best_bid().unwrap().price(), 100_00000000);
}

#[test]
fn crossed_or_empty_book_is_rejected() {
    let mut book = HypeOrderBook::new("HYPE", 1_000);
    let crossed = SnapshotInput::new(
        100,
        110,
        vec![level(101_00000000, 1_00000000, 1)],
        vec![level(100_00000000, 1_00000000, 1)],
    );

    assert_eq!(book.apply_snapshot(crossed), Err(BookError::CrossedBook));
    assert_eq!(book.state(), BookState::WaitingSnapshot);
}

#[test]
fn invalid_snapshot_after_ready_preserves_levels_and_marks_stale() {
    let mut book = HypeOrderBook::new("HYPE", 1_000);
    book.apply_snapshot(snapshot(100, 110)).unwrap();
    let crossed = SnapshotInput::new(
        101,
        120,
        vec![level(101_00000000, 1_00000000, 1)],
        vec![level(100_00000000, 1_00000000, 1)],
    );

    assert_eq!(book.apply_snapshot(crossed), Err(BookError::CrossedBook));
    assert_eq!(book.state(), BookState::Stale(StaleReason::InvalidSnapshot));
    assert_eq!(book.best_bid().unwrap().price(), 100_00000000);
    assert_eq!(book.best_ask().unwrap().price(), 100_10000000);
}

#[test]
fn disconnect_and_stale_timeout_disable_trading_until_new_snapshot() {
    let mut book = HypeOrderBook::new("HYPE", 1_000);
    book.apply_snapshot(snapshot(100, 110)).unwrap();
    assert!(book.is_tradeable(1_000));

    assert!(!book.is_tradeable(1_111));
    assert_eq!(book.state(), BookState::Stale(StaleReason::Timeout));

    book.mark_disconnected();
    assert_eq!(book.state(), BookState::Disconnected);

    book.mark_connected();
    assert_eq!(book.state(), BookState::WaitingSnapshot);
    assert!(!book.is_tradeable(1_112));

    book.apply_snapshot(snapshot(200, 1_120)).unwrap();
    assert_eq!(book.state(), BookState::Ready);
}

#[test]
fn estimate_buy_reports_average_price_levels_and_incomplete_depth() {
    let mut book = HypeOrderBook::new("HYPE", 1_000);
    book.apply_snapshot(snapshot(100, 110)).unwrap();

    let complete = book.estimate_buy(3_00000000).unwrap();
    assert!(complete.is_complete());
    assert_eq!(complete.filled_size(), 3_00000000);
    assert_eq!(complete.levels_used(), 2);

    let incomplete = book.estimate_buy(10_00000000).unwrap();
    assert!(!incomplete.is_complete());
    assert_eq!(incomplete.filled_size(), 5_50000000);
    assert_eq!(incomplete.remaining_size(), 4_50000000);
}

#[test]
fn quote_notional_vwap_requires_complete_depth() {
    let mut book = HypeOrderBook::new("HYPE", 1_000);
    book.apply_snapshot(snapshot(100, 110)).unwrap();

    assert!(book
        .vwap_for_quote_notional(BookSide::Bid, Decimal::from(200))
        .is_some());
    assert_eq!(
        book.vwap_for_quote_notional(BookSide::Ask, Decimal::from(1_000)),
        None
    );
}

#[test]
fn reference_vwap_uses_all_visible_depth_when_notional_is_incomplete() {
    let mut book = HypeOrderBook::new("HYPE", 1_000);
    book.apply_snapshot(snapshot(100, 110)).unwrap();

    let vwap = book
        .reference_vwap_for_quote_notional(BookSide::Ask, Decimal::from(1_000))
        .unwrap();

    assert!(vwap > Decimal::new(1_001, 1));
    assert!(vwap < Decimal::new(1_002, 1));
}

#[test]
fn unsorted_levels_are_normalized_and_duplicate_prices_are_rejected() {
    let mut book = HypeOrderBook::new("HYPE", 1_000);
    let unsorted = SnapshotInput::new(
        100,
        110,
        vec![
            level(99_00000000, 1_00000000, 1),
            level(100_00000000, 2_00000000, 1),
        ],
        vec![
            level(102_00000000, 1_00000000, 1),
            level(101_00000000, 2_00000000, 1),
        ],
    );
    book.apply_snapshot(unsorted).unwrap();

    assert_eq!(book.best_bid().unwrap().price(), 100_00000000);
    assert_eq!(book.best_ask().unwrap().price(), 101_00000000);

    let duplicate = SnapshotInput::new(
        101,
        120,
        vec![
            level(100_00000000, 1_00000000, 1),
            level(100_00000000, 2_00000000, 1),
        ],
        vec![level(101_00000000, 1_00000000, 1)],
    );
    assert_eq!(
        book.apply_snapshot(duplicate),
        Err(BookError::DuplicatePrice {
            side: bybot_hype::orderbook::BookSide::Bid,
            price: 100_00000000,
        })
    );
}

#[test]
fn estimate_sell_uses_bid_depth_and_calculates_slippage() {
    let mut book = HypeOrderBook::new("HYPE", 1_000);
    book.apply_snapshot(snapshot(100, 110)).unwrap();

    let estimate = book.estimate_sell(4_00000000).unwrap();

    assert!(estimate.is_complete());
    assert_eq!(estimate.filled_size(), 4_00000000);
    assert_eq!(estimate.levels_used(), 2);
    assert_eq!(estimate.worst_price(), 99_90000000);
    assert_eq!(estimate.slippage_bps(100_00000000, false).unwrap(), 5);
}
