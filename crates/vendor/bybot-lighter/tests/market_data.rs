use bybot_lighter::{
    data::{parse_order_book_message, LighterBookMessageKind},
    local_book::{LighterBookStatus, LighterDepthSide, LighterLocalBook},
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[test]
fn parses_snapshot_with_object_and_array_levels() {
    let message = parse_order_book_message(
        r#"{
            "type":"subscribed/order_book",
            "channel":"order_book:1",
            "order_book":{
                "nonce":10,
                "bids":[{"price":"99.5","size":"2"}],
                "asks":[["101.25","3"]]
            }
        }"#,
    )
    .unwrap()
    .unwrap();

    assert_eq!(message.kind, LighterBookMessageKind::Snapshot);
    assert_eq!(message.channel.as_deref(), Some("order_book:1"));
    assert_eq!(message.nonce, 10);
    assert_eq!(message.bids[0].price, dec!(99.5));
    assert_eq!(message.asks[0].size, dec!(3));
}

#[test]
fn snapshot_and_continuous_update_produce_ready_top_of_book() {
    let mut book = LighterLocalBook::new();
    let snapshot = parse_order_book_message(
        r#"{"type":"subscribed/order_book","order_book":{"nonce":10,"bids":[{"price":"99","size":"2"}],"asks":[{"price":"101","size":"3"}]}}"#,
    )
    .unwrap()
    .unwrap();
    let update = parse_order_book_message(
        r#"{"type":"update/order_book","order_book":{"begin_nonce":10,"nonce":11,"bids":[{"price":"100","size":"1"}],"asks":[{"price":"101","size":"0"},{"price":"102","size":"4"}]}}"#,
    )
    .unwrap()
    .unwrap();

    assert!(book.apply(&snapshot).unwrap().applied);
    let outcome = book.apply(&update).unwrap();

    assert!(outcome.applied);
    assert!(!outcome.requires_resubscribe);
    assert_eq!(book.nonce(), Some(11));
    assert_eq!(
        book.status(),
        &LighterBookStatus::Ready { current_nonce: 11 }
    );
    assert_eq!(
        book.top_of_book().unwrap(),
        bybot_lighter::local_book::LighterTopOfBook {
            bid_price: dec!(100),
            bid_size: dec!(1),
            ask_price: dec!(102),
            ask_size: dec!(4),
        }
    );
}

#[test]
fn duplicate_update_is_ignored_without_resubscribe() {
    let mut book = seeded_book();
    let duplicate = parse_order_book_message(
        r#"{"type":"update/order_book","order_book":{"begin_nonce":9,"nonce":10,"bids":[{"price":"100","size":"1"}],"asks":[]}}"#,
    )
    .unwrap()
    .unwrap();

    let outcome = book.apply(&duplicate).unwrap();

    assert!(!outcome.applied);
    assert!(!outcome.requires_resubscribe);
    assert_eq!(book.nonce(), Some(10));
}

#[test]
fn nonce_gap_clears_book_and_requires_resubscribe() {
    let mut book = seeded_book();
    let gap = parse_order_book_message(
        r#"{"type":"update/order_book","order_book":{"begin_nonce":12,"nonce":13,"bids":[],"asks":[]}}"#,
    )
    .unwrap()
    .unwrap();

    let outcome = book.apply(&gap).unwrap();

    assert!(!outcome.applied);
    assert!(outcome.requires_resubscribe);
    assert_eq!(book.nonce(), None);
    assert_eq!(book.top_of_book(), None);
    assert!(matches!(
        book.status(),
        LighterBookStatus::Resubscribing { reason } if reason.contains("nonce gap")
    ));
}

#[test]
fn crossed_snapshot_is_stale_until_valid_snapshot_arrives() {
    let mut book = LighterLocalBook::new();
    let crossed = parse_order_book_message(
        r#"{"type":"subscribed/order_book","order_book":{"nonce":20,"bids":[{"price":"102","size":"1"}],"asks":[{"price":"101","size":"1"}]}}"#,
    )
    .unwrap()
    .unwrap();

    let outcome = book.apply(&crossed).unwrap();

    assert!(outcome.applied);
    assert_eq!(book.status(), &LighterBookStatus::Stale);
    assert_eq!(book.top_of_book(), None);
}

#[test]
fn computes_bid_and_ask_vwap_for_quote_notional() {
    let mut book = LighterLocalBook::new();
    let snapshot = parse_order_book_message(
        r#"{"type":"subscribed/order_book","order_book":{"nonce":10,"bids":[{"price":"100","size":"10"},{"price":"99","size":"20"}],"asks":[{"price":"101","size":"10"},{"price":"102","size":"20"}]}}"#,
    )
    .unwrap()
    .unwrap();
    book.apply(&snapshot).unwrap();

    let bid = book
        .vwap_for_quote_notional(LighterDepthSide::Bid, dec!(1500))
        .unwrap();
    let ask = book
        .vwap_for_quote_notional(LighterDepthSide::Ask, dec!(1510))
        .unwrap();

    assert_eq!(bid, dec!(1500) / (dec!(10) + dec!(500) / dec!(99)));
    assert_eq!(ask, dec!(1510) / (dec!(10) + dec!(500) / dec!(102)));
}

#[test]
fn vwap_rejects_incomplete_depth() {
    let book = seeded_book();

    assert_eq!(
        book.vwap_for_quote_notional(LighterDepthSide::Bid, dec!(1000)),
        None
    );
    assert_eq!(
        book.vwap_for_quote_notional(LighterDepthSide::Ask, Decimal::ZERO),
        None
    );
}

fn seeded_book() -> LighterLocalBook {
    let mut book = LighterLocalBook::new();
    let snapshot = parse_order_book_message(
        r#"{"type":"subscribed/order_book","order_book":{"nonce":10,"bids":[{"price":"99","size":"2"}],"asks":[{"price":"101","size":"3"}]}}"#,
    )
    .unwrap()
    .unwrap();
    book.apply(&snapshot).unwrap();
    book
}
