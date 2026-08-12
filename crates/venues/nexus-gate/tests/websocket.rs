use nautilus_core::UnixNanos;
use nexus_gate::websocket::{
    messages::GateWsMessage,
    parse::{parse_gate_orderbook_deltas, parse_gate_orderbook_quote},
};
use nautilus_model::{
    data::QuoteTick,
    enums::{BookAction, OrderSide, RecordFlag},
    instruments::{Instrument, InstrumentAny, stubs::crypto_perpetual_ethusdt},
    types::{Price, Quantity},
};

const TS_INIT: UnixNanos = UnixNanos::new(1_700_000_000_000_000_000);

fn instrument() -> InstrumentAny {
    InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt())
}

fn message(json: &str) -> GateWsMessage {
    serde_json::from_str(json).unwrap()
}

#[test]
fn parse_full_snapshot_to_clear_and_add_deltas() {
    let instrument = instrument();
    let msg = message(
        r#"{"time":1700000000,"time_ms":1700000000123,"channel":"futures.obu","event":"update","result":{"full":true,"s":"ETH_USDT","t":1700000000123,"U":10,"u":10,"b":[["100.00","3.000"]],"a":[["100.10","2.000"]]}}"#,
    );

    let deltas = parse_gate_orderbook_deltas(&msg, &instrument, TS_INIT, None).unwrap();

    assert_eq!(deltas.instrument_id, instrument.id());
    assert_eq!(deltas.deltas.len(), 3);
    assert_eq!(deltas.deltas[0].action, BookAction::Clear);
    assert_eq!(deltas.deltas[1].action, BookAction::Add);
    assert_eq!(deltas.deltas[1].order.side, OrderSide::Buy);
    assert_eq!(deltas.deltas[1].order.price, Price::from("100.00"));
    assert_eq!(deltas.deltas[1].order.size, Quantity::from("3.000"));
    assert_eq!(deltas.deltas[2].action, BookAction::Add);
    assert_eq!(
        deltas.deltas[2].flags & RecordFlag::F_LAST as u8,
        RecordFlag::F_LAST as u8
    );
}

#[test]
fn parse_orderbook_deltas_can_limit_each_side_depth() {
    let instrument = instrument();
    let msg = message(
        r#"{"time":1700000000,"time_ms":1700000000123,"channel":"futures.obu","event":"update","result":{"full":true,"s":"ETH_USDT","t":1700000000123,"U":10,"u":10,"b":[["100.00","3.000"],["99.90","1.000"],["99.80","1.000"]],"a":[["100.10","2.000"],["100.20","1.000"],["100.30","1.000"]]}}"#,
    );

    let deltas = parse_gate_orderbook_deltas(&msg, &instrument, TS_INIT, Some(2)).unwrap();

    assert_eq!(deltas.deltas.len(), 5);
    assert_eq!(deltas.deltas[1].order.price, Price::from("100.00"));
    assert_eq!(deltas.deltas[2].order.price, Price::from("99.90"));
    assert_eq!(deltas.deltas[3].order.price, Price::from("100.10"));
    assert_eq!(deltas.deltas[4].order.price, Price::from("100.20"));
    assert_eq!(
        deltas.deltas[4].flags & RecordFlag::F_LAST as u8,
        RecordFlag::F_LAST as u8
    );
}

#[test]
fn parse_delta_update_to_update_delta() {
    let instrument = instrument();
    let msg = message(
        r#"{"time_ms":1700000000124,"channel":"futures.obu","event":"update","result":{"s":"ETH_USDT","t":1700000000124,"U":11,"u":11,"b":[["100.00","4.000"]],"a":[]}}"#,
    );

    let deltas = parse_gate_orderbook_deltas(&msg, &instrument, TS_INIT, None).unwrap();

    assert_eq!(deltas.deltas.len(), 1);
    assert_eq!(deltas.deltas[0].action, BookAction::Update);
    assert_eq!(deltas.deltas[0].order.side, OrderSide::Buy);
    assert_eq!(deltas.deltas[0].order.size, Quantity::from("4.000"));
}

#[test]
fn parse_zero_amount_to_delete_delta() {
    let instrument = instrument();
    let msg = message(
        r#"{"time_ms":1700000000125,"channel":"futures.obu","event":"update","result":{"s":"ETH_USDT","t":1700000000125,"U":12,"u":12,"b":[],"a":[["100.10","0"]]}}"#,
    );

    let deltas = parse_gate_orderbook_deltas(&msg, &instrument, TS_INIT, None).unwrap();

    assert_eq!(deltas.deltas.len(), 1);
    assert_eq!(deltas.deltas[0].action, BookAction::Delete);
    assert_eq!(deltas.deltas[0].order.side, OrderSide::Sell);
}

#[test]
fn repeated_full_snapshot_replaces_book_with_snapshot_deltas() {
    let instrument = instrument();
    let msg = message(
        r#"{"time_ms":1700000000222,"channel":"futures.obu","event":"update","result":{"full":true,"s":"ETH_USDT","t":1700000000222,"U":20,"u":20,"b":[["99.90","1.000"]],"a":[["100.20","1.500"]]}}"#,
    );

    let deltas = parse_gate_orderbook_deltas(&msg, &instrument, TS_INIT, None).unwrap();

    assert_eq!(deltas.deltas[0].action, BookAction::Clear);
    assert_eq!(deltas.deltas[1].action, BookAction::Add);
    assert_eq!(deltas.deltas[2].action, BookAction::Add);
}

#[test]
fn missing_orderbook_result_returns_error() {
    let instrument = instrument();
    let msg = message(r#"{"time_ms":1700000000123,"channel":"futures.obu","event":"update"}"#);

    let result = parse_gate_orderbook_deltas(&msg, &instrument, TS_INIT, None);

    assert!(result.is_err());
}

#[test]
fn quote_missing_one_side_uses_last_quote() {
    let instrument = instrument();
    let last = QuoteTick::new(
        instrument.id(),
        Price::from("100.00"),
        Price::from("100.10"),
        Quantity::from("3.000"),
        Quantity::from("2.000"),
        TS_INIT,
        TS_INIT,
    );
    let msg = message(
        r#"{"time_ms":1700000000124,"channel":"futures.obu","event":"update","result":{"s":"ETH_USDT","t":1700000000124,"U":11,"u":11,"b":[["100.01","4.000"]],"a":[]}}"#,
    );

    let quote = parse_gate_orderbook_quote(&msg, &instrument, Some(&last), TS_INIT).unwrap();

    assert_eq!(quote.bid_price, Price::from("100.01"));
    assert_eq!(quote.bid_size, Quantity::from("4.000"));
    assert_eq!(quote.ask_price, last.ask_price);
    assert_eq!(quote.ask_size, last.ask_size);
}

#[test]
fn quote_missing_side_without_last_quote_returns_error() {
    let instrument = instrument();
    let msg = message(
        r#"{"time_ms":1700000000124,"channel":"futures.obu","event":"update","result":{"s":"ETH_USDT","t":1700000000124,"U":11,"u":11,"b":[["100.01","4.000"]],"a":[]}}"#,
    );

    let result = parse_gate_orderbook_quote(&msg, &instrument, None, TS_INIT);

    assert!(result.is_err());
}
