use std::num::NonZeroUsize;

use nautilus_common::{
    clients::DataClient,
    live::runner::replace_data_event_sender,
    messages::DataEvent,
    messages::data::{
        SubscribeBookDeltas, SubscribeQuotes, UnsubscribeBookDeltas, UnsubscribeQuotes,
    },
};
use nautilus_core::{UUID4, UnixNanos};
use nexus_gate::{
    config::GateDataClientConfig,
    data::{GateDataClient, GateSubscriptionAction},
    http::models::GateFuturesContract,
    instrument::parse_gate_futures_contract,
    websocket::messages::GateWsMessage,
};
use nautilus_model::{
    data::Data,
    enums::BookType,
    identifiers::{ClientId, InstrumentId},
    types::{Price, Quantity},
};

const TS_INIT: UnixNanos = UnixNanos::new(1_700_000_000_000_000_000);

fn client() -> GateDataClient {
    GateDataClient::new(ClientId::from("GATE"), GateDataClientConfig::default()).unwrap()
}

fn subscribe_book(depth: Option<usize>, book_type: BookType) -> SubscribeBookDeltas {
    SubscribeBookDeltas::new(
        InstrumentId::from("ETH_USDT.GATE"),
        book_type,
        Some(ClientId::from("GATE")),
        None,
        UUID4::new(),
        TS_INIT,
        depth.and_then(NonZeroUsize::new),
        false,
        None,
        None,
    )
}

fn subscribe_quotes() -> SubscribeQuotes {
    SubscribeQuotes::new(
        InstrumentId::from("ETH_USDT.GATE"),
        Some(ClientId::from("GATE")),
        None,
        UUID4::new(),
        TS_INIT,
        None,
        None,
    )
}

fn message(json: &str) -> GateWsMessage {
    serde_json::from_str(json).unwrap()
}

fn btc_usdt_contract() -> GateFuturesContract {
    serde_json::from_str(
        r#"{"name":"BTC_USDT","quanto_multiplier":"0.0001","order_price_round":"0.1","order_size_min":1,"order_size_max":12000000,"enable_decimal":false,"status":"trading","maker_fee_rate":"-0.0001","taker_fee_rate":"0.00075"}"#,
    )
    .unwrap()
}

fn drain_events(rx: &mut tokio::sync::mpsc::UnboundedReceiver<DataEvent>) -> Vec<DataEvent> {
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    events
}

fn snapshot_with_levels(depth: usize) -> GateWsMessage {
    let bids = (0..depth)
        .map(|index| format!(r#"["{}","1.000"]"#, 1000 - index))
        .collect::<Vec<_>>()
        .join(",");
    let asks = (0..depth)
        .map(|index| format!(r#"["{}","1.000"]"#, 1001 + index))
        .collect::<Vec<_>>()
        .join(",");
    message(&format!(
        r#"{{"time":1700000000,"time_ms":1700000000123,"channel":"futures.obu","event":"update","result":{{"full":true,"s":"ob.ETH_USDT.50","t":1700000000123,"U":10,"u":10,"b":[{bids}],"a":[{asks}]}}}}"#
    ))
}

#[test]
fn subscribe_book_deltas_only_supports_l2_mbp() {
    let mut client = client();

    let result = client.subscribe_book_deltas(subscribe_book(Some(50), BookType::L1_MBP));

    assert!(result.is_err());
}

#[test]
fn start_does_not_mark_transport_connected() {
    let mut client = client();

    client.start().unwrap();

    assert!(client.is_disconnected());
}

#[test]
fn subscribe_book_deltas_only_allows_gate_depths() {
    let mut client = client();

    let result = client.subscribe_book_deltas(subscribe_book(Some(200), BookType::L2_MBP));

    assert!(result.is_err());
}

#[test]
fn subscribe_quotes_reuses_orderbook_stream() {
    let mut client = client();

    client
        .subscribe_book_deltas(subscribe_book(Some(50), BookType::L2_MBP))
        .unwrap();
    client.subscribe_quotes(subscribe_quotes()).unwrap();

    assert_eq!(client.planned_actions().len(), 1);
    assert_eq!(
        client.planned_actions()[0],
        GateSubscriptionAction::Subscribe("ob.ETH_USDT.50".to_string())
    );
}

#[test]
fn duplicate_book_subscription_does_not_repeat_stream_subscription() {
    let mut client = client();

    client
        .subscribe_book_deltas(subscribe_book(Some(50), BookType::L2_MBP))
        .unwrap();
    client
        .subscribe_book_deltas(subscribe_book(Some(50), BookType::L2_MBP))
        .unwrap();

    assert_eq!(client.planned_actions().len(), 1);
}

#[test]
fn gap_triggers_unsubscribe_and_resubscribe() {
    let mut client = client();
    let instrument_id = InstrumentId::from("ETH_USDT.GATE");
    client
        .subscribe_book_deltas(subscribe_book(Some(50), BookType::L2_MBP))
        .unwrap();
    client.set_local_last_id_for_test(instrument_id, 10);

    client.handle_sequence_for_test(instrument_id, 12, 12);

    assert_eq!(
        client.planned_actions(),
        &[
            GateSubscriptionAction::Subscribe("ob.ETH_USDT.50".to_string()),
            GateSubscriptionAction::Unsubscribe("ob.ETH_USDT.50".to_string()),
            GateSubscriptionAction::Subscribe("ob.ETH_USDT.50".to_string()),
        ]
    );
}

#[test]
fn unsubscribe_quotes_keeps_book_stream_when_book_subscription_exists() {
    let mut client = client();
    let cmd = UnsubscribeQuotes::new(
        InstrumentId::from("ETH_USDT.GATE"),
        Some(ClientId::from("GATE")),
        None,
        UUID4::new(),
        TS_INIT,
        None,
        None,
    );

    client
        .subscribe_book_deltas(subscribe_book(Some(50), BookType::L2_MBP))
        .unwrap();
    client.subscribe_quotes(subscribe_quotes()).unwrap();
    client.unsubscribe_quotes(&cmd).unwrap();

    assert_eq!(client.planned_actions().len(), 1);
}

#[test]
fn unsubscribe_book_deltas_keeps_quote_stream_when_quote_subscription_exists() {
    let mut client = client();
    let cmd = UnsubscribeBookDeltas::new(
        InstrumentId::from("ETH_USDT.GATE"),
        Some(ClientId::from("GATE")),
        None,
        UUID4::new(),
        TS_INIT,
        None,
        None,
    );

    client
        .subscribe_book_deltas(subscribe_book(Some(50), BookType::L2_MBP))
        .unwrap();
    client.subscribe_quotes(subscribe_quotes()).unwrap();
    client.unsubscribe_book_deltas(&cmd).unwrap();

    assert_eq!(client.planned_actions().len(), 1);
}

#[test]
fn handle_ws_message_emits_instrument_deltas_and_quote_for_subscribed_contract() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = client();
    let msg = message(
        r#"{"time":1700000000,"time_ms":1700000000123,"channel":"futures.obu","event":"update","result":{"full":true,"s":"ETH_USDT","t":1700000000123,"U":10,"u":10,"b":[["100.00","3.000"]],"a":[["100.10","2.000"]]}}"#,
    );

    client
        .subscribe_book_deltas(subscribe_book(Some(50), BookType::L2_MBP))
        .unwrap();
    client.subscribe_quotes(subscribe_quotes()).unwrap();
    client.handle_ws_message_for_test(&msg).unwrap();

    assert!(matches!(rx.try_recv().unwrap(), DataEvent::Instrument(_)));
    assert!(matches!(
        rx.try_recv().unwrap(),
        DataEvent::Data(Data::Deltas(_))
    ));
    assert!(matches!(
        rx.try_recv().unwrap(),
        DataEvent::Data(Data::Quote(_))
    ));
}

#[test]
fn handle_ws_message_accepts_gate_orderbook_stream_symbol() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = client();
    let msg = message(
        r#"{"time":1700000000,"time_ms":1700000000123,"channel":"futures.obu","event":"update","result":{"full":true,"s":"ob.ETH_USDT.50","t":1700000000123,"U":10,"u":10,"b":[["100.00","3.000"]],"a":[["100.10","2.000"]]}}"#,
    );

    client
        .subscribe_book_deltas(subscribe_book(Some(50), BookType::L2_MBP))
        .unwrap();
    client.handle_ws_message_for_test(&msg).unwrap();

    assert!(matches!(rx.try_recv().unwrap(), DataEvent::Instrument(_)));
    assert!(matches!(
        rx.try_recv().unwrap(),
        DataEvent::Data(Data::Deltas(_))
    ));
}

#[test]
fn handle_ws_message_uses_cached_contract_instrument_for_stream_symbol() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = client();
    let instrument = parse_gate_futures_contract(&btc_usdt_contract(), TS_INIT, TS_INIT).unwrap();
    let msg = message(
        r#"{"time":1700000000,"time_ms":1700000000123,"channel":"futures.obu","event":"update","result":{"full":true,"s":"ob.BTC_USDT.50","t":1700000000123,"U":10,"u":10,"b":[["75858.7","4544"]],"a":[["75858.8","120"]]}}"#,
    );

    client.add_instrument_for_test(instrument);
    client
        .subscribe_book_deltas(SubscribeBookDeltas::new(
            InstrumentId::from("BTC_USDT.GATE"),
            BookType::L2_MBP,
            Some(ClientId::from("GATE")),
            None,
            UUID4::new(),
            TS_INIT,
            NonZeroUsize::new(50),
            false,
            None,
            None,
        ))
        .unwrap();
    client.handle_ws_message_for_test(&msg).unwrap();

    assert!(matches!(
        rx.try_recv().unwrap(),
        DataEvent::Data(Data::Deltas(_))
    ));
}

#[test]
fn handle_ws_message_ignores_empty_orderbook_update() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = client();
    let msg = message(
        r#"{"time":1700000000,"time_ms":1700000000123,"channel":"futures.obu","event":"update","result":{"s":"ob.ETH_USDT.50","t":1700000000123,"U":10,"u":10,"b":[],"a":[]}}"#,
    );

    client
        .subscribe_book_deltas(subscribe_book(Some(50), BookType::L2_MBP))
        .unwrap();
    client.handle_ws_message_for_test(&msg).unwrap();

    assert!(matches!(rx.try_recv().unwrap(), DataEvent::Instrument(_)));
    assert!(rx.try_recv().is_err());
}

#[test]
fn full_snapshot_is_pruned_to_subscribed_depth() {
    let mut client = client();

    client
        .subscribe_book_deltas(subscribe_book(Some(50), BookType::L2_MBP))
        .unwrap();
    client
        .handle_ws_message_for_test(&snapshot_with_levels(55))
        .unwrap();

    assert_eq!(
        client.local_book_side_depths_for_test(InstrumentId::from("ETH_USDT.GATE")),
        Some((50, 50))
    );
    assert_eq!(
        client.best_bid_ask_for_test(InstrumentId::from("ETH_USDT.GATE")),
        Some((Price::from("1000"), Price::from("1001")))
    );
    assert_eq!(client.stats_for_test().snapshot_count, 1);
}

#[test]
fn delta_prunes_levels_beyond_subscribed_depth_and_quote_matches_book_top() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = client();
    let insert_inside_depth = message(
        r#"{"time":1700000000,"time_ms":1700000000124,"channel":"futures.obu","event":"update","result":{"s":"ob.ETH_USDT.50","t":1700000000124,"U":11,"u":11,"b":[["950.5","2.000"]],"a":[["1050.5","3.000"]]}}"#,
    );

    client
        .subscribe_book_deltas(subscribe_book(Some(50), BookType::L2_MBP))
        .unwrap();
    client.subscribe_quotes(subscribe_quotes()).unwrap();
    drain_events(&mut rx);
    client
        .handle_ws_message_for_test(&snapshot_with_levels(50))
        .unwrap();
    drain_events(&mut rx);
    client
        .handle_ws_message_for_test(&insert_inside_depth)
        .unwrap();

    assert_eq!(
        client.local_book_side_depths_for_test(InstrumentId::from("ETH_USDT.GATE")),
        Some((50, 50))
    );
    let quote = client
        .last_quote_for_test(InstrumentId::from("ETH_USDT.GATE"))
        .unwrap();
    let (bid, ask) = client
        .best_bid_ask_for_test(InstrumentId::from("ETH_USDT.GATE"))
        .unwrap();
    assert_eq!(quote.bid_price, bid);
    assert_eq!(quote.ask_price, ask);
    assert_eq!(quote.bid_size, Quantity::from("1.000"));
    assert_eq!(quote.ask_size, Quantity::from("1.000"));
    assert_eq!(client.stats_for_test().delta_count, 1);
    assert_eq!(client.stats_for_test().quote_count, 2);
}

#[test]
fn crossed_book_after_update_is_marked_not_ready_and_suppresses_quote() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = client();
    let snapshot = message(
        r#"{"time":1700000000,"time_ms":1700000000123,"channel":"futures.obu","event":"update","result":{"full":true,"s":"ob.ETH_USDT.50","t":1700000000123,"U":10,"u":10,"b":[["100.4","1.000"],["100.3","1.000"],["100.2","1.000"]],"a":[["100.5","1.000"],["100.6","1.000"],["100.7","1.000"]]}}"#,
    );
    let crossed = message(
        r#"{"time":1700000000,"time_ms":1700000000124,"channel":"futures.obu","event":"update","result":{"s":"ob.ETH_USDT.50","t":1700000000124,"U":11,"u":11,"b":[["100.1","2.000"]],"a":[["100.2","2.000"]]}}"#,
    );

    client
        .subscribe_book_deltas(subscribe_book(Some(50), BookType::L2_MBP))
        .unwrap();
    client.subscribe_quotes(subscribe_quotes()).unwrap();
    drain_events(&mut rx);
    client.handle_ws_message_for_test(&snapshot).unwrap();
    drain_events(&mut rx);
    client.handle_ws_message_for_test(&crossed).unwrap();

    assert!(!client.local_book_ready_for_test(InstrumentId::from("ETH_USDT.GATE")));
    assert!(
        client
            .last_quote_for_test(InstrumentId::from("ETH_USDT.GATE"))
            .is_none()
    );
    assert!(rx.try_recv().is_err());
}

#[test]
fn stats_report_current_invalid_book_and_quote_suppression() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = client();
    let snapshot = message(
        r#"{"time":1700000000,"time_ms":1700000000123,"channel":"futures.obu","event":"update","result":{"full":true,"s":"ob.ETH_USDT.50","t":1700000000123,"U":10,"u":10,"b":[["100.4","1.000"]],"a":[["100.5","1.000"]]}}"#,
    );
    let crossed = message(
        r#"{"time":1700000000,"time_ms":1700000000124,"channel":"futures.obu","event":"update","result":{"s":"ob.ETH_USDT.50","t":1700000000124,"U":11,"u":11,"b":[["100.6","1.000"]],"a":[]}}"#,
    );

    client
        .subscribe_book_deltas(subscribe_book(Some(50), BookType::L2_MBP))
        .unwrap();
    client.subscribe_quotes(subscribe_quotes()).unwrap();
    drain_events(&mut rx);
    client.handle_ws_message_for_test(&snapshot).unwrap();
    drain_events(&mut rx);
    client.handle_ws_message_for_test(&crossed).unwrap();

    let stats = client.stats_for_test();
    assert_eq!(stats.invalid_book_count, 1);
    assert_eq!(stats.quote_suppressed_count, 1);
    assert_eq!(stats.current_invalid_book_count, 0);
    assert_eq!(stats.resubscribe_count, 1);
    assert_eq!(stats.quote_count, 1);
    assert_eq!(
        client.planned_actions(),
        &[
            GateSubscriptionAction::Subscribe("ob.ETH_USDT.50".to_string()),
            GateSubscriptionAction::Unsubscribe("ob.ETH_USDT.50".to_string()),
            GateSubscriptionAction::Subscribe("ob.ETH_USDT.50".to_string()),
        ]
    );
    assert!(rx.try_recv().is_err());
}

#[test]
fn no_op_update_advances_sequence_without_book_or_quote_emission() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = client();
    let no_op = message(
        r#"{"time":1700000000,"time_ms":1700000000124,"channel":"futures.obu","event":"update","result":{"s":"ob.ETH_USDT.50","t":1700000000124,"U":11,"u":11,"b":[],"a":[]}}"#,
    );

    client
        .subscribe_book_deltas(subscribe_book(Some(50), BookType::L2_MBP))
        .unwrap();
    client.subscribe_quotes(subscribe_quotes()).unwrap();
    drain_events(&mut rx);
    client
        .handle_ws_message_for_test(&snapshot_with_levels(2))
        .unwrap();
    drain_events(&mut rx);
    client.handle_ws_message_for_test(&no_op).unwrap();

    assert_eq!(
        client.local_last_update_id_for_test(InstrumentId::from("ETH_USDT.GATE")),
        Some(11)
    );
    assert!(rx.try_recv().is_err());
    assert_eq!(client.stats_for_test().no_op_count, 1);
    assert_eq!(client.stats_for_test().quote_count, 1);
}

#[test]
fn non_snapshot_update_before_ready_does_not_emit_book_or_quote() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = client();
    let msg = message(
        r#"{"time":1700000000,"time_ms":1700000000123,"channel":"futures.obu","event":"update","result":{"s":"ob.ETH_USDT.50","t":1700000000123,"U":10,"u":10,"b":[["100.00","3.000"]],"a":[["100.10","2.000"]]}}"#,
    );

    client
        .subscribe_book_deltas(subscribe_book(Some(50), BookType::L2_MBP))
        .unwrap();
    client.subscribe_quotes(subscribe_quotes()).unwrap();
    drain_events(&mut rx);
    client.handle_ws_message_for_test(&msg).unwrap();

    assert!(rx.try_recv().is_err());
}

#[test]
fn gap_suppresses_updates_until_next_full_snapshot() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = client();
    let snapshot = message(
        r#"{"time":1700000000,"time_ms":1700000000123,"channel":"futures.obu","event":"update","result":{"full":true,"s":"ob.ETH_USDT.50","t":1700000000123,"U":10,"u":10,"b":[["100.00","3.000"]],"a":[["100.10","2.000"]]}}"#,
    );
    let gap = message(
        r#"{"time":1700000000,"time_ms":1700000000124,"channel":"futures.obu","event":"update","result":{"s":"ob.ETH_USDT.50","t":1700000000124,"U":12,"u":12,"b":[["100.01","4.000"]],"a":[]}}"#,
    );
    let stale_update = message(
        r#"{"time":1700000000,"time_ms":1700000000125,"channel":"futures.obu","event":"update","result":{"s":"ob.ETH_USDT.50","t":1700000000125,"U":13,"u":13,"b":[["100.02","5.000"]],"a":[]}}"#,
    );
    let recovery_snapshot = message(
        r#"{"time":1700000000,"time_ms":1700000000126,"channel":"futures.obu","event":"update","result":{"full":true,"s":"ob.ETH_USDT.50","t":1700000000126,"U":20,"u":20,"b":[["100.03","6.000"]],"a":[["100.13","7.000"]]}}"#,
    );

    client
        .subscribe_book_deltas(subscribe_book(Some(50), BookType::L2_MBP))
        .unwrap();
    client.subscribe_quotes(subscribe_quotes()).unwrap();
    drain_events(&mut rx);
    client.handle_ws_message_for_test(&snapshot).unwrap();
    drain_events(&mut rx);

    let gap_actions = client.handle_ws_message_actions_for_test(&gap).unwrap();
    client.handle_ws_message_for_test(&stale_update).unwrap();

    assert!(rx.try_recv().is_err());
    assert_eq!(
        gap_actions,
        &[
            GateSubscriptionAction::Unsubscribe("ob.ETH_USDT.50".to_string()),
            GateSubscriptionAction::Subscribe("ob.ETH_USDT.50".to_string()),
        ]
    );
    assert_eq!(
        client.planned_actions(),
        &[
            GateSubscriptionAction::Subscribe("ob.ETH_USDT.50".to_string()),
            GateSubscriptionAction::Unsubscribe("ob.ETH_USDT.50".to_string()),
            GateSubscriptionAction::Subscribe("ob.ETH_USDT.50".to_string()),
        ]
    );

    client
        .handle_ws_message_for_test(&recovery_snapshot)
        .unwrap();

    assert!(matches!(
        rx.try_recv().unwrap(),
        DataEvent::Data(Data::Deltas(_))
    ));
    assert!(matches!(
        rx.try_recv().unwrap(),
        DataEvent::Data(Data::Quote(_))
    ));
}

#[test]
fn duplicate_or_old_update_after_ready_is_ignored_without_resubscribe() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = client();
    let snapshot = message(
        r#"{"time":1700000000,"time_ms":1700000000123,"channel":"futures.obu","event":"update","result":{"full":true,"s":"ob.ETH_USDT.50","t":1700000000123,"U":10,"u":10,"b":[["100.00","3.000"]],"a":[["100.10","2.000"]]}}"#,
    );
    let duplicate = message(
        r#"{"time":1700000000,"time_ms":1700000000124,"channel":"futures.obu","event":"update","result":{"s":"ob.ETH_USDT.50","t":1700000000124,"U":10,"u":10,"b":[["100.01","4.000"]],"a":[]}}"#,
    );
    let old = message(
        r#"{"time":1700000000,"time_ms":1700000000125,"channel":"futures.obu","event":"update","result":{"s":"ob.ETH_USDT.50","t":1700000000125,"U":9,"u":9,"b":[["100.02","5.000"]],"a":[]}}"#,
    );

    client
        .subscribe_book_deltas(subscribe_book(Some(50), BookType::L2_MBP))
        .unwrap();
    drain_events(&mut rx);
    client.handle_ws_message_for_test(&snapshot).unwrap();
    drain_events(&mut rx);

    client.handle_ws_message_for_test(&duplicate).unwrap();
    client.handle_ws_message_for_test(&old).unwrap();

    assert!(rx.try_recv().is_err());
    assert_eq!(
        client.planned_actions(),
        &[GateSubscriptionAction::Subscribe(
            "ob.ETH_USDT.50".to_string()
        )]
    );
    assert_eq!(client.stats_for_test().duplicate_or_old_count, 2);
}

#[test]
fn reconnect_replays_subscription_and_waits_for_full_snapshot() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = client();
    let snapshot = message(
        r#"{"time":1700000000,"time_ms":1700000000123,"channel":"futures.obu","event":"update","result":{"full":true,"s":"ob.ETH_USDT.50","t":1700000000123,"U":10,"u":10,"b":[["100.00","3.000"]],"a":[["100.10","2.000"]]}}"#,
    );
    let post_reconnect_delta = message(
        r#"{"time":1700000000,"time_ms":1700000000124,"channel":"futures.obu","event":"update","result":{"s":"ob.ETH_USDT.50","t":1700000000124,"U":11,"u":11,"b":[["100.01","4.000"]],"a":[]}}"#,
    );
    let post_reconnect_snapshot = message(
        r#"{"time":1700000000,"time_ms":1700000000125,"channel":"futures.obu","event":"update","result":{"full":true,"s":"ob.ETH_USDT.50","t":1700000000125,"U":20,"u":20,"b":[["100.02","5.000"]],"a":[["100.12","6.000"]]}}"#,
    );

    client
        .subscribe_book_deltas(subscribe_book(Some(50), BookType::L2_MBP))
        .unwrap();
    client.subscribe_quotes(subscribe_quotes()).unwrap();
    drain_events(&mut rx);
    client.handle_ws_message_for_test(&snapshot).unwrap();
    drain_events(&mut rx);

    client.handle_reconnected_for_test();
    client
        .handle_ws_message_for_test(&post_reconnect_delta)
        .unwrap();

    assert!(rx.try_recv().is_err());
    assert_eq!(
        client.planned_actions(),
        &[
            GateSubscriptionAction::Subscribe("ob.ETH_USDT.50".to_string()),
            GateSubscriptionAction::Subscribe("ob.ETH_USDT.50".to_string()),
        ]
    );

    client
        .handle_ws_message_for_test(&post_reconnect_snapshot)
        .unwrap();

    assert!(matches!(
        rx.try_recv().unwrap(),
        DataEvent::Data(Data::Deltas(_))
    ));
    assert!(matches!(
        rx.try_recv().unwrap(),
        DataEvent::Data(Data::Quote(_))
    ));
    assert_eq!(client.stats_for_test().reconnect_count, 1);
    assert_eq!(client.stats_for_test().resubscribe_count, 1);
}

#[test]
fn gap_counts_resubscribe_and_stale_recovery_duration() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = client();
    let snapshot = message(
        r#"{"time":1700000000,"time_ms":1700000000123,"channel":"futures.obu","event":"update","result":{"full":true,"s":"ob.ETH_USDT.50","t":1700000000123,"U":10,"u":10,"b":[["100.00","3.000"]],"a":[["100.10","2.000"]]}}"#,
    );
    let gap = message(
        r#"{"time":1700000000,"time_ms":1700000000124,"channel":"futures.obu","event":"update","result":{"s":"ob.ETH_USDT.50","t":1700000000124,"U":12,"u":12,"b":[["100.01","4.000"]],"a":[]}}"#,
    );
    let recovery_snapshot = message(
        r#"{"time":1700000000,"time_ms":1700000001124,"channel":"futures.obu","event":"update","result":{"full":true,"s":"ob.ETH_USDT.50","t":1700000001124,"U":20,"u":20,"b":[["100.02","5.000"]],"a":[["100.12","6.000"]]}}"#,
    );

    client
        .subscribe_book_deltas(subscribe_book(Some(50), BookType::L2_MBP))
        .unwrap();
    client.subscribe_quotes(subscribe_quotes()).unwrap();
    drain_events(&mut rx);
    client.handle_ws_message_for_test(&snapshot).unwrap();
    drain_events(&mut rx);
    client.handle_ws_message_for_test(&gap).unwrap();
    client
        .handle_ws_message_for_test(&recovery_snapshot)
        .unwrap();

    let stats = client.stats_for_test();
    assert_eq!(stats.gap_count, 1);
    assert_eq!(stats.resubscribe_count, 1);
    assert_eq!(stats.snapshot_count, 2);
    assert!(stats.max_stale_duration_ms <= 1_000);
}
