use bybot_lighter::execution::{
    parse_lighter_private_ws_messages, parse_lighter_sendtx_ack_envelope, parse_lighter_submit_ack,
    LighterExecutionEffect, LighterExecutionReducer, LighterPrivateTradeEvent,
    LighterPrivateWsMessage, LighterSubmitAck, LighterTerminalClassification,
    LighterTerminalReason,
};

#[test]
fn parses_sendtx_and_replays_pre_ack_trade_once() {
    let ack = parse_lighter_sendtx_ack_envelope(
        r#"{"type":"jsonapi/sendtx","code":200,"data":{"tx_hash":"0xabc","timestamp":1700000000000}}"#,
    )
    .unwrap();
    assert_eq!(ack.tx_hash.as_deref(), Some("0xabc"));

    let trade = LighterPrivateTradeEvent {
        trade_id: "trade-1".to_owned(),
        bid_client_index: Some(101),
        ask_client_index: None,
        size: "0.1000".to_owned(),
        price: Some("50000.0".to_owned()),
        fee: 12,
        ts_event_ms: 1_700_000_000_100,
    };
    let mut reducer = LighterExecutionReducer::new(10);
    assert!(reducer.on_trade_event(trade.clone()).is_empty());
    let effects = reducer.on_submit_ack(LighterSubmitAck {
        client_order_id: "plan-leg-left".to_owned(),
        client_order_index: Some(101),
        tx_hash: "0xabc".to_owned(),
        ts_event_ms: 1_700_000_000_000,
    });
    assert!(effects.iter().any(|effect| matches!(
        effect,
        LighterExecutionEffect::Fill {
            trade_id: Some(trade_id),
            ..
        } if trade_id == "trade-1"
    )));
    assert!(reducer.on_trade_event(trade).is_empty());
}

#[test]
fn pre_ack_trade_replays_when_our_order_is_the_ask_side() {
    let trade = LighterPrivateTradeEvent {
        trade_id: "trade-ask".to_owned(),
        bid_client_index: Some(900),
        ask_client_index: Some(202),
        size: "0.2000".to_owned(),
        price: Some("50001.0".to_owned()),
        fee: 13,
        ts_event_ms: 1_700_000_000_100,
    };
    let mut reducer = LighterExecutionReducer::new(10);

    assert!(reducer.on_trade_event(trade).is_empty());
    let effects = reducer.on_submit_ack(LighterSubmitAck {
        client_order_id: "plan-leg-ask".to_owned(),
        client_order_index: Some(202),
        tx_hash: "0xdef".to_owned(),
        ts_event_ms: 1_700_000_000_000,
    });

    assert!(effects.iter().any(|effect| matches!(
        effect,
        LighterExecutionEffect::Fill {
            trade_id: Some(trade_id),
            ..
        } if trade_id == "trade-ask"
    )));
}

#[test]
fn parses_private_order_and_normalizes_microsecond_timestamp() {
    let messages = parse_lighter_private_ws_messages(
        r#"{
            "channel":"account_orders",
            "data":{
                "client_order_index":101,
                "order_index":9001,
                "status":"open",
                "filled_base_amount":0,
                "transaction_time":1781078425283429
            }
        }"#,
    )
    .unwrap();

    assert!(matches!(
        &messages[0],
        LighterPrivateWsMessage::Order(event)
            if event.client_order_index == Some(101)
                && event.order_index == Some(9001)
                && event.ts_event_ms == 1_781_078_425_283
    ));
}

#[test]
fn sendtx_error_and_terminal_reason_remain_machine_readable() {
    let error = parse_lighter_submit_ack(
        r#"{"code":429,"message":"nonce already used"}"#,
        "order-1".to_owned(),
        Some(101),
    )
    .unwrap_err();
    assert_eq!(error.code, 429);

    let reason = LighterTerminalReason::from_raw("canceled-nonce-already-used");
    assert_eq!(reason.normalized, "LIGHTER_NONCE_ALREADY_USED");
    assert_eq!(
        reason.classification,
        LighterTerminalClassification::ExchangeReject
    );
}
