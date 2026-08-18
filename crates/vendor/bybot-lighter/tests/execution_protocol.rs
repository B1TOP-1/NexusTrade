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
        market_id: Some(1),
        bid_client_index: Some(101),
        ask_client_index: None,
        side: None,
        bid_account_index: None,
        ask_account_index: None,
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
        market_id: Some(1),
        bid_client_index: Some(900),
        ask_client_index: Some(202),
        side: None,
        bid_account_index: None,
        ask_account_index: None,
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
fn parses_position_and_funding_private_events() {
    let messages = parse_lighter_private_ws_messages(
        r#"{
            "type":"update/account_all_positions",
            "positions":[{
                "market_id":1,
                "sign":-1,
                "position":"0.25",
                "avg_entry_price":"50000",
                "unrealized_pnl":"12.5",
                "return_on_equity":"0.01",
                "liquidation_price":"42000"
            }],
            "funding_histories":[{
                "timestamp":1700000000,
                "market_id":1,
                "funding_id":42,
                "change":"-0.1",
                "rate":"0.0001",
                "position_size":"0.25",
                "position_side":"short"
            }]
        }"#,
    )
    .unwrap();

    assert!(matches!(
        &messages[0],
        LighterPrivateWsMessage::PositionUpdate(positions)
            if positions[0].signed_quantity.to_string() == "-0.25"
                && positions[0].unrealized_pnl.as_ref().is_some_and(|pnl| pnl.to_string() == "12.5")
    ));
    assert!(messages.iter().any(|message| matches!(
        message,
        LighterPrivateWsMessage::Funding(funding)
            if funding[0].funding_id == 42 && funding[0].discount.is_zero()
    )));
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

#[test]
fn sendtx_ack_second_epoch_normalizes_to_millis() {
    // Lighter sendTx ack `timestamp` 为秒级；必须升尺度为毫秒，不得当 ms 直用。
    let ack = parse_lighter_submit_ack(
        r#"{"code":200,"data":{"tx_hash":"0xabc"},"timestamp":1781078425}"#,
        "order-2".to_owned(),
        Some(102),
    )
    .unwrap();
    assert_eq!(ack.ts_event_ms, 1_781_078_425_000);

    // 已是毫秒级的值必须原样通过。
    let ack_ms = parse_lighter_submit_ack(
        r#"{"code":200,"data":{"tx_hash":"0xdef"},"timestamp":1781078425283}"#,
        "order-3".to_owned(),
        Some(103),
    )
    .unwrap();
    assert_eq!(ack_ms.ts_event_ms, 1_781_078_425_283);
}

#[test]
fn attributed_pre_ack_trade_is_not_later_reported_as_external() {
    let trade = LighterPrivateTradeEvent {
        trade_id: "trade-two-sided".to_owned(),
        market_id: Some(1),
        bid_client_index: Some(303),
        ask_client_index: Some(404),
        side: None,
        bid_account_index: None,
        ask_account_index: None,
        size: "0.1000".to_owned(),
        price: Some("50000.0".to_owned()),
        fee: 0,
        ts_event_ms: 1_700_000_000_100,
    };
    let mut reducer = LighterExecutionReducer::new(10);
    assert!(reducer.on_trade_event(trade).is_empty());

    let effects = reducer.on_submit_ack(LighterSubmitAck {
        client_order_id: "our-bid".to_owned(),
        client_order_index: Some(303),
        tx_hash: "0x123".to_owned(),
        ts_event_ms: 1_700_000_000_101,
    });
    assert!(effects.iter().any(|effect| matches!(
        effect,
        LighterExecutionEffect::Fill {
            trade_id: Some(trade_id),
            ..
        } if trade_id == "trade-two-sided"
    )));
    assert!(reducer
        .flush_expired_external_trades(1_700_000_000_200)
        .is_empty());
}
