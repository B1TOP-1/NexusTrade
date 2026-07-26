use std::str::FromStr;

use bybot_hype::{
    gateway::{normalize_user_stream_event, GatewayOrderStatus, GatewayPrivateEvent, HypeGateway},
    user_stream::UserStreamEvent,
};
use hypersdk::hypercore::{
    types::{Fill, FillDirection, OrderStatus, OrderUpdate, Side, WsBasicOrder},
    Cloid,
};
use rust_decimal::Decimal;
use tokio::time::Instant;

fn assert_private_stream_contract(gateway: &HypeGateway, symbols: &[String]) {
    let runtime = gateway.spawn_user_stream(symbols).unwrap();
    let _: tokio::sync::broadcast::Receiver<UserStreamEvent> = runtime.subscribe();
}

fn cloid() -> Cloid {
    Cloid::from_str("0x0000000000000000000000000000002a").unwrap()
}

#[test]
fn normalizes_private_order_without_leaking_sdk_types() {
    let event = UserStreamEvent::Order {
        at: Instant::now(),
        update: OrderUpdate {
            status: OrderStatus::Open,
            status_timestamp: 1_700_000_000_000,
            order: WsBasicOrder {
                timestamp: 1_700_000_000_000,
                coin: "BTC".to_owned(),
                side: Side::Bid,
                limit_px: Decimal::new(70_000, 0),
                sz: Decimal::new(1, 1),
                oid: 42,
                orig_sz: Decimal::new(2, 1),
                cloid: Some(cloid()),
            },
        },
    };

    let normalized = normalize_user_stream_event(&event).unwrap();
    assert!(matches!(
        normalized,
        GatewayPrivateEvent::Order {
            status: GatewayOrderStatus::PartiallyFilled,
            exchange_order_id,
            ..
        } if exchange_order_id == "42"
    ));
}

#[test]
fn normalizes_private_fill_with_stable_trade_identity() {
    let event = UserStreamEvent::Fill {
        at: Instant::now(),
        fill: Fill {
            coin: "BTC".to_owned(),
            px: Decimal::new(70_000, 0),
            sz: Decimal::new(1, 1),
            side: Side::Bid,
            time: 1_700_000_000_100,
            start_position: Decimal::ZERO,
            dir: FillDirection::Buy,
            closed_pnl: Decimal::ZERO,
            hash: "0xabc".to_owned(),
            oid: 42,
            crossed: true,
            fee: Decimal::new(7, 3),
            tid: 99,
            cloid: Some(cloid()),
            fee_token: "USDC".to_owned(),
            builder_fee: None,
            liquidation: None,
        },
    };

    let normalized = normalize_user_stream_event(&event).unwrap();
    assert!(matches!(
        normalized,
        GatewayPrivateEvent::Fill { trade_id, .. } if trade_id == "99"
    ));
}

#[test]
fn private_stream_contract_is_project_owned() {
    let _: fn(&HypeGateway, &[String]) = assert_private_stream_contract;
}
