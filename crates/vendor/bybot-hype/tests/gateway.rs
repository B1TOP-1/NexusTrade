use bybot_hype::{
    gateway::{build_ioc_intent, parse_client_order_id, GatewaySubmission},
    orders::OrderSide,
};
use hypersdk::hypercore::types::OrderResponseStatus;
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    value.parse().unwrap()
}

#[test]
fn signed_quantity_builds_the_expected_ioc_intent() {
    let buy = build_ioc_intent("BTC", 0, decimal("0.2"), decimal("118000"), false).unwrap();
    assert_eq!(buy.side(), OrderSide::Buy);
    assert_eq!(buy.size(), decimal("0.2"));
    assert!(buy.is_immediate_or_cancel());

    let sell = build_ioc_intent("BTC", 0, decimal("-0.3"), decimal("118100"), true).unwrap();
    assert_eq!(sell.side(), OrderSide::Sell);
    assert_eq!(sell.size(), decimal("0.3"));
    assert!(sell.reduce_only());
}

#[test]
fn invalid_quantity_price_and_client_order_id_are_rejected() {
    assert!(build_ioc_intent("BTC", 0, Decimal::ZERO, decimal("118000"), false).is_err());
    assert!(build_ioc_intent("BTC", 0, decimal("0.2"), Decimal::ZERO, false).is_err());
    assert!(parse_client_order_id("not-a-hype-cloid").is_err());
}

#[test]
fn exchange_response_is_normalized_without_losing_order_identity() {
    let client_order_id = "0x0000000000000000000000000000002a";
    let cloid = parse_client_order_id(client_order_id).unwrap();
    let resting = GatewaySubmission::from_response(
        cloid,
        &[OrderResponseStatus::Resting {
            oid: 42,
            cloid: Some(cloid),
        }],
    )
    .unwrap();
    assert_eq!(resting.client_order_id, client_order_id);
    assert_eq!(resting.exchange_order_id.as_deref(), Some("42"));
    assert!(!resting.filled);

    let filled = GatewaySubmission::from_response(
        cloid,
        &[OrderResponseStatus::Filled {
            total_sz: decimal("0.2"),
            avg_px: decimal("118000"),
            oid: 43,
        }],
    )
    .unwrap();
    assert_eq!(filled.exchange_order_id.as_deref(), Some("43"));
    assert!(filled.filled);

    assert!(GatewaySubmission::from_response(
        cloid,
        &[OrderResponseStatus::Error(
            "insufficient margin".to_string()
        )],
    )
    .is_err());
}
