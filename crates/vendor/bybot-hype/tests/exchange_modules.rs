use std::time::Duration;

use bybot_hype::{
    execution::RecoveryPlan,
    latency::{LatencyStep, LatencyTrace},
    order_state::{LifecycleEvent, OrderLifecycle},
    orders::{OrderIntent, OrderSide},
    precision::MarketPrecision,
    user_stream::UserStreamConfig,
};
use rust_decimal::Decimal;
use tokio::time::Instant;

#[test]
fn test_market_precision_computes_safe_minimum_size() {
    let precision = MarketPrecision::new(2).unwrap();

    let size = precision
        .minimum_size(Decimal::new(1289, 1), Decimal::from(11_i64))
        .unwrap();

    assert_eq!(precision.size_step(), Decimal::new(1, 2));
    assert_eq!(size, Decimal::new(9, 2));
}

#[test]
fn test_user_stream_config_requires_market_symbols() {
    assert!(UserStreamConfig::new(Vec::<String>::new(), Vec::new()).is_err());
    assert!(UserStreamConfig::new(["BTC", "xyz:SPCX"], [Some("xyz".to_string())]).is_ok());
}

#[test]
fn test_recovery_plan_uses_actual_position_direction() {
    let long = RecoveryPlan::from_position(Decimal::new(9, 2)).unwrap();
    let short = RecoveryPlan::from_position(Decimal::new(-9, 2)).unwrap();

    assert_eq!(long.side(), OrderSide::Sell);
    assert_eq!(long.size(), Decimal::new(9, 2));
    assert_eq!(short.side(), OrderSide::Buy);
    assert_eq!(short.size(), Decimal::new(9, 2));
    assert!(RecoveryPlan::from_position(Decimal::ZERO).is_none());
}

#[test]
fn test_order_lifecycle_rejects_terminal_regression() {
    let mut lifecycle = OrderLifecycle::new();

    lifecycle.apply(LifecycleEvent::Sent).unwrap();
    lifecycle.apply(LifecycleEvent::Open).unwrap();
    lifecycle.apply(LifecycleEvent::Canceled).unwrap();

    assert!(lifecycle.apply(LifecycleEvent::Open).is_err());
    assert!(lifecycle.is_terminal());
}

#[test]
fn test_order_intent_builders_preserve_execution_semantics() {
    let maker = OrderIntent::limit_maker(
        "BTC",
        0,
        OrderSide::Buy,
        Decimal::from(64_000_i64),
        Decimal::new(18, 5),
    );
    let close = OrderIntent::aggressive_ioc(
        "xyz:SPCX",
        110_076,
        OrderSide::Sell,
        Decimal::new(12759, 2),
        Decimal::new(9, 2),
        true,
    );

    assert!(maker.is_maker_only());
    assert!(!maker.reduce_only());
    assert!(close.is_immediate_or_cancel());
    assert!(close.reduce_only());
}

#[test]
fn test_latency_trace_measures_each_confirmation_path() {
    let started = Instant::now();
    let mut trace = LatencyTrace::new(started);

    trace.record(
        LatencyStep::TransportAck,
        started + Duration::from_millis(500),
    );
    trace.record(
        LatencyStep::OrderConfirmed,
        started + Duration::from_millis(480),
    );

    assert_eq!(
        trace.elapsed(LatencyStep::TransportAck),
        Some(Duration::from_millis(500))
    );
    assert_eq!(
        trace.elapsed(LatencyStep::OrderConfirmed),
        Some(Duration::from_millis(480))
    );
}
