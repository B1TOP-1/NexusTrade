use bybot_lighter::{
    data::parse_lighter_market_specs,
    execution_client::{
        LighterExecutionClient, LighterExecutionConfig, LighterOrderRequest, LighterOrderType,
        LighterTimeInForce,
    },
};
use rust_decimal_macros::dec;

const TEST_PRIVATE_KEY: &str =
    "0xc89d22df8df76acee9f31bd35bdc15afde6324378e760ba8d4feaa233c6292318ad4849dc4285a50";

#[test]
fn parses_active_perpetual_market_specs_without_framework_types() {
    let specs = parse_lighter_market_specs(
        r#"{"code":200,"order_books":[
            {"symbol":"BTC","market_id":1,"market_type":"perp","status":"active","min_base_amount":"0.0001","supported_size_decimals":4,"supported_price_decimals":1},
            {"symbol":"ETH/USDC","market_id":2048,"market_type":"spot","status":"active","min_base_amount":"0.005","supported_size_decimals":4,"supported_price_decimals":2}
        ]}"#,
    )
    .unwrap();

    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].symbol, "BTC");
    assert_eq!(specs[0].market_id, 1);
    assert_eq!(specs[0].size_multiplier, 10_000);
    assert_eq!(specs[0].price_multiplier, 10);
}

#[test]
fn prepares_sell_ioc_with_scaled_values_and_monotonic_nonce() {
    let client = client();
    client.seed_nonce(77);
    let request = LighterOrderRequest {
        symbol: "btc".to_string(),
        client_order_id: "plan-left".to_string(),
        client_order_index: 101,
        signed_quantity: dec!(-0.0012),
        limit_price: Some(dec!(61150.79)),
        order_type: LighterOrderType::Limit,
        time_in_force: LighterTimeInForce::ImmediateOrCancel,
        reduce_only: false,
    };

    let first = client.prepare_order(&request).unwrap();
    let second = client
        .prepare_order(&LighterOrderRequest {
            client_order_id: "plan-right".to_string(),
            client_order_index: 102,
            ..request
        })
        .unwrap();

    assert_eq!(first.nonce, 77);
    assert_eq!(second.nonce, 78);
    assert_eq!(first.client_order_index, 101);
    assert!(first.signed_tx.tx_info.contains(r#""BaseAmount":12"#));
    assert!(first.signed_tx.tx_info.contains(r#""Price":611507"#));
    assert!(first.signed_tx.tx_info.contains(r#""IsAsk":1"#));
    assert!(first.signed_tx.tx_info.contains(r#""TimeInForce":0"#));
    assert!(first.signed_tx.tx_info.contains(r#""OrderExpiry":0"#));
}

#[test]
fn rejects_unseeded_nonce_unknown_symbol_and_zero_quantity() {
    let client = client();
    let mut request = order_request();
    assert!(client
        .prepare_order(&request)
        .unwrap_err()
        .to_string()
        .contains("nonce"));

    client.seed_nonce(1);
    request.symbol = "UNKNOWN".to_string();
    assert!(client.prepare_order(&request).is_err());
    request.symbol = "BTC".to_string();
    request.signed_quantity = dec!(0);
    assert!(client.prepare_order(&request).is_err());
}

#[test]
fn config_and_client_debug_never_expose_private_key() {
    let config = config();
    let client =
        LighterExecutionClient::new(config.clone(), TEST_PRIVATE_KEY, market_specs()).unwrap();

    assert!(!format!("{config:?}").contains(TEST_PRIVATE_KEY));
    assert!(!format!("{client:?}").contains(TEST_PRIVATE_KEY));
}

#[test]
fn account_channels_use_auth_only_when_required() {
    let client = client();
    assert_eq!(
        client.private_channels(),
        vec!["account_all_orders/42".to_string()]
    );
    assert_eq!(
        client.public_account_channels(),
        vec![
            "account_all_trades/42".to_string(),
            "account_all_positions/42".to_string(),
            "user_stats/42".to_string(),
        ]
    );
}

#[test]
fn private_websocket_rejection_is_not_silently_ignored() {
    let error = client()
        .ingest_private_ws_text(r#"{"type":"error/auth","code":"401","message":"invalid auth"}"#)
        .unwrap_err();

    assert!(error.to_string().contains("invalid auth"));
}

#[test]
fn nested_websocket_rejection_is_not_silently_ignored() {
    let error = client()
        .ingest_private_ws_text(r#"{"error":{"code":20001,"message":"auth field is required"}}"#)
        .unwrap_err();

    assert!(error.to_string().contains("auth field is required"));
}

#[test]
fn restores_active_order_indices_from_account_websocket() {
    let client = client();
    client
        .ingest_private_ws_text(
            r#"{"type":"update/account_all_orders","orders":{"1":[
                {"client_order_index":101,"order_index":9001,"status":"open"},
                {"client_order_index":"102","order_index":"9002","status":"open"}
            ]}}"#,
        )
        .unwrap();

    assert_eq!(client.venue_order_index(101), Some(9001));
    assert_eq!(client.venue_order_index(102), Some(9002));
}

#[tokio::test]
async fn account_snapshot_is_built_from_private_websocket_frames() {
    let client = client();
    for payload in [
        r#"{"type":"subscribed/account_all_orders","orders":{}}"#,
        r#"{"type":"subscribed/account_all_trades","trades":[]}"#,
        r#"{"type":"subscribed/account_all_positions","positions":{"1":{"market_id":1,"sign":-1,"position":"0.25","avg_entry_price":"61150.7"}}}"#,
        r#"{"type":"subscribed/user_stats","stats":{"collateral":"1000.5","available_balance":"750.25"}}"#,
    ] {
        client.ingest_private_ws_text(payload).unwrap();
    }

    client.wait_account_snapshot().await.unwrap();
    assert!(client.account_snapshot_ready());
    let snapshot = client.account_snapshot().await.unwrap();
    assert_eq!(snapshot.collateral, dec!(1000.5));
    assert_eq!(snapshot.available_balance, dec!(750.25));
    assert_eq!(snapshot.positions[0].signed_quantity, dec!(-0.25));

    client
        .ingest_private_ws_text(r#"{"type":"update/account_all_positions","positions":{}}"#)
        .unwrap();
    assert_eq!(
        client
            .position("BTC")
            .await
            .unwrap()
            .unwrap()
            .signed_quantity,
        dec!(-0.25)
    );

    client
        .ingest_private_ws_text(
            r#"{"type":"update/account_all_positions","positions":{"1":{"market_id":1,"sign":1,"position":"0.125","avg_entry_price":"62000"}}}"#,
        )
        .unwrap();
    let updated = client.position("BTC").await.unwrap().unwrap();
    assert_eq!(updated.signed_quantity, dec!(0.125));
    assert_eq!(updated.average_price, dec!(62000));
}

#[tokio::test]
async fn account_snapshot_parses_the_live_lighter_position_shape() {
    let client = client();
    for payload in [
        r#"{"type":"subscribed/account_all_orders","orders":{}}"#,
        r#"{"type":"subscribed/account_all_trades","trades":[]}"#,
        r#"{"type":"subscribed/account_all_positions","positions":{"0":{"market_id":0,"symbol":"ETH","sign":1,"position":"0.0000","avg_entry_price":"0.00"},"1":{"market_id":1,"symbol":"BTC","sign":1,"position":"0.01100","avg_entry_price":"65996.5"}}}"#,
        r#"{"type":"subscribed/user_stats","stats":{"collateral":"90.378147","available_balance":"63.826106"}}"#,
    ] {
        client.ingest_private_ws_text(payload).unwrap();
    }

    client.wait_account_snapshot().await.unwrap();
    let position = client.position("BTC").await.unwrap().unwrap();
    assert_eq!(position.signed_quantity, dec!(0.01100));
    assert_eq!(position.average_price, dec!(65996.5));
}

#[tokio::test]
async fn submit_is_blocked_until_account_websocket_snapshot_is_ready() {
    let client = client();
    client.seed_nonce(1);

    let error = client.submit_order(&order_request()).await.unwrap_err();

    assert!(error
        .to_string()
        .contains("WebSocket snapshot is not ready"));
}

fn config() -> LighterExecutionConfig {
    LighterExecutionConfig::new(
        "https://mainnet.zklighter.elliot.ai",
        "wss://mainnet.zklighter.elliot.ai/stream",
        42,
        1,
        304,
    )
    .unwrap()
}

fn market_specs() -> Vec<bybot_lighter::data::LighterMarketSpec> {
    parse_lighter_market_specs(
        r#"{"order_books":[{"symbol":"BTC","market_id":1,"market_type":"perp","status":"active","min_base_amount":"0.0001","supported_size_decimals":4,"supported_price_decimals":1}]}"#,
    )
    .unwrap()
}

fn client() -> LighterExecutionClient {
    LighterExecutionClient::new(config(), TEST_PRIVATE_KEY, market_specs()).unwrap()
}

fn order_request() -> LighterOrderRequest {
    LighterOrderRequest {
        symbol: "BTC".to_string(),
        client_order_id: "plan-left".to_string(),
        client_order_index: 101,
        signed_quantity: dec!(0.001),
        limit_price: Some(dec!(61150.7)),
        order_type: LighterOrderType::Limit,
        time_in_force: LighterTimeInForce::ImmediateOrCancel,
        reduce_only: false,
    }
}
