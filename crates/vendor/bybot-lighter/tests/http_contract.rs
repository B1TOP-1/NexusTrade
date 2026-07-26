use bybot_lighter::http::{
    parse_account_snapshot, parse_next_nonce, LighterHttpClient, LighterSignedTx,
};
use rust_decimal_macros::dec;

#[test]
fn builds_lighter_urls_and_sendtx_form_without_nautilus() {
    let client = LighterHttpClient::new("https://example.test/").unwrap();
    assert_eq!(client.sendtx_url(), "https://example.test/api/v1/sendTx");
    assert_eq!(
        client.next_nonce_url(42, 1),
        "https://example.test/api/v1/nextNonce?account_index=42&api_key_index=1"
    );

    let form = LighterHttpClient::sendtx_form(&LighterSignedTx {
        client_order_id: "plan-left".to_owned(),
        client_order_index: Some(101),
        tx_type: 14,
        tx_info: r#"{"signed":"payload"}"#.to_owned(),
        price_protection: true,
    });
    assert_eq!(form.get("tx_type").unwrap(), "14");
    assert_eq!(form.get("price_protection").unwrap(), "true");
}

#[test]
fn account_snapshot_preserves_decimal_precision() {
    let snapshot = parse_account_snapshot(
        r#"{
            "accounts":[{
                "collateral":"12345.67890123",
                "available_balance":"10000.00000001",
                "positions":[{
                    "market_id":1,
                    "sign":-1,
                    "position":"0.12345678",
                    "avg_entry_price":"61150.70000001"
                }]
            }]
        }"#,
    )
    .unwrap();

    assert_eq!(snapshot.collateral, dec!(12345.67890123));
    assert_eq!(snapshot.available_balance, dec!(10000.00000001));
    assert_eq!(snapshot.positions[0].signed_quantity, dec!(-0.12345678));
    assert_eq!(snapshot.positions[0].average_price, dec!(61150.70000001));
}

#[test]
fn parses_nonce() {
    assert_eq!(parse_next_nonce(r#"{"code":200,"nonce":77}"#).unwrap(), 77);
}

#[test]
fn rejects_lighter_application_errors() {
    let error = parse_next_nonce(r#"{"code":401,"message":"invalid auth"}"#).unwrap_err();
    assert!(error.to_string().contains("invalid auth"));
}
