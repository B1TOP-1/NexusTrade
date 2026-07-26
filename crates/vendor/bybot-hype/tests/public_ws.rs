use bybot_hype::public_ws::parse_l2_book_message;

#[test]
fn parses_btc_snapshot_into_fixed_point_levels() {
    let message = r#"{
        "channel":"l2Book",
        "data":{
            "coin":"BTC",
            "time":1750000000000,
            "levels":[
                [{"px":"117250.5","sz":"0.125","n":3}],
                [{"px":"117251","sz":"0.25001","n":2}]
            ]
        }
    }"#;

    let parsed = parse_l2_book_message(message, 1750000000012)
        .unwrap()
        .unwrap();

    assert_eq!(parsed.coin, "BTC");
    assert_eq!(parsed.snapshot.exchange_time_ms(), 1750000000000);
    assert_eq!(parsed.snapshot.bids()[0].price(), 11_725_050_000_000);
    assert_eq!(parsed.snapshot.asks()[0].size(), 25_001_000);
}

#[test]
fn parses_xyz_spcx_snapshot_without_special_case() {
    let message = r#"{
        "channel":"l2Book",
        "data":{
            "coin":"xyz:SPCX",
            "time":1750000001000,
            "levels":[
                [{"px":"212.35","sz":"4","n":1}],
                [{"px":"212.40","sz":"7.5","n":2}]
            ]
        }
    }"#;

    let parsed = parse_l2_book_message(message, 1750000001010)
        .unwrap()
        .unwrap();

    assert_eq!(parsed.coin, "xyz:SPCX");
    assert_eq!(parsed.snapshot.bids()[0].size(), 400_000_000);
    assert_eq!(parsed.snapshot.asks()[0].price(), 21_240_000_000);
}

#[test]
fn ignores_non_orderbook_channels() {
    let message = r#"{"channel":"pong"}"#;
    assert!(parse_l2_book_message(message, 1).unwrap().is_none());
}
