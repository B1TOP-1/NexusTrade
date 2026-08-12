use nautilus_core::UnixNanos;
use nexus_gate::{
    http::models::GateFuturesContract,
    instrument::{gate_contract_quantity_to_base, parse_gate_futures_contract},
};
use nautilus_model::{
    identifiers::InstrumentId,
    instruments::InstrumentAny,
    types::{Price, Quantity},
};

const TS_INIT: UnixNanos = UnixNanos::new(1_700_000_000_000_000_000);

fn btc_usdt_contract() -> GateFuturesContract {
    serde_json::from_str(
        r#"{"funding_rate_indicative":"-0.000029","mark_price_round":"0.01","funding_offset":0,"in_delisting":false,"risk_limit_base":"500000","interest_rate":"0.0003","index_price":"75894.06","order_price_round":"0.1","order_size_min":1,"enable_decimal":false,"ref_rebate_rate":"0.2","name":"BTC_USDT","ref_discount_rate":"0","order_price_deviate":"0.04","maintenance_rate":"0.003","mark_type":"index","funding_interval":28800,"type":"direct","risk_limit_step":"1499500000","enable_bonus":true,"enable_credit":true,"leverage_min":"1","funding_rate":"-0.000029","last_price":"75858.7","mark_price":"75858.7","order_size_max":12000000,"funding_next_apply":1779897600,"short_users":12736,"config_change_time":1776852890,"create_time":1574035200,"trade_size":590157635663,"position_size":306752364,"long_users":18157,"quanto_multiplier":"0.0001","funding_impact_value":"30000","leverage_max":"200","cross_leverage_default":"10","risk_limit_max":"1500000000","maker_fee_rate":"-0.0001","taker_fee_rate":"0.00075","orders_limit":100,"trade_id":754908368,"orderbook_id":113493762261,"funding_cap_ratio":"0.75","voucher_leverage":"2","is_pre_market":false,"status":"trading","launch_time":1574035200,"enable_circuit_breaker":false,"funding_rate_limit":"0.003","market_order_slip_ratio":"0.02","market_order_size_max":"8000000","contract_type":""}"#,
    )
    .unwrap()
}

#[test]
fn parse_btc_usdt_futures_contract_to_crypto_perpetual() {
    let contract = btc_usdt_contract();

    let instrument = parse_gate_futures_contract(&contract, TS_INIT, TS_INIT).unwrap();

    let InstrumentAny::CryptoPerpetual(perp) = instrument else {
        panic!("expected crypto perpetual");
    };
    assert_eq!(perp.id, InstrumentId::from("BTC_USDT.GATE"));
    assert_eq!(perp.raw_symbol.as_str(), "BTC_USDT");
    assert_eq!(perp.base_currency.code.as_str(), "BTC");
    assert_eq!(perp.quote_currency.code.as_str(), "USDT");
    assert_eq!(perp.settlement_currency.code.as_str(), "USDT");
    assert!(!perp.is_inverse);
    assert_eq!(perp.price_increment, Price::from("0.1"));
    assert_eq!(perp.price_precision, 1);
    assert_eq!(perp.size_increment, Quantity::from("1"));
    assert_eq!(perp.size_precision, 0);
    assert_eq!(perp.lot_size, Quantity::from("1"));
    assert_eq!(perp.multiplier, Quantity::from("0.0001"));
    assert_eq!(perp.min_quantity, Some(Quantity::from("1")));
    assert_eq!(perp.max_quantity, Some(Quantity::from("12000000")));
}

#[test]
fn gate_contract_quantity_to_base_uses_quanto_multiplier() {
    let contract = btc_usdt_contract();

    let base_quantity = gate_contract_quantity_to_base(&contract, Quantity::from("4544"));

    assert_eq!(base_quantity, 0.4544);
}

#[test]
fn parse_invalid_contract_returns_error_without_panicking() {
    let mut contract = btc_usdt_contract();
    contract.order_size_min = 0;

    let result = parse_gate_futures_contract(&contract, TS_INIT, TS_INIT);

    assert!(result.is_err());
}
