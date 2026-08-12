// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Gate futures execution helpers: order request-param construction and the
//! base-quantity <-> contract-count (张) conversion.
//!
//! Gate's `size` field is an integer **contract count** whose base value is
//! `size * quanto_multiplier` (the multiplier varies per contract, e.g. BTC_USDT
//! 1 contract = 0.0001 BTC). The Nautilus Gate instrument is defined in contracts
//! (size_increment = 1), so an order's `Quantity` is already a contract count and
//! is used directly as Gate `size`. Strategies converting from a base amount use
//! [`base_qty_to_contracts`].

use nexus_core::{Side, Tif};

use crate::common::credential::{normalize_order_text, signed_size};

/// Maps a Nautilus time-in-force (+ post-only flag) to Gate's `tif` encoding.
///
/// # Errors
///
/// Returns an error if the time-in-force is not supported by Gate futures.
pub fn map_gate_tif(tif: Tif, post_only: bool) -> anyhow::Result<&'static str> {
    if post_only {
        return Ok("poc"); // pending-or-cancelled (post-only)
    }
    match tif {
        Tif::Gtc => Ok("gtc"),
        Tif::Ioc => Ok("ioc"),
        Tif::Fok => Ok("fok"),
        other => anyhow::bail!("Unsupported Gate time in force: {other:?}"),
    }
}

/// Converts a base quantity to Gate's integer contract count using the contract's
/// `quanto_multiplier` (base value per contract). Rounds up by default so the
/// filled base amount is at least the requested quantity.
///
/// # Errors
///
/// Returns an error if the multiplier or quantity is non-positive.
pub fn base_qty_to_contracts(
    quanto_multiplier: f64,
    base_qty: f64,
    round_up: bool,
) -> anyhow::Result<u64> {
    if quanto_multiplier <= 0.0 {
        anyhow::bail!("Gate quanto_multiplier must be positive, got {quanto_multiplier}");
    }
    if base_qty <= 0.0 {
        anyhow::bail!("Gate base quantity must be positive, got {base_qty}");
    }
    let raw = base_qty / quanto_multiplier;
    let contracts = if round_up { raw.ceil() } else { raw.floor() };
    Ok(contracts as u64)
}

/// Builds the Gate WS-API order `req_param`.
///
/// `size_contracts` is the (unsigned) contract count; the sign is applied from
/// `side` (buy = long = positive, sell = short = negative). Pass `price = Some(..)`
/// for a limit order or `None` for a market order (encoded as price `"0"`, which
/// Gate requires paired with `tif = "ioc"`).
///
/// # Errors
///
/// Returns an error if the size or side is invalid for Gate.
pub fn build_order_req_param(
    contract: &str,
    side: Side,
    size_contracts: u64,
    price: Option<&str>,
    tif: &str,
    reduce_only: bool,
    client_order_id: &str,
) -> anyhow::Result<serde_json::Value> {
    let signed = signed_size(side, size_contracts)?;
    let mut body = serde_json::json!({
        "contract": contract,
        "size": signed,
        "price": price.unwrap_or("0"),
        "tif": tif,
        "text": normalize_order_text(client_order_id),
    });
    if reduce_only {
        body["reduce_only"] = serde_json::Value::Bool(true);
    }
    // Market orders (price "0") require a slippage tolerance ratio.
    if price.is_none() {
        body["market_order_slip_ratio"] = serde_json::Value::String("0.001".to_string());
    }
    Ok(body)
}

/// Parsed Gate WS-API response (order_place/order_cancel ack), correlated by
/// `request_id`. `status_ok` reflects `header.status == "200"`.
///
/// `x_in_time`/`x_out_time` are Gate's server-side receive/send timestamps (from
/// the response header) — latency reference points (see the execution client).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateApiResponse {
    pub request_id: String,
    pub status_ok: bool,
    pub order_id: Option<String>,
    pub reason: Option<String>,
    /// Order lifecycle status from `data.result.status` ("open"/"finished").
    pub order_status: Option<String>,
    pub x_in_time: Option<String>,
    pub x_out_time: Option<String>,
}

/// Extracts a WS-API response if `text` carries a `request_id` (vs a channel push).
/// Returns `None` for non-response frames and intermediate `ack:true` frames.
#[must_use]
pub fn parse_ws_api_response(text: &str) -> Option<GateApiResponse> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    let request_id = value.get("request_id")?.as_str()?.to_string();
    // Skip the intermediate ack frame; wait for the result-bearing response.
    if value.get("ack").and_then(serde_json::Value::as_bool) == Some(true) {
        return None;
    }
    let status_ok = value
        .get("header")
        .and_then(|h| h.get("status"))
        .and_then(|s| s.as_str().map(str::to_string).or_else(|| s.as_i64().map(|n| n.to_string())))
        .is_some_and(|s| s == "200");
    let header = value.get("header");
    let header_time = |key: &str| -> Option<String> {
        header.and_then(|h| h.get(key)).and_then(json_id_to_string)
    };
    let result = value.get("data").and_then(|d| d.get("result"));
    let order_id = result
        .and_then(|r| r.get("id"))
        .and_then(json_id_to_string);
    let reason = if status_ok {
        None
    } else {
        result
            .and_then(|r| r.get("label").or_else(|| r.get("message")))
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .or_else(|| Some("rejected".to_string()))
    };
    let order_status = result
        .and_then(|r| r.get("status"))
        .and_then(|v| v.as_str())
        .map(str::to_string);
    Some(GateApiResponse {
        request_id,
        status_ok,
        order_id,
        reason,
        order_status,
        x_in_time: header_time("x_in_time"),
        x_out_time: header_time("x_out_time"),
    })
}

/// A fill parsed from a `futures.usertrades` push.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateTradeUpdate {
    pub order_id: String,
    pub trade_id: String,
    pub size: String,
    pub price: String,
    pub fee: String,
    pub role: String,
    /// Client order text (`t-...`) — maps to our order without ack-timing races.
    pub text: String,
}

/// Parses `futures.usertrades` fills (`result` is an array).
#[must_use]
pub fn parse_usertrades(text: &str) -> Vec<GateTradeUpdate> {
    let Some(items) = channel_result_items(text, "futures.usertrades") else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|t| {
            Some(GateTradeUpdate {
                order_id: json_id_to_string(t.get("order_id")?)?,
                trade_id: json_id_to_string(t.get("id")?)?,
                size: str_field(t, "size")?,
                price: str_field(t, "price").unwrap_or_default(),
                fee: str_field(t, "fee").unwrap_or_default(),
                role: str_field(t, "role").unwrap_or_default(),
                text: str_field(t, "text").unwrap_or_default(),
            })
        })
        .collect()
}

/// An order-status update parsed from a `futures.orders` push.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateOrderUpdate {
    pub order_id: String,
    pub text: Option<String>,
    pub status: String,
    pub finish_as: Option<String>,
}

/// Parses `futures.orders` status updates (`result` is an array).
#[must_use]
pub fn parse_orders_push(text: &str) -> Vec<GateOrderUpdate> {
    let Some(items) = channel_result_items(text, "futures.orders") else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|o| {
            Some(GateOrderUpdate {
                order_id: json_id_to_string(o.get("id")?)?,
                text: str_field(o, "text"),
                status: str_field(o, "status").unwrap_or_default(),
                finish_as: str_field(o, "finish_as"),
            })
        })
        .collect()
}

/// Returns the `result` array items for a push on `channel` (handles object or array).
fn channel_result_items(text: &str, channel: &str) -> Option<Vec<serde_json::Value>> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    if value.get("channel")?.as_str()? != channel {
        return None;
    }
    match value.get("result")? {
        serde_json::Value::Array(items) => Some(items.clone()),
        obj @ serde_json::Value::Object(_) => Some(vec![obj.clone()]),
        _ => None,
    }
}

/// Renders a JSON id (string or integer) as a string.
fn json_id_to_string(value: &serde_json::Value) -> Option<String> {
    value
        .as_str()
        .map(str::to_string)
        .or_else(|| value.as_i64().map(|n| n.to_string()))
        .or_else(|| value.as_u64().map(|n| n.to_string()))
}

/// Reads a field that may be a decimal string (with `X-Gate-Size-Decimal`) or a number.
fn str_field(value: &serde_json::Value, key: &str) -> Option<String> {
    value.get(key).and_then(|v| {
        v.as_str()
            .map(str::to_string)
            .or_else(|| v.as_i64().map(|n| n.to_string()))
            .or_else(|| v.as_f64().map(|n| n.to_string()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_buy_req_param() {
        let v =
            build_order_req_param("BTC_USDT", Side::Buy, 2, Some("61000"), "gtc", false, "abc")
                .unwrap();
        assert_eq!(v["contract"], "BTC_USDT");
        assert_eq!(v["size"], 2);
        assert_eq!(v["price"], "61000");
        assert_eq!(v["tif"], "gtc");
        assert_eq!(v["text"], "t-abc");
        assert!(v.get("reduce_only").is_none());
    }

    #[test]
    fn market_sell_req_param() {
        let v =
            build_order_req_param("ETH_USDT", Side::Sell, 5, None, "ioc", true, "t-x").unwrap();
        assert_eq!(v["size"], -5); // sell -> negative
        assert_eq!(v["price"], "0"); // market
        assert_eq!(v["tif"], "ioc");
        assert_eq!(v["reduce_only"], true);
        assert_eq!(v["market_order_slip_ratio"], "0.001"); // required for market
    }

    #[test]
    fn base_qty_conversion_rounds_up() {
        // BTC_USDT: 1 contract = 0.0001 BTC.
        assert_eq!(base_qty_to_contracts(0.0001, 0.0002, true).unwrap(), 2);
        // 0.00025 / 0.0001 = 2.5 -> ceil = 3.
        assert_eq!(base_qty_to_contracts(0.0001, 0.00025, true).unwrap(), 3);
        // multiplier 1: contracts == base.
        assert_eq!(base_qty_to_contracts(1.0, 7.0, true).unwrap(), 7);
        assert!(base_qty_to_contracts(0.0, 1.0, true).is_err());
    }

    #[test]
    fn tif_mapping() {
        assert_eq!(map_gate_tif(Tif::Gtc, false).unwrap(), "gtc");
        assert_eq!(map_gate_tif(Tif::Gtc, true).unwrap(), "poc");
        assert_eq!(map_gate_tif(Tif::Ioc, false).unwrap(), "ioc");
        assert_eq!(map_gate_tif(Tif::Fok, false).unwrap(), "fok");
    }

    #[test]
    fn api_response_success_and_failure() {
        let ok = parse_ws_api_response(
            r#"{"request_id":"order-1","header":{"status":"200"},"data":{"result":{"id":888,"status":"open"}}}"#,
        )
        .unwrap();
        assert_eq!(ok.request_id, "order-1");
        assert!(ok.status_ok);
        assert_eq!(ok.order_id.as_deref(), Some("888"));

        let err = parse_ws_api_response(
            r#"{"request_id":"order-2","header":{"status":"400"},"data":{"result":{"label":"INVALID_PARAM"}}}"#,
        )
        .unwrap();
        assert!(!err.status_ok);
        assert_eq!(err.reason.as_deref(), Some("INVALID_PARAM"));

        // Intermediate ack and non-response frames are skipped.
        assert!(parse_ws_api_response(r#"{"request_id":"x","ack":true}"#).is_none());
        assert!(parse_ws_api_response(r#"{"channel":"futures.orders"}"#).is_none());
    }

    #[test]
    fn usertrades_parsed_with_decimal_size() {
        // Size as decimal string (X-Gate-Size-Decimal): preserved, not floored.
        let trades = parse_usertrades(
            r#"{"channel":"futures.usertrades","event":"update","result":[{"order_id":456,"id":99,"size":"0.00015","price":"61000","fee":"0.01","role":"taker"}]}"#,
        );
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].order_id, "456");
        assert_eq!(trades[0].trade_id, "99");
        assert_eq!(trades[0].size, "0.00015");
        assert_eq!(trades[0].role, "taker");
    }

    #[test]
    fn orders_push_parsed() {
        let orders = parse_orders_push(
            r#"{"channel":"futures.orders","event":"update","result":[{"id":456,"text":"t-abc","status":"finished","finish_as":"cancelled"}]}"#,
        );
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].order_id, "456");
        assert_eq!(orders[0].text.as_deref(), Some("t-abc"));
        assert_eq!(orders[0].finish_as.as_deref(), Some("cancelled"));
    }
}

/// Builds a Gate WS-API request envelope (`event:"api"`).
#[must_use]
pub fn build_api_envelope(
    channel: &str,
    req_id: &str,
    req_param: &serde_json::Value,
    timestamp: i64,
) -> String {
    serde_json::json!({
        "time": timestamp,
        "channel": channel,
        "event": "api",
        "payload": {"req_id": req_id, "req_param": req_param},
    })
    .to_string()
}

/// Builds a private channel subscribe envelope with `api_key` auth.
#[must_use]
pub fn build_subscribe_private_envelope(
    channel: &str,
    payload: &[String],
    api_key: &str,
    signature: &str,
    timestamp: i64,
) -> String {
    serde_json::json!({
        "time": timestamp,
        "channel": channel,
        "event": "subscribe",
        "payload": payload,
        "auth": {"method": "api_key", "KEY": api_key, "SIGN": signature},
    })
    .to_string()
}
