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

use std::collections::{HashMap, HashSet};

use rust_decimal::Decimal;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterSubmitAck {
    pub client_order_id: String,
    pub client_order_index: Option<u64>,
    pub tx_hash: String,
    pub ts_event_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterPrivateOrderEvent {
    pub client_order_id: Option<String>,
    pub client_order_index: Option<u64>,
    /// Venue-assigned order index (from `order_index`/`order_id`), used to cancel.
    pub order_index: Option<u64>,
    pub status: String,
    pub filled_base_amount: u64,
    pub ts_event_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterPrivateTradeEvent {
    pub trade_id: String,
    /// Client order index of the bid side (`bid_client_id`).
    pub bid_client_index: Option<u64>,
    /// Client order index of the ask side (`ask_client_id`).
    pub ask_client_index: Option<u64>,
    /// Fill base size as a decimal string (e.g. "0.00020").
    pub size: String,
    /// Fill price as a decimal string (e.g. "61150.7").
    pub price: Option<String>,
    pub fee: i64,
    pub ts_event_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterPrivatePositionEvent {
    pub market_id: u64,
    pub signed_quantity: Decimal,
    pub average_price: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterPrivateAccountStatsEvent {
    pub collateral: Decimal,
    pub available_balance: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LighterAccountChannel {
    Orders,
    Trades,
    Positions,
    Stats,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterCancelAck {
    pub client_order_id: String,
    pub client_order_index: Option<u64>,
    pub tx_hash: String,
    pub ts_event_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterSendTxError {
    pub code: i64,
    pub message: String,
}

pub fn parse_lighter_submit_ack(
    payload: &str,
    client_order_id: String,
    client_order_index: Option<u64>,
) -> Result<LighterSubmitAck, LighterSendTxError> {
    let parsed = parse_sendtx_ack_payload(payload)?;
    Ok(LighterSubmitAck {
        client_order_id,
        client_order_index,
        tx_hash: parsed.tx_hash,
        ts_event_ms: parsed.ts_event_ms,
    })
}

pub fn parse_lighter_cancel_ack(
    payload: &str,
    client_order_id: String,
    client_order_index: Option<u64>,
) -> Result<LighterCancelAck, LighterSendTxError> {
    let parsed = parse_sendtx_ack_payload(payload)?;
    Ok(LighterCancelAck {
        client_order_id,
        client_order_index,
        tx_hash: parsed.tx_hash,
        ts_event_ms: parsed.ts_event_ms,
    })
}

/// A parsed `jsonapi/sendtx` acknowledgement envelope (success or failure),
/// used to match the ack back to its originating order by `tx_hash`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterSendTxAck {
    pub code: i64,
    pub tx_hash: Option<String>,
    pub message: Option<String>,
    pub ts_event_ms: u64,
}

/// Detects and parses a `jsonapi/sendtx` acknowledgement from an inbound WS
/// message. Returns `None` for account channel pushes (`account_orders`,
/// `account_all_trades`, `subscribed/...`) which are handled by the reducer.
#[must_use]
pub fn parse_lighter_sendtx_ack_envelope(payload: &str) -> Option<LighterSendTxAck> {
    let value: Value = serde_json::from_str(payload).ok()?;
    let msg_type = string_field(&value, &["type"]).unwrap_or_default();

    let is_sendtx = msg_type == "jsonapi/sendtx"
        || (value.get("code").is_some()
            && value.get("channel").is_none()
            && !msg_type.starts_with("update/")
            && !msg_type.starts_with("subscribed/"));
    if !is_sendtx {
        return None;
    }

    let code = i64_field(&value, &["code"]).ok().flatten().unwrap_or(-1);
    let body = value.get("data").unwrap_or(&value);
    let tx_hash = string_field(body, &["tx_hash", "txHash", "hash"])
        .or_else(|| string_field(&value, &["tx_hash", "txHash", "hash"]));
    let message = string_field(&value, &["message", "error", "msg"]);
    let ts_event_ms = u64_field(&value, &["timestamp", "ts", "time"])
        .ok()
        .flatten()
        .or_else(|| u64_field(body, &["timestamp", "ts", "time"]).ok().flatten())
        .map(normalize_ack_epoch_millis)
        .unwrap_or(0);

    Some(LighterSendTxAck {
        code,
        tx_hash,
        message,
        ts_event_ms,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedSendTxAck {
    tx_hash: String,
    ts_event_ms: u64,
}

fn parse_sendtx_ack_payload(payload: &str) -> Result<ParsedSendTxAck, LighterSendTxError> {
    let value: Value = serde_json::from_str(payload).map_err(|e| LighterSendTxError {
        code: -1,
        message: format!("invalid sendtx JSON: {e}"),
    })?;
    let code = i64_field(&value, &["code"])
        .map_err(|e| LighterSendTxError {
            code: -1,
            message: format!("invalid sendtx code: {e}"),
        })?
        .unwrap_or(-1);
    if code != 200 {
        return Err(LighterSendTxError {
            code,
            message: string_field(&value, &["message", "error", "msg"])
                .unwrap_or_else(|| format!("sendtx failed with code {code}")),
        });
    }

    let body = value.get("data").unwrap_or(&value);
    let tx_hash =
        string_field(body, &["tx_hash", "txHash", "hash"]).ok_or_else(|| LighterSendTxError {
            code,
            message: "sendtx code=200 missing transaction hash".to_string(),
        })?;
    let ts_event_ms = u64_field(&value, &["timestamp", "ts", "time"])
        .map_err(|e| LighterSendTxError {
            code,
            message: format!("invalid sendtx timestamp: {e}"),
        })?
        .or_else(|| u64_field(body, &["timestamp", "ts", "time"]).ok().flatten())
        .map(normalize_ack_epoch_millis)
        .unwrap_or(0);

    Ok(ParsedSendTxAck {
        tx_hash,
        ts_event_ms,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LighterExecutionEffect {
    Submitted {
        client_order_id: String,
        client_order_index: Option<u64>,
        tx_hash: String,
        ts_event_ms: u64,
    },
    Accepted {
        client_order_id: String,
        client_order_index: Option<u64>,
        order_index: Option<u64>,
        ts_event_ms: u64,
    },
    Fill {
        client_order_id: String,
        client_order_index: Option<u64>,
        trade_id: Option<String>,
        /// Fill base quantity as a decimal string (Lighter sends e.g. "0.00020").
        quantity: String,
        /// Fill price as a decimal string (Lighter sends e.g. "61150.7").
        price: Option<String>,
        fee: i64,
        synthetic: bool,
        ts_event_ms: u64,
    },
    Canceled {
        client_order_id: String,
        client_order_index: Option<u64>,
        reason: String,
        ts_event_ms: u64,
    },
    Rejected {
        client_order_id: String,
        client_order_index: Option<u64>,
        reason: String,
        ts_event_ms: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LighterPrivateWsMessage {
    Order(LighterPrivateOrderEvent),
    Trade(LighterPrivateTradeEvent),
    PositionSnapshot(Vec<LighterPrivatePositionEvent>),
    PositionUpdate(Vec<LighterPrivatePositionEvent>),
    AccountStats(LighterPrivateAccountStatsEvent),
    Ready(LighterAccountChannel),
}

/// Normalises a Lighter epoch value to milliseconds by magnitude.
///
/// Lighter mixes units across fields (`timestamp` is seconds, `transaction_time`
/// is microseconds), so the unit is inferred from the value's order of magnitude
/// rather than the field name. Values already in milliseconds pass through.
fn normalize_epoch_millis(value: u64) -> u64 {
    const US: u64 = 1_000_000_000_000_000; // ~2001-09 in us (>= 16 digits)
    const NS: u64 = 1_000_000_000_000_000_000; // ~2001-09 in ns (>= 19 digits)
                                               // Down-scale microsecond/nanosecond epochs to milliseconds. Values already in
                                               // milliseconds (or smaller test fixtures) pass through unchanged. Real Lighter
                                               // order/trade pushes always carry `transaction_time` in microseconds.
    if value >= NS {
        value / 1_000_000
    } else if value >= US {
        value / 1_000
    } else {
        value
    }
}

/// Normalises sendTx ack epochs to milliseconds, including second-magnitude
/// values: the REST/WS ack `timestamp` field is emitted in *seconds*, which
/// `normalize_epoch_millis` would otherwise pass through as-if-milliseconds
/// (landing in 1970). Live epochs in ms are always >= 1e12; seconds are ~1e9.
fn normalize_ack_epoch_millis(value: u64) -> u64 {
    const SECONDS_MIN: u64 = 1_000_000_000; // 2001-09 in s (10 digits)
    const MILLIS_MIN: u64 = 1_000_000_000_000; // 2001-09 in ms (13 digits)
    if (SECONDS_MIN..MILLIS_MIN).contains(&value) {
        value * 1_000
    } else {
        normalize_epoch_millis(value)
    }
}
/// Flattens a Lighter `orders`/`trades` payload into individual event objects.
///
/// Lighter sends these grouped by market index (`{"1": [ ... ]}`), but a flat
/// array or single object is also accepted for robustness.
fn collect_event_items(value: &Value) -> Vec<&Value> {
    match value {
        Value::Array(items) => items.iter().collect(),
        Value::Object(map) => map
            .values()
            .flat_map(|v| match v {
                Value::Array(items) => items.iter().collect::<Vec<_>>(),
                other => vec![other],
            })
            .collect(),
        other => vec![other],
    }
}

/// Parses a Lighter private WS message into zero or more order/trade events.
///
/// Real Lighter pushes carry arrays under `orders` / `trades` (e.g. message type
/// `update/account_orders`). Singular `order` / `trade` keys are also accepted.
/// Subscription confirmations (no order/trade payload) yield an empty vec rather
/// than an error.
pub fn parse_lighter_private_ws_messages(
    payload: &str,
) -> anyhow::Result<Vec<LighterPrivateWsMessage>> {
    let value: Value = serde_json::from_str(payload)?;
    let message_type = string_field(&value, &["type"]).unwrap_or_default();
    let server_error = value.get("error");
    if message_type.contains("error") || message_type.contains("reject") || server_error.is_some() {
        let error_value = server_error.unwrap_or(&value);
        let code = string_field(error_value, &["code"]).unwrap_or_else(|| "unknown".to_string());
        let message = string_field(error_value, &["message", "error"])
            .unwrap_or_else(|| "no server message".to_string());
        anyhow::bail!(
            "Lighter private WebSocket rejected subscription: type={message_type} code={code} message={message}"
        );
    }
    let mut out = Vec::new();

    // Lighter groups `orders`/`trades` by market index: {"1": [ ... ]}. Also
    // accept a flat array or a singular `order`/`trade` object.
    if let Some(orders) = value.get("orders") {
        for item in collect_event_items(orders) {
            out.push(LighterPrivateWsMessage::Order(parse_private_order_event(
                item,
            )?));
        }
    } else if let Some(item) = value.get("order") {
        out.push(LighterPrivateWsMessage::Order(parse_private_order_event(
            item,
        )?));
    }

    if let Some(trades) = value.get("trades") {
        for item in collect_event_items(trades) {
            out.push(LighterPrivateWsMessage::Trade(parse_private_trade_event(
                item,
            )?));
        }
    } else if let Some(item) = value.get("trade") {
        out.push(LighterPrivateWsMessage::Trade(parse_private_trade_event(
            item,
        )?));
    }

    if let Some(positions) = value.get("positions") {
        let positions = collect_event_items(positions)
            .into_iter()
            .map(parse_private_position_event)
            .collect::<anyhow::Result<Vec<_>>>()?;
        if message_type == "update/account_all_positions" {
            out.push(LighterPrivateWsMessage::PositionUpdate(positions));
        } else {
            out.push(LighterPrivateWsMessage::PositionSnapshot(positions));
        }
    }

    if let Some(stats) = value.get("stats") {
        out.push(LighterPrivateWsMessage::AccountStats(
            parse_private_account_stats_event(stats)?,
        ));
    }

    // Fallback: a `data` envelope (array or object) tagged by channel/type.
    if out.is_empty() {
        if let (Some(data), Some(channel)) = (
            value.get("data"),
            string_field(&value, &["channel", "type"]),
        ) {
            let items: Vec<&Value> = data
                .as_array()
                .map_or_else(|| vec![data], |items| items.iter().collect());
            for item in items {
                if channel.contains("account_orders") || channel.contains("account_all_orders") {
                    out.push(LighterPrivateWsMessage::Order(parse_private_order_event(
                        item,
                    )?));
                } else if channel.contains("account_all_trades") {
                    out.push(LighterPrivateWsMessage::Trade(parse_private_trade_event(
                        item,
                    )?));
                }
            }
        }
    }

    let ready = match message_type.as_str() {
        "subscribed/account_all_orders" | "update/account_all_orders" => {
            Some(LighterAccountChannel::Orders)
        }
        "subscribed/account_all_trades" | "update/account_all_trades" => {
            Some(LighterAccountChannel::Trades)
        }
        "subscribed/account_all_positions" | "update/account_all_positions" => {
            Some(LighterAccountChannel::Positions)
        }
        "subscribed/user_stats" | "update/user_stats" => Some(LighterAccountChannel::Stats),
        _ => None,
    };
    if let Some(channel) = ready {
        out.push(LighterPrivateWsMessage::Ready(channel));
    }

    Ok(out)
}

fn parse_private_order_event(value: &Value) -> anyhow::Result<LighterPrivateOrderEvent> {
    Ok(LighterPrivateOrderEvent {
        client_order_id: string_field(value, &["client_order_id", "clientOrderId"]),
        client_order_index: u64_field(value, &["client_order_index", "clientOrderIndex"])?,
        order_index: u64_field(value, &["order_index", "order_id", "orderIndex"])?,
        status: required_string_field(value, &["status", "order_status", "orderStatus"])?,
        // Lighter sends `filled_base_amount` as a decimal string ("0.00000") here;
        // integer base units live in the trades channel. Be lenient so the order
        // status (open/canceled) still parses — fills are tracked via trades.
        filled_base_amount: u64_field(value, &["filled_base_amount", "filledBaseAmount"])
            .ok()
            .flatten()
            .unwrap_or(0),
        ts_event_ms: normalize_epoch_millis(
            u64_field(value, &["transaction_time", "timestamp", "ts", "time"])?.unwrap_or(0),
        ),
    })
}

fn parse_private_trade_event(value: &Value) -> anyhow::Result<LighterPrivateTradeEvent> {
    Ok(LighterPrivateTradeEvent {
        trade_id: required_string_field(value, &["trade_id_str", "trade_id", "tradeId", "id"])?,
        // The trade carries both sides' client order indexes; the reducer matches
        // whichever one belongs to a tracked order.
        bid_client_index: u64_field(value, &["bid_client_id", "bid_client_id_str"])
            .ok()
            .flatten(),
        ask_client_index: u64_field(value, &["ask_client_id", "ask_client_id_str"])
            .ok()
            .flatten(),
        // Decimal strings; passed through to Quantity/Price as-is.
        size: string_field(value, &["size", "base_amount", "filled_base_amount"])
            .unwrap_or_default(),
        price: string_field(value, &["price", "px", "execution_price"]),
        // Lighter reports `maker_fee` (integer); taker fee not separately provided.
        fee: i64_field(value, &["taker_fee", "maker_fee", "fee"])
            .ok()
            .flatten()
            .unwrap_or(0),
        ts_event_ms: normalize_epoch_millis(
            u64_field(value, &["transaction_time", "timestamp", "ts", "time"])
                .ok()
                .flatten()
                .unwrap_or(0),
        ),
    })
}

fn parse_private_position_event(value: &Value) -> anyhow::Result<LighterPrivatePositionEvent> {
    let sign = i64_field(value, &["sign"])?.unwrap_or(0);
    let quantity = decimal_field(value, &["position", "size"])?;
    let signed_quantity = if sign < 0 { -quantity } else { quantity };
    Ok(LighterPrivatePositionEvent {
        market_id: u64_field(value, &["market_id", "marketId"])?
            .ok_or_else(|| anyhow::anyhow!("missing Lighter position market id"))?,
        signed_quantity,
        average_price: decimal_field(value, &["avg_entry_price", "average_price"])
            .unwrap_or(Decimal::ZERO),
    })
}

fn parse_private_account_stats_event(
    value: &Value,
) -> anyhow::Result<LighterPrivateAccountStatsEvent> {
    Ok(LighterPrivateAccountStatsEvent {
        collateral: decimal_field(value, &["collateral"])?,
        available_balance: decimal_field(value, &["available_balance"])?,
    })
}

fn required_string_field(value: &Value, keys: &[&str]) -> anyhow::Result<String> {
    string_field(value, keys).ok_or_else(|| anyhow::anyhow!("missing required field {keys:?}"))
}

fn string_field(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        value.get(key).and_then(|value| match value {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        })
    })
}

fn u64_field(value: &Value, keys: &[&str]) -> anyhow::Result<Option<u64>> {
    keys.iter()
        .find_map(|key| value.get(key).map(parse_u64_value))
        .transpose()
}

fn i64_field(value: &Value, keys: &[&str]) -> anyhow::Result<Option<i64>> {
    keys.iter()
        .find_map(|key| value.get(key).map(parse_i64_value))
        .transpose()
}

fn decimal_field(value: &Value, keys: &[&str]) -> anyhow::Result<Decimal> {
    required_string_field(value, keys)?
        .parse::<Decimal>()
        .map_err(|error| anyhow::anyhow!("invalid decimal field {keys:?}: {error}"))
}

fn parse_u64_value(value: &Value) -> anyhow::Result<u64> {
    match value {
        Value::Number(number) => number
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("invalid unsigned number {number}")),
        Value::String(text) => text
            .parse::<u64>()
            .map_err(|e| anyhow::anyhow!("invalid unsigned string {text}: {e}")),
        other => anyhow::bail!("invalid unsigned field type {other}"),
    }
}

fn parse_i64_value(value: &Value) -> anyhow::Result<i64> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .ok_or_else(|| anyhow::anyhow!("invalid signed number {number}")),
        Value::String(text) => text
            .parse::<i64>()
            .map_err(|e| anyhow::anyhow!("invalid signed string {text}: {e}")),
        other => anyhow::bail!("invalid signed field type {other}"),
    }
}

#[derive(Debug, Default)]
pub struct LighterExecutionReducer {
    drain_window_ms: u64,
    orders: HashMap<String, LighterOrderState>,
    ids_by_index: HashMap<u64, String>,
    pending_order_events_by_index: HashMap<u64, Vec<LighterPrivateOrderEvent>>,
    pending_trade_events_by_index: HashMap<u64, Vec<LighterPrivateTradeEvent>>,
}

#[derive(Debug, Default)]
struct LighterOrderState {
    client_order_index: Option<u64>,
    accepted: bool,
    pending_cancel: bool,
    terminal_at_ms: Option<u64>,
    seen_trade_ids: HashSet<String>,
}

impl LighterExecutionReducer {
    #[must_use]
    pub fn new(drain_window_ms: u64) -> Self {
        Self {
            drain_window_ms,
            ..Self::default()
        }
    }

    pub fn restore_order(&mut self, client_order_id: &str, client_order_index: u64) {
        self.ids_by_index
            .insert(client_order_index, client_order_id.to_string());
        self.orders
            .entry(client_order_id.to_string())
            .or_insert_with(|| LighterOrderState {
                client_order_index: Some(client_order_index),
                ..LighterOrderState::default()
            });
    }

    pub fn on_submit_ack(&mut self, ack: LighterSubmitAck) -> Vec<LighterExecutionEffect> {
        let mut effects = vec![LighterExecutionEffect::Submitted {
            client_order_id: ack.client_order_id.clone(),
            client_order_index: ack.client_order_index,
            tx_hash: ack.tx_hash,
            ts_event_ms: ack.ts_event_ms,
        }];

        let state = self
            .orders
            .entry(ack.client_order_id.clone())
            .or_insert_with(|| LighterOrderState {
                client_order_index: ack.client_order_index,
                ..LighterOrderState::default()
            });
        state.client_order_index = ack.client_order_index;

        if let Some(index) = ack.client_order_index {
            self.ids_by_index.insert(index, ack.client_order_id.clone());
            if let Some(events) = self.pending_order_events_by_index.remove(&index) {
                for event in events {
                    effects.extend(self.apply_order_event(&ack.client_order_id, &event));
                }
            }
            if let Some(events) = self.pending_trade_events_by_index.remove(&index) {
                for event in events {
                    effects.extend(self.apply_trade_event(&ack.client_order_id, event));
                }
            }
        }

        effects
    }

    pub fn on_order_event(
        &mut self,
        event: LighterPrivateOrderEvent,
    ) -> Vec<LighterExecutionEffect> {
        // Match by client_order_index first: it is the key we control and mapped
        // to the Nautilus client order id at submit-ack time. Lighter's pushed
        // `client_order_id` is the numeric index as a string, not our id.
        if let Some(index) = event.client_order_index {
            if let Some(client_order_id) = self.ids_by_index.get(&index).cloned() {
                return self.apply_order_event(&client_order_id, &event);
            }
            // Index not yet mapped (push arrived before submit ack): replay later.
            self.pending_order_events_by_index
                .entry(index)
                .or_default()
                .push(event);
            return Vec::new();
        }

        if let Some(client_order_id) = event.client_order_id.clone() {
            return self.apply_order_event(&client_order_id, &event);
        }

        Vec::new()
    }

    pub fn on_trade_event(
        &mut self,
        event: LighterPrivateTradeEvent,
    ) -> Vec<LighterExecutionEffect> {
        // The trade lists both sides' client order indexes; resolve whichever
        // belongs to one of our tracked orders.
        let resolved = event
            .bid_client_index
            .and_then(|i| self.ids_by_index.get(&i).cloned())
            .or_else(|| {
                event
                    .ask_client_index
                    .and_then(|i| self.ids_by_index.get(&i).cloned())
            });
        if let Some(client_order_id) = resolved {
            return self.apply_trade_event(&client_order_id, event);
        }

        // Not yet mapped (trade before submit ack): either side may be ours, so
        // retain the event under both distinct client indexes for later replay.
        if let Some(index) = event.bid_client_index {
            self.pending_trade_events_by_index
                .entry(index)
                .or_default()
                .push(event.clone());
        }
        if let Some(index) = event
            .ask_client_index
            .filter(|index| Some(*index) != event.bid_client_index)
        {
            self.pending_trade_events_by_index
                .entry(index)
                .or_default()
                .push(event);
        }
        Vec::new()
    }

    pub fn on_cancel_ack(&mut self, ack: LighterCancelAck) -> Vec<LighterExecutionEffect> {
        let state = self
            .orders
            .entry(ack.client_order_id.clone())
            .or_insert_with(|| LighterOrderState {
                client_order_index: ack.client_order_index,
                ..LighterOrderState::default()
            });
        state.client_order_index = state.client_order_index.or(ack.client_order_index);
        state.pending_cancel = true;

        if let Some(index) = ack.client_order_index {
            self.ids_by_index.insert(index, ack.client_order_id);
        }

        Vec::new()
    }

    pub fn on_private_ws_text(
        &mut self,
        payload: &str,
    ) -> anyhow::Result<Vec<LighterExecutionEffect>> {
        let mut effects = Vec::new();
        for message in parse_lighter_private_ws_messages(payload)? {
            match message {
                LighterPrivateWsMessage::Order(event) => {
                    effects.extend(self.on_order_event(event));
                }
                LighterPrivateWsMessage::Trade(event) => {
                    effects.extend(self.on_trade_event(event));
                }
                LighterPrivateWsMessage::PositionSnapshot(_)
                | LighterPrivateWsMessage::PositionUpdate(_)
                | LighterPrivateWsMessage::AccountStats(_)
                | LighterPrivateWsMessage::Ready(_) => {}
            }
        }
        Ok(effects)
    }

    fn apply_order_event(
        &mut self,
        client_order_id: &str,
        event: &LighterPrivateOrderEvent,
    ) -> Vec<LighterExecutionEffect> {
        let state = self
            .orders
            .entry(client_order_id.to_string())
            .or_insert_with(|| LighterOrderState {
                client_order_index: event.client_order_index,
                ..LighterOrderState::default()
            });
        state.client_order_index = state.client_order_index.or(event.client_order_index);

        let mut effects = Vec::new();

        if event.status.starts_with("canceled") {
            if state.terminal_at_ms.is_none() {
                state.terminal_at_ms = Some(event.ts_event_ms);
                let reason = LighterTerminalReason::from_raw(&event.status);
                match reason.classification {
                    LighterTerminalClassification::UserCancel => {
                        effects.push(LighterExecutionEffect::Canceled {
                            client_order_id: client_order_id.to_string(),
                            client_order_index: state.client_order_index,
                            reason: reason.normalized,
                            ts_event_ms: event.ts_event_ms,
                        });
                    }
                    LighterTerminalClassification::ExchangeReject => {
                        effects.push(LighterExecutionEffect::Rejected {
                            client_order_id: client_order_id.to_string(),
                            client_order_index: state.client_order_index,
                            reason: reason.normalized,
                            ts_event_ms: event.ts_event_ms,
                        });
                    }
                }
            }
            return effects;
        }

        if event.status == "open" && !state.accepted {
            state.accepted = true;
            effects.push(LighterExecutionEffect::Accepted {
                client_order_id: client_order_id.to_string(),
                client_order_index: state.client_order_index,
                order_index: event.order_index,
                ts_event_ms: event.ts_event_ms,
            });
        }

        // Fills are sourced authoritatively from the trades channel (with price,
        // size and trade id); the order channel only drives status transitions.
        effects
    }

    fn apply_trade_event(
        &mut self,
        client_order_id: &str,
        event: LighterPrivateTradeEvent,
    ) -> Vec<LighterExecutionEffect> {
        let state = self.orders.entry(client_order_id.to_string()).or_default();

        if let Some(terminal_at_ms) = state.terminal_at_ms {
            if event.ts_event_ms > terminal_at_ms.saturating_add(self.drain_window_ms) {
                return Vec::new();
            }
        }

        if !state.seen_trade_ids.insert(event.trade_id.clone()) {
            return Vec::new();
        }

        vec![LighterExecutionEffect::Fill {
            client_order_id: client_order_id.to_string(),
            client_order_index: state.client_order_index,
            trade_id: Some(event.trade_id),
            quantity: event.size,
            price: event.price,
            fee: event.fee,
            synthetic: false,
            ts_event_ms: event.ts_event_ms,
        }]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LighterTerminalClassification {
    UserCancel,
    ExchangeReject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterTerminalReason {
    pub raw: String,
    pub normalized: String,
    pub classification: LighterTerminalClassification,
}

impl LighterTerminalReason {
    #[must_use]
    pub fn from_raw(raw: &str) -> Self {
        let (normalized, classification) = match raw {
            "canceled-by-user" | "canceled-user" | "canceled" => (
                "LIGHTER_USER_CANCELED".to_string(),
                LighterTerminalClassification::UserCancel,
            ),
            "canceled-too-much-slippage" => (
                "LIGHTER_TOO_MUCH_SLIPPAGE".to_string(),
                LighterTerminalClassification::ExchangeReject,
            ),
            "canceled-not-enough-liquidity" => (
                "LIGHTER_NOT_ENOUGH_LIQUIDITY".to_string(),
                LighterTerminalClassification::ExchangeReject,
            ),
            "canceled-post-only" | "canceled-post-only-would-take" => (
                "LIGHTER_POST_ONLY_WOULD_TAKE".to_string(),
                LighterTerminalClassification::ExchangeReject,
            ),
            "canceled-reduce-only" | "canceled-reduce-only-rejected" => (
                "LIGHTER_REDUCE_ONLY_REJECTED".to_string(),
                LighterTerminalClassification::ExchangeReject,
            ),
            "canceled-insufficient-margin" | "canceled-margin" => (
                "LIGHTER_INSUFFICIENT_MARGIN".to_string(),
                LighterTerminalClassification::ExchangeReject,
            ),
            "canceled-self-trade" | "canceled-self-trade-prevention" => (
                "LIGHTER_SELF_TRADE_PREVENTED".to_string(),
                LighterTerminalClassification::ExchangeReject,
            ),
            "canceled-invalid-balance" => (
                "LIGHTER_INVALID_BALANCE".to_string(),
                LighterTerminalClassification::ExchangeReject,
            ),
            "canceled-nonce-expired" | "canceled-nonce-too-low" => (
                "LIGHTER_NONCE_EXPIRED".to_string(),
                LighterTerminalClassification::ExchangeReject,
            ),
            "canceled-nonce-already-used" | "canceled-duplicate-nonce" => (
                "LIGHTER_NONCE_ALREADY_USED".to_string(),
                LighterTerminalClassification::ExchangeReject,
            ),
            other => (
                format!("LIGHTER_UNKNOWN_TERMINAL:{other}"),
                LighterTerminalClassification::ExchangeReject,
            ),
        };

        Self {
            raw: raw.to_string(),
            normalized,
            classification,
        }
    }
}
