use serde::{Deserialize, Serialize};
use ustr::Ustr;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GateWsEvent {
    Subscribe,
    Unsubscribe,
    Update,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GateWsRequest {
    pub time: i64,
    pub channel: Ustr,
    pub event: GateWsEvent,
    pub payload: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GateWsMessage {
    pub time: Option<i64>,
    pub time_ms: Option<i64>,
    pub channel: Ustr,
    pub event: GateWsEvent,
    pub result: Option<GateOrderBookResult>,
}

#[derive(Clone, Debug)]
pub enum GateWsEventMessage {
    Message(GateWsMessage),
    /// Raw text that did not parse as a public order-book message (private
    /// channel pushes and WS-API responses are delivered here for the execution
    /// client to parse flexibly).
    Raw(String),
    /// Binary frame (SBE data push on the `/sbe` endpoint; opcode 2 = SBE).
    Binary(Vec<u8>),
    Reconnected,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GateOrderBookResult {
    pub full: Option<bool>,
    pub s: Ustr,
    pub t: Option<i64>,
    #[serde(rename = "U")]
    pub first_update_id: Option<u64>,
    #[serde(rename = "u")]
    pub last_update_id: u64,
    #[serde(default)]
    pub b: Vec<Vec<String>>,
    #[serde(default)]
    pub a: Vec<Vec<String>>,
}
