use nautilus_model::identifiers::{AccountId, TraderId};
use nautilus_network::websocket::TransportBackend;
use serde::{Deserialize, Serialize};

use crate::common::{
    consts::{
        GATE_DEFAULT_DEPTH, GATE_DEFAULT_SETTLE, GATE_HTTP_PUBLIC_URL, GATE_WS_PUBLIC_URL,
    },
    credential::GateCredential,
};

#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
pub struct GateDataClientConfig {
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub base_url_http: Option<String>,
    pub base_url_ws_public: Option<String>,
    #[builder(default = GATE_DEFAULT_SETTLE.to_string())]
    pub settle: String,
    #[builder(default = GATE_DEFAULT_DEPTH)]
    pub depth: u32,
    pub proxy_url: Option<String>,
    #[builder(default = 20)]
    pub heartbeat_interval_secs: u64,
    #[builder(default = 100)]
    pub stale_ms: u64,
    #[builder(default = 500)]
    pub reconnect_ms: u64,
    pub update_instruments_interval_mins: Option<u64>,
    #[builder(default)]
    pub transport_backend: TransportBackend,
}

impl Default for GateDataClientConfig {
    fn default() -> Self {
        Self {
            api_key: None,
            api_secret: None,
            base_url_http: None,
            base_url_ws_public: None,
            settle: GATE_DEFAULT_SETTLE.to_string(),
            depth: GATE_DEFAULT_DEPTH,
            proxy_url: None,
            heartbeat_interval_secs: 20,
            stale_ms: 100,
            reconnect_ms: 500,
            update_instruments_interval_mins: None,
            transport_backend: TransportBackend::default(),
        }
    }
}

/// Configuration for the Gate futures execution client (WS order placement +
/// private channel reception). Private WS-API and channels share the public
/// `/v4/ws/{settle}` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
pub struct GateExecutionClientConfig {
    pub trader_id: TraderId,
    pub account_id: AccountId,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub base_url_http: Option<String>,
    pub base_url_ws: Option<String>,
    #[builder(default = GATE_DEFAULT_SETTLE.to_string())]
    pub settle: String,
    /// Contracts to trade and subscribe private channels for (e.g. `BTC_USDT`).
    #[builder(default)]
    pub contracts: Vec<String>,
    pub proxy_url: Option<String>,
    #[builder(default = 20)]
    pub heartbeat_interval_secs: u64,
    #[builder(default)]
    pub transport_backend: TransportBackend,
}

impl Default for GateExecutionClientConfig {
    fn default() -> Self {
        Self {
            trader_id: TraderId::from("TRADER-000"),
            account_id: AccountId::from("GATE-001"),
            api_key: None,
            api_secret: None,
            base_url_http: None,
            base_url_ws: None,
            settle: GATE_DEFAULT_SETTLE.to_string(),
            contracts: Vec::new(),
            proxy_url: None,
            heartbeat_interval_secs: 20,
            transport_backend: TransportBackend::default(),
        }
    }
}

impl GateExecutionClientConfig {
    #[must_use]
    pub fn new(trader_id: TraderId, account_id: AccountId) -> Self {
        Self {
            trader_id,
            account_id,
            ..Self::default()
        }
    }

    #[must_use]
    pub fn ws_url(&self) -> String {
        self.base_url_ws
            .clone()
            .unwrap_or_else(|| GATE_WS_PUBLIC_URL.to_string())
    }

    #[must_use]
    pub fn http_url(&self) -> String {
        self.base_url_http
            .clone()
            .unwrap_or_else(|| GATE_HTTP_PUBLIC_URL.to_string())
    }

    /// Builds the signing credential when both key and secret are configured.
    #[must_use]
    pub fn credential(&self) -> Option<GateCredential> {
        match (&self.api_key, &self.api_secret) {
            (Some(key), Some(secret)) => {
                Some(GateCredential::new(key.clone(), secret.clone()))
            }
            _ => None,
        }
    }
}

impl GateDataClientConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn ws_public_url(&self) -> String {
        self.base_url_ws_public
            .clone()
            .unwrap_or_else(|| GATE_WS_PUBLIC_URL.to_string())
    }

    #[must_use]
    pub fn http_public_url(&self) -> String {
        self.base_url_http
            .clone()
            .unwrap_or_else(|| GATE_HTTP_PUBLIC_URL.to_string())
    }
}
