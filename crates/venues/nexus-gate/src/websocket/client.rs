use std::{
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use futures_util::Stream;
use nautilus_core::{consts::NAUTILUS_USER_AGENT, time::get_atomic_clock_realtime};
use nautilus_network::{
    RECONNECTED,
    mode::ConnectionMode,
    websocket::{TransportBackend, WebSocketClient, WebSocketConfig, channel_message_handler},
};
use serde_json::json;
use tokio_tungstenite::tungstenite::Message;
use ustr::Ustr;

use crate::{
    common::consts::GATE_WS_CHANNEL_FUTURES_OBU,
    websocket::messages::{GateWsEvent, GateWsEventMessage, GateWsMessage, GateWsRequest},
};

#[derive(Clone)]
pub struct GateWebSocketClient {
    url: String,
    heartbeat: Option<u64>,
    transport_backend: TransportBackend,
    proxy_url: Option<String>,
    client: Option<Arc<WebSocketClient>>,
    out_rx: Option<Arc<tokio::sync::mpsc::UnboundedReceiver<GateWsEventMessage>>>,
    signal: Arc<AtomicBool>,
    extra_headers: Vec<(String, String)>,
}

impl Debug for GateWebSocketClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(GateWebSocketClient))
            .field("url", &self.url)
            .field("heartbeat", &self.heartbeat)
            .finish()
    }
}

impl GateWebSocketClient {
    #[must_use]
    pub fn new(
        url: String,
        heartbeat: u64,
        transport_backend: TransportBackend,
        proxy_url: Option<String>,
    ) -> Self {
        Self {
            url,
            heartbeat: Some(heartbeat),
            transport_backend,
            proxy_url,
            client: None,
            out_rx: None,
            signal: Arc::new(AtomicBool::new(false)),
            extra_headers: Vec::new(),
        }
    }

    /// Adds a handshake header (e.g. [`GATE_WS_SIZE_DECIMAL_HEADER`] on the private
    /// connection so fractional contract sizes are pushed as decimal strings).
    #[must_use]
    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.extra_headers.push((key.to_string(), value.to_string()));
        self
    }

    pub async fn connect(&mut self) -> anyhow::Result<()> {
        self.signal.store(false, Ordering::Relaxed);
        let (raw_handler, mut raw_rx) = channel_message_handler();
        let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel();
        self.out_rx = Some(Arc::new(out_rx));

        let mut headers = default_headers();
        headers.extend(self.extra_headers.iter().cloned());
        let config = WebSocketConfig {
            url: self.url.clone(),
            headers,
            heartbeat: self.heartbeat,
            heartbeat_msg: Some(
                json!({"time": unix_seconds(), "channel": "futures.ping"}).to_string(),
            ),
            reconnect_timeout_ms: Some(5_000),
            reconnect_delay_initial_ms: Some(500),
            reconnect_delay_max_ms: Some(5_000),
            reconnect_backoff_factor: Some(1.5),
            reconnect_jitter_ms: Some(250),
            reconnect_max_attempts: None,
            idle_timeout_ms: None,
            backend: self.transport_backend,
            proxy_url: self.proxy_url.clone(),
        };

        let client =
            WebSocketClient::connect(config, Some(raw_handler), None, None, vec![], None).await?;
        self.client = Some(Arc::new(client));

        let signal = Arc::clone(&self.signal);
        tokio::spawn(async move {
            while !signal.load(Ordering::Relaxed) {
                let Some(raw) = raw_rx.recv().await else {
                    break;
                };
                match raw {
                    Message::Text(text)
                        if text.as_str() == RECONNECTED
                            && out_tx.send(GateWsEventMessage::Reconnected).is_err() =>
                    {
                        break;
                    }
                    Message::Text(text) => match serde_json::from_str::<GateWsMessage>(&text) {
                        Ok(msg) => {
                            if out_tx.send(GateWsEventMessage::Message(msg)).is_err() {
                                break;
                            }
                        }
                        // Private channel pushes / WS-API responses don't match the
                        // public order-book schema; pass them through as Raw.
                        Err(_) => {
                            if out_tx
                                .send(GateWsEventMessage::Raw(text.to_string()))
                                .is_err()
                            {
                                break;
                            }
                        }
                    },
                    // SBE data push (opcode 2): hand the raw bytes to the consumer.
                    Message::Binary(payload)
                        if out_tx
                            .send(GateWsEventMessage::Binary(payload.to_vec()))
                            .is_err() =>
                    {
                        break;
                    }
                    Message::Ping(payload) => {
                        log::trace!("收到 Gate ping 帧: {} 字节", payload.len());
                    }
                    _ => {}
                }
            }
        });

        Ok(())
    }

    pub async fn close(&mut self) -> anyhow::Result<()> {
        self.signal.store(true, Ordering::Relaxed);
        if let Some(client) = &self.client {
            client.disconnect().await;
        }
        self.client = None;
        Ok(())
    }

    #[must_use]
    pub fn is_active(&self) -> bool {
        self.client.as_ref().is_some_and(|client| {
            ConnectionMode::from_atomic(&client.connection_mode_atomic()).is_active()
        }) && !self.signal.load(Ordering::Relaxed)
    }

    pub async fn subscribe_orderbook(&self, stream: &str) -> anyhow::Result<()> {
        self.send_subscription(GateWsEvent::Subscribe, stream).await
    }

    pub async fn unsubscribe_orderbook(&self, stream: &str) -> anyhow::Result<()> {
        self.send_subscription(GateWsEvent::Unsubscribe, stream)
            .await
    }

    /// # Panics
    ///
    /// Panics if the stream receiver was already taken, the client has not been connected, or
    /// another reference to the receiver still exists.
    pub fn stream(&mut self) -> impl Stream<Item = GateWsEventMessage> + use<> {
        let rx = self
            .out_rx
            .take()
            .expect("Stream receiver already taken or client not connected");
        let mut rx = Arc::try_unwrap(rx).expect("Cannot take ownership - other references exist");
        async_stream::stream! {
            while let Some(msg) = rx.recv().await {
                yield msg;
            }
        }
    }

    /// Sends a raw JSON text frame (single write point for the execution client).
    pub async fn send_raw(&self, payload: String) -> anyhow::Result<()> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Gate WebSocket client is not connected"))?;
        client
            .send_text(payload, Some(&[Ustr::from("gate-private")]))
            .await?;
        Ok(())
    }

    /// Authenticates the WS-API session (`futures.login`, signed once).
    pub async fn login(
        &self,
        api_key: &str,
        signature: &str,
        req_id: &str,
        timestamp: i64,
    ) -> anyhow::Result<()> {
        self.send_raw(build_login_envelope(api_key, signature, req_id, timestamp))
            .await
    }

    /// Subscribes to a private channel with `api_key` auth and the given payload
    /// (e.g. `[user_id, contract]` for orders/usertrades/positions, `[user_id]`
    /// for balances).
    pub async fn subscribe_private(
        &self,
        channel: &str,
        payload: &[String],
        api_key: &str,
        signature: &str,
        timestamp: i64,
    ) -> anyhow::Result<()> {
        self.send_raw(build_subscribe_private_envelope(
            channel, payload, api_key, signature, timestamp,
        ))
        .await
    }

    /// Sends a WS-API request (`event:"api"`), e.g. `futures.order_place`.
    pub async fn send_api(
        &self,
        channel: &str,
        req_id: &str,
        req_param: &serde_json::Value,
        timestamp: i64,
    ) -> anyhow::Result<()> {
        self.send_raw(build_api_envelope(channel, req_id, req_param, timestamp))
            .await
    }

    async fn send_subscription(&self, event: GateWsEvent, stream: &str) -> anyhow::Result<()> {
        let request = GateWsRequest {
            time: unix_seconds(),
            channel: Ustr::from(GATE_WS_CHANNEL_FUTURES_OBU),
            event,
            payload: vec![stream.to_string()],
        };
        let payload = serde_json::to_string(&request)?;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Gate WebSocket client is not connected"))?;
        client
            .send_text(payload, Some(&[Ustr::from("gate-subscription")]))
            .await?;
        Ok(())
    }
}

fn default_headers() -> Vec<(String, String)> {
    vec![
        ("Content-Type".to_string(), "application/json".to_string()),
        ("User-Agent".to_string(), NAUTILUS_USER_AGENT.to_string()),
    ]
}

fn unix_seconds() -> i64 {
    (get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000) as i64
}

/// Builds the `futures.login` WS-API envelope (signed session authentication).
#[must_use]
pub fn build_login_envelope(
    api_key: &str,
    signature: &str,
    req_id: &str,
    timestamp: i64,
) -> String {
    json!({
        "time": timestamp,
        "channel": "futures.login",
        "event": "api",
        "payload": {
            "api_key": api_key,
            "signature": signature,
            "timestamp": timestamp.to_string(),
            "req_id": req_id,
        },
    })
    .to_string()
}

/// Builds a WS-API request envelope (`event:"api"`) with `req_id`/`req_param`.
#[must_use]
pub fn build_api_envelope(
    channel: &str,
    req_id: &str,
    req_param: &serde_json::Value,
    timestamp: i64,
) -> String {
    json!({
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
    json!({
        "time": timestamp,
        "channel": channel,
        "event": "subscribe",
        "payload": payload,
        "auth": {"method": "api_key", "KEY": api_key, "SIGN": signature},
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    #[test]
    fn login_envelope_shape() {
        let v: Value =
            serde_json::from_str(&build_login_envelope("k", "sig", "login-1", 1_700_000_000))
                .unwrap();
        assert_eq!(v["channel"], "futures.login");
        assert_eq!(v["event"], "api");
        assert_eq!(v["payload"]["api_key"], "k");
        assert_eq!(v["payload"]["signature"], "sig");
        assert_eq!(v["payload"]["timestamp"], "1700000000");
        assert_eq!(v["payload"]["req_id"], "login-1");
    }

    #[test]
    fn subscribe_private_envelope_shape() {
        let v: Value = serde_json::from_str(&build_subscribe_private_envelope(
            "futures.usertrades",
            &["12345".to_string(), "BTC_USDT".to_string()],
            "k",
            "sig",
            1_700_000_000,
        ))
        .unwrap();
        assert_eq!(v["channel"], "futures.usertrades");
        assert_eq!(v["event"], "subscribe");
        assert_eq!(v["payload"][0], "12345");
        assert_eq!(v["payload"][1], "BTC_USDT");
        assert_eq!(v["auth"]["method"], "api_key");
        assert_eq!(v["auth"]["KEY"], "k");
        assert_eq!(v["auth"]["SIGN"], "sig");
    }

    #[test]
    fn api_envelope_shape() {
        let req = json!({"contract": "BTC_USDT", "size": -1, "price": "0", "tif": "ioc"});
        let v: Value = serde_json::from_str(&build_api_envelope(
            "futures.order_place",
            "order-1",
            &req,
            1_700_000_000,
        ))
        .unwrap();
        assert_eq!(v["channel"], "futures.order_place");
        assert_eq!(v["event"], "api");
        assert_eq!(v["payload"]["req_id"], "order-1");
        assert_eq!(v["payload"]["req_param"]["contract"], "BTC_USDT");
        assert_eq!(v["payload"]["req_param"]["size"], -1);
    }
}
