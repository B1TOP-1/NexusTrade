use std::{collections::BTreeSet, fmt, time::Duration};

use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, http::HeaderValue, Message},
    MaybeTlsStream, WebSocketStream,
};

type LighterSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LighterWsEvent {
    Text(String),
    Reconnected,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterWebSocketConfig {
    pub url: String,
    pub heartbeat_interval: Duration,
    pub reconnect_policy: LighterReconnectPolicy,
}

impl LighterWebSocketConfig {
    pub fn new(url: impl Into<String>) -> Result<Self> {
        let url = url.into();
        validate_ws_url(&url)?;
        Ok(Self {
            url,
            heartbeat_interval: Duration::from_secs(60),
            reconnect_policy: LighterReconnectPolicy::default(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LighterReconnectPolicy {
    pub initial_delay: Duration,
    pub maximum_delay: Duration,
    pub backoff_numerator: u32,
    pub backoff_denominator: u32,
    pub maximum_attempts: Option<usize>,
}

impl Default for LighterReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_millis(500),
            maximum_delay: Duration::from_secs(5),
            backoff_numerator: 3,
            backoff_denominator: 2,
            maximum_attempts: None,
        }
    }
}

impl LighterReconnectPolicy {
    #[must_use]
    pub fn delay_for_attempt(self, attempt: usize) -> Duration {
        let mut delay_ms = self.initial_delay.as_millis();
        let maximum_ms = self.maximum_delay.as_millis();
        let denominator = u128::from(self.backoff_denominator.max(1));
        for _ in 0..attempt {
            delay_ms = delay_ms
                .saturating_mul(u128::from(self.backoff_numerator))
                .checked_div(denominator)
                .unwrap_or(maximum_ms)
                .min(maximum_ms);
        }
        Duration::from_millis(u64::try_from(delay_ms).unwrap_or(u64::MAX))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LighterSubscriptionSet {
    public_channels: BTreeSet<String>,
    private_channels: BTreeSet<String>,
}

impl LighterSubscriptionSet {
    pub fn subscribe_public(&mut self, channel: impl Into<String>) -> Result<bool> {
        let channel = validated_channel(channel.into())?;
        Ok(self.public_channels.insert(channel))
    }

    pub fn subscribe_private(&mut self, channel: impl Into<String>) -> Result<bool> {
        let channel = validated_channel(channel.into())?;
        Ok(self.private_channels.insert(channel))
    }

    pub fn unsubscribe_public(&mut self, channel: &str) -> bool {
        self.public_channels.remove(channel)
    }

    pub fn unsubscribe_private(&mut self, channel: &str) -> bool {
        self.private_channels.remove(channel)
    }

    pub fn reconnect_payloads(&self, auth: &str) -> Result<Vec<String>> {
        let mut payloads =
            Vec::with_capacity(self.public_channels.len() + self.private_channels.len());
        for channel in &self.public_channels {
            payloads.push(public_subscription_payload("subscribe", channel)?);
        }
        for channel in &self.private_channels {
            payloads.push(private_subscription_payload("subscribe", channel, auth)?);
        }
        Ok(payloads)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.public_channels.is_empty() && self.private_channels.is_empty()
    }
}

#[derive(Clone)]
pub struct LighterWebSocketClient {
    config: LighterWebSocketConfig,
    subscriptions: LighterSubscriptionSet,
}

impl fmt::Debug for LighterWebSocketClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LighterWebSocketClient")
            .field("url", &self.config.url)
            .field("subscriptions", &self.subscriptions)
            .finish()
    }
}

impl LighterWebSocketClient {
    #[must_use]
    pub fn new(config: LighterWebSocketConfig) -> Self {
        Self {
            config,
            subscriptions: LighterSubscriptionSet::default(),
        }
    }

    #[must_use]
    pub fn subscriptions(&self) -> &LighterSubscriptionSet {
        &self.subscriptions
    }

    pub fn subscriptions_mut(&mut self) -> &mut LighterSubscriptionSet {
        &mut self.subscriptions
    }

    pub async fn connect(&self) -> Result<LighterWebSocketConnection> {
        let mut request = self
            .config
            .url
            .as_str()
            .into_client_request()
            .context("build Lighter WebSocket request")?;
        request.headers_mut().insert(
            "User-Agent",
            HeaderValue::from_static("bybot-rust-lighter/0.1"),
        );
        let (socket, _) = connect_async(request)
            .await
            .with_context(|| format!("connect Lighter WebSocket {}", self.config.url))?;
        Ok(LighterWebSocketConnection {
            socket,
            heartbeat_interval: self.config.heartbeat_interval,
        })
    }

    pub async fn reconnect(
        &self,
        connection: &mut LighterWebSocketConnection,
        auth: &str,
    ) -> Result<LighterWsEvent> {
        let mut replacement = self.connect_with_retry().await?;
        replacement
            .send_all(self.subscriptions.reconnect_payloads(auth)?)
            .await?;
        *connection = replacement;
        Ok(LighterWsEvent::Reconnected)
    }

    async fn connect_with_retry(&self) -> Result<LighterWebSocketConnection> {
        let mut attempt = 0;
        loop {
            match self.connect().await {
                Ok(connection) => return Ok(connection),
                Err(error) => {
                    attempt += 1;
                    if self
                        .config
                        .reconnect_policy
                        .maximum_attempts
                        .is_some_and(|maximum| attempt >= maximum)
                    {
                        return Err(error).context("Lighter WebSocket reconnect exhausted");
                    }
                    tokio::time::sleep(self.config.reconnect_policy.delay_for_attempt(attempt - 1))
                        .await;
                }
            }
        }
    }
}

pub struct LighterWebSocketConnection {
    socket: LighterSocket,
    heartbeat_interval: Duration,
}

impl fmt::Debug for LighterWebSocketConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LighterWebSocketConnection")
            .field("heartbeat_interval", &self.heartbeat_interval)
            .finish_non_exhaustive()
    }
}

impl LighterWebSocketConnection {
    #[must_use]
    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    pub async fn send_raw(&mut self, payload: impl Into<String>) -> Result<()> {
        self.socket
            .send(Message::Text(payload.into().into()))
            .await
            .context("send Lighter WebSocket text frame")
    }

    pub async fn subscribe_public(&mut self, channel: &str) -> Result<()> {
        self.send_raw(public_subscription_payload("subscribe", channel)?)
            .await
    }

    pub async fn subscribe_private(&mut self, channel: &str, auth: &str) -> Result<()> {
        self.send_raw(private_subscription_payload("subscribe", channel, auth)?)
            .await
    }

    pub async fn send_ping(&mut self) -> Result<()> {
        self.socket
            .send(Message::Ping(Vec::new().into()))
            .await
            .context("send Lighter WebSocket ping")
    }

    pub async fn next_event(&mut self) -> Result<LighterWsEvent> {
        loop {
            let Some(message) = self.socket.next().await else {
                return Ok(LighterWsEvent::Closed);
            };
            match message.context("read Lighter WebSocket frame")? {
                Message::Text(text) => return Ok(LighterWsEvent::Text(text.to_string())),
                Message::Ping(payload) => {
                    self.socket
                        .send(Message::Pong(payload))
                        .await
                        .context("send Lighter WebSocket pong")?;
                }
                Message::Close(_) => return Ok(LighterWsEvent::Closed),
                _ => {}
            }
        }
    }

    pub async fn close(&mut self) -> Result<()> {
        self.socket
            .close(None)
            .await
            .context("close Lighter WebSocket")
    }

    async fn send_all(&mut self, payloads: Vec<String>) -> Result<()> {
        for payload in payloads {
            self.send_raw(payload).await?;
        }
        Ok(())
    }
}

pub fn public_subscription_payload(event: &str, channel: &str) -> Result<String> {
    let event = validated_event(event)?;
    let channel = validated_channel(channel.to_owned())?;
    Ok(json!({"type": event, "channel": channel}).to_string())
}

pub fn private_subscription_payload(event: &str, channel: &str, auth: &str) -> Result<String> {
    let event = validated_event(event)?;
    let channel = validated_channel(channel.to_owned())?;
    if auth.trim().is_empty() {
        bail!("Lighter private subscription auth must not be empty");
    }
    Ok(json!({"type": event, "channel": channel, "auth": auth}).to_string())
}

fn validate_ws_url(url: &str) -> Result<()> {
    if url.starts_with("wss://") || url.starts_with("ws://") {
        return Ok(());
    }
    bail!("Lighter WebSocket URL must use ws or wss")
}

fn validated_channel(channel: String) -> Result<String> {
    if channel.trim().is_empty() {
        bail!("Lighter WebSocket channel must not be empty");
    }
    Ok(channel)
}

fn validated_event(event: &str) -> Result<&str> {
    match event {
        "subscribe" | "unsubscribe" => Ok(event),
        _ => bail!("unsupported Lighter subscription event: {event}"),
    }
}
