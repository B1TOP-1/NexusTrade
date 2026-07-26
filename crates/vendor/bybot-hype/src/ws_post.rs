use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use hypersdk::hypercore::types::api::{ActionRequest, Response};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug)]
pub struct WsPostRequest {
    value: Value,
}

impl WsPostRequest {
    #[must_use]
    pub fn action(id: u64, payload: Value) -> Self {
        Self {
            value: json!({
                "method": "post",
                "id": id,
                "request": {"type": "action", "payload": payload}
            }),
        }
    }

    #[must_use]
    pub fn into_value(self) -> Value {
        self.value
    }
}

#[derive(Debug)]
pub struct WsPostResponse {
    channel: String,
    data: WsPostResponseData,
}

#[derive(Debug)]
struct WsPostResponseData {
    id: u64,
    response: WsPostResponseBody,
}

#[derive(Debug)]
struct WsPostResponseBody {
    response_type: String,
    payload: Value,
}

impl WsPostResponse {
    #[must_use]
    pub fn request_id(&self) -> u64 {
        self.data.id
    }

    #[must_use]
    pub fn response_type(&self) -> &str {
        &self.data.response.response_type
    }

    fn into_action_response(self) -> Result<Response> {
        if self.channel != "post" || self.data.response.response_type != "action" {
            bail!("unexpected websocket post response: {self:?}");
        }
        serde_json::from_value(self.data.response.payload).context("decode action response")
    }

    pub fn from_value(value: Value) -> Result<Self> {
        let channel = value
            .get("channel")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing post response channel"))?
            .to_string();
        let data = value
            .get("data")
            .ok_or_else(|| anyhow!("missing post response data"))?;
        let id = data
            .get("id")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("missing post response id"))?;
        let response = data
            .get("response")
            .ok_or_else(|| anyhow!("missing post response body"))?;
        let response_type = response
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing post response type"))?
            .to_string();
        let payload = response
            .get("payload")
            .cloned()
            .ok_or_else(|| anyhow!("missing post response payload"))?;
        Ok(Self {
            channel,
            data: WsPostResponseData {
                id,
                response: WsPostResponseBody {
                    response_type,
                    payload,
                },
            },
        })
    }
}

pub struct WsPostGateway {
    socket: Socket,
    next_request_id: u64,
    response_timeout: Duration,
}

impl WsPostGateway {
    pub async fn connect_mainnet(response_timeout: Duration) -> Result<Self> {
        let (socket, _) = connect_async("wss://api.hyperliquid.xyz/ws")
            .await
            .context("connect Hyperliquid websocket post endpoint")?;
        Ok(Self {
            socket,
            next_request_id: 1,
            response_timeout,
        })
    }

    pub async fn post_action(&mut self, payload: ActionRequest) -> Result<Response> {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1).max(1);
        let payload = serde_json::to_value(payload)?;
        let request = WsPostRequest::action(request_id, payload);
        let text = serde_json::to_string(&request.into_value())?;
        self.socket.send(Message::Text(text.into())).await?;

        timeout(self.response_timeout, async {
            loop {
                let message = self
                    .socket
                    .next()
                    .await
                    .ok_or_else(|| anyhow!("websocket post connection closed"))??;
                match message {
                    Message::Text(text) => {
                        let value: Value = serde_json::from_str(&text)?;
                        if value.get("channel").and_then(Value::as_str) == Some("pong") {
                            continue;
                        }
                        if value.get("channel").and_then(Value::as_str) != Some("post") {
                            continue;
                        }
                        let response = WsPostResponse::from_value(value)?;
                        if response.request_id() == request_id {
                            return response.into_action_response();
                        }
                    }
                    Message::Ping(payload) => {
                        self.socket.send(Message::Pong(payload)).await?;
                    }
                    Message::Close(frame) => {
                        bail!("websocket post connection closed: {frame:?}");
                    }
                    _ => {}
                }
            }
        })
        .await
        .context("websocket post response timeout")?
    }
}
