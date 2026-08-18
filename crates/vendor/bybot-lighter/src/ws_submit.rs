//! Explicit, single-flight WebSocket `jsonapi/sendtx` submission.
//!
//! This is intentionally separate from `LighterExecutionClient`'s HTTP path.
//! An acknowledgement only confirms that the gateway accepted the transaction;
//! private order and trade events remain the authoritative lifecycle evidence.

use std::{fmt, sync::Arc, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};

use anyhow::{Context, Result};
use serde_json::{json, Value};
use tokio::sync::Mutex;

use crate::{
    execution::{parse_lighter_sendtx_ack_envelope, LighterSendTxAck},
    http::LighterSignedTx,
    websocket::{LighterWebSocketClient, LighterWebSocketConfig, LighterWsEvent},
};

const DEFAULT_ACK_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECTED_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LighterWsSubmitTiming {
    pub send_ms: u64,
    pub ack_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterWsSubmitReceipt {
    pub tx_hash: String,
    pub ts_event_ms: u64,
    pub timing: LighterWsSubmitTiming,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LighterWsSubmitError {
    /// The exchange explicitly rejected the transaction. The caller must still
    /// resynchronize its nonce before submitting another transaction.
    Rejected { code: i64, message: String },
    /// The socket failed after a write was attempted, so the exchange may have
    /// received the transaction. Retrying would risk a duplicate submission.
    OutcomeUnknown { cause: String },
    Protocol { message: String },
    NonceResynchronizationRequired,
}

impl fmt::Display for LighterWsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected { code, message } => {
                write!(formatter, "Lighter WS sendTx rejected: code={code} message={message}")
            }
            Self::OutcomeUnknown { cause } => {
                write!(formatter, "Lighter WS sendTx outcome is unknown: {cause}")
            }
            Self::Protocol { message } => write!(formatter, "Lighter WS sendTx protocol error: {message}"),
            Self::NonceResynchronizationRequired => write!(
                formatter,
                "Lighter WS submitter requires nonce resynchronization before another submission"
            ),
        }
    }
}

impl std::error::Error for LighterWsSubmitError {}

#[derive(Debug)]
struct SubmitState {
    connection: Option<crate::websocket::LighterWebSocketConnection>,
    nonce_resynchronization_required: bool,
}

/// A dedicated submission socket. Its mutex intentionally covers both write
/// and ACK wait, because the protocol does not provide a client request ID.
#[derive(Debug, Clone)]
pub struct LighterWsSubmitter {
    websocket: LighterWebSocketClient,
    ack_timeout: Duration,
    state: Arc<Mutex<SubmitState>>,
}

impl LighterWsSubmitter {
    pub async fn connect(config: LighterWebSocketConfig) -> Result<Self> {
        Self::connect_with_timeout(config, DEFAULT_ACK_TIMEOUT).await
    }

    pub async fn connect_with_timeout(
        config: LighterWebSocketConfig,
        ack_timeout: Duration,
    ) -> Result<Self> {
        if ack_timeout.is_zero() {
            anyhow::bail!("Lighter WS sendTx ACK timeout must be non-zero");
        }
        let websocket = LighterWebSocketClient::new(config);
        let connection = connect_and_confirm(&websocket).await?;
        Ok(Self {
            websocket,
            ack_timeout,
            state: Arc::new(Mutex::new(SubmitState {
                connection: Some(connection),
                nonce_resynchronization_required: false,
            })),
        })
    }

    /// Sends a signed transaction and waits for its only permitted in-flight ACK.
    ///
    /// No retry is performed here. Any rejection, ACK timeout, frame read error,
    /// or closure requires the caller to fetch and apply the authoritative nonce
    /// before re-enabling this submitter with `confirm_nonce_resynchronized`.
    pub async fn submit_tx(
        &self,
        tx: &LighterSignedTx,
    ) -> std::result::Result<LighterWsSubmitReceipt, LighterWsSubmitError> {
        let frame = lighter_sendtx_ws_frame(tx).map_err(|error| LighterWsSubmitError::Protocol {
            message: error.to_string(),
        })?;
        let mut state = self.state.lock().await;
        if state.nonce_resynchronization_required {
            return Err(LighterWsSubmitError::NonceResynchronizationRequired);
        }
        let connection = state.connection.as_mut().ok_or_else(|| {
            LighterWsSubmitError::NonceResynchronizationRequired
        })?;

        let started = Instant::now();
        if let Err(error) = connection.send_raw(frame).await {
            state.connection = None;
            state.nonce_resynchronization_required = true;
            return Err(LighterWsSubmitError::OutcomeUnknown {
                cause: error.to_string(),
            });
        }
        let send_ms = elapsed_millis(started);

        let deadline = started.checked_add(self.ack_timeout).unwrap_or(started);
        loop {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                state.connection = None;
                state.nonce_resynchronization_required = true;
                return Err(LighterWsSubmitError::OutcomeUnknown {
                    cause: format!("ACK timed out after {} ms", self.ack_timeout.as_millis()),
                });
            };
            let event = match tokio::time::timeout(remaining, connection.next_event()).await {
                Ok(Ok(event)) => event,
                Ok(Err(error)) => {
                    state.connection = None;
                    state.nonce_resynchronization_required = true;
                    return Err(LighterWsSubmitError::OutcomeUnknown {
                        cause: error.to_string(),
                    });
                }
                Err(_) => {
                    state.connection = None;
                    state.nonce_resynchronization_required = true;
                    return Err(LighterWsSubmitError::OutcomeUnknown {
                        cause: format!("ACK timed out after {} ms", self.ack_timeout.as_millis()),
                    });
                }
            };
            match event {
                LighterWsEvent::Text(payload) => {
                    let Some(ack) = parse_lighter_sendtx_ack_envelope(&payload) else {
                        continue;
                    };
                    return Self::receipt_from_ack(ack, started, send_ms, &mut state);
                }
                LighterWsEvent::Closed => {
                    state.connection = None;
                    state.nonce_resynchronization_required = true;
                    return Err(LighterWsSubmitError::OutcomeUnknown {
                        cause: "WebSocket closed before sendTx ACK".to_owned(),
                    });
                }
                // A submitter connection does not use subscription reconnects.
                LighterWsEvent::Reconnected => continue,
            }
        }
    }

    /// Re-enables the submitter only after the owning execution client has
    /// fetched and applied the exchange's next nonce. It reconnects the socket
    /// and never retries the uncertain prior transaction.
    pub async fn confirm_nonce_resynchronized(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        if !state.nonce_resynchronization_required {
            return Ok(());
        }
        let connection = connect_and_confirm(&self.websocket).await?;
        state.connection = Some(connection);
        state.nonce_resynchronization_required = false;
        Ok(())
    }

    fn receipt_from_ack(
        ack: LighterSendTxAck,
        started: Instant,
        send_ms: u64,
        state: &mut SubmitState,
    ) -> std::result::Result<LighterWsSubmitReceipt, LighterWsSubmitError> {
        let ack_ms = elapsed_millis(started);
        if ack.code != 200 {
            state.nonce_resynchronization_required = true;
            return Err(LighterWsSubmitError::Rejected {
                code: ack.code,
                message: ack.message.unwrap_or_else(|| format!("sendTx failed with code {}", ack.code)),
            });
        }
        let Some(tx_hash) = ack.tx_hash else {
            state.connection = None;
            state.nonce_resynchronization_required = true;
            return Err(LighterWsSubmitError::OutcomeUnknown {
                cause: "successful sendTx ACK omitted transaction hash".to_owned(),
            });
        };
        Ok(LighterWsSubmitReceipt {
            tx_hash,
            ts_event_ms: if ack.ts_event_ms == 0 { now_millis() } else { ack.ts_event_ms },
            timing: LighterWsSubmitTiming { send_ms, ack_ms },
        })
    }
}

async fn connect_and_confirm(
    websocket: &LighterWebSocketClient,
) -> Result<crate::websocket::LighterWebSocketConnection> {
    let mut connection = websocket.connect().await?;
    let event = tokio::time::timeout(CONNECTED_TIMEOUT, connection.next_event())
        .await
        .context("Lighter WS connected frame timed out")??;
    match event {
        LighterWsEvent::Text(payload) => {
            let message: Value = serde_json::from_str(&payload)
                .context("Lighter WS connected frame was not JSON")?;
            if message.get("type").and_then(Value::as_str) != Some("connected") {
                anyhow::bail!("Lighter WS expected connected frame before submit")
            }
            Ok(connection)
        }
        LighterWsEvent::Closed => anyhow::bail!("Lighter WS closed before connected frame"),
        LighterWsEvent::Reconnected => anyhow::bail!("unexpected Lighter WS reconnect before connected frame"),
    }
}

/// Constructs the exact `jsonapi/sendtx` wire envelope used by the legacy
/// Python execution path without exposing signed transaction data in logs.
pub fn lighter_sendtx_ws_frame(tx: &LighterSignedTx) -> Result<String> {
    let tx_info: Value = serde_json::from_str(&tx.tx_info).context("invalid signed Lighter tx_info JSON")?;
    Ok(json!({
        "type": "jsonapi/sendtx",
        "data": {"tx_type": tx.tx_type, "tx_info": tx_info},
    })
    .to_string())
}

fn elapsed_millis(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_util::{SinkExt, StreamExt};
    use serde_json::Value;
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    use crate::{
        http::LighterSignedTx,
        websocket::LighterWebSocketConfig,
        ws_submit::{lighter_sendtx_ws_frame, LighterWsSubmitError, LighterWsSubmitter},
    };

    fn signed_tx() -> LighterSignedTx {
        LighterSignedTx {
            client_order_id: "test".to_owned(),
            client_order_index: Some(1),
            tx_type: 7,
            tx_info: r#"{"nonce":42,"signature":"redacted"}"#.to_owned(),
            price_protection: true,
        }
    }

    #[test]
    fn builds_python_compatible_sendtx_envelope() {
        let frame = lighter_sendtx_ws_frame(&signed_tx()).unwrap();
        let value: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(value["type"], "jsonapi/sendtx");
        assert_eq!(value["data"]["tx_type"], 7);
        assert_eq!(value["data"]["tx_info"]["nonce"], 42);
    }

    #[test]
    fn refuses_non_json_signed_payload() {
        let mut tx = signed_tx();
        tx.tx_info = "not-json".to_owned();
        let error = lighter_sendtx_ws_frame(&tx).unwrap_err();
        assert!(error.to_string().contains("tx_info"));
    }

    #[tokio::test]
    async fn waits_for_connected_then_correlates_single_flight_ack() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            socket
                .send(Message::Text(r#"{"type":"connected"}"#.into()))
                .await
                .unwrap();
            let frame = socket.next().await.unwrap().unwrap();
            let frame = frame.into_text().unwrap();
            assert!(frame.contains("jsonapi/sendtx"));
            socket
                .send(Message::Text(
                    r#"{"type":"jsonapi/sendtx","code":200,"data":{"tx_hash":"0xabc"}}"#
                        .into(),
                ))
                .await
                .unwrap();
        });

        let submitter = LighterWsSubmitter::connect(
            LighterWebSocketConfig::new(format!("ws://{address}")).unwrap(),
        )
        .await
        .unwrap();
        let receipt = submitter.submit_tx(&signed_tx()).await.unwrap();
        assert_eq!(receipt.tx_hash, "0xabc");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn timeout_after_write_is_unknown_and_blocks_another_submission() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            socket
                .send(Message::Text(r#"{"type":"connected"}"#.into()))
                .await
                .unwrap();
            let _ = socket.next().await.unwrap().unwrap();
            for _ in 0..10 {
                if socket
                    .send(Message::Text(r#"{"type":"heartbeat"}"#.into()))
                    .await
                    .is_err()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        let submitter = LighterWsSubmitter::connect_with_timeout(
            LighterWebSocketConfig::new(format!("ws://{address}")).unwrap(),
            Duration::from_millis(35),
        )
        .await
        .unwrap();
        let first_result = tokio::time::timeout(
            Duration::from_secs(1),
            submitter.submit_tx(&signed_tx()),
        )
        .await
        .expect("submitter must honor its ACK deadline");
        assert!(matches!(
            first_result,
            Err(LighterWsSubmitError::OutcomeUnknown { .. })
        ));
        assert_eq!(
            submitter.submit_tx(&signed_tx()).await.unwrap_err(),
            LighterWsSubmitError::NonceResynchronizationRequired
        );
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("mock server must exit after client closes")
            .unwrap();
    }
}
