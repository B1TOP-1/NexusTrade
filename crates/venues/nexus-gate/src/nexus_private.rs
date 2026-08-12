//! Gate 私有流层（nexus-core 重写版）。
//!
//! 实现 `PrivateVenue`：订阅 Gate 私有流（orders/usertrades/positions/balances），
//! 输出统一 `AccountEvent`。
//! 复用纯解析（parse_usertrades/parse_orders_push）→ nexus-core 事件。

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use nexus_core::{
    now_ms, AccountEvent, AccountSnapshot, AccountStream, ClientOrderId, Decimal, Fill, OrderRef,
    OrderState, OrderUpdate, PrivateVenue, Result, Side, Symbol, VenueId,
};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::execution::{parse_orders_push, parse_usertrades};
use crate::nexus_exec::GateVenue;

/// Gate 私有流配置。
#[derive(Debug, Clone)]
pub struct GatePrivateConfig {
    pub ws_url: String,
    pub api_key: String,
    pub api_secret: String,
    pub reconnect_delay: Duration,
}

impl Default for GatePrivateConfig {
    fn default() -> Self {
        Self {
            ws_url: "wss://fx-ws.gateio.ws/v4/ws/usdt".to_string(),
            api_key: String::new(),
            api_secret: String::new(),
            reconnect_delay: Duration::from_millis(500),
        }
    }
}

/// Gate 私有流签名（auth header）。
fn auth_sign(channel: &str, event: &str, timestamp: i64, secret: &str) -> String {
    let sign_string = format!("channel={channel}&event={event}&time={timestamp}");
    let mut mac = hmac_sha512(secret.as_bytes(), sign_string.as_bytes());
    // 返回 hex
    let mut hex = String::with_capacity(128);
    for b in mac.drain(..) {
        hex.push_str(&format!("{:02x}", b));
    }
    hex
}

fn hmac_sha512(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    use sha2::Sha512;
    type HmacSha512 = Hmac<Sha512>;
    let mut mac = HmacSha512::new_from_slice(key).expect("hmac accepts any key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Gate 私有流 venue。
#[derive(Debug)]
pub struct GatePrivate {
    config: GatePrivateConfig,
    _exec: Arc<GateVenue>,
}

impl GatePrivate {
    pub fn new(config: GatePrivateConfig, exec: Arc<GateVenue>) -> Self {
        Self { config, _exec: exec }
    }

    fn subscribe_envelope(&self, channel: &str, timestamp: i64) -> String {
        let signature = auth_sign(channel, "subscribe", timestamp, &self.config.api_secret);
        serde_json::json!({
            "time": timestamp,
            "channel": channel,
            "event": "subscribe",
            "payload": [channel],
            "auth": {
                "method": "api_key",
                "KEY": self.config.api_key,
                "SIGN": signature,
            },
        })
        .to_string()
    }
}

#[async_trait]
impl PrivateVenue for GatePrivate {
    fn venue(&self) -> VenueId {
        VenueId::GATE
    }

    async fn subscribe(&self) -> Result<AccountStream> {
        let (tx, rx) = mpsc::channel(4096);
        let config = self.config.clone();
        let mut write_tx = tx;

        tokio::spawn(async move {
            loop {
                // 裸连接（Binance 用户流教训）
                let ws = match tokio_tungstenite::connect_async(&config.ws_url).await {
                    Ok((ws, _)) => ws,
                    Err(_) => {
                        tokio::time::sleep(config.reconnect_delay).await;
                        continue;
                    }
                };
                let (mut write, mut read) = ws.split();

                // 订阅 4 个私有 channel
                let ts = unix_seconds();
                for ch in ["futures.orders", "futures.usertrades", "futures.positions", "futures.balances"] {
                    let env = {
                        let signature = auth_sign(ch, "subscribe", ts, &config.api_secret);
                        serde_json::json!({
                            "time": ts,
                            "channel": ch,
                            "event": "subscribe",
                            "payload": [ch],
                            "auth": {
                                "method": "api_key",
                                "KEY": config.api_key,
                                "SIGN": signature,
                            },
                        })
                        .to_string()
                    };
                    let _ = write.send(Message::Text(env.into())).await;
                }

                let _ = write_tx.send(AccountEvent::ConnectionState(nexus_core::ConnState::Connected)).await;

                // 收事件
                loop {
                    match tokio::time::timeout(Duration::from_secs(30), read.next()).await {
                        Ok(Some(Ok(Message::Text(text)))) => {
                            let s = text.to_string();
                            if s.contains("futures.usertrades") {
                                for t in parse_usertrades(&s) {
                                    let price = Decimal::from_str(&t.price).unwrap_or_default();
                                    let qty = Decimal::from_str(&t.size).unwrap_or_default();
                                    let fee = Decimal::from_str(&t.fee).unwrap_or_default();
                                    let fill = Fill {
                                        order: OrderRef {
                                            symbol: Symbol::new("", "USDT", "BTC_USDT"),
                                            client_id: ClientOrderId(t.text.clone()),
                                            venue_order_id: Some(t.order_id.clone()),
                                        },
                                        side: if t.role == "taker" { Side::Buy } else { Side::Buy },
                                        price,
                                        qty,
                                        fee,
                                        fee_currency: "USDT".to_string(),
                                        is_maker: t.role == "maker",
                                        venue_ts_ms: now_ms(),
                                        local_recv_ms: now_ms(),
                                    };
                                    if write_tx.send(AccountEvent::Fill(fill)).await.is_err() {
                                        return;
                                    }
                                }
                            } else if s.contains("futures.orders") {
                                for o in parse_orders_push(&s) {
                                    let st = match o.status.as_str() {
                                        "open" => OrderState::Open,
                                        "finished" if o.finish_as.as_deref() == Some("filled") => OrderState::Filled,
                                        "finished" => OrderState::Canceled,
                                        _ => OrderState::Open,
                                    };
                                    let update = OrderUpdate {
                                        client_id: ClientOrderId(o.text.unwrap_or_default()),
                                        symbol: Symbol::new("", "USDT", "BTC_USDT"),
                                        state: st,
                                        filled_qty: Decimal::ZERO,
                                        reason: None,
                                        local_recv_ms: now_ms(),
                                    };
                                    if write_tx.send(AccountEvent::OrderUpdate(update)).await.is_err() {
                                        return;
                                    }
                                }
                            }
                        }
                        Ok(Some(Ok(Message::Ping(data)))) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Ok(Some(Err(_))) | Ok(None) | Err(_) => {
                            let _ = write_tx.send(AccountEvent::ConnectionState(nexus_core::ConnState::Reconnecting)).await;
                            break;
                        }
                        _ => {}
                    }
                }
                tokio::time::sleep(config.reconnect_delay).await;
            }
        });

        Ok(rx)
    }

    async fn snapshot(&self) -> Result<AccountSnapshot> {
        Ok(AccountSnapshot {
            positions: Vec::new(),
            balances: Vec::new(),
            open_orders: Vec::new(),
            local_recv_ms: now_ms(),
        })
    }
}

fn unix_seconds() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
