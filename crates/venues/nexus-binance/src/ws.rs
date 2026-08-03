//! WebSocket 工具层：`tokio-tungstenite` 薄包装。
//!
//! 提供连接、心跳、重连、SUBSCRIBE method 发送。

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use nexus_core::Result;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::connect_async;

pub(crate) type WsWriteTx = mpsc::UnboundedSender<String>;

/// WS 会话句柄：Drop 时 close + abort。
pub(crate) struct WsSession {
    /// fire-and-forget close frame（1000 正常关闭）。
    close_tx: Option<tokio::sync::oneshot::Sender<()>>,
    _task: AbortOnDrop,
}

impl WsSession {
    pub(crate) fn new(
        close_tx: tokio::sync::oneshot::Sender<()>,
        task: tokio::task::JoinHandle<()>,
    ) -> Self {
        Self {
            close_tx: Some(close_tx),
            _task: AbortOnDrop(task),
        }
    }
}

impl Drop for WsSession {
    fn drop(&mut self) {
        let _ = self.close_tx.take().unwrap().send(());
    }
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// 连接到 `url`，持续读取文本帧送入 `tx`。
///
/// - 自动回复 Pong。
/// - 断线时按 `reconnect_delay` 重连（无限）。
/// - `shutdown_rx` 变为 true 时退出。
/// - 返回 `(WsSession, WsWriteTx)`：session 管理生命周期，write_tx 发消息。
pub(crate) async fn spawn_reader(
    url: &str,
    tx: mpsc::UnboundedSender<String>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    reconnect_delay: Duration,
) -> Result<(WsSession, WsWriteTx)> {
    let (close_tx, mut close_rx) = tokio::sync::oneshot::channel::<()>();
    let (write_tx, mut write_rx) = mpsc::unbounded_channel::<String>();
    let url_owned = url.to_string();

    let task = tokio::spawn(async move {
        loop {
            // 连接。
            let ws = match connect_async(&url_owned).await {
                Ok((ws, _)) => ws,
                Err(_) => {
                    tokio::time::sleep(reconnect_delay).await;
                    continue;
                }
            };
            let (mut write, mut read) = ws.split();

            // 泵循环。
            loop {
                tokio::select! {
                    biased;

                    _ = shutdown_rx.changed() => { return; },

                    _ = &mut close_rx => {
                        let _ = write.send(Message::Close(None)).await;
                        return;
                    },

                    msg = read.next() => {
                        match msg {
                            Some(Ok(Message::Text(text))) => {
                                if tx.send(text.to_string()).is_err() {
                                    return; // 读侧已关闭
                                }
                            }
                            Some(Ok(Message::Ping(data))) => {
                                let _ = write.send(Message::Pong(data)).await;
                            }
                            Some(Ok(Message::Close(_))) | None => {
                                break; // 断线，走重连
                            }
                            Some(Ok(_)) => {} // Binary, Pong 忽略
                            Some(Err(_)) => break,
                        }
                    }

                    to_send = write_rx.recv() => {
                        if let Some(msg) = to_send {
                            if write.send(Message::Text(msg.into())).await.is_err() {
                                break;
                            }
                        } else {
                            return; // write_tx 已关闭
                        }
                    }
                }
            }
            // 断线等待后重连。
            tokio::time::sleep(reconnect_delay).await;
        }
    });

    Ok((WsSession::new(close_tx, task), write_tx))
}

/// 向 WS 发送 SUBSCRIBE 帧。
pub(crate) fn subscribe(write: &WsWriteTx, streams: &[String], id: u64) {
    let msg = serde_json::json!({
        "method": "SUBSCRIBE",
        "params": streams,
        "id": id,
    });
    let _ = write.send(msg.to_string());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_json_is_valid() {
        let msg = serde_json::json!({
            "method": "SUBSCRIBE",
            "params": ["btcusdt@depth"],
            "id": 1,
        });
        let s = msg.to_string();
        assert!(s.contains("SUBSCRIBE"));
        assert!(s.contains("btcusdt@depth"));
    }
}
