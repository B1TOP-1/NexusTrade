//! WebSocket 工具层：`tokio-tungstenite` 薄包装。
//!
//! 提供连接、心跳、重连、SUBSCRIBE method 发送。

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use nexus_core::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{client_async_tls_with_config, MaybeTlsStream};

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
        eprintln!("[binance-ws] reader task started");
        loop {
            // 连接（10s 超时，避免 DNS/TCP/TLS 无限挂起）。
            let ws = match tokio::time::timeout(
                Duration::from_secs(10),
                connect_with_proxy(&url_owned),
            )
            .await
            {
                Ok(Ok((ws, _))) => ws,
                Ok(Err(e)) => {
                    eprintln!("[binance-ws] connect failed: {e}");
                    tokio::time::sleep(reconnect_delay).await;
                    continue;
                }
                Err(_) => {
                    eprintln!("[binance-ws] connect TIMEOUT to {url_owned}");
                    tokio::time::sleep(reconnect_delay).await;
                    continue;
                }
            };
            eprintln!("[binance-ws] connected to {url_owned}");
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

/// 连接 WebSocket：**直连优先，直连不通再走代理**。
///
/// - 直连成功 → 直接用（VPS 环境：直连快，不受本机代理干扰）。
/// - 直连失败/超时 → 从环境变量读取 HTTPS_PROXY/HTTP_PROXY 走代理。
/// - 无代理或代理也失败 → 返回直连的错误（保留原始错误）。
///
/// 直连超时 `DIRECT_TIMEOUT`：被墙时 TCP/TLS 会挂起，4s 足够判定不可达。
/// 返回 `(WebSocketStream, Response)`，兼容 tokio-tungstenite 的 connect_async。
const DIRECT_TIMEOUT: Duration = Duration::from_secs(4);

async fn connect_with_proxy(
    url: &str,
) -> std::result::Result<
    (
        tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
        tokio_tungstenite::tungstenite::handshake::client::Response,
    ),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let request = url.into_client_request()?;

    // ── 1. 先试直连 ──
    match tokio::time::timeout(DIRECT_TIMEOUT, tokio_tungstenite::connect_async(request.clone())).await {
        Ok(Ok((ws, resp))) => {
            eprintln!("[binance-ws] connected DIRECT");
            return Ok((ws, resp));
        }
        Ok(Err(e)) => {
            eprintln!("[binance-ws] direct connect failed: {e}, falling back to proxy");
        }
        Err(_) => {
            eprintln!("[binance-ws] direct connect TIMEOUT ({DIRECT_TIMEOUT:?}), falling back to proxy");
        }
    }

    // ── 2. 直连失败 → 走代理 ──
    let proxy_url = std::env::var("HTTPS_PROXY")
        .or_else(|_| std::env::var("https_proxy"))
        .or_else(|_| std::env::var("HTTP_PROXY"))
        .or_else(|_| std::env::var("http_proxy"))
        .ok()
        .filter(|s| !s.is_empty());

    let Some(proxy_url) = proxy_url else {
        return Err("direct connect failed and no proxy configured".into());
    };
    eprintln!("[binance-ws] using proxy {proxy_url}");

    // HTTP 代理：先 CONNECT 隧道，再 TLS，再 WebSocket
    let proxy_uri = proxy_url
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let proxy_addr = if let Some(idx) = proxy_uri.rfind(':') {
        proxy_uri.to_string()
    } else {
        format!("{proxy_uri}:7890")
    };

    let host = request
        .uri()
        .host()
        .ok_or("wss url missing host")?
        .to_string();
    let port = request.uri().port_u16().unwrap_or(443);

    // 1. TCP 连代理
    let mut tcp = match TcpStream::connect(&proxy_addr).await {
        Ok(t) => t,
        Err(e) => return Err(format!("proxy tcp connect: {e}").into()),
    };

    // 2. CONNECT 请求
    let connect_req = format!("CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n");
    tcp.write_all(connect_req.as_bytes()).await?;
    tcp.flush().await?;

    // 3. 读响应头（直到 \r\n\r\n）
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    let mut found = false;
    for _ in 0..(8 * 1024) {
        match tcp.read(&mut byte).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                buf.push(byte[0]);
                if buf.ends_with(b"\r\n\r\n") {
                    found = true;
                    break;
                }
            }
        }
    }
    if !found {
        return Err("proxy CONNECT: no response".into());
    }
    let head = String::from_utf8_lossy(&buf);
    if !head.starts_with("HTTP/1.1 200") && !head.starts_with("HTTP/1.0 200") {
        return Err(format!("proxy CONNECT failed: {head}").into());
    }

    // 4. 在已建立隧道上跑 TLS + WebSocket
    eprintln!("[binance-ws] tunnel established, starting TLS+WS");
    let (ws, resp) = client_async_tls_with_config(request, tcp, None, None).await?;
    Ok((ws, resp))
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
