use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::{sync::watch, task::JoinHandle};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, http::HeaderValue, Message},
};

use crate::lighter_book::LighterBook;
use crate::model::SCALE;

pub const MAINNET_API_URL: &str = "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks";
pub const MAINNET_WS_URL: &str = "wss://mainnet.zklighter.elliot.ai/stream";
pub const MAINNET_READONLY_WS_URL: &str = "wss://mainnet.zklighter.elliot.ai/stream?readonly=true";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterMarket {
    pub symbol: String,
    pub market_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterBookUpdate {
    pub ticker: String,
    pub market_id: u64,
    pub bid: String,
    pub ask: String,
    pub weighted_bid: Option<String>,
    pub weighted_ask: Option<String>,
    pub timestamp_ns: u64,
    pub nonce: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct LiveLighterConfig {
    pub ticker: String,
    pub market_id: u64,
    pub ws_url: String,
    pub reconnect_delay_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub depth_notional_usd: f64,
}

pub async fn resolve_lighter_market(symbol: &str, api_url: &str) -> Result<LighterMarket, String> {
    let response = reqwest::get(api_url)
        .await
        .map_err(|error| error.to_string())?
        .error_for_status()
        .map_err(|error| error.to_string())?
        .text()
        .await
        .map_err(|error| error.to_string())?;
    parse_lighter_market(&response, symbol)
}

pub fn spawn_live_lighter(
    config: LiveLighterConfig,
) -> (watch::Receiver<Option<LighterBookUpdate>>, JoinHandle<()>) {
    let (sender, receiver) = watch::channel(None);
    let task = tokio::spawn(async move { run_live_lighter_updates(config, sender).await });
    (receiver, task)
}

pub async fn run_live_lighter(config: LiveLighterConfig) -> Result<(), String> {
    let (sender, _receiver) = watch::channel(None);
    run_live_lighter_updates(config, sender).await;
    Ok(())
}

async fn run_live_lighter_updates(
    config: LiveLighterConfig,
    sender: watch::Sender<Option<LighterBookUpdate>>,
) {
    loop {
        if let Err(err) = run_connection(&config, &sender).await {
            let _ = sender.send(None);
            eprintln!("[RustLive][LighterOnly] reconnect: {err}");
            tokio::time::sleep(Duration::from_millis(config.reconnect_delay_ms)).await;
        }
    }
}

async fn run_connection(
    config: &LiveLighterConfig,
    sender: &watch::Sender<Option<LighterBookUpdate>>,
) -> Result<(), String> {
    let mut request = config
        .ws_url
        .as_str()
        .into_client_request()
        .map_err(|err| err.to_string())?;
    request.headers_mut().insert(
        "User-Agent",
        HeaderValue::from_static("bybot-rust-lighter-book/0.1"),
    );
    let (mut ws, _) = connect_async(request)
        .await
        .map_err(|err| err.to_string())?;
    let subscribe = format!(
        "{{\"type\":\"subscribe\",\"channel\":\"order_book/{}\"}}",
        config.market_id
    );
    ws.send(Message::Text(subscribe.into()))
        .await
        .map_err(|err| err.to_string())?;

    let mut book = LighterBook::new();
    let mut heartbeat =
        tokio::time::interval(Duration::from_millis(config.heartbeat_interval_ms.max(1)));
    heartbeat.tick().await;
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                ws.send(Message::Ping(Vec::new().into()))
                    .await
                    .map_err(|err| err.to_string())?;
            }
            message = ws.next() => {
                let Some(message) = message else {
                    return Err("lighter websocket ended".to_string());
                };
                match message.map_err(|err| err.to_string())? {
                    Message::Text(text) if book.apply_json(&text)? => {
                        if let Some(update) = lighter_update(config, &book) {
                            let _ = sender.send(Some(update));
                        }
                    }
                    Message::Text(_) => {}
                    Message::Ping(payload) => ws
                        .send(Message::Pong(payload))
                        .await
                        .map_err(|err| err.to_string())?,
                    Message::Close(_) => return Err("lighter websocket closed".to_string()),
                    _ => {}
                }
            }
        }
    }
}

fn lighter_update(config: &LiveLighterConfig, book: &LighterBook) -> Option<LighterBookUpdate> {
    let (bid, _) = book.book().best_bid_raw()?;
    let (ask, _) = book.book().best_ask_raw()?;
    let weighted_bid = book
        .book()
        .weighted_fill_by_quote(crate::book::FillSide::Sell, config.depth_notional_usd)
        .map(|fill| fill.avg_price.to_string());
    let weighted_ask = book
        .book()
        .weighted_fill_by_quote(crate::book::FillSide::Buy, config.depth_notional_usd)
        .map(|fill| fill.avg_price.to_string());
    Some(LighterBookUpdate {
        ticker: config.ticker.clone(),
        market_id: config.market_id,
        bid: fixed_to_string(bid),
        ask: fixed_to_string(ask),
        weighted_bid,
        weighted_ask,
        timestamp_ns: now_ns(),
        nonce: book.nonce(),
    })
}

fn parse_lighter_market(payload: &str, symbol: &str) -> Result<LighterMarket, String> {
    let root: Value = serde_json::from_str(payload).map_err(|error| error.to_string())?;
    let expected = symbol.trim().to_uppercase();
    root.get("order_books")
        .and_then(Value::as_array)
        .and_then(|markets| {
            markets.iter().find(|market| {
                market.get("symbol").and_then(Value::as_str) == Some(expected.as_str())
                    && market.get("status").and_then(Value::as_str) == Some("active")
            })
        })
        .and_then(|market| {
            Some(LighterMarket {
                symbol: market.get("symbol")?.as_str()?.to_string(),
                market_id: market.get("market_id")?.as_u64()?,
            })
        })
        .ok_or_else(|| format!("active Lighter market not found: {expected}"))
}

fn fixed_to_string(value: i64) -> String {
    let whole = value / SCALE;
    let fraction = value % SCALE;
    if fraction == 0 {
        return whole.to_string();
    }
    let fraction = format!("{fraction:08}").trim_end_matches('0').to_string();
    format!("{whole}.{fraction}")
}

#[cfg(test)]
fn book_state_json(config: &LiveLighterConfig, book: &LighterBook) -> String {
    let local = book.book();
    let (bid, bid_size) = local.best_bid().unwrap_or((0.0, 0.0));
    let (ask, ask_size) = local.best_ask().unwrap_or((0.0, 0.0));
    serde_json::json!({
        "type": "lighter_book",
        "timestamp_ns": now_ns(),
        "ticker": config.ticker,
        "market_id": config.market_id,
        "nonce": book.nonce(),
        "status": local.status().as_str(),
        "ready": local.status().as_str() == "ready",
        "bid": bid,
        "bid_size": bid_size,
        "ask": ask,
        "ask_size": ask_size,
        "bid_depth": local.bid_depth(),
        "ask_depth": local.ask_depth(),
        "bids": local.bid_levels(20).iter().map(|level| serde_json::json!([level.price, level.size])).collect::<Vec<_>>(),
        "asks": local.ask_levels(20).iter().map(|level| serde_json::json!([level.price, level.size])).collect::<Vec<_>>(),
    })
    .to_string()
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    #[test]
    fn emits_lighter_only_book_state() {
        let config = LiveLighterConfig {
            ticker: "BTC".to_string(),
            market_id: 1,
            ws_url: "wss://example.invalid".to_string(),
            reconnect_delay_ms: 1000,
            heartbeat_interval_ms: 20_000,
            depth_notional_usd: 2_000.0,
        };
        let mut book = LighterBook::new();
        book.apply_json(
            r#"{"type":"subscribed/order_book","order_book":{"nonce":10,"bids":[{"price":"99","size":"2"}],"asks":[{"price":"101","size":"3"}]}}"#,
        )
        .unwrap();

        let payload: serde_json::Value =
            serde_json::from_str(&book_state_json(&config, &book)).unwrap();
        assert_eq!(payload["type"], "lighter_book");
        assert_eq!(payload["market_id"], 1);
        assert_eq!(payload["bid"], 99.0);
        assert_eq!(payload["ask"], 101.0);
        assert_eq!(payload["ready"], true);
    }

    #[test]
    fn resolves_active_market_from_official_order_books() {
        let market = parse_lighter_market(
            r#"{"code":200,"order_books":[{"symbol":"ETH","market_id":0,"status":"active"},{"symbol":"BTC","market_id":1,"status":"active"}]}"#,
            "btc",
        )
        .unwrap();

        assert_eq!(market.symbol, "BTC");
        assert_eq!(market.market_id, 1);
    }

    #[test]
    fn live_update_uses_all_visible_depth_when_reference_notional_is_incomplete() {
        let mut book = LighterBook::new();
        book.apply_json(
            r#"{"type":"subscribed/order_book","order_book":{"nonce":10,"bids":[{"price":"100","size":"10"},{"price":"99","size":"20"}],"asks":[{"price":"101","size":"10"},{"price":"102","size":"20"}]}}"#,
        )
        .unwrap();
        let mut config = LiveLighterConfig {
            ticker: "BTC".to_string(),
            market_id: 1,
            ws_url: "wss://example.invalid".to_string(),
            reconnect_delay_ms: 1_000,
            heartbeat_interval_ms: 20_000,
            depth_notional_usd: 2_000.0,
        };

        let complete = lighter_update(&config, &book).unwrap();
        assert!(complete.weighted_bid.is_some());
        assert!(complete.weighted_ask.is_some());

        config.depth_notional_usd = 10_000.0;
        let incomplete = lighter_update(&config, &book).unwrap();
        let weighted_bid = incomplete.weighted_bid.unwrap().parse::<f64>().unwrap();
        let weighted_ask = incomplete.weighted_ask.unwrap().parse::<f64>().unwrap();
        assert!((weighted_bid - 99.33333333333333).abs() < 1e-10);
        assert!((weighted_ask - 101.66666666666667).abs() < 1e-10);
    }

    #[tokio::test]
    async fn quiet_order_book_stays_connected_when_next_nonce_is_continuous() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let _subscribe = socket.next().await.unwrap().unwrap();
            socket
                .send(Message::Text(
                    r#"{"type":"subscribed/order_book","order_book":{"nonce":10,"bids":[{"price":"99","size":"2"}],"asks":[{"price":"101","size":"3"}]}}"#
                        .into(),
                ))
                .await
                .unwrap();
            let ping = tokio::time::timeout(Duration::from_millis(100), socket.next())
                .await
                .expect("client should send a heartbeat while the book is quiet")
                .unwrap()
                .unwrap();
            assert!(matches!(ping, Message::Ping(_)));
            tokio::time::sleep(Duration::from_millis(150)).await;
            socket
                .send(Message::Text(
                    r#"{"type":"update/order_book","order_book":{"begin_nonce":10,"nonce":11,"bids":[{"price":"100","size":"1"}],"asks":[]}}"#
                        .into(),
                ))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        });
        let config = LiveLighterConfig {
            ticker: "WTI".to_string(),
            market_id: 145,
            ws_url: format!("ws://{address}"),
            reconnect_delay_ms: 10,
            heartbeat_interval_ms: 20,
            depth_notional_usd: 2_000.0,
        };
        let (sender, receiver) = watch::channel(None);

        let result = run_connection(&config, &sender).await;

        server.await.unwrap();
        assert!(result.is_err(), "server close should end the connection");
        assert_eq!(
            receiver.borrow().as_ref().and_then(|update| update.nonce),
            Some(11)
        );
    }
}
