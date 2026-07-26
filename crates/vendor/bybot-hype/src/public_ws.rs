use std::{
    collections::HashMap,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use tokio::{sync::watch, task::JoinHandle};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, http::HeaderValue, Message},
};

use crate::{
    local_book::{LocalBookConfig, LocalOrderBookModule},
    orderbook::{BookLevel, BookState, SnapshotInput},
};

pub const MAINNET_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
pub const FIXED_SCALE: i64 = 100_000_000;
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveBookUpdate {
    pub symbol: String,
    pub bid: String,
    pub ask: String,
    pub weighted_bid: Option<String>,
    pub weighted_ask: Option<String>,
    pub exchange_time_ms: u64,
    pub received_time_ms: u64,
}

#[derive(Debug, Clone)]
pub struct LiveBookConfig {
    pub symbol: String,
    pub ws_url: String,
    pub fast: bool,
    pub stale_after_ms: u64,
    pub reconnect_delay: Duration,
    pub heartbeat_interval: Duration,
    pub depth_notional_usd: Decimal,
}

impl LiveBookConfig {
    #[must_use]
    pub fn mainnet(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            ws_url: MAINNET_WS_URL.to_string(),
            fast: true,
            stale_after_ms: 3_000,
            reconnect_delay: Duration::from_millis(500),
            heartbeat_interval: Duration::from_secs(20),
            depth_notional_usd: Decimal::from(2_000),
        }
    }
}

pub fn spawn_live_book(
    config: LiveBookConfig,
) -> (watch::Receiver<Option<LiveBookUpdate>>, JoinHandle<()>) {
    let (sender, receiver) = watch::channel(None);
    let task = tokio::spawn(async move {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut consecutive_failures = 0_u32;
        loop {
            let mut received_snapshot = false;
            if let Err(error) = run_live_connection(&config, &sender, &mut received_snapshot).await
            {
                let _ = sender.send(None);
                if received_snapshot {
                    consecutive_failures = 0;
                }
                let delay = reconnect_delay(config.reconnect_delay, consecutive_failures);
                eprintln!(
                    "[HypeBook] symbol={} reconnect_attempt={} retry_in_ms={} error={error}",
                    config.symbol,
                    consecutive_failures.saturating_add(1),
                    delay.as_millis()
                );
                tokio::time::sleep(delay).await;
                consecutive_failures = consecutive_failures.saturating_add(1);
            }
        }
    });
    (receiver, task)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedBookSnapshot {
    pub coin: String,
    pub snapshot: SnapshotInput,
}

#[derive(Debug, Clone)]
pub struct MonitorConfig {
    pub symbols: Vec<String>,
    pub ws_url: String,
    pub fast: bool,
    pub run_for: Duration,
    pub stale_after_ms: u64,
    pub reconnect_delay: Duration,
    pub heartbeat_interval: Duration,
    pub report_interval: Duration,
}

impl MonitorConfig {
    #[must_use]
    pub fn mainnet(symbols: Vec<String>, run_for: Duration) -> Self {
        Self {
            symbols,
            ws_url: MAINNET_WS_URL.to_string(),
            fast: false,
            run_for,
            stale_after_ms: 10_000,
            reconnect_delay: Duration::from_millis(500),
            heartbeat_interval: Duration::from_secs(20),
            report_interval: Duration::from_secs(5),
        }
    }

    #[must_use]
    pub fn mainnet_fast(symbols: Vec<String>, run_for: Duration) -> Self {
        Self {
            symbols,
            ws_url: MAINNET_WS_URL.to_string(),
            fast: true,
            run_for,
            stale_after_ms: 3_000,
            reconnect_delay: Duration::from_millis(500),
            heartbeat_interval: Duration::from_secs(20),
            report_interval: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MarketReport {
    pub symbol: String,
    pub state: BookState,
    pub updates: u64,
    pub rejected: u64,
    pub min_latency_ms: Option<i64>,
    pub max_latency_ms: Option<i64>,
    pub average_latency_ms: Option<f64>,
    pub best_bid: Option<BookLevel>,
    pub best_ask: Option<BookLevel>,
    pub bid_levels: usize,
    pub ask_levels: usize,
}

#[derive(Debug, Clone)]
pub struct MonitorReport {
    pub elapsed: Duration,
    pub connections: u64,
    pub disconnections: u64,
    pub application_pongs: u64,
    pub markets: Vec<MarketReport>,
}

#[derive(Debug)]
struct MarketRuntime {
    symbol: String,
    updates: u64,
    rejected: u64,
    latency_count: u64,
    latency_sum_ms: i128,
    min_latency_ms: Option<i64>,
    max_latency_ms: Option<i64>,
}

impl MarketRuntime {
    fn new(symbol: String) -> Self {
        Self {
            symbol,
            updates: 0,
            rejected: 0,
            latency_count: 0,
            latency_sum_ms: 0,
            min_latency_ms: None,
            max_latency_ms: None,
        }
    }

    fn record_snapshot(&mut self, books: &mut LocalOrderBookModule, snapshot: SnapshotInput) {
        let latency = signed_latency_ms(snapshot.received_time_ms(), snapshot.exchange_time_ms());
        match books.apply_snapshot(&self.symbol, snapshot) {
            Ok(()) => {
                self.updates += 1;
                self.latency_count += 1;
                self.latency_sum_ms += i128::from(latency);
                self.min_latency_ms = Some(
                    self.min_latency_ms
                        .map_or(latency, |value| value.min(latency)),
                );
                self.max_latency_ms = Some(
                    self.max_latency_ms
                        .map_or(latency, |value| value.max(latency)),
                );
            }
            Err(_) => {
                self.rejected += 1;
            }
        }
    }

    fn report(&self, books: &LocalOrderBookModule) -> Result<MarketReport, String> {
        let book = books
            .snapshot(&self.symbol)
            .map_err(|error| error.to_string())?;
        Ok(MarketReport {
            symbol: self.symbol.clone(),
            state: book.state(),
            updates: self.updates,
            rejected: self.rejected,
            min_latency_ms: self.min_latency_ms,
            max_latency_ms: self.max_latency_ms,
            average_latency_ms: (self.latency_count > 0)
                .then(|| self.latency_sum_ms as f64 / self.latency_count as f64),
            best_bid: book.bids().first().copied(),
            best_ask: book.asks().first().copied(),
            bid_levels: book.bids().len(),
            ask_levels: book.asks().len(),
        })
    }
}

pub fn parse_l2_book_message(
    message: &str,
    received_time_ms: u64,
) -> Result<Option<ParsedBookSnapshot>, String> {
    let payload: Value = serde_json::from_str(message).map_err(|error| error.to_string())?;
    if payload.get("channel").and_then(Value::as_str) != Some("l2Book") {
        return Ok(None);
    }

    let data = payload
        .get("data")
        .and_then(Value::as_object)
        .ok_or_else(|| "l2Book data must be an object".to_string())?;
    let coin = data
        .get("coin")
        .and_then(Value::as_str)
        .ok_or_else(|| "l2Book coin is missing".to_string())?
        .to_string();
    let exchange_time_ms = data
        .get("time")
        .and_then(Value::as_u64)
        .ok_or_else(|| "l2Book time is missing".to_string())?;
    let levels = data
        .get("levels")
        .and_then(Value::as_array)
        .ok_or_else(|| "l2Book levels must be an array".to_string())?;
    if levels.len() != 2 {
        return Err("l2Book levels must contain bids and asks".to_string());
    }

    let bids = parse_levels(&levels[0])?;
    let asks = parse_levels(&levels[1])?;
    Ok(Some(ParsedBookSnapshot {
        coin,
        snapshot: SnapshotInput::new(exchange_time_ms, received_time_ms, bids, asks),
    }))
}

pub async fn monitor_books(config: MonitorConfig) -> Result<MonitorReport, String> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    if config.symbols.is_empty() {
        return Err("at least one symbol is required".to_string());
    }

    let started = Instant::now();
    let deadline = started + config.run_for;
    let mut books = LocalOrderBookModule::new(
        config.symbols.clone(),
        LocalBookConfig::new(config.stale_after_ms),
    )
    .map_err(|error| error.to_string())?;
    let mut runtimes = config
        .symbols
        .iter()
        .cloned()
        .map(|symbol| {
            let runtime = MarketRuntime::new(symbol.clone());
            (symbol, runtime)
        })
        .collect::<HashMap<_, _>>();
    let mut connections = 0_u64;
    let mut disconnections = 0_u64;
    let mut application_pongs = 0_u64;

    while Instant::now() < deadline {
        books.mark_connected();
        connections += 1;

        match run_connection(
            &config,
            deadline,
            &mut books,
            &mut runtimes,
            &mut application_pongs,
        )
        .await
        {
            Ok(()) => break,
            Err(error) => {
                disconnections += 1;
                books.mark_disconnected();
                if Instant::now() >= deadline {
                    break;
                }
                eprintln!("[HypeBook] connection lost: {error}; reconnecting");
                tokio::time::sleep(config.reconnect_delay).await;
            }
        }
    }

    let mut markets = runtimes
        .values()
        .map(|runtime| runtime.report(&books))
        .collect::<Result<Vec<_>, _>>()?;
    markets.sort_by(|left, right| left.symbol.cmp(&right.symbol));
    Ok(MonitorReport {
        elapsed: started.elapsed(),
        connections,
        disconnections,
        application_pongs,
        markets,
    })
}

async fn run_connection(
    config: &MonitorConfig,
    deadline: Instant,
    books: &mut LocalOrderBookModule,
    runtimes: &mut HashMap<String, MarketRuntime>,
    application_pongs: &mut u64,
) -> Result<(), String> {
    let mut request = config
        .ws_url
        .as_str()
        .into_client_request()
        .map_err(|error| error.to_string())?;
    request.headers_mut().insert(
        "User-Agent",
        HeaderValue::from_static("bybot-hype-book/0.1"),
    );
    let (mut websocket, _) = connect_async(request)
        .await
        .map_err(|error| error.to_string())?;

    for symbol in &config.symbols {
        let request = json!({
            "method": "subscribe",
            "subscription": {
                "type": "l2Book",
                "coin": symbol,
                "fast": config.fast,
            }
        });
        websocket
            .send(Message::Text(request.to_string().into()))
            .await
            .map_err(|error| error.to_string())?;
    }

    let mut heartbeat = tokio::time::interval(config.heartbeat_interval);
    heartbeat.tick().await;
    let mut report_tick = tokio::time::interval(config.report_interval);
    report_tick.tick().await;

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline.into()) => return Ok(()),
            _ = heartbeat.tick() => {
                websocket
                    .send(Message::Text(r#"{"method":"ping"}"#.into()))
                    .await
                    .map_err(|error| error.to_string())?;
            }
            _ = report_tick.tick() => {
                print_runtime_status(books, runtimes);
            }
            message = websocket.next() => {
                let Some(message) = message else {
                    return Err("websocket stream ended".to_string());
                };
                match message.map_err(|error| error.to_string())? {
                    Message::Text(text) => {
                        let received_time_ms = now_ms();
                        if text.contains(r#""channel":"pong""#) {
                            *application_pongs += 1;
                            continue;
                        }
                        if let Some(parsed) = parse_l2_book_message(&text, received_time_ms)? {
                            if let Some(runtime) = runtimes.get_mut(&parsed.coin) {
                                runtime.record_snapshot(books, parsed.snapshot);
                            }
                        }
                    }
                    Message::Ping(payload) => {
                        websocket
                            .send(Message::Pong(payload))
                            .await
                            .map_err(|error| error.to_string())?;
                    }
                    Message::Close(frame) => {
                        return Err(format!("server closed websocket: {frame:?}"));
                    }
                    _ => {}
                }
            }
        }
    }
}

fn parse_levels(value: &Value) -> Result<Vec<BookLevel>, String> {
    let levels = value
        .as_array()
        .ok_or_else(|| "order book side must be an array".to_string())?;
    levels
        .iter()
        .map(|level| {
            let level = level
                .as_object()
                .ok_or_else(|| "order book level must be an object".to_string())?;
            let price = parse_fixed_8(value_as_decimal(level.get("px"))?)?;
            let size = parse_fixed_8(value_as_decimal(level.get("sz"))?)?;
            let orders = level
                .get("n")
                .and_then(Value::as_u64)
                .ok_or_else(|| "order count is missing".to_string())?;
            let orders =
                u32::try_from(orders).map_err(|_| "order count is too large".to_string())?;
            Ok(BookLevel::new(price, size, orders))
        })
        .collect()
}

fn value_as_decimal(value: Option<&Value>) -> Result<&str, String> {
    value
        .and_then(Value::as_str)
        .ok_or_else(|| "decimal field must be a string".to_string())
}

fn parse_fixed_8(value: &str) -> Result<i64, String> {
    if value.starts_with('-') {
        return Err("negative order book values are invalid".to_string());
    }
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if fraction.len() > 8 {
        return Err(format!("decimal has more than 8 places: {value}"));
    }
    let whole = whole
        .parse::<i128>()
        .map_err(|error| format!("invalid decimal {value}: {error}"))?;
    let fraction = if fraction.is_empty() {
        0_i128
    } else {
        fraction
            .parse::<i128>()
            .map_err(|error| format!("invalid decimal {value}: {error}"))?
            * 10_i128.pow((8 - fraction.len()) as u32)
    };
    let scaled = whole
        .checked_mul(i128::from(FIXED_SCALE))
        .and_then(|result| result.checked_add(fraction))
        .ok_or_else(|| format!("decimal overflow: {value}"))?;
    i64::try_from(scaled).map_err(|_| format!("decimal overflow: {value}"))
}

fn print_runtime_status(
    books: &mut LocalOrderBookModule,
    runtimes: &HashMap<String, MarketRuntime>,
) {
    let now = now_ms();
    let mut symbols = runtimes.keys().cloned().collect::<Vec<_>>();
    symbols.sort();
    for symbol in symbols {
        let Some(runtime) = runtimes.get(&symbol) else {
            continue;
        };
        let top = books.top_of_book(&symbol, now).ok();
        let state = books
            .snapshot(&symbol)
            .map(|snapshot| snapshot.state())
            .unwrap_or(BookState::Disconnected);
        let tradeable = top.is_some();
        let bid = top
            .map(|value| fixed_to_string(value.best_bid().price()))
            .unwrap_or_else(|| "-".to_string());
        let ask = top
            .map(|value| fixed_to_string(value.best_ask().price()))
            .unwrap_or_else(|| "-".to_string());
        println!(
            "[HypeBook] symbol={} state={:?} tradeable={} updates={} rejected={} bid={} ask={}",
            symbol, state, tradeable, runtime.updates, runtime.rejected, bid, ask,
        );
    }
}

#[must_use]
pub fn fixed_to_string(value: i64) -> String {
    let whole = value / FIXED_SCALE;
    let fraction = value % FIXED_SCALE;
    if fraction == 0 {
        return whole.to_string();
    }
    let fraction = format!("{fraction:08}").trim_end_matches('0').to_string();
    format!("{whole}.{fraction}")
}

fn signed_latency_ms(received_time_ms: u64, exchange_time_ms: u64) -> i64 {
    if received_time_ms >= exchange_time_ms {
        i64::try_from(received_time_ms - exchange_time_ms).unwrap_or(i64::MAX)
    } else {
        -i64::try_from(exchange_time_ms - received_time_ms).unwrap_or(i64::MAX)
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

async fn run_live_connection(
    config: &LiveBookConfig,
    sender: &watch::Sender<Option<LiveBookUpdate>>,
    received_snapshot: &mut bool,
) -> Result<(), String> {
    let symbol = config.symbol.trim().to_string();
    if symbol.is_empty() {
        return Err("symbol cannot be empty".to_string());
    }
    let mut request = config
        .ws_url
        .as_str()
        .into_client_request()
        .map_err(|error| error.to_string())?;
    request.headers_mut().insert(
        "User-Agent",
        HeaderValue::from_static("bybot-hype-book/0.1"),
    );
    let (mut websocket, _) = connect_async(request)
        .await
        .map_err(|error| error.to_string())?;
    websocket
        .send(Message::Text(
            json!({
                "method": "subscribe",
                "subscription": {"type": "l2Book", "coin": symbol, "fast": config.fast}
            })
            .to_string()
            .into(),
        ))
        .await
        .map_err(|error| error.to_string())?;

    let mut books = LocalOrderBookModule::new(
        [config.symbol.clone()],
        LocalBookConfig::new(config.stale_after_ms),
    )
    .map_err(|error| error.to_string())?;
    books.mark_connected();
    let mut last_update = tokio::time::Instant::now();
    let mut heartbeat = tokio::time::interval(config.heartbeat_interval);
    heartbeat.tick().await;
    let mut stale_check =
        tokio::time::interval(Duration::from_millis((config.stale_after_ms / 2).max(100)));
    stale_check.tick().await;

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                websocket.send(Message::Text(r#"{"method":"ping"}"#.into())).await.map_err(|error| error.to_string())?;
            }
            _ = stale_check.tick() => {
                if last_update.elapsed() >= Duration::from_millis(config.stale_after_ms)
                    && books.top_of_book(&config.symbol, now_ms()).is_err()
                {
                    let _ = sender.send(None);
                    return Err(format!(
                        "Hyperliquid order book stale for {} ms",
                        config.stale_after_ms
                    ));
                }
            }
            message = websocket.next() => {
                let Some(message) = message else { return Err("websocket stream ended".to_string()); };
                match message.map_err(|error| error.to_string())? {
                    Message::Text(text) => {
                        let received_time_ms = now_ms();
                        if text.contains(r#""channel":"pong""#) { continue; }
                        let Some(parsed) = parse_l2_book_message(&text, received_time_ms)? else { continue; };
                        if parsed.coin != config.symbol { continue; }
                        books.apply_snapshot(&parsed.coin, parsed.snapshot).map_err(|error| error.to_string())?;
                        last_update = tokio::time::Instant::now();
                        let top = books.top_of_book(&parsed.coin, received_time_ms).map_err(|error| error.to_string())?;
                        *received_snapshot = true;
                        let weighted_bid = books
                            .vwap_for_quote_notional(
                                &parsed.coin,
                                crate::orderbook::BookSide::Bid,
                                config.depth_notional_usd,
                                received_time_ms,
                            )
                            .map_err(|error| error.to_string())?;
                        let weighted_ask = books
                            .vwap_for_quote_notional(
                                &parsed.coin,
                                crate::orderbook::BookSide::Ask,
                                config.depth_notional_usd,
                                received_time_ms,
                            )
                            .map_err(|error| error.to_string())?;
                        let _ = sender.send(Some(live_update_from_top(
                            &parsed.coin,
                            top.exchange_time_ms(),
                            top.received_time_ms(),
                            top.best_bid(),
                            top.best_ask(),
                            weighted_bid,
                            weighted_ask,
                        )));
                    }
                    Message::Ping(payload) => websocket.send(Message::Pong(payload)).await.map_err(|error| error.to_string())?,
                    Message::Close(_) => return Err("websocket closed".to_string()),
                    _ => {}
                }
            }
        }
    }
}

fn reconnect_delay(base: Duration, failure_index: u32) -> Duration {
    let multiplier = 1_u32.checked_shl(failure_index.min(16)).unwrap_or(u32::MAX);
    base.saturating_mul(multiplier).min(MAX_RECONNECT_DELAY)
}

fn live_update_from_top(
    symbol: &str,
    exchange_time_ms: u64,
    received_time_ms: u64,
    bid: BookLevel,
    ask: BookLevel,
    weighted_bid: Option<Decimal>,
    weighted_ask: Option<Decimal>,
) -> LiveBookUpdate {
    LiveBookUpdate {
        symbol: symbol.to_string(),
        bid: fixed_to_string(bid.price()),
        ask: fixed_to_string(ask.price()),
        weighted_bid: weighted_bid.map(|value| value.normalize().to_string()),
        weighted_ask: weighted_ask.map(|value| value.normalize().to_string()),
        exchange_time_ms,
        received_time_ms,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{live_update_from_top, reconnect_delay, Decimal, LiveBookUpdate};

    #[test]
    fn reconnect_delay_uses_capped_exponential_backoff() {
        let base = Duration::from_millis(500);

        assert_eq!(reconnect_delay(base, 0), Duration::from_millis(500));
        assert_eq!(reconnect_delay(base, 1), Duration::from_secs(1));
        assert_eq!(reconnect_delay(base, 2), Duration::from_secs(2));
        assert_eq!(reconnect_delay(base, 10), Duration::from_secs(30));
    }
    use crate::orderbook::BookLevel;

    #[test]
    fn live_update_preserves_fixed_point_prices() {
        let update = live_update_from_top(
            "BTC",
            100,
            110,
            BookLevel::new(12_345_678_900_000, 100_000_000, 1),
            BookLevel::new(12_345_679_000_000, 200_000_000, 1),
            Some(Decimal::new(12_345_678_800_000, 8)),
            Some(Decimal::new(12_345_679_100_000, 8)),
        );

        assert_eq!(
            update,
            LiveBookUpdate {
                symbol: "BTC".to_string(),
                bid: "123456.789".to_string(),
                ask: "123456.79".to_string(),
                weighted_bid: Some("123456.788".to_string()),
                weighted_ask: Some("123456.791".to_string()),
                exchange_time_ms: 100,
                received_time_ms: 110,
            }
        );
    }
}
