use std::{collections::HashSet, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use futures_util::StreamExt;
use hypersdk::{
    hypercore::{
        self,
        types::{
            Fill, Incoming, L2Book, OrderStatus, Subscription, UserEvent, UserFundingEntry,
            WsBasicOrder,
        },
        ws::Event,
        Cloid,
    },
    Address,
};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use tokio::{
    sync::broadcast,
    task::JoinHandle,
    time::{timeout, Instant},
};

use crate::{
    local_book::{LocalBookConfig, LocalBookSnapshot, LocalOrderBookModule},
    orderbook::{BookLevel, SnapshotInput},
};

const EVENT_CAPACITY: usize = 4_096;
const STEP_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone)]
pub struct UserStreamConfig {
    symbols: Vec<String>,
    position_dexes: Vec<Option<String>>,
    stale_after_ms: u64,
}

impl UserStreamConfig {
    pub fn new<S, D>(symbols: S, position_dexes: D) -> Result<Self>
    where
        S: IntoIterator,
        S::Item: Into<String>,
        D: IntoIterator<Item = Option<String>>,
    {
        let symbols = symbols.into_iter().map(Into::into).collect::<Vec<_>>();
        if symbols.is_empty() || symbols.iter().any(|symbol| symbol.trim().is_empty()) {
            bail!("user stream requires at least one non-empty symbol");
        }
        let unique = symbols.iter().collect::<HashSet<_>>();
        if unique.len() != symbols.len() {
            bail!("user stream symbols must be unique");
        }
        Ok(Self {
            symbols,
            position_dexes: position_dexes.into_iter().collect(),
            stale_after_ms: 3_000,
        })
    }

    #[must_use]
    pub fn with_stale_after_ms(mut self, stale_after_ms: u64) -> Self {
        self.stale_after_ms = stale_after_ms;
        self
    }
}

#[derive(Debug, Clone)]
pub enum UserStreamEvent {
    Connected {
        at: Instant,
    },
    Disconnected {
        at: Instant,
    },
    Book {
        at: Instant,
        snapshot: LocalBookSnapshot,
    },
    Order {
        at: Instant,
        update: hypersdk::hypercore::OrderUpdate<WsBasicOrder>,
    },
    Fill {
        at: Instant,
        fill: Fill,
    },
    Funding {
        at: Instant,
        funding: UserFundingEntry,
    },
    UserEvent {
        at: Instant,
        event: UserEvent,
    },
    LedgerUpdate {
        at: Instant,
        update: serde_json::Value,
    },
    Position {
        at: Instant,
        coin: String,
        size: Decimal,
    },
    RuntimeError {
        at: Instant,
        message: String,
    },
}

#[derive(Debug, Clone)]
pub struct FillConfirmation {
    pub fill: Fill,
    pub order_ws_at: Instant,
    pub fill_ws_at: Instant,
}

pub struct UserStreamRuntime {
    receiver: broadcast::Receiver<UserStreamEvent>,
    handle: JoinHandle<()>,
}

impl UserStreamRuntime {
    pub fn spawn(user: Address, config: UserStreamConfig) -> Self {
        let (sender, receiver) = broadcast::channel(EVENT_CAPACITY);
        let handle = spawn_runtime(user, config, sender);
        Self { receiver, handle }
    }

    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<UserStreamEvent> {
        self.receiver.resubscribe()
    }

    pub async fn wait_connected(&mut self) -> Result<Instant> {
        match self
            .receive_matching(|event| matches!(event, UserStreamEvent::Connected { .. }))
            .await?
        {
            UserStreamEvent::Connected { at } => Ok(at),
            _ => unreachable!(),
        }
    }

    pub async fn wait_book(&mut self, symbol: &str) -> Result<LocalBookSnapshot> {
        match self
            .receive_matching(
                |event| matches!(event, UserStreamEvent::Book { snapshot, .. } if snapshot.symbol() == symbol),
            )
            .await?
        {
            UserStreamEvent::Book { snapshot, .. } => Ok(snapshot),
            _ => unreachable!(),
        }
    }

    pub async fn wait_order_status<F>(
        &mut self,
        cloid: Cloid,
        oid: u64,
        status_matches: F,
    ) -> Result<Instant>
    where
        F: Fn(OrderStatus) -> bool,
    {
        match self
            .receive_matching(|event| match event {
                UserStreamEvent::Order { update, .. } => {
                    order_matches(&update.order, cloid, oid) && status_matches(update.status)
                }
                _ => false,
            })
            .await?
        {
            UserStreamEvent::Order { at, .. } => Ok(at),
            _ => unreachable!(),
        }
    }

    pub async fn wait_fill_confirmation(
        &mut self,
        cloid: Cloid,
        oid: u64,
        symbol: &str,
    ) -> Result<FillConfirmation> {
        let deadline = Instant::now() + STEP_TIMEOUT;
        let mut order_ws_at = None;
        let mut fill_event = None;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                bail!("timeout waiting for order and fill confirmation oid={oid}");
            }
            let event = timeout(remaining, self.receiver.recv())
                .await
                .context("timeout waiting for user stream event")?
                .context("user stream event channel closed")?;
            match event {
                UserStreamEvent::Order { at, update }
                    if order_matches(&update.order, cloid, oid)
                        && matches!(update.status, OrderStatus::Filled) =>
                {
                    order_ws_at = Some(at);
                }
                UserStreamEvent::Fill { at, fill }
                    if fill.oid == oid
                        && fill.coin == symbol
                        && fill.cloid.is_none_or(|value| value == cloid) =>
                {
                    fill_event = Some((at, fill));
                }
                UserStreamEvent::Disconnected { at } => {
                    bail!("user websocket disconnected while confirming fill at {at:?}");
                }
                UserStreamEvent::RuntimeError { message, .. } => {
                    eprintln!("[HypeUserStream] warning: {message}");
                }
                _ => {}
            }
            if let (Some(order_ws_at), Some((fill_ws_at, fill))) = (order_ws_at, fill_event.clone())
            {
                return Ok(FillConfirmation {
                    fill,
                    order_ws_at,
                    fill_ws_at,
                });
            }
        }
    }

    pub fn stop(self) {
        self.handle.abort();
    }

    async fn receive_matching<F>(&mut self, predicate: F) -> Result<UserStreamEvent>
    where
        F: Fn(&UserStreamEvent) -> bool,
    {
        timeout(STEP_TIMEOUT, async {
            loop {
                let event = self
                    .receiver
                    .recv()
                    .await
                    .context("user stream event channel closed")?;
                match &event {
                    UserStreamEvent::Disconnected { at } => {
                        bail!("user websocket disconnected at {at:?}");
                    }
                    UserStreamEvent::RuntimeError { message, .. } => {
                        eprintln!("[HypeUserStream] warning: {message}");
                    }
                    _ => {}
                }
                if predicate(&event) {
                    return Ok(event);
                }
            }
        })
        .await
        .context("timeout waiting for user stream event")?
    }
}

impl Drop for UserStreamRuntime {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

fn spawn_runtime(
    user: Address,
    config: UserStreamConfig,
    sender: broadcast::Sender<UserStreamEvent>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut books = match LocalOrderBookModule::new(
            config.symbols.clone(),
            LocalBookConfig::new(config.stale_after_ms),
        ) {
            Ok(books) => books,
            Err(error) => {
                let _ = sender.send(UserStreamEvent::RuntimeError {
                    at: Instant::now(),
                    message: error.to_string(),
                });
                return;
            }
        };
        let mut ws = hypercore::mainnet_ws();
        for symbol in config.symbols {
            ws.subscribe(Subscription::L2Book {
                coin: symbol,
                n_sig_figs: None,
                mantissa: None,
                fast: true,
            });
        }
        ws.subscribe(Subscription::OrderUpdates { user });
        ws.subscribe(Subscription::UserFills { user });
        ws.subscribe(Subscription::UserEvents { user });
        ws.subscribe(Subscription::UserFundings { user });
        ws.subscribe(Subscription::UserNonFundingLedgerUpdates { user });
        for dex in config.position_dexes {
            ws.subscribe(Subscription::ClearinghouseState { user, dex });
        }

        while let Some(event) = ws.next().await {
            match event {
                Event::Connected => {
                    books.mark_connected();
                    let _ = sender.send(UserStreamEvent::Connected { at: Instant::now() });
                }
                Event::Disconnected => {
                    books.mark_disconnected();
                    let _ = sender.send(UserStreamEvent::Disconnected { at: Instant::now() });
                }
                Event::Message(message) => handle_message(&mut books, &sender, message),
            }
        }
    })
}

fn handle_message(
    books: &mut LocalOrderBookModule,
    sender: &broadcast::Sender<UserStreamEvent>,
    message: Incoming,
) {
    let at = Instant::now();
    match message {
        Incoming::L2Book(book) => {
            match convert_l2_book(&book, unix_time_ms()).and_then(|snapshot| {
                books
                    .apply_snapshot(&book.coin, snapshot)
                    .map_err(|error| anyhow!(error))
            }) {
                Ok(()) => {
                    if books.top_of_book(&book.coin, unix_time_ms()).is_ok() {
                        if let Ok(snapshot) = books.snapshot(&book.coin) {
                            let _ = sender.send(UserStreamEvent::Book { at, snapshot });
                        }
                    }
                }
                Err(error) => {
                    let _ = sender.send(UserStreamEvent::RuntimeError {
                        at,
                        message: format!("{} order book: {error}", book.coin),
                    });
                }
            }
        }
        Incoming::OrderUpdates(updates) => {
            for update in updates {
                let _ = sender.send(UserStreamEvent::Order { at, update });
            }
        }
        Incoming::UserFills {
            is_snapshot: false,
            fills,
            ..
        } => {
            for fill in fills {
                let _ = sender.send(UserStreamEvent::Fill { at, fill });
            }
        }
        Incoming::UserFundings { fundings, .. } => {
            for funding in fundings {
                let _ = sender.send(UserStreamEvent::Funding { at, funding });
            }
        }
        Incoming::UserEvents(event) => {
            let _ = sender.send(UserStreamEvent::UserEvent { at, event });
        }
        Incoming::UserNonFundingLedgerUpdates { updates, .. } => {
            for update in updates {
                let _ = sender.send(UserStreamEvent::LedgerUpdate { at, update });
            }
        }
        Incoming::ClearinghouseState {
            clearinghouse_state,
            ..
        } => {
            for position in clearinghouse_state.asset_positions {
                let _ = sender.send(UserStreamEvent::Position {
                    at,
                    coin: position.position.coin,
                    size: position.position.szi,
                });
            }
        }
        _ => {}
    }
}

fn order_matches(order: &WsBasicOrder, cloid: Cloid, oid: u64) -> bool {
    order.oid == oid || order.cloid == Some(cloid)
}

fn convert_l2_book(book: &L2Book, received_time_ms: u64) -> Result<SnapshotInput> {
    let bids = book
        .bids()
        .iter()
        .map(convert_level)
        .collect::<Result<Vec<_>>>()?;
    let asks = book
        .asks()
        .iter()
        .map(convert_level)
        .collect::<Result<Vec<_>>>()?;
    Ok(SnapshotInput::new(book.time, received_time_ms, bids, asks))
}

fn convert_level(level: &hypersdk::hypercore::BookLevel) -> Result<BookLevel> {
    Ok(BookLevel::new(
        decimal_to_fixed_8(level.px)?,
        decimal_to_fixed_8(level.sz)?,
        u32::try_from(level.n).context("order count exceeds u32")?,
    ))
}

fn decimal_to_fixed_8(value: Decimal) -> Result<i64> {
    let scaled = value
        .checked_mul(Decimal::from(100_000_000_i64))
        .ok_or_else(|| anyhow!("fixed-point multiplication overflow"))?;
    if !scaled.fract().is_zero() {
        bail!("value has more than eight decimal places: {value}");
    }
    scaled.to_i64().context("fixed-point value exceeds i64")
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
