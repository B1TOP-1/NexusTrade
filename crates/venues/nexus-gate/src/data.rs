use std::{
    collections::BTreeMap,
    str::FromStr,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use ahash::{AHashMap, AHashSet};
use async_trait::async_trait;
use futures_util::StreamExt;
use nautilus_common::{
    clients::DataClient,
    live::try_get_data_event_sender,
    messages::{
        DataEvent,
        data::{SubscribeBookDeltas, SubscribeQuotes, UnsubscribeBookDeltas, UnsubscribeQuotes},
    },
};
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    data::{BookOrder, Data, OrderBookDelta, OrderBookDeltas, OrderBookDeltas_API, QuoteTick},
    enums::{BookAction, BookType, OrderSide, RecordFlag},
    identifiers::{ClientId, InstrumentId, Symbol, Venue},
    instruments::{CryptoPerpetual, Instrument, InstrumentAny},
    orderbook::OrderBook,
    types::{Currency, Price, Quantity},
};
use tokio_util::sync::CancellationToken;

use crate::{
    common::consts::GATE_VENUE,
    config::GateDataClientConfig,
    http::client::GateHttpClient,
    instrument::parse_gate_futures_contract,
    websocket::{
        client::GateWebSocketClient,
        messages::{GateWsEvent, GateWsEventMessage, GateWsMessage},
        parse::{parse_gate_orderbook_deltas, parse_gate_orderbook_quote},
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateSubscriptionAction {
    Subscribe(String),
    Unsubscribe(String),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GateDataClientStats {
    pub delta_count: u64,
    pub quote_count: u64,
    pub snapshot_count: u64,
    pub no_op_count: u64,
    pub duplicate_or_old_count: u64,
    pub gap_count: u64,
    pub invalid_book_count: u64,
    pub quote_suppressed_count: u64,
    pub resubscribe_count: u64,
    pub reconnect_count: u64,
    pub current_invalid_book_count: usize,
    pub max_stale_duration_ms: u64,
}

#[derive(Debug)]
pub struct GateDataClient {
    client_id: ClientId,
    config: GateDataClientConfig,
    http_client: GateHttpClient,
    is_connected: bool,
    state: Arc<Mutex<GateDataClientState>>,
    ws_client: Option<GateWebSocketClient>,
    cancellation_token: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
    data_sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
}

#[derive(Debug, Default)]
struct GateDataClientState {
    book_depths: AHashMap<InstrumentId, u32>,
    quote_depths: AHashMap<InstrumentId, u32>,
    stream_refs: AHashMap<String, usize>,
    local_last_ids: AHashMap<InstrumentId, u64>,
    book_states: AHashMap<InstrumentId, GateOrderBookState>,
    local_books: AHashMap<InstrumentId, OrderBook>,
    invalid_books: AHashSet<InstrumentId>,
    gate_prices: AHashMap<(InstrumentId, OrderSide, String), Price>,
    gate_books: AHashMap<InstrumentId, GateLocalBook>,
    last_quotes: AHashMap<InstrumentId, QuoteTick>,
    instruments: AHashMap<InstrumentId, InstrumentAny>,
    planned_actions: Vec<GateSubscriptionAction>,
    stats: GateDataClientStats,
    stale_since_ms: AHashMap<InstrumentId, i64>,
}

#[derive(Debug, Default)]
struct GateLocalBook {
    bids: BTreeMap<String, GateLocalLevel>,
    asks: BTreeMap<String, GateLocalLevel>,
}

#[derive(Debug, Clone, Copy)]
struct GateLocalLevel {
    price: Price,
    size: Quantity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum GateOrderBookState {
    Uninitialized,
    Ready { last_update_id: u64 },
    Stale { reason: String },
    Resubscribing { reason: String },
}

impl GateDataClient {
    pub fn new(client_id: ClientId, config: GateDataClientConfig) -> anyhow::Result<Self> {
        validate_depth(config.depth)?;
        let http_client = GateHttpClient::new(
            Some(config.http_public_url()),
            Some(10),
            config.proxy_url.clone(),
        )?;
        Ok(Self {
            client_id,
            config,
            http_client,
            is_connected: false,
            state: Arc::new(Mutex::new(GateDataClientState::default())),
            ws_client: None,
            cancellation_token: CancellationToken::new(),
            task: None,
            data_sender: try_get_data_event_sender().unwrap_or_else(|| {
                let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
                sender
            }),
        })
    }

    #[must_use]
    pub fn planned_actions(&self) -> Vec<GateSubscriptionAction> {
        self.lock_state().planned_actions.clone()
    }

    #[must_use]
    pub fn stats(&self) -> GateDataClientStats {
        let state = self.lock_state();
        let mut stats = state.stats;
        stats.current_invalid_book_count = state.invalid_books.len();
        stats
    }

    pub fn set_local_last_id_for_test(&mut self, instrument_id: InstrumentId, last_id: u64) {
        let mut state = self.lock_state();
        state.local_last_ids.insert(instrument_id, last_id);
        state.book_states.insert(
            instrument_id,
            GateOrderBookState::Ready {
                last_update_id: last_id,
            },
        );
    }

    pub fn add_instrument_for_test(&mut self, instrument: InstrumentAny) {
        self.lock_state()
            .instruments
            .insert(instrument.id(), instrument);
    }

    pub fn handle_ws_message_for_test(&mut self, message: &GateWsMessage) -> anyhow::Result<()> {
        let mut state = self.lock_state();
        Self::handle_ws_message(&mut state, &self.data_sender, message).map(|_| ())
    }

    pub fn handle_ws_message_actions_for_test(
        &mut self,
        message: &GateWsMessage,
    ) -> anyhow::Result<Vec<GateSubscriptionAction>> {
        let mut state = self.lock_state();
        Self::handle_ws_message(&mut state, &self.data_sender, message)
    }

    pub fn handle_sequence_for_test(
        &mut self,
        instrument_id: InstrumentId,
        first_update_id: u64,
        last_update_id: u64,
    ) {
        let mut state = self.lock_state();
        Self::handle_sequence(&mut state, instrument_id, first_update_id, last_update_id);
    }

    pub fn handle_reconnected_for_test(&mut self) {
        let actions = {
            let mut state = self.lock_state();
            Self::handle_reconnected(&mut state)
        };
        for action in actions {
            self.execute_action(&action);
        }
    }

    pub fn stats_for_test(&self) -> GateDataClientStats {
        self.stats()
    }

    pub fn local_book_side_depths_for_test(
        &self,
        instrument_id: InstrumentId,
    ) -> Option<(usize, usize)> {
        let state = self.lock_state();
        let book = state.local_books.get(&instrument_id)?;
        Some((book.bids(None).count(), book.asks(None).count()))
    }

    pub fn best_bid_ask_for_test(&self, instrument_id: InstrumentId) -> Option<(Price, Price)> {
        let state = self.lock_state();
        if state.invalid_books.contains(&instrument_id) {
            return None;
        }
        let (bid_price, _, ask_price, _) = Self::gate_book_bbo(&state, instrument_id)?;
        Some((bid_price, ask_price))
    }

    pub fn best_bid_ask_with_sizes_for_test(
        &self,
        instrument_id: InstrumentId,
    ) -> Option<(Price, Quantity, Price, Quantity)> {
        let state = self.lock_state();
        if state.invalid_books.contains(&instrument_id) {
            return None;
        }
        Self::gate_book_bbo(&state, instrument_id)
    }

    pub fn local_book_ready_for_test(&self, instrument_id: InstrumentId) -> bool {
        let state = self.lock_state();
        Self::local_book_ready(&state, instrument_id)
    }

    pub fn last_quote_for_test(&self, instrument_id: InstrumentId) -> Option<QuoteTick> {
        self.lock_state().last_quotes.get(&instrument_id).copied()
    }

    pub fn local_last_update_id_for_test(&self, instrument_id: InstrumentId) -> Option<u64> {
        self.lock_state()
            .local_last_ids
            .get(&instrument_id)
            .copied()
    }

    fn lock_state(&self) -> MutexGuard<'_, GateDataClientState> {
        self.state
            .lock()
            .expect("Gate data client state lock poisoned")
    }

    fn handle_sequence(
        state: &mut GateDataClientState,
        instrument_id: InstrumentId,
        first_update_id: u64,
        last_update_id: u64,
    ) {
        let current = Self::last_ready_update_id(state, instrument_id);
        if let Some(current) = current
            && first_update_id != current + 1
        {
            state.stats.gap_count += 1;
            Self::resubscribe_instrument(
                state,
                instrument_id,
                format!(
                    "sequence gap: expected {}, received {}",
                    current + 1,
                    first_update_id
                ),
            );
            return;
        }
        state.local_last_ids.insert(instrument_id, last_update_id);
        state
            .book_states
            .insert(instrument_id, GateOrderBookState::Ready { last_update_id });
    }

    fn resubscribe_instrument(
        state: &mut GateDataClientState,
        instrument_id: InstrumentId,
        reason: String,
    ) {
        let Some(depth) = Self::depth_for_instrument(state, instrument_id) else {
            return;
        };
        let stream = stream_name(instrument_id, depth);
        state.stats.resubscribe_count += 1;
        state.local_books.remove(&instrument_id);
        state.invalid_books.remove(&instrument_id);
        state.gate_books.remove(&instrument_id);
        state
            .gate_prices
            .retain(|(mapped_id, _, _), _| *mapped_id != instrument_id);
        state.last_quotes.remove(&instrument_id);
        state.local_last_ids.remove(&instrument_id);
        state.stale_since_ms.insert(
            instrument_id,
            get_atomic_clock_realtime().get_time_ms() as i64,
        );
        state
            .book_states
            .insert(instrument_id, GateOrderBookState::Resubscribing { reason });
        state
            .planned_actions
            .push(GateSubscriptionAction::Unsubscribe(stream.clone()));
        state
            .planned_actions
            .push(GateSubscriptionAction::Subscribe(stream));
    }

    fn subscribe_stream(state: &mut GateDataClientState, instrument_id: InstrumentId, depth: u32) {
        let stream = stream_name(instrument_id, depth);
        let refs = state.stream_refs.entry(stream.clone()).or_insert(0);
        if *refs == 0 {
            state
                .planned_actions
                .push(GateSubscriptionAction::Subscribe(stream));
        }
        *refs += 1;
    }

    fn unsubscribe_stream(
        state: &mut GateDataClientState,
        instrument_id: InstrumentId,
        depth: u32,
    ) {
        let stream = stream_name(instrument_id, depth);
        if let Some(refs) = state.stream_refs.get_mut(&stream) {
            *refs = refs.saturating_sub(1);
            if *refs == 0 {
                state.stream_refs.remove(&stream);
                state
                    .planned_actions
                    .push(GateSubscriptionAction::Unsubscribe(stream));
            }
        }
    }

    fn depth_for_instrument(
        state: &GateDataClientState,
        instrument_id: InstrumentId,
    ) -> Option<u32> {
        state
            .book_depths
            .get(&instrument_id)
            .or_else(|| state.quote_depths.get(&instrument_id))
            .copied()
    }

    fn last_ready_update_id(
        state: &GateDataClientState,
        instrument_id: InstrumentId,
    ) -> Option<u64> {
        match state.book_states.get(&instrument_id) {
            Some(GateOrderBookState::Ready { last_update_id }) => Some(*last_update_id),
            _ => state.local_last_ids.get(&instrument_id).copied(),
        }
    }

    fn should_accept_update(
        state: &mut GateDataClientState,
        instrument_id: InstrumentId,
        first_update_id: Option<u64>,
        last_update_id: u64,
        is_snapshot: bool,
    ) -> bool {
        if is_snapshot {
            return true;
        }

        let current_state = state
            .book_states
            .get(&instrument_id)
            .cloned()
            .unwrap_or(GateOrderBookState::Uninitialized);

        let GateOrderBookState::Ready {
            last_update_id: current,
        } = current_state
        else {
            return false;
        };

        if last_update_id <= current {
            state.stats.duplicate_or_old_count += 1;
            return false;
        }

        let Some(first_update_id) = first_update_id else {
            state.stats.gap_count += 1;
            state.book_states.insert(
                instrument_id,
                GateOrderBookState::Stale {
                    reason: "delta update missing U".to_string(),
                },
            );
            state.stale_since_ms.insert(
                instrument_id,
                get_atomic_clock_realtime().get_time_ms() as i64,
            );
            return false;
        };

        if first_update_id != current + 1 {
            state.stats.gap_count += 1;
            Self::resubscribe_instrument(
                state,
                instrument_id,
                format!(
                    "sequence gap: expected {}, received {}",
                    current + 1,
                    first_update_id
                ),
            );
            return false;
        }

        true
    }

    fn handle_reconnected(state: &mut GateDataClientState) -> Vec<GateSubscriptionAction> {
        let streams = state
            .stream_refs
            .iter()
            .filter_map(|(stream, refs)| (*refs > 0).then_some(stream.clone()))
            .collect::<Vec<_>>();
        if streams.is_empty() {
            return Vec::new();
        }

        let reason = "websocket reconnected".to_string();
        state.stats.reconnect_count += 1;
        let subscribed_instruments = state
            .book_depths
            .keys()
            .chain(state.quote_depths.keys())
            .copied()
            .collect::<Vec<_>>();
        for instrument_id in subscribed_instruments {
            state.local_books.remove(&instrument_id);
            state.invalid_books.remove(&instrument_id);
            state.gate_books.remove(&instrument_id);
            state
                .gate_prices
                .retain(|(mapped_id, _, _), _| *mapped_id != instrument_id);
            state.last_quotes.remove(&instrument_id);
            state.local_last_ids.remove(&instrument_id);
            state.stale_since_ms.insert(
                instrument_id,
                get_atomic_clock_realtime().get_time_ms() as i64,
            );
            state.book_states.insert(
                instrument_id,
                GateOrderBookState::Resubscribing {
                    reason: reason.clone(),
                },
            );
        }

        let actions = streams
            .into_iter()
            .map(GateSubscriptionAction::Subscribe)
            .collect::<Vec<_>>();
        state.stats.resubscribe_count += actions.len() as u64;
        state.planned_actions.extend(actions.iter().cloned());
        actions
    }

    fn ensure_instrument_cached(
        state: &mut GateDataClientState,
        instrument_id: InstrumentId,
    ) -> anyhow::Result<Option<InstrumentAny>> {
        if state.instruments.contains_key(&instrument_id) {
            return Ok(None);
        }

        let instrument = make_gate_crypto_perpetual(instrument_id)?;
        state.instruments.insert(instrument_id, instrument.clone());
        Ok(Some(instrument))
    }

    fn handle_ws_message(
        state: &mut GateDataClientState,
        data_sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
        message: &GateWsMessage,
    ) -> anyhow::Result<Vec<GateSubscriptionAction>> {
        if message.channel.as_str() != crate::common::consts::GATE_WS_CHANNEL_FUTURES_OBU {
            return Ok(Vec::new());
        }
        if !matches!(message.event, GateWsEvent::Update) {
            return Ok(Vec::new());
        }

        let result = message
            .result
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing Gate order book result"))?;
        let Some(instrument_id) = Self::instrument_id_for_symbol(state, result.s.as_str()) else {
            log::warn!("未知 Gate 订单簿 symbol: {}", result.s);
            return Ok(Vec::new());
        };

        let is_snapshot = result.full.unwrap_or(false);
        let planned_actions_before = state.planned_actions.len();
        if !Self::should_accept_update(
            state,
            instrument_id,
            result.first_update_id,
            result.last_update_id,
            is_snapshot,
        ) {
            return Ok(state.planned_actions[planned_actions_before..].to_vec());
        }

        let Some(instrument) = state.instruments.get(&instrument_id).cloned() else {
            log::warn!("无缓存的 Gate instrument: {instrument_id}");
            return Ok(Vec::new());
        };
        if !is_snapshot && result.b.is_empty() && result.a.is_empty() {
            state.stats.no_op_count += 1;
            state
                .local_last_ids
                .insert(instrument_id, result.last_update_id);
            state.book_states.insert(
                instrument_id,
                GateOrderBookState::Ready {
                    last_update_id: result.last_update_id,
                },
            );
            return Ok(Vec::new());
        }
        let ts_init = get_atomic_clock_realtime().get_time_ns();
        let depth_limit =
            Self::depth_for_instrument(state, instrument_id).map(|depth| depth as usize);

        if state.book_depths.contains_key(&instrument_id) {
            let deltas = Self::parse_gate_orderbook_deltas_with_price_cache(
                state,
                message,
                &instrument,
                ts_init,
                depth_limit,
            )?;
            Self::apply_gate_local_book(state, instrument_id, message, &deltas, depth_limit);
            Self::apply_local_deltas(state, instrument_id, &deltas)?;
            Self::prune_local_book(state, instrument_id, result.last_update_id, ts_init)?;
            Self::record_book_update(state, instrument_id, is_snapshot);
            if !Self::validate_local_book(state, instrument_id) && !is_snapshot {
                state.stats.quote_suppressed_count +=
                    u64::from(state.quote_depths.contains_key(&instrument_id));
                Self::resubscribe_instrument(
                    state,
                    instrument_id,
                    "invalid Gate local book BBO".to_string(),
                );
                return Ok(state.planned_actions[planned_actions_before..].to_vec());
            }
            if !state.invalid_books.contains(&instrument_id) {
                send_data(data_sender, Data::Deltas(OrderBookDeltas_API::new(deltas)));
            }
        } else {
            let deltas = Self::parse_gate_orderbook_deltas_with_price_cache(
                state,
                message,
                &instrument,
                ts_init,
                depth_limit,
            )?;
            Self::apply_gate_local_book(state, instrument_id, message, &deltas, depth_limit);
            Self::apply_local_deltas(state, instrument_id, &deltas)?;
            Self::prune_local_book(state, instrument_id, result.last_update_id, ts_init)?;
            Self::record_book_update(state, instrument_id, is_snapshot);
            if !Self::validate_local_book(state, instrument_id) && !is_snapshot {
                state.stats.quote_suppressed_count +=
                    u64::from(state.quote_depths.contains_key(&instrument_id));
                Self::resubscribe_instrument(
                    state,
                    instrument_id,
                    "invalid Gate local book BBO".to_string(),
                );
                return Ok(state.planned_actions[planned_actions_before..].to_vec());
            }
        }

        state
            .local_last_ids
            .insert(instrument_id, result.last_update_id);
        state.book_states.insert(
            instrument_id,
            GateOrderBookState::Ready {
                last_update_id: result.last_update_id,
            },
        );

        if state.quote_depths.contains_key(&instrument_id) {
            if Self::local_book_ready(state, instrument_id)
                && let Ok(quote) =
                    Self::quote_from_local_book_or_message(state, message, &instrument, ts_init)
            {
                state.last_quotes.insert(instrument_id, quote);
                state.stats.quote_count += 1;
                send_data(data_sender, Data::Quote(quote));
            } else {
                state.stats.quote_suppressed_count += 1;
            }
        }

        Ok(Vec::new())
    }

    fn parse_gate_orderbook_deltas_with_price_cache(
        state: &mut GateDataClientState,
        message: &GateWsMessage,
        instrument: &InstrumentAny,
        ts_init: UnixNanos,
        depth_limit: Option<usize>,
    ) -> anyhow::Result<OrderBookDeltas> {
        let mut deltas = parse_gate_orderbook_deltas(message, instrument, ts_init, depth_limit)?;
        let Some(result) = message.result.as_ref() else {
            return Ok(deltas);
        };
        if result.full.unwrap_or(false) {
            state
                .gate_prices
                .retain(|(mapped_id, _, _), _| *mapped_id != instrument.id());
        }
        let mut delta_index = usize::from(result.full.unwrap_or(false));
        for level in result.b.iter().take(depth_limit.unwrap_or(usize::MAX)) {
            Self::apply_gate_price_cache(
                state,
                instrument.id(),
                OrderSide::Buy,
                level,
                deltas.deltas.get_mut(delta_index),
            );
            delta_index += 1;
        }
        for level in result.a.iter().take(depth_limit.unwrap_or(usize::MAX)) {
            Self::apply_gate_price_cache(
                state,
                instrument.id(),
                OrderSide::Sell,
                level,
                deltas.deltas.get_mut(delta_index),
            );
            delta_index += 1;
        }
        Ok(deltas)
    }

    fn apply_gate_price_cache(
        state: &mut GateDataClientState,
        instrument_id: InstrumentId,
        side: OrderSide,
        level: &[String],
        delta: Option<&mut OrderBookDelta>,
    ) {
        let Some(delta) = delta else {
            return;
        };
        let Some(raw_price) = level.first() else {
            return;
        };
        let Some(raw_size) = level.get(1) else {
            return;
        };
        let key = (instrument_id, side, raw_price.clone());
        if raw_size == "0" {
            if let Some(cached_price) = state.gate_prices.remove(&key) {
                delta.order.price = cached_price;
            }
        } else {
            state.gate_prices.insert(key, delta.order.price);
        }
    }

    fn apply_gate_local_book(
        state: &mut GateDataClientState,
        instrument_id: InstrumentId,
        message: &GateWsMessage,
        deltas: &OrderBookDeltas,
        depth_limit: Option<usize>,
    ) {
        let Some(result) = message.result.as_ref() else {
            return;
        };
        let book = state.gate_books.entry(instrument_id).or_default();
        if result.full.unwrap_or(false) {
            book.bids.clear();
            book.asks.clear();
        }
        let mut delta_index = usize::from(result.full.unwrap_or(false));
        for level in result.b.iter().take(depth_limit.unwrap_or(usize::MAX)) {
            if let Some(raw_price) = level.first()
                && let Some(delta) = deltas.deltas.get(delta_index)
            {
                if delta.order.size.is_zero() {
                    book.bids.remove(raw_price);
                } else {
                    book.bids.insert(
                        raw_price.clone(),
                        GateLocalLevel {
                            price: delta.order.price,
                            size: delta.order.size,
                        },
                    );
                }
            }
            delta_index += 1;
        }
        for level in result.a.iter().take(depth_limit.unwrap_or(usize::MAX)) {
            if let Some(raw_price) = level.first()
                && let Some(delta) = deltas.deltas.get(delta_index)
            {
                if delta.order.size.is_zero() {
                    book.asks.remove(raw_price);
                } else {
                    book.asks.insert(
                        raw_price.clone(),
                        GateLocalLevel {
                            price: delta.order.price,
                            size: delta.order.size,
                        },
                    );
                }
            }
            delta_index += 1;
        }
    }

    fn record_book_update(
        state: &mut GateDataClientState,
        instrument_id: InstrumentId,
        is_snapshot: bool,
    ) {
        if is_snapshot {
            state.stats.snapshot_count += 1;
            Self::record_stale_recovery(state, instrument_id);
        } else {
            state.stats.delta_count += 1;
        }
    }

    fn record_stale_recovery(state: &mut GateDataClientState, instrument_id: InstrumentId) {
        let Some(started_ms) = state.stale_since_ms.remove(&instrument_id) else {
            return;
        };
        let finished_ms = get_atomic_clock_realtime().get_time_ms() as i64;
        let stale_duration_ms = finished_ms.saturating_sub(started_ms) as u64;
        state.stats.max_stale_duration_ms =
            state.stats.max_stale_duration_ms.max(stale_duration_ms);
    }

    fn apply_local_deltas(
        state: &mut GateDataClientState,
        instrument_id: InstrumentId,
        deltas: &nautilus_model::data::OrderBookDeltas,
    ) -> anyhow::Result<()> {
        let book = state
            .local_books
            .entry(instrument_id)
            .or_insert_with(|| OrderBook::new(instrument_id, BookType::L2_MBP));
        book.apply_deltas(deltas)?;
        Ok(())
    }

    fn prune_local_book(
        state: &mut GateDataClientState,
        instrument_id: InstrumentId,
        sequence: u64,
        ts_event: UnixNanos,
    ) -> anyhow::Result<()> {
        let Some(depth) = Self::depth_for_instrument(state, instrument_id) else {
            return Ok(());
        };
        let Some(book) = state.local_books.get_mut(&instrument_id) else {
            return Ok(());
        };
        let depth = depth as usize;
        let mut deletes = Vec::new();
        deletes.extend(book.bids(None).skip(depth).filter_map(|level| {
            level.first().map(|order| {
                OrderBookDelta::new(
                    instrument_id,
                    BookAction::Delete,
                    BookOrder::new(
                        OrderSide::Buy,
                        order.price,
                        Quantity::zero(order.size.precision),
                        order.order_id,
                    ),
                    RecordFlag::F_MBP as u8,
                    sequence,
                    ts_event,
                    ts_event,
                )
            })
        }));
        deletes.extend(book.asks(None).skip(depth).filter_map(|level| {
            level.first().map(|order| {
                OrderBookDelta::new(
                    instrument_id,
                    BookAction::Delete,
                    BookOrder::new(
                        OrderSide::Sell,
                        order.price,
                        Quantity::zero(order.size.precision),
                        order.order_id,
                    ),
                    RecordFlag::F_MBP as u8,
                    sequence,
                    ts_event,
                    ts_event,
                )
            })
        }));

        for delta in &deletes {
            book.apply_delta(delta)?;
        }

        Ok(())
    }

    fn validate_local_book(state: &mut GateDataClientState, instrument_id: InstrumentId) -> bool {
        let ready = Self::gate_book_bbo(state, instrument_id)
            .is_some_and(|(bid, _, ask, _)| bid.as_f64() < ask.as_f64());

        if ready {
            state.invalid_books.remove(&instrument_id);
        } else {
            if state.invalid_books.insert(instrument_id) {
                state.stats.invalid_book_count += 1;
            }
            state.last_quotes.remove(&instrument_id);
        }
        ready
    }

    fn local_book_ready(state: &GateDataClientState, instrument_id: InstrumentId) -> bool {
        matches!(
            state.book_states.get(&instrument_id),
            Some(GateOrderBookState::Ready { .. })
        ) && !state.invalid_books.contains(&instrument_id)
    }

    fn gate_book_bbo(
        state: &GateDataClientState,
        instrument_id: InstrumentId,
    ) -> Option<(Price, Quantity, Price, Quantity)> {
        let book = state.gate_books.get(&instrument_id)?;
        let bid = book.bids.values().max_by(|left, right| {
            left.price
                .as_f64()
                .partial_cmp(&right.price.as_f64())
                .unwrap_or(std::cmp::Ordering::Equal)
        })?;
        let ask = book.asks.values().min_by(|left, right| {
            left.price
                .as_f64()
                .partial_cmp(&right.price.as_f64())
                .unwrap_or(std::cmp::Ordering::Equal)
        })?;
        Some((bid.price, bid.size, ask.price, ask.size))
    }

    fn quote_from_local_book_or_message(
        state: &GateDataClientState,
        message: &GateWsMessage,
        instrument: &InstrumentAny,
        ts_init: UnixNanos,
    ) -> anyhow::Result<QuoteTick> {
        let instrument_id = instrument.id();
        if let Some((bid_price, bid_size, ask_price, ask_size)) =
            Self::gate_book_bbo(state, instrument_id)
        {
            let result = message
                .result
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("missing Gate order book result"))?;
            let ts_event = result
                .t
                .or(message.time_ms)
                .map(|ts| crate::common::parse::parse_millis_timestamp(ts, "gate.orderbook"))
                .transpose()?
                .unwrap_or(ts_init);
            return QuoteTick::new_checked(
                instrument_id,
                bid_price,
                ask_price,
                bid_size,
                ask_size,
                ts_event,
                ts_init,
            );
        }
        parse_gate_orderbook_quote(
            message,
            instrument,
            state.last_quotes.get(&instrument_id),
            ts_init,
        )
    }

    fn instrument_id_for_symbol(state: &GateDataClientState, symbol: &str) -> Option<InstrumentId> {
        let symbol = normalize_orderbook_symbol(symbol);
        state
            .instruments
            .keys()
            .copied()
            .find(|instrument_id| raw_symbol(instrument_id.symbol.as_str()) == symbol)
    }

    fn execute_action(&self, action: &GateSubscriptionAction) {
        let Some(ws) = self.ws_client.clone() else {
            return;
        };
        let action = action.clone();
        tokio::spawn(async move {
            let result = match action {
                GateSubscriptionAction::Subscribe(stream) => ws.subscribe_orderbook(&stream).await,
                GateSubscriptionAction::Unsubscribe(stream) => {
                    ws.unsubscribe_orderbook(&stream).await
                }
            };
            if let Err(e) = result {
                log::error!("Gate 订阅动作失败: {e:?}");
            }
        });
    }
}

#[async_trait(?Send)]
impl DataClient for GateDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(*GATE_VENUE)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.is_connected = false;
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        *self.lock_state() = GateDataClientState::default();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.reset()?;
        self.is_connected = false;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.is_connected
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected {
            return Ok(());
        }

        let mut ws = GateWebSocketClient::new(
            self.config.ws_public_url(),
            self.config.heartbeat_interval_secs,
            self.config.transport_backend,
            self.config.proxy_url.clone(),
        );
        ws.connect().await?;
        let mut stream = Box::pin(ws.stream());
        self.ws_client = Some(ws);

        match self
            .http_client
            .get_futures_contracts(self.config.settle.as_str())
            .await
        {
            Ok(contracts) => {
                let ts_init = get_atomic_clock_realtime().get_time_ns();
                let mut instruments = Vec::new();
                for contract in contracts {
                    match parse_gate_futures_contract(&contract, ts_init, ts_init) {
                        Ok(instrument) => instruments.push(instrument),
                        Err(e) => {
                            log::debug!("跳过 Gate 期货合约 {}: {e:?}", contract.name);
                        }
                    }
                }
                {
                    let mut state = self.lock_state();
                    for instrument in &instruments {
                        state
                            .instruments
                            .insert(instrument.id(), instrument.clone());
                    }
                }
                for instrument in instruments {
                    send_instrument(&self.data_sender, instrument);
                }
            }
            Err(e) => {
                log::warn!("连接时从 REST 加载 Gate 期货合约失败: {e:?}");
            }
        }

        let cancellation = self.cancellation_token.clone();
        let data_sender = self.data_sender.clone();
        let state = Arc::clone(&self.state);
        let ws_handle = self.ws_client.clone();
        self.task = Some(tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = cancellation.cancelled() => break,
                    Some(event) = stream.next() => {
                        match event {
                            GateWsEventMessage::Message(message) => {
                                let result = {
                                    let mut state = state
                                        .lock()
                                        .expect("Gate data client state lock poisoned");
                                    Self::handle_ws_message(&mut state, &data_sender, &message)
                                };
                                match result {
                                    Ok(actions) => {
                                        if let Some(ws) = &ws_handle {
                                            for action in actions {
                                                let result = match action {
                                                    GateSubscriptionAction::Subscribe(stream) => {
                                                        ws.subscribe_orderbook(&stream).await
                                                    }
                                                    GateSubscriptionAction::Unsubscribe(stream) => {
                                                        ws.unsubscribe_orderbook(&stream).await
                                                    }
                                                };
                                                if let Err(e) = result {
                                                    log::error!(
                                                        "Gate subscription action failed after WS message: {e:?}"
                                                    );
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("处理 Gate WS 消息失败: {e:?}");
                                    }
                                }
                            }
                            GateWsEventMessage::Raw(_) => {} // private/api frames (execution side)
                            GateWsEventMessage::Binary(_) => {} // SBE frames (execution side)
                            GateWsEventMessage::Reconnected => {
                                log::info!("Gate WebSocket 已重连");
                                let actions = {
                                    let mut state = state
                                        .lock()
                                        .expect("Gate data client state lock poisoned");
                                    Self::handle_reconnected(&mut state)
                                };
                                if let Some(ws) = &ws_handle {
                                    for action in actions {
                                        if let GateSubscriptionAction::Subscribe(stream) = action
                                            && let Err(e) = ws.subscribe_orderbook(&stream).await
                                        {
                                            log::error!(
                                                "Gate reconnect resubscribe failed for {stream}: {e:?}"
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                    else => break,
                }
            }
            drop(data_sender);
        }));

        self.is_connected = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if !self.is_connected {
            return Ok(());
        }
        self.cancellation_token.cancel();
        if let Some(ws) = &mut self.ws_client {
            ws.close().await?;
        }
        if let Some(handle) = self.task.take() {
            let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        }
        self.cancellation_token = CancellationToken::new();
        self.ws_client = None;
        self.is_connected = false;
        Ok(())
    }

    fn subscribe_book_deltas(&mut self, cmd: SubscribeBookDeltas) -> anyhow::Result<()> {
        if cmd.book_type != BookType::L2_MBP {
            anyhow::bail!("Gate only supports L2_MBP order book deltas");
        }
        let depth = cmd
            .depth
            .map_or(self.config.depth, |depth| depth.get() as u32);
        validate_depth(depth)?;

        let (instrument, action) = {
            let mut state = self.lock_state();
            let instrument = Self::ensure_instrument_cached(&mut state, cmd.instrument_id)?;
            let action = if state.book_depths.insert(cmd.instrument_id, depth).is_none() {
                Self::subscribe_stream(&mut state, cmd.instrument_id, depth);
                state.planned_actions.last().cloned()
            } else {
                None
            };
            (instrument, action)
        };
        if let Some(instrument) = instrument {
            send_instrument(&self.data_sender, instrument);
        }
        if let Some(action) = action {
            self.execute_action(&action);
        }
        Ok(())
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        let (instrument, action) = {
            let mut state = self.lock_state();
            let depth = state
                .book_depths
                .get(&cmd.instrument_id)
                .copied()
                .unwrap_or(self.config.depth);
            validate_depth(depth)?;

            let instrument = Self::ensure_instrument_cached(&mut state, cmd.instrument_id)?;
            let action = if state
                .quote_depths
                .insert(cmd.instrument_id, depth)
                .is_none()
            {
                Self::subscribe_stream(&mut state, cmd.instrument_id, depth);
                state.planned_actions.last().cloned()
            } else {
                None
            };
            (instrument, action)
        };
        if let Some(instrument) = instrument {
            send_instrument(&self.data_sender, instrument);
        }
        if let Some(action) = action {
            self.execute_action(&action);
        }
        Ok(())
    }

    fn unsubscribe_book_deltas(&mut self, cmd: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        let action = {
            let mut state = self.lock_state();
            if let Some(depth) = state.book_depths.remove(&cmd.instrument_id) {
                Self::unsubscribe_stream(&mut state, cmd.instrument_id, depth);
                state.planned_actions.last().cloned()
            } else {
                None
            }
        };
        if let Some(action) = action {
            self.execute_action(&action);
        }
        Ok(())
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        let action = {
            let mut state = self.lock_state();
            if let Some(depth) = state.quote_depths.remove(&cmd.instrument_id) {
                Self::unsubscribe_stream(&mut state, cmd.instrument_id, depth);
                state.planned_actions.last().cloned()
            } else {
                None
            }
        };
        if let Some(action) = action {
            self.execute_action(&action);
        }
        Ok(())
    }
}

fn make_gate_crypto_perpetual(instrument_id: InstrumentId) -> anyhow::Result<InstrumentAny> {
    let symbol = raw_symbol(instrument_id.symbol.as_str());
    let (base, quote) = symbol
        .split_once('_')
        .ok_or_else(|| anyhow::anyhow!("invalid Gate futures symbol: {symbol}"))?;
    let quote_currency = Currency::from_str(quote)?;

    Ok(InstrumentAny::CryptoPerpetual(CryptoPerpetual::new(
        instrument_id,
        Symbol::from(symbol),
        Currency::from_str(base)?,
        quote_currency,
        quote_currency,
        false,
        8,
        8,
        Price::from("0.00000001"),
        Quantity::from("0.00000001"),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        UnixNanos::default(),
        UnixNanos::default(),
    )))
}

fn validate_depth(depth: u32) -> anyhow::Result<()> {
    if matches!(depth, 50 | 400) {
        Ok(())
    } else {
        anyhow::bail!("invalid Gate futures.obu depth {depth}; valid values are 50 or 400")
    }
}

fn stream_name(instrument_id: InstrumentId, depth: u32) -> String {
    format!("ob.{}.{}", raw_symbol(instrument_id.symbol.as_str()), depth)
}

fn raw_symbol(symbol: &str) -> &str {
    symbol.rsplit_once('-').map_or(symbol, |(prefix, _)| prefix)
}

fn normalize_orderbook_symbol(symbol: &str) -> &str {
    symbol
        .strip_prefix("ob.")
        .and_then(|symbol| symbol.rsplit_once('.').map(|(contract, _depth)| contract))
        .unwrap_or(symbol)
}

fn send_data(sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>, data: Data) {
    if let Err(e) = sender.send(DataEvent::Data(data)) {
        log::error!("发送 Gate 数据事件失败: {e}");
    }
}

fn send_instrument(
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    instrument: InstrumentAny,
) {
    if let Err(e) = sender.send(DataEvent::Instrument(instrument)) {
        log::error!("发送 Gate instrument 事件失败: {e}");
    }
}
