// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Gate futures execution client (WS order placement + private channel reception).
//!
//! Order placement uses the WS API (`futures.login` once, then `futures.order_place`
//! correlated by `req_id`). The private connection carries the
//! [`crate::common::consts::GATE_WS_SIZE_DECIMAL_HEADER`] so fractional contract
//! sizes survive (otherwise small fills are floored to 0 and vanish).

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use futures_util::StreamExt;
use nautilus_common::{
    clients::ExecutionClient,
    factories::OrderEventFactory,
    live::try_get_exec_event_sender,
    messages::{
        ExecutionEvent,
        execution::{
            CancelOrder, GenerateOrderStatusReports, GeneratePositionStatusReports, SubmitOrder,
        },
    },
};
use nautilus_core::{UUID4, time::get_atomic_clock_realtime};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    accounts::AccountAny,
    enums::{
        LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType, PositionSideSpecified,
        TimeInForce,
    },
    events::AccountState,
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, Symbol, TradeId, Venue, VenueOrderId},
    orders::{Order, OrderAny},
    reports::{ExecutionMassStatus, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Money, Price, Quantity},
};
use tokio_util::sync::CancellationToken;

use crate::{
    common::{
        consts::{GATE_VENUE, GATE_WS_SIZE_DECIMAL_HEADER},
        credential::GateCredential,
    },
    config::GateExecutionClientConfig,
    execution::{
        build_order_req_param, map_gate_tif, parse_orders_push, parse_usertrades,
        parse_ws_api_response,
    },
    http::client::GateHttpClient,
    websocket::{
        client::{GateWebSocketClient, build_api_envelope},
        messages::GateWsEventMessage,
    },
};

type OrderMap = Arc<Mutex<HashMap<ClientOrderId, OrderAny>>>;
type IdMap = Arc<Mutex<HashMap<String, ClientOrderId>>>;

#[derive(Debug)]
pub struct GateExecutionClient {
    core: ExecutionClientCore,
    config: GateExecutionClientConfig,
    credential: Option<GateCredential>,
    factory: OrderEventFactory,
    http_client: GateHttpClient,
    ws_client: Option<GateWebSocketClient>,
    /// Second connection to `/sbe` for binary userTrade fills (dual-source).
    sbe_ws_client: Option<GateWebSocketClient>,
    event_sender: Arc<Mutex<Option<tokio::sync::mpsc::UnboundedSender<ExecutionEvent>>>>,
    /// Single WS write point: order frames flow through this channel to the writer task.
    outbound_tx: Arc<Mutex<Option<tokio::sync::mpsc::UnboundedSender<String>>>>,
    /// In-flight WS-API requests, keyed by `req_id`, for response correlation.
    pending: Arc<Mutex<HashMap<String, PendingRequest>>>,
    /// Order snapshots (by client order id) for building OrderEvents off-thread.
    tracked_orders: OrderMap,
    /// Maps Gate venue order id -> our client order id (fills carry venue id).
    client_by_venue: IdMap,
    /// Maps the Gate `text` field (normalized client id) -> our client order id.
    client_by_text: IdMap,
    /// Deduplicates fills by `trade_id` across sources (JSON + SBE) — first wins.
    seen_trade_ids: Arc<Mutex<std::collections::HashSet<String>>>,
    /// Submit instant per order, for the signal->fill-confirmation latency.
    submit_instants: Arc<Mutex<HashMap<ClientOrderId, std::time::Instant>>>,
    /// Gate server-side processing time (x_out-x_in, ms) per order from the ack.
    server_proc_ms: Arc<Mutex<HashMap<ClientOrderId, f64>>>,
    cancellation_token: CancellationToken,
    req_counter: Arc<AtomicU64>,
}

/// Tracks an in-flight WS-API order request so its ack can be matched by `req_id`.
#[derive(Debug, Clone)]
struct PendingRequest {
    client_order_id: ClientOrderId,
    kind: PendingKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingKind {
    Submit,
    Cancel,
}

impl GateExecutionClient {
    /// Creates a new [`GateExecutionClient`].
    ///
    /// # Panics
    ///
    /// Panics if the Gate HTTP client cannot be constructed from the config.
    #[must_use]
    pub fn new(core: ExecutionClientCore, config: GateExecutionClientConfig) -> Self {
        let credential = config.credential();
        let factory = OrderEventFactory::new(
            core.trader_id,
            core.account_id,
            core.account_type,
            core.base_currency,
        );
        let http_client = GateHttpClient::new(Some(config.http_url()), Some(10), config.proxy_url.clone())
            .expect("Failed to construct Gate HTTP client");
        Self {
            core,
            config,
            credential,
            factory,
            http_client,
            ws_client: None,
            sbe_ws_client: None,
            event_sender: Arc::new(Mutex::new(None)),
            outbound_tx: Arc::new(Mutex::new(None)),
            pending: Arc::new(Mutex::new(HashMap::new())),
            tracked_orders: Arc::new(Mutex::new(HashMap::new())),
            client_by_venue: Arc::new(Mutex::new(HashMap::new())),
            client_by_text: Arc::new(Mutex::new(HashMap::new())),
            seen_trade_ids: Arc::new(Mutex::new(std::collections::HashSet::new())),
            submit_instants: Arc::new(Mutex::new(HashMap::new())),
            server_proc_ms: Arc::new(Mutex::new(HashMap::new())),
            cancellation_token: CancellationToken::new(),
            req_counter: Arc::new(AtomicU64::new(1)),
        }
    }

    #[must_use]
    pub fn config(&self) -> &GateExecutionClientConfig {
        &self.config
    }

    fn set_event_sender(
        &self,
        sender: tokio::sync::mpsc::UnboundedSender<ExecutionEvent>,
    ) {
        *self
            .event_sender
            .lock()
            .expect("Gate event sender lock poisoned") = Some(sender);
    }

    /// Generates a unique WS-API request id (used to correlate responses).
    fn next_req_id(&self, prefix: &str) -> String {
        let n = self.req_counter.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{}-{n}", unix_millis())
    }

    fn track_pending(&self, req_id: &str, client_order_id: ClientOrderId, kind: PendingKind) {
        self.pending
            .lock()
            .expect("Gate pending lock poisoned")
            .insert(
                req_id.to_string(),
                PendingRequest {
                    client_order_id,
                    kind,
                },
            );
    }

    /// Fetches the futures account balance and emits an `AccountState` so the
    /// framework registers the account.
    async fn report_account_state(&self, credential: &GateCredential) -> anyhow::Result<()> {
        let (total, available) = self
            .http_client
            .get_futures_account(&self.config.settle, credential)
            .await?;
        let currency = Currency::from(self.config.settle.to_uppercase().as_str());
        let balance = AccountBalance::new(
            Money::new(total, currency),
            Money::new((total - available).max(0.0), currency),
            Money::new(available, currency),
        );
        let now = get_atomic_clock_realtime().get_time_ns();
        self.generate_account_state(vec![balance], vec![], true, now)?;
        log::info!(
            "Gate 账户状态: total={total} available={available} {}",
            self.config.settle.to_uppercase()
        );
        Ok(())
    }

    /// Reconciliation: open orders across configured contracts -> reports.
    async fn fetch_order_status_reports(&self) -> anyhow::Result<Vec<OrderStatusReport>> {
        let Some(credential) = self.credential.as_ref() else {
            return Ok(Vec::new());
        };
        let now = get_atomic_clock_realtime().get_time_ns();
        let mut reports = Vec::new();
        for contract in &self.config.contracts {
            let orders = self
                .http_client
                .get_open_orders(&self.config.settle, contract, credential)
                .await?;
            let instrument_id = contract_to_instrument_id(contract);
            for order in &orders {
                if let Some(report) =
                    map_gate_order_to_report(self.core.account_id, instrument_id, order, now)
                {
                    reports.push(report);
                }
            }
        }
        log::info!("Gate 对账: {} 个挂单报告", reports.len());
        Ok(reports)
    }

    /// Reconciliation: open positions -> reports (skips flat).
    async fn fetch_position_status_reports(&self) -> anyhow::Result<Vec<PositionStatusReport>> {
        let Some(credential) = self.credential.as_ref() else {
            return Ok(Vec::new());
        };
        let now = get_atomic_clock_realtime().get_time_ns();
        let positions = self
            .http_client
            .get_positions(&self.config.settle, credential)
            .await?;
        let mut reports = Vec::new();
        for position in &positions {
            if let Some(report) = map_gate_position_to_report(self.core.account_id, position, now) {
                reports.push(report);
            }
        }
        log::info!("Gate 对账: {} 个持仓报告", reports.len());
        Ok(reports)
    }

    fn send_outbound(&self, payload: String) -> anyhow::Result<()> {
        let guard = self
            .outbound_tx
            .lock()
            .expect("Gate outbound sender lock poisoned");
        let tx = guard
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Gate outbound channel not connected"))?;
        tx.send(payload)
            .map_err(|e| anyhow::anyhow!("Gate outbound send failed: {e}"))
    }
}

#[async_trait(?Send)]
impl ExecutionClient for GateExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected()
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        self.core.venue
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.core.cache().account_owned(&self.core.account_id)
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: nautilus_core::UnixNanos,
    ) -> anyhow::Result<()> {
        let state = AccountState::new(
            self.core.account_id,
            self.core.account_type,
            balances,
            margins,
            reported,
            UUID4::new(),
            ts_event,
            get_atomic_clock_realtime().get_time_ns(),
            self.core.base_currency,
        );
        if let Some(sender) = self
            .event_sender
            .lock()
            .expect("Gate event sender lock poisoned")
            .as_ref()
            && let Err(e) = sender.send(ExecutionEvent::Account(state))
        {
            log::warn!("发送 Gate 账户状态失败: {e}");
        }
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if let Some(sender) = try_get_exec_event_sender() {
            self.set_event_sender(sender);
        }
        self.core.set_started();
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();
        self.core.set_stopped();
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        let credential = self
            .credential
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Gate API key/secret missing; cannot connect execution"))?;

        // Private connection MUST carry the size-decimal header (see consts).
        let mut ws = GateWebSocketClient::new(
            self.config.ws_url(),
            self.config.heartbeat_interval_secs,
            self.config.transport_backend,
            self.config.proxy_url.clone(),
        )
        .with_header(GATE_WS_SIZE_DECIMAL_HEADER.0, GATE_WS_SIZE_DECIMAL_HEADER.1);
        ws.connect().await?;

        // Authenticate the WS-API session (signed once).
        let timestamp = unix_seconds();
        let req_id = self.next_req_id("login");
        let signature = credential.sign_ws_api("futures.login", "", timestamp);
        ws.login(credential.api_key(), &signature, &req_id, timestamp)
            .await?;

        // Read the login ack to obtain the account user_id, which is required in
        // every private channel subscription payload.
        let mut stream = Box::pin(ws.stream());
        let user_id = read_login_uid(&mut stream).await?;
        log::info!("Gate 执行 WS 登录成功 user_id={user_id}");

        // Subscribe private channels. orders/usertrades/positions are per-contract
        // (payload [user_id, contract]); balances is account-wide ([user_id]).
        let sub_ts = unix_seconds();
        for contract in &self.config.contracts {
            for channel in ["futures.orders", "futures.usertrades", "futures.positions"] {
                let sig = credential.sign_ws_auth(channel, "subscribe", sub_ts);
                ws.subscribe_private(
                    channel,
                    &[user_id.clone(), contract.clone()],
                    credential.api_key(),
                    &sig,
                    sub_ts,
                )
                .await?;
            }
        }
        let bal_sig = credential.sign_ws_auth("futures.balances", "subscribe", sub_ts);
        ws.subscribe_private(
            "futures.balances",
            std::slice::from_ref(&user_id),
            credential.api_key(),
            &bal_sig,
            sub_ts,
        )
        .await?;
        log::info!(
            "Gate 私有频道订阅完成 contracts={:?}",
            self.config.contracts
        );

        // Reconciliation: report the futures account balance so the framework
        // registers the account (otherwise fills are rejected: "no account").
        if let Err(e) = self.report_account_state(&credential).await {
            log::warn!("上报 Gate 账户状态失败: {e}");
        }

        // Single outbound write point: all order frames flow through this task.
        let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        *self
            .outbound_tx
            .lock()
            .expect("Gate outbound sender lock poisoned") = Some(out_tx);
        let ws_writer = ws.clone();
        let cancel_writer = self.cancellation_token.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = cancel_writer.cancelled() => break,
                    msg = out_rx.recv() => match msg {
                        Some(payload) => {
                            if let Err(e) = ws_writer.send_raw(payload).await {
                                log::warn!("Gate 下单帧发送失败: {e}");
                            }
                        }
                        None => break,
                    },
                }
            }
        });

        // Inbound task: parse private channel pushes + WS-API responses into
        // OrderEvents and emit them (the framework FSM/Cache applies the state).
        let cancellation = self.cancellation_token.clone();
        let pending = self.pending.clone();
        let tracked_orders = self.tracked_orders.clone();
        let client_by_venue = self.client_by_venue.clone();
        let client_by_text = self.client_by_text.clone();
        let seen_trade_ids = self.seen_trade_ids.clone();
        let submit_instants = self.submit_instants.clone();
        let server_proc_ms = self.server_proc_ms.clone();
        let factory = self.factory.clone();
        let event_sender = self.event_sender.clone();
        let quote_currency = Currency::from(self.config.settle.to_uppercase().as_str());
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = cancellation.cancelled() => break,
                    Some(event) = stream.next() => match event {
                        GateWsEventMessage::Raw(text) => {
                            log::debug!("[Gate 私有] {text}");
                            let events = convert_raw_to_events(
                                &text,
                                &pending,
                                &tracked_orders,
                                &client_by_venue,
                                &client_by_text,
                                &seen_trade_ids,
                                &submit_instants,
                                &server_proc_ms,
                                &factory,
                                quote_currency,
                            );
                            if let Some(sender) = event_sender
                                .lock()
                                .expect("Gate event sender lock poisoned")
                                .as_ref()
                            {
                                for event in events {
                                    if let Err(e) = sender.send(ExecutionEvent::Order(event)) {
                                        log::warn!("发送 Gate 订单事件失败: {e}");
                                        break;
                                    }
                                }
                            }
                        }
                        GateWsEventMessage::Reconnected => {
                            // TODO(P4): re-login + re-subscribe with fresh auth.
                            log::info!("Gate 执行 WS 重连");
                        }
                        GateWsEventMessage::Binary(_) | GateWsEventMessage::Message(_) => {}
                    },
                    else => break,
                }
            }
        });

        // Second connection to /sbe for binary userTrade fills (dual source).
        // Verification phase: decode + log to confirm the layout matches a live
        // frame before feeding fills into the dedup (see TODO below).
        let sbe_url = format!("{}/sbe", self.config.ws_url());
        let mut sbe_ws = GateWebSocketClient::new(
            sbe_url,
            self.config.heartbeat_interval_secs,
            self.config.transport_backend,
            self.config.proxy_url.clone(),
        )
        .with_header(GATE_WS_SIZE_DECIMAL_HEADER.0, GATE_WS_SIZE_DECIMAL_HEADER.1);
        sbe_ws.connect().await?;
        let sbe_ts = unix_seconds();
        for contract in &self.config.contracts {
            let sig = credential.sign_ws_auth("futures.usertrades", "subscribe", sbe_ts);
            sbe_ws
                .subscribe_private(
                    "futures.usertrades",
                    &[user_id.clone(), contract.clone()],
                    credential.api_key(),
                    &sig,
                    sbe_ts,
                )
                .await?;
        }
        log::info!("Gate SBE 连接已订阅 futures.usertrades (二进制成交源)");
        let mut sbe_stream = Box::pin(sbe_ws.stream());
        let sbe_cancel = self.cancellation_token.clone();
        let sbe_seen = self.seen_trade_ids.clone();
        let sbe_venue = self.client_by_venue.clone();
        let sbe_text = self.client_by_text.clone();
        let sbe_submit = self.submit_instants.clone();
        let sbe_server = self.server_proc_ms.clone();
        let sbe_tracked = self.tracked_orders.clone();
        let sbe_factory = self.factory.clone();
        let sbe_sender = self.event_sender.clone();
        let sbe_currency = Currency::from(self.config.settle.to_uppercase().as_str());
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = sbe_cancel.cancelled() => break,
                    Some(event) = sbe_stream.next() => match event {
                        GateWsEventMessage::Binary(bytes) => match crate::sbe::decode_user_trades(&bytes) {
                            Ok(trades) => {
                                let now = get_atomic_clock_realtime().get_time_ns();
                                for t in trades {
                                    // Feed the shared dedup: SBE or JSON, first wins.
                                    if let Some(event) = build_fill_event(
                                        "SBE", &t.order_id, &t.trade_id, &t.text, &t.size, &t.price, &t.fee, &t.role,
                                        &sbe_seen, &sbe_venue, &sbe_text, &sbe_submit, &sbe_server, &sbe_tracked, &sbe_factory, sbe_currency, now,
                                    ) && let Some(sender) = sbe_sender
                                        .lock()
                                        .expect("Gate event sender lock poisoned")
                                        .as_ref()
                                    {
                                        let _ = sender.send(ExecutionEvent::Order(event));
                                    }
                                }
                            }
                            Err(e) => log::warn!("Gate SBE 解码失败: {e} ({} bytes)", bytes.len()),
                        },
                        GateWsEventMessage::Raw(text) => log::debug!("[Gate SBE/json] {text}"),
                        _ => {}
                    },
                    else => break,
                }
            }
        });
        self.sbe_ws_client = Some(sbe_ws);

        self.ws_client = Some(ws);
        self.core.set_connected();
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();
        if let Some(mut ws) = self.ws_client.take() {
            ws.close().await?;
        }
        if let Some(mut sbe_ws) = self.sbe_ws_client.take() {
            sbe_ws.close().await?;
        }
        self.core.set_disconnected();
        Ok(())
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let order = self.core.get_order(&cmd.client_order_id)?;
        // Gate contract symbol, e.g. "BTC_USDT".
        let contract = order.instrument_id().symbol.to_string();
        // Gate size is an integer contract count; the Nautilus quantity is already
        // in contracts (size_increment = 1).
        let size = order.quantity().as_f64().round() as u64;
        let price = match order.order_type() {
            OrderType::Market => None,
            _ => order.price().map(|p| p.to_string()),
        };
        let tif = map_gate_tif(order.time_in_force(), order.is_post_only())?;
        let client_order_id = order.client_order_id();
        let text = crate::common::credential::normalize_order_text(client_order_id.as_ref());
        let req_param = build_order_req_param(
            &contract,
            order.order_side(),
            size,
            price.as_deref(),
            tif,
            order.is_reduce_only(),
            client_order_id.as_ref(),
        )?;

        // Emit OrderSubmitted up-front so the FSM reaches Submitted before the
        // ack and fill arrive (they race across separate WS connections, and a
        // fast IOC fill can beat the ack). Both Submitted->Accepted and
        // Submitted->Filled are valid, so either arrival order works.
        if let Some(sender) = self
            .event_sender
            .lock()
            .expect("Gate event sender lock poisoned")
            .as_ref()
        {
            let submitted = self
                .factory
                .generate_order_submitted(&order, get_atomic_clock_realtime().get_time_ns());
            let _ = sender.send(ExecutionEvent::Order(submitted));
        }

        // Snapshot the order + text mapping so the inbound task can build events
        // (it runs off the cache thread).
        self.tracked_orders
            .lock()
            .expect("Gate tracked orders lock poisoned")
            .insert(client_order_id, order);
        self.client_by_text
            .lock()
            .expect("Gate client_by_text lock poisoned")
            .insert(text, client_order_id);
        self.submit_instants
            .lock()
            .expect("Gate submit_instants lock poisoned")
            .insert(client_order_id, std::time::Instant::now());

        let req_id = self.next_req_id("order");
        self.track_pending(&req_id, cmd.client_order_id, PendingKind::Submit);
        let envelope =
            build_api_envelope("futures.order_place", &req_id, &req_param, unix_seconds());
        log::debug!("Gate 下单 req_id={req_id} contract={contract} size={size}");
        self.send_outbound(envelope)
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let order = self.core.get_order(&cmd.client_order_id)?;
        // Gate 的 order_id 接受「数字 venue 单号」或「客户自定义 text(t-...)」。
        // 有 venue 单号用它(精确); 没有(下单确认未回)则回退用 text, 让快速撤单
        // 立即发出、不必等下单 ack —— 否则单子滞留会被延迟成交+大滑点。
        let order_id_field = cmd
            .venue_order_id
            .or_else(|| order.venue_order_id())
            .map_or_else(
                || crate::common::credential::normalize_order_text(cmd.client_order_id.as_ref()),
                |v| v.to_string(),
            );
        let req_param = serde_json::json!({"order_id": order_id_field});

        let req_id = self.next_req_id("cancel");
        self.track_pending(&req_id, cmd.client_order_id, PendingKind::Cancel);
        let envelope =
            build_api_envelope("futures.order_cancel", &req_id, &req_param, unix_seconds());
        log::debug!("Gate 撤单 req_id={req_id} order_id={order_id_field}");
        self.send_outbound(envelope)
    }

    async fn generate_order_status_reports(
        &self,
        _cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        self.fetch_order_status_reports().await
    }

    async fn generate_position_status_reports(
        &self,
        _cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        self.fetch_position_status_reports().await
    }

    /// Reconciliation entry point: bundles open-order and position reports.
    async fn generate_mass_status(
        &self,
        _lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        let mut status = ExecutionMassStatus::new(
            self.core.client_id,
            self.core.account_id,
            self.core.venue,
            get_atomic_clock_realtime().get_time_ns(),
            None,
        );
        status.add_order_reports(self.fetch_order_status_reports().await.unwrap_or_default());
        status.add_position_reports(self.fetch_position_status_reports().await.unwrap_or_default());
        Ok(Some(status))
    }
}

/// Converts a raw private/WS-API frame into OrderEvents using the tracked order
/// snapshots and venue/text id maps. Returns the events to emit (may be empty).
#[allow(clippy::too_many_arguments)]
fn convert_raw_to_events(
    text: &str,
    pending: &Arc<Mutex<HashMap<String, PendingRequest>>>,
    tracked: &OrderMap,
    client_by_venue: &IdMap,
    client_by_text: &IdMap,
    seen_trade_ids: &Arc<Mutex<std::collections::HashSet<String>>>,
    submit_instants: &Arc<Mutex<HashMap<ClientOrderId, std::time::Instant>>>,
    server_proc_ms: &Arc<Mutex<HashMap<ClientOrderId, f64>>>,
    factory: &OrderEventFactory,
    quote_currency: Currency,
) -> Vec<nautilus_model::events::OrderEventAny> {
    let now = get_atomic_clock_realtime().get_time_ns();
    let mut events = Vec::new();

    // 1) WS-API order_place / order_cancel response (correlated by req_id).
    if let Some(resp) = parse_ws_api_response(text) {
        let req = pending
            .lock()
            .expect("Gate pending lock poisoned")
            .remove(&resp.request_id);
        if let Some(req) = req
            && let Some(order) = tracked
                .lock()
                .expect("Gate tracked lock poisoned")
                .get(&req.client_order_id)
                .cloned()
        {
            // Capture Gate's server-side processing time (x_out - x_in, same server
            // clock) keyed by order, so the fill-confirmation log can report it in
            // parentheses. We don't log a separate order-placement latency.
            if let (Some(in_us), Some(out_us)) = (
                resp.x_in_time.as_deref().and_then(|s| s.parse::<i64>().ok()),
                resp.x_out_time.as_deref().and_then(|s| s.parse::<i64>().ok()),
            ) {
                server_proc_ms
                    .lock()
                    .expect("Gate server_proc lock poisoned")
                    .insert(req.client_order_id, (out_us - in_us) as f64 / 1000.0);
            }
            match req.kind {
                PendingKind::Submit if resp.status_ok => {
                    if let Some(oid) = resp.order_id.as_deref() {
                        client_by_venue
                            .lock()
                            .expect("Gate venue map lock poisoned")
                            .insert(oid.to_string(), req.client_order_id);
                        // Only emit Accepted for a resting (open) order. A finished
                        // order (filled/cancelled IOC) reaches its terminal state via
                        // the usertrades/orders pushes; emitting Accepted after a fill
                        // would be an invalid Filled->Accepted transition.
                        if resp.order_status.as_deref() != Some("finished") {
                            events.push(factory.generate_order_accepted(
                                &order,
                                VenueOrderId::from(oid),
                                now,
                                now,
                            ));
                        }
                    }
                }
                PendingKind::Submit => {
                    let reason = resp.reason.as_deref().unwrap_or("rejected");
                    events.push(factory.generate_order_rejected(&order, reason, now, now, false));
                }
                PendingKind::Cancel if resp.status_ok => {
                    // 撤单成功的 OrderCanceled 由 futures.orders 推送(finish_as=cancelled)
                    // 统一发出, 此处不再重复发, 否则 Canceled->Canceled 触发
                    // InvalidStateTrigger WARN。
                    log::debug!("Gate 撤单已确认 req_id={}", resp.request_id);
                }
                PendingKind::Cancel => {
                    let reason = resp.reason.as_deref().unwrap_or("cancel rejected");
                    log::warn!("Gate 撤单被拒 req_id={} 原因={reason}", resp.request_id);
                    events.push(factory.generate_order_cancel_rejected(
                        &order,
                        order.venue_order_id(),
                        reason,
                        now,
                        now,
                    ));
                }
            }
        }
        return events;
    }

    // 2) usertrades fills -> OrderFilled (dedup by trade_id across SBE + JSON).
    for trade in parse_usertrades(text) {
        if let Some(event) = build_fill_event(
            "JSON",
            &trade.order_id,
            &trade.trade_id,
            &trade.text,
            &trade.size,
            &trade.price,
            &trade.fee,
            &trade.role,
            seen_trade_ids,
            client_by_venue,
            client_by_text,
            submit_instants,
            server_proc_ms,
            tracked,
            factory,
            quote_currency,
            now,
        ) {
            events.push(event);
        }
    }

    // 3) order-status pushes: record venue->client map; emit Canceled on finish.
    for ord in parse_orders_push(text) {
        let client = ord
            .text
            .as_ref()
            .and_then(|t| {
                client_by_text
                    .lock()
                    .expect("Gate text map lock poisoned")
                    .get(t)
                    .copied()
            })
            .or_else(|| {
                client_by_venue
                    .lock()
                    .expect("Gate venue map lock poisoned")
                    .get(&ord.order_id)
                    .copied()
            });
        if let Some(client) = client {
            client_by_venue
                .lock()
                .expect("Gate venue map lock poisoned")
                .insert(ord.order_id.clone(), client);
            if ord.finish_as.as_deref() == Some("cancelled")
                && let Some(order) = tracked
                    .lock()
                    .expect("Gate tracked lock poisoned")
                    .get(&client)
                    .cloned()
            {
                events.push(factory.generate_order_canceled(
                    &order,
                    Some(VenueOrderId::from(ord.order_id.as_str())),
                    now,
                    now,
                ));
            }
        }
    }

    events
}

fn contract_to_instrument_id(contract: &str) -> InstrumentId {
    InstrumentId::new(Symbol::new(contract), *GATE_VENUE)
}

/// Reads a JSON field as i64 (number or string).
fn json_i64(value: &serde_json::Value, key: &str) -> Option<i64> {
    value.get(key).and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
    })
}

/// Reads a JSON field as a string (string or number).
fn json_str(value: &serde_json::Value, key: &str) -> Option<String> {
    value.get(key).and_then(|v| {
        v.as_str()
            .map(str::to_string)
            .or_else(|| v.as_i64().map(|n| n.to_string()))
            .or_else(|| v.as_f64().map(|n| n.to_string()))
    })
}

/// Maps a Gate open-order JSON object to an `OrderStatusReport`.
fn map_gate_order_to_report(
    account_id: AccountId,
    instrument_id: InstrumentId,
    order: &serde_json::Value,
    now: nautilus_core::UnixNanos,
) -> Option<OrderStatusReport> {
    let id = json_str(order, "id")?;
    let size = json_i64(order, "size")?; // signed contract count
    let left = json_i64(order, "left").unwrap_or(0);
    let side = if size >= 0 {
        OrderSide::Buy
    } else {
        OrderSide::Sell
    };
    let quantity = Quantity::from(size.unsigned_abs().to_string().as_str());
    let filled = Quantity::from((size.unsigned_abs() - left.unsigned_abs()).to_string().as_str());
    let price_str = json_str(order, "price").unwrap_or_else(|| "0".to_string());
    let order_type = if price_str == "0" {
        OrderType::Market
    } else {
        OrderType::Limit
    };
    let time_in_force = match json_str(order, "tif").as_deref() {
        Some("ioc") => TimeInForce::Ioc,
        Some("fok") => TimeInForce::Fok,
        _ => TimeInForce::Gtc,
    };
    Some(OrderStatusReport::new(
        account_id,
        instrument_id,
        None,
        VenueOrderId::from(id.as_str()),
        side,
        order_type,
        time_in_force,
        OrderStatus::Accepted,
        quantity,
        filled,
        now,
        now,
        now,
        None,
    ))
}

/// Maps a Gate position JSON object to a `PositionStatusReport` (None if flat).
fn map_gate_position_to_report(
    account_id: AccountId,
    position: &serde_json::Value,
    now: nautilus_core::UnixNanos,
) -> Option<PositionStatusReport> {
    let contract = json_str(position, "contract")?;
    let size = json_i64(position, "size")?; // signed contract count
    let side = match size.signum() {
        1 => PositionSideSpecified::Long,
        -1 => PositionSideSpecified::Short,
        _ => return None, // flat
    };
    let quantity = Quantity::from(size.unsigned_abs().to_string().as_str());
    let avg_px = json_str(position, "entry_price").and_then(|s| s.parse::<rust_decimal::Decimal>().ok());
    Some(PositionStatusReport::new(
        account_id,
        contract_to_instrument_id(&contract),
        side,
        quantity,
        now,
        now,
        None,
        None,
        avg_px,
    ))
}

/// Builds an `OrderFilled` from a fill (JSON or SBE source), deduplicated by
/// `trade_id` (first source wins). Returns `None` on duplicate or unknown order.
#[allow(clippy::too_many_arguments)]
fn build_fill_event(
    source: &str,
    order_id: &str,
    trade_id: &str,
    text: &str,
    size: &str,
    price: &str,
    fee: &str,
    role: &str,
    seen_trade_ids: &Arc<Mutex<std::collections::HashSet<String>>>,
    client_by_venue: &IdMap,
    client_by_text: &IdMap,
    submit_instants: &Arc<Mutex<HashMap<ClientOrderId, std::time::Instant>>>,
    server_proc_ms: &Arc<Mutex<HashMap<ClientOrderId, f64>>>,
    tracked: &OrderMap,
    factory: &OrderEventFactory,
    quote_currency: Currency,
    now: nautilus_core::UnixNanos,
) -> Option<nautilus_model::events::OrderEventAny> {
    // Resolve our order FIRST (via text, populated at submit — no ack-timing race;
    // fall back to the venue id map). Only then dedup, so an unresolved early fill
    // doesn't consume the trade_id and starve the other source.
    let client = client_by_text
        .lock()
        .expect("Gate text map lock poisoned")
        .get(text)
        .copied()
        .or_else(|| {
            client_by_venue
                .lock()
                .expect("Gate venue map lock poisoned")
                .get(order_id)
                .copied()
        })?;
    let order = tracked
        .lock()
        .expect("Gate tracked lock poisoned")
        .get(&client)
        .cloned()?;
    // First source to deliver this trade_id wins; later duplicates are dropped.
    if !seen_trade_ids
        .lock()
        .expect("Gate seen_trade_ids lock poisoned")
        .insert(trade_id.to_string())
    {
        return None;
    }
    // 成交延迟 = signal sent -> fill confirmation (monotonic); the parenthesised
    // value is Gate's server-side processing time (x_out-x_in from the ack).
    let fill_latency = submit_instants
        .lock()
        .expect("Gate submit_instants lock poisoned")
        .get(&client)
        .map_or_else(
            || "n/a".to_string(),
            |t| format!("{:.2}ms", t.elapsed().as_secs_f64() * 1000.0),
        );
    let server_proc = server_proc_ms
        .lock()
        .expect("Gate server_proc lock poisoned")
        .get(&client)
        .map_or_else(|| "n/a".to_string(), |ms| format!("{ms:.2}ms"));
    log::info!(
        "[Gate 成交·{source}] 成交延迟={fill_latency} (服务端处理={server_proc}) trade_id={trade_id} 价={price} 量={size} 角色={role}"
    );
    // Fill size may be signed (sell = negative); last_qty is the magnitude.
    let magnitude = size.trim_start_matches('-');
    Some(factory.generate_order_filled(
        &order,
        VenueOrderId::from(order_id),
        None,
        TradeId::from(trade_id),
        Quantity::from(magnitude),
        Price::from(price),
        quote_currency,
        fee_to_money(fee, quote_currency),
        liquidity_from_role(role),
        now,
        now,
    ))
}

fn liquidity_from_role(role: &str) -> LiquiditySide {
    match role {
        "maker" => LiquiditySide::Maker,
        _ => LiquiditySide::Taker,
    }
}

fn fee_to_money(fee: &str, currency: Currency) -> Option<Money> {
    let amount: f64 = fee.parse().ok()?;
    Some(Money::new(amount.abs(), currency))
}

/// Reads the WS stream until the `futures.login` ack arrives, returning the
/// account `user_id` (5s timeout).
async fn read_login_uid(
    stream: &mut (impl futures_util::Stream<Item = GateWsEventMessage> + Unpin),
) -> anyhow::Result<String> {
    let read = async {
        while let Some(event) = stream.next().await {
            if let GateWsEventMessage::Raw(text) = event
                && let Some(uid) = extract_login_uid(&text)
            {
                return Ok(uid);
            }
        }
        Err(anyhow::anyhow!("Gate WS stream ended before login ack"))
    };
    tokio::time::timeout(Duration::from_secs(5), read)
        .await
        .map_err(|_| anyhow::anyhow!("Gate WS login ack timed out"))?
}

/// Extracts a non-zero `data.result.uid` from a `futures.login` response frame.
fn extract_login_uid(text: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    let uid = value.get("data")?.get("result")?.get("uid")?;
    let uid = uid
        .as_str()
        .map(str::to_string)
        .or_else(|| uid.as_i64().map(|n| n.to_string()))?;
    if uid.is_empty() || uid == "0" {
        return None;
    }
    Some(uid)
}

fn unix_seconds() -> i64 {
    (get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000) as i64
}

fn unix_millis() -> i64 {
    (get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn login_uid_extracted() {
        let text = r#"{"channel":"futures.login","event":"api","data":{"result":{"uid":"12345678"}}}"#;
        assert_eq!(extract_login_uid(text).as_deref(), Some("12345678"));
    }

    #[test]
    fn login_uid_rejects_zero_and_non_login() {
        assert_eq!(
            extract_login_uid(r#"{"data":{"result":{"uid":"0"}}}"#),
            None
        );
        // order_place ack has data.result but no uid.
        assert_eq!(
            extract_login_uid(r#"{"channel":"futures.order_place","data":{"result":{"id":"1"}}}"#),
            None
        );
        assert_eq!(extract_login_uid("not json"), None);
    }
}
