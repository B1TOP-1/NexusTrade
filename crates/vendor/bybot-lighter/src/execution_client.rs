use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;
use tokio::{
    sync::{broadcast, watch, Mutex as AsyncMutex},
    task::JoinHandle,
};

use crate::{
    data::LighterMarketSpec,
    execution::{
        parse_lighter_private_ws_messages, LighterAccountChannel, LighterCancelAck,
        LighterExecutionEffect, LighterExecutionReducer, LighterPrivateWsMessage, LighterSubmitAck,
    },
    http::{LighterAccountSnapshot, LighterHttpClient, LighterPositionSnapshot, LighterSignedTx},
    scaling::{scale_base_amount, scale_price, NonceManager},
    signer::LighterSigner,
    websocket::{LighterWebSocketClient, LighterWebSocketConfig, LighterWsEvent},
    ws_submit::{LighterWsSubmitError, LighterWsSubmitter},
};

const AUTH_TOKEN_TTL_SECS: i64 = 600;
const ORDER_TYPE_LIMIT: u8 = 0;
const ORDER_TYPE_MARKET: u8 = 1;
const TIF_IOC: u8 = 0;
const TIF_GTT: u8 = 1;
const TIF_POST_ONLY: u8 = 2;
const DEFAULT_ORDER_EXPIRY: i64 = -1;
const IOC_ORDER_EXPIRY: i64 = 0;
const EVENT_CAPACITY: usize = 4_096;
const ACCOUNT_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterOrderRejected(pub String);

impl fmt::Display for LighterOrderRejected {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Lighter order rejected: {}", self.0)
    }
}

impl std::error::Error for LighterOrderRejected {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterOrderOutcomeUnknown(pub String);

impl fmt::Display for LighterOrderOutcomeUnknown {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Lighter order outcome is unknown: {}", self.0)
    }
}

impl std::error::Error for LighterOrderOutcomeUnknown {}

#[derive(Clone, PartialEq, Eq)]
pub struct LighterExecutionConfig {
    pub http_url: String,
    pub private_ws_url: String,
    pub account_index: u64,
    pub api_key_index: u8,
    pub chain_id: u32,
    pub drain_window_ms: u64,
}

impl LighterExecutionConfig {
    pub fn new(
        http_url: impl Into<String>,
        private_ws_url: impl Into<String>,
        account_index: u64,
        api_key_index: u8,
        chain_id: u32,
    ) -> Result<Self> {
        let http_url = http_url.into();
        LighterHttpClient::new(&http_url)?;
        let private_ws_url = private_ws_url.into();
        LighterWebSocketConfig::new(private_ws_url.clone())?;
        i64::try_from(account_index).context("Lighter account index exceeds i64")?;
        Ok(Self {
            http_url,
            private_ws_url,
            account_index,
            api_key_index,
            chain_id,
            drain_window_ms: 250,
        })
    }
}

impl fmt::Debug for LighterExecutionConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LighterExecutionConfig")
            .field("http_url", &self.http_url)
            .field("private_ws_url", &self.private_ws_url)
            .field("account_index", &self.account_index)
            .field("api_key_index", &self.api_key_index)
            .field("chain_id", &self.chain_id)
            .field("drain_window_ms", &self.drain_window_ms)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LighterOrderType {
    Limit,
    Market,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LighterTimeInForce {
    ImmediateOrCancel,
    GoodTilTime,
    PostOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterOrderRequest {
    pub symbol: String,
    pub client_order_id: String,
    pub client_order_index: u64,
    pub signed_quantity: Decimal,
    pub limit_price: Option<Decimal>,
    pub order_type: LighterOrderType,
    pub time_in_force: LighterTimeInForce,
    pub reduce_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterCancelRequest {
    pub symbol: String,
    pub client_order_id: String,
    pub client_order_index: Option<u64>,
    pub order_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterPreparedOrder {
    pub client_order_id: String,
    pub client_order_index: u64,
    pub nonce: i64,
    pub signed_tx: LighterSignedTx,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterSubmitReceipt {
    pub ack: LighterSubmitAck,
    pub effects: Vec<LighterExecutionEffect>,
    pub timing: LighterSubmitTiming,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LighterSubmitTiming {
    pub submit_started_at_ms: u64,
    /// Time spent waiting for the per-API-key submission lock.
    pub lock_wait_ms: u64,
    pub sign_ms: u64,
    pub send_ms: u64,
    pub ack_ms: u64,
    /// End-to-end local monotonic time from the submission call to its ACK.
    pub submit_total_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterCancelReceipt {
    pub ack: LighterCancelAck,
    pub effects: Vec<LighterExecutionEffect>,
}

struct LighterExecutionInner {
    config: LighterExecutionConfig,
    http: LighterHttpClient,
    signer: LighterSigner,
    nonce: NonceManager,
    submission_lock: AsyncMutex<()>,
    ws_submitter: AsyncMutex<Option<LighterWsSubmitter>>,
    markets: BTreeMap<String, LighterMarketSpec>,
    reducer: Mutex<LighterExecutionReducer>,
    order_indices: Mutex<HashMap<u64, u64>>,
    account_stream: Mutex<LighterAccountStreamState>,
    account_ready: watch::Sender<bool>,
    account_error: watch::Sender<Option<String>>,
}

#[derive(Debug, Default)]
struct LighterAccountStreamState {
    positions: HashMap<u64, LighterPositionSnapshot>,
    collateral: Option<Decimal>,
    available_balance: Option<Decimal>,
    orders_ready: bool,
    trades_ready: bool,
    positions_ready: bool,
    stats_ready: bool,
}

impl LighterAccountStreamState {
    fn is_ready(&self) -> bool {
        self.orders_ready && self.trades_ready && self.positions_ready && self.stats_ready
    }

    fn missing_channels(&self) -> Vec<&'static str> {
        let mut missing = Vec::new();
        if !self.orders_ready {
            missing.push("account_all_orders");
        }
        if !self.trades_ready {
            missing.push("account_all_trades");
        }
        if !self.positions_ready {
            missing.push("account_all_positions");
        }
        if !self.stats_ready {
            missing.push("user_stats");
        }
        missing
    }

    fn mark_ready(&mut self, channel: LighterAccountChannel) {
        match channel {
            LighterAccountChannel::Orders => self.orders_ready = true,
            LighterAccountChannel::Trades => self.trades_ready = true,
            LighterAccountChannel::Positions => self.positions_ready = true,
            LighterAccountChannel::Stats => self.stats_ready = true,
        }
    }

    fn reset(&mut self) {
        self.positions.clear();
        self.collateral = None;
        self.available_balance = None;
        self.orders_ready = false;
        self.trades_ready = false;
        self.positions_ready = false;
        self.stats_ready = false;
    }
}

#[derive(Clone)]
pub struct LighterExecutionClient {
    inner: Arc<LighterExecutionInner>,
}

impl fmt::Debug for LighterExecutionClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LighterExecutionClient")
            .field("config", &self.inner.config)
            .field("markets", &self.inner.markets.keys().collect::<Vec<_>>())
            .field("signer", &"configured")
            .finish()
    }
}

impl LighterExecutionClient {
    pub async fn connect(config: LighterExecutionConfig, private_key: &str) -> Result<Self> {
        let market_specs = LighterHttpClient::new(&config.http_url)?
            .market_specs()
            .await?;
        let client = Self::new(config, private_key, market_specs)?;
        client.initialize().await?;
        Ok(client)
    }

    pub fn new(
        config: LighterExecutionConfig,
        private_key: &str,
        market_specs: Vec<LighterMarketSpec>,
    ) -> Result<Self> {
        if market_specs.is_empty() {
            bail!("Lighter execution requires at least one active market spec");
        }
        let account_index =
            i64::try_from(config.account_index).context("Lighter account index exceeds i64")?;
        let signer = LighterSigner::new(
            private_key,
            config.chain_id,
            config.api_key_index,
            account_index,
        )
        .map_err(|error| anyhow::anyhow!("invalid Lighter private key: {error}"))?;
        let http = LighterHttpClient::new(&config.http_url)?;
        let markets = market_specs
            .into_iter()
            .map(|spec| (normalize_symbol(&spec.symbol), spec))
            .collect();
        let drain_window_ms = config.drain_window_ms;
        let (account_ready, _) = watch::channel(false);
        let (account_error, _) = watch::channel(None);
        Ok(Self {
            inner: Arc::new(LighterExecutionInner {
                config,
                http,
                signer,
                nonce: NonceManager::new(),
                submission_lock: AsyncMutex::new(()),
                ws_submitter: AsyncMutex::new(None),
                markets,
                reducer: Mutex::new(LighterExecutionReducer::new(drain_window_ms)),
                order_indices: Mutex::new(HashMap::new()),
                account_stream: Mutex::new(LighterAccountStreamState::default()),
                account_ready,
                account_error,
            }),
        })
    }

    #[must_use]
    pub fn config(&self) -> &LighterExecutionConfig {
        &self.inner.config
    }

    pub fn seed_nonce(&self, next_nonce: i64) {
        self.inner.nonce.reset(next_nonce);
    }

    pub async fn initialize(&self) -> Result<()> {
        let next_nonce = self.fetch_next_nonce().await?;
        self.seed_nonce(next_nonce);
        Ok(())
    }

    async fn fetch_next_nonce(&self) -> Result<i64> {
        self.inner
            .http
            .next_nonce(
                self.inner.config.account_index,
                self.inner.config.api_key_index,
            )
            .await
    }

    async fn resynchronize_nonce(&self) -> Result<i64> {
        let next_nonce = self.fetch_next_nonce().await?;
        self.inner.nonce.resynchronize(next_nonce);
        Ok(next_nonce)
    }

    /// Opens the opt-in, dedicated WebSocket `jsonapi/sendtx` connection.
    /// HTTP remains the default submission transport until a caller uses one of
    /// the explicit `*_ws` methods below.
    pub async fn enable_ws_submission(&self) -> Result<()> {
        if self.inner.ws_submitter.lock().await.is_some() {
            return Ok(());
        }
        let submitter = LighterWsSubmitter::connect(LighterWebSocketConfig::new(
            self.inner.config.private_ws_url.clone(),
        )?)
        .await?;
        let mut guard = self.inner.ws_submitter.lock().await;
        if guard.is_none() {
            *guard = Some(submitter);
        }
        Ok(())
    }

    async fn ws_submitter(&self) -> Result<LighterWsSubmitter> {
        self.inner
            .ws_submitter
            .lock()
            .await
            .clone()
            .context("Lighter WebSocket submission is not enabled")
    }

    async fn recover_ws_submission_failure(&self, submitter: &LighterWsSubmitter) -> String {
        match self.resynchronize_nonce().await {
            Ok(next_nonce) => match submitter.confirm_nonce_resynchronized().await {
                Ok(()) => format!("; nonce resynchronized to {next_nonce}"),
                Err(error) => format!(
                    "; nonce resynchronized to {next_nonce}, but WS reconnect failed: {error}"
                ),
            },
            Err(error) => format!("; nonce resynchronization failed: {error}"),
        }
    }

    pub async fn account_snapshot(&self) -> Result<LighterAccountSnapshot> {
        let state = self
            .inner
            .account_stream
            .lock()
            .map_err(|_| anyhow::anyhow!("Lighter account-stream lock poisoned"))?;
        if !state.is_ready() {
            bail!("Lighter account WebSocket snapshot is not ready");
        }
        let collateral = state
            .collateral
            .context("Lighter WS collateral is unavailable")?;
        let available_balance = state
            .available_balance
            .context("Lighter WS available balance is unavailable")?;
        Ok(LighterAccountSnapshot {
            collateral,
            available_balance,
            positions: state.positions.values().cloned().collect(),
        })
    }

    pub async fn position(&self, symbol: &str) -> Result<Option<LighterPositionSnapshot>> {
        let market = self.market(symbol)?;
        Ok(self
            .account_snapshot()
            .await?
            .positions
            .into_iter()
            .find(|position| position.market_id == market.market_id))
    }

    pub fn market_id(&self, symbol: &str) -> Result<u64> {
        Ok(self.market(symbol)?.market_id)
    }

    pub fn prepare_order(&self, request: &LighterOrderRequest) -> Result<LighterPreparedOrder> {
        if !self.inner.nonce.is_seeded() {
            bail!("Lighter nonce is not seeded");
        }
        if request.client_order_id.trim().is_empty() {
            bail!("Lighter client order id must not be empty");
        }
        if request.signed_quantity.is_zero() {
            bail!("Lighter order quantity must not be zero");
        }
        let market = self.market(&request.symbol)?;
        let absolute_quantity = request.signed_quantity.abs();
        if absolute_quantity < market.min_base_amount {
            bail!(
                "Lighter order quantity {absolute_quantity} is below minimum {}",
                market.min_base_amount
            );
        }
        let base_amount = scale_base_amount(absolute_quantity, market.size_multiplier)?;
        if base_amount <= 0 {
            bail!("Lighter scaled base amount must be positive");
        }
        let price = match (request.order_type, request.limit_price) {
            (LighterOrderType::Limit, Some(price)) => price,
            (LighterOrderType::Limit, None) => bail!("Lighter limit order requires a price"),
            (LighterOrderType::Market, price) => price.unwrap_or(Decimal::ZERO),
        };
        let scaled_price = scale_price(price, market.price_multiplier)?;
        let market_index =
            i16::try_from(market.market_id).context("Lighter market id exceeds i16")?;
        let client_order_index = i64::try_from(request.client_order_index)
            .context("Lighter client order index exceeds i64")?;
        let time_in_force = map_time_in_force(request.time_in_force);
        let order_expiry = if time_in_force == TIF_IOC {
            IOC_ORDER_EXPIRY
        } else {
            DEFAULT_ORDER_EXPIRY
        };
        let nonce = self.inner.nonce.take();
        let payload = self
            .inner
            .signer
            .sign_create_order(
                market_index,
                client_order_index,
                base_amount,
                scaled_price,
                side_to_is_ask(request.signed_quantity),
                map_order_type(request.order_type),
                time_in_force,
                u8::from(request.reduce_only),
                0,
                order_expiry,
                nonce,
            )
            .map_err(|error| anyhow::anyhow!("Lighter create-order signing failed: {error}"))?;
        Ok(LighterPreparedOrder {
            client_order_id: request.client_order_id.clone(),
            client_order_index: request.client_order_index,
            nonce,
            signed_tx: LighterSignedTx {
                client_order_id: request.client_order_id.clone(),
                client_order_index: Some(request.client_order_index),
                tx_type: payload.tx_type,
                tx_info: payload.tx_info,
                price_protection: true,
            },
        })
    }

    pub async fn submit_order(
        &self,
        request: &LighterOrderRequest,
    ) -> Result<LighterSubmitReceipt> {
        if !self.account_snapshot_ready() {
            bail!("Lighter account WebSocket snapshot is not ready");
        }
        // Lighter requires one strictly sequential nonce stream per API key.
        // This lock covers signing, submission, and any failure resynchronization.
        let submit_started = Instant::now();
        let _submission = self.inner.submission_lock.lock().await;
        let lock_wait_ms = elapsed_millis(submit_started);
        let submit_started_at_ms = now_millis();
        let sign_started = Instant::now();
        let prepared = match self.prepare_order(request) {
            Ok(prepared) => prepared,
            Err(error) => {
                let _ = self.resynchronize_nonce().await;
                return Err(error);
            }
        };
        let sign_ms = elapsed_millis(sign_started);
        let (mut ack, transport_timing) = match self
            .inner
            .http
            .submit_tx_timed(&prepared.signed_tx)
            .await
        {
            Ok(result) => result,
            Err(error) => {
                let resync = self.resynchronize_nonce().await;
                let suffix = match resync {
                    Ok(next_nonce) => format!("; nonce resynchronized to {next_nonce}"),
                    Err(resync_error) => format!("; nonce resynchronization failed: {resync_error}"),
                };
                if error.code >= 0 {
                    return Err(anyhow::Error::new(LighterOrderRejected(format!(
                        "code={} message={}{}",
                        error.code, error.message, suffix
                    ))));
                }
                return Err(anyhow::anyhow!(
                    "Lighter submit transport failure: {}{}",
                    error.message,
                    suffix
                ));
            }
        };
        if ack.ts_event_ms == 0 {
            ack.ts_event_ms = now_millis();
        }
        let effects = self
            .inner
            .reducer
            .lock()
            .map_err(|_| anyhow::anyhow!("Lighter reducer lock poisoned"))?
            .on_submit_ack(ack.clone());
        Ok(LighterSubmitReceipt {
            ack,
            effects,
            timing: LighterSubmitTiming {
                submit_started_at_ms,
                lock_wait_ms,
                sign_ms,
                send_ms: transport_timing.send_ms,
                ack_ms: transport_timing.ack_ms,
                submit_total_ms: elapsed_millis(submit_started),
            },
        })
    }

    /// Explicit WebSocket order path. It is not used by `submit_order` and
    /// follows the same API-key lock and nonce recovery invariants as HTTP.
    pub async fn submit_order_ws(
        &self,
        request: &LighterOrderRequest,
    ) -> Result<LighterSubmitReceipt> {
        if !self.account_snapshot_ready() {
            bail!("Lighter account WebSocket snapshot is not ready");
        }
        let submit_started = Instant::now();
        let _submission = self.inner.submission_lock.lock().await;
        let lock_wait_ms = elapsed_millis(submit_started);
        // Fetch this before reserving a nonce: disabled WS entry must not
        // consume a nonce or be mistaken for an ambiguous submission.
        let submitter = self.ws_submitter().await?;
        let submit_started_at_ms = now_millis();
        let sign_started = Instant::now();
        let prepared = match self.prepare_order(request) {
            Ok(prepared) => prepared,
            Err(error) => {
                let _ = self.resynchronize_nonce().await;
                return Err(error);
            }
        };
        let sign_ms = elapsed_millis(sign_started);
        let receipt = match submitter.submit_tx(&prepared.signed_tx).await {
            Ok(receipt) => receipt,
            Err(error) => {
                let suffix = self.recover_ws_submission_failure(&submitter).await;
                let message = format!("WebSocket submission failed: {error}{suffix}");
                return match error {
                    LighterWsSubmitError::Rejected { .. } => {
                        Err(anyhow::Error::new(LighterOrderRejected(message)))
                    }
                    LighterWsSubmitError::OutcomeUnknown { .. }
                    | LighterWsSubmitError::Protocol { .. }
                    | LighterWsSubmitError::NonceResynchronizationRequired => {
                        Err(anyhow::Error::new(LighterOrderOutcomeUnknown(message)))
                    }
                };
            }
        };
        let ack = LighterSubmitAck {
            client_order_id: prepared.client_order_id,
            client_order_index: Some(prepared.client_order_index),
            tx_hash: receipt.tx_hash,
            ts_event_ms: receipt.ts_event_ms,
        };
        let effects = self
            .inner
            .reducer
            .lock()
            .map_err(|_| anyhow::anyhow!("Lighter reducer lock poisoned"))?
            .on_submit_ack(ack.clone());
        Ok(LighterSubmitReceipt {
            ack,
            effects,
            timing: LighterSubmitTiming {
                submit_started_at_ms,
                lock_wait_ms,
                sign_ms,
                send_ms: receipt.timing.send_ms,
                ack_ms: receipt.timing.ack_ms,
                submit_total_ms: elapsed_millis(submit_started),
            },
        })
    }

    pub async fn cancel_order(
        &self,
        request: &LighterCancelRequest,
    ) -> Result<LighterCancelReceipt> {
        if !self.account_snapshot_ready() {
            bail!("Lighter account WebSocket snapshot is not ready");
        }
        if !self.inner.nonce.is_seeded() {
            bail!("Lighter nonce is not seeded");
        }
        let _submission = self.inner.submission_lock.lock().await;
        let market = self.market(&request.symbol)?;
        let market_index =
            i16::try_from(market.market_id).context("Lighter market id exceeds i16")?;
        let order_index =
            i64::try_from(request.order_index).context("Lighter order index exceeds i64")?;
        let nonce = self.inner.nonce.take();
        let payload = match self
            .inner
            .signer
            .sign_cancel_order(market_index, order_index, nonce)
        {
            Ok(payload) => payload,
            Err(error) => {
                let _ = self.resynchronize_nonce().await;
                return Err(anyhow::anyhow!("Lighter cancel signing failed: {error}"));
            }
        };
        let tx = LighterSignedTx {
            client_order_id: request.client_order_id.clone(),
            client_order_index: request.client_order_index,
            tx_type: payload.tx_type,
            tx_info: payload.tx_info,
            price_protection: true,
        };
        let ack = match self.inner.http.cancel_tx(&tx).await {
            Ok(ack) => ack,
            Err(error) => {
                let suffix = match self.resynchronize_nonce().await {
                    Ok(next_nonce) => format!("; nonce resynchronized to {next_nonce}"),
                    Err(resync_error) => format!("; nonce resynchronization failed: {resync_error}"),
                };
                return Err(anyhow::anyhow!(
                    "Lighter cancel rejected: code={} message={}{}",
                    error.code,
                    error.message,
                    suffix
                ));
            }
        };
        let effects = self
            .inner
            .reducer
            .lock()
            .map_err(|_| anyhow::anyhow!("Lighter reducer lock poisoned"))?
            .on_cancel_ack(ack.clone());
        Ok(LighterCancelReceipt { ack, effects })
    }

    /// Explicit WebSocket cancellation path paired with `submit_order_ws`.
    pub async fn cancel_order_ws(
        &self,
        request: &LighterCancelRequest,
    ) -> Result<LighterCancelReceipt> {
        if !self.account_snapshot_ready() {
            bail!("Lighter account WebSocket snapshot is not ready");
        }
        if !self.inner.nonce.is_seeded() {
            bail!("Lighter nonce is not seeded");
        }
        let _submission = self.inner.submission_lock.lock().await;
        let market = self.market(&request.symbol)?;
        let market_index =
            i16::try_from(market.market_id).context("Lighter market id exceeds i16")?;
        let order_index =
            i64::try_from(request.order_index).context("Lighter order index exceeds i64")?;
        let nonce = self.inner.nonce.take();
        let payload = match self
            .inner
            .signer
            .sign_cancel_order(market_index, order_index, nonce)
        {
            Ok(payload) => payload,
            Err(error) => {
                let _ = self.resynchronize_nonce().await;
                return Err(anyhow::anyhow!("Lighter cancel signing failed: {error}"));
            }
        };
        let tx = LighterSignedTx {
            client_order_id: request.client_order_id.clone(),
            client_order_index: request.client_order_index,
            tx_type: payload.tx_type,
            tx_info: payload.tx_info,
            price_protection: true,
        };
        let submitter = self.ws_submitter().await?;
        let receipt = match submitter.submit_tx(&tx).await {
            Ok(receipt) => receipt,
            Err(error) => {
                let suffix = self.recover_ws_submission_failure(&submitter).await;
                return Err(anyhow::anyhow!(
                    "Lighter WebSocket cancel failed: {error}{suffix}"
                ));
            }
        };
        let ack = LighterCancelAck {
            client_order_id: request.client_order_id.clone(),
            client_order_index: request.client_order_index,
            tx_hash: receipt.tx_hash,
            ts_event_ms: receipt.ts_event_ms,
        };
        let effects = self
            .inner
            .reducer
            .lock()
            .map_err(|_| anyhow::anyhow!("Lighter reducer lock poisoned"))?
            .on_cancel_ack(ack.clone());
        Ok(LighterCancelReceipt { ack, effects })
    }

    pub fn ingest_private_ws_text(&self, payload: &str) -> Result<Vec<LighterExecutionEffect>> {
        let messages = parse_lighter_private_ws_messages(payload)?;
        let mut reducer = self
            .inner
            .reducer
            .lock()
            .map_err(|_| anyhow::anyhow!("Lighter reducer lock poisoned"))?;
        let mut account = self
            .inner
            .account_stream
            .lock()
            .map_err(|_| anyhow::anyhow!("Lighter account-stream lock poisoned"))?;
        let mut effects = Vec::new();
        for message in messages {
            match message {
                LighterPrivateWsMessage::Order(order) => {
                    if let (Some(client_order_index), Some(order_index)) =
                        (order.client_order_index, order.order_index)
                    {
                        self.inner
                            .order_indices
                            .lock()
                            .map_err(|_| anyhow::anyhow!("Lighter order-index lock poisoned"))?
                            .insert(client_order_index, order_index);
                    }
                    effects.extend(reducer.on_order_event(order));
                }
                LighterPrivateWsMessage::Trade(trade) => {
                    effects.extend(reducer.on_trade_event(trade));
                }
                LighterPrivateWsMessage::PositionSnapshot(positions) => {
                    account.positions = positions
                        .iter()
                        .map(|position| {
                            (
                                position.market_id,
                                LighterPositionSnapshot {
                                    market_id: position.market_id,
                                    signed_quantity: position.signed_quantity,
                                    average_price: position.average_price,
                                    unrealized_pnl: position.unrealized_pnl,
                                    return_on_equity: position.return_on_equity,
                                    liquidation_price: position.liquidation_price,
                                },
                            )
                        })
                        .collect();
                    effects.extend(
                        positions
                            .into_iter()
                            .map(|position| LighterExecutionEffect::Position { position }),
                    );
                }
                LighterPrivateWsMessage::PositionUpdate(positions) => {
                    for position in &positions {
                        account.positions.insert(
                            position.market_id,
                            LighterPositionSnapshot {
                                market_id: position.market_id,
                                signed_quantity: position.signed_quantity,
                                average_price: position.average_price,
                                unrealized_pnl: position.unrealized_pnl,
                                return_on_equity: position.return_on_equity,
                                liquidation_price: position.liquidation_price,
                            },
                        );
                    }
                    effects.extend(
                        positions
                            .into_iter()
                            .map(|position| LighterExecutionEffect::Position { position }),
                    );
                }
                LighterPrivateWsMessage::AccountStats(stats) => {
                    account.collateral = Some(stats.collateral);
                    account.available_balance = Some(stats.available_balance);
                }
                LighterPrivateWsMessage::Funding(funding) => {
                    effects.extend(
                        funding
                            .into_iter()
                            .map(|funding| LighterExecutionEffect::Funding { funding }),
                    );
                }
                LighterPrivateWsMessage::Ready(channel) => account.mark_ready(channel),
            }
        }
        if account.is_ready() {
            self.inner.account_ready.send_replace(true);
        }
        Ok(effects)
    }

    pub fn flush_expired_external_trades(&self) -> Result<Vec<LighterExecutionEffect>> {
        self.inner
            .reducer
            .lock()
            .map_err(|_| anyhow::anyhow!("Lighter reducer lock poisoned"))
            .map(|mut reducer| reducer.flush_expired_external_trades(now_millis()))
    }

    pub fn restore_order_tracking(
        &self,
        client_order_id: &str,
        client_order_index: u64,
    ) -> Result<()> {
        self.inner
            .reducer
            .lock()
            .map_err(|_| anyhow::anyhow!("Lighter reducer lock poisoned"))?
            .restore_order(client_order_id, client_order_index);
        Ok(())
    }

    #[must_use]
    pub fn venue_order_index(&self, client_order_index: u64) -> Option<u64> {
        self.inner
            .order_indices
            .lock()
            .ok()
            .and_then(|indices| indices.get(&client_order_index).copied())
    }

    #[must_use]
    pub fn private_channels(&self) -> Vec<String> {
        let account_index = self.inner.config.account_index;
        vec![
            format!("account_all_orders/{account_index}"),
            format!("account_all/{account_index}"),
        ]
    }

    #[must_use]
    pub fn public_account_channels(&self) -> Vec<String> {
        let account_index = self.inner.config.account_index;
        vec![
            format!("account_all_trades/{account_index}"),
            format!("account_all_positions/{account_index}"),
            format!("user_stats/{account_index}"),
        ]
    }

    pub async fn wait_account_snapshot(&self) -> Result<()> {
        let mut ready = self.inner.account_ready.subscribe();
        let mut account_error = self.inner.account_error.subscribe();
        if *ready.borrow() {
            return Ok(());
        }
        if let Some(error) = account_error.borrow().clone() {
            bail!(error);
        }
        let wait_result = tokio::time::timeout(ACCOUNT_SNAPSHOT_TIMEOUT, async move {
            loop {
                tokio::select! {
                    result = ready.changed() => {
                        result.context("Lighter account WebSocket readiness channel closed")?;
                        if *ready.borrow() {
                            return Ok(());
                        }
                    }
                    result = account_error.changed() => {
                        result.context("Lighter account WebSocket error channel closed")?;
                        if let Some(error) = account_error.borrow().clone() {
                            bail!(error);
                        }
                    }
                }
            }
        })
        .await;
        match wait_result {
            Ok(result) => result?,
            Err(_) => {
                let missing = self
                    .inner
                    .account_stream
                    .lock()
                    .map_err(|_| anyhow::anyhow!("Lighter account-stream lock poisoned"))?
                    .missing_channels()
                    .join(",");
                bail!(
                    "timed out waiting for Lighter account WebSocket snapshot; missing={missing}"
                );
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn account_snapshot_ready(&self) -> bool {
        *self.inner.account_ready.borrow()
    }

    fn reset_account_snapshot(&self) -> Result<()> {
        self.inner
            .account_stream
            .lock()
            .map_err(|_| anyhow::anyhow!("Lighter account-stream lock poisoned"))?
            .reset();
        self.inner.account_ready.send_replace(false);
        self.inner.account_error.send_replace(None);
        Ok(())
    }

    fn record_account_stream_error(&self, error: impl Into<String>) {
        self.inner.account_error.send_replace(Some(error.into()));
    }

    pub fn auth_token(&self) -> Result<String> {
        self.inner
            .signer
            .create_auth_token(now_secs().saturating_add(AUTH_TOKEN_TTL_SECS))
            .map_err(|error| anyhow::anyhow!("Lighter auth signing failed: {error}"))
    }

    pub async fn spawn_private_runtime(&self) -> Result<LighterExecutionRuntime> {
        self.reset_account_snapshot()?;
        let mut websocket = LighterWebSocketClient::new(LighterWebSocketConfig::new(
            self.inner.config.private_ws_url.clone(),
        )?);
        for channel in self.public_account_channels() {
            websocket.subscriptions_mut().subscribe_public(channel)?;
        }
        for channel in self.private_channels() {
            websocket.subscriptions_mut().subscribe_private(channel)?;
        }
        let mut connection = websocket.connect().await?;
        for channel in self.public_account_channels() {
            connection.subscribe_public(&channel).await?;
        }
        let auth = self.auth_token()?;
        for channel in self.private_channels() {
            connection.subscribe_private(&channel, &auth).await?;
        }

        let client = self.clone();
        let (sender, _) = broadcast::channel(EVENT_CAPACITY);
        let task_sender = sender.clone();
        let task = tokio::spawn(async move {
            let mut heartbeat = tokio::time::interval(connection.heartbeat_interval());
            let mut external_trade_flush = tokio::time::interval(Duration::from_millis(50));
            heartbeat.tick().await;
            external_trade_flush.tick().await;
            loop {
                tokio::select! {
                    _ = heartbeat.tick() => {
                        if connection.send_ping().await.is_err()
                            && reconnect(&client, &websocket, &mut connection).await.is_err()
                        {
                            break;
                        }
                    }
                    event = connection.next_event() => {
                        match event {
                            Ok(LighterWsEvent::Text(payload)) => {
                                match client.ingest_private_ws_text(&payload) {
                                    Ok(effects) => {
                                        for effect in effects {
                                            let _ = task_sender.send(effect);
                                        }
                                    }
                                    Err(error) => {
                                        client.record_account_stream_error(format!(
                                            "Lighter account WebSocket payload failed: {error}"
                                        ));
                                        break;
                                    }
                                }
                            }
                            Ok(LighterWsEvent::Closed) | Err(_) => {
                                if reconnect(&client, &websocket, &mut connection).await.is_err() {
                                    break;
                                }
                            }
                            Ok(LighterWsEvent::Reconnected) => {}
                        }
                    }
                    _ = external_trade_flush.tick() => {
                        match client.flush_expired_external_trades() {
                            Ok(effects) => {
                                for effect in effects {
                                    let _ = task_sender.send(effect);
                                }
                            }
                            Err(error) => client.record_account_stream_error(format!(
                                "Lighter external-trade attribution flush failed: {error}"
                            )),
                        }
                    }
                }
            }
        });
        Ok(LighterExecutionRuntime { sender, task })
    }

    fn market(&self, symbol: &str) -> Result<&LighterMarketSpec> {
        let symbol = normalize_symbol(symbol);
        self.inner
            .markets
            .get(&symbol)
            .ok_or_else(|| anyhow::anyhow!("unknown Lighter market: {symbol}"))
    }
}

pub struct LighterExecutionRuntime {
    sender: broadcast::Sender<LighterExecutionEffect>,
    task: JoinHandle<()>,
}

impl LighterExecutionRuntime {
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<LighterExecutionEffect> {
        self.sender.subscribe()
    }
}

impl fmt::Debug for LighterExecutionRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LighterExecutionRuntime")
            .field("finished", &self.task.is_finished())
            .finish()
    }
}

impl Drop for LighterExecutionRuntime {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn reconnect(
    client: &LighterExecutionClient,
    websocket: &LighterWebSocketClient,
    connection: &mut crate::websocket::LighterWebSocketConnection,
) -> Result<()> {
    client.reset_account_snapshot()?;
    let auth = client.auth_token()?;
    websocket.reconnect(connection, &auth).await?;
    Ok(())
}

fn normalize_symbol(symbol: &str) -> String {
    symbol.trim().to_uppercase()
}

/// 将带符号数量映射为 Lighter 线协议的 `is_ask` 标志。
///
/// Lighter 线协议约定：正数量 = 买入/bid → 0，负数量 = 卖出/ask → 1。
/// 零数量在 `prepare_order` 入口已被拒绝，不会到达此处。
const fn side_to_is_ask(signed_quantity: Decimal) -> u8 {
    if signed_quantity.is_sign_negative() {
        1
    } else {
        0
    }
}

const fn map_order_type(order_type: LighterOrderType) -> u8 {
    match order_type {
        LighterOrderType::Limit => ORDER_TYPE_LIMIT,
        LighterOrderType::Market => ORDER_TYPE_MARKET,
    }
}

const fn map_time_in_force(time_in_force: LighterTimeInForce) -> u8 {
    match time_in_force {
        LighterTimeInForce::ImmediateOrCancel => TIF_IOC,
        LighterTimeInForce::GoodTilTime => TIF_GTT,
        LighterTimeInForce::PostOnly => TIF_POST_ONLY,
    }
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn elapsed_millis(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}
