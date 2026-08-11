//! ExecutionVenue + PrivateVenue：HTTP 下单 (HMAC-SHA256)，对标 M1 模式。
//!
//! Binance WS API 需要 Ed25519 密钥对（未引入），采用 REST 下单。

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use nexus_core::{
    now_ms, AccountEvent, AccountSnapshot, AccountStream, Balance, ClientOrderId, Decimal,
    ExecutionVenue, Fill, NewOrder, NexusError, OrderAck, OrderEvent, OrderKind, OrderRef,
    OrderTracker, OrderUpdate, Position, PrivateVenue, Result, Side, Symbol, Tif,
    VenueCapabilities, VenueId,
};
use tokio::sync::mpsc;

use crate::auth::{sign_request, ListenKeyGuard};
use crate::types::{AccountInfo, OrderData, UserStreamEvent};
use crate::ws;

/// Binance 执行 venue 配置。
#[derive(Debug, Clone)]
pub struct BinanceVenueConfig {
    pub rest_url: String,
    pub ws_base: String,
    pub api_key: String,
    pub api_secret: String,
}

impl BinanceVenueConfig {
    pub fn mainnet(api_key: String, api_secret: String) -> Self {
        Self {
            rest_url: "https://fapi.binance.com".to_string(),
            ws_base: "wss://fstream.binance.com/private/ws".to_string(),
            api_key,
            api_secret,
        }
    }

    pub fn testnet(api_key: String, api_secret: String) -> Self {
        Self {
            rest_url: "https://testnet.binancefuture.com".to_string(),
            ws_base: "wss://stream.binancefuture.com/private/ws".to_string(),
            api_key,
            api_secret,
        }
    }

    fn private_ws_url(&self, listen_key: &str) -> String {
        format!("{}/{}", self.ws_base, listen_key)
    }
}

struct OrderEntry {
    tracker: OrderTracker,
    symbol: Symbol,
    side: Side,
    venue_order_id: Option<u64>,
}

type Registry = Arc<Mutex<HashMap<String, OrderEntry>>>;

/// Binance Futures 执行 + 私有流 venue。
pub struct BinanceVenue {
    config: BinanceVenueConfig,
    http: reqwest::Client,
    listen_key: Mutex<Option<ListenKeyGuard>>,
    registry: Registry,
    ready: AtomicBool,
    _ws: Mutex<Option<ws::WsSession>>,
    shutdown_tx: Mutex<Option<tokio::sync::watch::Sender<bool>>>,
    id_counter: AtomicU64,
}

impl BinanceVenue {
    pub async fn connect(config: BinanceVenueConfig) -> Result<Arc<Self>> {
        let http = reqwest::Client::new();
        let listen_key =
            ListenKeyGuard::acquire(&http, &config.rest_url, &config.api_key).await?;

        let venue = Arc::new(Self {
            config,
            http,
            listen_key: Mutex::new(Some(listen_key)),
            registry: Arc::new(Mutex::new(HashMap::new())),
            ready: AtomicBool::new(false),
            _ws: Mutex::new(None),
            shutdown_tx: Mutex::new(None),
            id_counter: AtomicU64::new(1),
        });

        venue.ready.store(true, Ordering::SeqCst);
        Ok(venue)
    }

    fn next_id(&self) -> String {
        let n = self.id_counter.fetch_add(1, Ordering::Relaxed);
        format!("nxbn-{n}")
    }

    fn map_tif(tif: Tif, kind: &OrderKind) -> Result<(&'static str, Option<&'static str>)> {
        match (tif, kind) {
            (Tif::Gtc, _) => Ok(("LIMIT", Some("GTC"))),
            (Tif::Ioc, OrderKind::Limit { .. }) => Ok(("LIMIT", Some("IOC"))),
            (Tif::Ioc, OrderKind::Market) => Ok(("MARKET", None)),
            (Tif::Fok, _) => Ok(("LIMIT", Some("FOK"))),
            (Tif::PostOnly, _) => Ok(("LIMIT", Some("GTX"))),
        }
    }
}

#[async_trait]
impl ExecutionVenue for BinanceVenue {
    fn venue(&self) -> VenueId {
        VenueId::BINANCE_FUT
    }

    fn capabilities(&self) -> VenueCapabilities {
        VenueCapabilities {
            ws_order_entry: false, // HTTP (HMAC-SHA256)
            batch_orders: false,
            post_only: true,
            reduce_only: true,
            cancel_all_native: false,
            book_fastest_interval_ms: 100,
            dual_feed: false,
        }
    }

    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    async fn place(&self, order: NewOrder) -> Result<OrderAck> {
        let (order_type, time_in_force) = Self::map_tif(order.tif, &order.kind)?;

        let price_str = order.price().map(|p| p.to_string());
        let qty_str = order.qty.to_string();
        let client_order_id = order.client_id.0.clone();

        let mut params = vec![
            ("symbol", order.symbol.venue_native.clone()),
            ("side", format!("{:?}", order.side).to_uppercase()),
            ("type", order_type.to_string()),
            ("quantity", qty_str),
            ("newClientOrderId", client_order_id.clone()),
        ];
        if let Some(tif_str) = time_in_force {
            params.push(("timeInForce", tif_str.to_string()));
        }
        if let Some(ref px) = price_str {
            params.push(("price", px.clone()));
        }
        if order.reduce_only {
            params.push(("reduceOnly", "true".to_string()));
        }

        let query = params
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        let signed = sign_request(&query, &self.config.api_secret);

        // 登记。
        {
            let mut tracker = OrderTracker::new(order.qty);
            let _ = tracker.apply(OrderEvent::SubmitSent);
            self.registry.lock().unwrap().insert(
                client_order_id.clone(),
                OrderEntry {
                    tracker,
                    symbol: order.symbol.clone(),
                    side: order.side,
                    venue_order_id: None,
                },
            );
        }

        let url = format!("{}/fapi/v1/order", self.config.rest_url);
        let resp = self
            .http
            .post(&url)
            .header("X-MBX-APIKEY", &self.config.api_key)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(signed)
            .send()
            .await
            .map_err(|e| NexusError::Transport(format!("order POST: {e}")))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            if let Some(entry) = self.registry.lock().unwrap().get_mut(&client_order_id) {
                let _ = entry.tracker.apply(OrderEvent::Rejected {
                    reason: body.clone(),
                });
            }
            return Err(NexusError::VenueReject {
                code: "BINANCE_ORDER".into(),
                msg: body,
            });
        }

        let result: serde_json::Value =
            resp.json().await.map_err(|e| {
                NexusError::Transport(format!("order response: {e}"))
            })?;

        let venue_oid = result["orderId"].as_u64();
        let cid = ClientOrderId(client_order_id.clone());

        if let Some(entry) = self.registry.lock().unwrap().get_mut(&client_order_id) {
            entry.venue_order_id = venue_oid;
            let _ = entry.tracker.apply(OrderEvent::Acked);
        }

        Ok(OrderAck {
            client_id: cid,
            venue_order_id: venue_oid.map(|id| id.to_string()),
        })
    }

    async fn place_batch(&self, orders: Vec<NewOrder>) -> Result<Vec<Result<OrderAck>>> {
        let mut out = Vec::with_capacity(orders.len());
        for o in orders {
            out.push(self.place(o).await);
        }
        Ok(out)
    }

    async fn cancel(&self, order: &OrderRef) -> Result<()> {
        let query = format!(
            "symbol={}&origClientOrderId={}",
            order.symbol.venue_native, order.client_id.0
        );
        let signed = sign_request(&query, &self.config.api_secret);

        let url = format!("{}/fapi/v1/order", self.config.rest_url);
        let resp = self
            .http
            .delete(&url)
            .header("X-MBX-APIKEY", &self.config.api_key)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(signed)
            .send()
            .await
            .map_err(|e| NexusError::Transport(format!("cancel: {e}")))?;

        if !resp.status().is_success() {
            return Err(NexusError::VenueReject {
                code: "BINANCE_CANCEL".into(),
                msg: resp.text().await.unwrap_or_default(),
            });
        }
        Ok(())
    }

    async fn cancel_batch(&self, orders: &[OrderRef]) -> Result<Vec<Result<()>>> {
        let mut out = Vec::with_capacity(orders.len());
        for o in orders {
            out.push(self.cancel(o).await);
        }
        Ok(out)
    }

    async fn cancel_all(&self, symbol: Option<&Symbol>) -> Result<()> {
        let targets: Vec<OrderRef> = {
            let reg = self.registry.lock().unwrap();
            reg.iter()
                .filter(|(_, e)| !e.tracker.is_terminal())
                .filter(|(_, e)| symbol.map(|s| e.symbol == *s).unwrap_or(true))
                .map(|(id, e)| OrderRef {
                    symbol: e.symbol.clone(),
                    client_id: ClientOrderId(id.clone()),
                    venue_order_id: e.venue_order_id.map(|i| i.to_string()),
                })
                .collect()
        };
        for t in targets {
            let _ = self.cancel(&t).await;
        }
        Ok(())
    }
}

#[async_trait]
impl PrivateVenue for BinanceVenue {
    fn venue(&self) -> VenueId {
        VenueId::BINANCE_FUT
    }

    async fn subscribe(&self) -> Result<AccountStream> {
        let (tx, rx) = mpsc::channel(4096);
        let registry = Arc::clone(&self.registry);
        let config = self.config.clone();
        let http = self.http.clone();

        tokio::spawn(async move {
            if let Err(e) = run_private_loop(&config, &http, &registry, &tx).await {
                eprintln!("[binance-private] event loop: {e}");
            }
        });

        Ok(rx)
    }

    async fn snapshot(&self) -> Result<AccountSnapshot> {
        let ts = now_ms();
        let query = sign_request("", &self.config.api_secret);
        let resp = self
            .http
            .get(format!("{}/fapi/v2/account", self.config.rest_url))
            .header("X-MBX-APIKEY", &self.config.api_key)
            .query(&parse_query(&query))
            .send()
            .await
            .map_err(|e| NexusError::Transport(format!("account: {e}")))?;

        let account: AccountInfo = resp
            .json()
            .await
            .map_err(|e| NexusError::Transport(format!("account parse: {e}")))?;

        let positions = account
            .positions
            .iter()
            .map(|p| Position {
                symbol: Symbol::new(p.symbol.clone(), "USDT", p.symbol.clone()),
                qty: Decimal::from_str(&p.positionAmt).unwrap_or_default(),
                entry_price: Decimal::from_str(&p.entryPrice).ok(),
                local_recv_ms: ts,
            })
            .collect();

        let balances = account
            .assets
            .iter()
            .map(|a| Balance {
                asset: a.asset.clone(),
                total: Decimal::from_str(&a.walletBalance).unwrap_or_default(),
                available: Decimal::from_str(&a.availableBalance).unwrap_or_default(),
                local_recv_ms: ts,
            })
            .collect();

        Ok(AccountSnapshot {
            positions,
            balances,
            open_orders: Vec::new(),
            local_recv_ms: ts,
        })
    }
}

fn parse_query(q: &str) -> Vec<(String, String)> {
    q.split('&')
        .filter_map(|kv| {
            let mut parts = kv.splitn(2, '=');
            Some((parts.next()?.to_string(), parts.next().unwrap_or("").to_string()))
        })
        .collect()
}

async fn run_private_loop(
    config: &BinanceVenueConfig,
    http: &reqwest::Client,
    registry: &Registry,
    tx: &mpsc::Sender<AccountEvent>,
) -> Result<()> {
    loop {
        let guard = ListenKeyGuard::acquire(http, &config.rest_url, &config.api_key).await?;
        let url = config.private_ws_url(guard.key());
        let (raw_tx, mut raw_rx) = mpsc::unbounded_channel::<String>();
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let (_session, _write) =
            ws::spawn_reader(&url, raw_tx, shutdown_rx, std::time::Duration::from_millis(500))
                .await?;

        while let Some(msg) = raw_rx.recv().await {
            let event: UserStreamEvent = match serde_json::from_str(&msg) {
                Ok(ev) => ev,
                Err(_) => continue,
            };
            match event {
                UserStreamEvent::OrderTradeUpdate(otu) => {
                    for e in map_order_update(registry, &otu.order) {
                        if tx.send(e).await.is_err() {
                            return Ok(());
                        }
                    }
                }
                UserStreamEvent::AccountUpdate(au) => {
                    let ts = now_ms();
                    for pos in &au.data.positions {
                        if tx
                            .send(AccountEvent::PositionUpdate(Position {
                                symbol: Symbol::new(
                                    pos.symbol.clone(),
                                    "USDT",
                                    pos.symbol.clone(),
                                ),
                                qty: Decimal::from_str(&pos.position_amount)
                                    .unwrap_or_default(),
                                entry_price: Decimal::from_str(&pos.entry_price).ok(),
                                local_recv_ms: ts,
                            }))
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                    for bal in &au.data.balances {
                        if tx
                            .send(AccountEvent::BalanceUpdate(Balance {
                                asset: bal.asset.clone(),
                                total: Decimal::from_str(&bal.wallet_balance)
                                    .unwrap_or_default(),
                                available: Decimal::from_str(
                                    &bal.cross_wallet_balance,
                                )
                                .unwrap_or_default(),
                                local_recv_ms: ts,
                            }))
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                }
                UserStreamEvent::ListenKeyExpired => break,
                _ => {}
            }
        }
    }
}

fn map_order_update(registry: &Registry, data: &OrderData) -> Vec<AccountEvent> {
    let mut events = Vec::new();
    let mut reg = registry.lock().unwrap();

    let Some(entry) = reg.get_mut(&data.client_order_id) else {
        // 非本 SDK 订单 — 只发 Fill。
        let px = parse_dec(&data.last_filled_price);
        let qty = parse_dec(&data.last_filled_qty);
        if let (Some(px), Some(qty)) = (px, qty) {
            if !px.is_zero() && !qty.is_zero() {
                events.push(AccountEvent::Fill(Fill {
                    order: OrderRef {
                        symbol: Symbol::new(
                            data.symbol.clone(),
                            "USDT",
                            data.symbol.clone(),
                        ),
                        client_id: ClientOrderId(data.client_order_id.clone()),
                        venue_order_id: Some(data.order_id.to_string()),
                    },
                    side: side_from_str(&data.side),
                    price: px,
                    qty,
                    fee: parse_dec(&data.commission).unwrap_or_default(),
                    fee_currency: data.commission_asset.clone(),
                    is_maker: data.is_maker,
                    venue_ts_ms: data.trade_time as i64,
                    local_recv_ms: now_ms(),
                }));
            }
        }
        return events;
    };

    let order_event = match data.status.as_str() {
        "NEW" => Some(OrderEvent::Acked),
        "PARTIALLY_FILLED" | "FILLED" => {
            let z = parse_dec(&data.executed_qty).unwrap_or_default();
            let delta = z - entry.tracker.filled_qty();
            if delta > Decimal::ZERO {
                Some(OrderEvent::Fill { qty: delta })
            } else {
                None
            }
        }
        "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" => Some(OrderEvent::CancelAcked),
        "REJECTED" => Some(OrderEvent::Rejected {
            reason: "venue rejected".into(),
        }),
        _ => None,
    };

    if let Some(ev) = order_event {
        if let Ok(state) = entry.tracker.apply(ev) {
            events.push(AccountEvent::OrderUpdate(OrderUpdate {
                client_id: ClientOrderId(data.client_order_id.clone()),
                symbol: entry.symbol.clone(),
                state,
                filled_qty: entry.tracker.filled_qty(),
                reason: None,
                local_recv_ms: now_ms(),
            }));
        }
    }

    // 独立 Fill 事件。
    let px = parse_dec(&data.last_filled_price);
    let qty = parse_dec(&data.last_filled_qty);
    if let (Some(px), Some(qty)) = (px, qty) {
        if !px.is_zero() && !qty.is_zero() {
            events.push(AccountEvent::Fill(Fill {
                order: OrderRef {
                    symbol: entry.symbol.clone(),
                    client_id: ClientOrderId(data.client_order_id.clone()),
                    venue_order_id: Some(data.order_id.to_string()),
                },
                side: entry.side,
                price: px,
                qty,
                fee: parse_dec(&data.commission).unwrap_or_default(),
                fee_currency: data.commission_asset.clone(),
                is_maker: data.is_maker,
                venue_ts_ms: data.trade_time as i64,
                local_recv_ms: now_ms(),
            }));
        }
    }

    events
}

fn side_from_str(s: &str) -> Side {
    match s {
        "BUY" => Side::Buy,
        _ => Side::Sell,
    }
}

fn parse_dec(s: &str) -> Option<Decimal> {
    Decimal::from_str(s).ok()
}
