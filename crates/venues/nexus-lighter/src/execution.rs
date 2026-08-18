//! Lighter 执行 + 私有流 adapter：包装 bybot-lighter 的 LighterExecutionClient。
//!
//! 薄包装：签名、nonce、限流、账户快照就绪门禁全部由 vendor 层负责。
//! 本层职责：类型转换、client_order_index 派生、订单状态机驱动、事件映射。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bybot_lighter::data::LighterMarketSpec;
use bybot_lighter::execution::LighterExecutionEffect;
use bybot_lighter::execution_client::{
    LighterCancelRequest, LighterExecutionClient, LighterExecutionConfig, LighterExecutionRuntime,
    LighterOrderOutcomeUnknown, LighterOrderRequest, LighterOrderType, LighterTimeInForce,
};
use bybot_lighter::http::LighterHttpClient;
use nexus_core::{
    now_ms, AccountEvent, AccountSnapshot, AccountStream, Balance, ClientOrderId, Decimal,
    ExecutionVenue, Fill, NewOrder, NexusError, OrderAck, OrderEvent, OrderKind, OrderRef,
    OrderState, OrderTracker, OrderUpdate, Position, PrivateVenue, Result, Side, Symbol,
    VenueCapabilities, VenueId,
};
use rust_decimal::prelude::FromPrimitive;
use std::str::FromStr;
use tokio::sync::mpsc;

/// uint48 上限（Lighter client_order_index 空间）。
const CLIENT_INDEX_MASK: u64 = (1 << 48) - 1;

/// 每笔订单的登记项：状态机 + 路由信息。
struct OrderEntry {
    tracker: OrderTracker,
    symbol: Symbol,
    side: Side,
    client_order_index: u64,
}

type Registry = Arc<Mutex<HashMap<String, OrderEntry>>>;

/// Lighter 执行 venue 配置。
#[derive(Debug, Clone)]
pub struct LighterVenueConfig {
    pub http_url: String,
    pub private_ws_url: String,
    pub account_index: u64,
    pub api_key_index: u8,
    pub chain_id: u32,
    /// client_order_index 种子（建议启动时间戳），保证跨进程重启唯一。
    pub index_seed: u64,
}

impl LighterVenueConfig {
    pub fn mainnet(account_index: u64, api_key_index: u8, index_seed: u64) -> Self {
        Self {
            http_url: "https://mainnet.zklighter.elliot.ai".to_string(),
            private_ws_url: "wss://mainnet.zklighter.elliot.ai/stream".to_string(),
            account_index,
            api_key_index,
            chain_id: 304,
            index_seed,
        }
    }
}

/// Lighter 执行 + 私有流 venue。
pub struct LighterVenue {
    client: LighterExecutionClient,
    specs: Vec<LighterMarketSpec>,
    registry: Registry,
    /// client_order_index 反查表（私有流事件只带 index）。
    index_to_id: Arc<Mutex<HashMap<u64, String>>>,
    index_counter: AtomicU64,
    index_seed: u64,
    /// HTTP remains the default until this is explicitly enabled by the caller.
    ws_order_entry: AtomicBool,
    /// 私有流运行时：与 venue 同生命周期（Drop 即 abort 底层任务）。
    runtime: Mutex<Option<LighterExecutionRuntime>>,
}

impl LighterVenue {
    /// 连接：加载市场规格 → 建执行客户端 → 起私有流 → 等账户快照就绪。
    pub async fn connect(config: LighterVenueConfig, private_key: &str) -> Result<Self> {
        let exec_config = LighterExecutionConfig::new(
            config.http_url.clone(),
            config.private_ws_url.clone(),
            config.account_index,
            config.api_key_index,
            config.chain_id,
        )
        .map_err(|e| NexusError::Auth(format!("lighter config: {e}")))?;

        let http = LighterHttpClient::new(&config.http_url)
            .map_err(|e| NexusError::Transport(format!("lighter http: {e}")))?;
        let specs = http
            .market_specs()
            .await
            .map_err(|e| NexusError::Transport(format!("lighter specs: {e}")))?;

        let client = LighterExecutionClient::connect(exec_config, private_key)
            .await
            .map_err(|e| NexusError::Auth(format!("lighter connect: {e}")))?;
        client
            .initialize()
            .await
            .map_err(|e| NexusError::Transport(format!("lighter nonce init: {e}")))?;

        Ok(Self {
            client,
            specs,
            registry: Arc::new(Mutex::new(HashMap::new())),
            index_to_id: Arc::new(Mutex::new(HashMap::new())),
            index_counter: AtomicU64::new(0),
            index_seed: config.index_seed,
            ws_order_entry: AtomicBool::new(false),
            runtime: Mutex::new(None),
        })
    }

    /// 启动私有流运行时（PrivateVenue::subscribe 内部调用）。
    async fn spawn_runtime(
        &self,
    ) -> Result<tokio::sync::broadcast::Receiver<LighterExecutionEffect>> {
        let runtime = self
            .client
            .spawn_private_runtime()
            .await
            .map_err(|e| NexusError::Transport(format!("lighter private runtime: {e}")))?;
        let rx = runtime.subscribe();
        // 运行时与 venue 同生命周期；venue Drop 时底层 WS 任务随之 abort。
        *self.runtime.lock().unwrap() = Some(runtime);
        Ok(rx)
    }

    fn next_client_index(&self) -> u64 {
        let n = self.index_counter.fetch_add(1, Ordering::Relaxed);
        ((self.index_seed.wrapping_add(n)) & CLIENT_INDEX_MASK).max(1)
    }

    fn symbol_by_market_id(&self, market_id: u64) -> Option<Symbol> {
        self.specs
            .iter()
            .find(|s| s.market_id == market_id)
            .map(|s| Symbol::new(s.symbol.clone(), "USDC", s.symbol.clone()))
    }

    /// Enables the separately connected, single-flight WS sendTx path.
    /// This is an explicit opt-in and does not send an order by itself.
    pub async fn enable_ws_order_entry(&self) -> Result<()> {
        self.client
            .enable_ws_submission()
            .await
            .map_err(|error| NexusError::Transport(format!("lighter WS submit connect: {error}")))?;
        self.ws_order_entry.store(true, Ordering::Release);
        Ok(())
    }

    pub fn disable_ws_order_entry(&self) {
        self.ws_order_entry.store(false, Ordering::Release);
    }

    fn map_tif(order: &NewOrder) -> Result<LighterTimeInForce> {
        use nexus_core::Tif;
        match order.tif {
            Tif::Ioc => Ok(LighterTimeInForce::ImmediateOrCancel),
            Tif::Gtc => Ok(LighterTimeInForce::GoodTilTime),
            Tif::PostOnly => Ok(LighterTimeInForce::PostOnly),
            Tif::Fok => Err(NexusError::Unsupported("lighter FOK".into())),
        }
    }
}

#[async_trait]
impl ExecutionVenue for LighterVenue {
    fn venue(&self) -> VenueId {
        VenueId::LIGHTER
    }

    fn capabilities(&self) -> VenueCapabilities {
        VenueCapabilities {
            ws_order_entry: self.ws_order_entry.load(Ordering::Acquire),
            batch_orders: false,
            post_only: true,
            reduce_only: true,
            cancel_all_native: false,
            book_fastest_interval_ms: 0, // 增量流无节流档位
            dual_feed: false,
        }
    }

    fn is_ready(&self) -> bool {
        self.client.account_snapshot_ready()
    }

    async fn place(&self, order: NewOrder) -> Result<OrderAck> {
        let signed_quantity = match order.side {
            Side::Buy => order.qty,
            Side::Sell => -order.qty,
        };
        let (order_type, limit_price) = match &order.kind {
            OrderKind::Limit { price } => (LighterOrderType::Limit, Some(*price)),
            OrderKind::Market => (LighterOrderType::Market, None),
        };
        let tif = Self::map_tif(&order)?;
        let client_order_index = self.next_client_index();

        let request = LighterOrderRequest {
            symbol: order.symbol.venue_native.clone(),
            client_order_id: order.client_id.0.clone(),
            client_order_index,
            signed_quantity,
            limit_price,
            order_type,
            time_in_force: tif,
            reduce_only: order.reduce_only,
        };

        // 登记先于提交：提交失败也保留登记（状态机转 Rejected/Unknown）。
        {
            let mut tracker = OrderTracker::new(order.qty);
            let _ = tracker.apply(OrderEvent::SubmitSent);
            self.registry.lock().unwrap().insert(
                order.client_id.0.clone(),
                OrderEntry {
                    tracker,
                    symbol: order.symbol.clone(),
                    side: order.side,
                    client_order_index,
                },
            );
            self.index_to_id
                .lock()
                .unwrap()
                .insert(client_order_index, order.client_id.0.clone());
        }

        let submit_result = if self.ws_order_entry.load(Ordering::Acquire) {
            self.client.submit_order_ws(&request).await
        } else {
            self.client.submit_order(&request).await
        };
        match submit_result {
            Ok(_receipt) => Ok(OrderAck {
                client_id: order.client_id,
                venue_order_id: None, // order_index 经私有流异步到达
            }),
            Err(e) => {
                let msg = e.to_string();
                if e.downcast_ref::<LighterOrderOutcomeUnknown>().is_some() {
                    if let Some(entry) = self.registry.lock().unwrap().get_mut(&order.client_id.0) {
                        let _ = entry.tracker.apply(OrderEvent::SubmitTimeout);
                    }
                    return Err(NexusError::Unknown(msg));
                }
                if let Some(entry) = self.registry.lock().unwrap().get_mut(&order.client_id.0) {
                    let _ = entry.tracker.apply(OrderEvent::Rejected {
                        reason: msg.clone(),
                    });
                }
                Err(NexusError::VenueReject {
                    code: "LIGHTER_SUBMIT".into(),
                    msg,
                })
            }
        }
    }

    async fn place_batch(&self, orders: Vec<NewOrder>) -> Result<Vec<Result<OrderAck>>> {
        let mut out = Vec::with_capacity(orders.len());
        for order in orders {
            out.push(self.place(order).await);
        }
        Ok(out)
    }

    async fn cancel(&self, order: &OrderRef) -> Result<()> {
        let client_order_index = self
            .registry
            .lock()
            .unwrap()
            .get(&order.client_id.0)
            .map(|e| e.client_order_index)
            .ok_or_else(|| NexusError::Unknown(format!("untracked order {}", order.client_id)))?;
        let order_index = self
            .client
            .venue_order_index(client_order_index)
            .ok_or_else(|| {
                NexusError::Unknown(format!("no venue order_index yet for {}", order.client_id))
            })?;

        let request = LighterCancelRequest {
            symbol: order.symbol.venue_native.clone(),
            client_order_id: order.client_id.0.clone(),
            client_order_index: Some(client_order_index),
            order_index,
        };
        let cancel_result = if self.ws_order_entry.load(Ordering::Acquire) {
            self.client.cancel_order_ws(&request).await
        } else {
            self.client.cancel_order(&request).await
        };
        cancel_result
            .map(|_| ())
            .map_err(|e| {
                let msg = e.to_string();
                if e.downcast_ref::<LighterOrderOutcomeUnknown>().is_some() {
                    NexusError::Unknown(msg)
                } else {
                    NexusError::VenueReject {
                        code: "LIGHTER_CANCEL".into(),
                        msg,
                    }
                }
            })
    }

    async fn cancel_batch(&self, orders: &[OrderRef]) -> Result<Vec<Result<()>>> {
        let mut out = Vec::with_capacity(orders.len());
        for order in orders {
            out.push(self.cancel(order).await);
        }
        Ok(out)
    }

    async fn cancel_all(&self, symbol: Option<&Symbol>) -> Result<()> {
        // 无原生一键撤单：遍历本 venue 登记的非终态订单逐一撤。
        let targets: Vec<OrderRef> = {
            let reg = self.registry.lock().unwrap();
            reg.iter()
                .filter(|(_, e)| !e.tracker.is_terminal())
                .filter(|(_, e)| symbol.map(|s| e.symbol == *s).unwrap_or(true))
                .map(|(id, e)| OrderRef {
                    symbol: e.symbol.clone(),
                    client_id: ClientOrderId(id.clone()),
                    venue_order_id: None,
                })
                .collect()
        };
        for target in targets {
            // 已消亡的 IOC 单撤单失败是常态，尽力而为不中断。
            let _ = self.cancel(&target).await;
        }
        Ok(())
    }
}

#[async_trait]
impl PrivateVenue for LighterVenue {
    fn venue(&self) -> VenueId {
        VenueId::LIGHTER
    }

    async fn subscribe(&self) -> Result<AccountStream> {
        let mut effects = self.spawn_runtime().await?;
        let (tx, rx) = mpsc::channel(4096);
        let registry = Arc::clone(&self.registry);
        let index_to_id = Arc::clone(&self.index_to_id);
        let specs = self.specs.clone();

        tokio::spawn(async move {
            while let Ok(effect) = effects.recv().await {
                for event in map_effect(&registry, &index_to_id, &specs, effect) {
                    if tx.send(event).await.is_err() {
                        return; // 订阅方已放弃
                    }
                }
            }
        });
        Ok(rx)
    }

    async fn snapshot(&self) -> Result<AccountSnapshot> {
        let snap = self
            .client
            .account_snapshot()
            .await
            .map_err(|e| NexusError::Transport(format!("lighter snapshot: {e}")))?;
        let ts = now_ms();
        let positions = snap
            .positions
            .iter()
            .filter_map(|p| {
                self.symbol_by_market_id(p.market_id)
                    .map(|symbol| Position {
                        symbol,
                        qty: p.signed_quantity,
                        entry_price: Some(p.average_price),
                        local_recv_ms: ts,
                    })
            })
            .collect();
        Ok(AccountSnapshot {
            positions,
            balances: vec![Balance {
                asset: "USDC".into(),
                total: snap.collateral,
                available: snap.available_balance,
                local_recv_ms: ts,
            }],
            open_orders: Vec::new(), // vendor 层无 open orders 查询；对账依赖私有流回放
            local_recv_ms: ts,
        })
    }
}

/// 把 vendor 执行效果映射为统一事件，并驱动订单状态机。
fn map_effect(
    registry: &Registry,
    index_to_id: &Arc<Mutex<HashMap<u64, String>>>,
    specs: &[LighterMarketSpec],
    effect: LighterExecutionEffect,
) -> Vec<AccountEvent> {
    let resolve_id = |cid: &str, coi: Option<u64>| -> Option<String> {
        if !cid.is_empty() && registry.lock().unwrap().contains_key(cid) {
            return Some(cid.to_string());
        }
        coi.and_then(|i| index_to_id.lock().unwrap().get(&i).cloned())
    };

    match effect {
        LighterExecutionEffect::Submitted { .. } => Vec::new(), // place() 已登记 SubmitSent
        LighterExecutionEffect::Accepted {
            client_order_id,
            client_order_index,
            order_index,
            ts_event_ms,
        } => {
            let Some(id) = resolve_id(&client_order_id, client_order_index) else {
                return Vec::new(); // 非本 SDK 订单
            };
            drive(registry, &id, OrderEvent::Acked, None, ts_event_ms)
                .map(|mut u| {
                    if let Some(oid) = order_index {
                        u.reason = Some(format!("order_index={oid}"));
                    }
                    vec![AccountEvent::OrderUpdate(u)]
                })
                .unwrap_or_default()
        }
        LighterExecutionEffect::Fill {
            client_order_id,
            client_order_index,
            trade_id: _,
            quantity,
            price,
            fee,
            synthetic: _,
            ts_event_ms,
        } => {
            let Some(id) = resolve_id(&client_order_id, client_order_index) else {
                return Vec::new();
            };
            let Some(qty) = Decimal::from_str(&quantity).ok() else {
                return Vec::new();
            };
            let px = price
                .as_deref()
                .and_then(|p| Decimal::from_str(p).ok())
                .unwrap_or(Decimal::ZERO);
            let (symbol, side) = {
                let reg = registry.lock().unwrap();
                let Some(e) = reg.get(&id) else {
                    return Vec::new();
                };
                (e.symbol.clone(), e.side)
            };
            let mut events = Vec::new();
            if let Some(update) = drive(
                registry,
                &id,
                OrderEvent::Fill {
                    qty,
                    price: None,
                    fee: None,
                    fee_asset: None,
                    venue_ts_ms: ts_event_ms as i64,
                },
                None,
                ts_event_ms,
            )
            {
                events.push(AccountEvent::OrderUpdate(update));
            }
            events.push(AccountEvent::Fill(Fill {
                order: OrderRef {
                    symbol: symbol.clone(),
                    client_id: ClientOrderId(id),
                    venue_order_id: None,
                },
                side,
                price: px,
                qty,
                // vendor 回传原始整数计费单位，此处保持原值不猜比例（对账层换算）。
                fee: Decimal::from_i64(fee).unwrap_or(Decimal::ZERO),
                fee_currency: "USDC_RAW".into(),
                is_maker: false, // Lighter 私有流未区分，taker 策略默认
                venue_ts_ms: ts_event_ms as i64,
                local_recv_ms: now_ms(),
            }));
            events
        }
        LighterExecutionEffect::Position { position } => position_effect(specs, position),
        // These effects are intentionally retained in the vendor stream but do not
        // have a lossless equivalent in nexus-core's AccountEvent yet.
        LighterExecutionEffect::ExternalTrade { .. } | LighterExecutionEffect::Funding { .. } => {
            Vec::new()
        }
        LighterExecutionEffect::Canceled {
            client_order_id,
            client_order_index,
            reason,
            ts_event_ms,
        } => {
            let Some(id) = resolve_id(&client_order_id, client_order_index) else {
                return Vec::new();
            };
            drive(
                registry,
                &id,
                OrderEvent::CancelAcked,
                Some(reason),
                ts_event_ms,
            )
            .map(|u| vec![AccountEvent::OrderUpdate(u)])
            .unwrap_or_default()
        }
        LighterExecutionEffect::Rejected {
            client_order_id,
            client_order_index,
            reason,
            ts_event_ms,
        } => {
            let Some(id) = resolve_id(&client_order_id, client_order_index) else {
                return Vec::new();
            };
            drive(
                registry,
                &id,
                OrderEvent::Rejected {
                    reason: reason.clone(),
                },
                Some(reason),
                ts_event_ms,
            )
            .map(|u| vec![AccountEvent::OrderUpdate(u)])
            .unwrap_or_default()
        }
    }
}

fn position_effect(
    specs: &[LighterMarketSpec],
    position: bybot_lighter::execution::LighterPrivatePositionEvent,
) -> Vec<AccountEvent> {
    let symbol = specs
        .iter()
        .find(|spec| spec.market_id == position.market_id)
        .map(|spec| Symbol::new(spec.symbol.clone(), "USDC", spec.symbol.clone()));
    symbol
        .map(|symbol| {
            vec![AccountEvent::PositionUpdate(Position {
                symbol,
                qty: position.signed_quantity,
                entry_price: Some(position.average_price),
                local_recv_ms: now_ms(),
            })]
        })
        .unwrap_or_default()
}

/// 驱动状态机并产出 OrderUpdate。非法迁移（如迟到事件打在终态上）丢弃。
fn drive(
    registry: &Registry,
    id: &str,
    event: OrderEvent,
    reason: Option<String>,
    ts_event_ms: u64,
) -> Option<OrderUpdate> {
    let mut reg = registry.lock().unwrap();
    let entry = reg.get_mut(id)?;
    let state: OrderState = entry.tracker.apply(event).ok()?;
    Some(OrderUpdate {
        client_id: ClientOrderId(id.to_string()),
        symbol: entry.symbol.clone(),
        state,
        filled_qty: entry.tracker.filled_qty(),
        reason,
        local_recv_ms: ts_event_ms as i64,
    })
}
