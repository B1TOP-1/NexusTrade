//! Hyperliquid 执行 + 私有流 adapter：包装 bybot-hype 的 HypeGateway。
//!
//! 薄包装：签名、账户角色解析、市场目录、价格舍入全部由 vendor 层负责。
//! 本层职责：类型转换、cloid 派生、订单状态机驱动、事件映射。
//!
//! M1 边界：gateway 层只暴露 IOC 限价（与实盘验证路径一致），
//! GTC/PostOnly 待 M2 经 conformance 验收后开放。

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bybot_hype::gateway::{
    normalize_user_stream_event, GatewayOrderStatus, GatewayPrivateEvent, HypeGateway,
};
use bybot_hype::user_stream::UserStreamRuntime;
use nexus_core::{
    now_ms, AccountEvent, AccountSnapshot, AccountStream, Balance, ClientOrderId, ConnState,
    Decimal, ExecutionVenue, Fill, NewOrder, NexusError, OrderAck, OrderEvent, OrderRef,
    OrderUpdate, Position, PrivateVenue, Result, Side, Symbol, Tif, VenueCapabilities, VenueId,
};
use tokio::sync::mpsc;

/// 每笔订单的登记项：状态机 + 路由信息。
struct OrderEntry {
    tracker: nexus_core::OrderTracker,
    symbol: Symbol,
    side: Side,
    cloid: String,
}

type Registry = Arc<Mutex<HashMap<String, OrderEntry>>>;
type CloidIndex = Arc<Mutex<HashMap<String, String>>>;

/// Hyperliquid 执行 + 私有流 venue。
pub struct HypeVenue {
    gateway: HypeGateway,
    symbols: Vec<String>,
    registry: Registry,
    cloid_to_id: CloidIndex,
    /// 私有流运行时：与 venue 同生命周期。
    runtime: Mutex<Option<UserStreamRuntime>>,
}

impl HypeVenue {
    /// 连接主网。`symbols` 用 venue 原生符号（决定加载哪些 HIP-3 dex）。
    pub async fn connect_mainnet(
        private_key: &str,
        vault_address: Option<&str>,
        symbols: &[String],
    ) -> Result<Self> {
        let gateway = HypeGateway::connect_mainnet(private_key, vault_address, symbols)
            .await
            .map_err(|e| NexusError::Auth(format!("hype connect: {e}")))?;
        Ok(Self {
            gateway,
            symbols: symbols.to_vec(),
            registry: Arc::new(Mutex::new(HashMap::new())),
            cloid_to_id: Arc::new(Mutex::new(HashMap::new())),
            runtime: Mutex::new(None),
        })
    }

    /// nexus client_id → Hyperliquid cloid（0x + 32 hex，确定性派生）。
    fn cloid_for(client_id: &str) -> String {
        let mut h1 = DefaultHasher::new();
        client_id.hash(&mut h1);
        let a = h1.finish();
        let mut h2 = DefaultHasher::new();
        (client_id, a).hash(&mut h2);
        let b = h2.finish();
        format!("0x{a:016x}{b:016x}")
    }
}

#[async_trait]
impl ExecutionVenue for HypeVenue {
    fn venue(&self) -> VenueId {
        VenueId::HYPE
    }

    fn capabilities(&self) -> VenueCapabilities {
        VenueCapabilities {
            ws_order_entry: false, // gateway 走 HTTP（ws_post 待 M2 接入）
            batch_orders: false,
            post_only: false, // M1 仅 IOC（实盘验证路径）
            reduce_only: true,
            cancel_all_native: false,
            book_fastest_interval_ms: 0,
            dual_feed: false,
        }
    }

    fn is_ready(&self) -> bool {
        true // 与实盘一致：Hype 无就绪门禁
    }

    async fn place(&self, order: NewOrder) -> Result<OrderAck> {
        if order.tif != Tif::Ioc {
            return Err(NexusError::Unsupported(
                "hype M1 only supports IOC (battle-tested path)".into(),
            ));
        }
        let Some(limit_price) = order.price() else {
            return Err(NexusError::Unsupported(
                "hype M1 requires a limit price (marketable IOC)".into(),
            ));
        };
        let signed_quantity = match order.side {
            Side::Buy => order.qty,
            Side::Sell => -order.qty,
        };
        let cloid = Self::cloid_for(&order.client_id.0);

        // 登记先于提交。
        {
            let mut tracker = nexus_core::OrderTracker::new(order.qty);
            let _ = tracker.apply(OrderEvent::SubmitSent);
            self.registry.lock().unwrap().insert(
                order.client_id.0.clone(),
                OrderEntry {
                    tracker,
                    symbol: order.symbol.clone(),
                    side: order.side,
                    cloid: cloid.clone(),
                },
            );
            self.cloid_to_id
                .lock()
                .unwrap()
                .insert(cloid.clone(), order.client_id.0.clone());
        }

        match self
            .gateway
            .place_ioc(
                &order.symbol.venue_native,
                &cloid,
                signed_quantity,
                limit_price,
                order.reduce_only,
            )
            .await
        {
            Ok(submission) => Ok(OrderAck {
                client_id: order.client_id,
                venue_order_id: submission.exchange_order_id,
            }),
            Err(e) => {
                let msg = e.to_string();
                if let Some(entry) = self.registry.lock().unwrap().get_mut(&order.client_id.0) {
                    let _ = entry.tracker.apply(OrderEvent::Rejected {
                        reason: msg.clone(),
                    });
                }
                Err(NexusError::VenueReject {
                    code: "HYPE_SUBMIT".into(),
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
        let cloid = self
            .registry
            .lock()
            .unwrap()
            .get(&order.client_id.0)
            .map(|e| e.cloid.clone())
            .ok_or_else(|| NexusError::Unknown(format!("untracked order {}", order.client_id)))?;
        self.gateway
            .cancel_by_client_order_id(&order.symbol.venue_native, &cloid)
            .await
            .map(|_| ())
            .map_err(|e| NexusError::VenueReject {
                code: "HYPE_CANCEL".into(),
                msg: e.to_string(),
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
impl PrivateVenue for HypeVenue {
    fn venue(&self) -> VenueId {
        VenueId::HYPE
    }

    async fn subscribe(&self) -> Result<AccountStream> {
        let runtime = self
            .gateway
            .spawn_user_stream(&self.symbols)
            .map_err(|e| NexusError::Transport(format!("hype user stream: {e}")))?;
        let mut events = runtime.subscribe();
        *self.runtime.lock().unwrap() = Some(runtime);

        let (tx, rx) = mpsc::channel(4096);
        let registry = Arc::clone(&self.registry);
        let cloid_to_id = Arc::clone(&self.cloid_to_id);

        tokio::spawn(async move {
            while let Ok(event) = events.recv().await {
                let Some(private_event) = normalize_user_stream_event(&event) else {
                    continue;
                };
                for mapped in map_private_event(&registry, &cloid_to_id, private_event) {
                    if tx.send(mapped).await.is_err() {
                        return;
                    }
                }
            }
        });
        Ok(rx)
    }

    async fn snapshot(&self) -> Result<AccountSnapshot> {
        let ts = now_ms();
        let mut positions = Vec::new();
        let mut balances = Vec::new();
        for symbol in &self.symbols {
            let details = self
                .gateway
                .position(symbol)
                .await
                .map_err(|e| NexusError::Transport(format!("hype position {symbol}: {e}")))?;
            positions.push(Position {
                symbol: Symbol::new(symbol.clone(), "USDC", symbol.clone()),
                qty: details.signed_quantity,
                entry_price: Some(details.average_price),
                local_recv_ms: ts,
            });
            if balances.is_empty() {
                balances.push(Balance {
                    asset: "USDC".into(),
                    total: details.available_balance + details.margin_used,
                    available: details.available_balance,
                    local_recv_ms: ts,
                });
            }
        }
        Ok(AccountSnapshot {
            positions,
            balances,
            open_orders: Vec::new(), // IOC-only 路径无常驻单；对账依赖私有流回放
            local_recv_ms: ts,
        })
    }
}

/// 把 gateway 私有事件映射为统一事件，并驱动订单状态机。
///
/// 成交数量口径：状态机由 Order 事件（original−remaining 绝对量差分）驱动，
/// Fill 事件只产出 AccountEvent::Fill，避免双重计数。
fn map_private_event(
    registry: &Registry,
    cloid_to_id: &CloidIndex,
    event: GatewayPrivateEvent,
) -> Vec<AccountEvent> {
    let resolve = |cloid: &Option<String>| -> Option<String> {
        cloid
            .as_deref()
            .and_then(|c| cloid_to_id.lock().unwrap().get(c).cloned())
    };

    match event {
        GatewayPrivateEvent::Connected => {
            vec![AccountEvent::ConnectionState(ConnState::Connected)]
        }
        GatewayPrivateEvent::Disconnected => {
            vec![AccountEvent::ConnectionState(ConnState::Reconnecting)]
        }
        GatewayPrivateEvent::Order {
            client_order_id,
            exchange_order_id: _,
            status,
            original_quantity,
            remaining_quantity,
            occurred_at_ms,
        } => {
            let Some(id) = resolve(&client_order_id) else {
                return Vec::new(); // 非本 SDK 订单
            };
            let mut reg = registry.lock().unwrap();
            let Some(entry) = reg.get_mut(&id) else {
                return Vec::new();
            };

            let filled_abs = (original_quantity - remaining_quantity).max(Decimal::ZERO);
            let delta = filled_abs - entry.tracker.filled_qty();

            let mut updates = Vec::new();
            let events_to_apply: Vec<OrderEvent> = match status {
                GatewayOrderStatus::Acknowledged => vec![OrderEvent::Acked],
                GatewayOrderStatus::PartiallyFilled | GatewayOrderStatus::Filled
                    if delta > Decimal::ZERO =>
                {
                    vec![OrderEvent::Fill { qty: delta }]
                }
                GatewayOrderStatus::Canceled => vec![OrderEvent::CancelAcked],
                GatewayOrderStatus::Rejected => vec![OrderEvent::Rejected {
                    reason: "venue rejected".into(),
                }],
                _ => Vec::new(),
            };
            for ev in events_to_apply {
                if let Ok(state) = entry.tracker.apply(ev) {
                    updates.push(AccountEvent::OrderUpdate(OrderUpdate {
                        client_id: ClientOrderId(id.clone()),
                        symbol: entry.symbol.clone(),
                        state,
                        filled_qty: entry.tracker.filled_qty(),
                        reason: None,
                        local_recv_ms: occurred_at_ms as i64,
                    }));
                }
            }
            updates
        }
        GatewayPrivateEvent::Fill {
            client_order_id,
            exchange_order_id,
            trade_id: _,
            quantity,
            price,
            fee,
            occurred_at_ms,
        } => {
            let Some(id) = resolve(&client_order_id) else {
                return Vec::new();
            };
            let (symbol, side) = {
                let reg = registry.lock().unwrap();
                let Some(e) = reg.get(&id) else {
                    return Vec::new();
                };
                (e.symbol.clone(), e.side)
            };
            vec![AccountEvent::Fill(Fill {
                order: OrderRef {
                    symbol,
                    client_id: ClientOrderId(id),
                    venue_order_id: Some(exchange_order_id),
                },
                side,
                price,
                qty: quantity,
                fee,
                fee_currency: "USDC".into(),
                is_maker: false, // M1 IOC-only 路径
                venue_ts_ms: occurred_at_ms as i64,
                local_recv_ms: now_ms(),
            })]
        }
        GatewayPrivateEvent::Position {
            symbol,
            signed_quantity,
        } => vec![AccountEvent::PositionUpdate(Position {
            symbol: Symbol::new(symbol.clone(), "USDC", symbol),
            qty: signed_quantity,
            entry_price: None,
            local_recv_ms: now_ms(),
        })],
        GatewayPrivateEvent::RuntimeError { .. } => Vec::new(),
    }
}
