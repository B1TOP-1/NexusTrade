//! Gate 交易执行层（nexus-core 重写版）。
//!
//! 从鹦鹉螺 execution_client 剥离，解耦为 nexus-core `ExecutionVenue`。
//! 复用纯逻辑（build_order_req_param / build_api_envelope / parse_ws_api_response），
//! 替换 nautilus 类型为 nexus-core。
//!
//! 下单走 WS API（`futures.order_place`），P1 WS-first。

use async_trait::async_trait;
use nexus_core::{
    ExecutionVenue, NewOrder, NexusError, OrderAck, OrderKind, OrderRef, Result, Symbol,
    Tif, VenueCapabilities, VenueId,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::execution::build_order_req_param;
use crate::execution::build_api_envelope;

/// Gate 交易配置。
#[derive(Debug, Clone)]
pub struct GateExecConfig {
    pub ws_url: String,
    pub api_key: String,
    pub api_secret: String,
}

/// Gate 交易 venue（nexus-core 版）。
#[derive(Debug)]
pub struct GateVenue {
    config: GateExecConfig,
    ready: AtomicBool,
    /// WS 发送通道（连接层持有）。
    send_tx: tokio::sync::Mutex<Option<mpsc::UnboundedSender<String>>>,
}

impl GateVenue {
    pub fn new(config: GateExecConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            ready: AtomicBool::new(false),
            send_tx: tokio::sync::Mutex::new(None),
        })
    }

    /// 注册 WS 发送通道（连接层 connect 后调用）。
    pub fn set_sender(&self, tx: mpsc::UnboundedSender<String>) {
        *self.send_tx.blocking_lock() = Some(tx);
        self.ready.store(true, Ordering::SeqCst);
    }

    /// 发送 WS 请求。
    async fn send_envelope(&self, envelope: String) -> Result<()> {
        let guard = self.send_tx.lock().await;
        let tx = guard
            .as_ref()
            .ok_or_else(|| NexusError::Transport("gate ws not connected".into()))?;
        tx.send(envelope)
            .map_err(|_| NexusError::Transport("gate ws send failed".into()))
    }

    /// Gate 合约符号：BTCUSDT → BTC_USDT。
    fn contract(symbol: &Symbol) -> String {
        symbol.venue_native.replace("_", "_").to_uppercase()
    }
}

/// Gate TIF 映射（post-only → poc）。
fn gate_tif(tif: Tif) -> &'static str {
    match tif {
        Tif::PostOnly => "poc",
        Tif::Ioc => "ioc",
        Tif::Fok => "fok",
        _ => "gtc",
    }
}

#[async_trait]
impl ExecutionVenue for GateVenue {
    fn venue(&self) -> VenueId {
        VenueId::GATE
    }

    fn capabilities(&self) -> VenueCapabilities {
        VenueCapabilities {
            ws_order_entry: true, // Gate WS API 下单
            batch_orders: false,
            post_only: true,
            reduce_only: true,
            cancel_all_native: false,
            book_fastest_interval_ms: 20, // 20ms 订单簿
            dual_feed: false,
        }
    }

    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    async fn place(&self, order: NewOrder) -> Result<OrderAck> {
        // Gate size 是整数合约数
        let size = order
            .qty
            .to_string()
            .parse::<f64>()
            .map(|f| f.round() as u64)
            .unwrap_or(0);
        if size == 0 {
            return Err(NexusError::InvalidOrder("qty too small".into()));
        }

        let contract = Self::contract(&order.symbol);
        let price = match &order.kind {
            OrderKind::Limit { price } => Some(price.to_string()),
            OrderKind::Market => None,
        };
        let tif = gate_tif(order.tif);
        let client_order_id = order.client_id.0.clone();
        let text = crate::common::credential::normalize_order_text(&client_order_id);

        let req_param = build_order_req_param(
            &contract,
            order.side,
            size,
            price.as_deref(),
            tif,
            order.reduce_only,
            &text,
        )
        .map_err(|e| NexusError::VenueReject {
            code: "GATE_REQ".into(),
            msg: e.to_string(),
        })?;

        let req_id = format!("nx-gate-{}", client_order_id);
        let envelope = build_api_envelope("futures.order_place", &req_id, &req_param, unix_seconds());
        self.send_envelope(envelope).await?;

        // 返回 ACK（真实订单确认由私有流/OrderManager 处理）。
        Ok(OrderAck {
            client_id: order.client_id,
            venue_order_id: None,
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
        // Gate 接受 venue 单号或 text
        let order_id_field = order.venue_order_id.clone().unwrap_or_else(|| {
            crate::common::credential::normalize_order_text(&order.client_id.0)
        });
        let req_param = serde_json::json!({"order_id": order_id_field});
        let req_id = format!("nx-gate-c-{}", order.client_id.0);
        let envelope = build_api_envelope("futures.order_cancel", &req_id, &req_param, unix_seconds());
        self.send_envelope(envelope).await
    }

    async fn cancel_batch(&self, orders: &[OrderRef]) -> Result<Vec<Result<()>>> {
        let mut out = Vec::with_capacity(orders.len());
        for o in orders {
            out.push(self.cancel(o).await);
        }
        Ok(out)
    }

    async fn cancel_all(&self, symbol: Option<&Symbol>) -> Result<()> {
        // Gate 无原生一键撤单，逐单模拟；简单版：报不支持由 SDK 兜底
        let _ = symbol;
        Err(NexusError::Unsupported(
            "gate cancel_all not native; SDK simulates per-order".into(),
        ))
    }
}

fn unix_seconds() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
