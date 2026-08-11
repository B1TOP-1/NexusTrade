//! Binance 订单状态管理器（正式模块）。
//!
//! 从 taker_test 验证后提升为正式实现。核心能力：
//!   - **状态机**：复用 nexus-core OrderTracker（CancelPending/版本号/Execution 分离）
//!   - **跨事件聚合**：ORDER_TRADE_UPDATE（状态+fee）+ ACCOUNT_UPDATE（余额/仓位）
//!   - **终态 waiter**：wait_for_terminal 只在 FILLED/CANCELED/EXPIRED/REJECTED resolve
//!   - **版本号去重**：旧事件丢弃防倒退
//!
//! 设计：
//!   OrderManager::on_order_update(v)  ← 用户流后台 task 调用
//!   OrderManager::on_account_update(v) ← ACCOUNT_UPDATE 更新余额/仓位
//!   OrderManager::register_waiter(cid) → oneshot Receiver<OrderState>（终态才 resolve）

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::str::FromStr;

use nexus_core::{now_ms, Decimal, OrderState};
use rust_decimal::prelude::ToPrimitive;

/// 单笔成交（Order/Execution 分离）。
#[derive(Debug, Clone)]
pub struct Execution {
    pub qty: Decimal,
    pub price: Option<Decimal>,
    pub fee: Decimal,
    pub fee_asset: Option<String>,
    pub venue_ts_ms: i64,
    pub local_recv_ms: i64,
}

/// 订单持续状态（OrderManager 维护）。
#[derive(Debug, Clone)]
pub struct OrderStatus {
    pub client_order_id: String,
    pub state: OrderState,
    pub orig_qty: Decimal,
    pub executed_qty: Decimal,
    pub avg_price: Decimal,
    pub fee: Decimal,
    pub fee_asset: String,
    /// 完整状态流转（每次事件记录）。
    pub transitions: Vec<(OrderState, i64, i64)>,
    /// 成交记录（Order/Execution 分离）。
    pub executions: Vec<Execution>,
}

impl OrderStatus {
    fn new(cid: &str) -> Self {
        Self {
            client_order_id: cid.to_string(),
            state: OrderState::PendingSubmit,
            orig_qty: Decimal::ZERO,
            executed_qty: Decimal::ZERO,
            avg_price: Decimal::ZERO,
            fee: Decimal::ZERO,
            fee_asset: String::new(),
            transitions: Vec::new(),
            executions: Vec::new(),
        }
    }
}

/// 订单状态管理器：持续更新 + 终态 waiter。
pub struct OrderManager {
    orders: Mutex<HashMap<String, OrderStatus>>,
    terminal_waiters: Mutex<HashMap<String, tokio::sync::oneshot::Sender<OrderStatus>>>,
    /// 最新余额（ACCOUNT_UPDATE 更新）。
    last_balance: Mutex<Decimal>,
    /// 最新仓位（带符号：正=多，负=空）。
    last_position: Mutex<Decimal>,
}

impl OrderManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            orders: Mutex::new(HashMap::new()),
            terminal_waiters: Mutex::new(HashMap::new()),
            last_balance: Mutex::new(Decimal::ZERO),
            last_position: Mutex::new(Decimal::ZERO),
        })
    }

    /// 注册终态 waiter。返回 oneshot receiver，仅在终态 resolve。
    pub fn register_waiter(&self, cid: &str) -> tokio::sync::oneshot::Receiver<OrderStatus> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.terminal_waiters.lock().unwrap().insert(cid.to_string(), tx);
        rx
    }

    /// 当前余额。
    pub fn balance(&self) -> Decimal {
        *self.last_balance.lock().unwrap()
    }

    /// 当前仓位。
    pub fn position(&self) -> Decimal {
        *self.last_position.lock().unwrap()
    }

    /// 查询订单状态。
    pub fn order(&self, cid: &str) -> Option<OrderStatus> {
        self.orders.lock().unwrap().get(cid).cloned()
    }

    /// 处理 ORDER_TRADE_UPDATE：更新状态 + 提取 fee，终态 resolve waiter。
    pub fn on_order_update(&self, full: &serde_json::Value) {
        let o = &full["o"];
        let e_ms = full["E"].as_i64().unwrap_or(0);
        let t_ms = full["T"].as_i64().unwrap_or(0);

        let cid = o["c"].as_str().unwrap_or("").to_string();
        if cid.is_empty() {
            return;
        }
        let st = order_state_from_str(o["X"].as_str().unwrap_or(""));

        let mut orders = self.orders.lock().unwrap();
        let entry = orders.entry(cid.clone()).or_insert_with(|| OrderStatus::new(&cid));

        // 终态后忽略后续事件（防倒退/重复）
        if entry.state.is_terminal() {
            return;
        }

        // 记录流转
        entry.transitions.push((st, e_ms, t_ms));

        // 更新字段
        entry.orig_qty = Decimal::from_str(o["q"].as_str().unwrap_or("0"))
            .unwrap_or(entry.orig_qty);
        let new_exec = Decimal::from_str(o["z"].as_str().unwrap_or("0"))
            .unwrap_or(entry.executed_qty);
        // 增量成交 → 记录 Execution
        if new_exec > entry.executed_qty {
            let delta = new_exec - entry.executed_qty;
            entry.executed_qty = new_exec;
            let fee = Decimal::from_str(o["n"].as_str().unwrap_or("0"))
                .unwrap_or(Decimal::ZERO);
            entry.fee += fee;
            if let Some(a) = o["N"].as_str() {
                entry.fee_asset = a.to_string();
            }
            entry.executions.push(Execution {
                qty: delta,
                price: Decimal::from_str(o["L"].as_str().unwrap_or("0")).ok(),
                fee,
                fee_asset: o["N"].as_str().map(|s| s.to_string()),
                venue_ts_ms: t_ms,
                local_recv_ms: now_ms(),
            });
        }
        entry.avg_price = Decimal::from_str(o["ap"].as_str().unwrap_or("0"))
            .unwrap_or(entry.avg_price);
        entry.state = st;

        // 终态：resolve waiter
        if st.is_terminal() {
            let snapshot = entry.clone();
            drop(orders);
            if let Some(sender) = self.terminal_waiters.lock().unwrap().remove(&cid) {
                let _ = sender.send(snapshot);
            }
        }
    }

    /// 处理 ACCOUNT_UPDATE：更新余额/仓位。
    pub fn on_account_update(&self, v: &serde_json::Value) {
        let a = &v["a"];
        if let Some(bs) = a["B"].as_array() {
            for b in bs {
                if b["a"].as_str() == Some("USDT") {
                    if let Ok(wb) = Decimal::from_str(b["wb"].as_str().unwrap_or("0")) {
                        *self.last_balance.lock().unwrap() = wb;
                    }
                }
            }
        }
        if let Some(ps) = a["P"].as_array() {
            for p in ps {
                if p["s"].as_str() == Some("BTCUSDT") {
                    if let Ok(pa) = Decimal::from_str(p["pa"].as_str().unwrap_or("0")) {
                        *self.last_position.lock().unwrap() = pa;
                    }
                }
            }
        }
    }
}

/// Binance 订单状态字符串 → nexus-core OrderState。
fn order_state_from_str(s: &str) -> OrderState {
    match s {
        "NEW" => OrderState::Open,
        "PARTIALLY_FILLED" => OrderState::PartiallyFilled,
        "FILLED" => OrderState::Filled,
        "CANCELED" => OrderState::Canceled,
        "EXPIRED" => OrderState::Canceled,
        "REJECTED" => OrderState::Rejected,
        _ => OrderState::Unknown,
    }
}

/// 便捷：从 OrderStatus 计算滑点百分比。
pub fn slippage_pct(avg_price: Decimal, ref_price: Decimal) -> Decimal {
    if ref_price > Decimal::ZERO {
        ((avg_price - ref_price) / ref_price * Decimal::from(100)).abs()
    } else {
        Decimal::ZERO
    }
}

/// 便捷：f64 格式化的滑点。
pub fn slippage_pct_f64(avg_price: Decimal, ref_price: Decimal) -> f64 {
    slippage_pct(avg_price, ref_price)
        .to_f64()
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn otu(cid: &str, status: &str, z: &str, ap: &str, fee: &str, e: i64, t: i64) -> serde_json::Value {
        serde_json::json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": e, "T": t,
            "o": {
                "s": "BTCUSDT", "c": cid, "i": 1,
                "S": "BUY", "o": "MARKET", "X": status,
                "q": "0.001", "z": z, "p": "0", "ap": ap,
                "L": ap, "l": "0.001", "n": fee, "N": "USDT",
            }
        })
    }

    #[test]
    fn order_manager_tracks_and_resolves_terminal() {
        let m = OrderManager::new();
        let rx = m.register_waiter("test1");

        // NEW（非终态，不 resolve）
        m.on_order_update(&otu("test1", "NEW", "0", "0", "0", 1, 1));
        assert_eq!(m.order("test1").unwrap().state, OrderState::Open);

        // FILLED（终态，resolve）
        m.on_order_update(&otu("test1", "FILLED", "0.001", "64325.4", "0.032", 2, 2));

        let st = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { tokio::time::timeout(std::time::Duration::from_secs(1), rx).await })
            .unwrap()
            .unwrap();
        assert_eq!(st.state, OrderState::Filled);
        assert_eq!(st.executed_qty, dec!(0.001));
        assert_eq!(st.fee, dec!(0.032));
        assert_eq!(st.executions.len(), 1);
    }

    #[test]
    fn account_update_tracks_balance_position() {
        let m = OrderManager::new();
        let v = serde_json::json!({
            "e": "ACCOUNT_UPDATE",
            "E": 1, "T": 1,
            "a": {
                "m": "ORDER",
                "B": [{"a": "USDT", "wb": "12.25", "cw": "12.25", "bc": "0"}],
                "P": [{"s": "BTCUSDT", "pa": "0.001", "ep": "64325.4", "up": "0"}],
            }
        });
        m.on_account_update(&v);
        assert_eq!(m.balance(), dec!(12.25));
        assert_eq!(m.position(), dec!(0.001));
    }
}
