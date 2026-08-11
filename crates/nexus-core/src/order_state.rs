//! 订单状态机（原则 P2：严苛流转，无灰色地带）。
//!
//! 状态图见 docs/architecture.md §5。要点：
//! - `InFlight` 超时/断线 → `Unknown`，必须走对账（`Reconciled`）收敛；
//! - 成交可先于 ack 到达（交易所常见），`InFlight` 允许直接吃 `Fill`；
//! - 终态（Filled/Canceled/Rejected/Lost）后任何事件都是错误；
//! - 超量成交（filled > total）立即报错，绝不静默吞。

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// 订单状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderState {
    /// 已创建，未出网。
    PendingSubmit,
    /// 已发送，未收到交易所确认。
    InFlight,
    /// 交易所已挂单，零成交。
    Open,
    /// 部分成交。
    PartiallyFilled,
    /// 撤单已发出，尚未收到交易所确认。
    /// ⚠ cancel request 发出 ≠ 已取消。可过渡到 Canceled 或 Filled
    /// （撤单后仍可能成交，如 taker 单）。
    CancelPending,
    /// 全部成交（终态）。
    Filled,
    /// 已撤销（终态，可能带部分成交）。
    Canceled,
    /// 交易所或本地拒绝（终态，无敞口）。
    Rejected,
    /// 歧义态：结果未知，等待对账。
    Unknown,
    /// 对账后确认订单不存在（终态）。
    Lost,
}

impl OrderState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            OrderState::Filled | OrderState::Canceled | OrderState::Rejected | OrderState::Lost
        )
    }
}

/// 单笔成交（Order/Execution 分离）。
/// 一笔订单可有多次成交，各自独立记录。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Execution {
    /// 成交数量（增量）。
    pub qty: Decimal,
    /// 成交价格。
    pub price: Option<Decimal>,
    /// 手续费（交易所侧）。
    pub fee: Decimal,
    /// 手续费币种。
    pub fee_asset: Option<String>,
    /// 交易所成交时间 T（ms epoch）。
    pub venue_ts_ms: i64,
    /// 本地接收时间（ms epoch）。
    pub local_recv_ms: i64,
}

/// 驱动状态机的事件。由 adapter/SDK 产生，策略侧只读。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderEvent {
    /// 请求已写入网络。
    SubmitSent,
    /// 交易所确认接受。
    Acked,
    /// 交易所或本地拒绝。
    Rejected { reason: String },
    /// 一笔成交（增量数量 + 明细）。
    Fill {
        qty: Decimal,
        price: Option<Decimal>,
        fee: Option<Decimal>,
        fee_asset: Option<String>,
        venue_ts_ms: i64,
    },
    /// 撤单请求已发出（等待交易所确认）。
    CancelSent,
    /// 撤单确认。
    CancelAcked,
    /// 提交后超时未确认。
    SubmitTimeout,
    /// 确认前连接中断。
    ConnectionLost,
    /// 对账结论（仅 Unknown 态合法）。
    Reconciled(ReconcileOutcome),
}

/// 对账结论。`filled` 为交易所侧的累计成交量（绝对值，非增量）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileOutcome {
    Open { filled: Decimal },
    Filled,
    Canceled { filled: Decimal },
    Lost,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum StateError {
    #[error("invalid transition: {state:?} + {event}")]
    InvalidTransition { state: OrderState, event: String },

    #[error("overfill: filled {filled} + {qty} exceeds total {total}")]
    Overfill {
        filled: Decimal,
        qty: Decimal,
        total: Decimal,
    },

    #[error("fill qty must be positive, got {0}")]
    NonPositiveFill(Decimal),
}

/// 单笔订单的状态跟踪器。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderTracker {
    state: OrderState,
    total_qty: Decimal,
    filled_qty: Decimal,
    /// 事件版本号：每次 apply 递增。旧事件（version <= 当前）直接丢弃，
    /// 天然防倒退（如 FILLED 后收到旧 PARTIALLY_FILLED）。
    version: u64,
    /// 成交记录（Order/Execution 分离）：每笔 Fill 一条。
    executions: Vec<Execution>,
}

impl OrderTracker {
    pub fn new(total_qty: Decimal) -> Self {
        Self {
            state: OrderState::PendingSubmit,
            total_qty,
            filled_qty: Decimal::ZERO,
            version: 0,
            executions: Vec::new(),
        }
    }

    pub fn state(&self) -> OrderState {
        self.state
    }

    pub fn filled_qty(&self) -> Decimal {
        self.filled_qty
    }

    pub fn remaining_qty(&self) -> Decimal {
        self.total_qty - self.filled_qty
    }

    pub fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn executions(&self) -> &[Execution] {
        &self.executions
    }

    /// 应用事件，带版本号。旧事件（version <= 当前）直接丢弃，不改变状态。
    /// 非法迁移返回错误且状态不变（调用方决定 fail-closed 动作）。
    pub fn apply_with_version(&mut self, event: OrderEvent, version: u64) -> Result<OrderState, StateError> {
        // 事件版本去重：旧事件丢弃（FILLED 后收到旧 PARTIALLY_FILLED）
        if version <= self.version {
            return Ok(self.state);
        }
        self.version = version;
        self.apply(event)
    }

    /// 应用事件（自动递增版本号）。
    pub fn apply(&mut self, event: OrderEvent) -> Result<OrderState, StateError> {
        use OrderEvent as E;
        use OrderState as S;

        if self.state.is_terminal() {
            return Err(self.invalid(&event));
        }

        let next = match (self.state, &event) {
            (S::PendingSubmit, E::SubmitSent) => S::InFlight,
            (S::PendingSubmit, E::Rejected { .. }) => S::Rejected,

            (S::InFlight, E::Acked) => S::Open,
            (S::InFlight, E::Rejected { .. }) => S::Rejected,
            (S::InFlight, E::SubmitTimeout) => S::Unknown,
            (S::InFlight, E::ConnectionLost) => S::Unknown,
            // 成交先于 ack 到达：直接进入成交路径。
            (S::InFlight, E::Fill { .. }) => self.apply_fill(&event)?,

            (S::Open, E::Fill { .. }) => self.apply_fill(&event)?,
            // 撤单请求发出 → CancelPending（非终态，可能仍成交）。
            (S::Open, E::CancelSent) => S::CancelPending,
            (S::Open, E::CancelAcked) => S::Canceled,

            (S::PartiallyFilled, E::Fill { .. }) => self.apply_fill(&event)?,
            (S::PartiallyFilled, E::CancelSent) => S::CancelPending,
            (S::PartiallyFilled, E::CancelAcked) => S::Canceled,
            // 迟到 ack（fill 先到场景）：无操作，保持现状。
            (S::PartiallyFilled, E::Acked) => S::PartiallyFilled,

            // CancelPending：撤单后仍可能成交（taker 单），或确认取消。
            (S::CancelPending, E::Fill { .. }) => self.apply_fill(&event)?,
            (S::CancelPending, E::CancelAcked) => S::Canceled,
            (S::CancelPending, E::Rejected { .. }) => S::Rejected,

            (S::Unknown, E::Reconciled(outcome)) => self.apply_reconcile(outcome)?,

            _ => return Err(self.invalid(&event)),
        };

        self.state = next;
        Ok(next)
    }

    fn apply_fill(&mut self, event: &OrderEvent) -> Result<OrderState, StateError> {
        let (qty, price, fee, fee_asset, venue_ts_ms) = match event {
            OrderEvent::Fill {
                qty,
                price,
                fee,
                fee_asset,
                venue_ts_ms,
            } => (*qty, *price, *fee, fee_asset.clone(), *venue_ts_ms),
            _ => unreachable!("apply_fill called with non-fill event"),
        };
        if qty <= Decimal::ZERO {
            return Err(StateError::NonPositiveFill(qty));
        }
        if self.filled_qty + qty > self.total_qty {
            return Err(StateError::Overfill {
                filled: self.filled_qty,
                qty,
                total: self.total_qty,
            });
        }
        self.filled_qty += qty;
        // 记录 Execution（Order/Execution 分离）
        self.executions.push(Execution {
            qty,
            price,
            fee: fee.unwrap_or(Decimal::ZERO),
            fee_asset,
            venue_ts_ms,
            local_recv_ms: crate::now_ms(),
        });
        Ok(if self.filled_qty == self.total_qty {
            OrderState::Filled
        } else {
            OrderState::PartiallyFilled
        })
    }

    fn apply_reconcile(&mut self, outcome: &ReconcileOutcome) -> Result<OrderState, StateError> {
        match outcome {
            ReconcileOutcome::Open { filled } => {
                self.set_reconciled_fill(*filled)?;
                Ok(if *filled > Decimal::ZERO {
                    OrderState::PartiallyFilled
                } else {
                    OrderState::Open
                })
            }
            ReconcileOutcome::Filled => {
                self.filled_qty = self.total_qty;
                Ok(OrderState::Filled)
            }
            ReconcileOutcome::Canceled { filled } => {
                self.set_reconciled_fill(*filled)?;
                Ok(OrderState::Canceled)
            }
            ReconcileOutcome::Lost => Ok(OrderState::Lost),
        }
    }

    /// 对账给出的是绝对累计成交量，直接覆盖本地值（交易所是真源）。
    fn set_reconciled_fill(&mut self, filled: Decimal) -> Result<(), StateError> {
        if filled < Decimal::ZERO || filled > self.total_qty {
            return Err(StateError::Overfill {
                filled: self.filled_qty,
                qty: filled,
                total: self.total_qty,
            });
        }
        self.filled_qty = filled;
        Ok(())
    }

    fn invalid(&self, event: &OrderEvent) -> StateError {
        StateError::InvalidTransition {
            state: self.state,
            event: format!("{event:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn tracker() -> OrderTracker {
        OrderTracker::new(dec!(0.010))
    }

    #[test]
    fn happy_path_full_fill() {
        let mut t = tracker();
        assert_eq!(
            t.apply(OrderEvent::SubmitSent).unwrap(),
            OrderState::InFlight
        );
        assert_eq!(t.apply(OrderEvent::Acked).unwrap(), OrderState::Open);
        assert_eq!(
            t.apply(OrderEvent::Fill { qty: dec!(0.004), price: None, fee: None, fee_asset: None, venue_ts_ms: 0 }).unwrap(),
            OrderState::PartiallyFilled
        );
        assert_eq!(
            t.apply(OrderEvent::Fill { qty: dec!(0.006), price: None, fee: None, fee_asset: None, venue_ts_ms: 0 }).unwrap(),
            OrderState::Filled
        );
        assert!(t.is_terminal());
        assert_eq!(t.filled_qty(), dec!(0.010));
        assert_eq!(t.remaining_qty(), Decimal::ZERO);
    }

    #[test]
    fn partial_fill_then_cancel_preserves_filled() {
        let mut t = tracker();
        t.apply(OrderEvent::SubmitSent).unwrap();
        t.apply(OrderEvent::Acked).unwrap();
        t.apply(OrderEvent::Fill { qty: dec!(0.003), price: None, fee: None, fee_asset: None, venue_ts_ms: 0 }).unwrap();
        assert_eq!(
            t.apply(OrderEvent::CancelAcked).unwrap(),
            OrderState::Canceled
        );
        assert_eq!(t.filled_qty(), dec!(0.003));
        assert!(t.is_terminal());
    }

    #[test]
    fn reject_from_in_flight() {
        let mut t = tracker();
        t.apply(OrderEvent::SubmitSent).unwrap();
        assert_eq!(
            t.apply(OrderEvent::Rejected {
                reason: "px".into()
            })
            .unwrap(),
            OrderState::Rejected
        );
        assert!(t.is_terminal());
    }

    #[test]
    fn fill_before_ack_then_late_ack() {
        let mut t = tracker();
        t.apply(OrderEvent::SubmitSent).unwrap();
        assert_eq!(
            t.apply(OrderEvent::Fill { qty: dec!(0.002), price: None, fee: None, fee_asset: None, venue_ts_ms: 0 }).unwrap(),
            OrderState::PartiallyFilled
        );
        // 迟到 ack 是合法无操作。
        assert_eq!(
            t.apply(OrderEvent::Acked).unwrap(),
            OrderState::PartiallyFilled
        );
        assert_eq!(
            t.apply(OrderEvent::Fill { qty: dec!(0.008), price: None, fee: None, fee_asset: None, venue_ts_ms: 0 }).unwrap(),
            OrderState::Filled
        );
    }

    #[test]
    fn unknown_reconciles_to_all_outcomes() {
        // → Filled
        let mut t = tracker();
        t.apply(OrderEvent::SubmitSent).unwrap();
        t.apply(OrderEvent::SubmitTimeout).unwrap();
        assert_eq!(t.state(), OrderState::Unknown);
        assert_eq!(
            t.apply(OrderEvent::Reconciled(ReconcileOutcome::Filled))
                .unwrap(),
            OrderState::Filled
        );
        assert_eq!(t.filled_qty(), dec!(0.010));

        // → Open（零成交）
        let mut t = tracker();
        t.apply(OrderEvent::SubmitSent).unwrap();
        t.apply(OrderEvent::ConnectionLost).unwrap();
        assert_eq!(
            t.apply(OrderEvent::Reconciled(ReconcileOutcome::Open {
                filled: Decimal::ZERO
            }))
            .unwrap(),
            OrderState::Open
        );

        // → PartiallyFilled（对账回填绝对量）
        let mut t = tracker();
        t.apply(OrderEvent::SubmitSent).unwrap();
        t.apply(OrderEvent::ConnectionLost).unwrap();
        assert_eq!(
            t.apply(OrderEvent::Reconciled(ReconcileOutcome::Open {
                filled: dec!(0.004)
            }))
            .unwrap(),
            OrderState::PartiallyFilled
        );
        assert_eq!(t.filled_qty(), dec!(0.004));

        // → Lost
        let mut t = tracker();
        t.apply(OrderEvent::SubmitSent).unwrap();
        t.apply(OrderEvent::SubmitTimeout).unwrap();
        assert_eq!(
            t.apply(OrderEvent::Reconciled(ReconcileOutcome::Lost))
                .unwrap(),
            OrderState::Lost
        );
    }

    #[test]
    fn overfill_is_rejected_and_state_unchanged() {
        let mut t = tracker();
        t.apply(OrderEvent::SubmitSent).unwrap();
        t.apply(OrderEvent::Acked).unwrap();
        t.apply(OrderEvent::Fill { qty: dec!(0.008), price: None, fee: None, fee_asset: None, venue_ts_ms: 0 }).unwrap();
        let err = t.apply(OrderEvent::Fill { qty: dec!(0.005), price: None, fee: None, fee_asset: None, venue_ts_ms: 0 }).unwrap_err();
        assert!(matches!(err, StateError::Overfill { .. }));
        assert_eq!(t.state(), OrderState::PartiallyFilled);
        assert_eq!(t.filled_qty(), dec!(0.008));
    }

    #[test]
    fn invalid_transitions_error_without_mutation() {
        // PendingSubmit 不接受 Fill。
        let mut t = tracker();
        assert!(t.apply(OrderEvent::Fill { qty: dec!(0.001), price: None, fee: None, fee_asset: None, venue_ts_ms: 0 }).is_err());
        assert_eq!(t.state(), OrderState::PendingSubmit);

        // Open 不接受 SubmitTimeout（超时语义只属于 InFlight）。
        let mut t = tracker();
        t.apply(OrderEvent::SubmitSent).unwrap();
        t.apply(OrderEvent::Acked).unwrap();
        assert!(t.apply(OrderEvent::SubmitTimeout).is_err());
        assert_eq!(t.state(), OrderState::Open);

        // 非 Unknown 态不接受 Reconciled。
        assert!(t
            .apply(OrderEvent::Reconciled(ReconcileOutcome::Filled))
            .is_err());
    }

    #[test]
    fn terminal_states_reject_all_events() {
        let mut t = tracker();
        t.apply(OrderEvent::SubmitSent).unwrap();
        t.apply(OrderEvent::Rejected { reason: "x".into() })
            .unwrap();
        assert!(t.apply(OrderEvent::Acked).is_err());
        assert!(t.apply(OrderEvent::Fill { qty: dec!(0.001), price: None, fee: None, fee_asset: None, venue_ts_ms: 0 }).is_err());
        assert!(t.apply(OrderEvent::CancelAcked).is_err());
    }

    #[test]
    fn non_positive_fill_rejected() {
        let mut t = tracker();
        t.apply(OrderEvent::SubmitSent).unwrap();
        t.apply(OrderEvent::Acked).unwrap();
        assert!(matches!(
            t.apply(OrderEvent::Fill { qty: Decimal::ZERO, price: None, fee: None, fee_asset: None, venue_ts_ms: 0 }),
            Err(StateError::NonPositiveFill(_))
        ));
    }

    // ═══ 新状态机能力：CancelPending / 版本号 / Execution 分离 ═══

    /// 便捷构造 Fill 事件。
    fn fill(qty: Decimal) -> OrderEvent {
        OrderEvent::Fill {
            qty,
            price: Some(dec!(100)),
            fee: Some(dec!(0.01)),
            fee_asset: Some("USDT".to_string()),
            venue_ts_ms: 1234,
        }
    }

    #[test]
    fn cancel_pending_then_canceled() {
        // Open → CancelSent → CancelPending → CancelAcked → Canceled
        let mut t = tracker();
        t.apply(OrderEvent::SubmitSent).unwrap();
        t.apply(OrderEvent::Acked).unwrap();
        assert_eq!(t.apply(OrderEvent::CancelSent).unwrap(), OrderState::CancelPending);
        assert_eq!(t.apply(OrderEvent::CancelAcked).unwrap(), OrderState::Canceled);
        assert!(t.is_terminal());
    }

    #[test]
    fn cancel_pending_then_filled() {
        // 撤单后仍可能成交：PartiallyFilled → CancelPending → Fill → Filled
        let mut t = tracker();
        t.apply(OrderEvent::SubmitSent).unwrap();
        t.apply(OrderEvent::Acked).unwrap();
        t.apply(fill(dec!(0.004))).unwrap();
        assert_eq!(t.state(), OrderState::PartiallyFilled);
        assert_eq!(t.apply(OrderEvent::CancelSent).unwrap(), OrderState::CancelPending);
        // 撤单请求发出后，剩余 0.006 成交 → Filled
        assert_eq!(t.apply(fill(dec!(0.006))).unwrap(), OrderState::Filled);
        assert!(t.is_terminal());
        assert_eq!(t.filled_qty(), dec!(0.010));
    }

    #[test]
    fn cancel_pending_is_not_terminal() {
        let mut t = tracker();
        t.apply(OrderEvent::SubmitSent).unwrap();
        t.apply(OrderEvent::Acked).unwrap();
        t.apply(OrderEvent::CancelSent).unwrap();
        assert_eq!(t.state(), OrderState::CancelPending);
        assert!(!t.is_terminal(), "CancelPending 不是终态");
        // 还能收 Fill
        assert_eq!(t.apply(fill(dec!(0.010))).unwrap(), OrderState::Filled);
    }

    #[test]
    fn version_dedupe_ignores_old_events() {
        // FILLED 后收到旧 PARTIALLY_FILLED（低版本）→ 丢弃，不倒退
        let mut t = tracker();
        t.apply_with_version(OrderEvent::SubmitSent, 1).unwrap();
        t.apply_with_version(OrderEvent::Acked, 2).unwrap();
        t.apply_with_version(fill(dec!(0.006)), 3).unwrap();
        assert_eq!(t.state(), OrderState::PartiallyFilled);
        t.apply_with_version(fill(dec!(0.004)), 4).unwrap();
        assert_eq!(t.state(), OrderState::Filled);

        // 旧事件（version=3 的 PARTIALLY_FILLED）→ 丢弃
        let st = t.apply_with_version(fill(dec!(0.001)), 3).unwrap();
        assert_eq!(st, OrderState::Filled);
        assert_eq!(t.state(), OrderState::Filled);
        assert_eq!(t.filled_qty(), dec!(0.010));
        assert_eq!(t.version(), 4);
    }

    #[test]
    fn executions_record_each_fill() {
        // 多次成交各自记录（Order/Execution 分离）
        let mut t = tracker();
        t.apply(OrderEvent::SubmitSent).unwrap();
        t.apply(OrderEvent::Acked).unwrap();
        t.apply(fill(dec!(0.003))).unwrap();
        t.apply(fill(dec!(0.007))).unwrap();
        assert_eq!(t.state(), OrderState::Filled);
        assert_eq!(t.executions().len(), 2);
        assert_eq!(t.executions()[0].qty, dec!(0.003));
        assert_eq!(t.executions()[1].qty, dec!(0.007));
        assert_eq!(t.executions()[0].fee, dec!(0.01));
        assert_eq!(t.executions()[0].fee_asset.as_deref(), Some("USDT"));
        assert_eq!(t.executions()[0].price, Some(dec!(100)));
    }
}
