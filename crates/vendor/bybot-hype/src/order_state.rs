use anyhow::{bail, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    Created,
    Sent,
    Open,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleEvent {
    Sent,
    Open,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderLifecycle {
    status: OrderStatus,
}

impl OrderLifecycle {
    #[must_use]
    pub fn new() -> Self {
        Self {
            status: OrderStatus::Created,
        }
    }

    pub fn apply(&mut self, event: LifecycleEvent) -> Result<OrderStatus> {
        if self.is_terminal() {
            bail!("terminal order cannot transition from {:?}", self.status);
        }
        let next = match (self.status, event) {
            (OrderStatus::Created, LifecycleEvent::Sent) => OrderStatus::Sent,
            (OrderStatus::Sent, LifecycleEvent::Open) => OrderStatus::Open,
            (OrderStatus::Sent | OrderStatus::Open, LifecycleEvent::PartiallyFilled) => {
                OrderStatus::PartiallyFilled
            }
            (
                OrderStatus::Sent | OrderStatus::Open | OrderStatus::PartiallyFilled,
                LifecycleEvent::Filled,
            ) => OrderStatus::Filled,
            (
                OrderStatus::Sent | OrderStatus::Open | OrderStatus::PartiallyFilled,
                LifecycleEvent::Canceled,
            ) => OrderStatus::Canceled,
            (
                OrderStatus::Created
                | OrderStatus::Sent
                | OrderStatus::Open
                | OrderStatus::PartiallyFilled,
                LifecycleEvent::Rejected,
            ) => OrderStatus::Rejected,
            _ => bail!("invalid order transition {:?} -> {event:?}", self.status),
        };
        self.status = next;
        Ok(next)
    }

    #[must_use]
    pub fn status(self) -> OrderStatus {
        self.status
    }

    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self.status,
            OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected
        )
    }
}

impl Default for OrderLifecycle {
    fn default() -> Self {
        Self::new()
    }
}
