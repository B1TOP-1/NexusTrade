use std::{collections::BTreeMap, time::Duration};

use tokio::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LatencyStep {
    TransportAck,
    OrderConfirmed,
    FillConfirmed,
    CancelConfirmed,
}

#[derive(Debug, Clone)]
pub struct LatencyTrace {
    started: Instant,
    checkpoints: BTreeMap<LatencyStep, Instant>,
}

impl LatencyTrace {
    #[must_use]
    pub fn new(started: Instant) -> Self {
        Self {
            started,
            checkpoints: BTreeMap::new(),
        }
    }

    pub fn record(&mut self, step: LatencyStep, at: Instant) {
        self.checkpoints.entry(step).or_insert(at);
    }

    #[must_use]
    pub fn elapsed(&self, step: LatencyStep) -> Option<Duration> {
        self.checkpoints
            .get(&step)
            .map(|completed| completed.duration_since(self.started))
    }
}
