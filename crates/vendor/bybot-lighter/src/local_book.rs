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

use std::collections::BTreeMap;

use anyhow::Result;
use rust_decimal::Decimal;

use crate::data::{LighterBookMessageKind, LighterOrderBookMessage, LighterPriceLevel};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LighterDepthSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum LighterBookStatus {
    #[default]
    Uninitialized,
    Ready {
        current_nonce: u64,
    },
    Stale,
    Resubscribing {
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LighterTopOfBook {
    pub bid_price: Decimal,
    pub bid_size: Decimal,
    pub ask_price: Decimal,
    pub ask_size: Decimal,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LighterBookMetrics {
    pub snapshots: u64,
    pub updates: u64,
    pub duplicates: u64,
    pub gaps: u64,
    pub last_nonce: Option<u64>,
    pub last_event_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LighterUpdateOutcome {
    pub applied: bool,
    pub requires_resubscribe: bool,
    pub top_of_book: Option<LighterTopOfBook>,
}

#[derive(Debug, Default)]
pub struct LighterLocalBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    current_nonce: Option<u64>,
    status: LighterBookStatus,
    metrics: LighterBookMetrics,
}

impl LighterLocalBook {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn status(&self) -> &LighterBookStatus {
        &self.status
    }

    #[must_use]
    pub fn nonce(&self) -> Option<u64> {
        self.current_nonce
    }

    #[must_use]
    pub fn metrics(&self) -> LighterBookMetrics {
        self.metrics
    }

    #[must_use]
    pub fn bid_depth(&self) -> usize {
        self.bids.len()
    }

    #[must_use]
    pub fn ask_depth(&self) -> usize {
        self.asks.len()
    }

    #[must_use]
    pub fn top_of_book(&self) -> Option<LighterTopOfBook> {
        let (bid_price, bid_size) = self.bids.iter().next_back()?;
        let (ask_price, ask_size) = self.asks.iter().next()?;
        if bid_price >= ask_price || bid_size.is_zero() || ask_size.is_zero() {
            return None;
        }
        Some(LighterTopOfBook {
            bid_price: *bid_price,
            bid_size: *bid_size,
            ask_price: *ask_price,
            ask_size: *ask_size,
        })
    }

    /// Returns the volume-weighted price for consuming `quote_notional`.
    /// Bids are walked high-to-low for a sell; asks low-to-high for a buy.
    /// Returns `None` unless the complete requested notional can be filled.
    #[must_use]
    pub fn vwap_for_quote_notional(
        &self,
        side: LighterDepthSide,
        quote_notional: Decimal,
    ) -> Option<Decimal> {
        if quote_notional <= Decimal::ZERO {
            return None;
        }
        let mut remaining = quote_notional;
        let mut total_base = Decimal::ZERO;
        let mut total_quote = Decimal::ZERO;
        let levels: Box<dyn Iterator<Item = (&Decimal, &Decimal)> + '_> = match side {
            LighterDepthSide::Bid => Box::new(self.bids.iter().rev()),
            LighterDepthSide::Ask => Box::new(self.asks.iter()),
        };
        for (price, size) in levels {
            let level_quote = *price * *size;
            let take_quote = level_quote.min(remaining);
            total_quote += take_quote;
            total_base += take_quote / *price;
            remaining -= take_quote;
            if remaining <= Decimal::ZERO {
                break;
            }
        }
        if total_base <= Decimal::ZERO || remaining > Decimal::ZERO {
            None
        } else {
            Some(total_quote / total_base)
        }
    }

    pub fn reset_for_reconnect(&mut self, reason: impl Into<String>) {
        self.clear();
        self.status = LighterBookStatus::Resubscribing {
            reason: reason.into(),
        };
    }

    pub fn apply(&mut self, message: &LighterOrderBookMessage) -> Result<LighterUpdateOutcome> {
        match message.kind {
            LighterBookMessageKind::Snapshot => self.apply_snapshot(message),
            LighterBookMessageKind::Update => self.apply_update(message),
        }
    }

    fn apply_snapshot(
        &mut self,
        message: &LighterOrderBookMessage,
    ) -> Result<LighterUpdateOutcome> {
        self.clear();
        apply_levels(&mut self.bids, &message.bids);
        apply_levels(&mut self.asks, &message.asks);
        self.current_nonce = Some(message.nonce);
        self.metrics.snapshots = self.metrics.snapshots.saturating_add(1);
        self.record_message(message);
        self.refresh_status();
        Ok(self.outcome(true, false))
    }

    fn apply_update(&mut self, message: &LighterOrderBookMessage) -> Result<LighterUpdateOutcome> {
        let Some(current_nonce) = self.current_nonce else {
            return Ok(self.outcome(false, false));
        };
        let begin_nonce = message
            .begin_nonce
            .ok_or_else(|| anyhow::anyhow!("Lighter order-book update missing begin_nonce"))?;
        if begin_nonce < current_nonce && message.nonce <= current_nonce {
            self.metrics.duplicates = self.metrics.duplicates.saturating_add(1);
            return Ok(self.outcome(false, false));
        }
        if begin_nonce != current_nonce {
            self.metrics.gaps = self.metrics.gaps.saturating_add(1);
            self.metrics.last_nonce = Some(current_nonce);
            self.metrics.last_event_ms = message.ts_event_ms;
            let reason = format!(
                "Lighter nonce gap: current={current_nonce} begin={begin_nonce} end={}",
                message.nonce
            );
            self.reset_for_reconnect(reason);
            return Ok(self.outcome(false, true));
        }

        apply_levels(&mut self.bids, &message.bids);
        apply_levels(&mut self.asks, &message.asks);
        self.current_nonce = Some(message.nonce);
        self.metrics.updates = self.metrics.updates.saturating_add(1);
        self.record_message(message);
        self.refresh_status();
        Ok(self.outcome(true, false))
    }

    fn record_message(&mut self, message: &LighterOrderBookMessage) {
        self.metrics.last_nonce = Some(message.nonce);
        self.metrics.last_event_ms = message.ts_event_ms;
    }

    fn refresh_status(&mut self) {
        self.status = match (self.current_nonce, self.top_of_book()) {
            (Some(current_nonce), Some(_)) => LighterBookStatus::Ready { current_nonce },
            _ => LighterBookStatus::Stale,
        };
    }

    fn outcome(&self, applied: bool, requires_resubscribe: bool) -> LighterUpdateOutcome {
        LighterUpdateOutcome {
            applied,
            requires_resubscribe,
            top_of_book: self.top_of_book(),
        }
    }

    fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.current_nonce = None;
    }
}

fn apply_levels(side: &mut BTreeMap<Decimal, Decimal>, levels: &[LighterPriceLevel]) {
    for level in levels {
        if level.size.is_zero() {
            side.remove(&level.price);
        } else {
            side.insert(level.price, level.size);
        }
    }
}
