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

//! Order quantity/price scaling and L2 nonce management for Lighter execution.
//!
//! Lighter signs integer `BaseAmount` (i64) and `Price` (u32) fields. Decimal
//! quantities/prices are scaled by the per-market multipliers (`10^decimals`)
//! exposed by the instrument provider. Scaling truncates toward zero to match
//! the reference implementation (`int(quantity * multiplier)`), never rounding
//! up past the venue tick.

use std::sync::atomic::{AtomicI64, Ordering};

use rust_decimal::{prelude::ToPrimitive, Decimal};

/// Scales a decimal quantity to Lighter's integer `BaseAmount`.
///
/// Truncates toward zero, matching the reference `int(quantity * multiplier)`.
///
/// # Errors
///
/// Returns an error if the scaled value does not fit in `i64`.
pub fn scale_base_amount(quantity: Decimal, size_multiplier: u64) -> anyhow::Result<i64> {
    let scaled = (quantity * Decimal::from(size_multiplier)).trunc();
    scaled.to_i64().ok_or_else(|| {
        anyhow::anyhow!(
            "Scaled base amount {scaled} (qty={quantity}, mult={size_multiplier}) overflows i64"
        )
    })
}

/// Scales a decimal price to Lighter's integer `Price`.
///
/// Truncates toward zero, matching the reference `int(price * multiplier)`.
///
/// # Errors
///
/// Returns an error if the scaled value is negative or does not fit in `u32`.
pub fn scale_price(price: Decimal, price_multiplier: u64) -> anyhow::Result<u32> {
    let scaled = (price * Decimal::from(price_multiplier)).trunc();
    let as_i64 = scaled.to_i64().ok_or_else(|| {
        anyhow::anyhow!(
            "Scaled price {scaled} (price={price}, mult={price_multiplier}) overflows i64"
        )
    })?;
    u32::try_from(as_i64).map_err(|_| {
        anyhow::anyhow!(
            "Scaled price {as_i64} (price={price}, mult={price_multiplier}) out of u32 range"
        )
    })
}

/// Tracks the next Lighter L2 transaction nonce for one API key.
///
/// Lighter requires a strictly increasing nonce per API key. The manager is
/// seeded from the server (`GET /api/v1/nextNonce`) and then advanced locally
/// on every signed transaction, avoiding a network round-trip on the hot path.
#[derive(Debug)]
pub struct NonceManager {
    next: AtomicI64,
    seeded: AtomicI64,
}

impl NonceManager {
    /// Creates an unseeded nonce manager. [`Self::reset`] must be called with a
    /// server value before [`Self::take`] returns a usable nonce.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            next: AtomicI64::new(0),
            seeded: AtomicI64::new(0),
        }
    }

    /// Seeds the manager with the authoritative next nonce from the server.
    ///
    /// Only moves the local counter forward; a stale (lower) server value never
    /// rewinds an already-advanced local nonce.
    pub fn reset(&self, server_next: i64) {
        self.seeded.store(1, Ordering::Release);
        self.next.fetch_max(server_next, Ordering::AcqRel);
    }

    /// Replaces the local value with the authoritative server nonce.
    ///
    /// Callers must first serialize all transactions for this API key. This is
    /// used after a failed request, when the rejected transaction may not have
    /// consumed its locally allocated nonce.
    pub fn resynchronize(&self, server_next: i64) {
        self.seeded.store(1, Ordering::Release);
        self.next.store(server_next, Ordering::Release);
    }

    /// Returns whether the manager has been seeded from the server at least once.
    #[must_use]
    pub fn is_seeded(&self) -> bool {
        self.seeded.load(Ordering::Acquire) == 1
    }

    /// Atomically returns the next nonce and advances the counter by one.
    #[must_use]
    pub fn take(&self) -> i64 {
        self.next.fetch_add(1, Ordering::AcqRel)
    }

    /// Returns the value [`Self::take`] would return next, without advancing.
    #[must_use]
    pub fn peek(&self) -> i64 {
        self.next.load(Ordering::Acquire)
    }
}

impl Default for NonceManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scale_base_amount_truncates_toward_zero() {
        // 0.0001 BTC with size_decimals=4 -> multiplier 10_000 -> 1.
        assert_eq!(scale_base_amount(Decimal::new(1, 4), 10_000).unwrap(), 1);
        // 1.23456 with mult 10_000 -> 12345.6 -> trunc 12345 (not rounded up).
        assert_eq!(
            scale_base_amount(Decimal::new(123_456, 5), 10_000).unwrap(),
            12345
        );
        assert_eq!(scale_base_amount(Decimal::ZERO, 10_000).unwrap(), 0);
    }

    #[test]
    fn scale_price_truncates_toward_zero() {
        // price 92000.5 with price_decimals=1 -> multiplier 10 -> 920005.
        assert_eq!(scale_price(Decimal::new(920_005, 1), 10).unwrap(), 920_005);
        // 50000.19 with mult 10 -> 500001.9 -> trunc 500001.
        assert_eq!(
            scale_price(Decimal::new(5_000_019, 2), 10).unwrap(),
            500_001
        );
    }

    #[test]
    fn scale_price_rejects_overflow() {
        // 5_000_000_000 * 1 exceeds u32::MAX.
        assert!(scale_price(Decimal::from(5_000_000_000_i64), 1).is_err());
    }

    #[test]
    fn nonce_manager_seeds_advances_and_never_rewinds() {
        let nm = NonceManager::new();
        assert!(!nm.is_seeded());

        nm.reset(100);
        assert!(nm.is_seeded());
        assert_eq!(nm.peek(), 100);
        assert_eq!(nm.take(), 100);
        assert_eq!(nm.take(), 101);
        assert_eq!(nm.peek(), 102);

        // A stale server value must not rewind the locally advanced counter.
        nm.reset(50);
        assert_eq!(nm.peek(), 102);

        // A fresh higher server value jumps the counter forward.
        nm.reset(500);
        assert_eq!(nm.take(), 500);

        // Failure recovery uses the authoritative exchange value, which may
        // legitimately be lower than a nonce reserved locally.
        nm.resynchronize(99);
        assert_eq!(nm.peek(), 99);
    }
}
