//! 权重感知令牌桶（P5）。
//!
//! per-key 一个桶；adapter 按端点成本表传入 weight。
//! 非阻塞判定：不足即返回 `NexusError::RateLimited { retry_after }`，绝不盲目排队。

use std::sync::Mutex;
use std::time::{Duration, Instant};

use nexus_core::{NexusError, Result};

struct BucketState {
    tokens: f64,
    last_refill: Instant,
}

/// 权重感知令牌桶。线程安全，可 `Arc` 共享。
pub struct WeightedTokenBucket {
    capacity: f64,
    refill_per_sec: f64,
    state: Mutex<BucketState>,
}

impl WeightedTokenBucket {
    /// `capacity`：桶容量（初始满桶）；`refill_per_sec`：每秒补充令牌数。
    ///
    /// # Panics
    /// `capacity == 0` 或 `refill_per_sec <= 0` 属配置错误，直接 panic（fail-fast）。
    pub fn new(capacity: u32, refill_per_sec: f64) -> Self {
        assert!(capacity > 0, "token bucket capacity must be positive");
        assert!(
            refill_per_sec > 0.0 && refill_per_sec.is_finite(),
            "token bucket refill rate must be positive"
        );
        Self {
            capacity: f64::from(capacity),
            refill_per_sec,
            state: Mutex::new(BucketState {
                tokens: f64::from(capacity),
                last_refill: Instant::now(),
            }),
        }
    }

    /// 非阻塞获取 `weight` 个令牌。
    ///
    /// - 足额：扣减并返回 `Ok(())`。
    /// - 不足：返回 `RateLimited { retry_after: Some(补足所需时长) }`，不扣减。
    /// - `weight > capacity`：永不可满足，返回 `RateLimited { retry_after: None }`。
    pub fn acquire(&self, weight: u32) -> Result<()> {
        let weight = f64::from(weight);
        if weight > self.capacity {
            return Err(NexusError::RateLimited { retry_after: None });
        }
        let mut state = self.state.lock().expect("token bucket lock poisoned");
        self.refill(&mut state);
        if state.tokens >= weight {
            state.tokens -= weight;
            Ok(())
        } else {
            let deficit = weight - state.tokens;
            Err(NexusError::RateLimited {
                retry_after: Some(Duration::from_secs_f64(deficit / self.refill_per_sec)),
            })
        }
    }

    /// `acquire` 的布尔便捷形式。
    pub fn try_acquire(&self, weight: u32) -> bool {
        self.acquire(weight).is_ok()
    }

    /// 当前可用令牌数（先补充后读取，诊断用）。
    pub fn available(&self) -> f64 {
        let mut state = self.state.lock().expect("token bucket lock poisoned");
        self.refill(&mut state);
        state.tokens
    }

    fn refill(&self, state: &mut BucketState) {
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_refill);
        state.tokens =
            (state.tokens + elapsed.as_secs_f64() * self.refill_per_sec).min(self.capacity);
        state.last_refill = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn acquire_within_capacity_succeeds_then_limits() {
        let bucket = WeightedTokenBucket::new(10, 100.0);
        assert!(bucket.acquire(6).is_ok());
        assert!(bucket.acquire(4).is_ok());

        match bucket.acquire(5) {
            Err(NexusError::RateLimited {
                retry_after: Some(wait),
            }) => {
                // 缺 ~5 个令牌，100/s → ~50ms（放宽容差）。
                assert!(wait > Duration::ZERO && wait <= Duration::from_millis(80));
            }
            other => panic!("expected RateLimited with retry_after, got {other:?}"),
        }
        assert!(!bucket.try_acquire(5));
    }

    #[test]
    fn refills_over_time() {
        let bucket = WeightedTokenBucket::new(10, 100.0);
        assert!(bucket.acquire(10).is_ok());
        assert!(bucket.acquire(1).is_err());

        std::thread::sleep(Duration::from_millis(60));
        // 60ms @ 100/s ≈ 6 个令牌。
        assert!(bucket.acquire(3).is_ok());
    }

    #[test]
    fn refill_caps_at_capacity() {
        let bucket = WeightedTokenBucket::new(5, 1000.0);
        std::thread::sleep(Duration::from_millis(20));
        assert!(bucket.available() <= 5.0);
        assert!(bucket.acquire(5).is_ok());
        assert!(bucket.acquire(6).is_err());
    }

    #[test]
    fn weight_above_capacity_is_never_satisfiable() {
        let bucket = WeightedTokenBucket::new(10, 100.0);
        assert!(matches!(
            bucket.acquire(11),
            Err(NexusError::RateLimited { retry_after: None })
        ));
        // 且不扣减既有令牌。
        assert!(bucket.acquire(10).is_ok());
    }
}
