//! 延迟仪表（P6）：HDR 直方图，按 label 分流（行情流与私有流分开统计）。
//!
//! 口径：微秒；p95/p99/p99.9 为验收指标，p50 仅供参考。

use std::collections::HashMap;
use std::sync::Mutex;

use hdrhistogram::Histogram;

/// 直方图记录范围：1µs ~ 60s，3 位有效数字。
const LOW_MICROS: u64 = 1;
const HIGH_MICROS: u64 = 60_000_000;
const SIGFIG: u8 = 3;

/// 分位数快照（微秒）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Percentiles {
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub p999: u64,
}

/// 按 label 分流的延迟记录器。线程安全，可 `Arc` 共享。
#[derive(Default)]
pub struct LatencyRecorder {
    inner: Mutex<HashMap<String, Histogram<u64>>>,
}

impl LatencyRecorder {
    pub fn new() -> Self {
        Self::default()
    }

    /// 记录一次延迟采样（微秒）。超出范围的值饱和记录，不丢弃。
    pub fn record(&self, label: &str, micros: u64) {
        let mut map = self.inner.lock().expect("latency recorder lock poisoned");
        let hist = map.entry(label.to_string()).or_insert_with(|| {
            Histogram::new_with_bounds(LOW_MICROS, HIGH_MICROS, SIGFIG)
                .expect("static histogram bounds are valid")
        });
        hist.saturating_record(micros.max(LOW_MICROS));
    }

    /// 读取指定流的分位数。label 无记录返回 None。
    pub fn percentiles(&self, label: &str) -> Option<Percentiles> {
        let map = self.inner.lock().expect("latency recorder lock poisoned");
        let hist = map.get(label)?;
        if hist.is_empty() {
            return None;
        }
        Some(Percentiles {
            p50: hist.value_at_quantile(0.50),
            p95: hist.value_at_quantile(0.95),
            p99: hist.value_at_quantile(0.99),
            p999: hist.value_at_quantile(0.999),
        })
    }

    /// 已有记录的全部 label（诊断/巡检用）。
    pub fn labels(&self) -> Vec<String> {
        self.inner
            .lock()
            .expect("latency recorder lock poisoned")
            .keys()
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_match_known_distribution() {
        let rec = LatencyRecorder::new();
        for v in 1..=1000u64 {
            rec.record("ws.book", v);
        }

        let p = rec.percentiles("ws.book").expect("label recorded");
        // 3 位有效数字精度下允许 ±1% 桶误差。
        assert!((495..=505).contains(&p.p50), "p50={}", p.p50);
        assert!((940..=960).contains(&p.p95), "p95={}", p.p95);
        assert!((980..=1000).contains(&p.p99), "p99={}", p.p99);
        assert!((994..=1001).contains(&p.p999), "p999={}", p.p999);
    }

    #[test]
    fn labels_are_isolated() {
        let rec = LatencyRecorder::new();
        rec.record("fast", 10);
        rec.record("slow", 10_000);

        assert!(rec.percentiles("fast").unwrap().p99 < 20);
        assert!(rec.percentiles("slow").unwrap().p50 > 9_000);
        assert_eq!(rec.percentiles("missing"), None);

        let mut labels = rec.labels();
        labels.sort();
        assert_eq!(labels, vec!["fast".to_string(), "slow".to_string()]);
    }

    #[test]
    fn out_of_range_samples_saturate() {
        let rec = LatencyRecorder::new();
        rec.record("edge", 0); // 钳到下界
        rec.record("edge", u64::MAX); // 饱和到上界
        let p = rec.percentiles("edge").expect("recorded");
        assert!(p.p999 <= 60_000_000 * 2); // 饱和不 panic，且不超上界等效桶
    }
}
