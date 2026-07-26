use crate::book::LocalBook;
use crate::model::Level;
use serde_json::Value;
use std::collections::BTreeSet;

pub const DEFAULT_BYBIT_BOOK_DEPTH: usize = 25;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BybitBookCheck {
    pub topic_depth: Option<usize>,
    pub push_interval_ms: Option<u64>,
    pub bid_depth: usize,
    pub ask_depth: usize,
    pub best_bid_price: f64,
    pub best_bid_size: f64,
    pub best_ask_price: f64,
    pub best_ask_size: f64,
    pub spread: f64,
}

#[derive(Debug, Clone)]
pub struct BybitBook {
    book: LocalBook,
    symbol: Option<String>,
    topic_depth: Option<usize>,
    update_id: Option<u64>,
    seq: Option<u64>,
    cts: Option<u64>,
}

impl BybitBook {
    pub fn new() -> Self {
        Self {
            book: LocalBook::new(),
            symbol: None,
            topic_depth: None,
            update_id: None,
            seq: None,
            cts: None,
        }
    }

    pub fn apply_json(&mut self, message: &str) -> Result<bool, String> {
        let root: Value = serde_json::from_str(message).map_err(|err| err.to_string())?;
        let Some(topic) = root.get("topic").and_then(Value::as_str) else {
            return Ok(false);
        };
        if !topic.starts_with("orderbook.") {
            return Ok(false);
        }
        let topic_info = parse_topic(topic)?;
        let message_type = root
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| "missing bybit orderbook type".to_string())?;
        let data = root
            .get("data")
            .ok_or_else(|| "missing bybit orderbook data".to_string())?;
        let update_id = u64_field(data, "u")?;
        let seq = u64_field(data, "seq")?;
        let bids = levels_field(data, "b")?;
        let asks = levels_field(data, "a")?;
        validate_symbol(data, &topic_info.symbol)?;
        validate_depth("bid", &bids, topic_info.depth)?;
        validate_depth("ask", &asks, topic_info.depth)?;
        validate_levels("bid", &bids)?;
        validate_levels("ask", &asks)?;

        if message_type == "snapshot" || update_id == 1 {
            let mut next_book = self.book.clone();
            next_book.apply_snapshot(&bids, &asks, Some(update_id));
            check_book(&next_book, Some(topic_info.depth))?;
            self.book = next_book;
            self.update_id = Some(update_id);
            self.seq = Some(seq);
            self.cts = message_cts(&root, data)?;
            self.symbol = data
                .get("s")
                .and_then(Value::as_str)
                .map(|value| value.to_string());
            self.topic_depth = Some(topic_info.depth);
            return Ok(true);
        }

        if message_type != "delta" {
            return Ok(false);
        }
        let Some(current_update_id) = self.update_id else {
            return Err("bybit delta before snapshot".to_string());
        };
        validate_stream_continuity(
            self.symbol.as_deref(),
            self.topic_depth,
            &topic_info.symbol,
            topic_info.depth,
        )?;
        if update_id <= current_update_id {
            return Ok(false);
        }
        if let Some(current_seq) = self.seq {
            if seq <= current_seq {
                return Ok(false);
            }
        }

        let mut next_book = self.book.clone();
        next_book.apply_update(&bids, &asks, None, Some(update_id));
        check_book(&next_book, Some(topic_info.depth))?;
        self.book = next_book;
        self.update_id = Some(update_id);
        self.seq = Some(seq);
        self.cts = message_cts(&root, data)?;
        Ok(true)
    }

    pub fn book(&self) -> &LocalBook {
        &self.book
    }

    pub fn symbol(&self) -> Option<&str> {
        self.symbol.as_deref()
    }

    pub fn topic_depth(&self) -> Option<usize> {
        self.topic_depth
    }

    pub fn push_interval_ms(&self) -> Option<u64> {
        self.topic_depth.and_then(bybit_depth_push_interval_ms)
    }

    pub fn update_id(&self) -> Option<u64> {
        self.update_id
    }

    pub fn seq(&self) -> Option<u64> {
        self.seq
    }

    pub fn cts(&self) -> Option<u64> {
        self.cts
    }

    pub fn check(&self) -> Result<BybitBookCheck, String> {
        check_book(&self.book, self.topic_depth)
    }
}

impl Default for BybitBook {
    fn default() -> Self {
        Self::new()
    }
}

pub fn default_bybit_book_depth() -> usize {
    DEFAULT_BYBIT_BOOK_DEPTH
}

pub fn bybit_orderbook_topic(symbol: &str, depth: Option<usize>) -> Result<String, String> {
    let depth = depth.unwrap_or(DEFAULT_BYBIT_BOOK_DEPTH);
    if bybit_depth_push_interval_ms(depth).is_none() {
        return Err(format!("unsupported bybit orderbook topic depth: {depth}"));
    }
    let symbol = symbol.trim().to_uppercase();
    if symbol.is_empty() {
        return Err("missing bybit orderbook topic symbol".to_string());
    }
    Ok(format!("orderbook.{depth}.{symbol}"))
}

fn levels_field(data: &Value, key: &str) -> Result<Vec<Level>, String> {
    let Some(rows) = data.get(key).and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    rows.iter()
        .map(|row| {
            let values = row
                .as_array()
                .ok_or_else(|| format!("invalid bybit level {key}: expected array"))?;
            if values.len() < 2 {
                return Err(format!("invalid bybit level {key}: missing price/size"));
            }
            Ok(Level {
                price: parse_decimal_scaled(value_as_str(&values[0])?)?,
                size: parse_decimal_scaled(value_as_str(&values[1])?)?,
            })
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TopicInfo {
    depth: usize,
    symbol: String,
}

fn parse_topic(topic: &str) -> Result<TopicInfo, String> {
    let mut parts = topic.split('.');
    let Some(prefix) = parts.next() else {
        return Err("invalid bybit orderbook topic".to_string());
    };
    let Some(depth_text) = parts.next() else {
        return Err("missing bybit orderbook topic depth".to_string());
    };
    let Some(symbol) = parts.next() else {
        return Err("missing bybit orderbook topic symbol".to_string());
    };
    if prefix != "orderbook" || parts.next().is_some() {
        return Err("invalid bybit orderbook topic".to_string());
    }
    let depth = depth_text
        .parse::<usize>()
        .map_err(|err| format!("invalid bybit orderbook topic depth: {err}"))?;
    if bybit_depth_push_interval_ms(depth).is_none() {
        return Err(format!("unsupported bybit orderbook topic depth: {depth}"));
    }
    Ok(TopicInfo {
        depth,
        symbol: symbol.to_string(),
    })
}

fn validate_symbol(data: &Value, topic_symbol: &str) -> Result<(), String> {
    let Some(data_symbol) = data.get("s").and_then(Value::as_str) else {
        return Ok(());
    };
    if data_symbol != topic_symbol {
        return Err(format!(
            "bybit orderbook symbol mismatch: topic={topic_symbol} data={data_symbol}"
        ));
    }
    Ok(())
}

fn validate_stream_continuity(
    current_symbol: Option<&str>,
    current_depth: Option<usize>,
    topic_symbol: &str,
    topic_depth: usize,
) -> Result<(), String> {
    if let Some(symbol) = current_symbol {
        if symbol != topic_symbol {
            return Err(format!(
                "bybit orderbook stream symbol changed: current={symbol} topic={topic_symbol}"
            ));
        }
    }
    if let Some(depth) = current_depth {
        if depth != topic_depth {
            return Err(format!(
                "bybit orderbook stream depth changed: current={depth} topic={topic_depth}"
            ));
        }
    }
    Ok(())
}

fn validate_depth(side: &str, levels: &[Level], topic_depth: usize) -> Result<(), String> {
    if levels.len() > topic_depth {
        return Err(format!(
            "bybit {side} depth overflow: levels={} topic_depth={topic_depth}",
            levels.len()
        ));
    }
    Ok(())
}

fn validate_levels(side: &str, levels: &[Level]) -> Result<(), String> {
    let mut prices = BTreeSet::new();
    for level in levels {
        if level.price <= 0 {
            return Err(format!("invalid bybit {side} price"));
        }
        if level.size < 0 {
            return Err(format!("invalid bybit {side} size"));
        }
        if !prices.insert(level.price) {
            return Err(format!("duplicate bybit {side} price"));
        }
    }
    Ok(())
}

fn check_book(book: &LocalBook, topic_depth: Option<usize>) -> Result<BybitBookCheck, String> {
    let (best_bid_price, best_bid_size) = book
        .best_bid()
        .ok_or_else(|| "bybit local book missing bids".to_string())?;
    let (best_ask_price, best_ask_size) = book
        .best_ask()
        .ok_or_else(|| "bybit local book missing asks".to_string())?;
    if best_bid_price <= 0.0 || best_bid_size <= 0.0 {
        return Err("bybit local book invalid best bid".to_string());
    }
    if best_ask_price <= 0.0 || best_ask_size <= 0.0 {
        return Err("bybit local book invalid best ask".to_string());
    }
    if best_bid_price >= best_ask_price {
        return Err(format!(
            "bybit local book crossed: bid={best_bid_price} ask={best_ask_price}"
        ));
    }
    Ok(BybitBookCheck {
        topic_depth,
        push_interval_ms: topic_depth.and_then(bybit_depth_push_interval_ms),
        bid_depth: book.bid_depth(),
        ask_depth: book.ask_depth(),
        best_bid_price,
        best_bid_size,
        best_ask_price,
        best_ask_size,
        spread: best_ask_price - best_bid_price,
    })
}

pub fn bybit_depth_push_interval_ms(depth: usize) -> Option<u64> {
    match depth {
        1 => Some(10),
        25 | 50 => Some(20),
        100 | 200 => Some(100),
        1000 => Some(200),
        _ => None,
    }
}

fn message_cts(root: &Value, data: &Value) -> Result<Option<u64>, String> {
    match optional_u64_field(data, "cts")? {
        Some(value) => Ok(Some(value)),
        None => optional_u64_field(root, "cts"),
    }
}

fn value_as_str(value: &Value) -> Result<&str, String> {
    value
        .as_str()
        .ok_or_else(|| "expected string decimal".to_string())
}

fn u64_field(data: &Value, key: &str) -> Result<u64, String> {
    optional_u64_field(data, key)?.ok_or_else(|| format!("missing bybit orderbook {key}"))
}

fn optional_u64_field(data: &Value, key: &str) -> Result<Option<u64>, String> {
    let Some(value) = data.get(key) else {
        return Ok(None);
    };
    if let Some(number) = value.as_u64() {
        return Ok(Some(number));
    }
    if let Some(text) = value.as_str() {
        return text.parse::<u64>().map(Some).map_err(|err| err.to_string());
    }
    Err(format!("invalid u64 field {key}"))
}

fn parse_decimal_scaled(value: &str) -> Result<i64, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("empty decimal".to_string());
    }
    let negative = value.starts_with('-');
    let unsigned = value.trim_start_matches(['-', '+']);
    let mut parts = unsigned.split('.');
    let whole = parts.next().unwrap_or("0");
    let frac = parts.next().unwrap_or("");
    if parts.next().is_some() {
        return Err(format!("invalid decimal: {value}"));
    }
    let mut digits = String::new();
    digits.push_str(if whole.is_empty() { "0" } else { whole });
    let mut frac_padded = frac.to_string();
    if frac_padded.len() > 8 {
        frac_padded.truncate(8);
    }
    while frac_padded.len() < 8 {
        frac_padded.push('0');
    }
    digits.push_str(&frac_padded);
    let parsed = digits.parse::<i64>().map_err(|err| err.to_string())?;
    Ok(if negative { -parsed } else { parsed })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::BookStatus;

    #[test]
    fn snapshot_resets_local_book_and_metadata() {
        let mut book = BybitBook::new();

        let changed = book
            .apply_json(
                r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1672304484978,"data":{"s":"BTCUSDT","b":[["16493.50","0.006"],["16493.00","0.100"]],"a":[["16611.00","0.029"],["16612.00","0.213"]],"u":18521288,"seq":7961638724,"cts":1672304484976}}"#,
            )
            .unwrap();

        assert!(changed);
        assert_eq!(book.symbol(), Some("BTCUSDT"));
        assert_eq!(book.topic_depth(), Some(50));
        assert_eq!(book.push_interval_ms(), Some(20));
        assert_eq!(book.update_id(), Some(18_521_288));
        assert_eq!(book.seq(), Some(7_961_638_724));
        assert_eq!(book.cts(), Some(1_672_304_484_976));
        assert_eq!(book.book().status(), BookStatus::Ready);
        assert_eq!(book.book().best_bid(), Some((16493.5, 0.006)));
        assert_eq!(book.book().best_ask(), Some((16611.0, 0.029)));
        assert_eq!(
            book.check().unwrap(),
            BybitBookCheck {
                topic_depth: Some(50),
                push_interval_ms: Some(20),
                bid_depth: 2,
                ask_depth: 2,
                best_bid_price: 16493.5,
                best_bid_size: 0.006,
                best_ask_price: 16611.0,
                best_ask_size: 0.029,
                spread: 117.5,
            }
        );
    }

    #[test]
    fn delta_inserts_updates_and_deletes_levels() {
        let mut book = seeded_book();

        book.apply_json(
            r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":1687940967466,"data":{"s":"BTCUSDT","b":[["16494.00","0.500"],["16493.50","0"]],"a":[["16611.00","0"],["16610.50","0.250"]],"u":18521289,"seq":7961638725},"cts":1687940967464}"#,
        )
        .unwrap();

        assert_eq!(book.update_id(), Some(18_521_289));
        assert_eq!(book.seq(), Some(7_961_638_725));
        assert_eq!(book.book().best_bid(), Some((16494.0, 0.5)));
        assert_eq!(book.book().best_ask(), Some((16610.5, 0.25)));
    }

    #[test]
    fn crossed_snapshot_is_rejected() {
        let mut book = BybitBook::new();

        let err = book
            .apply_json(
                r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","data":{"s":"BTCUSDT","b":[["101","1"]],"a":[["100","1"]],"u":1,"seq":1}}"#,
            )
            .unwrap_err();

        assert_eq!(err, "bybit local book crossed: bid=101 ask=100");
        assert_eq!(book.update_id(), None);
    }

    #[test]
    fn empty_side_snapshot_is_rejected() {
        let mut book = BybitBook::new();

        let err = book
            .apply_json(
                r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","data":{"s":"BTCUSDT","b":[["100","1"]],"a":[],"u":1,"seq":1}}"#,
            )
            .unwrap_err();

        assert_eq!(err, "bybit local book missing asks");
        assert_eq!(book.update_id(), None);
    }

    #[test]
    fn invalid_negative_size_is_rejected() {
        let mut book = BybitBook::new();

        let err = book
            .apply_json(
                r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","data":{"s":"BTCUSDT","b":[["100","-1"]],"a":[["101","1"]],"u":1,"seq":1}}"#,
            )
            .unwrap_err();

        assert_eq!(err, "invalid bybit bid size");
    }

    #[test]
    fn duplicate_price_is_rejected() {
        let mut book = BybitBook::new();

        let err = book
            .apply_json(
                r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","data":{"s":"BTCUSDT","b":[["100","1"],["100","2"]],"a":[["101","1"]],"u":1,"seq":1}}"#,
            )
            .unwrap_err();

        assert_eq!(err, "duplicate bybit bid price");
    }

    #[test]
    fn topic_symbol_mismatch_is_rejected() {
        let mut book = BybitBook::new();

        let err = book
            .apply_json(
                r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","data":{"s":"ETHUSDT","b":[["100","1"]],"a":[["101","1"]],"u":1,"seq":1}}"#,
            )
            .unwrap_err();

        assert_eq!(
            err,
            "bybit orderbook symbol mismatch: topic=BTCUSDT data=ETHUSDT"
        );
    }

    #[test]
    fn topic_depth_overflow_is_rejected() {
        let mut book = BybitBook::new();

        let err = book
            .apply_json(
                r#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","data":{"s":"BTCUSDT","b":[["100","1"],["99","1"]],"a":[["101","1"]],"u":1,"seq":1}}"#,
            )
            .unwrap_err();

        assert_eq!(err, "bybit bid depth overflow: levels=2 topic_depth=1");
    }

    #[test]
    fn unsupported_topic_depth_is_rejected() {
        let mut book = BybitBook::new();

        let err = book
            .apply_json(
                r#"{"topic":"orderbook.500.BTCUSDT","type":"snapshot","data":{"s":"BTCUSDT","b":[["100","1"]],"a":[["101","1"]],"u":1,"seq":1}}"#,
            )
            .unwrap_err();

        assert_eq!(err, "unsupported bybit orderbook topic depth: 500");
    }

    #[test]
    fn official_depth_push_intervals_are_known() {
        assert_eq!(bybit_depth_push_interval_ms(1), Some(10));
        assert_eq!(bybit_depth_push_interval_ms(25), Some(20));
        assert_eq!(bybit_depth_push_interval_ms(50), Some(20));
        assert_eq!(bybit_depth_push_interval_ms(100), Some(100));
        assert_eq!(bybit_depth_push_interval_ms(200), Some(100));
        assert_eq!(bybit_depth_push_interval_ms(1000), Some(200));
        assert_eq!(bybit_depth_push_interval_ms(500), None);
    }

    #[test]
    fn default_topic_uses_25_depth() {
        assert_eq!(default_bybit_book_depth(), 25);
        assert_eq!(
            bybit_orderbook_topic("btcusdt", None).unwrap(),
            "orderbook.25.BTCUSDT"
        );
        assert_eq!(
            bybit_orderbook_topic("BTCUSDT", Some(50)).unwrap(),
            "orderbook.50.BTCUSDT"
        );
        assert_eq!(
            bybit_orderbook_topic("BTCUSDT", Some(500)).unwrap_err(),
            "unsupported bybit orderbook topic depth: 500"
        );
    }

    #[test]
    fn stream_depth_change_is_rejected() {
        let mut book = seeded_book();

        let err = book
            .apply_json(
                r#"{"topic":"orderbook.1.BTCUSDT","type":"delta","data":{"s":"BTCUSDT","b":[["16494","1"]],"a":[],"u":18521289,"seq":7961638725}}"#,
            )
            .unwrap_err();

        assert_eq!(
            err,
            "bybit orderbook stream depth changed: current=50 topic=1"
        );
    }

    #[test]
    fn invalid_delta_does_not_pollute_existing_book() {
        let mut book = seeded_book();

        let err = book
            .apply_json(
                r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","data":{"s":"BTCUSDT","b":[["20000","1"]],"a":[],"u":18521289,"seq":7961638725}}"#,
            )
            .unwrap_err();

        assert_eq!(err, "bybit local book crossed: bid=20000 ask=16611");
        assert_eq!(book.update_id(), Some(18_521_288));
        assert_eq!(book.seq(), Some(7_961_638_724));
        assert_eq!(book.book().best_bid(), Some((16493.5, 0.006)));
        assert_eq!(book.book().best_ask(), Some((16611.0, 0.029)));
    }

    #[test]
    fn stale_delta_before_snapshot_is_rejected() {
        let mut book = BybitBook::new();

        let err = book
            .apply_json(
                r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","data":{"s":"BTCUSDT","b":[["1","1"]],"a":[],"u":2,"seq":2}}"#,
            )
            .unwrap_err();

        assert_eq!(err, "bybit delta before snapshot");
    }

    #[test]
    fn older_delta_is_ignored() {
        let mut book = seeded_book();

        let changed = book
            .apply_json(
                r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","data":{"s":"BTCUSDT","b":[["20000","1"]],"a":[],"u":18521288,"seq":7961638724}}"#,
            )
            .unwrap();

        assert!(!changed);
        assert_eq!(book.book().best_bid(), Some((16493.5, 0.006)));
    }

    #[test]
    fn update_id_one_replaces_book_as_restart_snapshot() {
        let mut book = seeded_book();

        let changed = book
            .apply_json(
                r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","data":{"s":"BTCUSDT","b":[["100","2"]],"a":[["101","3"]],"u":1,"seq":10}}"#,
            )
            .unwrap();

        assert!(changed);
        assert_eq!(book.update_id(), Some(1));
        assert_eq!(book.seq(), Some(10));
        assert_eq!(book.book().best_bid(), Some((100.0, 2.0)));
        assert_eq!(book.book().best_ask(), Some((101.0, 3.0)));
    }

    fn seeded_book() -> BybitBook {
        let mut book = BybitBook::new();
        book.apply_json(
            r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","data":{"s":"BTCUSDT","b":[["16493.50","0.006"]],"a":[["16611.00","0.029"]],"u":18521288,"seq":7961638724}}"#,
        )
        .unwrap();
        book
    }
}
