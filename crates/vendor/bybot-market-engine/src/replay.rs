use std::fs;
use std::io::{BufRead, Write};
use std::path::Path;

use crate::book::LocalBook;
use crate::model::{DecimalLevel, EngineConfig, FillMetadata, Level, SignalRow, SCALE};
use crate::signal::SignalEngine;

pub fn replay_jsonl(path: &Path) -> Result<Vec<SignalRow>, String> {
    let content = fs::read_to_string(path).map_err(|err| err.to_string())?;
    replay_jsonl_str(&content)
}

pub fn signal_to_json(row: &SignalRow) -> String {
    let depth_json = signal_depth_json(row);
    let layered_json = signal_layered_json(row);
    format!(
        "{{\"sequence\":{},\"timestamp_ns\":{},\"source\":\"{}\",\"ticker\":\"{}\",\"gate_contract\":\"{}\",\"lighter_market_id\":{},\"ready\":{},\"sample_count\":{},\"lighter_bid\":\"{}\",\"lighter_bid_size\":\"{}\",\"lighter_ask\":\"{}\",\"lighter_ask_size\":\"{}\",\"gate_bid\":\"{}\",\"gate_bid_size\":\"{}\",\"gate_ask\":\"{}\",\"gate_ask_size\":\"{}\",\"long_spread\":\"{}\",\"short_spread\":\"{}\",\"long_median\":\"{}\",\"short_median\":\"{}\",\"long_threshold\":\"{}\",\"short_threshold\":\"{}\",\"basis\":\"{}\",\"long_ok\":{},\"short_ok\":{},\"gate_book_status\":\"{}\",\"lighter_book_status\":\"{}\"{}{},\"schema_version\":2}}",
        row.sequence,
        row.timestamp_ns,
        escape_json_string(&row.source),
        escape_json_string(&row.ticker),
        escape_json_string(&row.gate_contract),
        row.lighter_market_id,
        row.ready,
        row.sample_count,
        fmt(row.lighter_bid),
        fmt(row.lighter_bid_size),
        fmt(row.lighter_ask),
        fmt(row.lighter_ask_size),
        fmt(row.gate_bid),
        fmt(row.gate_bid_size),
        fmt(row.gate_ask),
        fmt(row.gate_ask_size),
        fmt(row.long_spread),
        fmt(row.short_spread),
        fmt(row.long_median),
        fmt(row.short_median),
        fmt(row.long_threshold),
        fmt(row.short_threshold),
        fmt(row.basis),
        row.long_ok,
        row.short_ok,
        row.gate_book_status.as_str(),
        row.lighter_book_status.as_str(),
        depth_json,
        layered_json,
    )
}

fn signal_layered_json(row: &SignalRow) -> String {
    format!(
        ",\"hot\":{{\"sequence\":{},\"timestamp_ns\":{},\"ready\":{},\"sample_count\":{},\"long_ok\":{},\"short_ok\":{},\"long_spread\":\"{}\",\"short_spread\":\"{}\",\"long_median\":\"{}\",\"short_median\":\"{}\",\"long_threshold\":\"{}\",\"short_threshold\":\"{}\",\"gate_bid\":\"{}\",\"gate_ask\":\"{}\",\"lighter_bid\":\"{}\",\"lighter_ask\":\"{}\"}},\"diagnostics\":{{\"source\":\"{}\",\"ticker\":\"{}\",\"gate_contract\":\"{}\",\"lighter_market_id\":{},\"basis\":\"{}\",\"gate_book_status\":\"{}\",\"lighter_book_status\":\"{}\"{}}}",
        row.sequence,
        row.timestamp_ns,
        row.ready,
        row.sample_count,
        row.long_ok,
        row.short_ok,
        fmt(row.long_spread),
        fmt(row.short_spread),
        fmt(row.long_median),
        fmt(row.short_median),
        fmt(row.long_threshold),
        fmt(row.short_threshold),
        fmt(row.gate_bid),
        fmt(row.gate_ask),
        fmt(row.lighter_bid),
        fmt(row.lighter_ask),
        escape_json_string(&row.source),
        escape_json_string(&row.ticker),
        escape_json_string(&row.gate_contract),
        row.lighter_market_id,
        fmt(row.basis),
        row.gate_book_status.as_str(),
        row.lighter_book_status.as_str(),
        signal_depth_diagnostics_json(row),
    )
}

fn signal_depth_json(row: &SignalRow) -> String {
    let Some(depth) = &row.depth else {
        return String::new();
    };
    format!(
        ",\"gate_bid_levels\":{},\"gate_ask_levels\":{},\"lighter_bid_levels\":{},\"lighter_ask_levels\":{},\"gate_bid_fill\":{},\"gate_ask_fill\":{},\"lighter_bid_fill\":{},\"lighter_ask_fill\":{}",
        levels_json(&depth.gate_bid_levels),
        levels_json(&depth.gate_ask_levels),
        levels_json(&depth.lighter_bid_levels),
        levels_json(&depth.lighter_ask_levels),
        fill_json(depth.gate_bid_fill),
        fill_json(depth.gate_ask_fill),
        fill_json(depth.lighter_bid_fill),
        fill_json(depth.lighter_ask_fill),
    )
}

fn signal_depth_diagnostics_json(row: &SignalRow) -> String {
    let Some(depth) = &row.depth else {
        return String::new();
    };
    format!(
        ",\"depth\":{{\"gate_bid_levels\":{},\"gate_ask_levels\":{},\"lighter_bid_levels\":{},\"lighter_ask_levels\":{},\"gate_bid_fill\":{},\"gate_ask_fill\":{},\"lighter_bid_fill\":{},\"lighter_ask_fill\":{}}}",
        levels_json(&depth.gate_bid_levels),
        levels_json(&depth.gate_ask_levels),
        levels_json(&depth.lighter_bid_levels),
        levels_json(&depth.lighter_ask_levels),
        fill_json(depth.gate_bid_fill),
        fill_json(depth.gate_ask_fill),
        fill_json(depth.lighter_bid_fill),
        fill_json(depth.lighter_ask_fill),
    )
}

fn levels_json(levels: &[DecimalLevel]) -> String {
    let rows = levels
        .iter()
        .map(|level| format!("[\"{}\",\"{}\"]", fmt(level.price), fmt(level.size)))
        .collect::<Vec<_>>()
        .join(",");
    format!("[{rows}]")
}

fn fill_json(fill: Option<FillMetadata>) -> String {
    let Some(fill) = fill else {
        return "null".to_string();
    };
    format!(
        "{{\"vwap_avg_price\":\"{}\",\"filled_quantity\":\"{}\",\"levels_used\":{},\"remaining_quote\":\"{}\",\"is_complete\":{}}}",
        fmt(fill.avg_price),
        fmt(fill.filled_quantity),
        fill.levels_used,
        fmt(fill.remaining_quote),
        fill.is_complete,
    )
}

fn replay_jsonl_str(content: &str) -> Result<Vec<SignalRow>, String> {
    let mut gate = LocalBook::new();
    let mut lighter = LocalBook::new();
    let mut engine: Option<SignalEngine> = None;
    let mut rows = Vec::new();

    for (line_number, raw_line) in content.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        rows.extend(apply_jsonl_line(
            line,
            line_number + 1,
            &mut gate,
            &mut lighter,
            &mut engine,
        )?);
    }

    Ok(rows)
}

pub fn stream_jsonl<R: BufRead, W: Write>(reader: R, writer: &mut W) -> Result<(), String> {
    let mut gate = LocalBook::new();
    let mut lighter = LocalBook::new();
    let mut engine: Option<SignalEngine> = None;

    for (line_number, raw_line) in reader.lines().enumerate() {
        let raw_line = raw_line.map_err(|err| err.to_string())?;
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        for row in apply_jsonl_line(line, line_number + 1, &mut gate, &mut lighter, &mut engine)? {
            writeln!(writer, "{}", signal_to_json(&row)).map_err(|err| err.to_string())?;
            writer.flush().map_err(|err| err.to_string())?;
        }
    }
    Ok(())
}

fn apply_jsonl_line(
    line: &str,
    line_number: usize,
    gate: &mut LocalBook,
    lighter: &mut LocalBook,
    engine: &mut Option<SignalEngine>,
) -> Result<Vec<SignalRow>, String> {
    let event_type =
        string_field(line, "type").map_err(|err| format!("line {line_number}: {err}"))?;
    if event_type == "config" {
        let config = EngineConfig {
            window_size: usize_field(line, "window_size")?,
            min_samples: usize_field(line, "min_samples")?,
            threshold_bps: f64_string_field(line, "threshold_bps")?,
            ticker: string_field(line, "ticker")?,
            gate_contract: string_field(line, "gate_contract")?,
            lighter_market_id: u64_field(line, "lighter_market_id")?,
        };
        *engine = Some(SignalEngine::new(config));
        return Ok(Vec::new());
    }

    let engine_ref = engine
        .as_mut()
        .ok_or_else(|| "fixture must start with config".to_string())?;
    let sequence = u64_field(line, "sequence")?;
    let timestamp_ns = u64_field(line, "timestamp_ns")?;
    let emit = bool_field(line, "emit")?.unwrap_or(true);

    match event_type.as_str() {
        "gate_snapshot" => {
            gate.apply_snapshot(
                &levels_field(line, "bids")?,
                &levels_field(line, "asks")?,
                Some(u64_field(line, "book_id")?),
            );
        }
        "lighter_snapshot" => {
            lighter.apply_snapshot(
                &levels_field(line, "bids")?,
                &levels_field(line, "asks")?,
                Some(u64_field(line, "book_id")?),
            );
        }
        "gate_update" => {
            gate.apply_update(
                &levels_field(line, "bids")?,
                &levels_field(line, "asks")?,
                Some(u64_field(line, "first_id")?),
                Some(u64_field(line, "last_id")?),
            );
        }
        "lighter_update" => {
            lighter.apply_update(
                &levels_field(line, "bids")?,
                &levels_field(line, "asks")?,
                None,
                None,
            );
        }
        _ => return Err(format!("unknown event type: {event_type}")),
    }

    if !emit {
        return Ok(Vec::new());
    }
    Ok(engine_ref
        .maybe_signal(sequence, timestamp_ns, &event_type, gate, lighter)
        .into_iter()
        .collect())
}

fn fmt(value: f64) -> String {
    format!("{value:.8}")
}

fn string_field(line: &str, key: &str) -> Result<String, String> {
    let marker = format!("\"{key}\":\"");
    let start = line
        .find(&marker)
        .ok_or_else(|| format!("missing string field {key}"))?
        + marker.len();
    let rest = &line[start..];
    let end = rest
        .find('"')
        .ok_or_else(|| format!("unterminated string field {key}"))?;
    Ok(rest[..end].to_string())
}

fn f64_string_field(line: &str, key: &str) -> Result<f64, String> {
    string_field(line, key)?
        .parse::<f64>()
        .map_err(|err| err.to_string())
}

fn usize_field(line: &str, key: &str) -> Result<usize, String> {
    number_field(line, key)?
        .parse::<usize>()
        .map_err(|err| err.to_string())
}

fn u64_field(line: &str, key: &str) -> Result<u64, String> {
    number_field(line, key)?
        .parse::<u64>()
        .map_err(|err| err.to_string())
}

fn bool_field(line: &str, key: &str) -> Result<Option<bool>, String> {
    let marker = format!("\"{key}\":");
    let Some(start) = line.find(&marker).map(|idx| idx + marker.len()) else {
        return Ok(None);
    };
    let rest = &line[start..];
    let end = rest.find([',', '}']).unwrap_or(rest.len());
    match rest[..end].trim() {
        "true" => Ok(Some(true)),
        "false" => Ok(Some(false)),
        value => Err(format!("invalid bool field {key}: {value}")),
    }
}

fn number_field(line: &str, key: &str) -> Result<String, String> {
    let marker = format!("\"{key}\":");
    let start = line
        .find(&marker)
        .ok_or_else(|| format!("missing number field {key}"))?
        + marker.len();
    let rest = &line[start..];
    let end = rest.find([',', '}']).unwrap_or(rest.len());
    Ok(rest[..end].trim().to_string())
}

fn levels_field(line: &str, key: &str) -> Result<Vec<Level>, String> {
    let marker = format!("\"{key}\":[");
    let start = line
        .find(&marker)
        .ok_or_else(|| format!("missing levels field {key}"))?
        + marker.len();
    let rest = &line[start..];
    let end = matching_array_end(rest).ok_or_else(|| format!("unterminated levels field {key}"))?;
    let body = &rest[..end];
    let mut levels = Vec::new();

    for raw_level in body.split("],[") {
        let clean = raw_level.trim_matches(|ch| ch == '[' || ch == ']');
        if clean.trim().is_empty() {
            continue;
        }
        let mut pieces = clean.split(',');
        let price = pieces
            .next()
            .ok_or_else(|| "missing price".to_string())?
            .trim()
            .trim_matches('"');
        let size = pieces
            .next()
            .ok_or_else(|| "missing size".to_string())?
            .trim()
            .trim_matches('"');
        if pieces.next().is_some() {
            return Err("too many level fields".to_string());
        }
        levels.push(Level {
            price: scale_decimal_str(price)?,
            size: scale_decimal_str(size)?,
        });
    }

    Ok(levels)
}

fn matching_array_end(value: &str) -> Option<usize> {
    let mut depth = 1;
    for (index, ch) in value.char_indices() {
        match ch {
            '[' => depth += 1,
            ']' => {
                depth -= 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

fn scale_decimal_str(value: &str) -> Result<i64, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("empty decimal".to_string());
    }

    let (negative, unsigned) = match value.strip_prefix('-') {
        Some(stripped) => (true, stripped),
        None => (false, value),
    };
    let mut parts = unsigned.split('.');
    let whole = parts
        .next()
        .ok_or_else(|| format!("invalid decimal {value}"))?;
    let fraction = parts.next().unwrap_or("");
    if parts.next().is_some() || whole.is_empty() {
        return Err(format!("invalid decimal {value}"));
    }
    if !whole.chars().all(|ch| ch.is_ascii_digit())
        || !fraction.chars().all(|ch| ch.is_ascii_digit())
    {
        return Err(format!("invalid decimal {value}"));
    }

    let whole_value = whole.parse::<i64>().map_err(|err| err.to_string())?;
    let mut fraction_digits = fraction.to_string();
    if fraction_digits.len() > 8 {
        let ninth = fraction_digits.as_bytes()[8];
        fraction_digits.truncate(8);
        let mut scaled = whole_value
            .checked_mul(SCALE)
            .ok_or_else(|| format!("decimal overflow {value}"))?
            + fraction_digits
                .parse::<i64>()
                .map_err(|err| err.to_string())?;
        if ninth >= b'5' {
            scaled += 1;
        }
        return Ok(if negative { -scaled } else { scaled });
    }
    while fraction_digits.len() < 8 {
        fraction_digits.push('0');
    }
    let fraction_value = fraction_digits
        .parse::<i64>()
        .map_err(|err| err.to_string())?;
    let scaled = whole_value
        .checked_mul(SCALE)
        .ok_or_else(|| format!("decimal overflow {value}"))?
        + fraction_value;
    Ok(if negative { -scaled } else { scaled })
}

fn escape_json_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEMP_FIXTURE_COUNTER: AtomicU64 = AtomicU64::new(0);

    const FIXTURE: &str = r#"{"type":"config","window_size":3,"min_samples":3,"threshold_bps":"1.5","ticker":"BTC","gate_contract":"BTC_USDT","lighter_market_id":1}
{"type":"gate_snapshot","sequence":1,"timestamp_ns":1000000000,"bids":[["100.0","3.0"],["99.9","2.0"]],"asks":[["100.1","2.5"],["100.2","4.0"]],"book_id":10}
{"type":"lighter_snapshot","sequence":2,"timestamp_ns":1000001000,"bids":[["99.8","1.0"],["99.7","2.0"]],"asks":[["99.9","1.5"],["100.0","3.0"]],"book_id":20}
{"type":"gate_update","sequence":3,"timestamp_ns":1000002000,"first_id":11,"last_id":11,"bids":[["100.0","3.5"]],"asks":[]}
{"type":"lighter_update","sequence":4,"timestamp_ns":1000003000,"bids":[["99.85","1.2"]],"asks":[]}
{"type":"gate_update","sequence":5,"timestamp_ns":1000004000,"first_id":12,"last_id":12,"bids":[],"asks":[["100.1","0"]]}
{"type":"lighter_update","sequence":6,"timestamp_ns":1000005000,"bids":[],"asks":[["99.95","1.1"]]}
{"type":"gate_update","sequence":7,"timestamp_ns":1000006000,"first_id":13,"last_id":13,"bids":[["100.05","1.0"]],"asks":[["100.15","2.0"]]}
{"type":"lighter_update","sequence":8,"timestamp_ns":1000007000,"bids":[["100.2","1.0"]],"asks":[["100.3","1.0"]]}
"#;

    #[test]
    fn replay_fixture_produces_signal_rows_and_jsonl() {
        let path = temp_fixture_path();
        fs::write(&path, FIXTURE).unwrap();

        let rows = replay_jsonl(&path).unwrap();
        fs::remove_file(&path).unwrap();

        assert_eq!(rows.len(), 6);
        assert!(!rows[0].ready);
        assert!(rows[5].ready);
        assert_eq!(rows[5].sequence, 7);
        assert_eq!(format!("{:.8}", rows[5].long_spread), "0.30000000");
        assert_eq!(format!("{:.8}", rows[5].short_spread), "0.15000000");
        assert!(rows[5].long_ok);
        assert!(rows[5].short_ok);

        let json = signal_to_json(&rows[5]);
        assert!(json.contains(r#""sequence":7"#));
        assert!(json.contains(r#""long_spread":"0.30000000""#));
        assert!(json.contains(r#""short_ok":true"#));
        assert!(json.contains(r#""hot":{"sequence":7"#));
        assert!(json.contains(r#""diagnostics":{"source":"#));
        assert!(json.contains(r#""gate_book_status":"ready""#));
    }

    #[test]
    fn gate_gap_suppresses_later_signals() {
        let fixture = r#"{"type":"config","window_size":3,"min_samples":1,"threshold_bps":"1.5","ticker":"BTC","gate_contract":"BTC_USDT","lighter_market_id":1}
{"type":"gate_snapshot","sequence":1,"timestamp_ns":1000000000,"bids":[["100.0","3.0"]],"asks":[["100.1","2.5"]],"book_id":10}
{"type":"lighter_snapshot","sequence":2,"timestamp_ns":1000001000,"bids":[["99.8","1.0"]],"asks":[["99.9","1.5"]],"book_id":20}
{"type":"gate_update","sequence":3,"timestamp_ns":1000002000,"first_id":12,"last_id":12,"bids":[["100.2","1.0"]],"asks":[]}
{"type":"lighter_update","sequence":4,"timestamp_ns":1000003000,"bids":[["100.3","1.0"]],"asks":[]}
{"type":"lighter_update","sequence":5,"timestamp_ns":1000004000,"bids":[],"asks":[["100.4","1.0"]]}
"#;
        let path = temp_fixture_path();
        fs::write(&path, fixture).unwrap();

        let rows = replay_jsonl(&path).unwrap();
        fs::remove_file(&path).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].sequence, 2);
        assert!(rows[0].ready);
    }

    #[test]
    fn stream_jsonl_writes_rows_incrementally() {
        let input = r#"{"type":"config","window_size":3,"min_samples":1,"threshold_bps":"1.5","ticker":"BTC","gate_contract":"BTC_USDT","lighter_market_id":1}
{"type":"gate_snapshot","sequence":1,"timestamp_ns":1000000000,"bids":[["100.0","3.0"]],"asks":[["100.1","2.5"]],"book_id":10}
{"type":"lighter_snapshot","sequence":2,"timestamp_ns":1000001000,"bids":[["99.8","1.0"]],"asks":[["99.9","1.5"]],"book_id":20}
"#;
        let mut output = Vec::new();

        stream_jsonl(std::io::Cursor::new(input), &mut output).unwrap();

        let text = String::from_utf8(output).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains(r#""sequence":2"#));
        assert!(lines[0].contains(r#""ready":true"#));
    }

    fn temp_fixture_path() -> std::path::PathBuf {
        let id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = TEMP_FIXTURE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let process_id = std::process::id();
        std::env::temp_dir().join(format!(
            "bybot-market-engine-{process_id}-{id}-{counter}.jsonl"
        ))
    }
}
