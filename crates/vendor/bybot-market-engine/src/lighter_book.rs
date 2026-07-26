use crate::book::LocalBook;
use crate::model::Level;

pub struct LighterBook {
    book: LocalBook,
    nonce: Option<u64>,
}

impl LighterBook {
    pub fn new() -> Self {
        Self {
            book: LocalBook::new(),
            nonce: None,
        }
    }

    pub fn apply_json(&mut self, message: &str) -> Result<bool, String> {
        let message_type = optional_string_field(message, "type")?.unwrap_or("");
        if message_type == "subscribed/order_book" {
            let body = object_field(message, "order_book")?;
            let nonce = optional_u64_field(body, "nonce")?
                .or(optional_u64_field(body, "last_nonce")?)
                .or(optional_u64_field(body, "offset")?)
                .ok_or_else(|| "missing lighter snapshot nonce".to_string())?;
            self.book.apply_snapshot(
                &levels_field(body, "bids")?,
                &levels_field(body, "asks")?,
                Some(nonce),
            );
            self.nonce = Some(nonce);
            return Ok(true);
        }
        if message_type == "update/order_book" {
            let current = self
                .nonce
                .ok_or_else(|| "lighter update before snapshot".to_string())?;
            let body = object_field(message, "order_book")?;
            let begin_nonce = optional_u64_field(body, "begin_nonce")?
                .ok_or_else(|| "missing lighter begin_nonce".to_string())?;
            let end_nonce = optional_u64_field(body, "nonce")?
                .or(optional_u64_field(body, "last_nonce")?)
                .ok_or_else(|| "missing lighter nonce".to_string())?;
            if begin_nonce < current && end_nonce <= current {
                return Ok(false);
            }
            if begin_nonce != current {
                return Err(format!(
                    "lighter nonce gap: current={current} begin={begin_nonce} end={end_nonce}"
                ));
            }
            self.book.apply_update(
                &levels_field(body, "bids")?,
                &levels_field(body, "asks")?,
                None,
                Some(end_nonce),
            );
            self.nonce = Some(end_nonce);
            return Ok(true);
        }
        Ok(false)
    }

    pub fn book(&self) -> &LocalBook {
        &self.book
    }

    pub fn nonce(&self) -> Option<u64> {
        self.nonce
    }
}

fn string_field<'a>(input: &'a str, key: &str) -> Result<&'a str, String> {
    optional_string_field(input, key)?.ok_or_else(|| format!("missing string field {key}"))
}

fn optional_string_field<'a>(input: &'a str, key: &str) -> Result<Option<&'a str>, String> {
    let marker = format!("\"{key}\":\"");
    let Some(start) = input.find(&marker).map(|idx| idx + marker.len()) else {
        return Ok(None);
    };
    let rest = &input[start..];
    let end = rest
        .find('"')
        .ok_or_else(|| format!("unterminated string field {key}"))?;
    Ok(Some(&rest[..end]))
}

fn optional_u64_field(input: &str, key: &str) -> Result<Option<u64>, String> {
    let marker = format!("\"{key}\":");
    let Some(start) = input.find(&marker).map(|idx| idx + marker.len()) else {
        return Ok(None);
    };
    let rest = &input[start..];
    let end = rest.find([',', '}']).unwrap_or(rest.len());
    rest[..end]
        .trim()
        .parse::<u64>()
        .map(Some)
        .map_err(|err| err.to_string())
}

fn object_field<'a>(input: &'a str, key: &str) -> Result<&'a str, String> {
    let marker = format!("\"{key}\":{{");
    let start = input
        .find(&marker)
        .ok_or_else(|| format!("missing object field {key}"))?
        + marker.len()
        - 1;
    let end = matching_brace_end(&input[start..])
        .ok_or_else(|| format!("unterminated object field {key}"))?;
    Ok(&input[start..=start + end])
}

fn levels_field(input: &str, key: &str) -> Result<Vec<Level>, String> {
    let marker = format!("\"{key}\":[");
    let start = input
        .find(&marker)
        .ok_or_else(|| format!("missing levels field {key}"))?
        + marker.len()
        - 1;
    let rest = &input[start..];
    let end = matching_array_end(rest).ok_or_else(|| format!("unterminated levels field {key}"))?;
    let body = &rest[1..end];
    let mut levels = Vec::new();
    let mut cursor = 0usize;
    while let Some(relative_start) = body[cursor..].find('{') {
        let object_start = cursor + relative_start;
        let object_end = matching_brace_end(&body[object_start..])
            .ok_or_else(|| format!("unterminated level object {key}"))?
            + object_start;
        let object = &body[object_start..=object_end];
        levels.push(Level {
            price: parse_decimal_scaled(string_field(object, "price")?)?,
            size: parse_decimal_scaled(string_field(object, "size")?)?,
        });
        cursor = object_end + 1;
    }
    Ok(levels)
}

fn matching_brace_end(input: &str) -> Option<usize> {
    matching_end(input, '{', '}')
}

fn matching_array_end(input: &str) -> Option<usize> {
    matching_end(input, '[', ']')
}

fn matching_end(input: &str, open: char, close: char) -> Option<usize> {
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escape = false;
    for (idx, ch) in input.char_indices() {
        if in_string {
            if escape {
                escape = false;
            } else if ch == '\\' {
                escape = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        if ch == '"' {
            in_string = true;
            continue;
        }
        if ch == open {
            depth += 1;
        } else if ch == close {
            depth -= 1;
            if depth == 0 {
                return Some(idx);
            }
        }
    }
    None
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

impl Default for LighterBook {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::BookStatus;

    #[test]
    fn snapshot_sets_bbo_and_nonce() {
        let mut book = LighterBook::new();

        book.apply_json(
            r#"{"type":"subscribed/order_book","order_book":{"nonce":10,"bids":[{"price":"99","size":"2"}],"asks":[{"price":"101","size":"3"}]}}"#,
        )
        .unwrap();

        assert_eq!(book.nonce(), Some(10));
        assert_eq!(book.book().status(), BookStatus::Ready);
        assert_eq!(book.book().best_bid(), Some((99.0, 2.0)));
        assert_eq!(book.book().best_ask(), Some((101.0, 3.0)));
    }

    #[test]
    fn update_replaces_levels_and_advances_nonce() {
        let mut book = LighterBook::new();
        book.apply_json(
            r#"{"type":"subscribed/order_book","order_book":{"nonce":10,"bids":[{"price":"99","size":"2"}],"asks":[{"price":"101","size":"3"}]}}"#,
        )
        .unwrap();

        book.apply_json(
            r#"{"type":"update/order_book","order_book":{"begin_nonce":10,"nonce":11,"bids":[{"price":"100","size":"1"}],"asks":[{"price":"101","size":"0"}]}}"#,
        )
        .unwrap();

        assert_eq!(book.nonce(), Some(11));
        assert_eq!(book.book().best_bid(), Some((100.0, 1.0)));
        assert_eq!(book.book().best_ask(), None);
    }

    #[test]
    fn rejects_nonce_gap() {
        let mut book = LighterBook::new();
        book.apply_json(
            r#"{"type":"subscribed/order_book","order_book":{"nonce":10,"bids":[{"price":"99","size":"2"}],"asks":[{"price":"101","size":"3"}]}}"#,
        )
        .unwrap();

        assert!(book
            .apply_json(
                r#"{"type":"update/order_book","order_book":{"begin_nonce":12,"nonce":13,"bids":[],"asks":[]}}"#,
            )
            .is_err());
    }

    #[test]
    fn skips_old_nonce_update_without_reconnect() {
        let mut book = LighterBook::new();
        book.apply_json(
            r#"{"type":"subscribed/order_book","order_book":{"nonce":10,"bids":[{"price":"99","size":"2"}],"asks":[{"price":"101","size":"3"}]}}"#,
        )
        .unwrap();

        let applied = book
            .apply_json(
                r#"{"type":"update/order_book","order_book":{"begin_nonce":8,"nonce":10,"bids":[{"price":"100","size":"1"}],"asks":[]}}"#,
            )
            .unwrap();

        assert!(!applied);
        assert_eq!(book.nonce(), Some(10));
        assert_eq!(book.book().best_bid(), Some((99.0, 2.0)));
    }
}
