use std::{
    collections::HashMap,
    str::FromStr,
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use reqwest::{Client, StatusCode};
use rust_decimal::Decimal;
use serde_json::Value;

use crate::{
    data::{parse_lighter_market_specs, LighterMarketSpec},
    execution::{
        parse_lighter_cancel_ack, parse_lighter_submit_ack, LighterCancelAck, LighterSendTxError,
        LighterSubmitAck,
    },
};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const GET_MAX_ATTEMPTS: usize = 4;

#[derive(Clone, PartialEq, Eq)]
pub struct LighterSignedTx {
    pub client_order_id: String,
    pub client_order_index: Option<u64>,
    pub tx_type: u8,
    pub tx_info: String,
    pub price_protection: bool,
}

impl std::fmt::Debug for LighterSignedTx {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LighterSignedTx")
            .field("client_order_id", &self.client_order_id)
            .field("client_order_index", &self.client_order_index)
            .field("tx_type", &self.tx_type)
            .field("tx_info", &"<redacted>")
            .field("price_protection", &self.price_protection)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterPositionSnapshot {
    pub market_id: u64,
    pub signed_quantity: Decimal,
    pub average_price: Decimal,
    pub unrealized_pnl: Option<Decimal>,
    pub return_on_equity: Option<Decimal>,
    pub liquidation_price: Option<Decimal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LighterSubmitTransportTiming {
    /// Time until the HTTP response headers are available.
    pub send_ms: u64,
    /// Time until the complete sendTx response body is available.
    pub ack_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterAccountSnapshot {
    pub collateral: Decimal,
    pub available_balance: Decimal,
    pub positions: Vec<LighterPositionSnapshot>,
}

#[derive(Clone, Debug)]
pub struct LighterHttpClient {
    client: Client,
    base_url: String,
}

impl LighterHttpClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let base_url = normalize_base_url(base_url)?;
        let client = Client::builder()
            .timeout(DEFAULT_TIMEOUT)
            .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
            .user_agent("bybot-lighter/0.1")
            .build()
            .context("failed to build Lighter HTTP client")?;
        Ok(Self { client, base_url })
    }

    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    #[must_use]
    pub fn sendtx_url(&self) -> String {
        self.endpoint("/api/v1/sendTx")
    }

    #[must_use]
    pub fn next_nonce_url(&self, account_index: u64, api_key_index: u8) -> String {
        format!(
            "{}?account_index={account_index}&api_key_index={api_key_index}",
            self.endpoint("/api/v1/nextNonce")
        )
    }

    #[must_use]
    pub fn account_url(&self, account_index: u64) -> String {
        format!(
            "{}?by=index&value={account_index}",
            self.endpoint("/api/v1/account")
        )
    }

    #[must_use]
    pub fn order_books_url(&self) -> String {
        self.endpoint("/api/v1/orderBooks")
    }

    #[must_use]
    pub fn sendtx_form(tx: &LighterSignedTx) -> HashMap<String, String> {
        HashMap::from([
            ("tx_type".to_owned(), tx.tx_type.to_string()),
            ("tx_info".to_owned(), tx.tx_info.clone()),
            (
                "price_protection".to_owned(),
                tx.price_protection.to_string(),
            ),
        ])
    }

    pub async fn next_nonce(&self, account_index: u64, api_key_index: u8) -> Result<i64> {
        let payload = self
            .get_text(self.next_nonce_url(account_index, api_key_index))
            .await?;
        parse_next_nonce(&payload)
    }

    pub async fn account_snapshot(&self, account_index: u64) -> Result<LighterAccountSnapshot> {
        let payload = self.get_text(self.account_url(account_index)).await?;
        parse_account_snapshot(&payload)
    }

    pub async fn market_specs(&self) -> Result<Vec<LighterMarketSpec>> {
        let payload = self.get_text(self.order_books_url()).await?;
        parse_lighter_market_specs(&payload)
    }

    pub async fn submit_tx(
        &self,
        tx: &LighterSignedTx,
    ) -> Result<LighterSubmitAck, LighterSendTxError> {
        let (ack, _) = self.submit_tx_timed(tx).await?;
        Ok(ack)
    }

    pub async fn submit_tx_timed(
        &self,
        tx: &LighterSignedTx,
    ) -> Result<(LighterSubmitAck, LighterSubmitTransportTiming), LighterSendTxError> {
        let (payload, timing) = self.sendtx_timed(tx).await?;
        let ack = parse_lighter_submit_ack(
            &payload,
            tx.client_order_id.clone(),
            tx.client_order_index,
        )?;
        Ok((ack, timing))
    }

    pub async fn cancel_tx(
        &self,
        tx: &LighterSignedTx,
    ) -> Result<LighterCancelAck, LighterSendTxError> {
        let payload = self.sendtx(tx).await?;
        parse_lighter_cancel_ack(&payload, tx.client_order_id.clone(), tx.client_order_index)
    }

    async fn sendtx(&self, tx: &LighterSignedTx) -> Result<String, LighterSendTxError> {
        Ok(self.sendtx_timed(tx).await?.0)
    }

    async fn sendtx_timed(
        &self,
        tx: &LighterSignedTx,
    ) -> Result<(String, LighterSubmitTransportTiming), LighterSendTxError> {
        let request_started = Instant::now();
        let response = self
            .client
            .post(self.sendtx_url())
            .form(&Self::sendtx_form(tx))
            .send()
            .await
            .map_err(transport_error)?;
        let send_ms = elapsed_millis(request_started);
        let status = response.status();
        let payload = response.text().await.map_err(transport_error)?;
        let ack_ms = elapsed_millis(request_started);
        if !status.is_success() {
            return Err(LighterSendTxError {
                code: i64::from(status.as_u16()),
                message: sanitized_sendtx_error(status, &payload),
            });
        }
        Ok((
            payload,
            LighterSubmitTransportTiming { send_ms, ack_ms },
        ))
    }

    async fn get_text(&self, url: String) -> Result<String> {
        for attempt in 0..GET_MAX_ATTEMPTS {
            let response = self
                .client
                .get(&url)
                .send()
                .await
                .context("Lighter HTTP request failed")?;
            let status = response.status();
            if status.is_success() {
                return response
                    .text()
                    .await
                    .context("failed to read Lighter HTTP response");
            }
            let retryable = matches!(
                status,
                StatusCode::TOO_MANY_REQUESTS | StatusCode::METHOD_NOT_ALLOWED
            );
            if !retryable || attempt + 1 == GET_MAX_ATTEMPTS {
                bail!("Lighter read-only HTTP status {status}");
            }
            tokio::time::sleep(Duration::from_secs(1_u64 << attempt)).await;
        }
        unreachable!("bounded Lighter GET retry loop always returns")
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }
}

fn elapsed_millis(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn sanitized_sendtx_error(status: StatusCode, payload: &str) -> String {
    let message = serde_json::from_str::<Value>(payload)
        .ok()
        .and_then(|value| {
            ["message", "error", "msg"]
                .iter()
                .find_map(|key| value.get(key).and_then(Value::as_str))
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "response body redacted".to_string());
    format!("Lighter sendTx HTTP status {status}: {message}")
}

pub fn parse_next_nonce(payload: &str) -> Result<i64> {
    let value: Value = serde_json::from_str(payload).context("invalid nextNonce JSON")?;
    ensure_success_code(&value, "nextNonce")?;
    let body = value.get("data").unwrap_or(&value);
    body.get("nonce")
        .or_else(|| value.get("nonce"))
        .and_then(Value::as_i64)
        .context("nextNonce response missing integer nonce")
}

pub fn parse_account_snapshot(payload: &str) -> Result<LighterAccountSnapshot> {
    let value: Value = serde_json::from_str(payload).context("invalid account JSON")?;
    ensure_success_code(&value, "account")?;
    let account = value
        .get("accounts")
        .and_then(Value::as_array)
        .and_then(|accounts| accounts.first())
        .context("account response contains no account")?;

    let collateral = decimal_field(account, "collateral")?;
    let available_balance = decimal_field(account, "available_balance")?;
    let positions = account
        .get("positions")
        .and_then(Value::as_array)
        .map(|positions| {
            positions
                .iter()
                .map(parse_position_snapshot)
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();

    Ok(LighterAccountSnapshot {
        collateral,
        available_balance,
        positions,
    })
}

fn normalize_base_url(base_url: &str) -> Result<String> {
    let base_url = base_url.trim().trim_end_matches('/');
    if base_url.is_empty() {
        bail!("Lighter HTTP base URL must not be empty");
    }
    if !(base_url.starts_with("https://") || base_url.starts_with("http://")) {
        bail!("Lighter HTTP base URL must use http or https");
    }
    Ok(base_url.to_owned())
}

fn parse_position_snapshot(value: &Value) -> Result<LighterPositionSnapshot> {
    let market_id = value
        .get("market_id")
        .and_then(Value::as_u64)
        .context("position missing market_id")?;
    let sign = value.get("sign").and_then(Value::as_i64).unwrap_or(1);
    let quantity = decimal_field(value, "position")?;
    let signed_quantity = quantity
        .checked_mul(Decimal::from(sign))
        .context("position quantity overflow")?;
    Ok(LighterPositionSnapshot {
        market_id,
        signed_quantity,
        average_price: decimal_field(value, "avg_entry_price")?,
        unrealized_pnl: optional_decimal_field(value, "unrealized_pnl"),
        return_on_equity: optional_decimal_field(value, "return_on_equity"),
        liquidation_price: optional_decimal_field(value, "liquidation_price"),
    })
}

fn optional_decimal_field(value: &Value, field: &str) -> Option<Decimal> {
    value.get(field).and_then(|value| match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.to_string().parse().ok(),
        _ => None,
    })
}

fn decimal_field(value: &Value, field: &str) -> Result<Decimal> {
    let raw = value
        .get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("missing decimal field {field}"))?;
    Decimal::from_str(raw).with_context(|| format!("invalid decimal field {field}: {raw}"))
}

fn ensure_success_code(value: &Value, endpoint: &str) -> Result<()> {
    let Some(code) = value.get("code").and_then(Value::as_i64) else {
        return Ok(());
    };
    if code == 200 {
        return Ok(());
    }
    let message = value
        .get("message")
        .or_else(|| value.get("error"))
        .and_then(Value::as_str)
        .unwrap_or("unknown error");
    bail!("Lighter {endpoint} failed with code {code}: {message}")
}

fn transport_error(error: reqwest::Error) -> LighterSendTxError {
    LighterSendTxError {
        code: -1,
        message: format!("Lighter sendTx transport failure: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use reqwest::StatusCode;

    use super::{sanitized_sendtx_error, LighterSignedTx};

    #[test]
    fn signed_transaction_debug_and_http_error_redact_payloads() {
        let secret = "signed-secret-payload";
        let transaction = LighterSignedTx {
            client_order_id: "plan-left".to_string(),
            client_order_index: Some(7),
            tx_type: 14,
            tx_info: secret.to_string(),
            price_protection: true,
        };
        let debug = format!("{transaction:?}");
        let error = sanitized_sendtx_error(
            StatusCode::BAD_REQUEST,
            &format!(r#"{{"message":"invalid order","tx_info":"{secret}"}}"#),
        );

        assert!(!debug.contains(secret));
        assert!(debug.contains("<redacted>"));
        assert!(!error.contains(secret));
        assert!(error.contains("invalid order"));
    }
}
