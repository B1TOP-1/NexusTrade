use std::{env, str::FromStr, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use hypersdk::{
    hypercore::{self, types::OrderStatus, OrderResponseStatus, PrivateKeySigner},
    Address,
};
use rust_decimal::Decimal;
use tokio::time::Instant;

use crate::{
    account::resolve_execution_account,
    execution::ExecutionService,
    fees_funding::FeeFundingService,
    latency::{LatencyStep, LatencyTrace},
    local_book::LocalBookSnapshot,
    markets::{MarketCatalog, MarketDescriptor},
    order_gateway::SubmittedOrder,
    orders::{OrderIntent, OrderSide},
    user_stream::{FillConfirmation, UserStreamConfig, UserStreamRuntime},
};

const BTC_SYMBOL: &str = "BTC";
const SPCX_SYMBOL: &str = "xyz:SPCX";
const XYZ_DEX: &str = "xyz";
const TEST_NOTIONAL_USD: Decimal = Decimal::from_parts(11, 0, 0, false, 0);
const MARKET_SLIPPAGE_BPS: i64 = 100;

#[derive(Debug, Clone)]
pub struct SmokeTestConfig {
    pub confirm_live: bool,
    pub private_key_env: String,
    pub vault_address_env: String,
}

impl Default for SmokeTestConfig {
    fn default() -> Self {
        Self {
            confirm_live: false,
            private_key_env: "HYPE_PRIVATE_KEY".to_string(),
            vault_address_env: "HYPE_VAULT_ADDRESS".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct OrderAck {
    oid: u64,
}

pub async fn run_execution_smoke(config: SmokeTestConfig) -> Result<()> {
    validate_live_confirmation(&config)?;
    let signer = load_signer(&config.private_key_env)?;
    let account_client = hypercore::mainnet();
    let requested_vault = load_optional_address(&config.vault_address_env)?;
    let account = resolve_execution_account(&account_client, &signer, requested_vault).await?;
    let catalog = MarketCatalog::load_selected(&account_client, &[XYZ_DEX]).await?;
    let btc = catalog
        .get(BTC_SYMBOL)
        .cloned()
        .ok_or_else(|| anyhow!("BTC market not found"))?;
    let spcx = catalog
        .get(SPCX_SYMBOL)
        .cloned()
        .ok_or_else(|| anyhow!("SPCX market not found"))?;
    let fees = FeeFundingService::new(hypercore::mainnet(), account.user());
    print_market_metadata(&fees, account.user(), &btc, &spcx).await?;

    let execution = ExecutionService::mainnet(signer, account);
    execution
        .positions()
        .ensure_flat(None, &[BTC_SYMBOL])
        .await?;
    execution
        .positions()
        .ensure_flat(Some(XYZ_DEX), &[SPCX_SYMBOL, "SPCX"])
        .await?;

    let stream_config =
        UserStreamConfig::new([BTC_SYMBOL, SPCX_SYMBOL], [None, Some(XYZ_DEX.to_string())])?;
    let mut stream = UserStreamRuntime::spawn(account.user(), stream_config);
    let mut btc_cleanup_oid = None;
    let sequence_result =
        run_sequence(&execution, &btc, &spcx, &mut stream, &mut btc_cleanup_oid).await;

    if let Err(sequence_error) = sequence_result {
        let cleanup_result = cleanup_execution(&execution, &btc, &spcx, btc_cleanup_oid).await;
        stream.stop();
        return match cleanup_result {
            Ok(()) => Err(sequence_error),
            Err(cleanup_error) => Err(anyhow!(
                "execution failed: {sequence_error}; emergency cleanup failed: {cleanup_error}"
            )),
        };
    }

    stream.stop();
    println!("[HypeExec] SUCCESS BTC maker/cancel and SPCX open/close confirmed");
    Ok(())
}

async fn run_sequence(
    execution: &ExecutionService,
    btc: &MarketDescriptor,
    spcx: &MarketDescriptor,
    stream: &mut UserStreamRuntime,
    btc_cleanup_oid: &mut Option<u64>,
) -> Result<()> {
    let connected_at = stream.wait_connected().await?;
    println!("[HypeExec] user_ws_connected at={connected_at:?}");
    let btc_book = stream.wait_book(BTC_SYMBOL).await?;
    let spcx_book = stream.wait_book(SPCX_SYMBOL).await?;
    print_book_ready(&btc_book);
    print_book_ready(&spcx_book);

    let btc_bid_fifth = price_at_level(&btc_book, true, 4)?;
    let btc_size = btc
        .precision()
        .minimum_size(btc_bid_fifth, TEST_NOTIONAL_USD)?;
    let btc_price = btc.maker_price(btc_bid_fifth, OrderSide::Buy)?;
    let btc_intent = OrderIntent::limit_maker(
        btc.symbol(),
        btc.market().index,
        OrderSide::Buy,
        btc_price,
        btc_size,
    );
    println!(
        "[HypeExec] BTC maker plan price={} size={} level=bid5",
        btc_price, btc_size
    );
    let (btc_submission, btc_trace) = submit_with_trace(execution, &btc_intent, "BTC").await?;
    let btc_ack = require_resting(&btc_submission.statuses, "BTC maker")?;
    *btc_cleanup_oid = Some(btc_ack.oid);
    print_trace("BTC post_ack", &btc_trace, LatencyStep::TransportAck);
    let btc_open_at = stream
        .wait_order_status(btc_submission.cloid, btc_ack.oid, |status| {
            matches!(status, OrderStatus::Open)
        })
        .await?;
    print_elapsed("BTC order_resting", btc_trace, btc_open_at);

    let cancel_started = Instant::now();
    println!("[HypeExec] BTC cancel_send oid={}", btc_ack.oid);
    let cancel_statuses = execution.cancel(btc.market().index, btc_ack.oid).await?;
    let cancel_ack_at = Instant::now();
    require_success(&cancel_statuses, "BTC cancel")?;
    print_elapsed_at("BTC cancel_ack", cancel_started, cancel_ack_at);
    let cancel_ws_at = stream
        .wait_order_status(btc_submission.cloid, btc_ack.oid, |status| {
            matches!(status, OrderStatus::Canceled)
        })
        .await?;
    *btc_cleanup_oid = None;
    print_elapsed_at("BTC order_canceled", cancel_started, cancel_ws_at);

    let spcx_ask = price_at_level(&spcx_book, false, 0)?;
    let spcx_size = spcx.precision().minimum_size(spcx_ask, TEST_NOTIONAL_USD)?;
    let open_price = spcx.aggressive_price(spcx_ask, true, MARKET_SLIPPAGE_BPS)?;
    let open_intent = OrderIntent::aggressive_ioc(
        spcx.symbol(),
        spcx.market().index,
        OrderSide::Buy,
        open_price,
        spcx_size,
        false,
    );
    println!(
        "[HypeExec] SPCX open plan price_cap={} size={} step={}",
        open_price,
        spcx_size,
        spcx.precision().size_step()
    );
    let (open_submission, open_trace) =
        submit_with_trace(execution, &open_intent, "SPCX open").await?;
    let open_ack = require_filled(&open_submission.statuses, "SPCX open")?;
    print_trace("SPCX open_post_ack", &open_trace, LatencyStep::TransportAck);
    let open_confirmation = stream
        .wait_fill_confirmation(open_submission.cloid, open_ack.oid, SPCX_SYMBOL)
        .await?;
    print_fill_latency("SPCX open", open_trace, &open_confirmation);

    let fresh_book = stream.wait_book(SPCX_SYMBOL).await?;
    let spcx_bid = price_at_level(&fresh_book, true, 0)?;
    let close_price = spcx.aggressive_price(spcx_bid, false, MARKET_SLIPPAGE_BPS)?;
    let close_intent = OrderIntent::aggressive_ioc(
        spcx.symbol(),
        spcx.market().index,
        OrderSide::Sell,
        close_price,
        open_confirmation.fill.sz,
        true,
    );
    println!(
        "[HypeExec] SPCX close plan price_floor={} size={} reduce_only=true",
        close_price, open_confirmation.fill.sz
    );
    let (close_submission, close_trace) =
        submit_with_trace(execution, &close_intent, "SPCX close").await?;
    let close_ack = require_filled(&close_submission.statuses, "SPCX close")?;
    print_trace(
        "SPCX close_post_ack",
        &close_trace,
        LatencyStep::TransportAck,
    );
    let close_confirmation = stream
        .wait_fill_confirmation(close_submission.cloid, close_ack.oid, SPCX_SYMBOL)
        .await?;
    print_fill_latency("SPCX close", close_trace, &close_confirmation);

    execution
        .positions()
        .ensure_flat(Some(XYZ_DEX), &[SPCX_SYMBOL, "SPCX"])
        .await
}

async fn submit_with_trace(
    execution: &ExecutionService,
    intent: &OrderIntent,
    label: &str,
) -> Result<(SubmittedOrder, LatencyTrace)> {
    let started = Instant::now();
    let mut trace = LatencyTrace::new(started);
    println!("[HypeExec] {label} order_send");
    let submission = execution.submit(intent).await?;
    trace.record(LatencyStep::TransportAck, Instant::now());
    Ok((submission, trace))
}

async fn cleanup_execution(
    execution: &ExecutionService,
    btc: &MarketDescriptor,
    spcx: &MarketDescriptor,
    btc_oid: Option<u64>,
) -> Result<()> {
    let mut errors = Vec::new();
    if let Some(oid) = btc_oid {
        println!("[HypeExecCleanup] BTC cancel_send oid={oid}");
        if let Err(error) = execution.cancel(btc.market().index, oid).await {
            errors.push(format!("BTC emergency cancel failed: {error}"));
        }
    }
    if let Err(error) = execution
        .emergency_flatten(btc, &[BTC_SYMBOL], MARKET_SLIPPAGE_BPS)
        .await
    {
        errors.push(format!("BTC emergency flatten failed: {error}"));
    }
    if let Err(error) = execution
        .emergency_flatten(spcx, &[SPCX_SYMBOL, "SPCX"], MARKET_SLIPPAGE_BPS)
        .await
    {
        errors.push(format!("SPCX emergency flatten failed: {error}"));
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    if let Err(error) = execution.positions().ensure_flat(None, &[BTC_SYMBOL]).await {
        errors.push(format!("BTC final state invalid: {error}"));
    }
    if let Err(error) = execution
        .positions()
        .ensure_flat(Some(XYZ_DEX), &[SPCX_SYMBOL, "SPCX"])
        .await
    {
        errors.push(format!("SPCX final state invalid: {error}"));
    }
    if errors.is_empty() {
        println!("[HypeExecCleanup] completed");
        Ok(())
    } else {
        bail!(errors.join("; "))
    }
}

async fn print_market_metadata(
    fees: &FeeFundingService,
    user: Address,
    btc: &MarketDescriptor,
    spcx: &MarketDescriptor,
) -> Result<()> {
    let fee_rates = fees.fees().await?;
    let now_ms = unix_time_ms();
    let funding = fees
        .funding_history(SPCX_SYMBOL, now_ms.saturating_sub(3_600_000), Some(now_ms))
        .await?;
    println!(
        "[HypeExec] account={user} maker_fee={} taker_fee={}",
        fee_rates.maker_rate, fee_rates.taker_rate
    );
    println!(
        "[HypeExec] BTC asset={} sz_decimals={} | SPCX asset={} sz_decimals={} funding_rows={}",
        btc.market().index,
        btc.market().sz_decimals,
        spcx.market().index,
        spcx.market().sz_decimals,
        funding.len()
    );
    Ok(())
}

fn validate_live_confirmation(config: &SmokeTestConfig) -> Result<()> {
    if !config.confirm_live || env::var("HYPE_CONFIRM_LIVE").as_deref() != Ok("YES") {
        bail!("live trading blocked: pass --confirm-live and set HYPE_CONFIRM_LIVE=YES");
    }
    Ok(())
}

fn load_signer(variable: &str) -> Result<PrivateKeySigner> {
    let private_key = env::var(variable).with_context(|| format!("missing {variable}"))?;
    PrivateKeySigner::from_str(private_key.trim()).context("invalid Hype private key")
}

fn load_optional_address(variable: &str) -> Result<Option<Address>> {
    match env::var(variable) {
        Ok(value) if !value.trim().is_empty() => Address::from_str(value.trim())
            .map(Some)
            .with_context(|| format!("invalid {variable}")),
        _ => Ok(None),
    }
}

fn require_resting(statuses: &[OrderResponseStatus], label: &str) -> Result<OrderAck> {
    match statuses.first() {
        Some(OrderResponseStatus::Resting { oid, .. }) => Ok(OrderAck { oid: *oid }),
        Some(status) => bail!("{label} did not rest: {status:?}"),
        None => bail!("{label} returned no status"),
    }
}

fn require_filled(statuses: &[OrderResponseStatus], label: &str) -> Result<OrderAck> {
    match statuses.first() {
        Some(OrderResponseStatus::Filled { oid, .. }) => Ok(OrderAck { oid: *oid }),
        Some(status) => bail!("{label} did not fill immediately: {status:?}"),
        None => bail!("{label} returned no status"),
    }
}

fn require_success(statuses: &[OrderResponseStatus], label: &str) -> Result<()> {
    match statuses.first() {
        Some(status) if status.is_ok() => Ok(()),
        Some(status) => bail!("{label} failed: {status:?}"),
        None => bail!("{label} returned no status"),
    }
}

fn price_at_level(snapshot: &LocalBookSnapshot, bid: bool, index: usize) -> Result<Decimal> {
    let levels = if bid {
        snapshot.bids()
    } else {
        snapshot.asks()
    };
    levels
        .get(index)
        .map(|level| Decimal::from_i128_with_scale(i128::from(level.price()), 8))
        .ok_or_else(|| {
            anyhow!(
                "{} missing order-book level {}",
                snapshot.symbol(),
                index + 1
            )
        })
}

fn print_book_ready(snapshot: &LocalBookSnapshot) {
    println!(
        "[HypeExec] book_ready symbol={} levels={}/{}",
        snapshot.symbol(),
        snapshot.bids().len(),
        snapshot.asks().len()
    );
}

fn print_trace(label: &str, trace: &LatencyTrace, step: LatencyStep) {
    if let Some(elapsed) = trace.elapsed(step) {
        println!(
            "[HypeExecLatency] step={label} latency_us={}",
            elapsed.as_micros()
        );
    }
}

fn print_elapsed(label: &str, trace: LatencyTrace, completed: Instant) {
    let mut trace = trace;
    trace.record(LatencyStep::OrderConfirmed, completed);
    print_trace(label, &trace, LatencyStep::OrderConfirmed);
}

fn print_elapsed_at(label: &str, started: Instant, completed: Instant) {
    println!(
        "[HypeExecLatency] step={label} latency_us={}",
        completed.duration_since(started).as_micros()
    );
}

fn print_fill_latency(label: &str, trace: LatencyTrace, confirmation: &FillConfirmation) {
    let mut trace = trace;
    trace.record(LatencyStep::OrderConfirmed, confirmation.order_ws_at);
    trace.record(LatencyStep::FillConfirmed, confirmation.fill_ws_at);
    print_trace(
        &format!("{label}_order_filled"),
        &trace,
        LatencyStep::OrderConfirmed,
    );
    print_trace(
        &format!("{label}_user_fill"),
        &trace,
        LatencyStep::FillConfirmed,
    );
    println!(
        "[HypeExec] user_fill oid={} coin={} size={} price={} fee={} crossed={}",
        confirmation.fill.oid,
        confirmation.fill.coin,
        confirmation.fill.sz,
        confirmation.fill.px,
        confirmation.fill.fee,
        confirmation.fill.crossed
    );
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_live_confirmation_requires_both_gates() {
        assert!(validate_live_confirmation(&SmokeTestConfig::default()).is_err());
    }
}
