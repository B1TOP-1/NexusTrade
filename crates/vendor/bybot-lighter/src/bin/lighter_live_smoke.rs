use std::{
    env,
    str::FromStr,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use bybot_lighter::{
    data::{parse_order_book_message, LighterMarketSpec},
    execution::LighterExecutionEffect,
    execution_client::{
        LighterExecutionClient, LighterExecutionConfig, LighterOrderRequest, LighterOrderType,
        LighterSubmitTiming, LighterTimeInForce,
    },
    http::LighterHttpClient,
    local_book::LighterLocalBook,
    websocket::{LighterWebSocketClient, LighterWebSocketConfig, LighterWsEvent},
};
use rust_decimal::Decimal;
use tokio::sync::broadcast;

const SYMBOL: &str = "BTC";
const QUANTITY_TEXT: &str = "0.001";
const CONFIRMATION: &str = "BUY_0.001_BTC_AND_AUTO_CLOSE";
const WS_TRANSPORT_CONFIRMATION: &str = "WS";
const DEFAULT_HTTP_URL: &str = "https://mainnet.zklighter.elliot.ai";
const DEFAULT_PRIVATE_WS_URL: &str = "wss://mainnet.zklighter.elliot.ai/stream";
const DEFAULT_PUBLIC_WS_URL: &str = "wss://mainnet.zklighter.elliot.ai/stream?readonly=true";
const CLIENT_ORDER_INDEX_MAX: u64 = (1_u64 << 48) - 1;
const EVENT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct FillResult {
    quantity: Decimal,
    average_price: Decimal,
    fee: Decimal,
    position: Decimal,
    first_fill_ms: u64,
    position_confirm_ms: u64,
}

#[derive(Debug, Clone, Copy)]
struct PublicBbo {
    bid: Decimal,
    ask: Decimal,
    connect_ms: u64,
    subscribe_ms: u64,
    first_book_ms: u64,
    total_ms: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    run().await
}

async fn run() -> Result<()> {
    let use_ws_submission = env::args().any(|argument| argument == "--ws");
    let quantity = Decimal::from_str(QUANTITY_TEXT)?;
    let http_url = optional_env("LIGHTER_HTTP_URL", DEFAULT_HTTP_URL);
    let public_ws_url = optional_env("LIGHTER_PUBLIC_WS_URL", DEFAULT_PUBLIC_WS_URL);
    let market_started = Instant::now();
    let market = load_btc_market(&http_url).await?;
    println!("STEP latency_market_specs total_ms={}", elapsed_millis(market_started));
    if quantity < market.min_base_amount {
        bail!(
            "BTC smoke quantity {quantity} is below Lighter minimum {}",
            market.min_base_amount
        );
    }
    let open_bbo = public_bbo(&public_ws_url, market.market_id).await?;
    println!(
        "STEP public_ws_ok symbol={SYMBOL} market_id={} bid={} ask={}",
        market.market_id, open_bbo.bid, open_bbo.ask
    );
    println!(
        "STEP latency_public_bbo connect_ms={} subscribe_ms={} first_book_ms={} total_ms={}",
        open_bbo.connect_ms,
        open_bbo.subscribe_ms,
        open_bbo.first_book_ms,
        open_bbo.total_ms,
    );
    if env::args().any(|argument| argument == "--public-only") {
        println!("PASS lighter_live_smoke public_price=true private_fill=skipped position_ws=skipped auto_closed=skipped");
        return Ok(());
    }

    require_confirmation()?;
    if use_ws_submission {
        require_ws_transport_confirmation()?;
    }
    let private_ws_url = optional_env("LIGHTER_PRIVATE_WS_URL", DEFAULT_PRIVATE_WS_URL);
    let account_index = required_env("LIGHTER_ACCOUNT_INDEX")?.parse::<u64>()?;
    let api_key_index = required_env("LIGHTER_API_KEY_INDEX")?.parse::<u8>()?;
    let chain_id = env::var("LIGHTER_CHAIN_ID")
        .unwrap_or_else(|_| "304".to_string())
        .parse::<u32>()?;
    let private_key = required_env("LIGHTER_PRIVATE_KEY")?;

    let config = LighterExecutionConfig::new(
        http_url,
        private_ws_url,
        account_index,
        api_key_index,
        chain_id,
    )?;
    let execution_connect_started = Instant::now();
    let client = LighterExecutionClient::connect(config, &private_key).await?;
    println!(
        "STEP latency_execution_connect total_ms={}",
        elapsed_millis(execution_connect_started)
    );
    let private_runtime_started = Instant::now();
    let runtime = client.spawn_private_runtime().await?;
    let private_runtime_start_ms = elapsed_millis(private_runtime_started);
    let mut effects = runtime.subscribe();
    let snapshot_started = Instant::now();
    client.wait_account_snapshot().await?;
    println!(
        "STEP latency_private_snapshot runtime_start_ms={} snapshot_ready_ms={}",
        private_runtime_start_ms,
        elapsed_millis(snapshot_started)
    );
    if use_ws_submission {
        let ws_entry_started = Instant::now();
        client
            .enable_ws_submission()
            .await
            .context("connect Lighter WS order-entry socket")?;
        println!("STEP ws_order_entry_connected=true");
        println!(
            "STEP latency_ws_order_entry connected_ms={}",
            elapsed_millis(ws_entry_started)
        );
    }
    let initial_position = btc_position(&client).await?;
    println!("STEP private_ws_ok initial_btc_position={initial_position}");
    if !initial_position.is_zero() {
        bail!(
            "BTC position must be zero before smoke test; current={initial_position}. No order was sent"
        );
    }

    let open_index = next_client_order_index()?;
    let open_id = format!("lighter-smoke-open-{open_index}");
    client.restore_order_tracking(&open_id, open_index)?;
    let open_order_started = Instant::now();
    let open_receipt = submit_order(
        &client,
        market_order(
            &open_id,
            open_index,
            quantity,
            open_bbo.ask * Decimal::new(101, 2),
            false,
        ),
        use_ws_submission,
    )
    .await?;
    println!(
        "STEP open_submitted client_order_index={open_index} tx_hash={}",
        open_receipt.ack.tx_hash
    );
    println!("{}", format_submit_timing("open", open_receipt.timing));
    let open_fill =
        wait_for_fill_and_position(&client, &mut effects, open_index, quantity, quantity).await?;
    println!(
        "STEP open_filled quantity={} average_price={} fee={} ws_position={}",
        open_fill.quantity, open_fill.average_price, open_fill.fee, open_fill.position
    );
    println!(
        "STEP latency_private_lifecycle order=open ack_to_first_fill_ms={} ack_to_position_confirm_ms={} order_e2e_ms={}",
        open_fill.first_fill_ms,
        open_fill.position_confirm_ms,
        elapsed_millis(open_order_started)
    );

    let close_bbo = public_bbo(&public_ws_url, market.market_id).await?;
    println!("STEP close_public_ws_ok bid={} ask={}", close_bbo.bid, close_bbo.ask);
    println!(
        "STEP latency_close_public_bbo connect_ms={} subscribe_ms={} first_book_ms={} total_ms={}",
        close_bbo.connect_ms,
        close_bbo.subscribe_ms,
        close_bbo.first_book_ms,
        close_bbo.total_ms,
    );
    let close_index = ((open_index + 1) & CLIENT_ORDER_INDEX_MAX).max(1);
    let close_id = format!("lighter-smoke-close-{close_index}");
    client.restore_order_tracking(&close_id, close_index)?;
    let close_order_started = Instant::now();
    let close_receipt = submit_order(
        &client,
        market_order(
            &close_id,
            close_index,
            -open_fill.quantity,
            close_bbo.bid * Decimal::new(99, 2),
            true,
        ),
        use_ws_submission,
    )
    .await?;
    println!(
        "STEP close_submitted client_order_index={close_index} tx_hash={}",
        close_receipt.ack.tx_hash
    );
    println!("{}", format_submit_timing("close", close_receipt.timing));
    let close_fill = wait_for_fill_and_position(
        &client,
        &mut effects,
        close_index,
        open_fill.quantity,
        Decimal::ZERO,
    )
    .await?;
    println!(
        "STEP close_filled quantity={} average_price={} fee={} ws_position={}",
        close_fill.quantity, close_fill.average_price, close_fill.fee, close_fill.position
    );
    println!(
        "STEP latency_private_lifecycle order=close ack_to_first_fill_ms={} ack_to_position_confirm_ms={} order_e2e_ms={}",
        close_fill.first_fill_ms,
        close_fill.position_confirm_ms,
        elapsed_millis(close_order_started)
    );
    println!(
        "PASS lighter_live_smoke transport={} public_price=true private_fill=true position_ws=true auto_closed=true",
        if use_ws_submission { "ws" } else { "http" }
    );
    Ok(())
}

fn require_confirmation() -> Result<()> {
    let confirmation = env::var("LIGHTER_LIVE_SMOKE_CONFIRM").unwrap_or_default();
    if confirmation != CONFIRMATION {
        bail!(
            "live order blocked: set LIGHTER_LIVE_SMOKE_CONFIRM={CONFIRMATION} to authorize one 0.001 BTC buy and automatic reduce-only close"
        );
    }
    Ok(())
}

fn require_ws_transport_confirmation() -> Result<()> {
    let confirmation = env::var("LIGHTER_LIVE_SMOKE_TRANSPORT").unwrap_or_default();
    if confirmation != WS_TRANSPORT_CONFIRMATION {
        bail!(
            "WS order entry blocked: set LIGHTER_LIVE_SMOKE_TRANSPORT={WS_TRANSPORT_CONFIRMATION} together with LIGHTER_LIVE_SMOKE_CONFIRM to authorize the WS smoke path"
        );
    }
    Ok(())
}

async fn submit_order(
    client: &LighterExecutionClient,
    request: LighterOrderRequest,
    use_ws_submission: bool,
) -> Result<bybot_lighter::execution_client::LighterSubmitReceipt> {
    if use_ws_submission {
        client.submit_order_ws(&request).await
    } else {
        client.submit_order(&request).await
    }
}

async fn load_btc_market(http_url: &str) -> Result<LighterMarketSpec> {
    LighterHttpClient::new(http_url)?
        .market_specs()
        .await?
        .into_iter()
        .find(|market| market.symbol == SYMBOL)
        .context("Lighter BTC market is unavailable")
}

async fn public_bbo(ws_url: &str, market_id: u64) -> Result<PublicBbo> {
    tokio::time::timeout(EVENT_TIMEOUT, async move {
        let started = Instant::now();
        let websocket = LighterWebSocketClient::new(LighterWebSocketConfig::new(ws_url)?);
        let mut connection = websocket.connect().await?;
        let connect_ms = elapsed_millis(started);
        connection
            .subscribe_public(&format!("order_book/{market_id}"))
            .await?;
        let subscribe_ms = elapsed_millis(started);
        let mut book = LighterLocalBook::new();
        loop {
            match connection.next_event().await? {
                LighterWsEvent::Text(payload) => {
                    let Some(message) = parse_order_book_message(&payload)? else {
                        continue;
                    };
                    let outcome = book.apply(&message)?;
                    if outcome.requires_resubscribe {
                        bail!("Lighter BTC order book has a nonce gap");
                    }
                    if let Some(top) = book.top_of_book() {
                        let first_book_ms = elapsed_millis(started);
                        return Ok(PublicBbo {
                            bid: top.bid_price,
                            ask: top.ask_price,
                            connect_ms,
                            subscribe_ms,
                            first_book_ms,
                            total_ms: elapsed_millis(started),
                        });
                    }
                }
                LighterWsEvent::Closed => bail!("Lighter public WebSocket closed"),
                LighterWsEvent::Reconnected => {}
            }
        }
    })
    .await
    .context("timed out waiting for Lighter BTC public order book")?
}

fn market_order(
    client_order_id: &str,
    client_order_index: u64,
    signed_quantity: Decimal,
    protection_price: Decimal,
    reduce_only: bool,
) -> LighterOrderRequest {
    LighterOrderRequest {
        symbol: SYMBOL.to_string(),
        client_order_id: client_order_id.to_string(),
        client_order_index,
        signed_quantity,
        limit_price: Some(protection_price),
        order_type: LighterOrderType::Market,
        time_in_force: LighterTimeInForce::ImmediateOrCancel,
        reduce_only,
    }
}

async fn wait_for_fill_and_position(
    client: &LighterExecutionClient,
    effects: &mut broadcast::Receiver<LighterExecutionEffect>,
    client_order_index: u64,
    expected_fill: Decimal,
    expected_position: Decimal,
) -> Result<FillResult> {
    tokio::time::timeout(EVENT_TIMEOUT, async {
        let started = Instant::now();
        let mut quantity = Decimal::ZERO;
        let mut quote = Decimal::ZERO;
        let mut fee = Decimal::ZERO;
        let mut observed_fill = false;
        let mut first_fill_ms = None;
        let mut position_interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            tokio::select! {
                effect = effects.recv() => {
                    let effect = effect.context("Lighter private execution event stream closed")?;
                    match effect {
                        LighterExecutionEffect::Fill { client_order_index: Some(index), quantity: fill_quantity, price, fee: fill_fee, .. }
                            if index == client_order_index => {
                                let fill_quantity = Decimal::from_str(&fill_quantity)?;
                                let fill_price = price.as_deref().map(Decimal::from_str).transpose()?.unwrap_or(Decimal::ZERO);
                                quantity += fill_quantity;
                                quote += fill_quantity * fill_price;
                                fee += Decimal::new(fill_fee, 6);
                                observed_fill = true;
                                first_fill_ms.get_or_insert_with(|| elapsed_millis(started));
                            }
                        LighterExecutionEffect::Rejected { client_order_index: Some(index), reason, .. }
                            if index == client_order_index => bail!("Lighter order rejected by private WebSocket: {reason}"),
                        LighterExecutionEffect::Canceled { client_order_index: Some(index), reason, .. }
                            if index == client_order_index && quantity < expected_fill => bail!("Lighter order canceled before full fill: {reason}"),
                        _ => {}
                    }
                }
                _ = position_interval.tick() => {
                    let position = btc_position(client).await?;
                    if observed_fill && quantity >= expected_fill && position == expected_position {
                        return Ok(FillResult {
                            quantity,
                            average_price: if quantity.is_zero() { Decimal::ZERO } else { quote / quantity },
                            fee,
                            position,
                            first_fill_ms: first_fill_ms.unwrap_or_default(),
                            position_confirm_ms: elapsed_millis(started),
                        });
                    }
                }
            }
        }
    })
    .await
    .with_context(|| format!("timed out waiting for Lighter private fill/position client_order_index={client_order_index}"))?
}

async fn btc_position(client: &LighterExecutionClient) -> Result<Decimal> {
    Ok(client
        .position(SYMBOL)
        .await?
        .map_or(Decimal::ZERO, |position| position.signed_quantity))
}

fn next_client_order_index() -> Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_millis();
    Ok((u64::try_from(millis)? & CLIENT_ORDER_INDEX_MAX).max(1))
}

fn required_env(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("missing {name}"))?;
    if value.trim().is_empty() {
        bail!("{name} cannot be blank");
    }
    Ok(value)
}

fn optional_env(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

fn elapsed_millis(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn format_submit_timing(order: &str, timing: LighterSubmitTiming) -> String {
    format!(
        "STEP latency_submit order={order} lock_wait_ms={} sign_ms={} send_ms={} ack_ms={} submit_total_ms={}",
        timing.lock_wait_ms,
        timing.sign_ms,
        timing.send_ms,
        timing.ack_ms,
        timing.submit_total_ms,
    )
}

#[cfg(test)]
mod tests {
    use bybot_lighter::execution_client::LighterSubmitTiming;

    use super::format_submit_timing;

    #[test]
    fn formats_complete_submit_latency_breakdown() {
        let timing = LighterSubmitTiming {
            submit_started_at_ms: 1_700_000_000_000,
            lock_wait_ms: 2,
            sign_ms: 3,
            send_ms: 5,
            ack_ms: 7,
            submit_total_ms: 11,
        };

        assert_eq!(
            format_submit_timing("open", timing),
            "STEP latency_submit order=open lock_wait_ms=2 sign_ms=3 send_ms=5 ack_ms=7 submit_total_ms=11"
        );
    }
}
