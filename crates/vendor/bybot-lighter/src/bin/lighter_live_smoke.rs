use std::{
    env,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use bybot_lighter::{
    data::{parse_order_book_message, LighterMarketSpec},
    execution::LighterExecutionEffect,
    execution_client::{
        LighterExecutionClient, LighterExecutionConfig, LighterOrderRequest, LighterOrderType,
        LighterTimeInForce,
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
}

#[tokio::main]
async fn main() -> Result<()> {
    run().await
}

async fn run() -> Result<()> {
    let quantity = Decimal::from_str(QUANTITY_TEXT)?;
    let http_url = optional_env("LIGHTER_HTTP_URL", DEFAULT_HTTP_URL);
    let public_ws_url = optional_env("LIGHTER_PUBLIC_WS_URL", DEFAULT_PUBLIC_WS_URL);
    let market = load_btc_market(&http_url).await?;
    if quantity < market.min_base_amount {
        bail!(
            "BTC smoke quantity {quantity} is below Lighter minimum {}",
            market.min_base_amount
        );
    }
    let (bid, ask) = public_bbo(&public_ws_url, market.market_id).await?;
    println!(
        "STEP public_ws_ok symbol={SYMBOL} market_id={} bid={bid} ask={ask}",
        market.market_id
    );
    if env::args().any(|argument| argument == "--public-only") {
        println!("PASS lighter_live_smoke public_price=true private_fill=skipped position_ws=skipped auto_closed=skipped");
        return Ok(());
    }

    require_confirmation()?;
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
    let client = LighterExecutionClient::connect(config, &private_key).await?;
    let runtime = client.spawn_private_runtime().await?;
    let mut effects = runtime.subscribe();
    client.wait_account_snapshot().await?;
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
    let open_receipt = client
        .submit_order(&market_order(
            &open_id,
            open_index,
            quantity,
            ask * Decimal::new(101, 2),
            false,
        ))
        .await?;
    println!(
        "STEP open_submitted client_order_index={open_index} tx_hash={}",
        open_receipt.ack.tx_hash
    );
    let open_fill =
        wait_for_fill_and_position(&client, &mut effects, open_index, quantity, quantity).await?;
    println!(
        "STEP open_filled quantity={} average_price={} fee={} ws_position={}",
        open_fill.quantity, open_fill.average_price, open_fill.fee, open_fill.position
    );

    let (close_bid, close_ask) = public_bbo(&public_ws_url, market.market_id).await?;
    println!("STEP close_public_ws_ok bid={close_bid} ask={close_ask}");
    let close_index = ((open_index + 1) & CLIENT_ORDER_INDEX_MAX).max(1);
    let close_id = format!("lighter-smoke-close-{close_index}");
    client.restore_order_tracking(&close_id, close_index)?;
    let close_receipt = client
        .submit_order(&market_order(
            &close_id,
            close_index,
            -open_fill.quantity,
            close_bid * Decimal::new(99, 2),
            true,
        ))
        .await?;
    println!(
        "STEP close_submitted client_order_index={close_index} tx_hash={}",
        close_receipt.ack.tx_hash
    );
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
        "PASS lighter_live_smoke public_price=true private_fill=true position_ws=true auto_closed=true"
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

async fn load_btc_market(http_url: &str) -> Result<LighterMarketSpec> {
    LighterHttpClient::new(http_url)?
        .market_specs()
        .await?
        .into_iter()
        .find(|market| market.symbol == SYMBOL)
        .context("Lighter BTC market is unavailable")
}

async fn public_bbo(ws_url: &str, market_id: u64) -> Result<(Decimal, Decimal)> {
    tokio::time::timeout(EVENT_TIMEOUT, async move {
        let websocket = LighterWebSocketClient::new(LighterWebSocketConfig::new(ws_url)?);
        let mut connection = websocket.connect().await?;
        connection
            .subscribe_public(&format!("order_book/{market_id}"))
            .await?;
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
                        return Ok((top.bid_price, top.ask_price));
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
        let mut quantity = Decimal::ZERO;
        let mut quote = Decimal::ZERO;
        let mut fee = Decimal::ZERO;
        let mut observed_fill = false;
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
