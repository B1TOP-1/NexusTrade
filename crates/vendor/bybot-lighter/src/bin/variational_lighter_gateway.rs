use std::{collections::HashMap, env, str::FromStr, time::Duration};

use anyhow::{Context, Result};
use bybot_lighter::{
    data::{parse_order_book_message, LighterMarketSpec},
    execution::LighterExecutionEffect,
    execution_client::{
        LighterCancelRequest, LighterExecutionClient, LighterExecutionConfig, LighterOrderRequest,
        LighterOrderType, LighterTimeInForce,
    },
    http::LighterHttpClient,
    local_book::{LighterDepthSide, LighterLocalBook},
    websocket::{LighterWebSocketClient, LighterWebSocketConfig, LighterWsEvent},
};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::{mpsc, watch},
    task::JoinHandle,
};

const OUTPUT_CAPACITY: usize = 4_096;
const DEFAULT_HTTP_URL: &str = "https://mainnet.zklighter.elliot.ai";
const DEFAULT_WS_URL: &str = "wss://mainnet.zklighter.elliot.ai/stream";
const DEFAULT_PUBLIC_WS_URL: &str = "wss://mainnet.zklighter.elliot.ai/stream?readonly=true";

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum GatewayCommand {
    SetMarket {
        id: String,
        symbol: String,
        depth_notional: String,
    },
    PlaceOrder {
        id: String,
        symbol: String,
        client_order_id: String,
        client_order_index: u64,
        signed_quantity: String,
        limit_price: String,
        #[serde(default)]
        reduce_only: bool,
    },
    CancelOrder {
        id: String,
        symbol: String,
        client_order_id: String,
        client_order_index: Option<u64>,
        order_index: u64,
    },
    GetAccountSnapshot {
        id: String,
    },
    Shutdown {
        id: String,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct MarketSelection {
    symbol: String,
    market_id: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let http_url = env::var("LIGHTER_HTTP_URL").unwrap_or_else(|_| DEFAULT_HTTP_URL.to_string());
    let public_ws_url =
        env::var("LIGHTER_PUBLIC_WS_URL").unwrap_or_else(|_| DEFAULT_PUBLIC_WS_URL.to_string());
    let execution_enabled = env_flag("LIGHTER_EXECUTION_ENABLED", true);
    let markets: HashMap<String, LighterMarketSpec> = LighterHttpClient::new(&http_url)?
        .market_specs()
        .await?
        .into_iter()
        .map(|market| (market.symbol.clone(), market))
        .collect();

    let (client, private_runtime) = if execution_enabled {
        let account_index = required_env("LIGHTER_ACCOUNT_INDEX")?.parse::<u64>()?;
        let api_key_index = required_env("LIGHTER_API_KEY_INDEX")?.parse::<u8>()?;
        let chain_id = env::var("LIGHTER_CHAIN_ID")
            .unwrap_or_else(|_| "304".to_string())
            .parse::<u32>()?;
        let private_key = required_env("LIGHTER_PRIVATE_KEY")?;
        let ws_url =
            env::var("LIGHTER_PRIVATE_WS_URL").unwrap_or_else(|_| DEFAULT_WS_URL.to_string());
        let config =
            LighterExecutionConfig::new(http_url, ws_url, account_index, api_key_index, chain_id)?;
        let client = LighterExecutionClient::connect(config, &private_key).await?;
        let runtime = client.spawn_private_runtime().await?;
        client.wait_account_snapshot().await?;
        (Some(client), Some(runtime))
    } else {
        (None, None)
    };

    let (output_tx, mut output_rx) = mpsc::channel::<Value>(OUTPUT_CAPACITY);
    let output_task = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(value) = output_rx.recv().await {
            let mut encoded = serde_json::to_vec(&value).unwrap_or_else(|_| b"{}".to_vec());
            encoded.push(b'\n');
            if stdout.write_all(&encoded).await.is_err() || stdout.flush().await.is_err() {
                break;
            }
        }
    });

    send_event(&output_tx, json!({"type":"health","ready":true})).await;

    let effect_task = private_runtime.as_ref().map(|runtime| {
        let mut effect_receiver = runtime.subscribe();
        let effect_output = output_tx.clone();
        tokio::spawn(async move {
            loop {
                match effect_receiver.recv().await {
                    Ok(effect) => send_event(&effect_output, effect_json(effect)).await,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                        send_event(
                            &effect_output,
                            json!({"type":"error","source":"private_ws","error":format!("lagged {count} execution events")}),
                        )
                        .await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    });

    let (market_tx, market_rx) = watch::channel(MarketSelection::default());
    let position_task = client
        .clone()
        .map(|client| spawn_position_publisher(client, market_rx, output_tx.clone()));
    let mut book_task: Option<JoinHandle<()>> = None;
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let command = match serde_json::from_str::<GatewayCommand>(&line) {
            Ok(command) => command,
            Err(error) => {
                send_event(
                    &output_tx,
                    json!({"type":"command_result","ok":false,"error":format!("invalid command: {error}")}),
                )
                .await;
                continue;
            }
        };
        match command {
            GatewayCommand::SetMarket {
                id,
                symbol,
                depth_notional,
            } => {
                let symbol = symbol.trim().to_uppercase();
                let market = markets
                    .get(&symbol)
                    .with_context(|| format!("unknown Lighter market: {symbol}"))?;
                let market_id = market.market_id;
                let notional = Decimal::from_str(&depth_notional)
                    .context("invalid set_market depth_notional")?;
                let position_quantity = if let Some(client) = client.as_ref() {
                    Some(
                        client
                            .position(&symbol)
                            .await?
                            .map(|position| position.signed_quantity)
                            .unwrap_or(Decimal::ZERO),
                    )
                } else {
                    None
                };
                if let Some(task) = book_task.take() {
                    task.abort();
                }
                market_tx.send_replace(MarketSelection {
                    symbol: symbol.clone(),
                    market_id,
                });
                if let Some(quantity) = position_quantity {
                    send_event(
                        &output_tx,
                        json!({"type":"position","symbol":symbol,"market_id":market_id,"quantity":quantity.to_string()}),
                    )
                    .await;
                }
                book_task = Some(spawn_public_book(
                    public_ws_url.clone(),
                    symbol.clone(),
                    market_id,
                    notional,
                    output_tx.clone(),
                ));
                send_result(
                    &output_tx,
                    &id,
                    true,
                    None,
                    json!({
                        "market_id":market_id,
                        "min_base_amount":market.min_base_amount.to_string(),
                        "size_multiplier":market.size_multiplier,
                        "price_multiplier":market.price_multiplier,
                        "position_quantity":position_quantity.map(|quantity| quantity.to_string()),
                    }),
                )
                .await;
            }
            GatewayCommand::PlaceOrder {
                id,
                symbol,
                client_order_id,
                client_order_index,
                signed_quantity,
                limit_price,
                reduce_only,
            } => {
                let result = async {
                    let client = client.as_ref().context("Lighter execution is disabled")?;
                    let receipt = client
                        .submit_order(&LighterOrderRequest {
                            symbol,
                            client_order_id,
                            client_order_index,
                            signed_quantity: Decimal::from_str(&signed_quantity)?,
                            limit_price: Some(Decimal::from_str(&limit_price)?),
                            order_type: LighterOrderType::Limit,
                            time_in_force: gateway_time_in_force(),
                            reduce_only,
                        })
                        .await?;
                    for effect in receipt.effects {
                        send_event(&output_tx, effect_json(effect)).await;
                    }
                    Ok::<Value, anyhow::Error>(json!({"tx_hash":receipt.ack.tx_hash}))
                }
                .await;
                match result {
                    Ok(data) => send_result(&output_tx, &id, true, None, data).await,
                    Err(error) => {
                        send_result(&output_tx, &id, false, Some(error.to_string()), json!({}))
                            .await
                    }
                }
            }
            GatewayCommand::CancelOrder {
                id,
                symbol,
                client_order_id,
                client_order_index,
                order_index,
            } => {
                let result = async {
                    let client = client.as_ref().context("Lighter execution is disabled")?;
                    client
                        .cancel_order(&LighterCancelRequest {
                            symbol,
                            client_order_id,
                            client_order_index,
                            order_index,
                        })
                        .await
                }
                .await;
                match result {
                    Ok(receipt) => {
                        for effect in receipt.effects {
                            send_event(&output_tx, effect_json(effect)).await;
                        }
                        send_result(
                            &output_tx,
                            &id,
                            true,
                            None,
                            json!({"tx_hash":receipt.ack.tx_hash}),
                        )
                        .await;
                    }
                    Err(error) => {
                        send_result(&output_tx, &id, false, Some(error.to_string()), json!({}))
                            .await
                    }
                }
            }
            GatewayCommand::GetAccountSnapshot { id } => {
                let result = async {
                    let client = client.as_ref().context("Lighter execution is disabled")?;
                    let snapshot = client.account_snapshot().await?;
                    Ok::<Value, anyhow::Error>(json!({
                        "collateral": snapshot.collateral.to_string(),
                        "available_balance": snapshot.available_balance.to_string(),
                    }))
                }
                .await;
                match result {
                    Ok(data) => send_result(&output_tx, &id, true, None, data).await,
                    Err(error) => {
                        send_result(&output_tx, &id, false, Some(error.to_string()), json!({}))
                            .await
                    }
                }
            }
            GatewayCommand::Shutdown { id } => {
                send_result(&output_tx, &id, true, None, json!({})).await;
                break;
            }
        }
    }

    if let Some(task) = book_task {
        task.abort();
    }
    if let Some(task) = position_task {
        task.abort();
    }
    if let Some(task) = effect_task {
        task.abort();
    }
    drop(private_runtime);
    drop(output_tx);
    let _ = output_task.await;
    Ok(())
}

const fn gateway_time_in_force() -> LighterTimeInForce {
    LighterTimeInForce::ImmediateOrCancel
}

fn spawn_public_book(
    ws_url: String,
    symbol: String,
    market_id: u64,
    depth_notional: Decimal,
    output: mpsc::Sender<Value>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            send_event(
                &output,
                json!({"type":"book","symbol":symbol,"market_id":market_id,"ready":false}),
            )
            .await;
            let result =
                run_public_book_connection(&ws_url, &symbol, market_id, depth_notional, &output)
                    .await;
            if let Err(error) = result {
                send_event(
                    &output,
                    json!({"type":"error","source":"public_ws","error":format!("{error:#}")}),
                )
                .await;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
}

async fn run_public_book_connection(
    ws_url: &str,
    symbol: &str,
    market_id: u64,
    depth_notional: Decimal,
    output: &mpsc::Sender<Value>,
) -> Result<()> {
    let websocket = LighterWebSocketClient::new(LighterWebSocketConfig::new(ws_url)?);
    let mut connection = websocket.connect().await?;
    connection
        .subscribe_public(&format!("order_book/{market_id}"))
        .await?;
    let mut heartbeat = tokio::time::interval(connection.heartbeat_interval());
    heartbeat.tick().await;
    let mut publish = tokio::time::interval(Duration::from_millis(50));
    publish.tick().await;
    let mut book = LighterLocalBook::new();
    let mut dirty = false;
    loop {
        tokio::select! {
            _ = heartbeat.tick() => connection.send_ping().await?,
            _ = publish.tick() => {
                if !dirty {
                    continue;
                }
                let Some(top) = book.top_of_book() else { continue; };
                send_event(output, json!({
                    "type":"book",
                    "symbol":symbol,
                    "market_id":market_id,
                    "ready":true,
                    "nonce":book.nonce(),
                    "bid":top.bid_price.to_string(),
                    "ask":top.ask_price.to_string(),
                    "vwap_bid":book.vwap_for_quote_notional(LighterDepthSide::Bid, depth_notional).map(|value| value.to_string()),
                    "vwap_ask":book.vwap_for_quote_notional(LighterDepthSide::Ask, depth_notional).map(|value| value.to_string()),
                })).await;
                dirty = false;
            }
            event = connection.next_event() => match event? {
                LighterWsEvent::Text(payload) => {
                    let Some(message) = parse_order_book_message(&payload)? else { continue; };
                    let outcome = book.apply(&message)?;
                    if outcome.requires_resubscribe {
                        anyhow::bail!("Lighter order-book nonce gap");
                    }
                    dirty |= outcome.applied;
                }
                LighterWsEvent::Closed => anyhow::bail!("Lighter public websocket closed"),
                LighterWsEvent::Reconnected => {}
            }
        }
    }
}

fn spawn_position_publisher(
    client: LighterExecutionClient,
    mut market: watch::Receiver<MarketSelection>,
    output: mpsc::Sender<Value>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut last: Option<(String, Decimal)> = None;
        loop {
            let selected = market.borrow_and_update().clone();
            if !selected.symbol.is_empty() {
                if let Ok(position) = client.position(&selected.symbol).await {
                    let quantity = position
                        .map(|position| position.signed_quantity)
                        .unwrap_or(Decimal::ZERO);
                    let current = (selected.symbol.clone(), quantity);
                    if last.as_ref() != Some(&current) {
                        send_event(
                            &output,
                            json!({"type":"position","symbol":selected.symbol,"market_id":selected.market_id,"quantity":quantity.to_string()}),
                        )
                        .await;
                        last = Some(current);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
}

fn effect_json(effect: LighterExecutionEffect) -> Value {
    match effect {
        LighterExecutionEffect::Submitted {
            client_order_id,
            client_order_index,
            tx_hash,
            ts_event_ms,
        } => {
            json!({"type":"execution","kind":"submitted","client_order_id":client_order_id,"client_order_index":client_order_index,"tx_hash":tx_hash,"ts_event_ms":ts_event_ms})
        }
        LighterExecutionEffect::Accepted {
            client_order_id,
            client_order_index,
            order_index,
            ts_event_ms,
        } => {
            json!({"type":"execution","kind":"accepted","client_order_id":client_order_id,"client_order_index":client_order_index,"order_index":order_index,"ts_event_ms":ts_event_ms})
        }
        LighterExecutionEffect::Fill {
            client_order_id,
            client_order_index,
            trade_id,
            quantity,
            price,
            fee,
            synthetic,
            ts_event_ms,
        } => {
            json!({"type":"execution","kind":"fill","client_order_id":client_order_id,"client_order_index":client_order_index,"trade_id":trade_id,"quantity":quantity,"price":price,"fee":fee,"synthetic":synthetic,"ts_event_ms":ts_event_ms})
        }
        LighterExecutionEffect::ExternalTrade { trade } => {
            json!({"type":"execution","kind":"external_trade","trade_id":trade.trade_id,"market_id":trade.market_id,"ts_event_ms":trade.ts_event_ms})
        }
        LighterExecutionEffect::Position { position } => {
            json!({"type":"execution","kind":"position","market_id":position.market_id,"quantity":position.signed_quantity,"average_price":position.average_price})
        }
        LighterExecutionEffect::Canceled {
            client_order_id,
            client_order_index,
            reason,
            ts_event_ms,
        } => {
            json!({"type":"execution","kind":"canceled","client_order_id":client_order_id,"client_order_index":client_order_index,"reason":reason,"ts_event_ms":ts_event_ms})
        }
        LighterExecutionEffect::Rejected {
            client_order_id,
            client_order_index,
            reason,
            ts_event_ms,
        } => {
            json!({"type":"execution","kind":"rejected","client_order_id":client_order_id,"client_order_index":client_order_index,"reason":reason,"ts_event_ms":ts_event_ms})
        }
        LighterExecutionEffect::Funding { funding } => {
            json!({"type":"execution","kind":"funding","market_id":funding.market_id,"funding_id":funding.funding_id,"change":funding.change,"timestamp_ms":funding.timestamp_ms})
        }
    }
}

async fn send_result(
    output: &mpsc::Sender<Value>,
    id: &str,
    ok: bool,
    error: Option<String>,
    data: Value,
) {
    send_event(
        output,
        json!({"type":"command_result","id":id,"ok":ok,"error":error,"data":data}),
    )
    .await;
}

async fn send_event(output: &mpsc::Sender<Value>, event: Value) {
    let _ = output.send(event).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variational_hedges_are_immediate_or_cancel() {
        assert_eq!(
            gateway_time_in_force(),
            LighterTimeInForce::ImmediateOrCancel
        );
    }

    #[test]
    fn account_snapshot_command_deserializes() {
        let command: GatewayCommand =
            serde_json::from_str(r#"{"type":"get_account_snapshot","id":"py-1"}"#)
                .expect("account snapshot command should deserialize");
        assert!(matches!(
            command,
            GatewayCommand::GetAccountSnapshot { id } if id == "py-1"
        ));
    }
}

fn required_env(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("missing required environment variable {name}"))
}

fn env_flag(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .map(|value| {
            !matches!(
                value.trim().to_lowercase().as_str(),
                "0" | "false" | "no" | "off"
            )
        })
        .unwrap_or(default)
}
