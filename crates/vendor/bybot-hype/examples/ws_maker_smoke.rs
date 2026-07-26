use std::{env, str::FromStr, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use bybot_hype::{
    account::resolve_execution_account,
    markets::MarketCatalog,
    orders::OrderSide,
    positions::PositionService,
    user_stream::{UserStreamConfig, UserStreamRuntime},
    ws_post::WsPostGateway,
};
use hypersdk::{
    hypercore::{
        self,
        types::{
            api::{Action, OkResponse, Response},
            BatchCancel, BatchOrder, Cancel, OrderGrouping, OrderRequest, OrderResponseStatus,
            OrderStatus, OrderTypePlacement, TimeInForce,
        },
        Chain, Cloid, NonceHandler, PrivateKeySigner,
    },
    Address,
};
use rust_decimal::Decimal;
use tokio::time::Instant;

const SYMBOL: &str = "BTC";
const TEST_NOTIONAL_USD: Decimal = Decimal::from_parts(11, 0, 0, false, 0);

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    require_live_confirmation()?;
    let signer = load_signer("HYPE_PRIVATE_KEY")?;
    let client = hypercore::mainnet();
    let vault = load_optional_address("HYPE_VAULT_ADDRESS")?;
    let account = resolve_execution_account(&client, &signer, vault).await?;
    let market = MarketCatalog::load_selected(&client, &[])
        .await?
        .get(SYMBOL)
        .cloned()
        .ok_or_else(|| anyhow!("BTC market missing"))?;
    PositionService::new(hypercore::mainnet(), account.user())
        .ensure_flat(None, &[SYMBOL])
        .await?;

    let mut user_stream =
        UserStreamRuntime::spawn(account.user(), UserStreamConfig::new([SYMBOL], [None])?);
    user_stream.wait_connected().await?;
    let book = user_stream.wait_book(SYMBOL).await?;
    let fifth_bid_raw = book
        .bids()
        .get(4)
        .ok_or_else(|| anyhow!("BTC fifth bid missing"))?
        .price();
    let fifth_bid = Decimal::from_i128_with_scale(i128::from(fifth_bid_raw), 8);
    let price = market.maker_price(fifth_bid, OrderSide::Buy)?;
    let size = market.precision().minimum_size(price, TEST_NOTIONAL_USD)?;
    let cloid = Cloid::random();
    let nonce = NonceHandler::default();

    let connect_started = Instant::now();
    let mut gateway = WsPostGateway::connect_mainnet(Duration::from_secs(8)).await?;
    println!(
        "[WsMaker] connected_us={} price={} size={} level=bid5 cloid={cloid}",
        connect_started.elapsed().as_micros(),
        price,
        size
    );

    let sign_started = Instant::now();
    let request = Action::Order(BatchOrder {
        orders: vec![OrderRequest {
            asset: market.market().index,
            is_buy: true,
            limit_px: price,
            sz: size,
            reduce_only: false,
            order_type: OrderTypePlacement::Limit {
                tif: TimeInForce::Alo,
            },
            cloid,
        }],
        grouping: OrderGrouping::Na,
        builder: None,
    })
    .sign_sync(
        &signer,
        nonce.next(),
        account.vault_address(),
        None,
        Chain::Mainnet,
    )?;
    let signed_at = Instant::now();
    let post_started = Instant::now();
    let response = gateway.post_action(request).await?;
    let post_ack_at = Instant::now();
    let oid = resting_oid(response)?;
    let open_at = user_stream
        .wait_order_status(cloid, oid, |status| matches!(status, OrderStatus::Open))
        .await?;
    println!(
        "[WsMaker] place sign_us={} post_ack_us={} order_open_us={} oid={oid}",
        signed_at.duration_since(sign_started).as_micros(),
        post_ack_at.duration_since(post_started).as_micros(),
        open_at.duration_since(post_started).as_micros()
    );

    let cancel_sign_started = Instant::now();
    let cancel_request = Action::Cancel(BatchCancel {
        cancels: vec![Cancel {
            asset: market.market().index,
            oid,
        }],
    })
    .sign_sync(
        &signer,
        nonce.next(),
        account.vault_address(),
        None,
        Chain::Mainnet,
    )?;
    let cancel_signed_at = Instant::now();
    let cancel_post_started = Instant::now();
    let cancel_response = gateway.post_action(cancel_request).await?;
    let cancel_ack_at = Instant::now();
    require_cancel_success(cancel_response)?;
    let canceled_at = user_stream
        .wait_order_status(cloid, oid, |status| matches!(status, OrderStatus::Canceled))
        .await?;
    println!(
        "[WsMaker] cancel sign_us={} post_ack_us={} order_canceled_us={}",
        cancel_signed_at
            .duration_since(cancel_sign_started)
            .as_micros(),
        cancel_ack_at
            .duration_since(cancel_post_started)
            .as_micros(),
        canceled_at.duration_since(cancel_post_started).as_micros()
    );

    PositionService::new(hypercore::mainnet(), account.user())
        .ensure_flat(None, &[SYMBOL])
        .await?;
    println!("[WsMaker] SUCCESS BTC maker order rested, canceled, account flat");
    Ok(())
}

fn resting_oid(response: Response) -> Result<u64> {
    match response {
        Response::Ok(OkResponse::Order { statuses }) => match statuses.first() {
            Some(OrderResponseStatus::Resting { oid, .. }) => Ok(*oid),
            other => bail!("maker order did not rest: {other:?}"),
        },
        other => bail!("unexpected maker response: {other:?}"),
    }
}

fn require_cancel_success(response: Response) -> Result<()> {
    match response {
        Response::Ok(OkResponse::Cancel { statuses })
            if matches!(statuses.first(), Some(OrderResponseStatus::Success)) =>
        {
            Ok(())
        }
        other => bail!("unexpected cancel response: {other:?}"),
    }
}

fn require_live_confirmation() -> Result<()> {
    if env::args().any(|argument| argument == "--confirm-live") {
        Ok(())
    } else {
        bail!("live test requires --confirm-live")
    }
}

fn load_signer(variable: &str) -> Result<PrivateKeySigner> {
    let value = env::var(variable).with_context(|| format!("missing {variable}"))?;
    PrivateKeySigner::from_str(value.trim()).context("invalid Hyperliquid private key")
}

fn load_optional_address(variable: &str) -> Result<Option<Address>> {
    env::var(variable)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| Address::from_str(value.trim()).context("invalid vault address"))
        .transpose()
}
