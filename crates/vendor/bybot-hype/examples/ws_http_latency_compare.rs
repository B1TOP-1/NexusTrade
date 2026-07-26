use std::{env, str::FromStr, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use bybot_hype::{
    account::{resolve_execution_account, ExecutionAccount},
    markets::{MarketCatalog, MarketDescriptor},
    orders::OrderSide,
    positions::PositionService,
    user_stream::{UserStreamConfig, UserStreamRuntime},
    ws_post::WsPostGateway,
};
use hypersdk::{
    hypercore::{
        self,
        types::{
            api::{Action, ActionRequest, OkResponse, Response},
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
const ROUNDS_PER_TRANSPORT: usize = 5;

#[derive(Debug, Clone, Copy)]
enum Transport {
    Http,
    WebSocket,
}

impl Transport {
    const fn label(self) -> &'static str {
        match self {
            Self::Http => "HTTP",
            Self::WebSocket => "WS",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    sign_us: u128,
    place_ack_us: u128,
    open_us: u128,
    cancel_sign_us: u128,
    cancel_ack_us: u128,
    canceled_us: u128,
    cycle_us: u128,
}

struct RoundRunner<'a> {
    http: &'a hypercore::HttpClient,
    post_ws: &'a mut WsPostGateway,
    signer: &'a PrivateKeySigner,
    account: ExecutionAccount,
    market: &'a MarketDescriptor,
    nonce: &'a NonceHandler,
    user_stream: &'a mut UserStreamRuntime,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    require_live_confirmation()?;
    let signer = load_signer("HYPE_PRIVATE_KEY")?;
    let http = hypercore::mainnet();
    let vault = load_optional_address("HYPE_VAULT_ADDRESS")?;
    let account = resolve_execution_account(&http, &signer, vault).await?;
    let market = MarketCatalog::load_selected(&http, &[])
        .await?
        .get(SYMBOL)
        .cloned()
        .ok_or_else(|| anyhow!("BTC market missing"))?;
    let positions = PositionService::new(hypercore::mainnet(), account.user());
    positions.ensure_flat(None, &[SYMBOL]).await?;

    let mut user_stream =
        UserStreamRuntime::spawn(account.user(), UserStreamConfig::new([SYMBOL], [None])?);
    let user_ws_started = Instant::now();
    user_stream.wait_connected().await?;
    let user_ws_connect_us = user_ws_started.elapsed().as_micros();
    user_stream.wait_book(SYMBOL).await?;

    let post_ws_started = Instant::now();
    let mut post_ws = WsPostGateway::connect_mainnet(Duration::from_secs(8)).await?;
    let post_ws_connect_us = post_ws_started.elapsed().as_micros();
    println!(
        "[Compare] ready user_ws_connect_us={user_ws_connect_us} post_ws_connect_us={post_ws_connect_us} rounds_each={ROUNDS_PER_TRANSPORT}"
    );

    let nonce = NonceHandler::default();
    let schedule = [
        Transport::Http,
        Transport::WebSocket,
        Transport::WebSocket,
        Transport::Http,
        Transport::Http,
        Transport::WebSocket,
        Transport::WebSocket,
        Transport::Http,
        Transport::Http,
        Transport::WebSocket,
    ];
    let mut http_samples = Vec::with_capacity(ROUNDS_PER_TRANSPORT);
    let mut ws_samples = Vec::with_capacity(ROUNDS_PER_TRANSPORT);

    for (index, transport) in schedule.into_iter().enumerate() {
        let mut runner = RoundRunner {
            http: &http,
            post_ws: &mut post_ws,
            signer: &signer,
            account,
            market: &market,
            nonce: &nonce,
            user_stream: &mut user_stream,
        };
        let sample = runner
            .run(transport)
            .await
            .with_context(|| format!("{} round {} failed", transport.label(), index + 1))?;
        print_sample(index + 1, transport, sample);
        match transport {
            Transport::Http => http_samples.push(sample),
            Transport::WebSocket => ws_samples.push(sample),
        }
        positions.ensure_flat(None, &[SYMBOL]).await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    print_summary("HTTP", &http_samples);
    print_summary("WS", &ws_samples);
    print_comparison(&http_samples, &ws_samples);
    println!("[Compare] SUCCESS all orders canceled and account flat");
    Ok(())
}

impl RoundRunner<'_> {
    async fn run(&mut self, transport: Transport) -> Result<Sample> {
        let cycle_started = Instant::now();
        let book = self.user_stream.wait_book(SYMBOL).await?;
        let fifth_bid_raw = book
            .bids()
            .get(4)
            .ok_or_else(|| anyhow!("BTC fifth bid missing"))?
            .price();
        let fifth_bid = Decimal::from_i128_with_scale(i128::from(fifth_bid_raw), 8);
        let price = self.market.maker_price(fifth_bid, OrderSide::Buy)?;
        let size = self
            .market
            .precision()
            .minimum_size(price, TEST_NOTIONAL_USD)?;
        let cloid = Cloid::random();

        let sign_started = Instant::now();
        let order_request = signed_order(
            self.signer,
            self.account,
            self.market,
            self.nonce.next(),
            cloid,
            price,
            size,
        )?;
        let sign_us = sign_started.elapsed().as_micros();
        let place_started = Instant::now();
        let order_response = send(transport, self.http, self.post_ws, order_request).await?;
        let place_ack_us = place_started.elapsed().as_micros();
        let oid = resting_oid(order_response)?;
        let open_at = self
            .user_stream
            .wait_order_status(cloid, oid, |status| matches!(status, OrderStatus::Open))
            .await?;
        let open_us = open_at.duration_since(place_started).as_micros();

        let cancel_sign_started = Instant::now();
        let cancel_request = signed_cancel(
            self.signer,
            self.account,
            self.market,
            self.nonce.next(),
            oid,
        )?;
        let cancel_sign_us = cancel_sign_started.elapsed().as_micros();
        let cancel_started = Instant::now();
        let cancel_response = send(transport, self.http, self.post_ws, cancel_request).await?;
        let cancel_ack_us = cancel_started.elapsed().as_micros();
        require_cancel_success(cancel_response)?;
        let canceled_at = self
            .user_stream
            .wait_order_status(cloid, oid, |status| matches!(status, OrderStatus::Canceled))
            .await?;
        let canceled_us = canceled_at.duration_since(cancel_started).as_micros();

        Ok(Sample {
            sign_us,
            place_ack_us,
            open_us,
            cancel_sign_us,
            cancel_ack_us,
            canceled_us,
            cycle_us: cycle_started.elapsed().as_micros(),
        })
    }
}

async fn send(
    transport: Transport,
    http: &hypercore::HttpClient,
    post_ws: &mut WsPostGateway,
    request: ActionRequest,
) -> Result<Response> {
    match transport {
        Transport::Http => http.send(request).await,
        Transport::WebSocket => post_ws.post_action(request).await,
    }
}

fn signed_order(
    signer: &PrivateKeySigner,
    account: ExecutionAccount,
    market: &MarketDescriptor,
    nonce: u64,
    cloid: Cloid,
    price: Decimal,
    size: Decimal,
) -> Result<ActionRequest> {
    Action::Order(BatchOrder {
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
    .sign_sync(signer, nonce, account.vault_address(), None, Chain::Mainnet)
}

fn signed_cancel(
    signer: &PrivateKeySigner,
    account: ExecutionAccount,
    market: &MarketDescriptor,
    nonce: u64,
    oid: u64,
) -> Result<ActionRequest> {
    Action::Cancel(BatchCancel {
        cancels: vec![Cancel {
            asset: market.market().index,
            oid,
        }],
    })
    .sign_sync(signer, nonce, account.vault_address(), None, Chain::Mainnet)
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

fn print_sample(round: usize, transport: Transport, sample: Sample) {
    println!(
        "[Compare] round={round} transport={} sign_us={} place_ack_us={} open_us={} cancel_sign_us={} cancel_ack_us={} canceled_us={} cycle_us={}",
        transport.label(),
        sample.sign_us,
        sample.place_ack_us,
        sample.open_us,
        sample.cancel_sign_us,
        sample.cancel_ack_us,
        sample.canceled_us,
        sample.cycle_us
    );
}

fn print_summary(label: &str, samples: &[Sample]) {
    println!(
        "[CompareSummary] transport={label} place_ack_p50_us={} place_ack_p90_us={} open_p50_us={} open_p90_us={} cancel_ack_p50_us={} cancel_ack_p90_us={} canceled_p50_us={} canceled_p90_us={} cycle_p50_us={} cycle_p90_us={}",
        percentile(samples, |sample| sample.place_ack_us, 50),
        percentile(samples, |sample| sample.place_ack_us, 90),
        percentile(samples, |sample| sample.open_us, 50),
        percentile(samples, |sample| sample.open_us, 90),
        percentile(samples, |sample| sample.cancel_ack_us, 50),
        percentile(samples, |sample| sample.cancel_ack_us, 90),
        percentile(samples, |sample| sample.canceled_us, 50),
        percentile(samples, |sample| sample.canceled_us, 90),
        percentile(samples, |sample| sample.cycle_us, 50),
        percentile(samples, |sample| sample.cycle_us, 90)
    );
}

fn print_comparison(http: &[Sample], ws: &[Sample]) {
    let http_place = percentile(http, |sample| sample.place_ack_us, 50);
    let ws_place = percentile(ws, |sample| sample.place_ack_us, 50);
    let http_open = percentile(http, |sample| sample.open_us, 50);
    let ws_open = percentile(ws, |sample| sample.open_us, 50);
    println!(
        "[CompareResult] place_ack_p50_delta_us={} open_p50_delta_us={} ws_place_faster={} ws_open_faster={}",
        signed_delta(http_place, ws_place),
        signed_delta(http_open, ws_open),
        ws_place < http_place,
        ws_open < http_open
    );
}

fn percentile(samples: &[Sample], select: fn(&Sample) -> u128, percentile: usize) -> u128 {
    let mut values: Vec<u128> = samples.iter().map(select).collect();
    values.sort_unstable();
    let index = ((values.len() - 1) * percentile).div_ceil(100);
    values[index]
}

fn signed_delta(http: u128, ws: u128) -> i128 {
    i128::try_from(http).unwrap_or(i128::MAX) - i128::try_from(ws).unwrap_or(i128::MAX)
}

fn require_live_confirmation() -> Result<()> {
    if env::args().any(|argument| argument == "--confirm-live") {
        Ok(())
    } else {
        bail!("live comparison requires --confirm-live")
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
