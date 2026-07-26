use std::{collections::BTreeSet, fmt, str::FromStr};

use anyhow::{anyhow, bail, Context, Result};
use hypersdk::{
    hypercore::{
        self,
        types::{OrderResponseStatus, OrderStatus},
        Cloid, OidOrCloid, PrivateKeySigner,
    },
    Address,
};
use rust_decimal::Decimal;

use crate::{
    account::resolve_execution_account,
    execution::ExecutionService,
    markets::MarketCatalog,
    orders::{OrderIntent, OrderSide},
    positions::PositionDetails,
    user_stream::{UserStreamConfig, UserStreamEvent, UserStreamRuntime},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewaySubmission {
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub filled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayOrderRejected(pub String);

impl fmt::Display for GatewayOrderRejected {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Hyperliquid order rejected: {}", self.0)
    }
}

impl std::error::Error for GatewayOrderRejected {}

impl GatewaySubmission {
    pub fn from_response(cloid: Cloid, statuses: &[OrderResponseStatus]) -> Result<Self> {
        let client_order_id = format!("{cloid:#x}");
        match statuses.first() {
            Some(OrderResponseStatus::Resting { oid, .. }) => Ok(Self {
                client_order_id,
                exchange_order_id: Some(oid.to_string()),
                filled: false,
            }),
            Some(OrderResponseStatus::Filled { oid, .. }) => Ok(Self {
                client_order_id,
                exchange_order_id: Some(oid.to_string()),
                filled: true,
            }),
            Some(OrderResponseStatus::Error(reason)) => {
                Err(GatewayOrderRejected(reason.clone()).into())
            }
            Some(
                OrderResponseStatus::Success
                | OrderResponseStatus::WaitingForTrigger
                | OrderResponseStatus::WaitingForFill,
            ) => Ok(Self {
                client_order_id,
                exchange_order_id: None,
                filled: false,
            }),
            None => Err(anyhow!("Hyperliquid returned no order status")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayOrderStatus {
    Unknown,
    Acknowledged,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GatewayPrivateEvent {
    Connected,
    Disconnected,
    Order {
        client_order_id: Option<String>,
        exchange_order_id: String,
        status: GatewayOrderStatus,
        original_quantity: Decimal,
        remaining_quantity: Decimal,
        occurred_at_ms: u64,
    },
    Fill {
        client_order_id: Option<String>,
        exchange_order_id: String,
        trade_id: String,
        quantity: Decimal,
        price: Decimal,
        fee: Decimal,
        occurred_at_ms: u64,
    },
    Position {
        symbol: String,
        signed_quantity: Decimal,
    },
    RuntimeError {
        message: String,
    },
}

#[must_use]
pub fn normalize_user_stream_event(event: &UserStreamEvent) -> Option<GatewayPrivateEvent> {
    match event {
        UserStreamEvent::Connected { .. } => Some(GatewayPrivateEvent::Connected),
        UserStreamEvent::Disconnected { .. } => Some(GatewayPrivateEvent::Disconnected),
        UserStreamEvent::Order { update, .. } => {
            let status = if matches!(update.status, OrderStatus::Open | OrderStatus::Triggered)
                && update.order.sz < update.order.orig_sz
            {
                GatewayOrderStatus::PartiallyFilled
            } else {
                normalize_order_status(update.status)
            };
            Some(GatewayPrivateEvent::Order {
                client_order_id: update.order.cloid.map(|value| format!("{value:#x}")),
                exchange_order_id: update.order.oid.to_string(),
                status,
                original_quantity: update.order.orig_sz,
                remaining_quantity: update.order.sz,
                occurred_at_ms: update.status_timestamp,
            })
        }
        UserStreamEvent::Fill { fill, .. } => Some(GatewayPrivateEvent::Fill {
            client_order_id: fill.cloid.map(|value| format!("{value:#x}")),
            exchange_order_id: fill.oid.to_string(),
            trade_id: fill.tid.to_string(),
            quantity: fill.sz,
            price: fill.px,
            fee: fill.fee,
            occurred_at_ms: fill.time,
        }),
        UserStreamEvent::Position { coin, size, .. } => Some(GatewayPrivateEvent::Position {
            symbol: coin.clone(),
            signed_quantity: *size,
        }),
        UserStreamEvent::RuntimeError { message, .. } => Some(GatewayPrivateEvent::RuntimeError {
            message: message.clone(),
        }),
        UserStreamEvent::Book { .. }
        | UserStreamEvent::Funding { .. }
        | UserStreamEvent::UserEvent { .. }
        | UserStreamEvent::LedgerUpdate { .. } => None,
    }
}

pub struct HypeGateway {
    execution: ExecutionService,
    markets: MarketCatalog,
}

impl HypeGateway {
    pub async fn connect_mainnet(
        private_key: &str,
        vault_address: Option<&str>,
        symbols: &[String],
    ) -> Result<Self> {
        let signer = PrivateKeySigner::from_str(private_key.trim())
            .context("invalid Hyperliquid private key")?;
        let requested_vault = vault_address
            .map(|value| Address::from_str(value.trim()).context("invalid Hyperliquid vault"))
            .transpose()?;
        let client = hypercore::mainnet();
        let account = resolve_execution_account(&client, &signer, requested_vault).await?;
        let dex_names = symbols
            .iter()
            .filter_map(|symbol| symbol.split_once(':').map(|(dex, _)| dex.to_lowercase()))
            .collect::<BTreeSet<_>>();
        let dex_refs = dex_names.iter().map(String::as_str).collect::<Vec<_>>();
        let markets = MarketCatalog::load_selected(&client, &dex_refs).await?;
        Ok(Self {
            execution: ExecutionService::mainnet(signer, account),
            markets,
        })
    }

    pub fn spawn_user_stream(&self, symbols: &[String]) -> Result<UserStreamRuntime> {
        let position_dexes = symbols
            .iter()
            .map(|symbol| symbol.split_once(':').map(|(dex, _)| dex.to_lowercase()))
            .collect::<BTreeSet<_>>();
        let config = UserStreamConfig::new(symbols.iter().cloned(), position_dexes)?;
        Ok(UserStreamRuntime::spawn(self.execution.user(), config))
    }

    pub async fn place_ioc(
        &self,
        symbol: &str,
        client_order_id: &str,
        signed_quantity: Decimal,
        limit_price: Decimal,
        reduce_only: bool,
    ) -> Result<GatewaySubmission> {
        let market = self.market(symbol)?;
        let limit_price =
            market.aggressive_price(limit_price, signed_quantity.is_sign_positive(), 0)?;
        let intent = build_ioc_intent(
            market.symbol(),
            market.market().index,
            signed_quantity,
            limit_price,
            reduce_only,
        )?;
        let cloid = parse_client_order_id(client_order_id)?;
        let submitted = self.execution.submit_with_cloid(&intent, cloid).await?;
        GatewaySubmission::from_response(cloid, &submitted.statuses)
    }

    pub async fn cancel_by_client_order_id(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<GatewaySubmission> {
        let market = self.market(symbol)?;
        let asset = u32::try_from(market.market().index).context("Hype asset index overflow")?;
        let cloid = parse_client_order_id(client_order_id)?;
        let statuses = self.execution.cancel_by_cloid(asset, cloid).await?;
        match statuses.first() {
            Some(OrderResponseStatus::Success) => Ok(GatewaySubmission {
                client_order_id: format!("{cloid:#x}"),
                exchange_order_id: None,
                filled: false,
            }),
            Some(OrderResponseStatus::Error(reason)) => {
                Err(anyhow!("Hyperliquid cancel rejected: {reason}"))
            }
            Some(status) => Err(anyhow!("unexpected Hyperliquid cancel status: {status:?}")),
            None => Err(anyhow!("Hyperliquid returned no cancel status")),
        }
    }

    pub async fn order_status(&self, client_order_id: &str) -> Result<GatewayOrderStatus> {
        let cloid = parse_client_order_id(client_order_id)?;
        let Some(update) = self
            .execution
            .positions()
            .order_status(OidOrCloid::Right(cloid))
            .await?
        else {
            return Ok(GatewayOrderStatus::Unknown);
        };
        Ok(normalize_order_status(update.status))
    }

    pub async fn position(&self, symbol: &str) -> Result<PositionDetails> {
        let market = self.market(symbol)?;
        let aliases = [market.symbol(), symbol];
        self.execution
            .positions()
            .position_details(market.dex(), &aliases)
            .await
    }

    fn market(&self, symbol: &str) -> Result<&crate::markets::MarketDescriptor> {
        self.markets
            .get(symbol)
            .ok_or_else(|| anyhow!("Hyperliquid market not loaded: {symbol}"))
    }
}

pub fn parse_client_order_id(value: &str) -> Result<Cloid> {
    Cloid::from_str(value.trim()).context("invalid Hyperliquid client order id")
}

pub fn build_ioc_intent(
    symbol: &str,
    asset: usize,
    signed_quantity: Decimal,
    limit_price: Decimal,
    reduce_only: bool,
) -> Result<OrderIntent> {
    if signed_quantity.is_zero() {
        bail!("Hyperliquid order quantity cannot be zero");
    }
    if limit_price <= Decimal::ZERO {
        bail!("Hyperliquid limit price must be positive");
    }
    let side = if signed_quantity.is_sign_positive() {
        OrderSide::Buy
    } else {
        OrderSide::Sell
    };
    Ok(OrderIntent::aggressive_ioc(
        symbol,
        asset,
        side,
        limit_price,
        signed_quantity.abs(),
        reduce_only,
    ))
}

fn normalize_order_status(status: OrderStatus) -> GatewayOrderStatus {
    match status {
        OrderStatus::Open | OrderStatus::Triggered => GatewayOrderStatus::Acknowledged,
        OrderStatus::Filled => GatewayOrderStatus::Filled,
        OrderStatus::Canceled
        | OrderStatus::MarginCanceled
        | OrderStatus::VaultWithdrawalCanceled
        | OrderStatus::OpenInterestCapCanceled
        | OrderStatus::SelfTradeCanceled
        | OrderStatus::ReduceOnlyCanceled
        | OrderStatus::SiblingFilledCanceled
        | OrderStatus::DelistedCanceled
        | OrderStatus::LiquidatedCanceled
        | OrderStatus::ScheduledCancel => GatewayOrderStatus::Canceled,
        _ => GatewayOrderStatus::Rejected,
    }
}
