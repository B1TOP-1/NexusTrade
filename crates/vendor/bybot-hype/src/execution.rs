use anyhow::{anyhow, Result};
use hypersdk::{
    hypercore::{self, types::OrderResponseStatus, Cloid, PrivateKeySigner},
    Address,
};
use rust_decimal::Decimal;

use crate::{
    account::ExecutionAccount,
    markets::MarketDescriptor,
    order_gateway::{OrderGateway, SubmittedOrder},
    orders::{OrderIntent, OrderSide},
    positions::PositionService,
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RecoveryPlan {
    side: OrderSide,
    size: Decimal,
}

impl RecoveryPlan {
    #[must_use]
    pub fn from_position(position_size: Decimal) -> Option<Self> {
        if position_size.is_zero() {
            return None;
        }
        Some(Self {
            side: if position_size.is_sign_negative() {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
            size: position_size.abs(),
        })
    }

    #[must_use]
    pub fn side(self) -> OrderSide {
        self.side
    }

    #[must_use]
    pub fn size(self) -> Decimal {
        self.size
    }
}

pub struct ExecutionService {
    gateway: OrderGateway,
    positions: PositionService,
    market_client: hypercore::HttpClient,
    user: Address,
}

impl ExecutionService {
    #[must_use]
    pub fn mainnet(signer: PrivateKeySigner, account: ExecutionAccount) -> Self {
        Self {
            gateway: OrderGateway::new(hypercore::mainnet(), signer, account.vault_address()),
            positions: PositionService::new(hypercore::mainnet(), account.user()),
            market_client: hypercore::mainnet(),
            user: account.user(),
        }
    }

    #[must_use]
    pub fn user(&self) -> Address {
        self.user
    }

    #[must_use]
    pub fn positions(&self) -> &PositionService {
        &self.positions
    }

    pub async fn submit(&self, intent: &OrderIntent) -> Result<SubmittedOrder> {
        self.gateway.submit(intent).await
    }

    pub async fn submit_with_cloid(
        &self,
        intent: &OrderIntent,
        cloid: Cloid,
    ) -> Result<SubmittedOrder> {
        self.gateway.submit_with_cloid(intent, cloid).await
    }

    pub async fn cancel(&self, asset: usize, oid: u64) -> Result<Vec<OrderResponseStatus>> {
        self.gateway.cancel(asset, oid).await
    }

    pub async fn cancel_by_cloid(
        &self,
        asset: u32,
        cloid: Cloid,
    ) -> Result<Vec<OrderResponseStatus>> {
        self.gateway.cancel_by_cloid(asset, cloid).await
    }

    pub async fn emergency_flatten(
        &self,
        market: &MarketDescriptor,
        symbols: &[&str],
        slippage_bps: i64,
    ) -> Result<Option<SubmittedOrder>> {
        let position_size = self.positions.position_size(market.dex(), symbols).await?;
        let Some(plan) = RecoveryPlan::from_position(position_size) else {
            return Ok(None);
        };
        let mids = self
            .market_client
            .all_mids(market.dex().map(str::to_string))
            .await?;
        let reference = symbols
            .iter()
            .find_map(|symbol| mids.get(*symbol))
            .copied()
            .ok_or_else(|| anyhow!("mid price missing for {}", market.symbol()))?;
        let is_buy = plan.side() == OrderSide::Buy;
        let limit_price = market.aggressive_price(reference, is_buy, slippage_bps)?;
        let intent = OrderIntent::aggressive_ioc(
            market.symbol(),
            market.market().index,
            plan.side(),
            limit_price,
            plan.size(),
            true,
        );
        let submitted = self.submit(&intent).await?;
        match submitted.statuses.first() {
            Some(OrderResponseStatus::Filled { .. }) => Ok(Some(submitted)),
            Some(OrderResponseStatus::Error(error)) => {
                Err(anyhow!("emergency flatten rejected: {error}"))
            }
            Some(status) => Err(anyhow!(
                "emergency flatten did not fill immediately: {status:?}"
            )),
            None => Err(anyhow!("emergency flatten returned no status")),
        }
    }
}
