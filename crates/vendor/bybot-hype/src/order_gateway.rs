use anyhow::{bail, Result};
use hypersdk::{
    hypercore::{
        self,
        types::{
            BatchCancel, BatchCancelCloid, BatchModify, BatchOrder, Cancel, CancelByCloid, Modify,
            OrderGrouping, OrderRequest, OrderResponseStatus, OrderTypePlacement, TimeInForce,
        },
        Cloid, NonceHandler, OidOrCloid, PrivateKeySigner,
    },
    Address,
};

use crate::{
    order_state::{LifecycleEvent, OrderLifecycle},
    orders::{ExecutionPolicy, OrderIntent, OrderSide},
};

#[derive(Debug)]
pub struct SubmittedOrder {
    pub cloid: Cloid,
    pub statuses: Vec<OrderResponseStatus>,
    pub lifecycle: OrderLifecycle,
}

pub struct OrderGateway {
    client: hypercore::HttpClient,
    signer: PrivateKeySigner,
    nonce: NonceHandler,
    vault_address: Option<Address>,
}

impl OrderGateway {
    #[must_use]
    pub fn new(
        client: hypercore::HttpClient,
        signer: PrivateKeySigner,
        vault_address: Option<Address>,
    ) -> Self {
        Self {
            client,
            signer,
            nonce: NonceHandler::default(),
            vault_address,
        }
    }

    pub async fn submit(&self, intent: &OrderIntent) -> Result<SubmittedOrder> {
        self.submit_with_cloid(intent, Cloid::random()).await
    }

    pub async fn submit_with_cloid(
        &self,
        intent: &OrderIntent,
        cloid: Cloid,
    ) -> Result<SubmittedOrder> {
        validate_intent(intent)?;
        let statuses = self
            .client
            .place(
                &self.signer,
                BatchOrder {
                    orders: vec![request_from_intent(intent, cloid)],
                    grouping: OrderGrouping::Na,
                    builder: None,
                },
                self.nonce.next(),
                self.vault_address,
                None,
            )
            .await?;
        let lifecycle = lifecycle_from_response(&statuses)?;
        Ok(SubmittedOrder {
            cloid,
            statuses,
            lifecycle,
        })
    }

    pub async fn cancel(&self, asset: usize, oid: u64) -> Result<Vec<OrderResponseStatus>> {
        Ok(self
            .client
            .cancel(
                &self.signer,
                BatchCancel {
                    cancels: vec![Cancel { asset, oid }],
                },
                self.nonce.next(),
                self.vault_address,
                None,
            )
            .await?)
    }

    pub async fn cancel_by_cloid(
        &self,
        asset: u32,
        cloid: Cloid,
    ) -> Result<Vec<OrderResponseStatus>> {
        Ok(self
            .client
            .cancel_by_cloid(
                &self.signer,
                BatchCancelCloid {
                    cancels: vec![CancelByCloid { asset, cloid }],
                },
                self.nonce.next(),
                self.vault_address,
                None,
            )
            .await?)
    }

    pub async fn modify(
        &self,
        order: OidOrCloid,
        intent: &OrderIntent,
    ) -> Result<Vec<OrderResponseStatus>> {
        validate_intent(intent)?;
        let cloid = Cloid::random();
        Ok(self
            .client
            .modify(
                &self.signer,
                BatchModify {
                    modifies: vec![Modify {
                        oid: order,
                        order: request_from_intent(intent, cloid),
                    }],
                },
                self.nonce.next(),
                self.vault_address,
                None,
            )
            .await?)
    }
}

fn lifecycle_from_response(statuses: &[OrderResponseStatus]) -> Result<OrderLifecycle> {
    let mut lifecycle = OrderLifecycle::new();
    lifecycle.apply(LifecycleEvent::Sent)?;
    match statuses.first() {
        Some(OrderResponseStatus::Resting { .. }) => {
            lifecycle.apply(LifecycleEvent::Open)?;
        }
        Some(OrderResponseStatus::Filled { .. }) => {
            lifecycle.apply(LifecycleEvent::Filled)?;
        }
        Some(OrderResponseStatus::Error(_)) => {
            lifecycle.apply(LifecycleEvent::Rejected)?;
        }
        Some(
            OrderResponseStatus::Success
            | OrderResponseStatus::WaitingForTrigger
            | OrderResponseStatus::WaitingForFill,
        )
        | None => {}
    }
    Ok(lifecycle)
}

fn validate_intent(intent: &OrderIntent) -> Result<()> {
    if intent.limit_price() <= rust_decimal::Decimal::ZERO {
        bail!("order price must be positive");
    }
    if intent.size() <= rust_decimal::Decimal::ZERO {
        bail!("order size must be positive");
    }
    Ok(())
}

fn request_from_intent(intent: &OrderIntent, cloid: Cloid) -> OrderRequest {
    let tif = match intent.policy() {
        ExecutionPolicy::MakerOnly => TimeInForce::Alo,
        ExecutionPolicy::ImmediateOrCancel => TimeInForce::Ioc,
        ExecutionPolicy::GoodTillCanceled => TimeInForce::Gtc,
    };
    OrderRequest {
        asset: intent.asset(),
        is_buy: intent.side() == OrderSide::Buy,
        limit_px: intent.limit_price(),
        sz: intent.size(),
        reduce_only: intent.reduce_only(),
        order_type: OrderTypePlacement::Limit { tif },
        cloid,
    }
}
