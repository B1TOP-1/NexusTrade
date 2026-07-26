use anyhow::{bail, Result};
use hypersdk::{
    hypercore::{
        self,
        types::{BasicOrder, ClearinghouseState, Fill, OrderUpdate},
        OidOrCloid,
    },
    Address,
};
use rust_decimal::Decimal;

#[derive(Debug, Clone, PartialEq)]
pub struct PositionDetails {
    pub signed_quantity: Decimal,
    pub average_price: Decimal,
    pub available_balance: Decimal,
    pub margin_used: Decimal,
}

pub struct PositionService {
    client: hypercore::HttpClient,
    user: Address,
}

impl PositionService {
    #[must_use]
    pub fn new(client: hypercore::HttpClient, user: Address) -> Self {
        Self { client, user }
    }

    pub async fn state(&self, dex: Option<&str>) -> Result<ClearinghouseState> {
        self.client
            .clearinghouse_state(self.user, dex.map(str::to_string))
            .await
    }

    pub async fn position_size(&self, dex: Option<&str>, symbols: &[&str]) -> Result<Decimal> {
        let state = self.state(dex).await?;
        Ok(state
            .asset_positions
            .iter()
            .find(|position| symbols.contains(&position.position.coin.as_str()))
            .map_or(Decimal::ZERO, |position| position.position.szi))
    }

    pub async fn position_details(
        &self,
        dex: Option<&str>,
        symbols: &[&str],
    ) -> Result<PositionDetails> {
        let state = self.state(dex).await?;
        let position = state
            .asset_positions
            .iter()
            .find(|position| symbols.contains(&position.position.coin.as_str()));
        Ok(PositionDetails {
            signed_quantity: position.map_or(Decimal::ZERO, |value| value.position.szi),
            average_price: position
                .and_then(|value| value.position.entry_px)
                .unwrap_or(Decimal::ZERO),
            available_balance: state.withdrawable,
            margin_used: position.map_or(Decimal::ZERO, |value| value.position.margin_used),
        })
    }

    pub async fn open_orders(&self, dex: Option<&str>) -> Result<Vec<BasicOrder>> {
        self.client
            .open_orders(self.user, dex.map(str::to_string))
            .await
    }

    pub async fn ensure_flat(&self, dex: Option<&str>, symbols: &[&str]) -> Result<()> {
        let size = self.position_size(dex, symbols).await?;
        if !size.is_zero() {
            bail!("position is not flat: {size}");
        }
        if self
            .open_orders(dex)
            .await?
            .iter()
            .any(|order| symbols.contains(&order.coin.as_str()))
        {
            bail!("market has an open order");
        }
        Ok(())
    }

    pub async fn order_status(&self, order: OidOrCloid) -> Result<Option<OrderUpdate<BasicOrder>>> {
        self.client.order_status(self.user, order).await
    }

    pub async fn historical_orders(&self) -> Result<Vec<OrderUpdate<BasicOrder>>> {
        self.client.historical_orders(self.user).await
    }

    pub async fn fills(&self) -> Result<Vec<Fill>> {
        self.client.user_fills(self.user).await
    }
}
