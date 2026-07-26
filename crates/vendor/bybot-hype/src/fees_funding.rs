use anyhow::Result;
use hypersdk::{
    hypercore::{
        self,
        types::{FundingRate, UserFees, UserFundingEntry, UserRateLimit},
    },
    Address,
};

pub struct FeeFundingService {
    client: hypercore::HttpClient,
    user: Address,
}

impl FeeFundingService {
    #[must_use]
    pub fn new(client: hypercore::HttpClient, user: Address) -> Self {
        Self { client, user }
    }

    pub async fn fees(&self) -> Result<UserFees> {
        self.client.user_fees(self.user).await
    }

    pub async fn rate_limit(&self) -> Result<UserRateLimit> {
        self.client.user_rate_limit(self.user).await
    }

    pub async fn funding_history(
        &self,
        symbol: &str,
        start_time_ms: u64,
        end_time_ms: Option<u64>,
    ) -> Result<Vec<FundingRate>> {
        self.client
            .funding_history(symbol, start_time_ms, end_time_ms)
            .await
    }

    pub async fn user_funding(
        &self,
        start_time_ms: u64,
        end_time_ms: Option<u64>,
    ) -> Result<Vec<UserFundingEntry>> {
        self.client
            .user_funding(self.user, start_time_ms, end_time_ms)
            .await
    }

    pub async fn market_contexts(&self, dex: Option<&str>) -> Result<serde_json::Value> {
        self.client
            .meta_and_asset_ctxs(dex.map(str::to_string))
            .await
    }
}
