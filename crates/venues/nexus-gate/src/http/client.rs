use std::{collections::HashMap, num::NonZeroU32};

use nautilus_core::{consts::NAUTILUS_USER_AGENT, time::get_atomic_clock_realtime};
use nautilus_network::{
    http::{HttpClient, Method, USER_AGENT},
    ratelimiter::quota::Quota,
};
use serde::de::DeserializeOwned;

use crate::{common::credential::GateCredential, http::models::GateFuturesContract};

#[derive(Clone, Debug)]
pub struct GateHttpClient {
    client: HttpClient,
    base_url: String,
    timeout_secs: Option<u64>,
}

impl GateHttpClient {
    pub fn new(
        base_url: Option<String>,
        timeout_secs: Option<u64>,
        proxy_url: Option<String>,
    ) -> anyhow::Result<Self> {
        let quota = Quota::per_second(
            NonZeroU32::new(10).ok_or_else(|| anyhow::anyhow!("invalid Gate REST quota"))?,
        )
        .ok_or_else(|| anyhow::anyhow!("invalid Gate REST quota"))?;
        let client = HttpClient::new(
            default_headers(),
            Vec::new(),
            Vec::new(),
            Some(quota),
            timeout_secs,
            proxy_url,
        )?;
        Ok(Self {
            client,
            base_url: base_url.unwrap_or_else(|| "https://api.gateio.ws/api/v4".to_string()),
            timeout_secs,
        })
    }

    pub async fn get_futures_contract(
        &self,
        settle: &str,
        contract: &str,
    ) -> anyhow::Result<GateFuturesContract> {
        self.get(&format!("/futures/{settle}/contracts/{contract}"))
            .await
    }

    pub async fn get_futures_contracts(
        &self,
        settle: &str,
    ) -> anyhow::Result<Vec<GateFuturesContract>> {
        self.get(&format!("/futures/{settle}/contracts")).await
    }

    /// Fetches the futures account, returning `(total, available)` balance.
    ///
    /// # Errors
    ///
    /// Returns an error if the signed request fails or the response is invalid.
    pub async fn get_futures_account(
        &self,
        settle: &str,
        credential: &GateCredential,
    ) -> anyhow::Result<(f64, f64)> {
        let path = format!("/futures/{settle}/accounts");
        let value: serde_json::Value = self.get_signed(&path, "", credential).await?;
        let parse = |key: &str| -> f64 {
            value
                .get(key)
                .and_then(|v| {
                    v.as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .or_else(|| v.as_f64())
                })
                .unwrap_or(0.0)
        };
        Ok((parse("total"), parse("available")))
    }

    /// Fetches open orders for one contract (signed).
    ///
    /// # Errors
    ///
    /// Returns an error if the signed request fails or the response is invalid.
    pub async fn get_open_orders(
        &self,
        settle: &str,
        contract: &str,
        credential: &GateCredential,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let path = format!("/futures/{settle}/orders");
        let query = format!("contract={}&status=open", contract.to_uppercase());
        let value: serde_json::Value = self.get_signed(&path, &query, credential).await?;
        Ok(value.as_array().cloned().unwrap_or_default())
    }

    /// Fetches open positions (signed).
    ///
    /// # Errors
    ///
    /// Returns an error if the signed request fails or the response is invalid.
    pub async fn get_positions(
        &self,
        settle: &str,
        credential: &GateCredential,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let path = format!("/futures/{settle}/positions");
        let value: serde_json::Value = self.get_signed(&path, "", credential).await?;
        Ok(value.as_array().cloned().unwrap_or_default())
    }

    /// Signed GET (Gate APIv4 KEY/SIGN/Timestamp headers). `query` is the raw
    /// query string (without `?`) and is included in both the signature and URL.
    async fn get_signed<T>(
        &self,
        path: &str,
        query: &str,
        credential: &GateCredential,
    ) -> anyhow::Result<T>
    where
        T: DeserializeOwned,
    {
        let timestamp = (get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000) as i64;
        let signature = credential.sign_rest("GET", path, query, "", timestamp);
        let headers = HashMap::from([
            ("KEY".to_string(), credential.api_key().to_string()),
            ("SIGN".to_string(), signature),
            ("Timestamp".to_string(), timestamp.to_string()),
            ("Accept".to_string(), "application/json".to_string()),
        ]);
        let url = if query.is_empty() {
            format!("{}{}", self.base_url, path)
        } else {
            format!("{}{}?{}", self.base_url, path, query)
        };
        let response = self
            .client
            .request_with_params(
                Method::GET,
                url,
                Option::<&()>::None,
                Some(headers),
                None,
                self.timeout_secs,
                Some(vec![path.to_string()]),
            )
            .await?;
        Ok(serde_json::from_slice(&response.body)?)
    }

    async fn get<T>(&self, path: &str) -> anyhow::Result<T>
    where
        T: DeserializeOwned,
    {
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .client
            .request_with_params(
                Method::GET,
                url,
                Option::<&()>::None,
                None,
                None,
                self.timeout_secs,
                Some(vec![path.to_string()]),
            )
            .await?;
        Ok(serde_json::from_slice(&response.body)?)
    }
}

fn default_headers() -> HashMap<String, String> {
    HashMap::from([(USER_AGENT.to_string(), NAUTILUS_USER_AGENT.to_string())])
}
