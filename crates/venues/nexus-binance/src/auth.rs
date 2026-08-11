//! HMAC-SHA256 签名 + listenKey 生命周期管理。

use std::time::Duration;

use hmac::{Hmac, Mac};
use nexus_core::{NexusError, Result};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// 对查询字符串做 HMAC-SHA256 签名，返回 hex 字符串。
pub fn sign(query_string: &str, api_secret: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(api_secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(query_string.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// 构造签名后的查询串：`{params}&timestamp={ms}&signature={hex}`
///
/// ⚠ 空参数时必须避免前导 `&`（`&timestamp=...`），否则签名串与真实请求
/// 不一致，Binance 报 `-1022 Signature not valid`。签名串必须与发送的
/// query string 逐字节一致。
pub fn sign_request(params: &str, api_secret: &str) -> String {
    let ts = chrono::Utc::now().timestamp_millis();
    let query = if params.is_empty() {
        format!("timestamp={ts}")
    } else {
        format!("{params}&timestamp={ts}")
    };
    let sig = sign(&query, api_secret);
    format!("{query}&signature={sig}")
}

// ── ListenKey RAII guard ──

/// listenKey 生命周期守卫：POST 获取 → PUT 续期 → Drop 时 DELETE。
pub struct ListenKeyGuard {
    pub listen_key: String,
    cancel_tx: Option<tokio::sync::watch::Sender<bool>>,
    _keepalive: Option<tokio::task::JoinHandle<()>>,
    /// 用于 Drop 时 spawn DELETE 的 client + base_url 副本。
    cleanup: Option<(reqwest::Client, String)>,
}

impl ListenKeyGuard {
    /// POST /fapi/v1/listenKey 获取 listenKey，并 spawn 每 30min PUT keepalive。
    ///
    /// ⚠ Binance 的 listenKey 端点必须带 `X-MBX-APIKEY` header（不需要签名）。
    pub async fn acquire(client: &reqwest::Client, base_url: &str, api_key: &str) -> Result<Self> {
        let resp: serde_json::Value = client
            .post(format!("{base_url}/fapi/v1/listenKey"))
            .header("X-MBX-APIKEY", api_key)
            .send()
            .await
            .map_err(|e| NexusError::Transport(format!("listenKey POST: {e}")))?
            .json()
            .await
            .map_err(|e| NexusError::Transport(format!("listenKey parse: {e}")))?;

        let listen_key = resp["listenKey"]
            .as_str()
            .ok_or_else(|| {
                NexusError::Transport(format!(
                    "listenKey missing in response: {resp} (api key header missing?)"
                ))
            })?
            .to_string();

        let (cancel_tx, mut cancel_rx) = tokio::sync::watch::channel(false);
        let key = listen_key.clone();
        let c = client.clone();
        let url = base_url.to_string();
        let api_key_owned = api_key.to_string();

        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30 * 60));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let _ = c
                            .put(format!("{url}/fapi/v1/listenKey"))
                            .header("X-MBX-APIKEY", &api_key_owned)
                            .header("Content-Type", "application/x-www-form-urlencoded")
                            .body(format!("listenKey={key}"))
                            .send()
                            .await;
                    }
                    _ = cancel_rx.changed() => break,
                }
            }
        });

        Ok(Self {
            listen_key,
            cancel_tx: Some(cancel_tx),
            _keepalive: Some(task),
            cleanup: Some((client.clone(), base_url.to_string())),
        })
    }

    pub fn key(&self) -> &str {
        &self.listen_key
    }
}

impl Drop for ListenKeyGuard {
    fn drop(&mut self) {
        // 停止 keepalive 任务。
        if let Some(tx) = self.cancel_tx.take() {
            let _ = tx.send(true);
        }
        // fire-and-forget DELETE。
        if let Some((client, base_url)) = self.cleanup.take() {
            let key = self.listen_key.clone();
            tokio::spawn(async move {
                let _ = client
                    .delete(format!("{base_url}/fapi/v1/listenKey"))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .body(format!("listenKey={key}"))
                    .send()
                    .await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_matches_known_vector() {
        // Binance 官方示例
        let secret = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j";
        let query = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559";
        let sig = sign(query, secret);
        assert_eq!(
            sig,
            "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71"
        );
    }
}
