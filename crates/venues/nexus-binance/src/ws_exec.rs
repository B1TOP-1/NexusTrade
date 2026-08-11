//! WebSocket 下单通道：ws-fapi (wss://ws-fapi.binance.com/ws-fapi/v1)。
//!
//! 架构底线①：下单/撤单走 WS，不用 REST。本地 buffer 攒单一次性 flush。
//!
//! 已用 Python 实测验证（2026-08-11）：
//! - 端点 `wss://ws-fapi.binance.com/ws-fapi/v1` 可连通
//! - HMAC-SHA256 签名可用（无需 Ed25519，现有 API Key 直接复用）
//! - `order.place` / `order.cancel` 全链路 200（下单 orderId 真实返回）
//!
//! 签名规则（与 REST 一致）：
//! - params 除 signature 外按 key 字母序排序 → `k=v&k=v` → HMAC-SHA256 hex
//! - apiKey + timestamp 都在 params 里，apiKey 参与签名
//! - 每请求独立 `id`，服务端原样回显

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use nexus_core::{NewOrder, NexusError, OrderKind, Result, Symbol, Tif};
use tokio::sync::{mpsc, oneshot};

use crate::ws;

/// ws-fapi 端点。
const WS_FAPI_MAINNET: &str = "wss://ws-fapi.binance.com/ws-fapi/v1";
const WS_FAPI_TESTNET: &str = "wss://testnet.binancefuture.com/ws-fapi/v1";

/// 挂起的请求：id → 响应回传通道。
type PendingMap = Arc<Mutex<HashMap<String, oneshot::Sender<serde_json::Value>>>>;

/// WS 下单客户端。
#[derive(Clone)]
pub struct WsFapiClient {
    api_key: String,
    api_secret: String,
    id_counter: Arc<AtomicU64>,
    write_tx: mpsc::UnboundedSender<String>,
    pending: PendingMap,
}

impl WsFapiClient {
    /// 连接 ws-fapi 端点。
    pub async fn connect(api_key: String, api_secret: String, testnet: bool) -> Result<Self> {
        let url = if testnet {
            WS_FAPI_TESTNET.to_string()
        } else {
            WS_FAPI_MAINNET.to_string()
        };
        Self::connect_url(url, api_key, api_secret).await
    }

    async fn connect_url(
        url: String,
        api_key: String,
        api_secret: String,
    ) -> Result<Self> {
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let (session, write_tx) = ws::spawn_reader(
            &url,
            tx,
            shutdown_rx,
            std::time::Duration::from_millis(500),
        )
        .await?;

        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));

        // 后台：保持会话存活 + 路由响应到挂起请求
        let pending_reader = Arc::clone(&pending);
        tokio::spawn(async move {
            let _keep_alive = (shutdown_tx, session);
            while let Some(msg) = rx.recv().await {
                let Ok(v) = serde_json::from_str::<serde_json::Value>(&msg) else {
                    continue;
                };
                let id = v["id"].as_str().unwrap_or("");
                let sender = pending_reader.lock().unwrap().remove(id);
                if let Some(sender) = sender {
                    let _ = sender.send(v);
                }
            }
        });

        Ok(Self {
            api_key,
            api_secret,
            id_counter: Arc::new(AtomicU64::new(1)),
            write_tx,
            pending,
        })
    }

    fn next_id(&self) -> String {
        let n = self.id_counter.fetch_add(1, Ordering::Relaxed);
        format!("nx-ws-{n}")
    }

    /// 签名 + 发送一条请求，等待响应（按 id 匹配）。
    async fn request(&self, method: &str, params: Vec<(String, String)>) -> Result<serde_json::Value> {
        let req_id = self.next_id();

        // 构造参数（不含 signature 和 timestamp）并排序
        let mut all_params = params;
        all_params.push((
            "timestamp".to_string(),
            chrono::Utc::now().timestamp_millis().to_string(),
        ));
        all_params.push(("apiKey".to_string(), self.api_key.clone()));
        all_params.sort_by(|a, b| a.0.cmp(&b.0));

        // ws-fapi 签名：所有 params（含 apiKey、timestamp，不含 signature）按 key
        // 排序 → `k=v&k=v` → HMAC-SHA256 hex。不能用 sign_request（它把 timestamp
        // 追加到末尾，破坏排序）。
        let query = all_params
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        let signed = crate::auth::sign(&query, &self.api_secret);

        // 注册 pending 通道
        let (resp_tx, resp_rx) = oneshot::channel();
        self.pending
            .lock()
            .unwrap()
            .insert(req_id.clone(), resp_tx);

        // 构造完整请求 JSON（params 含 signature）
        let mut params_map = serde_json::Map::new();
        for (k, v) in all_params {
            params_map.insert(k, serde_json::Value::String(v));
        }
        params_map.insert("signature".to_string(), serde_json::Value::String(signed));
        let payload = serde_json::json!({
            "id": req_id,
            "method": method,
            "params": params_map,
        });
        let _ = self.write_tx.send(payload.to_string());

        // 等待响应
        resp_rx
            .await
            .map_err(|_| NexusError::Transport("ws-fapi response timeout/dropped".into()))
    }

    /// 下单。返回 orderId。
    pub async fn place(&self, order: &NewOrder) -> Result<u64> {
        let (order_type, tif) = map_tif(order.tif, &order.kind)?;

        let mut params = vec![
            ("symbol".to_string(), order.symbol.venue_native.clone()),
            ("side".to_string(), format!("{:?}", order.side).to_uppercase()),
            ("type".to_string(), order_type.to_string()),
            ("quantity".to_string(), order.qty.to_string()),
            ("newClientOrderId".to_string(), order.client_id.0.clone()),
        ];
        if let Some(tif_str) = tif {
            params.push(("timeInForce".to_string(), tif_str.to_string()));
        }
        if let Some(price) = order.price() {
            params.push(("price".to_string(), price.to_string()));
        }
        if order.reduce_only {
            params.push(("reduceOnly".to_string(), "true".to_string()));
        }

        let resp = self.request("order.place", params).await?;
        if resp["status"].as_i64() != Some(200) {
            return Err(NexusError::VenueReject {
                code: "WS_FAPI".into(),
                msg: resp["error"].to_string(),
            });
        }
        resp["result"]["orderId"]
            .as_u64()
            .ok_or_else(|| NexusError::Transport("ws-fapi response missing orderId".into()))
    }

    /// 撤单。
    pub async fn cancel(&self, symbol: &Symbol, order_id: u64) -> Result<()> {
        let params = vec![
            ("symbol".to_string(), symbol.venue_native.clone()),
            ("orderId".to_string(), order_id.to_string()),
        ];
        let resp = self.request("order.cancel", params).await?;
        if resp["status"].as_i64() != Some(200) {
            return Err(NexusError::VenueReject {
                code: "WS_FAPI".into(),
                msg: resp["error"].to_string(),
            });
        }
        Ok(())
    }

    /// 查询订单状态（order.status）。返回完整响应（含 avgPrice/executedQty/status）。
    pub async fn query(&self, symbol: &Symbol, order_id: u64) -> Result<serde_json::Value> {
        let params = vec![
            ("symbol".to_string(), symbol.venue_native.clone()),
            ("orderId".to_string(), order_id.to_string()),
        ];
        let resp = self.request("order.status", params).await?;
        if resp["status"].as_i64() != Some(200) {
            return Err(NexusError::VenueReject {
                code: "WS_FAPI".into(),
                msg: resp["error"].to_string(),
            });
        }
        Ok(resp)
    }
}

/// TIF + 类型映射（与 REST execution.rs 保持一致）。
fn map_tif(tif: Tif, kind: &OrderKind) -> Result<(&'static str, Option<&'static str>)> {
    match (tif, kind) {
        (Tif::Gtc, _) => Ok(("LIMIT", Some("GTC"))),
        (Tif::Ioc, OrderKind::Limit { .. }) => Ok(("LIMIT", Some("IOC"))),
        (Tif::Ioc, OrderKind::Market) => Ok(("MARKET", None)),
        (Tif::Fok, _) => Ok(("LIMIT", Some("FOK"))),
        (Tif::PostOnly, _) => Ok(("LIMIT", Some("GTX"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_core::{ClientOrderId, Decimal};
    use rust_decimal_macros::dec;

    #[test]
    fn map_tif_gtx_is_post_only() {
        let sym = Symbol::new("BTC", "USDT", "BTCUSDT");
        let order = NewOrder::limit(
            sym,
            nexus_core::Side::Buy,
            dec!(60000),
            dec!(0.001),
            ClientOrderId("t-1".into()),
        )
        .post_only();
        let (t, tif) = map_tif(order.tif, &order.kind).unwrap();
        assert_eq!(t, "LIMIT");
        assert_eq!(tif, Some("GTX"));
    }

    #[test]
    fn signature_includes_api_key_and_timestamp() {
        // 验证签名串包含 apiKey 和 timestamp（排序后）。
        let mut params = vec![
            ("symbol".to_string(), "BTCUSDT".to_string()),
            ("apiKey".to_string(), "k".to_string()),
            ("timestamp".to_string(), "1".to_string()),
        ];
        params.sort_by(|a, b| a.0.cmp(&b.0));
        let query = params
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        assert_eq!(query, "apiKey=k&symbol=BTCUSDT&timestamp=1");
    }
}
