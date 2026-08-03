//! Binance Futures JSON 线格式类型。
//!
//! serde Deserialize，仅取所需字段。数量/价格字符串由 adapter 层转 Decimal。

use serde::{Deserialize, Serialize};

// ── exchangeInfo ──

#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeInfo {
    pub symbols: Vec<SymbolInfo>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SymbolInfo {
    pub symbol: String,
    pub status: String,
    #[serde(default)]
    pub filters: Vec<FilterValue>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "filterType")]
pub enum FilterValue {
    #[serde(rename = "PRICE_FILTER")]
    PriceFilter { tickSize: String },
    #[serde(rename = "LOT_SIZE")]
    LotSize {
        stepSize: String,
        minQty: String,
    },
    #[serde(rename = "MIN_NOTIONAL")]
    MinNotional { notional: String },
    // 其余 filter 类型忽略。
    #[serde(other)]
    Unknown,
}

// ── depth REST snapshot ──

#[derive(Debug, Clone, Deserialize)]
pub struct DepthSnapshot {
    pub lastUpdateId: u64,
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
}

// ── WebSocket depth stream ──

#[derive(Debug, Clone, Deserialize)]
pub struct DepthStreamData {
    #[serde(rename = "e")]
    pub event_type: String, // "depthUpdate"
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "U")]
    pub first_update_id: u64,
    #[serde(rename = "u")]
    pub final_update_id: u64,
    #[serde(rename = "pu")]
    pub prev_final_id: u64,
    #[serde(rename = "b")]
    pub bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    pub asks: Vec<[String; 2]>,
}

/// 订阅响应（SUBSCRIBE 或订阅确认）。
#[derive(Debug, Clone, Deserialize)]
pub struct SubscribeResponse {
    pub result: Option<serde_json::Value>,
    pub id: Option<u64>,
}

// ── aggTrade ──

#[derive(Debug, Clone, Deserialize)]
pub struct AggTradeData {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "q")]
    pub qty: String,
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
}

// ── User data stream events ──

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "e")]
pub enum UserStreamEvent {
    #[serde(rename = "listenKeyExpired")]
    ListenKeyExpired,
    #[serde(rename = "ORDER_TRADE_UPDATE")]
    OrderTradeUpdate(OrderTradeUpdate),
    #[serde(rename = "ACCOUNT_UPDATE")]
    AccountUpdate(AccountUpdateWrapper),
    #[serde(rename = "ACCOUNT_CONFIG_UPDATE")]
    AccountConfigUpdate,
    #[serde(rename = "MARGIN_CALL")]
    MarginCall,
    // 未知类型忽略
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderTradeUpdate {
    #[serde(rename = "o")]
    pub order: OrderData,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderData {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "c")]
    pub client_order_id: String,
    #[serde(rename = "i")]
    pub order_id: u64,
    #[serde(rename = "S")]
    pub side: String,
    #[serde(rename = "o")]
    pub order_type: String,
    #[serde(rename = "X")]
    pub status: String,
    #[serde(rename = "q")]
    pub original_qty: String,
    #[serde(rename = "z")]
    pub executed_qty: String,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "T")]
    pub trade_time: u64,
    #[serde(rename = "L")]
    pub last_filled_price: String,
    #[serde(rename = "l")]
    pub last_filled_qty: String,
    #[serde(rename = "n")]
    pub commission: String,
    #[serde(rename = "N")]
    pub commission_asset: String,
    #[serde(rename = "r")]
    pub reduce_only: bool,
    #[serde(rename = "m")]
    pub is_maker: bool,
    #[serde(rename = "R")]
    pub realized_pnl: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccountUpdateWrapper {
    #[serde(rename = "a")]
    pub data: AccountUpdateData,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccountUpdateData {
    #[serde(rename = "B")]
    pub balances: Vec<BalanceEntry>,
    #[serde(rename = "P")]
    pub positions: Vec<PositionEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BalanceEntry {
    #[serde(rename = "a")]
    pub asset: String,
    #[serde(rename = "wb")]
    pub wallet_balance: String,
    #[serde(rename = "cw")]
    pub cross_wallet_balance: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PositionEntry {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "pa")]
    pub position_amount: String,
    #[serde(rename = "ep")]
    pub entry_price: String,
}

// ── listenKey ──

#[derive(Debug, Clone, Deserialize)]
pub struct ListenKeyResponse {
    pub listenKey: String,
}

// ── WS order request (serialize only) ──

#[derive(Debug, Clone, Serialize)]
pub struct WsOrderRequest {
    pub id: String,
    pub method: String,
    pub params: WsOrderParams,
}

#[derive(Debug, Clone, Serialize)]
pub struct WsOrderParams {
    pub symbol: String,
    pub side: String,
    #[serde(rename = "type")]
    pub order_type: String,
    #[serde(rename = "timeInForce", skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<String>,
    pub quantity: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    #[serde(rename = "newClientOrderId")]
    pub client_order_id: String,
    #[serde(rename = "reduceOnly", skip_serializing_if = "Option::is_none")]
    pub reduce_only: Option<bool>,
    pub timestamp: i64,
    pub signature: String,
    #[serde(rename = "apiKey")]
    pub api_key: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct WsCancelRequest {
    pub id: String,
    pub method: String,
    pub params: WsCancelParams,
}

#[derive(Debug, Clone, Serialize)]
pub struct WsCancelParams {
    pub symbol: String,
    #[serde(rename = "origClientOrderId")]
    pub orig_client_order_id: String,
    #[serde(rename = "orderId", skip_serializing_if = "Option::is_none")]
    pub order_id: Option<u64>,
    pub timestamp: i64,
    pub signature: String,
    #[serde(rename = "apiKey")]
    pub api_key: String,
}

// ── REST account snapshot ──

#[derive(Debug, Clone, Deserialize)]
pub struct AccountInfo {
    pub positions: Vec<AccountPosition>,
    pub assets: Vec<AccountAsset>,
    #[serde(default)]
    pub canDeposit: bool,
    #[serde(default)]
    pub canTrade: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccountPosition {
    pub symbol: String,
    #[serde(default)]
    pub positionAmt: String,
    #[serde(default)]
    pub entryPrice: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccountAsset {
    pub asset: String,
    #[serde(default)]
    pub walletBalance: String,
    #[serde(default)]
    pub availableBalance: String,
}

// ── depth response wrapper (for combined stream) ──

#[derive(Debug, Clone, Deserialize)]
pub struct CombinedStream<T> {
    pub stream: String,
    pub data: T,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_depth_snapshot() {
        let json = r#"{"lastUpdateId":1027024,"bids":[["27789.19","0.159"],["27788.85","3.740"]],"asks":[["27789.20","0.353"],["27789.67","0.319"]]}"#;
        let snap: DepthSnapshot = serde_json::from_str(json).unwrap();
        assert_eq!(snap.lastUpdateId, 1027024);
        assert_eq!(snap.bids[0][0], "27789.19");
        assert_eq!(snap.asks.len(), 2);
    }

    #[test]
    fn parse_depth_stream_event() {
        let json = r#"{"e":"depthUpdate","E":1680000000000,"s":"BTCUSDT","U":1027025,"u":1027030,"pu":1027024,"b":[["27789.19","0.000"]],"a":[["27789.67","0.500"],["27790.00","1.200"]]}"#;
        let ev: DepthStreamData = serde_json::from_str(json).unwrap();
        assert_eq!(ev.event_type, "depthUpdate");
        assert_eq!(ev.first_update_id, 1027025);
        assert_eq!(ev.final_update_id, 1027030);
        assert_eq!(ev.bids.len(), 1);
        assert_eq!(ev.asks.len(), 2);
    }

    #[test]
    fn parse_order_trade_update() {
        let json = r#"{"e":"ORDER_TRADE_UPDATE","o":{"s":"BTCUSDT","c":"nx-1-5","i":123456,"S":"BUY","o":"LIMIT","X":"FILLED","q":"0.001","z":"0.001","p":"65000.00","T":1680000000000,"L":"65000.00","l":"0.001","n":"0.00000001","N":"BNB","r":false,"m":false,"R":"0"}}"#;
        let ev: UserStreamEvent = serde_json::from_str(json).unwrap();
        match ev {
            UserStreamEvent::OrderTradeUpdate(otu) => {
                assert_eq!(otu.order.status, "FILLED");
                assert_eq!(otu.order.order_id, 123456);
            }
            _ => panic!("expected OrderTradeUpdate"),
        }
    }

    #[test]
    fn parse_account_update() {
        let json = r#"{"e":"ACCOUNT_UPDATE","a":{"B":[{"a":"USDT","wb":"1000.5","cw":"1000.5"}],"P":[{"s":"BTCUSDT","pa":"0.001","ep":"65000.00"}]}}"#;
        let ev: UserStreamEvent = serde_json::from_str(json).unwrap();
        match ev {
            UserStreamEvent::AccountUpdate(au) => {
                assert_eq!(au.data.balances[0].asset, "USDT");
                assert_eq!(au.data.positions[0].symbol, "BTCUSDT");
            }
            _ => panic!("expected AccountUpdate"),
        }
    }
}
