use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GateFuturesContract {
    pub name: String,
    pub quanto_multiplier: String,
    pub order_price_round: String,
    pub order_size_min: u64,
    pub order_size_max: Option<u64>,
    pub enable_decimal: bool,
    pub status: String,
    pub maker_fee_rate: Option<String>,
    pub taker_fee_rate: Option<String>,
}
