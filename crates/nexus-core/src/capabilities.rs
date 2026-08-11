//! 交易所能力声明。策略下单前可查询，避免踩不支持的功能。

use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct VenueCapabilities {
    /// WS 下单通道（P1：能走 WS 就不走 REST）。
    pub ws_order_entry: bool,
    /// 原生批量下单/撤单（单帧多指令）。
    pub batch_orders: bool,
    pub post_only: bool,
    pub reduce_only: bool,
    /// 交易所原生一键撤单；false 时 SDK 逐单模拟 cancel_all。
    pub cancel_all_native: bool,
    /// 最快增量盘口档位（毫秒）。Binance futures = 100ms。
    pub book_fastest_interval_ms: u32,
    /// 支持双轨订阅（P7）。
    pub dual_feed: bool,
}

impl Default for VenueCapabilities {
    /// 保守默认：什么都不支持，adapter 必须显式声明。
    fn default() -> Self {
        Self {
            ws_order_entry: false,
            batch_orders: false,
            post_only: false,
            reduce_only: false,
            cancel_all_native: false,
            book_fastest_interval_ms: 1000,
            dual_feed: false,
        }
    }
}
