use anyhow::Context;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{BookOrder, OrderBookDelta, OrderBookDeltas, QuoteTick},
    enums::{BookAction, OrderSide, RecordFlag},
    instruments::{Instrument, InstrumentAny},
};

use crate::{
    common::parse::{parse_level, parse_millis_timestamp},
    websocket::messages::GateWsMessage,
};

pub fn parse_gate_orderbook_deltas(
    message: &GateWsMessage,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
    depth_limit: Option<usize>,
) -> anyhow::Result<OrderBookDeltas> {
    let result = message
        .result
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("missing Gate order book result"))?;
    let is_snapshot = result.full.unwrap_or(false);
    // ts_event 统一用本地接收时刻(ts_init, 单调递增), 避免 Gate 服务端时间(ms)与
    // 本地时钟(ns)混用导致 ts_event 非单调 -> 框架判定乱序丢弃更新(满屏 WARN +
    // 簿短暂陈旧)。ts_init 为 0(非实时路径如单测)时回退 Gate 时间。
    let ts_event = if ts_init.is_zero() {
        result
            .t
            .or(message.time_ms)
            .map(|ts| parse_millis_timestamp(ts, "gate.orderbook"))
            .transpose()?
            .unwrap_or_default()
    } else {
        ts_init
    };
    let ts_init = if ts_init.is_zero() { ts_event } else { ts_init };
    let instrument_id = instrument.id();
    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();

    let bid_levels = result.b.iter().take(depth_limit.unwrap_or(usize::MAX));
    let ask_levels = result.a.iter().take(depth_limit.unwrap_or(usize::MAX));
    let total_levels = bid_levels.len() + ask_levels.len();
    let mut deltas = Vec::with_capacity(total_levels + usize::from(is_snapshot));

    if is_snapshot {
        deltas.push(OrderBookDelta::clear(
            instrument_id,
            result.last_update_id,
            ts_event,
            ts_init,
        ));
    }

    let mut processed = 0_usize;
    let mut push_level = |level: &[String], side: OrderSide| -> anyhow::Result<()> {
        let (price, size) = parse_level(level, price_precision, size_precision, "gate.orderbook")?;
        let action = if size.is_zero() {
            BookAction::Delete
        } else if is_snapshot {
            BookAction::Add
        } else {
            BookAction::Update
        };

        processed += 1;
        let mut flags = RecordFlag::F_MBP as u8;
        if processed == total_levels {
            flags |= RecordFlag::F_LAST as u8;
        }

        let order = BookOrder::new(side, price, size, result.last_update_id);
        deltas.push(
            OrderBookDelta::new_checked(
                instrument_id,
                action,
                order,
                flags,
                result.last_update_id,
                ts_event,
                ts_init,
            )
            .context("failed to construct Gate OrderBookDelta")?,
        );
        Ok(())
    };

    for level in result.b.iter().take(depth_limit.unwrap_or(usize::MAX)) {
        push_level(level, OrderSide::Buy)?;
    }
    for level in result.a.iter().take(depth_limit.unwrap_or(usize::MAX)) {
        push_level(level, OrderSide::Sell)?;
    }

    if total_levels == 0
        && let Some(last) = deltas.last_mut()
    {
        last.flags |= RecordFlag::F_LAST as u8;
    }

    OrderBookDeltas::new_checked(instrument_id, deltas)
        .context("failed to assemble Gate OrderBookDeltas")
}

pub fn parse_gate_orderbook_quote(
    message: &GateWsMessage,
    instrument: &InstrumentAny,
    last_quote: Option<&QuoteTick>,
    ts_init: UnixNanos,
) -> anyhow::Result<QuoteTick> {
    let result = message
        .result
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("missing Gate order book result"))?;
    let ts_event = result
        .t
        .or(message.time_ms)
        .map(|ts| parse_millis_timestamp(ts, "gate.orderbook"))
        .transpose()?
        .unwrap_or(ts_init);
    let ts_init = if ts_init.is_zero() { ts_event } else { ts_init };
    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();
    let best = |levels: &[Vec<String>], label: &str| -> anyhow::Result<Option<_>> {
        levels
            .first()
            .map(|level| parse_level(level, price_precision, size_precision, label))
            .transpose()
    };

    let bids = best(&result.b, "gate.bid")?;
    let asks = best(&result.a, "gate.ask")?;

    let (bid_price, bid_size) = match (bids, last_quote) {
        (Some(level), _) => level,
        (None, Some(prev)) => (prev.bid_price, prev.bid_size),
        (None, None) => anyhow::bail!("Gate order book update missing bid levels"),
    };
    let (ask_price, ask_size) = match (asks, last_quote) {
        (Some(level), _) => level,
        (None, Some(prev)) => (prev.ask_price, prev.ask_size),
        (None, None) => anyhow::bail!("Gate order book update missing ask levels"),
    };

    QuoteTick::new_checked(
        instrument.id(),
        bid_price,
        ask_price,
        bid_size,
        ask_size,
        ts_event,
        ts_init,
    )
    .context("failed to construct Gate QuoteTick")
}
