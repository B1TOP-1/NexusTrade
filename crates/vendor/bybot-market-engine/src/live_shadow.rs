use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, http::HeaderValue, Error as WsError, Message},
};

use crate::book::{FillSide, LocalBook};
use crate::gate_sbe::decode_order_book_update;
use crate::lighter_book::LighterBook;
use crate::model::{EngineConfig, Level, SignalDepthMetadata, SignalRow};
use crate::replay::signal_to_json;
use crate::signal::SignalEngine;

const GATE_STALE_THRESHOLD_MS: u64 = 100;
const GATE_RECONNECT_THRESHOLD_MS: u64 = 500;
const LIGHTER_STALE_THRESHOLD_MS: u64 = 200;
const LIGHTER_RECONNECT_THRESHOLD_MS: u64 = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepthEmitMode {
    Always,
    SignalOnly,
    Never,
}

impl DepthEmitMode {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "always" => Ok(Self::Always),
            "signal_only" | "signal-only" | "signalonly" => Ok(Self::SignalOnly),
            "never" => Ok(Self::Never),
            other => Err(format!("invalid depth emit mode: {other}")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LiveShadowConfig {
    pub ticker: String,
    pub gate_contract: String,
    pub gate_settle: String,
    pub gate_depth: usize,
    pub gate_interval: String,
    pub lighter_market_id: u64,
    pub threshold_bps: f64,
    pub window_size: usize,
    pub min_samples: usize,
    pub sample_interval_ms: u64,
    pub run_seconds: u64,
    pub vwap_quote_usd: f64,
    pub gate_sbe_url: String,
    pub lighter_ws_url: String,
    pub depth_emit_mode: DepthEmitMode,
}

pub async fn run_live_shadow(config: LiveShadowConfig) -> Result<(), String> {
    let (tx, mut rx) = mpsc::unbounded_channel::<MarketEvent>();
    let gate_config = config.clone();
    let gate_tx = tx.clone();
    tokio::spawn(async move {
        if let Err(err) = gate_loop(gate_config, gate_tx).await {
            eprintln!("[RustLive][Gate] {err}");
        }
    });
    let lighter_config = config.clone();
    let lighter_tx = tx;
    tokio::spawn(async move {
        if let Err(err) = lighter_loop(lighter_config, lighter_tx).await {
            eprintln!("[RustLive][Lighter] {err}");
        }
    });

    let mut engine = SignalEngine::new(EngineConfig {
        window_size: config.window_size,
        min_samples: config.min_samples,
        threshold_bps: config.threshold_bps,
        ticker: config.ticker.clone(),
        gate_contract: config.gate_contract.clone(),
        lighter_market_id: config.lighter_market_id,
    });
    let mut gate = LocalBook::new();
    let mut lighter = LocalBook::new();
    let started = Instant::now();
    let mut sequence = 0u64;
    let mut last_gate_update = Instant::now() - Duration::from_millis(GATE_STALE_THRESHOLD_MS);
    let mut last_lighter_update = Instant::now() - Duration::from_millis(LIGHTER_STALE_THRESHOLD_MS);
    let mut last_sample_at: Option<Instant> = None;
    let mut last_heartbeat: Option<Instant> = None;

    while config.run_seconds == 0 || started.elapsed() < Duration::from_secs(config.run_seconds) {
        let Some(event) = rx.recv().await else {
            break;
        };
        let calc_start = Instant::now();
        if let Some(row) = handle_market_event_for_signal(
            event,
            &config,
            &mut engine,
            &mut gate,
            &mut lighter,
            &mut sequence,
            &mut last_gate_update,
            &mut last_lighter_update,
            &mut last_sample_at,
        ) {
            let calc_elapsed = calc_start.elapsed();
            if row.long_ok || row.short_ok {
                let signal_id = row.sequence.to_string();
                println!("{}", live_hot_signal_to_json(&config, &row, calc_elapsed, &gate, &lighter));
                if row.depth.is_some() {
                    println!("{}", live_diagnostic_to_json(&signal_id, &row));
                }
                last_heartbeat = Some(Instant::now());
            } else {
                let heartbeat_due = last_heartbeat
                    .map_or(true, |t| t.elapsed() >= Duration::from_secs(2));
                if heartbeat_due {
                    let mut hb = row.clone();
                    hb.depth = None;
                    println!("{}", live_signal_to_json(&config, &hb, calc_elapsed, &gate, &lighter));
                    last_heartbeat = Some(Instant::now());
                }
            }
        }
    }
    Ok(())
}

fn handle_market_event_for_signal(
    event: MarketEvent,
    config: &LiveShadowConfig,
    engine: &mut SignalEngine,
    gate: &mut LocalBook,
    lighter: &mut LocalBook,
    sequence: &mut u64,
    last_gate_update: &mut Instant,
    last_lighter_update: &mut Instant,
    last_sample_at: &mut Option<Instant>,
) -> Option<SignalRow> {
    match event {
        MarketEvent::GateSnapshot {
            bids,
            asks,
            book_id,
        } => {
            gate.apply_snapshot(&bids, &asks, Some(book_id));
            *last_gate_update = Instant::now();
        }
        MarketEvent::GateUpdate {
            bids,
            asks,
            first_id,
            last_id,
        } => {
            gate.apply_update(&bids, &asks, Some(first_id), Some(last_id));
            *last_gate_update = Instant::now();
        }
        MarketEvent::LighterSnapshot { bids, asks, nonce } => {
            lighter.apply_snapshot(&bids, &asks, Some(nonce));
            *last_lighter_update = Instant::now();
        }
        MarketEvent::LighterUpdate { bids, asks, nonce } => {
            lighter.apply_update(&bids, &asks, None, Some(nonce));
            *last_lighter_update = Instant::now();
        }
    }
    if gate.status() != crate::model::BookStatus::Ready {
        return None;
    }
    if last_gate_update.elapsed() >= Duration::from_millis(GATE_STALE_THRESHOLD_MS) {
        return None;
    }
    if last_lighter_update.elapsed() >= Duration::from_millis(LIGHTER_STALE_THRESHOLD_MS) {
        return None;
    }
    emit_vwap_signal_for_event(config, engine, gate, lighter, sequence, last_sample_at)
}

fn live_signal_to_json(
    config: &LiveShadowConfig,
    row: &SignalRow,
    calc_elapsed: Duration,
    gate: &LocalBook,
    lighter: &LocalBook,
) -> String {
    let row = row_for_depth_emit_mode(row, config.depth_emit_mode);
    let base = signal_to_json(&row);
    let Some(prefix) = base.strip_suffix('}') else {
        return base;
    };
    let (gate_bbo_bid, gate_bbo_bid_size) = gate.best_bid().unwrap_or((0.0, 0.0));
    let (gate_bbo_ask, gate_bbo_ask_size) = gate.best_ask().unwrap_or((0.0, 0.0));
    let (lighter_bbo_bid, lighter_bbo_bid_size) = lighter.best_bid().unwrap_or((0.0, 0.0));
    let (lighter_bbo_ask, lighter_bbo_ask_size) = lighter.best_ask().unwrap_or((0.0, 0.0));
    let gate_bbo_bid_qty_btc = gate_contract_qty_to_base(&row.gate_contract, gate_bbo_bid_size);
    let gate_bbo_ask_qty_btc = gate_contract_qty_to_base(&row.gate_contract, gate_bbo_ask_size);
    format!(
        "{},\"rust_calc_ms\":\"{:.8}\",\"gate_bbo_bid\":\"{}\",\"gate_bbo_bid_size_btc\":\"{}\",\"gate_bbo_ask\":\"{}\",\"gate_bbo_ask_size_btc\":\"{}\",\"lighter_bbo_bid\":\"{}\",\"lighter_bbo_bid_size\":\"{}\",\"lighter_bbo_ask\":\"{}\",\"lighter_bbo_ask_size\":\"{}\"}}",
        prefix,
        calc_elapsed.as_secs_f64() * 1000.0,
        fmt_live(gate_bbo_bid),
        fmt_live(gate_bbo_bid_qty_btc),
        fmt_live(gate_bbo_ask),
        fmt_live(gate_bbo_ask_qty_btc),
        fmt_live(lighter_bbo_bid),
        fmt_live(lighter_bbo_bid_size),
        fmt_live(lighter_bbo_ask),
        fmt_live(lighter_bbo_ask_size),
    )
}

fn row_for_depth_emit_mode(row: &SignalRow, mode: DepthEmitMode) -> SignalRow {
    let include_depth = match mode {
        DepthEmitMode::Always => true,
        DepthEmitMode::SignalOnly => row.long_ok || row.short_ok,
        DepthEmitMode::Never => false,
    };
    if include_depth || row.depth.is_none() {
        return row.clone();
    }
    let mut output = row.clone();
    output.depth = None;
    output
}

fn live_hot_signal_to_json(
    _config: &LiveShadowConfig,
    row: &SignalRow,
    calc_elapsed: Duration,
    gate: &LocalBook,
    lighter: &LocalBook,
) -> String {
    let (gate_bbo_bid, gate_bbo_bid_size) = gate.best_bid().unwrap_or((0.0, 0.0));
    let (gate_bbo_ask, gate_bbo_ask_size) = gate.best_ask().unwrap_or((0.0, 0.0));
    let (lighter_bbo_bid, lighter_bbo_bid_size) = lighter.best_bid().unwrap_or((0.0, 0.0));
    let (lighter_bbo_ask, lighter_bbo_ask_size) = lighter.best_ask().unwrap_or((0.0, 0.0));
    let gate_bbo_bid_qty_btc = gate_contract_qty_to_base(&row.gate_contract, gate_bbo_bid_size);
    let gate_bbo_ask_qty_btc = gate_contract_qty_to_base(&row.gate_contract, gate_bbo_ask_size);
    let side = if row.long_ok { "long_gate_short_lighter" } else { "short_gate_long_lighter" };
    format!(
        "{{\"event\":\"hot_signal\",\"signal_id\":\"{seq}\",\"sequence\":{seq},\"timestamp_ns\":{ts},\"side\":\"{side}\",\"ready\":{ready},\"sample_count\":{sc},\"long_ok\":{lo},\"short_ok\":{so},\"long_spread\":\"{ls}\",\"short_spread\":\"{ss}\",\"long_median\":\"{lm}\",\"short_median\":\"{sm}\",\"long_threshold\":\"{lt}\",\"short_threshold\":\"{st}\",\"basis\":\"{basis}\",\"gate_bid\":\"{gb}\",\"gate_bid_size\":\"{gbs}\",\"gate_ask\":\"{ga}\",\"gate_ask_size\":\"{gas}\",\"lighter_bid\":\"{lb}\",\"lighter_bid_size\":\"{lbs}\",\"lighter_ask\":\"{la}\",\"lighter_ask_size\":\"{las}\",\"gate_bbo_bid\":\"{gbb}\",\"gate_bbo_bid_size_btc\":\"{gbbs}\",\"gate_bbo_ask\":\"{gba}\",\"gate_bbo_ask_size_btc\":\"{gbas}\",\"lighter_bbo_bid\":\"{lbb}\",\"lighter_bbo_bid_size\":\"{lbbs}\",\"lighter_bbo_ask\":\"{lba}\",\"lighter_bbo_ask_size\":\"{lbas}\",\"rust_calc_ms\":\"{rc:.8}\"}}",
        seq = row.sequence,
        ts = row.timestamp_ns,
        side = side,
        ready = row.ready,
        sc = row.sample_count,
        lo = row.long_ok,
        so = row.short_ok,
        ls = fmt_live(row.long_spread),
        ss = fmt_live(row.short_spread),
        lm = fmt_live(row.long_median),
        sm = fmt_live(row.short_median),
        lt = fmt_live(row.long_threshold),
        st = fmt_live(row.short_threshold),
        basis = fmt_live(row.basis),
        gb = fmt_live(row.gate_bid),
        gbs = fmt_live(row.gate_bid_size),
        ga = fmt_live(row.gate_ask),
        gas = fmt_live(row.gate_ask_size),
        lb = fmt_live(row.lighter_bid),
        lbs = fmt_live(row.lighter_bid_size),
        la = fmt_live(row.lighter_ask),
        las = fmt_live(row.lighter_ask_size),
        gbb = fmt_live(gate_bbo_bid),
        gbbs = fmt_live(gate_bbo_bid_qty_btc),
        gba = fmt_live(gate_bbo_ask),
        gbas = fmt_live(gate_bbo_ask_qty_btc),
        lbb = fmt_live(lighter_bbo_bid),
        lbbs = fmt_live(lighter_bbo_bid_size),
        lba = fmt_live(lighter_bbo_ask),
        lbas = fmt_live(lighter_bbo_ask_size),
        rc = calc_elapsed.as_secs_f64() * 1000.0,
    )
}

fn fmt_depth_levels(levels: &[crate::model::DecimalLevel]) -> String {
    let rows: Vec<String> = levels
        .iter()
        .map(|l| format!("[\"{}\",\"{}\"]", fmt_live(l.price), fmt_live(l.size)))
        .collect();
    format!("[{}]", rows.join(","))
}

fn fmt_fill_metadata(fill: Option<crate::model::FillMetadata>) -> String {
    let Some(fill) = fill else {
        return "null".to_string();
    };
    format!(
        "{{\"vwap_avg_price\":\"{}\",\"filled_quantity\":\"{}\",\"levels_used\":{},\"remaining_quote\":\"{}\",\"is_complete\":{}}}",
        fmt_live(fill.avg_price),
        fmt_live(fill.filled_quantity),
        fill.levels_used,
        fmt_live(fill.remaining_quote),
        fill.is_complete,
    )
}

fn live_diagnostic_to_json(signal_id: &str, row: &SignalRow) -> String {
    let Some(depth) = &row.depth else {
        return String::new();
    };
    format!(
        "{{\"event\":\"diagnostic_snapshot\",\"signal_id\":\"{sid}\",\"gate_bid_levels\":{gbl},\"gate_ask_levels\":{gal},\"lighter_bid_levels\":{lbl},\"lighter_ask_levels\":{lal},\"gate_bid_fill\":{gbf},\"gate_ask_fill\":{gaf},\"lighter_bid_fill\":{lbf},\"lighter_ask_fill\":{laf}}}",
        sid = signal_id,
        gbl = fmt_depth_levels(&depth.gate_bid_levels),
        gal = fmt_depth_levels(&depth.gate_ask_levels),
        lbl = fmt_depth_levels(&depth.lighter_bid_levels),
        lal = fmt_depth_levels(&depth.lighter_ask_levels),
        gbf = fmt_fill_metadata(depth.gate_bid_fill),
        gaf = fmt_fill_metadata(depth.gate_ask_fill),
        lbf = fmt_fill_metadata(depth.lighter_bid_fill),
        laf = fmt_fill_metadata(depth.lighter_ask_fill),
    )
}

#[allow(dead_code)]
fn maybe_vwap_signal_for_test(
    gate: &LocalBook,
    lighter: &LocalBook,
    vwap_quote_usd: f64,
) -> Option<(f64, f64)> {
    let gate_bid = gate.weighted_fill_by_quote(FillSide::Sell, vwap_quote_usd)?;
    let gate_ask = gate.weighted_fill_by_quote(FillSide::Buy, vwap_quote_usd)?;
    let lighter_bid = lighter.weighted_fill_by_quote(FillSide::Sell, vwap_quote_usd)?;
    let lighter_ask = lighter.weighted_fill_by_quote(FillSide::Buy, vwap_quote_usd)?;
    if !gate_bid.is_complete
        || !gate_ask.is_complete
        || !lighter_bid.is_complete
        || !lighter_ask.is_complete
    {
        return None;
    }
    Some((gate_bid.avg_price, lighter_ask.avg_price))
}

fn emit_vwap_signal_for_event(
    config: &LiveShadowConfig,
    engine: &mut SignalEngine,
    gate: &LocalBook,
    lighter: &LocalBook,
    sequence: &mut u64,
    last_sample_at: &mut Option<Instant>,
) -> Option<SignalRow> {
    maybe_vwap_signal(config, engine, gate, lighter, sequence, last_sample_at)
}

#[allow(dead_code)]
fn gate_sampling_allowed(gate: &LocalBook, gate_idle_ms: u64) -> bool {
    gate.status() == crate::model::BookStatus::Ready && gate_idle_ms < GATE_STALE_THRESHOLD_MS
}

#[allow(dead_code)]
fn gate_reconnect_required(gate_idle_ms: u64) -> bool {
    gate_idle_ms >= GATE_RECONNECT_THRESHOLD_MS
}

fn fmt_live(value: f64) -> String {
    format!("{value:.8}")
}

fn maybe_vwap_signal(
    config: &LiveShadowConfig,
    engine: &mut SignalEngine,
    gate: &LocalBook,
    lighter: &LocalBook,
    sequence: &mut u64,
    last_sample_at: &mut Option<Instant>,
) -> Option<SignalRow> {
    let gate_bid = gate.weighted_fill_by_quote(FillSide::Sell, config.vwap_quote_usd)?;
    let gate_ask = gate.weighted_fill_by_quote(FillSide::Buy, config.vwap_quote_usd)?;
    let lighter_bid = lighter.weighted_fill_by_quote(FillSide::Sell, config.vwap_quote_usd)?;
    let lighter_ask = lighter.weighted_fill_by_quote(FillSide::Buy, config.vwap_quote_usd)?;
    if !gate_bid.is_complete
        || !gate_ask.is_complete
        || !lighter_bid.is_complete
        || !lighter_ask.is_complete
    {
        return None;
    }

    let mut vwap_gate = LocalBook::new();
    let mut vwap_lighter = LocalBook::new();
    vwap_gate.apply_snapshot(
        &[Level {
            price: f64_to_scaled(gate_bid.avg_price),
            size: f64_to_scaled(gate_bid.filled_quantity),
        }],
        &[Level {
            price: f64_to_scaled(gate_ask.avg_price),
            size: f64_to_scaled(gate_ask.filled_quantity),
        }],
        Some(*sequence),
    );
    vwap_lighter.apply_snapshot(
        &[Level {
            price: f64_to_scaled(lighter_bid.avg_price),
            size: f64_to_scaled(lighter_bid.filled_quantity),
        }],
        &[Level {
            price: f64_to_scaled(lighter_ask.avg_price),
            size: f64_to_scaled(lighter_ask.filled_quantity),
        }],
        Some(*sequence),
    );
    *sequence += 1;
    let sample_now = Instant::now();
    if median_sample_due(*last_sample_at, sample_now, config.sample_interval_ms)
        && engine.sample(&vwap_gate, &vwap_lighter)
    {
        *last_sample_at = Some(sample_now);
    }
    let mut row = engine.evaluate(*sequence, now_ns(), "rust_live", &vwap_gate, &vwap_lighter)?;
    row.depth = Some(SignalDepthMetadata {
        gate_bid_levels: gate.bid_levels(20),
        gate_ask_levels: gate.ask_levels(20),
        lighter_bid_levels: lighter.bid_levels(20),
        lighter_ask_levels: lighter.ask_levels(20),
        gate_bid_fill: Some(gate_bid.metadata()),
        gate_ask_fill: Some(gate_ask.metadata()),
        lighter_bid_fill: Some(lighter_bid.metadata()),
        lighter_ask_fill: Some(lighter_ask.metadata()),
    });
    Some(row)
}

fn median_sample_due(last_sample_at: Option<Instant>, now: Instant, interval_ms: u64) -> bool {
    match last_sample_at {
        None => true,
        Some(last) => now.saturating_duration_since(last) >= Duration::from_millis(interval_ms),
    }
}

async fn gate_loop(
    config: LiveShadowConfig,
    tx: mpsc::UnboundedSender<MarketEvent>,
) -> Result<(), String> {
    loop {
        if let Err(err) = gate_once(&config, &tx).await {
            eprintln!("[RustLive][Gate] reconnect: {err}");
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
    }
}

async fn gate_once(
    config: &LiveShadowConfig,
    tx: &mpsc::UnboundedSender<MarketEvent>,
) -> Result<(), String> {
    let mut request = config
        .gate_sbe_url
        .as_str()
        .into_client_request()
        .map_err(|err| err.to_string())?;
    request
        .headers_mut()
        .insert("X-Gate-Size-Decimal", HeaderValue::from_static("1"));
    let (mut ws, _) = connect_async(request)
        .await
        .map_err(|err| err.to_string())?;
    let subscribe = format!(
        "{{\"time\":{},\"channel\":\"futures.obu\",\"event\":\"subscribe\",\"payload\":[\"ob.{}.{}\"]}}",
        now_secs(),
        config.gate_contract,
        config.gate_depth,
    );
    ws.send(Message::Text(subscribe.into()))
        .await
        .map_err(|err| err.to_string())?;
    let mut book = LocalBook::new();
    let mut last_update = Instant::now();

    loop {
        match tokio::time::timeout(Duration::from_millis(GATE_STALE_THRESHOLD_MS), ws.next()).await
        {
            Ok(Some(message)) => match message.map_err(|err| err.to_string())? {
                Message::Binary(frame) => {
                    let update = decode_order_book_update(&frame)?;
                    if !gate_obu_stream_matches(
                        &update.symbol,
                        &config.gate_contract,
                        config.gate_depth,
                    ) {
                        continue;
                    }
                    if update.full {
                        book.apply_snapshot(&update.bids, &update.asks, Some(update.last_id));
                        tx.send(MarketEvent::GateSnapshot {
                            bids: update.bids,
                            asks: update.asks,
                            book_id: update.last_id,
                        })
                        .map_err(|err| err.to_string())?;
                    } else {
                        apply_gate_update_and_emit(
                            &mut book,
                            &tx,
                            update.bids,
                            update.asks,
                            update.first_id,
                            update.last_id,
                        )?;
                    }
                    last_update = Instant::now();
                }
                Message::Text(text) => {
                    if is_gate_obu_subscribe_ack(&text) {
                        eprintln!("[RustLive][Gate] subscribed {}", config.gate_contract);
                        continue;
                    }
                    if !is_gate_obu_update_text(&text) {
                        continue;
                    }
                    let (bids, asks, first_id, last_id, stream, is_snapshot) =
                        extract_gate_obu_update(&text)?;
                    if !gate_obu_stream_matches(&stream, &config.gate_contract, config.gate_depth) {
                        continue;
                    }
                    if is_snapshot {
                        book.apply_snapshot(&bids, &asks, Some(last_id));
                        tx.send(MarketEvent::GateSnapshot {
                            bids,
                            asks,
                            book_id: last_id,
                        })
                        .map_err(|err| err.to_string())?;
                    } else {
                        apply_gate_update_and_emit(&mut book, &tx, bids, asks, first_id, last_id)?;
                    }
                    last_update = Instant::now();
                }
                Message::Ping(payload) => {
                    ws.send(Message::Pong(payload))
                        .await
                        .map_err(|err| err.to_string())?;
                }
                Message::Close(_) => break,
                _ => {}
            },
            Ok(None) => break,
            Err(_) => {
                let idle_ms = last_update.elapsed().as_secs_f64() * 1000.0;
                if idle_ms >= GATE_RECONNECT_THRESHOLD_MS as f64 {
                    return Err(format!("gate stale for {:.3}ms", idle_ms));
                }
            }
        }
    }
    Ok(())
}

async fn lighter_loop(
    config: LiveShadowConfig,
    tx: mpsc::UnboundedSender<MarketEvent>,
) -> Result<(), String> {
    loop {
        if let Err(err) = lighter_once(&config, &tx).await {
            eprintln!("[RustLive][Lighter] reconnect: {err}");
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
    }
}

async fn lighter_once(
    config: &LiveShadowConfig,
    tx: &mpsc::UnboundedSender<MarketEvent>,
) -> Result<(), String> {
    let mut request = config
        .lighter_ws_url
        .as_str()
        .into_client_request()
        .map_err(|err| err.to_string())?;
    request.headers_mut().insert(
        "User-Agent",
        HeaderValue::from_static("bybot-rust-market-engine/0.1"),
    );
    let (mut ws, _) = connect_async(request).await.map_err(format_ws_error)?;
    let subscribe = format!(
        "{{\"type\":\"subscribe\",\"channel\":\"order_book/{}\"}}",
        config.lighter_market_id
    );
    ws.send(Message::Text(subscribe.into()))
        .await
        .map_err(|err| err.to_string())?;
    let mut book = LighterBook::new();
    let reconnect_timeout = Duration::from_millis(LIGHTER_RECONNECT_THRESHOLD_MS);
    loop {
        let next = tokio::time::timeout(reconnect_timeout, ws.next())
            .await
            .map_err(|_| format!("lighter silent for {}ms", LIGHTER_RECONNECT_THRESHOLD_MS))?;
        let Some(message) = next else { break };
        match message.map_err(|err| err.to_string())? {
            Message::Text(text) => {
                let before = book.nonce();
                let applied = book.apply_json(&text)?;
                if !applied {
                    continue;
                }
                let Some(nonce) = book.nonce() else {
                    continue;
                };
                let (bids, asks) = extract_lighter_levels(&text)?;
                let event = if before.is_none() {
                    MarketEvent::LighterSnapshot { bids, asks, nonce }
                } else {
                    MarketEvent::LighterUpdate { bids, asks, nonce }
                };
                tx.send(event).map_err(|err| err.to_string())?;
            }
            Message::Ping(payload) => {
                ws.send(Message::Pong(payload))
                    .await
                    .map_err(|err| err.to_string())?;
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    Ok(())
}

fn format_ws_error(err: WsError) -> String {
    match err {
        WsError::Http(response) => {
            format!(
                "HTTP error: {} headers={:?} body_len={}",
                response.status(),
                response.headers(),
                response.body().as_ref().map(|body| body.len()).unwrap_or(0),
            )
        }
        other => other.to_string(),
    }
}

fn apply_gate_update_and_emit(
    book: &mut LocalBook,
    tx: &mpsc::UnboundedSender<MarketEvent>,
    bids: Vec<Level>,
    asks: Vec<Level>,
    first_id: u64,
    last_id: u64,
) -> Result<(), String> {
    let before = book.last_id();
    book.apply_update(&bids, &asks, Some(first_id), Some(last_id));
    let after = book.last_id();
    if let Some(current) = before {
        let expected = current + 1;
        if after == before && last_id < expected {
            return Ok(());
        }
        if after == before && first_id != expected {
            return Err(format!(
                "gate obu sequence gap: last={} expected={} first={} last_id={}",
                current, expected, first_id, last_id
            ));
        }
    }
    tx.send(MarketEvent::GateUpdate {
        bids,
        asks,
        first_id,
        last_id,
    })
    .map_err(|err| err.to_string())
}

fn gate_contract_qty_to_base(contract: &str, quantity: f64) -> f64 {
    if contract.to_ascii_uppercase().starts_with("BTC_") {
        quantity * 0.0001
    } else {
        quantity
    }
}

fn extract_lighter_levels(message: &str) -> Result<(Vec<Level>, Vec<Level>), String> {
    let value: Value = serde_json::from_str(message).map_err(|err| err.to_string())?;
    let book = value
        .get("order_book")
        .ok_or_else(|| "Lighter message missing order_book".to_string())?;
    Ok((value_levels(book, "bids")?, value_levels(book, "asks")?))
}

fn is_gate_obu_update_text(message: &str) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(message) else {
        return false;
    };
    value.get("channel").and_then(Value::as_str) == Some("futures.obu")
        && value.get("event").and_then(Value::as_str) == Some("update")
        && value
            .get("result")
            .and_then(|result| result.get("s"))
            .is_some()
}

fn is_gate_obu_subscribe_ack(message: &str) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(message) else {
        return false;
    };
    value.get("channel").and_then(Value::as_str) == Some("futures.obu")
        && value.get("event").and_then(Value::as_str) == Some("subscribe")
        && value
            .get("result")
            .and_then(|result| result.get("status"))
            .and_then(Value::as_str)
            == Some("success")
}

fn extract_gate_obu_update(
    message: &str,
) -> Result<(Vec<Level>, Vec<Level>, u64, u64, String, bool), String> {
    let value: Value = serde_json::from_str(message).map_err(|err| err.to_string())?;
    let result = value
        .get("result")
        .ok_or_else(|| "Gate OBU message missing result".to_string())?;
    let bids = value_levels(result, "b")?;
    let asks = value_levels(result, "a")?;
    let first_id = result
        .get("U")
        .or_else(|| result.get("u"))
        .and_then(value_to_u64)
        .ok_or_else(|| "Gate OBU message missing update id".to_string())?;
    let last_id = result
        .get("u")
        .and_then(value_to_u64)
        .ok_or_else(|| "Gate OBU message missing end update id".to_string())?;
    let symbol = result
        .get("s")
        .and_then(Value::as_str)
        .ok_or_else(|| "Gate OBU message missing symbol".to_string())?
        .to_string();
    let is_snapshot = result.get("full").and_then(Value::as_bool).unwrap_or(false);
    Ok((bids, asks, first_id, last_id, symbol, is_snapshot))
}

fn gate_obu_stream_matches(stream: &str, contract: &str, depth: usize) -> bool {
    stream == contract || stream == format!("ob.{contract}.{depth}")
}

fn value_to_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|item| u64::try_from(item).ok()))
        .or_else(|| value.as_str().and_then(|item| item.parse::<u64>().ok()))
}

fn value_levels(value: &Value, key: &str) -> Result<Vec<Level>, String> {
    let Some(levels) = value.get(key).and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    let mut output = Vec::with_capacity(levels.len());
    for level in levels {
        let (price, size) = if let Some(items) = level.as_array() {
            (
                value_to_scaled(items.first(), "price")?,
                value_to_scaled(items.get(1), "size")?,
            )
        } else {
            (
                value_to_scaled(level.get("p").or_else(|| level.get("price")), "price")?,
                value_to_scaled(level.get("s").or_else(|| level.get("size")), "size")?,
            )
        };
        output.push(Level { price, size });
    }
    Ok(output)
}

fn value_to_scaled(value: Option<&Value>, field: &str) -> Result<i64, String> {
    let raw = value.ok_or_else(|| format!("missing level {field}"))?;
    if let Some(text) = raw.as_str() {
        return parse_decimal_scaled(text);
    }
    if let Some(number) = raw.as_f64() {
        return Ok(f64_to_scaled(number));
    }
    Err(format!("invalid level {field}"))
}

fn parse_decimal_scaled(value: &str) -> Result<i64, String> {
    let value = value.trim();
    let negative = value.starts_with('-');
    let unsigned = value.trim_start_matches(['-', '+']);
    let mut parts = unsigned.split('.');
    let whole = parts.next().unwrap_or("0");
    let frac = parts.next().unwrap_or("");
    if parts.next().is_some() {
        return Err(format!("invalid decimal: {value}"));
    }
    let mut digits = String::new();
    digits.push_str(if whole.is_empty() { "0" } else { whole });
    let mut frac_padded = frac.to_string();
    if frac_padded.len() > 8 {
        frac_padded.truncate(8);
    }
    while frac_padded.len() < 8 {
        frac_padded.push('0');
    }
    digits.push_str(&frac_padded);
    let parsed = digits.parse::<i64>().map_err(|err| err.to_string())?;
    Ok(if negative { -parsed } else { parsed })
}

fn f64_to_scaled(value: f64) -> i64 {
    (value * 100_000_000.0).round() as i64
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

enum MarketEvent {
    GateSnapshot {
        bids: Vec<Level>,
        asks: Vec<Level>,
        book_id: u64,
    },
    GateUpdate {
        bids: Vec<Level>,
        asks: Vec<Level>,
        first_id: u64,
        last_id: u64,
    },
    LighterSnapshot {
        bids: Vec<Level>,
        asks: Vec<Level>,
        nonce: u64,
    },
    LighterUpdate {
        bids: Vec<Level>,
        asks: Vec<Level>,
        nonce: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{BookStatus, DecimalLevel};

    #[test]
    fn extracts_lighter_levels_from_snapshot_json() {
        let (bids, asks) = extract_lighter_levels(
            r#"{"type":"subscribed/order_book","order_book":{"nonce":10,"bids":[{"price":"99.1","size":"2"}],"asks":[{"price":"101.2","size":"3"}]}}"#,
        )
        .unwrap();

        assert_eq!(
            bids[0],
            Level {
                price: 99_10000000,
                size: 2_00000000
            }
        );
        assert_eq!(
            asks[0],
            Level {
                price: 101_20000000,
                size: 3_00000000
            }
        );
    }

    #[test]
    fn extracts_gate_rest_levels_from_array_or_object_json() {
        let value: Value = serde_json::from_str(
            r#"{"bids":[["99.1","2"],{"p":"99.0","s":"1"}],"asks":[["101.2","3"]]}"#,
        )
        .unwrap();

        let bids = value_levels(&value, "bids").unwrap();
        let asks = value_levels(&value, "asks").unwrap();

        assert_eq!(
            bids[0],
            Level {
                price: 99_10000000,
                size: 2_00000000
            }
        );
        assert_eq!(
            bids[1],
            Level {
                price: 99_00000000,
                size: 1_00000000
            }
        );
        assert_eq!(
            asks[0],
            Level {
                price: 101_20000000,
                size: 3_00000000
            }
        );
    }

    #[test]
    fn gate_obu_stream_matches_v2_stream_name() {
        assert!(gate_obu_stream_matches("ob.BTC_USDT.50", "BTC_USDT", 50));
        assert!(gate_obu_stream_matches("BTC_USDT", "BTC_USDT", 50));
        assert!(!gate_obu_stream_matches("ob.BTC_USDT.400", "BTC_USDT", 50));
    }

    #[test]
    fn gate_bbo_contract_qty_is_normalized_to_base_btc_for_output_only() {
        assert_eq!(gate_contract_qty_to_base("BTC_USDT", 4544.0), 0.4544);
    }

    #[test]
    fn gate_vwap_keeps_existing_depth_units() {
        let mut gate = LocalBook::new();
        let mut lighter = LocalBook::new();
        gate.apply_snapshot(
            &[Level {
                price: 100_00000000,
                size: 2_000_00000000,
            }],
            &[Level {
                price: 101_00000000,
                size: 2_000_00000000,
            }],
            Some(10),
        );
        lighter.apply_snapshot(
            &[Level {
                price: 99_00000000,
                size: 1_00000000,
            }],
            &[Level {
                price: 102_00000000,
                size: 1_00000000,
            }],
            Some(10),
        );

        let (_, gate_buy_qty) = gate.best_ask().unwrap();
        let signal = maybe_vwap_signal_for_test(&gate, &lighter, 20.0).unwrap();

        assert_eq!(gate_buy_qty, 2000.0);
        assert_eq!(signal.0, 100.0);
        assert_eq!(signal.1, 102.0);
    }

    #[test]
    fn empty_gate_obu_delta_advances_sequence_and_keeps_book_ready() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut book = LocalBook::new();
        book.apply_snapshot(
            &[Level {
                price: 100_00000000,
                size: 1_00000000,
            }],
            &[Level {
                price: 100_10000000,
                size: 1_00000000,
            }],
            Some(10),
        );

        apply_gate_update_and_emit(&mut book, &tx, Vec::new(), Vec::new(), 11, 11).unwrap();

        assert_eq!(book.last_id(), Some(11));
        assert_eq!(book.status(), BookStatus::Ready);
        assert!(matches!(
            rx.try_recv().unwrap(),
            MarketEvent::GateUpdate {
                first_id: 11,
                last_id: 11,
                ..
            }
        ));
    }

    #[test]
    fn gate_obu_gap_marks_stale_and_returns_reconnect_error() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut book = LocalBook::new();
        book.apply_snapshot(
            &[Level {
                price: 100_00000000,
                size: 1_00000000,
            }],
            &[Level {
                price: 100_10000000,
                size: 1_00000000,
            }],
            Some(10),
        );

        let err =
            apply_gate_update_and_emit(&mut book, &tx, Vec::new(), Vec::new(), 12, 12).unwrap_err();

        assert!(err.contains("sequence gap"));
        assert_eq!(book.last_id(), Some(10));
        assert_eq!(book.status(), BookStatus::Stale);
    }

    #[test]
    fn gate_obu_overlap_is_rejected_when_first_id_is_not_expected() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut book = LocalBook::new();
        book.apply_snapshot(
            &[Level {
                price: 100_00000000,
                size: 1_00000000,
            }],
            &[Level {
                price: 100_10000000,
                size: 1_00000000,
            }],
            Some(10),
        );

        let err =
            apply_gate_update_and_emit(&mut book, &tx, Vec::new(), Vec::new(), 10, 11).unwrap_err();

        assert!(err.contains("sequence gap"));
        assert_eq!(book.last_id(), Some(10));
        assert_eq!(book.status(), BookStatus::Stale);
    }

    #[test]
    fn gate_sampling_pauses_after_100ms_and_reconnects_after_500ms() {
        let mut gate = LocalBook::new();
        gate.apply_snapshot(
            &[Level {
                price: 100_00000000,
                size: 1_00000000,
            }],
            &[Level {
                price: 100_10000000,
                size: 1_00000000,
            }],
            Some(10),
        );

        assert!(gate_sampling_allowed(&gate, 99));
        assert!(!gate_sampling_allowed(&gate, 100));
        assert!(!gate_reconnect_required(499));
        assert!(gate_reconnect_required(500));
    }

    #[test]
    fn market_event_update_emits_signal_without_waiting_for_sample_tick() {
        let config = LiveShadowConfig {
            ticker: "BTC".to_string(),
            gate_contract: "BTC_USDT".to_string(),
            gate_settle: "usdt".to_string(),
            gate_depth: 50,
            gate_interval: "20ms".to_string(),
            lighter_market_id: 1,
            threshold_bps: 0.0,
            window_size: 3,
            min_samples: 1,
            sample_interval_ms: 1000,
            run_seconds: 0,
            vwap_quote_usd: 100.0,
            gate_sbe_url: "wss://gate".to_string(),
            lighter_ws_url: "wss://lighter".to_string(),
            depth_emit_mode: DepthEmitMode::Always,
        };
        let mut gate = LocalBook::new();
        let mut lighter = LocalBook::new();
        let mut engine = SignalEngine::new(EngineConfig {
            window_size: config.window_size,
            min_samples: config.min_samples,
            threshold_bps: config.threshold_bps,
            ticker: config.ticker.clone(),
            gate_contract: config.gate_contract.clone(),
            lighter_market_id: config.lighter_market_id,
        });
        let mut sequence = 0;
        let mut last_sample_at = None;
        gate.apply_snapshot(
            &[Level {
                price: 100_00000000,
                size: 2_00000000,
            }],
            &[Level {
                price: 101_00000000,
                size: 2_00000000,
            }],
            Some(1),
        );
        lighter.apply_snapshot(
            &[Level {
                price: 100_50000000,
                size: 2_00000000,
            }],
            &[Level {
                price: 102_00000000,
                size: 2_00000000,
            }],
            Some(1),
        );
        let warmup = emit_vwap_signal_for_event(
            &config,
            &mut engine,
            &gate,
            &lighter,
            &mut sequence,
            &mut last_sample_at,
        );
        assert!(warmup.is_some());

        let signal = handle_market_event_for_signal(
            MarketEvent::LighterUpdate {
                bids: vec![Level {
                    price: 101_50000000,
                    size: 2_00000000,
                }],
                asks: vec![],
                nonce: 2,
            },
            &config,
            &mut engine,
            &mut gate,
            &mut lighter,
            &mut sequence,
            &mut Instant::now(),
            &mut Instant::now(),
            &mut last_sample_at,
        );

        let row = signal.expect("event update should emit immediately");
        assert!(row.long_ok);
        assert_eq!(row.sample_count, 1);
        assert_eq!(row.source, "rust_live");
    }

    #[test]
    fn median_sampling_interval_controls_sample_count_not_event_evaluation() {
        let config = LiveShadowConfig {
            ticker: "BTC".to_string(),
            gate_contract: "BTC_USDT".to_string(),
            gate_settle: "usdt".to_string(),
            gate_depth: 50,
            gate_interval: "20ms".to_string(),
            lighter_market_id: 1,
            threshold_bps: 0.0,
            window_size: 3600,
            min_samples: 1,
            sample_interval_ms: 1000,
            run_seconds: 0,
            vwap_quote_usd: 100.0,
            gate_sbe_url: "wss://gate".to_string(),
            lighter_ws_url: "wss://lighter".to_string(),
            depth_emit_mode: DepthEmitMode::Always,
        };
        let mut gate = LocalBook::new();
        let mut lighter = LocalBook::new();
        let mut engine = SignalEngine::new(EngineConfig {
            window_size: config.window_size,
            min_samples: config.min_samples,
            threshold_bps: config.threshold_bps,
            ticker: config.ticker.clone(),
            gate_contract: config.gate_contract.clone(),
            lighter_market_id: config.lighter_market_id,
        });
        let mut sequence = 0;
        let mut last_sample_at = None;

        gate.apply_snapshot(
            &[Level {
                price: 99_00000000,
                size: 20_00000000,
            }],
            &[Level {
                price: 100_00000000,
                size: 20_00000000,
            }],
            Some(1),
        );
        lighter.apply_snapshot(
            &[Level {
                price: 100_00000000,
                size: 20_00000000,
            }],
            &[Level {
                price: 101_00000000,
                size: 20_00000000,
            }],
            Some(1),
        );
        let warmup = emit_vwap_signal_for_event(
            &config,
            &mut engine,
            &gate,
            &lighter,
            &mut sequence,
            &mut last_sample_at,
        )
        .unwrap();
        assert_eq!(warmup.sample_count, 1);

        let event_row = handle_market_event_for_signal(
            MarketEvent::LighterUpdate {
                bids: vec![Level {
                    price: 100_50000000,
                    size: 20_00000000,
                }],
                asks: vec![],
                nonce: 2,
            },
            &config,
            &mut engine,
            &mut gate,
            &mut lighter,
            &mut sequence,
            &mut Instant::now(),
            &mut Instant::now(),
            &mut last_sample_at,
        )
        .unwrap();

        assert_eq!(event_row.sample_count, 1);
        assert!(event_row.long_ok);
    }

    #[test]
    fn signal_only_depth_emit_mode_omits_depth_until_signal() {
        let mut config = LiveShadowConfig {
            ticker: "BTC".to_string(),
            gate_contract: "BTC_USDT".to_string(),
            gate_settle: "usdt".to_string(),
            gate_depth: 50,
            gate_interval: "20ms".to_string(),
            lighter_market_id: 1,
            threshold_bps: 0.0,
            window_size: 3,
            min_samples: 1,
            sample_interval_ms: 1000,
            run_seconds: 0,
            vwap_quote_usd: 100.0,
            gate_sbe_url: "wss://gate".to_string(),
            lighter_ws_url: "wss://lighter".to_string(),
            depth_emit_mode: DepthEmitMode::SignalOnly,
        };
        let gate = LocalBook::new();
        let lighter = LocalBook::new();
        let mut row = SignalRow {
            sequence: 1,
            timestamp_ns: 100,
            source: "rust_live".to_string(),
            ticker: "BTC".to_string(),
            gate_contract: "BTC_USDT".to_string(),
            lighter_market_id: 1,
            ready: true,
            sample_count: 1,
            lighter_bid: 99.0,
            lighter_bid_size: 1.0,
            lighter_ask: 100.0,
            lighter_ask_size: 1.0,
            gate_bid: 101.0,
            gate_bid_size: 1.0,
            gate_ask: 102.0,
            gate_ask_size: 1.0,
            long_spread: 2.0,
            short_spread: 1.0,
            long_median: 0.0,
            short_median: 0.0,
            long_threshold: -1.0,
            short_threshold: 2.0,
            basis: 0.0,
            long_ok: false,
            short_ok: false,
            gate_book_status: BookStatus::Ready,
            lighter_book_status: BookStatus::Ready,
            depth: Some(SignalDepthMetadata {
                gate_bid_levels: vec![DecimalLevel {
                    price: 101.0,
                    size: 1.0,
                }],
                gate_ask_levels: vec![DecimalLevel {
                    price: 102.0,
                    size: 1.0,
                }],
                lighter_bid_levels: vec![DecimalLevel {
                    price: 99.0,
                    size: 1.0,
                }],
                lighter_ask_levels: vec![DecimalLevel {
                    price: 100.0,
                    size: 1.0,
                }],
                gate_bid_fill: None,
                gate_ask_fill: None,
                lighter_bid_fill: None,
                lighter_ask_fill: None,
            }),
        };

        let no_signal_json = live_signal_to_json(&config, &row, Duration::ZERO, &gate, &lighter);
        assert!(!no_signal_json.contains("gate_bid_levels"));

        row.long_ok = true;
        let signal_json = live_signal_to_json(&config, &row, Duration::ZERO, &gate, &lighter);
        assert!(signal_json.contains("gate_bid_levels"));

        config.depth_emit_mode = DepthEmitMode::Never;
        let never_json = live_signal_to_json(&config, &row, Duration::ZERO, &gate, &lighter);
        assert!(!never_json.contains("gate_bid_levels"));
    }

    #[test]
    fn live_hot_signal_json_has_event_field_and_key_signal_fields() {
        let mut gate = LocalBook::new();
        let mut lighter = LocalBook::new();
        gate.apply_snapshot(
            &[Level { price: 101_00000000, size: 1_00000000 }],
            &[Level { price: 102_00000000, size: 1_00000000 }],
            Some(1),
        );
        lighter.apply_snapshot(
            &[Level { price: 99_00000000, size: 1_00000000 }],
            &[Level { price: 100_00000000, size: 1_00000000 }],
            Some(1),
        );
        let config = LiveShadowConfig {
            ticker: "BTC".to_string(),
            gate_contract: "BTC_USDT".to_string(),
            gate_settle: "usdt".to_string(),
            gate_depth: 50,
            gate_interval: "20ms".to_string(),
            lighter_market_id: 1,
            threshold_bps: 2.0,
            window_size: 3600,
            min_samples: 1,
            sample_interval_ms: 1000,
            run_seconds: 0,
            vwap_quote_usd: 100.0,
            gate_sbe_url: "wss://gate".to_string(),
            lighter_ws_url: "wss://lighter".to_string(),
            depth_emit_mode: DepthEmitMode::Always,
        };
        let row = SignalRow {
            sequence: 42,
            timestamp_ns: 123456,
            source: "rust_live".to_string(),
            ticker: "BTC".to_string(),
            gate_contract: "BTC_USDT".to_string(),
            lighter_market_id: 1,
            ready: true,
            sample_count: 100,
            lighter_bid: 99.0,
            lighter_bid_size: 1.0,
            lighter_ask: 100.0,
            lighter_ask_size: 1.0,
            gate_bid: 101.0,
            gate_bid_size: 1.0,
            gate_ask: 102.0,
            gate_ask_size: 1.0,
            long_spread: 3.0,
            short_spread: 1.0,
            long_median: 5.0,
            short_median: -1.0,
            long_threshold: 3.5,
            short_threshold: 0.5,
            basis: 0.2,
            long_ok: true,
            short_ok: false,
            gate_book_status: BookStatus::Ready,
            lighter_book_status: BookStatus::Ready,
            depth: None,
        };

        let json = live_hot_signal_to_json(&config, &row, Duration::from_micros(20), &gate, &lighter);

        assert!(json.contains(r#""event":"hot_signal""#));
        assert!(json.contains(r#""signal_id":"42""#));
        assert!(json.contains(r#""sequence":42"#));
        assert!(json.contains(r#""side":"long_gate_short_lighter""#));
        assert!(json.contains(r#""ready":true"#));
        assert!(json.contains(r#""long_ok":true"#));
        assert!(json.contains(r#""short_ok":false"#));
        assert!(json.contains(r#""rust_calc_ms""#));
        assert!(json.contains(r#""gate_bbo_bid""#));
        assert!(json.contains(r#""lighter_bbo_ask""#));
        assert!(!json.contains("gate_bid_levels"));
        assert!(!json.contains("diagnostic"));
    }

    #[test]
    fn live_hot_signal_short_side_uses_short_gate_long_lighter() {
        let gate = LocalBook::new();
        let lighter = LocalBook::new();
        let config = LiveShadowConfig {
            ticker: "BTC".to_string(),
            gate_contract: "BTC_USDT".to_string(),
            gate_settle: "usdt".to_string(),
            gate_depth: 50,
            gate_interval: "20ms".to_string(),
            lighter_market_id: 1,
            threshold_bps: 2.0,
            window_size: 3600,
            min_samples: 1,
            sample_interval_ms: 1000,
            run_seconds: 0,
            vwap_quote_usd: 100.0,
            gate_sbe_url: "wss://gate".to_string(),
            lighter_ws_url: "wss://lighter".to_string(),
            depth_emit_mode: DepthEmitMode::Always,
        };
        let row = SignalRow {
            sequence: 7,
            timestamp_ns: 0,
            source: "rust_live".to_string(),
            ticker: "BTC".to_string(),
            gate_contract: "BTC_USDT".to_string(),
            lighter_market_id: 1,
            ready: true,
            sample_count: 1,
            lighter_bid: 99.0, lighter_bid_size: 1.0,
            lighter_ask: 100.0, lighter_ask_size: 1.0,
            gate_bid: 101.0, gate_bid_size: 1.0,
            gate_ask: 102.0, gate_ask_size: 1.0,
            long_spread: 3.0, short_spread: 1.0,
            long_median: 5.0, short_median: -1.0,
            long_threshold: 3.5, short_threshold: 0.5,
            basis: 0.2,
            long_ok: false,
            short_ok: true,
            gate_book_status: BookStatus::Ready,
            lighter_book_status: BookStatus::Ready,
            depth: None,
        };

        let json = live_hot_signal_to_json(&config, &row, Duration::ZERO, &gate, &lighter);

        assert!(json.contains(r#""side":"short_gate_long_lighter""#));
    }

    #[test]
    fn live_diagnostic_json_has_event_and_depth_fields() {
        let row = SignalRow {
            sequence: 99,
            timestamp_ns: 0,
            source: "rust_live".to_string(),
            ticker: "BTC".to_string(),
            gate_contract: "BTC_USDT".to_string(),
            lighter_market_id: 1,
            ready: true,
            sample_count: 1,
            lighter_bid: 99.0, lighter_bid_size: 1.0,
            lighter_ask: 100.0, lighter_ask_size: 1.0,
            gate_bid: 101.0, gate_bid_size: 1.0,
            gate_ask: 102.0, gate_ask_size: 1.0,
            long_spread: 3.0, short_spread: 1.0,
            long_median: 5.0, short_median: -1.0,
            long_threshold: 3.5, short_threshold: 0.5,
            basis: 0.2,
            long_ok: true,
            short_ok: false,
            gate_book_status: BookStatus::Ready,
            lighter_book_status: BookStatus::Ready,
            depth: Some(SignalDepthMetadata {
                gate_bid_levels: vec![DecimalLevel { price: 101.0, size: 2.0 }],
                gate_ask_levels: vec![DecimalLevel { price: 102.0, size: 3.0 }],
                lighter_bid_levels: vec![DecimalLevel { price: 99.0, size: 4.0 }],
                lighter_ask_levels: vec![DecimalLevel { price: 100.0, size: 5.0 }],
                gate_bid_fill: None,
                gate_ask_fill: None,
                lighter_bid_fill: None,
                lighter_ask_fill: None,
            }),
        };

        let json = live_diagnostic_to_json("99", &row);

        assert!(json.contains(r#""event":"diagnostic_snapshot""#));
        assert!(json.contains(r#""signal_id":"99""#));
        assert!(json.contains("gate_bid_levels"));
        assert!(json.contains("lighter_ask_levels"));
        assert!(json.contains(r#"["101.00000000","2.00000000"]"#));
    }

    #[test]
    fn live_diagnostic_json_is_empty_when_no_depth() {
        let row = SignalRow {
            sequence: 5,
            timestamp_ns: 0,
            source: "rust_live".to_string(),
            ticker: "BTC".to_string(),
            gate_contract: "BTC_USDT".to_string(),
            lighter_market_id: 1,
            ready: true,
            sample_count: 1,
            lighter_bid: 99.0, lighter_bid_size: 1.0,
            lighter_ask: 100.0, lighter_ask_size: 1.0,
            gate_bid: 101.0, gate_bid_size: 1.0,
            gate_ask: 102.0, gate_ask_size: 1.0,
            long_spread: 3.0, short_spread: 1.0,
            long_median: 0.0, short_median: 0.0,
            long_threshold: 0.0, short_threshold: 0.0,
            basis: 0.0,
            long_ok: true,
            short_ok: false,
            gate_book_status: BookStatus::Ready,
            lighter_book_status: BookStatus::Ready,
            depth: None,
        };

        assert_eq!(live_diagnostic_to_json("5", &row), "");
    }
}
