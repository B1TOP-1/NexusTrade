use std::env;
use std::io;
use std::path::Path;

use bybot_market_engine::live_lighter::{
    run_live_lighter, LiveLighterConfig, MAINNET_READONLY_WS_URL,
};
use bybot_market_engine::live_shadow::{run_live_shadow, DepthEmitMode, LiveShadowConfig};
use bybot_market_engine::replay::{replay_jsonl, signal_to_json, stream_jsonl};

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let command = args.next().ok_or_else(|| "missing command".to_string())?;
    if command == "--stdin-jsonl" {
        if args.next().is_some() {
            return Err("too many arguments".to_string());
        }
        let stdin = io::stdin();
        let mut stdout = io::stdout();
        return stream_jsonl(stdin.lock(), &mut stdout);
    }
    if command == "--live-shadow" {
        let config = parse_live_shadow_config(args.collect::<Vec<_>>())?;
        let runtime = tokio::runtime::Runtime::new().map_err(|err| err.to_string())?;
        return runtime.block_on(run_live_shadow(config));
    }
    if command == "--live-lighter-book" {
        let config = parse_live_lighter_config(args.collect::<Vec<_>>())?;
        let runtime = tokio::runtime::Runtime::new().map_err(|err| err.to_string())?;
        return runtime.block_on(run_live_lighter(config));
    }
    if command != "--replay-jsonl" {
        return Err(format!("unsupported command: {command}"));
    }
    let path = args
        .next()
        .ok_or_else(|| "missing replay path".to_string())?;
    if args.next().is_some() {
        return Err("too many arguments".to_string());
    }

    let rows = replay_jsonl(Path::new(&path))?;
    for row in rows {
        println!("{}", signal_to_json(&row));
    }
    Ok(())
}

fn parse_live_lighter_config(args: Vec<String>) -> Result<LiveLighterConfig, String> {
    let mut config = LiveLighterConfig {
        ticker: "BTC".to_string(),
        market_id: 1,
        ws_url: MAINNET_READONLY_WS_URL.to_string(),
        reconnect_delay_ms: 1000,
        heartbeat_interval_ms: 20_000,
        depth_notional_usd: 2_000.0,
    };
    let mut index = 0usize;
    while index < args.len() {
        let key = &args[index];
        let value = args
            .get(index + 1)
            .ok_or_else(|| format!("missing value for {key}"))?;
        match key.as_str() {
            "--ticker" => config.ticker = value.to_uppercase(),
            "--market-id" => config.market_id = parse_value(value, key)?,
            "--ws-url" => config.ws_url = value.to_string(),
            "--reconnect-delay-ms" => config.reconnect_delay_ms = parse_value(value, key)?,
            "--heartbeat-interval-ms" => config.heartbeat_interval_ms = parse_value(value, key)?,
            "--depth-notional-usd" => config.depth_notional_usd = parse_value(value, key)?,
            _ => return Err(format!("unsupported live-lighter-book argument: {key}")),
        }
        index += 2;
    }
    Ok(config)
}

fn parse_live_shadow_config(args: Vec<String>) -> Result<LiveShadowConfig, String> {
    let mut config = LiveShadowConfig {
        ticker: "BTC".to_string(),
        gate_contract: "BTC_USDT".to_string(),
        gate_settle: "usdt".to_string(),
        gate_depth: 50,
        gate_interval: "20ms".to_string(),
        lighter_market_id: 1,
        threshold_bps: 1.5,
        window_size: 3600,
        min_samples: 1000,
        sample_interval_ms: 1000,
        run_seconds: 120,
        vwap_quote_usd: 4000.0,
        gate_sbe_url: "wss://fx-ws.gateio.ws/v4/ws/usdt/sbe".to_string(),
        lighter_ws_url: MAINNET_READONLY_WS_URL.to_string(),
        depth_emit_mode: DepthEmitMode::Always,
    };
    let mut index = 0usize;
    while index < args.len() {
        let key = &args[index];
        let value = args
            .get(index + 1)
            .ok_or_else(|| format!("missing value for {key}"))?;
        match key.as_str() {
            "--ticker" => {
                config.ticker = value.to_uppercase();
                config.gate_contract = format!("{}_USDT", config.ticker);
            }
            "--gate-contract" => config.gate_contract = value.to_uppercase(),
            "--gate-settle" => config.gate_settle = value.to_lowercase(),
            "--gate-depth" => config.gate_depth = parse_value(value, key)?,
            "--gate-interval" => config.gate_interval = value.to_string(),
            "--lighter-market-id" => config.lighter_market_id = parse_value(value, key)?,
            "--threshold-bps" => config.threshold_bps = parse_value(value, key)?,
            "--window-size" => config.window_size = parse_value(value, key)?,
            "--min-samples" => config.min_samples = parse_value(value, key)?,
            "--sample-interval-ms" => config.sample_interval_ms = parse_value(value, key)?,
            "--run-seconds" => config.run_seconds = parse_value(value, key)?,
            "--vwap-quote-usd" => config.vwap_quote_usd = parse_value(value, key)?,
            "--gate-sbe-url" => config.gate_sbe_url = value.to_string(),
            "--lighter-ws-url" => config.lighter_ws_url = value.to_string(),
            "--depth-emit-mode" => config.depth_emit_mode = DepthEmitMode::parse(value)?,
            _ => return Err(format!("unsupported live-shadow argument: {key}")),
        }
        index += 2;
    }
    Ok(config)
}

fn parse_value<T: std::str::FromStr>(value: &str, key: &str) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    value
        .parse::<T>()
        .map_err(|err| format!("invalid {key}: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_lighter_book_defaults_to_readonly_websocket() {
        let config = parse_live_lighter_config(Vec::new()).unwrap();

        assert_eq!(
            config.ws_url,
            "wss://mainnet.zklighter.elliot.ai/stream?readonly=true"
        );
    }
}
