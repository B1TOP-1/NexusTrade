// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Gate execution connect/login/subscribe smoke test (NO order placement).
//!
//! Verifies P4b: WS connect -> `futures.login` -> read account uid -> subscribe
//! private channels. Logs private frames (inbound task, debug) for a few seconds.
//!
//! ```text
//! cargo run --example gate-exec-tester --package nautilus-gate -- --contract BTC_USDT --run-seconds 12
//! ```
//! Credentials from `.env`: GATE_API_KEY, GATE_API_SECRET.

use std::{cell::RefCell, collections::HashMap, env, fs, rc::Rc, time::Duration};

use nautilus_common::{cache::Cache, clients::ExecutionClient};
use nexus_gate::{config::GateExecutionClientConfig, execution_client::GateExecutionClient};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::{AccountId, ClientId, TraderId, Venue},
};

struct StderrLogger;
impl log::Log for StderrLogger {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }
    fn log(&self, record: &log::Record) {
        eprintln!("[{}] {}", record.level(), record.args());
    }
    fn flush(&self) {}
}
static LOGGER: StderrLogger = StderrLogger;

fn load_env_file(path: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let Ok(content) = fs::read_to_string(path) else {
        return map;
    };
    for line in content.lines() {
        let line = line.trim().strip_prefix("export ").unwrap_or(line.trim());
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            map.insert(
                k.trim().to_string(),
                v.trim().trim_matches('"').trim_matches('\'').to_string(),
            );
        }
    }
    map
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = log::set_logger(&LOGGER);
    log::set_max_level(log::LevelFilter::Debug);

    let argv: Vec<String> = env::args().collect();
    let mut contract = "BTC_USDT".to_string();
    let mut run_seconds: u64 = 12;
    let mut env_file = ".env".to_string();
    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--contract" => {
                contract = argv[i + 1].clone();
                i += 2;
            }
            "--run-seconds" => {
                run_seconds = argv[i + 1].parse()?;
                i += 2;
            }
            "--env-file" => {
                env_file = argv[i + 1].clone();
                i += 2;
            }
            other => anyhow::bail!("unknown arg: {other}"),
        }
    }

    let env_map = load_env_file(&env_file);
    let get = |k: &str| -> anyhow::Result<String> {
        env_map
            .get(k)
            .cloned()
            .or_else(|| env::var(k).ok())
            .ok_or_else(|| anyhow::anyhow!("missing env key {k}"))
    };
    let api_key = get("GATE_API_KEY")?;
    let api_secret = get("GATE_API_SECRET")?;

    let trader_id = TraderId::from("GATE-TESTER-001");
    let account_id = AccountId::from("GATE-001");
    let config = GateExecutionClientConfig {
        trader_id,
        account_id,
        api_key: Some(api_key),
        api_secret: Some(api_secret),
        contracts: vec![contract.clone()],
        ..Default::default()
    };

    let core = ExecutionClientCore::new(
        trader_id,
        ClientId::from("GATE"),
        Venue::from("GATE"),
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None,
        Rc::new(RefCell::new(Cache::default())),
    );
    let mut client = GateExecutionClient::new(core, config);

    println!("Gate exec smoke test: connect -> login -> subscribe ({contract}); NO orders ...");
    client.start()?;
    client.connect().await?;
    println!("Connected. Watching private frames for {run_seconds}s ...");
    tokio::time::sleep(Duration::from_secs(run_seconds)).await;
    client.disconnect().await?;
    println!("Done.");
    Ok(())
}
