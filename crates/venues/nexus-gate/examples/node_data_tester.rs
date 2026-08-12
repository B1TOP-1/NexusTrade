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

//! Example demonstrating live public futures data testing with the Gate adapter.
//!
//! Run with:
//! `cargo run --example gate-data-tester --package nautilus-gate -- --contract BTC_USDT --depth 50 --run-seconds 30`

use std::{env, num::NonZeroUsize, time::Duration};

use nautilus_common::messages::data::SubscribeQuotes;
use nautilus_common::{
    clients::DataClient, live::runner::replace_data_event_sender,
    messages::data::SubscribeBookDeltas,
};
use nautilus_core::{UUID4, time::get_atomic_clock_realtime};
use nexus_gate::{
    common::consts::GATE,
    config::GateDataClientConfig,
    data::{GateDataClient, GateDataClientStats},
};
use nautilus_model::{
    enums::BookType,
    identifiers::{ClientId, InstrumentId},
};

#[derive(Debug, Clone)]
struct ExampleArgs {
    contract: String,
    depth: u32,
    run_seconds: u64,
}

impl Default for ExampleArgs {
    fn default() -> Self {
        Self {
            contract: "BTC_USDT".to_string(),
            depth: 50,
            run_seconds: 30,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = ExampleArgs::parse()?;
    let instrument_id = InstrumentId::from(format!("{}.{}", args.contract, GATE));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(tx);

    let gate_config = GateDataClientConfig {
        depth: args.depth,
        ..Default::default()
    };
    let mut client = GateDataClient::new(ClientId::from(GATE), gate_config)?;

    println!(
        "Starting Gate public data soak: contract={}, depth={}, run_seconds={}",
        args.contract, args.depth, args.run_seconds
    );

    let drain_task = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    client.connect().await?;
    let ts_init = get_atomic_clock_realtime().get_time_ns();
    client.subscribe_book_deltas(SubscribeBookDeltas::new(
        instrument_id,
        BookType::L2_MBP,
        Some(ClientId::from(GATE)),
        None,
        UUID4::new(),
        ts_init,
        NonZeroUsize::new(args.depth as usize),
        false,
        None,
        None,
    ))?;
    client.subscribe_quotes(SubscribeQuotes::new(
        instrument_id,
        Some(ClientId::from(GATE)),
        None,
        UUID4::new(),
        ts_init,
        None,
        None,
    ))?;

    tokio::time::sleep(Duration::from_secs(args.run_seconds)).await;
    let stats = client.stats();
    client.disconnect().await?;
    drop(client);
    let _ = tokio::time::timeout(Duration::from_secs(2), drain_task).await;

    print_stats(stats);

    Ok(())
}

impl ExampleArgs {
    fn parse() -> anyhow::Result<Self> {
        let mut parsed = Self::default();
        let mut args = env::args().skip(1);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--contract" => {
                    parsed.contract = next_value(&mut args, "--contract")?;
                }
                "--depth" => {
                    parsed.depth = next_value(&mut args, "--depth")?.parse()?;
                    if parsed.depth == 0 {
                        anyhow::bail!("--depth must be greater than zero");
                    }
                }
                "--run-seconds" => {
                    parsed.run_seconds = next_value(&mut args, "--run-seconds")?.parse()?;
                }
                "--help" | "-h" => {
                    println!(
                        "Usage: cargo run --example gate-data-tester --package nautilus-gate -- [--contract BTC_USDT] [--depth 50] [--run-seconds 30]",
                    );
                    std::process::exit(0);
                }
                value => anyhow::bail!("unknown argument: {value}"),
            }
        }

        Ok(parsed)
    }
}

fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> anyhow::Result<String> {
    args.next()
        .ok_or_else(|| anyhow::anyhow!("missing value for {flag}"))
}

fn print_stats(stats: GateDataClientStats) {
    println!("Gate public data soak stats:");
    println!("  delta_count={}", stats.delta_count);
    println!("  quote_count={}", stats.quote_count);
    println!("  snapshot_count={}", stats.snapshot_count);
    println!("  no_op_count={}", stats.no_op_count);
    println!("  duplicate_or_old_count={}", stats.duplicate_or_old_count);
    println!("  gap_count={}", stats.gap_count);
    println!("  resubscribe_count={}", stats.resubscribe_count);
    println!("  reconnect_count={}", stats.reconnect_count);
    println!("  max_stale_duration_ms={}", stats.max_stale_duration_ms);
}
