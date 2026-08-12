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

//! Gate LiveNode + Strategy market round-trip (buy 1 contract -> hold -> sell 1).
//!
//! Verifies the full Gate execution path: data -> strategy -> WS order_place ->
//! ack/usertrades -> OrderEvents -> Cache. Latency is logged by the adapter
//! (`[Gate 延迟]` line: local round-trip + Gate x_in_time/x_out_time) plus the
//! strategy's submit->fill timing.
//!
//! ```text
//! cargo run --example gate-live-node --package nautilus-gate -- --contract BTC_USDT --hold-seconds 2 --run-seconds 20 --armed
//! ```
//! Credentials from `.env`: GATE_API_KEY, GATE_API_SECRET. `--armed` places REAL orders.

use std::{collections::HashMap, env, fs, time::{Duration, Instant}};

use nautilus_common::{actor::DataActor, enums::Environment, logging::logger::LoggerConfig};
use nexus_gate::{
    config::{GateDataClientConfig, GateExecutionClientConfig},
    factories::{GateDataClientFactory, GateExecutionClientFactory},
};
use nautilus_live::node::LiveNode;
use nautilus_model::{
    data::QuoteTick,
    enums::{OrderSide, TimeInForce},
    events::{OrderAccepted, OrderFilled, OrderRejected, OrderSubmitted},
    identifiers::{AccountId, InstrumentId, TraderId},
    types::Quantity,
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};

pub struct GateRoundTrip {
    core: StrategyCore,
    instrument_id: InstrumentId,
    size: Quantity,
    hold: Duration,
    armed: bool,
    bought: bool,
    sold: bool,
    buy_at: Option<Instant>,
}

impl GateRoundTrip {
    #[must_use]
    pub fn new(instrument_id: InstrumentId, size: Quantity, hold: Duration, armed: bool) -> Self {
        Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: None,
                order_id_tag: Some("GATE-RT".to_string()),
                ..Default::default()
            }),
            instrument_id,
            size,
            hold,
            armed,
            bought: false,
            sold: false,
            buy_at: None,
        }
    }

    fn market(&mut self, side: OrderSide, reduce_only: bool) -> anyhow::Result<()> {
        // Gate market orders are IOC at price 0.
        let order = self.core.order_factory().market(
            self.instrument_id,
            side,
            self.size,
            Some(TimeInForce::Ioc),
            Some(reduce_only),
            None, // quote_quantity
            None, // display_qty
            None, // expire_time
            None, // emulation_trigger
            None, // tags
        );
        let side_cn = if side == OrderSide::Buy { "买入" } else { "卖出" };
        log::info!("提交市价{side_cn} {} 张 (只减仓={reduce_only})", self.size);
        self.submit_order(order, None, None, None)
    }
}

nautilus_strategy!(GateRoundTrip, {
    fn on_order_submitted(&mut self, event: OrderSubmitted) {
        log::info!("[状态] 订单已提交 coid={}", event.client_order_id);
    }
    fn on_order_accepted(&mut self, event: OrderAccepted) {
        log::info!(
            "[状态] 订单已接受 coid={} 交易所单号={}",
            event.client_order_id,
            event.venue_order_id
        );
    }
    fn on_order_rejected(&mut self, event: OrderRejected) {
        log::info!(
            "[状态] 订单被拒 coid={} 原因={}",
            event.client_order_id,
            event.reason
        );
    }
});

impl std::fmt::Debug for GateRoundTrip {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GateRoundTrip")
            .field("instrument_id", &self.instrument_id)
            .field("armed", &self.armed)
            .finish()
    }
}

impl DataActor for GateRoundTrip {
    fn on_start(&mut self) -> anyhow::Result<()> {
        log::info!("策略 on_start: 订阅 {} 行情", self.instrument_id);
        self.subscribe_quotes(self.instrument_id, None, None);
        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn on_quote(&mut self, _quote: &QuoteTick) -> anyhow::Result<()> {
        if !self.armed {
            return Ok(());
        }
        if !self.bought {
            self.bought = true;
            self.buy_at = Some(Instant::now());
            self.market(OrderSide::Buy, false)?;
        } else if !self.sold
            && self.buy_at.is_some_and(|t| t.elapsed() >= self.hold)
        {
            self.sold = true;
            self.market(OrderSide::Sell, true)?;
        }
        Ok(())
    }

    fn on_order_filled(&mut self, event: &OrderFilled) -> anyhow::Result<()> {
        log::info!(
            "[状态] 订单已成交 coid={} 成交量={} 成交价={} 流动性={:?} 手续费={:?}",
            event.client_order_id,
            event.last_qty,
            event.last_px,
            event.liquidity_side,
            event.commission,
        );
        Ok(())
    }
}

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
    let argv: Vec<String> = env::args().collect();
    let mut contract = "BTC_USDT".to_string();
    let mut run_seconds: u64 = 20;
    let mut hold_seconds: u64 = 2;
    let mut armed = false;
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
            "--hold-seconds" => {
                hold_seconds = argv[i + 1].parse()?;
                i += 2;
            }
            "--env-file" => {
                env_file = argv[i + 1].clone();
                i += 2;
            }
            "--armed" => {
                armed = true;
                i += 1;
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

    let trader_id = TraderId::from("GATE-NODE-001");
    let account_id = AccountId::from("GATE-001");
    let instrument_id = InstrumentId::from(format!("{contract}.GATE").as_str());

    let data_config = GateDataClientConfig {
        api_key: Some(api_key.clone()),
        api_secret: Some(api_secret.clone()),
        ..Default::default()
    };
    let exec_config = GateExecutionClientConfig {
        trader_id,
        account_id,
        api_key: Some(api_key),
        api_secret: Some(api_secret),
        contracts: vec![contract.clone()],
        ..Default::default()
    };

    let strategy = GateRoundTrip::new(
        instrument_id,
        Quantity::from(1), // 1 contract (张)
        Duration::from_secs(hold_seconds),
        armed,
    );

    // Quiet framework noise; keep adapter + strategy info.
    let mut component_level = ahash::AHashMap::default();
    for noisy in [
        "LiveNode",
        "nautilus_common::actor::data_actor",
        "nautilus_portfolio::portfolio",
        "nautilus_risk::engine",
        "nautilus_system::trader",
        "nautilus_system::kernel",
        "nautilus_live::builder",
        "nautilus_live::node",
        "nautilus_live::manager",
        "nautilus_execution::engine",
        "nautilus_execution::order_manager::manager",
        "nautilus_trading::strategy",
        "GateRoundTrip-GATE-RT-GATE-RT", // strategy-id lifecycle (Ready/Starting/...)
    ] {
        component_level.insert(ustr::Ustr::from(noisy), log::LevelFilter::Warn);
    }
    // Info shows strategy events + [Gate 延迟]; bump to Debug to see raw private frames.
    let logging = LoggerConfig {
        stdout_level: log::LevelFilter::Info,
        component_level,
        ..Default::default()
    };

    println!("构建 Gate LiveNode (armed={armed}) ...");
    let mut node = LiveNode::builder(trader_id, Environment::Live)?
        .with_logging(logging)
        .add_data_client(None, Box::new(GateDataClientFactory::new()), Box::new(data_config))?
        .add_exec_client(
            None,
            Box::new(GateExecutionClientFactory::new()),
            Box::new(exec_config),
        )?
        .build()?;
    node.add_strategy(strategy)?;

    println!("启动节点事件循环; 运行 {run_seconds} 秒 ...");
    let handle = node.handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(run_seconds)).await;
        handle.stop();
    });
    node.run().await?;
    println!("完成。");
    Ok(())
}
