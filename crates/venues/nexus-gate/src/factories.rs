use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::{DataClient, ExecutionClient},
    clock::Clock,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::ClientId,
};

use crate::{
    common::consts::{GATE, GATE_VENUE},
    config::{GateDataClientConfig, GateExecutionClientConfig},
    data::GateDataClient,
    execution_client::GateExecutionClient,
};

impl ClientConfig for GateDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ClientConfig for GateExecutionClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct GateDataClientFactory;

impl GateDataClientFactory {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl DataClientFactory for GateDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let gate_config = config
            .as_any()
            .downcast_ref::<GateDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for GateDataClientFactory. Expected GateDataClientConfig, was {config:?}",
                )
            })?
            .clone();
        Ok(Box::new(GateDataClient::new(
            ClientId::from(name),
            gate_config,
        )?))
    }

    fn name(&self) -> &str {
        GATE
    }

    fn config_type(&self) -> &'static str {
        "GateDataClientConfig"
    }
}

#[derive(Debug, Clone, Default)]
pub struct GateExecutionClientFactory;

impl GateExecutionClientFactory {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl ExecutionClientFactory for GateExecutionClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let gate_config = config
            .as_any()
            .downcast_ref::<GateExecutionClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for GateExecutionClientFactory. Expected GateExecutionClientConfig, was {config:?}",
                )
            })?
            .clone();

        let core = ExecutionClientCore::new(
            gate_config.trader_id,
            ClientId::from(name),
            *GATE_VENUE,
            OmsType::Netting,
            gate_config.account_id,
            AccountType::Margin,
            None,
            cache,
        );

        Ok(Box::new(GateExecutionClient::new(core, gate_config)))
    }

    fn name(&self) -> &str {
        GATE
    }

    fn config_type(&self) -> &'static str {
        "GateExecutionClientConfig"
    }
}
