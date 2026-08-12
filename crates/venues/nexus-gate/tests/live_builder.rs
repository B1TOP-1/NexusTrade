use nautilus_common::enums::Environment;
use nexus_gate::{config::GateDataClientConfig, factories::GateDataClientFactory};
use nautilus_live::node::LiveNode;
use nautilus_model::identifiers::TraderId;

#[test]
fn gate_data_client_factory_builds_live_node() {
    let result = LiveNode::builder(TraderId::from("TESTER-001"), Environment::Live)
        .unwrap()
        .add_data_client(
            None,
            Box::new(GateDataClientFactory::new()),
            Box::new(GateDataClientConfig::default()),
        )
        .unwrap()
        .build();

    assert!(result.is_ok());
}
