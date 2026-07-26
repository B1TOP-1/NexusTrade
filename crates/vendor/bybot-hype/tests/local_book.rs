use bybot_hype::{
    local_book::{LocalBookConfig, LocalBookError, LocalOrderBookModule},
    orderbook::{BookLevel, BookState, SnapshotInput, StaleReason},
};

fn level(price: i64, size: i64) -> BookLevel {
    BookLevel::new(price, size, 1)
}

fn snapshot(exchange_time_ms: u64, received_time_ms: u64) -> SnapshotInput {
    SnapshotInput::new(
        exchange_time_ms,
        received_time_ms,
        vec![
            level(100_00000000, 2_00000000),
            level(99_90000000, 3_00000000),
        ],
        vec![
            level(100_10000000, 2_00000000),
            level(100_20000000, 3_00000000),
        ],
    )
}

#[test]
fn module_registers_multiple_markets_and_exposes_sorted_symbols() {
    let module =
        LocalOrderBookModule::new(["xyz:SPCX", "BTC"], LocalBookConfig::new(3_000)).unwrap();

    assert_eq!(module.symbols(), vec!["BTC", "xyz:SPCX"]);
    assert_eq!(module.len(), 2);
    assert!(!module.is_empty());
}

#[test]
fn module_rejects_invalid_configuration_and_unknown_markets() {
    assert!(matches!(
        LocalOrderBookModule::new(Vec::<String>::new(), LocalBookConfig::new(3_000)),
        Err(LocalBookError::NoSymbols)
    ));
    assert!(matches!(
        LocalOrderBookModule::new(["BTC", "BTC"], LocalBookConfig::new(3_000)),
        Err(LocalBookError::DuplicateSymbol(symbol)) if symbol == "BTC"
    ));

    let mut module = LocalOrderBookModule::new(["BTC"], LocalBookConfig::new(3_000)).unwrap();
    assert_eq!(
        module.apply_snapshot("ETH", snapshot(100, 110)),
        Err(LocalBookError::UnknownSymbol("ETH".to_string()))
    );
}

#[test]
fn accepted_snapshot_opens_trade_gate_and_supports_execution_estimates() {
    let mut module = LocalOrderBookModule::new(["BTC"], LocalBookConfig::new(3_000)).unwrap();
    module.mark_connected();
    module.apply_snapshot("BTC", snapshot(100, 110)).unwrap();

    let top = module.top_of_book("BTC", 1_000).unwrap();
    assert_eq!(top.best_bid().price(), 100_00000000);
    assert_eq!(top.best_ask().price(), 100_10000000);

    let buy = module.estimate_buy("BTC", 4_00000000, 1_000).unwrap();
    assert!(buy.is_complete());
    assert_eq!(buy.levels_used(), 2);

    let sell = module.estimate_sell("BTC", 4_00000000, 1_000).unwrap();
    assert!(sell.is_complete());
    assert_eq!(sell.levels_used(), 2);
}

#[test]
fn stale_or_disconnected_books_cannot_be_used_for_execution() {
    let mut module = LocalOrderBookModule::new(["BTC"], LocalBookConfig::new(1_000)).unwrap();
    module.apply_snapshot("BTC", snapshot(100, 110)).unwrap();

    assert!(matches!(
        module.top_of_book("BTC", 1_111),
        Err(LocalBookError::NotTradeable {
            state: BookState::Stale(StaleReason::Timeout),
            ..
        })
    ));
    assert!(matches!(
        module.estimate_buy("BTC", 1_00000000, 1_111),
        Err(LocalBookError::NotTradeable { .. })
    ));

    module.mark_disconnected();
    let disconnected = module.snapshot("BTC").unwrap();
    assert_eq!(disconnected.state(), BookState::Disconnected);
}

#[test]
fn reconnect_requires_a_new_snapshot_before_trading_resumes() {
    let mut module = LocalOrderBookModule::new(["BTC"], LocalBookConfig::new(3_000)).unwrap();
    module.apply_snapshot("BTC", snapshot(100, 110)).unwrap();
    module.mark_disconnected();
    module.mark_connected();

    assert!(matches!(
        module.top_of_book("BTC", 120),
        Err(LocalBookError::NotTradeable {
            state: BookState::WaitingSnapshot,
            ..
        })
    ));

    module.apply_snapshot("BTC", snapshot(101, 130)).unwrap();
    assert!(module.top_of_book("BTC", 131).is_ok());
}

#[test]
fn invalid_stale_timeout_and_blank_symbols_are_rejected() {
    assert!(matches!(
        LocalOrderBookModule::new(["BTC"], LocalBookConfig::new(0)),
        Err(LocalBookError::InvalidStaleAfter)
    ));
    assert!(matches!(
        LocalOrderBookModule::new(["  "], LocalBookConfig::new(3_000)),
        Err(LocalBookError::EmptySymbol)
    ));
}
