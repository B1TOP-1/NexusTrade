# Hype

Reusable Rust modules for Hyperliquid market data and execution.

## Module architecture

- `account.rs`: resolves regular users, API agents, and explicit vault execution accounts.
- `markets.rs`: loads main perpetuals or selected HIP-3 DEX markets, exposes asset indexes, precision, midpoint queries, and price rounding.
- `precision.rs`: owns quantity steps, minimum-notional sizing, and deterministic floor rounding.
- `fees_funding.rs`: queries account fee rates, rate-limit state, market funding history, user funding records, and asset contexts.
- `positions.rs`: queries clearinghouse state, actual positions, open orders, historical orders, order status, and fills.
- `orders.rs`: exchange-independent order intent, side, reduce-only flag, and execution policy.
- `order_gateway.rs`: SDK signing and submission for place, cancel by OID, cancel by CLOID, and modify operations.
- `order_state.rs`: deterministic order lifecycle and terminal-state protection.
- `user_stream.rs`: independent WebSocket reader for Fast5 books, order updates, fills, positions, funding subscriptions, reconnects, and confirmations.
- `execution.rs`: execution facade and actual-position-based emergency flattening.
- `latency.rs`: separate transport, order confirmation, fill confirmation, and cancellation timing.
- `execution_smoke.rs`: live scenario orchestration only; it no longer implements exchange business logic.

The project depends on `hypersdk` 0.2.14 instead of copying the complete upstream repository. Required HyperCore functionality is absorbed behind project-owned modules. This avoids importing unrelated HyperEVM, cloud signer, and multisig surfaces while preserving an explicit migration boundary for a future WebSocket-post gateway. The upstream project is licensed under MPL-2.0.

Production startup uses `MarketCatalog::load_selected` to load the main perpetual market and only the required HIP-3 DEXes. A mainnet read-only probe loading BTC and `xyz:SPCX` completed in approximately 3.0 seconds. Loading every HIP-3 DEX remains available through `MarketCatalog::load`, but it is not used on the execution-critical startup path.

## Local order book contract

Hyperliquid `l2Book` messages are treated as complete finite-depth snapshots. The module never merges them as incremental deltas and never relies on the optional SDK `snapshot` flag.

The trading path is entirely in-process:

`Hyperliquid WebSocket -> LocalOrderBookModule -> strategy calculation -> OrderGateway`

`LocalOrderBookModule` is a Rust memory module, not an HTTP or frontend API. Frontend parameters may configure the strategy, but trigger prices, spread calculations, depth checks, and execution estimates must read the current local order book directly. Frontend rendering must remain outside the execution-critical path.

The module owns the configured markets and provides one controlled boundary for connection state, snapshot application, tradeability checks, top-of-book reads, immutable UI snapshots, and depth-based buy or sell estimates. A strategy must not trade from `LocalBookSnapshot` alone; it must use `top_of_book`, `estimate_buy`, or `estimate_sell` with the current local timestamp so the stale gate is evaluated immediately before a decision. These trading methods return an error instead of returning stale prices when the book is disconnected, waiting for its first snapshot, invalid, or timed out.

The order book becomes tradeable only after a valid snapshot has been accepted. A disconnect, reconnect, stale timeout, crossed market, empty side, invalid level, duplicate price, or non-increasing exchange timestamp prevents the invalid snapshot from replacing the last valid snapshot and closes the trading gate when appropriate.

All prices and sizes use signed fixed-point integers supplied by the caller. The order book core performs no floating-point arithmetic.

The public WebSocket layer uses Tokio Tungstenite and explicitly installs Rustls' `ring` crypto provider before connecting so TLS behavior does not depend on process-wide feature resolution.

The mainnet monitor uses a 10-second stale threshold for the standard 20-level snapshot stream. The one-minute BTC and `xyz:SPCX` validation observed snapshots arriving in roughly five-second batches, so a three-second threshold produced false stale transitions.

`MonitorConfig::mainnet_fast` subscribes with `fast: true`, expects five levels per side, and uses a three-second stale threshold for the faster stream.

## Mainnet validation

On July 19, 2026, the Fast5 monitor ran `BTC` and `xyz:SPCX` together for 30 minutes. Each market accepted 3,316 snapshots, averaging approximately 0.543 seconds per update. The connection remained continuous with zero disconnects, both books finished `Ready` with five bid and five ask levels, and average exchange-to-local latency was approximately 218 milliseconds. BTC rejected one snapshot through the local validation gate; `xyz:SPCX` rejected none.

On July 22, 2026, a fresh 30-second Fast5 read-only probe accepted 55 updates for each market with zero rejected snapshots and zero disconnects. Both books finished `Ready` at five levels per side. Average exchange-to-local latency was approximately 141.84 milliseconds for BTC and 144.11 milliseconds for `xyz:SPCX`.

