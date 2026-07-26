# Hype execution optimization

## Current implementation

The first execution milestone uses `hypersdk` 0.2.14 for EIP-712 signing, nonce generation, market discovery, precision metadata, fee queries, position queries, order placement, cancellation, and response parsing.

The execution smoke test keeps the WebSocket reader independent from SDK HTTP requests. Market books, order updates, fills, positions, funding events, heartbeats, reconnects, and re-subscriptions continue to be consumed while an HTTP order request is in flight. The strategy path reads prices from `LocalOrderBookModule`; frontend code is not part of the execution path.

## Known boundary

The SDK sends signed order, cancel, and information requests through HTTP. Its WebSocket implementation currently supports subscriptions, unsubscriptions, heartbeat messages, automatic reconnect, and incoming user or market events, but it does not expose Hyperliquid WebSocket `post` actions.

The SDK `market_open` helper should not be used for the high-performance path until its implementation and documented time-in-force behavior are aligned. The current test creates explicit SDK `place` requests with `ALO` for maker validation and aggressive `IOC` plus a slippage cap for market-taking validation.

## Future WebSocket post migration

1. Keep `hypersdk` signing, nonce, market, precision, and response types.
2. Add a bounded WebSocket command queue with unique request IDs.
3. Add a request registry mapping request IDs to one-shot response channels.
4. Send signed `action` and `info` requests through Hyperliquid WebSocket post messages.
5. Preserve `orderUpdates` and `userFills` as the final source of truth.
6. Keep the local order book and user stream readers independent from command response handling.
7. Apply explicit queue limits, timeouts, reconnect fencing, and request idempotency.
8. Compare HTTP and WebSocket-post latency using the same smoke-test state machine.

## Migration acceptance criteria

- No strategy or frontend contract changes.
- No polling for order confirmation.
- No blocking inside the WebSocket reader.
- No lost order or fill events during an in-flight request.
- Every order has send, transport acknowledgement, order update, fill, cancellation, and final-position timestamps.
- A disconnect closes the execution gate until subscriptions and account state are restored.
- Failed or timed-out commands cannot silently retry with the same nonce or create duplicate orders.

## Mainnet verification

Verified on 2026-07-19 with the live safety gate enabled:

- BTC Fast5 fifth-bid ALO order rested and was confirmed through `orderUpdates`.
- BTC cancellation was acknowledged and confirmed as `Canceled` through `orderUpdates`.
- BTC order did not fill and the final BTC position remained flat.
- SPCX precision was discovered as two decimals; the minimum safe test size was `0.09` because the precision step alone would not satisfy the minimum trade notional.
- SPCX `0.09` aggressive IOC open was confirmed by both `userFills` and a `Filled` order update.
- SPCX `0.09` reduce-only IOC close was confirmed by both `userFills` and a `Filled` order update.
- Final SPCX position and open-order preflight checks passed.

Observed end-to-end latencies from send timestamp:

- BTC HTTP placement acknowledgement: `488522us`.
- BTC WebSocket open confirmation: `481899us`.
- BTC HTTP cancellation acknowledgement: `780038us`.
- BTC WebSocket cancellation confirmation: `776689us`.
- SPCX HTTP open acknowledgement: `1156646us`.
- SPCX WebSocket open order/fill confirmation: about `725ms`.
- SPCX HTTP close acknowledgement: `775417us`.
- SPCX WebSocket close order/fill confirmation: about `787ms`.

These measurements validate correctness of the SDK-first architecture. They also establish the baseline for the future WebSocket-post migration.

## Modularization regression

The execution smoke test was subsequently migrated onto the project-owned account, market, precision, fee/funding, position, order gateway, user stream, execution recovery, and latency modules. The same mainnet BTC maker/cancel and SPCX `0.09` open/close sequence completed successfully after the migration, with both markets flat at completion.

Observed modularized-run timings:

- BTC placement acknowledgement and WebSocket open confirmation: approximately `3.49s` in this run.
- BTC cancellation acknowledgement: approximately `816ms`.
- BTC WebSocket cancellation confirmation: approximately `883ms`.
- SPCX open acknowledgement: approximately `800ms`; order and fill confirmation: approximately `862ms`.
- SPCX close acknowledgement and WebSocket confirmation: approximately `770ms`.

The slower BTC placement sample was not accompanied by local event-loop blocking or order-book staleness; SPCX and cancellation timings remained within the earlier HTTP baseline. Latency sampling must therefore retain distributions and percentiles rather than treating one request as a stable performance guarantee.

The first modularized run also exposed an unrelated startup issue: loading every HIP-3 DEX serially delayed readiness by roughly 50 seconds. The production path now loads only main perpetuals plus explicitly requested DEXes. A read-only BTC + `xyz:SPCX` market probe completed in `3022ms`.

## WebSocket post maker validation

On July 19, 2026, an experimental mainnet WebSocket post gateway signed actions with the SDK and submitted a BTC fifth-bid ALO order through `wss://api.hyperliquid.xyz/ws`. The order rested, was confirmed through `orderUpdates`, was canceled through the same WebSocket post connection, and the final BTC position and open-order state were flat.

- WebSocket connection setup: `1,418,323us`.
- Local order signing: `2,545us`.
- Order post response: `539,527us`.
- `orderUpdates` open confirmation: `535,677us` from send start.
- Local cancel signing: `3,478us`.
- Cancel post response: `847,074us`.
- `orderUpdates` canceled confirmation: `864,977us` from cancel send start.

The open subscription event arrived approximately `3.85ms` before the matching post response. This confirms that the WebSocket post acknowledgement and the user event stream are independent paths; execution state must therefore correlate both by CLOID/OID and must not assume acknowledgement ordering. The experimental gateway remains separate from the production HTTP gateway until reconnect fencing, unknown-state recovery, request multiplexing, and latency distribution tests are complete.

### HTTP versus WebSocket post comparison

On July 19, 2026, the mainnet comparison alternated five HTTP and five WebSocket-post BTC fifth-bid ALO orders. Both transports reused established clients, used the same SDK signing code, order size, market level, user event stream, and nonce source. Every order rested and was canceled, and the account finished flat.

| Measurement | HTTP P50 | HTTP P90 | WS P50 | WS P90 |
| --- | ---: | ---: | ---: | ---: |
| Place response | `441,305us` | `551,073us` | `457,506us` | `489,799us` |
| Open confirmation | `454,332us` | `497,773us` | `528,332us` | `832,426us` |
| Cancel response | `806,620us` | `1,065,221us` | `772,564us` | `843,230us` |
| Canceled confirmation | `813,756us` | `1,031,537us` | `1,003,748us` | `1,303,558us` |
| Full place/cancel cycle | `1,292,491us` | `1,495,769us` | `1,622,768us` | `1,921,640us` |

Median WebSocket place acknowledgement was `16,201us` slower than HTTP, while median WebSocket cancel acknowledgement was `34,056us` faster. Median order-open confirmation was `74,000us` slower through the WebSocket-post sample, and the median full cycle was `330,277us` slower. With five samples per path, this proves only that WebSocket post is not automatically faster in the current network location and implementation; it does not establish stable production percentiles. More samples and a geographically closer runtime are required before changing the production transport.
