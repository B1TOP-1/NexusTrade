# NexusTrade

高性能加密货币永续合约交易框架（Rust）。架构：模块化 venue adapter + 本地订单簿引擎 + 严苛订单状态机 + 独立风控层。

## 已接入交易所

| 交易所 | 类型 | 行情 | 交易 | 账户/用户流 | 状态 |
|---|---|---|---|---|---|
| **Binance USDⓈ-M** | 永续合约 | ✅ 本地订单簿 | ✅ REST + **WS (ws-fapi)** | ✅ listenKey 用户流 | 实测通过 |
| Hyperliquid | 永续合约 | ✅ | ✅ | ✅ | 已接入 |
| Lighter | 永续合约 | ✅ | ✅ | ✅ | 已接入 |

> 后续新增交易所：实现 `MarketVenue` / `ExecutionVenue` / `PrivateVenue` 三个 trait 即可接入。

## 实测延迟（VPS 直连，Binance 主网，us 精度）

> 口径：**挂单确认 = 用户流 NEW**（ACK 仅是"发出"）；local-E = 本地收−E；local-T = 本地收−T。
> 工具：`cargo run -p nexus-binance --example latency_detail -- --rounds 5`

### 下单链路

| 环节 | WS 下单 (min/avg/max) | REST 下单 (min/avg/max) |
|---|---|---|
| 策略→网卡（本地路径） | **242 / 272 / 330 us** | — |
| 网卡→币安 ACK（网络） | 950us / 2.46 / 3.95 ms | — |
| 策略→ACK（发出） | 1.19 / 2.73 / 4.24 ms | 2.65 / 5.22 / 8.92 ms |
| **策略→用户流 NEW（挂单确认）** | **2.48 / 3.42 / 4.45 ms** | **3.30 / 5.76 / 9.77 ms** |
| local-E（本地收−E） | 1 / 1.8 / 3 us | 1 / 1.4 / 2 us |
| local-T（本地收−T） | 1 / 2.4 / 4 us | 1 / 1.6 / 3 us |

### 撤单链路

| 环节 | WS | REST |
|---|---|---|
| 发起→网卡（本地） | 213 / 236 / 270 us | — |
| **发起→用户流 CANCELED（确认）** | **2.26 / 2.75 / 3.30 ms** | 3.08 / 3.89 / 5.55 ms |
| 发起→ACK（发出） | 1.4 / 1.5 / 1.6 ms | 2.70 / 3.60 / 5.33 ms |

### 关键结论

- **WS 下单比 REST 快约 40%**：挂单确认 WS 3.42ms vs REST 5.76ms
- **local-E / local-T = 1-3us**：用户流推送到达本地与交易所 E/T 时间戳几乎零偏差
- **本地路径 ~272us**（策略→网卡）：大头是 tokio 调度 + 跨 task channel，暂不优化

## 核心模块

| 模块 | 职责 |
|---|---|
| `nexus-core` | 统一类型、三大 trait、订单状态机（严苛流转无灰色地带） |
| `nexus-book` | 本地订单簿引擎（快照+增量、序列校验、arc-swap 无锁读） |
| `nexus-net` | 延迟仪表、速率限制（令牌桶） |
| `nexus-risk` | Kill switch、staleness watchdog（>500ms 熔断+撤单） |
| `nexus-sdk` | 策略侧门面（venue 注册表） |
| `venues/nexus-binance` | Binance adapter：行情+WS下单+REST+用户流 |
| `venues/nexus-hype` | Hyperliquid adapter |
| `venues/nexus-lighter` | Lighter adapter |

## 运行示例

```bash
# 本地订单簿（行情，默认 100ms）
cargo run -p nexus-binance --example live_book -- BTCUSDT 300

# 本地订单簿（0ms 尽可能实时推送，~3 倍事件密度）
BINANCE_FUTURES_DEPTH_UPDATE_SPEED=0ms cargo run -p nexus-binance --example live_book -- BTCUSDT 300

# WS 下单验证（post-only 不成交）
cargo run -p nexus-binance --example ws_place

# 下单全链路延迟明细（WS/REST 对比 + local-E/T）
cargo run -p nexus-binance --example latency_detail -- --rounds 5

# 本地簿测试（Python 参考）
python3 test_local_order_book.py BTCUSDT
```

## 架构底线

1. **报价走 WS**：行情 WS，下单/撤单走 ws-fapi，本地 buffer 攒单 flush
2. **API 限流硬隔离**：令牌桶（锁 Key）+ IP 池（锁 IP）
3. **执行 p50 ≤ 30us**：策略→网卡本地路径（当前 272us，待优化）
4. **网络分离**：发单 EIP 与行情 ENI 分开
5. **风控独立**：Kill switch + watchdog 与策略解耦，行情延迟>500ms 熔断撤单

## 文档

- [`docs/architecture.md`](docs/architecture.md)：唯一真理源（架构/状态机/接入规范）
- [`docs/binance_local_order_book.md`](docs/binance_local_order_book.md)：行情 URL/端点/本地簿算法/用户流陷阱/实测数据
- [`docs/binance_order_spec.md`](docs/binance_order_spec.md)：挂单/市价/状态机/用户流事件完整规范（供 AI 实现参考）
