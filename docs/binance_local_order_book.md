# Binance Futures 本地订单簿 — 完整参考

> 基于 `nexus-binance` (Rust) 和 `test_local_order_book.py` (Python) 的实际实现整理。
> 所有 URL / 端点 / 流名均经过实盘验证。更新时间：2026-08-10。

---

## 1. 网络环境

| 环境 | REST Base URL | WebSocket Base URL |
|---|---|---|
| **主网** | `https://fapi.binance.com` | `wss://fstream.binance.com/ws` |
| **测试网** | `https://testnet.binancefuture.com` | `wss://stream.binancefuture.com/ws` |

- 主网 REST 前缀：`/fapi/v1/`、`/fapi/v2/`
- 测试网 REST 前缀：同主网
- WS 组合流地址：主网 `wss://fstream.binance.com/stream`，测试网 `wss://stream.binancefuture.com/stream`
- 公共行情 WS 无需 API Key
- **Rust 连接策略**：`nexus-binance/src/ws.rs` 实现 **直连优先（4s 超时）→ 代理 fallback**。
  直连成功（VPS 等直连通的环境）直接使用，不经过代理；直连被墙/超时才读取
  `HTTPS_PROXY` / `HTTP_PROXY` 环境变量走 HTTP CONNECT 隧道。两种环境互不干扰。

---

## 2. REST 端点 (行情相关)

### 2.1 深度快照

```
GET /fapi/v1/depth?symbol={SYMBOL}&limit={limit}
```

| 参数 | 说明 |
|---|---|
| `symbol` | 交易对，如 `BTCUSDT` |
| `limit` | 档位数：5 / 10 / 20 / 50 / 100 / 500 / 1000（默认 500，最大 1000） |

**权重**：见官方权重表（按 limit 分档）

**返回格式**：
```json
{
  "lastUpdateId": 11255544050554,
  "E": 1786369118571,
  "T": 1786369118564,
  "bids": [["64713.50", "4.129"], ...],
  "asks": [["64713.60", "7.947"], ...]
}
```

- `lastUpdateId`：快照序列号，本地簿初始化 / 重建时的锚点
- `bids`：`[[price, qty], ...]`，价格降序
- `asks`：`[[price, qty], ...]`，价格升序

### 2.2 交易对信息

```
GET /fapi/v1/exchangeInfo
```

返回所有交易对的 `symbol`、`status`、`filters`（`PRICE_FILTER` → tickSize、`LOT_SIZE` → stepSize/minQty、`MIN_NOTIONAL` → notional）。

---

## 3. WebSocket 行情流

### 3.1 连接方式

**方式 A — Raw Stream（单流直连）**：
```
wss://fstream.binance.com/ws/{streamName}
```
每个连接一个流，适合 Python SDK `is_combined=False`。

**方式 B — Combined Stream（组合流）**：
```
wss://fstream.binance.com/stream?streams={name1}/{name2}/...
```
或连接 `wss://fstream.binance.com/stream` 后发送：
```json
{"method": "SUBSCRIBE", "params": ["btcusdt@depth@100ms"], "id": 1}
```

### 3.2 Diff Book Depth（增量深度）

**流名**：`{symbol}@depth` 或 `{symbol}@depth@{speed}ms`

| 速度 | 间隔 | 说明 |
|---|---|---|
| `@depth`（无后缀） | **250ms** | 默认，实测平均 270ms |
| `@depth@100ms` | **100ms** | ✅ 官方最快（当前脚本使用） |
| `@depth@250ms` | 250ms | 等同于无后缀 |
| `@depth@500ms` | 500ms | 低频 |

> `@0ms` 已废弃，无官方支持，不建议生产使用。

**推送字段**：
```json
{
  "e": "depthUpdate",
  "E": 1680000000000,
  "s": "BTCUSDT",
  "U": 1027025,       // first_update_id  (本次更新起始ID)
  "u": 1027030,       // final_update_id  (本次更新结束ID)
  "pu": 1027024,      // prev_final_id    (上一事件的 final_update_id)
  "b": [["27789.19", "0.000"], ...],  // bids: [price, qty]
  "a": [["27789.67", "0.500"], ...]   // asks: [price, qty]
}
```

- 数量为**绝对值**（非差值）：`qty=0` 表示删除该档位，否则 upsert
- `pu` 用于校验连续性：应严格等于上一个已应用事件的 `u`
- 完整的 Binance 官方簿维护算法见第 4 节

### 3.3 Partial Book Depth（Top N 快照）

**流名**：`{symbol}@depth{levels}` 或 `{symbol}@depth{levels}@{speed}ms`

| levels | speed | 说明 |
|---|---|---|
| 5 / 10 / 20 | 100ms / 250ms / 500ms | 直接推送 Top N 档，不维护本地簿 |

- 不需要快照 + 增量对齐，直接就是当前状态
- 适合只需要 BBO 或几档深度的场景
- 数量同样为绝对值

### 3.4 其他行情流

| 流名 | 推送频率 | 用途 |
|---|---|---|
| `{symbol}@aggTrade` | 实时（每次成交） | 逐笔成交，真实时，无频率限制 |
| `{symbol}@bookTicker` | 实时（每次变更） | BBO 单档，真实时 |
| `{symbol}@kline_{interval}` | 按 K 线周期 | K 线数据 |
| `{symbol}@markPrice` | 1s / 3s | 标记价格 |
| `{symbol}@ticker` | 1s | 24hr 行情统计 |
| `{symbol}@miniTicker` | 1s | 精简版 24hr 行情 |
| `{symbol}@forceOrder` | 实时 | 强平订单 |
| `{symbol}@liquidationOrder` | 实时 | 强平订单（别名） |

---

## 4. 本地订单簿维护算法（Binance 官方）

### 4.1 初始化 / 重建流程

```
1. 订阅 WS  diff_book_depth → 事件持续入 buffer
2. GET REST /fapi/v1/depth?limit=1000 → 获取快照 (lastUpdateId)
3. 丢弃 buffer 中 u < lastUpdateId + 1 的过期事件
4. 快照写入本地簿，last_u = lastUpdateId
5. 在 buffer 中找桥接事件：
   - 条件: U <= last_u + 1 && u >= last_u + 1
   - 找到 → 应用该 delta，继续消费后续事件
   - 没找到 → 立即重拉快照（buffer 仍在积累）
6. 持续消费：每个新事件 pu 必须 == last_u，否则 gap → 回到步骤 2
7. qty = 0 删除档位，否则 upsert
```

### 4.2 桥接问题（实测重点）

快照 `lastUpdateId` 和 WS 事件的 `U`/`u` 之间天然存在时间窗口：

```
快照 lastUpdateId ──[gap]── 到达的 WS 事件 U...u
```

这个 gap 的处理逻辑：

- **buffer 中首个事件的 U > last_u + 1**：桥接失败，说明 buffer 中没有覆盖快照时刻的事件 → 重试拉快照（buffer 不清空，下次继续尝试对齐）
- **buffer 中首个事件的 u <= last_u**：事件在快照之前，丢弃
- **找到 U <= last_u + 1 <= u**：桥接成功，开始正常消费

实测中，BTCUSDT 主网约 0.3-0.5s 内能完成桥接。如果超过 5 次重试仍无桥接事件（buffer 完全空且无覆盖快照的事件），**强制推进**用第一个 U > last_u 的事件。

### 4.3 正常消费循环

```
while buffer 非空:
    d = buffer[0]
    if d.u <= last_u:      → 过期，丢弃
    if d.pu != last_u:     → 丢包，清空簿，goto 步骤 2
    apply_delta(d)
    last_u = d.u
    buffer.popleft()
```

### 4.4 本地簿数据结构

```
order_book:
    last_update_id: int       # 当前序列号
    bids: [(price, qty), ...] # 降序，二分查找 upsert
    asks: [(price, qty), ...] # 升序，二分查找 upsert
    update_count: int         # 已消费的增量事件数
```

价格按 `float` 存储（Python），或 `Decimal`（Rust）。二分查找复杂度 O(log N)，1000 档 upsert 极快。

---

## 5. 实测数据（BTCUSDT 主网，2026-08-10）

| 指标 | 数值 |
|---|---|
| 快照初始化 | ~0.3s |
| 桥接成功（首轮） | 稳定 |
| 增量推送频率 (@100ms) | ~9.8 events/s |
| 300 秒同步重建次数 | 0 |
| 300 秒总增量事件 | 2,947 |
| Spread (BTCUSDT) | 0.10 USD (0.0002%) |

---

## 6. Python SDK 映射

```python
from binance.um_futures import UMFutures            # REST
from binance.websocket.um_futures.websocket_client \
    import UMFuturesWebsocketClient                  # WebSocket

# REST
client = UMFutures(base_url="https://fapi.binance.com")
depth = client.depth("BTCUSDT", limit=1000)         # 深度快照
info  = client.exchange_info()                       # 交易对信息

# WebSocket
ws = UMFuturesWebsocketClient(on_message=handler, is_combined=False)
ws.diff_book_depth(symbol="btcusdt", speed=100, id=1)  # 增量深度
ws.agg_trade(symbol="btcusdt", id=2)                    # 逐笔成交
ws.book_ticker(symbol="btcusdt", id=3)                  # 实时BBO
ws.stop()                                                # 关闭
```

---

## 7. Rust 运行方式

```bash
cd /Users/b1top/git/NexusTrade
cargo run -p nexus-binance --example live_book -- BTCUSDT 300   # 主网 5 分钟
cargo run -p nexus-binance --example live_book -- ETHUSDT 60    # 任意币种 60 秒
cargo run -p nexus-binance --example live_book -- --testnet BTCUSDT 30
```

> Rust 端 `live_book` 示例：快照 + 增量本地簿，`fastest=true` → `@depth@100ms`。
> 连接层**直连优先（4s 超时）→ 代理 fallback**，VPS 直连与本地代理互不干扰。

---

## 8. WS 用户数据流连接陷阱（实测踩坑记录）

> 时间：2026-08-11，VPS 直连 + 本地代理环境交叉验证。

### 8.1 现象

ws-fapi 下单成功（`status=200`），但用户数据流（listenKey 通道）收不到
`ORDER_TRADE_UPDATE`（NEW/CANCELED），等待超时。

### 8.2 根因：spawn_reader 封装收不到消息，裸连接能收到

用户流连接方式对比：

| 连接方式 | VPS 直连结果 |
|---|---|
| **裸连接** `tokio_tungstenite::connect_async().await` | ✅ 能收到 NEW/CANCELED |
| **spawn_reader**（内部 `connect_with_proxy` + 后台重连循环） | ❌ 连接成功但收不到消息 |

**关键差异**：
- 裸连接：**同步** `connect_async().await` 等连接完成，再直接读 `SplitStream`
- spawn_reader：**后台 task** 异步连接 + 重连循环，消息经 `tx.send` → `rx.recv()`

虽然两者最终都走 `tokio_tungstenite::connect_async`，但 spawn_reader 的
封装在 VPS 直连下实测收不到消息。**用裸连接替换后立即全部收到。**

### 8.3 次要因素

1. **listenKey 竞争**：`BinanceVenue::connect` 会 acquire 一个 listenKey（私有流），
   与用户流 listenKey 竞争同一账号私有流。REST 通道改用纯 reqwest 签名（不 acquire
   listenKey）后消除。但即使排除此因素，spawn_reader 仍收不到——非主因。
2. **时序**：spawn_reader 后台连接，若未建立就下单，事件错过。裸连接同步建立无此问题。

### 8.4 决定性验证

| 测试 | 结果 |
|---|---|
| Python 裸连接 + ws-fapi 下单 | ✅ 收到 NEW |
| Rust 裸连接 + REST 下单 | ✅ 收到 NEW |
| Rust spawn_reader + 下单 | ❌ 收不到 |

### 8.5 修复

用户流改用**裸连接**（`connect_async` 直连），同步建立、直接读 `SplitStream`。
见 `latency_detail.rs` 的 `connect_user_stream_raw`。

---

## 9. 下单全链路延迟实测（VPS 直连，2026-08-11，us 精度）

> 工具：`cargo run -p nexus-binance --example latency_detail -- --rounds 5`
> 口径：**挂单确认 = 用户流 NEW**（ACK 仅是"发出"）；local-E = 本地收−E；local-T = 本地收−T。
> 本地路径/网络 RTT 用 `Instant` 微秒计时；local-E/T 用毫秒时间戳差。

### 9.1 WS 下单链路（us 精度）

| 环节 | min | avg | max |
|---|---|---|---|
| **策略→网卡**（本地路径） | **242us** | **272us** | 330us |
| 网卡→币安 ACK（网络） | 950us | 2.46ms | 3.95ms |
| 策略→ACK（发出） | 1.19ms | 2.73ms | 4.24ms |
| **策略→用户流 NEW（挂单确认）** | **2.48ms** | **3.42ms** | 4.45ms |
| local-E（本地收−E） | 1us | 1.8us | 3us |
| local-T（本地收−T） | 1us | 2.4us | 4us |

### 9.2 REST 下单链路

| 环节 | min | avg | max |
|---|---|---|---|
| 策略→ACK（发出） | 2.65ms | 5.22ms | 8.92ms |
| **策略→用户流 NEW（挂单确认）** | **3.30ms** | **5.76ms** | 9.77ms |
| local-E | 1us | 1.4us | 2us |
| local-T | 1us | 1.6us | 3us |

### 9.3 撤单链路

| 环节 | WS | REST |
|---|---|---|
| 发起→网卡（本地） | **236us** | — |
| 发起→CANCELED（挂单确认） | **2.75ms** | 3.89ms |
| 发起→ACK（发出） | 1.5ms | 3.60ms |

### 9.4 结论

- **WS 下单比 REST 快约 40%**：挂单确认 WS 3.42ms vs REST 5.76ms；撤单确认 WS 2.75ms vs REST 3.89ms
- **local-E / local-T = 1-3us**：用户流推送到达本地与交易所 E/T 时间戳几乎零偏差（推送即撮合瞬间）
- **本地路径 ~272us**（策略→网卡）：大头是 tokio task 调度 + 跨 task channel 传输
  （`place → channel → spawn_reader task → socket` 两次调度）。
  可优化点：合并 task、签名缓存、零拷贝帧，预估可压到 ~120us。
  当前阶段不做（网卡→ACK 1-3ms 才是网络大头，本地 272us 已很快）。
- **架构底线①验证**：WS 下单全面优于 REST，交易主通道定为 ws-fapi。

---

## 10. 参考文件

| 文件 | 说明 |
|---|---|
| `test_local_order_book.py` | Python 本地簿测试脚本（本项目） |
| `crates/nexus-book/src/book.rs` | Rust 订单簿引擎（含 `apply_delta_both` 双面批量） |
| `crates/nexus-book/src/dual.rs` | Rust 双轨合并 |
| `crates/venues/nexus-binance/src/market.rs` | Rust Binance 行情 adapter（100ms 订阅 + 桥接逻辑） |
| `crates/venues/nexus-binance/src/ws.rs` | Rust WebSocket 传输层（直连优先/代理 fallback） |
| `crates/venues/nexus-binance/src/ws_exec.rs` | Rust ws-fapi 下单客户端 |
| `crates/venues/nexus-binance/src/types.rs` | Rust JSON 线格式定义 |
| `crates/venues/nexus-binance/examples/live_book.rs` | Rust 本地订单簿集成示例 |
| `crates/venues/nexus-binance/examples/latency_detail.rs` | Rust 下单全链路延迟测量（WS/REST 对比 + local-E/T） |
