# Gate 接入文档

> 状态：**已接入，未实盘验证**。使用前必须完成实盘验证（见 §7）。
> 来源：从鹦鹉螺（nautilus-gate crate）剥离 + 彻底解耦 nexus-core。

---

## 1. 接入状态

| 阶段 | 状态 |
|---|---|
| 剥离独立编译 | ✅ |
| ExecutionVenue（WS 下单）| ✅ |
| MarketVenue（20ms 本地簿）| ✅ |
| PrivateVenue（私有流）| ✅ |
| SDK 接入（gate feature）| ✅ |
| **实盘验证** | ⬜ **必须做** |

---

## 2. 端点

| 类型 | URL |
|---|---|
| REST | `https://api.gateio.ws/api/v4/futures/usdt` |
| WS 行情 | `wss://fx-ws.gateio.ws/v4/ws/usdt` |
| WS 私有流 | `wss://fx-ws.gateio.ws/v4/ws/usdt` |
| 合约格式 | `BTC_USDT`（下划线）|

---

## 3. 交易（ExecutionVenue）

### 3.1 WS 下单（P1 WS-first）

```
channel: futures.order_place
event:   api
payload: { req_id, req_param }
```

`req_param`（由 `build_order_req_param` 构造）：
```json
{
  "contract": "BTC_USDT",
  "size": 1,              // 整数合约数，买正卖负
  "price": "61000",       // 市价单为 "0"
  "tif": "gtc",           // gtc/ioc/fok/poc
  "text": "t-xxx",        // 客户端订单标识
  "reduce_only": false
}
```

- **挂单**：`tif=gtc` + price（post-only 用 `poc`）
- **市价单**：`price="0"` + `tif=ioc`（需 `market_order_slip_ratio`）
- **撤单**：`futures.order_cancel`，按 venue 单号或 text

### 3.2 签名

```
sign_string = "channel={channel}&event={event}&time={timestamp}"
SIGN = HMAC-SHA512(sign_string, api_secret)
```

Header：`KEY` / `SIGN` / `Timestamp`（REST）；WS 用 `auth` 块。

### 3.3 精度

- `size` 是**整数合约数**（张），base 值 = size × quanto_multiplier
- BTC_USDT：1 合约 = 0.0001 BTC
- `base_qty_to_contracts` 转换 base 量 → 合约数

---

## 4. 行情（MarketVenue，20ms 本地簿）

### 4.1 订阅

```
channel: futures.order_book_update
event:   subscribe
payload: [contract, interval, depth]   // ["BTC_USDT", "20ms", "20"]
```

支持：`20ms`/`100ms` 速度，深度 20/100。

### 4.2 增量帧

```json
{
  "channel": "futures.order_book_update",
  "event": "update",
  "result": {
    "full": true,          // true=快照帧, false=增量帧
    "s": "BTC_USDT",
    "U": 123,              // first_update_id
    "u": 456,              // last_update_id
    "b": [["61000","1"]],  // bids [price, qty]
    "a": [["61001","2"]]   // asks
  }
}
```

**算法**（同 Binance）：
- `full=true` → 快照，直接应用
- `full=false` → 增量，`U`/`u` 对齐

喂 `nexus-book` 的 `BookEngine`。

---

## 5. 私有流（PrivateVenue）

订阅 channel：
- `futures.orders`（订单状态）
- `futures.usertrades`（成交，含 fee）
- `futures.positions`（仓位）
- `futures.balances`（余额）

**成交字段**（usertrades）：`order_id`/`id`(trade_id)/`size`/`price`/`fee`/`role`/`text`
**订单状态**（orders）：`id`/`text`/`status`(open/finished)/`finish_as`(filled/cancelled)

输出统一 `AccountEvent`（Fill/OrderUpdate/PositionUpdate/BalanceUpdate）。

---

## 6. 运行示例

```bash
# 集成冒烟（行情+连接+私有流）
GATE_API_KEY=xxx GATE_API_SECRET=yyy \
  cargo run -p nexus-sdk --example gate_smoke --features gate -- BTC_USDT
```

---

## 7. 实盘验证清单（必须完成）

> ⚠ Gate 已接入但**未实盘验证**。使用交易前必须：

1. 配置 `GATE_API_KEY`/`GATE_API_SECRET`（.env）
2. `gate_smoke` 验证行情 + 连接
3. **小额真实下单**：挂单（post-only）→ 撤单；市价单（开/平）→ 成交
4. 用 OrderManager 确认状态机实盘正确（NEW/FILLED/CANCELED）
5. 记录延迟/滑点数据，达标后才可用于策略

---

## 8. 与 Binance 对比

| 项 | Gate | Binance |
|---|---|---|
| WS 下单 | ✅ 原生 | ✅ ws-fapi |
| 行情速度 | **20ms** | 0ms/100ms |
| 下单延迟 | 未测 | 3.42ms |
| 私有流 | SBE | listenKey |
| 状态机 | 复用 OrderManager | 复用 OrderManager |
