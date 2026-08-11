# Binance 挂单 / 市价单 / 订单状态 — 完整规范

> 实测验证（2026-08-11，VPS 直连）。本文档供 AI 实现参考，覆盖：
> 挂单（maker）、市价单（taker）、订单状态机、用户流事件、延迟口径。

---

## 1. 下单通道

### 1.1 两种通道

| 通道 | 用途 | 延迟（VPS 直连实测） |
|---|---|---|
| **WS 下单（ws-fapi）** | 挂单 / 市价 / 撤单 | 策略→FILLED 2.45-4.25ms |
| **REST 下单（fapi/v1/order）** | 兜底 / 低频 | 策略→FILLED 5-10ms |

**WS 下单端点**：`wss://ws-fapi.binance.com/ws-fapi/v1`

### 1.2 挂单（maker，限价）

```json
{
  "id": "请求ID",
  "method": "order.place",
  "params": {
    "symbol": "BTCUSDT",
    "side": "BUY",
    "type": "LIMIT",
    "timeInForce": "GTX",        // GTC/IOC/FOK/GTX(post-only)
    "quantity": "0.001",
    "price": "64273.40",        // 必须对齐 tick(0.10)
    "newClientOrderId": "幂等ID",
    "reduceOnly": false,
    "apiKey": "API_KEY",
    "timestamp": 1234567890123,
    "signature": "HMAC-SHA256"
  }
}
```

**挂单确认路径**：`order.place` 返回 ACK → 用户流推送 `ORDER_TRADE_UPDATE`（NEW → ... → FILLED/CANCELED）。
挂单（maker）不成交则停在 NEW，撤单后 CANCELED。

### 1.3 市价单（taker）

```json
{
  "method": "order.place",
  "params": {
    "symbol": "BTCUSDT",
    "side": "SELL",
    "type": "MARKET",
    "quantity": "0.001",
    "reduceOnly": true,          // 平仓时必须 true
    "newClientOrderId": "幂等ID",
    "apiKey": "API_KEY",
    "timestamp": 1234567890123,
    "signature": "HMAC-SHA256"
  }
}
```

**市价单特点**：
- 立即成交（taker），吃掉盘口
- **成交价 ≈ 盘口 BBO**（实测滑点 0.000%）
- **私有成交可能直接 FILLED，无 NEW**（先到 TRADE_LITE，再 ORDER_TRADE_UPDATE）
- 平仓用 `reduceOnly: true`（不需要额外保证金）

### 1.4 签名规则

```
params 除 signature 外按 key 字母序排序 → k=v&k=v → HMAC-SHA256 hex
apiKey 参与签名，timestamp 参与签名
```

---

## 2. 订单状态机

### 2.1 状态图

```
             submit             ack
 PendingSubmit ──────▶ InFlight ────▶ Open ──┬──▶ PartiallyFilled ──▶ Filled
      │                   │                  ├──▶ Filled
      │ reject            │ timeout/断线     └──▶ Canceled
      ▼                   ▼
   Rejected            Unknown ──reconcile──▶ (Open | Filled | Canceled | Lost)
```

### 2.2 撤单路径（CancelPending）

```
 Open ──cancel请求──▶ CancelPending ──▶ Canceled
   │                       │
   │                       └──▶ Filled   （撤单后仍可能成交，如 taker 单）
 PartiallyFilled ──cancel请求──▶ CancelPending ──▶ Canceled / Filled
```

⚠ **cancel request 发出 ≠ 已取消**。CancelPending 非终态，撤单请求后仍可能收到 Fill。

### 2.3 状态表

| 状态 | 终态 | 含义 |
|---|---|---|
| PendingSubmit | ❌ | 策略本地，未出网 |
| InFlight | ❌ | 已发送，未确认 |
| Unknown | ❌ | 结果未知，需对账 |
| Open | ❌ | 交易所挂单 |
| PartiallyFilled | ❌ | 部分成交 |
| **CancelPending** | ❌ | 撤单已发，未确认 |
| Filled | ✅ | 全部成交 |
| Canceled | ✅ | 已撤销（可带部分成交）|
| Rejected | ✅ | 拒绝 |
| Lost | ✅ | 对账后不存在 |

### 2.4 状态机规则

1. **状态只前进**：禁止倒退（FILLED 后收到旧 PARTIALLY_FILLED 丢弃）
2. **事件版本号**：每条事件带 version，旧事件（version <= 当前）丢弃
3. **成交可先于 ack**：InFlight 允许直接吃 Fill
4. **终态后忽略**：FILLED/CANCELED 后任何事件无效
5. **CancelPending 非终态**：撤单后仍可成交

---

## 3. 用户流事件（listenKey 通道）

### 3.1 连接

```
POST /fapi/v1/listenKey  → 拿 listenKey
wss://fstream.binance.com/private/ws/{listenKey}  （裸连接，直连）
```

⚠ 用 `connect_async` 直连，不要走代理逻辑。listenKey 60 分钟有效，需 PUT 续期。

### 3.2 事件类型

| 事件 `e` | 用途 | 优先级 |
|---|---|---|
| **ORDER_TRADE_UPDATE** | 订单状态 + 私有成交（fee/PnL）| 核心 |
| **TRADE_LITE** | 私有成交轻量版（最快成交信号）| 核心 |
| **ACCOUNT_UPDATE** | 余额（B）+ 仓位（P）| 核心 |
| **MARGIN_CALL** | 强平预警 | 风控 |
| **ACCOUNT_CONFIG_UPDATE** | 杠杆/多资产变更 | 低频 |
| **listenKeyExpired** | key 过期需重建 | 生命周期 |

### 3.3 ORDER_TRADE_UPDATE 完整字段

```json
{
  "e": "ORDER_TRADE_UPDATE",
  "E": 事件时间,
  "T": 撮合时间,
  "o": {
    "s": "BTCUSDT",       // 交易对
    "i": 订单ID,
    "c": "clientOrderId", // 幂等键
    "S": "BUY/SELL",
    "o": "MARKET/LIMIT",
    "X": "NEW/FILLED/...",// 订单状态
    "f": "GTC/IOC/...",   // timeInForce
    "q": 原始数量,
    "z": 已成交量,
    "p": 委托价,
    "ap": "均价",         // 市价单成交均价
    "L": "最新成交价",
    "l": "最新成交量",
    "n": "手续费",
    "N": "手续费币种",
    "m": false,           // 是否被动成交
    "R": "已实现盈亏"
  }
}
```

### 3.4 TRADE_LITE（私有成交轻量版）

```json
{
  "e": "TRADE_LITE",
  "E": 事件时间, "T": 撮合时间,
  "s": "BTCUSDT", "S": "BUY/SELL",
  "i": 订单ID, "t": 成交ID,
  "q": 数量, "L": 成交价, "l": 成交量,
  "m": false, "p": "0"
}
```

**实测**：TRADE_LITE **先于** ORDER_TRADE_UPDATE 到达，是**最快的成交信号**。

---

## 4. 延迟口径（VPS 直连实测）

| 指标 | 定义 | 实测 |
|---|---|---|
| 策略→ACK | 本地发出 → 交易所受理 | 1.57-3.76ms |
| ACK→FILLED | 受理 → 用户流 FILLED | 0.50-0.88ms |
| **策略→FILLED** | **完整链路** | **2.45-4.25ms** |
| local-E | 本地收 − E（事件时间）| 1-2ms |
| local-T | 本地收 − T（撮合时间）| 1-2ms |
| 滑点 | 成交价 vs 盘口 BBO | 0.000% |

**成交行格式**：
```
[成交] FILLED BUY 0.001 @ 64273.4 滑点0.00% fee=0.0321USDT 余额=12.19USDT 仓位=0.001BTC
  延迟: 策略→ACK=1.57ms ACK→FILLED=0.88ms 策略→FILLED=2.45ms local-E=1ms local-T=1ms
```

---

## 5. 关键实现要点

1. **本地订单簿用 `depth@0ms`**（尽可能实时，~3 倍 100ms 事件密度）
2. **下单走 ws-fapi**（HMAC-SHA256，无需 Ed25519）
3. **用户流裸连接**（`connect_async` 直连，不走代理逻辑）
4. **OrderManager 跨事件聚合**：ORDER_TRADE_UPDATE（状态+fee）+ ACCOUNT_UPDATE（余额/仓位）
5. **状态机复用 nexus-core `OrderTracker`**（CancelPending + 版本号 + Execution 分离）
6. **私有成交确认走用户流**，不用公开 Trade 流
