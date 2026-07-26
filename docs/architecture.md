# NexusTrade 架构设计 v0.1

> 状态：待审定
> 决策人：B1TOP
> 本文档是 NexusTrade 的唯一真理来源。任何实现与本文冲突，以本文为准；要改行为，先改本文。

---

## 1. 定位与目标

NexusTrade 是一个**全平台交易所连接层 SDK**（纯 Rust library workspace，非服务、非策略）。

- **一句话**：策略工程 `cargo add nexus-sdk` 之后，永远不再写任何交易所相关代码。
- **统一提供**：WS 本地订单簿、WS 下单、WS 私有状态流、签名鉴权、重连心跳、限流、风控原语、延迟仪表。
- **首批交易所**：Lighter、Hyperliquid（迁移自已实盘验证的 `bybot-hype` / `bybot-lighter` / `bybot-market-engine`，复制迁移、独立演进）。
- **后续路线**：Binance → OKX → Bybit → Gate → Bitget →（可随时插入任意新所）。

### 成功标准

```rust
// 策略侧的全部交易所代码，只允许长这样：
let nexus = Nexus::builder()
    .venue(Hype::from_env()?)
    .venue(Lighter::from_env()?)
    .build()
    .await?;

let book = nexus.book(VenueId::HYPE, &sym("BTC")).await?;
let (bid, ask) = book.vwap(dec!(2000)).ok_or(Stale)?;
let ack = nexus.exec(VenueId::HYPE).place(NewOrder::limit(Side::Buy, px, qty).ioc()).await?;
```

新增一个交易所 = 新增一个 adapter crate 实现三个 trait，**策略代码零改动**。

---

## 2. 设计原则（不可协商项）

来源：variational-v1 实盘经验、high-performance 工程实践、外部 HFT 基建方法论（已逐条鉴定采信）。

| # | 原则 | 说明 |
|---|------|------|
| P1 | **WS-first** | 报价/下单/撤单/私有状态全走 WS；REST 只做初始化查询与兜底。批量指令本地 buffer 攒单，一次 flush |
| P2 | **严苛订单状态机** | 状态流转无灰色地带；断线/超时进入 `Unknown` 态并强制走对账兜底路径，SDK 层负责，不推给策略 |
| P3 | **Staleness watchdog** | 每条行情流带本地时间戳；超阈值（默认 500ms，可配）发熔断事件，可选自动 cancel-all。宁可不报价，绝不用残影定价 |
| P4 | **Kill switch 独立** | 与策略完全解耦的一键闸：全所撤单 + 禁新单（原子布尔门，`place()` 入口强制检查） |
| P5 | **限流内置** | per-key 权重感知令牌桶；各 adapter 声明端点成本表。IP 池切换仅留扩展接口，默认不启用 |
| P6 | **延迟仪表内置** | 每条流打 local-E 时间戳，HDR 直方图，p95/p99/p99.9 开箱可读（p50 不作为验收指标） |
| P7 | **双轨订阅可选** | 订单簿支持同源双 WS 连接，按序列号去重，丢包瞬时互补 |
| P8 | **热路径零分配** | 解析用零分配库（`sonic-rs`），buffer 预分配复用；务实执行，不为微秒级过度设计 |
| P9 | **fail-closed** | 任何不一致（簿断裂、仓位对不上、状态未知）一律停下来暴露给上层，绝不静默猜测、绝不自动重试下单 |
| P10 | **Decimal 全程** | 价格/数量统一 `rust_decimal::Decimal`，浮点数不允许出现在资金相关路径 |

**私有回报优先**：自己的成交只认私有订单回报通道；公开 Trade 流只做特征输入（主动买卖方向、成交量），永不作为成交确认或延迟基准。

---

## 3. Workspace 布局

```text
NexusTrade/
├── Cargo.toml                    # workspace root
├── docs/
│   └── architecture.md           # 本文档（唯一真理源）
├── crates/
│   ├── nexus-core/               # L0 统一类型 + trait 定义。零 IO、零依赖交易所
│   ├── nexus-net/                # L1 连接基建：WS 管理、重连退避、心跳、限流器、延迟仪表
│   ├── nexus-book/               # L1 本地订单簿引擎：快照+增量、序列校验、VWAP、双轨合并
│   ├── nexus-risk/               # L1 风控原语：staleness watchdog、kill switch、总敞口闸
│   ├── nexus-sdk/                # L2 门面 crate：策略唯一依赖。Builder + re-export
│   ├── nexus-conformance/        # 测试基建：adapter 一致性测试套件 + 订单簿回放夹具
│   └── venues/
│       ├── nexus-hype/           # 迁移自 bybot-hype（行情+执行）
│       ├── nexus-lighter/        # 迁移合并 bybot-lighter（执行）+ market_engine（行情）
│       ├── nexus-binance/        # 规划：含 depth@0ms 增量流
│       ├── nexus-okx/            # 规划
│       ├── nexus-bybit/          # 规划
│       ├── nexus-gate/           # 规划
│       └── nexus-bitget/         # 规划
```

依赖方向（严格单向，禁止反向引用）：

```text
strategy ──▶ nexus-sdk ──▶ venues/* ──▶ nexus-core
                │                        ▲
                └──▶ nexus-net / nexus-book / nexus-risk ──┘
```

- `nexus-core` 不依赖任何其他 nexus crate，编译最快、最稳定。
- venue crate 之间**互相不可见**。
- 策略只 `use nexus_sdk::*`，feature flag 按需启用交易所：`nexus-sdk = { features = ["hype", "lighter"] }`。

---

## 4. 核心抽象（nexus-core）

### 4.1 统一类型

```rust
pub struct VenueId(pub &'static str);          // "HYPE" / "LIGHTER" / "BINANCE_FUT" ...
pub struct Symbol { pub base: String, pub quote: String, pub venue_native: String }

pub enum Side { Buy, Sell }
pub enum Tif { Gtc, Ioc, Fok, PostOnly }

pub struct NewOrder {
    pub symbol: Symbol,
    pub side: Side,
    pub kind: OrderKind,        // Limit { price } | Market
    pub qty: Decimal,
    pub tif: Tif,
    pub reduce_only: bool,
    pub client_id: ClientOrderId,   // SDK 生成，全局唯一，幂等键
}

pub struct SymbolMeta {         // 初始化时从交易所拉取，Adapter 负责
    pub tick_size: Decimal,
    pub lot_size: Decimal,
    pub min_notional: Decimal,
}
// core 提供 quantize_price / quantize_qty（floor 对齐），策略不自己算精度
```

### 4.2 三大 trait（每个交易所必须实现）

```rust
/// 行情：本地订单簿 + 公开成交
#[async_trait]
pub trait MarketVenue: Send + Sync {
    async fn subscribe_book(&self, symbol: &Symbol, opts: BookOptions) -> Result<BookHandle>;
    async fn subscribe_trades(&self, symbol: &Symbol) -> Result<TradeStream>;
    fn symbol_meta(&self, symbol: &Symbol) -> Result<SymbolMeta>;
}

/// 执行：下单/撤单，WS 优先
#[async_trait]
pub trait ExecutionVenue: Send + Sync {
    async fn place(&self, order: NewOrder) -> Result<OrderAck>;
    async fn place_batch(&self, orders: Vec<NewOrder>) -> Result<Vec<OrderAck>>;
    async fn cancel(&self, r: &OrderRef) -> Result<()>;
    async fn cancel_batch(&self, rs: &[OrderRef]) -> Result<Vec<CancelResult>>;
    async fn cancel_all(&self, symbol: Option<&Symbol>) -> Result<()>;   // kill-switch 快速通道，必须实现
    fn capabilities(&self) -> VenueCapabilities;
    fn is_ready(&self) -> bool;
}

/// 私有状态：订单回报、成交、仓位、余额
#[async_trait]
pub trait PrivateVenue: Send + Sync {
    async fn subscribe(&self) -> Result<AccountStream>;   // 统一事件流
    async fn snapshot(&self) -> Result<AccountSnapshot>;  // REST 兜底对账用
}
```

### 4.3 能力声明（策略可查询，避免踩不支持的功能）

```rust
pub struct VenueCapabilities {
    pub ws_order_entry: bool,
    pub batch_orders: bool,
    pub post_only: bool,
    pub reduce_only: bool,
    pub cancel_all_native: bool,     // 交易所原生一键撤单 vs SDK 逐单模拟
    pub book_fastest_interval_ms: u32,  // Binance futures = 0 (depth@0ms)
    pub dual_feed: bool,
}
```

### 4.4 统一事件流

```rust
pub enum AccountEvent {
    OrderUpdate(OrderUpdate),     // 状态机流转事件（见 §5）
    Fill(Fill),                   // 真实成交：价格/数量/手续费/是否 maker
    PositionUpdate(Position),
    BalanceUpdate(Balance),
    ConnectionState(ConnState),   // Connected / Reconnecting / Down
}
```

### 4.5 错误分类

```rust
pub enum NexusError {
    Transport(..),       // 网络层，可重连
    Auth(..),            // 签名/鉴权，不可重试
    RateLimited { retry_after: Option<Duration> },
    VenueReject { code: String, msg: String },   // 交易所明确拒绝
    Stale,               // 数据陈旧，fail-closed
    Unknown(..),         // 歧义结果 → 必须走对账路径
}
```

---

## 5. 订单状态机（P2，SDK 层强制）

```text
             submit             ack
 PendingSubmit ──────▶ InFlight ────▶ Open ──┬──▶ PartiallyFilled ──▶ Filled
      │                   │                  ├──▶ Filled
      │ reject            │ timeout/断线     └──▶ Canceled
      ▼                   ▼
   Rejected            Unknown ──reconcile──▶ (Open | Filled | Canceled | Lost)
```

规则：

1. 每笔订单由 SDK 生成全局唯一 `client_id`，作为全链路幂等键。
2. `InFlight` 超时（可配，默认 5s）或连接中断 → 强制转 `Unknown`，**SDK 自动触发对账**（REST snapshot 查单），对账结果驱动最终态。
3. `Unknown` 期间该 symbol 的新单默认被 SDK 拒绝（可配置放行），fail-closed。
4. 所有状态迁移发出 `OrderUpdate` 事件，策略侧永远看到一致的状态序列，绝不跳变。
5. SDK 不做任何自动重试下单。失败/歧义暴露给策略层决策。

---

## 6. 本地订单簿引擎（nexus-book）

- **维护**：首帧完整快照 + 增量 diff；序列号/nonce 连续性校验，断裂即清簿重建（沿用 market_engine 已实盘验证的模式）。
- **新鲜度**：每次更新打 `local_recv_ts`（local-E 口径）；`BookHandle::staleness()` 随时可查。**统一口径：所有交易所都有时间陈旧阈值**（修正 high-performance 中 Lighter 无时间口径的缺口）。
- **读取模型**：写侧单任务独占，读侧 `arc-swap` 快照，策略读无锁、无等待。
- **双轨模式（P7，可选）**：同 symbol 两条 WS，按序列号合并去重，任一条断流零感知切换。
- **读 API**：

```rust
impl BookHandle {
    pub fn top(&self) -> Option<TopOfBook>;                          // bid/ask 一档
    pub fn vwap(&self, notional: Decimal) -> Option<(Decimal, Decimal)>; // 指定名义深度 VWAP（Edge 计算标配）
    pub fn depth(&self, n: usize) -> BookView;
    pub fn staleness(&self) -> Duration;
    pub fn seq(&self) -> u64;
}
```

深度不足指定名义额时 `vwap` 返回 `None`（不可交易），不返回劣化值——与 high-performance 现行为一致。

---

## 7. 连接层（nexus-net）

- **重连**：指数退避（500ms 起，上限可配），重连后自动重订阅 + 触发簿重建。
- **心跳**：per-venue ping/pong 策略由 adapter 声明，超时判死立即重连。
- **限流器（P5）**：权重感知令牌桶，per-key 一个桶；adapter 提供 `fn cost(endpoint) -> u32` 成本表；触发限流返回 `RateLimited` 而不是盲目排队。
- **批量 flush（P1）**：`place_batch`/`cancel_batch` 在 adapter 内合帧，单 WS message 发出（交易所支持时）。
- **延迟仪表（P6）**：每条流内置 HDR 直方图（`hdrhistogram`），`nexus.metrics()` 可读各流 p95/p99/p99.9；行情流与私有流分开统计。

---

## 8. 风控原语（nexus-risk）

策略层管赚钱，本层管别亏死，两者物理分离。

| 原语 | 行为 |
|---|---|
| **StalenessWatchdog** | 监控所有已订阅簿；超阈值发 `RiskEvent::Stale`，可配 `auto_cancel_all: bool` |
| **KillSwitch** | `trip()`：原子置位 → 全 venue 并发 `cancel_all` → 后续 `place()` 一律拒绝。`reset()` 需显式调用 |
| **ExposureGate**（预留 v0.2） | 跨 venue 总名义敞口上限，超限拒新单 |

Kill switch 是 SDK 内唯一允许"未经策略同意就动订单"的模块。

---

## 9. 交易所接入规范（新 adapter 上线清单）

新增交易所 = 新建 `venues/nexus-xxx`，完成以下全部项方可标记 ready：

1. 实现 `MarketVenue` + `ExecutionVenue` + `PrivateVenue` 三 trait；
2. 声明 `VenueCapabilities` 与限流成本表；
3. 通过 `nexus-conformance` 全套一致性测试：
   - 订单簿回放夹具（快照+增量+乱序+断裂场景）；
   - 状态机流转黄金用例（含 Unknown→对账路径）；
   - 精度对齐用例（tick/lot/min_notional）；
4. 测试网或小额实盘验证双腿往返一次；
5. 在本文档 §12 登记状态。

### Binance 专项

- 期货订单簿使用 `depth@0ms` 增量流（`book_fastest_interval_ms = 0`）；快照走 REST depth 接口对齐 `lastUpdateId`。
- 现货评估 SBE 行情流；下单走 WS API（`order.place`）。
- Trade/aggTrade 流仅接入为特征数据源，不参与订单确认。

---

## 10. 迁移与实施路线图

| 阶段 | 内容 | 验收 |
|---|---|---|
| **M0** | workspace 骨架 + `nexus-core` 全部类型与 trait + 状态机（纯逻辑，含单元测试） | `cargo test` 全绿 |
| **M1** | 复制迁移 `bybot-hype` → `nexus-hype`，`bybot-lighter` + `market_engine` → `nexus-lighter`；各套一层 trait adapter，不改动已实盘验证的内部逻辑 | conformance 通过 + 与原 crate 行为 diff 为零 |
| **M2** | `nexus-net`（限流/仪表）+ `nexus-book`（统一新鲜度口径/双轨）+ `nexus-risk`（watchdog/kill switch） | 集成测试 + 断线注入测试 |
| **M3** | `nexus-sdk` 门面 + 示例策略（双所 Edge 打印 demo） | 示例跑通，策略侧代码 ≤ 30 行 |
| **M4** | `nexus-binance`（depth@0ms + WS 下单） | conformance + 测试网往返 |
| **M5+** | OKX → Bybit → Gate → Bitget，每所一个原子迭代 | 同 §9 清单 |

迁移铁律：M1 阶段对 hype/lighter **只做包装不做重构**——已实盘验证的代码是资产，先跑通再演进。

---

## 11. 决策记录（ADR）

| # | 决策 | 定夺 | 日期 |
|---|---|---|---|
| A1 | 复制迁移，独立演进，与 bybot 脱钩 | B1TOP | 2026-07-26 |
| A2 | 价格/数量全程 `rust_decimal::Decimal` | B1TOP | 2026-07-26 |
| A3 | Binance `depth@0ms` 作为正式能力项接入 | B1TOP | 2026-07-26 |
| A4 | IP 池动态切换不做默认，仅留扩展接口 | 待确认 | — |
| A5 | 不采纳 EC2 hunting/网络分离进 SDK（归运维文档） | 待确认 | — |
