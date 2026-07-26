# Bybot Lighter Rust Adapter

该 crate 是从 Nautilus Lighter 适配器抽离的独立 Rust 实现，运行时不依赖 NautilusTrader。

## 当前模块

- `signer`：Goldilocks、Poseidon2、Schnorr 原生签名，保留官方离线向量。
- `scaling`：数量与价格整数缩放、单 API Key 原子 nonce 管理。
- `execution`：sendTx 回执、私有订单/成交 DTO、WebSocket 消息解析、ACK 前事件回放、trade ID 幂等和终态分类。
- `http`：独立 reqwest 传输、sendTx 表单、nonce、账户 Decimal 快照和活动订单解析。
- `websocket`：公共/私有订阅、Ping/Pong 保活、确定性订阅重放和有上限指数退避重连。
- `data`：公共订单簿消息 DTO、对象/数组档位解析和交易所时间戳归一化。
- `local_book`：snapshot、连续增量、重复消息幂等、nonce gap/resync、陈旧状态与 Decimal 最优价。
- `execution_client`：市场精度缓存、nonce 初始化、原生签名、HTTP 下单/撤单、账户/持仓快照、REST 活跃订单恢复及私有 WebSocket 运行时。

## Bybot 接入

`high-performance/backend` 通过原生 `LighterExecutionVenue` 使用本 crate，不依赖 Nautilus 工厂、领域类型或消息总线。启动配置只从环境变量读取：

- `LIGHTER_PRIVATE_KEY`
- `LIGHTER_ACCOUNT_INDEX`
- `LIGHTER_API_KEY_INDEX`
- `LIGHTER_CHAIN_ID`（可选签名网络覆盖；主网默认 `304`，与代币或市场选择无关）
- `LIGHTER_HTTP_URL`、`LIGHTER_PRIVATE_WS_URL`（可选）

代币与 `market_id` 由策略卡的市场选择触发，通过 Lighter markets API 自动解析并建立对应 WebSocket 订阅，不写入 `LIGHTER_CHAIN_ID`。

当前离线门禁覆盖 59 项 Lighter 测试和 42 项后端测试；测试网与真实下单不在默认验证范围内。

## 来源与许可边界

`src/signer/`、`src/scaling.rs`、`src/data.rs` 与 `src/local_book.rs` 的来源文件保留原始 Nautech Systems LGPL-3.0 版权和许可声明。Bybot 网关、HTTP、WebSocket 与执行编排采用项目自有边界，避免引入 Nautilus 领域类型和生命周期。
