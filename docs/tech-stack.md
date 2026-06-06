# 技术栈文档

## 基础语言和工具链

- 语言：Rust
- Edition：2024
- 本机已验证工具链：`rustc 1.96.0`，`cargo 1.96.0`
- 目标平台：`x86_64-pc-windows-msvc`
- 编译器工具：Visual Studio C++ Build Tools
- 辅助构建：CMake
- 质量工具：`rustfmt`、`clippy`

本项目实盘主链路必须使用 Rust。Python 只允许作为离线分析或报表工具，不能进入
实时信号、风控、执行、对账链路。

## 当前依赖

当前项目已引入：

- `tokio`：异步运行时
- `reqwest`：REST HTTP 客户端
- `rustls` via `reqwest`：TLS
- `serde` / `serde_json`：序列化和反序列化
- `tracing` / `tracing-subscriber`：结构化日志
- `anyhow`：应用层错误传播
- `hyperliquid_rust_sdk`：官方 Rust SDK，负责 `/exchange` 签名、order、cancel
- `ethers`：SDK 使用的 EVM wallet/signing 类型
- `uuid`：deterministic `cloid`
- `axum`：本地前端控制台 HTTP API
- `argon2` + `chacha20poly1305`：本地 vault 加密

## 推荐核心依赖

后续按需加入：

- `thiserror`：领域错误类型
- `tokio-tungstenite`：WebSocket
- `futures`：异步流处理
- `rust_decimal`：资金、价格、数量计算
- `chrono` 或 `time`：时间处理
- `config` / `figment`：配置加载
- `secrecy`：密钥内存包装
- `sqlx` + SQLite：本地状态存储
- `clap`：CLI
- `axum`：基本操作模块 HTTP API 和本地 Web UI
- `tower-http`：静态文件、CORS、trace layer
- `askama` 或 `maud`：server-rendered HTML
- `interprocess` 或本机 TCP：coordinator 和 account workers 的 IPC
- `rmp-serde` / `bincode`：后续低延迟二进制 IPC 编码
- `metrics` 或 `opentelemetry`：指标

## Hyperliquid SDK 策略

优先使用官方或足够可信的 Rust SDK 处理签名和交易动作。如果 SDK 覆盖不足：

- REST `/info` 可先用自定义强类型 client。
- `/exchange` signed action 必须和官方 SDK 或官方文档行为对齐。
- 签名、asset id、nonce、精度相关代码必须有测试。
- 任何主网前必须在 testnet 完成 order / cancel smoke test。

## 异步模型

使用 `tokio` 多线程 runtime：

- WebSocket ingestion：独立任务。
- REST reconciliation：独立任务。
- Strategy engine：独立任务或按策略拆分任务。
- Manual ops API：独立 HTTP server 任务，只生成人工意图和撤单命令。
- Signal coordinator：独立进程，负责信号生成、广播、worker 监督。
- Account worker：每个本地交易地址一个独立进程，负责本地址风控、nonce 和执行。
- Risk gateway：单独任务或同步函数，但必须串行化关键风控状态。
- Executor：单独任务，维护 nonce 和订单提交顺序。
- Storage writer：单独任务，所有审计事件有序写入。

进程内模块使用有界 `mpsc` channel，禁止无限队列。
进程间通信使用长连接 IPC，必须有心跳、ack、backpressure 和 signal TTL。

## 数据类型原则

- 外部 API 类型放在 `infra/hyperliquid/types.rs`。
- 内部领域类型放在 `domain/`。
- 策略不得依赖外部 API 原始字段名。
- 金额、价格、数量不要用 `f64` 做最终下单计算。
- 所有 symbol 在进入策略前标准化为 `xyz:<SYMBOL>`。

## 日志和指标

日志使用 `tracing`：

- 每个事件包含 `event_id`。
- 每个交易意图包含 `intent_id` 和 `strategy_id`。
- 每个订单包含 `cloid`、`coin`、`side`、`size`、`price`、`mode`。
- 风控拒绝必须记录明确原因。
- signed action 不记录私钥、seed、完整签名原文。

最低指标：

- WebSocket 延迟和重连次数
- REST 成功率、失败率、延迟
- 策略信号数量
- 人工操作请求数量、拒绝数量和二次确认数量
- signal fan-out 延迟、worker ack 延迟、worker 心跳状态
- 风控拒绝数量和原因
- 下单成功率、失败率、成交延迟
- 当前仓位、最大风险暴露、PnL

## Windows 本地注意事项

当前机器上 Windows 应用控制策略会拦截部分目录下 Cargo 生成的 build script 或
exe。本项目通过 `.cargo/config.toml` 固定：

```toml
[build]
target-dir = "C:/Users/T14S/.cargo/target-trade_xyz_bot"
```

不要随意改回项目内 `target/`，否则可能重新触发 `os error 4551`。

## 质量门槛

每次提交前至少运行：

```powershell
cargo fmt --check
cargo check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

涉及交易执行、风控、精度、签名、nonce 的改动必须增加对应测试。
