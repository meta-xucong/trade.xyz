# 实施路线图

## Phase 0：工程基础

目标：把 Rust 工程骨架和开发质量门槛建好。

任务：

- 固定目录结构。
- 接入 `clap`、`config`、`thiserror`、`rust_decimal`。
- 建立 `domain/` 类型。
- 建立 process role：coordinator / worker / smoke-test。
- 建立 `tracing` 日志。
- 建立 CI 等价本地命令。
- 保留当前 Hyperliquid `/info` smoke test。

完成标准：

- `cargo fmt --check`
- `cargo check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test`
- smoke test 可运行

## Phase 1：基础信息获取模块

目标：完成只读数据链路。

任务：

- `hyperliquid_client` REST `/info`。
- `xyz_market` metadata、symbol normalization、precision。
- WebSocket 行情订阅。
- 账户状态查询。
- leader fills / order updates 监听可行性验证。
- Signal Coordinator、事件总线和状态快照初版。
- account worker 注册、心跳和 mock fan-out。

完成标准：

- 能实时输出 `MarketEvent`。
- 能发现并标准化 `xyz` 资产。
- 能监听目标账户事件或确定替代数据源。
- 不需要私钥即可运行只读模式。
- 能启动多个 mock workers 并接收同一个 signal。

## Phase 2：基础交易模块

目标：完成 testnet 下单、撤单、对账闭环。

任务：

- API wallet 配置和加载。
- 每地址 account worker。
- 每 worker 独立 nonce manager。
- `cloid` 生成。
- signed order / cancel。
- order report 标准化。
- open orders / fills 对账。
- dry-run executor。
- coordinator 到 worker 的 order signal。

完成标准：

- dry-run 订单完整记录。
- testnet 极小订单可提交并撤销。
- 对账后订单状态一致。
- 一个 worker 只服务一个本地交易地址。
- 任何失败都有明确错误类型和日志。

## Phase 3：风控网关

目标：所有下单意图必须经过风控。

任务：

- `TradeIntent`。
- `RiskGateway`。
- `StrategyRisk` 接口。
- `PortfolioRisk`。
- `ExecutionRisk`。
- kill switch。
- 风控审计日志。

完成标准：

- 策略不能直接触达 executor。
- 所有拒绝有 reason code。
- 边界测试覆盖主要风控规则。

## Phase 4：斐波那契回撤策略

目标：支持第一套独立策略。

任务：

- timeframe 数据聚合。
- 有效上涨波段识别，基础版至少校验 `swing_low` 早于 `swing_high`。
- 0.382 / 0.618 回撤计算。
- 入场回撤冗余：回撤价上方提前挂单、回撤价下方容忍区间。
- 基础版策略实例状态机：启动、暂停、刷新参数、撤销未成交挂单、成交后保护。
- 成交后交易所原生 TP/SL 提交与对账。
- 策略专属风控。
- 回放测试。
- AI 进阶版只落 observe/suggest 框架，不接入实盘自动决策。

完成标准：

- dry-run 下能创建基础版实例、产生接针挂单、模拟成交后产生 TP/SL。
- 同一档位不会重复买入，刷新参数不会重复开仓。
- 止盈止损基于实际成交价。
- AI 页能展示候选配置框架，并能把建议参数带入基础版。

## Phase 5：前端控制台与基本操作模块

目标：提供本地前端控制台，支持账号总览、多账号手动操作、斐波那契管理、聪明钱
跟单管理和基础模块测试。

任务：

- Dashboard：资金、仓位、PnL、open orders、系统健康状态。
- Dashboard：每地址 account worker 健康状态和 signal 执行状态。
- `manual_ops` Rust HTTP API。
- 本地 Web UI。
- 账号、仓位、open orders、行情展示。
- 人工买入/卖出请求转 `ManualTradeIntent`。
- 手动撤单。
- 手动止盈止损。
- 多账号批量操作拆分。
- 多账号批量操作 fan-out 到多个 account workers。
- Fib Retracement 策略配置、启停、监控页面。
- Smart Money Copy leader 配置、启停、监控页面。
- 策略配置变更命令和审计日志。
- `ManualOpsRisk`。
- 审计日志。

完成标准：

- dry-run 下点击买卖不触发真实订单。
- testnet 下可提交极小订单并撤单。
- 多账号操作逐账号过风控。
- 多账号操作由对应 worker 并行执行。
- 主网 live 未显式允许时，前端下单被拒绝。
- Dashboard 能展示资金、仓位、PnL。
- 策略页面配置变更不直接下单。

## Phase 6：聪明钱跟单策略

目标：支持目标地址跟单。

任务：

- 按 [Smart Money Copy Development Spec](smart-money-copy-development.md) 实施。
- leader watcher 只读探针：订阅 `userFills`、`userEvents`、`orderUpdates`、
  `allDexsClearinghouseState`，记录 stream health 和 gap。
- leader event 标准化：通过 fill + 仓位 delta 区分 open / increase / reduce /
  close / flip；无法可靠识别时 fail-closed。
- 持久化去重：覆盖 WS snapshot、重连 replay、REST backfill、leader group 重复和
  worker 重复 signal。
- 冲突裁决：同 symbol 多 leader 短窗口聚合，识别同向合并、反向冲突、close 优先。
- 跟单比例和买量计算：按 leader ratio、本地 account ratio、leader/symbol/account/global
  上限、pending exposure 和交易所最小开仓 notional 截断。
- copy ledger：记录本地仓位与 leader/group/signal 的映射，支持 reduce/close 和重启恢复。
- 策略专属风控：leader、symbol、signal delay、mapping、pending exposure、spread、stale
  state、short permission 等检查。
- 多账号跟单：同一 copy signal 广播给多个 account workers 并行执行，单 worker 异常不阻塞
  其他 worker。
- 回放测试、集成测试、主网 dry-run shadow。

完成标准：

- 同一 leader 事件不会重复跟单。
- open / increase / reduce / close 可区分。
- 反向冲突会跳过或按配置明确裁决，并写审计证据。
- 任一启用 leader 的可靠 close 信号能触发本地 mapped exposure 的 reduce-only close。
- leader flip 会拆成 close + 可选 open，分别过风控。
- 跟单订单受 leader、symbol、account 限额约束。
- pending open 会计入 exposure，不能在 fill 确认前重复超买。
- 重启不会重复跟单，无法恢复 dedupe/ledger 时禁止新开仓。
- 单个 worker 异常不影响其他 worker 跟单。

## Phase 7：实盘前加固

目标：进入主网 dry-run shadow 和小额实盘准备。

任务：

- SQLite event store。
- 快照和重启恢复。
- WebSocket 断线恢复。
- REST reconciliation。
- 指标和报警。
- 操作手册补充。

完成标准：

- 主网 dry-run shadow 至少稳定运行一段时间。
- 重启不会重复跟单。
- 状态不一致时 fail-closed。

## Phase 8：小额主网

目标：在严格限额下实盘验证。

要求：

- 极低 max order notional。
- 极低 max position notional。
- 开启 kill switch。
- 开启 schedule cancel。
- 开启完整审计日志。
- 只启用少量 symbol 和 leader。

完成标准：

- order / fill / position 全部可对账。
- 风控拒绝符合预期。
- 异常时能停止并恢复。
