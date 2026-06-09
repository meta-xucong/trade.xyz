# 测试策略

## 测试分层

```text
unit tests
  -> module tests
  -> replay tests
  -> integration tests
  -> testnet smoke tests
  -> mainnet dry-run shadow tests
```

## 单元测试

必须覆盖：

- symbol normalization。
- price / size precision。
- fib retracement 计算。
- fib 入场、止盈、止损信号。
- smart-money 事件标准化。
- smart-money 去重。
- 跟单比例和 notional 截断。
- manual order request 到 `ManualTradeIntent` 的转换。
- manual batch request 的逐账号拆分。
- signed preflight / frontend readiness 对交易所最低开仓 notional 的拦截，且不阻断
  `reduce-only` 平仓。
- strategy config change 校验。
- dashboard account / position / PnL summary 聚合。
- 风控 reason code。
- cloid 生成。
- nonce manager 单调性。
- coordinator signal fan-out。
- account worker 只处理自己的 account id。

## 回放测试

回放测试从存储的事件日志读取：

```text
MarketEvent + LeaderEvent + AccountEvent
  -> Strategy / ManualOps
  -> TradeIntent
  -> RiskGateway
```

目标：

- 同一份事件输入，多次运行结果一致。
- 重启恢复后不会重复跟单。
- WebSocket 重放事件不会重复下单。
- 策略状态能从快照和事件日志恢复。

## 集成测试

在不接真实私钥的情况下：

- mock Hyperliquid `/info`。
- mock WebSocket stream。
- mock executor。
- mock manual ops HTTP API。
- 验证模块间 API。
- 验证 fail-closed 行为。
- 启动多个 mock account workers，验证同一个 signal 并行广播。
- 验证某个 worker 失败不阻塞其他 worker。

## 基本操作模块测试

必须覆盖：

- 页面能显示环境和 dry-run/live 状态。
- dry-run 点击买入只产生人工意图，不提交真实订单。
- mainnet live 未显式开启时，人工下单被拒绝。
- 超过确认阈值时必须二次确认。
- 多账号批量操作拆成独立 intent。
- 多账号批量操作广播给多个 account workers，并按地址独立返回结果。
- reduce-only 卖出不会扩大仓位。
- 撤单请求会校验账号和订单归属。

## 前端控制台测试

必须覆盖：

- Dashboard 展示资金、仓位、PnL、open orders。
- Dashboard 按地址展示 worker 健康状态和最近 signal 执行结果。
- Dashboard 不包含 secrets。
- Fib 页面提交配置变更后写审计日志。
- Fib 页面启停策略不直接下单。
- Fib 基础版启动、刷新参数必须走 `/api/fib/*` 专用接口；不得从 Fib 前端直接调用手动下单或 runbook 接口。
- Smart Money 页面新增 leader 后写审计日志。
- Smart Money 页面修改 copy ratio 会触发配置校验。
- 前端状态流断开后可重连。

## Testnet smoke test

有 testnet API wallet 后执行：

- 拉取 metadata。
- 拉取账户状态。
- 构造 dry-run order。
- 提交极小限价订单。
- 运行 `signed-preflight`，确认所有 blocker 清除且 `ready_for_testnet_submit=true`。
- `signed-preflight` / 前端 `Preflight` 必须包含 `clearinghouseState` 摘要：新开仓账号
  要有正的 account value 或 withdrawable；reduce-only 平仓方向必须和当前仓位方向相反。
- 运行只读 `signed-runbook`，确认 JSON 中包含 preflight、pre-submit reconciliation、
  acceptance plan，并且不读取私钥、不提交订单。
- 真实提交优先运行 `signed-runbook --submit true`，确认它只在 preflight ready 后加载
  Vault 密码并提交；有 blocker 时必须输出 JSON 证据并非 0 退出。
- CLI 必须覆盖 `signed-live-window`：主网 order smoke 前默认只输出 `.codex-longrun/`
  临时配置预览和逐账号 `signed-runbook`/reduce-only close/reconcile 命令；`--write true`
  只能写 `.codex-longrun/` 下的显式 output config，不得修改 `config/local.toml`，不得读取
  Vault、签名或提交。
- CLI 必须覆盖 `mainnet-smoke-plan`：当前主网 dry-run 状态下，它要返回三层资金诊断、
  USDC 转入 batch preflight、USDC transfer live-window 预览和 signed order live-window 预览；
  `stop_reasons` 必须阻止未转入 `xyz` 保证金前的 order submit，且命令不得写配置、读 Vault、
  签名或提交。
- 前端 Manual 页必须覆盖 `Smoke Plan`：调用 `/api/mainnet-smoke-plan` 后返回与 CLI
  `mainnet-smoke-plan` 等价的只读总控证据；当前主网 dry-run 状态下必须指出资金仍在
  default perps、`xyz` 保证金为 0、live/Vault/确认 gate 未满足，且不得写配置、签名或提交。
- 前端 Manual 页必须覆盖 `Runbook Plan` / `Runbook Submit`：只读 plan 能返回 runbook
  JSON；Vault 已解锁时只读 plan 能把 secret availability 纳入 preflight；submit 在
  blocker 未清除时只展示 readiness 结果，不弹确认、不提交订单。
- 前端 Manual 页必须覆盖 `Preflight Selected`：多账号选择时返回 batch readiness JSON，
  包含 `ready_account_ids`、`blocked_account_ids` 和每个账号的 readiness 结果；该路径
  不得读取私钥、签名或提交。
- 前端 Manual 页必须覆盖 `Funding Check`：只读返回 default perps、XYZ perps 和 spot
  三层资金状态；单账号和多选账号批量查询都必须经由后端 batch API 返回明确资金层诊断。当账号
  无 XYZ 可用保证金时，响应必须给出明确 `funding_summary` 和 `next_actions`，且该路径不得读取
  私钥、签名或提交。
- CLI 必须覆盖 `account-funding`：对单账号或多账号返回 default perps、当前 `dex` perps 和
  spot 的只读资金层 JSON；该命令不读取 Vault、不签名、不提交。资金转入后应使用该命令验证
  `ready_account_ids`，再进入已批准的 order/cancel smoke。
- CLI 必须覆盖 `usdc-dex-transfer` 只读计划：当 default perps 有 USDC、XYZ perps 为 0
  时，计划应显示 source/destination、USDC token、amount 和划转前余额。主网真实提交必须
  因缺少 `app.dry_run=false`、`manual_ops.manual_live_enabled`、
  `manual_ops.mainnet_live_enabled` 或 `--confirm-mainnet-live` 被拒绝；单次超过 10 USDC
  也必须被拒绝。前端 `Plan Transfer` 必须能通过后端 batch API 对单账号和多选账号返回只读计划；
  batch API 的 `submit=true` 必须被后端拒绝。`Transfer USDC` 在 dry-run 状态必须本地阻断，且
  真实提交应支持多账号 fan-out（逐账号 runbook）。
- CLI 必须覆盖 `usdc-dex-transfer-preflight`：当前主网 dry-run 状态下，它要返回可用的只读
  2 USDC transfer plan、确认短语、rate limit、`readiness_summary`、`failed_blockers` 和
  `next_actions`；没有 `TRADE_XYZ_VAULT_PASSWORD` 时必须只报告 Vault/secret blocker，不得读取
  或输出任何 secret。
- CLI 必须覆盖 `usdc-dex-transfer-batch-preflight`：当前主网 dry-run 状态下，它要对选中账号
  汇总 `ready_account_ids`、`blocked_account_ids`、`failed_account_ids` 和每个账号的
  transfer preflight evidence；未全 ready 时 `next_actions` 必须明确要求停止提交并逐账号清除
  blocker。
- CLI 必须覆盖 `usdc-dex-transfer-runbook`：未传 `--submit true` 时只返回 preflight 和
  `transfer=null`；传 `--submit true` 但 blocker 未清除时必须打印完整 JSON 证据、`submitted=false`
  并非零退出，不得加载 secret 或提交。blocker 全清后的真实提交必须回显 transfer response 和
  post-transfer balance evidence。
- CLI 必须覆盖 `usdc-dex-transfer-live-window`：默认只输出临时 live 配置预览，不写文件；
  `--write true` 只能写 `.codex-longrun/` 下的显式 output config，不得修改 `config/local.toml`。
  生成的临时配置应只包含必要 live gate：`app.dry_run=false`、
  `manual_ops.manual_live_enabled=true`、`manual_ops.mainnet_live_enabled=true`。
- 前端 Manual 页必须覆盖 `Transfer Preflight`：多账号选择时返回每个账号的
  `ready_for_mainnet_transfer`/`ready_for_testnet_transfer`、`failed_blockers`、`next_actions`；
  当前主网 dry-run 状态下它必须指出 `app.dry_run=false`、manual live gate、
  mainnet gate、Vault 解锁/EVM transfer signer 等 blocker，而不读取私钥、不签名、不提交。
- 前端 Manual 页必须覆盖 `Transfer Runbook`：单账号只读调用
  `/api/usdc-dex-transfer-runbook` 时必须返回 preflight、`transfer=null`、`submitted=false`
  和 blocker evidence；`Transfer USDC` 提交按钮必须调用同一个 runbook 端点，而不是绕过
  preflight 直接提交。多账号 Live 提交时必须按账号 fan-out，返回逐账号结果。
- 前端和 CLI preflight/runbook 验收必须检查 `readiness_summary`、`failed_blockers` 和
  `next_actions`。未 ready 时，这些字段要准确指出缺失项；ready 时，blocker 列表应为空。
- 前端 Manual 页默认只显示 `Preflight` / `Runbook Plan` / `Runbook Submit`，底层
  `Signed` / `Accept` 操作应在 `Advanced` 展开后才出现。
- 通过 `signed-acceptance --submit true` 提交极小订单。
- 至少跑一次 `--execution-mode taker` 计划，确认 JSON 中 `tif=Ioc`。
- 至少跑一次 `--execution-mode maker` 计划，确认 JSON 中 `tif=Alo` 且限价位于非穿价方向。
- 通过 `orderStatus`、open orders、recent fills 和 `userRateLimit` 对账。
- CLI smoke 必须覆盖只读 `reconcile-account` 和 `order-status`：前者能返回
  clearinghouse state、open orders、recent fills、`userRateLimit`；后者能按 `oid` 或
  `cloid` 查询单笔状态，且 `--oid`/`--cloid` 必须二选一。
- 若订单 resting，按 `cloid` 撤单并再次对账。
- CLI smoke 也必须覆盖 `signed-cancel --cloid <uuid>` 的 fail-closed 行为；真实 testnet
  订单 resting 时用它完成 signed cancel 并再次对账。
- 前端 Manual 页必须验证 `Order Status` 对 `oid`/`cloid` 的二选一校验：两者都空、两者
  都填、无效 UUID 都应在提交前或后端返回明确错误。
- 前端 Vault 页必须验证日常解锁只需要共享密码，确认密码只在展开 `Change Password`
  时出现；改密码提交前要本地校验新密码长度和确认一致性；保存 secret 在已解锁会话下
  不得要求每个账号单独输入密码。
- 平仓/卖出验收必须用 `--reduce-only true` 或前端 `Reduce Only`，确认不会扩大仓位。
- 前端 TP/SL 验收必须覆盖 `Check TP/SL` 和 `Submit TP/SL`：dry-run 下 submit 必须被
  阻断；未触发时 submit 不得要求 Vault；live testnet 下只有触发后的 reduce-only 退出单
  可以提交并对账。触发且 live gate 通过但 Vault 锁定时，应写入 attempt audit 后停止。
- 确认 CLI 结构化 JSON 中所有 `checks[].ok` 均为 `true`。

任何主网前必须完成 testnet order / cancel smoke test。

## Mainnet dry-run shadow

主网实盘前至少跑：

- 主网 metadata。
- 主网真实行情。
- 主网目标 leader 监听。
- 策略生成 intent。
- 风控裁决。
- executor 只记录，不下单。

验证：

- 信号量合理。
- 风控拒绝原因合理。
- 不存在重复跟单。
- 不存在明显过大订单。
- 不存在 stale price 下单意图。

## 性能测试

至少测试：

- 每秒市场事件吞吐。
- leader event 到 intent 延迟。
- intent 到 ApprovedOrder 延迟。
- ApprovedOrder 到提交前延迟。
- coordinator 广播 signal 到所有 workers 的 fan-out 延迟。
- 单 worker 故障时其他 workers 的执行延迟。
- 队列满时是否 fail-closed。

## 测试数据

测试数据不得包含：

- 真实私钥。
- 未脱敏用户地址，除非用户明确授权公开使用。
- 可恢复账户身份的敏感组合信息。

可包含：

- 公开行情。
- 脱敏 leader 地址。
- 模拟成交。

## 提交前命令

```powershell
cargo fmt --check
cargo check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```
