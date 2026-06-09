# 运行手册

## 本地开发

常用命令：

```powershell
cargo fmt --check
cargo check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo run
```

当前 `cargo run` 是只读 smoke test：访问 Hyperliquid `/info` 并确认 `xyz` DEX
存在，不会下单。

## 运行模式

建议支持三种模式：

```text
smoke-test      只读连通性测试
coordinator     启动信号协调层、前端、worker 编排
worker          启动单个本地交易地址的 account worker
dry-run         真实行情和信号，但不提交订单
live            实盘提交订单
console         启动本地前端控制台
```

第一版 CLI 可设计为：

```powershell
cargo run -- smoke-test
cargo run -- coordinator --config config/testnet.toml --dry-run
cargo run -- worker --account-id addr_a --config config/testnet.toml --dry-run
cargo run -- worker --account-id addr_b --config config/testnet.toml --dry-run
cargo run -- console --config config/testnet.toml --dry-run
cargo run -- signed-smoke --config config/testnet.toml --account-id addr_a --coin xyz:NVDA --notional-usd 1
```

主网 live 必须要求显式参数，不得由默认配置开启。

## Signed Smoke Test

`signed-smoke` 是基础交易模块进入实盘前的最小闭环命令。默认只拉取 live metadata、
计算 HIP-3 asset id、按交易所精度生成订单计划，不签名、不提交：

```powershell
cargo run -- signed-smoke --config config/local.toml --account-id addr_a --coin xyz:NVDA --side buy --notional-usd 1 --max-slippage-bps 20
```

注意：2026-05-31 主网实测中，Hyperliquid 对开仓订单返回过 `Order must have minimum
value of $10`。因此 1 USD 只适合作为只读计划、dry-run 或 testnet smoke 的示例；主网真实
开仓 smoke 必须在用户明确批准后使用不低于 10 USD 的 notional，并同步调整
`manual_ops.max_manual_order_notional_usd` 和账号级 notional 上限。`reduce-only` 平仓路径保留
小额通道，用于关闭已有小残仓。

默认 `--execution-mode taker`，实际订单 TIF 为 IOC。要验收 maker-only 路径，显式传入
`--execution-mode maker`；此时实际订单 TIF 为 ALO，买单限价会低于参考价，卖单限价会
高于参考价，避免主动穿价。

提交 testnet 极小订单时必须额外满足：

- `app.dry_run = false`
- `manual_ops.manual_live_enabled = true`
- `risk.global.kill_switch = false`
- `TRADE_XYZ_VAULT_PASSWORD` 已在当前 PowerShell 会话中设置
- 命令显式传入 `--submit true`

```powershell
$env:TRADE_XYZ_VAULT_PASSWORD = "<本机 vault 解锁密码>"
cargo run -- signed-smoke --config config/local.toml --account-id addr_a --coin xyz:NVDA --side buy --notional-usd 1 --max-slippage-bps 20 --submit true --cancel-resting true
```

`signed-acceptance` 是推荐的验收编排命令。它会先生成订单计划，然后读取该账号的
open orders、recent fills 和 `userRateLimit`，并以结构化 JSON 输出证据。默认不签名、
不提交：

```powershell
cargo run -- signed-acceptance --config config/local.toml --account-id addr_a --coin xyz:NVDA --side buy --notional-usd 1 --max-slippage-bps 20
```

`signed-acceptance` JSON 的 `plan` 会回显 `execution_mode` 和实际 `tif`，用于确认前端或
CLI 选择的 maker/taker 已经进入签名订单路径。

推荐在最终 testnet 1U smoke 时优先使用 `signed-runbook`。它把多条只读/提交/对账步骤
放进一个 JSON 报告：`signed-preflight`、pre-submit `reconcile-account`、read-only
acceptance plan、可选提交、post-submit reconciliation 和 `order-status`。默认不提交：

```powershell
cargo run -- signed-runbook --config config/local.toml --account-id addr_a --coin xyz:NVDA --side buy --notional-usd 1 --max-slippage-bps 20 --execution-mode taker
```

只有在加上 `--submit true`，且 runbook 内部 preflight 已经 ready 时，才读取
`TRADE_XYZ_VAULT_PASSWORD` 并提交。若仍有 blocker，runbook 会输出 JSON 证据并以非 0
退出，不加载密钥、不签名、不提交。

在准备真实提交之前，CLI 可先跑同一套只读 preflight。它会输出所有 blocker，包括真实
地址、`app.dry_run`、`manual_live_enabled`、kill switch、Vault 文件、当前 PowerShell
是否设置 `TRADE_XYZ_VAULT_PASSWORD`、对应 API wallet secret 是否能加载、`userRateLimit`
和订单计划：

```powershell
cargo run -- signed-preflight --config config/local.toml --account-id addr_a --coin xyz:NVDA --side buy --notional-usd 1 --max-slippage-bps 20 --execution-mode taker
```

只有 `ready_for_testnet_submit = true` 后，才进入 `signed-acceptance --submit true`。
主网还必须额外满足 `ready_for_mainnet_submit = true`。

主网小额 smoke 不应手改 `config/local.toml`。开仓 notional 必须不低于交易所最低 10 USD。
先用 `signed-live-window` 生成一次性 live 配置预览或写入
`.codex-longrun/mainnet-order-live-window.toml`：

```powershell
cargo run -- signed-live-window --config config/local.toml --account-id addr_a --account-id addr_b --coin xyz:NVDA --side buy --notional-usd 10 --max-slippage-bps 20 --execution-mode taker
```

该命令只准备临时 live gate 和逐账号 `signed-runbook` / reduce-only close /
`reconcile-account` 命令，不读取 Vault、不签名、不提交。只有资金转入 `xyz` 并收到明确
主网下单审批后，才加 `--write true --overwrite true` 写入临时配置并逐账号执行输出的
submit runbook。

需要一份完整总览时，运行 `mainnet-smoke-plan`。它是只读总控报告：同时返回
`account-funding`、`usdc-dex-transfer-batch-preflight`、USDC transfer live-window 预览、
以及 `signed-live-window` 预览，并用 `stop_reasons` / `next_actions` 指明当前应该停在哪一步：

```powershell
cargo run -- mainnet-smoke-plan --config config/local.toml --account-id addr_a --account-id addr_b --funding-amount-usdc 2 --coin xyz:NVDA --side buy --order-notional-usd 10 --max-slippage-bps 20 --execution-mode taker
```

前端 Manual 页的 `Smoke Plan` 调用同一套 Rust 逻辑，只读汇总当前选中账号的资金层、
USDC 转入 readiness、转入 live-window 预览和 order live-window 预览。它不写配置、
不读取 Vault secret、不签名、不提交。

下单前后可用只读 CLI 命令保存账户和订单证据。它们不读取 Vault、不签名、不提交：

```powershell
cargo run -- reconcile-account --config config/local.toml --account-id addr_a
cargo run -- order-status --config config/local.toml --account-id addr_a --oid "<order-oid>"
cargo run -- order-status --config config/local.toml --account-id addr_a --cloid "<order-cloid>"
```

`order-status` 的 `--oid` 和 `--cloid` 必须二选一。两者都空或两者都填时命令应非 0
退出，避免把错误查询键写进验收记录。

当 testnet 真实地址、Vault 密钥、live gate 都已经准备好后，再显式提交：

```powershell
$env:TRADE_XYZ_VAULT_PASSWORD = "<本机 vault 解锁密码>"
cargo run -- signed-runbook --config config/local.toml --account-id addr_a --coin xyz:NVDA --side buy --notional-usd 1 --max-slippage-bps 20 --submit true --cancel-resting true
cargo run -- signed-acceptance --config config/local.toml --account-id addr_a --coin xyz:NVDA --side buy --notional-usd 1 --max-slippage-bps 20 --submit true --cancel-resting true
```

测试平仓或止盈止损退出路径时必须显式使用 reduce-only，避免在没有对应仓位时扩大风险：

```powershell
cargo run -- signed-acceptance --config config/local.toml --account-id addr_a --coin xyz:NVDA --side sell --notional-usd 1 --max-slippage-bps 20 --reduce-only true --submit true --cancel-resting true
```

如果提交后需要按 `cloid` 独立撤单，可使用 CLI signed cancel。它会先检查 live gate、
真实地址和 `cloid` UUID 格式，再读取 Vault 密码并签名撤单：

```powershell
$env:TRADE_XYZ_VAULT_PASSWORD = "<本机 vault 解锁密码>"
cargo run -- signed-cancel --config config/local.toml --account-id addr_a --coin xyz:NVDA --cloid "<order-cloid>"
```

`signed-acceptance --submit true` 会在读取 Vault 密钥之前检查：

- `app.dry_run = false`
- `manual_ops.manual_live_enabled = true`
- `risk.global.kill_switch = false`
- 非示例真实地址
- `manual_ops.max_manual_order_notional_usd`
- `module_symbol_policies.<module>_blocked_symbols`（按来源模块检查：manual/fib/copy）
- 主网时还需要 `manual_ops.mainnet_live_enabled = true` 和 `--confirm-mainnet-live`

如果传入 `--reduce-only true`，全局 kill switch 开启时只有在
`risk.global.allow_reduce_only_when_killed = true` 的情况下才允许继续。该路径只用于平仓、
止盈止损或缩仓验收，不应用作开仓 smoke。

提交后命令必须拿到 submitted report、执行 open order / fill / `orderStatus` 对账，并再次
读取 `userRateLimit`。任一验收检查失败时命令应以非 0 退出，不能把未完成的 signed smoke
误报为通过。

主网 smoke 还必须同时设置 `manual_ops.mainnet_live_enabled = true` 并传入
`--confirm-mainnet-live`。实际主网最小额测试前，应先用 testnet 完成 order/cancel
对账。

前端 Manual 页也提供同样的实测辅助入口：

- 页面按钮按 `Dry Run`、`Signed Single`、`Protection`、`Order State` 分组。只有
  `Runbook Submit`、`Signed Submit`、`Accept Submit` 和 `Cancel CLOID` 可能进入签名动作；
  其余计划/对账按钮是只读或 dry-run。
- `Preflight`：检查 signed submit 的所有 blocker。
- `Preflight` 会检查 `risk.global.kill_switch`，熔断开启时 signed submit 不得继续。
- `Preflight` 会额外读取 Hyperliquid `userRateLimit`，确认当前账号还有请求容量；读取失败
  或容量耗尽时不得继续 signed submit。
- `Preflight` 会读取 `clearinghouseState` 并检查账号状态。新开仓需要看到正的 account
  value 或 withdrawable；reduce-only 卖出必须有多头可减，reduce-only 买入必须有空头
  可减。
- `Preflight Selected`：对当前选中的多个账号批量运行同一套 readiness，返回
  `ready_account_ids`、`blocked_account_ids` 和每个账号的 blocker/next actions。多地址
  testnet smoke 前先用它确认哪些 account worker 已经可以独立提交。
- `Funding Check`：只读查询所选账号的 default perps、当前 `dex` perps 和 spot 资金层，
  支持对当前选中账号批量返回资金诊断。它不读取 Vault、不签名、不提交；当 Vault/API wallet
  已通过但 Preflight 仍显示
  `account_has_available_collateral=false` 时，用它确认资金是否在 default/spot，或是否
  需要先给 testnet XYZ perp account 充值/划转 USDC。
- `account-funding`：CLI 版只读资金层诊断，返回与前端 Funding Check 等价的 default perps、
  当前 `dex` perps 和 spot 信息。资金转入后应先运行它确认 `ready_account_ids` 包含目标账号，
  再运行 Preflight/Runbook 进入已批准的 signed smoke。
- `usdc-dex-transfer-batch-preflight` / `usdc-dex-transfer-preflight` /
  `usdc-dex-transfer-runbook` / `usdc-dex-transfer`：CLI 的小额
  资金划转助手，用于在 default perps / spot / 各 perp dex 资金层之间划转 USDC。先运行
  `usdc-dex-transfer-batch-preflight` 对所有选中账号汇总 `ready_account_ids`、
  `blocked_account_ids`、rate limit 和只读计划；再用单账号
  `usdc-dex-transfer-preflight` 复查目标账号的 `readiness_summary`、`failed_blockers`、
  `next_actions`。推荐用 `usdc-dex-transfer-runbook` 执行最终流程，因为它会
  先输出 preflight，再在 blocker 全清时才提交，并在 blocker 仍存在时非零退出、不签名；
  `usdc-dex-transfer` 未传 `--submit true` 时只生成计划和划转前余额；真实提交需要
  `app.dry_run=false`、`manual_ops.manual_live_enabled=true`、Vault 密码、
  `manual_ops.mainnet_live_enabled = true`、`--confirm-mainnet-live`，并且单次每账号硬上限
  10 USDC。Manual 页也提供 `Transfer Preflight` / `Transfer Runbook` / `Plan Transfer` /
  `Transfer USDC`，其中 preflight 和计划可批量，Live 提交按账号 fan-out 到同一 runbook
  端点返回 preflight/submit 证据。该路径只用于 smoke 前补足
  trade[XYZ] 保证金，大额资金管理应在官方界面或专门资金模块中执行。
  划转 submit 不使用 API wallet；它必须加载 `transfer_secret_id` /
  `transfer_wallet_env` 对应的 EVM transfer signer，并校验 signer 地址等于账户
  `address`。相关 readiness blocker 名为 `evm_transfer_signer_available`。
- `usdc-dex-transfer-live-window`：只生成主网资金转入窗口计划和一次性临时 live 配置，不读取
  Vault、不签名、不提交。未传 `--write true` 时只输出 JSON 预览；传 `--write true` 时写入
  `.codex-longrun/mainnet-usdc-transfer-window.toml` 一类临时配置，主 `config/local.toml`
  必须保持 `dry_run=true`。真实转入后应停止继续使用该临时配置。
- `Preflight` 和 `Runbook Plan` 的 JSON 会包含 `readiness_summary`、`failed_blockers`
  和 `next_actions`。最终 signed smoke 前优先看这三个字段：它们应为空 blocker，并显示
  ready；否则按 `next_actions` 补齐 Vault、API wallet secret（交易）、
  EVM transfer signer（划转）、测试资金、仓位或配置 gate。
- `Runbook Plan`：调用 signed runbook 只读路径，一次展示 preflight、pre-submit
  reconciliation 和 acceptance plan。若前端 Vault 已解锁，该按钮会沿用当前解锁会话验证
  secret 可用性，但仍不得签名或提交。
- 底层 `Signed` / `Accept` 按钮默认收进 `Advanced`，日常手动验收优先走
  `Runbook Plan` / `Runbook Submit`。
- `Runbook Submit`：先自动执行 `Preflight`；通过后才弹确认并进入 signed runbook
  提交流程，提交后展示 post-submit reconciliation 和 `orderStatus` 证据。
- `Signed Submit`：先自动执行 `Preflight`；只有 readiness 通过后才弹确认并进入 signed
  smoke 提交。
- `Accept Plan`：调用 signed acceptance 只读路径，展示订单计划、open order count、
  fill count、`userRateLimit` 和 checks。
- `Accept Submit`：先自动执行 `Preflight`；通过后才弹确认并进入 signed acceptance
  提交流程，提交后展示 submitted report、撤单/对账结果和验收 checks。
- Manual 页的 `Reduce Only` 勾选会传入 signed plan / signed acceptance / preflight；用于
  平仓验收或保护退出验收。
- `Set TP/SL`：统一入口。Dry Run 只返回 TP/SL 计划预览；Live 直接提交 Hyperliquid
  原生 trigger/TP-SL 订单，不再依赖本地持久化监控规则。
- Live TP/SL 提交仍受同一套 live gate 约束：dry-run 禁止提交、Vault 未解锁禁止继续。
  读取 Vault 前必须写入 sanitized audit attempt。
- `Reconcile`：读取所选账号的 XYZ open orders、recent fills 和 `userRateLimit`。
- `Order Status`：按 `oid` 或 `cloid` 调用 Hyperliquid `orderStatus`，用于 signed smoke
  下单/撤单后确认单笔订单处于 `open`、`filled`、`canceled`、`rejected` 等状态。`oid`
  和 `cloid` 必须二选一，不能同时填写。
- `Cancel CLOID`：对指定 `cloid` 发起 signed cancel-by-cloid，和 signed submit 共用
  live gate。

核心 signed submit / cancel 路径还会在加载 API wallet secret 之前复查账号地址，拒绝
`0x000...0001` 这类示例地址，避免绕过前端 readiness 后误进入签名流程。

前端交易与配置操作会写入 `storage.audit_log_path` 指向的 JSONL 审计日志。当前默认是
`logs/audit.jsonl`。审计事件记录 action、account、symbol、结果和错误摘要，但不得记录
vault 密码、API wallet 私钥或完整签名 payload。Dashboard 的 Recent Events 会显示最近
审计结果，便于实盘 smoke 前后复盘。

## 启动流程

```text
load config
  -> validate config
  -> initialize logging
  -> initialize storage
  -> fetch live metadata
  -> validate symbols and precision
  -> start coordinator
  -> start or attach account workers
  -> each worker loads its own snapshot
  -> each worker reconciles its own account state
  -> connect websocket streams
  -> start strategies
  -> start frontend console and manual ops API if enabled
  -> start risk gateway
  -> start executor inside each account worker
```

## 停止流程

收到 Ctrl+C 或停止信号：

```text
stop generating new intents
  -> drain risk queue
  -> optionally cancel open strategy orders
  -> flush storage
  -> write final snapshot
  -> close websocket
  -> exit
```

是否自动撤单由配置控制。实盘默认建议撤销非 reduce-only 的未成交挂单。

## 重启恢复

重启后必须：

- 读取最近状态快照。
- 通过 WebSocket realtime cache 获取 open orders、fills、positions；REST 只用于启动/重连补
  快照、显式对账、缓存过期兜底和提交后确认。
- Dashboard 资金、可用余额、PnL 和持仓来自 `clearinghouseState` + `dex = "xyz"`。
- 对比本地订单状态和交易所状态。
- 用 `orderStatus`、`cloid` 和 fill id 去重。
- 恢复策略状态。
- 恢复每个 worker 的账户状态。
- 确认状态健康后才允许新开仓。

如果对账不一致：

- 进入 fail-closed。
- 禁止新开仓。
- 允许人工查看审计日志。

## 监控项

最低监控：

- WebSocket 连接状态和重连次数。
- 行情事件延迟。
- leader 事件延迟。
- REST 请求延迟和失败率。
- 风控拒绝数量。
- 下单成功率和失败原因。
- 当前仓位和风险暴露。
- 存储写入延迟。
- nonce manager 健康状态。
- 每个 account worker 的进程状态、心跳和信号延迟。
- 每个 signal_id 在各 worker 的执行结果。

## 故障处理

### WebSocket 断线

- 暂停依赖实时行情的策略信号。
- 触发 REST reconciliation。
- 重连后等待快照同步。
- 状态恢复前禁止新开仓。

### REST 失败

- 短暂失败可指数退避重试。
- 连续失败超过阈值，暂停新开仓。
- `/exchange` action-level error 必须作为失败处理。

### 存储失败

- 立即 fail-closed。
- 禁止继续实盘下单。
- 保留内存状态用于安全退出。

### nonce 异常

- 停止对应 account worker 的所有 signed action。
- 不自动猜测修复已损坏 nonce。
- 等待重新对账或人工确认。

### worker 异常

- 单个 worker 异常不阻塞其他 worker。
- coordinator 标记该 worker unhealthy。
- 对该 worker 拒绝新信号或进入 fail-closed。
- Dashboard 按地址展示异常。

## 日志要求

必须记录：

- 启动配置摘要，不含密钥。
- 交易所 metadata 摘要。
- 策略信号。
- 人工操作请求和二次确认结果。
- 策略配置变更和策略控制命令。
- 风控批准/拒绝。
- 订单提交和响应。
- 成交回报。
- 每个 signal_id 的 worker fan-out 和执行结果。
- 异常和恢复过程。

禁止记录：

- 私钥。
- seed phrase。
- 完整签名密文。
- 未脱敏敏感账户配置。
