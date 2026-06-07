# trade_xyz_bot

基于 Rust 的 trade[XYZ] / Hyperliquid 自动交易系统。

当前目标是构建一个模块化交易内核，先支持两类策略：

- 斐波那契回撤接针策略
- 聪明钱地址跟单策略

核心原则：

- 纯 Rust 实盘主链路
- 策略只产生交易意图，不直接下单
- 所有意图必须经过统一风控网关
- 基础信息、交易执行、策略、风控、状态存储严格分层
- 默认 dry-run / testnet，实盘必须显式开启

## 文档入口

- [文档总览](docs/README.md)
- [系统架构](docs/architecture.md)
- [产品需求说明](docs/product-requirements.md)
- [技术栈](docs/tech-stack.md)
- [多进程与多地址执行模型](docs/process-model.md)
- [内部 API 契约](docs/internal-apis.md)
- [策略开发指南](docs/strategy-development.md)
- [前端控制台](docs/frontend-console.md)
- [基本操作模块](docs/manual-operations.md)
- [风控模型](docs/risk-model.md)
- [配置规范](docs/configuration.md)
- [运行手册](docs/operations.md)
- [测试策略](docs/testing.md)
- [安全与密钥](docs/security-and-secrets.md)
- [实施路线图](docs/implementation-roadmap.md)

## 本地验证

```powershell
cargo fmt --check
cargo check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo run -- smoke-test
cargo run -- dry-run --config config/dry-run.example.toml
cargo run -- signed-runbook --config config/local.toml --account-id addr_a --coin xyz:NVDA --side buy --notional-usd 1 --max-slippage-bps 20
cargo run -- signed-acceptance --config config/local.toml --account-id addr_a --coin xyz:NVDA --side buy --notional-usd 1 --max-slippage-bps 20
cargo run -- reconcile-account --config config/local.toml --account-id addr_a
cargo run -- order-status --config config/local.toml --account-id addr_a --cloid 00000000-0000-0000-0000-000000000001
```

`cargo run -- smoke-test` 会执行只读 smoke test，访问 Hyperliquid `/info` 并确认
`xyz` builder DEX 可发现。它不会读取私钥，也不会下单。

`cargo run -- dry-run --config config/dry-run.example.toml` 会启动 coordinator，并为
配置里的每个本地地址启动一个独立 worker 进程，广播一笔 dry-run 信号，验证多地址
并行执行链路。

`signed-runbook` 是推荐的最终验收编排命令。默认只读，按顺序输出 preflight、
pre-submit account reconciliation 和 acceptance plan；传入 `--submit true` 后，只有
preflight ready 才读取 Vault 密码并提交。`signed-acceptance` 是底层验收命令。默认只读，
输出订单计划、open orders、
recent fills 和 `userRateLimit`。只有显式传入 `--submit true`，并且真实地址、Vault、
live gate、kill switch、symbol/notional 风控全部通过后，才会进入签名提交。
preflight 还会读取 `clearinghouseState`：新开仓必须看到正的 account value 或
withdrawable；`reduce-only` 买/卖必须分别存在可减少的空头/多头仓位。
主网 order smoke 前可先运行 `signed-live-window`，它只生成 `.codex-longrun/` 下的一次性
live 配置和逐账号 `signed-runbook`/reduce-only close/reconcile 命令，不读取 Vault、
不签名、不提交，也不会修改 `config/local.toml`。
也可以运行 `mainnet-smoke-plan` 生成总览 JSON：它把三层资金诊断、USDC 转入 preflight、
转账 live-window 预览和 signed order live-window 预览放在一起，仍然只读、不写配置、不提交。
主网真实开仓 notional 必须不低于交易所当前 10 USD 最低订单价值，并且只能在明确批准后提交。
前端 Manual 页也提供 `Runbook Plan` / `Runbook Submit`，用于在浏览器中执行同一套
preflight、对账、acceptance 证据流；submit 按钮仍会先执行 readiness，未通过时不会
弹确认或提交订单。Vault 已在前端进程解锁时，`Runbook Plan` 会用当前会话验证 API wallet
secret 可用性，但不会签名、提交或输出 secret。
Manual 页默认只显示推荐的 `Preflight`、`Runbook Plan`、`Runbook Submit`；底层
`Signed` / `Accept` 按钮收在 `Advanced` 中，主要用于排障。
Manual 页的 `Submit TP/SL` 是 reduce-only 保护退出入口：它先做本地触发判断，触发后
仍必须通过 live gates、真实地址、Vault 解锁和风控检查才会提交。
传入 `--execution-mode maker` 时会生成 ALO maker-only 计划；默认 `taker` 为 IOC。
`signed-preflight` 可在提交前输出同一套 live blocker 检查，确认
`ready_for_testnet_submit` 或 `ready_for_mainnet_submit` 是否已经为 `true`。
Preflight 和 Runbook 的 JSON 同时包含 `readiness_summary`、`failed_blockers`、
`next_actions`，用于快速定位还缺 Vault 解锁、API wallet secret、测试资金、仓位或配置
gate。
前端 Manual 页还提供 `Preflight Selected`，可对多个选中账号批量返回
`ready_account_ids` / `blocked_account_ids`，方便多地址并行验收前确认哪些 worker 已经
具备提交条件。
`Funding Check` 是只读资金层诊断：同一个账号会同时查询 default perps、`dex=xyz`
perps 和 spot，判断 USDC/保证金到底在不在 XYZ perp 层。它不读取 Vault、不签名、不提交；
当 Preflight 已解锁但仍提示 zero collateral 时，优先用它定位是否需要从 default/spot
转入 XYZ perp。
CLI 也提供同等只读诊断：`account-funding --account-id addr_a --account-id addr_b`，
用于资金转入后独立确认 `xyz` perp collateral 已可见，再进入已批准的下单验收。
如需把默认合约账户里的 USDC 小额划到 trade[XYZ]，先用
`usdc-dex-transfer-batch-preflight --amount-usdc <N>`、
`usdc-dex-transfer-preflight --amount-usdc <N>`、`usdc-dex-transfer-runbook --amount-usdc <N>`、
`usdc-dex-transfer --amount-usdc <N>` 或 Manual 页 `Transfer Preflight` / `Transfer Runbook`
/ `Plan Transfer` 生成证据；CLI batch preflight 和 Manual 页 preflight/计划都支持多选账号批量只读检查，
`Transfer Runbook` 和真实提交仍只允许单账号。提交必须显式加 `--submit true`、关闭
`app.dry_run`、开启 `manual_ops.manual_live_enabled`，主网还要开启
`manual_ops.mainnet_live_enabled = true` 并带确认参数。该助手单次每账号硬上限 10 USDC，只用于
smoke 前补足保证金，不作为日常大额资金划转工具。当前 helper 支持 default perps / spot /
当前 `dex` perps 之间划转，也支持跨账号划转；提交路径仍建议按单账号 runbook 逐个执行并核对证据。
为避免手改主配置，推荐用 `usdc-dex-transfer-live-window` 先生成一次性临时 live
配置预览；只有在收到明确金额确认后才加 `--write true` 生成临时配置并逐账号运行 runbook。
`signed-cancel` 可在有 resting `cloid` 时从 CLI 发起 signed cancel-by-cloid，并返回
撤单后的 `orderStatus` 和 open-order 对账信息。

`reconcile-account` 和 `order-status` 是只读 CLI 对账命令，不读取 Vault、不签名、不提交。
前者输出 clearinghouse state、open orders、recent fills 和 `userRateLimit`；后者要求
`--oid`/`--cloid` 二选一，用于提交或撤单后的单笔状态核验。

## 本地密钥保险箱

前端 Console 的 `Vault` 页可以写入本地加密文件：

```text
secrets/trade_xyz.vault
```

普通配置只写 `account_id`、地址和 `secret_id`。API wallet 私钥写入 vault，并由你设置
的共享解锁密码保护。日常解锁只填一次共享密码；确认密码只在 `Change Password`
改密码面板中使用。保存 secret 或解锁已有 vault 后，vault 中的地址和 `secret_id` 会同步
进入本地配置并参与后续交易检查，但私钥不会写入普通配置。`secrets/` 已加入
`.gitignore`。

如果当前 PowerShell 还没有加载 Cargo 的 PATH，可以临时使用：

```powershell
& "$env:USERPROFILE\.cargo\bin\cargo.exe" run -- dry-run --config config/dry-run.example.toml
```
