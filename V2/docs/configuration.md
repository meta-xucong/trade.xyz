# 配置规范

## 原则

- 配置决定系统行为，代码只定义默认安全值。
- 实盘开关必须显式配置，不能通过默认值开启。
- 私钥和敏感信息不进入普通配置文件。
- 所有 symbol 使用 canonical 格式，例如 `xyz:TSLA`。

## 推荐文件

```text
config/
  default.toml
  testnet.toml
  mainnet.toml
  local.example.toml
```

真实本地配置建议放在 `config/local.toml`，并加入 `.gitignore`。

## 顶层配置示例

```toml
[app]
name = "trade_xyz_bot"
environment = "testnet"
dry_run = true
fail_closed = true

[hyperliquid]
info_url = "https://api.hyperliquid-testnet.xyz/info"
exchange_url = "https://api.hyperliquid-testnet.xyz/exchange"
ws_url = "wss://api.hyperliquid-testnet.xyz/ws"
dex = "xyz"

[account]
master_address = "0x0000000000000000000000000000000000000000"
subaccount_address = ""
api_wallet_env = "HL_API_WALLET_PRIVATE_KEY"
```

## 多进程账号配置

实盘和低延迟 dry-run 使用每地址一个 account worker：

```toml
[process]
role = "coordinator"
ipc_bind_addr = "127.0.0.1:8788"
worker_heartbeat_ms = 500
signal_ttl_ms = 1500

[secrets]
vault_path = "secrets/trade_xyz.vault"
allow_env_fallback = false

[[accounts]]
account_id = "addr_a"
address = "0x0000000000000000000000000000000000000000"
secret_id = "addr_a_api_wallet"
api_wallet_env = "HL_API_WALLET_PRIVATE_KEY_ADDR_A"
transfer_secret_id = "addr_a_evm_wallet"
transfer_wallet_env = "HL_EVM_TRANSFER_PRIVATE_KEY_ADDR_A"
enabled = true
worker_enabled = true
copy_ratio = 0.10
max_order_notional_usd = 100.0

[[accounts]]
account_id = "addr_b"
address = "0x1111111111111111111111111111111111111111"
secret_id = "addr_b_api_wallet"
api_wallet_env = "HL_API_WALLET_PRIVATE_KEY_ADDR_B"
transfer_secret_id = "addr_b_evm_wallet"
transfer_wallet_env = "HL_EVM_TRANSFER_PRIVATE_KEY_ADDR_B"
enabled = true
worker_enabled = true
copy_ratio = 0.05
max_order_notional_usd = 50.0
```

每个 worker 进程启动时只加载自己的 `account_id`：

```powershell
cargo run -- worker --account-id addr_a --config config/testnet.toml
cargo run -- worker --account-id addr_b --config config/testnet.toml
```

一个 worker 只能服务一个本地交易地址。

`secret_id` / `api_wallet_env` 是交易 API wallet；`transfer_secret_id` /
`transfer_wallet_env` 是资金划转 EVM signer。USDC 划转会校验 transfer signer
派生地址必须等于 `address`，不能使用 API wallet 代替。

通过前端 Vault 页面新增地址时，程序会把私钥写入 `secrets/trade_xyz.vault`，
同时把非敏感账号信息追加或更新到当前启动时使用的配置文件 `[[accounts]]` 中。解锁
已有 vault 文件时，后端也会把 vault 条目的 `address` 和 `secret_id` 同步回当前配置中
同名 `account_id`，并保留原有 `copy_ratio`、`max_order_notional_usd` 等风控字段。新增
账号默认启用 worker，默认 `copy_ratio = 0.10`、`max_order_notional_usd = 100.0`；
上线前应按实际账号风控手动调整这些字段。

## 全局风控配置

```toml
[risk.global]
kill_switch = false
allow_reduce_only_when_killed = true
max_total_position_notional_usd = 5000.0
max_order_notional_usd = 500.0
max_daily_loss_usd = 300.0
min_margin_health = 0.25
max_leverage = 5
max_signal_delay_ms = 2000

[[risk.symbols]]
coin = "xyz:TSLA"
enabled = true
max_position_notional_usd = 1500.0
max_order_notional_usd = 300.0
max_spread_bps = 25
```

## 前端控制台配置

```toml
[frontend]
enabled = true
bind_addr = "127.0.0.1:8787"
dashboard_refresh_ms = 1000
state_stream = "sse"

[manual_ops]
enabled = true
manual_trading_enabled = true
manual_live_enabled = false
mainnet_live_enabled = false
require_confirm_above_notional_usd = 100.0
max_manual_order_notional_usd = 300.0
max_manual_batch_accounts = 5
blocked_symbols = []

[module_symbol_policies]
manual_blocked_symbols = []
fib_blocked_symbols = []
copy_blocked_symbols = []

[[manual_ops.accounts]]
account_id = "test_account_1"
address = "0x0000000000000000000000000000000000000000"
enabled = true
allow_live = false
max_order_notional_usd = 100.0
```

前端和基本操作模块配置不得包含私钥。账号签名仍由后端从环境变量或安全本地配置读取。

说明：

- `module_symbol_policies` 是模块级黑名单来源，分别作用于 `manual`、`fib`、`copy`。
- 空数组表示“该模块不屏蔽任何可交易符号（全部放开）”。
- `manual_ops.blocked_symbols` 仅保留兼容用途；新逻辑以 `module_symbol_policies` 为主。

## 斐波那契策略配置

```toml
[strategies.fib_retracement.xyz_tsla_1h]
enabled = true
coin = "xyz:TSLA"
timeframe = "1h"
lookback_bars = 120
levels = [0.382, 0.618]
entry_tolerance_usd = 0.25
max_entries_per_level = 1
cooldown_secs = 900
take_profit_usd = 3.0
stop_loss_pct = 0.015
max_order_notional_usd = 500.0
execution_mode = "taker"
```

## 聪明钱跟单配置

```toml
[strategies.smart_money_copy.main]
enabled = true
mode = "dry_run"
max_signal_delay_ms = 1500
default_copy_ratio = 0.10
dedupe_window_secs = 3600
conflict_window_ms = 500
min_direction_score_ratio = 1.5
close_overrides_open = true
flip_open_enabled = false
execution_mode = "taker"
allow_short = true
require_fresh_position_for_open = true
pending_open_ttl_secs = 900
post_close_reentry_guard_secs = 1800
max_slippage_bps_open = 25
max_slippage_bps_close = 75

[[strategies.smart_money_copy.leaders]]
leader_id = "leader_alpha"
leader_group = "fund_alpha"
account = "0x0000000000000000000000000000000000000000"
enabled = true
tier = "primary"
weight = 2.0
copy_ratio = 0.08
max_notional_usd_per_trade = 300.0
max_daily_notional_usd = 1000.0
allow_open = true
allow_close = true

[[strategies.smart_money_copy.symbol_limits]]
coin = "xyz:TSLA"
market = "xyz_perp"
enabled = true
allow_short = true
max_order_notional_usd = 300.0
max_position_notional_usd = 1000.0
max_daily_copy_notional_usd = 2000.0
max_spread_bps_open = 25
max_spread_bps_close = 75
```

完整跟单开发规格见 [Smart Money Copy Development Spec](smart-money-copy-development.md)。
配置校验必须拒绝重复 leader、非法地址、无启用 leader 的启用策略、spot 做空、负数限额、
非法 ratio，以及主网 live 跟单缺少显式 live gate 的情况。

## 执行配置

```toml
[execution]
default_mode = "taker"
max_retries = 2
retry_backoff_ms = 250
use_cloid = true
schedule_cancel_enabled = true
schedule_cancel_timeout_ms = 60000

[execution.slippage]
default_max_slippage_bps = 20
stop_loss_max_slippage_bps = 50
```

## 存储配置

```toml
[storage]
audit_log_path = "logs/audit.jsonl"
```

第一版使用本地 JSONL 审计日志记录前端人工操作、preflight、签名计划、撤单请求、对账
和 vault 元操作结果。日志不得包含 vault 密码、API wallet 私钥或签名 payload。后续需要
完整事件溯源时，再升级为 SQLite event store。

## 环境变量

敏感信息只从环境变量读取：

```text
HL_API_WALLET_PRIVATE_KEY
HL_MASTER_ADDRESS
HL_SUBACCOUNT_ADDRESS
```

禁止把私钥写进 `toml`、日志、README、测试快照。

## 配置校验

启动时必须校验：

- environment 和 URL 是否匹配。
- mainnet + dry_run=false 是否被明确允许。
- 所有 symbol 是否存在于 live metadata。
- 所有百分比和 notional 是否为正。
- copy ratio 是否在合理范围。
- stop loss pct 是否在合理范围。
- max order notional 不大于 symbol/global 限制。
- manual live 未显式允许时，不允许主网前端下单。
- testnet signed smoke 需要 `manual_live_enabled = true` 和显式 `--submit`。
- mainnet signed action 需要 `mainnet_live_enabled = true` 和命令级显式确认。
- frontend bind 地址默认只能是 `127.0.0.1`。
- 每个启用 account 必须有独立 API wallet env var。
- worker 模式必须指定唯一 `account_id`。
- 一个 worker 不能配置多个本地交易地址。
- 策略页面配置变更必须通过同一套配置校验。

配置校验失败时禁止启动实盘模式。
