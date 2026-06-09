# 安全与密钥

## 基本原则

- 默认不实盘。
- 默认不读取私钥。
- 默认不下单。
- 主网 live 必须显式开启。
- 前端控制台默认只绑定本机地址。
- 私钥只通过环境变量或本地被忽略的秘密文件读取。

## 密钥来源

推荐：

```text
secrets/trade_xyz.vault
```

`secrets/trade_xyz.vault` 是本地加密 vault 文件。前端 Vault 页面负责写入和解锁，
普通配置文件只保存 `secret_id`，不保存私钥明文。

可选兼容环境变量：

```text
HL_API_WALLET_PRIVATE_KEY_ADDR_A
HL_API_WALLET_PRIVATE_KEY_ADDR_B
TRADE_XYZ_VAULT_PASSWORD
```

只有当配置显式设置 `allow_env_fallback = true` 时才允许从环境变量读取 API wallet
私钥。默认不启用环境变量回退。

禁止：

- 把私钥写入 `Cargo.toml`、`config/*.toml`、README、测试文件。
- 在日志里打印私钥或完整签名输入。
- 把真实私钥提交到 git。

## API wallet

自动交易优先使用 API wallet / agent wallet：

- API wallet 负责签名。
- 查询账户状态时使用 master 或 subaccount 地址。
- 每个 account worker 必须使用独立 API wallet，避免 nonce 冲突。
- 一个 worker 只能加载一个本地交易地址的 secret。
- API wallet 不作为资金所有者使用。USDC 划转必须使用
  `transfer_secret_id` / `transfer_wallet_env` 指向的 EVM transfer signer，
  并校验派生地址等于配置里的 `address`。

## 本地 Vault

默认 vault 路径：

```text
secrets/trade_xyz.vault
```

vault 文件格式由程序维护：

- 密码通过 Argon2id 派生加密密钥。
- 私钥 JSON 用 XChaCha20-Poly1305 认证加密。
- 文件中保存 salt、nonce 和密文，不保存密码。
- 解锁成功后只返回 `secret_id`、`account_id`、地址和更新时间，不回显私钥。
- 前端控制台解锁成功后，后端进程会保留一个本机内存解锁会话；刷新页面或点击刷新
  状态不需要重复输入密码。该会话不写入磁盘，控制台服务重启后必须重新解锁。
- 解锁已有 vault 文件时，后端会把 vault 条目的非敏感账号信息同步进当前本地配置，
  让 vault 中已保存的地址立即参与 Dashboard、Manual 和 signed preflight；同步过程不写
  入私钥，不覆盖账号级 notional/copy ratio 等风控字段。

前端 Vault 页面写入 API wallet secret 时需要：

- `account_id`
- `secret_id`
- 地址
- API wallet private key
- 已解锁的本机后端会话，或本次请求输入的共享 vault 密码

确认密码只用于 `Change Password` 操作，不用于日常解锁或保存 secret。前端应默认收起
改密码面板，只有用户点击 `Change Password` 时才显示当前密码、新密码和确认新密码。
修改 vault 密码时需要输入新密码和确认新密码；如果当前后端会话已经解锁，当前密码可由
内存会话提供，否则需要重新输入当前密码。

Vault 页面支持两种写入方式：

- 选择已有配置账号，更新该账号的 API wallet secret。
- 选择新增地址，写入 secret 后同步把非敏感账号信息写入当前运行配置的
  `[[accounts]]`。新增账号默认启用 `enabled = true`、`worker_enabled = true`、
  `copy_ratio = 0.10`、`max_order_notional_usd = 100.0`，私钥仍只保存在 vault。

worker 实盘运行时通过 `account.secret_id` 查找对应密钥。若需要非交互启动，可用
`TRADE_XYZ_VAULT_PASSWORD` 提供解锁密码；后续可升级为 Windows DPAPI 本机记住模式。

## 实盘保护

主网 live 必须同时满足：

- `environment = "mainnet"`
- `dry_run = false`
- CLI 参数显式指定 live。
- `manual_ops.manual_live_enabled = true`
- `manual_ops.mainnet_live_enabled = true`
- 配置校验通过。
- 风控状态健康。
- 存储可写。
- nonce manager 健康。
- 对应 account worker 健康。
- manual live 操作已显式允许，且请求完成二次确认。

任一条件不满足时禁止 signed action。

testnet signed smoke 可以只开启 `manual_ops.manual_live_enabled = true`；主网必须额外开启
`manual_ops.mainnet_live_enabled = true` 并在命令中传入显式确认参数。Vault 密码只允许通
过本机前端解锁会话或当前 PowerShell 的 `TRADE_XYZ_VAULT_PASSWORD` 提供，不写入配置。
主网 order/cancel smoke 应优先通过 `signed-live-window` 生成一次性 `.codex-longrun/`
临时配置和逐账号 runbook 命令；该 helper 只能准备配置/命令，不得读取 Vault、签名、提交，
也不得自动清除 kill switch 或修改主 `config/local.toml`。
主网真实开仓 notional 必须不低于交易所当前 10 USD 最低订单价值；低于该值的开仓只能作为
只读计划、dry-run 或 testnet 验证，不得进入 signed submit。

默认合约账户到 trade[XYZ] 的 USDC 划转同样是主网 signed action。第一版
`usdc-dex-transfer-batch-preflight`、`usdc-dex-transfer-preflight`、
`usdc-dex-transfer-runbook` 和 `usdc-dex-transfer` 只能小额
补保证金，单次每账号最多 10 USDC；提交前必须显示划转前 default perps 和目标 `dex` perps
余额、readiness blocker、next actions 和主网确认短语，且提交必须受 `app.dry_run=false`、
`manual_ops.manual_live_enabled=true` 约束。主网提交还必须同时有
`manual_ops.mainnet_live_enabled = true` 和显式确认参数；前端提交必须要求输入精确确认短语。
不得把该助手扩展成无上限自动资金调度。
划转 readiness 使用 `evm_transfer_signer_available` 检查项；如果该检查失败，说明缺少
EVM transfer signer、Vault 未解锁，或 signer 派生地址不是账户 `address`。

## 前端控制台安全

- 默认绑定 `127.0.0.1`，不得默认暴露到公网。
- 前端不得保存或展示私钥；Vault 密码只在提交解锁、保存或改密请求时发送给本机后端。
- 主网 live 下单必须二次确认。
- 主网 live 关键策略参数修改必须二次确认。
- 超过配置阈值的订单必须二次确认。
- 多账号批量操作必须展示每个账号的预期订单。
- 所有人工操作必须写审计日志。
- 所有策略配置变更和启停操作必须写审计日志。

## 日志脱敏

日志中地址建议保留：

```text
0x1234...abcd
```

私钥、seed phrase、签名 payload 永不输出。

## 配置文件保护

建议 `.gitignore` 包含：

```text
.env
.env.*
config/local.toml
config/secrets.toml
secrets/
data/
logs/
```

如果后续添加这些路径，需要同步更新 `.gitignore`。

## 依赖安全

- 新增 crate 前确认维护状态、许可证、下载量、源码质量。
- 签名、私钥、交易执行相关 crate 优先选择官方或广泛使用版本。
- 不引入不必要的宏重型或运行时侵入型依赖。

## 故障安全

系统遇到以下情况必须 fail-closed：

- 存储不可写。
- 账户状态过期。
- WebSocket 长时间断线且无法 REST 对账。
- nonce 状态异常。
- account worker IPC 断开。
- risk gateway 不健康。
- 配置热更新失败。
- 发现本地仓位和交易所仓位不一致且无法自动解释。

fail-closed 后：

- 禁止新开仓和加仓。
- 可按配置允许 reduce-only。
- 可允许撤单。
- 必须写审计日志。
