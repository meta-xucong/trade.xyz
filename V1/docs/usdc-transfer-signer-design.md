# USDC 划转签名设计

本文件记录 USDC 资金划转的官方依据、失败模式和代码规则。该规则适用于
default perps、spot、HIP-3 perp DEX（例如 `xyz`）之间的小额资金划转。

## 官方依据

- Hyperliquid [`Nonces and API wallets`](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/nonces-and-api-wallets)
  文档说明：API wallet / agent wallet
  可代表 master account 或 subaccount 签名，但账户数据查询必须传真实
  master/subaccount 地址。把 agent 地址当账户查询会得到空结果。
- Hyperliquid 官方 Rust SDK 的
  [`agent.rs`](https://github.com/hyperliquid-dex/hyperliquid-rust-sdk/blob/master/src/bin/agent.rs)
  示例说明：agent wallet 不能转账或
  提现，但可以下单。
- Hyperliquid
  [`/exchange`](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint)
  文档中：
  - `sendAsset` 用于在不同 perp DEX、spot balance、用户和 subaccount 之间转移
    token；`""` 表示默认 USDC perp DEX，`"spot"` 表示 spot。
  - `usdClassTransfer` 用于用户 spot wallet 和 perp wallet 之间转移 USDC。
  - subaccount/vault 没有私钥，相关动作必须由 master 签名，并设置对应地址字段。

## 失败模式

本项目此前的划转路径存在一个容易误判的组合：

1. 余额查询使用 `account.address`，也就是真实 EVM master/subaccount 地址。
2. 提交 `sendAsset` 时却通过 `account.secret_id` 加载私钥。
3. `account.secret_id` 在配置和 Vault 里长期代表 API wallet/agent wallet。

结果是：预检查看到 EVM 账户有 USDC，但真实签名来自 API wallet。API wallet 不是资金
所有者，也不应作为资金划转 signer，因此 live submit 会失败或表现为“有钱但不能转”。

## 数据模型

账户配置必须区分交易签名和资金签名：

```toml
[[accounts]]
account_id = "addr_a"
address = "0x..."                 # 真实 master/subaccount EVM 地址
secret_id = "addr_a_api_wallet"   # 交易 API wallet，仅用于下单/撤单/TP/SL
transfer_secret_id = "addr_a_evm" # 资金划转 EVM signer，仅用于 USDC 划转
```

- `secret_id` / `api_wallet_env`：交易 API wallet。用于 order、cancel、modify、
  leverage、native TP/SL 等交易动作。
- `transfer_secret_id` / `transfer_wallet_env`：资金划转 signer。必须派生出
  `account.address`。用于 USDC funding transfer。
- 兼容旧配置：如果 `transfer_secret_id` 为空，代码会临时回退到 `secret_id`，但必须
  派生地址等于 `account.address` 才允许继续。若该 key 是 API wallet，校验会失败。

## 执行规则

- 只读资金检查和 transfer plan 永远查询 `account.address`，不得查询 API wallet 地址。
- live transfer 提交前必须：
  - 通过 dry-run/live/mainnet/amount gate；
  - Vault 已解锁或 CLI 提供 `TRADE_XYZ_VAULT_PASSWORD`；
  - 加载 `transfer_secret_id` 指向的私钥；
  - 派生 signer 地址并与 `account.address` 精确匹配；
  - 不匹配时 fail closed，错误必须提示 API wallet 不能用于资金划转。
- readiness blocker 名称使用 `evm_transfer_signer_available`，不得再使用
  `api_wallet_secret_available` 表示划转可用。
- trading readiness 仍使用 `api_wallet_secret_available`，因为下单路径本来就需要
  API wallet。

## 后续扩展

官方提供 `agentSendAsset`，但它是独立的 agent 资金动作，不等于把 API wallet 当作有
USDC 的 EVM 钱包。本项目暂不隐式使用 `agentSendAsset`。若未来需要支持，应单独设计
权限、目标地址限制、subaccount 字段和实盘验收测试。
