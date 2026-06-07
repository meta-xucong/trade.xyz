# 基本操作模块

## 定位

基本操作模块是前端控制台中的 Manual Trading 页面和对应后端 API，用于人工点击
买入、卖出、撤单、设置止盈止损和多账号测试。它是人工操作入口，不是绕过系统的
快捷交易通道。

所有人工操作必须走同一条核心链路：

```text
Frontend
  -> Manual Ops API
  -> ManualTradeIntent
  -> RiskGateway
  -> ApprovedOrder / RejectedIntent
  -> Executor
  -> ExecutionReport
  -> UI State Update
```

## 目标

- 测试基础信息模块是否能正确展示行情、账户、仓位、订单。
- 测试基础交易模块是否能正确下单、撤单、对账。
- 支持多账号手动操作。
- 支持人工设置止盈止损。
- 支持人工 dry-run 和 testnet 验证。
- 手动页右侧显示所选交易对实时行情（mark/mid/oracle/funding/OI/24h volume），辅助人工确认。
- 为后续策略调试提供可观察界面。
- 作为系统前端控制台的一部分，与 Dashboard、Fib Retracement、Smart Money Copy
  页面共用状态和审计能力。

## 非目标

第一版不做：

- 复杂图表交易终端。
- 高频手动交易。
- 社交化多用户权限系统。
- 绕过风控的强制下单。

## 推荐技术形态

保持实盘主链路纯 Rust：

- Rust 后端：`axum` 或同类 HTTP server。
- 前端：Rust 后端服务的轻量 Web UI。
- 实时状态：Server-Sent Events 或 WebSocket。
- 页面渲染：第一版可用 server-rendered HTML 加少量交互脚本；后续再考虑
  WASM/Leptos/Dioxus。

前端可以有浏览器脚本，但交易、风控、签名、状态机必须在 Rust 后端完成。

## 功能范围

### 行情和账户展示

页面至少展示：

- 当前环境：testnet / mainnet。
- 当前 dry-run / live 状态。
- 当前账号列表。
- 每个账号的余额、权益、保证金状态。
- 每个账号的持仓。
- 每个账号的 open orders。
- 已启用 symbol 的 mark、mid、oracle、spread。
- 最近订单回报和风控拒绝原因。

完整资金、仓位、PnL、系统健康状态由 [前端控制台](frontend-console.md) 的
Dashboard 页面统一展示。

### 手动下单

支持：

- 选择账号。
- 选择 symbol。
- 现货市场：买入 / 卖出。
- 合约市场（Default Perps、XYZ Perps）：开多 / 开空，以及平多 / 平空。
- 合约平仓由前端“平仓”模式自动转换为 `reduce_only=true`，不要再让用户手工判断
  Buy/Sell 和 reduce-only 的组合含义。
- 合约开仓必须按“新仓”处理：实盘开仓前先强制读取同账号、同市场、同交易对的最新仓位；
  若存在任何非零残仓，先提交 `reduce_only + close_full_position` 的 IOC 平仓单并确认变平，
  再继续提交新开仓。清理失败时必须停止后续开仓。
- 合约平仓完成后也要再次强制读取仓位；若还有残余，自动追加同方向 reduce-only 扫尾。
  若残余小到交易所数量精度无法平掉，必须明确提示，不能继续假装已经完全平仓。
- 本金 USD 和杠杆倍率（前端自动计算 notional 并回传后端）。
- 订单类型按交易所语义展示并执行：
  - `Market (IOC)`：立即成交，剩余未成交部分撤销。
  - `Limit Post-Only (ALO)`：只挂单，若会立即成交则取消。
- 最大滑点。
- client note。

手动下单输出 `ManualTradeIntent`，不得直接生成 `ApprovedOrder`。

### 手动撤单

支持：

- 按账号撤单。
- 按 symbol 撤单。
- 按订单 ID / cloid 撤单。
- 一键撤销某账号所有 open orders。
- 一键撤销某 symbol 所有 open orders。

撤单仍然写审计日志。撤单可以走 `ExecutionCommand::Cancel`，但必须检查账号和订单
归属。

### 手动止盈止损

支持：

- 对已有持仓设置止盈价。
- 对已有持仓设置止损价。
- 按成交均价设置止盈 N 美元。
- 按成交均价设置止损 X%。
- 选择触发后的执行模式：taker / limit / reduce-only。

当前前端以“下单自动附带 TP/SL”为主路径。TP/SL 面板仅负责参数配置和计划预览，不单独提交
真实 TP/SL；Live 开仓订单成功后，系统按账号自动提交对应 TP/SL。Dry Run 模式只返回计划预览：

- 用户填写 entry price、take profit USD、stop loss 百分比（UI 输入口径为“本金亏损比例”，
  `1` 表示本金亏损 `1%`）；entry price 为空时使用当前 XYZ 市场参考价。前端会结合当前杠杆
  先换算价格触发比例（`price_move_pct = loss_pct / leverage`），再传入后端 `stop_loss_pct`
  小数口径。
- 后端计算止盈/止损触发价、退出方向、reduce-only 标记、slippage 保护限价、size 和
  `cloid`。
- Dry Run 下接口只生成计划，不读取私钥、不提交交易所条件单。
- Live 下按所选账号逐一 fan-out 下单与自动附带 TP/SL；每个账号独立通过 live gate、
  独立返回附带 TP/SL 的提交结果。只有开仓单自动附带 TP/SL；平仓 / reduce-only 单
  本身是退出动作，不再附带新的 TP/SL。
- Live 开仓提交后，前端必须先用强制新鲜的账户资金/仓位快照确认该账号已经形成与
  开仓方向一致的实际持仓，再提交交易所原生 TP/SL。若订单只是挂单、未成交、只平掉
  旧方向仓位，或仍残留反向尘埃仓位，则不得提交保护单，界面必须明确提示“订单已提交，
  但自动 TP/SL 未完成”。

### Signed maker/taker 验收

Manual 页的 `Execution` 选择必须作用到 signed smoke 和 signed acceptance：

- Taker -> IOC，使用滑点保护限价，适合最小额快速成交 smoke。
- Maker -> ALO，买单限价低于参考价、卖单限价高于参考价，避免主动吃单。
- `Accept Plan`、`Signed Plan` 和 `Preflight` 都必须在返回结果中展示 `execution_mode` 和
  `tif`，防止界面选择和签名订单参数不一致。

### 多账号操作

支持：

- 账号列表配置。
- 单账号操作。
- 多账号批量操作。
- 每个账号独立跟踪订单、仓位、风控限制。
- 多账号 `Funding Check` 和 `Plan Transfer` 必须走后端批量只读接口，返回每个账号的独立
  资金层或划转计划结果；真实 `Transfer USDC` 在 Live 下也支持多账号 fan-out，但每个账号
  仍走独立 runbook 与独立风控裁决。
- 多账号 `Smoke Plan` 必须走 `/api/mainnet-smoke-plan` 只读接口，把资金诊断、转入
  preflight、转入 live-window 预览和 order live-window 预览汇总在一份结果里；该路径
  不得写配置、签名或提交。
- 多账号 `Transfer Preflight` 必须在真实划转前返回每个账号的 live gate、Vault、API wallet
  secret、default perp 余额、rate limit 和 next actions；只要任一 blocker 未清除，
  不得进入主网资金划转。
- `Transfer Runbook` 是单账号证据链；真实 `Transfer USDC` 无论单账号还是多账号，都应
  逐账号走同一个 runbook 后端端点，先确认 blocker 全清，再提交。
- 批量操作必须由 coordinator fan-out 到每账号对应的 account worker，逐个经过风控。

批量操作不能因为某个账号通过风控，就默认其他账号也通过。每个 worker 独立裁决、
独立下单、独立上报。

## 风控要求

人工操作默认只比策略多一个权限来源，不降低风控标准。

必须检查：

- 当前是否 dry-run。
- 当前账号是否允许手动操作。
- 当前 symbol 是否允许手动操作。
- 单笔 notional。
- 单账号仓位上限。
- reduce-only 方向。
- 精度。
- 滑点。
- 盘口新鲜度。
- kill switch。

建议增加人工操作专属风控：

- `manual_trading_enabled`
- `manual_live_enabled`
- `require_confirm_above_notional_usd`
- `max_manual_order_notional_usd`
- `max_manual_batch_accounts`
- `allowed_manual_symbols`

## UI 安全

- 主网页面必须显眼展示 testnet/mainnet 和 dry-run/live。
- mainnet live 下单必须二次确认。
- 超过阈值的订单必须二次确认。
- 一键批量操作必须展示每个账号的预计订单。
- 禁止在前端保存私钥。
- 禁止把私钥返回给浏览器。

## 审计日志

每个人工操作必须记录：

- 操作时间。
- 操作来源：manual。
- 请求账号。
- symbol。
- side。
- size / notional。
- price policy。
- execution policy。
- 风控裁决。
- 最终订单回报。

如果未来支持多用户，需要记录 operator id。第一版单机使用时可记录本地 operator
标签。

## 和策略模块的关系

基本操作模块不是策略模块，但它和策略一样产生 `TradeIntent`：

```text
FibStrategy -> TradeIntent(strategy_id = "fib...")
CopyStrategy -> TradeIntent(strategy_id = "copy...")
ManualOps -> ManualTradeIntent(strategy_id = "manual_ops")
```

统一风控网关通过 `intent.source = Manual` 选择人工操作专属风控，并继续执行组合级和
执行级风控。

## 第一版验收标准

- 能打开本地 Web UI。
- 能看到环境、dry-run 状态、账号、symbol、行情、open orders。
- dry-run 下点击买入/卖出会产生 `ManualTradeIntent` 和风控裁决。
- `Signed Plan` 只生成签名订单计划，不读取私钥、不提交订单；它必须明确选择单个
  账号，避免多账号页面误把第一个账号当成签名目标。
- `Runbook Plan` 是手动验收的推荐只读入口，返回 preflight、pre-submit reconciliation
  和 acceptance plan，便于在一次操作里保存完整证据。Vault 已解锁时，Runbook Plan 可验证
  匹配 API wallet secret 是否存在，但不得签名、提交或回显 secret。
- `Runbook Submit` 复用 signed runbook 提交流程。前端必须先自动执行 `Preflight`；任何
  blocker 未清除时只展示 readiness 结果，不弹出提交确认、不读取私钥、不提交订单。
- `Accept Plan` 走 signed acceptance 只读路径，返回订单计划、open order count、fill
  count、`userRateLimit` 和 checks，用于实盘前保存验收证据。
- `Preflight` 在签名提交前检查当前账号、symbol、notional、vault 解锁状态、API wallet
  secret、`app.dry_run`、`manual_live_enabled` 和主网额外开关；任何 blocker 未通过都不应
  继续 signed submit。
- `Preflight` / readiness 必须提前拦截低于交易所最低订单价值的开仓。2026-05-31 主网
  实测显示开仓订单最低价值为 10 USD；低于该值只能用于只读计划、dry-run 或 testnet，
  不得进入主网 signed submit。Perp `reduce-only` 平仓不按该开仓最低值拦截；spot
  `sell + close_full_position` 也视作库存平仓路径，不按开仓最低值拦截，但提交到交易所时
  不携带 `reduce_only` 标志。
- `Preflight` 还必须读取 `clearinghouseState` 并返回账号资金/仓位摘要。新开仓需要正的
  account value 或 withdrawable；`reduce-only` sell 只能在已有多头时继续，`reduce-only`
  buy 只能在已有空头时继续。Spot 平仓则要求对应 base inventory 可卖出。
- `Reconcile` 通过 REST 查询所选账号的 XYZ open orders 和 recent fills，用于 testnet
  下单前后对账。
- `Cancel CLOID` 通过 signed cancel-by-cloid 撤销所选账号、所选 symbol 的指定 `cloid`。
  它和 signed submit 共用 live gate：`app.dry_run=false`、`manual_live_enabled=true`、
  主网额外确认、已解锁 vault 和匹配 API wallet secret。
- `Order Status` 必须要求 `oid` 和 `cloid` 二选一。两者都空或两者都填时必须在前端和
  后端都拒绝，不能默认优先使用其中一个。
- TP/SL 面板统一入口：Dry Run 只生成保护计划；Live 不单独提交，改为在 `Buy/Sell` 成功后
  自动附带对应 TP/SL。Perp 路径保持 reduce-only 退出方向：多头 entry 的退出方向为 sell，
  空头 entry 的退出方向为 buy；spot 路径也走交易所原生 trigger/TP-SL 订单，不再依赖本地
  监听器。
- Live 提交前仍必须通过 live gates、真实地址、Vault 解锁和 reduce-only 保护。进入 Vault
  读取前必须记录 sanitized audit attempt。
- Manual 页默认只选中第一个账号，`Select All` 用于批量操作，`First Account` 用于快速回到
  单账号视图。
- Manual 页改为新手模式：同一套字段 + 一个 `Dry Run/Live` 状态切换，不再暴露大量底层按钮。
- 页面保留 4 类主动作：手动交易（现货显示 `Buy/Sell`，合约显示 `Open Long/Open Short`
  和 `Close Long/Close Short`）、`Transfer USDC`、`TP/SL 预览`、`Reconcile/Order Status`。
- 手动交易在 Dry Run 下只走 `/api/manual-order`；在 Live 下对已选账号逐一执行
  `signed-runbook submit`，并自动执行每账号 readiness 阻断链路。
- Dry Run 的 `/api/manual-order` 请求必须携带 `source_module`，后端按
  `module_symbol_policies.<module>_blocked_symbols` 做模块级校验（空数组=不屏蔽任何交易对）。
- `Transfer USDC` 在 Dry Run 下只生成批量划转计划；在 Live 下按所选账号逐一提交 runbook
  划转（fan-out），且受 10 USDC 上限约束。
- `Transfer USDC` 在界面上单独放在右侧底部面板，与买卖逻辑分区，避免误操作。
- `TP/SL` 在 Dry Run 下仅生成保护计划；Live 下不单独提交，按已选账号在开仓成功后
  自动附带提交。
- `Advanced` 区域包含：
  - `Advanced` 按钮与高级字段放在同一位置，展开关系可见。
  - 在线修改 `manual_ops.max_manual_order_notional_usd` 与所选账户 `max_order_notional_usd`，并持久化回 `config/local.toml`。
  - 上限输入统一按 1 USD 步进。
  - 杠杆倍率不再通过单独按钮生效：Live 下单流程会先自动执行交易所 `updateLeverage`，再提交订单。
- TP/SL 输入支持两种模式：
  - `Trigger Price (Exchange-like)`：直接填止盈/止损触发价（需要入场价）。
  - `Ratio Mode (TP % / SL %)`：止盈和止损都按本金百分比输入；perp 按杠杆换算为价格触发
    比例，spot 固定按 1 倍计算。
- Live 模式下手动交易（开仓可自动附带 TP/SL）与 `Transfer USDC` 都支持多账号批量 fan-out；
  每个账号独立通过 vault/live gate，并满足风控与最小下单额限制。
- testnet 下能用极小订单买入、撤单。
- reduce-only 卖出不能扩大仓位。
- 多账号批量操作会 fan-out 到多个 account workers，并按地址返回结果。
- 所有人工操作可在审计日志中追踪。
