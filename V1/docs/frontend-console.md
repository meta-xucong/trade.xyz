# 前端控制台

## 定位

前端控制台是整个交易系统的本地操作和观察界面，不只服务基本手动交易，也服务
斐波那契策略、聪明钱跟单策略和未来策略。

前端只负责展示状态、提交配置变更请求、提交人工操作请求。交易、风控、签名、状态机
都必须在 Rust 后端完成。

## 总体页面

第一版至少包含四个页面：

```text
Dashboard
Manual Trading
Fib Retracement
Smart Money Copy
```

后续可以增加：

- Risk Center
- Orders and Fills
- Event Log
- Settings

## Dashboard

Dashboard 是默认首页，展示当前账号和系统基础情况。

必须展示：

- 当前环境：testnet / mainnet。
- 当前模式：dry-run / live。
- 总览市场切换：`xyz_perp` / `hl_perp` / `spot`。
- 系统健康状态：行情、账户、风控、执行器、存储、WebSocket。
- 当前账号列表。
- 每个账号对应 account worker 的健康状态、心跳、最近 signal 延迟。
- 每个账号的可用资金、账户权益、保证金状态。
- 每个账号的仓位总价值（position value）。
- 每个账号的持仓。
- 每个账号的未实现 PnL、已实现 PnL。
- 每个账号的 open orders。
- 今日成交 notional。
- 今日 PnL。
- 最近风控拒绝。
- 最近订单和成交。
- 最近 `signal_id` 在各地址上的执行结果。

资金、可用余额、未实现 PnL 和持仓应优先来自后端维护的 Hyperliquid WebSocket realtime
cache；REST `/info` 只作为冷启动、重连、显式对账或缓存过期兜底。Dashboard 数据层必须
随“总览市场切换”同步：

- `xyz_perp`：使用 `clearinghouseState` + `dex = "xyz"`。
- `hl_perp`：使用默认 perp `clearinghouseState`（不传 `dex`）。
- `spot`：使用 `spotClearinghouseState`。

如果单个账号状态拉取失败，Dashboard 可以保留其他账号状态，但必须在 recent events
中暴露该账号状态错误，不能静默显示为真实资金。

Dashboard 不直接下单，但可以链接到 Manual Trading 页面。

Dashboard 资金口径统一为：

- `总权益 = 可用资金 + 仓位总价值`
- `仓位总价值` 为当前持仓按市场价格估值后的总和（spot 为 token/USDC 估值，perp 为仓位名义价值汇总）。

Dashboard 的 Recent Events 应展示最近的业务动作记录，优先保留三类：

- 最近成交记录。
- 最近止盈止损设定记录。
- 最近止盈止损触发记录。

它来源于 `storage.audit_log_path` 的 JSONL 审计日志与运行期状态检查，但前端必须过滤掉
烟测、验收、撤单、轮询、预览、调试和其他工程噪音。它不能展示密码、私钥、签名
payload 或其他敏感字段。

Dashboard 必须提供 `Current Orders / Strategy Orders` 面板，用于展示当前交易所仍然
有效的挂单，尤其是 Fib Basic 这类 maker 接针策略生成的入场挂单，以及 Manual Trading
产生的 maker resting orders。该面板必须：

- 通过内部 API 聚合执行模块的 open-order 快照与策略模块的实例状态，不得由前端直接推断
  策略状态。
- 按市场分组展示，避免 `xyz_perp` / `hl_perp` / `spot` 的挂单混在一起。
- 显示账号、交易对、来源模块、策略 ID、timeframe、Fib 档位、接针区、计划 TP/SL、
  买卖方向、挂单价、当前价、距离、数量和状态。
- 提供当前版本的 `Cancel All / Stop Strategies` 动作。该动作必须由后端重新拉取
  交易所 open orders 后执行，不得信任前端缓存；它会跨所有市场撤销当前版本能证明归属的
  Fib 入场挂单，并把当前版本所有仍会自动运行的 Fib 策略标记为 stopped / `auto_loop=false`，
  防止无挂单策略稍后继续自动挂单。
- 单个 Fib 策略的停止按钮仍必须调用 Fib 模块内部 API；Manual maker resting orders、其他
  版本/其他工具的订单，以及已有持仓保护用的 TP/SL 单必须显示但不能被这个批量动作撤销。

## Manual Trading

Manual Trading 页面用于手动买卖、撤单、多账号操作、人工止盈止损。

它对应 [基本操作模块](manual-operations.md)。

关键规则：

- 点击买卖生成 `ManualTradeIntent`。
- 撤单生成 `CancelCommand`。
- 所有人工交易请求进入风控网关。
- 多账号批量操作拆成每账号独立请求。
- 每个账号请求由对应 account worker 独立执行。
- Manual 页采用统一表单 + 模式切换：`Dry Run` / `Live` 两个状态按钮，字段布局保持一致。
- 下单金额输入采用“本金 USD + 杠杆倍率 X”，前端自动计算有效 notional 并展示。
- Manual 页字段布局按交易所常见下单顺序组织：先账户/标的，再方向与订单类型，再本金杠杆与风控参数，最后执行动作与结果。
- Live 下单时，前端会先自动应用当前填写的杠杆倍率，再提交订单；不再提供单独“Set Leverage”按钮。
- 下单 `Order Type` 语义与交易所一致：
  - `Market (IOC)`：立即成交，剩余撤销。
  - `Limit Post-Only (ALO)`：只挂单，若会立即成交则取消。
- 主下单按钮跟随方向联动：买入为绿色“Place Buy Order”，卖出为红色“Place Sell Order”。
- Manual 页右侧提供按当前选中 `coin` 刷新的实时行情卡片（mark / mid / oracle / funding / OI / 24h volume）。
- `Transfer USDC` 从主下单表单中拆出，放在 Manual 页右侧底部独立面板，和买卖逻辑分区展示。
- 页面默认只选中第一个账号；`Select All` 用于批量操作，`First Account` 用于快速回到
  单账号视图。
- 页面只保留 4 类主动作：
  - 点击 `Buy` / `Sell`：立即执行买卖。Dry Run 模式走 `/api/manual-order`；Live 模式走
    `signed-runbook submit`（含 preflight 阻断）。若 TP/SL 参数已填写，Live 下单成功后会自动
    追加提交对应 TP/SL。
  - `Transfer USDC`：资金划转。Dry Run 模式走批量只读计划；Live 模式按所选账号逐一 fan-out
    到 `/api/usdc-dex-transfer-runbook` 提交。
  - `TP/SL` 面板：统一入口。Manual 页该面板仅用于配置和预览，不单独提交真实 TP/SL 订单；
    真实提交仅在 Live 买卖成功后自动触发。Dashboard 持仓面板可直接对当前持仓提交原生
    TP/SL 更新。
- `Advanced` 区域提供：
  - `Advanced` 按钮放在高级字段旁边，同位置展开/收起，避免与主模式按钮分离。
  - 在线修改 `max_manual_order_notional_usd` 与所选账户 `max_order_notional_usd`，并持久化写回
    `config/local.toml`。
  - 上限输入采用 1 USD 步进。
  - `manual` 模块黑名单：通过 `module_symbol_policies.manual_blocked_symbols` 管理；留空表示不屏蔽任何符号。
- TP/SL 提供两种输入模式：
  - `Trigger Price (Exchange-like)`：直接输入止盈/止损触发价（更贴近交易所）。
  - `Ratio Mode (TP % / SL %)`：止盈和止损都按本金百分比输入；perp 按杠杆换算为
    价格触发比例（`price_move_pct = principal_pct / leverage`），spot 固定按 1 倍计算。
- Manual 页的 TP/SL 按钮只做计划预览，不单独提交真实 TP/SL；实盘 `Buy/Sell` 成功后
  自动用官方 `/exchange` trigger order 字段提交保护单。Dashboard 持仓面板可直接对
  当前已有仓位提交交易所原生 TP/SL。
- Live 模式下 `Buy/Sell`、持仓面板 `Set TP/SL` 与 `Transfer USDC` 都支持多账号批量执行
  （前端按账号 fan-out，每账号独立提交与返回结果）。
- 所有 Live 动作必须先通过 readiness / live gate：`app.dry_run=false`、
  `manual_live_enabled=true`、Vault 已解锁、密钥可用、账户资金/仓位满足条件、最小下单额
  满足要求。
- 如果 Vault 未解锁，前端必须在用户点击 Live 动作时立即提示并引导到 Vault 页，而不是静默无响应。
- 主网 `Buy/Sell` 与 `Transfer USDC` 都以 Vault 解锁 + live/readiness gate 作为提交前置条件。
- `Order Type`、`Reduce Only` 参数在 Dry Run 与 Live 两种模式下保持同样语义，并传入同一套
  后端校验链路。
- 前端可以轮询本地后端接口刷新画面，但后端数据源必须 WebSocket-first：
  - `state` 与手动行情等 UI 刷新读取本地 realtime cache；
  - Dashboard 深度快照（资金层汇总/持仓汇总）读取 WS 账户状态，缓存缺失才兜底 REST；
  - 页面重新激活（窗口回到前台）时可以补拉本地后端状态，不得直接触发 REST 热循环。
- 后端 `/info` 请求必须启用本地防抖与风控：
  - 市场 universe / snapshot 使用短 TTL 缓存，避免每次刷新都请求重型元数据；
  - 轻量行情优先走 WS `allMids` realtime cache；
  - open orders、fills、clearinghouse/spot state 优先走对应 WS 订阅缓存；
  - 命中 HTTP 429 或可重试状态时必须执行指数退避（含 jitter）并遵守 `Retry-After`；
  - 触发全局冷却窗口期间，新请求必须等待冷却结束再发送。

## Vault

Vault 页面用于共享本地密钥库，不是每个账号单独设置密码。

规则：

- `Unlock Vault` 只需要共享 vault 密码，不出现确认密码。
- `Change Password` 默认收起，展开后才显示当前密码、新密码、确认新密码；前端必须先校验
  新密码长度和确认一致性，再调用后端改密接口。
- 保存或测试 API wallet secret 时，如果后端会话已经解锁，可以复用该会话；否则必须在
  共享密码输入框中提供 vault 密码。
- 解锁成功后，后端必须把 vault 中已有条目的 `account_id`、地址和 `secret_id` 同步到
  当前运行配置，使这些地址出现在 Dashboard/Manual 中并参与 preflight。同步不得回显或
  写入私钥，也不得覆盖账号原有风控额度。
- API wallet secret 表单只管理每个账号的 `account_id`、`secret_id`、地址和 API wallet
  private key；它不得暗示每个账号拥有独立 vault 密码。

## Fib Retracement

Fib Retracement 页面用于斐波那契策略配置、启停、监控和人工干预。详细规格见
[斐波那契回撤策略开发文档](fibonacci-retracement-development.md)。

页面必须拆成两个页签：

- `基础版`：用户手动配置 timeframe、回撤档位、回撤冗余、本金杠杆、止盈止损模式；
  点击启动后由策略状态机自动挂单、调单、成交后提交 TP/SL。
- `AI 进阶版`：第一阶段只展示观察和建议框架，未来用于 ZigZag/Pivot 波段选择、支撑
  压力共振和风控过滤；不得直接绕过基础版状态机实盘下单。

当前控制台实现（MVP）以基础版状态机为入口：

- 支持与 Manual 页一致的 `Dry Run / Live` 模式切换、账号多选、币种下拉与搜索。
- 支持方向选择：`做多回撤` / `做空反弹`。`hl_perp` 和 `xyz_perp` 可选择做空；
  `spot` 下做空按钮必须灰色不可点击，并在切入 spot 时自动回到做多。
- 支持基于 `candleSnapshot` 的自动识别：按 timeframe + lookback 自动计算 swing high/low 与
  0.382 / 0.618 回撤位，并展示当前价格距离与容差命中状态。
- Fib 页必须提供 `Running Fib Strategies` 面板，展示本模块当前运行实例、最新状态、账号、
  挂单数量、回撤线、接针区和计划 TP/SL。该面板可以复用 Dashboard 聚合接口返回的数据，
  但不能越过 Fib 内部 API 修改订单或状态。
- Running Fib Strategies 面板必须提供：
  - `Load / Edit`：把运行实例加载到左侧表单，用户可修改 timeframe、回撤档位、金额、杠杆、
    TP/SL 等参数后点击 `Refresh Params`。
  - `Stop + Cancel`：停止策略并撤销未成交入场挂单；已有持仓的交易所原生 TP/SL 保护不得被
    随意取消。
- Fib 基础版支持 `Auto Loop`：一轮买入、交易所原生 TP/SL 卖出完成并确认仓位归零后，按
  当前策略参数和冷却时间自动重新挂下一轮。
- `Start Basic Strategy` / `Refresh Params` 只能调用 Fib 模块自己的内部 API，生成或更新
  策略实例和入场信号；不得直接调用 `/api/manual-order` 或 `/api/signed-runbook`。
- 页面文案必须明确区分“策略实例/策略信号”和“立即提交交易所订单”。在执行 worker 尚未
  接管某个信号前，不能把按钮标成“实盘提交订单”。
- Fib 页高级设置维护 `module_symbol_policies.fib_blocked_symbols`；留空表示不屏蔽任何符号。

必须展示：

- 策略实例列表。
- 每个实例的 enabled / paused / killed 状态。
- symbol。
- timeframe。
- lookback bars。
- 当前 high / low。
- 0.382 和 0.618 回撤价格。
- 当前价格距离回撤点的距离。
- entry tolerance。
- 已触发档位。
- 当前策略持仓。
- 策略止盈价和止损价。
- 最近策略信号。
- 最近风控拒绝。

允许操作：

- 新增策略实例。
- 修改策略参数。
- 刷新参数：未成交时撤旧挂新；已成交时只更新 TP/SL，不重复开仓。
- 启用 / 暂停策略。
- 触发策略级 kill switch。
- 清理已完成实例状态。

约束：

- 页面修改配置不应直接改变正在运行的关键状态，必须走配置变更命令。
- 配置变更需要校验，失败时不得热更新。
- 主网 live 修改关键参数建议二次确认。

## Smart Money Copy

Smart Money Copy 页面用于跟单策略配置、启停、监控和去重状态观察。

必须展示：

- leader 地址列表。
- leader group。
- 每个 leader 的启用状态、copy ratio、限额。
- 每个 symbol 的跟单限额。
- 最近 leader events。
- 识别后的 leader 行为：open / increase / reduce / close / flip。
- 生成的跟单意图。
- 去重命中记录。
- 跟单延迟。
- 每个 `signal_id` 对各 account workers 的广播、ack、拒绝、提交、成交状态。
- 最近风控拒绝。
- 本账户和 leader 的仓位映射状态。

允许操作：

- 新增 / 禁用 leader。
- 调整 leader copy ratio。
- 调整 symbol limit。
- 暂停跟单策略。
- 对某个 leader 或 symbol 触发 kill switch。
- 手动标记 leader group。

约束：

- 前端不得因为 leader 信号直接下单。
- 配置变更必须写审计日志。
- 删除 leader 不应删除历史审计记录。
- Copy 页高级设置维护 `module_symbol_policies.copy_blocked_symbols`；留空表示不屏蔽任何符号。

模块隔离要求：

- Manual/Fib/Copy 共享后端 API 端点时，前端必须显式携带 `source_module`。
- 后端只允许按 `source_module` 对应模块黑名单做校验，不得回退为全局混用。

## 风控展示

所有页面都应能看到关键风控状态：

- 全局 kill switch。
- 策略级 kill switch。
- symbol 级 kill switch。
- 当前账户风险暴露。
- 当前 max order / max position 使用比例。
- 最近拒绝原因。

## 实时更新

当前实现：

- 手动交易行情使用 WebSocket 推送（`/ws/manual-quote`），前端按当前 market/coin 订阅，
  断线自动重连；连接不可用时回退到低频 HTTP 拉取。
- 交易与配置操作仍通过 HTTP POST。

后续可继续把 dashboard/策略状态从轮询升级到统一推送通道。

## 安全边界

- 默认只绑定 `127.0.0.1`。
- 不在前端保存私钥。
- 不把私钥、签名 payload、完整 secret 返回前端。
- mainnet live 下单和关键配置修改必须二次确认。
- 所有操作写入审计日志。

## 第一版验收标准

- 能打开 Dashboard，并看到资金、仓位、PnL、open orders。
- Dashboard 能看到每个地址 worker 的健康状态。
- 能打开 Manual Trading，并提交 dry-run 手动订单。
- 能打开 Fib Retracement，并查看/修改策略配置。
- 能打开 Smart Money Copy，并查看/修改 leader 配置。
- 所有前端操作都有审计日志。
- 所有交易类操作都经过风控网关。
