# 斐波那契回撤策略开发文档

## 定位

斐波那契模块是一套半自动接下跌针、成交后自动止盈止损的策略系统。它必须遵守项目
既有边界：策略只生成交易意图，不直接签名、不直接调用交易所 `/exchange`，所有订单都
必须经过策略风控、账户风控、执行风控和对应 account worker。

本模块分两层：

- 基础版：用户手动选择市场、交易对、时间周期、回撤档位和止盈止损参数，系统按规则
  自动挂单、调单、成交后提交保护单。
- AI 进阶版：系统辅助识别有效波段和支撑压力共振，自动挑选更合适的区间和参数。第一
  阶段只落架构和占位，不接入实盘自动决策。

第一阶段优先落基础版。AI 进阶版只能作为 `observe/suggest` 框架存在，不能绕过基础版
的风控和执行链路。

## 常规量化口径

本项目基础版支持“做多回撤”和“做空反弹”两个方向；第一版历史实现默认做多。多空扩展
的完整实现规格见 [斐波那契基础版多空方向扩展开发文档](fibonacci-long-short-extension.md)。

一个合格的斐波那契多头回撤区间应满足：

- 有一段明确上涨波段：`swing_low` 在时间上早于 `swing_high`。
- 回撤线基于这段上涨波段计算，而不是任意窗口里的最高低点简单相减。
- 基础版可以先用确定性窗口算法寻找波段，但必须校验低点早于高点。
- AI 版未来使用 ZigZag/Pivot、历史支撑压力、成交量、VWAP/均线、波动率和多周期共振来
  选择更可靠的波段。

做多回撤价：

```text
fib_price(level) = swing_high - (swing_high - swing_low) * level
```

做空反弹区间应满足 `swing_high` 在时间上早于 `swing_low`，做空反弹价为：

```text
fib_price_short(level) = swing_low + (swing_high - swing_low) * level
```

做空适用于合约市场：`hl_perp`、`xyz_perp` 和 `cash_perp`；`spot` 前端
必须把做空置灰不可点击，后端也必须拒绝 spot short payload。`cash_perp`
对应 HIP-3 `cash` perp DEX，应在 UI 中显示为 `Cash Perps`，不要归类为
trade[XYZ] 的 `xyz` DEX。

常用档位：

```text
0.236, 0.382, 0.5, 0.618, 0.786
```

第一版默认启用：

```text
0.382, 0.618
```

如果价格跌破 `swing_low` 或用户设置的结构失效价，当前斐波那契区间失效，未成交挂单
必须撤销，已成交仓位只能按止损、平仓或用户新参数处理。

### 残余仓位清理规则

- Fib Basic 的实盘合约入场按“新仓”处理。每个目标账号在提交入场信号前，先强制读取
  同市场、同交易对的最新仓位；若存在任何非零残仓，先通过内部交易 API 提交
  `reduce_only + close_full_position + IOC` 清理单，确认仓位严格归零后才允许提交新的
  Fib 入场。
- Fib Basic 一轮 TP/SL 出场后，不能只按“低于 0.5U 视为平仓”的展示口径进入下一轮。
  若交易所仍返回非零合约 size，先自动提交 reduce-only 扫尾；扫尾失败或 Vault 未解锁时，
  暂不重启下一轮，避免残仓影响下一次开多/开空方向。
- 该清理逻辑只适用于合约市场。Spot 受 lot size 限制，不能假设所有 dust 都可通过订单簿卖出。

## 基础版功能规格

### 用户输入

基础版页面应提供以下字段：

- 市场：`xyz_perp` / `hl_perp` / `cash_perp` / `spot`。
- 方向：做多回撤 / 做空反弹。spot 只允许做多，做空按钮置灰不可点击。
- 账户：支持单账号和多账号，按当前市场过滤可用账户。
- 交易对：下拉列表 + 搜索，显示交易所前端友好的 label，同时内部保存 canonical coin。
- 时间周期：`1m` / `5m` / `15m` / `1h` / `4h` / `1d`，后续可扩展 `1w` / `1M`。
- 区间来源：
  - 自动窗口：按 timeframe + lookback bars 找最近有效上涨波段。
  - 锁定当前区间：用户确认后固定 `swing_low/swing_high`，刷新参数不重新找区间。
- 回撤档位：默认 `0.382`、`0.618`，支持多选。
- 回撤冗余：
  - `entry_above_tolerance`：允许在回撤价上方提前接针。
  - `entry_below_tolerance`：允许在回撤价下方继续接受成交。
  - 单位支持 USD 价格差；后续可扩展为百分比或 ATR。
- 本金 USD。
- 杠杆倍率：
  - perp 可填。
  - spot 固定为 1，字段置灰。
- 执行方式：
  - `成交优先 / Taker IOC`：基础版默认。价格进入接针区后提交 IOC，按当前参考价和
    `max_slippage_bps` 生成保护限价，优先保证买到；不得把 Fib 回撤价作为固定限价传入，
    否则会在价格位于接针区上沿时错过成交。
  - `挂单优先 / Maker Post-only`：高级选项。提前挂限价单，若会立即吃单则取消或被交易所
    ALO 拒绝，适合愿意用错过窗口换 maker 手续费优势的场景。
- 每档位下单方式：
  - 等额拆分：每个启用档位分配相同 notional。
  - 权重拆分：例如 0.382 小仓，0.618 大仓。
  - 第一版可先做等额拆分。
- 止盈模式：
  - 价格绝对差：`take_profit_price = fill_price + tp_delta_usd`。
  - 本金盈利百分比：perp 用 `price_move_pct = profit_pct / leverage`，spot 用
    `price_move_pct = profit_pct`。
- 止损模式：
  - 价格绝对差：`stop_loss_price = fill_price - sl_delta_usd`。
  - 本金亏损百分比：perp 用 `price_move_pct = loss_pct / leverage`，spot 用
    `price_move_pct = loss_pct`。
- 最大滑点 bps。
- 策略级上限：
  - 单次最大本金。
  - 单策略最大持仓价值。
  - 每档位最大成交次数。
  - 冷却时间。
  - 自动循环：一轮买入、交易所原生 TP/SL 卖出完成后，按当前参数重新计算并挂下一轮。
  - 模块黑名单。

### 回撤冗余语义

以做多为例，某档回撤价为 `L`：

```text
entry_zone_high = L + entry_above_tolerance
entry_zone_low  = L - entry_below_tolerance
```

处理规则：

- Maker 模式：默认挂买入限价在回撤线/策略计算出的 maker 入场价。这样价格跌到回撤价上方的
  冗余区域时即可提前成交，适合接快速下跌针，但若提交瞬间会吃单，交易所可能拒绝 ALO。
- Taker 模式：当参考价进入 `[entry_zone_low, entry_zone_high]` 时提交 IOC 买入。该模式使用
  当前参考价和滑点保护生成限价，不使用 Fib 线价格作为固定限价。
- 如果价格直接跌破 `entry_zone_low`，未成交挂单按策略配置选择：
  - 保守模式：撤单并等待下一次区间刷新。
  - 激进模式：保留到结构失效价前，但必须受最大滑点和最大亏损约束。
- 第一版默认保守模式。

### 基础版状态机

每个账号、市场、交易对、时间周期、区间版本组合形成一个策略实例。

```text
Draft
  -> ArmedUnfilled
  -> EntryPending
  -> EntryFilled
  -> ProtectionPending
  -> Protected
  -> Exiting
  -> Completed

Paused / Killed / Error 可以从任意运行态进入。
```

状态含义：

- `Draft`：页面已配置但未启动。
- `ArmedUnfilled`：策略已启动，当前没有已提交入场单。
- `EntryPending`：已有一个或多个回撤挂单。
- `EntryFilled`：入场已成交，等待提交 TP/SL。
- `ProtectionPending`：TP/SL 正在提交或对账。
- `Protected`：交易所原生 TP/SL 已生效。
- `Exiting`：止盈、止损、一键平仓或结构失效触发退出。
- `Completed`：本轮实例已结束。
- `Paused`：暂停新增入场，但保留已成交仓位保护。
- `Killed`：停止策略并尽可能撤销未成交挂单；是否平仓由用户选项决定。
- `Error`：发生不可静默恢复错误，必须 fail-closed。

### 自动循环

基础版策略默认支持自动循环。循环规则：

1. 只有在本轮入场成交后，交易所原生 TP/SL 或人工退出使对应账号、市场、交易对仓位归零，
   且策略保护单不再处于 open orders 中时，才认为本轮完成。
2. 本轮完成后先进入 `Completed`，记录 `completed_cycles`、`last_cycle_completed_at_ms`
   与可识别的 `last_cycle_exit_kind`。
3. 如果本轮由止损触发退出，且用户启用“止损后停止策略”，实例必须置为 `Killed`，
   关闭 `auto_loop`，不再重挂。
4. 如果本轮由止损触发退出，且用户未启用“止损后停止策略”，下一轮必须使用
   `stop_loss_cooldown_secs` 作为冷却时间。
5. 如果本轮由止盈退出，下一轮立即重新评估 Fib 入场条件；是否真正挂单仍由当前价格、
   回撤区间、执行模式和风控决定。
6. 如果本轮无法可靠识别退出原因，或本轮没有形成已接受的实盘入场单，下一轮使用普通
   `cooldown_secs` 作为异常/重试冷却。
7. 如果 `auto_loop=true`，reconciliation loop 在冷却时间结束后，用同一个 `strategy_id` 和
   当前保存的策略参数重新计算 swing、Fib 档位、接针区和 TP/SL，并通过 Fib 内部 API 重新
   生成入场挂单。
8. 自动循环不得在仍有持仓、仍有保护单或仍有未成交入场挂单时重复开仓。
9. 如果用户在运行中刷新参数，后续循环必须使用刷新后的时间周期、回撤档位、金额、杠杆和
   TP/SL 设置。
10. 多账号策略实例必须把每轮入场视为同步批次。每个生成的入场信号都必须覆盖所有目标
   账号；若只有部分账号提交成功，系统必须撤销尚未成交的部分挂单，保护已经成交的仓位，
   暂停 `auto_loop`，并在前端和历史日志中明确显示缺失账号。

### 启动流程

用户点击 `启动` 后：

1. 前端提交 `CreateFibInstance` 或 `StartFibInstance` 命令。
2. 后端校验账户、市场、交易对、时间周期、参数范围、模块黑名单和 live gate。
3. 策略引擎计算或读取当前 `swing_low/swing_high`。
4. 生成 `fib_line_version`。
5. 对每个启用档位计算 entry zone 和订单计划。
6. 生成 `TradeIntent` 或 `OrderPlan`，通过风控网关。
7. 每个账号 worker 独立提交订单。
8. 后端校验每个入场信号是否覆盖所有目标账号。只要有账号缺失，就不得把策略视为健康
   `EntryPending` 或 `Protected`；必须暂停自动循环并记录缺失账号。
9. 写审计日志和策略状态。

### 刷新参数流程

用户运行中修改参数后点击 `刷新参数`。

#### 未买到

未买到包括 `ArmedUnfilled` 和 `EntryPending`：

1. 校验新参数。
2. 如果区间来源是自动窗口，重新计算 `swing_low/swing_high` 和 `fib_line_version`。
3. 撤销旧版本未成交入场单。
4. 按新参数重新生成挂单。
5. 写入 `ConfigUpdated` 和 `OrdersReplaced` 审计事件。

#### 已买到

已买到包括 `EntryFilled`、`ProtectionPending`、`Protected`：

1. 入场价必须固定为真实成交均价，不因刷新参数改变。
2. 刷新参数只影响止盈止损、冷却和后续是否允许加仓。
3. 如 TP/SL 已提交，必须取消或替换旧保护单，再提交新保护单。
4. 如保护单替换失败，策略进入 `Error` 或 `ProtectionPending`，并保留清晰提示。
5. 不得因为刷新参数而重复开仓，除非用户明确启用“已成交后继续接下一档”。

### 停止策略与撤单

用户可以在 Dashboard 的当前委托面板或 Fib 页运行实例面板点击 `Stop + Cancel`：

- 后端调用 Fib 模块自己的内部 API，不得走 Manual Trading 模块 API。
- 对未成交入场挂单按 `cloid` 提交交易所原生撤单。
- 将策略状态置为 `Killed`，并关闭 `auto_loop`，防止后续自动重挂。
- 如果策略已经有持仓和交易所原生 TP/SL，停止策略默认不取消保护单，避免裸仓。
- 停止请求必须先写入控制面状态，再执行撤单和实例更新。后台 reconciliation 可能已经拿到
  旧的 `Completed + auto_loop=true` 快照；因此自动重启链路在提交 entry 前、提交后、
  写回实例状态前，都必须重新检查 stop request。旧快照不得把 `Killed` / `auto_loop=false`
  覆盖回活动策略状态。

### 策略历史与恢复

Fib 基础版必须同时维护两类状态：

- `logs/fib_instances.json`：当前可恢复、可继续运行或可操作的实例表。
- `logs/fib_instance_history.jsonl`：append-only 生命周期账本，记录每次启动、刷新、挂单、
  成交后保护、完成一轮、等待下一轮、停止和异常。

当前实例表允许被最新状态覆盖，但历史账本不得覆盖或删除。即使策略止盈/止损平仓、
进入冷却、用户停止或后台重启，前端也必须能通过历史账本说明该策略最后处于什么
状态。

旧版本只写入 `audit.jsonl`、没有写入 `fib_instances.json` 或历史账本的策略，只能恢复为
“审计日志历史记录”。这类记录用于解释历史，不得被当作当前运行策略自动恢复下单。

### 成交后保护

入场成交后，策略必须用真实成交信息计算保护单：

- `fill_price`：成交均价。
- `fill_size`：实际成交数量。
- `remaining_size`：剩余仓位数量。
- `entry_level`：命中的 fib 档位。

止盈止损通过交易所原生 TP/SL 提交：

- Perp 优先使用交易所原生 position TP/SL 或兼容 trigger order。
- Spot 使用交易所支持的 native generic TP/SL grouping；若某交易对或最小数量不支持，必须
  明确拒绝或跳过，不得伪装成已保护。
- 所有保护单必须 reduce/close 语义正确，不能造成反向开仓。
- 保护单必须带 `cloid`，便于重启恢复和对账。

## AI 进阶版框架

AI 进阶版的目标是自动选择“更值得接”的回撤区间，而不是直接绕过风控自动乱下单。

### AI 工作模式

第一阶段只预留三种模式：

- `Observe`：只分析并展示建议，不生成交易意图。
- `Suggest`：生成候选策略参数，等待用户点击启动。
- `Auto`：未来可在严格限制下自动启动基础版实例；第一阶段禁用实盘。

### AI 输入

AI 版可使用的输入：

- 多周期 K 线。
- ZigZag/Pivot 波段。
- 当前 mark/mid/oracle。
- 历史成交量和波动率。
- VWAP、均线、ATR。
- 历史支撑压力位。
- 当前账户风险状态。
- 当前市场可交易状态和 funding。

AI 版不得读取或接收：

- 私钥。
- 签名 payload。
- 未脱敏的 vault 内容。
- 任何不需要进入模型的敏感账户信息。

### AI 选区规则

候选波段至少包含：

- `swing_low`
- `swing_high`
- `start_time`
- `end_time`
- `timeframe`
- `trend_strength`
- `volatility`
- `confluence_score`
- `invalidation_price`

AI 或规则引擎可对以下因素评分：

- ZigZag/Pivot 波段清晰度。
- 0.382/0.618 是否与历史水平支撑重合。
- 是否靠近 VWAP、均线或成交密集区。
- 回撤前上涨幅度是否足够。
- 当前价差、流动性和滑点是否可接受。
- 高周期趋势是否同向。
- 距离结构失效价的风险收益比。

### AI 启停条件

用户只需要配置工作边界：

- 启动价格区间：价格在区间内才允许策略启动。
- 停止价格区间：价格离开区间则平仓或停止新开仓。
- 启动时间段：例如只在美股常规盘或指定小时运行。
- 停止条件：
  - 达到日内盈利目标。
  - 达到日内亏损上限。
  - 连续失败次数超过阈值。
  - 波动率或价差异常。
  - 行情/账户/订单状态失联。
- 最大自动启动实例数。
- 最低 AI 置信度或最低共振分数。

### AI 输出

AI 输出只能是候选配置，不是订单：

```rust
pub struct FibAiProposal {
    pub proposal_id: String,
    pub mode: FibAiMode,
    pub market: MarketKind,
    pub coin: CanonicalCoin,
    pub timeframe: String,
    pub swing_low: Decimal,
    pub swing_high: Decimal,
    pub levels: Vec<Decimal>,
    pub entry_tolerance: EntryTolerance,
    pub take_profit: FibTakeProfitConfig,
    pub stop_loss: FibStopLossConfig,
    pub invalidation_price: Decimal,
    pub confidence: Decimal,
    pub reasons: Vec<String>,
}
```

即使未来 `Auto` 模式启用，AI 输出也必须先转成基础版 `FibStrategyConfig`，再走同一套策略
状态机、风控网关、account worker 和审计日志。

## 后端 API 设计

第一版建议新增或重构以下内部接口：

```text
GET  /api/fib/instances
POST /api/fib/instances
PATCH /api/fib/instances/{strategy_id}
POST /api/fib/instances/{strategy_id}/start
POST /api/fib/instances/{strategy_id}/pause
POST /api/fib/instances/{strategy_id}/resume
POST /api/fib/instances/{strategy_id}/refresh-params
POST /api/fib/instances/{strategy_id}/cancel-entry-orders
POST /api/fib/instances/{strategy_id}/close-position
GET  /api/fib/instances/{strategy_id}/status
POST /api/fib/ai/proposals
```

兼容现有 `/api/fib/preview` 和 `/api/fib/auto-detect`：

- `preview` 保留为计算器，不提交订单。
- `auto-detect` 可作为基础版创建实例前的区间预览。
- 旧 `sniper` 一键下单口径已废弃；前端按钮应表达为“启动基础版策略实例”，避免临时
  按钮绕过状态机或直接调用 Manual 下单接口。

当前已落地的 v0 前端兼容接口：

```text
GET  /api/fib/instances
POST /api/fib/instances
POST /api/fib/instances/start
POST /api/fib/instances/refresh-params
POST /api/fib/ws-candle-probe
POST /api/fib/ai/proposals
```

这些接口属于 Fib 模块自己的内部 API。Fib 前端不得直接调用 `/api/manual-order`、
`/api/signed-runbook` 等手动交易接口；需要下单时只能输出 `CoordinatorSignal`
或后续扩展的策略实例命令，由 coordinator/account worker 按模块来源继续处理。
`start` 和未成交状态下的 `refresh-params` 当前会生成 Fib 自己的
`CoordinatorSignal`，再通过 `RiskGateway -> AccountExecutor` 内部链路执行。
dry-run 模式会返回 account-worker 提交报告；live 模式只允许在显式关闭
frontend/config dry-run、Vault 已解锁、风控通过时继续。Fib 仍不得直接调用 Manual
下单接口。

当价格未进入接针条件时状态保持 `armed_unfilled`，不得显示成已有挂单或已成交。
`armed_unfilled` 不是停止态，而是运行中等待入场条件的状态。后台 reconciliation loop
必须优先读取 websocket/local realtime cache 更新当前价和距离；当 maker/taker 入场条件
重新满足时，只能通过 Fib `CoordinatorSignal -> RiskGateway -> AccountExecutor` 内部链路提交
entry，不得从策略层直接调用手动交易接口。maker 模式下，如果当前价已经跌破买入限价，
说明这一轮接针已错过，不得提交会立即成交或被 ALO 拒绝的限价单；应继续等待价格重新
回到入场价上方，或等待用户刷新参数/停止策略。若 `auto_loop=true` 且 `locked_range=false`，
未入场状态不得无限死守旧回撤线；本轮未能形成有效 entry signal 时应进入冷却等待，
冷却结束后重新拉取当前周期 candles、重新推断 swing，再按最新 Fib 线判断是否挂单。

maker 模式会按选定斐波那契点生成限价/挂单意图。参数刷新在未成交时会重新生成和提交
新的 entry intents；后续必须补上 live resting entry order 的 cancel/replace 对账。

`/api/fib/ws-candle-probe` 是官方 Hyperliquid candle websocket 探针，用于验证当前
market/coin/timeframe 的 WS 数据链路。它不是完整的常驻策略行情循环；后续常驻运行时
仍应订阅官方 market/order/user streams，并把事件转成内部策略事件。

## 核心数据结构

```rust
pub enum FibMode {
    Basic,
    AiAdvanced,
}

pub enum FibAnchorMode {
    AutoWindow,
    LockedRange,
    AiSelected,
}

pub enum FibInstanceStatus {
    Draft,
    ArmedUnfilled,
    EntryPending,
    EntryFilled,
    ProtectionPending,
    Protected,
    Exiting,
    Completed,
    Paused,
    Killed,
    Error,
}

pub enum ProfitLossMode {
    PriceDeltaUsd,
    PrincipalPercent,
}

pub struct FibStrategyConfig {
    pub strategy_id: String,
    pub mode: FibMode,
    pub market: MarketKind,
    pub account_ids: Vec<String>,
    pub coin: CanonicalCoin,
    pub timeframe: String,
    pub lookback_bars: u32,
    pub anchor_mode: FibAnchorMode,
    pub locked_swing_low: Option<Decimal>,
    pub locked_swing_high: Option<Decimal>,
    pub levels: Vec<Decimal>,
    pub entry_above_tolerance_usd: Decimal,
    pub entry_below_tolerance_usd: Decimal,
    pub principal_usd: Decimal,
    pub leverage: Decimal,
    pub execution_mode: ExecutionMode,
    pub take_profit_mode: ProfitLossMode,
    pub take_profit_value: Decimal,
    pub stop_loss_mode: ProfitLossMode,
    pub stop_loss_value: Decimal,
    pub max_slippage_bps: u32,
    pub max_entries_per_level: u32,
    pub cooldown_secs: u64,
}

pub struct FibStrategyState {
    pub strategy_id: String,
    pub status: FibInstanceStatus,
    pub fib_line_version: String,
    pub swing_low: Decimal,
    pub swing_high: Decimal,
    pub active_levels: Vec<FibLevelState>,
    pub entry_orders: Vec<StrategyOrderRef>,
    pub entry_fill: Option<EntryFillState>,
    pub protective_orders: Vec<StrategyOrderRef>,
    pub last_error: Option<String>,
    pub updated_at: i64,
}
```

## 前端设计

Fib 页面拆成两个页签：

```text
基础版 | AI 进阶版
```

### 基础版页面

顶部：

- Dry Run / Live 状态切换。
- 市场、账户、交易对、时间周期。
- 当前行情卡片。

参数区：

- 区间来源。
- lookback bars。
- 回撤档位多选。
- 回撤冗余。
- 本金、杠杆、有效 notional。
- 执行方式。
- 止盈模式和值。
- 止损模式和值。
- 高级设置：黑名单、最大仓位、冷却、每档最大次数。

操作区：

- `预览回撤线`：只计算，不下单。
- `启动策略`：创建/启动实例。
- `刷新参数`：按已买到/未买到状态处理。
- `暂停`：停止新增入场。
- `撤销未成交挂单`。
- `一键平仓`。

状态区：

- 当前区间高低点。
- 0.382 / 0.618 等回撤价格。
- 每档位 entry zone。
- 当前策略状态。
- 未成交挂单。
- 已成交入场。
- TP/SL 保护单。
- 最近策略事件。
- 最近风控拒绝。

### AI 进阶版页面

第一阶段只展示框架：

- AI 工作模式：Observe / Suggest / Auto（Auto 置灰）。
- 启动价格区间。
- 停止价格区间。
- 启动时间段。
- 停止条件。
- 最低置信度。
- 候选区间列表。
- AI 建议原因。
- `采用为基础版参数` 按钮。

AI 页不得直接显示“实盘启动”按钮；采用建议后进入基础版参数页，再由用户启动。

## 风控规则

基础版策略专属风控至少覆盖：

- 单策略最大本金。
- 单账户最大本金。
- 单交易对最大策略持仓价值。
- 每档位最大成交次数。
- 未成交挂单数量上限。
- 冷却时间。
- 模块黑名单。
- 行情过期拒绝。
- candle 数据不足拒绝。
- `swing_low` 晚于 `swing_high` 拒绝。
- 当前价已经跌破结构失效价时拒绝新入场。
- 价格超出 entry zone 时拒绝 taker 入场。
- 保护单数量或精度四舍五入为 0 时拒绝。

全局风控继续覆盖：

- app dry-run/live gate。
- Vault 解锁。
- 最小下单额。
- 账户可用资金。
- 杠杆上限。
- 价格和数量精度。
- rate limit 和 429 冷却。
- global kill switch。

## 持久化和恢复

策略实例必须持久化：

- 配置。
- 状态。
- line version。
- open order refs。
- protective order refs。
- entry fill refs。
- 最近参数版本。
- 最近审计事件游标。

重启恢复顺序：

1. 读取策略实例。
2. 拉取交易所 open orders、positions、fills。
3. 用 `cloid` 和 order id 对账。
4. 对未成交入场单恢复 `EntryPending`。
5. 对已有仓位但无保护单的实例进入 `ProtectionPending` 并尝试补保护。
6. 对状态无法确认的实例进入 `Error`，禁止新增入场。

## 测试计划

### 单元测试

- fib level 计算。
- 0.382 / 0.618 价格计算。
- `swing_low` 必须早于 `swing_high`。
- entry zone 计算。
- profit/loss 触发价计算。
- perp 杠杆下本金百分比换算。
- spot 下百分比换算。
- 参数刷新：未成交撤旧换新。
- 参数刷新：已成交只替换 TP/SL。
- 重复档位去重。
- 冷却时间。

### 回放测试

- 正常触及 0.382 成交后止盈。
- 正常触及 0.618 成交后止损。
- 快速穿透 entry zone，保守模式撤单。
- 更新 swing high 后旧 line version 失效。
- 跌破 swing low 后未成交挂单撤销。
- 重启后恢复未成交挂单。
- 重启后发现已有仓位但缺保护单。

### Dry Run 集成测试

- 创建实例。
- 预览回撤线。
- 启动策略并生成模拟挂单。
- 刷新参数替换模拟挂单。
- 模拟成交后生成 TP/SL。
- 多账号 fan-out 结果按账号分开展示。

### 小额实盘前检查

实盘前必须至少通过：

- 当前市场 metadata 正常。
- 当前交易对精度和最小数量正常。
- 当前账户资金可用。
- Vault 已解锁。
- 当前 app live gate 正确。
- 只启用一个交易对和极低 notional。
- 未成交挂单可以撤销。
- 成交后 TP/SL 能在交易所 open orders 中对账。

## 第一阶段验收标准

- 前端 Fib 基础版能创建、启动、暂停、刷新参数和查看状态。
- 回撤档位默认正确使用 `0.382` 和 `0.618`。
- 未买到时刷新参数会撤旧挂新。
- 已买到时刷新参数不会重复开仓，只更新 TP/SL。
- 成交后 TP/SL 基于真实成交均价，不基于预览价。
- 多账号运行时，每个账号独立 worker 执行，同一策略实例共享 `signal_id`。
- 任何失败都能在前端用用户可读文本展示，不输出原始 JSON 结构。
- AI 进阶版页面只做建议框架，不直接实盘下单。
