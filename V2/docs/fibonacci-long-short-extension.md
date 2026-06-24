# 斐波那契基础版多空方向扩展开发文档

## 定位

本文档定义 Fib 基础版从“只做多回撤”扩展为“合约可做多/做空、现货只做多”的开发规格。
它只覆盖基础版半自动交易场景：用户人工判断大方向，系统负责按参数计算 Fib 点位、提交
入场意图、成交后提交交易所原生 TP/SL，并按既有自动循环规则继续下一轮。

本扩展不得改变既有模块边界：

- Fib 策略模块只生成 Fib 自己的 `CoordinatorSignal` / `TradeIntent`。
- 不得从 Fib 前端直接调用 Manual Trading 的下单接口。
- 不得绕过 `RiskGateway`、account worker、executor、Vault 和 live gate。
- TP/SL 继续使用交易所原生触发单，不恢复本地监听触发。

## 市场能力

四个市场前端可以使用同一套方向控件，但能力必须按市场限制：

| 市场 | 做多 | 做空 | 说明 |
| --- | --- | --- | --- |
| `hl_perp` / Default Perps | 支持 | 支持 | Hyperliquid 原生 perp，Buy 开多，Sell 开空。 |
| `xyz_perp` / Equities [XYZ] | 支持 | 支持 | trade[XYZ] 是 Hyperliquid HIP-3 DEX，使用 `dex = "xyz"`。 |
| `cash_perp` / Cash Perps | 支持 | 支持 | HIP-3 `cash` perp DEX，使用 `dex = "cash"`；不是 trade[XYZ] 的 `xyz` DEX。 |
| `spot` | 支持 | 不支持 | 现货没有负仓位语义，前端做空按钮置灰不可点击，后端也必须拒绝。 |

现货页仍展示方向控件以保持四个市场的前端布局一致，但 `做空` 必须：

- disabled / 灰色；
- 鼠标悬停说明“现货不支持开空，只能买入持有后卖出”；
- 如果用户从合约市场的做空切到 spot，前端自动切回做多，并给出轻提示；
- 后端再次校验，拒绝 `market=spot && direction=short`，不能只依赖前端。

## 方向模型

新增策略方向字段：

```rust
pub enum FibTradeDirection {
    Long,
    Short,
}
```

序列化建议使用：

```text
long
short
```

兼容旧策略：

- `FibBasicPayload.direction` 缺省为 `long`。
- `FibBasicConfig.direction` 缺省为 `long`。
- 旧 `logs/fib_instances.json` 和历史日志中没有 direction 的记录，加载时按 `long` 解释。
- `strategy_id` 允许保留旧值，但新建默认 ID 建议包含方向，例如
  `fib_basic_hl_perp_ETH_5m_long` / `fib_basic_hl_perp_ETH_5m_short`。

一个市场 + 一个交易对仍然只能运行一个 Fib 策略，不允许同一交易对同时开多空两个 Fib
策略。基础版以简单和避免误判为优先；如果用户要从做多切换到做空，必须先停止旧策略并撤销
未成交入场单。

## 波段与 Fib 价格

### 做多回撤

做多是当前已实现逻辑的正式化：找一段上涨波段，等待价格回撤到 Fib 档位附近买入。

有效区间要求：

- `swing_low` 在时间上早于 `swing_high`。
- `swing_high > swing_low`。

公式：

```text
long_entry(level) = swing_high - (swing_high - swing_low) * level
```

语义：

- 越大的 level，入场价格越低，代表更深回撤。
- 入场方向：`Buy`。
- 平仓方向：`Sell reduce-only`。

### 做空反弹

做空是对称逻辑：找一段下跌波段，等待价格反弹到 Fib 档位附近卖出开空。

有效区间要求：

- `swing_high` 在时间上早于 `swing_low`。
- `swing_high > swing_low`。

公式：

```text
short_entry(level) = swing_low + (swing_high - swing_low) * level
```

语义：

- 越大的 level，入场价格越高，代表反弹越深。
- 入场方向：`Sell`。
- 平仓方向：`Buy reduce-only`。

## 自动识别

自动识别必须按方向选择不同的有效波段：

- `direction=long`：寻找有效上涨波段，低点早于高点。
- `direction=short`：寻找有效下跌波段，高点早于低点。

基础版仍保持简单确定性算法，不引入 AI 趋势判断：

1. 按 `timeframe + lookback_bars` 获取 K 线。
2. 在可用窗口中寻找方向匹配的最大有效波段。
3. 输出 swing high / swing low、时间戳、当前价、Fib 档位、当前价距离。
4. 如果找不到方向匹配波段，返回可读错误，不生成策略。

手动区间模式下，用户只填写价格高低点，前端按方向解释：

- 做多：人工低点到人工高点。
- 做空：人工高点到人工低点。

由于手动输入没有可靠时间戳，后端只校验 `high > low`、价格为正；时间顺序由用户人工负责。

## 入场区间

对任一方向，先计算方向对应的 `entry_price`。

```text
entry_zone_high = entry_price + entry_above_tolerance_usd
entry_zone_low  = entry_price - entry_below_tolerance_usd
```

### Taker / 成交优先

做多和做空都使用同一命中规则：

```text
entry_zone_low <= current_price <= entry_zone_high
```

命中后：

- 做多提交 `Buy` IOC，使用当前参考价 + `max_slippage_bps` 生成保护限价。
- 做空提交 `Sell` IOC，使用当前参考价 - `max_slippage_bps` 生成保护限价。
- Taker 模式不得把 Fib entry price 当作固定限价传给 executor，否则可能错过本应成交的窗口。

### Maker / 挂单优先

Maker 只在不会立即跨价的情况下提交 post-only：

- 做多：只有 `current_price > entry_price` 时，才允许挂 `Buy` ALO 到 entry price。
- 做空：只有 `current_price < entry_price` 时，才允许挂 `Sell` ALO 到 entry price。

如果价格已经越过 entry price：

- 做多：`current_price <= entry_price`，视为 maker 窗口已错过，等待价格回到 entry 上方或用户刷新参数。
- 做空：`current_price >= entry_price`，视为 maker 窗口已错过，等待价格回到 entry 下方或用户刷新参数。

## 止盈止损

TP/SL 必须基于真实成交均价，而不是预览价或 Fib 线价格。

### 做多

```text
tp(price_delta_usd)       = fill_price + value
sl(price_delta_usd)       = fill_price - value
tp(principal_percent)     = fill_price * (1 + value / 100 / leverage)
sl(principal_percent)     = fill_price * (1 - value / 100 / leverage)
entry_side                = Buy
exit_side                 = Sell reduce-only
```

### 做空

```text
tp(price_delta_usd)       = fill_price - value
sl(price_delta_usd)       = fill_price + value
tp(principal_percent)     = fill_price * (1 - value / 100 / leverage)
sl(principal_percent)     = fill_price * (1 + value / 100 / leverage)
entry_side                = Sell
exit_side                 = Buy reduce-only
```

校验：

- 所有触发价必须为正。
- 做多 TP 必须高于入场价，SL 必须低于入场价。
- 做空 TP 必须低于入场价，SL 必须高于入场价。
- perp 使用杠杆折算本金盈亏百分比；spot 固定 `leverage = 1`。

## 交易所原生保护单

做空成交后，保护单提交必须复用现有原生 TP/SL 链路，但传入正确方向：

- `entry_side = "sell"`。
- `exit_side` 由保护单构建逻辑推导为 `Buy`。
- 对 perp 使用现有 position/native TP/SL 语义。
- 不得使用 spot 的 sell-to-close 特殊逻辑来模拟 perp 平空。

保护单状态、对账、完成判定、自动循环、止损后冷却和止损后停止，沿用现有 Fib 状态机。

## 风控与状态约束

为保持基础版简单可靠，第一版多空支持采用更严格的开仓前状态：

- Fib 新开仓前，每个目标账号在该市场该交易对必须是空仓。
- 如果任一账号已有同交易对仓位，无论同向还是反向，启动/重启本轮都应拒绝或暂停，并提示用户先平仓或停止旧策略。
- 多账号策略仍按同步批次处理；任何账号缺失或提交失败，不能把整个策略标记为健康。
- 同一市场 + 同一交易对只能有一个 Fib 策略占用，不按方向放宽。
- spot 做空必须在前端和后端双重拒绝。
- 所有开仓仍受模块黑名单、单档最小名义金额、账户额度、杠杆、滑点、Vault、live gate 和 kill switch 限制。

## 数据结构变更

### Rust 策略模型

需要增加：

```rust
pub enum FibTradeDirection {
    Long,
    Short,
}
```

并加入：

- `FibBasicConfig.direction`
- `FibBasicPayload.direction`
- `FibBasicPlan.direction`
- `FibEntrySignalResponse.side`
- `FibInstanceRecord` 持久化兼容默认值
- `fib_line_version` 种子加入 direction，避免多空共用同一线版本

建议新增工具函数：

```rust
fib_entry_price(direction, swing_high, swing_low, level)
fib_entry_side(direction) -> OrderSide
fib_exit_side(direction) -> OrderSide
fib_take_profit_trigger_from_entry(direction, config, fill_price)
fib_stop_loss_trigger_from_entry(direction, config, fill_price)
leveraged_return_pct(direction, entry_price, exit_price, leverage)
```

避免继续保留只适用于做多的函数名，如 `take_profit_price_for_long`、`stop_loss_price_for_long`
直接被通用路径调用。可以保留旧函数作为测试辅助，但生产路径应走方向参数。

### API Payload

新增字段：

```json
{
  "direction": "long"
}
```

允许值：

```text
long, short
```

默认：

```text
long
```

影响接口：

- `POST /api/fib/auto-detect`
- `POST /api/fib/preview`
- `POST /api/fib/instances`
- `POST /api/fib/instances/start`
- `POST /api/fib/instances/refresh-params`
- `GET /api/fib/instances`
- `GET /api/fib/history`
- Dashboard 当前委托 / 策略挂单聚合接口

## 前端规格

### 参数区

在市场、交易对、周期附近新增方向控件：

```text
方向：做多回撤 / 做空反弹
```

交互：

- `hl_perp`、`xyz_perp`：两个按钮都可选。
- `spot`：做多可选，做空置灰不可点击。
- 切换市场到 spot 时，如果当前为做空，自动切回做多。
- 方向切换后，自动识别、计划预览、运行实例卡片和入场说明全部按方向改文案。

### 文案

做多时：

- “上涨波段”
- “回撤接针”
- “买入开多”
- “上涨止盈 / 下跌止损”

做空时：

- “下跌波段”
- “反弹做空”
- “卖出开空”
- “下跌止盈 / 上涨止损”

spot 做空按钮 tooltip：

```text
现货没有负仓位，不能开空；如需做空请选择 Default Perps 或 XYZ Perps。
```

### 运行实例和 Dashboard

运行中的策略卡片必须显示：

- 方向：做多 / 做空。
- 入场动作：买入开多 / 卖出开空。
- 平仓动作：卖出平多 / 买入平空。
- 当前等待状态：等待回撤到接针区 / 等待反弹到做空区。
- TP/SL 的方向含义：做空时 TP 在下方，SL 在上方。

当前委托 / 策略挂单中，Fib maker 单也必须显示方向：

- 做多挂单：`买入开多`
- 做空挂单：`卖出开空`

## 实现顺序

1. 文档和测试用例先行。
2. 增加 `FibTradeDirection`，并给旧记录做 serde default。
3. 把 Fib 价格、TP/SL、收益率计算改成方向参数。
4. 改自动识别：long 找上涨波段，short 找下跌波段。
5. 改入场信号：long -> `Buy`，short -> `Sell`。
6. 改成交后保护：long -> `entry_side=buy`，short -> `entry_side=sell`。
7. 加后端 spot short 拒绝。
8. 改前端方向控件和 spot disabled 状态。
9. 改运行实例、历史、Dashboard 当前委托的方向展示。
10. 跑单元测试、dry-run 集成测试，再做小额合约实盘 smoke。

## 测试计划

### 单元测试

- long Fib 价格：`high=100, low=80, 0.382 -> 92.36`。
- short Fib 价格：`high=100, low=80, 0.382 -> 87.64`。
- long TP/SL：入场 100，TP 102，SL 98。
- short TP/SL：入场 100，TP 98，SL 102。
- long principal percent 按杠杆折算。
- short principal percent 按杠杆折算。
- spot short payload 被拒绝。
- 旧 Fib JSON 无 direction 时按 long 加载。

### 后端 dry-run

- `hl_perp + ETH + long`：生成 `Buy` 入场信号。
- `hl_perp + ETH + short`：生成 `Sell` 入场信号。
- `xyz_perp + xyz:NVDA + short`：生成 `Sell` 入场信号，且 dex 为 `xyz`。
- `spot + HYPE/USDC + short`：返回用户可读拒绝。
- short 成交报告模拟后，保护单计划为 `Buy reduce-only`。

### 前端验收

- 四个市场方向控件布局一致。
- spot 下做空按钮灰色不可点，有 tooltip。
- 从合约做空切到 spot，自动回到做多。
- 做空自动识别显示“下跌波段”和“反弹做空区”。
- 做空策略卡片显示 TP 在下、SL 在上。
- 当前委托显示“卖出开空”，不显示成普通卖出。

### 实盘 smoke

只在用户明确确认后执行：

- 使用 `hl_perp` 或 `xyz_perp`，选择一个低风险交易对。
- 每账号使用满足交易所最小订单要求的最小名义金额。
- 先 dry-run 校验信号和保护计划。
- 再 live 小额开空，成交后确认交易所原生 TP/SL 为 `Buy reduce-only`。
- 立即人工或保护单平仓后，确认无残留仓位、无残留保护单、历史和 PnL 记录正确。

## 不纳入本次扩展

- spot 做空或借贷做空。
- 多空同交易对同时运行。
- 自动判断应该做多还是做空。
- AI 趋势过滤、ZigZag/Pivot 自动方向决策。
- 结构失效价、连续止损次数、最大运行时长等更高级终止条件。
