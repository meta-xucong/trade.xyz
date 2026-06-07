# 策略开发指南

## 策略开发原则

策略模块只负责“根据事实生成意图”。它不负责签名、下单、撤单、全局仓位管理或最终
资金安全。

每个策略必须包含：

- 策略配置
- 策略状态
- 事件处理逻辑
- 定时处理逻辑
- 策略专属风控
- 单元测试和回放测试

## 策略生命周期

```text
load config
  -> initialize state
  -> subscribe events
  -> process events and timers
  -> emit TradeIntent
  -> receive execution reports
  -> update strategy state
```

## Strategy trait

建议所有策略实现统一接口：

```rust
pub trait Strategy {
    fn id(&self) -> StrategyId;
    fn subscriptions(&self) -> StrategySubscriptions;
    fn on_event(&mut self, ctx: &StrategyContext, event: &Event) -> Vec<TradeIntent>;
    fn on_timer(&mut self, ctx: &StrategyContext, now: Timestamp) -> Vec<TradeIntent>;
    fn on_execution_report(&mut self, report: &ExecutionReport);
}
```

## 斐波那契回撤策略

详细产品和开发规格见 [斐波那契回撤策略开发文档](fibonacci-retracement-development.md)。
本节只保留核心策略约束。

### 目标

在指定时间维度内识别价格从上涨波段高点回撤到 0.382 或 0.618 附近的接针机会，成交后基于
实际成交价自动设置止盈和止损。

### 输入

- K 线或聚合行情
- 当前 mark/mid/oracle
- 当前账户仓位
- 当前 open orders
- 策略状态

### 配置字段

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

### 回撤计算

基础版对给定时间窗口必须先找到有效上涨波段，并校验 `swing_low` 在时间上早于
`swing_high`。不得直接把无序最高点和最低点拼成回撤区间。

```text
swing_high = effective impulse high
swing_low = effective impulse low
retracement_price(level) = swing_high - (swing_high - swing_low) * level
```

Fib 基础版支持做空时，必须对下跌后反弹回撤单独定义方向；完整规格见
[斐波那契基础版多空方向扩展开发文档](fibonacci-long-short-extension.md)。

### 入场条件

最少条件：

- 策略启用。
- symbol 存在于 live metadata。
- 当前价格接近 0.382 或 0.618 回撤点。
- 当前时间段允许交易。
- 当前档位没有超过最大接针次数。
- 当前策略和全局风险允许新开仓。
- 盘口价格仍满足滑点保护。

策略只发出 `TradeIntent`：

```text
Open long xyz:TSLA at fib level 0.618
```

### 止盈止损

止盈和止损必须基于实际成交价，而不是信号触发价：

```text
take_profit_price = fill_price + take_profit_usd
stop_loss_price = fill_price * (1 - stop_loss_pct)
```

成交后策略状态记录：

- `entry_fill_id`
- `entry_price`
- `entry_size`
- `fib_level`
- `take_profit_price`
- `stop_loss_price`
- `remaining_size`

止盈和止损优先使用交易所原生 TP/SL 或 trigger order 能力；策略负责计算保护价和生成
保护意图，执行层负责提交、撤换和对账。不得把前端预览价当作成交价。

### 斐波那契策略专属风控

- 单 symbol 最大同时接针仓位。
- 单 fib level 最大入场次数。
- 接针冷却时间。
- 不在流动性过差或价差过大时入场。
- 禁止在行情状态不完整时入场。

## 聪明钱跟单策略

### 目标

监听一批目标地址的交易行为，并按配置比例和风控限制复制买入、卖出、减仓、平仓
动作。

跟单信号必须由 coordinator 标准化并生成全局 `signal_id`，再广播给所有目标
account workers。每个 worker 按本地址配置独立计算 sizing、独立风控、独立下单。
worker 之间不得互相等待。

### 输入

- 目标地址成交事件
- 目标地址订单或仓位变化
- 本账户仓位和订单
- 市场行情和盘口
- 去重状态

### 配置字段

```toml
[strategies.smart_money_copy.main]
enabled = true
max_signal_delay_ms = 1500
default_copy_ratio = 0.10
dedupe_window_secs = 600
execution_mode = "taker"

[[strategies.smart_money_copy.leaders]]
leader_id = "leader_alpha"
account = "0x0000000000000000000000000000000000000000"
enabled = true
copy_ratio = 0.08
max_notional_usd_per_trade = 300.0

[[strategies.smart_money_copy.symbol_limits]]
coin = "xyz:TSLA"
enabled = true
max_position_notional_usd = 1000.0
max_daily_copy_notional_usd = 2000.0
```

### 行为识别

目标地址事件应标准化为：

- `LeaderOpen`
- `LeaderIncrease`
- `LeaderReduce`
- `LeaderClose`
- `LeaderFlip`
- `LeaderCancel`

跟单策略根据标准化行为生成本账户意图：

- leader 开多 -> 本账户按比例开多。
- leader 加多 -> 本账户按比例加多，但受 symbol limit 限制。
- leader 减仓 -> 本账户按映射比例 reduce-only 减仓。
- leader 平仓 -> 本账户 reduce-only 平仓对应跟单仓位。
- leader 反手 -> 第一版建议拆成 close + open，并分别过风控。

### 多账号跟单去重

去重至少覆盖：

- 同一 leader fill 被 WebSocket 重放。
- 同一 leader 事件被 REST 对账再次发现。
- 多个 leader 地址属于同一实体或同一策略。
- 多个本地账户跟同一事件时，按账户维度生成不同 intent，但共享 source event。
- 同一个 `signal_id` 在同一个 account worker 内只能执行一次。

建议 key：

```text
dedupe_key = leader_group + leader_event_id + local_account + worker_id + coin + action
```

### 跟单比例

第一版支持：

- 固定比例：`our_size = leader_size * copy_ratio`
- 固定 notional 上限：超过上限截断
- symbol 最大仓位上限

后续可扩展：

- 按 leader 历史胜率动态权重
- 按波动率缩放
- 按账户权益比例缩放

### 跟单策略专属风控

- leader 白名单和启用状态。
- leader event 延迟阈值。
- 单 leader、单 symbol、单日、单账户限额。
- 最大复制比例。
- 多账号去重。
- 每地址 worker 独立风控和独立 nonce。
- 禁止复制低流动性或高价差事件。
- leader 平仓事件必须优先处理，但仍需 reduce-only。

## 新策略接入流程

1. 在 `strategies/<name>/` 下创建模块。
2. 定义 `<Name>Config`。
3. 定义 `<Name>State`。
4. 实现 `Strategy` trait。
5. 实现 `<Name>RiskCheck`。
6. 在 config loader 注册。
7. 在 app bootstrap 注册策略实例和订阅。
8. 增加单元测试、回放测试、dry-run 集成测试。

## 策略输出要求

每个 `TradeIntent` 必须包含：

- 策略 ID
- symbol
- side
- sizing request
- price policy
- execution policy
- reduce-only 标记
- source event
- reason

没有完整上下文的意图必须被策略自己丢弃，或被策略专属风控拒绝。
