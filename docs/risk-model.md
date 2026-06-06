# 风控模型

## 总体原则

风控采用“统一风控网关 + 分层风控插件”。

统一的是入口，不是把所有规则写成一坨。每个策略可以有自己的专属风控，基本操作
模块也可以有人工操作专属风控，但所有 `TradeIntent` 都必须经过同一个
`RiskGateway`。

```text
TradeIntent
  -> StrategyRisk
  -> PortfolioRisk
  -> ExecutionRisk
  -> ApprovedOrder / RejectedIntent
```

## 分层职责

### StrategyRisk

策略专属风控。它理解策略语义。

斐波那契策略关心：

- 当前价格是否真的接近配置档位。
- 同一 fib level 是否已经买过。
- 是否在冷却时间内。
- 当前策略仓位是否超过上限。
- 是否已经有未完成的止盈止损订单。

聪明钱跟单策略关心：

- leader 是否启用。
- leader event 是否延迟过高。
- 是否重复跟单。
- 跟单比例是否超过上限。
- 单 leader / 单 symbol / 单账户跟单数量是否超过限制。
- 平仓是否能映射到本地跟单仓位。

### ManualOpsRisk

人工操作专属风控。它理解“前端点击”和“多账号批量操作”的语义。

人工操作关心：

- 当前是否允许人工操作。
- 当前是否允许 mainnet live 人工操作。
- 请求账号是否在允许列表。
- 请求 symbol 是否在允许列表。
- 批量账号数量是否超过上限。
- 超过阈值的订单是否完成二次确认。
- reduce-only 是否会扩大仓位。
- 手动止盈止损是否绑定已有仓位。

### PortfolioRisk

账户和组合级风控。它不关心策略细节，只关心全局风险。

必须包含：

- 总仓位 notional 上限。
- 单 symbol 总仓位上限。
- 单策略总风险暴露上限。
- 单日最大亏损。
- 最大杠杆。
- 保证金健康度。
- 策略之间方向冲突检测。
- 全局 kill switch。

### ExecutionRisk

执行前最后检查。它关心能不能安全发订单。

必须包含：

- symbol 存在于 live metadata。
- size 和 price 精度正确。
- 下单 notional 满足交易所要求。
- 开仓 notional 不低于 Hyperliquid 当前最低订单价值。2026-05-31 主网实测返回过
  `Order must have minimum value of $10`，所以预检和前端 readiness 必须提前拦截低于
  10 USD 的开仓；Perp `reduce-only` 平仓可放行以避免小残仓无法退出。Spot 卖出平仓
  走 inventory close 路径，同样不按开仓最低值拦截，但不能把 `reduce_only` 透传给交易所。
- maker/taker 语义正确。
- reduce-only 和方向一致。
- 滑点保护有效。
- 盘口数据没有过期。
- nonce manager 健康。
- rate limit 健康。

对人工操作：

```text
ManualTradeIntent
  -> ManualOpsRisk
  -> PortfolioRisk
  -> ExecutionRisk
  -> ApprovedOrder / RejectedIntent
```

## 风控裁决

```rust
pub enum RiskDecision {
    Approved(ApprovedOrder),
    Rejected(RejectedIntent),
}
```

拒绝时必须记录：

- `intent_id`
- `strategy_id`
- `risk_layer`
- `risk_check_name`
- `reason_code`
- `human_message`
- 当前关键状态快照

## Reason code

建议用稳定 reason code：

```text
STRATEGY_DISABLED
SYMBOL_DISABLED
DUPLICATE_SIGNAL
SIGNAL_TOO_OLD
POSITION_LIMIT_EXCEEDED
DAILY_NOTIONAL_LIMIT_EXCEEDED
LOSS_LIMIT_EXCEEDED
MARGIN_HEALTH_TOO_LOW
PRICE_STALE
SPREAD_TOO_WIDE
SLIPPAGE_LIMIT_EXCEEDED
INVALID_PRECISION
RATE_LIMIT_UNHEALTHY
NONCE_UNHEALTHY
KILL_SWITCH_ACTIVE
STORAGE_UNHEALTHY
MANUAL_TRADING_DISABLED
MANUAL_LIVE_DISABLED
MANUAL_CONFIRMATION_REQUIRED
BATCH_ACCOUNT_LIMIT_EXCEEDED
```

## Kill switch

Kill switch 分三层：

- 全局：禁止所有新开仓。
- 策略级：禁用某个策略，只允许处理已有仓位退出。
- symbol 级：禁用某个交易对。

kill switch 激活后：

- 新开仓 intent 一律拒绝。
- reduce-only 平仓 intent 可按配置允许。
- 撤单命令可允许。
- 所有拒绝写入审计日志。

## 止盈止损

止盈止损属于策略语义，但必须经过风控网关。

斐波那契策略：

- 成交后以实际成交价计算止盈和止损。
- 止盈 intent 应设置 reduce-only。
- 止损 intent 应设置 reduce-only。
- 止损触发时可以优先使用 taker，具体由配置决定。

聪明钱跟单策略：

- leader 减仓和平仓映射为本账户 reduce-only。
- 不允许为了跟随 leader 平仓而扩大本账户仓位。
- 本地跟单仓位和 leader 仓位映射不清晰时，优先保守减仓或拒绝。

## 风控状态一致性

风控依赖状态：

- 本账户权益和保证金。
- 本账户当前仓位。
- open orders。
- 策略持仓映射。
- 今日成交 notional。
- PnL。
- rate limit。

任一关键状态过期时：

- 禁止新开仓。
- 禁止加仓。
- 可允许 reduce-only。
- 触发 REST reconciliation。

## 配置优先级

风控配置优先级：

```text
global limit
  > account limit
  > symbol limit
  > strategy limit
  > leader limit
  > signal-level request
```

更严格的配置优先生效。

## 测试要求

风控必须有表驱动测试：

- 每个拒绝 reason code 至少一个测试。
- 每个策略专属风控至少覆盖正常批准、重复信号、超过上限。
- 精度和滑点必须覆盖边界值。
- kill switch 必须覆盖全局、策略、symbol 三种粒度。
