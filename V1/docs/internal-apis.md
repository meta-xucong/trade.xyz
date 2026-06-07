# 内部 API 契约

## 目标

内部 API 的目标是让模块独立演进。模块之间传递强类型事件和命令，不互相穿透实现
细节。

核心链路：

```text
MarketEvent / LeaderEvent / AccountEvent
  -> SignalCoordinator
  -> CoordinatorSignal
  -> AccountWorker
  -> TradeIntent
  -> RiskGateway
  -> ApprovedOrder / RejectedIntent
  -> Executor
  -> ExecutionReport / FillReport / PositionUpdate
```

## 通用 ID

所有核心对象都必须有稳定 ID：

- `event_id`：市场事件、leader 事件、账户事件。
- `strategy_id`：策略实例 ID，例如 `fib_xyz_tsla_1h`。
- `intent_id`：策略生成的交易意图 ID。
- `risk_decision_id`：风控裁决 ID。
- `execution_command_id`：执行命令 ID。
- `cloid`：发送给交易所的 client order id。
- `signal_id`：coordinator 广播给多个 account workers 的全局同步信号 ID。
- `worker_id`：单个本地交易地址 worker 的进程 ID。

ID 必须支持重启后去重。对于 leader fills，建议由 `leader + coin + fill_id 或 time +
side + px + sz` 派生确定性 ID。

## 事件类型

### CoordinatorSignal

用于把同一个交易信号广播给多个 account workers。

```rust
pub enum CoordinatorSignal {
    Fib(FibSignal),
    SmartMoney(SmartMoneySignal),
    Manual(ManualSignal),
    Cancel(CancelSignal),
    ConfigUpdate(ConfigUpdateSignal),
    KillSwitch(KillSwitchSignal),
}
```

最少字段：

- `signal_id`
- `source`
- `created_at`
- `dispatch_at`
- `expires_at`
- `target_accounts`
- `dedupe_key`

### WorkerReport

用于 account worker 向 coordinator 汇报结果：

```rust
pub enum WorkerReport {
    Ack(WorkerAck),
    Rejected(RejectedIntent),
    Submitted(OrderSubmitted),
    Filled(FillReport),
    Cancelled(CancelReport),
    Error(WorkerError),
    Health(WorkerHealth),
}
```

### MarketEvent

用于描述市场事实：

```rust
pub enum MarketEvent {
    Book(BookUpdate),
    Trade(PublicTrade),
    Candle(CandleUpdate),
    MarkPrice(MarkPriceUpdate),
    Funding(FundingUpdate),
    Metadata(MarketMetadataUpdate),
}
```

最少字段：

- `event_id`
- `received_at`
- `exchange_time`
- `coin`
- `source`

### LeaderEvent

用于描述目标地址行为：

```rust
pub enum LeaderEvent {
    Fill(LeaderFill),
    OrderUpdate(LeaderOrderUpdate),
    PositionSnapshot(LeaderPositionSnapshot),
}
```

最少字段：

- `event_id`
- `leader_id`
- `account`
- `coin`
- `side`
- `price`
- `size`
- `is_reduce_only`
- `exchange_time`
- `received_at`

### AccountEvent

用于描述本账户事实：

```rust
pub enum AccountEvent {
    Balance(AccountBalance),
    Position(PositionUpdate),
    OpenOrders(OpenOrdersSnapshot),
    Fill(FillReport),
    RateLimit(RateLimitState),
}
```

### ManualOpsEvent

用于描述前端人工操作请求：

```rust
pub enum ManualOpsEvent {
    PlaceOrder(ManualOrderRequest),
    CancelOrder(ManualCancelRequest),
    SetTakeProfitStopLoss(ManualTpSlRequest),
    BatchOperation(ManualBatchRequest),
    StrategyConfigChange(StrategyConfigChangeRequest),
    StrategyControl(StrategyControlRequest),
}
```

最少字段：

- `request_id`
- `operator`
- `source_module`（`manual` / `fib` / `copy`）
- `account`
- `coin`
- `requested_at`
- `dry_run_expected`
- `client_note`

`source_module` 用于把共享 API 请求路由到对应模块风控域。即使共用同一个 HTTP 端点，
也必须按模块隔离黑名单、风险检查和审计标签，不能退化为全局配置混用。

### FrontendState

前端 Dashboard、策略页面和手动交易页面从后端订阅只读状态：

```rust
pub struct FrontendState {
    pub app: AppStatus,
    pub accounts: Vec<AccountSummary>,
    pub positions: Vec<PositionSummary>,
    pub open_orders: Vec<OrderSummary>,
    pub pnl: PnlSummary,
    pub risk: RiskSummary,
    pub strategies: Vec<StrategySummary>,
    pub recent_events: Vec<AuditEventSummary>,
}
```

前端状态不得包含私钥、签名 payload 或未脱敏 secrets。

## 策略接口

策略只接收事实事件和状态快照，只输出交易意图。

```rust
pub trait Strategy {
    fn id(&self) -> StrategyId;
    fn on_event(&mut self, ctx: &StrategyContext, event: &Event) -> Vec<TradeIntent>;
    fn on_timer(&mut self, ctx: &StrategyContext, now: Timestamp) -> Vec<TradeIntent>;
}
```

策略不得：

- 直接访问交易所 `/exchange`。
- 自己读取私钥。
- 自己绕过风控下单。
- 修改其他策略状态。

## TradeIntent

`TradeIntent` 是策略、基本操作模块和风控之间的唯一交易请求格式。

```rust
pub struct TradeIntent {
    pub intent_id: IntentId,
    pub signal_id: Option<SignalId>,
    pub worker_id: WorkerId,
    pub account: AccountId,
    pub strategy_id: StrategyId,
    pub created_at: Timestamp,
    pub coin: CanonicalCoin,
    pub side: OrderSide,
    pub intent_kind: IntentKind,
    pub sizing: SizingRequest,
    pub price_policy: PricePolicy,
    pub execution_policy: ExecutionPolicy,
    pub reduce_only: bool,
    pub reason: IntentReason,
    pub source: IntentSource,
    pub source_event_id: Option<EventId>,
}
```

```rust
pub enum IntentSource {
    Strategy,
    Manual,
    System,
}
```

典型 `IntentKind`：

- `Open`
- `Increase`
- `Reduce`
- `Close`
- `StopLoss`
- `TakeProfit`
- `Cancel`

典型 `PricePolicy`：

- `MarketWithSlippageLimit`
- `Limit`
- `MakerOnly`
- `PegBestBidAsk`

典型 `ExecutionPolicy`：

- `Taker`
- `Maker`
- `Alo`
- `Ioc`
- `Gtc`

## ManualTradeIntent

`ManualTradeIntent` 是人工请求进入风控前的语义包装。进入风控网关前应转换成
`TradeIntent { source: IntentSource::Manual, ... }`。

```rust
pub struct ManualTradeIntent {
    pub request_id: ManualRequestId,
    pub operator: OperatorId,
    pub account: AccountId,
    pub coin: CanonicalCoin,
    pub side: OrderSide,
    pub sizing: SizingRequest,
    pub price_policy: PricePolicy,
    pub execution_policy: ExecutionPolicy,
    pub reduce_only: bool,
    pub client_note: Option<String>,
}
```

人工批量操作必须拆分成多个 `ManualTradeIntent`，不能作为一个聚合订单提交给
executor。

## StrategyConfigChange

策略页面提交配置变更命令，而不是直接修改策略内部状态。

```rust
pub struct StrategyConfigChange {
    pub request_id: ManualRequestId,
    pub operator: OperatorId,
    pub strategy_id: StrategyId,
    pub patch: StrategyConfigPatch,
    pub requested_at: Timestamp,
    pub requires_confirmation: bool,
}
```

配置变更必须：

- 校验字段范围。
- 写审计日志。
- 在主网 live 关键参数变更时二次确认。
- 失败时保持旧配置。

## StrategyControlCommand

策略控制命令用于启停、暂停、恢复、策略级 kill switch。

```rust
pub enum StrategyControlKind {
    Enable,
    Pause,
    Resume,
    Kill,
}

pub struct StrategyControlCommand {
    pub request_id: ManualRequestId,
    pub operator: OperatorId,
    pub strategy_id: StrategyId,
    pub kind: StrategyControlKind,
    pub reason: Option<String>,
}
```

策略控制命令不直接下单。控制命令导致的平仓行为，必须由策略或系统安全逻辑生成
`TradeIntent` 并经过风控网关。

## 风控接口

```rust
pub trait RiskCheck {
    fn name(&self) -> &'static str;
    fn check(&self, ctx: &RiskContext, intent: &TradeIntent) -> RiskCheckResult;
}
```

风控网关编排：

```rust
pub trait RiskGateway {
    fn evaluate(&self, ctx: &RiskContext, intent: TradeIntent) -> RiskDecision;
}
```

输出：

```rust
pub enum RiskDecision {
    Approved(ApprovedOrder),
    Rejected(RejectedIntent),
}
```

## ApprovedOrder

`ApprovedOrder` 是执行模块唯一可接受的新订单输入。

```rust
pub struct ApprovedOrder {
    pub risk_decision_id: RiskDecisionId,
    pub intent_id: IntentId,
    pub signal_id: Option<SignalId>,
    pub worker_id: WorkerId,
    pub account: AccountId,
    pub strategy_id: StrategyId,
    pub coin: CanonicalCoin,
    pub side: OrderSide,
    pub size: Decimal,
    pub price: Option<Decimal>,
    pub order_type: OrderType,
    pub reduce_only: bool,
    pub cloid: ClientOrderId,
    pub expires_at: Option<Timestamp>,
}
```

## 执行接口

```rust
pub trait OrderExecutor {
    async fn submit(&self, order: ApprovedOrder) -> ExecutionReport;
    async fn cancel(&self, command: CancelCommand) -> ExecutionReport;
}
```

执行模块必须：

- 只执行本 worker 对应本地交易地址的订单。
- 对 signed action 使用单一 nonce manager。
- 记录每次提交、响应、失败。
- 把 HTTP 200 但 action-level error 视为失败。
- 支持重启后通过 `cloid` 和 open orders 对账。

## 存储接口

```rust
pub trait EventStore {
    async fn append(&self, event: StoredEvent) -> StoreResult<()>;
    async fn load_since(&self, cursor: EventCursor) -> StoreResult<Vec<StoredEvent>>;
}
```

所有模块都可以读取状态快照，但写入审计事件应通过统一 writer，保证顺序和可恢复性。

## 错误分类

错误至少分为：

- `Recoverable`：可重试，例如临时网络错误。
- `Rejected`：风控或验证拒绝。
- `Desync`：本地状态和交易所状态不一致。
- `Fatal`：不能继续实盘，例如 nonce 状态损坏、存储不可写。

`Fatal` 必须触发 fail-closed。
