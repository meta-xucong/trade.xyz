# 系统架构

## 总体定位

本项目是基于 Rust 的 trade[XYZ] 自动交易系统。trade[XYZ] 是 Hyperliquid 上
名为 `xyz` 的 HIP-3 builder-deployed perp DEX；程序化交易通过 Hyperliquid
REST、WebSocket 和签名交易接口完成。

系统的核心不是写死某个策略，而是建立一个可扩展交易内核：

- 模块 1：基础信息获取模块
- 模块 2：基础交易模块
- 模块 3：斐波那契回撤策略模块
- 模块 4：聪明钱跟单策略模块
- 模块 5：前端控制台与基本操作模块
- 横切模块：事件总线、状态存储、统一风控网关、配置、运行监控

实盘和低延迟 dry-run 必须采用多进程执行模型：一个 coordinator 进程负责信号生成、
前端和 worker 编排；每个本地交易地址启动一个独立 account worker 进程负责风控和
下单。详见 [多进程与多地址执行模型](process-model.md)。

## 关键架构原则

- 数据模块只生产事实。
- 策略模块只生产交易意图。
- 前端控制台只生产人工交易意图、撤单命令、策略配置变更命令或策略控制命令。
- coordinator 只广播信号和聚合结果，不替多个本地地址下单。
- account worker 一个进程只服务一个本地交易地址。
- 风控模块只做裁决和转换。
- 交易模块只执行已批准命令。
- 存储模块记录事实、意图、裁决、订单、成交、错误和恢复点。

## 数据流

```text
                 +----------------+
                 |  Config Loader |
                 +--------+-------+
                          |
                          v
+----------------+  +-----+------+  +-----------------+  +-------------+
| Market Streams |  | State Store|  | Leader Streams  |  | Frontend    |
+-------+--------+  +-----+------+  +--------+--------+  +------+------+
        |                 ^                  |                  |
        v                 |                  v                  v
 +------+-----------------+------------------+------------------+------+
 |                     Signal Coordinator                         |
 |      Fib / Copy / Manual signals, config commands, fan-out       |
 +------+-----------------+------------------+------------------+------+
        |                 |                  |
        v                 v                  v
+-------+-------+ +-------+-------+ +--------+------+
| Worker Addr A | | Worker Addr B | | Worker Addr C |
+-------+-------+ +-------+-------+ +--------+------+
        |                 |                  |
        v                 v                  v
  Risk + Executor   Risk + Executor    Risk + Executor
        |                 |                  |
        v                 v                  v
             Hyperliquid /exchange
```

## 推荐代码结构

```text
src/
  main.rs
  app.rs
  config/
    mod.rs
    model.rs
    loader.rs
  domain/
    mod.rs
    account.rs
    event.rs
    market.rs
    order.rs
    risk.rs
    strategy.rs
    process.rs
  bus/
    mod.rs
  infra/
    hyperliquid/
      mod.rs
      rest.rs
      websocket.rs
      signing.rs
      types.rs
    clock.rs
  information/
    mod.rs
    market_data.rs
    leader_watcher.rs
    account_watcher.rs
    reconciler.rs
  coordinator/
    mod.rs
    signal.rs
    fanout.rs
    supervisor.rs
    ipc.rs
  account_worker/
    mod.rs
    runtime.rs
    account_state.rs
    ipc.rs
  trading/
    mod.rs
    executor.rs
    order_manager.rs
    nonce.rs
    cloid.rs
  risk/
    mod.rs
    gateway.rs
    strategy.rs
    portfolio.rs
    execution.rs
    kill_switch.rs
  strategies/
    mod.rs
    fib_retracement/
      mod.rs
      config.rs
      engine.rs
      risk.rs
      state.rs
    smart_money_copy/
      mod.rs
      config.rs
      engine.rs
      risk.rs
      state.rs
  frontend/
    mod.rs
    dashboard.rs
    fib_page.rs
    copy_page.rs
    state_stream.rs
  manual_ops/
    mod.rs
    api.rs
    command.rs
    risk.rs
    state.rs
  storage/
    mod.rs
    event_log.rs
    sqlite.rs
    snapshot.rs
  telemetry/
    mod.rs
```

## 模块职责

### 基础信息获取模块

负责从 Hyperliquid 和本地状态中获取事实：

- 实时行情：book、trades、candles、mark、mid、oracle、funding。
- 市场元数据：`dex: "xyz"`、symbol、`szDecimals`、杠杆、margin mode。
- 账户状态：余额、仓位、open orders、fills、rate limit。
- 目标账户行为：目标地址成交、订单变化、仓位变化。
- 对账：WebSocket 断线后通过 REST 补齐状态。

运行期读模型必须以 WebSocket 为主。Dashboard、策略监听、斐波那契成交检测、当前委托、
账户资金卡片和行情卡片都应优先读取本地 realtime cache；REST 只用于冷启动/重连补快照、
缓存失效兜底、显式对账、签名前预检查、提交后确认、元数据和 candle snapshot。

该模块只发布 `MarketEvent`、`LeaderEvent`、`AccountEvent`，不做策略判断。

### 基础交易模块

负责在 account worker 进程内执行该地址已批准的交易命令：

- 下买单、下卖单、撤单、改单。
- maker / taker / IOC / GTC / ALO / reduce-only。
- nonce 管理、签名、重试、幂等、`cloid`。
- 订单状态同步、成交回报、失败回报。

该模块只接受该 worker 本地风控批准后的 `ApprovedOrder` 或 `ExecutionCommand`，
不接受策略、前端或 coordinator 直接调用。一个 worker 不得替多个本地地址串行下单。

### 斐波那契回撤策略模块

负责根据不同时间维度计算回撤点并发出交易意图：

- 支持多个时间周期，例如 1m、5m、15m、1h、4h、1d。
- 默认支持 0.382、0.618 两档回撤，并允许扩展到 0.236、0.5、0.786。
- 价格接近回撤点时产生接针买入意图。
- 基于实际成交价计算止盈 N 美元和止损 X%。
- 同一档位重复买入、冷却时间、最大接针次数由策略内风控处理。

### 聪明钱跟单策略模块

负责识别目标地址行为并发出跟单意图：

- 目标地址配置、启用状态、权重、跟单比例。
- 行为识别：开仓、加仓、减仓、平仓、反向、撤单。
- 多账号去重、leader 事件去重、交易对限额。
- 跟单比例、最大跟单数量、延迟阈值、滑点阈值。
- 买入和卖出均以目标地址行为为输入，但必须通过风控网关。

### 前端控制台与基本操作模块

负责提供本地前端控制台和人工操作 API：

- Dashboard 展示环境、dry-run/live 状态、多账号、资金、权益、保证金、仓位、
  PnL、open orders、系统健康状态。
- Manual Trading 页面支持点击买入、卖出、撤单、批量多账号操作。
- Fib Retracement 页面支持斐波那契策略配置、启停、监控、策略级 kill switch。
- Smart Money Copy 页面支持 leader 配置、跟单比例、symbol limit、启停、监控。
- 展示已启用 symbol 的行情、价差、mark、mid、oracle。
- 支持为已有仓位设置人工止盈止损。
- 支持 dry-run 和 testnet 基础交易测试。

该模块不得直接下单。人工点击买卖会生成 `ManualTradeIntent`，撤单会生成
`CancelCommand`，然后由 coordinator 广播到目标 account workers。多账号批量操作
必须拆成每账号独立信号，逐个 worker 过风控、逐个审计。策略页面提交
`StrategyConfigChange` 或
`StrategyControlCommand`，不得直接生成交易所订单。

### Signal Coordinator

负责：

- 生成或接收 `FibSignal`、`SmartMoneySignal`、`ManualSignal`。
- 给同一个事件分配全局 `signal_id`。
- 将信号广播给目标 account workers。
- 聚合 worker ack、拒绝、提交、成交和错误。
- 监督 worker 健康状态。

Signal Coordinator 不持有私钥，不直接下单。

### Account Worker

每个本地交易地址一个独立进程，负责：

- 该地址 API wallet secret。
- 该地址 nonce manager。
- 该地址账户状态、仓位、PnL、open orders。
- 该地址风控。
- 该地址订单执行。
- 该地址状态存储分区和审计日志。

## 风控网关定位

风控网关不是“一套写死的统一风控逻辑”，而是统一入口和编排器：

```text
TradeIntent
  -> StrategyRisk
  -> PortfolioRisk
  -> ExecutionRisk
  -> ApprovedOrder / RejectedIntent
```

不同策略有不同 `StrategyRisk`，基本操作模块有 `ManualOpsRisk`。所有交易意图共享
`PortfolioRisk` 和 `ExecutionRisk`。

在多进程模式下，`StrategyRisk` 可以在 coordinator 生成信号前做一次策略语义校验；
最终是否下单仍由每个 account worker 在本地执行账户级 `PortfolioRisk` 和
`ExecutionRisk` 后决定。

## 状态存储定位

状态存储必须支持：

- 事件日志：原始事件、标准化事件、策略信号、风控裁决。
- 订单日志：意图、批准、提交、成交、取消、失败。
- 去重记录：leader event id、strategy signal id、cloid。
- 快照：账户、仓位、订单、策略状态。
- 重启恢复：从最近快照和后续事件恢复系统状态。

## 故障处理原则

- 行情流丢失：暂停策略信号，触发 REST 对账。
- 账户状态丢失：禁止新开仓，只允许安全平仓逻辑。
- nonce 状态异常：停止所有 signed action，等待人工或自动恢复。
- 风控状态落后：拒绝所有新意图。
- 存储不可写：进入 fail-closed，不允许继续实盘下单。
