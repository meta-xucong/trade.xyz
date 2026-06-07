# 多进程与多地址执行模型

## 核心要求

系统必须采用“每个本地交易地址一个独立 worker 进程”的执行架构。

这里的“地址”指本系统控制的本地交易地址、子账户或 API wallet 对应的执行账户。目标
聪明钱 leader 地址可以由信号协调层集中监听，也可以后续按 leader 分片监听；但本地
跟单执行必须按地址拆成独立进程。

## 为什么每地址一进程

- 每个地址拥有独立 API wallet、nonce manager 和签名状态，避免 nonce 冲突。
- 每个地址独立下单，跟单信号到来时可以并行提交订单。
- 某个地址异常不会拖慢其他地址。
- 风控、仓位、PnL、open orders 可以按地址隔离。
- 手动多账号操作也能并发执行，而不是在一个进程里串行循环。

## 进程拓扑

```text
                           +-------------------+
                           | Frontend Console  |
                           +---------+---------+
                                     |
                                     v
+-------------------+      +---------+---------+
| Market Data Feed  |----->| Signal Coordinator|
+-------------------+      +---------+---------+
+-------------------+                |
| Leader Watcher    |----------------+
+-------------------+                |
                                     v
                       signal broadcast / IPC
          +--------------------------+--------------------------+
          |                          |                          |
          v                          v                          v
+---------+---------+      +---------+---------+      +---------+---------+
| Account Worker A  |      | Account Worker B  |      | Account Worker C  |
| address/API key A |      | address/API key B |      | address/API key C |
+---------+---------+      +---------+---------+      +---------+---------+
          |                          |                          |
          v                          v                          v
   Hyperliquid /exchange      Hyperliquid /exchange      Hyperliquid /exchange
```

## 进程职责

### Signal Coordinator

负责只读信号和编排：

- 市场行情归一化。
- leader 地址行为监听。
- 斐波那契策略信号生成，或向 worker 广播必要行情事件。
- 聪明钱行为标准化：open / increase / reduce / close / flip。
- 手动前端请求拆分。
- 生成全局 `signal_id`。
- 把同一个信号广播给所有相关 account workers。
- 收集 worker ack、风控拒绝、订单回报。

Signal Coordinator 不持有私钥，不直接调用 `/exchange` 下单。

### Account Worker

每个本地交易地址启动一个 worker 进程。

负责：

- 读取该地址的 API wallet secret。
- 维护该地址独立 nonce manager。
- 维护该地址账户状态、仓位、open orders、PnL。
- 执行该地址的 `StrategyRisk`、`ManualOpsRisk`、`PortfolioRisk`、`ExecutionRisk`。
- 把批准的订单提交到 Hyperliquid。
- 上报订单、成交、拒绝、错误和健康状态。
- 持久化该地址的本地状态或写入按地址分区的存储。

Account Worker 不监听其他本地地址私钥，不替其他地址下单。

## 同步操作语义

“同步操作”定义为：

- 同一个 leader event、fib signal 或 manual batch 会产生一个全局 `signal_id`。
- Signal Coordinator 给目标 account workers 广播同一个 `signal_id` 和同一个
  `dispatch_at`。
- 每个 worker 收到后立即按本地址配置独立风控、独立计算 sizing、独立下单。
- worker 之间不互相等待，避免慢地址拖累快地址。
- 前端和审计日志按 `signal_id` 聚合展示所有地址的执行结果。

如果某个 worker 未就绪：

- 不阻塞其他 worker。
- 该 worker 对该 signal 记录 `WORKER_NOT_READY` 或 `SIGNAL_EXPIRED`。
- 其他健康 worker 正常执行。

## IPC 要求

第一版可以使用本机 loopback TCP 或 Windows named pipe。要求：

- 长连接。
- 低延迟。
- 有心跳。
- 有 worker 注册和健康检查。
- 有 backpressure。
- 有 signal ack。
- 消息有 `signal_id`、`account_id`、`created_at`、`dispatch_at`、`expires_at`。

后续如果需要进一步降延迟，可把消息编码从 JSON 切到 MessagePack、bincode 或
FlatBuffers。

## 信号类型

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

每个 signal 必须包含：

- `signal_id`
- `source`
- `created_at`
- `dispatch_at`
- `expires_at`
- `target_accounts`
- `dedupe_key`

## worker 输出

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

## 配置原则

- 每个 account worker 有独立 `account_id`、地址、API wallet env var、风险限制。
- 每个 account worker 可配置独立 copy ratio 和 symbol limit。
- 一个 worker 只能服务一个执行地址。
- 多地址批量操作由 coordinator 广播，不允许单 worker 串行替多个地址下单。

## 故障隔离

- 单个 worker nonce 异常，只 kill 该 worker 的 signed actions。
- 单个 worker 账户状态过期，只暂停该 worker。
- Signal Coordinator 异常，所有 worker 停止接收新信号，但可继续安全撤单或
  reduce-only，取决于配置。
- IPC 断开后 worker 进入 fail-closed，不接受新开仓。

## 前端展示

Dashboard 必须按地址展示：

- worker 健康状态。
- 账户资金和 PnL。
- 当前仓位。
- open orders。
- 最近 signal 执行结果。
- 最近拒绝原因。

聪明钱页面必须按 `signal_id` 聚合展示各地址执行结果：

```text
leader event -> signal_id -> worker A/B/C reports
```

## 验收标准

- 启动 N 个 account worker，每个绑定不同本地交易地址。
- 同一个 dry-run signal 能被广播给所有目标 worker。
- 每个 worker 独立生成风控裁决。
- 每个 worker 独立生成 cloid。
- 每个 worker 独立维护 nonce。
- 某个 worker 故障不影响其他 worker 接收和执行信号。
