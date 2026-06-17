# 测试策略

## 测试分层

```text
unit tests
  -> module tests
  -> replay tests
  -> integration tests
  -> testnet smoke tests
  -> mainnet dry-run shadow tests
```

## 单元测试

必须覆盖：

- symbol normalization。
- price / size precision。
- fib retracement 计算。
- fib 入场、止盈、止损信号。
- smart-money 事件标准化。
- smart-money 去重。
- smart-money fill + 仓位 delta 行为分类：open / increase / reduce / close / flip。
- smart-money 反向冲突裁决：跳过、权重胜出、close 优先。
- 跟单比例和 notional 截断。
- copy ledger 仓位映射、pending exposure、post-close reentry guard。
- manual order request 到 `ManualTradeIntent` 的转换。
- manual batch request 的逐账号拆分。
- signed preflight / frontend readiness 对交易所最低开仓 notional 的拦截，且不阻断
  `reduce-only` 平仓。
- strategy config change 校验。
- dashboard account / position / PnL summary 聚合。
- 风控 reason code。
- cloid 生成。
- nonce manager 单调性。
- coordinator signal fan-out。
- account worker 只处理自己的 account id。

## 回放测试

回放测试从存储的事件日志读取：

```text
MarketEvent + LeaderEvent + AccountEvent
  -> Strategy / ManualOps
  -> TradeIntent
  -> RiskGateway
```

目标：

- 同一份事件输入，多次运行结果一致。
- 重启恢复后不会重复跟单。
- WebSocket 重放事件不会重复下单。
- 策略状态能从快照和事件日志恢复。
- leader reconnect + REST backfill 不会重复跟单。
- 任一 leader close 信号会生成 mapped exposure 的 reduce-only close。
- leader flip 拆成 close + 可选 open，且两条意图分别过风控。

## 集成测试

在不接真实私钥的情况下：

- mock Hyperliquid `/info`。
- mock WebSocket stream。
- mock executor。
- mock manual ops HTTP API。
- 验证模块间 API。
- 验证 fail-closed 行为。
- 启动多个 mock account workers，验证同一个 signal 并行广播。
- 验证某个 worker 失败不阻塞其他 worker。

## 基本操作模块测试

必须覆盖：

- 页面能显示环境和 dry-run/live 状态。
- dry-run 点击买入只产生人工意图，不提交真实订单。
- mainnet live 未显式开启时，人工下单被拒绝。
- 超过确认阈值时必须二次确认。
- 多账号批量操作拆成独立 intent。
- 多账号批量操作广播给多个 account workers，并按地址独立返回结果。
- reduce-only 卖出不会扩大仓位。
- 撤单请求会校验账号和订单归属。

## 前端控制台测试

必须覆盖：

- Dashboard 展示资金、仓位、PnL、open orders。
- Dashboard 按地址展示 worker 健康状态和最近 signal 执行结果。
- Dashboard 不包含 secrets。
- Fib 页面提交配置变更后写审计日志。
- Fib 页面启停策略不直接下单。
- Fib 基础版启动、刷新参数必须走 `/api/fib/*` 专用接口；不得从 Fib 前端直接调用手动下单或 runbook 接口。
- Smart Money 页面新增 leader 后写审计日志。
- Smart Money 页面修改 copy ratio 会触发配置校验。
- 前端状态流断开后可重连。

## Testnet smoke test

有 testnet API wallet 后执行：

- 拉取 metadata。
- 拉取账户状态。
- 构造 dry-run order。
- 提交极小限价订单。
- 运行 `signed-preflight`，确认所有 blocker 清除且 `ready_for_testnet_submit=true`。
- `signed-preflight` / 前端 `Preflight` 必须包含 `clearinghouseState` 摘要：新开仓账号
  要有正的 account value 或 withdrawable；reduce-only 平仓方向必须和当前仓位方向相反。
- 运行只读 `signed-runbook`，确认 JSON 中包含 preflight、pre-submit reconciliation、
  acceptance plan，并且不读取私钥、不提交订单。
- 真实提交优先运行 `signed-runbook --submit true`，确认它只在 preflight ready 后加载
  Vault 密码并提交；有 blocker 时必须输出 JSON 证据并非 0 退出。
- CLI 必须覆盖 `signed-live-window`：主网 order smoke 前默认只输出 `.codex-longrun/`
  临时配置预览和逐账号 `signed-runbook`/reduce-only close/reconcile 命令；`--write true`
  只能写 `.codex-longrun/` 下的显式 output config，不得修改 `config/local.toml`，不得读取
  Vault、签名或提交。
- CLI 必须覆盖 `mainnet-smoke-plan`：当前主网 dry-run 状态下，它要返回三层资金诊断、
  USDC 转入 batch preflight、USDC transfer live-window 预览和 signed order live-window 预览；
  `stop_reasons` 必须阻止未转入 `xyz` 保证金前的 order submit，且命令不得写配置、读 Vault、
  签名或提交。
- 前端 Manual 页必须覆盖 `Smoke Plan`：调用 `/api/mainnet-smoke-plan` 后返回与 CLI
  `mainnet-smoke-plan` 等价的只读总控证据；当前主网 dry-run 状态下必须指出资金仍在
  default perps、`xyz` 保证金为 0、live/Vault/确认 gate 未满足，且不得写配置、签名或提交。
- 前端 Manual 页必须覆盖 `Runbook Plan` / `Runbook Submit`：只读 plan 能返回 runbook
  JSON；Vault 已解锁时只读 plan 能把 secret availability 纳入 preflight；submit 在
  blocker 未清除时只展示 readiness 结果，不弹确认、不提交订单。
- 前端 Manual 页必须覆盖 `Preflight Selected`：多账号选择时返回 batch readiness JSON，
  包含 `ready_account_ids`、`blocked_account_ids` 和每个账号的 readiness 结果；该路径
  不得读取私钥、签名或提交。
- 前端 Manual 页必须覆盖 `Funding Check`：只读返回 default perps、XYZ perps 和 spot
  三层资金状态；单账号和多选账号批量查询都必须经由后端 batch API 返回明确资金层诊断。当账号
  无 XYZ 可用保证金时，响应必须给出明确 `funding_summary` 和 `next_actions`，且该路径不得读取
  私钥、签名或提交。
- CLI 必须覆盖 `account-funding`：对单账号或多账号返回 default perps、当前 `dex` perps 和
  spot 的只读资金层 JSON；该命令不读取 Vault、不签名、不提交。资金转入后应使用该命令验证
  `ready_account_ids`，再进入已批准的 order/cancel smoke。
- CLI 必须覆盖 `usdc-dex-transfer` 只读计划：当 default perps 有 USDC、XYZ perps 为 0
  时，计划应显示 source/destination、USDC token、amount 和划转前余额。主网真实提交必须
  因缺少 `app.dry_run=false`、`manual_ops.manual_live_enabled`、
  `manual_ops.mainnet_live_enabled` 或 `--confirm-mainnet-live` 被拒绝；单次超过 10 USDC
  也必须被拒绝。前端 `Plan Transfer` 必须能通过后端 batch API 对单账号和多选账号返回只读计划；
  batch API 的 `submit=true` 必须被后端拒绝。`Transfer USDC` 在 dry-run 状态必须本地阻断，且
  真实提交应支持多账号 fan-out（逐账号 runbook）。
- CLI 必须覆盖 `usdc-dex-transfer-preflight`：当前主网 dry-run 状态下，它要返回可用的只读
  2 USDC transfer plan、确认短语、rate limit、`readiness_summary`、`failed_blockers` 和
  `next_actions`；没有 `TRADE_XYZ_VAULT_PASSWORD` 时必须只报告 Vault/secret blocker，不得读取
  或输出任何 secret。
- CLI 必须覆盖 `usdc-dex-transfer-batch-preflight`：当前主网 dry-run 状态下，它要对选中账号
  汇总 `ready_account_ids`、`blocked_account_ids`、`failed_account_ids` 和每个账号的
  transfer preflight evidence；未全 ready 时 `next_actions` 必须明确要求停止提交并逐账号清除
  blocker。
- CLI 必须覆盖 `usdc-dex-transfer-runbook`：未传 `--submit true` 时只返回 preflight 和
  `transfer=null`；传 `--submit true` 但 blocker 未清除时必须打印完整 JSON 证据、`submitted=false`
  并非零退出，不得加载 secret 或提交。blocker 全清后的真实提交必须回显 transfer response 和
  post-transfer balance evidence。
- CLI 必须覆盖 `usdc-dex-transfer-live-window`：默认只输出临时 live 配置预览，不写文件；
  `--write true` 只能写 `.codex-longrun/` 下的显式 output config，不得修改 `config/local.toml`。
  生成的临时配置应只包含必要 live gate：`app.dry_run=false`、
  `manual_ops.manual_live_enabled=true`、`manual_ops.mainnet_live_enabled=true`。
- 前端 Manual 页必须覆盖 `Transfer Preflight`：多账号选择时返回每个账号的
  `ready_for_mainnet_transfer`/`ready_for_testnet_transfer`、`failed_blockers`、`next_actions`；
  当前主网 dry-run 状态下它必须指出 `app.dry_run=false`、manual live gate、
  mainnet gate、Vault 解锁/EVM transfer signer 等 blocker，而不读取私钥、不签名、不提交。
- 前端 Manual 页必须覆盖 `Transfer Runbook`：单账号只读调用
  `/api/usdc-dex-transfer-runbook` 时必须返回 preflight、`transfer=null`、`submitted=false`
  和 blocker evidence；`Transfer USDC` 提交按钮必须调用同一个 runbook 端点，而不是绕过
  preflight 直接提交。多账号 Live 提交时必须按账号 fan-out，返回逐账号结果。
- 前端和 CLI preflight/runbook 验收必须检查 `readiness_summary`、`failed_blockers` 和
  `next_actions`。未 ready 时，这些字段要准确指出缺失项；ready 时，blocker 列表应为空。
- 前端 Manual 页默认只显示 `Preflight` / `Runbook Plan` / `Runbook Submit`，底层
  `Signed` / `Accept` 操作应在 `Advanced` 展开后才出现。
- 通过 `signed-acceptance --submit true` 提交极小订单。
- 至少跑一次 `--execution-mode taker` 计划，确认 JSON 中 `tif=Ioc`。
- 至少跑一次 `--execution-mode maker` 计划，确认 JSON 中 `tif=Alo` 且限价位于非穿价方向。
- 通过 `orderStatus`、open orders、recent fills 和 `userRateLimit` 对账。
- CLI smoke 必须覆盖只读 `reconcile-account` 和 `order-status`：前者能返回
  clearinghouse state、open orders、recent fills、`userRateLimit`；后者能按 `oid` 或
  `cloid` 查询单笔状态，且 `--oid`/`--cloid` 必须二选一。
- 若订单 resting，按 `cloid` 撤单并再次对账。
- CLI smoke 也必须覆盖 `signed-cancel --cloid <uuid>` 的 fail-closed 行为；真实 testnet
  订单 resting 时用它完成 signed cancel 并再次对账。
- 前端 Manual 页必须验证 `Order Status` 对 `oid`/`cloid` 的二选一校验：两者都空、两者
  都填、无效 UUID 都应在提交前或后端返回明确错误。
- 前端 Vault 页必须验证日常解锁只需要共享密码，确认密码只在展开 `Change Password`
  时出现；改密码提交前要本地校验新密码长度和确认一致性；保存 secret 在已解锁会话下
  不得要求每个账号单独输入密码。
- 平仓/卖出验收必须用 `--reduce-only true` 或前端 `Reduce Only`，确认不会扩大仓位。
- 前端 TP/SL 验收必须覆盖 `Check TP/SL` 和 `Submit TP/SL`：dry-run 下 submit 必须被
  阻断；未触发时 submit 不得要求 Vault；live testnet 下只有触发后的 reduce-only 退出单
  可以提交并对账。触发且 live gate 通过但 Vault 锁定时，应写入 attempt audit 后停止。
- 确认 CLI 结构化 JSON 中所有 `checks[].ok` 均为 `true`。

任何主网前必须完成 testnet order / cancel smoke test。

## Mainnet dry-run shadow

主网实盘前至少跑：

- 主网 metadata。
- 主网真实行情。
- 主网目标 leader 监听。
- 策略生成 intent。
- 风控裁决。
- executor 只记录，不下单。
- CLI `copy-shadow-smoke`：

```powershell
cargo run --manifest-path V2\Cargo.toml -- copy-shadow-smoke `
  --config config/dry-run.example.toml `
  --leader leader_a=0xLEADER_ADDRESS `
  --coin xyz:XYZ100 `
  --shadow-history logs/copy_shadow_history.jsonl `
  --synthetic-event true
```

该命令必须在 `app.dry_run=true` 下运行；它只生成 read-only watcher 订阅计划并可选跑一条本地
synthetic shadow 管线，不读取 Vault、不签名、不提交订单。输出 JSON 中 `checks[].ok` 应全为
`true`，`synthetic_records_written` 应大于 0。

真实 leader 的限时 read-only watch 可使用：

```powershell
cargo run --manifest-path V2\Cargo.toml -- copy-shadow-watch `
  --config V2\config\dry-run.example.toml `
  --leader scalp_1=0x6d6d7c05ef7f31b31b618400495b4ce4092a5089 `
  --leader scalp_2=0x6ac0b46b32dc429dbd129a503292f88649d2b8a0 `
  --leader scalp_3=0x117a7c349b953d54154312d97a20c9a2769adbd4 `
  --leader swing_1=0x9dead8fffcbf130e7658f672d2c081d91178d617 `
  --leader swing_2=0xd8c5228c515db3043dfa0c8cd6f22450ee9a99b0 `
  --environment mainnet `
  --shadow-history .codex-longrun\copy-shadow-watch-leaders.jsonl `
  --duration-secs 300 `
  --max-events 5000
```

`copy-shadow-watch` 只连接 read-only WebSocket 订阅并把事件喂给 dry-run shadow pipeline。
首包 `userFills` snapshot 只作为上下文观测，不触发跟单信号；只有实时增量 fill 且有匹配
position snapshot 时才可能生成 shadow record。

验证：

- 信号量合理。
- 风控拒绝原因合理。
- 不存在重复跟单。
- 不存在明显过大订单。
- 不存在 stale price 下单意图。

## Unattended copy live daemon acceptance

在启动任何无人值守 live copy daemon 或长时间 live window 前，先跑 gate：

```powershell
cargo run --manifest-path V2\Cargo.toml -- copy-live-daemon-acceptance `
  --config V2\config\dry-run.example.toml `
  --leader scalp_1=0x6d6d7c05ef7f31b31b618400495b4ce4092a5089 `
  --leader scalp_2=0x6ac0b46b32dc429dbd129a503292f88649d2b8a0 `
  --leader scalp_3=0x117a7c349b953d54154312d97a20c9a2769adbd4 `
  --leader swing_1=0x9dead8fffcbf130e7658f672d2c081d91178d617 `
  --leader swing_2=0xd8c5228c515db3043dfa0c8cd6f22450ee9a99b0 `
  --account-id addr_a `
  --coin xyz:XYZ100 `
  --side buy `
  --persistence .codex-longrun\copy-live-daemon-acceptance-dryrun-snapshot.json `
  --shadow-history .codex-longrun\copy-live-daemon-acceptance-dryrun-shadow.jsonl `
  --leader-notional-usd 120 `
  --leader-size 1 `
  --max-duration-secs 300 `
  --max-live-orders 1 `
  --max-total-notional-usd 50 `
  --max-total-fees-usd 0.10 `
  --max-slippage-bps 50
```

该命令只做准入验收，不读取 Vault、不签名、不提交订单。输出必须满足：

- `ok=true`。
- `checks[].ok` 全部为 `true`。
- `would_submit_orders` 非空且每笔都有 `cloid`。
- `restart_dedupe_probe.replay_emit_count=0`。
- `max_live_orders`、`max_total_notional_usd`、`max_total_fees_usd`、
  `max_slippage_bps` 均是显式有界值。这里的 `max_total_*` 是测试窗口熔断，
  不是 Smart Money Copy 的正式 sizing 规则。

对 live-capable config 复跑时需要额外传：

```powershell
--live true --allow-live-submit true --confirm-mainnet-live true
```

这仍然是 no-submit gate。它只确认 live 配置、主网确认、cleanup policy、
flat reconcile policy、kill-switch reduce-only policy 和 replay dedupe 都齐备。
真正 live daemon 长跑必须另有最大时长、最大订单数、异常清仓、kill switch 和最终对账证据。
正式跟单 sizing 按每交易对 `10U` 本金上限、最多 `5x` 杠杆计算；测试命令里的
`max_total_notional_usd` / `max_total_fees_usd` 仅作为测试窗口熔断。

通过 daemon acceptance gate 后，先跑 persistent daemon supervisor 的 no-submit
观察窗口：

```powershell
cargo run --manifest-path V2\Cargo.toml -- copy-live-daemon-supervisor `
  --config V2\config\local.toml `
  --leader scalp_1=0x6d6d7c05ef7f31b31b618400495b4ce4092a5089 `
  --leader scalp_2=0x6ac0b46b32dc429dbd129a503292f88649d2b8a0 `
  --leader scalp_3=0x117a7c349b953d54154312d97a20c9a2769adbd4 `
  --leader swing_1=0x9dead8fffcbf130e7658f672d2c081d91178d617 `
  --leader swing_2=0xd8c5228c515db3043dfa0c8cd6f22450ee9a99b0 `
  --account-id addr_a `
  --coin xyz:XYZ100 `
  --side buy `
  --persistence .codex-longrun\copy-live-daemon-supervisor-snapshot.json `
  --shadow-history .codex-longrun\copy-live-daemon-supervisor-shadow.jsonl `
  --leader-notional-usd 120 `
  --leader-size 1 `
  --duration-secs 300 `
  --max-events 5000 `
  --max-live-orders 1 `
  --max-total-notional-usd 50 `
  --max-total-fees-usd 0.10 `
  --max-slippage-bps 50 `
  --live-gate true `
  --allow-live-submit true `
  --confirm-mainnet-live true
```

该 supervisor 阶段仍然没有 live submit 路径：它不读取 Vault、不签名、不提交订单。
输出必须满足：

- `mode=copy_live_daemon_supervisor_no_submit`。
- `no_submit=true`。
- `acceptance.ok=true`。
- `checks[].ok` 全部为 `true`。
- `would_submit_orders` 只表示观察到的全部候选计划，不是已提交订单。
- `executable_would_submit_orders` 是当前 `max_live_orders`、
  `max_total_notional_usd`、`max_total_fees_usd` 测试窗口熔断内的候选集合。
  `max_live_orders` 约束的是可执行开仓/加仓候选数量；`reduce_only=true` 的 mapped close
  信号不得仅因为开仓 cap 已满而被 suppress。
- `suppressed_would_submit_orders` 保留超出 cap 的候选及原因；这些候选只作为
  observation evidence，不得进入未来 unattended submit。
- `executable_submit_plan_refs` 必须和 `executable_would_submit_orders` 一一对应，并保留
  `record_index`、`signal_id`、`leader_id`、`leader_address`，供未来 submit path 追溯到
  原始 shadow record。
- `suppressed_submit_plan_refs` 必须和 `suppressed_would_submit_orders` 一一对应；这些 refs
  仍然只是观察证据，不得提交。
- `submit_plan_contract.ok=true`，且 checks 必须证明 submit 只来自
  `executable_submit_plan_refs`、suppressed refs 与 executable refs 没有 cloid 重叠、
  signal/record refs 唯一、开仓数量和测试窗口 notional/fee 熔断均通过、pre-submit reconcile flat。
- `persistent_submit_dry_run.ok=true`；该字段只模拟 future persistent submit queue：
  逐条 executable ref 重新过 dry-run Risk Gateway，生成 `planned_reports`，且
  `dry_run_only=true`。它不得读取 Vault、签名或提交交易所订单。
- `planned_notional_usd <= max_total_notional_usd`，作为测试窗口熔断。
- `estimated_fees_usd <= max_total_fees_usd`，作为测试窗口熔断。
- `persistence_seen_keys_after >= persistence_seen_keys_before`。
- `persistence_ledger_entries_after >= persistence_ledger_entries_before`。
- `final_reconciliations[].ok=true`。
- `submit_evidence_contract.ready_for_unattended_submit=false` until the
  persistent daemon submit path records the same strict evidence as the bounded
  canary path.
- `submit_evidence_contract.checks` must include
  `persistent_live_submit_path_connected=false` in the current no-submit phase.

如果 `shadow_records_written=0` 但其他 checks 通过，只能说明窗口内没有捕捉到可跟单
leader 动作；需要拉长 no-submit soak，不得因此直接进入无人值守 live submit。

The daemon supervisor can pass as a no-submit observation window while still
blocking unattended submit. That is intentional. The submit evidence contract
lists the live evidence that must exist before widening: deterministic cloid,
orderStatus by oid/cloid, matching `userFills`/`userFillsByTime`, cleanup or
mapped close handling, formal per-pair principal/leverage caps, optional
test-window circuit breakers, and final flat reconcile.

通过 daemon acceptance gate 后，先跑 bounded live window 的 no-submit 包装验收：

```powershell
cargo run --manifest-path V2\Cargo.toml -- copy-bounded-live-window `
  --config V2\config\local.toml `
  --leader scalp_1=0x6d6d7c05ef7f31b31b618400495b4ce4092a5089 `
  --leader scalp_2=0x6ac0b46b32dc429dbd129a503292f88649d2b8a0 `
  --leader scalp_3=0x117a7c349b953d54154312d97a20c9a2769adbd4 `
  --leader swing_1=0x9dead8fffcbf130e7658f672d2c081d91178d617 `
  --leader swing_2=0xd8c5228c515db3043dfa0c8cd6f22450ee9a99b0 `
  --account-id addr_a `
  --coin xyz:XYZ100 `
  --side buy `
  --persistence .codex-longrun\copy-bounded-live-window-no-submit-snapshot.json `
  --shadow-history .codex-longrun\copy-bounded-live-window-no-submit-shadow.jsonl `
  --leader-notional-usd 120 `
  --leader-size 1 `
  --max-duration-secs 300 `
  --max-live-orders 1 `
  --max-total-notional-usd 50 `
  --max-total-fees-usd 0.10 `
  --max-slippage-bps 50 `
  --cleanup-max-slippage-bps 50 `
  --allow-live-submit true `
  --confirm-mainnet-live true
```

No-submit window must return `execution=null`, `preflight.submitted_reports=[]`,
`final_reconciliations[].ok=true`, and `ok=true`.

Only after that may a real bounded canary-live submit add `--submit true`. The
submit report must include:

- exactly one live submitted report;
- a passed `cleanup_notional_limit` preflight check proving the reduce-only
  cleanup path can cover the largest planned opening notional before anything
  is submitted;
- bundled cleanup runbook with no cleanup errors;
- final reconciliation for every target account with `open_order_count=0`,
  `asset_positions=0`, `total_ntl_pos=0`, and `total_margin_used=0`;
- `ok=true` before any wider live window is considered.

After the one-order canary is clean, use `copy-live-stability-soak` before
increasing account count, duration, or notional. This command repeats the
bounded live window under test-window circuit breakers; it still allows only
one target account and one live order per bounded round. It must stop if any
round fails, if the next round would exceed the configured test-window
notional/fee breaker, or if final reconciliation is not flat.

No-submit stability gate:

```powershell
cargo run --manifest-path V2\Cargo.toml -- copy-live-stability-soak `
  --config V2\config\local.toml `
  --leader scalp_1=0x6d6d7c05ef7f31b31b618400495b4ce4092a5089 `
  --leader scalp_2=0x6ac0b46b32dc429dbd129a503292f88649d2b8a0 `
  --leader scalp_3=0x117a7c349b953d54154312d97a20c9a2769adbd4 `
  --leader swing_1=0x9dead8fffcbf130e7658f672d2c081d91178d617 `
  --leader swing_2=0xd8c5228c515db3043dfa0c8cd6f22450ee9a99b0 `
  --account-id addr_a `
  --coin xyz:XYZ100 `
  --side buy `
  --duration-secs 300 `
  --interval-secs 60 `
  --max-rounds 2 `
  --max-live-orders 1 `
  --max-total-notional-usd 50 `
  --max-total-fees-usd 0.10 `
  --max-slippage-bps 50 `
  --cleanup-max-slippage-bps 50 `
  --allow-live-submit true `
  --confirm-mainnet-live true `
  --submit false
```

Submit stability gate:

```powershell
$env:TRADE_XYZ_VAULT_PASSWORD = "<operator-provided transient password>"
cargo run --manifest-path V2\Cargo.toml -- copy-live-stability-soak `
  --config V2\config\local.toml `
  --leader scalp_1=0x6d6d7c05ef7f31b31b618400495b4ce4092a5089 `
  --leader scalp_2=0x6ac0b46b32dc429dbd129a503292f88649d2b8a0 `
  --leader scalp_3=0x117a7c349b953d54154312d97a20c9a2769adbd4 `
  --leader swing_1=0x9dead8fffcbf130e7658f672d2c081d91178d617 `
  --leader swing_2=0xd8c5228c515db3043dfa0c8cd6f22450ee9a99b0 `
  --account-id addr_a `
  --coin xyz:XYZ100 `
  --side buy `
  --duration-secs 900 `
  --interval-secs 60 `
  --max-rounds 2 `
  --max-live-orders 1 `
  --max-total-notional-usd 100 `
  --max-total-fees-usd 0.50 `
  --max-slippage-bps 50 `
  --cleanup-max-slippage-bps 50 `
  --allow-live-submit true `
  --confirm-mainnet-live true `
  --submit true
Remove-Item Env:TRADE_XYZ_VAULT_PASSWORD
```

The stability report must include:

- `ok=true`.
- `rounds_attempted == rounds_passed`.
- `total_submitted_orders == rounds_attempted` in submit mode.
- `total_submitted_notional_usd <= max_total_notional_usd`.
- `estimated_fees_usd <= max_total_fees_usd`, using conservative open and
  cleanup fee estimation.
- Each round has `execution.ok=true`, orderStatus/userFills evidence, no
  cleanup errors, and flat final reconciliation.
- Top-level `final_reconciliations[].ok=true`.

## 性能测试

至少测试：

- 每秒市场事件吞吐。
- leader event 到 intent 延迟。
- intent 到 ApprovedOrder 延迟。
- ApprovedOrder 到提交前延迟。
- coordinator 广播 signal 到所有 workers 的 fan-out 延迟。
- 单 worker 故障时其他 workers 的执行延迟。
- 队列满时是否 fail-closed。

## 测试数据

测试数据不得包含：

- 真实私钥。
- 未脱敏用户地址，除非用户明确授权公开使用。
- 可恢复账户身份的敏感组合信息。

可包含：

- 公开行情。
- 脱敏 leader 地址。
- 模拟成交。

## 提交前命令

```powershell
cargo fmt --check
cargo check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```
