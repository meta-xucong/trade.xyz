# Smart Money Copy Preflight Checklist

Use this checklist before implementing or enabling the V2 smart-money copy
module. The authoritative implementation spec is
[Smart Money Copy Development Spec](smart-money-copy-development.md).

## Documentation Ready

- `smart-money-copy-development.md` has been read.
- `architecture.md`, `internal-apis.md`, `risk-model.md`,
  `low-latency-execution.md`, and `execution-worker-model.md` have been read.
- Any planned behavior that differs from this checklist is documented before
  implementation.

## Exchange Data Ready

- Confirm target environment: `testnet` or `mainnet`.
- Confirm target market scope: `xyz_perp`, `hl_perp`, or `spot`.
- Confirm configured `hyperliquid.dex` for trade[XYZ].
- Confirm leader watcher subscription budget, including local account streams.
- Confirm fallback plan if unique user-specific WS subscription limits are hit.
- Confirm REST backfill window and rate-limit budget.

## Config Ready

- At least one enabled local account worker exists.
- Each local account has `copy_ratio`, `max_order_notional_usd`, and market
  permissions reviewed. For Smart Money Copy, sizing is principal-first:
  `leader_notional * copy_ratio` is treated as copy principal, capped at
  `10U` per trading pair, then multiplied by leverage capped at `5x`. The
  resulting formal per-pair order notional cap is therefore `50U`; test-window
  fee/notional caps are additional circuit breakers, not production sizing
  rules.
- At least one enabled leader exists.
- Each leader has `leader_id`, `leader_group`, address, weight, copy ratio,
  per-trade cap, daily cap, and open/close permission.
- Each enabled symbol has market, short permission, order cap, position cap,
  daily cap, and spread limits.
- `module_symbol_policies.copy_blocked_symbols` is reviewed.
- Mainnet live gates remain disabled until shadow acceptance passes.

## State Ready

- Decide the durable store for dedupe keys and copy ledger.
- Define retention and compaction policy for dedupe entries.
- Define startup order: load ledger, seed local state, seed leader state, then
  subscribe streams.
- Define fail-closed behavior when ledger cannot be read or written.

## Risk Ready

- Copy-specific reason codes are mapped to test cases.
- New opens require fresh leader position, fresh local account state, and fresh
  market data.
- Reduce-only closes can be allowed during kill switch only if configured and
  mapping is fresh.
- Pending exposure is counted against caps.
- Ambiguous leader events cannot open or increase exposure.
- Opposite-direction leader conflicts skip unless the configured score threshold
  is met.

## Test Fixtures Ready

- Raw leader fill fixture.
- Leader position snapshot fixture.
- Open, increase, reduce, close, and flip replay fixtures.
- WebSocket snapshot replay fixture.
- Reconnect + REST backfill duplicate fixture.
- Multi-leader same-direction fixture.
- Multi-leader opposite-direction fixture.
- Close-over-open fixture.
- Restart with pending open fixture.
- Restart with pending close fixture.

## Shadow Ready

- Dry-run executor is active.
- Audit logging is enabled.
- `copy-shadow-smoke` has been run with `app.dry_run=true`, target leaders,
  and the intended shadow history path.
- `copy-shadow-smoke --synthetic-event true` writes at least one
  `would_copy` shadow history entry without connecting a signer or submitting
  an order.
- Dashboard/Copy page can display leader health, semantic events, conflict
  decisions, risk rejections, and hypothetical orders.
- Shadow run stop conditions are written down.
- Review procedure is defined for duplicate signals, stale events, oversized
  hypothetical orders, and unmapped closes.

## Execution Canary Ready

- Run `copy-execution-canary` without `--live true` first. It must produce an
  approved shadow record and an `AccountExecutor::dry_run` submitted report.
- The canary must target explicit account ids and keep `--max-orders` equal to
  the expected number of approved records.
- A live canary must fail closed unless all live gates are explicit:
  `app.dry_run=false`, `manual_ops.manual_live_enabled=true`,
  `--allow-live-submit true`, one account, one order, and
  `--cleanup-after-submit true`; mainnet additionally requires
  `--confirm-mainnet-live true`.
- Before a real live canary submit, run the same command with
  `--preflight-only true`. It must return `ok=true`, non-empty
  `would_submit_orders`, empty `submitted_reports`, empty `cleanup_runbooks`,
  and empty `cleanup_errors`.
- For any live canary that will auto-clean up, the cleanup runbook notional
  limit must be at least the largest planned non-reduce-only copy order
  notional. In the default formal sizing profile this means
  `manual_ops.max_manual_order_notional_usd >= 50.0`, because the canary may
  open the full `10U * 5x` per-pair cap before submitting the reduce-only
  cleanup.
- Do not start unattended mainnet live copy after an opening canary unless the
  bundled reduce-only cleanup runbook and immediate post-submit reconcile path
  have also been tested for the same account, coin, and notional range.
- If `submitted_reports` is empty, treat the canary as a gate failure, not as a
  live-order event that needs cleanup.
- If `cleanup_errors` is non-empty or a live canary returns `ok=false`, treat it
  as an urgent reconciliation event before any wider live window.

## Unattended Daemon Acceptance Ready

- Run `copy-live-daemon-acceptance` before any unattended live copy daemon or
  long live window.
- This command is a gate, not the daemon. It must not load Vault secrets or
  submit orders. It verifies the configured leaders/accounts, bounded operator
  limits, persistence readability, restart/replay dedupe, deterministic cloid
  planning, cleanup policy, and post-submit reconciliation policy.
- The dry-run gate must return `ok=true`, non-empty `would_submit_orders`,
  `restart_dedupe_probe.replay_emit_count=0`, and all `checks[].ok=true`.
- For a live-capable configuration, the gate must fail closed unless
  `app.dry_run=false`, `manual_ops.manual_live_enabled=true`,
  `--allow-live-submit true`, bounded duration/order/notional/fee/slippage
  limits, cleanup policy, reconcile policy, and mainnet confirmation are all
  explicit.
- Official Hyperliquid websocket subscriptions can include snapshot replay
  data; snapshot messages such as `userFills` can be tagged with
  `isSnapshot=true`. The daemon must therefore use persisted seen-event keys
  and copy ledger state for idempotency across startup, reconnect, and replay.
- Official Hyperliquid order payloads include `reduceOnly`, TIF values such as
  `Ioc`, and optional `cloid`. Every V2-owned live copy order must carry a
  deterministic cloid so status queries, cleanup, and ownership checks can
  avoid touching unowned/manual orders.
- Persistent daemon supervisor reports must include
  `submit_evidence_contract`. In the current no-submit phase,
  `submit_evidence_contract.ready_for_unattended_submit` must remain `false`
  and `persistent_live_submit_path_connected` must remain a failed contract
  check. This prevents an observation-only `ok=true` report from being treated
  as permission to start unattended live submit.
- Persistent daemon supervisor reports must also include
  `persistent_submit_dry_run`. It may only consume
  `executable_submit_plan_refs`, must preserve each executable ref's cloid, and
  must return `dry_run_only=true` for every planned item. This stage is a
  worker-plan rehearsal only: no Vault unlock, signing, or exchange submit is
  allowed.
- Before unattended daemon submit can be considered, the persistent path must
  record the same strict evidence as the bounded canary path: deterministic
  cloid ownership, orderStatus by oid/cloid, at least one matching
  `userFills`/`userFillsByTime` fill for every filled order, cleanup or mapped
  close handling, per-pair principal/leverage caps, optional test-window
  circuit breakers, and final flat reconciliation.

## Live Window Ready

- One or two leaders only.
- One market only.
- Formal Smart Money Copy sizing uses per-pair `10U` principal cap and `5x`
  leverage cap. Any lower `max_live_orders`, `max_total_notional_usd`, or
  `max_total_fees_usd` value used in a command is a test-window circuit
  breaker.
- Dedicated local test account if possible.
- Kill switch tested.
- Reduce-only close path tested separately.
- Rollback procedure tested: pause strategy, stop opens, close mapped exposure
  if needed, reconcile, archive logs.
