# Smart Money Copy Ultimate Operations Plan

Last updated: 2026-06-24

## Goal

Smart Money Copy must run as a long-lived, real-submit copy module that a non-technical operator can use from the frontend:

1. paste target smart-money addresses;
2. choose one or more local accounts;
3. choose one or more markets;
4. set copy ratio and per-symbol principal cap;
5. start or cancel copy trading;
6. read the result from one compact strategy card.

The live path must remain:

`Strategy/Operator Intent -> Risk Gateway -> Account Worker -> Executor`.

Frontend and API handlers configure, observe, and send operator controls only. They must not directly submit exchange orders.

## Current Acceptance Baseline

The final acceptance state is evidence-based, not configuration-based:

- Settings file contains the intended leaders, local accounts, markets, copy ratio, principal cap, and leverage.
- The running process command line contains one `--account-id` for every selected local account.
- The running process command line contains one `--market` for every entry-enabled market.
- In live-submit mode, every selected local account must pass signer preflight before the watcher window is considered healthy. A missing Vault password, expired local Vault session cache, or unavailable account secret is a hard pre-submit failure and must be shown by account id.
- The live-soak status API reads the runtime account and market scope from the latest runtime log, not just saved settings.
- The copy summary API returns:
  - aggregate `local_summary` for all selected local accounts;
  - per-account `local_summaries`, including flat local accounts;
  - target account list and target comparison fields;
  - current run submitted/evidence counts;
  - current live open-position value and unrealized PnL when available.
- A no-submit window with zero submitted orders is acceptable only when the report explains that there were no executable signals or that candidates were suppressed by explicit reason codes.
- A real-submit window with held positions is healthy when final account reconciliation is readable, open orders are zero, exposure is within cap, and follow-position mode is intentionally waiting for mapped target close signals.

## Final Runtime Model

### Accounts

- A selected local account is a real execution target.
- Multiple selected local accounts must be planned and executed independently.
- A multi-account live run is only accepted after every selected account's trading signer can be loaded from either the current process environment or the 30-day local Vault session cache. This prevents a late live signal from failing only because a secondary account was not warmed.
- The current implementation uses one watcher/supervisor window to classify target signals, then fans out one account-specific intent per selected local account through the normal risk and worker path. A future one-supervisor-lane-per-account split is acceptable if nonce, margin, ledger, and failure isolation need to be stronger.
- A status API may aggregate lanes into one strategy card, but it must preserve per-account evidence:
  - configured accounts;
  - running accounts;
  - latest round;
  - latest failed check;
  - submitted order count;
  - order evidence count;
  - current exposure and PnL when available.
- The summary API must expose both an aggregate `local_summary` and per-account `local_summaries`, so the frontend can stay compact while still showing which local account is carrying which PnL.
- Flat local accounts still appear in `local_summaries`. A missing account row means an actual read/configuration problem, not merely no position.

### Markets

- Selected markets are entry-enabled.
- Unselected markets are exit-only after the new config is effective.
- The watcher may subscribe broadly, but submit planning must suppress new opens outside selected markets.
- The default selectable production scope is `xyz_perp`, `hl_perp`, `cash_perp`,
  and `spot`. A leader coin such as `cash:USA500` is therefore a first-class
  copy target instead of an unknown market.
- Reduce-only closes must remain allowed when they map to local exposure, even if the market has since been removed from entry scope.

### Signal Classification

- Perp fills require position-delta confirmation where available, because trade side alone can misclassify close vs open.
- Spot fills do not have perps clearinghouse position snapshots and may be classified directly:
  - spot buy => open/increase spot exposure;
  - spot sell => same-source mapped close/reduce.
- Snapshot/replay fills are ignored for new entries.
- Duplicate fill events are deduped before pending classification.

### Scalp-Friendly Rules

The old over-conservative approach protected the account but missed fast target trades. The final rule is:

- Do not blindly chase old increases.
- Do react quickly to fresh opens when the action is strongly classified.
- Do not follow late increases without a same-source local mapping.
- Do close/reduce aggressively when a mapped target close is observed.
- Do generate reconnect catch-up reduce-only closes when a target is already flat and the local ledger still has mapped exposure.

The practical boundary is:

- Fresh open signals may create new mapped exposure after risk checks pass.
- Increase signals on an already observed target position are allowed only if the same leader already has a local mapping; otherwise they are rejected as `COPY_INCREASE_WITHOUT_LOCAL_MAPPING`.
- Close/reduce signals without a local mapping are recorded as `COPY_CLOSE_WITHOUT_LOCAL_MAPPING`, but must never close another leader's or another strategy's exposure.
- Reduce-only candidates bypass open exposure caps and open-order caps, but still require readable local exposure and exchange-valid size.

Recommended defaults:

- `max_signal_delay_ms`: observability and alerting threshold only. It must not
  change open/increase/reduce/close behavior by itself.
- scalp profile: strict slippage, smaller principal cap, no tail-chasing
  increases;
- swing profile: wider alert tolerance, same mapped-close discipline.
- When high signal delay is observed, optimize the event path and scheduling
  priority. Do not "fix" delay by refusing otherwise valid copy signals.

### Sizing And Risk

Open sizing:

`leader_notional * copy_ratio * account_copy_ratio`, capped by per-symbol principal cap, then multiplied by leverage.

Rules:

- open notional must meet exchange minimum;
- open notional must meet the exchange minimum after market precision and size
  rounding, not only before rounding;
- open notional must fit per-symbol cap;
- account total exposure cap must include existing exposure plus planned opens;
- margin precheck must use selected leverage plus a buffer;
- margin-resized opens that cannot still produce an exchange-valid effective
  notional must be suppressed before submit planning, with an explicit reason
  such as `COPY_DAEMON_EFFECTIVE_NOTIONAL_BELOW_MIN`;
- reduce-only closes do not consume open-order notional or fee budget;
- reduce-only closes must be capped to actual local matching exposure.
- Multi-account caps must be interpreted deliberately:
  - per-symbol principal cap is per local account;
  - total notional/fee caps are live-soak guardrails across the current run;
  - `max_live_orders` must be at least the number of selected local accounts so a valid signal can fan out to every selected account. Do not apply an arbitrary low cap such as 3 when more local accounts are selected.

### Leverage Handling

Before live submit:

- ensure target leverage is supported for that coin/market/account;
- if setting leverage fails, retry boundedly and record the exact account/coin/target leverage;
- if current leverage is known and safe, optionally recompute required margin with current leverage;
- otherwise fail closed before submitting an open order.

The operator card must show this as an actionable failure, not as a generic stopped run.

### Persistence And Recovery

The durable copy ledger is the source of truth for mapped copied exposure.

On restart or reconnect:

- load the previous ledger;
- use the shared resume snapshot by default for operator/frontend starts and
  monitor restarts, so a normal restart does not silently lose mapped exposure;
- restore the Vault session from the local DPAPI-protected 30-day cache when
  `TRADE_XYZ_VAULT_PASSWORD` is not present in the spawned CLI process;
- reconcile readable live positions;
- prune stale ledger entries only when a readable account snapshot proves no matching local exposure;
- keep reduce-only catch-up active for mapped exposure;
- never close unrelated manual/V1/unowned exposure unless explicitly instructed by the operator.
- If network/DNS breaks while targets sell, recovery must compare the restored ledger and live target/local snapshots. Mapped local exposure whose target is confirmed flat should be reduced after reconnect.
- Recovery must also handle partial active mapping. If live local notional is
  materially larger than the currently mapped active Copy open notional for the
  same account/coin/side, the daemon must recover only the uncovered residual
  instead of treating any small surviving active mapping as complete coverage.
  The current implementation note and fix plan are documented in
  [smart-money-copy-uncovered-position-recovery-fix.md](/D:/AI/trade.xyz/V2/docs/smart-money-copy-uncovered-position-recovery-fix.md).

### Health Monitor

The monitor must distinguish:

- running healthy;
- watcher-only degraded network/DNS;
- stale heartbeat;
- hard submit failure;
- account health failure;
- restart failed.

It must not loop on an old failed run id as if it were fresh. After a restart request, it must verify:

- new process id exists;
- new run id or new log timestamp exists;
- account ids and markets match current settings.
- live-submit signer preflight passes for every selected local account. If it
  does not, stop/retry only after the Vault is unlocked or the cache is
  refreshed; do not keep a process running that can watch signals but cannot
  execute one selected account.

The versioned scripts under `V2/scripts` are the source of truth. Runtime files
under `.codex-longrun` may store pid/log/snapshot state, but must not silently
override newer V2 script behavior. Frontend start and monitor fallback should
prefer `V2/scripts` and use `.codex-longrun` script copies only as compatibility
fallbacks.

Health-monitor restarts must preserve the operator's configured leader list. A
restart helper must never shrink the target account set back to an older
default. The runtime proof is the `copy settings leaders=N ...` line in the
latest soak log, plus the live supervisor command line.

### Submit Failure Policy

Not every failed candidate means the long-running copy process is unsafe:

- A failure before an exchange order is submitted, such as a bounded leverage
  setup failure, is a pre-submit skip. It must be surfaced to the operator with
  account, market, coin, and reason, but it should not stop the whole soak by
  itself because no new local exposure was created.
- A failure after an exchange order may have been submitted, missing order
  evidence, cleanup failure, unreadable account health, or an unknown submit
  state remains a hard stop until reconciled.
- Leverage setup should cap to exchange metadata when available. If the
  exchange rejects the bounded leverage update anyway, skip that candidate,
  keep watching, and preserve the error in `submitted_reports`/status details.
- Reduce-only candidates are exit safety actions. They must not consume
  open-order count, open-notional budget, or open-fee budget, though they still
  require readable matching local exposure and valid exchange sizing before
  submit.

## Frontend UX Contract

The Smart Money Copy card should stay simple:

- target accounts text area;
- local account multi-select;
- copy ratio;
- per-symbol principal cap;
- market multi-select;
- start real test;
- one-click cancel.

The result card should show:

- running/stopped/degraded;
- configured local accounts;
- actually running local accounts;
- selected markets and exit-only markets;
- latest round and last update time;
- trades submitted and evidenced;
- local PnL and target comparison when available;
- last failure reason in plain language.
- current-run order count separately from historical or pre-existing live positions, so "0 submitted this run" does not hide existing held copy exposure.
- true submitted order count must include only live `Submitted` worker reports.
  Pre-submit skips, leverage skips, and exchange-minimum suppressions are shown
  as skipped/suppressed reasons, never as trades.

Advanced details may be collapsible, but refresh must not collapse a user-opened section.

## Acceptance Checklist

- Settings persist across refresh.
- Backend start consumes all selected account ids.
- Live-submit preflight proves all selected account signers are available from
  the current unlock session or 30-day cache.
- Status proves which accounts are actually running from the latest runtime log,
  not merely from saved settings.
- Summary returns one row per selected local account, including flat accounts.
- All selected markets are entry-enabled, including `cash_perp` when selected.
- Removed markets are exit-only.
- Sell/reduce mapped close remains sensitive and reduce-only.
- Leverage setup failure has bounded retry/fail-closed diagnostics.
- Precision-rounded minimum order failures are suppressed before submit and are
  not counted as trades.
- Health monitor can restart from a fresh run instead of looping on stale failed status.
- Focused tests pass.
- A fresh real-submit all-market multi-account soak starts from a D-backed binary and is visible in the frontend.

## Final Audit Addendum

For the 2026-06-24 final pass, acceptance evidence must also check the difference between "no signal", "signal skipped", and "signal submitted":

- A live run can be healthy with zero submitted orders only when the report shows either zero executable target signals or explicit pre-submit suppression reasons.
- Margin-resized opens below the exchange minimum are safe skips, not missed signals, but the frontend must display the reason so the operator can decide whether to add margin, reduce exposure, or wait for target exits.
- Target-account position data must not be displayed as confirmed flat when the frontend only has no fresh target snapshot. Use the summary `target_position_state` field:
  - `current`: target positions were refreshed for every configured leader;
  - `partial`: at least one configured leader was refreshed;
  - `unavailable`: target positions were not refreshed and should be treated as unknown.
- Runtime proof for multi-account/all-market copy is the active process command line plus status API, not the saved settings alone.
- A successful multi-account live fan-out requires the same signal to produce account-specific submitted reports for every selected local account or explicit per-account skip reasons.
- A missing signer is never an acceptable per-account skip after a target signal
  arrives. It must be caught by `copy_signers_available` before the run is
  treated as healthy.
- Per-account skip reasons are first-class acceptance evidence. If `addr_a`
  cannot follow because `withdrawable` margin only supports a resized notional
  below the exchange minimum, while `addr_b` submits the same signal, that is a
  correct risk skip rather than a signal miss. The frontend must show this in
  plain language with account and coin.
- Immediate live submit and final window reporting must use one coherent plan
  view. A cloid that was already submitted inside the watcher loop must not be
  counted again as a later suppressed candidate, otherwise the operator would
  see the impossible state "0 executable plans but 1 submitted order".
- The copy summary endpoint is an observability endpoint, not a trading gate.
  Slow exchange reads must time out quickly and return the latest local
  runtime evidence instead of freezing the operator page.

## 2026-06-24 Ultimate Audit Result

The final implementation target is not "copy every target fill blindly"; it is
"classify every configured leader fill, fan it out to every selected local
account, and either submit or show the exact per-account reason it was skipped".

Current evidence requirements:

- local fan-out is accepted only when runtime logs/API show every selected
  account, such as `addr_a` and `addr_b`, not merely when settings contain them;
- market coverage is accepted only when `selected_markets` and watcher evidence
  include every selected market scope (`xyz_perp`, `hl_perp`, `cash_perp`,
  `spot`);
- a target signal is not considered missed when it becomes a risk skip with a
  stable reason code such as `COPY_DAEMON_MARGIN_RESIZED_BELOW_MIN`;
- if only one local account submits and another local account skips, the result
  is accepted only when the report carries both the submitted order evidence and
  the skipped account's reason;
- the live-soak process may be `paused` only after an explicit operator cancel
  flag. A paused process is not a crash, and the health monitor must not restart
  it until the operator starts copy again;
- reports must never count the same cloid as both submitted and suppressed in
  the same final window.

The remaining live-testing risk is operational capital, not signal detection:
when withdrawable margin is almost fully consumed, the bot correctly resizes or
skips new opens before submit. The frontend must make this visible in plain
language so the operator can decide whether to add margin, close exposure, lower
the cap, or wait for mapped target exits.
