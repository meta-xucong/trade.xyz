# Smart Money Copy Uncovered Position Recovery Fix

Last updated: 2026-07-03

## Goal

Fix the recurring Smart Money Copy long-soak restart loop where
`final_reconcile_health` reports a meaningful unmanaged live position even
though the position is partially mapped by the Copy ledger.

The current concrete incident is:

- local account: `addr_b`
- coin: `xyz:SP500`
- side: `buy`
- live position notional: about `59.7248`
- mapped active Copy open notional: about `7.3786`
- uncovered residual: about `52.3462`

This document is the implementation-ready bug-fix plan for that class of
failure.

## Symptom

The health monitor repeatedly reports that the soak is no longer healthy and
restarts it after three polls.

Observed report detail:

```text
unmanaged live position(s) without copy ledger mapping:
addr_b:xyz:SP500:buy:uncovered=52.346200:live=59.724800:mapped=7.378600
```

Observed monitor behavior:

1. the child round finishes with `ok=false`;
2. the latest report carries `failed_checks=final_reconcile_health`;
3. the monitor records `soak_not_running`;
4. after three polls, the monitor restarts the soak;
5. the next run encounters the same residual and stops again.

This is not a pure websocket disconnect. The restart loop is caused by a
persistent ownership-attribution mismatch during post-round reconciliation.

## Root Cause

### What the snapshot contains

The current persistence snapshot contains an active `Open` ledger entry for
`addr_b xyz:SP500 buy`, but only for a small residual:

- `remaining_notional_usd ~= 7.3786`
- status: `Open`

The same snapshot also contains a `PendingReduce` for roughly the same small
residual:

- `pending_notional_usd ~= 7.4229`

Most older `SP500` open entries are already `Closed`.

### What live reconcile sees

The exchange reconcile sees a larger live position:

- `position_value ~= 59.7248`

The final health check builds active Copy open mapping notional from the
persistence snapshot and gets only the active residual:

- `mapped ~= 7.3786`

The remainder is treated as unmanaged:

- `uncovered = live - mapped ~= 52.3462`

### Why recovery does not fix it

`copy_live_daemon_recover_open_ledger_from_live_positions` currently short
circuits recovery whenever any `Open` or `PendingOpen` already exists for the
same:

- local account
- coin
- side

That logic is too coarse.

It correctly distinguishes:

- no mapping exists
- some mapping exists

But it does not distinguish:

- mapping is complete
- mapping exists but is materially smaller than the live position

As a result, a small surviving active `Open` residual prevents reconstruction of
the missing `52.3462` notional.

## Required Behavioral Change

Recovery must become notional-aware.

Instead of asking:

- "Does any active mapping exist for this account/coin/side?"

It must ask:

- "How much active mapping exists for this account/coin/side?"
- "Is the live position still materially larger than the mapped notional?"

If a meaningful uncovered residual remains, recovery must reconstruct only that
residual rather than skipping recovery entirely.

## Target Fix

### 1. Replace boolean active-mapping gating with residual gating

Current behavior in the recovery path:

- find any active `Open/PendingOpen`
- skip recovery entirely

New behavior:

1. compute active mapped notional for `account + coin + side`;
2. compute `uncovered_notional = live_position_notional - mapped_notional`;
3. if `uncovered_notional` is below tolerance, skip recovery;
4. otherwise recover only the uncovered residual as a new `Open` entry.

### 2. Recover only the uncovered residual

Recovered `Open` notional must be:

```text
max(live_position_notional - mapped_notional, 0)
```

Do not recover the full live notional when a partial active mapping already
exists, otherwise the active lineage will double count.

### 3. Preserve existing residual lineage

The existing `7.3786` active `Open` residual must remain in place.

The new recovery entry should account only for the missing residual, so that:

- ownership truth matches the full live position;
- pending reduce lineage can still consume the already-mapped residual;
- old active lineage is not destroyed;
- the same live position is not counted twice.

### 4. Keep existing safety boundaries

The fix must not:

- treat closed historical opens as active mapping;
- treat `PendingReduce` or `PendingClose` as sufficient live mapping;
- recover notional already explained by unrealized PnL drift;
- recover tiny uncovered residuals below exchange minimum when they are
  operationally ignorable;
- reintroduce the old bug fixed in `d56743e`, where stale reduce ordering could
  consume later opens incorrectly.

## Proposed Implementation Shape

Primary code path:

- [main.rs](/D:/AI/trade.xyz/V2/src/main.rs:16916)

Recommended implementation steps:

1. Add a helper that computes active mapped open notional for one
   `account + coin + side` from the snapshot.
2. In `copy_live_daemon_recover_open_ledger_from_live_positions`, replace the
   current `has_live_mapping` boolean with:
   - `mapped_notional`
   - `uncovered_notional`
3. Reuse the same uncovered/PnL drift logic family used by final reconcile to
   decide whether the residual is meaningful enough to recover.
4. When recovery is required, append a synthetic `Open` for only the residual.
5. Leave dedupe behavior intact so recovered residual entries can still be
   superseded later by stronger order-evidenced lineage if that becomes
   available.

## Data Contract For The Recovery Entry

Recovered residual `Open` should continue to follow the existing recovery shape:

- `status = Open`
- `filled_at_ms = now_ms()`
- `submitted_at_ms = shadow.occurred_at_ms`
- `filled_notional_usd = uncovered_notional`
- `remaining_notional_usd = uncovered_notional`
- `pending_notional_usd = 0`

But the notional source changes:

- old: full live position notional
- new: only uncovered live residual

## Test Plan

Add or update focused tests in [main.rs](/D:/AI/trade.xyz/V2/src/main.rs).

### Required new regression

`copy_live_daemon_recovers_uncovered_residual_when_partial_open_mapping_exists`

Setup:

- snapshot already contains an active `Open` for `addr_b xyz:SP500 buy` with
  `remaining_notional_usd = 7.3786`
- reconcile sees live `position_value = 59.7248`
- shadow recovery input contains a valid matching `would_copy` open lineage

Expectation:

- recovery appends a second `Open`
- recovered residual is about `52.3462`
- total active mapped notional becomes about `59.7248`
- final unmapped residual for that position disappears

### Required guardrail regression

`copy_live_daemon_recovery_does_not_duplicate_full_notional_when_partial_mapping_exists`

Expectation:

- total active mapping after recovery equals live notional
- not `live + preexisting_residual`

### Required no-op regression

`copy_live_daemon_recovery_skips_when_residual_is_only_pnl_drift`

Expectation:

- if uncovered residual is explainable by unrealized PnL drift, no recovery
  entry is appended

### Required tiny-residual regression

`copy_live_daemon_recovery_skips_below_minimum_uncovered_tail`

Expectation:

- small operational dust does not create spurious recovery opens

### Existing suites to rerun

- `cargo test --manifest-path V2\\Cargo.toml copy_live_daemon -- --nocapture`
- any focused `copy_live_daemon_follow_position_health` tests
- any focused persistence merge/recovery tests impacted by the new residual logic

## Live Verification Plan

After focused tests pass:

1. build a fresh D-backed target;
2. restart API and soak on the new binary;
3. watch the next round that previously failed on `SP500`;
4. verify that:
   - `final_reconcile_health=true`
   - no `unmanaged live position(s) without copy ledger mapping` appears for
     `addr_b xyz:SP500`
   - `/api/state` and `/api/copy/summary` agree on ownership
   - open orders remain zero unless a real executable signal appears

## Acceptance Criteria

The fix is complete when all of the following are true:

1. Focused Rust regressions pass.
2. The active mapping for `addr_b xyz:SP500` can recover from partial residual
   lineage to full live ownership truth.
3. The long-soak restart loop caused by `SP500 uncovered` no longer occurs.
4. The fix does not regress:
   - ghost-open filtering
   - pending-reduce retry recovery
   - reduce ordering
   - PnL-drift tolerance
5. No new double-counted Copy ownership appears in `/api/state` or
   `/api/copy/summary`.

## Non-Goals For This Fix

This document does not solve:

- the separate `submitted_reports=1, order_evidence=0` failure from
  run `20260702-155841` round 206;
- general websocket transport reliability;
- broader stale-ledger cleanup or historical compaction strategy;
- operator UI improvements around restart-loop explanation.

Those should be tracked separately once the `SP500` residual-attribution loop is
closed.
