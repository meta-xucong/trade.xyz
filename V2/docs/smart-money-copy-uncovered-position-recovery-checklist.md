# Smart Money Copy Uncovered Position Recovery Checklist

Last updated: 2026-07-03

Use this checklist when implementing and verifying the uncovered-position
recovery fix described in
[smart-money-copy-uncovered-position-recovery-fix.md](/D:/AI/trade.xyz/V2/docs/smart-money-copy-uncovered-position-recovery-fix.md).

## Development Checklist

- Replace boolean `has_live_mapping` recovery gating with mapped-notional
  calculation.
- Recover only `uncovered_notional`, not the full live position notional.
- Reuse existing PnL-drift tolerance rules so normal mark drift does not create
  synthetic recovery opens.
- Keep `PendingReduce` and `PendingClose` excluded from active live mapping.
- Keep order-evidenced lineage preferred over recovered synthetic lineage.
- Preserve existing closed-reduce ordering semantics.

## Test Checklist

- Add a regression where an active open residual already exists and live notional
  is materially larger.
- Assert that the recovered residual matches `live - mapped`.
- Assert that total active mapping after recovery equals the live notional.
- Assert that tiny uncovered dust below the minimum does not create a recovered
  open.
- Assert that unrealized-PnL drift does not create a recovered open.
- Re-run `copy_live_daemon` focused tests.
- Re-run any focused follow-position health tests touched by the change.

## Runtime Verification Checklist

- Build a fresh isolated target directory.
- Confirm API restarts on the new binary, not an older fallback path.
- Confirm live soak start passes explicit `BotExePath`.
- Confirm the next soak report does not fail `final_reconcile_health` for
  `addr_b xyz:SP500`.
- Confirm `/api/state` shows Copy ownership that matches the live `SP500`
  position after recovery.
- Confirm `/api/dashboard-open-orders` remains empty unless a real executable
  order is created.
- Confirm the health monitor stops restarting the soak for the same `SP500`
  uncovered residual.

## Regression Watchpoints

- Do not double-count a partially mapped live position.
- Do not convert historical `Closed` records into active ownership.
- Do not revive ghost opens without execution evidence.
- Do not let a recovered residual hide a true manual or non-Copy position.
- Do not break the previously fixed `PendingReduce` recovery path.
