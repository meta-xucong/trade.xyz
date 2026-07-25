# Copy health monitor recovery/restart race fix

## Incident

On 2026-07-24 the health monitor detected live positions that were not yet
attributed to the Smart Money copy ledger. It correctly stopped the current
soak and ran the no-submit unattributed-ledger recovery.

The recovery report succeeded and cleared the position-truth risk at
`2026-07-24T15:21:23+08:00`. A later restart/status request then timed out.
The old monitor branch caught that later timeout in the same `catch` block as
the recovery itself, wrote `.codex-longrun/copy-live-soak-paused.flag` with
`position_truth_unattributed`, and disabled automatic restarts.

The `/api/copy/live-soak/start` request likely still reached the API despite
the client-side timeout: the next run id was `20260724-152138`, matching the
same time window. That runner later stopped at
`2026-07-25T07:04:40+08:00` after a recoverable websocket disconnect plus
temporary Hyperliquid info request failures, but the stale pause flag prevented
the monitor from restarting it.

## Root cause

The monitor conflated three different states:

1. unattributed recovery failed and the unsafe position risk remains;
2. unattributed recovery succeeded, but restarting the soak failed;
3. the frontend start request timed out even though the API asynchronously
   started a new soak runner.

Only state 1 should create a `position_truth_unattributed` pause. State 2 may
fail closed as `restart_failed`. State 3 should be treated as a successful
restart once status or process inspection proves a new runner exists.

## Fix

- `Start-CopyLiveSoak` now handles a timeout/error from
  `/api/copy/live-soak/start` by probing `/api/copy/live-soak/status` and the
  process table before propagating the error.
- If the API status or process table shows the soak is running after the
  client-side error, the monitor records the PID when possible and does not set
  a pause flag.
- The `position_truth_unattributed` branch now separates recovery errors from
  post-recovery restart errors.
- After a successful unattributed-ledger recovery, a restart failure is
  recorded as `restart_failed` instead of rewriting the incident as
  `position_truth_unattributed`.

## Acceptance checks

- PowerShell parser check for `V2/scripts/copy-health-monitor.ps1` passes.
- Source audit confirms there is no `Start-CopyLiveSoak` call inside the same
  `try/catch` block that writes `position_truth_unattributed` for recovery
  errors.
- Live restart is only attempted after confirming open orders and current
  position-truth risk are safe for the operator-approved long soak.
