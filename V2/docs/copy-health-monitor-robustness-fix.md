# Copy Health Monitor Robustness Fix

## Incident

On 2026-07-17 the live-soak wrapper and API stayed healthy, but the PowerShell
health monitor exited after a status request timeout. Its exception path called
`Get-CimInstance Win32_Process` without protection; Windows returned
`ResourceBusy`/`内存不足`, so the monitor terminated instead of continuing its
watch loop.

## Fix

- Route every monitor process query through `Invoke-SafeCimProcessQuery`.
- Convert CIM failures into a bounded degraded state and rate-limited log entry.
- When process inspection is unavailable, prefer the live-soak API status and
  defer automatic restart rather than guessing that the soak is dead.
- Apply the same safe query to PID lookup, wrapper/child discovery, process
  cleanup, and restart PID capture.

## Verification

- PowerShell AST parse: passed.
- Simulated CIM `ResourceBusy`/`内存不足`: safe fallback returned without
  throwing and marked inspection unavailable.
- Static audit: only the safe helper contains direct `Get-CimInstance
  Win32_Process` calls.
- Full Rust suite: 423 passed, 0 failed.
- Repaired monitor: multiple 30-second cycles remained alive and logged healthy
  for run `20260717-155406` rounds 94-95.

## Operational boundary

The monitor now fails safe when Windows process inspection is temporarily
unavailable. It does not restart a running soak based only on an incomplete
process inventory; the next poll retries inspection while preserving API-based
health evidence.
