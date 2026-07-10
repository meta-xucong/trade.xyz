# Smart Money Copy partial-close ledger fix

## Incident

On 2026-07-10, persistent live soak run `20260709-221743` stopped during round 407. The previous round had submitted one evidenced reduce-only order. The next round did not produce its report, and the health monitor paused automatic restart after position truth reported unattributed exposure.

The V2 API process remained alive. The stopped component was the persistent live-soak runner.

## Root causes

1. A completed reduce-only order whose actual fill was smaller than its planned close notional was recorded as if the full planned notional had closed. Persistence merge then consumed too much mapped open exposure.
2. Keeping a partially satisfied close in the same ledger entry would make order-evidence replay capable of consuming the fill twice and would retain the old CLOID, blocking a replacement reduce-only retry.
3. Pending full-close residuals were not included in persisted reduce retry recovery.
4. The health monitor gated on aggregate unattributed value across different accounts and symbols. Several individually untradeable residuals below the 10 USD exchange minimum could therefore add up to a false stop.
5. Persistence replay reset an open entry from its original filled notional for every closed-reduce record. Multiple partial closes therefore overwrote one another instead of accumulating; for example, a 100 USD open followed by 30 USD and 20 USD closes reconstructed as 80 USD rather than 50 USD.
6. After removing the aggregate monitor gate, a nonzero aggregate at or above 10 USD with missing or inconsistent position details could fail open because there was no individual row to evaluate.
7. The stop API trusted the derived `running` flag before checking the recorded PID. During the five-second round handoff it could report `running=false` while `pid_running=true`, write the pause flag, and return without killing the live wrapper; the wrapper then started the next round despite the operator stop.
8. The stop API treated PID liveness as sufficient process identity. If a stale PID were reused by an unrelated process, `taskkill /T /F` could terminate the wrong process tree.
9. The monitor's first aggregate/detail fail-closed guard covered missing details and an empty unattributed list, but not a partially returned non-empty list. An aggregate at or above 10 USD could therefore still fail open when the visible detail rows summed to a much smaller value and every visible row was individually below 10 USD.
10. The status API still treated a recorded PID as running based on liveness alone. A stale PID reused by an unrelated process could therefore make the soak appear healthy and prevent monitor recovery even though stop now refused to terminate that unrelated process.

## Fix

- Closed-reduce persistence consumes only `filled_notional_usd`.
- A terminal reduce fill now closes the evidenced ledger entry. Any unfilled target residual becomes a new deterministic, evidence-free pending entry that can receive a new CLOID and be retried safely.
- Replaying the original submitted report or order evidence resolves to the closed evidenced entry and is idempotent.
- Persisted retry recovery includes both `pending_reduce` and `pending_close` entries.
- The monitor only treats an unattributed position as a stop risk when that individual account/symbol position is at least 10 USD. Values are never aggregated across unrelated symbols to cross the threshold.
- Persistence replay establishes each open entry's original baseline once per merge, then applies every eligible close cumulatively in event order. Repeating the merge is idempotent.
- When aggregate unattributed value is at least 10 USD but position details are missing or contradict the aggregate, the monitor fails closed with an explicit `positions=unavailable` or `positions=inconsistent` diagnostic.
- The stop API targets a recorded live PID whenever either the active-round state or PID liveness proves the wrapper is running. A handoff window can no longer bypass process termination.
- Before termination, the stop API verifies that the candidate PID command line belongs to `run-persistent-live-soak.ps1` or `copy-live-daemon-supervisor`; otherwise it searches for the actual soak process and refuses to target the unrelated PID.
- For aggregate unattributed exposure at or above 10 USD, the monitor also verifies that unattributed detail values sum to the aggregate within one cent. A partial non-empty detail response now fails closed as `positions=inconsistent` without changing the valid cross-symbol behavior when complete detail rows reconcile to the aggregate.
- In the real `.codex-longrun` status directory, a recorded PID counts as running only when its command line also identifies a live-soak wrapper or daemon. Isolated status fixtures retain a liveness-only test seam, while production can recover from a reused unrelated PID.

## Verification requirements

- Partial close leaves the exact residual mapped and retryable.
- Duplicate report/evidence replay does not change the residual or create another retry entry.
- Closed-reduce merge uses actual fill and does not consume a later open.
- Multiple partial closed reduces accumulate against one open and repeated merge produces the same remaining amount.
- Pending close residuals produce deterministic reduce-only retry refs.
- The complete Rust test suite passes.
- PowerShell scripts parse successfully and the monitor threshold regression passes.
- Monitor regression covers complete sub-minimum cross-symbol details, one individually meaningful position, and missing/inconsistent details.
- Stop-target regression covers `running=false`, `pid_running=true` round handoff state.
- Stop-target identity regression accepts the wrapper and daemon commands, and rejects an unrelated PowerShell command plus the current test process.
- Monitor regression covers a partially returned non-empty detail list whose value contradicts the aggregate, plus the live complete-detail case where three individually sub-minimum positions reconcile to an aggregate above 10 USD without causing a false stop.
- Status/stop PID regression rejects the current unrelated test process for the production directory while preserving isolated status fixture behavior.
- A newly built binary completes a no-submit reconciliation before live soak is resumed.
- After restart, at least one persistent live-soak round reports `ok=true`, `final_reconcile_health=true`, no cleanup errors, and submitted/evidence parity.
