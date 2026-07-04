# Smart Money Copy Flip Follow Checklist

## Development Checklist

- [x] Document the failure mode and target behavior.
- [x] Add failing pipeline tests for Flip actions.
- [x] Aggregate pending fills by `(leader, coin)` before classifying against one snapshot.
- [x] Add leg-level risk decisions for close and open legs.
- [x] Preserve `flip-open` in the shadow pipeline.
- [x] Keep separate ledger entries for `flip-close` and `flip-open`.
- [x] Add reconnect/opposite snapshot Flip catch-up.
- [x] Add live submit close-open barrier.
- [x] Add same-round verified release after close chunk health.
- [x] Keep existing reduce-only precision/minimum fixes intact.
- [ ] Optional follow-up only if live evidence needs it: add durable deferred flip-open persistence.

## Verification Checklist

- [x] `cargo test semantic_flip_action_emits_close_then_open_legs`
- [x] `cargo test dry_run_shadow_`
- [x] `cargo test smart_money::tests`
- [x] `cargo test copy_live_daemon_`
- [x] `cargo test copy_ledger_`
- [x] `cargo fmt --check`
- [x] `git diff --check`

## Dry-Run Runtime Checklist

- [x] Generate a synthetic short-to-long Flip and confirm close/open records.
- [x] Confirm close ref is reduce-only.
- [x] Confirm open ref is non-reduce and normally risk-gated.
- [x] Confirm blocked-symbol Flip still closes but rejects the open leg.
- [x] Confirm no duplicate Flip records from multiple pending fills.
- [x] Confirm reconnect/opposite snapshot emits close/open Flip catch-up.

## Long-Soak Restart Checklist

- [x] Build a fresh D-backed binary so the active executable is not locked.
- [x] Stop or replace only the intended live-soak wrapper/health monitor.
- [x] Start real-submit follow-position soak with explicit `BotExePath`.
- [x] Verify API status reports the new binary and current run id.
- [x] Verify the first completed round has `ok=true`, `failed_checks=[]`, and `final_reconcile_health=true`.
- [ ] Watch for `FlipLongToShort`, `FlipShortToLong`, `COPY_CLOSE_WITHOUT_LOCAL_MAPPING`, and deferred flip-open counters.
