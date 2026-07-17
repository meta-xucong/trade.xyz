# Smart Money Copy zero-to-open classification fix

## Incident

On 2026-07-17, leader maker order `495433473972` filled `0.1` of
`xyz:SP500`. The leader changed from flat to long, but the local copy workers
did not receive an open signal. The runner remained online and received the
fill; the action was rejected as `COPY_ACTION_AMBIGUOUS`, leaving the target
accounts flat.

## Root cause

The clearinghouse adapter discarded position snapshots whose signed size was
zero. The classifier requires a before and after position snapshot to classify
perpetual fills. Consequently, a flat-before snapshot was unavailable and a
valid flat-to-long transition was treated as missing-snapshot ambiguity.

## Fix

- Preserve zero-size position snapshots so a flat position is an explicit
  classification boundary.
- If a non-reduce-only fill has no before snapshot but has a matching non-zero
  after snapshot, infer an open in the after-position direction. Reduce-only
  fills remain fail-closed when their before snapshot is unavailable.
- Keep the existing scope checks and position-delta classifier for complete
  snapshots.

## Safety and recovery

This change does not create a position or replay a historical fill. It only
allows a live, non-reduce-only fill with a matching post-fill position to enter
the normal Risk Gateway path. The deterministic signal/event id and ledger
dedupe rules remain unchanged. Historical missed orders require a separately
approved operator recovery; the runtime must not auto-backfill them.

## Verification

- Unit coverage for `0 -> positive` and `0 -> negative` position deltas.
- Unit coverage for open inference without a before snapshot.
- Adapter regression proving zero-size snapshots are retained.
- Existing `copy_live_daemon_*` and full Rust suites remain required before
  restarting the real-submit soak.
- A post-fix soak must show healthy rounds and submitted/evidence parity.
