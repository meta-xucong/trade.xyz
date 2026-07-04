# Smart Money Copy Flip Follow Fix

## Problem

When a target account closes one side and quickly opens the opposite side, the watcher can observe one position transition:

```text
target snapshot before: short
target fills: buy-to-close, buy-to-open
target snapshot after: long
```

The semantic layer can classify this as `FlipShortToLong` or `FlipLongToShort`, and the signal generator can produce two legs:

- `flip-close`: reduce-only close of the old local side;
- `flip-open`: normal open of the new opposite side.

The current pipeline does not preserve that contract end to end. It treats Flip as a full close and filters out the non-reduce `flip-open` leg, so local accounts can become flat while the target is already reversed. Later target reduces can then fail mapping with `COPY_CLOSE_WITHOUT_LOCAL_MAPPING`, and local exposure can permanently diverge from the target.

## Root Causes

1. Action-level risk is reused for every leg. A Flip has both close and open semantics, but `risk_decision_for_action` returns one `CopySignalRiskDecision`.
2. Pipeline filtering treats Flip as close-only because `is_full_close()` includes `FlipLongToShort` and `FlipShortToLong`.
3. Pending fills can be classified repeatedly against one before/after snapshot when multiple fills arrive before the same position snapshot.
4. Live submit needs a close-open barrier. The open leg must not submit while the old local side is still ambiguous.
5. Deferred open must be recoverable. Existing persistence intentionally drops unsubmitted pending opens to avoid ghost exposure.

## Target Behavior

For a strongly classified perp Flip:

1. Aggregate same `(leader, coin)` pending fills that share the same position snapshot into one semantic action.
2. Produce exactly two candidate legs in order:
   - `flip-close`, reduce-only, capped to mapped local exposure;
   - `flip-open`, non-reduce, sized from target after-position notional and normal copy risk.
3. Allow the close leg to proceed even if the open leg is rejected by symbol, short, margin, or effective-min rules.
4. Submit the open leg only after the close leg has exchange evidence and live local exposure is reconciled.
5. If close state is ambiguous, defer the open leg and retry after reconciliation instead of submitting it blindly.
6. If the target is no longer in the new side when the deferred open is retried, cancel the deferred open.

## Implementation Status

Implemented on 2026-07-04:

- `CopyDryRunShadowPipeline` aggregates same `(leader, coin)` pending perp fills before classifying one before/after snapshot.
- Flip actions now run leg-level risk:
  - `flip-close` is reduce-only and capped to mapped local exposure;
  - `flip-open` is non-reduce and goes through normal open risk.
- `flip-open` is no longer filtered merely because the parent action is a full close.
- Reconnect/opposite snapshots now generate a Flip catch-up action instead of close-only catch-up when local mapped exposure is on the old side and the target snapshot is already on the new side.
- Live submit now adds a same account/coin close-open barrier: if a batch contains reduce-only and open refs for the same account and coin, it submits reduce-only refs as a first persistent-submit chunk and only proceeds to the open chunk if that close chunk is healthy.
- Signal age/timeout handling is unchanged. Delayed signals are not rejected or converted into no-ops by this fix.
- Runtime hardening found during redeploy: the exchange action-level rejection `Only post-only orders allowed immediately after network upgrade` is now treated as a safe pre-submit skip because no live order is accepted and there is no missing evidence to chase.

Not implemented as a separate data model: a durable deferred flip-open intent table. The current recovery path is same-round verified release plus reconnect/opposite snapshot catch-up. Add a durable deferred-intent queue only if live observation shows a second-chunk open transport failure can leave the account flat while the target stays reversed.

## Implementation Plan

### 1. Aggregate Pending Perp Fills

In `CopyDryRunShadowPipeline::handle_watcher_event(PositionSnapshots)`:

- collect pending fills matching the snapshot by `(leader_id, coin)`;
- use the earliest non-empty `before` snapshot and the new `after` snapshot;
- build one aggregate `LeaderFillEvent` for classification:
  - stable `event_id` from sorted pending fill event ids;
  - side from the last matched fill;
  - notional as the sum of matched fills for observability;
  - exchange time from the latest matched fill;
  - received time from the latest matched fill.

This prevents two rapid fills from generating duplicate Flip records.

### 2. Add Leg-Level Risk

Introduce an internal leg planning path:

```text
SemanticLeaderAction + CopySignalLeg -> CopySignalRiskDecision
```

Rules:

- close legs use mapped local exposure, `reduce_only=true`, leverage/copy ratio `1.0`, no principal cap;
- open legs use the leg's leader notional, normal copy ratio, account ratio, principal cap, leverage, symbol cap, short permission, and effective exposure cap;
- blocked symbols block opens but do not block mapped reduce-only closes;
- `allow_short=false` blocks `flip-open` short, while still allowing `flip-close`.

### 3. Preserve Flip Legs

Do not filter `flip-open` merely because the action is a full close. Pure `CloseLong` and `CloseShort` already produce only close legs. Flip must keep both generated legs.

Each emitted `CopyDryRunShadowRecord` should carry the risk result for its own leg. Ledger entries should keep separate signal ids and dedupe keys for `flip-close` and `flip-open`.

### 4. Live Submit Close-Open Barrier

Before live submit, group refs by Flip lineage:

```text
leader + action event id + account + coin
```

For each group:

- submit all reduce-only close refs first;
- collect order evidence for those close refs;
- fetch live local exposure;
- only then release the matching non-reduce `flip-open` refs.

If the close ref has a safe no-op because the account is already flat, the open can proceed only after live exposure confirms flat. If the close ref is a transport failure, timeout, no-fill, or ambiguous exchange result, the open must not submit in that round.

### 5. Deferred Open Recovery

If a `flip-open` cannot submit because the close leg was not safely confirmed:

- retry through reconnect/opposite snapshot catch-up while the old local mapping is still active;
- prefer same-round release after close evidence when live submit can prove the close chunk healthy;
- if a future durable deferred-intent queue is added, reload deferred intents and check:
  - target still holds the new side;
  - local old side is flat or dust-only;
  - local already-open same side does not duplicate the intent;
  - normal open risk still passes.

Only then recreate the submit ref. If the target no longer holds the new side, mark it cancelled.

## Test Plan

- `semantic_flip_action_emits_close_then_open_legs` remains green.
- Pipeline Flip from short to long emits two records: reduce-only buy close, then non-reduce buy open.
- Pipeline Flip from long to short emits two records: reduce-only sell close, then non-reduce sell open.
- Multiple pending fills before one opposite snapshot produce one Flip, not duplicate Flip actions.
- `allow_short=false` still emits close but rejects `flip-open` short.
- Blocked symbol still emits mapped close but rejects `flip-open`.
- Live submit sequencing splits same account/coin close-open batches into reduce-only then open chunks.
- Live submit sequencing does not release the open chunk when the close chunk is unhealthy.
- Reconnect/opposite snapshot catch-up emits close/open Flip instead of close-only catch-up.

## Acceptance Criteria

- Fast target reversals do not leave local accounts flat while the target is reversed.
- Close legs never close unrelated or unmapped exposure.
- Open legs never bypass normal open risk.
- Live submit never opens the new side while the old local side is still ambiguous.
- Tests cover generator, pipeline, reconnect catch-up, and submit sequencing.
- Dry-run verification passes before restarting real-submit soak.
