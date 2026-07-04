# Smart Money Copy Account-Coin Conflict Arbitration

Last updated: 2026-07-04.

## Problem

Hyperliquid perp accounts are net-position accounts for one account and one
coin. If the same local copy account submits a new opposite-side order while it
still has mapped exposure on the other side, the exchange can reduce or flip the
net position even when the new signal belongs to a different leader. That can
break copy attribution and make a later mapped close hit the wrong exposure.

The existing `resolve_copy_conflict` function defines useful leader conflict
rules, but the live daemon did not call it in the submit path. The live path
therefore needed an explicit submit-plan arbitration layer.

## Scope

The arbitration scope is:

```text
local_account_id + coin
```

Different local accounts are independent. `addr_a` may be long while `addr_b`
is short on the same coin.

## Required Rules

- Reduce-only closes are never suppressed by conflict arbitration. They still
  need the normal mapping, minimum-size, and live-submit checks.
- Same-account same-coin opposite open candidates in the same submit batch must
  be resolved before open-order caps are applied:
  - if one side wins by the configured score ratio, only that side remains;
  - if neither side wins, all new opens for that account/coin are suppressed;
  - the current implementation uses the existing `resolve_copy_conflict`
    function with local planned notional as score weight and the default ratio
    `1.5`.
- A new open must not be submitted against existing opposite copy exposure
  unless executable reduce-only closes in the same account/coin plan cover that
  opposite exposure first.
- A new open must not be submitted against live opposite exposure that cannot be
  attributed to the copy ledger after planned reduce-only closes are applied.
- Same-leader close-then-open flips remain valid only when the close leg is
  still executable. The existing close/open barrier then submits reduce-only
  orders before open orders for the same account/coin.

## Implementation Plan

1. Add a submit-plan conflict resolver in `V2/src/main.rs`.
2. Run it once after margin/follow-position preparation and before
   `max_live_orders` partitioning, so same-batch opposite opens cannot be hidden
   by the order cap.
3. Run it again after effective-min filtering, so an open is not allowed based
   on a close leg that was later suppressed as exchange-invalid.
4. Emit explicit suppression reason codes:
   - `COPY_DAEMON_ACCOUNT_COIN_CONFLICT_NO_DECISION`
   - `COPY_DAEMON_ACCOUNT_COIN_CONFLICT_LOST`
   - `COPY_DAEMON_OPPOSITE_COPY_EXPOSURE`
   - `COPY_DAEMON_OPPOSITE_UNATTRIBUTED_EXPOSURE`
5. Add focused unit tests proving:
   - same-account balanced opposite opens are suppressed;
   - weighted/notional winner side remains executable;
   - different local accounts may hold opposite directions;
   - existing opposite mapped exposure blocks a new open;
   - a close-then-open flip remains executable when the reduce leg covers the
     opposite exposure.

## Deferred Work

Leader `leader_group`, `weight`, and conflict ratio are documented in the
operator config spec but are not yet fully represented in the current runtime
config structs. This fix keeps the first live safety boundary local to the
submit plan and defaults each leader group to `leader_id` with weight `1.0`.
Making those fields operator-configurable is a follow-up config/API task.
