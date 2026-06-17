# V2 Testing And Acceptance

## Required Local Tests

- Config parsing and validation.
- Market symbol normalization.
- Precision and rounding.
- Risk approval and rejection.
- Worker command idempotency.
- Nonce monotonicity.
- WebSocket reconnect state recovery.
- Signal fan-out to multiple account workers.
- Duplicate signal deduplication.
- Kill switch behavior.

## Required Integration Tests

- Manual intent -> risk -> worker -> mock exchange.
- Fib intent -> risk -> worker -> mock exchange.
- Copy intent -> risk -> worker -> mock exchange.
- Copy leader watcher mock stream -> semantic event -> conflict decision -> risk -> mock workers.
- Multi-account concurrent submit with one slow worker.
- Worker restart with open orders.
- Native TP/SL arming and replacement policy.
- Copy restart with persisted dedupe/ledger must not rebuy the same leader event.
- Copy close signal must submit only reduce-only close commands for mapped exposure.

## Live Smoke Rules

- Live smoke requires explicit user approval.
- Use minimum practical notional.
- Test one market and one action class at a time.
- Confirm final position and open-order state after every smoke.
- Do not call a feature accepted until both the critical submit latency and
  final reconciliation are recorded.

## Acceptance Criteria For First V2 Runtime

- Frontend HTTP is not on the automated trading critical path.
- Two account workers can execute the same approved signal concurrently.
- Worker signer warm-up works after Vault unlock.
- Fast order submit uses official SDK signing and Hyperliquid WebSocket post.
- REST polling is not used in hot loops.
- Small live smoke leaves no unintended position or stale order.

## Acceptance Criteria For Smart Money Copy

- Raw `Buy` / `Sell` is not treated as strategy intent without position-delta classification.
- Open, increase, reduce, close, and flip are distinguishable in replay tests.
- Ambiguous leader actions fail closed for new exposure.
- Same leader event is deduped across WebSocket replay, reconnect, REST backfill, and worker retry.
- Same-symbol opposite-direction leader events are skipped or explicitly resolved by configured score.
- Any reliable leader close signal can close mapped local exposure with reduce-only orders.
- Leader partial-reduce signals are rejected unless V2 can prove mapped local
  exposure exists; approved reduce notional is capped by that mapped exposure.
- Pending reduce/close ledger entries reduce remaining effective exposure so
  repeated close signals cannot over-plan the same V2-owned position.
- Owned submit/fill reconciliation records V2 `cloid`, exchange `oid`, submit
  time, and fill time before moving copy ledger entries from pending states to
  `Open` or `Closed`.
- Read-only post-submit reconciliation queries `orderStatus` by owned `oid` or
  deterministic `cloid`, supplements filled size and average price from
  matching `userFills`, and does not mutate the copy ledger when order evidence
  is unowned or mismatched.
- Duplicate owned fill/order reports are idempotent, and unowned reports must
  not mutate the copy ledger.
- Pending opens count toward exposure caps before fill confirmation.
- Mainnet live copy is preceded by a dry-run shadow window with no duplicate or stale-price signals.
- Submit-capable bounded copy canaries fail closed before opening a live order
  when their configured reduce-only cleanup notional limit is lower than the
  largest planned opening notional.

