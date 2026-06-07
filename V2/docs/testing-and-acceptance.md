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
- Multi-account concurrent submit with one slow worker.
- Worker restart with open orders.
- Native TP/SL arming and replacement policy.

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

