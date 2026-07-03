# Smart Money Copy WebSocket Submit Fail-Closed Fix

## Problem

The live soak can stop with a misleading shape:

- `persistent_live_submit.submitted_reports` contains one `WorkerReport::Error`
- `order_evidence` is empty
- the message is `Hyperliquid websocket order post failed`

This is not the same as a submitted order missing evidence. It means the copy
submit path failed while posting the signed action through the Hyperliquid
WebSocket post channel.

Observed example:

- run: `20260704-003142`
- round: `30`
- account/coin: `addr_b xyz:BOT`
- order type: reduce-only close/reduce
- result: the health monitor restarted the soak, then ledger recovery cleared
  the temporary unattributed exposure.

## Risk

The order state is ambiguous at the instant the transport fails:

- the request may not have reached the exchange;
- the request may have reached the exchange but the response was lost;
- retrying other refs immediately can create ordering ambiguity across accounts.

The system must not treat this as a harmless safe skip. It should fail closed
for the current submit batch, keep the deterministic cloid and ledger intent in
place, then let the next exchange reconciliation decide whether to retry, map
evidence, or recover attribution.

## Fix

1. Preserve the lower-level WebSocket post error chain in `submit_fast` reports.
2. Classify WebSocket post failures as `submit_transport_failure`, separate from
   safe pre-submit skips such as below-minimum notional.
3. Abort the remaining submit refs in the current batch after the first
   transport failure.
4. Allow the long-soak health contract to continue when the only live submit
   failure is this fail-closed transport class and no live submitted report or
   cleanup error exists.
5. Keep timeout behavior strict: submit timeouts still fail closed and remain
   distinct from safe skips.

## Acceptance

- WebSocket post failures are visible with their root cause, such as response
  channel closed, send failure, stream ended, or request timeout.
- A transport failure does not masquerade as
  `submitted_reports=1/order_evidence=0`.
- The current batch stops after the first ambiguous submit result.
- The next live-soak round can reconcile and retry instead of requiring a full
  monitor restart when final position truth is otherwise healthy.

## Regression Tests

Required focused tests:

- transport failure classifier accepts WebSocket post failures and rejects safe
  pre-submit skips;
- live submit health accepts a fail-closed transport error with zero live
  submissions and no cleanup errors;
- full `copy_live_daemon` focused suite remains green.
