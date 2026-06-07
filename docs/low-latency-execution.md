# Low Latency Execution Fast Path

Last updated: 2026-06-07.

This document defines the low-latency execution path for follow trading,
Fibonacci automation, and other strategy-driven trading. It complements the
existing conservative signed runbook path; it does not replace it.

## Goal

Reduce real trading latency, not only UI waiting time, by removing synchronous
REST reconciliation from the critical submit path.

The fast path is optimized for:

- smart-money follow orders
- Fib Basic automatic entry and exit
- manual smoke tests that explicitly choose fast submit
- multi-account fan-out where every account-worker submits independently

The conservative runbook remains optimized for:

- pre-live acceptance
- manual diagnostics
- evidence-heavy troubleshooting
- operator workflows where full synchronous reconciliation is more important
  than speed

## Official API Basis

Hyperliquid WebSocket supports `post` requests for both `info` and signed
`action` payloads. The request format is:

```json
{
  "method": "post",
  "id": 123,
  "request": {
    "type": "action",
    "payload": {
      "action": {},
      "nonce": 1713825891591,
      "signature": {},
      "vaultAddress": null
    }
  }
}
```

The response arrives on channel `post` and is correlated by `id`.

The same signed action payloads used by `/exchange` are valid through WebSocket
post. Therefore this project must keep using the official Hyperliquid Rust SDK
for action construction, asset mapping, nonce, and signing, and only swap the
transport from HTTP POST to WebSocket post.

## Critical Path

Fast submit must be:

```text
strategy/manual signal
  -> local realtime cache risk/state check
  -> construct order plan from cached metadata/quote
  -> official SDK signs action payload
  -> WebSocket post action
  -> return accepted/rejected exchange response
  -> background reconciliation updates local state
```

The critical path must not synchronously call:

- `userFills`
- `openOrders`
- `orderStatus`
- full account reconciliation
- repeated market metadata fetches

Those calls are allowed only for:

- startup/reconnect seed
- cache miss fallback
- explicit diagnostics
- conservative runbook
- background reconciliation after the exchange accepted or rejected the action

## Realtime Cache Requirements

Before fast submit, the relevant cache entries should be fresh:

- market quote from `allMids`, `activeAssetCtx`, or equivalent stream
- positions from `allDexsClearinghouseState` or spot state
- open orders from `orderUpdates`
- fills from `userFills` / `userEvents`

If a cache entry is stale or missing, the fast path may perform one REST fallback
when the operation is operator-triggered. For strategy automation, stale cache
must fail closed unless the strategy explicitly allows a REST fallback.

## Submit Modes

### `fast`

Low-latency path. Returns after the WebSocket post action response. Background
reconciliation confirms fills, position changes, residual cleanup, and stale
protective order state.

### `strict`

Existing signed runbook path. Returns only after synchronous preflight,
submission, order-status checks, and reconciliation.

### `dry_run`

No signed action. Builds the same order plan and returns deterministic local
reports.

## TP/SL

Native TP/SL is still exchange-native:

- perps use `grouping = "positionTpsl"`
- spot uses `grouping = "normalTpsl"`

For post-open automatic protection, fast TP/SL should skip expensive historical
fill lookup and submit the two trigger legs directly after a fresh matching
position is visible in realtime cache. If a matching position is not visible,
the strategy must wait for the fill stream or perform a single bounded fallback.

Manual replacement of existing TP/SL can use strict mode because cancelling and
replacing old protection is less latency-sensitive than first protection after
entry.

## Close Orders

Fast close must:

- prefer realtime position size for `close_full_position`
- submit `reduce_only + IOC`
- optionally cancel same-coin resting orders using realtime open-order cache
- reconcile residuals in the background

If a residual remains after close, the background cleanup loop may submit a
second reduce-only cleanup order. The initial fast response should not wait for
full historical fill reconciliation.

## Multi-Account Rules

- The coordinator broadcasts one `signal_id`.
- Each account-worker signs and posts independently.
- Cross-account operations run concurrently.
- Within one account, signed actions stay sequential through that account's
  nonce manager.
- A failure from one account must not delay another account's first submit, but
  the aggregate result must surface missing/failed accounts immediately.

## Rate Limits

WebSocket post does not remove Hyperliquid action limits. It reduces transport
overhead and avoids REST info polling in the hot path. Continue to respect:

- address-based action limits
- WebSocket message limits
- WebSocket in-flight post limits
- REST `/info` weight limits for fallbacks and diagnostics

## Frontend Contract

Fast path UI should distinguish:

- `已提交`: exchange accepted the action
- `已成交`: fill or position update observed
- `已保护`: TP/SL trigger orders observed
- `已平仓`: position flat and no same-coin stale protection remains

Do not call a fast submit "fully confirmed" until background reconciliation
has observed the necessary state transition.

## Acceptance Criteria

- Rust tests pass.
- Fast submit endpoint signs with official SDK logic and posts via WebSocket.
- Conservative runbook endpoint still works.
- A dry-run/simulated fast submit returns an order report without REST
  reconciliation.
- A small mainnet smoke records:
  - fast open submit latency
  - fast TP/SL submit latency
  - fast close submit latency
  - final strict or fresh-state verification that the position is flat and no
    stale protective order remains

