# AGENTS.md

Project guidance for V2 of the trade.xyz automated trading system.

## V2 Objective

V2 replaces frontend-driven live execution with a low-latency runtime:

- one long-lived account worker per local trading account
- pre-warmed signer state after Vault unlock
- WebSocket-first market/account state
- internal signal channels from strategies and copy trading into account workers
- frontend used only for configuration, observation, and operator controls

The goal is faster and more deterministic strategy/copy execution while
preserving V1 safety rules.

## Source Of Truth

- Official Hyperliquid docs and SDK behavior remain the protocol source of
  truth.
- Re-check official docs or live metadata before changing exchange behavior,
  precision rules, TP/SL grouping, transfer flows, or rate-limit assumptions.
- Keep observations from live behavior documented separately from official
  rules.

## Version Boundary

- Do not depend on V1 source modules at runtime.
- V2 may reuse ideas and documented lessons from V1, but code should be copied
  deliberately only when it is reviewed and adapted to V2 boundaries.
- V2 must have its own config, runtime state, logs, tests, and frontend assets.
- Never change V1 while implementing V2 unless the user explicitly asks for a
  V1 compatibility or hotfix change.
- V2 may observe exchange-open orders on accounts shared with V1, but bulk
  cancel/strategy-stop actions must only cancel orders that V2 can prove it
  owns through local order refs, deterministic cloids, or an equivalent durable
  ownership ledger. Orders from V1, other tools, or manual orders without V2
  ownership evidence must be displayed as unowned/manual and left untouched.
  Existing exchange-native TP/SL protection for open positions must not be
  cancelled by a bulk strategy-stop action unless the user explicitly asks to
  remove protection or close the position.

## Architecture Rules

- Build V2 in pure Rust for all live paths.
- Keep modules independent and communicate through typed internal APIs.
- Strategies must not call exchange adapters directly.
- Frontend/API handlers must not submit exchange orders directly.
- All live orders must pass:
  `Strategy/Operator Intent -> Risk Gateway -> Account Worker -> Executor`.
- Each account worker owns exactly one local trading account or subaccount.
- Account workers must execute independently and concurrently after receiving
  the same signal id.
- Use deterministic signal ids and client order ids for idempotency and restart
  recovery.

## Runtime Rules

- WebSocket streams seed and maintain the realtime cache.
- REST is reserved for startup snapshots, reconnect reconciliation, explicit
  diagnostics, metadata, candles, and bounded fallback.
- Signed state-changing actions prefer Hyperliquid WebSocket post where it is
  officially supported and safe.
- Maintain nonce state per signer. A signer must not be shared by multiple
  live processes without a single nonce owner.
- Use bounded queues and fail closed if a worker falls behind.

## Security Rules

- Vault unlock may warm account-worker signers in memory, but secrets must never
  be written to logs, config, or audit files.
- Workers must drop signer state on shutdown, crash, lock, or kill switch.
- The frontend must show Vault state, worker state, and kill-switch state
  clearly.
- Live actions require risk approval even when the signer is already warm.
- Process-level dry-run is a hard live-action gate. Fast endpoints must not
  bypass it merely because config live gates are enabled.
- Strategy instances must persist their original `dry_run/live` execution mode.
  Background submission, recovery, completion, and auto-loop code must read the
  persisted mode and must never upgrade a dry-run strategy into live execution
  because the current process has live gates enabled.

## Testing Rules

- Add focused unit tests for every risk rule, precision rule, internal API
  contract, and worker state transition.
- Add integration tests for:
  - signal fan-out to multiple workers
  - duplicate signal deduplication
  - worker restart and recovery
  - reconnect after WebSocket interruption
  - live-action fail-closed gates
- Live smoke tests must remain small and must leave no unintended positions or
  stale orders.

## Documentation Rules

- Update `docs/` before or alongside durable architecture, risk, or operating
  procedure changes.
- Keep docs concise enough to guide implementation. Avoid stale wish lists.
- If V2 changes a V1 behavior, document whether it is intentionally replaced,
  preserved, or deferred.

