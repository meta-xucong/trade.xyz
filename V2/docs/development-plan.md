# V2 Development Plan

## Goal

Build a lower-latency V2 system where strategies and copy trading submit orders
through warm per-account workers instead of frontend HTTP handlers.

## Non-Goals For The First V2 Pass

- Do not redesign Fib strategy math.
- Do not add AI strategy logic.
- Do not change exchange semantics without official-doc verification.
- Do not remove V1.

## Phase 1: Skeleton

- Create a V2 Rust workspace.
- Define typed domain models, config loading, risk interfaces, and worker
  message contracts.
- Add a minimal coordinator and account-worker runtime without live submit.
- Add mock exchange adapters for deterministic tests.

Initial status:

- V2 now starts from a full copied V1 baseline.
- The first dedicated V2 module is `src/v2_runtime.rs`.
- The initial worker runtime is intentionally side-effect free: it validates
  typed commands, signer warm/cold state, idempotency, queue bounds, and
  multi-account fan-out without submitting exchange actions.

## Phase 2: Realtime State

- Implement WebSocket-first market and account state caches.
- Add startup REST snapshot seeding and reconnect reconciliation.
- Add local rate-limit and backoff policies.
- Prove cache correctness with unit and integration tests.

## Phase 3: Worker Fast Path

- Load Vault once, then warm signers inside the owning worker.
- Keep nonce state inside the worker.
- Submit signed exchange actions through the official SDK and WebSocket post.
- Add idempotent order submit, cancel, and native TP/SL flows.

## Phase 4: Strategy Integration

- Connect manual operator intents, Fib Basic, and copy-trading signals to the
  internal signal bus.
- Ensure every live action passes risk before entering a worker.
- Use the same signal id across all target workers for multi-account sync.

## Phase 5: Frontend

- Keep frontend controls for configuration, status, and emergency actions.
- Show worker health, Vault state, kill switch, open orders, strategy state,
  and recent trading events.
- Do not let frontend HTTP handlers directly submit live exchange orders.

## Phase 6: Acceptance

- Run dry-run integration tests.
- Run testnet smoke where possible.
- Run small mainnet smoke only after explicit approval.
- Verify every live smoke leaves no unintended positions or stale orders.

