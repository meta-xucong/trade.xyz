# V2 Security And Operations

## Secret Handling

- Keep secrets out of Git.
- Keep Vault files out of Git.
- Do not log private keys, seed phrases, raw signer bytes, or passwords.
- Warm signers only after explicit Vault unlock.
- Provide a clear lock action that drops all warm signer state.

## Live Safety

- Keep global kill switch above every module.
- Keep module-specific risk rules for manual, Fib, and copy trading.
- Do not implement one global risk bucket that makes strategy behavior unclear.
- Opening orders must pass notional, leverage, collateral, slippage, precision,
  and symbol checks before signing.
- Closing and cleanup paths must be reduce-only for perps.

## Process Supervision

- Coordinator supervises account workers.
- Worker crash must be visible in frontend.
- A crashed worker must not be silently restarted into live trading without
  state recovery and risk gates.
- Windows sleep/lock/network interruption must be treated as a first-class
  operational risk.

## Operator Surfaces

Frontend must show:

- Vault locked/unlocked
- worker online/offline
- signer warm/cold
- WebSocket connected/reconnecting
- kill switch state
- active strategies
- open orders
- current positions
- recent trading events

## Audit

Every live attempt records:

- intent
- risk result
- worker command
- exchange response
- latency timings
- final reconciliation state

Audit records must be sanitized.

