# V2 Architecture

## High-Level Flow

```text
Frontend / CLI / Strategy
        |
        v
Coordinator
        |
        v
Risk Gateway
        |
        v
Internal Signal Bus
        |
        v
Per-Account Worker(s)
        |
        v
Exchange Adapter / Hyperliquid WebSocket Post
```

## Modules

- `config`: versioned V2 configuration, market selection, account registry,
  module-specific risk settings, and environment gates.
- `secrets`: Vault unlock, signer hydration, memory-only secret handling, and
  lock/drop behavior.
- `market_data`: WebSocket-first prices, candles, metadata, funding, and
  exchange session state.
- `account_state`: per-account positions, balances, open orders, fills, and
  protective orders.
- `coordinator`: strategy scheduling, manual intent intake, copy-trading intake,
  worker supervision, and result aggregation.
- `risk`: global risk, module risk, strategy risk, per-account limits, kill
  switch, and pre-trade validation.
- `signal_bus`: bounded typed channels from approved intents to target workers.
- `account_worker`: one process or task group per local account; owns signer,
  nonce, current account state, and execution.
- `executor`: order submit, cancel, modify, native TP/SL, transfer, retry, and
  reconciliation.
- `frontend`: read-only status, configuration forms, and operator actions. It
  cannot bypass coordinator/risk/worker.
- `storage`: append-only audit logs, recoverable current state, event replay,
  and test fixtures.

## Critical Invariants

- One account worker owns one local trading account.
- One signer has one nonce owner.
- Every live action has a signal id, risk approval, audit event, and worker
  result.
- Strategies emit intent, not exchange actions.
- Exchange adapters do not know strategy rules.
- Frontend never submits live orders directly.

