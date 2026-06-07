# V2 Internal APIs

## Intent Types

- `ManualTradeIntent`: operator-selected market, account set, side/action,
  size, leverage, TP/SL plan, and execution preference.
- `StrategyTradeIntent`: strategy id, cycle id, level id, target accounts,
  desired entry/exit action, and risk context.
- `CopyTradeIntent`: leader event id, leader address, market, coin, side,
  sizing rule, dedupe key, and target accounts.
- `CancelIntent`: scope, market, coin, strategy id, account ids, and reason.
- `TransferIntent`: source layer, destination layer, amount, account id, and
  approval context.

## Risk Gateway Contract

Input:

- intent
- latest market snapshot
- latest account snapshot
- module risk config
- global risk config

Output:

- `ApprovedSignal` with deterministic `signal_id`
- or `RiskRejection` with blocker details

The risk gateway must not sign or submit orders.

## Worker Command Contract

Account workers receive only approved commands:

- `SubmitOrder`
- `CancelOrder`
- `ArmNativeTpsl`
- `ClosePosition`
- `Transfer`
- `RefreshState`
- `LockSigner`
- `Shutdown`

Every command includes:

- `signal_id`
- `account_id`
- `market`
- `coin`
- `idempotency_key`
- `created_at_ms`
- `risk_approval_id`

Current V2 implementation note:

- `src/v2_runtime.rs` currently models `SubmitOrder`, `CancelOrder`,
  `ArmNativeTpsl`, `ClosePosition`, and control commands.
- `Transfer` remains part of the contract but is deferred until the worker
  execution adapter is connected to real signed exchange actions.
- Commands enter async workers through bounded channels. If the channel is full,
  the worker returns a fail-closed `QueueFull` rejection instead of buffering
  unbounded live trading work.

## Worker Result Contract

Worker results include:

- command id
- accepted/rejected status
- exchange response summary
- cloid/oid refs when available
- fill summary when immediately known
- latency timings
- retry count
- final worker state hash

Results are sent to coordinator and storage. They are not parsed from frontend
text.

The current mock exchange adapter returns a `V2ExchangeSubmission` for accepted
order/cancel/TP-SL/close commands. Real adapters must preserve the same result
shape and idempotency semantics.

