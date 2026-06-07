# V2 Execution Worker Model

## Why Workers

V1 can submit through fast WebSocket post, but live manual flows still enter
through frontend HTTP handlers. V2 removes that from automated trading:

- strategies emit internal signals
- coordinator routes approved work
- workers submit directly with warm signer and nonce state

## Worker Ownership

Each worker owns:

- one account id
- one master/subaccount address
- one API wallet signer after unlock
- one nonce source
- one account-state subscription set
- one execution queue

Workers must not submit for other accounts.

## Signer Lifecycle

1. Vault is unlocked by operator.
2. Coordinator asks each worker to hydrate its signer.
3. Worker stores signer in memory only.
4. Worker drops signer on lock, crash, kill switch, or shutdown.
5. Live commands are rejected while signer is unavailable.

## Queueing

- Use bounded queues.
- Reject or pause when queues are full.
- Do not allow unbounded signal buildup during WebSocket disconnects.
- Preserve order within one account worker, but allow different workers to
  execute concurrently.

Implementation status:

- `V2AsyncAccountWorker` uses a bounded `tokio::mpsc` queue.
- `try_send` failures are surfaced as explicit worker rejections instead of
  hidden retries.
- `V2AsyncWorkerCoordinator` dispatches target-account commands concurrently
  while preserving the same `signal_id` across accounts.
- Tests use a delayed mock exchange adapter to prove account workers do not
  wait for each other before submitting accepted commands.

## Latency Measurements

Every live command records:

- coordinator receive time
- risk approval time
- worker receive time
- signing start/end
- exchange submit start/end
- exchange acknowledgement time
- reconciliation completion time when applicable

Report both critical submit latency and full user-visible latency.

## Recovery

On restart:

- load current recoverable state
- seed account and open-order snapshots from REST
- resume WebSocket streams
- reconcile in-flight command ids
- fail closed on ambiguous state

