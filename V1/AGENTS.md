# AGENTS.md

Project guidance for building automated trading software for trade[XYZ] on
Hyperliquid. Last reviewed against official docs on 2026-06-02.

## Product Context

- trade[XYZ] is an interface and HIP-3 DEX named `xyz` on Hyperliquid, not an
  independent matching engine or standalone trading API.
- All programmatic trading goes through Hyperliquid APIs:
  - Mainnet REST: `https://api.hyperliquid.xyz`
  - Testnet REST: `https://api.hyperliquid-testnet.xyz`
  - Mainnet WS: `wss://api.hyperliquid.xyz/ws`
  - Testnet WS: `wss://api.hyperliquid-testnet.xyz/ws`
- XYZ markets are builder-deployed perps. Use `dex: "xyz"` for perp info
  requests and full coin names such as `xyz:XYZ100`, `xyz:TSLA`, or `xyz:NVDA`
  where the API or SDK expects HIP-3-prefixed names.
- XYZ perps are linear, margined and settled in USDC. The oracle is USD
  denominated; there is no USDC/USD conversion in PnL.

## Source Of Truth

- Prefer official docs and SDKs over reverse engineering:
  - trade[XYZ] docs: `https://docs.trade.xyz`
  - Hyperliquid docs: `https://hyperliquid.gitbook.io/hyperliquid-docs`
  - Python SDK: `https://github.com/hyperliquid-dex/hyperliquid-python-sdk`
  - Rust SDK: `https://github.com/hyperliquid-dex/hyperliquid-rust-sdk`
- Re-check docs or live `/info` responses before changing protocol behavior,
  supported markets, fee/risk logic, or SDK assumptions.
- Treat live metadata as dynamic. Do not hardcode universe order, max leverage,
  margin mode, `szDecimals`, open interest caps, or funding parameters unless a
  feature explicitly snapshots them with a timestamp.
- Officially re-checked on 2026-06-02:
  - Hyperliquid `/exchange` order actions support limit and trigger orders with
    `grouping = "na" | "normalTpsl" | "positionTpsl"`.
  - Hyperliquid `/info` works for both perps and spot, but spot mids/open
    orders are only exposed through the first perp dex namespace (empty `dex` in
    `/info` queries).
  - Spot API identifiers and `/info` coin names differ from UI labels: use live
    `spotMeta` / asset-id mapping and account for UI remaps such as
    `BTC/USDC -> UBTC/USDC` on mainnet HyperCore.
  - Hyperliquid address-based action limits and weighted `/info` limits are real
    constraints. `userFills` and some other endpoints add response-size-based
    weight; do not poll them on a hot loop.
  - trade[XYZ] account balances are separated across Perps, Equities [XYZ], and
    Spot; transfers are required between these layers.

## Official Doc Notes

- Keep a clear distinction between:
  - official documentation rules
  - live exchange observations
- If they differ, document both and prefer code paths that fail closed until the
  difference is understood.
- Current live observation to preserve alongside the official docs:
  - trade[XYZ] UI documentation describes a broad Reduce-Only concept in the
    order pane, but mainnet raw Hyperliquid spot signed orders in this project
    have rejected exchange-level `reduce_only` for sell-to-close. Therefore the
    internal API must model spot inventory close separately from exchange
    `reduce_only`.

## Project Documents

- Read `docs/README.md` before implementing new features.
- Product requirements live in `docs/product-requirements.md`.
- Architecture and module boundaries live in `docs/architecture.md`.
- Multi-process account-worker rules live in `docs/process-model.md`.
- Internal event, strategy, risk, and execution contracts live in
  `docs/internal-apis.md`.
- Strategy rules live in `docs/strategy-development.md`.
- Frontend console rules live in `docs/frontend-console.md`.
- Manual operation UI rules live in `docs/manual-operations.md`.
- Risk rules live in `docs/risk-model.md`.
- Runtime, testing, configuration, and security rules live in the remaining
  `docs/` files.
- Update the relevant document before or alongside any change that modifies a
  durable architecture rule, risk rule, strategy behavior, or operating
  procedure.

## Preferred Implementation Path

- Build this project as a pure Rust trading system by default. Python must not be
  part of the live signal, risk, execution, or reconciliation path unless the
  user explicitly changes this decision.
- Python may be used only for optional offline research, notebooks, one-off data
  inspection, or report generation. Any such tooling must consume exported data
  and must not be required to run the live trading bot.
- Use the official Hyperliquid Rust SDK where it is sufficient, especially for
  signing and order actions. Avoid hand-rolled signing; if unavoidable, match the
  official SDK behavior exactly and test against testnet before mainnet use.
- Build one Rust binary with multiple process roles, but run one account-worker
  process per local trading address in live and latency-sensitive dry-run modes.
  A coordinator process may handle market data, leader tracking, frontend, and
  signal broadcast, but it must not submit orders for multiple addresses itself.
- Keep protocol-specific code isolated behind a small adapter layer so strategy
  code does not know raw wire fields such as `a`, `b`, `p`, `s`, `r`, and `t`.

## Rust Architecture Rules

- Use async Rust for I/O-heavy components. Prefer `tokio`, `reqwest`,
  `tokio-tungstenite` or the SDK's websocket client, `serde`, `tracing`, and
  `thiserror`/`anyhow` according to local project style.
- Keep these responsibilities separated:
  - `coordinator`: market/leader signal generation, manual request fan-out,
    worker supervision, and result aggregation.
  - `account_worker`: one process per local trading address; owns that address's
    API wallet, nonce manager, account state, risk checks, and executor.
  - `hyperliquid_client`: REST, WebSocket, signing, request/response types.
  - `xyz_market`: `dex: "xyz"`, symbol normalization, metadata, precision,
    market-session awareness, and trade[XYZ]-specific mechanics.
  - `leader_tracker`: target address/account subscriptions, fill/order parsing,
    deduplication, and smart-money event classification.
  - `copy_engine`: sizing, symbol filters, side mapping, cooldowns, leader-to-user
    position mapping, and copy-trade decisions.
  - `risk`: max notional, max position, leverage limits, slippage guards,
    loss limits, account health, and kill switch.
  - `executor`: nonce management, order placement, cancel/modify flows,
    retries, idempotency, and order reconciliation.
  - `storage`: durable events, signals, orders, positions, fills, and audit logs.
  - `config`: environment selection, secrets loading, watched leaders, symbols,
    sizing rules, and safety limits.
- Prefer strongly typed domain models over passing raw JSON maps through the
  system. Preserve raw payloads in logs/storage when they are useful for audit or
  replay.
- Use bounded channels or explicit backpressure between websocket ingestion,
  signal generation, risk checks, and execution. Trading code must fail closed if
  queues overflow or state falls behind.
- Use deterministic event IDs for leader fills/signals and `cloid` for local
  orders so restarts can deduplicate safely.
- Broadcast the same `signal_id` to all target account workers for synchronized
  multi-address copy execution. Workers execute independently and must not wait
  for slower workers before submitting approved orders.

## API Rules

- Use `/info` for market data, account state, open orders, fills, order status,
  rate-limit state, and metadata.
- Use `/exchange` only for signed state-changing actions such as order, cancel,
  modify, leverage/margin updates, transfers, API wallet approval, and builder
  fee approval.
- Spot-specific `/info` rules:
  - `allMids` spot prices are only included through the first perp dex.
  - `openOrders` / `frontendOpenOrders` for spot are only included through the
    first perp dex.
  - `userFills` returns spot fills with spot-style coin identifiers such as
    `@107`; do not assume UI labels like `HYPE/USDC` will come back from the
    raw fill feed.
- TP/SL grouping rules:
  - `positionTpsl` is the perp-style grouping tied to a position object.
  - `normalTpsl` is the spot-compatible/native generic TP/SL grouping.
  - Do not reuse perp TP/SL grouping for spot trigger batches.
- For XYZ perp metadata and contexts, request:
  - `{"type": "perpDexs"}` to discover builder DEXs.
  - `{"type": "meta", "dex": "xyz"}` for universe and `szDecimals`.
  - `{"type": "metaAndAssetCtxs", "dex": "xyz"}` for mark, mid, oracle,
    funding, open interest, and volume context.
  - `{"type": "clearinghouseState", "user": <account>, "dex": "xyz"}` for XYZ
    perp positions and margin state.
- For candles on HIP-3 markets, prefix the coin in the request, e.g.
  `{"type": "candleSnapshot", "req": {"coin": "xyz:XYZ100", ...}}`.
- WebSocket clients must reconnect gracefully. Snapshot acks may contain data
  already processed; make consumers idempotent.
- Runtime read models must be WebSocket-first. Dashboard refreshes, strategy
  monitors, Fib fill detection, open-order views, account state cards, and
  quote widgets must read the local realtime cache first. REST `/info` is
  reserved for startup/reconnect snapshot seeding, explicit reconciliation,
  cache-miss fallback, signed-action preflight, post-submit confirmation,
  metadata/candle snapshots, and user-requested diagnostics.
- WebSocket streams that can be idle must send heartbeat pings before the
  official 60-second idle timeout. On reconnect, seed the realtime cache once
  from REST and then resume streaming updates.
- Use `cloid` for strategy-owned orders whenever practical so order recovery and
  deduplication do not depend only on server-generated order IDs.
- For account queries, always pass the actual master/subaccount address, not the
  API wallet/agent address.

## Asset And Precision Rules

- Never assume plain symbols like `TSLA` are enough for XYZ trading. Normalize
  user-facing symbols to canonical API names, usually `xyz:<SYMBOL>`.
- Builder-deployed perp asset IDs are derived from DEX index and meta index:
  `100000 + perp_dex_index * 10000 + index_in_meta`. Prefer SDK mapping instead
  of manually computing it.
- Spot asset IDs are `10000 + spot index` from `spotMeta`. Spot IDs are not the
  same thing as token IDs, and mainnet/testnet spot IDs differ.
- Prices and sizes must be rounded using live `szDecimals`.
- Perp prices allow up to 5 significant figures and no more than
  `6 - szDecimals` decimal places. Spot differs; do not reuse perp rounding for
  spot.
- Spot lot size is deployer-defined per asset. Hide or block spot operations
  when the post-rounding size would become zero, even if the mark value looks
  nontrivial in USD terms.
- Order validation should happen before signing:
  - canonical coin exists in live metadata
  - side, size, reduce-only flag, and TIF are coherent
  - price and size pass precision checks
  - notional and leverage constraints are satisfied
  - opening order notional is at least 10 USD until a dynamic exchange-provided
    minimum is wired in; this reflects a 2026-05-31 mainnet action-level error
    for smaller orders
  - spot orders may also hit quote-token minimums (`MinTradeSpotNtl`) and
    insufficient-balance errors; do not assume perp minimum-order behavior is a
    complete model for spot
  - non-reduce-only TP/SL batches can be rejected at pre-validation time for
    the whole payload, so batch callers must handle single-payload failures

## Accounts, Wallets, And Secrets

- There are separate balances for Hyperliquid-native perps, Equities [XYZ], and
  Spot. Collateral in one account cannot be assumed available in another.
- Use transfer actions deliberately when moving USDC between spot, default perps,
  and `xyz` perps. Log the source and destination account/Dex explicitly.
- Treat USDC movement between default perps and `xyz` perps as a signed live
  action. It must pass dry-run/live gates, manual live gates, mainnet explicit
  confirmation, Vault availability, amount caps, and sanitized audit logging
  before any private key is loaded.
- Prefer API wallets/agent wallets for automated trading. They sign on behalf of
  the master account but account queries must use the actual master/subaccount
  address, not the agent address.
- Never commit private keys, seed phrases, API wallet secrets, or real account
  addresses unless the user explicitly provides a public address for examples.
- Load secrets from environment variables or an ignored local config file.
- Core signed submit/cancel paths must reject example or placeholder account
  addresses before loading API wallet secrets or constructing live actions.
- Default new trading flows to testnet or dry-run. Require an explicit user
  instruction before enabling mainnet order submission.

## Nonce, Rate Limit, And Reliability Rules

- Nonces are tracked per signer. One API wallet shared by multiple processes can
  collide; prefer one API wallet per trading process or subaccount.
- A live account-worker process must control exactly one local trading address or
  subaccount. Do not make one worker serially submit orders for multiple local
  addresses.
- Use a monotonic atomic nonce source for signed actions. It may fast-forward to
  current Unix milliseconds but must never reuse a nonce for the same signer.
- Batch order and cancel actions when appropriate, and separate ALO-only batches
  from IOC/GTC batches when doing market-making style flows.
- Query `userRateLimit` and implement local throttling/backoff. Do not rely on
  unlimited polling; prefer WebSocket streams plus REST reconciliation.
- Respect official IP-weight limits:
  - REST aggregate weight is 1200/minute
  - `allMids`, `clearinghouseState`, `spotClearinghouseState`, `orderStatus`,
    and `exchangeStatus` have lower weight than generic `info`, but `userFills`
    and similar endpoints add weight based on response size
  - maximum 10 websocket connections, 1000 subscriptions, and 2000 WS messages
    per minute
- Use exponential backoff with jitter and local cooldown windows after 429 or
  transport retry bursts. Prefer cached snapshots for UI refresh loops.
- Treat API responses with HTTP 200 but action-level errors as failures.
  Persist enough context to safely retry, cancel, or reconcile.
- Use `scheduleCancel` / dead-man-switch behavior for unattended live strategies
  when the strategy design allows it.
- Fib Basic currently has a dry-run verified internal execution chain
  (`CoordinatorSignal -> RiskGateway -> AccountExecutor`) and official candle
  websocket probe support. It records entry order refs and has a refresh
  cancel-by-cloid path for previous non-dry-run refs. Do not call it a fully
  accepted unattended live bot until persistent websocket runtime state, live
  resting entry cancel/replace validation, background fill reconciliation, and
  post-fill native TP/SL live smoke tests are all implemented and documented.
- Fib Basic strategy cancellation means stopping auto-loop and cancelling
  unfilled entry orders by `cloid`. Do not cancel existing exchange-native
  TP/SL protection for an already-open position unless the user explicitly asks
  to remove protection or close the position. Auto-loop may only re-arm after
  the previous cycle is flat, protective orders are no longer open, and the
  configured cooldown has elapsed.
- Fib Basic stop/cancel requests must be treated as a hard control-plane gate.
  Background reconciliation may run from an older `Completed + auto_loop=true`
  snapshot; before submitting entry signals, after submitting them, and before
  writing a new instance state, it must re-check whether the strategy has been
  stopped. A stale background snapshot must never overwrite `Killed` /
  `auto_loop=false` back into an active auto-loop state.
- Fib Basic `armed_unfilled` is an active waiting state, not a completed or
  stopped state. The reconciliation loop must keep its plan current from the
  WebSocket-first realtime cache and submit entries only through the Fib
  `CoordinatorSignal -> RiskGateway -> AccountExecutor` path once the entry
  condition is valid. For maker entries, if price has already moved below the
  planned buy limit, do not submit an order that would cross or be rejected by
  ALO; keep waiting for recovery above the entry or for an explicit parameter
  refresh/cancel. If `auto_loop=true` and `locked_range=false`, an unfilled Fib
  strategy must not wait forever on a stale swing; when no valid entry signal is
  available, move into cooldown and re-infer the Fib range from fresh candles
  before the next entry attempt.
- Fib Basic "fill first" entry mode is the default for user-facing basic
  strategies. It maps to taker/IOC and must not pass the Fib level as a fixed
  limit price; the executor should derive a current-reference-price limit using
  `max_slippage_bps` so the order prioritizes filling while still retaining a
  slippage guard. "Post-only first" is the explicit maker/ALO mode and is
  allowed to miss or be rejected if the order would immediately match.
- Fib Basic must keep an append-only lifecycle history in
  `logs/fib_instance_history.jsonl` in addition to the current recoverable
  instance table at `logs/fib_instances.json`. The current table may be
  overwritten by latest state, but lifecycle history must record starts,
  refreshes, entry/protection reconciliation, cycle completion, auto-loop waits,
  cancels, and errors. Audit-only historical Fib events may be shown as
  recovered history for explanation, but must never be treated as live strategy
  instances or used to submit orders automatically.
- Fib Basic stop-loss exits have distinct post-exit behavior. When the
  exchange-native stop-loss protective order is identified as the exit fill,
  `stop_loss_stop_strategy=true` must mark the strategy `Killed` and set
  `auto_loop=false`; otherwise the next auto-loop may only re-arm after
  `stop_loss_cooldown_secs`. Take-profit exits should re-arm immediately and
  let the next Fib entry condition decide whether a new order is placed.
  Unknown exit reasons and no-accepted-entry retry states use the normal
  `cooldown_secs` fail-safe.
- Fib Basic multi-account cycles must be treated as synchronized batches. For
  every generated entry signal, each configured target account must produce a
  successful submitted report before the cycle can be treated as healthy. If
  only part of the target account set submits successfully, cancel any still
  resting partial entry orders, preserve and protect any already-filled
  position, set `auto_loop=false`, and surface the missing account/signal pairs
  in the frontend and lifecycle history. A single account's entry or TP/SL
  success must never mark a multi-account strategy as globally `Protected`.
- Fib Basic strategy instances must persist their original `dry_run/live`
  execution mode. Background armed-entry submission, recovery, completion, and
  auto-loop restart must read that persisted mode and must never upgrade a
  dry-run instance into live execution because the console process is currently
  running with live gates enabled.
- Before any live Fib Basic perp entry, and before any live manual perp opening
  order, force-read the current same-market/same-coin position for the account.
  If any nonzero residual perp position exists, submit an internal-API
  `reduce_only + close_full_position + IOC` cleanup order and confirm the
  position is strictly flat before opening the new position. After live manual
  perp closes and after Fib Basic exchange-native TP/SL exits, run the same
  strict residual cleanup before treating the account or cycle as flat. Do not
  apply this rule to spot dust because spot lot-size constraints can make tiny
  balances impossible to sell through the order book.
- Dashboard `Current Orders / Strategy Orders` is an explicit order-management
  surface, not just a read-only strategy view. Its market-scoped
  cancel/stop action must re-fetch exchange `openOrders` on the backend but may
  only cancel orders that this V1 runtime can prove it owns through local order
  refs, deterministic cloids, or an equivalent durable ownership ledger. Orders
  that are merely visible on the same exchange account, including V2/other-tool
  orders and manual maker orders without V1 ownership evidence, must be shown
  as unowned/manual and left untouched. Existing exchange-native TP/SL
  protection for open positions must not be cancelled by this bulk action unless
  the user explicitly requests removing protection or closing the position.
  Linked Fib strategies may be stopped with `auto_loop=false` after their
  owned entry orders are cancelled or preserved for retry.

## XYZ Market Risk Rules

- XYZ assets have external and internal pricing sessions. Strategies must know
  whether they are trading during live external pricing, internal pricing,
  weekend/off-hours, holiday closures, or futures roll periods.
- The relayer publishes oracle, mark, and external price updates. The mark price
  is used for margining, liquidations, triggers, and unrealized PnL.
- Funding is hourly and uses oracle price for notional conversion. Include
  funding estimates in carry-sensitive strategies.
- Discovery bounds can constrain mark price during internal pricing sessions.
  Risk controls must account for bounds, possible re-anchoring, and reopening
  gaps.
- For commodities with futures-based feeds, account for roll schedules. Do not
  interpret roll-driven oracle behavior as normal spot movement without context.
- For equities, account for pre-market, regular, post-market, overnight sessions,
  weekends, and exchange holidays.

## Safety Defaults

- Every order path must support:
  - dry-run mode
  - max order notional
  - max position notional
  - max leverage
  - max slippage or limit price guard
  - reduce-only close path
  - global kill switch
  - structured audit logs
- Before live trading, run at least:
  - metadata fetch
  - symbol normalization test
  - rounding/precision tests
  - dry-run order construction
  - account `clearinghouseState` check for collateral and reduce-only position
    direction
  - testnet order/cancel smoke test when credentials are available
  - reconnect/reconciliation test for market data streams
- Never silently continue after losing account state, websocket state, nonce
  state, or risk-limit state. Fail closed or pause trading.

## Documentation For Future Agents

- When adding or changing trading behavior, update this file if the new behavior
  creates a durable rule for future work.
- Frontend labels must match the actual exchange action. Do not label a button as
  live submit when it only creates a strategy instance or internal signal; do not
  present deprecated local TP/SL monitoring as an active trading feature.
- Document any assumptions that come from live API observation separately from
  assumptions that come from official docs.
- Keep strategy logic, exchange adapter logic, and risk controls separated so
  the Rust system can evolve without entangling protocol code, strategy code,
  and live execution safety.
