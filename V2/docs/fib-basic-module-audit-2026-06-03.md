# Fib Basic Module Audit - 2026-06-03

This audit focuses only on the Fib Basic strategy. Smart-money copy is out of
scope.

## Official Baseline

- Candles must be read from Hyperliquid `/info` with `type = "candleSnapshot"`.
- HIP-3 perp candles must use canonical prefixed coins such as `xyz:NVDA`.
- Default perp candles use default perp names such as `BTC`.
- Spot candle identifiers differ from UI labels. `PURR/USDC` is special; most
  spot pairs must use `@{spot_index}` from live `spotMeta`.
- Opening orders must be signed `/exchange` order actions and must meet exchange
  precision and minimum notional constraints.
- TP/SL must be exchange-native trigger orders through `/exchange`, with
  `positionTpsl` for perp position TP/SL and `normalTpsl` for spot/generic TP/SL.
- Realtime strategy operation should use official websocket market/order/user
  streams where latency matters.

Official references:

- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/asset-ids
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/tick-and-lot-size
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket

## Checked Paths

| Area | Current Status | Result |
| --- | --- | --- |
| Fib market/account selection | Implemented through Fib API payloads and module-scoped config. | Pass |
| Fib blacklist | Uses `module_symbol_policies.fib_blocked_symbols`, independent from manual/copy. | Pass |
| K-line data | Uses `/info` `candleSnapshot`; fixed spot pair mapping to official `@spot_index` where required. | Pass |
| Reference price | Uses cached `allMids` first for perp markets and falls back to live market snapshots; spot uses spot snapshot context. | Pass |
| Swing detection | Finds a valid long swing where low occurs before high. | Pass for basic deterministic mode |
| Fib level calculation | Uses `high - (high - low) * level`; supports 0.382/0.618 and other configured levels. | Pass |
| Profit/loss calculation | Supports price delta and principal percent; percent mode converts by leverage for perps and leverage=1 for spot. | Pass |
| Per-level notional | Fixed to reject generated per-level opening orders below the exchange minimum and below the precision-rounded effective minimum. | Pass |
| Entry signal generation | Maker generates one signal per selected level; taker generates only when current price is inside the entry zone. | Pass as signal generation |
| Basic order module | Fib start/refresh converts plans to `CoordinatorSignal`, passes through `RiskGateway`, then through account-worker execution. | Pass for dry-run and same-request live path |
| Fib entry price propagation | `SignalOrder.limit_price` carries the Fib entry price. Maker maps to maker-only, taker maps to limit intent. | Pass |
| Automatic entry order submit | `fib_instance_start` and unfilled `refresh-params` submit strategy-owned entry intents through internal APIs; no direct manual endpoint call. | Pass for dry-run; live path is wired but not submitted in this audit |
| Parameter refresh | Recalculates plan, records entry order refs, and resubmits updated entry intents when still unfilled. Live refresh now has a cancel-by-cloid path for previous non-dry-run entry refs before submitting replacements. | Partial: dry-run verified; live cancellation not state-changing tested in this audit |
| Fill reconciliation | Same-request live fills from the executor can be used immediately; no background Fib fill/open-order reconciliation loop yet. | Partial |
| Automatic TP/SL after fill | If the entry executor reports a real fill with average fill price and size, Fib calls the existing exchange-native protective arm path using the actual fill price. | Partial: not exercised with a live state-changing order in this audit |
| Official websocket price feed | Added official candle websocket probe endpoint and verified `hl_perp / BTC / 1m`; runtime reference pricing still uses cached REST mids/snapshots. | Partial |

## Live Non-State-Changing Smoke

All checks were run against the local frontend process on `127.0.0.1:8790`
without submitting orders or transfers.

- `xyz_perp / xyz:NVDA`
  - Fib auto-detect: OK, 120 candles, 2 levels.
  - Fib start with 11 USD per selected level: OK, status `entry_pending`, 2
    maker entry signals.
  - Fib source-module readiness works; after frontend restart it was blocked
    only by Vault lock / missing unlocked API wallet secret.
  - 2026-06-03 final smoke after restart and Vault unlock: quote OK; Fib start
    dry-run OK with 1 maker entry signal and 2 account-worker submitted reports.
- `hl_perp / BTC`
  - Fib auto-detect: OK, 120 candles, 2 levels.
  - Fib start with 11 USD per selected level: OK, status `entry_pending`, 2
    maker entry signals.
  - A 10 USD per-level request was correctly rejected because size rounding made
    the effective order value 9.568420 USD with `szDecimals=5`.
  - Fib source-module readiness works; after frontend restart it was blocked
    only by Vault lock / missing unlocked API wallet secret.
  - 2026-06-03 final smoke: quote OK; official websocket candle probe OK for
    `BTC / 1m`; Fib start dry-run OK with 2 account-worker submitted reports.
- `spot / HYPE/USDC`
  - Before fix: candle fetch failed because `HYPE/USDC` was sent directly to
    `candleSnapshot`.
  - After fix: Fib auto-detect OK, 120 candles, 2 levels.
  - Fib start OK with 22 USD principal and two levels, producing 11 USD per
    level.
  - Fib start correctly rejects 11 USD principal with two levels because 5.5 USD
    per level is below the exchange minimum.
  - Fib source-module readiness works; after frontend restart it was blocked
    only by Vault lock / missing unlocked API wallet secret.
  - 2026-06-03 final smoke: quote OK; Fib start dry-run OK with 2
    account-worker submitted reports.

## Code Changes Made

- Added `SpotMarketSnapshot::candle_coin` to convert spot UI labels to official
  candle identifiers.
- Routed Fib spot candle requests through that mapping.
- Added backend per-level opening notional validation.
- Extended backend validation to account for `szDecimals` size rounding and
  precision-rounded effective order value.
- Added frontend validation so users see the same rule before submitting.
- Added tests for spot candle mapping and Fib per-level notional validation.
- Added `SignalOrder.market`, `SignalOrder.dex`, `SignalOrder.limit_price`, and
  `SignalOrder.apply_account_ratio` so strategy signals keep their market and
  price semantics when converted into worker trade intents.
- Scoped Fib execution through `RiskGateway` and `AccountExecutor`; copy-trade
  sizing still applies account ratios, while Fib/manual fixed USD sizing does
  not.
- Added cached `allMids` reads for high-frequency quote/reference-price paths to
  reduce official `/info` weight and 429 risk.
- Added `/api/fib/ws-candle-probe` to verify official Hyperliquid candle
  websocket connectivity and spot/perp candle symbol mapping.
- Boxed `CoordinatorMessage::Signal` for IPC so the enlarged typed signal does
  not bloat the coordinator message enum.

## Acceptance Judgment

Fib Basic is now valid as a dry-run automatic entry-intent module across
`xyz_perp`, `hl_perp`, and `spot`: it can compute levels, generate price-aware
entry signals, pass risk, and fan out dry-run worker submissions to multiple
accounts through internal APIs.

It is still not fully accepted as an unattended live bot. The remaining gaps are:

1. A persistent official websocket runtime for Fib prices, order updates, and
   user fills rather than a probe plus cached REST fallback.
2. Live resting order cancel/replace on parameter refresh must be state-changing
   tested in an explicitly authorized live smoke window.
3. Background fill/open-order reconciliation for maker orders filled after the
   original start request returns.
4. Live verification of the post-fill exchange-native TP/SL arm path on a
   deliberately authorized mainnet smoke window.

Until those are completed and live-tested, the UI may describe Fib Basic as
"dry-run verified / live path wired", but not yet as a fully accepted unattended
live dip-buy bot.
