# Official Compliance Audit - 2026-06-03

This audit checks the current Rust trading console against the official
trade[XYZ] and Hyperliquid documentation and the local project rule that modules
must communicate only through internal APIs.

## Official References

- trade[XYZ] account types: https://docs.trade.xyz/getting-started/account-types
- Hyperliquid exchange endpoint: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint
- Hyperliquid info endpoint: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint
- Hyperliquid asset IDs: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/asset-ids
- Hyperliquid tick and lot size: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/tick-and-lot-size
- Hyperliquid websocket docs: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket
- Hyperliquid rate limits: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits

## Official Baseline

- Read-only data may use `/info`, but runtime state should be websocket-first
  where official subscriptions exist: mids, open orders, fills, clearinghouse
  state, and spot state.
- Signed state-changing actions must use `/exchange`: order, cancel,
  cancel-by-cloid, modify, leverage updates, trigger TP/SL, and transfers.
- Order actions support limit and trigger order types. TP/SL grouping must be
  one of `na`, `normalTpsl`, or `positionTpsl`.
- Builder-deployed HIP-3 perps use `{dex}:{coin}` names and builder asset IDs.
  Spot asset IDs use `10000 + spotMeta index`.
- Prices and sizes must be rounded from live metadata. Perp and spot precision
  are different.
- Spot mids and spot open orders are queried through the first perp namespace,
  so spot `/info` requests must not pass `dex = "spot"` blindly.
- trade[XYZ] separates balances between Perps, Equities [XYZ], and Spot. The
  system must not assume funds are shared across these layers.
- REST and websocket rate limits are real constraints. Hot UI/strategy loops
  must use websocket caches first, throttle fallback REST, and back off after
  429 or burst failures.

## Module Results

| Module | Status | Evidence | Notes |
| --- | --- | --- | --- |
| Hyperliquid adapter | Pass | Uses `/info` for `perpDexs`, `meta`, `metaAndAssetCtxs`, `spotMetaAndAssetCtxs`, `orderStatus`, `userRateLimit`, `candleSnapshot`, startup/reconnect snapshots, explicit reconciliation, and signed-action confirmation. Uses official websocket subscriptions for realtime `allMids`, `openOrders`, `orderUpdates`, `userEvents`, `userFills`, `allDexsClearinghouseState`, and `spotState`. Uses `/exchange` through the SDK or signed helper for actions. | 429 backoff, cache, stale fallback, Retry-After handling, WS reconnect, and heartbeat pings are present. |
| Market and symbol normalization | Pass | XYZ symbols normalize to `xyz:<SYMBOL>`. HL perps use default perp names. Spot uses live `spotMetaAndAssetCtxs` labels and asset IDs. Later V2 work added `cash_perp` for the HIP-3 `cash` perp DEX, distinct from trade[XYZ] `xyz_perp`. | Verified live universe/quote reads for `xyz_perp`, `hl_perp`, and `spot` in this audit; `cash_perp` is covered by later market-capability acceptance. |
| Dashboard/account overview | Pass | `/api/state` and funding diagnostics read account state from the right account layer. Recent events are business-filtered and market-tagged. | The current state endpoint defaults to the configured market for summary, while the UI uses market selectors and funding/protective endpoints for market-specific reads. |
| Manual trading | Pass for plan/readiness; live submit not executed in this audit | Mainnet readiness passed for 2 accounts on XYZ, HL perp, and Spot without submitting orders. Manual live path preflights before signed submit, applies perp leverage before order, and auto-arms TP/SL only after a live order succeeds. | No state-changing order was sent during this audit. |
| Native TP/SL | Pass with legacy naming debt | Current arm path submits exchange-native trigger orders. Perps use `positionTpsl`; spot uses `normalTpsl`. Position TP/SL view is reconstructed from open orders/orderStatus. | Historical local-monitor audit labels and the serialized `local_trigger` field remain for compatibility; they should be renamed/deprecated in a future cleanup to avoid confusion. |
| Spot close handling | Pass | Spot sell-to-close is modeled as inventory close rather than exchange-level reduce-only, matching live rejection observations while still checking inventory. | This is a live-observation compatibility rule preserved in `AGENTS.md`. |
| USDC transfers | Pass for plan/readiness; live transfer not executed in this audit | Transfer design uses official `sendAsset` semantics for transfers between spot, default perp, named perp DEXs, users, and subaccounts. | No transfer submit was sent during this audit. |
| Vault and secrets | Pass | Vault exists, was unlocked in the current frontend process, and readiness confirmed both API wallet secrets are available. Tests cover encryption, password rotation, and vault-only accounts. | Do not log private keys or signatures. |
| Multi-account worker model | Partial | The coordinator can spawn one worker process per enabled account and the dry-run fan-out child-process test passed with 2 workers. | The manual frontend currently fan-outs through the frontend service, not through long-running live worker processes. This is acceptable for manual UI but not enough for lowest-latency smart-money following. |
| Fib basic strategy | Partial, improved | Fib auto-detect and instance start work through `/api/fib/*`, compute levels from candles, generate price-aware strategy signals, record entry order refs, and submit dry-run account-worker reports through `RiskGateway -> AccountExecutor` without directly calling manual endpoints. | Live path is wired for same-request fills, native TP/SL arm, and refresh cancel-by-cloid for stored refs, but live state-changing tests and background fill reconciliation are not fully accepted yet. |
| Fib AI advanced | Framework only | Proposal endpoint exists as observe/suggest scaffolding. | Not live-trading-ready by design. |
| Smart-money copy | Framework only | Preview/classification emits capped/deduped copy signals from a synthetic leader fill event. | No production leader listener, userEvents/orderUpdates websocket stream, or one-process-per-account low-latency execution path is implemented yet. |
| Rate-limit and reliability | Pass for UI/strategy read paths; partial for full worker runtime | REST client has retry/backoff and caches high-weight metadata. The frontend/coordinator read model keeps persistent websocket caches for mids, open orders, fills, clearinghouse states, and spot state. | Full one-process-per-account production worker runtime still needs separate acceptance, but UI refresh and Fib reconciliation no longer depend on hot REST polling. |

## Live API Smoke Results

All calls below were non-state-changing.

- Vault status: unlocked in the frontend process; 2 configured entries.
- Market capabilities in this audit: `xyz_perp`, `hl_perp`, and `spot` are exposed and marked
  live-trading capable.
- Market universe and quote:
  - `xyz_perp / xyz:NVDA`: OK.
  - `hl_perp / BTC`: OK.
  - `spot / HYPE/USDC`: OK, with spot-only fields such as no funding or
    leverage.
- Signed readiness, no submit:
  - `xyz_perp / xyz:NVDA / 11 USD`: 2 accounts ready for mainnet signed submit.
  - `hl_perp / BTC / 11 USD`: 2 accounts ready for mainnet signed submit.
  - `spot / HYPE/USDC / 11 USD`: 2 accounts ready for mainnet signed submit.
- Funding batch: 2 accounts read successfully; funding readiness and order
  readiness returned expected account-layer diagnostics.
- Fib:
  - Auto-detect for `xyz:NVDA / 1h / 120 candles`: OK.
  - Basic instance start for `xyz:NVDA`: OK. Final dry-run smoke after restart
    produced status `entry_pending`, 1 maker entry signal, and 2 submitted
    account-worker reports.
  - Official websocket candle probe for `hl_perp / BTC / 1m`: OK.
- Smart-money preview:
  - Synthetic leader fill for `xyz:NVDA`: OK, 1 capped copy signal generated.
- Protective rules query:
  - `xyz_perp`, `hl_perp`, `spot`: OK, no current open native TP/SL rules.

## Local Test Results

- `cargo fmt -- --check`: passed.
- Frontend inline JavaScript syntax check: passed.
- `cargo check`: passed.
- `cargo test -- --nocapture`: passed, 88 tests.
- `cargo clippy --all-targets --all-features -- -D warnings`: passed.

## Gaps To Close Before Full Strategy Acceptance

1. Smart-money copy is not production-ready until it has official websocket
   subscriptions for leader fills/order updates, restart-safe dedupe, and
   account-worker live execution per local address.
2. Fib basic is dry-run verified through the internal execution chain and now
   reads open orders/fills/account state through the persistent websocket cache
   before REST fallback, but it is not yet a fully accepted unattended live bot.
   It still needs long-horizon live continuity acceptance through an actual
   entry fill, native TP/SL close, and auto-loop re-arm.
3. The active frontend has a legacy `/api/fib/preview` calculator that does not
   support spot. It is not used by the current Fib UI, but should either be
   removed or marked explicitly deprecated.
4. The `local_trigger` field in protective plan DTOs is misleading now that the
   preferred path is exchange-native trigger orders. Rename or deprecate it in a
   compatibility-safe cleanup.
5. Full account-worker productionization still needs separate acceptance for
   one-process-per-account deployment, but the console/coordinator now has a
   persistent official Hyperliquid websocket read model. UI refreshes and Fib
   reconciliation should use that cache before REST fallback.

## Conclusion

The manual trading, market data, account overview, Vault, transfer planning,
native TP/SL arm/read, and REST rate-limit handling paths are aligned with the
official docs and passed current non-state-changing validation.

The strategy layer is only partially complete: Fib basic now has a compliant
dry-run execution chain, and smart-money copy has a compliant preview/signal
core, but neither is yet a fully accepted unattended live automation system.
