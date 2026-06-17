# Smart Money Copy Development Spec

Last updated: 2026-06-11.

This document is the implementation-ready specification for the V2 smart-money
copy trading module. It expands the high-level strategy notes in
`strategy-development.md` into concrete runtime, state, risk, testing, and
operating requirements.

## Goal

Support 24/7 copy trading for one or more configured leader accounts:

1. Listen to leader trading behavior in near real time.
2. Normalize raw fills and position changes into semantic actions.
3. Deduplicate replays, snapshots, and multi-source duplicates.
4. Resolve same-symbol conflicts across multiple leaders.
5. Apply copy sizing and risk controls before any local order.
6. Fan out approved copy signals to local account workers.
7. Execute through the V2 low-latency path.
8. Reconcile fills, positions, and copy attribution after submission.

The module must optimize for fast follow buys while staying conservative when
the leader action is ambiguous.

## Non-Goals

The first production version will not:

- infer private leader intent from open orders alone;
- copy hidden strategy logic beyond observable fills and position deltas;
- execute when leader behavior cannot be reliably classified;
- bypass the V2 `RiskGateway` or per-account workers;
- run unlimited leader subscriptions from one process/IP.

## Official API Basis

trade[XYZ] runs on Hyperliquid HIP-3 perps. The source of truth for exchange
behavior remains the official Hyperliquid API and SDK behavior.

Leader monitoring should use WebSocket-first data:

- `userFills`: primary source for executed leader trades.
- `userEvents`: supplementary fill and non-user-cancel events.
- `orderUpdates`: order lifecycle observation only; not a standalone copy
  trigger.
- `allDexsClearinghouseState`: leader position state across default and builder
  dexes.
- `allMids`, `activeAssetCtx`, or an equivalent market stream for fresh prices
  and spread checks.

REST `/info` is reserved for:

- startup snapshots;
- reconnect reconciliation;
- historical backfill after a gap;
- explicit diagnostics;
- bounded fallback when a configured dry-run/shadow mode allows it.

Post-submit copy reconciliation uses the Info endpoint in read-only mode:

- `orderStatus` is queried by V2-owned exchange `oid` first, or deterministic
  `cloid` when `oid` is unavailable.
- `userFills` is fetched once per local account and filtered by `oid + coin` to
  supplement actual filled size, weighted average fill price, and fill time.
- `userFills` alone is not ownership proof; the copy ledger must already map
  the order through a V2-owned `oid` or `cloid`.
- Any lookup, parse, side, coin, or ownership mismatch fails closed and leaves
  the ledger unchanged.

Important limit: Hyperliquid documents a cap on unique user-specific WebSocket
subscriptions per IP. A production deployment with many local accounts and
leaders may need watcher sharding, multiple network egresses, or a hybrid
listener/backfill design.

## Runtime Architecture

```text
Leader WS Watcher(s)
        |
        v
Leader Event Normalizer
        |
        v
Copy Decision Engine
  - dedupe
  - semantic action classification
  - conflict micro-batching
  - position mapping lookup
        |
        v
CopyTradeIntent / CoordinatorSignal
        |
        v
RiskGateway
        |
        v
Per-Account Worker(s)
        |
        v
Executor / Hyperliquid WebSocket Post
        |
        v
Background Reconciliation + Copy Ledger
```

No strategy component may sign, submit, cancel, or directly call the exchange
adapter. Strategies emit intents only.

## Module Boundaries

### `leader_watcher`

Responsibilities:

- maintain WebSocket subscriptions for configured leader addresses;
- seed leader state on startup/reconnect;
- parse raw WS messages into typed raw events;
- emit raw fills, order updates, and position snapshots with receive timestamps;
- surface stream health and gap warnings.

It must not:

- decide whether to copy;
- calculate local sizing;
- submit local orders.

### `leader_state`

Responsibilities:

- maintain latest leader positions by `leader_id + market + coin`;
- track prior position snapshots for delta classification;
- keep recent leader fills for dedupe/backfill;
- expose freshness timestamps.

### `copy_decision`

Responsibilities:

- transform raw leader events into semantic leader actions;
- dedupe snapshots/replays/backfill duplicates;
- aggregate multiple leaders over a short conflict window;
- select final copy action or conservative skip;
- generate `CoordinatorSignal` / `CopyTradeIntent` candidates.

### `copy_ledger`

Responsibilities:

- persist source attribution for local copy positions;
- map local fills back to `leader_event_id`, `leader_id`, `leader_group`,
  `signal_id`, account, coin, and side;
- record pending exposure before local fill confirmation;
- record V2-owned `cloid`, exchange `oid`, submit time, and fill time for every
  submit-capable copy order;
- reconcile owned submit/fill evidence into durable ledger lifecycle state:
  `PendingOpen -> Open`, `PendingReduce/PendingClose -> Closed`;
- reconcile read-only `orderStatus` + `userFills` evidence after submit or
  restart without generating any exchange action;
- support restart recovery and close/reduce mapping.

### `copy_risk`

Responsibilities:

- enforce copy-specific risk checks before portfolio/execution checks;
- reject stale, duplicated, blacklisted, ambiguous, oversized, or unmapped
  events with stable reason codes.

## Data Model

### Raw Leader Fill

Required fields:

- `leader_id`
- `leader_group`
- `leader_address`
- `market`
- `dex`
- `coin`
- `side`
- `price`
- `size`
- `notional_usd`
- `oid`
- `hash`
- `exchange_time_ms`
- `received_at_ms`
- `is_snapshot`

Recommended identity:

```text
leader_fill_id = leader_address + market + coin + hash + oid + exchange_time_ms
```

### Leader Position Snapshot

Required fields:

- `leader_id`
- `market`
- `coin`
- `signed_size`
- `position_notional_usd`
- `entry_price`
- `snapshot_time_ms`
- `received_at_ms`
- `source`

`signed_size > 0` means long, `signed_size < 0` means short, and zero means
flat.

### Semantic Leader Action

The normalizer must emit one of:

- `LeaderOpenLong`
- `LeaderIncreaseLong`
- `LeaderReduceLong`
- `LeaderCloseLong`
- `LeaderOpenShort`
- `LeaderIncreaseShort`
- `LeaderReduceShort`
- `LeaderCloseShort`
- `LeaderFlipLongToShort`
- `LeaderFlipShortToLong`
- `LeaderAmbiguous`

Every semantic action must include:

- raw event references;
- before/after leader position if known;
- confidence tier;
- classification reason;
- whether it is a close signal;
- whether it is eligible for new local exposure.

### Copy Position Ledger Entry

Required fields:

- `ledger_id`
- `local_account_id`
- `leader_id`
- `leader_group`
- `source_event_id`
- `signal_id`
- `market`
- `coin`
- `local_side`
- `planned_notional_usd`
- `planned_size`
- `pending_notional_usd`
- `filled_size`
- `avg_fill_price`
- `remaining_size`
- `opened_at_ms`
- `last_updated_ms`
- `status`

Statuses:

- `pending_open`
- `open`
- `pending_reduce`
- `pending_close`
- `closed`
- `orphaned`
- `rejected`

## Behavior Classification

Raw `Buy` / `Sell` is not enough for perps. The classifier must compare the
leader's position before and after the fill or after the next fresh position
snapshot.

Classification rules:

```text
before = previous signed leader position
after  = latest signed leader position

before == 0, after > 0  -> LeaderOpenLong
before > 0,  after > before -> LeaderIncreaseLong
before > 0,  0 < after < before -> LeaderReduceLong
before > 0,  after == 0 -> LeaderCloseLong
before > 0,  after < 0  -> LeaderFlipLongToShort

before == 0, after < 0  -> LeaderOpenShort
before < 0,  after < before -> LeaderIncreaseShort
before < 0,  before < after < 0 -> LeaderReduceShort
before < 0,  after == 0 -> LeaderCloseShort
before < 0,  after > 0  -> LeaderFlipShortToLong
```

If position snapshots are stale or unavailable:

- opening/increase classification must fail closed;
- reduce/close may be marked `LeaderAmbiguous` and queued for bounded
  reconciliation;
- no new local exposure may be opened from ambiguous events.

Flip handling in version 1:

1. Emit a close signal for the existing local mapped exposure.
2. Emit a new open signal only if flip-open copying is enabled and the open leg
   passes conflict and risk checks.

## Buy Decision Flow

```text
raw leader fill
  -> dedupe
  -> classify semantic action
  -> reject ambiguous new exposure
  -> conflict micro-batch by market+coin
  -> select direction or skip
  -> calculate candidate notional
  -> cap by leader/symbol/account/global limits
  -> create copy signal
  -> per-account risk
  -> fast submit
```

Eligible buy/open actions:

- `LeaderOpenLong` -> local `Buy`
- `LeaderIncreaseLong` -> local `Buy`
- `LeaderOpenShort` -> local `Sell`
- `LeaderIncreaseShort` -> local `Sell`

Whether shorts are allowed is market/account/symbol configurable. Spot markets
must reject short opens.

## Sell / Close Decision Flow

Close signals are intentionally more aggressive than open signals.

Policy:

- If any enabled leader emits a confident close signal for a coin, local mapped
  exposure for that coin should be closed.
- The close action must be `reduce_only`.
- The local close must never enlarge exposure.
- `ReduceLong` / `ReduceShort` partial-reduce actions follow the same ownership
  rule as full closes: they may only reduce V2-owned, ledger-mapped exposure.
- If exact mapping is unavailable, prefer conservative close of current local
  copy exposure for the same `coin + side`, or reject if that cannot be proven.
- Exchange `reduce_only` is not sufficient proof of ownership. The copy runtime
  must prove the local exposure through its durable copy ledger before producing
  any submit-capable reduce-only intent.

Flow:

```text
leader close/reduce/flip event
  -> dedupe
  -> classify close/reduce
  -> lookup local copy ledger mappings
  -> choose exact reduce size or full mapped close
  -> generate reduce-only intent
  -> risk allows reduce-only under configured kill switch policy
  -> fast close or queued close retry
  -> reconcile residuals
```

Version 1 default:

- `LeaderClose*` and flip close legs trigger local full close of mapped copy
  exposure for that coin/side.
- `LeaderReduce*` may be copied proportionally only when mapping is clear;
  planned reduce notional is capped by remaining mapped local exposure.
- Missing or exhausted mapped local exposure must reject the signal with
  `COPY_MAPPING_MISSING`; it must not fall back to closing manual or unowned
  account positions.
- Any close signal from any enabled leader can close local mapped exposure, even
  if another leader still holds, unless `require_remaining_holder_check` is
  enabled for that leader group.

## Conflict Resolution

The conflict resolver handles multiple leaders acting on the same symbol within
a short time window.

Suggested defaults:

```toml
conflict_window_ms = 500
min_direction_score_ratio = 1.5
primary_leader_can_override = true
close_overrides_open = true
```

Events are grouped by:

```text
market + coin + action_family + window_bucket
```

Direction scores may include:

- leader configured weight;
- leader tier;
- leader notional;
- leader group dedupe;
- event freshness;
- whether the event is open/increase or close/reduce.

Default decisions:

- same-direction opens: merge or choose weighted notional, then cap by risk;
- opposite-direction opens: skip unless one side exceeds the configured score
  threshold;
- close vs open: close wins for existing mapped exposure; new open can be
  evaluated after close if enabled;
- same leader group duplicates: keep the strongest event, do not count the
  group multiple times.

A skipped conflict must write a structured audit event with the competing
leaders, scores, and reason.

## Dedupe

Dedupe must be persistent across restart.

Minimum dedupe keys:

```text
raw_fill_key = leader_address + market + coin + hash + oid + exchange_time_ms
semantic_key = leader_group + leader_event_id + market + coin + semantic_action
signal_key = semantic_key + target_account + worker_id
```

Dedupe must cover:

- WebSocket snapshot replay;
- WebSocket reconnect replay;
- REST backfill finding already-seen fills;
- same leader entity configured as multiple addresses;
- one signal fanning out to multiple local accounts;
- account worker receiving the same signal twice.

Expired dedupe entries may be compacted only after their audit and ledger
records are durable.

## Sizing

Candidate open notional:

```text
leader_notional = fill.price * abs(fill.size)
leader_scaled = leader_notional * leader.copy_ratio
account_scaled = leader_scaled * local_account.copy_ratio
final_notional = min(
  account_scaled,
  leader.max_notional_usd_per_trade,
  symbol.max_order_notional_usd,
  account.max_order_notional_usd,
  remaining_symbol_position_cap,
  remaining_daily_copy_cap,
  remaining_global_cap
)
```

If `final_notional` is below exchange minimum open notional, reject new exposure.
Reduce-only closes may bypass the open minimum so small residuals can exit.

If the symbol-level position cap is hit:

- cap to the remaining allowed amount when positive;
- reject when remaining allowed amount is zero or below the exchange minimum.

Pending exposure must be counted:

```text
effective_exposure = confirmed_position + pending_open - pending_close
```

This prevents repeated fast signals from exceeding caps before fills are
confirmed.

## Risk Checks

Copy-specific checks run before portfolio and execution checks.

Required reason codes:

- `COPY_STRATEGY_DISABLED`
- `LEADER_DISABLED`
- `LEADER_NOT_CONFIGURED`
- `LEADER_EVENT_DUPLICATE`
- `LEADER_EVENT_TOO_OLD`
- `LEADER_POSITION_STALE`
- `LEADER_ACTION_AMBIGUOUS`
- `COPY_CONFLICT_NO_DECISION`
- `COPY_SYMBOL_BLOCKED`
- `COPY_MARKET_DISABLED`
- `COPY_SHORT_NOT_ALLOWED`
- `COPY_RATIO_INVALID`
- `COPY_NOTIONAL_TOO_SMALL`
- `COPY_LEADER_TRADE_CAP_EXCEEDED`
- `COPY_LEADER_DAILY_CAP_EXCEEDED`
- `COPY_SYMBOL_CAP_EXCEEDED`
- `COPY_ACCOUNT_CAP_EXCEEDED`
- `COPY_DAILY_CAP_EXCEEDED`
- `COPY_MAPPING_MISSING`
- `COPY_REDUCE_WOULD_EXPAND`
- `COPY_PENDING_EXPOSURE_LIMIT`
- `COPY_SPREAD_TOO_WIDE`
- `COPY_PRICE_STALE`
- `COPY_RATE_LIMIT_UNHEALTHY`

Portfolio/execution risk then applies existing global checks:

- kill switch;
- account targeting;
- margin health;
- max leverage;
- precision;
- min notional;
- stale market cache;
- nonce/rate-limit health.

## Configuration

Recommended TOML shape:

```toml
[strategies.smart_money_copy.main]
enabled = true
mode = "dry_run"
max_signal_delay_ms = 1500
dedupe_window_secs = 3600
conflict_window_ms = 500
min_direction_score_ratio = 1.5
close_overrides_open = true
flip_open_enabled = false
default_copy_ratio = 0.10
default_leader_weight = 1.0
max_slippage_bps_open = 25
max_slippage_bps_close = 75
allow_short = true
require_fresh_position_for_open = true
pending_open_ttl_secs = 900
post_close_reentry_guard_secs = 1800

[[strategies.smart_money_copy.leaders]]
leader_id = "leader_alpha"
leader_group = "fund_alpha"
account = "0x0000000000000000000000000000000000000000"
enabled = true
tier = "primary"
weight = 2.0
copy_ratio = 0.08
max_notional_usd_per_trade = 300.0
max_daily_notional_usd = 1000.0
allow_open = true
allow_close = true

[[strategies.smart_money_copy.symbol_limits]]
coin = "xyz:TSLA"
market = "xyz_perp"
enabled = true
allow_short = true
max_order_notional_usd = 300.0
max_position_notional_usd = 1000.0
max_daily_copy_notional_usd = 2000.0
max_spread_bps_open = 25
max_spread_bps_close = 75
```

Configuration validation must reject:

- duplicate `leader_id`;
- duplicate active `leader_id + account`;
- invalid addresses;
- copy ratios outside the allowed range;
- negative or zero notional caps;
- `allow_short=true` on spot symbols;
- enabled strategy with no enabled leaders;
- mainnet live copy without explicit live gates.

## State Persistence

Before mainnet live, use a durable store. JSONL may be enough for audit logs,
but copy trading state should move to SQLite or an equivalent event store before
larger live exposure.

Persist:

- leader raw event log;
- semantic event log;
- dedupe keys;
- conflict decisions;
- generated signals;
- risk decisions;
- worker reports;
- copy ledger entries;
- pending exposure;
- post-close reentry guards;
- watcher gap/reconnect events.

Restart requirements:

1. Load dedupe keys and copy ledger.
2. Seed local account positions/open orders.
3. Seed leader positions.
4. Reconcile pending local orders/fills.
5. Resume WebSocket streams.
6. Backfill only the bounded missed window.
7. Do not emit new exposure until leader and local caches are fresh.

## Latency Targets

Measure separately:

- exchange event time to local receive time;
- receive time to semantic action;
- semantic action to conflict decision;
- decision to risk approval;
- risk approval to worker receive;
- worker receive to WS post ack;
- WS post ack to fill/position confirmation.

Initial targets:

- open signal classification under 50 ms after event receive;
- conflict window normally 300-500 ms;
- risk + signal fan-out under 100 ms;
- worker submit critical path under 300 ms after approval, excluding exchange
  matching time.

Do not hide conflict-window latency in submit latency metrics.

## 24/7 Operation

The copy module must run as a supervised runtime component:

- automatic WebSocket reconnect with jittered backoff;
- heartbeat and last-message timestamps per leader stream;
- bounded queues with fail-closed behavior;
- no unbounded signal backlog during disconnects;
- explicit degraded mode when leader/account/market state is stale;
- health status surfaced in Dashboard and Smart Money Copy pages.

Actions during degraded state:

- new opens: reject;
- increases: reject;
- reduce/close: allow if local position mapping is fresh and risk permits;
- reconciliation: continue;
- REST fallback: bounded and audited.

## Frontend Requirements

The Smart Money Copy page must show:

- leader list, group, tier, enabled status, weight, copy ratio, caps;
- stream health per leader;
- recent raw leader fills;
- recent semantic actions;
- conflict decisions and skipped reasons;
- generated copy signals;
- signal fan-out status per local account worker;
- copy ledger / local-position attribution;
- pending exposure;
- recent risk rejections.

Allowed operations:

- add/disable leader;
- edit group, tier, weight, ratio, caps;
- edit symbol limits;
- pause/resume strategy;
- leader/symbol/module kill switch;
- dry-run replay of recent leader events.

Frontend must not:

- submit orders directly;
- delete audit history when removing a leader;
- infer copy state from UI-only data.

## Testing Plan

### Unit Tests

Required:

- raw fill identity and dedupe;
- position delta classification for open/increase/reduce/close/flip;
- ambiguous event fail-closed behavior;
- same-direction merge;
- opposite-direction conflict skip;
- weighted leader override;
- close-over-open priority;
- same leader group dedupe;
- sizing cap order;
- pending exposure cap;
- reduce-only close cannot expand exposure;
- post-close reentry guard;
- config validation.

### Replay Tests

Required scenarios:

- WebSocket snapshot replay does not duplicate orders.
- Reconnect + REST backfill does not duplicate orders.
- Multiple leaders buy same coin same direction.
- Multiple leaders open opposite directions within the conflict window.
- Any leader close signal closes mapped local exposure.
- Leader flip becomes close then optional open.
- Process restart with pending open does not rebuy.
- Process restart with pending close continues residual cleanup.

### Integration Tests

Required:

- leader watcher mock WS -> semantic event -> copy signal;
- copy signal -> risk -> multiple mock workers;
- one slow/failing worker does not delay others;
- stale leader position blocks new exposure;
- stale market price blocks new exposure;
- reduce-only close can pass when global kill switch allows reduce-only;
- queue full returns explicit fail-closed rejection.

### Mainnet Dry-Run Shadow

Before any live copy:

- run with real leader streams and dry-run executor;
- record at least one full trading session or agreed observation window;
- review signal count, skipped conflicts, rejections, and hypothetical notional;
- verify no duplicate signals for replay/backfill;
- verify no stale-price opens;
- verify close signals map to local dry-run ledger state.

### Small Live Acceptance

Only after dry-run shadow passes:

- enable one market and one or two leaders;
- use very low per-trade and per-symbol caps;
- use dedicated test local account(s);
- start with opens disabled if close/reduce path needs validation separately;
- confirm order, fill, position, and ledger reconciliation after every live
  action;
- stop immediately on duplicate signal, unmapped close, stale state, or
  unexpected notional.

## Implementation Phases

### Phase C0: Specification and Fixtures

- Add this document and supporting config/test docs.
- Define typed fixtures for raw fills, positions, and expected semantic actions.
- No live code changes required.

### Phase C1: Watcher Probe

- Build read-only leader watcher.
- Support `userFills`, `userEvents`, `orderUpdates`, and leader position
  snapshots.
- Log raw events and stream health.

### Phase C2: Semantic Classifier

- Implement leader state and delta classifier.
- Add replay tests for open/increase/reduce/close/flip.
- Fail closed on stale/ambiguous new exposure.

### Phase C3: Decision Engine

- Add dedupe, conflict micro-batching, and final copy signal selection.
- Add same-group dedupe and close priority.
- Add dry-run preview endpoint for recent events.

### Phase C4: Copy Risk and Ledger

- Implement copy-specific risk checks.
- Persist copy ledger, pending exposure, and post-close reentry guards.
- Add restart recovery tests.

### Phase C5: Worker Fast Path Integration

- Route approved copy signals to per-account workers.
- Use deterministic signal ids and cloids.
- Submit through WebSocket post fast path where supported.
- Reconcile asynchronously.

### Phase C6: Shadow and Live Readiness

- Mainnet dry-run shadow.
- Dashboard/Copy page health and audit panels.
- Small live window plan and rollback checklist.

## Operational Stop Conditions

Immediately pause copy opens if any of the following occurs:

- duplicate live order for the same leader event;
- leader position stream is stale beyond threshold;
- local account state is stale beyond threshold;
- copy ledger cannot be written;
- pending exposure cannot be reconstructed after restart;
- conflict resolver emits inconsistent decisions for the same window;
- exchange rejects due to precision/min-notional assumptions;
- rate limit health is degraded;
- user activates global/strategy/symbol kill switch.

Reduce-only closes may remain enabled during a pause only when mapping and local
account state are fresh and the kill-switch policy allows it.
