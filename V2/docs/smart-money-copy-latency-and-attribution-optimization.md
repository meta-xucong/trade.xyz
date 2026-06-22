# Smart Money Copy Latency And Attribution Optimization

## Background

The 2026-06-21 live JPY review showed a repeatable loss pattern:

- A leader had an existing profitable position.
- The leader emitted a small increase/open fill near the end of the move.
- The local account copied that small fresh fill.
- The leader then reduced an older profitable position seconds later.
- The local account had no equivalent old-cost basis, so it closed a fresh
  position and paid spread, slippage, and fees.

This is execution-correct but strategy-wrong for very short scalp flows. The
copy engine must distinguish a true new leader position from a late add to a
leader position we did not originally copy.

## Goals

- Do not chase a leader's historical-position tail.
- Preserve sensitive sell handling for positions the local account actually
  copied.
- Avoid one leader closing another leader's mapped exposure by default.
- Make latency and PnL attribution visible enough to diagnose whether losses
  came from target quality, delayed execution, or incorrect mapping.

## Non-Goals

- Do not disable reduce-only safety closes for positions with clear local
  mapping.
- Do not change V1 behavior.
- Do not introduce a new frontend-heavy execution path. Live actions still flow
  through strategy, risk, account worker, and executor.

## Rules

### 1. Open Only From Fresh Leader Positions

`OpenLong` and `OpenShort` may open a new local mapped position.

`IncreaseLong` and `IncreaseShort` are only eligible when the local ledger
already has effective exposure for the same account, leader group, coin, and
side. If no such local mapping exists, reject the signal with:

`COPY_INCREASE_WITHOUT_LOCAL_MAPPING`

Rationale: a leader increase while they already have a position may be profitable
for their historical cost basis, but it is not equivalent to a fresh copy entry.

### 2. Same-Source Reduce By Default

Reduce and close signals should first consume only mapped exposure from the same
leader group. A different leader's close must not flatten unrelated copied
exposure just because the coin and direction match.

Emergency/global close can be added later as an explicit operator mode. It must
be visible in audit output if enabled.

### 3. Sensitive But Mapped Sell

When a same-source reduce/close signal exists, submit reduce-only as fast as the
current live path permits. Reduce-only sizing is capped by readable local
exchange exposure before submission.

### 4. Latency Attribution

Reports should make these timestamps inspectable for every submitted copy order:

- leader exchange fill time
- local signal creation time
- local order submit time
- local exchange fill time

The minimum useful derived metrics are:

- leader-to-signal delay
- signal-to-submit delay
- leader-to-local-fill delay

### 5. PnL Attribution

The copy result page and reports should be able to explain:

- which leader/group opened the local position
- which leader/group reduced or closed it
- local entry price, local exit price, fees, and realized PnL
- whether the target's close PnL came from a pre-existing position

## Acceptance

- A synthetic `IncreaseLong` with no local same-leader exposure is rejected and
  emits no order.
- A synthetic `IncreaseLong` with existing same-leader exposure is allowed and
  updates mapped exposure.
- A `leader_b` reduce signal does not close `leader_a` exposure on the same coin
  by default.
- A same-leader reduce signal still produces a reduce-only candidate.
- Focused Rust tests for smart-money strategy and ledger pass.
- Existing copy live daemon submit-plan tests continue to pass.
