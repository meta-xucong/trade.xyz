# Smart Money Copy Reduce-Only Effective Minimum Fix

## Problem

The copy daemon already had a reduce-only safety rule:

- cap a close/reduce order to the live local position;
- if the remaining local position would be below the exchange minimum order value, expand the order to close the whole local position.

That rule used nominal USD notional only. For high-priced markets such as `xyz:SP500`, a nominal reduce order can look larger than `$10` while the exchange-size-rounded order is smaller than `$10`. Example:

- planned reduce notional: about `14.75` USD;
- SP500 reference price: about `7,470`;
- size precision: `0.001`;
- rounded size: `0.001`;
- effective exchange order value: about `7.47` USD.

The exchange then rejects the reduce-only order with minimum value errors, even though the daemon believed the order was valid. Repeating the same pending reduce can leave a copy-owned residual position open.

## Required Behavior

Reduce-only preparation must use the same order size and price semantics that the exchange sees:

1. Read live local exposure before submitting a reduce-only order.
2. Cap requested reduce notional to the live local position.
3. Build the effective order using current market metadata, reference price, limit price, and size precision.
4. If the rounded effective order value is at least the exchange minimum, submit normally.
5. If the rounded effective value is below the exchange minimum but the full live local position can produce a valid order, expand the order to close the full local position.
6. If the full live local position is also below the effective exchange minimum, keep it as dust/pending and do not submit a guaranteed-rejected order.

This changes only order sizing before submit. It must not change signal freshness behavior, open/add behavior, or whether a valid close should be followed.

## Implementation

- The live reduce-only exposure check now records both local position notional and local position size.
- The reduce-only filter fetches the relevant market snapshot before live submit.
- A precision-aware decision helper computes:
  - nominal capped notional;
  - rounded planned size;
  - effective order value after precision rounding;
  - full-position rounded size and effective value.
- The daemon expands a reduce-only order to full-position notional when precision rounding would otherwise make the submitted order fall below `$10`.
- Immediately before live submit, full-close or near-full-close reduce-only orders re-read the live local position and attach the exact live position size to the approved order. This prevents a later price snapshot from turning a full-close notional back into a one-size-step-short order.
- The live report now exposes separate counts for:
  - nominal residual flattening;
  - precision-min full-position expansion;
  - true dust where the whole local position is still below the effective minimum.

## Acceptance

- SP500-style reduce-only orders whose nominal notional is above `$10` but whose rounded effective value is below `$10` are expanded to full close when the live position can support a valid order.
- Full-close or sub-min-residual close orders submit with exact live position size, while normal partial reductions keep notional sizing.
- True dust positions are not repeatedly sent to the exchange as guaranteed failures.
- Existing nominal residual flattening behavior remains intact.
- Open/increase effective-min checks remain unchanged.
