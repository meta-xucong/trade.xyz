# Smart Money Copy downtime close backfill

## Root cause

The persistent copy supervisor only deduplicates and persists leader events that
arrive while the WebSocket watcher is online. After a host/process outage, the
restart path reloads the local ledger and resumes watching new leader events,
but it does not query `userFillsByTime` for the outage window and therefore
does not replay missed leader close/reduce fills.

This leaves a gap for follow-position mode:

- local copied exposure can remain open in the persisted ledger;
- the leader can close the same exposure while the runner is offline;
- on restart, no historical close event is converted into a reduce-only local
  order, so the local account keeps the stale position until some later online
  catch-up condition happens.

The 2026-07-21/2026-07-22 outage showed exactly this failure mode: `leader_4`
closed `xyz:SP500` long exposure during the downtime, while local `addr_b`
still held the mapped `xyz:SP500` long after restart.

## Design

At supervisor startup, before normal WebSocket watching begins, run a bounded
close-only backfill pass:

1. Build active exposure candidates from the persisted copy ledger for the
   selected local accounts.
2. Only consider candidates with a unique local ownership scope for
   `(account, coin, side)`. If multiple leader groups share the same local coin
   side, skip automatic recovery and report the ambiguity.
3. For each candidate leader, query a bounded `userFillsByTime` window that
   starts from the earliest active local open entry, capped by the built-in
   72-hour startup backfill window.
4. Keep only historical leader fills that are close/reduce direction for the
   same coin and same side as the local copied exposure.
5. Deduplicate against both the normal persisted leader event keys and
   previous backfill event keys.
6. Confirm the current leader clearinghouse state no longer has the same side
   on that coin. If the leader still has the side, report but do not submit an
   automatic close.
7. Persist a deterministic `PendingClose` ledger entry before submitting the
   recovery order.
8. Submit only a reduce-only order through the existing plan contract,
   risk gateway, account worker, and live reduce-only exposure filter.

The backfill pass intentionally does not replay historical opens. Catching up
historical buys after a downtime can unintentionally enter stale positions; the
safe unattended recovery action is closing local exposure that is provably stale
against the current leader state.

## Safety properties

- No exchange-changing action bypasses the existing live gates, account scope
  checks, order planner, account worker, or reduce-only live exposure filter.
- Generated client order ids and signal ids are deterministic, so restart retry
  is idempotent.
- Recovery entries are written as `PendingClose` before order submission, so
  ledger reconciliation can match the eventual order evidence.
- Ambiguous multi-leader local exposure is skipped instead of force-closed.
- Below-min residuals remain handled by the existing reduce-only effective
  minimum filter.
- Startup backfill refs may stop the watcher window immediately so recovery can
  submit without waiting. Recovered historical `PendingReduce`/`PendingClose`
  residuals must not by themselves shorten every watcher round; otherwise a
  sub-min dust position can repeatedly turn ordinary new leader events into
  noisy zero-submit rounds.

## Verification checklist

- Unit tests cover:
  - generating one reduce-only recovery ref for a flat leader with active local
    mapped exposure;
  - suppressing recovery when the current leader still has the same side;
  - suppressing ambiguous local multi-leader ownership;
  - suppressing already-seen historical fills;
  - ignoring already-known pending reduce refs when deciding whether a new
    watcher event should stop the round for immediate submit.
- Live restart audit must show:
  - a startup `downtime_backfill` report with the scanned window and generated
    refs;
  - matching submitted report and order evidence for any generated recovery;
  - final reconciliation showing the stale local `SP500` position removed or
    reduced by the exchange-side reduce-only filter.
