use std::collections::{HashMap, HashSet};

use crate::{
    domain::{CoordinatorSignal, ExecutionMode, OrderSide, SignalOrder, SignalSource, now_ms},
    strategy::{LeaderFillEvent, Strategy, StrategyContext, StrategyEvent},
};

const POSITION_EPS: f64 = 1e-9;
pub const COPY_DEFAULT_PRINCIPAL_CAP_USD: f64 = 10.0;
pub const COPY_MAX_LEVERAGE: f64 = 5.0;
pub const COPY_DEFAULT_MAX_SIGNAL_NOTIONAL_USD: f64 =
    COPY_DEFAULT_PRINCIPAL_CAP_USD * COPY_MAX_LEVERAGE;
mod conflict;
mod ledger;
mod persistence;
mod risk;
mod shadow_history;
mod watcher;

pub use conflict::{CopyConflictInput, CopyConflictResolution, resolve_copy_conflict};
pub use ledger::{CopyLedger, CopyLedgerEntry, CopyLedgerReconcileResult, CopyLedgerStatus};
pub use persistence::{
    CopyPersistenceSnapshot, load_copy_persistence_snapshot, save_copy_persistence_snapshot,
};
pub use risk::{
    CopyLiveGateDecision, CopyLiveGateInput, CopySignalRiskDecision, CopySignalRiskInput,
    CopySizingDecision, CopySizingInput, calculate_copy_notional, evaluate_copy_live_gate,
    evaluate_copy_signal_risk,
};
pub use shadow_history::{
    CopyShadowHistoryEntry, append_copy_shadow_history_entry, append_copy_shadow_history_records,
    read_recent_copy_shadow_history_entries,
};
pub use watcher::{
    CopyLeaderWatcherEvent, ReadOnlyLeaderWatcherConfig, SmartMoneyLeaderWatch,
    parse_read_only_leader_watcher_message, read_only_leader_watcher_subscriptions,
    run_read_only_leader_watcher_once,
};

#[cfg(test)]
pub(crate) use watcher::copy_watcher_ws_url;

#[derive(Debug, Clone)]
pub struct LeaderRule {
    pub leader_id: String,
    pub leader_address: String,
    pub enabled: bool,
    pub copy_ratio: f64,
}

#[derive(Debug, Clone)]
pub struct SymbolCopyLimit {
    pub coin: String,
    pub max_signal_notional_usd: f64,
}

#[derive(Debug, Clone)]
pub struct SmartMoneyCopyConfig {
    pub strategy_id: String,
    pub default_copy_ratio: f64,
    pub max_slippage_bps: f64,
    pub leaders: Vec<LeaderRule>,
    pub symbol_limits: Vec<SymbolCopyLimit>,
}

#[derive(Debug, Clone)]
pub struct SmartMoneyCopyStrategy {
    config: SmartMoneyCopyConfig,
    seen_events: HashSet<String>,
    leader_by_address: HashMap<String, LeaderRule>,
    symbol_limits: HashMap<String, SymbolCopyLimit>,
}

impl SmartMoneyCopyStrategy {
    pub fn new(config: SmartMoneyCopyConfig) -> Self {
        let leader_by_address = config
            .leaders
            .iter()
            .cloned()
            .map(|leader| (leader.leader_address.to_lowercase(), leader))
            .collect();
        let symbol_limits = config
            .symbol_limits
            .iter()
            .cloned()
            .map(|limit| (limit.coin.clone(), limit))
            .collect();
        Self {
            config,
            seen_events: HashSet::new(),
            leader_by_address,
            symbol_limits,
        }
    }

    pub fn new_with_seen_event_keys(
        config: SmartMoneyCopyConfig,
        seen_event_keys: impl IntoIterator<Item = String>,
    ) -> Self {
        let mut strategy = Self::new(config);
        strategy.seen_events = seen_event_keys.into_iter().collect();
        strategy
    }

    pub fn seen_event_keys(&self) -> Vec<String> {
        let mut keys = self.seen_events.iter().cloned().collect::<Vec<_>>();
        keys.sort();
        keys
    }

    pub fn persistence_snapshot(
        &self,
        saved_at_ms: u64,
        ledger: &CopyLedger,
    ) -> CopyPersistenceSnapshot {
        CopyPersistenceSnapshot::new(saved_at_ms, self.seen_event_keys(), ledger)
    }

    fn handle_leader_fill(
        &mut self,
        ctx: &StrategyContext,
        fill: &LeaderFillEvent,
    ) -> Vec<CoordinatorSignal> {
        let Some(leader) = self
            .leader_by_address
            .get(&fill.leader_address.to_lowercase())
            .cloned()
        else {
            return Vec::new();
        };
        if !leader.enabled {
            return Vec::new();
        }
        if !self.seen_events.insert(fill.event_id.clone()) {
            return Vec::new();
        }

        let copy_ratio = if leader.copy_ratio > 0.0 {
            leader.copy_ratio
        } else {
            self.config.default_copy_ratio
        };
        let mut signal_notional_usd = fill.notional_usd * copy_ratio;
        if let Some(limit) = self.symbol_limits.get(&fill.coin) {
            signal_notional_usd = signal_notional_usd.min(limit.max_signal_notional_usd);
        }
        if signal_notional_usd <= 0.0 {
            return Vec::new();
        }

        let now = now_ms();
        vec![CoordinatorSignal {
            signal_id: format!("copy-{}-{}-{now}", leader.leader_id, fill.event_id),
            source: SignalSource::SmartMoney,
            created_at_ms: fill.received_at_ms,
            dispatch_at_ms: now,
            expires_at_ms: now + ctx.signal_ttl_ms,
            target_accounts: ctx.target_accounts.clone(),
            dedupe_key: dedupe_key(fill),
            order: SignalOrder {
                market: None,
                dex: None,
                coin: fill.coin.clone(),
                side: fill.side,
                notional_usd: signal_notional_usd,
                reduce_only: fill.reduce_only,
                execution_mode: ExecutionMode::Taker,
                max_slippage_bps: self.config.max_slippage_bps,
                limit_price: None,
                apply_account_ratio: true,
            },
        }]
    }

    pub fn signal_from_semantic_action(
        &mut self,
        ctx: &StrategyContext,
        action: &SemanticLeaderAction,
    ) -> Option<CoordinatorSignal> {
        self.signals_from_semantic_action(ctx, action)
            .into_iter()
            .next()
    }

    pub fn signals_from_semantic_action(
        &mut self,
        ctx: &StrategyContext,
        action: &SemanticLeaderAction,
    ) -> Vec<CoordinatorSignal> {
        if !matches!(action.confidence, LeaderActionConfidence::Strong) {
            return Vec::new();
        }
        let leader = self
            .leader_by_address
            .get(&action.leader_address.to_lowercase())
            .cloned();
        let Some(leader) = leader else {
            return Vec::new();
        };
        if !leader.enabled {
            return Vec::new();
        };

        let copy_ratio = if leader.copy_ratio > 0.0 {
            leader.copy_ratio
        } else {
            self.config.default_copy_ratio
        };
        let now = now_ms();
        let mut signals = Vec::new();

        for leg in action.kind.signal_legs() {
            let action_key = semantic_leg_dedupe_key(action, leg.dedupe_suffix);
            if self.seen_events.contains(&action_key) {
                continue;
            }
            let mut signal_notional_usd = leg.leader_notional_usd(action) * copy_ratio;
            if let Some(limit) = self.symbol_limits.get(&action.coin) {
                signal_notional_usd = signal_notional_usd.min(limit.max_signal_notional_usd);
            }
            if signal_notional_usd <= 0.0 {
                continue;
            }
            self.seen_events.insert(action_key.clone());
            signals.push(CoordinatorSignal {
                signal_id: format!(
                    "copy-{}-{}-{:?}-{}-{now}",
                    leader.leader_id, action.event_id, action.kind, leg.signal_id_suffix
                ),
                source: SignalSource::SmartMoney,
                created_at_ms: action.received_at_ms,
                dispatch_at_ms: now,
                expires_at_ms: now + ctx.signal_ttl_ms,
                target_accounts: ctx.target_accounts.clone(),
                dedupe_key: action_key,
                order: SignalOrder {
                    market: action.market.clone(),
                    dex: action.dex.clone(),
                    coin: action.coin.clone(),
                    side: leg.side,
                    notional_usd: signal_notional_usd,
                    reduce_only: leg.reduce_only,
                    execution_mode: ExecutionMode::Taker,
                    max_slippage_bps: self.config.max_slippage_bps,
                    limit_price: None,
                    apply_account_ratio: true,
                },
            });
        }

        signals
    }
}

impl Strategy for SmartMoneyCopyStrategy {
    fn id(&self) -> &str {
        &self.config.strategy_id
    }

    fn on_event(&mut self, ctx: &StrategyContext, event: StrategyEvent) -> Vec<CoordinatorSignal> {
        match event {
            StrategyEvent::LeaderFill(fill) => self.handle_leader_fill(ctx, &fill),
            StrategyEvent::MarketPrice(_) => Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaderActionKind {
    OpenLong,
    IncreaseLong,
    ReduceLong,
    CloseLong,
    OpenShort,
    IncreaseShort,
    ReduceShort,
    CloseShort,
    FlipLongToShort,
    FlipShortToLong,
    Ambiguous,
}

impl LeaderActionKind {
    pub fn opens_or_increases(self) -> bool {
        matches!(
            self,
            Self::OpenLong | Self::IncreaseLong | Self::OpenShort | Self::IncreaseShort
        )
    }

    pub fn closes_or_reduces(self) -> bool {
        matches!(
            self,
            Self::ReduceLong
                | Self::CloseLong
                | Self::ReduceShort
                | Self::CloseShort
                | Self::FlipLongToShort
                | Self::FlipShortToLong
        )
    }

    pub fn is_full_close(self) -> bool {
        matches!(
            self,
            Self::CloseLong | Self::CloseShort | Self::FlipLongToShort | Self::FlipShortToLong
        )
    }

    pub fn open_side(self) -> Option<OrderSide> {
        match self {
            Self::OpenLong | Self::IncreaseLong => Some(OrderSide::Buy),
            Self::OpenShort | Self::IncreaseShort => Some(OrderSide::Sell),
            _ => None,
        }
    }

    pub fn close_side(self) -> Option<OrderSide> {
        match self {
            Self::ReduceLong | Self::CloseLong | Self::FlipLongToShort => Some(OrderSide::Sell),
            Self::ReduceShort | Self::CloseShort | Self::FlipShortToLong => Some(OrderSide::Buy),
            _ => None,
        }
    }

    fn signal_legs(self) -> Vec<CopySignalLeg> {
        match self {
            Self::OpenLong | Self::IncreaseLong => {
                vec![CopySignalLeg::open(OrderSide::Buy, "open")]
            }
            Self::OpenShort | Self::IncreaseShort => {
                vec![CopySignalLeg::open(OrderSide::Sell, "open")]
            }
            Self::ReduceLong | Self::CloseLong => {
                vec![CopySignalLeg::close(OrderSide::Sell, "close")]
            }
            Self::ReduceShort | Self::CloseShort => {
                vec![CopySignalLeg::close(OrderSide::Buy, "close")]
            }
            Self::FlipLongToShort => vec![
                CopySignalLeg::flip_close(OrderSide::Sell),
                CopySignalLeg::flip_open(OrderSide::Sell),
            ],
            Self::FlipShortToLong => vec![
                CopySignalLeg::flip_close(OrderSide::Buy),
                CopySignalLeg::flip_open(OrderSide::Buy),
            ],
            Self::Ambiguous => Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaderActionConfidence {
    Strong,
    Ambiguous,
}

#[derive(Debug, Clone, Copy)]
enum CopySignalLegNotional {
    Trade,
    FlipClose,
    FlipOpen,
}

#[derive(Debug, Clone, Copy)]
struct CopySignalLeg {
    side: OrderSide,
    reduce_only: bool,
    dedupe_suffix: &'static str,
    signal_id_suffix: &'static str,
    notional: CopySignalLegNotional,
}

impl CopySignalLeg {
    const fn open(side: OrderSide, suffix: &'static str) -> Self {
        Self {
            side,
            reduce_only: false,
            dedupe_suffix: suffix,
            signal_id_suffix: suffix,
            notional: CopySignalLegNotional::Trade,
        }
    }

    const fn close(side: OrderSide, suffix: &'static str) -> Self {
        Self {
            side,
            reduce_only: true,
            dedupe_suffix: suffix,
            signal_id_suffix: suffix,
            notional: CopySignalLegNotional::Trade,
        }
    }

    const fn flip_close(side: OrderSide) -> Self {
        Self {
            side,
            reduce_only: true,
            dedupe_suffix: "flip-close",
            signal_id_suffix: "flip-close",
            notional: CopySignalLegNotional::FlipClose,
        }
    }

    const fn flip_open(side: OrderSide) -> Self {
        Self {
            side,
            reduce_only: false,
            dedupe_suffix: "flip-open",
            signal_id_suffix: "flip-open",
            notional: CopySignalLegNotional::FlipOpen,
        }
    }

    fn leader_notional_usd(self, action: &SemanticLeaderAction) -> f64 {
        match self.notional {
            CopySignalLegNotional::Trade => action.leader_notional_usd,
            CopySignalLegNotional::FlipClose => action
                .close_leader_notional_usd
                .unwrap_or(action.leader_notional_usd),
            CopySignalLegNotional::FlipOpen => action
                .open_leader_notional_usd
                .unwrap_or(action.leader_notional_usd),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LeaderPositionSnapshot {
    pub leader_id: String,
    pub market: Option<String>,
    pub dex: Option<String>,
    pub coin: String,
    pub signed_size: f64,
    pub position_notional_usd: f64,
    pub snapshot_time_ms: u64,
    pub received_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct SemanticLeaderAction {
    pub leader_id: String,
    pub leader_address: String,
    pub market: Option<String>,
    pub dex: Option<String>,
    pub coin: String,
    pub event_id: String,
    pub kind: LeaderActionKind,
    pub confidence: LeaderActionConfidence,
    pub leader_notional_usd: f64,
    pub close_leader_notional_usd: Option<f64>,
    pub open_leader_notional_usd: Option<f64>,
    pub exchange_time_ms: u64,
    pub received_at_ms: u64,
    pub reason: String,
}

pub fn classify_leader_fill(
    fill: &LeaderFillEvent,
    before: Option<&LeaderPositionSnapshot>,
    after: Option<&LeaderPositionSnapshot>,
) -> SemanticLeaderAction {
    let (kind, confidence, reason) = match (before, after) {
        (Some(before), Some(after)) if same_position_scope(fill, before, after) => {
            let kind = classify_position_delta(before.signed_size, after.signed_size);
            (
                kind,
                if matches!(kind, LeaderActionKind::Ambiguous) {
                    LeaderActionConfidence::Ambiguous
                } else {
                    LeaderActionConfidence::Strong
                },
                format!(
                    "position_delta:{:.8}->{:.8}",
                    before.signed_size, after.signed_size
                ),
            )
        }
        _ => (
            LeaderActionKind::Ambiguous,
            LeaderActionConfidence::Ambiguous,
            "missing_or_mismatched_position_snapshot".to_string(),
        ),
    };

    let (close_leader_notional_usd, open_leader_notional_usd) = match (kind, before, after) {
        (
            LeaderActionKind::FlipLongToShort | LeaderActionKind::FlipShortToLong,
            Some(before),
            Some(after),
        ) => (
            Some(before.position_notional_usd.abs()),
            Some(after.position_notional_usd.abs()),
        ),
        _ => (None, None),
    };

    SemanticLeaderAction {
        leader_id: fill.leader_id.clone(),
        leader_address: fill.leader_address.clone(),
        market: after.and_then(|snapshot| snapshot.market.clone()),
        dex: after.and_then(|snapshot| snapshot.dex.clone()),
        coin: fill.coin.clone(),
        event_id: fill.event_id.clone(),
        kind,
        confidence,
        leader_notional_usd: fill.notional_usd,
        close_leader_notional_usd,
        open_leader_notional_usd,
        exchange_time_ms: fill.exchange_time_ms,
        received_at_ms: fill.received_at_ms,
        reason,
    }
}

pub fn classify_position_delta(before: f64, after: f64) -> LeaderActionKind {
    let before = normalize_position(before);
    let after = normalize_position(after);

    if before == 0.0 && after > 0.0 {
        LeaderActionKind::OpenLong
    } else if before > 0.0 && after > before {
        LeaderActionKind::IncreaseLong
    } else if before > 0.0 && after > 0.0 && after < before {
        LeaderActionKind::ReduceLong
    } else if before > 0.0 && after == 0.0 {
        LeaderActionKind::CloseLong
    } else if before > 0.0 && after < 0.0 {
        LeaderActionKind::FlipLongToShort
    } else if before == 0.0 && after < 0.0 {
        LeaderActionKind::OpenShort
    } else if before < 0.0 && after < before {
        LeaderActionKind::IncreaseShort
    } else if before < 0.0 && after < 0.0 && after > before {
        LeaderActionKind::ReduceShort
    } else if before < 0.0 && after == 0.0 {
        LeaderActionKind::CloseShort
    } else if before < 0.0 && after > 0.0 {
        LeaderActionKind::FlipShortToLong
    } else {
        LeaderActionKind::Ambiguous
    }
}

pub fn leader_fill_event_from_user_fill(
    leader_id: &str,
    leader_address: &str,
    fill: &crate::hyperliquid::UserFill,
    received_at_ms: u64,
) -> Option<LeaderFillEvent> {
    let price = parse_positive_f64(&fill.px)?;
    let size = parse_positive_f64(&fill.sz)?;
    let side = order_side_from_hyperliquid_fill(&fill.side)?;
    Some(LeaderFillEvent {
        event_id: format!("{}:{}:{}:{}", fill.hash, fill.oid, fill.time, fill.coin),
        leader_id: leader_id.to_string(),
        leader_address: leader_address.to_string(),
        coin: fill.coin.clone(),
        side,
        price,
        size,
        notional_usd: price * size,
        reduce_only: fill.dir.to_ascii_lowercase().contains("close"),
        exchange_time_ms: fill.time,
        received_at_ms,
    })
}

pub fn leader_position_snapshots_from_clearinghouse_state(
    leader_id: &str,
    market: Option<String>,
    dex: Option<String>,
    state: &crate::hyperliquid::ClearinghouseState,
    received_at_ms: u64,
) -> Vec<LeaderPositionSnapshot> {
    let snapshot_time_ms = state.time.unwrap_or(received_at_ms);
    state
        .asset_positions
        .iter()
        .filter_map(|asset_position| {
            let position = &asset_position.position;
            let signed_size = position.szi.parse::<f64>().ok()?;
            if normalize_position(signed_size) == 0.0 {
                return None;
            }
            Some(LeaderPositionSnapshot {
                leader_id: leader_id.to_string(),
                market: market.clone(),
                dex: dex.clone(),
                coin: position.coin.clone(),
                signed_size,
                position_notional_usd: position
                    .position_value
                    .as_deref()
                    .and_then(|value| value.parse::<f64>().ok())
                    .unwrap_or(0.0)
                    .abs(),
                snapshot_time_ms,
                received_at_ms,
            })
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct CopyDryRunShadowConfig {
    pub local_account_id: String,
    pub target_accounts: Vec<String>,
    pub signal_ttl_ms: u64,
    pub max_signal_delay_ms: u64,
    pub account_copy_ratio: f64,
    pub principal_cap_usd: f64,
    pub leverage: f64,
    pub max_signal_notional_usd: Option<f64>,
    pub exchange_min_open_notional_usd: f64,
    pub allow_short: bool,
    pub max_effective_exposure_usd: Option<f64>,
    pub blocked_symbols: Vec<String>,
    pub live_gate: CopyLiveGateInput,
}

#[derive(Debug, Clone)]
pub struct CopyDryRunShadowRecord {
    pub action: SemanticLeaderAction,
    pub live_gate: CopyLiveGateDecision,
    pub risk_decision: CopySignalRiskDecision,
    pub signal: Option<CoordinatorSignal>,
    pub ledger_entry: Option<CopyLedgerEntry>,
    pub persistence_snapshot: CopyPersistenceSnapshot,
}

pub fn approved_shadow_record_to_trade_intent(
    record: &CopyDryRunShadowRecord,
    account_id: &str,
    worker_id: &str,
    account_copy_ratio: f64,
) -> Option<crate::domain::TradeIntent> {
    let signal = record.signal.as_ref()?;
    if !matches!(record.live_gate, CopyLiveGateDecision::DryRunOnly) {
        return None;
    }
    if !matches!(
        record.risk_decision,
        CopySignalRiskDecision::Approved { .. }
    ) {
        return None;
    }
    Some(signal.to_trade_intent(account_id, worker_id, account_copy_ratio))
}

#[derive(Debug, Clone)]
struct PendingLeaderFill {
    fill: LeaderFillEvent,
    before: Option<LeaderPositionSnapshot>,
}

#[derive(Debug, Clone)]
pub struct CopyDryRunShadowPipeline {
    config: CopyDryRunShadowConfig,
    strategy: SmartMoneyCopyStrategy,
    ledger: CopyLedger,
    last_positions: HashMap<String, LeaderPositionSnapshot>,
    pending_fills: Vec<PendingLeaderFill>,
}

impl CopyDryRunShadowPipeline {
    pub fn new(
        config: CopyDryRunShadowConfig,
        strategy: SmartMoneyCopyStrategy,
        ledger: CopyLedger,
    ) -> Self {
        Self {
            config,
            strategy,
            ledger,
            last_positions: HashMap::new(),
            pending_fills: Vec::new(),
        }
    }

    pub fn ledger(&self) -> &CopyLedger {
        &self.ledger
    }

    pub fn persistence_snapshot(&self, saved_at_ms: u64) -> CopyPersistenceSnapshot {
        self.strategy
            .persistence_snapshot(saved_at_ms, &self.ledger)
    }

    pub fn handle_watcher_event(
        &mut self,
        event: CopyLeaderWatcherEvent,
        now_ms: u64,
    ) -> Vec<CopyDryRunShadowRecord> {
        match event {
            CopyLeaderWatcherEvent::Fill {
                fill, is_snapshot, ..
            } => {
                if is_snapshot {
                    return Vec::new();
                }
                let before = self
                    .last_positions
                    .get(&position_key(&fill.leader_id, &fill.coin))
                    .cloned();
                self.pending_fills.push(PendingLeaderFill { fill, before });
                Vec::new()
            }
            CopyLeaderWatcherEvent::PositionSnapshots { snapshots, .. } => {
                let mut records = Vec::new();
                for snapshot in snapshots {
                    let key = position_key(&snapshot.leader_id, &snapshot.coin);
                    let matching_pending = self
                        .pending_fills
                        .iter()
                        .enumerate()
                        .filter_map(|(index, pending)| {
                            (pending.fill.leader_id == snapshot.leader_id
                                && pending.fill.coin == snapshot.coin)
                                .then_some(index)
                        })
                        .collect::<Vec<_>>();
                    for index in matching_pending.into_iter().rev() {
                        let pending = self.pending_fills.remove(index);
                        let action = classify_leader_fill(
                            &pending.fill,
                            pending.before.as_ref(),
                            Some(&snapshot),
                        );
                        records.extend(self.shadow_records_from_action(action, now_ms));
                    }
                    self.last_positions.insert(key, snapshot);
                }
                records
            }
            CopyLeaderWatcherEvent::OrderUpdate { .. } => Vec::new(),
        }
    }

    fn shadow_records_from_action(
        &mut self,
        action: SemanticLeaderAction,
        now_ms: u64,
    ) -> Vec<CopyDryRunShadowRecord> {
        let live_gate = evaluate_copy_live_gate(self.config.live_gate);
        let risk_decision = self.risk_decision_for_action(&action, now_ms);
        if matches!(live_gate, CopyLiveGateDecision::Rejected { .. })
            || !matches!(risk_decision, CopySignalRiskDecision::Approved { .. })
        {
            return vec![CopyDryRunShadowRecord {
                action,
                live_gate,
                risk_decision,
                signal: None,
                ledger_entry: None,
                persistence_snapshot: self.persistence_snapshot(now_ms),
            }];
        }

        let ctx = StrategyContext {
            target_accounts: self.config.target_accounts.clone(),
            signal_ttl_ms: self.config.signal_ttl_ms,
        };
        let signals = self.strategy.signals_from_semantic_action(&ctx, &action);
        if signals.is_empty() {
            return vec![CopyDryRunShadowRecord {
                action,
                live_gate,
                risk_decision,
                signal: None,
                ledger_entry: None,
                persistence_snapshot: self.persistence_snapshot(now_ms),
            }];
        }

        signals
            .into_iter()
            .filter_map(|mut signal| {
                if action.kind.is_full_close() && !signal.order.reduce_only {
                    return None;
                }
                if let CopySignalRiskDecision::Approved {
                    reduce_only,
                    notional_usd,
                    ..
                } = risk_decision
                {
                    signal.order.reduce_only = reduce_only;
                    signal.order.notional_usd = notional_usd;
                    signal.order.apply_account_ratio = false;
                }
                let ledger_entry = ledger_entry_from_shadow_signal(
                    &self.config.local_account_id,
                    &action,
                    &signal,
                );
                self.ledger.push(ledger_entry.clone());
                Some(CopyDryRunShadowRecord {
                    action: action.clone(),
                    live_gate: live_gate.clone(),
                    risk_decision: risk_decision.clone(),
                    signal: Some(signal),
                    ledger_entry: Some(ledger_entry),
                    persistence_snapshot: self.persistence_snapshot(now_ms),
                })
            })
            .collect()
    }

    fn risk_decision_for_action(
        &self,
        action: &SemanticLeaderAction,
        now_ms: u64,
    ) -> CopySignalRiskDecision {
        let leader = self
            .strategy
            .leader_by_address
            .get(&action.leader_address.to_ascii_lowercase());
        let leader_copy_ratio = leader
            .map(|leader| {
                if leader.copy_ratio > 0.0 {
                    leader.copy_ratio
                } else {
                    self.strategy.config.default_copy_ratio
                }
            })
            .unwrap_or(self.strategy.config.default_copy_ratio);
        let mapped_close_notional_usd = self.mapped_close_notional_usd(action);
        if mapped_close_notional_usd.is_some_and(|notional| notional <= 0.0) {
            return CopySignalRiskDecision::Rejected {
                reason_code: "COPY_MAPPING_MISSING".to_string(),
            };
        }
        let is_mapped_close = mapped_close_notional_usd.is_some();
        let sizing_leader_notional_usd = if action.kind.is_full_close() {
            mapped_close_notional_usd.unwrap_or(action.leader_notional_usd)
        } else {
            action.leader_notional_usd
        };
        let current_effective_exposure_usd = action
            .kind
            .open_side()
            .map(|side| {
                if matches!(
                    evaluate_copy_live_gate(self.config.live_gate),
                    CopyLiveGateDecision::LiveAllowed
                ) {
                    self.ledger.submitted_effective_exposure_usd(
                        &self.config.local_account_id,
                        &action.coin,
                        side,
                    )
                } else {
                    self.ledger.effective_exposure_usd(
                        &self.config.local_account_id,
                        &action.coin,
                        side,
                    )
                }
            })
            .unwrap_or(0.0);
        evaluate_copy_signal_risk(CopySignalRiskInput {
            action,
            sizing: CopySizingInput {
                leader_notional_usd: sizing_leader_notional_usd,
                leader_copy_ratio: if action.kind.is_full_close() {
                    1.0
                } else {
                    leader_copy_ratio
                },
                account_copy_ratio: if action.kind.is_full_close() {
                    1.0
                } else {
                    self.config.account_copy_ratio
                },
                principal_cap_usd: if action.kind.is_full_close() {
                    None
                } else {
                    Some(self.config.principal_cap_usd)
                },
                leverage: if action.kind.is_full_close() {
                    1.0
                } else {
                    self.config.leverage.min(COPY_MAX_LEVERAGE)
                },
                leader_trade_cap_usd: None,
                symbol_order_cap_usd: if is_mapped_close {
                    None
                } else {
                    self.config.max_signal_notional_usd
                },
                account_order_cap_usd: None,
                remaining_symbol_position_cap_usd: mapped_close_notional_usd,
                remaining_daily_cap_usd: None,
                exchange_min_open_notional_usd: self.config.exchange_min_open_notional_usd,
                reduce_only: action.kind.close_side().is_some()
                    && action.kind.open_side().is_none(),
            },
            now_ms,
            max_signal_delay_ms: self.config.max_signal_delay_ms,
            leader_enabled: leader.is_some_and(|leader| leader.enabled),
            symbol_blocked: self
                .config
                .blocked_symbols
                .iter()
                .any(|symbol| symbol.eq_ignore_ascii_case(&action.coin)),
            allow_short: self.config.allow_short,
            current_effective_exposure_usd,
            max_effective_exposure_usd: self.config.max_effective_exposure_usd,
        })
    }

    fn mapped_close_notional_usd(&self, action: &SemanticLeaderAction) -> Option<f64> {
        let close_side = action.kind.close_side()?;
        Some(self.ledger.mapped_close_notional_usd(
            &self.config.local_account_id,
            &action.coin,
            close_side,
        ))
    }
}

fn ledger_entry_from_shadow_signal(
    local_account_id: &str,
    action: &SemanticLeaderAction,
    signal: &CoordinatorSignal,
) -> CopyLedgerEntry {
    CopyLedgerEntry {
        local_account_id: local_account_id.to_string(),
        leader_id: action.leader_id.clone(),
        leader_group: action.leader_id.clone(),
        signal_id: signal.signal_id.clone(),
        coin: signal.order.coin.clone(),
        local_side: if signal.order.reduce_only {
            opposite_order_side(signal.order.side)
        } else {
            signal.order.side
        },
        order_cloid: None,
        order_oid: None,
        submitted_at_ms: None,
        filled_at_ms: None,
        planned_notional_usd: signal.order.notional_usd,
        pending_notional_usd: signal.order.notional_usd,
        filled_notional_usd: 0.0,
        remaining_notional_usd: if signal.order.reduce_only {
            signal.order.notional_usd
        } else {
            0.0
        },
        status: if signal.order.reduce_only && action.kind.is_full_close() {
            CopyLedgerStatus::PendingClose
        } else if signal.order.reduce_only {
            CopyLedgerStatus::PendingReduce
        } else {
            CopyLedgerStatus::PendingOpen
        },
    }
}

fn opposite_order_side(side: OrderSide) -> OrderSide {
    match side {
        OrderSide::Buy => OrderSide::Sell,
        OrderSide::Sell => OrderSide::Buy,
    }
}

fn position_key(leader_id: &str, coin: &str) -> String {
    format!("{leader_id}:{coin}")
}

fn same_position_scope(
    fill: &LeaderFillEvent,
    before: &LeaderPositionSnapshot,
    after: &LeaderPositionSnapshot,
) -> bool {
    before.leader_id == fill.leader_id
        && after.leader_id == fill.leader_id
        && before.coin == fill.coin
        && after.coin == fill.coin
}

fn normalize_position(value: f64) -> f64 {
    if value.abs() <= POSITION_EPS {
        0.0
    } else {
        value
    }
}

fn order_side_from_hyperliquid_fill(side: &str) -> Option<OrderSide> {
    match side.trim().to_ascii_lowercase().as_str() {
        "b" | "buy" => Some(OrderSide::Buy),
        "a" | "ask" | "s" | "sell" => Some(OrderSide::Sell),
        _ => None,
    }
}

fn parse_positive_f64(value: &str) -> Option<f64> {
    value
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite() && *value > 0.0)
}

fn dedupe_key(fill: &LeaderFillEvent) -> String {
    format!(
        "leader:{}:{}:{}:{}:{:.8}:{:.8}:{}:{}",
        fill.leader_id,
        fill.leader_address.to_ascii_lowercase(),
        fill.event_id,
        fill.coin,
        fill.price,
        fill.size,
        fill.exchange_time_ms,
        match fill.side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        }
    )
}

fn semantic_leg_dedupe_key(action: &SemanticLeaderAction, leg: &str) -> String {
    format!(
        "leader-semantic:{}:{}:{}:{}:{:?}:{}:{}",
        action.leader_id,
        action.leader_address.to_ascii_lowercase(),
        action.event_id,
        action.coin,
        action.kind,
        action.exchange_time_ms,
        leg
    )
}

#[cfg(test)]
mod tests {
    use crate::{
        config::{AccountConfig, AppConfig},
        domain::{OrderSide, OrderSubmitted, now_ms},
        hyperliquid::{OrderStatusInfo, OrderStatusOrder, OrderStatusResponse, UserFill},
        risk::{RiskContext, RiskDecision, RiskGateway},
        strategy::{LeaderFillEvent, Strategy, StrategyContext, StrategyEvent},
    };

    use super::{
        COPY_DEFAULT_MAX_SIGNAL_NOTIONAL_USD, COPY_DEFAULT_PRINCIPAL_CAP_USD, COPY_MAX_LEVERAGE,
        CopyConflictInput, CopyConflictResolution, CopyDryRunShadowConfig,
        CopyDryRunShadowPipeline, CopyLeaderWatcherEvent, CopyLedger, CopyLedgerEntry,
        CopyLedgerStatus, CopyLiveGateDecision, CopyLiveGateInput, CopyPersistenceSnapshot,
        CopyShadowHistoryEntry, CopySignalRiskDecision, CopySignalRiskInput, CopySizingDecision,
        CopySizingInput, LeaderActionConfidence, LeaderActionKind, LeaderPositionSnapshot,
        LeaderRule, SemanticLeaderAction, SmartMoneyCopyConfig, SmartMoneyCopyStrategy,
        SmartMoneyLeaderWatch, SymbolCopyLimit, append_copy_shadow_history_records,
        approved_shadow_record_to_trade_intent, calculate_copy_notional, classify_leader_fill,
        classify_position_delta, copy_watcher_ws_url, evaluate_copy_live_gate,
        evaluate_copy_signal_risk, leader_fill_event_from_user_fill,
        leader_position_snapshots_from_clearinghouse_state, load_copy_persistence_snapshot,
        parse_read_only_leader_watcher_message, read_only_leader_watcher_subscriptions,
        read_recent_copy_shadow_history_entries, resolve_copy_conflict,
        save_copy_persistence_snapshot,
    };

    #[test]
    fn leader_fill_emits_once_and_caps_notional() {
        let mut strategy = SmartMoneyCopyStrategy::new(SmartMoneyCopyConfig {
            strategy_id: "copy_main".to_string(),
            default_copy_ratio: 0.1,
            max_slippage_bps: 25.0,
            leaders: vec![LeaderRule {
                leader_id: "leader_a".to_string(),
                leader_address: "0xabc".to_string(),
                enabled: true,
                copy_ratio: 0.5,
            }],
            symbol_limits: vec![SymbolCopyLimit {
                coin: "xyz:XYZ100".to_string(),
                max_signal_notional_usd: 20.0,
            }],
        });
        let ctx = StrategyContext {
            target_accounts: vec!["addr_a".to_string(), "addr_b".to_string()],
            signal_ttl_ms: 3000,
        };
        let event = StrategyEvent::LeaderFill(LeaderFillEvent {
            event_id: "fill-1".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0xABC".to_string(),
            coin: "xyz:XYZ100".to_string(),
            side: OrderSide::Buy,
            price: 10.0,
            size: 10.0,
            notional_usd: 100.0,
            reduce_only: false,
            exchange_time_ms: now_ms(),
            received_at_ms: now_ms(),
        });

        let first = strategy.on_event(&ctx, event.clone());
        let second = strategy.on_event(&ctx, event);

        assert_eq!(first.len(), 1);
        assert!(second.is_empty());
        assert_eq!(first[0].target_accounts.len(), 2);
        assert_eq!(first[0].order.notional_usd, 20.0);
        assert_eq!(strategy.id(), "copy_main");
    }

    #[test]
    fn position_delta_classifies_core_copy_actions() {
        assert_eq!(
            classify_position_delta(0.0, 2.0),
            LeaderActionKind::OpenLong
        );
        assert_eq!(
            classify_position_delta(2.0, 3.0),
            LeaderActionKind::IncreaseLong
        );
        assert_eq!(
            classify_position_delta(3.0, 1.0),
            LeaderActionKind::ReduceLong
        );
        assert_eq!(
            classify_position_delta(1.0, 0.0),
            LeaderActionKind::CloseLong
        );
        assert_eq!(
            classify_position_delta(1.0, -1.0),
            LeaderActionKind::FlipLongToShort
        );
        assert_eq!(
            classify_position_delta(0.0, -2.0),
            LeaderActionKind::OpenShort
        );
        assert_eq!(
            classify_position_delta(-2.0, -3.0),
            LeaderActionKind::IncreaseShort
        );
        assert_eq!(
            classify_position_delta(-3.0, -1.0),
            LeaderActionKind::ReduceShort
        );
        assert_eq!(
            classify_position_delta(-1.0, 0.0),
            LeaderActionKind::CloseShort
        );
        assert_eq!(
            classify_position_delta(-1.0, 1.0),
            LeaderActionKind::FlipShortToLong
        );
    }

    #[test]
    fn classify_fill_fails_closed_without_matching_position_snapshots() {
        let fill = leader_fill("fill-ambiguous", "leader_a", OrderSide::Buy, 100.0);
        let action = classify_leader_fill(&fill, None, None);

        assert_eq!(action.kind, LeaderActionKind::Ambiguous);
        assert_eq!(action.confidence, LeaderActionConfidence::Ambiguous);
        assert!(action.reason.contains("missing"));
    }

    #[test]
    fn classify_fill_uses_position_delta_not_raw_side() {
        let fill = leader_fill("fill-close-short", "leader_a", OrderSide::Buy, 100.0);
        let before = leader_position("leader_a", "xyz:XYZ100", -5.0);
        let after = leader_position("leader_a", "xyz:XYZ100", 0.0);

        let action = classify_leader_fill(&fill, Some(&before), Some(&after));

        assert_eq!(action.kind, LeaderActionKind::CloseShort);
        assert_eq!(action.kind.close_side(), Some(OrderSide::Buy));
        assert_eq!(action.confidence, LeaderActionConfidence::Strong);
    }

    #[test]
    fn conflict_resolver_skips_balanced_opposite_opens() {
        let events = vec![
            conflict_event(
                "buy-1",
                "leader_a",
                "group_a",
                LeaderActionKind::OpenLong,
                100.0,
                1.0,
            ),
            conflict_event(
                "sell-1",
                "leader_b",
                "group_b",
                LeaderActionKind::OpenShort,
                90.0,
                1.0,
            ),
        ];

        let decision = resolve_copy_conflict(&events, 1.5, true);

        match decision {
            CopyConflictResolution::Skip {
                reason_code,
                long_score,
                short_score,
                ..
            } => {
                assert_eq!(reason_code, "COPY_CONFLICT_NO_DECISION");
                assert_eq!(long_score, 100.0);
                assert_eq!(short_score, 90.0);
            }
            other => panic!("expected conflict skip, got {other:?}"),
        }
    }

    #[test]
    fn conflict_resolver_allows_weighted_direction_winner() {
        let events = vec![
            conflict_event(
                "buy-1",
                "leader_a",
                "group_a",
                LeaderActionKind::OpenLong,
                100.0,
                2.0,
            ),
            conflict_event(
                "sell-1",
                "leader_b",
                "group_b",
                LeaderActionKind::OpenShort,
                90.0,
                1.0,
            ),
        ];

        let decision = resolve_copy_conflict(&events, 1.5, true);

        assert_eq!(
            decision,
            CopyConflictResolution::FollowOpen {
                side: OrderSide::Buy,
                score: 200.0,
                notional_usd: 100.0,
                event_ids: vec!["buy-1".to_string()],
            }
        );
    }

    #[test]
    fn conflict_resolver_dedupes_same_leader_group_votes() {
        let events = vec![
            conflict_event(
                "buy-1",
                "leader_a",
                "group_a",
                LeaderActionKind::OpenLong,
                100.0,
                1.0,
            ),
            conflict_event(
                "buy-2",
                "leader_b",
                "group_a",
                LeaderActionKind::OpenLong,
                250.0,
                1.0,
            ),
            conflict_event(
                "sell-1",
                "leader_c",
                "group_c",
                LeaderActionKind::OpenShort,
                200.0,
                1.0,
            ),
        ];

        let decision = resolve_copy_conflict(&events, 1.2, true);

        assert_eq!(
            decision,
            CopyConflictResolution::FollowOpen {
                side: OrderSide::Buy,
                score: 250.0,
                notional_usd: 250.0,
                event_ids: vec!["buy-2".to_string()],
            }
        );
    }

    #[test]
    fn conflict_resolver_close_overrides_new_open() {
        let events = vec![
            conflict_event(
                "buy-1",
                "leader_a",
                "group_a",
                LeaderActionKind::OpenLong,
                500.0,
                1.0,
            ),
            conflict_event(
                "close-1",
                "leader_b",
                "group_b",
                LeaderActionKind::CloseLong,
                50.0,
                1.0,
            ),
        ];

        let decision = resolve_copy_conflict(&events, 2.0, true);

        assert_eq!(
            decision,
            CopyConflictResolution::FollowClose {
                side: OrderSide::Sell,
                event_ids: vec!["buy-1".to_string(), "close-1".to_string()],
            }
        );
    }

    #[test]
    fn semantic_open_action_becomes_copy_open_signal() {
        let mut strategy = copy_strategy();
        let ctx = strategy_context();
        let action = semantic_action(
            "open-1",
            LeaderActionKind::OpenLong,
            LeaderActionConfidence::Strong,
            100.0,
        );

        let signal = strategy
            .signal_from_semantic_action(&ctx, &action)
            .expect("semantic open should emit a signal");

        assert_eq!(signal.order.side, OrderSide::Buy);
        assert!(!signal.order.reduce_only);
        assert_eq!(signal.order.notional_usd, 20.0);
        assert_eq!(signal.order.market.as_deref(), Some("xyz_perp"));
        assert_eq!(signal.order.dex.as_deref(), Some("xyz"));
    }

    #[test]
    fn semantic_close_action_becomes_reduce_only_signal() {
        let mut strategy = copy_strategy();
        let ctx = strategy_context();
        let action = semantic_action(
            "close-1",
            LeaderActionKind::CloseLong,
            LeaderActionConfidence::Strong,
            100.0,
        );

        let signal = strategy
            .signal_from_semantic_action(&ctx, &action)
            .expect("semantic close should emit a signal");

        assert_eq!(signal.order.side, OrderSide::Sell);
        assert!(signal.order.reduce_only);
    }

    #[test]
    fn ambiguous_semantic_action_does_not_emit_signal() {
        let mut strategy = copy_strategy();
        let ctx = strategy_context();
        let action = semantic_action(
            "ambiguous-1",
            LeaderActionKind::Ambiguous,
            LeaderActionConfidence::Ambiguous,
            100.0,
        );

        assert!(
            strategy
                .signal_from_semantic_action(&ctx, &action)
                .is_none()
        );
    }

    #[test]
    fn semantic_action_is_deduped() {
        let mut strategy = copy_strategy();
        let ctx = strategy_context();
        let action = semantic_action(
            "open-1",
            LeaderActionKind::OpenLong,
            LeaderActionConfidence::Strong,
            100.0,
        );

        assert!(
            strategy
                .signal_from_semantic_action(&ctx, &action)
                .is_some()
        );
        assert!(
            strategy
                .signal_from_semantic_action(&ctx, &action)
                .is_none()
        );
    }

    #[test]
    fn semantic_action_replay_is_deduped_after_restart_snapshot() {
        let ctx = strategy_context();
        let action = semantic_action(
            "replay-open-1",
            LeaderActionKind::OpenLong,
            LeaderActionConfidence::Strong,
            100.0,
        );
        let mut first_process = copy_strategy();
        let signal = first_process
            .signal_from_semantic_action(&ctx, &action)
            .expect("first replay event should emit");
        let snapshot = first_process.persistence_snapshot(123456, &CopyLedger::new());

        let mut restarted = SmartMoneyCopyStrategy::new_with_seen_event_keys(
            copy_config(),
            snapshot.seen_event_keys,
        );
        let duplicate = restarted.signal_from_semantic_action(&ctx, &action);
        let fresh = restarted.signal_from_semantic_action(
            &ctx,
            &semantic_action(
                "replay-open-2",
                LeaderActionKind::OpenLong,
                LeaderActionConfidence::Strong,
                100.0,
            ),
        );

        assert!(signal.dedupe_key.ends_with(":open"));
        assert!(duplicate.is_none());
        assert!(fresh.is_some());
    }

    #[test]
    fn semantic_flip_action_emits_close_then_open_legs() {
        let mut strategy = copy_strategy();
        let ctx = strategy_context();
        let mut action = semantic_action(
            "flip-1",
            LeaderActionKind::FlipLongToShort,
            LeaderActionConfidence::Strong,
            180.0,
        );
        action.close_leader_notional_usd = Some(120.0);
        action.open_leader_notional_usd = Some(60.0);

        let signals = strategy.signals_from_semantic_action(&ctx, &action);
        let duplicate = strategy.signals_from_semantic_action(&ctx, &action);

        assert_eq!(signals.len(), 2);
        assert_eq!(signals[0].order.side, OrderSide::Sell);
        assert!(signals[0].order.reduce_only);
        assert_eq!(signals[0].order.notional_usd, 24.0);
        assert!(signals[0].dedupe_key.ends_with(":flip-close"));
        assert_eq!(signals[1].order.side, OrderSide::Sell);
        assert!(!signals[1].order.reduce_only);
        assert_eq!(signals[1].order.notional_usd, 12.0);
        assert!(signals[1].dedupe_key.ends_with(":flip-open"));
        assert!(duplicate.is_empty());
    }

    #[test]
    fn copy_sizing_applies_all_caps_in_order_of_strictness() {
        let decision = calculate_copy_notional(CopySizingInput {
            leader_notional_usd: 1_000.0,
            leader_copy_ratio: 0.5,
            account_copy_ratio: 0.5,
            principal_cap_usd: None,
            leverage: 1.0,
            leader_trade_cap_usd: Some(240.0),
            symbol_order_cap_usd: Some(200.0),
            account_order_cap_usd: Some(150.0),
            remaining_symbol_position_cap_usd: Some(125.0),
            remaining_daily_cap_usd: Some(100.0),
            exchange_min_open_notional_usd: 10.0,
            reduce_only: false,
        });

        assert_eq!(
            decision,
            CopySizingDecision::Approved {
                notional_usd: 100.0,
            }
        );
    }

    #[test]
    fn copy_sizing_caps_principal_before_applying_leverage() {
        let decision = calculate_copy_notional(CopySizingInput {
            leader_notional_usd: 1_000.0,
            leader_copy_ratio: 0.1,
            account_copy_ratio: 1.0,
            principal_cap_usd: Some(COPY_DEFAULT_PRINCIPAL_CAP_USD),
            leverage: COPY_MAX_LEVERAGE,
            leader_trade_cap_usd: None,
            symbol_order_cap_usd: None,
            account_order_cap_usd: None,
            remaining_symbol_position_cap_usd: None,
            remaining_daily_cap_usd: None,
            exchange_min_open_notional_usd: 10.0,
            reduce_only: false,
        });

        assert_eq!(
            decision,
            CopySizingDecision::Approved {
                notional_usd: COPY_DEFAULT_MAX_SIGNAL_NOTIONAL_USD,
            }
        );
    }

    #[test]
    fn copy_sizing_rejects_open_below_exchange_minimum() {
        let decision = calculate_copy_notional(CopySizingInput {
            leader_notional_usd: 100.0,
            leader_copy_ratio: 0.05,
            account_copy_ratio: 1.0,
            principal_cap_usd: None,
            leverage: 1.0,
            leader_trade_cap_usd: None,
            symbol_order_cap_usd: None,
            account_order_cap_usd: None,
            remaining_symbol_position_cap_usd: None,
            remaining_daily_cap_usd: None,
            exchange_min_open_notional_usd: 10.0,
            reduce_only: false,
        });

        assert_eq!(
            decision,
            CopySizingDecision::Rejected {
                reason_code: "COPY_NOTIONAL_TOO_SMALL".to_string(),
            }
        );
    }

    #[test]
    fn copy_sizing_allows_small_reduce_only_close() {
        let decision = calculate_copy_notional(CopySizingInput {
            leader_notional_usd: 20.0,
            leader_copy_ratio: 0.1,
            account_copy_ratio: 1.0,
            principal_cap_usd: None,
            leverage: 1.0,
            leader_trade_cap_usd: None,
            symbol_order_cap_usd: None,
            account_order_cap_usd: None,
            remaining_symbol_position_cap_usd: None,
            remaining_daily_cap_usd: None,
            exchange_min_open_notional_usd: 10.0,
            reduce_only: true,
        });

        assert_eq!(decision, CopySizingDecision::Approved { notional_usd: 2.0 });
    }

    #[test]
    fn copy_risk_rejects_disabled_or_stale_leader_signal() {
        let action = semantic_action(
            "risk-open-1",
            LeaderActionKind::OpenLong,
            LeaderActionConfidence::Strong,
            100.0,
        );

        assert_eq!(
            evaluate_copy_signal_risk(CopySignalRiskInput {
                leader_enabled: false,
                ..risk_input(&action)
            }),
            CopySignalRiskDecision::Rejected {
                reason_code: "COPY_LEADER_DISABLED".to_string(),
            }
        );
        assert_eq!(
            evaluate_copy_signal_risk(CopySignalRiskInput {
                now_ms: action.received_at_ms + 10_001,
                max_signal_delay_ms: 10_000,
                ..risk_input(&action)
            }),
            CopySignalRiskDecision::Rejected {
                reason_code: "COPY_SIGNAL_TOO_OLD".to_string(),
            }
        );
    }

    #[test]
    fn copy_risk_rejects_ambiguous_and_blocklisted_symbols() {
        let ambiguous = semantic_action(
            "risk-ambiguous-1",
            LeaderActionKind::Ambiguous,
            LeaderActionConfidence::Ambiguous,
            100.0,
        );
        let open = semantic_action(
            "risk-open-2",
            LeaderActionKind::OpenLong,
            LeaderActionConfidence::Strong,
            100.0,
        );

        assert_eq!(
            evaluate_copy_signal_risk(risk_input(&ambiguous)),
            CopySignalRiskDecision::Rejected {
                reason_code: "COPY_ACTION_AMBIGUOUS".to_string(),
            }
        );
        assert_eq!(
            evaluate_copy_signal_risk(CopySignalRiskInput {
                symbol_blocked: true,
                ..risk_input(&open)
            }),
            CopySignalRiskDecision::Rejected {
                reason_code: "COPY_SYMBOL_BLOCKED".to_string(),
            }
        );
    }

    #[test]
    fn copy_risk_rejects_short_when_account_disallows_shorting() {
        let action = semantic_action(
            "risk-short-1",
            LeaderActionKind::OpenShort,
            LeaderActionConfidence::Strong,
            100.0,
        );

        assert_eq!(
            evaluate_copy_signal_risk(CopySignalRiskInput {
                allow_short: false,
                ..risk_input(&action)
            }),
            CopySignalRiskDecision::Rejected {
                reason_code: "COPY_SHORT_NOT_ALLOWED".to_string(),
            }
        );
    }

    #[test]
    fn copy_risk_uses_pending_exposure_limit_before_opening() {
        let action = semantic_action(
            "risk-exposure-1",
            LeaderActionKind::OpenLong,
            LeaderActionConfidence::Strong,
            1_000.0,
        );

        assert_eq!(
            evaluate_copy_signal_risk(CopySignalRiskInput {
                current_effective_exposure_usd: 95.0,
                max_effective_exposure_usd: Some(100.0),
                ..risk_input(&action)
            }),
            CopySignalRiskDecision::Rejected {
                reason_code: "COPY_PENDING_EXPOSURE_LIMIT".to_string(),
            }
        );
        assert_eq!(
            evaluate_copy_signal_risk(CopySignalRiskInput {
                current_effective_exposure_usd: 80.0,
                max_effective_exposure_usd: Some(100.0),
                ..risk_input(&action)
            }),
            CopySignalRiskDecision::Approved {
                side: OrderSide::Buy,
                reduce_only: false,
                notional_usd: 20.0,
            }
        );
    }

    #[test]
    fn copy_risk_propagates_sizing_rejections_for_new_opens() {
        let action = semantic_action(
            "risk-sizing-1",
            LeaderActionKind::OpenLong,
            LeaderActionConfidence::Strong,
            20.0,
        );

        assert_eq!(
            evaluate_copy_signal_risk(CopySignalRiskInput {
                sizing: CopySizingInput {
                    leader_notional_usd: action.leader_notional_usd,
                    leader_copy_ratio: 0.1,
                    account_copy_ratio: 1.0,
                    principal_cap_usd: None,
                    leverage: 1.0,
                    leader_trade_cap_usd: None,
                    symbol_order_cap_usd: None,
                    account_order_cap_usd: None,
                    remaining_symbol_position_cap_usd: None,
                    remaining_daily_cap_usd: None,
                    exchange_min_open_notional_usd: 10.0,
                    reduce_only: false,
                },
                ..risk_input(&action)
            }),
            CopySignalRiskDecision::Rejected {
                reason_code: "COPY_NOTIONAL_TOO_SMALL".to_string(),
            }
        );
    }

    #[test]
    fn copy_risk_allows_reduce_only_close_below_open_minimum() {
        let action = semantic_action(
            "risk-close-1",
            LeaderActionKind::CloseLong,
            LeaderActionConfidence::Strong,
            20.0,
        );

        assert_eq!(
            evaluate_copy_signal_risk(CopySignalRiskInput {
                max_effective_exposure_usd: Some(0.0),
                sizing: CopySizingInput {
                    leader_notional_usd: action.leader_notional_usd,
                    leader_copy_ratio: 0.1,
                    account_copy_ratio: 1.0,
                    principal_cap_usd: None,
                    leverage: 1.0,
                    leader_trade_cap_usd: None,
                    symbol_order_cap_usd: None,
                    account_order_cap_usd: None,
                    remaining_symbol_position_cap_usd: None,
                    remaining_daily_cap_usd: None,
                    exchange_min_open_notional_usd: 10.0,
                    reduce_only: false,
                },
                ..risk_input(&action)
            }),
            CopySignalRiskDecision::Approved {
                side: OrderSide::Sell,
                reduce_only: true,
                notional_usd: 2.0,
            }
        );
    }

    #[test]
    fn copy_live_gate_requires_dry_run_or_explicit_live_enablement() {
        assert_eq!(
            evaluate_copy_live_gate(CopyLiveGateInput {
                process_dry_run: true,
                live_copy_enabled: false,
                account_worker_live: false,
            }),
            CopyLiveGateDecision::DryRunOnly
        );
        assert_eq!(
            evaluate_copy_live_gate(CopyLiveGateInput {
                process_dry_run: false,
                live_copy_enabled: false,
                account_worker_live: true,
            }),
            CopyLiveGateDecision::Rejected {
                reason_code: "COPY_LIVE_GATE_DISABLED".to_string(),
            }
        );
        assert_eq!(
            evaluate_copy_live_gate(CopyLiveGateInput {
                process_dry_run: false,
                live_copy_enabled: true,
                account_worker_live: false,
            }),
            CopyLiveGateDecision::Rejected {
                reason_code: "COPY_ACCOUNT_WORKER_NOT_LIVE".to_string(),
            }
        );
        assert_eq!(
            evaluate_copy_live_gate(CopyLiveGateInput {
                process_dry_run: false,
                live_copy_enabled: true,
                account_worker_live: true,
            }),
            CopyLiveGateDecision::LiveAllowed
        );
    }

    #[test]
    fn dry_run_shadow_records_would_copy_without_live_submission() {
        let now = now_ms();
        let mut pipeline = CopyDryRunShadowPipeline::new(
            dry_run_shadow_config(),
            copy_strategy(),
            CopyLedger::new(),
        );

        let initial_records =
            pipeline.handle_watcher_event(position_event("leader_a", 1.0, 10.0), now);
        assert!(initial_records.is_empty());

        let fill_records = pipeline.handle_watcher_event(
            CopyLeaderWatcherEvent::Fill {
                leader_id: "leader_a".to_string(),
                leader_address: "0xABC".to_string(),
                fill: leader_fill("shadow-fill-1", "leader_a", OrderSide::Buy, 100.0),
                is_snapshot: false,
            },
            now + 1,
        );
        assert!(fill_records.is_empty());

        let records = pipeline.handle_watcher_event(position_event("leader_a", 2.0, 20.0), now + 2);

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].action.kind, LeaderActionKind::IncreaseLong);
        assert_eq!(records[0].live_gate, CopyLiveGateDecision::DryRunOnly);
        assert_eq!(
            records[0].risk_decision,
            CopySignalRiskDecision::Approved {
                side: OrderSide::Buy,
                reduce_only: false,
                notional_usd: COPY_DEFAULT_MAX_SIGNAL_NOTIONAL_USD,
            }
        );
        let signal = records[0].signal.as_ref().expect("would-copy signal");
        assert_eq!(signal.order.side, OrderSide::Buy);
        assert!(!signal.order.reduce_only);
        assert_eq!(
            signal.order.notional_usd,
            COPY_DEFAULT_MAX_SIGNAL_NOTIONAL_USD
        );
        let ledger_entry = records[0].ledger_entry.as_ref().expect("ledger entry");
        assert_eq!(ledger_entry.status, CopyLedgerStatus::PendingOpen);
        assert_eq!(
            ledger_entry.pending_notional_usd,
            COPY_DEFAULT_MAX_SIGNAL_NOTIONAL_USD
        );
        assert_eq!(
            pipeline
                .ledger()
                .effective_exposure_usd("addr_a", "xyz:XYZ100", OrderSide::Buy),
            COPY_DEFAULT_MAX_SIGNAL_NOTIONAL_USD
        );
        assert!(
            records[0]
                .persistence_snapshot
                .seen_event_keys
                .iter()
                .any(|key| key.contains("shadow-fill-1") && key.ends_with(":open"))
        );
        assert_eq!(records[0].persistence_snapshot.ledger_entries.len(), 1);
    }

    #[test]
    fn dry_run_shadow_caps_approved_open_signal_to_configured_max() {
        let now = now_ms();
        let mut config = dry_run_shadow_config();
        config.max_signal_notional_usd = Some(12.0);
        config.max_effective_exposure_usd = Some(1_000.0);
        let mut pipeline =
            CopyDryRunShadowPipeline::new(config, copy_strategy(), CopyLedger::new());

        assert!(
            pipeline
                .handle_watcher_event(position_event("leader_a", 1.0, 10.0), now)
                .is_empty()
        );
        assert!(
            pipeline
                .handle_watcher_event(
                    CopyLeaderWatcherEvent::Fill {
                        leader_id: "leader_a".to_string(),
                        leader_address: "0xABC".to_string(),
                        fill: leader_fill("shadow-fill-capped", "leader_a", OrderSide::Buy, 1000.0),
                        is_snapshot: false,
                    },
                    now + 1,
                )
                .is_empty()
        );
        let records =
            pipeline.handle_watcher_event(position_event("leader_a", 2.0, 1000.0), now + 2);

        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].risk_decision,
            CopySignalRiskDecision::Approved {
                side: OrderSide::Buy,
                reduce_only: false,
                notional_usd: 12.0,
            }
        );
        let signal = records[0].signal.as_ref().expect("would-copy signal");
        assert_eq!(signal.order.notional_usd, 12.0);
        assert!(!signal.order.apply_account_ratio);
        let ledger_entry = records[0].ledger_entry.as_ref().expect("ledger entry");
        assert_eq!(ledger_entry.pending_notional_usd, 12.0);
        assert_eq!(
            pipeline
                .ledger()
                .effective_exposure_usd("addr_a", "xyz:XYZ100", OrderSide::Buy),
            12.0
        );
    }

    #[test]
    fn dry_run_shadow_rejects_when_live_gate_is_not_dry_run_or_live_ready() {
        let now = now_ms();
        let mut config = dry_run_shadow_config();
        config.live_gate = CopyLiveGateInput {
            process_dry_run: false,
            live_copy_enabled: false,
            account_worker_live: false,
        };
        let mut pipeline =
            CopyDryRunShadowPipeline::new(config, copy_strategy(), CopyLedger::new());

        assert!(
            pipeline
                .handle_watcher_event(position_event("leader_a", 1.0, 10.0), now)
                .is_empty()
        );
        assert!(
            pipeline
                .handle_watcher_event(
                    CopyLeaderWatcherEvent::Fill {
                        leader_id: "leader_a".to_string(),
                        leader_address: "0xABC".to_string(),
                        fill: leader_fill("shadow-fill-2", "leader_a", OrderSide::Buy, 100.0),
                        is_snapshot: false,
                    },
                    now + 1,
                )
                .is_empty()
        );
        let records = pipeline.handle_watcher_event(position_event("leader_a", 2.0, 20.0), now + 2);

        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].live_gate,
            CopyLiveGateDecision::Rejected {
                reason_code: "COPY_LIVE_GATE_DISABLED".to_string(),
            }
        );
        assert!(records[0].signal.is_none());
        assert!(records[0].ledger_entry.is_none());
        assert!(records[0].persistence_snapshot.ledger_entries.is_empty());
    }

    #[test]
    fn dry_run_shadow_generates_candidate_when_live_gate_is_allowed() {
        let now = now_ms();
        let mut config = dry_run_shadow_config();
        config.live_gate = CopyLiveGateInput {
            process_dry_run: false,
            live_copy_enabled: true,
            account_worker_live: true,
        };
        let mut pipeline =
            CopyDryRunShadowPipeline::new(config, copy_strategy(), CopyLedger::new());

        assert!(
            pipeline
                .handle_watcher_event(position_event("leader_a", 1.0, 10.0), now)
                .is_empty()
        );
        assert!(
            pipeline
                .handle_watcher_event(
                    CopyLeaderWatcherEvent::Fill {
                        leader_id: "leader_a".to_string(),
                        leader_address: "0xABC".to_string(),
                        fill: leader_fill(
                            "shadow-live-allowed-1",
                            "leader_a",
                            OrderSide::Buy,
                            100.0
                        ),
                        is_snapshot: false,
                    },
                    now + 1,
                )
                .is_empty()
        );
        let records = pipeline.handle_watcher_event(position_event("leader_a", 2.0, 20.0), now + 2);

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].live_gate, CopyLiveGateDecision::LiveAllowed);
        assert!(records[0].signal.is_some());
        assert!(records[0].ledger_entry.is_some());
    }

    #[test]
    fn dry_run_shadow_rejects_blocked_symbol_without_ledger_update() {
        let now = now_ms();
        let mut config = dry_run_shadow_config();
        config.blocked_symbols = vec!["xyz:XYZ100".to_string()];
        let mut pipeline =
            CopyDryRunShadowPipeline::new(config, copy_strategy(), CopyLedger::new());

        assert!(
            pipeline
                .handle_watcher_event(position_event("leader_a", 1.0, 10.0), now)
                .is_empty()
        );
        assert!(
            pipeline
                .handle_watcher_event(
                    CopyLeaderWatcherEvent::Fill {
                        leader_id: "leader_a".to_string(),
                        leader_address: "0xABC".to_string(),
                        fill: leader_fill(
                            "shadow-risk-reject-1",
                            "leader_a",
                            OrderSide::Buy,
                            100.0
                        ),
                        is_snapshot: false,
                    },
                    now + 1,
                )
                .is_empty()
        );
        let records = pipeline.handle_watcher_event(position_event("leader_a", 2.0, 20.0), now + 2);

        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].risk_decision,
            CopySignalRiskDecision::Rejected {
                reason_code: "COPY_SYMBOL_BLOCKED".to_string(),
            }
        );
        assert!(records[0].signal.is_none());
        assert!(records[0].ledger_entry.is_none());
        assert_eq!(
            pipeline
                .ledger()
                .effective_exposure_usd("addr_a", "xyz:XYZ100", OrderSide::Buy),
            0.0
        );
    }

    #[test]
    fn dry_run_shadow_full_close_uses_mapped_local_exposure() {
        let now = now_ms();
        let mut ledger = CopyLedger::new();
        ledger.push(ledger_entry(
            "sig-open",
            OrderSide::Buy,
            75.0,
            0.0,
            75.0,
            75.0,
            CopyLedgerStatus::Open,
        ));
        let mut pipeline =
            CopyDryRunShadowPipeline::new(dry_run_shadow_config(), copy_strategy(), ledger);

        pipeline.handle_watcher_event(position_event("leader_a", 1.0, 10.0), now);
        pipeline.handle_watcher_event(
            CopyLeaderWatcherEvent::Fill {
                leader_id: "leader_a".to_string(),
                leader_address: "0xABC".to_string(),
                fill: leader_fill("shadow-full-close-1", "leader_a", OrderSide::Sell, 10.0),
                is_snapshot: false,
            },
            now + 1,
        );
        let records = pipeline.handle_watcher_event(position_event("leader_a", 0.0, 0.0), now + 2);

        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].risk_decision,
            CopySignalRiskDecision::Approved {
                side: OrderSide::Sell,
                reduce_only: true,
                notional_usd: 75.0,
            }
        );
        let signal = records[0].signal.as_ref().expect("full close signal");
        assert_eq!(signal.order.side, OrderSide::Sell);
        assert!(signal.order.reduce_only);
        assert_eq!(signal.order.notional_usd, 75.0);
        assert!(!signal.order.apply_account_ratio);
        let ledger_entry = records[0]
            .ledger_entry
            .as_ref()
            .expect("full close ledger entry");
        assert_eq!(ledger_entry.status, CopyLedgerStatus::PendingClose);
        assert_eq!(ledger_entry.pending_notional_usd, 75.0);
    }

    #[test]
    fn dry_run_shadow_full_close_rejects_without_mapped_exposure() {
        let now = now_ms();
        let mut pipeline = CopyDryRunShadowPipeline::new(
            dry_run_shadow_config(),
            copy_strategy(),
            CopyLedger::new(),
        );

        pipeline.handle_watcher_event(position_event("leader_a", 1.0, 10.0), now);
        pipeline.handle_watcher_event(
            CopyLeaderWatcherEvent::Fill {
                leader_id: "leader_a".to_string(),
                leader_address: "0xABC".to_string(),
                fill: leader_fill(
                    "shadow-full-close-missing-1",
                    "leader_a",
                    OrderSide::Sell,
                    10.0,
                ),
                is_snapshot: false,
            },
            now + 1,
        );
        let records = pipeline.handle_watcher_event(position_event("leader_a", 0.0, 0.0), now + 2);

        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].risk_decision,
            CopySignalRiskDecision::Rejected {
                reason_code: "COPY_MAPPING_MISSING".to_string(),
            }
        );
        assert!(records[0].signal.is_none());
        assert!(records[0].ledger_entry.is_none());
    }

    #[test]
    fn dry_run_shadow_partial_reduce_rejects_without_mapped_exposure() {
        let now = now_ms();
        let mut pipeline = CopyDryRunShadowPipeline::new(
            dry_run_shadow_config(),
            copy_strategy(),
            CopyLedger::new(),
        );

        pipeline.handle_watcher_event(position_event("leader_a", 3.0, 30.0), now);
        pipeline.handle_watcher_event(
            CopyLeaderWatcherEvent::Fill {
                leader_id: "leader_a".to_string(),
                leader_address: "0xABC".to_string(),
                fill: leader_fill(
                    "shadow-partial-reduce-missing-1",
                    "leader_a",
                    OrderSide::Sell,
                    10.0,
                ),
                is_snapshot: false,
            },
            now + 1,
        );
        let records = pipeline.handle_watcher_event(position_event("leader_a", 2.0, 20.0), now + 2);

        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].risk_decision,
            CopySignalRiskDecision::Rejected {
                reason_code: "COPY_MAPPING_MISSING".to_string(),
            }
        );
        assert!(records[0].signal.is_none());
        assert!(records[0].ledger_entry.is_none());
    }

    #[test]
    fn dry_run_shadow_partial_reduce_caps_to_mapped_local_exposure() {
        let now = now_ms();
        let mut ledger = CopyLedger::new();
        ledger.push(ledger_entry(
            "sig-open",
            OrderSide::Buy,
            8.0,
            0.0,
            8.0,
            8.0,
            CopyLedgerStatus::Open,
        ));
        let mut pipeline =
            CopyDryRunShadowPipeline::new(dry_run_shadow_config(), copy_strategy(), ledger);

        pipeline.handle_watcher_event(position_event("leader_a", 10.0, 100.0), now);
        pipeline.handle_watcher_event(
            CopyLeaderWatcherEvent::Fill {
                leader_id: "leader_a".to_string(),
                leader_address: "0xABC".to_string(),
                fill: leader_fill(
                    "shadow-partial-reduce-cap-1",
                    "leader_a",
                    OrderSide::Sell,
                    100.0,
                ),
                is_snapshot: false,
            },
            now + 1,
        );
        let records = pipeline.handle_watcher_event(position_event("leader_a", 1.0, 10.0), now + 2);

        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].risk_decision,
            CopySignalRiskDecision::Approved {
                side: OrderSide::Sell,
                reduce_only: true,
                notional_usd: 8.0,
            }
        );
        let signal = records[0].signal.as_ref().expect("partial reduce signal");
        assert_eq!(signal.order.side, OrderSide::Sell);
        assert!(signal.order.reduce_only);
        assert_eq!(signal.order.notional_usd, 8.0);
        assert!(!signal.order.apply_account_ratio);
        let ledger_entry = records[0]
            .ledger_entry
            .as_ref()
            .expect("partial reduce ledger entry");
        assert_eq!(ledger_entry.local_side, OrderSide::Buy);
        assert_eq!(ledger_entry.status, CopyLedgerStatus::PendingReduce);
        assert_eq!(ledger_entry.pending_notional_usd, 8.0);
        assert_eq!(
            pipeline
                .ledger()
                .effective_exposure_usd("addr_a", "xyz:XYZ100", OrderSide::Buy),
            0.0
        );
    }

    #[test]
    fn dry_run_shadow_replay_snapshot_is_observable_but_does_not_duplicate_ledger() {
        let now = now_ms();
        let mut first = CopyDryRunShadowPipeline::new(
            dry_run_shadow_config(),
            copy_strategy(),
            CopyLedger::new(),
        );
        let replay_fill = leader_fill("shadow-replay-1", "leader_a", OrderSide::Buy, 100.0);
        first.handle_watcher_event(position_event("leader_a", 1.0, 10.0), now);
        first.handle_watcher_event(
            CopyLeaderWatcherEvent::Fill {
                leader_id: "leader_a".to_string(),
                leader_address: "0xABC".to_string(),
                fill: replay_fill.clone(),
                is_snapshot: false,
            },
            now + 1,
        );
        let first_records =
            first.handle_watcher_event(position_event("leader_a", 2.0, 20.0), now + 2);
        assert_eq!(first_records.len(), 1);
        assert!(first_records[0].signal.is_some());

        let snapshot = first.persistence_snapshot(now + 3);
        let restarted_strategy = SmartMoneyCopyStrategy::new_with_seen_event_keys(
            copy_config(),
            snapshot.seen_event_keys.clone(),
        );
        let mut restarted = CopyDryRunShadowPipeline::new(
            dry_run_shadow_config(),
            restarted_strategy,
            snapshot.ledger(),
        );
        restarted.handle_watcher_event(position_event("leader_a", 1.0, 10.0), now + 4);
        restarted.handle_watcher_event(
            CopyLeaderWatcherEvent::Fill {
                leader_id: "leader_a".to_string(),
                leader_address: "0xABC".to_string(),
                fill: replay_fill,
                is_snapshot: true,
            },
            now + 5,
        );
        let replay_records =
            restarted.handle_watcher_event(position_event("leader_a", 2.0, 20.0), now + 6);

        assert!(replay_records.is_empty());
        assert_eq!(
            restarted
                .ledger()
                .effective_exposure_usd("addr_a", "xyz:XYZ100", OrderSide::Buy),
            COPY_DEFAULT_MAX_SIGNAL_NOTIONAL_USD
        );
    }

    #[test]
    fn copy_shadow_history_appends_and_reads_recent_shadow_records() {
        let now = now_ms();
        let dir = std::env::temp_dir().join(format!("trade_xyz_copy_shadow_{}", now));
        let path = dir.join("shadow.jsonl");
        let mut pipeline = CopyDryRunShadowPipeline::new(
            dry_run_shadow_config(),
            copy_strategy(),
            CopyLedger::new(),
        );
        pipeline.handle_watcher_event(position_event("leader_a", 1.0, 10.0), now);
        pipeline.handle_watcher_event(
            CopyLeaderWatcherEvent::Fill {
                leader_id: "leader_a".to_string(),
                leader_address: "0xABC".to_string(),
                fill: leader_fill("shadow-history-1", "leader_a", OrderSide::Buy, 100.0),
                is_snapshot: false,
            },
            now + 1,
        );
        let records = pipeline.handle_watcher_event(position_event("leader_a", 2.0, 20.0), now + 2);

        append_copy_shadow_history_records(&path, &records, now + 3)
            .expect("append shadow history");
        let entries: Vec<CopyShadowHistoryEntry> =
            read_recent_copy_shadow_history_entries(&path, 10).expect("read shadow history");

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].status, "would_copy");
        assert_eq!(entries[0].leader_id, "leader_a");
        assert_eq!(entries[0].coin, "xyz:XYZ100");
        assert_eq!(entries[0].action_event_id, "shadow-history-1");
        assert_eq!(entries[0].live_gate, "dry_run_only");
        assert_eq!(entries[0].side, Some(OrderSide::Buy));
        assert_eq!(
            entries[0].notional_usd,
            Some(COPY_DEFAULT_MAX_SIGNAL_NOTIONAL_USD)
        );
        assert_eq!(
            entries[0].ledger_status,
            Some(CopyLedgerStatus::PendingOpen)
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn approved_shadow_record_can_be_evaluated_by_risk_gateway_without_live_submission() {
        let now = now_ms();
        let mut pipeline = CopyDryRunShadowPipeline::new(
            dry_run_shadow_config(),
            copy_strategy(),
            CopyLedger::new(),
        );
        pipeline.handle_watcher_event(position_event("leader_a", 1.0, 10.0), now);
        pipeline.handle_watcher_event(
            CopyLeaderWatcherEvent::Fill {
                leader_id: "leader_a".to_string(),
                leader_address: "0xABC".to_string(),
                fill: leader_fill("shadow-risk-gateway-1", "leader_a", OrderSide::Buy, 100.0),
                is_snapshot: false,
            },
            now + 1,
        );
        let records = pipeline.handle_watcher_event(position_event("leader_a", 2.0, 20.0), now + 2);

        let intent =
            approved_shadow_record_to_trade_intent(&records[0], "addr_a", "worker-addr_a", 1.0)
                .expect("approved shadow record should become a dry-run intent");
        let account = risk_gateway_account();
        let ctx =
            RiskContext::from_account_for_module(&AppConfig::default(), &account, true, "copy");
        let decision = RiskGateway::dry_run_default().evaluate(&ctx, intent);

        match decision {
            RiskDecision::Approved(order) => {
                assert_eq!(order.account_id, "addr_a");
                assert_eq!(order.coin, "xyz:XYZ100");
                assert_eq!(order.notional_usd, COPY_DEFAULT_MAX_SIGNAL_NOTIONAL_USD);
                assert!(!order.reduce_only);
            }
            RiskDecision::Rejected(rejection) => {
                panic!("expected dry-run risk approval, got {rejection:?}")
            }
        }
    }

    #[test]
    fn rejected_shadow_record_does_not_create_risk_gateway_intent() {
        let now = now_ms();
        let mut config = dry_run_shadow_config();
        config.blocked_symbols = vec!["xyz:XYZ100".to_string()];
        let mut pipeline =
            CopyDryRunShadowPipeline::new(config, copy_strategy(), CopyLedger::new());
        pipeline.handle_watcher_event(position_event("leader_a", 1.0, 10.0), now);
        pipeline.handle_watcher_event(
            CopyLeaderWatcherEvent::Fill {
                leader_id: "leader_a".to_string(),
                leader_address: "0xABC".to_string(),
                fill: leader_fill(
                    "shadow-risk-gateway-reject-1",
                    "leader_a",
                    OrderSide::Buy,
                    100.0,
                ),
                is_snapshot: false,
            },
            now + 1,
        );
        let records = pipeline.handle_watcher_event(position_event("leader_a", 2.0, 20.0), now + 2);

        assert!(
            approved_shadow_record_to_trade_intent(&records[0], "addr_a", "worker-addr_a", 1.0,)
                .is_none()
        );
    }

    #[test]
    fn copy_ledger_counts_pending_open_before_fill_confirmation() {
        let mut ledger = CopyLedger::new();
        ledger.push(ledger_entry(
            "sig-pending",
            OrderSide::Buy,
            20.0,
            20.0,
            0.0,
            0.0,
            CopyLedgerStatus::PendingOpen,
        ));
        ledger.push(ledger_entry(
            "sig-open",
            OrderSide::Buy,
            30.0,
            0.0,
            30.0,
            30.0,
            CopyLedgerStatus::Open,
        ));

        assert_eq!(
            ledger.effective_exposure_usd("addr_a", "xyz:XYZ100", OrderSide::Buy),
            50.0
        );
    }

    #[test]
    fn copy_ledger_pending_close_reduces_effective_exposure() {
        let mut ledger = CopyLedger::new();
        ledger.push(ledger_entry(
            "sig-open",
            OrderSide::Buy,
            30.0,
            0.0,
            30.0,
            30.0,
            CopyLedgerStatus::Open,
        ));
        ledger.push(ledger_entry(
            "sig-close",
            OrderSide::Buy,
            10.0,
            10.0,
            0.0,
            0.0,
            CopyLedgerStatus::PendingClose,
        ));
        ledger.push(ledger_entry(
            "sig-closed",
            OrderSide::Buy,
            30.0,
            0.0,
            30.0,
            0.0,
            CopyLedgerStatus::Closed,
        ));

        assert_eq!(
            ledger.effective_exposure_usd("addr_a", "xyz:XYZ100", OrderSide::Buy),
            20.0
        );
    }

    #[test]
    fn copy_ledger_reconciles_filled_open_submission() {
        let mut ledger = CopyLedger::new();
        ledger.push(ledger_entry(
            "sig-open-fill",
            OrderSide::Buy,
            12.0,
            12.0,
            0.0,
            0.0,
            CopyLedgerStatus::PendingOpen,
        ));

        let report = order_submitted("sig-open-fill", OrderSide::Buy, 12.0, false);
        let result = ledger.apply_order_submission(&report);

        assert!(result.applied);
        assert_eq!(result.status, Some(CopyLedgerStatus::Open));
        assert_eq!(result.filled_notional_usd, 12.0);
        let entry = ledger
            .entries()
            .iter()
            .find(|entry| entry.signal_id == "sig-open-fill")
            .expect("open entry");
        assert_eq!(entry.status, CopyLedgerStatus::Open);
        assert_eq!(entry.order_cloid.as_deref(), Some(report.cloid.as_str()));
        assert_eq!(entry.order_oid, report.oid);
        assert_eq!(entry.pending_notional_usd, 0.0);
        assert_eq!(entry.remaining_notional_usd, 12.0);
        assert_eq!(
            ledger.effective_exposure_usd("addr_a", "xyz:XYZ100", OrderSide::Buy),
            12.0
        );
    }

    #[test]
    fn copy_ledger_reconciles_reduce_submission_against_open_exposure() {
        let mut ledger = CopyLedger::new();
        ledger.push(ledger_entry(
            "sig-open",
            OrderSide::Buy,
            30.0,
            0.0,
            30.0,
            30.0,
            CopyLedgerStatus::Open,
        ));
        ledger.push(ledger_entry(
            "sig-reduce-fill",
            OrderSide::Buy,
            10.0,
            10.0,
            0.0,
            0.0,
            CopyLedgerStatus::PendingReduce,
        ));

        let report = order_submitted("sig-reduce-fill", OrderSide::Sell, 10.0, true);
        let result = ledger.apply_order_submission(&report);

        assert!(result.applied);
        assert_eq!(result.status, Some(CopyLedgerStatus::Closed));
        assert_eq!(result.filled_notional_usd, 10.0);
        assert_eq!(result.consumed_notional_usd, 10.0);
        assert_eq!(
            ledger.effective_exposure_usd("addr_a", "xyz:XYZ100", OrderSide::Buy),
            20.0
        );
        let open = ledger
            .entries()
            .iter()
            .find(|entry| entry.signal_id == "sig-open")
            .expect("open entry");
        assert_eq!(open.status, CopyLedgerStatus::Open);
        assert_eq!(open.remaining_notional_usd, 20.0);
        let reduce = ledger
            .entries()
            .iter()
            .find(|entry| entry.signal_id == "sig-reduce-fill")
            .expect("reduce entry");
        assert_eq!(reduce.status, CopyLedgerStatus::Closed);
        assert_eq!(reduce.order_cloid.as_deref(), Some(report.cloid.as_str()));
    }

    #[test]
    fn copy_ledger_reconciles_carried_pending_reduce_when_accumulated_order_fills() {
        let mut ledger = CopyLedger::new();
        ledger.push(ledger_entry(
            "sig-open",
            OrderSide::Buy,
            50.0,
            0.0,
            50.0,
            50.0,
            CopyLedgerStatus::Open,
        ));
        ledger.push(ledger_entry(
            "sig-prior-reduce",
            OrderSide::Buy,
            7.4,
            7.4,
            0.0,
            7.4,
            CopyLedgerStatus::PendingReduce,
        ));
        ledger.push(ledger_entry(
            "sig-next-reduce",
            OrderSide::Buy,
            2.7,
            2.7,
            0.0,
            2.7,
            CopyLedgerStatus::PendingReduce,
        ));

        let report = order_submitted("sig-next-reduce", OrderSide::Sell, 10.1, true);
        let result = ledger.apply_order_submission(&report);

        assert!(result.applied);
        assert_eq!(result.status, Some(CopyLedgerStatus::Closed));
        assert_eq!(result.consumed_notional_usd, 10.1);
        let open = ledger
            .entries()
            .iter()
            .find(|entry| entry.signal_id == "sig-open")
            .expect("open entry");
        let prior_reduce = ledger
            .entries()
            .iter()
            .find(|entry| entry.signal_id == "sig-prior-reduce")
            .expect("prior reduce entry");
        let next_reduce = ledger
            .entries()
            .iter()
            .find(|entry| entry.signal_id == "sig-next-reduce")
            .expect("next reduce entry");
        assert_eq!(open.remaining_notional_usd, 39.9);
        assert_eq!(prior_reduce.status, CopyLedgerStatus::Closed);
        assert_eq!(prior_reduce.pending_notional_usd, 0.0);
        assert_eq!(next_reduce.status, CopyLedgerStatus::Closed);
        assert_eq!(next_reduce.pending_notional_usd, 0.0);
        assert_eq!(
            ledger.effective_exposure_usd("addr_a", "xyz:XYZ100", OrderSide::Buy),
            39.9
        );
    }

    #[test]
    fn copy_ledger_reconciliation_is_idempotent_for_duplicate_reports() {
        let mut ledger = CopyLedger::new();
        ledger.push(ledger_entry(
            "sig-open",
            OrderSide::Buy,
            30.0,
            0.0,
            30.0,
            30.0,
            CopyLedgerStatus::Open,
        ));
        ledger.push(ledger_entry(
            "sig-close-fill",
            OrderSide::Buy,
            30.0,
            30.0,
            0.0,
            0.0,
            CopyLedgerStatus::PendingClose,
        ));

        let report = order_submitted("sig-close-fill", OrderSide::Sell, 30.0, true);
        let first = ledger.apply_order_submission(&report);
        let second = ledger.apply_order_submission(&report);

        assert!(first.applied);
        assert_eq!(first.consumed_notional_usd, 30.0);
        assert!(second.applied);
        assert_eq!(
            second.reason_code.as_deref(),
            Some("COPY_LEDGER_ALREADY_RECONCILED")
        );
        assert_eq!(second.consumed_notional_usd, 0.0);
        assert_eq!(
            ledger.effective_exposure_usd("addr_a", "xyz:XYZ100", OrderSide::Buy),
            0.0
        );
    }

    #[test]
    fn copy_ledger_reconciliation_ignores_unowned_submission() {
        let mut ledger = CopyLedger::new();
        ledger.push(ledger_entry(
            "sig-owned",
            OrderSide::Buy,
            12.0,
            12.0,
            0.0,
            0.0,
            CopyLedgerStatus::PendingOpen,
        ));

        let report = order_submitted("sig-other", OrderSide::Buy, 12.0, false);
        let result = ledger.apply_order_submission(&report);

        assert!(!result.applied);
        assert_eq!(
            result.reason_code.as_deref(),
            Some("COPY_LEDGER_UNOWNED_REPORT")
        );
        let entry = ledger.entries().first().expect("owned entry");
        assert_eq!(entry.status, CopyLedgerStatus::PendingOpen);
        assert!(entry.order_cloid.is_none());
        assert_eq!(
            ledger.effective_exposure_usd("addr_a", "xyz:XYZ100", OrderSide::Buy),
            12.0
        );
    }

    #[test]
    fn copy_ledger_reconciles_filled_order_status_with_user_fills() {
        let mut ledger = CopyLedger::new();
        let cloid = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, b"status-cloid").to_string();
        let mut entry = ledger_entry(
            "sig-status-fill",
            OrderSide::Buy,
            12.0,
            12.0,
            0.0,
            0.0,
            CopyLedgerStatus::PendingOpen,
        );
        entry.order_cloid = Some(cloid.clone());
        ledger.push(entry);

        let status = order_status_response(
            "filled",
            "xyz:XYZ100",
            "B",
            "10.0",
            "1.2",
            4201,
            1_000,
            1_050,
            Some("0x".to_string() + &cloid.replace('-', "")),
        );
        let fills = vec![
            user_fill(4201, "xyz:XYZ100", "10.0", "0.5", 1_020),
            user_fill(4201, "xyz:XYZ100", "11.0", "0.5", 1_030),
        ];

        let result = ledger.apply_order_status_evidence("addr_a", "worker-addr_a", &status, &fills);

        assert!(result.applied);
        assert_eq!(result.status, Some(CopyLedgerStatus::Open));
        assert_eq!(result.filled_notional_usd, 10.5);
        let entry = ledger.entries().first().expect("ledger entry");
        assert_eq!(entry.status, CopyLedgerStatus::Open);
        assert_eq!(entry.order_cloid.as_deref(), Some(cloid.as_str()));
        assert_eq!(entry.order_oid, Some(4201));
        assert_eq!(entry.filled_at_ms, Some(1_030));
        assert_eq!(entry.remaining_notional_usd, 10.5);
    }

    #[test]
    fn copy_ledger_reconciles_order_status_by_owned_oid_without_fills() {
        let mut ledger = CopyLedger::new();
        let mut entry = ledger_entry(
            "sig-status-oid",
            OrderSide::Sell,
            20.0,
            20.0,
            0.0,
            0.0,
            CopyLedgerStatus::PendingOpen,
        );
        entry.order_oid = Some(4301);
        ledger.push(entry);

        let status = order_status_response(
            "filled",
            "xyz:XYZ100",
            "A",
            "5.0",
            "4.0",
            4301,
            2_000,
            2_010,
            None,
        );

        let result = ledger.apply_order_status_evidence("addr_a", "worker-addr_a", &status, &[]);

        assert!(result.applied);
        assert_eq!(result.status, Some(CopyLedgerStatus::Open));
        assert_eq!(result.filled_notional_usd, 20.0);
        let entry = ledger.entries().first().expect("ledger entry");
        assert_eq!(entry.status, CopyLedgerStatus::Open);
        assert_eq!(entry.order_oid, Some(4301));
        assert_eq!(entry.filled_at_ms, Some(2_010));
    }

    #[test]
    fn copy_ledger_order_status_ignores_unowned_or_wrong_side_evidence() {
        let mut ledger = CopyLedger::new();
        let mut entry = ledger_entry(
            "sig-status-owned",
            OrderSide::Buy,
            12.0,
            12.0,
            0.0,
            0.0,
            CopyLedgerStatus::PendingOpen,
        );
        entry.order_oid = Some(4401);
        ledger.push(entry);

        let unowned_status = order_status_response(
            "filled",
            "xyz:XYZ100",
            "B",
            "10.0",
            "1.2",
            9999,
            3_000,
            3_010,
            None,
        );
        let unowned =
            ledger.apply_order_status_evidence("addr_a", "worker-addr_a", &unowned_status, &[]);
        assert!(!unowned.applied);
        assert_eq!(
            unowned.reason_code.as_deref(),
            Some("COPY_LEDGER_UNOWNED_ORDER_STATUS")
        );

        let wrong_side_status = order_status_response(
            "filled",
            "xyz:XYZ100",
            "A",
            "10.0",
            "1.2",
            4401,
            3_000,
            3_010,
            None,
        );
        let wrong_side =
            ledger.apply_order_status_evidence("addr_a", "worker-addr_a", &wrong_side_status, &[]);
        assert!(!wrong_side.applied);
        assert_eq!(
            wrong_side.reason_code.as_deref(),
            Some("COPY_LEDGER_ORDER_SIDE_MISMATCH")
        );
        let entry = ledger.entries().first().expect("ledger entry");
        assert_eq!(entry.status, CopyLedgerStatus::PendingOpen);
        assert_eq!(entry.order_oid, Some(4401));
        assert!(entry.order_cloid.is_none());
    }

    #[test]
    fn copy_persistence_round_trips_seen_keys_and_ledger_entries() {
        let dir = std::env::temp_dir().join(format!(
            "trade_xyz_copy_persistence_{}",
            crate::domain::now_ms()
        ));
        let path = dir.join("copy-state.json");
        let mut ledger = CopyLedger::new();
        ledger.push(ledger_entry(
            "sig-b",
            OrderSide::Buy,
            40.0,
            0.0,
            40.0,
            40.0,
            CopyLedgerStatus::Open,
        ));
        ledger.push(ledger_entry(
            "sig-a",
            OrderSide::Sell,
            25.0,
            25.0,
            0.0,
            0.0,
            CopyLedgerStatus::PendingOpen,
        ));

        let snapshot = CopyPersistenceSnapshot::new(
            123456,
            vec![
                "leader:event-b".to_string(),
                "leader:event-a".to_string(),
                "leader:event-a".to_string(),
            ],
            &ledger,
        );
        save_copy_persistence_snapshot(&path, &snapshot).expect("save copy persistence");

        let loaded = load_copy_persistence_snapshot(&path).expect("load copy persistence");
        let recovered_ledger = loaded.ledger();

        assert_eq!(loaded.saved_at_ms, 123456);
        assert_eq!(
            loaded.seen_event_keys,
            vec!["leader:event-a".to_string(), "leader:event-b".to_string(),]
        );
        assert!(loaded.seen_event_key_set().contains("leader:event-a"));
        assert_eq!(loaded.ledger_entries.len(), 2);
        assert_eq!(loaded.ledger_entries[0].signal_id, "sig-a");
        assert_eq!(loaded.ledger_entries[1].signal_id, "sig-b");
        assert_eq!(
            recovered_ledger.effective_exposure_usd("addr_a", "xyz:XYZ100", OrderSide::Buy),
            40.0
        );
        assert_eq!(
            recovered_ledger.effective_exposure_usd("addr_a", "xyz:XYZ100", OrderSide::Sell),
            25.0
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn copy_persistence_missing_or_empty_file_loads_empty_snapshot() {
        let dir = std::env::temp_dir().join(format!(
            "trade_xyz_copy_persistence_empty_{}",
            crate::domain::now_ms()
        ));
        let missing_path = dir.join("missing.json");
        let empty_path = dir.join("empty.json");
        std::fs::create_dir_all(&dir).expect("test dir");
        std::fs::write(&empty_path, b"   \n").expect("empty state file");

        let missing = load_copy_persistence_snapshot(&missing_path).expect("missing snapshot");
        let empty = load_copy_persistence_snapshot(&empty_path).expect("empty snapshot");

        assert!(missing.seen_event_keys.is_empty());
        assert!(missing.ledger_entries.is_empty());
        assert!(empty.seen_event_keys.is_empty());
        assert!(empty.ledger_entries.is_empty());

        let _ = std::fs::remove_file(&empty_path);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn user_fill_adapter_builds_leader_fill_identity_and_notional() {
        let fill: crate::hyperliquid::UserFill = serde_json::from_str(
            r#"{
                "coin": "xyz:TSLA",
                "px": "12.5",
                "sz": "3",
                "side": "B",
                "time": 123456,
                "dir": "Open Long",
                "closedPnl": "0",
                "hash": "0xabc",
                "oid": 42,
                "crossed": true,
                "fee": "0.01"
            }"#,
        )
        .expect("user fill");

        let event =
            leader_fill_event_from_user_fill("leader_a", "0xLeader", &fill, 999).expect("event");

        assert_eq!(event.event_id, "0xabc:42:123456:xyz:TSLA");
        assert_eq!(event.leader_id, "leader_a");
        assert_eq!(event.leader_address, "0xLeader");
        assert_eq!(event.coin, "xyz:TSLA");
        assert_eq!(event.side, OrderSide::Buy);
        assert_eq!(event.price, 12.5);
        assert_eq!(event.size, 3.0);
        assert_eq!(event.notional_usd, 37.5);
        assert!(!event.reduce_only);
        assert_eq!(event.exchange_time_ms, 123456);
        assert_eq!(event.received_at_ms, 999);
    }

    #[test]
    fn user_fill_adapter_marks_close_dir_reduce_only() {
        let fill: crate::hyperliquid::UserFill = serde_json::from_str(
            r#"{
                "coin": "xyz:TSLA",
                "px": "12.5",
                "sz": "3",
                "side": "A",
                "time": 123456,
                "dir": "Close Long",
                "closedPnl": "1.2",
                "hash": "0xdef",
                "oid": 43,
                "crossed": true,
                "fee": "0.01"
            }"#,
        )
        .expect("user fill");

        let event =
            leader_fill_event_from_user_fill("leader_a", "0xLeader", &fill, 999).expect("event");

        assert_eq!(event.side, OrderSide::Sell);
        assert!(event.reduce_only);
    }

    #[test]
    fn clearinghouse_adapter_extracts_nonzero_leader_positions() {
        let state: crate::hyperliquid::ClearinghouseState = serde_json::from_str(
            r#"{
                "marginSummary": {
                    "accountValue": "100",
                    "totalNtlPos": "25",
                    "totalRawUsd": "100",
                    "totalMarginUsed": "5"
                },
                "time": 123456,
                "assetPositions": [
                    {
                        "type": "oneWay",
                        "position": {
                            "coin": "xyz:TSLA",
                            "szi": "2.5",
                            "entryPx": "10",
                            "positionValue": "25"
                        }
                    },
                    {
                        "type": "oneWay",
                        "position": {
                            "coin": "xyz:NVDA",
                            "szi": "0",
                            "entryPx": "100",
                            "positionValue": "0"
                        }
                    }
                ]
            }"#,
        )
        .expect("clearinghouse state");

        let snapshots = leader_position_snapshots_from_clearinghouse_state(
            "leader_a",
            Some("xyz_perp".to_string()),
            Some("xyz".to_string()),
            &state,
            999,
        );

        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].leader_id, "leader_a");
        assert_eq!(snapshots[0].market.as_deref(), Some("xyz_perp"));
        assert_eq!(snapshots[0].dex.as_deref(), Some("xyz"));
        assert_eq!(snapshots[0].coin, "xyz:TSLA");
        assert_eq!(snapshots[0].signed_size, 2.5);
        assert_eq!(snapshots[0].position_notional_usd, 25.0);
        assert_eq!(snapshots[0].snapshot_time_ms, 123456);
        assert_eq!(snapshots[0].received_at_ms, 999);
    }

    #[test]
    fn read_only_watcher_builds_expected_leader_subscriptions() {
        let leaders = watcher_leaders();
        let subscriptions = read_only_leader_watcher_subscriptions(&leaders, Some("XYZ"));

        assert_eq!(subscriptions.len(), 5);
        assert_eq!(subscriptions[0]["type"], "userFills");
        assert_eq!(subscriptions[0]["user"], "0xLeader");
        assert_eq!(subscriptions[1]["type"], "userEvents");
        assert_eq!(subscriptions[2]["type"], "orderUpdates");
        assert_eq!(subscriptions[3]["type"], "allDexsClearinghouseState");
        assert_eq!(subscriptions[4]["type"], "clearinghouseState");
        assert_eq!(subscriptions[4]["dex"], "xyz");
        assert_eq!(
            copy_watcher_ws_url(None, "testnet"),
            "wss://api.hyperliquid-testnet.xyz/ws"
        );
        assert_eq!(
            copy_watcher_ws_url(Some("wss://example.test/ws"), "mainnet"),
            "wss://example.test/ws"
        );
    }

    #[test]
    fn read_only_watcher_parses_user_fills_snapshot() {
        let text = r#"{
            "channel": "userFills",
            "data": {
                "isSnapshot": true,
                "user": "0xLeader",
                "fills": [{
                    "coin": "xyz:TSLA",
                    "px": "12.5",
                    "sz": "3",
                    "side": "B",
                    "time": 123456,
                    "dir": "Open Long",
                    "closedPnl": "0",
                    "hash": "0xabc",
                    "oid": 42,
                    "crossed": true,
                    "fee": "0.01"
                }]
            }
        }"#;

        let events =
            parse_read_only_leader_watcher_message(&watcher_leaders(), Some("xyz"), text, 999)
                .expect("parse userFills");

        assert_eq!(events.len(), 1);
        match &events[0] {
            CopyLeaderWatcherEvent::Fill {
                leader_id,
                leader_address,
                fill,
                is_snapshot,
            } => {
                assert_eq!(leader_id, "leader_a");
                assert_eq!(leader_address, "0xLeader");
                assert_eq!(fill.event_id, "0xabc:42:123456:xyz:TSLA");
                assert_eq!(fill.side, OrderSide::Buy);
                assert!(*is_snapshot);
            }
            other => panic!("unexpected watcher event: {other:?}"),
        }
    }

    #[test]
    fn read_only_watcher_parses_user_events_fills_without_live_action() {
        let text = r#"{
            "channel": "userEvents",
            "data": {
                "fills": [{
                    "coin": "xyz:TSLA",
                    "px": "12.5",
                    "sz": "3",
                    "side": "A",
                    "time": 123456,
                    "dir": "Close Long",
                    "closedPnl": "1.2",
                    "hash": "0xdef",
                    "oid": 43,
                    "crossed": true,
                    "fee": "0.01"
                }]
            }
        }"#;

        let events =
            parse_read_only_leader_watcher_message(&watcher_leaders(), Some("xyz"), text, 999)
                .expect("parse userEvents");

        assert_eq!(events.len(), 1);
        match &events[0] {
            CopyLeaderWatcherEvent::Fill {
                fill, is_snapshot, ..
            } => {
                assert_eq!(fill.side, OrderSide::Sell);
                assert!(fill.reduce_only);
                assert!(!*is_snapshot);
            }
            other => panic!("unexpected watcher event: {other:?}"),
        }
    }

    #[test]
    fn read_only_watcher_parses_order_updates_for_single_leader_stream() {
        let text = r#"{
            "channel": "orderUpdates",
            "data": [{
                "order": {
                    "coin": "xyz:TSLA",
                    "oid": 44,
                    "side": "B",
                    "limitPx": "12.5",
                    "sz": "3",
                    "timestamp": 123456,
                    "origSz": "3"
                },
                "status": "filled",
                "statusTimestamp": 123999
            }]
        }"#;

        let events =
            parse_read_only_leader_watcher_message(&watcher_leaders(), Some("xyz"), text, 999)
                .expect("parse orderUpdates");

        assert_eq!(events.len(), 1);
        match &events[0] {
            CopyLeaderWatcherEvent::OrderUpdate {
                leader_id,
                coin,
                oid,
                status,
                status_timestamp_ms,
                ..
            } => {
                assert_eq!(leader_id, "leader_a");
                assert_eq!(coin, "xyz:TSLA");
                assert_eq!(*oid, 44);
                assert_eq!(status, "filled");
                assert_eq!(*status_timestamp_ms, 123999);
            }
            other => panic!("unexpected watcher event: {other:?}"),
        }
    }

    #[test]
    fn read_only_watcher_parses_clearinghouse_position_snapshots() {
        let text = r#"{
            "channel": "allDexsClearinghouseState",
            "data": {
                "user": "0xLeader",
                "clearinghouseStates": {
                    "xyz": {
                        "marginSummary": {
                            "accountValue": "100",
                            "totalNtlPos": "25",
                            "totalRawUsd": "100",
                            "totalMarginUsed": "5"
                        },
                        "time": 123456,
                        "assetPositions": [{
                            "type": "oneWay",
                            "position": {
                                "coin": "xyz:TSLA",
                                "szi": "-2.5",
                                "entryPx": "10",
                                "positionValue": "25"
                            }
                        }]
                    },
                    "other": {
                        "marginSummary": {
                            "accountValue": "100",
                            "totalNtlPos": "50",
                            "totalRawUsd": "100",
                            "totalMarginUsed": "5"
                        },
                        "time": 123456,
                        "assetPositions": [{
                            "type": "oneWay",
                            "position": {
                                "coin": "other:BTC",
                                "szi": "1",
                                "entryPx": "50",
                                "positionValue": "50"
                            }
                        }]
                    }
                }
            }
        }"#;

        let events =
            parse_read_only_leader_watcher_message(&watcher_leaders(), Some("xyz"), text, 999)
                .expect("parse clearinghouse");

        assert_eq!(events.len(), 1);
        match &events[0] {
            CopyLeaderWatcherEvent::PositionSnapshots {
                leader_id,
                leader_address,
                snapshots,
            } => {
                assert_eq!(leader_id, "leader_a");
                assert_eq!(leader_address, "0xLeader");
                assert_eq!(snapshots.len(), 1);
                assert_eq!(snapshots[0].market.as_deref(), Some("xyz_perp"));
                assert_eq!(snapshots[0].dex.as_deref(), Some("xyz"));
                assert_eq!(snapshots[0].coin, "xyz:TSLA");
                assert_eq!(snapshots[0].signed_size, -2.5);
            }
            other => panic!("unexpected watcher event: {other:?}"),
        }
    }

    fn leader_fill(
        event_id: &str,
        leader_id: &str,
        side: OrderSide,
        notional_usd: f64,
    ) -> LeaderFillEvent {
        LeaderFillEvent {
            event_id: event_id.to_string(),
            leader_id: leader_id.to_string(),
            leader_address: "0xABC".to_string(),
            coin: "xyz:XYZ100".to_string(),
            side,
            price: 10.0,
            size: notional_usd / 10.0,
            notional_usd,
            reduce_only: false,
            exchange_time_ms: now_ms(),
            received_at_ms: now_ms(),
        }
    }

    fn copy_strategy() -> SmartMoneyCopyStrategy {
        SmartMoneyCopyStrategy::new(copy_config())
    }

    fn copy_config() -> SmartMoneyCopyConfig {
        SmartMoneyCopyConfig {
            strategy_id: "copy_main".to_string(),
            default_copy_ratio: 0.1,
            max_slippage_bps: 25.0,
            leaders: vec![LeaderRule {
                leader_id: "leader_a".to_string(),
                leader_address: "0xabc".to_string(),
                enabled: true,
                copy_ratio: 0.2,
            }],
            symbol_limits: vec![SymbolCopyLimit {
                coin: "xyz:XYZ100".to_string(),
                max_signal_notional_usd: 30.0,
            }],
        }
    }

    fn strategy_context() -> StrategyContext {
        StrategyContext {
            target_accounts: vec!["addr_a".to_string(), "addr_b".to_string()],
            signal_ttl_ms: 3000,
        }
    }

    fn watcher_leaders() -> Vec<SmartMoneyLeaderWatch> {
        vec![SmartMoneyLeaderWatch {
            leader_id: "leader_a".to_string(),
            leader_address: "0xLeader".to_string(),
        }]
    }

    fn dry_run_shadow_config() -> CopyDryRunShadowConfig {
        CopyDryRunShadowConfig {
            local_account_id: "addr_a".to_string(),
            target_accounts: vec!["addr_a".to_string()],
            signal_ttl_ms: 3000,
            max_signal_delay_ms: 10_000,
            account_copy_ratio: 1.0,
            principal_cap_usd: COPY_DEFAULT_PRINCIPAL_CAP_USD,
            leverage: COPY_MAX_LEVERAGE,
            max_signal_notional_usd: Some(1_000.0),
            exchange_min_open_notional_usd: 10.0,
            allow_short: true,
            max_effective_exposure_usd: Some(1_000.0),
            blocked_symbols: Vec::new(),
            live_gate: CopyLiveGateInput {
                process_dry_run: true,
                live_copy_enabled: false,
                account_worker_live: false,
            },
        }
    }

    fn risk_gateway_account() -> AccountConfig {
        AccountConfig {
            account_id: "addr_a".to_string(),
            address: "0x1".to_string(),
            secret_id: "addr_a_api_wallet".to_string(),
            api_wallet_env: String::new(),
            transfer_secret_id: String::new(),
            transfer_wallet_env: String::new(),
            enabled: true,
            worker_enabled: true,
            copy_ratio: 1.0,
            max_order_notional_usd: 100.0,
            blocked_markets: Vec::new(),
        }
    }

    fn position_event(
        leader_id: &str,
        signed_size: f64,
        position_notional_usd: f64,
    ) -> CopyLeaderWatcherEvent {
        CopyLeaderWatcherEvent::PositionSnapshots {
            leader_id: leader_id.to_string(),
            leader_address: "0xABC".to_string(),
            snapshots: vec![LeaderPositionSnapshot {
                leader_id: leader_id.to_string(),
                market: Some("xyz_perp".to_string()),
                dex: Some("xyz".to_string()),
                coin: "xyz:XYZ100".to_string(),
                signed_size,
                position_notional_usd,
                snapshot_time_ms: now_ms(),
                received_at_ms: now_ms(),
            }],
        }
    }

    fn semantic_action(
        event_id: &str,
        kind: LeaderActionKind,
        confidence: LeaderActionConfidence,
        leader_notional_usd: f64,
    ) -> SemanticLeaderAction {
        SemanticLeaderAction {
            leader_id: "leader_a".to_string(),
            leader_address: "0xABC".to_string(),
            market: Some("xyz_perp".to_string()),
            dex: Some("xyz".to_string()),
            coin: "xyz:XYZ100".to_string(),
            event_id: event_id.to_string(),
            kind,
            confidence,
            leader_notional_usd,
            close_leader_notional_usd: None,
            open_leader_notional_usd: None,
            exchange_time_ms: 123,
            received_at_ms: now_ms(),
            reason: "test".to_string(),
        }
    }

    fn leader_position(leader_id: &str, coin: &str, signed_size: f64) -> LeaderPositionSnapshot {
        LeaderPositionSnapshot {
            leader_id: leader_id.to_string(),
            market: Some("xyz_perp".to_string()),
            dex: Some("xyz".to_string()),
            coin: coin.to_string(),
            signed_size,
            position_notional_usd: signed_size.abs() * 10.0,
            snapshot_time_ms: now_ms(),
            received_at_ms: now_ms(),
        }
    }

    fn conflict_event(
        event_id: &str,
        leader_id: &str,
        leader_group: &str,
        kind: LeaderActionKind,
        leader_notional_usd: f64,
        weight: f64,
    ) -> CopyConflictInput {
        CopyConflictInput {
            event_id: event_id.to_string(),
            leader_id: leader_id.to_string(),
            leader_group: leader_group.to_string(),
            coin: "xyz:XYZ100".to_string(),
            kind,
            leader_notional_usd,
            weight,
            received_at_ms: now_ms(),
        }
    }

    fn risk_input(action: &SemanticLeaderAction) -> CopySignalRiskInput<'_> {
        CopySignalRiskInput {
            action,
            sizing: CopySizingInput {
                leader_notional_usd: action.leader_notional_usd,
                leader_copy_ratio: 0.2,
                account_copy_ratio: 1.0,
                principal_cap_usd: None,
                leverage: 1.0,
                leader_trade_cap_usd: None,
                symbol_order_cap_usd: None,
                account_order_cap_usd: None,
                remaining_symbol_position_cap_usd: None,
                remaining_daily_cap_usd: None,
                exchange_min_open_notional_usd: 10.0,
                reduce_only: false,
            },
            now_ms: action.received_at_ms + 100,
            max_signal_delay_ms: 10_000,
            leader_enabled: true,
            symbol_blocked: false,
            allow_short: true,
            current_effective_exposure_usd: 0.0,
            max_effective_exposure_usd: Some(1_000.0),
        }
    }

    fn ledger_entry(
        signal_id: &str,
        local_side: OrderSide,
        planned_notional_usd: f64,
        pending_notional_usd: f64,
        filled_notional_usd: f64,
        remaining_notional_usd: f64,
        status: CopyLedgerStatus,
    ) -> CopyLedgerEntry {
        CopyLedgerEntry {
            local_account_id: "addr_a".to_string(),
            leader_id: "leader_a".to_string(),
            leader_group: "group_a".to_string(),
            signal_id: signal_id.to_string(),
            coin: "xyz:XYZ100".to_string(),
            local_side,
            order_cloid: None,
            order_oid: None,
            submitted_at_ms: None,
            filled_at_ms: None,
            planned_notional_usd,
            pending_notional_usd,
            filled_notional_usd,
            remaining_notional_usd,
            status,
        }
    }

    fn order_submitted(
        signal_id: &str,
        side: OrderSide,
        notional_usd: f64,
        _reduce_only: bool,
    ) -> OrderSubmitted {
        OrderSubmitted {
            signal_id: signal_id.to_string(),
            intent_id: format!("intent-{signal_id}"),
            worker_id: "worker-addr_a".to_string(),
            account_id: "addr_a".to_string(),
            cloid: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, signal_id.as_bytes()).to_string(),
            coin: "xyz:XYZ100".to_string(),
            side,
            notional_usd,
            submitted_price: Some(10.0),
            submitted_size: Some(notional_usd / 10.0),
            exchange_status: Some("filled".to_string()),
            oid: Some(42),
            filled_size: Some(notional_usd / 10.0),
            avg_fill_price: Some(10.0),
            dry_run: false,
            submitted_at_ms: now_ms(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn order_status_response(
        status: &str,
        coin: &str,
        side: &str,
        limit_px: &str,
        sz: &str,
        oid: u64,
        timestamp: u64,
        status_timestamp: u64,
        cloid: Option<String>,
    ) -> OrderStatusResponse {
        OrderStatusResponse {
            status: "order".to_string(),
            order: Some(OrderStatusInfo {
                order: OrderStatusOrder {
                    coin: coin.to_string(),
                    side: side.to_string(),
                    limit_px: limit_px.to_string(),
                    sz: sz.to_string(),
                    oid,
                    timestamp,
                    trigger_condition: "N/A".to_string(),
                    is_trigger: false,
                    trigger_px: "0.0".to_string(),
                    children: Vec::new(),
                    is_position_tpsl: false,
                    reduce_only: false,
                    order_type: "Limit".to_string(),
                    orig_sz: sz.to_string(),
                    tif: "Ioc".to_string(),
                    cloid,
                },
                status: status.to_string(),
                status_timestamp,
            }),
        }
    }

    fn user_fill(oid: u64, coin: &str, px: &str, sz: &str, time: u64) -> UserFill {
        UserFill {
            coin: coin.to_string(),
            px: px.to_string(),
            sz: sz.to_string(),
            side: "B".to_string(),
            time,
            dir: "Open Long".to_string(),
            closed_pnl: "0.0".to_string(),
            hash: format!("0x{oid:x}{time:x}"),
            oid,
            crossed: true,
            fee: "0.0".to_string(),
        }
    }
}
