use std::{
    collections::{BTreeMap, HashSet},
    fmt,
};

use serde::{Deserialize, Serialize};

pub type V2TimestampMs = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum V2SignalSource {
    Manual,
    FibBasic,
    CopyTrading,
    System,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum V2ExecutionMode {
    DryRun,
    Live,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum V2OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum V2WorkerCommandKind {
    SubmitOrder {
        market: String,
        coin: String,
        side: V2OrderSide,
        notional_usd: f64,
        reduce_only: bool,
    },
    CancelOrder {
        market: String,
        coin: String,
        cloid: String,
    },
    ArmNativeTpsl {
        market: String,
        coin: String,
        take_profit_trigger_price: f64,
        stop_loss_trigger_price: f64,
    },
    ClosePosition {
        market: String,
        coin: String,
        side: V2OrderSide,
    },
    RefreshState,
    LockSigner,
    Shutdown,
}

impl V2WorkerCommandKind {
    fn requires_warm_signer(&self) -> bool {
        matches!(
            self,
            Self::SubmitOrder { .. }
                | Self::CancelOrder { .. }
                | Self::ArmNativeTpsl { .. }
                | Self::ClosePosition { .. }
        )
    }

    fn is_control(&self) -> bool {
        matches!(self, Self::RefreshState | Self::LockSigner | Self::Shutdown)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct V2WorkerCommand {
    pub signal_id: String,
    pub account_id: String,
    pub idempotency_key: String,
    pub source: V2SignalSource,
    pub mode: V2ExecutionMode,
    pub risk_approval_id: String,
    pub created_at_ms: V2TimestampMs,
    pub kind: V2WorkerCommandKind,
}

impl V2WorkerCommand {
    pub fn new_order(
        signal_id: impl Into<String>,
        account_id: impl Into<String>,
        market: impl Into<String>,
        coin: impl Into<String>,
        side: V2OrderSide,
        notional_usd: f64,
        mode: V2ExecutionMode,
        created_at_ms: V2TimestampMs,
    ) -> Self {
        let signal_id = signal_id.into();
        let account_id = account_id.into();
        let market = market.into();
        let coin = coin.into();
        Self {
            idempotency_key: format!("{signal_id}:{account_id}:{market}:{coin}:order"),
            signal_id,
            account_id,
            source: V2SignalSource::Manual,
            mode,
            risk_approval_id: "risk-approved-test".to_string(),
            created_at_ms,
            kind: V2WorkerCommandKind::SubmitOrder {
                market,
                coin,
                side,
                notional_usd,
                reduce_only: false,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum V2SignerState {
    Cold,
    Warm {
        fingerprint: String,
        warmed_at_ms: V2TimestampMs,
    },
}

impl V2SignerState {
    pub fn is_warm(&self) -> bool {
        matches!(self, Self::Warm { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum V2WorkerRejectReason {
    WrongAccount,
    SignerCold,
    DuplicateCommand,
    QueueFull,
    ShuttingDown,
}

impl fmt::Display for V2WorkerRejectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongAccount => write!(f, "command account does not match worker account"),
            Self::SignerCold => write!(f, "live command requires a warm signer"),
            Self::DuplicateCommand => write!(f, "duplicate idempotency key ignored"),
            Self::QueueFull => write!(f, "worker queue is full"),
            Self::ShuttingDown => write!(f, "worker is shutting down"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum V2WorkerCommandStatus {
    AcceptedDryRun,
    AcceptedLiveReady,
    AcceptedControl,
    IgnoredDuplicate,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct V2WorkerResult {
    pub account_id: String,
    pub signal_id: String,
    pub idempotency_key: String,
    pub status: V2WorkerCommandStatus,
    pub reject_reason: Option<V2WorkerRejectReason>,
    pub signer_warm: bool,
    pub queue_depth_after: usize,
    pub received_at_ms: V2TimestampMs,
}

impl V2WorkerResult {
    fn accepted_status(command: &V2WorkerCommand) -> V2WorkerCommandStatus {
        match command.kind {
            V2WorkerCommandKind::RefreshState
            | V2WorkerCommandKind::LockSigner
            | V2WorkerCommandKind::Shutdown => V2WorkerCommandStatus::AcceptedControl,
            _ if command.mode == V2ExecutionMode::Live => V2WorkerCommandStatus::AcceptedLiveReady,
            _ => V2WorkerCommandStatus::AcceptedDryRun,
        }
    }
}

#[derive(Debug, Clone)]
pub struct V2AccountWorkerRuntime {
    account_id: String,
    queue_capacity: usize,
    signer_state: V2SignerState,
    seen_idempotency_keys: HashSet<String>,
    accepted_commands: Vec<V2WorkerCommand>,
    shutting_down: bool,
}

impl V2AccountWorkerRuntime {
    pub fn new(account_id: impl Into<String>, queue_capacity: usize) -> Self {
        Self {
            account_id: account_id.into(),
            queue_capacity: queue_capacity.max(1),
            signer_state: V2SignerState::Cold,
            seen_idempotency_keys: HashSet::new(),
            accepted_commands: Vec::new(),
            shutting_down: false,
        }
    }

    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    pub fn signer_state(&self) -> &V2SignerState {
        &self.signer_state
    }

    pub fn accepted_count(&self) -> usize {
        self.accepted_commands.len()
    }

    pub fn warm_signer(&mut self, fingerprint: impl Into<String>, warmed_at_ms: V2TimestampMs) {
        self.signer_state = V2SignerState::Warm {
            fingerprint: fingerprint.into(),
            warmed_at_ms,
        };
    }

    pub fn lock_signer(&mut self) {
        self.signer_state = V2SignerState::Cold;
    }

    pub fn handle_command(
        &mut self,
        command: V2WorkerCommand,
        received_at_ms: V2TimestampMs,
    ) -> V2WorkerResult {
        if command.account_id != self.account_id {
            return self.rejected(command, V2WorkerRejectReason::WrongAccount, received_at_ms);
        }

        if self
            .seen_idempotency_keys
            .contains(&command.idempotency_key)
        {
            return V2WorkerResult {
                account_id: self.account_id.clone(),
                signal_id: command.signal_id,
                idempotency_key: command.idempotency_key,
                status: V2WorkerCommandStatus::IgnoredDuplicate,
                reject_reason: Some(V2WorkerRejectReason::DuplicateCommand),
                signer_warm: self.signer_state.is_warm(),
                queue_depth_after: self.accepted_commands.len(),
                received_at_ms,
            };
        }

        if self.shutting_down
            && !matches!(
                command.kind,
                V2WorkerCommandKind::RefreshState | V2WorkerCommandKind::LockSigner
            )
        {
            return self.rejected(command, V2WorkerRejectReason::ShuttingDown, received_at_ms);
        }

        if command.mode == V2ExecutionMode::Live
            && command.kind.requires_warm_signer()
            && !self.signer_state.is_warm()
        {
            return self.rejected(command, V2WorkerRejectReason::SignerCold, received_at_ms);
        }

        if self.accepted_commands.len() >= self.queue_capacity && !command.kind.is_control() {
            return self.rejected(command, V2WorkerRejectReason::QueueFull, received_at_ms);
        }

        let status = V2WorkerResult::accepted_status(&command);
        let result = V2WorkerResult {
            account_id: self.account_id.clone(),
            signal_id: command.signal_id.clone(),
            idempotency_key: command.idempotency_key.clone(),
            status,
            reject_reason: None,
            signer_warm: self.signer_state.is_warm(),
            queue_depth_after: self.accepted_commands.len() + 1,
            received_at_ms,
        };

        self.seen_idempotency_keys
            .insert(command.idempotency_key.clone());
        match command.kind {
            V2WorkerCommandKind::LockSigner => self.lock_signer(),
            V2WorkerCommandKind::Shutdown => self.shutting_down = true,
            _ => {}
        }
        self.accepted_commands.push(command);
        result
    }

    fn rejected(
        &self,
        command: V2WorkerCommand,
        reason: V2WorkerRejectReason,
        received_at_ms: V2TimestampMs,
    ) -> V2WorkerResult {
        V2WorkerResult {
            account_id: self.account_id.clone(),
            signal_id: command.signal_id,
            idempotency_key: command.idempotency_key,
            status: V2WorkerCommandStatus::Rejected,
            reject_reason: Some(reason),
            signer_warm: self.signer_state.is_warm(),
            queue_depth_after: self.accepted_commands.len(),
            received_at_ms,
        }
    }
}

#[derive(Debug, Default)]
pub struct V2WorkerCoordinator {
    workers: BTreeMap<String, V2AccountWorkerRuntime>,
}

impl V2WorkerCoordinator {
    pub fn add_worker(&mut self, worker: V2AccountWorkerRuntime) {
        self.workers.insert(worker.account_id().to_string(), worker);
    }

    pub fn warm_worker(
        &mut self,
        account_id: &str,
        fingerprint: impl Into<String>,
        warmed_at_ms: V2TimestampMs,
    ) -> bool {
        let Some(worker) = self.workers.get_mut(account_id) else {
            return false;
        };
        worker.warm_signer(fingerprint, warmed_at_ms);
        true
    }

    pub fn dispatch(
        &mut self,
        command: V2WorkerCommand,
        received_at_ms: V2TimestampMs,
    ) -> V2WorkerResult {
        let account_id = command.account_id.clone();
        let Some(worker) = self.workers.get_mut(&account_id) else {
            return V2WorkerResult {
                account_id,
                signal_id: command.signal_id,
                idempotency_key: command.idempotency_key,
                status: V2WorkerCommandStatus::Rejected,
                reject_reason: Some(V2WorkerRejectReason::WrongAccount),
                signer_warm: false,
                queue_depth_after: 0,
                received_at_ms,
            };
        };
        worker.handle_command(command, received_at_ms)
    }

    pub fn dispatch_many(
        &mut self,
        commands: impl IntoIterator<Item = V2WorkerCommand>,
        received_at_ms: V2TimestampMs,
    ) -> Vec<V2WorkerResult> {
        commands
            .into_iter()
            .map(|command| self.dispatch(command, received_at_ms))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live_order(account_id: &str, signal_id: &str) -> V2WorkerCommand {
        V2WorkerCommand::new_order(
            signal_id,
            account_id,
            "xyz_perp",
            "xyz:NVDA",
            V2OrderSide::Buy,
            11.0,
            V2ExecutionMode::Live,
            1_000,
        )
    }

    #[test]
    fn live_order_requires_warm_signer() {
        let mut worker = V2AccountWorkerRuntime::new("addr_a", 8);
        let rejected = worker.handle_command(live_order("addr_a", "sig-1"), 1_001);

        assert_eq!(rejected.status, V2WorkerCommandStatus::Rejected);
        assert_eq!(
            rejected.reject_reason,
            Some(V2WorkerRejectReason::SignerCold)
        );
        assert_eq!(worker.accepted_count(), 0);

        worker.warm_signer("api-wallet-a", 1_002);
        let accepted = worker.handle_command(live_order("addr_a", "sig-2"), 1_003);

        assert_eq!(accepted.status, V2WorkerCommandStatus::AcceptedLiveReady);
        assert!(accepted.signer_warm);
        assert_eq!(worker.accepted_count(), 1);
    }

    #[test]
    fn duplicate_idempotency_key_is_ignored_without_requeueing() {
        let mut worker = V2AccountWorkerRuntime::new("addr_a", 8);
        let command = V2WorkerCommand::new_order(
            "sig-dupe",
            "addr_a",
            "hl_perp",
            "ETH",
            V2OrderSide::Buy,
            11.0,
            V2ExecutionMode::DryRun,
            2_000,
        );

        let first = worker.handle_command(command.clone(), 2_001);
        let second = worker.handle_command(command, 2_002);

        assert_eq!(first.status, V2WorkerCommandStatus::AcceptedDryRun);
        assert_eq!(second.status, V2WorkerCommandStatus::IgnoredDuplicate);
        assert_eq!(
            second.reject_reason,
            Some(V2WorkerRejectReason::DuplicateCommand)
        );
        assert_eq!(worker.accepted_count(), 1);
    }

    #[test]
    fn worker_rejects_cross_account_command() {
        let mut worker = V2AccountWorkerRuntime::new("addr_a", 8);
        worker.warm_signer("api-wallet-a", 3_000);

        let result = worker.handle_command(live_order("addr_b", "sig-cross"), 3_001);

        assert_eq!(result.status, V2WorkerCommandStatus::Rejected);
        assert_eq!(
            result.reject_reason,
            Some(V2WorkerRejectReason::WrongAccount)
        );
        assert_eq!(worker.accepted_count(), 0);
    }

    #[test]
    fn queue_capacity_fails_closed() {
        let mut worker = V2AccountWorkerRuntime::new("addr_a", 1);

        let first = worker.handle_command(
            V2WorkerCommand::new_order(
                "sig-1",
                "addr_a",
                "hl_perp",
                "BTC",
                V2OrderSide::Buy,
                11.0,
                V2ExecutionMode::DryRun,
                4_000,
            ),
            4_001,
        );
        let second = worker.handle_command(
            V2WorkerCommand::new_order(
                "sig-2",
                "addr_a",
                "hl_perp",
                "ETH",
                V2OrderSide::Buy,
                11.0,
                V2ExecutionMode::DryRun,
                4_002,
            ),
            4_003,
        );

        assert_eq!(first.status, V2WorkerCommandStatus::AcceptedDryRun);
        assert_eq!(second.status, V2WorkerCommandStatus::Rejected);
        assert_eq!(second.reject_reason, Some(V2WorkerRejectReason::QueueFull));
    }

    #[test]
    fn coordinator_fans_same_signal_to_multiple_warm_workers() {
        let mut coordinator = V2WorkerCoordinator::default();
        coordinator.add_worker(V2AccountWorkerRuntime::new("addr_a", 8));
        coordinator.add_worker(V2AccountWorkerRuntime::new("addr_b", 8));
        assert!(coordinator.warm_worker("addr_a", "api-wallet-a", 5_000));
        assert!(coordinator.warm_worker("addr_b", "api-wallet-b", 5_000));

        let commands = ["addr_a", "addr_b"]
            .into_iter()
            .map(|account_id| live_order(account_id, "shared-signal-1"));
        let results = coordinator.dispatch_many(commands, 5_001);

        assert_eq!(results.len(), 2);
        assert!(
            results
                .iter()
                .all(|result| result.signal_id == "shared-signal-1")
        );
        assert!(
            results
                .iter()
                .all(|result| result.status == V2WorkerCommandStatus::AcceptedLiveReady)
        );
        assert_eq!(results[0].account_id, "addr_a");
        assert_eq!(results[1].account_id, "addr_b");
    }

    #[test]
    fn lock_signer_command_drops_warm_signer() {
        let mut worker = V2AccountWorkerRuntime::new("addr_a", 8);
        worker.warm_signer("api-wallet-a", 6_000);

        let result = worker.handle_command(
            V2WorkerCommand {
                signal_id: "lock-1".to_string(),
                account_id: "addr_a".to_string(),
                idempotency_key: "lock-1:addr_a".to_string(),
                source: V2SignalSource::System,
                mode: V2ExecutionMode::Live,
                risk_approval_id: "control".to_string(),
                created_at_ms: 6_001,
                kind: V2WorkerCommandKind::LockSigner,
            },
            6_002,
        );

        assert_eq!(result.status, V2WorkerCommandStatus::AcceptedControl);
        assert_eq!(worker.signer_state(), &V2SignerState::Cold);
    }
}
