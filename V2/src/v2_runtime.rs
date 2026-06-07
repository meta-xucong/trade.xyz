use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::future::{BoxFuture, FutureExt, join_all};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

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
    /// Convenience constructor for tests and early adapters. Full live paths
    /// should come from typed intents after risk approval.
    #[allow(clippy::too_many_arguments)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum V2ExchangeActionKind {
    SubmitOrder,
    CancelOrder,
    ArmNativeTpsl,
    ClosePosition,
}

impl V2ExchangeActionKind {
    fn from_command_kind(kind: &V2WorkerCommandKind) -> Option<Self> {
        match kind {
            V2WorkerCommandKind::SubmitOrder { .. } => Some(Self::SubmitOrder),
            V2WorkerCommandKind::CancelOrder { .. } => Some(Self::CancelOrder),
            V2WorkerCommandKind::ArmNativeTpsl { .. } => Some(Self::ArmNativeTpsl),
            V2WorkerCommandKind::ClosePosition { .. } => Some(Self::ClosePosition),
            V2WorkerCommandKind::RefreshState
            | V2WorkerCommandKind::LockSigner
            | V2WorkerCommandKind::Shutdown => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct V2ExchangeSubmission {
    pub account_id: String,
    pub signal_id: String,
    pub idempotency_key: String,
    pub mode: V2ExecutionMode,
    pub action: V2ExchangeActionKind,
    pub submitted_at_ms: V2TimestampMs,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct V2WorkerExecutionEvent {
    pub result: V2WorkerResult,
    pub submission: Option<V2ExchangeSubmission>,
}

#[derive(Debug, Clone)]
pub struct V2MockExchangeAdapter {
    submissions: Arc<Mutex<Vec<V2ExchangeSubmission>>>,
    processing_delay: Duration,
}

impl Default for V2MockExchangeAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl V2MockExchangeAdapter {
    pub fn new() -> Self {
        Self {
            submissions: Arc::new(Mutex::new(Vec::new())),
            processing_delay: Duration::ZERO,
        }
    }

    pub fn with_processing_delay(processing_delay: Duration) -> Self {
        Self {
            submissions: Arc::new(Mutex::new(Vec::new())),
            processing_delay,
        }
    }

    pub fn submissions(&self) -> Vec<V2ExchangeSubmission> {
        self.submissions
            .lock()
            .expect("mock exchange submissions mutex poisoned")
            .clone()
    }

    async fn execute_if_accepted(
        &self,
        command: &V2WorkerCommand,
        result: &V2WorkerResult,
        submitted_at_ms: V2TimestampMs,
    ) -> Option<V2ExchangeSubmission> {
        if result.reject_reason.is_some()
            || !matches!(
                result.status,
                V2WorkerCommandStatus::AcceptedDryRun | V2WorkerCommandStatus::AcceptedLiveReady
            )
        {
            return None;
        }

        let action = V2ExchangeActionKind::from_command_kind(&command.kind)?;
        if !self.processing_delay.is_zero() {
            tokio::time::sleep(self.processing_delay).await;
        }

        let submission = V2ExchangeSubmission {
            account_id: command.account_id.clone(),
            signal_id: command.signal_id.clone(),
            idempotency_key: command.idempotency_key.clone(),
            mode: command.mode,
            action,
            submitted_at_ms,
        };
        self.submissions
            .lock()
            .expect("mock exchange submissions mutex poisoned")
            .push(submission.clone());
        Some(submission)
    }
}

struct V2AsyncWorkerEnvelope {
    command: V2WorkerCommand,
    received_at_ms: V2TimestampMs,
    reply: oneshot::Sender<V2WorkerExecutionEvent>,
}

#[derive(Debug, Clone)]
pub struct V2AsyncWorkerHandle {
    account_id: String,
    command_tx: mpsc::Sender<V2AsyncWorkerEnvelope>,
}

impl V2AsyncWorkerHandle {
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    pub async fn submit(
        &self,
        command: V2WorkerCommand,
        received_at_ms: V2TimestampMs,
    ) -> V2WorkerExecutionEvent {
        let (reply, reply_rx) = oneshot::channel();
        let envelope = V2AsyncWorkerEnvelope {
            command,
            received_at_ms,
            reply,
        };

        match self.command_tx.try_send(envelope) {
            Ok(()) => match reply_rx.await {
                Ok(event) => event,
                Err(_) => Self::worker_closed_event(self.account_id.clone(), received_at_ms),
            },
            Err(mpsc::error::TrySendError::Full(envelope)) => Self::rejected_event(
                envelope.command,
                V2WorkerRejectReason::QueueFull,
                received_at_ms,
            ),
            Err(mpsc::error::TrySendError::Closed(envelope)) => Self::rejected_event(
                envelope.command,
                V2WorkerRejectReason::ShuttingDown,
                received_at_ms,
            ),
        }
    }

    fn rejected_event(
        command: V2WorkerCommand,
        reason: V2WorkerRejectReason,
        received_at_ms: V2TimestampMs,
    ) -> V2WorkerExecutionEvent {
        V2WorkerExecutionEvent {
            result: V2WorkerResult {
                account_id: command.account_id,
                signal_id: command.signal_id,
                idempotency_key: command.idempotency_key,
                status: V2WorkerCommandStatus::Rejected,
                reject_reason: Some(reason),
                signer_warm: false,
                queue_depth_after: 0,
                received_at_ms,
            },
            submission: None,
        }
    }

    fn worker_closed_event(
        account_id: String,
        received_at_ms: V2TimestampMs,
    ) -> V2WorkerExecutionEvent {
        V2WorkerExecutionEvent {
            result: V2WorkerResult {
                account_id,
                signal_id: "worker-closed".to_string(),
                idempotency_key: "worker-closed".to_string(),
                status: V2WorkerCommandStatus::Rejected,
                reject_reason: Some(V2WorkerRejectReason::ShuttingDown),
                signer_warm: false,
                queue_depth_after: 0,
                received_at_ms,
            },
            submission: None,
        }
    }
}

pub struct V2AsyncAccountWorker {
    handle: V2AsyncWorkerHandle,
    join_handle: JoinHandle<()>,
}

impl V2AsyncAccountWorker {
    pub fn spawn(
        runtime: V2AccountWorkerRuntime,
        adapter: V2MockExchangeAdapter,
        channel_capacity: usize,
    ) -> Self {
        let account_id = runtime.account_id().to_string();
        let (command_tx, command_rx) = mpsc::channel(channel_capacity.max(1));
        let handle = V2AsyncWorkerHandle {
            account_id,
            command_tx,
        };
        let join_handle = tokio::spawn(run_async_worker(runtime, adapter, command_rx));
        Self {
            handle,
            join_handle,
        }
    }

    pub fn handle(&self) -> V2AsyncWorkerHandle {
        self.handle.clone()
    }

    pub async fn join(self) {
        let _ = self.join_handle.await;
    }
}

async fn run_async_worker(
    mut runtime: V2AccountWorkerRuntime,
    adapter: V2MockExchangeAdapter,
    mut command_rx: mpsc::Receiver<V2AsyncWorkerEnvelope>,
) {
    while let Some(envelope) = command_rx.recv().await {
        let shutdown_requested = matches!(envelope.command.kind, V2WorkerCommandKind::Shutdown);
        let result = runtime.handle_command(envelope.command.clone(), envelope.received_at_ms);
        let submission = adapter
            .execute_if_accepted(&envelope.command, &result, envelope.received_at_ms)
            .await;
        let _ = envelope
            .reply
            .send(V2WorkerExecutionEvent { result, submission });

        if shutdown_requested {
            break;
        }
    }
}

#[derive(Debug, Default)]
pub struct V2AsyncWorkerCoordinator {
    workers: BTreeMap<String, V2AsyncWorkerHandle>,
}

impl V2AsyncWorkerCoordinator {
    pub fn add_worker(&mut self, worker: V2AsyncWorkerHandle) {
        self.workers.insert(worker.account_id().to_string(), worker);
    }

    pub async fn dispatch_many(
        &self,
        commands: impl IntoIterator<Item = V2WorkerCommand>,
        received_at_ms: V2TimestampMs,
    ) -> Vec<V2WorkerExecutionEvent> {
        let futures = commands
            .into_iter()
            .map(|command| {
                if let Some(worker) = self.workers.get(&command.account_id).cloned() {
                    async move { worker.submit(command, received_at_ms).await }.boxed()
                } else {
                    async move {
                        V2AsyncWorkerHandle::rejected_event(
                            command,
                            V2WorkerRejectReason::WrongAccount,
                            received_at_ms,
                        )
                    }
                    .boxed()
                }
            })
            .collect::<Vec<BoxFuture<'static, V2WorkerExecutionEvent>>>();

        join_all(futures).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Instant;

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

    #[tokio::test]
    async fn async_worker_records_live_submission_after_warm_signer() {
        let mut runtime = V2AccountWorkerRuntime::new("addr_a", 8);
        runtime.warm_signer("api-wallet-a", 7_000);
        let adapter = V2MockExchangeAdapter::new();
        let worker = V2AsyncAccountWorker::spawn(runtime, adapter.clone(), 8);

        let event = worker
            .handle()
            .submit(live_order("addr_a", "sig-async-live"), 7_001)
            .await;

        assert_eq!(
            event.result.status,
            V2WorkerCommandStatus::AcceptedLiveReady
        );
        assert_eq!(
            event
                .submission
                .as_ref()
                .map(|submission| submission.action),
            Some(V2ExchangeActionKind::SubmitOrder)
        );
        assert_eq!(adapter.submissions().len(), 1);
    }

    #[tokio::test]
    async fn async_worker_rejects_cold_live_without_exchange_submit() {
        let runtime = V2AccountWorkerRuntime::new("addr_a", 8);
        let adapter = V2MockExchangeAdapter::new();
        let worker = V2AsyncAccountWorker::spawn(runtime, adapter.clone(), 8);

        let event = worker
            .handle()
            .submit(live_order("addr_a", "sig-async-cold"), 8_001)
            .await;

        assert_eq!(event.result.status, V2WorkerCommandStatus::Rejected);
        assert_eq!(
            event.result.reject_reason,
            Some(V2WorkerRejectReason::SignerCold)
        );
        assert!(event.submission.is_none());
        assert!(adapter.submissions().is_empty());
    }

    #[tokio::test]
    async fn bounded_async_queue_fails_closed_when_worker_is_backed_up() {
        let runtime = V2AccountWorkerRuntime::new("addr_a", 8);
        let adapter = V2MockExchangeAdapter::with_processing_delay(Duration::from_millis(100));
        let worker = V2AsyncAccountWorker::spawn(runtime, adapter, 1);
        let handle = worker.handle();

        let first_handle = handle.clone();
        let first = tokio::spawn(async move {
            first_handle
                .submit(
                    V2WorkerCommand::new_order(
                        "sig-queue-1",
                        "addr_a",
                        "hl_perp",
                        "ETH",
                        V2OrderSide::Buy,
                        11.0,
                        V2ExecutionMode::DryRun,
                        9_000,
                    ),
                    9_001,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        let second_handle = handle.clone();
        let second = tokio::spawn(async move {
            second_handle
                .submit(
                    V2WorkerCommand::new_order(
                        "sig-queue-2",
                        "addr_a",
                        "hl_perp",
                        "ETH",
                        V2OrderSide::Buy,
                        11.0,
                        V2ExecutionMode::DryRun,
                        9_002,
                    ),
                    9_003,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        let third = handle
            .submit(
                V2WorkerCommand::new_order(
                    "sig-queue-3",
                    "addr_a",
                    "hl_perp",
                    "ETH",
                    V2OrderSide::Buy,
                    11.0,
                    V2ExecutionMode::DryRun,
                    9_004,
                ),
                9_005,
            )
            .await;

        assert_eq!(third.result.status, V2WorkerCommandStatus::Rejected);
        assert_eq!(
            third.result.reject_reason,
            Some(V2WorkerRejectReason::QueueFull)
        );
        assert!(third.submission.is_none());

        let _ = first.await.expect("first queue test task panicked");
        let _ = second.await.expect("second queue test task panicked");
    }

    #[tokio::test]
    async fn async_coordinator_dispatches_workers_concurrently() {
        let mut runtime_a = V2AccountWorkerRuntime::new("addr_a", 8);
        let mut runtime_b = V2AccountWorkerRuntime::new("addr_b", 8);
        runtime_a.warm_signer("api-wallet-a", 10_000);
        runtime_b.warm_signer("api-wallet-b", 10_000);

        let adapter = V2MockExchangeAdapter::with_processing_delay(Duration::from_millis(100));
        let worker_a = V2AsyncAccountWorker::spawn(runtime_a, adapter.clone(), 8);
        let worker_b = V2AsyncAccountWorker::spawn(runtime_b, adapter.clone(), 8);
        let mut coordinator = V2AsyncWorkerCoordinator::default();
        coordinator.add_worker(worker_a.handle());
        coordinator.add_worker(worker_b.handle());

        let started = Instant::now();
        let events = coordinator
            .dispatch_many(
                [
                    live_order("addr_a", "sig-parallel"),
                    live_order("addr_b", "sig-parallel"),
                ],
                10_001,
            )
            .await;
        let elapsed = started.elapsed();

        assert_eq!(events.len(), 2);
        assert!(
            events
                .iter()
                .all(|event| event.result.status == V2WorkerCommandStatus::AcceptedLiveReady)
        );
        assert_eq!(adapter.submissions().len(), 2);
        assert!(
            elapsed < Duration::from_millis(180),
            "expected concurrent worker execution, elapsed={elapsed:?}"
        );
    }
}
