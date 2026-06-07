use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    process::Stdio,
    sync::Arc,
};

use anyhow::{Context, Result};
use tokio::{
    io::BufReader,
    net::{TcpListener, tcp::OwnedWriteHalf},
    process::Child,
    sync::{Mutex, mpsc},
    time::{Duration, timeout},
};
use tracing::{info, warn};

use crate::{
    config::{AccountConfig, load_config},
    domain::{
        CoordinatorSignal, ExecutionMode, OrderSide, SignalOrder, SignalSource, WorkerRegistration,
        WorkerReport, now_ms,
    },
    ipc::{CoordinatorMessage, WorkerMessage, read_json_line, write_json_line},
};

#[derive(Debug)]
pub struct CoordinatorOptions {
    pub config_path: PathBuf,
    pub dry_run: bool,
    pub spawn_workers: bool,
}

struct WorkerHandle {
    registration: WorkerRegistration,
    writer: Arc<Mutex<OwnedWriteHalf>>,
}

pub async fn run(options: CoordinatorOptions) -> Result<()> {
    let config = load_config(&options.config_path)?;
    let accounts = config
        .enabled_worker_accounts()
        .cloned()
        .collect::<Vec<_>>();

    let listener = TcpListener::bind(&config.process.ipc_bind_addr)
        .await
        .with_context(|| {
            format!(
                "failed to bind coordinator IPC listener on {}",
                config.process.ipc_bind_addr
            )
        })?;

    info!(
        app = %config.app.name,
        environment = %config.app.environment,
        dex = %config.hyperliquid.dex,
        bind_addr = %config.process.ipc_bind_addr,
        role = %config.process.role,
        worker_heartbeat_ms = config.process.worker_heartbeat_ms,
        account_workers = accounts.len(),
        dry_run = options.dry_run,
        "coordinator listening"
    );

    let mut children = if options.spawn_workers {
        spawn_workers(&options, &config.process.ipc_bind_addr, &accounts).await?
    } else {
        Vec::new()
    };

    let (report_tx, mut report_rx) = mpsc::channel::<WorkerReport>(256);
    let mut workers = accept_workers(
        &listener,
        &accounts,
        Duration::from_millis(config.process.worker_startup_timeout_ms),
        report_tx,
    )
    .await?;

    let signal = build_dry_run_signal(&config, &accounts);
    broadcast_signal(&mut workers, &signal).await?;

    let expected = workers.len();
    let mut submitted = HashSet::<String>::new();
    let mut rejected = Vec::new();
    let wait_reports = wait_for_reports(
        &mut report_rx,
        &mut submitted,
        &mut rejected,
        expected,
        Duration::from_millis(config.process.signal_ttl_ms + 2500),
    )
    .await;

    shutdown_workers(&mut workers).await;
    wait_for_children(&mut children).await;

    wait_reports?;

    if submitted.len() != expected {
        anyhow::bail!(
            "dry-run failed: received submitted reports from {}/{} workers; rejected={:?}",
            submitted.len(),
            expected,
            rejected
        );
    }

    println!(
        "Dry-run fan-out succeeded: signal {} submitted by {} account workers",
        signal.signal_id,
        submitted.len()
    );
    for account_id in submitted {
        println!("  - {account_id}: dry-run submitted");
    }

    Ok(())
}

async fn spawn_workers(
    options: &CoordinatorOptions,
    coordinator_addr: &str,
    accounts: &[AccountConfig],
) -> Result<Vec<Child>> {
    let exe = std::env::current_exe().context("failed to resolve current executable")?;
    let mut children = Vec::with_capacity(accounts.len());

    for account in accounts {
        let mut command = tokio::process::Command::new(&exe);
        command
            .arg("worker")
            .arg("--account-id")
            .arg(&account.account_id)
            .arg("--config")
            .arg(&options.config_path)
            .arg("--coordinator-addr")
            .arg(coordinator_addr)
            .arg("--dry-run")
            .arg(options.dry_run.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());

        let child = command
            .spawn()
            .with_context(|| format!("failed to spawn worker {}", account.account_id))?;
        children.push(child);
    }

    Ok(children)
}

async fn accept_workers(
    listener: &TcpListener,
    accounts: &[AccountConfig],
    startup_timeout: Duration,
    report_tx: mpsc::Sender<WorkerReport>,
) -> Result<HashMap<String, WorkerHandle>> {
    let expected_accounts = accounts
        .iter()
        .map(|account| account.account_id.clone())
        .collect::<HashSet<_>>();
    let mut workers = HashMap::new();

    while workers.len() < expected_accounts.len() {
        let (stream, _) = timeout(startup_timeout, listener.accept())
            .await
            .context("timed out waiting for account workers to connect")?
            .context("failed to accept worker connection")?;
        let (read_half, write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        let registration = match read_json_line::<WorkerMessage>(&mut reader).await? {
            Some(WorkerMessage::Register(registration)) => registration,
            Some(WorkerMessage::Report(_)) => {
                anyhow::bail!("worker sent report before registration");
            }
            None => anyhow::bail!("worker disconnected before registration"),
        };

        if !expected_accounts.contains(&registration.account_id) {
            anyhow::bail!(
                "unexpected worker account {} connected",
                registration.account_id
            );
        }

        let writer = Arc::new(Mutex::new(write_half));
        let worker_id = registration.worker_id.clone();
        let account_id = registration.account_id.clone();
        let tx = report_tx.clone();
        tokio::spawn(async move {
            let mut reader = reader;
            loop {
                match read_json_line::<WorkerMessage>(&mut reader).await {
                    Ok(Some(WorkerMessage::Report(report))) => {
                        if tx.send(report).await.is_err() {
                            break;
                        }
                    }
                    Ok(Some(WorkerMessage::Register(_))) => {
                        warn!(worker_id, account_id, "worker sent duplicate registration");
                    }
                    Ok(None) => break,
                    Err(error) => {
                        warn!(worker_id, account_id, %error, "worker IPC read failed");
                        break;
                    }
                }
            }
        });

        info!(
            worker_id = %registration.worker_id,
            account_id = %registration.account_id,
            dry_run = registration.dry_run,
            "account worker registered"
        );
        workers.insert(
            registration.account_id.clone(),
            WorkerHandle {
                registration,
                writer,
            },
        );
    }

    Ok(workers)
}

fn build_dry_run_signal(
    config: &crate::config::AppConfig,
    accounts: &[AccountConfig],
) -> CoordinatorSignal {
    let now = now_ms();
    let signal_id = format!("dryrun-{now}");
    let target_accounts = accounts
        .iter()
        .map(|account| account.account_id.clone())
        .collect::<Vec<_>>();

    CoordinatorSignal {
        signal_id: signal_id.clone(),
        source: SignalSource::DryRun,
        created_at_ms: now,
        dispatch_at_ms: now,
        expires_at_ms: now + config.process.signal_ttl_ms,
        target_accounts,
        dedupe_key: format!("manual-dryrun-{now}"),
        order: SignalOrder {
            market: None,
            dex: None,
            coin: config.default_coin(),
            side: OrderSide::Buy,
            notional_usd: config
                .manual_ops
                .max_manual_order_notional_usd
                .clamp(1.0, 25.0),
            reduce_only: false,
            execution_mode: ExecutionMode::Taker,
            max_slippage_bps: 20.0,
            limit_price: None,
            apply_account_ratio: false,
        },
    }
}

async fn broadcast_signal(
    workers: &mut HashMap<String, WorkerHandle>,
    signal: &CoordinatorSignal,
) -> Result<()> {
    for worker in workers.values_mut() {
        let mut writer = worker.writer.lock().await;
        write_json_line(
            &mut writer,
            &CoordinatorMessage::Signal(Box::new(signal.clone())),
        )
        .await
        .with_context(|| {
            format!(
                "failed to send signal {} to worker {}",
                signal.signal_id, worker.registration.account_id
            )
        })?;
    }
    Ok(())
}

async fn wait_for_reports(
    report_rx: &mut mpsc::Receiver<WorkerReport>,
    submitted: &mut HashSet<String>,
    rejected: &mut Vec<String>,
    expected: usize,
    wait_timeout: Duration,
) -> Result<()> {
    let wait = async {
        while submitted.len() < expected {
            let Some(report) = report_rx.recv().await else {
                break;
            };
            match report {
                WorkerReport::Ack(ack) => {
                    info!(
                        account_id = %ack.account_id,
                        signal_id = %ack.signal_id,
                        "worker acked signal"
                    );
                }
                WorkerReport::Submitted(order) => {
                    info!(
                        account_id = %order.account_id,
                        signal_id = %order.signal_id,
                        cloid = %order.cloid,
                        dry_run = order.dry_run,
                        "worker submitted dry-run order"
                    );
                    submitted.insert(order.account_id);
                }
                WorkerReport::Rejected(rejection) => {
                    warn!(
                        account_id = %rejection.account_id,
                        reason = %rejection.reason_code,
                        message = %rejection.message,
                        "worker rejected signal"
                    );
                    rejected.push(format!(
                        "{}:{}",
                        rejection.account_id, rejection.reason_code
                    ));
                }
                WorkerReport::Health(_) => {}
                WorkerReport::Error(error) => {
                    rejected.push(format!("{}:{}", error.account_id, error.message));
                }
            }
        }
    };

    timeout(wait_timeout, wait)
        .await
        .context("timed out waiting for dry-run worker reports")?;
    Ok(())
}

async fn shutdown_workers(workers: &mut HashMap<String, WorkerHandle>) {
    for worker in workers.values_mut() {
        let mut writer = worker.writer.lock().await;
        let _ = write_json_line(&mut writer, &CoordinatorMessage::Shutdown).await;
    }
}

async fn wait_for_children(children: &mut [Child]) {
    for child in children {
        match timeout(Duration::from_secs(5), child.wait()).await {
            Ok(Ok(status)) if status.success() => {}
            Ok(Ok(status)) => warn!(%status, "worker exited with non-zero status"),
            Ok(Err(error)) => warn!(%error, "failed to wait for worker"),
            Err(_) => {
                warn!("worker did not exit after shutdown; killing");
                let _ = child.kill().await;
            }
        }
    }
}
