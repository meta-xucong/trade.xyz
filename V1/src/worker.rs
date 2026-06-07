use std::path::PathBuf;

use anyhow::{Context, Result};
use tokio::{io::BufReader, net::TcpStream, time::Duration};
use tracing::{info, warn};

use crate::{
    config::load_config,
    domain::{CoordinatorSignal, WorkerAck, WorkerError, WorkerRegistration, WorkerReport, now_ms},
    ipc::{CoordinatorMessage, WorkerMessage, read_json_line, write_json_line},
    risk::{RiskContext, RiskDecision, RiskGateway},
    secrets::{account_secret_id, load_account_secret},
    trading::AccountExecutor,
};

#[derive(Debug)]
pub struct WorkerOptions {
    pub account_id: String,
    pub config_path: PathBuf,
    pub coordinator_addr: Option<String>,
    pub dry_run: bool,
}

pub async fn run(options: WorkerOptions) -> Result<()> {
    let config = load_config(&options.config_path)?;
    let account = config
        .account(&options.account_id)
        .cloned()
        .with_context(|| format!("account {} not found in config", options.account_id))?;

    if !account.enabled || !account.worker_enabled {
        anyhow::bail!("account {} worker is disabled", account.account_id);
    }

    let executor = if options.dry_run {
        AccountExecutor::dry_run(true)
    } else {
        let vault_password = std::env::var("TRADE_XYZ_VAULT_PASSWORD").ok();
        let secret = load_account_secret(&config, &account, vault_password.as_deref())?;
        info!(
            account_id = %account.account_id,
            secret_id = %account_secret_id(&account),
            "account worker unlocked API wallet secret"
        );
        AccountExecutor::live(config.clone(), account.clone(), secret)
    };

    let coordinator_addr = options
        .coordinator_addr
        .clone()
        .unwrap_or_else(|| config.process.ipc_bind_addr.clone());
    let stream = connect_with_retry(&coordinator_addr).await?;
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    let worker_id = format!("worker-{}", account.account_id);
    let registration = WorkerRegistration {
        worker_id: worker_id.clone(),
        account_id: account.account_id.clone(),
        address: account.address.clone(),
        pid: std::process::id(),
        dry_run: options.dry_run,
    };
    write_json_line(&mut write_half, &WorkerMessage::Register(registration)).await?;

    info!(
        worker_id,
        account_id = %account.account_id,
        coordinator_addr,
        dry_run = options.dry_run,
        "account worker connected"
    );

    loop {
        let Some(message) = read_json_line::<CoordinatorMessage>(&mut reader).await? else {
            warn!("coordinator disconnected");
            break;
        };
        match message {
            CoordinatorMessage::Signal(signal) => {
                handle_signal(
                    &config,
                    &worker_id,
                    &account,
                    &executor,
                    options.dry_run,
                    *signal,
                    &mut write_half,
                )
                .await?;
            }
            CoordinatorMessage::Shutdown => {
                info!(account_id = %account.account_id, "worker shutting down");
                break;
            }
        }
    }

    Ok(())
}

async fn connect_with_retry(coordinator_addr: &str) -> Result<TcpStream> {
    let mut last_error = None;
    for _ in 0..100 {
        match TcpStream::connect(coordinator_addr).await {
            Ok(stream) => return Ok(stream),
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    let error = last_error.context("worker did not attempt to connect")?;
    Err(error).with_context(|| format!("failed to connect to coordinator at {coordinator_addr}"))
}

async fn handle_signal(
    config: &crate::config::AppConfig,
    worker_id: &str,
    account: &crate::config::AccountConfig,
    executor: &AccountExecutor,
    dry_run: bool,
    signal: CoordinatorSignal,
    write_half: &mut tokio::net::tcp::OwnedWriteHalf,
) -> Result<()> {
    let ack = WorkerReport::Ack(WorkerAck {
        signal_id: signal.signal_id.clone(),
        worker_id: worker_id.to_string(),
        account_id: account.account_id.clone(),
        received_at_ms: now_ms(),
    });
    write_json_line(write_half, &WorkerMessage::Report(ack)).await?;

    let module_scope = signal.source.module_scope();
    let intent = signal.to_trade_intent(&account.account_id, worker_id, account.copy_ratio);
    let risk_context = RiskContext::from_account_for_module(config, account, dry_run, module_scope);
    match RiskGateway::dry_run_default().evaluate(&risk_context, intent) {
        RiskDecision::Approved(order) => {
            let submitted = executor.submit(order).await;
            write_json_line(write_half, &WorkerMessage::Report(submitted)).await?;
        }
        RiskDecision::Rejected(rejection) => {
            write_json_line(
                write_half,
                &WorkerMessage::Report(WorkerReport::Rejected(rejection)),
            )
            .await?;
        }
    }
    Ok(())
}

#[allow(dead_code)]
fn worker_error(
    account: &crate::config::AccountConfig,
    worker_id: &str,
    message: String,
) -> WorkerReport {
    WorkerReport::Error(WorkerError {
        worker_id: worker_id.to_string(),
        account_id: account.account_id.clone(),
        message,
        error_at_ms: now_ms(),
    })
}
