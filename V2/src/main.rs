pub mod audit;
pub mod config;
pub mod coordinator;
pub mod domain;
pub mod frontend;
pub mod hyperliquid;
pub mod ipc;
pub mod manual_ops;
pub mod realtime;
pub mod risk;
pub mod secrets;
pub mod strategies;
pub mod strategy;
pub mod trading;
pub mod v2_runtime;
pub mod worker;
pub mod ws_post;

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

const DEFAULT_CONFIG_PATH: &str = "config/dry-run.example.toml";

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[cfg(test)]
mod tests {
    use std::{fs, process::Command};

    use anyhow::{Context, Result};

    use crate::{coordinator, domain::now_ms, frontend, worker};

    #[test]
    fn dry_run_worker_child_process() -> Result<()> {
        let Ok(account_id) = std::env::var("TRADE_XYZ_TEST_WORKER_ACCOUNT") else {
            return Ok(());
        };
        let config_path = std::env::var("TRADE_XYZ_TEST_CONFIG")
            .context("TRADE_XYZ_TEST_CONFIG is required for worker child")?;
        let coordinator_addr = std::env::var("TRADE_XYZ_TEST_COORDINATOR_ADDR")
            .context("TRADE_XYZ_TEST_COORDINATOR_ADDR is required for worker child")?;

        tokio::runtime::Runtime::new()?.block_on(worker::run(worker::WorkerOptions {
            account_id,
            config_path: config_path.into(),
            coordinator_addr: Some(coordinator_addr),
            dry_run: true,
        }))
    }

    #[test]
    fn dry_run_fanout_with_worker_processes() -> Result<()> {
        let test_id = now_ms();
        let coordinator_addr = format!("127.0.0.1:{}", 18_000 + (test_id % 10_000));
        let config_path = write_test_config(&coordinator_addr)?;
        let current_exe = std::env::current_exe().context("failed to resolve test executable")?;

        let mut children = Vec::new();
        for account_id in ["addr_a", "addr_b"] {
            let child = Command::new(&current_exe)
                .arg("tests::dry_run_worker_child_process")
                .arg("--exact")
                .arg("--nocapture")
                .env("TRADE_XYZ_TEST_WORKER_ACCOUNT", account_id)
                .env("TRADE_XYZ_TEST_CONFIG", &config_path)
                .env("TRADE_XYZ_TEST_COORDINATOR_ADDR", &coordinator_addr)
                .spawn()
                .with_context(|| format!("failed to spawn test worker {account_id}"))?;
            children.push(child);
        }

        tokio::runtime::Runtime::new()?.block_on(coordinator::run(
            coordinator::CoordinatorOptions {
                config_path: config_path.clone().into(),
                dry_run: true,
                spawn_workers: false,
            },
        ))?;

        for mut child in children {
            let status = child.wait().context("failed to wait for test worker")?;
            anyhow::ensure!(status.success(), "test worker exited with {status}");
        }

        Ok(())
    }

    #[test]
    fn frontend_console_server_child_process() -> Result<()> {
        if std::env::var("TRADE_XYZ_TEST_FRONTEND_SERVER")
            .ok()
            .as_deref()
            != Some("1")
        {
            return Ok(());
        }
        let config_path = std::env::var("TRADE_XYZ_TEST_CONFIG")
            .context("TRADE_XYZ_TEST_CONFIG is required for frontend child")?;
        let bind_addr = std::env::var("TRADE_XYZ_TEST_FRONTEND_BIND")
            .unwrap_or_else(|_| "127.0.0.1:8790".to_string());

        tokio::runtime::Runtime::new()?.block_on(frontend::run(frontend::FrontendOptions {
            config_path: config_path.into(),
            bind_addr,
            dry_run: true,
        }))
    }

    fn write_test_config(coordinator_addr: &str) -> Result<String> {
        let dir = std::env::temp_dir().join(format!("trade_xyz_bot_test_{}", now_ms()));
        fs::create_dir_all(&dir).context("failed to create test config dir")?;
        let path = dir.join("dry-run.toml");
        let config = format!(
            r#"
[app]
name = "trade_xyz_bot_test"
environment = "testnet"
dry_run = true
fail_closed = true

[hyperliquid]
info_url = "https://api.hyperliquid.xyz/info"
exchange_url = "https://api.hyperliquid.xyz/exchange"
ws_url = "wss://api.hyperliquid.xyz/ws"
dex = "xyz"

[process]
role = "coordinator"
ipc_bind_addr = "{coordinator_addr}"
worker_heartbeat_ms = 500
signal_ttl_ms = 3000
worker_startup_timeout_ms = 10000

[[accounts]]
account_id = "addr_a"
address = "0x0000000000000000000000000000000000000001"
api_wallet_env = "HL_API_WALLET_PRIVATE_KEY_ADDR_A"
enabled = true
worker_enabled = true
copy_ratio = 0.10
max_order_notional_usd = 100.0

[[accounts]]
account_id = "addr_b"
address = "0x0000000000000000000000000000000000000002"
api_wallet_env = "HL_API_WALLET_PRIVATE_KEY_ADDR_B"
enabled = true
worker_enabled = true
copy_ratio = 0.05
max_order_notional_usd = 100.0

[manual_ops]
enabled = true
manual_trading_enabled = true
manual_live_enabled = false
require_confirm_above_notional_usd = 100.0
max_manual_order_notional_usd = 25.0
max_manual_batch_accounts = 5
blocked_symbols = []

[module_symbol_policies]
manual_blocked_symbols = []
fib_blocked_symbols = []
copy_blocked_symbols = []
"#
        );
        fs::write(&path, config).context("failed to write test config")?;
        Ok(path.to_string_lossy().into_owned())
    }
}

#[derive(Debug, Subcommand)]
enum Command {
    SmokeTest {
        #[arg(long)]
        info_url: Option<String>,
    },
    Coordinator {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        dry_run: bool,
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        spawn_workers: bool,
    },
    Worker {
        #[arg(long)]
        account_id: String,
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long)]
        coordinator_addr: Option<String>,
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        dry_run: bool,
    },
    DryRun {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },
    Console {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long, default_value = "127.0.0.1:8790")]
        bind: String,
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        dry_run: bool,
    },
    SignedSmoke {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long)]
        account_id: String,
        #[arg(long, default_value = "xyz:XYZ100")]
        coin: String,
        #[arg(long, default_value = "buy")]
        side: String,
        #[arg(long, default_value_t = 1.0)]
        notional_usd: f64,
        #[arg(long, default_value_t = 20.0)]
        max_slippage_bps: f64,
        #[arg(long, default_value = "taker")]
        execution_mode: String,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        reduce_only: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        submit: bool,
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        cancel_resting: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        confirm_mainnet_live: bool,
    },
    SignedAcceptance {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long)]
        account_id: String,
        #[arg(long, default_value = "xyz:XYZ100")]
        coin: String,
        #[arg(long, default_value = "buy")]
        side: String,
        #[arg(long, default_value_t = 1.0)]
        notional_usd: f64,
        #[arg(long, default_value_t = 20.0)]
        max_slippage_bps: f64,
        #[arg(long, default_value = "taker")]
        execution_mode: String,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        reduce_only: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        submit: bool,
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        cancel_resting: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        confirm_mainnet_live: bool,
    },
    SignedPreflight {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long)]
        account_id: String,
        #[arg(long, default_value = "xyz:XYZ100")]
        coin: String,
        #[arg(long, default_value = "buy")]
        side: String,
        #[arg(long, default_value_t = 1.0)]
        notional_usd: f64,
        #[arg(long, default_value_t = 20.0)]
        max_slippage_bps: f64,
        #[arg(long, default_value = "taker")]
        execution_mode: String,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        reduce_only: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        confirm_mainnet_live: bool,
    },
    SignedRunbook {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long)]
        account_id: String,
        #[arg(long, default_value = "xyz:XYZ100")]
        coin: String,
        #[arg(long, default_value = "buy")]
        side: String,
        #[arg(long, default_value_t = 1.0)]
        notional_usd: f64,
        #[arg(long, default_value_t = 20.0)]
        max_slippage_bps: f64,
        #[arg(long, default_value = "taker")]
        execution_mode: String,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        reduce_only: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        submit: bool,
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        cancel_resting: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        confirm_mainnet_live: bool,
    },
    SignedLiveWindow {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long = "account-id")]
        account_ids: Vec<String>,
        #[arg(long, default_value = "xyz:XYZ100")]
        coin: String,
        #[arg(long, default_value = "buy")]
        side: String,
        #[arg(long, default_value_t = 1.0)]
        notional_usd: f64,
        #[arg(long, default_value_t = 20.0)]
        max_slippage_bps: f64,
        #[arg(long, default_value = "taker")]
        execution_mode: String,
        #[arg(long, default_value = ".codex-longrun/mainnet-order-live-window.toml")]
        output_config: PathBuf,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        write: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        overwrite: bool,
    },
    MainnetSmokePlan {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long = "account-id")]
        account_ids: Vec<String>,
        #[arg(long, default_value_t = 2.0)]
        funding_amount_usdc: f64,
        #[arg(long)]
        destination_dex: Option<String>,
        #[arg(long, default_value = "xyz:NVDA")]
        coin: String,
        #[arg(long, default_value = "buy")]
        side: String,
        #[arg(long, default_value_t = 1.0)]
        order_notional_usd: f64,
        #[arg(long, default_value_t = 20.0)]
        max_slippage_bps: f64,
        #[arg(long, default_value = "taker")]
        execution_mode: String,
        #[arg(
            long,
            default_value = ".codex-longrun/mainnet-usdc-transfer-window.toml"
        )]
        transfer_output_config: PathBuf,
        #[arg(long, default_value = ".codex-longrun/mainnet-order-live-window.toml")]
        order_output_config: PathBuf,
    },
    SignedCancel {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long)]
        account_id: String,
        #[arg(long, default_value = "xyz:XYZ100")]
        coin: String,
        #[arg(long)]
        cloid: String,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        confirm_mainnet_live: bool,
    },
    UsdcDexTransfer {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        destination_account_id: Option<String>,
        #[arg(long)]
        amount_usdc: f64,
        #[arg(long)]
        source_dex: Option<String>,
        #[arg(long)]
        destination_dex: Option<String>,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        submit: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        confirm_mainnet_live: bool,
    },
    UsdcDexTransferPreflight {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        destination_account_id: Option<String>,
        #[arg(long)]
        amount_usdc: f64,
        #[arg(long)]
        source_dex: Option<String>,
        #[arg(long)]
        destination_dex: Option<String>,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        confirm_mainnet_live: bool,
    },
    UsdcDexTransferRunbook {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        destination_account_id: Option<String>,
        #[arg(long)]
        amount_usdc: f64,
        #[arg(long)]
        source_dex: Option<String>,
        #[arg(long)]
        destination_dex: Option<String>,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        submit: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        confirm_mainnet_live: bool,
    },
    UsdcDexTransferBatchPreflight {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long = "account-id")]
        account_ids: Vec<String>,
        #[arg(long)]
        destination_account_id: Option<String>,
        #[arg(long)]
        amount_usdc: f64,
        #[arg(long)]
        source_dex: Option<String>,
        #[arg(long)]
        destination_dex: Option<String>,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        confirm_mainnet_live: bool,
    },
    UsdcDexTransferLiveWindow {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long = "account-id")]
        account_ids: Vec<String>,
        #[arg(long)]
        amount_usdc: f64,
        #[arg(long)]
        destination_dex: Option<String>,
        #[arg(
            long,
            default_value = ".codex-longrun/mainnet-usdc-transfer-window.toml"
        )]
        output_config: PathBuf,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        write: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        overwrite: bool,
    },
    AccountFunding {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long = "account-id")]
        account_ids: Vec<String>,
    },
    ReconcileAccount {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long)]
        account_id: String,
    },
    OrderStatus {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        oid: Option<u64>,
        #[arg(long)]
        cloid: Option<String>,
    },
}

fn main() -> Result<()> {
    let handle = std::thread::Builder::new()
        .name("trade_xyz_runtime".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_stack_size(16 * 1024 * 1024)
                .build()
                .context("failed to build Tokio runtime")?;
            runtime.block_on(async_main())
        })
        .context("failed to spawn trade_xyz runtime thread")?;

    handle
        .join()
        .map_err(|_| anyhow::anyhow!("trade_xyz runtime thread panicked"))?
}

async fn async_main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .init();

    let cli = Cli::parse();

    match cli.command.unwrap_or(Command::SmokeTest { info_url: None }) {
        Command::SmokeTest { info_url } => hyperliquid::run_smoke_test(info_url).await,
        Command::Coordinator {
            config,
            dry_run,
            spawn_workers,
        } => {
            coordinator::run(coordinator::CoordinatorOptions {
                config_path: config,
                dry_run,
                spawn_workers,
            })
            .await
        }
        Command::DryRun { config } => {
            coordinator::run(coordinator::CoordinatorOptions {
                config_path: config,
                dry_run: true,
                spawn_workers: true,
            })
            .await
        }
        Command::Console {
            config,
            bind,
            dry_run,
        } => {
            frontend::run(frontend::FrontendOptions {
                config_path: config,
                bind_addr: bind,
                dry_run,
            })
            .await
        }
        Command::Worker {
            account_id,
            config,
            coordinator_addr,
            dry_run,
        } => {
            worker::run(worker::WorkerOptions {
                account_id,
                config_path: config,
                coordinator_addr,
                dry_run,
            })
            .await
        }
        Command::SignedSmoke {
            config,
            account_id,
            coin,
            side,
            notional_usd,
            max_slippage_bps,
            execution_mode,
            reduce_only,
            submit,
            cancel_resting,
            confirm_mainnet_live,
        } => {
            let config = config::load_config(&config)?;
            let side = parse_order_side(&side)?;
            let execution_mode = parse_execution_mode(&execution_mode)?;
            trading::run_signed_smoke(
                config,
                trading::SignedSmokeOptions {
                    account_id,
                    coin,
                    side,
                    notional_usd,
                    max_slippage_bps,
                    execution_mode,
                    reduce_only,
                    close_full_position: false,
                    submit,
                    cancel_resting,
                    confirm_mainnet_live,
                },
            )
            .await
        }
        Command::SignedAcceptance {
            config,
            account_id,
            coin,
            side,
            notional_usd,
            max_slippage_bps,
            execution_mode,
            reduce_only,
            submit,
            cancel_resting,
            confirm_mainnet_live,
        } => {
            let config = config::load_config(&config)?;
            let side = parse_order_side(&side)?;
            let execution_mode = parse_execution_mode(&execution_mode)?;
            trading::run_signed_acceptance(
                config,
                trading::SignedAcceptanceOptions {
                    account_id,
                    coin,
                    side,
                    notional_usd,
                    max_slippage_bps,
                    execution_mode,
                    reduce_only,
                    close_full_position: false,
                    submit,
                    cancel_resting,
                    confirm_mainnet_live,
                },
            )
            .await
        }
        Command::SignedPreflight {
            config,
            account_id,
            coin,
            side,
            notional_usd,
            max_slippage_bps,
            execution_mode,
            reduce_only,
            confirm_mainnet_live,
        } => {
            let config = config::load_config(&config)?;
            let side = parse_order_side(&side)?;
            let execution_mode = parse_execution_mode(&execution_mode)?;
            trading::run_signed_preflight(
                config,
                trading::SignedPreflightOptions {
                    account_id,
                    coin,
                    side,
                    notional_usd,
                    max_slippage_bps,
                    execution_mode,
                    reduce_only,
                    close_full_position: false,
                    confirm_mainnet_live,
                },
            )
            .await
        }
        Command::SignedRunbook {
            config,
            account_id,
            coin,
            side,
            notional_usd,
            max_slippage_bps,
            execution_mode,
            reduce_only,
            submit,
            cancel_resting,
            confirm_mainnet_live,
        } => {
            let config = config::load_config(&config)?;
            let side = parse_order_side(&side)?;
            let execution_mode = parse_execution_mode(&execution_mode)?;
            trading::run_signed_runbook(
                config,
                trading::SignedRunbookOptions {
                    account_id,
                    coin,
                    side,
                    notional_usd,
                    max_slippage_bps,
                    execution_mode,
                    reduce_only,
                    close_full_position: false,
                    submit,
                    cancel_resting,
                    confirm_mainnet_live,
                },
            )
            .await
        }
        Command::SignedLiveWindow {
            config,
            account_ids,
            coin,
            side,
            notional_usd,
            max_slippage_bps,
            execution_mode,
            output_config,
            write,
            overwrite,
        } => {
            let loaded_config = config::load_config(&config)?;
            let side = parse_order_side(&side)?;
            let execution_mode = parse_execution_mode(&execution_mode)?;
            trading::run_signed_live_window(
                config,
                loaded_config,
                trading::SignedLiveWindowOptions {
                    account_ids,
                    coin,
                    side,
                    notional_usd,
                    max_slippage_bps,
                    execution_mode,
                    output_config_path: output_config,
                    write,
                    overwrite,
                },
            )
        }
        Command::MainnetSmokePlan {
            config,
            account_ids,
            funding_amount_usdc,
            destination_dex,
            coin,
            side,
            order_notional_usd,
            max_slippage_bps,
            execution_mode,
            transfer_output_config,
            order_output_config,
        } => {
            let loaded_config = config::load_config(&config)?;
            let side = parse_order_side(&side)?;
            let execution_mode = parse_execution_mode(&execution_mode)?;
            Box::pin(trading::run_mainnet_smoke_plan(
                config,
                loaded_config,
                trading::MainnetSmokePlanOptions {
                    account_ids,
                    funding_amount_usdc,
                    destination_dex,
                    coin,
                    side,
                    order_notional_usd,
                    max_slippage_bps,
                    execution_mode,
                    transfer_output_config_path: transfer_output_config,
                    order_output_config_path: order_output_config,
                },
            ))
            .await
        }
        Command::SignedCancel {
            config,
            account_id,
            coin,
            cloid,
            confirm_mainnet_live,
        } => {
            let config = config::load_config(&config)?;
            trading::run_signed_cancel_by_cloid(
                config,
                account_id,
                coin,
                cloid,
                confirm_mainnet_live,
            )
            .await
        }
        Command::UsdcDexTransfer {
            config,
            account_id,
            destination_account_id,
            amount_usdc,
            source_dex,
            destination_dex,
            submit,
            confirm_mainnet_live,
        } => {
            let config = config::load_config(&config)?;
            trading::run_usdc_dex_transfer(
                config,
                trading::UsdcDexTransferOptions {
                    account_id,
                    destination_account_id,
                    amount_usdc,
                    source_dex,
                    destination_dex,
                    submit,
                    confirm_mainnet_live,
                },
            )
            .await
        }
        Command::UsdcDexTransferPreflight {
            config,
            account_id,
            destination_account_id,
            amount_usdc,
            source_dex,
            destination_dex,
            confirm_mainnet_live,
        } => {
            let config = config::load_config(&config)?;
            trading::run_usdc_dex_transfer_preflight(
                config,
                trading::UsdcDexTransferOptions {
                    account_id,
                    destination_account_id,
                    amount_usdc,
                    source_dex,
                    destination_dex,
                    submit: false,
                    confirm_mainnet_live,
                },
            )
            .await
        }
        Command::UsdcDexTransferRunbook {
            config,
            account_id,
            destination_account_id,
            amount_usdc,
            source_dex,
            destination_dex,
            submit,
            confirm_mainnet_live,
        } => {
            let config = config::load_config(&config)?;
            trading::run_usdc_dex_transfer_runbook(
                config,
                trading::UsdcDexTransferOptions {
                    account_id,
                    destination_account_id,
                    amount_usdc,
                    source_dex,
                    destination_dex,
                    submit,
                    confirm_mainnet_live,
                },
            )
            .await
        }
        Command::UsdcDexTransferBatchPreflight {
            config,
            account_ids,
            destination_account_id,
            amount_usdc,
            source_dex,
            destination_dex,
            confirm_mainnet_live,
        } => {
            let config = config::load_config(&config)?;
            Box::pin(trading::run_usdc_dex_transfer_batch_preflight(
                config,
                trading::UsdcDexTransferBatchPreflightOptions {
                    account_ids,
                    destination_account_id,
                    amount_usdc,
                    source_dex,
                    destination_dex,
                    confirm_mainnet_live,
                },
            ))
            .await
        }
        Command::UsdcDexTransferLiveWindow {
            config,
            account_ids,
            amount_usdc,
            destination_dex,
            output_config,
            write,
            overwrite,
        } => {
            let loaded_config = config::load_config(&config)?;
            trading::run_usdc_dex_transfer_live_window(
                config,
                loaded_config,
                trading::UsdcDexTransferLiveWindowOptions {
                    account_ids,
                    amount_usdc,
                    destination_dex,
                    output_config_path: output_config,
                    write,
                    overwrite,
                },
            )
        }
        Command::AccountFunding {
            config,
            account_ids,
        } => {
            let config = config::load_config(&config)?;
            trading::run_account_funding(config, account_ids).await
        }
        Command::ReconcileAccount { config, account_id } => {
            let config = config::load_config(&config)?;
            trading::run_account_reconciliation(config, account_id).await
        }
        Command::OrderStatus {
            config,
            account_id,
            oid,
            cloid,
        } => {
            let config = config::load_config(&config)?;
            trading::run_order_status(config, account_id, oid, cloid).await
        }
    }
}

fn parse_order_side(value: &str) -> Result<domain::OrderSide> {
    match value.trim().to_ascii_lowercase().as_str() {
        "buy" | "b" => Ok(domain::OrderSide::Buy),
        "sell" | "s" => Ok(domain::OrderSide::Sell),
        other => anyhow::bail!("unsupported side {other}; use buy or sell"),
    }
}

fn parse_execution_mode(value: &str) -> Result<domain::ExecutionMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "taker" | "ioc" => Ok(domain::ExecutionMode::Taker),
        "maker" | "alo" => Ok(domain::ExecutionMode::Maker),
        other => anyhow::bail!("unsupported execution mode {other}; use taker or maker"),
    }
}
