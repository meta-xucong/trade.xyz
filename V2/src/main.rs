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

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde::Serialize;
use serde_json::Value;

const DEFAULT_CONFIG_PATH: &str = "config/dry-run.example.toml";
const COPY_CANARY_ORDER_EVIDENCE_RETRIES: usize = 8;
const COPY_CANARY_ORDER_EVIDENCE_RETRY_DELAY_MS: u64 = 3_000;
const COPY_CANARY_FILL_LOOKBACK_MS: u64 = 60_000;
const COPY_DAEMON_MIN_SIGNAL_DELAY_MS: u64 = 30_000;
const COPY_CANARY_FILL_LOOKAHEAD_MS: u64 = 180_000;
const COPY_RECONCILE_RETRIES: usize = 3;
const COPY_RECONCILE_RETRY_DELAY_MS: u64 = 1_500;
const COPY_DAEMON_SUBMIT_REF_TIMEOUT_SECS: u64 = 120;
const COPY_DAEMON_ORDER_EVIDENCE_TIMEOUT_SECS: u64 = 120;
const COPY_DAEMON_RECONCILE_BATCH_TIMEOUT_SECS: u64 = 120;
const COPY_DAEMON_EFFECTIVE_MIN_CHECK_TIMEOUT_SECS: u64 = 45;
const COPY_DAEMON_MAX_LEVERAGE_TIMEOUT_SECS: u64 = 30;
const COPY_DAEMON_LEVERAGE_UPDATE_TIMEOUT_SECS: u64 = 45;
const COPY_DAEMON_ORDER_SUBMIT_TIMEOUT_SECS: u64 = 60;
const COPY_DAEMON_MARGIN_BUFFER_RATIO: f64 = 0.10;
const COPY_DAEMON_FEE_BUFFER_RATIO: f64 = 0.001;
const COPY_DAEMON_RECOVERED_LEDGER_DUPLICATE_TOLERANCE_USD: f64 = 1.0;
const COPY_DAEMON_PNL_DRIFT_TOLERANCE_USD: f64 = 1.0;
const COPY_DAEMON_LINEAGE_RESIDUAL_TOLERANCE_USD: f64 = 0.005;

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        fs,
        process::Command,
        sync::atomic::{AtomicU64, Ordering},
    };

    use anyhow::{Context, Result};

    use crate::{coordinator, domain::now_ms, frontend, worker};

    use super::{
        CopyBoundedLiveWindowReconcile, CopyExecutionCanaryOptions,
        CopyExecutionCanaryOrderEvidence, CopyExecutionCanaryWouldSubmit,
        CopyLiveDaemonAcceptanceOptions, CopyLiveDaemonAcceptanceReport,
        CopyLiveDaemonPersistentLiveSubmitReport, CopyLiveDaemonRestartProbe,
        CopyLiveDaemonSubmitPlanContract, CopyLiveDaemonSupervisorOptions,
        CopyLiveDaemonSuppressedWouldSubmitRef, CopyLiveDaemonWouldSubmitRef,
        CopyLiveStabilitySoakOptions, CopyShadowSmokeCheck, CopyShadowSmokeLeader,
        CopyShadowSmokeOptions, append_unique_copy_daemon_would_submit_orders,
        append_unique_copy_daemon_would_submit_refs, approved_copy_daemon_order_from_ref,
        build_synthetic_copy_shadow_records, copy_bounded_live_window_ok,
        copy_daemon_live_leverage_update_options,
        copy_daemon_live_leverage_update_options_with_max,
        copy_daemon_submitted_reports_needing_cleanup, copy_execution_canary_report,
        copy_live_daemon_defer_open_refs_after_immediate_live_submit,
        copy_live_daemon_error_is_safe_pre_submit_skip,
        copy_live_daemon_error_is_submit_transport_failure,
        copy_live_daemon_immediate_submit_should_stop_round,
        copy_live_daemon_live_submit_health_ok,
        copy_live_daemon_merge_persistence_snapshots_for_save,
        copy_live_daemon_merge_persistent_live_submit_reports,
        copy_live_daemon_open_notional_usd_from_refs, copy_live_daemon_pending_plan_refs,
        copy_live_daemon_pending_suppressed_refs, copy_live_daemon_persistence_snapshot_for_save,
        copy_live_daemon_persistent_live_submit, copy_live_daemon_persistent_submit_dry_run,
        copy_live_daemon_persistent_submit_snapshot_safe_to_save,
        copy_live_daemon_prepare_refs_for_follow_position_limits,
        copy_live_daemon_reconcile_healthy_for_mode,
        copy_live_daemon_reconcile_only_degraded_round, copy_live_daemon_recoverable_watcher_error,
        copy_live_daemon_reduce_only_matching_position_notional_usd,
        copy_live_daemon_reduce_only_ref_has_matching_position,
        copy_live_daemon_submit_evidence_contract, copy_live_daemon_submit_plan_contract,
        copy_live_daemon_submitted_report_cloids, copy_live_daemon_supervisor_ok,
        copy_live_daemon_suppress_refs_rejected_by_submit_contract,
        copy_live_daemon_watcher_progress_check, copy_live_stability_round_submission_totals,
        copy_live_stability_soak_ok, copy_shadow_smoke_check, normalize_report_zero,
        partition_copy_live_daemon_would_submit_refs, plan_copy_daemon_acceptance_order_refs,
        run_copy_execution_canary, run_copy_live_daemon_acceptance, run_copy_shadow_smoke,
    };

    static TEST_CONFIG_SEQ: AtomicU64 = AtomicU64::new(1);

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

    #[test]
    fn copy_shadow_smoke_rejects_live_capable_config() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18000")?;
        let mut config = crate::config::load_config(std::path::Path::new(&config_path))?;
        config.app.dry_run = false;

        let error = run_copy_shadow_smoke(
            &config,
            CopyShadowSmokeOptions {
                leaders: vec!["leader_a=0x00000000000000000000000000000000000000aa".to_string()],
                coin: "xyz:XYZ100".to_string(),
                local_account_id: Some("addr_a".to_string()),
                shadow_history_path: std::env::temp_dir().join("unused-copy-shadow.jsonl"),
                synthetic_event: false,
                leader_notional_usd: 100.0,
                leader_size: 1.0,
            },
        )
        .expect_err("live-capable copy shadow smoke must fail closed");

        assert!(
            error
                .to_string()
                .contains("copy-shadow-smoke requires app.dry_run=true")
        );
        Ok(())
    }

    #[test]
    fn copy_shadow_smoke_synthetic_event_writes_shadow_history() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18001")?;
        let config = crate::config::load_config(std::path::Path::new(&config_path))?;
        let dir = std::env::temp_dir().join(format!("trade_xyz_copy_shadow_smoke_{}", now_ms()));
        fs::create_dir_all(&dir).context("failed to create copy smoke test dir")?;
        let shadow_history_path = dir.join("copy_shadow_history.jsonl");

        let report = run_copy_shadow_smoke(
            &config,
            CopyShadowSmokeOptions {
                leaders: vec!["leader_a=0x00000000000000000000000000000000000000aa".to_string()],
                coin: "xyz:XYZ100".to_string(),
                local_account_id: Some("addr_a".to_string()),
                shadow_history_path: shadow_history_path.clone(),
                synthetic_event: true,
                leader_notional_usd: 100.0,
                leader_size: 1.0,
            },
        )?;

        assert!(report.ok, "{report:#?}");
        assert_eq!(report.synthetic_records_written, 1);
        assert_eq!(report.recent_shadow_entries, 1);
        assert!(!report.watcher_subscriptions.is_empty());
        let entries = crate::strategies::smart_money::read_recent_copy_shadow_history_entries(
            &shadow_history_path,
            10,
        )?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].status, "would_copy");
        assert_eq!(entries[0].coin, "xyz:XYZ100");
        assert_eq!(entries[0].live_gate, "dry_run_only");
        Ok(())
    }

    #[test]
    fn copy_execution_canary_dry_run_submits_only_signal_targets() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18002")?;
        let config = crate::config::load_config(std::path::Path::new(&config_path))?;
        let dir =
            std::env::temp_dir().join(format!("trade_xyz_copy_execution_canary_{}", now_ms()));
        fs::create_dir_all(&dir).context("failed to create copy execution canary test dir")?;
        let shadow_history_path = dir.join("copy_shadow_history.jsonl");

        let report = tokio::runtime::Runtime::new()?.block_on(run_copy_execution_canary(
            &config,
            CopyExecutionCanaryOptions {
                leaders: vec!["leader_a=0x00000000000000000000000000000000000000aa".to_string()],
                account_ids: vec!["addr_a".to_string(), "addr_b".to_string()],
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                local_account_id: None,
                shadow_history_path: shadow_history_path.clone(),
                leader_notional_usd: 240.0,
                leader_size: 1.0,
                live: false,
                allow_live_submit: false,
                confirm_mainnet_live: false,
                cleanup_after_submit: false,
                cleanup_max_slippage_bps: 50.0,
                preflight_only: false,
                max_orders: 2,
            },
        ))?;

        assert!(report.ok, "{report:#?}");
        assert_eq!(report.approved_shadow_records, 2);
        assert_eq!(report.submitted_reports.len(), 2);
        let submitted_accounts = report
            .submitted_reports
            .iter()
            .map(|report| match report {
                crate::domain::WorkerReport::Submitted(submitted) => Ok((
                    submitted.account_id.clone(),
                    submitted.dry_run,
                    submitted.notional_usd,
                )),
                other => anyhow::bail!("unexpected canary report: {other:?}"),
            })
            .collect::<Result<Vec<_>>>()?;
        assert!(
            submitted_accounts
                .iter()
                .any(|(account_id, dry_run, notional)| account_id == "addr_a"
                    && *dry_run
                    && *notional
                        == crate::strategies::smart_money::COPY_DEFAULT_MAX_SIGNAL_NOTIONAL_USD)
        );
        assert!(
            submitted_accounts
                .iter()
                .any(|(account_id, dry_run, notional)| account_id == "addr_b"
                    && *dry_run
                    && *notional
                        == crate::strategies::smart_money::COPY_DEFAULT_MAX_SIGNAL_NOTIONAL_USD)
        );
        assert_eq!(report.ledger_reconciliations.len(), 2);
        assert!(report.ledger_reconciliations.iter().all(|result| {
            !result.applied && result.reason_code.as_deref() == Some("COPY_LEDGER_DRY_RUN_REPORT")
        }));
        assert_eq!(
            report.ledger_reconciliation_snapshot.ledger_entries.len(),
            2
        );
        assert!(
            report
                .ledger_reconciliation_snapshot
                .ledger_entries
                .iter()
                .all(|entry| matches!(
                    entry.status,
                    crate::strategies::smart_money::CopyLedgerStatus::PendingOpen
                ))
        );
        assert!(shadow_history_path.exists());
        Ok(())
    }

    #[test]
    fn copy_execution_canary_live_refuses_without_explicit_submit_gate() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18003")?;
        let mut config = crate::config::load_config(std::path::Path::new(&config_path))?;
        config.app.dry_run = false;
        config.manual_ops.manual_live_enabled = true;
        config.manual_ops.max_manual_order_notional_usd =
            crate::strategies::smart_money::COPY_DEFAULT_MAX_SIGNAL_NOTIONAL_USD;

        let report = tokio::runtime::Runtime::new()?.block_on(run_copy_execution_canary(
            &config,
            CopyExecutionCanaryOptions {
                leaders: vec!["leader_a=0x00000000000000000000000000000000000000aa".to_string()],
                account_ids: vec!["addr_a".to_string()],
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                local_account_id: None,
                shadow_history_path: std::env::temp_dir()
                    .join("unused-copy-execution-canary.jsonl"),
                leader_notional_usd: 240.0,
                leader_size: 1.0,
                live: true,
                allow_live_submit: false,
                confirm_mainnet_live: false,
                cleanup_after_submit: false,
                cleanup_max_slippage_bps: 50.0,
                preflight_only: false,
                max_orders: 1,
            },
        ))?;

        assert!(!report.ok);
        assert!(report.submitted_reports.is_empty());
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.name == "allow_live_submit" && !check.ok)
        );
        Ok(())
    }

    #[test]
    fn copy_execution_canary_live_requires_cleanup_gate() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18004")?;
        let mut config = crate::config::load_config(std::path::Path::new(&config_path))?;
        config.app.dry_run = false;
        config.manual_ops.manual_live_enabled = true;
        config.manual_ops.max_manual_order_notional_usd =
            crate::strategies::smart_money::COPY_DEFAULT_MAX_SIGNAL_NOTIONAL_USD;

        let report = tokio::runtime::Runtime::new()?.block_on(run_copy_execution_canary(
            &config,
            CopyExecutionCanaryOptions {
                leaders: vec!["leader_a=0x00000000000000000000000000000000000000aa".to_string()],
                account_ids: vec!["addr_a".to_string()],
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                local_account_id: None,
                shadow_history_path: std::env::temp_dir()
                    .join("unused-copy-execution-canary-cleanup.jsonl"),
                leader_notional_usd: 240.0,
                leader_size: 1.0,
                live: true,
                allow_live_submit: true,
                confirm_mainnet_live: false,
                cleanup_after_submit: false,
                cleanup_max_slippage_bps: 50.0,
                preflight_only: false,
                max_orders: 1,
            },
        ))?;

        assert!(!report.ok);
        assert!(report.submitted_reports.is_empty());
        assert!(report.cleanup_runbooks.is_empty());
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.name == "cleanup_after_submit" && !check.ok)
        );
        Ok(())
    }

    #[test]
    fn copy_execution_canary_live_preflight_only_plans_without_submit() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18006")?;
        let mut config = crate::config::load_config(std::path::Path::new(&config_path))?;
        config.app.dry_run = false;
        config.manual_ops.manual_live_enabled = true;
        config.manual_ops.max_manual_order_notional_usd =
            crate::strategies::smart_money::COPY_DEFAULT_MAX_SIGNAL_NOTIONAL_USD;

        let report = tokio::runtime::Runtime::new()?.block_on(run_copy_execution_canary(
            &config,
            CopyExecutionCanaryOptions {
                leaders: vec!["leader_a=0x00000000000000000000000000000000000000aa".to_string()],
                account_ids: vec!["addr_a".to_string()],
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                local_account_id: None,
                shadow_history_path: std::env::temp_dir()
                    .join("unused-copy-execution-canary-preflight-only.jsonl"),
                leader_notional_usd: 120.0,
                leader_size: 1.0,
                live: true,
                allow_live_submit: true,
                confirm_mainnet_live: false,
                cleanup_after_submit: true,
                cleanup_max_slippage_bps: 50.0,
                preflight_only: true,
                max_orders: 1,
            },
        ))?;

        assert!(report.ok, "{report:#?}");
        assert!(report.preflight_only);
        assert_eq!(report.would_submit_orders.len(), 1);
        assert_eq!(report.would_submit_orders[0].account_id, "addr_a");
        assert_eq!(
            report.would_submit_orders[0].notional_usd,
            crate::strategies::smart_money::COPY_DEFAULT_MAX_SIGNAL_NOTIONAL_USD
        );
        assert!(report.submitted_reports.is_empty());
        assert!(report.cleanup_runbooks.is_empty());
        assert!(report.cleanup_errors.is_empty());
        Ok(())
    }

    #[test]
    fn copy_execution_canary_preflight_only_gate_failure_does_not_claim_passed() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18007")?;
        let config = crate::config::load_config(std::path::Path::new(&config_path))?;

        let report = tokio::runtime::Runtime::new()?.block_on(run_copy_execution_canary(
            &config,
            CopyExecutionCanaryOptions {
                leaders: vec!["leader_a=0x00000000000000000000000000000000000000aa".to_string()],
                account_ids: vec!["addr_a".to_string()],
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                local_account_id: None,
                shadow_history_path: std::env::temp_dir()
                    .join("unused-copy-execution-canary-preflight-failed.jsonl"),
                leader_notional_usd: 120.0,
                leader_size: 1.0,
                live: true,
                allow_live_submit: true,
                confirm_mainnet_live: false,
                cleanup_after_submit: true,
                cleanup_max_slippage_bps: 50.0,
                preflight_only: true,
                max_orders: 1,
            },
        ))?;

        assert!(!report.ok);
        assert!(report.would_submit_orders.is_empty());
        assert!(report.next_actions.iter().any(|action| {
            action.contains("No live order was submitted") || action.contains("fix failed checks")
        }));
        assert!(
            !report
                .next_actions
                .iter()
                .any(|action| action.contains("Preflight-only live canary passed"))
        );
        Ok(())
    }

    #[test]
    fn copy_execution_canary_blocks_live_when_cleanup_notional_limit_is_too_low() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18014")?;
        let mut config = crate::config::load_config(std::path::Path::new(&config_path))?;
        config.app.dry_run = false;
        config.manual_ops.manual_live_enabled = true;
        config.manual_ops.max_manual_order_notional_usd = 12.0;

        let report = tokio::runtime::Runtime::new()?.block_on(run_copy_execution_canary(
            &config,
            CopyExecutionCanaryOptions {
                leaders: vec!["leader_a=0x00000000000000000000000000000000000000aa".to_string()],
                account_ids: vec!["addr_a".to_string()],
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                local_account_id: None,
                shadow_history_path: std::env::temp_dir()
                    .join("unused-copy-execution-cleanup-cap.jsonl"),
                leader_notional_usd: 120.0,
                leader_size: 1.0,
                live: true,
                allow_live_submit: true,
                confirm_mainnet_live: false,
                cleanup_after_submit: true,
                cleanup_max_slippage_bps: 50.0,
                preflight_only: false,
                max_orders: 1,
            },
        ))?;

        assert!(!report.ok);
        assert_eq!(report.would_submit_orders.len(), 1);
        assert!(report.submitted_reports.is_empty());
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.name == "cleanup_notional_limit" && !check.ok)
        );
        Ok(())
    }

    #[test]
    fn copy_execution_canary_live_report_fails_without_cleanup_evidence() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18005")?;
        let mut config = crate::config::load_config(std::path::Path::new(&config_path))?;
        config.app.dry_run = false;
        config.manual_ops.manual_live_enabled = true;
        let account = config
            .account("addr_a")
            .context("addr_a should exist in test config")?;
        let leader = CopyShadowSmokeLeader {
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
        };
        let options = CopyExecutionCanaryOptions {
            leaders: vec!["leader_a=0x00000000000000000000000000000000000000aa".to_string()],
            account_ids: vec!["addr_a".to_string()],
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            local_account_id: None,
            shadow_history_path: std::env::temp_dir().join("unused-copy-execution-report.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live: true,
            allow_live_submit: true,
            confirm_mainnet_live: false,
            cleanup_after_submit: true,
            cleanup_max_slippage_bps: 50.0,
            preflight_only: false,
            max_orders: 1,
        };
        let records = build_synthetic_copy_shadow_records(
            &config,
            &options,
            account,
            &leader,
            &["addr_a".to_string()],
        );
        let signal = records
            .iter()
            .find_map(|record| record.signal.as_ref())
            .context("synthetic canary should emit one copy signal")?;
        let intent_id = signal
            .to_trade_intent("addr_a", "worker-addr_a", account.copy_ratio)
            .intent_id;
        let submitted_reports = vec![crate::domain::WorkerReport::Submitted(
            crate::domain::OrderSubmitted {
                signal_id: signal.signal_id.clone(),
                intent_id,
                worker_id: "worker-addr_a".to_string(),
                account_id: "addr_a".to_string(),
                cloid: "00000000-0000-0000-0000-000000000001".to_string(),
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 12.0,
                submitted_price: Some(30_000.0),
                submitted_size: Some(0.0004),
                exchange_status: Some("filled".to_string()),
                oid: Some(1),
                filled_size: Some(0.0004),
                avg_fill_price: Some(30_000.0),
                dry_run: false,
                submitted_at_ms: now_ms(),
            },
        )];
        let report = copy_execution_canary_report(
            &config,
            &options,
            false,
            vec!["addr_a".to_string()],
            Some(leader),
            vec![
                CopyShadowSmokeCheck {
                    name: "all_live_gates".to_string(),
                    ok: true,
                    detail: "test live gates satisfied".to_string(),
                },
                CopyShadowSmokeCheck {
                    name: "cleanup_runbook_completed".to_string(),
                    ok: false,
                    detail: "cleanup missing".to_string(),
                },
            ],
            records,
            vec![super::CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 12.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000001".to_string(),
            }],
            submitted_reports,
            Vec::new(),
            Vec::new(),
            vec!["cleanup missing".to_string()],
        );

        assert!(!report.ok);
        assert_eq!(report.cleanup_errors, vec!["cleanup missing".to_string()]);
        assert!(report.next_actions.iter().any(|action| {
            action.contains("reconcile the account immediately")
                && action.contains("reduce-only close")
        }));
        assert_eq!(report.ledger_reconciliations.len(), 1);
        assert!(report.ledger_reconciliations[0].applied);
        assert_eq!(
            report.ledger_reconciliations[0].status,
            Some(crate::strategies::smart_money::CopyLedgerStatus::Open)
        );
        assert_eq!(
            report.ledger_reconciliation_snapshot.ledger_entries[0].status,
            crate::strategies::smart_money::CopyLedgerStatus::Open
        );
        assert_eq!(
            report.ledger_reconciliation_snapshot.ledger_entries[0]
                .order_cloid
                .as_deref(),
            Some("00000000-0000-0000-0000-000000000001")
        );
        assert_eq!(
            report.ledger_reconciliation_snapshot.ledger_entries[0].order_oid,
            Some(1)
        );
        Ok(())
    }

    #[test]
    fn copy_execution_canary_live_report_accepts_owned_order_evidence() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18006")?;
        let mut config = crate::config::load_config(std::path::Path::new(&config_path))?;
        config.app.dry_run = false;
        config.manual_ops.manual_live_enabled = true;
        let account = config
            .account("addr_a")
            .context("addr_a should exist in test config")?;
        let leader = CopyShadowSmokeLeader {
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
        };
        let options = CopyExecutionCanaryOptions {
            leaders: vec!["leader_a=0x00000000000000000000000000000000000000aa".to_string()],
            account_ids: vec!["addr_a".to_string()],
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            local_account_id: None,
            shadow_history_path: std::env::temp_dir().join("unused-copy-execution-evidence.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live: true,
            allow_live_submit: true,
            confirm_mainnet_live: false,
            cleanup_after_submit: true,
            cleanup_max_slippage_bps: 50.0,
            preflight_only: false,
            max_orders: 1,
        };
        let records = build_synthetic_copy_shadow_records(
            &config,
            &options,
            account,
            &leader,
            &["addr_a".to_string()],
        );
        let signal = records
            .iter()
            .find_map(|record| record.signal.as_ref())
            .context("synthetic canary should emit one copy signal")?;
        let intent_id = signal
            .to_trade_intent("addr_a", "worker-addr_a", account.copy_ratio)
            .intent_id;
        let cloid = "00000000-0000-0000-0000-000000000001".to_string();
        let submitted_at_ms = now_ms();
        let oid = 42;
        let submitted_reports = vec![crate::domain::WorkerReport::Submitted(
            crate::domain::OrderSubmitted {
                signal_id: signal.signal_id.clone(),
                intent_id,
                worker_id: "worker-addr_a".to_string(),
                account_id: "addr_a".to_string(),
                cloid: cloid.clone(),
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 12.0,
                submitted_price: Some(30_000.0),
                submitted_size: Some(0.0004),
                exchange_status: Some("open".to_string()),
                oid: Some(oid),
                filled_size: None,
                avg_fill_price: None,
                dry_run: false,
                submitted_at_ms,
            },
        )];
        let order_evidence = vec![super::CopyExecutionCanaryOrderEvidence {
            account_id: "addr_a".to_string(),
            worker_id: "worker-addr_a".to_string(),
            signal_id: signal.signal_id.clone(),
            coin: "xyz:XYZ100".to_string(),
            oid: Some(oid),
            cloid: cloid.clone(),
            order_status: Some(crate::hyperliquid::OrderStatusResponse {
                status: "order".to_string(),
                order: Some(crate::hyperliquid::OrderStatusInfo {
                    order: crate::hyperliquid::OrderStatusOrder {
                        coin: "xyz:XYZ100".to_string(),
                        side: "B".to_string(),
                        limit_px: "30000.0".to_string(),
                        sz: "0.0004".to_string(),
                        oid,
                        timestamp: submitted_at_ms,
                        trigger_condition: String::new(),
                        is_trigger: false,
                        trigger_px: String::new(),
                        children: Vec::new(),
                        is_position_tpsl: false,
                        reduce_only: false,
                        order_type: "Limit".to_string(),
                        orig_sz: "0.0004".to_string(),
                        tif: "Ioc".to_string(),
                        cloid: Some(cloid.clone()),
                    },
                    status: "filled".to_string(),
                    status_timestamp: submitted_at_ms + 10,
                }),
            }),
            user_fill_count: 1,
            matching_fill_count: 1,
            matching_fills: vec![crate::hyperliquid::UserFill {
                coin: "xyz:XYZ100".to_string(),
                px: "30000.0".to_string(),
                sz: "0.0004".to_string(),
                side: "B".to_string(),
                time: submitted_at_ms + 20,
                dir: "Open Long".to_string(),
                closed_pnl: "0.0".to_string(),
                hash: "0xabc".to_string(),
                oid,
                crossed: true,
                fee: "0.001".to_string(),
            }],
            error: None,
        }];

        let report = copy_execution_canary_report(
            &config,
            &options,
            false,
            vec!["addr_a".to_string()],
            Some(leader),
            vec![
                CopyShadowSmokeCheck {
                    name: "all_live_gates".to_string(),
                    ok: true,
                    detail: "test live gates satisfied".to_string(),
                },
                CopyShadowSmokeCheck {
                    name: "cleanup_runbook_completed".to_string(),
                    ok: true,
                    detail: "cleanup completed".to_string(),
                },
            ],
            records,
            vec![super::CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 12.0,
                reduce_only: false,
                cloid: cloid.clone(),
            }],
            submitted_reports,
            order_evidence,
            Vec::new(),
            Vec::new(),
        );

        assert!(
            !report.ok,
            "cleanup evidence is intentionally absent in this focused unit test"
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.name == "order_status_evidence" && check.ok)
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.name == "ledger_reconciliation" && check.ok)
        );
        assert_eq!(report.order_evidence.len(), 1);
        assert_eq!(report.ledger_reconciliations.len(), 2);
        assert!(
            report
                .ledger_reconciliations
                .iter()
                .all(|result| result.applied)
        );
        let entry = &report.ledger_reconciliation_snapshot.ledger_entries[0];
        assert_eq!(
            entry.status,
            crate::strategies::smart_money::CopyLedgerStatus::Open
        );
        assert_eq!(entry.order_cloid.as_deref(), Some(cloid.as_str()));
        assert_eq!(entry.order_oid, Some(oid));
        assert_eq!(entry.filled_at_ms, Some(submitted_at_ms + 20));
        Ok(())
    }

    #[test]
    fn copy_canary_merge_user_fills_deduplicates_time_window_results() {
        let mut fills = vec![crate::hyperliquid::UserFill {
            coin: "xyz:XYZ100".to_string(),
            px: "30000.0".to_string(),
            sz: "0.0004".to_string(),
            side: "B".to_string(),
            time: 1000,
            dir: "Open Long".to_string(),
            closed_pnl: "0.0".to_string(),
            hash: "0xabc".to_string(),
            oid: 42,
            crossed: true,
            fee: "0.001".to_string(),
        }];
        super::copy_canary_merge_user_fills(
            &mut fills,
            vec![
                crate::hyperliquid::UserFill {
                    coin: "xyz:XYZ100".to_string(),
                    px: "30000.0".to_string(),
                    sz: "0.0004".to_string(),
                    side: "B".to_string(),
                    time: 1000,
                    dir: "Open Long".to_string(),
                    closed_pnl: "0.0".to_string(),
                    hash: "0xabc".to_string(),
                    oid: 42,
                    crossed: true,
                    fee: "0.001".to_string(),
                },
                crate::hyperliquid::UserFill {
                    coin: "xyz:XYZ100".to_string(),
                    px: "30001.0".to_string(),
                    sz: "0.0002".to_string(),
                    side: "B".to_string(),
                    time: 1001,
                    dir: "Open Long".to_string(),
                    closed_pnl: "0.0".to_string(),
                    hash: "0xdef".to_string(),
                    oid: 42,
                    crossed: true,
                    fee: "0.001".to_string(),
                },
            ],
        );

        let matching = super::copy_canary_matching_fills(&fills, Some(42), "xyz:XYZ100");
        assert_eq!(fills.len(), 2);
        assert_eq!(matching.len(), 2);
    }

    #[test]
    fn copy_live_daemon_acceptance_dry_run_passes_with_restart_dedupe_probe() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18008")?;
        let config = crate::config::load_config(std::path::Path::new(&config_path))?;
        let dir = std::env::temp_dir().join(format!(
            "trade_xyz_copy_live_daemon_acceptance_{}",
            now_ms()
        ));
        fs::create_dir_all(&dir).context("failed to create daemon acceptance dir")?;

        let report = run_copy_live_daemon_acceptance(
            &config,
            CopyLiveDaemonAcceptanceOptions {
                leaders: vec!["leader_a=0x00000000000000000000000000000000000000aa".to_string()],
                account_ids: vec!["addr_a".to_string()],
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                persistence_path: dir.join("copy-persistence.json"),
                shadow_history_path: dir.join("copy-shadow.jsonl"),
                leader_notional_usd: 120.0,
                leader_size: 1.0,
                live: false,
                allow_live_submit: false,
                confirm_mainnet_live: false,
                max_duration_secs: 300,
                max_live_orders: 1,
                max_total_notional_usd: 100.0,
                max_total_fees_usd: 0.10,
                max_slippage_bps: 50.0,
                require_cleanup_after_submit: true,
                require_flat_reconcile_after_submit: true,
            },
        )?;

        assert!(report.ok, "{report:#?}");
        assert_eq!(report.mode, "copy_live_daemon_acceptance_dry_run");
        assert_eq!(report.would_submit_orders.len(), 1);
        assert_eq!(
            report.would_submit_orders[0].notional_usd,
            crate::strategies::smart_money::COPY_DEFAULT_MAX_SIGNAL_NOTIONAL_USD
        );
        assert_eq!(report.restart_dedupe_probe.first_emit_count, 1);
        assert_eq!(report.restart_dedupe_probe.replay_emit_count, 0);
        assert_eq!(
            report.restart_dedupe_probe.fresh_after_restart_emit_count,
            1
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.name == "cloid_plan" && check.ok)
        );
        Ok(())
    }

    #[test]
    fn copy_live_daemon_acceptance_live_gate_fails_closed_without_operator_gates() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18009")?;
        let mut config = crate::config::load_config(std::path::Path::new(&config_path))?;
        config.app.dry_run = false;
        config.app.environment = "mainnet".to_string();
        let dir = std::env::temp_dir().join(format!(
            "trade_xyz_copy_live_daemon_acceptance_live_{}",
            now_ms()
        ));
        fs::create_dir_all(&dir).context("failed to create daemon acceptance dir")?;

        let report = run_copy_live_daemon_acceptance(
            &config,
            CopyLiveDaemonAcceptanceOptions {
                leaders: vec!["leader_a=0x00000000000000000000000000000000000000aa".to_string()],
                account_ids: vec!["addr_a".to_string()],
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                persistence_path: dir.join("copy-persistence.json"),
                shadow_history_path: dir.join("copy-shadow.jsonl"),
                leader_notional_usd: 120.0,
                leader_size: 1.0,
                live: true,
                allow_live_submit: false,
                confirm_mainnet_live: false,
                max_duration_secs: 300,
                max_live_orders: 1,
                max_total_notional_usd: 100.0,
                max_total_fees_usd: 0.10,
                max_slippage_bps: 50.0,
                require_cleanup_after_submit: true,
                require_flat_reconcile_after_submit: true,
            },
        )?;

        assert!(!report.ok);
        for expected in [
            "allow_live_submit",
            "mainnet_confirmation",
            "manual_live_enabled",
            "mainnet_live_enabled",
        ] {
            assert!(
                report
                    .checks
                    .iter()
                    .any(|check| check.name == expected && !check.ok),
                "missing failed check {expected}: {:#?}",
                report.checks
            );
        }
        assert!(report.would_submit_orders.len() == 1);
        assert!(report.next_actions[0].contains("Do not start unattended live copy"));
        Ok(())
    }

    #[test]
    fn copy_live_daemon_acceptance_rejects_unbounded_operator_limits() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18010")?;
        let config = crate::config::load_config(std::path::Path::new(&config_path))?;
        let dir = std::env::temp_dir().join(format!(
            "trade_xyz_copy_live_daemon_acceptance_limits_{}",
            now_ms()
        ));
        fs::create_dir_all(&dir).context("failed to create daemon acceptance dir")?;

        let report = run_copy_live_daemon_acceptance(
            &config,
            CopyLiveDaemonAcceptanceOptions {
                leaders: vec!["leader_a=0x00000000000000000000000000000000000000aa".to_string()],
                account_ids: vec!["addr_a".to_string(), "addr_b".to_string()],
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                persistence_path: dir.join("copy-persistence.json"),
                shadow_history_path: dir.join("copy-shadow.jsonl"),
                leader_notional_usd: 240.0,
                leader_size: 1.0,
                live: false,
                allow_live_submit: false,
                confirm_mainnet_live: false,
                max_duration_secs: 7_200,
                max_live_orders: 1,
                max_total_notional_usd: 10.0,
                max_total_fees_usd: 2.0,
                max_slippage_bps: 250.0,
                require_cleanup_after_submit: false,
                require_flat_reconcile_after_submit: false,
            },
        )?;

        assert!(!report.ok);
        for expected in [
            "bounded_duration",
            "bounded_total_notional",
            "bounded_total_fees",
            "bounded_slippage",
            "cleanup_policy",
            "flat_reconcile_policy",
            "max_live_order_count",
        ] {
            assert!(
                report
                    .checks
                    .iter()
                    .any(|check| check.name == expected && !check.ok),
                "missing failed check {expected}: {:#?}",
                report.checks
            );
        }
        Ok(())
    }

    #[test]
    fn copy_live_daemon_supervisor_ok_requires_acceptance_checks_and_reconcile_health() {
        let checks = vec![CopyShadowSmokeCheck {
            name: "all_checks".to_string(),
            ok: true,
            detail: "test".to_string(),
        }];
        let failed_checks = vec![CopyShadowSmokeCheck {
            name: "cap".to_string(),
            ok: false,
            detail: "over cap".to_string(),
        }];
        assert!(copy_live_daemon_supervisor_ok(
            false, true, &checks, true, false
        ));
        assert!(copy_live_daemon_supervisor_ok(
            true, true, &checks, true, true
        ));
        assert!(!copy_live_daemon_supervisor_ok(
            true, true, &checks, true, false
        ));
        assert!(!copy_live_daemon_supervisor_ok(
            false, false, &checks, true, false
        ));
        assert!(!copy_live_daemon_supervisor_ok(
            false,
            true,
            &failed_checks,
            true,
            false
        ));
        assert!(!copy_live_daemon_supervisor_ok(
            false, true, &checks, false, false
        ));
        assert!(!copy_live_daemon_supervisor_ok(
            false, true, &checks, false, false
        ));
    }

    #[test]
    fn copy_live_daemon_watcher_progress_fails_zero_event_disconnects() {
        for status in [
            "watcher_recoverable_disconnect",
            "watcher_channel_closed",
            "watcher_error",
            "watcher_join_error",
        ] {
            let check = copy_live_daemon_watcher_progress_check(status, 0, 600, 87);
            assert_eq!(check.name, "watcher_progress");
            assert!(!check.ok, "{status} should be degraded: {check:#?}");
            assert!(check.detail.contains("restart/backoff required"));
        }
    }

    #[test]
    fn copy_live_daemon_supervisor_ok_rejects_zero_event_disconnect_check() {
        let checks = vec![
            copy_shadow_smoke_check("baseline", true, "ok"),
            copy_live_daemon_watcher_progress_check("watcher_recoverable_disconnect", 0, 600, 65),
        ];

        assert!(!copy_live_daemon_supervisor_ok(
            false, true, &checks, true, false
        ));
    }

    #[test]
    fn copy_live_daemon_reconcile_only_degraded_allows_read_failure_classification() {
        let checks = vec![
            copy_shadow_smoke_check("exchange_submit_mode", false, "no submit reports"),
            copy_shadow_smoke_check(
                "final_reconcile_health",
                false,
                "failed to fetch open orders",
            ),
        ];
        let final_reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: false,
            open_order_count: None,
            asset_positions: None,
            position_summaries: Vec::new(),
            account_value: None,
            withdrawable: None,
            total_ntl_pos: None,
            total_margin_used: None,
            error: Some("failed to fetch open orders".to_string()),
        }];
        let report = CopyLiveDaemonPersistentLiveSubmitReport {
            ok: false,
            mode: "persistent_live_submit".to_string(),
            submit_requested: true,
            submit_plan_contract_ok: false,
            submitted_reports: Vec::new(),
            order_evidence: Vec::new(),
            cleanup_runbooks: Vec::new(),
            cleanup_errors: Vec::new(),
            ledger_reconciliations: Vec::new(),
            ledger_reconciliation_snapshot:
                crate::strategies::smart_money::CopyPersistenceSnapshot::new(
                    1781745839800,
                    Vec::new(),
                    &crate::strategies::smart_money::CopyLedger::new(),
                ),
            checks: vec![copy_shadow_smoke_check(
                "submit_plan_contract_ok",
                false,
                "submit_plan_contract.ok=false",
            )],
        };

        assert!(copy_live_daemon_reconcile_only_degraded_round(
            true,
            &checks,
            &final_reconciliations,
            &report,
        ));

        let mut submitted_report = report.clone();
        submitted_report
            .submitted_reports
            .push(crate::domain::WorkerReport::Error(
                crate::domain::WorkerError {
                    worker_id: "worker-addr_a".to_string(),
                    account_id: "addr_a".to_string(),
                    message: "real submit failure".to_string(),
                    error_at_ms: 1781745839801,
                },
            ));
        assert!(!copy_live_daemon_reconcile_only_degraded_round(
            true,
            &checks,
            &final_reconciliations,
            &submitted_report,
        ));
    }

    #[test]
    fn copy_live_daemon_watcher_progress_allows_events_before_disconnect() {
        let check = copy_live_daemon_watcher_progress_check(
            "watcher_recoverable_disconnect",
            390,
            600,
            600_000,
        );
        assert!(check.ok, "{check:#?}");
        assert!(check.detail.contains("events_received=390"));
    }

    #[test]
    fn copy_live_daemon_watcher_progress_allows_empty_completed_window() {
        let check = copy_live_daemon_watcher_progress_check("completed_duration", 0, 600, 600_000);
        assert!(check.ok, "{check:#?}");
        assert!(check.detail.contains("completed_duration"));
    }

    #[test]
    fn copy_live_daemon_watcher_progress_allows_immediate_submit_stop() {
        let check = copy_live_daemon_watcher_progress_check(
            "stopped_after_immediate_live_submit",
            42,
            600,
            45_000,
        );
        assert!(check.ok, "{check:#?}");
        assert!(check.detail.contains("stopped_after_immediate_live_submit"));
    }

    #[test]
    fn copy_live_daemon_watcher_progress_allows_submit_candidate_priority_stop() {
        let check = copy_live_daemon_watcher_progress_check(
            "stopped_after_submit_candidate",
            42,
            600,
            45_000,
        );
        assert!(check.ok, "{check:#?}");
        assert!(check.detail.contains("pre-submit checks and live submit"));
    }

    #[test]
    fn copy_live_daemon_immediate_submit_stop_requires_live_submission() {
        let submitted_at_ms = now_ms();
        let live_report = CopyLiveDaemonPersistentLiveSubmitReport {
            ok: true,
            mode: "persistent_live_submit".to_string(),
            submit_requested: true,
            submit_plan_contract_ok: true,
            submitted_reports: vec![crate::domain::WorkerReport::Submitted(
                crate::domain::OrderSubmitted {
                    signal_id: "sig-live".to_string(),
                    intent_id: "intent-live".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    account_id: "addr_a".to_string(),
                    cloid: "11111111-1111-5111-8111-111111111111".to_string(),
                    coin: "xyz:QNT".to_string(),
                    side: crate::domain::OrderSide::Sell,
                    notional_usd: 50.0,
                    submitted_price: Some(74.0),
                    submitted_size: Some(0.67),
                    exchange_status: Some("filled".to_string()),
                    oid: Some(90001),
                    filled_size: Some(0.67),
                    avg_fill_price: Some(74.0),
                    dry_run: false,
                    submitted_at_ms,
                },
            )],
            order_evidence: Vec::new(),
            cleanup_runbooks: Vec::new(),
            cleanup_errors: Vec::new(),
            ledger_reconciliations: Vec::new(),
            ledger_reconciliation_snapshot:
                crate::strategies::smart_money::CopyPersistenceSnapshot {
                    schema_version: 1,
                    saved_at_ms: submitted_at_ms,
                    seen_event_keys: Vec::new(),
                    ledger_entries: Vec::new(),
                },
            checks: Vec::new(),
        };
        assert!(copy_live_daemon_immediate_submit_should_stop_round(
            &live_report
        ));

        let mut dry_run_report = live_report.clone();
        if let crate::domain::WorkerReport::Submitted(submitted) =
            &mut dry_run_report.submitted_reports[0]
        {
            submitted.dry_run = true;
        }
        assert!(!copy_live_daemon_immediate_submit_should_stop_round(
            &dry_run_report
        ));

        let mut empty_report = live_report;
        empty_report.submitted_reports.clear();
        assert!(!copy_live_daemon_immediate_submit_should_stop_round(
            &empty_report
        ));
    }

    #[test]
    fn copy_live_daemon_live_submit_health_accepts_filled_evidence_after_watcher_close() {
        let report = CopyLiveDaemonPersistentLiveSubmitReport {
            ok: false,
            mode: "persistent_live_submit".to_string(),
            submit_requested: true,
            submit_plan_contract_ok: true,
            submitted_reports: vec![crate::domain::WorkerReport::Submitted(
                crate::domain::OrderSubmitted {
                    signal_id: "signal".to_string(),
                    intent_id: "intent".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    account_id: "addr_a".to_string(),
                    cloid: "3e45b7c8-8322-5e0c-81a2-7cafc276de89".to_string(),
                    coin: "xyz:GBP".to_string(),
                    side: crate::domain::OrderSide::Sell,
                    notional_usd: 41.7787,
                    submitted_price: Some(1.3397),
                    submitted_size: Some(31.0),
                    exchange_status: Some("filled".to_string()),
                    oid: Some(469047367049),
                    filled_size: Some(31.0),
                    avg_fill_price: Some(1.345209),
                    dry_run: false,
                    submitted_at_ms: 1781468319872,
                },
            )],
            order_evidence: vec![CopyExecutionCanaryOrderEvidence {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                signal_id: "signal".to_string(),
                coin: "xyz:GBP".to_string(),
                oid: Some(469047367049),
                cloid: "3e45b7c8-8322-5e0c-81a2-7cafc276de89".to_string(),
                order_status: Some(crate::hyperliquid::OrderStatusResponse {
                    status: "order".to_string(),
                    order: None,
                }),
                user_fill_count: 1,
                matching_fill_count: 1,
                matching_fills: Vec::new(),
                error: None,
            }],
            cleanup_runbooks: Vec::new(),
            cleanup_errors: Vec::new(),
            ledger_reconciliations: Vec::new(),
            ledger_reconciliation_snapshot:
                crate::strategies::smart_money::CopyPersistenceSnapshot::new(
                    1781468320692,
                    Vec::new(),
                    &crate::strategies::smart_money::CopyLedger::new(),
                ),
            checks: vec![CopyShadowSmokeCheck {
                name: "submit_path".to_string(),
                ok: true,
                detail: "submitted and evidenced before watcher close".to_string(),
            }],
        };

        assert!(copy_live_daemon_live_submit_health_ok(&report));
    }

    #[test]
    fn copy_canary_order_evidence_accepts_filled_status_without_matching_fill() {
        let now = now_ms();
        let cloid = "3e45b7c8-8322-5e0c-81a2-7cafc276de89".to_string();
        let mut evidence = CopyExecutionCanaryOrderEvidence {
            account_id: "addr_a".to_string(),
            worker_id: "worker-addr_a".to_string(),
            signal_id: "signal".to_string(),
            coin: "@166".to_string(),
            oid: Some(479539771358),
            cloid: cloid.clone(),
            order_status: Some(crate::hyperliquid::OrderStatusResponse {
                status: "order".to_string(),
                order: Some(crate::hyperliquid::OrderStatusInfo {
                    order: crate::hyperliquid::OrderStatusOrder {
                        coin: "@166".to_string(),
                        side: "B".to_string(),
                        limit_px: "1.0037".to_string(),
                        sz: "59.06".to_string(),
                        oid: 479539771358,
                        timestamp: now,
                        trigger_condition: "N/A".to_string(),
                        is_trigger: false,
                        trigger_px: "0.0".to_string(),
                        children: Vec::new(),
                        is_position_tpsl: false,
                        reduce_only: false,
                        order_type: "Limit".to_string(),
                        orig_sz: "60.37".to_string(),
                        tif: "Ioc".to_string(),
                        cloid: Some("0x".to_string() + &cloid.replace('-', "")),
                    },
                    status: "filled".to_string(),
                    status_timestamp: now,
                }),
            }),
            user_fill_count: 235,
            matching_fill_count: 0,
            matching_fills: Vec::new(),
            error: None,
        };

        assert!(super::copy_execution_canary_order_evidence_ok(&evidence));

        evidence
            .order_status
            .as_mut()
            .unwrap()
            .order
            .as_mut()
            .unwrap()
            .status = "open".to_string();
        assert!(!super::copy_execution_canary_order_evidence_ok(&evidence));
    }

    #[test]
    fn copy_live_daemon_live_submit_health_rejects_missing_evidence() {
        let report = CopyLiveDaemonPersistentLiveSubmitReport {
            ok: false,
            mode: "persistent_live_submit".to_string(),
            submit_requested: true,
            submit_plan_contract_ok: true,
            submitted_reports: vec![crate::domain::WorkerReport::Submitted(
                crate::domain::OrderSubmitted {
                    signal_id: "signal".to_string(),
                    intent_id: "intent".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    account_id: "addr_a".to_string(),
                    cloid: "missing-evidence".to_string(),
                    coin: "xyz:GBP".to_string(),
                    side: crate::domain::OrderSide::Sell,
                    notional_usd: 41.7787,
                    submitted_price: Some(1.3397),
                    submitted_size: Some(31.0),
                    exchange_status: Some("filled".to_string()),
                    oid: Some(469047367049),
                    filled_size: Some(31.0),
                    avg_fill_price: Some(1.345209),
                    dry_run: false,
                    submitted_at_ms: 1781468319872,
                },
            )],
            order_evidence: Vec::new(),
            cleanup_runbooks: Vec::new(),
            cleanup_errors: Vec::new(),
            ledger_reconciliations: Vec::new(),
            ledger_reconciliation_snapshot:
                crate::strategies::smart_money::CopyPersistenceSnapshot::new(
                    1781468320692,
                    Vec::new(),
                    &crate::strategies::smart_money::CopyLedger::new(),
                ),
            checks: vec![CopyShadowSmokeCheck {
                name: "submit_path".to_string(),
                ok: true,
                detail: "submitted without evidence".to_string(),
            }],
        };

        assert!(!copy_live_daemon_live_submit_health_ok(&report));
    }

    #[test]
    fn copy_live_daemon_live_submit_health_accepts_safe_pre_submit_skip() {
        let report = CopyLiveDaemonPersistentLiveSubmitReport {
            ok: false,
            mode: "persistent_live_submit".to_string(),
            submit_requested: true,
            submit_plan_contract_ok: true,
            submitted_reports: vec![crate::domain::WorkerReport::Error(
                crate::domain::WorkerError {
                    worker_id: "worker-addr_b".to_string(),
                    account_id: "addr_b".to_string(),
                    message:
                        "exchange returned action-level order error: Order must have minimum value of $10. asset=110052"
                            .to_string(),
                    error_at_ms: 1782240035380,
                },
            )],
            order_evidence: Vec::new(),
            cleanup_runbooks: Vec::new(),
            cleanup_errors: Vec::new(),
            ledger_reconciliations: Vec::new(),
            ledger_reconciliation_snapshot:
                crate::strategies::smart_money::CopyPersistenceSnapshot::new(
                    1782240035380,
                    Vec::new(),
                    &crate::strategies::smart_money::CopyLedger::new(),
                ),
            checks: vec![
                CopyShadowSmokeCheck {
                    name: "submitted_reports".to_string(),
                    ok: false,
                    detail: "0 live submitted report(s), 1 pre-submit skipped ref(s)"
                        .to_string(),
                },
                CopyShadowSmokeCheck {
                    name: "persistent_live_submit_chunks".to_string(),
                    ok: false,
                    detail: "1 persistent live submit chunk(s) merged".to_string(),
                },
            ],
        };

        assert!(copy_live_daemon_live_submit_health_ok(&report));
    }

    #[test]
    fn copy_live_daemon_live_submit_health_accepts_evidenced_submit_plus_safe_skip() {
        let report = CopyLiveDaemonPersistentLiveSubmitReport {
            ok: false,
            mode: "persistent_live_submit".to_string(),
            submit_requested: true,
            submit_plan_contract_ok: true,
            submitted_reports: vec![
                crate::domain::WorkerReport::Submitted(crate::domain::OrderSubmitted {
                    signal_id: "signal".to_string(),
                    intent_id: "intent".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    account_id: "addr_a".to_string(),
                    cloid: "3e45b7c8-8322-5e0c-81a2-7cafc276de89".to_string(),
                    coin: "xyz:SP500".to_string(),
                    side: crate::domain::OrderSide::Buy,
                    notional_usd: 14.7592,
                    submitted_price: Some(7379.6),
                    submitted_size: Some(0.002),
                    exchange_status: Some("filled".to_string()),
                    oid: Some(477403388270),
                    filled_size: Some(0.002),
                    avg_fill_price: Some(7379.6),
                    dry_run: false,
                    submitted_at_ms: 1782239875081,
                }),
                crate::domain::WorkerReport::Error(crate::domain::WorkerError {
                    worker_id: "worker-addr_b".to_string(),
                    account_id: "addr_b".to_string(),
                    message:
                        "copy submit skipped before exchange: addr_b xyz:SP500 requested_notional=11.172000 effective_notional=7.416100 below exchange minimum 10.000000"
                            .to_string(),
                    error_at_ms: 1782239875081,
                }),
            ],
            order_evidence: vec![CopyExecutionCanaryOrderEvidence {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                signal_id: "signal".to_string(),
                coin: "xyz:SP500".to_string(),
                oid: Some(477403388270),
                cloid: "3e45b7c8-8322-5e0c-81a2-7cafc276de89".to_string(),
                order_status: Some(crate::hyperliquid::OrderStatusResponse {
                    status: "order".to_string(),
                    order: None,
                }),
                user_fill_count: 1,
                matching_fill_count: 1,
                matching_fills: Vec::new(),
                error: None,
            }],
            cleanup_runbooks: Vec::new(),
            cleanup_errors: Vec::new(),
            ledger_reconciliations: Vec::new(),
            ledger_reconciliation_snapshot:
                crate::strategies::smart_money::CopyPersistenceSnapshot::new(
                    1782239875081,
                    Vec::new(),
                    &crate::strategies::smart_money::CopyLedger::new(),
                ),
            checks: vec![CopyShadowSmokeCheck {
                name: "submitted_reports".to_string(),
                ok: false,
                detail: "1 live submitted report(s), 1 pre-submit skipped ref(s)".to_string(),
            }],
        };

        assert!(copy_live_daemon_live_submit_health_ok(&report));
    }

    #[test]
    fn copy_live_daemon_live_submit_health_accepts_reduce_only_already_flat_error() {
        let report = CopyLiveDaemonPersistentLiveSubmitReport {
            ok: false,
            mode: "persistent_live_submit".to_string(),
            submit_requested: true,
            submit_plan_contract_ok: true,
            submitted_reports: vec![
                crate::domain::WorkerReport::Submitted(crate::domain::OrderSubmitted {
                    signal_id: "signal-qnt-close".to_string(),
                    intent_id: "intent-qnt-close".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    account_id: "addr_a".to_string(),
                    cloid: "8b9ffb1e-0623-55fd-a1b1-84634a911c91".to_string(),
                    coin: "xyz:QNT".to_string(),
                    side: crate::domain::OrderSide::Buy,
                    notional_usd: 242.15425,
                    submitted_price: Some(74.509),
                    submitted_size: Some(3.25),
                    exchange_status: Some("filled".to_string()),
                    oid: Some(481205177012),
                    filled_size: Some(3.25),
                    avg_fill_price: Some(74.509),
                    dry_run: false,
                    submitted_at_ms: 1782572600145,
                }),
                crate::domain::WorkerReport::Error(crate::domain::WorkerError {
                    worker_id: "worker-addr_b".to_string(),
                    account_id: "addr_b".to_string(),
                    message:
                        "exchange returned action-level order error: Reduce only order would increase position. asset=110081"
                            .to_string(),
                    error_at_ms: 1782572605421,
                }),
            ],
            order_evidence: vec![CopyExecutionCanaryOrderEvidence {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                signal_id: "signal-qnt-close".to_string(),
                coin: "xyz:QNT".to_string(),
                oid: Some(481205177012),
                cloid: "8b9ffb1e-0623-55fd-a1b1-84634a911c91".to_string(),
                order_status: Some(crate::hyperliquid::OrderStatusResponse {
                    status: "order".to_string(),
                    order: None,
                }),
                user_fill_count: 1,
                matching_fill_count: 1,
                matching_fills: Vec::new(),
                error: None,
            }],
            cleanup_runbooks: Vec::new(),
            cleanup_errors: Vec::new(),
            ledger_reconciliations: Vec::new(),
            ledger_reconciliation_snapshot:
                crate::strategies::smart_money::CopyPersistenceSnapshot::new(
                    1782572605421,
                    Vec::new(),
                    &crate::strategies::smart_money::CopyLedger::new(),
                ),
            checks: vec![
                CopyShadowSmokeCheck {
                    name: "submitted_reports".to_string(),
                    ok: false,
                    detail: "1 live submitted report(s), 1 pre-submit skipped ref(s)"
                        .to_string(),
                },
                CopyShadowSmokeCheck {
                    name: "persistent_live_submit_chunks".to_string(),
                    ok: false,
                    detail: "1 persistent live submit chunk(s) merged".to_string(),
                },
            ],
        };

        assert!(copy_live_daemon_live_submit_health_ok(&report));
    }

    #[test]
    fn copy_live_daemon_follow_position_mode_allows_bounded_open_position_health() {
        let mut options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: std::env::temp_dir().join("unused-follow-position-health.json"),
            shadow_history_path: std::env::temp_dir().join("unused-follow-position-health.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: false,
            submit: true,
            hold_positions_after_submit: true,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 60,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 50.0,
            max_total_fees_usd: 0.10,
            max_slippage_bps: 50.0,
            environment: None,
            ws_url: None,
        };
        let held_position = CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: false,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: Vec::new(),
            account_value: Some("100.0".to_string()),
            withdrawable: Some("100.0".to_string()),
            total_ntl_pos: Some("49.5".to_string()),
            total_margin_used: Some("9.9".to_string()),
            error: None,
        };
        let over_cap_position = CopyBoundedLiveWindowReconcile {
            total_ntl_pos: Some("60.1".to_string()),
            ..held_position.clone()
        };
        let stale_order_position = CopyBoundedLiveWindowReconcile {
            open_order_count: Some(1),
            ..held_position.clone()
        };

        assert!(copy_live_daemon_reconcile_healthy_for_mode(
            &options,
            &held_position
        ));
        assert!(!copy_live_daemon_reconcile_healthy_for_mode(
            &options,
            &over_cap_position
        ));
        assert!(!copy_live_daemon_reconcile_healthy_for_mode(
            &options,
            &stale_order_position
        ));

        options.hold_positions_after_submit = false;
        assert!(!copy_live_daemon_reconcile_healthy_for_mode(
            &options,
            &held_position
        ));
    }

    #[test]
    fn copy_live_daemon_recoverable_watcher_error_matches_remote_reset() {
        let error = anyhow::anyhow!(
            "failed to read copy watcher websocket message: IO error: remote host forcibly closed an existing connection (os error 10054)"
        );
        assert!(copy_live_daemon_recoverable_watcher_error(&error));

        let mojibake_error = anyhow::anyhow!(
            "failed to read copy watcher websocket message: IO error: 杩滅▼涓绘満寮鸿揩鍏抽棴浜嗕竴涓幇鏈夌殑杩炴帴銆?(os error 10054)"
        );
        assert!(copy_live_daemon_recoverable_watcher_error(&mojibake_error));

        let protocol_error = anyhow::anyhow!("malformed subscription payload");
        assert!(!copy_live_daemon_recoverable_watcher_error(&protocol_error));
    }

    #[test]
    fn copy_live_daemon_submit_evidence_contract_blocks_until_persistent_submit_connected()
    -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18013")?;
        let mut config = crate::config::load_config(std::path::Path::new(&config_path))?;
        config.app.dry_run = false;
        config.manual_ops.manual_live_enabled = true;
        let dir = std::env::temp_dir().join(format!(
            "trade_xyz_copy_live_daemon_submit_contract_{}",
            now_ms()
        ));
        fs::create_dir_all(&dir).context("failed to create daemon contract test dir")?;
        let leaders = vec!["leader_a=0x00000000000000000000000000000000000000aa".to_string()];
        let acceptance = run_copy_live_daemon_acceptance(
            &config,
            CopyLiveDaemonAcceptanceOptions {
                leaders: leaders.clone(),
                account_ids: vec!["addr_a".to_string()],
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                persistence_path: dir.join("acceptance-persistence.json"),
                shadow_history_path: dir.join("acceptance-shadow.jsonl"),
                leader_notional_usd: 120.0,
                leader_size: 1.0,
                live: true,
                allow_live_submit: true,
                confirm_mainnet_live: false,
                max_duration_secs: 300,
                max_live_orders: 1,
                max_total_notional_usd: 100.0,
                max_total_fees_usd: 0.10,
                max_slippage_bps: 50.0,
                require_cleanup_after_submit: true,
                require_flat_reconcile_after_submit: true,
            },
        )?;
        let options = CopyLiveDaemonSupervisorOptions {
            leaders,
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: dir.join("supervisor-persistence.json"),
            shadow_history_path: dir.join("supervisor-shadow.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: false,
            submit: false,
            hold_positions_after_submit: false,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 300,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 50.0,
            max_total_fees_usd: 0.10,
            max_slippage_bps: 50.0,
            environment: None,
            ws_url: None,
        };
        let would_submit_orders = vec![CopyExecutionCanaryWouldSubmit {
            account_id: "addr_a".to_string(),
            worker_id: "worker-addr_a".to_string(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            notional_usd: 12.0,
            reduce_only: false,
            cloid: "00000000-0000-0000-0000-000000000001".to_string(),
        }];
        let flat = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: true,
            open_order_count: Some(0),
            asset_positions: Some(0),
            position_summaries: Vec::new(),
            account_value: Some("100.0".to_string()),
            withdrawable: Some("100.0".to_string()),
            total_ntl_pos: Some("0.0".to_string()),
            total_margin_used: Some("0.0".to_string()),
            error: None,
        }];

        assert!(acceptance.ok, "{acceptance:#?}");
        let contract = copy_live_daemon_submit_evidence_contract(
            &options,
            &acceptance,
            &would_submit_orders,
            12.0,
            0.024,
            &flat,
            None,
            &HashMap::new(),
        );

        assert!(!contract.ready_for_unattended_submit);
        assert!(
            contract
                .blocker
                .as_deref()
                .unwrap_or("")
                .contains("unattended live submit remains gated")
        );
        assert!(
            contract
                .required_live_evidence
                .iter()
                .any(|item| item.contains("orderStatus"))
        );
        assert!(
            contract
                .required_live_evidence
                .iter()
                .any(|item| item.contains("userFillsByTime"))
        );
        assert!(
            contract.checks.iter().any(|check| {
                check.name == "persistent_live_submit_path_connected" && !check.ok
            })
        );
        Ok(())
    }

    #[test]
    fn copy_live_daemon_supervisor_appends_would_submit_plans_once() {
        let mut plans = vec![CopyExecutionCanaryWouldSubmit {
            account_id: "addr_a".to_string(),
            worker_id: "worker-addr_a".to_string(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            notional_usd: 12.0,
            reduce_only: false,
            cloid: "00000000-0000-0000-0000-000000000001".to_string(),
        }];

        append_unique_copy_daemon_would_submit_orders(
            &mut plans,
            vec![
                CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:XYZ100".to_string(),
                    side: crate::domain::OrderSide::Buy,
                    notional_usd: 12.0,
                    reduce_only: false,
                    cloid: "00000000-0000-0000-0000-000000000001".to_string(),
                },
                CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:XYZ100".to_string(),
                    side: crate::domain::OrderSide::Sell,
                    notional_usd: 10.0,
                    reduce_only: true,
                    cloid: "00000000-0000-0000-0000-000000000002".to_string(),
                },
            ],
        );

        assert_eq!(plans.len(), 2);
        assert_eq!(plans[1].cloid, "00000000-0000-0000-0000-000000000002");
    }

    #[test]
    fn copy_live_daemon_partition_suppresses_candidates_over_live_order_cap() {
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: std::env::temp_dir().join("unused-daemon-partition.json"),
            shadow_history_path: std::env::temp_dir().join("unused-daemon-partition.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: false,
            submit: false,
            hold_positions_after_submit: false,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 900,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 50.0,
            max_total_fees_usd: 0.10,
            max_slippage_bps: 50.0,
            environment: None,
            ws_url: None,
        };
        let orders = vec![
            CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:NATGAS".to_string(),
                side: crate::domain::OrderSide::Sell,
                notional_usd: 12.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000001".to_string(),
            },
            CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:SPCX".to_string(),
                side: crate::domain::OrderSide::Sell,
                notional_usd: 12.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000002".to_string(),
            },
        ];

        let (executable, suppressed) =
            super::partition_copy_live_daemon_would_submit_orders(&orders, &options);

        assert_eq!(executable.len(), 1);
        assert_eq!(executable[0].coin, "xyz:NATGAS");
        assert_eq!(suppressed.len(), 1);
        assert_eq!(suppressed[0].order.coin, "xyz:SPCX");
        assert_eq!(suppressed[0].reason_code, "COPY_DAEMON_MAX_LIVE_ORDERS");
    }

    #[test]
    fn copy_live_daemon_plan_refs_preserve_shadow_record_identity() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18012")?;
        let config = crate::config::load_config(std::path::Path::new(&config_path))?;
        let account = config.account("addr_a").context("missing addr_a")?;
        let leader = CopyShadowSmokeLeader {
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
        };
        let records = build_synthetic_copy_shadow_records(
            &config,
            &CopyExecutionCanaryOptions {
                leaders: vec!["leader_a=0x00000000000000000000000000000000000000aa".to_string()],
                account_ids: vec!["addr_a".to_string()],
                local_account_id: Some("addr_a".to_string()),
                shadow_history_path: std::env::temp_dir().join("copy-plan-ref-shadow.jsonl"),
                leader_notional_usd: 120.0,
                leader_size: 1.0,
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                live: false,
                allow_live_submit: false,
                confirm_mainnet_live: false,
                cleanup_after_submit: false,
                cleanup_max_slippage_bps: 50.0,
                preflight_only: false,
                max_orders: 1,
            },
            account,
            &leader,
            &["addr_a".to_string()],
        );

        let refs = plan_copy_daemon_acceptance_order_refs(&config, &records)?;

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].record_index, 0);
        assert_eq!(
            refs[0].signal_id,
            records[0].signal.as_ref().unwrap().signal_id
        );
        assert_eq!(refs[0].leader_id, records[0].action.leader_id);
        assert_eq!(refs[0].leader_address, records[0].action.leader_address);
        assert_eq!(refs[0].order.account_id, "addr_a");
        assert_eq!(refs[0].order.coin, "xyz:XYZ100");
        Ok(())
    }

    #[test]
    fn copy_live_daemon_ref_partition_keeps_only_executable_refs_under_cap() {
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: std::env::temp_dir().join("unused-daemon-ref-partition.json"),
            shadow_history_path: std::env::temp_dir().join("unused-daemon-ref-partition.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: false,
            submit: false,
            hold_positions_after_submit: false,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 900,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 50.0,
            max_total_fees_usd: 0.10,
            max_slippage_bps: 50.0,
            environment: None,
            ws_url: None,
        };
        let refs = vec![
            CopyLiveDaemonWouldSubmitRef {
                record_index: 3,
                signal_id: "sig-natgas".to_string(),
                leader_id: "leader_a".to_string(),
                leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:NATGAS".to_string(),
                    side: crate::domain::OrderSide::Sell,
                    notional_usd: 12.0,
                    reduce_only: false,
                    cloid: "00000000-0000-0000-0000-000000000001".to_string(),
                },
            },
            CopyLiveDaemonWouldSubmitRef {
                record_index: 4,
                signal_id: "sig-spcx".to_string(),
                leader_id: "leader_b".to_string(),
                leader_address: "0x00000000000000000000000000000000000000bb".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:SPCX".to_string(),
                    side: crate::domain::OrderSide::Sell,
                    notional_usd: 12.0,
                    reduce_only: false,
                    cloid: "00000000-0000-0000-0000-000000000002".to_string(),
                },
            },
        ];

        let (executable, suppressed) =
            super::partition_copy_live_daemon_would_submit_refs(&refs, &options);

        assert_eq!(executable.len(), 1);
        assert_eq!(executable[0].signal_id, "sig-natgas");
        assert_eq!(executable[0].record_index, 3);
        assert_eq!(suppressed.len(), 1);
        assert_eq!(suppressed[0].plan.signal_id, "sig-spcx");
        assert_eq!(suppressed[0].plan.record_index, 4);
        assert_eq!(suppressed[0].reason_code, "COPY_DAEMON_MAX_LIVE_ORDERS");
    }

    #[test]
    fn copy_live_daemon_ref_partition_keeps_multi_account_fanout_together_under_cap() {
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string(), "addr_b".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: std::env::temp_dir().join("unused-daemon-fanout-partition.json"),
            shadow_history_path: std::env::temp_dir().join("unused-daemon-fanout-partition.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: false,
            submit: false,
            hold_positions_after_submit: true,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 900,
            max_events: 1000,
            max_live_orders: 2,
            max_total_notional_usd: 1000.0,
            max_total_fees_usd: 1.0,
            max_slippage_bps: 50.0,
            environment: None,
            ws_url: None,
        };
        let refs = vec![
            CopyLiveDaemonWouldSubmitRef {
                record_index: 0,
                signal_id: "copy-leader_a-event-one-IncreaseLong-open-1001".to_string(),
                leader_id: "leader_a".to_string(),
                leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:JPY".to_string(),
                    side: crate::domain::OrderSide::Buy,
                    notional_usd: 120.0,
                    reduce_only: false,
                    cloid: "00000000-0000-0000-0000-000000000021".to_string(),
                },
            },
            CopyLiveDaemonWouldSubmitRef {
                record_index: 1,
                signal_id: "copy-leader_a-event-two-IncreaseLong-open-1002".to_string(),
                leader_id: "leader_a".to_string(),
                leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:JPY".to_string(),
                    side: crate::domain::OrderSide::Buy,
                    notional_usd: 120.0,
                    reduce_only: false,
                    cloid: "00000000-0000-0000-0000-000000000022".to_string(),
                },
            },
            CopyLiveDaemonWouldSubmitRef {
                record_index: 2,
                signal_id: "copy-leader_a-event-one-IncreaseLong-open-2001".to_string(),
                leader_id: "leader_a".to_string(),
                leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_b".to_string(),
                    worker_id: "worker-addr_b".to_string(),
                    coin: "xyz:JPY".to_string(),
                    side: crate::domain::OrderSide::Buy,
                    notional_usd: 120.0,
                    reduce_only: false,
                    cloid: "00000000-0000-0000-0000-000000000023".to_string(),
                },
            },
            CopyLiveDaemonWouldSubmitRef {
                record_index: 3,
                signal_id: "copy-leader_a-event-two-IncreaseLong-open-2002".to_string(),
                leader_id: "leader_a".to_string(),
                leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_b".to_string(),
                    worker_id: "worker-addr_b".to_string(),
                    coin: "xyz:JPY".to_string(),
                    side: crate::domain::OrderSide::Buy,
                    notional_usd: 120.0,
                    reduce_only: false,
                    cloid: "00000000-0000-0000-0000-000000000024".to_string(),
                },
            },
        ];

        let (executable, suppressed) =
            super::partition_copy_live_daemon_would_submit_refs(&refs, &options);

        assert_eq!(executable.len(), 2);
        assert_eq!(
            executable
                .iter()
                .map(|plan| plan.order.account_id.as_str())
                .collect::<Vec<_>>(),
            vec!["addr_a", "addr_b"]
        );
        assert!(
            executable
                .iter()
                .all(|plan| plan.signal_id.contains("event-one"))
        );
        assert_eq!(suppressed.len(), 2);
        assert!(
            suppressed
                .iter()
                .all(|suppressed| suppressed.plan.signal_id.contains("event-two"))
        );
        assert!(
            suppressed
                .iter()
                .all(|suppressed| suppressed.reason_code == "COPY_DAEMON_MAX_LIVE_ORDERS")
        );
    }

    #[test]
    fn copy_live_daemon_ref_partition_does_not_suppress_reduce_only_close_by_open_order_cap() {
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: std::env::temp_dir().join("unused-daemon-close-cap.json"),
            shadow_history_path: std::env::temp_dir().join("unused-daemon-close-cap.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: false,
            submit: false,
            hold_positions_after_submit: false,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 900,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 50.0,
            max_total_fees_usd: 0.10,
            max_slippage_bps: 50.0,
            environment: None,
            ws_url: None,
        };
        let refs = vec![
            CopyLiveDaemonWouldSubmitRef {
                record_index: 0,
                signal_id: "sig-open-1".to_string(),
                leader_id: "leader_a".to_string(),
                leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:SP500".to_string(),
                    side: crate::domain::OrderSide::Buy,
                    notional_usd: 12.0,
                    reduce_only: false,
                    cloid: "00000000-0000-0000-0000-000000000001".to_string(),
                },
            },
            CopyLiveDaemonWouldSubmitRef {
                record_index: 1,
                signal_id: "sig-close-1".to_string(),
                leader_id: "leader_a".to_string(),
                leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:SP500".to_string(),
                    side: crate::domain::OrderSide::Sell,
                    notional_usd: 12.0,
                    reduce_only: true,
                    cloid: "00000000-0000-0000-0000-000000000002".to_string(),
                },
            },
            CopyLiveDaemonWouldSubmitRef {
                record_index: 2,
                signal_id: "sig-open-2".to_string(),
                leader_id: "leader_b".to_string(),
                leader_address: "0x00000000000000000000000000000000000000bb".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:XYZ100".to_string(),
                    side: crate::domain::OrderSide::Buy,
                    notional_usd: 12.0,
                    reduce_only: false,
                    cloid: "00000000-0000-0000-0000-000000000003".to_string(),
                },
            },
        ];

        let (executable, suppressed) =
            super::partition_copy_live_daemon_would_submit_refs(&refs, &options);

        assert_eq!(executable.len(), 2);
        assert_eq!(executable[0].signal_id, "sig-open-1");
        assert_eq!(executable[1].signal_id, "sig-close-1");
        assert!(executable[1].order.reduce_only);
        assert_eq!(suppressed.len(), 1);
        assert_eq!(suppressed[0].plan.signal_id, "sig-open-2");
        assert_eq!(suppressed[0].reason_code, "COPY_DAEMON_MAX_LIVE_ORDERS");
    }

    #[test]
    fn copy_live_daemon_ref_partition_does_not_charge_reduce_only_against_open_budget() {
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: std::env::temp_dir().join("unused-daemon-close-budget.json"),
            shadow_history_path: std::env::temp_dir().join("unused-daemon-close-budget.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: false,
            submit: false,
            hold_positions_after_submit: true,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 900,
            max_events: 1000,
            max_live_orders: 2,
            max_total_notional_usd: 12.0,
            max_total_fees_usd: 0.012,
            max_slippage_bps: 50.0,
            environment: None,
            ws_url: None,
        };
        let refs = vec![
            CopyLiveDaemonWouldSubmitRef {
                record_index: 0,
                signal_id: "sig-open".to_string(),
                leader_id: "leader_a".to_string(),
                leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:SP500".to_string(),
                    side: crate::domain::OrderSide::Buy,
                    notional_usd: 12.0,
                    reduce_only: false,
                    cloid: "00000000-0000-0000-0000-000000000011".to_string(),
                },
            },
            CopyLiveDaemonWouldSubmitRef {
                record_index: 1,
                signal_id: "sig-close-large".to_string(),
                leader_id: "leader_a".to_string(),
                leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:SP500".to_string(),
                    side: crate::domain::OrderSide::Sell,
                    notional_usd: 500.0,
                    reduce_only: true,
                    cloid: "00000000-0000-0000-0000-000000000012".to_string(),
                },
            },
            CopyLiveDaemonWouldSubmitRef {
                record_index: 2,
                signal_id: "sig-open-extra".to_string(),
                leader_id: "leader_b".to_string(),
                leader_address: "0x00000000000000000000000000000000000000bb".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:XYZ100".to_string(),
                    side: crate::domain::OrderSide::Buy,
                    notional_usd: 1.0,
                    reduce_only: false,
                    cloid: "00000000-0000-0000-0000-000000000013".to_string(),
                },
            },
        ];

        let (executable, suppressed) =
            super::partition_copy_live_daemon_would_submit_refs(&refs, &options);

        assert_eq!(
            executable
                .iter()
                .map(|plan| plan.signal_id.as_str())
                .collect::<Vec<_>>(),
            vec!["sig-open", "sig-close-large"]
        );
        assert!(executable[1].order.reduce_only);
        assert_eq!(suppressed.len(), 1);
        assert_eq!(suppressed[0].plan.signal_id, "sig-open-extra");
        assert_eq!(suppressed[0].reason_code, "COPY_DAEMON_MAX_TOTAL_NOTIONAL");
    }

    #[test]
    fn copy_live_daemon_reduce_only_position_filter_matches_local_direction() {
        fn state_with_position(coin: &str, szi: &str) -> crate::hyperliquid::ClearinghouseState {
            crate::hyperliquid::ClearinghouseState {
                margin_summary: crate::hyperliquid::MarginSummary::default(),
                cross_margin_summary: None,
                cross_maintenance_margin_used: None,
                withdrawable: None,
                asset_positions: vec![crate::hyperliquid::AssetPosition {
                    position: crate::hyperliquid::PerpPosition {
                        coin: coin.to_string(),
                        szi: szi.to_string(),
                        position_value: Some("123.45".to_string()),
                        unrealized_pnl: None,
                        ..Default::default()
                    },
                    position_type: None,
                }],
                time: None,
            }
        }
        fn reduce_plan(side: crate::domain::OrderSide) -> CopyLiveDaemonWouldSubmitRef {
            CopyLiveDaemonWouldSubmitRef {
                record_index: 0,
                signal_id: "sig-close".to_string(),
                leader_id: "leader_a".to_string(),
                leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:NATGAS".to_string(),
                    side,
                    notional_usd: 50.0,
                    reduce_only: true,
                    cloid: "00000000-0000-0000-0000-000000000001".to_string(),
                },
            }
        }

        let buy_to_close_short = reduce_plan(crate::domain::OrderSide::Buy);
        assert!(copy_live_daemon_reduce_only_ref_has_matching_position(
            &state_with_position("xyz:NATGAS", "-7.6"),
            &buy_to_close_short
        ));
        assert!(!copy_live_daemon_reduce_only_ref_has_matching_position(
            &state_with_position("xyz:NATGAS", "7.6"),
            &buy_to_close_short
        ));
        assert!(!copy_live_daemon_reduce_only_ref_has_matching_position(
            &state_with_position("xyz:NATGAS", "0.0"),
            &buy_to_close_short
        ));
        assert!(!copy_live_daemon_reduce_only_ref_has_matching_position(
            &state_with_position("xyz:GBP", "-7.6"),
            &buy_to_close_short
        ));

        let sell_to_close_long = reduce_plan(crate::domain::OrderSide::Sell);
        assert!(copy_live_daemon_reduce_only_ref_has_matching_position(
            &state_with_position("xyz:NATGAS", "7.6"),
            &sell_to_close_long
        ));
        assert!(!copy_live_daemon_reduce_only_ref_has_matching_position(
            &state_with_position("xyz:NATGAS", "-7.6"),
            &sell_to_close_long
        ));
    }

    #[test]
    fn copy_live_daemon_reduce_only_position_filter_caps_to_local_position_notional() {
        let state = crate::hyperliquid::ClearinghouseState {
            margin_summary: crate::hyperliquid::MarginSummary::default(),
            cross_margin_summary: None,
            cross_maintenance_margin_used: None,
            withdrawable: None,
            asset_positions: vec![crate::hyperliquid::AssetPosition {
                position: crate::hyperliquid::PerpPosition {
                    coin: "xyz:GBP".to_string(),
                    szi: "38.0".to_string(),
                    position_value: Some("12.5".to_string()),
                    unrealized_pnl: None,
                    ..Default::default()
                },
                position_type: None,
            }],
            time: None,
        };
        let plan = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-close".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:GBP".to_string(),
                side: crate::domain::OrderSide::Sell,
                notional_usd: 50.0,
                reduce_only: true,
                cloid: "00000000-0000-0000-0000-000000000001".to_string(),
            },
        };

        assert_eq!(
            copy_live_daemon_reduce_only_matching_position_notional_usd(&state, &plan),
            Some(12.5)
        );
    }

    #[test]
    fn copy_live_daemon_reduce_only_effective_notional_flattens_sub_min_residual() {
        assert_eq!(
            super::copy_live_daemon_reduce_only_effective_notional_usd(678.7088, 680.5952),
            680.5952
        );
        assert_eq!(
            super::copy_live_daemon_reduce_only_effective_notional_usd(70.0, 100.0),
            70.0
        );
        assert_eq!(
            super::copy_live_daemon_reduce_only_effective_notional_usd(150.0, 100.0),
            100.0
        );
        assert_eq!(
            super::copy_live_daemon_reduce_only_effective_notional_usd(8.0, 12.0),
            12.0
        );
        assert_eq!(
            super::copy_live_daemon_reduce_only_effective_notional_usd(7.0, 8.0),
            8.0
        );
        assert_eq!(
            super::copy_live_daemon_reduce_only_effective_notional_usd(0.0, 8.0),
            0.0
        );
    }

    #[test]
    fn copy_live_daemon_submit_plan_contract_allows_only_executable_refs() {
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: std::env::temp_dir().join("unused-submit-plan-contract.json"),
            shadow_history_path: std::env::temp_dir().join("unused-submit-plan-contract.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: false,
            submit: false,
            hold_positions_after_submit: false,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 900,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 50.0,
            max_total_fees_usd: 0.10,
            max_slippage_bps: 50.0,
            environment: None,
            ws_url: None,
        };
        let executable_refs = vec![
            CopyLiveDaemonWouldSubmitRef {
                record_index: 0,
                signal_id: "sig-open".to_string(),
                leader_id: "leader_a".to_string(),
                leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:SP500".to_string(),
                    side: crate::domain::OrderSide::Buy,
                    notional_usd: 12.0,
                    reduce_only: false,
                    cloid: "00000000-0000-0000-0000-000000000001".to_string(),
                },
            },
            CopyLiveDaemonWouldSubmitRef {
                record_index: 1,
                signal_id: "sig-close".to_string(),
                leader_id: "leader_a".to_string(),
                leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:SP500".to_string(),
                    side: crate::domain::OrderSide::Sell,
                    notional_usd: 12.0,
                    reduce_only: true,
                    cloid: "00000000-0000-0000-0000-000000000002".to_string(),
                },
            },
        ];
        let suppressed_refs = vec![CopyLiveDaemonSuppressedWouldSubmitRef {
            plan: CopyLiveDaemonWouldSubmitRef {
                record_index: 2,
                signal_id: "sig-suppressed".to_string(),
                leader_id: "leader_b".to_string(),
                leader_address: "0x00000000000000000000000000000000000000bb".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:XYZ100".to_string(),
                    side: crate::domain::OrderSide::Buy,
                    notional_usd: 12.0,
                    reduce_only: false,
                    cloid: "00000000-0000-0000-0000-000000000003".to_string(),
                },
            },
            reason_code: "COPY_DAEMON_MAX_LIVE_ORDERS".to_string(),
            message: "observation only".to_string(),
        }];
        let flat = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: true,
            open_order_count: Some(0),
            asset_positions: Some(0),
            position_summaries: Vec::new(),
            account_value: Some("100.0".to_string()),
            withdrawable: Some("100.0".to_string()),
            total_ntl_pos: Some("0.0".to_string()),
            total_margin_used: Some("0.0".to_string()),
            error: None,
        }];

        let contract = copy_live_daemon_submit_plan_contract(
            &options,
            &executable_refs,
            &suppressed_refs,
            24.0,
            0.024,
            &flat,
        );

        assert!(contract.ok, "{contract:#?}");
        assert_eq!(contract.executable_plan_count, 2);
        assert_eq!(contract.suppressed_plan_count, 1);
        assert_eq!(contract.executable_open_plan_count, 1);
        assert_eq!(contract.executable_reduce_only_plan_count, 1);
        assert!(
            contract
                .checks
                .iter()
                .any(|check| { check.name == "suppressed_refs_excluded_from_submit" && check.ok })
        );
    }

    #[test]
    fn copy_live_daemon_submit_evidence_contract_counts_open_orders_not_reduce_only_closes() {
        let acceptance = CopyLiveDaemonAcceptanceReport {
            ok: true,
            mode: "copy_live_daemon_acceptance_live_gate".to_string(),
            environment: "mainnet".to_string(),
            live_requested: true,
            live_submit_allowed: true,
            confirm_mainnet_live: true,
            max_duration_secs: 300,
            max_live_orders: 1,
            max_total_notional_usd: 50.0,
            max_total_fees_usd: 0.10,
            max_slippage_bps: 50.0,
            require_cleanup_after_submit: true,
            require_flat_reconcile_after_submit: true,
            target_accounts: vec!["addr_a".to_string()],
            leaders: Vec::new(),
            checks: Vec::new(),
            persistence_path: "unused".to_string(),
            shadow_history_path: "unused".to_string(),
            persistence_seen_keys_before: 0,
            persistence_ledger_entries_before: 0,
            restart_dedupe_probe: CopyLiveDaemonRestartProbe {
                event_id: "unused".to_string(),
                first_emit_count: 1,
                replay_emit_count: 0,
                fresh_after_restart_emit_count: 1,
                saved_seen_event_keys: 1,
                loaded_seen_event_keys: 1,
            },
            shadow_records_written: 0,
            approved_shadow_records: 0,
            would_submit_orders: Vec::new(),
            next_actions: Vec::new(),
        };
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: std::env::temp_dir().join("unused-evidence-open-count.json"),
            shadow_history_path: std::env::temp_dir().join("unused-evidence-open-count.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: true,
            submit: false,
            hold_positions_after_submit: false,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 300,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 50.0,
            max_total_fees_usd: 0.10,
            max_slippage_bps: 50.0,
            environment: Some("mainnet".to_string()),
            ws_url: None,
        };
        let orders = vec![
            CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:SP500".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 12.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000001".to_string(),
            },
            CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:SP500".to_string(),
                side: crate::domain::OrderSide::Sell,
                notional_usd: 12.0,
                reduce_only: true,
                cloid: "00000000-0000-0000-0000-000000000002".to_string(),
            },
        ];
        let flat = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: true,
            open_order_count: Some(0),
            asset_positions: Some(0),
            position_summaries: Vec::new(),
            account_value: Some("100.0".to_string()),
            withdrawable: Some("100.0".to_string()),
            total_ntl_pos: Some("0.0".to_string()),
            total_margin_used: Some("0.0".to_string()),
            error: None,
        }];

        let contract = copy_live_daemon_submit_evidence_contract(
            &options,
            &acceptance,
            &orders,
            24.0,
            0.024,
            &flat,
            None,
            &HashMap::new(),
        );

        let bounded = contract
            .checks
            .iter()
            .find(|check| check.name == "bounded_live_orders")
            .expect("bounded_live_orders check");
        assert!(bounded.ok, "{bounded:#?}");
        assert!(bounded.detail.contains("1 planned open/increase"));
        assert!(bounded.detail.contains("1 reduce-only close"));
    }

    #[test]
    fn copy_live_daemon_submit_plan_contract_rejects_suppressed_ref_overlap() {
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: std::env::temp_dir().join("unused-submit-plan-overlap.json"),
            shadow_history_path: std::env::temp_dir().join("unused-submit-plan-overlap.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: false,
            submit: false,
            hold_positions_after_submit: false,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 900,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 50.0,
            max_total_fees_usd: 0.10,
            max_slippage_bps: 50.0,
            environment: None,
            ws_url: None,
        };
        let plan = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-overlap".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:SP500".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 12.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000001".to_string(),
            },
        };
        let flat = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: true,
            open_order_count: Some(0),
            asset_positions: Some(0),
            position_summaries: Vec::new(),
            account_value: Some("100.0".to_string()),
            withdrawable: Some("100.0".to_string()),
            total_ntl_pos: Some("0.0".to_string()),
            total_margin_used: Some("0.0".to_string()),
            error: None,
        }];

        let plan_for_contract = plan.clone();
        let contract = copy_live_daemon_submit_plan_contract(
            &options,
            std::slice::from_ref(&plan_for_contract),
            &[CopyLiveDaemonSuppressedWouldSubmitRef {
                plan,
                reason_code: "COPY_DAEMON_MAX_LIVE_ORDERS".to_string(),
                message: "should not overlap".to_string(),
            }],
            12.0,
            0.012,
            &flat,
        );

        assert!(!contract.ok, "{contract:#?}");
        assert!(
            contract
                .checks
                .iter()
                .any(|check| { check.name == "suppressed_refs_excluded_from_submit" && !check.ok })
        );
    }

    #[test]
    fn copy_live_daemon_submit_plan_contract_rejects_account_exposure_over_cap() {
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: std::env::temp_dir().join("unused-submit-plan-exposure.json"),
            shadow_history_path: std::env::temp_dir().join("unused-submit-plan-exposure.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: true,
            submit: true,
            hold_positions_after_submit: true,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 900,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 50.0,
            max_total_fees_usd: 0.10,
            max_slippage_bps: 50.0,
            environment: Some("mainnet".to_string()),
            ws_url: None,
        };
        let open_ref = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-open-over-exposure".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:SP500".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 50.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000011".to_string(),
            },
        };
        let held_position = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: false,
            open_order_count: Some(0),
            asset_positions: Some(2),
            position_summaries: Vec::new(),
            account_value: Some("100.0".to_string()),
            withdrawable: Some("100.0".to_string()),
            total_ntl_pos: Some("42.0".to_string()),
            total_margin_used: Some("2.14".to_string()),
            error: None,
        }];

        let contract = copy_live_daemon_submit_plan_contract(
            &options,
            &[open_ref],
            &[],
            50.0,
            0.05,
            &held_position,
        );

        assert!(!contract.ok, "{contract:#?}");
        assert!(
            contract
                .checks
                .iter()
                .any(|check| { check.name == "bounded_account_total_exposure" && !check.ok }),
            "{contract:#?}"
        );
    }

    #[test]
    fn copy_live_daemon_submit_plan_contract_allows_reduce_only_over_exposure() {
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Sell,
            persistence_path: std::env::temp_dir().join("unused-submit-plan-reduce-exposure.json"),
            shadow_history_path: std::env::temp_dir()
                .join("unused-submit-plan-reduce-exposure.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: true,
            submit: true,
            hold_positions_after_submit: true,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 900,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 50.0,
            max_total_fees_usd: 0.10,
            max_slippage_bps: 50.0,
            environment: Some("mainnet".to_string()),
            ws_url: None,
        };
        let reduce_ref = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-reduce-held-position".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:JPY".to_string(),
                side: crate::domain::OrderSide::Sell,
                notional_usd: 42.0,
                reduce_only: true,
                cloid: "00000000-0000-0000-0000-000000000012".to_string(),
            },
        };
        let held_position = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: false,
            open_order_count: Some(0),
            asset_positions: Some(2),
            position_summaries: Vec::new(),
            account_value: Some("100.0".to_string()),
            withdrawable: Some("100.0".to_string()),
            total_ntl_pos: Some("42.0".to_string()),
            total_margin_used: Some("2.14".to_string()),
            error: None,
        }];

        let contract = copy_live_daemon_submit_plan_contract(
            &options,
            &[reduce_ref],
            &[],
            0.0,
            0.0,
            &held_position,
        );

        let exposure = contract
            .checks
            .iter()
            .find(|check| check.name == "bounded_account_total_exposure")
            .expect("bounded_account_total_exposure check");
        assert!(exposure.ok, "{contract:#?}");
        assert!(contract.ok, "{contract:#?}");
    }

    #[test]
    fn copy_live_daemon_submit_plan_contract_scopes_reduce_only_reconcile_to_submit_accounts() {
        let mut options = follow_position_options();
        options.account_ids = vec!["addr_a".to_string(), "addr_b".to_string()];
        options.max_total_notional_usd = 700.0;
        let reduce_ref = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-close-bot".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:BOT".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 29.6,
                reduce_only: true,
                cloid: "00000000-0000-0000-0000-000000000122".to_string(),
            },
        };
        let reconciliations = vec![
            CopyBoundedLiveWindowReconcile {
                account_id: "addr_a".to_string(),
                ok: false,
                open_order_count: Some(0),
                asset_positions: Some(1),
                position_summaries: Vec::new(),
                account_value: Some("78.0".to_string()),
                withdrawable: Some("73.0".to_string()),
                total_ntl_pos: Some("50.0".to_string()),
                total_margin_used: Some("5.0".to_string()),
                error: None,
            },
            CopyBoundedLiveWindowReconcile {
                account_id: "addr_b".to_string(),
                ok: false,
                open_order_count: None,
                asset_positions: None,
                position_summaries: Vec::new(),
                account_value: None,
                withdrawable: None,
                total_ntl_pos: None,
                total_margin_used: None,
                error: Some("temporary read timeout".to_string()),
            },
        ];

        let contract = copy_live_daemon_submit_plan_contract(
            &options,
            std::slice::from_ref(&reduce_ref),
            &[],
            0.0,
            0.0,
            &reconciliations,
        );

        assert!(contract.ok, "{contract:#?}");
        assert!(
            contract
                .checks
                .iter()
                .any(|check| check.name == "pre_submit_reconcile_health"
                    && check.ok
                    && check.detail.contains("scoped_accounts=addr_a")),
            "{contract:#?}"
        );

        let blocked_reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: false,
            open_order_count: Some(1),
            asset_positions: Some(1),
            position_summaries: Vec::new(),
            account_value: Some("78.0".to_string()),
            withdrawable: Some("73.0".to_string()),
            total_ntl_pos: Some("50.0".to_string()),
            total_margin_used: Some("5.0".to_string()),
            error: None,
        }];
        let blocked = copy_live_daemon_submit_plan_contract(
            &options,
            &[reduce_ref],
            &[],
            0.0,
            0.0,
            &blocked_reconciliations,
        );

        assert!(!blocked.ok, "{blocked:#?}");
        assert!(
            blocked
                .checks
                .iter()
                .any(|check| check.name == "pre_submit_reconcile_health" && !check.ok),
            "{blocked:#?}"
        );
    }

    #[test]
    fn copy_live_daemon_submit_plan_contract_does_not_cap_reduce_only_notional_or_fees() {
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:GBP".to_string(),
            side: crate::domain::OrderSide::Sell,
            persistence_path: std::env::temp_dir().join("unused-reduce-cap.json"),
            shadow_history_path: std::env::temp_dir().join("unused-reduce-cap.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: true,
            submit: true,
            hold_positions_after_submit: true,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 900,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 10.0,
            max_total_fees_usd: 0.001,
            max_slippage_bps: 50.0,
            environment: Some("mainnet".to_string()),
            ws_url: None,
        };
        let reduce_ref = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-close".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:GBP".to_string(),
                side: crate::domain::OrderSide::Sell,
                notional_usd: 50.0,
                reduce_only: true,
                cloid: "00000000-0000-0000-0000-000000000001".to_string(),
            },
        };
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: false,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: Vec::new(),
            account_value: Some("42.0".to_string()),
            withdrawable: Some("3.0".to_string()),
            total_ntl_pos: Some("200.0".to_string()),
            total_margin_used: Some("39.0".to_string()),
            error: None,
        }];
        let planned_open_notional =
            copy_live_daemon_open_notional_usd_from_refs(std::slice::from_ref(&reduce_ref));
        let estimated_open_fees = normalize_report_zero(planned_open_notional * 0.001);
        let contract = copy_live_daemon_submit_plan_contract(
            &options,
            &[reduce_ref],
            &[],
            planned_open_notional,
            estimated_open_fees,
            &reconciliations,
        );

        assert!(contract.ok, "{contract:#?}");
        assert_eq!(contract.planned_notional_usd, 0.0);
        assert_eq!(contract.estimated_fees_usd, 0.0);
        assert_eq!(contract.executable_reduce_only_plan_count, 1);
    }

    #[test]
    fn copy_live_daemon_market_scope_keeps_unselected_market_exit_only() {
        let mut options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: vec!["xyz_perp".to_string()],
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: std::env::temp_dir().join("unused-market-scope.json"),
            shadow_history_path: std::env::temp_dir().join("unused-market-scope.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: true,
            submit: true,
            hold_positions_after_submit: true,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 900,
            max_events: 1000,
            max_live_orders: 10,
            max_total_notional_usd: 500.0,
            max_total_fees_usd: 1.0,
            max_slippage_bps: 50.0,
            environment: Some("mainnet".to_string()),
            ws_url: None,
        };
        let open_hl_ref = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-hl-open".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "BTC".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 50.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000101".to_string(),
            },
        };
        let mut reduce_hl_ref = open_hl_ref.clone();
        reduce_hl_ref.signal_id = "sig-hl-close".to_string();
        reduce_hl_ref.order.reduce_only = true;
        reduce_hl_ref.order.side = crate::domain::OrderSide::Sell;
        reduce_hl_ref.order.cloid = "00000000-0000-0000-0000-000000000102".to_string();

        let open_hl_ref_for_partition = open_hl_ref.clone();
        let (executable, suppressed) = partition_copy_live_daemon_would_submit_refs(
            &[open_hl_ref_for_partition, reduce_hl_ref],
            &options,
        );
        assert_eq!(executable.len(), 1);
        assert!(executable[0].order.reduce_only);
        assert_eq!(suppressed.len(), 1);
        assert_eq!(suppressed[0].reason_code, "COPY_DAEMON_MARKET_EXIT_ONLY");

        options.markets.push("hl_perp".to_string());
        let (executable, suppressed) =
            partition_copy_live_daemon_would_submit_refs(&[open_hl_ref], &options);
        assert_eq!(executable.len(), 1);
        assert!(suppressed.is_empty());
    }

    #[test]
    fn copy_live_daemon_pipeline_uses_account_ratio_and_configured_principal_cap() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18039")?;
        let mut config = crate::config::load_config(std::path::Path::new(&config_path))?;
        let account = config
            .accounts
            .iter_mut()
            .find(|account| account.account_id == "addr_a")
            .context("addr_a test account")?;
        account.copy_ratio = 0.2;
        account.max_order_notional_usd = 350.0;
        let account = account.clone();
        let leader = crate::strategies::smart_money::SmartMoneyLeaderWatch {
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
        };
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: vec!["xyz_perp".to_string()],
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: std::env::temp_dir().join("unused-ratio-cap-pipeline.json"),
            shadow_history_path: std::env::temp_dir().join("unused-ratio-cap-pipeline.jsonl"),
            leader_notional_usd: 1000.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: true,
            submit: true,
            hold_positions_after_submit: true,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 60,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 500.0,
            max_total_fees_usd: 1.0,
            max_slippage_bps: 50.0,
            environment: Some("mainnet".to_string()),
            ws_url: None,
        };
        let mut pipeline = crate::copy_live_daemon_supervisor_pipeline(
            &config,
            &options,
            &account,
            &["addr_a".to_string()],
            std::slice::from_ref(&leader),
            &crate::strategies::smart_money::CopyPersistenceSnapshot::new(
                0,
                Vec::new(),
                &crate::strategies::smart_money::CopyLedger::new(),
            ),
        );
        let now = crate::domain::now_ms();
        let before = crate::copy_shadow_position_event(&leader, "xyz:XYZ100", 0.0, 0.0, now, "xyz");
        let fill = crate::strategy::LeaderFillEvent {
            event_id: format!("ratio-cap-fill-{now}"),
            leader_id: leader.leader_id.clone(),
            leader_address: leader.leader_address.clone(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            price: 1.0,
            size: 1000.0,
            notional_usd: 1000.0,
            reduce_only: false,
            exchange_time_ms: now + 1,
            received_at_ms: now + 1,
        };
        let after = crate::copy_shadow_position_event(
            &leader,
            "xyz:XYZ100",
            1000.0,
            1000.0,
            now + 2,
            "xyz",
        );

        let mut records = Vec::new();
        records.extend(pipeline.handle_watcher_event(before, now));
        records.extend(pipeline.handle_watcher_event(
            crate::strategies::smart_money::CopyLeaderWatcherEvent::Fill {
                leader_id: leader.leader_id.clone(),
                leader_address: leader.leader_address.clone(),
                fill,
                is_snapshot: false,
            },
            now + 1,
        ));
        records.extend(pipeline.handle_watcher_event(after, now + 2));

        let approved = records
            .iter()
            .find_map(|record| record.signal.as_ref())
            .context("approved copy signal")?;
        assert_eq!(approved.order.notional_usd, 350.0);
        Ok(())
    }

    #[test]
    fn copy_live_daemon_supervisor_pipelines_size_each_local_account_independently() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18040")?;
        let mut config = crate::config::load_config(std::path::Path::new(&config_path))?;
        for account in &mut config.accounts {
            match account.account_id.as_str() {
                "addr_a" => {
                    account.copy_ratio = 0.2;
                    account.max_order_notional_usd = 100.0;
                }
                "addr_b" => {
                    account.copy_ratio = 0.5;
                    account.max_order_notional_usd = 300.0;
                }
                _ => {}
            }
        }
        let leader = crate::strategies::smart_money::SmartMoneyLeaderWatch {
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
        };
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string(), "addr_b".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: vec!["xyz_perp".to_string()],
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: std::env::temp_dir().join("unused-multi-pipeline-sizing.json"),
            shadow_history_path: std::env::temp_dir().join("unused-multi-pipeline-sizing.jsonl"),
            leader_notional_usd: 1000.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: true,
            submit: true,
            hold_positions_after_submit: true,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 60,
            max_events: 1000,
            max_live_orders: 2,
            max_total_notional_usd: 1000.0,
            max_total_fees_usd: 1.0,
            max_slippage_bps: 50.0,
            environment: Some("mainnet".to_string()),
            ws_url: None,
        };
        let persistence = crate::strategies::smart_money::CopyPersistenceSnapshot::new(
            0,
            Vec::new(),
            &crate::strategies::smart_money::CopyLedger::new(),
        );
        let now = crate::domain::now_ms();
        let before = crate::copy_shadow_position_event(&leader, "xyz:XYZ100", 0.0, 0.0, now, "xyz");
        let fill_event = crate::strategies::smart_money::CopyLeaderWatcherEvent::Fill {
            leader_id: leader.leader_id.clone(),
            leader_address: leader.leader_address.clone(),
            fill: crate::strategy::LeaderFillEvent {
                event_id: format!("multi-account-sizing-fill-{now}"),
                leader_id: leader.leader_id.clone(),
                leader_address: leader.leader_address.clone(),
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                price: 1.0,
                size: 1000.0,
                notional_usd: 1000.0,
                reduce_only: false,
                exchange_time_ms: now + 1,
                received_at_ms: now + 1,
            },
            is_snapshot: false,
        };
        let after = crate::copy_shadow_position_event(
            &leader,
            "xyz:XYZ100",
            1000.0,
            1000.0,
            now + 2,
            "xyz",
        );

        let mut records = Vec::new();
        let mut merged_snapshot = persistence.clone();
        for account_id in ["addr_a", "addr_b"] {
            let account = config.account(account_id).context("test account")?;
            let mut pipeline = crate::copy_live_daemon_supervisor_pipeline(
                &config,
                &options,
                account,
                std::slice::from_ref(&account.account_id),
                std::slice::from_ref(&leader),
                &persistence,
            );
            records.extend(pipeline.handle_watcher_event(before.clone(), now));
            records.extend(pipeline.handle_watcher_event(fill_event.clone(), now + 1));
            records.extend(pipeline.handle_watcher_event(after.clone(), now + 2));
            merged_snapshot = crate::copy_live_daemon_merge_persistence_snapshots(
                merged_snapshot,
                pipeline.persistence_snapshot(now + 3),
            );
        }

        let refs = plan_copy_daemon_acceptance_order_refs(&config, &records)?;
        let by_account = refs
            .iter()
            .map(|plan| (plan.order.account_id.as_str(), plan.order.notional_usd))
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(by_account.get("addr_a").copied(), Some(100.0));
        assert_eq!(by_account.get("addr_b").copied(), Some(300.0));
        assert_eq!(merged_snapshot.ledger_entries.len(), 2);
        assert!(merged_snapshot.ledger_entries.iter().any(|entry| {
            entry.local_account_id == "addr_a" && (entry.planned_notional_usd - 100.0).abs() < 1e-9
        }));
        assert!(merged_snapshot.ledger_entries.iter().any(|entry| {
            entry.local_account_id == "addr_b" && (entry.planned_notional_usd - 300.0).abs() < 1e-9
        }));
        Ok(())
    }

    #[test]
    fn copy_live_daemon_submit_refs_allow_large_reduce_only_exit() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18041")?;
        let mut config = crate::config::load_config(std::path::Path::new(&config_path))?;
        let account = config
            .accounts
            .iter_mut()
            .find(|account| account.account_id == "addr_b")
            .context("addr_b test account")?;
        account.max_order_notional_usd = 350.0;

        let now = crate::domain::now_ms();
        let signal = crate::domain::CoordinatorSignal {
            signal_id: format!("copy-leader_a-large-gbp-reduce-close-{now}"),
            source: crate::domain::SignalSource::SmartMoney,
            created_at_ms: now,
            dispatch_at_ms: now,
            expires_at_ms: now + 60_000,
            target_accounts: vec!["addr_b".to_string()],
            dedupe_key: "leader_a-large-gbp-reduce".to_string(),
            order: crate::domain::SignalOrder {
                market: Some("xyz_perp".to_string()),
                dex: Some("xyz".to_string()),
                coin: "xyz:GBP".to_string(),
                side: crate::domain::OrderSide::Sell,
                notional_usd: 700.0,
                reduce_only: true,
                execution_mode: crate::domain::ExecutionMode::Taker,
                max_slippage_bps: 50.0,
                limit_price: None,
                apply_account_ratio: false,
            },
        };
        let record = crate::strategies::smart_money::CopyDryRunShadowRecord {
            action: crate::strategies::smart_money::SemanticLeaderAction {
                leader_id: "leader_a".to_string(),
                leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
                market: Some("xyz_perp".to_string()),
                dex: Some("xyz".to_string()),
                coin: "xyz:GBP".to_string(),
                event_id: "large-gbp-reduce".to_string(),
                kind: crate::strategies::smart_money::LeaderActionKind::ReduceLong,
                confidence: crate::strategies::smart_money::LeaderActionConfidence::Strong,
                leader_notional_usd: 700.0,
                close_leader_notional_usd: Some(700.0),
                open_leader_notional_usd: None,
                exchange_time_ms: now,
                received_at_ms: now,
                reason: "test large mapped reduce".to_string(),
            },
            live_gate: crate::strategies::smart_money::CopyLiveGateDecision::LiveAllowed,
            risk_decision: crate::strategies::smart_money::CopySignalRiskDecision::Approved {
                side: crate::domain::OrderSide::Sell,
                reduce_only: true,
                notional_usd: 700.0,
            },
            signal: Some(signal),
            ledger_entry: None,
            persistence_snapshot: crate::strategies::smart_money::CopyPersistenceSnapshot::empty(),
        };

        let refs = plan_copy_daemon_acceptance_order_refs(&config, &[record])?;

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].order.account_id, "addr_b");
        assert!(refs[0].order.reduce_only);
        assert_eq!(refs[0].order.notional_usd, 700.0);
        Ok(())
    }

    #[test]
    fn copy_live_daemon_contract_exposure_failure_suppresses_open_ref() {
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Sell,
            persistence_path: std::env::temp_dir().join("unused-submit-plan-suppress.json"),
            shadow_history_path: std::env::temp_dir().join("unused-submit-plan-suppress.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: true,
            submit: true,
            hold_positions_after_submit: true,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 900,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 50.0,
            max_total_fees_usd: 0.25,
            max_slippage_bps: 50.0,
            environment: Some("mainnet".to_string()),
            ws_url: None,
        };
        let open_ref = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-open-over-exposure".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Sell,
                notional_usd: 50.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000013".to_string(),
            },
        };
        let held_position = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: false,
            open_order_count: Some(0),
            asset_positions: Some(2),
            position_summaries: Vec::new(),
            account_value: Some("100.0".to_string()),
            withdrawable: Some("100.0".to_string()),
            total_ntl_pos: Some("42.0".to_string()),
            total_margin_used: Some("2.14".to_string()),
            error: None,
        }];
        let contract = copy_live_daemon_submit_plan_contract(
            &options,
            std::slice::from_ref(&open_ref),
            &[],
            50.0,
            0.05,
            &held_position,
        );
        assert!(!contract.ok, "{contract:#?}");

        let (executable, suppressed, adjusted_contract) =
            copy_live_daemon_suppress_refs_rejected_by_submit_contract(
                &options,
                vec![open_ref],
                Vec::new(),
                50.0,
                0.05,
                &held_position,
                None,
                contract,
                &HashMap::new(),
            );

        assert!(executable.is_empty());
        assert_eq!(suppressed.len(), 1);
        assert_eq!(
            suppressed[0].reason_code,
            "COPY_DAEMON_MAX_ACCOUNT_EXPOSURE"
        );
        assert!(adjusted_contract.ok, "{adjusted_contract:#?}");
        assert_eq!(adjusted_contract.executable_plan_count, 0);
        assert_eq!(adjusted_contract.suppressed_plan_count, 1);
    }

    #[test]
    fn copy_live_daemon_contract_margin_failure_suppresses_open_ref() {
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: std::env::temp_dir().join("unused-submit-plan-margin.json"),
            shadow_history_path: std::env::temp_dir().join("unused-submit-plan-margin.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: true,
            submit: true,
            hold_positions_after_submit: true,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 900,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 150.0,
            max_total_fees_usd: 1.0,
            max_slippage_bps: 50.0,
            environment: Some("mainnet".to_string()),
            ws_url: None,
        };
        let open_ref = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-open-low-margin".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:SP500".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 50.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000014".to_string(),
            },
        };
        let low_margin_position = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: false,
            open_order_count: Some(0),
            asset_positions: Some(2),
            position_summaries: Vec::new(),
            account_value: Some("7.14".to_string()),
            withdrawable: Some("5.0".to_string()),
            total_ntl_pos: Some("42.0".to_string()),
            total_margin_used: Some("2.14".to_string()),
            error: None,
        }];

        let contract = copy_live_daemon_submit_plan_contract(
            &options,
            std::slice::from_ref(&open_ref),
            &[],
            50.0,
            0.05,
            &low_margin_position,
        );
        assert!(!contract.ok, "{contract:#?}");
        assert!(
            contract
                .checks
                .iter()
                .any(|check| { check.name == "bounded_open_margin_available" && !check.ok }),
            "{contract:#?}"
        );

        let (executable, suppressed, adjusted_contract) =
            copy_live_daemon_suppress_refs_rejected_by_submit_contract(
                &options,
                vec![open_ref],
                Vec::new(),
                50.0,
                0.05,
                &low_margin_position,
                None,
                contract,
                &HashMap::new(),
            );

        assert!(executable.is_empty());
        assert_eq!(suppressed.len(), 1);
        assert_eq!(suppressed[0].reason_code, "COPY_DAEMON_INSUFFICIENT_MARGIN");
        assert!(adjusted_contract.ok, "{adjusted_contract:#?}");
        assert_eq!(adjusted_contract.executable_plan_count, 0);
        assert_eq!(adjusted_contract.suppressed_plan_count, 1);
    }

    #[test]
    fn copy_live_daemon_margin_resize_shrinks_open_ref_before_cap_partition() {
        let refs = vec![
            CopyLiveDaemonWouldSubmitRef {
                record_index: 0,
                signal_id: "sig-open-low-margin".to_string(),
                leader_id: "leader_a".to_string(),
                leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:XYZ100".to_string(),
                    side: crate::domain::OrderSide::Sell,
                    notional_usd: 50.0,
                    reduce_only: false,
                    cloid: "00000000-0000-0000-0000-000000000101".to_string(),
                },
            },
            CopyLiveDaemonWouldSubmitRef {
                record_index: 1,
                signal_id: "sig-open-second".to_string(),
                leader_id: "leader_b".to_string(),
                leader_address: "0x00000000000000000000000000000000000000bb".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:GBP".to_string(),
                    side: crate::domain::OrderSide::Buy,
                    notional_usd: 50.0,
                    reduce_only: false,
                    cloid: "00000000-0000-0000-0000-000000000102".to_string(),
                },
            },
        ];
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: true,
            open_order_count: Some(0),
            asset_positions: Some(0),
            position_summaries: Vec::new(),
            account_value: Some("20.0".to_string()),
            withdrawable: Some("3.267497".to_string()),
            total_ntl_pos: Some("0.0".to_string()),
            total_margin_used: Some("0.0".to_string()),
            error: None,
        }];

        let (prepared, suppressed) =
            super::copy_live_daemon_resize_open_refs_for_margin(&refs, &reconciliations);

        assert_eq!(prepared.len(), 1);
        assert_eq!(suppressed.len(), 1);
        assert_eq!(
            suppressed[0].reason_code,
            "COPY_DAEMON_MARGIN_RESIZED_BELOW_MIN"
        );
        assert_eq!(prepared[0].signal_id, "sig-open-low-margin");
        let expected_resized_notional = 3.267497
            / ((1.0 + super::COPY_DAEMON_MARGIN_BUFFER_RATIO) / 10.0
                + super::COPY_DAEMON_FEE_BUFFER_RATIO);
        assert!(
            (prepared[0].order.notional_usd - expected_resized_notional).abs() < 0.0001,
            "{prepared:#?}"
        );
    }

    #[test]
    fn copy_live_daemon_margin_resize_below_min_does_not_consume_live_order_slot() {
        let options = follow_position_options();
        let refs = vec![
            CopyLiveDaemonWouldSubmitRef {
                record_index: 0,
                signal_id: "sig-too-small".to_string(),
                leader_id: "leader_a".to_string(),
                leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:XYZ100".to_string(),
                    side: crate::domain::OrderSide::Sell,
                    notional_usd: 50.0,
                    reduce_only: false,
                    cloid: "00000000-0000-0000-0000-000000000111".to_string(),
                },
            },
            CopyLiveDaemonWouldSubmitRef {
                record_index: 1,
                signal_id: "sig-other-account".to_string(),
                leader_id: "leader_b".to_string(),
                leader_address: "0x00000000000000000000000000000000000000bb".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_b".to_string(),
                    worker_id: "worker-addr_b".to_string(),
                    coin: "xyz:GBP".to_string(),
                    side: crate::domain::OrderSide::Buy,
                    notional_usd: 12.0,
                    reduce_only: false,
                    cloid: "00000000-0000-0000-0000-000000000112".to_string(),
                },
            },
        ];
        let reconciliations = vec![
            CopyBoundedLiveWindowReconcile {
                account_id: "addr_a".to_string(),
                ok: true,
                open_order_count: Some(0),
                asset_positions: Some(0),
                position_summaries: Vec::new(),
                account_value: Some("2.0".to_string()),
                withdrawable: Some("1.0".to_string()),
                total_ntl_pos: Some("0.0".to_string()),
                total_margin_used: Some("0.0".to_string()),
                error: None,
            },
            CopyBoundedLiveWindowReconcile {
                account_id: "addr_b".to_string(),
                ok: true,
                open_order_count: Some(0),
                asset_positions: Some(0),
                position_summaries: Vec::new(),
                account_value: Some("20.0".to_string()),
                withdrawable: Some("20.0".to_string()),
                total_ntl_pos: Some("0.0".to_string()),
                total_margin_used: Some("0.0".to_string()),
                error: None,
            },
        ];

        let (margin_prepared, mut margin_suppressed) =
            super::copy_live_daemon_resize_open_refs_for_margin(&refs, &reconciliations);
        let (executable, mut cap_suppressed) =
            super::partition_copy_live_daemon_would_submit_refs(&margin_prepared, &options);
        margin_suppressed.append(&mut cap_suppressed);

        assert_eq!(executable.len(), 1);
        assert_eq!(executable[0].signal_id, "sig-other-account");
        assert_eq!(margin_suppressed.len(), 1);
        assert_eq!(
            margin_suppressed[0].reason_code,
            "COPY_DAEMON_MARGIN_RESIZED_BELOW_MIN"
        );
    }

    #[test]
    fn copy_live_daemon_live_constraints_resize_before_account_exposure_cap() {
        let mut options = follow_position_options();
        options.max_total_notional_usd = 3_000.0;
        options.max_total_fees_usd = 1.0;
        let persistence = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![crate::strategies::smart_money::CopyLedgerEntry {
                local_account_id: "addr_a".to_string(),
                leader_id: "leader_a".to_string(),
                leader_group: "leader_a".to_string(),
                signal_id: "sig-existing-open".to_string(),
                coin: "xyz:MU".to_string(),
                local_side: crate::domain::OrderSide::Buy,
                order_cloid: Some("existing-cloid".to_string()),
                order_oid: Some(123),
                submitted_at_ms: Some(now_ms()),
                filled_at_ms: Some(now_ms()),
                planned_notional_usd: 2_905.0,
                pending_notional_usd: 0.0,
                filled_notional_usd: 2_905.0,
                remaining_notional_usd: 2_905.0,
                status: crate::strategies::smart_money::CopyLedgerStatus::Open,
            }],
        };
        let refs = vec![CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-resizable-open".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:MU".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 350.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000131".to_string(),
            },
        }];
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: true,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: Vec::new(),
            account_value: Some("200.0".to_string()),
            withdrawable: Some("10.0".to_string()),
            total_ntl_pos: Some("1030.0".to_string()),
            total_margin_used: Some("103.0".to_string()),
            error: None,
        }];

        let (executable, suppressed) = super::copy_live_daemon_executable_refs_for_snapshot(
            &refs,
            &options,
            &persistence,
            &reconciliations,
        );

        assert!(suppressed.is_empty(), "{suppressed:#?}");
        assert_eq!(executable.len(), 1);
        assert_eq!(executable[0].signal_id, "sig-resizable-open");
        assert!(executable[0].order.notional_usd < 350.0);
        assert!(
            executable[0].order.notional_usd > crate::trading::HYPERLIQUID_MIN_ORDER_NOTIONAL_USD
        );
    }

    #[test]
    fn copy_live_daemon_live_total_exposure_overrides_stale_symbol_ledger_for_open_cap() {
        let mut options = follow_position_options();
        options.max_total_notional_usd = 3_000.0;
        let persistence = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![crate::strategies::smart_money::CopyLedgerEntry {
                local_account_id: "addr_a".to_string(),
                leader_id: "leader_a".to_string(),
                leader_group: "leader_a".to_string(),
                signal_id: "sig-stale-mu-open".to_string(),
                coin: "xyz:MU".to_string(),
                local_side: crate::domain::OrderSide::Buy,
                order_cloid: Some("stale-cloid".to_string()),
                order_oid: Some(123),
                submitted_at_ms: Some(now_ms()),
                filled_at_ms: Some(now_ms()),
                planned_notional_usd: 2_939.0,
                pending_notional_usd: 0.0,
                filled_notional_usd: 2_939.0,
                remaining_notional_usd: 2_939.0,
                status: crate::strategies::smart_money::CopyLedgerStatus::Open,
            }],
        };
        let open_ref = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-new-mu-open".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:MU".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 133.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000132".to_string(),
            },
        };
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: true,
            open_order_count: Some(0),
            asset_positions: Some(3),
            position_summaries: Vec::new(),
            account_value: Some("139.0".to_string()),
            withdrawable: Some("14.7".to_string()),
            total_ntl_pos: Some("1024.5".to_string()),
            total_margin_used: Some("124.8".to_string()),
            error: None,
        }];

        let (prepared, suppressed) = copy_live_daemon_prepare_refs_for_follow_position_limits(
            &[open_ref],
            &options,
            &persistence,
            &reconciliations,
        );

        assert!(suppressed.is_empty(), "{suppressed:#?}");
        assert_eq!(prepared.len(), 1);
        assert_eq!(prepared[0].signal_id, "sig-new-mu-open");
    }

    #[test]
    fn copy_live_daemon_live_symbol_cap_suppresses_additional_same_coin_open() {
        let mut options = follow_position_options();
        options.max_total_notional_usd = 3_000.0;
        let persistence = crate::strategies::smart_money::CopyPersistenceSnapshot::empty();
        let open_ref = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-new-qnt-open".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_b".to_string(),
                worker_id: "worker-addr_b".to_string(),
                coin: "xyz:QNT".to_string(),
                side: crate::domain::OrderSide::Sell,
                notional_usd: 50.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000135".to_string(),
            },
        };
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_b".to_string(),
            ok: true,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: vec![super::CopyBoundedLiveWindowPositionSummary {
                coin: "xyz:QNT".to_string(),
                szi: "-8.58".to_string(),
                position_value: Some("637.30".to_string()),
                unrealized_pnl: None,
            }],
            account_value: Some("78.0".to_string()),
            withdrawable: Some("100.0".to_string()),
            total_ntl_pos: Some("720.0".to_string()),
            total_margin_used: Some("72.0".to_string()),
            error: None,
        }];
        let account_symbol_caps = std::collections::HashMap::from([("addr_b".to_string(), 350.0)]);

        let (prepared, suppressed) =
            super::copy_live_daemon_prepare_refs_for_follow_position_limits_with_symbol_caps(
                &[open_ref],
                &options,
                &persistence,
                &reconciliations,
                &account_symbol_caps,
            );

        assert!(prepared.is_empty());
        assert_eq!(suppressed.len(), 1);
        assert_eq!(suppressed[0].reason_code, "COPY_DAEMON_MAX_SYMBOL_EXPOSURE");
    }

    #[test]
    fn copy_live_daemon_live_symbol_cap_allows_reduce_only_exit_above_cap() {
        let mut options = follow_position_options();
        options.max_total_notional_usd = 3_000.0;
        let persistence = crate::strategies::smart_money::CopyPersistenceSnapshot::empty();
        let reduce_ref = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-qnt-reduce".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_b".to_string(),
                worker_id: "worker-addr_b".to_string(),
                coin: "xyz:QNT".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 637.30,
                reduce_only: true,
                cloid: "00000000-0000-0000-0000-000000000136".to_string(),
            },
        };
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_b".to_string(),
            ok: true,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: vec![super::CopyBoundedLiveWindowPositionSummary {
                coin: "xyz:QNT".to_string(),
                szi: "-8.58".to_string(),
                position_value: Some("637.30".to_string()),
                unrealized_pnl: None,
            }],
            account_value: Some("78.0".to_string()),
            withdrawable: Some("0.0".to_string()),
            total_ntl_pos: Some("720.0".to_string()),
            total_margin_used: Some("72.0".to_string()),
            error: None,
        }];
        let account_symbol_caps = std::collections::HashMap::from([("addr_b".to_string(), 350.0)]);

        let (prepared, suppressed) =
            super::copy_live_daemon_prepare_refs_for_follow_position_limits_with_symbol_caps(
                &[reduce_ref],
                &options,
                &persistence,
                &reconciliations,
                &account_symbol_caps,
            );

        assert_eq!(prepared.len(), 1);
        assert!(suppressed.is_empty(), "{suppressed:#?}");
        assert!(prepared[0].order.reduce_only);
        assert_eq!(prepared[0].order.notional_usd, 637.30);
    }

    #[test]
    fn copy_live_daemon_live_account_cap_suppresses_new_symbol_when_global_budget_allows() {
        let mut options = follow_position_options();
        options.max_total_notional_usd = 700.0;
        let persistence = crate::strategies::smart_money::CopyPersistenceSnapshot::empty();
        let open_ref = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-account-cap-open".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:JPY".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 20.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000137".to_string(),
            },
        };
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: true,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: vec![super::CopyBoundedLiveWindowPositionSummary {
                coin: "xyz:GBP".to_string(),
                szi: "250.0".to_string(),
                position_value: Some("340.0".to_string()),
                unrealized_pnl: None,
            }],
            account_value: Some("90.0".to_string()),
            withdrawable: Some("80.0".to_string()),
            total_ntl_pos: Some("340.0".to_string()),
            total_margin_used: Some("34.0".to_string()),
            error: None,
        }];
        let account_caps = HashMap::from([("addr_a".to_string(), 350.0)]);

        let (prepared, suppressed) =
            super::copy_live_daemon_prepare_refs_for_follow_position_limits_with_symbol_caps(
                &[open_ref],
                &options,
                &persistence,
                &reconciliations,
                &account_caps,
            );

        assert!(prepared.is_empty());
        assert_eq!(suppressed.len(), 1);
        assert_eq!(
            suppressed[0].reason_code,
            "COPY_DAEMON_MAX_ACCOUNT_EXPOSURE"
        );
    }

    #[test]
    fn copy_live_daemon_submit_plan_uses_account_cap_not_global_budget() {
        let mut options = follow_position_options();
        options.max_total_notional_usd = 700.0;
        options.max_total_fees_usd = 1.0;
        let open_ref = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-submit-account-cap".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:JPY".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 20.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000138".to_string(),
            },
        };
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: true,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: Vec::new(),
            account_value: Some("90.0".to_string()),
            withdrawable: Some("80.0".to_string()),
            total_ntl_pos: Some("340.0".to_string()),
            total_margin_used: Some("34.0".to_string()),
            error: None,
        }];
        let account_caps = HashMap::from([("addr_a".to_string(), 350.0)]);

        let contract = super::copy_live_daemon_submit_plan_contract_with_account_caps(
            &options,
            &[open_ref],
            &[],
            20.0,
            0.02,
            &reconciliations,
            &account_caps,
        );

        assert!(!contract.ok, "{contract:#?}");
        assert!(
            contract
                .checks
                .iter()
                .any(|check| check.name == "bounded_account_total_exposure" && !check.ok),
            "{contract:#?}"
        );
    }

    #[test]
    fn copy_live_daemon_margin_resize_leaves_reduce_only_refs_untouched() {
        let refs = vec![CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-close".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:GBP".to_string(),
                side: crate::domain::OrderSide::Sell,
                notional_usd: 50.0,
                reduce_only: true,
                cloid: "00000000-0000-0000-0000-000000000121".to_string(),
            },
        }];
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: true,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: Vec::new(),
            account_value: Some("1.0".to_string()),
            withdrawable: Some("0.0".to_string()),
            total_ntl_pos: Some("50.0".to_string()),
            total_margin_used: Some("1.0".to_string()),
            error: None,
        }];

        let (prepared, suppressed) =
            super::copy_live_daemon_resize_open_refs_for_margin(&refs, &reconciliations);

        assert_eq!(prepared.len(), 1);
        assert!(suppressed.is_empty(), "{suppressed:#?}");
        assert!(prepared[0].order.reduce_only);
        assert_eq!(prepared[0].order.notional_usd, 50.0);
    }

    #[test]
    fn copy_live_daemon_reduce_refs_wait_until_pending_min_notional_accumulates() {
        let pending_entry = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_a".to_string(),
            leader_id: "leader_a".to_string(),
            leader_group: "leader_a".to_string(),
            signal_id: "sig-prior-reduce".to_string(),
            coin: "xyz:GBP".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: None,
            order_oid: None,
            submitted_at_ms: None,
            filled_at_ms: None,
            planned_notional_usd: 7.4,
            pending_notional_usd: 7.4,
            filled_notional_usd: 0.0,
            remaining_notional_usd: 7.4,
            status: crate::strategies::smart_money::CopyLedgerStatus::PendingReduce,
        };
        let persistence = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![pending_entry],
        };
        let reduce_ref = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-next-reduce".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:GBP".to_string(),
                side: crate::domain::OrderSide::Sell,
                notional_usd: 2.7,
                reduce_only: true,
                cloid: "33333333-3333-5333-8333-333333333333".to_string(),
            },
        };

        let (prepared, suppressed) = copy_live_daemon_prepare_refs_for_follow_position_limits(
            &[reduce_ref],
            &follow_position_options(),
            &persistence,
            &[],
        );

        assert!(suppressed.is_empty(), "{suppressed:#?}");
        assert_eq!(prepared.len(), 1);
        assert!((prepared[0].order.notional_usd - 10.1).abs() < 1e-9);
    }

    #[test]
    fn copy_live_daemon_recovers_persisted_pending_reduce_refs_for_retry() {
        let pending_entry = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_a".to_string(),
            leader_id: "leader_a".to_string(),
            leader_group: "leader_a".to_string(),
            signal_id: "sig-persisted-reduce".to_string(),
            coin: "xyz:JPY".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: None,
            order_oid: None,
            submitted_at_ms: None,
            filled_at_ms: None,
            planned_notional_usd: 59.8438,
            pending_notional_usd: 59.8438,
            filled_notional_usd: 0.0,
            remaining_notional_usd: 59.8438,
            status: crate::strategies::smart_money::CopyLedgerStatus::PendingReduce,
        };
        let open_entry = crate::strategies::smart_money::CopyLedgerEntry {
            status: crate::strategies::smart_money::CopyLedgerStatus::Open,
            signal_id: "sig-open".to_string(),
            ..pending_entry.clone()
        };
        let persistence = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![pending_entry, open_entry],
        };

        let refs = super::copy_live_daemon_recover_pending_reduce_plan_refs(
            &persistence,
            &["addr_a".to_string(), "addr_b".to_string()],
        );

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].signal_id, "sig-persisted-reduce");
        assert_eq!(refs[0].order.account_id, "addr_a");
        assert_eq!(refs[0].order.coin, "xyz:JPY");
        assert_eq!(refs[0].order.side, crate::domain::OrderSide::Sell);
        assert!(refs[0].order.reduce_only);
        assert!((refs[0].order.notional_usd - 59.8438).abs() < 1e-9);
        uuid::Uuid::parse_str(&refs[0].order.cloid).expect("stable retry cloid");

        let refs_again = super::copy_live_daemon_recover_pending_reduce_plan_refs(
            &persistence,
            &["addr_a".to_string()],
        );
        assert_eq!(refs_again[0].order.cloid, refs[0].order.cloid);
    }

    #[test]
    fn copy_live_daemon_reduce_refs_suppress_below_accumulated_min_notional() {
        let persistence = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: Vec::new(),
        };
        let reduce_ref = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-small-reduce".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:GBP".to_string(),
                side: crate::domain::OrderSide::Sell,
                notional_usd: 7.4,
                reduce_only: true,
                cloid: "44444444-4444-5444-8444-444444444444".to_string(),
            },
        };

        let (prepared, suppressed) = copy_live_daemon_prepare_refs_for_follow_position_limits(
            &[reduce_ref],
            &follow_position_options(),
            &persistence,
            &[],
        );

        assert!(prepared.is_empty());
        assert_eq!(suppressed.len(), 1);
        assert_eq!(
            suppressed[0].reason_code,
            "COPY_DAEMON_PENDING_REDUCE_BELOW_MIN_NOTIONAL"
        );
    }

    #[test]
    fn copy_live_daemon_follow_position_allows_different_symbol_under_symbol_cap() {
        let mut options = follow_position_options();
        options.max_total_notional_usd = 100.0;
        let open_entry = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_a".to_string(),
            leader_id: "leader_a".to_string(),
            leader_group: "leader_a".to_string(),
            signal_id: "sig-open".to_string(),
            coin: "xyz:GBP".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: None,
            order_oid: None,
            submitted_at_ms: None,
            filled_at_ms: None,
            planned_notional_usd: 41.7,
            pending_notional_usd: 0.0,
            filled_notional_usd: 41.7,
            remaining_notional_usd: 41.7,
            status: crate::strategies::smart_money::CopyLedgerStatus::Open,
        };
        let persistence = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![open_entry],
        };
        let open_ref = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-next-open".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:JPY".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 34.3,
                reduce_only: false,
                cloid: "55555555-5555-5555-8555-555555555555".to_string(),
            },
        };

        let (prepared, suppressed) = copy_live_daemon_prepare_refs_for_follow_position_limits(
            &[open_ref],
            &options,
            &persistence,
            &[],
        );

        assert!(suppressed.is_empty(), "{suppressed:#?}");
        assert_eq!(prepared.len(), 1);
    }

    #[test]
    fn copy_live_daemon_follow_position_suppresses_same_symbol_above_cap() {
        let open_entry = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_a".to_string(),
            leader_id: "leader_a".to_string(),
            leader_group: "leader_a".to_string(),
            signal_id: "sig-open".to_string(),
            coin: "xyz:JPY".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: None,
            order_oid: Some(123),
            submitted_at_ms: Some(now_ms()),
            filled_at_ms: Some(now_ms()),
            planned_notional_usd: 41.7,
            pending_notional_usd: 0.0,
            filled_notional_usd: 41.7,
            remaining_notional_usd: 41.7,
            status: crate::strategies::smart_money::CopyLedgerStatus::Open,
        };
        let persistence = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![open_entry],
        };
        let open_ref = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-next-open".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:JPY".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 34.3,
                reduce_only: false,
                cloid: "55555555-5555-5555-8555-555555555555".to_string(),
            },
        };

        let (prepared, suppressed) = copy_live_daemon_prepare_refs_for_follow_position_limits(
            &[open_ref],
            &follow_position_options(),
            &persistence,
            &[],
        );

        assert!(prepared.is_empty());
        assert_eq!(suppressed.len(), 1);
        assert_eq!(
            suppressed[0].reason_code,
            "COPY_DAEMON_MAX_ACCOUNT_EXPOSURE"
        );
    }

    #[test]
    fn copy_live_daemon_follow_position_ignores_unsubmitted_pending_open_exposure() {
        let pending_entry = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_a".to_string(),
            leader_id: "leader_a".to_string(),
            leader_group: "leader_a".to_string(),
            signal_id: "sig-shadow-only-open".to_string(),
            coin: "xyz:GBP".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: None,
            order_oid: None,
            submitted_at_ms: None,
            filled_at_ms: None,
            planned_notional_usd: 50.0,
            pending_notional_usd: 50.0,
            filled_notional_usd: 0.0,
            remaining_notional_usd: 0.0,
            status: crate::strategies::smart_money::CopyLedgerStatus::PendingOpen,
        };
        let persistence = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![pending_entry],
        };
        let open_ref = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-next-open".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:JPY".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 50.0,
                reduce_only: false,
                cloid: "55555555-5555-5555-8555-555555555555".to_string(),
            },
        };

        let (prepared, suppressed) = copy_live_daemon_prepare_refs_for_follow_position_limits(
            &[open_ref],
            &follow_position_options(),
            &persistence,
            &[],
        );

        assert!(suppressed.is_empty(), "{suppressed:#?}");
        assert_eq!(prepared.len(), 1);
    }

    #[test]
    fn copy_live_daemon_follow_position_ignores_current_round_ref_in_snapshot() {
        let current_signal = "sig-current-open";
        let pending_entry = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_a".to_string(),
            leader_id: "leader_a".to_string(),
            leader_group: "leader_a".to_string(),
            signal_id: current_signal.to_string(),
            coin: "xyz:JPY".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: None,
            order_oid: None,
            submitted_at_ms: None,
            filled_at_ms: None,
            planned_notional_usd: 50.0,
            pending_notional_usd: 50.0,
            filled_notional_usd: 0.0,
            remaining_notional_usd: 0.0,
            status: crate::strategies::smart_money::CopyLedgerStatus::PendingOpen,
        };
        let persistence = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![pending_entry],
        };
        let open_ref = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: current_signal.to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:JPY".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 50.0,
                reduce_only: false,
                cloid: "66666666-6666-5666-8666-666666666666".to_string(),
            },
        };

        let (prepared, suppressed) = copy_live_daemon_prepare_refs_for_follow_position_limits(
            &[open_ref],
            &follow_position_options(),
            &persistence,
            &[],
        );

        assert!(suppressed.is_empty(), "{suppressed:#?}");
        assert_eq!(prepared.len(), 1);
    }

    #[test]
    fn copy_live_daemon_persistence_save_drops_unsubmitted_pending_opens() {
        let pending_open = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_a".to_string(),
            leader_id: "leader_a".to_string(),
            leader_group: "leader_a".to_string(),
            signal_id: "sig-shadow-only-open".to_string(),
            coin: "xyz:JPY".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: None,
            order_oid: None,
            submitted_at_ms: None,
            filled_at_ms: None,
            planned_notional_usd: 20.0,
            pending_notional_usd: 20.0,
            filled_notional_usd: 0.0,
            remaining_notional_usd: 0.0,
            status: crate::strategies::smart_money::CopyLedgerStatus::PendingOpen,
        };
        let pending_reduce = crate::strategies::smart_money::CopyLedgerEntry {
            status: crate::strategies::smart_money::CopyLedgerStatus::PendingReduce,
            signal_id: "sig-pending-reduce".to_string(),
            pending_notional_usd: 7.4,
            remaining_notional_usd: 7.4,
            ..pending_open.clone()
        };
        let submitted_open = crate::strategies::smart_money::CopyLedgerEntry {
            signal_id: "sig-submitted-open".to_string(),
            order_cloid: Some("11111111-1111-5111-8111-111111111111".to_string()),
            submitted_at_ms: Some(now_ms()),
            ..pending_open.clone()
        };
        let snapshot = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: vec!["seen-a".to_string()],
            ledger_entries: vec![pending_open, pending_reduce, submitted_open],
        };

        let filtered = copy_live_daemon_persistence_snapshot_for_save(snapshot);

        assert_eq!(filtered.seen_event_keys, vec!["seen-a"]);
        assert_eq!(filtered.ledger_entries.len(), 2);
        assert!(
            filtered
                .ledger_entries
                .iter()
                .all(|entry| entry.signal_id != "sig-shadow-only-open")
        );
        assert!(
            filtered
                .ledger_entries
                .iter()
                .any(|entry| entry.signal_id == "sig-pending-reduce")
        );
        assert!(
            filtered
                .ledger_entries
                .iter()
                .any(|entry| entry.signal_id == "sig-submitted-open")
        );
    }

    #[test]
    fn copy_live_daemon_persistence_merge_preserves_existing_open_entries() {
        let existing_open = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_a".to_string(),
            leader_id: "leader_a".to_string(),
            leader_group: "leader_a".to_string(),
            signal_id: "sig-existing-jpy".to_string(),
            coin: "xyz:JPY".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: Some("11111111-1111-5111-8111-111111111111".to_string()),
            order_oid: Some(1001),
            submitted_at_ms: Some(now_ms()),
            filled_at_ms: Some(now_ms()),
            planned_notional_usd: 50.0,
            pending_notional_usd: 0.0,
            filled_notional_usd: 41.7,
            remaining_notional_usd: 41.7,
            status: crate::strategies::smart_money::CopyLedgerStatus::Open,
        };
        let incoming_open = crate::strategies::smart_money::CopyLedgerEntry {
            signal_id: "sig-new-natgas".to_string(),
            coin: "xyz:NATGAS".to_string(),
            local_side: crate::domain::OrderSide::Sell,
            order_cloid: Some("22222222-2222-5222-8222-222222222222".to_string()),
            order_oid: Some(1002),
            filled_notional_usd: 0.32,
            remaining_notional_usd: 0.32,
            ..existing_open.clone()
        };
        let unsubmitted_pending = crate::strategies::smart_money::CopyLedgerEntry {
            signal_id: "sig-shadow-only".to_string(),
            coin: "xyz:NATGAS".to_string(),
            order_cloid: None,
            order_oid: None,
            submitted_at_ms: None,
            filled_at_ms: None,
            pending_notional_usd: 50.0,
            status: crate::strategies::smart_money::CopyLedgerStatus::PendingOpen,
            ..existing_open.clone()
        };
        let existing = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: vec!["seen-existing".to_string()],
            ledger_entries: vec![existing_open],
        };
        let incoming = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: vec!["seen-new".to_string()],
            ledger_entries: vec![incoming_open, unsubmitted_pending],
        };

        let merged = copy_live_daemon_merge_persistence_snapshots_for_save(existing, incoming);

        assert_eq!(merged.seen_event_keys, vec!["seen-existing", "seen-new"]);
        assert_eq!(merged.ledger_entries.len(), 2);
        assert!(
            merged
                .ledger_entries
                .iter()
                .any(|entry| entry.signal_id == "sig-existing-jpy")
        );
        assert!(
            merged
                .ledger_entries
                .iter()
                .any(|entry| entry.signal_id == "sig-new-natgas")
        );
        assert!(
            merged
                .ledger_entries
                .iter()
                .all(|entry| entry.signal_id != "sig-shadow-only")
        );
    }

    #[test]
    fn copy_live_daemon_persistence_merge_drops_recovered_open_covered_by_evidence() {
        let recovered_open = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_a".to_string(),
            leader_id: "leader_4".to_string(),
            leader_group: "leader_4".to_string(),
            signal_id: "sig-recovered-sp500".to_string(),
            coin: "xyz:SP500".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: None,
            order_oid: None,
            submitted_at_ms: Some(1_000),
            filled_at_ms: Some(1_100),
            planned_notional_usd: 162.20,
            pending_notional_usd: 0.0,
            filled_notional_usd: 162.20,
            remaining_notional_usd: 162.20,
            status: crate::strategies::smart_money::CopyLedgerStatus::Open,
        };
        let evidenced_open = crate::strategies::smart_money::CopyLedgerEntry {
            signal_id: "sig-evidenced-sp500".to_string(),
            order_cloid: Some("33333333-3333-5333-8333-333333333333".to_string()),
            order_oid: Some(3003),
            submitted_at_ms: Some(2_000),
            filled_at_ms: Some(2_100),
            planned_notional_usd: 162.05,
            filled_notional_usd: 162.05,
            remaining_notional_usd: 162.05,
            ..recovered_open.clone()
        };
        let existing = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![recovered_open],
        };
        let incoming = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![evidenced_open],
        };

        let merged = copy_live_daemon_merge_persistence_snapshots_for_save(existing, incoming);

        assert!(
            merged
                .ledger_entries
                .iter()
                .all(|entry| entry.signal_id != "sig-recovered-sp500")
        );
        let evidenced = merged
            .ledger_entries
            .iter()
            .find(|entry| entry.signal_id == "sig-evidenced-sp500")
            .expect("evidenced open");
        assert_eq!(
            evidenced.status,
            crate::strategies::smart_money::CopyLedgerStatus::Open
        );
        assert_eq!(merged.ledger_entries.len(), 1);
    }

    #[test]
    fn copy_live_daemon_persistence_merge_keeps_uncovered_recovered_residual() {
        let recovered_open = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_a".to_string(),
            leader_id: "leader_4".to_string(),
            leader_group: "leader_4".to_string(),
            signal_id: "sig-recovered-sp500".to_string(),
            coin: "xyz:SP500".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: None,
            order_oid: None,
            submitted_at_ms: Some(1_000),
            filled_at_ms: Some(1_100),
            planned_notional_usd: 500.0,
            pending_notional_usd: 0.0,
            filled_notional_usd: 500.0,
            remaining_notional_usd: 500.0,
            status: crate::strategies::smart_money::CopyLedgerStatus::Open,
        };
        let evidenced_open = crate::strategies::smart_money::CopyLedgerEntry {
            signal_id: "sig-evidenced-sp500".to_string(),
            order_cloid: Some("44444444-4444-5444-8444-444444444444".to_string()),
            order_oid: Some(4004),
            submitted_at_ms: Some(2_000),
            filled_at_ms: Some(2_100),
            planned_notional_usd: 300.0,
            filled_notional_usd: 300.0,
            remaining_notional_usd: 300.0,
            ..recovered_open.clone()
        };
        let existing = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![recovered_open],
        };
        let incoming = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![evidenced_open],
        };

        let merged = copy_live_daemon_merge_persistence_snapshots_for_save(existing, incoming);

        let recovered = merged
            .ledger_entries
            .iter()
            .find(|entry| entry.signal_id == "sig-recovered-sp500")
            .expect("residual recovered open");
        assert_eq!(
            recovered.status,
            crate::strategies::smart_money::CopyLedgerStatus::Open
        );
        assert!((recovered.remaining_notional_usd - 200.0).abs() < 1e-9);
        assert!((recovered.filled_notional_usd - 200.0).abs() < 1e-9);
        assert_eq!(merged.ledger_entries.len(), 2);
    }

    #[test]
    fn copy_live_daemon_persistence_merge_replaces_pending_reduce_with_closed_submission() {
        let pending_reduce = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_a".to_string(),
            leader_id: "leader_a".to_string(),
            leader_group: "leader_a".to_string(),
            signal_id: "sig-close".to_string(),
            coin: "xyz:SKHX".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: None,
            order_oid: None,
            submitted_at_ms: None,
            filled_at_ms: None,
            planned_notional_usd: 49.0,
            pending_notional_usd: 49.0,
            filled_notional_usd: 0.0,
            remaining_notional_usd: 49.0,
            status: crate::strategies::smart_money::CopyLedgerStatus::PendingReduce,
        };
        let closed_reduce = crate::strategies::smart_money::CopyLedgerEntry {
            order_cloid: Some("77777777-7777-5777-8777-777777777777".to_string()),
            order_oid: Some(7001),
            submitted_at_ms: Some(now_ms()),
            filled_at_ms: Some(now_ms()),
            pending_notional_usd: 0.0,
            filled_notional_usd: 48.9,
            remaining_notional_usd: 0.0,
            status: crate::strategies::smart_money::CopyLedgerStatus::Closed,
            ..pending_reduce.clone()
        };
        let existing = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: vec!["seen-a".to_string()],
            ledger_entries: vec![pending_reduce],
        };
        let incoming = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: vec!["seen-b".to_string()],
            ledger_entries: vec![closed_reduce],
        };

        let merged = copy_live_daemon_merge_persistence_snapshots_for_save(existing, incoming);

        assert_eq!(merged.ledger_entries.len(), 1);
        let entry = &merged.ledger_entries[0];
        assert_eq!(entry.signal_id, "sig-close");
        assert_eq!(
            entry.status,
            crate::strategies::smart_money::CopyLedgerStatus::Closed
        );
        assert_eq!(entry.order_oid, Some(7001));
        assert_eq!(entry.remaining_notional_usd, 0.0);
    }

    #[test]
    fn copy_live_daemon_persistence_merge_closed_reduce_consumes_open_entry() {
        let open_entry = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_a".to_string(),
            leader_id: "leader_a".to_string(),
            leader_group: "leader_a".to_string(),
            signal_id: "sig-open".to_string(),
            coin: "xyz:SKHX".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: Some("11111111-1111-5111-8111-111111111111".to_string()),
            order_oid: Some(9001),
            submitted_at_ms: Some(now_ms()),
            filled_at_ms: Some(now_ms()),
            planned_notional_usd: 50.0,
            pending_notional_usd: 0.0,
            filled_notional_usd: 49.0941,
            remaining_notional_usd: 49.0941,
            status: crate::strategies::smart_money::CopyLedgerStatus::Open,
        };
        let closed_reduce = crate::strategies::smart_money::CopyLedgerEntry {
            signal_id: "sig-close-close-123".to_string(),
            order_cloid: Some("22222222-2222-5222-8222-222222222222".to_string()),
            order_oid: Some(9002),
            planned_notional_usd: 49.0941,
            pending_notional_usd: 0.0,
            filled_notional_usd: 49.0622,
            remaining_notional_usd: 0.0,
            status: crate::strategies::smart_money::CopyLedgerStatus::Closed,
            ..open_entry.clone()
        };
        let existing = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![open_entry],
        };
        let incoming = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![closed_reduce],
        };

        let merged = copy_live_daemon_merge_persistence_snapshots_for_save(existing, incoming);

        let open = merged
            .ledger_entries
            .iter()
            .find(|entry| entry.signal_id == "sig-open")
            .expect("open entry");
        assert_eq!(
            open.status,
            crate::strategies::smart_money::CopyLedgerStatus::Closed
        );
        assert_eq!(open.remaining_notional_usd, 0.0);
        let close = merged
            .ledger_entries
            .iter()
            .find(|entry| entry.signal_id == "sig-close-close-123")
            .expect("closed reduce entry");
        assert_eq!(
            close.status,
            crate::strategies::smart_money::CopyLedgerStatus::Closed
        );
    }

    #[test]
    fn copy_live_daemon_persistence_merge_closed_reduce_does_not_consume_later_open() {
        let earlier_open = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_a".to_string(),
            leader_id: "leader_a".to_string(),
            leader_group: "leader_a".to_string(),
            signal_id: "sig-open-before".to_string(),
            coin: "xyz:JPY".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: Some("11111111-1111-5111-8111-111111111111".to_string()),
            order_oid: Some(9001),
            submitted_at_ms: Some(1_000),
            filled_at_ms: Some(1_100),
            planned_notional_usd: 50.0,
            pending_notional_usd: 0.0,
            filled_notional_usd: 49.0,
            remaining_notional_usd: 49.0,
            status: crate::strategies::smart_money::CopyLedgerStatus::Open,
        };
        let later_open = crate::strategies::smart_money::CopyLedgerEntry {
            signal_id: "sig-open-after".to_string(),
            order_cloid: Some("33333333-3333-5333-8333-333333333333".to_string()),
            order_oid: Some(9003),
            submitted_at_ms: Some(3_000),
            filled_at_ms: Some(3_100),
            ..earlier_open.clone()
        };
        let closed_reduce = crate::strategies::smart_money::CopyLedgerEntry {
            signal_id: "sig-close-close-123".to_string(),
            order_cloid: Some("22222222-2222-5222-8222-222222222222".to_string()),
            order_oid: Some(9002),
            submitted_at_ms: Some(2_000),
            filled_at_ms: Some(2_100),
            planned_notional_usd: 49.0,
            pending_notional_usd: 0.0,
            filled_notional_usd: 49.0,
            remaining_notional_usd: 0.0,
            status: crate::strategies::smart_money::CopyLedgerStatus::Closed,
            ..earlier_open.clone()
        };
        let existing = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![earlier_open, later_open],
        };
        let incoming = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![closed_reduce],
        };

        let merged = copy_live_daemon_merge_persistence_snapshots_for_save(existing, incoming);

        let earlier = merged
            .ledger_entries
            .iter()
            .find(|entry| entry.signal_id == "sig-open-before")
            .expect("earlier open entry");
        assert_eq!(
            earlier.status,
            crate::strategies::smart_money::CopyLedgerStatus::Closed
        );
        let later = merged
            .ledger_entries
            .iter()
            .find(|entry| entry.signal_id == "sig-open-after")
            .expect("later open entry");
        assert_eq!(
            later.status,
            crate::strategies::smart_money::CopyLedgerStatus::Open
        );
        assert_eq!(later.remaining_notional_usd, 49.0);
    }

    #[test]
    fn copy_live_daemon_prunes_stale_ledger_entries_without_live_position() {
        let stale_open = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_a".to_string(),
            leader_id: "leader_a".to_string(),
            leader_group: "leader_a".to_string(),
            signal_id: "sig-stale-gbp-open".to_string(),
            coin: "xyz:GBP".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: Some("11111111-1111-5111-8111-111111111111".to_string()),
            order_oid: Some(9001),
            submitted_at_ms: Some(now_ms()),
            filled_at_ms: Some(now_ms()),
            planned_notional_usd: 50.0,
            pending_notional_usd: 0.0,
            filled_notional_usd: 49.3025,
            remaining_notional_usd: 0.0814,
            status: crate::strategies::smart_money::CopyLedgerStatus::Open,
        };
        let stale_reduce = crate::strategies::smart_money::CopyLedgerEntry {
            signal_id: "sig-stale-gbp-reduce".to_string(),
            order_cloid: None,
            order_oid: None,
            submitted_at_ms: None,
            filled_at_ms: None,
            planned_notional_usd: 0.0814,
            pending_notional_usd: 0.0814,
            filled_notional_usd: 0.0,
            remaining_notional_usd: 0.0814,
            status: crate::strategies::smart_money::CopyLedgerStatus::PendingReduce,
            ..stale_open.clone()
        };
        let live_jpy = crate::strategies::smart_money::CopyLedgerEntry {
            signal_id: "sig-live-jpy-open".to_string(),
            coin: "xyz:JPY".to_string(),
            remaining_notional_usd: 49.0,
            ..stale_open.clone()
        };
        let snapshot = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: vec!["seen-gbp".to_string()],
            ledger_entries: vec![stale_open, stale_reduce, live_jpy],
        };
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: false,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: vec![super::CopyBoundedLiveWindowPositionSummary {
                coin: "xyz:JPY".to_string(),
                szi: "0.62".to_string(),
                position_value: Some("99.9254".to_string()),
                unrealized_pnl: None,
            }],
            account_value: Some("42.5".to_string()),
            withdrawable: Some("13.1".to_string()),
            total_ntl_pos: Some("99.9".to_string()),
            total_margin_used: Some("19.9".to_string()),
            error: None,
        }];

        let pruned = super::copy_live_daemon_prune_snapshot_against_reconciliations(
            snapshot,
            &reconciliations,
        );

        assert_eq!(pruned.seen_event_keys, vec!["seen-gbp"]);
        assert_eq!(pruned.ledger_entries.len(), 1);
        assert_eq!(pruned.ledger_entries[0].signal_id, "sig-live-jpy-open");
    }

    #[test]
    fn copy_live_daemon_prune_keeps_snapshot_when_reconcile_unreadable() {
        let entry = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_a".to_string(),
            leader_id: "leader_a".to_string(),
            leader_group: "leader_a".to_string(),
            signal_id: "sig-open".to_string(),
            coin: "xyz:GBP".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: Some("11111111-1111-5111-8111-111111111111".to_string()),
            order_oid: Some(9001),
            submitted_at_ms: Some(now_ms()),
            filled_at_ms: Some(now_ms()),
            planned_notional_usd: 50.0,
            pending_notional_usd: 0.0,
            filled_notional_usd: 49.3025,
            remaining_notional_usd: 49.3025,
            status: crate::strategies::smart_money::CopyLedgerStatus::Open,
        };
        let snapshot = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![entry],
        };
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: false,
            open_order_count: None,
            asset_positions: None,
            position_summaries: Vec::new(),
            account_value: None,
            withdrawable: None,
            total_ntl_pos: None,
            total_margin_used: None,
            error: Some("network timeout".to_string()),
        }];

        let pruned = super::copy_live_daemon_prune_snapshot_against_reconciliations(
            snapshot,
            &reconciliations,
        );

        assert_eq!(pruned.ledger_entries.len(), 1);
        assert_eq!(pruned.ledger_entries[0].signal_id, "sig-open");
    }

    #[test]
    fn copy_live_daemon_market_state_prune_keeps_non_default_perp_pending_reduce() {
        let entry = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_a".to_string(),
            leader_id: "leader_hl".to_string(),
            leader_group: "leader_hl".to_string(),
            signal_id: "sig-hl-reduce".to_string(),
            coin: "hl:BTC".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: None,
            order_oid: None,
            submitted_at_ms: None,
            filled_at_ms: None,
            planned_notional_usd: 6.8,
            pending_notional_usd: 6.8,
            filled_notional_usd: 0.0,
            remaining_notional_usd: 6.8,
            status: crate::strategies::smart_money::CopyLedgerStatus::PendingReduce,
        };
        let snapshot = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: vec!["seen-hl".to_string()],
            ledger_entries: vec![entry],
        };
        let mut config = crate::config::AppConfig::default();
        config.hyperliquid.dex = "xyz".to_string();
        let readable_accounts = HashSet::from(["addr_a".to_string()]);
        let mut perp_state_by_scope = std::collections::HashMap::new();
        perp_state_by_scope.insert(
            ("addr_a".to_string(), "hl".to_string()),
            Ok(crate::hyperliquid::ClearinghouseState {
                margin_summary: crate::hyperliquid::MarginSummary::default(),
                cross_margin_summary: None,
                cross_maintenance_margin_used: None,
                withdrawable: None,
                asset_positions: vec![crate::hyperliquid::AssetPosition {
                    position: crate::hyperliquid::PerpPosition {
                        coin: "hl:BTC".to_string(),
                        szi: "0.15".to_string(),
                        ..Default::default()
                    },
                    position_type: None,
                }],
                time: None,
            }),
        );

        let pruned = super::copy_live_daemon_prune_snapshot_against_market_state_maps(
            &config,
            snapshot,
            &readable_accounts,
            &perp_state_by_scope,
            &std::collections::HashMap::new(),
        );

        assert_eq!(pruned.seen_event_keys, vec!["seen-hl"]);
        assert_eq!(pruned.ledger_entries.len(), 1);
        assert_eq!(pruned.ledger_entries[0].signal_id, "sig-hl-reduce");
    }

    #[test]
    fn copy_live_daemon_market_state_prune_keeps_spot_pending_reduce_with_inventory() {
        let entry = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_a".to_string(),
            leader_id: "leader_spot".to_string(),
            leader_group: "leader_spot".to_string(),
            signal_id: "sig-spot-reduce".to_string(),
            coin: "BTC/USDC".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: None,
            order_oid: None,
            submitted_at_ms: None,
            filled_at_ms: None,
            planned_notional_usd: 4.2,
            pending_notional_usd: 4.2,
            filled_notional_usd: 0.0,
            remaining_notional_usd: 4.2,
            status: crate::strategies::smart_money::CopyLedgerStatus::PendingReduce,
        };
        let snapshot = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: vec!["seen-spot".to_string()],
            ledger_entries: vec![entry],
        };
        let readable_accounts = HashSet::from(["addr_a".to_string()]);
        let mut spot_state_by_account = std::collections::HashMap::new();
        spot_state_by_account.insert(
            "addr_a".to_string(),
            Ok(crate::hyperliquid::SpotClearinghouseState {
                balances: vec![crate::hyperliquid::SpotBalance {
                    coin: "BTC".to_string(),
                    total: "0.025".to_string(),
                    hold: "0.0001".to_string(),
                    ..Default::default()
                }],
            }),
        );

        let pruned = super::copy_live_daemon_prune_snapshot_against_market_state_maps(
            &crate::config::AppConfig::default(),
            snapshot,
            &readable_accounts,
            &std::collections::HashMap::new(),
            &spot_state_by_account,
        );

        assert_eq!(pruned.seen_event_keys, vec!["seen-spot"]);
        assert_eq!(pruned.ledger_entries.len(), 1);
        assert_eq!(pruned.ledger_entries[0].signal_id, "sig-spot-reduce");
    }

    #[test]
    fn copy_live_daemon_recovers_open_ledger_from_live_position_and_shadow() {
        let temp = std::env::temp_dir().join(format!("trade_xyz_copy_recover_ledger_{}", now_ms()));
        std::fs::create_dir_all(&temp).expect("test dir");
        let shadow_path = temp.join("shadow.jsonl");
        let shadow = crate::strategies::smart_money::CopyShadowHistoryEntry {
            schema_version: 1,
            occurred_at_ms: 12345,
            status: "would_copy".to_string(),
            leader_id: "leader_5".to_string(),
            leader_address: "0xd8c5228c515db3043dfa0c8cd6f22450ee9a99b0".to_string(),
            coin: "xyz:GOLD".to_string(),
            action_kind: "IncreaseLong".to_string(),
            action_event_id: "evt-gold".to_string(),
            live_gate: "live_allowed".to_string(),
            risk_reject_reason: None,
            signal_id: Some("sig-gold".to_string()),
            side: Some(crate::domain::OrderSide::Buy),
            reduce_only: Some(false),
            notional_usd: Some(350.0),
            ledger_status: Some(crate::strategies::smart_money::CopyLedgerStatus::PendingOpen),
            ..Default::default()
        };
        std::fs::write(
            &shadow_path,
            format!("{}\n", serde_json::to_string(&shadow).expect("shadow json")),
        )
        .expect("shadow file");

        let snapshot = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: Vec::new(),
        };
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: false,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: vec![super::CopyBoundedLiveWindowPositionSummary {
                coin: "xyz:GOLD".to_string(),
                szi: "0.085".to_string(),
                position_value: Some("349.996".to_string()),
                unrealized_pnl: None,
            }],
            account_value: None,
            withdrawable: None,
            total_ntl_pos: Some("349.996".to_string()),
            total_margin_used: Some("35.0".to_string()),
            error: None,
        }];
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: vec!["leader_5=0xd8c5228c515db3043dfa0c8cd6f22450ee9a99b0".to_string()],
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: vec!["xyz_perp".to_string()],
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: temp.join("snapshot.json"),
            shadow_history_path: shadow_path,
            leader_notional_usd: 1750.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: true,
            submit: true,
            hold_positions_after_submit: true,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 600,
            max_events: 20_000,
            max_live_orders: 1,
            max_total_notional_usd: 3000.0,
            max_total_fees_usd: 1.0,
            max_slippage_bps: 50.0,
            environment: None,
            ws_url: None,
        };

        let recovered = super::copy_live_daemon_recover_open_ledger_from_live_positions(
            snapshot,
            &reconciliations,
            &options,
        )
        .expect("recover ledger");

        assert_eq!(recovered.ledger_entries.len(), 1);
        let entry = &recovered.ledger_entries[0];
        assert_eq!(entry.signal_id, "sig-gold");
        assert_eq!(entry.leader_id, "leader_5");
        assert_eq!(entry.coin, "xyz:GOLD");
        assert_eq!(entry.local_side, crate::domain::OrderSide::Buy);
        assert_eq!(
            entry.status,
            crate::strategies::smart_money::CopyLedgerStatus::Open
        );
        assert!((entry.remaining_notional_usd - 349.996).abs() < 1e-9);
    }

    #[test]
    fn copy_live_daemon_recovers_open_ledger_when_only_pending_reduce_exists() {
        let temp = std::env::temp_dir().join(format!(
            "trade_xyz_copy_recover_pending_reduce_{}",
            now_ms()
        ));
        std::fs::create_dir_all(&temp).expect("test dir");
        let shadow_path = temp.join("shadow.jsonl");
        let shadow = crate::strategies::smart_money::CopyShadowHistoryEntry {
            schema_version: 1,
            occurred_at_ms: 3_000,
            status: "would_copy".to_string(),
            leader_id: "leader_4".to_string(),
            leader_address: "0x9dead8fffcbf130e7658f672d2c081d91178d617".to_string(),
            coin: "xyz:JPY".to_string(),
            action_kind: "IncreaseLong".to_string(),
            action_event_id: "evt-jpy-open-after-reduce".to_string(),
            live_gate: "live_allowed".to_string(),
            risk_reject_reason: None,
            signal_id: Some("sig-jpy-live-open".to_string()),
            side: Some(crate::domain::OrderSide::Buy),
            reduce_only: Some(false),
            notional_usd: Some(82.41),
            ledger_status: Some(crate::strategies::smart_money::CopyLedgerStatus::PendingOpen),
            local_account_id: Some("addr_b".to_string()),
            ..Default::default()
        };
        std::fs::write(
            &shadow_path,
            format!("{}\n", serde_json::to_string(&shadow).expect("shadow json")),
        )
        .expect("shadow file");

        let pending_reduce = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_b".to_string(),
            leader_id: "leader_4".to_string(),
            leader_group: "leader_4".to_string(),
            signal_id: "sig-jpy-old-reduce-close-123".to_string(),
            coin: "xyz:JPY".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: None,
            order_oid: None,
            submitted_at_ms: Some(2_000),
            filled_at_ms: None,
            planned_notional_usd: 85.6,
            pending_notional_usd: 85.6,
            filled_notional_usd: 0.0,
            remaining_notional_usd: 85.6,
            status: crate::strategies::smart_money::CopyLedgerStatus::PendingReduce,
        };
        let snapshot = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![pending_reduce],
        };
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_b".to_string(),
            ok: false,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: vec![super::CopyBoundedLiveWindowPositionSummary {
                coin: "xyz:JPY".to_string(),
                szi: "0.51".to_string(),
                position_value: Some("82.4109".to_string()),
                unrealized_pnl: None,
            }],
            account_value: None,
            withdrawable: None,
            total_ntl_pos: Some("82.4109".to_string()),
            total_margin_used: Some("8.24".to_string()),
            error: None,
        }];
        let mut options = follow_position_options();
        options.account_ids = vec!["addr_a".to_string(), "addr_b".to_string()];
        options.shadow_history_path = shadow_path;

        let recovered = super::copy_live_daemon_recover_open_ledger_from_live_positions(
            snapshot,
            &reconciliations,
            &options,
        )
        .expect("recover ledger");

        assert!(recovered.ledger_entries.iter().any(|entry| {
            entry.local_account_id == "addr_b"
                && entry.coin == "xyz:JPY"
                && entry.signal_id == "sig-jpy-live-open"
                && entry.status == crate::strategies::smart_money::CopyLedgerStatus::Open
                && (entry.remaining_notional_usd - 82.4109).abs() < 1e-9
        }));
    }

    #[test]
    fn copy_live_daemon_recovers_uncovered_residual_when_partial_open_mapping_exists() {
        let temp = std::env::temp_dir().join(format!(
            "trade_xyz_copy_recover_partial_mapping_{}",
            now_ms()
        ));
        std::fs::create_dir_all(&temp).expect("test dir");
        let shadow_path = temp.join("shadow.jsonl");
        let shadow = crate::strategies::smart_money::CopyShadowHistoryEntry {
            schema_version: 1,
            occurred_at_ms: 3_000,
            status: "would_copy".to_string(),
            leader_id: "leader_4".to_string(),
            leader_address: "0x9dead8fffcbf130e7658f672d2c081d91178d617".to_string(),
            coin: "xyz:SP500".to_string(),
            action_kind: "IncreaseLong".to_string(),
            action_event_id: "evt-sp500-open-after-partial".to_string(),
            live_gate: "live_allowed".to_string(),
            risk_reject_reason: None,
            signal_id: Some("sig-sp500-live-open".to_string()),
            side: Some(crate::domain::OrderSide::Buy),
            reduce_only: Some(false),
            notional_usd: Some(59.7248),
            ledger_status: Some(crate::strategies::smart_money::CopyLedgerStatus::PendingOpen),
            local_account_id: Some("addr_b".to_string()),
            ..Default::default()
        };
        std::fs::write(
            &shadow_path,
            format!("{}\n", serde_json::to_string(&shadow).expect("shadow json")),
        )
        .expect("shadow file");

        let active_residual = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_b".to_string(),
            leader_id: "leader_4".to_string(),
            leader_group: "leader_4".to_string(),
            signal_id: "sig-sp500-active-residual".to_string(),
            coin: "xyz:SP500".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: Some("aaaaaaaa-aaaa-5aaa-8aaa-aaaaaaaaaaaa".to_string()),
            order_oid: Some(123456789),
            submitted_at_ms: Some(2_000),
            filled_at_ms: Some(2_000),
            planned_notional_usd: 44.6292,
            pending_notional_usd: 0.0,
            filled_notional_usd: 37.191,
            remaining_notional_usd: 7.3786,
            status: crate::strategies::smart_money::CopyLedgerStatus::Open,
        };
        let snapshot = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![active_residual],
        };
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_b".to_string(),
            ok: false,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: vec![super::CopyBoundedLiveWindowPositionSummary {
                coin: "xyz:SP500".to_string(),
                szi: "0.008".to_string(),
                position_value: Some("59.7248".to_string()),
                unrealized_pnl: None,
            }],
            account_value: None,
            withdrawable: None,
            total_ntl_pos: Some("59.7248".to_string()),
            total_margin_used: Some("6.0".to_string()),
            error: None,
        }];
        let mut options = follow_position_options();
        options.account_ids = vec!["addr_a".to_string(), "addr_b".to_string()];
        options.shadow_history_path = shadow_path;

        let recovered = super::copy_live_daemon_recover_open_ledger_from_live_positions(
            snapshot,
            &reconciliations,
            &options,
        )
        .expect("recover ledger");

        assert_eq!(recovered.ledger_entries.len(), 2);
        let recovered_entry = recovered
            .ledger_entries
            .iter()
            .find(|entry| entry.signal_id == "sig-sp500-live-open")
            .expect("recovered residual entry");
        assert_eq!(
            recovered_entry.status,
            crate::strategies::smart_money::CopyLedgerStatus::Open
        );
        assert!((recovered_entry.remaining_notional_usd - 52.3462).abs() < 1e-6);

        let mapped = super::copy_live_daemon_active_open_mapping_notional_for_key(
            &recovered,
            "addr_b",
            "xyz:SP500",
            crate::domain::OrderSide::Buy,
        );
        assert!((mapped - 59.7248).abs() < 1e-6);
        assert!(
            super::copy_live_daemon_unmapped_position_keys(&recovered, &reconciliations).is_empty()
        );
    }

    #[test]
    fn copy_live_daemon_recovers_open_ledger_for_all_selected_accounts() {
        let temp =
            std::env::temp_dir().join(format!("trade_xyz_copy_recover_multi_account_{}", now_ms()));
        std::fs::create_dir_all(&temp).expect("test dir");
        let shadow_path = temp.join("shadow.jsonl");
        let shadows = [
            crate::strategies::smart_money::CopyShadowHistoryEntry {
                schema_version: 1,
                occurred_at_ms: 12345,
                status: "would_copy".to_string(),
                leader_id: "leader_4".to_string(),
                leader_address: "0x9dead8fffcbf130e7658f672d2c081d91178d617".to_string(),
                coin: "xyz:SP500".to_string(),
                action_kind: "IncreaseLong".to_string(),
                action_event_id: "evt-sp500".to_string(),
                live_gate: "live_allowed".to_string(),
                risk_reject_reason: None,
                signal_id: Some("sig-sp500".to_string()),
                side: Some(crate::domain::OrderSide::Buy),
                reduce_only: Some(false),
                notional_usd: Some(110.0),
                ledger_status: Some(crate::strategies::smart_money::CopyLedgerStatus::PendingOpen),
                ..Default::default()
            },
            crate::strategies::smart_money::CopyShadowHistoryEntry {
                schema_version: 1,
                occurred_at_ms: 12346,
                status: "would_copy".to_string(),
                leader_id: "leader_5".to_string(),
                leader_address: "0xd8c5228c515db3043dfa0c8cd6f22450ee9a99b0".to_string(),
                coin: "xyz:GOLD".to_string(),
                action_kind: "IncreaseLong".to_string(),
                action_event_id: "evt-gold".to_string(),
                live_gate: "live_allowed".to_string(),
                risk_reject_reason: None,
                signal_id: Some("sig-gold".to_string()),
                side: Some(crate::domain::OrderSide::Buy),
                reduce_only: Some(false),
                notional_usd: Some(350.0),
                ledger_status: Some(crate::strategies::smart_money::CopyLedgerStatus::PendingOpen),
                ..Default::default()
            },
        ];
        let body = shadows
            .iter()
            .map(|entry| serde_json::to_string(entry).expect("shadow json"))
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&shadow_path, format!("{body}\n")).expect("shadow file");

        let snapshot = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: Vec::new(),
        };
        let reconciliations = vec![
            CopyBoundedLiveWindowReconcile {
                account_id: "addr_a".to_string(),
                ok: false,
                open_order_count: Some(0),
                asset_positions: Some(1),
                position_summaries: vec![super::CopyBoundedLiveWindowPositionSummary {
                    coin: "xyz:GOLD".to_string(),
                    szi: "0.085".to_string(),
                    position_value: Some("349.996".to_string()),
                    unrealized_pnl: None,
                }],
                account_value: None,
                withdrawable: None,
                total_ntl_pos: Some("349.996".to_string()),
                total_margin_used: Some("35.0".to_string()),
                error: None,
            },
            CopyBoundedLiveWindowReconcile {
                account_id: "addr_b".to_string(),
                ok: false,
                open_order_count: Some(0),
                asset_positions: Some(1),
                position_summaries: vec![super::CopyBoundedLiveWindowPositionSummary {
                    coin: "xyz:SP500".to_string(),
                    szi: "0.015".to_string(),
                    position_value: Some("110.748".to_string()),
                    unrealized_pnl: None,
                }],
                account_value: None,
                withdrawable: None,
                total_ntl_pos: Some("110.748".to_string()),
                total_margin_used: Some("11.0".to_string()),
                error: None,
            },
        ];
        let mut options = follow_position_options();
        options.account_ids = vec!["addr_a".to_string(), "addr_b".to_string()];
        options.local_account_id = Some("addr_a".to_string());
        options.shadow_history_path = shadow_path;

        let recovered = super::copy_live_daemon_recover_open_ledger_from_live_positions(
            snapshot,
            &reconciliations,
            &options,
        )
        .expect("recover ledger");

        assert_eq!(recovered.ledger_entries.len(), 2);
        assert!(
            recovered
                .ledger_entries
                .iter()
                .any(|entry| { entry.local_account_id == "addr_a" && entry.coin == "xyz:GOLD" })
        );
        assert!(
            recovered
                .ledger_entries
                .iter()
                .any(|entry| { entry.local_account_id == "addr_b" && entry.coin == "xyz:SP500" })
        );
    }

    #[test]
    fn copy_live_daemon_follow_position_health_fails_on_unmapped_live_position() {
        let mut options = follow_position_options();
        options.max_total_notional_usd = 1_000.0;
        let snapshot = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: Vec::new(),
        };
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: false,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: vec![super::CopyBoundedLiveWindowPositionSummary {
                coin: "xyz:GOLD".to_string(),
                szi: "0.085".to_string(),
                position_value: Some("349.996".to_string()),
                unrealized_pnl: None,
            }],
            account_value: Some("158.0".to_string()),
            withdrawable: Some("0.2".to_string()),
            total_ntl_pos: Some("349.996".to_string()),
            total_margin_used: Some("35.0".to_string()),
            error: None,
        }];

        assert!(
            !super::copy_live_daemon_reconciliations_healthy_for_snapshot(
                &options,
                &reconciliations,
                &snapshot
            )
        );
        let detail = super::copy_live_daemon_reconcile_health_detail_for_snapshot(
            &options,
            &reconciliations,
            &snapshot,
        );
        assert!(detail.contains("unmanaged live position"));
        assert!(detail.contains("addr_a:xyz:GOLD:buy"));
    }

    #[test]
    fn copy_live_daemon_follow_position_health_accepts_same_coin_lineage_residual() {
        let mut options = follow_position_options();
        options.max_total_notional_usd = 1_000.0;
        let active_residual = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_b".to_string(),
            leader_id: "leader_4".to_string(),
            leader_group: "leader_4".to_string(),
            signal_id: "sig-sp500-active-residual".to_string(),
            coin: "xyz:SP500".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: Some("bbbbbbbb-bbbb-5bbb-8bbb-bbbbbbbbbbbb".to_string()),
            order_oid: Some(22334455),
            submitted_at_ms: Some(2_000),
            filled_at_ms: Some(2_000),
            planned_notional_usd: 44.6292,
            pending_notional_usd: 0.0,
            filled_notional_usd: 37.191,
            remaining_notional_usd: 7.3786,
            status: crate::strategies::smart_money::CopyLedgerStatus::Open,
        };
        let historical_open = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_b".to_string(),
            leader_id: "leader_5".to_string(),
            leader_group: "leader_5".to_string(),
            signal_id: "copy-leader_5-sp500-older-open-123".to_string(),
            coin: "xyz:SP500".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: Some("cccccccc-cccc-5ccc-8ccc-cccccccccccc".to_string()),
            order_oid: Some(66778899),
            submitted_at_ms: Some(1_000),
            filled_at_ms: Some(1_000),
            planned_notional_usd: 52.4014,
            pending_notional_usd: 0.0,
            filled_notional_usd: 52.4014,
            remaining_notional_usd: 0.0,
            status: crate::strategies::smart_money::CopyLedgerStatus::Closed,
        };
        let snapshot = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![active_residual, historical_open],
        };
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_b".to_string(),
            ok: false,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: vec![super::CopyBoundedLiveWindowPositionSummary {
                coin: "xyz:SP500".to_string(),
                szi: "0.008".to_string(),
                position_value: Some("59.7800".to_string()),
                unrealized_pnl: Some("0.245667".to_string()),
            }],
            account_value: Some("47.385558".to_string()),
            withdrawable: Some("30.712313".to_string()),
            total_ntl_pos: Some("59.7800".to_string()),
            total_margin_used: Some("5.97".to_string()),
            error: None,
        }];

        assert!(
            super::copy_live_daemon_unmapped_position_keys(&snapshot, &reconciliations).is_empty()
        );
        assert!(
            super::copy_live_daemon_reconciliations_healthy_for_snapshot(
                &options,
                &reconciliations,
                &snapshot
            )
        );
        let detail = super::copy_live_daemon_reconcile_health_detail_for_snapshot(
            &options,
            &reconciliations,
            &snapshot,
        );
        assert!(!detail.contains("unmanaged live position"));
    }

    #[test]
    fn copy_live_daemon_follow_position_health_rejects_unsubmitted_qnt_shadow_tail() {
        let mut options = follow_position_options();
        options.max_total_notional_usd = 3_000.0;
        let snapshot = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: vec!["leader-semantic:leader_4:xyz:QNT:open".to_string()],
            ledger_entries: vec![crate::strategies::smart_money::CopyLedgerEntry {
                local_account_id: "addr_a".to_string(),
                leader_id: "leader_4".to_string(),
                leader_group: "leader_4".to_string(),
                signal_id: "copy-leader_4-qnt-open".to_string(),
                coin: "xyz:QNT".to_string(),
                local_side: crate::domain::OrderSide::Sell,
                order_cloid: None,
                order_oid: None,
                submitted_at_ms: None,
                filled_at_ms: None,
                planned_notional_usd: 350.0,
                pending_notional_usd: 350.0,
                filled_notional_usd: 0.0,
                remaining_notional_usd: 0.0,
                status: crate::strategies::smart_money::CopyLedgerStatus::PendingOpen,
            }],
        };
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: false,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: vec![super::CopyBoundedLiveWindowPositionSummary {
                coin: "xyz:QNT".to_string(),
                szi: "-4.9".to_string(),
                position_value: Some("365.6282".to_string()),
                unrealized_pnl: None,
            }],
            account_value: Some("158.0".to_string()),
            withdrawable: Some("0.2".to_string()),
            total_ntl_pos: Some("365.6282".to_string()),
            total_margin_used: Some("36.6".to_string()),
            error: None,
        }];

        assert!(
            !super::copy_live_daemon_reconciliations_healthy_for_snapshot(
                &options,
                &reconciliations,
                &snapshot
            )
        );
        let detail = super::copy_live_daemon_reconcile_health_detail_for_snapshot(
            &options,
            &reconciliations,
            &snapshot,
        );
        assert!(detail.contains("addr_a:xyz:QNT:sell"));
        assert!(detail.contains("uncovered=365.628200"));
    }

    #[test]
    fn copy_live_daemon_follow_position_health_accepts_evidenced_copy_lineage_residual() {
        let mut options = follow_position_options();
        options.max_total_notional_usd = 3_000.0;
        let active_sp500 = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_a".to_string(),
            leader_id: "leader_4".to_string(),
            leader_group: "leader_4".to_string(),
            signal_id: "copy-leader_4-sp500-open-active".to_string(),
            coin: "xyz:SP500".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: Some("11111111-1111-5111-8111-111111111111".to_string()),
            order_oid: Some(9001),
            submitted_at_ms: Some(now_ms()),
            filled_at_ms: Some(now_ms()),
            planned_notional_usd: 500.67,
            pending_notional_usd: 0.0,
            filled_notional_usd: 500.67,
            remaining_notional_usd: 500.67,
            status: crate::strategies::smart_money::CopyLedgerStatus::Open,
        };
        let closed_open_sp500 = crate::strategies::smart_money::CopyLedgerEntry {
            signal_id: "copy-leader_4-sp500-open-closed".to_string(),
            order_cloid: Some("22222222-2222-5222-8222-222222222222".to_string()),
            order_oid: Some(9002),
            submitted_at_ms: Some(now_ms()),
            filled_at_ms: Some(now_ms()),
            planned_notional_usd: 149.33,
            filled_notional_usd: 149.33,
            remaining_notional_usd: 0.0,
            status: crate::strategies::smart_money::CopyLedgerStatus::Closed,
            ..active_sp500.clone()
        };
        let snapshot = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![active_sp500, closed_open_sp500],
        };
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: false,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: vec![super::CopyBoundedLiveWindowPositionSummary {
                coin: "xyz:SP500".to_string(),
                szi: "0.081".to_string(),
                position_value: Some("594.10".to_string()),
                unrealized_pnl: Some("93.43".to_string()),
            }],
            account_value: Some("158.0".to_string()),
            withdrawable: Some("0.2".to_string()),
            total_ntl_pos: Some("594.10".to_string()),
            total_margin_used: Some("59.4".to_string()),
            error: None,
        }];

        assert!(
            super::copy_live_daemon_reconciliations_healthy_for_snapshot(
                &options,
                &reconciliations,
                &snapshot
            )
        );
    }

    #[test]
    fn copy_live_daemon_follow_position_health_tolerates_small_mark_drift_above_account_cap() {
        let mut options = follow_position_options();
        options.max_total_notional_usd = 700.0;
        let active_purrdat = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_b".to_string(),
            leader_id: "leader_4".to_string(),
            leader_group: "leader_4".to_string(),
            signal_id: "copy-leader_4-purrdat-open-active".to_string(),
            coin: "xyz:PURRDAT".to_string(),
            local_side: crate::domain::OrderSide::Sell,
            order_cloid: Some("33333333-3333-5333-8333-333333333333".to_string()),
            order_oid: Some(9003),
            submitted_at_ms: Some(now_ms()),
            filled_at_ms: Some(now_ms()),
            planned_notional_usd: 349.08,
            pending_notional_usd: 0.0,
            filled_notional_usd: 349.08,
            remaining_notional_usd: 349.08,
            status: crate::strategies::smart_money::CopyLedgerStatus::Open,
        };
        let snapshot = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![active_purrdat],
        };
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_b".to_string(),
            ok: false,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: vec![super::CopyBoundedLiveWindowPositionSummary {
                coin: "xyz:PURRDAT".to_string(),
                szi: "-40.0".to_string(),
                position_value: Some("351.612".to_string()),
                unrealized_pnl: None,
            }],
            account_value: Some("71.33".to_string()),
            withdrawable: Some("43.08".to_string()),
            total_ntl_pos: Some("351.612".to_string()),
            total_margin_used: Some("28.25".to_string()),
            error: None,
        }];
        let account_caps = HashMap::from([("addr_b".to_string(), 350.0)]);

        assert!(
            super::copy_live_daemon_reconciliations_healthy_for_mode_with_account_caps(
                &options,
                &reconciliations,
                &account_caps,
            )
        );
        assert!(
            super::copy_live_daemon_unmapped_position_keys(&snapshot, &reconciliations).is_empty()
        );
        let detail = super::copy_live_daemon_reconcile_health_detail_for_snapshot_with_account_caps(
            &options,
            &reconciliations,
            &snapshot,
            &account_caps,
        );
        assert!(detail.contains("addr_b<= 350.000000 (health<= 360.000000)"));
    }

    #[test]
    fn copy_live_daemon_follow_position_health_tolerates_pnl_drift_above_account_health_cap_for_mapped_copy()
     {
        let mut options = follow_position_options();
        options.max_total_notional_usd = 700.0;
        let active_spcx = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_b".to_string(),
            leader_id: "leader_4".to_string(),
            leader_group: "leader_4".to_string(),
            signal_id: "copy-leader_4-spcx-open-active".to_string(),
            coin: "xyz:SPCX".to_string(),
            local_side: crate::domain::OrderSide::Sell,
            order_cloid: Some("44444444-4444-5444-8444-444444444444".to_string()),
            order_oid: Some(9004),
            submitted_at_ms: Some(now_ms()),
            filled_at_ms: Some(now_ms()),
            planned_notional_usd: 350.0,
            pending_notional_usd: 0.0,
            filled_notional_usd: 349.9875,
            remaining_notional_usd: 349.9875,
            status: crate::strategies::smart_money::CopyLedgerStatus::Open,
        };
        let snapshot = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![active_spcx],
        };
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_b".to_string(),
            ok: false,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: vec![super::CopyBoundedLiveWindowPositionSummary {
                coin: "xyz:SPCX".to_string(),
                szi: "-2.25".to_string(),
                position_value: Some("368.1225".to_string()),
                unrealized_pnl: Some("-18.1350".to_string()),
            }],
            account_value: Some("61.37".to_string()),
            withdrawable: Some("43.97".to_string()),
            total_ntl_pos: Some("368.1225".to_string()),
            total_margin_used: Some("17.40".to_string()),
            error: None,
        }];
        let account_caps = HashMap::from([("addr_b".to_string(), 350.0)]);

        assert!(
            !super::copy_live_daemon_reconciliations_healthy_for_mode_with_account_caps(
                &options,
                &reconciliations,
                &account_caps,
            )
        );
        assert!(
            super::copy_live_daemon_reconciliations_healthy_for_snapshot_with_account_caps(
                &options,
                &reconciliations,
                &snapshot,
                &account_caps,
            )
        );
        assert!(
            super::copy_live_daemon_unmapped_position_keys(&snapshot, &reconciliations).is_empty()
        );
        let detail = super::copy_live_daemon_reconcile_health_detail_for_snapshot_with_account_caps(
            &options,
            &reconciliations,
            &snapshot,
            &account_caps,
        );
        assert!(detail.contains("1/1 account(s) healthy"));
        assert!(detail.contains("mapped_copy_principal=349.987500"));
    }

    #[test]
    fn copy_live_daemon_submit_plan_contract_tolerates_snapshot_pnl_drift_after_all_opens_suppressed()
     {
        let mut options = follow_position_options();
        options.max_total_notional_usd = 700.0;
        let active_spcx = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_b".to_string(),
            leader_id: "leader_4".to_string(),
            leader_group: "leader_4".to_string(),
            signal_id: "copy-leader_4-spcx-open-active".to_string(),
            coin: "xyz:SPCX".to_string(),
            local_side: crate::domain::OrderSide::Sell,
            order_cloid: Some("66666666-6666-5666-8666-666666666666".to_string()),
            order_oid: Some(9006),
            submitted_at_ms: Some(now_ms()),
            filled_at_ms: Some(now_ms()),
            planned_notional_usd: 350.0,
            pending_notional_usd: 0.0,
            filled_notional_usd: 349.9875,
            remaining_notional_usd: 349.9875,
            status: crate::strategies::smart_money::CopyLedgerStatus::Open,
        };
        let snapshot = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![active_spcx],
        };
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_b".to_string(),
            ok: false,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: vec![super::CopyBoundedLiveWindowPositionSummary {
                coin: "xyz:SPCX".to_string(),
                szi: "-2.25".to_string(),
                position_value: Some("368.1225".to_string()),
                unrealized_pnl: Some("-18.1350".to_string()),
            }],
            account_value: Some("61.37".to_string()),
            withdrawable: Some("43.97".to_string()),
            total_ntl_pos: Some("368.1225".to_string()),
            total_margin_used: Some("17.40".to_string()),
            error: None,
        }];
        let account_caps = HashMap::from([("addr_b".to_string(), 350.0)]);

        let old_contract = super::copy_live_daemon_submit_plan_contract_with_account_caps(
            &options,
            &[],
            &[],
            0.0,
            0.0,
            &reconciliations,
            &account_caps,
        );
        assert!(!old_contract.ok, "{old_contract:#?}");

        let snapshot_contract =
            super::copy_live_daemon_submit_plan_contract_with_snapshot_and_account_caps(
                &options,
                &[],
                &[],
                0.0,
                0.0,
                &reconciliations,
                &snapshot,
                &account_caps,
            );

        assert!(snapshot_contract.ok, "{snapshot_contract:#?}");
        assert!(
            snapshot_contract
                .checks
                .iter()
                .any(|check| check.name == "pre_submit_reconcile_health"
                    && check.ok
                    && check.detail.contains("mapped_copy_principal=349.987500")),
            "{snapshot_contract:#?}"
        );
    }

    #[test]
    fn copy_live_daemon_follow_position_health_rejects_large_account_cap_breach() {
        let mut options = follow_position_options();
        options.max_total_notional_usd = 700.0;
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_b".to_string(),
            ok: false,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: vec![super::CopyBoundedLiveWindowPositionSummary {
                coin: "xyz:PURRDAT".to_string(),
                szi: "-40.0".to_string(),
                position_value: Some("371.0".to_string()),
                unrealized_pnl: None,
            }],
            account_value: Some("71.33".to_string()),
            withdrawable: Some("43.08".to_string()),
            total_ntl_pos: Some("371.0".to_string()),
            total_margin_used: Some("28.25".to_string()),
            error: None,
        }];
        let account_caps = HashMap::from([("addr_b".to_string(), 350.0)]);

        assert!(
            !super::copy_live_daemon_reconciliations_healthy_for_mode_with_account_caps(
                &options,
                &reconciliations,
                &account_caps,
            )
        );
    }

    #[test]
    fn copy_live_daemon_follow_position_health_rejects_snapshot_principal_above_account_health_cap()
    {
        let mut options = follow_position_options();
        options.max_total_notional_usd = 700.0;
        let active_purrdat = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_b".to_string(),
            leader_id: "leader_4".to_string(),
            leader_group: "leader_4".to_string(),
            signal_id: "copy-leader_4-purrdat-open-oversized".to_string(),
            coin: "xyz:PURRDAT".to_string(),
            local_side: crate::domain::OrderSide::Sell,
            order_cloid: Some("55555555-5555-5555-8555-555555555555".to_string()),
            order_oid: Some(9005),
            submitted_at_ms: Some(now_ms()),
            filled_at_ms: Some(now_ms()),
            planned_notional_usd: 371.0,
            pending_notional_usd: 0.0,
            filled_notional_usd: 371.0,
            remaining_notional_usd: 371.0,
            status: crate::strategies::smart_money::CopyLedgerStatus::Open,
        };
        let snapshot = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: vec![active_purrdat],
        };
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_b".to_string(),
            ok: false,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: vec![super::CopyBoundedLiveWindowPositionSummary {
                coin: "xyz:PURRDAT".to_string(),
                szi: "-40.0".to_string(),
                position_value: Some("371.0".to_string()),
                unrealized_pnl: Some("0.0".to_string()),
            }],
            account_value: Some("71.33".to_string()),
            withdrawable: Some("43.08".to_string()),
            total_ntl_pos: Some("371.0".to_string()),
            total_margin_used: Some("28.25".to_string()),
            error: None,
        }];
        let account_caps = HashMap::from([("addr_b".to_string(), 350.0)]);

        assert!(
            !super::copy_live_daemon_reconciliations_healthy_for_snapshot_with_account_caps(
                &options,
                &reconciliations,
                &snapshot,
                &account_caps,
            )
        );
    }

    #[test]
    fn copy_live_daemon_recovers_open_ledger_from_historical_shadow_files() {
        let temp = std::env::temp_dir().join(format!(
            "trade_xyz_copy_recover_historical_shadow_{}",
            now_ms()
        ));
        std::fs::create_dir_all(&temp).expect("test dir");
        let current_shadow_path = temp.join("persistent-live-soak-current-shadow.jsonl");
        std::fs::write(&current_shadow_path, "").expect("current shadow");
        let historical_shadow_path = temp.join("persistent-live-soak-20260624-075143-shadow.jsonl");
        let shadow = crate::strategies::smart_money::CopyShadowHistoryEntry {
            schema_version: 1,
            occurred_at_ms: 12345,
            status: "would_copy".to_string(),
            leader_id: "leader_5".to_string(),
            leader_address: "0xd8c5228c515db3043dfa0c8cd6f22450ee9a99b0".to_string(),
            local_account_id: Some("addr_a".to_string()),
            coin: "xyz:GOLD".to_string(),
            action_kind: "IncreaseLong".to_string(),
            action_event_id: "evt-gold".to_string(),
            live_gate: "live_allowed".to_string(),
            risk_reject_reason: None,
            signal_id: Some("sig-gold".to_string()),
            side: Some(crate::domain::OrderSide::Buy),
            reduce_only: Some(false),
            notional_usd: Some(350.0),
            ledger_status: Some(crate::strategies::smart_money::CopyLedgerStatus::PendingOpen),
            ..Default::default()
        };
        std::fs::write(
            &historical_shadow_path,
            format!("{}\n", serde_json::to_string(&shadow).expect("shadow json")),
        )
        .expect("historical shadow");
        let snapshot = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: Vec::new(),
        };
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: false,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: vec![super::CopyBoundedLiveWindowPositionSummary {
                coin: "xyz:GOLD".to_string(),
                szi: "0.085".to_string(),
                position_value: Some("347.582".to_string()),
                unrealized_pnl: None,
            }],
            account_value: None,
            withdrawable: None,
            total_ntl_pos: Some("347.582".to_string()),
            total_margin_used: Some("35.0".to_string()),
            error: None,
        }];
        let mut options = follow_position_options();
        options.account_ids = vec!["addr_a".to_string(), "addr_b".to_string()];
        options.shadow_history_path = current_shadow_path;

        let recovered = super::copy_live_daemon_recover_open_ledger_from_live_positions(
            snapshot,
            &reconciliations,
            &options,
        )
        .expect("recover ledger");

        assert_eq!(recovered.ledger_entries.len(), 1);
        assert_eq!(recovered.ledger_entries[0].local_account_id, "addr_a");
        assert_eq!(recovered.ledger_entries[0].coin, "xyz:GOLD");
        assert_eq!(recovered.ledger_entries[0].remaining_notional_usd, 347.582);
    }

    #[test]
    fn copy_live_daemon_follow_position_health_ignores_unmapped_dust() {
        let mut options = follow_position_options();
        options.max_total_notional_usd = 1_000.0;
        let snapshot = crate::strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: now_ms(),
            seen_event_keys: Vec::new(),
            ledger_entries: Vec::new(),
        };
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: false,
            open_order_count: Some(0),
            asset_positions: Some(1),
            position_summaries: vec![super::CopyBoundedLiveWindowPositionSummary {
                coin: "xyz:BE".to_string(),
                szi: "-0.01".to_string(),
                position_value: Some("3.2677".to_string()),
                unrealized_pnl: None,
            }],
            account_value: Some("158.0".to_string()),
            withdrawable: Some("0.2".to_string()),
            total_ntl_pos: Some("3.2677".to_string()),
            total_margin_used: Some("0.3".to_string()),
            error: None,
        }];

        assert!(
            super::copy_live_daemon_reconciliations_healthy_for_snapshot(
                &options,
                &reconciliations,
                &snapshot
            )
        );
        let detail = super::copy_live_daemon_reconcile_health_detail_for_snapshot(
            &options,
            &reconciliations,
            &snapshot,
        );
        assert!(!detail.contains("unmanaged live position"));
    }

    #[test]
    fn copy_live_daemon_snapshot_save_allows_evidenced_submit_even_if_health_false() {
        let cloid = "88888888-8888-5888-8888-888888888888".to_string();
        let submitted = crate::domain::OrderSubmitted {
            signal_id: "sig-evidenced-submit".to_string(),
            intent_id: "intent-evidenced-submit".to_string(),
            worker_id: "worker-addr_a".to_string(),
            account_id: "addr_a".to_string(),
            cloid: cloid.clone(),
            coin: "xyz:GOLD".to_string(),
            side: crate::domain::OrderSide::Sell,
            notional_usd: 50.0,
            submitted_price: Some(4380.0),
            submitted_size: Some(0.0114),
            exchange_status: Some("filled".to_string()),
            oid: Some(8001),
            filled_size: Some(0.0114),
            avg_fill_price: Some(4380.0),
            dry_run: false,
            submitted_at_ms: now_ms(),
        };
        let report = CopyLiveDaemonPersistentLiveSubmitReport {
            ok: false,
            mode: "persistent_live_submit".to_string(),
            submit_requested: true,
            submit_plan_contract_ok: true,
            submitted_reports: vec![crate::domain::WorkerReport::Submitted(submitted)],
            order_evidence: vec![CopyExecutionCanaryOrderEvidence {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                signal_id: "sig-evidenced-submit".to_string(),
                coin: "xyz:GOLD".to_string(),
                oid: Some(8001),
                cloid: cloid.clone(),
                order_status: Some(crate::hyperliquid::OrderStatusResponse {
                    status: "order".to_string(),
                    order: Some(crate::hyperliquid::OrderStatusInfo {
                        order: crate::hyperliquid::OrderStatusOrder {
                            coin: "xyz:GOLD".to_string(),
                            side: "A".to_string(),
                            limit_px: "4380.0".to_string(),
                            sz: "0.0".to_string(),
                            oid: 8001,
                            timestamp: now_ms(),
                            trigger_condition: "N/A".to_string(),
                            is_trigger: false,
                            trigger_px: "0.0".to_string(),
                            children: Vec::new(),
                            is_position_tpsl: false,
                            reduce_only: false,
                            order_type: "Limit".to_string(),
                            orig_sz: "0.0114".to_string(),
                            tif: "Ioc".to_string(),
                            cloid: Some("0x".to_string() + &cloid.replace('-', "")),
                        },
                        status: "filled".to_string(),
                        status_timestamp: now_ms(),
                    }),
                }),
                user_fill_count: 1,
                matching_fill_count: 1,
                matching_fills: Vec::new(),
                error: None,
            }],
            cleanup_runbooks: Vec::new(),
            cleanup_errors: Vec::new(),
            ledger_reconciliations: Vec::new(),
            ledger_reconciliation_snapshot:
                crate::strategies::smart_money::CopyPersistenceSnapshot {
                    schema_version: 1,
                    saved_at_ms: now_ms(),
                    seen_event_keys: Vec::new(),
                    ledger_entries: Vec::new(),
                },
            checks: vec![copy_shadow_smoke_check(
                "ledger_reconciliation",
                false,
                "duplicate replay was unhealthy but order evidence is complete",
            )],
        };

        assert!(copy_live_daemon_persistent_submit_snapshot_safe_to_save(
            &report
        ));
    }

    #[test]
    fn copy_live_daemon_saves_progress_snapshot_immediately_after_submit() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18053")?;
        let mut config = crate::config::load_config(std::path::Path::new(&config_path))?;
        config.app.dry_run = false;
        config.manual_ops.manual_live_enabled = true;
        let account = config
            .account("addr_a")
            .context("addr_a should exist in test config")?;
        let leader = CopyShadowSmokeLeader {
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
        };
        let canary_options = CopyExecutionCanaryOptions {
            leaders: vec!["leader_a=0x00000000000000000000000000000000000000aa".to_string()],
            account_ids: vec!["addr_a".to_string()],
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            local_account_id: None,
            shadow_history_path: std::env::temp_dir()
                .join(format!("unused-copy-progress-{}.jsonl", now_ms())),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live: true,
            allow_live_submit: true,
            confirm_mainnet_live: false,
            cleanup_after_submit: true,
            cleanup_max_slippage_bps: 50.0,
            preflight_only: false,
            max_orders: 1,
        };
        let records = build_synthetic_copy_shadow_records(
            &config,
            &canary_options,
            account,
            &leader,
            &["addr_a".to_string()],
        );
        let signal = records
            .iter()
            .find_map(|record| record.signal.as_ref())
            .context("synthetic canary should emit one copy signal")?;
        let cloid = "99999999-9999-5999-8999-999999999999".to_string();
        let submitted_at_ms = now_ms();
        let submitted = crate::domain::WorkerReport::Submitted(crate::domain::OrderSubmitted {
            signal_id: signal.signal_id.clone(),
            intent_id: "intent-progress".to_string(),
            worker_id: "worker-addr_a".to_string(),
            account_id: "addr_a".to_string(),
            cloid: cloid.clone(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            notional_usd: 50.0,
            submitted_price: Some(100.0),
            submitted_size: Some(0.5),
            exchange_status: Some("filled".to_string()),
            oid: Some(9001),
            filled_size: Some(0.5),
            avg_fill_price: Some(100.0),
            dry_run: false,
            submitted_at_ms,
        });
        let persistence_path = std::env::temp_dir().join(format!(
            "copy-live-progress-snapshot-{}.json",
            submitted_at_ms
        ));
        let mut options = follow_position_options();
        options.persistence_path = persistence_path.clone();

        assert!(super::copy_live_daemon_save_live_submit_progress_snapshot(
            &options,
            &records,
            &[submitted],
        )?);

        let saved =
            crate::strategies::smart_money::load_copy_persistence_snapshot(&persistence_path)?;
        fs::remove_file(&persistence_path).ok();
        assert_eq!(saved.ledger_entries.len(), 1);
        let entry = &saved.ledger_entries[0];
        assert_eq!(entry.signal_id, signal.signal_id);
        assert_eq!(entry.order_cloid.as_deref(), Some(cloid.as_str()));
        assert_eq!(entry.order_oid, Some(9001));
        assert_eq!(entry.submitted_at_ms, Some(submitted_at_ms));
        assert_eq!(
            entry.status,
            crate::strategies::smart_money::CopyLedgerStatus::Open
        );
        assert!(entry.remaining_notional_usd > 0.0);
        Ok(())
    }

    fn follow_position_options() -> CopyLiveDaemonSupervisorOptions {
        CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: std::env::temp_dir().join("unused-follow-position-options.json"),
            shadow_history_path: std::env::temp_dir().join("unused-follow-position-options.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: false,
            submit: true,
            hold_positions_after_submit: true,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 60,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 50.0,
            max_total_fees_usd: 0.10,
            max_slippage_bps: 50.0,
            environment: None,
            ws_url: None,
        }
    }

    #[test]
    fn copy_live_daemon_signer_preflight_fails_before_runtime_submit_when_secret_missing() {
        let mut config = crate::config::AppConfig::default();
        config.secrets.vault_path = std::env::temp_dir()
            .join(format!("missing-copy-vault-{}.vault", now_ms()))
            .display()
            .to_string();
        config.secrets.allow_env_fallback = false;
        config.accounts = vec![
            crate::config::AccountConfig {
                account_id: "addr_a".to_string(),
                address: "0x0000000000000000000000000000000000000001".to_string(),
                secret_id: "addr_a_api_wallet".to_string(),
                api_wallet_env: String::new(),
                transfer_secret_id: String::new(),
                transfer_wallet_env: String::new(),
                enabled: true,
                worker_enabled: true,
                copy_ratio: 1.0,
                max_order_notional_usd: 100.0,
                blocked_markets: Vec::new(),
            },
            crate::config::AccountConfig {
                account_id: "addr_b".to_string(),
                address: "0x0000000000000000000000000000000000000002".to_string(),
                secret_id: "addr_b_api_wallet".to_string(),
                api_wallet_env: String::new(),
                transfer_secret_id: String::new(),
                transfer_wallet_env: String::new(),
                enabled: true,
                worker_enabled: true,
                copy_ratio: 1.0,
                max_order_notional_usd: 100.0,
                blocked_markets: Vec::new(),
            },
        ];

        let checks = super::copy_live_daemon_signer_preflight_checks(
            &config,
            &["addr_a".to_string(), "addr_b".to_string()],
            true,
        );

        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].name, "copy_signers_available");
        assert!(!checks[0].ok);
        assert!(checks[0].detail.contains("addr_a"));
        assert!(checks[0].detail.contains("addr_b"));
    }

    #[test]
    fn copy_live_daemon_submit_contract_accepts_multi_account_fanout_from_one_signal() {
        let mut options = follow_position_options();
        options.account_ids = vec!["addr_a".to_string(), "addr_b".to_string()];
        options.max_live_orders = 2;
        options.max_total_notional_usd = 1000.0;
        options.max_total_fees_usd = 1.0;
        let refs = vec![
            CopyLiveDaemonWouldSubmitRef {
                record_index: 7,
                signal_id: "sig-shared-target-fill".to_string(),
                leader_id: "leader_a".to_string(),
                leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:SILVER".to_string(),
                    side: crate::domain::OrderSide::Sell,
                    notional_usd: 120.0,
                    reduce_only: false,
                    cloid: "99999999-9999-5999-8999-999999999991".to_string(),
                },
            },
            CopyLiveDaemonWouldSubmitRef {
                record_index: 7,
                signal_id: "sig-shared-target-fill".to_string(),
                leader_id: "leader_a".to_string(),
                leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_b".to_string(),
                    worker_id: "worker-addr_b".to_string(),
                    coin: "xyz:SILVER".to_string(),
                    side: crate::domain::OrderSide::Sell,
                    notional_usd: 120.0,
                    reduce_only: false,
                    cloid: "99999999-9999-5999-8999-999999999992".to_string(),
                },
            },
        ];
        let reconciliations = vec![
            CopyBoundedLiveWindowReconcile {
                account_id: "addr_a".to_string(),
                ok: true,
                open_order_count: Some(0),
                asset_positions: Some(0),
                position_summaries: Vec::new(),
                account_value: Some("200".to_string()),
                withdrawable: Some("50".to_string()),
                total_ntl_pos: Some("0".to_string()),
                total_margin_used: Some("0".to_string()),
                error: None,
            },
            CopyBoundedLiveWindowReconcile {
                account_id: "addr_b".to_string(),
                ok: true,
                open_order_count: Some(0),
                asset_positions: Some(0),
                position_summaries: Vec::new(),
                account_value: Some("200".to_string()),
                withdrawable: Some("50".to_string()),
                total_ntl_pos: Some("0".to_string()),
                total_margin_used: Some("0".to_string()),
                error: None,
            },
        ];

        let contract = copy_live_daemon_submit_plan_contract(
            &options,
            &refs,
            &[],
            240.0,
            0.24,
            &reconciliations,
        );

        assert!(
            contract.ok,
            "multi-account fanout should pass submit contract: {:#?}",
            contract.checks
        );
        assert!(
            contract
                .checks
                .iter()
                .any(|check| check.name == "account_signal_refs_unique" && check.ok)
        );
    }

    #[test]
    fn copy_live_daemon_acceptance_live_order_cap_scales_with_selected_accounts() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18041")?;
        let mut config = crate::config::load_config(std::path::Path::new(&config_path))?;
        for (account_id, address_suffix) in [("addr_c", "3"), ("addr_d", "4")] {
            config.accounts.push(crate::config::AccountConfig {
                account_id: account_id.to_string(),
                address: format!("0x000000000000000000000000000000000000000{address_suffix}"),
                secret_id: String::new(),
                api_wallet_env: format!(
                    "HL_API_WALLET_PRIVATE_KEY_{}",
                    account_id.to_ascii_uppercase()
                ),
                transfer_secret_id: String::new(),
                transfer_wallet_env: format!(
                    "HL_EVM_TRANSFER_PRIVATE_KEY_{}",
                    account_id.to_ascii_uppercase()
                ),
                enabled: true,
                worker_enabled: true,
                copy_ratio: 0.05,
                max_order_notional_usd: 100.0,
                blocked_markets: Vec::new(),
            });
        }
        config.app.dry_run = false;
        config.manual_ops.manual_live_enabled = true;
        let dir = std::env::temp_dir().join(format!(
            "trade_xyz_copy_acceptance_live_order_cap_{}",
            now_ms()
        ));
        fs::create_dir_all(&dir).context("failed to create acceptance test dir")?;
        let account_ids = vec![
            "addr_a".to_string(),
            "addr_b".to_string(),
            "addr_c".to_string(),
            "addr_d".to_string(),
        ];
        let base_options = CopyLiveDaemonAcceptanceOptions {
            leaders: vec!["leader_a=0x00000000000000000000000000000000000000aa".to_string()],
            account_ids: account_ids.clone(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: dir.join("acceptance-persistence.json"),
            shadow_history_path: dir.join("acceptance-shadow.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live: true,
            allow_live_submit: true,
            confirm_mainnet_live: false,
            max_duration_secs: 300,
            max_live_orders: account_ids.len(),
            max_total_notional_usd: 1000.0,
            max_total_fees_usd: 1.0,
            max_slippage_bps: 50.0,
            require_cleanup_after_submit: true,
            require_flat_reconcile_after_submit: true,
        };

        let accepted = run_copy_live_daemon_acceptance(&config, base_options.clone())?;
        let bounded = accepted
            .checks
            .iter()
            .find(|check| check.name == "bounded_live_orders")
            .expect("bounded_live_orders check");
        assert!(bounded.ok, "{bounded:#?}");

        let mut too_low = base_options;
        too_low.max_live_orders = account_ids.len() - 1;
        let rejected = run_copy_live_daemon_acceptance(&config, too_low)?;
        let bounded = rejected
            .checks
            .iter()
            .find(|check| check.name == "bounded_live_orders")
            .expect("bounded_live_orders check");
        assert!(!bounded.ok, "{bounded:#?}");
        Ok(())
    }

    #[test]
    fn copy_live_daemon_margin_resize_accounts_for_fee_buffer() {
        let mut options = follow_position_options();
        options.max_total_notional_usd = 1000.0;
        options.max_total_fees_usd = 1.0;
        let refs = vec![CopyLiveDaemonWouldSubmitRef {
            record_index: 2,
            signal_id: "sig-tight-margin-open".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:SILVER".to_string(),
                side: crate::domain::OrderSide::Sell,
                notional_usd: 350.0,
                reduce_only: false,
                cloid: "88888888-8888-5888-8888-888888888888".to_string(),
            },
        }];
        let reconciliations = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: true,
            open_order_count: Some(0),
            asset_positions: Some(0),
            position_summaries: Vec::new(),
            account_value: Some("50".to_string()),
            withdrawable: Some("14.275086".to_string()),
            total_ntl_pos: Some("0".to_string()),
            total_margin_used: Some("0".to_string()),
            error: None,
        }];
        let (prepared, suppressed) =
            super::copy_live_daemon_resize_open_refs_for_margin(&refs, &reconciliations);

        assert!(suppressed.is_empty(), "{suppressed:#?}");
        assert_eq!(prepared.len(), 1);
        assert!(prepared[0].order.notional_usd < refs[0].order.notional_usd);
        let planned_notional = prepared[0].order.notional_usd;
        let contract = copy_live_daemon_submit_plan_contract(
            &options,
            &prepared,
            &[],
            planned_notional,
            planned_notional * 0.001,
            &reconciliations,
        );
        assert!(
            contract.ok,
            "resized plan should satisfy final margin contract: {:#?}",
            contract.checks
        );
    }

    #[test]
    fn copy_live_daemon_persistent_submit_dry_run_plans_only_executable_refs() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18014")?;
        let config = crate::config::load_config(std::path::Path::new(&config_path))?;
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: std::env::temp_dir().join("unused-persistent-plan.json"),
            shadow_history_path: std::env::temp_dir().join("unused-persistent-plan.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: false,
            submit: false,
            hold_positions_after_submit: false,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 900,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 50.0,
            max_total_fees_usd: 0.10,
            max_slippage_bps: 50.0,
            environment: None,
            ws_url: None,
        };
        let executable_refs = vec![CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-open".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:SP500".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 12.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000001".to_string(),
            },
        }];
        let suppressed_refs = vec![CopyLiveDaemonSuppressedWouldSubmitRef {
            plan: CopyLiveDaemonWouldSubmitRef {
                record_index: 1,
                signal_id: "sig-suppressed".to_string(),
                leader_id: "leader_b".to_string(),
                leader_address: "0x00000000000000000000000000000000000000bb".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:XYZ100".to_string(),
                    side: crate::domain::OrderSide::Buy,
                    notional_usd: 12.0,
                    reduce_only: false,
                    cloid: "00000000-0000-0000-0000-000000000002".to_string(),
                },
            },
            reason_code: "COPY_DAEMON_MAX_LIVE_ORDERS".to_string(),
            message: "observation only".to_string(),
        }];
        let flat = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: true,
            open_order_count: Some(0),
            asset_positions: Some(0),
            position_summaries: Vec::new(),
            account_value: Some("100.0".to_string()),
            withdrawable: Some("100.0".to_string()),
            total_ntl_pos: Some("0.0".to_string()),
            total_margin_used: Some("0.0".to_string()),
            error: None,
        }];
        let contract = copy_live_daemon_submit_plan_contract(
            &options,
            &executable_refs,
            &suppressed_refs,
            12.0,
            0.012,
            &flat,
        );

        let dry_run = copy_live_daemon_persistent_submit_dry_run(
            &config,
            &contract,
            &executable_refs,
            &suppressed_refs,
            options.max_slippage_bps,
        );

        assert!(dry_run.ok, "{dry_run:#?}");
        assert_eq!(dry_run.planned_reports.len(), 1);
        assert!(dry_run.planned_reports[0].would_submit);
        assert!(dry_run.planned_reports[0].dry_run_only);
        assert_eq!(dry_run.planned_reports[0].signal_id, "sig-open");
        assert_eq!(
            dry_run.planned_reports[0].cloid,
            executable_refs[0].order.cloid
        );
        assert!(
            dry_run
                .checks
                .iter()
                .any(|check| { check.name == "suppressed_refs_not_planned" && check.ok })
        );
        assert!(
            dry_run
                .checks
                .iter()
                .any(|check| { check.name == "planned_cloids_match_executable_refs" && check.ok })
        );
        Ok(())
    }

    #[test]
    fn copy_live_daemon_approved_order_from_ref_preserves_ref_cloid() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18018")?;
        let mut config = crate::config::load_config(std::path::Path::new(&config_path))?;
        config.app.dry_run = false;
        config.manual_ops.manual_live_enabled = true;
        let account = config.account("addr_a").context("missing addr_a")?;
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: std::env::temp_dir().join("unused-live-ref-order.json"),
            shadow_history_path: std::env::temp_dir().join("unused-live-ref-order.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: false,
            submit: true,
            hold_positions_after_submit: false,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 60,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 50.0,
            max_total_fees_usd: 0.10,
            max_slippage_bps: 50.0,
            environment: None,
            ws_url: None,
        };
        let plan = CopyLiveDaemonWouldSubmitRef {
            record_index: 7,
            signal_id: "sig-live-ref".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 50.0,
                reduce_only: false,
                cloid: "11111111-1111-5111-8111-111111111111".to_string(),
            },
        };

        let order = approved_copy_daemon_order_from_ref(&config, &options, account, &plan, false)?;

        assert_eq!(order.cloid, plan.order.cloid);
        assert_eq!(order.account_id, "addr_a");
        assert_eq!(order.notional_usd, 50.0);
        assert_eq!(order.signal_id.as_deref(), Some("sig-live-ref"));
        Ok(())
    }

    #[test]
    fn copy_live_daemon_submit_ref_infers_market_dex_from_coin() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18034")?;
        let mut config = crate::config::load_config(std::path::Path::new(&config_path))?;
        config.hyperliquid.dex = "xyz".to_string();
        config.app.dry_run = false;
        config.manual_ops.manual_live_enabled = true;
        let account = config
            .account("addr_a")
            .context("addr_a should exist in test config")?;
        let options = follow_position_options();

        let mut plan = CopyLiveDaemonWouldSubmitRef {
            record_index: 8,
            signal_id: "sig-other-dex".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "otherdex:ABC".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 25.0,
                reduce_only: false,
                cloid: "11111111-1111-5111-8111-111111111112".to_string(),
            },
        };

        let other_order =
            approved_copy_daemon_order_from_ref(&config, &options, account, &plan, false)?;
        assert_eq!(other_order.dex.as_deref(), Some("otherdex"));
        assert_eq!(other_order.market.as_deref(), Some("otherdex_perp"));

        plan.order.coin = "cash:USA500".to_string();
        plan.signal_id = "sig-cash-perp".to_string();
        plan.order.cloid = "11111111-1111-5111-8111-111111111114".to_string();
        let cash_order =
            approved_copy_daemon_order_from_ref(&config, &options, account, &plan, false)?;
        assert_eq!(cash_order.dex.as_deref(), Some("cash"));
        assert_eq!(
            cash_order.market.as_deref(),
            Some(crate::config::MARKET_CASH_PERP)
        );

        plan.order.coin = "BTC".to_string();
        plan.signal_id = "sig-hl-perp".to_string();
        plan.order.cloid = "11111111-1111-5111-8111-111111111113".to_string();
        let hl_order =
            approved_copy_daemon_order_from_ref(&config, &options, account, &plan, false)?;
        assert_eq!(hl_order.dex.as_deref(), Some(""));
        assert_eq!(
            hl_order.market.as_deref(),
            Some(crate::config::MARKET_HL_PERP)
        );
        Ok(())
    }

    #[test]
    fn copy_live_daemon_open_ref_sets_isolated_target_leverage() -> Result<()> {
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:JPY".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: std::env::temp_dir().join("unused-live-leverage.json"),
            shadow_history_path: std::env::temp_dir().join("unused-live-leverage.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: true,
            submit: true,
            hold_positions_after_submit: true,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 60,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 200.0,
            max_total_fees_usd: 1.0,
            max_slippage_bps: 50.0,
            environment: None,
            ws_url: None,
        };
        let plan = CopyLiveDaemonWouldSubmitRef {
            record_index: 8,
            signal_id: "sig-live-leverage".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:JPY".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 50.0,
                reduce_only: false,
                cloid: "22222222-2222-5222-8222-222222222222".to_string(),
            },
        };

        let leverage_options = copy_daemon_live_leverage_update_options(&options, &plan)?
            .context("opening copy order should require leverage update")?;

        assert_eq!(leverage_options.account_id, "addr_a");
        assert_eq!(leverage_options.coin, "xyz:JPY");
        assert_eq!(
            leverage_options.leverage,
            crate::strategies::smart_money::COPY_MAX_LEVERAGE as u32
        );
        assert_eq!(leverage_options.margin_mode, "isolated");
        assert!(leverage_options.submit);
        assert!(leverage_options.confirm_mainnet_live);
        Ok(())
    }

    #[test]
    fn copy_live_daemon_open_ref_caps_leverage_to_asset_max() -> Result<()> {
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:HYUNDAI".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: std::env::temp_dir().join("unused-live-leverage-cap.json"),
            shadow_history_path: std::env::temp_dir().join("unused-live-leverage-cap.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: true,
            submit: true,
            hold_positions_after_submit: true,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 60,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 200.0,
            max_total_fees_usd: 1.0,
            max_slippage_bps: 50.0,
            environment: None,
            ws_url: None,
        };
        let plan = CopyLiveDaemonWouldSubmitRef {
            record_index: 8,
            signal_id: "sig-live-leverage-cap".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:HYUNDAI".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 50.0,
                reduce_only: false,
                cloid: "22222222-2222-5222-8222-222222222223".to_string(),
            },
        };

        let leverage_options =
            copy_daemon_live_leverage_update_options_with_max(&options, &plan, Some(5))?
                .context("opening copy order should require leverage update")?;

        assert_eq!(leverage_options.leverage, 5);
        assert_eq!(leverage_options.margin_mode, "isolated");
        assert!(leverage_options.submit);
        Ok(())
    }

    #[test]
    fn copy_live_daemon_reduce_only_ref_skips_leverage_update() -> Result<()> {
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:JPY".to_string(),
            side: crate::domain::OrderSide::Sell,
            persistence_path: std::env::temp_dir().join("unused-live-leverage-reduce.json"),
            shadow_history_path: std::env::temp_dir().join("unused-live-leverage-reduce.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: true,
            submit: true,
            hold_positions_after_submit: true,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 60,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 200.0,
            max_total_fees_usd: 1.0,
            max_slippage_bps: 50.0,
            environment: None,
            ws_url: None,
        };
        let plan = CopyLiveDaemonWouldSubmitRef {
            record_index: 9,
            signal_id: "sig-live-leverage-reduce".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:JPY".to_string(),
                side: crate::domain::OrderSide::Sell,
                notional_usd: 20.0,
                reduce_only: true,
                cloid: "33333333-3333-5333-8333-333333333333".to_string(),
            },
        };

        assert!(copy_daemon_live_leverage_update_options(&options, &plan)?.is_none());
        Ok(())
    }

    #[test]
    fn copy_live_daemon_persistent_live_submit_stays_noop_without_submit_flag() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18019")?;
        let config = crate::config::load_config(std::path::Path::new(&config_path))?;
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: std::env::temp_dir().join("unused-live-submit-noop.json"),
            shadow_history_path: std::env::temp_dir().join("unused-live-submit-noop.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: false,
            submit: false,
            hold_positions_after_submit: false,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 60,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 50.0,
            max_total_fees_usd: 0.10,
            max_slippage_bps: 50.0,
            environment: None,
            ws_url: None,
        };
        let executable_refs = vec![CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-no-submit".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 50.0,
                reduce_only: false,
                cloid: "22222222-2222-5222-8222-222222222222".to_string(),
            },
        }];
        let flat = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: true,
            open_order_count: Some(0),
            asset_positions: Some(0),
            position_summaries: Vec::new(),
            account_value: Some("100.0".to_string()),
            withdrawable: Some("100.0".to_string()),
            total_ntl_pos: Some("0.0".to_string()),
            total_margin_used: Some("0.0".to_string()),
            error: None,
        }];
        let contract = copy_live_daemon_submit_plan_contract(
            &options,
            &executable_refs,
            &[],
            50.0,
            0.05,
            &flat,
        );
        let base_snapshot = crate::strategies::smart_money::CopyPersistenceSnapshot::empty();

        let live_submit =
            tokio::runtime::Runtime::new()?.block_on(copy_live_daemon_persistent_live_submit(
                &config,
                &options,
                &contract,
                &executable_refs,
                &[],
                &base_snapshot,
                &[],
            ));

        assert!(!live_submit.ok);
        assert!(live_submit.submitted_reports.is_empty());
        assert!(
            live_submit
                .checks
                .iter()
                .any(|check| check.name == "submit_requested" && !check.ok)
        );
        Ok(())
    }

    #[test]
    fn copy_live_daemon_persistent_live_submit_accepts_multi_account_empty_executable_window()
    -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18020")?;
        let mut config = crate::config::load_config(std::path::Path::new(&config_path))?;
        config.app.dry_run = false;
        config.manual_ops.manual_live_enabled = true;
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string(), "addr_b".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: std::env::temp_dir().join("unused-live-submit-empty.json"),
            shadow_history_path: std::env::temp_dir().join("unused-live-submit-empty.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: false,
            submit: true,
            hold_positions_after_submit: false,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 60,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 50.0,
            max_total_fees_usd: 0.10,
            max_slippage_bps: 50.0,
            environment: None,
            ws_url: None,
        };
        let flat = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: true,
            open_order_count: Some(0),
            asset_positions: Some(0),
            position_summaries: Vec::new(),
            account_value: Some("100.0".to_string()),
            withdrawable: Some("100.0".to_string()),
            total_ntl_pos: Some("0.0".to_string()),
            total_margin_used: Some("0.0".to_string()),
            error: None,
        }];
        let contract = copy_live_daemon_submit_plan_contract(&options, &[], &[], 0.0, 0.0, &flat);
        let base_snapshot = crate::strategies::smart_money::CopyPersistenceSnapshot::empty();

        let live_submit =
            tokio::runtime::Runtime::new()?.block_on(copy_live_daemon_persistent_live_submit(
                &config,
                &options,
                &contract,
                &[],
                &[],
                &base_snapshot,
                &[],
            ));

        assert!(live_submit.ok);
        assert!(live_submit.submitted_reports.is_empty());
        assert!(live_submit.cleanup_runbooks.is_empty());
        assert!(
            live_submit
                .checks
                .iter()
                .any(|check| check.name == "cleanup_runbook_completed" && check.ok)
        );
        assert!(
            live_submit
                .checks
                .iter()
                .any(|check| check.name == "selected_submit_accounts" && check.ok)
        );
        Ok(())
    }

    #[test]
    fn copy_live_daemon_submit_accounts_scope_allows_selected_multi_account_refs() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18020")?;
        let config = crate::config::load_config(std::path::Path::new(&config_path))?;
        let options = CopyLiveDaemonSupervisorOptions {
            leaders: Vec::new(),
            account_ids: vec!["addr_a".to_string(), "addr_b".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: std::env::temp_dir().join("unused-live-submit-scope.json"),
            shadow_history_path: std::env::temp_dir().join("unused-live-submit-scope.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: false,
            submit: true,
            hold_positions_after_submit: true,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 60,
            max_events: 1000,
            max_live_orders: 2,
            max_total_notional_usd: 100.0,
            max_total_fees_usd: 0.10,
            max_slippage_bps: 50.0,
            environment: None,
            ws_url: None,
        };
        let selected_ref = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-selected-account".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_b".to_string(),
                worker_id: "worker-addr_b".to_string(),
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 25.0,
                reduce_only: false,
                cloid: "66666666-6666-5666-8666-666666666666".to_string(),
            },
        };
        let unselected_ref = CopyLiveDaemonWouldSubmitRef {
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_c".to_string(),
                worker_id: "worker-addr_c".to_string(),
                cloid: "77777777-7777-5777-8777-777777777777".to_string(),
                ..selected_ref.order.clone()
            },
            ..selected_ref.clone()
        };

        assert!(super::copy_live_daemon_submit_accounts_in_scope(
            &config,
            &options,
            &[selected_ref]
        ));
        assert!(!super::copy_live_daemon_submit_accounts_in_scope(
            &config,
            &options,
            &[unselected_ref]
        ));
        Ok(())
    }

    #[test]
    fn copy_live_daemon_accounts_for_refs_scopes_immediate_precheck() {
        let addr_a_ref = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-a".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 25.0,
                reduce_only: false,
                cloid: "aaaaaaaa-aaaa-5aaa-8aaa-aaaaaaaaaaaa".to_string(),
            },
        };
        let duplicate_addr_a_ref = CopyLiveDaemonWouldSubmitRef {
            order: CopyExecutionCanaryWouldSubmit {
                cloid: "bbbbbbbb-bbbb-5bbb-8bbb-bbbbbbbbbbbb".to_string(),
                ..addr_a_ref.order.clone()
            },
            ..addr_a_ref.clone()
        };
        let addr_b_ref = CopyLiveDaemonWouldSubmitRef {
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_b".to_string(),
                worker_id: "worker-addr_b".to_string(),
                cloid: "cccccccc-cccc-5ccc-8ccc-cccccccccccc".to_string(),
                ..addr_a_ref.order.clone()
            },
            ..addr_a_ref.clone()
        };

        assert_eq!(
            super::copy_live_daemon_accounts_for_refs(&[
                addr_b_ref,
                addr_a_ref,
                duplicate_addr_a_ref
            ]),
            vec!["addr_a".to_string(), "addr_b".to_string()]
        );
    }

    #[test]
    fn copy_live_daemon_submit_evidence_contract_requires_real_live_evidence() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18021")?;
        let mut config = crate::config::load_config(std::path::Path::new(&config_path))?;
        config.app.dry_run = false;
        config.manual_ops.manual_live_enabled = true;
        let dir = std::env::temp_dir().join(format!(
            "trade_xyz_copy_live_daemon_empty_submit_contract_{}",
            now_ms()
        ));
        fs::create_dir_all(&dir).context("failed to create daemon contract test dir")?;
        let leaders = vec!["leader_a=0x00000000000000000000000000000000000000aa".to_string()];
        let acceptance = run_copy_live_daemon_acceptance(
            &config,
            CopyLiveDaemonAcceptanceOptions {
                leaders: leaders.clone(),
                account_ids: vec!["addr_a".to_string()],
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                persistence_path: dir.join("acceptance-persistence.json"),
                shadow_history_path: dir.join("acceptance-shadow.jsonl"),
                leader_notional_usd: 120.0,
                leader_size: 1.0,
                live: true,
                allow_live_submit: true,
                confirm_mainnet_live: false,
                max_duration_secs: 300,
                max_live_orders: 1,
                max_total_notional_usd: 150.0,
                max_total_fees_usd: 0.10,
                max_slippage_bps: 50.0,
                require_cleanup_after_submit: true,
                require_flat_reconcile_after_submit: true,
            },
        )?;
        let options = CopyLiveDaemonSupervisorOptions {
            leaders,
            account_ids: vec!["addr_a".to_string()],
            local_account_id: Some("addr_a".to_string()),
            markets: Vec::new(),
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: dir.join("supervisor-persistence.json"),
            shadow_history_path: dir.join("supervisor-shadow.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            live_gate: true,
            allow_live_submit: true,
            confirm_mainnet_live: false,
            submit: true,
            hold_positions_after_submit: false,
            cleanup_max_slippage_bps: 50.0,
            duration_secs: 300,
            max_events: 1000,
            max_live_orders: 1,
            max_total_notional_usd: 50.0,
            max_total_fees_usd: 0.10,
            max_slippage_bps: 50.0,
            environment: None,
            ws_url: None,
        };
        let flat = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: true,
            open_order_count: Some(0),
            asset_positions: Some(0),
            position_summaries: Vec::new(),
            account_value: Some("100.0".to_string()),
            withdrawable: Some("100.0".to_string()),
            total_ntl_pos: Some("0.0".to_string()),
            total_margin_used: Some("0.0".to_string()),
            error: None,
        }];
        let plan_contract =
            copy_live_daemon_submit_plan_contract(&options, &[], &[], 0.0, 0.0, &flat);
        let base_snapshot = crate::strategies::smart_money::CopyPersistenceSnapshot::empty();
        let live_submit =
            tokio::runtime::Runtime::new()?.block_on(copy_live_daemon_persistent_live_submit(
                &config,
                &options,
                &plan_contract,
                &[],
                &[],
                &base_snapshot,
                &[],
            ));

        let contract = copy_live_daemon_submit_evidence_contract(
            &options,
            &acceptance,
            &[],
            0.0,
            0.0,
            &flat,
            Some(&live_submit),
            &HashMap::new(),
        );

        assert!(live_submit.ok);
        assert!(!contract.ready_for_unattended_submit);
        assert!(
            contract
                .checks
                .iter()
                .any(|check| { check.name == "persistent_live_submit_path_connected" && check.ok })
        );
        assert!(
            contract
                .checks
                .iter()
                .any(|check| { check.name == "real_live_submit_evidence_present" && !check.ok })
        );
        assert!(
            contract
                .blocker
                .as_deref()
                .unwrap_or("")
                .contains("no real order was submitted")
        );
        Ok(())
    }

    #[test]
    fn copy_daemon_cleanup_targets_exclude_reduce_only_submit_refs() {
        let open_cloid = "33333333-3333-5333-8333-333333333333".to_string();
        let close_cloid = "44444444-4444-5444-8444-444444444444".to_string();
        let executable_refs = vec![
            CopyLiveDaemonWouldSubmitRef {
                record_index: 0,
                signal_id: "sig-open".to_string(),
                leader_id: "leader_a".to_string(),
                leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:XYZ100".to_string(),
                    side: crate::domain::OrderSide::Buy,
                    notional_usd: 50.0,
                    reduce_only: false,
                    cloid: open_cloid.clone(),
                },
            },
            CopyLiveDaemonWouldSubmitRef {
                record_index: 1,
                signal_id: "sig-close".to_string(),
                leader_id: "leader_a".to_string(),
                leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: "addr_a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    coin: "xyz:XYZ100".to_string(),
                    side: crate::domain::OrderSide::Sell,
                    notional_usd: 50.0,
                    reduce_only: true,
                    cloid: close_cloid.clone(),
                },
            },
        ];
        let reports = vec![
            crate::domain::WorkerReport::Submitted(crate::domain::OrderSubmitted {
                signal_id: "sig-open".to_string(),
                intent_id: "intent-open".to_string(),
                worker_id: "worker-addr_a".to_string(),
                account_id: "addr_a".to_string(),
                cloid: open_cloid.clone(),
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 50.0,
                submitted_price: Some(100.0),
                submitted_size: Some(0.5),
                exchange_status: Some("filled".to_string()),
                oid: Some(1001),
                filled_size: Some(0.5),
                avg_fill_price: Some(100.0),
                dry_run: false,
                submitted_at_ms: now_ms(),
            }),
            crate::domain::WorkerReport::Submitted(crate::domain::OrderSubmitted {
                signal_id: "sig-close".to_string(),
                intent_id: "intent-close".to_string(),
                worker_id: "worker-addr_a".to_string(),
                account_id: "addr_a".to_string(),
                cloid: close_cloid,
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Sell,
                notional_usd: 50.0,
                submitted_price: Some(100.0),
                submitted_size: Some(0.5),
                exchange_status: Some("filled".to_string()),
                oid: Some(1002),
                filled_size: Some(0.5),
                avg_fill_price: Some(100.0),
                dry_run: false,
                submitted_at_ms: now_ms(),
            }),
        ];

        let cleanup_targets =
            copy_daemon_submitted_reports_needing_cleanup(&reports, &executable_refs);

        assert_eq!(cleanup_targets.len(), 1);
        match &cleanup_targets[0] {
            crate::domain::WorkerReport::Submitted(submitted) => {
                assert_eq!(submitted.cloid, open_cloid);
            }
            other => panic!("unexpected cleanup target: {other:?}"),
        }
    }

    #[test]
    fn copy_live_daemon_merge_persistent_live_submit_reports_preserves_chunks() {
        let unsubmitted_pending = crate::strategies::smart_money::CopyLedgerEntry {
            local_account_id: "addr_a".to_string(),
            leader_id: "leader_a".to_string(),
            leader_group: "leader_a".to_string(),
            signal_id: "sig-shadow-only-open".to_string(),
            coin: "xyz:XYZ100".to_string(),
            local_side: crate::domain::OrderSide::Buy,
            order_cloid: None,
            order_oid: None,
            submitted_at_ms: None,
            filled_at_ms: None,
            planned_notional_usd: 50.0,
            pending_notional_usd: 50.0,
            filled_notional_usd: 0.0,
            remaining_notional_usd: 0.0,
            status: crate::strategies::smart_money::CopyLedgerStatus::PendingOpen,
        };
        let submitted_pending = crate::strategies::smart_money::CopyLedgerEntry {
            signal_id: "sig-submitted-open".to_string(),
            order_cloid: Some("55555555-5555-5555-8555-555555555555".to_string()),
            submitted_at_ms: Some(now_ms()),
            ..unsubmitted_pending.clone()
        };
        let chunk_a = CopyLiveDaemonPersistentLiveSubmitReport {
            ok: true,
            mode: "persistent_live_submit".to_string(),
            submit_requested: true,
            submit_plan_contract_ok: true,
            submitted_reports: vec![crate::domain::WorkerReport::Submitted(
                crate::domain::OrderSubmitted {
                    signal_id: "sig-a".to_string(),
                    intent_id: "intent-a".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    account_id: "addr_a".to_string(),
                    cloid: "55555555-5555-5555-8555-555555555555".to_string(),
                    coin: "xyz:XYZ100".to_string(),
                    side: crate::domain::OrderSide::Buy,
                    notional_usd: 50.0,
                    submitted_price: Some(100.0),
                    submitted_size: Some(0.5),
                    exchange_status: Some("filled".to_string()),
                    oid: Some(2001),
                    filled_size: Some(0.5),
                    avg_fill_price: Some(100.0),
                    dry_run: false,
                    submitted_at_ms: now_ms(),
                },
            )],
            order_evidence: Vec::new(),
            cleanup_runbooks: Vec::new(),
            cleanup_errors: Vec::new(),
            ledger_reconciliations: Vec::new(),
            ledger_reconciliation_snapshot:
                crate::strategies::smart_money::CopyPersistenceSnapshot {
                    schema_version: 1,
                    saved_at_ms: now_ms(),
                    seen_event_keys: vec!["event-a".to_string()],
                    ledger_entries: vec![unsubmitted_pending, submitted_pending],
                },
            checks: vec![copy_shadow_smoke_check("chunk_a", true, "ok")],
        };
        let chunk_b = CopyLiveDaemonPersistentLiveSubmitReport {
            ok: true,
            mode: "persistent_live_submit".to_string(),
            submit_requested: true,
            submit_plan_contract_ok: true,
            submitted_reports: Vec::new(),
            order_evidence: Vec::new(),
            cleanup_runbooks: Vec::new(),
            cleanup_errors: Vec::new(),
            ledger_reconciliations: Vec::new(),
            ledger_reconciliation_snapshot:
                crate::strategies::smart_money::CopyPersistenceSnapshot {
                    schema_version: 1,
                    saved_at_ms: now_ms(),
                    seen_event_keys: vec!["event-b".to_string()],
                    ledger_entries: Vec::new(),
                },
            checks: vec![copy_shadow_smoke_check("chunk_b", true, "ok")],
        };

        let merged = copy_live_daemon_merge_persistent_live_submit_reports(
            true,
            true,
            vec![chunk_a, chunk_b],
        );

        assert!(merged.ok);
        assert_eq!(merged.submitted_reports.len(), 1);
        assert_eq!(
            merged.ledger_reconciliation_snapshot.seen_event_keys,
            vec!["event-a".to_string(), "event-b".to_string()]
        );
        assert_eq!(
            merged.ledger_reconciliation_snapshot.ledger_entries.len(),
            1
        );
        assert_eq!(
            merged.ledger_reconciliation_snapshot.ledger_entries[0].signal_id,
            "sig-submitted-open"
        );
        assert!(
            merged
                .checks
                .iter()
                .any(|check| check.name == "persistent_live_submit_chunks" && check.ok)
        );
    }

    #[test]
    fn copy_live_daemon_classifies_safe_pre_submit_skips_only() {
        assert!(copy_live_daemon_error_is_safe_pre_submit_skip(
            "failed to set xyz:HYUNDAI leverage to 10x before copy submit"
        ));
        assert!(copy_live_daemon_error_is_safe_pre_submit_skip(
            "COPY_LIVE_MAX_LEVERAGE_TIMEOUT: addr_b xyz:NATGAS max leverage lookup timed out after 30s"
        ));
        assert!(copy_live_daemon_error_is_safe_pre_submit_skip(
            "COPY_LIVE_LEVERAGE_UPDATE_TIMEOUT: addr_b xyz:NATGAS leverage update to 10x timed out after 45s; exchange state must be reconciled before retry"
        ));
        assert!(copy_live_daemon_error_is_safe_pre_submit_skip(
            "copy submit skipped before exchange: addr_b xyz:SP500 requested_notional=11.172000 effective_notional=7.416100 below exchange minimum 10.000000"
        ));
        assert!(copy_live_daemon_error_is_safe_pre_submit_skip(
            "order size rounds to zero for xyz:GME at notional 0.2269 and price 22.693"
        ));
        assert!(copy_live_daemon_error_is_safe_pre_submit_skip(
            "exchange returned action-level order error: Order must have minimum value of $10. asset=110052"
        ));
        assert!(!copy_live_daemon_error_is_safe_pre_submit_skip(
            "COPY_LIVE_ORDER_SUBMIT_TIMEOUT: submit ref addr_b xyz:NATGAS timed out after 60s"
        ));
        assert!(!copy_live_daemon_error_is_safe_pre_submit_skip(
            "exchange submit failed after order was sent"
        ));
        assert!(!copy_live_daemon_error_is_safe_pre_submit_skip(
            "failed to fetch order evidence after copy submit"
        ));
    }

    #[test]
    fn copy_live_daemon_classifies_submit_transport_failures() {
        assert!(copy_live_daemon_error_is_submit_transport_failure(
            "COPY_LIVE_WS_POST_FAILED: Hyperliquid websocket order post failed: websocket post response channel closed"
        ));
        assert!(copy_live_daemon_error_is_submit_transport_failure(
            "Hyperliquid websocket order post failed"
        ));
        assert!(copy_live_daemon_error_is_submit_transport_failure(
            "failed to send websocket post request: IO error"
        ));
        assert!(!copy_live_daemon_error_is_submit_transport_failure(
            "order size rounds to zero for xyz:GME at notional 0.2269 and price 22.693"
        ));
        assert!(!copy_live_daemon_error_is_submit_transport_failure(
            "exchange returned action-level order error: Reduce only order would increase position."
        ));
        assert!(!copy_live_daemon_error_is_safe_pre_submit_skip(
            "COPY_LIVE_WS_POST_FAILED: Hyperliquid websocket order post failed: websocket post response channel closed"
        ));
    }

    #[test]
    fn copy_live_daemon_live_submit_health_accepts_fail_closed_transport_error() {
        let report = CopyLiveDaemonPersistentLiveSubmitReport {
            ok: false,
            mode: "persistent_live_submit".to_string(),
            submit_requested: true,
            submit_plan_contract_ok: true,
            submitted_reports: vec![crate::domain::WorkerReport::Error(
                crate::domain::WorkerError {
                    worker_id: "worker-addr_b".to_string(),
                    account_id: "addr_b".to_string(),
                    message:
                        "COPY_LIVE_WS_POST_FAILED: Hyperliquid websocket order post failed: websocket post response channel closed"
                            .to_string(),
                    error_at_ms: now_ms(),
                },
            )],
            order_evidence: Vec::new(),
            cleanup_runbooks: Vec::new(),
            cleanup_errors: Vec::new(),
            ledger_reconciliations: Vec::new(),
            ledger_reconciliation_snapshot:
                crate::strategies::smart_money::CopyPersistenceSnapshot::empty(),
            checks: vec![
                copy_shadow_smoke_check(
                    "submitted_reports",
                    false,
                    "0 live submitted report(s), 0 pre-submit skipped ref(s), for 1 submit-eligible ref(s); 0 reduce-only no-op ref(s) skipped",
                ),
                copy_shadow_smoke_check(
                    "submit_transport_fail_closed",
                    true,
                    "live submit transport failed for addr_b xyz:BOT; skipped 0 remaining submit ref(s) until the next exchange reconciliation",
                ),
                copy_shadow_smoke_check(
                    "order_status_evidence",
                    true,
                    "0 live submitted report(s), 0 order evidence record(s)",
                ),
                copy_shadow_smoke_check(
                    "ledger_reconciliation",
                    true,
                    "0 live submitted report(s), 0 ledger reconciliation result(s)",
                ),
            ],
        };

        assert!(copy_live_daemon_live_submit_health_ok(&report));
    }

    #[test]
    fn copy_live_daemon_live_submit_health_accepts_round206_safe_skip_shape() {
        let report = CopyLiveDaemonPersistentLiveSubmitReport {
            ok: true,
            mode: "persistent_live_submit".to_string(),
            submit_requested: true,
            submit_plan_contract_ok: true,
            submitted_reports: vec![crate::domain::WorkerReport::Error(
                crate::domain::WorkerError {
                    worker_id: "worker-addr_b".to_string(),
                    account_id: "addr_b".to_string(),
                    message:
                        "order size rounds to zero for xyz:GME at notional 0.2269 and price 22.693"
                            .to_string(),
                    error_at_ms: now_ms(),
                },
            )],
            order_evidence: Vec::new(),
            cleanup_runbooks: Vec::new(),
            cleanup_errors: Vec::new(),
            ledger_reconciliations: Vec::new(),
            ledger_reconciliation_snapshot:
                crate::strategies::smart_money::CopyPersistenceSnapshot::empty(),
            checks: vec![
                copy_shadow_smoke_check(
                    "submitted_reports",
                    true,
                    "0 live submitted report(s), 1 pre-submit skipped ref(s), for 1 submit-eligible ref(s); 0 reduce-only no-op ref(s) skipped",
                ),
                copy_shadow_smoke_check(
                    "order_status_evidence",
                    true,
                    "0 live submitted report(s), 0 order evidence record(s)",
                ),
                copy_shadow_smoke_check(
                    "persistent_live_submit_chunks",
                    true,
                    "1 persistent live submit chunk(s) merged",
                ),
            ],
        };

        assert!(copy_live_daemon_live_submit_health_ok(&report));
    }

    #[test]
    fn copy_live_daemon_persistent_submit_dry_run_blocks_when_plan_contract_fails() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18015")?;
        let config = crate::config::load_config(std::path::Path::new(&config_path))?;
        let executable_refs = vec![CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-blocked".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:SP500".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 12.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000001".to_string(),
            },
        }];
        let contract = CopyLiveDaemonSubmitPlanContract {
            ok: false,
            checks: vec![copy_shadow_smoke_check(
                "forced_failure",
                false,
                "test contract failure",
            )],
            executable_plan_count: executable_refs.len(),
            suppressed_plan_count: 0,
            executable_open_plan_count: 1,
            executable_reduce_only_plan_count: 0,
            planned_notional_usd: 12.0,
            estimated_fees_usd: 0.012,
        };

        let dry_run = copy_live_daemon_persistent_submit_dry_run(
            &config,
            &contract,
            &executable_refs,
            &[],
            50.0,
        );

        assert!(!dry_run.ok, "{dry_run:#?}");
        assert!(dry_run.planned_reports.is_empty());
        assert!(
            dry_run
                .checks
                .iter()
                .any(|check| { check.name == "submit_plan_contract_ok" && !check.ok })
        );
        Ok(())
    }

    #[test]
    fn copy_live_daemon_supervisor_appends_would_submit_refs_once() {
        let mut refs = vec![CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-1".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 12.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000001".to_string(),
            },
        }];

        let duplicate = refs[0].clone();
        append_unique_copy_daemon_would_submit_refs(
            &mut refs,
            vec![
                duplicate,
                CopyLiveDaemonWouldSubmitRef {
                    record_index: 1,
                    signal_id: "sig-2".to_string(),
                    leader_id: "leader_b".to_string(),
                    leader_address: "0x00000000000000000000000000000000000000bb".to_string(),
                    order: CopyExecutionCanaryWouldSubmit {
                        account_id: "addr_a".to_string(),
                        worker_id: "worker-addr_a".to_string(),
                        coin: "xyz:XYZ100".to_string(),
                        side: crate::domain::OrderSide::Sell,
                        notional_usd: 10.0,
                        reduce_only: true,
                        cloid: "00000000-0000-0000-0000-000000000002".to_string(),
                    },
                },
            ],
        );

        assert_eq!(refs.len(), 2);
        assert_eq!(refs[1].signal_id, "sig-2");
    }

    #[test]
    fn copy_live_daemon_pending_plan_refs_excludes_already_submitted_cloids() {
        let submitted = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-submitted".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 12.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000001".to_string(),
            },
        };
        let pending = CopyLiveDaemonWouldSubmitRef {
            record_index: 1,
            signal_id: "sig-pending".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_b".to_string(),
                worker_id: "worker-addr_b".to_string(),
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 12.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000002".to_string(),
            },
        };
        let mut submitted_cloids = HashSet::new();
        submitted_cloids.insert(submitted.order.cloid.clone());

        let refs = copy_live_daemon_pending_plan_refs(&[submitted, pending], &submitted_cloids);

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].signal_id, "sig-pending");
        assert_eq!(refs[0].order.account_id, "addr_b");
    }

    #[test]
    fn copy_live_daemon_defers_open_refs_after_immediate_live_submit() {
        let open_ref = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-open".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:QNT".to_string(),
                side: crate::domain::OrderSide::Sell,
                notional_usd: 120.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000011".to_string(),
            },
        };
        let reduce_ref = CopyLiveDaemonWouldSubmitRef {
            record_index: 1,
            signal_id: "sig-reduce".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:QNT".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 25.0,
                reduce_only: true,
                cloid: "00000000-0000-0000-0000-000000000012".to_string(),
            },
        };
        let submitted_at_ms = now_ms();
        let live_chunk = CopyLiveDaemonPersistentLiveSubmitReport {
            ok: true,
            mode: "persistent_live_submit".to_string(),
            submit_requested: true,
            submit_plan_contract_ok: true,
            submitted_reports: vec![crate::domain::WorkerReport::Submitted(
                crate::domain::OrderSubmitted {
                    signal_id: "sig-live".to_string(),
                    intent_id: "intent-live".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    account_id: "addr_a".to_string(),
                    cloid: "00000000-0000-0000-0000-000000000010".to_string(),
                    coin: "xyz:QNT".to_string(),
                    side: crate::domain::OrderSide::Sell,
                    notional_usd: 50.0,
                    submitted_price: Some(74.0),
                    submitted_size: Some(0.67),
                    exchange_status: Some("filled".to_string()),
                    oid: Some(90010),
                    filled_size: Some(0.67),
                    avg_fill_price: Some(74.0),
                    dry_run: false,
                    submitted_at_ms,
                },
            )],
            order_evidence: Vec::new(),
            cleanup_runbooks: Vec::new(),
            cleanup_errors: Vec::new(),
            ledger_reconciliations: Vec::new(),
            ledger_reconciliation_snapshot:
                crate::strategies::smart_money::CopyPersistenceSnapshot {
                    schema_version: 1,
                    saved_at_ms: submitted_at_ms,
                    seen_event_keys: Vec::new(),
                    ledger_entries: Vec::new(),
                },
            checks: Vec::new(),
        };

        let (pending, suppressed) = copy_live_daemon_defer_open_refs_after_immediate_live_submit(
            vec![open_ref.clone(), reduce_ref.clone()],
            std::slice::from_ref(&live_chunk),
        );

        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].signal_id, reduce_ref.signal_id);
        assert_eq!(suppressed.len(), 1);
        assert_eq!(suppressed[0].plan.signal_id, open_ref.signal_id);
        assert_eq!(
            suppressed[0].reason_code,
            "COPY_DAEMON_DEFER_OPEN_AFTER_LIVE_SUBMIT"
        );

        let (pending_without_live_submit, suppressed_without_live_submit) =
            copy_live_daemon_defer_open_refs_after_immediate_live_submit(
                vec![open_ref, reduce_ref],
                &[],
            );
        assert_eq!(pending_without_live_submit.len(), 2);
        assert!(suppressed_without_live_submit.is_empty());
    }

    #[test]
    fn copy_live_daemon_pending_suppressed_refs_excludes_already_submitted_cloids() {
        let submitted = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-submitted".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 12.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000001".to_string(),
            },
        };
        let pending = CopyLiveDaemonWouldSubmitRef {
            record_index: 1,
            signal_id: "sig-pending".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_b".to_string(),
                worker_id: "worker-addr_b".to_string(),
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 12.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000002".to_string(),
            },
        };
        let refs = vec![
            CopyLiveDaemonSuppressedWouldSubmitRef {
                plan: submitted.clone(),
                reason_code: "COPY_DAEMON_MARGIN_RESIZED_BELOW_MIN".to_string(),
                message: "submitted cloid should not remain suppressed".to_string(),
            },
            CopyLiveDaemonSuppressedWouldSubmitRef {
                plan: pending.clone(),
                reason_code: "COPY_DAEMON_MARGIN_RESIZED_BELOW_MIN".to_string(),
                message: "pending cloid remains suppressed".to_string(),
            },
        ];
        let mut submitted_cloids = HashSet::new();
        submitted_cloids.insert(submitted.order.cloid.clone());

        let refs = copy_live_daemon_pending_suppressed_refs(&refs, &submitted_cloids);

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].plan.signal_id, "sig-pending");
        assert_eq!(refs[0].plan.order.account_id, "addr_b");
    }

    #[test]
    fn copy_live_daemon_submitted_report_cloids_drive_final_suppression_cleanup() {
        let submitted_cloid = "00000000-0000-0000-0000-000000000001".to_string();
        let report = CopyLiveDaemonPersistentLiveSubmitReport {
            ok: true,
            mode: "persistent_live_submit".to_string(),
            submit_requested: true,
            submit_plan_contract_ok: true,
            submitted_reports: vec![crate::domain::WorkerReport::Submitted(
                crate::domain::OrderSubmitted {
                    signal_id: "sig-submitted".to_string(),
                    intent_id: "intent-submitted".to_string(),
                    worker_id: "worker-addr_a".to_string(),
                    account_id: "addr_a".to_string(),
                    cloid: submitted_cloid.clone(),
                    coin: "xyz:XYZ100".to_string(),
                    side: crate::domain::OrderSide::Buy,
                    notional_usd: 12.0,
                    submitted_price: Some(100.0),
                    submitted_size: Some(0.12),
                    exchange_status: Some("filled".to_string()),
                    oid: Some(42),
                    filled_size: Some(0.12),
                    avg_fill_price: Some(100.0),
                    dry_run: false,
                    submitted_at_ms: now_ms(),
                },
            )],
            order_evidence: Vec::new(),
            cleanup_runbooks: Vec::new(),
            cleanup_errors: Vec::new(),
            ledger_reconciliations: Vec::new(),
            ledger_reconciliation_snapshot:
                crate::strategies::smart_money::CopyPersistenceSnapshot::empty(),
            checks: Vec::new(),
        };
        let submitted = CopyLiveDaemonWouldSubmitRef {
            record_index: 0,
            signal_id: "sig-submitted".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_a".to_string(),
                worker_id: "worker-addr_a".to_string(),
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 12.0,
                reduce_only: false,
                cloid: submitted_cloid.clone(),
            },
        };
        let pending = CopyLiveDaemonWouldSubmitRef {
            record_index: 1,
            signal_id: "sig-pending".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0x00000000000000000000000000000000000000aa".to_string(),
            order: CopyExecutionCanaryWouldSubmit {
                account_id: "addr_b".to_string(),
                worker_id: "worker-addr_b".to_string(),
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 12.0,
                reduce_only: false,
                cloid: "00000000-0000-0000-0000-000000000002".to_string(),
            },
        };
        let suppressed = vec![
            CopyLiveDaemonSuppressedWouldSubmitRef {
                plan: submitted,
                reason_code: "COPY_DAEMON_MARGIN_RESIZED_BELOW_MIN".to_string(),
                message: "already submitted inside watcher loop".to_string(),
            },
            CopyLiveDaemonSuppressedWouldSubmitRef {
                plan: pending,
                reason_code: "COPY_DAEMON_MARGIN_RESIZED_BELOW_MIN".to_string(),
                message: "still pending".to_string(),
            },
        ];

        let submitted_cloids = copy_live_daemon_submitted_report_cloids(&report);
        let filtered = copy_live_daemon_pending_suppressed_refs(&suppressed, &submitted_cloids);

        assert!(submitted_cloids.contains(&submitted_cloid));
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].plan.signal_id, "sig-pending");
    }

    #[test]
    fn copy_bounded_live_window_ok_requires_submit_execution_and_flat_reconcile() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18011")?;
        let mut config = crate::config::load_config(std::path::Path::new(&config_path))?;
        config.app.dry_run = false;
        config.manual_ops.manual_live_enabled = true;
        config.manual_ops.max_manual_order_notional_usd =
            crate::strategies::smart_money::COPY_DEFAULT_MAX_SIGNAL_NOTIONAL_USD;
        let dir = std::env::temp_dir().join(format!(
            "trade_xyz_copy_bounded_live_window_ok_{}",
            now_ms()
        ));
        fs::create_dir_all(&dir).context("failed to create bounded window test dir")?;

        let leaders = vec!["leader_a=0x00000000000000000000000000000000000000aa".to_string()];
        let acceptance = run_copy_live_daemon_acceptance(
            &config,
            CopyLiveDaemonAcceptanceOptions {
                leaders: leaders.clone(),
                account_ids: vec!["addr_a".to_string()],
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                persistence_path: dir.join("copy-persistence.json"),
                shadow_history_path: dir.join("copy-shadow.jsonl"),
                leader_notional_usd: 120.0,
                leader_size: 1.0,
                live: true,
                allow_live_submit: true,
                confirm_mainnet_live: false,
                max_duration_secs: 300,
                max_live_orders: 1,
                max_total_notional_usd: 150.0,
                max_total_fees_usd: 0.10,
                max_slippage_bps: 50.0,
                require_cleanup_after_submit: true,
                require_flat_reconcile_after_submit: true,
            },
        )?;
        let preflight = tokio::runtime::Runtime::new()?.block_on(run_copy_execution_canary(
            &config,
            CopyExecutionCanaryOptions {
                leaders,
                account_ids: vec!["addr_a".to_string()],
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                local_account_id: None,
                shadow_history_path: dir.join("copy-canary-shadow.jsonl"),
                leader_notional_usd: 120.0,
                leader_size: 1.0,
                live: true,
                allow_live_submit: true,
                confirm_mainnet_live: false,
                cleanup_after_submit: true,
                cleanup_max_slippage_bps: 50.0,
                preflight_only: true,
                max_orders: 1,
            },
        ))?;
        let checks = vec![CopyShadowSmokeCheck {
            name: "all_checks".to_string(),
            ok: true,
            detail: "test".to_string(),
        }];
        let flat = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: true,
            open_order_count: Some(0),
            asset_positions: Some(0),
            position_summaries: Vec::new(),
            account_value: Some("100.0".to_string()),
            withdrawable: Some("100.0".to_string()),
            total_ntl_pos: Some("0.0".to_string()),
            total_margin_used: Some("0.0".to_string()),
            error: None,
        }];
        let not_flat = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: false,
            open_order_count: Some(1),
            asset_positions: Some(0),
            position_summaries: Vec::new(),
            account_value: Some("100.0".to_string()),
            withdrawable: Some("100.0".to_string()),
            total_ntl_pos: Some("0.0".to_string()),
            total_margin_used: Some("0.0".to_string()),
            error: None,
        }];

        assert!(acceptance.ok, "{acceptance:#?}");
        assert!(preflight.ok, "{preflight:#?}");
        assert!(copy_bounded_live_window_ok(
            false,
            &checks,
            &acceptance,
            &preflight,
            None,
            &flat
        ));
        assert!(!copy_bounded_live_window_ok(
            false,
            &checks,
            &acceptance,
            &preflight,
            Some(&preflight),
            &flat
        ));
        assert!(!copy_bounded_live_window_ok(
            true,
            &checks,
            &acceptance,
            &preflight,
            None,
            &flat
        ));
        assert!(copy_bounded_live_window_ok(
            true,
            &checks,
            &acceptance,
            &preflight,
            Some(&preflight),
            &flat
        ));
        assert!(!copy_bounded_live_window_ok(
            true,
            &checks,
            &acceptance,
            &preflight,
            Some(&preflight),
            &not_flat
        ));
        Ok(())
    }

    #[test]
    fn copy_live_stability_soak_ok_requires_rounds_limits_and_flat_reconcile() -> Result<()> {
        let config_path = write_test_config("127.0.0.1:18012")?;
        let mut config = crate::config::load_config(std::path::Path::new(&config_path))?;
        config.app.dry_run = false;
        config.manual_ops.manual_live_enabled = true;
        let dir = std::env::temp_dir().join(format!(
            "trade_xyz_copy_live_stability_soak_ok_{}",
            now_ms()
        ));
        fs::create_dir_all(&dir).context("failed to create stability soak test dir")?;

        let leaders = vec!["leader_a=0x00000000000000000000000000000000000000aa".to_string()];
        let acceptance = run_copy_live_daemon_acceptance(
            &config,
            CopyLiveDaemonAcceptanceOptions {
                leaders: leaders.clone(),
                account_ids: vec!["addr_a".to_string()],
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                persistence_path: dir.join("copy-persistence.json"),
                shadow_history_path: dir.join("copy-shadow.jsonl"),
                leader_notional_usd: 120.0,
                leader_size: 1.0,
                live: true,
                allow_live_submit: true,
                confirm_mainnet_live: false,
                max_duration_secs: 300,
                max_live_orders: 1,
                max_total_notional_usd: 50.0,
                max_total_fees_usd: 0.10,
                max_slippage_bps: 50.0,
                require_cleanup_after_submit: true,
                require_flat_reconcile_after_submit: true,
            },
        )?;
        let preflight = tokio::runtime::Runtime::new()?.block_on(run_copy_execution_canary(
            &config,
            CopyExecutionCanaryOptions {
                leaders,
                account_ids: vec!["addr_a".to_string()],
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                local_account_id: None,
                shadow_history_path: dir.join("copy-canary-shadow.jsonl"),
                leader_notional_usd: 120.0,
                leader_size: 1.0,
                live: true,
                allow_live_submit: true,
                confirm_mainnet_live: false,
                cleanup_after_submit: true,
                cleanup_max_slippage_bps: 50.0,
                preflight_only: true,
                max_orders: 1,
            },
        ))?;
        let flat = vec![CopyBoundedLiveWindowReconcile {
            account_id: "addr_a".to_string(),
            ok: true,
            open_order_count: Some(0),
            asset_positions: Some(0),
            position_summaries: Vec::new(),
            account_value: Some("100.0".to_string()),
            withdrawable: Some("100.0".to_string()),
            total_ntl_pos: Some("0.0".to_string()),
            total_margin_used: Some("0.0".to_string()),
            error: None,
        }];
        let options = CopyLiveStabilitySoakOptions {
            leaders: vec!["leader_a=0x00000000000000000000000000000000000000aa".to_string()],
            account_ids: vec!["addr_a".to_string()],
            coin: "xyz:XYZ100".to_string(),
            side: crate::domain::OrderSide::Buy,
            persistence_path: dir.join("soak-persistence.json"),
            shadow_history_path: dir.join("soak-shadow.jsonl"),
            leader_notional_usd: 120.0,
            leader_size: 1.0,
            submit: true,
            allow_live_submit: true,
            confirm_mainnet_live: false,
            duration_secs: 300,
            interval_secs: 1,
            max_rounds: 2,
            max_live_orders: 1,
            max_total_notional_usd: 50.0,
            max_total_fees_usd: 0.10,
            max_slippage_bps: 50.0,
            cleanup_max_slippage_bps: 50.0,
        };
        let mut execution = preflight.clone();
        execution.submitted_reports = vec![crate::domain::WorkerReport::Submitted(
            crate::domain::OrderSubmitted {
                signal_id: "sig-soak".to_string(),
                intent_id: "intent-soak".to_string(),
                worker_id: "worker-addr_a".to_string(),
                account_id: "addr_a".to_string(),
                cloid: "00000000-0000-0000-0000-000000000001".to_string(),
                coin: "xyz:XYZ100".to_string(),
                side: crate::domain::OrderSide::Buy,
                notional_usd: 12.0,
                submitted_price: Some(30_000.0),
                submitted_size: Some(0.0004),
                exchange_status: Some("filled".to_string()),
                oid: Some(42),
                filled_size: Some(0.0004),
                avg_fill_price: Some(30_000.0),
                dry_run: false,
                submitted_at_ms: now_ms(),
            },
        )];
        let round = super::CopyBoundedLiveWindowReport {
            ok: true,
            mode: "copy_bounded_live_window_submit".to_string(),
            environment: config.app.environment.clone(),
            submit_requested: true,
            live_submit_allowed: true,
            confirm_mainnet_live: false,
            max_duration_secs: 300,
            max_live_orders: 1,
            max_total_notional_usd: 50.0,
            max_total_fees_usd: 0.10,
            max_slippage_bps: 50.0,
            cleanup_max_slippage_bps: 50.0,
            target_accounts: vec!["addr_a".to_string()],
            checks: vec![CopyShadowSmokeCheck {
                name: "all_round_checks".to_string(),
                ok: true,
                detail: "test".to_string(),
            }],
            acceptance,
            preflight,
            execution: Some(execution),
            final_reconciliations: flat.clone(),
            next_actions: Vec::new(),
        };
        let rounds = vec![round];
        let (orders, notional) = copy_live_stability_round_submission_totals(&rounds[0]);
        let checks = vec![CopyShadowSmokeCheck {
            name: "all_soak_checks".to_string(),
            ok: true,
            detail: "test".to_string(),
        }];

        assert_eq!(orders, 1);
        assert_eq!(notional, 12.0);
        assert!(copy_live_stability_soak_ok(
            true,
            &checks,
            &rounds,
            orders,
            notional,
            notional * 0.001,
            &options,
            &flat
        ));
        assert!(!copy_live_stability_soak_ok(
            true,
            &checks,
            &rounds,
            0,
            notional,
            notional * 0.001,
            &options,
            &flat
        ));
        assert!(!copy_live_stability_soak_ok(
            true,
            &checks,
            &rounds,
            orders,
            51.0,
            notional * 0.001,
            &options,
            &flat
        ));
        assert!(!copy_live_stability_soak_ok(
            true,
            &checks,
            &[],
            0,
            0.0,
            0.0,
            &options,
            &flat
        ));
        Ok(())
    }

    fn write_test_config(coordinator_addr: &str) -> Result<String> {
        let seq = TEST_CONFIG_SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("trade_xyz_bot_test_{}_{}", now_ms(), seq));
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
transfer_wallet_env = "HL_EVM_TRANSFER_PRIVATE_KEY_ADDR_A"
enabled = true
worker_enabled = true
copy_ratio = 0.10
max_order_notional_usd = 100.0

[[accounts]]
account_id = "addr_b"
address = "0x0000000000000000000000000000000000000002"
api_wallet_env = "HL_API_WALLET_PRIVATE_KEY_ADDR_B"
transfer_wallet_env = "HL_EVM_TRANSFER_PRIVATE_KEY_ADDR_B"
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
    CopyShadowSmoke {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long = "leader")]
        leaders: Vec<String>,
        #[arg(long, default_value = "xyz:XYZ100")]
        coin: String,
        #[arg(long)]
        local_account_id: Option<String>,
        #[arg(long, default_value = "logs/copy_shadow_history.jsonl")]
        shadow_history: PathBuf,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        synthetic_event: bool,
        #[arg(long, default_value_t = 120.0)]
        leader_notional_usd: f64,
        #[arg(long, default_value_t = 1.0)]
        leader_size: f64,
    },
    CopyShadowWatch {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long = "leader")]
        leaders: Vec<String>,
        #[arg(long)]
        local_account_id: Option<String>,
        #[arg(long, default_value = "logs/copy_shadow_history.jsonl")]
        shadow_history: PathBuf,
        #[arg(long, default_value_t = 60)]
        duration_secs: u64,
        #[arg(long, default_value_t = 1000)]
        max_events: usize,
        #[arg(long)]
        environment: Option<String>,
        #[arg(long)]
        ws_url: Option<String>,
    },
    CopyExecutionCanary {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long = "leader")]
        leaders: Vec<String>,
        #[arg(long = "account-id")]
        account_ids: Vec<String>,
        #[arg(long, default_value = "xyz:XYZ100")]
        coin: String,
        #[arg(long, default_value = "buy")]
        side: String,
        #[arg(long)]
        local_account_id: Option<String>,
        #[arg(long, default_value = "logs/copy_shadow_history.jsonl")]
        shadow_history: PathBuf,
        #[arg(long, default_value_t = 120.0)]
        leader_notional_usd: f64,
        #[arg(long, default_value_t = 1.0)]
        leader_size: f64,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        live: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        allow_live_submit: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        confirm_mainnet_live: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        cleanup_after_submit: bool,
        #[arg(long, default_value_t = 50.0)]
        cleanup_max_slippage_bps: f64,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        preflight_only: bool,
        #[arg(long, default_value_t = 1)]
        max_orders: usize,
    },
    CopyLiveDaemonAcceptance {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long = "leader")]
        leaders: Vec<String>,
        #[arg(long = "account-id")]
        account_ids: Vec<String>,
        #[arg(long, default_value = "xyz:XYZ100")]
        coin: String,
        #[arg(long, default_value = "buy")]
        side: String,
        #[arg(
            long,
            default_value = ".codex-longrun/copy-live-daemon-acceptance-snapshot.json"
        )]
        persistence: PathBuf,
        #[arg(
            long,
            default_value = ".codex-longrun/copy-live-daemon-acceptance-shadow.jsonl"
        )]
        shadow_history: PathBuf,
        #[arg(long, default_value_t = 120.0)]
        leader_notional_usd: f64,
        #[arg(long, default_value_t = 1.0)]
        leader_size: f64,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        live: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        allow_live_submit: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        confirm_mainnet_live: bool,
        #[arg(long, default_value_t = 300)]
        max_duration_secs: u64,
        #[arg(long, default_value_t = 1)]
        max_live_orders: usize,
        #[arg(long, default_value_t = 50.0)]
        max_total_notional_usd: f64,
        #[arg(long, default_value_t = 0.10)]
        max_total_fees_usd: f64,
        #[arg(long, default_value_t = 50.0)]
        max_slippage_bps: f64,
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        require_cleanup_after_submit: bool,
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        require_flat_reconcile_after_submit: bool,
    },
    CopyLiveDaemonSupervisor {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long = "leader")]
        leaders: Vec<String>,
        #[arg(long = "account-id")]
        account_ids: Vec<String>,
        #[arg(long)]
        local_account_id: Option<String>,
        #[arg(long = "market")]
        markets: Vec<String>,
        #[arg(long, default_value = "xyz:XYZ100")]
        coin: String,
        #[arg(long, default_value = "buy")]
        side: String,
        #[arg(
            long,
            default_value = ".codex-longrun/copy-live-daemon-supervisor-snapshot.json"
        )]
        persistence: PathBuf,
        #[arg(
            long,
            default_value = ".codex-longrun/copy-live-daemon-supervisor-shadow.jsonl"
        )]
        shadow_history: PathBuf,
        #[arg(long, default_value_t = 120.0)]
        leader_notional_usd: f64,
        #[arg(long, default_value_t = 1.0)]
        leader_size: f64,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        live_gate: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        allow_live_submit: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        confirm_mainnet_live: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        submit: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        hold_positions_after_submit: bool,
        #[arg(long, default_value_t = 50.0)]
        cleanup_max_slippage_bps: f64,
        #[arg(long, default_value_t = 300)]
        duration_secs: u64,
        #[arg(long, default_value_t = 5000)]
        max_events: usize,
        #[arg(long, default_value_t = 1)]
        max_live_orders: usize,
        #[arg(long, default_value_t = 50.0)]
        max_total_notional_usd: f64,
        #[arg(long, default_value_t = 0.10)]
        max_total_fees_usd: f64,
        #[arg(long, default_value_t = 50.0)]
        max_slippage_bps: f64,
        #[arg(long)]
        environment: Option<String>,
        #[arg(long)]
        ws_url: Option<String>,
    },
    CopyBoundedLiveWindow {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long = "leader")]
        leaders: Vec<String>,
        #[arg(long = "account-id")]
        account_ids: Vec<String>,
        #[arg(long, default_value = "xyz:XYZ100")]
        coin: String,
        #[arg(long, default_value = "buy")]
        side: String,
        #[arg(
            long,
            default_value = ".codex-longrun/copy-bounded-live-window-snapshot.json"
        )]
        persistence: PathBuf,
        #[arg(
            long,
            default_value = ".codex-longrun/copy-bounded-live-window-shadow.jsonl"
        )]
        shadow_history: PathBuf,
        #[arg(long, default_value_t = 120.0)]
        leader_notional_usd: f64,
        #[arg(long, default_value_t = 1.0)]
        leader_size: f64,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        submit: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        allow_live_submit: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        confirm_mainnet_live: bool,
        #[arg(long, default_value_t = 300)]
        max_duration_secs: u64,
        #[arg(long, default_value_t = 1)]
        max_live_orders: usize,
        #[arg(long, default_value_t = 50.0)]
        max_total_notional_usd: f64,
        #[arg(long, default_value_t = 0.10)]
        max_total_fees_usd: f64,
        #[arg(long, default_value_t = 50.0)]
        max_slippage_bps: f64,
        #[arg(long, default_value_t = 50.0)]
        cleanup_max_slippage_bps: f64,
    },
    CopyLiveStabilitySoak {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long = "leader")]
        leaders: Vec<String>,
        #[arg(long = "account-id")]
        account_ids: Vec<String>,
        #[arg(long, default_value = "xyz:XYZ100")]
        coin: String,
        #[arg(long, default_value = "buy")]
        side: String,
        #[arg(
            long,
            default_value = ".codex-longrun/copy-live-stability-soak-snapshot.json"
        )]
        persistence: PathBuf,
        #[arg(
            long,
            default_value = ".codex-longrun/copy-live-stability-soak-shadow.jsonl"
        )]
        shadow_history: PathBuf,
        #[arg(long, default_value_t = 120.0)]
        leader_notional_usd: f64,
        #[arg(long, default_value_t = 1.0)]
        leader_size: f64,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        submit: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        allow_live_submit: bool,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        confirm_mainnet_live: bool,
        #[arg(long, default_value_t = 900)]
        duration_secs: u64,
        #[arg(long, default_value_t = 60)]
        interval_secs: u64,
        #[arg(long, default_value_t = 3)]
        max_rounds: usize,
        #[arg(long, default_value_t = 1)]
        max_live_orders: usize,
        #[arg(long, default_value_t = 50.0)]
        max_total_notional_usd: f64,
        #[arg(long, default_value_t = 0.10)]
        max_total_fees_usd: f64,
        #[arg(long, default_value_t = 50.0)]
        max_slippage_bps: f64,
        #[arg(long, default_value_t = 50.0)]
        cleanup_max_slippage_bps: f64,
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

fn emit_json_report<T: Serialize>(report: &T) -> Result<()> {
    let rendered = serde_json::to_string_pretty(report)?;
    if let Ok(path) = std::env::var("TRADE_XYZ_JSON_REPORT_PATH") {
        let path = path.trim();
        if !path.is_empty() {
            let path = Path::new(path);
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).with_context(|| {
                        format!("failed to create report directory {}", parent.display())
                    })?;
                }
            }
            std::fs::write(path, rendered)
                .with_context(|| format!("failed to write JSON report {}", path.display()))?;
            return Ok(());
        }
    }
    println!("{rendered}");
    Ok(())
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
        Command::CopyShadowSmoke {
            config,
            leaders,
            coin,
            local_account_id,
            shadow_history,
            synthetic_event,
            leader_notional_usd,
            leader_size,
        } => {
            let config = config::load_config(&config)?;
            let report = run_copy_shadow_smoke(
                &config,
                CopyShadowSmokeOptions {
                    leaders,
                    coin,
                    local_account_id,
                    shadow_history_path: shadow_history,
                    synthetic_event,
                    leader_notional_usd,
                    leader_size,
                },
            )?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
        Command::CopyShadowWatch {
            config,
            leaders,
            local_account_id,
            shadow_history,
            duration_secs,
            max_events,
            environment,
            ws_url,
        } => {
            let config = config::load_config(&config)?;
            let report = run_copy_shadow_watch(
                &config,
                CopyShadowWatchOptions {
                    leaders,
                    local_account_id,
                    shadow_history_path: shadow_history,
                    duration_secs,
                    max_events,
                    environment,
                    ws_url,
                },
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
        Command::CopyExecutionCanary {
            config,
            leaders,
            account_ids,
            coin,
            side,
            local_account_id,
            shadow_history,
            leader_notional_usd,
            leader_size,
            live,
            allow_live_submit,
            confirm_mainnet_live,
            cleanup_after_submit,
            cleanup_max_slippage_bps,
            preflight_only,
            max_orders,
        } => {
            let config = config::load_config(&config)?;
            let side = parse_order_side(&side)?;
            let report = run_copy_execution_canary(
                &config,
                CopyExecutionCanaryOptions {
                    leaders,
                    account_ids,
                    coin,
                    side,
                    local_account_id,
                    shadow_history_path: shadow_history,
                    leader_notional_usd,
                    leader_size,
                    live,
                    allow_live_submit,
                    confirm_mainnet_live,
                    cleanup_after_submit,
                    cleanup_max_slippage_bps,
                    preflight_only,
                    max_orders,
                },
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
        Command::CopyLiveDaemonAcceptance {
            config,
            leaders,
            account_ids,
            coin,
            side,
            persistence,
            shadow_history,
            leader_notional_usd,
            leader_size,
            live,
            allow_live_submit,
            confirm_mainnet_live,
            max_duration_secs,
            max_live_orders,
            max_total_notional_usd,
            max_total_fees_usd,
            max_slippage_bps,
            require_cleanup_after_submit,
            require_flat_reconcile_after_submit,
        } => {
            let config = config::load_config(&config)?;
            let side = parse_order_side(&side)?;
            let report = run_copy_live_daemon_acceptance(
                &config,
                CopyLiveDaemonAcceptanceOptions {
                    leaders,
                    account_ids,
                    coin,
                    side,
                    persistence_path: persistence,
                    shadow_history_path: shadow_history,
                    leader_notional_usd,
                    leader_size,
                    live,
                    allow_live_submit,
                    confirm_mainnet_live,
                    max_duration_secs,
                    max_live_orders,
                    max_total_notional_usd,
                    max_total_fees_usd,
                    max_slippage_bps,
                    require_cleanup_after_submit,
                    require_flat_reconcile_after_submit,
                },
            )?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
        Command::CopyLiveDaemonSupervisor {
            config,
            leaders,
            account_ids,
            local_account_id,
            markets,
            coin,
            side,
            persistence,
            shadow_history,
            leader_notional_usd,
            leader_size,
            live_gate,
            allow_live_submit,
            confirm_mainnet_live,
            submit,
            hold_positions_after_submit,
            cleanup_max_slippage_bps,
            duration_secs,
            max_events,
            max_live_orders,
            max_total_notional_usd,
            max_total_fees_usd,
            max_slippage_bps,
            environment,
            ws_url,
        } => {
            let config = config::load_config(&config)?;
            let side = parse_order_side(&side)?;
            let report = run_copy_live_daemon_supervisor(
                &config,
                CopyLiveDaemonSupervisorOptions {
                    leaders,
                    account_ids,
                    local_account_id,
                    markets,
                    coin,
                    side,
                    persistence_path: persistence,
                    shadow_history_path: shadow_history,
                    leader_notional_usd,
                    leader_size,
                    live_gate,
                    allow_live_submit,
                    confirm_mainnet_live,
                    submit,
                    hold_positions_after_submit,
                    cleanup_max_slippage_bps,
                    duration_secs,
                    max_events,
                    max_live_orders,
                    max_total_notional_usd,
                    max_total_fees_usd,
                    max_slippage_bps,
                    environment,
                    ws_url,
                },
            )
            .await?;
            emit_json_report(&report)?;
            Ok(())
        }
        Command::CopyBoundedLiveWindow {
            config,
            leaders,
            account_ids,
            coin,
            side,
            persistence,
            shadow_history,
            leader_notional_usd,
            leader_size,
            submit,
            allow_live_submit,
            confirm_mainnet_live,
            max_duration_secs,
            max_live_orders,
            max_total_notional_usd,
            max_total_fees_usd,
            max_slippage_bps,
            cleanup_max_slippage_bps,
        } => {
            let config = config::load_config(&config)?;
            let side = parse_order_side(&side)?;
            let report = run_copy_bounded_live_window(
                &config,
                CopyBoundedLiveWindowOptions {
                    leaders,
                    account_ids,
                    coin,
                    side,
                    persistence_path: persistence,
                    shadow_history_path: shadow_history,
                    leader_notional_usd,
                    leader_size,
                    submit,
                    allow_live_submit,
                    confirm_mainnet_live,
                    max_duration_secs,
                    max_live_orders,
                    max_total_notional_usd,
                    max_total_fees_usd,
                    max_slippage_bps,
                    cleanup_max_slippage_bps,
                },
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
        Command::CopyLiveStabilitySoak {
            config,
            leaders,
            account_ids,
            coin,
            side,
            persistence,
            shadow_history,
            leader_notional_usd,
            leader_size,
            submit,
            allow_live_submit,
            confirm_mainnet_live,
            duration_secs,
            interval_secs,
            max_rounds,
            max_live_orders,
            max_total_notional_usd,
            max_total_fees_usd,
            max_slippage_bps,
            cleanup_max_slippage_bps,
        } => {
            let config = config::load_config(&config)?;
            let side = parse_order_side(&side)?;
            let report = run_copy_live_stability_soak(
                &config,
                CopyLiveStabilitySoakOptions {
                    leaders,
                    account_ids,
                    coin,
                    side,
                    persistence_path: persistence,
                    shadow_history_path: shadow_history,
                    leader_notional_usd,
                    leader_size,
                    submit,
                    allow_live_submit,
                    confirm_mainnet_live,
                    duration_secs,
                    interval_secs,
                    max_rounds,
                    max_live_orders,
                    max_total_notional_usd,
                    max_total_fees_usd,
                    max_slippage_bps,
                    cleanup_max_slippage_bps,
                },
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
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

#[derive(Debug, Clone)]
struct CopyShadowSmokeOptions {
    leaders: Vec<String>,
    coin: String,
    local_account_id: Option<String>,
    shadow_history_path: PathBuf,
    synthetic_event: bool,
    leader_notional_usd: f64,
    leader_size: f64,
}

#[derive(Debug, Clone, Serialize)]
struct CopyShadowSmokeReport {
    ok: bool,
    mode: String,
    environment: String,
    process_dry_run: bool,
    local_account_id: Option<String>,
    target_accounts: Vec<String>,
    leaders: Vec<CopyShadowSmokeLeader>,
    watcher_subscriptions: Vec<Value>,
    checks: Vec<CopyShadowSmokeCheck>,
    shadow_history_path: String,
    synthetic_records_written: usize,
    recent_shadow_entries: usize,
    next_commands: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CopyShadowSmokeLeader {
    leader_id: String,
    leader_address: String,
}

#[derive(Debug, Clone, Serialize)]
struct CopyShadowSmokeCheck {
    name: String,
    ok: bool,
    detail: String,
}

struct CopyShadowSmokeReportInput {
    local_account_id: Option<String>,
    target_accounts: Vec<String>,
    leaders: Vec<CopyShadowSmokeLeader>,
    watcher_subscriptions: Vec<Value>,
    checks: Vec<CopyShadowSmokeCheck>,
    synthetic_records_written: usize,
}

#[derive(Debug, Clone)]
struct CopyShadowWatchOptions {
    leaders: Vec<String>,
    local_account_id: Option<String>,
    shadow_history_path: PathBuf,
    duration_secs: u64,
    max_events: usize,
    environment: Option<String>,
    ws_url: Option<String>,
}

#[derive(Debug, Clone)]
struct CopyExecutionCanaryOptions {
    leaders: Vec<String>,
    account_ids: Vec<String>,
    coin: String,
    side: domain::OrderSide,
    local_account_id: Option<String>,
    shadow_history_path: PathBuf,
    leader_notional_usd: f64,
    leader_size: f64,
    live: bool,
    allow_live_submit: bool,
    confirm_mainnet_live: bool,
    cleanup_after_submit: bool,
    cleanup_max_slippage_bps: f64,
    preflight_only: bool,
    max_orders: usize,
}

#[derive(Debug, Clone)]
struct CopyLiveDaemonAcceptanceOptions {
    leaders: Vec<String>,
    account_ids: Vec<String>,
    coin: String,
    side: domain::OrderSide,
    persistence_path: PathBuf,
    shadow_history_path: PathBuf,
    leader_notional_usd: f64,
    leader_size: f64,
    live: bool,
    allow_live_submit: bool,
    confirm_mainnet_live: bool,
    max_duration_secs: u64,
    max_live_orders: usize,
    max_total_notional_usd: f64,
    max_total_fees_usd: f64,
    max_slippage_bps: f64,
    require_cleanup_after_submit: bool,
    require_flat_reconcile_after_submit: bool,
}

#[derive(Debug, Clone)]
struct CopyLiveDaemonSupervisorOptions {
    leaders: Vec<String>,
    account_ids: Vec<String>,
    local_account_id: Option<String>,
    markets: Vec<String>,
    coin: String,
    side: domain::OrderSide,
    persistence_path: PathBuf,
    shadow_history_path: PathBuf,
    leader_notional_usd: f64,
    leader_size: f64,
    live_gate: bool,
    allow_live_submit: bool,
    confirm_mainnet_live: bool,
    submit: bool,
    hold_positions_after_submit: bool,
    cleanup_max_slippage_bps: f64,
    duration_secs: u64,
    max_events: usize,
    max_live_orders: usize,
    max_total_notional_usd: f64,
    max_total_fees_usd: f64,
    max_slippage_bps: f64,
    environment: Option<String>,
    ws_url: Option<String>,
}

#[derive(Debug, Clone)]
struct CopyBoundedLiveWindowOptions {
    leaders: Vec<String>,
    account_ids: Vec<String>,
    coin: String,
    side: domain::OrderSide,
    persistence_path: PathBuf,
    shadow_history_path: PathBuf,
    leader_notional_usd: f64,
    leader_size: f64,
    submit: bool,
    allow_live_submit: bool,
    confirm_mainnet_live: bool,
    max_duration_secs: u64,
    max_live_orders: usize,
    max_total_notional_usd: f64,
    max_total_fees_usd: f64,
    max_slippage_bps: f64,
    cleanup_max_slippage_bps: f64,
}

#[derive(Debug, Clone)]
struct CopyLiveStabilitySoakOptions {
    leaders: Vec<String>,
    account_ids: Vec<String>,
    coin: String,
    side: domain::OrderSide,
    persistence_path: PathBuf,
    shadow_history_path: PathBuf,
    leader_notional_usd: f64,
    leader_size: f64,
    submit: bool,
    allow_live_submit: bool,
    confirm_mainnet_live: bool,
    duration_secs: u64,
    interval_secs: u64,
    max_rounds: usize,
    max_live_orders: usize,
    max_total_notional_usd: f64,
    max_total_fees_usd: f64,
    max_slippage_bps: f64,
    cleanup_max_slippage_bps: f64,
}

#[derive(Debug, Clone, Serialize)]
struct CopyShadowWatchReport {
    ok: bool,
    mode: String,
    environment: String,
    ws_url: Option<String>,
    process_dry_run: bool,
    local_account_id: Option<String>,
    target_accounts: Vec<String>,
    leaders: Vec<CopyShadowSmokeLeader>,
    watcher_subscriptions: Vec<Value>,
    checks: Vec<CopyShadowSmokeCheck>,
    shadow_history_path: String,
    duration_secs: u64,
    elapsed_ms: u64,
    max_events: usize,
    events_received: usize,
    fill_events: usize,
    snapshot_fill_events: usize,
    position_snapshot_events: usize,
    position_snapshots: usize,
    order_update_events: usize,
    shadow_records_written: usize,
    recent_shadow_entries: usize,
    watcher_status: String,
    findings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CopyExecutionCanaryReport {
    ok: bool,
    mode: String,
    environment: String,
    execution_dry_run: bool,
    live_requested: bool,
    live_submit_allowed: bool,
    confirm_mainnet_live: bool,
    cleanup_after_submit: bool,
    cleanup_max_slippage_bps: f64,
    preflight_only: bool,
    coin: String,
    side: domain::OrderSide,
    target_accounts: Vec<String>,
    local_account_id: Option<String>,
    leader: Option<CopyShadowSmokeLeader>,
    checks: Vec<CopyShadowSmokeCheck>,
    shadow_records_written: usize,
    approved_shadow_records: usize,
    would_submit_orders: Vec<CopyExecutionCanaryWouldSubmit>,
    submitted_reports: Vec<domain::WorkerReport>,
    order_evidence: Vec<CopyExecutionCanaryOrderEvidence>,
    ledger_reconciliations: Vec<strategies::smart_money::CopyLedgerReconcileResult>,
    ledger_reconciliation_snapshot: strategies::smart_money::CopyPersistenceSnapshot,
    cleanup_runbooks: Vec<trading::SignedRunbookResult>,
    cleanup_errors: Vec<String>,
    next_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CopyExecutionCanaryWouldSubmit {
    account_id: String,
    worker_id: String,
    coin: String,
    side: domain::OrderSide,
    notional_usd: f64,
    reduce_only: bool,
    cloid: String,
}

#[derive(Debug, Clone, Serialize)]
struct CopyExecutionCanaryOrderEvidence {
    account_id: String,
    worker_id: String,
    signal_id: String,
    coin: String,
    oid: Option<u64>,
    cloid: String,
    order_status: Option<hyperliquid::OrderStatusResponse>,
    user_fill_count: usize,
    matching_fill_count: usize,
    matching_fills: Vec<hyperliquid::UserFill>,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CopyLiveDaemonAcceptanceReport {
    ok: bool,
    mode: String,
    environment: String,
    live_requested: bool,
    live_submit_allowed: bool,
    confirm_mainnet_live: bool,
    max_duration_secs: u64,
    max_live_orders: usize,
    max_total_notional_usd: f64,
    max_total_fees_usd: f64,
    max_slippage_bps: f64,
    require_cleanup_after_submit: bool,
    require_flat_reconcile_after_submit: bool,
    target_accounts: Vec<String>,
    leaders: Vec<CopyShadowSmokeLeader>,
    checks: Vec<CopyShadowSmokeCheck>,
    persistence_path: String,
    shadow_history_path: String,
    persistence_seen_keys_before: usize,
    persistence_ledger_entries_before: usize,
    restart_dedupe_probe: CopyLiveDaemonRestartProbe,
    shadow_records_written: usize,
    approved_shadow_records: usize,
    would_submit_orders: Vec<CopyExecutionCanaryWouldSubmit>,
    next_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CopyLiveDaemonSupervisorReport {
    ok: bool,
    mode: String,
    environment: String,
    ws_url: Option<String>,
    no_submit: bool,
    live_gate_requested: bool,
    live_submit_allowed: bool,
    confirm_mainnet_live: bool,
    submit_requested: bool,
    hold_positions_after_submit: bool,
    cleanup_max_slippage_bps: f64,
    duration_secs: u64,
    elapsed_ms: u64,
    max_events: usize,
    max_live_orders: usize,
    max_total_notional_usd: f64,
    max_total_fees_usd: f64,
    max_slippage_bps: f64,
    selected_markets: Vec<String>,
    acceptance_coin: String,
    target_accounts: Vec<String>,
    local_account_id: Option<String>,
    leaders: Vec<CopyShadowSmokeLeader>,
    checks: Vec<CopyShadowSmokeCheck>,
    acceptance: CopyLiveDaemonAcceptanceReport,
    watcher_subscriptions: Vec<Value>,
    persistence_path: String,
    shadow_history_path: String,
    persistence_seen_keys_before: usize,
    persistence_seen_keys_after: usize,
    persistence_ledger_entries_before: usize,
    persistence_ledger_entries_after: usize,
    events_received: usize,
    fill_events: usize,
    snapshot_fill_events: usize,
    position_snapshot_events: usize,
    position_snapshots: usize,
    order_update_events: usize,
    pending_unclassified_fill_count: usize,
    pending_unclassified_fill_labels: Vec<String>,
    shadow_records_written: usize,
    approved_shadow_records: usize,
    would_submit_orders: Vec<CopyExecutionCanaryWouldSubmit>,
    executable_would_submit_orders: Vec<CopyExecutionCanaryWouldSubmit>,
    suppressed_would_submit_orders: Vec<CopyLiveDaemonSuppressedWouldSubmit>,
    executable_submit_plan_refs: Vec<CopyLiveDaemonWouldSubmitRef>,
    suppressed_submit_plan_refs: Vec<CopyLiveDaemonSuppressedWouldSubmitRef>,
    planned_notional_usd: f64,
    estimated_fees_usd: f64,
    submit_plan_contract: CopyLiveDaemonSubmitPlanContract,
    persistent_submit_dry_run: CopyLiveDaemonPersistentSubmitDryRunReport,
    persistent_live_submit: CopyLiveDaemonPersistentLiveSubmitReport,
    submit_evidence_contract: CopyLiveDaemonSubmitEvidenceContract,
    watcher_status: String,
    final_reconciliations: Vec<CopyBoundedLiveWindowReconcile>,
    next_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CopyLiveDaemonSuppressedWouldSubmit {
    order: CopyExecutionCanaryWouldSubmit,
    reason_code: String,
    message: String,
}

#[derive(Debug, Clone, Serialize)]
struct CopyLiveDaemonWouldSubmitRef {
    record_index: usize,
    signal_id: String,
    leader_id: String,
    leader_address: String,
    order: CopyExecutionCanaryWouldSubmit,
}

#[derive(Debug, Clone, Serialize)]
struct CopyLiveDaemonSuppressedWouldSubmitRef {
    plan: CopyLiveDaemonWouldSubmitRef,
    reason_code: String,
    message: String,
}

#[derive(Debug, Clone, Serialize)]
struct CopyLiveDaemonSubmitPlanContract {
    ok: bool,
    checks: Vec<CopyShadowSmokeCheck>,
    executable_plan_count: usize,
    suppressed_plan_count: usize,
    executable_open_plan_count: usize,
    executable_reduce_only_plan_count: usize,
    planned_notional_usd: f64,
    estimated_fees_usd: f64,
}

#[derive(Debug, Clone, Serialize)]
struct CopyLiveDaemonPersistentSubmitDryRunReport {
    ok: bool,
    mode: String,
    submit_plan_contract_ok: bool,
    planned_reports: Vec<CopyLiveDaemonPersistentSubmitDryRunPlan>,
    checks: Vec<CopyShadowSmokeCheck>,
}

#[derive(Debug, Clone, Serialize)]
struct CopyLiveDaemonPersistentSubmitDryRunPlan {
    record_index: usize,
    signal_id: String,
    leader_id: String,
    leader_address: String,
    account_id: String,
    worker_id: String,
    coin: String,
    side: domain::OrderSide,
    notional_usd: f64,
    reduce_only: bool,
    cloid: String,
    would_submit: bool,
    dry_run_only: bool,
    rejected_reason_code: Option<String>,
    rejected_message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CopyLiveDaemonPersistentLiveSubmitReport {
    ok: bool,
    mode: String,
    submit_requested: bool,
    submit_plan_contract_ok: bool,
    submitted_reports: Vec<domain::WorkerReport>,
    order_evidence: Vec<CopyExecutionCanaryOrderEvidence>,
    cleanup_runbooks: Vec<trading::SignedRunbookResult>,
    cleanup_errors: Vec<String>,
    ledger_reconciliations: Vec<strategies::smart_money::CopyLedgerReconcileResult>,
    ledger_reconciliation_snapshot: strategies::smart_money::CopyPersistenceSnapshot,
    checks: Vec<CopyShadowSmokeCheck>,
}

#[derive(Debug, Clone, Serialize)]
struct CopyLiveDaemonSubmitEvidenceContract {
    ready_for_unattended_submit: bool,
    checks: Vec<CopyShadowSmokeCheck>,
    required_live_evidence: Vec<String>,
    blocker: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CopyLiveDaemonRestartProbe {
    event_id: String,
    first_emit_count: usize,
    replay_emit_count: usize,
    fresh_after_restart_emit_count: usize,
    saved_seen_event_keys: usize,
    loaded_seen_event_keys: usize,
}

#[derive(Debug, Clone, Serialize)]
struct CopyBoundedLiveWindowReport {
    ok: bool,
    mode: String,
    environment: String,
    submit_requested: bool,
    live_submit_allowed: bool,
    confirm_mainnet_live: bool,
    max_duration_secs: u64,
    max_live_orders: usize,
    max_total_notional_usd: f64,
    max_total_fees_usd: f64,
    max_slippage_bps: f64,
    cleanup_max_slippage_bps: f64,
    target_accounts: Vec<String>,
    checks: Vec<CopyShadowSmokeCheck>,
    acceptance: CopyLiveDaemonAcceptanceReport,
    preflight: CopyExecutionCanaryReport,
    execution: Option<CopyExecutionCanaryReport>,
    final_reconciliations: Vec<CopyBoundedLiveWindowReconcile>,
    next_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CopyLiveStabilitySoakReport {
    ok: bool,
    mode: String,
    environment: String,
    submit_requested: bool,
    live_submit_allowed: bool,
    confirm_mainnet_live: bool,
    duration_secs: u64,
    interval_secs: u64,
    elapsed_ms: u64,
    max_rounds: usize,
    rounds_attempted: usize,
    rounds_passed: usize,
    max_live_orders: usize,
    max_total_notional_usd: f64,
    max_total_fees_usd: f64,
    max_slippage_bps: f64,
    cleanup_max_slippage_bps: f64,
    target_accounts: Vec<String>,
    checks: Vec<CopyShadowSmokeCheck>,
    rounds: Vec<CopyBoundedLiveWindowReport>,
    total_submitted_orders: usize,
    total_submitted_notional_usd: f64,
    estimated_fees_usd: f64,
    final_reconciliations: Vec<CopyBoundedLiveWindowReconcile>,
    stop_reason: String,
    next_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CopyBoundedLiveWindowReconcile {
    account_id: String,
    ok: bool,
    open_order_count: Option<usize>,
    asset_positions: Option<usize>,
    position_summaries: Vec<CopyBoundedLiveWindowPositionSummary>,
    account_value: Option<String>,
    withdrawable: Option<String>,
    total_ntl_pos: Option<String>,
    total_margin_used: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CopyBoundedLiveWindowPositionSummary {
    coin: String,
    szi: String,
    position_value: Option<String>,
    unrealized_pnl: Option<String>,
}

fn run_copy_shadow_smoke(
    config: &config::AppConfig,
    options: CopyShadowSmokeOptions,
) -> Result<CopyShadowSmokeReport> {
    anyhow::ensure!(
        config.app.dry_run,
        "copy-shadow-smoke requires app.dry_run=true; refusing to run in live-capable config"
    );
    anyhow::ensure!(
        options.leader_size > 0.0,
        "leader_size must be positive for copy shadow smoke"
    );
    anyhow::ensure!(
        options.leader_notional_usd > 0.0,
        "leader_notional_usd must be positive for copy shadow smoke"
    );

    let target_accounts = config
        .enabled_worker_accounts()
        .map(|account| account.account_id.clone())
        .collect::<Vec<_>>();
    let local_account_id = options
        .local_account_id
        .clone()
        .or_else(|| target_accounts.first().cloned());
    let local_account = local_account_id
        .as_deref()
        .and_then(|account_id| config.account(account_id));
    let leaders = parse_copy_shadow_smoke_leaders(&options.leaders)?;
    let watcher_leaders = leaders
        .iter()
        .map(|leader| strategies::smart_money::SmartMoneyLeaderWatch {
            leader_id: leader.leader_id.clone(),
            leader_address: leader.leader_address.clone(),
        })
        .collect::<Vec<_>>();
    let watcher_subscriptions =
        strategies::smart_money::read_only_leader_watcher_subscriptions(&watcher_leaders, None);

    let mut checks = Vec::new();
    checks.push(copy_shadow_smoke_check(
        "config_dry_run",
        config.app.dry_run,
        "app.dry_run=true; command is restricted to read-only/dry-run copy validation",
    ));
    checks.push(copy_shadow_smoke_check(
        "enabled_worker_accounts",
        !target_accounts.is_empty(),
        format!("{} enabled worker account(s)", target_accounts.len()),
    ));
    checks.push(copy_shadow_smoke_check(
        "local_account",
        local_account.is_some(),
        local_account_id
            .as_deref()
            .map(|account_id| format!("local dry-run account {account_id} is configured"))
            .unwrap_or_else(|| "no local dry-run account is available".to_string()),
    ));
    checks.push(copy_shadow_smoke_check(
        "leaders_configured",
        !leaders.is_empty(),
        format!("{} leader watch target(s)", leaders.len()),
    ));
    checks.push(copy_shadow_smoke_check(
        "live_gate",
        matches!(
            strategies::smart_money::evaluate_copy_live_gate(
                strategies::smart_money::CopyLiveGateInput {
                    process_dry_run: true,
                    live_copy_enabled: false,
                    account_worker_live: false,
                },
            ),
            strategies::smart_money::CopyLiveGateDecision::DryRunOnly
        ),
        "process dry-run forces copy shadow pipeline into dry_run_only",
    ));
    checks.push(copy_shadow_smoke_check(
        "watcher_subscriptions",
        !watcher_subscriptions.is_empty(),
        format!(
            "{} read-only websocket subscription payload(s) prepared",
            watcher_subscriptions.len()
        ),
    ));

    let mut synthetic_records_written = 0usize;
    if options.synthetic_event {
        let Some(account) = local_account else {
            checks.push(copy_shadow_smoke_check(
                "synthetic_shadow_pipeline",
                false,
                "cannot run synthetic event without a configured local account",
            ));
            return copy_shadow_smoke_report(
                config,
                options,
                CopyShadowSmokeReportInput {
                    local_account_id,
                    target_accounts,
                    leaders,
                    watcher_subscriptions,
                    checks,
                    synthetic_records_written,
                },
            );
        };
        let Some(leader) = watcher_leaders.first() else {
            checks.push(copy_shadow_smoke_check(
                "synthetic_shadow_pipeline",
                false,
                "cannot run synthetic event without at least one --leader",
            ));
            return copy_shadow_smoke_report(
                config,
                options,
                CopyShadowSmokeReportInput {
                    local_account_id,
                    target_accounts,
                    leaders,
                    watcher_subscriptions,
                    checks,
                    synthetic_records_written,
                },
            );
        };
        synthetic_records_written =
            run_synthetic_copy_shadow_event(config, &options, account, leader, &target_accounts)?;
        checks.push(copy_shadow_smoke_check(
            "synthetic_shadow_pipeline",
            synthetic_records_written > 0,
            format!(
                "{synthetic_records_written} shadow history record(s) appended to {}",
                options.shadow_history_path.display()
            ),
        ));
    }

    copy_shadow_smoke_report(
        config,
        options,
        CopyShadowSmokeReportInput {
            local_account_id,
            target_accounts,
            leaders,
            watcher_subscriptions,
            checks,
            synthetic_records_written,
        },
    )
}

fn copy_shadow_smoke_report(
    config: &config::AppConfig,
    options: CopyShadowSmokeOptions,
    input: CopyShadowSmokeReportInput,
) -> Result<CopyShadowSmokeReport> {
    let recent_shadow_entries = strategies::smart_money::read_recent_copy_shadow_history_entries(
        &options.shadow_history_path,
        20,
    )?
    .len();
    let next_commands = copy_shadow_smoke_next_commands(&options, &input.leaders);
    Ok(CopyShadowSmokeReport {
        ok: input.checks.iter().all(|check| check.ok),
        mode: "read_only_dry_run_shadow".to_string(),
        environment: config.app.environment.clone(),
        process_dry_run: config.app.dry_run,
        local_account_id: input.local_account_id,
        target_accounts: input.target_accounts,
        leaders: input.leaders,
        watcher_subscriptions: input.watcher_subscriptions,
        checks: input.checks,
        shadow_history_path: options.shadow_history_path.display().to_string(),
        synthetic_records_written: input.synthetic_records_written,
        recent_shadow_entries,
        next_commands,
    })
}

async fn run_copy_shadow_watch(
    config: &config::AppConfig,
    options: CopyShadowWatchOptions,
) -> Result<CopyShadowWatchReport> {
    anyhow::ensure!(
        config.app.dry_run,
        "copy-shadow-watch requires app.dry_run=true; refusing to run in live-capable config"
    );
    anyhow::ensure!(
        options.duration_secs > 0,
        "duration_secs must be positive for copy shadow watch"
    );
    anyhow::ensure!(
        options.max_events > 0,
        "max_events must be positive for copy shadow watch"
    );

    let target_accounts = config
        .enabled_worker_accounts()
        .map(|account| account.account_id.clone())
        .collect::<Vec<_>>();
    let local_account_id = options
        .local_account_id
        .clone()
        .or_else(|| target_accounts.first().cloned());
    let local_account = local_account_id
        .as_deref()
        .and_then(|account_id| config.account(account_id));
    let leaders = parse_copy_shadow_smoke_leaders(&options.leaders)?;
    let watcher_leaders = leaders
        .iter()
        .map(|leader| strategies::smart_money::SmartMoneyLeaderWatch {
            leader_id: leader.leader_id.clone(),
            leader_address: leader.leader_address.clone(),
        })
        .collect::<Vec<_>>();
    let watcher_subscriptions =
        strategies::smart_money::read_only_leader_watcher_subscriptions(&watcher_leaders, None);
    let environment = options
        .environment
        .clone()
        .unwrap_or_else(|| config.app.environment.clone());
    let ws_url = options.ws_url.clone().or_else(|| {
        options
            .environment
            .is_none()
            .then(|| config.hyperliquid.ws_url.clone())
    });

    let mut checks = Vec::new();
    checks.push(copy_shadow_smoke_check(
        "config_dry_run",
        config.app.dry_run,
        "app.dry_run=true; command is restricted to read-only/dry-run copy validation",
    ));
    checks.push(copy_shadow_smoke_check(
        "enabled_worker_accounts",
        !target_accounts.is_empty(),
        format!("{} enabled worker account(s)", target_accounts.len()),
    ));
    checks.push(copy_shadow_smoke_check(
        "local_account",
        local_account.is_some(),
        local_account_id
            .as_deref()
            .map(|account_id| format!("local dry-run account {account_id} is configured"))
            .unwrap_or_else(|| "no local dry-run account is available".to_string()),
    ));
    checks.push(copy_shadow_smoke_check(
        "leaders_configured",
        !leaders.is_empty(),
        format!("{} leader watch target(s)", leaders.len()),
    ));
    checks.push(copy_shadow_smoke_check(
        "watcher_subscriptions",
        !watcher_subscriptions.is_empty(),
        format!(
            "{} read-only websocket subscription payload(s) prepared",
            watcher_subscriptions.len()
        ),
    ));

    let Some(account) = local_account else {
        return copy_shadow_watch_report(
            config,
            options,
            CopyShadowWatchReportInput::new(CopyShadowWatchReportBase {
                environment,
                ws_url,
                local_account_id,
                target_accounts,
                leaders,
                watcher_subscriptions,
                checks,
                watcher_status: "not_started_no_local_account".to_string(),
            }),
        );
    };
    if watcher_leaders.is_empty() {
        return copy_shadow_watch_report(
            config,
            options,
            CopyShadowWatchReportInput::new(CopyShadowWatchReportBase {
                environment,
                ws_url,
                local_account_id,
                target_accounts,
                leaders,
                watcher_subscriptions,
                checks,
                watcher_status: "not_started_no_leaders".to_string(),
            }),
        );
    }

    let strategy = strategies::smart_money::SmartMoneyCopyStrategy::new(
        strategies::smart_money::SmartMoneyCopyConfig {
            strategy_id: "copy_shadow_watch".to_string(),
            default_copy_ratio: 1.0,
            max_slippage_bps: 25.0,
            leaders: watcher_leaders
                .iter()
                .map(|leader| strategies::smart_money::LeaderRule {
                    leader_id: leader.leader_id.clone(),
                    leader_address: leader.leader_address.clone(),
                    enabled: true,
                    copy_ratio: 1.0,
                })
                .collect(),
            symbol_limits: Vec::new(),
        },
    );
    let mut pipeline = strategies::smart_money::CopyDryRunShadowPipeline::new(
        strategies::smart_money::CopyDryRunShadowConfig {
            local_account_id: account.account_id.clone(),
            target_accounts: target_accounts.clone(),
            signal_ttl_ms: config.process.signal_ttl_ms,
            max_signal_delay_ms: copy_daemon_max_signal_delay_ms(config),
            account_copy_ratio: account.copy_ratio,
            principal_cap_usd: account.max_order_notional_usd
                / strategies::smart_money::COPY_MAX_LEVERAGE.max(1.0),
            leverage: strategies::smart_money::COPY_MAX_LEVERAGE,
            max_signal_notional_usd: Some(account.max_order_notional_usd),
            exchange_min_open_notional_usd: trading::HYPERLIQUID_MIN_ORDER_NOTIONAL_USD,
            allow_short: true,
            max_effective_exposure_usd: Some(account.max_order_notional_usd),
            blocked_symbols: config.module_blocked_symbols("copy").to_vec(),
            live_gate: strategies::smart_money::CopyLiveGateInput {
                process_dry_run: true,
                live_copy_enabled: false,
                account_worker_live: false,
            },
        },
        strategy,
        strategies::smart_money::CopyLedger::new(),
    );

    let (sender, mut receiver) = tokio::sync::mpsc::channel(1024);
    let watcher_config = strategies::smart_money::ReadOnlyLeaderWatcherConfig {
        environment: environment.clone(),
        ws_url: ws_url.clone(),
        dex: None,
        leaders: watcher_leaders,
    };
    let watcher_handle = tokio::spawn(async move {
        strategies::smart_money::run_read_only_leader_watcher_once(watcher_config, sender).await
    });

    let started = Instant::now();
    let deadline = started + Duration::from_secs(options.duration_secs);
    let mut input = CopyShadowWatchReportInput::new(CopyShadowWatchReportBase {
        environment,
        ws_url,
        local_account_id,
        target_accounts,
        leaders,
        watcher_subscriptions,
        checks,
        watcher_status: "completed_duration".to_string(),
    });

    loop {
        if input.events_received >= options.max_events {
            input.watcher_status = "stopped_max_events".to_string();
            break;
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            break;
        };
        match tokio::time::timeout(remaining, receiver.recv()).await {
            Ok(Some(event)) => {
                input.events_received += 1;
                count_copy_shadow_watch_event(&mut input, &event);
                let now = domain::now_ms();
                let records = pipeline.handle_watcher_event(event, now);
                if !records.is_empty() {
                    strategies::smart_money::append_copy_shadow_history_records(
                        &options.shadow_history_path,
                        &records,
                        now,
                    )?;
                    input.shadow_records_written += records.len();
                }
            }
            Ok(None) => {
                input.watcher_status = "watcher_channel_closed".to_string();
                break;
            }
            Err(_) => break,
        }
    }

    if !watcher_handle.is_finished() && input.events_received > 0 {
        input.watcher_status = if input.events_received >= options.max_events {
            "stopped_max_events".to_string()
        } else {
            "completed_duration_watcher_open".to_string()
        };
    }

    if watcher_handle.is_finished() {
        match watcher_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                input.checks.push(copy_shadow_smoke_check(
                    "watcher_runtime",
                    false,
                    error.to_string(),
                ));
                input.watcher_status = "watcher_error".to_string();
            }
            Err(error) => {
                input.checks.push(copy_shadow_smoke_check(
                    "watcher_runtime",
                    false,
                    error.to_string(),
                ));
                input.watcher_status = "watcher_join_error".to_string();
            }
        }
    } else {
        watcher_handle.abort();
    }

    input.elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    copy_shadow_watch_report(config, options, input)
}

async fn run_copy_execution_canary(
    config: &config::AppConfig,
    options: CopyExecutionCanaryOptions,
) -> Result<CopyExecutionCanaryReport> {
    anyhow::ensure!(
        options.leader_size > 0.0,
        "leader_size must be positive for copy execution canary"
    );
    anyhow::ensure!(
        options.leader_notional_usd > 0.0,
        "leader_notional_usd must be positive for copy execution canary"
    );
    anyhow::ensure!(
        options.max_orders > 0,
        "max_orders must be positive for copy execution canary"
    );
    anyhow::ensure!(
        options.cleanup_max_slippage_bps.is_finite()
            && (0.0..10_000.0).contains(&options.cleanup_max_slippage_bps),
        "cleanup_max_slippage_bps must be >= 0 and < 10000"
    );
    anyhow::ensure!(
        !options.preflight_only || options.live,
        "copy execution canary --preflight-only requires --live true"
    );

    let leaders = parse_copy_shadow_smoke_leaders(&options.leaders)?;
    let leader = leaders.first().cloned();
    let enabled_accounts = config
        .enabled_worker_accounts()
        .map(|account| account.account_id.clone())
        .collect::<Vec<_>>();
    let target_accounts = copy_execution_canary_target_accounts(
        config,
        &options.account_ids,
        options.local_account_id.as_deref(),
    );
    let execution_dry_run = !options.live;

    let mut checks = Vec::new();
    checks.push(copy_shadow_smoke_check(
        "leaders_configured",
        leader.is_some(),
        format!(
            "{} leader target(s), first leader is used for canary",
            leaders.len()
        ),
    ));
    checks.push(copy_shadow_smoke_check(
        "enabled_worker_accounts",
        !enabled_accounts.is_empty(),
        format!("{} enabled worker account(s)", enabled_accounts.len()),
    ));
    checks.push(copy_shadow_smoke_check(
        "target_accounts",
        !target_accounts.is_empty(),
        format!("{} canary target account(s)", target_accounts.len()),
    ));
    for account_id in &target_accounts {
        let account = config.account(account_id);
        checks.push(copy_shadow_smoke_check(
            &format!("account_configured_{account_id}"),
            account.is_some_and(|account| account.enabled && account.worker_enabled),
            format!("{account_id} must be configured, enabled, and worker_enabled"),
        ));
    }
    checks.push(copy_shadow_smoke_check(
        "max_orders_positive",
        options.max_orders > 0,
        format!("max_orders={}", options.max_orders),
    ));

    if options.live {
        checks.extend(copy_execution_canary_live_checks(
            config,
            &options,
            &target_accounts,
        ));
    } else {
        checks.push(copy_shadow_smoke_check(
            "dry_run_execution",
            true,
            "copy execution canary defaults to AccountExecutor::dry_run and cannot submit exchange actions",
        ));
    }

    if !checks.iter().all(|check| check.ok) {
        return Ok(copy_execution_canary_report(
            config,
            &options,
            execution_dry_run,
            target_accounts,
            leader,
            checks,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ));
    }

    let leader = leader.context("leader check should prevent missing leader")?;
    let mut records = Vec::new();
    for account_id in &target_accounts {
        let account = config
            .account(account_id)
            .with_context(|| format!("account {account_id} disappeared after validation"))?;
        records.extend(build_synthetic_copy_shadow_records(
            config,
            &options,
            account,
            &leader,
            std::slice::from_ref(&account.account_id),
        ));
    }

    let approved_records = records
        .iter()
        .filter(|record| {
            record.signal.is_some()
                && matches!(
                    record.risk_decision,
                    strategies::smart_money::CopySignalRiskDecision::Approved { .. }
                )
        })
        .count();
    checks.push(copy_shadow_smoke_check(
        "approved_shadow_records",
        approved_records > 0,
        format!("{approved_records} approved shadow record(s)"),
    ));
    checks.push(copy_shadow_smoke_check(
        "max_orders_guard",
        approved_records <= options.max_orders,
        format!(
            "{approved_records} approved shadow record(s) must be <= max_orders {}",
            options.max_orders
        ),
    ));

    if !records.is_empty() {
        strategies::smart_money::append_copy_shadow_history_records(
            &options.shadow_history_path,
            &records,
            domain::now_ms(),
        )?;
    }

    if !checks.iter().all(|check| check.ok) {
        return Ok(copy_execution_canary_report(
            config,
            &options,
            execution_dry_run,
            target_accounts,
            Some(leader),
            checks,
            records,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ));
    }

    let would_submit_orders = plan_copy_canary_orders(config, &records, execution_dry_run)?;
    checks.push(copy_shadow_smoke_check(
        "would_submit_orders",
        !would_submit_orders.is_empty(),
        format!("{} approved order plan(s)", would_submit_orders.len()),
    ));
    let max_would_submit_notional = would_submit_orders
        .iter()
        .filter(|order| !order.reduce_only)
        .map(|order| order.notional_usd)
        .fold(0.0_f64, f64::max);
    if options.live && options.cleanup_after_submit {
        checks.push(copy_shadow_smoke_check(
            "cleanup_notional_limit",
            max_would_submit_notional <= config.manual_ops.max_manual_order_notional_usd,
            format!(
                "max planned open notional {max_would_submit_notional:.6} must be <= manual_ops.max_manual_order_notional_usd {:.6} so bundled reduce-only cleanup cannot be blocked",
                config.manual_ops.max_manual_order_notional_usd
            ),
        ));
    }

    if !checks.iter().all(|check| check.ok) {
        return Ok(copy_execution_canary_report(
            config,
            &options,
            execution_dry_run,
            target_accounts,
            Some(leader),
            checks,
            records,
            would_submit_orders,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ));
    }

    let reports = if options.preflight_only {
        Vec::new()
    } else {
        execute_copy_canary_records(config, &records, execution_dry_run, options.live).await?
    };
    let order_evidence = if options.live && copy_canary_has_live_submission(&reports) {
        collect_copy_canary_order_evidence(config, &reports).await
    } else {
        Vec::new()
    };
    let (cleanup_runbooks, cleanup_errors) =
        if options.live && copy_canary_has_live_submission(&reports) {
            execute_copy_canary_cleanup_runbooks(config, &options, &reports).await
        } else {
            (Vec::new(), Vec::new())
        };

    if !cleanup_errors.is_empty() {
        checks.push(copy_shadow_smoke_check(
            "cleanup_runbook_completed",
            false,
            cleanup_errors.join("; "),
        ));
    } else {
        checks.push(copy_shadow_smoke_check(
            "cleanup_runbook_completed",
            !options.live
                || cleanup_runbooks
                    .iter()
                    .all(copy_execution_canary_cleanup_runbook_ok),
            if options.preflight_only {
                "preflight-only canary did not submit live orders or cleanup runbooks".to_string()
            } else if options.live {
                format!("{} cleanup runbook(s) completed", cleanup_runbooks.len())
            } else {
                "dry-run canary does not submit live orders or cleanup runbooks".to_string()
            },
        ));
    };

    Ok(copy_execution_canary_report(
        config,
        &options,
        execution_dry_run,
        target_accounts,
        Some(leader),
        checks,
        records,
        would_submit_orders,
        reports,
        order_evidence,
        cleanup_runbooks,
        cleanup_errors,
    ))
}

fn copy_execution_canary_target_accounts(
    config: &config::AppConfig,
    requested: &[String],
    local_account_id: Option<&str>,
) -> Vec<String> {
    if !requested.is_empty() {
        return requested.to_vec();
    }
    if let Some(account_id) = local_account_id {
        return vec![account_id.to_string()];
    }
    config
        .enabled_worker_accounts()
        .next()
        .map(|account| vec![account.account_id.clone()])
        .unwrap_or_default()
}

fn copy_execution_canary_live_checks(
    config: &config::AppConfig,
    options: &CopyExecutionCanaryOptions,
    target_accounts: &[String],
) -> Vec<CopyShadowSmokeCheck> {
    vec![
        copy_shadow_smoke_check(
            "live_config_not_dry_run",
            !config.app.dry_run,
            "live copy canary requires app.dry_run=false",
        ),
        copy_shadow_smoke_check(
            "allow_live_submit",
            options.allow_live_submit,
            "live copy canary requires --allow-live-submit true",
        ),
        copy_shadow_smoke_check(
            "single_account_live_canary",
            target_accounts.len() == 1,
            "live copy canary is restricted to exactly one account",
        ),
        copy_shadow_smoke_check(
            "single_order_live_canary",
            options.max_orders == 1,
            "live copy canary is restricted to --max-orders 1",
        ),
        copy_shadow_smoke_check(
            "cleanup_after_submit",
            options.cleanup_after_submit,
            "live copy canary requires --cleanup-after-submit true so reduce-only cleanup is bundled",
        ),
        copy_shadow_smoke_check(
            "cleanup_slippage_valid",
            options.cleanup_max_slippage_bps.is_finite()
                && (0.0..10_000.0).contains(&options.cleanup_max_slippage_bps),
            format!(
                "cleanup_max_slippage_bps={} must be >= 0 and < 10000",
                options.cleanup_max_slippage_bps
            ),
        ),
        copy_shadow_smoke_check(
            "preflight_only_mode",
            !options.preflight_only || options.live,
            "copy execution canary preflight-only mode is only meaningful with --live true",
        ),
        copy_shadow_smoke_check(
            "mainnet_confirmation",
            config.app.environment != "mainnet" || options.confirm_mainnet_live,
            "mainnet live copy canary requires --confirm-mainnet-live true",
        ),
        copy_shadow_smoke_check(
            "manual_live_enabled",
            config.manual_ops.manual_live_enabled,
            "live copy canary uses the same manual live gate as signed smoke",
        ),
        copy_shadow_smoke_check(
            "mainnet_live_enabled",
            config.app.environment != "mainnet" || config.manual_ops.mainnet_live_enabled,
            "mainnet live copy canary requires manual_ops.mainnet_live_enabled=true",
        ),
    ]
}

fn run_copy_live_daemon_acceptance(
    config: &config::AppConfig,
    options: CopyLiveDaemonAcceptanceOptions,
) -> Result<CopyLiveDaemonAcceptanceReport> {
    anyhow::ensure!(
        options.leader_size > 0.0,
        "leader_size must be positive for copy live daemon acceptance"
    );
    anyhow::ensure!(
        options.leader_notional_usd > 0.0,
        "leader_notional_usd must be positive for copy live daemon acceptance"
    );
    anyhow::ensure!(
        options.max_slippage_bps.is_finite() && (0.0..10_000.0).contains(&options.max_slippage_bps),
        "max_slippage_bps must be >= 0 and < 10000"
    );
    anyhow::ensure!(
        options.max_total_notional_usd.is_finite() && options.max_total_notional_usd > 0.0,
        "max_total_notional_usd must be positive"
    );
    anyhow::ensure!(
        options.max_total_fees_usd.is_finite() && options.max_total_fees_usd >= 0.0,
        "max_total_fees_usd must be non-negative"
    );

    let leaders = parse_copy_shadow_smoke_leaders(&options.leaders)?;
    let target_accounts = copy_execution_canary_target_accounts(config, &options.account_ids, None);
    let persistence =
        strategies::smart_money::load_copy_persistence_snapshot(&options.persistence_path)?;
    let restart_probe = copy_live_daemon_restart_dedupe_probe(config, &options, &persistence)?;
    let would_submit_orders = copy_live_daemon_synthetic_would_submit_orders(
        config,
        &options,
        &leaders,
        &target_accounts,
        true,
    )?;

    let approved_shadow_records = would_submit_orders.len();
    let planned_notional = would_submit_orders
        .iter()
        .map(|order| order.notional_usd.max(0.0))
        .sum::<f64>();
    let all_cloids_present = would_submit_orders
        .iter()
        .all(|order| !order.cloid.trim().is_empty());

    let mut checks = Vec::new();
    checks.push(copy_shadow_smoke_check(
        "leaders_configured",
        !leaders.is_empty(),
        format!("{} leader watch target(s)", leaders.len()),
    ));
    checks.push(copy_shadow_smoke_check(
        "target_accounts",
        !target_accounts.is_empty(),
        format!("{} target account(s)", target_accounts.len()),
    ));
    for account_id in &target_accounts {
        let account = config.account(account_id);
        checks.push(copy_shadow_smoke_check(
            &format!("account_configured_{account_id}"),
            account.is_some_and(|account| account.enabled && account.worker_enabled),
            format!("{account_id} must be configured, enabled, and worker_enabled"),
        ));
    }
    checks.push(copy_shadow_smoke_check(
        "bounded_duration",
        (1..=3_600).contains(&options.max_duration_secs),
        format!(
            "max_duration_secs={} must be between 1 and 3600 for acceptance",
            options.max_duration_secs
        ),
    ));
    let min_live_orders_for_accounts = target_accounts.len().max(1);
    let max_live_orders_for_acceptance = min_live_orders_for_accounts.saturating_mul(3).max(3);
    checks.push(copy_shadow_smoke_check(
        "bounded_live_orders",
        options.max_live_orders >= min_live_orders_for_accounts
            && options.max_live_orders <= max_live_orders_for_acceptance,
        format!(
            "max_live_orders={} must be between {} and {} for {} selected account(s)",
            options.max_live_orders,
            min_live_orders_for_accounts,
            max_live_orders_for_acceptance,
            target_accounts.len()
        ),
    ));
    checks.push(copy_shadow_smoke_check(
        "bounded_total_notional",
        planned_notional <= options.max_total_notional_usd,
        format!(
            "planned_notional_usd={planned_notional:.6} must be <= max_total_notional_usd={:.6}",
            options.max_total_notional_usd
        ),
    ));
    checks.push(copy_shadow_smoke_check(
        "bounded_total_fees",
        options.max_total_fees_usd <= 1.0,
        format!(
            "max_total_fees_usd={} must be <= 1.0 for acceptance",
            options.max_total_fees_usd
        ),
    ));
    checks.push(copy_shadow_smoke_check(
        "bounded_slippage",
        options.max_slippage_bps <= 100.0,
        format!(
            "max_slippage_bps={} must be <= 100 for acceptance",
            options.max_slippage_bps
        ),
    ));
    checks.push(copy_shadow_smoke_check(
        "cleanup_policy",
        options.require_cleanup_after_submit,
        "unattended live acceptance requires automatic reduce-only cleanup policy",
    ));
    checks.push(copy_shadow_smoke_check(
        "flat_reconcile_policy",
        options.require_flat_reconcile_after_submit,
        "unattended live acceptance requires post-submit flat/no-open-order reconciliation",
    ));
    checks.push(copy_shadow_smoke_check(
        "kill_switch_policy",
        config.risk.global.allow_reduce_only_when_killed,
        "global kill switch must allow reduce-only cleanup while blocking new opens",
    ));
    checks.push(copy_shadow_smoke_check(
        "persistence_readable",
        true,
        format!(
            "loaded persistence snapshot from {} with {} seen keys and {} ledger entries",
            options.persistence_path.display(),
            persistence.seen_event_keys.len(),
            persistence.ledger_entries.len()
        ),
    ));
    checks.push(copy_shadow_smoke_check(
        "restart_dedupe_probe",
        restart_probe.first_emit_count > 0
            && restart_probe.replay_emit_count == 0
            && restart_probe.fresh_after_restart_emit_count > 0
            && restart_probe.loaded_seen_event_keys >= restart_probe.saved_seen_event_keys,
        format!(
            "first_emit={} replay_emit={} fresh_after_restart={} loaded_seen_keys={}",
            restart_probe.first_emit_count,
            restart_probe.replay_emit_count,
            restart_probe.fresh_after_restart_emit_count,
            restart_probe.loaded_seen_event_keys
        ),
    ));
    checks.push(copy_shadow_smoke_check(
        "would_submit_orders",
        !would_submit_orders.is_empty(),
        format!("{} approved order plan(s)", would_submit_orders.len()),
    ));
    checks.push(copy_shadow_smoke_check(
        "max_live_order_count",
        would_submit_orders.len() <= options.max_live_orders,
        format!(
            "{} planned order(s) must be <= max_live_orders {}",
            would_submit_orders.len(),
            options.max_live_orders
        ),
    ));
    checks.push(copy_shadow_smoke_check(
        "cloid_plan",
        all_cloids_present,
        "every planned order has a deterministic cloid for order ownership/status lookup",
    ));

    if options.live {
        checks.extend(copy_live_daemon_live_checks(config, &options));
    } else {
        checks.push(copy_shadow_smoke_check(
            "dry_run_acceptance",
            config.app.dry_run,
            "dry-run acceptance does not submit exchange orders or load Vault secrets",
        ));
    }

    let ok = checks.iter().all(|check| check.ok);
    let next_actions = if ok && options.live {
        vec![
            "Acceptance gate passed for live-capable configuration; run only a bounded canary-live window and require immediate cleanup/reconcile evidence.".to_string(),
        ]
    } else if ok {
        vec![
            "Dry-run daemon acceptance gate passed; rerun against live-capable config with --live true for a no-submit gate review before any daemon submit.".to_string(),
        ]
    } else {
        vec![
            "Do not start unattended live copy; fix failed acceptance checks and rerun this command.".to_string(),
        ]
    };

    Ok(CopyLiveDaemonAcceptanceReport {
        ok,
        mode: if options.live {
            "copy_live_daemon_acceptance_live_gate".to_string()
        } else {
            "copy_live_daemon_acceptance_dry_run".to_string()
        },
        environment: config.app.environment.clone(),
        live_requested: options.live,
        live_submit_allowed: options.allow_live_submit,
        confirm_mainnet_live: options.confirm_mainnet_live,
        max_duration_secs: options.max_duration_secs,
        max_live_orders: options.max_live_orders,
        max_total_notional_usd: options.max_total_notional_usd,
        max_total_fees_usd: options.max_total_fees_usd,
        max_slippage_bps: options.max_slippage_bps,
        require_cleanup_after_submit: options.require_cleanup_after_submit,
        require_flat_reconcile_after_submit: options.require_flat_reconcile_after_submit,
        target_accounts,
        leaders,
        checks,
        persistence_path: options.persistence_path.display().to_string(),
        shadow_history_path: options.shadow_history_path.display().to_string(),
        persistence_seen_keys_before: persistence.seen_event_keys.len(),
        persistence_ledger_entries_before: persistence.ledger_entries.len(),
        restart_dedupe_probe: restart_probe,
        shadow_records_written: approved_shadow_records,
        approved_shadow_records,
        would_submit_orders,
        next_actions,
    })
}

fn copy_live_daemon_live_checks(
    config: &config::AppConfig,
    options: &CopyLiveDaemonAcceptanceOptions,
) -> Vec<CopyShadowSmokeCheck> {
    vec![
        copy_shadow_smoke_check(
            "live_config_not_dry_run",
            !config.app.dry_run,
            "live daemon acceptance requires app.dry_run=false",
        ),
        copy_shadow_smoke_check(
            "allow_live_submit",
            options.allow_live_submit,
            "live daemon acceptance requires --allow-live-submit true",
        ),
        copy_shadow_smoke_check(
            "mainnet_confirmation",
            config.app.environment != "mainnet" || options.confirm_mainnet_live,
            "mainnet live daemon acceptance requires --confirm-mainnet-live true",
        ),
        copy_shadow_smoke_check(
            "manual_live_enabled",
            config.manual_ops.manual_live_enabled,
            "live daemon acceptance uses the same manual live gate as signed smoke",
        ),
        copy_shadow_smoke_check(
            "mainnet_live_enabled",
            config.app.environment != "mainnet" || config.manual_ops.mainnet_live_enabled,
            "mainnet live daemon acceptance requires manual_ops.mainnet_live_enabled=true",
        ),
    ]
}

async fn run_copy_live_daemon_supervisor(
    config: &config::AppConfig,
    options: CopyLiveDaemonSupervisorOptions,
) -> Result<CopyLiveDaemonSupervisorReport> {
    anyhow::ensure!(
        options.duration_secs > 0,
        "duration_secs must be positive for copy live daemon supervisor"
    );
    anyhow::ensure!(
        options.max_events > 0,
        "max_events must be positive for copy live daemon supervisor"
    );
    let target_accounts = copy_execution_canary_target_accounts(config, &options.account_ids, None);
    let market_scope = copy_daemon_normalize_market_scope(&options.markets);
    let acceptance_options = CopyLiveDaemonAcceptanceOptions {
        leaders: options.leaders.clone(),
        account_ids: target_accounts.clone(),
        coin: options.coin.clone(),
        side: options.side,
        persistence_path: copy_live_daemon_supervisor_sidecar_path(
            &options.persistence_path,
            "acceptance",
        ),
        shadow_history_path: copy_live_daemon_supervisor_sidecar_path(
            &options.shadow_history_path,
            "acceptance",
        ),
        leader_notional_usd: options.leader_notional_usd,
        leader_size: options.leader_size,
        live: options.live_gate,
        allow_live_submit: options.allow_live_submit,
        confirm_mainnet_live: options.confirm_mainnet_live,
        max_duration_secs: options.duration_secs,
        max_live_orders: options.max_live_orders,
        max_total_notional_usd: options.max_total_notional_usd,
        max_total_fees_usd: options.max_total_fees_usd,
        max_slippage_bps: options.max_slippage_bps,
        require_cleanup_after_submit: true,
        require_flat_reconcile_after_submit: true,
    };
    let acceptance = run_copy_live_daemon_acceptance(config, acceptance_options)?;
    let leaders = parse_copy_shadow_smoke_leaders(&options.leaders)?;
    let watcher_leaders = leaders
        .iter()
        .map(|leader| strategies::smart_money::SmartMoneyLeaderWatch {
            leader_id: leader.leader_id.clone(),
            leader_address: leader.leader_address.clone(),
        })
        .collect::<Vec<_>>();
    let watcher_subscriptions =
        strategies::smart_money::read_only_leader_watcher_subscriptions(&watcher_leaders, None);
    let local_account_id = options
        .local_account_id
        .clone()
        .or_else(|| target_accounts.first().cloned());
    let local_account = local_account_id
        .as_deref()
        .and_then(|account_id| config.account(account_id));
    let persistence =
        strategies::smart_money::load_copy_persistence_snapshot(&options.persistence_path)?;
    let persistence_seen_keys_before = persistence.seen_event_keys.len();
    let persistence_ledger_entries_before = persistence.ledger_entries.len();

    let environment = options
        .environment
        .clone()
        .unwrap_or_else(|| config.app.environment.clone());
    let ws_url = options.ws_url.clone().or_else(|| {
        options
            .environment
            .is_none()
            .then(|| config.hyperliquid.ws_url.clone())
    });
    let mut checks = vec![
        copy_shadow_smoke_check(
            "submit_mode",
            !options.submit || (options.live_gate && options.allow_live_submit),
            if options.submit {
                "copy-live-daemon-supervisor submit mode requires live_gate and allow_live_submit"
                    .to_string()
            } else {
                "copy-live-daemon-supervisor is running in no-submit observation mode".to_string()
            },
        ),
        copy_shadow_smoke_check(
            "acceptance_gate",
            acceptance.ok,
            format!("copy-live-daemon-acceptance ok={}", acceptance.ok),
        ),
        copy_shadow_smoke_check(
            "target_accounts",
            !target_accounts.is_empty(),
            format!("{} target account(s)", target_accounts.len()),
        ),
        copy_shadow_smoke_check(
            "local_account",
            local_account.is_some(),
            local_account_id
                .as_deref()
                .map(|account_id| format!("local supervisor account {account_id} is configured"))
                .unwrap_or_else(|| "no local supervisor account is available".to_string()),
        ),
        copy_shadow_smoke_check(
            "leaders_configured",
            !watcher_leaders.is_empty(),
            format!("{} leader watch target(s)", watcher_leaders.len()),
        ),
        copy_shadow_smoke_check(
            "watcher_subscriptions",
            !watcher_subscriptions.is_empty(),
            format!(
                "{} read-only websocket subscription payload(s) prepared",
                watcher_subscriptions.len()
            ),
        ),
        copy_shadow_smoke_check(
            "bounded_events",
            options.max_events <= 100_000,
            format!("max_events={} must be <= 100000", options.max_events),
        ),
        copy_shadow_smoke_check(
            "market_scope",
            !market_scope.is_empty(),
            format!(
                "copy entry markets={}; unselected markets are exit-only",
                market_scope.join(",")
            ),
        ),
    ];
    checks.extend(copy_live_daemon_signer_preflight_checks(
        config,
        &target_accounts,
        options.submit,
    ));

    let mut input = CopyShadowWatchReportInput::new(CopyShadowWatchReportBase {
        environment: environment.clone(),
        ws_url: ws_url.clone(),
        local_account_id: local_account_id.clone(),
        target_accounts: target_accounts.clone(),
        leaders: leaders.clone(),
        watcher_subscriptions: watcher_subscriptions.clone(),
        checks: Vec::new(),
        watcher_status: "not_started".to_string(),
    });
    let mut approved_records = Vec::new();
    let mut would_submit_plan_refs = Vec::new();
    let recovered_pending_reduce_refs =
        copy_live_daemon_recover_pending_reduce_plan_refs(&persistence, &target_accounts);
    if !recovered_pending_reduce_refs.is_empty() {
        checks.push(copy_shadow_smoke_check(
            "recovered_pending_reduce_refs",
            true,
            format!(
                "{} persisted pending reduce ref(s) recovered for retry",
                recovered_pending_reduce_refs.len()
            ),
        ));
        append_unique_copy_daemon_would_submit_refs(
            &mut would_submit_plan_refs,
            recovered_pending_reduce_refs,
        );
    }
    let mut submitted_plan_cloids = HashSet::new();
    let mut live_submit_chunks = Vec::new();
    let mut pending_unclassified_fill_count = 0usize;
    let mut pending_unclassified_fill_labels = Vec::new();
    let mut stop_for_submit_candidate = false;
    let started = Instant::now();

    if acceptance.ok && local_account.is_some() && !watcher_leaders.is_empty() {
        let mut pipelines = target_accounts
            .iter()
            .filter_map(|account_id| {
                config.account(account_id).map(|account| {
                    (
                        account.account_id.clone(),
                        copy_live_daemon_supervisor_pipeline(
                            config,
                            &options,
                            account,
                            std::slice::from_ref(&account.account_id),
                            &watcher_leaders,
                            &persistence,
                        ),
                    )
                })
            })
            .collect::<Vec<_>>();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1024);
        let watcher_config = strategies::smart_money::ReadOnlyLeaderWatcherConfig {
            environment: environment.clone(),
            ws_url: ws_url.clone(),
            dex: None,
            leaders: watcher_leaders,
        };
        let watcher_handle = tokio::spawn(async move {
            strategies::smart_money::run_read_only_leader_watcher_once(watcher_config, sender).await
        });
        let deadline = started + Duration::from_secs(options.duration_secs);
        input.watcher_status = "completed_duration".to_string();

        loop {
            if input.events_received >= options.max_events {
                input.watcher_status = "stopped_max_events".to_string();
                break;
            }
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            match tokio::time::timeout(remaining, receiver.recv()).await {
                Ok(Some(event)) => {
                    input.events_received += 1;
                    count_copy_shadow_watch_event(&mut input, &event);
                    let now = domain::now_ms();
                    let mut records = Vec::new();
                    for (_, pipeline) in &mut pipelines {
                        records.extend(pipeline.handle_watcher_event(event.clone(), now));
                    }
                    if !records.is_empty() {
                        strategies::smart_money::append_copy_shadow_history_records(
                            &options.shadow_history_path,
                            &records,
                            now,
                        )?;
                        input.shadow_records_written += records.len();
                        let new_approved_records = records
                            .iter()
                            .filter(|record| {
                                matches!(
                                    record.risk_decision,
                                    strategies::smart_money::CopySignalRiskDecision::Approved { .. }
                                ) && record.signal.is_some()
                            })
                            .cloned()
                            .collect::<Vec<_>>();
                        let base_record_index = approved_records.len();
                        let new_would_submit_refs =
                            plan_copy_daemon_acceptance_order_refs_with_offset(
                                config,
                                &new_approved_records,
                                base_record_index,
                            )?;
                        append_unique_copy_daemon_would_submit_refs(
                            &mut would_submit_plan_refs,
                            new_would_submit_refs,
                        );
                        approved_records.extend(new_approved_records);
                        if options.submit {
                            let pending_candidate_plan_refs = copy_live_daemon_pending_plan_refs(
                                &would_submit_plan_refs,
                                &submitted_plan_cloids,
                            );
                            if !pending_candidate_plan_refs.is_empty() {
                                stop_for_submit_candidate = true;
                                input.watcher_status = "stopped_after_submit_candidate".to_string();
                                break;
                            }
                        }
                    }
                }
                Ok(None) => {
                    input.watcher_status = "watcher_channel_closed".to_string();
                    break;
                }
                Err(_) => break,
            }
        }

        if !watcher_handle.is_finished()
            && input.events_received > 0
            && input.watcher_status == "completed_duration"
        {
            input.watcher_status = if input.events_received >= options.max_events {
                "stopped_max_events".to_string()
            } else {
                "completed_duration_watcher_open".to_string()
            };
        }
        if watcher_handle.is_finished() {
            match watcher_handle.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    let recoverable = copy_live_daemon_recoverable_watcher_error(&error);
                    checks.push(copy_shadow_smoke_check(
                        "watcher_runtime",
                        recoverable,
                        if recoverable {
                            format!(
                                "read-only watcher disconnected with recoverable error; outer soak loop may reconnect next round: {error:#}"
                            )
                        } else {
                            format!("read-only watcher failed: {error:#}")
                        },
                    ));
                    input.watcher_status = if recoverable {
                        "watcher_recoverable_disconnect".to_string()
                    } else {
                        "watcher_error".to_string()
                    };
                }
                Err(error) => {
                    checks.push(copy_shadow_smoke_check(
                        "watcher_join",
                        false,
                        format!("read-only watcher task join failed: {error}"),
                    ));
                    input.watcher_status = "watcher_join_error".to_string();
                }
            }
        } else {
            watcher_handle.abort();
        }

        let now = domain::now_ms();
        let mut snapshot = persistence.clone();
        pending_unclassified_fill_labels = Vec::new();
        for (account_id, pipeline) in &pipelines {
            snapshot = copy_live_daemon_merge_persistence_snapshots(
                snapshot,
                pipeline.persistence_snapshot(now),
            );
            pending_unclassified_fill_count += pipeline.pending_fill_count();
            pending_unclassified_fill_labels.extend(
                pipeline
                    .pending_fill_labels(20)
                    .into_iter()
                    .map(|label| format!("{account_id}:{label}")),
            );
        }
        pending_unclassified_fill_labels.truncate(20);
        let snapshot = copy_live_daemon_persistence_snapshot_for_save(snapshot);
        strategies::smart_money::save_copy_persistence_snapshot(
            &options.persistence_path,
            &snapshot,
        )?;
    } else if !acceptance.ok {
        input.watcher_status = "skipped_acceptance_failed".to_string();
    } else if local_account.is_none() {
        input.watcher_status = "skipped_missing_local_account".to_string();
    } else {
        input.watcher_status = "skipped_no_leaders".to_string();
    }

    input.elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    let saved = strategies::smart_money::load_copy_persistence_snapshot(&options.persistence_path)?;
    let preliminary_pending_would_submit_plan_refs =
        copy_live_daemon_pending_plan_refs(&would_submit_plan_refs, &submitted_plan_cloids);
    let pre_submit_accounts = if options.submit && stop_for_submit_candidate {
        let accounts =
            copy_live_daemon_accounts_for_refs(&preliminary_pending_would_submit_plan_refs);
        if accounts.is_empty() {
            target_accounts.clone()
        } else {
            accounts
        }
    } else {
        target_accounts.clone()
    };
    let pre_submit_reconciliations = reconcile_copy_bounded_window_accounts_bounded(
        config,
        &pre_submit_accounts,
        "pre_submit_reconcile",
    )
    .await;
    let saved = copy_live_daemon_prune_snapshot_against_live_markets(
        config,
        saved,
        &pre_submit_reconciliations,
    )
    .await;
    let saved = copy_live_daemon_recover_open_ledger_from_live_positions(
        saved,
        &pre_submit_reconciliations,
        &options,
    )?;
    strategies::smart_money::save_copy_persistence_snapshot(&options.persistence_path, &saved)?;
    let pending_would_submit_plan_refs =
        copy_live_daemon_pending_plan_refs(&would_submit_plan_refs, &submitted_plan_cloids);
    let (pending_would_submit_plan_refs, mut deferred_open_suppressed_refs) =
        copy_live_daemon_defer_open_refs_after_immediate_live_submit(
            pending_would_submit_plan_refs,
            &live_submit_chunks,
        );
    let account_symbol_caps = copy_live_daemon_account_symbol_caps(config, &target_accounts);
    let (executable_submit_plan_refs, mut suppressed_submit_plan_refs) =
        copy_live_daemon_executable_refs_for_snapshot_with_symbol_caps(
            &pending_would_submit_plan_refs,
            &options,
            &saved,
            &pre_submit_reconciliations,
            &account_symbol_caps,
        );
    suppressed_submit_plan_refs.append(&mut deferred_open_suppressed_refs);
    let (executable_submit_plan_refs, mut effective_min_suppressed_refs) =
        copy_live_daemon_suppress_refs_below_effective_min(
            config,
            &options,
            &executable_submit_plan_refs,
        )
        .await;
    suppressed_submit_plan_refs.append(&mut effective_min_suppressed_refs);
    let would_submit_orders = copy_live_daemon_order_refs_to_orders(&would_submit_plan_refs);
    let executable_would_submit_orders =
        copy_live_daemon_order_refs_to_orders(&executable_submit_plan_refs);
    let planned_notional_usd =
        copy_live_daemon_open_notional_usd_from_orders(&executable_would_submit_orders);
    let estimated_fees_usd = normalize_report_zero(planned_notional_usd * 0.001);
    let submit_plan_contract = copy_live_daemon_submit_plan_contract_with_snapshot_and_account_caps(
        &options,
        &executable_submit_plan_refs,
        &suppressed_submit_plan_refs,
        planned_notional_usd,
        estimated_fees_usd,
        &pre_submit_reconciliations,
        &saved,
        &account_symbol_caps,
    );
    let (
        executable_submit_plan_refs,
        mut contract_suppressed_submit_plan_refs,
        mut submit_plan_contract,
    ) = copy_live_daemon_suppress_refs_rejected_by_submit_contract(
        &options,
        executable_submit_plan_refs,
        suppressed_submit_plan_refs.clone(),
        planned_notional_usd,
        estimated_fees_usd,
        &pre_submit_reconciliations,
        Some(&saved),
        submit_plan_contract,
        &account_symbol_caps,
    );
    suppressed_submit_plan_refs.append(&mut contract_suppressed_submit_plan_refs);
    suppressed_submit_plan_refs = copy_live_daemon_pending_suppressed_refs(
        &suppressed_submit_plan_refs,
        &submitted_plan_cloids,
    );
    let executable_would_submit_orders =
        copy_live_daemon_order_refs_to_orders(&executable_submit_plan_refs);
    let mut suppressed_would_submit_orders = suppressed_submit_plan_refs
        .iter()
        .map(|suppressed| CopyLiveDaemonSuppressedWouldSubmit {
            order: suppressed.plan.order.clone(),
            reason_code: suppressed.reason_code.clone(),
            message: suppressed.message.clone(),
        })
        .collect::<Vec<_>>();
    let planned_notional_usd =
        copy_live_daemon_open_notional_usd_from_orders(&executable_would_submit_orders);
    let estimated_fees_usd = normalize_report_zero(planned_notional_usd * 0.001);
    let pending_final_executable_refs = executable_submit_plan_refs
        .iter()
        .filter(|plan| !submitted_plan_cloids.contains(&plan.order.cloid))
        .cloned()
        .collect::<Vec<_>>();
    let persistent_submit_dry_run = copy_live_daemon_persistent_submit_dry_run(
        config,
        &submit_plan_contract,
        &pending_final_executable_refs,
        &suppressed_submit_plan_refs,
        options.max_slippage_bps,
    );
    if options.submit && !pending_final_executable_refs.is_empty() {
        let final_live_submit = copy_live_daemon_persistent_live_submit(
            config,
            &options,
            &submit_plan_contract,
            &pending_final_executable_refs,
            &suppressed_submit_plan_refs,
            &saved,
            &approved_records,
        )
        .await;
        for plan in &pending_final_executable_refs {
            submitted_plan_cloids.insert(plan.order.cloid.clone());
        }
        live_submit_chunks.push(final_live_submit);
    } else if !options.submit {
        live_submit_chunks.push(
            copy_live_daemon_persistent_live_submit(
                config,
                &options,
                &submit_plan_contract,
                &pending_final_executable_refs,
                &suppressed_submit_plan_refs,
                &saved,
                &approved_records,
            )
            .await,
        );
    } else if live_submit_chunks.is_empty() {
        live_submit_chunks.push(
            copy_live_daemon_persistent_live_submit(
                config,
                &options,
                &submit_plan_contract,
                &[],
                &suppressed_submit_plan_refs,
                &saved,
                &approved_records,
            )
            .await,
        );
    }
    let persistent_live_submit = copy_live_daemon_merge_persistent_live_submit_reports(
        options.submit,
        submit_plan_contract.ok,
        live_submit_chunks,
    );
    let persistent_submitted_cloids =
        copy_live_daemon_submitted_report_cloids(&persistent_live_submit);
    if !persistent_submitted_cloids.is_empty() {
        suppressed_submit_plan_refs = copy_live_daemon_pending_suppressed_refs(
            &suppressed_submit_plan_refs,
            &persistent_submitted_cloids,
        );
        suppressed_would_submit_orders = suppressed_submit_plan_refs
            .iter()
            .map(|suppressed| CopyLiveDaemonSuppressedWouldSubmit {
                order: suppressed.plan.order.clone(),
                reason_code: suppressed.reason_code.clone(),
                message: suppressed.message.clone(),
            })
            .collect::<Vec<_>>();
        submit_plan_contract = copy_live_daemon_submit_plan_contract_with_snapshot_and_account_caps(
            &options,
            &executable_submit_plan_refs,
            &suppressed_submit_plan_refs,
            planned_notional_usd,
            estimated_fees_usd,
            &pre_submit_reconciliations,
            &saved,
            &account_symbol_caps,
        );
    }
    if options.submit
        && !persistent_live_submit.submitted_reports.is_empty()
        && copy_live_daemon_persistent_submit_snapshot_safe_to_save(&persistent_live_submit)
    {
        let existing_snapshot =
            strategies::smart_money::load_copy_persistence_snapshot(&options.persistence_path)?;
        let snapshot_for_save = copy_live_daemon_merge_persistence_snapshots_for_save(
            existing_snapshot,
            persistent_live_submit
                .ledger_reconciliation_snapshot
                .clone(),
        );
        strategies::smart_money::save_copy_persistence_snapshot(
            &options.persistence_path,
            &snapshot_for_save,
        )?;
    }
    let saved = strategies::smart_money::load_copy_persistence_snapshot(&options.persistence_path)?;
    checks.push(copy_shadow_smoke_check(
        "approved_records_have_submit_refs",
        would_submit_plan_refs.len() >= approved_records.len(),
        format!(
            "{} approved shadow record(s), {} submit plan ref(s)",
            approved_records.len(),
            would_submit_plan_refs.len()
        ),
    ));
    checks.push(copy_shadow_smoke_check(
        "max_live_order_count",
        executable_would_submit_orders
            .iter()
            .filter(|order| !order.reduce_only)
            .count()
            <= options.max_live_orders,
        format!(
            "{} executable open order(s), {} executable reduce-only close(s), {} suppressed candidate(s), max_live_orders {}",
            executable_would_submit_orders
                .iter()
                .filter(|order| !order.reduce_only)
                .count(),
            executable_would_submit_orders
                .iter()
                .filter(|order| order.reduce_only)
                .count(),
            suppressed_would_submit_orders.len(),
            options.max_live_orders
        ),
    ));
    checks.push(copy_shadow_smoke_check(
        "max_total_notional",
        planned_notional_usd <= options.max_total_notional_usd,
        format!(
            "planned_notional_usd={planned_notional_usd:.6} must be <= {:.6}",
            options.max_total_notional_usd
        ),
    ));
    checks.push(copy_shadow_smoke_check(
        "max_total_fees",
        estimated_fees_usd <= options.max_total_fees_usd,
        format!(
            "estimated_fees_usd={estimated_fees_usd:.6} must be <= {:.6}",
            options.max_total_fees_usd
        ),
    ));
    let live_submit_required = options.submit && !executable_would_submit_orders.is_empty();
    let persistent_live_submit_health_ok =
        copy_live_daemon_live_submit_health_ok(&persistent_live_submit);
    checks.push(copy_shadow_smoke_check(
        "exchange_submit_mode",
        !live_submit_required || persistent_live_submit_health_ok,
        if live_submit_required {
            format!(
                "submit requested; persistent_live_submit ok={} health_ok={} submitted_reports={} order_evidence={} cleanup_errors={}",
                persistent_live_submit.ok,
                persistent_live_submit_health_ok,
                persistent_live_submit.submitted_reports.len(),
                persistent_live_submit.order_evidence.len(),
                persistent_live_submit.cleanup_errors.len()
            )
        } else if options.submit {
            format!(
                "submit enabled but no executable order refs in this window; submitted_reports={} order_evidence={} cleanup_errors={}",
                persistent_live_submit.submitted_reports.len(),
                persistent_live_submit.order_evidence.len(),
                persistent_live_submit.cleanup_errors.len()
            )
        } else {
            "supervisor stayed no-submit; submitted reports are not produced".to_string()
        },
    ));
    checks.push(copy_live_daemon_watcher_progress_check(
        &input.watcher_status,
        input.events_received,
        options.duration_secs,
        input.elapsed_ms,
    ));
    checks.push(copy_shadow_smoke_check(
        "persistence_saved",
        saved.saved_at_ms > 0 || input.shadow_records_written == 0,
        format!(
            "saved_at_ms={} seen_keys={} ledger_entries={}",
            saved.saved_at_ms,
            saved.seen_event_keys.len(),
            saved.ledger_entries.len()
        ),
    ));
    let final_reconciliations =
        reconcile_copy_bounded_window_accounts_bounded(config, &target_accounts, "final_reconcile")
            .await;
    let final_reconcile_health_ok =
        copy_live_daemon_reconciliations_healthy_for_snapshot_with_account_caps(
            &options,
            &final_reconciliations,
            &saved,
            &account_symbol_caps,
        );
    checks.push(copy_shadow_smoke_check(
        "final_reconcile_health",
        final_reconcile_health_ok,
        copy_live_daemon_reconcile_health_detail_for_snapshot_with_account_caps(
            &options,
            &final_reconciliations,
            &saved,
            &account_symbol_caps,
        ),
    ));
    let submit_evidence_contract = copy_live_daemon_submit_evidence_contract(
        &options,
        &acceptance,
        &executable_would_submit_orders,
        planned_notional_usd,
        estimated_fees_usd,
        &final_reconciliations,
        options.submit.then_some(&persistent_live_submit),
        &account_symbol_caps,
    );
    let ok = copy_live_daemon_supervisor_ok(
        live_submit_required,
        acceptance.ok,
        &checks,
        final_reconcile_health_ok,
        persistent_live_submit_health_ok,
    );
    let next_actions = if ok && options.submit && options.hold_positions_after_submit {
        vec![
            "Follow-position live daemon window was healthy; submitted open positions are intentionally held until mapped target close signals arrive.".to_string(),
        ]
    } else if ok && options.submit && submit_evidence_contract.ready_for_unattended_submit {
        vec![
            "Persistent daemon submit bridge completed this bounded window; review submitted reports, evidence, cleanup, and final reconciliation before extending duration.".to_string(),
        ]
    } else if ok && options.submit {
        vec![submit_evidence_contract.blocker.clone().unwrap_or_else(|| {
            "Persistent daemon submit window was healthy, but no real submitted order evidence was produced yet; keep running during active leader trading before declaring unattended readiness.".to_string()
        })]
    } else if ok && !submit_evidence_contract.ready_for_unattended_submit {
        vec![
            submit_evidence_contract.blocker.clone().unwrap_or_else(|| {
                "No-submit daemon supervisor passed, but unattended submit remains gated until persistent submit evidence is wired.".to_string()
            }),
        ]
    } else if ok && input.shadow_records_written > 0 {
        vec![
            "No-submit daemon supervisor passed with real shadow records; review would_submit_orders and run a longer no-submit soak before enabling any submit path.".to_string(),
        ]
    } else if ok {
        vec![
            "No-submit daemon supervisor passed but saw no shadow records; run a longer soak during active leader trading.".to_string(),
        ]
    } else {
        vec![
            "Do not enable live submit; fix failed supervisor checks and rerun no-submit daemon supervisor.".to_string(),
        ]
    };

    Ok(CopyLiveDaemonSupervisorReport {
        ok,
        mode: if options.submit {
            "copy_live_daemon_supervisor_submit".to_string()
        } else {
            "copy_live_daemon_supervisor_no_submit".to_string()
        },
        environment,
        ws_url,
        no_submit: !options.submit,
        live_gate_requested: options.live_gate,
        live_submit_allowed: options.allow_live_submit,
        confirm_mainnet_live: options.confirm_mainnet_live,
        submit_requested: options.submit,
        hold_positions_after_submit: options.hold_positions_after_submit,
        cleanup_max_slippage_bps: options.cleanup_max_slippage_bps,
        duration_secs: options.duration_secs,
        elapsed_ms: input.elapsed_ms,
        max_events: options.max_events,
        max_live_orders: options.max_live_orders,
        max_total_notional_usd: options.max_total_notional_usd,
        max_total_fees_usd: options.max_total_fees_usd,
        max_slippage_bps: options.max_slippage_bps,
        selected_markets: market_scope,
        acceptance_coin: options.coin.clone(),
        target_accounts,
        local_account_id,
        leaders,
        checks,
        acceptance,
        watcher_subscriptions,
        persistence_path: options.persistence_path.display().to_string(),
        shadow_history_path: options.shadow_history_path.display().to_string(),
        persistence_seen_keys_before,
        persistence_seen_keys_after: saved.seen_event_keys.len(),
        persistence_ledger_entries_before,
        persistence_ledger_entries_after: saved.ledger_entries.len(),
        events_received: input.events_received,
        fill_events: input.fill_events,
        snapshot_fill_events: input.snapshot_fill_events,
        position_snapshot_events: input.position_snapshot_events,
        position_snapshots: input.position_snapshots,
        order_update_events: input.order_update_events,
        pending_unclassified_fill_count,
        pending_unclassified_fill_labels,
        shadow_records_written: input.shadow_records_written,
        approved_shadow_records: approved_records.len(),
        would_submit_orders,
        executable_would_submit_orders,
        suppressed_would_submit_orders,
        executable_submit_plan_refs,
        suppressed_submit_plan_refs,
        planned_notional_usd,
        estimated_fees_usd,
        submit_plan_contract,
        persistent_submit_dry_run,
        persistent_live_submit,
        submit_evidence_contract,
        watcher_status: input.watcher_status,
        final_reconciliations,
        next_actions,
    })
}

fn copy_live_daemon_supervisor_sidecar_path(path: &std::path::Path, suffix: &str) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("copy-live-daemon-supervisor");
    path.with_file_name(format!("{file_name}.{suffix}"))
}

fn normalize_report_zero(value: f64) -> f64 {
    if value.abs() <= f64::EPSILON {
        0.0
    } else {
        value
    }
}

#[cfg(test)]
fn copy_live_daemon_open_notional_usd_from_refs(refs: &[CopyLiveDaemonWouldSubmitRef]) -> f64 {
    normalize_report_zero(
        refs.iter()
            .filter(|plan| !plan.order.reduce_only)
            .map(|plan| plan.order.notional_usd.max(0.0))
            .sum::<f64>(),
    )
}

fn copy_live_daemon_open_notional_usd_from_orders(
    orders: &[CopyExecutionCanaryWouldSubmit],
) -> f64 {
    normalize_report_zero(
        orders
            .iter()
            .filter(|order| !order.reduce_only)
            .map(|order| order.notional_usd.max(0.0))
            .sum::<f64>(),
    )
}

fn copy_daemon_max_signal_delay_ms(config: &config::AppConfig) -> u64 {
    config
        .process
        .signal_ttl_ms
        .max(COPY_DAEMON_MIN_SIGNAL_DELAY_MS)
}

fn copy_live_daemon_live_submit_health_ok(
    report: &CopyLiveDaemonPersistentLiveSubmitReport,
) -> bool {
    if report.ok {
        return true;
    }
    if !report.cleanup_errors.is_empty() {
        return false;
    }
    let live_submitted = copy_canary_live_submitted_reports(&report.submitted_reports);
    let all_reports_accounted_for = report.submitted_reports.iter().all(|submitted| {
        matches!(submitted, domain::WorkerReport::Submitted(_))
            || copy_live_daemon_report_is_safe_pre_submit_skip(submitted)
            || copy_live_daemon_report_is_submit_transport_failure(submitted)
    });
    if !all_reports_accounted_for {
        return false;
    }
    let non_critical_checks_ok = report.checks.iter().all(|check| {
        check.ok
            || matches!(
                check.name.as_str(),
                "submitted_reports"
                    | "submit_transport_fail_closed"
                    | "persistent_live_submit_chunks"
            )
    });
    if !non_critical_checks_ok {
        return false;
    }
    live_submitted.is_empty()
        || (report.order_evidence.len() == live_submitted.len()
            && report
                .order_evidence
                .iter()
                .all(copy_execution_canary_order_evidence_ok))
}

fn copy_live_daemon_supervisor_ok(
    submit_requested: bool,
    acceptance_ok: bool,
    checks: &[CopyShadowSmokeCheck],
    final_reconcile_health_ok: bool,
    persistent_live_submit_ok: bool,
) -> bool {
    acceptance_ok
        && checks.iter().all(|check| check.ok)
        && final_reconcile_health_ok
        && (!submit_requested || persistent_live_submit_ok)
}

#[cfg(test)]
fn copy_live_daemon_reconcile_only_degraded_round(
    submit_requested: bool,
    checks: &[CopyShadowSmokeCheck],
    final_reconciliations: &[CopyBoundedLiveWindowReconcile],
    persistent_live_submit: &CopyLiveDaemonPersistentLiveSubmitReport,
) -> bool {
    if !submit_requested
        || !persistent_live_submit.submitted_reports.is_empty()
        || !persistent_live_submit.order_evidence.is_empty()
        || !persistent_live_submit.cleanup_errors.is_empty()
        || !persistent_live_submit.ledger_reconciliations.is_empty()
    {
        return false;
    }
    let failed_names = checks
        .iter()
        .filter(|check| !check.ok)
        .map(|check| check.name.as_str())
        .collect::<Vec<_>>();
    if failed_names.is_empty()
        || !failed_names
            .iter()
            .all(|name| matches!(*name, "exchange_submit_mode" | "final_reconcile_health"))
        || !failed_names.contains(&"final_reconcile_health")
    {
        return false;
    }
    !final_reconciliations.is_empty()
        && final_reconciliations
            .iter()
            .all(|reconcile| reconcile.error.is_some())
}

fn copy_live_daemon_immediate_submit_should_stop_round(
    report: &CopyLiveDaemonPersistentLiveSubmitReport,
) -> bool {
    !copy_canary_live_submitted_reports(&report.submitted_reports).is_empty()
}

fn copy_live_daemon_watcher_progress_check(
    watcher_status: &str,
    events_received: usize,
    duration_secs: u64,
    elapsed_ms: u64,
) -> CopyShadowSmokeCheck {
    let status = watcher_status.to_ascii_lowercase();
    let stopped_for_submit_candidate = status == "stopped_after_submit_candidate";
    let disconnected_before_progress = events_received == 0
        && matches!(
            status.as_str(),
            "watcher_recoverable_disconnect"
                | "watcher_channel_closed"
                | "watcher_error"
                | "watcher_join_error"
        );
    copy_shadow_smoke_check(
        "watcher_progress",
        !disconnected_before_progress,
        if disconnected_before_progress {
            format!(
                "watcher ended before receiving any events: status={watcher_status} events_received=0 elapsed_ms={elapsed_ms} duration_secs={duration_secs}; restart/backoff required before treating this as a healthy monitoring window"
            )
        } else if stopped_for_submit_candidate {
            format!(
                "watcher stopped after submit candidate so pre-submit checks and live submit can run before lower-priority event consumption: events_received={events_received} elapsed_ms={elapsed_ms} duration_secs={duration_secs}"
            )
        } else {
            format!(
                "watcher progress acceptable: status={watcher_status} events_received={events_received} elapsed_ms={elapsed_ms} duration_secs={duration_secs}"
            )
        },
    )
}

#[cfg(test)]
#[allow(dead_code)]
fn copy_live_daemon_reconciliations_healthy_for_mode(
    options: &CopyLiveDaemonSupervisorOptions,
    reconciliations: &[CopyBoundedLiveWindowReconcile],
) -> bool {
    copy_live_daemon_reconciliations_healthy_for_mode_with_account_caps(
        options,
        reconciliations,
        &HashMap::new(),
    )
}

fn copy_live_daemon_reconciliations_healthy_for_mode_with_account_caps(
    options: &CopyLiveDaemonSupervisorOptions,
    reconciliations: &[CopyBoundedLiveWindowReconcile],
    account_exposure_caps: &HashMap<String, f64>,
) -> bool {
    !reconciliations.is_empty()
        && reconciliations.iter().all(|reconcile| {
            copy_live_daemon_reconcile_healthy_for_mode_with_account_caps(
                options,
                reconcile,
                account_exposure_caps,
            )
        })
}

#[cfg(test)]
fn copy_live_daemon_reconcile_healthy_for_mode(
    options: &CopyLiveDaemonSupervisorOptions,
    reconcile: &CopyBoundedLiveWindowReconcile,
) -> bool {
    copy_live_daemon_reconcile_healthy_for_mode_with_account_caps(
        options,
        reconcile,
        &HashMap::new(),
    )
}

fn copy_live_daemon_account_exposure_cap(
    options: &CopyLiveDaemonSupervisorOptions,
    account_exposure_caps: &HashMap<String, f64>,
    account_id: &str,
) -> f64 {
    account_exposure_caps
        .get(account_id)
        .copied()
        .filter(|cap| cap.is_finite() && *cap > 0.0)
        .unwrap_or(options.max_total_notional_usd)
}

fn copy_live_daemon_account_exposure_health_cap(account_cap: f64) -> f64 {
    let drift_tolerance = (account_cap * 0.02).max(10.0);
    account_cap + drift_tolerance
}

fn copy_live_daemon_reconcile_healthy_for_mode_with_account_caps(
    options: &CopyLiveDaemonSupervisorOptions,
    reconcile: &CopyBoundedLiveWindowReconcile,
    account_exposure_caps: &HashMap<String, f64>,
) -> bool {
    if !options.hold_positions_after_submit {
        return reconcile.ok;
    }
    if reconcile.error.is_some() || reconcile.open_order_count != Some(0) {
        return false;
    }
    let Some(total_ntl_pos) = reconcile
        .total_ntl_pos
        .as_deref()
        .and_then(|value| value.parse::<f64>().ok())
    else {
        return false;
    };
    let account_cap = copy_live_daemon_account_exposure_cap(
        options,
        account_exposure_caps,
        &reconcile.account_id,
    );
    let health_cap = copy_live_daemon_account_exposure_health_cap(account_cap);
    total_ntl_pos.abs() <= health_cap + 1e-6
}

#[cfg(test)]
fn copy_live_daemon_reconciliations_healthy_for_snapshot(
    options: &CopyLiveDaemonSupervisorOptions,
    reconciliations: &[CopyBoundedLiveWindowReconcile],
    snapshot: &strategies::smart_money::CopyPersistenceSnapshot,
) -> bool {
    copy_live_daemon_reconciliations_healthy_for_snapshot_with_account_caps(
        options,
        reconciliations,
        snapshot,
        &HashMap::new(),
    )
}

fn copy_live_daemon_reconciliations_healthy_for_snapshot_with_account_caps(
    options: &CopyLiveDaemonSupervisorOptions,
    reconciliations: &[CopyBoundedLiveWindowReconcile],
    snapshot: &strategies::smart_money::CopyPersistenceSnapshot,
    account_exposure_caps: &HashMap<String, f64>,
) -> bool {
    if copy_live_daemon_unmapped_position_keys(snapshot, reconciliations).is_empty() {
        let account_principal =
            copy_live_daemon_active_copy_principal_notional_by_account(snapshot);
        !reconciliations.is_empty()
            && reconciliations.iter().all(|reconcile| {
                copy_live_daemon_reconcile_healthy_for_snapshot_with_account_caps(
                    options,
                    reconcile,
                    account_exposure_caps,
                    &account_principal,
                )
            })
    } else {
        false
    }
}

fn copy_live_daemon_reconcile_healthy_for_snapshot_with_account_caps(
    options: &CopyLiveDaemonSupervisorOptions,
    reconcile: &CopyBoundedLiveWindowReconcile,
    account_exposure_caps: &HashMap<String, f64>,
    account_principal_notional: &HashMap<String, f64>,
) -> bool {
    if copy_live_daemon_reconcile_healthy_for_mode_with_account_caps(
        options,
        reconcile,
        account_exposure_caps,
    ) {
        return true;
    }
    if !options.hold_positions_after_submit {
        return false;
    }
    if reconcile.error.is_some() || reconcile.open_order_count != Some(0) {
        return false;
    }
    let Some(total_ntl_pos) = reconcile
        .total_ntl_pos
        .as_deref()
        .and_then(|value| value.parse::<f64>().ok())
        .map(f64::abs)
        .filter(|value| value.is_finite())
    else {
        return false;
    };
    let account_cap = copy_live_daemon_account_exposure_cap(
        options,
        account_exposure_caps,
        &reconcile.account_id,
    );
    let health_cap = copy_live_daemon_account_exposure_health_cap(account_cap);
    if total_ntl_pos <= health_cap + 1e-6 {
        return true;
    }
    let mapped_principal = account_principal_notional
        .get(&reconcile.account_id)
        .copied()
        .unwrap_or_default();
    mapped_principal > 0.0 && mapped_principal <= health_cap + 1e-6
}

fn copy_live_daemon_reconcile_health_detail_with_account_caps(
    options: &CopyLiveDaemonSupervisorOptions,
    reconciliations: &[CopyBoundedLiveWindowReconcile],
    account_exposure_caps: &HashMap<String, f64>,
) -> String {
    if options.hold_positions_after_submit {
        let healthy_count = reconciliations
            .iter()
            .filter(|reconcile| {
                copy_live_daemon_reconcile_healthy_for_mode_with_account_caps(
                    options,
                    reconcile,
                    account_exposure_caps,
                )
            })
            .count();
        let no_open_orders = reconciliations
            .iter()
            .filter(|reconcile| reconcile.open_order_count == Some(0))
            .count();
        let max_total_ntl = reconciliations
            .iter()
            .filter_map(|reconcile| {
                reconcile
                    .total_ntl_pos
                    .as_deref()
                    .and_then(|value| value.parse::<f64>().ok())
            })
            .map(f64::abs)
            .fold(0.0_f64, f64::max);
        let cap_detail = if account_exposure_caps.is_empty() {
            format!("global fallback cap {:.6}", options.max_total_notional_usd)
        } else {
            reconciliations
                .iter()
                .map(|reconcile| {
                    let cap = copy_live_daemon_account_exposure_cap(
                        options,
                        account_exposure_caps,
                        &reconcile.account_id,
                    );
                    let health_cap = copy_live_daemon_account_exposure_health_cap(cap);
                    format!(
                        "{}<= {:.6} (health<= {:.6})",
                        reconcile.account_id, cap, health_cap
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        format!(
            "follow-position mode: {healthy_count}/{} account(s) healthy, {no_open_orders}/{} account(s) have no open orders, max_total_ntl_pos={max_total_ntl:.6}; per-account exposure caps: {cap_detail}; positions may remain until target close signals",
            reconciliations.len(),
            reconciliations.len()
        )
    } else {
        format!(
            "{}/{} account(s) flat with no open orders",
            reconciliations
                .iter()
                .filter(|reconcile| reconcile.ok)
                .count(),
            reconciliations.len()
        )
    }
}

#[cfg(test)]
fn copy_live_daemon_reconcile_health_detail_for_snapshot(
    options: &CopyLiveDaemonSupervisorOptions,
    reconciliations: &[CopyBoundedLiveWindowReconcile],
    snapshot: &strategies::smart_money::CopyPersistenceSnapshot,
) -> String {
    copy_live_daemon_reconcile_health_detail_for_snapshot_with_account_caps(
        options,
        reconciliations,
        snapshot,
        &HashMap::new(),
    )
}

fn copy_live_daemon_reconcile_health_detail_for_snapshot_with_account_caps(
    options: &CopyLiveDaemonSupervisorOptions,
    reconciliations: &[CopyBoundedLiveWindowReconcile],
    snapshot: &strategies::smart_money::CopyPersistenceSnapshot,
    account_exposure_caps: &HashMap<String, f64>,
) -> String {
    let account_principal = copy_live_daemon_active_copy_principal_notional_by_account(snapshot);
    let base = if options.hold_positions_after_submit {
        let healthy_count = reconciliations
            .iter()
            .filter(|reconcile| {
                copy_live_daemon_reconcile_healthy_for_snapshot_with_account_caps(
                    options,
                    reconcile,
                    account_exposure_caps,
                    &account_principal,
                )
            })
            .count();
        let no_open_orders = reconciliations
            .iter()
            .filter(|reconcile| reconcile.open_order_count == Some(0))
            .count();
        let max_total_ntl = reconciliations
            .iter()
            .filter_map(|reconcile| {
                reconcile
                    .total_ntl_pos
                    .as_deref()
                    .and_then(|value| value.parse::<f64>().ok())
            })
            .map(f64::abs)
            .fold(0.0_f64, f64::max);
        let cap_detail = if account_exposure_caps.is_empty() {
            format!("global fallback cap {:.6}", options.max_total_notional_usd)
        } else {
            reconciliations
                .iter()
                .map(|reconcile| {
                    let cap = copy_live_daemon_account_exposure_cap(
                        options,
                        account_exposure_caps,
                        &reconcile.account_id,
                    );
                    let health_cap = copy_live_daemon_account_exposure_health_cap(cap);
                    let mapped_principal = account_principal
                        .get(&reconcile.account_id)
                        .copied()
                        .unwrap_or_default();
                    format!(
                        "{}<= {:.6} (health<= {:.6}) mapped_copy_principal={mapped_principal:.6}",
                        reconcile.account_id, cap, health_cap
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        format!(
            "follow-position mode: {healthy_count}/{} account(s) healthy, {no_open_orders}/{} account(s) have no open orders, max_total_ntl_pos={max_total_ntl:.6}; per-account exposure caps: {cap_detail}; positions may remain until target close signals",
            reconciliations.len(),
            reconciliations.len()
        )
    } else {
        copy_live_daemon_reconcile_health_detail_with_account_caps(
            options,
            reconciliations,
            account_exposure_caps,
        )
    };
    let unmapped = copy_live_daemon_unmapped_position_keys(snapshot, reconciliations);
    if unmapped.is_empty() {
        return base;
    }
    format!(
        "{base}; unmanaged live position(s) without copy ledger mapping: {}",
        unmapped.join(",")
    )
}

fn copy_live_daemon_reconciliations_ready_for_reduce_only(
    executable_refs: &[CopyLiveDaemonWouldSubmitRef],
    reconciliations: &[CopyBoundedLiveWindowReconcile],
) -> bool {
    let accounts = executable_refs
        .iter()
        .filter(|plan| plan.order.reduce_only)
        .map(|plan| plan.order.account_id.as_str())
        .collect::<HashSet<_>>();
    !accounts.is_empty()
        && accounts.iter().all(|account_id| {
            reconciliations
                .iter()
                .find(|reconcile| reconcile.account_id == *account_id)
                .is_some_and(|reconcile| {
                    reconcile.error.is_none() && reconcile.open_order_count == Some(0)
                })
        })
}

fn copy_live_daemon_reduce_only_reconcile_health_detail(
    executable_refs: &[CopyLiveDaemonWouldSubmitRef],
    reconciliations: &[CopyBoundedLiveWindowReconcile],
) -> String {
    let accounts = executable_refs
        .iter()
        .filter(|plan| plan.order.reduce_only)
        .map(|plan| plan.order.account_id.as_str())
        .collect::<HashSet<_>>();
    let required_count = accounts.len();
    let readable_count = accounts
        .iter()
        .filter(|account_id| {
            reconciliations
                .iter()
                .find(|reconcile| reconcile.account_id == **account_id)
                .is_some_and(|reconcile| reconcile.error.is_none())
        })
        .count();
    let no_open_orders = accounts
        .iter()
        .filter(|account_id| {
            reconciliations
                .iter()
                .find(|reconcile| reconcile.account_id == **account_id)
                .is_some_and(|reconcile| reconcile.open_order_count == Some(0))
        })
        .count();
    let mut scoped_accounts = accounts.into_iter().collect::<Vec<_>>();
    scoped_accounts.sort_unstable();
    format!(
        "reduce-only submit precheck: {readable_count}/{required_count} submit account(s) readable, {no_open_orders}/{required_count} submit account(s) have no open orders; scoped_accounts={}; total exposure cap and unrelated account read errors are intentionally not applied to risk-reducing closes",
        scoped_accounts.join(",")
    )
}

fn copy_live_daemon_recoverable_watcher_error(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    [
        "os error 10054",
        "connection reset",
        "remote host",
        "远程主机",
        "forcibly closed",
        "unexpected eof",
        "websocket protocol error: connection reset",
    ]
    .iter()
    .any(|needle| message.contains(&needle.to_ascii_lowercase()))
}

fn copy_live_daemon_submit_evidence_contract(
    options: &CopyLiveDaemonSupervisorOptions,
    acceptance: &CopyLiveDaemonAcceptanceReport,
    would_submit_orders: &[CopyExecutionCanaryWouldSubmit],
    planned_notional_usd: f64,
    estimated_fees_usd: f64,
    final_reconciliations: &[CopyBoundedLiveWindowReconcile],
    persistent_live_submit: Option<&CopyLiveDaemonPersistentLiveSubmitReport>,
    account_exposure_caps: &HashMap<String, f64>,
) -> CopyLiveDaemonSubmitEvidenceContract {
    let cleanup_or_hold_evidence = if options.hold_positions_after_submit {
        "every accepted open is held under follow-position health gates until a mapped target close signal"
    } else {
        "every accepted open has a bundled reduce-only cleanup or mapped close path"
    };
    let final_reconcile_evidence = if options.hold_positions_after_submit {
        "final reconciliation proves every target account has no open orders and bounded notional exposure"
    } else {
        "final reconciliation proves every target account is flat with no open orders"
    };
    let required_live_evidence = vec![
        "every submitted order has an owned deterministic cloid".to_string(),
        "every submitted order has orderStatus evidence by oid or cloid".to_string(),
        "every filled order has at least one matching userFills/userFillsByTime fill".to_string(),
        cleanup_or_hold_evidence.to_string(),
        "cumulative submitted notional and conservative fee estimate remain under operator caps"
            .to_string(),
        final_reconcile_evidence.to_string(),
    ];
    let planned_open_order_count = would_submit_orders
        .iter()
        .filter(|order| !order.reduce_only)
        .count();
    let planned_reduce_only_count = would_submit_orders
        .iter()
        .filter(|order| order.reduce_only)
        .count();
    let live_submit_path_connected = persistent_live_submit.is_some_and(|report| report.ok);
    let live_submit_evidence_present = persistent_live_submit.is_some_and(|report| {
        report.ok && !report.submitted_reports.is_empty() && !report.order_evidence.is_empty()
    });
    let checks = vec![
        copy_shadow_smoke_check(
            "acceptance_gate",
            acceptance.ok,
            format!("copy-live-daemon-acceptance ok={}", acceptance.ok),
        ),
        copy_shadow_smoke_check(
            "deterministic_cloids",
            would_submit_orders
                .iter()
                .all(|order| !order.cloid.trim().is_empty()),
            format!(
                "{} planned order(s) have cloid ownership refs",
                would_submit_orders.len()
            ),
        ),
        copy_shadow_smoke_check(
            "bounded_live_orders",
            planned_open_order_count <= options.max_live_orders,
            format!(
                "{planned_open_order_count} planned open/increase order(s), {planned_reduce_only_count} reduce-only close order(s), max_live_orders {}",
                options.max_live_orders
            ),
        ),
        copy_shadow_smoke_check(
            "bounded_total_notional",
            planned_notional_usd <= options.max_total_notional_usd,
            format!(
                "planned_notional_usd={planned_notional_usd:.6} must be <= {:.6}",
                options.max_total_notional_usd
            ),
        ),
        copy_shadow_smoke_check(
            "bounded_total_fees",
            estimated_fees_usd <= options.max_total_fees_usd,
            format!(
                "estimated_fees_usd={estimated_fees_usd:.6} must be <= {:.6}",
                options.max_total_fees_usd
            ),
        ),
        copy_shadow_smoke_check(
            "cleanup_policy",
            acceptance.require_cleanup_after_submit || options.hold_positions_after_submit,
            if options.hold_positions_after_submit {
                "follow-position mode intentionally skips immediate cleanup and waits for mapped target close signals"
            } else {
                "unattended submit requires bundled reduce-only cleanup or mapped close handling"
            },
        ),
        copy_shadow_smoke_check(
            "flat_reconcile_policy",
            acceptance.require_flat_reconcile_after_submit
                && copy_live_daemon_reconciliations_healthy_for_mode_with_account_caps(
                    options,
                    final_reconciliations,
                    account_exposure_caps,
                ),
            copy_live_daemon_reconcile_health_detail_with_account_caps(
                options,
                final_reconciliations,
                account_exposure_caps,
            ),
        ),
        copy_shadow_smoke_check(
            "strict_order_evidence_policy",
            true,
            "future persistent submit must require orderStatus plus matching userFillsByTime-backed fill evidence",
        ),
        copy_shadow_smoke_check(
            "persistent_live_submit_path_connected",
            live_submit_path_connected,
            if let Some(report) = persistent_live_submit {
                format!(
                    "persistent live submit report ok={} submitted_reports={} order_evidence={}",
                    report.ok,
                    report.submitted_reports.len(),
                    report.order_evidence.len()
                )
            } else {
                "copy-live-daemon-supervisor ran no-submit; persistent live submit evidence was not requested".to_string()
            },
        ),
        copy_shadow_smoke_check(
            "real_live_submit_evidence_present",
            live_submit_evidence_present,
            if let Some(report) = persistent_live_submit {
                format!(
                    "unattended readiness requires at least one real submitted order with evidence; submitted_reports={} order_evidence={}",
                    report.submitted_reports.len(),
                    report.order_evidence.len()
                )
            } else {
                "no persistent live submit report was requested".to_string()
            },
        ),
    ];
    let blocker = (!checks.iter().all(|check| check.ok)).then(|| {
        if persistent_live_submit.is_none() {
            "Persistent daemon no-submit passed its local gates, but unattended live submit remains gated until the daemon submit path records owned orderStatus, matching fill evidence, cleanup, and final reconcile per submitted order."
                .to_string()
        } else if live_submit_path_connected && !live_submit_evidence_present {
            "Persistent daemon submit path is connected and the window was healthy, but no real order was submitted in this window; unattended readiness remains gated until a real submit records owned orderStatus, matching fill evidence, cleanup, and final reconcile."
                .to_string()
        } else {
            "Persistent daemon live submit was requested but did not satisfy every evidence contract check."
                .to_string()
        }
    });
    CopyLiveDaemonSubmitEvidenceContract {
        ready_for_unattended_submit: checks.iter().all(|check| check.ok),
        checks,
        required_live_evidence,
        blocker,
    }
}

#[cfg(test)]
fn copy_live_daemon_submit_plan_contract(
    options: &CopyLiveDaemonSupervisorOptions,
    executable_refs: &[CopyLiveDaemonWouldSubmitRef],
    suppressed_refs: &[CopyLiveDaemonSuppressedWouldSubmitRef],
    planned_notional_usd: f64,
    estimated_fees_usd: f64,
    final_reconciliations: &[CopyBoundedLiveWindowReconcile],
) -> CopyLiveDaemonSubmitPlanContract {
    copy_live_daemon_submit_plan_contract_with_account_caps(
        options,
        executable_refs,
        suppressed_refs,
        planned_notional_usd,
        estimated_fees_usd,
        final_reconciliations,
        &HashMap::new(),
    )
}

#[cfg(test)]
fn copy_live_daemon_submit_plan_contract_with_account_caps(
    options: &CopyLiveDaemonSupervisorOptions,
    executable_refs: &[CopyLiveDaemonWouldSubmitRef],
    suppressed_refs: &[CopyLiveDaemonSuppressedWouldSubmitRef],
    planned_notional_usd: f64,
    estimated_fees_usd: f64,
    final_reconciliations: &[CopyBoundedLiveWindowReconcile],
    account_exposure_caps: &HashMap<String, f64>,
) -> CopyLiveDaemonSubmitPlanContract {
    copy_live_daemon_submit_plan_contract_impl(
        options,
        executable_refs,
        suppressed_refs,
        planned_notional_usd,
        estimated_fees_usd,
        final_reconciliations,
        None,
        account_exposure_caps,
    )
}

fn copy_live_daemon_submit_plan_contract_with_snapshot_and_account_caps(
    options: &CopyLiveDaemonSupervisorOptions,
    executable_refs: &[CopyLiveDaemonWouldSubmitRef],
    suppressed_refs: &[CopyLiveDaemonSuppressedWouldSubmitRef],
    planned_notional_usd: f64,
    estimated_fees_usd: f64,
    final_reconciliations: &[CopyBoundedLiveWindowReconcile],
    snapshot: &strategies::smart_money::CopyPersistenceSnapshot,
    account_exposure_caps: &HashMap<String, f64>,
) -> CopyLiveDaemonSubmitPlanContract {
    copy_live_daemon_submit_plan_contract_impl(
        options,
        executable_refs,
        suppressed_refs,
        planned_notional_usd,
        estimated_fees_usd,
        final_reconciliations,
        Some(snapshot),
        account_exposure_caps,
    )
}

fn copy_live_daemon_submit_plan_contract_impl(
    options: &CopyLiveDaemonSupervisorOptions,
    executable_refs: &[CopyLiveDaemonWouldSubmitRef],
    suppressed_refs: &[CopyLiveDaemonSuppressedWouldSubmitRef],
    planned_notional_usd: f64,
    estimated_fees_usd: f64,
    final_reconciliations: &[CopyBoundedLiveWindowReconcile],
    snapshot: Option<&strategies::smart_money::CopyPersistenceSnapshot>,
    account_exposure_caps: &HashMap<String, f64>,
) -> CopyLiveDaemonSubmitPlanContract {
    let executable_cloids = executable_refs
        .iter()
        .map(|plan| plan.order.cloid.as_str())
        .collect::<HashSet<_>>();
    let executable_account_signal_refs = executable_refs
        .iter()
        .map(|plan| {
            (
                plan.record_index,
                plan.signal_id.as_str(),
                plan.order.account_id.as_str(),
            )
        })
        .collect::<HashSet<_>>();
    let suppressed_cloids = suppressed_refs
        .iter()
        .map(|suppressed| suppressed.plan.order.cloid.as_str())
        .collect::<HashSet<_>>();
    let executable_open_plan_count = executable_refs
        .iter()
        .filter(|plan| !plan.order.reduce_only)
        .count();
    let executable_reduce_only_plan_count = executable_refs
        .iter()
        .filter(|plan| plan.order.reduce_only)
        .count();
    let reduce_only_only_plan =
        executable_open_plan_count == 0 && executable_reduce_only_plan_count > 0;
    let planned_open_notional_usd = normalize_report_zero(
        executable_refs
            .iter()
            .filter(|plan| !plan.order.reduce_only)
            .map(|plan| plan.order.notional_usd.max(0.0))
            .sum::<f64>(),
    );
    let existing_total_ntl_values = final_reconciliations
        .iter()
        .filter_map(|reconcile| {
            reconcile
                .total_ntl_pos
                .as_deref()
                .and_then(|value| value.parse::<f64>().ok())
                .map(f64::abs)
        })
        .collect::<Vec<_>>();
    let max_existing_total_ntl_pos_usd = existing_total_ntl_values
        .iter()
        .copied()
        .fold(0.0f64, f64::max);
    let projected_total_exposure_usd =
        normalize_report_zero(max_existing_total_ntl_pos_usd + planned_open_notional_usd);
    let existing_total_ntl_by_account = final_reconciliations
        .iter()
        .filter_map(|reconcile| {
            let value = reconcile
                .total_ntl_pos
                .as_deref()
                .and_then(|value| value.parse::<f64>().ok())?
                .abs();
            Some((reconcile.account_id.clone(), value))
        })
        .collect::<HashMap<_, _>>();
    let mut planned_open_notional_by_account = HashMap::<String, f64>::new();
    for plan in executable_refs
        .iter()
        .filter(|plan| !plan.order.reduce_only)
    {
        *planned_open_notional_by_account
            .entry(plan.order.account_id.clone())
            .or_insert(0.0) += plan.order.notional_usd.max(0.0);
    }
    let account_exposure_details = planned_open_notional_by_account
        .iter()
        .map(|(account_id, planned)| {
            let cap =
                copy_live_daemon_account_exposure_cap(options, account_exposure_caps, account_id);
            match existing_total_ntl_by_account.get(account_id) {
                Some(existing) => {
                    let projected = normalize_report_zero(*existing + *planned);
                    (
                        projected <= cap + 1e-9,
                        format!(
                            "{account_id}: existing_total_ntl_pos_usd={existing:.6} + planned_open_notional_usd={planned:.6} => projected_total_exposure_usd={projected:.6}, must be <= {cap:.6}"
                        ),
                    )
                }
                None => (
                    false,
                    format!(
                        "{account_id}: planned_open_notional_usd={planned:.6} requires a successful account total_ntl_pos reconciliation before live submit"
                    ),
                ),
            }
        })
        .collect::<Vec<_>>();
    let bounded_account_total_exposure_ok = if planned_open_notional_usd <= 1e-9 {
        true
    } else if account_exposure_caps.is_empty() {
        !existing_total_ntl_values.is_empty()
            && projected_total_exposure_usd <= options.max_total_notional_usd + 1e-9
    } else {
        !account_exposure_details.is_empty() && account_exposure_details.iter().all(|(ok, _)| *ok)
    };
    let open_margin_requirements =
        copy_live_daemon_open_margin_requirements(executable_refs, estimated_fees_usd);
    let open_margin_check =
        copy_live_daemon_open_margin_check(&open_margin_requirements, final_reconciliations);
    let checks = vec![
        copy_shadow_smoke_check(
            "submit_from_executable_refs_only",
            executable_refs
                .iter()
                .all(|plan| !plan.order.cloid.trim().is_empty())
                && executable_cloids.len() == executable_refs.len(),
            format!(
                "{} executable submit plan ref(s) have unique deterministic cloids",
                executable_refs.len()
            ),
        ),
        copy_shadow_smoke_check(
            "suppressed_refs_excluded_from_submit",
            executable_cloids.is_disjoint(&suppressed_cloids),
            format!(
                "{} suppressed plan ref(s) are retained as evidence only",
                suppressed_refs.len()
            ),
        ),
        copy_shadow_smoke_check(
            "signal_refs_present",
            executable_refs
                .iter()
                .all(|plan| !plan.signal_id.trim().is_empty()),
            format!(
                "{} executable plan ref(s) have non-empty signal ids",
                executable_refs.len()
            ),
        ),
        copy_shadow_smoke_check(
            "account_signal_refs_unique",
            executable_account_signal_refs.len() == executable_refs.len(),
            format!(
                "{} executable plan ref(s) map to unique record/signal/account tuples",
                executable_refs.len()
            ),
        ),
        copy_shadow_smoke_check(
            "bounded_open_submit_plan_count",
            executable_open_plan_count <= options.max_live_orders,
            format!(
                "{executable_open_plan_count} executable open/increase plan(s), {executable_reduce_only_plan_count} reduce-only close plan(s), max_live_orders {}",
                options.max_live_orders
            ),
        ),
        copy_shadow_smoke_check(
            "bounded_submit_plan_notional",
            planned_notional_usd <= options.max_total_notional_usd,
            format!(
                "planned_open_notional_usd={planned_notional_usd:.6} must be <= {:.6}; reduce-only closes do not add exposure",
                options.max_total_notional_usd
            ),
        ),
        copy_shadow_smoke_check(
            "bounded_submit_plan_fees",
            estimated_fees_usd <= options.max_total_fees_usd,
            format!(
                "estimated_open_fees_usd={estimated_fees_usd:.6} must be <= {:.6}; reduce-only closes are not blocked by open-order fee budget",
                options.max_total_fees_usd
            ),
        ),
        copy_shadow_smoke_check(
            "bounded_account_total_exposure",
            bounded_account_total_exposure_ok,
            if planned_open_notional_usd <= 1e-9 {
                format!(
                    "no open/increase notional in executable plan; reduce-only closes do not add exposure (existing_total_ntl_pos_usd={max_existing_total_ntl_pos_usd:.6})"
                )
            } else if existing_total_ntl_values.is_empty() {
                format!(
                    "planned_open_notional_usd={planned_open_notional_usd:.6} requires a successful account total_ntl_pos reconciliation before live submit"
                )
            } else if !account_exposure_caps.is_empty() {
                account_exposure_details
                    .iter()
                    .map(|(_, detail)| detail.as_str())
                    .collect::<Vec<_>>()
                    .join("; ")
            } else {
                format!(
                    "existing_total_ntl_pos_usd={max_existing_total_ntl_pos_usd:.6} + planned_open_notional_usd={planned_open_notional_usd:.6} => projected_total_exposure_usd={projected_total_exposure_usd:.6}, must be <= {:.6}",
                    options.max_total_notional_usd
                )
            },
        ),
        copy_shadow_smoke_check(
            "bounded_open_margin_available",
            open_margin_check.ok,
            open_margin_check.detail,
        ),
        copy_shadow_smoke_check(
            if options.hold_positions_after_submit {
                "pre_submit_reconcile_health"
            } else {
                "pre_submit_reconcile_flat"
            },
            if reduce_only_only_plan {
                copy_live_daemon_reconciliations_ready_for_reduce_only(
                    executable_refs,
                    final_reconciliations,
                )
            } else if let Some(snapshot) = snapshot {
                copy_live_daemon_reconciliations_healthy_for_snapshot_with_account_caps(
                    options,
                    final_reconciliations,
                    snapshot,
                    account_exposure_caps,
                )
            } else {
                copy_live_daemon_reconciliations_healthy_for_mode_with_account_caps(
                    options,
                    final_reconciliations,
                    account_exposure_caps,
                )
            },
            if reduce_only_only_plan {
                copy_live_daemon_reduce_only_reconcile_health_detail(
                    executable_refs,
                    final_reconciliations,
                )
            } else if let Some(snapshot) = snapshot {
                copy_live_daemon_reconcile_health_detail_for_snapshot_with_account_caps(
                    options,
                    final_reconciliations,
                    snapshot,
                    account_exposure_caps,
                )
            } else {
                copy_live_daemon_reconcile_health_detail_with_account_caps(
                    options,
                    final_reconciliations,
                    account_exposure_caps,
                )
            },
        ),
    ];
    CopyLiveDaemonSubmitPlanContract {
        ok: checks.iter().all(|check| check.ok),
        checks,
        executable_plan_count: executable_refs.len(),
        suppressed_plan_count: suppressed_refs.len(),
        executable_open_plan_count,
        executable_reduce_only_plan_count,
        planned_notional_usd,
        estimated_fees_usd,
    }
}

#[derive(Debug, Clone)]
struct CopyLiveDaemonOpenMarginCheck {
    ok: bool,
    detail: String,
}

fn copy_live_daemon_open_margin_requirements(
    executable_refs: &[CopyLiveDaemonWouldSubmitRef],
    _estimated_fees_usd: f64,
) -> HashMap<String, f64> {
    let mut requirements = HashMap::new();
    let leverage = strategies::smart_money::COPY_MAX_LEVERAGE.max(1.0);
    let notional_to_margin =
        (1.0 + COPY_DAEMON_MARGIN_BUFFER_RATIO) / leverage + COPY_DAEMON_FEE_BUFFER_RATIO;
    for plan in executable_refs
        .iter()
        .filter(|plan| !plan.order.reduce_only)
    {
        let principal = plan.order.notional_usd.max(0.0) * notional_to_margin;
        *requirements
            .entry(plan.order.account_id.clone())
            .or_insert(0.0) += principal;
    }
    requirements
}

fn copy_live_daemon_open_margin_check(
    requirements: &HashMap<String, f64>,
    reconciliations: &[CopyBoundedLiveWindowReconcile],
) -> CopyLiveDaemonOpenMarginCheck {
    if requirements.is_empty() {
        return CopyLiveDaemonOpenMarginCheck {
            ok: true,
            detail: "no open/increase notional in executable plan; reduce-only closes do not require opening margin".to_string(),
        };
    }

    let mut details = Vec::new();
    let mut ok = true;
    for (account_id, required) in requirements {
        let available = reconciliations
            .iter()
            .find(|reconcile| reconcile.account_id == *account_id)
            .and_then(|reconcile| reconcile.withdrawable.as_deref())
            .and_then(|value| value.parse::<f64>().ok());
        match available {
            Some(available) if available + 1e-9 >= *required => {
                details.push(format!(
                    "{account_id} withdrawable={available:.6} >= required_open_margin={required:.6}"
                ));
            }
            Some(available) => {
                ok = false;
                details.push(format!(
                    "{account_id} withdrawable={available:.6} < required_open_margin={required:.6}"
                ));
            }
            None => {
                ok = false;
                details.push(format!(
                    "{account_id} withdrawable unavailable; cannot prove required_open_margin={required:.6}"
                ));
            }
        }
    }

    CopyLiveDaemonOpenMarginCheck {
        ok,
        detail: format!(
            "opening margin precheck uses notional/{}x plus {:.0}% buffer and fee estimate: {}",
            strategies::smart_money::COPY_MAX_LEVERAGE,
            COPY_DAEMON_MARGIN_BUFFER_RATIO * 100.0,
            details.join("; ")
        ),
    }
}

fn copy_live_daemon_suppress_refs_rejected_by_submit_contract(
    options: &CopyLiveDaemonSupervisorOptions,
    executable_refs: Vec<CopyLiveDaemonWouldSubmitRef>,
    suppressed_refs: Vec<CopyLiveDaemonSuppressedWouldSubmitRef>,
    planned_notional_usd: f64,
    estimated_fees_usd: f64,
    reconciliations: &[CopyBoundedLiveWindowReconcile],
    snapshot: Option<&strategies::smart_money::CopyPersistenceSnapshot>,
    contract: CopyLiveDaemonSubmitPlanContract,
    account_exposure_caps: &HashMap<String, f64>,
) -> (
    Vec<CopyLiveDaemonWouldSubmitRef>,
    Vec<CopyLiveDaemonSuppressedWouldSubmitRef>,
    CopyLiveDaemonSubmitPlanContract,
) {
    if contract.ok {
        return (executable_refs, Vec::new(), contract);
    }

    let failed_checks = contract
        .checks
        .iter()
        .filter(|check| !check.ok)
        .map(|check| check.name.as_str())
        .collect::<Vec<_>>();
    let suppressible_open_failure = !failed_checks.is_empty()
        && failed_checks.iter().all(|check| {
            matches!(
                *check,
                "bounded_account_total_exposure" | "bounded_open_margin_available"
            )
        });
    if !suppressible_open_failure {
        return (executable_refs, Vec::new(), contract);
    }

    let margin_detail = contract
        .checks
        .iter()
        .find(|check| check.name == "bounded_open_margin_available" && !check.ok)
        .map(|check| check.detail.clone());
    let account_exposure_detail = contract
        .checks
        .iter()
        .find(|check| check.name == "bounded_account_total_exposure")
        .map(|check| check.detail.clone())
        .unwrap_or_else(|| {
            "candidate would exceed account total exposure cap; kept as observation only"
                .to_string()
        });
    let reason_code = if margin_detail.is_some() {
        "COPY_DAEMON_INSUFFICIENT_MARGIN"
    } else {
        "COPY_DAEMON_MAX_ACCOUNT_EXPOSURE"
    };
    let suppression_message = margin_detail.unwrap_or(account_exposure_detail);
    let mut retained_executable = Vec::new();
    let mut newly_suppressed = Vec::new();
    for plan in executable_refs {
        if plan.order.reduce_only {
            retained_executable.push(plan);
        } else {
            newly_suppressed.push(CopyLiveDaemonSuppressedWouldSubmitRef {
                plan,
                reason_code: reason_code.to_string(),
                message: suppression_message.clone(),
            });
        }
    }

    if newly_suppressed.is_empty() {
        return (retained_executable, Vec::new(), contract);
    }

    let mut all_suppressed_refs = suppressed_refs;
    all_suppressed_refs.extend(newly_suppressed.clone());
    let adjusted_planned_notional_usd = normalize_report_zero(
        retained_executable
            .iter()
            .map(|plan| plan.order.notional_usd.max(0.0))
            .sum::<f64>(),
    );
    let adjusted_estimated_fees_usd = if planned_notional_usd > 0.0 {
        normalize_report_zero(
            estimated_fees_usd * adjusted_planned_notional_usd / planned_notional_usd,
        )
    } else {
        0.0
    };
    let adjusted_contract = copy_live_daemon_submit_plan_contract_impl(
        options,
        &retained_executable,
        &all_suppressed_refs,
        adjusted_planned_notional_usd,
        adjusted_estimated_fees_usd,
        reconciliations,
        snapshot,
        account_exposure_caps,
    );
    (retained_executable, newly_suppressed, adjusted_contract)
}

fn copy_live_daemon_persistent_submit_dry_run(
    config: &config::AppConfig,
    submit_plan_contract: &CopyLiveDaemonSubmitPlanContract,
    executable_refs: &[CopyLiveDaemonWouldSubmitRef],
    suppressed_refs: &[CopyLiveDaemonSuppressedWouldSubmitRef],
    max_slippage_bps: f64,
) -> CopyLiveDaemonPersistentSubmitDryRunReport {
    let mut planned_reports = Vec::new();
    if submit_plan_contract.ok {
        for plan in executable_refs {
            let Some(account) = config.account(&plan.order.account_id) else {
                planned_reports.push(CopyLiveDaemonPersistentSubmitDryRunPlan {
                    record_index: plan.record_index,
                    signal_id: plan.signal_id.clone(),
                    leader_id: plan.leader_id.clone(),
                    leader_address: plan.leader_address.clone(),
                    account_id: plan.order.account_id.clone(),
                    worker_id: plan.order.worker_id.clone(),
                    coin: plan.order.coin.clone(),
                    side: plan.order.side,
                    notional_usd: plan.order.notional_usd,
                    reduce_only: plan.order.reduce_only,
                    cloid: plan.order.cloid.clone(),
                    would_submit: false,
                    dry_run_only: true,
                    rejected_reason_code: Some("ACCOUNT_NOT_FOUND".to_string()),
                    rejected_message: Some(format!(
                        "account {} not found for persistent submit dry-run",
                        plan.order.account_id
                    )),
                });
                continue;
            };
            let (market, dex) = copy_daemon_market_dex_for_coin(&plan.order.coin);
            let approved_order = domain::ApprovedOrder {
                risk_decision_id: format!(
                    "copy-daemon-dry-run-{}-{}",
                    plan.record_index, plan.signal_id
                ),
                intent_id: format!(
                    "copy-daemon-intent-{}-{}",
                    plan.record_index, plan.signal_id
                ),
                signal_id: Some(plan.signal_id.clone()),
                worker_id: plan.order.worker_id.clone(),
                account_id: plan.order.account_id.clone(),
                strategy_id: "smart_money_copy".to_string(),
                market,
                dex,
                coin: plan.order.coin.clone(),
                side: plan.order.side,
                notional_usd: plan.order.notional_usd,
                exact_size: None,
                price: None,
                execution_mode: domain::ExecutionMode::Taker,
                execution_policy: domain::ExecutionPolicy::Taker,
                max_slippage_bps,
                reduce_only: plan.order.reduce_only,
                cloid: plan.order.cloid.clone(),
                expires_at_ms: None,
            };
            let risk_context =
                risk::RiskContext::from_account_for_module(config, account, true, "copy");
            let dry_run_intent = domain::TradeIntent {
                intent_id: approved_order.intent_id.clone(),
                signal_id: approved_order.signal_id.clone(),
                worker_id: approved_order.worker_id.clone(),
                account_id: approved_order.account_id.clone(),
                target_accounts: vec![approved_order.account_id.clone()],
                strategy_id: approved_order.strategy_id.clone(),
                created_at_ms: domain::now_ms(),
                market: approved_order.market.clone(),
                dex: approved_order.dex.clone(),
                coin: approved_order.coin.clone(),
                side: approved_order.side,
                intent_kind: if approved_order.reduce_only {
                    domain::IntentKind::Reduce
                } else {
                    domain::IntentKind::Open
                },
                sizing: domain::SizingRequest {
                    notional_usd: approved_order.notional_usd,
                },
                price_policy: domain::PricePolicy::MarketWithSlippageLimit {
                    max_slippage_bps: approved_order.max_slippage_bps,
                },
                execution_policy: approved_order.execution_policy,
                reduce_only: approved_order.reduce_only,
                reason: "persistent daemon submit dry-run plan".to_string(),
                source: domain::IntentSource::Strategy,
                source_event_id: Some(plan.signal_id.clone()),
                expires_at_ms: approved_order.expires_at_ms,
            };
            match risk::RiskGateway::dry_run_default().evaluate(&risk_context, dry_run_intent) {
                risk::RiskDecision::Approved(order) => {
                    planned_reports.push(CopyLiveDaemonPersistentSubmitDryRunPlan {
                        record_index: plan.record_index,
                        signal_id: plan.signal_id.clone(),
                        leader_id: plan.leader_id.clone(),
                        leader_address: plan.leader_address.clone(),
                        account_id: order.account_id,
                        worker_id: order.worker_id,
                        coin: order.coin,
                        side: order.side,
                        notional_usd: order.notional_usd,
                        reduce_only: order.reduce_only,
                        cloid: plan.order.cloid.clone(),
                        would_submit: true,
                        dry_run_only: true,
                        rejected_reason_code: None,
                        rejected_message: None,
                    });
                }
                risk::RiskDecision::Rejected(rejection) => {
                    planned_reports.push(CopyLiveDaemonPersistentSubmitDryRunPlan {
                        record_index: plan.record_index,
                        signal_id: plan.signal_id.clone(),
                        leader_id: plan.leader_id.clone(),
                        leader_address: plan.leader_address.clone(),
                        account_id: rejection.account_id,
                        worker_id: rejection.worker_id,
                        coin: plan.order.coin.clone(),
                        side: plan.order.side,
                        notional_usd: plan.order.notional_usd,
                        reduce_only: plan.order.reduce_only,
                        cloid: plan.order.cloid.clone(),
                        would_submit: false,
                        dry_run_only: true,
                        rejected_reason_code: Some(rejection.reason_code),
                        rejected_message: Some(rejection.message),
                    });
                }
            }
        }
    }
    let checks = vec![
        copy_shadow_smoke_check(
            "submit_plan_contract_ok",
            submit_plan_contract.ok,
            format!("submit_plan_contract.ok={}", submit_plan_contract.ok),
        ),
        copy_shadow_smoke_check(
            "suppressed_refs_not_planned",
            planned_reports.len() == executable_refs.len(),
            format!(
                "{} dry-run plan(s) from {} executable ref(s); {} suppressed ref(s) excluded",
                planned_reports.len(),
                executable_refs.len(),
                suppressed_refs.len()
            ),
        ),
        copy_shadow_smoke_check(
            "all_plans_dry_run_only",
            planned_reports.iter().all(|plan| plan.dry_run_only),
            "persistent submit dry-run does not load Vault, sign, or submit exchange orders",
        ),
        copy_shadow_smoke_check(
            "planned_cloids_match_executable_refs",
            planned_reports.len() == executable_refs.len()
                && planned_reports
                    .iter()
                    .zip(executable_refs.iter())
                    .all(|(planned, executable)| planned.cloid == executable.order.cloid),
            "persistent submit dry-run preserves executable ref cloids for ownership evidence",
        ),
        copy_shadow_smoke_check(
            "all_executable_refs_risk_approved",
            planned_reports.iter().all(|plan| plan.would_submit),
            format!(
                "{}/{} executable ref(s) approved by dry-run RiskGateway",
                planned_reports
                    .iter()
                    .filter(|plan| plan.would_submit)
                    .count(),
                planned_reports.len()
            ),
        ),
    ];
    CopyLiveDaemonPersistentSubmitDryRunReport {
        ok: checks.iter().all(|check| check.ok),
        mode: "persistent_submit_dry_run_worker_plan".to_string(),
        submit_plan_contract_ok: submit_plan_contract.ok,
        planned_reports,
        checks,
    }
}

#[cfg(test)]
fn copy_live_daemon_reduce_only_ref_has_matching_position(
    state: &hyperliquid::ClearinghouseState,
    plan: &CopyLiveDaemonWouldSubmitRef,
) -> bool {
    copy_live_daemon_reduce_only_matching_position_notional_usd(state, plan)
        .is_some_and(|notional| notional > 0.0)
}

fn copy_live_daemon_reduce_only_matching_position_notional_usd(
    state: &hyperliquid::ClearinghouseState,
    plan: &CopyLiveDaemonWouldSubmitRef,
) -> Option<f64> {
    if !plan.order.reduce_only {
        return Some(plan.order.notional_usd.max(0.0));
    }
    state
        .asset_positions
        .iter()
        .filter(|asset| {
            asset
                .position
                .coin
                .eq_ignore_ascii_case(plan.order.coin.as_str())
        })
        .filter_map(|asset| {
            let szi = asset.position.szi.parse::<f64>().ok()?;
            let matches_side = match plan.order.side {
                domain::OrderSide::Buy => szi < -1e-12,
                domain::OrderSide::Sell => szi > 1e-12,
            };
            if !matches_side {
                return None;
            }
            let position_value = asset
                .position
                .position_value
                .as_deref()?
                .parse::<f64>()
                .ok()?;
            Some(position_value.abs())
        })
        .max_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal))
}

fn copy_live_daemon_reduce_only_effective_notional_usd(
    requested_notional_usd: f64,
    local_position_notional_usd: f64,
) -> f64 {
    let local_position_notional_usd = local_position_notional_usd.max(0.0);
    if local_position_notional_usd <= 0.0 {
        return 0.0;
    }
    let requested_notional_usd = requested_notional_usd.max(0.0);
    if requested_notional_usd <= 1e-9 {
        return 0.0;
    }
    let capped_notional = requested_notional_usd.min(local_position_notional_usd);
    let residual_notional = (local_position_notional_usd - capped_notional).max(0.0);
    if residual_notional > 1e-9 && residual_notional < trading::HYPERLIQUID_MIN_ORDER_NOTIONAL_USD {
        return local_position_notional_usd;
    }
    capped_notional
}

fn copy_live_daemon_spot_available_balance(
    state: &hyperliquid::SpotClearinghouseState,
    token: &str,
) -> f64 {
    state
        .balances
        .iter()
        .filter(|balance| balance.coin.eq_ignore_ascii_case(token))
        .map(|balance| {
            let total = balance.total.parse::<f64>().unwrap_or_default();
            let hold = balance.hold.parse::<f64>().unwrap_or_default();
            (total - hold).max(0.0)
        })
        .sum::<f64>()
}

fn copy_live_daemon_spot_base_available_for_coin(
    state: &hyperliquid::SpotClearinghouseState,
    coin: &str,
) -> f64 {
    let normalized = hyperliquid::normalize_spot_coin(coin);
    let Some((base, _quote)) = normalized.split_once('/') else {
        return 0.0;
    };
    copy_live_daemon_spot_available_balance(state, base)
}

fn copy_live_daemon_spot_entry_has_matching_position(
    state: &hyperliquid::SpotClearinghouseState,
    coin: &str,
    local_side: domain::OrderSide,
) -> bool {
    matches!(local_side, domain::OrderSide::Buy)
        && copy_live_daemon_spot_base_available_for_coin(state, coin) > 1e-12
}

fn copy_live_daemon_reduce_only_matching_spot_position_notional_usd(
    state: &hyperliquid::SpotClearinghouseState,
    snapshot: &hyperliquid::SpotMarketSnapshot,
    plan: &CopyLiveDaemonWouldSubmitRef,
) -> Result<Option<f64>> {
    if !plan.order.reduce_only {
        return Ok(Some(plan.order.notional_usd.max(0.0)));
    }
    if !matches!(plan.order.side, domain::OrderSide::Sell) {
        return Ok(None);
    }

    let base_available = copy_live_daemon_spot_base_available_for_coin(state, &plan.order.coin);
    if base_available <= 1e-12 {
        return Ok(None);
    }

    let asset = snapshot.asset(&plan.order.coin)?;
    let reference_price = asset
        .context
        .mid_px
        .as_deref()
        .or(Some(asset.context.mark_px.as_str()))
        .or(Some(asset.context.prev_day_px.as_str()))
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .with_context(|| {
            format!(
                "failed to derive spot reference price for reduce-only verification on {}",
                plan.order.coin
            )
        })?;
    Ok(Some(base_available * reference_price))
}

fn copy_live_daemon_perp_entry_has_matching_position(
    state: &hyperliquid::ClearinghouseState,
    coin: &str,
    local_side: domain::OrderSide,
) -> bool {
    state.asset_positions.iter().any(|asset| {
        if !asset.position.coin.eq_ignore_ascii_case(coin) {
            return false;
        }
        let Some(side) = copy_live_daemon_local_side_from_position_szi(&asset.position.szi) else {
            return false;
        };
        side == local_side
    })
}

async fn copy_live_daemon_filter_submit_refs_for_live_reduce_exposure(
    config: &config::AppConfig,
    executable_refs: &[CopyLiveDaemonWouldSubmitRef],
) -> (Vec<CopyLiveDaemonWouldSubmitRef>, Vec<CopyShadowSmokeCheck>) {
    let reduce_only_count = executable_refs
        .iter()
        .filter(|plan| plan.order.reduce_only)
        .count();
    if reduce_only_count == 0 {
        return (
            executable_refs.to_vec(),
            vec![copy_shadow_smoke_check(
                "reduce_only_local_exposure_filter",
                true,
                "0 reduce-only ref(s) required live local exposure verification",
            )],
        );
    }

    let mut perp_state_by_scope: HashMap<
        (String, String),
        std::result::Result<hyperliquid::ClearinghouseState, String>,
    > = HashMap::new();
    let mut spot_state_by_account: HashMap<
        String,
        std::result::Result<hyperliquid::SpotClearinghouseState, String>,
    > = HashMap::new();
    let mut spot_snapshot: Option<std::result::Result<hyperliquid::SpotMarketSnapshot, String>> =
        None;
    for plan in executable_refs.iter().filter(|plan| plan.order.reduce_only) {
        let (_, dex) = copy_daemon_market_dex_for_coin(&plan.order.coin);
        if dex.as_deref() == Some("spot") {
            if !spot_state_by_account.contains_key(&plan.order.account_id) {
                let state = if let Some(account) = config.account(&plan.order.account_id) {
                    hyperliquid::fetch_spot_clearinghouse_state(
                        &config.app.environment,
                        &account.address,
                    )
                    .await
                    .map_err(|error| error.to_string())
                } else {
                    Err(format!(
                        "account {} not found for reduce-only spot exposure verification",
                        plan.order.account_id
                    ))
                };
                spot_state_by_account.insert(plan.order.account_id.clone(), state);
            }
            if spot_snapshot.is_none() {
                spot_snapshot = Some(
                    hyperliquid::fetch_spot_market_snapshot_cached(&config.app.environment, 15_000)
                        .await
                        .map_err(|error| error.to_string()),
                );
            }
            continue;
        }

        let dex_key = dex.unwrap_or_else(|| config.hyperliquid.dex.clone());
        let scope = (plan.order.account_id.clone(), dex_key.clone());
        if perp_state_by_scope.contains_key(&scope) {
            continue;
        }
        let state = if let Some(account) = config.account(&plan.order.account_id) {
            hyperliquid::fetch_clearinghouse_state(
                &config.app.environment,
                dex_key.as_str(),
                &account.address,
            )
            .await
            .map_err(|error| error.to_string())
        } else {
            Err(format!(
                "account {} not found for reduce-only local exposure verification",
                plan.order.account_id
            ))
        };
        perp_state_by_scope.insert(scope, state);
    }

    let mut eligible_refs = Vec::new();
    let mut verified_reduce_only_count = 0usize;
    let mut resized_reduce_only_count = 0usize;
    let mut dust_flattened_reduce_only_count = 0usize;
    let mut skipped_no_exposure_count = 0usize;
    let mut verification_error_count = 0usize;
    for plan in executable_refs {
        if !plan.order.reduce_only {
            eligible_refs.push(plan.clone());
            continue;
        }
        let (_, dex) = copy_daemon_market_dex_for_coin(&plan.order.coin);
        match dex.as_deref() {
            Some("spot") => match (
                spot_state_by_account.get(&plan.order.account_id),
                spot_snapshot.as_ref(),
            ) {
                (Some(Ok(state)), Some(Ok(snapshot))) => {
                    let Some(local_position_notional) =
                        copy_live_daemon_reduce_only_matching_spot_position_notional_usd(
                            state, snapshot, plan,
                        )
                        .ok()
                        .flatten()
                    else {
                        skipped_no_exposure_count += 1;
                        continue;
                    };
                    verified_reduce_only_count += 1;
                    let mut prepared = plan.clone();
                    let requested_notional = prepared.order.notional_usd;
                    let capped_notional = copy_live_daemon_reduce_only_effective_notional_usd(
                        requested_notional,
                        local_position_notional,
                    );
                    if capped_notional > requested_notional.max(0.0) + 1e-9 {
                        dust_flattened_reduce_only_count += 1;
                    }
                    if (requested_notional - capped_notional).abs() > 1e-9 {
                        resized_reduce_only_count += 1;
                        prepared.order.notional_usd = capped_notional;
                    }
                    eligible_refs.push(prepared);
                }
                (Some(Err(_)), _) | (_, Some(Err(_))) | (_, None) | (None, _) => {
                    verification_error_count += 1;
                }
            },
            _ => {
                let dex_key = dex.unwrap_or_else(|| config.hyperliquid.dex.clone());
                match perp_state_by_scope.get(&(plan.order.account_id.clone(), dex_key)) {
                    Some(Ok(state)) => {
                        let Some(local_position_notional) =
                            copy_live_daemon_reduce_only_matching_position_notional_usd(
                                state, plan,
                            )
                        else {
                            skipped_no_exposure_count += 1;
                            continue;
                        };
                        if local_position_notional <= 0.0 {
                            skipped_no_exposure_count += 1;
                            continue;
                        }
                        verified_reduce_only_count += 1;
                        let mut prepared = plan.clone();
                        let requested_notional = prepared.order.notional_usd;
                        let capped_notional = copy_live_daemon_reduce_only_effective_notional_usd(
                            requested_notional,
                            local_position_notional,
                        );
                        if capped_notional > requested_notional.max(0.0) + 1e-9 {
                            dust_flattened_reduce_only_count += 1;
                        }
                        if (requested_notional - capped_notional).abs() > 1e-9 {
                            resized_reduce_only_count += 1;
                            prepared.order.notional_usd = capped_notional;
                        }
                        eligible_refs.push(prepared);
                    }
                    Some(Err(_)) | None => {
                        verification_error_count += 1;
                    }
                }
            }
        }
    }

    (
        eligible_refs,
        vec![copy_shadow_smoke_check(
            "reduce_only_local_exposure_filter",
            verification_error_count == 0,
            format!(
                "{verified_reduce_only_count}/{reduce_only_count} reduce-only ref(s) matched live local exposure; {resized_reduce_only_count} resized to local position notional; {dust_flattened_reduce_only_count} expanded to flatten sub-min residuals; {skipped_no_exposure_count} skipped as no-op; {verification_error_count} verification error(s)"
            ),
        )],
    )
}

fn copy_live_daemon_submit_accounts_in_scope(
    config: &config::AppConfig,
    options: &CopyLiveDaemonSupervisorOptions,
    executable_refs: &[CopyLiveDaemonWouldSubmitRef],
) -> bool {
    let selected_accounts =
        copy_execution_canary_target_accounts(config, &options.account_ids, None)
            .into_iter()
            .collect::<HashSet<_>>();
    !selected_accounts.is_empty()
        && executable_refs.iter().all(|plan| {
            selected_accounts.contains(&plan.order.account_id)
                && config.account(&plan.order.account_id).is_some()
        })
}

async fn copy_live_daemon_persistent_live_submit(
    config: &config::AppConfig,
    options: &CopyLiveDaemonSupervisorOptions,
    submit_plan_contract: &CopyLiveDaemonSubmitPlanContract,
    executable_refs: &[CopyLiveDaemonWouldSubmitRef],
    suppressed_refs: &[CopyLiveDaemonSuppressedWouldSubmitRef],
    base_snapshot: &strategies::smart_money::CopyPersistenceSnapshot,
    approved_records: &[strategies::smart_money::CopyDryRunShadowRecord],
) -> CopyLiveDaemonPersistentLiveSubmitReport {
    let mut checks = vec![
        copy_shadow_smoke_check(
            "submit_requested",
            options.submit,
            "--submit true is required before persistent daemon live submit can run",
        ),
        copy_shadow_smoke_check(
            "submit_plan_contract_ok",
            submit_plan_contract.ok,
            format!("submit_plan_contract.ok={}", submit_plan_contract.ok),
        ),
        copy_shadow_smoke_check(
            "suppressed_refs_not_submitted",
            suppressed_refs.is_empty()
                || executable_refs.iter().all(|executable| {
                    suppressed_refs
                        .iter()
                        .all(|suppressed| suppressed.plan.order.cloid != executable.order.cloid)
                }),
            format!(
                "{} executable ref(s); {} suppressed ref(s) excluded",
                executable_refs.len(),
                suppressed_refs.len()
            ),
        ),
        copy_shadow_smoke_check(
            "live_gate",
            options.live_gate,
            "persistent live submit requires --live-gate true",
        ),
        copy_shadow_smoke_check(
            "allow_live_submit",
            options.allow_live_submit,
            "persistent live submit requires --allow-live-submit true",
        ),
        copy_shadow_smoke_check(
            "mainnet_confirmation",
            config.app.environment != "mainnet" || options.confirm_mainnet_live,
            "mainnet persistent live submit requires --confirm-mainnet-live true",
        ),
        copy_shadow_smoke_check(
            "process_dry_run_disabled",
            !config.app.dry_run,
            "persistent live submit requires app.dry_run=false",
        ),
        copy_shadow_smoke_check(
            "manual_live_enabled",
            config.manual_ops.manual_live_enabled,
            "persistent live submit uses the same manual live gate as signed smoke",
        ),
        copy_shadow_smoke_check(
            "mainnet_live_enabled",
            config.app.environment != "mainnet" || config.manual_ops.mainnet_live_enabled,
            "mainnet persistent live submit requires manual_ops.mainnet_live_enabled=true",
        ),
        copy_shadow_smoke_check(
            "selected_submit_accounts",
            copy_live_daemon_submit_accounts_in_scope(config, options, executable_refs),
            format!(
                "{} selected account(s); {} submit-eligible ref(s) must belong to configured selected accounts",
                options.account_ids.len(),
                executable_refs.len()
            ),
        ),
        copy_shadow_smoke_check(
            "single_open_order_submit",
            executable_refs
                .iter()
                .filter(|plan| !plan.order.reduce_only)
                .count()
                <= options.max_live_orders,
            format!(
                "{} executable open/increase order(s), max_live_orders {}",
                executable_refs
                    .iter()
                    .filter(|plan| !plan.order.reduce_only)
                    .count(),
                options.max_live_orders
            ),
        ),
        copy_shadow_smoke_check(
            "cleanup_slippage_valid",
            options.cleanup_max_slippage_bps.is_finite()
                && (0.0..10_000.0).contains(&options.cleanup_max_slippage_bps),
            format!(
                "cleanup_max_slippage_bps={} must be >= 0 and < 10000",
                options.cleanup_max_slippage_bps
            ),
        ),
    ];

    let max_open_notional = executable_refs
        .iter()
        .filter(|plan| !plan.order.reduce_only)
        .map(|plan| plan.order.notional_usd)
        .fold(0.0_f64, f64::max);
    checks.push(copy_shadow_smoke_check(
        "cleanup_notional_limit",
        max_open_notional <= config.manual_ops.max_manual_order_notional_usd,
        format!(
            "max planned open notional {max_open_notional:.6} must be <= manual_ops.max_manual_order_notional_usd {:.6}",
            config.manual_ops.max_manual_order_notional_usd
        ),
    ));

    if !checks.iter().all(|check| check.ok) {
        return CopyLiveDaemonPersistentLiveSubmitReport {
            ok: false,
            mode: "persistent_live_submit".to_string(),
            submit_requested: options.submit,
            submit_plan_contract_ok: submit_plan_contract.ok,
            submitted_reports: Vec::new(),
            order_evidence: Vec::new(),
            cleanup_runbooks: Vec::new(),
            cleanup_errors: Vec::new(),
            ledger_reconciliations: Vec::new(),
            ledger_reconciliation_snapshot: strategies::smart_money::CopyPersistenceSnapshot::new(
                domain::now_ms(),
                Vec::new(),
                &strategies::smart_money::CopyLedger::new(),
            ),
            checks,
        };
    }

    let (submit_ready_refs, reduce_only_exposure_checks) =
        copy_live_daemon_filter_submit_refs_for_live_reduce_exposure(config, executable_refs).await;
    checks.extend(reduce_only_exposure_checks);
    if !checks.iter().all(|check| check.ok) {
        return CopyLiveDaemonPersistentLiveSubmitReport {
            ok: false,
            mode: "persistent_live_submit".to_string(),
            submit_requested: options.submit,
            submit_plan_contract_ok: submit_plan_contract.ok,
            submitted_reports: Vec::new(),
            order_evidence: Vec::new(),
            cleanup_runbooks: Vec::new(),
            cleanup_errors: Vec::new(),
            ledger_reconciliations: Vec::new(),
            ledger_reconciliation_snapshot: strategies::smart_money::CopyPersistenceSnapshot::new(
                domain::now_ms(),
                Vec::new(),
                &strategies::smart_money::CopyLedger::new(),
            ),
            checks,
        };
    }

    let mut submitted_reports = Vec::new();
    let mut progress_snapshot_save_count = 0usize;
    let mut progress_snapshot_errors = Vec::new();
    let mut live_submit_timeout_abort: Option<(String, usize)> = None;
    let mut live_submit_transport_abort: Option<(String, usize, String)> = None;
    for plan in &submit_ready_refs {
        let mut submit_timed_out = false;
        let submitted_report = match tokio::time::timeout(
            Duration::from_secs(COPY_DAEMON_SUBMIT_REF_TIMEOUT_SECS),
            execute_copy_daemon_submit_ref(config, options, plan),
        )
        .await
        {
            Ok(Ok(report)) => report,
            Ok(Err(error)) => domain::WorkerReport::Error(domain::WorkerError {
                worker_id: plan.order.worker_id.clone(),
                account_id: plan.order.account_id.clone(),
                message: error.to_string(),
                error_at_ms: domain::now_ms(),
            }),
            Err(_) => {
                submit_timed_out = true;
                domain::WorkerReport::Error(domain::WorkerError {
                    worker_id: plan.order.worker_id.clone(),
                    account_id: plan.order.account_id.clone(),
                    message: format!(
                        "COPY_LIVE_SUBMIT_TIMEOUT: submit ref {} {} timed out after {}s; exchange state must be reconciled before retry",
                        plan.order.account_id, plan.order.coin, COPY_DAEMON_SUBMIT_REF_TIMEOUT_SECS
                    ),
                    error_at_ms: domain::now_ms(),
                })
            }
        };
        submitted_reports.push(submitted_report);
        if let Some(error_message) = submitted_reports
            .last()
            .and_then(copy_live_daemon_report_submit_transport_failure_message)
        {
            live_submit_transport_abort = Some((
                format!("{} {}", plan.order.account_id, plan.order.coin),
                submit_ready_refs
                    .len()
                    .saturating_sub(submitted_reports.len()),
                error_message.to_string(),
            ));
            break;
        }
        if submit_timed_out {
            live_submit_timeout_abort = Some((
                format!("{} {}", plan.order.account_id, plan.order.coin),
                submit_ready_refs
                    .len()
                    .saturating_sub(submitted_reports.len()),
            ));
            break;
        }
        if copy_canary_has_live_submission(&submitted_reports) {
            match copy_live_daemon_save_live_submit_progress_snapshot(
                options,
                approved_records,
                &submitted_reports,
            ) {
                Ok(true) => progress_snapshot_save_count += 1,
                Ok(false) => {}
                Err(error) => progress_snapshot_errors.push(error.to_string()),
            }
        }
    }
    let live_submitted_count = copy_canary_live_submitted_reports(&submitted_reports).len();
    let mut order_evidence_timed_out = false;
    let order_evidence = if live_submitted_count > 0 {
        match tokio::time::timeout(
            Duration::from_secs(COPY_DAEMON_ORDER_EVIDENCE_TIMEOUT_SECS),
            collect_copy_canary_order_evidence(config, &submitted_reports),
        )
        .await
        {
            Ok(evidence) => evidence,
            Err(_) => {
                order_evidence_timed_out = true;
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };
    let cleanup_targets =
        copy_daemon_submitted_reports_needing_cleanup(&submitted_reports, &submit_ready_refs);
    let (cleanup_runbooks, cleanup_errors) = if options.hold_positions_after_submit {
        (Vec::new(), Vec::new())
    } else {
        let cleanup_options = CopyExecutionCanaryOptions {
            leaders: options.leaders.clone(),
            account_ids: options.account_ids.clone(),
            coin: options.coin.clone(),
            side: options.side,
            local_account_id: options.local_account_id.clone(),
            shadow_history_path: options.shadow_history_path.clone(),
            leader_notional_usd: options.leader_notional_usd,
            leader_size: options.leader_size,
            live: true,
            allow_live_submit: options.allow_live_submit,
            confirm_mainnet_live: options.confirm_mainnet_live,
            cleanup_after_submit: true,
            cleanup_max_slippage_bps: options.cleanup_max_slippage_bps,
            preflight_only: false,
            max_orders: options.max_live_orders,
        };
        execute_copy_canary_cleanup_runbooks(config, &cleanup_options, &cleanup_targets).await
    };
    let (ledger_reconciliations, ledger_reconciliation_snapshot) = reconcile_copy_canary_ledger(
        Some(base_snapshot),
        approved_records,
        &submitted_reports,
        &order_evidence,
    );

    let pre_submit_skipped_count = submitted_reports
        .iter()
        .filter(|report| copy_live_daemon_report_is_safe_pre_submit_skip(report))
        .count();
    let submitted_reports_ok = submitted_reports.iter().all(|report| {
        matches!(report, domain::WorkerReport::Submitted(_))
            || copy_live_daemon_report_is_safe_pre_submit_skip(report)
    });
    checks.push(copy_shadow_smoke_check(
        "submitted_reports",
        live_submitted_count + pre_submit_skipped_count == submit_ready_refs.len()
            && submitted_reports_ok,
        format!(
            "{} live submitted report(s), {} pre-submit skipped ref(s), for {} submit-eligible ref(s); {} reduce-only no-op ref(s) skipped",
            live_submitted_count,
            pre_submit_skipped_count,
            submit_ready_refs.len(),
            executable_refs.len().saturating_sub(submit_ready_refs.len())
        ),
    ));
    if let Some((timed_out_ref, skipped_after_timeout)) = &live_submit_timeout_abort {
        checks.push(copy_shadow_smoke_check(
            "submit_timeout_fail_closed",
            false,
            format!(
                "live submit timed out for {timed_out_ref}; skipped {skipped_after_timeout} remaining submit ref(s) until exchange reconciliation confirms state"
            ),
        ));
    }
    if let Some((failed_ref, skipped_after_failure, error_message)) = &live_submit_transport_abort {
        checks.push(copy_shadow_smoke_check(
            "submit_transport_fail_closed",
            true,
            format!(
                "live submit transport failed for {failed_ref}; skipped {skipped_after_failure} remaining submit ref(s) until the next exchange reconciliation; error={error_message}"
            ),
        ));
    }
    checks.push(copy_shadow_smoke_check(
        "live_submit_progress_snapshot_saved",
        live_submitted_count == 0
            || (progress_snapshot_save_count >= live_submitted_count
                && progress_snapshot_errors.is_empty()),
        if live_submitted_count == 0 {
            "no live submitted report required an immediate progress snapshot".to_string()
        } else if progress_snapshot_errors.is_empty() {
            format!(
                "{progress_snapshot_save_count} immediate progress snapshot save(s) for {live_submitted_count} live submitted report(s)"
            )
        } else {
            format!(
                "{} immediate progress snapshot save error(s): {}",
                progress_snapshot_errors.len(),
                progress_snapshot_errors.join("; ")
            )
        },
    ));
    let evidence_ok = live_submitted_count == 0
        || (order_evidence.len() == live_submitted_count
            && order_evidence
                .iter()
                .all(copy_execution_canary_order_evidence_ok));
    checks.push(copy_shadow_smoke_check(
        "order_status_evidence",
        evidence_ok,
        if order_evidence_timed_out {
            format!(
                "{} live submitted report(s), order evidence collection timed out after {}s",
                live_submitted_count, COPY_DAEMON_ORDER_EVIDENCE_TIMEOUT_SECS
            )
        } else {
            format!(
                "{} live submitted report(s), {} order evidence record(s)",
                live_submitted_count,
                order_evidence.len()
            )
        },
    ));
    let open_submitted_count = copy_canary_live_submitted_reports(&cleanup_targets).len();
    let cleanup_ok = if options.hold_positions_after_submit {
        cleanup_errors.is_empty()
    } else {
        open_submitted_count == 0
            || cleanup_runbooks.len() == open_submitted_count
                && cleanup_errors.is_empty()
                && cleanup_runbooks
                    .iter()
                    .all(copy_execution_canary_cleanup_runbook_ok)
    };
    checks.push(copy_shadow_smoke_check(
        if options.hold_positions_after_submit {
            "follow_position_cleanup_policy"
        } else {
            "cleanup_runbook_completed"
        },
        cleanup_ok,
        if options.hold_positions_after_submit {
            format!(
                "follow-position mode holds {} open submission(s) until target close signals; cleanup skipped, {} cleanup error(s)",
                open_submitted_count,
                cleanup_errors.len()
            )
        } else {
            format!(
                "{} open submission(s) require cleanup; {} cleanup runbook(s), {} cleanup error(s)",
                open_submitted_count,
                cleanup_runbooks.len(),
                cleanup_errors.len()
            )
        },
    ));
    let ledger_ok =
        live_submitted_count == 0 || ledger_reconciliations.iter().all(|result| result.applied);
    checks.push(copy_shadow_smoke_check(
        "ledger_reconciliation",
        ledger_ok,
        format!(
            "{} live submitted report(s), {} ledger reconciliation result(s)",
            live_submitted_count,
            ledger_reconciliations.len()
        ),
    ));

    CopyLiveDaemonPersistentLiveSubmitReport {
        ok: checks.iter().all(|check| check.ok),
        mode: "persistent_live_submit".to_string(),
        submit_requested: options.submit,
        submit_plan_contract_ok: submit_plan_contract.ok,
        submitted_reports,
        order_evidence,
        cleanup_runbooks,
        cleanup_errors,
        ledger_reconciliations,
        ledger_reconciliation_snapshot,
        checks,
    }
}

fn copy_live_daemon_report_is_safe_pre_submit_skip(report: &domain::WorkerReport) -> bool {
    let domain::WorkerReport::Error(error) = report else {
        return false;
    };
    copy_live_daemon_error_is_safe_pre_submit_skip(&error.message)
}

fn copy_live_daemon_report_is_submit_transport_failure(report: &domain::WorkerReport) -> bool {
    copy_live_daemon_report_submit_transport_failure_message(report).is_some()
}

fn copy_live_daemon_report_submit_transport_failure_message(
    report: &domain::WorkerReport,
) -> Option<&str> {
    let domain::WorkerReport::Error(error) = report else {
        return None;
    };
    if copy_live_daemon_error_is_submit_transport_failure(&error.message) {
        Some(error.message.as_str())
    } else {
        None
    }
}

fn copy_live_daemon_error_is_safe_pre_submit_skip(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("failed to set ")
        && normalized.contains(" leverage to ")
        && normalized.contains(" before copy submit")
        || normalized.contains("copy_live_max_leverage_timeout")
        || normalized.contains("copy_live_leverage_update_timeout")
        || normalized.contains("copy submit skipped before exchange")
            && normalized.contains("below exchange minimum")
        || normalized.contains("order size rounds to zero for ")
        || normalized.contains("exchange returned action-level order error")
            && normalized.contains("minimum value")
        || normalized.contains("exchange returned action-level order error")
            && normalized.contains("reduce only order would increase position")
}

fn copy_live_daemon_error_is_submit_transport_failure(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("copy_live_ws_post_failed")
        || normalized.contains("hyperliquid websocket order post failed")
        || normalized.contains("websocket post worker is not running")
        || normalized.contains("websocket post request timed out")
        || normalized.contains("websocket post response channel closed")
        || normalized.contains("failed to send websocket post request")
        || normalized.contains("websocket post channel closed")
        || normalized.contains("websocket post read error")
        || normalized.contains("websocket post stream ended")
}

async fn copy_live_daemon_suppress_refs_below_effective_min(
    config: &config::AppConfig,
    options: &CopyLiveDaemonSupervisorOptions,
    refs: &[CopyLiveDaemonWouldSubmitRef],
) -> (
    Vec<CopyLiveDaemonWouldSubmitRef>,
    Vec<CopyLiveDaemonSuppressedWouldSubmitRef>,
) {
    let mut executable = Vec::new();
    let mut suppressed = Vec::new();
    for plan in refs {
        if plan.order.reduce_only {
            executable.push(plan.clone());
            continue;
        }

        let scoped_config = copy_daemon_config_for_coin(config, &plan.order.coin);
        let account = match scoped_config.account(&plan.order.account_id).cloned() {
            Some(account) => account,
            None => {
                suppressed.push(CopyLiveDaemonSuppressedWouldSubmitRef {
                    plan: plan.clone(),
                    reason_code: "COPY_DAEMON_ACCOUNT_NOT_CONFIGURED".to_string(),
                    message: format!(
                        "{} is not configured for copy submit",
                        plan.order.account_id
                    ),
                });
                continue;
            }
        };

        let approved_order = match approved_copy_daemon_order_from_ref(
            &scoped_config,
            options,
            &account,
            plan,
            false,
        ) {
            Ok(order) => order,
            Err(error) => {
                suppressed.push(CopyLiveDaemonSuppressedWouldSubmitRef {
                    plan: plan.clone(),
                    reason_code: "COPY_DAEMON_ORDER_PREPARE_FAILED".to_string(),
                    message: error.to_string(),
                });
                continue;
            }
        };

        match copy_live_daemon_open_order_effective_min_check_bounded(
            &scoped_config,
            &approved_order,
        )
        .await
        {
            Ok(Some(message)) => suppressed.push(CopyLiveDaemonSuppressedWouldSubmitRef {
                plan: plan.clone(),
                reason_code: "COPY_DAEMON_EFFECTIVE_NOTIONAL_BELOW_MIN".to_string(),
                message,
            }),
            Ok(None) => executable.push(plan.clone()),
            Err(error) => suppressed.push(CopyLiveDaemonSuppressedWouldSubmitRef {
                plan: plan.clone(),
                reason_code: "COPY_DAEMON_EFFECTIVE_MIN_CHECK_FAILED".to_string(),
                message: error.to_string(),
            }),
        }
    }
    (executable, suppressed)
}

fn copy_daemon_submitted_reports_needing_cleanup(
    submitted_reports: &[domain::WorkerReport],
    executable_refs: &[CopyLiveDaemonWouldSubmitRef],
) -> Vec<domain::WorkerReport> {
    let open_cloids = executable_refs
        .iter()
        .filter(|plan| !plan.order.reduce_only)
        .map(|plan| plan.order.cloid.as_str())
        .collect::<HashSet<_>>();
    submitted_reports
        .iter()
        .filter(|report| match report {
            domain::WorkerReport::Submitted(submitted) if !submitted.dry_run => {
                open_cloids.contains(submitted.cloid.as_str())
            }
            _ => false,
        })
        .cloned()
        .collect()
}

fn copy_live_daemon_merge_persistent_live_submit_reports(
    submit_requested: bool,
    submit_plan_contract_ok: bool,
    chunks: Vec<CopyLiveDaemonPersistentLiveSubmitReport>,
) -> CopyLiveDaemonPersistentLiveSubmitReport {
    if chunks.is_empty() {
        let checks = vec![copy_shadow_smoke_check(
            "persistent_live_submit_chunks",
            !submit_requested,
            "no persistent live submit chunks were produced",
        )];
        return CopyLiveDaemonPersistentLiveSubmitReport {
            ok: checks.iter().all(|check| check.ok),
            mode: "persistent_live_submit".to_string(),
            submit_requested,
            submit_plan_contract_ok,
            submitted_reports: Vec::new(),
            order_evidence: Vec::new(),
            cleanup_runbooks: Vec::new(),
            cleanup_errors: Vec::new(),
            ledger_reconciliations: Vec::new(),
            ledger_reconciliation_snapshot: strategies::smart_money::CopyPersistenceSnapshot::new(
                domain::now_ms(),
                Vec::new(),
                &strategies::smart_money::CopyLedger::new(),
            ),
            checks,
        };
    }

    let mut submitted_reports = Vec::new();
    let mut order_evidence = Vec::new();
    let mut cleanup_runbooks = Vec::new();
    let mut cleanup_errors = Vec::new();
    let mut ledger_reconciliations = Vec::new();
    let mut checks = Vec::new();
    let mut seen_event_keys = Vec::new();
    let mut ledger_entries = Vec::new();
    let chunk_count = chunks.len();
    let chunks_ok = chunks.iter().all(|chunk| chunk.ok);
    for mut chunk in chunks {
        submitted_reports.append(&mut chunk.submitted_reports);
        order_evidence.append(&mut chunk.order_evidence);
        cleanup_runbooks.append(&mut chunk.cleanup_runbooks);
        cleanup_errors.append(&mut chunk.cleanup_errors);
        ledger_reconciliations.append(&mut chunk.ledger_reconciliations);
        checks.append(&mut chunk.checks);
        seen_event_keys.append(&mut chunk.ledger_reconciliation_snapshot.seen_event_keys);
        ledger_entries.append(&mut chunk.ledger_reconciliation_snapshot.ledger_entries);
    }
    checks.push(copy_shadow_smoke_check(
        "persistent_live_submit_chunks",
        chunks_ok,
        format!("{chunk_count} persistent live submit chunk(s) merged"),
    ));
    let ledger_reconciliation_snapshot = copy_live_daemon_persistence_snapshot_for_save(
        strategies::smart_money::CopyPersistenceSnapshot {
            schema_version: 1,
            saved_at_ms: domain::now_ms(),
            seen_event_keys,
            ledger_entries,
        },
    );

    CopyLiveDaemonPersistentLiveSubmitReport {
        ok: checks.iter().all(|check| check.ok),
        mode: "persistent_live_submit".to_string(),
        submit_requested,
        submit_plan_contract_ok,
        submitted_reports,
        order_evidence,
        cleanup_runbooks,
        cleanup_errors,
        ledger_reconciliations,
        ledger_reconciliation_snapshot,
        checks,
    }
}

async fn execute_copy_daemon_submit_ref(
    config: &config::AppConfig,
    options: &CopyLiveDaemonSupervisorOptions,
    plan: &CopyLiveDaemonWouldSubmitRef,
) -> Result<domain::WorkerReport> {
    let scoped_config = copy_daemon_config_for_coin(config, &plan.order.coin);
    let account = scoped_config
        .account(&plan.order.account_id)
        .cloned()
        .with_context(|| format!("account {} not found", plan.order.account_id))?;
    let approved_order =
        approved_copy_daemon_order_from_ref(&scoped_config, options, &account, plan, false)?;
    if let Some(message) =
        copy_live_daemon_open_order_effective_min_check_bounded(&scoped_config, &approved_order)
            .await?
    {
        return Ok(domain::WorkerReport::Error(domain::WorkerError {
            worker_id: approved_order.worker_id.clone(),
            account_id: approved_order.account_id.clone(),
            message,
            error_at_ms: domain::now_ms(),
        }));
    }
    let vault_password = copy_daemon_vault_password(&scoped_config)?;
    let max_leverage = match tokio::time::timeout(
        Duration::from_secs(COPY_DAEMON_MAX_LEVERAGE_TIMEOUT_SECS),
        copy_daemon_live_plan_max_leverage(&scoped_config, plan),
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => anyhow::bail!(
            "COPY_LIVE_MAX_LEVERAGE_TIMEOUT: {} {} max leverage lookup timed out after {}s",
            plan.order.account_id,
            plan.order.coin,
            COPY_DAEMON_MAX_LEVERAGE_TIMEOUT_SECS
        ),
    };
    if let Some(leverage_options) =
        copy_daemon_live_leverage_update_options_with_max(options, plan, max_leverage)?
    {
        let leverage = leverage_options.leverage;
        match tokio::time::timeout(
            Duration::from_secs(COPY_DAEMON_LEVERAGE_UPDATE_TIMEOUT_SECS),
            trading::execute_manual_leverage_update(
                scoped_config.clone(),
                leverage_options,
                vault_password.as_deref(),
            ),
        )
        .await
        {
            Ok(result) => {
                result.with_context(|| {
                    format!(
                        "failed to set {} leverage to {}x before copy submit",
                        plan.order.coin, leverage
                    )
                })?;
            }
            Err(_) => anyhow::bail!(
                "COPY_LIVE_LEVERAGE_UPDATE_TIMEOUT: {} {} leverage update to {}x timed out after {}s; exchange state must be reconciled before retry",
                plan.order.account_id,
                plan.order.coin,
                leverage,
                COPY_DAEMON_LEVERAGE_UPDATE_TIMEOUT_SECS
            ),
        }
    }
    let secret = secrets::load_account_secret(&scoped_config, &account, vault_password.as_deref())?;
    let executor = trading::AccountExecutor::live(scoped_config, account, secret);
    match tokio::time::timeout(
        Duration::from_secs(COPY_DAEMON_ORDER_SUBMIT_TIMEOUT_SECS),
        executor.submit_fast(approved_order),
    )
    .await
    {
        Ok(report) => Ok(report),
        Err(_) => anyhow::bail!(
            "COPY_LIVE_ORDER_SUBMIT_TIMEOUT: submit ref {} {} timed out after {}s; exchange state must be reconciled before retry",
            plan.order.account_id,
            plan.order.coin,
            COPY_DAEMON_ORDER_SUBMIT_TIMEOUT_SECS
        ),
    }
}

fn copy_daemon_vault_password(config: &config::AppConfig) -> Result<Option<String>> {
    if let Ok(password) = std::env::var("TRADE_XYZ_VAULT_PASSWORD")
        && !password.trim().is_empty()
    {
        return Ok(Some(password));
    }
    let vault_path = std::path::PathBuf::from(&config.secrets.vault_path);
    secrets::read_cached_vault_password(&vault_path, domain::now_ms())
}

fn copy_live_daemon_signer_preflight_checks(
    config: &config::AppConfig,
    target_accounts: &[String],
    submit_requested: bool,
) -> Vec<CopyShadowSmokeCheck> {
    if !submit_requested {
        return vec![copy_shadow_smoke_check(
            "copy_signers_available",
            true,
            "signer preflight skipped in no-submit mode",
        )];
    }
    let vault_password = match copy_daemon_vault_password(config) {
        Ok(password) => password,
        Err(error) => {
            return vec![copy_shadow_smoke_check(
                "copy_signers_available",
                false,
                format!("failed to read Vault session cache for signer preflight: {error:#}"),
            )];
        }
    };
    let mut missing = Vec::new();
    for account_id in target_accounts {
        match config.account(account_id) {
            Some(account) => {
                if let Err(error) =
                    secrets::load_account_secret(config, account, vault_password.as_deref())
                {
                    missing.push(format!("{account_id}: {error:#}"));
                }
            }
            None => missing.push(format!("{account_id}: account is not configured")),
        }
    }
    if missing.is_empty() {
        vec![copy_shadow_smoke_check(
            "copy_signers_available",
            true,
            format!(
                "all {} selected local account signer(s) can be loaded from the current Vault session/cache",
                target_accounts.len()
            ),
        )]
    } else {
        vec![copy_shadow_smoke_check(
            "copy_signers_available",
            false,
            format!(
                "selected local account signer preflight failed: {}",
                missing.join("; ")
            ),
        )]
    }
}

async fn copy_live_daemon_open_order_effective_min_check(
    config: &config::AppConfig,
    order: &domain::ApprovedOrder,
) -> Result<Option<String>> {
    if order.reduce_only {
        return Ok(None);
    }

    let (_, dex) = copy_daemon_market_dex_for_coin(&order.coin);
    let effective_notional = if dex.as_deref() == Some("spot") {
        let snapshot =
            hyperliquid::fetch_spot_market_snapshot_cached(&config.app.environment, 15_000)
                .await
                .context("failed to fetch spot market metadata for copy submit min check")?;
        let plan = hyperliquid::build_spot_order_plan(
            &snapshot,
            &order.coin,
            matches!(order.side, domain::OrderSide::Buy),
            order.notional_usd,
            order.price,
            order.max_slippage_bps,
        )
        .context("failed to build spot copy submit precision plan")?;
        trading::effective_order_notional_usd(plan.limit_price, plan.size)
    } else {
        let snapshot = hyperliquid::fetch_xyz_market_snapshot_cached(
            &config.app.environment,
            dex.as_deref().unwrap_or(config.hyperliquid.dex.as_str()),
            15_000,
        )
        .await
        .context("failed to fetch perp market metadata for copy submit min check")?;
        let plan = trading::build_signed_order_plan(
            &snapshot,
            &order.coin,
            order.side,
            order.notional_usd,
            order.max_slippage_bps,
            order.execution_mode,
            order.exact_size,
        )
        .context("failed to build perp copy submit precision plan")?;
        trading::effective_order_notional_usd(plan.limit_price, plan.size)
    };

    if effective_notional + 1e-9 < trading::HYPERLIQUID_MIN_ORDER_NOTIONAL_USD {
        return Ok(Some(format!(
            "copy submit skipped before exchange: {} {} requested_notional={:.6} effective_notional={:.6} below exchange minimum {:.6}",
            order.account_id,
            order.coin,
            order.notional_usd,
            effective_notional,
            trading::HYPERLIQUID_MIN_ORDER_NOTIONAL_USD
        )));
    }
    Ok(None)
}

async fn copy_live_daemon_open_order_effective_min_check_bounded(
    config: &config::AppConfig,
    order: &domain::ApprovedOrder,
) -> Result<Option<String>> {
    match tokio::time::timeout(
        Duration::from_secs(COPY_DAEMON_EFFECTIVE_MIN_CHECK_TIMEOUT_SECS),
        copy_live_daemon_open_order_effective_min_check(config, order),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => anyhow::bail!(
            "copy submit effective min check for {} {} timed out after {}s",
            order.account_id,
            order.coin,
            COPY_DAEMON_EFFECTIVE_MIN_CHECK_TIMEOUT_SECS
        ),
    }
}

fn copy_daemon_config_for_coin(config: &config::AppConfig, coin: &str) -> config::AppConfig {
    let mut scoped = config.clone();
    let (_, dex) = copy_daemon_market_dex_for_coin(coin);
    if let Some(dex) = dex {
        scoped.hyperliquid.dex = dex;
    }
    scoped
}

fn copy_daemon_market_dex_for_coin(coin: &str) -> (Option<String>, Option<String>) {
    let trimmed = coin.trim();
    if trimmed.contains('/') || trimmed.starts_with('@') {
        return (
            Some(config::MARKET_SPOT.to_string()),
            Some("spot".to_string()),
        );
    }
    if let Some((dex, _symbol)) = trimmed.split_once(':') {
        let dex = dex.trim().to_ascii_lowercase();
        if !dex.is_empty() {
            return (Some(format!("{dex}_perp")), Some(dex));
        }
    }
    (
        Some(config::MARKET_HL_PERP.to_string()),
        Some(String::new()),
    )
}

fn copy_daemon_normalize_market_scope(raw: &[String]) -> Vec<String> {
    let tokens = raw
        .iter()
        .flat_map(|item| {
            item.split(|ch: char| {
                ch.is_whitespace() || ch == ',' || ch == ';' || ch == '，' || ch == '；'
            })
        })
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    let normalized = config::normalize_market_list(&tokens);
    if normalized.is_empty() {
        return config::supported_market_ids()
            .iter()
            .map(|market| (*market).to_string())
            .collect();
    }
    normalized
}

fn copy_daemon_market_scope_allows_open(
    options: &CopyLiveDaemonSupervisorOptions,
    coin: &str,
) -> bool {
    let (market, _) = copy_daemon_market_dex_for_coin(coin);
    let market = market.unwrap_or_else(|| config::MARKET_XYZ_PERP.to_string());
    copy_daemon_normalize_market_scope(&options.markets)
        .iter()
        .any(|allowed| allowed == &market)
}

#[cfg(test)]
fn copy_daemon_live_leverage_update_options(
    options: &CopyLiveDaemonSupervisorOptions,
    plan: &CopyLiveDaemonWouldSubmitRef,
) -> Result<Option<trading::ManualLeverageUpdateOptions>> {
    copy_daemon_live_leverage_update_options_with_max(options, plan, None)
}

fn copy_daemon_live_leverage_update_options_with_max(
    options: &CopyLiveDaemonSupervisorOptions,
    plan: &CopyLiveDaemonWouldSubmitRef,
    max_leverage: Option<u32>,
) -> Result<Option<trading::ManualLeverageUpdateOptions>> {
    if plan.order.reduce_only {
        return Ok(None);
    }
    let (_, dex) = copy_daemon_market_dex_for_coin(&plan.order.coin);
    if dex.as_deref() == Some("spot") {
        return Ok(None);
    }

    let target = copy_daemon_live_target_leverage()?;
    let leverage = max_leverage
        .filter(|max| *max >= 1)
        .map(|max| target.min(max))
        .unwrap_or(target);
    Ok(Some(trading::ManualLeverageUpdateOptions {
        account_id: plan.order.account_id.clone(),
        coin: plan.order.coin.clone(),
        leverage,
        margin_mode: "isolated".to_string(),
        submit: true,
        confirm_mainnet_live: options.confirm_mainnet_live,
    }))
}

async fn copy_daemon_live_plan_max_leverage(
    config: &config::AppConfig,
    plan: &CopyLiveDaemonWouldSubmitRef,
) -> Result<Option<u32>> {
    if plan.order.reduce_only {
        return Ok(None);
    }
    let (_, dex) = copy_daemon_market_dex_for_coin(&plan.order.coin);
    if dex.as_deref() == Some("spot") {
        return Ok(None);
    }
    let snapshot = hyperliquid::fetch_xyz_market_snapshot_cached(
        &config.app.environment,
        dex.as_deref().unwrap_or(config.hyperliquid.dex.as_str()),
        15_000,
    )
    .await
    .context("failed to fetch copy submit market metadata")?;
    let asset = snapshot.asset(&plan.order.coin)?;
    Ok(asset.meta.max_leverage)
}

fn copy_daemon_live_target_leverage() -> Result<u32> {
    let leverage = strategies::smart_money::COPY_MAX_LEVERAGE;
    anyhow::ensure!(
        leverage.is_finite() && leverage >= 1.0,
        "copy max leverage must be a finite value >= 1"
    );
    let rounded = leverage.round();
    anyhow::ensure!(
        (leverage - rounded).abs() < f64::EPSILON,
        "copy max leverage must be a whole-number exchange leverage"
    );
    Ok(rounded as u32)
}

fn approved_copy_daemon_order_from_ref(
    config: &config::AppConfig,
    options: &CopyLiveDaemonSupervisorOptions,
    account: &config::AccountConfig,
    plan: &CopyLiveDaemonWouldSubmitRef,
    execution_dry_run: bool,
) -> Result<domain::ApprovedOrder> {
    let (market, dex) = copy_daemon_market_dex_for_coin(&plan.order.coin);
    let intent = domain::TradeIntent {
        intent_id: format!(
            "copy-daemon-intent-{}-{}",
            plan.record_index, plan.signal_id
        ),
        signal_id: Some(plan.signal_id.clone()),
        worker_id: plan.order.worker_id.clone(),
        account_id: plan.order.account_id.clone(),
        target_accounts: vec![plan.order.account_id.clone()],
        strategy_id: "smart_money_copy".to_string(),
        created_at_ms: domain::now_ms(),
        market,
        dex,
        coin: plan.order.coin.clone(),
        side: plan.order.side,
        intent_kind: if plan.order.reduce_only {
            domain::IntentKind::Reduce
        } else {
            domain::IntentKind::Open
        },
        sizing: domain::SizingRequest {
            notional_usd: plan.order.notional_usd,
        },
        price_policy: domain::PricePolicy::MarketWithSlippageLimit {
            max_slippage_bps: options.max_slippage_bps,
        },
        execution_policy: domain::ExecutionPolicy::Taker,
        reduce_only: plan.order.reduce_only,
        reason: format!(
            "persistent daemon submit ref {} from leader {}",
            plan.record_index, plan.leader_id
        ),
        source: domain::IntentSource::Strategy,
        source_event_id: Some(plan.signal_id.clone()),
        expires_at_ms: None,
    };
    let risk_context =
        risk::RiskContext::from_account_for_module(config, account, execution_dry_run, "copy");
    match risk::RiskGateway::dry_run_default().evaluate(&risk_context, intent) {
        risk::RiskDecision::Approved(mut order) => {
            order.cloid = plan.order.cloid.clone();
            Ok(order)
        }
        risk::RiskDecision::Rejected(rejection) => anyhow::bail!(
            "persistent daemon submit ref rejected by RiskGateway: {} {}",
            rejection.reason_code,
            rejection.message
        ),
    }
}

fn copy_live_daemon_supervisor_pipeline(
    config: &config::AppConfig,
    options: &CopyLiveDaemonSupervisorOptions,
    account: &config::AccountConfig,
    target_accounts: &[String],
    watcher_leaders: &[strategies::smart_money::SmartMoneyLeaderWatch],
    persistence: &strategies::smart_money::CopyPersistenceSnapshot,
) -> strategies::smart_money::CopyDryRunShadowPipeline {
    let strategy = strategies::smart_money::SmartMoneyCopyStrategy::new_with_seen_event_keys(
        strategies::smart_money::SmartMoneyCopyConfig {
            strategy_id: "copy_live_daemon_supervisor".to_string(),
            default_copy_ratio: 1.0,
            max_slippage_bps: options.max_slippage_bps,
            leaders: watcher_leaders
                .iter()
                .map(|leader| strategies::smart_money::LeaderRule {
                    leader_id: leader.leader_id.clone(),
                    leader_address: leader.leader_address.clone(),
                    enabled: true,
                    copy_ratio: 1.0,
                })
                .collect(),
            symbol_limits: Vec::new(),
        },
        persistence.seen_event_keys.clone(),
    );
    strategies::smart_money::CopyDryRunShadowPipeline::new(
        strategies::smart_money::CopyDryRunShadowConfig {
            local_account_id: account.account_id.clone(),
            target_accounts: target_accounts.to_vec(),
            signal_ttl_ms: config.process.signal_ttl_ms,
            max_signal_delay_ms: copy_daemon_max_signal_delay_ms(config),
            account_copy_ratio: account.copy_ratio,
            principal_cap_usd: account.max_order_notional_usd
                / strategies::smart_money::COPY_MAX_LEVERAGE.max(1.0),
            leverage: strategies::smart_money::COPY_MAX_LEVERAGE,
            max_signal_notional_usd: Some(account.max_order_notional_usd),
            exchange_min_open_notional_usd: trading::HYPERLIQUID_MIN_ORDER_NOTIONAL_USD,
            allow_short: true,
            max_effective_exposure_usd: Some(account.max_order_notional_usd),
            blocked_symbols: config.module_blocked_symbols("copy").to_vec(),
            live_gate: strategies::smart_money::CopyLiveGateInput {
                process_dry_run: config.app.dry_run,
                live_copy_enabled: options.live_gate && options.allow_live_submit,
                account_worker_live: !config.app.dry_run,
            },
        },
        strategy,
        persistence.ledger(),
    )
}

async fn run_copy_bounded_live_window(
    config: &config::AppConfig,
    options: CopyBoundedLiveWindowOptions,
) -> Result<CopyBoundedLiveWindowReport> {
    let target_accounts = copy_execution_canary_target_accounts(config, &options.account_ids, None);
    let acceptance_options = CopyLiveDaemonAcceptanceOptions {
        leaders: options.leaders.clone(),
        account_ids: target_accounts.clone(),
        coin: options.coin.clone(),
        side: options.side,
        persistence_path: options.persistence_path.clone(),
        shadow_history_path: options.shadow_history_path.clone(),
        leader_notional_usd: options.leader_notional_usd,
        leader_size: options.leader_size,
        live: true,
        allow_live_submit: options.allow_live_submit,
        confirm_mainnet_live: options.confirm_mainnet_live,
        max_duration_secs: options.max_duration_secs,
        max_live_orders: options.max_live_orders,
        max_total_notional_usd: options.max_total_notional_usd,
        max_total_fees_usd: options.max_total_fees_usd,
        max_slippage_bps: options.max_slippage_bps,
        require_cleanup_after_submit: true,
        require_flat_reconcile_after_submit: true,
    };
    let acceptance = run_copy_live_daemon_acceptance(config, acceptance_options)?;

    let preflight_options = CopyExecutionCanaryOptions {
        leaders: options.leaders.clone(),
        account_ids: target_accounts.clone(),
        coin: options.coin.clone(),
        side: options.side,
        local_account_id: None,
        shadow_history_path: options.shadow_history_path.clone(),
        leader_notional_usd: options.leader_notional_usd,
        leader_size: options.leader_size,
        live: true,
        allow_live_submit: options.allow_live_submit,
        confirm_mainnet_live: options.confirm_mainnet_live,
        cleanup_after_submit: true,
        cleanup_max_slippage_bps: options.cleanup_max_slippage_bps,
        preflight_only: true,
        max_orders: options.max_live_orders,
    };
    let preflight = run_copy_execution_canary(config, preflight_options).await?;

    let mut checks = Vec::new();
    checks.push(copy_shadow_smoke_check(
        "acceptance_gate",
        acceptance.ok,
        format!("copy-live-daemon-acceptance ok={}", acceptance.ok),
    ));
    checks.push(copy_shadow_smoke_check(
        "preflight_only_canary",
        preflight.ok && preflight.submitted_reports.is_empty(),
        format!(
            "preflight ok={} would_submit_orders={} submitted_reports={}",
            preflight.ok,
            preflight.would_submit_orders.len(),
            preflight.submitted_reports.len()
        ),
    ));
    checks.push(copy_shadow_smoke_check(
        "bounded_submit_flag",
        !options.submit || (acceptance.ok && preflight.ok),
        "--submit true is allowed only after acceptance and preflight checks pass",
    ));

    let execution = if options.submit && checks.iter().all(|check| check.ok) {
        let execution_options = CopyExecutionCanaryOptions {
            leaders: options.leaders.clone(),
            account_ids: target_accounts.clone(),
            coin: options.coin.clone(),
            side: options.side,
            local_account_id: None,
            shadow_history_path: options.shadow_history_path.clone(),
            leader_notional_usd: options.leader_notional_usd,
            leader_size: options.leader_size,
            live: true,
            allow_live_submit: options.allow_live_submit,
            confirm_mainnet_live: options.confirm_mainnet_live,
            cleanup_after_submit: true,
            cleanup_max_slippage_bps: options.cleanup_max_slippage_bps,
            preflight_only: false,
            max_orders: options.max_live_orders,
        };
        Some(run_copy_execution_canary(config, execution_options).await?)
    } else {
        None
    };

    if let Some(execution) = &execution {
        checks.push(copy_shadow_smoke_check(
            "execution_canary",
            execution.ok && !execution.submitted_reports.is_empty(),
            format!(
                "execution ok={} submitted_reports={} cleanup_errors={}",
                execution.ok,
                execution.submitted_reports.len(),
                execution.cleanup_errors.len()
            ),
        ));
    } else {
        checks.push(copy_shadow_smoke_check(
            "execution_canary",
            !options.submit,
            if options.submit {
                "submit requested but execution was skipped because earlier gates failed"
                    .to_string()
            } else {
                "no-submit bounded window stopped before live execution".to_string()
            },
        ));
    }

    let final_reconciliations =
        reconcile_copy_bounded_window_accounts(config, &target_accounts).await;
    checks.push(copy_shadow_smoke_check(
        "final_reconcile_flat",
        final_reconciliations.iter().all(|reconcile| reconcile.ok),
        format!(
            "{}/{} account(s) flat with no open orders",
            final_reconciliations
                .iter()
                .filter(|reconcile| reconcile.ok)
                .count(),
            final_reconciliations.len()
        ),
    ));

    let ok = copy_bounded_live_window_ok(
        options.submit,
        &checks,
        &acceptance,
        &preflight,
        execution.as_ref(),
        &final_reconciliations,
    );
    let next_actions = if ok && options.submit {
        vec![
            "Bounded canary-live window completed with cleanup and final flat reconciliation; archive the report before any wider live window.".to_string(),
        ]
    } else if ok {
        vec![
            "No-submit bounded live window passed; rerun with --submit true only for one-account/one-order canary-live execution.".to_string(),
        ]
    } else {
        vec![
            "Do not submit or widen live copy; inspect failed checks and reconcile every target account before retrying.".to_string(),
        ]
    };

    Ok(copy_bounded_live_window_report(
        config,
        options,
        target_accounts,
        checks,
        acceptance,
        preflight,
        execution,
        final_reconciliations,
        next_actions,
        ok,
    ))
}

fn copy_bounded_live_window_ok(
    submit_requested: bool,
    checks: &[CopyShadowSmokeCheck],
    acceptance: &CopyLiveDaemonAcceptanceReport,
    preflight: &CopyExecutionCanaryReport,
    execution: Option<&CopyExecutionCanaryReport>,
    final_reconciliations: &[CopyBoundedLiveWindowReconcile],
) -> bool {
    let execution_ok = if submit_requested {
        execution.is_some_and(|report| report.ok)
    } else {
        execution.is_none()
    };
    checks.iter().all(|check| check.ok)
        && acceptance.ok
        && preflight.ok
        && execution_ok
        && final_reconciliations.iter().all(|reconcile| reconcile.ok)
}

#[allow(clippy::too_many_arguments)]
fn copy_bounded_live_window_report(
    config: &config::AppConfig,
    options: CopyBoundedLiveWindowOptions,
    target_accounts: Vec<String>,
    checks: Vec<CopyShadowSmokeCheck>,
    acceptance: CopyLiveDaemonAcceptanceReport,
    preflight: CopyExecutionCanaryReport,
    execution: Option<CopyExecutionCanaryReport>,
    final_reconciliations: Vec<CopyBoundedLiveWindowReconcile>,
    next_actions: Vec<String>,
    ok: bool,
) -> CopyBoundedLiveWindowReport {
    CopyBoundedLiveWindowReport {
        ok,
        mode: if options.submit {
            "copy_bounded_live_window_submit".to_string()
        } else {
            "copy_bounded_live_window_no_submit".to_string()
        },
        environment: config.app.environment.clone(),
        submit_requested: options.submit,
        live_submit_allowed: options.allow_live_submit,
        confirm_mainnet_live: options.confirm_mainnet_live,
        max_duration_secs: options.max_duration_secs,
        max_live_orders: options.max_live_orders,
        max_total_notional_usd: options.max_total_notional_usd,
        max_total_fees_usd: options.max_total_fees_usd,
        max_slippage_bps: options.max_slippage_bps,
        cleanup_max_slippage_bps: options.cleanup_max_slippage_bps,
        target_accounts,
        checks,
        acceptance,
        preflight,
        execution,
        final_reconciliations,
        next_actions,
    }
}

async fn run_copy_live_stability_soak(
    config: &config::AppConfig,
    options: CopyLiveStabilitySoakOptions,
) -> Result<CopyLiveStabilitySoakReport> {
    anyhow::ensure!(
        options.duration_secs > 0,
        "duration_secs must be positive for copy live stability soak"
    );
    anyhow::ensure!(
        options.max_rounds > 0,
        "max_rounds must be positive for copy live stability soak"
    );
    anyhow::ensure!(
        options.max_live_orders == 1,
        "copy live stability soak is restricted to --max-live-orders 1"
    );
    anyhow::ensure!(
        options.max_total_notional_usd.is_finite() && options.max_total_notional_usd > 0.0,
        "max_total_notional_usd must be positive"
    );
    anyhow::ensure!(
        options.max_total_fees_usd.is_finite() && options.max_total_fees_usd >= 0.0,
        "max_total_fees_usd must be non-negative"
    );

    let target_accounts = copy_execution_canary_target_accounts(config, &options.account_ids, None);
    let mut checks = vec![
        copy_shadow_smoke_check(
            "bounded_accounts",
            target_accounts.len() == 1,
            "live stability soak is restricted to exactly one account",
        ),
        copy_shadow_smoke_check(
            "bounded_rounds",
            (1..=24).contains(&options.max_rounds),
            format!("max_rounds={} must be between 1 and 24", options.max_rounds),
        ),
        copy_shadow_smoke_check(
            "bounded_duration",
            (1..=86_400).contains(&options.duration_secs),
            format!(
                "duration_secs={} must be between 1 and 86400",
                options.duration_secs
            ),
        ),
        copy_shadow_smoke_check(
            "bounded_live_orders",
            options.max_live_orders == 1,
            "stability soak permits only one live order per bounded round",
        ),
        copy_shadow_smoke_check(
            "bounded_total_notional",
            options.max_total_notional_usd <= 250.0,
            format!(
                "max_total_notional_usd={} must be <= 250 for this guarded soak",
                options.max_total_notional_usd
            ),
        ),
        copy_shadow_smoke_check(
            "bounded_total_fees",
            options.max_total_fees_usd <= 1.0,
            format!(
                "max_total_fees_usd={} must be <= 1.0 for this guarded soak",
                options.max_total_fees_usd
            ),
        ),
        copy_shadow_smoke_check(
            "allow_live_submit_flag",
            !options.submit || options.allow_live_submit,
            "submit mode requires --allow-live-submit true",
        ),
        copy_shadow_smoke_check(
            "mainnet_confirmation",
            !options.submit || config.app.environment != "mainnet" || options.confirm_mainnet_live,
            "mainnet submit mode requires --confirm-mainnet-live true",
        ),
    ];

    let started = Instant::now();
    let mut rounds = Vec::new();
    let mut total_submitted_orders = 0usize;
    let mut total_submitted_notional_usd = 0.0f64;
    let mut estimated_fees_usd = 0.0f64;
    let mut stop_reason = "completed_max_rounds".to_string();
    let expected_round_notional_usd =
        copy_live_stability_expected_round_notional(config, &options, &target_accounts)?;
    let expected_round_fees_usd =
        copy_live_stability_estimated_fees_usd(expected_round_notional_usd);
    checks.push(copy_shadow_smoke_check(
        "expected_round_notional",
        !options.submit || expected_round_notional_usd > 0.0,
        format!(
            "expected_round_notional_usd={expected_round_notional_usd:.6} for each bounded round"
        ),
    ));
    checks.push(copy_shadow_smoke_check(
        "expected_round_fee_budget",
        !options.submit || expected_round_fees_usd <= options.max_total_fees_usd,
        format!(
            "expected_round_fees_usd={expected_round_fees_usd:.6} must be <= max_total_fees_usd={:.6}",
            options.max_total_fees_usd
        ),
    ));

    if checks.iter().all(|check| check.ok) {
        while rounds.len() < options.max_rounds {
            if started.elapsed() >= Duration::from_secs(options.duration_secs) {
                stop_reason = "completed_duration".to_string();
                break;
            }
            let remaining_notional = options.max_total_notional_usd - total_submitted_notional_usd;
            let remaining_fees = options.max_total_fees_usd - estimated_fees_usd;
            if remaining_notional <= 0.0 {
                stop_reason = "stopped_total_notional_limit".to_string();
                break;
            }
            if remaining_fees < 0.0 {
                stop_reason = "stopped_total_fee_limit".to_string();
                break;
            }
            if options.submit && expected_round_notional_usd > remaining_notional + 1e-9 {
                stop_reason = "stopped_total_notional_limit_before_round".to_string();
                break;
            }
            if options.submit && expected_round_fees_usd > remaining_fees + 1e-9 {
                stop_reason = "stopped_total_fee_limit_before_round".to_string();
                break;
            }

            let round_number = rounds.len() + 1;
            let round_report = run_copy_bounded_live_window(
                config,
                CopyBoundedLiveWindowOptions {
                    leaders: options.leaders.clone(),
                    account_ids: target_accounts.clone(),
                    coin: options.coin.clone(),
                    side: options.side,
                    persistence_path: copy_stability_round_path(
                        &options.persistence_path,
                        round_number,
                    ),
                    shadow_history_path: copy_stability_round_path(
                        &options.shadow_history_path,
                        round_number,
                    ),
                    leader_notional_usd: options.leader_notional_usd,
                    leader_size: options.leader_size,
                    submit: options.submit,
                    allow_live_submit: options.allow_live_submit,
                    confirm_mainnet_live: options.confirm_mainnet_live,
                    max_duration_secs: options.duration_secs,
                    max_live_orders: options.max_live_orders,
                    max_total_notional_usd: remaining_notional,
                    max_total_fees_usd: remaining_fees.max(0.0),
                    max_slippage_bps: options.max_slippage_bps,
                    cleanup_max_slippage_bps: options.cleanup_max_slippage_bps,
                },
            )
            .await?;
            let (round_orders, round_notional) =
                copy_live_stability_round_submission_totals(&round_report);
            total_submitted_orders += round_orders;
            total_submitted_notional_usd += round_notional;
            estimated_fees_usd = normalize_report_zero(copy_live_stability_estimated_fees_usd(
                total_submitted_notional_usd,
            ));
            let round_ok = round_report.ok;
            rounds.push(round_report);
            if !round_ok {
                stop_reason = format!("stopped_round_{}_failed", round_number);
                break;
            }
            if total_submitted_notional_usd > options.max_total_notional_usd {
                stop_reason = "stopped_total_notional_limit".to_string();
                break;
            }
            if estimated_fees_usd > options.max_total_fees_usd {
                stop_reason = "stopped_total_fee_limit".to_string();
                break;
            }
            if rounds.len() >= options.max_rounds {
                break;
            }
            let Some(remaining_duration) =
                Duration::from_secs(options.duration_secs).checked_sub(started.elapsed())
            else {
                stop_reason = "completed_duration".to_string();
                break;
            };
            let sleep_duration = Duration::from_secs(options.interval_secs).min(remaining_duration);
            if sleep_duration.is_zero() {
                stop_reason = "completed_duration".to_string();
                break;
            }
            tokio::time::sleep(sleep_duration).await;
        }
    } else {
        stop_reason = "skipped_initial_gate_failed".to_string();
    }

    let elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    let final_reconciliations =
        reconcile_copy_bounded_window_accounts(config, &target_accounts).await;
    let rounds_passed = rounds.iter().filter(|round| round.ok).count();
    checks.push(copy_shadow_smoke_check(
        "rounds_attempted",
        !rounds.is_empty(),
        format!("{} bounded round(s) attempted", rounds.len()),
    ));
    checks.push(copy_shadow_smoke_check(
        "all_rounds_passed",
        !rounds.is_empty() && rounds_passed == rounds.len(),
        format!("{rounds_passed}/{} bounded round(s) passed", rounds.len()),
    ));
    checks.push(copy_shadow_smoke_check(
        "total_live_order_count",
        if options.submit {
            total_submitted_orders == rounds.len()
        } else {
            total_submitted_orders == 0
        },
        format!(
            "total_submitted_orders={} rounds={}",
            total_submitted_orders,
            rounds.len()
        ),
    ));
    checks.push(copy_shadow_smoke_check(
        "total_notional_limit",
        total_submitted_notional_usd <= options.max_total_notional_usd,
        format!(
            "total_submitted_notional_usd={total_submitted_notional_usd:.6} must be <= {:.6}",
            options.max_total_notional_usd
        ),
    ));
    checks.push(copy_shadow_smoke_check(
        "total_fee_limit",
        estimated_fees_usd <= options.max_total_fees_usd,
        format!(
            "estimated_fees_usd={estimated_fees_usd:.6} must be <= {:.6}",
            options.max_total_fees_usd
        ),
    ));
    checks.push(copy_shadow_smoke_check(
        "final_reconcile_flat",
        final_reconciliations.iter().all(|reconcile| reconcile.ok),
        format!(
            "{}/{} account(s) flat with no open orders",
            final_reconciliations
                .iter()
                .filter(|reconcile| reconcile.ok)
                .count(),
            final_reconciliations.len()
        ),
    ));

    let ok = copy_live_stability_soak_ok(
        options.submit,
        &checks,
        &rounds,
        total_submitted_orders,
        total_submitted_notional_usd,
        estimated_fees_usd,
        &options,
        &final_reconciliations,
    );
    let next_actions = if ok && options.submit {
        vec![
            "Bounded live stability soak passed; review every round report before increasing duration, account count, or total notional.".to_string(),
        ]
    } else if ok {
        vec![
            "No-submit live stability soak passed; rerun with --submit true only when ready for a bounded live stability run.".to_string(),
        ]
    } else {
        vec![
            "Do not widen live copy; inspect failed soak checks, reconcile the target account, and rerun with the same or lower caps.".to_string(),
        ]
    };

    Ok(CopyLiveStabilitySoakReport {
        ok,
        mode: if options.submit {
            "copy_live_stability_soak_submit".to_string()
        } else {
            "copy_live_stability_soak_no_submit".to_string()
        },
        environment: config.app.environment.clone(),
        submit_requested: options.submit,
        live_submit_allowed: options.allow_live_submit,
        confirm_mainnet_live: options.confirm_mainnet_live,
        duration_secs: options.duration_secs,
        interval_secs: options.interval_secs,
        elapsed_ms,
        max_rounds: options.max_rounds,
        rounds_attempted: rounds.len(),
        rounds_passed,
        max_live_orders: options.max_live_orders,
        max_total_notional_usd: options.max_total_notional_usd,
        max_total_fees_usd: options.max_total_fees_usd,
        max_slippage_bps: options.max_slippage_bps,
        cleanup_max_slippage_bps: options.cleanup_max_slippage_bps,
        target_accounts,
        checks,
        rounds,
        total_submitted_orders,
        total_submitted_notional_usd: normalize_report_zero(total_submitted_notional_usd),
        estimated_fees_usd: normalize_report_zero(estimated_fees_usd),
        final_reconciliations,
        stop_reason,
        next_actions,
    })
}

fn copy_live_stability_round_submission_totals(
    report: &CopyBoundedLiveWindowReport,
) -> (usize, f64) {
    let Some(execution) = &report.execution else {
        return (0, 0.0);
    };
    execution
        .submitted_reports
        .iter()
        .filter_map(|report| match report {
            domain::WorkerReport::Submitted(submitted) if !submitted.dry_run => {
                Some(submitted.notional_usd.max(0.0))
            }
            _ => None,
        })
        .fold((0usize, 0.0f64), |(count, total), notional| {
            (count + 1, total + notional)
        })
}

fn copy_live_stability_expected_round_notional(
    config: &config::AppConfig,
    options: &CopyLiveStabilitySoakOptions,
    target_accounts: &[String],
) -> Result<f64> {
    let acceptance_options = CopyLiveDaemonAcceptanceOptions {
        leaders: options.leaders.clone(),
        account_ids: target_accounts.to_vec(),
        coin: options.coin.clone(),
        side: options.side,
        persistence_path: options.persistence_path.clone(),
        shadow_history_path: options.shadow_history_path.clone(),
        leader_notional_usd: options.leader_notional_usd,
        leader_size: options.leader_size,
        live: true,
        allow_live_submit: options.allow_live_submit,
        confirm_mainnet_live: options.confirm_mainnet_live,
        max_duration_secs: options.duration_secs,
        max_live_orders: options.max_live_orders,
        max_total_notional_usd: options.max_total_notional_usd,
        max_total_fees_usd: options.max_total_fees_usd,
        max_slippage_bps: options.max_slippage_bps,
        require_cleanup_after_submit: true,
        require_flat_reconcile_after_submit: true,
    };
    let leaders = parse_copy_shadow_smoke_leaders(&options.leaders)?;
    let orders = copy_live_daemon_synthetic_would_submit_orders(
        config,
        &acceptance_options,
        &leaders,
        target_accounts,
        false,
    )?;
    Ok(normalize_report_zero(
        orders
            .iter()
            .map(|order| order.notional_usd.max(0.0))
            .sum::<f64>(),
    ))
}

fn copy_live_stability_estimated_fees_usd(open_notional_usd: f64) -> f64 {
    normalize_report_zero(open_notional_usd.max(0.0) * 0.002)
}

#[allow(clippy::too_many_arguments)]
fn copy_live_stability_soak_ok(
    submit_requested: bool,
    checks: &[CopyShadowSmokeCheck],
    rounds: &[CopyBoundedLiveWindowReport],
    total_submitted_orders: usize,
    total_submitted_notional_usd: f64,
    estimated_fees_usd: f64,
    options: &CopyLiveStabilitySoakOptions,
    final_reconciliations: &[CopyBoundedLiveWindowReconcile],
) -> bool {
    let expected_orders_ok = if submit_requested {
        total_submitted_orders == rounds.len()
    } else {
        total_submitted_orders == 0
    };
    !rounds.is_empty()
        && checks.iter().all(|check| check.ok)
        && rounds.iter().all(|round| round.ok)
        && expected_orders_ok
        && total_submitted_notional_usd <= options.max_total_notional_usd
        && estimated_fees_usd <= options.max_total_fees_usd
        && final_reconciliations.iter().all(|reconcile| reconcile.ok)
}

fn copy_stability_round_path(path: &std::path::Path, round_number: usize) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("copy-live-stability-soak");
    path.with_file_name(format!("{file_name}.round-{round_number:03}"))
}

async fn reconcile_copy_bounded_window_accounts(
    config: &config::AppConfig,
    target_accounts: &[String],
) -> Vec<CopyBoundedLiveWindowReconcile> {
    let mut reconciliations = Vec::new();
    for account_id in target_accounts {
        let reconciliation = match reconcile_copy_account_with_retries(config, account_id).await {
            Ok(report) => copy_bounded_live_window_reconcile_from_report(report),
            Err(error) => CopyBoundedLiveWindowReconcile {
                account_id: account_id.clone(),
                ok: false,
                open_order_count: None,
                asset_positions: None,
                position_summaries: Vec::new(),
                account_value: None,
                withdrawable: None,
                total_ntl_pos: None,
                total_margin_used: None,
                error: Some(error.to_string()),
            },
        };
        reconciliations.push(reconciliation);
    }
    reconciliations
}

async fn reconcile_copy_bounded_window_accounts_bounded(
    config: &config::AppConfig,
    target_accounts: &[String],
    label: &str,
) -> Vec<CopyBoundedLiveWindowReconcile> {
    match tokio::time::timeout(
        Duration::from_secs(COPY_DAEMON_RECONCILE_BATCH_TIMEOUT_SECS),
        reconcile_copy_bounded_window_accounts(config, target_accounts),
    )
    .await
    {
        Ok(reconciliations) => reconciliations,
        Err(_) => target_accounts
            .iter()
            .map(|account_id| CopyBoundedLiveWindowReconcile {
                account_id: account_id.clone(),
                ok: false,
                open_order_count: None,
                asset_positions: None,
                position_summaries: Vec::new(),
                account_value: None,
                withdrawable: None,
                total_ntl_pos: None,
                total_margin_used: None,
                error: Some(format!(
                    "{label} timed out after {}s",
                    COPY_DAEMON_RECONCILE_BATCH_TIMEOUT_SECS
                )),
            })
            .collect(),
    }
}

async fn reconcile_copy_account_with_retries(
    config: &config::AppConfig,
    account_id: &str,
) -> Result<trading::AccountReconciliationReport> {
    let mut last_error = None;
    for attempt in 0..=COPY_RECONCILE_RETRIES {
        match trading::reconcile_account(config, account_id).await {
            Ok(report) => return Ok(report),
            Err(error) => {
                last_error = Some(error);
                if attempt < COPY_RECONCILE_RETRIES {
                    tokio::time::sleep(Duration::from_millis(COPY_RECONCILE_RETRY_DELAY_MS)).await;
                }
            }
        }
    }
    Err(last_error
        .unwrap_or_else(|| anyhow::anyhow!("account reconciliation failed without an error")))
}

fn copy_bounded_live_window_reconcile_from_report(
    report: trading::AccountReconciliationReport,
) -> CopyBoundedLiveWindowReconcile {
    let margin = &report.clearinghouse_state.margin_summary;
    let asset_positions = report.clearinghouse_state.asset_positions.len();
    let position_summaries = report
        .clearinghouse_state
        .asset_positions
        .iter()
        .map(|asset| CopyBoundedLiveWindowPositionSummary {
            coin: asset.position.coin.clone(),
            szi: asset.position.szi.clone(),
            position_value: asset.position.position_value.clone(),
            unrealized_pnl: asset.position.unrealized_pnl.clone(),
        })
        .collect::<Vec<_>>();
    let total_ntl_zero = margin
        .total_ntl_pos
        .parse::<f64>()
        .is_ok_and(|value| value.abs() <= 1e-9);
    let total_margin_zero = margin
        .total_margin_used
        .parse::<f64>()
        .is_ok_and(|value| value.abs() <= 1e-9);
    CopyBoundedLiveWindowReconcile {
        account_id: report.account_id,
        ok: report.open_order_count == 0
            && asset_positions == 0
            && total_ntl_zero
            && total_margin_zero,
        open_order_count: Some(report.open_order_count),
        asset_positions: Some(asset_positions),
        position_summaries,
        account_value: Some(margin.account_value.clone()),
        withdrawable: report.clearinghouse_state.withdrawable.clone(),
        total_ntl_pos: Some(margin.total_ntl_pos.clone()),
        total_margin_used: Some(margin.total_margin_used.clone()),
        error: None,
    }
}

fn copy_live_daemon_synthetic_would_submit_orders(
    config: &config::AppConfig,
    options: &CopyLiveDaemonAcceptanceOptions,
    leaders: &[CopyShadowSmokeLeader],
    target_accounts: &[String],
    append_shadow_history: bool,
) -> Result<Vec<CopyExecutionCanaryWouldSubmit>> {
    let Some(leader) = leaders.first() else {
        return Ok(Vec::new());
    };
    let canary_options = CopyExecutionCanaryOptions {
        leaders: options.leaders.clone(),
        account_ids: target_accounts.to_vec(),
        coin: options.coin.clone(),
        side: options.side,
        local_account_id: None,
        shadow_history_path: options.shadow_history_path.clone(),
        leader_notional_usd: options.leader_notional_usd,
        leader_size: options.leader_size,
        live: false,
        allow_live_submit: false,
        confirm_mainnet_live: false,
        cleanup_after_submit: false,
        cleanup_max_slippage_bps: options.max_slippage_bps,
        preflight_only: false,
        max_orders: options.max_live_orders.max(1),
    };
    let mut records = Vec::new();
    for account_id in target_accounts {
        let Some(account) = config.account(account_id) else {
            continue;
        };
        records.extend(build_synthetic_copy_shadow_records(
            config,
            &canary_options,
            account,
            leader,
            std::slice::from_ref(&account.account_id),
        ));
    }
    if append_shadow_history && !records.is_empty() {
        strategies::smart_money::append_copy_shadow_history_records(
            &options.shadow_history_path,
            &records,
            domain::now_ms(),
        )?;
    }
    plan_copy_daemon_acceptance_orders(config, &records)
}

fn plan_copy_daemon_acceptance_orders(
    config: &config::AppConfig,
    records: &[strategies::smart_money::CopyDryRunShadowRecord],
) -> Result<Vec<CopyExecutionCanaryWouldSubmit>> {
    Ok(copy_live_daemon_order_refs_to_orders(
        &plan_copy_daemon_acceptance_order_refs(config, records)?,
    ))
}

fn plan_copy_daemon_acceptance_order_refs(
    config: &config::AppConfig,
    records: &[strategies::smart_money::CopyDryRunShadowRecord],
) -> Result<Vec<CopyLiveDaemonWouldSubmitRef>> {
    plan_copy_daemon_acceptance_order_refs_with_offset(config, records, 0)
}

fn plan_copy_daemon_acceptance_order_refs_with_offset(
    config: &config::AppConfig,
    records: &[strategies::smart_money::CopyDryRunShadowRecord],
    base_record_index: usize,
) -> Result<Vec<CopyLiveDaemonWouldSubmitRef>> {
    let mut plans = Vec::new();
    for (record_offset, record) in records.iter().enumerate() {
        let (
            strategies::smart_money::CopySignalRiskDecision::Approved {
                side,
                reduce_only,
                notional_usd,
            },
            Some(signal),
        ) = (&record.risk_decision, record.signal.as_ref())
        else {
            continue;
        };
        for account_id in &signal.target_accounts {
            let account = config
                .account(account_id)
                .with_context(|| format!("account {account_id} not found"))?;
            let worker_id = format!("worker-{}", account.account_id);
            let mut intent = signal.to_trade_intent(&account.account_id, &worker_id, 1.0);
            intent.sizing.notional_usd = *notional_usd;
            intent.reduce_only = *reduce_only;
            intent.side = *side;
            let risk_context =
                risk::RiskContext::from_account_for_module(config, account, true, "copy");
            if let risk::RiskDecision::Approved(order) =
                risk::RiskGateway::dry_run_default().evaluate(&risk_context, intent)
            {
                plans.push(CopyLiveDaemonWouldSubmitRef {
                    record_index: base_record_index + record_offset,
                    signal_id: signal.signal_id.clone(),
                    leader_id: record.action.leader_id.clone(),
                    leader_address: record.action.leader_address.clone(),
                    order: CopyExecutionCanaryWouldSubmit {
                        account_id: order.account_id,
                        worker_id: order.worker_id,
                        coin: order.coin,
                        side: order.side,
                        notional_usd: order.notional_usd,
                        reduce_only: order.reduce_only,
                        cloid: order.cloid,
                    },
                });
            }
        }
    }
    Ok(plans)
}

fn copy_live_daemon_recover_pending_reduce_plan_refs(
    persistence: &strategies::smart_money::CopyPersistenceSnapshot,
    target_accounts: &[String],
) -> Vec<CopyLiveDaemonWouldSubmitRef> {
    let target_accounts = target_accounts.iter().collect::<HashSet<_>>();
    persistence
        .ledger_entries
        .iter()
        .enumerate()
        .filter(|(_, entry)| {
            entry.status == strategies::smart_money::CopyLedgerStatus::PendingReduce
                && target_accounts.contains(&entry.local_account_id)
                && entry.remaining_notional_usd > 1e-9
        })
        .map(|(offset, entry)| {
            let cloid_seed = format!(
                "copy-pending-reduce-retry:{}:{}:{}:{}",
                entry.local_account_id, entry.coin, entry.signal_id, entry.remaining_notional_usd
            );
            CopyLiveDaemonWouldSubmitRef {
                record_index: offset,
                signal_id: entry.signal_id.clone(),
                leader_id: entry.leader_id.clone(),
                leader_address: String::new(),
                order: CopyExecutionCanaryWouldSubmit {
                    account_id: entry.local_account_id.clone(),
                    worker_id: format!("worker-{}", entry.local_account_id),
                    coin: entry.coin.clone(),
                    side: opposite_order_side(entry.local_side),
                    notional_usd: entry.remaining_notional_usd,
                    reduce_only: true,
                    cloid: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, cloid_seed.as_bytes())
                        .to_string(),
                },
            }
        })
        .collect()
}

fn copy_live_daemon_order_refs_to_orders(
    refs: &[CopyLiveDaemonWouldSubmitRef],
) -> Vec<CopyExecutionCanaryWouldSubmit> {
    refs.iter().map(|plan| plan.order.clone()).collect()
}

#[cfg(test)]
fn append_unique_copy_daemon_would_submit_orders(
    existing: &mut Vec<CopyExecutionCanaryWouldSubmit>,
    new_orders: Vec<CopyExecutionCanaryWouldSubmit>,
) {
    for order in new_orders {
        if !existing
            .iter()
            .any(|existing| existing.cloid == order.cloid)
        {
            existing.push(order);
        }
    }
}

fn append_unique_copy_daemon_would_submit_refs(
    existing: &mut Vec<CopyLiveDaemonWouldSubmitRef>,
    new_refs: Vec<CopyLiveDaemonWouldSubmitRef>,
) {
    for new_ref in new_refs {
        if !existing
            .iter()
            .any(|existing| existing.order.cloid == new_ref.order.cloid)
        {
            existing.push(new_ref);
        }
    }
}

fn copy_live_daemon_pending_plan_refs(
    refs: &[CopyLiveDaemonWouldSubmitRef],
    submitted_plan_cloids: &HashSet<String>,
) -> Vec<CopyLiveDaemonWouldSubmitRef> {
    refs.iter()
        .filter(|plan| !submitted_plan_cloids.contains(&plan.order.cloid))
        .cloned()
        .collect()
}

fn copy_live_daemon_pending_suppressed_refs(
    refs: &[CopyLiveDaemonSuppressedWouldSubmitRef],
    submitted_plan_cloids: &HashSet<String>,
) -> Vec<CopyLiveDaemonSuppressedWouldSubmitRef> {
    refs.iter()
        .filter(|suppressed| !submitted_plan_cloids.contains(&suppressed.plan.order.cloid))
        .cloned()
        .collect()
}

fn copy_live_daemon_accounts_for_refs(refs: &[CopyLiveDaemonWouldSubmitRef]) -> Vec<String> {
    let mut accounts = refs
        .iter()
        .filter_map(|plan| {
            let account_id = plan.order.account_id.trim();
            (!account_id.is_empty()).then(|| account_id.to_string())
        })
        .collect::<Vec<_>>();
    accounts.sort();
    accounts.dedup();
    accounts
}

fn copy_live_daemon_defer_open_refs_after_immediate_live_submit(
    refs: Vec<CopyLiveDaemonWouldSubmitRef>,
    live_submit_chunks: &[CopyLiveDaemonPersistentLiveSubmitReport],
) -> (
    Vec<CopyLiveDaemonWouldSubmitRef>,
    Vec<CopyLiveDaemonSuppressedWouldSubmitRef>,
) {
    if !live_submit_chunks
        .iter()
        .any(copy_live_daemon_immediate_submit_should_stop_round)
    {
        return (refs, Vec::new());
    }

    let mut pending = Vec::new();
    let mut suppressed = Vec::new();
    for plan in refs {
        if plan.order.reduce_only {
            pending.push(plan);
        } else {
            suppressed.push(CopyLiveDaemonSuppressedWouldSubmitRef {
                plan,
                reason_code: "COPY_DAEMON_DEFER_OPEN_AFTER_LIVE_SUBMIT".to_string(),
                message: "open/increase ref deferred after an immediate live submit; next soak round must reload exchange state and ledger evidence before opening more exposure".to_string(),
            });
        }
    }
    (pending, suppressed)
}

fn copy_live_daemon_submitted_report_cloids(
    report: &CopyLiveDaemonPersistentLiveSubmitReport,
) -> HashSet<String> {
    report
        .submitted_reports
        .iter()
        .filter_map(|report| match report {
            domain::WorkerReport::Submitted(submitted) => {
                let cloid = submitted.cloid.trim();
                (!cloid.is_empty()).then(|| cloid.to_string())
            }
            _ => None,
        })
        .collect()
}

#[cfg(test)]
fn partition_copy_live_daemon_would_submit_orders(
    orders: &[CopyExecutionCanaryWouldSubmit],
    options: &CopyLiveDaemonSupervisorOptions,
) -> (
    Vec<CopyExecutionCanaryWouldSubmit>,
    Vec<CopyLiveDaemonSuppressedWouldSubmit>,
) {
    let mut executable = Vec::new();
    let mut suppressed = Vec::new();
    let mut planned_notional_usd = 0.0;
    let mut planned_open_order_count = 0usize;
    for order in orders {
        let is_open_candidate = !order.reduce_only;
        let next_open_order_count = planned_open_order_count + usize::from(is_open_candidate);
        let next_notional_usd = if is_open_candidate {
            planned_notional_usd + order.notional_usd.max(0.0)
        } else {
            planned_notional_usd
        };
        let next_fee_estimate_usd = normalize_report_zero(next_notional_usd * 0.001);
        let suppression = if is_open_candidate && next_open_order_count > options.max_live_orders {
            Some((
                "COPY_DAEMON_MAX_LIVE_ORDERS".to_string(),
                format!(
                    "open candidate would exceed max_live_orders {}; kept as observation only",
                    options.max_live_orders
                ),
            ))
        } else if next_notional_usd > options.max_total_notional_usd {
            Some((
                "COPY_DAEMON_MAX_TOTAL_NOTIONAL".to_string(),
                format!(
                    "candidate would raise executable notional to {next_notional_usd:.6}, above cap {:.6}",
                    options.max_total_notional_usd
                ),
            ))
        } else if next_fee_estimate_usd > options.max_total_fees_usd {
            Some((
                "COPY_DAEMON_MAX_TOTAL_FEES".to_string(),
                format!(
                    "candidate would raise estimated fees to {next_fee_estimate_usd:.6}, above cap {:.6}",
                    options.max_total_fees_usd
                ),
            ))
        } else {
            None
        };

        if let Some((reason_code, message)) = suppression {
            suppressed.push(CopyLiveDaemonSuppressedWouldSubmit {
                order: order.clone(),
                reason_code,
                message,
            });
        } else {
            planned_notional_usd = next_notional_usd;
            if is_open_candidate {
                planned_open_order_count = next_open_order_count;
            }
            executable.push(order.clone());
        }
    }
    (executable, suppressed)
}

fn partition_copy_live_daemon_would_submit_refs(
    refs: &[CopyLiveDaemonWouldSubmitRef],
    options: &CopyLiveDaemonSupervisorOptions,
) -> (
    Vec<CopyLiveDaemonWouldSubmitRef>,
    Vec<CopyLiveDaemonSuppressedWouldSubmitRef>,
) {
    let mut executable = Vec::new();
    let mut suppressed = Vec::new();
    let mut planned_notional_usd = 0.0;
    let mut planned_open_order_count = 0usize;
    let mut processed_open_groups = HashSet::new();
    for plan in refs {
        let is_open_candidate = !plan.order.reduce_only;
        if is_open_candidate {
            let group_key = copy_live_daemon_open_fanout_group_key(plan);
            if !processed_open_groups.insert(group_key.clone()) {
                continue;
            }
            let group = refs
                .iter()
                .filter(|candidate| {
                    !candidate.order.reduce_only
                        && copy_live_daemon_open_fanout_group_key(candidate) == group_key
                })
                .collect::<Vec<_>>();
            let group_notional_usd = group
                .iter()
                .map(|candidate| candidate.order.notional_usd.max(0.0))
                .sum::<f64>();
            let next_open_order_count = planned_open_order_count + group.len();
            let next_notional_usd = planned_notional_usd + group_notional_usd;
            let next_fee_estimate_usd = normalize_report_zero(next_notional_usd * 0.001);
            let suppression = if !copy_daemon_market_scope_allows_open(options, &plan.order.coin) {
                let (market, _) = copy_daemon_market_dex_for_coin(&plan.order.coin);
                Some((
                    "COPY_DAEMON_MARKET_EXIT_ONLY".to_string(),
                    format!(
                        "{} is not selected for new copy entries; reduce-only exits remain enabled",
                        market.unwrap_or_else(|| "unknown_market".to_string())
                    ),
                ))
            } else if next_open_order_count > options.max_live_orders {
                Some((
                    "COPY_DAEMON_MAX_LIVE_ORDERS".to_string(),
                    format!(
                        "open fan-out group would exceed max_live_orders {}; kept as observation only",
                        options.max_live_orders
                    ),
                ))
            } else if next_notional_usd > options.max_total_notional_usd {
                Some((
                    "COPY_DAEMON_MAX_TOTAL_NOTIONAL".to_string(),
                    format!(
                        "fan-out group would raise executable notional to {next_notional_usd:.6}, above cap {:.6}",
                        options.max_total_notional_usd
                    ),
                ))
            } else if next_fee_estimate_usd > options.max_total_fees_usd {
                Some((
                    "COPY_DAEMON_MAX_TOTAL_FEES".to_string(),
                    format!(
                        "fan-out group would raise estimated fees to {next_fee_estimate_usd:.6}, above cap {:.6}",
                        options.max_total_fees_usd
                    ),
                ))
            } else {
                None
            };

            if let Some((reason_code, message)) = suppression {
                for grouped_plan in group {
                    suppressed.push(CopyLiveDaemonSuppressedWouldSubmitRef {
                        plan: grouped_plan.clone(),
                        reason_code: reason_code.clone(),
                        message: message.clone(),
                    });
                }
            } else {
                planned_notional_usd = next_notional_usd;
                planned_open_order_count = next_open_order_count;
                executable.extend(group.into_iter().cloned());
            }
            continue;
        }

        let next_open_order_count = planned_open_order_count;
        let next_notional_usd = if is_open_candidate {
            planned_notional_usd + plan.order.notional_usd.max(0.0)
        } else {
            planned_notional_usd
        };
        let next_fee_estimate_usd = normalize_report_zero(next_notional_usd * 0.001);
        let suppression = if is_open_candidate
            && !copy_daemon_market_scope_allows_open(options, &plan.order.coin)
        {
            let (market, _) = copy_daemon_market_dex_for_coin(&plan.order.coin);
            Some((
                "COPY_DAEMON_MARKET_EXIT_ONLY".to_string(),
                format!(
                    "{} is not selected for new copy entries; reduce-only exits remain enabled",
                    market.unwrap_or_else(|| "unknown_market".to_string())
                ),
            ))
        } else if is_open_candidate && next_open_order_count > options.max_live_orders {
            Some((
                "COPY_DAEMON_MAX_LIVE_ORDERS".to_string(),
                format!(
                    "open candidate would exceed max_live_orders {}; kept as observation only",
                    options.max_live_orders
                ),
            ))
        } else if next_notional_usd > options.max_total_notional_usd {
            Some((
                "COPY_DAEMON_MAX_TOTAL_NOTIONAL".to_string(),
                format!(
                    "candidate would raise executable notional to {next_notional_usd:.6}, above cap {:.6}",
                    options.max_total_notional_usd
                ),
            ))
        } else if next_fee_estimate_usd > options.max_total_fees_usd {
            Some((
                "COPY_DAEMON_MAX_TOTAL_FEES".to_string(),
                format!(
                    "candidate would raise estimated fees to {next_fee_estimate_usd:.6}, above cap {:.6}",
                    options.max_total_fees_usd
                ),
            ))
        } else {
            None
        };

        if let Some((reason_code, message)) = suppression {
            suppressed.push(CopyLiveDaemonSuppressedWouldSubmitRef {
                plan: plan.clone(),
                reason_code,
                message,
            });
        } else {
            planned_notional_usd = next_notional_usd;
            if is_open_candidate {
                planned_open_order_count = next_open_order_count;
            }
            executable.push(plan.clone());
        }
    }
    (executable, suppressed)
}

fn copy_live_daemon_open_fanout_group_key(plan: &CopyLiveDaemonWouldSubmitRef) -> String {
    format!(
        "{}|{}|{:?}|{}|{}",
        plan.leader_id,
        plan.order.coin,
        plan.order.side,
        plan.order.reduce_only,
        copy_live_daemon_signal_id_without_timestamp(&plan.signal_id)
    )
}

fn copy_live_daemon_signal_id_without_timestamp(signal_id: &str) -> &str {
    signal_id
        .rsplit_once('-')
        .filter(|(_, suffix)| suffix.chars().all(|ch| ch.is_ascii_digit()))
        .map(|(prefix, _)| prefix)
        .unwrap_or(signal_id)
}

#[cfg(test)]
fn copy_live_daemon_executable_refs_for_snapshot(
    refs: &[CopyLiveDaemonWouldSubmitRef],
    options: &CopyLiveDaemonSupervisorOptions,
    persistence: &strategies::smart_money::CopyPersistenceSnapshot,
    reconciliations: &[CopyBoundedLiveWindowReconcile],
) -> (
    Vec<CopyLiveDaemonWouldSubmitRef>,
    Vec<CopyLiveDaemonSuppressedWouldSubmitRef>,
) {
    copy_live_daemon_executable_refs_for_snapshot_with_symbol_caps(
        refs,
        options,
        persistence,
        reconciliations,
        &HashMap::new(),
    )
}

fn copy_live_daemon_executable_refs_for_snapshot_with_symbol_caps(
    refs: &[CopyLiveDaemonWouldSubmitRef],
    options: &CopyLiveDaemonSupervisorOptions,
    persistence: &strategies::smart_money::CopyPersistenceSnapshot,
    reconciliations: &[CopyBoundedLiveWindowReconcile],
    account_symbol_caps: &HashMap<String, f64>,
) -> (
    Vec<CopyLiveDaemonWouldSubmitRef>,
    Vec<CopyLiveDaemonSuppressedWouldSubmitRef>,
) {
    let (margin_adjusted_refs, mut suppressed_refs) =
        copy_live_daemon_resize_open_refs_for_margin(refs, reconciliations);
    let (prepared_refs, mut follow_suppressed_refs) =
        copy_live_daemon_prepare_refs_for_follow_position_limits_with_symbol_caps(
            &margin_adjusted_refs,
            options,
            persistence,
            reconciliations,
            account_symbol_caps,
        );
    suppressed_refs.append(&mut follow_suppressed_refs);
    let (executable_refs, mut cap_suppressed_refs) =
        partition_copy_live_daemon_would_submit_refs(&prepared_refs, options);
    suppressed_refs.append(&mut cap_suppressed_refs);
    (executable_refs, suppressed_refs)
}

fn copy_live_daemon_account_symbol_caps(
    config: &config::AppConfig,
    account_ids: &[String],
) -> HashMap<String, f64> {
    account_ids
        .iter()
        .filter_map(|account_id| {
            let account = config.account(account_id)?;
            let cap = account.max_order_notional_usd;
            if cap.is_finite() && cap > 0.0 {
                Some((account.account_id.clone(), cap))
            } else {
                None
            }
        })
        .collect()
}

fn copy_live_daemon_resize_open_refs_for_margin(
    refs: &[CopyLiveDaemonWouldSubmitRef],
    reconciliations: &[CopyBoundedLiveWindowReconcile],
) -> (
    Vec<CopyLiveDaemonWouldSubmitRef>,
    Vec<CopyLiveDaemonSuppressedWouldSubmitRef>,
) {
    let mut prepared = Vec::new();
    let mut suppressed = Vec::new();
    let mut remaining_margin_by_account = reconciliations
        .iter()
        .filter_map(|reconcile| {
            let withdrawable = reconcile
                .withdrawable
                .as_deref()
                .and_then(|value| value.parse::<f64>().ok())?;
            Some((reconcile.account_id.clone(), withdrawable.max(0.0)))
        })
        .collect::<HashMap<_, _>>();
    let leverage = strategies::smart_money::COPY_MAX_LEVERAGE.max(1.0);
    let margin_multiplier = 1.0 + COPY_DAEMON_MARGIN_BUFFER_RATIO;
    let notional_to_margin = margin_multiplier / leverage + COPY_DAEMON_FEE_BUFFER_RATIO;

    for plan in refs {
        if plan.order.reduce_only {
            prepared.push(plan.clone());
            continue;
        }

        let Some(remaining_margin) = remaining_margin_by_account.get_mut(&plan.order.account_id)
        else {
            suppressed.push(CopyLiveDaemonSuppressedWouldSubmitRef {
                plan: plan.clone(),
                reason_code: "COPY_DAEMON_MARGIN_UNAVAILABLE".to_string(),
                message: format!(
                    "{} withdrawable unavailable; cannot prove opening margin for {:.6} notional",
                    plan.order.account_id, plan.order.notional_usd
                ),
            });
            continue;
        };

        let requested_notional = plan.order.notional_usd.max(0.0);
        let requested_margin = requested_notional * notional_to_margin;
        if *remaining_margin + 1e-9 >= requested_margin {
            *remaining_margin = (*remaining_margin - requested_margin).max(0.0);
            prepared.push(plan.clone());
            continue;
        }

        let resized_notional = normalize_report_zero(*remaining_margin / notional_to_margin);
        if resized_notional + 1e-9 < trading::HYPERLIQUID_MIN_ORDER_NOTIONAL_USD {
            suppressed.push(CopyLiveDaemonSuppressedWouldSubmitRef {
                plan: plan.clone(),
                reason_code: "COPY_DAEMON_MARGIN_RESIZED_BELOW_MIN".to_string(),
                message: format!(
                    "{} withdrawable={:.6} supports resized_notional={:.6}, below exchange minimum {:.6}; requested_notional={:.6}",
                    plan.order.account_id,
                    *remaining_margin,
                    resized_notional,
                    trading::HYPERLIQUID_MIN_ORDER_NOTIONAL_USD,
                    requested_notional
                ),
            });
            continue;
        }

        let mut resized = plan.clone();
        resized.order.notional_usd = resized_notional.min(requested_notional);
        let resized_margin = (resized.order.notional_usd / leverage) * margin_multiplier;
        *remaining_margin = (*remaining_margin - resized_margin).max(0.0);
        prepared.push(resized);
    }

    (prepared, suppressed)
}

#[cfg(test)]
fn copy_live_daemon_prepare_refs_for_follow_position_limits(
    refs: &[CopyLiveDaemonWouldSubmitRef],
    options: &CopyLiveDaemonSupervisorOptions,
    persistence: &strategies::smart_money::CopyPersistenceSnapshot,
    reconciliations: &[CopyBoundedLiveWindowReconcile],
) -> (
    Vec<CopyLiveDaemonWouldSubmitRef>,
    Vec<CopyLiveDaemonSuppressedWouldSubmitRef>,
) {
    copy_live_daemon_prepare_refs_for_follow_position_limits_with_symbol_caps(
        refs,
        options,
        persistence,
        reconciliations,
        &HashMap::new(),
    )
}

fn copy_live_daemon_prepare_refs_for_follow_position_limits_with_symbol_caps(
    refs: &[CopyLiveDaemonWouldSubmitRef],
    options: &CopyLiveDaemonSupervisorOptions,
    persistence: &strategies::smart_money::CopyPersistenceSnapshot,
    reconciliations: &[CopyBoundedLiveWindowReconcile],
    account_symbol_caps: &HashMap<String, f64>,
) -> (
    Vec<CopyLiveDaemonWouldSubmitRef>,
    Vec<CopyLiveDaemonSuppressedWouldSubmitRef>,
) {
    let current_signals = refs
        .iter()
        .map(|plan| plan.signal_id.as_str())
        .collect::<HashSet<_>>();
    let mut pending_by_key = HashMap::<(String, String, String), f64>::new();
    let mut effective_exposure_by_symbol = HashMap::<(String, String), f64>::new();
    let mut effective_exposure_by_account = HashMap::<String, f64>::new();
    let mut live_exposure_by_symbol = reconciliations
        .iter()
        .filter(|reconcile| reconcile.error.is_none())
        .flat_map(|reconcile| {
            reconcile
                .position_summaries
                .iter()
                .filter_map(move |position| {
                    let value = position
                        .position_value
                        .as_deref()
                        .and_then(|value| value.trim().parse::<f64>().ok())?
                        .abs();
                    if value.is_finite() {
                        Some(((reconcile.account_id.clone(), position.coin.clone()), value))
                    } else {
                        None
                    }
                })
        })
        .fold(
            HashMap::<(String, String), f64>::new(),
            |mut acc, (key, value)| {
                *acc.entry(key).or_insert(0.0) += value;
                acc
            },
        );
    let mut live_exposure_by_account = reconciliations
        .iter()
        .filter(|reconcile| reconcile.error.is_none())
        .filter_map(|reconcile| {
            let total_ntl = reconcile
                .total_ntl_pos
                .as_deref()
                .and_then(|value| value.trim().parse::<f64>().ok())?
                .abs();
            if total_ntl.is_finite() {
                Some((reconcile.account_id.clone(), total_ntl))
            } else {
                None
            }
        })
        .collect::<HashMap<_, _>>();
    for entry in &persistence.ledger_entries {
        if current_signals.contains(entry.signal_id.as_str()) {
            continue;
        }
        let exposure_delta = match entry.status {
            strategies::smart_money::CopyLedgerStatus::PendingOpen
                if copy_live_daemon_ledger_entry_has_submission(entry) =>
            {
                entry.pending_notional_usd.max(0.0)
            }
            strategies::smart_money::CopyLedgerStatus::Open
                if strategies::smart_money::copy_ledger_entry_has_execution_evidence(entry) =>
            {
                entry.remaining_notional_usd.max(0.0)
            }
            strategies::smart_money::CopyLedgerStatus::PendingReduce
            | strategies::smart_money::CopyLedgerStatus::PendingClose
                if copy_live_daemon_ledger_entry_has_submission(entry) =>
            {
                -entry.pending_notional_usd.max(0.0)
            }
            strategies::smart_money::CopyLedgerStatus::PendingOpen
            | strategies::smart_money::CopyLedgerStatus::Open
            | strategies::smart_money::CopyLedgerStatus::PendingReduce
            | strategies::smart_money::CopyLedgerStatus::PendingClose => 0.0,
            strategies::smart_money::CopyLedgerStatus::Closed
            | strategies::smart_money::CopyLedgerStatus::Rejected => 0.0,
        };
        if exposure_delta != 0.0 {
            *effective_exposure_by_symbol
                .entry((entry.local_account_id.clone(), entry.coin.clone()))
                .or_insert(0.0) += exposure_delta;
            *effective_exposure_by_account
                .entry(entry.local_account_id.clone())
                .or_insert(0.0) += exposure_delta;
        }
        if !matches!(
            entry.status,
            strategies::smart_money::CopyLedgerStatus::PendingReduce
                | strategies::smart_money::CopyLedgerStatus::PendingClose
        ) || current_signals.contains(entry.signal_id.as_str())
        {
            continue;
        }
        let key = (
            entry.local_account_id.clone(),
            entry.coin.clone(),
            copy_live_daemon_order_side_key(entry.local_side),
        );
        *pending_by_key.entry(key).or_insert(0.0) += entry.pending_notional_usd.max(0.0);
    }

    let mut prepared = Vec::new();
    let mut suppressed = Vec::new();
    for plan in refs {
        if !plan.order.reduce_only {
            let exposure_key = (plan.order.account_id.clone(), plan.order.coin.clone());
            let live_symbol_exposure = live_exposure_by_symbol
                .get(&exposure_key)
                .copied()
                .map(|value| value.max(0.0));
            let current_symbol_exposure = live_symbol_exposure.unwrap_or_else(|| {
                effective_exposure_by_symbol
                    .get(&exposure_key)
                    .copied()
                    .unwrap_or(0.0)
                    .max(0.0)
            });
            let next_symbol_exposure = current_symbol_exposure + plan.order.notional_usd.max(0.0);
            if options.hold_positions_after_submit {
                if let Some(symbol_cap) = account_symbol_caps
                    .get(&plan.order.account_id)
                    .copied()
                    .filter(|cap| cap.is_finite() && *cap > 0.0)
                {
                    if next_symbol_exposure > symbol_cap + 1e-9 {
                        let exposure_source = if live_symbol_exposure.is_some() {
                            "live symbol"
                        } else {
                            "ledger symbol"
                        };
                        suppressed.push(CopyLiveDaemonSuppressedWouldSubmitRef {
                            plan: plan.clone(),
                            reason_code: "COPY_DAEMON_MAX_SYMBOL_EXPOSURE".to_string(),
                            message: format!(
                                "follow-position {exposure_source} exposure for {} {} would become {next_symbol_exposure:.6}, above account symbol cap {symbol_cap:.6}; existing exposure {:.6}, candidate {:.6}",
                                plan.order.account_id,
                                plan.order.coin,
                                current_symbol_exposure,
                                plan.order.notional_usd
                            ),
                        });
                        continue;
                    }
                }
            }

            let live_account_exposure = live_exposure_by_account
                .get(&plan.order.account_id)
                .copied()
                .map(|value| value.max(0.0));
            let current_exposure = live_account_exposure.unwrap_or_else(|| {
                effective_exposure_by_account
                    .get(&plan.order.account_id)
                    .copied()
                    .unwrap_or(0.0)
                    .max(0.0)
            });
            let next_exposure = current_exposure + plan.order.notional_usd.max(0.0);
            let account_cap = copy_live_daemon_account_exposure_cap(
                options,
                account_symbol_caps,
                &plan.order.account_id,
            );
            if options.hold_positions_after_submit && next_exposure > account_cap + 1e-9 {
                let exposure_source = if live_account_exposure.is_some() {
                    "live account"
                } else {
                    "ledger effective"
                };
                suppressed.push(CopyLiveDaemonSuppressedWouldSubmitRef {
                    plan: plan.clone(),
                    reason_code: "COPY_DAEMON_MAX_ACCOUNT_EXPOSURE".to_string(),
                    message: format!(
                        "follow-position {exposure_source} exposure would become {next_exposure:.6}, above cap {:.6}; existing exposure {:.6}, candidate {:.6}",
                        account_cap,
                        current_exposure,
                        plan.order.notional_usd
                    ),
                });
            } else {
                if live_account_exposure.is_some() {
                    live_exposure_by_account.insert(plan.order.account_id.clone(), next_exposure);
                    live_exposure_by_symbol.insert(exposure_key, next_symbol_exposure);
                } else {
                    let next_account_exposure = next_exposure;
                    effective_exposure_by_account
                        .insert(plan.order.account_id.clone(), next_account_exposure);
                    effective_exposure_by_symbol.insert(exposure_key, next_symbol_exposure);
                }
                prepared.push(plan.clone());
            }
            continue;
        }
        let local_side = opposite_order_side(plan.order.side);
        let key = (
            plan.order.account_id.clone(),
            plan.order.coin.clone(),
            copy_live_daemon_order_side_key(local_side),
        );
        let previous_pending = pending_by_key.get(&key).copied().unwrap_or(0.0);
        let cumulative_notional = previous_pending + plan.order.notional_usd.max(0.0);
        if cumulative_notional < trading::HYPERLIQUID_MIN_ORDER_NOTIONAL_USD {
            pending_by_key.insert(key, cumulative_notional);
            suppressed.push(CopyLiveDaemonSuppressedWouldSubmitRef {
                plan: plan.clone(),
                reason_code: "COPY_DAEMON_PENDING_REDUCE_BELOW_MIN_NOTIONAL".to_string(),
                message: format!(
                    "reduce-only notional {:.6} plus prior pending {:.6} totals {:.6}, below exchange minimum {:.6}; pending reduce will accumulate before submit",
                    plan.order.notional_usd,
                    previous_pending,
                    cumulative_notional,
                    trading::HYPERLIQUID_MIN_ORDER_NOTIONAL_USD
                ),
            });
        } else {
            let mut prepared_plan = plan.clone();
            prepared_plan.order.notional_usd = cumulative_notional;
            pending_by_key.insert(key, 0.0);
            prepared.push(prepared_plan);
        }
    }
    (prepared, suppressed)
}

fn copy_live_daemon_ledger_entry_has_submission(
    entry: &strategies::smart_money::CopyLedgerEntry,
) -> bool {
    entry.submitted_at_ms.is_some()
        || entry.order_oid.is_some()
        || entry
            .order_cloid
            .as_deref()
            .is_some_and(|cloid| !cloid.trim().is_empty())
}

fn copy_live_daemon_ledger_entry_has_live_order_evidence(
    entry: &strategies::smart_money::CopyLedgerEntry,
) -> bool {
    strategies::smart_money::copy_ledger_entry_has_execution_evidence(entry)
        || entry.filled_notional_usd > 0.0
}

fn copy_live_daemon_ledger_entry_is_open_lineage(
    entry: &strategies::smart_money::CopyLedgerEntry,
) -> bool {
    if matches!(
        entry.status,
        strategies::smart_money::CopyLedgerStatus::Rejected
    ) {
        return false;
    }
    let signal_id = entry.signal_id.to_ascii_lowercase();
    if signal_id.contains("-close-") {
        return false;
    }
    matches!(
        entry.status,
        strategies::smart_money::CopyLedgerStatus::PendingOpen
            | strategies::smart_money::CopyLedgerStatus::Open
    ) || signal_id.contains("-open-")
}

fn copy_live_daemon_persistent_submit_snapshot_safe_to_save(
    report: &CopyLiveDaemonPersistentLiveSubmitReport,
) -> bool {
    let live_submitted_count = copy_canary_live_submitted_reports(&report.submitted_reports).len();
    if live_submitted_count == 0 {
        return false;
    }

    let evidence_ok = report.order_evidence.len() == live_submitted_count
        && report
            .order_evidence
            .iter()
            .all(copy_execution_canary_order_evidence_ok);
    evidence_ok && report.cleanup_errors.is_empty()
}

fn copy_live_daemon_save_live_submit_progress_snapshot(
    options: &CopyLiveDaemonSupervisorOptions,
    approved_records: &[strategies::smart_money::CopyDryRunShadowRecord],
    submitted_reports: &[domain::WorkerReport],
) -> Result<bool> {
    if !copy_canary_has_live_submission(submitted_reports) {
        return Ok(false);
    }
    let existing_snapshot =
        strategies::smart_money::load_copy_persistence_snapshot(&options.persistence_path)?;
    let (_reconciliations, progress_snapshot) = reconcile_copy_canary_ledger(
        Some(&existing_snapshot),
        approved_records,
        submitted_reports,
        &[],
    );
    let snapshot_for_save =
        copy_live_daemon_merge_persistence_snapshots_for_save(existing_snapshot, progress_snapshot);
    strategies::smart_money::save_copy_persistence_snapshot(
        &options.persistence_path,
        &snapshot_for_save,
    )?;
    Ok(true)
}

fn copy_live_daemon_persistence_snapshot_for_save(
    mut snapshot: strategies::smart_money::CopyPersistenceSnapshot,
) -> strategies::smart_money::CopyPersistenceSnapshot {
    snapshot.ledger_entries.retain(|entry| {
        if !strategies::smart_money::copy_persistence_entry_is_safe_to_load(entry) {
            return false;
        }
        !matches!(
            entry.status,
            strategies::smart_money::CopyLedgerStatus::PendingOpen
        ) || copy_live_daemon_ledger_entry_has_submission(entry)
    });
    snapshot
}

#[cfg(test)]
fn copy_live_daemon_prune_snapshot_against_reconciliations(
    mut snapshot: strategies::smart_money::CopyPersistenceSnapshot,
    reconciliations: &[CopyBoundedLiveWindowReconcile],
) -> strategies::smart_money::CopyPersistenceSnapshot {
    let readable_accounts = reconciliations
        .iter()
        .filter(|reconcile| reconcile.error.is_none())
        .map(|reconcile| reconcile.account_id.as_str())
        .collect::<HashSet<_>>();
    if readable_accounts.is_empty() {
        return snapshot;
    }

    let live_position_keys = reconciliations
        .iter()
        .filter(|reconcile| reconcile.error.is_none())
        .flat_map(|reconcile| {
            reconcile
                .position_summaries
                .iter()
                .filter_map(move |position| {
                    let local_side = copy_live_daemon_local_side_from_position_szi(&position.szi)?;
                    Some((
                        reconcile.account_id.clone(),
                        position.coin.clone(),
                        copy_live_daemon_order_side_key(local_side),
                    ))
                })
        })
        .collect::<HashSet<_>>();

    snapshot.ledger_entries.retain(|entry| {
        if !readable_accounts.contains(entry.local_account_id.as_str())
            || !matches!(
                entry.status,
                strategies::smart_money::CopyLedgerStatus::Open
                    | strategies::smart_money::CopyLedgerStatus::PendingReduce
                    | strategies::smart_money::CopyLedgerStatus::PendingClose
            )
        {
            return true;
        }
        let key = (
            entry.local_account_id.clone(),
            entry.coin.clone(),
            copy_live_daemon_order_side_key(entry.local_side),
        );
        live_position_keys.contains(&key)
    });
    snapshot
}

fn copy_live_daemon_snapshot_entry_matches_live_market_state(
    config: &config::AppConfig,
    entry: &strategies::smart_money::CopyLedgerEntry,
    perp_state_by_scope: &HashMap<
        (String, String),
        std::result::Result<hyperliquid::ClearinghouseState, String>,
    >,
    spot_state_by_account: &HashMap<
        String,
        std::result::Result<hyperliquid::SpotClearinghouseState, String>,
    >,
) -> bool {
    let (_, dex) = copy_daemon_market_dex_for_coin(&entry.coin);
    match dex.as_deref() {
        Some("spot") => match spot_state_by_account.get(&entry.local_account_id) {
            Some(Ok(state)) => copy_live_daemon_spot_entry_has_matching_position(
                state,
                &entry.coin,
                entry.local_side,
            ),
            Some(Err(_)) | None => true,
        },
        _ => {
            let dex_key = dex.unwrap_or_else(|| config.hyperliquid.dex.clone());
            match perp_state_by_scope.get(&(entry.local_account_id.clone(), dex_key)) {
                Some(Ok(state)) => copy_live_daemon_perp_entry_has_matching_position(
                    state,
                    &entry.coin,
                    entry.local_side,
                ),
                Some(Err(_)) | None => true,
            }
        }
    }
}

fn copy_live_daemon_prune_snapshot_against_market_state_maps(
    config: &config::AppConfig,
    snapshot: strategies::smart_money::CopyPersistenceSnapshot,
    readable_accounts: &HashSet<String>,
    perp_state_by_scope: &HashMap<
        (String, String),
        std::result::Result<hyperliquid::ClearinghouseState, String>,
    >,
    spot_state_by_account: &HashMap<
        String,
        std::result::Result<hyperliquid::SpotClearinghouseState, String>,
    >,
) -> strategies::smart_money::CopyPersistenceSnapshot {
    let mut ledger_entries = Vec::with_capacity(snapshot.ledger_entries.len());
    for entry in snapshot.ledger_entries {
        if !readable_accounts.contains(&entry.local_account_id)
            || !matches!(
                entry.status,
                strategies::smart_money::CopyLedgerStatus::Open
                    | strategies::smart_money::CopyLedgerStatus::PendingReduce
                    | strategies::smart_money::CopyLedgerStatus::PendingClose
            )
        {
            ledger_entries.push(entry);
            continue;
        }

        if copy_live_daemon_snapshot_entry_matches_live_market_state(
            config,
            &entry,
            perp_state_by_scope,
            spot_state_by_account,
        ) {
            ledger_entries.push(entry);
        }
    }

    strategies::smart_money::CopyPersistenceSnapshot {
        ledger_entries,
        ..snapshot
    }
}

async fn copy_live_daemon_prune_snapshot_against_live_markets(
    config: &config::AppConfig,
    snapshot: strategies::smart_money::CopyPersistenceSnapshot,
    reconciliations: &[CopyBoundedLiveWindowReconcile],
) -> strategies::smart_money::CopyPersistenceSnapshot {
    let readable_accounts = reconciliations
        .iter()
        .filter(|reconcile| reconcile.error.is_none())
        .map(|reconcile| reconcile.account_id.clone())
        .collect::<HashSet<_>>();
    if readable_accounts.is_empty() {
        return snapshot;
    }

    let mut required_perp_scopes = HashSet::<(String, String)>::new();
    let mut required_spot_accounts = HashSet::<String>::new();
    for entry in snapshot.ledger_entries.iter().filter(|entry| {
        readable_accounts.contains(&entry.local_account_id)
            && matches!(
                entry.status,
                strategies::smart_money::CopyLedgerStatus::Open
                    | strategies::smart_money::CopyLedgerStatus::PendingReduce
                    | strategies::smart_money::CopyLedgerStatus::PendingClose
            )
    }) {
        let (_, dex) = copy_daemon_market_dex_for_coin(&entry.coin);
        if dex.as_deref() == Some("spot") {
            required_spot_accounts.insert(entry.local_account_id.clone());
        } else {
            required_perp_scopes.insert((
                entry.local_account_id.clone(),
                dex.unwrap_or_else(|| config.hyperliquid.dex.clone()),
            ));
        }
    }

    let mut perp_state_by_scope = HashMap::<
        (String, String),
        std::result::Result<hyperliquid::ClearinghouseState, String>,
    >::new();
    for (account_id, dex) in required_perp_scopes {
        let state = if let Some(account) = config.account(&account_id) {
            hyperliquid::fetch_clearinghouse_state(
                &config.app.environment,
                dex.as_str(),
                &account.address,
            )
            .await
            .map_err(|error| error.to_string())
        } else {
            Err(format!(
                "account {} not found for multi-market copy prune",
                account_id
            ))
        };
        perp_state_by_scope.insert((account_id, dex), state);
    }

    let mut spot_state_by_account =
        HashMap::<String, std::result::Result<hyperliquid::SpotClearinghouseState, String>>::new();
    for account_id in required_spot_accounts {
        let state = if let Some(account) = config.account(&account_id) {
            hyperliquid::fetch_spot_clearinghouse_state(&config.app.environment, &account.address)
                .await
                .map_err(|error| error.to_string())
        } else {
            Err(format!(
                "account {} not found for spot copy prune",
                account_id
            ))
        };
        spot_state_by_account.insert(account_id, state);
    }

    copy_live_daemon_prune_snapshot_against_market_state_maps(
        config,
        snapshot,
        &readable_accounts,
        &perp_state_by_scope,
        &spot_state_by_account,
    )
}

fn copy_live_daemon_recover_open_ledger_from_live_positions(
    mut snapshot: strategies::smart_money::CopyPersistenceSnapshot,
    reconciliations: &[CopyBoundedLiveWindowReconcile],
    options: &CopyLiveDaemonSupervisorOptions,
) -> Result<strategies::smart_money::CopyPersistenceSnapshot> {
    let recent_shadow_entries =
        copy_live_daemon_recent_shadow_entries_for_recovery(options, 2_000)?;
    if recent_shadow_entries.is_empty() {
        return Ok(snapshot);
    }

    let selected_accounts = copy_live_daemon_selected_account_set(options);

    for reconciliation in reconciliations {
        if reconciliation.error.is_some()
            || !selected_accounts.contains(reconciliation.account_id.as_str())
        {
            continue;
        }
        for position in &reconciliation.position_summaries {
            let Some(local_side) = copy_live_daemon_local_side_from_position_szi(&position.szi)
            else {
                continue;
            };
            let Some(shadow) = recent_shadow_entries.iter().find(|entry| {
                entry.status.eq_ignore_ascii_case("would_copy")
                    && entry.coin == position.coin
                    && entry.side == Some(local_side)
                    && !entry.reduce_only.unwrap_or(false)
                    && entry
                        .local_account_id
                        .as_deref()
                        .is_none_or(|account_id| account_id == reconciliation.account_id)
                    && entry
                        .signal_id
                        .as_deref()
                        .is_some_and(|signal_id| !signal_id.trim().is_empty())
            }) else {
                continue;
            };

            let position_notional = position
                .position_value
                .as_deref()
                .and_then(|value| value.trim().parse::<f64>().ok())
                .map(f64::abs)
                .filter(|value| value.is_finite() && *value > 0.0)
                .unwrap_or_else(|| shadow.notional_usd.unwrap_or_default().max(0.0));
            let mapped_notional = copy_live_daemon_active_open_mapping_notional_for_key(
                &snapshot,
                &reconciliation.account_id,
                &position.coin,
                local_side,
            );
            let uncovered_notional = (position_notional - mapped_notional).max(0.0);
            if uncovered_notional <= 0.0
                || uncovered_notional + 1e-9 < trading::HYPERLIQUID_MIN_ORDER_NOTIONAL_USD
                || copy_live_daemon_uncovered_position_notional_explained_by_unrealized_pnl(
                    position,
                    uncovered_notional,
                )
            {
                continue;
            }
            if position_notional <= 0.0 {
                continue;
            }

            snapshot
                .ledger_entries
                .push(strategies::smart_money::CopyLedgerEntry {
                    local_account_id: reconciliation.account_id.clone(),
                    leader_id: shadow.leader_id.clone(),
                    leader_group: shadow.leader_id.clone(),
                    signal_id: shadow.signal_id.clone().unwrap_or_default(),
                    coin: position.coin.clone(),
                    local_side,
                    order_cloid: None,
                    order_oid: None,
                    submitted_at_ms: Some(shadow.occurred_at_ms),
                    filled_at_ms: Some(domain::now_ms()),
                    planned_notional_usd: uncovered_notional,
                    pending_notional_usd: 0.0,
                    filled_notional_usd: uncovered_notional,
                    remaining_notional_usd: uncovered_notional,
                    status: strategies::smart_money::CopyLedgerStatus::Open,
                });
        }
    }

    Ok(snapshot)
}

fn copy_live_daemon_recent_shadow_entries_for_recovery(
    options: &CopyLiveDaemonSupervisorOptions,
    limit: usize,
) -> Result<Vec<strategies::smart_money::CopyShadowHistoryEntry>> {
    let mut paths = vec![options.shadow_history_path.clone()];
    if let Some(parent) = options.shadow_history_path.parent()
        && let Ok(read_dir) = std::fs::read_dir(parent)
    {
        let mut siblings = read_dir
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path != &options.shadow_history_path)
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.starts_with("persistent-live-soak-") && name.ends_with("-shadow.jsonl")
                    })
            })
            .filter_map(|path| {
                let modified = std::fs::metadata(&path)
                    .ok()
                    .and_then(|metadata| metadata.modified().ok())?;
                Some((modified, path))
            })
            .collect::<Vec<_>>();
        siblings.sort_by_key(|right| std::cmp::Reverse(right.0));
        paths.extend(siblings.into_iter().take(8).map(|(_, path)| path));
    }

    let mut entries = Vec::new();
    let per_path_limit = limit.max(1);
    for path in paths {
        match strategies::smart_money::read_recent_copy_shadow_history_entries(
            &path,
            per_path_limit,
        ) {
            Ok(mut path_entries) => entries.append(&mut path_entries),
            Err(error) if path == options.shadow_history_path => return Err(error),
            Err(_) => {}
        }
    }
    entries.sort_by_key(|right| std::cmp::Reverse(right.occurred_at_ms));
    if entries.len() > limit {
        entries.truncate(limit);
    }
    Ok(entries)
}

fn copy_live_daemon_selected_account_set(
    options: &CopyLiveDaemonSupervisorOptions,
) -> HashSet<&str> {
    let mut accounts = HashSet::new();
    for account_id in &options.account_ids {
        if !account_id.trim().is_empty() {
            accounts.insert(account_id.as_str());
        }
    }
    if accounts.is_empty()
        && let Some(account_id) = options.local_account_id.as_deref()
        && !account_id.trim().is_empty()
    {
        accounts.insert(account_id);
    }
    accounts
}

fn copy_live_daemon_active_copy_principal_notional_by_account(
    snapshot: &strategies::smart_money::CopyPersistenceSnapshot,
) -> HashMap<String, f64> {
    snapshot
        .ledger_entries
        .iter()
        .filter(|entry| {
            copy_live_daemon_ledger_entry_is_active_open_mapping(entry)
                && copy_live_daemon_ledger_entry_has_live_order_evidence(entry)
                && !entry.local_account_id.trim().is_empty()
        })
        .fold(HashMap::<String, f64>::new(), |mut acc, entry| {
            let notional = copy_live_daemon_active_open_mapping_notional(entry);
            if notional > 1e-9 {
                *acc.entry(entry.local_account_id.clone()).or_insert(0.0) += notional;
            }
            acc
        })
}

fn copy_live_daemon_ledger_entry_is_active_open_mapping(
    entry: &strategies::smart_money::CopyLedgerEntry,
) -> bool {
    matches!(
        entry.status,
        strategies::smart_money::CopyLedgerStatus::Open
            | strategies::smart_money::CopyLedgerStatus::PendingOpen
    ) && copy_live_daemon_ledger_entry_is_open_lineage(entry)
}

fn copy_live_daemon_active_open_mapping_notional(
    entry: &strategies::smart_money::CopyLedgerEntry,
) -> f64 {
    if entry.remaining_notional_usd > 1e-9 {
        return entry.remaining_notional_usd.max(0.0);
    }
    if matches!(
        entry.status,
        strategies::smart_money::CopyLedgerStatus::PendingOpen
    ) {
        return entry
            .filled_notional_usd
            .max(entry.pending_notional_usd)
            .max(entry.planned_notional_usd)
            .max(0.0);
    }
    0.0
}

fn copy_live_daemon_active_open_mapping_notional_for_key(
    snapshot: &strategies::smart_money::CopyPersistenceSnapshot,
    account_id: &str,
    coin: &str,
    local_side: domain::OrderSide,
) -> f64 {
    snapshot
        .ledger_entries
        .iter()
        .filter(|entry| {
            entry.local_account_id == account_id
                && entry.coin == coin
                && entry.local_side == local_side
                && copy_live_daemon_ledger_entry_is_active_open_mapping(entry)
                && copy_live_daemon_ledger_entry_has_live_order_evidence(entry)
        })
        .map(copy_live_daemon_active_open_mapping_notional)
        .sum()
}

fn copy_live_daemon_open_lineage_notional_for_key(
    snapshot: &strategies::smart_money::CopyPersistenceSnapshot,
    account_id: &str,
    coin: &str,
    local_side: domain::OrderSide,
) -> f64 {
    snapshot
        .ledger_entries
        .iter()
        .filter(|entry| {
            entry.local_account_id == account_id
                && entry.coin == coin
                && entry.local_side == local_side
                && copy_live_daemon_ledger_entry_is_open_lineage(entry)
                && copy_live_daemon_ledger_entry_has_live_order_evidence(entry)
        })
        .map(|entry| {
            entry
                .filled_notional_usd
                .max(entry.remaining_notional_usd)
                .max(entry.planned_notional_usd)
                .max(0.0)
        })
        .sum()
}

fn copy_live_daemon_uncovered_position_notional_explained_by_unrealized_pnl(
    position: &CopyBoundedLiveWindowPositionSummary,
    uncovered_notional: f64,
) -> bool {
    let pnl = position
        .unrealized_pnl
        .as_deref()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .map(f64::abs)
        .unwrap_or_default();
    pnl.is_finite() && uncovered_notional <= pnl + COPY_DAEMON_PNL_DRIFT_TOLERANCE_USD
}

fn copy_live_daemon_uncovered_position_notional_explained_by_same_coin_lineage(
    snapshot: &strategies::smart_money::CopyPersistenceSnapshot,
    account_id: &str,
    coin: &str,
    local_side: domain::OrderSide,
    position_notional: f64,
    mapped_notional: f64,
) -> bool {
    mapped_notional > COPY_DAEMON_LINEAGE_RESIDUAL_TOLERANCE_USD
        && mapped_notional + COPY_DAEMON_LINEAGE_RESIDUAL_TOLERANCE_USD < position_notional
        && copy_live_daemon_open_lineage_notional_for_key(snapshot, account_id, coin, local_side)
            + COPY_DAEMON_LINEAGE_RESIDUAL_TOLERANCE_USD
            >= position_notional
}

fn copy_live_daemon_unmapped_position_keys(
    snapshot: &strategies::smart_money::CopyPersistenceSnapshot,
    reconciliations: &[CopyBoundedLiveWindowReconcile],
) -> Vec<String> {
    let copy_lineage_notional_by_key = snapshot
        .ledger_entries
        .iter()
        .filter(|entry| {
            copy_live_daemon_ledger_entry_is_active_open_mapping(entry)
                && copy_live_daemon_ledger_entry_has_live_order_evidence(entry)
                && !entry.local_account_id.trim().is_empty()
                && !entry.coin.trim().is_empty()
        })
        .fold(
            HashMap::<(String, String, String), f64>::new(),
            |mut acc, entry| {
                let notional = copy_live_daemon_active_open_mapping_notional(entry);
                if notional > 1e-9 {
                    *acc.entry((
                        entry.local_account_id.clone(),
                        entry.coin.clone(),
                        copy_live_daemon_order_side_key(entry.local_side),
                    ))
                    .or_insert(0.0) += notional;
                }
                acc
            },
        );
    let mut unmapped = Vec::new();
    for reconciliation in reconciliations
        .iter()
        .filter(|reconciliation| reconciliation.error.is_none())
    {
        for position in &reconciliation.position_summaries {
            let Some(local_side) = copy_live_daemon_local_side_from_position_szi(&position.szi)
            else {
                continue;
            };
            let position_notional = position
                .position_value
                .as_deref()
                .and_then(|value| value.trim().parse::<f64>().ok())
                .map(f64::abs)
                .unwrap_or_default();
            if position_notional + 1e-9 < trading::HYPERLIQUID_MIN_ORDER_NOTIONAL_USD {
                continue;
            }
            let side_key = copy_live_daemon_order_side_key(local_side);
            let mapped_notional = copy_lineage_notional_by_key
                .get(&(
                    reconciliation.account_id.clone(),
                    position.coin.clone(),
                    side_key.clone(),
                ))
                .copied()
                .unwrap_or_default();
            let uncovered_notional = (position_notional - mapped_notional).max(0.0);
            if uncovered_notional + 1e-9 >= trading::HYPERLIQUID_MIN_ORDER_NOTIONAL_USD
                && !copy_live_daemon_uncovered_position_notional_explained_by_unrealized_pnl(
                    position,
                    uncovered_notional,
                )
                && !copy_live_daemon_uncovered_position_notional_explained_by_same_coin_lineage(
                    snapshot,
                    &reconciliation.account_id,
                    &position.coin,
                    local_side,
                    position_notional,
                    mapped_notional,
                )
            {
                unmapped.push(format!(
                    "{}:{}:{}:uncovered={uncovered_notional:.6}:live={position_notional:.6}:mapped={mapped_notional:.6}",
                    reconciliation.account_id, position.coin, side_key
                ));
            }
        }
    }
    unmapped.sort();
    unmapped.dedup();
    unmapped
}

fn copy_live_daemon_local_side_from_position_szi(szi: &str) -> Option<domain::OrderSide> {
    let value = szi.parse::<f64>().ok()?;
    if value > 1e-12 {
        Some(domain::OrderSide::Buy)
    } else if value < -1e-12 {
        Some(domain::OrderSide::Sell)
    } else {
        None
    }
}

fn copy_live_daemon_merge_persistence_snapshots(
    existing: strategies::smart_money::CopyPersistenceSnapshot,
    incoming: strategies::smart_money::CopyPersistenceSnapshot,
) -> strategies::smart_money::CopyPersistenceSnapshot {
    let mut seen_event_keys = existing.seen_event_keys;
    seen_event_keys.extend(incoming.seen_event_keys);
    seen_event_keys.sort();
    seen_event_keys.dedup();

    let mut entries_by_key = HashMap::<String, strategies::smart_money::CopyLedgerEntry>::new();
    for entry in existing
        .ledger_entries
        .into_iter()
        .chain(incoming.ledger_entries)
    {
        entries_by_key.insert(copy_live_daemon_ledger_entry_identity(&entry), entry);
    }
    let mut ledger_entries = entries_by_key.into_values().collect::<Vec<_>>();
    copy_live_daemon_apply_closed_reduces_to_open_entries(&mut ledger_entries);
    copy_live_daemon_deduplicate_recovered_open_entries(&mut ledger_entries);
    ledger_entries.sort_by(|left, right| {
        copy_live_daemon_ledger_entry_identity(left)
            .cmp(&copy_live_daemon_ledger_entry_identity(right))
    });

    strategies::smart_money::CopyPersistenceSnapshot {
        schema_version: 1,
        saved_at_ms: domain::now_ms(),
        seen_event_keys,
        ledger_entries,
    }
}

fn copy_live_daemon_merge_persistence_snapshots_for_save(
    existing: strategies::smart_money::CopyPersistenceSnapshot,
    incoming: strategies::smart_money::CopyPersistenceSnapshot,
) -> strategies::smart_money::CopyPersistenceSnapshot {
    copy_live_daemon_persistence_snapshot_for_save(copy_live_daemon_merge_persistence_snapshots(
        existing, incoming,
    ))
}

fn copy_live_daemon_ledger_entry_identity(
    entry: &strategies::smart_money::CopyLedgerEntry,
) -> String {
    format!(
        "signal:{}:{}:{}:{}",
        entry.local_account_id, entry.coin, entry.signal_id, entry.leader_id
    )
}

fn copy_live_daemon_ledger_open_mapping_key(
    entry: &strategies::smart_money::CopyLedgerEntry,
) -> Option<(String, String, String, String)> {
    if !matches!(
        entry.status,
        strategies::smart_money::CopyLedgerStatus::Open
    ) {
        return None;
    }
    Some((
        entry.local_account_id.clone(),
        entry.leader_group.clone(),
        entry.coin.clone(),
        copy_live_daemon_order_side_key(entry.local_side),
    ))
}

fn copy_live_daemon_open_entry_has_order_evidence(
    entry: &strategies::smart_money::CopyLedgerEntry,
) -> bool {
    entry.order_oid.is_some()
        || entry
            .order_cloid
            .as_deref()
            .is_some_and(|cloid| !cloid.trim().is_empty())
}

fn copy_live_daemon_deduplicate_recovered_open_entries(
    ledger_entries: &mut Vec<strategies::smart_money::CopyLedgerEntry>,
) {
    let mut evidenced_open_notional = HashMap::<(String, String, String, String), f64>::new();
    for entry in ledger_entries.iter() {
        if !copy_live_daemon_open_entry_has_order_evidence(entry) {
            continue;
        }
        let Some(key) = copy_live_daemon_ledger_open_mapping_key(entry) else {
            continue;
        };
        let remaining = entry.remaining_notional_usd.max(0.0);
        if remaining.is_finite() && remaining > 0.0 {
            *evidenced_open_notional.entry(key).or_insert(0.0) += remaining;
        }
    }

    let mut duplicate_recovered_identities = HashSet::<String>::new();
    for entry in ledger_entries.iter_mut() {
        if copy_live_daemon_open_entry_has_order_evidence(entry) {
            continue;
        }
        let Some(key) = copy_live_daemon_ledger_open_mapping_key(entry) else {
            continue;
        };
        let Some(covered_notional) = evidenced_open_notional.get_mut(&key) else {
            continue;
        };
        if *covered_notional <= 0.0 {
            continue;
        }

        let current_remaining = entry.remaining_notional_usd.max(0.0);
        let covered = current_remaining.min(*covered_notional);
        *covered_notional = (*covered_notional - covered).max(0.0);
        let residual = (current_remaining - covered).max(0.0);
        if residual <= COPY_DAEMON_RECOVERED_LEDGER_DUPLICATE_TOLERANCE_USD {
            duplicate_recovered_identities.insert(copy_live_daemon_ledger_entry_identity(entry));
            entry.remaining_notional_usd = 0.0;
            entry.pending_notional_usd = 0.0;
            entry.status = strategies::smart_money::CopyLedgerStatus::Closed;
        } else {
            entry.planned_notional_usd = residual;
            entry.filled_notional_usd = residual;
            entry.remaining_notional_usd = residual;
            entry.pending_notional_usd = 0.0;
        }
    }

    ledger_entries.retain(|entry| {
        !duplicate_recovered_identities.contains(&copy_live_daemon_ledger_entry_identity(entry))
    });
}

fn copy_live_daemon_apply_closed_reduces_to_open_entries(
    ledger_entries: &mut [strategies::smart_money::CopyLedgerEntry],
) {
    let mut reductions = Vec::<((String, String, String), f64, Option<u64>)>::new();
    for entry in ledger_entries.iter() {
        if !copy_live_daemon_closed_reduce_entry_consumes_open(entry) {
            continue;
        }
        let reduction_notional = entry
            .planned_notional_usd
            .max(entry.filled_notional_usd)
            .max(0.0);
        if reduction_notional <= 0.0 || !reduction_notional.is_finite() {
            continue;
        }
        let key = (
            entry.local_account_id.clone(),
            entry.coin.clone(),
            copy_live_daemon_order_side_key(entry.local_side),
        );
        reductions.push((
            key,
            reduction_notional,
            copy_live_daemon_ledger_entry_event_time_ms(entry),
        ));
    }

    if reductions.is_empty() {
        return;
    }
    reductions.sort_by(|left, right| left.2.cmp(&right.2));

    let mut open_indices_by_key = HashMap::<(String, String, String), Vec<usize>>::new();
    for (index, entry) in ledger_entries.iter().enumerate() {
        if !matches!(
            entry.status,
            strategies::smart_money::CopyLedgerStatus::Open
        ) {
            continue;
        }
        let key = (
            entry.local_account_id.clone(),
            entry.coin.clone(),
            copy_live_daemon_order_side_key(entry.local_side),
        );
        open_indices_by_key.entry(key).or_default().push(index);
    }
    for indices in open_indices_by_key.values_mut() {
        indices.sort_by(|left, right| {
            copy_live_daemon_ledger_entry_event_time_ms(&ledger_entries[*left]).cmp(
                &copy_live_daemon_ledger_entry_event_time_ms(&ledger_entries[*right]),
            )
        });
    }

    for (key, mut reduction_notional, reduction_time_ms) in reductions {
        let Some(indices) = open_indices_by_key.get(&key) else {
            continue;
        };
        for index in indices {
            if reduction_notional <= 1e-9 {
                break;
            }
            let open_time_ms = copy_live_daemon_ledger_entry_event_time_ms(&ledger_entries[*index]);
            if let (Some(open_time_ms), Some(reduction_time_ms)) = (open_time_ms, reduction_time_ms)
                && open_time_ms > reduction_time_ms
            {
                continue;
            }
            let baseline = ledger_entries[*index]
                .filled_notional_usd
                .max(ledger_entries[*index].remaining_notional_usd)
                .max(0.0);
            let remaining = (baseline - reduction_notional).max(0.0);
            let consumed = baseline - remaining;
            reduction_notional = (reduction_notional - consumed).max(0.0);
            ledger_entries[*index].remaining_notional_usd = remaining;
            if remaining <= 1e-9 {
                ledger_entries[*index].remaining_notional_usd = 0.0;
                ledger_entries[*index].status = strategies::smart_money::CopyLedgerStatus::Closed;
            }
        }
    }
}

fn copy_live_daemon_ledger_entry_event_time_ms(
    entry: &strategies::smart_money::CopyLedgerEntry,
) -> Option<u64> {
    entry.filled_at_ms.or(entry.submitted_at_ms)
}

fn copy_live_daemon_closed_reduce_entry_consumes_open(
    entry: &strategies::smart_money::CopyLedgerEntry,
) -> bool {
    matches!(
        entry.status,
        strategies::smart_money::CopyLedgerStatus::Closed
    ) && entry.signal_id.contains("-close-")
}

fn copy_live_daemon_order_side_key(side: domain::OrderSide) -> String {
    match side {
        domain::OrderSide::Buy => "buy".to_string(),
        domain::OrderSide::Sell => "sell".to_string(),
    }
}

fn copy_live_daemon_restart_dedupe_probe(
    config: &config::AppConfig,
    options: &CopyLiveDaemonAcceptanceOptions,
    persistence: &strategies::smart_money::CopyPersistenceSnapshot,
) -> Result<CopyLiveDaemonRestartProbe> {
    let leaders = parse_copy_shadow_smoke_leaders(&options.leaders)?;
    let leader = leaders
        .first()
        .cloned()
        .unwrap_or_else(|| CopyShadowSmokeLeader {
            leader_id: "acceptance_leader".to_string(),
            leader_address: "0x0000000000000000000000000000000000000000".to_string(),
        });
    let target_accounts = copy_execution_canary_target_accounts(config, &options.account_ids, None);
    let now = domain::now_ms();
    let event_id = format!("copy-live-daemon-acceptance-replay-{now}");
    let (market, dex) = copy_daemon_market_dex_for_coin(&options.coin);
    let action = strategies::smart_money::SemanticLeaderAction {
        leader_id: leader.leader_id.clone(),
        leader_address: leader.leader_address.clone(),
        market,
        dex,
        coin: options.coin.clone(),
        event_id: event_id.clone(),
        kind: match options.side {
            domain::OrderSide::Buy => strategies::smart_money::LeaderActionKind::OpenLong,
            domain::OrderSide::Sell => strategies::smart_money::LeaderActionKind::OpenShort,
        },
        confidence: strategies::smart_money::LeaderActionConfidence::Strong,
        leader_notional_usd: options.leader_notional_usd,
        close_leader_notional_usd: None,
        open_leader_notional_usd: None,
        exchange_time_ms: now,
        received_at_ms: now,
        reason: "acceptance_restart_dedupe_probe".to_string(),
    };
    let config_for_strategy = strategies::smart_money::SmartMoneyCopyConfig {
        strategy_id: "copy_live_daemon_acceptance".to_string(),
        default_copy_ratio: 1.0,
        max_slippage_bps: options.max_slippage_bps,
        leaders: vec![strategies::smart_money::LeaderRule {
            leader_id: leader.leader_id.clone(),
            leader_address: leader.leader_address.clone(),
            enabled: true,
            copy_ratio: 1.0,
        }],
        symbol_limits: vec![strategies::smart_money::SymbolCopyLimit {
            coin: options.coin.clone(),
            max_signal_notional_usd: options.leader_notional_usd,
        }],
    };
    let ctx = strategy::StrategyContext {
        target_accounts,
        signal_ttl_ms: config.process.signal_ttl_ms,
    };
    let mut first_process =
        strategies::smart_money::SmartMoneyCopyStrategy::new_with_seen_event_keys(
            config_for_strategy.clone(),
            persistence.seen_event_keys.clone(),
        );
    let first = first_process.signals_from_semantic_action(&ctx, &action);
    let saved_snapshot = first_process.persistence_snapshot(now + 1, &persistence.ledger());
    strategies::smart_money::save_copy_persistence_snapshot(
        &options.persistence_path,
        &saved_snapshot,
    )?;
    let loaded =
        strategies::smart_money::load_copy_persistence_snapshot(&options.persistence_path)?;
    let mut restarted = strategies::smart_money::SmartMoneyCopyStrategy::new_with_seen_event_keys(
        config_for_strategy,
        loaded.seen_event_keys.clone(),
    );
    let replay = restarted.signals_from_semantic_action(&ctx, &action);
    let mut fresh_action = action.clone();
    fresh_action.event_id = format!("{event_id}-fresh");
    let fresh = restarted.signals_from_semantic_action(&ctx, &fresh_action);

    Ok(CopyLiveDaemonRestartProbe {
        event_id,
        first_emit_count: first.len(),
        replay_emit_count: replay.len(),
        fresh_after_restart_emit_count: fresh.len(),
        saved_seen_event_keys: saved_snapshot.seen_event_keys.len(),
        loaded_seen_event_keys: loaded.seen_event_keys.len(),
    })
}

fn build_synthetic_copy_shadow_records(
    config: &config::AppConfig,
    options: &CopyExecutionCanaryOptions,
    account: &config::AccountConfig,
    leader: &CopyShadowSmokeLeader,
    target_accounts: &[String],
) -> Vec<strategies::smart_money::CopyDryRunShadowRecord> {
    let now = domain::now_ms();
    let watch = strategies::smart_money::SmartMoneyLeaderWatch {
        leader_id: leader.leader_id.clone(),
        leader_address: leader.leader_address.clone(),
    };
    let strategy = strategies::smart_money::SmartMoneyCopyStrategy::new(
        strategies::smart_money::SmartMoneyCopyConfig {
            strategy_id: "copy_execution_canary".to_string(),
            default_copy_ratio: 1.0,
            max_slippage_bps: 25.0,
            leaders: vec![strategies::smart_money::LeaderRule {
                leader_id: leader.leader_id.clone(),
                leader_address: leader.leader_address.clone(),
                enabled: true,
                copy_ratio: 1.0,
            }],
            symbol_limits: vec![strategies::smart_money::SymbolCopyLimit {
                coin: options.coin.clone(),
                max_signal_notional_usd: options.leader_notional_usd,
            }],
        },
    );
    let mut pipeline = strategies::smart_money::CopyDryRunShadowPipeline::new(
        strategies::smart_money::CopyDryRunShadowConfig {
            local_account_id: account.account_id.clone(),
            target_accounts: target_accounts.to_vec(),
            signal_ttl_ms: config.process.signal_ttl_ms,
            max_signal_delay_ms: config.process.signal_ttl_ms.max(1),
            account_copy_ratio: account.copy_ratio,
            principal_cap_usd: account.max_order_notional_usd
                / strategies::smart_money::COPY_MAX_LEVERAGE.max(1.0),
            leverage: strategies::smart_money::COPY_MAX_LEVERAGE,
            max_signal_notional_usd: Some(account.max_order_notional_usd),
            exchange_min_open_notional_usd: trading::HYPERLIQUID_MIN_ORDER_NOTIONAL_USD,
            allow_short: true,
            max_effective_exposure_usd: Some(account.max_order_notional_usd),
            blocked_symbols: config.module_blocked_symbols("copy").to_vec(),
            live_gate: strategies::smart_money::CopyLiveGateInput {
                process_dry_run: true,
                live_copy_enabled: false,
                account_worker_live: false,
            },
        },
        strategy,
        strategies::smart_money::CopyLedger::new(),
    );

    let signed_size = match options.side {
        domain::OrderSide::Buy => options.leader_size,
        domain::OrderSide::Sell => -options.leader_size,
    };
    let before = copy_shadow_position_event(
        &watch,
        &options.coin,
        0.0,
        0.0,
        now,
        config.hyperliquid.dex.as_str(),
    );
    let fill = strategy::LeaderFillEvent {
        event_id: format!("copy-execution-canary-{now}-{}", account.account_id),
        leader_id: leader.leader_id.clone(),
        leader_address: leader.leader_address.clone(),
        coin: options.coin.clone(),
        side: options.side,
        price: options.leader_notional_usd / options.leader_size,
        size: options.leader_size,
        notional_usd: options.leader_notional_usd,
        reduce_only: false,
        exchange_time_ms: now,
        received_at_ms: now,
    };
    let after = copy_shadow_position_event(
        &watch,
        &options.coin,
        signed_size,
        options.leader_notional_usd,
        now + 2,
        config.hyperliquid.dex.as_str(),
    );

    let mut records = Vec::new();
    records.extend(pipeline.handle_watcher_event(before, now));
    records.extend(pipeline.handle_watcher_event(
        strategies::smart_money::CopyLeaderWatcherEvent::Fill {
            leader_id: leader.leader_id.clone(),
            leader_address: leader.leader_address.clone(),
            fill,
            is_snapshot: false,
        },
        now + 1,
    ));
    records.extend(pipeline.handle_watcher_event(after, now + 2));
    records
}

async fn execute_copy_canary_records(
    config: &config::AppConfig,
    records: &[strategies::smart_money::CopyDryRunShadowRecord],
    execution_dry_run: bool,
    live: bool,
) -> Result<Vec<domain::WorkerReport>> {
    let mut reports = Vec::new();
    for record in records {
        if !matches!(
            record.risk_decision,
            strategies::smart_money::CopySignalRiskDecision::Approved { .. }
        ) {
            continue;
        }
        let Some(signal) = record.signal.as_ref() else {
            continue;
        };
        for account_id in &signal.target_accounts {
            let account = config
                .account(account_id)
                .with_context(|| format!("account {account_id} not found"))?;
            let worker_id = format!("worker-{}", account.account_id);
            let intent =
                signal.to_trade_intent(&account.account_id, &worker_id, account.copy_ratio);
            let risk_context = risk::RiskContext::from_account_for_module(
                config,
                account,
                execution_dry_run,
                "copy",
            );
            match risk::RiskGateway::dry_run_default().evaluate(&risk_context, intent) {
                risk::RiskDecision::Approved(order) => {
                    let executor = if live {
                        let vault_password = std::env::var("TRADE_XYZ_VAULT_PASSWORD").ok();
                        let secret = secrets::load_account_secret(
                            config,
                            account,
                            vault_password.as_deref(),
                        )?;
                        trading::AccountExecutor::live(config.clone(), account.clone(), secret)
                    } else {
                        trading::AccountExecutor::dry_run(true)
                    };
                    reports.push(executor.submit(order).await);
                }
                risk::RiskDecision::Rejected(rejection) => {
                    reports.push(domain::WorkerReport::Rejected(rejection));
                }
            }
        }
    }
    Ok(reports)
}

fn plan_copy_canary_orders(
    config: &config::AppConfig,
    records: &[strategies::smart_money::CopyDryRunShadowRecord],
    execution_dry_run: bool,
) -> Result<Vec<CopyExecutionCanaryWouldSubmit>> {
    let mut plans = Vec::new();
    for record in records {
        if !matches!(
            record.risk_decision,
            strategies::smart_money::CopySignalRiskDecision::Approved { .. }
        ) {
            continue;
        }
        let Some(signal) = record.signal.as_ref() else {
            continue;
        };
        for account_id in &signal.target_accounts {
            let account = config
                .account(account_id)
                .with_context(|| format!("account {account_id} not found"))?;
            let worker_id = format!("worker-{}", account.account_id);
            let intent =
                signal.to_trade_intent(&account.account_id, &worker_id, account.copy_ratio);
            let risk_context = risk::RiskContext::from_account_for_module(
                config,
                account,
                execution_dry_run,
                "copy",
            );
            if let risk::RiskDecision::Approved(order) =
                risk::RiskGateway::dry_run_default().evaluate(&risk_context, intent)
            {
                plans.push(CopyExecutionCanaryWouldSubmit {
                    account_id: order.account_id,
                    worker_id: order.worker_id,
                    coin: order.coin,
                    side: order.side,
                    notional_usd: order.notional_usd,
                    reduce_only: order.reduce_only,
                    cloid: order.cloid,
                });
            }
        }
    }
    Ok(plans)
}

#[allow(clippy::too_many_arguments)]
fn copy_execution_canary_report(
    config: &config::AppConfig,
    options: &CopyExecutionCanaryOptions,
    execution_dry_run: bool,
    target_accounts: Vec<String>,
    leader: Option<CopyShadowSmokeLeader>,
    mut checks: Vec<CopyShadowSmokeCheck>,
    records: Vec<strategies::smart_money::CopyDryRunShadowRecord>,
    would_submit_orders: Vec<CopyExecutionCanaryWouldSubmit>,
    submitted_reports: Vec<domain::WorkerReport>,
    order_evidence: Vec<CopyExecutionCanaryOrderEvidence>,
    cleanup_runbooks: Vec<trading::SignedRunbookResult>,
    cleanup_errors: Vec<String>,
) -> CopyExecutionCanaryReport {
    let (ledger_reconciliations, ledger_reconciliation_snapshot) =
        reconcile_copy_canary_ledger(None, &records, &submitted_reports, &order_evidence);
    let approved_shadow_records = records
        .iter()
        .filter(|record| {
            record.signal.is_some()
                && matches!(
                    record.risk_decision,
                    strategies::smart_money::CopySignalRiskDecision::Approved { .. }
                )
        })
        .count();
    let execution_reports_ok = submitted_reports.iter().all(|report| {
        matches!(
            report,
            domain::WorkerReport::Submitted(_) | domain::WorkerReport::Ack(_)
        )
    });
    let live_submitted_count = copy_canary_live_submitted_reports(&submitted_reports).len();
    let order_evidence_ok = options.preflight_only
        || !options.live
        || live_submitted_count == 0
        || (order_evidence.len() == live_submitted_count
            && order_evidence
                .iter()
                .all(copy_execution_canary_order_evidence_ok));
    checks.push(copy_shadow_smoke_check(
        "order_status_evidence",
        order_evidence_ok,
        if options.preflight_only {
            "preflight-only canary has no live submitted orders to query".to_string()
        } else if !options.live {
            "dry-run canary does not query live orderStatus/userFills evidence".to_string()
        } else {
            format!(
                "{} live submitted report(s), {} order evidence record(s)",
                live_submitted_count,
                order_evidence.len()
            )
        },
    ));
    let ledger_reconciliation_ok = options.preflight_only
        || !options.live
        || live_submitted_count == 0
        || ledger_reconciliations.iter().all(|result| result.applied);
    checks.push(copy_shadow_smoke_check(
        "ledger_reconciliation",
        ledger_reconciliation_ok,
        if options.preflight_only {
            "preflight-only canary has no submitted reports to reconcile".to_string()
        } else if !options.live {
            format!(
                "{} dry-run submitted report(s) observed without mutating the copy ledger",
                ledger_reconciliations.len()
            )
        } else {
            format!(
                "{} live submitted report(s), {} ledger reconciliation result(s)",
                live_submitted_count,
                ledger_reconciliations.len()
            )
        },
    ));
    let cleanup_ok = if options.preflight_only {
        true
    } else if options.live {
        live_submitted_count > 0
            && cleanup_runbooks.len() == live_submitted_count
            && cleanup_errors.is_empty()
            && cleanup_runbooks
                .iter()
                .all(copy_execution_canary_cleanup_runbook_ok)
    } else {
        true
    };
    let execution_ok = if options.preflight_only {
        !would_submit_orders.is_empty() && submitted_reports.is_empty()
    } else {
        !submitted_reports.is_empty() && execution_reports_ok
    };
    let ok = checks.iter().all(|check| check.ok)
        && approved_shadow_records > 0
        && execution_ok
        && order_evidence_ok
        && ledger_reconciliation_ok
        && cleanup_ok;
    let checks_ok = checks.iter().all(|check| check.ok);
    let next_actions = if options.live && submitted_reports.is_empty() {
        if options.preflight_only && checks_ok && !would_submit_orders.is_empty() {
            vec![
                "Preflight-only live canary passed without loading secrets or submitting; review would_submit_orders before real one-account canary."
                    .to_string(),
            ]
        } else {
            vec![
                "No live order was submitted; fix failed checks before retrying live canary."
                    .to_string(),
            ]
        }
    } else if options.live && cleanup_ok {
        vec![
            "Live canary submitted and bundled cleanup runbook completed; inspect post-submit reconciliation before widening scope."
                .to_string(),
        ]
    } else if options.live {
        vec![
            "Live canary did not complete bundled cleanup; reconcile the account immediately and use reduce-only close if any position remains."
                .to_string(),
        ]
    } else {
        vec![
            "Review submitted_reports and shadow history, then rerun with --live true only for a one-account canary."
                .to_string(),
        ]
    };
    CopyExecutionCanaryReport {
        ok,
        mode: if execution_dry_run {
            "copy_execution_canary_dry_run".to_string()
        } else {
            "copy_execution_canary_live".to_string()
        },
        environment: config.app.environment.clone(),
        execution_dry_run,
        live_requested: options.live,
        live_submit_allowed: options.allow_live_submit,
        confirm_mainnet_live: options.confirm_mainnet_live,
        cleanup_after_submit: options.cleanup_after_submit,
        cleanup_max_slippage_bps: options.cleanup_max_slippage_bps,
        preflight_only: options.preflight_only,
        coin: options.coin.clone(),
        side: options.side,
        target_accounts,
        local_account_id: options.local_account_id.clone(),
        leader,
        checks,
        shadow_records_written: records.len(),
        approved_shadow_records,
        would_submit_orders,
        submitted_reports,
        order_evidence,
        ledger_reconciliations,
        ledger_reconciliation_snapshot,
        cleanup_runbooks,
        cleanup_errors,
        next_actions,
    }
}

fn copy_execution_canary_order_evidence_ok(evidence: &CopyExecutionCanaryOrderEvidence) -> bool {
    evidence.error.is_none()
        && evidence.order_status.is_some()
        && (evidence.matching_fill_count > 0
            || evidence
                .order_status
                .as_ref()
                .is_some_and(copy_execution_canary_order_status_is_filled))
}

fn copy_execution_canary_order_status_is_filled(status: &hyperliquid::OrderStatusResponse) -> bool {
    status
        .order
        .as_ref()
        .is_some_and(|info| info.status.eq_ignore_ascii_case("filled"))
}

fn reconcile_copy_canary_ledger(
    base_snapshot: Option<&strategies::smart_money::CopyPersistenceSnapshot>,
    records: &[strategies::smart_money::CopyDryRunShadowRecord],
    submitted_reports: &[domain::WorkerReport],
    order_evidence: &[CopyExecutionCanaryOrderEvidence],
) -> (
    Vec<strategies::smart_money::CopyLedgerReconcileResult>,
    strategies::smart_money::CopyPersistenceSnapshot,
) {
    let mut seen_event_keys = base_snapshot
        .map(|snapshot| snapshot.seen_event_keys.clone())
        .unwrap_or_default();
    let mut entries_by_key = base_snapshot
        .map(|snapshot| {
            snapshot
                .ledger_entries
                .iter()
                .cloned()
                .map(|entry| (copy_live_daemon_ledger_entry_identity(&entry), entry))
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default();
    for record in records {
        if let Some(entry) = record.ledger_entry.clone() {
            entries_by_key.insert(copy_live_daemon_ledger_entry_identity(&entry), entry);
        }
        seen_event_keys.extend(record.persistence_snapshot.seen_event_keys.iter().cloned());
    }
    seen_event_keys.sort();
    seen_event_keys.dedup();

    let mut ledger =
        strategies::smart_money::CopyLedger::from_entries(entries_by_key.into_values().collect());
    let mut reconciliations = Vec::new();
    for report in submitted_reports {
        if let domain::WorkerReport::Submitted(submitted) = report {
            reconciliations.push(ledger.apply_order_submission(submitted));
        }
    }
    for evidence in order_evidence {
        if let Some(order_status) = &evidence.order_status {
            reconciliations.push(ledger.apply_order_status_evidence(
                &evidence.account_id,
                &evidence.worker_id,
                order_status,
                &evidence.matching_fills,
            ));
        }
    }
    let snapshot = strategies::smart_money::CopyPersistenceSnapshot::new(
        domain::now_ms(),
        seen_event_keys,
        &ledger,
    );
    (reconciliations, snapshot)
}

fn copy_canary_live_submitted_reports(
    submitted_reports: &[domain::WorkerReport],
) -> Vec<&domain::OrderSubmitted> {
    submitted_reports
        .iter()
        .filter_map(|report| match report {
            domain::WorkerReport::Submitted(submitted) if !submitted.dry_run => Some(submitted),
            _ => None,
        })
        .collect()
}

fn copy_canary_has_live_submission(submitted_reports: &[domain::WorkerReport]) -> bool {
    !copy_canary_live_submitted_reports(submitted_reports).is_empty()
}

async fn collect_copy_canary_order_evidence(
    config: &config::AppConfig,
    submitted_reports: &[domain::WorkerReport],
) -> Vec<CopyExecutionCanaryOrderEvidence> {
    let live_submitted = copy_canary_live_submitted_reports(submitted_reports);
    if live_submitted.is_empty() {
        return Vec::new();
    }

    let account_ids = live_submitted
        .iter()
        .map(|submitted| submitted.account_id.clone())
        .collect::<HashSet<_>>();
    let mut fills_by_account = HashMap::<String, Vec<hyperliquid::UserFill>>::new();
    let mut fill_errors_by_account = HashMap::<String, String>::new();
    for account_id in account_ids {
        match config.account(&account_id) {
            Some(account) => match hyperliquid::fetch_user_fills(
                &config.app.environment,
                &config.hyperliquid.dex,
                &account.address,
            )
            .await
            {
                Ok(fills) => {
                    fills_by_account.insert(account_id, fills);
                }
                Err(error) => {
                    fill_errors_by_account
                        .insert(account_id, format!("userFills lookup failed: {error}"));
                }
            },
            None => {
                fill_errors_by_account.insert(
                    account_id.clone(),
                    format!("account {account_id} not found"),
                );
            }
        }
    }

    let mut evidence = Vec::new();
    for submitted in live_submitted {
        let mut errors = Vec::new();
        if let Some(error) = fill_errors_by_account.get(&submitted.account_id) {
            errors.push(error.clone());
        }

        let order_status = match query_copy_canary_order_status_evidence(config, submitted).await {
            Ok(status) => Some(status),
            Err(error) => {
                errors.push(error);
                None
            }
        };

        let status_oid = order_status
            .as_ref()
            .and_then(|status| status.order.as_ref())
            .map(|info| info.order.oid);
        let matching_oid = submitted.oid.or(status_oid);
        let mut account_fills = fills_by_account
            .get(&submitted.account_id)
            .cloned()
            .unwrap_or_default();
        let mut matching_fills =
            copy_canary_matching_fills(&account_fills, matching_oid, &submitted.coin);

        if matching_fills.is_empty()
            && let Some(account) = config.account(&submitted.account_id)
        {
            match fetch_copy_canary_user_fills_by_time(config, account, submitted, &order_status)
                .await
            {
                Ok(time_window_fills) => {
                    copy_canary_merge_user_fills(&mut account_fills, time_window_fills);
                    matching_fills =
                        copy_canary_matching_fills(&account_fills, matching_oid, &submitted.coin);
                }
                Err(error) => errors.push(error),
            }
        }

        evidence.push(CopyExecutionCanaryOrderEvidence {
            account_id: submitted.account_id.clone(),
            worker_id: submitted.worker_id.clone(),
            signal_id: submitted.signal_id.clone(),
            coin: submitted.coin.clone(),
            oid: matching_oid,
            cloid: submitted.cloid.clone(),
            order_status,
            user_fill_count: account_fills.len(),
            matching_fill_count: matching_fills.len(),
            matching_fills,
            error: (!errors.is_empty()).then(|| errors.join("; ")),
        });
    }
    evidence
}

async fn fetch_copy_canary_user_fills_by_time(
    config: &config::AppConfig,
    account: &config::AccountConfig,
    submitted: &domain::OrderSubmitted,
    order_status: &Option<hyperliquid::OrderStatusResponse>,
) -> Result<Vec<hyperliquid::UserFill>, String> {
    let (start_time_ms, end_time_ms) =
        copy_canary_fill_time_window(submitted.submitted_at_ms, order_status);
    hyperliquid::fetch_user_fills_by_time(
        &config.app.environment,
        &config.hyperliquid.dex,
        &account.address,
        start_time_ms,
        Some(end_time_ms),
    )
    .await
    .map_err(|error| {
        format!(
            "userFillsByTime lookup failed for {}..{}: {error}",
            start_time_ms, end_time_ms
        )
    })
}

fn copy_canary_fill_time_window(
    submitted_at_ms: u64,
    order_status: &Option<hyperliquid::OrderStatusResponse>,
) -> (u64, u64) {
    let order_time_ms = order_status
        .as_ref()
        .and_then(|status| status.order.as_ref())
        .map(|info| info.order.timestamp.min(info.status_timestamp))
        .unwrap_or(submitted_at_ms);
    let status_time_ms = order_status
        .as_ref()
        .and_then(|status| status.order.as_ref())
        .map(|info| info.order.timestamp.max(info.status_timestamp))
        .unwrap_or(submitted_at_ms);
    let start_time_ms = submitted_at_ms
        .min(order_time_ms)
        .saturating_sub(COPY_CANARY_FILL_LOOKBACK_MS);
    let end_time_ms = domain::now_ms()
        .max(submitted_at_ms)
        .max(status_time_ms)
        .saturating_add(COPY_CANARY_FILL_LOOKAHEAD_MS);
    (start_time_ms, end_time_ms)
}

fn copy_canary_matching_fills(
    fills: &[hyperliquid::UserFill],
    oid: Option<u64>,
    coin: &str,
) -> Vec<hyperliquid::UserFill> {
    oid.map(|oid| {
        fills
            .iter()
            .filter(|fill| fill.oid == oid && fill.coin == coin)
            .cloned()
            .collect::<Vec<_>>()
    })
    .unwrap_or_default()
}

fn copy_canary_merge_user_fills(
    target: &mut Vec<hyperliquid::UserFill>,
    fills: Vec<hyperliquid::UserFill>,
) {
    for fill in fills {
        let exists = target.iter().any(|existing| {
            existing.oid == fill.oid
                && existing.time == fill.time
                && existing.hash == fill.hash
                && existing.coin == fill.coin
                && existing.side == fill.side
        });
        if !exists {
            target.push(fill);
        }
    }
}

async fn query_copy_canary_order_status_evidence(
    config: &config::AppConfig,
    submitted: &domain::OrderSubmitted,
) -> Result<hyperliquid::OrderStatusResponse, String> {
    let mut last_error = None;
    for attempt in 0..COPY_CANARY_ORDER_EVIDENCE_RETRIES {
        let mut lookups = Vec::new();
        if let Some(oid) = submitted.oid {
            lookups.push(trading::OrderStatusLookup::Oid { oid });
        }
        if !submitted.cloid.trim().is_empty() {
            match trading::order_status_lookup(None, Some(submitted.cloid.clone())) {
                Ok(lookup) => lookups.push(lookup),
                Err(error) => {
                    last_error = Some(format!("orderStatus cloid lookup invalid: {error}"))
                }
            }
        }
        if lookups.is_empty() {
            return Err(
                "submitted report has neither oid nor cloid for orderStatus lookup".to_string(),
            );
        }

        for lookup in lookups {
            match trading::query_order_status(config, &submitted.account_id, lookup.clone()).await {
                Ok(report) => {
                    if report.order_status.order.is_some() {
                        return Ok(report.order_status);
                    }
                    last_error = Some(format!(
                        "orderStatus lookup returned {} without order for {:?}",
                        report.order_status.status, lookup
                    ));
                }
                Err(error) => {
                    last_error = Some(format!(
                        "orderStatus lookup failed for {:?}: {error}",
                        lookup
                    ));
                }
            }
        }

        if attempt + 1 < COPY_CANARY_ORDER_EVIDENCE_RETRIES {
            tokio::time::sleep(Duration::from_millis(
                COPY_CANARY_ORDER_EVIDENCE_RETRY_DELAY_MS,
            ))
            .await;
        }
    }
    Err(last_error.unwrap_or_else(|| "orderStatus lookup failed without detail".to_string()))
}

async fn execute_copy_canary_cleanup_runbooks(
    config: &config::AppConfig,
    options: &CopyExecutionCanaryOptions,
    submitted_reports: &[domain::WorkerReport],
) -> (Vec<trading::SignedRunbookResult>, Vec<String>) {
    let live_submitted = copy_canary_live_submitted_reports(submitted_reports);
    if live_submitted.is_empty() {
        return (Vec::new(), Vec::new());
    }
    let vault_password = std::env::var("TRADE_XYZ_VAULT_PASSWORD").ok();
    let mut cleanup_runbooks = Vec::new();
    let mut cleanup_errors = Vec::new();
    for submitted in live_submitted {
        let cleanup_options = trading::SignedRunbookOptions {
            account_id: submitted.account_id.clone(),
            coin: submitted.coin.clone(),
            side: opposite_order_side(submitted.side),
            notional_usd: submitted
                .notional_usd
                .max(trading::HYPERLIQUID_MIN_ORDER_NOTIONAL_USD),
            max_slippage_bps: options.cleanup_max_slippage_bps,
            execution_mode: domain::ExecutionMode::Taker,
            reduce_only: true,
            close_full_position: true,
            submit: true,
            cancel_resting: true,
            confirm_mainnet_live: options.confirm_mainnet_live,
        };
        match trading::execute_signed_runbook(
            config.clone(),
            cleanup_options,
            vault_password.as_deref(),
        )
        .await
        {
            Ok(runbook) => cleanup_runbooks.push(runbook),
            Err(error) => cleanup_errors.push(format!(
                "cleanup failed for account={} coin={} side={:?}: {error:#}",
                submitted.account_id,
                submitted.coin,
                opposite_order_side(submitted.side)
            )),
        }
    }
    (cleanup_runbooks, cleanup_errors)
}

fn opposite_order_side(side: domain::OrderSide) -> domain::OrderSide {
    match side {
        domain::OrderSide::Buy => domain::OrderSide::Sell,
        domain::OrderSide::Sell => domain::OrderSide::Buy,
    }
}

fn copy_execution_canary_cleanup_runbook_ok(runbook: &trading::SignedRunbookResult) -> bool {
    runbook.submitted && runbook.post_submit_reconciliation.is_some()
}

struct CopyShadowWatchReportBase {
    environment: String,
    ws_url: Option<String>,
    local_account_id: Option<String>,
    target_accounts: Vec<String>,
    leaders: Vec<CopyShadowSmokeLeader>,
    watcher_subscriptions: Vec<Value>,
    checks: Vec<CopyShadowSmokeCheck>,
    watcher_status: String,
}

struct CopyShadowWatchReportInput {
    environment: String,
    ws_url: Option<String>,
    local_account_id: Option<String>,
    target_accounts: Vec<String>,
    leaders: Vec<CopyShadowSmokeLeader>,
    watcher_subscriptions: Vec<Value>,
    checks: Vec<CopyShadowSmokeCheck>,
    events_received: usize,
    fill_events: usize,
    snapshot_fill_events: usize,
    position_snapshot_events: usize,
    position_snapshots: usize,
    order_update_events: usize,
    shadow_records_written: usize,
    elapsed_ms: u64,
    watcher_status: String,
}

impl CopyShadowWatchReportInput {
    fn new(base: CopyShadowWatchReportBase) -> Self {
        Self {
            environment: base.environment,
            ws_url: base.ws_url,
            local_account_id: base.local_account_id,
            target_accounts: base.target_accounts,
            leaders: base.leaders,
            watcher_subscriptions: base.watcher_subscriptions,
            checks: base.checks,
            events_received: 0,
            fill_events: 0,
            snapshot_fill_events: 0,
            position_snapshot_events: 0,
            position_snapshots: 0,
            order_update_events: 0,
            shadow_records_written: 0,
            elapsed_ms: 0,
            watcher_status: base.watcher_status,
        }
    }
}

fn count_copy_shadow_watch_event(
    input: &mut CopyShadowWatchReportInput,
    event: &strategies::smart_money::CopyLeaderWatcherEvent,
) {
    match event {
        strategies::smart_money::CopyLeaderWatcherEvent::Fill { is_snapshot, .. } => {
            input.fill_events += 1;
            if *is_snapshot {
                input.snapshot_fill_events += 1;
            }
        }
        strategies::smart_money::CopyLeaderWatcherEvent::PositionSnapshots {
            snapshots, ..
        } => {
            input.position_snapshot_events += 1;
            input.position_snapshots += snapshots.len();
        }
        strategies::smart_money::CopyLeaderWatcherEvent::OrderUpdate { .. } => {
            input.order_update_events += 1;
        }
    }
}

fn copy_shadow_watch_report(
    config: &config::AppConfig,
    options: CopyShadowWatchOptions,
    input: CopyShadowWatchReportInput,
) -> Result<CopyShadowWatchReport> {
    let recent_shadow_entries = strategies::smart_money::read_recent_copy_shadow_history_entries(
        &options.shadow_history_path,
        20,
    )?
    .len();
    let mut findings = Vec::new();
    if input.events_received == 0 {
        findings.push("no watcher events received during this bounded window".to_string());
    }
    if input.fill_events > 0 && input.fill_events == input.snapshot_fill_events {
        findings.push(
            "only snapshot fills were observed; snapshot fills are recorded as context and do not trigger copy signals"
                .to_string(),
        );
    }
    if input.position_snapshot_events == 0 {
        findings.push(
            "no position snapshots were parsed; semantic open/close classification requires fresh position snapshots"
                .to_string(),
        );
    }
    if input.shadow_records_written == 0 {
        findings.push(
            "no copy shadow records written; leader fills may need matching before/after position snapshots"
                .to_string(),
        );
    }
    if options.environment.is_none()
        && config.app.environment.eq_ignore_ascii_case("testnet")
        && options.ws_url.is_none()
    {
        findings.push(
            "config uses testnet; pass --environment mainnet for mainnet leader addresses"
                .to_string(),
        );
    }
    Ok(CopyShadowWatchReport {
        ok: input.checks.iter().all(|check| check.ok),
        mode: "read_only_live_ws_dry_run_shadow".to_string(),
        environment: input.environment,
        ws_url: input.ws_url,
        process_dry_run: config.app.dry_run,
        local_account_id: input.local_account_id,
        target_accounts: input.target_accounts,
        leaders: input.leaders,
        watcher_subscriptions: input.watcher_subscriptions,
        checks: input.checks,
        shadow_history_path: options.shadow_history_path.display().to_string(),
        duration_secs: options.duration_secs,
        elapsed_ms: input.elapsed_ms,
        max_events: options.max_events,
        events_received: input.events_received,
        fill_events: input.fill_events,
        snapshot_fill_events: input.snapshot_fill_events,
        position_snapshot_events: input.position_snapshot_events,
        position_snapshots: input.position_snapshots,
        order_update_events: input.order_update_events,
        shadow_records_written: input.shadow_records_written,
        recent_shadow_entries,
        watcher_status: input.watcher_status,
        findings,
    })
}

fn run_synthetic_copy_shadow_event(
    config: &config::AppConfig,
    options: &CopyShadowSmokeOptions,
    account: &config::AccountConfig,
    leader: &strategies::smart_money::SmartMoneyLeaderWatch,
    target_accounts: &[String],
) -> Result<usize> {
    let now = domain::now_ms();
    let strategy = strategies::smart_money::SmartMoneyCopyStrategy::new(
        strategies::smart_money::SmartMoneyCopyConfig {
            strategy_id: "copy_shadow_smoke".to_string(),
            default_copy_ratio: 1.0,
            max_slippage_bps: 25.0,
            leaders: vec![strategies::smart_money::LeaderRule {
                leader_id: leader.leader_id.clone(),
                leader_address: leader.leader_address.clone(),
                enabled: true,
                copy_ratio: 1.0,
            }],
            symbol_limits: vec![strategies::smart_money::SymbolCopyLimit {
                coin: options.coin.clone(),
                max_signal_notional_usd: options.leader_notional_usd,
            }],
        },
    );
    let mut pipeline = strategies::smart_money::CopyDryRunShadowPipeline::new(
        strategies::smart_money::CopyDryRunShadowConfig {
            local_account_id: account.account_id.clone(),
            target_accounts: target_accounts.to_vec(),
            signal_ttl_ms: config.process.signal_ttl_ms,
            max_signal_delay_ms: config.process.signal_ttl_ms.max(1),
            account_copy_ratio: account.copy_ratio,
            principal_cap_usd: account.max_order_notional_usd
                / strategies::smart_money::COPY_MAX_LEVERAGE.max(1.0),
            leverage: strategies::smart_money::COPY_MAX_LEVERAGE,
            max_signal_notional_usd: Some(account.max_order_notional_usd),
            exchange_min_open_notional_usd: trading::HYPERLIQUID_MIN_ORDER_NOTIONAL_USD,
            allow_short: true,
            max_effective_exposure_usd: Some(account.max_order_notional_usd),
            blocked_symbols: config.module_blocked_symbols("copy").to_vec(),
            live_gate: strategies::smart_money::CopyLiveGateInput {
                process_dry_run: true,
                live_copy_enabled: false,
                account_worker_live: false,
            },
        },
        strategy,
        strategies::smart_money::CopyLedger::new(),
    );

    let before = copy_shadow_position_event(
        leader,
        &options.coin,
        0.0,
        0.0,
        now,
        config.hyperliquid.dex.as_str(),
    );
    let fill = strategy::LeaderFillEvent {
        event_id: format!("copy-shadow-smoke-{now}"),
        leader_id: leader.leader_id.clone(),
        leader_address: leader.leader_address.clone(),
        coin: options.coin.clone(),
        side: domain::OrderSide::Buy,
        price: options.leader_notional_usd / options.leader_size,
        size: options.leader_size,
        notional_usd: options.leader_notional_usd,
        reduce_only: false,
        exchange_time_ms: now,
        received_at_ms: now,
    };
    let after = copy_shadow_position_event(
        leader,
        &options.coin,
        options.leader_size,
        options.leader_notional_usd,
        now + 2,
        config.hyperliquid.dex.as_str(),
    );

    let mut records = Vec::new();
    records.extend(pipeline.handle_watcher_event(before, now));
    records.extend(pipeline.handle_watcher_event(
        strategies::smart_money::CopyLeaderWatcherEvent::Fill {
            leader_id: leader.leader_id.clone(),
            leader_address: leader.leader_address.clone(),
            fill,
            is_snapshot: false,
        },
        now + 1,
    ));
    records.extend(pipeline.handle_watcher_event(after, now + 2));
    strategies::smart_money::append_copy_shadow_history_records(
        &options.shadow_history_path,
        &records,
        now + 3,
    )?;
    Ok(records.len())
}

fn copy_shadow_position_event(
    leader: &strategies::smart_money::SmartMoneyLeaderWatch,
    coin: &str,
    signed_size: f64,
    position_notional_usd: f64,
    now: u64,
    dex: &str,
) -> strategies::smart_money::CopyLeaderWatcherEvent {
    strategies::smart_money::CopyLeaderWatcherEvent::PositionSnapshots {
        leader_id: leader.leader_id.clone(),
        leader_address: leader.leader_address.clone(),
        dex: Some(dex.to_string()),
        snapshots: vec![strategies::smart_money::LeaderPositionSnapshot {
            leader_id: leader.leader_id.clone(),
            market: None,
            dex: Some(dex.to_string()),
            coin: coin.to_string(),
            signed_size,
            position_notional_usd,
            snapshot_time_ms: now,
            received_at_ms: now,
        }],
    }
}

fn parse_copy_shadow_smoke_leaders(raw: &[String]) -> Result<Vec<CopyShadowSmokeLeader>> {
    let mut leaders = Vec::new();
    for (index, item) in raw.iter().enumerate() {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            continue;
        }
        let (leader_id, leader_address) = if let Some((id, address)) = trimmed.split_once('=') {
            (id.trim(), address.trim())
        } else if let Some((id, address)) = trimmed.split_once(':') {
            (id.trim(), address.trim())
        } else {
            (trimmed, trimmed)
        };
        anyhow::ensure!(
            !leader_id.is_empty() && !leader_address.is_empty(),
            "invalid --leader {}; use leader_id=0xAddress",
            item
        );
        let leader_id = if leader_id.eq_ignore_ascii_case(leader_address) {
            format!("leader_{}", index + 1)
        } else {
            leader_id.to_string()
        };
        leaders.push(CopyShadowSmokeLeader {
            leader_id,
            leader_address: leader_address.to_string(),
        });
    }
    Ok(leaders)
}

fn copy_shadow_smoke_check(
    name: &str,
    ok: bool,
    detail: impl Into<String>,
) -> CopyShadowSmokeCheck {
    CopyShadowSmokeCheck {
        name: name.to_string(),
        ok,
        detail: detail.into(),
    }
}

fn copy_shadow_smoke_next_commands(
    options: &CopyShadowSmokeOptions,
    leaders: &[CopyShadowSmokeLeader],
) -> Vec<String> {
    let leader_args = leaders
        .iter()
        .map(|leader| format!("--leader {}={}", leader.leader_id, leader.leader_address))
        .collect::<Vec<_>>()
        .join(" ");
    let leader_segment = if leader_args.is_empty() {
        "--leader leader_a=0xLEADER_ADDRESS".to_string()
    } else {
        leader_args
    };
    vec![format!(
        "cargo run --manifest-path V2\\Cargo.toml -- copy-shadow-smoke --config {} {} --coin {} --shadow-history {} --synthetic-event true",
        DEFAULT_CONFIG_PATH,
        leader_segment,
        options.coin,
        options.shadow_history_path.display()
    )]
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
