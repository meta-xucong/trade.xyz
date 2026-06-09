use std::{
    collections::{HashMap, HashSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderValue, header},
    response::{Html, IntoResponse},
    routing::{any, get, post},
};
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use zeroize::Zeroize;

use crate::{
    audit::{AuditEvent, append_audit_event, read_recent_audit_events},
    config::{
        AccountConfig, AppConfig, MARKET_HL_PERP, MARKET_SPOT, MARKET_XYZ_PERP, load_config,
        normalize_market_id, save_config, supported_market_ids,
    },
    domain::{
        CoordinatorSignal, ExecutionMode, OrderSide, SignalOrder, SignalSource, WorkerReport,
        now_ms,
    },
    hyperliquid::{
        ClearinghouseState, OpenOrder, OrderPlan, OrderStatusResponse, SpotClearinghouseState,
        SpotMarketSnapshot, UserFill, UserRateLimit, build_spot_order_plan, fetch_candle_snapshot,
        fetch_clearinghouse_state, fetch_default_clearinghouse_state, fetch_open_orders,
        fetch_order_status_by_cloid, fetch_order_status_by_oid, fetch_perp_all_mids_cached,
        fetch_spot_clearinghouse_state, fetch_spot_market_snapshot_cached, fetch_user_fills,
        fetch_user_rate_limit, fetch_ws_candle_probe, fetch_xyz_market_snapshot_cached,
        normalize_cloid_for_info, normalize_dex_coin, normalize_spot_coin, round_size_down,
        round_spot_price,
    },
    manual_ops::{ManualOrderRequest, manual_order_to_signal},
    realtime::{RealtimeState, spawn_realtime_runtime},
    risk::{RiskContext, RiskDecision, RiskGateway},
    secrets::{
        SecretUpsert, VaultSummary, account_secret_id, change_vault_password, load_account_secret,
        load_secret_by_id, load_transfer_secret, transfer_secret_id, unlock_vault, upsert_secret,
        vault_status,
    },
    strategies::{
        fib::{
            FibBasicConfig, FibBasicLevelPlan, FibBasicPlan, FibInstanceRecord, FibInstanceStatus,
            FibOrderRef, FibProfitLossMode, FibRetracementConfig, FibRetracementStrategy,
            FibTradeDirection, build_basic_plan, fib_entry_side, fib_stop_loss_price,
            fib_take_profit_price, normalized_level_set,
        },
        smart_money::{LeaderRule, SmartMoneyCopyConfig, SmartMoneyCopyStrategy, SymbolCopyLimit},
    },
    strategy::{LeaderFillEvent, Strategy, StrategyContext, StrategyEvent},
    trading::{
        AccountExecutor, AccountReadinessState, CancelByCloidResult, CancelOpenOrderResult,
        FastProtectiveExitArmResult, FastSignedOrderResult, HYPERLIQUID_MIN_ORDER_NOTIONAL_USD,
        MainnetSmokePlanOptions, MainnetSmokePlanResult, ManualLeverageUpdateOptions,
        ManualLeverageUpdateResult, ProtectiveExitArmOptions, ProtectiveExitArmResult,
        ProtectiveExitOptions, ProtectiveExitPlanResult, ProtectiveExitSubmitOptions,
        ProtectiveExitSubmitResult, ProtectiveExitTriggerCheckOptions,
        ProtectiveExitTriggerCheckResult, SignedAcceptanceOptions, SignedAcceptanceResult,
        SignedRunbookOptions, SignedRunbookResult, SignedSmokeOptions, SignedSmokeResult,
        UsdcDexTransferOptions, UsdcDexTransferResult, UsdcDexTransferRunbookResult,
        account_has_opening_collateral, build_mainnet_smoke_plan, build_protective_exit_plan,
        build_signed_order_plan, check_protective_exit_trigger,
        effective_exchange_min_order_notional_ok, effective_order_notional_usd,
        ensure_live_account_address, execute_cancel_by_cloid, execute_cancel_open_order,
        execute_fast_protective_exit_arm, execute_fast_signed_order,
        execute_manual_leverage_update, execute_protective_exit_arm,
        execute_protective_exit_submit, execute_signed_acceptance, execute_signed_runbook,
        execute_signed_smoke, execute_usdc_dex_transfer, execute_usdc_dex_transfer_runbook,
        reduce_only_position_available, reduce_only_position_detail,
        signed_close_exempt_from_opening_rules, summarize_account_readiness_state,
        tif_for_execution_mode, validate_exchange_min_order_notional, validate_live_action_gates,
        validate_live_order_gates, validate_usdc_dex_transfer_gates,
    },
};
use serde_json::{Value, json};

const INDEX_HTML: &str = include_str!("../frontend/index.html");
const RECENT_AUDIT_SCAN_LIMIT: usize = 50_000;
const RECENT_TRADE_EVENTS_PER_MARKET: usize = 12;
const MARKET_SNAPSHOT_QUOTE_CACHE_TTL_MS: u64 = 15_000;
const FIB_ENTRY_SIGNAL_EXECUTION_TTL_MS: u64 = 300_000;
const MARKET_SNAPSHOT_UNIVERSE_CACHE_TTL_MS: u64 = 60_000;
const ACCOUNT_FUNDING_BATCH_CACHE_TTL_MS: u64 = 5_000;
const MANUAL_PROTECTIVE_RULES_CACHE_TTL_MS: u64 = 10_000;
const FIB_RECONCILE_INTERVAL_MS: u64 = 15_000;
const FIB_COMPLETION_RESIDUAL_POSITION_DUST_USD: f64 = 0.50;
const FIB_COMPLETION_RESIDUAL_POSITION_EPSILON: f64 = 0.000_000_01;
const FIB_CLEANUP_POSITION_EPSILON: f64 = 0.000_000_000_001;
const DISPLAY_USD_EPSILON: f64 = 0.005;
const FIB_INSTANCES_PATH: &str = "logs/fib_instances.json";
const FIB_INSTANCE_HISTORY_PATH: &str = "logs/fib_instance_history.jsonl";
const FIB_HISTORY_RESPONSE_LIMIT: usize = 80;
const FIB_HISTORY_RECOVERY_SCAN_LIMIT: usize = 100_000;

#[derive(Debug)]
pub struct FrontendOptions {
    pub config_path: PathBuf,
    pub bind_addr: String,
    pub dry_run: bool,
}

#[derive(Clone)]
struct FrontendAppState {
    config: Arc<RwLock<AppConfig>>,
    config_path: PathBuf,
    dry_run: bool,
    started_at_ms: u64,
    vault_session: Arc<RwLock<Option<UnlockedVaultSession>>>,
    fib_instances: Arc<RwLock<HashMap<String, FibInstanceRecord>>>,
    fib_stop_requests: Arc<RwLock<HashMap<String, u64>>>,
    realtime: RealtimeState,
    account_funding_batch_cache: Arc<RwLock<HashMap<String, AccountFundingBatchCacheEntry>>>,
    manual_protective_rules_cache: Arc<RwLock<HashMap<String, ManualProtectiveRulesCacheEntry>>>,
}

#[derive(Debug)]
struct UnlockedVaultSession {
    path: PathBuf,
    password: String,
    summary: VaultSummary,
}

#[derive(Debug, Clone)]
struct AccountFundingBatchCacheEntry {
    fetched_at_ms: u64,
    response: AccountFundingBatchResponse,
}

#[derive(Debug, Clone)]
struct ManualProtectiveRulesCacheEntry {
    fetched_at_ms: u64,
    response: ManualProtectiveRulesResponse,
}

impl Drop for UnlockedVaultSession {
    fn drop(&mut self) {
        self.password.zeroize();
    }
}

impl FrontendAppState {
    fn config_snapshot(&self) -> Result<AppConfig> {
        self.config
            .read()
            .map_err(|_| anyhow::anyhow!("config lock is poisoned"))
            .map(|config| config.clone())
    }

    fn record_fib_stop_request(&self, strategy_id: &str) -> Result<u64> {
        let requested_at_ms = now_ms();
        self.fib_stop_requests
            .write()
            .map_err(|_| anyhow::anyhow!("fib stop request lock is poisoned"))?
            .insert(strategy_id.to_string(), requested_at_ms);
        Ok(requested_at_ms)
    }

    fn clear_fib_stop_request(&self, strategy_id: &str) -> Result<()> {
        self.fib_stop_requests
            .write()
            .map_err(|_| anyhow::anyhow!("fib stop request lock is poisoned"))?
            .remove(strategy_id);
        Ok(())
    }

    fn fib_stop_requested_after(&self, strategy_id: &str, snapshot_ms: u64) -> Result<bool> {
        let requests = self
            .fib_stop_requests
            .read()
            .map_err(|_| anyhow::anyhow!("fib stop request lock is poisoned"))?;
        Ok(requests
            .get(strategy_id)
            .is_some_and(|requested_at_ms| *requested_at_ms >= snapshot_ms))
    }

    fn cached_account_funding_batch(
        &self,
        key: &str,
    ) -> Result<Option<AccountFundingBatchResponse>> {
        let now = now_ms();
        let cache = self
            .account_funding_batch_cache
            .read()
            .map_err(|_| anyhow::anyhow!("account funding cache lock is poisoned"))?;
        Ok(cache.get(key).and_then(|entry| {
            (now.saturating_sub(entry.fetched_at_ms) <= ACCOUNT_FUNDING_BATCH_CACHE_TTL_MS)
                .then(|| entry.response.clone())
        }))
    }

    fn store_account_funding_batch_cache(
        &self,
        key: String,
        response: &AccountFundingBatchResponse,
    ) -> Result<()> {
        self.account_funding_batch_cache
            .write()
            .map_err(|_| anyhow::anyhow!("account funding cache lock is poisoned"))?
            .insert(
                key,
                AccountFundingBatchCacheEntry {
                    fetched_at_ms: now_ms(),
                    response: response.clone(),
                },
            );
        Ok(())
    }

    fn cached_manual_protective_rules(
        &self,
        key: &str,
    ) -> Result<Option<ManualProtectiveRulesResponse>> {
        let now = now_ms();
        let cache = self
            .manual_protective_rules_cache
            .read()
            .map_err(|_| anyhow::anyhow!("manual protective rules cache lock is poisoned"))?;
        Ok(cache.get(key).and_then(|entry| {
            (now.saturating_sub(entry.fetched_at_ms) <= MANUAL_PROTECTIVE_RULES_CACHE_TTL_MS)
                .then(|| entry.response.clone())
        }))
    }

    fn store_manual_protective_rules_cache(
        &self,
        key: String,
        response: &ManualProtectiveRulesResponse,
    ) -> Result<()> {
        self.manual_protective_rules_cache
            .write()
            .map_err(|_| anyhow::anyhow!("manual protective rules cache lock is poisoned"))?
            .insert(
                key,
                ManualProtectiveRulesCacheEntry {
                    fetched_at_ms: now_ms(),
                    response: response.clone(),
                },
            );
        Ok(())
    }

    fn upsert_config_account(&self, input: &SecretUpsert, secret_usage: SecretUsage) -> Result<()> {
        self.upsert_config_account_fields(
            &input.account_id,
            &input.address,
            &input.secret_id,
            secret_usage,
        )
    }

    fn upsert_config_account_fields(
        &self,
        account_id: &str,
        address: &str,
        secret_id: &str,
        secret_usage: SecretUsage,
    ) -> Result<()> {
        let mut next_config = self.config_snapshot()?;
        let changed = upsert_config_account_record(
            &mut next_config,
            account_id,
            address,
            secret_id,
            secret_usage,
        );
        if !changed {
            return Ok(());
        }
        save_config(&self.config_path, &next_config)?;
        let mut guard = self
            .config
            .write()
            .map_err(|_| anyhow::anyhow!("config lock is poisoned"))?;
        *guard = next_config;
        Ok(())
    }

    fn apply_manual_settings(
        &self,
        payload: ManualSettingsPayload,
    ) -> Result<ManualSettingsResponse> {
        let ManualSettingsPayload {
            max_manual_order_notional_usd,
            account_max_order_notional_usd,
            account_ids,
        } = payload;
        let mut next_config = self.config_snapshot()?;
        let mut changed = false;
        let mut updated_account_limits = Vec::new();

        if let Some(global_cap) = max_manual_order_notional_usd {
            anyhow::ensure!(
                global_cap.is_finite() && global_cap > 0.0,
                "max_manual_order_notional_usd must be positive"
            );
            if (next_config.manual_ops.max_manual_order_notional_usd - global_cap).abs()
                > f64::EPSILON
            {
                next_config.manual_ops.max_manual_order_notional_usd = global_cap;
                changed = true;
            }
        }

        if let Some(account_cap) = account_max_order_notional_usd {
            anyhow::ensure!(
                account_cap.is_finite() && account_cap > 0.0,
                "account_max_order_notional_usd must be positive"
            );
            let mut normalized_account_ids = Vec::new();
            for raw in account_ids {
                let account_id = raw.trim();
                if account_id.is_empty() {
                    continue;
                }
                if !normalized_account_ids
                    .iter()
                    .any(|existing: &String| existing == account_id)
                {
                    normalized_account_ids.push(account_id.to_string());
                }
            }
            anyhow::ensure!(
                !normalized_account_ids.is_empty(),
                "account_ids cannot be empty when account_max_order_notional_usd is provided"
            );
            for account_id in normalized_account_ids {
                let account = next_config
                    .accounts
                    .iter_mut()
                    .find(|account| account.account_id == account_id)
                    .with_context(|| format!("account {} not found in config", account_id))?;
                if (account.max_order_notional_usd - account_cap).abs() > f64::EPSILON {
                    account.max_order_notional_usd = account_cap;
                    changed = true;
                }
                updated_account_limits.push(ManualAccountLimitResponse {
                    account_id: account.account_id.clone(),
                    max_order_notional_usd: account.max_order_notional_usd,
                });
            }
        }

        anyhow::ensure!(
            max_manual_order_notional_usd.is_some() || account_max_order_notional_usd.is_some(),
            "manual settings payload must include at least one limit field"
        );

        if changed {
            save_config(&self.config_path, &next_config)?;
            let mut guard = self
                .config
                .write()
                .map_err(|_| anyhow::anyhow!("config lock is poisoned"))?;
            *guard = next_config.clone();
        }

        Ok(ManualSettingsResponse {
            max_manual_order_notional_usd: next_config.manual_ops.max_manual_order_notional_usd,
            updated_account_limits,
        })
    }

    fn apply_module_symbol_policy(
        &self,
        payload: ModuleSymbolPolicyPayload,
    ) -> Result<ModuleSymbolPolicyResponse> {
        let mut next_config = self.config_snapshot()?;
        let module = normalize_module_name(&payload.module)?;
        let mut normalized = payload
            .blocked_symbols
            .iter()
            .map(|coin| normalize_dex_coin(&next_config.hyperliquid.dex, coin))
            .filter(|coin| !coin.trim().is_empty())
            .collect::<Vec<_>>();
        normalized.sort_unstable();
        normalized.dedup();

        let changed = match module {
            "manual" => {
                let changed =
                    next_config.module_symbol_policies.manual_blocked_symbols != normalized;
                next_config.module_symbol_policies.manual_blocked_symbols = normalized.clone();
                // Keep legacy field aligned to avoid mixed behavior in older code paths.
                next_config.manual_ops.blocked_symbols = normalized.clone();
                changed
            }
            "fib" => {
                let changed = next_config.module_symbol_policies.fib_blocked_symbols != normalized;
                next_config.module_symbol_policies.fib_blocked_symbols = normalized.clone();
                changed
            }
            "copy" => {
                let changed = next_config.module_symbol_policies.copy_blocked_symbols != normalized;
                next_config.module_symbol_policies.copy_blocked_symbols = normalized.clone();
                changed
            }
            _ => false,
        };

        if changed {
            save_config(&self.config_path, &next_config)?;
            let mut guard = self
                .config
                .write()
                .map_err(|_| anyhow::anyhow!("config lock is poisoned"))?;
            *guard = next_config.clone();
        }

        Ok(ModuleSymbolPolicyResponse {
            module: module.to_string(),
            blocked_symbols: next_config.module_blocked_symbols(module).to_vec(),
            block_none: next_config.module_blocked_symbols(module).is_empty(),
        })
    }

    fn sync_vault_entries_into_config(&self, summary: &VaultSummary) -> Result<()> {
        if !summary.unlocked || summary.entries.is_empty() {
            return Ok(());
        }

        let mut next_config = self.config_snapshot()?;
        let mut changed = false;
        for entry in &summary.entries {
            let secret_usage = secret_usage_for_vault_entry(&next_config, entry);
            changed |= upsert_config_account_record(
                &mut next_config,
                &entry.account_id,
                &entry.address,
                &entry.secret_id,
                secret_usage,
            );
        }
        if !changed {
            return Ok(());
        }
        save_config(&self.config_path, &next_config)?;
        let mut guard = self
            .config
            .write()
            .map_err(|_| anyhow::anyhow!("config lock is poisoned"))?;
        *guard = next_config;
        Ok(())
    }

    fn store_vault_session(
        &self,
        path: PathBuf,
        password: String,
        summary: VaultSummary,
    ) -> Result<VaultSummary> {
        self.sync_vault_entries_into_config(&summary)?;
        let mut guard = self
            .vault_session
            .write()
            .map_err(|_| anyhow::anyhow!("vault session lock is poisoned"))?;
        *guard = Some(UnlockedVaultSession {
            path,
            password,
            summary: summary.clone(),
        });
        Ok(summary)
    }

    fn vault_summary(&self, path: &Path) -> Result<VaultSummary> {
        let guard = self
            .vault_session
            .read()
            .map_err(|_| anyhow::anyhow!("vault session lock is poisoned"))?;
        if let Some(session) = guard.as_ref()
            && session.path == path
            && path.exists()
        {
            let mut summary = session.summary.clone();
            summary.exists = true;
            summary.unlocked = true;
            return Ok(summary);
        }
        Ok(vault_status(path))
    }

    fn resolve_vault_password(&self, path: &Path, password: &str) -> Result<String> {
        if !password.is_empty() {
            return Ok(password.to_string());
        }

        let guard = self
            .vault_session
            .read()
            .map_err(|_| anyhow::anyhow!("vault session lock is poisoned"))?;
        let session = guard
            .as_ref()
            .filter(|session| session.path == path)
            .context("vault is locked; unlock before this action or enter password")?;
        Ok(session.password.clone())
    }

    fn unlocked_vault_password(&self, path: &Path) -> Result<Option<String>> {
        let guard = self
            .vault_session
            .read()
            .map_err(|_| anyhow::anyhow!("vault session lock is poisoned"))?;
        Ok(guard
            .as_ref()
            .filter(|session| session.path == path && path.exists())
            .map(|session| session.password.clone()))
    }

    fn write_audit_event(&self, event: AuditEvent) -> Result<()> {
        let config = self.config_snapshot()?;
        append_audit_event(Path::new(&config.storage.audit_log_path), &event)
    }

    fn audit_attempt(
        &self,
        action: &str,
        account_id: Option<String>,
        coin: Option<String>,
        details: Value,
    ) -> Result<()> {
        self.write_audit_event(AuditEvent::new(
            "frontend", action, true, account_id, coin, None, details,
        ))
    }

    fn audit_api_result<T>(
        &self,
        action: &str,
        account_id: Option<String>,
        coin: Option<String>,
        details: Value,
        response: &ApiResult<T>,
    ) {
        let event = AuditEvent::new(
            "frontend",
            action,
            response.ok,
            account_id,
            coin,
            response.error.clone(),
            details,
        );
        if let Err(error) = self.write_audit_event(event) {
            tracing::error!(%action, error = %error, "failed to append audit event");
        }
    }
}

fn upsert_config_account_record(
    next_config: &mut AppConfig,
    account_id: &str,
    address: &str,
    secret_id: &str,
    secret_usage: SecretUsage,
) -> bool {
    let account_id = account_id.trim();
    let address = address.trim();
    let secret_id = secret_id.trim();
    if account_id.is_empty() || address.is_empty() || secret_id.is_empty() {
        return false;
    }
    if let Some(account) = next_config
        .accounts
        .iter_mut()
        .find(|account| account.account_id == account_id)
    {
        let secret_changed = match secret_usage {
            SecretUsage::Trading => account.secret_id != secret_id,
            SecretUsage::Transfer => account.transfer_secret_id != secret_id,
        };
        let changed = account.address != address
            || secret_changed
            || !account.enabled
            || !account.worker_enabled;
        account.address = address.to_string();
        match secret_usage {
            SecretUsage::Trading => account.secret_id = secret_id.to_string(),
            SecretUsage::Transfer => account.transfer_secret_id = secret_id.to_string(),
        }
        account.enabled = true;
        account.worker_enabled = true;
        changed
    } else {
        let (trading_secret_id, transfer_secret_id) = match secret_usage {
            SecretUsage::Trading => (secret_id.to_string(), String::new()),
            SecretUsage::Transfer => (format!("{account_id}_api_wallet"), secret_id.to_string()),
        };
        next_config.accounts.push(AccountConfig {
            account_id: account_id.to_string(),
            address: address.to_string(),
            secret_id: trading_secret_id,
            api_wallet_env: String::new(),
            transfer_secret_id,
            transfer_wallet_env: String::new(),
            enabled: true,
            worker_enabled: true,
            copy_ratio: 0.10,
            max_order_notional_usd: 100.0,
            blocked_markets: Vec::new(),
        });
        true
    }
}

fn secret_usage_for_vault_entry(
    config: &AppConfig,
    entry: &crate::secrets::VaultEntrySummary,
) -> SecretUsage {
    if let Some(account) = config.account(&entry.account_id) {
        if !account.transfer_secret_id.trim().is_empty()
            && entry.secret_id == account.transfer_secret_id.trim()
        {
            return SecretUsage::Transfer;
        }
        if entry.secret_id == account_secret_id(account) {
            return SecretUsage::Trading;
        }
    }
    let lower = entry.secret_id.to_ascii_lowercase();
    if lower.contains("transfer") || lower.contains("evm") {
        SecretUsage::Transfer
    } else {
        SecretUsage::Trading
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SecretUsage {
    Trading,
    Transfer,
}

impl SecretUsage {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "trading" | "api" | "api_wallet" => Ok(Self::Trading),
            "transfer" | "evm" | "funding" => Ok(Self::Transfer),
            other => anyhow::bail!("unsupported secret usage {other}"),
        }
    }
}

pub async fn run(options: FrontendOptions) -> Result<()> {
    let config = load_config(&options.config_path)?;
    let persisted_fib_instances = match load_fib_instances_from_disk() {
        Ok(instances) => instances,
        Err(error) => {
            tracing::warn!(%error, path = FIB_INSTANCES_PATH, "failed to load persisted Fib instances");
            HashMap::new()
        }
    };
    let realtime = RealtimeState::new();
    let state = FrontendAppState {
        config: Arc::new(RwLock::new(config)),
        config_path: options.config_path.clone(),
        dry_run: options.dry_run,
        started_at_ms: now_ms(),
        vault_session: Arc::new(RwLock::new(None)),
        fib_instances: Arc::new(RwLock::new(persisted_fib_instances)),
        fib_stop_requests: Arc::new(RwLock::new(HashMap::new())),
        realtime: realtime.clone(),
        account_funding_batch_cache: Arc::new(RwLock::new(HashMap::new())),
        manual_protective_rules_cache: Arc::new(RwLock::new(HashMap::new())),
    };
    spawn_realtime_runtime(state.config_snapshot()?, realtime);
    tokio::spawn(fib_reconciliation_loop(state.clone()));
    let app = Router::new()
        .route("/", get(index))
        .route("/api/state", get(frontend_state))
        .route("/api/realtime-status", get(realtime_status))
        .route(
            "/api/manual-market-universe",
            get(manual_market_universe).post(manual_market_universe_with_payload),
        )
        .route(
            "/api/manual-market-capabilities",
            get(manual_market_capabilities),
        )
        .route("/api/manual-market-quote", post(manual_market_quote))
        .route("/ws/manual-quote", any(manual_quote_ws))
        .route("/api/manual-order", post(manual_order))
        .route("/api/manual-settings", post(manual_settings_update))
        .route(
            "/api/module-symbol-policy",
            post(module_symbol_policy_update),
        )
        .route("/api/manual-set-leverage", post(manual_set_leverage))
        .route("/api/fib-set-leverage", post(fib_set_leverage))
        .route("/api/signed-smoke", post(signed_smoke))
        .route("/api/signed-acceptance", post(signed_acceptance))
        .route("/api/signed-runbook", post(signed_runbook))
        .route("/api/fast-signed-runbook", post(fast_signed_runbook))
        .route("/api/manual-protective-exit", post(manual_protective_exit))
        .route(
            "/api/manual-protective-trigger",
            post(manual_protective_trigger),
        )
        .route(
            "/api/manual-protective-submit",
            post(manual_protective_submit),
        )
        .route("/api/manual-protective-arm", post(manual_protective_arm))
        .route(
            "/api/fast-manual-protective-arm",
            post(fast_manual_protective_arm),
        )
        .route(
            "/api/manual-protective-rules",
            post(manual_protective_rules),
        )
        .route("/api/live-readiness", post(live_readiness))
        .route("/api/live-readiness-batch", post(live_readiness_batch))
        .route("/api/account-funding", post(account_funding))
        .route("/api/account-funding-batch", post(account_funding_batch))
        .route("/api/usdc-dex-transfer", post(usdc_dex_transfer))
        .route(
            "/api/usdc-dex-transfer-readiness",
            post(usdc_dex_transfer_readiness),
        )
        .route(
            "/api/usdc-dex-transfer-runbook",
            post(usdc_dex_transfer_runbook),
        )
        .route(
            "/api/usdc-dex-transfer-batch",
            post(usdc_dex_transfer_batch),
        )
        .route(
            "/api/usdc-dex-transfer-readiness-batch",
            post(usdc_dex_transfer_readiness_batch),
        )
        .route("/api/mainnet-smoke-plan", post(mainnet_smoke_plan))
        .route("/api/reconcile-account", post(reconcile_account))
        .route("/api/order-status", post(order_status))
        .route("/api/cancel-by-cloid", post(cancel_by_cloid))
        .route("/api/dashboard-open-orders", get(dashboard_open_orders))
        .route("/ws/dashboard-open-orders", any(dashboard_open_orders_ws))
        .route(
            "/api/dashboard-open-orders/cancel",
            post(dashboard_open_orders_cancel),
        )
        .route("/api/fib/preview", post(fib_preview))
        .route("/api/fib/auto-detect", post(fib_auto_detect))
        .route("/api/fib/ws-candle-probe", post(fib_ws_candle_probe))
        .route(
            "/api/fib/instances",
            get(fib_instances).post(fib_instance_create),
        )
        .route("/api/fib/history", get(fib_history))
        .route("/api/fib/instances/start", post(fib_instance_start))
        .route(
            "/api/fib/instances/refresh-params",
            post(fib_instance_refresh_params),
        )
        .route("/api/fib/instances/cancel", post(fib_instance_cancel))
        .route("/api/fib/ai/proposals", post(fib_ai_proposals))
        .route("/api/smart-money/preview", post(smart_money_preview))
        .route("/api/vault/status", get(vault_status_route))
        .route("/api/vault/unlock", post(vault_unlock))
        .route("/api/vault/change-password", post(vault_change_password))
        .route("/api/vault/upsert", post(vault_upsert))
        .route("/api/vault/check-secret", post(vault_check_secret))
        .with_state(state);

    let listener = TcpListener::bind(&options.bind_addr)
        .await
        .with_context(|| format!("failed to bind frontend console at {}", options.bind_addr))?;
    println!("Frontend console listening on http://{}", options.bind_addr);
    axum::serve(listener, app)
        .await
        .context("frontend console server failed")
}

async fn index() -> impl IntoResponse {
    (
        [
            (
                header::CACHE_CONTROL,
                HeaderValue::from_static("no-store, no-cache, must-revalidate, max-age=0"),
            ),
            (header::PRAGMA, HeaderValue::from_static("no-cache")),
            (header::EXPIRES, HeaderValue::from_static("0")),
        ],
        Html(INDEX_HTML),
    )
}

async fn frontend_state(State(state): State<FrontendAppState>) -> Json<FrontendStateResponse> {
    Json(FrontendStateResponse::from_state(&state).await)
}

async fn realtime_status(
    State(state): State<FrontendAppState>,
) -> Json<ApiResult<crate::realtime::RealtimeStatus>> {
    Json(ApiResult {
        ok: true,
        data: Some(state.realtime.status()),
        error: None,
    })
}

async fn dashboard_open_orders(
    State(state): State<FrontendAppState>,
) -> Json<ApiResult<DashboardOpenOrdersResponse>> {
    Json(ApiResult::from_result(
        build_dashboard_open_orders_response(&state).await,
    ))
}

async fn dashboard_open_orders_ws(
    ws: WebSocketUpgrade,
    State(state): State<FrontendAppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| dashboard_open_orders_ws_session(state, socket))
}

async fn dashboard_open_orders_ws_session(state: FrontendAppState, mut socket: WebSocket) {
    let mut next_tick = tokio::time::Instant::now();

    loop {
        tokio::select! {
            incoming = socket.recv() => {
                let Some(message_result) = incoming else {
                    break;
                };
                match message_result {
                    Ok(Message::Ping(payload)) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Ok(Message::Close(_)) => break,
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
            _ = tokio::time::sleep_until(next_tick) => {
                next_tick = tokio::time::Instant::now()
                    + std::time::Duration::from_millis(1_000);
                let response = ApiResult::from_result(
                    build_dashboard_open_orders_response(&state).await,
                );
                if send_ws_json(&mut socket, &response).await.is_err() {
                    break;
                }
            }
        }
    }
}

async fn dashboard_open_orders_cancel(
    State(state): State<FrontendAppState>,
    Json(payload): Json<DashboardOpenOrdersCancelPayload>,
) -> Json<ApiResult<DashboardOpenOrdersCancelResponse>> {
    let audit_details = json!({
        "market": payload.market.clone().unwrap_or_else(|| MARKET_XYZ_PERP.to_string()),
        "dry_run": payload.dry_run,
        "live": payload.live,
        "confirm_mainnet_live": payload.confirm_mainnet_live,
    });
    let result = cancel_dashboard_open_orders(&state, payload).await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "dashboard_open_orders_cancel",
        None,
        None,
        audit_details,
        &response,
    );
    Json(response)
}

async fn manual_order(
    State(state): State<FrontendAppState>,
    Json(payload): Json<ManualOrderPayload>,
) -> Json<ApiResult<ManualOrderResponse>> {
    let source_module = payload
        .source_module()
        .map(ToString::to_string)
        .unwrap_or_else(|_| "manual".to_string());
    let audit_details = json!({
        "target_accounts": payload.target_accounts.clone(),
        "source_module": source_module,
        "market": payload.market.clone().unwrap_or_else(|| MARKET_XYZ_PERP.to_string()),
        "side": payload.side.clone(),
        "notional_usd": payload.notional_usd,
        "reduce_only": payload.reduce_only,
        "execution_mode": payload.execution_mode.clone(),
        "max_slippage_bps": payload.max_slippage_bps,
        "dry_run": state.dry_run,
    });
    let audit_account_id = audit_details["target_accounts"]
        .as_array()
        .and_then(|accounts| accounts.first())
        .and_then(|account| account.as_str())
        .map(ToString::to_string);
    let audit_coin = Some(payload.coin.clone());
    let result = state.config_snapshot().and_then(|config| {
        let module = payload.source_module()?;
        let market = payload.market_profile(&config)?;
        let scoped = scoped_config_for_module_and_market(config, module, &market);
        let mut request = payload.try_into_request(state.dry_run)?;
        request.coin = if market.is_spot() {
            normalize_spot_coin(&request.coin)
        } else {
            normalize_dex_coin(&market.dex, &request.coin)
        };
        ensure_accounts_allowed_for_market(&scoped, &request.target_accounts, market.id)?;
        manual_order_to_signal(&scoped, request).map(|signal| ManualOrderResponse {
            accepted: true,
            signal_id: signal.signal_id,
            target_accounts: signal.target_accounts,
            coin: signal.order.coin,
            side: format!("{:?}", signal.order.side),
            notional_usd: signal.order.notional_usd,
            reduce_only: signal.order.reduce_only,
            dry_run: state.dry_run,
        })
    });
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "manual_order",
        audit_account_id,
        audit_coin,
        audit_details,
        &response,
    );
    Json(response)
}

async fn manual_settings_update(
    State(state): State<FrontendAppState>,
    Json(payload): Json<ManualSettingsPayload>,
) -> Json<ApiResult<ManualSettingsResponse>> {
    let audit_details = json!({
        "max_manual_order_notional_usd": payload.max_manual_order_notional_usd,
        "account_max_order_notional_usd": payload.account_max_order_notional_usd,
        "account_ids": payload.account_ids,
    });
    let result = state.apply_manual_settings(payload);
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "manual_settings_update",
        None,
        None,
        audit_details,
        &response,
    );
    Json(response)
}

async fn module_symbol_policy_update(
    State(state): State<FrontendAppState>,
    Json(payload): Json<ModuleSymbolPolicyPayload>,
) -> Json<ApiResult<ModuleSymbolPolicyResponse>> {
    let audit_details = json!({
        "module": payload.module,
        "blocked_symbols": payload.blocked_symbols,
    });
    let result = state.apply_module_symbol_policy(payload);
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "module_symbol_policy_update",
        None,
        None,
        audit_details,
        &response,
    );
    Json(response)
}

async fn manual_set_leverage(
    State(state): State<FrontendAppState>,
    Json(payload): Json<ManualLeveragePayload>,
) -> Json<ApiResult<ManualLeverageUpdateResult>> {
    set_leverage_for_module(state, payload, "manual").await
}

async fn fib_set_leverage(
    State(state): State<FrontendAppState>,
    Json(payload): Json<ManualLeveragePayload>,
) -> Json<ApiResult<ManualLeverageUpdateResult>> {
    set_leverage_for_module(state, payload, "fib").await
}

async fn set_leverage_for_module(
    state: FrontendAppState,
    payload: ManualLeveragePayload,
    module: &'static str,
) -> Json<ApiResult<ManualLeverageUpdateResult>> {
    let audit_details = json!({
        "module": module,
        "market": payload.market.clone().unwrap_or_else(|| MARKET_XYZ_PERP.to_string()),
        "coin": payload.coin.clone(),
        "leverage": payload.leverage,
        "margin_mode": payload.margin_mode.clone(),
        "submit": payload.submit,
        "confirm_mainnet_live": payload.confirm_mainnet_live,
    });
    let audit_action = if payload.submit {
        if module == "fib" {
            "fib_set_leverage_submit"
        } else {
            "manual_set_leverage_submit"
        }
    } else {
        if module == "fib" {
            "fib_set_leverage_plan"
        } else {
            "manual_set_leverage_plan"
        }
    };
    let audit_account_id = Some(payload.account_id.clone());
    let audit_coin = Some(payload.coin.clone());
    let result = async {
        let base_config = state.config_snapshot()?;
        let market = payload.market_profile(&base_config)?;
        anyhow::ensure!(
            !market.is_spot(),
            "spot market does not support perp leverage updates"
        );
        let config = scoped_config_for_module_and_market(base_config, module, &market);
        let mut options = payload.try_into_options()?;
        options.coin = normalize_dex_coin(&market.dex, &options.coin);
        ensure_accounts_allowed_for_market(&config, &[options.account_id.clone()], market.id)?;
        let password = if options.submit {
            validate_live_action_gates(&config, options.confirm_mainnet_live)?;
            let account = config
                .account(&options.account_id)
                .with_context(|| format!("account {} not found in config", options.account_id))?;
            anyhow::ensure!(
                account.enabled && account.worker_enabled,
                "account {} is not enabled for worker execution",
                account.account_id
            );
            ensure_live_account_address(account)?;
            state.audit_attempt(
                if module == "fib" {
                    "fib_set_leverage_submit_attempt"
                } else {
                    "manual_set_leverage_submit_attempt"
                },
                Some(options.account_id.clone()),
                Some(options.coin.clone()),
                audit_details.clone(),
            )?;
            let path = PathBuf::from(&config.secrets.vault_path);
            Some(state.resolve_vault_password(&path, "")?)
        } else {
            None
        };
        execute_manual_leverage_update(config, options, password.as_deref()).await
    }
    .await;

    let response = ApiResult::from_result(result);
    state.audit_api_result(
        audit_action,
        audit_account_id,
        audit_coin,
        audit_details,
        &response,
    );
    Json(response)
}

async fn manual_market_quote(
    State(state): State<FrontendAppState>,
    Json(payload): Json<ManualMarketQuotePayload>,
) -> Json<ApiResult<ManualMarketQuoteResponse>> {
    let result = async {
        let config = state.config_snapshot()?;
        build_manual_market_quote(&config, &payload, Some(&state.realtime)).await
    }
    .await;
    Json(ApiResult::from_result(result))
}

async fn manual_quote_ws(
    ws: WebSocketUpgrade,
    State(state): State<FrontendAppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| manual_quote_ws_session(state, socket))
}

async fn manual_quote_ws_session(state: FrontendAppState, mut socket: WebSocket) {
    let mut subscribed: Option<ManualMarketQuotePayload> = None;
    let mut interval_ms: u64 = 1_000;
    let mut next_tick = tokio::time::Instant::now() + std::time::Duration::from_millis(1_000);

    loop {
        tokio::select! {
            incoming = socket.recv() => {
                let Some(message_result) = incoming else {
                    break;
                };
                match message_result {
                    Ok(Message::Text(text)) => {
                        match serde_json::from_str::<ManualQuoteWsRequest>(&text) {
                            Ok(request) => {
                                let action = request
                                    .action
                                    .as_deref()
                                    .unwrap_or("subscribe")
                                    .trim()
                                    .to_ascii_lowercase();
                                if action == "subscribe" {
                                    let mut next = subscribed
                                        .clone()
                                        .unwrap_or(ManualMarketQuotePayload {
                                            market: None,
                                            coin: String::new(),
                                        });
                                    if let Some(market) = request.market {
                                        next.market = Some(market);
                                    }
                                    if let Some(coin) = request.coin {
                                        next.coin = coin;
                                    }
                                    if !next.coin.trim().is_empty() && next.market.is_some() {
                                        subscribed = Some(next);
                                    }
                                }
                                if let Some(next_interval) = request.interval_ms {
                                    interval_ms = next_interval.clamp(800, 3_000);
                                }
                                // Push the first quote immediately after (re)subscribe; later frames use interval_ms.
                                next_tick = tokio::time::Instant::now();
                            }
                            Err(error) => {
                                let response = ApiResult::<ManualMarketQuoteResponse> {
                                    ok: false,
                                    data: None,
                                    error: Some(format!("invalid quote ws payload: {error}")),
                                };
                                if send_ws_json(&mut socket, &response).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Ok(Message::Ping(payload)) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Ok(Message::Close(_)) => break,
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
            _ = tokio::time::sleep_until(next_tick), if subscribed.is_some() => {
                next_tick = tokio::time::Instant::now()
                    + std::time::Duration::from_millis(interval_ms);
                let response = match (&subscribed, state.config_snapshot()) {
                    (Some(payload), Ok(config)) => {
                        ApiResult::from_result(
                            build_manual_market_quote(&config, payload, Some(&state.realtime))
                                .await,
                        )
                    }
                    (_, Err(error)) => ApiResult::<ManualMarketQuoteResponse> {
                        ok: false,
                        data: None,
                        error: Some(format_anyhow_error(&error)),
                    },
                    _ => ApiResult::<ManualMarketQuoteResponse> {
                        ok: false,
                        data: None,
                        error: Some("manual quote ws is not subscribed".to_string()),
                    },
                };
                if send_ws_json(&mut socket, &response).await.is_err() {
                    break;
                }
            }
        }
    }
}

async fn send_ws_json<T: Serialize>(socket: &mut WebSocket, payload: &T) -> Result<()> {
    let text = serde_json::to_string(payload).context("failed to serialize websocket payload")?;
    socket
        .send(Message::Text(text.into()))
        .await
        .context("failed to send websocket message")
}

async fn build_manual_market_quote(
    config: &AppConfig,
    payload: &ManualMarketQuotePayload,
    realtime: Option<&RealtimeState>,
) -> Result<ManualMarketQuoteResponse> {
    let market = resolve_market_profile(payload.market.as_deref(), config)?;
    if market.is_spot() {
        let canonical_coin = normalize_spot_coin(&payload.coin);
        let snapshot = fetch_spot_market_snapshot_cached(
            &config.app.environment,
            MARKET_SNAPSHOT_QUOTE_CACHE_TTL_MS,
        )
        .await
        .context("failed to fetch spot market snapshot")?;
        let context = snapshot.asset_context(&canonical_coin)?;
        let ws_mid = realtime.and_then(|realtime| {
            let candidates = [
                canonical_coin.clone(),
                snapshot.candle_coin(&canonical_coin).unwrap_or_default(),
            ]
            .into_iter()
            .filter(|value| !value.trim().is_empty())
            .collect::<Vec<_>>();
            realtime.mid_price(MARKET_SPOT, &candidates)
        });
        let reference_price = ws_mid
            .or_else(|| parse_optional_decimal(context.mid_px.as_deref()))
            .or_else(|| context.mark_px.parse::<f64>().ok())
            .or_else(|| context.prev_day_px.parse::<f64>().ok());
        Ok(ManualMarketQuoteResponse {
            environment: config.app.environment.clone(),
            market: market.id.to_string(),
            market_label: market.label.to_string(),
            dex: market.dex_display(),
            coin: canonical_coin,
            reference_price,
            mark_price: context.mark_px.parse::<f64>().ok(),
            mid_price: ws_mid.or_else(|| parse_optional_decimal(context.mid_px.as_deref())),
            oracle_price: None,
            funding_rate: None,
            open_interest: None,
            day_notional_volume: context.day_ntl_vlm.parse::<f64>().ok(),
            max_leverage: None,
            only_isolated: None,
            margin_mode: None,
            fetched_at_ms: now_ms(),
        })
    } else {
        let canonical_coin = normalize_dex_coin(&market.dex, &payload.coin);
        let snapshot = fetch_xyz_market_snapshot_cached(
            &config.app.environment,
            &market.dex,
            MARKET_SNAPSHOT_QUOTE_CACHE_TTL_MS,
        )
        .await
        .with_context(|| format!("failed to fetch {} market snapshot", market.id))?;
        let asset = snapshot.asset(&canonical_coin)?;
        let live_mid = if let Some(ws_mid) = realtime.and_then(|realtime| {
            realtime.mid_price(market.id, std::slice::from_ref(&canonical_coin))
        }) {
            Some(ws_mid)
        } else {
            fetch_perp_all_mids_cached(
                &config.app.environment,
                &market.dex,
                MARKET_SNAPSHOT_QUOTE_CACHE_TTL_MS,
            )
            .await
            .ok()
            .and_then(|mids| mids.get(&canonical_coin).cloned())
            .and_then(|value| value.parse::<f64>().ok())
        };
        let mid_price =
            live_mid.or_else(|| parse_optional_decimal(asset.context.mid_px.as_deref()));
        let mark_price = parse_optional_decimal(asset.context.mark_px.as_deref());
        let oracle_price = parse_optional_decimal(asset.context.oracle_px.as_deref());
        let reference_price = mid_price
            .or(mark_price)
            .or(oracle_price)
            .or_else(|| asset.reference_price().ok());
        Ok(ManualMarketQuoteResponse {
            environment: config.app.environment.clone(),
            market: market.id.to_string(),
            market_label: market.label.to_string(),
            dex: market.dex_display(),
            coin: canonical_coin,
            reference_price,
            mark_price,
            mid_price,
            oracle_price,
            funding_rate: parse_optional_decimal(asset.context.funding.as_deref()),
            open_interest: parse_optional_decimal(asset.context.open_interest.as_deref()),
            day_notional_volume: parse_optional_decimal(asset.context.day_ntl_vlm.as_deref()),
            max_leverage: asset.meta.max_leverage,
            only_isolated: asset.meta.only_isolated,
            margin_mode: asset.meta.margin_mode.clone(),
            fetched_at_ms: now_ms(),
        })
    }
}

async fn manual_market_universe(
    State(state): State<FrontendAppState>,
) -> Json<ApiResult<ManualMarketUniverseResponse>> {
    manual_market_universe_with_input(state, ManualMarketUniversePayload::default()).await
}

async fn manual_market_universe_with_payload(
    State(state): State<FrontendAppState>,
    Json(payload): Json<ManualMarketUniversePayload>,
) -> Json<ApiResult<ManualMarketUniverseResponse>> {
    manual_market_universe_with_input(state, payload).await
}

async fn manual_market_universe_with_input(
    state: FrontendAppState,
    payload: ManualMarketUniversePayload,
) -> Json<ApiResult<ManualMarketUniverseResponse>> {
    let result = async {
        let config = state.config_snapshot()?;
        let market = resolve_market_profile(payload.market.as_deref(), &config)?;
        let mut assets = if market.is_spot() {
            let snapshot = fetch_spot_market_snapshot_cached(
                &config.app.environment,
                MARKET_SNAPSHOT_UNIVERSE_CACHE_TTL_MS,
            )
            .await
            .context("failed to fetch spot market snapshot")?;
            let mut rows = snapshot
                .universe()
                .into_iter()
                .filter_map(|coin| {
                    snapshot.asset(&coin).ok().map(|asset| {
                        let sz_decimals = asset.sz_decimals;
                        ManualMarketUniverseAssetResponse {
                            coin,
                            sz_decimals: Some(sz_decimals),
                            size_step: Some(10_f64.powi(-(sz_decimals as i32))),
                        }
                    })
                })
                .collect::<Vec<_>>();
            rows.sort_by(|left, right| left.coin.cmp(&right.coin));
            rows.dedup_by(|left, right| left.coin == right.coin);
            rows
        } else {
            let snapshot = fetch_xyz_market_snapshot_cached(
                &config.app.environment,
                &market.dex,
                MARKET_SNAPSHOT_UNIVERSE_CACHE_TTL_MS,
            )
            .await
            .with_context(|| format!("failed to fetch {} market snapshot", market.id))?;
            snapshot
                .meta
                .universe
                .iter()
                .filter(|asset| asset.is_delisted != Some(true))
                .map(|asset| {
                    let sz_decimals = asset.sz_decimals;
                    ManualMarketUniverseAssetResponse {
                        coin: asset.name.clone(),
                        sz_decimals: Some(sz_decimals),
                        size_step: Some(10_f64.powi(-(sz_decimals as i32))),
                    }
                })
                .collect::<Vec<_>>()
        };
        let mut coins = assets
            .iter()
            .map(|asset| asset.coin.clone())
            .collect::<Vec<_>>();
        coins.sort_unstable();
        coins.dedup();
        assets.sort_by(|left, right| left.coin.cmp(&right.coin));
        assets.dedup_by(|left, right| left.coin == right.coin);
        let default_coin = if market.is_spot() {
            normalize_spot_coin(&config.default_coin_for_market(market.id))
        } else {
            normalize_dex_coin(&market.dex, &config.default_coin_for_market(market.id))
        };
        if !coins.iter().any(|coin| coin == &default_coin) {
            coins.insert(0, default_coin.clone());
        }
        Ok(ManualMarketUniverseResponse {
            environment: config.app.environment,
            market: market.id.to_string(),
            market_label: market.label.to_string(),
            dex: market.dex_display(),
            default_coin,
            coins,
            assets,
            fetched_at_ms: now_ms(),
        })
    }
    .await;
    Json(ApiResult::from_result(result))
}

async fn manual_market_capabilities(
    State(state): State<FrontendAppState>,
) -> Json<ApiResult<MarketCapabilitiesResponse>> {
    let result = async {
        let config = state.config_snapshot()?;
        let markets = supported_market_ids()
            .iter()
            .map(|market_id| resolve_market_profile(Some(market_id), &config))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|profile| MarketCapability {
                market: profile.id.to_string(),
                label: profile.label.to_string(),
                dex: profile.dex_display(),
                live_trading_supported: profile.live_trading_supported,
            })
            .collect::<Vec<_>>();
        Ok(MarketCapabilitiesResponse {
            environment: config.app.environment,
            default_market: MARKET_XYZ_PERP.to_string(),
            markets,
        })
    }
    .await;
    Json(ApiResult::from_result(result))
}

async fn signed_smoke(
    State(state): State<FrontendAppState>,
    Json(payload): Json<SignedSmokePayload>,
) -> Json<ApiResult<SignedSmokeResult>> {
    let source_module = payload
        .source_module()
        .map(ToString::to_string)
        .unwrap_or_else(|_| "manual".to_string());
    let audit_details = json!({
        "source_module": source_module,
        "market": payload.market.clone().unwrap_or_else(|| MARKET_XYZ_PERP.to_string()),
        "side": payload.side.clone(),
        "notional_usd": payload.notional_usd,
        "max_slippage_bps": payload.max_slippage_bps,
        "execution_mode": payload.execution_mode.clone(),
        "reduce_only": payload.reduce_only,
        "submit": payload.submit,
        "cancel_resting": payload.cancel_resting,
        "confirm_mainnet_live": payload.confirm_mainnet_live,
    });
    let audit_action = if payload.submit {
        "signed_smoke_submit"
    } else {
        "signed_smoke_plan"
    };
    let audit_account_id = Some(payload.account_id.clone());
    let audit_coin = Some(payload.coin.clone());
    let result = async {
        let module = payload.source_module()?;
        let base_config = state.config_snapshot()?;
        let market = payload.market_profile(&base_config)?;
        let config = scoped_config_for_module_and_market(base_config, module, &market);
        let mut options = payload.try_into_options()?;
        options.coin = normalize_coin_for_market(&market, &options.coin);
        ensure_accounts_allowed_for_market(&config, &[options.account_id.clone()], market.id)?;
        let password = if options.submit {
            let close_gate = signed_close_exempt_from_opening_rules(
                &config.hyperliquid.dex,
                options.side,
                options.reduce_only,
                options.close_full_position,
            );
            validate_live_order_gates(&config, options.confirm_mainnet_live, close_gate)?;
            validate_exchange_min_order_notional(options.notional_usd, close_gate)?;
            let account = config
                .account(&options.account_id)
                .with_context(|| format!("account {} not found in config", options.account_id))?;
            anyhow::ensure!(
                account.enabled && account.worker_enabled,
                "account {} is not enabled for worker execution",
                account.account_id
            );
            ensure_live_account_address(account)?;
            state.audit_attempt(
                "signed_smoke_submit_attempt",
                Some(options.account_id.clone()),
                Some(options.coin.clone()),
                audit_details.clone(),
            )?;
            let path = PathBuf::from(&config.secrets.vault_path);
            Some(state.resolve_vault_password(&path, "")?)
        } else {
            None
        };
        execute_signed_smoke(config, options, password.as_deref()).await
    }
    .await;

    let response = ApiResult::from_result(result);
    state.audit_api_result(
        audit_action,
        audit_account_id,
        audit_coin,
        audit_details,
        &response,
    );
    Json(response)
}

async fn signed_acceptance(
    State(state): State<FrontendAppState>,
    Json(payload): Json<SignedSmokePayload>,
) -> Json<ApiResult<SignedAcceptanceResult>> {
    let source_module = payload
        .source_module()
        .map(ToString::to_string)
        .unwrap_or_else(|_| "manual".to_string());
    let audit_details = json!({
        "source_module": source_module,
        "market": payload.market.clone().unwrap_or_else(|| MARKET_XYZ_PERP.to_string()),
        "side": payload.side.clone(),
        "notional_usd": payload.notional_usd,
        "max_slippage_bps": payload.max_slippage_bps,
        "execution_mode": payload.execution_mode.clone(),
        "reduce_only": payload.reduce_only,
        "submit": payload.submit,
        "cancel_resting": payload.cancel_resting,
        "confirm_mainnet_live": payload.confirm_mainnet_live,
    });
    let audit_action = if payload.submit {
        "signed_acceptance_submit"
    } else {
        "signed_acceptance_plan"
    };
    let audit_account_id = Some(payload.account_id.clone());
    let audit_coin = Some(payload.coin.clone());
    let result = async {
        let module = payload.source_module()?;
        let base_config = state.config_snapshot()?;
        let market = payload.market_profile(&base_config)?;
        let config = scoped_config_for_module_and_market(base_config, module, &market);
        let mut options = payload.try_into_acceptance_options()?;
        options.coin = normalize_coin_for_market(&market, &options.coin);
        ensure_accounts_allowed_for_market(&config, &[options.account_id.clone()], market.id)?;
        let password = if options.submit {
            let close_gate = signed_close_exempt_from_opening_rules(
                &config.hyperliquid.dex,
                options.side,
                options.reduce_only,
                options.close_full_position,
            );
            validate_live_order_gates(&config, options.confirm_mainnet_live, close_gate)?;
            validate_exchange_min_order_notional(options.notional_usd, close_gate)?;
            let account = config
                .account(&options.account_id)
                .with_context(|| format!("account {} not found in config", options.account_id))?;
            anyhow::ensure!(
                account.enabled && account.worker_enabled,
                "account {} is not enabled for worker execution",
                account.account_id
            );
            ensure_live_account_address(account)?;
            state.audit_attempt(
                "signed_acceptance_submit_attempt",
                Some(options.account_id.clone()),
                Some(options.coin.clone()),
                audit_details.clone(),
            )?;
            let path = PathBuf::from(&config.secrets.vault_path);
            Some(state.resolve_vault_password(&path, "")?)
        } else {
            None
        };
        execute_signed_acceptance(config, options, password.as_deref()).await
    }
    .await;

    let response = ApiResult::from_result(result);
    state.audit_api_result(
        audit_action,
        audit_account_id,
        audit_coin,
        audit_details,
        &response,
    );
    Json(response)
}

async fn signed_runbook(
    State(state): State<FrontendAppState>,
    Json(payload): Json<SignedSmokePayload>,
) -> Json<ApiResult<SignedRunbookResult>> {
    let source_module = payload
        .source_module()
        .map(ToString::to_string)
        .unwrap_or_else(|_| "manual".to_string());
    let audit_details = json!({
        "source_module": source_module,
        "market": payload.market.clone().unwrap_or_else(|| MARKET_XYZ_PERP.to_string()),
        "side": payload.side.clone(),
        "notional_usd": payload.notional_usd,
        "max_slippage_bps": payload.max_slippage_bps,
        "execution_mode": payload.execution_mode.clone(),
        "reduce_only": payload.reduce_only,
        "submit": payload.submit,
        "cancel_resting": payload.cancel_resting,
        "confirm_mainnet_live": payload.confirm_mainnet_live,
    });
    let audit_action = if payload.submit {
        "signed_runbook_submit"
    } else {
        "signed_runbook_plan"
    };
    let audit_account_id = Some(payload.account_id.clone());
    let audit_coin = Some(payload.coin.clone());
    let result = async {
        let module = payload.source_module()?;
        let base_config = state.config_snapshot()?;
        let market = payload.market_profile(&base_config)?;
        let config = scoped_config_for_module_and_market(base_config, module, &market);
        let mut options = payload.try_into_runbook_options()?;
        options.coin = normalize_coin_for_market(&market, &options.coin);
        ensure_accounts_allowed_for_market(&config, &[options.account_id.clone()], market.id)?;
        let path = PathBuf::from(&config.secrets.vault_path);
        let password = if options.submit {
            let close_gate = signed_close_exempt_from_opening_rules(
                &config.hyperliquid.dex,
                options.side,
                options.reduce_only,
                options.close_full_position,
            );
            validate_live_order_gates(&config, options.confirm_mainnet_live, close_gate)?;
            validate_exchange_min_order_notional(options.notional_usd, close_gate)?;
            let account = config
                .account(&options.account_id)
                .with_context(|| format!("account {} not found in config", options.account_id))?;
            anyhow::ensure!(
                account.enabled && account.worker_enabled,
                "account {} is not enabled for worker execution",
                account.account_id
            );
            ensure_live_account_address(account)?;
            state.audit_attempt(
                "signed_runbook_submit_attempt",
                Some(options.account_id.clone()),
                Some(options.coin.clone()),
                audit_details.clone(),
            )?;
            Some(state.resolve_vault_password(&path, "")?)
        } else {
            state.unlocked_vault_password(&path)?
        };
        execute_signed_runbook(config, options, password.as_deref()).await
    }
    .await;

    let response = ApiResult::from_result(result);
    state.audit_api_result(
        audit_action,
        audit_account_id,
        audit_coin,
        audit_details,
        &response,
    );
    Json(response)
}

async fn fast_signed_runbook(
    State(state): State<FrontendAppState>,
    Json(payload): Json<SignedSmokePayload>,
) -> Json<ApiResult<FastSignedOrderResult>> {
    let source_module = payload
        .source_module()
        .map(ToString::to_string)
        .unwrap_or_else(|_| "manual".to_string());
    let audit_details = json!({
        "source_module": source_module,
        "market": payload.market.clone().unwrap_or_else(|| MARKET_XYZ_PERP.to_string()),
        "side": payload.side.clone(),
        "notional_usd": payload.notional_usd,
        "max_slippage_bps": payload.max_slippage_bps,
        "execution_mode": payload.execution_mode.clone(),
        "reduce_only": payload.reduce_only,
        "close_full_position": payload.close_full_position,
        "submit": payload.submit,
        "cancel_resting": payload.cancel_resting,
        "confirm_mainnet_live": payload.confirm_mainnet_live,
        "transport": "websocket_post_action",
    });
    let audit_action = if payload.submit {
        "fast_signed_runbook_submit"
    } else {
        "fast_signed_runbook_plan"
    };
    let audit_account_id = Some(payload.account_id.clone());
    let audit_coin = Some(payload.coin.clone());
    let result = async {
        let module = payload.source_module()?;
        let base_config = state.config_snapshot()?;
        let market = payload.market_profile(&base_config)?;
        let config = scoped_config_for_module_and_market(base_config, module, &market);
        let mut options = payload.try_into_options()?;
        options.coin = normalize_coin_for_market(&market, &options.coin);
        ensure_accounts_allowed_for_market(&config, &[options.account_id.clone()], market.id)?;
        let path = PathBuf::from(&config.secrets.vault_path);
        let password = if options.submit {
            let close_gate = signed_close_exempt_from_opening_rules(
                &config.hyperliquid.dex,
                options.side,
                options.reduce_only,
                options.close_full_position,
            );
            validate_live_order_gates(&config, options.confirm_mainnet_live, close_gate)?;
            validate_exchange_min_order_notional(options.notional_usd, close_gate)?;
            let account = config
                .account(&options.account_id)
                .with_context(|| format!("account {} not found in config", options.account_id))?;
            anyhow::ensure!(
                account.enabled && account.worker_enabled,
                "account {} is not enabled for worker execution",
                account.account_id
            );
            ensure_live_account_address(account)?;
            state.audit_attempt(
                "fast_signed_runbook_submit_attempt",
                Some(options.account_id.clone()),
                Some(options.coin.clone()),
                audit_details.clone(),
            )?;
            Some(state.resolve_vault_password(&path, "")?)
        } else {
            state.unlocked_vault_password(&path)?
        };
        execute_fast_signed_order(config, options, password.as_deref(), Some(&state.realtime)).await
    }
    .await;

    let response = ApiResult::from_result(result);
    state.audit_api_result(
        audit_action,
        audit_account_id,
        audit_coin,
        audit_details,
        &response,
    );
    Json(response)
}

async fn manual_protective_exit(
    State(state): State<FrontendAppState>,
    Json(payload): Json<ProtectiveExitPayload>,
) -> Json<ApiResult<ProtectiveExitPlanResult>> {
    let audit_details = json!({
        "market": payload.market.clone().unwrap_or_else(|| MARKET_XYZ_PERP.to_string()),
        "entry_side": payload.exit.entry_side.clone(),
        "entry_price": payload.exit.entry_price,
        "notional_usd": payload.exit.notional_usd,
        "take_profit_usd": payload.exit.take_profit_usd,
        "stop_loss_pct": payload.exit.stop_loss_pct,
        "take_profit_trigger_price": payload.exit.take_profit_trigger_price,
        "stop_loss_trigger_price": payload.exit.stop_loss_trigger_price,
        "max_slippage_bps": payload.exit.max_slippage_bps,
        "dry_run": state.dry_run,
    });
    let audit_account_id = Some(payload.exit.account_id.clone());
    let audit_coin = Some(payload.exit.coin.clone());
    let result = async {
        let base_config = state.config_snapshot()?;
        let market = payload.market_profile(&base_config)?;
        ensure_accounts_allowed_for_market(
            &base_config,
            std::slice::from_ref(&payload.exit.account_id),
            market.id,
        )?;
        let config = scoped_config_for_module_and_market(base_config, "manual", &market);
        let options = payload.into_options(&market);
        build_protective_exit_plan(&config, options, state.dry_run).await
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "manual_protective_exit_plan",
        audit_account_id,
        audit_coin,
        audit_details,
        &response,
    );
    Json(response)
}

async fn manual_protective_trigger(
    State(state): State<FrontendAppState>,
    Json(payload): Json<ProtectiveExitTriggerPayload>,
) -> Json<ApiResult<ProtectiveExitTriggerCheckResult>> {
    let audit_details = json!({
        "market": payload.market.clone().unwrap_or_else(|| MARKET_XYZ_PERP.to_string()),
        "entry_side": payload.trigger.exit.entry_side.clone(),
        "entry_price": payload.trigger.exit.entry_price,
        "notional_usd": payload.trigger.exit.notional_usd,
        "take_profit_usd": payload.trigger.exit.take_profit_usd,
        "stop_loss_pct": payload.trigger.exit.stop_loss_pct,
        "take_profit_trigger_price": payload.trigger.exit.take_profit_trigger_price,
        "stop_loss_trigger_price": payload.trigger.exit.stop_loss_trigger_price,
        "max_slippage_bps": payload.trigger.exit.max_slippage_bps,
        "observed_price": payload.trigger.observed_price,
        "dry_run": state.dry_run,
    });
    let audit_account_id = Some(payload.trigger.exit.account_id.clone());
    let audit_coin = Some(payload.trigger.exit.coin.clone());
    let result = async {
        let base_config = state.config_snapshot()?;
        let market = payload.market_profile(&base_config)?;
        ensure_accounts_allowed_for_market(
            &base_config,
            std::slice::from_ref(&payload.trigger.exit.account_id),
            market.id,
        )?;
        let config = scoped_config_for_module_and_market(base_config, "manual", &market);
        let options = payload.into_options(&market);
        check_protective_exit_trigger(&config, options, state.dry_run).await
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "manual_protective_trigger_check",
        audit_account_id,
        audit_coin,
        audit_details,
        &response,
    );
    Json(response)
}

async fn manual_protective_submit(
    State(state): State<FrontendAppState>,
    Json(payload): Json<ProtectiveExitSubmitPayload>,
) -> Json<ApiResult<ProtectiveExitSubmitResult>> {
    let audit_details = json!({
        "market": payload.market.clone().unwrap_or_else(|| MARKET_XYZ_PERP.to_string()),
        "entry_side": payload.submit.trigger.exit.entry_side.clone(),
        "entry_price": payload.submit.trigger.exit.entry_price,
        "observed_price": payload.submit.trigger.observed_price,
        "notional_usd": payload.submit.trigger.exit.notional_usd,
        "take_profit_usd": payload.submit.trigger.exit.take_profit_usd,
        "stop_loss_pct": payload.submit.trigger.exit.stop_loss_pct,
        "take_profit_trigger_price": payload.submit.trigger.exit.take_profit_trigger_price,
        "stop_loss_trigger_price": payload.submit.trigger.exit.stop_loss_trigger_price,
        "max_slippage_bps": payload.submit.trigger.exit.max_slippage_bps,
        "submit": payload.submit.submit,
        "confirm_mainnet_live": payload.submit.confirm_mainnet_live,
    });
    let audit_account_id = Some(payload.submit.trigger.exit.account_id.clone());
    let audit_coin = Some(payload.submit.trigger.exit.coin.clone());
    let result = async {
        let base_config = state.config_snapshot()?;
        let market = payload.market_profile(&base_config)?;
        ensure_accounts_allowed_for_market(
            &base_config,
            std::slice::from_ref(&payload.submit.trigger.exit.account_id),
            market.id,
        )?;
        let config = scoped_config_for_module_and_market(base_config, "manual", &market);
        let payload = payload.into_options(&market);
        let path = PathBuf::from(&config.secrets.vault_path);
        let password = if payload.submit {
            let trigger =
                check_protective_exit_trigger(&config, payload.trigger.clone(), state.dry_run)
                    .await?;
            if !trigger.triggered {
                return Ok(ProtectiveExitSubmitResult {
                    trigger,
                    submit_requested: true,
                    submitted: false,
                    submit_report: None,
                    post_submit_reconciliation: None,
                    order_status: None,
                });
            }
            validate_live_order_gates(&config, payload.confirm_mainnet_live, true)?;
            let account = config
                .account(&payload.trigger.exit.account_id)
                .with_context(|| {
                    format!(
                        "account {} not found in config",
                        payload.trigger.exit.account_id
                    )
                })?;
            anyhow::ensure!(
                account.enabled && account.worker_enabled,
                "account {} is not enabled for worker execution",
                account.account_id
            );
            ensure_live_account_address(account)?;
            state.audit_attempt(
                "manual_protective_exit_submit_attempt",
                Some(payload.trigger.exit.account_id.clone()),
                Some(payload.trigger.exit.coin.clone()),
                audit_details.clone(),
            )?;
            Some(state.resolve_vault_password(&path, "")?)
        } else {
            state.unlocked_vault_password(&path)?
        };
        execute_protective_exit_submit(config, payload, state.dry_run, password.as_deref()).await
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "manual_protective_exit_submit",
        audit_account_id,
        audit_coin,
        audit_details,
        &response,
    );
    Json(response)
}

async fn manual_protective_arm(
    State(state): State<FrontendAppState>,
    Json(payload): Json<ProtectiveExitArmPayload>,
) -> Json<ApiResult<ProtectiveExitArmResult>> {
    let audit_details = json!({
        "market": payload.market.clone().unwrap_or_else(|| MARKET_XYZ_PERP.to_string()),
        "entry_side": payload.arm.exit.entry_side.clone(),
        "entry_price": payload.arm.exit.entry_price,
        "notional_usd": payload.arm.exit.notional_usd,
        "take_profit_usd": payload.arm.exit.take_profit_usd,
        "stop_loss_pct": payload.arm.exit.stop_loss_pct,
        "take_profit_trigger_price": payload.arm.exit.take_profit_trigger_price,
        "stop_loss_trigger_price": payload.arm.exit.stop_loss_trigger_price,
        "max_slippage_bps": payload.arm.exit.max_slippage_bps,
        "submit": payload.arm.submit,
        "confirm_mainnet_live": payload.arm.confirm_mainnet_live,
        "dry_run": state.dry_run,
    });
    let audit_account_id = Some(payload.arm.exit.account_id.clone());
    let audit_coin = Some(payload.arm.exit.coin.clone());
    let result = async {
        let base_config = state.config_snapshot()?;
        let market = payload.market_profile(&base_config)?;
        ensure_accounts_allowed_for_market(
            &base_config,
            std::slice::from_ref(&payload.arm.exit.account_id),
            market.id,
        )?;
        let config = scoped_config_for_module_and_market(base_config, "manual", &market);
        let payload = payload.into_options(&market);
        if payload.submit {
            let account = config.account(&payload.exit.account_id).with_context(|| {
                format!("account {} not found in config", payload.exit.account_id)
            })?;
            anyhow::ensure!(
                account.enabled && account.worker_enabled,
                "account {} is not enabled for worker execution",
                account.account_id
            );
            ensure_live_account_address(account)?;

            let entry_side = parse_side(&payload.exit.entry_side)?;
            let exit_side = match entry_side {
                OrderSide::Buy => OrderSide::Sell,
                OrderSide::Sell => OrderSide::Buy,
            };
            if market.is_spot() {
                let spot_state =
                    fetch_spot_clearinghouse_state(&config.app.environment, &account.address)
                        .await
                        .context("failed to fetch spot state for protective TP/SL pre-check")?;
                let readiness = spot_account_readiness_state(&spot_state, &payload.exit.coin);
                anyhow::ensure!(
                    reduce_only_spot_position_available(exit_side, readiness.coin_position_size),
                    "cannot set native spot TP/SL before holding matching inventory; {}",
                    reduce_only_spot_position_detail(exit_side, readiness.coin_position_size)
                );
            } else {
                let clearinghouse_state = fetch_clearinghouse_state(
                    &config.app.environment,
                    &config.hyperliquid.dex,
                    &account.address,
                )
                .await
                .context("failed to fetch clearinghouse state for protective TP/SL pre-check")?;
                let readiness = summarize_account_readiness_state(
                    &config.hyperliquid.dex,
                    &clearinghouse_state,
                    &payload.exit.coin,
                );
                anyhow::ensure!(
                    reduce_only_position_available(exit_side, readiness.coin_position_size),
                    "cannot set exchange-native TP/SL before opening a matching position; {}",
                    reduce_only_position_detail(exit_side, readiness.coin_position_size)
                );
            }
        }
        let path = PathBuf::from(&config.secrets.vault_path);
        let password = if payload.submit {
            state.audit_attempt(
                "manual_protective_exit_arm_submit_attempt",
                Some(payload.exit.account_id.clone()),
                Some(payload.exit.coin.clone()),
                audit_details.clone(),
            )?;
            Some(state.resolve_vault_password(&path, "")?)
        } else {
            None
        };
        let mut result = execute_protective_exit_arm(
            config.clone(),
            payload.clone(),
            state.dry_run,
            password.as_deref(),
        )
        .await?;

        if payload.submit {
            result.armed = Some(result.submitted);
            result.monitor_mode = Some("exchange_native_trigger".to_string());
        }

        Ok(result)
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "manual_protective_exit_arm",
        audit_account_id,
        audit_coin,
        audit_details,
        &response,
    );
    Json(response)
}

async fn fast_manual_protective_arm(
    State(state): State<FrontendAppState>,
    Json(payload): Json<ProtectiveExitArmPayload>,
) -> Json<ApiResult<FastProtectiveExitArmResult>> {
    let audit_details = json!({
        "market": payload.market.clone().unwrap_or_else(|| MARKET_XYZ_PERP.to_string()),
        "entry_side": payload.arm.exit.entry_side.clone(),
        "entry_price": payload.arm.exit.entry_price,
        "notional_usd": payload.arm.exit.notional_usd,
        "take_profit_usd": payload.arm.exit.take_profit_usd,
        "stop_loss_pct": payload.arm.exit.stop_loss_pct,
        "take_profit_trigger_price": payload.arm.exit.take_profit_trigger_price,
        "stop_loss_trigger_price": payload.arm.exit.stop_loss_trigger_price,
        "max_slippage_bps": payload.arm.exit.max_slippage_bps,
        "submit": payload.arm.submit,
        "confirm_mainnet_live": payload.arm.confirm_mainnet_live,
        "dry_run": state.dry_run,
        "transport": "websocket_post_action",
    });
    let audit_account_id = Some(payload.arm.exit.account_id.clone());
    let audit_coin = Some(payload.arm.exit.coin.clone());
    let result = async {
        let base_config = state.config_snapshot()?;
        let market = payload.market_profile(&base_config)?;
        ensure_accounts_allowed_for_market(
            &base_config,
            std::slice::from_ref(&payload.arm.exit.account_id),
            market.id,
        )?;
        let config = scoped_config_for_module_and_market(base_config, "manual", &market);
        let options = payload.into_options(&market);
        let path = PathBuf::from(&config.secrets.vault_path);
        let password = if options.submit {
            state.audit_attempt(
                "fast_manual_protective_exit_arm_submit_attempt",
                Some(options.exit.account_id.clone()),
                Some(options.exit.coin.clone()),
                audit_details.clone(),
            )?;
            Some(state.resolve_vault_password(&path, "")?)
        } else {
            None
        };
        execute_fast_protective_exit_arm(
            config,
            options,
            state.dry_run,
            password.as_deref(),
            Some(&state.realtime),
        )
        .await
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "fast_manual_protective_exit_arm",
        audit_account_id,
        audit_coin,
        audit_details,
        &response,
    );
    Json(response)
}

async fn manual_protective_rules(
    State(state): State<FrontendAppState>,
    Json(payload): Json<ManualProtectiveRulesPayload>,
) -> Json<ApiResult<ManualProtectiveRulesResponse>> {
    let audit_details = json!({
        "market": payload.market.clone().unwrap_or_else(|| MARKET_XYZ_PERP.to_string()),
        "account_ids": payload.account_ids.clone(),
        "include_disabled": payload.include_disabled,
    });
    let result = async {
        let config = state.config_snapshot()?;
        let market = payload.market_profile(&config)?;
        let account_ids = payload.selected_account_ids(&config);
        validate_batch_account_count("manual protective rules query", &config, &account_ids)?;
        ensure_accounts_allowed_for_market(&config, &account_ids, market.id)?;
        let cache_key = snapshot_cache_key(
            &config.app.environment,
            market.id,
            &account_ids,
            if payload.include_disabled {
                "include_disabled"
            } else {
                "enabled_only"
            },
        );
        if let Some(cached) = state.cached_manual_protective_rules(&cache_key)? {
            return Ok(cached);
        }

        let rule_futures = account_ids
            .iter()
            .map(|account_id| {
                let account = config
                    .account(account_id)
                    .with_context(|| format!("account {} not found in config", account_id))?;
                let config = config.clone();
                let realtime = state.realtime.clone();
                let market = market.clone();
                let account_id = account_id.clone();
                let address = account.address.clone();
                Ok(async move {
                    load_manual_protective_rule_views_for_account(
                        &config,
                        &realtime,
                        &market,
                        &account_id,
                        &address,
                    )
                    .await
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut rules = Vec::new();
        for rule_group in join_all(rule_futures).await {
            rules.extend(rule_group?);
        }
        rules.sort_by(|left, right| {
            right
                .updated_at_ms
                .cmp(&left.updated_at_ms)
                .then_with(|| left.account_id.cmp(&right.account_id))
                .then_with(|| left.coin.cmp(&right.coin))
        });

        let response = ManualProtectiveRulesResponse {
            environment: config.app.environment.clone(),
            market: market.id.to_string(),
            market_label: market.label.to_string(),
            dex: market.dex_display(),
            include_disabled: payload.include_disabled,
            account_ids,
            rules,
            fetched_at_ms: now_ms(),
        };
        state.store_manual_protective_rules_cache(cache_key, &response)?;
        Ok(response)
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "manual_protective_rules_query",
        None,
        None,
        audit_details,
        &response,
    );
    Json(response)
}

async fn live_readiness(
    State(state): State<FrontendAppState>,
    Json(payload): Json<LiveReadinessPayload>,
) -> Json<ApiResult<LiveReadinessResponse>> {
    let source_module = normalize_optional_module_name(payload.source_module.as_deref())
        .map(ToString::to_string)
        .unwrap_or_else(|_| "manual".to_string());
    let audit_details = json!({
        "source_module": source_module,
        "market": payload.market.clone().unwrap_or_else(|| MARKET_XYZ_PERP.to_string()),
        "side": payload.side.clone(),
        "notional_usd": payload.notional_usd,
        "max_slippage_bps": payload.max_slippage_bps,
        "execution_mode": payload.execution_mode.clone(),
        "reduce_only": payload.reduce_only,
    });
    let audit_account_id = Some(payload.account_id.clone());
    let audit_coin = Some(payload.coin.clone());
    let result = async {
        let module = normalize_optional_module_name(payload.source_module.as_deref())?;
        let base_config = state.config_snapshot()?;
        let market = resolve_market_profile(payload.market.as_deref(), &base_config)?;
        let config = scoped_config_for_module_and_market(base_config, module, &market);
        let mut payload = payload;
        payload.coin = normalize_coin_for_market(&market, &payload.coin);
        ensure_accounts_allowed_for_market(&config, &[payload.account_id.clone()], market.id)?;
        build_live_readiness(&state, &config, module, payload).await
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "live_readiness",
        audit_account_id,
        audit_coin,
        audit_details,
        &response,
    );
    Json(response)
}

async fn live_readiness_batch(
    State(state): State<FrontendAppState>,
    Json(payload): Json<LiveReadinessBatchPayload>,
) -> Json<ApiResult<LiveReadinessBatchResponse>> {
    let source_module = payload
        .source_module()
        .map(ToString::to_string)
        .unwrap_or_else(|_| "manual".to_string());
    let audit_details = json!({
        "source_module": source_module,
        "market": payload.market.clone().unwrap_or_else(|| MARKET_XYZ_PERP.to_string()),
        "account_ids": payload.account_ids.clone(),
        "side": payload.side.clone(),
        "notional_usd": payload.notional_usd,
        "max_slippage_bps": payload.max_slippage_bps,
        "execution_mode": payload.execution_mode.clone(),
        "reduce_only": payload.reduce_only,
    });
    let audit_coin = Some(payload.coin.clone());
    let result = async {
        let module = payload.source_module()?;
        let base_config = state.config_snapshot()?;
        let market = resolve_market_profile(payload.market.as_deref(), &base_config)?;
        let config = scoped_config_for_module_and_market(base_config, module, &market);
        let account_ids = payload.selected_account_ids(&config);
        anyhow::ensure!(
            !account_ids.is_empty(),
            "readiness batch requires at least one selected or enabled account"
        );
        ensure_accounts_allowed_for_market(&config, &account_ids, market.id)?;

        let readiness_futures = account_ids.iter().cloned().map(|account_id| {
            let state = state.clone();
            let config = config.clone();
            let mut account_payload = payload.for_account(account_id);
            account_payload.coin = normalize_coin_for_market(&market, &account_payload.coin);
            async move { build_live_readiness(&state, &config, module, account_payload).await }
        });
        let results = join_all(readiness_futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let ready_account_ids = results
            .iter()
            .filter(|result| result.ready_for_testnet_submit || result.ready_for_mainnet_submit)
            .map(|result| result.account_id.clone())
            .collect::<Vec<_>>();
        let blocked_account_ids = results
            .iter()
            .filter(|result| !result.ready_for_testnet_submit && !result.ready_for_mainnet_submit)
            .map(|result| result.account_id.clone())
            .collect::<Vec<_>>();

        Ok(LiveReadinessBatchResponse {
            environment: config.app.environment.clone(),
            dry_run: config.app.dry_run,
            coin: normalize_coin_for_market(&market, &payload.coin),
            side: payload.side,
            notional_usd: payload.notional_usd,
            execution_mode: payload.execution_mode,
            reduce_only: payload.reduce_only,
            ready_account_ids,
            blocked_account_ids,
            results,
        })
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "live_readiness_batch",
        None,
        audit_coin,
        audit_details,
        &response,
    );
    Json(response)
}

async fn account_funding(
    State(state): State<FrontendAppState>,
    Json(payload): Json<AccountFundingPayload>,
) -> Json<ApiResult<AccountFundingResponse>> {
    let audit_account_id = Some(payload.account_id.clone());
    let audit_details = json!({ "account_id": payload.account_id.clone() });
    let result = async {
        let config = state.config_snapshot()?;
        build_account_funding(&config, &payload.account_id, Some(&state.realtime)).await
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "account_funding",
        audit_account_id,
        None,
        audit_details,
        &response,
    );
    Json(response)
}

async fn account_funding_batch(
    State(state): State<FrontendAppState>,
    Json(payload): Json<AccountFundingBatchPayload>,
) -> Json<ApiResult<AccountFundingBatchResponse>> {
    let audit_details = json!({
        "account_ids": payload.account_ids.clone(),
        "force_fresh": payload.force_fresh,
    });
    let result = async {
        let config = state.config_snapshot()?;
        let account_ids = selected_enabled_account_ids(&config, &payload.account_ids);
        validate_batch_account_count("funding batch", &config, &account_ids)?;
        let cache_key = snapshot_cache_key(
            &config.app.environment,
            "account_funding",
            &account_ids,
            "all_layers",
        );
        if !payload.force_fresh
            && let Some(cached) = state.cached_account_funding_batch(&cache_key)?
        {
            return Ok(cached);
        }

        let force_fresh = payload.force_fresh;
        let funding_futures = account_ids.iter().cloned().map(|account_id| {
            let config = config.clone();
            let realtime = state.realtime.clone();
            async move {
                ApiResult::from_result(
                    build_account_funding(
                        &config,
                        &account_id,
                        (!force_fresh).then_some(&realtime),
                    )
                    .await,
                )
            }
        });
        let results = join_all(funding_futures).await;

        let ready_account_ids = results
            .iter()
            .filter_map(|result| {
                result.data.as_ref().and_then(|funding| {
                    funding
                        .xyz_perp
                        .has_collateral()
                        .then(|| funding.account_id.clone())
                })
            })
            .collect::<Vec<_>>();
        let transfer_needed_account_ids = results
            .iter()
            .filter_map(|result| {
                result.data.as_ref().and_then(|funding| {
                    (!funding.xyz_perp.has_collateral() && funding.default_perp.has_collateral())
                        .then(|| funding.account_id.clone())
                })
            })
            .collect::<Vec<_>>();
        let failed_account_ids = results
            .iter()
            .enumerate()
            .filter(|(_, result)| !result.ok)
            .filter_map(|(index, _)| account_ids.get(index).cloned())
            .collect::<Vec<_>>();

        let response = AccountFundingBatchResponse {
            environment: config.app.environment.clone(),
            dex: config.hyperliquid.dex.clone(),
            account_ids,
            ready_account_ids,
            transfer_needed_account_ids,
            failed_account_ids,
            results,
        };
        if !payload.force_fresh {
            state.store_account_funding_batch_cache(cache_key, &response)?;
        }
        Ok(response)
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "account_funding_batch",
        None,
        None,
        audit_details,
        &response,
    );
    Json(response)
}

async fn usdc_dex_transfer(
    State(state): State<FrontendAppState>,
    Json(payload): Json<UsdcDexTransferOptions>,
) -> Json<ApiResult<UsdcDexTransferResult>> {
    let audit_account_id = Some(payload.account_id.clone());
    let audit_details = json!({
        "account_id": payload.account_id.clone(),
        "destination_account_id": payload.destination_account_id.clone(),
        "amount_usdc": payload.amount_usdc,
        "source_dex": payload.source_dex.clone(),
        "destination_dex": payload.destination_dex.clone(),
        "submit": payload.submit,
        "confirm_mainnet_live": payload.confirm_mainnet_live,
    });
    let result = async {
        let config = state.config_snapshot()?;
        let password = if payload.submit {
            validate_usdc_dex_transfer_gates(&config, &payload)?;
            state.audit_attempt(
                "usdc_dex_transfer_submit_attempt",
                Some(payload.account_id.clone()),
                None,
                audit_details.clone(),
            )?;
            let path = PathBuf::from(&config.secrets.vault_path);
            Some(state.resolve_vault_password(&path, "")?)
        } else {
            None
        };
        execute_usdc_dex_transfer(config, payload, password.as_deref()).await
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "usdc_dex_transfer",
        audit_account_id,
        None,
        audit_details,
        &response,
    );
    Json(response)
}

async fn usdc_dex_transfer_readiness(
    State(state): State<FrontendAppState>,
    Json(payload): Json<UsdcDexTransferOptions>,
) -> Json<ApiResult<UsdcDexTransferReadinessResponse>> {
    let audit_account_id = Some(payload.account_id.clone());
    let audit_details = json!({
        "account_id": payload.account_id.clone(),
        "destination_account_id": payload.destination_account_id.clone(),
        "amount_usdc": payload.amount_usdc,
        "source_dex": payload.source_dex.clone(),
        "destination_dex": payload.destination_dex.clone(),
        "confirm_mainnet_live": payload.confirm_mainnet_live,
    });
    let result = async {
        let config = state.config_snapshot()?;
        build_usdc_dex_transfer_readiness(&state, &config, payload).await
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "usdc_dex_transfer_readiness",
        audit_account_id,
        None,
        audit_details,
        &response,
    );
    Json(response)
}

async fn usdc_dex_transfer_runbook(
    State(state): State<FrontendAppState>,
    Json(payload): Json<UsdcDexTransferOptions>,
) -> Json<ApiResult<UsdcDexTransferRunbookResult>> {
    let audit_account_id = Some(payload.account_id.clone());
    let audit_details = json!({
        "account_id": payload.account_id.clone(),
        "destination_account_id": payload.destination_account_id.clone(),
        "amount_usdc": payload.amount_usdc,
        "source_dex": payload.source_dex.clone(),
        "destination_dex": payload.destination_dex.clone(),
        "submit": payload.submit,
        "confirm_mainnet_live": payload.confirm_mainnet_live,
    });
    let audit_action = if payload.submit {
        "usdc_dex_transfer_runbook_submit"
    } else {
        "usdc_dex_transfer_runbook_plan"
    };
    let result = async {
        let config = state.config_snapshot()?;
        if payload.submit {
            state.audit_attempt(
                "usdc_dex_transfer_runbook_submit_attempt",
                Some(payload.account_id.clone()),
                None,
                audit_details.clone(),
            )?;
        }
        let vault_path = PathBuf::from(&config.secrets.vault_path);
        let password = state.unlocked_vault_password(&vault_path)?;
        execute_usdc_dex_transfer_runbook(config, payload, password.as_deref()).await
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        audit_action,
        audit_account_id,
        None,
        audit_details,
        &response,
    );
    Json(response)
}

async fn usdc_dex_transfer_batch(
    State(state): State<FrontendAppState>,
    Json(payload): Json<UsdcDexTransferBatchPayload>,
) -> Json<ApiResult<UsdcDexTransferBatchResponse>> {
    let audit_details = json!({
        "account_ids": payload.account_ids.clone(),
        "destination_account_id": payload.destination_account_id.clone(),
        "amount_usdc": payload.amount_usdc,
        "source_dex": payload.source_dex.clone(),
        "destination_dex": payload.destination_dex.clone(),
        "submit": payload.submit,
        "confirm_mainnet_live": payload.confirm_mainnet_live,
    });
    let result = async {
        anyhow::ensure!(
            !payload.submit,
            "batch USDC transfer submit is disabled; use per-account runbook submit for live transfers"
        );
        let config = state.config_snapshot()?;
        let account_ids = selected_enabled_account_ids(&config, &payload.account_ids);
        validate_batch_account_count("USDC transfer batch plan", &config, &account_ids)?;

        let transfer_futures = account_ids.iter().map(|account_id| {
            let options = payload.for_account(account_id.clone());
            let config = config.clone();
            async move {
                ApiResult::from_result(execute_usdc_dex_transfer(config, options, None).await)
            }
        });
        let results = join_all(transfer_futures).await;

        let planned_account_ids = results
            .iter()
            .filter_map(|result| result.data.as_ref().map(|plan| plan.account_id.clone()))
            .collect::<Vec<_>>();
        let failed_account_ids = results
            .iter()
            .enumerate()
            .filter(|(_, result)| !result.ok)
            .filter_map(|(index, _)| account_ids.get(index).cloned())
            .collect::<Vec<_>>();

        Ok(UsdcDexTransferBatchResponse {
            environment: config.app.environment.clone(),
            dex: config.hyperliquid.dex.clone(),
            account_ids,
            destination_account_id: payload.destination_account_id.clone(),
            planned_account_ids,
            failed_account_ids,
            amount_usdc: payload.amount_usdc,
            source_dex: normalize_transfer_layer_value(
                payload.source_dex.as_deref().unwrap_or_default(),
            ),
            destination_dex: payload
                .destination_dex
                .as_deref()
                .map(normalize_transfer_layer_value)
                .unwrap_or_else(|| normalize_transfer_layer_value(&config.hyperliquid.dex)),
            submit_requested: false,
            submitted: false,
            results,
        })
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "usdc_dex_transfer_batch",
        None,
        None,
        audit_details,
        &response,
    );
    Json(response)
}

async fn usdc_dex_transfer_readiness_batch(
    State(state): State<FrontendAppState>,
    Json(payload): Json<UsdcDexTransferBatchPayload>,
) -> Json<ApiResult<UsdcDexTransferReadinessBatchResponse>> {
    let audit_details = json!({
        "account_ids": payload.account_ids.clone(),
        "destination_account_id": payload.destination_account_id.clone(),
        "amount_usdc": payload.amount_usdc,
        "source_dex": payload.source_dex.clone(),
        "destination_dex": payload.destination_dex.clone(),
        "confirm_mainnet_live": payload.confirm_mainnet_live,
    });
    let result = async {
        let config = state.config_snapshot()?;
        let account_ids = selected_enabled_account_ids(&config, &payload.account_ids);
        validate_batch_account_count("USDC transfer readiness batch", &config, &account_ids)?;

        let readiness_futures = account_ids.iter().map(|account_id| {
            let options = payload.for_account(account_id.clone());
            let state = state.clone();
            let config = config.clone();
            async move { build_usdc_dex_transfer_readiness(&state, &config, options).await }
        });
        let results = join_all(readiness_futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let ready_account_ids = results
            .iter()
            .filter(|result| result.ready_for_testnet_transfer || result.ready_for_mainnet_transfer)
            .map(|result| result.account_id.clone())
            .collect::<Vec<_>>();
        let blocked_account_ids = results
            .iter()
            .filter(|result| {
                !result.ready_for_testnet_transfer && !result.ready_for_mainnet_transfer
            })
            .map(|result| result.account_id.clone())
            .collect::<Vec<_>>();

        Ok(UsdcDexTransferReadinessBatchResponse {
            environment: config.app.environment.clone(),
            dex: config.hyperliquid.dex.clone(),
            account_ids,
            destination_account_id: payload.destination_account_id.clone(),
            ready_account_ids,
            blocked_account_ids,
            amount_usdc: payload.amount_usdc,
            source_dex: normalize_transfer_layer_value(
                payload.source_dex.as_deref().unwrap_or_default(),
            ),
            destination_dex: payload
                .destination_dex
                .as_deref()
                .map(normalize_transfer_layer_value)
                .unwrap_or_else(|| normalize_transfer_layer_value(&config.hyperliquid.dex)),
            results,
        })
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "usdc_dex_transfer_readiness_batch",
        None,
        None,
        audit_details,
        &response,
    );
    Json(response)
}

async fn mainnet_smoke_plan(
    State(state): State<FrontendAppState>,
    Json(payload): Json<MainnetSmokePlanPayload>,
) -> Json<ApiResult<MainnetSmokePlanResult>> {
    let audit_details = json!({
        "account_ids": payload.account_ids.clone(),
        "funding_amount_usdc": payload.funding_amount_usdc,
        "destination_dex": payload.destination_dex.clone(),
        "coin": payload.coin.clone(),
        "side": payload.side.clone(),
        "order_notional_usd": payload.order_notional_usd,
        "max_slippage_bps": payload.max_slippage_bps,
        "execution_mode": payload.execution_mode.clone(),
    });
    let result = async {
        let config = state.config_snapshot()?;
        let account_ids = selected_enabled_account_ids(&config, &payload.account_ids);
        validate_batch_account_count("mainnet smoke plan", &config, &account_ids)?;
        let side = parse_side(&payload.side)?;
        let execution_mode = parse_execution_mode(&payload.execution_mode)?;
        let vault_path = PathBuf::from(&config.secrets.vault_path);
        let password = state.unlocked_vault_password(&vault_path)?;
        build_mainnet_smoke_plan(
            state.config_path.clone(),
            config,
            MainnetSmokePlanOptions {
                account_ids,
                funding_amount_usdc: payload.funding_amount_usdc,
                destination_dex: payload.destination_dex,
                coin: payload.coin,
                side,
                order_notional_usd: payload.order_notional_usd,
                max_slippage_bps: payload.max_slippage_bps,
                execution_mode,
                transfer_output_config_path: PathBuf::from(
                    ".codex-longrun/mainnet-usdc-transfer-window.toml",
                ),
                order_output_config_path: PathBuf::from(
                    ".codex-longrun/mainnet-order-live-window.toml",
                ),
            },
            password.as_deref(),
        )
        .await
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result("mainnet_smoke_plan", None, None, audit_details, &response);
    Json(response)
}

async fn reconcile_account(
    State(state): State<FrontendAppState>,
    Json(payload): Json<AccountReconciliationPayload>,
) -> Json<ApiResult<AccountReconciliationResponse>> {
    let audit_account_id = Some(payload.account_id.clone());
    let audit_details = json!({ "account_id": payload.account_id.clone() });
    let result = async {
        let config = state.config_snapshot()?;
        let account = config
            .account(&payload.account_id)
            .cloned()
            .with_context(|| format!("account {} not found in config", payload.account_id))?;
        let open_orders = fetch_open_orders(
            &config.app.environment,
            &config.hyperliquid.dex,
            &account.address,
        )
        .await
        .context("failed to fetch open orders")?;
        let fills = fetch_user_fills(
            &config.app.environment,
            &config.hyperliquid.dex,
            &account.address,
        )
        .await
        .context("failed to fetch user fills")?;
        let rate_limit = fetch_user_rate_limit(&config.app.environment, &account.address)
            .await
            .context("failed to fetch user rate limit")?;
        Ok(AccountReconciliationResponse {
            environment: config.app.environment.clone(),
            dex: config.hyperliquid.dex.clone(),
            account_id: account.account_id.clone(),
            address: account.address.clone(),
            rate_limit,
            open_order_count: open_orders.len(),
            fill_count: fills.len(),
            open_orders,
            recent_fills: fills,
        })
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "reconcile_account",
        audit_account_id,
        None,
        audit_details,
        &response,
    );
    Json(response)
}

async fn order_status(
    State(state): State<FrontendAppState>,
    Json(payload): Json<OrderStatusPayload>,
) -> Json<ApiResult<OrderStatusQueryResponse>> {
    let audit_account_id = Some(payload.account_id.clone());
    let audit_details = json!({
        "account_id": payload.account_id.clone(),
        "oid": payload.oid,
        "cloid_present": payload.cloid.as_deref().is_some_and(|cloid| !cloid.trim().is_empty()),
    });
    let result = async {
        let config = state.config_snapshot()?;
        let account = config
            .account(&payload.account_id)
            .cloned()
            .with_context(|| format!("account {} not found in config", payload.account_id))?;
        let query = payload.query()?;
        let order_status = match &query {
            OrderStatusQuery::Oid { oid } => {
                fetch_order_status_by_oid(&config.app.environment, &account.address, *oid).await
            }
            OrderStatusQuery::Cloid { cloid } => {
                fetch_order_status_by_cloid(&config.app.environment, &account.address, cloid).await
            }
        }
        .context("failed to fetch order status")?;

        Ok(OrderStatusQueryResponse {
            environment: config.app.environment.clone(),
            dex: config.hyperliquid.dex.clone(),
            account_id: account.account_id.clone(),
            address: account.address.clone(),
            query,
            order_status,
        })
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "order_status",
        audit_account_id,
        None,
        audit_details,
        &response,
    );
    Json(response)
}

async fn cancel_by_cloid(
    State(state): State<FrontendAppState>,
    Json(payload): Json<CancelByCloidPayload>,
) -> Json<ApiResult<CancelByCloidResult>> {
    let audit_details = json!({
        "market": payload.market.clone().unwrap_or_else(|| MARKET_XYZ_PERP.to_string()),
        "coin": payload.coin.clone(),
        "cloid": payload.cloid.clone(),
        "confirm_mainnet_live": payload.confirm_mainnet_live,
    });
    let audit_account_id = Some(payload.account_id.clone());
    let audit_coin = Some(payload.coin.clone());
    let result = async {
        let base_config = state.config_snapshot()?;
        let market = payload.market_profile(&base_config)?;
        ensure_accounts_allowed_for_market(
            &base_config,
            std::slice::from_ref(&payload.account_id),
            market.id,
        )?;
        let config = scoped_config_for_module_and_market(base_config, "manual", &market);
        validate_live_action_gates(&config, payload.confirm_mainnet_live)?;
        let account = config
            .account(&payload.account_id)
            .with_context(|| format!("account {} not found in config", payload.account_id))?;
        anyhow::ensure!(
            account.enabled && account.worker_enabled,
            "account {} is not enabled for worker execution",
            account.account_id
        );
        ensure_live_account_address(account)?;
        let canonical_coin = normalize_coin_for_market(&market, &payload.coin);
        state.audit_attempt(
            "cancel_by_cloid_attempt",
            Some(payload.account_id.clone()),
            Some(canonical_coin.clone()),
            audit_details.clone(),
        )?;
        let path = PathBuf::from(&config.secrets.vault_path);
        let password = state.resolve_vault_password(&path, "")?;
        execute_cancel_by_cloid(
            config,
            payload.account_id,
            canonical_coin,
            payload.cloid,
            payload.confirm_mainnet_live,
            &password,
        )
        .await
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "cancel_by_cloid",
        audit_account_id,
        audit_coin,
        audit_details,
        &response,
    );
    Json(response)
}

async fn fib_preview(
    State(state): State<FrontendAppState>,
    Json(payload): Json<FibPreviewPayload>,
) -> Json<ApiResult<FibPreviewResponse>> {
    let audit_details = json!({
        "strategy_id": payload.strategy_id.clone(),
        "direction": payload.direction.clone().unwrap_or_else(|| "long".to_string()),
        "market": payload.market.clone().unwrap_or_else(|| MARKET_XYZ_PERP.to_string()),
        "coin": payload.coin.clone(),
        "timeframe": payload.timeframe.clone(),
        "notional_usd": payload.notional_usd,
        "levels": payload.levels.clone(),
    });
    let audit_coin = Some(payload.coin.clone());
    let result = state.config_snapshot().and_then(|base_config| {
        let market = payload.market_profile(&base_config)?;
        anyhow::ensure!(
            !market.is_spot(),
            "spot market fib preview is not implemented in this release"
        );
        let scoped = scoped_config_for_module_and_market(base_config, "fib", &market);
        let canonical_coin = normalize_dex_coin(&market.dex, &payload.coin);
        anyhow::ensure!(
            scoped.symbol_allowed_for_module("fib", &canonical_coin),
            "fib symbol {} is blocked",
            canonical_coin
        );
        let mut payload = payload;
        payload.coin = canonical_coin;
        payload.try_into_strategy().map(|strategy| {
            let plans = strategy
                .level_plan()
                .into_iter()
                .map(|plan| FibLevelResponse {
                    level: plan.level,
                    entry_price: plan.entry_price,
                    take_profit_price: plan.take_profit_price,
                    stop_loss_price: plan.stop_loss_price,
                })
                .collect::<Vec<_>>();
            FibPreviewResponse {
                dry_run: state.dry_run,
                levels: plans,
            }
        })
    });
    let response = ApiResult::from_result(result);
    state.audit_api_result("fib_preview", None, audit_coin, audit_details, &response);
    Json(response)
}

async fn fib_auto_detect(
    State(state): State<FrontendAppState>,
    Json(payload): Json<FibAutoDetectPayload>,
) -> Json<ApiResult<FibAutoDetectResponse>> {
    let audit_details = json!({
        "direction": payload.direction.clone().unwrap_or_else(|| "long".to_string()),
        "market": payload.market.clone().unwrap_or_else(|| MARKET_XYZ_PERP.to_string()),
        "coin": payload.coin.clone(),
        "timeframe": payload.timeframe.clone(),
        "lookback_bars": payload.lookback_bars,
        "levels": payload.levels.clone(),
        "entry_tolerance_usd": payload.entry_tolerance_usd,
    });
    let audit_coin = Some(payload.coin.clone());
    let result = async {
        let base_config = state.config_snapshot()?;
        let market = payload.market_profile(&base_config)?;
        let config = scoped_config_for_module_and_market(base_config, "fib", &market);
        let canonical_coin = normalize_coin_for_market(&market, &payload.coin);
        anyhow::ensure!(
            config.symbol_allowed_for_module("fib", &canonical_coin),
            "fib symbol {} is blocked",
            canonical_coin
        );
        let mut payload = payload;
        payload.coin = canonical_coin;
        build_fib_auto_detect_response(&config, payload).await
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "fib_auto_detect",
        None,
        audit_coin,
        audit_details,
        &response,
    );
    Json(response)
}

async fn fib_ws_candle_probe(
    State(state): State<FrontendAppState>,
    Json(payload): Json<FibWsCandleProbePayload>,
) -> Json<ApiResult<crate::hyperliquid::WsCandleProbe>> {
    let audit_details = json!({
        "market": payload.market.clone().unwrap_or_else(|| MARKET_XYZ_PERP.to_string()),
        "coin": payload.coin.clone(),
        "timeframe": payload.timeframe.clone(),
        "timeout_ms": payload.timeout_ms,
    });
    let audit_coin = Some(payload.coin.clone());
    let result = async {
        let base_config = state.config_snapshot()?;
        let market = resolve_market_profile(payload.market.as_deref(), &base_config)?;
        let config = scoped_config_for_module_and_market(base_config, "fib", &market);
        let canonical_coin = normalize_coin_for_market(&market, &payload.coin);
        anyhow::ensure!(
            config.symbol_allowed_for_module("fib", &canonical_coin),
            "fib symbol {} is blocked",
            canonical_coin
        );
        let timeframe = payload.timeframe.trim().to_ascii_lowercase();
        timeframe_interval_ms(&timeframe)?;
        let candle_coin = if market.is_spot() {
            fetch_spot_market_snapshot_cached(
                &config.app.environment,
                MARKET_SNAPSHOT_QUOTE_CACHE_TTL_MS,
            )
            .await
            .context("failed to fetch spot market snapshot for websocket candle symbol")?
            .candle_coin(&canonical_coin)?
        } else {
            canonical_coin
        };
        fetch_ws_candle_probe(
            &config.app.environment,
            &candle_coin,
            &timeframe,
            payload.timeout_ms.unwrap_or(5_000),
        )
        .await
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "fib_ws_candle_probe",
        None,
        audit_coin,
        audit_details,
        &response,
    );
    Json(response)
}

async fn fib_instances(
    State(state): State<FrontendAppState>,
) -> Json<ApiResult<FibInstancesResponse>> {
    let result = state
        .fib_instances
        .read()
        .map_err(|_| anyhow::anyhow!("fib instance lock is poisoned"))
        .map(|instances| {
            let mut instances = instances.values().cloned().collect::<Vec<_>>();
            instances.sort_by_key(|record| std::cmp::Reverse(record.updated_at_ms));
            FibInstancesResponse {
                instances,
                fetched_at_ms: now_ms(),
            }
        });
    Json(ApiResult::from_result(result))
}

async fn fib_history(State(state): State<FrontendAppState>) -> Json<ApiResult<FibHistoryResponse>> {
    Json(ApiResult::from_result(build_fib_history_response(
        &state,
        FIB_HISTORY_RESPONSE_LIMIT,
    )))
}

async fn fib_instance_create(
    State(state): State<FrontendAppState>,
    Json(payload): Json<FibBasicPayload>,
) -> Json<ApiResult<FibInstanceActionResponse>> {
    let audit_details = payload.audit_details("create");
    let audit_coin = Some(payload.coin.clone());
    let result = async {
        let record =
            build_fib_instance_record(&state, payload, FibInstanceStatus::Draft, None).await?;
        ensure_fib_pair_available(&state, &record, None)?;
        upsert_fib_instance(&state, record.clone())?;
        Ok(FibInstanceActionResponse {
            action: "create".to_string(),
            instance: record,
            entry_signals: Vec::new(),
            entry_reports: Vec::new(),
            cancel_reports: Vec::new(),
            protection_reports: Vec::new(),
            ai_proposals: Vec::new(),
        })
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "fib_instance_create",
        None,
        audit_coin,
        audit_details,
        &response,
    );
    Json(response)
}

async fn fib_instance_start(
    State(state): State<FrontendAppState>,
    Json(payload): Json<FibBasicPayload>,
) -> Json<ApiResult<FibInstanceActionResponse>> {
    let audit_details = payload.audit_details("start");
    let audit_coin = Some(payload.coin.clone());
    let result = async {
        let dry_run_requested = payload.dry_run;
        let live_requested = payload.live;
        let mut record =
            build_fib_instance_record(&state, payload, FibInstanceStatus::ArmedUnfilled, None)
                .await?;
        record.config.validate_execution()?;
        state.clear_fib_stop_request(&record.strategy_id)?;
        ensure_fib_pair_available(&state, &record, None)?;
        let coordinator_signals = fib_coordinator_signals_from_plan(&record.config, &record.plan)?;
        let entry_signals =
            fib_entry_signal_responses_from_signals(&coordinator_signals, &record.plan);
        record.entry_signal_ids = entry_signals
            .iter()
            .map(|signal| signal.signal_id.clone())
            .collect();
        let (entry_reports, protection_reports) = execute_fib_entry_signals(
            &state,
            &record,
            &coordinator_signals,
            dry_run_requested,
            live_requested,
        )
        .await?;
        let has_live_fill = entry_reports.iter().any(worker_report_has_live_fill);
        record.entry_order_refs =
            fib_entry_order_refs_from_reports(&coordinator_signals, &entry_reports, &record.plan);
        record.protective_order_refs = fib_protective_order_refs_from_reports(&protection_reports);
        let mut cancel_reports = Vec::new();
        let sync = fib_entry_sync_assessment(&coordinator_signals, &entry_reports);
        if !sync.is_complete() {
            let resting_refs = fib_resting_entry_order_refs_from_reports(
                &coordinator_signals,
                &entry_reports,
                &record.plan,
            );
            cancel_reports = cancel_incomplete_fib_entry_orders(
                &state,
                &record,
                resting_refs,
                dry_run_requested,
                live_requested,
            )
            .await?;
            remove_successfully_cancelled_fib_entry_refs(&mut record, &cancel_reports);
            mark_fib_record_incomplete_entry_submission(
                &mut record,
                &sync,
                &entry_reports,
                has_live_fill,
                &cancel_reports,
            );
        } else {
            record.status =
                if has_live_fill && fib_all_target_accounts_have_complete_protection(&record) {
                    FibInstanceStatus::Protected
                } else if has_live_fill && !protection_reports.is_empty() {
                    FibInstanceStatus::ProtectionPending
                } else if has_live_fill {
                    FibInstanceStatus::EntryFilled
                } else if entry_signals.is_empty() {
                    FibInstanceStatus::ArmedUnfilled
                } else {
                    FibInstanceStatus::EntryPending
                };
            record.last_message = Some(if entry_signals.is_empty() {
                fib_waiting_for_entry_message(&record)
            } else if entry_reports.is_empty() {
                format!(
                    "strategy started with {} planned entry signal(s)",
                    entry_signals.len()
                )
            } else {
                fib_execution_message("strategy started", &entry_reports, &protection_reports)
            });
        }
        record.updated_at_ms = now_ms();
        upsert_fib_instance(&state, record.clone())?;
        Ok(FibInstanceActionResponse {
            action: "start".to_string(),
            instance: record,
            entry_signals,
            entry_reports,
            cancel_reports,
            protection_reports,
            ai_proposals: Vec::new(),
        })
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "fib_instance_start",
        None,
        audit_coin,
        audit_details,
        &response,
    );
    Json(response)
}

async fn fib_instance_refresh_params(
    State(state): State<FrontendAppState>,
    Json(payload): Json<FibBasicPayload>,
) -> Json<ApiResult<FibInstanceActionResponse>> {
    let audit_details = payload.audit_details("refresh_params");
    let audit_coin = Some(payload.coin.clone());
    let result = async {
        let dry_run_requested = payload.dry_run;
        let live_requested = payload.live;
        let strategy_id = payload
            .strategy_id
            .clone()
            .unwrap_or_else(|| payload.default_strategy_id());
        let previous = state
            .fib_instances
            .read()
            .map_err(|_| anyhow::anyhow!("fib instance lock is poisoned"))?
            .get(&strategy_id)
            .cloned();
        let previous_status = previous
            .as_ref()
            .map(|record| record.status)
            .unwrap_or(FibInstanceStatus::Draft);
        let bought = matches!(
            previous_status,
            FibInstanceStatus::EntryFilled
                | FibInstanceStatus::ProtectionPending
                | FibInstanceStatus::Protected
                | FibInstanceStatus::Exiting
        );
        let next_status = if bought {
            FibInstanceStatus::ProtectionPending
        } else {
            FibInstanceStatus::ArmedUnfilled
        };
        let mut record =
            build_fib_instance_record(&state, payload, next_status, previous.as_ref()).await?;
        record.config.validate_execution()?;
        ensure_fib_pair_available(&state, &record, Some(&strategy_id))?;
        let coordinator_signals = if bought {
            Vec::new()
        } else {
            fib_coordinator_signals_from_plan(&record.config, &record.plan)?
        };
        let entry_signals =
            fib_entry_signal_responses_from_signals(&coordinator_signals, &record.plan);
        record.entry_signal_ids = entry_signals
            .iter()
            .map(|signal| signal.signal_id.clone())
            .collect();
        let mut cancel_reports = if bought {
            Vec::new()
        } else {
            cancel_fib_entry_order_refs(
                &state,
                previous.as_ref(),
                dry_run_requested,
                live_requested,
            )
            .await?
        };
        if let Some(failed) = cancel_reports.iter().find(|report| !report.ok) {
            anyhow::bail!(
                failed.error.clone().unwrap_or_else(|| format!(
                    "failed to cancel Fib entry order {}",
                    failed.cloid
                ))
            );
        }
        let (entry_reports, protection_reports) = if bought {
            (Vec::new(), Vec::new())
        } else {
            execute_fib_entry_signals(
                &state,
                &record,
                &coordinator_signals,
                dry_run_requested,
                live_requested,
            )
            .await?
        };
        let has_live_fill = entry_reports.iter().any(worker_report_has_live_fill);
        if !bought {
            record.entry_order_refs = fib_entry_order_refs_from_reports(
                &coordinator_signals,
                &entry_reports,
                &record.plan,
            );
            record.protective_order_refs =
                fib_protective_order_refs_from_reports(&protection_reports);
        }
        let sync = fib_entry_sync_assessment(&coordinator_signals, &entry_reports);
        if !bought && !sync.is_complete() {
            let resting_refs = fib_resting_entry_order_refs_from_reports(
                &coordinator_signals,
                &entry_reports,
                &record.plan,
            );
            let partial_cancel_reports = cancel_incomplete_fib_entry_orders(
                &state,
                &record,
                resting_refs,
                dry_run_requested,
                live_requested,
            )
            .await?;
            cancel_reports.extend(partial_cancel_reports);
            remove_successfully_cancelled_fib_entry_refs(&mut record, &cancel_reports);
            mark_fib_record_incomplete_entry_submission(
                &mut record,
                &sync,
                &entry_reports,
                has_live_fill,
                &cancel_reports,
            );
        } else {
            if has_live_fill && fib_all_target_accounts_have_complete_protection(&record) {
                record.status = FibInstanceStatus::Protected;
            } else if has_live_fill && !protection_reports.is_empty() {
                record.status = FibInstanceStatus::ProtectionPending;
            } else if has_live_fill {
                record.status = FibInstanceStatus::EntryFilled;
            } else if !bought && !entry_signals.is_empty() {
                record.status = FibInstanceStatus::EntryPending;
            }
            record.last_message = Some(if bought {
                "parameters refreshed after entry; only TP/SL should be replaced, no new entry order"
                    .to_string()
            } else if entry_signals.is_empty() {
                format!("parameters refreshed; {}", fib_waiting_for_entry_message(&record))
            } else if entry_reports.is_empty() {
                "parameters refreshed before entry; pending entries should be replaced by the new plan"
                    .to_string()
            } else {
                fib_execution_message(
                    "parameters refreshed before entry",
                    &entry_reports,
                    &protection_reports,
                )
            });
        }
        record.updated_at_ms = now_ms();
        upsert_fib_instance(&state, record.clone())?;
        Ok(FibInstanceActionResponse {
            action: "refresh_params".to_string(),
            instance: record,
            entry_signals,
            entry_reports,
            cancel_reports,
            protection_reports,
            ai_proposals: Vec::new(),
        })
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "fib_instance_refresh_params",
        None,
        audit_coin,
        audit_details,
        &response,
    );
    Json(response)
}

async fn fib_instance_cancel(
    State(state): State<FrontendAppState>,
    Json(payload): Json<FibCancelPayload>,
) -> Json<ApiResult<FibInstanceActionResponse>> {
    let audit_details = json!({
        "strategy_id": payload.strategy_id.clone(),
        "dry_run": payload.dry_run,
        "live": payload.live,
    });
    let result = async {
        state.record_fib_stop_request(&payload.strategy_id)?;
        let previous = state
            .fib_instances
            .read()
            .map_err(|_| anyhow::anyhow!("fib instance lock is poisoned"))?
            .get(&payload.strategy_id)
            .cloned()
            .with_context(|| format!("fib strategy {} not found", payload.strategy_id))?;
        let cancel_reports =
            cancel_fib_entry_order_refs(&state, Some(&previous), payload.dry_run, payload.live)
                .await?;
        let mut record = previous;
        record.status = FibInstanceStatus::Killed;
        let successful_cancel_cloids = cancel_reports
            .iter()
            .filter(|report| report.ok)
            .map(|report| report.cloid.clone())
            .collect::<HashSet<_>>();
        if payload.live && !payload.dry_run {
            record
                .entry_order_refs
                .retain(|order_ref| !successful_cancel_cloids.contains(&order_ref.cloid));
        } else {
            record.entry_order_refs.clear();
        }
        record.config.auto_loop = false;
        let failed_count = cancel_reports.iter().filter(|report| !report.ok).count();
        record.last_message = Some(if failed_count == 0 {
            "strategy stopped; pending Fib entry orders were cancelled or marked inactive"
                .to_string()
        } else {
            format!(
                "strategy stopped; {} Fib entry cancel request(s) failed and remain visible for retry",
                failed_count
            )
        });
        record.updated_at_ms = now_ms();
        upsert_fib_instance(&state, record.clone())?;
        Ok(FibInstanceActionResponse {
            action: "cancel".to_string(),
            instance: record,
            entry_signals: Vec::new(),
            entry_reports: Vec::new(),
            cancel_reports,
            protection_reports: Vec::new(),
            ai_proposals: Vec::new(),
        })
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result("fib_instance_cancel", None, None, audit_details, &response);
    Json(response)
}

async fn fib_ai_proposals(
    State(state): State<FrontendAppState>,
    Json(payload): Json<FibAiProposalPayload>,
) -> Json<ApiResult<FibInstanceActionResponse>> {
    let audit_details = json!({
        "market": payload.market.clone().unwrap_or_else(|| MARKET_XYZ_PERP.to_string()),
        "coin": payload.coin.clone(),
        "timeframe": payload.timeframe.clone(),
        "lookback_bars": payload.lookback_bars,
        "mode": payload.mode.clone(),
    });
    let audit_coin = Some(payload.coin.clone());
    let result = async {
        anyhow::ensure!(
            payload.mode.trim().eq_ignore_ascii_case("observe")
                || payload.mode.trim().eq_ignore_ascii_case("suggest"),
            "AI Auto mode is intentionally disabled in this release"
        );
        let mode = payload.mode.clone();
        let market_id = payload
            .market
            .clone()
            .unwrap_or_else(|| MARKET_XYZ_PERP.to_string());
        let base_config = state.config_snapshot()?;
        let market = resolve_market_profile(payload.market.as_deref(), &base_config)?;
        let scoped = scoped_config_for_module_and_market(base_config, "fib", &market);
        let detect = build_fib_auto_detect_response(
            &scoped,
            FibAutoDetectPayload {
                direction: payload.direction.clone(),
                coin: payload.coin.clone(),
                timeframe: payload.timeframe.clone(),
                lookback_bars: payload.lookback_bars,
                levels: payload.levels.clone(),
                entry_tolerance_usd: payload.entry_tolerance_usd,
                take_profit_usd: payload.take_profit_value,
                stop_loss_pct: (payload.stop_loss_value / 100.0) / payload.leverage.max(1.0),
                market: payload.market.clone(),
            },
        )
        .await?;
        let nearest_distance = detect
            .nearest_level
            .as_ref()
            .map(|level| level.distance_usd)
            .unwrap_or(f64::INFINITY);
        let confidence = if detect.triggered {
            0.72
        } else if nearest_distance <= payload.entry_tolerance_usd * 2.0 {
            0.58
        } else {
            0.42
        };
        let proposals = vec![FibAiProposalResponse {
            proposal_id: format!(
                "fib-ai-{}-{}",
                detect.coin.replace([':', '/'], "_"),
                now_ms()
            ),
            mode: mode.clone(),
            market: market.id.to_string(),
            direction: detect.direction,
            coin: detect.coin.clone(),
            timeframe: detect.timeframe.clone(),
            swing_high: detect.swing_high,
            swing_low: detect.swing_low,
            levels: detect.levels.iter().map(|level| level.level).collect(),
            confidence,
            reasons: vec![
                "ZigZag/Pivot scoring is reserved for the AI advanced phase".to_string(),
                "This proposal uses the basic swing-direction rule".to_string(),
                if detect.direction == FibTradeDirection::Short {
                    "Direction is short: the basic engine looks for a high-before-low downswing and rebound entry zones".to_string()
                } else {
                    "Direction is long: the basic engine looks for a low-before-high upswing and pullback entry zones".to_string()
                },
                format!(
                    "Nearest fib distance is {:.6} USD; triggered={}",
                    nearest_distance, detect.triggered
                ),
            ],
        }];
        let preview_config = FibBasicConfig {
            strategy_id: "ai_proposal_preview".to_string(),
            direction: detect.direction,
            market: market_id,
            dex: market.dex.to_string(),
            account_ids: Vec::new(),
            coin: detect.coin.clone(),
            timeframe: detect.timeframe.clone(),
            lookback_bars: payload.lookback_bars,
            swing_high: detect.swing_high,
            swing_low: detect.swing_low,
            current_price: detect.current_price,
            levels: detect.levels.iter().map(|level| level.level).collect(),
            entry_above_tolerance_usd: payload.entry_tolerance_usd,
            entry_below_tolerance_usd: payload.entry_tolerance_usd,
            principal_usd: payload.principal_usd,
            leverage: payload.leverage.max(1.0),
            execution_mode: ExecutionMode::Taker,
            take_profit_mode: FibProfitLossMode::PrincipalPercent,
            take_profit_value: payload.take_profit_value,
            stop_loss_mode: FibProfitLossMode::PrincipalPercent,
            stop_loss_value: payload.stop_loss_value,
            max_slippage_bps: payload.max_slippage_bps,
            max_entries_per_level: 1,
            cooldown_secs: 300,
            stop_loss_cooldown_secs: 300,
            stop_loss_stop_strategy: false,
            locked_range: false,
            auto_loop: false,
        };
        let preview_plan = build_basic_plan(&preview_config)?;
        Ok(FibInstanceActionResponse {
            action: "ai_proposals".to_string(),
            instance: FibInstanceRecord {
                strategy_id: "ai_proposal_preview".to_string(),
                status: FibInstanceStatus::Draft,
                config: preview_config,
                plan: preview_plan,
                dry_run: true,
                live: false,
                entry_signal_ids: Vec::new(),
                entry_order_refs: Vec::new(),
                protective_order_refs: Vec::new(),
                last_message: Some(
                    "AI advanced framework preview only; no order generated".to_string(),
                ),
                created_at_ms: now_ms(),
                updated_at_ms: now_ms(),
                completed_cycles: 0,
                last_cycle_completed_at_ms: None,
                last_cycle_exit_kind: None,
            },
            entry_signals: Vec::new(),
            entry_reports: Vec::new(),
            cancel_reports: Vec::new(),
            protection_reports: Vec::new(),
            ai_proposals: proposals,
        })
    }
    .await;
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "fib_ai_proposals",
        None,
        audit_coin,
        audit_details,
        &response,
    );
    Json(response)
}

async fn smart_money_preview(
    State(state): State<FrontendAppState>,
    Json(payload): Json<SmartMoneyPreviewPayload>,
) -> Json<ApiResult<SmartMoneyPreviewResponse>> {
    let audit_details = json!({
        "leader_id": payload.leader_id.clone(),
        "leader_address": payload.leader_address.clone(),
        "market": payload.market.clone().unwrap_or_else(|| MARKET_XYZ_PERP.to_string()),
        "coin": payload.coin.clone(),
        "side": payload.side.clone(),
        "leader_notional_usd": payload.leader_notional_usd,
        "copy_ratio": payload.copy_ratio,
        "max_signal_notional_usd": payload.max_signal_notional_usd,
        "reduce_only": payload.reduce_only,
    });
    let audit_coin = Some(payload.coin.clone());
    let result = state.config_snapshot().and_then(|base_config| {
        let market = payload.market_profile(&base_config)?;
        anyhow::ensure!(
            !market.is_spot(),
            "spot market smart-money preview is not implemented in this release"
        );
        let config = scoped_config_for_module_and_market(base_config, "copy", &market);
        let canonical_coin = normalize_dex_coin(&market.dex, &payload.coin);
        anyhow::ensure!(
            config.symbol_allowed_for_module("copy", &canonical_coin),
            "copy symbol {} is blocked",
            canonical_coin
        );
        let mut payload = payload;
        payload.coin = canonical_coin;
        payload.try_into_signal(&config).map(|signal| {
            let copied_notional_usd = signal
                .first()
                .map(|signal| signal.order.notional_usd)
                .unwrap_or_default();
            SmartMoneyPreviewResponse {
                dry_run: state.dry_run,
                signals: signal.len(),
                copied_notional_usd,
            }
        })
    });
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "smart_money_preview",
        None,
        audit_coin,
        audit_details,
        &response,
    );
    Json(response)
}

async fn vault_status_route(
    State(state): State<FrontendAppState>,
) -> Json<ApiResult<VaultSummary>> {
    let result = state.config_snapshot().and_then(|config| {
        let path = PathBuf::from(&config.secrets.vault_path);
        state.vault_summary(&path)
    });
    Json(ApiResult::from_result(result))
}

async fn vault_unlock(
    State(state): State<FrontendAppState>,
    Json(payload): Json<VaultUnlockPayload>,
) -> Json<ApiResult<VaultSummary>> {
    let result = state.config_snapshot().and_then(|config| {
        let path = PathBuf::from(&config.secrets.vault_path);
        unlock_vault(&path, &payload.password)
            .and_then(|summary| state.store_vault_session(path, payload.password, summary))
    });
    let response = ApiResult::from_result(result);
    state.audit_api_result("vault_unlock", None, None, json!({}), &response);
    Json(response)
}

async fn vault_change_password(
    State(state): State<FrontendAppState>,
    Json(payload): Json<VaultChangePasswordPayload>,
) -> Json<ApiResult<VaultSummary>> {
    let result = state.config_snapshot().and_then(|config| {
        let path = PathBuf::from(&config.secrets.vault_path);
        anyhow::ensure!(
            payload.new_password == payload.new_password_confirm,
            "new vault password confirmation does not match"
        );
        let current_password = state.resolve_vault_password(&path, &payload.current_password)?;
        let summary = change_vault_password(&path, &current_password, &payload.new_password)?;
        state.store_vault_session(path, payload.new_password, summary)
    });
    let response = ApiResult::from_result(result);
    state.audit_api_result("vault_change_password", None, None, json!({}), &response);
    Json(response)
}

async fn vault_upsert(
    State(state): State<FrontendAppState>,
    Json(payload): Json<VaultUpsertPayload>,
) -> Json<ApiResult<VaultSummary>> {
    let audit_details = json!({
        "account_id": payload.account_id.clone(),
        "secret_id": payload.secret_id.clone(),
        "address": payload.address.clone(),
    });
    let audit_account_id = Some(payload.account_id.clone());
    let result = state.config_snapshot().and_then(|config| {
        let path = PathBuf::from(&config.secrets.vault_path);
        payload
            .try_into_upsert(&config)
            .and_then(|(password, secret_usage, upsert)| {
                let password = state.resolve_vault_password(&path, &password)?;
                let summary = upsert_secret(&path, &password, upsert.clone())?;
                state.upsert_config_account(&upsert, secret_usage)?;
                state.store_vault_session(path, password, summary)
            })
    });
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "vault_upsert",
        audit_account_id,
        None,
        audit_details,
        &response,
    );
    Json(response)
}

async fn vault_check_secret(
    State(state): State<FrontendAppState>,
    Json(payload): Json<VaultSecretCheckPayload>,
) -> Json<ApiResult<VaultSecretCheckResponse>> {
    let audit_details = json!({
        "account_id": payload.account_id.clone(),
        "secret_id": payload.secret_id.clone(),
    });
    let audit_account_id = Some(payload.account_id.clone());
    let result = state.config_snapshot().and_then(|config| {
        let path = PathBuf::from(&config.secrets.vault_path);
        state
            .resolve_vault_password(&path, &payload.password)
            .and_then(|password| payload.try_check(&config, &path, &password))
    });
    let response = ApiResult::from_result(result);
    state.audit_api_result(
        "vault_check_secret",
        audit_account_id,
        None,
        audit_details,
        &response,
    );
    Json(response)
}

#[derive(Debug, Serialize)]
struct FrontendStateResponse {
    app: AppStatusResponse,
    symbol_policies: ModuleSymbolPoliciesStateResponse,
    accounts: Vec<AccountResponse>,
    workers: Vec<WorkerResponse>,
    positions: Vec<PositionResponse>,
    pnl: PnlResponse,
    strategies: Vec<StrategyResponse>,
    recent_events: Vec<EventResponse>,
}

impl FrontendStateResponse {
    async fn from_state(state: &FrontendAppState) -> Self {
        let config = state
            .config_snapshot()
            .unwrap_or_else(|_| AppConfig::default());
        let uptime_ms = now_ms().saturating_sub(state.started_at_ms);

        let mut recent_events = Vec::new();
        let mut accounts = Vec::new();
        let mut positions = Vec::new();

        for account in config.enabled_worker_accounts() {
            let state_market_id = frontend_perp_market_id_for_dex(&config.hyperliquid.dex);
            let account_state = if let Some(cached) = state
                .realtime
                .clearinghouse_state(state_market_id, &account.address)
            {
                Ok(cached)
            } else if state_market_id == MARKET_HL_PERP {
                fetch_default_clearinghouse_state(&config.app.environment, &account.address).await
            } else {
                fetch_clearinghouse_state(
                    &config.app.environment,
                    &config.hyperliquid.dex,
                    &account.address,
                )
                .await
            };
            match account_state {
                Ok(account_state) => {
                    let equity_usd = parse_decimal(&account_state.margin_summary.account_value);
                    let available_usdc = account_state
                        .withdrawable
                        .as_deref()
                        .map(parse_decimal)
                        .unwrap_or_default();
                    let account_positions =
                        positions_from_clearinghouse(account, &config, &account_state);
                    let unrealized_pnl_usd = account_positions
                        .iter()
                        .map(|position| position.pnl_usd)
                        .sum();
                    accounts.push(AccountResponse {
                        account_id: account.account_id.clone(),
                        address: account.address.clone(),
                        secret_id: account_secret_id(account),
                        transfer_secret_id: account.transfer_secret_id.clone(),
                        blocked_markets: account.normalized_blocked_markets(),
                        copy_ratio: account.copy_ratio,
                        max_order_notional_usd: account.max_order_notional_usd,
                        equity_usd,
                        available_usdc,
                        unrealized_pnl_usd,
                    });
                    positions.extend(account_positions);
                }
                Err(error) => {
                    tracing::warn!(
                        account_id = %account.account_id,
                        error = %error,
                        "failed to fetch account state for frontend snapshot"
                    );
                    accounts.push(AccountResponse {
                        account_id: account.account_id.clone(),
                        address: account.address.clone(),
                        secret_id: account_secret_id(account),
                        transfer_secret_id: account.transfer_secret_id.clone(),
                        blocked_markets: account.normalized_blocked_markets(),
                        copy_ratio: account.copy_ratio,
                        max_order_notional_usd: account.max_order_notional_usd,
                        equity_usd: 0.0,
                        available_usdc: 0.0,
                        unrealized_pnl_usd: 0.0,
                    });
                }
            }
        }

        match read_recent_audit_events(
            Path::new(&config.storage.audit_log_path),
            RECENT_AUDIT_SCAN_LIMIT,
        ) {
            Ok(audit_events) => {
                let mut xyz_events = Vec::new();
                let mut hl_events = Vec::new();
                let mut spot_events = Vec::new();
                let mut seen_keys = HashSet::new();
                for event in audit_events {
                    if let Some(key) = recent_event_dedupe_key(&event)
                        && !seen_keys.insert(key)
                    {
                        continue;
                    }
                    if let Some(trade_event) = render_trade_recent_event(&event) {
                        match normalize_market_id(&trade_event.market) {
                            Some(MARKET_XYZ_PERP)
                                if xyz_events.len() < RECENT_TRADE_EVENTS_PER_MARKET =>
                            {
                                xyz_events.push(trade_event);
                            }
                            Some(MARKET_HL_PERP)
                                if hl_events.len() < RECENT_TRADE_EVENTS_PER_MARKET =>
                            {
                                hl_events.push(trade_event);
                            }
                            Some(MARKET_SPOT)
                                if spot_events.len() < RECENT_TRADE_EVENTS_PER_MARKET =>
                            {
                                spot_events.push(trade_event);
                            }
                            _ => {}
                        }
                    }
                    if xyz_events.len() >= RECENT_TRADE_EVENTS_PER_MARKET
                        && hl_events.len() >= RECENT_TRADE_EVENTS_PER_MARKET
                        && spot_events.len() >= RECENT_TRADE_EVENTS_PER_MARKET
                    {
                        break;
                    }
                }
                recent_events.extend(xyz_events);
                recent_events.extend(hl_events);
                recent_events.extend(spot_events);
            }
            Err(error) => {
                tracing::warn!(error = %error, "failed to read audit log for frontend events");
            }
        }
        extend_realtime_fill_events(&mut recent_events, &config, state);
        recent_events = cap_recent_events_by_market(recent_events);

        let total_equity_usd = accounts.iter().map(|account| account.equity_usd).sum();
        let total_available_usdc = accounts.iter().map(|account| account.available_usdc).sum();
        let total_unrealized_pnl_usd = accounts
            .iter()
            .map(|account| account.unrealized_pnl_usd)
            .sum();

        let workers = accounts
            .iter()
            .map(|account| WorkerResponse {
                worker_id: format!("worker-{}", account.account_id),
                account_id: account.account_id.clone(),
                status: "ready".to_string(),
                last_signal_latency_ms: 0,
            })
            .collect::<Vec<_>>();

        Self {
            app: AppStatusResponse {
                name: config.app.name.clone(),
                environment: config.app.environment.clone(),
                dex: config.hyperliquid.dex.clone(),
                default_market: MARKET_XYZ_PERP.to_string(),
                dry_run: state.dry_run,
                uptime_ms,
                worker_count: workers.len(),
                max_manual_order_notional_usd: config.manual_ops.max_manual_order_notional_usd,
            },
            symbol_policies: ModuleSymbolPoliciesStateResponse {
                manual_blocked_symbols: config.module_blocked_symbols("manual").to_vec(),
                fib_blocked_symbols: config.module_blocked_symbols("fib").to_vec(),
                copy_blocked_symbols: config.module_blocked_symbols("copy").to_vec(),
            },
            accounts,
            workers,
            positions,
            pnl: PnlResponse {
                total_equity_usd,
                total_available_usdc,
                total_unrealized_pnl_usd,
                daily_realized_pnl_usd: 0.0,
            },
            strategies: vec![
                StrategyResponse {
                    strategy_id: "manual_ops".to_string(),
                    status: "enabled".to_string(),
                    signal_count: 0,
                },
                StrategyResponse {
                    strategy_id: "fib_retracement".to_string(),
                    status: "configured".to_string(),
                    signal_count: 0,
                },
                StrategyResponse {
                    strategy_id: "smart_money_copy".to_string(),
                    status: "configured".to_string(),
                    signal_count: 0,
                },
            ],
            recent_events,
        }
    }
}

fn positions_from_clearinghouse(
    account: &AccountConfig,
    config: &AppConfig,
    state: &ClearinghouseState,
) -> Vec<PositionResponse> {
    state
        .asset_positions
        .iter()
        .filter_map(|asset_position| {
            let position = &asset_position.position;
            if position.coin.trim().is_empty() {
                return None;
            }
            let size = parse_decimal(&position.szi);
            let pnl_usd = position
                .unrealized_pnl
                .as_deref()
                .map(parse_decimal)
                .unwrap_or_default();
            let entry_price = position
                .entry_px
                .as_deref()
                .map(parse_decimal)
                .unwrap_or_default();
            let position_value = position
                .position_value
                .as_deref()
                .map(parse_decimal)
                .unwrap_or_default();
            let mark_price = if size != 0.0 {
                (position_value / size.abs()).abs()
            } else {
                0.0
            };
            Some(PositionResponse {
                account_id: account.account_id.clone(),
                coin: normalize_dex_coin(&config.hyperliquid.dex, &position.coin),
                size,
                entry_price,
                mark_price,
                pnl_usd,
            })
        })
        .collect::<Vec<_>>()
}

fn parse_decimal(value: &str) -> f64 {
    value.parse::<f64>().unwrap_or_default()
}

fn parse_optional_decimal(value: Option<&str>) -> Option<f64> {
    value.and_then(|raw| raw.parse::<f64>().ok())
}

fn timeframe_interval_ms(raw: &str) -> Result<u64> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1m" => Ok(60_000),
        "3m" => Ok(180_000),
        "5m" => Ok(300_000),
        "15m" => Ok(900_000),
        "30m" => Ok(1_800_000),
        "1h" => Ok(3_600_000),
        "2h" => Ok(7_200_000),
        "4h" => Ok(14_400_000),
        "8h" => Ok(28_800_000),
        "12h" => Ok(43_200_000),
        "1d" => Ok(86_400_000),
        other => anyhow::bail!("unsupported timeframe interval: {other}"),
    }
}

fn normalize_fib_levels(levels: &[f64]) -> Result<Vec<f64>> {
    normalized_level_set(levels)
}

#[derive(Debug, Clone)]
struct FibSwingWindow {
    swing_high: f64,
    swing_low: f64,
    swing_high_time_ms: u64,
    swing_low_time_ms: u64,
}

fn infer_fib_swing(
    candles: &[crate::hyperliquid::CandleSnapshot],
    direction: FibTradeDirection,
) -> Result<FibSwingWindow> {
    anyhow::ensure!(
        candles.len() >= 2,
        "at least two candles are required for fib swing inference"
    );
    let parsed = candles
        .iter()
        .map(|candle| {
            let high = candle
                .h
                .parse::<f64>()
                .with_context(|| format!("invalid candle high {}", candle.h))?;
            let low = candle
                .l
                .parse::<f64>()
                .with_context(|| format!("invalid candle low {}", candle.l))?;
            let close = candle
                .c
                .parse::<f64>()
                .with_context(|| format!("invalid candle close {}", candle.c))?;
            anyhow::ensure!(
                high.is_finite() && low.is_finite() && close.is_finite(),
                "candle values must be finite"
            );
            Ok((candle.t, high, low, close))
        })
        .collect::<Result<Vec<_>>>()?;

    let mut best: Option<(usize, usize, f64, f64)> = None;
    match direction {
        FibTradeDirection::Long => {
            for (low_index, (_, _, low, _)) in parsed.iter().enumerate() {
                for (high_index, (_, high, _, _)) in parsed.iter().enumerate().skip(low_index + 1) {
                    if high <= low {
                        continue;
                    }
                    let range = high - low;
                    let replace = best
                        .as_ref()
                        .map(|(_, _, best_high, best_low)| range > best_high - best_low)
                        .unwrap_or(true);
                    if replace {
                        best = Some((low_index, high_index, *high, *low));
                    }
                }
            }
        }
        FibTradeDirection::Short => {
            for (high_index, (_, high, _, _)) in parsed.iter().enumerate() {
                for (low_index, (_, _, low, _)) in parsed.iter().enumerate().skip(high_index + 1) {
                    if high <= low {
                        continue;
                    }
                    let range = high - low;
                    let replace = best
                        .as_ref()
                        .map(|(_, _, best_high, best_low)| range > best_high - best_low)
                        .unwrap_or(true);
                    if replace {
                        best = Some((low_index, high_index, *high, *low));
                    }
                }
            }
        }
    }

    let Some((low_index, high_index, swing_high, swing_low)) = best else {
        match direction {
            FibTradeDirection::Long => {
                anyhow::bail!("failed to infer a valid long swing where low occurs before high")
            }
            FibTradeDirection::Short => {
                anyhow::bail!("failed to infer a valid short swing where high occurs before low")
            }
        }
    };
    Ok(FibSwingWindow {
        swing_high,
        swing_low,
        swing_high_time_ms: parsed[high_index].0,
        swing_low_time_ms: parsed[low_index].0,
    })
}

async fn fetch_fib_reference_price(
    config: &AppConfig,
    market: &MarketProfile,
    coin: &str,
    realtime: Option<&RealtimeState>,
) -> Result<f64> {
    if market.is_spot() {
        let snapshot = fetch_spot_market_snapshot_cached(
            &config.app.environment,
            MARKET_SNAPSHOT_QUOTE_CACHE_TTL_MS,
        )
        .await
        .context("failed to fetch spot market snapshot for fib reference price")?;
        let asset = snapshot.asset(coin)?;
        let ws_mid = realtime.and_then(|realtime| {
            let candidates = [
                asset.coin.clone(),
                snapshot.candle_coin(&asset.coin).unwrap_or_default(),
            ]
            .into_iter()
            .filter(|value| !value.trim().is_empty())
            .collect::<Vec<_>>();
            realtime.mid_price(MARKET_SPOT, &candidates)
        });
        ws_mid
            .or_else(|| parse_optional_decimal(asset.context.mid_px.as_deref()))
            .or_else(|| parse_optional_decimal(Some(asset.context.mark_px.as_str())))
            .or_else(|| parse_optional_decimal(Some(asset.context.prev_day_px.as_str())))
            .filter(|price| price.is_finite() && *price > 0.0)
            .with_context(|| format!("spot reference price unavailable for {coin}"))
    } else {
        let live_mid =
            realtime.and_then(|realtime| realtime.mid_price(market.id, &[coin.to_string()]));
        if let Some(price) = live_mid {
            return Ok(price);
        }
        let rest_mid = fetch_perp_all_mids_cached(
            &config.app.environment,
            &market.dex,
            MARKET_SNAPSHOT_QUOTE_CACHE_TTL_MS,
        )
        .await
        .ok()
        .and_then(|mids| mids.get(coin).cloned())
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|price| price.is_finite() && *price > 0.0);
        if let Some(price) = rest_mid {
            return Ok(price);
        }
        let snapshot = fetch_xyz_market_snapshot_cached(
            &config.app.environment,
            &market.dex,
            MARKET_SNAPSHOT_QUOTE_CACHE_TTL_MS,
        )
        .await
        .context("failed to fetch perp market snapshot for fib reference price")?;
        snapshot.asset(coin)?.reference_price()
    }
}

async fn fetch_fib_size_decimals(
    config: &AppConfig,
    market: &MarketProfile,
    coin: &str,
) -> Result<u32> {
    if market.is_spot() {
        let snapshot = fetch_spot_market_snapshot_cached(
            &config.app.environment,
            MARKET_SNAPSHOT_QUOTE_CACHE_TTL_MS,
        )
        .await
        .context("failed to fetch spot market snapshot for fib precision")?;
        Ok(snapshot.asset(coin)?.sz_decimals)
    } else {
        let snapshot = fetch_xyz_market_snapshot_cached(
            &config.app.environment,
            &market.dex,
            MARKET_SNAPSHOT_QUOTE_CACHE_TTL_MS,
        )
        .await
        .context("failed to fetch perp market snapshot for fib precision")?;
        Ok(snapshot.asset(coin)?.meta.sz_decimals)
    }
}

async fn fetch_fib_candles(
    config: &AppConfig,
    market: &MarketProfile,
    canonical_coin: &str,
    timeframe: &str,
    start_time_ms: u64,
    end_time_ms: u64,
) -> Result<Vec<crate::hyperliquid::CandleSnapshot>> {
    let candle_coin = if market.is_spot() {
        fetch_spot_market_snapshot_cached(
            &config.app.environment,
            MARKET_SNAPSHOT_QUOTE_CACHE_TTL_MS,
        )
        .await
        .context("failed to fetch spot market snapshot for fib candle symbol")?
        .candle_coin(canonical_coin)?
    } else {
        canonical_coin.to_string()
    };
    fetch_candle_snapshot(
        &config.app.environment,
        &candle_coin,
        timeframe,
        start_time_ms,
        end_time_ms,
    )
    .await
}

async fn build_fib_instance_record(
    state: &FrontendAppState,
    payload: FibBasicPayload,
    status: FibInstanceStatus,
    previous: Option<&FibInstanceRecord>,
) -> Result<FibInstanceRecord> {
    let base_config = state.config_snapshot()?;
    let market = resolve_market_profile(payload.market.as_deref(), &base_config)?;
    let scoped = scoped_config_for_module_and_market(base_config, "fib", &market);
    let canonical_coin = normalize_coin_for_market(&market, &payload.coin);
    anyhow::ensure!(
        scoped.symbol_allowed_for_module("fib", &canonical_coin),
        "fib symbol {} is blocked",
        canonical_coin
    );
    let account_ids = selected_enabled_account_ids(&scoped, &payload.account_ids);
    validate_batch_account_count("fib strategy", &scoped, &account_ids)?;
    ensure_accounts_allowed_for_market(&scoped, &account_ids, market.id)?;

    let levels = normalize_fib_levels(&payload.levels)?;
    let timeframe = payload.timeframe.trim().to_ascii_lowercase();
    let interval_ms = timeframe_interval_ms(&timeframe)?;
    let end_time_ms = now_ms();
    let start_time_ms =
        end_time_ms.saturating_sub(interval_ms.saturating_mul(payload.lookback_bars as u64 + 8));
    let candles = fetch_fib_candles(
        &scoped,
        &market,
        &canonical_coin,
        &timeframe,
        start_time_ms,
        end_time_ms,
    )
    .await
    .with_context(|| {
        format!(
            "failed to fetch candle snapshot for fib instance {} {}",
            canonical_coin, timeframe
        )
    })?;
    anyhow::ensure!(
        !candles.is_empty(),
        "candle snapshot returned no data for {} {}",
        canonical_coin,
        timeframe
    );
    let usable_start = candles.len().saturating_sub(payload.lookback_bars as usize);
    let usable = &candles[usable_start..];
    let direction = payload.trade_direction()?;
    anyhow::ensure!(
        !market.is_spot() || direction == FibTradeDirection::Long,
        "spot market does not support Fib short strategies"
    );
    let (swing_high, swing_low) = if payload.locked_range {
        (
            payload
                .locked_swing_high
                .context("locked_swing_high is required when locked_range=true")?,
            payload
                .locked_swing_low
                .context("locked_swing_low is required when locked_range=true")?,
        )
    } else {
        let swing = infer_fib_swing(usable, direction)?;
        (swing.swing_high, swing.swing_low)
    };
    let current_price =
        fetch_fib_reference_price(&scoped, &market, &canonical_coin, Some(&state.realtime)).await?;
    let execution_mode = parse_execution_mode(&payload.execution_mode)?;
    let take_profit_mode = FibProfitLossMode::from_raw(&payload.take_profit_mode)?;
    let stop_loss_mode = FibProfitLossMode::from_raw(&payload.stop_loss_mode)?;
    let leverage = if market.is_spot() {
        1.0
    } else {
        payload.leverage
    };
    let strategy_id = payload
        .strategy_id
        .clone()
        .unwrap_or_else(|| payload.default_strategy_id());
    let config = FibBasicConfig {
        strategy_id: strategy_id.clone(),
        direction,
        market: market.id.to_string(),
        dex: market.dex.to_string(),
        account_ids,
        coin: canonical_coin,
        timeframe,
        lookback_bars: payload.lookback_bars,
        swing_high,
        swing_low,
        current_price,
        levels,
        entry_above_tolerance_usd: payload.entry_above_tolerance_usd,
        entry_below_tolerance_usd: payload.entry_below_tolerance_usd,
        principal_usd: payload.principal_usd,
        leverage,
        execution_mode,
        take_profit_mode,
        take_profit_value: payload.take_profit_value,
        stop_loss_mode,
        stop_loss_value: payload.stop_loss_value,
        max_slippage_bps: payload.max_slippage_bps,
        max_entries_per_level: payload.max_entries_per_level,
        cooldown_secs: payload.cooldown_secs,
        stop_loss_cooldown_secs: payload.stop_loss_cooldown_secs,
        stop_loss_stop_strategy: payload.stop_loss_stop_strategy,
        locked_range: payload.locked_range,
        auto_loop: payload.auto_loop,
    };
    let plan = build_basic_plan(&config)?;
    let size_decimals = fetch_fib_size_decimals(&scoped, &market, &config.coin).await?;
    validate_fib_per_level_opening_notional(&plan, size_decimals)?;
    validate_fib_per_level_account_order_caps(&scoped, &config.account_ids, &plan)?;
    let created_at_ms = previous
        .map(|record| record.created_at_ms)
        .unwrap_or_else(now_ms);
    Ok(FibInstanceRecord {
        strategy_id,
        status,
        config,
        plan,
        dry_run: payload.dry_run,
        live: payload.live,
        entry_signal_ids: previous
            .map(|record| record.entry_signal_ids.clone())
            .unwrap_or_default(),
        entry_order_refs: previous
            .map(|record| record.entry_order_refs.clone())
            .unwrap_or_default(),
        protective_order_refs: previous
            .map(|record| record.protective_order_refs.clone())
            .unwrap_or_default(),
        last_message: None,
        created_at_ms,
        updated_at_ms: now_ms(),
        completed_cycles: previous
            .map(|record| record.completed_cycles)
            .unwrap_or_default(),
        last_cycle_completed_at_ms: previous.and_then(|record| record.last_cycle_completed_at_ms),
        last_cycle_exit_kind: previous.and_then(|record| record.last_cycle_exit_kind.clone()),
    })
}

fn validate_fib_per_level_opening_notional(plan: &FibBasicPlan, size_decimals: u32) -> Result<()> {
    for level in &plan.levels {
        anyhow::ensure!(
            level.order_notional_usd >= HYPERLIQUID_MIN_ORDER_NOTIONAL_USD,
            "fib per-level opening notional {:.6} is below Hyperliquid minimum {} USD; increase principal/leverage or select fewer levels",
            level.order_notional_usd,
            HYPERLIQUID_MIN_ORDER_NOTIONAL_USD
        );
        anyhow::ensure!(
            level.entry_price.is_finite() && level.entry_price > 0.0,
            "fib entry price must be positive before validating order notional"
        );
        let rounded_size =
            round_size_down(level.order_notional_usd / level.entry_price, size_decimals);
        let effective_notional = effective_order_notional_usd(level.entry_price, rounded_size);
        anyhow::ensure!(
            effective_exchange_min_order_notional_ok(level.entry_price, rounded_size, false),
            "fib per-level opening notional {:.6} becomes {:.6} USD after size rounding with szDecimals={}; Hyperliquid opening orders must remain at least {} USD",
            level.order_notional_usd,
            effective_notional,
            size_decimals,
            HYPERLIQUID_MIN_ORDER_NOTIONAL_USD
        );
    }
    Ok(())
}

fn validate_fib_per_level_account_order_caps(
    config: &AppConfig,
    account_ids: &[String],
    plan: &FibBasicPlan,
) -> Result<()> {
    for level in &plan.levels {
        for account_id in account_ids {
            let account = config
                .account(account_id)
                .with_context(|| format!("fib account {account_id} is not configured"))?;
            anyhow::ensure!(
                level.order_notional_usd <= account.max_order_notional_usd,
                "fib per-level opening notional {:.6} for {} exceeds account {} max_order_notional_usd {:.6}; reduce principal/leverage, select fewer levels, or raise that account limit",
                level.order_notional_usd,
                plan.coin,
                account.account_id,
                account.max_order_notional_usd
            );
        }
    }
    Ok(())
}

fn fib_coordinator_signals_from_plan(
    config: &FibBasicConfig,
    plan: &FibBasicPlan,
) -> Result<Vec<CoordinatorSignal>> {
    let mut signals = Vec::new();
    for level in &plan.levels {
        let should_prepare = match config.execution_mode {
            ExecutionMode::Maker => match config.direction {
                FibTradeDirection::Long => config.current_price > level.entry_price,
                FibTradeDirection::Short => config.current_price < level.entry_price,
            },
            ExecutionMode::Taker => level.current_within_zone,
        };
        if !should_prepare {
            continue;
        }
        let now = now_ms();
        let signal = CoordinatorSignal {
            signal_id: format!(
                "fib-basic-{}-{:.3}-{now}",
                config.strategy_id.replace(':', "_"),
                level.level
            ),
            source: SignalSource::Fib,
            created_at_ms: now,
            dispatch_at_ms: now,
            expires_at_ms: now + FIB_ENTRY_SIGNAL_EXECUTION_TTL_MS,
            target_accounts: config.account_ids.clone(),
            dedupe_key: format!(
                "fib-basic:{}:{}:{:.6}:{}",
                plan.line_version, config.coin, level.level, config.timeframe
            ),
            order: SignalOrder {
                market: Some(config.market.clone()),
                dex: Some(config.dex.clone()),
                coin: config.coin.clone(),
                side: fib_entry_side(config.direction),
                notional_usd: level.order_notional_usd,
                reduce_only: false,
                execution_mode: config.execution_mode,
                max_slippage_bps: config.max_slippage_bps,
                limit_price: match config.execution_mode {
                    ExecutionMode::Maker => Some(level.entry_price),
                    ExecutionMode::Taker => None,
                },
                apply_account_ratio: false,
            },
        };
        signals.push(signal);
    }
    Ok(signals)
}

fn fib_entry_signal_responses_from_signals(
    signals: &[CoordinatorSignal],
    plan: &FibBasicPlan,
) -> Vec<FibEntrySignalResponse> {
    signals
        .iter()
        .filter_map(|signal| {
            let level = plan
                .levels
                .iter()
                .find(|level| signal.dedupe_key.contains(&format!(":{:.6}:", level.level)))?;
            Some(fib_signal_response(signal, level))
        })
        .collect()
}

fn fib_signal_response(
    signal: &CoordinatorSignal,
    level: &FibBasicLevelPlan,
) -> FibEntrySignalResponse {
    FibEntrySignalResponse {
        signal_id: signal.signal_id.clone(),
        target_accounts: signal.target_accounts.clone(),
        market: signal.order.market.clone(),
        dex: signal.order.dex.clone(),
        coin: signal.order.coin.clone(),
        level: level.level,
        side: match signal.order.side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        }
        .to_string(),
        order_notional_usd: signal.order.notional_usd,
        entry_price: level.entry_price,
        limit_price: signal.order.limit_price,
        entry_zone_high: level.entry_zone_high,
        entry_zone_low: level.entry_zone_low,
        execution_mode: signal.order.execution_mode,
    }
}

fn worker_report_has_live_fill(report: &WorkerReport) -> bool {
    matches!(
        report,
        WorkerReport::Submitted(submitted)
            if !submitted.dry_run
                && submitted.filled_size.unwrap_or_default().abs() > 0.0
                && submitted.avg_fill_price.is_some()
    )
}

fn fib_execution_message(
    prefix: &str,
    entry_reports: &[WorkerReport],
    protection_reports: &[ProtectiveExitArmResult],
) -> String {
    let submitted = entry_reports
        .iter()
        .filter(|report| matches!(report, WorkerReport::Submitted(_)))
        .count();
    let rejected = entry_reports
        .iter()
        .filter(|report| matches!(report, WorkerReport::Rejected(_) | WorkerReport::Error(_)))
        .count();
    let protected = protection_reports
        .iter()
        .filter(|report| report.submitted)
        .count();
    format!(
        "{prefix}; entry reports: {submitted} submitted, {rejected} rejected/error; protective TP/SL submitted: {protected}"
    )
}

#[derive(Debug, Clone)]
struct FibEntrySyncAssessment {
    expected_count: usize,
    submitted_count: usize,
    missing_targets: Vec<String>,
}

impl FibEntrySyncAssessment {
    fn is_complete(&self) -> bool {
        self.missing_targets.is_empty()
    }
}

fn fib_entry_sync_assessment(
    signals: &[CoordinatorSignal],
    reports: &[WorkerReport],
) -> FibEntrySyncAssessment {
    let expected = signals
        .iter()
        .flat_map(|signal| {
            signal
                .target_accounts
                .iter()
                .map(move |account_id| (signal.signal_id.clone(), account_id.clone()))
        })
        .collect::<HashSet<_>>();
    let submitted = reports
        .iter()
        .filter_map(|report| {
            let WorkerReport::Submitted(submitted) = report else {
                return None;
            };
            Some((submitted.signal_id.clone(), submitted.account_id.clone()))
        })
        .collect::<HashSet<_>>();
    let mut missing_targets = expected
        .difference(&submitted)
        .map(|(signal_id, account_id)| format!("{account_id}/{signal_id}"))
        .collect::<Vec<_>>();
    missing_targets.sort();
    FibEntrySyncAssessment {
        expected_count: expected.len(),
        submitted_count: submitted.len(),
        missing_targets,
    }
}

fn fib_entry_signal_account_groups(
    signals: &[CoordinatorSignal],
) -> Vec<(String, Vec<CoordinatorSignal>)> {
    let mut seen = HashSet::new();
    let mut account_order = Vec::new();
    for signal in signals {
        for account_id in &signal.target_accounts {
            if seen.insert(account_id.clone()) {
                account_order.push(account_id.clone());
            }
        }
    }

    account_order
        .into_iter()
        .map(|account_id| {
            let account_signals = signals
                .iter()
                .filter(|signal| signal.target_accounts.contains(&account_id))
                .cloned()
                .collect::<Vec<_>>();
            (account_id, account_signals)
        })
        .collect()
}

fn refresh_fib_signal_for_submission(signal: &CoordinatorSignal) -> CoordinatorSignal {
    let mut refreshed = signal.clone();
    let now = now_ms();
    refreshed.dispatch_at_ms = now;
    refreshed.expires_at_ms = now + FIB_ENTRY_SIGNAL_EXECUTION_TTL_MS;
    refreshed
}

fn fib_resting_entry_order_refs_from_reports(
    signals: &[CoordinatorSignal],
    reports: &[WorkerReport],
    plan: &FibBasicPlan,
) -> Vec<FibOrderRef> {
    reports
        .iter()
        .filter_map(|report| {
            let WorkerReport::Submitted(submitted) = report else {
                return None;
            };
            if submitted.dry_run || submitted.filled_size.unwrap_or_default().abs() > 0.0 {
                return None;
            }
            let level = signals
                .iter()
                .find(|signal| signal.signal_id == submitted.signal_id)
                .and_then(|signal| fib_level_from_signal(signal, plan));
            Some(FibOrderRef {
                account_id: submitted.account_id.clone(),
                coin: submitted.coin.clone(),
                cloid: submitted.cloid.clone(),
                oid: submitted.oid,
                level,
                role: Some("entry".to_string()),
                dry_run: submitted.dry_run,
                submitted_at_ms: submitted.submitted_at_ms,
            })
        })
        .collect()
}

async fn cancel_incomplete_fib_entry_orders(
    state: &FrontendAppState,
    record: &FibInstanceRecord,
    resting_refs: Vec<FibOrderRef>,
    dry_run_requested: bool,
    live_requested: bool,
) -> Result<Vec<FibCancelOrderReport>> {
    if resting_refs.is_empty() {
        return Ok(Vec::new());
    }
    let mut cancel_record = record.clone();
    cancel_record.entry_order_refs = resting_refs;
    cancel_record.protective_order_refs.clear();
    cancel_fib_entry_order_refs(
        state,
        Some(&cancel_record),
        dry_run_requested,
        live_requested,
    )
    .await
}

fn remove_successfully_cancelled_fib_entry_refs(
    record: &mut FibInstanceRecord,
    cancel_reports: &[FibCancelOrderReport],
) {
    let successful_cancel_cloids = cancel_reports
        .iter()
        .filter(|report| report.ok)
        .map(|report| report.cloid.clone())
        .collect::<HashSet<_>>();
    if successful_cancel_cloids.is_empty() {
        return;
    }
    record
        .entry_order_refs
        .retain(|order_ref| !successful_cancel_cloids.contains(&order_ref.cloid));
}

fn mark_fib_record_incomplete_entry_submission(
    record: &mut FibInstanceRecord,
    assessment: &FibEntrySyncAssessment,
    reports: &[WorkerReport],
    has_live_fill: bool,
    cancel_reports: &[FibCancelOrderReport],
) {
    if fib_entry_reports_are_retryable_maker_miss(record, reports) {
        record.entry_order_refs.clear();
        record.protective_order_refs.clear();
        record.status = FibInstanceStatus::ArmedUnfilled;
        record.config.auto_loop = true;
        record.last_message = Some(format!(
            "Fib maker entry was not accepted because the limit would cross or no longer rest; auto-loop stays active and will retry. {}",
            fib_entry_report_failure_summary(reports)
        ));
        return;
    }

    record.config.auto_loop = false;
    record.status = if has_live_fill || !record.protective_order_refs.is_empty() {
        FibInstanceStatus::ProtectionPending
    } else {
        FibInstanceStatus::Error
    };
    let cancelled = cancel_reports.iter().filter(|report| report.ok).count();
    let failed_cancel = cancel_reports.iter().filter(|report| !report.ok).count();
    record.last_message = Some(format!(
        "Fib multi-account entry incomplete; auto-loop paused. Submitted {}/{} target entries; missing: {}. Cancelled {} resting partial order(s), {} cancel failed. {}",
        assessment.submitted_count,
        assessment.expected_count,
        assessment.missing_targets.join(", "),
        cancelled,
        failed_cancel,
        fib_entry_report_failure_summary(reports)
    ));
}

fn fib_entry_reports_are_retryable_maker_miss(
    record: &FibInstanceRecord,
    reports: &[WorkerReport],
) -> bool {
    if !matches!(record.config.execution_mode, ExecutionMode::Maker) || reports.is_empty() {
        return false;
    }
    let actionable = reports
        .iter()
        .filter(|report| matches!(report, WorkerReport::Rejected(_) | WorkerReport::Error(_)))
        .count();
    actionable == reports.len() && reports.iter().all(fib_entry_report_is_retryable_maker_miss)
}

fn fib_entry_report_is_retryable_maker_miss(report: &WorkerReport) -> bool {
    let message = match report {
        WorkerReport::Rejected(rejection) => rejection.message.as_str(),
        WorkerReport::Error(error) => error.message.as_str(),
        _ => return false,
    }
    .trim()
    .to_ascii_lowercase();
    message.contains("post only")
        || message.contains("post-only")
        || message.contains("would immediately match")
        || message.contains("immediately matched")
        || message.contains("would cross")
        || message.contains("alo")
}

fn fib_entry_report_failure_summary(reports: &[WorkerReport]) -> String {
    let mut details = reports
        .iter()
        .filter_map(|report| match report {
            WorkerReport::Rejected(rejection) => Some(format!(
                "{} rejected: {} ({})",
                rejection.account_id, rejection.reason_code, rejection.message
            )),
            WorkerReport::Error(error) => Some(format!(
                "{} error: {}",
                error.account_id,
                humanize_recent_event_error_text(&error.message)
            )),
            WorkerReport::Submitted(_) => None,
            WorkerReport::Ack(_) | WorkerReport::Health(_) => None,
        })
        .collect::<Vec<_>>();
    details.sort();
    if details.is_empty() {
        "No rejected/error report details were recorded.".to_string()
    } else {
        format!("Report details: {}", details.join(" | "))
    }
}

fn fib_account_has_complete_protection(record: &FibInstanceRecord, account_id: &str) -> bool {
    let mut roles = HashSet::new();
    for order_ref in &record.protective_order_refs {
        if order_ref.dry_run || order_ref.account_id != account_id {
            continue;
        }
        if let Some(role) = order_ref.role.as_deref() {
            roles.insert(role.trim().to_ascii_lowercase());
        }
    }
    roles.contains("take_profit") && roles.contains("stop_loss")
}

fn fib_missing_protected_accounts(record: &FibInstanceRecord) -> Vec<String> {
    let mut missing = record
        .config
        .account_ids
        .iter()
        .filter(|account_id| !fib_account_has_complete_protection(record, account_id))
        .cloned()
        .collect::<Vec<_>>();
    missing.sort();
    missing
}

fn fib_all_target_accounts_have_complete_protection(record: &FibInstanceRecord) -> bool {
    !record.config.account_ids.is_empty() && fib_missing_protected_accounts(record).is_empty()
}

fn fib_record_has_live_entry_refs(record: &FibInstanceRecord) -> bool {
    record
        .entry_order_refs
        .iter()
        .any(|order_ref| !order_ref.dry_run)
}

fn fib_entry_order_refs_from_reports(
    signals: &[CoordinatorSignal],
    reports: &[WorkerReport],
    plan: &FibBasicPlan,
) -> Vec<FibOrderRef> {
    reports
        .iter()
        .filter_map(|report| {
            let WorkerReport::Submitted(submitted) = report else {
                return None;
            };
            let level = signals
                .iter()
                .find(|signal| signal.signal_id == submitted.signal_id)
                .and_then(|signal| fib_level_from_signal(signal, plan));
            Some(FibOrderRef {
                account_id: submitted.account_id.clone(),
                coin: submitted.coin.clone(),
                cloid: submitted.cloid.clone(),
                oid: submitted.oid,
                level,
                role: Some("entry".to_string()),
                dry_run: submitted.dry_run,
                submitted_at_ms: submitted.submitted_at_ms,
            })
        })
        .collect()
}

fn fib_protective_order_refs_from_reports(reports: &[ProtectiveExitArmResult]) -> Vec<FibOrderRef> {
    reports
        .iter()
        .filter(|report| report.submitted)
        .flat_map(|report| {
            let submitted_by_cloid = report
                .submit_reports
                .iter()
                .filter_map(|submit_report| {
                    let WorkerReport::Submitted(submitted) = submit_report else {
                        return None;
                    };
                    Some((submitted.cloid.to_ascii_lowercase(), submitted))
                })
                .collect::<HashMap<_, _>>();
            report.plan.legs.iter().map(move |leg| {
                let submitted = submitted_by_cloid.get(&leg.cloid.to_ascii_lowercase());
                FibOrderRef {
                    account_id: report.plan.account_id.clone(),
                    coin: report.plan.coin.clone(),
                    cloid: leg.cloid.clone(),
                    oid: submitted.and_then(|submitted| submitted.oid),
                    level: None,
                    role: Some(leg.kind.clone()),
                    dry_run: false,
                    submitted_at_ms: submitted
                        .map(|submitted| submitted.submitted_at_ms)
                        .unwrap_or_else(now_ms),
                }
            })
        })
        .collect()
}

fn fib_level_from_signal(signal: &CoordinatorSignal, plan: &FibBasicPlan) -> Option<f64> {
    plan.levels
        .iter()
        .find(|level| signal.dedupe_key.contains(&format!(":{:.6}:", level.level)))
        .map(|level| level.level)
}

async fn cancel_fib_entry_order_refs(
    state: &FrontendAppState,
    previous: Option<&FibInstanceRecord>,
    dry_run_requested: bool,
    live_requested: bool,
) -> Result<Vec<FibCancelOrderReport>> {
    let Some(previous) = previous else {
        return Ok(Vec::new());
    };
    let execute_live = live_requested && !dry_run_requested;
    if !execute_live {
        return Ok(Vec::new());
    }
    let refs = previous
        .entry_order_refs
        .iter()
        .filter(|order_ref| !order_ref.dry_run)
        .collect::<Vec<_>>();
    if refs.is_empty() {
        return Ok(Vec::new());
    }

    let base_config = state.config_snapshot()?;
    let market = resolve_market_profile(Some(&previous.config.market), &base_config)?;
    let config = scoped_config_for_module_and_market(base_config.clone(), "fib", &market);
    anyhow::ensure!(
        !state.dry_run && !config.app.dry_run,
        "fib live cancel requires frontend and config dry_run=false"
    );
    let vault_path = Path::new(&config.secrets.vault_path);
    let vault_password = state.resolve_vault_password(vault_path, "")?;
    let mut reports = Vec::with_capacity(refs.len());
    for order_ref in refs {
        match execute_cancel_by_cloid(
            config.clone(),
            order_ref.account_id.clone(),
            order_ref.coin.clone(),
            order_ref.cloid.clone(),
            true,
            &vault_password,
        )
        .await
        {
            Ok(data) => reports.push(FibCancelOrderReport {
                account_id: data.account_id.clone(),
                coin: data.coin.clone(),
                cloid: data.cloid.clone(),
                ok: !data.matching_open_after,
                cancel_response: Some(data.cancel_response),
                open_orders_after: Some(data.open_orders_after),
                matching_open_after: Some(data.matching_open_after),
                error: None,
            }),
            Err(error) => {
                let message = format!(
                    "failed to cancel previous Fib entry order {} for {}: {}",
                    order_ref.cloid,
                    order_ref.account_id,
                    format_anyhow_error(&error)
                );
                tracing::warn!(
                    strategy_id = %previous.strategy_id,
                    account_id = %order_ref.account_id,
                    cloid = %order_ref.cloid,
                    error = %format_anyhow_error(&error),
                    "Fib entry cancel failed; preserving order ref for retry"
                );
                reports.push(FibCancelOrderReport {
                    account_id: order_ref.account_id.clone(),
                    coin: order_ref.coin.clone(),
                    cloid: order_ref.cloid.clone(),
                    ok: false,
                    cancel_response: None,
                    open_orders_after: None,
                    matching_open_after: None,
                    error: Some(message),
                });
            }
        }
    }
    Ok(reports)
}

#[derive(Debug, Clone)]
struct FibEntryFill {
    avg_fill_price: f64,
    filled_size: f64,
}

async fn fib_reconciliation_loop(state: FrontendAppState) {
    let mut interval =
        tokio::time::interval(std::time::Duration::from_millis(FIB_RECONCILE_INTERVAL_MS));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        if let Err(error) = reconcile_fib_instances_once(&state).await {
            tracing::warn!(%error, "Fib reconciliation loop iteration failed");
        }
    }
}

async fn reconcile_fib_instances_once(state: &FrontendAppState) -> Result<()> {
    if state.dry_run {
        return Ok(());
    }
    let records = state
        .fib_instances
        .read()
        .map_err(|_| anyhow::anyhow!("fib instance lock is poisoned"))?
        .values()
        .cloned()
        .collect::<Vec<_>>();

    for record in records {
        if !fib_record_allows_background_live_actions(&record) {
            continue;
        }
        if maybe_recover_and_protect_fib_open_position(state, &record).await? {
            continue;
        }
        if maybe_mark_fib_partial_protection_error(state, &record)? {
            continue;
        }
        if matches!(
            record.status,
            FibInstanceStatus::Protected
                | FibInstanceStatus::ProtectionPending
                | FibInstanceStatus::Error
        ) && !record.protective_order_refs.is_empty()
        {
            maybe_complete_fib_cycle_after_exit(state, &record).await?;
            continue;
        }
        if matches!(record.status, FibInstanceStatus::ArmedUnfilled) {
            maybe_submit_armed_fib_entry(state, &record).await?;
            continue;
        }
        if matches!(record.status, FibInstanceStatus::Completed) && record.config.auto_loop {
            maybe_restart_completed_fib_cycle(state, &record).await?;
            continue;
        }
        if !matches!(
            record.status,
            FibInstanceStatus::EntryPending
                | FibInstanceStatus::EntryFilled
                | FibInstanceStatus::ProtectionPending
        ) {
            continue;
        }
        let live_entry_refs = record
            .entry_order_refs
            .iter()
            .filter(|order_ref| !order_ref.dry_run)
            .cloned()
            .collect::<Vec<_>>();
        if live_entry_refs.is_empty() {
            if matches!(record.status, FibInstanceStatus::EntryPending) && record.config.auto_loop {
                mark_fib_record_auto_loop_retry_wait(
                    state,
                    &record.strategy_id,
                    "Fib auto loop has no accepted live entry order; waiting for the next cooldown retry",
                )?;
            }
            continue;
        }

        let base_config = state.config_snapshot()?;
        let market = resolve_market_profile(Some(&record.config.market), &base_config)?;
        let config = scoped_config_for_module_and_market(base_config, "fib", &market);
        if config.app.dry_run {
            continue;
        }
        let vault_password =
            match state.resolve_vault_password(Path::new(&config.secrets.vault_path), "") {
                Ok(password) => password,
                Err(error) => {
                    tracing::warn!(
                        strategy_id = %record.strategy_id,
                        %error,
                        "Fib reconciliation cannot arm protection until Vault is unlocked"
                    );
                    continue;
                }
            };

        for order_ref in live_entry_refs {
            let Some(account) = config.account(&order_ref.account_id).cloned() else {
                tracing::warn!(
                    strategy_id = %record.strategy_id,
                    account_id = %order_ref.account_id,
                    "Fib reconciliation skipped missing account"
                );
                continue;
            };
            let Some(fill) =
                (match fib_entry_fill_from_exchange(state, &config, &account, &order_ref).await {
                    Ok(fill) => fill,
                    Err(error) => {
                        tracing::warn!(
                            strategy_id = %record.strategy_id,
                            account_id = %order_ref.account_id,
                            cloid = %order_ref.cloid,
                            %error,
                            "Fib reconciliation entry fill lookup failed"
                        );
                        continue;
                    }
                })
            else {
                continue;
            };

            match arm_fib_protection_for_reconciled_fill(
                &config,
                &record,
                &account,
                &order_ref,
                &fill,
                &vault_password,
            )
            .await
            {
                Ok(protection) => {
                    update_fib_record_after_reconciled_protection(
                        state,
                        &record.strategy_id,
                        &order_ref,
                        protection,
                    )?;
                }
                Err(error) => {
                    tracing::warn!(
                        strategy_id = %record.strategy_id,
                        account_id = %order_ref.account_id,
                        cloid = %order_ref.cloid,
                        %error,
                        "Fib reconciliation failed to arm exchange-native protection"
                    );
                    mark_fib_record_protection_error(
                        state,
                        &record.strategy_id,
                        &order_ref,
                        &error,
                    )?;
                }
            }
        }
    }

    Ok(())
}

async fn fib_entry_fill_from_exchange(
    state: &FrontendAppState,
    config: &AppConfig,
    account: &AccountConfig,
    order_ref: &FibOrderRef,
) -> Result<Option<FibEntryFill>> {
    let query_dex = info_query_dex_for_frontend(&config.hyperliquid.dex);
    let market_id = frontend_market_id_for_coin(&order_ref.coin);
    let open_orders =
        if let Some(open_orders) = state.realtime.open_orders(market_id, &account.address) {
            open_orders
        } else {
            fetch_open_orders(&config.app.environment, &query_dex, &account.address)
                .await
                .context("failed to fetch open orders for Fib entry reconciliation")?
        };
    if open_orders
        .iter()
        .any(|order| open_order_matches_fib_ref(order, order_ref))
    {
        return Ok(None);
    }

    let fills = if let Some(fills) = state.realtime.fills(market_id, &account.address) {
        fills
    } else {
        fetch_user_fills(&config.app.environment, &query_dex, &account.address)
            .await
            .context("failed to fetch user fills for Fib entry reconciliation")?
    };
    let matched = fills
        .iter()
        .filter(|fill| order_ref.oid.map(|oid| fill.oid == oid).unwrap_or(false))
        .collect::<Vec<_>>();
    if matched.is_empty() {
        return Ok(None);
    }

    let mut filled_size = 0.0;
    let mut filled_notional = 0.0;
    for fill in matched {
        let size = parse_optional_decimal(Some(fill.sz.as_str()))
            .unwrap_or_default()
            .abs();
        let price = parse_optional_decimal(Some(fill.px.as_str())).unwrap_or_default();
        if size <= 0.0 || price <= 0.0 {
            continue;
        }
        filled_size += size;
        filled_notional += size * price;
    }
    if filled_size <= 0.0 || filled_notional <= 0.0 {
        return Ok(None);
    }
    Ok(Some(FibEntryFill {
        avg_fill_price: filled_notional / filled_size,
        filled_size,
    }))
}

fn open_order_matches_fib_ref(order: &OpenOrder, order_ref: &FibOrderRef) -> bool {
    order_ref.oid == Some(order.oid) || order.cloid.as_deref() == Some(order_ref.cloid.as_str())
}

fn info_query_dex_for_frontend(dex: &str) -> String {
    if dex.trim().eq_ignore_ascii_case("spot") {
        String::new()
    } else {
        dex.trim().to_ascii_lowercase()
    }
}

async fn arm_fib_protection_for_reconciled_fill(
    config: &AppConfig,
    record: &FibInstanceRecord,
    account: &AccountConfig,
    order_ref: &FibOrderRef,
    fill: &FibEntryFill,
    vault_password: &str,
) -> Result<ProtectiveExitArmResult> {
    let take_profit_trigger_price =
        fib_take_profit_trigger_from_entry(&record.config, fill.avg_fill_price)?;
    let stop_loss_trigger_price =
        fib_stop_loss_trigger_from_entry(&record.config, fill.avg_fill_price)?;
    let notional_usd = fill.avg_fill_price * fill.filled_size;
    let options = ProtectiveExitArmOptions {
        exit: ProtectiveExitOptions {
            account_id: account.account_id.clone(),
            coin: order_ref.coin.clone(),
            entry_side: match fib_entry_side(record.config.direction) {
                OrderSide::Buy => "buy",
                OrderSide::Sell => "sell",
            }
            .to_string(),
            entry_price: Some(fill.avg_fill_price),
            notional_usd,
            take_profit_usd: 0.0,
            stop_loss_pct: 0.0,
            take_profit_trigger_price: Some(take_profit_trigger_price),
            stop_loss_trigger_price: Some(stop_loss_trigger_price),
            max_slippage_bps: record.config.max_slippage_bps,
        },
        submit: true,
        confirm_mainnet_live: true,
    };
    let mut result =
        execute_protective_exit_arm(config.clone(), options, false, Some(vault_password)).await?;
    result.persistent_rule_id = Some(format!(
        "fib:{}:{}:{}",
        record.config.strategy_id,
        order_ref
            .level
            .map(|level| format!("{level:.3}"))
            .unwrap_or_else(|| "unknown".to_string()),
        order_ref.cloid
    ));
    Ok(result)
}

fn update_fib_record_after_reconciled_protection(
    state: &FrontendAppState,
    strategy_id: &str,
    order_ref: &FibOrderRef,
    protection: ProtectiveExitArmResult,
) -> Result<()> {
    let mut guard = state
        .fib_instances
        .write()
        .map_err(|_| anyhow::anyhow!("fib instance lock is poisoned"))?;
    let Some(record) = guard.get_mut(strategy_id) else {
        return Ok(());
    };
    record.entry_order_refs.retain(|entry_ref| {
        !(entry_ref.account_id == order_ref.account_id && entry_ref.cloid == order_ref.cloid)
    });
    if protection.submitted {
        let refs = fib_protective_order_refs_from_reports(&[protection]);
        record.protective_order_refs.extend(refs);
        record.status = if fib_record_has_live_entry_refs(record) {
            FibInstanceStatus::ProtectionPending
        } else if fib_all_target_accounts_have_complete_protection(record) {
            FibInstanceStatus::Protected
        } else {
            record.config.auto_loop = false;
            FibInstanceStatus::Error
        };
        record.last_message = Some(if matches!(record.status, FibInstanceStatus::Error) {
            format!(
                "Fib entry filled for {}; exchange-native TP/SL submitted, but multi-account protection is incomplete. Missing protected account(s): {}. Auto-loop paused.",
                order_ref.account_id,
                fib_missing_protected_accounts(record).join(", ")
            )
        } else {
            format!(
                "Fib entry filled for {}; exchange-native TP/SL submitted",
                order_ref.account_id
            )
        });
    } else {
        record.status = FibInstanceStatus::ProtectionPending;
        record.last_message = Some(format!(
            "Fib entry filled for {}; protection plan created but not submitted",
            order_ref.account_id
        ));
    }
    record.updated_at_ms = now_ms();
    let history_record = record.clone();
    persist_fib_instances_best_effort(&guard);
    drop(guard);
    append_fib_instance_history_best_effort(&history_record, "reconciled_protection");
    Ok(())
}

fn mark_fib_record_protection_error(
    state: &FrontendAppState,
    strategy_id: &str,
    order_ref: &FibOrderRef,
    error: &anyhow::Error,
) -> Result<()> {
    let mut guard = state
        .fib_instances
        .write()
        .map_err(|_| anyhow::anyhow!("fib instance lock is poisoned"))?;
    let history_record = if let Some(record) = guard.get_mut(strategy_id) {
        record.status = FibInstanceStatus::ProtectionPending;
        record.last_message = Some(format!(
            "Fib entry filled for {}, but TP/SL arm failed: {}",
            order_ref.account_id,
            format_anyhow_error(error)
        ));
        record.updated_at_ms = now_ms();
        Some(record.clone())
    } else {
        None
    };
    persist_fib_instances_best_effort(&guard);
    drop(guard);
    if let Some(record) = history_record {
        append_fib_instance_history_best_effort(&record, "protection_error");
    }
    Ok(())
}

fn maybe_mark_fib_partial_protection_error(
    state: &FrontendAppState,
    record: &FibInstanceRecord,
) -> Result<bool> {
    if !matches!(record.status, FibInstanceStatus::Protected)
        || record.protective_order_refs.is_empty()
        || fib_record_has_live_entry_refs(record)
        || fib_all_target_accounts_have_complete_protection(record)
    {
        return Ok(false);
    }
    let missing = fib_missing_protected_accounts(record);
    let mut guard = state
        .fib_instances
        .write()
        .map_err(|_| anyhow::anyhow!("fib instance lock is poisoned"))?;
    let history_record = if let Some(current) = guard.get_mut(&record.strategy_id) {
        current.status = FibInstanceStatus::Error;
        current.config.auto_loop = false;
        current.last_message = Some(format!(
            "Fib multi-account cycle is out of sync: some accounts are protected while others are not. Missing protected account(s): {}. Auto-loop paused; existing TP/SL orders remain active.",
            missing.join(", ")
        ));
        current.updated_at_ms = now_ms();
        Some(current.clone())
    } else {
        None
    };
    persist_fib_instances_best_effort(&guard);
    drop(guard);
    if let Some(record) = history_record {
        append_fib_instance_history_best_effort(&record, "partial_protection_error");
        return Ok(true);
    }
    Ok(false)
}

#[derive(Debug, Clone)]
struct FibRecoveredPositionCandidate {
    account: AccountConfig,
    order_ref: FibOrderRef,
    fill: FibEntryFill,
}

async fn maybe_recover_and_protect_fib_open_position(
    state: &FrontendAppState,
    record: &FibInstanceRecord,
) -> Result<bool> {
    if state.dry_run
        || !fib_record_allows_background_live_actions(record)
        || !fib_record_can_recover_unprotected_position(record)
    {
        return Ok(false);
    }

    let base_config = state.config_snapshot()?;
    let market = resolve_market_profile(Some(&record.config.market), &base_config)?;
    let config = scoped_config_for_module_and_market(base_config.clone(), "fib", &market);
    if config.app.dry_run {
        return Ok(false);
    }

    let mut candidates = Vec::new();
    let query_dex = info_query_dex_for_frontend(&market.dex);
    for account_id in &record.config.account_ids {
        if record
            .entry_order_refs
            .iter()
            .any(|order_ref| order_ref.account_id == *account_id && !order_ref.dry_run)
        {
            continue;
        }
        if fib_account_has_complete_protection(record, account_id) {
            continue;
        }
        let Some(account) = config.account(account_id).cloned() else {
            continue;
        };
        let position =
            fib_position_for_account(state, &config, &market, &account, &record.config.coin)
                .await?;
        if !fib_position_matches_direction(record.config.direction, position) {
            continue;
        }
        let Some(entry_price) = position.entry_price else {
            tracing::warn!(
                strategy_id = %record.strategy_id,
                account_id = %account.account_id,
                "Fib position recovery skipped because entry price is unavailable"
            );
            continue;
        };
        let Some(level) = fib_recoverable_position_level(record, entry_price) else {
            continue;
        };
        let open_orders =
            if let Some(open_orders) = state.realtime.open_orders(market.id, &account.address) {
                open_orders
            } else {
                fetch_open_orders(&config.app.environment, &query_dex, &account.address)
                    .await
                    .context("failed to fetch open orders for Fib position recovery")?
            };
        if fib_account_has_open_protective_order(&market, &record.config.coin, &open_orders) {
            continue;
        }
        let recovered_cloid = uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            format!(
                "fib-recovered-position:{}:{}:{}:{level:.6}:{entry_price:.8}",
                record.strategy_id, account.account_id, record.config.coin
            )
            .as_bytes(),
        )
        .to_string();
        candidates.push(FibRecoveredPositionCandidate {
            account,
            order_ref: FibOrderRef {
                account_id: account_id.clone(),
                coin: record.config.coin.clone(),
                cloid: recovered_cloid,
                oid: None,
                level: Some(level),
                role: Some("entry".to_string()),
                dry_run: false,
                submitted_at_ms: now_ms(),
            },
            fill: FibEntryFill {
                avg_fill_price: entry_price,
                filled_size: position.size.abs(),
            },
        });
    }

    if candidates.is_empty() {
        return Ok(false);
    }

    let vault_password = match state
        .resolve_vault_password(Path::new(&config.secrets.vault_path), "")
    {
        Ok(password) => password,
        Err(error) => {
            mark_fib_record_unprotected_position_waiting_vault(state, &record.strategy_id, &error)?;
            return Ok(true);
        }
    };

    for candidate in candidates {
        match arm_fib_protection_for_reconciled_fill(
            &config,
            record,
            &candidate.account,
            &candidate.order_ref,
            &candidate.fill,
            &vault_password,
        )
        .await
        {
            Ok(protection) => {
                update_fib_record_after_reconciled_protection(
                    state,
                    &record.strategy_id,
                    &candidate.order_ref,
                    protection,
                )?;
            }
            Err(error) => {
                mark_fib_record_protection_error(
                    state,
                    &record.strategy_id,
                    &candidate.order_ref,
                    &error,
                )?;
            }
        }
    }
    Ok(true)
}

fn fib_record_can_recover_unprotected_position(record: &FibInstanceRecord) -> bool {
    if !record.config.auto_loop {
        return false;
    }
    matches!(
        record.status,
        FibInstanceStatus::ArmedUnfilled
            | FibInstanceStatus::EntryPending
            | FibInstanceStatus::ProtectionPending
            | FibInstanceStatus::Protected
            | FibInstanceStatus::Completed
            | FibInstanceStatus::Error
    )
}

fn fib_recoverable_position_level(record: &FibInstanceRecord, entry_price: f64) -> Option<f64> {
    record
        .plan
        .levels
        .iter()
        .find(|level| {
            (entry_price - level.entry_price).abs()
                <= fib_entry_recovery_price_tolerance(level.entry_price)
        })
        .map(|level| level.level)
}

fn fib_account_has_open_protective_order(
    market: &MarketProfile,
    coin: &str,
    open_orders: &[OpenOrder],
) -> bool {
    open_orders.iter().any(|order| {
        order.is_trigger
            && open_order_native_protective_kind(order).is_some()
            && dashboard_order_coin_matches_strategy(market, &order.coin, coin)
    })
}

fn mark_fib_record_unprotected_position_waiting_vault(
    state: &FrontendAppState,
    strategy_id: &str,
    error: &anyhow::Error,
) -> Result<()> {
    let mut guard = state
        .fib_instances
        .write()
        .map_err(|_| anyhow::anyhow!("fib instance lock is poisoned"))?;
    let history_record = if let Some(record) = guard.get_mut(strategy_id) {
        record.status = FibInstanceStatus::ProtectionPending;
        record.last_message = Some(format!(
            "Fib position found without TP/SL; Vault unlock is required before protection can be submitted: {}",
            format_anyhow_error(error)
        ));
        record.updated_at_ms = now_ms();
        Some(record.clone())
    } else {
        None
    };
    persist_fib_instances_best_effort(&guard);
    drop(guard);
    if let Some(record) = history_record {
        append_fib_instance_history_best_effort(&record, "protection_waiting_vault");
    }
    Ok(())
}

async fn maybe_complete_fib_cycle_after_exit(
    state: &FrontendAppState,
    record: &FibInstanceRecord,
) -> Result<()> {
    if state.dry_run || !fib_record_allows_background_live_actions(record) {
        return Ok(());
    }
    if record.protective_order_refs.is_empty() {
        return Ok(());
    }
    let base_config = state.config_snapshot()?;
    let market = resolve_market_profile(Some(&record.config.market), &base_config)?;
    let config = scoped_config_for_module_and_market(base_config, "fib", &market);
    if config.app.dry_run {
        return Ok(());
    }
    let vault_path = Path::new(&config.secrets.vault_path);
    let vault_password = state.resolve_vault_password(vault_path, "").ok();

    let mut protective_still_open = false;
    let mut all_positions_flat = true;
    for account_id in &record.config.account_ids {
        let Some(account) = config.account(account_id).cloned() else {
            continue;
        };
        let query_dex = info_query_dex_for_frontend(&market.dex);
        let open_orders =
            if let Some(open_orders) = state.realtime.open_orders(market.id, &account.address) {
                open_orders
            } else {
                fetch_open_orders(&config.app.environment, &query_dex, &account.address)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to fetch open orders for Fib completion check {}",
                            account.account_id
                        )
                    })?
            };
        protective_still_open |= record
            .protective_order_refs
            .iter()
            .filter(|order_ref| order_ref.account_id == account.account_id)
            .any(|order_ref| {
                open_orders
                    .iter()
                    .any(|order| dashboard_open_order_matches_ref(order, order_ref))
            });

        let position =
            fib_position_for_account(state, &config, &market, &account, &record.config.coin)
                .await
                .with_context(|| {
                    format!(
                        "failed to fetch position size for Fib completion check {}",
                        account.account_id
                    )
                })?;
        if !fib_position_is_effectively_flat(position) {
            all_positions_flat = false;
        } else if !market.is_spot() && fib_position_requires_residual_cleanup(&position) {
            if let Some(password) = vault_password.as_deref() {
                match execute_fib_residual_cleanup_if_needed(
                    &config,
                    &market,
                    &account,
                    &record.config.coin,
                    record.config.max_slippage_bps,
                    Some(password),
                    "post-exit",
                )
                .await
                {
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(
                            strategy_id = %record.strategy_id,
                            account_id = %account.account_id,
                            coin = %record.config.coin,
                            error = %error,
                            "Fib post-exit residual cleanup failed; cycle will not restart yet"
                        );
                        all_positions_flat = false;
                    }
                }
            } else {
                tracing::warn!(
                    strategy_id = %record.strategy_id,
                    account_id = %account.account_id,
                    coin = %record.config.coin,
                    "Fib post-exit residual cleanup needs Vault unlock; cycle will not restart yet"
                );
                all_positions_flat = false;
            }
        }
    }

    if protective_still_open || !all_positions_flat {
        return Ok(());
    }

    let exit_kind = detect_fib_protective_exit_kind(&config, &market, record).await?;
    mark_fib_record_cycle_completed(state, &record.strategy_id, exit_kind.as_deref())?;
    Ok(())
}

async fn detect_fib_protective_exit_kind(
    config: &AppConfig,
    market: &MarketProfile,
    record: &FibInstanceRecord,
) -> Result<Option<String>> {
    let protective_refs = record
        .protective_order_refs
        .iter()
        .filter(|order_ref| !order_ref.dry_run)
        .filter_map(|order_ref| {
            let oid = order_ref.oid?;
            let role = order_ref.role.as_deref()?.trim().to_ascii_lowercase();
            if role == "take_profit" || role == "stop_loss" {
                Some((order_ref.account_id.clone(), oid, role))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    if protective_refs.is_empty() {
        return Ok(None);
    }

    let by_account = protective_refs.iter().fold(
        HashMap::<String, HashMap<u64, String>>::new(),
        |mut map, item| {
            map.entry(item.0.clone())
                .or_default()
                .insert(item.1, item.2.clone());
            map
        },
    );
    let earliest_ref_ms = record
        .protective_order_refs
        .iter()
        .filter(|order_ref| !order_ref.dry_run)
        .map(|order_ref| order_ref.submitted_at_ms)
        .min()
        .unwrap_or(record.updated_at_ms)
        .saturating_sub(5_000);
    let query_dex = info_query_dex_for_frontend(&market.dex);
    let mut saw_take_profit = false;
    for (account_id, oid_to_role) in by_account {
        let Some(account) = config.account(&account_id) else {
            continue;
        };
        let fills =
            match fetch_user_fills(&config.app.environment, &query_dex, &account.address).await {
                Ok(fills) => fills,
                Err(error) => {
                    tracing::warn!(
                        account_id = %account.account_id,
                        strategy_id = %record.strategy_id,
                        error = %error,
                        "best-effort Fib exit-kind fill lookup failed; completing cycle as unknown"
                    );
                    continue;
                }
            };
        for fill in fills {
            if fill.time < earliest_ref_ms {
                continue;
            }
            let Some(role) = oid_to_role.get(&fill.oid) else {
                continue;
            };
            if role == "stop_loss" {
                return Ok(Some("stop_loss".to_string()));
            }
            if role == "take_profit" {
                saw_take_profit = true;
            }
        }
    }
    if saw_take_profit {
        Ok(Some("take_profit".to_string()))
    } else {
        Ok(None)
    }
}

#[derive(Debug, Clone, Copy)]
struct FibPositionSnapshot {
    size: f64,
    value_usd: f64,
    entry_price: Option<f64>,
}

fn fib_position_is_effectively_flat(position: FibPositionSnapshot) -> bool {
    position.size.abs() <= FIB_COMPLETION_RESIDUAL_POSITION_EPSILON
        || (position.value_usd.is_finite()
            && position.value_usd.abs() < FIB_COMPLETION_RESIDUAL_POSITION_DUST_USD)
}

fn fib_position_matches_direction(
    direction: FibTradeDirection,
    position: FibPositionSnapshot,
) -> bool {
    if fib_position_is_effectively_flat(position) {
        return false;
    }
    match direction {
        FibTradeDirection::Long => position.size > FIB_COMPLETION_RESIDUAL_POSITION_EPSILON,
        FibTradeDirection::Short => position.size < -FIB_COMPLETION_RESIDUAL_POSITION_EPSILON,
    }
}

async fn fib_position_for_account(
    state: &FrontendAppState,
    config: &AppConfig,
    market: &MarketProfile,
    account: &AccountConfig,
    coin: &str,
) -> Result<FibPositionSnapshot> {
    if market.is_spot() {
        let spot_state = if let Some(spot_state) = state.realtime.spot_state(&account.address) {
            spot_state
        } else {
            fetch_spot_clearinghouse_state(&config.app.environment, &account.address)
                .await
                .context("failed to fetch spotClearinghouseState for Fib position check")?
        };
        let size = spot_account_readiness_state(&spot_state, coin).coin_position_size;
        let value_usd = if size.abs() <= FIB_COMPLETION_RESIDUAL_POSITION_EPSILON {
            0.0
        } else {
            fetch_fib_reference_price(config, market, coin, Some(&state.realtime))
                .await
                .map(|price| price * size.abs())
                .unwrap_or_default()
        };
        return Ok(FibPositionSnapshot {
            size,
            value_usd,
            entry_price: None,
        });
    }

    let clearinghouse_state = if let Some(clearinghouse_state) = state
        .realtime
        .clearinghouse_state(market.id, &account.address)
    {
        clearinghouse_state
    } else {
        fetch_clearinghouse_state(&config.app.environment, &market.dex, &account.address)
            .await
            .context("failed to fetch clearinghouseState for Fib position check")?
    };
    let canonical = normalize_dex_coin(&market.dex, coin);
    let mut size = 0.0;
    let mut value_usd = 0.0;
    let mut entry_price = None;
    for asset_position in &clearinghouse_state.asset_positions {
        let position = &asset_position.position;
        if normalize_dex_coin(&market.dex, &position.coin) == canonical {
            let position_size = parse_decimal(&position.szi);
            size += position_size;
            value_usd += position
                .position_value
                .as_deref()
                .map(parse_decimal)
                .unwrap_or_default()
                .abs();
            if entry_price.is_none() {
                entry_price = position
                    .entry_px
                    .as_deref()
                    .map(parse_decimal)
                    .filter(|value| *value > 0.0);
            }
        }
    }
    if value_usd <= 0.0 && size.abs() > FIB_COMPLETION_RESIDUAL_POSITION_EPSILON {
        value_usd = fetch_fib_reference_price(config, market, coin, Some(&state.realtime))
            .await
            .map(|price| price * size.abs())
            .unwrap_or_default();
    }
    Ok(FibPositionSnapshot {
        size,
        value_usd,
        entry_price,
    })
}

fn fib_position_requires_residual_cleanup(position: &FibPositionSnapshot) -> bool {
    position.size.abs() > FIB_CLEANUP_POSITION_EPSILON
}

async fn fib_fresh_perp_position_for_account(
    config: &AppConfig,
    market: &MarketProfile,
    account: &AccountConfig,
    coin: &str,
) -> Result<FibPositionSnapshot> {
    anyhow::ensure!(
        !market.is_spot(),
        "Fib residual cleanup only supports perp markets"
    );
    let clearinghouse_state =
        fetch_clearinghouse_state(&config.app.environment, &market.dex, &account.address)
            .await
            .context("failed to fetch fresh clearinghouseState for Fib residual cleanup")?;
    let canonical = normalize_dex_coin(&market.dex, coin);
    let mut size = 0.0;
    let mut value_usd = 0.0;
    let mut entry_price = None;
    for asset_position in &clearinghouse_state.asset_positions {
        let position = &asset_position.position;
        if normalize_dex_coin(&market.dex, &position.coin) == canonical {
            let position_size = parse_decimal(&position.szi);
            size += position_size;
            value_usd += position
                .position_value
                .as_deref()
                .map(parse_decimal)
                .unwrap_or_default()
                .abs();
            if entry_price.is_none() {
                entry_price = position
                    .entry_px
                    .as_deref()
                    .map(parse_decimal)
                    .filter(|value| *value > 0.0);
            }
        }
    }
    if value_usd <= 0.0 && size.abs() > FIB_CLEANUP_POSITION_EPSILON {
        value_usd = fetch_fib_reference_price(config, market, coin, None)
            .await
            .map(|price| price * size.abs())
            .unwrap_or_default();
    }
    Ok(FibPositionSnapshot {
        size,
        value_usd,
        entry_price,
    })
}

async fn wait_for_fib_strictly_flat_perp_position(
    config: &AppConfig,
    market: &MarketProfile,
    account: &AccountConfig,
    coin: &str,
) -> Result<FibPositionSnapshot> {
    let delays_ms = [0_u64, 800, 1_200, 1_800, 2_600, 3_500];
    let mut last = FibPositionSnapshot {
        size: 0.0,
        value_usd: 0.0,
        entry_price: None,
    };
    for delay_ms in delays_ms {
        if delay_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
        let position = fib_fresh_perp_position_for_account(config, market, account, coin).await?;
        if !fib_position_requires_residual_cleanup(&position) {
            return Ok(position);
        }
        last = position;
    }
    anyhow::bail!(
        "Fib reduce-only residual cleanup did not fully flatten {} for {}; remaining size={} value_usd={}",
        coin,
        account.account_id,
        last.size,
        last.value_usd
    );
}

async fn execute_fib_residual_cleanup_if_needed(
    config: &AppConfig,
    market: &MarketProfile,
    account: &AccountConfig,
    coin: &str,
    max_slippage_bps: f64,
    vault_password: Option<&str>,
    stage: &str,
) -> Result<Option<FastSignedOrderResult>> {
    if market.is_spot() {
        return Ok(None);
    }
    let position = fib_fresh_perp_position_for_account(config, market, account, coin).await?;
    if !fib_position_requires_residual_cleanup(&position) {
        return Ok(None);
    }
    if position.value_usd <= 0.0 {
        anyhow::bail!(
            "Fib {stage} residual cleanup for {} / {} found size={} but no positive notional value to close",
            account.account_id,
            coin,
            position.size
        );
    }
    let side = if position.size > 0.0 {
        OrderSide::Sell
    } else {
        OrderSide::Buy
    };
    let options = SignedSmokeOptions {
        account_id: account.account_id.clone(),
        coin: coin.to_string(),
        side,
        notional_usd: position.value_usd.max(0.000_001),
        max_slippage_bps,
        execution_mode: ExecutionMode::Taker,
        reduce_only: true,
        close_full_position: true,
        submit: true,
        cancel_resting: true,
        confirm_mainnet_live: config.app.environment == "mainnet",
    };
    let result = execute_fast_signed_order(config.clone(), options, vault_password, None)
        .await
        .with_context(|| {
            format!(
                "failed to submit Fib {stage} reduce-only residual cleanup for {} / {}",
                account.account_id, coin
            )
        })?;
    anyhow::ensure!(
        result.submitted,
        "Fib {stage} residual cleanup for {} / {} did not submit: {}",
        account.account_id,
        coin,
        fib_fast_signed_result_summary(&result)
    );
    wait_for_fib_strictly_flat_perp_position(config, market, account, coin).await?;
    Ok(Some(result))
}

fn fib_fast_signed_result_summary(result: &FastSignedOrderResult) -> String {
    let mut parts = vec![format!("transport={}", result.transport)];
    parts.push(format!(
        "planned_size={} limit_px={} reduce_only={}",
        result.plan.size, result.plan.limit_price, result.plan.reduce_only
    ));
    if let Some(report) = &result.submit_report {
        parts.push(format!("submit_report={report:?}"));
    }
    if !result.warnings.is_empty() {
        parts.push(format!("warnings={}", result.warnings.join("; ")));
    }
    parts.join(", ")
}

fn mark_fib_record_cycle_completed(
    state: &FrontendAppState,
    strategy_id: &str,
    exit_kind: Option<&str>,
) -> Result<()> {
    let mut guard = state
        .fib_instances
        .write()
        .map_err(|_| anyhow::anyhow!("fib instance lock is poisoned"))?;
    let history_record = if let Some(record) = guard.get_mut(strategy_id) {
        let normalized_exit_kind = normalize_fib_exit_kind(exit_kind);
        let stopped_after_stop_loss =
            fib_should_stop_after_cycle(record, normalized_exit_kind.as_deref());
        record.status = if stopped_after_stop_loss {
            FibInstanceStatus::Killed
        } else {
            FibInstanceStatus::Completed
        };
        record.entry_order_refs.clear();
        record.protective_order_refs.clear();
        record.completed_cycles = record.completed_cycles.saturating_add(1);
        record.last_cycle_completed_at_ms = Some(now_ms());
        record.last_cycle_exit_kind = normalized_exit_kind.clone();
        if stopped_after_stop_loss {
            record.config.auto_loop = false;
        }
        record.last_message = Some(fib_cycle_completed_message(
            normalized_exit_kind.as_deref(),
            stopped_after_stop_loss,
        ));
        record.updated_at_ms = now_ms();
        Some(record.clone())
    } else {
        None
    };
    persist_fib_instances_best_effort(&guard);
    drop(guard);
    if let Some(record) = history_record {
        append_fib_instance_history_best_effort(&record, "cycle_completed");
    }
    Ok(())
}

fn normalize_fib_exit_kind(exit_kind: Option<&str>) -> Option<String> {
    match exit_kind
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "take_profit" | "tp" => Some("take_profit".to_string()),
        "stop_loss" | "sl" => Some("stop_loss".to_string()),
        _ => None,
    }
}

fn fib_cycle_completed_message(exit_kind: Option<&str>, stopped_after_stop_loss: bool) -> String {
    match (exit_kind, stopped_after_stop_loss) {
        (Some("stop_loss"), true) => {
            "Fib cycle completed by stop loss: position is flat, protective orders are closed, and the strategy was stopped by configuration".to_string()
        }
        (Some("stop_loss"), false) => {
            "Fib cycle completed by stop loss: position is flat and protective orders are closed; waiting for stop-loss cooldown before re-arming".to_string()
        }
        (Some("take_profit"), _) => {
            "Fib cycle completed by take profit: position is flat and protective orders are closed".to_string()
        }
        _ => "Fib cycle completed: position is flat and protective orders are closed".to_string(),
    }
}

fn mark_fib_record_auto_loop_retry_wait(
    state: &FrontendAppState,
    strategy_id: &str,
    message: &str,
) -> Result<()> {
    let mut guard = state
        .fib_instances
        .write()
        .map_err(|_| anyhow::anyhow!("fib instance lock is poisoned"))?;
    let history_record = if let Some(record) = guard.get_mut(strategy_id) {
        record.status = FibInstanceStatus::ArmedUnfilled;
        record.entry_order_refs.clear();
        record.protective_order_refs.clear();
        record.last_message = Some(message.to_string());
        record.updated_at_ms = now_ms();
        Some(record.clone())
    } else {
        None
    };
    persist_fib_instances_best_effort(&guard);
    drop(guard);
    if let Some(record) = history_record {
        append_fib_instance_history_best_effort(&record, "auto_loop_wait");
    }
    Ok(())
}

async fn maybe_submit_armed_fib_entry(
    state: &FrontendAppState,
    record: &FibInstanceRecord,
) -> Result<()> {
    if state.dry_run || !fib_record_allows_background_live_actions(record) {
        return Ok(());
    }
    let snapshot_ms = record.updated_at_ms;
    if fib_background_stop_requested(state, record, snapshot_ms)? {
        return Ok(());
    }
    let base_config = state.config_snapshot()?;
    let market = resolve_market_profile(Some(&record.config.market), &base_config)?;
    let config = scoped_config_for_module_and_market(base_config, "fib", &market);
    if config.app.dry_run {
        return Ok(());
    }

    let current_price =
        fetch_fib_reference_price(&config, &market, &record.config.coin, Some(&state.realtime))
            .await?;
    let mut next = record.clone();
    next.config.current_price = current_price;
    next.plan = build_basic_plan(&next.config)?;
    let coordinator_signals = fib_coordinator_signals_from_plan(&next.config, &next.plan)?;
    let entry_signals = fib_entry_signal_responses_from_signals(&coordinator_signals, &next.plan);
    next.entry_signal_ids = entry_signals
        .iter()
        .map(|signal| signal.signal_id.clone())
        .collect();

    if entry_signals.is_empty() {
        if fib_should_wait_for_auto_replan(&next) {
            mark_fib_record_auto_loop_retry_wait(
                state,
                &next.strategy_id,
                &format!(
                    "{}; unlocked range will be recalculated after cooldown",
                    fib_waiting_for_entry_message(&next)
                ),
            )?;
            return Ok(());
        }
        next.entry_order_refs.clear();
        next.protective_order_refs.clear();
        next.status = FibInstanceStatus::ArmedUnfilled;
        next.last_message = Some(fib_waiting_for_entry_message(&next));
        next.updated_at_ms = now_ms();
        upsert_fib_instance(state, next)?;
        return Ok(());
    }

    if fib_background_stop_requested(state, &next, snapshot_ms)? {
        return Ok(());
    }
    let (entry_reports, protection_reports) =
        execute_fib_entry_signals(state, &next, &coordinator_signals, next.dry_run, next.live)
            .await?;
    let has_live_fill = entry_reports.iter().any(worker_report_has_live_fill);
    next.entry_order_refs =
        fib_entry_order_refs_from_reports(&coordinator_signals, &entry_reports, &next.plan);
    next.protective_order_refs = fib_protective_order_refs_from_reports(&protection_reports);
    let sync = fib_entry_sync_assessment(&coordinator_signals, &entry_reports);
    if !sync.is_complete() {
        let resting_refs = fib_resting_entry_order_refs_from_reports(
            &coordinator_signals,
            &entry_reports,
            &next.plan,
        );
        let cancel_reports =
            cancel_incomplete_fib_entry_orders(state, &next, resting_refs, next.dry_run, next.live)
                .await?;
        remove_successfully_cancelled_fib_entry_refs(&mut next, &cancel_reports);
        mark_fib_record_incomplete_entry_submission(
            &mut next,
            &sync,
            &entry_reports,
            has_live_fill,
            &cancel_reports,
        );
    } else {
        next.status = if has_live_fill && fib_all_target_accounts_have_complete_protection(&next) {
            FibInstanceStatus::Protected
        } else if has_live_fill && !protection_reports.is_empty() {
            FibInstanceStatus::ProtectionPending
        } else if has_live_fill {
            FibInstanceStatus::EntryFilled
        } else if next.entry_order_refs.is_empty() {
            FibInstanceStatus::ArmedUnfilled
        } else {
            FibInstanceStatus::EntryPending
        };
        next.last_message = Some(if next.entry_order_refs.is_empty() && !has_live_fill {
            fib_execution_message(
                "armed Fib entry condition matched, but no live entry order was accepted",
                &entry_reports,
                &protection_reports,
            )
        } else {
            fib_execution_message(
                "armed Fib entry condition matched; entry submitted",
                &entry_reports,
                &protection_reports,
            )
        });
    }
    if fib_background_stop_requested(state, &next, snapshot_ms)? {
        next.config.auto_loop = false;
        next.status = if has_live_fill && !next.protective_order_refs.is_empty() {
            FibInstanceStatus::Protected
        } else if has_live_fill {
            FibInstanceStatus::EntryFilled
        } else {
            FibInstanceStatus::Killed
        };
        next.last_message = Some(
            "stop requested while Fib entry was being submitted; no further Fib cycles will be armed"
                .to_string(),
        );
    }
    next.updated_at_ms = now_ms();
    upsert_fib_instance(state, next)?;
    Ok(())
}

fn fib_should_wait_for_auto_replan(record: &FibInstanceRecord) -> bool {
    record.config.auto_loop && !record.config.locked_range
}

fn fib_waiting_for_entry_message(record: &FibInstanceRecord) -> String {
    let nearest = record.plan.levels.iter().min_by(|left, right| {
        left.current_distance_usd
            .partial_cmp(&right.current_distance_usd)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let Some(level) = nearest else {
        return "strategy armed; no Fib entry level is configured".to_string();
    };
    match (record.config.execution_mode, record.config.direction) {
        (ExecutionMode::Maker, FibTradeDirection::Long)
            if record.config.current_price <= level.entry_price =>
        {
            format!(
                "strategy armed; maker entry was missed for long because current price {:.6} is below Fib entry {:.6}; waiting for price to recover above the entry before posting a limit order",
                record.config.current_price, level.entry_price
            )
        }
        (ExecutionMode::Maker, FibTradeDirection::Short)
            if record.config.current_price >= level.entry_price =>
        {
            format!(
                "strategy armed; maker entry was missed for short because current price {:.6} is above Fib entry {:.6}; waiting for price to recover below the entry before posting a limit order",
                record.config.current_price, level.entry_price
            )
        }
        (ExecutionMode::Maker, direction) => {
            let label = match direction {
                FibTradeDirection::Long => "long",
                FibTradeDirection::Short => "short",
            };
            format!(
                "strategy armed; waiting to post maker {label} entry at {:.6}; current price {:.6}",
                level.entry_price, record.config.current_price
            )
        }
        (ExecutionMode::Taker, _) => format!(
            "strategy armed; waiting for current price {:.6} to enter {:.6}-{:.6}",
            record.config.current_price, level.entry_zone_low, level.entry_zone_high
        ),
    }
}

async fn maybe_restart_completed_fib_cycle(
    state: &FrontendAppState,
    record: &FibInstanceRecord,
) -> Result<()> {
    if !record.config.auto_loop
        || state.dry_run
        || !fib_record_allows_background_live_actions(record)
    {
        return Ok(());
    }
    let elapsed_ms = record
        .last_cycle_completed_at_ms
        .map(|completed_at| now_ms().saturating_sub(completed_at))
        .unwrap_or(u64::MAX);
    let cooldown_ms = fib_restart_cooldown_secs(record).saturating_mul(1000);
    if elapsed_ms < cooldown_ms {
        return Ok(());
    }
    restart_fib_cycle(state, record).await
}

fn fib_restart_cooldown_secs(record: &FibInstanceRecord) -> u64 {
    match record.last_cycle_exit_kind.as_deref() {
        Some("stop_loss") => record.config.stop_loss_cooldown_secs,
        Some("take_profit") => 0,
        _ => record.config.cooldown_secs,
    }
}

fn fib_record_allows_background_live_actions(record: &FibInstanceRecord) -> bool {
    record.live && !record.dry_run
}

fn fib_should_stop_after_cycle(record: &FibInstanceRecord, exit_kind: Option<&str>) -> bool {
    exit_kind == Some("stop_loss") && record.config.stop_loss_stop_strategy
}

fn fib_background_stop_requested(
    state: &FrontendAppState,
    record: &FibInstanceRecord,
    snapshot_ms: u64,
) -> Result<bool> {
    if state.fib_stop_requested_after(&record.strategy_id, snapshot_ms)? {
        return Ok(true);
    }
    let guard = state
        .fib_instances
        .read()
        .map_err(|_| anyhow::anyhow!("fib instance lock is poisoned"))?;
    Ok(guard.get(&record.strategy_id).is_some_and(|current| {
        matches!(current.status, FibInstanceStatus::Killed) || !current.config.auto_loop
    }))
}

async fn restart_fib_cycle(state: &FrontendAppState, previous: &FibInstanceRecord) -> Result<()> {
    let snapshot_ms = previous.updated_at_ms;
    if fib_background_stop_requested(state, previous, snapshot_ms)? {
        return Ok(());
    }
    let payload = fib_payload_from_record(previous);
    let mut record = build_fib_instance_record(
        state,
        payload,
        FibInstanceStatus::ArmedUnfilled,
        Some(previous),
    )
    .await?;
    record.config.validate_execution()?;
    record.entry_order_refs.clear();
    record.protective_order_refs.clear();
    let coordinator_signals = fib_coordinator_signals_from_plan(&record.config, &record.plan)?;
    let entry_signals = fib_entry_signal_responses_from_signals(&coordinator_signals, &record.plan);
    record.entry_signal_ids = entry_signals
        .iter()
        .map(|signal| signal.signal_id.clone())
        .collect();
    if fib_background_stop_requested(state, &record, snapshot_ms)? {
        return Ok(());
    }
    let (entry_reports, protection_reports) = execute_fib_entry_signals(
        state,
        &record,
        &coordinator_signals,
        record.dry_run,
        record.live,
    )
    .await?;
    let has_live_fill = entry_reports.iter().any(worker_report_has_live_fill);
    record.entry_order_refs =
        fib_entry_order_refs_from_reports(&coordinator_signals, &entry_reports, &record.plan);
    record.protective_order_refs = fib_protective_order_refs_from_reports(&protection_reports);
    let sync = fib_entry_sync_assessment(&coordinator_signals, &entry_reports);
    if !sync.is_complete() {
        let resting_refs = fib_resting_entry_order_refs_from_reports(
            &coordinator_signals,
            &entry_reports,
            &record.plan,
        );
        let cancel_reports = cancel_incomplete_fib_entry_orders(
            state,
            &record,
            resting_refs,
            record.dry_run,
            record.live,
        )
        .await?;
        remove_successfully_cancelled_fib_entry_refs(&mut record, &cancel_reports);
        mark_fib_record_incomplete_entry_submission(
            &mut record,
            &sync,
            &entry_reports,
            has_live_fill,
            &cancel_reports,
        );
    } else {
        record.status =
            if has_live_fill && fib_all_target_accounts_have_complete_protection(&record) {
                FibInstanceStatus::Protected
            } else if has_live_fill && !protection_reports.is_empty() {
                FibInstanceStatus::ProtectionPending
            } else if has_live_fill {
                FibInstanceStatus::EntryFilled
            } else if entry_signals.is_empty() || record.entry_order_refs.is_empty() {
                FibInstanceStatus::ArmedUnfilled
            } else {
                FibInstanceStatus::EntryPending
            };
        record.last_message = Some(if entry_signals.is_empty() {
            format!(
                "auto loop waiting; {}",
                fib_waiting_for_entry_message(&record)
            )
        } else if record.entry_order_refs.is_empty() && !has_live_fill {
            fib_execution_message(
                "auto loop skipped this cycle; no live entry order was accepted",
                &entry_reports,
                &protection_reports,
            )
        } else {
            fib_execution_message("auto loop restarted", &entry_reports, &protection_reports)
        });
    }
    if fib_background_stop_requested(state, &record, snapshot_ms)? {
        record.config.auto_loop = false;
        record.status = if has_live_fill && !record.protective_order_refs.is_empty() {
            FibInstanceStatus::Protected
        } else if has_live_fill {
            FibInstanceStatus::EntryFilled
        } else {
            FibInstanceStatus::Killed
        };
        record.last_message = Some(
            "stop requested while auto loop was restarting; no further Fib cycles will be armed"
                .to_string(),
        );
    }
    record.updated_at_ms = now_ms();
    upsert_fib_instance(state, record)?;
    Ok(())
}

fn fib_payload_from_record(record: &FibInstanceRecord) -> FibBasicPayload {
    FibBasicPayload {
        strategy_id: Some(record.strategy_id.clone()),
        direction: Some(record.config.direction.as_str().to_string()),
        market: Some(record.config.market.clone()),
        account_ids: record.config.account_ids.clone(),
        coin: record.config.coin.clone(),
        timeframe: record.config.timeframe.clone(),
        lookback_bars: record.config.lookback_bars,
        levels: record.config.levels.clone(),
        entry_above_tolerance_usd: record.config.entry_above_tolerance_usd,
        entry_below_tolerance_usd: record.config.entry_below_tolerance_usd,
        principal_usd: record.config.principal_usd,
        leverage: record.config.leverage,
        execution_mode: match record.config.execution_mode {
            ExecutionMode::Maker => "limit_post_only_alo".to_string(),
            ExecutionMode::Taker => "market_ioc".to_string(),
        },
        take_profit_mode: match record.config.take_profit_mode {
            FibProfitLossMode::PriceDeltaUsd => "price_delta_usd".to_string(),
            FibProfitLossMode::PrincipalPercent => "principal_percent".to_string(),
        },
        take_profit_value: record.config.take_profit_value,
        stop_loss_mode: match record.config.stop_loss_mode {
            FibProfitLossMode::PriceDeltaUsd => "price_delta_usd".to_string(),
            FibProfitLossMode::PrincipalPercent => "principal_percent".to_string(),
        },
        stop_loss_value: record.config.stop_loss_value,
        max_slippage_bps: record.config.max_slippage_bps,
        max_entries_per_level: record.config.max_entries_per_level,
        cooldown_secs: record.config.cooldown_secs,
        stop_loss_cooldown_secs: record.config.stop_loss_cooldown_secs,
        stop_loss_stop_strategy: record.config.stop_loss_stop_strategy,
        locked_range: record.config.locked_range,
        locked_swing_high: Some(record.config.swing_high),
        locked_swing_low: Some(record.config.swing_low),
        auto_loop: record.config.auto_loop,
        dry_run: record.dry_run,
        live: record.live,
    }
}

async fn execute_fib_entry_signals(
    state: &FrontendAppState,
    record: &FibInstanceRecord,
    signals: &[CoordinatorSignal],
    dry_run_requested: bool,
    live_requested: bool,
) -> Result<(Vec<WorkerReport>, Vec<ProtectiveExitArmResult>)> {
    if signals.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    if state.fib_stop_requested_after(&record.strategy_id, record.updated_at_ms)? {
        anyhow::bail!(
            "fib strategy {} has a pending stop request; refusing to submit entry signals",
            record.strategy_id
        );
    }

    let base_config = state.config_snapshot()?;
    let market = resolve_market_profile(Some(&record.config.market), &base_config)?;
    let config = scoped_config_for_module_and_market(base_config.clone(), "fib", &market);
    let execute_live = live_requested && !dry_run_requested;
    anyhow::ensure!(
        !execute_live || (!state.dry_run && !config.app.dry_run),
        "fib live execution requires frontend and config dry_run=false"
    );
    let vault_password = if execute_live {
        let path = Path::new(&config.secrets.vault_path);
        Some(state.resolve_vault_password(path, "")?)
    } else {
        None
    };

    let account_groups = fib_entry_signal_account_groups(signals);
    let account_futures = account_groups
        .into_iter()
        .map(|(account_id, account_signals)| {
            let account = config.account(&account_id).cloned();
            let config = config.clone();
            let base_config = base_config.clone();
            let market = market.clone();
            let record = record.clone();
            let vault_password = vault_password.clone();
            let state = state.clone();
            async move {
                let mut account_entry_reports = Vec::new();
                let mut account_protection_reports = Vec::new();

                if state.fib_stop_requested_after(&record.strategy_id, record.updated_at_ms)? {
                    for signal in account_signals {
                        account_entry_reports.push(WorkerReport::Rejected(
                            crate::domain::RejectedIntent {
                                signal_id: signal.signal_id,
                                worker_id: format!("worker-{account_id}"),
                                account_id: account_id.clone(),
                                reason_code: "FIB_STOP_REQUESTED".to_string(),
                                message: format!(
                                    "fib strategy {} was stopped before this account submitted",
                                    record.strategy_id
                                ),
                                rejected_at_ms: now_ms(),
                            },
                        ));
                    }
                    return Ok::<_, anyhow::Error>((
                        account_entry_reports,
                        account_protection_reports,
                    ));
                }

                let Some(account) = account else {
                    for signal in account_signals {
                        account_entry_reports.push(WorkerReport::Rejected(
                            crate::domain::RejectedIntent {
                                signal_id: signal.signal_id,
                                worker_id: format!("worker-{account_id}"),
                                account_id: account_id.clone(),
                                reason_code: "ACCOUNT_NOT_CONFIGURED".to_string(),
                                message: format!("account {account_id} not found in config"),
                                rejected_at_ms: now_ms(),
                            },
                        ));
                    }
                    return Ok::<_, anyhow::Error>((
                        account_entry_reports,
                        account_protection_reports,
                    ));
                };

                let worker_id = format!("worker-{}", account.account_id);
                if execute_live
                    && !market.is_spot()
                    && let Err(error) = execute_fib_residual_cleanup_if_needed(
                        &config,
                        &market,
                        &account,
                        &record.config.coin,
                        record.config.max_slippage_bps,
                        vault_password.as_deref(),
                        "pre-entry",
                    )
                    .await
                {
                    for _ in account_signals {
                        account_entry_reports.push(WorkerReport::Error(
                            crate::domain::WorkerError {
                                worker_id: worker_id.clone(),
                                account_id: account.account_id.clone(),
                                message: error.to_string(),
                                error_at_ms: now_ms(),
                            },
                        ));
                    }
                    return Ok::<_, anyhow::Error>((
                        account_entry_reports,
                        account_protection_reports,
                    ));
                }

                let mut approved_orders = Vec::new();
                for signal in account_signals {
                    if state.fib_stop_requested_after(&record.strategy_id, record.updated_at_ms)? {
                        account_entry_reports.push(WorkerReport::Rejected(
                            crate::domain::RejectedIntent {
                                signal_id: signal.signal_id,
                                worker_id: worker_id.clone(),
                                account_id: account.account_id.clone(),
                                reason_code: "FIB_STOP_REQUESTED".to_string(),
                                message: format!(
                                    "fib strategy {} was stopped before this account submitted",
                                    record.strategy_id
                                ),
                                rejected_at_ms: now_ms(),
                            },
                        ));
                        continue;
                    }

                    let signal = refresh_fib_signal_for_submission(&signal);
                    let intent =
                        signal.to_trade_intent(&account.account_id, &worker_id, account.copy_ratio);
                    let risk_context = RiskContext::from_account_for_module(
                        &config,
                        &account,
                        !execute_live,
                        "fib",
                    );
                    let report =
                        match RiskGateway::dry_run_default().evaluate(&risk_context, intent) {
                            RiskDecision::Approved(order) => {
                                approved_orders.push(order);
                                continue;
                            }
                            RiskDecision::Rejected(rejection) => WorkerReport::Rejected(rejection),
                        };

                    account_entry_reports.push(report);
                }

                if !approved_orders.is_empty() {
                    let executor = if execute_live {
                        ensure_live_account_address(&account)?;
                        let secret =
                            load_account_secret(&config, &account, vault_password.as_deref())?;
                        AccountExecutor::live(config.clone(), account.clone(), secret)
                    } else {
                        AccountExecutor::dry_run(true)
                    };
                    let mut submit_reports = executor.submit_bulk(approved_orders).await;
                    account_entry_reports.append(&mut submit_reports);
                }

                if let Some(protection) = maybe_arm_fib_protection_after_entry_reports(
                    &base_config,
                    &record,
                    &account,
                    &account_entry_reports,
                    execute_live,
                    vault_password.as_deref(),
                )
                .await?
                {
                    account_protection_reports.push(protection);
                }

                Ok::<_, anyhow::Error>((account_entry_reports, account_protection_reports))
            }
        });

    let mut entry_reports = Vec::new();
    let mut protection_reports = Vec::new();
    for result in join_all(account_futures).await {
        let (mut account_entry_reports, mut account_protection_reports) = result?;
        entry_reports.append(&mut account_entry_reports);
        protection_reports.append(&mut account_protection_reports);
    }
    Ok((entry_reports, protection_reports))
}

#[derive(Debug, Clone, Copy)]
struct FibFilledEntryAggregate {
    avg_entry_price: f64,
    filled_size: f64,
    notional_usd: f64,
}

fn fib_filled_entry_aggregate(reports: &[WorkerReport]) -> Option<FibFilledEntryAggregate> {
    let mut filled_size = 0.0;
    let mut filled_notional = 0.0;
    for report in reports {
        let WorkerReport::Submitted(submitted) = report else {
            continue;
        };
        let Some(entry_price) = submitted.avg_fill_price else {
            continue;
        };
        let size = submitted.filled_size.unwrap_or_default().abs();
        if size <= 0.0 || entry_price <= 0.0 {
            continue;
        }
        filled_size += size;
        filled_notional += size * entry_price;
    }
    if filled_size <= 0.0 || filled_notional <= 0.0 {
        return None;
    }
    Some(FibFilledEntryAggregate {
        avg_entry_price: filled_notional / filled_size,
        filled_size,
        notional_usd: filled_notional,
    })
}

async fn maybe_arm_fib_protection_after_entry_reports(
    base_config: &AppConfig,
    record: &FibInstanceRecord,
    account: &AccountConfig,
    reports: &[WorkerReport],
    execute_live: bool,
    vault_password: Option<&str>,
) -> Result<Option<ProtectiveExitArmResult>> {
    let Some(aggregate) = fib_filled_entry_aggregate(reports) else {
        return Ok(None);
    };

    let take_profit_trigger_price =
        fib_take_profit_trigger_from_entry(&record.config, aggregate.avg_entry_price)?;
    let stop_loss_trigger_price =
        fib_stop_loss_trigger_from_entry(&record.config, aggregate.avg_entry_price)?;
    let market = resolve_market_profile(Some(&record.config.market), base_config)?;
    let config = scoped_config_for_module_and_market(base_config.clone(), "fib", &market);
    let notional_usd = aggregate
        .notional_usd
        .max(aggregate.filled_size * aggregate.avg_entry_price);
    let options = ProtectiveExitArmOptions {
        exit: ProtectiveExitOptions {
            account_id: account.account_id.clone(),
            coin: record.config.coin.clone(),
            entry_side: match record.config.direction {
                FibTradeDirection::Long => "buy",
                FibTradeDirection::Short => "sell",
            }
            .to_string(),
            entry_price: Some(aggregate.avg_entry_price),
            notional_usd,
            take_profit_usd: 0.0,
            stop_loss_pct: 0.0,
            take_profit_trigger_price: Some(take_profit_trigger_price),
            stop_loss_trigger_price: Some(stop_loss_trigger_price),
            max_slippage_bps: record.config.max_slippage_bps,
        },
        submit: execute_live,
        confirm_mainnet_live: execute_live,
    };
    let mut result =
        execute_protective_exit_arm(config, options, !execute_live, vault_password).await?;
    result.persistent_rule_id = Some(format!(
        "fib:{}:{}:aggregate",
        record.config.strategy_id, account.account_id
    ));
    Ok(Some(result))
}

fn fib_take_profit_trigger_from_entry(config: &FibBasicConfig, entry_price: f64) -> Result<f64> {
    fib_take_profit_price(
        config.direction,
        entry_price,
        config.leverage.max(1.0),
        config.take_profit_mode,
        config.take_profit_value,
    )
}

fn fib_stop_loss_trigger_from_entry(config: &FibBasicConfig, entry_price: f64) -> Result<f64> {
    fib_stop_loss_price(
        config.direction,
        entry_price,
        config.leverage.max(1.0),
        config.stop_loss_mode,
        config.stop_loss_value,
    )
}

fn fib_instance_pair_key(record: &FibInstanceRecord) -> (String, String) {
    (
        record.config.market.trim().to_ascii_lowercase(),
        record.config.coin.trim().to_ascii_lowercase(),
    )
}

fn fib_instance_reserves_pair(record: &FibInstanceRecord) -> bool {
    match record.status {
        FibInstanceStatus::Draft | FibInstanceStatus::Killed => false,
        FibInstanceStatus::Completed => record.config.auto_loop,
        FibInstanceStatus::ArmedUnfilled
        | FibInstanceStatus::EntryPending
        | FibInstanceStatus::EntryFilled
        | FibInstanceStatus::ProtectionPending
        | FibInstanceStatus::Protected
        | FibInstanceStatus::Exiting
        | FibInstanceStatus::Paused
        | FibInstanceStatus::Error => true,
    }
}

fn ensure_fib_pair_available(
    state: &FrontendAppState,
    candidate: &FibInstanceRecord,
    allow_strategy_id: Option<&str>,
) -> Result<()> {
    let candidate_key = fib_instance_pair_key(candidate);
    let guard = state
        .fib_instances
        .read()
        .map_err(|_| anyhow::anyhow!("fib instance lock is poisoned"))?;
    let allowed = allow_strategy_id.unwrap_or_default();
    if let Some(existing) = guard.values().find(|existing| {
        fib_instance_reserves_pair(existing)
            && existing.strategy_id == candidate.strategy_id
            && (allowed.is_empty() || existing.strategy_id != allowed)
    }) {
        anyhow::bail!(
            "fib strategy conflict: strategy id {} is already active for {} / {} (status: {}). Stop the old strategy before reusing this strategy id.",
            existing.strategy_id,
            existing.config.market,
            existing.config.coin,
            fib_instance_status_key(existing.status)
        );
    }
    if let Some(conflict) = guard.values().find(|existing| {
        fib_instance_reserves_pair(existing)
            && fib_instance_pair_key(existing) == candidate_key
            && (allowed.is_empty() || existing.strategy_id != allowed)
    }) {
        anyhow::bail!(
            "fib strategy conflict: {} / {} already has active strategy {} (status: {}). Stop the old strategy before starting another Fib strategy for the same trading pair.",
            candidate.config.market,
            candidate.config.coin,
            conflict.strategy_id,
            fib_instance_status_key(conflict.status)
        );
    }
    Ok(())
}

fn upsert_fib_instance(state: &FrontendAppState, record: FibInstanceRecord) -> Result<()> {
    let mut guard = state
        .fib_instances
        .write()
        .map_err(|_| anyhow::anyhow!("fib instance lock is poisoned"))?;
    guard.insert(record.strategy_id.clone(), record.clone());
    persist_fib_instances_best_effort(&guard);
    drop(guard);
    append_fib_instance_history_best_effort(&record, "upsert");
    Ok(())
}

fn fib_history_entry_from_record(
    record: &FibInstanceRecord,
    action: &str,
    source: &str,
    occurred_at_ms: u64,
    recovered_from_audit: bool,
    details: Value,
) -> FibInstanceHistoryEntry {
    let seed = format!(
        "{}:{}:{}:{}:{}",
        source,
        action,
        record.strategy_id,
        fib_instance_status_key(record.status),
        occurred_at_ms
    );
    FibInstanceHistoryEntry {
        history_id: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, seed.as_bytes()).to_string(),
        occurred_at_ms,
        source: source.to_string(),
        action: action.to_string(),
        strategy_id: record.strategy_id.clone(),
        status: fib_instance_status_key(record.status).to_string(),
        market: record.config.market.clone(),
        coin: record.config.coin.clone(),
        timeframe: record.config.timeframe.clone(),
        message: record.last_message.clone(),
        recovered_from_audit,
        details,
        record: Some(record.clone()),
    }
}

fn append_fib_instance_history_best_effort(record: &FibInstanceRecord, action: &str) {
    if fib_persistence_disabled_in_tests() {
        return;
    }
    if let Err(error) = append_fib_instance_history(record, action) {
        tracing::warn!(
            %error,
            path = FIB_INSTANCE_HISTORY_PATH,
            strategy_id = %record.strategy_id,
            "failed to append Fib instance history"
        );
    }
}

fn append_fib_instance_history(record: &FibInstanceRecord, action: &str) -> Result<()> {
    let entry = fib_history_entry_from_record(
        record,
        action,
        "fib_instance_store",
        record.updated_at_ms.max(record.created_at_ms),
        false,
        json!({
            "entry_order_ref_count": record.entry_order_refs.len(),
            "protective_order_ref_count": record.protective_order_refs.len(),
            "completed_cycles": record.completed_cycles,
            "last_cycle_exit_kind": record.last_cycle_exit_kind,
            "auto_loop": record.config.auto_loop,
            "stop_loss_cooldown_secs": record.config.stop_loss_cooldown_secs,
            "stop_loss_stop_strategy": record.config.stop_loss_stop_strategy,
        }),
    );
    append_fib_history_entry(&entry)
}

fn append_fib_history_entry(entry: &FibInstanceHistoryEntry) -> Result<()> {
    let path = Path::new(FIB_INSTANCE_HISTORY_PATH);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create Fib history storage directory {}",
                parent.display()
            )
        })?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open Fib instance history log {}", path.display()))?;
    let mut line =
        serde_json::to_vec(entry).context("failed to serialize Fib instance history entry")?;
    line.push(b'\n');
    file.write_all(&line).with_context(|| {
        format!(
            "failed to write Fib instance history log {}",
            path.display()
        )
    })?;
    Ok(())
}

fn build_fib_history_response(
    state: &FrontendAppState,
    limit: usize,
) -> Result<FibHistoryResponse> {
    let config = state.config_snapshot()?;
    let mut entries = read_fib_instance_history_entries(limit.saturating_mul(4).max(limit))?;
    let active_records = state
        .fib_instances
        .read()
        .map_err(|_| anyhow::anyhow!("fib instance lock is poisoned"))?
        .values()
        .cloned()
        .collect::<Vec<_>>();

    let mut known_strategy_ids = entries
        .iter()
        .map(|entry| entry.strategy_id.clone())
        .collect::<HashSet<_>>();
    for record in &active_records {
        known_strategy_ids.insert(record.strategy_id.clone());
        entries.push(fib_history_entry_from_record(
            record,
            "current_snapshot",
            "fib_instance_store",
            record.updated_at_ms.max(record.created_at_ms),
            false,
            json!({
                "entry_order_ref_count": record.entry_order_refs.len(),
                "protective_order_ref_count": record.protective_order_refs.len(),
                "completed_cycles": record.completed_cycles,
                "last_cycle_exit_kind": record.last_cycle_exit_kind,
                "auto_loop": record.config.auto_loop,
                "stop_loss_cooldown_secs": record.config.stop_loss_cooldown_secs,
                "stop_loss_stop_strategy": record.config.stop_loss_stop_strategy,
            }),
        ));
    }

    entries.extend(recover_fib_history_from_audit(
        &config,
        &known_strategy_ids,
        limit,
    )?);
    entries.sort_by_key(|entry| std::cmp::Reverse(entry.occurred_at_ms));

    let mut seen = HashSet::<String>::new();
    entries.retain(|entry| seen.insert(entry.history_id.clone()));
    entries.truncate(limit);

    Ok(FibHistoryResponse {
        entries,
        fetched_at_ms: now_ms(),
    })
}

fn read_fib_instance_history_entries(limit: usize) -> Result<Vec<FibInstanceHistoryEntry>> {
    if fib_persistence_disabled_in_tests() {
        return Ok(Vec::new());
    }
    if limit == 0 {
        return Ok(Vec::new());
    }
    let path = Path::new(FIB_INSTANCE_HISTORY_PATH);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = fs::read(path)
        .with_context(|| format!("failed to read Fib instance history {}", path.display()))?;
    let mut entries = Vec::new();
    for (line_from_end, line) in raw.split(|byte| *byte == b'\n').rev().enumerate() {
        let text = String::from_utf8_lossy(line);
        let text = text.trim();
        if text.is_empty() {
            continue;
        }
        let entry = match serde_json::from_str::<FibInstanceHistoryEntry>(text) {
            Ok(entry) => entry,
            Err(error) => {
                tracing::warn!(
                    path = %path.display(),
                    line_from_end = line_from_end + 1,
                    error = %error,
                    "skipping malformed Fib instance history line"
                );
                continue;
            }
        };
        entries.push(entry);
        if entries.len() >= limit {
            break;
        }
    }
    Ok(entries)
}

fn recover_fib_history_from_audit(
    config: &AppConfig,
    known_strategy_ids: &HashSet<String>,
    limit: usize,
) -> Result<Vec<FibInstanceHistoryEntry>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let audit_events = read_recent_audit_events(
        Path::new(&config.storage.audit_log_path),
        FIB_HISTORY_RECOVERY_SCAN_LIMIT,
    )?;
    let mut recovered = Vec::new();
    for event in audit_events {
        if !matches!(
            event.action.as_str(),
            "fib_instance_create"
                | "fib_instance_start"
                | "fib_instance_refresh_params"
                | "fib_instance_cancel"
        ) {
            continue;
        }
        let Some(strategy_id) = detail_str(&event.details, "strategy_id") else {
            continue;
        };
        let live_event = detail_bool(&event.details, "live").unwrap_or(false);
        let dry_run_event = detail_bool(&event.details, "dry_run").unwrap_or(true);
        if !event.ok || !live_event || dry_run_event {
            continue;
        }
        if known_strategy_ids.contains(strategy_id) {
            continue;
        }
        recovered.push(fib_history_entry_from_audit_event(&event, strategy_id));
        if recovered.len() >= limit {
            break;
        }
    }
    Ok(recovered)
}

fn fib_history_entry_from_audit_event(
    event: &AuditEvent,
    strategy_id: &str,
) -> FibInstanceHistoryEntry {
    let market = detail_str(&event.details, "market")
        .unwrap_or(MARKET_XYZ_PERP)
        .to_string();
    let coin = event_coin_text(event)
        .unwrap_or_else(|| detail_str(&event.details, "coin").unwrap_or("-"))
        .to_string();
    let timeframe = detail_str(&event.details, "timeframe")
        .unwrap_or("-")
        .to_string();
    let status = if !event.ok {
        "error"
    } else if event.action == "fib_instance_cancel" {
        "killed"
    } else {
        "history_only"
    };
    let message = if event.ok {
        Some(
            "historical Fib event recovered from audit log; no persisted live instance was recovered"
                .to_string(),
        )
    } else {
        Some(event_error_text(event))
    };
    FibInstanceHistoryEntry {
        history_id: format!("audit:{}", event.event_id),
        occurred_at_ms: event.occurred_at_ms,
        source: "audit_recovered".to_string(),
        action: event.action.clone(),
        strategy_id: strategy_id.to_string(),
        status: status.to_string(),
        market,
        coin,
        timeframe,
        message,
        recovered_from_audit: true,
        details: event.details.clone(),
        record: None,
    }
}

fn load_fib_instances_from_disk() -> Result<HashMap<String, FibInstanceRecord>> {
    if fib_persistence_disabled_in_tests() {
        return Ok(HashMap::new());
    }
    let path = Path::new(FIB_INSTANCES_PATH);
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let raw = fs::read_to_string(path).with_context(|| {
        format!("failed to read persisted Fib instances from {FIB_INSTANCES_PATH}")
    })?;
    if raw.trim().is_empty() {
        return Ok(HashMap::new());
    }
    let mut records = serde_json::from_str::<Vec<FibInstanceRecord>>(&raw).with_context(|| {
        format!("failed to parse persisted Fib instances from {FIB_INSTANCES_PATH}")
    })?;
    let mut migrated = false;
    for record in &mut records {
        migrated |= normalize_recovered_fib_instance(record);
    }
    if migrated {
        let instances = records
            .iter()
            .cloned()
            .map(|record| (record.strategy_id.clone(), record))
            .collect::<HashMap<_, _>>();
        persist_fib_instances_best_effort(&instances);
    }
    Ok(records
        .into_iter()
        .map(|record| (record.strategy_id.clone(), record))
        .collect())
}

fn normalize_recovered_fib_instance(record: &mut FibInstanceRecord) -> bool {
    let mut migrated = false;
    if fib_record_has_any_live_order_ref(record) && (record.dry_run || !record.live) {
        record.dry_run = false;
        record.live = true;
        migrated = true;
    }
    if matches!(record.status, FibInstanceStatus::Completed)
        && record.config.auto_loop
        && record.completed_cycles == 0
        && record.last_cycle_completed_at_ms.is_none()
        && record.entry_order_refs.is_empty()
        && record.protective_order_refs.is_empty()
    {
        record.status = FibInstanceStatus::ArmedUnfilled;
        if record
            .last_message
            .as_deref()
            .unwrap_or_default()
            .is_empty()
        {
            record.last_message =
                Some("strategy recovered as active and waiting for entry".to_string());
        }
        record.updated_at_ms = now_ms();
        migrated = true;
    }
    migrated
}

fn fib_record_has_any_live_order_ref(record: &FibInstanceRecord) -> bool {
    record
        .entry_order_refs
        .iter()
        .chain(record.protective_order_refs.iter())
        .any(|order_ref| !order_ref.dry_run)
}

fn persist_fib_instances_best_effort(instances: &HashMap<String, FibInstanceRecord>) {
    if fib_persistence_disabled_in_tests() {
        return;
    }
    if let Err(error) = persist_fib_instances(instances) {
        tracing::warn!(%error, path = FIB_INSTANCES_PATH, "failed to persist Fib instances");
    }
}

#[cfg(test)]
fn fib_persistence_disabled_in_tests() -> bool {
    true
}

#[cfg(not(test))]
fn fib_persistence_disabled_in_tests() -> bool {
    false
}

fn persist_fib_instances(instances: &HashMap<String, FibInstanceRecord>) -> Result<()> {
    let path = Path::new(FIB_INSTANCES_PATH);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create Fib instance storage directory {parent:?}")
        })?;
    }
    let mut records = instances.values().cloned().collect::<Vec<_>>();
    records.sort_by_key(|record| record.strategy_id.clone());
    let encoded = serde_json::to_vec_pretty(&records)
        .context("failed to serialize persisted Fib instances")?;
    fs::write(path, encoded).with_context(|| {
        format!("failed to write persisted Fib instances to {FIB_INSTANCES_PATH}")
    })?;
    Ok(())
}

async fn build_fib_auto_detect_response(
    config: &AppConfig,
    payload: FibAutoDetectPayload,
) -> Result<FibAutoDetectResponse> {
    anyhow::ensure!(
        payload.lookback_bars >= 20 && payload.lookback_bars <= 5000,
        "lookback_bars must be between 20 and 5000"
    );
    anyhow::ensure!(
        payload.entry_tolerance_usd >= 0.0,
        "entry_tolerance_usd must be >= 0"
    );
    anyhow::ensure!(
        payload.take_profit_usd > 0.0 && payload.take_profit_usd.is_finite(),
        "take_profit_usd must be positive"
    );
    anyhow::ensure!(
        payload.stop_loss_pct.is_finite() && (0.0..1.0).contains(&payload.stop_loss_pct),
        "stop_loss_pct must be within (0, 1)"
    );
    let levels = normalize_fib_levels(&payload.levels)?;
    let timeframe = payload.timeframe.trim().to_ascii_lowercase();
    let interval_ms = timeframe_interval_ms(&timeframe)?;
    let market = payload.market_profile(config)?;
    let direction = payload.trade_direction()?;
    anyhow::ensure!(
        !market.is_spot() || direction == FibTradeDirection::Long,
        "spot market does not support Fib short strategies"
    );
    let canonical_coin = normalize_coin_for_market(&market, &payload.coin);
    let end_time_ms = now_ms();
    let start_time_ms =
        end_time_ms.saturating_sub(interval_ms.saturating_mul(payload.lookback_bars as u64 + 8));
    let candles = fetch_fib_candles(
        config,
        &market,
        &canonical_coin,
        &timeframe,
        start_time_ms,
        end_time_ms,
    )
    .await
    .with_context(|| {
        format!(
            "failed to fetch candle snapshot for {} {}",
            canonical_coin, timeframe
        )
    })?;
    anyhow::ensure!(
        !candles.is_empty(),
        "candle snapshot returned no data for {} {}",
        canonical_coin,
        timeframe
    );

    let usable_start = candles.len().saturating_sub(payload.lookback_bars as usize);
    let usable = &candles[usable_start..];
    let mut latest_close = None;
    for candle in usable {
        let close = candle
            .c
            .parse::<f64>()
            .with_context(|| format!("invalid candle close {}", candle.c))?;
        anyhow::ensure!(close.is_finite(), "candle close must be finite");
        latest_close = Some(close);
    }
    let swing = infer_fib_swing(usable, direction)?;
    let swing_high = swing.swing_high;
    let swing_low = swing.swing_low;

    let current_price = fetch_fib_reference_price(config, &market, &canonical_coin, None)
        .await
        .or_else(|_| {
            latest_close.with_context(|| {
                format!(
                    "failed to derive current price for {} from market snapshot and candles",
                    canonical_coin
                )
            })
        })?;
    anyhow::ensure!(
        current_price.is_finite() && current_price > 0.0,
        "current price must be positive"
    );

    let strategy = FibRetracementStrategy::new(FibRetracementConfig {
        strategy_id: format!(
            "auto_detect_{}_{}",
            canonical_coin.replace(':', "_"),
            timeframe
        ),
        direction,
        coin: canonical_coin.clone(),
        timeframe: timeframe.clone(),
        swing_high,
        swing_low,
        levels: levels.clone(),
        entry_tolerance_usd: payload.entry_tolerance_usd,
        take_profit_usd: payload.take_profit_usd,
        stop_loss_pct: payload.stop_loss_pct,
        notional_usd: 1.0,
        execution_mode: ExecutionMode::Taker,
        max_slippage_bps: 20.0,
    });
    let plans = strategy
        .level_plan()
        .into_iter()
        .map(|plan| {
            let distance_usd = (current_price - plan.entry_price).abs();
            FibAutoLevelResponse {
                level: plan.level,
                entry_price: plan.entry_price,
                take_profit_price: plan.take_profit_price,
                stop_loss_price: plan.stop_loss_price,
                distance_usd,
                within_tolerance: distance_usd <= payload.entry_tolerance_usd,
            }
        })
        .collect::<Vec<_>>();

    let nearest = plans
        .iter()
        .min_by(|left, right| {
            left.distance_usd
                .partial_cmp(&right.distance_usd)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .cloned();
    let triggered_levels = plans
        .iter()
        .filter(|plan| plan.within_tolerance)
        .map(|plan| plan.level)
        .collect::<Vec<_>>();

    Ok(FibAutoDetectResponse {
        dry_run: config.app.dry_run,
        environment: config.app.environment.clone(),
        dex: config.hyperliquid.dex.clone(),
        direction,
        coin: canonical_coin,
        timeframe,
        lookback_bars: payload.lookback_bars,
        start_time_ms,
        end_time_ms,
        candles_used: usable.len(),
        swing_high,
        swing_low,
        swing_high_time_ms: swing.swing_high_time_ms,
        swing_low_time_ms: swing.swing_low_time_ms,
        current_price,
        entry_tolerance_usd: payload.entry_tolerance_usd,
        triggered: !triggered_levels.is_empty(),
        triggered_levels,
        nearest_level: nearest,
        levels: plans,
    })
}

#[derive(Debug, Serialize)]
struct AppStatusResponse {
    name: String,
    environment: String,
    dex: String,
    default_market: String,
    dry_run: bool,
    uptime_ms: u64,
    worker_count: usize,
    max_manual_order_notional_usd: f64,
}

#[derive(Debug, Serialize)]
struct ModuleSymbolPoliciesStateResponse {
    manual_blocked_symbols: Vec<String>,
    fib_blocked_symbols: Vec<String>,
    copy_blocked_symbols: Vec<String>,
}

#[derive(Debug, Serialize)]
struct AccountResponse {
    account_id: String,
    address: String,
    secret_id: String,
    transfer_secret_id: String,
    blocked_markets: Vec<String>,
    copy_ratio: f64,
    max_order_notional_usd: f64,
    equity_usd: f64,
    available_usdc: f64,
    unrealized_pnl_usd: f64,
}

#[derive(Debug, Serialize)]
struct WorkerResponse {
    worker_id: String,
    account_id: String,
    status: String,
    last_signal_latency_ms: u64,
}

#[derive(Debug, Serialize)]
struct PositionResponse {
    account_id: String,
    coin: String,
    size: f64,
    entry_price: f64,
    mark_price: f64,
    pnl_usd: f64,
}

#[derive(Debug, Serialize)]
struct PnlResponse {
    total_equity_usd: f64,
    total_available_usdc: f64,
    total_unrealized_pnl_usd: f64,
    daily_realized_pnl_usd: f64,
}

#[derive(Debug, Serialize)]
struct StrategyResponse {
    strategy_id: String,
    status: String,
    signal_count: u64,
}

#[derive(Debug, Serialize)]
struct EventResponse {
    event_id: String,
    market: String,
    source: String,
    event_type: String,
    account_id: Option<String>,
    coin: Option<String>,
    side: Option<String>,
    amount_usd: Option<f64>,
    pnl_usd: Option<f64>,
    message: String,
    occurred_at_ms: u64,
}

#[derive(Debug, Serialize)]
struct DashboardOpenOrdersResponse {
    open_orders: Vec<DashboardOpenOrderResponse>,
    fib_instances: Vec<DashboardFibInstanceResponse>,
    fetched_at_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
struct DashboardFibInstanceResponse {
    #[serde(flatten)]
    record: FibInstanceRecord,
    pnl_summary: FibStrategyPnlSummary,
}

#[derive(Debug, Clone, Serialize, Default)]
struct FibStrategyPnlSummary {
    precise: bool,
    attribution: String,
    completed_cycles: u64,
    total_closed_pnl_usd: f64,
    total_fee_usd: f64,
    total_net_pnl_usd: f64,
    total_notional_usd: f64,
    total_fill_count: usize,
    total_close_fill_count: usize,
    last_cycle_closed_pnl_usd: f64,
    last_cycle_fee_usd: f64,
    last_cycle_net_pnl_usd: f64,
    last_cycle_notional_usd: f64,
    last_cycle_fill_count: usize,
    last_cycle_close_fill_count: usize,
    open_position_size: f64,
    open_avg_entry_price: Option<f64>,
    open_unrealized_pnl_usd: Option<f64>,
    current_price: Option<f64>,
    account_summaries: Vec<FibStrategyAccountPnlSummary>,
    missing_account_ids: Vec<String>,
    matched_order_count: usize,
    updated_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
struct FibStrategyAccountPnlSummary {
    account_id: String,
    closed_pnl_usd: f64,
    fee_usd: f64,
    net_pnl_usd: f64,
    notional_usd: f64,
    fill_count: usize,
    close_fill_count: usize,
    open_position_size: f64,
    open_avg_entry_price: Option<f64>,
    open_unrealized_pnl_usd: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct DashboardOpenOrdersCancelPayload {
    #[serde(default)]
    market: Option<String>,
    #[serde(default)]
    dry_run: bool,
    #[serde(default)]
    live: bool,
    #[serde(default)]
    confirm_mainnet_live: bool,
}

#[derive(Debug, Serialize)]
struct DashboardOpenOrdersCancelResponse {
    environment: String,
    market: String,
    market_label: String,
    scope: String,
    dry_run: bool,
    target_count: usize,
    skipped_unowned_count: usize,
    skipped_protective_count: usize,
    stopped_without_open_orders_count: usize,
    cancel_reports: Vec<DashboardCancelOpenOrderReport>,
    stopped_strategy_ids: Vec<String>,
    open_orders_after: usize,
    open_owned_orders_after: usize,
    fetched_at_ms: u64,
}

#[derive(Debug, Serialize)]
struct DashboardCancelOpenOrderReport {
    account_id: String,
    coin: String,
    source_module: String,
    strategy_id: Option<String>,
    oid: Option<u64>,
    cloid: String,
    ok: bool,
    data: Option<CancelOpenOrderResult>,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct FibCancelOrderReport {
    account_id: String,
    coin: String,
    cloid: String,
    ok: bool,
    cancel_response: Option<String>,
    open_orders_after: Option<usize>,
    matching_open_after: Option<bool>,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct DashboardOpenOrderResponse {
    market: String,
    market_label: String,
    account_id: String,
    coin: String,
    source_module: String,
    strategy_id: Option<String>,
    strategy_status: Option<String>,
    timeframe: Option<String>,
    fib_level: Option<f64>,
    fib_line_version: Option<String>,
    swing_high: Option<f64>,
    swing_low: Option<f64>,
    entry_zone_high: Option<f64>,
    entry_zone_low: Option<f64>,
    planned_take_profit_price: Option<f64>,
    planned_stop_loss_price: Option<f64>,
    order_role: String,
    side: String,
    order_type: String,
    limit_price: Option<f64>,
    trigger_price: Option<f64>,
    size: Option<f64>,
    current_price: Option<f64>,
    distance_usd: Option<f64>,
    distance_pct: Option<f64>,
    cloid: String,
    oid: Option<u64>,
    exchange_open: bool,
    status: String,
    submitted_at_ms: u64,
    updated_at_ms: u64,
}

async fn build_dashboard_open_orders_response(
    state: &FrontendAppState,
) -> Result<DashboardOpenOrdersResponse> {
    let config = state.config_snapshot()?;
    let mut fib_instances = state
        .fib_instances
        .read()
        .map_err(|_| anyhow::anyhow!("fib instance lock is poisoned"))?
        .values()
        .cloned()
        .collect::<Vec<_>>();
    let mut open_order_cache = HashMap::<String, Vec<OpenOrder>>::new();
    if recover_fib_entry_refs_from_exchange_open_orders(
        state,
        &config,
        &state.realtime,
        &fib_instances,
        &mut open_order_cache,
    )
    .await?
    {
        fib_instances = state
            .fib_instances
            .read()
            .map_err(|_| anyhow::anyhow!("fib instance lock is poisoned"))?
            .values()
            .cloned()
            .collect::<Vec<_>>();
    }
    let mut open_orders = build_dashboard_fib_open_orders(
        &config,
        &state.realtime,
        &fib_instances,
        &mut open_order_cache,
    )
    .await;
    let manual_open_orders = build_dashboard_manual_open_orders(
        &config,
        &state.realtime,
        &open_orders,
        &mut open_order_cache,
    )
    .await;
    open_orders.extend(manual_open_orders);
    if reconcile_fib_order_ref_oids_from_dashboard(state, &open_orders)? {
        fib_instances = state
            .fib_instances
            .read()
            .map_err(|_| anyhow::anyhow!("fib instance lock is poisoned"))?
            .values()
            .cloned()
            .collect::<Vec<_>>();
    }
    open_orders.sort_by(|left, right| {
        left.market
            .cmp(&right.market)
            .then(left.strategy_id.cmp(&right.strategy_id))
            .then(left.account_id.cmp(&right.account_id))
            .then(left.submitted_at_ms.cmp(&right.submitted_at_ms))
    });
    let fib_history = read_fib_instance_history_entries(FIB_HISTORY_RECOVERY_SCAN_LIMIT)
        .unwrap_or_else(|error| {
            tracing::warn!(
                %error,
                "failed to read Fib history while building dashboard strategy PnL"
            );
            Vec::new()
        });
    let fib_instances = fib_instances
        .into_iter()
        .map(|record| {
            let pnl_summary =
                build_fib_strategy_pnl_summary(&config, &state.realtime, &record, &fib_history);
            DashboardFibInstanceResponse {
                record,
                pnl_summary,
            }
        })
        .collect();
    Ok(DashboardOpenOrdersResponse {
        open_orders,
        fib_instances,
        fetched_at_ms: now_ms(),
    })
}

#[derive(Debug, Clone, Default)]
struct FibPnlAccumulator {
    closed_pnl_usd: f64,
    fee_usd: f64,
    notional_usd: f64,
    fill_count: usize,
    close_fill_count: usize,
    open_size: f64,
    open_cost_usd: f64,
}

impl FibPnlAccumulator {
    fn add_fill(&mut self, fill: &UserFill) {
        self.fill_count = self.fill_count.saturating_add(1);
        self.notional_usd += fill_amount_usd(fill).unwrap_or_default();
        self.fee_usd += fill_fee_usd(fill);
        if let Some(pnl) = fill_pnl_usd(fill) {
            self.closed_pnl_usd += pnl;
            self.close_fill_count = self.close_fill_count.saturating_add(1);
        }
        self.apply_open_position_fill(fill);
    }

    fn net_pnl_usd(&self) -> f64 {
        self.closed_pnl_usd - self.fee_usd
    }

    fn open_avg_entry_price(&self) -> Option<f64> {
        (self.open_size > FIB_COMPLETION_RESIDUAL_POSITION_EPSILON)
            .then_some(self.open_cost_usd / self.open_size)
            .filter(|price| price.is_finite() && *price > 0.0)
    }

    fn open_unrealized_pnl_usd(&self, current_price: Option<f64>) -> Option<f64> {
        let current_price = current_price.filter(|price| price.is_finite() && *price > 0.0)?;
        let avg = self.open_avg_entry_price()?;
        Some((current_price - avg) * self.open_size)
    }

    fn apply_open_position_fill(&mut self, fill: &UserFill) {
        let price = parse_decimal(&fill.px);
        let size = parse_decimal(&fill.sz).abs();
        if price <= 0.0 || size <= 0.0 {
            return;
        }
        if fill_is_buy(fill) {
            self.open_cost_usd += price * size;
            self.open_size += size;
            return;
        }
        if !fill_is_sell(fill) || self.open_size <= FIB_COMPLETION_RESIDUAL_POSITION_EPSILON {
            return;
        }
        let close_size = size.min(self.open_size);
        let avg_cost = if self.open_size > FIB_COMPLETION_RESIDUAL_POSITION_EPSILON {
            self.open_cost_usd / self.open_size
        } else {
            0.0
        };
        self.open_size = (self.open_size - close_size).max(0.0);
        self.open_cost_usd = (self.open_cost_usd - avg_cost * close_size).max(0.0);
        if self.open_size <= FIB_COMPLETION_RESIDUAL_POSITION_EPSILON {
            self.open_size = 0.0;
            self.open_cost_usd = 0.0;
        }
    }
}

fn build_fib_strategy_pnl_summary(
    config: &AppConfig,
    realtime: &RealtimeState,
    record: &FibInstanceRecord,
    history: &[FibInstanceHistoryEntry],
) -> FibStrategyPnlSummary {
    let Ok(market) = resolve_market_profile(Some(&record.config.market), config) else {
        return FibStrategyPnlSummary {
            completed_cycles: record.completed_cycles,
            attribution: "market_unavailable".to_string(),
            updated_at_ms: now_ms(),
            ..Default::default()
        };
    };
    let order_oids = fib_strategy_order_oids(record, history);
    let current_price = fib_strategy_current_price(realtime, &market, record)
        .or_else(|| (record.plan.current_price > 0.0).then_some(record.plan.current_price))
        .or_else(|| (record.config.current_price > 0.0).then_some(record.config.current_price));
    let last_cycle_window = fib_strategy_last_cycle_window(record, history);
    let mut total = FibPnlAccumulator::default();
    let mut last_cycle = FibPnlAccumulator::default();
    let mut account_summaries = Vec::new();
    let mut missing_account_ids = Vec::new();

    for account_id in &record.config.account_ids {
        let Some(account) = config.account(account_id) else {
            missing_account_ids.push(account_id.clone());
            continue;
        };
        let Some(fills) = realtime.fills(market.id, &account.address) else {
            missing_account_ids.push(account.account_id.clone());
            continue;
        };
        let mut account_total = FibPnlAccumulator::default();
        for fill in fills
            .iter()
            .filter(|fill| fib_fill_matches_strategy(&market, record, fill, &order_oids))
        {
            total.add_fill(fill);
            account_total.add_fill(fill);
            if last_cycle_window.is_some_and(|(start, end)| fill.time >= start && fill.time <= end)
            {
                last_cycle.add_fill(fill);
            }
        }
        if account_total.fill_count == 0 {
            missing_account_ids.push(account.account_id.clone());
        }
        account_summaries.push(FibStrategyAccountPnlSummary {
            account_id: account.account_id.clone(),
            closed_pnl_usd: account_total.closed_pnl_usd,
            fee_usd: account_total.fee_usd,
            net_pnl_usd: account_total.net_pnl_usd(),
            notional_usd: account_total.notional_usd,
            fill_count: account_total.fill_count,
            close_fill_count: account_total.close_fill_count,
            open_position_size: account_total.open_size,
            open_avg_entry_price: account_total.open_avg_entry_price(),
            open_unrealized_pnl_usd: account_total.open_unrealized_pnl_usd(current_price),
        });
    }

    missing_account_ids.sort();
    missing_account_ids.dedup();
    account_summaries.sort_by(|left, right| left.account_id.cmp(&right.account_id));

    FibStrategyPnlSummary {
        precise: !order_oids.is_empty(),
        attribution: if order_oids.is_empty() {
            "no_strategy_order_oids".to_string()
        } else {
            "strategy_order_oids".to_string()
        },
        completed_cycles: record.completed_cycles,
        total_closed_pnl_usd: total.closed_pnl_usd,
        total_fee_usd: total.fee_usd,
        total_net_pnl_usd: total.net_pnl_usd(),
        total_notional_usd: total.notional_usd,
        total_fill_count: total.fill_count,
        total_close_fill_count: total.close_fill_count,
        last_cycle_closed_pnl_usd: last_cycle.closed_pnl_usd,
        last_cycle_fee_usd: last_cycle.fee_usd,
        last_cycle_net_pnl_usd: last_cycle.net_pnl_usd(),
        last_cycle_notional_usd: last_cycle.notional_usd,
        last_cycle_fill_count: last_cycle.fill_count,
        last_cycle_close_fill_count: last_cycle.close_fill_count,
        open_position_size: total.open_size,
        open_avg_entry_price: total.open_avg_entry_price(),
        open_unrealized_pnl_usd: total.open_unrealized_pnl_usd(current_price),
        current_price,
        account_summaries,
        missing_account_ids,
        matched_order_count: order_oids.len(),
        updated_at_ms: now_ms(),
    }
}

fn fib_strategy_current_price(
    realtime: &RealtimeState,
    market: &MarketProfile,
    record: &FibInstanceRecord,
) -> Option<f64> {
    let mut candidates = vec![record.config.coin.clone(), record.plan.coin.clone()];
    if !market.is_spot() {
        candidates.push(normalize_dex_coin(&market.dex, &record.config.coin));
    } else {
        candidates.push(normalize_spot_coin(&record.config.coin));
    }
    candidates.retain(|coin| !coin.trim().is_empty());
    candidates.sort();
    candidates.dedup();
    realtime.mid_price(market.id, &candidates)
}

fn fib_strategy_order_oids(
    record: &FibInstanceRecord,
    history: &[FibInstanceHistoryEntry],
) -> HashSet<u64> {
    let mut oids = HashSet::new();
    collect_fib_order_oids_from_record(record, &mut oids);
    for entry in history
        .iter()
        .filter(|entry| entry.strategy_id == record.strategy_id)
    {
        if let Some(history_record) = entry.record.as_ref() {
            collect_fib_order_oids_from_record(history_record, &mut oids);
        }
    }
    oids
}

fn collect_fib_order_oids_from_record(record: &FibInstanceRecord, oids: &mut HashSet<u64>) {
    oids.extend(
        record
            .entry_order_refs
            .iter()
            .chain(record.protective_order_refs.iter())
            .filter_map(|order_ref| order_ref.oid),
    );
}

fn fib_strategy_last_cycle_window(
    record: &FibInstanceRecord,
    history: &[FibInstanceHistoryEntry],
) -> Option<(u64, u64)> {
    let end = record.last_cycle_completed_at_ms?;
    let previous_end = history
        .iter()
        .filter(|entry| {
            entry.strategy_id == record.strategy_id
                && entry.action == "cycle_completed"
                && entry.occurred_at_ms < end
        })
        .map(|entry| entry.occurred_at_ms)
        .max();
    let start = previous_end.unwrap_or(record.created_at_ms);
    Some((start.saturating_sub(5_000), end.saturating_add(5_000)))
}

fn fib_fill_matches_strategy(
    market: &MarketProfile,
    record: &FibInstanceRecord,
    fill: &UserFill,
    order_oids: &HashSet<u64>,
) -> bool {
    if order_oids.is_empty() || !order_oids.contains(&fill.oid) {
        return false;
    }
    dashboard_order_coin_matches_strategy(market, &fill.coin, &record.config.coin)
}

fn fill_fee_usd(fill: &UserFill) -> f64 {
    parse_decimal(&fill.fee).abs()
}

fn fill_is_buy(fill: &UserFill) -> bool {
    matches!(
        fill.side.trim().to_ascii_lowercase().as_str(),
        "b" | "buy" | "bid"
    )
}

fn fill_is_sell(fill: &UserFill) -> bool {
    matches!(
        fill.side.trim().to_ascii_lowercase().as_str(),
        "a" | "ask" | "sell"
    )
}

fn reconcile_fib_order_ref_oids_from_dashboard(
    state: &FrontendAppState,
    open_orders: &[DashboardOpenOrderResponse],
) -> Result<bool> {
    let oid_by_ref = open_orders
        .iter()
        .filter(|order| order.source_module == "fib")
        .filter_map(|order| {
            let strategy_id = order.strategy_id.as_ref()?;
            let oid = order.oid?;
            if order.cloid.trim().is_empty() {
                return None;
            }
            Some((
                (
                    strategy_id.clone(),
                    order.account_id.clone(),
                    order.cloid.to_ascii_lowercase(),
                ),
                oid,
            ))
        })
        .collect::<HashMap<_, _>>();
    if oid_by_ref.is_empty() {
        return Ok(false);
    }

    let mut guard = state
        .fib_instances
        .write()
        .map_err(|_| anyhow::anyhow!("fib instance lock is poisoned"))?;
    let mut changed_records = Vec::new();
    for record in guard.values_mut() {
        let mut changed = false;
        for order_ref in record
            .entry_order_refs
            .iter_mut()
            .chain(record.protective_order_refs.iter_mut())
        {
            if order_ref.oid.is_some() || order_ref.cloid.trim().is_empty() {
                continue;
            }
            let key = (
                record.strategy_id.clone(),
                order_ref.account_id.clone(),
                order_ref.cloid.to_ascii_lowercase(),
            );
            if let Some(oid) = oid_by_ref.get(&key) {
                order_ref.oid = Some(*oid);
                changed = true;
            }
        }
        if changed {
            record.updated_at_ms = now_ms();
            changed_records.push(record.clone());
        }
    }
    if changed_records.is_empty() {
        return Ok(false);
    }
    persist_fib_instances_best_effort(&guard);
    drop(guard);
    for record in changed_records {
        append_fib_instance_history_best_effort(&record, "order_oid_reconciled");
    }
    Ok(true)
}

async fn cancel_dashboard_open_orders(
    state: &FrontendAppState,
    payload: DashboardOpenOrdersCancelPayload,
) -> Result<DashboardOpenOrdersCancelResponse> {
    let base_config = state.config_snapshot()?;
    let execute_live = payload.live && !payload.dry_run;
    let vault_password = if execute_live {
        let default_market = resolve_market_profile(payload.market.as_deref(), &base_config)?;
        let config =
            scoped_config_for_module_and_market(base_config.clone(), "manual", &default_market);
        anyhow::ensure!(
            !state.dry_run && !config.app.dry_run,
            "dashboard live cancel requires frontend and config dry_run=false"
        );
        validate_live_action_gates(&config, payload.confirm_mainnet_live)?;
        let path = PathBuf::from(&config.secrets.vault_path);
        Some(state.resolve_vault_password(&path, "")?)
    } else {
        None
    };

    let snapshot = build_dashboard_open_orders_response(state).await?;
    let visible_orders = snapshot
        .open_orders
        .into_iter()
        .filter(|order| order.exchange_open)
        .collect::<Vec<_>>();
    let skipped_unowned_count = visible_orders
        .iter()
        .filter(|order| order.source_module != "fib")
        .count();
    let skipped_protective_count = visible_orders
        .iter()
        .filter(|order| {
            order.source_module == "fib" && !dashboard_order_is_cancelable_fib_entry(order)
        })
        .count();
    let mut targets = visible_orders
        .iter()
        .filter(|order| dashboard_order_is_cancelable_fib_entry(order))
        .cloned()
        .collect::<Vec<_>>();
    targets.sort_by(|left, right| {
        left.market
            .cmp(&right.market)
            .then(left.account_id.cmp(&right.account_id))
            .then(left.oid.cmp(&right.oid))
            .then(left.cloid.cmp(&right.cloid))
    });
    let target_count = targets.len();
    let mut strategy_ids = dashboard_cancel_strategy_ids(state, &visible_orders)?;
    let stopped_without_open_orders_count =
        dashboard_cancel_stopped_without_open_orders_count(&strategy_ids, &visible_orders);
    let mut seen = HashSet::<String>::new();
    let mut cancel_reports = Vec::new();

    for target in targets {
        let identity = target
            .oid
            .map(|oid| format!("{}:{}:oid:{oid}", target.market, target.account_id))
            .unwrap_or_else(|| {
                format!(
                    "{}:{}:cloid:{}",
                    target.market,
                    target.account_id,
                    target.cloid.to_ascii_lowercase()
                )
            });
        if !seen.insert(identity) {
            continue;
        }
        let report = if execute_live {
            if target.oid.is_none() && target.cloid.trim().is_empty() {
                DashboardCancelOpenOrderReport {
                    account_id: target.account_id.clone(),
                    coin: target.coin.clone(),
                    source_module: target.source_module.clone(),
                    strategy_id: target.strategy_id.clone(),
                    oid: target.oid,
                    cloid: target.cloid.clone(),
                    ok: false,
                    data: None,
                    error: Some("open order has neither oid nor cloid".to_string()),
                }
            } else {
                let module = normalize_optional_module_name(Some(&target.source_module))?;
                let target_market = resolve_market_profile(Some(&target.market), &base_config)?;
                let config = scoped_config_for_module_and_market(
                    base_config.clone(),
                    module,
                    &target_market,
                );
                let coin = normalize_coin_for_market(&target_market, &target.coin);
                match execute_cancel_open_order(
                    config,
                    target.account_id.clone(),
                    coin,
                    (!target.cloid.trim().is_empty()).then_some(target.cloid.clone()),
                    target.oid,
                    payload.confirm_mainnet_live,
                    vault_password
                        .as_deref()
                        .expect("vault password exists for live dashboard cancel"),
                )
                .await
                {
                    Ok(data) => DashboardCancelOpenOrderReport {
                        account_id: target.account_id.clone(),
                        coin: target.coin.clone(),
                        source_module: target.source_module.clone(),
                        strategy_id: target.strategy_id.clone(),
                        oid: target.oid,
                        cloid: target.cloid.clone(),
                        ok: !data.matching_open_after,
                        data: Some(data),
                        error: None,
                    },
                    Err(error) => DashboardCancelOpenOrderReport {
                        account_id: target.account_id.clone(),
                        coin: target.coin.clone(),
                        source_module: target.source_module.clone(),
                        strategy_id: target.strategy_id.clone(),
                        oid: target.oid,
                        cloid: target.cloid.clone(),
                        ok: false,
                        data: None,
                        error: Some(format_anyhow_error(&error)),
                    },
                }
            }
        } else {
            DashboardCancelOpenOrderReport {
                account_id: target.account_id.clone(),
                coin: target.coin.clone(),
                source_module: target.source_module.clone(),
                strategy_id: target.strategy_id.clone(),
                oid: target.oid,
                cloid: target.cloid.clone(),
                ok: true,
                data: None,
                error: None,
            }
        };
        if let Some(strategy_id) = target.strategy_id
            && target.source_module == "fib"
        {
            strategy_ids.insert(strategy_id);
        }
        cancel_reports.push(report);
    }

    let stopped_strategy_ids = if execute_live {
        stop_dashboard_fib_strategies(state, &strategy_ids, &cancel_reports)?
    } else {
        Vec::new()
    };
    let after_orders = build_dashboard_open_orders_response(state)
        .await?
        .open_orders;
    let open_orders_after = after_orders
        .iter()
        .filter(|order| order.exchange_open)
        .count();
    let open_owned_orders_after = after_orders
        .iter()
        .filter(|order| order.exchange_open && dashboard_order_is_cancelable_fib_entry(order))
        .count();

    Ok(DashboardOpenOrdersCancelResponse {
        environment: base_config.app.environment,
        market: "all".to_string(),
        market_label: "All Markets".to_string(),
        scope: "version_all_markets".to_string(),
        dry_run: !execute_live,
        target_count,
        skipped_unowned_count,
        skipped_protective_count,
        stopped_without_open_orders_count,
        cancel_reports,
        stopped_strategy_ids,
        open_orders_after,
        open_owned_orders_after,
        fetched_at_ms: now_ms(),
    })
}

fn active_dashboard_fib_strategy_ids(state: &FrontendAppState) -> Result<HashSet<String>> {
    let guard = state
        .fib_instances
        .read()
        .map_err(|_| anyhow::anyhow!("fib instance lock is poisoned"))?;
    Ok(guard
        .values()
        .filter(|record| fib_instance_reserves_pair(record))
        .map(|record| record.strategy_id.clone())
        .collect())
}

fn dashboard_cancel_strategy_ids(
    state: &FrontendAppState,
    visible_orders: &[DashboardOpenOrderResponse],
) -> Result<HashSet<String>> {
    let mut strategy_ids = active_dashboard_fib_strategy_ids(state)?;
    strategy_ids.extend(
        visible_orders
            .iter()
            .filter(|order| order.source_module == "fib")
            .filter_map(|order| order.strategy_id.clone()),
    );
    Ok(strategy_ids)
}

fn dashboard_cancel_stopped_without_open_orders_count(
    strategy_ids: &HashSet<String>,
    visible_orders: &[DashboardOpenOrderResponse],
) -> usize {
    strategy_ids
        .iter()
        .filter(|strategy_id| {
            !visible_orders.iter().any(|order| {
                order
                    .strategy_id
                    .as_deref()
                    .is_some_and(|visible_strategy_id| visible_strategy_id == *strategy_id)
            })
        })
        .count()
}

fn dashboard_order_is_cancelable_fib_entry(order: &DashboardOpenOrderResponse) -> bool {
    order.source_module == "fib"
        && order
            .strategy_id
            .as_deref()
            .is_some_and(|id| !id.trim().is_empty())
        && order.order_role == "entry"
}

fn stop_dashboard_fib_strategies(
    state: &FrontendAppState,
    strategy_ids: &HashSet<String>,
    cancel_reports: &[DashboardCancelOpenOrderReport],
) -> Result<Vec<String>> {
    if strategy_ids.is_empty() {
        return Ok(Vec::new());
    }
    let successful_cancel_cloids = cancel_reports
        .iter()
        .filter(|report| report.ok)
        .filter(|report| report.source_module == "fib")
        .map(|report| report.cloid.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let failed_cancel_count = cancel_reports
        .iter()
        .filter(|report| !report.ok && report.source_module == "fib")
        .count();
    let mut guard = state
        .fib_instances
        .write()
        .map_err(|_| anyhow::anyhow!("fib instance lock is poisoned"))?;
    let mut stopped = Vec::new();
    let mut history_records = Vec::new();
    for strategy_id in strategy_ids {
        let Some(record) = guard.get_mut(strategy_id) else {
            continue;
        };
        record.status = FibInstanceStatus::Killed;
        record.config.auto_loop = false;
        if !successful_cancel_cloids.is_empty() {
            record.entry_order_refs.retain(|order_ref| {
                !successful_cancel_cloids.contains(&order_ref.cloid.to_ascii_lowercase())
            });
        }
        record.last_message = Some(if failed_cancel_count > 0 {
            format!(
                "strategy stopped from dashboard; {failed_cancel_count} owned entry cancel request(s) failed and are preserved for retry; protective TP/SL orders were left active"
            )
        } else {
            "strategy stopped from dashboard; owned entry orders were cancelled; protective TP/SL orders were left active".to_string()
        });
        record.updated_at_ms = now_ms();
        history_records.push(record.clone());
        stopped.push(strategy_id.clone());
    }
    if !stopped.is_empty() {
        persist_fib_instances_best_effort(&guard);
    }
    drop(guard);
    for record in &history_records {
        append_fib_instance_history_best_effort(record, "dashboard_stop");
    }
    stopped.sort();
    Ok(stopped)
}

#[derive(Debug, Clone)]
struct FibRecoveredEntryRef {
    strategy_id: String,
    order_ref: FibOrderRef,
}

async fn recover_fib_entry_refs_from_exchange_open_orders(
    state: &FrontendAppState,
    config: &AppConfig,
    realtime: &RealtimeState,
    fib_instances: &[FibInstanceRecord],
    open_order_cache: &mut HashMap<String, Vec<OpenOrder>>,
) -> Result<bool> {
    let mut recovered = Vec::new();
    for record in fib_instances {
        if !fib_record_can_recover_open_entry(record) {
            continue;
        }
        let Ok(market) = resolve_market_profile(Some(&record.config.market), config) else {
            continue;
        };
        for account_id in &record.config.account_ids {
            let Some(account) = config.account(account_id) else {
                continue;
            };
            let orders = dashboard_open_orders_for_account(
                config,
                realtime,
                &market,
                account,
                open_order_cache,
            )
            .await
            .clone();
            for order in orders {
                if record
                    .entry_order_refs
                    .iter()
                    .chain(record.protective_order_refs.iter())
                    .any(|order_ref| dashboard_open_order_matches_ref(&order, order_ref))
                {
                    continue;
                }
                let Some(level) =
                    fib_recoverable_entry_level_from_open_order(record, &market, &order)
                else {
                    continue;
                };
                recovered.push(FibRecoveredEntryRef {
                    strategy_id: record.strategy_id.clone(),
                    order_ref: FibOrderRef {
                        account_id: account.account_id.clone(),
                        coin: record.config.coin.clone(),
                        cloid: order.cloid.clone().unwrap_or_default(),
                        oid: Some(order.oid),
                        level: Some(level),
                        role: Some("entry".to_string()),
                        dry_run: false,
                        submitted_at_ms: order.timestamp,
                    },
                });
            }
        }
    }

    if recovered.is_empty() {
        return Ok(false);
    }

    let mut guard = state
        .fib_instances
        .write()
        .map_err(|_| anyhow::anyhow!("fib instance lock is poisoned"))?;
    let mut changed_records = Vec::new();
    for recovered_ref in recovered {
        let Some(record) = guard.get_mut(&recovered_ref.strategy_id) else {
            continue;
        };
        if record
            .entry_order_refs
            .iter()
            .any(|order_ref| order_ref.oid == recovered_ref.order_ref.oid)
        {
            continue;
        }
        record.entry_order_refs.push(recovered_ref.order_ref);
        record.status = FibInstanceStatus::EntryPending;
        record.last_message =
            Some("recovered open Fib entry order from exchange; waiting for fill".to_string());
        record.updated_at_ms = now_ms();
        changed_records.push(record.clone());
    }
    if changed_records.is_empty() {
        return Ok(false);
    }
    persist_fib_instances_best_effort(&guard);
    drop(guard);
    for record in changed_records {
        append_fib_instance_history_best_effort(&record, "recovered_entry_order");
    }
    Ok(true)
}

fn fib_record_can_recover_open_entry(record: &FibInstanceRecord) -> bool {
    if !record.config.auto_loop {
        return false;
    }
    matches!(
        record.status,
        FibInstanceStatus::ArmedUnfilled
            | FibInstanceStatus::EntryPending
            | FibInstanceStatus::Completed
    )
}

fn fib_recoverable_entry_level_from_open_order(
    record: &FibInstanceRecord,
    market: &MarketProfile,
    order: &OpenOrder,
) -> Option<f64> {
    if order.is_trigger || open_order_native_protective_kind(order).is_some() {
        return None;
    }
    if !matches!(
        order.side.trim().to_ascii_uppercase().as_str(),
        "B" | "BID" | "BUY"
    ) {
        return None;
    }
    if !dashboard_order_coin_matches_strategy(market, &order.coin, &record.config.coin) {
        return None;
    }
    let limit_price = order.limit_px.parse::<f64>().ok()?;
    if limit_price <= 0.0 {
        return None;
    }
    let earliest_expected_ms = record
        .last_cycle_completed_at_ms
        .unwrap_or(record.created_at_ms)
        .saturating_sub(5_000);
    if order.timestamp < earliest_expected_ms {
        return None;
    }
    record
        .plan
        .levels
        .iter()
        .find(|level| {
            (limit_price - level.entry_price).abs()
                <= fib_entry_recovery_price_tolerance(level.entry_price)
        })
        .map(|level| level.level)
}

fn dashboard_order_coin_matches_strategy(
    market: &MarketProfile,
    order_coin: &str,
    strategy_coin: &str,
) -> bool {
    if market.is_spot() {
        normalize_spot_coin(order_coin) == normalize_spot_coin(strategy_coin)
    } else {
        normalize_dex_coin(&market.dex, order_coin)
            == normalize_dex_coin(&market.dex, strategy_coin)
    }
}

fn fib_entry_recovery_price_tolerance(entry_price: f64) -> f64 {
    (entry_price.abs() * 0.000_02).max(0.05)
}

async fn build_dashboard_fib_open_orders(
    config: &AppConfig,
    realtime: &RealtimeState,
    fib_instances: &[FibInstanceRecord],
    open_order_cache: &mut HashMap<String, Vec<OpenOrder>>,
) -> Vec<DashboardOpenOrderResponse> {
    let mut responses = Vec::new();
    let mut price_cache = HashMap::<String, Option<f64>>::new();

    for record in fib_instances {
        let Ok(market) = resolve_market_profile(Some(&record.config.market), config) else {
            continue;
        };
        let price_key = format!("{}::{}", market.id, record.config.coin.to_ascii_lowercase());
        let current_price = if let Some(price) = price_cache.get(&price_key) {
            *price
        } else {
            let price =
                fetch_fib_reference_price(config, &market, &record.config.coin, Some(realtime))
                    .await
                    .ok();
            price_cache.insert(price_key, price);
            price
        };

        for order_ref in &record.entry_order_refs {
            let open_order = dashboard_open_order_for_ref(
                config,
                realtime,
                &market,
                order_ref,
                open_order_cache,
            )
            .await;
            if open_order.is_none() && !order_ref.dry_run {
                continue;
            }
            responses.push(dashboard_fib_order_response(
                &market,
                record,
                order_ref,
                open_order.as_ref(),
                current_price,
                "entry",
            ));
        }

        for order_ref in &record.protective_order_refs {
            let open_order = dashboard_open_order_for_ref(
                config,
                realtime,
                &market,
                order_ref,
                open_order_cache,
            )
            .await;
            if open_order.is_none() && !order_ref.dry_run {
                continue;
            }
            responses.push(dashboard_fib_order_response(
                &market,
                record,
                order_ref,
                open_order.as_ref(),
                current_price,
                "protective",
            ));
        }
    }

    responses
}

async fn dashboard_open_order_for_ref(
    config: &AppConfig,
    realtime: &RealtimeState,
    market: &MarketProfile,
    order_ref: &FibOrderRef,
    cache: &mut HashMap<String, Vec<OpenOrder>>,
) -> Option<OpenOrder> {
    if order_ref.dry_run {
        return None;
    }
    let account = config.account(&order_ref.account_id)?;
    dashboard_open_orders_for_account(config, realtime, market, account, cache)
        .await
        .iter()
        .find(|order| dashboard_open_order_matches_ref(order, order_ref))
        .cloned()
}

async fn dashboard_open_orders_for_account<'a>(
    config: &AppConfig,
    realtime: &RealtimeState,
    market: &MarketProfile,
    account: &AccountConfig,
    cache: &'a mut HashMap<String, Vec<OpenOrder>>,
) -> &'a Vec<OpenOrder> {
    let cache_key = format!("{}::{}", market.id, account.address.to_ascii_lowercase());
    if !cache.contains_key(&cache_key) {
        let fetched = if let Some(orders) = realtime.open_orders(market.id, &account.address) {
            orders
        } else {
            fetch_open_orders(&config.app.environment, &market.dex, &account.address)
                .await
                .unwrap_or_default()
                .into_iter()
                .filter(|order| open_order_matches_market(order, market))
                .collect::<Vec<_>>()
        };
        cache.insert(cache_key.clone(), fetched);
    }
    cache.get(&cache_key).expect("open-order cache inserted")
}

async fn build_dashboard_manual_open_orders(
    config: &AppConfig,
    realtime: &RealtimeState,
    existing: &[DashboardOpenOrderResponse],
    open_order_cache: &mut HashMap<String, Vec<OpenOrder>>,
) -> Vec<DashboardOpenOrderResponse> {
    let mut responses = Vec::new();
    let mut price_cache = HashMap::<String, Option<f64>>::new();
    let mut seen = existing
        .iter()
        .filter_map(dashboard_response_identity)
        .collect::<HashSet<_>>();

    for market_id in supported_market_ids() {
        let Ok(market) = resolve_market_profile(Some(market_id), config) else {
            continue;
        };
        for account in config.accounts.iter().filter(|account| {
            account.enabled && account.worker_enabled && account.market_allowed(market.id)
        }) {
            let orders = dashboard_open_orders_for_account(
                config,
                realtime,
                &market,
                account,
                open_order_cache,
            )
            .await
            .clone();
            for order in orders {
                let identity = dashboard_open_order_identity(account.account_id.as_str(), &order);
                if !seen.insert(identity) {
                    continue;
                }
                let price_key = format!("{}::{}", market.id, order.coin.to_ascii_lowercase());
                let current_price = if let Some(price) = price_cache.get(&price_key) {
                    *price
                } else {
                    let price =
                        fetch_fib_reference_price(config, &market, &order.coin, Some(realtime))
                            .await
                            .ok();
                    price_cache.insert(price_key, price);
                    price
                };
                responses.push(dashboard_manual_order_response(
                    &market,
                    account,
                    &order,
                    current_price,
                ));
            }
        }
    }

    responses
}

fn dashboard_open_order_matches_ref(order: &OpenOrder, order_ref: &FibOrderRef) -> bool {
    if order_ref.oid == Some(order.oid) {
        return true;
    }
    let Some(order_cloid) = order.cloid.as_deref() else {
        return false;
    };
    if order_cloid.eq_ignore_ascii_case(&order_ref.cloid) {
        return true;
    }
    normalize_cloid_for_info(&order_ref.cloid)
        .is_ok_and(|normalized| order_cloid.eq_ignore_ascii_case(&normalized))
}

fn dashboard_open_order_identity(account_id: &str, order: &OpenOrder) -> String {
    if order.oid > 0 {
        return format!("{account_id}:oid:{}", order.oid);
    }
    order
        .cloid
        .as_deref()
        .map(|cloid| format!("{account_id}:cloid:{}", cloid.to_ascii_lowercase()))
        .unwrap_or_else(|| {
            format!(
                "{}:fallback:{}:{}:{}:{}",
                account_id, order.coin, order.side, order.limit_px, order.timestamp
            )
        })
}

fn dashboard_response_identity(response: &DashboardOpenOrderResponse) -> Option<String> {
    if let Some(oid) = response.oid {
        return Some(format!("{}:oid:{oid}", response.account_id));
    }
    if !response.cloid.trim().is_empty() {
        return Some(format!(
            "{}:cloid:{}",
            response.account_id,
            response.cloid.to_ascii_lowercase()
        ));
    }
    None
}

fn fib_instance_status_key(status: FibInstanceStatus) -> &'static str {
    match status {
        FibInstanceStatus::Draft => "draft",
        FibInstanceStatus::ArmedUnfilled => "armed_unfilled",
        FibInstanceStatus::EntryPending => "entry_pending",
        FibInstanceStatus::EntryFilled => "entry_filled",
        FibInstanceStatus::ProtectionPending => "protection_pending",
        FibInstanceStatus::Protected => "protected",
        FibInstanceStatus::Exiting => "exiting",
        FibInstanceStatus::Completed => "completed",
        FibInstanceStatus::Paused => "paused",
        FibInstanceStatus::Killed => "killed",
        FibInstanceStatus::Error => "error",
    }
}

fn dashboard_manual_order_response(
    market: &MarketProfile,
    account: &AccountConfig,
    order: &OpenOrder,
    current_price: Option<f64>,
) -> DashboardOpenOrderResponse {
    let side = order.side.clone();
    let role = open_order_native_protective_kind(order)
        .unwrap_or("manual_order")
        .to_string();
    let order_type = if order.order_type.trim().is_empty() {
        if order.is_trigger {
            "Trigger".to_string()
        } else {
            "Limit".to_string()
        }
    } else {
        order.order_type.clone()
    };
    let limit_price = order.limit_px.parse::<f64>().ok();
    let trigger_price = order
        .trigger_px
        .parse::<f64>()
        .ok()
        .filter(|price| *price > 0.0);
    let size = order
        .orig_sz
        .parse::<f64>()
        .ok()
        .filter(|value| *value > 0.0)
        .or_else(|| order.sz.parse::<f64>().ok());
    let reference_price = trigger_price.or(limit_price);
    let distance_usd = current_price
        .zip(reference_price)
        .map(|(current, reference)| {
            if matches!(
                side.trim().to_ascii_uppercase().as_str(),
                "A" | "ASK" | "SELL"
            ) {
                reference - current
            } else {
                current - reference
            }
        });
    let distance_pct = distance_usd
        .zip(current_price)
        .and_then(|(distance, current)| {
            if current.abs() > f64::EPSILON {
                Some(distance / current.abs() * 100.0)
            } else {
                None
            }
        });
    DashboardOpenOrderResponse {
        market: market.id.to_string(),
        market_label: market.label.to_string(),
        account_id: account.account_id.clone(),
        coin: order.coin.clone(),
        source_module: "manual".to_string(),
        strategy_id: None,
        strategy_status: None,
        timeframe: None,
        fib_level: None,
        fib_line_version: None,
        swing_high: None,
        swing_low: None,
        entry_zone_high: None,
        entry_zone_low: None,
        planned_take_profit_price: None,
        planned_stop_loss_price: None,
        order_role: role,
        side,
        order_type,
        limit_price,
        trigger_price,
        size,
        current_price,
        distance_usd,
        distance_pct,
        cloid: order.cloid.clone().unwrap_or_default(),
        oid: Some(order.oid),
        exchange_open: true,
        status: if order.is_trigger {
            "waiting_trigger".to_string()
        } else {
            "waiting_fill".to_string()
        },
        submitted_at_ms: order.timestamp,
        updated_at_ms: now_ms(),
    }
}

fn dashboard_fib_order_response(
    market: &MarketProfile,
    record: &FibInstanceRecord,
    order_ref: &FibOrderRef,
    open_order: Option<&OpenOrder>,
    current_price: Option<f64>,
    fallback_role: &str,
) -> DashboardOpenOrderResponse {
    let level_plan = order_ref.level.and_then(|level| {
        record
            .plan
            .levels
            .iter()
            .find(|plan| (plan.level - level).abs() < 0.000_5)
    });
    let side = open_order
        .map(|order| order.side.clone())
        .unwrap_or_else(|| "B".to_string());
    let order_type = open_order
        .map(|order| order.order_type.clone())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| {
            if fallback_role == "entry" {
                match record.config.execution_mode {
                    ExecutionMode::Maker => "Limit Post-Only".to_string(),
                    ExecutionMode::Taker => "Market/IOC".to_string(),
                }
            } else {
                "Trigger".to_string()
            }
        });
    let role = open_order
        .and_then(open_order_native_protective_kind)
        .unwrap_or(fallback_role)
        .to_string();
    let limit_price = open_order
        .and_then(|order| order.limit_px.parse::<f64>().ok())
        .or_else(|| level_plan.map(|plan| plan.entry_price));
    let trigger_price = open_order
        .and_then(|order| order.trigger_px.parse::<f64>().ok())
        .filter(|price| *price > 0.0);
    let size = open_order.and_then(|order| {
        order
            .orig_sz
            .parse::<f64>()
            .ok()
            .filter(|value| *value > 0.0)
            .or_else(|| order.sz.parse::<f64>().ok())
    });
    let reference_price = trigger_price.or(limit_price);
    let distance_usd = current_price
        .zip(reference_price)
        .map(|(current, reference)| {
            if matches!(
                side.trim().to_ascii_uppercase().as_str(),
                "A" | "ASK" | "SELL"
            ) {
                reference - current
            } else {
                current - reference
            }
        });
    let distance_pct = distance_usd
        .zip(current_price)
        .and_then(|(distance, current)| {
            if current.abs() > f64::EPSILON {
                Some(distance / current.abs() * 100.0)
            } else {
                None
            }
        });
    let status = if order_ref.dry_run {
        "dry_run"
    } else if open_order.is_some() {
        if role == "entry" {
            "waiting_fill"
        } else {
            "waiting_trigger"
        }
    } else {
        "not_open"
    };

    DashboardOpenOrderResponse {
        market: market.id.to_string(),
        market_label: market.label.to_string(),
        account_id: order_ref.account_id.clone(),
        coin: order_ref.coin.clone(),
        source_module: "fib".to_string(),
        strategy_id: Some(record.strategy_id.clone()),
        strategy_status: Some(fib_instance_status_key(record.status).to_string()),
        timeframe: Some(record.config.timeframe.clone()),
        fib_level: order_ref.level,
        fib_line_version: Some(record.plan.line_version.clone()),
        swing_high: Some(record.plan.swing_high),
        swing_low: Some(record.plan.swing_low),
        entry_zone_high: level_plan.map(|plan| plan.entry_zone_high),
        entry_zone_low: level_plan.map(|plan| plan.entry_zone_low),
        planned_take_profit_price: level_plan.map(|plan| plan.take_profit_price),
        planned_stop_loss_price: level_plan.map(|plan| plan.stop_loss_price),
        order_role: role,
        side,
        order_type,
        limit_price,
        trigger_price,
        size,
        current_price,
        distance_usd,
        distance_pct,
        cloid: order_ref.cloid.clone(),
        oid: order_ref.oid.or_else(|| open_order.map(|order| order.oid)),
        exchange_open: open_order.is_some(),
        status: status.to_string(),
        submitted_at_ms: order_ref.submitted_at_ms,
        updated_at_ms: record.updated_at_ms,
    }
}

fn render_trade_recent_event(event: &AuditEvent) -> Option<EventResponse> {
    let market = resolve_event_market_id(event)?;
    let event_type = match event.action.as_str() {
        "signed_runbook_submit" if event.ok => trade_record_event_type(event),
        "manual_protective_exit_arm"
            if event.ok && detail_bool(&event.details, "submit").unwrap_or(false) =>
        {
            "止盈止损设定".to_string()
        }
        "manual_protective_rule_triggered"
            if event.ok && detail_bool(&event.details, "submitted").unwrap_or(false) =>
        {
            protective_trigger_event_type(event)
        }
        "manual_protective_exit_submit" if event.ok => protective_trigger_event_type(event),
        _ => return None,
    };
    let source = event_source_module_text(event);
    let message = match event.action.as_str() {
        "signed_runbook_submit" => format_trade_record_event_message(event),
        "manual_protective_exit_arm" => format_protective_arm_event_message(event),
        "manual_protective_rule_triggered" => format_protective_rule_triggered_event_message(event),
        "manual_protective_exit_submit" => format_protective_submit_event_message(event),
        _ => return None,
    };
    Some(EventResponse {
        event_id: event.event_id.clone(),
        market,
        source,
        event_type,
        account_id: event
            .account_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(ToString::to_string),
        coin: event_coin_text(event).map(|coin| pretty_coin_label(Some(coin))),
        side: event_side_text(event),
        amount_usd: event_amount_usd(event),
        pnl_usd: event_pnl_usd(event),
        message,
        occurred_at_ms: event.occurred_at_ms,
    })
}

fn extend_realtime_fill_events(
    events: &mut Vec<EventResponse>,
    config: &AppConfig,
    state: &FrontendAppState,
) {
    for account in config.enabled_worker_accounts() {
        for market_id in [MARKET_XYZ_PERP, MARKET_HL_PERP, MARKET_SPOT] {
            let Some(fills) = state.realtime.fills(market_id, &account.address) else {
                continue;
            };
            events.extend(
                fills
                    .iter()
                    .rev()
                    .take(RECENT_TRADE_EVENTS_PER_MARKET)
                    .map(|fill| render_realtime_fill_event(market_id, account, fill)),
            );
        }
    }
}

fn render_realtime_fill_event(
    market_id: &str,
    account: &AccountConfig,
    fill: &UserFill,
) -> EventResponse {
    let amount_usd = fill_amount_usd(fill);
    let pnl_usd = fill_pnl_usd(fill);
    let coin = pretty_coin_label(Some(&fill.coin));
    let side = fill_side_text(fill);
    let action_text = fill_action_text(market_id, fill, pnl_usd);
    let liquidity = if fill.crossed { "吃单" } else { "挂单" };
    let amount_text = amount_usd
        .map(format_usd_value)
        .unwrap_or_else(|| "-".to_string());
    let pnl_text = pnl_usd
        .map(|pnl| {
            if pnl.abs() < DISPLAY_USD_EPSILON {
                "，已实现盈亏 0 USD".to_string()
            } else if pnl > 0.0 {
                format!("，已实现盈亏 +{} USD", format_usd_value(pnl.abs()))
            } else {
                format!("，已实现盈亏 -{} USD", format_usd_value(pnl.abs()))
            }
        })
        .unwrap_or_default();
    EventResponse {
        event_id: format!(
            "fill:{}:{}:{}:{}:{}",
            market_id, account.account_id, fill.hash, fill.oid, fill.time
        ),
        market: market_id.to_string(),
        source: "成交".to_string(),
        event_type: fill_event_type(market_id, fill),
        account_id: Some(account.account_id.clone()),
        coin: Some(coin.clone()),
        side: Some(side.clone()),
        amount_usd,
        pnl_usd,
        message: format!(
            "{}：{} {} 成交，实际金额 {} USD（{}）{}",
            account.account_id, action_text, coin, amount_text, liquidity, pnl_text
        ),
        occurred_at_ms: fill.time,
    }
}

fn cap_recent_events_by_market(events: Vec<EventResponse>) -> Vec<EventResponse> {
    let mut by_market: HashMap<String, Vec<EventResponse>> = HashMap::new();
    let mut seen = HashSet::new();
    for event in events {
        if !seen.insert(event.event_id.clone()) {
            continue;
        }
        let market = normalize_market_id(&event.market)
            .unwrap_or(MARKET_XYZ_PERP)
            .to_string();
        by_market.entry(market).or_default().push(event);
    }
    let mut capped = Vec::new();
    for market_id in [MARKET_XYZ_PERP, MARKET_HL_PERP, MARKET_SPOT] {
        if let Some(mut market_events) = by_market.remove(market_id) {
            market_events.sort_by_key(|event| std::cmp::Reverse(event.occurred_at_ms));
            capped.extend(
                market_events
                    .into_iter()
                    .take(RECENT_TRADE_EVENTS_PER_MARKET),
            );
        }
    }
    capped
}

fn fill_amount_usd(fill: &UserFill) -> Option<f64> {
    let price = fill.px.trim().parse::<f64>().ok()?;
    let size = fill.sz.trim().parse::<f64>().ok()?;
    let amount = (price * size).abs();
    amount.is_finite().then_some(amount)
}

fn fill_pnl_usd(fill: &UserFill) -> Option<f64> {
    let pnl = fill.closed_pnl.trim().parse::<f64>().ok()?;
    if fill.dir.to_ascii_lowercase().contains("close") || pnl.abs() > f64::EPSILON {
        Some(pnl)
    } else {
        None
    }
}

fn fill_side_text(fill: &UserFill) -> String {
    human_side_text(Some(&fill.side), false).to_string()
}

fn fill_event_type(market_id: &str, fill: &UserFill) -> String {
    let pnl = fill_pnl_usd(fill);
    let action = fill_action_text(market_id, fill, pnl);
    if matches!(action.as_str(), "平多" | "平空" | "平仓") {
        return match pnl {
            Some(value) if value > DISPLAY_USD_EPSILON => format!("盈利{action}成交"),
            Some(value) if value < -DISPLAY_USD_EPSILON => format!("亏损{action}成交"),
            _ => format!("{action}成交"),
        };
    }
    if action == "成交" {
        action
    } else {
        format!("{action}成交")
    }
}

fn fill_action_text(market_id: &str, fill: &UserFill, pnl: Option<f64>) -> String {
    if normalize_market_id(market_id) == Some(MARKET_SPOT) {
        return match fill.side.trim().to_ascii_lowercase().as_str() {
            "b" | "buy" | "bid" => "买入".to_string(),
            "a" | "ask" | "sell" => "卖出".to_string(),
            _ => "成交".to_string(),
        };
    }

    let dir = fill.dir.trim().to_ascii_lowercase();
    if dir.contains("open long") {
        return "开多".to_string();
    }
    if dir.contains("open short") {
        return "开空".to_string();
    }
    if dir.contains("close long") {
        return "平多".to_string();
    }
    if dir.contains("close short") {
        return "平空".to_string();
    }
    if dir.contains("close") || pnl.is_some() {
        if fill_is_buy(fill) {
            return "平空".to_string();
        }
        if fill_is_sell(fill) {
            return "平多".to_string();
        }
        return "平仓".to_string();
    }
    if fill_is_buy(fill) {
        "开多".to_string()
    } else if fill_is_sell(fill) {
        "开空".to_string()
    } else {
        "成交".to_string()
    }
}

fn event_source_module_text(event: &AuditEvent) -> String {
    match detail_str(&event.details, "source_module")
        .or_else(|| detail_str(&event.details, "module"))
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("fib") => "斐波那契".to_string(),
        Some("manual") => "手动".to_string(),
        Some("copy") => "跟单".to_string(),
        _ if event.action.starts_with("manual_") => "手动".to_string(),
        _ if event.action.starts_with("fib_") => "斐波那契".to_string(),
        _ => "交易".to_string(),
    }
}

fn trade_record_event_type(event: &AuditEvent) -> String {
    let source = detail_str(&event.details, "source_module")
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_else(|| "manual".to_string());
    let side = detail_str(&event.details, "side")
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();
    let reduce_only = detail_bool(&event.details, "reduce_only").unwrap_or(false)
        || detail_bool(&event.details, "close_full_position").unwrap_or(false);
    let execution = detail_str(&event.details, "execution_mode")
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();
    let execution_prefix = if matches!(
        execution.as_str(),
        "maker" | "alo" | "post_only_alo" | "limit_post_only_alo"
    ) {
        "挂单"
    } else {
        ""
    };
    let market = resolve_event_market_id(event);
    let is_spot = market.as_deref() == Some(MARKET_SPOT);
    let module_prefix = match source.as_str() {
        "fib" => "Fib",
        "copy" => "跟单",
        _ => "手动",
    };
    let action = if is_spot && (side == "buy" || side == "long") {
        "买入"
    } else if is_spot && (side == "sell" || side == "short") {
        "卖出"
    } else if reduce_only && (side == "buy" || side == "long") {
        "平空"
    } else if reduce_only && (side == "sell" || side == "short") {
        "平多"
    } else if reduce_only {
        "平仓"
    } else if side == "buy" || side == "long" {
        "开多"
    } else if side == "sell" || side == "short" {
        "开空"
    } else {
        "下单"
    };
    if execution_prefix.is_empty() {
        format!("{module_prefix}{action}")
    } else {
        format!("{module_prefix}{execution_prefix}{action}")
    }
}

fn protective_trigger_event_type(event: &AuditEvent) -> String {
    match detail_str(&event.details, "trigger_kind")
        .or_else(|| detail_str(&event.details, "kind"))
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("take_profit") | Some("tp") => "止盈触发".to_string(),
        Some("stop_loss") | Some("sl") => "止损触发".to_string(),
        _ => "止盈止损触发".to_string(),
    }
}

fn event_side_text(event: &AuditEvent) -> Option<String> {
    let side =
        detail_str(&event.details, "side").or_else(|| detail_str(&event.details, "entry_side"))?;
    Some(
        human_side_text(
            Some(side),
            detail_bool(&event.details, "reduce_only").unwrap_or(false),
        )
        .to_string(),
    )
}

fn event_amount_usd(event: &AuditEvent) -> Option<f64> {
    detail_number_any(
        &event.details,
        &[
            "actual_notional_usd",
            "filled_notional_usd",
            "amount_usd",
            "notional_usd",
            "order_notional_usd",
        ],
    )
    .or_else(|| {
        let price = detail_number_any(&event.details, &["avg_fill_price", "fill_price", "price"]);
        let size = detail_number_any(&event.details, &["filled_size", "size", "sz"]);
        match (price, size) {
            (Some(price), Some(size)) if price.is_finite() && size.is_finite() => {
                Some((price * size).abs())
            }
            _ => None,
        }
    })
}

fn event_pnl_usd(event: &AuditEvent) -> Option<f64> {
    detail_number_any(
        &event.details,
        &[
            "pnl_usd",
            "realized_pnl_usd",
            "closed_pnl",
            "closedPnl",
            "profit_usd",
        ],
    )
}

fn resolve_event_market_id(event: &AuditEvent) -> Option<String> {
    if let Some(raw_market) = detail_str(&event.details, "market")
        && let Some(normalized) = normalize_market_id(raw_market)
    {
        return Some(normalized.to_string());
    }
    let coin = event
        .coin
        .as_deref()
        .or_else(|| detail_str(&event.details, "coin"))?;
    let normalized_coin = coin.trim();
    if normalized_coin.is_empty() {
        return None;
    }
    if normalized_coin.contains('/') {
        return Some(MARKET_SPOT.to_string());
    }
    if normalized_coin.contains(':') {
        return Some(MARKET_XYZ_PERP.to_string());
    }
    Some(MARKET_HL_PERP.to_string())
}

fn format_trade_record_event_message(event: &AuditEvent) -> String {
    let account = event
        .account_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("未指定账户");
    let side = human_side_text(
        detail_str(&event.details, "side"),
        detail_bool(&event.details, "reduce_only").unwrap_or(false),
    );
    let coin = pretty_coin_label(
        event
            .coin
            .as_deref()
            .or_else(|| detail_str(&event.details, "coin")),
    );
    let notional = detail_f64(&event.details, "notional_usd")
        .map(format_usd_value)
        .unwrap_or_else(|| "-".to_string());
    let execution = human_execution_mode(detail_str(&event.details, "execution_mode"));
    format!(
        "{}：{} {}，名义金额 {} USD（{}）",
        account, side, coin, notional, execution
    )
}

fn format_protective_arm_event_message(event: &AuditEvent) -> String {
    let account = event
        .account_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("未指定账户");
    let coin = pretty_coin_label(
        event
            .coin
            .as_deref()
            .or_else(|| detail_str(&event.details, "coin")),
    );
    let submitted = detail_bool(&event.details, "submit").unwrap_or(false);
    let market = detail_str(&event.details, "market")
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();
    let action = if submitted && market == "spot" {
        "止盈止损监控"
    } else if submitted {
        "交易所原生止盈止损"
    } else {
        "止盈止损计划"
    };
    if event.ok {
        let status = if submitted && market == "spot" {
            "已启用"
        } else if submitted {
            "已提交"
        } else {
            "已保存"
        };
        format!("{}{}：{} / {}", action, status, account, coin)
    } else {
        format!(
            "{}处理失败：{}（{} / {}）",
            action,
            event_error_text(event),
            account,
            coin
        )
    }
}

fn format_protective_submit_event_message(event: &AuditEvent) -> String {
    let account = event
        .account_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("未指定账户");
    let coin = pretty_coin_label(
        event
            .coin
            .as_deref()
            .or_else(|| detail_str(&event.details, "coin")),
    );
    if event.ok {
        format!("止盈止损触发后提交完成：{} / {}", account, coin)
    } else {
        format!(
            "止盈止损触发提交失败：{}（{} / {}）",
            event_error_text(event),
            account,
            coin
        )
    }
}

fn format_protective_rule_triggered_event_message(event: &AuditEvent) -> String {
    let account = event
        .account_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("未指定账户");
    let coin = pretty_coin_label(
        event
            .coin
            .as_deref()
            .or_else(|| detail_str(&event.details, "coin")),
    );
    let trigger_kind = detail_str(&event.details, "trigger_kind").unwrap_or("未知触发");
    let submitted = detail_bool(&event.details, "submitted").unwrap_or(false);
    if event.ok && submitted {
        format!(
            "止盈止损已触发并提交平仓：{} / {}（{}）",
            account, coin, trigger_kind
        )
    } else if !event.ok && !submitted {
        let error = detail_str(&event.details, "error")
            .filter(|value| !value.trim().is_empty())
            .map(humanize_recent_event_error_text)
            .or_else(|| {
                event
                    .error
                    .as_deref()
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                    .map(humanize_recent_event_error_text)
            })
            .unwrap_or_else(|| "未知错误".to_string());
        format!(
            "止盈止损触发后暂未提交：{}（{} / {}，{}）",
            error, account, coin, trigger_kind
        )
    } else {
        format!(
            "止盈止损触发检查：{} / {}（{}）",
            account, coin, trigger_kind
        )
    }
}

fn detail_str<'a>(details: &'a Value, key: &str) -> Option<&'a str> {
    details.get(key)?.as_str()
}

fn detail_bool(details: &Value, key: &str) -> Option<bool> {
    details.get(key)?.as_bool()
}

fn detail_f64(details: &Value, key: &str) -> Option<f64> {
    details.get(key)?.as_f64()
}

fn detail_number_any(details: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|key| detail_number_recursive(details, key))
}

fn detail_number_recursive(value: &Value, key: &str) -> Option<f64> {
    match value {
        Value::Object(map) => {
            if let Some(number) = map.get(key).and_then(value_as_f64) {
                return Some(number);
            }
            map.values()
                .find_map(|child| detail_number_recursive(child, key))
        }
        Value::Array(items) => items
            .iter()
            .find_map(|child| detail_number_recursive(child, key)),
        _ => None,
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.trim().parse::<f64>().ok())
        .filter(|number| number.is_finite())
}

fn event_account_text(event: &AuditEvent) -> &str {
    event
        .account_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("未指定账户")
}

fn event_coin_text(event: &AuditEvent) -> Option<&str> {
    event
        .coin
        .as_deref()
        .or_else(|| detail_str(&event.details, "coin"))
}

fn humanize_recent_event_error_text(raw: &str) -> String {
    let text = raw.trim();
    if text.is_empty() {
        return "未知错误".to_string();
    }
    let lower = text.to_ascii_lowercase();
    if lower.contains("vault is locked") || lower.contains("unlock before this action") {
        return "Vault 未解锁".to_string();
    }
    if lower.contains("no available base inventory") {
        return "现货可卖持仓不足".to_string();
    }
    if lower.contains("too many requests") || lower.contains("429") {
        return "请求过于频繁".to_string();
    }
    if lower.contains("connection error")
        || lower.contains("error sending request")
        || lower.contains("timed out")
        || lower.contains("unexpected eof")
    {
        return "网络连接异常".to_string();
    }
    if lower.contains("failed to initialize hyperliquid exchange client") {
        return "交易客户端初始化失败".to_string();
    }
    if lower.contains("unknown error") {
        return "未知错误".to_string();
    }
    text.to_string()
}

fn snapshot_cache_key(
    environment: &str,
    scope: &str,
    account_ids: &[String],
    suffix: &str,
) -> String {
    let mut ids = account_ids
        .iter()
        .map(|id| id.trim().to_ascii_lowercase())
        .filter(|id| !id.is_empty())
        .collect::<Vec<_>>();
    ids.sort();
    ids.dedup();
    format!(
        "{}|{}|{}|{}",
        environment.trim().to_ascii_lowercase(),
        scope.trim().to_ascii_lowercase(),
        ids.join(","),
        suffix.trim().to_ascii_lowercase()
    )
}

fn event_error_text(event: &AuditEvent) -> String {
    let raw = event
        .error
        .as_deref()
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .unwrap_or("未知错误");
    humanize_recent_event_error_text(raw)
}

fn format_usd_value(value: f64) -> String {
    format!("{value:.2}")
}

fn human_execution_mode(mode: Option<&str>) -> &'static str {
    match mode
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("maker") | Some("alo") | Some("post_only_alo") | Some("limit_post_only_alo") => "挂单",
        Some("taker") | Some("ioc") | Some("market") | Some("market_ioc") => "吃单",
        _ => "默认",
    }
}

fn human_side_text(side: Option<&str>, reduce_only: bool) -> &'static str {
    let normalized = side.map(|value| value.trim().to_ascii_lowercase());
    match normalized.as_deref() {
        Some("b") | Some("bid") | Some("buy") | Some("long") => {
            if reduce_only {
                "减仓买入"
            } else {
                "买入"
            }
        }
        Some("a") | Some("ask") | Some("sell") | Some("short") => {
            if reduce_only {
                "减仓卖出"
            } else {
                "卖出"
            }
        }
        _ => {
            if reduce_only {
                "减仓"
            } else {
                "下单"
            }
        }
    }
}

fn pretty_coin_label(raw_coin: Option<&str>) -> String {
    let coin = raw_coin.unwrap_or("-").trim();
    if coin.is_empty() {
        return "-".to_string();
    }
    if let Some((prefix, symbol)) = coin.split_once(':')
        && !symbol.trim().is_empty()
    {
        return format!(
            "{}-USD ({})",
            symbol.trim().to_ascii_uppercase(),
            prefix.trim().to_ascii_uppercase()
        );
    }
    if let Some((base, quote)) = coin.split_once('/') {
        return format!(
            "{}-{}",
            base.trim().to_ascii_uppercase(),
            quote.trim().to_ascii_uppercase()
        );
    }
    coin.to_ascii_uppercase()
}

fn frontend_market_id_for_coin(coin: &str) -> &'static str {
    let coin = coin.trim();
    if coin.contains(':') {
        MARKET_XYZ_PERP
    } else if coin.contains('/') || coin.contains('-') || coin.starts_with('@') {
        MARKET_SPOT
    } else {
        MARKET_HL_PERP
    }
}

fn frontend_perp_market_id_for_dex(dex: &str) -> &'static str {
    let dex = dex.trim();
    if dex.is_empty() || dex.eq_ignore_ascii_case("default") || dex.eq_ignore_ascii_case("spot") {
        MARKET_HL_PERP
    } else {
        MARKET_XYZ_PERP
    }
}

fn recent_event_dedupe_key(event: &AuditEvent) -> Option<String> {
    let market = resolve_event_market_id(event)?;
    let account = event_account_text(event);
    let coin = event_coin_text(event).unwrap_or("-");
    match event.action.as_str() {
        "manual_protective_rule_triggered" => {
            let trigger_kind = detail_str(&event.details, "trigger_kind").unwrap_or("-");
            Some(format!(
                "{market}:{}:{account}:{coin}:{trigger_kind}",
                event.action
            ))
        }
        "manual_protective_exit_arm" => {
            let submitted = detail_bool(&event.details, "submit").unwrap_or(false);
            Some(format!(
                "{market}:{}:{account}:{coin}:submit={submitted}:ok={}",
                event.action, event.ok
            ))
        }
        _ => None,
    }
}

#[derive(Debug, Deserialize)]
struct ManualOrderPayload {
    operator: String,
    target_accounts: Vec<String>,
    coin: String,
    side: String,
    notional_usd: f64,
    reduce_only: bool,
    execution_mode: String,
    max_slippage_bps: f64,
    client_note: Option<String>,
    #[serde(default)]
    source_module: Option<String>,
    #[serde(default)]
    market: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ManualSettingsPayload {
    #[serde(default)]
    max_manual_order_notional_usd: Option<f64>,
    #[serde(default)]
    account_max_order_notional_usd: Option<f64>,
    #[serde(default)]
    account_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ManualSettingsResponse {
    max_manual_order_notional_usd: f64,
    updated_account_limits: Vec<ManualAccountLimitResponse>,
}

#[derive(Debug, Serialize)]
struct ManualAccountLimitResponse {
    account_id: String,
    max_order_notional_usd: f64,
}

#[derive(Debug, Deserialize)]
struct ManualLeveragePayload {
    account_id: String,
    coin: String,
    leverage: f64,
    #[serde(default = "default_isolated")]
    margin_mode: String,
    #[serde(default)]
    submit: bool,
    #[serde(default)]
    confirm_mainnet_live: bool,
    #[serde(default)]
    market: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ProtectiveExitPayload {
    #[serde(flatten)]
    exit: ProtectiveExitOptions,
    #[serde(default)]
    market: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ProtectiveExitTriggerPayload {
    #[serde(flatten)]
    trigger: ProtectiveExitTriggerCheckOptions,
    #[serde(default)]
    market: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ProtectiveExitSubmitPayload {
    #[serde(flatten)]
    submit: ProtectiveExitSubmitOptions,
    #[serde(default)]
    market: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ProtectiveExitArmPayload {
    #[serde(flatten)]
    arm: ProtectiveExitArmOptions,
    #[serde(default)]
    market: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ManualMarketQuotePayload {
    #[serde(default)]
    market: Option<String>,
    coin: String,
}

#[derive(Debug, Deserialize)]
struct ManualQuoteWsRequest {
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    market: Option<String>,
    #[serde(default)]
    coin: Option<String>,
    #[serde(default)]
    interval_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
struct ManualMarketUniversePayload {
    #[serde(default)]
    market: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ManualProtectiveRulesPayload {
    #[serde(default)]
    market: Option<String>,
    #[serde(default)]
    account_ids: Vec<String>,
    #[serde(default)]
    include_disabled: bool,
}

#[derive(Debug, Serialize)]
struct ManualMarketUniverseResponse {
    environment: String,
    market: String,
    market_label: String,
    dex: String,
    default_coin: String,
    coins: Vec<String>,
    assets: Vec<ManualMarketUniverseAssetResponse>,
    fetched_at_ms: u64,
}

#[derive(Debug, Serialize)]
struct ManualMarketUniverseAssetResponse {
    coin: String,
    sz_decimals: Option<u32>,
    size_step: Option<f64>,
}

#[derive(Debug, Serialize)]
struct ManualMarketQuoteResponse {
    environment: String,
    market: String,
    market_label: String,
    dex: String,
    coin: String,
    reference_price: Option<f64>,
    mark_price: Option<f64>,
    mid_price: Option<f64>,
    oracle_price: Option<f64>,
    funding_rate: Option<f64>,
    open_interest: Option<f64>,
    day_notional_volume: Option<f64>,
    max_leverage: Option<u32>,
    only_isolated: Option<bool>,
    margin_mode: Option<String>,
    fetched_at_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
struct ManualProtectiveRulesResponse {
    environment: String,
    market: String,
    market_label: String,
    dex: String,
    include_disabled: bool,
    account_ids: Vec<String>,
    rules: Vec<ManualProtectiveRuleView>,
    fetched_at_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
struct ManualProtectiveRuleView {
    rule_id: String,
    account_id: String,
    coin: String,
    entry_side: String,
    entry_price: Option<f64>,
    notional_usd: f64,
    take_profit_usd: f64,
    stop_loss_pct: f64,
    take_profit_trigger_price: Option<f64>,
    stop_loss_trigger_price: Option<f64>,
    enabled: bool,
    created_at_ms: u64,
    updated_at_ms: u64,
    trigger_count: u32,
    last_checked_at_ms: Option<u64>,
    last_observed_price: Option<f64>,
    last_triggered_at_ms: Option<u64>,
    last_trigger_kind: Option<String>,
    last_submit_status: Option<String>,
    last_error: Option<String>,
    retry_after_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
struct MarketCapabilitiesResponse {
    environment: String,
    default_market: String,
    markets: Vec<MarketCapability>,
}

#[derive(Debug, Serialize)]
struct MarketCapability {
    market: String,
    label: String,
    dex: String,
    live_trading_supported: bool,
}

impl ManualOrderPayload {
    fn source_module(&self) -> Result<&'static str> {
        if let Some(module) = self.source_module.as_deref() {
            return normalize_module_name(module);
        }
        let operator = self.operator.trim().to_ascii_lowercase();
        if operator.starts_with("fib") {
            return Ok("fib");
        }
        if operator.starts_with("copy") {
            return Ok("copy");
        }
        Ok("manual")
    }

    fn try_into_request(self, dry_run: bool) -> Result<ManualOrderRequest> {
        let source_module = self.source_module()?.to_string();
        Ok(ManualOrderRequest {
            request_id: format!("req-{}", now_ms()),
            operator: self.operator,
            source_module,
            target_accounts: self.target_accounts,
            coin: self.coin,
            side: parse_side(&self.side)?,
            notional_usd: self.notional_usd,
            reduce_only: self.reduce_only,
            execution_mode: parse_execution_mode(&self.execution_mode)?,
            max_slippage_bps: self.max_slippage_bps,
            dry_run_expected: dry_run,
            requested_at_ms: now_ms(),
            client_note: self.client_note,
        })
    }

    fn market_profile(&self, config: &AppConfig) -> Result<MarketProfile> {
        resolve_market_profile(self.market.as_deref(), config)
    }
}

impl ManualLeveragePayload {
    fn try_into_options(self) -> Result<ManualLeverageUpdateOptions> {
        anyhow::ensure!(
            self.leverage.is_finite() && self.leverage >= 1.0,
            "leverage must be at least 1x"
        );
        anyhow::ensure!(
            (self.leverage - self.leverage.round()).abs() < 1e-9,
            "leverage must be an integer value"
        );
        let leverage = self.leverage.round() as u32;
        Ok(ManualLeverageUpdateOptions {
            account_id: self.account_id,
            coin: self.coin,
            leverage,
            margin_mode: self.margin_mode,
            submit: self.submit,
            confirm_mainnet_live: self.confirm_mainnet_live,
        })
    }

    fn market_profile(&self, config: &AppConfig) -> Result<MarketProfile> {
        resolve_market_profile(self.market.as_deref(), config)
    }
}

impl ProtectiveExitPayload {
    fn market_profile(&self, config: &AppConfig) -> Result<MarketProfile> {
        resolve_market_profile(self.market.as_deref(), config)
    }

    fn into_options(mut self, market: &MarketProfile) -> ProtectiveExitOptions {
        self.exit.coin = normalize_coin_for_market(market, &self.exit.coin);
        self.exit
    }
}

impl ProtectiveExitTriggerPayload {
    fn market_profile(&self, config: &AppConfig) -> Result<MarketProfile> {
        resolve_market_profile(self.market.as_deref(), config)
    }

    fn into_options(mut self, market: &MarketProfile) -> ProtectiveExitTriggerCheckOptions {
        self.trigger.exit.coin = normalize_coin_for_market(market, &self.trigger.exit.coin);
        self.trigger
    }
}

impl ProtectiveExitSubmitPayload {
    fn market_profile(&self, config: &AppConfig) -> Result<MarketProfile> {
        resolve_market_profile(self.market.as_deref(), config)
    }

    fn into_options(mut self, market: &MarketProfile) -> ProtectiveExitSubmitOptions {
        self.submit.trigger.exit.coin =
            normalize_coin_for_market(market, &self.submit.trigger.exit.coin);
        self.submit
    }
}

impl ProtectiveExitArmPayload {
    fn market_profile(&self, config: &AppConfig) -> Result<MarketProfile> {
        resolve_market_profile(self.market.as_deref(), config)
    }

    fn into_options(mut self, market: &MarketProfile) -> ProtectiveExitArmOptions {
        self.arm.exit.coin = normalize_coin_for_market(market, &self.arm.exit.coin);
        self.arm
    }
}

impl ManualProtectiveRulesPayload {
    fn market_profile(&self, config: &AppConfig) -> Result<MarketProfile> {
        resolve_market_profile(self.market.as_deref(), config)
    }

    fn selected_account_ids(&self, config: &AppConfig) -> Vec<String> {
        selected_enabled_account_ids(config, &self.account_ids)
    }
}

#[derive(Debug, Deserialize)]
struct SignedSmokePayload {
    account_id: String,
    coin: String,
    side: String,
    notional_usd: f64,
    max_slippage_bps: f64,
    #[serde(default = "default_taker")]
    execution_mode: String,
    #[serde(default)]
    reduce_only: bool,
    #[serde(default)]
    close_full_position: bool,
    #[serde(default)]
    submit: bool,
    #[serde(default = "default_true")]
    cancel_resting: bool,
    #[serde(default)]
    confirm_mainnet_live: bool,
    #[serde(default)]
    source_module: Option<String>,
    #[serde(default)]
    market: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct LiveReadinessPayload {
    account_id: String,
    coin: String,
    side: String,
    notional_usd: f64,
    max_slippage_bps: f64,
    #[serde(default = "default_taker")]
    execution_mode: String,
    #[serde(default)]
    reduce_only: bool,
    #[serde(default)]
    source_module: Option<String>,
    #[serde(default)]
    market: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LiveReadinessBatchPayload {
    #[serde(default)]
    account_ids: Vec<String>,
    coin: String,
    side: String,
    notional_usd: f64,
    max_slippage_bps: f64,
    #[serde(default = "default_taker")]
    execution_mode: String,
    #[serde(default)]
    reduce_only: bool,
    #[serde(default)]
    source_module: Option<String>,
    #[serde(default)]
    market: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ModuleSymbolPolicyPayload {
    module: String,
    #[serde(default, alias = "allowed_symbols")]
    blocked_symbols: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ModuleSymbolPolicyResponse {
    module: String,
    blocked_symbols: Vec<String>,
    block_none: bool,
}

impl LiveReadinessBatchPayload {
    fn source_module(&self) -> Result<&'static str> {
        normalize_optional_module_name(self.source_module.as_deref())
    }

    fn selected_account_ids(&self, config: &AppConfig) -> Vec<String> {
        selected_enabled_account_ids(config, &self.account_ids)
    }

    fn for_account(&self, account_id: String) -> LiveReadinessPayload {
        LiveReadinessPayload {
            account_id,
            coin: self.coin.clone(),
            side: self.side.clone(),
            notional_usd: self.notional_usd,
            max_slippage_bps: self.max_slippage_bps,
            execution_mode: self.execution_mode.clone(),
            reduce_only: self.reduce_only,
            source_module: self.source_module.clone(),
            market: self.market.clone(),
        }
    }
}

fn selected_enabled_account_ids(config: &AppConfig, account_ids: &[String]) -> Vec<String> {
    let source: Vec<String> = if account_ids.is_empty() {
        config
            .accounts
            .iter()
            .filter(|account| account.enabled && account.worker_enabled)
            .map(|account| account.account_id.clone())
            .collect()
    } else {
        account_ids.to_vec()
    };
    let mut selected = Vec::new();
    for account_id in source {
        let account_id = account_id.trim();
        if !account_id.is_empty() && !selected.iter().any(|seen| seen == account_id) {
            selected.push(account_id.to_string());
        }
    }
    selected
}

fn ensure_accounts_allowed_for_market(
    config: &AppConfig,
    account_ids: &[String],
    market: &str,
) -> Result<()> {
    let canonical_market =
        normalize_market_id(market).ok_or_else(|| anyhow::anyhow!("unknown market: {market}"))?;
    for account_id in account_ids {
        let account = config
            .account(account_id)
            .with_context(|| format!("account {account_id} not found in config"))?;
        anyhow::ensure!(
            account.market_allowed(canonical_market),
            "account {} is blocked for market {}",
            account.account_id,
            canonical_market
        );
    }
    Ok(())
}

fn validate_batch_account_count(
    label: &str,
    config: &AppConfig,
    account_ids: &[String],
) -> Result<()> {
    anyhow::ensure!(
        !account_ids.is_empty(),
        "{label} requires at least one account"
    );
    anyhow::ensure!(
        account_ids.len() <= config.manual_ops.max_manual_batch_accounts,
        "{label} targets more accounts than max_manual_batch_accounts"
    );
    Ok(())
}

#[derive(Debug, Serialize)]
struct LiveReadinessBatchResponse {
    environment: String,
    dry_run: bool,
    coin: String,
    side: String,
    notional_usd: f64,
    execution_mode: String,
    reduce_only: bool,
    ready_account_ids: Vec<String>,
    blocked_account_ids: Vec<String>,
    results: Vec<LiveReadinessResponse>,
}

#[derive(Debug, Deserialize)]
struct AccountReconciliationPayload {
    account_id: String,
}

#[derive(Debug, Deserialize)]
struct AccountFundingPayload {
    account_id: String,
}

#[derive(Debug, Deserialize)]
struct AccountFundingBatchPayload {
    #[serde(default)]
    account_ids: Vec<String>,
    #[serde(default)]
    force_fresh: bool,
}

#[derive(Debug, Clone, Serialize)]
struct AccountFundingResponse {
    environment: String,
    dex: String,
    account_id: String,
    address: String,
    default_perp: PerpFundingLayer,
    xyz_perp: PerpFundingLayer,
    spot: SpotFundingLayer,
    funding_summary: String,
    next_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct AccountFundingBatchResponse {
    environment: String,
    dex: String,
    account_ids: Vec<String>,
    ready_account_ids: Vec<String>,
    transfer_needed_account_ids: Vec<String>,
    failed_account_ids: Vec<String>,
    results: Vec<ApiResult<AccountFundingResponse>>,
}

#[derive(Debug, Deserialize)]
struct UsdcDexTransferBatchPayload {
    #[serde(default)]
    account_ids: Vec<String>,
    #[serde(default)]
    destination_account_id: Option<String>,
    amount_usdc: f64,
    #[serde(default)]
    source_dex: Option<String>,
    #[serde(default)]
    destination_dex: Option<String>,
    #[serde(default)]
    submit: bool,
    #[serde(default)]
    confirm_mainnet_live: bool,
}

impl UsdcDexTransferBatchPayload {
    fn for_account(&self, account_id: String) -> UsdcDexTransferOptions {
        UsdcDexTransferOptions {
            account_id,
            destination_account_id: self.destination_account_id.clone(),
            amount_usdc: self.amount_usdc,
            source_dex: self.source_dex.clone(),
            destination_dex: self.destination_dex.clone(),
            submit: false,
            confirm_mainnet_live: self.confirm_mainnet_live,
        }
    }
}

#[derive(Debug, Deserialize)]
struct MainnetSmokePlanPayload {
    #[serde(default)]
    account_ids: Vec<String>,
    funding_amount_usdc: f64,
    #[serde(default)]
    destination_dex: Option<String>,
    coin: String,
    side: String,
    order_notional_usd: f64,
    max_slippage_bps: f64,
    execution_mode: String,
}

#[derive(Debug, Serialize)]
struct UsdcDexTransferBatchResponse {
    environment: String,
    dex: String,
    account_ids: Vec<String>,
    destination_account_id: Option<String>,
    planned_account_ids: Vec<String>,
    failed_account_ids: Vec<String>,
    amount_usdc: f64,
    source_dex: String,
    destination_dex: String,
    submit_requested: bool,
    submitted: bool,
    results: Vec<ApiResult<UsdcDexTransferResult>>,
}

#[derive(Debug, Serialize)]
struct UsdcDexTransferReadinessResponse {
    environment: String,
    dry_run: bool,
    account_id: String,
    address: Option<String>,
    destination_account_id: String,
    destination_address: Option<String>,
    amount_usdc: f64,
    source_dex: String,
    destination_dex: String,
    confirmation_phrase: Option<String>,
    ready_for_testnet_transfer: bool,
    ready_for_mainnet_transfer: bool,
    rate_limit: Option<UserRateLimit>,
    plan: Option<UsdcDexTransferResult>,
    checks: Vec<ReadinessCheckResponse>,
    failed_blockers: Vec<String>,
    next_actions: Vec<String>,
    readiness_summary: String,
}

#[derive(Debug, Serialize)]
struct UsdcDexTransferReadinessBatchResponse {
    environment: String,
    dex: String,
    account_ids: Vec<String>,
    destination_account_id: Option<String>,
    ready_account_ids: Vec<String>,
    blocked_account_ids: Vec<String>,
    amount_usdc: f64,
    source_dex: String,
    destination_dex: String,
    results: Vec<UsdcDexTransferReadinessResponse>,
}

#[derive(Debug, Clone, Serialize)]
struct PerpFundingLayer {
    name: String,
    query_ok: bool,
    error: Option<String>,
    account_value_usd: f64,
    withdrawable_usd: f64,
    total_notional_position_usd: f64,
    total_margin_used_usd: f64,
    position_count: usize,
    positions: Vec<PerpFundingPosition>,
}

impl PerpFundingLayer {
    fn from_result(name: &str, result: Result<ClearinghouseState>) -> Self {
        match result {
            Ok(state) => Self::from_state(name, &state),
            Err(error) => Self {
                name: name.to_string(),
                query_ok: false,
                error: Some(error.to_string()),
                account_value_usd: 0.0,
                withdrawable_usd: 0.0,
                total_notional_position_usd: 0.0,
                total_margin_used_usd: 0.0,
                position_count: 0,
                positions: Vec::new(),
            },
        }
    }

    fn from_state(name: &str, state: &ClearinghouseState) -> Self {
        let positions = state
            .asset_positions
            .iter()
            .map(|asset_position| {
                let position = &asset_position.position;
                PerpFundingPosition {
                    coin: position.coin.clone(),
                    size: parse_frontend_decimal(&position.szi),
                    entry_price: position
                        .entry_px
                        .as_deref()
                        .map(parse_frontend_decimal)
                        .unwrap_or_default(),
                    position_value_usd: position
                        .position_value
                        .as_deref()
                        .map(parse_frontend_decimal)
                        .unwrap_or_default(),
                    unrealized_pnl_usd: position
                        .unrealized_pnl
                        .as_deref()
                        .map(parse_frontend_decimal)
                        .unwrap_or_default(),
                }
            })
            .collect::<Vec<_>>();
        Self {
            name: name.to_string(),
            query_ok: true,
            error: None,
            account_value_usd: parse_frontend_decimal(&state.margin_summary.account_value),
            withdrawable_usd: state
                .withdrawable
                .as_deref()
                .map(parse_frontend_decimal)
                .unwrap_or_default(),
            total_notional_position_usd: parse_frontend_decimal(
                &state.margin_summary.total_ntl_pos,
            ),
            total_margin_used_usd: parse_frontend_decimal(&state.margin_summary.total_margin_used),
            position_count: positions.len(),
            positions,
        }
    }

    fn has_collateral(&self) -> bool {
        self.account_value_usd > 0.0 || self.withdrawable_usd > 0.0
    }
}

#[derive(Debug, Clone, Serialize)]
struct PerpFundingPosition {
    coin: String,
    size: f64,
    entry_price: f64,
    position_value_usd: f64,
    unrealized_pnl_usd: f64,
}

#[derive(Debug, Clone, Serialize)]
struct SpotFundingLayer {
    query_ok: bool,
    error: Option<String>,
    total_usdc: f64,
    hold_usdc: f64,
    balance_count: usize,
    balances: Vec<SpotFundingBalance>,
}

impl SpotFundingLayer {
    fn from_result(result: Result<SpotClearinghouseState>) -> Self {
        match result {
            Ok(state) => Self::from_state(&state),
            Err(error) => Self {
                query_ok: false,
                error: Some(error.to_string()),
                total_usdc: 0.0,
                hold_usdc: 0.0,
                balance_count: 0,
                balances: Vec::new(),
            },
        }
    }

    fn from_state(state: &SpotClearinghouseState) -> Self {
        let balances = state
            .balances
            .iter()
            .map(|balance| SpotFundingBalance {
                coin: balance.coin.clone(),
                total: parse_frontend_decimal(&balance.total),
                hold: parse_frontend_decimal(&balance.hold),
            })
            .collect::<Vec<_>>();
        let total_usdc = balances
            .iter()
            .filter(|balance| balance.coin.eq_ignore_ascii_case("USDC"))
            .map(|balance| balance.total)
            .sum::<f64>();
        let hold_usdc = balances
            .iter()
            .filter(|balance| balance.coin.eq_ignore_ascii_case("USDC"))
            .map(|balance| balance.hold)
            .sum::<f64>();
        Self {
            query_ok: true,
            error: None,
            total_usdc: normalize_frontend_zero(total_usdc),
            hold_usdc: normalize_frontend_zero(hold_usdc),
            balance_count: balances.len(),
            balances,
        }
    }

    fn has_usdc(&self) -> bool {
        self.total_usdc > 0.0
    }
}

#[derive(Debug, Clone, Serialize)]
struct SpotFundingBalance {
    coin: String,
    total: f64,
    hold: f64,
}

async fn build_account_funding(
    config: &AppConfig,
    account_id: &str,
    realtime: Option<&RealtimeState>,
) -> Result<AccountFundingResponse> {
    let account = config
        .account(account_id)
        .cloned()
        .with_context(|| format!("account {account_id} not found in config"))?;
    anyhow::ensure!(
        account.enabled && account.worker_enabled,
        "account {} is not enabled for worker execution",
        account.account_id
    );

    let default_ws =
        realtime.and_then(|state| state.clearinghouse_state(MARKET_HL_PERP, &account.address));
    let xyz_ws =
        realtime.and_then(|state| state.clearinghouse_state(MARKET_XYZ_PERP, &account.address));
    let spot_ws = realtime.and_then(|state| state.spot_state(&account.address));

    let default_perp = if let Some(state) = default_ws {
        PerpFundingLayer::from_state("default_perp", &state)
    } else {
        PerpFundingLayer::from_result(
            "default_perp",
            fetch_default_clearinghouse_state(&config.app.environment, &account.address).await,
        )
    };
    let xyz_perp = if let Some(state) = xyz_ws {
        PerpFundingLayer::from_state("xyz_perp", &state)
    } else {
        PerpFundingLayer::from_result(
            "xyz_perp",
            fetch_clearinghouse_state(
                &config.app.environment,
                &config.hyperliquid.dex,
                &account.address,
            )
            .await,
        )
    };
    let spot = if let Some(state) = spot_ws {
        SpotFundingLayer::from_state(&state)
    } else {
        SpotFundingLayer::from_result(
            fetch_spot_clearinghouse_state(&config.app.environment, &account.address).await,
        )
    };
    let next_actions = account_funding_next_actions(
        &default_perp,
        &xyz_perp,
        &spot,
        &config.app.environment,
        &config.hyperliquid.dex,
    );
    let funding_summary = account_funding_summary(&default_perp, &xyz_perp, &spot);

    Ok(AccountFundingResponse {
        environment: config.app.environment.clone(),
        dex: config.hyperliquid.dex.clone(),
        account_id: account.account_id,
        address: account.address,
        default_perp,
        xyz_perp,
        spot,
        funding_summary,
        next_actions,
    })
}

async fn build_usdc_dex_transfer_readiness(
    state: &FrontendAppState,
    config: &AppConfig,
    payload: UsdcDexTransferOptions,
) -> Result<UsdcDexTransferReadinessResponse> {
    let mut checks = Vec::new();
    let source_account = config.account(&payload.account_id);
    let destination_account_id = normalize_transfer_destination_account_id(
        &payload.account_id,
        payload.destination_account_id.as_deref(),
    );
    let destination_account = config.account(&destination_account_id);
    let source_dex =
        normalize_transfer_layer_value(payload.source_dex.as_deref().unwrap_or_default());
    let destination_dex = payload
        .destination_dex
        .as_deref()
        .map(normalize_transfer_layer_value)
        .unwrap_or_else(|| normalize_transfer_layer_value(&config.hyperliquid.dex));
    let destination_label = transfer_layer_label(&destination_dex);
    let confirmation_phrase = (config.app.environment == "mainnet").then(|| {
        format!(
            "TRANSFER {} USDC TO {} FOR {}",
            transfer_amount_label(payload.amount_usdc),
            destination_label,
            payload.account_id
        )
    });
    let needs_mainnet_confirmation = config.app.environment == "mainnet" && payload.submit;

    checks.push(ReadinessCheckResponse::blocker(
        "account_configured",
        source_account.is_some(),
        if source_account.is_some() {
            "account exists in current config".to_string()
        } else {
            format!("account {} is missing from config", payload.account_id)
        },
    ));

    if let Some(account) = source_account {
        checks.push(ReadinessCheckResponse::blocker(
            "account_enabled",
            account.enabled && account.worker_enabled,
            "account must be enabled and worker_enabled",
        ));
        checks.push(ReadinessCheckResponse::blocker(
            "address_not_placeholder",
            is_probably_real_address(&account.address),
            "account address must be a real master/subaccount address, not an example placeholder",
        ));
    }
    checks.push(ReadinessCheckResponse::blocker(
        "destination_account_configured",
        destination_account.is_some(),
        if destination_account.is_some() {
            "destination account exists in current config".to_string()
        } else {
            format!(
                "destination account {} is missing from config",
                destination_account_id
            )
        },
    ));
    if let Some(account) = destination_account {
        checks.push(ReadinessCheckResponse::blocker(
            "destination_account_enabled",
            account.enabled && account.worker_enabled,
            "destination account must be enabled and worker_enabled",
        ));
        checks.push(ReadinessCheckResponse::blocker(
            "destination_address_not_placeholder",
            is_probably_real_address(&account.address),
            "destination account address must be a real master/subaccount address, not an example placeholder",
        ));
    }

    checks.push(ReadinessCheckResponse::blocker(
        "amount_positive",
        payload.amount_usdc.is_finite() && payload.amount_usdc > 0.0,
        "amount_usdc must be positive",
    ));
    checks.push(ReadinessCheckResponse::blocker(
        "amount_within_helper_cap",
        payload.amount_usdc.is_finite() && payload.amount_usdc <= 10.0,
        "funding transfer helper is capped at 10 USDC per account",
    ));
    checks.push(ReadinessCheckResponse::blocker(
        "source_layer_supported",
        transfer_layer_supported(&source_dex),
        "source layer must be default_perp (empty), spot, or a valid perp dex name",
    ));
    checks.push(ReadinessCheckResponse::blocker(
        "destination_layer_supported",
        transfer_layer_supported(&destination_dex),
        "destination layer must be default_perp (empty), spot, or a valid perp dex name",
    ));
    let source_address = source_account.map(|account| account.address.as_str());
    let destination_address = destination_account.map(|account| account.address.as_str());
    checks.push(ReadinessCheckResponse::blocker(
        "route_changes_state",
        source_address.is_none()
            || destination_address.is_none()
            || source_address != destination_address
            || source_dex != destination_dex,
        "source and destination must not be the same account layer",
    ));
    checks.push(ReadinessCheckResponse::blocker(
        "config_dry_run_disabled",
        !config.app.dry_run,
        "USDC transfer submit requires app.dry_run=false in the config file",
    ));
    checks.push(ReadinessCheckResponse::blocker(
        "manual_live_enabled",
        config.manual_ops.manual_live_enabled,
        "USDC transfer submit requires manual_ops.manual_live_enabled=true",
    ));
    checks.push(ReadinessCheckResponse::blocker(
        "mainnet_gate",
        config.app.environment != "mainnet" || config.manual_ops.mainnet_live_enabled,
        "mainnet transfer requires manual_ops.mainnet_live_enabled=true",
    ));
    checks.push(ReadinessCheckResponse::blocker(
        "mainnet_explicit_confirmation",
        !needs_mainnet_confirmation || payload.confirm_mainnet_live,
        if needs_mainnet_confirmation {
            confirmation_phrase
                .as_ref()
                .map(|phrase| format!("mainnet submit requires exact phrase: {phrase}"))
                .unwrap_or_else(|| "mainnet submit requires explicit confirmation".to_string())
        } else {
            "mainnet confirmation is required only for submit requests".to_string()
        },
    ));

    let vault_path = PathBuf::from(&config.secrets.vault_path);
    let vault_summary = state.vault_summary(&vault_path)?;
    checks.push(ReadinessCheckResponse::blocker(
        "vault_file_exists",
        vault_summary.exists,
        format!("vault path: {}", vault_summary.path),
    ));
    checks.push(ReadinessCheckResponse::blocker(
        "vault_unlocked",
        vault_summary.unlocked,
        "vault must be unlocked in this console process before signed transfer",
    ));

    let transfer_secret_check = if let Some(account) = source_account {
        if let Ok(password) = state.resolve_vault_password(&vault_path, "") {
            load_transfer_secret(config, account, Some(&password)).map(|secret| {
                format!(
                    "EVM transfer signer {} is available and matches account {}",
                    secret.signer_address, account.account_id
                )
            })
        } else {
            Err(anyhow::anyhow!(
                "vault must be unlocked to validate transfer signer {}",
                transfer_secret_id(account)
            ))
        }
    } else {
        Err(anyhow::anyhow!("source account is missing from config"))
    };
    checks.push(ReadinessCheckResponse::blocker(
        "evm_transfer_signer_available",
        transfer_secret_check.is_ok(),
        transfer_secret_check.unwrap_or_else(|error| error.to_string()),
    ));

    let rate_limit = if let Some(account) = source_account {
        match fetch_user_rate_limit(&config.app.environment, &account.address).await {
            Ok(rate_limit) => {
                let remaining = rate_limit.request_capacity_remaining();
                checks.push(ReadinessCheckResponse::blocker(
                    "user_rate_limit_available",
                    true,
                    format!(
                        "used {} / cap {}, surplus {}, cumVlm {}",
                        rate_limit.n_requests_used,
                        rate_limit.n_requests_cap,
                        rate_limit.n_requests_surplus,
                        rate_limit.cum_vlm
                    ),
                ));
                checks.push(ReadinessCheckResponse::blocker(
                    "user_rate_limit_has_capacity",
                    remaining > 0,
                    format!("remaining request capacity before local throttling: {remaining}"),
                ));
                Some(rate_limit)
            }
            Err(error) => {
                checks.push(ReadinessCheckResponse::blocker(
                    "user_rate_limit_available",
                    false,
                    format!("failed to fetch userRateLimit: {error}"),
                ));
                None
            }
        }
    } else {
        None
    };

    let mut plan_options = payload.clone();
    plan_options.submit = false;
    let plan = match execute_usdc_dex_transfer(config.clone(), plan_options, None).await {
        Ok(plan) => {
            checks.push(ReadinessCheckResponse::blocker(
                "source_layer_available_sufficient",
                plan.before.source_available_usdc + 1e-9 >= payload.amount_usdc,
                format!(
                    "source layer {} available {} must cover transfer {}",
                    transfer_layer_label(&source_dex),
                    plan.before.source_available_usdc,
                    payload.amount_usdc
                ),
            ));
            checks.push(ReadinessCheckResponse::blocker(
                "transfer_plan_valid",
                true,
                "read-only transfer plan constructed without signing",
            ));
            Some(plan)
        }
        Err(error) => {
            checks.push(ReadinessCheckResponse::blocker(
                "transfer_plan_valid",
                false,
                error.to_string(),
            ));
            None
        }
    };

    checks.push(ReadinessCheckResponse::info(
        "console_process_dry_run",
        !state.dry_run,
        if state.dry_run {
            "frontend console was started with --dry-run true"
        } else {
            "frontend console process is live-capable"
        },
    ));

    let blockers_clear = checks
        .iter()
        .filter(|check| check.severity == "blocker")
        .all(|check| check.ok);
    let ready_for_testnet_transfer = blockers_clear && config.app.environment == "testnet";
    let ready_for_mainnet_transfer = blockers_clear
        && config.app.environment == "mainnet"
        && config.manual_ops.mainnet_live_enabled;
    let failed_blockers = failed_readiness_blockers(&checks);
    let next_actions = usdc_transfer_readiness_next_actions(
        &checks,
        &config.app.environment,
        &source_dex,
        &destination_dex,
        confirmation_phrase.as_deref(),
    );
    let readiness_summary = usdc_transfer_readiness_summary(
        &config.app.environment,
        ready_for_testnet_transfer,
        ready_for_mainnet_transfer,
        &failed_blockers,
    );

    Ok(UsdcDexTransferReadinessResponse {
        environment: config.app.environment.clone(),
        dry_run: config.app.dry_run,
        account_id: payload.account_id,
        address: source_account.map(|account| account.address.clone()),
        destination_account_id,
        destination_address: destination_account.map(|account| account.address.clone()),
        amount_usdc: payload.amount_usdc,
        source_dex,
        destination_dex,
        confirmation_phrase,
        ready_for_testnet_transfer,
        ready_for_mainnet_transfer,
        rate_limit,
        plan,
        checks,
        failed_blockers,
        next_actions,
        readiness_summary,
    })
}

fn account_funding_summary(
    default_perp: &PerpFundingLayer,
    xyz_perp: &PerpFundingLayer,
    spot: &SpotFundingLayer,
) -> String {
    if xyz_perp.has_collateral() {
        "XYZ perp account has available collateral; rerun Preflight Selected before submitting."
            .to_string()
    } else if default_perp.has_collateral() {
        "Funds appear to be in the default perp account, not the XYZ perp account.".to_string()
    } else if spot.has_usdc() {
        "USDC appears to be in spot, not the XYZ perp account.".to_string()
    } else if !default_perp.query_ok || !xyz_perp.query_ok || !spot.query_ok {
        "One or more funding layers could not be queried; fix read-only account diagnostics first."
            .to_string()
    } else {
        "No USDC collateral detected in default perps, XYZ perps, or spot for this address."
            .to_string()
    }
}

fn account_funding_next_actions(
    default_perp: &PerpFundingLayer,
    xyz_perp: &PerpFundingLayer,
    spot: &SpotFundingLayer,
    environment: &str,
    dex: &str,
) -> Vec<String> {
    let mut actions = Vec::new();
    if xyz_perp.has_collateral() {
        actions.push(format!(
            "Rerun Preflight Selected. If at least one account is ready, use Runbook Submit for the approved {environment} smoke."
        ));
    } else if default_perp.has_collateral() {
        actions.push(
            "Transfer USDC from default perps into the XYZ perp account, then rerun Funding Check."
                .to_string(),
        );
    } else if spot.has_usdc() {
        actions.push(
            "Move USDC from spot into the intended perp layer and then into the XYZ perp account before submitting."
                .to_string(),
        );
    } else {
        actions.push(format!(
            "Fund this {environment} address or transfer USDC into the selected {dex} perp account before opening a position."
        ));
    }
    if !default_perp.query_ok || !xyz_perp.query_ok || !spot.query_ok {
        actions.push(
            "Rerun the read-only funding diagnostic after network/API access is healthy; do not submit while account state is incomplete."
                .to_string(),
        );
    }
    actions
}

fn transfer_amount_label(amount: f64) -> String {
    if !amount.is_finite() {
        return amount.to_string();
    }
    let mut label = format!("{amount:.6}");
    while label.contains('.') && label.ends_with('0') {
        label.pop();
    }
    if label.ends_with('.') {
        label.pop();
    }
    label
}

fn normalize_transfer_layer_value(raw: &str) -> String {
    let trimmed = raw.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        return String::new();
    }
    match trimmed.as_str() {
        "default" | "default_perp" | "hl_perp" => String::new(),
        "xyz_perp" => "xyz".to_string(),
        other => other.to_string(),
    }
}

fn normalize_transfer_destination_account_id(account_id: &str, raw: Option<&str>) -> String {
    let destination = raw.unwrap_or_default().trim();
    if destination.is_empty() || destination == "__same__" {
        account_id.to_string()
    } else {
        destination.to_string()
    }
}

fn transfer_layer_label(layer: &str) -> String {
    let trimmed = layer.trim();
    if trimmed.is_empty() {
        "default_perp".to_string()
    } else {
        trimmed.to_string()
    }
}

fn transfer_layer_supported(layer: &str) -> bool {
    let canonical = normalize_transfer_layer_value(layer);
    if canonical.is_empty() || canonical == "spot" {
        return true;
    }
    canonical
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
}

fn usdc_transfer_readiness_next_actions(
    checks: &[ReadinessCheckResponse],
    environment: &str,
    source_dex: &str,
    destination_dex: &str,
    confirmation_phrase: Option<&str>,
) -> Vec<String> {
    let mut actions = Vec::new();
    let mut push_unique = |action: String| {
        if !actions.iter().any(|existing| existing == &action) {
            actions.push(action);
        }
    };

    if failed_readiness_check(checks, "account_configured")
        || failed_readiness_check(checks, "account_enabled")
        || failed_readiness_check(checks, "address_not_placeholder")
    {
        push_unique(
            "Fix the selected account in config/local.toml or save it again through the Vault page."
                .to_string(),
        );
    }
    if failed_readiness_check(checks, "destination_account_configured")
        || failed_readiness_check(checks, "destination_account_enabled")
        || failed_readiness_check(checks, "destination_address_not_placeholder")
    {
        push_unique(
            "Fix the destination account in config/local.toml or save it again through the Vault page."
                .to_string(),
        );
    }
    if failed_readiness_check(checks, "amount_positive")
        || failed_readiness_check(checks, "amount_within_helper_cap")
    {
        push_unique("Use a positive transfer amount at or below 10 USDC per account.".to_string());
    }
    if failed_readiness_check(checks, "source_layer_supported") {
        push_unique(
            "Set source layer to default_perp, spot, or a valid perp dex name.".to_string(),
        );
    }
    if failed_readiness_check(checks, "destination_layer_supported") {
        push_unique(
            "Set destination layer to default_perp, spot, or a valid perp dex name.".to_string(),
        );
    }
    if failed_readiness_check(checks, "route_changes_state") {
        push_unique(format!(
            "Choose a non-identical route. Current route: {} -> {}.",
            transfer_layer_label(source_dex),
            transfer_layer_label(destination_dex)
        ));
    }
    if failed_readiness_check(checks, "config_dry_run_disabled") {
        push_unique(format!(
            "Set app.dry_run=false only for the approved {environment} USDC transfer window."
        ));
    }
    if failed_readiness_check(checks, "manual_live_enabled") {
        push_unique(format!(
            "Set manual_ops.manual_live_enabled=true only for the approved {environment} transfer window."
        ));
    }
    if failed_readiness_check(checks, "mainnet_gate") {
        push_unique(
            "Set manual_ops.mainnet_live_enabled=true only after confirming the exact mainnet transfer amount."
                .to_string(),
        );
    }
    if failed_readiness_check(checks, "mainnet_explicit_confirmation") {
        if let Some(phrase) = confirmation_phrase {
            push_unique(format!(
                "For mainnet submit, type the exact confirmation phrase: {phrase}"
            ));
        } else {
            push_unique("Provide the explicit mainnet confirmation before submit.".to_string());
        }
    }
    if failed_readiness_check(checks, "vault_file_exists") {
        push_unique(
            "Create or restore secrets/trade_xyz.vault from the Vault page before signed transfer."
                .to_string(),
        );
    }
    if failed_readiness_check(checks, "vault_unlocked")
        || failed_readiness_check(checks, "evm_transfer_signer_available")
    {
        push_unique(
            "Unlock the Vault in the current frontend process, then validate the EVM transfer signer for this account."
                .to_string(),
        );
    }
    if failed_readiness_check(checks, "user_rate_limit_available")
        || failed_readiness_check(checks, "user_rate_limit_has_capacity")
    {
        push_unique(
            "Wait for Hyperliquid request capacity to recover, then rerun transfer preflight."
                .to_string(),
        );
    }
    if failed_readiness_check(checks, "source_layer_available_sufficient")
        || failed_readiness_check(checks, "transfer_plan_valid")
    {
        push_unique(
            "Rerun Funding Check and lower the transfer amount if source-layer available USDC is insufficient."
                .to_string(),
        );
    }

    actions
}

fn usdc_transfer_readiness_summary(
    environment: &str,
    ready_for_testnet_transfer: bool,
    ready_for_mainnet_transfer: bool,
    failed_blockers: &[String],
) -> String {
    if ready_for_testnet_transfer {
        return "ready for testnet USDC transfer".to_string();
    }
    if ready_for_mainnet_transfer {
        return "ready for mainnet USDC transfer".to_string();
    }
    if failed_blockers.is_empty() {
        return format!(
            "{environment} USDC transfer is not ready; inspect environment and confirmation gates"
        );
    }
    format!(
        "{} USDC transfer blocked by {} check(s)",
        environment,
        failed_blockers.len()
    )
}

fn parse_frontend_decimal(value: &str) -> f64 {
    let parsed = value.parse::<f64>().unwrap_or_default();
    normalize_frontend_zero(parsed)
}

fn normalize_frontend_zero(value: f64) -> f64 {
    if value == 0.0 { 0.0 } else { value }
}

#[derive(Debug, Serialize)]
struct AccountReconciliationResponse {
    environment: String,
    dex: String,
    account_id: String,
    address: String,
    rate_limit: UserRateLimit,
    open_order_count: usize,
    fill_count: usize,
    open_orders: Vec<OpenOrder>,
    recent_fills: Vec<UserFill>,
}

#[derive(Debug, Deserialize)]
struct OrderStatusPayload {
    account_id: String,
    #[serde(default)]
    oid: Option<u64>,
    #[serde(default)]
    cloid: Option<String>,
}

impl OrderStatusPayload {
    fn query(&self) -> Result<OrderStatusQuery> {
        let cloid = self
            .cloid
            .as_deref()
            .map(str::trim)
            .filter(|cloid| !cloid.is_empty());
        match (self.oid, cloid) {
            (Some(oid), None) => Ok(OrderStatusQuery::Oid { oid }),
            (None, Some(cloid)) => Ok(OrderStatusQuery::Cloid {
                cloid: cloid.to_string(),
            }),
            (None, None) => anyhow::bail!("order status requires exactly one oid or cloid"),
            (Some(_), Some(_)) => anyhow::bail!("order status accepts only one oid or cloid"),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum OrderStatusQuery {
    Oid { oid: u64 },
    Cloid { cloid: String },
}

#[derive(Debug, Serialize)]
struct OrderStatusQueryResponse {
    environment: String,
    dex: String,
    account_id: String,
    address: String,
    query: OrderStatusQuery,
    order_status: OrderStatusResponse,
}

#[derive(Debug, Deserialize)]
struct CancelByCloidPayload {
    account_id: String,
    coin: String,
    cloid: String,
    #[serde(default)]
    confirm_mainnet_live: bool,
    #[serde(default)]
    market: Option<String>,
}

impl CancelByCloidPayload {
    fn market_profile(&self, config: &AppConfig) -> Result<MarketProfile> {
        resolve_market_profile(self.market.as_deref(), config)
    }
}

#[derive(Debug, Serialize)]
struct LiveReadinessResponse {
    environment: String,
    dry_run: bool,
    account_id: String,
    coin: String,
    notional_usd: f64,
    execution_mode: ExecutionMode,
    reduce_only: bool,
    ready_for_testnet_submit: bool,
    ready_for_mainnet_submit: bool,
    rate_limit: Option<UserRateLimit>,
    account_state: Option<AccountReadinessState>,
    plan: Option<LiveReadinessPlanResponse>,
    checks: Vec<ReadinessCheckResponse>,
    failed_blockers: Vec<String>,
    next_actions: Vec<String>,
    readiness_summary: String,
}

#[derive(Debug, Serialize)]
struct LiveReadinessPlanResponse {
    coin: String,
    asset_id: u32,
    reference_price: f64,
    limit_price: f64,
    size: f64,
    sz_decimals: u32,
    execution_mode: ExecutionMode,
    tif: String,
}

#[derive(Debug, Serialize)]
struct ReadinessCheckResponse {
    name: String,
    ok: bool,
    severity: String,
    detail: String,
}

impl ReadinessCheckResponse {
    fn blocker(name: impl Into<String>, ok: bool, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ok,
            severity: "blocker".to_string(),
            detail: detail.into(),
        }
    }

    fn info(name: impl Into<String>, ok: bool, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ok,
            severity: "info".to_string(),
            detail: detail.into(),
        }
    }
}

fn failed_readiness_blockers(checks: &[ReadinessCheckResponse]) -> Vec<String> {
    checks
        .iter()
        .filter(|check| check.severity == "blocker" && !check.ok)
        .map(|check| format!("{}: {}", check.name, check.detail))
        .collect()
}

fn failed_readiness_check(checks: &[ReadinessCheckResponse], name: &str) -> bool {
    checks
        .iter()
        .any(|check| check.severity == "blocker" && !check.ok && check.name == name)
}

fn live_readiness_next_actions(
    checks: &[ReadinessCheckResponse],
    reduce_only: bool,
    environment: &str,
    dex: &str,
    source_module: &str,
) -> Vec<String> {
    let mut actions = Vec::new();
    let mut push_unique = |action: &str| {
        if !actions.iter().any(|existing| existing == action) {
            actions.push(action.to_string());
        }
    };

    if failed_readiness_check(checks, "account_configured")
        || failed_readiness_check(checks, "account_enabled")
        || failed_readiness_check(checks, "address_not_placeholder")
    {
        push_unique(
            "Fix the selected account in config/local.toml or save it again through the Vault page.",
        );
    }
    if failed_readiness_check(checks, "config_dry_run_disabled") {
        push_unique(&format!(
            "Set app.dry_run=false only for the intended {environment} live smoke window."
        ));
    }
    if failed_readiness_check(checks, "manual_live_enabled") {
        push_unique(&format!(
            "Set manual_ops.manual_live_enabled=true only for the intended {environment} live smoke window."
        ));
    }
    if failed_readiness_check(checks, "mainnet_gate") {
        push_unique(
            "Do not continue on mainnet until manual_ops.mainnet_live_enabled=true and the explicit mainnet confirmation gate are both present.",
        );
    }
    if failed_readiness_check(checks, "global_kill_switch_clear") {
        push_unique(
            "Clear risk.global.kill_switch for opening orders, or use reduce-only only when that policy is enabled.",
        );
    }
    if failed_readiness_check(checks, "manual_notional_limit")
        || failed_readiness_check(checks, "account_notional_limit")
    {
        push_unique(
            "Lower the order notional or raise the matching account/manual notional cap deliberately.",
        );
    }
    if failed_readiness_check(checks, "exchange_min_order_notional") {
        push_unique(
            "Do not submit the opening order below the exchange minimum; use at least 10 USD only after explicit approval, or use a supported close path solely to exit an existing position/inventory.",
        );
    }
    if failed_readiness_check(checks, "exchange_min_order_notional_effective") {
        push_unique(
            "Increase requested notional above the minimum so the precision-rounded order still stays >= 10 USD at submit time.",
        );
    }
    if failed_readiness_check(checks, "symbol_allowed") {
        push_unique(&format!(
            "Remove the canonical symbol from {source_module} module blacklist, or choose another XYZ market."
        ));
    }
    if failed_readiness_check(checks, "vault_file_exists") {
        push_unique(
            "Create or restore secrets/trade_xyz.vault from the Vault page before signed submit.",
        );
    }
    if failed_readiness_check(checks, "vault_unlocked")
        || failed_readiness_check(checks, "api_wallet_secret_available")
    {
        push_unique(
            "Unlock the Vault in the current frontend process, then test the API wallet secret.",
        );
    }
    if failed_readiness_check(checks, "user_rate_limit_available")
        || failed_readiness_check(checks, "user_rate_limit_has_capacity")
    {
        push_unique("Wait for Hyperliquid request capacity to recover, then rerun preflight.");
    }
    if failed_readiness_check(checks, "clearinghouse_state_available") {
        push_unique(
            "Restore read access to Hyperliquid clearinghouseState before any signed submit.",
        );
    }
    if failed_readiness_check(checks, "account_has_available_collateral") {
        if dex.trim().eq_ignore_ascii_case("spot") {
            push_unique(&format!(
                "Fund or transfer USDC into the selected {environment} spot balance before opening a spot position."
            ));
        } else {
            push_unique(&format!(
                "Fund or transfer USDC into the selected {environment} {dex} perp account before opening a position."
            ));
        }
    }
    if failed_readiness_check(checks, "reduce_only_position_available") && reduce_only {
        push_unique(&format!(
            "Select an account with a matching reducible position, or create the opening {environment} position first."
        ));
    }
    if failed_readiness_check(checks, "signed_order_plan_valid") {
        push_unique(
            "Choose a symbol/notional combination that produces a valid precision-rounded XYZ perp order plan.",
        );
    }

    actions
}

fn live_readiness_summary(
    environment: &str,
    ready_for_testnet_submit: bool,
    ready_for_mainnet_submit: bool,
    failed_blockers: &[String],
) -> String {
    if ready_for_testnet_submit {
        return "ready for testnet signed submit".to_string();
    }
    if ready_for_mainnet_submit {
        return "ready for mainnet signed submit".to_string();
    }
    if failed_blockers.is_empty() {
        return format!(
            "{environment} submit is not ready; inspect environment and confirmation gates"
        );
    }
    format!(
        "{} signed submit blocked by {} check(s)",
        environment,
        failed_blockers.len()
    )
}

async fn build_live_readiness(
    state: &FrontendAppState,
    config: &AppConfig,
    source_module: &str,
    payload: LiveReadinessPayload,
) -> Result<LiveReadinessResponse> {
    let mut checks = Vec::new();
    let canonical_coin = if config.hyperliquid.dex.trim().eq_ignore_ascii_case("spot") {
        normalize_spot_coin(&payload.coin)
    } else {
        normalize_dex_coin(&config.hyperliquid.dex, &payload.coin)
    };
    let side = parse_side(&payload.side)?;
    let execution_mode = parse_execution_mode(&payload.execution_mode)?;
    let account = config.account(&payload.account_id);

    checks.push(ReadinessCheckResponse::blocker(
        "account_configured",
        account.is_some(),
        if account.is_some() {
            "account exists in current config".to_string()
        } else {
            format!("account {} is missing from config", payload.account_id)
        },
    ));

    if let Some(account) = account {
        checks.push(ReadinessCheckResponse::blocker(
            "account_enabled",
            account.enabled && account.worker_enabled,
            "account must be enabled and worker_enabled",
        ));
        checks.push(ReadinessCheckResponse::blocker(
            "address_not_placeholder",
            is_probably_real_address(&account.address),
            "account address must be a real master/subaccount address, not an example placeholder",
        ));
        checks.push(ReadinessCheckResponse::blocker(
            "account_notional_limit",
            payload.notional_usd > 0.0 && payload.notional_usd <= account.max_order_notional_usd,
            format!(
                "requested notional {} must be > 0 and <= account max {}",
                payload.notional_usd, account.max_order_notional_usd
            ),
        ));
    }

    checks.push(ReadinessCheckResponse::blocker(
        "config_dry_run_disabled",
        !config.app.dry_run,
        "signed submit requires app.dry_run=false in the config file",
    ));
    checks.push(ReadinessCheckResponse::blocker(
        "manual_live_enabled",
        config.manual_ops.manual_live_enabled,
        "signed submit requires manual_ops.manual_live_enabled=true",
    ));
    checks.push(ReadinessCheckResponse::blocker(
        "mainnet_gate",
        config.app.environment != "mainnet" || config.manual_ops.mainnet_live_enabled,
        "mainnet requires manual_ops.mainnet_live_enabled=true in addition to manual_live_enabled",
    ));
    let kill_switch_allows_order = !config.risk.global.kill_switch
        || (payload.reduce_only && config.risk.global.allow_reduce_only_when_killed);
    checks.push(ReadinessCheckResponse::blocker(
        "global_kill_switch_clear",
        kill_switch_allows_order,
        if payload.reduce_only && config.risk.global.kill_switch {
            "global kill switch is active, but reduce-only signed orders are allowed by config"
                .to_string()
        } else {
            "global kill switch must be false before signed submit can open a new position"
                .to_string()
        },
    ));
    checks.push(ReadinessCheckResponse::blocker(
        "manual_notional_limit",
        payload.notional_usd > 0.0
            && payload.notional_usd <= config.manual_ops.max_manual_order_notional_usd,
        format!(
            "requested notional {} must be > 0 and <= manual max {}",
            payload.notional_usd, config.manual_ops.max_manual_order_notional_usd
        ),
    ));
    checks.push(ReadinessCheckResponse::blocker(
        "exchange_min_order_notional",
        payload.reduce_only || payload.notional_usd >= HYPERLIQUID_MIN_ORDER_NOTIONAL_USD,
        format!(
            "opening orders must be at least {} USD; supported close paths are allowed to protect existing positions or spot inventory",
            HYPERLIQUID_MIN_ORDER_NOTIONAL_USD
        ),
    ));
    let symbol_allowed = config.symbol_allowed_for_module(source_module, &canonical_coin);
    checks.push(ReadinessCheckResponse::blocker(
        "symbol_allowed",
        symbol_allowed,
        if symbol_allowed {
            format!("{canonical_coin} is allowed by {source_module} module blacklist")
        } else {
            format!("{canonical_coin} is blocked by {source_module} module blacklist")
        },
    ));

    let vault_path = PathBuf::from(&config.secrets.vault_path);
    let vault_summary = state.vault_summary(&vault_path)?;
    checks.push(ReadinessCheckResponse::blocker(
        "vault_file_exists",
        vault_summary.exists,
        format!("vault path: {}", vault_summary.path),
    ));
    checks.push(ReadinessCheckResponse::blocker(
        "vault_unlocked",
        vault_summary.unlocked,
        "vault must be unlocked in this console process before signed submit",
    ));

    let secret_available = if let Some(account) = account {
        if let Ok(password) = state.resolve_vault_password(&vault_path, "") {
            let secret_id = account_secret_id(account);
            load_secret_by_id(
                &vault_path,
                &password,
                &secret_id,
                Some(&account.account_id),
            )
            .is_ok()
        } else {
            false
        }
    } else {
        false
    };
    checks.push(ReadinessCheckResponse::blocker(
        "api_wallet_secret_available",
        secret_available,
        "matching API wallet private key must be available in the unlocked vault",
    ));

    let rate_limit = if let Some(account) = account {
        match fetch_user_rate_limit(&config.app.environment, &account.address).await {
            Ok(rate_limit) => {
                let remaining = rate_limit.request_capacity_remaining();
                checks.push(ReadinessCheckResponse::blocker(
                    "user_rate_limit_available",
                    true,
                    format!(
                        "used {} / cap {}, surplus {}, cumVlm {}",
                        rate_limit.n_requests_used,
                        rate_limit.n_requests_cap,
                        rate_limit.n_requests_surplus,
                        rate_limit.cum_vlm
                    ),
                ));
                checks.push(ReadinessCheckResponse::blocker(
                    "user_rate_limit_has_capacity",
                    remaining > 0,
                    format!("remaining request capacity before local throttling: {remaining}"),
                ));
                Some(rate_limit)
            }
            Err(error) => {
                checks.push(ReadinessCheckResponse::blocker(
                    "user_rate_limit_available",
                    false,
                    format!("failed to fetch userRateLimit: {error}"),
                ));
                None
            }
        }
    } else {
        None
    };

    let account_state = if let Some(account) = account {
        if config.hyperliquid.dex.trim().eq_ignore_ascii_case("spot") {
            let spot_state = if let Some(cached) = state.realtime.spot_state(&account.address) {
                Ok(cached)
            } else {
                fetch_spot_clearinghouse_state(&config.app.environment, &account.address).await
            };
            match spot_state {
                Ok(state) => {
                    let summary = spot_account_readiness_state(&state, &canonical_coin);
                    checks.push(ReadinessCheckResponse::blocker(
                        "clearinghouse_state_available",
                        true,
                        format!(
                            "spotAvailableUsdc={}, spotBaseAvailable={}",
                            summary.withdrawable_usd, summary.coin_position_size
                        ),
                    ));
                    if payload.reduce_only {
                        checks.push(ReadinessCheckResponse::blocker(
                            "reduce_only_position_available",
                            reduce_only_spot_position_available(side, summary.coin_position_size),
                            reduce_only_spot_position_detail(side, summary.coin_position_size),
                        ));
                    } else {
                        checks.push(ReadinessCheckResponse::blocker(
                            "account_has_available_collateral",
                            account_has_opening_collateral(&summary),
                            format!(
                                "spotAvailableUsdc={}, requested notional={}",
                                summary.withdrawable_usd, payload.notional_usd
                            ),
                        ));
                    }
                    Some(summary)
                }
                Err(error) => {
                    checks.push(ReadinessCheckResponse::blocker(
                        "clearinghouse_state_available",
                        false,
                        format!("failed to fetch spotClearinghouseState: {error}"),
                    ));
                    if payload.reduce_only {
                        checks.push(ReadinessCheckResponse::blocker(
                            "reduce_only_position_available",
                            false,
                            "spotClearinghouseState is required before reduce-only spot validation",
                        ));
                    } else {
                        checks.push(ReadinessCheckResponse::blocker(
                            "account_has_available_collateral",
                            false,
                            "spotClearinghouseState is required before opening-order collateral validation",
                        ));
                    }
                    None
                }
            }
        } else {
            let market_id = frontend_perp_market_id_for_dex(&config.hyperliquid.dex);
            let clearinghouse_state = if let Some(cached) = state
                .realtime
                .clearinghouse_state(market_id, &account.address)
            {
                Ok(cached)
            } else {
                fetch_clearinghouse_state(
                    &config.app.environment,
                    &config.hyperliquid.dex,
                    &account.address,
                )
                .await
            };
            match clearinghouse_state {
                Ok(state) => {
                    let summary = summarize_account_readiness_state(
                        &config.hyperliquid.dex,
                        &state,
                        &canonical_coin,
                    );
                    checks.push(ReadinessCheckResponse::blocker(
                        "clearinghouse_state_available",
                        true,
                        format!(
                            "accountValue={}, withdrawable={}, coinPositionSize={}",
                            summary.account_value_usd,
                            summary.withdrawable_usd,
                            summary.coin_position_size
                        ),
                    ));
                    if payload.reduce_only {
                        checks.push(ReadinessCheckResponse::blocker(
                            "reduce_only_position_available",
                            reduce_only_position_available(side, summary.coin_position_size),
                            reduce_only_position_detail(side, summary.coin_position_size),
                        ));
                    } else {
                        checks.push(ReadinessCheckResponse::blocker(
                            "account_has_available_collateral",
                            account_has_opening_collateral(&summary),
                            format!(
                                "accountValue={}, withdrawable={}, requested notional={}",
                                summary.account_value_usd,
                                summary.withdrawable_usd,
                                payload.notional_usd
                            ),
                        ));
                    }
                    Some(summary)
                }
                Err(error) => {
                    checks.push(ReadinessCheckResponse::blocker(
                        "clearinghouse_state_available",
                        false,
                        format!("failed to fetch clearinghouseState: {error}"),
                    ));
                    if payload.reduce_only {
                        checks.push(ReadinessCheckResponse::blocker(
                            "reduce_only_position_available",
                            false,
                            "clearinghouseState is required before reduce-only position validation",
                        ));
                    } else {
                        checks.push(ReadinessCheckResponse::blocker(
                            "account_has_available_collateral",
                            false,
                            "clearinghouseState is required before opening-order collateral validation",
                        ));
                    }
                    None
                }
            }
        }
    } else {
        checks.push(ReadinessCheckResponse::blocker(
            "clearinghouse_state_available",
            false,
            "account must be configured before clearinghouseState can be fetched",
        ));
        None
    };

    let plan_result = if config.hyperliquid.dex.trim().eq_ignore_ascii_case("spot") {
        match fetch_spot_market_snapshot_cached(
            &config.app.environment,
            MARKET_SNAPSHOT_QUOTE_CACHE_TTL_MS,
        )
        .await
        {
            Ok(snapshot) => build_signed_spot_order_plan(
                &snapshot,
                &canonical_coin,
                side,
                payload.notional_usd,
                payload.max_slippage_bps,
                execution_mode,
            )
            .map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        }
    } else {
        match fetch_xyz_market_snapshot_cached(
            &config.app.environment,
            &config.hyperliquid.dex,
            MARKET_SNAPSHOT_QUOTE_CACHE_TTL_MS,
        )
        .await
        {
            Ok(snapshot) => build_signed_order_plan(
                &snapshot,
                &canonical_coin,
                side,
                payload.notional_usd,
                payload.max_slippage_bps,
                execution_mode,
                None,
            )
            .map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        }
    };
    let plan = match plan_result {
        Ok(plan) => {
            checks.push(ReadinessCheckResponse::blocker(
                "signed_order_plan_valid",
                true,
                "metadata, price, precision, and size checks passed",
            ));
            let effective_notional = effective_order_notional_usd(plan.limit_price, plan.size);
            checks.push(ReadinessCheckResponse::blocker(
                "exchange_min_order_notional_effective",
                effective_exchange_min_order_notional_ok(
                    plan.limit_price,
                    plan.size,
                    payload.reduce_only,
                ),
                format!(
                    "planned order value after precision rounding is {:.6} USD; opening orders must be at least {} USD",
                    effective_notional,
                    HYPERLIQUID_MIN_ORDER_NOTIONAL_USD
                ),
            ));
            Some(LiveReadinessPlanResponse {
                coin: plan.coin,
                asset_id: plan.asset_id,
                reference_price: plan.reference_price,
                limit_price: plan.limit_price,
                size: plan.size,
                sz_decimals: plan.sz_decimals,
                execution_mode,
                tif: tif_for_execution_mode(execution_mode),
            })
        }
        Err(error) => {
            checks.push(ReadinessCheckResponse::blocker(
                "signed_order_plan_valid",
                false,
                error,
            ));
            None
        }
    };

    checks.push(ReadinessCheckResponse::info(
        "console_process_dry_run",
        !state.dry_run,
        if state.dry_run {
            "frontend console was started with --dry-run true"
        } else {
            "frontend console process is live-capable"
        },
    ));

    let blockers_clear = checks
        .iter()
        .filter(|check| check.severity == "blocker")
        .all(|check| check.ok);
    let ready_for_testnet_submit = blockers_clear && config.app.environment == "testnet";
    let ready_for_mainnet_submit = blockers_clear
        && config.app.environment == "mainnet"
        && config.manual_ops.mainnet_live_enabled;
    let failed_blockers = failed_readiness_blockers(&checks);
    let next_actions = live_readiness_next_actions(
        &checks,
        payload.reduce_only,
        &config.app.environment,
        &config.hyperliquid.dex,
        source_module,
    );
    let readiness_summary = live_readiness_summary(
        &config.app.environment,
        ready_for_testnet_submit,
        ready_for_mainnet_submit,
        &failed_blockers,
    );

    Ok(LiveReadinessResponse {
        environment: config.app.environment.clone(),
        dry_run: config.app.dry_run,
        account_id: payload.account_id,
        coin: canonical_coin,
        notional_usd: payload.notional_usd,
        execution_mode,
        reduce_only: payload.reduce_only,
        ready_for_testnet_submit,
        ready_for_mainnet_submit,
        rate_limit,
        account_state,
        plan,
        checks,
        failed_blockers,
        next_actions,
        readiness_summary,
    })
}

fn is_probably_real_address(address: &str) -> bool {
    let trimmed = address.trim();
    let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    else {
        return false;
    };
    hex.len() == 40
        && hex.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit())
        && !hex.chars().take(38).all(|ch| ch == '0')
}

impl SignedSmokePayload {
    fn source_module(&self) -> Result<&'static str> {
        normalize_optional_module_name(self.source_module.as_deref())
    }

    fn market_profile(&self, config: &AppConfig) -> Result<MarketProfile> {
        resolve_market_profile(self.market.as_deref(), config)
    }

    fn try_into_options(self) -> Result<SignedSmokeOptions> {
        Ok(SignedSmokeOptions {
            account_id: self.account_id,
            coin: self.coin,
            side: parse_side(&self.side)?,
            notional_usd: self.notional_usd,
            max_slippage_bps: self.max_slippage_bps,
            execution_mode: parse_execution_mode(&self.execution_mode)?,
            reduce_only: self.reduce_only,
            close_full_position: self.close_full_position,
            submit: self.submit,
            cancel_resting: self.cancel_resting,
            confirm_mainnet_live: self.confirm_mainnet_live,
        })
    }

    fn try_into_acceptance_options(self) -> Result<SignedAcceptanceOptions> {
        Ok(SignedAcceptanceOptions {
            account_id: self.account_id,
            coin: self.coin,
            side: parse_side(&self.side)?,
            notional_usd: self.notional_usd,
            max_slippage_bps: self.max_slippage_bps,
            execution_mode: parse_execution_mode(&self.execution_mode)?,
            reduce_only: self.reduce_only,
            close_full_position: self.close_full_position,
            submit: self.submit,
            cancel_resting: self.cancel_resting,
            confirm_mainnet_live: self.confirm_mainnet_live,
        })
    }

    fn try_into_runbook_options(self) -> Result<SignedRunbookOptions> {
        Ok(SignedRunbookOptions {
            account_id: self.account_id,
            coin: self.coin,
            side: parse_side(&self.side)?,
            notional_usd: self.notional_usd,
            max_slippage_bps: self.max_slippage_bps,
            execution_mode: parse_execution_mode(&self.execution_mode)?,
            reduce_only: self.reduce_only,
            close_full_position: self.close_full_position,
            submit: self.submit,
            cancel_resting: self.cancel_resting,
            confirm_mainnet_live: self.confirm_mainnet_live,
        })
    }
}

#[derive(Debug, Serialize)]
struct ManualOrderResponse {
    accepted: bool,
    signal_id: String,
    target_accounts: Vec<String>,
    coin: String,
    side: String,
    notional_usd: f64,
    reduce_only: bool,
    dry_run: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct FibBasicPayload {
    #[serde(default)]
    strategy_id: Option<String>,
    #[serde(default)]
    direction: Option<String>,
    #[serde(default)]
    market: Option<String>,
    #[serde(default)]
    account_ids: Vec<String>,
    coin: String,
    timeframe: String,
    #[serde(default = "default_fib_lookback_bars")]
    lookback_bars: u32,
    #[serde(default)]
    levels: Vec<f64>,
    #[serde(default)]
    entry_above_tolerance_usd: f64,
    #[serde(default)]
    entry_below_tolerance_usd: f64,
    principal_usd: f64,
    #[serde(default = "default_one_f64")]
    leverage: f64,
    #[serde(default = "default_taker")]
    execution_mode: String,
    #[serde(default = "default_fib_price_delta_mode")]
    take_profit_mode: String,
    take_profit_value: f64,
    #[serde(default = "default_fib_principal_percent_mode")]
    stop_loss_mode: String,
    stop_loss_value: f64,
    #[serde(default = "default_fib_slippage_bps")]
    max_slippage_bps: f64,
    #[serde(default = "default_one_u32")]
    max_entries_per_level: u32,
    #[serde(default = "default_fib_cooldown_secs")]
    cooldown_secs: u64,
    #[serde(default = "default_fib_cooldown_secs")]
    stop_loss_cooldown_secs: u64,
    #[serde(default)]
    stop_loss_stop_strategy: bool,
    #[serde(default)]
    locked_range: bool,
    #[serde(default)]
    locked_swing_high: Option<f64>,
    #[serde(default)]
    locked_swing_low: Option<f64>,
    #[serde(default = "default_true")]
    auto_loop: bool,
    #[serde(default = "default_true")]
    dry_run: bool,
    #[serde(default)]
    live: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct FibCancelPayload {
    strategy_id: String,
    #[serde(default = "default_true")]
    dry_run: bool,
    #[serde(default)]
    live: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct FibAiProposalPayload {
    #[serde(default = "default_ai_mode")]
    mode: String,
    #[serde(default)]
    direction: Option<String>,
    #[serde(default)]
    market: Option<String>,
    coin: String,
    timeframe: String,
    #[serde(default = "default_fib_lookback_bars")]
    lookback_bars: u32,
    #[serde(default)]
    levels: Vec<f64>,
    #[serde(default)]
    entry_tolerance_usd: f64,
    #[serde(default = "default_fib_principal_usd")]
    principal_usd: f64,
    #[serde(default = "default_one_f64")]
    leverage: f64,
    #[serde(default = "default_fib_take_profit_value")]
    take_profit_value: f64,
    #[serde(default = "default_fib_stop_loss_value")]
    stop_loss_value: f64,
    #[serde(default = "default_fib_slippage_bps")]
    max_slippage_bps: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct FibWsCandleProbePayload {
    #[serde(default)]
    market: Option<String>,
    coin: String,
    timeframe: String,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct FibEntrySignalResponse {
    signal_id: String,
    target_accounts: Vec<String>,
    market: Option<String>,
    dex: Option<String>,
    coin: String,
    level: f64,
    side: String,
    order_notional_usd: f64,
    entry_price: f64,
    limit_price: Option<f64>,
    entry_zone_high: f64,
    entry_zone_low: f64,
    execution_mode: ExecutionMode,
}

#[derive(Debug, Clone, Serialize)]
struct FibAiProposalResponse {
    proposal_id: String,
    mode: String,
    market: String,
    direction: FibTradeDirection,
    coin: String,
    timeframe: String,
    swing_high: f64,
    swing_low: f64,
    levels: Vec<f64>,
    confidence: f64,
    reasons: Vec<String>,
}

#[derive(Debug, Serialize)]
struct FibInstancesResponse {
    instances: Vec<FibInstanceRecord>,
    fetched_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FibInstanceHistoryEntry {
    history_id: String,
    occurred_at_ms: u64,
    source: String,
    action: String,
    strategy_id: String,
    status: String,
    market: String,
    coin: String,
    timeframe: String,
    message: Option<String>,
    recovered_from_audit: bool,
    details: Value,
    record: Option<FibInstanceRecord>,
}

#[derive(Debug, Serialize)]
struct FibHistoryResponse {
    entries: Vec<FibInstanceHistoryEntry>,
    fetched_at_ms: u64,
}

#[derive(Debug, Serialize)]
struct FibInstanceActionResponse {
    action: String,
    instance: FibInstanceRecord,
    entry_signals: Vec<FibEntrySignalResponse>,
    entry_reports: Vec<WorkerReport>,
    cancel_reports: Vec<FibCancelOrderReport>,
    protection_reports: Vec<ProtectiveExitArmResult>,
    ai_proposals: Vec<FibAiProposalResponse>,
}

#[derive(Debug, Deserialize)]
struct FibPreviewPayload {
    strategy_id: String,
    #[serde(default)]
    direction: Option<String>,
    coin: String,
    timeframe: String,
    swing_high: f64,
    swing_low: f64,
    levels: Vec<f64>,
    entry_tolerance_usd: f64,
    take_profit_usd: f64,
    stop_loss_pct: f64,
    notional_usd: f64,
    #[serde(default)]
    market: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FibAutoDetectPayload {
    #[serde(default)]
    direction: Option<String>,
    coin: String,
    timeframe: String,
    #[serde(default = "default_fib_lookback_bars")]
    lookback_bars: u32,
    #[serde(default)]
    levels: Vec<f64>,
    entry_tolerance_usd: f64,
    take_profit_usd: f64,
    stop_loss_pct: f64,
    #[serde(default)]
    market: Option<String>,
}

impl FibPreviewPayload {
    fn market_profile(&self, config: &AppConfig) -> Result<MarketProfile> {
        resolve_market_profile(self.market.as_deref(), config)
    }

    fn try_into_strategy(self) -> Result<FibRetracementStrategy> {
        let direction = self.trade_direction()?;
        anyhow::ensure!(
            self.swing_high > self.swing_low,
            "swing_high must exceed swing_low"
        );
        anyhow::ensure!(
            !self.levels.is_empty(),
            "at least one fib level is required"
        );
        Ok(FibRetracementStrategy::new(FibRetracementConfig {
            strategy_id: self.strategy_id,
            direction,
            coin: self.coin,
            timeframe: self.timeframe,
            swing_high: self.swing_high,
            swing_low: self.swing_low,
            levels: self.levels,
            entry_tolerance_usd: self.entry_tolerance_usd,
            take_profit_usd: self.take_profit_usd,
            stop_loss_pct: self.stop_loss_pct,
            notional_usd: self.notional_usd,
            execution_mode: ExecutionMode::Taker,
            max_slippage_bps: 20.0,
        }))
    }

    fn trade_direction(&self) -> Result<FibTradeDirection> {
        FibTradeDirection::from_raw(self.direction.as_deref().unwrap_or("long"))
    }
}

impl FibAutoDetectPayload {
    fn market_profile(&self, config: &AppConfig) -> Result<MarketProfile> {
        resolve_market_profile(self.market.as_deref(), config)
    }

    fn trade_direction(&self) -> Result<FibTradeDirection> {
        FibTradeDirection::from_raw(self.direction.as_deref().unwrap_or("long"))
    }
}

impl FibBasicPayload {
    fn trade_direction(&self) -> Result<FibTradeDirection> {
        FibTradeDirection::from_raw(self.direction.as_deref().unwrap_or("long"))
    }

    fn default_strategy_id(&self) -> String {
        let market = self
            .market
            .as_deref()
            .unwrap_or(MARKET_XYZ_PERP)
            .replace('-', "_");
        let direction = self
            .direction
            .as_deref()
            .unwrap_or("long")
            .trim()
            .to_ascii_lowercase();
        format!(
            "fib_basic_{}_{}_{}_{}",
            market,
            self.coin.replace([':', '/'], "_"),
            self.timeframe,
            direction
        )
    }

    fn audit_details(&self, action: &str) -> Value {
        json!({
            "action": action,
            "strategy_id": self.strategy_id.clone().unwrap_or_else(|| self.default_strategy_id()),
            "direction": self.direction.clone().unwrap_or_else(|| "long".to_string()),
            "market": self.market.clone().unwrap_or_else(|| MARKET_XYZ_PERP.to_string()),
            "account_ids": self.account_ids.clone(),
            "coin": self.coin.clone(),
            "timeframe": self.timeframe.clone(),
            "lookback_bars": self.lookback_bars,
            "levels": self.levels.clone(),
            "entry_above_tolerance_usd": self.entry_above_tolerance_usd,
            "entry_below_tolerance_usd": self.entry_below_tolerance_usd,
            "principal_usd": self.principal_usd,
            "leverage": self.leverage,
            "execution_mode": self.execution_mode.clone(),
            "take_profit_mode": self.take_profit_mode.clone(),
            "take_profit_value": self.take_profit_value,
            "stop_loss_mode": self.stop_loss_mode.clone(),
            "stop_loss_value": self.stop_loss_value,
            "dry_run": self.dry_run,
            "live": self.live,
            "cooldown_secs": self.cooldown_secs,
            "stop_loss_cooldown_secs": self.stop_loss_cooldown_secs,
            "stop_loss_stop_strategy": self.stop_loss_stop_strategy,
        })
    }
}

fn default_fib_lookback_bars() -> u32 {
    120
}

fn default_one_f64() -> f64 {
    1.0
}

fn default_one_u32() -> u32 {
    1
}

fn default_fib_principal_usd() -> f64 {
    11.0
}

fn default_fib_take_profit_value() -> f64 {
    2.0
}

fn default_fib_stop_loss_value() -> f64 {
    3.0
}

fn default_fib_price_delta_mode() -> String {
    "price_delta_usd".to_string()
}

fn default_fib_principal_percent_mode() -> String {
    "principal_percent".to_string()
}

fn default_fib_slippage_bps() -> f64 {
    20.0
}

fn default_fib_cooldown_secs() -> u64 {
    300
}

fn default_ai_mode() -> String {
    "observe".to_string()
}

#[derive(Debug, Serialize)]
struct FibPreviewResponse {
    dry_run: bool,
    levels: Vec<FibLevelResponse>,
}

#[derive(Debug, Serialize)]
struct FibLevelResponse {
    level: f64,
    entry_price: f64,
    take_profit_price: f64,
    stop_loss_price: f64,
}

#[derive(Debug, Serialize)]
struct FibAutoDetectResponse {
    dry_run: bool,
    environment: String,
    dex: String,
    direction: FibTradeDirection,
    coin: String,
    timeframe: String,
    lookback_bars: u32,
    start_time_ms: u64,
    end_time_ms: u64,
    candles_used: usize,
    swing_high: f64,
    swing_low: f64,
    swing_high_time_ms: u64,
    swing_low_time_ms: u64,
    current_price: f64,
    entry_tolerance_usd: f64,
    triggered: bool,
    triggered_levels: Vec<f64>,
    nearest_level: Option<FibAutoLevelResponse>,
    levels: Vec<FibAutoLevelResponse>,
}

#[derive(Debug, Clone, Serialize)]
struct FibAutoLevelResponse {
    level: f64,
    entry_price: f64,
    take_profit_price: f64,
    stop_loss_price: f64,
    distance_usd: f64,
    within_tolerance: bool,
}

#[derive(Debug, Deserialize)]
struct SmartMoneyPreviewPayload {
    leader_id: String,
    leader_address: String,
    coin: String,
    side: String,
    leader_notional_usd: f64,
    copy_ratio: f64,
    max_signal_notional_usd: f64,
    reduce_only: bool,
    #[serde(default)]
    market: Option<String>,
}

impl SmartMoneyPreviewPayload {
    fn market_profile(&self, config: &AppConfig) -> Result<MarketProfile> {
        resolve_market_profile(self.market.as_deref(), config)
    }

    fn try_into_signal(self, config: &AppConfig) -> Result<Vec<crate::domain::CoordinatorSignal>> {
        anyhow::ensure!(
            self.leader_notional_usd > 0.0,
            "leader notional must be positive"
        );
        let mut strategy = SmartMoneyCopyStrategy::new(SmartMoneyCopyConfig {
            strategy_id: "smart_money_preview".to_string(),
            default_copy_ratio: self.copy_ratio,
            max_slippage_bps: 25.0,
            leaders: vec![LeaderRule {
                leader_id: self.leader_id.clone(),
                leader_address: self.leader_address.clone(),
                enabled: true,
                copy_ratio: self.copy_ratio,
            }],
            symbol_limits: vec![SymbolCopyLimit {
                coin: self.coin.clone(),
                max_signal_notional_usd: self.max_signal_notional_usd,
            }],
        });
        let ctx = StrategyContext {
            target_accounts: config
                .enabled_worker_accounts()
                .map(|account| account.account_id.clone())
                .collect(),
            signal_ttl_ms: config.process.signal_ttl_ms,
        };
        Ok(strategy.on_event(
            &ctx,
            StrategyEvent::LeaderFill(LeaderFillEvent {
                event_id: format!("preview-fill-{}", now_ms()),
                leader_id: self.leader_id,
                leader_address: self.leader_address,
                coin: self.coin,
                side: parse_side(&self.side)?,
                price: 1.0,
                size: self.leader_notional_usd,
                notional_usd: self.leader_notional_usd,
                reduce_only: self.reduce_only,
                exchange_time_ms: now_ms(),
                received_at_ms: now_ms(),
            }),
        ))
    }
}

#[derive(Debug, Serialize)]
struct SmartMoneyPreviewResponse {
    dry_run: bool,
    signals: usize,
    copied_notional_usd: f64,
}

#[derive(Debug, Deserialize)]
struct VaultUnlockPayload {
    password: String,
}

#[derive(Debug, Deserialize)]
struct VaultChangePasswordPayload {
    #[serde(default)]
    current_password: String,
    new_password: String,
    new_password_confirm: String,
}

#[derive(Debug, Deserialize)]
struct VaultUpsertPayload {
    #[serde(default)]
    password: String,
    account_id: String,
    address: String,
    secret_id: String,
    private_key: String,
    #[serde(default)]
    secret_usage: String,
}

#[derive(Debug, Deserialize)]
struct VaultSecretCheckPayload {
    #[serde(default)]
    password: String,
    account_id: String,
    #[serde(default)]
    secret_id: String,
    #[serde(default)]
    secret_usage: String,
}

#[derive(Debug, Serialize)]
struct VaultSecretCheckResponse {
    account_id: String,
    secret_id: String,
    available: bool,
    private_key_loaded: bool,
}

impl VaultSecretCheckPayload {
    fn try_check(
        self,
        config: &AppConfig,
        vault_path: &Path,
        password: &str,
    ) -> Result<VaultSecretCheckResponse> {
        let secret_id = if !self.secret_id.trim().is_empty() {
            self.secret_id.trim().to_string()
        } else if let Some(account) = config.account(&self.account_id) {
            match SecretUsage::parse(&self.secret_usage)? {
                SecretUsage::Trading => account_secret_id(account),
                SecretUsage::Transfer => transfer_secret_id(account),
            }
        } else {
            self.account_id.clone()
        };
        let secret = load_secret_by_id(vault_path, password, &secret_id, Some(&self.account_id))?;
        let response = VaultSecretCheckResponse {
            account_id: secret.account_id.clone(),
            secret_id: secret.secret_id.clone(),
            available: true,
            private_key_loaded: true,
        };
        Ok(response)
    }
}

impl VaultUpsertPayload {
    fn try_into_upsert(self, config: &AppConfig) -> Result<(String, SecretUsage, SecretUpsert)> {
        let secret_usage = SecretUsage::parse(&self.secret_usage)?;
        let fallback_account = config.account(&self.account_id);
        let secret_id = if self.secret_id.trim().is_empty() {
            if let Some(account) = fallback_account {
                match secret_usage {
                    SecretUsage::Trading => account_secret_id(account),
                    SecretUsage::Transfer => {
                        if account.transfer_secret_id.trim().is_empty() {
                            format!("{}_evm_wallet", account.account_id)
                        } else {
                            transfer_secret_id(account)
                        }
                    }
                }
            } else {
                match secret_usage {
                    SecretUsage::Trading => format!("{}_api_wallet", self.account_id),
                    SecretUsage::Transfer => format!("{}_evm_wallet", self.account_id),
                }
            }
        } else {
            self.secret_id
        };
        let address = if self.address.trim().is_empty() {
            fallback_account
                .map(|account| account.address.clone())
                .unwrap_or_default()
        } else {
            self.address
        };
        Ok((
            self.password,
            secret_usage,
            SecretUpsert {
                secret_id,
                account_id: self.account_id,
                address,
                api_wallet_private_key: self.private_key,
            },
        ))
    }
}

#[derive(Debug, Clone, Serialize)]
struct ApiResult<T> {
    ok: bool,
    data: Option<T>,
    error: Option<String>,
}

impl<T> ApiResult<T> {
    fn from_result(result: Result<T>) -> Self {
        match result {
            Ok(data) => Self {
                ok: true,
                data: Some(data),
                error: None,
            },
            Err(error) => Self {
                ok: false,
                data: None,
                error: Some(format_anyhow_error(&error)),
            },
        }
    }
}

fn format_anyhow_error(error: &anyhow::Error) -> String {
    format!("{error:#}")
}

fn open_order_matches_market(order: &OpenOrder, market: &MarketProfile) -> bool {
    if market.is_spot() {
        return order.coin.contains('/') || order.coin.trim().starts_with('@');
    }
    if market.id == MARKET_HL_PERP {
        return !order.coin.contains(':') && !order.coin.contains('/');
    }
    let prefix = format!("{}:", market.dex.trim().to_ascii_lowercase());
    order.coin.trim().to_ascii_lowercase().starts_with(&prefix)
}

fn open_order_native_protective_kind(order: &OpenOrder) -> Option<&'static str> {
    if !order.is_trigger {
        return None;
    }
    let order_type = order.order_type.trim().to_ascii_lowercase();
    let trigger_condition = order.trigger_condition.trim().to_ascii_lowercase();
    if order_type.contains("take") || trigger_condition.contains("tp") {
        return Some("take_profit");
    }
    if order_type.contains("stop") || trigger_condition.contains("sl") {
        return Some("stop_loss");
    }
    None
}

fn open_order_side(order: &OpenOrder) -> Option<OrderSide> {
    match order.side.trim().to_ascii_uppercase().as_str() {
        "A" | "ASK" | "SELL" => Some(OrderSide::Sell),
        "B" | "BID" | "BUY" => Some(OrderSide::Buy),
        _ => None,
    }
}

fn order_status_native_protective_kind(status: &OrderStatusResponse) -> Option<&'static str> {
    let order = status.order.as_ref()?.order.clone();
    if !order.is_trigger {
        return None;
    }
    let order_type = order.order_type.trim().to_ascii_lowercase();
    let trigger_condition = order.trigger_condition.trim().to_ascii_lowercase();
    if order_type.contains("take")
        || trigger_condition.contains("above")
        || trigger_condition.contains("tp")
    {
        return Some("take_profit");
    }
    if order_type.contains("stop")
        || trigger_condition.contains("below")
        || trigger_condition.contains("sl")
    {
        return Some("stop_loss");
    }
    None
}

async fn load_manual_protective_rule_views_for_account(
    config: &AppConfig,
    realtime: &RealtimeState,
    market: &MarketProfile,
    account_id: &str,
    address: &str,
) -> Result<Vec<ManualProtectiveRuleView>> {
    let open_orders = if let Some(orders) = realtime.open_orders(market.id, address) {
        orders
    } else {
        fetch_open_orders(&config.app.environment, &market.dex, address).await?
    };
    Ok(manual_protective_rule_views_from_open_orders(
        config,
        market,
        account_id,
        address,
        open_orders,
    )
    .await)
}

async fn manual_protective_rule_views_from_open_orders(
    config: &AppConfig,
    market: &MarketProfile,
    account_id: &str,
    address: &str,
    open_orders: Vec<OpenOrder>,
) -> Vec<ManualProtectiveRuleView> {
    let mut views = HashMap::<String, ManualProtectiveRuleView>::new();

    for order in open_orders {
        if !open_order_matches_market(&order, market) {
            continue;
        }
        if !order.reduce_only {
            continue;
        }
        let mut trigger_price = order.trigger_px.parse::<f64>().ok();
        let mut limit_price = order.limit_px.parse::<f64>().unwrap_or_default();
        let mut size = order
            .orig_sz
            .parse::<f64>()
            .ok()
            .unwrap_or_else(|| order.sz.parse::<f64>().unwrap_or_default());
        let mut updated_at_ms = order.timestamp;
        let mut exit_side = open_order_side(&order).unwrap_or(OrderSide::Sell);

        let kind = if let Some(kind) = open_order_native_protective_kind(&order) {
            kind
        } else if let Ok(status) =
            fetch_order_status_by_oid(&config.app.environment, address, order.oid).await
        {
            let Some(kind) = order_status_native_protective_kind(&status) else {
                continue;
            };
            if let Some(order_info) = status.order.as_ref().map(|entry| &entry.order) {
                trigger_price = order_info.trigger_px.parse::<f64>().ok().or(trigger_price);
                limit_price = order_info
                    .limit_px
                    .parse::<f64>()
                    .ok()
                    .unwrap_or(limit_price);
                size = order_info
                    .orig_sz
                    .parse::<f64>()
                    .ok()
                    .unwrap_or_else(|| order_info.sz.parse::<f64>().unwrap_or(size));
                updated_at_ms = order_info.timestamp.max(updated_at_ms);
                exit_side = match order_info.side.trim().to_ascii_uppercase().as_str() {
                    "A" | "ASK" | "SELL" => OrderSide::Sell,
                    "B" | "BID" | "BUY" => OrderSide::Buy,
                    _ => exit_side,
                };
            }
            kind
        } else {
            continue;
        };

        let coin = normalize_coin_for_market(market, &order.coin);
        let key = format!(
            "{}::{}",
            account_id.trim().to_ascii_lowercase(),
            coin.to_ascii_lowercase()
        );
        let entry_side = match exit_side {
            OrderSide::Buy => "sell",
            OrderSide::Sell => "buy",
        };

        let view = views
            .entry(key)
            .or_insert_with(|| ManualProtectiveRuleView {
                rule_id: format!("native:{}:{}", account_id, coin),
                account_id: account_id.to_string(),
                coin: coin.clone(),
                entry_side: entry_side.to_string(),
                entry_price: None,
                notional_usd: (size * limit_price).abs(),
                take_profit_usd: 0.0,
                stop_loss_pct: 0.0,
                take_profit_trigger_price: None,
                stop_loss_trigger_price: None,
                enabled: true,
                created_at_ms: updated_at_ms,
                updated_at_ms,
                trigger_count: 0,
                last_checked_at_ms: Some(updated_at_ms),
                last_observed_price: None,
                last_triggered_at_ms: None,
                last_trigger_kind: None,
                last_submit_status: Some("exchange_native_armed".to_string()),
                last_error: None,
                retry_after_ms: None,
            });

        view.updated_at_ms = view.updated_at_ms.max(updated_at_ms);
        view.last_checked_at_ms = Some(
            view.last_checked_at_ms
                .unwrap_or(updated_at_ms)
                .max(updated_at_ms),
        );
        if view.notional_usd <= 0.0 {
            view.notional_usd = (size * limit_price).abs();
        }
        if kind == "take_profit" {
            view.take_profit_trigger_price = trigger_price;
        } else {
            view.stop_loss_trigger_price = trigger_price;
        }
        if order.is_position_tpsl {
            view.last_submit_status = Some("exchange_native_armed".to_string());
        }
    }

    views.into_values().collect()
}

#[derive(Debug, Clone)]
struct MarketProfile {
    id: &'static str,
    label: &'static str,
    dex: String,
    live_trading_supported: bool,
}

impl MarketProfile {
    fn is_spot(&self) -> bool {
        self.id == MARKET_SPOT
    }

    fn dex_display(&self) -> String {
        if self.dex.trim().is_empty() {
            "default".to_string()
        } else {
            self.dex.clone()
        }
    }
}

fn resolve_market_profile(raw_market: Option<&str>, config: &AppConfig) -> Result<MarketProfile> {
    let requested = raw_market.unwrap_or(MARKET_XYZ_PERP);
    let market_id = normalize_market_id(requested)
        .ok_or_else(|| anyhow::anyhow!("unknown market: {requested}"))?;
    let profile = match market_id {
        MARKET_XYZ_PERP => MarketProfile {
            id: MARKET_XYZ_PERP,
            label: "XYZ Perps",
            dex: {
                let dex = config.hyperliquid.dex.trim().to_ascii_lowercase();
                if dex.is_empty() {
                    "xyz".to_string()
                } else {
                    dex
                }
            },
            live_trading_supported: true,
        },
        MARKET_HL_PERP => MarketProfile {
            id: MARKET_HL_PERP,
            label: "HL Perps",
            dex: String::new(),
            live_trading_supported: true,
        },
        MARKET_SPOT => MarketProfile {
            id: MARKET_SPOT,
            label: "Spot",
            dex: "spot".to_string(),
            live_trading_supported: true,
        },
        _ => anyhow::bail!("unsupported market: {market_id}"),
    };
    Ok(profile)
}

fn parse_side(raw: &str) -> Result<OrderSide> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "buy" | "long" => Ok(OrderSide::Buy),
        "sell" | "short" => Ok(OrderSide::Sell),
        _ => anyhow::bail!("invalid side: {raw}"),
    }
}

fn normalize_module_name(raw: &str) -> Result<&'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "manual" | "manual_ops" => Ok("manual"),
        "fib" | "fib_retracement" => Ok("fib"),
        "copy" | "smart_money" | "smart_money_copy" => Ok("copy"),
        _ => anyhow::bail!("unknown module: {raw}"),
    }
}

fn normalize_optional_module_name(raw: Option<&str>) -> Result<&'static str> {
    match raw {
        Some(value) if !value.trim().is_empty() => normalize_module_name(value),
        _ => Ok("manual"),
    }
}

fn scoped_config_for_module_and_market(
    mut config: AppConfig,
    module: &str,
    market: &MarketProfile,
) -> AppConfig {
    config.manual_ops.blocked_symbols = config.module_blocked_symbols(module).to_vec();
    config.hyperliquid.dex = market.dex.clone();
    config
}

fn normalize_coin_for_market(market: &MarketProfile, coin: &str) -> String {
    if market.is_spot() {
        normalize_spot_coin(coin)
    } else {
        normalize_dex_coin(&market.dex, coin)
    }
}

fn build_signed_spot_order_plan(
    snapshot: &SpotMarketSnapshot,
    coin: &str,
    side: OrderSide,
    notional_usd: f64,
    max_slippage_bps: f64,
    execution_mode: ExecutionMode,
) -> Result<OrderPlan> {
    match execution_mode {
        ExecutionMode::Taker => build_spot_order_plan(
            snapshot,
            coin,
            matches!(side, OrderSide::Buy),
            notional_usd,
            None,
            max_slippage_bps,
        ),
        ExecutionMode::Maker => {
            build_spot_maker_order_plan(snapshot, coin, side, notional_usd, max_slippage_bps)
        }
    }
}

fn build_spot_maker_order_plan(
    snapshot: &SpotMarketSnapshot,
    coin: &str,
    side: OrderSide,
    notional_usd: f64,
    max_slippage_bps: f64,
) -> Result<OrderPlan> {
    anyhow::ensure!(notional_usd > 0.0, "notional_usd must be positive");
    anyhow::ensure!(
        max_slippage_bps >= 0.0,
        "max_slippage_bps cannot be negative"
    );
    let asset = snapshot.asset(coin)?;
    let reference_price = asset
        .context
        .mid_px
        .as_deref()
        .and_then(|value| value.parse::<f64>().ok())
        .or_else(|| asset.context.mark_px.parse::<f64>().ok())
        .or_else(|| asset.context.prev_day_px.parse::<f64>().ok())
        .with_context(|| format!("reference price for {} is unavailable", asset.coin))?;
    anyhow::ensure!(
        reference_price.is_finite() && reference_price > 0.0,
        "reference price for {} must be positive",
        asset.coin
    );
    let offset = max_slippage_bps / 10_000.0;
    let limit_factor = match side {
        OrderSide::Buy => 1.0 - offset,
        OrderSide::Sell => 1.0 + offset,
    };
    anyhow::ensure!(
        limit_factor > 0.0,
        "maker offset produced non-positive price"
    );
    let limit_price = round_spot_price(reference_price * limit_factor, asset.sz_decimals);
    let size = round_size_down(notional_usd / reference_price, asset.sz_decimals);
    anyhow::ensure!(
        size > 0.0,
        "order size rounds to zero for {} at notional {} and price {}",
        asset.coin,
        notional_usd,
        reference_price
    );

    Ok(OrderPlan {
        coin: asset.coin,
        asset_id: asset.asset_id,
        sz_decimals: asset.sz_decimals,
        reference_price,
        limit_price,
        size,
    })
}

fn reduce_only_spot_position_available(side: OrderSide, base_available: f64) -> bool {
    matches!(side, OrderSide::Sell) && base_available > 0.0
}

fn reduce_only_spot_position_detail(side: OrderSide, base_available: f64) -> String {
    match side {
        OrderSide::Sell => format!(
            "spot protective close requires available base inventory; current base available={base_available}"
        ),
        OrderSide::Buy => {
            "spot protective buy is unsupported because spot has no short position state"
                .to_string()
        }
    }
}

fn spot_account_readiness_state(
    state: &SpotClearinghouseState,
    coin: &str,
) -> AccountReadinessState {
    let available_usdc = spot_available_balance(state, "USDC");
    let base_available = spot_base_available_for_coin(state, coin);
    AccountReadinessState {
        account_value_usd: available_usdc,
        withdrawable_usd: available_usdc,
        total_notional_position_usd: 0.0,
        total_margin_used_usd: 0.0,
        coin_position_size: base_available,
        coin_position_value_usd: 0.0,
        coin_unrealized_pnl_usd: 0.0,
    }
}

fn parse_state_decimal(value: &str) -> f64 {
    value.parse::<f64>().unwrap_or_default()
}

fn spot_available_balance(state: &SpotClearinghouseState, token: &str) -> f64 {
    state
        .balances
        .iter()
        .filter(|balance| balance.coin.eq_ignore_ascii_case(token))
        .map(|balance| {
            let total = parse_state_decimal(&balance.total);
            let hold = parse_state_decimal(&balance.hold);
            (total - hold).max(0.0)
        })
        .sum::<f64>()
}

fn spot_base_available_for_coin(state: &SpotClearinghouseState, coin: &str) -> f64 {
    let base = normalize_spot_coin(coin)
        .split_once('/')
        .map(|(base, _)| base.to_string());
    let Some(base_token) = base else {
        return 0.0;
    };
    spot_available_balance(state, &base_token)
}

fn parse_execution_mode(raw: &str) -> Result<ExecutionMode> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "maker" | "alo" | "post_only_alo" | "limit_post_only_alo" => Ok(ExecutionMode::Maker),
        "taker" | "ioc" | "market" | "market_ioc" => Ok(ExecutionMode::Taker),
        _ => anyhow::bail!("invalid execution mode: {raw}"),
    }
}

fn default_true() -> bool {
    true
}

fn default_taker() -> String {
    "taker".to_string()
}

fn default_isolated() -> String {
    "isolated".to_string()
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc};

    use crate::{
        audit::AuditEvent,
        config::{AppConfig, MARKET_HL_PERP, MARKET_SPOT, MARKET_XYZ_PERP},
        domain::{ExecutionMode, OrderSide, WorkerReport},
        hyperliquid::UserFill,
        realtime::RealtimeState,
        secrets::{SecretUpsert, VaultEntrySummary, VaultSummary},
    };

    use super::{
        DashboardOpenOrderResponse, FibBasicConfig, FibBasicLevelPlan, FibBasicPlan,
        FibInstanceRecord, FibInstanceStatus, FibOrderRef, FibPositionSnapshot, FibProfitLossMode,
        FibTradeDirection, FrontendAppState, ManualSettingsPayload, OrderStatusPayload,
        OrderStatusQuery, PerpFundingLayer, ReadinessCheckResponse, SecretUsage, SpotFundingLayer,
        account_funding_next_actions, account_funding_summary, build_basic_plan,
        build_fib_strategy_pnl_summary, dashboard_cancel_stopped_without_open_orders_count,
        dashboard_cancel_strategy_ids, dashboard_order_is_cancelable_fib_entry,
        ensure_fib_pair_available, failed_readiness_blockers,
        fib_all_target_accounts_have_complete_protection, fib_background_stop_requested,
        fib_coordinator_signals_from_plan, fib_entry_sync_assessment,
        fib_position_is_effectively_flat, fib_position_matches_direction,
        fib_restart_cooldown_secs, fib_should_stop_after_cycle, fib_waiting_for_entry_message,
        fill_event_type, live_readiness_next_actions, live_readiness_summary,
        mark_fib_record_auto_loop_retry_wait, mark_fib_record_incomplete_entry_submission,
        normalize_recovered_fib_instance, selected_enabled_account_ids, trade_record_event_type,
        transfer_amount_label, usdc_transfer_readiness_next_actions,
        usdc_transfer_readiness_summary, validate_batch_account_count,
    };

    #[test]
    fn vault_session_survives_frontend_refresh_state_checks() {
        let dir = std::env::temp_dir().join(format!(
            "trade_xyz_frontend_vault_session_{}",
            crate::domain::now_ms()
        ));
        fs::create_dir_all(&dir).expect("test dir");
        let path = dir.join("trade_xyz.vault");
        fs::write(&path, b"encrypted-placeholder").expect("vault placeholder");

        let state = FrontendAppState {
            config: Arc::new(std::sync::RwLock::new(AppConfig::default())),
            config_path: dir.join("dry-run.toml"),
            dry_run: true,
            started_at_ms: crate::domain::now_ms(),
            vault_session: Arc::new(std::sync::RwLock::new(None)),
            fib_instances: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            fib_stop_requests: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            realtime: RealtimeState::new(),
            account_funding_batch_cache: Arc::new(std::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
            manual_protective_rules_cache: Arc::new(std::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
        };
        let summary = VaultSummary {
            exists: true,
            unlocked: true,
            path: path.display().to_string(),
            entry_count: Some(1),
            entries: Vec::new(),
        };

        state
            .store_vault_session(path.clone(), "persistent password".to_string(), summary)
            .expect("store session");

        let status = state.vault_summary(&path).expect("vault status");
        assert!(status.unlocked);
        assert_eq!(status.entry_count, Some(1));

        let password = state
            .resolve_vault_password(&path, "")
            .expect("session password");
        assert_eq!(password, "persistent password");
        assert_eq!(
            state
                .unlocked_vault_password(&path)
                .expect("optional session password"),
            Some("persistent password".to_string())
        );
        assert_eq!(
            state
                .unlocked_vault_password(&dir.join("other.vault"))
                .expect("missing optional session password"),
            None
        );
    }

    #[test]
    fn selected_enabled_account_ids_defaults_to_enabled_workers_and_dedupes() {
        let mut config = AppConfig::default();
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
                copy_ratio: 0.1,
                max_order_notional_usd: 1.0,
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
                worker_enabled: false,
                copy_ratio: 0.1,
                max_order_notional_usd: 1.0,
                blocked_markets: Vec::new(),
            },
        ];

        assert_eq!(
            selected_enabled_account_ids(&config, &[]),
            vec!["addr_a".to_string()]
        );
        assert_eq!(
            selected_enabled_account_ids(
                &config,
                &[
                    " addr_b ".to_string(),
                    "addr_b".to_string(),
                    String::new(),
                    "addr_a".to_string(),
                ],
            ),
            vec!["addr_b".to_string(), "addr_a".to_string()]
        );
    }

    #[test]
    fn validate_batch_account_count_uses_manual_batch_limit() {
        let mut config = AppConfig::default();
        config.manual_ops.max_manual_batch_accounts = 2;

        validate_batch_account_count(
            "funding batch",
            &config,
            &["addr_a".to_string(), "addr_b".to_string()],
        )
        .expect("two accounts within limit");

        let error = validate_batch_account_count(
            "funding batch",
            &config,
            &[
                "addr_a".to_string(),
                "addr_b".to_string(),
                "addr_c".to_string(),
            ],
        )
        .expect_err("third account exceeds configured batch limit");
        assert!(error.to_string().contains("max_manual_batch_accounts"));
    }

    #[test]
    fn fib_per_level_notional_must_meet_exchange_minimum() {
        let mut plan = FibBasicPlan {
            strategy_id: "fib-test".to_string(),
            direction: FibTradeDirection::Long,
            market: "spot".to_string(),
            coin: "HYPE/USDC".to_string(),
            timeframe: "1h".to_string(),
            swing_high: 100.0,
            swing_low: 80.0,
            current_price: 90.0,
            line_version: "line".to_string(),
            levels: vec![FibBasicLevelPlan {
                level: 0.382,
                entry_price: 90.0,
                entry_zone_high: 91.0,
                entry_zone_low: 89.0,
                take_profit_price: 95.0,
                stop_loss_price: 85.0,
                take_profit_return_pct: 5.0,
                stop_loss_return_pct: 5.0,
                current_distance_usd: 0.0,
                current_within_zone: true,
                order_notional_usd: 9.99,
            }],
        };
        let error = super::validate_fib_per_level_opening_notional(&plan, 3)
            .expect_err("sub-minimum fib level should be rejected");
        assert!(error.to_string().contains("per-level opening notional"));

        plan.levels[0].entry_price = 66_848.5;
        plan.levels[0].order_notional_usd = 10.0;
        let error = super::validate_fib_per_level_opening_notional(&plan, 5)
            .expect_err("precision-rounded fib level should be rejected");
        assert!(error.to_string().contains("after size rounding"));

        plan.levels[0].order_notional_usd = 11.0;
        super::validate_fib_per_level_opening_notional(&plan, 5)
            .expect("buffered fib level should pass after precision rounding");
    }

    #[test]
    fn fib_per_level_notional_must_fit_each_selected_account_cap() {
        let mut config = AppConfig::default();
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
                copy_ratio: 0.1,
                max_order_notional_usd: 12.0,
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
                copy_ratio: 0.1,
                max_order_notional_usd: 12.0,
                blocked_markets: Vec::new(),
            },
        ];
        let mut plan = FibBasicPlan {
            strategy_id: "fib-test".to_string(),
            direction: FibTradeDirection::Short,
            market: MARKET_XYZ_PERP.to_string(),
            coin: "xyz:TSLA".to_string(),
            timeframe: "1h".to_string(),
            swing_high: 433.26,
            swing_low: 384.28,
            current_price: 402.9,
            line_version: "line".to_string(),
            levels: vec![FibBasicLevelPlan {
                level: 0.382,
                entry_price: 402.99036,
                entry_zone_high: 403.09,
                entry_zone_low: 402.89,
                take_profit_price: 401.78,
                stop_loss_price: 404.19,
                take_profit_return_pct: 3.0,
                stop_loss_return_pct: 3.0,
                current_distance_usd: 0.0,
                current_within_zone: true,
                order_notional_usd: 12.5,
            }],
        };

        let error = super::validate_fib_per_level_account_order_caps(
            &config,
            &["addr_a".to_string(), "addr_b".to_string()],
            &plan,
        )
        .expect_err("per-level notional above account cap should be rejected");
        assert!(error.to_string().contains("max_order_notional_usd"));

        plan.levels[0].order_notional_usd = 12.0;
        super::validate_fib_per_level_account_order_caps(
            &config,
            &["addr_a".to_string(), "addr_b".to_string()],
            &plan,
        )
        .expect("per-level notional equal to account cap should pass");
    }

    #[test]
    fn fib_completion_treats_sub_half_usd_residual_as_flat() {
        assert!(fib_position_is_effectively_flat(FibPositionSnapshot {
            size: 0.0001,
            value_usd: 0.18647,
            entry_price: None,
        }));
        assert!(!fib_position_is_effectively_flat(FibPositionSnapshot {
            size: 0.001,
            value_usd: 2.0,
            entry_price: None,
        }));
    }

    #[test]
    fn fib_maker_signal_skips_when_price_already_below_entry() {
        let mut config = FibBasicConfig {
            strategy_id: "fib-test".to_string(),
            direction: FibTradeDirection::Long,
            market: MARKET_HL_PERP.to_string(),
            dex: String::new(),
            account_ids: vec!["addr_a".to_string()],
            coin: "ETH".to_string(),
            timeframe: "5m".to_string(),
            lookback_bars: 30,
            swing_high: 100.0,
            swing_low: 80.0,
            current_price: 89.0,
            levels: vec![0.5],
            entry_above_tolerance_usd: 0.1,
            entry_below_tolerance_usd: 0.1,
            principal_usd: 1.0,
            leverage: 10.0,
            execution_mode: ExecutionMode::Maker,
            take_profit_mode: FibProfitLossMode::PrincipalPercent,
            take_profit_value: 5.0,
            stop_loss_mode: FibProfitLossMode::PrincipalPercent,
            stop_loss_value: 5.0,
            max_slippage_bps: 20.0,
            max_entries_per_level: 1,
            cooldown_secs: 300,
            stop_loss_cooldown_secs: 900,
            stop_loss_stop_strategy: false,
            locked_range: false,
            auto_loop: true,
        };
        let mut plan = build_basic_plan(&config).expect("plan");
        assert_eq!(
            fib_coordinator_signals_from_plan(&config, &plan)
                .expect("signals")
                .len(),
            0
        );

        config.current_price = 91.0;
        plan = build_basic_plan(&config).expect("plan after price recovery");
        assert_eq!(
            fib_coordinator_signals_from_plan(&config, &plan)
                .expect("signals after price recovery")
                .len(),
            1
        );
    }

    #[test]
    fn fib_short_maker_signal_skips_when_price_already_above_entry() {
        let mut config = FibBasicConfig {
            strategy_id: "fib-short-test".to_string(),
            direction: FibTradeDirection::Short,
            market: MARKET_HL_PERP.to_string(),
            dex: String::new(),
            account_ids: vec!["addr_a".to_string()],
            coin: "ETH".to_string(),
            timeframe: "5m".to_string(),
            lookback_bars: 30,
            swing_high: 100.0,
            swing_low: 80.0,
            current_price: 91.0,
            levels: vec![0.5],
            entry_above_tolerance_usd: 0.1,
            entry_below_tolerance_usd: 0.1,
            principal_usd: 1.0,
            leverage: 10.0,
            execution_mode: ExecutionMode::Maker,
            take_profit_mode: FibProfitLossMode::PrincipalPercent,
            take_profit_value: 5.0,
            stop_loss_mode: FibProfitLossMode::PrincipalPercent,
            stop_loss_value: 5.0,
            max_slippage_bps: 20.0,
            max_entries_per_level: 1,
            cooldown_secs: 300,
            stop_loss_cooldown_secs: 900,
            stop_loss_stop_strategy: false,
            locked_range: false,
            auto_loop: true,
        };
        let mut plan = build_basic_plan(&config).expect("short plan");
        assert_eq!(
            fib_coordinator_signals_from_plan(&config, &plan)
                .expect("signals")
                .len(),
            0
        );

        config.current_price = 89.0;
        plan = build_basic_plan(&config).expect("short plan after price recovery");
        let signals =
            fib_coordinator_signals_from_plan(&config, &plan).expect("signals after recovery");
        assert_eq!(signals.len(), 1);
        assert!(matches!(signals[0].order.side, OrderSide::Sell));
        assert_eq!(signals[0].order.limit_price, Some(90.0));
    }

    #[test]
    fn fib_taker_signal_uses_slippage_guard_instead_of_fixed_fib_price() {
        let config = FibBasicConfig {
            strategy_id: "fib-taker-test".to_string(),
            direction: FibTradeDirection::Long,
            market: MARKET_HL_PERP.to_string(),
            dex: String::new(),
            account_ids: vec!["addr_a".to_string()],
            coin: "ETH".to_string(),
            timeframe: "5m".to_string(),
            lookback_bars: 30,
            swing_high: 100.0,
            swing_low: 80.0,
            current_price: 90.05,
            levels: vec![0.5],
            entry_above_tolerance_usd: 0.1,
            entry_below_tolerance_usd: 0.1,
            principal_usd: 1.1,
            leverage: 10.0,
            execution_mode: ExecutionMode::Taker,
            take_profit_mode: FibProfitLossMode::PrincipalPercent,
            take_profit_value: 5.0,
            stop_loss_mode: FibProfitLossMode::PrincipalPercent,
            stop_loss_value: 5.0,
            max_slippage_bps: 20.0,
            max_entries_per_level: 1,
            cooldown_secs: 300,
            stop_loss_cooldown_secs: 900,
            stop_loss_stop_strategy: false,
            locked_range: false,
            auto_loop: true,
        };
        let plan = build_basic_plan(&config).expect("plan");
        let signals = fib_coordinator_signals_from_plan(&config, &plan).expect("signals");

        assert_eq!(signals.len(), 1);
        assert!(matches!(
            signals[0].order.execution_mode,
            ExecutionMode::Taker
        ));
        assert_eq!(signals[0].order.limit_price, None);
    }

    #[test]
    fn fib_entry_signal_notional_ignores_account_copy_ratio() {
        let config = FibBasicConfig {
            strategy_id: "fib-ratio-test".to_string(),
            direction: FibTradeDirection::Long,
            market: MARKET_HL_PERP.to_string(),
            dex: String::new(),
            account_ids: vec!["addr_a".to_string()],
            coin: "ETH".to_string(),
            timeframe: "5m".to_string(),
            lookback_bars: 30,
            swing_high: 100.0,
            swing_low: 80.0,
            current_price: 90.05,
            levels: vec![0.5],
            entry_above_tolerance_usd: 0.1,
            entry_below_tolerance_usd: 0.1,
            principal_usd: 1.1,
            leverage: 10.0,
            execution_mode: ExecutionMode::Taker,
            take_profit_mode: FibProfitLossMode::PrincipalPercent,
            take_profit_value: 5.0,
            stop_loss_mode: FibProfitLossMode::PrincipalPercent,
            stop_loss_value: 5.0,
            max_slippage_bps: 20.0,
            max_entries_per_level: 1,
            cooldown_secs: 300,
            stop_loss_cooldown_secs: 900,
            stop_loss_stop_strategy: false,
            locked_range: false,
            auto_loop: true,
        };
        let plan = build_basic_plan(&config).expect("plan");
        let signals = fib_coordinator_signals_from_plan(&config, &plan).expect("signals");
        let intent = signals[0].to_trade_intent("addr_a", "worker-addr_a", 0.05);

        assert!(!signals[0].order.apply_account_ratio);
        assert!((intent.sizing.notional_usd - 11.0).abs() < 0.0001);
    }

    #[test]
    fn fib_short_taker_signal_sells_without_fixed_fib_price() {
        let config = FibBasicConfig {
            strategy_id: "fib-short-taker-test".to_string(),
            direction: FibTradeDirection::Short,
            market: MARKET_HL_PERP.to_string(),
            dex: String::new(),
            account_ids: vec!["addr_a".to_string()],
            coin: "ETH".to_string(),
            timeframe: "5m".to_string(),
            lookback_bars: 30,
            swing_high: 100.0,
            swing_low: 80.0,
            current_price: 90.05,
            levels: vec![0.5],
            entry_above_tolerance_usd: 0.1,
            entry_below_tolerance_usd: 0.1,
            principal_usd: 1.1,
            leverage: 10.0,
            execution_mode: ExecutionMode::Taker,
            take_profit_mode: FibProfitLossMode::PrincipalPercent,
            take_profit_value: 5.0,
            stop_loss_mode: FibProfitLossMode::PrincipalPercent,
            stop_loss_value: 5.0,
            max_slippage_bps: 20.0,
            max_entries_per_level: 1,
            cooldown_secs: 300,
            stop_loss_cooldown_secs: 900,
            stop_loss_stop_strategy: false,
            locked_range: false,
            auto_loop: true,
        };
        let plan = build_basic_plan(&config).expect("short taker plan");
        let signals = fib_coordinator_signals_from_plan(&config, &plan).expect("signals");

        assert_eq!(signals.len(), 1);
        assert!(matches!(signals[0].order.side, OrderSide::Sell));
        assert!(matches!(
            signals[0].order.execution_mode,
            ExecutionMode::Taker
        ));
        assert_eq!(signals[0].order.limit_price, None);
    }

    #[test]
    fn fib_position_direction_match_handles_short_positions() {
        let long_position = FibPositionSnapshot {
            size: 0.01,
            value_usd: 20.0,
            entry_price: Some(100.0),
        };
        let short_position = FibPositionSnapshot {
            size: -0.01,
            value_usd: 20.0,
            entry_price: Some(100.0),
        };
        assert!(fib_position_matches_direction(
            FibTradeDirection::Long,
            long_position
        ));
        assert!(!fib_position_matches_direction(
            FibTradeDirection::Long,
            short_position
        ));
        assert!(fib_position_matches_direction(
            FibTradeDirection::Short,
            short_position
        ));
        assert!(!fib_position_matches_direction(
            FibTradeDirection::Short,
            long_position
        ));
    }

    #[test]
    fn fib_payload_from_record_preserves_dry_run_live_mode() {
        let mut record = test_fib_record(
            "fib-mode",
            MARKET_HL_PERP,
            "ETH",
            FibInstanceStatus::Completed,
            true,
        );
        record.dry_run = true;
        record.live = false;
        let payload = super::fib_payload_from_record(&record);
        assert!(payload.dry_run);
        assert!(!payload.live);
        assert!(!super::fib_record_allows_background_live_actions(&record));

        record.dry_run = false;
        record.live = true;
        let payload = super::fib_payload_from_record(&record);
        assert!(!payload.dry_run);
        assert!(payload.live);
        assert!(super::fib_record_allows_background_live_actions(&record));
    }

    fn test_fib_record(
        strategy_id: &str,
        market: &str,
        coin: &str,
        status: FibInstanceStatus,
        auto_loop: bool,
    ) -> FibInstanceRecord {
        let config = FibBasicConfig {
            strategy_id: strategy_id.to_string(),
            direction: FibTradeDirection::Long,
            market: market.to_string(),
            dex: String::new(),
            account_ids: vec!["addr_a".to_string()],
            coin: coin.to_string(),
            timeframe: "5m".to_string(),
            lookback_bars: 30,
            swing_high: 100.0,
            swing_low: 80.0,
            current_price: 91.0,
            levels: vec![0.5],
            entry_above_tolerance_usd: 0.1,
            entry_below_tolerance_usd: 0.1,
            principal_usd: 1.1,
            leverage: 10.0,
            execution_mode: ExecutionMode::Maker,
            take_profit_mode: FibProfitLossMode::PrincipalPercent,
            take_profit_value: 5.0,
            stop_loss_mode: FibProfitLossMode::PrincipalPercent,
            stop_loss_value: 5.0,
            max_slippage_bps: 20.0,
            max_entries_per_level: 1,
            cooldown_secs: 300,
            stop_loss_cooldown_secs: 900,
            stop_loss_stop_strategy: false,
            locked_range: false,
            auto_loop,
        };
        let plan = build_basic_plan(&config).expect("fib plan");
        FibInstanceRecord {
            strategy_id: strategy_id.to_string(),
            status,
            config,
            plan,
            dry_run: true,
            live: false,
            entry_signal_ids: Vec::new(),
            entry_order_refs: Vec::new(),
            protective_order_refs: Vec::new(),
            last_message: None,
            created_at_ms: 1,
            updated_at_ms: 1,
            completed_cycles: 0,
            last_cycle_completed_at_ms: None,
            last_cycle_exit_kind: None,
        }
    }

    fn test_frontend_state_with_fib_records(records: Vec<FibInstanceRecord>) -> FrontendAppState {
        let instances = records
            .into_iter()
            .map(|record| (record.strategy_id.clone(), record))
            .collect::<std::collections::HashMap<_, _>>();
        FrontendAppState {
            config: Arc::new(std::sync::RwLock::new(AppConfig::default())),
            config_path: std::env::temp_dir().join("trade_xyz_fib_conflict_test.toml"),
            dry_run: true,
            started_at_ms: crate::domain::now_ms(),
            vault_session: Arc::new(std::sync::RwLock::new(None)),
            fib_instances: Arc::new(std::sync::RwLock::new(instances)),
            fib_stop_requests: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            realtime: RealtimeState::new(),
            account_funding_batch_cache: Arc::new(std::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
            manual_protective_rules_cache: Arc::new(std::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn test_user_fill(
        oid: u64,
        coin: &str,
        side: &str,
        price: f64,
        size: f64,
        time: u64,
        closed_pnl: f64,
        fee: f64,
    ) -> UserFill {
        UserFill {
            coin: coin.to_string(),
            px: price.to_string(),
            sz: size.to_string(),
            side: side.to_string(),
            time,
            dir: if closed_pnl.abs() > f64::EPSILON {
                "Close Long".to_string()
            } else {
                "Open Long".to_string()
            },
            closed_pnl: closed_pnl.to_string(),
            hash: format!("hash-{oid}"),
            oid,
            crossed: true,
            fee: fee.to_string(),
        }
    }

    #[test]
    fn fill_event_type_uses_position_lifecycle_for_perps() {
        let mut open_short = test_user_fill(1, "BTC", "A", 100.0, 0.1, 1, 0.0, 0.0);
        open_short.dir = "Open Short".to_string();
        open_short.crossed = true;
        assert_eq!(fill_event_type(MARKET_HL_PERP, &open_short), "开空成交");

        let mut open_long = test_user_fill(2, "ETH", "B", 100.0, 0.1, 2, 0.0, 0.0);
        open_long.dir = "Open Long".to_string();
        open_long.crossed = false;
        assert_eq!(fill_event_type(MARKET_HL_PERP, &open_long), "开多成交");

        let mut close_short = test_user_fill(3, "BTC", "B", 100.0, 0.1, 3, 0.04, 0.0);
        close_short.dir = "Close Short".to_string();
        assert_eq!(
            fill_event_type(MARKET_HL_PERP, &close_short),
            "盈利平空成交"
        );

        let mut close_long = test_user_fill(4, "ETH", "A", 100.0, 0.1, 4, -0.03, 0.0);
        close_long.dir = "Close Long".to_string();
        assert_eq!(fill_event_type(MARKET_HL_PERP, &close_long), "亏损平多成交");
    }

    #[test]
    fn fill_event_type_keeps_spot_as_buy_sell() {
        let mut spot_sell = test_user_fill(5, "HYPE/USDC", "A", 10.0, 1.0, 5, 0.0, 0.0);
        spot_sell.dir = "Sell".to_string();
        spot_sell.crossed = true;
        assert_eq!(fill_event_type(MARKET_SPOT, &spot_sell), "卖出成交");
    }

    #[test]
    fn trade_record_event_type_uses_contract_lifecycle_for_perp_actions() {
        let open_short = AuditEvent {
            event_id: "open-short".to_string(),
            occurred_at_ms: 1,
            source: "frontend".to_string(),
            action: "signed_runbook_submit".to_string(),
            ok: true,
            account_id: Some("addr_a".to_string()),
            coin: Some("BTC".to_string()),
            error: None,
            details: serde_json::json!({
                "market": MARKET_HL_PERP,
                "source_module": "manual",
                "side": "sell",
                "reduce_only": false,
                "execution_mode": "taker"
            }),
        };
        assert_eq!(trade_record_event_type(&open_short), "手动开空");

        let close_short = AuditEvent {
            event_id: "close-short".to_string(),
            occurred_at_ms: 2,
            source: "frontend".to_string(),
            action: "signed_runbook_submit".to_string(),
            ok: true,
            account_id: Some("addr_a".to_string()),
            coin: Some("BTC".to_string()),
            error: None,
            details: serde_json::json!({
                "market": MARKET_HL_PERP,
                "source_module": "manual",
                "side": "buy",
                "reduce_only": true,
                "execution_mode": "taker"
            }),
        };
        assert_eq!(trade_record_event_type(&close_short), "手动平空");
    }

    #[test]
    fn fib_stop_request_blocks_background_auto_loop_snapshot() {
        let record = test_fib_record(
            "fib-stop-race",
            MARKET_HL_PERP,
            "BTC",
            FibInstanceStatus::Completed,
            true,
        );
        let snapshot_ms = record.updated_at_ms;
        let state = test_frontend_state_with_fib_records(vec![record.clone()]);

        assert!(!fib_background_stop_requested(&state, &record, snapshot_ms).expect("check"));
        state
            .record_fib_stop_request(&record.strategy_id)
            .expect("record stop");
        assert!(fib_background_stop_requested(&state, &record, snapshot_ms).expect("check"));
    }

    #[test]
    fn fib_pair_conflict_blocks_same_pair_but_allows_other_pairs_and_refresh() {
        let existing = test_fib_record(
            "fib_basic_hl_perp_ETH_5m",
            MARKET_HL_PERP,
            "ETH",
            FibInstanceStatus::ArmedUnfilled,
            true,
        );
        let state = test_frontend_state_with_fib_records(vec![existing]);

        let same_pair_new_strategy = test_fib_record(
            "fib_basic_hl_perp_ETH_15m",
            MARKET_HL_PERP,
            "ETH",
            FibInstanceStatus::ArmedUnfilled,
            true,
        );
        let error = ensure_fib_pair_available(&state, &same_pair_new_strategy, None)
            .expect_err("same market and coin must conflict");
        assert!(error.to_string().contains("already has active strategy"));

        let same_pair_current_refresh = test_fib_record(
            "fib_basic_hl_perp_ETH_5m",
            MARKET_HL_PERP,
            "ETH",
            FibInstanceStatus::ArmedUnfilled,
            true,
        );
        ensure_fib_pair_available(
            &state,
            &same_pair_current_refresh,
            Some("fib_basic_hl_perp_ETH_5m"),
        )
        .expect("refreshing the current strategy is allowed");

        let other_pair = test_fib_record(
            "fib_basic_hl_perp_BTC_5m",
            MARKET_HL_PERP,
            "BTC",
            FibInstanceStatus::ArmedUnfilled,
            true,
        );
        ensure_fib_pair_available(&state, &other_pair, None)
            .expect("different trading pairs can run in parallel");

        let reused_strategy_id_for_other_pair = test_fib_record(
            "fib_basic_hl_perp_ETH_5m",
            MARKET_HL_PERP,
            "BTC",
            FibInstanceStatus::ArmedUnfilled,
            true,
        );
        let error = ensure_fib_pair_available(&state, &reused_strategy_id_for_other_pair, None)
            .expect_err("active strategy ids cannot be reused for another pair");
        assert!(error.to_string().contains("strategy id"));
    }

    #[test]
    fn dashboard_fib_pnl_uses_strategy_order_oids_not_account_total_pnl() {
        let mut config = AppConfig::default();
        config.accounts = vec![crate::config::AccountConfig {
            account_id: "addr_a".to_string(),
            address: "0x0000000000000000000000000000000000000001".to_string(),
            secret_id: "addr_a_api_wallet".to_string(),
            api_wallet_env: String::new(),
            transfer_secret_id: String::new(),
            transfer_wallet_env: String::new(),
            enabled: true,
            worker_enabled: true,
            copy_ratio: 0.1,
            max_order_notional_usd: 12.0,
            blocked_markets: Vec::new(),
        }];
        let mut record = test_fib_record(
            "fib_basic_hl_perp_ETH_5m",
            MARKET_HL_PERP,
            "ETH",
            FibInstanceStatus::Completed,
            true,
        );
        record.created_at_ms = 1_000;
        record.completed_cycles = 1;
        record.last_cycle_completed_at_ms = Some(3_000);
        record.entry_order_refs = vec![FibOrderRef {
            account_id: "addr_a".to_string(),
            coin: "ETH".to_string(),
            cloid: "entry".to_string(),
            oid: Some(100),
            level: Some(0.5),
            role: Some("entry".to_string()),
            dry_run: false,
            submitted_at_ms: 1_100,
        }];
        record.protective_order_refs = vec![FibOrderRef {
            account_id: "addr_a".to_string(),
            coin: "ETH".to_string(),
            cloid: "take-profit".to_string(),
            oid: Some(101),
            level: None,
            role: Some("take_profit".to_string()),
            dry_run: false,
            submitted_at_ms: 2_000,
        }];

        let realtime = RealtimeState::new();
        realtime.update_fills(
            &config.accounts[0].address,
            MARKET_HL_PERP,
            vec![
                test_user_fill(100, "ETH", "B", 100.0, 0.1, 1_200, 0.0, 0.01),
                test_user_fill(101, "ETH", "A", 105.0, 0.1, 2_500, 0.5, 0.02),
                test_user_fill(999, "ETH", "A", 120.0, 0.1, 2_600, 99.0, 0.03),
            ],
            true,
        );

        let summary = build_fib_strategy_pnl_summary(&config, &realtime, &record, &[]);

        assert!(summary.precise);
        assert_eq!(summary.total_fill_count, 2);
        assert_eq!(summary.total_close_fill_count, 1);
        assert!((summary.total_closed_pnl_usd - 0.5).abs() < 0.000001);
        assert!((summary.total_fee_usd - 0.03).abs() < 0.000001);
        assert!((summary.total_net_pnl_usd - 0.47).abs() < 0.000001);
        assert!((summary.last_cycle_net_pnl_usd - 0.47).abs() < 0.000001);
        assert_eq!(summary.matched_order_count, 2);
    }

    #[test]
    fn dashboard_cancel_scope_only_targets_owned_fib_entries() {
        let owned_entry = DashboardOpenOrderResponse {
            market: MARKET_XYZ_PERP.to_string(),
            market_label: "XYZ".to_string(),
            account_id: "addr_a".to_string(),
            coin: "xyz:NVDA".to_string(),
            source_module: "fib".to_string(),
            strategy_id: Some("fib-owned".to_string()),
            strategy_status: Some("entry_pending".to_string()),
            timeframe: Some("5m".to_string()),
            fib_level: Some(0.382),
            fib_line_version: None,
            swing_high: None,
            swing_low: None,
            entry_zone_high: None,
            entry_zone_low: None,
            planned_take_profit_price: None,
            planned_stop_loss_price: None,
            order_role: "entry".to_string(),
            side: "A".to_string(),
            order_type: "Limit".to_string(),
            limit_price: Some(212.0),
            trigger_price: None,
            size: Some(0.05),
            current_price: Some(205.0),
            distance_usd: Some(7.0),
            distance_pct: Some(3.4),
            cloid: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, b"owned-entry").to_string(),
            oid: Some(1),
            exchange_open: true,
            status: "waiting_fill".to_string(),
            submitted_at_ms: 1,
            updated_at_ms: 1,
        };
        let mut protective = owned_entry.clone();
        protective.order_role = "take_profit".to_string();
        protective.strategy_id = Some("fib-owned".to_string());
        protective.cloid =
            uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, b"protective").to_string();
        protective.oid = Some(2);
        let mut unowned_manual = owned_entry.clone();
        unowned_manual.source_module = "manual".to_string();
        unowned_manual.strategy_id = None;
        unowned_manual.order_role = "manual_order".to_string();
        unowned_manual.cloid =
            uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, b"manual").to_string();
        unowned_manual.oid = Some(3);

        assert!(dashboard_order_is_cancelable_fib_entry(&owned_entry));
        assert!(!dashboard_order_is_cancelable_fib_entry(&protective));
        assert!(!dashboard_order_is_cancelable_fib_entry(&unowned_manual));
    }

    fn test_dashboard_order(
        market: &str,
        source_module: &str,
        strategy_id: Option<&str>,
        order_role: &str,
        oid: u64,
    ) -> DashboardOpenOrderResponse {
        DashboardOpenOrderResponse {
            market: market.to_string(),
            market_label: market.to_string(),
            account_id: "addr_a".to_string(),
            coin: if market == MARKET_XYZ_PERP {
                "xyz:NVDA".to_string()
            } else {
                "ETH".to_string()
            },
            source_module: source_module.to_string(),
            strategy_id: strategy_id.map(str::to_string),
            strategy_status: Some("entry_pending".to_string()),
            timeframe: Some("5m".to_string()),
            fib_level: Some(0.382),
            fib_line_version: None,
            swing_high: None,
            swing_low: None,
            entry_zone_high: None,
            entry_zone_low: None,
            planned_take_profit_price: None,
            planned_stop_loss_price: None,
            order_role: order_role.to_string(),
            side: "B".to_string(),
            order_type: "Limit".to_string(),
            limit_price: Some(100.0),
            trigger_price: None,
            size: Some(0.1),
            current_price: Some(101.0),
            distance_usd: Some(1.0),
            distance_pct: Some(1.0),
            cloid: uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_OID,
                format!("{market}:{source_module}:{order_role}:{oid}").as_bytes(),
            )
            .to_string(),
            oid: Some(oid),
            exchange_open: true,
            status: "waiting_fill".to_string(),
            submitted_at_ms: oid,
            updated_at_ms: oid,
        }
    }

    #[test]
    fn dashboard_cancel_scope_covers_all_version_markets_and_active_strategies() {
        let running_without_order = test_fib_record(
            "fib_basic_hl_perp_BTC_5m",
            MARKET_HL_PERP,
            "BTC",
            FibInstanceStatus::ArmedUnfilled,
            true,
        );
        let state = test_frontend_state_with_fib_records(vec![running_without_order]);
        let visible_orders = vec![
            test_dashboard_order(MARKET_XYZ_PERP, "fib", Some("fib_xyz"), "entry", 1),
            test_dashboard_order(MARKET_HL_PERP, "fib", Some("fib_hl"), "entry", 2),
            test_dashboard_order(MARKET_HL_PERP, "fib", Some("fib_hl"), "take_profit", 3),
            test_dashboard_order(MARKET_SPOT, "manual", None, "manual_order", 4),
        ];

        let targets = visible_orders
            .iter()
            .filter(|order| dashboard_order_is_cancelable_fib_entry(order))
            .collect::<Vec<_>>();
        assert_eq!(targets.len(), 2);
        assert!(targets.iter().any(|order| order.market == MARKET_XYZ_PERP));
        assert!(targets.iter().any(|order| order.market == MARKET_HL_PERP));

        let strategy_ids =
            dashboard_cancel_strategy_ids(&state, &visible_orders).expect("strategy ids");
        assert!(strategy_ids.contains("fib_xyz"));
        assert!(strategy_ids.contains("fib_hl"));
        assert!(strategy_ids.contains("fib_basic_hl_perp_BTC_5m"));
        assert_eq!(
            dashboard_cancel_stopped_without_open_orders_count(&strategy_ids, &visible_orders),
            1
        );
    }

    #[test]
    fn fib_stop_loss_exit_uses_dedicated_cooldown() {
        let mut record = test_fib_record(
            "fib_basic_hl_perp_ETH_5m",
            MARKET_HL_PERP,
            "ETH",
            FibInstanceStatus::Completed,
            true,
        );
        record.config.cooldown_secs = 300;
        record.config.stop_loss_cooldown_secs = 1800;

        record.last_cycle_exit_kind = Some("take_profit".to_string());
        assert_eq!(fib_restart_cooldown_secs(&record), 0);

        record.last_cycle_exit_kind = Some("stop_loss".to_string());
        assert_eq!(fib_restart_cooldown_secs(&record), 1800);

        record.last_cycle_exit_kind = None;
        assert_eq!(fib_restart_cooldown_secs(&record), 300);
    }

    #[test]
    fn fib_stop_loss_exit_can_stop_strategy() {
        let mut record = test_fib_record(
            "fib_basic_hl_perp_ETH_5m",
            MARKET_HL_PERP,
            "ETH",
            FibInstanceStatus::Completed,
            true,
        );
        record.config.stop_loss_stop_strategy = false;
        assert!(!fib_should_stop_after_cycle(&record, Some("stop_loss")));

        record.config.stop_loss_stop_strategy = true;
        assert!(fib_should_stop_after_cycle(&record, Some("stop_loss")));
        assert!(!fib_should_stop_after_cycle(&record, Some("take_profit")));
        assert!(!fib_should_stop_after_cycle(&record, None));
    }

    #[test]
    fn fib_entry_sync_detects_missing_multi_account_submission() {
        let mut record = test_fib_record(
            "fib_basic_hl_perp_ETH_5m",
            MARKET_HL_PERP,
            "ETH",
            FibInstanceStatus::ArmedUnfilled,
            true,
        );
        record.config.account_ids = vec!["addr_a".to_string(), "addr_b".to_string()];
        let plan = build_basic_plan(&record.config).expect("plan");
        let signals = fib_coordinator_signals_from_plan(&record.config, &plan).expect("signals");
        assert_eq!(signals.len(), 1);

        let submitted = crate::domain::OrderSubmitted {
            signal_id: signals[0].signal_id.clone(),
            intent_id: "intent-a".to_string(),
            worker_id: "worker-addr_a".to_string(),
            account_id: "addr_a".to_string(),
            cloid: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, b"fib-addr-a").to_string(),
            coin: "ETH".to_string(),
            side: OrderSide::Buy,
            notional_usd: 11.0,
            submitted_price: Some(90.0),
            submitted_size: Some(0.1),
            exchange_status: Some("resting".to_string()),
            oid: Some(42),
            filled_size: None,
            avg_fill_price: None,
            dry_run: false,
            submitted_at_ms: 1,
        };
        let reports = vec![WorkerReport::Submitted(submitted)];
        let assessment = fib_entry_sync_assessment(&signals, &reports);

        assert!(!assessment.is_complete());
        assert_eq!(assessment.expected_count, 2);
        assert_eq!(assessment.submitted_count, 1);
        assert!(
            assessment
                .missing_targets
                .iter()
                .any(|target| target.starts_with("addr_b/"))
        );
    }

    #[test]
    fn fib_entry_signal_account_groups_keep_all_levels_per_account() {
        let mut config = FibBasicConfig {
            strategy_id: "fib-group-test".to_string(),
            direction: FibTradeDirection::Long,
            market: MARKET_HL_PERP.to_string(),
            dex: String::new(),
            account_ids: vec!["addr_a".to_string(), "addr_b".to_string()],
            coin: "ETH".to_string(),
            timeframe: "5m".to_string(),
            lookback_bars: 30,
            swing_high: 100.0,
            swing_low: 80.0,
            current_price: 90.0,
            levels: vec![0.5, 0.618],
            entry_above_tolerance_usd: 100.0,
            entry_below_tolerance_usd: 100.0,
            principal_usd: 2.2,
            leverage: 10.0,
            execution_mode: ExecutionMode::Taker,
            take_profit_mode: FibProfitLossMode::PrincipalPercent,
            take_profit_value: 5.0,
            stop_loss_mode: FibProfitLossMode::PrincipalPercent,
            stop_loss_value: 5.0,
            max_slippage_bps: 20.0,
            max_entries_per_level: 1,
            cooldown_secs: 300,
            stop_loss_cooldown_secs: 900,
            stop_loss_stop_strategy: false,
            locked_range: false,
            auto_loop: true,
        };
        let plan = build_basic_plan(&config).expect("plan");
        let signals = fib_coordinator_signals_from_plan(&config, &plan).expect("signals");
        assert_eq!(signals.len(), 2);

        let groups = super::fib_entry_signal_account_groups(&signals);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].0, "addr_a");
        assert_eq!(groups[1].0, "addr_b");
        assert_eq!(groups[0].1.len(), 2);
        assert_eq!(groups[1].1.len(), 2);
        assert_eq!(groups[0].1[0].signal_id, signals[0].signal_id);
        assert_eq!(groups[0].1[1].signal_id, signals[1].signal_id);
        assert_eq!(groups[1].1[0].signal_id, signals[0].signal_id);
        assert_eq!(groups[1].1[1].signal_id, signals[1].signal_id);

        config.account_ids = vec!["addr_b".to_string(), "addr_a".to_string()];
        let plan = build_basic_plan(&config).expect("reordered plan");
        let signals = fib_coordinator_signals_from_plan(&config, &plan).expect("signals");
        let groups = super::fib_entry_signal_account_groups(&signals);
        assert_eq!(groups[0].0, "addr_b");
        assert_eq!(groups[1].0, "addr_a");
    }

    #[test]
    fn fib_filled_entry_aggregate_uses_weighted_average_and_total_size() {
        let first = WorkerReport::Submitted(crate::domain::OrderSubmitted {
            signal_id: "signal-1".to_string(),
            intent_id: "intent-1".to_string(),
            worker_id: "worker-addr_a".to_string(),
            account_id: "addr_a".to_string(),
            cloid: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, b"fib-fill-1").to_string(),
            coin: "ETH".to_string(),
            side: OrderSide::Buy,
            notional_usd: 11.0,
            submitted_price: Some(100.0),
            submitted_size: Some(0.11),
            exchange_status: Some("filled".to_string()),
            oid: Some(1),
            filled_size: Some(0.11),
            avg_fill_price: Some(100.0),
            dry_run: false,
            submitted_at_ms: 1,
        });
        let second = WorkerReport::Submitted(crate::domain::OrderSubmitted {
            signal_id: "signal-2".to_string(),
            intent_id: "intent-2".to_string(),
            worker_id: "worker-addr_a".to_string(),
            account_id: "addr_a".to_string(),
            cloid: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, b"fib-fill-2").to_string(),
            coin: "ETH".to_string(),
            side: OrderSide::Buy,
            notional_usd: 22.0,
            submitted_price: Some(110.0),
            submitted_size: Some(0.2),
            exchange_status: Some("filled".to_string()),
            oid: Some(2),
            filled_size: Some(0.2),
            avg_fill_price: Some(110.0),
            dry_run: false,
            submitted_at_ms: 2,
        });

        let aggregate = super::fib_filled_entry_aggregate(&[first, second]).expect("aggregate");

        assert!((aggregate.filled_size - 0.31).abs() < 0.000001);
        assert!((aggregate.notional_usd - 33.0).abs() < 0.000001);
        assert!((aggregate.avg_entry_price - (33.0 / 0.31)).abs() < 0.000001);
    }

    #[test]
    fn fib_incomplete_entry_submission_pauses_auto_loop() {
        let mut record = test_fib_record(
            "fib_basic_hl_perp_ETH_5m",
            MARKET_HL_PERP,
            "ETH",
            FibInstanceStatus::EntryPending,
            true,
        );
        record.config.account_ids = vec!["addr_a".to_string(), "addr_b".to_string()];
        let assessment = super::FibEntrySyncAssessment {
            expected_count: 2,
            submitted_count: 1,
            missing_targets: vec!["addr_b/signal-1".to_string()],
        };

        mark_fib_record_incomplete_entry_submission(&mut record, &assessment, &[], false, &[]);

        assert_eq!(record.status, FibInstanceStatus::Error);
        assert!(!record.config.auto_loop);
        assert!(
            record
                .last_message
                .as_deref()
                .unwrap_or_default()
                .contains("addr_b")
        );
    }

    #[test]
    fn fib_retryable_maker_miss_keeps_auto_loop_active() {
        let mut record = test_fib_record(
            "fib_basic_hl_perp_ETH_5m",
            MARKET_HL_PERP,
            "ETH",
            FibInstanceStatus::EntryPending,
            true,
        );
        record.config.account_ids = vec!["addr_a".to_string(), "addr_b".to_string()];
        let assessment = super::FibEntrySyncAssessment {
            expected_count: 2,
            submitted_count: 0,
            missing_targets: vec!["addr_a/signal-1".to_string(), "addr_b/signal-1".to_string()],
        };
        let reports = vec![
            WorkerReport::Error(crate::domain::WorkerError {
                worker_id: "worker-addr_a".to_string(),
                account_id: "addr_a".to_string(),
                message: "Post only order would immediately match".to_string(),
                error_at_ms: 1,
            }),
            WorkerReport::Error(crate::domain::WorkerError {
                worker_id: "worker-addr_b".to_string(),
                account_id: "addr_b".to_string(),
                message: "Post only order would immediately match".to_string(),
                error_at_ms: 1,
            }),
        ];

        mark_fib_record_incomplete_entry_submission(&mut record, &assessment, &reports, false, &[]);

        assert_eq!(record.status, FibInstanceStatus::ArmedUnfilled);
        assert!(record.config.auto_loop);
        assert!(
            record
                .last_message
                .as_deref()
                .unwrap_or_default()
                .contains("will retry")
        );
    }

    #[test]
    fn fib_auto_loop_retry_wait_remains_armed_unfilled() {
        let record = test_fib_record(
            "fib-wait",
            MARKET_HL_PERP,
            "xyz:TSLA",
            FibInstanceStatus::Completed,
            true,
        );
        let state = test_frontend_state_with_fib_records(vec![record.clone()]);

        mark_fib_record_auto_loop_retry_wait(&state, &record.strategy_id, "waiting for price")
            .expect("mark auto-loop wait");

        let guard = state.fib_instances.read().expect("fib lock");
        let next = guard.get(&record.strategy_id).expect("fib record");
        assert_eq!(next.status, FibInstanceStatus::ArmedUnfilled);
        assert!(next.config.auto_loop);
        assert!(next.entry_order_refs.is_empty());
        assert!(next.protective_order_refs.is_empty());
        assert_eq!(next.last_message.as_deref(), Some("waiting for price"));
    }

    #[test]
    fn fib_recovered_zero_cycle_completed_auto_loop_becomes_armed_unfilled() {
        let mut record = test_fib_record(
            "fib-recovered-wait",
            MARKET_HL_PERP,
            "xyz:TSLA",
            FibInstanceStatus::Completed,
            true,
        );

        assert!(normalize_recovered_fib_instance(&mut record));
        assert_eq!(record.status, FibInstanceStatus::ArmedUnfilled);
        assert!(record.config.auto_loop);
    }

    #[test]
    fn fib_recovered_live_order_refs_preserve_live_background_mode() {
        let mut record = test_fib_record(
            "fib-live-recovered",
            MARKET_HL_PERP,
            "ETH",
            FibInstanceStatus::EntryPending,
            true,
        );
        record.dry_run = true;
        record.live = false;
        record.entry_order_refs.push(FibOrderRef {
            account_id: "addr_a".to_string(),
            coin: "ETH".to_string(),
            cloid: "live-cloid".to_string(),
            oid: Some(42),
            level: Some(0.5),
            role: Some("entry".to_string()),
            dry_run: false,
            submitted_at_ms: 1,
        });

        assert!(normalize_recovered_fib_instance(&mut record));
        assert!(!record.dry_run);
        assert!(record.live);
        assert!(super::fib_record_allows_background_live_actions(&record));
    }

    #[test]
    fn fib_waiting_message_explains_maker_recovery_wait() {
        let mut record = test_fib_record(
            "fib_basic_hl_perp_ETH_5m",
            MARKET_HL_PERP,
            "ETH",
            FibInstanceStatus::ArmedUnfilled,
            true,
        );
        record.config.current_price = 89.0;
        record.plan = build_basic_plan(&record.config).expect("plan");

        let message = fib_waiting_for_entry_message(&record);

        assert!(message.contains("maker entry was missed"));
        assert!(message.contains("waiting for price to recover"));
    }

    #[test]
    fn fib_complete_protection_requires_every_target_account() {
        let mut record = test_fib_record(
            "fib_basic_hl_perp_ETH_5m",
            MARKET_HL_PERP,
            "ETH",
            FibInstanceStatus::Protected,
            true,
        );
        record.config.account_ids = vec!["addr_a".to_string(), "addr_b".to_string()];
        record.protective_order_refs = vec![
            FibOrderRef {
                account_id: "addr_a".to_string(),
                coin: "ETH".to_string(),
                cloid: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, b"addr-a-tp").to_string(),
                oid: Some(1),
                level: None,
                role: Some("take_profit".to_string()),
                dry_run: false,
                submitted_at_ms: 1,
            },
            FibOrderRef {
                account_id: "addr_a".to_string(),
                coin: "ETH".to_string(),
                cloid: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, b"addr-a-sl").to_string(),
                oid: Some(2),
                level: None,
                role: Some("stop_loss".to_string()),
                dry_run: false,
                submitted_at_ms: 1,
            },
        ];

        assert!(!fib_all_target_accounts_have_complete_protection(&record));

        record.protective_order_refs.push(FibOrderRef {
            account_id: "addr_b".to_string(),
            coin: "ETH".to_string(),
            cloid: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, b"addr-b-tp").to_string(),
            oid: Some(3),
            level: None,
            role: Some("take_profit".to_string()),
            dry_run: false,
            submitted_at_ms: 1,
        });
        assert!(!fib_all_target_accounts_have_complete_protection(&record));

        record.protective_order_refs.push(FibOrderRef {
            account_id: "addr_b".to_string(),
            coin: "ETH".to_string(),
            cloid: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, b"addr-b-sl").to_string(),
            oid: Some(4),
            level: None,
            role: Some("stop_loss".to_string()),
            dry_run: false,
            submitted_at_ms: 1,
        });
        assert!(fib_all_target_accounts_have_complete_protection(&record));
    }

    #[test]
    fn transfer_amount_label_trims_trailing_zeroes_for_confirmation_phrase() {
        assert_eq!(transfer_amount_label(2.0), "2");
        assert_eq!(transfer_amount_label(2.5), "2.5");
        assert_eq!(transfer_amount_label(0.000001), "0.000001");
    }

    #[test]
    fn usdc_transfer_readiness_lists_transfer_specific_next_actions() {
        let checks = vec![
            ReadinessCheckResponse::blocker("config_dry_run_disabled", false, "dry-run"),
            ReadinessCheckResponse::blocker("mainnet_explicit_confirmation", false, "phrase"),
            ReadinessCheckResponse::blocker("transfer_plan_valid", true, "plan ok"),
        ];

        let failed = failed_readiness_blockers(&checks);
        let actions = usdc_transfer_readiness_next_actions(
            &checks,
            "mainnet",
            "",
            "xyz",
            Some("TRANSFER 2 USDC TO xyz FOR addr_a"),
        );
        let summary = usdc_transfer_readiness_summary("mainnet", false, false, &failed);

        assert!(summary.contains("blocked by 2 check"));
        assert!(
            actions
                .iter()
                .any(|action| action.contains("app.dry_run=false"))
        );
        assert!(
            actions
                .iter()
                .any(|action| action.contains("TRANSFER 2 USDC TO xyz FOR addr_a"))
        );
    }

    #[test]
    fn vault_upsert_adds_account_to_runtime_config_file() {
        let dir = std::env::temp_dir().join(format!(
            "trade_xyz_frontend_config_account_{}",
            crate::domain::now_ms()
        ));
        fs::create_dir_all(&dir).expect("test dir");
        let config_path = dir.join("dry-run.toml");
        let mut config = AppConfig::default();
        config.secrets.vault_path = dir.join("trade_xyz.vault").to_string_lossy().into_owned();
        config.accounts = vec![crate::config::AccountConfig {
            account_id: "addr_a".to_string(),
            address: "0x0000000000000000000000000000000000000001".to_string(),
            secret_id: "addr_a_api_wallet".to_string(),
            api_wallet_env: String::new(),
            transfer_secret_id: String::new(),
            transfer_wallet_env: String::new(),
            enabled: true,
            worker_enabled: true,
            copy_ratio: 0.1,
            max_order_notional_usd: 100.0,
            blocked_markets: Vec::new(),
        }];
        crate::config::save_config(&config_path, &config).expect("write test config");

        let state = FrontendAppState {
            config: Arc::new(std::sync::RwLock::new(config)),
            config_path: config_path.clone(),
            dry_run: true,
            started_at_ms: crate::domain::now_ms(),
            vault_session: Arc::new(std::sync::RwLock::new(None)),
            fib_instances: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            fib_stop_requests: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            realtime: RealtimeState::new(),
            account_funding_batch_cache: Arc::new(std::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
            manual_protective_rules_cache: Arc::new(std::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
        };

        state
            .upsert_config_account(
                &SecretUpsert {
                    secret_id: "addr_c_api_wallet".to_string(),
                    account_id: "addr_c".to_string(),
                    address: "0x0000000000000000000000000000000000000003".to_string(),
                    api_wallet_private_key:
                        "0x3333333333333333333333333333333333333333333333333333333333333333"
                            .to_string(),
                },
                SecretUsage::Trading,
            )
            .expect("config account upsert");

        let config = crate::config::load_config(&config_path).expect("reload config");
        let account = config.account("addr_c").expect("new account in config");
        assert_eq!(
            account.address,
            "0x0000000000000000000000000000000000000003"
        );
        assert_eq!(account.secret_id, "addr_c_api_wallet");
        assert!(account.enabled);
        assert!(account.worker_enabled);
    }

    #[test]
    fn vault_upsert_transfer_secret_does_not_replace_trading_secret() {
        let dir = std::env::temp_dir().join(format!(
            "trade_xyz_frontend_config_transfer_account_{}",
            crate::domain::now_ms()
        ));
        fs::create_dir_all(&dir).expect("test dir");
        let config_path = dir.join("dry-run.toml");
        let mut config = AppConfig::default();
        config.accounts = vec![crate::config::AccountConfig {
            account_id: "addr_a".to_string(),
            address: "0x0000000000000000000000000000000000000001".to_string(),
            secret_id: "addr_a_api_wallet".to_string(),
            api_wallet_env: String::new(),
            transfer_secret_id: String::new(),
            transfer_wallet_env: String::new(),
            enabled: true,
            worker_enabled: true,
            copy_ratio: 0.1,
            max_order_notional_usd: 100.0,
            blocked_markets: Vec::new(),
        }];
        crate::config::save_config(&config_path, &config).expect("write test config");

        let state = FrontendAppState {
            config: Arc::new(std::sync::RwLock::new(config)),
            config_path: config_path.clone(),
            dry_run: true,
            started_at_ms: crate::domain::now_ms(),
            vault_session: Arc::new(std::sync::RwLock::new(None)),
            fib_instances: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            fib_stop_requests: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            realtime: RealtimeState::new(),
            account_funding_batch_cache: Arc::new(std::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
            manual_protective_rules_cache: Arc::new(std::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
        };

        state
            .upsert_config_account(
                &SecretUpsert {
                    secret_id: "addr_a_evm_wallet".to_string(),
                    account_id: "addr_a".to_string(),
                    address: "0x0000000000000000000000000000000000000001".to_string(),
                    api_wallet_private_key:
                        "0x3333333333333333333333333333333333333333333333333333333333333333"
                            .to_string(),
                },
                SecretUsage::Transfer,
            )
            .expect("transfer config upsert");

        let config = crate::config::load_config(&config_path).expect("reload config");
        let account = config.account("addr_a").expect("account in config");
        assert_eq!(account.secret_id, "addr_a_api_wallet");
        assert_eq!(account.transfer_secret_id, "addr_a_evm_wallet");
    }

    #[test]
    fn vault_unlock_syncs_existing_entries_into_runtime_config_file() {
        let dir = std::env::temp_dir().join(format!(
            "trade_xyz_frontend_vault_sync_{}",
            crate::domain::now_ms()
        ));
        fs::create_dir_all(&dir).expect("test dir");
        let config_path = dir.join("dry-run.toml");
        let mut config = AppConfig::default();
        config.secrets.vault_path = dir.join("trade_xyz.vault").to_string_lossy().into_owned();
        config.accounts = vec![crate::config::AccountConfig {
            account_id: "addr_a".to_string(),
            address: "0x0000000000000000000000000000000000000001".to_string(),
            secret_id: "addr_a_api_wallet".to_string(),
            api_wallet_env: String::new(),
            transfer_secret_id: String::new(),
            transfer_wallet_env: String::new(),
            enabled: true,
            worker_enabled: true,
            copy_ratio: 0.1,
            max_order_notional_usd: 1.0,
            blocked_markets: Vec::new(),
        }];
        crate::config::save_config(&config_path, &config).expect("write test config");

        let state = FrontendAppState {
            config: Arc::new(std::sync::RwLock::new(config)),
            config_path: config_path.clone(),
            dry_run: true,
            started_at_ms: crate::domain::now_ms(),
            vault_session: Arc::new(std::sync::RwLock::new(None)),
            fib_instances: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            fib_stop_requests: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            realtime: RealtimeState::new(),
            account_funding_batch_cache: Arc::new(std::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
            manual_protective_rules_cache: Arc::new(std::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
        };
        let vault_path = dir.join("trade_xyz.vault");
        fs::write(&vault_path, b"encrypted-placeholder").expect("vault placeholder");
        let summary = VaultSummary {
            exists: true,
            unlocked: true,
            path: vault_path.display().to_string(),
            entry_count: Some(2),
            entries: vec![
                VaultEntrySummary {
                    secret_id: "addr_a_api_wallet".to_string(),
                    account_id: "addr_a".to_string(),
                    address: "0x1234567890abcdef1234567890abcdef12345678".to_string(),
                    updated_at_ms: 1,
                },
                VaultEntrySummary {
                    secret_id: "addr_c_api_wallet".to_string(),
                    account_id: "addr_c".to_string(),
                    address: "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_string(),
                    updated_at_ms: 2,
                },
                VaultEntrySummary {
                    secret_id: "addr_a_evm_wallet".to_string(),
                    account_id: "addr_a".to_string(),
                    address: "0x1234567890abcdef1234567890abcdef12345678".to_string(),
                    updated_at_ms: 3,
                },
            ],
        };

        state
            .store_vault_session(vault_path, "persistent password".to_string(), summary)
            .expect("store session and sync config");

        let reloaded = crate::config::load_config(&config_path).expect("reload config");
        let addr_a = reloaded
            .account("addr_a")
            .expect("existing account updated");
        assert_eq!(addr_a.address, "0x1234567890abcdef1234567890abcdef12345678");
        assert_eq!(addr_a.secret_id, "addr_a_api_wallet");
        assert_eq!(addr_a.transfer_secret_id, "addr_a_evm_wallet");
        assert_eq!(addr_a.max_order_notional_usd, 1.0);
        let addr_c = reloaded.account("addr_c").expect("vault account added");
        assert_eq!(addr_c.address, "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        assert_eq!(addr_c.secret_id, "addr_c_api_wallet");
        assert!(addr_c.enabled);
        assert!(addr_c.worker_enabled);
    }

    #[test]
    fn manual_settings_update_persists_caps_to_config_file() {
        let dir = std::env::temp_dir().join(format!(
            "trade_xyz_frontend_manual_settings_{}",
            crate::domain::now_ms()
        ));
        fs::create_dir_all(&dir).expect("test dir");
        let config_path = dir.join("dry-run.toml");
        let mut config = AppConfig::default();
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
                copy_ratio: 0.1,
                max_order_notional_usd: 1.0,
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
                copy_ratio: 0.1,
                max_order_notional_usd: 1.0,
                blocked_markets: Vec::new(),
            },
        ];
        crate::config::save_config(&config_path, &config).expect("write test config");

        let state = FrontendAppState {
            config: Arc::new(std::sync::RwLock::new(config)),
            config_path: config_path.clone(),
            dry_run: true,
            started_at_ms: crate::domain::now_ms(),
            vault_session: Arc::new(std::sync::RwLock::new(None)),
            fib_instances: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            fib_stop_requests: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            realtime: RealtimeState::new(),
            account_funding_batch_cache: Arc::new(std::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
            manual_protective_rules_cache: Arc::new(std::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
        };

        let response = state
            .apply_manual_settings(ManualSettingsPayload {
                max_manual_order_notional_usd: Some(11.0),
                account_max_order_notional_usd: Some(7.5),
                account_ids: vec!["addr_a".to_string()],
            })
            .expect("apply manual settings");
        assert_eq!(response.max_manual_order_notional_usd, 11.0);
        assert_eq!(response.updated_account_limits.len(), 1);
        assert_eq!(response.updated_account_limits[0].account_id, "addr_a");
        assert_eq!(
            response.updated_account_limits[0].max_order_notional_usd,
            7.5
        );

        let reloaded = crate::config::load_config(&config_path).expect("reload config");
        assert_eq!(reloaded.manual_ops.max_manual_order_notional_usd, 11.0);
        assert_eq!(
            reloaded
                .account("addr_a")
                .expect("addr_a present")
                .max_order_notional_usd,
            7.5
        );
        assert_eq!(
            reloaded
                .account("addr_b")
                .expect("addr_b present")
                .max_order_notional_usd,
            1.0
        );
    }

    #[test]
    fn live_readiness_summary_lists_blockers_and_next_actions() {
        let checks = vec![
            ReadinessCheckResponse::blocker("vault_unlocked", false, "vault locked"),
            ReadinessCheckResponse::blocker(
                "account_has_available_collateral",
                false,
                "accountValue=0",
            ),
            ReadinessCheckResponse::blocker("signed_order_plan_valid", true, "plan ok"),
        ];

        let failed = failed_readiness_blockers(&checks);
        let actions = live_readiness_next_actions(&checks, false, "testnet", "xyz", "manual");
        let summary = live_readiness_summary("testnet", false, false, &failed);

        assert_eq!(failed.len(), 2);
        assert!(failed[0].contains("vault_unlocked"));
        assert!(actions.iter().any(|action| action.contains("Unlock")));
        assert!(actions.iter().any(|action| action.contains("Fund")));
        assert_eq!(summary, "testnet signed submit blocked by 2 check(s)");
    }

    #[test]
    fn live_readiness_lists_exchange_min_notional_blocker() {
        let checks = vec![ReadinessCheckResponse::blocker(
            "exchange_min_order_notional",
            false,
            "opening orders must be at least 10 USD",
        )];

        let failed = failed_readiness_blockers(&checks);
        let actions = live_readiness_next_actions(&checks, false, "mainnet", "xyz", "manual");

        assert_eq!(failed.len(), 1);
        assert!(failed[0].contains("exchange_min_order_notional"));
        assert!(
            actions
                .iter()
                .any(|action| action.contains("at least 10 USD"))
        );
    }

    #[test]
    fn account_funding_summary_points_to_correct_funding_layer() {
        let default_state: crate::hyperliquid::ClearinghouseState = serde_json::from_str(
            r#"{
                "marginSummary": {
                    "accountValue": "25",
                    "totalNtlPos": "0",
                    "totalRawUsd": "25",
                    "totalMarginUsed": "0"
                },
                "withdrawable": "25",
                "assetPositions": []
            }"#,
        )
        .expect("default perp state");
        let zero_xyz_state: crate::hyperliquid::ClearinghouseState = serde_json::from_str(
            r#"{
                "marginSummary": {
                    "accountValue": "0",
                    "totalNtlPos": "0",
                    "totalRawUsd": "0",
                    "totalMarginUsed": "0"
                },
                "withdrawable": "0",
                "assetPositions": []
            }"#,
        )
        .expect("xyz perp state");
        let spot_state: crate::hyperliquid::SpotClearinghouseState =
            serde_json::from_str(r#"{"balances":[]}"#).expect("spot state");

        let default_perp = PerpFundingLayer::from_state("default_perp", &default_state);
        let xyz_perp = PerpFundingLayer::from_state("xyz_perp", &zero_xyz_state);
        let spot = SpotFundingLayer::from_state(&spot_state);

        let summary = account_funding_summary(&default_perp, &xyz_perp, &spot);
        let actions =
            account_funding_next_actions(&default_perp, &xyz_perp, &spot, "mainnet", "xyz");

        assert!(summary.contains("default perp"));
        assert!(
            actions
                .iter()
                .any(|action| action.contains("Transfer USDC"))
        );
    }

    #[test]
    fn order_status_payload_requires_exactly_one_lookup_key() {
        let oid_query = OrderStatusPayload {
            account_id: "addr_a".to_string(),
            oid: Some(123),
            cloid: None,
        }
        .query()
        .expect("oid query");
        assert!(matches!(oid_query, OrderStatusQuery::Oid { oid: 123 }));

        let cloid_query = OrderStatusPayload {
            account_id: "addr_a".to_string(),
            oid: None,
            cloid: Some("00000000-0000-0000-0000-000000000001".to_string()),
        }
        .query()
        .expect("cloid query");
        assert!(matches!(cloid_query, OrderStatusQuery::Cloid { .. }));

        let missing = OrderStatusPayload {
            account_id: "addr_a".to_string(),
            oid: None,
            cloid: None,
        }
        .query()
        .expect_err("missing lookup key")
        .to_string();
        assert!(missing.contains("exactly one"));

        let duplicate = OrderStatusPayload {
            account_id: "addr_a".to_string(),
            oid: Some(123),
            cloid: Some("00000000-0000-0000-0000-000000000001".to_string()),
        }
        .query()
        .expect_err("duplicate lookup keys")
        .to_string();
        assert!(duplicate.contains("only one"));
    }
}
