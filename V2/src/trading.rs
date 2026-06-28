use std::{
    collections::HashSet,
    ffi::OsStr,
    path::{Component, Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use ethers::signers::LocalWallet;
use futures_util::future::join_all;
use hyperliquid_rust_sdk::{
    ClientCancelRequest, ClientCancelRequestCloid, ClientLimit, ClientOrder, ClientOrderRequest,
    ClientTrigger, ExchangeClient, ExchangeDataStatus, ExchangeResponseStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    config::{AccountConfig, AppConfig, MARKET_HL_PERP, MARKET_SPOT, MARKET_XYZ_PERP, save_config},
    domain::{
        ApprovedOrder, ExecutionMode, ExecutionPolicy, OrderSide, OrderSubmitted, WorkerError,
        WorkerReport, now_ms,
    },
    hyperliquid::{
        ClearinghouseState, MAINNET_USDC_TOKEN, OpenOrder, OrderPlan, OrderStatusResponse,
        SendAssetSubmitRequest, SpotClearinghouseState, SpotMarketSnapshot, UserFill,
        UserRateLimit, XyzMarketSnapshot, build_order_plan, build_spot_order_plan,
        fetch_clearinghouse_state, fetch_default_clearinghouse_state, fetch_open_orders,
        fetch_order_status_by_cloid, fetch_order_status_by_oid, fetch_spot_clearinghouse_state,
        fetch_spot_market_snapshot_cached, fetch_user_fills, fetch_user_rate_limit,
        fetch_xyz_market_snapshot_cached, normalize_dex_coin, normalize_spot_coin,
        round_perp_price, round_size_down, round_spot_price, sdk_base_url, submit_send_asset,
    },
    realtime::RealtimeState,
    secrets::{
        ApiWalletSecret, account_has_dedicated_transfer_secret, account_secret_id,
        load_account_secret, load_secret_by_id, load_transfer_secret, transfer_secret_id,
    },
    ws_post::WsPostClient,
};

// Observed from Hyperliquid mainnet action-level order errors on 2026-05-31.
// Keep this in one place until the exchange exposes a dynamic per-market min value.
pub const HYPERLIQUID_MIN_ORDER_NOTIONAL_USD: f64 = 10.0;
const EXCHANGE_ACTION_MAX_ATTEMPTS: usize = 5;
const EXCHANGE_ACTION_BASE_BACKOFF_MS: u64 = 750;
const EXCHANGE_ACTION_MAX_BACKOFF_MS: u64 = 12_000;

#[derive(Debug, Clone, Deserialize)]
pub struct ProtectiveExitOptions {
    pub account_id: String,
    pub coin: String,
    pub entry_side: String,
    #[serde(default)]
    pub entry_price: Option<f64>,
    pub notional_usd: f64,
    pub take_profit_usd: f64,
    pub stop_loss_pct: f64,
    #[serde(default)]
    pub take_profit_trigger_price: Option<f64>,
    #[serde(default)]
    pub stop_loss_trigger_price: Option<f64>,
    pub max_slippage_bps: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProtectiveExitPlanResult {
    pub environment: String,
    pub dex: String,
    pub account_id: String,
    pub coin: String,
    pub entry_side: OrderSide,
    pub exit_side: OrderSide,
    pub entry_price: f64,
    pub market_reference_price: f64,
    pub reduce_only: bool,
    pub dry_run: bool,
    pub legs: Vec<ProtectiveExitLeg>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProtectiveExitLeg {
    pub kind: String,
    pub trigger_price: f64,
    pub limit_price: f64,
    pub size: f64,
    pub asset_id: u32,
    pub sz_decimals: u32,
    pub cloid: String,
    pub local_trigger: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProtectiveExitTriggerCheckOptions {
    #[serde(flatten)]
    pub exit: ProtectiveExitOptions,
    #[serde(default)]
    pub observed_price: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProtectiveExitTriggerCheckResult {
    pub plan: ProtectiveExitPlanResult,
    pub observed_price: f64,
    pub triggered: bool,
    pub triggered_leg: Option<ProtectiveExitLeg>,
    pub exit_order: Option<ProtectiveExitOrderPreview>,
    pub checked_at_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProtectiveExitOrderPreview {
    pub account_id: String,
    pub coin: String,
    pub side: OrderSide,
    pub size: f64,
    pub limit_price: f64,
    pub reduce_only: bool,
    pub cloid: String,
    pub trigger_kind: String,
    pub trigger_price: f64,
    pub execution_policy: ExecutionPolicy,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProtectiveExitSubmitOptions {
    #[serde(flatten)]
    pub trigger: ProtectiveExitTriggerCheckOptions,
    #[serde(default)]
    pub submit: bool,
    #[serde(default)]
    pub confirm_mainnet_live: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProtectiveExitSubmitResult {
    pub trigger: ProtectiveExitTriggerCheckResult,
    pub submit_requested: bool,
    pub submitted: bool,
    pub submit_report: Option<WorkerReport>,
    pub post_submit_reconciliation: Option<AccountReconciliationReport>,
    pub order_status: Option<OrderStatusResponse>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProtectiveExitArmOptions {
    #[serde(flatten)]
    pub exit: ProtectiveExitOptions,
    #[serde(default)]
    pub submit: bool,
    #[serde(default)]
    pub confirm_mainnet_live: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProtectiveExitArmResult {
    pub plan: ProtectiveExitPlanResult,
    pub submit_requested: bool,
    pub submitted: bool,
    pub submit_reports: Vec<WorkerReport>,
    pub post_submit_reconciliation: Option<AccountReconciliationReport>,
    pub order_statuses: Vec<OrderStatusResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persistent_rule_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub armed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub monitor_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_check: Option<ProtectiveExitTriggerCheckResult>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UsdcDexTransferOptions {
    pub account_id: String,
    #[serde(default)]
    pub destination_account_id: Option<String>,
    pub amount_usdc: f64,
    #[serde(default)]
    pub source_dex: Option<String>,
    #[serde(default)]
    pub destination_dex: Option<String>,
    #[serde(default)]
    pub submit: bool,
    #[serde(default)]
    pub confirm_mainnet_live: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct UsdcDexTransferResult {
    pub environment: String,
    pub account_id: String,
    pub address: String,
    pub destination_account_id: String,
    pub destination_address: String,
    pub source_dex: String,
    pub destination_dex: String,
    pub token: String,
    pub amount: String,
    pub amount_usdc: f64,
    pub submit_requested: bool,
    pub submitted: bool,
    pub signer_address: Option<String>,
    pub exchange_response: Option<Value>,
    pub before: DexTransferBalances,
    pub after: Option<DexTransferBalances>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DexTransferBalances {
    pub source_total_usdc: f64,
    pub source_available_usdc: f64,
    pub destination_total_usdc: f64,
    pub destination_available_usdc: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct UsdcDexTransferPreflightResult {
    pub environment: String,
    pub dry_run: bool,
    pub account_id: String,
    pub address: Option<String>,
    pub destination_account_id: String,
    pub destination_address: Option<String>,
    pub amount_usdc: f64,
    pub source_dex: String,
    pub destination_dex: String,
    pub confirmation_phrase: Option<String>,
    pub ready_for_testnet_transfer: bool,
    pub ready_for_mainnet_transfer: bool,
    pub rate_limit: Option<UserRateLimit>,
    pub plan: Option<UsdcDexTransferResult>,
    pub checks: Vec<SignedPreflightCheck>,
    pub failed_blockers: Vec<String>,
    pub next_actions: Vec<String>,
    pub readiness_summary: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct UsdcDexTransferRunbookResult {
    pub environment: String,
    pub account_id: String,
    pub amount_usdc: f64,
    pub submit_requested: bool,
    pub submitted: bool,
    pub preflight: UsdcDexTransferPreflightResult,
    pub transfer: Option<UsdcDexTransferResult>,
    pub checks: Vec<SignedRunbookCheck>,
}

#[derive(Debug, Clone)]
pub struct UsdcDexTransferBatchPreflightOptions {
    pub account_ids: Vec<String>,
    pub destination_account_id: Option<String>,
    pub amount_usdc: f64,
    pub source_dex: Option<String>,
    pub destination_dex: Option<String>,
    pub confirm_mainnet_live: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct UsdcDexTransferBatchPreflightResult {
    pub environment: String,
    pub dex: String,
    pub account_ids: Vec<String>,
    pub ready_account_ids: Vec<String>,
    pub blocked_account_ids: Vec<String>,
    pub failed_account_ids: Vec<String>,
    pub amount_usdc: f64,
    pub source_dex: String,
    pub destination_dex: String,
    pub results: Vec<UsdcDexTransferBatchPreflightAccountResult>,
    pub next_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UsdcDexTransferBatchPreflightAccountResult {
    pub ok: bool,
    pub data: Option<UsdcDexTransferPreflightResult>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct UsdcDexTransferLiveWindowOptions {
    pub account_ids: Vec<String>,
    pub amount_usdc: f64,
    pub destination_dex: Option<String>,
    pub output_config_path: PathBuf,
    pub write: bool,
    pub overwrite: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct UsdcDexTransferLiveWindowResult {
    pub environment: String,
    pub source_config_path: String,
    pub output_config_path: String,
    pub config_written: bool,
    pub amount_usdc: f64,
    pub source_dex: String,
    pub destination_dex: String,
    pub account_ids: Vec<String>,
    pub required_config_changes: Vec<String>,
    pub accounts: Vec<UsdcDexTransferLiveWindowAccount>,
    pub next_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UsdcDexTransferLiveWindowAccount {
    pub account_id: String,
    pub address: String,
    pub secret_id: String,
    pub confirmation_phrase: String,
    pub runbook_args: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccountFundingReport {
    pub environment: String,
    pub dex: String,
    pub account_id: String,
    pub address: String,
    pub default_perp: PerpFundingLayerReport,
    pub xyz_perp: PerpFundingLayerReport,
    pub spot: SpotFundingLayerReport,
    pub funding_summary: String,
    pub next_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccountFundingBatchReport {
    pub environment: String,
    pub dex: String,
    pub account_ids: Vec<String>,
    pub ready_account_ids: Vec<String>,
    pub transfer_needed_account_ids: Vec<String>,
    pub failed_account_ids: Vec<String>,
    pub results: Vec<AccountFundingAccountResult>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccountFundingAccountResult {
    pub ok: bool,
    pub data: Option<AccountFundingReport>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PerpFundingLayerReport {
    pub name: String,
    pub query_ok: bool,
    pub error: Option<String>,
    pub account_value_usd: f64,
    pub withdrawable_usd: f64,
    pub total_notional_position_usd: f64,
    pub total_margin_used_usd: f64,
    pub position_count: usize,
    pub positions: Vec<PerpFundingPositionReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PerpFundingPositionReport {
    pub coin: String,
    pub size: f64,
    pub position_value_usd: f64,
    pub unrealized_pnl_usd: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpotFundingLayerReport {
    pub query_ok: bool,
    pub error: Option<String>,
    pub total_usdc: f64,
    pub hold_usdc: f64,
    pub balance_count: usize,
    pub balances: Vec<SpotFundingBalanceReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpotFundingBalanceReport {
    pub coin: String,
    pub total: f64,
    pub hold: f64,
}

#[derive(Debug, Clone)]
pub struct DryRunExecutor {
    pub dry_run: bool,
}

impl DryRunExecutor {
    pub fn new(dry_run: bool) -> Self {
        Self { dry_run }
    }

    pub fn submit(&self, order: ApprovedOrder) -> WorkerReport {
        WorkerReport::Submitted(OrderSubmitted {
            signal_id: order.signal_id.unwrap_or_default(),
            intent_id: order.intent_id,
            worker_id: order.worker_id,
            account_id: order.account_id,
            cloid: order.cloid,
            coin: order.coin,
            side: order.side,
            notional_usd: order.notional_usd,
            submitted_price: order.price,
            submitted_size: None,
            exchange_status: Some("dry_run_submitted".to_string()),
            oid: None,
            filled_size: None,
            avg_fill_price: None,
            dry_run: self.dry_run,
            submitted_at_ms: now_ms(),
        })
    }
}

#[derive(Debug)]
pub struct LiveExchangeExecutor {
    config: AppConfig,
    account: AccountConfig,
    secret: ApiWalletSecret,
}

#[derive(Debug)]
pub enum AccountExecutor {
    DryRun(DryRunExecutor),
    Live(Box<LiveExchangeExecutor>),
}

impl AccountExecutor {
    pub fn dry_run(dry_run: bool) -> Self {
        Self::DryRun(DryRunExecutor::new(dry_run))
    }

    pub fn live(config: AppConfig, account: AccountConfig, secret: ApiWalletSecret) -> Self {
        Self::Live(Box::new(LiveExchangeExecutor::new(config, account, secret)))
    }

    pub async fn submit(&self, order: ApprovedOrder) -> WorkerReport {
        match self {
            Self::DryRun(executor) => executor.submit(order),
            Self::Live(executor) => executor.submit(order).await,
        }
    }

    pub async fn submit_fast(&self, order: ApprovedOrder) -> WorkerReport {
        match self {
            Self::DryRun(executor) => executor.submit(order),
            Self::Live(executor) => executor.submit_fast(order).await,
        }
    }

    pub async fn submit_bulk(&self, orders: Vec<ApprovedOrder>) -> Vec<WorkerReport> {
        match self {
            Self::DryRun(executor) => orders
                .into_iter()
                .map(|order| executor.submit(order))
                .collect(),
            Self::Live(executor) => executor.submit_bulk(orders).await,
        }
    }
}

impl LiveExchangeExecutor {
    pub fn new(config: AppConfig, account: AccountConfig, secret: ApiWalletSecret) -> Self {
        Self {
            config,
            account,
            secret,
        }
    }

    pub async fn submit(&self, order: ApprovedOrder) -> WorkerReport {
        let fallback = ErrorFallback::from_order(&order);
        match self.submit_inner(order).await {
            Ok(report) => report,
            Err(error) => WorkerReport::Error(WorkerError {
                worker_id: fallback.worker_id,
                account_id: fallback.account_id,
                message: error.to_string(),
                error_at_ms: now_ms(),
            }),
        }
    }

    pub async fn submit_fast(&self, order: ApprovedOrder) -> WorkerReport {
        let fallback = ErrorFallback::from_order(&order);
        match self.submit_fast_inner(order).await {
            Ok(report) => report,
            Err(error) => WorkerReport::Error(WorkerError {
                worker_id: fallback.worker_id,
                account_id: fallback.account_id,
                message: error.to_string(),
                error_at_ms: now_ms(),
            }),
        }
    }

    pub async fn submit_bulk(&self, orders: Vec<ApprovedOrder>) -> Vec<WorkerReport> {
        let fallbacks = orders
            .iter()
            .map(ErrorFallback::from_order)
            .collect::<Vec<_>>();
        match self.submit_bulk_inner(orders).await {
            Ok(reports) => reports,
            Err(error) => fallbacks
                .into_iter()
                .map(|fallback| {
                    WorkerReport::Error(WorkerError {
                        worker_id: fallback.worker_id,
                        account_id: fallback.account_id,
                        message: error.to_string(),
                        error_at_ms: now_ms(),
                    })
                })
                .collect(),
        }
    }

    async fn submit_inner(&self, order: ApprovedOrder) -> Result<WorkerReport> {
        anyhow::ensure!(
            order.account_id == self.account.account_id,
            "order account {} does not match executor account {}",
            order.account_id,
            self.account.account_id
        );
        let effective_config = self.config_for_order(&order);
        let is_buy = matches!(order.side, OrderSide::Buy);
        let is_spot = is_spot_dex(&effective_config.hyperliquid.dex);
        let perp_snapshot = if is_spot {
            None
        } else {
            Some(
                fetch_xyz_market_snapshot_cached(
                    &effective_config.app.environment,
                    &effective_config.hyperliquid.dex,
                    15_000,
                )
                .await
                .context("failed to fetch perp market snapshot")?,
            )
        };
        let plan = if is_spot {
            let spot_snapshot =
                fetch_spot_market_snapshot_cached(&effective_config.app.environment, 15_000)
                    .await
                    .context("failed to fetch spot market snapshot")?;
            if let Some(exact_size) = order.exact_size {
                build_spot_order_plan_for_size(
                    &spot_snapshot,
                    &order.coin,
                    order.side,
                    exact_size,
                    order.max_slippage_bps,
                )?
            } else {
                build_spot_order_plan(
                    &spot_snapshot,
                    &order.coin,
                    is_buy,
                    order.notional_usd,
                    order.price,
                    order.max_slippage_bps,
                )?
            }
        } else {
            if let Some(exact_size) = order.exact_size {
                build_order_plan_for_size(
                    perp_snapshot
                        .as_ref()
                        .context("missing perp snapshot for order planning")?,
                    &order.coin,
                    order.side,
                    exact_size,
                    order.max_slippage_bps,
                )?
            } else {
                build_order_plan(
                    perp_snapshot
                        .as_ref()
                        .context("missing perp snapshot for order planning")?,
                    &order.coin,
                    is_buy,
                    order.notional_usd,
                    order.price,
                    order.max_slippage_bps,
                )?
            }
        };
        let exchange_client = if is_spot {
            self.exchange_client(None).await?
        } else {
            self.exchange_client(perp_snapshot.as_ref()).await?
        };
        let cloid = uuid::Uuid::parse_str(&order.cloid)
            .with_context(|| format!("risk cloid {} is not a UUID", order.cloid))?;
        let tif = tif_for_policy(order.execution_policy);
        let request = ClientOrderRequest {
            asset: plan.coin.clone(),
            is_buy,
            reduce_only: order.reduce_only,
            limit_px: plan.limit_price,
            sz: plan.size,
            cloid: Some(cloid),
            order_type: ClientOrder::Limit(ClientLimit { tif }),
        };

        let response = exchange_client
            .order(request, None)
            .await
            .context("Hyperliquid order request failed")?;

        response_to_worker_report(order, plan.limit_price, plan.size, response)
    }

    async fn submit_fast_inner(&self, order: ApprovedOrder) -> Result<WorkerReport> {
        anyhow::ensure!(
            order.account_id == self.account.account_id,
            "order account {} does not match executor account {}",
            order.account_id,
            self.account.account_id
        );
        let effective_config = self.config_for_order(&order);
        let is_buy = matches!(order.side, OrderSide::Buy);
        let is_spot = is_spot_dex(&effective_config.hyperliquid.dex);
        let perp_snapshot = if is_spot {
            None
        } else {
            Some(
                fetch_xyz_market_snapshot_cached(
                    &effective_config.app.environment,
                    &effective_config.hyperliquid.dex,
                    15_000,
                )
                .await
                .context("failed to fetch perp market snapshot")?,
            )
        };
        let plan = if is_spot {
            let spot_snapshot =
                fetch_spot_market_snapshot_cached(&effective_config.app.environment, 15_000)
                    .await
                    .context("failed to fetch spot market snapshot")?;
            if let Some(exact_size) = order.exact_size {
                build_spot_order_plan_for_size(
                    &spot_snapshot,
                    &order.coin,
                    order.side,
                    exact_size,
                    order.max_slippage_bps,
                )?
            } else {
                build_spot_order_plan(
                    &spot_snapshot,
                    &order.coin,
                    is_buy,
                    order.notional_usd,
                    order.price,
                    order.max_slippage_bps,
                )?
            }
        } else if let Some(exact_size) = order.exact_size {
            build_order_plan_for_size(
                perp_snapshot
                    .as_ref()
                    .context("missing perp snapshot for order planning")?,
                &order.coin,
                order.side,
                exact_size,
                order.max_slippage_bps,
            )?
        } else {
            build_order_plan(
                perp_snapshot
                    .as_ref()
                    .context("missing perp snapshot for order planning")?,
                &order.coin,
                is_buy,
                order.notional_usd,
                order.price,
                order.max_slippage_bps,
            )?
        };
        let exchange_client = if is_spot {
            self.exchange_client(None).await?
        } else {
            self.exchange_client_from_snapshot(
                perp_snapshot
                    .as_ref()
                    .context("missing perp snapshot for fast exchange client")?,
            )?
        };
        let cloid = uuid::Uuid::parse_str(&order.cloid)
            .with_context(|| format!("risk cloid {} is not a UUID", order.cloid))?;
        let tif = tif_for_policy(order.execution_policy);
        let request = ClientOrderRequest {
            asset: plan.coin.clone(),
            is_buy,
            reduce_only: order.reduce_only,
            limit_px: plan.limit_price,
            sz: plan.size,
            cloid: Some(cloid),
            order_type: ClientOrder::Limit(ClientLimit { tif }),
        };

        let payload = exchange_client
            .signed_bulk_order_payload_with_grouping(vec![request], None, "na")
            .context("failed to build signed websocket order payload")?;
        let response_payload = WsPostClient::for_environment(&effective_config.app.environment)
            .post_action(payload)
            .await
            .context("Hyperliquid websocket order post failed")?;
        let response: ExchangeResponseStatus = serde_json::from_value(response_payload)
            .context("failed to parse Hyperliquid websocket order response")?;

        response_to_worker_report(order, plan.limit_price, plan.size, response)
    }

    async fn submit_bulk_inner(&self, orders: Vec<ApprovedOrder>) -> Result<Vec<WorkerReport>> {
        anyhow::ensure!(
            !orders.is_empty(),
            "bulk submit requires at least one order"
        );
        let first = orders
            .first()
            .context("bulk submit requires at least one order")?;
        anyhow::ensure!(
            first.account_id == self.account.account_id,
            "order account {} does not match executor account {}",
            first.account_id,
            self.account.account_id
        );
        let first_dex = first.dex.clone();
        for order in &orders {
            anyhow::ensure!(
                order.account_id == self.account.account_id,
                "order account {} does not match executor account {}",
                order.account_id,
                self.account.account_id
            );
            anyhow::ensure!(
                order.dex == first_dex,
                "bulk submit requires all orders to use the same dex"
            );
        }

        let effective_config = self.config_for_order(first);
        let is_spot = is_spot_dex(&effective_config.hyperliquid.dex);
        let perp_snapshot = if is_spot {
            None
        } else {
            Some(
                fetch_xyz_market_snapshot_cached(
                    &effective_config.app.environment,
                    &effective_config.hyperliquid.dex,
                    15_000,
                )
                .await
                .context("failed to fetch perp market snapshot")?,
            )
        };
        let spot_snapshot = if is_spot {
            Some(
                fetch_spot_market_snapshot_cached(&effective_config.app.environment, 15_000)
                    .await
                    .context("failed to fetch spot market snapshot")?,
            )
        } else {
            None
        };

        let mut requests = Vec::with_capacity(orders.len());
        let mut report_orders = Vec::with_capacity(orders.len());
        for mut order in orders {
            let is_buy = matches!(order.side, OrderSide::Buy);
            let plan = if is_spot {
                let spot_snapshot = spot_snapshot
                    .as_ref()
                    .context("missing spot snapshot for order planning")?;
                if let Some(exact_size) = order.exact_size {
                    build_spot_order_plan_for_size(
                        spot_snapshot,
                        &order.coin,
                        order.side,
                        exact_size,
                        order.max_slippage_bps,
                    )?
                } else {
                    build_spot_order_plan(
                        spot_snapshot,
                        &order.coin,
                        is_buy,
                        order.notional_usd,
                        order.price,
                        order.max_slippage_bps,
                    )?
                }
            } else if let Some(exact_size) = order.exact_size {
                build_order_plan_for_size(
                    perp_snapshot
                        .as_ref()
                        .context("missing perp snapshot for order planning")?,
                    &order.coin,
                    order.side,
                    exact_size,
                    order.max_slippage_bps,
                )?
            } else {
                build_order_plan(
                    perp_snapshot
                        .as_ref()
                        .context("missing perp snapshot for order planning")?,
                    &order.coin,
                    is_buy,
                    order.notional_usd,
                    order.price,
                    order.max_slippage_bps,
                )?
            };

            let cloid = uuid::Uuid::parse_str(&order.cloid)
                .with_context(|| format!("risk cloid {} is not a UUID", order.cloid))?;
            let tif = tif_for_policy(order.execution_policy);
            requests.push(ClientOrderRequest {
                asset: plan.coin.clone(),
                is_buy,
                reduce_only: order.reduce_only,
                limit_px: plan.limit_price,
                sz: plan.size,
                cloid: Some(cloid),
                order_type: ClientOrder::Limit(ClientLimit { tif }),
            });
            order.price = Some(plan.limit_price);
            order.exact_size = Some(plan.size);
            report_orders.push(order);
        }

        let exchange_client = if is_spot {
            self.exchange_client(None).await?
        } else {
            self.exchange_client(perp_snapshot.as_ref()).await?
        };
        let response = exchange_client
            .bulk_order(requests, None)
            .await
            .context("Hyperliquid bulk order request failed")?;

        response_to_worker_reports(report_orders, &response)
    }

    fn config_for_order(&self, order: &ApprovedOrder) -> AppConfig {
        let mut config = self.config.clone();
        if let Some(dex) = order.dex.as_deref() {
            config.hyperliquid.dex = dex.to_string();
        }
        config
    }

    pub async fn submit_protective_trigger_orders(
        &self,
        plan: &ProtectiveExitPlanResult,
    ) -> Result<Vec<WorkerReport>> {
        anyhow::ensure!(
            plan.account_id == self.account.account_id,
            "protective plan account {} does not match executor account {}",
            plan.account_id,
            self.account.account_id
        );
        anyhow::ensure!(!plan.legs.is_empty(), "protective plan has no trigger legs");

        let is_spot = is_spot_dex(&self.config.hyperliquid.dex);
        let snapshot = if is_spot {
            None
        } else {
            Some(
                fetch_xyz_market_snapshot_cached(
                    &self.config.app.environment,
                    &self.config.hyperliquid.dex,
                    15_000,
                )
                .await
                .context("failed to fetch XYZ market snapshot")?,
            )
        };
        let exchange_client = if is_spot {
            self.exchange_client(None).await?
        } else {
            self.exchange_client_from_snapshot(
                snapshot
                    .as_ref()
                    .context("missing snapshot for fast protective exchange client")?,
            )?
        };
        let is_buy = matches!(plan.exit_side, OrderSide::Buy);
        self.cancel_existing_protective_trigger_orders(&exchange_client, &plan.coin)
            .await?;

        let now = now_ms();
        let mut approved_orders = Vec::with_capacity(plan.legs.len());
        let mut requests = Vec::with_capacity(plan.legs.len());
        for leg in &plan.legs {
            let tpsl = match leg.kind.as_str() {
                "take_profit" => "tp",
                "stop_loss" => "sl",
                _ => anyhow::bail!("unsupported protective leg kind {}", leg.kind),
            };
            let cloid = uuid::Uuid::parse_str(&leg.cloid)
                .with_context(|| format!("protective cloid {} is not a UUID", leg.cloid))?;
            requests.push(ClientOrderRequest {
                asset: plan.coin.clone(),
                is_buy,
                reduce_only: true,
                limit_px: leg.limit_price,
                sz: leg.size,
                cloid: Some(cloid),
                order_type: ClientOrder::Trigger(ClientTrigger {
                    is_market: false,
                    trigger_px: leg.trigger_price,
                    tpsl: tpsl.to_string(),
                }),
            });
            approved_orders.push(ApprovedOrder {
                risk_decision_id: format!("protective-exit-arm-risk-{}-{}", leg.kind, now),
                intent_id: format!("protective-exit-arm-intent-{}-{}", leg.kind, now),
                signal_id: Some(format!("protective-exit-arm-signal-{}-{}", leg.kind, now)),
                worker_id: format!("worker-{}", self.account.account_id),
                account_id: self.account.account_id.clone(),
                strategy_id: "manual_protective_exit".to_string(),
                market: None,
                dex: Some(self.config.hyperliquid.dex.clone()),
                coin: plan.coin.clone(),
                side: plan.exit_side,
                notional_usd: leg.size * leg.limit_price,
                exact_size: Some(leg.size),
                price: Some(leg.limit_price),
                execution_mode: ExecutionMode::Taker,
                execution_policy: ExecutionPolicy::Taker,
                max_slippage_bps: 0.0,
                reduce_only: true,
                cloid: leg.cloid.clone(),
                expires_at_ms: Some(now + self.config.process.signal_ttl_ms),
            });
        }

        let grouping = if is_spot {
            // Spot supports native trigger orders, but there is no perp-style position object.
            // Use normal TP/SL grouping instead of position-bound TP/SL grouping.
            "normalTpsl"
        } else {
            "positionTpsl"
        };
        let response = exchange_client
            .bulk_order_with_grouping(requests, None, grouping)
            .await
            .context("Hyperliquid protective trigger order request failed")?;
        response_to_worker_reports(approved_orders, &response)
    }

    pub async fn submit_protective_trigger_orders_fast(
        &self,
        plan: &ProtectiveExitPlanResult,
    ) -> Result<Vec<WorkerReport>> {
        anyhow::ensure!(
            plan.account_id == self.account.account_id,
            "protective plan account {} does not match executor account {}",
            plan.account_id,
            self.account.account_id
        );
        anyhow::ensure!(!plan.legs.is_empty(), "protective plan has no trigger legs");

        let is_spot = is_spot_dex(&self.config.hyperliquid.dex);
        let snapshot = if is_spot {
            None
        } else {
            Some(
                fetch_xyz_market_snapshot_cached(
                    &self.config.app.environment,
                    &self.config.hyperliquid.dex,
                    15_000,
                )
                .await
                .context("failed to fetch XYZ market snapshot")?,
            )
        };
        let exchange_client = if is_spot {
            self.exchange_client(None).await?
        } else {
            self.exchange_client_from_snapshot(
                snapshot
                    .as_ref()
                    .context("missing snapshot for fast protective exchange client")?,
            )?
        };
        let is_buy = matches!(plan.exit_side, OrderSide::Buy);

        let now = now_ms();
        let mut approved_orders = Vec::with_capacity(plan.legs.len());
        let mut requests = Vec::with_capacity(plan.legs.len());
        for leg in &plan.legs {
            let tpsl = match leg.kind.as_str() {
                "take_profit" => "tp",
                "stop_loss" => "sl",
                _ => anyhow::bail!("unsupported protective leg kind {}", leg.kind),
            };
            let cloid = uuid::Uuid::parse_str(&leg.cloid)
                .with_context(|| format!("protective cloid {} is not a UUID", leg.cloid))?;
            requests.push(ClientOrderRequest {
                asset: plan.coin.clone(),
                is_buy,
                reduce_only: true,
                limit_px: leg.limit_price,
                sz: leg.size,
                cloid: Some(cloid),
                order_type: ClientOrder::Trigger(ClientTrigger {
                    is_market: false,
                    trigger_px: leg.trigger_price,
                    tpsl: tpsl.to_string(),
                }),
            });
            approved_orders.push(ApprovedOrder {
                risk_decision_id: format!("protective-exit-arm-risk-{}-{}", leg.kind, now),
                intent_id: format!("protective-exit-arm-intent-{}-{}", leg.kind, now),
                signal_id: Some(format!("protective-exit-arm-signal-{}-{}", leg.kind, now)),
                worker_id: format!("worker-{}", self.account.account_id),
                account_id: self.account.account_id.clone(),
                strategy_id: "manual_protective_exit".to_string(),
                market: None,
                dex: Some(self.config.hyperliquid.dex.clone()),
                coin: plan.coin.clone(),
                side: plan.exit_side,
                notional_usd: leg.size * leg.limit_price,
                exact_size: Some(leg.size),
                price: Some(leg.limit_price),
                execution_mode: ExecutionMode::Taker,
                execution_policy: ExecutionPolicy::Taker,
                max_slippage_bps: 0.0,
                reduce_only: true,
                cloid: leg.cloid.clone(),
                expires_at_ms: Some(now + self.config.process.signal_ttl_ms),
            });
        }

        let grouping = if is_spot {
            "normalTpsl"
        } else {
            "positionTpsl"
        };
        let payload = exchange_client
            .signed_bulk_order_payload_with_grouping(requests, None, grouping)
            .context("failed to build signed websocket protective order payload")?;
        let response_payload = WsPostClient::for_environment(&self.config.app.environment)
            .post_action(payload)
            .await
            .context("Hyperliquid websocket protective order post failed")?;
        let response: ExchangeResponseStatus = serde_json::from_value(response_payload)
            .context("failed to parse Hyperliquid websocket protective order response")?;
        response_to_worker_reports(approved_orders, &response)
    }

    async fn cancel_existing_protective_trigger_orders(
        &self,
        exchange_client: &ExchangeClient,
        coin: &str,
    ) -> Result<()> {
        let open_orders = fetch_open_orders(
            &self.config.app.environment,
            &self.config.hyperliquid.dex,
            &self.account.address,
        )
        .await
        .context("failed to fetch open orders before protective TP/SL replace")?;

        let mut cancels = Vec::new();
        for order in open_orders {
            if is_native_protective_open_order(&order, coin) {
                cancels.push(ClientCancelRequest {
                    asset: normalize_order_coin_for_cancel(
                        &self.config.hyperliquid.dex,
                        &order.coin,
                    ),
                    oid: order.oid,
                });
                continue;
            }
            if !order.reduce_only {
                continue;
            }
            let status = match fetch_order_status_by_oid(
                &self.config.app.environment,
                &self.account.address,
                order.oid,
            )
            .await
            {
                Ok(status) => status,
                Err(error) => {
                    tracing::warn!(
                        account_id = %self.account.account_id,
                        oid = order.oid,
                        error = %error,
                        "failed to enrich open order status while replacing protective TP/SL; skipping this candidate"
                    );
                    continue;
                }
            };
            if protective_order_status_matches_coin(&status, coin) {
                let asset_coin = status
                    .order
                    .as_ref()
                    .map(|entry| entry.order.coin.clone())
                    .unwrap_or_else(|| order.coin.clone());
                cancels.push(ClientCancelRequest {
                    asset: normalize_order_coin_for_cancel(
                        &self.config.hyperliquid.dex,
                        &asset_coin,
                    ),
                    oid: order.oid,
                });
            }
        }

        if cancels.is_empty() {
            return Ok(());
        }

        exchange_client
            .bulk_cancel(cancels, None)
            .await
            .context("failed to cancel existing protective TP/SL trigger orders")?;
        Ok(())
    }

    pub async fn cancel_by_cloid(&self, coin: &str, cloid: &str) -> Result<ExchangeResponseStatus> {
        let is_spot = is_spot_dex(&self.config.hyperliquid.dex);
        let canonical = if is_spot {
            normalize_spot_coin(coin)
        } else {
            normalize_dex_coin(&self.config.hyperliquid.dex, coin)
        };
        let exchange_client = if is_spot {
            self.exchange_client(None).await?
        } else {
            let snapshot = fetch_xyz_market_snapshot_cached(
                &self.config.app.environment,
                &self.config.hyperliquid.dex,
                15_000,
            )
            .await
            .context("failed to fetch XYZ market snapshot")?;
            self.exchange_client_from_snapshot(&snapshot)?
        };
        let cloid =
            uuid::Uuid::parse_str(cloid).with_context(|| format!("invalid cloid {cloid}"))?;
        let mut attempt = 1;
        loop {
            let result = exchange_client
                .cancel_by_cloid(
                    ClientCancelRequestCloid {
                        asset: canonical.clone(),
                        cloid,
                    },
                    None,
                )
                .await
                .context("Hyperliquid cancel_by_cloid failed");
            match result {
                Ok(response) => return Ok(response),
                Err(error)
                    if exchange_action_error_is_retryable(&error)
                        && attempt < EXCHANGE_ACTION_MAX_ATTEMPTS =>
                {
                    let delay_ms = exchange_action_retry_delay_ms(attempt);
                    tracing::warn!(
                        %attempt,
                        max_attempts = EXCHANGE_ACTION_MAX_ATTEMPTS,
                        %delay_ms,
                        error = %format_anyhow_for_log(&error),
                        "Hyperliquid cancel_by_cloid failed; retrying with action backoff"
                    );
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    attempt += 1;
                }
                Err(error) => return Err(simplify_exchange_action_error(error)),
            }
        }
    }

    pub async fn cancel_by_oid(&self, coin: &str, oid: u64) -> Result<ExchangeResponseStatus> {
        let is_spot = is_spot_dex(&self.config.hyperliquid.dex);
        let canonical = normalize_order_coin_for_cancel(&self.config.hyperliquid.dex, coin);
        let exchange_client = if is_spot {
            self.exchange_client(None).await?
        } else {
            let snapshot = fetch_xyz_market_snapshot_cached(
                &self.config.app.environment,
                &self.config.hyperliquid.dex,
                15_000,
            )
            .await
            .context("failed to fetch XYZ market snapshot")?;
            self.exchange_client(Some(&snapshot)).await?
        };
        let mut attempt = 1;
        loop {
            let result = exchange_client
                .bulk_cancel(
                    vec![ClientCancelRequest {
                        asset: canonical.clone(),
                        oid,
                    }],
                    None,
                )
                .await
                .context("Hyperliquid cancel_by_oid failed");
            match result {
                Ok(response) => return Ok(response),
                Err(error)
                    if exchange_action_error_is_retryable(&error)
                        && attempt < EXCHANGE_ACTION_MAX_ATTEMPTS =>
                {
                    let delay_ms = exchange_action_retry_delay_ms(attempt);
                    tracing::warn!(
                        %attempt,
                        max_attempts = EXCHANGE_ACTION_MAX_ATTEMPTS,
                        %delay_ms,
                        error = %format_anyhow_for_log(&error),
                        "Hyperliquid cancel_by_oid failed; retrying with action backoff"
                    );
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    attempt += 1;
                }
                Err(error) => return Err(simplify_exchange_action_error(error)),
            }
        }
    }

    pub async fn cancel_open_orders_for_coin_fast(
        &self,
        coin: &str,
        realtime: Option<&RealtimeState>,
    ) -> Result<Option<String>> {
        let market_id = market_id_for_dex(&self.config.hyperliquid.dex);
        let open_orders = if let Some(orders) =
            realtime.and_then(|realtime| realtime.open_orders(market_id, &self.account.address))
        {
            orders
        } else {
            let query_dex = info_query_dex(&self.config.hyperliquid.dex);
            fetch_open_orders(
                &self.config.app.environment,
                &query_dex,
                &self.account.address,
            )
            .await
            .context("failed to fetch open orders for fast cancel fallback")?
        };
        let cancels = open_orders
            .into_iter()
            .filter(|order| order_coin_matches(&self.config.hyperliquid.dex, &order.coin, coin))
            .map(|order| ClientCancelRequest {
                asset: normalize_order_coin_for_cancel(&self.config.hyperliquid.dex, &order.coin),
                oid: order.oid,
            })
            .collect::<Vec<_>>();
        if cancels.is_empty() {
            return Ok(None);
        }

        let is_spot = is_spot_dex(&self.config.hyperliquid.dex);
        let exchange_client = if is_spot {
            self.exchange_client(None).await?
        } else {
            let snapshot = fetch_xyz_market_snapshot_cached(
                &self.config.app.environment,
                &self.config.hyperliquid.dex,
                15_000,
            )
            .await
            .context("failed to fetch XYZ market snapshot")?;
            self.exchange_client_from_snapshot(&snapshot)?
        };
        let payload = exchange_client
            .signed_bulk_cancel_payload(cancels, None)
            .context("failed to build signed websocket cancel payload")?;
        let response_payload = WsPostClient::for_environment(&self.config.app.environment)
            .post_action(payload)
            .await
            .context("Hyperliquid websocket cancel post failed")?;
        let response: ExchangeResponseStatus = serde_json::from_value(response_payload)
            .context("failed to parse Hyperliquid websocket cancel response")?;
        Ok(Some(format!("{response:?}")))
    }

    pub async fn update_leverage(
        &self,
        coin: &str,
        leverage: u32,
        is_cross: bool,
    ) -> Result<ExchangeResponseStatus> {
        let snapshot = fetch_xyz_market_snapshot_cached(
            &self.config.app.environment,
            &self.config.hyperliquid.dex,
            15_000,
        )
        .await
        .context("failed to fetch XYZ market snapshot")?;
        let canonical = normalize_dex_coin(&self.config.hyperliquid.dex, coin);
        let exchange_client = self.exchange_client(Some(&snapshot)).await?;
        exchange_client
            .update_leverage(leverage, &canonical, is_cross, None)
            .await
            .context("Hyperliquid update_leverage failed")
    }

    async fn exchange_client(
        &self,
        snapshot: Option<&XyzMarketSnapshot>,
    ) -> Result<ExchangeClient> {
        let wallet: LocalWallet = self.secret.private_key.parse().with_context(|| {
            format!(
                "failed to parse API wallet private key for account {} (secret_id {})",
                self.account.account_id, self.secret.secret_id
            )
        })?;
        let base_url = sdk_base_url(&self.config.app.environment)?;
        const MAX_ATTEMPTS: usize = 6;
        let mut backoff_ms = 500_u64;
        for attempt in 1..=MAX_ATTEMPTS {
            let init = ExchangeClient::new(
                None,
                wallet.clone(),
                Some(base_url),
                snapshot.map(|snapshot| snapshot.sdk_meta()),
                None,
            )
            .await;
            match init {
                Ok(mut exchange_client) => {
                    if let Some(snapshot) = snapshot {
                        exchange_client.coin_to_asset = snapshot.coin_to_asset.clone();
                    }
                    return Ok(exchange_client);
                }
                Err(error) => {
                    if attempt == MAX_ATTEMPTS {
                        return Err(error).with_context(|| {
                            format!(
                                "failed to initialize Hyperliquid exchange client for account {} (secret_id {}) after {MAX_ATTEMPTS} attempts",
                                self.account.account_id, self.secret.secret_id
                            )
                        });
                    }
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(4_000);
                }
            }
        }
        unreachable!("exchange client retry loop should always return before reaching this point")
    }

    fn exchange_client_from_snapshot(
        &self,
        snapshot: &XyzMarketSnapshot,
    ) -> Result<ExchangeClient> {
        let wallet: LocalWallet = self.secret.private_key.parse().with_context(|| {
            format!(
                "failed to parse API wallet private key for account {} (secret_id {})",
                self.account.account_id, self.secret.secret_id
            )
        })?;
        let base_url = sdk_base_url(&self.config.app.environment)?;
        Ok(ExchangeClient::new_with_asset_map(
            None,
            wallet,
            Some(base_url),
            snapshot.sdk_meta(),
            snapshot.coin_to_asset.clone(),
            None,
        ))
    }
}

#[derive(Debug, Clone)]
pub struct SignedSmokeOptions {
    pub account_id: String,
    pub coin: String,
    pub side: OrderSide,
    pub notional_usd: f64,
    pub max_slippage_bps: f64,
    pub execution_mode: ExecutionMode,
    pub reduce_only: bool,
    pub close_full_position: bool,
    pub submit: bool,
    pub cancel_resting: bool,
    pub confirm_mainnet_live: bool,
}

#[derive(Debug, Clone)]
pub struct ManualLeverageUpdateOptions {
    pub account_id: String,
    pub coin: String,
    pub leverage: u32,
    pub margin_mode: String,
    pub submit: bool,
    pub confirm_mainnet_live: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ManualLeverageUpdateResult {
    pub environment: String,
    pub dex: String,
    pub account_id: String,
    pub coin: String,
    pub leverage: u32,
    pub margin_mode: String,
    pub is_cross: bool,
    pub max_leverage: Option<u32>,
    pub only_isolated: Option<bool>,
    pub exchange_margin_mode: Option<String>,
    pub submit_requested: bool,
    pub submitted: bool,
    pub exchange_response: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SignedAcceptanceOptions {
    pub account_id: String,
    pub coin: String,
    pub side: OrderSide,
    pub notional_usd: f64,
    pub max_slippage_bps: f64,
    pub execution_mode: ExecutionMode,
    pub reduce_only: bool,
    pub close_full_position: bool,
    pub submit: bool,
    pub cancel_resting: bool,
    pub confirm_mainnet_live: bool,
}

#[derive(Debug, Clone)]
pub struct SignedPreflightOptions {
    pub account_id: String,
    pub coin: String,
    pub side: OrderSide,
    pub notional_usd: f64,
    pub max_slippage_bps: f64,
    pub execution_mode: ExecutionMode,
    pub reduce_only: bool,
    pub close_full_position: bool,
    pub confirm_mainnet_live: bool,
}

#[derive(Debug, Clone)]
pub struct SignedRunbookOptions {
    pub account_id: String,
    pub coin: String,
    pub side: OrderSide,
    pub notional_usd: f64,
    pub max_slippage_bps: f64,
    pub execution_mode: ExecutionMode,
    pub reduce_only: bool,
    pub close_full_position: bool,
    pub submit: bool,
    pub cancel_resting: bool,
    pub confirm_mainnet_live: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SignedSmokePlanReport {
    pub environment: String,
    pub account_id: String,
    pub coin: String,
    pub asset_id: u32,
    pub reference_price: f64,
    pub limit_price: f64,
    pub size: f64,
    pub effective_notional_usd: f64,
    pub minimum_requested_notional_usd: Option<f64>,
    pub sz_decimals: u32,
    pub execution_mode: ExecutionMode,
    pub tif: String,
    pub reduce_only: bool,
    pub submit: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SignedSmokeReconciliation {
    pub open_orders: usize,
    pub matching_open: bool,
    pub xyz_fills: usize,
    pub matching_fills: usize,
    pub order_status: Option<OrderStatusResponse>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SignedSmokeResult {
    pub plan: SignedSmokePlanReport,
    pub submit_report: Option<WorkerReport>,
    pub cancel_response: Option<String>,
    pub reconciliation: Option<SignedSmokeReconciliation>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FastSignedOrderResult {
    pub environment: String,
    pub account_id: String,
    pub coin: String,
    pub transport: String,
    pub submit_requested: bool,
    pub submitted: bool,
    pub submit_latency_ms: Option<u64>,
    pub plan: SignedSmokePlanReport,
    pub submit_report: Option<WorkerReport>,
    pub cancel_response: Option<String>,
    pub cache_notes: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FastProtectiveExitArmResult {
    pub environment: String,
    pub account_id: String,
    pub coin: String,
    pub transport: String,
    pub submit_requested: bool,
    pub submitted: bool,
    pub submit_latency_ms: Option<u64>,
    pub plan: ProtectiveExitPlanResult,
    pub submit_reports: Vec<WorkerReport>,
    pub cache_notes: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CancelByCloidResult {
    pub environment: String,
    pub account_id: String,
    pub coin: String,
    pub cloid: String,
    pub cancel_response: String,
    pub order_status_after: Option<OrderStatusResponse>,
    pub open_orders_after: usize,
    pub matching_open_after: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct CancelOpenOrderResult {
    pub environment: String,
    pub account_id: String,
    pub coin: String,
    pub cloid: Option<String>,
    pub oid: Option<u64>,
    pub cancel_response: String,
    pub open_orders_after: usize,
    pub matching_open_after: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SignedAcceptanceResult {
    pub environment: String,
    pub account_id: String,
    pub coin: String,
    pub submitted: bool,
    pub plan: SignedSmokePlanReport,
    pub pre_submit: SignedAcceptanceAccountSnapshot,
    pub signed_smoke: Option<SignedSmokeResult>,
    pub post_submit: Option<SignedAcceptanceAccountSnapshot>,
    pub checks: Vec<SignedAcceptanceCheck>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SignedAcceptanceAccountSnapshot {
    pub open_order_count: usize,
    pub fill_count: usize,
    pub request_capacity_remaining: i64,
    pub rate_limit: UserRateLimit,
}

#[derive(Debug, Clone, Serialize)]
pub struct SignedAcceptanceCheck {
    pub name: String,
    pub ok: bool,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SignedPreflightResult {
    pub environment: String,
    pub dry_run: bool,
    pub account_id: String,
    pub coin: String,
    pub notional_usd: f64,
    pub execution_mode: ExecutionMode,
    pub reduce_only: bool,
    pub ready_for_testnet_submit: bool,
    pub ready_for_mainnet_submit: bool,
    pub rate_limit: Option<UserRateLimit>,
    pub account_state: Option<AccountReadinessState>,
    pub plan: Option<SignedSmokePlanReport>,
    pub checks: Vec<SignedPreflightCheck>,
    pub failed_blockers: Vec<String>,
    pub next_actions: Vec<String>,
    pub readiness_summary: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SignedPreflightCheck {
    pub name: String,
    pub ok: bool,
    pub severity: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccountReadinessState {
    pub account_value_usd: f64,
    pub withdrawable_usd: f64,
    pub total_notional_position_usd: f64,
    pub total_margin_used_usd: f64,
    pub coin_position_size: f64,
    pub coin_position_value_usd: f64,
    pub coin_unrealized_pnl_usd: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccountReconciliationReport {
    pub environment: String,
    pub dex: String,
    pub account_id: String,
    pub address: String,
    pub rate_limit: UserRateLimit,
    pub clearinghouse_state: ClearinghouseState,
    pub open_order_count: usize,
    pub fill_count: usize,
    pub open_orders: Vec<OpenOrder>,
    pub recent_fills: Vec<UserFill>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrderStatusReport {
    pub environment: String,
    pub dex: String,
    pub account_id: String,
    pub address: String,
    pub query: OrderStatusLookup,
    pub order_status: OrderStatusResponse,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OrderStatusLookup {
    Oid { oid: u64 },
    Cloid { cloid: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct SignedRunbookResult {
    pub environment: String,
    pub account_id: String,
    pub coin: String,
    pub submit_requested: bool,
    pub submitted: bool,
    pub preflight: SignedPreflightResult,
    pub pre_submit_reconciliation: Option<AccountReconciliationReport>,
    pub acceptance: Option<SignedAcceptanceResult>,
    pub post_submit_reconciliation: Option<AccountReconciliationReport>,
    pub order_status_checks: Vec<OrderStatusReport>,
    pub checks: Vec<SignedRunbookCheck>,
}

#[derive(Debug, Clone)]
pub struct SignedLiveWindowOptions {
    pub account_ids: Vec<String>,
    pub coin: String,
    pub side: OrderSide,
    pub notional_usd: f64,
    pub max_slippage_bps: f64,
    pub execution_mode: ExecutionMode,
    pub output_config_path: PathBuf,
    pub write: bool,
    pub overwrite: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SignedLiveWindowResult {
    pub environment: String,
    pub source_config_path: String,
    pub output_config_path: String,
    pub config_written: bool,
    pub coin: String,
    pub side: OrderSide,
    pub notional_usd: f64,
    pub max_slippage_bps: f64,
    pub execution_mode: ExecutionMode,
    pub account_ids: Vec<String>,
    pub required_config_changes: Vec<String>,
    pub accounts: Vec<SignedLiveWindowAccount>,
    pub next_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SignedLiveWindowAccount {
    pub account_id: String,
    pub address: String,
    pub secret_id: String,
    pub preflight_args: Vec<String>,
    pub submit_runbook_args: Vec<String>,
    pub reduce_only_close_runbook_args: Vec<String>,
    pub reconcile_args: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct MainnetSmokePlanOptions {
    pub account_ids: Vec<String>,
    pub funding_amount_usdc: f64,
    pub destination_dex: Option<String>,
    pub coin: String,
    pub side: OrderSide,
    pub order_notional_usd: f64,
    pub max_slippage_bps: f64,
    pub execution_mode: ExecutionMode,
    pub transfer_output_config_path: PathBuf,
    pub order_output_config_path: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub struct MainnetSmokePlanResult {
    pub environment: String,
    pub dex: String,
    pub account_ids: Vec<String>,
    pub funding_amount_usdc: f64,
    pub order_notional_usd: f64,
    pub coin: String,
    pub side: OrderSide,
    pub execution_mode: ExecutionMode,
    pub funding: AccountFundingBatchReport,
    pub transfer_preflight: UsdcDexTransferBatchPreflightResult,
    pub transfer_live_window: UsdcDexTransferLiveWindowResult,
    pub signed_live_window: SignedLiveWindowResult,
    pub ready_for_funding_submit: bool,
    pub ready_for_order_submit: bool,
    pub stop_reasons: Vec<String>,
    pub next_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SignedRunbookCheck {
    pub name: String,
    pub ok: bool,
    pub detail: String,
}

impl SignedPreflightCheck {
    fn blocker(name: impl Into<String>, ok: bool, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ok,
            severity: "blocker".to_string(),
            detail: detail.into(),
        }
    }
}

fn failed_preflight_blockers(checks: &[SignedPreflightCheck]) -> Vec<String> {
    checks
        .iter()
        .filter(|check| check.severity == "blocker" && !check.ok)
        .map(|check| format!("{}: {}", check.name, check.detail))
        .collect()
}

fn failed_preflight_check(checks: &[SignedPreflightCheck], name: &str) -> bool {
    checks
        .iter()
        .any(|check| check.severity == "blocker" && !check.ok && check.name == name)
}

fn preflight_next_actions(checks: &[SignedPreflightCheck], reduce_only: bool) -> Vec<String> {
    let mut actions = Vec::new();
    let mut push_unique = |action: &str| {
        if !actions.iter().any(|existing| existing == action) {
            actions.push(action.to_string());
        }
    };

    if failed_preflight_check(checks, "account_configured")
        || failed_preflight_check(checks, "account_enabled")
        || failed_preflight_check(checks, "address_not_placeholder")
    {
        push_unique(
            "Fix the selected account in config/local.toml or save it again through the Vault page.",
        );
    }
    if failed_preflight_check(checks, "config_dry_run_disabled") {
        push_unique("Set app.dry_run=false only for the intended testnet live smoke window.");
    }
    if failed_preflight_check(checks, "manual_live_enabled") {
        push_unique(
            "Set manual_ops.manual_live_enabled=true for the intended testnet live smoke window.",
        );
    }
    if failed_preflight_check(checks, "mainnet_gate") {
        push_unique(
            "Do not continue on mainnet until manual_ops.mainnet_live_enabled=true and the explicit mainnet confirmation gate are both present.",
        );
    }
    if failed_preflight_check(checks, "global_kill_switch_clear") {
        push_unique(
            "Clear risk.global.kill_switch for opening orders, or use reduce-only only when that policy is enabled.",
        );
    }
    if failed_preflight_check(checks, "manual_notional_limit")
        || failed_preflight_check(checks, "account_notional_limit")
    {
        push_unique(
            "Lower the order notional or raise the matching account/manual notional cap deliberately.",
        );
    }
    if failed_preflight_check(checks, "exchange_min_order_notional") {
        push_unique(
            "Do not submit the opening order below the exchange minimum; use at least 10 USD only after explicit approval, or use a supported close path solely to exit an existing position/inventory.",
        );
    }
    if failed_preflight_check(checks, "exchange_min_order_notional_effective") {
        push_unique(
            "Increase requested notional to at least the recommendation in exchange_min_order_notional_effective, then verify it stays within account/manual caps.",
        );
    }
    if failed_preflight_check(checks, "symbol_allowed") {
        push_unique(
            "Remove the canonical symbol from manual_ops.blocked_symbols or choose another XYZ market.",
        );
    }
    if failed_preflight_check(checks, "vault_file_exists") {
        push_unique(
            "Create or restore secrets/trade_xyz.vault from the Vault page before signed submit.",
        );
    }
    if failed_preflight_check(checks, "vault_password_available")
        || failed_preflight_check(checks, "vault_unlocked")
        || failed_preflight_check(checks, "api_wallet_secret_available")
    {
        push_unique(
            "Unlock the Vault in the current frontend process, or set TRADE_XYZ_VAULT_PASSWORD for the CLI run, then test the API wallet secret.",
        );
    }
    if failed_preflight_check(checks, "user_rate_limit_available")
        || failed_preflight_check(checks, "user_rate_limit_has_capacity")
    {
        push_unique("Wait for Hyperliquid request capacity to recover, then rerun preflight.");
    }
    if failed_preflight_check(checks, "clearinghouse_state_available") {
        push_unique(
            "Restore read access to Hyperliquid clearinghouseState before any signed submit.",
        );
    }
    if failed_preflight_check(checks, "account_has_available_collateral") {
        push_unique(
            "Fund or transfer USDC into the selected trading layer before opening a position.",
        );
    }
    if failed_preflight_check(checks, "reduce_only_position_available") && reduce_only {
        push_unique(
            "Select an account with a matching reducible position or sellable spot inventory, or create the opening testnet position first.",
        );
    }
    if failed_preflight_check(checks, "signed_order_plan_valid") {
        push_unique(
            "Choose a symbol/notional combination that produces a valid precision-rounded XYZ perp order plan.",
        );
    }

    actions
}

fn preflight_readiness_summary(
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

fn usdc_transfer_amount_label(amount: f64) -> String {
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

fn usdc_transfer_confirmation_phrase(
    account_id: &str,
    amount_usdc: f64,
    destination_dex: &str,
) -> String {
    let destination_label = usdc_transfer_layer_label(destination_dex);
    format!(
        "TRANSFER {} USDC TO {} FOR {}",
        usdc_transfer_amount_label(amount_usdc),
        destination_label,
        account_id
    )
}

fn normalized_transfer_destination_account_id(account_id: &str, raw: Option<&str>) -> String {
    let destination = raw.unwrap_or_default().trim();
    if destination.is_empty() || destination == "__same__" {
        account_id.to_string()
    } else {
        destination.to_string()
    }
}

fn usdc_transfer_layer_label(layer: &str) -> String {
    let trimmed = layer.trim();
    if trimmed.is_empty() {
        "default_perp".to_string()
    } else {
        trimmed.to_string()
    }
}

fn normalize_transfer_layer(raw: &str) -> String {
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

fn usdc_transfer_layer_supported(layer: &str) -> bool {
    let canonical = normalize_transfer_layer(layer);
    if canonical.is_empty() || canonical == "spot" {
        return true;
    }
    canonical
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
}

fn order_side_cli_arg(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn opposite_order_side(side: OrderSide) -> OrderSide {
    match side {
        OrderSide::Buy => OrderSide::Sell,
        OrderSide::Sell => OrderSide::Buy,
    }
}

fn execution_mode_cli_arg(mode: ExecutionMode) -> &'static str {
    match mode {
        ExecutionMode::Taker => "taker",
        ExecutionMode::Maker => "maker",
    }
}

fn usdc_transfer_preflight_next_actions(
    checks: &[SignedPreflightCheck],
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

    if failed_preflight_check(checks, "account_configured")
        || failed_preflight_check(checks, "account_enabled")
        || failed_preflight_check(checks, "address_not_placeholder")
    {
        push_unique(
            "Fix the selected account in config/local.toml or save it again through the Vault page."
                .to_string(),
        );
    }
    if failed_preflight_check(checks, "destination_account_configured")
        || failed_preflight_check(checks, "destination_account_enabled")
        || failed_preflight_check(checks, "destination_address_not_placeholder")
    {
        push_unique(
            "Fix the destination account in config/local.toml or save it again through the Vault page."
                .to_string(),
        );
    }
    if failed_preflight_check(checks, "amount_positive")
        || failed_preflight_check(checks, "amount_within_helper_cap")
    {
        push_unique("Use a positive transfer amount at or below 10 USDC per account.".to_string());
    }
    if failed_preflight_check(checks, "source_layer_supported") {
        push_unique(
            "Set source layer to default_perp, spot, or a valid perp dex name.".to_string(),
        );
    }
    if failed_preflight_check(checks, "destination_layer_supported") {
        push_unique(
            "Set destination layer to default_perp, spot, or a valid perp dex name.".to_string(),
        );
    }
    if failed_preflight_check(checks, "route_changes_state") {
        push_unique(format!(
            "Choose a non-identical route. Current route: {} -> {}.",
            usdc_transfer_layer_label(source_dex),
            usdc_transfer_layer_label(destination_dex)
        ));
    }
    if failed_preflight_check(checks, "config_dry_run_disabled") {
        push_unique(format!(
            "Set app.dry_run=false only for the approved {environment} USDC transfer window."
        ));
    }
    if failed_preflight_check(checks, "manual_live_enabled") {
        push_unique(format!(
            "Set manual_ops.manual_live_enabled=true only for the approved {environment} transfer window."
        ));
    }
    if failed_preflight_check(checks, "mainnet_gate") {
        push_unique(
            "Set manual_ops.mainnet_live_enabled=true only after confirming the exact mainnet transfer amount."
                .to_string(),
        );
    }
    if failed_preflight_check(checks, "mainnet_explicit_confirmation") {
        if let Some(phrase) = confirmation_phrase {
            push_unique(format!(
                "For mainnet submit, type the exact confirmation phrase: {phrase}"
            ));
        } else {
            push_unique("Provide the explicit mainnet confirmation before submit.".to_string());
        }
    }
    if failed_preflight_check(checks, "vault_file_exists") {
        push_unique(
            "Create or restore secrets/trade_xyz.vault before signed transfer.".to_string(),
        );
    }
    if failed_preflight_check(checks, "vault_password_available")
        || failed_preflight_check(checks, "evm_transfer_signer_available")
    {
        push_unique(
            "Set TRADE_XYZ_VAULT_PASSWORD for CLI transfer preflight/submit, or unlock the Vault in the frontend; ensure the configured transfer signer is the EVM account wallet."
                .to_string(),
        );
    }
    if failed_preflight_check(checks, "user_rate_limit_available")
        || failed_preflight_check(checks, "user_rate_limit_has_capacity")
    {
        push_unique(
            "Wait for Hyperliquid request capacity to recover, then rerun transfer preflight."
                .to_string(),
        );
    }
    if failed_preflight_check(checks, "source_layer_available_sufficient")
        || failed_preflight_check(checks, "transfer_plan_valid")
    {
        push_unique(
            "Rerun Funding Check and lower the transfer amount if source-layer available USDC is insufficient."
                .to_string(),
        );
    }

    actions
}

fn usdc_transfer_preflight_summary(
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

fn usdc_transfer_batch_preflight_next_actions(
    account_ids: &[String],
    ready_account_ids: &[String],
    blocked_account_ids: &[String],
    failed_account_ids: &[String],
    amount_usdc: f64,
    source_dex: &str,
    destination_dex: &str,
) -> Vec<String> {
    let amount_label = usdc_transfer_amount_label(amount_usdc);
    let source_label = usdc_transfer_layer_label(source_dex);
    let destination_label = usdc_transfer_layer_label(destination_dex);
    let mut actions = Vec::new();
    if !failed_account_ids.is_empty() {
        actions.push(format!(
            "Fix batch preflight errors for account(s): {}.",
            failed_account_ids.join(", ")
        ));
    }
    if !blocked_account_ids.is_empty() {
        actions.push(format!(
            "Clear per-account blockers before transfer submit: {}.",
            blocked_account_ids.join(", ")
        ));
        actions.push(
            "Do not submit any USDC movement until every intended account appears in ready_account_ids."
                .to_string(),
        );
    }
    if ready_account_ids.len() == account_ids.len() && !account_ids.is_empty() {
        actions.push(format!(
            "All selected accounts are ready for {} USDC {} -> {} transfer preflight.",
            amount_label, source_label, destination_label
        ));
        actions.push(
            "Run usdc-dex-transfer-live-window to generate the temporary live config, then run one usdc-dex-transfer-runbook per account and inspect JSON after each submit."
                .to_string(),
        );
    }
    if actions.is_empty() {
        actions.push(
            "Select at least one enabled account and rerun batch transfer preflight.".to_string(),
        );
    }
    actions
}

fn mainnet_smoke_plan_next_actions(
    ready_for_funding_submit: bool,
    ready_for_order_submit: bool,
    transfer_preflight: &UsdcDexTransferBatchPreflightResult,
    funding: &AccountFundingBatchReport,
) -> Vec<String> {
    let mut actions = Vec::new();
    if !ready_for_funding_submit {
        actions.push(
            "Clear funding transfer blockers before generating or using a live transfer config."
                .to_string(),
        );
        actions.extend(transfer_preflight.next_actions.iter().cloned());
    } else {
        actions.push(
            "Funding transfer preflight is ready; generate the transfer live window config only after explicit user approval."
                .to_string(),
        );
    }

    if !ready_for_order_submit {
        actions.push(format!(
            "Do not submit opening orders yet; transfer_needed_account_ids={:?}.",
            funding.transfer_needed_account_ids
        ));
        actions.push(
            "After funding transfer, rerun account-funding and this mainnet-smoke-plan command."
                .to_string(),
        );
    } else {
        actions.push(
            "XYZ perp collateral is visible for every selected account; run signed-live-window and per-account signed-runbook only after explicit order approval."
                .to_string(),
        );
    }
    actions
}

pub fn order_status_lookup(oid: Option<u64>, cloid: Option<String>) -> Result<OrderStatusLookup> {
    let cloid = cloid
        .as_deref()
        .map(str::trim)
        .filter(|cloid| !cloid.is_empty());
    match (oid, cloid) {
        (Some(oid), None) => Ok(OrderStatusLookup::Oid { oid }),
        (None, Some(cloid)) => {
            uuid::Uuid::parse_str(cloid)
                .with_context(|| format!("order-status cloid must be a valid UUID: {cloid}"))?;
            Ok(OrderStatusLookup::Cloid {
                cloid: cloid.to_string(),
            })
        }
        (None, None) => anyhow::bail!("order-status requires exactly one of --oid or --cloid"),
        (Some(_), Some(_)) => anyhow::bail!("order-status accepts only one of --oid or --cloid"),
    }
}

pub async fn reconcile_account(
    config: &AppConfig,
    account_id: &str,
) -> Result<AccountReconciliationReport> {
    let account = config
        .account(account_id)
        .with_context(|| format!("account {account_id} not found in config"))?;
    let query_dex = info_query_dex(&config.hyperliquid.dex);
    let open_orders = fetch_open_orders(&config.app.environment, &query_dex, &account.address)
        .await
        .context("failed to fetch open orders")?;
    let recent_fills = fetch_user_fills(&config.app.environment, &query_dex, &account.address)
        .await
        .context("failed to fetch user fills")?;
    let rate_limit = fetch_user_rate_limit(&config.app.environment, &account.address)
        .await
        .context("failed to fetch user rate limit")?;
    let clearinghouse_state = if is_spot_dex(&config.hyperliquid.dex) {
        fetch_default_clearinghouse_state(&config.app.environment, &account.address)
            .await
            .context("failed to fetch default perps clearinghouse state for spot reconciliation")?
    } else {
        fetch_clearinghouse_state(
            &config.app.environment,
            &config.hyperliquid.dex,
            &account.address,
        )
        .await
        .context("failed to fetch clearinghouse state")?
    };

    Ok(AccountReconciliationReport {
        environment: config.app.environment.clone(),
        dex: config.hyperliquid.dex.clone(),
        account_id: account.account_id.clone(),
        address: account.address.clone(),
        rate_limit,
        clearinghouse_state,
        open_order_count: open_orders.len(),
        fill_count: recent_fills.len(),
        open_orders,
        recent_fills,
    })
}

async fn reconcile_account_best_effort(
    config: &AppConfig,
    account_id: &str,
) -> (Option<AccountReconciliationReport>, Option<String>) {
    let account = match config.account(account_id) {
        Some(account) => account,
        None => {
            return (
                None,
                Some(format!("account {account_id} not found in config")),
            );
        }
    };
    let query_dex = info_query_dex(&config.hyperliquid.dex);
    let mut warnings = Vec::new();

    let open_orders =
        match fetch_open_orders(&config.app.environment, &query_dex, &account.address).await {
            Ok(open_orders) => open_orders,
            Err(error) => {
                warnings.push(format!("open orders unavailable: {error}"));
                Vec::new()
            }
        };
    let recent_fills =
        match fetch_user_fills(&config.app.environment, &query_dex, &account.address).await {
            Ok(recent_fills) => recent_fills,
            Err(error) => {
                warnings.push(format!("user fills unavailable: {error}"));
                Vec::new()
            }
        };
    let rate_limit = match fetch_user_rate_limit(&config.app.environment, &account.address).await {
        Ok(rate_limit) => rate_limit,
        Err(error) => {
            warnings.push(format!("user rate limit unavailable: {error}"));
            return (None, Some(warnings.join(" | ")));
        }
    };
    let clearinghouse_state = if is_spot_dex(&config.hyperliquid.dex) {
        match fetch_default_clearinghouse_state(&config.app.environment, &account.address).await {
            Ok(state) => state,
            Err(error) => {
                warnings.push(format!(
                    "default perps clearinghouse state unavailable for spot reconciliation: {error}"
                ));
                return (None, Some(warnings.join(" | ")));
            }
        }
    } else {
        match fetch_clearinghouse_state(
            &config.app.environment,
            &config.hyperliquid.dex,
            &account.address,
        )
        .await
        {
            Ok(state) => state,
            Err(error) => {
                warnings.push(format!("clearinghouse state unavailable: {error}"));
                return (None, Some(warnings.join(" | ")));
            }
        }
    };

    let report = AccountReconciliationReport {
        environment: config.app.environment.clone(),
        dex: config.hyperliquid.dex.clone(),
        account_id: account.account_id.clone(),
        address: account.address.clone(),
        rate_limit,
        clearinghouse_state,
        open_order_count: open_orders.len(),
        fill_count: recent_fills.len(),
        open_orders,
        recent_fills,
    };
    let note = if warnings.is_empty() {
        None
    } else {
        Some(warnings.join(" | "))
    };
    (Some(report), note)
}

async fn fetch_signed_close_size_hint(
    config: &AppConfig,
    account: &AccountConfig,
    coin: &str,
    side: OrderSide,
    reduce_only: bool,
    close_full_position: bool,
) -> Result<Option<f64>> {
    let close_gate = signed_close_exempt_from_opening_rules(
        &config.hyperliquid.dex,
        side,
        reduce_only,
        close_full_position,
    );
    if !close_gate || !close_full_position {
        return Ok(None);
    }
    let canonical_coin = canonical_coin_for_dex(&config.hyperliquid.dex, coin);
    if is_spot_dex(&config.hyperliquid.dex) {
        let state =
            fetch_spot_clearinghouse_state(&config.app.environment, &account.address).await?;
        let summary = spot_account_readiness_state(&state, &canonical_coin);
        Ok(signed_close_size_hint(true, true, Some(&summary)))
    } else {
        let state = fetch_clearinghouse_state(
            &config.app.environment,
            &config.hyperliquid.dex,
            &account.address,
        )
        .await?;
        let summary =
            summarize_account_readiness_state(&config.hyperliquid.dex, &state, &canonical_coin);
        Ok(signed_close_size_hint(true, true, Some(&summary)))
    }
}

fn realtime_signed_close_size_hint(
    config: &AppConfig,
    account: &AccountConfig,
    coin: &str,
    side: OrderSide,
    reduce_only: bool,
    close_full_position: bool,
    realtime: Option<&RealtimeState>,
) -> Option<(f64, String)> {
    let close_gate = signed_close_exempt_from_opening_rules(
        &config.hyperliquid.dex,
        side,
        reduce_only,
        close_full_position,
    );
    if !close_gate || !close_full_position {
        return None;
    }
    let realtime = realtime?;
    let canonical_coin = canonical_coin_for_dex(&config.hyperliquid.dex, coin);
    if is_spot_dex(&config.hyperliquid.dex) {
        let state = realtime.spot_state(&account.address)?;
        let summary = spot_account_readiness_state(&state, &canonical_coin);
        signed_close_size_hint(true, true, Some(&summary)).map(|size| {
            (
                size,
                "close_full_position size from realtime spot state".to_string(),
            )
        })
    } else {
        let market_id = market_id_for_dex(&config.hyperliquid.dex);
        let state = realtime.clearinghouse_state(market_id, &account.address)?;
        let summary =
            summarize_account_readiness_state(&config.hyperliquid.dex, &state, &canonical_coin);
        signed_close_size_hint(true, true, Some(&summary)).map(|size| {
            (
                size,
                format!("close_full_position size from realtime {market_id} state"),
            )
        })
    }
}

pub async fn query_order_status(
    config: &AppConfig,
    account_id: &str,
    lookup: OrderStatusLookup,
) -> Result<OrderStatusReport> {
    let account = config
        .account(account_id)
        .with_context(|| format!("account {account_id} not found in config"))?;
    let order_status = match &lookup {
        OrderStatusLookup::Oid { oid } => {
            fetch_order_status_by_oid(&config.app.environment, &account.address, *oid).await
        }
        OrderStatusLookup::Cloid { cloid } => {
            fetch_order_status_by_cloid(&config.app.environment, &account.address, cloid).await
        }
    }
    .context("failed to fetch order status")?;

    Ok(OrderStatusReport {
        environment: config.app.environment.clone(),
        dex: config.hyperliquid.dex.clone(),
        account_id: account.account_id.clone(),
        address: account.address.clone(),
        query: lookup,
        order_status,
    })
}

pub async fn execute_signed_runbook(
    config: AppConfig,
    options: SignedRunbookOptions,
    vault_password: Option<&str>,
) -> Result<SignedRunbookResult> {
    let preflight_options = SignedPreflightOptions {
        account_id: options.account_id.clone(),
        coin: options.coin.clone(),
        side: options.side,
        notional_usd: options.notional_usd,
        max_slippage_bps: options.max_slippage_bps,
        execution_mode: options.execution_mode,
        reduce_only: options.reduce_only,
        close_full_position: options.close_full_position,
        confirm_mainnet_live: options.confirm_mainnet_live,
    };
    let preflight =
        execute_signed_preflight(config.clone(), preflight_options, vault_password).await?;
    let (pre_submit_reconciliation, pre_submit_reconciliation_note) =
        reconcile_account_best_effort(&config, &options.account_id).await;
    let pre_submit_reconciliation_ok = pre_submit_reconciliation.is_some();
    let pre_submit_reconciliation_detail = if let Some(report) = &pre_submit_reconciliation {
        let base = format!(
            "open_orders={} fills={} remaining_capacity={}",
            report.open_order_count,
            report.fill_count,
            report.rate_limit.request_capacity_remaining()
        );
        if let Some(note) = &pre_submit_reconciliation_note {
            format!("{base}; best-effort degradation: {note}")
        } else {
            base
        }
    } else {
        pre_submit_reconciliation_note
            .unwrap_or_else(|| "pre-submit reconciliation unavailable".to_string())
    };
    let mut checks = vec![
        signed_runbook_check(
            "preflight_ready",
            if config.app.environment == "mainnet" {
                preflight.ready_for_mainnet_submit
            } else {
                preflight.ready_for_testnet_submit
            },
            if config.app.environment == "mainnet" {
                "ready_for_mainnet_submit must be true before signed submit".to_string()
            } else {
                "ready_for_testnet_submit must be true before signed submit".to_string()
            },
        ),
        signed_runbook_check(
            "pre_submit_reconciliation_available",
            pre_submit_reconciliation_ok,
            pre_submit_reconciliation_detail,
        ),
    ];

    let read_only_acceptance_options = SignedAcceptanceOptions {
        account_id: options.account_id.clone(),
        coin: options.coin.clone(),
        side: options.side,
        notional_usd: options.notional_usd,
        max_slippage_bps: options.max_slippage_bps,
        execution_mode: options.execution_mode,
        reduce_only: options.reduce_only,
        close_full_position: options.close_full_position,
        submit: false,
        cancel_resting: options.cancel_resting,
        confirm_mainnet_live: options.confirm_mainnet_live,
    };
    let read_only_acceptance =
        execute_signed_acceptance(config.clone(), read_only_acceptance_options, None).await?;
    checks.push(signed_runbook_check(
        "acceptance_plan_available",
        true,
        format!(
            "asset_id={} tif={} size={}",
            read_only_acceptance.plan.asset_id,
            read_only_acceptance.plan.tif,
            read_only_acceptance.plan.size
        ),
    ));

    let mut acceptance = Some(read_only_acceptance);
    let mut post_submit_reconciliation = None;
    let mut order_status_checks = Vec::new();
    let mut submitted = false;

    if options.submit {
        let ready = if config.app.environment == "mainnet" {
            preflight.ready_for_mainnet_submit
        } else {
            preflight.ready_for_testnet_submit
        };
        if ready {
            let password = vault_password
                .context("TRADE_XYZ_VAULT_PASSWORD is required for signed runbook submit")?;
            let submit_acceptance_options = SignedAcceptanceOptions {
                account_id: options.account_id.clone(),
                coin: options.coin.clone(),
                side: options.side,
                notional_usd: options.notional_usd,
                max_slippage_bps: options.max_slippage_bps,
                execution_mode: options.execution_mode,
                reduce_only: options.reduce_only,
                close_full_position: options.close_full_position,
                submit: true,
                cancel_resting: options.cancel_resting,
                confirm_mainnet_live: options.confirm_mainnet_live,
            };
            let submitted_acceptance = execute_signed_acceptance(
                config.clone(),
                submit_acceptance_options,
                Some(password),
            )
            .await?;
            submitted = submitted_acceptance.submitted;
            let order_status_check_result = collect_runbook_order_status_checks(
                &config,
                &options.account_id,
                &submitted_acceptance,
                &mut order_status_checks,
            )
            .await;
            let (post_reconcile, post_reconcile_note) =
                reconcile_account_best_effort(&config, &options.account_id).await;
            let post_reconcile_ok = post_reconcile.is_some();
            let post_reconcile_detail = if let Some(report) = &post_reconcile {
                let base = format!(
                    "open_orders={} fills={} remaining_capacity={}",
                    report.open_order_count,
                    report.fill_count,
                    report.rate_limit.request_capacity_remaining()
                );
                if let Some(note) = &post_reconcile_note {
                    format!("{base}; best-effort degradation: {note}")
                } else {
                    base
                }
            } else {
                post_reconcile_note
                    .unwrap_or_else(|| "post-submit reconciliation unavailable".to_string())
            };
            checks.push(signed_runbook_check(
                "post_submit_reconciliation_available",
                post_reconcile_ok,
                post_reconcile_detail,
            ));
            checks.push(signed_runbook_check(
                "order_status_checked",
                order_status_check_result.is_ok() && !order_status_checks.is_empty(),
                match order_status_check_result {
                    Ok(()) => format!(
                        "order status checks collected: {}",
                        order_status_checks.len()
                    ),
                    Err(error) => {
                        format!("best-effort order status lookup failed after submit: {error}")
                    }
                },
            ));
            post_submit_reconciliation = post_reconcile;
            acceptance = Some(submitted_acceptance);
        } else {
            checks.push(signed_runbook_check(
                "submit_blocked_before_secret_loading",
                true,
                "preflight blockers remain; signed runbook did not load secrets or submit"
                    .to_string(),
            ));
        }
    }

    Ok(SignedRunbookResult {
        environment: config.app.environment.clone(),
        account_id: options.account_id,
        coin: preflight.coin.clone(),
        submit_requested: options.submit,
        submitted,
        preflight,
        pre_submit_reconciliation,
        acceptance,
        post_submit_reconciliation,
        order_status_checks,
        checks,
    })
}

pub async fn execute_signed_smoke(
    config: AppConfig,
    options: SignedSmokeOptions,
    vault_password: Option<&str>,
) -> Result<SignedSmokeResult> {
    let canonical_coin = canonical_coin_for_dex(&config.hyperliquid.dex, &options.coin);
    let mut options = options;
    options.coin = canonical_coin;
    let account = config
        .account(&options.account_id)
        .cloned()
        .with_context(|| format!("account {} not found in config", options.account_id))?;
    anyhow::ensure!(
        account.enabled && account.worker_enabled,
        "account {} is not enabled for worker execution",
        account.account_id
    );
    validate_signed_smoke_constraints(&config, &account, &options)?;
    let exact_close_size = fetch_signed_close_size_hint(
        &config,
        &account,
        &options.coin,
        options.side,
        options.reduce_only,
        options.close_full_position,
    )
    .await?;
    let plan = if is_spot_dex(&config.hyperliquid.dex) {
        let snapshot = fetch_spot_market_snapshot_cached(&config.app.environment, 15_000)
            .await
            .context("failed to fetch spot market snapshot")?;
        let plan = build_signed_spot_order_plan(
            &snapshot,
            &options.coin,
            options.side,
            options.notional_usd,
            options.max_slippage_bps,
            options.execution_mode,
            exact_close_size,
        )?;
        apply_order_plan_size_override(plan, None, &options.coin)?
    } else {
        let snapshot = fetch_xyz_market_snapshot_cached(
            &config.app.environment,
            &config.hyperliquid.dex,
            15_000,
        )
        .await
        .context("failed to fetch XYZ market snapshot")?;
        let plan = build_signed_order_plan(
            &snapshot,
            &options.coin,
            options.side,
            options.notional_usd,
            options.max_slippage_bps,
            options.execution_mode,
            exact_close_size,
        )?;
        apply_order_plan_size_override(plan, None, &options.coin)?
    };
    let execution_policy = execution_policy_for_mode(options.execution_mode);

    let effective_notional = effective_order_notional_usd(plan.limit_price, plan.size);
    let mut result = SignedSmokeResult {
        plan: SignedSmokePlanReport {
            environment: config.app.environment.clone(),
            account_id: account.account_id.clone(),
            coin: plan.coin.clone(),
            asset_id: plan.asset_id,
            reference_price: plan.reference_price,
            limit_price: plan.limit_price,
            size: plan.size,
            effective_notional_usd: effective_notional,
            minimum_requested_notional_usd: minimum_requested_notional_for_effective_min(
                plan.limit_price,
                plan.reference_price,
                plan.sz_decimals,
                HYPERLIQUID_MIN_ORDER_NOTIONAL_USD,
            ),
            sz_decimals: plan.sz_decimals,
            execution_mode: options.execution_mode,
            tif: tif_for_policy(execution_policy),
            reduce_only: options.reduce_only,
            submit: options.submit,
        },
        submit_report: None,
        cancel_response: None,
        reconciliation: None,
    };

    if !options.submit {
        return Ok(result);
    }

    validate_signed_submit_gates(&config, &options)?;
    ensure_live_account_address(&account)?;

    let password = vault_password.context("vault password is required for signed smoke submit")?;
    let secret = load_account_secret(&config, &account, Some(password))?;
    let executor = LiveExchangeExecutor::new(config.clone(), account.clone(), secret);
    let now = now_ms();
    let cloid_seed = format!(
        "signed-smoke:{}:{}:{:?}:{}",
        account.account_id, plan.coin, options.side, now
    );
    let cloid = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, cloid_seed.as_bytes()).to_string();
    let order = ApprovedOrder {
        risk_decision_id: format!("signed-smoke-risk-{now}"),
        intent_id: format!("signed-smoke-intent-{now}"),
        signal_id: Some(format!("signed-smoke-signal-{now}")),
        worker_id: format!("worker-{}", account.account_id),
        account_id: account.account_id.clone(),
        strategy_id: "signed_smoke".to_string(),
        market: None,
        dex: Some(config.hyperliquid.dex.clone()),
        coin: plan.coin.clone(),
        side: options.side,
        notional_usd: options.notional_usd,
        exact_size: Some(plan.size),
        price: None,
        execution_mode: options.execution_mode,
        execution_policy,
        max_slippage_bps: options.max_slippage_bps,
        reduce_only: signed_exchange_reduce_only_flag(
            &config.hyperliquid.dex,
            options.side,
            options.reduce_only,
            options.close_full_position,
        ),
        cloid: cloid.clone(),
        expires_at_ms: Some(now + config.process.signal_ttl_ms),
    };

    let report = executor.submit(order).await;

    if options.cancel_resting {
        let should_cancel = matches!(
            &report,
            WorkerReport::Submitted(submitted)
                if submitted.exchange_status.as_deref() == Some("resting")
        );
        if should_cancel {
            let cancel_response = executor.cancel_by_cloid(&plan.coin, &cloid).await?;
            result.cancel_response = Some(format!("{cancel_response:?}"));
        }
    }

    if let WorkerReport::Submitted(submitted) = &report {
        let info_dex = info_query_dex(&config.hyperliquid.dex);
        let open_orders =
            match fetch_open_orders(&config.app.environment, &info_dex, &account.address).await {
                Ok(open_orders) => open_orders,
                Err(error) => {
                    tracing::warn!(
                        account_id = %account.account_id,
                        error = %error,
                        "best-effort signed smoke open-orders reconciliation failed; continuing"
                    );
                    Vec::new()
                }
            };
        let matching_open = submitted
            .oid
            .map(|oid| open_orders.iter().any(|order| order.oid == oid))
            .unwrap_or(false);
        let fills =
            match fetch_user_fills(&config.app.environment, &info_dex, &account.address).await {
                Ok(fills) => fills,
                Err(error) => {
                    tracing::warn!(
                        account_id = %account.account_id,
                        error = %error,
                        "best-effort signed smoke user-fills reconciliation failed; continuing"
                    );
                    Vec::new()
                }
            };
        let matching_fills = submitted
            .oid
            .map(|oid| fills.iter().filter(|fill| fill.oid == oid).count())
            .unwrap_or(0);
        let order_status = match submitted.oid {
            Some(oid) => {
                match fetch_order_status_by_oid(&config.app.environment, &account.address, oid)
                    .await
                {
                    Ok(status) => Some(status),
                    Err(error) => {
                        tracing::warn!(
                            account_id = %account.account_id,
                            %oid,
                            error = %error,
                            "best-effort signed smoke order-status lookup failed; continuing"
                        );
                        None
                    }
                }
            }
            None => None,
        };
        result.reconciliation = Some(SignedSmokeReconciliation {
            open_orders: open_orders.len(),
            matching_open,
            xyz_fills: fills.len(),
            matching_fills,
            order_status,
        });
    }

    result.submit_report = Some(report);
    Ok(result)
}

pub async fn execute_fast_signed_order(
    config: AppConfig,
    options: SignedSmokeOptions,
    dry_run: bool,
    vault_password: Option<&str>,
    realtime: Option<&RealtimeState>,
) -> Result<FastSignedOrderResult> {
    let canonical_coin = canonical_coin_for_dex(&config.hyperliquid.dex, &options.coin);
    let mut options = options;
    options.coin = canonical_coin;
    let account = config
        .account(&options.account_id)
        .cloned()
        .with_context(|| format!("account {} not found in config", options.account_id))?;
    anyhow::ensure!(
        account.enabled && account.worker_enabled,
        "account {} is not enabled for worker execution",
        account.account_id
    );
    validate_signed_smoke_constraints(&config, &account, &options)?;

    let mut cache_notes = Vec::new();
    let exact_close_size = if let Some((size, note)) = realtime_signed_close_size_hint(
        &config,
        &account,
        &options.coin,
        options.side,
        options.reduce_only,
        options.close_full_position,
        realtime,
    ) {
        cache_notes.push(note);
        Some(size)
    } else {
        let size = fetch_signed_close_size_hint(
            &config,
            &account,
            &options.coin,
            options.side,
            options.reduce_only,
            options.close_full_position,
        )
        .await?;
        if size.is_some() {
            cache_notes.push("close_full_position size from bounded REST fallback".to_string());
        }
        size
    };

    let plan = if is_spot_dex(&config.hyperliquid.dex) {
        let snapshot = fetch_spot_market_snapshot_cached(&config.app.environment, 15_000)
            .await
            .context("failed to fetch spot market snapshot")?;
        let plan = build_signed_spot_order_plan(
            &snapshot,
            &options.coin,
            options.side,
            options.notional_usd,
            options.max_slippage_bps,
            options.execution_mode,
            exact_close_size,
        )?;
        apply_order_plan_size_override(plan, None, &options.coin)?
    } else {
        let snapshot = fetch_xyz_market_snapshot_cached(
            &config.app.environment,
            &config.hyperliquid.dex,
            15_000,
        )
        .await
        .context("failed to fetch XYZ market snapshot")?;
        let plan = build_signed_order_plan(
            &snapshot,
            &options.coin,
            options.side,
            options.notional_usd,
            options.max_slippage_bps,
            options.execution_mode,
            exact_close_size,
        )?;
        apply_order_plan_size_override(plan, None, &options.coin)?
    };
    let execution_policy = execution_policy_for_mode(options.execution_mode);
    let submit_requested = options.submit;
    let effective_submit = submit_requested && !dry_run;
    let effective_notional = effective_order_notional_usd(plan.limit_price, plan.size);
    let plan_report = SignedSmokePlanReport {
        environment: config.app.environment.clone(),
        account_id: account.account_id.clone(),
        coin: plan.coin.clone(),
        asset_id: plan.asset_id,
        reference_price: plan.reference_price,
        limit_price: plan.limit_price,
        size: plan.size,
        effective_notional_usd: effective_notional,
        minimum_requested_notional_usd: minimum_requested_notional_for_effective_min(
            plan.limit_price,
            plan.reference_price,
            plan.sz_decimals,
            HYPERLIQUID_MIN_ORDER_NOTIONAL_USD,
        ),
        sz_decimals: plan.sz_decimals,
        execution_mode: options.execution_mode,
        tif: tif_for_policy(execution_policy),
        reduce_only: options.reduce_only,
        submit: effective_submit,
    };

    if !effective_submit {
        if submit_requested && dry_run {
            cache_notes.push("console process dry-run prevented fast signed submit".to_string());
        }
        return Ok(FastSignedOrderResult {
            environment: config.app.environment.clone(),
            account_id: account.account_id,
            coin: plan.coin,
            transport: "dry_plan".to_string(),
            submit_requested,
            submitted: false,
            submit_latency_ms: None,
            plan: plan_report,
            submit_report: None,
            cancel_response: None,
            cache_notes,
            warnings: if submit_requested && dry_run {
                vec!["console dry-run is enabled; no exchange action was submitted".to_string()]
            } else {
                Vec::new()
            },
        });
    }

    validate_signed_submit_gates(&config, &options)?;
    ensure_live_account_address(&account)?;
    let password = vault_password.context("vault password is required for fast signed submit")?;
    let secret = load_account_secret(&config, &account, Some(password))?;
    let executor = LiveExchangeExecutor::new(config.clone(), account.clone(), secret);
    let cancel_response = if options.cancel_resting {
        let response = executor
            .cancel_open_orders_for_coin_fast(&plan.coin, realtime)
            .await?;
        if response.is_some() {
            cache_notes
                .push("same-coin resting orders cancelled through websocket post".to_string());
        } else {
            cache_notes
                .push("same-coin resting order cancel skipped because none were open".to_string());
        }
        response
    } else {
        None
    };
    let now = now_ms();
    let cloid_seed = format!(
        "fast-signed:{}:{}:{:?}:{}",
        account.account_id, plan.coin, options.side, now
    );
    let cloid = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, cloid_seed.as_bytes()).to_string();
    let order = ApprovedOrder {
        risk_decision_id: format!("fast-signed-risk-{now}"),
        intent_id: format!("fast-signed-intent-{now}"),
        signal_id: Some(format!("fast-signed-signal-{now}")),
        worker_id: format!("worker-{}", account.account_id),
        account_id: account.account_id.clone(),
        strategy_id: "fast_signed".to_string(),
        market: None,
        dex: Some(config.hyperliquid.dex.clone()),
        coin: plan.coin.clone(),
        side: options.side,
        notional_usd: options.notional_usd,
        exact_size: Some(plan.size),
        price: None,
        execution_mode: options.execution_mode,
        execution_policy,
        max_slippage_bps: options.max_slippage_bps,
        reduce_only: signed_exchange_reduce_only_flag(
            &config.hyperliquid.dex,
            options.side,
            options.reduce_only,
            options.close_full_position,
        ),
        cloid,
        expires_at_ms: Some(now + config.process.signal_ttl_ms),
    };

    let started = Instant::now();
    let report = executor.submit_fast(order).await;
    let submit_latency_ms = started.elapsed().as_millis() as u64;
    let submitted = matches!(report, WorkerReport::Submitted(_));

    Ok(FastSignedOrderResult {
        environment: config.app.environment,
        account_id: account.account_id,
        coin: plan.coin,
        transport: "websocket_post_action".to_string(),
        submit_requested: true,
        submitted,
        submit_latency_ms: Some(submit_latency_ms),
        plan: plan_report,
        submit_report: Some(report),
        cancel_response,
        cache_notes,
        warnings: Vec::new(),
    })
}

pub async fn execute_signed_acceptance(
    config: AppConfig,
    options: SignedAcceptanceOptions,
    vault_password: Option<&str>,
) -> Result<SignedAcceptanceResult> {
    let account = config
        .account(&options.account_id)
        .cloned()
        .with_context(|| format!("account {} not found in config", options.account_id))?;
    anyhow::ensure!(
        account.enabled && account.worker_enabled,
        "account {} is not enabled for worker execution",
        account.account_id
    );

    let smoke_options = SignedSmokeOptions {
        account_id: options.account_id.clone(),
        coin: options.coin.clone(),
        side: options.side,
        notional_usd: options.notional_usd,
        max_slippage_bps: options.max_slippage_bps,
        execution_mode: options.execution_mode,
        reduce_only: options.reduce_only,
        close_full_position: options.close_full_position,
        submit: false,
        cancel_resting: options.cancel_resting,
        confirm_mainnet_live: options.confirm_mainnet_live,
    };
    let plan_result = execute_signed_smoke(config.clone(), smoke_options, None).await?;
    let pre_submit = fetch_acceptance_account_snapshot(&config, &account).await?;
    let mut checks = vec![
        acceptance_check(
            "plan_valid",
            true,
            format!(
                "asset_id={} limit_px={} size={}",
                plan_result.plan.asset_id, plan_result.plan.limit_price, plan_result.plan.size
            ),
        ),
        acceptance_check(
            "rate_limit_has_capacity",
            pre_submit.request_capacity_remaining > 0,
            format!(
                "remaining request capacity before submit: {}",
                pre_submit.request_capacity_remaining
            ),
        ),
    ];

    if !options.submit {
        return Ok(SignedAcceptanceResult {
            environment: config.app.environment,
            account_id: account.account_id,
            coin: plan_result.plan.coin.clone(),
            submitted: false,
            plan: plan_result.plan,
            pre_submit,
            signed_smoke: None,
            post_submit: None,
            checks,
        });
    }

    let close_gate = signed_close_exempt_from_opening_rules(
        &config.hyperliquid.dex,
        options.side,
        options.reduce_only,
        options.close_full_position,
    );
    validate_live_order_gates(&config, options.confirm_mainnet_live, close_gate)?;
    ensure_live_account_address(&account)?;
    let password = vault_password
        .context("TRADE_XYZ_VAULT_PASSWORD is required for signed acceptance submit")?;
    let submit_options = SignedSmokeOptions {
        account_id: options.account_id,
        coin: options.coin,
        side: options.side,
        notional_usd: options.notional_usd,
        max_slippage_bps: options.max_slippage_bps,
        execution_mode: options.execution_mode,
        reduce_only: options.reduce_only,
        close_full_position: options.close_full_position,
        submit: true,
        cancel_resting: options.cancel_resting,
        confirm_mainnet_live: options.confirm_mainnet_live,
    };
    let signed_smoke = execute_signed_smoke(config.clone(), submit_options, Some(password)).await?;
    let submit_ok = matches!(signed_smoke.submit_report, Some(WorkerReport::Submitted(_)));
    checks.push(acceptance_check(
        "submit_report_submitted",
        submit_ok,
        if submit_ok {
            "exchange returned a submitted report".to_string()
        } else {
            format!("unexpected submit report: {:?}", signed_smoke.submit_report)
        },
    ));

    let reconciliation_ok = signed_smoke.reconciliation.is_some();
    checks.push(acceptance_check(
        "post_submit_reconciliation_available",
        reconciliation_ok,
        "open orders, fills, and orderStatus were queried after submit".to_string(),
    ));

    let post_submit = fetch_acceptance_account_snapshot(&config, &account).await?;
    checks.push(acceptance_check(
        "post_submit_rate_limit_has_capacity",
        post_submit.request_capacity_remaining > 0,
        format!(
            "remaining request capacity after submit: {}",
            post_submit.request_capacity_remaining
        ),
    ));

    anyhow::ensure!(
        checks.iter().all(|check| check.ok),
        "signed acceptance smoke did not satisfy all checks"
    );

    Ok(SignedAcceptanceResult {
        environment: config.app.environment,
        account_id: account.account_id,
        coin: plan_result.plan.coin.clone(),
        submitted: true,
        plan: plan_result.plan,
        pre_submit,
        signed_smoke: Some(signed_smoke),
        post_submit: Some(post_submit),
        checks,
    })
}

pub async fn execute_signed_preflight(
    config: AppConfig,
    options: SignedPreflightOptions,
    vault_password: Option<&str>,
) -> Result<SignedPreflightResult> {
    let canonical_coin = canonical_coin_for_dex(&config.hyperliquid.dex, &options.coin);
    let account = config.account(&options.account_id);
    let mut checks = Vec::new();

    checks.push(SignedPreflightCheck::blocker(
        "account_configured",
        account.is_some(),
        if account.is_some() {
            "account exists in current config".to_string()
        } else {
            format!("account {} is missing from config", options.account_id)
        },
    ));

    if let Some(account) = account {
        checks.push(SignedPreflightCheck::blocker(
            "account_enabled",
            account.enabled && account.worker_enabled,
            "account must be enabled and worker_enabled",
        ));
        checks.push(SignedPreflightCheck::blocker(
            "address_not_placeholder",
            is_probably_real_evm_address(&account.address),
            "account address must be a real master/subaccount address, not an example placeholder",
        ));
        checks.push(SignedPreflightCheck::blocker(
            "account_notional_limit",
            options.notional_usd > 0.0 && options.notional_usd <= account.max_order_notional_usd,
            format!(
                "requested notional {} must be > 0 and <= account max {}",
                options.notional_usd, account.max_order_notional_usd
            ),
        ));
    }

    checks.push(SignedPreflightCheck::blocker(
        "config_dry_run_disabled",
        !config.app.dry_run,
        "signed submit requires app.dry_run=false in the config file",
    ));
    checks.push(SignedPreflightCheck::blocker(
        "manual_live_enabled",
        config.manual_ops.manual_live_enabled,
        "signed submit requires manual_ops.manual_live_enabled=true",
    ));
    checks.push(SignedPreflightCheck::blocker(
        "mainnet_gate",
        config.app.environment != "mainnet"
            || (config.manual_ops.mainnet_live_enabled && options.confirm_mainnet_live),
        "mainnet requires manual_ops.mainnet_live_enabled=true and --confirm-mainnet-live",
    ));
    let close_gate = signed_close_exempt_from_opening_rules(
        &config.hyperliquid.dex,
        options.side,
        options.reduce_only,
        options.close_full_position,
    );
    let kill_switch_allows_order = !config.risk.global.kill_switch
        || (close_gate && config.risk.global.allow_reduce_only_when_killed);
    checks.push(SignedPreflightCheck::blocker(
        "global_kill_switch_clear",
        kill_switch_allows_order,
        if close_gate && config.risk.global.kill_switch {
            "global kill switch is active, but reduce-only signed orders are allowed by config"
                .to_string()
        } else {
            "global kill switch must be false before signed submit can open a new position"
                .to_string()
        },
    ));
    checks.push(SignedPreflightCheck::blocker(
        "manual_notional_limit",
        options.notional_usd > 0.0
            && options.notional_usd <= config.manual_ops.max_manual_order_notional_usd,
        format!(
            "requested notional {} must be > 0 and <= manual max {}",
            options.notional_usd, config.manual_ops.max_manual_order_notional_usd
        ),
    ));
    checks.push(SignedPreflightCheck::blocker(
        "exchange_min_order_notional",
        exchange_min_order_notional_ok(options.notional_usd, close_gate),
        format!(
            "opening orders must be at least {} USD; supported close paths are allowed to protect existing positions or spot inventory",
            HYPERLIQUID_MIN_ORDER_NOTIONAL_USD
        ),
    ));
    let symbol_allowed = config.manual_ops.blocked_symbols.is_empty()
        || !config.manual_ops.blocked_symbols.contains(&canonical_coin);
    checks.push(SignedPreflightCheck::blocker(
        "symbol_allowed",
        symbol_allowed,
        if symbol_allowed {
            format!("{canonical_coin} is allowed by manual_ops.blocked_symbols")
        } else {
            format!("{canonical_coin} must not be listed in manual_ops.blocked_symbols")
        },
    ));

    let vault_path = PathBuf::from(&config.secrets.vault_path);
    checks.push(SignedPreflightCheck::blocker(
        "vault_file_exists",
        vault_path.exists(),
        format!("vault path: {}", vault_path.display()),
    ));
    let password_available = vault_password
        .map(|password| !password.trim().is_empty())
        .unwrap_or(false);
    checks.push(SignedPreflightCheck::blocker(
        "vault_password_available",
        password_available,
        "TRADE_XYZ_VAULT_PASSWORD must be set in this PowerShell session for CLI signed submit",
    ));

    let secret_available = if let Some(account) = account {
        if let Some(password) = vault_password.filter(|password| !password.trim().is_empty()) {
            let secret_id = account_secret_id(account);
            load_secret_by_id(&vault_path, password, &secret_id, Some(&account.account_id)).is_ok()
        } else {
            false
        }
    } else {
        false
    };
    checks.push(SignedPreflightCheck::blocker(
        "api_wallet_secret_available",
        secret_available,
        "matching API wallet private key must be available in the encrypted vault",
    ));

    let rate_limit = if let Some(account) = account {
        match fetch_user_rate_limit(&config.app.environment, &account.address).await {
            Ok(rate_limit) => {
                let remaining = rate_limit.request_capacity_remaining();
                checks.push(SignedPreflightCheck::blocker(
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
                checks.push(SignedPreflightCheck::blocker(
                    "user_rate_limit_has_capacity",
                    remaining > 0,
                    format!("remaining request capacity before local throttling: {remaining}"),
                ));
                Some(rate_limit)
            }
            Err(error) => {
                checks.push(SignedPreflightCheck::blocker(
                    "user_rate_limit_available",
                    false,
                    format!("failed to fetch userRateLimit: {error}"),
                ));
                None
            }
        }
    } else {
        checks.push(SignedPreflightCheck::blocker(
            "user_rate_limit_available",
            false,
            "account must be configured before userRateLimit can be fetched",
        ));
        None
    };

    let account_state = if let Some(account) = account {
        if is_spot_dex(&config.hyperliquid.dex) {
            match fetch_spot_clearinghouse_state(&config.app.environment, &account.address).await {
                Ok(state) => {
                    let summary = spot_account_readiness_state(&state, &canonical_coin);
                    checks.push(SignedPreflightCheck::blocker(
                        "clearinghouse_state_available",
                        true,
                        format!(
                            "spotAvailableUsdc={}, spotBaseAvailable={}",
                            summary.withdrawable_usd, summary.coin_position_size
                        ),
                    ));
                    if close_gate {
                        checks.push(SignedPreflightCheck::blocker(
                            "reduce_only_position_available",
                            reduce_only_spot_position_available(
                                options.side,
                                summary.coin_position_size,
                            ),
                            reduce_only_spot_position_detail(
                                options.side,
                                summary.coin_position_size,
                            ),
                        ));
                    } else {
                        checks.push(SignedPreflightCheck::blocker(
                            "account_has_available_collateral",
                            account_has_opening_collateral(&summary),
                            format!(
                                "spotAvailableUsdc={}, requested notional={}",
                                summary.withdrawable_usd, options.notional_usd
                            ),
                        ));
                    }
                    Some(summary)
                }
                Err(error) => {
                    checks.push(SignedPreflightCheck::blocker(
                        "clearinghouse_state_available",
                        false,
                        format!("failed to fetch spotClearinghouseState: {error}"),
                    ));
                    if close_gate {
                        checks.push(SignedPreflightCheck::blocker(
                            "reduce_only_position_available",
                            false,
                            "spotClearinghouseState is required before reduce-only spot validation",
                        ));
                    } else {
                        checks.push(SignedPreflightCheck::blocker(
                            "account_has_available_collateral",
                            false,
                            "spotClearinghouseState is required before opening-order collateral validation",
                        ));
                    }
                    None
                }
            }
        } else {
            match fetch_clearinghouse_state(
                &config.app.environment,
                &config.hyperliquid.dex,
                &account.address,
            )
            .await
            {
                Ok(state) => {
                    let summary = summarize_account_readiness_state(
                        &config.hyperliquid.dex,
                        &state,
                        &canonical_coin,
                    );
                    checks.push(SignedPreflightCheck::blocker(
                        "clearinghouse_state_available",
                        true,
                        format!(
                            "accountValue={}, withdrawable={}, coinPositionSize={}",
                            summary.account_value_usd,
                            summary.withdrawable_usd,
                            summary.coin_position_size
                        ),
                    ));
                    if close_gate {
                        checks.push(SignedPreflightCheck::blocker(
                            "reduce_only_position_available",
                            reduce_only_position_available(
                                options.side,
                                summary.coin_position_size,
                            ),
                            reduce_only_position_detail(options.side, summary.coin_position_size),
                        ));
                    } else {
                        checks.push(SignedPreflightCheck::blocker(
                            "account_has_available_collateral",
                            account_has_opening_collateral(&summary),
                            format!(
                                "accountValue={}, withdrawable={}, requested notional={}",
                                summary.account_value_usd,
                                summary.withdrawable_usd,
                                options.notional_usd
                            ),
                        ));
                    }
                    Some(summary)
                }
                Err(error) => {
                    checks.push(SignedPreflightCheck::blocker(
                        "clearinghouse_state_available",
                        false,
                        format!("failed to fetch clearinghouseState: {error}"),
                    ));
                    if close_gate {
                        checks.push(SignedPreflightCheck::blocker(
                            "reduce_only_position_available",
                            false,
                            "clearinghouseState is required before reduce-only position validation",
                        ));
                    } else {
                        checks.push(SignedPreflightCheck::blocker(
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
        checks.push(SignedPreflightCheck::blocker(
            "clearinghouse_state_available",
            false,
            "account must be configured before clearinghouseState can be fetched",
        ));
        None
    };
    let exact_close_size = signed_close_size_hint(
        close_gate,
        options.close_full_position,
        account_state.as_ref(),
    );

    let plan = if is_spot_dex(&config.hyperliquid.dex) {
        match fetch_spot_market_snapshot_cached(&config.app.environment, 15_000).await {
            Ok(snapshot) => match build_signed_spot_order_plan(
                &snapshot,
                &canonical_coin,
                options.side,
                options.notional_usd,
                options.max_slippage_bps,
                options.execution_mode,
                exact_close_size,
            ) {
                Ok(plan) => {
                    checks.push(SignedPreflightCheck::blocker(
                        "signed_order_plan_valid",
                        true,
                        "metadata, price, precision, and size checks passed",
                    ));
                    let effective_notional =
                        effective_order_notional_usd(plan.limit_price, plan.size);
                    let minimum_requested_notional = minimum_requested_notional_for_effective_min(
                        plan.limit_price,
                        plan.reference_price,
                        plan.sz_decimals,
                        HYPERLIQUID_MIN_ORDER_NOTIONAL_USD,
                    );
                    checks.push(SignedPreflightCheck::blocker(
                        "exchange_min_order_notional_effective",
                        effective_exchange_min_order_notional_ok(
                            plan.limit_price,
                            plan.size,
                            close_gate,
                        ),
                        exchange_min_effective_detail(
                            effective_notional,
                            minimum_requested_notional,
                        ),
                    ));
                    Some(SignedSmokePlanReport {
                        environment: config.app.environment.clone(),
                        account_id: options.account_id.clone(),
                        coin: plan.coin,
                        asset_id: plan.asset_id,
                        reference_price: plan.reference_price,
                        limit_price: plan.limit_price,
                        size: plan.size,
                        effective_notional_usd: effective_notional,
                        minimum_requested_notional_usd: minimum_requested_notional,
                        sz_decimals: plan.sz_decimals,
                        execution_mode: options.execution_mode,
                        tif: tif_for_execution_mode(options.execution_mode),
                        reduce_only: options.reduce_only,
                        submit: false,
                    })
                }
                Err(error) => {
                    checks.push(SignedPreflightCheck::blocker(
                        "signed_order_plan_valid",
                        false,
                        error.to_string(),
                    ));
                    None
                }
            },
            Err(error) => {
                checks.push(SignedPreflightCheck::blocker(
                    "signed_order_plan_valid",
                    false,
                    format!("failed to fetch spot market snapshot: {error}"),
                ));
                None
            }
        }
    } else {
        match fetch_xyz_market_snapshot_cached(
            &config.app.environment,
            &config.hyperliquid.dex,
            15_000,
        )
        .await
        {
            Ok(snapshot) => match build_signed_order_plan(
                &snapshot,
                &canonical_coin,
                options.side,
                options.notional_usd,
                options.max_slippage_bps,
                options.execution_mode,
                exact_close_size,
            ) {
                Ok(plan) => {
                    checks.push(SignedPreflightCheck::blocker(
                        "signed_order_plan_valid",
                        true,
                        "metadata, price, precision, and size checks passed",
                    ));
                    let effective_notional =
                        effective_order_notional_usd(plan.limit_price, plan.size);
                    let minimum_requested_notional = minimum_requested_notional_for_effective_min(
                        plan.limit_price,
                        plan.reference_price,
                        plan.sz_decimals,
                        HYPERLIQUID_MIN_ORDER_NOTIONAL_USD,
                    );
                    checks.push(SignedPreflightCheck::blocker(
                        "exchange_min_order_notional_effective",
                        effective_exchange_min_order_notional_ok(
                            plan.limit_price,
                            plan.size,
                            close_gate,
                        ),
                        exchange_min_effective_detail(
                            effective_notional,
                            minimum_requested_notional,
                        ),
                    ));
                    Some(SignedSmokePlanReport {
                        environment: config.app.environment.clone(),
                        account_id: options.account_id.clone(),
                        coin: plan.coin,
                        asset_id: plan.asset_id,
                        reference_price: plan.reference_price,
                        limit_price: plan.limit_price,
                        size: plan.size,
                        effective_notional_usd: effective_notional,
                        minimum_requested_notional_usd: minimum_requested_notional,
                        sz_decimals: plan.sz_decimals,
                        execution_mode: options.execution_mode,
                        tif: tif_for_execution_mode(options.execution_mode),
                        reduce_only: options.reduce_only,
                        submit: false,
                    })
                }
                Err(error) => {
                    checks.push(SignedPreflightCheck::blocker(
                        "signed_order_plan_valid",
                        false,
                        error.to_string(),
                    ));
                    None
                }
            },
            Err(error) => {
                checks.push(SignedPreflightCheck::blocker(
                    "signed_order_plan_valid",
                    false,
                    format!("failed to fetch XYZ market snapshot: {error}"),
                ));
                None
            }
        }
    };

    let blockers_clear = checks
        .iter()
        .filter(|check| check.severity == "blocker")
        .all(|check| check.ok);
    let ready_for_testnet_submit = blockers_clear && config.app.environment == "testnet";
    let ready_for_mainnet_submit = blockers_clear
        && config.app.environment == "mainnet"
        && config.manual_ops.mainnet_live_enabled
        && options.confirm_mainnet_live;
    let failed_blockers = failed_preflight_blockers(&checks);
    let next_actions = preflight_next_actions(&checks, close_gate);
    let readiness_summary = preflight_readiness_summary(
        &config.app.environment,
        ready_for_testnet_submit,
        ready_for_mainnet_submit,
        &failed_blockers,
    );
    Ok(SignedPreflightResult {
        environment: config.app.environment.clone(),
        dry_run: config.app.dry_run,
        account_id: options.account_id,
        coin: canonical_coin,
        notional_usd: options.notional_usd,
        execution_mode: options.execution_mode,
        reduce_only: options.reduce_only,
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

pub async fn build_protective_exit_plan(
    config: &AppConfig,
    options: ProtectiveExitOptions,
    dry_run: bool,
) -> Result<ProtectiveExitPlanResult> {
    anyhow::ensure!(
        config.manual_ops.enabled && config.manual_ops.manual_trading_enabled,
        "manual trading is disabled"
    );
    let account = config
        .account(&options.account_id)
        .with_context(|| format!("account {} not found in config", options.account_id))?;
    anyhow::ensure!(
        account.enabled && account.worker_enabled,
        "account {} is not enabled for worker execution",
        account.account_id
    );
    let is_spot = is_spot_dex(&config.hyperliquid.dex);
    anyhow::ensure!(
        options.notional_usd > 0.0,
        "protective exit notional must be positive"
    );
    if is_spot {
        anyhow::ensure!(
            options.notional_usd <= account.max_order_notional_usd,
            "spot protective exit notional exceeds account max_order_notional_usd"
        );
        anyhow::ensure!(
            options.notional_usd <= config.manual_ops.max_manual_order_notional_usd,
            "spot protective exit notional exceeds max_manual_order_notional_usd"
        );
    }
    let has_any_explicit_trigger =
        options.take_profit_trigger_price.is_some() || options.stop_loss_trigger_price.is_some();
    if has_any_explicit_trigger {
        anyhow::ensure!(
            options.take_profit_trigger_price.is_some()
                && options.stop_loss_trigger_price.is_some(),
            "take_profit_trigger_price and stop_loss_trigger_price must both be set when using explicit trigger mode"
        );
    } else {
        anyhow::ensure!(
            options.take_profit_usd > 0.0,
            "take_profit_usd must be positive"
        );
        anyhow::ensure!(
            valid_stop_loss_pct(options.stop_loss_pct),
            "stop_loss_pct must be greater than 0 and less than 1"
        );
    }
    anyhow::ensure!(
        (0.0..10_000.0).contains(&options.max_slippage_bps),
        "max_slippage_bps must be >= 0 and < 10000"
    );

    let canonical_coin = canonical_coin_for_dex(&config.hyperliquid.dex, &options.coin);
    if !config.manual_ops.blocked_symbols.is_empty() {
        anyhow::ensure!(
            !config.manual_ops.blocked_symbols.contains(&canonical_coin),
            "manual symbol {} is blocked",
            canonical_coin
        );
    }

    let entry_side = parse_order_side(&options.entry_side)?;
    if is_spot {
        anyhow::ensure!(
            matches!(entry_side, OrderSide::Buy),
            "spot protective TP/SL currently supports buy entry only"
        );
        let snapshot = fetch_spot_market_snapshot_cached(&config.app.environment, 15_000)
            .await
            .context("failed to fetch spot market snapshot")?;
        let asset = snapshot.asset(&canonical_coin)?;
        let market_reference_price = asset
            .context
            .mid_px
            .as_deref()
            .and_then(|value| value.parse::<f64>().ok())
            .or_else(|| asset.context.mark_px.parse::<f64>().ok())
            .or_else(|| asset.context.prev_day_px.parse::<f64>().ok())
            .with_context(|| format!("reference price for {} is unavailable", asset.coin))?;
        let entry_price = options.entry_price.unwrap_or(market_reference_price);
        anyhow::ensure!(
            entry_price.is_finite() && entry_price > 0.0,
            "entry_price must be positive"
        );
        let (exit_side, take_profit_trigger, stop_loss_trigger) =
            protective_exit_prices_for_options(entry_side, entry_price, &options)?;
        let exit_size = crate::hyperliquid::round_size_down(
            options.notional_usd / entry_price,
            asset.sz_decimals,
        );
        anyhow::ensure!(
            exit_size > 0.0,
            "protective exit size rounds to zero for {} at notional {} and entry price {}",
            asset.coin,
            options.notional_usd,
            entry_price
        );
        let now = now_ms();
        let mut legs = Vec::new();
        for (kind, trigger_price) in [
            ("take_profit", take_profit_trigger),
            ("stop_loss", stop_loss_trigger),
        ] {
            let rounded_trigger =
                crate::hyperliquid::round_spot_price(trigger_price, asset.sz_decimals);
            let limit_guard_price =
                protective_exit_limit_price(rounded_trigger, exit_side, options.max_slippage_bps)?;
            let limit_price =
                crate::hyperliquid::round_spot_price(limit_guard_price, asset.sz_decimals);
            let cloid_seed = format!(
                "manual-protective:{}:{}:{:?}:{kind}:{}:{rounded_trigger}:{now}",
                account.account_id, canonical_coin, entry_side, options.notional_usd
            );
            legs.push(ProtectiveExitLeg {
                kind: kind.to_string(),
                trigger_price: rounded_trigger,
                limit_price,
                size: exit_size,
                asset_id: asset.asset_id,
                sz_decimals: asset.sz_decimals,
                cloid: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, cloid_seed.as_bytes())
                    .to_string(),
                local_trigger: true,
            });
        }
        return Ok(ProtectiveExitPlanResult {
            environment: config.app.environment.clone(),
            dex: config.hyperliquid.dex.clone(),
            account_id: account.account_id.clone(),
            coin: asset.coin,
            entry_side,
            exit_side,
            entry_price,
            market_reference_price,
            reduce_only: false,
            dry_run,
            legs,
        });
    }

    let snapshot =
        fetch_xyz_market_snapshot_cached(&config.app.environment, &config.hyperliquid.dex, 15_000)
            .await
            .context("failed to fetch XYZ market snapshot")?;
    let asset = snapshot.asset(&canonical_coin)?;
    let market_reference_price = asset.reference_price()?;
    let entry_price = options.entry_price.unwrap_or(market_reference_price);
    anyhow::ensure!(
        entry_price.is_finite() && entry_price > 0.0,
        "entry_price must be positive"
    );

    let (exit_side, take_profit_trigger, stop_loss_trigger) =
        protective_exit_prices_for_options(entry_side, entry_price, &options)?;
    let exit_size = crate::hyperliquid::round_size_down(
        options.notional_usd / entry_price,
        asset.meta.sz_decimals,
    );
    anyhow::ensure!(
        exit_size > 0.0,
        "protective exit size rounds to zero for {} at notional {} and entry price {}",
        asset.meta.name,
        options.notional_usd,
        entry_price
    );
    let now = now_ms();
    let mut legs = Vec::new();
    for (kind, trigger_price) in [
        ("take_profit", take_profit_trigger),
        ("stop_loss", stop_loss_trigger),
    ] {
        let rounded_trigger =
            crate::hyperliquid::round_perp_price(trigger_price, asset.meta.sz_decimals);
        let limit_guard_price =
            protective_exit_limit_price(rounded_trigger, exit_side, options.max_slippage_bps)?;
        let limit_price =
            crate::hyperliquid::round_perp_price(limit_guard_price, asset.meta.sz_decimals);
        let cloid_seed = format!(
            "manual-protective:{}:{}:{:?}:{kind}:{}:{rounded_trigger}:{now}",
            account.account_id, canonical_coin, entry_side, options.notional_usd
        );
        legs.push(ProtectiveExitLeg {
            kind: kind.to_string(),
            trigger_price: rounded_trigger,
            limit_price,
            size: exit_size,
            asset_id: asset.asset_id,
            sz_decimals: asset.meta.sz_decimals,
            cloid: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, cloid_seed.as_bytes())
                .to_string(),
            local_trigger: true,
        });
    }

    Ok(ProtectiveExitPlanResult {
        environment: config.app.environment.clone(),
        dex: config.hyperliquid.dex.clone(),
        account_id: account.account_id.clone(),
        coin: asset.meta.name,
        entry_side,
        exit_side,
        entry_price,
        market_reference_price,
        reduce_only: true,
        dry_run,
        legs,
    })
}

pub async fn check_protective_exit_trigger(
    config: &AppConfig,
    options: ProtectiveExitTriggerCheckOptions,
    dry_run: bool,
) -> Result<ProtectiveExitTriggerCheckResult> {
    let plan = build_protective_exit_plan(config, options.exit, dry_run).await?;
    let observed_price = options
        .observed_price
        .unwrap_or(plan.market_reference_price);
    evaluate_protective_exit_trigger(plan, observed_price)
}

pub fn evaluate_protective_exit_trigger(
    plan: ProtectiveExitPlanResult,
    observed_price: f64,
) -> Result<ProtectiveExitTriggerCheckResult> {
    anyhow::ensure!(
        observed_price.is_finite() && observed_price > 0.0,
        "observed_price must be positive"
    );
    let triggered_leg = plan
        .legs
        .iter()
        .filter(|leg| {
            protective_leg_triggers(plan.exit_side, &leg.kind, observed_price, leg.trigger_price)
        })
        .min_by_key(|leg| if leg.kind == "stop_loss" { 0 } else { 1 })
        .cloned();
    let exit_order = triggered_leg
        .as_ref()
        .map(|leg| ProtectiveExitOrderPreview {
            account_id: plan.account_id.clone(),
            coin: plan.coin.clone(),
            side: plan.exit_side,
            size: leg.size,
            limit_price: leg.limit_price,
            reduce_only: true,
            cloid: leg.cloid.clone(),
            trigger_kind: leg.kind.clone(),
            trigger_price: leg.trigger_price,
            execution_policy: ExecutionPolicy::Taker,
        });

    Ok(ProtectiveExitTriggerCheckResult {
        plan,
        observed_price,
        triggered: triggered_leg.is_some(),
        triggered_leg,
        exit_order,
        checked_at_ms: now_ms(),
    })
}

pub async fn execute_protective_exit_submit(
    config: AppConfig,
    options: ProtectiveExitSubmitOptions,
    dry_run: bool,
    vault_password: Option<&str>,
) -> Result<ProtectiveExitSubmitResult> {
    let trigger = check_protective_exit_trigger(&config, options.trigger, dry_run).await?;
    if !options.submit || !trigger.triggered {
        return Ok(ProtectiveExitSubmitResult {
            trigger,
            submit_requested: options.submit,
            submitted: false,
            submit_report: None,
            post_submit_reconciliation: None,
            order_status: None,
        });
    }

    let treat_as_reduce_only_gate =
        trigger.plan.reduce_only || is_spot_dex(&config.hyperliquid.dex);
    validate_live_order_gates(
        &config,
        options.confirm_mainnet_live,
        treat_as_reduce_only_gate,
    )?;
    let account = config
        .account(&trigger.plan.account_id)
        .cloned()
        .with_context(|| format!("account {} not found in config", trigger.plan.account_id))?;
    anyhow::ensure!(
        account.enabled && account.worker_enabled,
        "account {} is not enabled for worker execution",
        account.account_id
    );
    ensure_live_account_address(&account)?;

    let exit_order = trigger
        .exit_order
        .clone()
        .context("protective exit submit requested but no exit order was triggered")?;
    let password =
        vault_password.context("vault password is required for protective exit submit")?;
    let secret = load_account_secret(&config, &account, Some(password))?;
    let executor = LiveExchangeExecutor::new(config.clone(), account.clone(), secret);
    let now = now_ms();
    let mut order_size = exit_order.size;
    if is_spot_dex(&config.hyperliquid.dex) {
        anyhow::ensure!(
            matches!(exit_order.side, OrderSide::Sell),
            "spot protective TP/SL only supports sell-to-close"
        );
        let spot_state = fetch_spot_clearinghouse_state(&config.app.environment, &account.address)
            .await
            .context("failed to fetch spot state for protective TP/SL submit")?;
        let base_available = spot_base_available_for_coin(&spot_state, &exit_order.coin);
        anyhow::ensure!(
            base_available > 0.0,
            "spot protective TP/SL has no available base inventory for {}",
            exit_order.coin
        );
        let sz_decimals = trigger
            .triggered_leg
            .as_ref()
            .map(|leg| leg.sz_decimals)
            .unwrap_or(0);
        order_size = round_size_down(order_size.min(base_available), sz_decimals);
        anyhow::ensure!(
            order_size > 0.0,
            "spot protective TP/SL size rounds to zero after inventory clamp"
        );
    }
    let notional_usd = order_size * exit_order.limit_price;
    let order = ApprovedOrder {
        risk_decision_id: format!("protective-exit-risk-{now}"),
        intent_id: format!("protective-exit-intent-{now}"),
        signal_id: Some(format!("protective-exit-signal-{now}")),
        worker_id: format!("worker-{}", account.account_id),
        account_id: account.account_id.clone(),
        strategy_id: "manual_protective_exit".to_string(),
        market: None,
        dex: Some(config.hyperliquid.dex.clone()),
        coin: exit_order.coin.clone(),
        side: exit_order.side,
        notional_usd,
        exact_size: Some(order_size),
        price: Some(exit_order.limit_price),
        execution_mode: ExecutionMode::Taker,
        execution_policy: exit_order.execution_policy,
        max_slippage_bps: 0.0,
        reduce_only: trigger.plan.reduce_only,
        cloid: exit_order.cloid.clone(),
        expires_at_ms: Some(now + config.process.signal_ttl_ms),
    };

    let report = executor.submit(order).await;
    let mut order_status = None;
    if let WorkerReport::Submitted(submitted) = &report
        && let Some(oid) = submitted.oid
    {
        match fetch_order_status_by_oid(&config.app.environment, &account.address, oid).await {
            Ok(status) => {
                order_status = Some(status);
            }
            Err(error) => {
                tracing::warn!(
                    account_id = %account.account_id,
                    %oid,
                    error = %error,
                    "best-effort protective exit order-status lookup failed; continuing"
                );
            }
        }
    }
    let (post_submit_reconciliation, reconciliation_note) =
        reconcile_account_best_effort(&config, &account.account_id).await;
    if let Some(note) = reconciliation_note {
        tracing::warn!(
            account_id = %account.account_id,
            %note,
            "best-effort protective exit post-submit reconciliation degraded"
        );
    }
    let submitted = matches!(report, WorkerReport::Submitted(_));

    Ok(ProtectiveExitSubmitResult {
        trigger,
        submit_requested: true,
        submitted,
        submit_report: Some(report),
        post_submit_reconciliation,
        order_status,
    })
}

pub async fn execute_protective_exit_arm(
    config: AppConfig,
    options: ProtectiveExitArmOptions,
    dry_run: bool,
    vault_password: Option<&str>,
) -> Result<ProtectiveExitArmResult> {
    let plan = build_protective_exit_plan(&config, options.exit, dry_run).await?;
    if !options.submit {
        return Ok(ProtectiveExitArmResult {
            plan,
            submit_requested: false,
            submitted: false,
            submit_reports: Vec::new(),
            post_submit_reconciliation: None,
            order_statuses: Vec::new(),
            persistent_rule_id: None,
            armed: None,
            monitor_mode: None,
            trigger_check: None,
        });
    }

    validate_live_order_gates(&config, options.confirm_mainnet_live, true)?;
    let account = config
        .account(&plan.account_id)
        .cloned()
        .with_context(|| format!("account {} not found in config", plan.account_id))?;
    anyhow::ensure!(
        account.enabled && account.worker_enabled,
        "account {} is not enabled for worker execution",
        account.account_id
    );
    ensure_live_account_address(&account)?;
    if is_spot_dex(&config.hyperliquid.dex) {
        anyhow::ensure!(
            matches!(plan.exit_side, OrderSide::Sell),
            "spot protective TP/SL only supports sell-to-close"
        );
        let spot_state = fetch_spot_clearinghouse_state(&config.app.environment, &account.address)
            .await
            .context("failed to fetch spot state for protective TP/SL submit")?;
        let base_available = spot_base_available_for_coin(&spot_state, &plan.coin);
        anyhow::ensure!(
            base_available > 0.0,
            "cannot submit native spot TP/SL without available base inventory; {}",
            reduce_only_spot_position_detail(plan.exit_side, base_available)
        );
    } else {
        let clearinghouse_state = fetch_clearinghouse_state(
            &config.app.environment,
            &config.hyperliquid.dex,
            &account.address,
        )
        .await
        .context("failed to fetch clearinghouse state for protective TP/SL submit")?;
        let readiness = summarize_account_readiness_state(
            &config.hyperliquid.dex,
            &clearinghouse_state,
            &plan.coin,
        );
        anyhow::ensure!(
            reduce_only_position_available(plan.exit_side, readiness.coin_position_size),
            "cannot submit exchange-native TP/SL without a matching open position; {}",
            reduce_only_position_detail(plan.exit_side, readiness.coin_position_size)
        );
    }

    let password =
        vault_password.context("vault password is required for protective TP/SL submission")?;
    let secret = load_account_secret(&config, &account, Some(password))?;
    let executor = LiveExchangeExecutor::new(config.clone(), account.clone(), secret);

    let submit_reports = executor.submit_protective_trigger_orders(&plan).await?;

    let mut order_statuses = Vec::new();
    for report in &submit_reports {
        if let WorkerReport::Submitted(submitted) = report {
            if let Some(oid) = submitted.oid {
                match fetch_order_status_by_oid(&config.app.environment, &account.address, oid)
                    .await
                {
                    Ok(status) => order_statuses.push(status),
                    Err(error) => {
                        tracing::warn!(
                            account_id = %account.account_id,
                            %oid,
                            cloid = %submitted.cloid,
                            error = %error,
                            "best-effort protective trigger order-status lookup by oid failed; continuing"
                        );
                    }
                }
            } else {
                match fetch_order_status_by_cloid(
                    &config.app.environment,
                    &account.address,
                    &submitted.cloid,
                )
                .await
                {
                    Ok(status) => order_statuses.push(status),
                    Err(error) => {
                        tracing::warn!(
                            account_id = %account.account_id,
                            cloid = %submitted.cloid,
                            error = %error,
                            "best-effort protective trigger order-status lookup by cloid failed; continuing"
                        );
                    }
                }
            }
        }
    }

    let (post_submit_reconciliation, reconciliation_note) =
        reconcile_account_best_effort(&config, &account.account_id).await;
    if let Some(note) = reconciliation_note {
        tracing::warn!(
            account_id = %account.account_id,
            %note,
            "best-effort protective trigger post-submit reconciliation degraded"
        );
    }
    let submitted = !submit_reports.is_empty()
        && submit_reports
            .iter()
            .all(|report| matches!(report, WorkerReport::Submitted(_)));

    Ok(ProtectiveExitArmResult {
        plan,
        submit_requested: true,
        submitted,
        submit_reports,
        post_submit_reconciliation,
        order_statuses,
        persistent_rule_id: None,
        armed: None,
        monitor_mode: None,
        trigger_check: None,
    })
}

pub async fn execute_fast_protective_exit_arm(
    config: AppConfig,
    options: ProtectiveExitArmOptions,
    dry_run: bool,
    vault_password: Option<&str>,
    realtime: Option<&RealtimeState>,
) -> Result<FastProtectiveExitArmResult> {
    let plan = build_protective_exit_plan(&config, options.exit, dry_run).await?;
    if !options.submit {
        return Ok(FastProtectiveExitArmResult {
            environment: config.app.environment.clone(),
            account_id: plan.account_id.clone(),
            coin: plan.coin.clone(),
            transport: "dry_plan".to_string(),
            submit_requested: false,
            submitted: false,
            submit_latency_ms: None,
            plan,
            submit_reports: Vec::new(),
            cache_notes: Vec::new(),
            warnings: Vec::new(),
        });
    }

    validate_live_order_gates(&config, options.confirm_mainnet_live, true)?;
    let account = config
        .account(&plan.account_id)
        .cloned()
        .with_context(|| format!("account {} not found in config", plan.account_id))?;
    anyhow::ensure!(
        account.enabled && account.worker_enabled,
        "account {} is not enabled for worker execution",
        account.account_id
    );
    ensure_live_account_address(&account)?;

    let mut cache_notes = Vec::new();
    let mut warnings = Vec::new();
    let canonical_coin = canonical_coin_for_dex(&config.hyperliquid.dex, &plan.coin);
    if is_spot_dex(&config.hyperliquid.dex) {
        if let Some(state) = realtime.and_then(|realtime| realtime.spot_state(&account.address)) {
            cache_notes.push("matching inventory checked from realtime spot state".to_string());
            let readiness = spot_account_readiness_state(&state, &canonical_coin);
            anyhow::ensure!(
                reduce_only_spot_position_available(plan.exit_side, readiness.coin_position_size),
                "cannot set native spot TP/SL before holding matching inventory; {}",
                reduce_only_spot_position_detail(plan.exit_side, readiness.coin_position_size)
            );
        } else {
            warnings
                .push("spot realtime state unavailable; using bounded REST pre-check".to_string());
            let state = fetch_spot_clearinghouse_state(&config.app.environment, &account.address)
                .await
                .context("failed to fetch spot state for fast protective TP/SL pre-check")?;
            let readiness = spot_account_readiness_state(&state, &canonical_coin);
            anyhow::ensure!(
                reduce_only_spot_position_available(plan.exit_side, readiness.coin_position_size),
                "cannot set native spot TP/SL before holding matching inventory; {}",
                reduce_only_spot_position_detail(plan.exit_side, readiness.coin_position_size)
            );
        }
    } else {
        let market_id = market_id_for_dex(&config.hyperliquid.dex);
        if let Some(state) =
            realtime.and_then(|realtime| realtime.clearinghouse_state(market_id, &account.address))
        {
            cache_notes.push(format!(
                "matching position checked from realtime {market_id} state"
            ));
            let readiness =
                summarize_account_readiness_state(&config.hyperliquid.dex, &state, &canonical_coin);
            anyhow::ensure!(
                reduce_only_position_available(plan.exit_side, readiness.coin_position_size),
                "cannot set exchange-native TP/SL before opening a matching position; {}",
                reduce_only_position_detail(plan.exit_side, readiness.coin_position_size)
            );
        } else {
            warnings.push(
                "perp realtime state unavailable; using bounded REST position pre-check"
                    .to_string(),
            );
            let state = fetch_clearinghouse_state(
                &config.app.environment,
                &config.hyperliquid.dex,
                &account.address,
            )
            .await
            .context("failed to fetch clearinghouse state for fast protective TP/SL pre-check")?;
            let readiness =
                summarize_account_readiness_state(&config.hyperliquid.dex, &state, &canonical_coin);
            anyhow::ensure!(
                reduce_only_position_available(plan.exit_side, readiness.coin_position_size),
                "cannot set exchange-native TP/SL before opening a matching position; {}",
                reduce_only_position_detail(plan.exit_side, readiness.coin_position_size)
            );
        }
    }

    if let Some(realtime) = realtime {
        let market_id = market_id_for_dex(&config.hyperliquid.dex);
        if let Some(open_orders) = realtime.open_orders(market_id, &account.address) {
            cache_notes.push(format!(
                "existing protective order guard checked from realtime {market_id} open orders"
            ));
            anyhow::ensure!(
                !open_orders
                    .iter()
                    .any(|order| is_native_protective_open_order(order, &plan.coin)),
                "fast TP/SL refuses to replace existing protective orders; use strict replacement flow"
            );
        }
    }

    let password = vault_password
        .context("vault password is required for fast protective TP/SL submission")?;
    let secret = load_account_secret(&config, &account, Some(password))?;
    let executor = LiveExchangeExecutor::new(config.clone(), account, secret);
    let started = Instant::now();
    let submit_reports = executor
        .submit_protective_trigger_orders_fast(&plan)
        .await?;
    let submit_latency_ms = started.elapsed().as_millis() as u64;
    let submitted = !submit_reports.is_empty()
        && submit_reports
            .iter()
            .all(|report| matches!(report, WorkerReport::Submitted(_)));

    Ok(FastProtectiveExitArmResult {
        environment: config.app.environment,
        account_id: plan.account_id.clone(),
        coin: plan.coin.clone(),
        transport: "websocket_post_action".to_string(),
        submit_requested: true,
        submitted,
        submit_latency_ms: Some(submit_latency_ms),
        plan,
        submit_reports,
        cache_notes,
        warnings,
    })
}

pub async fn execute_cancel_by_cloid(
    config: AppConfig,
    account_id: String,
    coin: String,
    cloid: String,
    confirm_mainnet_live: bool,
    vault_password: &str,
) -> Result<CancelByCloidResult> {
    validate_live_action_gates(&config, confirm_mainnet_live)?;
    let account = config
        .account(&account_id)
        .cloned()
        .with_context(|| format!("account {account_id} not found in config"))?;
    anyhow::ensure!(
        account.enabled && account.worker_enabled,
        "account {} is not enabled for worker execution",
        account.account_id
    );
    ensure_live_account_address(&account)?;
    anyhow::ensure!(
        uuid::Uuid::parse_str(&cloid).is_ok(),
        "cancel cloid must be a valid UUID"
    );

    let secret = load_account_secret(&config, &account, Some(vault_password))?;
    let canonical_coin = canonical_coin_for_dex(&config.hyperliquid.dex, &coin);
    let executor = LiveExchangeExecutor::new(config.clone(), account.clone(), secret);
    let cancel_response = executor.cancel_by_cloid(&canonical_coin, &cloid).await?;
    // Post-cancel status lookup is useful for diagnostics but should not fail the cancel path.
    // Hyperliquid may occasionally return non-uniform status payloads immediately after cancel.
    let order_status_after =
        fetch_order_status_by_cloid(&config.app.environment, &account.address, &cloid)
            .await
            .ok();
    let open_orders = fetch_open_orders(
        &config.app.environment,
        &info_query_dex(&config.hyperliquid.dex),
        &account.address,
    )
    .await
    .context("failed to reconcile open orders after cancel")?;
    let matching_open_after = open_orders.iter().any(|order| {
        order.coin == canonical_coin && order.cloid.as_deref() == Some(cloid.as_str())
    });

    Ok(CancelByCloidResult {
        environment: config.app.environment,
        account_id: account.account_id,
        coin: canonical_coin,
        cloid,
        cancel_response: format!("{cancel_response:?}"),
        order_status_after,
        open_orders_after: open_orders.len(),
        matching_open_after,
    })
}

pub async fn execute_cancel_open_order(
    config: AppConfig,
    account_id: String,
    coin: String,
    cloid: Option<String>,
    oid: Option<u64>,
    confirm_mainnet_live: bool,
    vault_password: &str,
) -> Result<CancelOpenOrderResult> {
    validate_live_action_gates(&config, confirm_mainnet_live)?;
    let account = config
        .account(&account_id)
        .cloned()
        .with_context(|| format!("account {account_id} not found in config"))?;
    anyhow::ensure!(
        account.enabled && account.worker_enabled,
        "account {} is not enabled for worker execution",
        account.account_id
    );
    ensure_live_account_address(&account)?;
    anyhow::ensure!(
        oid.is_some()
            || cloid
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty()),
        "cancel requires oid or cloid"
    );

    let secret = load_account_secret(&config, &account, Some(vault_password))?;
    let canonical_coin = normalize_order_coin_for_cancel(&config.hyperliquid.dex, &coin);
    let executor = LiveExchangeExecutor::new(config.clone(), account.clone(), secret);
    let cancel_response = if let Some(oid) = oid {
        executor.cancel_by_oid(&canonical_coin, oid).await?
    } else {
        executor
            .cancel_by_cloid(
                &canonical_coin,
                cloid
                    .as_deref()
                    .expect("cloid existence checked before cancel"),
            )
            .await?
    };

    let open_orders = fetch_open_orders(
        &config.app.environment,
        &info_query_dex(&config.hyperliquid.dex),
        &account.address,
    )
    .await
    .context("failed to reconcile open orders after cancel")?;
    let matching_open_after = open_orders.iter().any(|order| {
        oid.is_some_and(|target_oid| order.oid == target_oid)
            || cloid
                .as_deref()
                .zip(order.cloid.as_deref())
                .is_some_and(|(target, open)| target.eq_ignore_ascii_case(open))
    });

    Ok(CancelOpenOrderResult {
        environment: config.app.environment,
        account_id: account.account_id,
        coin: canonical_coin,
        cloid,
        oid,
        cancel_response: format!("{cancel_response:?}"),
        open_orders_after: open_orders.len(),
        matching_open_after,
    })
}

pub async fn execute_manual_leverage_update(
    config: AppConfig,
    options: ManualLeverageUpdateOptions,
    vault_password: Option<&str>,
) -> Result<ManualLeverageUpdateResult> {
    anyhow::ensure!(
        config.manual_ops.enabled && config.manual_ops.manual_trading_enabled,
        "manual trading is disabled"
    );
    let account = config
        .account(&options.account_id)
        .cloned()
        .with_context(|| format!("account {} not found in config", options.account_id))?;
    anyhow::ensure!(
        account.enabled && account.worker_enabled,
        "account {} is not enabled for worker execution",
        account.account_id
    );

    let canonical_coin = normalize_dex_coin(&config.hyperliquid.dex, &options.coin);
    if !config.manual_ops.blocked_symbols.is_empty() {
        anyhow::ensure!(
            !config.manual_ops.blocked_symbols.contains(&canonical_coin),
            "manual symbol {} is blocked",
            canonical_coin
        );
    }

    let margin_mode = parse_manual_margin_mode(&options.margin_mode)?;
    anyhow::ensure!(options.leverage >= 1, "leverage must be at least 1x");
    let snapshot =
        fetch_xyz_market_snapshot_cached(&config.app.environment, &config.hyperliquid.dex, 15_000)
            .await
            .context("failed to fetch XYZ market snapshot")?;
    let asset = snapshot.asset(&canonical_coin)?;
    if let Some(max) = asset.meta.max_leverage {
        anyhow::ensure!(
            options.leverage <= max,
            "leverage {}x exceeds max leverage {}x for {}",
            options.leverage,
            max,
            canonical_coin
        );
    }
    if margin_mode.is_cross() {
        anyhow::ensure!(
            asset_supports_cross_margin(
                asset.meta.only_isolated,
                asset.meta.margin_mode.as_deref()
            ),
            "{} is isolated-only in current metadata and cannot be set to cross margin",
            canonical_coin
        );
    }

    let mut result = ManualLeverageUpdateResult {
        environment: config.app.environment.clone(),
        dex: config.hyperliquid.dex.clone(),
        account_id: account.account_id.clone(),
        coin: canonical_coin.clone(),
        leverage: options.leverage,
        margin_mode: margin_mode.as_str().to_string(),
        is_cross: margin_mode.is_cross(),
        max_leverage: asset.meta.max_leverage,
        only_isolated: asset.meta.only_isolated,
        exchange_margin_mode: asset.meta.margin_mode.clone(),
        submit_requested: options.submit,
        submitted: false,
        exchange_response: None,
    };

    if !options.submit {
        return Ok(result);
    }

    validate_live_action_gates(&config, options.confirm_mainnet_live)?;
    ensure_live_account_address(&account)?;
    let password = vault_password.context("vault password is required for leverage update")?;
    let secret = load_account_secret(&config, &account, Some(password))?;
    let executor = LiveExchangeExecutor::new(config, account, secret);
    let response = executor
        .update_leverage(&canonical_coin, options.leverage, margin_mode.is_cross())
        .await?;
    result.submitted = true;
    result.exchange_response = Some(format!("{response:?}"));
    Ok(result)
}

pub async fn run_signed_cancel_by_cloid(
    config: AppConfig,
    account_id: String,
    coin: String,
    cloid: String,
    confirm_mainnet_live: bool,
) -> Result<()> {
    validate_live_action_gates(&config, confirm_mainnet_live)?;
    let account = config
        .account(&account_id)
        .with_context(|| format!("account {account_id} not found in config"))?;
    anyhow::ensure!(
        account.enabled && account.worker_enabled,
        "account {} is not enabled for worker execution",
        account.account_id
    );
    ensure_live_account_address(account)?;
    anyhow::ensure!(
        uuid::Uuid::parse_str(&cloid).is_ok(),
        "cancel cloid must be a valid UUID"
    );
    let password = std::env::var("TRADE_XYZ_VAULT_PASSWORD")
        .context("TRADE_XYZ_VAULT_PASSWORD is required for signed cancel")?;
    let result = execute_cancel_by_cloid(
        config,
        account_id,
        coin,
        cloid,
        confirm_mainnet_live,
        &password,
    )
    .await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

pub async fn run_signed_smoke(config: AppConfig, options: SignedSmokeOptions) -> Result<()> {
    let password = if options.submit {
        validate_signed_submit_gates(&config, &options)?;
        let account = config
            .account(&options.account_id)
            .with_context(|| format!("account {} not found in config", options.account_id))?;
        anyhow::ensure!(
            account.enabled && account.worker_enabled,
            "account {} is not enabled for worker execution",
            account.account_id
        );
        ensure_live_account_address(account)?;
        Some(
            std::env::var("TRADE_XYZ_VAULT_PASSWORD")
                .context("TRADE_XYZ_VAULT_PASSWORD is required for signed smoke submit")?,
        )
    } else {
        None
    };
    let result = execute_signed_smoke(config, options, password.as_deref()).await?;
    println!(
        "signed smoke plan: env={} account={} coin={} asset_id={} ref_px={} limit_px={} size={} sz_decimals={} execution_mode={:?} tif={} reduce_only={} submit={}",
        result.plan.environment,
        result.plan.account_id,
        result.plan.coin,
        result.plan.asset_id,
        result.plan.reference_price,
        result.plan.limit_price,
        result.plan.size,
        result.plan.sz_decimals,
        result.plan.execution_mode,
        result.plan.tif,
        result.plan.reduce_only,
        result.plan.submit
    );
    if !result.plan.submit {
        println!(
            "signed smoke stopped before signing/submission; pass --submit for testnet live smoke"
        );
        return Ok(());
    }
    if let Some(report) = &result.submit_report {
        println!("signed smoke submit report: {report:?}");
    }
    if let Some(cancel_response) = &result.cancel_response {
        println!("signed smoke cancel_by_cloid response: {cancel_response}");
    }
    if let Some(reconciliation) = &result.reconciliation {
        println!(
            "signed smoke reconciliation: open_orders={} matching_open={} xyz_fills={} matching_fills={} order_status={}",
            reconciliation.open_orders,
            reconciliation.matching_open,
            reconciliation.xyz_fills,
            reconciliation.matching_fills,
            reconciliation
                .order_status
                .as_ref()
                .map(|status| status.status.as_str())
                .unwrap_or("n/a")
        );
    }
    Ok(())
}

pub async fn run_signed_acceptance(
    config: AppConfig,
    options: SignedAcceptanceOptions,
) -> Result<()> {
    let password = if options.submit {
        let close_gate = signed_close_exempt_from_opening_rules(
            &config.hyperliquid.dex,
            options.side,
            options.reduce_only,
            options.close_full_position,
        );
        validate_live_order_gates(&config, options.confirm_mainnet_live, close_gate)?;
        let account = config
            .account(&options.account_id)
            .with_context(|| format!("account {} not found in config", options.account_id))?;
        anyhow::ensure!(
            account.enabled && account.worker_enabled,
            "account {} is not enabled for worker execution",
            account.account_id
        );
        ensure_live_account_address(account)?;
        Some(
            std::env::var("TRADE_XYZ_VAULT_PASSWORD")
                .context("TRADE_XYZ_VAULT_PASSWORD is required for signed acceptance submit")?,
        )
    } else {
        None
    };
    let result = execute_signed_acceptance(config, options, password.as_deref()).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

pub async fn run_signed_preflight(
    config: AppConfig,
    options: SignedPreflightOptions,
) -> Result<()> {
    let password = std::env::var("TRADE_XYZ_VAULT_PASSWORD").ok();
    let result = execute_signed_preflight(config, options, password.as_deref()).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

pub async fn run_account_reconciliation(config: AppConfig, account_id: String) -> Result<()> {
    let result = reconcile_account(&config, &account_id).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

pub async fn run_order_status(
    config: AppConfig,
    account_id: String,
    oid: Option<u64>,
    cloid: Option<String>,
) -> Result<()> {
    let lookup = order_status_lookup(oid, cloid)?;
    let result = query_order_status(&config, &account_id, lookup).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

pub async fn execute_usdc_dex_transfer(
    config: AppConfig,
    options: UsdcDexTransferOptions,
    vault_password: Option<&str>,
) -> Result<UsdcDexTransferResult> {
    validate_usdc_dex_transfer_gates(&config, &options)?;
    let source_account = config
        .account(&options.account_id)
        .cloned()
        .with_context(|| format!("account {} not found in config", options.account_id))?;
    let destination_account_id = normalized_transfer_destination_account_id(
        &source_account.account_id,
        options.destination_account_id.as_deref(),
    );
    let destination_account = config
        .account(&destination_account_id)
        .cloned()
        .with_context(|| format!("account {} not found in config", destination_account_id))?;
    anyhow::ensure!(
        source_account.enabled && source_account.worker_enabled,
        "account {} is not enabled for worker execution",
        source_account.account_id
    );
    anyhow::ensure!(
        destination_account.enabled && destination_account.worker_enabled,
        "destination account {} is not enabled for worker execution",
        destination_account.account_id
    );
    ensure_live_account_address(&source_account)?;
    ensure_live_account_address(&destination_account)?;

    let source_dex = normalize_transfer_layer(options.source_dex.as_deref().unwrap_or_default());
    let destination_dex = options
        .destination_dex
        .as_deref()
        .map(normalize_transfer_layer)
        .unwrap_or_else(|| normalize_transfer_layer(&config.hyperliquid.dex));
    let amount = format_usdc_amount(options.amount_usdc)?;

    let before_source = fetch_usdc_layer_snapshot(
        &config.app.environment,
        &source_dex,
        &source_account.address,
    )
    .await
    .with_context(|| {
        format!(
            "failed to fetch source layer {} state before transfer",
            usdc_transfer_layer_label(&source_dex)
        )
    })?;
    let before_destination = fetch_usdc_layer_snapshot(
        &config.app.environment,
        &destination_dex,
        &destination_account.address,
    )
    .await
    .with_context(|| {
        format!(
            "failed to fetch destination layer {} state before transfer",
            usdc_transfer_layer_label(&destination_dex)
        )
    })?;
    let before = dex_transfer_balances(&before_source, &before_destination);
    anyhow::ensure!(
        before.source_available_usdc + 1e-9 >= options.amount_usdc,
        "source layer {} available {} is below requested transfer {}",
        usdc_transfer_layer_label(&source_dex),
        before.source_available_usdc,
        options.amount_usdc
    );

    if !options.submit {
        return Ok(UsdcDexTransferResult {
            environment: config.app.environment,
            account_id: source_account.account_id,
            address: source_account.address,
            destination_account_id: destination_account.account_id,
            destination_address: destination_account.address,
            source_dex,
            destination_dex,
            token: MAINNET_USDC_TOKEN.to_string(),
            amount,
            amount_usdc: options.amount_usdc,
            submit_requested: false,
            submitted: false,
            signer_address: None,
            exchange_response: None,
            before,
            after: None,
        });
    }

    let password = vault_password.context("vault password is required for USDC DEX transfer")?;
    let secret = load_transfer_secret(&config, &source_account, Some(password))?;
    let submit = submit_send_asset(SendAssetSubmitRequest {
        exchange_url: &config.hyperliquid.exchange_url,
        environment: &config.app.environment,
        wallet_private_key: &secret.private_key,
        destination: &destination_account.address,
        source_dex: &source_dex,
        destination_dex: &destination_dex,
        token: MAINNET_USDC_TOKEN,
        amount: &amount,
        from_sub_account: "",
    })
    .await?;
    anyhow::ensure!(
        submit
            .response
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|status| status == "ok"),
        "sendAsset rejected or returned unexpected status: {}",
        submit.response
    );

    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    let after_source = fetch_usdc_layer_snapshot(
        &config.app.environment,
        &source_dex,
        &source_account.address,
    )
    .await
    .with_context(|| {
        format!(
            "failed to fetch source layer {} state after transfer",
            usdc_transfer_layer_label(&source_dex)
        )
    })?;
    let after_destination = fetch_usdc_layer_snapshot(
        &config.app.environment,
        &destination_dex,
        &destination_account.address,
    )
    .await
    .with_context(|| {
        format!(
            "failed to fetch destination layer {} state after transfer",
            usdc_transfer_layer_label(&destination_dex)
        )
    })?;

    Ok(UsdcDexTransferResult {
        environment: config.app.environment,
        account_id: source_account.account_id,
        address: source_account.address,
        destination_account_id: destination_account.account_id,
        destination_address: destination_account.address,
        source_dex,
        destination_dex,
        token: MAINNET_USDC_TOKEN.to_string(),
        amount,
        amount_usdc: options.amount_usdc,
        submit_requested: true,
        submitted: true,
        signer_address: Some(submit.signer_address),
        exchange_response: Some(submit.response),
        before,
        after: Some(dex_transfer_balances(&after_source, &after_destination)),
    })
}

pub async fn run_usdc_dex_transfer(
    config: AppConfig,
    options: UsdcDexTransferOptions,
) -> Result<()> {
    let password = if options.submit {
        std::env::var("TRADE_XYZ_VAULT_PASSWORD").ok()
    } else {
        None
    };
    let result = execute_usdc_dex_transfer(config, options, password.as_deref()).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

pub async fn execute_usdc_dex_transfer_preflight(
    config: AppConfig,
    options: UsdcDexTransferOptions,
    vault_password: Option<&str>,
) -> Result<UsdcDexTransferPreflightResult> {
    let mut checks = Vec::new();
    let source_account = config.account(&options.account_id).cloned();
    let destination_account_id = normalized_transfer_destination_account_id(
        &options.account_id,
        options.destination_account_id.as_deref(),
    );
    let destination_account = config.account(&destination_account_id).cloned();
    let source_dex = normalize_transfer_layer(options.source_dex.as_deref().unwrap_or_default());
    let destination_dex = options
        .destination_dex
        .as_deref()
        .map(normalize_transfer_layer)
        .unwrap_or_else(|| normalize_transfer_layer(&config.hyperliquid.dex));
    let confirmation_phrase = (config.app.environment == "mainnet").then(|| {
        usdc_transfer_confirmation_phrase(
            &options.account_id,
            options.amount_usdc,
            &destination_dex,
        )
    });
    let needs_mainnet_confirmation = config.app.environment == "mainnet" && options.submit;

    checks.push(SignedPreflightCheck::blocker(
        "account_configured",
        source_account.is_some(),
        if source_account.is_some() {
            "account exists in current config".to_string()
        } else {
            format!("account {} is missing from config", options.account_id)
        },
    ));

    if let Some(account) = &source_account {
        checks.push(SignedPreflightCheck::blocker(
            "account_enabled",
            account.enabled && account.worker_enabled,
            "account must be enabled and worker_enabled",
        ));
        checks.push(SignedPreflightCheck::blocker(
            "address_not_placeholder",
            is_probably_real_evm_address(&account.address),
            "account address must be a real master/subaccount address, not an example placeholder",
        ));
    }
    checks.push(SignedPreflightCheck::blocker(
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
    if let Some(account) = &destination_account {
        checks.push(SignedPreflightCheck::blocker(
            "destination_account_enabled",
            account.enabled && account.worker_enabled,
            "destination account must be enabled and worker_enabled",
        ));
        checks.push(SignedPreflightCheck::blocker(
            "destination_address_not_placeholder",
            is_probably_real_evm_address(&account.address),
            "destination account address must be a real master/subaccount address, not an example placeholder",
        ));
    }

    checks.push(SignedPreflightCheck::blocker(
        "amount_positive",
        options.amount_usdc.is_finite() && options.amount_usdc > 0.0,
        "amount_usdc must be positive",
    ));
    checks.push(SignedPreflightCheck::blocker(
        "amount_within_helper_cap",
        options.amount_usdc.is_finite() && options.amount_usdc <= 10.0,
        "funding transfer helper is capped at 10 USDC per account",
    ));
    checks.push(SignedPreflightCheck::blocker(
        "source_layer_supported",
        usdc_transfer_layer_supported(&source_dex),
        "source layer must be default_perp (empty), spot, or a valid perp dex name",
    ));
    checks.push(SignedPreflightCheck::blocker(
        "destination_layer_supported",
        usdc_transfer_layer_supported(&destination_dex),
        "destination layer must be default_perp (empty), spot, or a valid perp dex name",
    ));
    let source_address = source_account
        .as_ref()
        .map(|account| account.address.as_str());
    let destination_address = destination_account
        .as_ref()
        .map(|account| account.address.as_str());
    checks.push(SignedPreflightCheck::blocker(
        "route_changes_state",
        source_address.is_none()
            || destination_address.is_none()
            || source_address != destination_address
            || source_dex != destination_dex,
        "source and destination must not be the same account layer",
    ));
    checks.push(SignedPreflightCheck::blocker(
        "config_dry_run_disabled",
        !config.app.dry_run,
        "USDC transfer submit requires app.dry_run=false in the config file",
    ));
    checks.push(SignedPreflightCheck::blocker(
        "manual_live_enabled",
        config.manual_ops.manual_live_enabled,
        "USDC transfer submit requires manual_ops.manual_live_enabled=true",
    ));
    checks.push(SignedPreflightCheck::blocker(
        "mainnet_gate",
        config.app.environment != "mainnet" || config.manual_ops.mainnet_live_enabled,
        "mainnet transfer requires manual_ops.mainnet_live_enabled=true",
    ));
    checks.push(SignedPreflightCheck::blocker(
        "mainnet_explicit_confirmation",
        !needs_mainnet_confirmation || options.confirm_mainnet_live,
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
    checks.push(SignedPreflightCheck::blocker(
        "vault_file_exists",
        vault_path.exists(),
        format!("vault path: {}", vault_path.display()),
    ));
    checks.push(SignedPreflightCheck::blocker(
        "vault_password_available",
        vault_password.is_some(),
        "Vault password must be available from the frontend unlock session or TRADE_XYZ_VAULT_PASSWORD",
    ));

    let transfer_secret_check =
        if let (Some(account), Some(password)) = (&source_account, vault_password) {
            transfer_secret_readiness_check(&config, account, Some(password))
        } else {
            Err(anyhow::anyhow!(
                "Vault password is required to load and validate the EVM transfer signer"
            ))
        };
    checks.push(SignedPreflightCheck::blocker(
        "evm_transfer_signer_available",
        transfer_secret_check.is_ok(),
        transfer_secret_check.unwrap_or_else(|error| error.to_string()),
    ));

    let rate_limit = if let Some(account) = &source_account {
        match fetch_user_rate_limit(&config.app.environment, &account.address).await {
            Ok(rate_limit) => {
                let remaining = rate_limit.request_capacity_remaining();
                checks.push(SignedPreflightCheck::blocker(
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
                checks.push(SignedPreflightCheck::blocker(
                    "user_rate_limit_has_capacity",
                    remaining > 0,
                    format!("remaining request capacity before local throttling: {remaining}"),
                ));
                Some(rate_limit)
            }
            Err(error) => {
                checks.push(SignedPreflightCheck::blocker(
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

    let mut plan_options = options.clone();
    plan_options.submit = false;
    let plan = match execute_usdc_dex_transfer(config.clone(), plan_options, None).await {
        Ok(plan) => {
            checks.push(SignedPreflightCheck::blocker(
                "source_layer_available_sufficient",
                plan.before.source_available_usdc + 1e-9 >= options.amount_usdc,
                format!(
                    "source layer {} available {} must cover transfer {}",
                    usdc_transfer_layer_label(&source_dex),
                    plan.before.source_available_usdc,
                    options.amount_usdc
                ),
            ));
            checks.push(SignedPreflightCheck::blocker(
                "transfer_plan_valid",
                true,
                "read-only transfer plan constructed without signing",
            ));
            Some(plan)
        }
        Err(error) => {
            checks.push(SignedPreflightCheck::blocker(
                "transfer_plan_valid",
                false,
                error.to_string(),
            ));
            None
        }
    };

    let blockers_clear = checks
        .iter()
        .filter(|check| check.severity == "blocker")
        .all(|check| check.ok);
    let ready_for_testnet_transfer = blockers_clear && config.app.environment == "testnet";
    let ready_for_mainnet_transfer = blockers_clear
        && config.app.environment == "mainnet"
        && config.manual_ops.mainnet_live_enabled;
    let failed_blockers = failed_preflight_blockers(&checks);
    let next_actions = usdc_transfer_preflight_next_actions(
        &checks,
        &config.app.environment,
        &source_dex,
        &destination_dex,
        confirmation_phrase.as_deref(),
    );
    let readiness_summary = usdc_transfer_preflight_summary(
        &config.app.environment,
        ready_for_testnet_transfer,
        ready_for_mainnet_transfer,
        &failed_blockers,
    );

    Ok(UsdcDexTransferPreflightResult {
        environment: config.app.environment,
        dry_run: config.app.dry_run,
        account_id: options.account_id,
        address: source_account.map(|account| account.address),
        destination_account_id,
        destination_address: destination_account.map(|account| account.address),
        amount_usdc: options.amount_usdc,
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

pub async fn run_usdc_dex_transfer_preflight(
    config: AppConfig,
    options: UsdcDexTransferOptions,
) -> Result<()> {
    let password = std::env::var("TRADE_XYZ_VAULT_PASSWORD").ok();
    let result = execute_usdc_dex_transfer_preflight(config, options, password.as_deref()).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

pub async fn execute_usdc_dex_transfer_batch_preflight(
    config: AppConfig,
    options: UsdcDexTransferBatchPreflightOptions,
    vault_password: Option<&str>,
) -> Result<UsdcDexTransferBatchPreflightResult> {
    anyhow::ensure!(
        options.amount_usdc.is_finite() && options.amount_usdc > 0.0,
        "amount_usdc must be positive"
    );
    anyhow::ensure!(
        options.amount_usdc <= 10.0,
        "USDC transfer batch preflight is capped at 10 USDC per account"
    );

    let account_ids = account_funding_account_ids(&config, &options.account_ids);
    anyhow::ensure!(
        !account_ids.is_empty(),
        "at least one account id is required for USDC transfer batch preflight"
    );
    anyhow::ensure!(
        account_ids.len() <= config.manual_ops.max_manual_batch_accounts,
        "selected account count {} exceeds manual_ops.max_manual_batch_accounts {}",
        account_ids.len(),
        config.manual_ops.max_manual_batch_accounts
    );

    let source_dex = normalize_transfer_layer(options.source_dex.as_deref().unwrap_or_default());
    let destination_dex = options
        .destination_dex
        .as_deref()
        .map(normalize_transfer_layer)
        .unwrap_or_else(|| normalize_transfer_layer(&config.hyperliquid.dex));
    let preflight_futures = account_ids.iter().cloned().map(|account_id| {
        let preflight_options = UsdcDexTransferOptions {
            account_id: account_id.clone(),
            destination_account_id: options.destination_account_id.clone(),
            amount_usdc: options.amount_usdc,
            source_dex: options.source_dex.clone(),
            destination_dex: options.destination_dex.clone(),
            submit: false,
            confirm_mainnet_live: options.confirm_mainnet_live,
        };
        let config = config.clone();
        async move {
            let result = Box::pin(execute_usdc_dex_transfer_preflight(
                config,
                preflight_options,
                vault_password,
            ))
            .await;
            (account_id, result)
        }
    });
    let mut ready_account_ids = Vec::new();
    let mut blocked_account_ids = Vec::new();
    let mut failed_account_ids = Vec::new();
    let mut results = Vec::new();

    for (account_id, result) in join_all(preflight_futures).await {
        match result {
            Ok(data) => {
                if data.ready_for_testnet_transfer || data.ready_for_mainnet_transfer {
                    ready_account_ids.push(account_id.clone());
                } else {
                    blocked_account_ids.push(account_id.clone());
                }
                results.push(UsdcDexTransferBatchPreflightAccountResult {
                    ok: true,
                    data: Some(data),
                    error: None,
                });
            }
            Err(error) => {
                failed_account_ids.push(account_id.clone());
                results.push(UsdcDexTransferBatchPreflightAccountResult {
                    ok: false,
                    data: None,
                    error: Some(error.to_string()),
                });
            }
        }
    }

    let next_actions = usdc_transfer_batch_preflight_next_actions(
        &account_ids,
        &ready_account_ids,
        &blocked_account_ids,
        &failed_account_ids,
        options.amount_usdc,
        &source_dex,
        &destination_dex,
    );

    Ok(UsdcDexTransferBatchPreflightResult {
        environment: config.app.environment,
        dex: config.hyperliquid.dex,
        account_ids,
        ready_account_ids,
        blocked_account_ids,
        failed_account_ids,
        amount_usdc: options.amount_usdc,
        source_dex,
        destination_dex,
        results,
        next_actions,
    })
}

pub async fn run_usdc_dex_transfer_batch_preflight(
    config: AppConfig,
    options: UsdcDexTransferBatchPreflightOptions,
) -> Result<()> {
    let password = std::env::var("TRADE_XYZ_VAULT_PASSWORD").ok();
    let result =
        execute_usdc_dex_transfer_batch_preflight(config, options, password.as_deref()).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

pub async fn execute_usdc_dex_transfer_runbook(
    config: AppConfig,
    mut options: UsdcDexTransferOptions,
    vault_password: Option<&str>,
) -> Result<UsdcDexTransferRunbookResult> {
    let submit_requested = options.submit;
    let mut preflight_options = options.clone();
    preflight_options.submit = false;
    let preflight =
        execute_usdc_dex_transfer_preflight(config.clone(), preflight_options, vault_password)
            .await?;

    let ready = preflight.ready_for_testnet_transfer || preflight.ready_for_mainnet_transfer;
    let mut checks = vec![
        signed_runbook_check(
            "preflight_ready",
            ready,
            preflight.readiness_summary.clone(),
        ),
        signed_runbook_check(
            "submit_requested",
            submit_requested,
            if submit_requested {
                "operator requested signed USDC transfer submit".to_string()
            } else {
                "read-only transfer runbook; no submit requested".to_string()
            },
        ),
    ];

    let mut transfer = None;
    let mut submitted = false;
    if submit_requested && ready {
        options.submit = true;
        let transfer_result =
            execute_usdc_dex_transfer(config.clone(), options.clone(), vault_password).await?;
        submitted = transfer_result.submitted;
        transfer = Some(transfer_result);
    }
    checks.push(signed_runbook_check(
        "transfer_submitted",
        !submit_requested || submitted,
        if submitted {
            "signed USDC transfer submitted and post-transfer balances fetched".to_string()
        } else if submit_requested {
            "preflight blockers remain; USDC transfer did not load secrets or submit".to_string()
        } else {
            "submit was not requested".to_string()
        },
    ));

    Ok(UsdcDexTransferRunbookResult {
        environment: config.app.environment,
        account_id: preflight.account_id.clone(),
        amount_usdc: preflight.amount_usdc,
        submit_requested,
        submitted,
        preflight,
        transfer,
        checks,
    })
}

pub async fn run_usdc_dex_transfer_runbook(
    config: AppConfig,
    options: UsdcDexTransferOptions,
) -> Result<()> {
    let password = std::env::var("TRADE_XYZ_VAULT_PASSWORD").ok();
    let result = execute_usdc_dex_transfer_runbook(config, options, password.as_deref()).await?;
    let submit_requested = result.submit_requested;
    let submitted = result.submitted;
    println!("{}", serde_json::to_string_pretty(&result)?);
    anyhow::ensure!(
        !submit_requested || submitted,
        "USDC transfer runbook requested submit but no transfer was submitted; inspect preflight checks"
    );
    Ok(())
}

pub fn prepare_usdc_dex_transfer_live_window(
    source_config_path: &Path,
    config: AppConfig,
    options: UsdcDexTransferLiveWindowOptions,
) -> Result<UsdcDexTransferLiveWindowResult> {
    anyhow::ensure!(
        options.amount_usdc.is_finite() && options.amount_usdc > 0.0,
        "amount_usdc must be positive"
    );
    anyhow::ensure!(
        options.amount_usdc <= 10.0,
        "USDC transfer live window is capped at 10 USDC per account"
    );
    anyhow::ensure!(
        config.app.environment == "mainnet",
        "live window helper is only for mainnet funding transfer preparation"
    );
    let destination_dex = options
        .destination_dex
        .clone()
        .unwrap_or_else(|| config.hyperliquid.dex.clone());
    let destination_dex = normalize_transfer_layer(&destination_dex);
    anyhow::ensure!(
        usdc_transfer_layer_supported(&destination_dex),
        "destination layer must be default_perp (empty), spot, or a valid perp dex name"
    );

    let account_ids = live_window_account_ids(&config, &options.account_ids)?;
    anyhow::ensure!(
        account_ids.len() <= config.manual_ops.max_manual_batch_accounts,
        "selected account count {} exceeds manual_ops.max_manual_batch_accounts {}",
        account_ids.len(),
        config.manual_ops.max_manual_batch_accounts
    );

    let amount_label = format_usdc_amount(options.amount_usdc)?;
    let output_config = options.output_config_path.clone();
    validate_live_window_output_path(&output_config)?;
    let mut accounts = Vec::new();
    for account_id in &account_ids {
        let account = config
            .account(account_id)
            .with_context(|| format!("account {account_id} not found in config"))?;
        anyhow::ensure!(
            account.enabled && account.worker_enabled,
            "account {} is not enabled for worker execution",
            account.account_id
        );
        ensure_live_account_address(account)?;
        let secret_id = account_secret_id(account);
        anyhow::ensure!(
            !secret_id.trim().is_empty(),
            "account {} must have a Vault secret_id/api_wallet_env for trading and a transfer_secret_id/transfer_wallet_env for USDC funding transfers",
            account.account_id
        );
        let confirmation_phrase =
            usdc_transfer_confirmation_phrase(account_id, options.amount_usdc, &destination_dex);
        let mut runbook_args = vec![
            "cargo".to_string(),
            "run".to_string(),
            "--".to_string(),
            "usdc-dex-transfer-runbook".to_string(),
            "--config".to_string(),
            output_config.to_string_lossy().into_owned(),
            "--account-id".to_string(),
            account_id.clone(),
            "--amount-usdc".to_string(),
            amount_label.clone(),
            "--submit".to_string(),
            "true".to_string(),
            "--confirm-mainnet-live".to_string(),
            "true".to_string(),
        ];
        if !destination_dex.trim().is_empty() {
            runbook_args.push("--destination-dex".to_string());
            runbook_args.push(destination_dex.clone());
        }
        accounts.push(UsdcDexTransferLiveWindowAccount {
            account_id: account_id.clone(),
            address: account.address.clone(),
            secret_id,
            confirmation_phrase,
            runbook_args,
        });
    }

    let mut live_config = config.clone();
    live_config.app.dry_run = false;
    live_config.manual_ops.manual_live_enabled = true;
    live_config.manual_ops.mainnet_live_enabled = true;
    let required_config_changes = vec![
        "app.dry_run=false".to_string(),
        "manual_ops.manual_live_enabled=true".to_string(),
        "manual_ops.mainnet_live_enabled=true".to_string(),
    ];

    let config_written = if options.write {
        if output_config.exists() && !options.overwrite {
            anyhow::bail!(
                "output config {} already exists; pass --overwrite true to replace it",
                output_config.display()
            );
        }
        if let Some(parent) = output_config.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        save_config(&output_config, &live_config)?;
        true
    } else {
        false
    };

    let next_actions = vec![
        "Unlock the Vault in the current frontend process or set TRADE_XYZ_VAULT_PASSWORD in the current PowerShell session.".to_string(),
        format!(
            "Before any submit, receive exact user approval for {} USDC per selected account: {}.",
            amount_label,
            account_ids.join(", ")
        ),
        "Run each listed usdc-dex-transfer-runbook command one account at a time, inspect JSON evidence after each account, and stop on the first failure.".to_string(),
        "After transfer, rerun Funding Check and Preflight Selected to confirm XYZ perp collateral before any opening order.".to_string(),
    ];

    Ok(UsdcDexTransferLiveWindowResult {
        environment: config.app.environment,
        source_config_path: source_config_path.to_string_lossy().into_owned(),
        output_config_path: output_config.to_string_lossy().into_owned(),
        config_written,
        amount_usdc: options.amount_usdc,
        source_dex: String::new(),
        destination_dex,
        account_ids,
        required_config_changes,
        accounts,
        next_actions,
    })
}

pub fn run_usdc_dex_transfer_live_window(
    source_config_path: PathBuf,
    config: AppConfig,
    options: UsdcDexTransferLiveWindowOptions,
) -> Result<()> {
    let result = prepare_usdc_dex_transfer_live_window(&source_config_path, config, options)?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

pub fn prepare_signed_live_window(
    source_config_path: &Path,
    config: AppConfig,
    options: SignedLiveWindowOptions,
) -> Result<SignedLiveWindowResult> {
    anyhow::ensure!(
        config.app.environment == "mainnet",
        "signed live window helper is only for mainnet order smoke preparation"
    );
    anyhow::ensure!(
        options.notional_usd.is_finite() && options.notional_usd > 0.0,
        "notional_usd must be positive"
    );
    anyhow::ensure!(
        options.notional_usd <= config.manual_ops.max_manual_order_notional_usd,
        "notional_usd exceeds manual_ops.max_manual_order_notional_usd"
    );
    anyhow::ensure!(
        (0.0..10_000.0).contains(&options.max_slippage_bps),
        "max_slippage_bps must be >= 0 and < 10000"
    );
    anyhow::ensure!(
        config.manual_ops.enabled && config.manual_ops.manual_trading_enabled,
        "manual trading must be enabled before preparing signed live window"
    );
    anyhow::ensure!(
        !config.risk.global.kill_switch,
        "signed live window preparation refuses to auto-clear the global kill switch"
    );

    let canonical_coin = normalize_dex_coin(&config.hyperliquid.dex, &options.coin);
    if !config.manual_ops.blocked_symbols.is_empty() {
        anyhow::ensure!(
            !config.manual_ops.blocked_symbols.contains(&canonical_coin),
            "manual symbol {} is blocked",
            canonical_coin
        );
    }

    let account_ids = live_window_account_ids(&config, &options.account_ids)?;
    anyhow::ensure!(
        account_ids.len() <= config.manual_ops.max_manual_batch_accounts,
        "selected account count {} exceeds manual_ops.max_manual_batch_accounts {}",
        account_ids.len(),
        config.manual_ops.max_manual_batch_accounts
    );

    let output_config = options.output_config_path.clone();
    validate_live_window_output_path(&output_config)?;
    let notional_label = usdc_transfer_amount_label(options.notional_usd);
    let slippage_label = usdc_transfer_amount_label(options.max_slippage_bps);
    let side_arg = order_side_cli_arg(options.side);
    let close_side_arg = order_side_cli_arg(opposite_order_side(options.side));
    let execution_arg = execution_mode_cli_arg(options.execution_mode);

    let mut accounts = Vec::new();
    for account_id in &account_ids {
        let account = config
            .account(account_id)
            .with_context(|| format!("account {account_id} not found in config"))?;
        anyhow::ensure!(
            account.enabled && account.worker_enabled,
            "account {} is not enabled for worker execution",
            account.account_id
        );
        ensure_live_account_address(account)?;
        anyhow::ensure!(
            options.notional_usd <= account.max_order_notional_usd,
            "notional_usd exceeds account {} max_order_notional_usd",
            account.account_id
        );
        let secret_id = account_secret_id(account);
        anyhow::ensure!(
            !secret_id.trim().is_empty(),
            "account {} must have a Vault secret_id/api_wallet_env for trading and a transfer_secret_id/transfer_wallet_env for USDC funding transfers",
            account.account_id
        );

        let base_args = vec![
            "cargo".to_string(),
            "run".to_string(),
            "--".to_string(),
            "signed-runbook".to_string(),
            "--config".to_string(),
            output_config.to_string_lossy().into_owned(),
            "--account-id".to_string(),
            account_id.clone(),
            "--coin".to_string(),
            canonical_coin.clone(),
            "--side".to_string(),
            side_arg.to_string(),
            "--notional-usd".to_string(),
            notional_label.clone(),
            "--max-slippage-bps".to_string(),
            slippage_label.clone(),
            "--execution-mode".to_string(),
            execution_arg.to_string(),
        ];
        let mut preflight_args = base_args.clone();
        preflight_args.push("--confirm-mainnet-live".to_string());
        preflight_args.push("true".to_string());

        let mut submit_runbook_args = preflight_args.clone();
        submit_runbook_args.push("--submit".to_string());
        submit_runbook_args.push("true".to_string());
        submit_runbook_args.push("--cancel-resting".to_string());
        submit_runbook_args.push("true".to_string());

        let mut reduce_only_close_runbook_args = vec![
            "cargo".to_string(),
            "run".to_string(),
            "--".to_string(),
            "signed-runbook".to_string(),
            "--config".to_string(),
            output_config.to_string_lossy().into_owned(),
            "--account-id".to_string(),
            account_id.clone(),
            "--coin".to_string(),
            canonical_coin.clone(),
            "--side".to_string(),
            close_side_arg.to_string(),
            "--notional-usd".to_string(),
            notional_label.clone(),
            "--max-slippage-bps".to_string(),
            slippage_label.clone(),
            "--execution-mode".to_string(),
            "taker".to_string(),
            "--reduce-only".to_string(),
            "true".to_string(),
            "--submit".to_string(),
            "true".to_string(),
            "--cancel-resting".to_string(),
            "true".to_string(),
            "--confirm-mainnet-live".to_string(),
            "true".to_string(),
        ];
        reduce_only_close_runbook_args.shrink_to_fit();

        let reconcile_args = vec![
            "cargo".to_string(),
            "run".to_string(),
            "--".to_string(),
            "reconcile-account".to_string(),
            "--config".to_string(),
            output_config.to_string_lossy().into_owned(),
            "--account-id".to_string(),
            account_id.clone(),
        ];

        accounts.push(SignedLiveWindowAccount {
            account_id: account_id.clone(),
            address: account.address.clone(),
            secret_id,
            preflight_args,
            submit_runbook_args,
            reduce_only_close_runbook_args,
            reconcile_args,
        });
    }

    let mut live_config = config.clone();
    live_config.app.dry_run = false;
    live_config.manual_ops.manual_live_enabled = true;
    live_config.manual_ops.mainnet_live_enabled = true;
    let required_config_changes = vec![
        "app.dry_run=false".to_string(),
        "manual_ops.manual_live_enabled=true".to_string(),
        "manual_ops.mainnet_live_enabled=true".to_string(),
        "risk.global.kill_switch=false (must already be false; helper will not auto-clear it)"
            .to_string(),
    ];

    let config_written = if options.write {
        if output_config.exists() && !options.overwrite {
            anyhow::bail!(
                "output config {} already exists; pass --overwrite true to replace it",
                output_config.display()
            );
        }
        if let Some(parent) = output_config.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        save_config(&output_config, &live_config)?;
        true
    } else {
        false
    };

    let next_actions = vec![
        "After funding transfer, rerun account-funding and signed-preflight for each account before any submit.".to_string(),
        "Unlock the Vault in the current frontend process or set TRADE_XYZ_VAULT_PASSWORD in the current PowerShell session.".to_string(),
        "Run each listed signed-runbook submit command one account at a time; inspect JSON evidence and stop on the first failure.".to_string(),
        "If the opening order fills, use the reduce-only close runbook for the same account before moving to the next account.".to_string(),
        "Delete or stop using the temporary live config after the smoke window ends.".to_string(),
    ];

    Ok(SignedLiveWindowResult {
        environment: config.app.environment,
        source_config_path: source_config_path.to_string_lossy().into_owned(),
        output_config_path: output_config.to_string_lossy().into_owned(),
        config_written,
        coin: canonical_coin,
        side: options.side,
        notional_usd: options.notional_usd,
        max_slippage_bps: options.max_slippage_bps,
        execution_mode: options.execution_mode,
        account_ids,
        required_config_changes,
        accounts,
        next_actions,
    })
}

pub fn run_signed_live_window(
    source_config_path: PathBuf,
    config: AppConfig,
    options: SignedLiveWindowOptions,
) -> Result<()> {
    let result = prepare_signed_live_window(&source_config_path, config, options)?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

pub async fn build_mainnet_smoke_plan(
    source_config_path: PathBuf,
    config: AppConfig,
    options: MainnetSmokePlanOptions,
    vault_password: Option<&str>,
) -> Result<MainnetSmokePlanResult> {
    anyhow::ensure!(
        config.app.environment == "mainnet",
        "mainnet smoke plan is only valid for mainnet"
    );
    let account_ids = account_funding_account_ids(&config, &options.account_ids);
    anyhow::ensure!(
        !account_ids.is_empty(),
        "at least one account id is required for mainnet smoke plan"
    );

    let funding = build_account_funding_batch_report(&config, &account_ids).await;
    let destination_dex = options
        .destination_dex
        .clone()
        .unwrap_or_else(|| config.hyperliquid.dex.clone());

    let transfer_preflight = Box::pin(execute_usdc_dex_transfer_batch_preflight(
        config.clone(),
        UsdcDexTransferBatchPreflightOptions {
            account_ids: account_ids.clone(),
            destination_account_id: None,
            amount_usdc: options.funding_amount_usdc,
            source_dex: None,
            destination_dex: Some(destination_dex.clone()),
            confirm_mainnet_live: false,
        },
        vault_password,
    ))
    .await?;

    let transfer_live_window = prepare_usdc_dex_transfer_live_window(
        &source_config_path,
        config.clone(),
        UsdcDexTransferLiveWindowOptions {
            account_ids: account_ids.clone(),
            amount_usdc: options.funding_amount_usdc,
            destination_dex: Some(destination_dex),
            output_config_path: options.transfer_output_config_path,
            write: false,
            overwrite: false,
        },
    )?;

    let signed_live_window = prepare_signed_live_window(
        &source_config_path,
        config.clone(),
        SignedLiveWindowOptions {
            account_ids: account_ids.clone(),
            coin: options.coin,
            side: options.side,
            notional_usd: options.order_notional_usd,
            max_slippage_bps: options.max_slippage_bps,
            execution_mode: options.execution_mode,
            output_config_path: options.order_output_config_path,
            write: false,
            overwrite: false,
        },
    )?;

    let ready_for_funding_submit = transfer_preflight.ready_account_ids.len() == account_ids.len();
    let ready_for_order_submit = funding.ready_account_ids.len() == account_ids.len();
    let mut stop_reasons = Vec::new();
    if !ready_for_funding_submit {
        stop_reasons.push(format!(
            "funding transfer is not ready for all accounts; blocked_account_ids={:?}, failed_account_ids={:?}",
            transfer_preflight.blocked_account_ids, transfer_preflight.failed_account_ids
        ));
    }
    if !ready_for_order_submit {
        stop_reasons.push(format!(
            "order smoke is not ready until XYZ perp collateral is visible for every account; ready_account_ids={:?}, transfer_needed_account_ids={:?}",
            funding.ready_account_ids, funding.transfer_needed_account_ids
        ));
    }
    let next_actions = mainnet_smoke_plan_next_actions(
        ready_for_funding_submit,
        ready_for_order_submit,
        &transfer_preflight,
        &funding,
    );

    Ok(MainnetSmokePlanResult {
        environment: config.app.environment,
        dex: config.hyperliquid.dex,
        account_ids,
        funding_amount_usdc: options.funding_amount_usdc,
        order_notional_usd: options.order_notional_usd,
        coin: signed_live_window.coin.clone(),
        side: options.side,
        execution_mode: options.execution_mode,
        funding,
        transfer_preflight,
        transfer_live_window,
        signed_live_window,
        ready_for_funding_submit,
        ready_for_order_submit,
        stop_reasons,
        next_actions,
    })
}

pub async fn run_mainnet_smoke_plan(
    source_config_path: PathBuf,
    config: AppConfig,
    options: MainnetSmokePlanOptions,
) -> Result<()> {
    let password = std::env::var("TRADE_XYZ_VAULT_PASSWORD").ok();
    let result =
        build_mainnet_smoke_plan(source_config_path, config, options, password.as_deref()).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

pub async fn build_account_funding_report(
    config: &AppConfig,
    account_id: &str,
) -> Result<AccountFundingReport> {
    let account = config
        .account(account_id)
        .cloned()
        .with_context(|| format!("account {account_id} not found in config"))?;
    anyhow::ensure!(
        account.enabled && account.worker_enabled,
        "account {} is not enabled for worker execution",
        account.account_id
    );

    let default_future =
        fetch_default_clearinghouse_state(&config.app.environment, &account.address);
    let xyz_future = fetch_clearinghouse_state(
        &config.app.environment,
        &config.hyperliquid.dex,
        &account.address,
    );
    let spot_future = fetch_spot_clearinghouse_state(&config.app.environment, &account.address);
    let (default_perp, xyz_perp, spot) = tokio::join!(default_future, xyz_future, spot_future);

    let default_perp = PerpFundingLayerReport::from_result("default_perp", default_perp);
    let xyz_perp = PerpFundingLayerReport::from_result("xyz_perp", xyz_perp);
    let spot = SpotFundingLayerReport::from_result(spot);
    let next_actions = account_funding_report_next_actions(
        &default_perp,
        &xyz_perp,
        &spot,
        &config.app.environment,
        &config.hyperliquid.dex,
    );
    let funding_summary = account_funding_report_summary(&default_perp, &xyz_perp, &spot);

    Ok(AccountFundingReport {
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

pub async fn build_account_funding_batch_report(
    config: &AppConfig,
    account_ids: &[String],
) -> AccountFundingBatchReport {
    let account_ids = account_funding_account_ids(config, account_ids);
    let funding_futures = account_ids.iter().cloned().map(|account_id| {
        let config = config.clone();
        async move {
            match build_account_funding_report(&config, &account_id).await {
                Ok(data) => AccountFundingAccountResult {
                    ok: true,
                    data: Some(data),
                    error: None,
                },
                Err(error) => AccountFundingAccountResult {
                    ok: false,
                    data: None,
                    error: Some(error.to_string()),
                },
            }
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

    AccountFundingBatchReport {
        environment: config.app.environment.clone(),
        dex: config.hyperliquid.dex.clone(),
        account_ids,
        ready_account_ids,
        transfer_needed_account_ids,
        failed_account_ids,
        results,
    }
}

pub async fn run_account_funding(config: AppConfig, account_ids: Vec<String>) -> Result<()> {
    let report = build_account_funding_batch_report(&config, &account_ids).await;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn account_funding_account_ids(config: &AppConfig, requested: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let source: Vec<String> = if requested.is_empty() {
        config
            .accounts
            .iter()
            .filter(|account| account.enabled && account.worker_enabled)
            .map(|account| account.account_id.clone())
            .collect()
    } else {
        requested
            .iter()
            .map(|account_id| account_id.trim().to_string())
            .filter(|account_id| !account_id.is_empty())
            .collect()
    };

    source
        .into_iter()
        .filter(|account_id| seen.insert(account_id.clone()))
        .collect()
}

fn live_window_account_ids(config: &AppConfig, requested: &[String]) -> Result<Vec<String>> {
    let mut seen = HashSet::new();
    let mut account_ids = Vec::new();
    let source: Vec<String> = if requested.is_empty() {
        config
            .accounts
            .iter()
            .filter(|account| account.enabled && account.worker_enabled)
            .map(|account| account.account_id.clone())
            .collect()
    } else {
        requested
            .iter()
            .map(|account_id| account_id.trim().to_string())
            .filter(|account_id| !account_id.is_empty())
            .collect()
    };

    for account_id in source {
        if seen.insert(account_id.clone()) {
            account_ids.push(account_id);
        }
    }
    anyhow::ensure!(
        !account_ids.is_empty(),
        "at least one account id is required for transfer live window preparation"
    );
    Ok(account_ids)
}

fn validate_live_window_output_path(output_config_path: &Path) -> Result<()> {
    anyhow::ensure!(
        output_config_path
            .extension()
            .and_then(|extension| extension.to_str())
            == Some("toml"),
        "output config must be a .toml file"
    );
    anyhow::ensure!(
        !output_config_path.is_absolute(),
        "output config must be a relative path under .codex-longrun"
    );

    let mut components = output_config_path.components();
    match components.next() {
        Some(Component::Normal(component)) if component == OsStr::new(".codex-longrun") => {}
        _ => anyhow::bail!("output config must be a relative path under .codex-longrun"),
    }

    for component in components {
        anyhow::ensure!(
            matches!(component, Component::Normal(_)),
            "output config path cannot contain parent, root, prefix, or current-dir components"
        );
    }
    Ok(())
}

impl PerpFundingLayerReport {
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
                PerpFundingPositionReport {
                    coin: position.coin.clone(),
                    size: parse_state_decimal(&position.szi),
                    position_value_usd: position
                        .position_value
                        .as_deref()
                        .map(parse_state_decimal)
                        .unwrap_or_default(),
                    unrealized_pnl_usd: position
                        .unrealized_pnl
                        .as_deref()
                        .map(parse_state_decimal)
                        .unwrap_or_default(),
                }
            })
            .collect::<Vec<_>>();
        Self {
            name: name.to_string(),
            query_ok: true,
            error: None,
            account_value_usd: parse_state_decimal(&state.margin_summary.account_value),
            withdrawable_usd: state
                .withdrawable
                .as_deref()
                .map(parse_state_decimal)
                .unwrap_or_default(),
            total_notional_position_usd: parse_state_decimal(&state.margin_summary.total_ntl_pos),
            total_margin_used_usd: parse_state_decimal(&state.margin_summary.total_margin_used),
            position_count: positions.len(),
            positions,
        }
    }

    fn has_collateral(&self) -> bool {
        self.account_value_usd > 0.0 || self.withdrawable_usd > 0.0
    }
}

impl SpotFundingLayerReport {
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
            .map(|balance| SpotFundingBalanceReport {
                coin: balance.coin.clone(),
                total: parse_state_decimal(&balance.total),
                hold: parse_state_decimal(&balance.hold),
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
            total_usdc: normalize_report_zero(total_usdc),
            hold_usdc: normalize_report_zero(hold_usdc),
            balance_count: balances.len(),
            balances,
        }
    }

    fn has_usdc(&self) -> bool {
        self.total_usdc > 0.0
    }
}

fn account_funding_report_summary(
    default_perp: &PerpFundingLayerReport,
    xyz_perp: &PerpFundingLayerReport,
    spot: &SpotFundingLayerReport,
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

fn account_funding_report_next_actions(
    default_perp: &PerpFundingLayerReport,
    xyz_perp: &PerpFundingLayerReport,
    spot: &SpotFundingLayerReport,
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

fn normalize_report_zero(value: f64) -> f64 {
    if value.abs() < f64::EPSILON {
        0.0
    } else {
        value
    }
}

pub async fn run_signed_runbook(config: AppConfig, options: SignedRunbookOptions) -> Result<()> {
    let password = std::env::var("TRADE_XYZ_VAULT_PASSWORD").ok();
    let result = execute_signed_runbook(config, options, password.as_deref()).await?;
    let submit_requested = result.submit_requested;
    let submitted = result.submitted;
    println!("{}", serde_json::to_string_pretty(&result)?);
    anyhow::ensure!(
        !submit_requested || submitted,
        "signed runbook requested submit but no order was submitted; inspect preflight checks"
    );
    Ok(())
}

async fn collect_runbook_order_status_checks(
    config: &AppConfig,
    account_id: &str,
    acceptance: &SignedAcceptanceResult,
    order_status_checks: &mut Vec<OrderStatusReport>,
) -> Result<()> {
    let Some(signed_smoke) = &acceptance.signed_smoke else {
        return Ok(());
    };
    let Some(WorkerReport::Submitted(submitted)) = &signed_smoke.submit_report else {
        return Ok(());
    };

    if let Some(oid) = submitted.oid {
        order_status_checks
            .push(query_order_status(config, account_id, OrderStatusLookup::Oid { oid }).await?);
    }
    if !submitted.cloid.trim().is_empty() {
        order_status_checks.push(
            query_order_status(
                config,
                account_id,
                OrderStatusLookup::Cloid {
                    cloid: submitted.cloid.clone(),
                },
            )
            .await?,
        );
    }
    Ok(())
}

fn signed_runbook_check(name: &str, ok: bool, detail: String) -> SignedRunbookCheck {
    SignedRunbookCheck {
        name: name.to_string(),
        ok,
        detail,
    }
}

fn transfer_secret_readiness_check(
    config: &AppConfig,
    account: &AccountConfig,
    password: Option<&str>,
) -> Result<String> {
    let dedicated = account_has_dedicated_transfer_secret(account);
    let secret_id = transfer_secret_id(account);
    let secret = load_transfer_secret(config, account, password)?;
    let mode = if dedicated {
        "dedicated transfer signer"
    } else {
        "legacy secret_id fallback"
    };
    Ok(format!(
        "{mode} {secret_id} is available and matches EVM account address {}",
        secret.signer_address
    ))
}

pub(crate) fn validate_usdc_dex_transfer_gates(
    config: &AppConfig,
    options: &UsdcDexTransferOptions,
) -> Result<()> {
    anyhow::ensure!(
        options.amount_usdc.is_finite() && options.amount_usdc > 0.0,
        "amount_usdc must be positive"
    );
    anyhow::ensure!(
        options.amount_usdc <= 10.0,
        "USDC DEX transfer helper is capped at 10 USDC per account"
    );
    let source_dex = normalize_transfer_layer(options.source_dex.as_deref().unwrap_or_default());
    let destination_dex = options
        .destination_dex
        .as_deref()
        .map(normalize_transfer_layer)
        .unwrap_or_else(|| normalize_transfer_layer(&config.hyperliquid.dex));
    let destination_account_id = normalized_transfer_destination_account_id(
        &options.account_id,
        options.destination_account_id.as_deref(),
    );
    anyhow::ensure!(
        usdc_transfer_layer_supported(&source_dex),
        "source layer must be default_perp (empty), spot, or a valid perp dex name"
    );
    anyhow::ensure!(
        usdc_transfer_layer_supported(&destination_dex),
        "destination layer must be default_perp (empty), spot, or a valid perp dex name"
    );
    anyhow::ensure!(
        destination_account_id != options.account_id || source_dex != destination_dex,
        "source and destination cannot be the same account layer"
    );
    if !options.submit {
        return Ok(());
    }
    anyhow::ensure!(
        !config.app.dry_run,
        "USDC DEX transfer submit requires app.dry_run=false"
    );
    anyhow::ensure!(
        config.manual_ops.manual_live_enabled,
        "USDC DEX transfer submit requires manual_ops.manual_live_enabled=true"
    );
    if config.app.environment == "mainnet" {
        anyhow::ensure!(
            config.manual_ops.mainnet_live_enabled && options.confirm_mainnet_live,
            "mainnet USDC DEX transfer requires manual_ops.mainnet_live_enabled=true and explicit mainnet confirmation"
        );
    }
    Ok(())
}

fn format_usdc_amount(amount: f64) -> Result<String> {
    anyhow::ensure!(
        amount.is_finite() && amount > 0.0,
        "amount_usdc must be positive"
    );
    let scaled = amount * 1_000_000.0;
    anyhow::ensure!(
        (scaled.round() - scaled).abs() < 1e-6,
        "amount_usdc supports at most 6 decimal places"
    );
    let mut amount = format!("{amount:.6}");
    while amount.contains('.') && amount.ends_with('0') {
        amount.pop();
    }
    if amount.ends_with('.') {
        amount.pop();
    }
    Ok(amount)
}

#[derive(Debug, Clone)]
struct UsdcLayerSnapshot {
    total_usdc: f64,
    available_usdc: f64,
}

fn usdc_total_and_available_from_spot(state: &SpotClearinghouseState) -> (f64, f64) {
    let total_usdc = state
        .balances
        .iter()
        .filter(|balance| balance.coin.eq_ignore_ascii_case("USDC"))
        .map(|balance| parse_usd_value(&balance.total))
        .sum::<f64>();
    let hold_usdc = state
        .balances
        .iter()
        .filter(|balance| balance.coin.eq_ignore_ascii_case("USDC"))
        .map(|balance| parse_usd_value(&balance.hold))
        .sum::<f64>();
    let available = (total_usdc - hold_usdc).max(0.0);
    (
        normalize_report_zero(total_usdc),
        normalize_report_zero(available),
    )
}

fn usdc_perp_layer_snapshot(state: &ClearinghouseState) -> UsdcLayerSnapshot {
    let total = parse_usd_value(&state.margin_summary.account_value);
    let available = state
        .withdrawable
        .as_deref()
        .map(parse_usd_value)
        .unwrap_or_default();
    UsdcLayerSnapshot {
        total_usdc: normalize_report_zero(total),
        available_usdc: normalize_report_zero(available),
    }
}

async fn fetch_usdc_layer_snapshot(
    environment: &str,
    layer: &str,
    address: &str,
) -> Result<UsdcLayerSnapshot> {
    if layer.eq_ignore_ascii_case("spot") {
        let state = fetch_spot_clearinghouse_state(environment, address).await?;
        let (total_usdc, available_usdc) = usdc_total_and_available_from_spot(&state);
        return Ok(UsdcLayerSnapshot {
            total_usdc,
            available_usdc,
        });
    }
    let state = if layer.trim().is_empty() {
        fetch_default_clearinghouse_state(environment, address).await?
    } else {
        fetch_clearinghouse_state(environment, layer, address).await?
    };
    Ok(usdc_perp_layer_snapshot(&state))
}

fn dex_transfer_balances(
    source: &UsdcLayerSnapshot,
    destination: &UsdcLayerSnapshot,
) -> DexTransferBalances {
    DexTransferBalances {
        source_total_usdc: source.total_usdc,
        source_available_usdc: source.available_usdc,
        destination_total_usdc: destination.total_usdc,
        destination_available_usdc: destination.available_usdc,
    }
}

fn parse_usd_value(value: &str) -> f64 {
    value.parse::<f64>().unwrap_or_default()
}

fn validate_signed_submit_gates(config: &AppConfig, options: &SignedSmokeOptions) -> Result<()> {
    if !options.submit {
        return Ok(());
    }
    let close_gate = signed_close_exempt_from_opening_rules(
        &config.hyperliquid.dex,
        options.side,
        options.reduce_only,
        options.close_full_position,
    );
    validate_live_order_gates(config, options.confirm_mainnet_live, close_gate)?;
    validate_exchange_min_order_notional(options.notional_usd, close_gate)
}

pub fn exchange_min_order_notional_ok(notional_usd: f64, reduce_only: bool) -> bool {
    reduce_only || notional_usd >= HYPERLIQUID_MIN_ORDER_NOTIONAL_USD
}

pub fn effective_order_notional_usd(limit_price: f64, size: f64) -> f64 {
    limit_price.abs() * size.abs()
}

pub fn minimum_requested_notional_for_effective_min(
    limit_price: f64,
    reference_price: f64,
    sz_decimals: u32,
    min_effective_notional_usd: f64,
) -> Option<f64> {
    if !limit_price.is_finite()
        || !reference_price.is_finite()
        || !min_effective_notional_usd.is_finite()
        || limit_price == 0.0
        || reference_price == 0.0
        || min_effective_notional_usd <= 0.0
    {
        return None;
    }
    let scale = 10_f64.powi(sz_decimals.try_into().ok()?);
    if !scale.is_finite() || scale <= 0.0 {
        return None;
    }
    let step = 1.0 / scale;
    let required_size = ((min_effective_notional_usd / limit_price.abs()) / step).ceil() * step;
    if !required_size.is_finite() || required_size <= 0.0 {
        return None;
    }
    Some(required_size * reference_price.abs() + 0.000_001)
}

pub fn effective_exchange_min_order_notional_ok(
    limit_price: f64,
    size: f64,
    reduce_only: bool,
) -> bool {
    reduce_only
        || effective_order_notional_usd(limit_price, size) >= HYPERLIQUID_MIN_ORDER_NOTIONAL_USD
}

pub fn validate_exchange_min_order_notional(notional_usd: f64, reduce_only: bool) -> Result<()> {
    anyhow::ensure!(
        exchange_min_order_notional_ok(notional_usd, reduce_only),
        "opening order notional must be at least {} USD; Hyperliquid rejected smaller mainnet orders with minimum value $10",
        HYPERLIQUID_MIN_ORDER_NOTIONAL_USD
    );
    Ok(())
}

pub fn exchange_min_effective_detail(
    effective_notional: f64,
    minimum_requested_notional: Option<f64>,
) -> String {
    let base = format!(
        "planned order value after precision rounding is {:.6} USD; opening orders must be at least {} USD",
        effective_notional, HYPERLIQUID_MIN_ORDER_NOTIONAL_USD
    );
    if let Some(minimum_requested_notional) = minimum_requested_notional {
        format!(
            "{base}; request at least {:.6} USD for the current price and size precision",
            minimum_requested_notional
        )
    } else {
        base
    }
}

fn is_spot_dex(dex: &str) -> bool {
    dex.trim().eq_ignore_ascii_case("spot")
}

fn exchange_action_error_is_retryable(error: &anyhow::Error) -> bool {
    let lower = format!("{error:#}").to_ascii_lowercase();
    lower.contains("429")
        || lower.contains("too many requests")
        || lower.contains("rate limit")
        || lower.contains("timeout")
        || lower.contains("connection")
}

fn exchange_action_retry_delay_ms(attempt: usize) -> u64 {
    let exp = EXCHANGE_ACTION_BASE_BACKOFF_MS
        .saturating_mul(1_u64 << attempt.saturating_sub(1).min(5))
        .min(EXCHANGE_ACTION_MAX_BACKOFF_MS);
    let jitter = (crate::domain::now_ms() % 997).saturating_add((attempt as u64 * 97) % 389);
    exp.saturating_add(jitter)
}

fn simplify_exchange_action_error(error: anyhow::Error) -> anyhow::Error {
    let detailed = format!("{error:#}");
    let lower = detailed.to_ascii_lowercase();
    if lower.contains("429") || lower.contains("too many requests") {
        anyhow::anyhow!(
            "Hyperliquid action rate limit hit (429 Too Many Requests) after {} attempt(s); retry later or reduce concurrent polling/cancel requests",
            EXCHANGE_ACTION_MAX_ATTEMPTS
        )
    } else {
        error
    }
}

fn format_anyhow_for_log(error: &anyhow::Error) -> String {
    format!("{error:#}")
}

pub fn signed_close_exempt_from_opening_rules(
    dex: &str,
    side: OrderSide,
    reduce_only: bool,
    close_full_position: bool,
) -> bool {
    reduce_only || (is_spot_dex(dex) && close_full_position && matches!(side, OrderSide::Sell))
}

pub fn signed_exchange_reduce_only_flag(
    dex: &str,
    side: OrderSide,
    reduce_only: bool,
    close_full_position: bool,
) -> bool {
    if is_spot_dex(dex) && close_full_position && matches!(side, OrderSide::Sell) {
        false
    } else {
        reduce_only
    }
}

fn canonical_coin_for_dex(dex: &str, coin: &str) -> String {
    if is_spot_dex(dex) {
        normalize_spot_coin(coin)
    } else {
        normalize_dex_coin(dex, coin)
    }
}

fn info_query_dex(dex: &str) -> String {
    if is_spot_dex(dex) {
        String::new()
    } else {
        dex.trim().to_ascii_lowercase()
    }
}

fn market_id_for_dex(dex: &str) -> &'static str {
    if is_spot_dex(dex) {
        MARKET_SPOT
    } else if dex.trim().eq_ignore_ascii_case("xyz") {
        MARKET_XYZ_PERP
    } else {
        MARKET_HL_PERP
    }
}

fn signed_close_size_hint(
    reduce_only: bool,
    close_full_position: bool,
    account_state: Option<&AccountReadinessState>,
) -> Option<f64> {
    if !reduce_only || !close_full_position {
        return None;
    }
    let size = account_state?.coin_position_size.abs();
    if size > 0.0 { Some(size) } else { None }
}

fn apply_order_plan_size_override(
    mut plan: OrderPlan,
    exact_size: Option<f64>,
    coin: &str,
) -> Result<OrderPlan> {
    let Some(exact_size) = exact_size else {
        return Ok(plan);
    };
    let rounded_size = round_size_down(exact_size.abs(), plan.sz_decimals);
    anyhow::ensure!(
        rounded_size > 0.0,
        "close_full_position size rounds to zero for {} at requested size {}",
        coin,
        exact_size
    );
    plan.size = rounded_size;
    Ok(plan)
}

fn build_signed_spot_order_plan(
    snapshot: &SpotMarketSnapshot,
    coin: &str,
    side: OrderSide,
    notional_usd: f64,
    max_slippage_bps: f64,
    execution_mode: ExecutionMode,
    exact_size: Option<f64>,
) -> Result<OrderPlan> {
    match execution_mode {
        ExecutionMode::Taker => {
            if let Some(exact_size) = exact_size {
                build_spot_order_plan_for_size(snapshot, coin, side, exact_size, max_slippage_bps)
            } else {
                build_spot_order_plan(
                    snapshot,
                    coin,
                    matches!(side, OrderSide::Buy),
                    notional_usd,
                    None,
                    max_slippage_bps,
                )
            }
        }
        ExecutionMode::Maker => build_spot_maker_order_plan(
            snapshot,
            coin,
            side,
            notional_usd,
            max_slippage_bps,
            exact_size,
        ),
    }
}

fn build_spot_maker_order_plan(
    snapshot: &SpotMarketSnapshot,
    coin: &str,
    side: OrderSide,
    notional_usd: f64,
    max_slippage_bps: f64,
    exact_size: Option<f64>,
) -> Result<OrderPlan> {
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
    let size = if let Some(exact_size) = exact_size {
        round_size_down(exact_size, asset.sz_decimals)
    } else {
        anyhow::ensure!(notional_usd > 0.0, "notional_usd must be positive");
        round_size_down(notional_usd / reference_price, asset.sz_decimals)
    };
    anyhow::ensure!(
        size > 0.0,
        "order size rounds to zero for {} at {} and price {}",
        asset.coin,
        exact_size.unwrap_or(notional_usd),
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

fn build_spot_order_plan_for_size(
    snapshot: &SpotMarketSnapshot,
    coin: &str,
    side: OrderSide,
    exact_size: f64,
    max_slippage_bps: f64,
) -> Result<OrderPlan> {
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
    let slippage_factor = match side {
        OrderSide::Buy => 1.0 + max_slippage_bps / 10_000.0,
        OrderSide::Sell => 1.0 - max_slippage_bps / 10_000.0,
    };
    anyhow::ensure!(
        slippage_factor > 0.0,
        "slippage guard produced non-positive price"
    );
    let limit_price = round_spot_price(reference_price * slippage_factor, asset.sz_decimals);
    let size = round_size_down(exact_size, asset.sz_decimals);
    anyhow::ensure!(
        size > 0.0,
        "order size rounds to zero for {} at requested size {} and price {}",
        asset.coin,
        exact_size,
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

pub(crate) fn build_signed_order_plan(
    snapshot: &XyzMarketSnapshot,
    coin: &str,
    side: OrderSide,
    notional_usd: f64,
    max_slippage_bps: f64,
    execution_mode: ExecutionMode,
    exact_size: Option<f64>,
) -> Result<OrderPlan> {
    match execution_mode {
        ExecutionMode::Taker => {
            if let Some(exact_size) = exact_size {
                build_order_plan_for_size(snapshot, coin, side, exact_size, max_slippage_bps)
            } else {
                build_order_plan(
                    snapshot,
                    coin,
                    matches!(side, OrderSide::Buy),
                    notional_usd,
                    None,
                    max_slippage_bps,
                )
            }
        }
        ExecutionMode::Maker => build_maker_order_plan(
            snapshot,
            coin,
            side,
            notional_usd,
            max_slippage_bps,
            exact_size,
        ),
    }
}

fn build_maker_order_plan(
    snapshot: &XyzMarketSnapshot,
    coin: &str,
    side: OrderSide,
    notional_usd: f64,
    max_slippage_bps: f64,
    exact_size: Option<f64>,
) -> Result<OrderPlan> {
    anyhow::ensure!(
        max_slippage_bps >= 0.0,
        "max_slippage_bps cannot be negative"
    );
    let asset = snapshot.asset(coin)?;
    let reference_price = asset.reference_price()?;
    anyhow::ensure!(
        reference_price.is_finite() && reference_price > 0.0,
        "reference price for {} must be positive",
        asset.meta.name
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
    let limit_price = round_perp_price(reference_price * limit_factor, asset.meta.sz_decimals);
    let size = if let Some(exact_size) = exact_size {
        round_size_down(exact_size, asset.meta.sz_decimals)
    } else {
        anyhow::ensure!(notional_usd > 0.0, "notional_usd must be positive");
        round_size_down(notional_usd / reference_price, asset.meta.sz_decimals)
    };
    anyhow::ensure!(
        size > 0.0,
        "order size rounds to zero for {} at {} and price {}",
        asset.meta.name,
        exact_size.unwrap_or(notional_usd),
        reference_price
    );

    Ok(OrderPlan {
        coin: asset.meta.name,
        asset_id: asset.asset_id,
        sz_decimals: asset.meta.sz_decimals,
        reference_price,
        limit_price,
        size,
    })
}

fn build_order_plan_for_size(
    snapshot: &XyzMarketSnapshot,
    coin: &str,
    side: OrderSide,
    exact_size: f64,
    max_slippage_bps: f64,
) -> Result<OrderPlan> {
    anyhow::ensure!(
        max_slippage_bps >= 0.0,
        "max_slippage_bps cannot be negative"
    );
    let asset = snapshot.asset(coin)?;
    let reference_price = asset.reference_price()?;
    anyhow::ensure!(
        reference_price.is_finite() && reference_price > 0.0,
        "reference price for {} must be positive",
        asset.meta.name
    );
    let slippage_factor = match side {
        OrderSide::Buy => 1.0 + max_slippage_bps / 10_000.0,
        OrderSide::Sell => 1.0 - max_slippage_bps / 10_000.0,
    };
    anyhow::ensure!(
        slippage_factor > 0.0,
        "slippage guard produced non-positive price"
    );
    let limit_price = round_perp_price(reference_price * slippage_factor, asset.meta.sz_decimals);
    let size = round_size_down(exact_size, asset.meta.sz_decimals);
    anyhow::ensure!(
        size > 0.0,
        "order size rounds to zero for {} at requested size {} and price {}",
        asset.meta.name,
        exact_size,
        reference_price
    );
    Ok(OrderPlan {
        coin: asset.meta.name,
        asset_id: asset.asset_id,
        sz_decimals: asset.meta.sz_decimals,
        reference_price,
        limit_price,
        size,
    })
}

pub(crate) fn execution_policy_for_mode(mode: ExecutionMode) -> ExecutionPolicy {
    match mode {
        ExecutionMode::Taker => ExecutionPolicy::Taker,
        ExecutionMode::Maker => ExecutionPolicy::Maker,
    }
}

pub(crate) fn tif_for_execution_mode(mode: ExecutionMode) -> String {
    tif_for_policy(execution_policy_for_mode(mode))
}

pub fn validate_live_action_gates(config: &AppConfig, confirm_mainnet_live: bool) -> Result<()> {
    anyhow::ensure!(
        !config.app.dry_run,
        "signed action requires app.dry_run=false"
    );
    anyhow::ensure!(
        config.manual_ops.manual_live_enabled,
        "signed action requires manual_ops.manual_live_enabled=true"
    );
    if config.app.environment == "mainnet" {
        anyhow::ensure!(
            config.manual_ops.mainnet_live_enabled && confirm_mainnet_live,
            "mainnet signed action requires manual_ops.mainnet_live_enabled=true and explicit mainnet confirmation"
        );
    }
    Ok(())
}

fn validate_signed_smoke_constraints(
    config: &AppConfig,
    account: &AccountConfig,
    options: &SignedSmokeOptions,
) -> Result<()> {
    anyhow::ensure!(
        config.manual_ops.enabled && config.manual_ops.manual_trading_enabled,
        "manual trading is disabled"
    );
    anyhow::ensure!(
        options.notional_usd.is_finite()
            && options.notional_usd > 0.0
            && options.notional_usd <= account.max_order_notional_usd,
        "smoke notional must be positive and <= account max_order_notional_usd"
    );
    anyhow::ensure!(
        options.notional_usd <= config.manual_ops.max_manual_order_notional_usd,
        "smoke notional exceeds max_manual_order_notional_usd"
    );
    anyhow::ensure!(
        (0.0..10_000.0).contains(&options.max_slippage_bps),
        "max_slippage_bps must be >= 0 and < 10000"
    );
    let canonical_coin = canonical_coin_for_dex(&config.hyperliquid.dex, &options.coin);
    if !config.manual_ops.blocked_symbols.is_empty() {
        anyhow::ensure!(
            !config.manual_ops.blocked_symbols.contains(&canonical_coin),
            "manual symbol {} is blocked",
            canonical_coin
        );
    }
    Ok(())
}

pub fn validate_live_submit_gates(config: &AppConfig, confirm_mainnet_live: bool) -> Result<()> {
    validate_live_action_gates(config, confirm_mainnet_live)?;
    anyhow::ensure!(
        !config.risk.global.kill_switch,
        "signed submit blocked because global kill switch is active"
    );
    Ok(())
}

pub fn validate_live_order_gates(
    config: &AppConfig,
    confirm_mainnet_live: bool,
    reduce_only: bool,
) -> Result<()> {
    if !reduce_only {
        return validate_live_submit_gates(config, confirm_mainnet_live);
    }
    validate_live_action_gates(config, confirm_mainnet_live)?;
    anyhow::ensure!(
        !config.risk.global.kill_switch || config.risk.global.allow_reduce_only_when_killed,
        "signed reduce-only action blocked because global kill switch is active and reduce-only is not allowed"
    );
    Ok(())
}

pub(crate) fn ensure_live_account_address(account: &AccountConfig) -> Result<()> {
    anyhow::ensure!(
        is_probably_real_evm_address(&account.address),
        "account {} address must be a real 0x-prefixed 40-hex master/subaccount address, not an example placeholder",
        account.account_id
    );
    Ok(())
}

fn is_probably_real_evm_address(address: &str) -> bool {
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

fn tif_for_policy(policy: ExecutionPolicy) -> String {
    match policy {
        ExecutionPolicy::Taker | ExecutionPolicy::Ioc => "Ioc",
        ExecutionPolicy::Maker | ExecutionPolicy::Alo => "Alo",
        ExecutionPolicy::Gtc => "Gtc",
    }
    .to_string()
}

fn parse_order_side(raw: &str) -> Result<OrderSide> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "buy" | "long" => Ok(OrderSide::Buy),
        "sell" | "short" => Ok(OrderSide::Sell),
        _ => anyhow::bail!("invalid side: {raw}"),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManualMarginMode {
    Isolated,
    Cross,
}

impl ManualMarginMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Isolated => "isolated",
            Self::Cross => "cross",
        }
    }

    fn is_cross(self) -> bool {
        matches!(self, Self::Cross)
    }
}

fn parse_manual_margin_mode(raw: &str) -> Result<ManualMarginMode> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "isolated" | "isolated_only" | "strict_isolated" | "strictisolated" => {
            Ok(ManualMarginMode::Isolated)
        }
        "cross" => Ok(ManualMarginMode::Cross),
        _ => anyhow::bail!("invalid margin mode: {raw}"),
    }
}

fn asset_supports_cross_margin(only_isolated: Option<bool>, margin_mode: Option<&str>) -> bool {
    if only_isolated.unwrap_or(false) {
        return false;
    }

    let Some(raw_mode) = margin_mode else {
        return true;
    };
    let normalized = raw_mode
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase();

    // trade[XYZ] commonly reports strict/normal isolated-like values for isolated-only assets.
    !matches!(
        normalized.as_str(),
        "strictisolated" | "isolated" | "normalisolated" | "nocross"
    )
}

fn protective_exit_prices(
    entry_side: OrderSide,
    entry_price: f64,
    take_profit_usd: f64,
    stop_loss_pct: f64,
) -> Result<(OrderSide, f64, f64)> {
    anyhow::ensure!(
        entry_price.is_finite() && entry_price > 0.0,
        "entry_price must be positive"
    );
    anyhow::ensure!(
        take_profit_usd.is_finite() && take_profit_usd > 0.0,
        "take_profit_usd must be positive"
    );
    anyhow::ensure!(
        valid_stop_loss_pct(stop_loss_pct),
        "stop_loss_pct must be greater than 0 and less than 1"
    );

    match entry_side {
        OrderSide::Buy => {
            let take_profit = entry_price + take_profit_usd;
            let stop_loss = entry_price * (1.0 - stop_loss_pct);
            anyhow::ensure!(stop_loss > 0.0, "stop loss trigger must be positive");
            Ok((OrderSide::Sell, take_profit, stop_loss))
        }
        OrderSide::Sell => {
            let take_profit = entry_price - take_profit_usd;
            let stop_loss = entry_price * (1.0 + stop_loss_pct);
            anyhow::ensure!(
                take_profit > 0.0,
                "short take profit trigger must be positive"
            );
            Ok((OrderSide::Buy, take_profit, stop_loss))
        }
    }
}

pub(crate) fn protective_exit_prices_for_options(
    entry_side: OrderSide,
    entry_price: f64,
    options: &ProtectiveExitOptions,
) -> Result<(OrderSide, f64, f64)> {
    anyhow::ensure!(
        entry_price.is_finite() && entry_price > 0.0,
        "entry_price must be positive"
    );
    let explicit_tp = options.take_profit_trigger_price;
    let explicit_sl = options.stop_loss_trigger_price;
    if explicit_tp.is_some() || explicit_sl.is_some() {
        let take_profit_trigger = explicit_tp
            .with_context(|| "take_profit_trigger_price is required in explicit trigger mode")?;
        let stop_loss_trigger = explicit_sl
            .with_context(|| "stop_loss_trigger_price is required in explicit trigger mode")?;
        anyhow::ensure!(
            take_profit_trigger.is_finite() && take_profit_trigger > 0.0,
            "take_profit_trigger_price must be positive"
        );
        anyhow::ensure!(
            stop_loss_trigger.is_finite() && stop_loss_trigger > 0.0,
            "stop_loss_trigger_price must be positive"
        );
        match entry_side {
            OrderSide::Buy => {
                anyhow::ensure!(
                    take_profit_trigger > entry_price,
                    "for long entry, take_profit_trigger_price must be above entry_price"
                );
                anyhow::ensure!(
                    stop_loss_trigger < entry_price,
                    "for long entry, stop_loss_trigger_price must be below entry_price"
                );
                Ok((OrderSide::Sell, take_profit_trigger, stop_loss_trigger))
            }
            OrderSide::Sell => {
                anyhow::ensure!(
                    take_profit_trigger < entry_price,
                    "for short entry, take_profit_trigger_price must be below entry_price"
                );
                anyhow::ensure!(
                    stop_loss_trigger > entry_price,
                    "for short entry, stop_loss_trigger_price must be above entry_price"
                );
                Ok((OrderSide::Buy, take_profit_trigger, stop_loss_trigger))
            }
        }
    } else {
        protective_exit_prices(
            entry_side,
            entry_price,
            options.take_profit_usd,
            options.stop_loss_pct,
        )
    }
}

fn protective_exit_limit_price(
    trigger_price: f64,
    exit_side: OrderSide,
    max_slippage_bps: f64,
) -> Result<f64> {
    anyhow::ensure!(
        trigger_price.is_finite() && trigger_price > 0.0,
        "trigger_price must be positive"
    );
    anyhow::ensure!(
        (0.0..10_000.0).contains(&max_slippage_bps),
        "max_slippage_bps must be >= 0 and < 10000"
    );
    let slippage = max_slippage_bps / 10_000.0;
    let limit_price = match exit_side {
        OrderSide::Buy => trigger_price * (1.0 + slippage),
        OrderSide::Sell => trigger_price * (1.0 - slippage),
    };
    anyhow::ensure!(limit_price > 0.0, "limit guard price must be positive");
    Ok(limit_price)
}

fn protective_leg_triggers(
    exit_side: OrderSide,
    kind: &str,
    observed_price: f64,
    trigger_price: f64,
) -> bool {
    match (exit_side, kind) {
        (OrderSide::Sell, "take_profit") => observed_price >= trigger_price,
        (OrderSide::Sell, "stop_loss") => observed_price <= trigger_price,
        (OrderSide::Buy, "take_profit") => observed_price <= trigger_price,
        (OrderSide::Buy, "stop_loss") => observed_price >= trigger_price,
        _ => false,
    }
}

fn valid_stop_loss_pct(value: f64) -> bool {
    value.is_finite() && value > 0.0 && value < 1.0
}

pub fn summarize_account_readiness_state(
    dex: &str,
    state: &ClearinghouseState,
    canonical_coin: &str,
) -> AccountReadinessState {
    let mut coin_position_size = 0.0;
    let mut coin_position_value_usd = 0.0;
    let mut coin_unrealized_pnl_usd = 0.0;
    for asset_position in &state.asset_positions {
        let position = &asset_position.position;
        if normalize_dex_coin(dex, &position.coin) == canonical_coin {
            coin_position_size += parse_state_decimal(&position.szi);
            coin_position_value_usd += position
                .position_value
                .as_deref()
                .map(parse_state_decimal)
                .unwrap_or_default();
            coin_unrealized_pnl_usd += position
                .unrealized_pnl
                .as_deref()
                .map(parse_state_decimal)
                .unwrap_or_default();
        }
    }

    AccountReadinessState {
        account_value_usd: parse_state_decimal(&state.margin_summary.account_value),
        withdrawable_usd: state
            .withdrawable
            .as_deref()
            .map(parse_state_decimal)
            .unwrap_or_default(),
        total_notional_position_usd: parse_state_decimal(&state.margin_summary.total_ntl_pos),
        total_margin_used_usd: parse_state_decimal(&state.margin_summary.total_margin_used),
        coin_position_size,
        coin_position_value_usd,
        coin_unrealized_pnl_usd,
    }
}

pub fn account_has_opening_collateral(state: &AccountReadinessState) -> bool {
    state.withdrawable_usd > 0.0 || state.account_value_usd > 0.0
}

pub fn reduce_only_position_available(side: OrderSide, coin_position_size: f64) -> bool {
    match side {
        OrderSide::Buy => coin_position_size < 0.0,
        OrderSide::Sell => coin_position_size > 0.0,
    }
}

pub fn reduce_only_position_detail(side: OrderSide, coin_position_size: f64) -> String {
    match side {
        OrderSide::Buy => format!(
            "reduce-only buy requires an existing short position; current coin position size={coin_position_size}"
        ),
        OrderSide::Sell => format!(
            "reduce-only sell requires an existing long position; current coin position size={coin_position_size}"
        ),
    }
}

fn normalize_order_coin_for_cancel(dex: &str, coin: &str) -> String {
    if is_spot_dex(dex) {
        normalize_spot_coin(coin)
    } else {
        normalize_dex_coin(dex, coin)
    }
}

fn order_coin_matches(dex: &str, order_coin: &str, coin: &str) -> bool {
    if is_spot_dex(dex)
        || order_coin.contains('/')
        || coin.contains('/')
        || order_coin.contains('-')
        || coin.contains('-')
    {
        normalize_spot_coin(order_coin) == normalize_spot_coin(coin)
    } else {
        normalize_dex_coin(dex, order_coin) == normalize_dex_coin(dex, coin)
    }
}

fn protective_open_order_kind(order: &OpenOrder) -> Option<&'static str> {
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

fn is_native_protective_open_order(order: &OpenOrder, coin: &str) -> bool {
    let same_coin = if order.coin.contains('/')
        || coin.contains('/')
        || order.coin.contains('-')
        || coin.contains('-')
    {
        normalize_spot_coin(&order.coin) == normalize_spot_coin(coin)
    } else {
        order.coin.trim().eq_ignore_ascii_case(coin.trim())
    };
    same_coin
        && order.is_trigger
        && order.reduce_only
        && protective_open_order_kind(order).is_some()
}

fn protective_order_status_kind(status: &OrderStatusResponse) -> Option<&'static str> {
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

fn protective_order_status_matches_coin(status: &OrderStatusResponse, coin: &str) -> bool {
    let Some(order) = status.order.as_ref().map(|entry| &entry.order) else {
        return false;
    };
    let same_coin = if order.coin.contains('/')
        || coin.contains('/')
        || order.coin.contains('-')
        || coin.contains('-')
    {
        normalize_spot_coin(&order.coin) == normalize_spot_coin(coin)
    } else {
        order.coin.trim().eq_ignore_ascii_case(coin.trim())
    };
    same_coin && order.reduce_only && protective_order_status_kind(status).is_some()
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

fn parse_state_decimal(value: &str) -> f64 {
    value.parse::<f64>().unwrap_or_default()
}

async fn fetch_acceptance_account_snapshot(
    config: &AppConfig,
    account: &AccountConfig,
) -> Result<SignedAcceptanceAccountSnapshot> {
    let query_dex = info_query_dex(&config.hyperliquid.dex);
    let open_orders =
        match fetch_open_orders(&config.app.environment, &query_dex, &account.address).await {
            Ok(open_orders) => open_orders,
            Err(error) => {
                tracing::warn!(
                    account_id = %account.account_id,
                    error = %error,
                    "best-effort acceptance open-orders snapshot failed; continuing"
                );
                Vec::new()
            }
        };
    let fills = match fetch_user_fills(&config.app.environment, &query_dex, &account.address).await
    {
        Ok(fills) => fills,
        Err(error) => {
            tracing::warn!(
                account_id = %account.account_id,
                error = %error,
                "best-effort acceptance user-fills snapshot failed; continuing"
            );
            Vec::new()
        }
    };
    let rate_limit = fetch_user_rate_limit(&config.app.environment, &account.address)
        .await
        .context("failed to fetch acceptance user rate limit")?;
    let request_capacity_remaining = rate_limit.request_capacity_remaining();
    Ok(SignedAcceptanceAccountSnapshot {
        open_order_count: open_orders.len(),
        fill_count: fills.len(),
        request_capacity_remaining,
        rate_limit,
    })
}

fn acceptance_check(name: &str, ok: bool, detail: String) -> SignedAcceptanceCheck {
    SignedAcceptanceCheck {
        name: name.to_string(),
        ok,
        detail,
    }
}

fn response_to_worker_report(
    order: ApprovedOrder,
    submitted_price: f64,
    submitted_size: f64,
    response: ExchangeResponseStatus,
) -> Result<WorkerReport> {
    let mut reports = response_to_worker_reports(vec![order], &response)?;
    let report = reports
        .drain(..)
        .next()
        .context("exchange response had no mapped worker reports")?;
    match report {
        WorkerReport::Submitted(mut submitted) => {
            submitted.submitted_price = Some(submitted_price);
            submitted.submitted_size = Some(submitted_size);
            Ok(WorkerReport::Submitted(submitted))
        }
        other => Ok(other),
    }
}

fn response_to_worker_reports(
    orders: Vec<ApprovedOrder>,
    response: &ExchangeResponseStatus,
) -> Result<Vec<WorkerReport>> {
    let statuses = match response {
        ExchangeResponseStatus::Ok(response) => response
            .data
            .as_ref()
            .context("exchange response missing order status data")?
            .statuses
            .clone(),
        ExchangeResponseStatus::Err(error) => anyhow::bail!("exchange rejected order: {error}"),
    };
    anyhow::ensure!(
        statuses.len() == orders.len(),
        "exchange status count {} does not match order count {}",
        statuses.len(),
        orders.len()
    );
    orders
        .into_iter()
        .zip(statuses)
        .map(|(order, status)| exchange_status_to_worker_report(order, status))
        .collect()
}

fn exchange_status_to_worker_report(
    order: ApprovedOrder,
    status: ExchangeDataStatus,
) -> Result<WorkerReport> {
    let submitted_price = order.price;
    let submitted_size = order.exact_size.or_else(|| {
        submitted_price.map(|price| {
            if price > 0.0 {
                order.notional_usd / price
            } else {
                0.0
            }
        })
    });
    match status {
        ExchangeDataStatus::Error(error) => {
            anyhow::bail!("exchange returned action-level order error: {error}")
        }
        ExchangeDataStatus::Filled(filled) => Ok(submitted_report(
            order,
            submitted_price.unwrap_or_default(),
            submitted_size.unwrap_or_default(),
            Some("filled".to_string()),
            Some(filled.oid),
            filled.total_sz.parse::<f64>().ok(),
            filled.avg_px.parse::<f64>().ok(),
        )),
        ExchangeDataStatus::Resting(resting) => Ok(submitted_report(
            order,
            submitted_price.unwrap_or_default(),
            submitted_size.unwrap_or_default(),
            Some("resting".to_string()),
            Some(resting.oid),
            None,
            None,
        )),
        ExchangeDataStatus::Success => Ok(submitted_report(
            order,
            submitted_price.unwrap_or_default(),
            submitted_size.unwrap_or_default(),
            Some("success".to_string()),
            None,
            None,
            None,
        )),
        ExchangeDataStatus::WaitingForFill => Ok(submitted_report(
            order,
            submitted_price.unwrap_or_default(),
            submitted_size.unwrap_or_default(),
            Some("waiting_for_fill".to_string()),
            None,
            None,
            None,
        )),
        ExchangeDataStatus::WaitingForTrigger => Ok(submitted_report(
            order,
            submitted_price.unwrap_or_default(),
            submitted_size.unwrap_or_default(),
            Some("waiting_for_trigger".to_string()),
            None,
            None,
            None,
        )),
    }
}

fn submitted_report(
    order: ApprovedOrder,
    submitted_price: f64,
    submitted_size: f64,
    exchange_status: Option<String>,
    oid: Option<u64>,
    filled_size: Option<f64>,
    avg_fill_price: Option<f64>,
) -> WorkerReport {
    WorkerReport::Submitted(OrderSubmitted {
        signal_id: order.signal_id.unwrap_or_default(),
        intent_id: order.intent_id,
        worker_id: order.worker_id,
        account_id: order.account_id,
        cloid: order.cloid,
        coin: order.coin,
        side: order.side,
        notional_usd: order.notional_usd,
        submitted_price: Some(submitted_price),
        submitted_size: Some(submitted_size),
        exchange_status,
        oid,
        filled_size,
        avg_fill_price,
        dry_run: false,
        submitted_at_ms: now_ms(),
    })
}

struct ErrorFallback {
    worker_id: String,
    account_id: String,
}

impl ErrorFallback {
    fn from_order(order: &ApprovedOrder) -> Self {
        Self {
            worker_id: order.worker_id.clone(),
            account_id: order.account_id.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        config::{AccountConfig, AppConfig},
        domain::{ApprovedOrder, ExecutionMode, ExecutionPolicy, OrderSide, WorkerReport, now_ms},
        hyperliquid::{
            ClearinghouseState, DexAssetContext, DexAssetMeta, DexMeta, SpotClearinghouseState,
            XyzMarketSnapshot, round_size_down,
        },
        trading::{
            AccountExecutor, AccountFundingBatchReport, HYPERLIQUID_MIN_ORDER_NOTIONAL_USD,
            MainnetSmokePlanOptions, ManualMarginMode, OrderStatusLookup, PerpFundingLayerReport,
            ProtectiveExitLeg, ProtectiveExitOptions, ProtectiveExitPlanResult,
            SignedLiveWindowOptions, SignedPreflightCheck, SignedSmokeOptions,
            SpotFundingLayerReport, UsdcDexTransferBatchPreflightResult,
            UsdcDexTransferLiveWindowOptions, UsdcDexTransferOptions, account_funding_account_ids,
            account_funding_report_next_actions, account_funding_report_summary,
            account_has_opening_collateral, asset_supports_cross_margin, build_mainnet_smoke_plan,
            build_signed_order_plan, effective_exchange_min_order_notional_ok,
            effective_order_notional_usd, ensure_live_account_address,
            evaluate_protective_exit_trigger, exchange_min_effective_detail,
            failed_preflight_blockers, mainnet_smoke_plan_next_actions,
            minimum_requested_notional_for_effective_min,
            normalized_transfer_destination_account_id, order_status_lookup,
            parse_manual_margin_mode, preflight_next_actions, preflight_readiness_summary,
            prepare_signed_live_window, prepare_usdc_dex_transfer_live_window,
            protective_exit_limit_price, protective_exit_prices,
            protective_exit_prices_for_options, reduce_only_position_available,
            response_to_worker_report, response_to_worker_reports, signed_runbook_check,
            summarize_account_readiness_state, usdc_transfer_amount_label,
            usdc_transfer_batch_preflight_next_actions, usdc_transfer_confirmation_phrase,
            usdc_transfer_preflight_next_actions, usdc_transfer_preflight_summary,
            validate_exchange_min_order_notional, validate_live_order_gates,
            validate_signed_smoke_constraints, validate_signed_submit_gates,
            validate_usdc_dex_transfer_gates,
        },
    };
    use hyperliquid_rust_sdk::{
        ExchangeDataStatus, ExchangeDataStatuses, ExchangeResponse, ExchangeResponseStatus,
        FilledOrder, RestingOrder,
    };
    use std::{collections::HashMap, fs};

    fn approved_order() -> ApprovedOrder {
        ApprovedOrder {
            risk_decision_id: "risk".to_string(),
            intent_id: "intent".to_string(),
            signal_id: Some("signal".to_string()),
            worker_id: "worker-addr_a".to_string(),
            account_id: "addr_a".to_string(),
            strategy_id: "manual_ops".to_string(),
            market: None,
            dex: None,
            coin: "xyz:NVDA".to_string(),
            side: OrderSide::Buy,
            notional_usd: 1.0,
            exact_size: None,
            price: None,
            execution_mode: ExecutionMode::Taker,
            execution_policy: ExecutionPolicy::Taker,
            max_slippage_bps: 20.0,
            reduce_only: false,
            cloid: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, b"test").to_string(),
            expires_at_ms: Some(now_ms() + 1000),
        }
    }

    fn signed_options(submit: bool) -> SignedSmokeOptions {
        SignedSmokeOptions {
            account_id: "addr_a".to_string(),
            coin: "xyz:NVDA".to_string(),
            side: OrderSide::Buy,
            notional_usd: 1.0,
            max_slippage_bps: 20.0,
            execution_mode: ExecutionMode::Taker,
            reduce_only: false,
            close_full_position: false,
            submit,
            cancel_resting: true,
            confirm_mainnet_live: false,
        }
    }

    #[tokio::test]
    async fn dry_run_bulk_submit_returns_one_report_per_order() {
        let first = approved_order();
        let mut second = approved_order();
        second.signal_id = Some("signal-2".to_string());
        second.intent_id = "intent-2".to_string();
        second.cloid = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, b"test-2").to_string();

        let reports = AccountExecutor::dry_run(true)
            .submit_bulk(vec![first, second])
            .await;

        assert_eq!(reports.len(), 2);
        assert!(matches!(&reports[0], WorkerReport::Submitted(_)));
        assert!(matches!(&reports[1], WorkerReport::Submitted(_)));
        if let WorkerReport::Submitted(submitted) = &reports[1] {
            assert_eq!(submitted.signal_id, "signal-2");
        }
    }

    #[test]
    fn order_status_lookup_requires_exactly_one_cli_key() {
        let oid_lookup = order_status_lookup(Some(42), None).expect("oid lookup");
        assert!(matches!(oid_lookup, OrderStatusLookup::Oid { oid: 42 }));

        let cloid_lookup = order_status_lookup(
            None,
            Some("00000000-0000-0000-0000-000000000001".to_string()),
        )
        .expect("cloid lookup");
        assert!(matches!(cloid_lookup, OrderStatusLookup::Cloid { .. }));

        let missing = order_status_lookup(None, None)
            .expect_err("missing lookup key")
            .to_string();
        assert!(missing.contains("exactly one"));

        let duplicate = order_status_lookup(
            Some(42),
            Some("00000000-0000-0000-0000-000000000001".to_string()),
        )
        .expect_err("duplicate lookup key")
        .to_string();
        assert!(duplicate.contains("only one"));

        let invalid = order_status_lookup(None, Some("not-a-uuid".to_string()))
            .expect_err("invalid uuid")
            .to_string();
        assert!(invalid.contains("valid UUID"));
    }

    fn test_account() -> AccountConfig {
        AccountConfig {
            account_id: "addr_a".to_string(),
            address: "0x1234567890abcdef1234567890abcdef12345678".to_string(),
            secret_id: "addr_a_api_wallet".to_string(),
            api_wallet_env: String::new(),
            transfer_secret_id: String::new(),
            transfer_wallet_env: String::new(),
            enabled: true,
            worker_enabled: true,
            copy_ratio: 0.1,
            max_order_notional_usd: 100.0,
            blocked_markets: Vec::new(),
        }
    }

    fn second_test_account() -> AccountConfig {
        AccountConfig {
            account_id: "addr_b".to_string(),
            address: "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_string(),
            secret_id: "addr_b_api_wallet".to_string(),
            api_wallet_env: String::new(),
            transfer_secret_id: String::new(),
            transfer_wallet_env: String::new(),
            enabled: true,
            worker_enabled: true,
            copy_ratio: 0.05,
            max_order_notional_usd: 100.0,
            blocked_markets: Vec::new(),
        }
    }

    fn protective_plan(entry_side: OrderSide, exit_side: OrderSide) -> ProtectiveExitPlanResult {
        ProtectiveExitPlanResult {
            environment: "testnet".to_string(),
            dex: "xyz".to_string(),
            account_id: "addr_a".to_string(),
            coin: "xyz:NVDA".to_string(),
            entry_side,
            exit_side,
            entry_price: 100.0,
            market_reference_price: 100.0,
            reduce_only: true,
            dry_run: true,
            legs: vec![
                ProtectiveExitLeg {
                    kind: "take_profit".to_string(),
                    trigger_price: if matches!(entry_side, OrderSide::Buy) {
                        102.0
                    } else {
                        98.0
                    },
                    limit_price: if matches!(exit_side, OrderSide::Sell) {
                        101.8
                    } else {
                        98.2
                    },
                    size: 0.01,
                    asset_id: 110_000,
                    sz_decimals: 3,
                    cloid: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, b"tp").to_string(),
                    local_trigger: true,
                },
                ProtectiveExitLeg {
                    kind: "stop_loss".to_string(),
                    trigger_price: if matches!(entry_side, OrderSide::Buy) {
                        97.0
                    } else {
                        103.0
                    },
                    limit_price: if matches!(exit_side, OrderSide::Sell) {
                        96.8
                    } else {
                        103.2
                    },
                    size: 0.01,
                    asset_id: 110_000,
                    sz_decimals: 3,
                    cloid: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, b"sl").to_string(),
                    local_trigger: true,
                },
            ],
        }
    }

    fn test_snapshot() -> XyzMarketSnapshot {
        XyzMarketSnapshot {
            dex: "xyz".to_string(),
            dex_index: 65,
            meta: DexMeta {
                universe: vec![DexAssetMeta {
                    name: "xyz:NVDA".to_string(),
                    sz_decimals: 3,
                    max_leverage: Some(20),
                    only_isolated: None,
                    is_delisted: None,
                    margin_mode: None,
                }],
            },
            asset_contexts: vec![DexAssetContext {
                funding: None,
                open_interest: None,
                prev_day_px: Some("180.0".to_string()),
                day_ntl_vlm: None,
                premium: None,
                oracle_px: Some("181.0".to_string()),
                mark_px: Some("182.0".to_string()),
                mid_px: Some("200.0".to_string()),
                impact_pxs: None,
            }],
            coin_to_asset: HashMap::from([("xyz:NVDA".to_string(), 750_000)]),
        }
    }

    #[test]
    fn account_readiness_state_summarizes_collateral_and_reduce_only_position() {
        let raw = r#"{
            "marginSummary": {
                "accountValue": "25.5",
                "totalNtlPos": "12.25",
                "totalRawUsd": "25.5",
                "totalMarginUsed": "1.5"
            },
            "withdrawable": "24.0",
            "assetPositions": [{
                "type": "oneWay",
                "position": {
                    "coin": "NVDA",
                    "szi": "0.006",
                    "entryPx": "180.0",
                    "positionValue": "1.08",
                    "unrealizedPnl": "0.03"
                }
            }, {
                "type": "oneWay",
                "position": {
                    "coin": "xyz:TSLA",
                    "szi": "-0.002",
                    "positionValue": "0.40"
                }
            }]
        }"#;
        let state: ClearinghouseState = serde_json::from_str(raw).expect("clearinghouse state");

        let summary = summarize_account_readiness_state("xyz", &state, "xyz:NVDA");

        assert_eq!(summary.account_value_usd, 25.5);
        assert_eq!(summary.withdrawable_usd, 24.0);
        assert_eq!(summary.total_notional_position_usd, 12.25);
        assert_eq!(summary.total_margin_used_usd, 1.5);
        assert_eq!(summary.coin_position_size, 0.006);
        assert_eq!(summary.coin_position_value_usd, 1.08);
        assert_eq!(summary.coin_unrealized_pnl_usd, 0.03);
        assert!(account_has_opening_collateral(&summary));
        assert!(reduce_only_position_available(
            OrderSide::Sell,
            summary.coin_position_size
        ));
        assert!(!reduce_only_position_available(
            OrderSide::Buy,
            summary.coin_position_size
        ));
    }

    #[test]
    fn account_readiness_state_blocks_zero_collateral_and_missing_reduce_only_side() {
        let raw = r#"{
            "marginSummary": {
                "accountValue": "0",
                "totalNtlPos": "0",
                "totalRawUsd": "0",
                "totalMarginUsed": "0"
            },
            "withdrawable": "0",
            "assetPositions": []
        }"#;
        let state: ClearinghouseState = serde_json::from_str(raw).expect("clearinghouse state");

        let summary = summarize_account_readiness_state("xyz", &state, "xyz:NVDA");

        assert!(!account_has_opening_collateral(&summary));
        assert!(!reduce_only_position_available(
            OrderSide::Sell,
            summary.coin_position_size
        ));
        assert!(!reduce_only_position_available(
            OrderSide::Buy,
            summary.coin_position_size
        ));
    }

    #[test]
    fn spot_reduce_only_sell_requires_inventory_and_buy_stays_blocked() {
        assert!(super::reduce_only_spot_position_available(
            OrderSide::Sell,
            10.0
        ));
        assert!(!super::reduce_only_spot_position_available(
            OrderSide::Sell,
            0.0
        ));
        assert!(!super::reduce_only_spot_position_available(
            OrderSide::Buy,
            10.0
        ));
        assert!(
            super::reduce_only_spot_position_detail(OrderSide::Sell, 0.0)
                .contains("available base inventory")
        );
    }

    #[test]
    fn preflight_summary_lists_blockers_and_next_actions() {
        let checks = vec![
            SignedPreflightCheck::blocker("vault_password_available", false, "vault locked"),
            SignedPreflightCheck::blocker(
                "account_has_available_collateral",
                false,
                "accountValue=0",
            ),
            SignedPreflightCheck::blocker("signed_order_plan_valid", true, "plan ok"),
        ];

        let failed = failed_preflight_blockers(&checks);
        let actions = preflight_next_actions(&checks, false);
        let summary = preflight_readiness_summary("testnet", false, false, &failed);

        assert_eq!(failed.len(), 2);
        assert!(failed[0].contains("vault_password_available"));
        assert!(actions.iter().any(|action| action.contains("Unlock")));
        assert!(actions.iter().any(|action| action.contains("Fund")));
        assert_eq!(summary, "testnet signed submit blocked by 2 check(s)");
    }

    #[test]
    fn exchange_min_order_notional_blocks_opening_but_not_reduce_only() {
        let error = validate_exchange_min_order_notional(1.0, false)
            .expect_err("opening order below exchange minimum must be blocked")
            .to_string();
        assert!(error.contains("at least 10"));

        validate_exchange_min_order_notional(1.0, true)
            .expect("reduce-only close path should remain available for small residual positions");

        let checks = vec![SignedPreflightCheck::blocker(
            "exchange_min_order_notional",
            false,
            "opening orders must be at least 10 USD",
        )];
        let actions = preflight_next_actions(&checks, false);
        assert!(
            actions
                .iter()
                .any(|action| action.contains("at least 10 USD"))
        );
    }

    #[test]
    fn effective_min_notional_recommends_request_above_precision_floor() {
        let reference_price = 28_829.0;
        let limit_price = 28_887.0;
        let sz_decimals = 4;
        let requested_notional = 10.0;
        let rounded_size = round_size_down(requested_notional / reference_price, sz_decimals);
        let effective_notional = effective_order_notional_usd(limit_price, rounded_size);

        assert_eq!(rounded_size, 0.0003);
        assert!(!effective_exchange_min_order_notional_ok(
            limit_price,
            rounded_size,
            false
        ));
        assert!((effective_notional - 8.6661).abs() < 0.000_001);

        let recommended = minimum_requested_notional_for_effective_min(
            limit_price,
            reference_price,
            sz_decimals,
            HYPERLIQUID_MIN_ORDER_NOTIONAL_USD,
        )
        .expect("current market precision should produce a recommendation");
        assert!(recommended > 11.53 && recommended < 11.54);

        let next_size = round_size_down(12.0 / reference_price, sz_decimals);
        assert!(effective_exchange_min_order_notional_ok(
            limit_price,
            next_size,
            false
        ));
        assert!(
            exchange_min_effective_detail(effective_notional, Some(recommended))
                .contains("request at least 11.531601 USD")
        );
    }

    #[test]
    fn spot_close_full_position_uses_close_gate_without_exchange_reduce_only() {
        assert!(super::signed_close_exempt_from_opening_rules(
            "spot",
            OrderSide::Sell,
            false,
            true,
        ));
        assert!(!super::signed_exchange_reduce_only_flag(
            "spot",
            OrderSide::Sell,
            false,
            true,
        ));
        validate_exchange_min_order_notional(0.5, true)
            .expect("spot sell-to-close should bypass the opening-order floor");
        assert!(!super::signed_close_exempt_from_opening_rules(
            "spot",
            OrderSide::Buy,
            false,
            true,
        ));
    }

    #[test]
    fn usdc_transfer_cli_preflight_helpers_format_phrase_and_actions() {
        let phrase = usdc_transfer_confirmation_phrase("addr_a", 2.0, "xyz");
        assert_eq!(phrase, "TRANSFER 2 USDC TO xyz FOR addr_a");
        assert_eq!(usdc_transfer_amount_label(2.500000), "2.5");

        let checks = vec![
            SignedPreflightCheck::blocker("config_dry_run_disabled", false, "dry-run"),
            SignedPreflightCheck::blocker("mainnet_explicit_confirmation", false, "phrase"),
            SignedPreflightCheck::blocker("vault_password_available", false, "missing"),
            SignedPreflightCheck::blocker(
                "evm_transfer_signer_available",
                false,
                "signer mismatch",
            ),
            SignedPreflightCheck::blocker("transfer_plan_valid", true, "plan ok"),
        ];
        let failed = failed_preflight_blockers(&checks);
        let actions =
            usdc_transfer_preflight_next_actions(&checks, "mainnet", "", "xyz", Some(&phrase));
        let summary = usdc_transfer_preflight_summary("mainnet", false, false, &failed);

        assert_eq!(failed.len(), 4);
        assert!(summary.contains("blocked by 4 check"));
        assert!(
            actions
                .iter()
                .any(|action| action.contains("app.dry_run=false"))
        );
        assert!(actions.iter().any(|action| action.contains(&phrase)));
        assert!(
            actions
                .iter()
                .any(|action| action.contains("EVM account wallet"))
        );
    }

    #[test]
    fn usdc_transfer_batch_preflight_next_actions_stop_until_all_ready() {
        let account_ids = vec!["addr_a".to_string(), "addr_b".to_string()];
        let ready = vec!["addr_a".to_string()];
        let blocked = vec!["addr_b".to_string()];
        let actions = usdc_transfer_batch_preflight_next_actions(
            &account_ids,
            &ready,
            &blocked,
            &[],
            2.0,
            "",
            "xyz",
        );

        assert!(actions.iter().any(|action| action.contains("addr_b")));
        assert!(
            actions
                .iter()
                .any(|action| action.contains("Do not submit"))
        );

        let ready_all = vec!["addr_a".to_string(), "addr_b".to_string()];
        let actions = usdc_transfer_batch_preflight_next_actions(
            &account_ids,
            &ready_all,
            &[],
            &[],
            2.0,
            "",
            "xyz",
        );
        assert!(
            actions
                .iter()
                .any(|action| action.contains("All selected accounts are ready"))
        );
        assert!(
            actions
                .iter()
                .any(|action| action.contains("usdc-dex-transfer-live-window"))
        );
    }

    #[test]
    fn usdc_transfer_runbook_submit_check_stops_when_preflight_blocked() {
        let check = signed_runbook_check(
            "transfer_submitted",
            false,
            "preflight blockers remain; USDC transfer did not load secrets or submit".to_string(),
        );

        assert_eq!(check.name, "transfer_submitted");
        assert!(!check.ok);
        assert!(check.detail.contains("did not load secrets"));
    }

    #[test]
    fn usdc_transfer_live_window_writes_isolated_live_config() {
        let test_id = now_ms();
        let dir = std::env::temp_dir().join(format!("trade_xyz_live_window_{test_id}"));
        fs::create_dir_all(&dir).expect("create temp dir");
        let source_config_path = dir.join("local.toml");
        let output_config_path = std::path::PathBuf::from(format!(
            ".codex-longrun/mainnet-transfer-window-{test_id}.toml"
        ));
        let _ = fs::remove_file(&output_config_path);

        let mut config = AppConfig::default();
        config.app.environment = "mainnet".to_string();
        config.app.dry_run = true;
        config.hyperliquid.dex = "xyz".to_string();
        config.manual_ops.manual_live_enabled = false;
        config.manual_ops.mainnet_live_enabled = false;
        config.accounts = vec![test_account(), second_test_account()];
        crate::config::save_config(&source_config_path, &config).expect("write source config");

        let result = prepare_usdc_dex_transfer_live_window(
            &source_config_path,
            config.clone(),
            UsdcDexTransferLiveWindowOptions {
                account_ids: vec!["addr_a".to_string(), "addr_b".to_string()],
                amount_usdc: 2.0,
                destination_dex: Some("xyz".to_string()),
                output_config_path: output_config_path.clone(),
                write: true,
                overwrite: false,
            },
        )
        .expect("live window plan");

        assert!(result.config_written);
        assert_eq!(result.account_ids, vec!["addr_a", "addr_b"]);
        assert_eq!(
            result.output_config_path,
            output_config_path.to_string_lossy()
        );
        assert!(
            result
                .required_config_changes
                .iter()
                .any(|change| change == "app.dry_run=false")
        );
        assert!(
            result.accounts[0]
                .confirmation_phrase
                .contains("TRANSFER 2 USDC TO xyz FOR addr_a")
        );
        assert!(
            result.accounts[0]
                .runbook_args
                .iter()
                .any(|arg| arg == "--submit")
        );

        let source_config = crate::config::load_config(&source_config_path).expect("source config");
        assert!(source_config.app.dry_run);
        assert!(!source_config.manual_ops.manual_live_enabled);
        assert!(!source_config.manual_ops.mainnet_live_enabled);

        let live_config = crate::config::load_config(&output_config_path).expect("live config");
        assert!(!live_config.app.dry_run);
        assert!(live_config.manual_ops.manual_live_enabled);
        assert!(live_config.manual_ops.mainnet_live_enabled);

        let _ = fs::remove_file(&output_config_path);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn usdc_transfer_live_window_rejects_output_outside_codex_longrun() {
        let source_config_path = std::path::PathBuf::from("config/local.toml");
        let mut config = AppConfig::default();
        config.app.environment = "mainnet".to_string();
        config.accounts = vec![test_account()];

        let error = prepare_usdc_dex_transfer_live_window(
            &source_config_path,
            config,
            UsdcDexTransferLiveWindowOptions {
                account_ids: vec!["addr_a".to_string()],
                amount_usdc: 2.0,
                destination_dex: Some("xyz".to_string()),
                output_config_path: std::path::PathBuf::from("config/local.toml"),
                write: false,
                overwrite: false,
            },
        )
        .expect_err("output outside .codex-longrun must be rejected")
        .to_string();

        assert!(error.contains(".codex-longrun"));
    }

    #[test]
    fn signed_live_window_writes_isolated_config_and_commands() {
        let test_id = now_ms();
        let dir = std::env::temp_dir().join(format!("trade_xyz_signed_live_window_{test_id}"));
        fs::create_dir_all(&dir).expect("create temp dir");
        let source_config_path = dir.join("local.toml");
        let output_config_path = std::path::PathBuf::from(format!(
            ".codex-longrun/mainnet-order-live-window-{test_id}.toml"
        ));
        let _ = fs::remove_file(&output_config_path);

        let mut config = AppConfig::default();
        config.app.environment = "mainnet".to_string();
        config.app.dry_run = true;
        config.hyperliquid.dex = "xyz".to_string();
        config.manual_ops.manual_live_enabled = false;
        config.manual_ops.mainnet_live_enabled = false;
        config.manual_ops.max_manual_order_notional_usd = 1.0;
        config.manual_ops.blocked_symbols = vec!["xyz:TSLA".to_string()];
        config.accounts = vec![test_account(), second_test_account()];
        crate::config::save_config(&source_config_path, &config).expect("write source config");

        let result = prepare_signed_live_window(
            &source_config_path,
            config.clone(),
            SignedLiveWindowOptions {
                account_ids: vec!["addr_a".to_string(), "addr_b".to_string()],
                coin: "NVDA".to_string(),
                side: OrderSide::Buy,
                notional_usd: 1.0,
                max_slippage_bps: 20.0,
                execution_mode: ExecutionMode::Taker,
                output_config_path: output_config_path.clone(),
                write: true,
                overwrite: false,
            },
        )
        .expect("signed live window plan");

        assert!(result.config_written);
        assert_eq!(result.coin, "xyz:NVDA");
        assert_eq!(result.account_ids, vec!["addr_a", "addr_b"]);
        assert!(
            result.accounts[0]
                .submit_runbook_args
                .iter()
                .any(|arg| arg == "--submit")
        );
        assert!(
            result.accounts[0]
                .reduce_only_close_runbook_args
                .windows(2)
                .any(|pair| pair[0] == "--reduce-only" && pair[1] == "true")
        );
        assert!(result.accounts.iter().all(|account| {
            account
                .reconcile_args
                .iter()
                .any(|arg| arg == "reconcile-account")
        }));

        let source_config = crate::config::load_config(&source_config_path).expect("source config");
        assert!(source_config.app.dry_run);
        assert!(!source_config.manual_ops.manual_live_enabled);
        assert!(!source_config.manual_ops.mainnet_live_enabled);

        let live_config = crate::config::load_config(&output_config_path).expect("live config");
        assert!(!live_config.app.dry_run);
        assert!(live_config.manual_ops.manual_live_enabled);
        assert!(live_config.manual_ops.mainnet_live_enabled);
        assert!(!live_config.risk.global.kill_switch);

        let _ = fs::remove_file(&output_config_path);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn mainnet_smoke_plan_actions_stop_before_unfunded_order_submit() {
        let transfer_preflight = UsdcDexTransferBatchPreflightResult {
            environment: "mainnet".to_string(),
            dex: "xyz".to_string(),
            account_ids: vec!["addr_a".to_string(), "addr_b".to_string()],
            ready_account_ids: vec![],
            blocked_account_ids: vec!["addr_a".to_string(), "addr_b".to_string()],
            failed_account_ids: vec![],
            amount_usdc: 2.0,
            source_dex: String::new(),
            destination_dex: "xyz".to_string(),
            results: vec![],
            next_actions: vec!["Do not submit any USDC movement yet.".to_string()],
        };
        let funding = AccountFundingBatchReport {
            environment: "mainnet".to_string(),
            dex: "xyz".to_string(),
            account_ids: vec!["addr_a".to_string(), "addr_b".to_string()],
            ready_account_ids: vec![],
            transfer_needed_account_ids: vec!["addr_a".to_string(), "addr_b".to_string()],
            failed_account_ids: vec![],
            results: vec![],
        };

        let actions = mainnet_smoke_plan_next_actions(false, false, &transfer_preflight, &funding);

        assert!(
            actions
                .iter()
                .any(|action| action.contains("Clear funding transfer blockers"))
        );
        assert!(
            actions
                .iter()
                .any(|action| action.contains("Do not submit opening orders yet"))
        );
    }

    #[tokio::test]
    async fn mainnet_smoke_plan_rejects_non_mainnet_config() {
        let mut config = AppConfig::default();
        config.app.environment = "testnet".to_string();
        config.accounts = vec![test_account()];

        let error = build_mainnet_smoke_plan(
            std::path::PathBuf::from("config/local.toml"),
            config,
            MainnetSmokePlanOptions {
                account_ids: vec!["addr_a".to_string()],
                funding_amount_usdc: 2.0,
                destination_dex: Some("xyz".to_string()),
                coin: "xyz:NVDA".to_string(),
                side: OrderSide::Buy,
                order_notional_usd: 1.0,
                max_slippage_bps: 20.0,
                execution_mode: ExecutionMode::Taker,
                transfer_output_config_path: std::path::PathBuf::from(
                    ".codex-longrun/mainnet-usdc-transfer-window.toml",
                ),
                order_output_config_path: std::path::PathBuf::from(
                    ".codex-longrun/mainnet-order-live-window.toml",
                ),
            },
            None,
        )
        .await
        .expect_err("non-mainnet config must be rejected")
        .to_string();

        assert!(error.contains("mainnet"));
    }

    #[test]
    fn account_funding_report_points_to_default_perp_layer() {
        let default_state: ClearinghouseState = serde_json::from_str(
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
        .expect("default state");
        let zero_xyz_state: ClearinghouseState = serde_json::from_str(
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
        .expect("xyz state");
        let spot_state: SpotClearinghouseState =
            serde_json::from_str(r#"{"balances":[]}"#).expect("spot state");

        let default_perp = PerpFundingLayerReport::from_state("default_perp", &default_state);
        let xyz_perp = PerpFundingLayerReport::from_state("xyz_perp", &zero_xyz_state);
        let spot = SpotFundingLayerReport::from_state(&spot_state);
        let summary = account_funding_report_summary(&default_perp, &xyz_perp, &spot);
        let actions =
            account_funding_report_next_actions(&default_perp, &xyz_perp, &spot, "mainnet", "xyz");

        assert!(summary.contains("default perp"));
        assert!(
            actions
                .iter()
                .any(|action| action.contains("Transfer USDC"))
        );
    }

    #[test]
    fn account_funding_account_ids_default_to_enabled_workers_and_dedupes() {
        let mut config = AppConfig::default();
        let mut disabled = second_test_account();
        disabled.account_id = "addr_disabled".to_string();
        disabled.enabled = false;
        config.accounts = vec![test_account(), second_test_account(), disabled];

        let defaults = account_funding_account_ids(&config, &[]);
        assert_eq!(defaults, vec!["addr_a", "addr_b"]);

        let requested = account_funding_account_ids(
            &config,
            &[
                " addr_b ".to_string(),
                "addr_a".to_string(),
                "addr_b".to_string(),
                "".to_string(),
            ],
        );
        assert_eq!(requested, vec!["addr_b", "addr_a"]);
    }

    #[test]
    fn signed_submit_requires_config_dry_run_false() {
        let mut config = AppConfig::default();
        config.manual_ops.manual_live_enabled = true;

        let error = validate_signed_submit_gates(&config, &signed_options(true))
            .expect_err("dry-run config must block submit")
            .to_string();

        assert!(error.contains("app.dry_run=false"));
    }

    #[test]
    fn signed_plan_does_not_require_live_gates() {
        let config = AppConfig::default();

        validate_signed_submit_gates(&config, &signed_options(false))
            .expect("plan-only mode should not require live gates");
    }

    #[test]
    fn usdc_dex_transfer_plan_does_not_require_live_gates() {
        let config = AppConfig::default();
        let options = UsdcDexTransferOptions {
            account_id: "addr_a".to_string(),
            destination_account_id: None,
            amount_usdc: 2.0,
            source_dex: None,
            destination_dex: None,
            submit: false,
            confirm_mainnet_live: false,
        };

        validate_usdc_dex_transfer_gates(&config, &options)
            .expect("plan-only funding transfer should be read-only");
    }

    #[test]
    fn usdc_transfer_same_destination_sentinel_maps_to_source_account() {
        assert_eq!(
            normalized_transfer_destination_account_id("addr_a", None),
            "addr_a"
        );
        assert_eq!(
            normalized_transfer_destination_account_id("addr_a", Some("__same__")),
            "addr_a"
        );
        assert_eq!(
            normalized_transfer_destination_account_id("addr_a", Some("addr_b")),
            "addr_b"
        );
    }

    #[test]
    fn usdc_dex_transfer_submit_requires_live_gates_and_cap() {
        let mut config = AppConfig::default();
        config.app.environment = "mainnet".to_string();

        let mut options = UsdcDexTransferOptions {
            account_id: "addr_a".to_string(),
            destination_account_id: None,
            amount_usdc: 2.0,
            source_dex: None,
            destination_dex: Some("xyz".to_string()),
            submit: true,
            confirm_mainnet_live: true,
        };

        let dry_run_error = validate_usdc_dex_transfer_gates(&config, &options)
            .expect_err("dry-run config must block state-changing funding transfer")
            .to_string();
        assert!(dry_run_error.contains("app.dry_run=false"));

        config.app.dry_run = false;
        let manual_live_error = validate_usdc_dex_transfer_gates(&config, &options)
            .expect_err("manual live gate must block transfer")
            .to_string();
        assert!(manual_live_error.contains("manual_live_enabled"));

        config.manual_ops.manual_live_enabled = true;
        let mainnet_error = validate_usdc_dex_transfer_gates(&config, &options)
            .expect_err("mainnet transfer must require mainnet live gate")
            .to_string();
        assert!(mainnet_error.contains("mainnet"));

        config.manual_ops.mainnet_live_enabled = true;
        validate_usdc_dex_transfer_gates(&config, &options)
            .expect("all explicit transfer gates should pass");

        options.amount_usdc = 11.0;
        let cap_error = validate_usdc_dex_transfer_gates(&config, &options)
            .expect_err("transfer helper cap must apply")
            .to_string();
        assert!(cap_error.contains("10 USDC"));

        options.amount_usdc = 2.0;
        options.source_dex = Some("spot".to_string());
        validate_usdc_dex_transfer_gates(&config, &options)
            .expect("spot source is supported when route changes state");

        options.destination_dex = Some("spot".to_string());
        let route_error = validate_usdc_dex_transfer_gates(&config, &options)
            .expect_err("same account-layer route must be blocked")
            .to_string();
        assert!(route_error.contains("same account layer"));
    }

    #[test]
    fn signed_submit_requires_global_kill_switch_clear() {
        let mut config = AppConfig::default();
        config.app.dry_run = false;
        config.manual_ops.manual_live_enabled = true;
        config.risk.global.kill_switch = true;

        let error = validate_signed_submit_gates(&config, &signed_options(true))
            .expect_err("kill switch must block signed submit")
            .to_string();

        assert!(error.contains("kill switch"));
    }

    #[test]
    fn signed_reduce_only_allows_kill_switch_when_configured() {
        let mut config = AppConfig::default();
        config.app.dry_run = false;
        config.manual_ops.manual_live_enabled = true;
        config.risk.global.kill_switch = true;
        config.risk.global.allow_reduce_only_when_killed = true;

        validate_live_order_gates(&config, false, true)
            .expect("reduce-only signed close should remain available during kill switch");
    }

    #[test]
    fn signed_reduce_only_respects_kill_switch_policy() {
        let mut config = AppConfig::default();
        config.app.dry_run = false;
        config.manual_ops.manual_live_enabled = true;
        config.risk.global.kill_switch = true;
        config.risk.global.allow_reduce_only_when_killed = false;

        let error = validate_live_order_gates(&config, false, true)
            .expect_err("reduce-only must be blocked when the policy disables it")
            .to_string();

        assert!(error.contains("reduce-only"));
    }

    #[test]
    fn signed_constraints_reject_manual_notional_over_limit() {
        let mut config = AppConfig::default();
        config.manual_ops.max_manual_order_notional_usd = 0.5;

        let error =
            validate_signed_smoke_constraints(&config, &test_account(), &signed_options(false))
                .expect_err("manual max notional must apply to CLI signed smoke")
                .to_string();

        assert!(error.contains("max_manual_order_notional_usd"));
    }

    #[test]
    fn signed_constraints_reject_disallowed_symbol() {
        let mut config = AppConfig::default();
        config.manual_ops.blocked_symbols = vec!["xyz:NVDA".to_string()];

        let error =
            validate_signed_smoke_constraints(&config, &test_account(), &signed_options(false))
                .expect_err("allowed symbols must apply to CLI signed smoke")
                .to_string();

        assert!(error.contains("manual symbol xyz:NVDA is blocked"));
    }

    #[test]
    fn signed_taker_plan_crosses_with_slippage_guard() {
        let plan = build_signed_order_plan(
            &test_snapshot(),
            "NVDA",
            OrderSide::Buy,
            10.0,
            20.0,
            ExecutionMode::Taker,
            None,
        )
        .expect("taker plan");

        assert_eq!(plan.reference_price, 200.0);
        assert!(plan.limit_price > plan.reference_price);
    }

    #[test]
    fn signed_maker_plan_offsets_away_from_crossing() {
        let buy = build_signed_order_plan(
            &test_snapshot(),
            "NVDA",
            OrderSide::Buy,
            10.0,
            20.0,
            ExecutionMode::Maker,
            None,
        )
        .expect("maker buy plan");
        let sell = build_signed_order_plan(
            &test_snapshot(),
            "NVDA",
            OrderSide::Sell,
            10.0,
            20.0,
            ExecutionMode::Maker,
            None,
        )
        .expect("maker sell plan");

        assert!(buy.limit_price < buy.reference_price);
        assert!(sell.limit_price > sell.reference_price);
        assert_eq!(buy.size, sell.size);
    }

    #[test]
    fn live_account_address_rejects_placeholder() {
        let account = AccountConfig {
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
        };

        let error = ensure_live_account_address(&account)
            .expect_err("placeholder address must not pass live signed execution")
            .to_string();

        assert!(error.contains("not an example placeholder"));
    }

    #[test]
    fn live_account_address_accepts_realistic_evm_address() {
        let account = AccountConfig {
            account_id: "addr_a".to_string(),
            address: "0x1234567890abcdef1234567890abcdef12345678".to_string(),
            secret_id: "addr_a_api_wallet".to_string(),
            api_wallet_env: String::new(),
            transfer_secret_id: String::new(),
            transfer_wallet_env: String::new(),
            enabled: true,
            worker_enabled: true,
            copy_ratio: 0.1,
            max_order_notional_usd: 100.0,
            blocked_markets: Vec::new(),
        };

        ensure_live_account_address(&account).expect("realistic EVM address should pass");
    }

    #[test]
    fn protective_exit_prices_for_long_entry() {
        let (exit_side, take_profit, stop_loss) =
            protective_exit_prices(OrderSide::Buy, 100.0, 2.5, 0.03)
                .expect("long protective exits");

        assert!(matches!(exit_side, OrderSide::Sell));
        assert_eq!(take_profit, 102.5);
        assert_eq!(stop_loss, 97.0);
    }

    #[test]
    fn protective_exit_prices_for_short_entry() {
        let (exit_side, take_profit, stop_loss) =
            protective_exit_prices(OrderSide::Sell, 100.0, 2.5, 0.03)
                .expect("short protective exits");

        assert!(matches!(exit_side, OrderSide::Buy));
        assert_eq!(take_profit, 97.5);
        assert_eq!(stop_loss, 103.0);
    }

    #[test]
    fn protective_exit_prices_for_explicit_long_triggers() {
        let options = ProtectiveExitOptions {
            account_id: "addr_a".to_string(),
            coin: "xyz:NVDA".to_string(),
            entry_side: "buy".to_string(),
            entry_price: Some(100.0),
            notional_usd: 10.0,
            take_profit_usd: 1.0,
            stop_loss_pct: 0.01,
            take_profit_trigger_price: Some(102.0),
            stop_loss_trigger_price: Some(98.0),
            max_slippage_bps: 20.0,
        };

        let (exit_side, take_profit, stop_loss) =
            protective_exit_prices_for_options(OrderSide::Buy, 100.0, &options)
                .expect("explicit long triggers");

        assert!(matches!(exit_side, OrderSide::Sell));
        assert_eq!(take_profit, 102.0);
        assert_eq!(stop_loss, 98.0);
    }

    #[test]
    fn protective_exit_prices_for_options_rejects_invalid_explicit_trigger_side() {
        let options = ProtectiveExitOptions {
            account_id: "addr_a".to_string(),
            coin: "xyz:NVDA".to_string(),
            entry_side: "buy".to_string(),
            entry_price: Some(100.0),
            notional_usd: 10.0,
            take_profit_usd: 1.0,
            stop_loss_pct: 0.01,
            take_profit_trigger_price: Some(99.0),
            stop_loss_trigger_price: Some(98.0),
            max_slippage_bps: 20.0,
        };

        let error = protective_exit_prices_for_options(OrderSide::Buy, 100.0, &options)
            .expect_err("invalid explicit long TP should fail")
            .to_string();
        assert!(error.contains("take_profit_trigger_price must be above"));
    }

    #[test]
    fn protective_exit_limit_price_applies_exit_slippage_direction() {
        let sell_limit =
            protective_exit_limit_price(100.0, OrderSide::Sell, 20.0).expect("sell exit limit");
        let buy_limit =
            protective_exit_limit_price(100.0, OrderSide::Buy, 20.0).expect("buy exit limit");

        assert_eq!(sell_limit, 99.8);
        assert_eq!(buy_limit, 100.2);
    }

    #[test]
    fn protective_trigger_fires_take_profit_for_long_exit_plan() {
        let result = evaluate_protective_exit_trigger(
            protective_plan(OrderSide::Buy, OrderSide::Sell),
            103.0,
        )
        .expect("trigger check");

        assert!(result.triggered);
        assert_eq!(
            result.triggered_leg.as_ref().map(|leg| leg.kind.as_str()),
            Some("take_profit")
        );
        let order = result.exit_order.expect("exit order preview");
        assert!(matches!(order.side, OrderSide::Sell));
        assert!(order.reduce_only);
        assert_eq!(order.size, 0.01);
    }

    #[test]
    fn protective_trigger_fires_stop_loss_for_short_exit_plan() {
        let result = evaluate_protective_exit_trigger(
            protective_plan(OrderSide::Sell, OrderSide::Buy),
            104.0,
        )
        .expect("trigger check");

        assert!(result.triggered);
        assert_eq!(
            result.triggered_leg.as_ref().map(|leg| leg.kind.as_str()),
            Some("stop_loss")
        );
        let order = result.exit_order.expect("exit order preview");
        assert!(matches!(order.side, OrderSide::Buy));
        assert!(order.reduce_only);
    }

    #[test]
    fn protective_trigger_waits_inside_band() {
        let result = evaluate_protective_exit_trigger(
            protective_plan(OrderSide::Buy, OrderSide::Sell),
            100.0,
        )
        .expect("trigger check");

        assert!(!result.triggered);
        assert!(result.triggered_leg.is_none());
        assert!(result.exit_order.is_none());
    }

    #[test]
    fn parse_manual_margin_mode_accepts_cross_and_isolated_aliases() {
        assert_eq!(
            parse_manual_margin_mode("cross").expect("cross mode"),
            ManualMarginMode::Cross
        );
        assert_eq!(
            parse_manual_margin_mode("strict_isolated").expect("isolated mode"),
            ManualMarginMode::Isolated
        );
        assert!(parse_manual_margin_mode("invalid_mode").is_err());
    }

    #[test]
    fn asset_supports_cross_margin_respects_isolated_flags() {
        assert!(!asset_supports_cross_margin(Some(true), None));
        assert!(!asset_supports_cross_margin(None, Some("strictIsolated")));
        assert!(asset_supports_cross_margin(None, Some("cross")));
    }

    #[test]
    fn action_level_error_is_not_treated_as_success() {
        let response = ExchangeResponseStatus::Ok(ExchangeResponse {
            response_type: "order".to_string(),
            data: Some(ExchangeDataStatuses {
                statuses: vec![ExchangeDataStatus::Error("bad order".to_string())],
            }),
        });

        let result = response_to_worker_report(approved_order(), 100.0, 0.01, response);

        assert!(result.is_err());
    }

    #[test]
    fn filled_response_becomes_submitted_report_with_fill_details() {
        let response = ExchangeResponseStatus::Ok(ExchangeResponse {
            response_type: "order".to_string(),
            data: Some(ExchangeDataStatuses {
                statuses: vec![ExchangeDataStatus::Filled(FilledOrder {
                    total_sz: "0.01".to_string(),
                    avg_px: "100.0".to_string(),
                    oid: 42,
                })],
            }),
        });

        let result =
            response_to_worker_report(approved_order(), 100.0, 0.01, response).expect("report");

        match result {
            WorkerReport::Submitted(report) => {
                assert_eq!(report.exchange_status.as_deref(), Some("filled"));
                assert_eq!(report.oid, Some(42));
                assert_eq!(report.filled_size, Some(0.01));
            }
            _ => panic!("expected submitted report"),
        }
    }

    #[test]
    fn bulk_response_maps_each_status_to_corresponding_report() {
        let first = approved_order();
        let mut second = approved_order();
        second.cloid = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, b"second").to_string();
        second.side = OrderSide::Sell;
        second.notional_usd = 2.0;
        second.price = Some(200.0);

        let response = ExchangeResponseStatus::Ok(ExchangeResponse {
            response_type: "order".to_string(),
            data: Some(ExchangeDataStatuses {
                statuses: vec![
                    ExchangeDataStatus::WaitingForTrigger,
                    ExchangeDataStatus::Resting(RestingOrder { oid: 77 }),
                ],
            }),
        });

        let reports =
            response_to_worker_reports(vec![first, second], &response).expect("bulk mapping");
        assert_eq!(reports.len(), 2);

        match &reports[0] {
            WorkerReport::Submitted(report) => {
                assert_eq!(
                    report.exchange_status.as_deref(),
                    Some("waiting_for_trigger")
                );
                assert!(report.oid.is_none());
            }
            _ => panic!("expected submitted report"),
        }
        match &reports[1] {
            WorkerReport::Submitted(report) => {
                assert_eq!(report.exchange_status.as_deref(), Some("resting"));
                assert_eq!(report.oid, Some(77));
            }
            _ => panic!("expected submitted report"),
        }
    }
}
