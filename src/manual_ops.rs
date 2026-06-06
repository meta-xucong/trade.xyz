use anyhow::{Context, Result};

use crate::{
    config::AppConfig,
    domain::{
        CoordinatorSignal, ExecutionMode, OrderSide, SignalOrder, SignalSource, TimestampMs, now_ms,
    },
};

#[derive(Debug, Clone)]
pub struct ManualOrderRequest {
    pub request_id: String,
    pub operator: String,
    pub source_module: String,
    pub target_accounts: Vec<String>,
    pub coin: String,
    pub side: OrderSide,
    pub notional_usd: f64,
    pub reduce_only: bool,
    pub execution_mode: ExecutionMode,
    pub max_slippage_bps: f64,
    pub dry_run_expected: bool,
    pub requested_at_ms: TimestampMs,
    pub client_note: Option<String>,
}

pub fn manual_order_to_signal(
    config: &AppConfig,
    request: ManualOrderRequest,
) -> Result<CoordinatorSignal> {
    let module = module_from_request(&request)?;
    validate_manual_order(config, &request, module)?;
    let now = now_ms();
    let note = request
        .client_note
        .as_deref()
        .map(str::trim)
        .filter(|note| !note.is_empty())
        .unwrap_or("manual");

    Ok(CoordinatorSignal {
        signal_id: format!("manual-{}-{now}", request.request_id),
        source: signal_source_from_module(module),
        created_at_ms: request.requested_at_ms,
        dispatch_at_ms: now,
        expires_at_ms: now + config.process.signal_ttl_ms,
        target_accounts: request.target_accounts,
        dedupe_key: format!(
            "{}:{}:{}:{note}",
            module, request.operator, request.request_id
        ),
        order: SignalOrder {
            market: None,
            dex: None,
            coin: request.coin,
            side: request.side,
            notional_usd: request.notional_usd,
            reduce_only: request.reduce_only,
            execution_mode: request.execution_mode,
            max_slippage_bps: request.max_slippage_bps,
            limit_price: None,
            apply_account_ratio: false,
        },
    })
}

fn validate_manual_order(
    config: &AppConfig,
    request: &ManualOrderRequest,
    module: &str,
) -> Result<()> {
    anyhow::ensure!(
        config.manual_ops.enabled && config.manual_ops.manual_trading_enabled,
        "manual trading is disabled"
    );
    anyhow::ensure!(
        request.dry_run_expected || config.manual_ops.manual_live_enabled,
        "manual live trading is disabled"
    );
    anyhow::ensure!(
        !config.risk.global.kill_switch
            || (request.reduce_only && config.risk.global.allow_reduce_only_when_killed),
        "global kill switch is active; only reduce-only manual orders are allowed"
    );
    anyhow::ensure!(
        !request.operator.trim().is_empty(),
        "manual operator cannot be empty"
    );
    anyhow::ensure!(
        !request.target_accounts.is_empty(),
        "manual request must target at least one account"
    );
    anyhow::ensure!(
        request.target_accounts.len() <= config.manual_ops.max_manual_batch_accounts,
        "manual request targets more accounts than max_manual_batch_accounts"
    );
    anyhow::ensure!(
        request.notional_usd > 0.0,
        "manual request notional must be positive"
    );
    anyhow::ensure!(
        request.notional_usd <= config.manual_ops.max_manual_order_notional_usd,
        "manual request notional exceeds max_manual_order_notional_usd"
    );

    anyhow::ensure!(
        config.symbol_allowed_for_module(module, &request.coin),
        "{module} symbol {} is blocked",
        request.coin
    );

    for account_id in &request.target_accounts {
        let account = config
            .account(account_id)
            .with_context(|| format!("manual target account {account_id} is not configured"))?;
        anyhow::ensure!(
            account.enabled && account.worker_enabled,
            "manual target account {} is disabled",
            account.account_id
        );
    }

    Ok(())
}

fn module_from_request(request: &ManualOrderRequest) -> Result<&'static str> {
    let explicit = request.source_module.trim();
    if explicit.is_empty() {
        return Ok(module_from_operator(&request.operator));
    }
    match explicit.to_ascii_lowercase().as_str() {
        "manual" | "manual_ops" => Ok("manual"),
        "fib" | "fib_retracement" => Ok("fib"),
        "copy" | "smart_money" | "smart_money_copy" => Ok("copy"),
        _ => anyhow::bail!("unknown source_module: {}", request.source_module),
    }
}

fn signal_source_from_module(module: &str) -> SignalSource {
    match module {
        "fib" => SignalSource::Fib,
        "copy" => SignalSource::SmartMoney,
        _ => SignalSource::Manual,
    }
}

fn module_from_operator(operator: &str) -> &'static str {
    let op = operator.trim().to_ascii_lowercase();
    if op.starts_with("fib") {
        "fib"
    } else if op.starts_with("copy") {
        "copy"
    } else {
        "manual"
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        config::{AccountConfig, AppConfig},
        domain::{ExecutionMode, OrderSide, SignalSource, now_ms},
    };

    use super::{ManualOrderRequest, manual_order_to_signal};

    #[test]
    fn manual_batch_request_becomes_single_broadcast_signal() {
        let mut config = AppConfig {
            accounts: vec![
                AccountConfig {
                    account_id: "addr_a".to_string(),
                    address: "0x1".to_string(),
                    secret_id: "addr_a_api_wallet".to_string(),
                    api_wallet_env: String::new(),
                    enabled: true,
                    worker_enabled: true,
                    copy_ratio: 1.0,
                    max_order_notional_usd: 100.0,
                    blocked_markets: Vec::new(),
                },
                AccountConfig {
                    account_id: "addr_b".to_string(),
                    address: "0x2".to_string(),
                    secret_id: "addr_b_api_wallet".to_string(),
                    api_wallet_env: String::new(),
                    enabled: true,
                    worker_enabled: true,
                    copy_ratio: 1.0,
                    max_order_notional_usd: 100.0,
                    blocked_markets: Vec::new(),
                },
            ],
            ..AppConfig::default()
        };
        config.manual_ops.blocked_symbols = vec!["xyz:TSLA".to_string()];

        let signal = manual_order_to_signal(
            &config,
            ManualOrderRequest {
                request_id: "req-1".to_string(),
                operator: "tester".to_string(),
                source_module: "manual".to_string(),
                target_accounts: vec!["addr_a".to_string(), "addr_b".to_string()],
                coin: "xyz:XYZ100".to_string(),
                side: OrderSide::Buy,
                notional_usd: 10.0,
                reduce_only: false,
                execution_mode: ExecutionMode::Taker,
                max_slippage_bps: 15.0,
                dry_run_expected: true,
                requested_at_ms: now_ms(),
                client_note: None,
            },
        )
        .expect("manual signal should be valid");

        assert_eq!(signal.target_accounts, vec!["addr_a", "addr_b"]);
        assert_eq!(signal.order.coin, "xyz:XYZ100");
    }

    #[test]
    fn manual_order_respects_global_kill_switch() {
        let mut config = AppConfig {
            accounts: vec![AccountConfig {
                account_id: "addr_a".to_string(),
                address: "0x1".to_string(),
                secret_id: "addr_a_api_wallet".to_string(),
                api_wallet_env: String::new(),
                enabled: true,
                worker_enabled: true,
                copy_ratio: 1.0,
                max_order_notional_usd: 100.0,
                blocked_markets: Vec::new(),
            }],
            ..AppConfig::default()
        };
        config.risk.global.kill_switch = true;

        let error = manual_order_to_signal(
            &config,
            ManualOrderRequest {
                request_id: "req-kill".to_string(),
                operator: "tester".to_string(),
                source_module: "manual".to_string(),
                target_accounts: vec!["addr_a".to_string()],
                coin: "xyz:XYZ100".to_string(),
                side: OrderSide::Buy,
                notional_usd: 10.0,
                reduce_only: false,
                execution_mode: ExecutionMode::Taker,
                max_slippage_bps: 15.0,
                dry_run_expected: true,
                requested_at_ms: now_ms(),
                client_note: None,
            },
        )
        .expect_err("kill switch blocks new manual orders")
        .to_string();

        assert!(error.contains("kill switch"));
    }

    #[test]
    fn manual_order_honors_explicit_source_module() {
        let mut config = AppConfig {
            accounts: vec![AccountConfig {
                account_id: "addr_a".to_string(),
                address: "0x1".to_string(),
                secret_id: "addr_a_api_wallet".to_string(),
                api_wallet_env: String::new(),
                enabled: true,
                worker_enabled: true,
                copy_ratio: 1.0,
                max_order_notional_usd: 100.0,
                blocked_markets: Vec::new(),
            }],
            ..AppConfig::default()
        };
        config.module_symbol_policies.manual_blocked_symbols = vec!["xyz:TSLA".to_string()];
        config.module_symbol_policies.fib_blocked_symbols = vec!["xyz:TSLA".to_string()];

        let signal = manual_order_to_signal(
            &config,
            ManualOrderRequest {
                request_id: "req-fib".to_string(),
                operator: "local".to_string(),
                source_module: "fib".to_string(),
                target_accounts: vec!["addr_a".to_string()],
                coin: "xyz:NVDA".to_string(),
                side: OrderSide::Buy,
                notional_usd: 10.0,
                reduce_only: false,
                execution_mode: ExecutionMode::Taker,
                max_slippage_bps: 15.0,
                dry_run_expected: true,
                requested_at_ms: now_ms(),
                client_note: None,
            },
        )
        .expect("fib-scoped manual request should pass fib blacklist checks");

        assert!(matches!(signal.source, SignalSource::Fib));
    }
}
