use crate::{
    config::{AccountConfig, AppConfig},
    domain::{
        ApprovedOrder, ExecutionMode, ExecutionPolicy, PricePolicy, RejectedIntent, TradeIntent,
        now_ms,
    },
};

#[derive(Debug, Clone)]
pub struct RiskContext {
    pub account_id: String,
    pub dry_run: bool,
    pub environment: String,
    pub live_execution_enabled: bool,
    pub mainnet_live_enabled: bool,
    pub kill_switch: bool,
    pub allow_reduce_only_when_killed: bool,
    pub max_order_notional_usd: f64,
    pub blocked_symbols: Vec<String>,
    pub now_ms: u64,
}

#[derive(Debug, Clone)]
pub struct RiskCheckResult {
    pub passed: bool,
    pub reason_code: String,
    pub message: String,
}

impl RiskCheckResult {
    pub fn pass() -> Self {
        Self {
            passed: true,
            reason_code: "OK".to_string(),
            message: "approved".to_string(),
        }
    }

    pub fn reject(reason_code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            passed: false,
            reason_code: reason_code.into(),
            message: message.into(),
        }
    }
}

pub trait RiskCheck: Send + Sync {
    fn check(&self, ctx: &RiskContext, intent: &TradeIntent) -> RiskCheckResult;
}

#[derive(Debug, Clone)]
pub enum RiskDecision {
    Approved(ApprovedOrder),
    Rejected(RejectedIntent),
}

pub struct RiskGateway {
    checks: Vec<Box<dyn RiskCheck>>,
}

impl RiskGateway {
    pub fn dry_run_default() -> Self {
        Self {
            checks: vec![
                Box::new(TargetAccountRisk),
                Box::new(SignalExpiryRisk),
                Box::new(KillSwitchRisk),
                Box::new(LiveExecutionGuardRisk),
                Box::new(PositiveNotionalRisk),
                Box::new(AccountNotionalRisk),
                Box::new(AllowedSymbolRisk),
            ],
        }
    }

    pub fn evaluate(&self, ctx: &RiskContext, intent: TradeIntent) -> RiskDecision {
        for check in &self.checks {
            let result = check.check(ctx, &intent);
            if !result.passed {
                return RiskDecision::Rejected(reject_from_intent(
                    &intent,
                    result.reason_code,
                    result.message,
                ));
            }
        }

        RiskDecision::Approved(approved_from_intent(intent))
    }
}

impl RiskContext {
    pub fn from_account(config: &AppConfig, account: &AccountConfig, dry_run: bool) -> Self {
        Self::from_account_for_module(config, account, dry_run, "manual")
    }

    pub fn from_account_for_module(
        config: &AppConfig,
        account: &AccountConfig,
        dry_run: bool,
        module: &str,
    ) -> Self {
        Self {
            account_id: account.account_id.clone(),
            dry_run,
            environment: config.app.environment.clone(),
            live_execution_enabled: config.manual_ops.manual_live_enabled,
            mainnet_live_enabled: config.manual_ops.mainnet_live_enabled,
            kill_switch: config.risk.global.kill_switch,
            allow_reduce_only_when_killed: config.risk.global.allow_reduce_only_when_killed,
            max_order_notional_usd: account.max_order_notional_usd,
            blocked_symbols: config.module_blocked_symbols(module).to_vec(),
            now_ms: now_ms(),
        }
    }
}

struct TargetAccountRisk;

impl RiskCheck for TargetAccountRisk {
    fn check(&self, ctx: &RiskContext, intent: &TradeIntent) -> RiskCheckResult {
        if intent.account_id == ctx.account_id && intent.target_accounts.contains(&ctx.account_id) {
            RiskCheckResult::pass()
        } else {
            RiskCheckResult::reject(
                "ACCOUNT_NOT_TARGETED",
                format!(
                    "intent account {} is not targeted for worker account {}",
                    intent.account_id, ctx.account_id
                ),
            )
        }
    }
}

struct SignalExpiryRisk;

impl RiskCheck for SignalExpiryRisk {
    fn check(&self, ctx: &RiskContext, intent: &TradeIntent) -> RiskCheckResult {
        match intent.expires_at_ms {
            Some(expires_at_ms) if ctx.now_ms > expires_at_ms => RiskCheckResult::reject(
                "SIGNAL_EXPIRED",
                format!("signal expired {}ms ago", ctx.now_ms - expires_at_ms),
            ),
            _ => RiskCheckResult::pass(),
        }
    }
}

struct LiveExecutionGuardRisk;

impl RiskCheck for LiveExecutionGuardRisk {
    fn check(&self, ctx: &RiskContext, _intent: &TradeIntent) -> RiskCheckResult {
        if ctx.dry_run {
            RiskCheckResult::pass()
        } else if !ctx.live_execution_enabled {
            RiskCheckResult::reject(
                "LIVE_EXECUTION_DISABLED",
                "live execution is disabled by manual_ops.manual_live_enabled=false",
            )
        } else if ctx.environment == "mainnet" && !ctx.mainnet_live_enabled {
            RiskCheckResult::reject(
                "MAINNET_LIVE_DISABLED",
                "mainnet live execution requires manual_ops.mainnet_live_enabled=true",
            )
        } else {
            RiskCheckResult::pass()
        }
    }
}

struct KillSwitchRisk;

impl RiskCheck for KillSwitchRisk {
    fn check(&self, ctx: &RiskContext, intent: &TradeIntent) -> RiskCheckResult {
        if !ctx.kill_switch {
            return RiskCheckResult::pass();
        }
        if intent.reduce_only && ctx.allow_reduce_only_when_killed {
            return RiskCheckResult::pass();
        }
        RiskCheckResult::reject(
            "KILL_SWITCH_ACTIVE",
            "global kill switch is active; new or non-reduce-only orders are blocked",
        )
    }
}

struct PositiveNotionalRisk;

impl RiskCheck for PositiveNotionalRisk {
    fn check(&self, _ctx: &RiskContext, intent: &TradeIntent) -> RiskCheckResult {
        if intent.sizing.notional_usd > 0.0 {
            RiskCheckResult::pass()
        } else {
            RiskCheckResult::reject("INVALID_NOTIONAL", "intent notional must be positive")
        }
    }
}

struct AccountNotionalRisk;

impl RiskCheck for AccountNotionalRisk {
    fn check(&self, ctx: &RiskContext, intent: &TradeIntent) -> RiskCheckResult {
        if intent.sizing.notional_usd <= ctx.max_order_notional_usd {
            RiskCheckResult::pass()
        } else {
            RiskCheckResult::reject(
                "POSITION_LIMIT_EXCEEDED",
                format!(
                    "intent notional {} exceeds account max {}",
                    intent.sizing.notional_usd, ctx.max_order_notional_usd
                ),
            )
        }
    }
}

struct AllowedSymbolRisk;

impl RiskCheck for AllowedSymbolRisk {
    fn check(&self, ctx: &RiskContext, intent: &TradeIntent) -> RiskCheckResult {
        if ctx.blocked_symbols.is_empty() || !ctx.blocked_symbols.contains(&intent.coin) {
            RiskCheckResult::pass()
        } else {
            RiskCheckResult::reject(
                "SYMBOL_BLOCKED",
                format!("symbol {} is in blocked_symbols", intent.coin),
            )
        }
    }
}

fn approved_from_intent(intent: TradeIntent) -> ApprovedOrder {
    let price = match intent.price_policy {
        PricePolicy::Limit { price } | PricePolicy::MakerOnly { price } => Some(price),
        PricePolicy::MarketWithSlippageLimit { .. } | PricePolicy::PegBestBidAsk => None,
    };
    let execution_mode = match intent.execution_policy {
        ExecutionPolicy::Maker | ExecutionPolicy::Alo | ExecutionPolicy::Gtc => {
            ExecutionMode::Maker
        }
        ExecutionPolicy::Taker | ExecutionPolicy::Ioc => ExecutionMode::Taker,
    };
    let signal_for_cloid = intent
        .signal_id
        .clone()
        .unwrap_or_else(|| format!("nosignal-{}", now_ms()));
    let cloid_seed = format!(
        "{}:{}:{signal_for_cloid}",
        intent.account_id, intent.intent_id
    );
    let cloid = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, cloid_seed.as_bytes());
    let max_slippage_bps = match intent.price_policy {
        PricePolicy::MarketWithSlippageLimit { max_slippage_bps } => max_slippage_bps,
        PricePolicy::Limit { .. } | PricePolicy::MakerOnly { .. } | PricePolicy::PegBestBidAsk => {
            0.0
        }
    };
    ApprovedOrder {
        risk_decision_id: format!("risk-{}-{}", intent.account_id, now_ms()),
        intent_id: intent.intent_id,
        signal_id: intent.signal_id,
        worker_id: intent.worker_id,
        account_id: intent.account_id.clone(),
        strategy_id: intent.strategy_id,
        market: intent.market,
        dex: intent.dex,
        coin: intent.coin,
        side: intent.side,
        notional_usd: intent.sizing.notional_usd,
        exact_size: None,
        price,
        execution_mode,
        execution_policy: intent.execution_policy,
        max_slippage_bps,
        reduce_only: intent.reduce_only,
        cloid: cloid.to_string(),
        expires_at_ms: intent.expires_at_ms,
    }
}

fn reject_from_intent(
    intent: &TradeIntent,
    reason_code: String,
    message: String,
) -> RejectedIntent {
    RejectedIntent {
        signal_id: intent.signal_id.clone().unwrap_or_default(),
        worker_id: intent.worker_id.clone(),
        account_id: intent.account_id.clone(),
        reason_code,
        message,
        rejected_at_ms: now_ms(),
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        config::{AccountConfig, AppConfig},
        domain::{CoordinatorSignal, ExecutionMode, OrderSide, SignalOrder, SignalSource, now_ms},
    };

    use super::{RiskContext, RiskDecision, RiskGateway};

    #[test]
    fn account_notional_risk_rejects_after_copy_sizing() {
        let config = AppConfig::default();
        let account = AccountConfig {
            account_id: "addr_a".to_string(),
            address: "0x1".to_string(),
            secret_id: "addr_a_api_wallet".to_string(),
            api_wallet_env: String::new(),
            enabled: true,
            worker_enabled: true,
            copy_ratio: 0.5,
            max_order_notional_usd: 10.0,
            blocked_markets: Vec::new(),
        };
        let signal = CoordinatorSignal {
            signal_id: "sig-1".to_string(),
            source: SignalSource::DryRun,
            created_at_ms: now_ms(),
            dispatch_at_ms: now_ms(),
            expires_at_ms: now_ms() + 1000,
            target_accounts: vec!["addr_a".to_string()],
            dedupe_key: "dedupe".to_string(),
            order: SignalOrder {
                market: None,
                dex: None,
                coin: "xyz:XYZ100".to_string(),
                side: OrderSide::Buy,
                notional_usd: 25.0,
                reduce_only: false,
                execution_mode: ExecutionMode::Taker,
                max_slippage_bps: 20.0,
                limit_price: None,
                apply_account_ratio: true,
            },
        };
        let intent = signal.to_trade_intent("addr_a", "worker-addr_a", account.copy_ratio);
        let ctx = RiskContext::from_account(&config, &account, true);
        let decision = RiskGateway::dry_run_default().evaluate(&ctx, intent);

        match decision {
            RiskDecision::Rejected(rejection) => {
                assert_eq!(rejection.reason_code, "POSITION_LIMIT_EXCEEDED");
            }
            RiskDecision::Approved(_) => panic!("expected risk rejection"),
        }
    }

    #[test]
    fn kill_switch_rejects_new_orders() {
        let mut config = AppConfig::default();
        config.risk.global.kill_switch = true;
        let account = AccountConfig {
            account_id: "addr_a".to_string(),
            address: "0x1".to_string(),
            secret_id: "addr_a_api_wallet".to_string(),
            api_wallet_env: String::new(),
            enabled: true,
            worker_enabled: true,
            copy_ratio: 1.0,
            max_order_notional_usd: 100.0,
            blocked_markets: Vec::new(),
        };
        let signal = CoordinatorSignal {
            signal_id: "sig-kill".to_string(),
            source: SignalSource::Manual,
            created_at_ms: now_ms(),
            dispatch_at_ms: now_ms(),
            expires_at_ms: now_ms() + 1000,
            target_accounts: vec!["addr_a".to_string()],
            dedupe_key: "dedupe-kill".to_string(),
            order: SignalOrder {
                market: None,
                dex: None,
                coin: "xyz:XYZ100".to_string(),
                side: OrderSide::Buy,
                notional_usd: 10.0,
                reduce_only: false,
                execution_mode: ExecutionMode::Taker,
                max_slippage_bps: 20.0,
                limit_price: None,
                apply_account_ratio: false,
            },
        };
        let intent = signal.to_trade_intent("addr_a", "worker-addr_a", account.copy_ratio);
        let ctx = RiskContext::from_account(&config, &account, true);
        let decision = RiskGateway::dry_run_default().evaluate(&ctx, intent);

        match decision {
            RiskDecision::Rejected(rejection) => {
                assert_eq!(rejection.reason_code, "KILL_SWITCH_ACTIVE");
            }
            RiskDecision::Approved(_) => panic!("expected kill switch rejection"),
        }
    }

    #[test]
    fn kill_switch_allows_reduce_only_when_configured() {
        let mut config = AppConfig::default();
        config.risk.global.kill_switch = true;
        config.risk.global.allow_reduce_only_when_killed = true;
        let account = AccountConfig {
            account_id: "addr_a".to_string(),
            address: "0x1".to_string(),
            secret_id: "addr_a_api_wallet".to_string(),
            api_wallet_env: String::new(),
            enabled: true,
            worker_enabled: true,
            copy_ratio: 1.0,
            max_order_notional_usd: 100.0,
            blocked_markets: Vec::new(),
        };
        let signal = CoordinatorSignal {
            signal_id: "sig-reduce".to_string(),
            source: SignalSource::Manual,
            created_at_ms: now_ms(),
            dispatch_at_ms: now_ms(),
            expires_at_ms: now_ms() + 1000,
            target_accounts: vec!["addr_a".to_string()],
            dedupe_key: "dedupe-reduce".to_string(),
            order: SignalOrder {
                market: None,
                dex: None,
                coin: "xyz:XYZ100".to_string(),
                side: OrderSide::Sell,
                notional_usd: 10.0,
                reduce_only: true,
                execution_mode: ExecutionMode::Taker,
                max_slippage_bps: 20.0,
                limit_price: None,
                apply_account_ratio: false,
            },
        };
        let intent = signal.to_trade_intent("addr_a", "worker-addr_a", account.copy_ratio);
        let ctx = RiskContext::from_account(&config, &account, true);
        let decision = RiskGateway::dry_run_default().evaluate(&ctx, intent);

        match decision {
            RiskDecision::Approved(order) => assert!(order.reduce_only),
            RiskDecision::Rejected(rejection) => {
                panic!("expected reduce-only approval, got {rejection:?}")
            }
        }
    }

    #[test]
    fn module_scoped_allowed_symbols_apply_to_risk_context() {
        let mut config = AppConfig::default();
        config.module_symbol_policies.manual_blocked_symbols = vec!["xyz:TSLA".to_string()];
        config.module_symbol_policies.fib_blocked_symbols = vec!["xyz:TSLA".to_string()];
        let account = AccountConfig {
            account_id: "addr_a".to_string(),
            address: "0x1".to_string(),
            secret_id: "addr_a_api_wallet".to_string(),
            api_wallet_env: String::new(),
            enabled: true,
            worker_enabled: true,
            copy_ratio: 1.0,
            max_order_notional_usd: 100.0,
            blocked_markets: Vec::new(),
        };
        let signal = CoordinatorSignal {
            signal_id: "sig-fib".to_string(),
            source: SignalSource::Fib,
            created_at_ms: now_ms(),
            dispatch_at_ms: now_ms(),
            expires_at_ms: now_ms() + 1000,
            target_accounts: vec!["addr_a".to_string()],
            dedupe_key: "dedupe-fib".to_string(),
            order: SignalOrder {
                market: None,
                dex: None,
                coin: "xyz:NVDA".to_string(),
                side: OrderSide::Buy,
                notional_usd: 10.0,
                reduce_only: false,
                execution_mode: ExecutionMode::Taker,
                max_slippage_bps: 20.0,
                limit_price: None,
                apply_account_ratio: false,
            },
        };
        let intent = signal.to_trade_intent("addr_a", "worker-addr_a", account.copy_ratio);
        let ctx = RiskContext::from_account_for_module(
            &config,
            &account,
            true,
            signal.source.module_scope(),
        );
        let decision = RiskGateway::dry_run_default().evaluate(&ctx, intent);
        match decision {
            RiskDecision::Approved(_) => {}
            RiskDecision::Rejected(rejection) => {
                panic!("expected fib symbol to pass fib blacklist checks, got {rejection:?}")
            }
        }
    }
}
