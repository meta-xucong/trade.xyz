use std::collections::HashSet;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{
    domain::{CoordinatorSignal, ExecutionMode, OrderSide, SignalOrder, SignalSource, now_ms},
    strategy::{MarketPriceEvent, Strategy, StrategyContext, StrategyEvent},
};

fn default_true() -> bool {
    true
}

fn default_fib_direction() -> FibTradeDirection {
    FibTradeDirection::Long
}

fn default_fib_cooldown_secs() -> u64 {
    300
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FibTradeDirection {
    Long,
    Short,
}

impl FibTradeDirection {
    pub fn from_raw(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "long" | "buy" => Ok(Self::Long),
            "short" | "sell" => Ok(Self::Short),
            other => anyhow::bail!("unsupported fib trade direction: {other}"),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Long => "long",
            Self::Short => "short",
        }
    }
}

#[derive(Debug, Clone)]
pub struct FibRetracementConfig {
    pub strategy_id: String,
    pub direction: FibTradeDirection,
    pub coin: String,
    pub timeframe: String,
    pub swing_high: f64,
    pub swing_low: f64,
    pub levels: Vec<f64>,
    pub entry_tolerance_usd: f64,
    pub take_profit_usd: f64,
    pub stop_loss_pct: f64,
    pub notional_usd: f64,
    pub execution_mode: ExecutionMode,
    pub max_slippage_bps: f64,
}

#[derive(Debug, Clone)]
pub struct FibLevelPlan {
    pub level: f64,
    pub entry_price: f64,
    pub take_profit_price: f64,
    pub stop_loss_price: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FibProfitLossMode {
    PriceDeltaUsd,
    PrincipalPercent,
}

impl FibProfitLossMode {
    pub fn from_raw(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "price_delta_usd" | "usd" | "absolute" => Ok(Self::PriceDeltaUsd),
            "principal_percent" | "percent" | "pct" => Ok(Self::PrincipalPercent),
            other => anyhow::bail!("unsupported fib profit/loss mode: {other}"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FibInstanceStatus {
    Draft,
    ArmedUnfilled,
    EntryPending,
    EntryFilled,
    ProtectionPending,
    Protected,
    Exiting,
    Completed,
    Paused,
    Killed,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FibBasicConfig {
    pub strategy_id: String,
    #[serde(default = "default_fib_direction")]
    pub direction: FibTradeDirection,
    pub market: String,
    pub dex: String,
    pub account_ids: Vec<String>,
    pub coin: String,
    pub timeframe: String,
    pub lookback_bars: u32,
    pub swing_high: f64,
    pub swing_low: f64,
    pub current_price: f64,
    pub levels: Vec<f64>,
    pub entry_above_tolerance_usd: f64,
    pub entry_below_tolerance_usd: f64,
    pub principal_usd: f64,
    pub leverage: f64,
    pub execution_mode: ExecutionMode,
    pub take_profit_mode: FibProfitLossMode,
    pub take_profit_value: f64,
    pub stop_loss_mode: FibProfitLossMode,
    pub stop_loss_value: f64,
    pub max_slippage_bps: f64,
    pub max_entries_per_level: u32,
    #[serde(default = "default_fib_cooldown_secs")]
    pub cooldown_secs: u64,
    #[serde(default = "default_fib_cooldown_secs")]
    pub stop_loss_cooldown_secs: u64,
    #[serde(default)]
    pub stop_loss_stop_strategy: bool,
    pub locked_range: bool,
    #[serde(default = "default_true")]
    pub auto_loop: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FibBasicLevelPlan {
    pub level: f64,
    pub entry_price: f64,
    pub entry_zone_high: f64,
    pub entry_zone_low: f64,
    pub take_profit_price: f64,
    pub stop_loss_price: f64,
    pub take_profit_return_pct: f64,
    pub stop_loss_return_pct: f64,
    pub current_distance_usd: f64,
    pub current_within_zone: bool,
    pub order_notional_usd: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FibBasicPlan {
    pub strategy_id: String,
    #[serde(default = "default_fib_direction")]
    pub direction: FibTradeDirection,
    pub market: String,
    pub coin: String,
    pub timeframe: String,
    pub swing_high: f64,
    pub swing_low: f64,
    pub current_price: f64,
    pub line_version: String,
    pub levels: Vec<FibBasicLevelPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FibInstanceRecord {
    pub strategy_id: String,
    pub status: FibInstanceStatus,
    pub config: FibBasicConfig,
    pub plan: FibBasicPlan,
    #[serde(default = "default_true")]
    pub dry_run: bool,
    #[serde(default)]
    pub live: bool,
    pub entry_signal_ids: Vec<String>,
    #[serde(default)]
    pub entry_order_refs: Vec<FibOrderRef>,
    #[serde(default)]
    pub protective_order_refs: Vec<FibOrderRef>,
    pub last_message: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    #[serde(default)]
    pub completed_cycles: u64,
    #[serde(default)]
    pub last_cycle_completed_at_ms: Option<u64>,
    #[serde(default)]
    pub last_cycle_exit_kind: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FibOrderRef {
    pub account_id: String,
    pub coin: String,
    pub cloid: String,
    #[serde(default)]
    pub oid: Option<u64>,
    pub level: Option<f64>,
    #[serde(default)]
    pub role: Option<String>,
    pub dry_run: bool,
    pub submitted_at_ms: u64,
}

impl FibBasicConfig {
    pub fn validate_plan(&self) -> Result<()> {
        anyhow::ensure!(
            !self.strategy_id.trim().is_empty(),
            "strategy_id is required"
        );
        anyhow::ensure!(!self.market.trim().is_empty(), "market is required");
        anyhow::ensure!(!self.coin.trim().is_empty(), "coin is required");
        anyhow::ensure!(
            self.lookback_bars >= 20 && self.lookback_bars <= 5000,
            "lookback_bars must be between 20 and 5000"
        );
        anyhow::ensure!(
            self.swing_high.is_finite()
                && self.swing_low.is_finite()
                && self.swing_high > self.swing_low,
            "swing_high must be greater than swing_low"
        );
        anyhow::ensure!(
            self.current_price.is_finite() && self.current_price > 0.0,
            "current_price must be positive"
        );
        anyhow::ensure!(
            !self.levels.is_empty(),
            "at least one fib level is required"
        );
        for level in &self.levels {
            anyhow::ensure!(
                level.is_finite() && (0.0..1.0).contains(level),
                "fib level must be within (0, 1): {level}"
            );
        }
        anyhow::ensure!(
            self.entry_above_tolerance_usd >= 0.0 && self.entry_below_tolerance_usd >= 0.0,
            "entry tolerances must be >= 0"
        );
        anyhow::ensure!(
            self.principal_usd.is_finite() && self.principal_usd > 0.0,
            "principal_usd must be positive"
        );
        anyhow::ensure!(
            self.leverage.is_finite() && self.leverage >= 1.0,
            "leverage must be at least 1"
        );
        anyhow::ensure!(
            self.take_profit_value.is_finite() && self.take_profit_value > 0.0,
            "take_profit_value must be positive"
        );
        anyhow::ensure!(
            self.stop_loss_value.is_finite() && self.stop_loss_value > 0.0,
            "stop_loss_value must be positive"
        );
        anyhow::ensure!(
            self.max_slippage_bps.is_finite() && self.max_slippage_bps >= 0.0,
            "max_slippage_bps must be >= 0"
        );
        anyhow::ensure!(
            self.max_entries_per_level > 0,
            "max_entries_per_level must be positive"
        );
        anyhow::ensure!(
            self.cooldown_secs <= 86_400,
            "cooldown_secs must be at most 86400 seconds"
        );
        anyhow::ensure!(
            self.stop_loss_cooldown_secs <= 86_400,
            "stop_loss_cooldown_secs must be at most 86400 seconds"
        );
        Ok(())
    }

    pub fn validate_execution(&self) -> Result<()> {
        self.validate_plan()?;
        anyhow::ensure!(
            !self.account_ids.is_empty(),
            "at least one account is required"
        );
        Ok(())
    }

    pub fn notional_usd(&self) -> f64 {
        self.principal_usd * self.leverage
    }
}

pub fn build_basic_plan(config: &FibBasicConfig) -> Result<FibBasicPlan> {
    config.validate_plan()?;
    let levels = normalized_level_set(&config.levels)?;
    let per_level_notional = config.notional_usd() / levels.len() as f64;
    let line_version = fib_line_version(
        &config.coin,
        &config.timeframe,
        config.direction,
        config.swing_low,
        config.swing_high,
    );
    let levels = levels
        .into_iter()
        .map(|level| {
            let entry_price =
                fib_entry_price(config.direction, config.swing_high, config.swing_low, level);
            let entry_zone_high = entry_price + config.entry_above_tolerance_usd;
            let entry_zone_low = (entry_price - config.entry_below_tolerance_usd).max(0.0);
            let take_profit_price = fib_take_profit_price(
                config.direction,
                entry_price,
                config.leverage,
                config.take_profit_mode,
                config.take_profit_value,
            )?;
            let stop_loss_price = fib_stop_loss_price(
                config.direction,
                entry_price,
                config.leverage,
                config.stop_loss_mode,
                config.stop_loss_value,
            )?;
            let take_profit_return_pct = leveraged_return_pct(
                config.direction,
                entry_price,
                take_profit_price,
                config.leverage,
            );
            let stop_loss_return_pct = leveraged_return_pct(
                config.direction,
                entry_price,
                stop_loss_price,
                config.leverage,
            )
            .abs();
            let current_distance_usd = (config.current_price - entry_price).abs();
            let current_within_zone =
                config.current_price >= entry_zone_low && config.current_price <= entry_zone_high;
            Ok(FibBasicLevelPlan {
                level,
                entry_price,
                entry_zone_high,
                entry_zone_low,
                take_profit_price,
                stop_loss_price,
                take_profit_return_pct,
                stop_loss_return_pct,
                current_distance_usd,
                current_within_zone,
                order_notional_usd: per_level_notional,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(FibBasicPlan {
        strategy_id: config.strategy_id.clone(),
        direction: config.direction,
        market: config.market.clone(),
        coin: config.coin.clone(),
        timeframe: config.timeframe.clone(),
        swing_high: config.swing_high,
        swing_low: config.swing_low,
        current_price: config.current_price,
        line_version,
        levels,
    })
}

pub fn normalized_level_set(levels: &[f64]) -> Result<Vec<f64>> {
    let source = if levels.is_empty() {
        vec![0.382, 0.618]
    } else {
        levels.to_vec()
    };
    let mut normalized = Vec::new();
    for level in source {
        anyhow::ensure!(
            level.is_finite() && (0.0..1.0).contains(&level),
            "fib level must be finite and within (0, 1): {level}"
        );
        if !normalized
            .iter()
            .any(|existing: &f64| (*existing - level).abs() < 1e-9)
        {
            normalized.push(level);
        }
    }
    normalized.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    Ok(normalized)
}

pub fn fib_entry_price(
    direction: FibTradeDirection,
    swing_high: f64,
    swing_low: f64,
    level: f64,
) -> f64 {
    match direction {
        FibTradeDirection::Long => retracement_price(swing_high, swing_low, level),
        FibTradeDirection::Short => swing_low + (swing_high - swing_low) * level,
    }
}

pub fn fib_entry_side(direction: FibTradeDirection) -> OrderSide {
    match direction {
        FibTradeDirection::Long => OrderSide::Buy,
        FibTradeDirection::Short => OrderSide::Sell,
    }
}

pub fn fib_exit_side(direction: FibTradeDirection) -> OrderSide {
    match direction {
        FibTradeDirection::Long => OrderSide::Sell,
        FibTradeDirection::Short => OrderSide::Buy,
    }
}

pub fn fib_take_profit_price(
    direction: FibTradeDirection,
    entry_price: f64,
    leverage: f64,
    mode: FibProfitLossMode,
    value: f64,
) -> Result<f64> {
    anyhow::ensure!(entry_price > 0.0, "entry_price must be positive");
    anyhow::ensure!(leverage >= 1.0, "leverage must be at least 1");
    anyhow::ensure!(value > 0.0, "take-profit value must be positive");
    let price = match mode {
        FibProfitLossMode::PriceDeltaUsd => match direction {
            FibTradeDirection::Long => entry_price + value,
            FibTradeDirection::Short => (entry_price - value).max(0.0),
        },
        FibProfitLossMode::PrincipalPercent => match direction {
            FibTradeDirection::Long => entry_price * (1.0 + (value / 100.0) / leverage),
            FibTradeDirection::Short => entry_price * (1.0 - (value / 100.0) / leverage),
        },
    };
    anyhow::ensure!(price > 0.0, "take-profit price must remain positive");
    Ok(price)
}

pub fn fib_stop_loss_price(
    direction: FibTradeDirection,
    entry_price: f64,
    leverage: f64,
    mode: FibProfitLossMode,
    value: f64,
) -> Result<f64> {
    anyhow::ensure!(entry_price > 0.0, "entry_price must be positive");
    anyhow::ensure!(leverage >= 1.0, "leverage must be at least 1");
    anyhow::ensure!(value > 0.0, "stop-loss value must be positive");
    let stop = match mode {
        FibProfitLossMode::PriceDeltaUsd => match direction {
            FibTradeDirection::Long => entry_price - value,
            FibTradeDirection::Short => entry_price + value,
        },
        FibProfitLossMode::PrincipalPercent => match direction {
            FibTradeDirection::Long => entry_price * (1.0 - (value / 100.0) / leverage),
            FibTradeDirection::Short => entry_price * (1.0 + (value / 100.0) / leverage),
        },
    };
    anyhow::ensure!(stop > 0.0, "stop-loss price must remain positive");
    Ok(stop)
}

pub fn take_profit_price_for_long(
    entry_price: f64,
    leverage: f64,
    mode: FibProfitLossMode,
    value: f64,
) -> Result<f64> {
    fib_take_profit_price(FibTradeDirection::Long, entry_price, leverage, mode, value)
}

pub fn stop_loss_price_for_long(
    entry_price: f64,
    leverage: f64,
    mode: FibProfitLossMode,
    value: f64,
) -> Result<f64> {
    fib_stop_loss_price(FibTradeDirection::Long, entry_price, leverage, mode, value)
}

pub fn leveraged_return_pct(
    direction: FibTradeDirection,
    entry_price: f64,
    exit_price: f64,
    leverage: f64,
) -> f64 {
    if entry_price <= 0.0 || leverage <= 0.0 {
        return 0.0;
    }
    let raw = (exit_price - entry_price) / entry_price;
    match direction {
        FibTradeDirection::Long => raw * leverage * 100.0,
        FibTradeDirection::Short => -raw * leverage * 100.0,
    }
}

pub fn leveraged_return_pct_for_long(entry_price: f64, exit_price: f64, leverage: f64) -> f64 {
    leveraged_return_pct(FibTradeDirection::Long, entry_price, exit_price, leverage)
}

pub fn fib_line_version(
    coin: &str,
    timeframe: &str,
    direction: FibTradeDirection,
    swing_low: f64,
    swing_high: f64,
) -> String {
    let seed = format!(
        "{coin}:{timeframe}:{}:{swing_low:.8}:{swing_high:.8}",
        direction.as_str()
    );
    uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, seed.as_bytes()).to_string()
}

#[derive(Debug, Clone)]
pub struct FibRetracementStrategy {
    config: FibRetracementConfig,
    emitted_level_keys: HashSet<String>,
}

impl FibRetracementStrategy {
    pub fn new(config: FibRetracementConfig) -> Self {
        Self {
            config,
            emitted_level_keys: HashSet::new(),
        }
    }

    pub fn level_plan(&self) -> Vec<FibLevelPlan> {
        self.config
            .levels
            .iter()
            .copied()
            .map(|level| {
                let entry_price = fib_entry_price(
                    self.config.direction,
                    self.config.swing_high,
                    self.config.swing_low,
                    level,
                );
                FibLevelPlan {
                    level,
                    entry_price,
                    take_profit_price: fib_take_profit_price(
                        self.config.direction,
                        entry_price,
                        1.0,
                        FibProfitLossMode::PriceDeltaUsd,
                        self.config.take_profit_usd,
                    )
                    .unwrap_or(entry_price),
                    stop_loss_price: fib_stop_loss_price(
                        self.config.direction,
                        entry_price,
                        1.0,
                        FibProfitLossMode::PrincipalPercent,
                        self.config.stop_loss_pct * 100.0,
                    )
                    .unwrap_or(entry_price),
                }
            })
            .collect()
    }

    fn maybe_signal(
        &mut self,
        ctx: &StrategyContext,
        event: &MarketPriceEvent,
    ) -> Vec<CoordinatorSignal> {
        if event.coin != self.config.coin {
            return Vec::new();
        }

        let mut signals = Vec::new();
        for plan in self.level_plan() {
            if (event.price - plan.entry_price).abs() > self.config.entry_tolerance_usd {
                continue;
            }

            let level_key = format!("{}:{:.6}", self.config.strategy_id, plan.level);
            if !self.emitted_level_keys.insert(level_key.clone()) {
                continue;
            }

            let now = now_ms();
            signals.push(CoordinatorSignal {
                signal_id: format!(
                    "fib-{}-{}-{}",
                    self.config.strategy_id,
                    level_key.replace(':', "_"),
                    now
                ),
                source: SignalSource::Fib,
                created_at_ms: event.received_at_ms,
                dispatch_at_ms: now,
                expires_at_ms: now + ctx.signal_ttl_ms,
                target_accounts: ctx.target_accounts.clone(),
                dedupe_key: format!("fib:{}:{}", event.event_id, level_key),
                order: SignalOrder {
                    market: None,
                    dex: None,
                    coin: self.config.coin.clone(),
                    side: fib_entry_side(self.config.direction),
                    notional_usd: self.config.notional_usd,
                    reduce_only: false,
                    execution_mode: self.config.execution_mode,
                    max_slippage_bps: self.config.max_slippage_bps,
                    limit_price: Some(plan.entry_price),
                    apply_account_ratio: false,
                },
            });
        }

        signals
    }
}

impl Strategy for FibRetracementStrategy {
    fn id(&self) -> &str {
        &self.config.strategy_id
    }

    fn on_event(&mut self, ctx: &StrategyContext, event: StrategyEvent) -> Vec<CoordinatorSignal> {
        match event {
            StrategyEvent::MarketPrice(market_event) => self.maybe_signal(ctx, &market_event),
            StrategyEvent::LeaderFill(_) => Vec::new(),
        }
    }
}

pub fn retracement_price(swing_high: f64, swing_low: f64, level: f64) -> f64 {
    swing_high - (swing_high - swing_low) * level
}

#[cfg(test)]
mod tests {
    use crate::{
        domain::{ExecutionMode, now_ms},
        strategy::{MarketPriceEvent, Strategy, StrategyContext, StrategyEvent},
    };

    use super::{
        FibRetracementConfig, FibRetracementStrategy, FibTradeDirection, fib_entry_price,
        fib_stop_loss_price, fib_take_profit_price, retracement_price,
    };

    #[test]
    fn computes_long_retracement_levels() {
        assert!((retracement_price(100.0, 80.0, 0.382) - 92.36).abs() < 0.0001);
        assert!((retracement_price(100.0, 80.0, 0.618) - 87.64).abs() < 0.0001);
    }

    #[test]
    fn computes_short_retracement_levels() {
        assert!(
            (fib_entry_price(FibTradeDirection::Short, 100.0, 80.0, 0.382) - 87.64).abs() < 0.0001
        );
        assert!(
            (fib_entry_price(FibTradeDirection::Short, 100.0, 80.0, 0.618) - 92.36).abs() < 0.0001
        );
    }

    #[test]
    fn converts_principal_percent_by_leverage() {
        let tp = super::take_profit_price_for_long(
            100.0,
            5.0,
            super::FibProfitLossMode::PrincipalPercent,
            10.0,
        )
        .expect("tp");
        let sl = super::stop_loss_price_for_long(
            100.0,
            5.0,
            super::FibProfitLossMode::PrincipalPercent,
            10.0,
        )
        .expect("sl");
        assert!((tp - 102.0).abs() < 0.0001);
        assert!((sl - 98.0).abs() < 0.0001);
    }

    #[test]
    fn converts_short_principal_percent_by_leverage() {
        let tp = fib_take_profit_price(
            FibTradeDirection::Short,
            100.0,
            5.0,
            super::FibProfitLossMode::PrincipalPercent,
            10.0,
        )
        .expect("tp");
        let sl = fib_stop_loss_price(
            FibTradeDirection::Short,
            100.0,
            5.0,
            super::FibProfitLossMode::PrincipalPercent,
            10.0,
        )
        .expect("sl");
        assert!((tp - 98.0).abs() < 0.0001);
        assert!((sl - 102.0).abs() < 0.0001);
    }

    #[test]
    fn builds_basic_entry_zones() {
        let config = super::FibBasicConfig {
            strategy_id: "fib-basic".to_string(),
            direction: FibTradeDirection::Long,
            market: "xyz_perp".to_string(),
            dex: "xyz".to_string(),
            account_ids: vec!["addr_a".to_string()],
            coin: "xyz:NVDA".to_string(),
            timeframe: "1h".to_string(),
            lookback_bars: 120,
            swing_high: 100.0,
            swing_low: 80.0,
            current_price: 92.4,
            levels: vec![0.382, 0.618],
            entry_above_tolerance_usd: 0.5,
            entry_below_tolerance_usd: 0.25,
            principal_usd: 11.0,
            leverage: 2.0,
            execution_mode: ExecutionMode::Maker,
            take_profit_mode: super::FibProfitLossMode::PriceDeltaUsd,
            take_profit_value: 2.0,
            stop_loss_mode: super::FibProfitLossMode::PrincipalPercent,
            stop_loss_value: 4.0,
            max_slippage_bps: 20.0,
            max_entries_per_level: 1,
            cooldown_secs: 300,
            stop_loss_cooldown_secs: 900,
            stop_loss_stop_strategy: false,
            locked_range: false,
            auto_loop: true,
        };
        let plan = super::build_basic_plan(&config).expect("basic plan");
        assert_eq!(plan.levels.len(), 2);
        assert!(plan.levels[0].current_within_zone);
        assert!((plan.levels[0].order_notional_usd - 11.0).abs() < 0.0001);
    }

    #[test]
    fn emits_once_when_price_hits_level() {
        let mut strategy = FibRetracementStrategy::new(FibRetracementConfig {
            strategy_id: "fib_xyz_1h".to_string(),
            direction: FibTradeDirection::Long,
            coin: "xyz:XYZ100".to_string(),
            timeframe: "1h".to_string(),
            swing_high: 100.0,
            swing_low: 80.0,
            levels: vec![0.382, 0.618],
            entry_tolerance_usd: 0.1,
            take_profit_usd: 2.0,
            stop_loss_pct: 0.03,
            notional_usd: 20.0,
            execution_mode: ExecutionMode::Taker,
            max_slippage_bps: 20.0,
        });
        let ctx = StrategyContext {
            target_accounts: vec!["addr_a".to_string()],
            signal_ttl_ms: 3000,
        };
        let event = StrategyEvent::MarketPrice(MarketPriceEvent {
            event_id: "price-1".to_string(),
            coin: "xyz:XYZ100".to_string(),
            price: 92.30,
            received_at_ms: now_ms(),
        });

        let first = strategy.on_event(&ctx, event.clone());
        let second = strategy.on_event(&ctx, event);

        assert_eq!(first.len(), 1);
        assert!(second.is_empty());
        assert_eq!(strategy.id(), "fib_xyz_1h");
    }
}
