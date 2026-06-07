use std::collections::{HashMap, HashSet};

use crate::{
    domain::{CoordinatorSignal, ExecutionMode, SignalOrder, SignalSource, now_ms},
    strategy::{LeaderFillEvent, Strategy, StrategyContext, StrategyEvent},
};

#[derive(Debug, Clone)]
pub struct LeaderRule {
    pub leader_id: String,
    pub leader_address: String,
    pub enabled: bool,
    pub copy_ratio: f64,
}

#[derive(Debug, Clone)]
pub struct SymbolCopyLimit {
    pub coin: String,
    pub max_signal_notional_usd: f64,
}

#[derive(Debug, Clone)]
pub struct SmartMoneyCopyConfig {
    pub strategy_id: String,
    pub default_copy_ratio: f64,
    pub max_slippage_bps: f64,
    pub leaders: Vec<LeaderRule>,
    pub symbol_limits: Vec<SymbolCopyLimit>,
}

#[derive(Debug, Clone)]
pub struct SmartMoneyCopyStrategy {
    config: SmartMoneyCopyConfig,
    seen_events: HashSet<String>,
    leader_by_address: HashMap<String, LeaderRule>,
    symbol_limits: HashMap<String, SymbolCopyLimit>,
}

impl SmartMoneyCopyStrategy {
    pub fn new(config: SmartMoneyCopyConfig) -> Self {
        let leader_by_address = config
            .leaders
            .iter()
            .cloned()
            .map(|leader| (leader.leader_address.to_lowercase(), leader))
            .collect();
        let symbol_limits = config
            .symbol_limits
            .iter()
            .cloned()
            .map(|limit| (limit.coin.clone(), limit))
            .collect();
        Self {
            config,
            seen_events: HashSet::new(),
            leader_by_address,
            symbol_limits,
        }
    }

    fn handle_leader_fill(
        &mut self,
        ctx: &StrategyContext,
        fill: &LeaderFillEvent,
    ) -> Vec<CoordinatorSignal> {
        let Some(leader) = self
            .leader_by_address
            .get(&fill.leader_address.to_lowercase())
            .cloned()
        else {
            return Vec::new();
        };
        if !leader.enabled {
            return Vec::new();
        }
        if !self.seen_events.insert(fill.event_id.clone()) {
            return Vec::new();
        }

        let copy_ratio = if leader.copy_ratio > 0.0 {
            leader.copy_ratio
        } else {
            self.config.default_copy_ratio
        };
        let mut signal_notional_usd = fill.notional_usd * copy_ratio;
        if let Some(limit) = self.symbol_limits.get(&fill.coin) {
            signal_notional_usd = signal_notional_usd.min(limit.max_signal_notional_usd);
        }
        if signal_notional_usd <= 0.0 {
            return Vec::new();
        }

        let now = now_ms();
        vec![CoordinatorSignal {
            signal_id: format!("copy-{}-{}-{now}", leader.leader_id, fill.event_id),
            source: SignalSource::SmartMoney,
            created_at_ms: fill.received_at_ms,
            dispatch_at_ms: now,
            expires_at_ms: now + ctx.signal_ttl_ms,
            target_accounts: ctx.target_accounts.clone(),
            dedupe_key: dedupe_key(fill),
            order: SignalOrder {
                market: None,
                dex: None,
                coin: fill.coin.clone(),
                side: fill.side,
                notional_usd: signal_notional_usd,
                reduce_only: fill.reduce_only,
                execution_mode: ExecutionMode::Taker,
                max_slippage_bps: self.config.max_slippage_bps,
                limit_price: None,
                apply_account_ratio: true,
            },
        }]
    }
}

impl Strategy for SmartMoneyCopyStrategy {
    fn id(&self) -> &str {
        &self.config.strategy_id
    }

    fn on_event(&mut self, ctx: &StrategyContext, event: StrategyEvent) -> Vec<CoordinatorSignal> {
        match event {
            StrategyEvent::LeaderFill(fill) => self.handle_leader_fill(ctx, &fill),
            StrategyEvent::MarketPrice(_) => Vec::new(),
        }
    }
}

fn dedupe_key(fill: &LeaderFillEvent) -> String {
    format!(
        "leader:{}:{}:{}:{:.8}:{:.8}:{}",
        fill.leader_id, fill.event_id, fill.coin, fill.price, fill.size, fill.exchange_time_ms
    )
}

#[cfg(test)]
mod tests {
    use crate::{
        domain::{OrderSide, now_ms},
        strategy::{LeaderFillEvent, Strategy, StrategyContext, StrategyEvent},
    };

    use super::{LeaderRule, SmartMoneyCopyConfig, SmartMoneyCopyStrategy, SymbolCopyLimit};

    #[test]
    fn leader_fill_emits_once_and_caps_notional() {
        let mut strategy = SmartMoneyCopyStrategy::new(SmartMoneyCopyConfig {
            strategy_id: "copy_main".to_string(),
            default_copy_ratio: 0.1,
            max_slippage_bps: 25.0,
            leaders: vec![LeaderRule {
                leader_id: "leader_a".to_string(),
                leader_address: "0xabc".to_string(),
                enabled: true,
                copy_ratio: 0.5,
            }],
            symbol_limits: vec![SymbolCopyLimit {
                coin: "xyz:XYZ100".to_string(),
                max_signal_notional_usd: 20.0,
            }],
        });
        let ctx = StrategyContext {
            target_accounts: vec!["addr_a".to_string(), "addr_b".to_string()],
            signal_ttl_ms: 3000,
        };
        let event = StrategyEvent::LeaderFill(LeaderFillEvent {
            event_id: "fill-1".to_string(),
            leader_id: "leader_a".to_string(),
            leader_address: "0xABC".to_string(),
            coin: "xyz:XYZ100".to_string(),
            side: OrderSide::Buy,
            price: 10.0,
            size: 10.0,
            notional_usd: 100.0,
            reduce_only: false,
            exchange_time_ms: now_ms(),
            received_at_ms: now_ms(),
        });

        let first = strategy.on_event(&ctx, event.clone());
        let second = strategy.on_event(&ctx, event);

        assert_eq!(first.len(), 1);
        assert!(second.is_empty());
        assert_eq!(first[0].target_accounts.len(), 2);
        assert_eq!(first[0].order.notional_usd, 20.0);
        assert_eq!(strategy.id(), "copy_main");
    }
}
