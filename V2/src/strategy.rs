use crate::domain::{CoordinatorSignal, TimestampMs};

#[derive(Debug, Clone)]
pub enum StrategyEvent {
    MarketPrice(MarketPriceEvent),
    LeaderFill(LeaderFillEvent),
}

#[derive(Debug, Clone)]
pub struct MarketPriceEvent {
    pub event_id: String,
    pub coin: String,
    pub price: f64,
    pub received_at_ms: TimestampMs,
}

#[derive(Debug, Clone)]
pub struct LeaderFillEvent {
    pub event_id: String,
    pub leader_id: String,
    pub leader_address: String,
    pub coin: String,
    pub side: crate::domain::OrderSide,
    pub price: f64,
    pub size: f64,
    pub notional_usd: f64,
    pub reduce_only: bool,
    pub exchange_time_ms: TimestampMs,
    pub received_at_ms: TimestampMs,
}

#[derive(Debug, Clone)]
pub struct StrategyContext {
    pub target_accounts: Vec<String>,
    pub signal_ttl_ms: u64,
}

pub trait Strategy {
    fn id(&self) -> &str;
    fn on_event(&mut self, ctx: &StrategyContext, event: StrategyEvent) -> Vec<CoordinatorSignal>;
    fn on_timer(&mut self, _ctx: &StrategyContext, _now_ms: TimestampMs) -> Vec<CoordinatorSignal> {
        Vec::new()
    }
}
