use std::collections::HashMap;

use crate::domain::OrderSide;

use super::LeaderActionKind;

#[derive(Debug, Clone)]
pub struct CopyConflictInput {
    pub event_id: String,
    pub leader_id: String,
    pub leader_group: String,
    pub coin: String,
    pub kind: LeaderActionKind,
    pub leader_notional_usd: f64,
    pub weight: f64,
    pub received_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CopyConflictResolution {
    FollowOpen {
        side: OrderSide,
        score: f64,
        notional_usd: f64,
        event_ids: Vec<String>,
    },
    FollowClose {
        side: OrderSide,
        event_ids: Vec<String>,
    },
    Skip {
        reason_code: String,
        long_score: f64,
        short_score: f64,
        event_ids: Vec<String>,
    },
}

pub fn resolve_copy_conflict(
    events: &[CopyConflictInput],
    min_direction_score_ratio: f64,
    close_overrides_open: bool,
) -> CopyConflictResolution {
    let event_ids = events
        .iter()
        .map(|event| event.event_id.clone())
        .collect::<Vec<_>>();

    if close_overrides_open
        && let Some(close) = events
            .iter()
            .filter_map(|event| event.kind.close_side().map(|side| (event, side)))
            .max_by(|(left, _), (right, _)| event_score(left).total_cmp(&event_score(right)))
    {
        return CopyConflictResolution::FollowClose {
            side: close.1,
            event_ids,
        };
    }

    let mut best_by_group: HashMap<String, &CopyConflictInput> = HashMap::new();
    for event in events
        .iter()
        .filter(|event| event.kind.opens_or_increases())
    {
        let group = if event.leader_group.trim().is_empty() {
            event.leader_id.clone()
        } else {
            event.leader_group.clone()
        };
        match best_by_group.get(&group).copied() {
            Some(existing) if event_score(existing) >= event_score(event) => {}
            _ => {
                best_by_group.insert(group, event);
            }
        }
    }

    let mut long_score = 0.0;
    let mut short_score = 0.0;
    let mut long_notional = 0.0;
    let mut short_notional = 0.0;
    let mut long_ids = Vec::new();
    let mut short_ids = Vec::new();

    for event in best_by_group.values().copied() {
        let score = event_score(event);
        match event.kind.open_side() {
            Some(OrderSide::Buy) => {
                long_score += score;
                long_notional += event.leader_notional_usd.max(0.0);
                long_ids.push(event.event_id.clone());
            }
            Some(OrderSide::Sell) => {
                short_score += score;
                short_notional += event.leader_notional_usd.max(0.0);
                short_ids.push(event.event_id.clone());
            }
            None => {}
        }
    }

    if long_score <= 0.0 && short_score <= 0.0 {
        return CopyConflictResolution::Skip {
            reason_code: "COPY_CONFLICT_NO_OPEN_SIGNAL".to_string(),
            long_score,
            short_score,
            event_ids,
        };
    }
    if long_score > 0.0 && short_score <= 0.0 {
        return CopyConflictResolution::FollowOpen {
            side: OrderSide::Buy,
            score: long_score,
            notional_usd: long_notional,
            event_ids: long_ids,
        };
    }
    if short_score > 0.0 && long_score <= 0.0 {
        return CopyConflictResolution::FollowOpen {
            side: OrderSide::Sell,
            score: short_score,
            notional_usd: short_notional,
            event_ids: short_ids,
        };
    }

    let ratio = (long_score.max(short_score) / long_score.min(short_score)).clamp(0.0, f64::MAX);
    if ratio >= min_direction_score_ratio.max(1.0) {
        if long_score > short_score {
            CopyConflictResolution::FollowOpen {
                side: OrderSide::Buy,
                score: long_score,
                notional_usd: long_notional,
                event_ids: long_ids,
            }
        } else {
            CopyConflictResolution::FollowOpen {
                side: OrderSide::Sell,
                score: short_score,
                notional_usd: short_notional,
                event_ids: short_ids,
            }
        }
    } else {
        CopyConflictResolution::Skip {
            reason_code: "COPY_CONFLICT_NO_DECISION".to_string(),
            long_score,
            short_score,
            event_ids,
        }
    }
}

fn event_score(event: &CopyConflictInput) -> f64 {
    event.weight.max(0.0) * event.leader_notional_usd.max(0.0)
}
