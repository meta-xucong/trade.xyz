use crate::domain::OrderSide;

use super::{LeaderActionConfidence, SemanticLeaderAction};

#[derive(Debug, Clone, Copy)]
pub struct CopySizingInput {
    pub leader_notional_usd: f64,
    pub leader_copy_ratio: f64,
    pub account_copy_ratio: f64,
    pub principal_cap_usd: Option<f64>,
    pub leverage: f64,
    pub leader_trade_cap_usd: Option<f64>,
    pub symbol_order_cap_usd: Option<f64>,
    pub account_order_cap_usd: Option<f64>,
    pub remaining_symbol_position_cap_usd: Option<f64>,
    pub remaining_daily_cap_usd: Option<f64>,
    pub exchange_min_open_notional_usd: f64,
    pub reduce_only: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CopySizingDecision {
    Approved { notional_usd: f64 },
    Rejected { reason_code: String },
}

#[derive(Debug, Clone, Copy)]
pub struct CopySignalRiskInput<'a> {
    pub action: &'a SemanticLeaderAction,
    pub sizing: CopySizingInput,
    pub now_ms: u64,
    pub max_signal_delay_ms: u64,
    pub leader_enabled: bool,
    pub symbol_blocked: bool,
    pub allow_short: bool,
    pub current_effective_exposure_usd: f64,
    pub same_leader_effective_exposure_usd: f64,
    pub max_effective_exposure_usd: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CopySignalRiskDecision {
    Approved {
        side: OrderSide,
        reduce_only: bool,
        notional_usd: f64,
    },
    Rejected {
        reason_code: String,
    },
}

pub fn calculate_copy_notional(input: CopySizingInput) -> CopySizingDecision {
    let mut principal =
        input.leader_notional_usd * input.leader_copy_ratio * input.account_copy_ratio;
    if !principal.is_finite() || principal <= 0.0 {
        return CopySizingDecision::Rejected {
            reason_code: "COPY_NOTIONAL_TOO_SMALL".to_string(),
        };
    }
    if let Some(principal_cap) = input.principal_cap_usd {
        principal = principal.min(principal_cap.max(0.0));
    }
    if principal <= 0.0 {
        return CopySizingDecision::Rejected {
            reason_code: "COPY_SYMBOL_CAP_EXCEEDED".to_string(),
        };
    }
    let leverage = input.leverage.max(1.0);
    let mut notional = principal * leverage;
    if !notional.is_finite() || notional <= 0.0 {
        return CopySizingDecision::Rejected {
            reason_code: "COPY_NOTIONAL_TOO_SMALL".to_string(),
        };
    }

    for cap in [
        input.leader_trade_cap_usd,
        input.symbol_order_cap_usd,
        input.account_order_cap_usd,
        input.remaining_symbol_position_cap_usd,
        input.remaining_daily_cap_usd,
    ]
    .into_iter()
    .flatten()
    {
        notional = notional.min(cap.max(0.0));
    }

    if notional <= 0.0 {
        return CopySizingDecision::Rejected {
            reason_code: "COPY_SYMBOL_CAP_EXCEEDED".to_string(),
        };
    }
    if !input.reduce_only && notional < input.exchange_min_open_notional_usd {
        return CopySizingDecision::Rejected {
            reason_code: "COPY_NOTIONAL_TOO_SMALL".to_string(),
        };
    }

    CopySizingDecision::Approved {
        notional_usd: notional,
    }
}

pub fn evaluate_copy_signal_risk(input: CopySignalRiskInput<'_>) -> CopySignalRiskDecision {
    if !input.leader_enabled {
        return copy_risk_rejected("COPY_LEADER_DISABLED");
    }
    if !matches!(input.action.confidence, LeaderActionConfidence::Strong) {
        return copy_risk_rejected("COPY_ACTION_AMBIGUOUS");
    }

    if let Some(side) = input.action.kind.open_side() {
        evaluate_copy_open_risk(input, side)
    } else if let Some(side) = input.action.kind.close_side() {
        evaluate_copy_close_risk(input, side)
    } else {
        copy_risk_rejected("COPY_ACTION_AMBIGUOUS")
    }
}

fn evaluate_copy_open_risk(
    input: CopySignalRiskInput<'_>,
    side: OrderSide,
) -> CopySignalRiskDecision {
    if input.symbol_blocked {
        return copy_risk_rejected("COPY_SYMBOL_BLOCKED");
    }
    if matches!(side, OrderSide::Sell) && !input.allow_short {
        return copy_risk_rejected("COPY_SHORT_NOT_ALLOWED");
    }
    let mut sizing = CopySizingInput {
        reduce_only: false,
        ..input.sizing
    };
    if let Some(max_exposure) = input.max_effective_exposure_usd {
        let current_exposure = input.current_effective_exposure_usd.max(0.0);
        let remaining_exposure = max_exposure - current_exposure;
        if remaining_exposure <= 0.0
            || remaining_exposure < sizing.exchange_min_open_notional_usd.max(0.0)
        {
            return copy_risk_rejected("COPY_PENDING_EXPOSURE_LIMIT");
        }
        sizing.remaining_symbol_position_cap_usd = Some(
            sizing
                .remaining_symbol_position_cap_usd
                .map_or(remaining_exposure, |configured_cap| {
                    configured_cap.min(remaining_exposure)
                }),
        );
    }

    match calculate_copy_notional(sizing) {
        CopySizingDecision::Approved { notional_usd } => CopySignalRiskDecision::Approved {
            side,
            reduce_only: false,
            notional_usd,
        },
        CopySizingDecision::Rejected { reason_code } => {
            CopySignalRiskDecision::Rejected { reason_code }
        }
    }
}

fn evaluate_copy_close_risk(
    input: CopySignalRiskInput<'_>,
    side: OrderSide,
) -> CopySignalRiskDecision {
    if input
        .sizing
        .remaining_symbol_position_cap_usd
        .unwrap_or(0.0)
        <= 0.0
    {
        return copy_risk_rejected("COPY_CLOSE_WITHOUT_LOCAL_MAPPING");
    }
    let sizing = CopySizingInput {
        reduce_only: true,
        ..input.sizing
    };
    match calculate_copy_notional(sizing) {
        CopySizingDecision::Approved { notional_usd } => CopySignalRiskDecision::Approved {
            side,
            reduce_only: true,
            notional_usd,
        },
        CopySizingDecision::Rejected { reason_code } => {
            CopySignalRiskDecision::Rejected { reason_code }
        }
    }
}

fn copy_risk_rejected(reason_code: &str) -> CopySignalRiskDecision {
    CopySignalRiskDecision::Rejected {
        reason_code: reason_code.to_string(),
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CopyLiveGateInput {
    pub process_dry_run: bool,
    pub live_copy_enabled: bool,
    pub account_worker_live: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyLiveGateDecision {
    DryRunOnly,
    LiveAllowed,
    Rejected { reason_code: String },
}

pub fn evaluate_copy_live_gate(input: CopyLiveGateInput) -> CopyLiveGateDecision {
    if input.process_dry_run {
        return CopyLiveGateDecision::DryRunOnly;
    }
    if !input.live_copy_enabled {
        return CopyLiveGateDecision::Rejected {
            reason_code: "COPY_LIVE_GATE_DISABLED".to_string(),
        };
    }
    if !input.account_worker_live {
        return CopyLiveGateDecision::Rejected {
            reason_code: "COPY_ACCOUNT_WORKER_NOT_LIVE".to_string(),
        };
    }
    CopyLiveGateDecision::LiveAllowed
}
