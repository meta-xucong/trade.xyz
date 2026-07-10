use serde::{Deserialize, Serialize};

use crate::{
    domain::{OrderSide, OrderSubmitted},
    hyperliquid::{OrderStatusResponse, UserFill},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CopyLedgerStatus {
    PendingOpen,
    Open,
    PendingReduce,
    PendingClose,
    Closed,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CopyLedgerEntry {
    pub local_account_id: String,
    pub leader_id: String,
    pub leader_group: String,
    pub signal_id: String,
    pub coin: String,
    pub local_side: OrderSide,
    #[serde(default)]
    pub order_cloid: Option<String>,
    #[serde(default)]
    pub order_oid: Option<u64>,
    #[serde(default)]
    pub submitted_at_ms: Option<u64>,
    #[serde(default)]
    pub filled_at_ms: Option<u64>,
    pub planned_notional_usd: f64,
    pub pending_notional_usd: f64,
    pub filled_notional_usd: f64,
    pub remaining_notional_usd: f64,
    pub status: CopyLedgerStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CopyLedgerReconcileResult {
    pub applied: bool,
    pub signal_id: String,
    pub status: Option<CopyLedgerStatus>,
    pub filled_notional_usd: f64,
    pub consumed_notional_usd: f64,
    pub reason_code: Option<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct CopyLedger {
    #[serde(default)]
    entries: Vec<CopyLedgerEntry>,
}

impl CopyLedger {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_entries(entries: Vec<CopyLedgerEntry>) -> Self {
        Self { entries }
    }

    pub fn entries(&self) -> &[CopyLedgerEntry] {
        &self.entries
    }

    pub fn push(&mut self, entry: CopyLedgerEntry) {
        self.entries.push(entry);
    }

    pub fn apply_order_submission(&mut self, report: &OrderSubmitted) -> CopyLedgerReconcileResult {
        self.apply_order_submission_with_fill_time(report, None)
    }

    fn apply_order_submission_with_fill_time(
        &mut self,
        report: &OrderSubmitted,
        filled_at_ms: Option<u64>,
    ) -> CopyLedgerReconcileResult {
        if report.dry_run {
            return copy_ledger_reconcile_ignored(report, "COPY_LEDGER_DRY_RUN_REPORT");
        }
        if report.signal_id.trim().is_empty() {
            return copy_ledger_reconcile_ignored(report, "COPY_LEDGER_MISSING_SIGNAL_ID");
        }

        let Some(index) = self.entries.iter().position(|entry| {
            entry.local_account_id == report.account_id
                && entry.signal_id == report.signal_id
                && entry.coin == report.coin
        }) else {
            return copy_ledger_reconcile_ignored(report, "COPY_LEDGER_UNOWNED_REPORT");
        };

        if let Some(existing_cloid) = self.entries[index].order_cloid.as_deref()
            && !report.cloid.trim().is_empty()
            && !copy_cloid_equivalent(existing_cloid, &report.cloid)
        {
            return copy_ledger_reconcile_ignored(report, "COPY_LEDGER_CLOID_MISMATCH");
        }

        let status = self.entries[index].status;
        let filled_notional_usd = copy_report_filled_notional_usd(report);
        let is_filled = copy_report_is_filled(report) && filled_notional_usd > 0.0;

        if self.entries[index].order_cloid.is_none() && !report.cloid.trim().is_empty() {
            self.entries[index].order_cloid = Some(report.cloid.clone());
        }
        self.entries[index].order_oid = report.oid.or(self.entries[index].order_oid);
        if self.entries[index].submitted_at_ms.is_none() {
            self.entries[index].submitted_at_ms = Some(report.submitted_at_ms);
        }

        if !is_filled {
            return CopyLedgerReconcileResult {
                applied: true,
                signal_id: report.signal_id.clone(),
                status: Some(self.entries[index].status),
                filled_notional_usd: 0.0,
                consumed_notional_usd: 0.0,
                reason_code: None,
            };
        }

        match status {
            CopyLedgerStatus::PendingOpen => {
                self.entries[index].pending_notional_usd = 0.0;
                self.entries[index].filled_notional_usd = filled_notional_usd;
                self.entries[index].remaining_notional_usd = filled_notional_usd;
                self.entries[index].filled_at_ms =
                    Some(filled_at_ms.unwrap_or(report.submitted_at_ms));
                self.entries[index].status = CopyLedgerStatus::Open;
                CopyLedgerReconcileResult {
                    applied: true,
                    signal_id: report.signal_id.clone(),
                    status: Some(CopyLedgerStatus::Open),
                    filled_notional_usd,
                    consumed_notional_usd: 0.0,
                    reason_code: None,
                }
            }
            CopyLedgerStatus::PendingReduce | CopyLedgerStatus::PendingClose => {
                let original_entry = self.entries[index].clone();
                let local_side = self.entries[index].local_side;
                let account_id = self.entries[index].local_account_id.clone();
                let coin = self.entries[index].coin.clone();
                let current_pending_notional = self.entries[index].pending_notional_usd.max(0.0);
                let filled_at = filled_at_ms.unwrap_or(report.submitted_at_ms);
                let consumed_notional_usd = self.consume_open_exposure(
                    index,
                    &account_id,
                    &coin,
                    local_side,
                    filled_notional_usd,
                );
                let filled_for_current_entry = filled_notional_usd.min(current_pending_notional);
                let remaining_pending_notional =
                    (current_pending_notional - filled_for_current_entry).max(0.0);
                let carried_reduce_notional =
                    (filled_notional_usd - filled_for_current_entry).max(0.0);
                if carried_reduce_notional > 0.0 {
                    self.close_carried_pending_reduces(
                        index,
                        &account_id,
                        &coin,
                        local_side,
                        carried_reduce_notional,
                        filled_at,
                    );
                }
                self.entries[index].pending_notional_usd = 0.0;
                self.entries[index].filled_notional_usd += filled_for_current_entry;
                self.entries[index].remaining_notional_usd = 0.0;
                self.entries[index].filled_at_ms = Some(filled_at);
                self.entries[index].status = CopyLedgerStatus::Closed;
                if remaining_pending_notional > 1e-9 {
                    self.entries.push(CopyLedgerEntry {
                        signal_id: copy_residual_reduce_signal_id(
                            &original_entry.signal_id,
                            report,
                        ),
                        order_cloid: None,
                        order_oid: None,
                        submitted_at_ms: None,
                        filled_at_ms: None,
                        planned_notional_usd: remaining_pending_notional,
                        pending_notional_usd: remaining_pending_notional,
                        filled_notional_usd: 0.0,
                        remaining_notional_usd: remaining_pending_notional,
                        status,
                        ..original_entry
                    });
                }
                CopyLedgerReconcileResult {
                    applied: true,
                    signal_id: report.signal_id.clone(),
                    status: Some(CopyLedgerStatus::Closed),
                    filled_notional_usd: self.entries[index].filled_notional_usd,
                    consumed_notional_usd,
                    reason_code: None,
                }
            }
            CopyLedgerStatus::Open | CopyLedgerStatus::Closed => CopyLedgerReconcileResult {
                applied: true,
                signal_id: report.signal_id.clone(),
                status: Some(status),
                filled_notional_usd: self.entries[index].filled_notional_usd,
                consumed_notional_usd: 0.0,
                reason_code: Some("COPY_LEDGER_ALREADY_RECONCILED".to_string()),
            },
            CopyLedgerStatus::Rejected => {
                copy_ledger_reconcile_ignored(report, "COPY_LEDGER_REJECTED_ENTRY")
            }
        }
    }

    pub fn apply_order_status_evidence(
        &mut self,
        local_account_id: &str,
        worker_id: &str,
        order_status: &OrderStatusResponse,
        user_fills: &[UserFill],
    ) -> CopyLedgerReconcileResult {
        let Some(info) = order_status.order.as_ref() else {
            return copy_ledger_reconcile_ignored_signal("", "COPY_LEDGER_ORDER_STATUS_UNKNOWN");
        };
        let order = &info.order;
        let Some(index) = self.entries.iter().position(|entry| {
            entry.local_account_id == local_account_id
                && entry.coin == order.coin
                && (entry.order_oid == Some(order.oid)
                    || entry
                        .order_cloid
                        .as_deref()
                        .zip(order.cloid.as_deref())
                        .is_some_and(|(left, right)| copy_cloid_equivalent(left, right)))
        }) else {
            return copy_ledger_reconcile_ignored_signal("", "COPY_LEDGER_UNOWNED_ORDER_STATUS");
        };

        if let Some(existing_cloid) = self.entries[index].order_cloid.as_deref()
            && let Some(status_cloid) = order.cloid.as_deref()
            && !copy_cloid_equivalent(existing_cloid, status_cloid)
        {
            return copy_ledger_reconcile_ignored_signal(
                &self.entries[index].signal_id,
                "COPY_LEDGER_CLOID_MISMATCH",
            );
        }

        let Some(side) = copy_order_side_from_exchange(&order.side) else {
            return copy_ledger_reconcile_ignored_signal(
                &self.entries[index].signal_id,
                "COPY_LEDGER_ORDER_SIDE_UNKNOWN",
            );
        };
        if matches!(self.entries[index].status, CopyLedgerStatus::Closed)
            && self.entries[index].order_oid == Some(order.oid)
        {
            return CopyLedgerReconcileResult {
                applied: true,
                signal_id: self.entries[index].signal_id.clone(),
                status: Some(CopyLedgerStatus::Closed),
                filled_notional_usd: self.entries[index].filled_notional_usd,
                consumed_notional_usd: 0.0,
                reason_code: Some("COPY_LEDGER_ALREADY_RECONCILED".to_string()),
            };
        }
        if side != expected_order_side_for_entry(&self.entries[index]) {
            return copy_ledger_reconcile_ignored_signal(
                &self.entries[index].signal_id,
                "COPY_LEDGER_ORDER_SIDE_MISMATCH",
            );
        }

        let Some(limit_price) = parse_copy_f64(&order.limit_px) else {
            return copy_ledger_reconcile_ignored_signal(
                &self.entries[index].signal_id,
                "COPY_LEDGER_ORDER_STATUS_PARSE_ERROR",
            );
        };
        let Some(order_size) =
            parse_copy_order_size_for_status(&order.sz, &order.orig_sz, &info.status)
        else {
            return copy_ledger_reconcile_ignored_signal(
                &self.entries[index].signal_id,
                "COPY_LEDGER_ORDER_STATUS_PARSE_ERROR",
            );
        };

        let fill_summary = summarize_copy_fills(order.oid, &order.coin, user_fills);
        let filled_at_ms = fill_summary
            .as_ref()
            .map(|summary| summary.latest_time_ms)
            .unwrap_or(info.status_timestamp);
        let status_is_filled = info.status.eq_ignore_ascii_case("filled");
        let filled_size = fill_summary
            .as_ref()
            .map(|summary| summary.size)
            .or_else(|| status_is_filled.then_some(order_size));
        let avg_fill_price = fill_summary
            .as_ref()
            .map(|summary| summary.avg_price)
            .or_else(|| status_is_filled.then_some(limit_price));
        let notional_usd = filled_size
            .zip(avg_fill_price)
            .map(|(size, price)| size * price)
            .unwrap_or(self.entries[index].planned_notional_usd);
        let cloid = self.entries[index]
            .order_cloid
            .clone()
            .or_else(|| order.cloid.clone())
            .unwrap_or_default();

        let report = OrderSubmitted {
            signal_id: self.entries[index].signal_id.clone(),
            intent_id: format!("reconcile-{}", self.entries[index].signal_id),
            worker_id: worker_id.to_string(),
            account_id: local_account_id.to_string(),
            cloid,
            coin: order.coin.clone(),
            side,
            notional_usd,
            submitted_price: Some(limit_price),
            submitted_size: Some(order_size),
            exchange_status: Some(info.status.clone()),
            oid: Some(order.oid),
            filled_size,
            avg_fill_price,
            dry_run: false,
            submitted_at_ms: order.timestamp,
        };
        self.apply_order_submission_with_fill_time(&report, Some(filled_at_ms))
    }

    fn consume_open_exposure(
        &mut self,
        close_index: usize,
        local_account_id: &str,
        coin: &str,
        side: OrderSide,
        notional_usd: f64,
    ) -> f64 {
        let mut remaining_to_consume = notional_usd.max(0.0);
        let mut consumed = 0.0;
        for (index, entry) in self.entries.iter_mut().enumerate() {
            if index == close_index
                || entry.local_account_id != local_account_id
                || entry.coin != coin
                || entry.local_side != side
                || !matches!(
                    entry.status,
                    CopyLedgerStatus::Open | CopyLedgerStatus::PendingOpen
                )
            {
                continue;
            }
            let available = match entry.status {
                CopyLedgerStatus::PendingOpen => entry.pending_notional_usd.max(0.0),
                _ => entry.remaining_notional_usd.max(0.0),
            };
            let this_consume = available.min(remaining_to_consume);
            if this_consume <= 0.0 {
                continue;
            }
            match entry.status {
                CopyLedgerStatus::PendingOpen => {
                    entry.pending_notional_usd =
                        (entry.pending_notional_usd - this_consume).max(0.0);
                    if entry.pending_notional_usd <= 1e-9 {
                        entry.pending_notional_usd = 0.0;
                        entry.status = CopyLedgerStatus::Closed;
                    }
                }
                _ => {
                    entry.remaining_notional_usd =
                        (entry.remaining_notional_usd - this_consume).max(0.0);
                    if entry.remaining_notional_usd <= 1e-9 {
                        entry.remaining_notional_usd = 0.0;
                        entry.status = CopyLedgerStatus::Closed;
                    }
                }
            }
            consumed += this_consume;
            remaining_to_consume -= this_consume;
            if remaining_to_consume <= 0.0 {
                break;
            }
        }
        consumed
    }

    fn close_carried_pending_reduces(
        &mut self,
        close_index: usize,
        local_account_id: &str,
        coin: &str,
        side: OrderSide,
        notional_usd: f64,
        filled_at_ms: u64,
    ) -> f64 {
        let mut remaining_to_close = notional_usd.max(0.0);
        let mut closed = 0.0;
        for (index, entry) in self.entries.iter_mut().enumerate() {
            if index == close_index
                || entry.local_account_id != local_account_id
                || entry.coin != coin
                || entry.local_side != side
                || !matches!(
                    entry.status,
                    CopyLedgerStatus::PendingReduce | CopyLedgerStatus::PendingClose
                )
            {
                continue;
            }
            let available = entry.pending_notional_usd.max(0.0);
            let this_close = available.min(remaining_to_close);
            if this_close <= 0.0 {
                continue;
            }
            entry.pending_notional_usd = (entry.pending_notional_usd - this_close).max(0.0);
            entry.filled_notional_usd += this_close;
            entry.remaining_notional_usd = entry.pending_notional_usd;
            entry.filled_at_ms = Some(filled_at_ms);
            if entry.pending_notional_usd <= 1e-9 {
                entry.pending_notional_usd = 0.0;
                entry.status = CopyLedgerStatus::Closed;
                entry.remaining_notional_usd = 0.0;
            }
            closed += this_close;
            remaining_to_close -= this_close;
            if remaining_to_close <= 0.0 {
                break;
            }
        }
        closed
    }

    pub fn effective_exposure_usd(
        &self,
        local_account_id: &str,
        coin: &str,
        side: OrderSide,
    ) -> f64 {
        let mut open_notional = 0.0;
        let mut pending_reduce_notional = 0.0;

        for entry in self.entries.iter().filter(|entry| {
            entry.local_account_id == local_account_id
                && entry.coin == coin
                && entry.local_side == side
        }) {
            match entry.status {
                CopyLedgerStatus::PendingOpen => {
                    open_notional += entry.pending_notional_usd.max(0.0);
                }
                CopyLedgerStatus::Open if copy_ledger_entry_has_execution_evidence(entry) => {
                    open_notional += entry.remaining_notional_usd.max(0.0);
                }
                CopyLedgerStatus::PendingReduce | CopyLedgerStatus::PendingClose => {
                    pending_reduce_notional += entry.pending_notional_usd.max(0.0);
                }
                CopyLedgerStatus::Open | CopyLedgerStatus::Closed | CopyLedgerStatus::Rejected => {}
            }
        }

        (open_notional - pending_reduce_notional).max(0.0)
    }

    pub fn effective_exposure_usd_for_leader_group(
        &self,
        local_account_id: &str,
        leader_group: &str,
        coin: &str,
        side: OrderSide,
    ) -> f64 {
        self.effective_exposure_usd_filtered(
            local_account_id,
            coin,
            side,
            Some(leader_group),
            false,
        )
    }

    pub fn submitted_effective_exposure_usd(
        &self,
        local_account_id: &str,
        coin: &str,
        side: OrderSide,
    ) -> f64 {
        let mut open_notional = 0.0;
        let mut pending_reduce_notional = 0.0;

        for entry in self.entries.iter().filter(|entry| {
            entry.local_account_id == local_account_id
                && entry.coin == coin
                && entry.local_side == side
        }) {
            match entry.status {
                CopyLedgerStatus::PendingOpen if copy_ledger_entry_has_submission(entry) => {
                    open_notional += entry.pending_notional_usd.max(0.0);
                }
                CopyLedgerStatus::Open if copy_ledger_entry_has_execution_evidence(entry) => {
                    open_notional += entry.remaining_notional_usd.max(0.0);
                }
                CopyLedgerStatus::PendingReduce | CopyLedgerStatus::PendingClose
                    if copy_ledger_entry_has_submission(entry) =>
                {
                    pending_reduce_notional += entry.pending_notional_usd.max(0.0);
                }
                CopyLedgerStatus::PendingOpen
                | CopyLedgerStatus::Open
                | CopyLedgerStatus::PendingReduce
                | CopyLedgerStatus::PendingClose
                | CopyLedgerStatus::Closed
                | CopyLedgerStatus::Rejected => {}
            }
        }

        (open_notional - pending_reduce_notional).max(0.0)
    }

    pub fn submitted_effective_exposure_usd_for_leader_group(
        &self,
        local_account_id: &str,
        leader_group: &str,
        coin: &str,
        side: OrderSide,
    ) -> f64 {
        self.effective_exposure_usd_filtered(local_account_id, coin, side, Some(leader_group), true)
    }

    pub fn mapped_close_notional_usd(
        &self,
        local_account_id: &str,
        coin: &str,
        close_order_side: OrderSide,
    ) -> f64 {
        let exposure_side = match close_order_side {
            OrderSide::Buy => OrderSide::Sell,
            OrderSide::Sell => OrderSide::Buy,
        };
        self.effective_exposure_usd(local_account_id, coin, exposure_side)
    }

    pub fn mapped_close_notional_usd_for_leader_group(
        &self,
        local_account_id: &str,
        leader_group: &str,
        coin: &str,
        close_order_side: OrderSide,
    ) -> f64 {
        let exposure_side = match close_order_side {
            OrderSide::Buy => OrderSide::Sell,
            OrderSide::Sell => OrderSide::Buy,
        };
        self.effective_exposure_usd_for_leader_group(
            local_account_id,
            leader_group,
            coin,
            exposure_side,
        )
    }

    fn effective_exposure_usd_filtered(
        &self,
        local_account_id: &str,
        coin: &str,
        side: OrderSide,
        leader_group: Option<&str>,
        submitted_only: bool,
    ) -> f64 {
        let mut open_notional = 0.0;
        let mut pending_reduce_notional = 0.0;

        for entry in self.entries.iter().filter(|entry| {
            entry.local_account_id == local_account_id
                && entry.coin == coin
                && entry.local_side == side
                && leader_group.is_none_or(|group| entry.leader_group == group)
        }) {
            match entry.status {
                CopyLedgerStatus::PendingOpen
                    if !submitted_only || copy_ledger_entry_has_submission(entry) =>
                {
                    open_notional += entry.pending_notional_usd.max(0.0);
                }
                CopyLedgerStatus::Open if copy_ledger_entry_has_execution_evidence(entry) => {
                    open_notional += entry.remaining_notional_usd.max(0.0);
                }
                CopyLedgerStatus::PendingReduce | CopyLedgerStatus::PendingClose
                    if !submitted_only || copy_ledger_entry_has_submission(entry) =>
                {
                    pending_reduce_notional += entry.pending_notional_usd.max(0.0);
                }
                CopyLedgerStatus::PendingOpen
                | CopyLedgerStatus::Open
                | CopyLedgerStatus::PendingReduce
                | CopyLedgerStatus::PendingClose
                | CopyLedgerStatus::Closed
                | CopyLedgerStatus::Rejected => {}
            }
        }

        (open_notional - pending_reduce_notional).max(0.0)
    }
}

fn copy_ledger_entry_has_submission(entry: &CopyLedgerEntry) -> bool {
    entry.submitted_at_ms.is_some()
        || entry.order_oid.is_some()
        || entry
            .order_cloid
            .as_deref()
            .is_some_and(|cloid| !cloid.trim().is_empty())
}

pub fn copy_ledger_entry_has_execution_evidence(entry: &CopyLedgerEntry) -> bool {
    entry.filled_at_ms.is_some()
        || entry.order_oid.is_some()
        || entry
            .order_cloid
            .as_deref()
            .is_some_and(|cloid| !cloid.trim().is_empty())
}

fn copy_ledger_reconcile_ignored(
    report: &OrderSubmitted,
    reason_code: &str,
) -> CopyLedgerReconcileResult {
    CopyLedgerReconcileResult {
        applied: false,
        signal_id: report.signal_id.clone(),
        status: None,
        filled_notional_usd: 0.0,
        consumed_notional_usd: 0.0,
        reason_code: Some(reason_code.to_string()),
    }
}

fn copy_ledger_reconcile_ignored_signal(
    signal_id: &str,
    reason_code: &str,
) -> CopyLedgerReconcileResult {
    CopyLedgerReconcileResult {
        applied: false,
        signal_id: signal_id.to_string(),
        status: None,
        filled_notional_usd: 0.0,
        consumed_notional_usd: 0.0,
        reason_code: Some(reason_code.to_string()),
    }
}

fn copy_report_is_filled(report: &OrderSubmitted) -> bool {
    report
        .exchange_status
        .as_deref()
        .is_some_and(|status| status.eq_ignore_ascii_case("filled"))
}

fn copy_report_filled_notional_usd(report: &OrderSubmitted) -> f64 {
    report
        .filled_size
        .zip(report.avg_fill_price)
        .map(|(size, price)| size.abs() * price.abs())
        .filter(|notional| notional.is_finite() && *notional > 0.0)
        .unwrap_or_else(|| {
            if copy_report_is_filled(report) {
                report.notional_usd.max(0.0)
            } else {
                0.0
            }
        })
}

fn copy_order_side_from_exchange(side: &str) -> Option<OrderSide> {
    match side.trim() {
        "B" | "b" => Some(OrderSide::Buy),
        "A" | "a" | "S" | "s" => Some(OrderSide::Sell),
        _ => None,
    }
}

fn expected_order_side_for_entry(entry: &CopyLedgerEntry) -> OrderSide {
    match entry.status {
        CopyLedgerStatus::PendingReduce | CopyLedgerStatus::PendingClose => {
            copy_opposite_order_side(entry.local_side)
        }
        CopyLedgerStatus::PendingOpen
        | CopyLedgerStatus::Open
        | CopyLedgerStatus::Closed
        | CopyLedgerStatus::Rejected => entry.local_side,
    }
}

fn copy_opposite_order_side(side: OrderSide) -> OrderSide {
    match side {
        OrderSide::Buy => OrderSide::Sell,
        OrderSide::Sell => OrderSide::Buy,
    }
}

#[derive(Debug, Clone, Copy)]
struct CopyFillSummary {
    size: f64,
    avg_price: f64,
    latest_time_ms: u64,
}

fn summarize_copy_fills(oid: u64, coin: &str, user_fills: &[UserFill]) -> Option<CopyFillSummary> {
    let mut size = 0.0_f64;
    let mut notional = 0.0_f64;
    let mut latest_time_ms = 0_u64;
    for fill in user_fills
        .iter()
        .filter(|fill| fill.oid == oid && fill.coin == coin)
    {
        let fill_size = parse_copy_f64(&fill.sz)?.abs();
        let fill_price = parse_copy_f64(&fill.px)?.abs();
        size += fill_size;
        notional += fill_size * fill_price;
        latest_time_ms = latest_time_ms.max(fill.time);
    }
    if size <= 0.0 || !size.is_finite() || notional <= 0.0 || !notional.is_finite() {
        return None;
    }
    Some(CopyFillSummary {
        size,
        avg_price: notional / size,
        latest_time_ms,
    })
}

fn parse_copy_f64(value: &str) -> Option<f64> {
    value
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
}

fn parse_copy_order_size_for_status(size: &str, original_size: &str, status: &str) -> Option<f64> {
    let parsed_size = parse_copy_f64(size)?;
    if parsed_size > 0.0 || !status.eq_ignore_ascii_case("filled") {
        return Some(parsed_size);
    }
    parse_copy_f64(original_size).filter(|size| *size > 0.0)
}

fn copy_cloid_equivalent(left: &str, right: &str) -> bool {
    copy_normalized_cloid(left)
        .zip(copy_normalized_cloid(right))
        .is_some_and(|(left, right)| left == right)
}

fn copy_residual_reduce_signal_id(original_signal_id: &str, report: &OrderSubmitted) -> String {
    let order_key = report
        .oid
        .map(|oid| oid.to_string())
        .or_else(|| copy_normalized_cloid(&report.cloid))
        .unwrap_or_else(|| report.submitted_at_ms.to_string());
    format!("{original_signal_id}-residual-{order_key}")
}

fn copy_normalized_cloid(cloid: &str) -> Option<String> {
    let trimmed = cloid.trim();
    if trimmed.is_empty() {
        return None;
    }
    let without_prefix = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);
    let hex = without_prefix.replace('-', "").to_ascii_lowercase();
    (hex.len() == 32 && hex.chars().all(|ch| ch.is_ascii_hexdigit())).then_some(hex)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_pending_close_fill_splits_closed_evidence_from_retryable_residual() {
        let mut ledger = CopyLedger::from_entries(vec![
            CopyLedgerEntry {
                local_account_id: "addr_a".to_string(),
                leader_id: "leader_a".to_string(),
                leader_group: "leader_a".to_string(),
                signal_id: "sig-shaz-open".to_string(),
                coin: "xyz:SHAZ".to_string(),
                local_side: OrderSide::Sell,
                order_cloid: Some("11111111-1111-5111-8111-111111111111".to_string()),
                order_oid: Some(12344),
                submitted_at_ms: Some(900),
                filled_at_ms: Some(950),
                planned_notional_usd: 98.60232,
                pending_notional_usd: 0.0,
                filled_notional_usd: 98.60232,
                remaining_notional_usd: 98.60232,
                status: CopyLedgerStatus::Open,
            },
            CopyLedgerEntry {
                local_account_id: "addr_a".to_string(),
                leader_id: "leader_a".to_string(),
                leader_group: "leader_a".to_string(),
                signal_id: "sig-close-close-1".to_string(),
                coin: "xyz:SHAZ".to_string(),
                local_side: OrderSide::Sell,
                order_cloid: None,
                order_oid: None,
                submitted_at_ms: Some(1000),
                filled_at_ms: None,
                planned_notional_usd: 98.60232,
                pending_notional_usd: 98.60232,
                filled_notional_usd: 0.0,
                remaining_notional_usd: 98.60232,
                status: CopyLedgerStatus::PendingClose,
            },
        ]);
        let report = OrderSubmitted {
            signal_id: "sig-close-close-1".to_string(),
            intent_id: "intent-close-1".to_string(),
            worker_id: "worker-addr_a".to_string(),
            account_id: "addr_a".to_string(),
            cloid: "aaaaaaaa-aaaa-5aaa-8aaa-aaaaaaaaaaaa".to_string(),
            coin: "xyz:SHAZ".to_string(),
            side: OrderSide::Buy,
            notional_usd: 56.57876,
            submitted_price: Some(1.0),
            submitted_size: Some(56.57876),
            exchange_status: Some("filled".to_string()),
            oid: Some(12345),
            filled_size: Some(56.57876),
            avg_fill_price: Some(1.0),
            dry_run: false,
            submitted_at_ms: 2000,
        };

        let result = ledger.apply_order_submission(&report);
        let open = ledger
            .entries()
            .iter()
            .find(|entry| entry.signal_id == "sig-shaz-open")
            .expect("open entry");
        let closed = ledger
            .entries()
            .iter()
            .find(|entry| entry.signal_id == "sig-close-close-1")
            .expect("closed evidence entry");
        let residual = ledger
            .entries()
            .iter()
            .find(|entry| entry.signal_id.starts_with("sig-close-close-1-residual-"))
            .expect("residual retry entry");

        assert!(result.applied);
        assert_eq!(result.status, Some(CopyLedgerStatus::Closed));
        assert_eq!(closed.status, CopyLedgerStatus::Closed);
        assert!((closed.filled_notional_usd - 56.57876).abs() < 1e-9);
        assert_eq!(closed.pending_notional_usd, 0.0);
        assert_eq!(residual.status, CopyLedgerStatus::PendingClose);
        assert!(residual.order_cloid.is_none());
        assert!(residual.order_oid.is_none());
        assert!((residual.pending_notional_usd - 42.02356).abs() < 1e-6);
        assert!((residual.remaining_notional_usd - 42.02356).abs() < 1e-6);
        assert!((open.remaining_notional_usd - 42.02356).abs() < 1e-6);

        let replay = ledger.apply_order_submission(&report);
        assert_eq!(
            replay.reason_code.as_deref(),
            Some("COPY_LEDGER_ALREADY_RECONCILED")
        );
        assert_eq!(ledger.entries().len(), 3);
        let open_after_replay = ledger
            .entries()
            .iter()
            .find(|entry| entry.signal_id == "sig-shaz-open")
            .expect("open entry after replay");
        assert!((open_after_replay.remaining_notional_usd - 42.02356).abs() < 1e-6);
    }
}
