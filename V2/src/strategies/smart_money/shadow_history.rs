use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::domain::OrderSide;

use super::{
    CopyDryRunShadowRecord, CopyLedgerStatus, CopyLiveGateDecision, CopySignalRiskDecision,
};

const COPY_SHADOW_HISTORY_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CopyShadowHistoryEntry {
    pub schema_version: u32,
    pub occurred_at_ms: u64,
    pub status: String,
    pub leader_id: String,
    pub leader_address: String,
    pub coin: String,
    pub action_kind: String,
    pub action_event_id: String,
    pub live_gate: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub risk_reject_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side: Option<OrderSide>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reduce_only: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notional_usd: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ledger_status: Option<CopyLedgerStatus>,
}

impl CopyShadowHistoryEntry {
    pub fn from_shadow_record(record: &CopyDryRunShadowRecord, occurred_at_ms: u64) -> Self {
        let risk_reject_reason = match &record.risk_decision {
            CopySignalRiskDecision::Rejected { reason_code } => Some(reason_code.clone()),
            CopySignalRiskDecision::Approved { .. } => None,
        };
        let status = if record.signal.is_some() {
            "would_copy"
        } else if risk_reject_reason.is_some()
            || matches!(record.live_gate, CopyLiveGateDecision::Rejected { .. })
        {
            "rejected"
        } else {
            "deduped"
        };
        Self {
            schema_version: COPY_SHADOW_HISTORY_SCHEMA_VERSION,
            occurred_at_ms,
            status: status.to_string(),
            leader_id: record.action.leader_id.clone(),
            leader_address: record.action.leader_address.clone(),
            coin: record.action.coin.clone(),
            action_kind: format!("{:?}", record.action.kind),
            action_event_id: record.action.event_id.clone(),
            live_gate: copy_live_gate_label(&record.live_gate),
            risk_reject_reason,
            signal_id: record
                .signal
                .as_ref()
                .map(|signal| signal.signal_id.clone()),
            side: record.signal.as_ref().map(|signal| signal.order.side),
            reduce_only: record
                .signal
                .as_ref()
                .map(|signal| signal.order.reduce_only),
            notional_usd: record
                .signal
                .as_ref()
                .map(|signal| signal.order.notional_usd),
            ledger_status: record.ledger_entry.as_ref().map(|entry| entry.status),
        }
    }
}

pub fn append_copy_shadow_history_entry(path: &Path, entry: &CopyShadowHistoryEntry) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create copy shadow history directory {}",
                parent.display()
            )
        })?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open copy shadow history {}", path.display()))?;
    let mut line =
        serde_json::to_vec(entry).context("failed to serialize copy shadow history entry")?;
    line.push(b'\n');
    file.write_all(&line)
        .with_context(|| format!("failed to write copy shadow history {}", path.display()))?;
    Ok(())
}

pub fn append_copy_shadow_history_records(
    path: &Path,
    records: &[CopyDryRunShadowRecord],
    occurred_at_ms: u64,
) -> Result<()> {
    for record in records {
        append_copy_shadow_history_entry(
            path,
            &CopyShadowHistoryEntry::from_shadow_record(record, occurred_at_ms),
        )?;
    }
    Ok(())
}

pub fn read_recent_copy_shadow_history_entries(
    path: &Path,
    limit: usize,
) -> Result<Vec<CopyShadowHistoryEntry>> {
    if limit == 0 || !path.exists() {
        return Ok(Vec::new());
    }
    let raw = fs::read(path)
        .with_context(|| format!("failed to read copy shadow history {}", path.display()))?;
    let mut entries = Vec::new();
    for line in raw.split(|byte| *byte == b'\n').rev() {
        let line = trim_ascii_bytes(line);
        if line.is_empty() {
            continue;
        }
        let entry = match serde_json::from_slice::<CopyShadowHistoryEntry>(line) {
            Ok(entry) if entry.schema_version == COPY_SHADOW_HISTORY_SCHEMA_VERSION => entry,
            Ok(_) => continue,
            Err(_) => continue,
        };
        entries.push(entry);
        if entries.len() >= limit {
            break;
        }
    }
    Ok(entries)
}

fn copy_live_gate_label(decision: &CopyLiveGateDecision) -> String {
    match decision {
        CopyLiveGateDecision::DryRunOnly => "dry_run_only".to_string(),
        CopyLiveGateDecision::LiveAllowed => "live_allowed".to_string(),
        CopyLiveGateDecision::Rejected { reason_code } => {
            format!("rejected:{reason_code}")
        }
    }
}

fn trim_ascii_bytes(mut input: &[u8]) -> &[u8] {
    while let Some((first, rest)) = input.split_first() {
        if !first.is_ascii_whitespace() {
            break;
        }
        input = rest;
    }
    while let Some((last, rest)) = input.split_last() {
        if !last.is_ascii_whitespace() {
            break;
        }
        input = rest;
    }
    input
}
