use std::{collections::HashSet, fs, path::Path};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use super::{CopyLedger, CopyLedgerEntry};

const COPY_PERSISTENCE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CopyPersistenceSnapshot {
    pub schema_version: u32,
    pub saved_at_ms: u64,
    #[serde(default)]
    pub seen_event_keys: Vec<String>,
    #[serde(default)]
    pub ledger_entries: Vec<CopyLedgerEntry>,
}

impl CopyPersistenceSnapshot {
    pub fn new(
        saved_at_ms: u64,
        seen_event_keys: impl IntoIterator<Item = String>,
        ledger: &CopyLedger,
    ) -> Self {
        let mut seen_event_keys = seen_event_keys.into_iter().collect::<Vec<_>>();
        seen_event_keys.sort();
        seen_event_keys.dedup();
        let mut ledger_entries = ledger.entries().to_vec();
        ledger_entries.sort_by(|left, right| left.signal_id.cmp(&right.signal_id));
        Self {
            schema_version: COPY_PERSISTENCE_SCHEMA_VERSION,
            saved_at_ms,
            seen_event_keys,
            ledger_entries,
        }
    }

    pub fn empty() -> Self {
        Self {
            schema_version: COPY_PERSISTENCE_SCHEMA_VERSION,
            saved_at_ms: 0,
            seen_event_keys: Vec::new(),
            ledger_entries: Vec::new(),
        }
    }

    pub fn ledger(&self) -> CopyLedger {
        CopyLedger::from_entries(self.ledger_entries.clone())
    }

    pub fn seen_event_key_set(&self) -> HashSet<String> {
        self.seen_event_keys.iter().cloned().collect()
    }
}

pub fn save_copy_persistence_snapshot(
    path: &Path,
    snapshot: &CopyPersistenceSnapshot,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create copy persistence directory {}",
                parent.display()
            )
        })?;
    }
    let encoded =
        serde_json::to_vec_pretty(snapshot).context("failed to serialize copy persistence")?;
    fs::write(path, encoded)
        .with_context(|| format!("failed to write copy persistence {}", path.display()))?;
    Ok(())
}

pub fn load_copy_persistence_snapshot(path: &Path) -> Result<CopyPersistenceSnapshot> {
    if !path.exists() {
        return Ok(CopyPersistenceSnapshot::empty());
    }
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read copy persistence {}", path.display()))?;
    if raw.trim().is_empty() {
        return Ok(CopyPersistenceSnapshot::empty());
    }
    let snapshot = serde_json::from_str::<CopyPersistenceSnapshot>(&raw)
        .with_context(|| format!("failed to parse copy persistence {}", path.display()))?;
    if snapshot.schema_version != COPY_PERSISTENCE_SCHEMA_VERSION {
        anyhow::bail!(
            "unsupported copy persistence schema version {}",
            snapshot.schema_version
        );
    }
    Ok(snapshot)
}
