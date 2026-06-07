use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::domain::now_ms;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub event_id: String,
    pub occurred_at_ms: u64,
    pub source: String,
    pub action: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub details: Value,
}

impl AuditEvent {
    pub fn new(
        source: impl Into<String>,
        action: impl Into<String>,
        ok: bool,
        account_id: Option<String>,
        coin: Option<String>,
        error: Option<String>,
        details: Value,
    ) -> Self {
        let occurred_at_ms = now_ms();
        let source = source.into();
        let action = sanitize_audit_text(&action.into());
        let seed = format!("{source}:{action}:{occurred_at_ms}");
        Self {
            event_id: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, seed.as_bytes()).to_string(),
            occurred_at_ms,
            source,
            action,
            ok,
            account_id,
            coin,
            error: error.map(|error| sanitize_audit_text(&error)),
            details: sanitize_audit_value(details),
        }
    }
}

fn sanitize_audit_text(input: &str) -> String {
    [
        ("api_wallet_private_key", "api_wallet_secret"),
        ("private_key", "secret_value"),
        ("password", "secret"),
        ("signature", "signed_payload"),
        ("seed phrase", "recovery phrase"),
    ]
    .into_iter()
    .fold(input.to_string(), |text, (needle, replacement)| {
        replace_case_insensitive(&text, needle, replacement)
    })
}

fn replace_case_insensitive(input: &str, needle: &str, replacement: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut rest = input;
    loop {
        let lower = rest.to_ascii_lowercase();
        let Some(index) = lower.find(needle) else {
            output.push_str(rest);
            break;
        };
        output.push_str(&rest[..index]);
        output.push_str(replacement);
        rest = &rest[index + needle.len()..];
    }
    output
}

fn sanitize_audit_value(value: Value) -> Value {
    match value {
        Value::String(text) => Value::String(sanitize_audit_text(&text)),
        Value::Array(items) => Value::Array(items.into_iter().map(sanitize_audit_value).collect()),
        Value::Object(map) => {
            let sanitized = map
                .into_iter()
                .map(|(key, value)| (sanitize_audit_text(&key), sanitize_audit_value(value)))
                .collect::<Map<String, Value>>();
            Value::Object(sanitized)
        }
        value => value,
    }
}

pub fn append_audit_event(path: &Path, event: &AuditEvent) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create audit log dir {}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open audit log {}", path.display()))?;
    let mut line = serde_json::to_vec(event).context("failed to serialize audit event")?;
    line.push(b'\n');
    file.write_all(&line)
        .with_context(|| format!("failed to write audit log {}", path.display()))?;
    Ok(())
}

pub fn read_recent_audit_events(path: &Path, limit: usize) -> Result<Vec<AuditEvent>> {
    if limit == 0 || !path.exists() {
        return Ok(Vec::new());
    }
    let raw =
        fs::read(path).with_context(|| format!("failed to read audit log {}", path.display()))?;
    let mut events = Vec::new();
    for (line_from_end, line) in raw.split(|byte| *byte == b'\n').rev().enumerate() {
        let line = trim_ascii_bytes(line);
        if line.is_empty() {
            continue;
        }
        let event = match serde_json::from_slice::<AuditEvent>(line) {
            Ok(event) => event,
            Err(error) => {
                tracing::warn!(
                    path = %path.display(),
                    line_from_end = line_from_end + 1,
                    error = %error,
                    "skipping malformed audit log line"
                );
                continue;
            }
        };
        events.push(event);
        if events.len() >= limit {
            break;
        }
    }
    Ok(events)
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

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;

    use super::{AuditEvent, append_audit_event, read_recent_audit_events};

    #[test]
    fn audit_events_append_as_json_lines_and_read_recent() {
        let dir =
            std::env::temp_dir().join(format!("trade_xyz_audit_test_{}", crate::domain::now_ms()));
        let path = dir.join("audit.jsonl");

        append_audit_event(
            &path,
            &AuditEvent::new(
                "frontend",
                "manual_order",
                true,
                Some("addr_a".to_string()),
                Some("xyz:NVDA".to_string()),
                None,
                json!({"notional_usd": 1.0}),
            ),
        )
        .expect("append first audit event");
        append_audit_event(
            &path,
            &AuditEvent::new(
                "frontend",
                "signed_submit",
                false,
                Some("addr_a".to_string()),
                Some("xyz:NVDA".to_string()),
                Some("blocked".to_string()),
                json!({"submit": true}),
            ),
        )
        .expect("append second audit event");

        let raw = fs::read_to_string(&path).expect("audit file");
        assert_eq!(raw.lines().count(), 2);
        assert!(!raw.contains("private_key"));
        assert!(!raw.contains("password"));

        let recent = read_recent_audit_events(&path, 1).expect("recent audit");
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].action, "signed_submit");
        assert!(!recent[0].ok);
    }

    #[test]
    fn audit_events_sanitize_sensitive_words() {
        let event = AuditEvent::new(
            "frontend",
            "vault_change_password",
            false,
            Some("addr_a".to_string()),
            None,
            Some("vault is locked; unlock before this action or enter password".to_string()),
            json!({
                "api_wallet_private_key": "not-a-real-key",
                "nested": {
                    "signature": "not-a-real-signature",
                    "message": "wrong password"
                }
            }),
        );

        let raw = serde_json::to_string(&event).expect("audit json");
        assert!(!raw.to_ascii_lowercase().contains("password"));
        assert!(!raw.to_ascii_lowercase().contains("private_key"));
        assert!(!raw.to_ascii_lowercase().contains("signature"));
        assert!(raw.contains("vault_change_secret"));
        assert!(raw.contains("api_wallet_secret"));
    }
}
