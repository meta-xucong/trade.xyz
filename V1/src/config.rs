use std::{fs, path::Path};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct AppConfig {
    #[serde(default)]
    pub app: AppSection,
    #[serde(default)]
    pub hyperliquid: HyperliquidSection,
    #[serde(default)]
    pub process: ProcessSection,
    #[serde(default)]
    pub secrets: SecretsSection,
    #[serde(default)]
    pub storage: StorageSection,
    #[serde(default)]
    pub accounts: Vec<AccountConfig>,
    #[serde(default)]
    pub manual_ops: ManualOpsSection,
    #[serde(default)]
    pub module_symbol_policies: ModuleSymbolPoliciesSection,
    #[serde(default)]
    pub risk: RiskSection,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppSection {
    #[serde(default = "default_app_name")]
    pub name: String,
    #[serde(default = "default_environment")]
    pub environment: String,
    #[serde(default = "default_true")]
    pub dry_run: bool,
    #[serde(default = "default_true")]
    pub fail_closed: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HyperliquidSection {
    #[serde(default = "default_info_url")]
    pub info_url: String,
    #[serde(default = "default_exchange_url")]
    pub exchange_url: String,
    #[serde(default = "default_ws_url")]
    pub ws_url: String,
    #[serde(default = "default_dex")]
    pub dex: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProcessSection {
    #[serde(default = "default_role")]
    pub role: String,
    #[serde(default = "default_ipc_bind_addr")]
    pub ipc_bind_addr: String,
    #[serde(default = "default_worker_heartbeat_ms")]
    pub worker_heartbeat_ms: u64,
    #[serde(default = "default_signal_ttl_ms")]
    pub signal_ttl_ms: u64,
    #[serde(default = "default_worker_startup_timeout_ms")]
    pub worker_startup_timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecretsSection {
    #[serde(default = "default_vault_path")]
    pub vault_path: String,
    #[serde(default)]
    pub allow_env_fallback: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageSection {
    #[serde(default = "default_audit_log_path")]
    pub audit_log_path: String,
    #[serde(default = "default_protective_rules_path")]
    pub protective_rules_path: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RiskSection {
    #[serde(default)]
    pub global: GlobalRiskSection,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GlobalRiskSection {
    #[serde(default)]
    pub kill_switch: bool,
    #[serde(default = "default_true")]
    pub allow_reduce_only_when_killed: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AccountConfig {
    pub account_id: String,
    pub address: String,
    #[serde(default)]
    pub secret_id: String,
    #[serde(default)]
    pub api_wallet_env: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub worker_enabled: bool,
    #[serde(default = "default_copy_ratio")]
    pub copy_ratio: f64,
    #[serde(default = "default_max_order_notional")]
    pub max_order_notional_usd: f64,
    #[serde(default)]
    pub blocked_markets: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ManualOpsSection {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub manual_trading_enabled: bool,
    #[serde(default)]
    pub manual_live_enabled: bool,
    #[serde(default)]
    pub mainnet_live_enabled: bool,
    #[serde(default = "default_confirm_notional")]
    pub require_confirm_above_notional_usd: f64,
    #[serde(default = "default_max_order_notional")]
    pub max_manual_order_notional_usd: f64,
    #[serde(default = "default_max_manual_batch_accounts")]
    pub max_manual_batch_accounts: usize,
    #[serde(default, alias = "allowed_symbols")]
    pub blocked_symbols: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ModuleSymbolPoliciesSection {
    #[serde(default, alias = "manual_allowed_symbols")]
    pub manual_blocked_symbols: Vec<String>,
    #[serde(default, alias = "fib_allowed_symbols")]
    pub fib_blocked_symbols: Vec<String>,
    #[serde(default, alias = "copy_allowed_symbols")]
    pub copy_blocked_symbols: Vec<String>,
}

impl Default for AppSection {
    fn default() -> Self {
        Self {
            name: default_app_name(),
            environment: default_environment(),
            dry_run: true,
            fail_closed: true,
        }
    }
}

impl Default for HyperliquidSection {
    fn default() -> Self {
        Self {
            info_url: default_info_url(),
            exchange_url: default_exchange_url(),
            ws_url: default_ws_url(),
            dex: default_dex(),
        }
    }
}

impl Default for ProcessSection {
    fn default() -> Self {
        Self {
            role: default_role(),
            ipc_bind_addr: default_ipc_bind_addr(),
            worker_heartbeat_ms: default_worker_heartbeat_ms(),
            signal_ttl_ms: default_signal_ttl_ms(),
            worker_startup_timeout_ms: default_worker_startup_timeout_ms(),
        }
    }
}

impl Default for SecretsSection {
    fn default() -> Self {
        Self {
            vault_path: default_vault_path(),
            allow_env_fallback: false,
        }
    }
}

impl Default for StorageSection {
    fn default() -> Self {
        Self {
            audit_log_path: default_audit_log_path(),
            protective_rules_path: default_protective_rules_path(),
        }
    }
}

impl Default for GlobalRiskSection {
    fn default() -> Self {
        Self {
            kill_switch: false,
            allow_reduce_only_when_killed: true,
        }
    }
}

impl Default for ManualOpsSection {
    fn default() -> Self {
        Self {
            enabled: true,
            manual_trading_enabled: true,
            manual_live_enabled: false,
            mainnet_live_enabled: false,
            require_confirm_above_notional_usd: default_confirm_notional(),
            max_manual_order_notional_usd: default_max_order_notional(),
            max_manual_batch_accounts: default_max_manual_batch_accounts(),
            blocked_symbols: Vec::new(),
        }
    }
}

pub fn load_config(path: &Path) -> Result<AppConfig> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read config file {}", path.display()))?;
    let config = toml::from_str::<AppConfig>(&raw)
        .with_context(|| format!("failed to parse config file {}", path.display()))?;
    validate_config(&config)?;
    Ok(config)
}

pub fn save_config(path: &Path, config: &AppConfig) -> Result<()> {
    validate_config(config)?;
    let raw = toml::to_string_pretty(config).context("failed to serialize config")?;
    fs::write(path, raw).with_context(|| format!("failed to write config file {}", path.display()))
}

pub fn validate_config(config: &AppConfig) -> Result<()> {
    if config.app.name.trim().is_empty() {
        anyhow::bail!("app.name cannot be empty");
    }
    if config.app.environment.trim().is_empty() {
        anyhow::bail!("app.environment cannot be empty");
    }
    if !config.app.dry_run && config.app.environment == "mainnet" && config.app.fail_closed {
        // This branch deliberately reads the safety fields together. Live execution still
        // requires an explicit CLI path and is not implemented in this MVP.
    }
    if config.hyperliquid.info_url.trim().is_empty()
        || config.hyperliquid.exchange_url.trim().is_empty()
        || config.hyperliquid.ws_url.trim().is_empty()
        || config.hyperliquid.dex.trim().is_empty()
    {
        anyhow::bail!("hyperliquid urls and dex must be configured");
    }
    if config.process.role.trim().is_empty() {
        anyhow::bail!("process.role cannot be empty");
    }
    if config.process.worker_heartbeat_ms == 0 {
        anyhow::bail!("process.worker_heartbeat_ms must be positive");
    }
    if config.secrets.vault_path.trim().is_empty() {
        anyhow::bail!("secrets.vault_path cannot be empty");
    }
    if config.storage.audit_log_path.trim().is_empty() {
        anyhow::bail!("storage.audit_log_path cannot be empty");
    }
    if config.storage.protective_rules_path.trim().is_empty() {
        anyhow::bail!("storage.protective_rules_path cannot be empty");
    }
    if !config.manual_ops.enabled {
        anyhow::bail!("manual_ops.enabled=false is not supported by dry-run MVP");
    }
    if !config.manual_ops.manual_trading_enabled {
        anyhow::bail!("manual trading must be enabled for dry-run MVP");
    }
    if config.app.environment == "mainnet"
        && config.manual_ops.manual_live_enabled
        && !config.manual_ops.mainnet_live_enabled
    {
        anyhow::bail!("mainnet manual live trading requires manual_ops.mainnet_live_enabled=true");
    }
    if config.manual_ops.require_confirm_above_notional_usd < 0.0 {
        anyhow::bail!("manual_ops.require_confirm_above_notional_usd cannot be negative");
    }
    if config.accounts.is_empty() {
        anyhow::bail!("config must define at least one account worker");
    }

    if config.manual_ops.max_manual_batch_accounts == 0 {
        anyhow::bail!("manual_ops.max_manual_batch_accounts must be positive");
    }

    let enabled_workers = config.enabled_worker_accounts().count();
    if enabled_workers == 0 {
        anyhow::bail!("config must define at least one enabled account worker");
    }

    for account in &config.accounts {
        if account.account_id.trim().is_empty() {
            anyhow::bail!("account_id cannot be empty");
        }
        if account.address.trim().is_empty() {
            anyhow::bail!("account {} address cannot be empty", account.account_id);
        }
        if account.max_order_notional_usd <= 0.0 {
            anyhow::bail!(
                "account {} max_order_notional_usd must be positive",
                account.account_id
            );
        }
        if !(0.0..=1.0).contains(&account.copy_ratio) {
            anyhow::bail!("account {} copy_ratio must be 0..=1", account.account_id);
        }
        for blocked in &account.blocked_markets {
            anyhow::ensure!(
                normalize_market_id(blocked).is_some(),
                "account {} blocked_markets contains unknown market {}",
                account.account_id,
                blocked
            );
        }
    }

    Ok(())
}

impl AppConfig {
    pub fn enabled_worker_accounts(&self) -> impl Iterator<Item = &AccountConfig> {
        self.accounts
            .iter()
            .filter(|account| account.enabled && account.worker_enabled)
    }

    pub fn account(&self, account_id: &str) -> Option<&AccountConfig> {
        self.accounts
            .iter()
            .find(|account| account.account_id == account_id)
    }

    pub fn default_coin(&self) -> String {
        "xyz:XYZ100".to_string()
    }

    pub fn default_coin_for_market(&self, market: &str) -> String {
        match normalize_market_id(market).unwrap_or(MARKET_XYZ_PERP) {
            MARKET_HL_PERP => "BTC".to_string(),
            MARKET_SPOT => "PURR/USDC".to_string(),
            _ => self.default_coin(),
        }
    }

    pub fn module_blocked_symbols(&self, module: &str) -> &[String] {
        match module.trim().to_ascii_lowercase().as_str() {
            "fib" | "fib_retracement" => &self.module_symbol_policies.fib_blocked_symbols,
            "copy" | "smart_money" | "smart_money_copy" => {
                &self.module_symbol_policies.copy_blocked_symbols
            }
            _ => {
                if !self
                    .module_symbol_policies
                    .manual_blocked_symbols
                    .is_empty()
                {
                    &self.module_symbol_policies.manual_blocked_symbols
                } else {
                    // Backward compatibility: if no module-specific manual policy is set yet,
                    // respect the legacy manual_ops field (aliased from old `allowed_symbols`).
                    &self.manual_ops.blocked_symbols
                }
            }
        }
    }

    pub fn symbol_allowed_for_module(&self, module: &str, coin: &str) -> bool {
        let blocked = self.module_blocked_symbols(module);
        blocked.is_empty() || !blocked.iter().any(|item| item == coin)
    }

    pub fn account_market_allowed(&self, account_id: &str, market: &str) -> bool {
        self.account(account_id)
            .map(|account| account.market_allowed(market))
            .unwrap_or(false)
    }

    pub fn account_blocked_markets(&self, account_id: &str) -> Vec<String> {
        self.account(account_id)
            .map(AccountConfig::normalized_blocked_markets)
            .unwrap_or_default()
    }
}

impl AccountConfig {
    pub fn normalized_blocked_markets(&self) -> Vec<String> {
        normalize_market_list(&self.blocked_markets)
    }

    pub fn market_allowed(&self, market: &str) -> bool {
        let canonical = normalize_market_id(market).unwrap_or(MARKET_XYZ_PERP);
        !self
            .normalized_blocked_markets()
            .iter()
            .any(|blocked| blocked == canonical)
    }
}

pub const MARKET_XYZ_PERP: &str = "xyz_perp";
pub const MARKET_HL_PERP: &str = "hl_perp";
pub const MARKET_SPOT: &str = "spot";

pub fn supported_market_ids() -> &'static [&'static str] {
    &[MARKET_XYZ_PERP, MARKET_HL_PERP, MARKET_SPOT]
}

pub fn normalize_market_id(raw: &str) -> Option<&'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "xyz" | "xyz_perp" | "xyz-perp" | "trade_xyz" | "trade-xyz" => Some(MARKET_XYZ_PERP),
        "hl" | "hl_perp" | "hl-perp" | "perp" | "default_perp" | "default-perp" => {
            Some(MARKET_HL_PERP)
        }
        "spot" => Some(MARKET_SPOT),
        _ => None,
    }
}

pub fn normalize_market_list(raw: &[String]) -> Vec<String> {
    let mut normalized = Vec::new();
    for item in raw {
        let Some(market) = normalize_market_id(item) else {
            continue;
        };
        let market = market.to_string();
        if !normalized.iter().any(|existing| existing == &market) {
            normalized.push(market);
        }
    }
    normalized
}

fn default_app_name() -> String {
    "trade_xyz_bot".to_string()
}

fn default_environment() -> String {
    "testnet".to_string()
}

fn default_info_url() -> String {
    "https://api.hyperliquid.xyz/info".to_string()
}

fn default_exchange_url() -> String {
    "https://api.hyperliquid.xyz/exchange".to_string()
}

fn default_ws_url() -> String {
    "wss://api.hyperliquid.xyz/ws".to_string()
}

fn default_dex() -> String {
    "xyz".to_string()
}

fn default_role() -> String {
    "coordinator".to_string()
}

fn default_ipc_bind_addr() -> String {
    "127.0.0.1:8788".to_string()
}

fn default_vault_path() -> String {
    "secrets/trade_xyz.vault".to_string()
}

fn default_audit_log_path() -> String {
    "logs/audit.jsonl".to_string()
}

fn default_protective_rules_path() -> String {
    "logs/protective_rules.json".to_string()
}

fn default_worker_heartbeat_ms() -> u64 {
    500
}

fn default_signal_ttl_ms() -> u64 {
    1500
}

fn default_worker_startup_timeout_ms() -> u64 {
    8000
}

fn default_copy_ratio() -> f64 {
    0.10
}

fn default_max_order_notional() -> f64 {
    100.0
}

fn default_confirm_notional() -> f64 {
    100.0
}

fn default_max_manual_batch_accounts() -> usize {
    5
}

fn default_true() -> bool {
    true
}
