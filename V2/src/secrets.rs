use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow};
use argon2::{Algorithm, Argon2, Params, Version};
use base64::{Engine, engine::general_purpose::STANDARD_NO_PAD};
use chacha20poly1305::{
    Key, XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit},
};
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::{
    config::{AccountConfig, AppConfig},
    domain::now_ms,
};

const VAULT_AAD: &[u8] = b"trade_xyz_bot.secret_vault.v1";
const SALT_LEN: usize = 16;
const NONCE_LEN: usize = 24;
const KEY_LEN: usize = 32;
const DEFAULT_ARGON2_MEMORY_KIB: u32 = 64 * 1024;
const DEFAULT_ARGON2_ITERATIONS: u32 = 3;
const DEFAULT_ARGON2_PARALLELISM: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultFile {
    pub version: u32,
    pub algorithm: String,
    pub kdf: VaultKdf,
    pub salt_b64: String,
    pub nonce_b64: String,
    pub ciphertext_b64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultKdf {
    pub name: String,
    pub memory_kib: u32,
    pub iterations: u32,
    pub parallelism: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PlainVault {
    version: u32,
    updated_at_ms: u64,
    entries: BTreeMap<String, VaultSecretEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
pub struct VaultSecretEntry {
    pub secret_id: String,
    pub account_id: String,
    pub address: String,
    pub api_wallet_private_key: String,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct SecretUpsert {
    pub secret_id: String,
    pub account_id: String,
    pub address: String,
    pub api_wallet_private_key: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct VaultSummary {
    pub exists: bool,
    pub unlocked: bool,
    pub path: String,
    pub entry_count: Option<usize>,
    pub entries: Vec<VaultEntrySummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct VaultEntrySummary {
    pub secret_id: String,
    pub account_id: String,
    pub address: String,
    pub updated_at_ms: u64,
}

#[derive(Debug, Zeroize, ZeroizeOnDrop)]
pub struct ApiWalletSecret {
    pub secret_id: String,
    pub account_id: String,
    pub private_key: String,
}

#[derive(Debug, Zeroize, ZeroizeOnDrop)]
pub struct TransferWalletSecret {
    pub secret_id: String,
    pub account_id: String,
    pub private_key: String,
    pub signer_address: String,
}

impl VaultKdf {
    pub fn default_interactive() -> Self {
        Self {
            name: "argon2id".to_string(),
            memory_kib: DEFAULT_ARGON2_MEMORY_KIB,
            iterations: DEFAULT_ARGON2_ITERATIONS,
            parallelism: DEFAULT_ARGON2_PARALLELISM,
        }
    }
}

pub fn vault_status(path: &Path) -> VaultSummary {
    VaultSummary {
        exists: path.exists(),
        unlocked: false,
        path: path.display().to_string(),
        entry_count: None,
        entries: Vec::new(),
    }
}

pub fn unlock_vault(path: &Path, password: &str) -> Result<VaultSummary> {
    let plain = decrypt_vault_file(path, password)?;
    Ok(summary_from_plain(path, &plain))
}

pub fn change_vault_password(
    path: &Path,
    current_password: &str,
    new_password: &str,
) -> Result<VaultSummary> {
    validate_password(new_password)?;
    anyhow::ensure!(path.exists(), "vault file does not exist");

    let mut plain = decrypt_vault_file(path, current_password)?;
    plain.updated_at_ms = now_ms();
    write_encrypted_vault(path, new_password, &plain, VaultKdf::default_interactive())?;
    Ok(summary_from_plain(path, &plain))
}

pub fn upsert_secret(path: &Path, password: &str, input: SecretUpsert) -> Result<VaultSummary> {
    validate_password(password)?;
    validate_secret_input(&input)?;

    let mut plain = if path.exists() {
        decrypt_vault_file(path, password)?
    } else {
        PlainVault {
            version: 1,
            updated_at_ms: now_ms(),
            entries: BTreeMap::new(),
        }
    };

    let entry = VaultSecretEntry {
        secret_id: input.secret_id,
        account_id: input.account_id,
        address: input.address,
        api_wallet_private_key: normalize_private_key(&input.api_wallet_private_key)?,
        updated_at_ms: now_ms(),
    };
    plain.updated_at_ms = now_ms();
    plain.entries.insert(entry.secret_id.clone(), entry);

    write_encrypted_vault(path, password, &plain, VaultKdf::default_interactive())?;
    Ok(summary_from_plain(path, &plain))
}

pub fn load_account_secret(
    config: &AppConfig,
    account: &AccountConfig,
    password: Option<&str>,
) -> Result<ApiWalletSecret> {
    if let Some(password) = password {
        let vault_path = PathBuf::from(&config.secrets.vault_path);
        let secret_id = account_secret_id(account);
        return load_secret_by_id(&vault_path, password, &secret_id, Some(&account.account_id));
    }

    if config.secrets.allow_env_fallback && !account.api_wallet_env.trim().is_empty() {
        let private_key = std::env::var(&account.api_wallet_env).with_context(|| {
            format!(
                "environment variable {} is not set for account {}",
                account.api_wallet_env, account.account_id
            )
        })?;
        return Ok(ApiWalletSecret {
            secret_id: account_secret_id(account),
            account_id: account.account_id.clone(),
            private_key: normalize_private_key(&private_key)?,
        });
    }

    anyhow::bail!(
        "account {} requires vault password for secret_id {}",
        account.account_id,
        account_secret_id(account)
    )
}

pub fn load_transfer_secret(
    config: &AppConfig,
    account: &AccountConfig,
    password: Option<&str>,
) -> Result<TransferWalletSecret> {
    let raw_secret = if let Some(password) = password {
        let vault_path = PathBuf::from(&config.secrets.vault_path);
        let secret_id = transfer_secret_id(account);
        load_secret_by_id(&vault_path, password, &secret_id, Some(&account.account_id))?
    } else if config.secrets.allow_env_fallback && !account.transfer_wallet_env.trim().is_empty() {
        let private_key = std::env::var(&account.transfer_wallet_env).with_context(|| {
            format!(
                "environment variable {} is not set for account {}",
                account.transfer_wallet_env, account.account_id
            )
        })?;
        ApiWalletSecret {
            secret_id: transfer_secret_id(account),
            account_id: account.account_id.clone(),
            private_key: normalize_private_key(&private_key)?,
        }
    } else {
        anyhow::bail!(
            "account {} requires vault password for transfer_secret_id {}",
            account.account_id,
            transfer_secret_id(account)
        );
    };

    let signer_address = private_key_address(&raw_secret.private_key)?;
    anyhow::ensure!(
        signer_address.eq_ignore_ascii_case(account.address.trim()),
        "transfer signer {} does not match configured EVM account address {} for {}; API wallets cannot be used for USDC funding transfers",
        signer_address,
        account.address,
        account.account_id
    );
    Ok(TransferWalletSecret {
        secret_id: raw_secret.secret_id.clone(),
        account_id: raw_secret.account_id.clone(),
        private_key: raw_secret.private_key.clone(),
        signer_address,
    })
}

pub fn load_secret_by_id(
    path: &Path,
    password: &str,
    secret_id: &str,
    expected_account_id: Option<&str>,
) -> Result<ApiWalletSecret> {
    let plain = decrypt_vault_file(path, password)?;
    let entry = plain
        .entries
        .get(secret_id)
        .with_context(|| format!("secret_id {secret_id} not found in vault"))?;
    if let Some(expected_account_id) = expected_account_id {
        anyhow::ensure!(
            entry.account_id == expected_account_id,
            "secret_id {} belongs to account {}, not {}",
            secret_id,
            entry.account_id,
            expected_account_id
        );
    }
    Ok(ApiWalletSecret {
        secret_id: secret_id.to_string(),
        account_id: entry.account_id.clone(),
        private_key: entry.api_wallet_private_key.clone(),
    })
}

pub fn account_secret_id(account: &AccountConfig) -> String {
    if account.secret_id.trim().is_empty() {
        account.account_id.clone()
    } else {
        account.secret_id.clone()
    }
}

pub fn transfer_secret_id(account: &AccountConfig) -> String {
    if account.transfer_secret_id.trim().is_empty() {
        account_secret_id(account)
    } else {
        account.transfer_secret_id.clone()
    }
}

pub fn private_key_address(private_key: &str) -> Result<String> {
    use ethers::signers::{LocalWallet, Signer};

    let wallet: LocalWallet = normalize_private_key(private_key)?
        .parse()
        .context("failed to parse private key for address derivation")?;
    Ok(format!("{:#x}", wallet.address()))
}

pub fn account_has_dedicated_transfer_secret(account: &AccountConfig) -> bool {
    !account.transfer_secret_id.trim().is_empty() || !account.transfer_wallet_env.trim().is_empty()
}

fn write_encrypted_vault(
    path: &Path,
    password: &str,
    plain: &PlainVault,
    kdf: VaultKdf,
) -> Result<()> {
    let parent = path
        .parent()
        .with_context(|| format!("vault path {} has no parent directory", path.display()))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create vault directory {}", parent.display()))?;

    let mut salt = [0u8; SALT_LEN];
    let mut nonce = [0u8; NONCE_LEN];
    OsRng.fill_bytes(&mut salt);
    OsRng.fill_bytes(&mut nonce);

    let mut key = derive_key(password, &salt, &kdf)?;
    let cipher = XChaCha20Poly1305::new(Key::from_slice(&key));
    let plaintext = serde_json::to_vec(plain).context("failed to serialize vault plaintext")?;
    let ciphertext = cipher
        .encrypt(
            XNonce::from_slice(&nonce),
            chacha20poly1305::aead::Payload {
                msg: &plaintext,
                aad: VAULT_AAD,
            },
        )
        .map_err(|_| anyhow!("failed to encrypt vault"))?;
    key.zeroize();

    let vault_file = VaultFile {
        version: 1,
        algorithm: "xchacha20poly1305".to_string(),
        kdf,
        salt_b64: STANDARD_NO_PAD.encode(salt),
        nonce_b64: STANDARD_NO_PAD.encode(nonce),
        ciphertext_b64: STANDARD_NO_PAD.encode(ciphertext),
    };

    let encoded =
        serde_json::to_vec_pretty(&vault_file).context("failed to serialize vault file")?;
    fs::write(path, encoded)
        .with_context(|| format!("failed to write vault {}", path.display()))?;
    Ok(())
}

fn decrypt_vault_file(path: &Path, password: &str) -> Result<PlainVault> {
    validate_password(password)?;
    let raw = fs::read(path).with_context(|| format!("failed to read vault {}", path.display()))?;
    let vault_file =
        serde_json::from_slice::<VaultFile>(&raw).context("failed to parse vault file")?;
    anyhow::ensure!(vault_file.version == 1, "unsupported vault version");
    anyhow::ensure!(
        vault_file.algorithm == "xchacha20poly1305",
        "unsupported vault algorithm"
    );
    anyhow::ensure!(vault_file.kdf.name == "argon2id", "unsupported vault kdf");

    let salt = decode_fixed::<SALT_LEN>(&vault_file.salt_b64, "salt")?;
    let nonce = decode_fixed::<NONCE_LEN>(&vault_file.nonce_b64, "nonce")?;
    let ciphertext = STANDARD_NO_PAD
        .decode(vault_file.ciphertext_b64)
        .context("failed to decode vault ciphertext")?;
    let mut key = derive_key(password, &salt, &vault_file.kdf)?;
    let cipher = XChaCha20Poly1305::new(Key::from_slice(&key));
    let plaintext = cipher
        .decrypt(
            XNonce::from_slice(&nonce),
            chacha20poly1305::aead::Payload {
                msg: &ciphertext,
                aad: VAULT_AAD,
            },
        )
        .map_err(|_| {
            anyhow!("failed to decrypt vault; password may be wrong or file may be damaged")
        })?;
    key.zeroize();

    let plain = serde_json::from_slice::<PlainVault>(&plaintext)
        .context("failed to parse vault plaintext")?;
    Ok(plain)
}

fn derive_key(password: &str, salt: &[u8], kdf: &VaultKdf) -> Result<[u8; KEY_LEN]> {
    let params = Params::new(
        kdf.memory_kib,
        kdf.iterations,
        kdf.parallelism,
        Some(KEY_LEN),
    )
    .map_err(|error| anyhow!("invalid argon2 parameters: {error:?}"))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut key = [0u8; KEY_LEN];
    argon2
        .hash_password_into(password.as_bytes(), salt, &mut key)
        .map_err(|error| anyhow!("failed to derive vault key: {error:?}"))?;
    Ok(key)
}

fn decode_fixed<const N: usize>(encoded: &str, label: &str) -> Result<[u8; N]> {
    let bytes = STANDARD_NO_PAD
        .decode(encoded)
        .with_context(|| format!("failed to decode vault {label}"))?;
    let fixed: [u8; N] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("vault {label} has wrong length"))?;
    Ok(fixed)
}

fn summary_from_plain(path: &Path, plain: &PlainVault) -> VaultSummary {
    VaultSummary {
        exists: path.exists(),
        unlocked: true,
        path: path.display().to_string(),
        entry_count: Some(plain.entries.len()),
        entries: plain
            .entries
            .values()
            .map(|entry| VaultEntrySummary {
                secret_id: entry.secret_id.clone(),
                account_id: entry.account_id.clone(),
                address: entry.address.clone(),
                updated_at_ms: entry.updated_at_ms,
            })
            .collect(),
    }
}

fn validate_password(password: &str) -> Result<()> {
    anyhow::ensure!(
        password.chars().count() >= 10,
        "vault password must be at least 10 characters"
    );
    Ok(())
}

fn validate_secret_input(input: &SecretUpsert) -> Result<()> {
    anyhow::ensure!(
        !input.secret_id.trim().is_empty(),
        "secret_id cannot be empty"
    );
    anyhow::ensure!(
        !input.account_id.trim().is_empty(),
        "account_id cannot be empty"
    );
    anyhow::ensure!(!input.address.trim().is_empty(), "address cannot be empty");
    normalize_private_key(&input.api_wallet_private_key)?;
    Ok(())
}

fn normalize_private_key(private_key: &str) -> Result<String> {
    let trimmed = private_key.trim();
    let hex = trimmed.strip_prefix("0x").unwrap_or(trimmed);
    anyhow::ensure!(hex.len() == 64, "private key must be 32 bytes hex");
    anyhow::ensure!(
        hex.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()),
        "private key contains non-hex characters"
    );
    Ok(format!("0x{}", hex.to_ascii_lowercase()))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::config::{AccountConfig, AppConfig};

    use super::{
        PlainVault, SecretUpsert, VaultKdf, change_vault_password, decrypt_vault_file,
        load_account_secret, load_secret_by_id, load_transfer_secret, private_key_address,
        summary_from_plain, upsert_secret, write_encrypted_vault,
    };

    #[test]
    fn upsert_and_unlock_vault_without_plaintext_leak() {
        let dir =
            std::env::temp_dir().join(format!("trade_xyz_vault_test_{}", crate::domain::now_ms()));
        fs::create_dir_all(&dir).expect("test dir");
        let path = dir.join("trade_xyz.vault");
        let password = "correct horse battery";
        let private_key = "0x1111111111111111111111111111111111111111111111111111111111111111";

        let summary = upsert_secret(
            &path,
            password,
            SecretUpsert {
                secret_id: "addr_a_api_wallet".to_string(),
                account_id: "addr_a".to_string(),
                address: "0x0000000000000000000000000000000000000001".to_string(),
                api_wallet_private_key: private_key.to_string(),
            },
        )
        .expect("vault upsert");

        assert!(summary.exists);
        assert_eq!(summary.entry_count, Some(1));
        let raw = fs::read_to_string(&path).expect("vault file");
        assert!(!raw.contains(private_key));

        let unlocked = decrypt_vault_file(&path, password).expect("unlock vault");
        let summary = summary_from_plain(&path, &unlocked);
        assert_eq!(summary.entries[0].secret_id, "addr_a_api_wallet");
        assert!(decrypt_vault_file(&path, "wrong password").is_err());
    }

    #[test]
    fn low_cost_kdf_round_trip_for_format_stability() {
        let dir = std::env::temp_dir().join(format!(
            "trade_xyz_vault_kdf_test_{}",
            crate::domain::now_ms()
        ));
        fs::create_dir_all(&dir).expect("test dir");
        let path = dir.join("trade_xyz.vault");
        let plain = PlainVault {
            version: 1,
            updated_at_ms: crate::domain::now_ms(),
            entries: Default::default(),
        };

        write_encrypted_vault(
            &path,
            "format password",
            &plain,
            VaultKdf {
                name: "argon2id".to_string(),
                memory_kib: 1024,
                iterations: 1,
                parallelism: 1,
            },
        )
        .expect("write low cost vault");

        let unlocked = decrypt_vault_file(&path, "format password").expect("unlock");
        assert_eq!(unlocked.version, 1);
    }

    #[test]
    fn load_account_secret_uses_configured_secret_id() {
        let dir = std::env::temp_dir().join(format!(
            "trade_xyz_vault_lookup_test_{}",
            crate::domain::now_ms()
        ));
        fs::create_dir_all(&dir).expect("test dir");
        let path = dir.join("trade_xyz.vault");
        let password = "lookup password";
        let private_key = "0x2222222222222222222222222222222222222222222222222222222222222222";

        upsert_secret(
            &path,
            password,
            SecretUpsert {
                secret_id: "addr_a_api_wallet".to_string(),
                account_id: "addr_a".to_string(),
                address: "0x0000000000000000000000000000000000000001".to_string(),
                api_wallet_private_key: private_key.to_string(),
            },
        )
        .expect("vault upsert");

        let mut config = AppConfig::default();
        config.secrets.vault_path = path.to_string_lossy().into_owned();
        let account = AccountConfig {
            account_id: "addr_a".to_string(),
            address: "0x0000000000000000000000000000000000000001".to_string(),
            secret_id: "addr_a_api_wallet".to_string(),
            api_wallet_env: String::new(),
            transfer_secret_id: String::new(),
            transfer_wallet_env: String::new(),
            enabled: true,
            worker_enabled: true,
            copy_ratio: 0.1,
            max_order_notional_usd: 100.0,
            blocked_markets: Vec::new(),
        };

        let secret =
            load_account_secret(&config, &account, Some(password)).expect("load account secret");
        assert_eq!(secret.secret_id, "addr_a_api_wallet");
        assert_eq!(secret.account_id, "addr_a");
        assert_eq!(secret.private_key, private_key);
    }

    #[test]
    fn transfer_secret_requires_evm_signer_matching_account_address() {
        let dir = std::env::temp_dir().join(format!(
            "trade_xyz_transfer_secret_test_{}",
            crate::domain::now_ms()
        ));
        fs::create_dir_all(&dir).expect("test dir");
        let path = dir.join("trade_xyz.vault");
        let password = "transfer lookup password";
        let api_private_key = "0x2222222222222222222222222222222222222222222222222222222222222222";
        let evm_private_key = "0x3333333333333333333333333333333333333333333333333333333333333333";
        let evm_address = private_key_address(evm_private_key).expect("derive evm address");

        upsert_secret(
            &path,
            password,
            SecretUpsert {
                secret_id: "addr_a_api_wallet".to_string(),
                account_id: "addr_a".to_string(),
                address: evm_address.clone(),
                api_wallet_private_key: api_private_key.to_string(),
            },
        )
        .expect("api vault upsert");
        upsert_secret(
            &path,
            password,
            SecretUpsert {
                secret_id: "addr_a_transfer_wallet".to_string(),
                account_id: "addr_a".to_string(),
                address: evm_address.clone(),
                api_wallet_private_key: evm_private_key.to_string(),
            },
        )
        .expect("transfer vault upsert");

        let mut config = AppConfig::default();
        config.secrets.vault_path = path.to_string_lossy().into_owned();
        let mut account = AccountConfig {
            account_id: "addr_a".to_string(),
            address: evm_address.clone(),
            secret_id: "addr_a_api_wallet".to_string(),
            api_wallet_env: String::new(),
            transfer_secret_id: String::new(),
            transfer_wallet_env: String::new(),
            enabled: true,
            worker_enabled: true,
            copy_ratio: 0.1,
            max_order_notional_usd: 100.0,
            blocked_markets: Vec::new(),
        };

        let legacy_error = load_transfer_secret(&config, &account, Some(password))
            .expect_err("api wallet fallback must not pass transfer signer check")
            .to_string();
        assert!(legacy_error.contains("API wallets cannot be used"));

        account.transfer_secret_id = "addr_a_transfer_wallet".to_string();
        let secret =
            load_transfer_secret(&config, &account, Some(password)).expect("load transfer secret");
        assert_eq!(secret.secret_id, "addr_a_transfer_wallet");
        assert_eq!(secret.signer_address, evm_address.to_ascii_lowercase());
    }

    #[test]
    fn load_secret_by_id_supports_vault_only_accounts() {
        let dir = std::env::temp_dir().join(format!(
            "trade_xyz_vault_custom_lookup_test_{}",
            crate::domain::now_ms()
        ));
        fs::create_dir_all(&dir).expect("test dir");
        let path = dir.join("trade_xyz.vault");
        let password = "custom lookup password";
        let private_key = "0x3333333333333333333333333333333333333333333333333333333333333333";

        upsert_secret(
            &path,
            password,
            SecretUpsert {
                secret_id: "addr_c_api_wallet".to_string(),
                account_id: "addr_c".to_string(),
                address: "0x0000000000000000000000000000000000000003".to_string(),
                api_wallet_private_key: private_key.to_string(),
            },
        )
        .expect("vault upsert");

        let secret = load_secret_by_id(&path, password, "addr_c_api_wallet", Some("addr_c"))
            .expect("load custom secret");
        assert_eq!(secret.secret_id, "addr_c_api_wallet");
        assert_eq!(secret.account_id, "addr_c");
        assert_eq!(secret.private_key, private_key);
        assert!(load_secret_by_id(&path, password, "addr_c_api_wallet", Some("addr_a")).is_err());
    }

    #[test]
    fn change_vault_password_preserves_entries_and_rotates_key() {
        let dir = std::env::temp_dir().join(format!(
            "trade_xyz_vault_password_change_test_{}",
            crate::domain::now_ms()
        ));
        fs::create_dir_all(&dir).expect("test dir");
        let path = dir.join("trade_xyz.vault");
        let old_password = "old password value";
        let new_password = "new password value";
        let private_key = "0x4444444444444444444444444444444444444444444444444444444444444444";

        upsert_secret(
            &path,
            old_password,
            SecretUpsert {
                secret_id: "addr_d_api_wallet".to_string(),
                account_id: "addr_d".to_string(),
                address: "0x0000000000000000000000000000000000000004".to_string(),
                api_wallet_private_key: private_key.to_string(),
            },
        )
        .expect("vault upsert");

        let summary =
            change_vault_password(&path, old_password, new_password).expect("change password");
        assert_eq!(summary.entry_count, Some(1));
        assert!(decrypt_vault_file(&path, old_password).is_err());

        let secret = load_secret_by_id(&path, new_password, "addr_d_api_wallet", Some("addr_d"))
            .expect("load with new password");
        assert_eq!(secret.private_key, private_key);
    }
}
