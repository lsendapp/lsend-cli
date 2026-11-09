//! Paired-device trust list for the receive path.
//!
//! Stored as `paired.json` in the OS config directory and shared by the CLI
//! and the desktop app. While the list is non-empty, the receiver only accepts
//! prepare-upload requests whose sender fingerprint (as announced by the peer,
//! `fingerprint` or legacy `token` field) appears in the list. An empty or
//! missing list keeps the historical auto-accept-from-anyone behavior so
//! existing automations keep working until pairing is deliberately engaged.
//!
//! Limitation: the fingerprint is the peer's *claimed* value from the
//! prepare-upload body, not the TLS client certificate fingerprint (the axum
//! stack does not surface the peer certificate to handlers). This stops
//! ordinary unpaired LAN peers, but cannot defend against an active spoofer
//! that announces someone else's fingerprint. Binding the claimed identity to
//! the mTLS certificate is tracked with the core-server adoption work.

use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

const PAIRED_FILE: &str = "paired.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairedDevice {
    pub fingerprint: String,
    pub alias: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_model: Option<String>,
    /// Unix seconds when the device was paired.
    pub paired_at: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PairedStore {
    pub devices: Vec<PairedDevice>,
}

impl PairedStore {
    pub fn load(config_dir: &Path) -> Result<Self> {
        let path = config_dir.join(PAIRED_FILE);
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("Failed to read {}", path.display()))?;
        if raw.trim().is_empty() {
            return Ok(Self::default());
        }
        serde_json::from_str(&raw).with_context(|| format!("Failed to parse {}", path.display()))
    }

    pub fn save(&self, config_dir: &Path) -> Result<()> {
        let path = config_dir.join(PAIRED_FILE);
        fs::write(&path, serde_json::to_string_pretty(self)?)
            .with_context(|| format!("Failed to write {}", path.display()))
    }

    pub fn contains(&self, fingerprint: &str) -> bool {
        self.devices
            .iter()
            .any(|d| d.fingerprint.eq_ignore_ascii_case(fingerprint))
    }

    /// Insert or refresh a device. Returns true when a new entry was created.
    pub fn upsert(&mut self, device: PairedDevice) -> bool {
        if let Some(existing) = self
            .devices
            .iter_mut()
            .find(|d| d.fingerprint.eq_ignore_ascii_case(&device.fingerprint))
        {
            existing.alias = device.alias;
            existing.device_model = device.device_model;
            false
        } else {
            self.devices.push(device);
            true
        }
    }

    /// Remove by fingerprint or alias (both case-insensitive). Returns the
    /// removed entry.
    pub fn remove(&mut self, key: &str) -> Option<PairedDevice> {
        let index = self.devices.iter().position(|d| {
            d.fingerprint.eq_ignore_ascii_case(key) || d.alias.eq_ignore_ascii_case(key)
        })?;
        Some(self.devices.remove(index))
    }
}

/// Why a transfer was rejected by the pairing gate.
#[derive(Debug)]
pub enum EnforceError {
    /// The sender is not in a non-empty pair list.
    NotPaired,
    /// `paired.json` exists but cannot be read or parsed. Fails closed: a
    /// corrupted store must not silently disable enforcement.
    StoreUnreadable(anyhow::Error),
}

/// Decide whether `sender_fingerprint` may start a transfer.
///
/// - Empty/missing list → allowed (pairing not engaged).
/// - Fingerprint listed → allowed.
/// - Otherwise → [`EnforceError::NotPaired`].
pub fn enforce(config_dir: &Path, sender_fingerprint: &str) -> Result<(), EnforceError> {
    let store = PairedStore::load(config_dir).map_err(EnforceError::StoreUnreadable)?;
    // An empty list means pairing was never engaged: accept anyone,
    // including peers that announce no fingerprint.
    if store.devices.is_empty() {
        return Ok(());
    }
    if sender_fingerprint.is_empty() {
        return Err(EnforceError::NotPaired);
    }
    if store.contains(sender_fingerprint) {
        Ok(())
    } else {
        Err(EnforceError::NotPaired)
    }
}

pub fn paired_at_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("lsend-pair-{tag}-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn device(fingerprint: &str, alias: &str) -> PairedDevice {
        PairedDevice {
            fingerprint: fingerprint.into(),
            alias: alias.into(),
            device_model: Some("test".into()),
            paired_at: 42,
        }
    }

    #[test]
    fn load_returns_empty_when_file_missing() {
        let dir = fresh_dir("missing");
        let store = PairedStore::load(&dir).unwrap();
        assert!(store.devices.is_empty());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn roundtrip_preserves_entries() {
        let dir = fresh_dir("roundtrip");
        let mut store = PairedStore::default();
        assert!(store.upsert(device("FP1", "Cute Apple")));
        store.save(&dir).unwrap();

        let loaded = PairedStore::load(&dir).unwrap();
        assert_eq!(loaded.devices.len(), 1);
        assert_eq!(loaded.devices[0].alias, "Cute Apple");
        assert_eq!(loaded.devices[0].paired_at, 42);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn upsert_refreshes_existing_without_duplicate() {
        let mut store = PairedStore::default();
        assert!(store.upsert(device("FP1", "Old")));
        assert!(!store.upsert(device("fp1", "New")));
        assert_eq!(store.devices.len(), 1);
        assert_eq!(store.devices[0].alias, "New");
    }

    #[test]
    fn remove_matches_fingerprint_or_alias_case_insensitively() {
        let mut store = PairedStore::default();
        store.upsert(device("ABCDEF", "Cute Apple"));
        let removed = store.remove("cute APPLE").unwrap();
        assert_eq!(removed.fingerprint, "ABCDEF");
        assert!(store.devices.is_empty());

        store.upsert(device("ABCDEF", "Cute Apple"));
        assert!(store.remove("abcdef").is_some());
    }

    #[test]
    fn enforce_allows_anyone_when_list_empty() {
        let dir = fresh_dir("open");
        assert!(enforce(&dir, "whoever").is_ok());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn enforce_rejects_unlisted_sender_and_empty_fingerprint() {
        let dir = fresh_dir("closed");
        let mut store = PairedStore::default();
        store.upsert(device("FP1", "Cute Apple"));
        store.save(&dir).unwrap();

        assert!(enforce(&dir, "FP1").is_ok());
        assert!(enforce(&dir, "fp1").is_ok());
        assert!(matches!(enforce(&dir, "FP2"), Err(EnforceError::NotPaired)));
        assert!(matches!(enforce(&dir, ""), Err(EnforceError::NotPaired)));
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn enforce_accepts_fingerprintless_sender_when_list_empty() {
        let dir = fresh_dir("open-nofp");
        assert!(enforce(&dir, "").is_ok());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn enforce_fails_closed_on_corrupt_store() {
        let dir = fresh_dir("corrupt");
        fs::write(dir.join(PAIRED_FILE), "{not json").unwrap();
        assert!(matches!(
            enforce(&dir, "FP1"),
            Err(EnforceError::StoreUnreadable(_))
        ));
        fs::remove_dir_all(dir).ok();
    }
}
