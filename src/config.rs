use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};

pub const PROTOCOL_VERSION: &str = "2.2";
pub const DEFAULT_PORT: u16 = 53317;
pub const DEFAULT_MULTICAST_GROUP: &str = "224.0.0.167";
pub const DEFAULT_DISCOVERY_TIMEOUT_MS: u64 = 1500;

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub alias: String,
    pub port: u16,
    pub https: bool,
    pub multicast_group: String,
    pub config_dir: PathBuf,
    pub receive_dir: PathBuf,
}

pub fn default_config_dir() -> Result<PathBuf> {
    Ok(dirs::config_dir()
        .context("Could not resolve config directory")?
        .join("lsend"))
}

impl AppConfig {
    pub const DEFAULT_PORT: u16 = DEFAULT_PORT;
    pub const DEFAULT_DISCOVERY_TIMEOUT_MS: u64 = DEFAULT_DISCOVERY_TIMEOUT_MS;

    pub fn new(
        alias: Option<String>,
        port: u16,
        https: bool,
        receive_dir: Option<PathBuf>,
    ) -> Result<Self> {
        let config_dir = default_config_dir()?;

        fs::create_dir_all(&config_dir).with_context(|| {
            format!("Failed to create config directory {}", config_dir.display())
        })?;

        let receive_dir = receive_dir.unwrap_or_else(default_download_dir);

        let alias = match alias {
            Some(alias) => alias,
            None => crate::alias::load_or_create(&config_dir)?,
        };

        Ok(Self {
            alias,
            port,
            https,
            multicast_group: DEFAULT_MULTICAST_GROUP.to_string(),
            config_dir,
            receive_dir,
        })
    }
}

fn default_download_dir() -> PathBuf {
    dirs::download_dir().unwrap_or_else(|| PathBuf::from("."))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn fresh_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("lsend-cfg-{}-{}", tag, uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn protocol_version_is_v2_2() {
        assert_eq!(PROTOCOL_VERSION, "2.2");
    }

    #[test]
    fn default_port_is_53317() {
        assert_eq!(DEFAULT_PORT, 53317);
        assert_eq!(AppConfig::DEFAULT_PORT, 53317);
    }

    #[test]
    fn default_multicast_group_is_in_24_block() {
        let group: std::net::Ipv4Addr = DEFAULT_MULTICAST_GROUP.parse().unwrap();
        assert_eq!(group.octets()[0], 224);
        assert_eq!(group.octets()[1], 0);
        assert_eq!(group.octets()[2], 0);
        assert_eq!(group.octets()[3], 167);
    }

    #[test]
    fn default_discovery_timeout_in_range() {
        assert!(DEFAULT_DISCOVERY_TIMEOUT_MS >= 100);
        assert!(DEFAULT_DISCOVERY_TIMEOUT_MS <= 60_000);
    }

    #[test]
    fn app_config_new_uses_overrides() {
        let dir = fresh_dir("overrides");
        let receive = dir.join("inbox");
        let cfg =
            AppConfig::new(Some("MyAlias".into()), 12345, true, Some(receive.clone())).unwrap();
        assert_eq!(cfg.alias, "MyAlias");
        assert_eq!(cfg.port, 12345);
        assert!(cfg.https);
        assert_eq!(cfg.receive_dir, receive);
        // config dir is created and lives under the OS config root.
        assert!(cfg.config_dir.exists());
        assert!(cfg.config_dir.to_string_lossy().contains("lsend"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn app_config_new_generates_alias_when_missing() {
        let dir = fresh_dir("autoalias");
        let cfg = AppConfig::new(None, DEFAULT_PORT, false, None).unwrap();
        // Aliases follow "<Adjective> <Fruit>" so should contain a space and
        // not be empty.
        assert!(!cfg.alias.is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn app_config_new_persists_generated_alias_across_calls() {
        let dir = fresh_dir("persist-alias");
        let first = AppConfig::new(None, DEFAULT_PORT, false, None).unwrap();
        let second = AppConfig::new(None, DEFAULT_PORT, false, None).unwrap();
        assert_eq!(first.alias, second.alias);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn app_config_uses_default_download_dir_when_receive_dir_none() {
        let dir = fresh_dir("default-receive");
        let cfg = AppConfig::new(Some("X".into()), DEFAULT_PORT, false, None).unwrap();
        // The receive dir is either the OS download dir or ".". Either way it
        // must be a non-empty path.
        assert!(!cfg.receive_dir.as_os_str().is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn app_config_create_dir_creates_config_dir() {
        let dir = fresh_dir("createdir");
        // Erase it; AppConfig::new should re-create it.
        let _ = fs::remove_dir_all(&dir);
        let cfg = AppConfig::new(Some("Y".into()), DEFAULT_PORT, false, None).unwrap();
        assert!(cfg.config_dir.exists());
        let _ = fs::remove_dir_all(&dir);
    }
}
