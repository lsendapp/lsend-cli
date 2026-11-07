use std::collections::HashMap;
use std::time::Duration;

use localsend::model::discovery::ProtocolType;
use serde::Deserialize;
use tokio::time::timeout;
use tracing::info;

use crate::config::AppConfig;
use crate::discovery::DiscoveredDevice;
use crate::identity::Identity;
use crate::network::{build_reqwest_client, list_ipv4_interfaces};
use crate::util::parse_device_type;

const MAX_SUBNETS: usize = 3;
const SCAN_CONCURRENCY: usize = 50;
const PROBE_TIMEOUT: Duration = Duration::from_millis(750);

pub async fn legacy_http_scan(
    config: &AppConfig,
    identity: &Identity,
) -> anyhow::Result<Vec<DiscoveredDevice>> {
    let raw = build_reqwest_client(identity, config.https)?;

    let interfaces = list_ipv4_interfaces();
    if interfaces.is_empty() {
        return Ok(Vec::new());
    }

    info!(
        "Multicast found no devices; running HTTP subnet scan on {} interface(s)",
        interfaces.len().min(MAX_SUBNETS)
    );

    let mut targets = Vec::new();
    for iface in interfaces.into_iter().take(MAX_SUBNETS) {
        targets.extend(subnet_hosts(iface));
    }

    let mut found = HashMap::<String, DiscoveredDevice>::new();
    let port = config.port;
    let fingerprint = identity.fingerprint.clone();
    let prefer_https = config.https;

    for chunk in targets.chunks(SCAN_CONCURRENCY) {
        let mut probes = Vec::with_capacity(chunk.len());
        for ip in chunk {
            probes.push(probe_device(&raw, ip, port, prefer_https, &fingerprint));
        }
        for device in futures_util::future::join_all(probes)
            .await
            .into_iter()
            .flatten()
        {
            found.insert(device.fingerprint.clone(), device);
        }
    }

    Ok(found.into_values().collect())
}

async fn probe_device(
    client: &localsend::reqwest::Client,
    ip: &str,
    port: u16,
    prefer_https: bool,
    fingerprint: &str,
) -> Option<DiscoveredDevice> {
    let protocols = if prefer_https {
        [ProtocolType::Https, ProtocolType::Http]
    } else {
        [ProtocolType::Http, ProtocolType::Https]
    };

    for protocol in protocols {
        if let Some(device) = probe_info(client, protocol, ip, port, fingerprint).await {
            return Some(device);
        }
    }

    None
}

async fn probe_info(
    client: &localsend::reqwest::Client,
    protocol: ProtocolType,
    ip: &str,
    port: u16,
    fingerprint: &str,
) -> Option<DiscoveredDevice> {
    let https = protocol == ProtocolType::Https;
    let url = format!(
        "{}://{ip}:{port}/api/localsend/v1/info?fingerprint={fingerprint}",
        protocol.as_str()
    );

    let response = timeout(PROBE_TIMEOUT, client.get(&url).send())
        .await
        .ok()
        .and_then(|r| r.ok())?;

    if !response.status().is_success() {
        return None;
    }

    let info: InfoResponseCompat = response.json().await.ok()?;
    Some(device_from_info(info, ip, port, https))
}

fn device_from_info(
    info: InfoResponseCompat,
    ip: &str,
    port: u16,
    https: bool,
) -> DiscoveredDevice {
    DiscoveredDevice {
        alias: info.alias,
        ip: ip.to_string(),
        port,
        fingerprint: info.fingerprint,
        https,
        version: info
            .version
            .unwrap_or_else(|| crate::config::PROTOCOL_VERSION.to_string()),
        device_type: info.device_type.as_deref().and_then(parse_device_type),
        device_model: info.device_model,
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InfoResponseCompat {
    alias: String,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    device_model: Option<String>,
    #[serde(default)]
    device_type: Option<String>,
    fingerprint: String,
}

fn subnet_hosts(iface: std::net::Ipv4Addr) -> Vec<String> {
    let octets = iface.octets();
    let prefix = format!("{}.{}.{}", octets[0], octets[1], octets[2]);
    let own = octets[3];

    (1..=254)
        .filter(|host| *host != own)
        .map(|host| format!("{prefix}.{host}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_official_info_json() {
        let json = r#"{"alias":"Happy Orange","version":"2.1","deviceModel":"macOS","deviceType":"desktop","fingerprint":"abc123","download":false}"#;
        let info: InfoResponseCompat = serde_json::from_str(json).expect("parse");
        assert_eq!(info.alias, "Happy Orange");
        assert_eq!(info.device_type.as_deref(), Some("desktop"));
    }

    #[test]
    fn subnet_hosts_excludes_self_and_returns_253() {
        let hosts = subnet_hosts(std::net::Ipv4Addr::new(192, 168, 1, 50));
        assert_eq!(hosts.len(), 253);
        assert!(!hosts.contains(&"192.168.1.50".to_string()));
        assert!(hosts.contains(&"192.168.1.1".to_string()));
        assert!(hosts.contains(&"192.168.1.254".to_string()));
        assert!(!hosts.contains(&"192.168.1.0".to_string()));
        assert!(!hosts.contains(&"192.168.1.255".to_string()));
    }

    #[test]
    fn subnet_hosts_when_iface_is_first_host_skips_to_2() {
        // 10.0.0.1 is excluded; first generated host is 10.0.0.2.
        let hosts = subnet_hosts(std::net::Ipv4Addr::new(10, 0, 0, 1));
        assert_eq!(hosts.first(), Some(&"10.0.0.2".to_string()));
        assert_eq!(hosts.last(), Some(&"10.0.0.254".to_string()));
    }

    #[test]
    fn info_compat_defaults_optional_fields() {
        // fingerprint is required by the schema; only optional fields default.
        let json = r#"{"alias":"X","fingerprint":"fp"}"#;
        let info: InfoResponseCompat = serde_json::from_str(json).expect("parse");
        assert_eq!(info.alias, "X");
        assert_eq!(info.fingerprint, "fp");
        assert!(info.version.is_none());
        assert!(info.device_model.is_none());
        assert!(info.device_type.is_none());
    }

    #[test]
    fn device_from_info_uses_provided_endpoint() {
        let info = InfoResponseCompat {
            alias: "Bob".into(),
            version: Some("2.1".into()),
            device_model: Some("iPhone".into()),
            device_type: Some("mobile".into()),
            fingerprint: "bob-fp".into(),
        };
        let device = device_from_info(info, "10.0.0.7", 12345, true);
        assert_eq!(device.alias, "Bob");
        assert_eq!(device.ip, "10.0.0.7");
        assert_eq!(device.port, 12345);
        assert!(device.https);
        assert_eq!(device.fingerprint, "bob-fp");
        assert_eq!(device.device_model.as_deref(), Some("iPhone"));
        use localsend::model::discovery::DeviceType;
        assert_eq!(device.device_type, Some(DeviceType::Mobile));
    }

    #[test]
    fn device_from_info_falls_back_to_current_protocol_version() {
        let info = InfoResponseCompat {
            alias: "Old".into(),
            version: None,
            device_model: None,
            device_type: None,
            fingerprint: "old-fp".into(),
        };
        let device = device_from_info(info, "10.0.0.8", 53317, false);
        // Missing version falls back to the current protocol version (2.1),
        // not to the v1.0 fallback used by the Dart InfoDto helper.
        assert_eq!(device.version, "2.1");
        assert_eq!(device.device_type, None);
        assert!(!device.https);
    }
}
