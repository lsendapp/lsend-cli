use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use localsend::http::dto::RegisterDto;
use localsend::model::discovery::{DeviceType, ProtocolType};
use serde::Deserialize;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};
use tracing::{debug, info, warn};

use crate::config::{AppConfig, DEFAULT_MULTICAST_GROUP};
use crate::identity::Identity;
use crate::legacy::legacy_http_scan;
use crate::network::{build_http_client, list_ipv4_interfaces};
use crate::scan_server::start as start_discovery_server;

#[derive(Clone, Debug)]
pub struct DiscoveredDevice {
    pub alias: String,
    pub ip: String,
    pub port: u16,
    pub fingerprint: String,
    pub https: bool,
    pub version: String,
    pub device_type: Option<DeviceType>,
    pub device_model: Option<String>,
}

impl DiscoveredDevice {
    pub fn display_label(&self) -> String {
        format!(
            "{} ({}{}:{})",
            self.alias,
            if self.https { "https://" } else { "http://" },
            self.ip,
            self.port
        )
    }
}

/// How the total `--timeout` budget is split between the multicast phase and
/// the legacy HTTP subnet fallback. `--timeout` bounds the WHOLE scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanBudget {
    /// Milliseconds for announcement bursts + multicast listen.
    pub multicast_ms: u64,
    /// Milliseconds reserved at the tail for the HTTP subnet fallback. Zero
    /// means the fallback cannot run within this budget and is skipped.
    pub fallback_ms: u64,
}

/// Announcement burst schedule (matches the LocalSend app).
const ANNOUNCE_DELAYS_MS: [u64; 3] = [100, 500, 2000];
/// Floor so a near-zero timeout still leaves time for one burst + listen.
const MIN_SCAN_BUDGET_MS: u64 = 200;
/// Tail reserve for the HTTP fallback; only engaged on generous budgets.
const FALLBACK_RESERVE_MS: u64 = 2000;
/// Budgets below this spend everything on multicast (an HTTP subnet sweep
/// cannot fit, so promising one would be a lie).
const FALLBACK_MIN_TOTAL_MS: u64 = 4000;

pub fn split_scan_budget(timeout_ms: u64) -> ScanBudget {
    let total = timeout_ms.max(MIN_SCAN_BUDGET_MS);
    let fallback_ms = if total >= FALLBACK_MIN_TOTAL_MS {
        FALLBACK_RESERVE_MS.min(total / 2)
    } else {
        0
    };
    ScanBudget {
        multicast_ms: total - fallback_ms,
        fallback_ms,
    }
}

pub async fn scan(
    config: &AppConfig,
    identity: &Identity,
    timeout_ms: u64,
) -> Result<Vec<DiscoveredDevice>> {
    scan_inner(config, identity, timeout_ms, true).await
}

/// Scan without starting the temporary discovery HTTP server.
///
/// Resident applications already own the LocalSend TCP port for receiving, so
/// attempting to bind a second server creates a startup race. UDP discovery and
/// the legacy HTTP fallback still find peers without competing for that port.
pub async fn scan_passive(
    config: &AppConfig,
    identity: &Identity,
    timeout_ms: u64,
) -> Result<Vec<DiscoveredDevice>> {
    scan_inner(config, identity, timeout_ms, false).await
}

async fn scan_inner(
    config: &AppConfig,
    identity: &Identity,
    timeout_ms: u64,
    start_http_server: bool,
) -> Result<Vec<DiscoveredDevice>> {
    let started = std::time::Instant::now();
    let budget = split_scan_budget(timeout_ms);
    let devices = Arc::new(Mutex::new(HashMap::<String, DiscoveredDevice>::new()));
    let multicast_group: Ipv4Addr = DEFAULT_MULTICAST_GROUP.parse()?;

    let discovery_server = if start_http_server {
        match start_discovery_server(config.clone(), identity.clone(), devices.clone()).await {
            Ok(handle) => Some(handle),
            Err(e) => {
                warn!(
                    "Could not start discovery HTTP server on port {}: {e}. \
                     Close any other process holding port {} (e.g. the LocalSend app, another `lsend receive`), \
                     or use another --port. Peers may only respond via UDP.",
                    config.port, config.port
                );
                None
            }
        }
    } else {
        None
    };

    let listen_sockets = open_multicast_listen_sockets(config.port, multicast_group)
        .context("Failed to open multicast listen sockets")?;
    let announce_sockets = open_multicast_announce_sockets(multicast_group);

    debug!(
        "Scan: {} listen socket(s), {} announce socket(s), port {}, https={}, budget {}ms multicast + {}ms fallback",
        listen_sockets.len(),
        announce_sockets.len(),
        config.port,
        config.https,
        budget.multicast_ms,
        budget.fallback_ms
    );

    let listener = if listen_sockets.is_empty() {
        None
    } else {
        Some(
            start_multicast_listener(
                config.clone(),
                identity.clone(),
                Some(devices.clone()),
                listen_sockets,
            )
            .await?,
        )
    };

    // Announce concurrently with listening; bursts that no longer fit before
    // the multicast deadline are skipped so the scan honors --timeout.
    let multicast_deadline = started + Duration::from_millis(budget.multicast_ms);
    let mut announce_task = None;
    if !announce_sockets.is_empty() {
        announce_task = Some(tokio::spawn(send_announcement(
            announce_sockets,
            config.clone(),
            identity.clone(),
            multicast_group,
            Some(multicast_deadline),
        )));
    }

    tokio::time::sleep_until(tokio::time::Instant::from_std(multicast_deadline)).await;

    if let Some(task) = announce_task {
        task.abort();
    }
    if let Some(listener) = listener {
        drop(listener);
    }
    if let Some(server) = discovery_server {
        server.abort();
    }

    let mut found: Vec<_> = devices.lock().await.values().cloned().collect();
    if found.is_empty() && budget.fallback_ms > 0 {
        let fallback_deadline =
            started + Duration::from_millis(budget.multicast_ms + budget.fallback_ms);
        found = legacy_http_scan(config, identity, fallback_deadline).await?;
    }

    if found.is_empty() {
        info!("Scan finished with no devices discovered");
    } else {
        info!("Scan discovered {} device(s)", found.len());
    }

    Ok(found)
}

pub async fn run_responder(config: AppConfig, identity: Identity) -> Result<MulticastGuard> {
    let multicast_group: Ipv4Addr = DEFAULT_MULTICAST_GROUP.parse()?;
    let listen_sockets = open_multicast_listen_sockets(config.port, multicast_group)
        .context("Failed to open multicast listen sockets")?;
    let mut guard =
        start_multicast_listener(config.clone(), identity.clone(), None, listen_sockets).await?;

    let announce_config = config.clone();
    let announce_identity = identity.clone();
    let announcement_task = tokio::spawn(async move {
        let announce_sockets = open_multicast_announce_sockets(multicast_group);
        if announce_sockets.is_empty() {
            return;
        }
        if let Err(e) = send_announcement(
            announce_sockets,
            announce_config.clone(),
            announce_identity.clone(),
            multicast_group,
            None,
        )
        .await
        {
            warn!("Failed to send startup announcement: {e}");
        }

        let mut interval = tokio::time::interval(Duration::from_secs(30));
        interval.tick().await;
        loop {
            interval.tick().await;
            let sockets = open_multicast_announce_sockets(multicast_group);
            if sockets.is_empty() {
                continue;
            }
            if let Err(e) = send_announcement(
                sockets,
                announce_config.clone(),
                announce_identity.clone(),
                multicast_group,
                None,
            )
            .await
            {
                debug!("Failed to send periodic announcement: {e}");
            }
        }
    });

    guard.tasks.push(announcement_task);
    Ok(guard)
}

pub struct MulticastGuard {
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for MulticastGuard {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

async fn start_multicast_listener(
    config: AppConfig,
    identity: Identity,
    collect: Option<Arc<Mutex<HashMap<String, DiscoveredDevice>>>>,
    sockets: Vec<Arc<UdpSocket>>,
) -> Result<MulticastGuard> {
    let mut tasks = Vec::with_capacity(sockets.len());

    for socket in sockets {
        let config = config.clone();
        let identity = identity.clone();
        let collect = collect.clone();
        let task = tokio::spawn(async move {
            let mut buf = vec![0u8; 65535];
            loop {
                match timeout(Duration::from_secs(1), socket.recv_from(&mut buf)).await {
                    Ok(Ok((len, addr))) => {
                        handle_datagram(
                            &buf[..len],
                            addr.ip(),
                            &config,
                            &identity,
                            collect.as_ref(),
                        )
                        .await;
                    }
                    Ok(Err(e)) => {
                        debug!("Multicast recv error: {e}");
                    }
                    Err(_) => {}
                }
            }
        });
        tasks.push(task);
    }

    Ok(MulticastGuard { tasks })
}

async fn handle_datagram(
    payload: &[u8],
    source_ip: IpAddr,
    config: &AppConfig,
    identity: &Identity,
    collect: Option<&Arc<Mutex<HashMap<String, DiscoveredDevice>>>>,
) {
    let Ok(message) = serde_json::from_slice::<MulticastMessageCompat>(payload) else {
        debug!(
            "Ignored unparsable multicast payload from {source_ip}: {}",
            String::from_utf8_lossy(payload)
                .chars()
                .take(200)
                .collect::<String>()
        );
        return;
    };

    if message.fingerprint == identity.fingerprint {
        return;
    }

    let device = message_to_device(&message, source_ip, config.port, config.https);
    info!(
        "Discovered via UDP multicast: {} ({}) announce={}",
        device.alias,
        device.ip,
        message.is_announce()
    );

    if let Some(collect) = collect {
        collect
            .lock()
            .await
            .insert(device.fingerprint.clone(), device.clone());
    }

    if message.is_announce() {
        let config = config.clone();
        let identity = identity.clone();
        tokio::spawn(async move {
            answer_announcement(&config, &identity, &device).await;
        });
    }
}

async fn answer_announcement(config: &AppConfig, identity: &Identity, peer: &DiscoveredDevice) {
    let payload = build_register_dto(config, identity);
    let protocol = if peer.https {
        ProtocolType::Https
    } else {
        ProtocolType::Http
    };

    match build_http_client(identity, peer.https) {
        Ok(client) => {
            match client
                .register(protocol, &peer.ip, peer.port, payload)
                .await
            {
                Ok(_) => {
                    info!(
                        "Responded to announcement from {} ({}) via HTTP /register",
                        peer.alias, peer.ip
                    );
                    return;
                }
                Err(e) => {
                    debug!(
                        "HTTP /register to {} ({}) failed ({e}), falling back to UDP",
                        peer.alias, peer.ip
                    );
                }
            }
        }
        Err(e) => {
            debug!("Failed to build HTTP client for announcement response: {e}");
        }
    }

    if let Err(e) = send_multicast_response(config, identity).await {
        debug!("UDP fallback for announcement response failed: {e}");
    } else {
        info!(
            "Responded to announcement from {} ({}) via UDP",
            peer.alias, peer.ip
        );
    }
}

fn build_register_dto(config: &AppConfig, identity: &Identity) -> RegisterDto {
    RegisterDto {
        alias: config.alias.clone(),
        version: crate::config::PROTOCOL_VERSION.to_string(),
        device_model: Some(crate::util::os_display_name()),
        device_type: Some(DeviceType::Headless),
        token: identity.fingerprint.clone(),
        port: config.port,
        protocol: if config.https {
            ProtocolType::Https
        } else {
            ProtocolType::Http
        },
        has_web_interface: false,
    }
}

async fn send_multicast_response(config: &AppConfig, identity: &Identity) -> Result<()> {
    let multicast_group: Ipv4Addr = DEFAULT_MULTICAST_GROUP.parse()?;
    let sockets = open_multicast_announce_sockets(multicast_group);
    if sockets.is_empty() {
        anyhow::bail!("No multicast announce sockets available");
    }

    let message = build_multicast_message(config, identity, false);
    let json = serde_json::to_vec(&message).context("Failed to serialize multicast response")?;
    let dest = SocketAddr::new(multicast_group.into(), config.port);

    for socket in &sockets {
        socket
            .send_to(&json, dest)
            .await
            .context("Failed to send multicast response")?;
    }

    Ok(())
}

async fn send_announcement(
    sockets: Vec<Arc<UdpSocket>>,
    config: AppConfig,
    identity: Identity,
    multicast_group: Ipv4Addr,
    deadline: Option<std::time::Instant>,
) -> Result<()> {
    let message = build_multicast_message(&config, &identity, true);
    let json = serde_json::to_vec(&message).context("Failed to serialize multicast message")?;
    let dest = SocketAddr::new(multicast_group.into(), config.port);

    for delay_ms in ANNOUNCE_DELAYS_MS {
        if deadline.is_some_and(|deadline| {
            std::time::Instant::now() + Duration::from_millis(delay_ms) >= deadline
        }) {
            break;
        }
        sleep(Duration::from_millis(delay_ms)).await;
        for socket in &sockets {
            // Best-effort: a single failed interface must not abort discovery.
            if let Err(e) = socket.send_to(&json, dest).await {
                warn!("Failed to send announcement burst: {e}");
            }
        }
    }

    Ok(())
}

/// Listen on the standard LocalSend UDP port (53317). Matches the LocalSend app listener.
fn open_multicast_listen_sockets(
    port: u16,
    multicast_group: Ipv4Addr,
) -> Result<Vec<Arc<UdpSocket>>> {
    let interfaces = list_ipv4_interfaces();
    let mut sockets = Vec::new();

    if interfaces.is_empty() {
        sockets.push(create_bound_udp_socket(
            port,
            multicast_group,
            Ipv4Addr::UNSPECIFIED,
        )?);
        return Ok(sockets);
    }

    for interface in interfaces {
        match create_bound_udp_socket(port, multicast_group, interface) {
            Ok(socket) => {
                debug!("Multicast listen socket on 0.0.0.0:{port} (iface {interface})");
                sockets.push(socket);
            }
            Err(e) => {
                debug!("Skipping multicast listen on {interface}: {e}");
            }
        }
    }

    Ok(sockets)
}

/// Ephemeral UDP ports for outbound announcements (matches the LocalSend app).
fn open_multicast_announce_sockets(multicast_group: Ipv4Addr) -> Vec<Arc<UdpSocket>> {
    let interfaces = list_ipv4_interfaces();
    let mut sockets = Vec::new();

    let targets: Vec<Ipv4Addr> = if interfaces.is_empty() {
        vec![Ipv4Addr::UNSPECIFIED]
    } else {
        interfaces
    };

    for interface in targets {
        match create_bound_udp_socket(0, multicast_group, interface) {
            Ok(socket) => {
                debug!("Multicast announce socket on iface {interface}");
                sockets.push(socket);
            }
            Err(e) => {
                debug!("Skipping multicast announce on {interface}: {e}");
            }
        }
    }

    sockets
}

fn create_bound_udp_socket(
    port: u16,
    multicast_group: Ipv4Addr,
    interface: Ipv4Addr,
) -> Result<Arc<UdpSocket>> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))
        .context("Failed to create UDP socket")?;
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    {
        let _ = socket.set_reuse_port(true);
    }

    socket
        .bind(&SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)).into())
        .with_context(|| format!("Failed to bind UDP 0.0.0.0:{port}"))?;

    // On macOS, joining with the interface address drops inbound multicast; use INADDR_ANY.
    socket
        .join_multicast_v4(&multicast_group, &Ipv4Addr::UNSPECIFIED)
        .with_context(|| format!("Failed to join multicast group {multicast_group}"))?;

    if !interface.is_unspecified() {
        let _ = socket.set_multicast_if_v4(&interface);
    }

    socket.set_nonblocking(true)?;
    Ok(Arc::new(UdpSocket::from_std(socket.into())?))
}

fn build_multicast_message(
    config: &AppConfig,
    identity: &Identity,
    announce: bool,
) -> MulticastMessageCompat {
    MulticastMessageCompat {
        alias: config.alias.clone(),
        version: Some(crate::config::PROTOCOL_VERSION.to_string()),
        device_model: Some(crate::util::os_display_name()),
        device_type: Some("headless".to_string()),
        fingerprint: identity.fingerprint.clone(),
        port: Some(config.port),
        protocol: Some(if config.https {
            ProtocolTypeCompat::Https
        } else {
            ProtocolTypeCompat::Http
        }),
        download: false,
        announce,
        announcement: announce,
    }
}

fn message_to_device(
    message: &MulticastMessageCompat,
    ip: IpAddr,
    fallback_port: u16,
    fallback_https: bool,
) -> DiscoveredDevice {
    DiscoveredDevice {
        alias: message.alias.clone(),
        ip: ip.to_string(),
        port: message.port.unwrap_or(fallback_port),
        fingerprint: message.fingerprint.clone(),
        https: message
            .protocol
            .as_ref()
            .map(|p| *p == ProtocolTypeCompat::Https)
            .unwrap_or(fallback_https),
        version: message
            .version
            .clone()
            .unwrap_or_else(|| crate::config::PROTOCOL_VERSION.to_string()),
        device_type: message
            .device_type
            .as_deref()
            .and_then(crate::util::parse_device_type),
        device_model: message.device_model.clone(),
    }
}

pub async fn resolve_target(
    target: &str,
    config: &AppConfig,
    identity: &Identity,
    allow_scan: bool,
) -> Result<(DiscoveredDevice, &'static str)> {
    if looks_like_ip(target) {
        return Ok((
            DiscoveredDevice {
                alias: target.to_string(),
                ip: target.to_string(),
                port: config.port,
                fingerprint: String::new(),
                https: config.https,
                version: crate::config::PROTOCOL_VERSION.to_string(),
                device_type: None,
                device_model: None,
            },
            "ip",
        ));
    }

    if !allow_scan {
        return Err(crate::error::CliError::TargetNotFound {
            target: target.to_string(),
        }
        .into());
    }

    let devices = scan(
        config,
        identity,
        AppConfig::DEFAULT_DISCOVERY_TIMEOUT_MS * 4,
    )
    .await?;
    let device = devices
        .into_iter()
        .find(|d| d.alias.eq_ignore_ascii_case(target))
        .ok_or_else(|| crate::error::CliError::TargetNotFound {
            target: target.to_string(),
        })?;

    Ok((device, "scan"))
}

fn looks_like_ip(value: &str) -> bool {
    value.parse::<IpAddr>().is_ok()
}

/// Wire representation of the multicast `protocol` field.
///
/// The core crate's `ProtocolType` serializes as SCREAMING_SNAKE_CASE for the
/// v3 protocol, while multicast announcements use lowercase v2.1 values.
#[derive(Clone, Copy, Debug, Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum ProtocolTypeCompat {
    Http,
    Https,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct MulticastMessageCompat {
    alias: String,
    #[serde(default)]
    version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    device_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    device_type: Option<String>,
    fingerprint: String,
    #[serde(default)]
    port: Option<u16>,
    #[serde(default)]
    protocol: Option<ProtocolTypeCompat>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    download: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    announce: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    announcement: bool,
}

impl MulticastMessageCompat {
    fn is_announce(&self) -> bool {
        self.announce || self.announcement
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_official_multicast_json() {
        let json = br#"{"alias":"Happy Orange","version":"2.1","deviceModel":"macOS","deviceType":"desktop","fingerprint":"abc123","port":53317,"protocol":"https","download":false,"announce":true,"announcement":true}"#;
        let msg: MulticastMessageCompat = serde_json::from_slice(json).expect("parse");
        assert_eq!(msg.alias, "Happy Orange");
        assert_eq!(msg.device_type.as_deref(), Some("desktop"));
        assert!(msg.is_announce());
    }

    #[test]
    fn is_announce_falls_back_to_legacy_announcement_field() {
        let mut msg = MulticastMessageCompat {
            alias: "x".into(),
            version: Some("1.0".into()),
            device_model: None,
            device_type: None,
            fingerprint: "f".into(),
            port: Some(53317),
            protocol: None,
            download: false,
            announce: false,
            announcement: true,
        };
        assert!(msg.is_announce());
        msg.announce = true;
        msg.announcement = false;
        assert!(msg.is_announce());
        msg.announce = false;
        msg.announcement = false;
        assert!(!msg.is_announce());
    }

    #[test]
    fn multicast_message_roundtrips_with_both_announce_fields() {
        let msg = MulticastMessageCompat {
            alias: "x".into(),
            version: Some("2.1".into()),
            device_model: None,
            device_type: Some("headless".to_string()),
            fingerprint: "f".into(),
            port: Some(53317),
            protocol: Some(ProtocolTypeCompat::Https),
            download: false,
            announce: true,
            announcement: true,
        };
        let json = serde_json::to_string(&msg).expect("serialize");
        // Both v1 and v2 announce fields are emitted for compat.
        assert!(json.contains("\"announce\":true"));
        assert!(json.contains("\"announcement\":true"));
    }

    #[test]
    fn looks_like_ip_accepts_v4_and_v6() {
        assert!(looks_like_ip("192.168.1.1"));
        assert!(looks_like_ip("10.0.0.1"));
        assert!(looks_like_ip("::1"));
        assert!(looks_like_ip("2001:db8::1"));
    }

    #[test]
    fn looks_like_ip_rejects_hostnames() {
        assert!(!looks_like_ip(""));
        assert!(!looks_like_ip("localhost"));
        assert!(!looks_like_ip("Bob's iPhone"));
        assert!(!looks_like_ip("192.168.1"));
        assert!(!looks_like_ip("999.999.999.999"));
    }

    #[test]
    fn message_to_device_falls_back_on_missing_fields() {
        let msg = MulticastMessageCompat {
            alias: "Peer".into(),
            version: None,
            device_model: None,
            device_type: None,
            fingerprint: "f".into(),
            port: None,
            protocol: None,
            download: true,
            announce: true,
            announcement: true,
        };
        let device = message_to_device(
            &msg,
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 1)),
            53317,
            true,
        );
        assert_eq!(device.alias, "Peer");
        assert_eq!(device.ip, "10.0.0.1");
        // Fallback port and https preference.
        assert_eq!(device.port, 53317);
        assert!(device.https);
        // device_type None → not populated (callers may default to desktop).
        assert!(device.device_type.is_none());
    }

    #[test]
    fn message_to_device_uses_provided_https_when_protocol_missing() {
        let msg = MulticastMessageCompat {
            alias: "Peer".into(),
            version: Some("2.1".into()),
            device_model: None,
            device_type: Some("mobile".to_string()),
            fingerprint: "f".into(),
            port: Some(6000),
            protocol: None,
            download: false,
            announce: true,
            announcement: true,
        };
        let device = message_to_device(
            &msg,
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 2)),
            53317,
            false,
        );
        // Uses protocol from message when present; falls back to caller hint.
        assert_eq!(device.port, 6000);
        assert!(!device.https);
        // device_type should be parsed.
        let dt = device.device_type.unwrap();
        use localsend::model::discovery::DeviceType;
        assert_eq!(dt, DeviceType::Mobile);
    }

    #[test]
    fn discovered_device_display_label_includes_protocol_and_endpoint() {
        let device = DiscoveredDevice {
            alias: "Bob".into(),
            ip: "10.0.0.5".into(),
            port: 53317,
            fingerprint: "f".into(),
            https: true,
            version: "2.1".into(),
            device_type: None,
            device_model: None,
        };
        let label = device.display_label();
        assert!(label.contains("Bob"));
        assert!(label.contains("https://"));
        assert!(label.contains("10.0.0.5"));
        assert!(label.contains("53317"));
    }

    #[test]
    fn split_scan_budget_short_timeouts_spend_all_on_multicast() {
        for timeout in [0u64, 200, 500, 1500, 3999] {
            let budget = split_scan_budget(timeout);
            assert_eq!(budget.fallback_ms, 0, "timeout {timeout}");
            assert_eq!(budget.multicast_ms, timeout.max(200), "timeout {timeout}");
        }
    }

    #[test]
    fn split_scan_budget_generous_timeouts_reserve_fallback_tail() {
        assert_eq!(split_scan_budget(4000).fallback_ms, 2000);
        assert_eq!(split_scan_budget(4000).multicast_ms, 2000);
        assert_eq!(split_scan_budget(5000).fallback_ms, 2000);
        assert_eq!(split_scan_budget(5000).multicast_ms, 3000);
        assert_eq!(split_scan_budget(20000).fallback_ms, 2000);
        assert_eq!(split_scan_budget(20000).multicast_ms, 18000);
    }

    /// Regression for the 15.3 s scan: a 300 ms budget must complete in well
    /// under two seconds (multicast only, no HTTP fallback, no 3.5 s floor).
    #[tokio::test]
    async fn scan_passive_honors_small_timeout_budget() {
        let config = AppConfig {
            alias: "Budget".into(),
            port: 53317,
            https: true,
            multicast_group: DEFAULT_MULTICAST_GROUP.into(),
            config_dir: std::env::temp_dir(),
            receive_dir: std::env::temp_dir(),
        };
        let identity = Identity {
            cert_pem: String::new(),
            key_pem: String::new(),
            fingerprint: "budget-test".into(),
        };
        let started = std::time::Instant::now();
        let _devices = scan_passive(&config, &identity, 300).await.expect("scan");
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "scan with 300ms budget took {elapsed:?}"
        );
    }
}
