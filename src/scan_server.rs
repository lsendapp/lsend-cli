use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::extract::{ConnectInfo, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use axum_server::tls_rustls::RustlsConfig;
use localsend::http::dto_v2::InfoResponseDtoV2;
use localsend::model::discovery::DeviceType;
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, info};

use crate::config::AppConfig;
use crate::discovery::DiscoveredDevice;
use crate::identity::Identity;

#[derive(Clone)]
pub struct DiscoveryServerState {
    config: AppConfig,
    identity: Identity,
    devices: Arc<Mutex<HashMap<String, DiscoveredDevice>>>,
}

pub struct DiscoveryServerHandle {
    task: JoinHandle<()>,
}

impl DiscoveryServerHandle {
    pub fn abort(self) {
        self.task.abort();
    }
}

pub async fn start(
    config: AppConfig,
    identity: Identity,
    devices: Arc<Mutex<HashMap<String, DiscoveredDevice>>>,
) -> Result<DiscoveryServerHandle> {
    let state = DiscoveryServerState {
        config: config.clone(),
        identity: identity.clone(),
        devices,
    };

    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    // Bind before spawning so the port is known to be available before
    // multicast announcements go out - no 100 ms sleep guesswork.
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("Failed to bind discovery server on port {}", config.port))?;

    let app = Router::new()
        .route("/api/localsend/v1/info", get(info_handler))
        .route("/api/localsend/v2/info", get(info_handler))
        .route("/api/localsend/v1/register", post(register_handler))
        .route("/api/localsend/v2/register", post(register_handler))
        .with_state(state)
        .into_make_service_with_connect_info::<SocketAddr>();

    let task = if config.https {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let tls = RustlsConfig::from_pem(
            identity.cert_pem.clone().into_bytes(),
            identity.key_pem.clone().into_bytes(),
        )
        .await
        .context("Failed to load TLS config for discovery server")?;
        let std_listener = listener.into_std()?;
        tokio::spawn(async move {
            if let Err(e) = axum_server::from_tcp_rustls(std_listener, tls)
                .serve(app)
                .await
            {
                debug!("Discovery HTTPS server stopped: {e}");
            }
        })
    } else {
        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                debug!("Discovery HTTP server stopped: {e}");
            }
        })
    };

    info!(
        "Discovery {} server listening on port {}",
        if config.https { "HTTPS" } else { "HTTP" },
        config.port
    );

    Ok(DiscoveryServerHandle { task })
}

async fn info_handler(State(state): State<DiscoveryServerState>) -> Json<InfoResponseDtoV2> {
    Json(info_response(&state))
}

async fn register_handler(
    State(state): State<DiscoveryServerState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(payload): Json<RegisterCompat>,
) -> Result<
    Json<InfoResponseDtoV2>,
    (
        axum::http::StatusCode,
        Json<localsend::http::dto::ErrorResponse>,
    ),
> {
    let fingerprint = payload.fingerprint();
    if fingerprint.is_empty() {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            Json(localsend::http::dto::ErrorResponse {
                message: "Missing fingerprint".to_string(),
            }),
        ));
    }

    if fingerprint == state.identity.fingerprint {
        return Err((
            axum::http::StatusCode::PRECONDITION_FAILED,
            Json(localsend::http::dto::ErrorResponse {
                message: "Self-discovered".to_string(),
            }),
        ));
    }

    let https = payload
        .protocol
        .as_deref()
        .map(|p| p.eq_ignore_ascii_case("https"))
        .unwrap_or(state.config.https);

    let device = DiscoveredDevice {
        alias: payload.alias.clone(),
        ip: addr.ip().to_string(),
        port: payload.port.unwrap_or(state.config.port),
        fingerprint: fingerprint.to_string(),
        https,
        version: payload
            .version
            .clone()
            .unwrap_or_else(|| crate::config::PROTOCOL_VERSION.to_string()),
        device_type: payload
            .device_type
            .as_deref()
            .and_then(crate::util::parse_device_type),
        device_model: payload.device_model.clone(),
    };

    info!(
        "Discovered via TCP /register: {} ({})",
        device.alias, device.ip
    );

    state
        .devices
        .lock()
        .await
        .insert(fingerprint.to_string(), device);

    Ok(Json(info_response(&state)))
}

fn info_response(state: &DiscoveryServerState) -> InfoResponseDtoV2 {
    InfoResponseDtoV2 {
        alias: state.config.alias.clone(),
        version: crate::config::PROTOCOL_VERSION.to_string(),
        device_model: Some(crate::util::os_display_name()),
        device_type: Some(DeviceType::Headless),
        fingerprint: state.identity.fingerprint.clone(),
        download: false,
    }
}

/// Accepts v2 `fingerprint` and legacy v1 `token` field names.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RegisterCompat {
    alias: String,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    device_model: Option<String>,
    #[serde(default)]
    device_type: Option<String>,
    #[serde(default)]
    fingerprint: String,
    #[serde(default)]
    token: String,
    #[serde(default)]
    port: Option<u16>,
    #[serde(default)]
    protocol: Option<String>,
}

impl RegisterCompat {
    fn fingerprint(&self) -> &str {
        if !self.fingerprint.is_empty() {
            &self.fingerprint
        } else {
            &self.token
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use crate::identity::Identity;

    fn dummy_state() -> DiscoveryServerState {
        let dir = std::env::temp_dir().join(format!("lsend-scan-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let config = AppConfig::new(None, 53317, true, None).unwrap();
        let identity = Identity {
            cert_pem: String::new(),
            key_pem: String::new(),
            fingerprint: "abc123".to_string(),
        };
        let devices = Arc::new(Mutex::new(HashMap::new()));
        std::fs::remove_dir_all(&dir).ok();
        DiscoveryServerState {
            config,
            identity,
            devices,
        }
    }

    #[test]
    fn info_response_uses_lowercase_headless_device_type() {
        let state = dummy_state();
        let resp = info_response(&state);
        let json = serde_json::to_string(&resp).expect("serialize");
        // deviceType should be lowercase to match the official client and the
        // protocol spec.
        assert!(json.contains("\"deviceType\":\"headless\""), "got: {json}");
        assert!(json.contains("\"alias\":"));
        assert!(json.contains("\"version\":\"2.2\""));
        assert!(json.contains("\"fingerprint\":\"abc123\""));
        assert!(json.contains("\"download\":false"));
        assert!(!json.contains("deviceType\":\"HEADLESS\""));
    }

    #[test]
    fn register_compat_prefers_fingerprint_over_legacy_token() {
        let v2 = RegisterCompat {
            alias: "a".into(),
            version: Some("2.1".into()),
            device_model: None,
            device_type: None,
            fingerprint: "fingerprint-value".into(),
            token: String::new(),
            port: Some(53317),
            protocol: Some("https".into()),
        };
        assert_eq!(v2.fingerprint(), "fingerprint-value");

        let v1 = RegisterCompat {
            alias: "a".into(),
            version: None,
            device_model: None,
            device_type: None,
            fingerprint: String::new(),
            token: "legacy-token".into(),
            port: Some(53317),
            protocol: Some("http".into()),
        };
        assert_eq!(v1.fingerprint(), "legacy-token");
    }

    #[test]
    fn register_compat_uses_empty_when_neither_field_present() {
        let c = RegisterCompat {
            alias: "a".into(),
            version: None,
            device_model: None,
            device_type: None,
            fingerprint: String::new(),
            token: String::new(),
            port: None,
            protocol: None,
        };
        assert_eq!(c.fingerprint(), "");
    }

    #[test]
    fn register_compat_parses_v2_payload() {
        let json = r#"{
            "alias": "Sender",
            "version": "2.1",
            "deviceModel": "iPhone",
            "deviceType": "mobile",
            "fingerprint": "abc",
            "port": 53317,
            "protocol": "https"
        }"#;
        let parsed: RegisterCompat = serde_json::from_str(json).expect("parse");
        assert_eq!(parsed.alias, "Sender");
        assert_eq!(parsed.port, Some(53317));
        assert_eq!(parsed.protocol.as_deref(), Some("https"));
    }

    #[test]
    fn register_compat_parses_v1_legacy_payload() {
        let json = r#"{
            "alias": "Sender",
            "token": "legacy-fp",
            "port": 53317,
            "protocol": "http"
        }"#;
        let parsed: RegisterCompat = serde_json::from_str(json).expect("parse");
        assert_eq!(parsed.fingerprint(), "legacy-fp");
    }
}
