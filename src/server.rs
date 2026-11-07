use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use axum::body::Body;
use axum::extract::{ConnectInfo, DefaultBodyLimit, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use axum_server::tls_rustls::RustlsConfig;
use futures_util::StreamExt;
use localsend::http::dto::ErrorResponse;
use localsend::http::dto_v2::{
    InfoResponseDtoV2, PrepareUploadResponseDtoV2, RegisterResponseDtoV2,
};
use localsend::model::transfer::FileDto;
use serde::Deserialize;
use serde::Serialize;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, mpsc, watch};
use uuid::Uuid;

use crate::config::AppConfig;
use crate::events::ReceiveEvent;
use crate::identity::Identity;
use crate::output::{OutputMode, ReceiveEventJson, print_json};

/// Delay before returning 204 for embedded text messages so the mobile sender
/// can finish its SendPage transition before reading the response.
const TEXT_MESSAGE_RESPONSE_DELAY: Duration = Duration::from_millis(1200);
/// Reclaim abandoned negotiations after five minutes without an upload.
/// Streaming uploads have a separate per-chunk idle timeout of the same length.
const SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
/// Discard PIN attempt counters after this idle period to bound memory in
/// long-running receivers (desktop app). Also prevents permanent lockout.
const PIN_ATTEMPTS_TTL: Duration = Duration::from_secs(300);
#[derive(Clone)]
pub struct ServerState {
    pub config: AppConfig,
    pub identity: Identity,
    pub receive_pin: Option<String>,
    output: OutputMode,
    stop_after_transfer: bool,
    stop_tx: Option<mpsc::UnboundedSender<()>>,
    event_tx: Option<mpsc::UnboundedSender<ReceiveEvent>>,
    shutdown: watch::Sender<bool>,
    inner: Arc<Mutex<InnerState>>,
    pin_attempts: Arc<Mutex<HashMap<String, PinAttempts>>>,
}

struct InnerState {
    session: Option<ReceiveSession>,
}

struct ReceiveSession {
    last_activity: Instant,
    session_id: String,
    sender_ip: String,
    sender_version: String,
    destination_dir: PathBuf,
    files: HashMap<String, ReceivingFileEntry>,
    status: SessionStatus,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SessionStatus {
    Sending,
    Finished,
    FinishedWithErrors,
}

struct ReceivingFileEntry {
    file: FileDto,
    token: String,
    desired_name: String,
    path: Option<PathBuf>,
    failed: bool,
    uploading: bool,
}

struct PinAttempts {
    count: u32,
    last: Instant,
}

impl ServerState {
    pub fn new(
        config: AppConfig,
        identity: Identity,
        receive_pin: Option<String>,
        output: OutputMode,
        stop_after_transfer: bool,
        stop_tx: Option<mpsc::UnboundedSender<()>>,
    ) -> Self {
        Self::new_with_events(
            config,
            identity,
            receive_pin,
            output,
            stop_after_transfer,
            stop_tx,
            None,
        )
    }

    /// Like [`ServerState::new`] but also forwards receive-lifecycle events to
    /// `event_tx` for programmatic consumers. The CLI passes `None`; the
    /// desktop app passes `Some(tx)` to drive tray state and notifications.
    pub fn new_with_events(
        config: AppConfig,
        identity: Identity,
        receive_pin: Option<String>,
        output: OutputMode,
        stop_after_transfer: bool,
        stop_tx: Option<mpsc::UnboundedSender<()>>,
        event_tx: Option<mpsc::UnboundedSender<ReceiveEvent>>,
    ) -> Self {
        Self {
            config,
            identity,
            receive_pin,
            output,
            stop_after_transfer,
            stop_tx,
            event_tx,
            shutdown: watch::channel(false).0,
            inner: Arc::new(Mutex::new(InnerState { session: None })),
            pin_attempts: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Stop accepting work and cancel streaming uploads so their reservations clean up.
    pub fn shutdown(&self) {
        self.shutdown.send_replace(true);
    }

    fn expire_idle_session(&self, slot: &mut Option<ReceiveSession>) {
        if slot.as_ref().is_some_and(|session| {
            session.last_activity.elapsed() >= SESSION_IDLE_TIMEOUT
                && !session.files.values().any(|file| file.uploading)
        }) {
            *slot = None;
            self.notify_human("Abandoned transfer expired.");
            self.emit_json_event(ReceiveEventJson::TransferCancelled);
            self.emit_event(ReceiveEvent::TransferCancelled);
        }
    }

    fn notify_human(&self, message: impl AsRef<str>) {
        if self.output == OutputMode::Human {
            println!("{}", message.as_ref());
        }
    }

    fn emit_json_event(&self, event: ReceiveEventJson) {
        if self.output == OutputMode::Json {
            print_json(&event);
        }
    }

    /// Forward a receive-lifecycle event to the programmatic channel, if any.
    /// Saturates silently: a slow/stalled consumer never blocks the server.
    fn emit_event(&self, event: ReceiveEvent) {
        if let Some(tx) = &self.event_tx {
            let _ = tx.send(event);
        }
    }

    /// Check if every file in the session is done (saved or failed) and
    /// transition the session status accordingly. On success the session is
    /// cleared; on partial failure it stays alive for sender retry.
    ///
    /// Extracted so both the save-success and save-error paths call it -
    /// previously the error path returned early and could leave the session
    /// stuck in `Sending` when the last file failed.
    fn maybe_complete_session(&self, session_slot: &mut Option<ReceiveSession>) {
        let Some(session) = session_slot.as_mut() else {
            return;
        };
        let all_done = session.files.values().all(|f| f.path.is_some() || f.failed);
        if !all_done {
            return;
        }
        let has_error = session.files.values().any(|f| f.failed);
        if has_error {
            // Keep the session alive so the sender can retry failed files.
            session.status = SessionStatus::FinishedWithErrors;
            self.notify_human("Transfer finished with errors.");
            if self.output == OutputMode::Json {
                self.emit_json_event(ReceiveEventJson::TransferFinishedWithErrors);
            }
            self.emit_event(ReceiveEvent::TransferFinishedWithErrors);
        } else {
            session.status = SessionStatus::Finished;
            self.notify_human("Transfer complete.");
            if self.output == OutputMode::Json {
                self.emit_json_event(ReceiveEventJson::TransferComplete);
            }
            self.emit_event(ReceiveEvent::TransferComplete);
            *session_slot = None;
            if self.stop_after_transfer {
                if let Some(tx) = &self.stop_tx {
                    let _ = tx.send(());
                }
            }
        }
    }
}

pub async fn run_http(state: ServerState, listener: tokio::net::TcpListener) -> Result<()> {
    let mut shutdown = state.shutdown.subscribe();
    let app = build_router(state);
    tracing::info!("Listening on http://{}", listener.local_addr()?);
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        let _ = shutdown.wait_for(|stopped| *stopped).await;
    })
    .await?;
    Ok(())
}

fn prepare_tls(identity: &Identity) -> Result<RustlsConfig> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    use rustls::pki_types::pem::PemObject;

    let certs: Vec<rustls::pki_types::CertificateDer<'static>> = vec![
        rustls::pki_types::CertificateDer::from_pem_slice(identity.cert_pem.as_bytes())
            .map_err(|e| anyhow::anyhow!("Failed to parse certificate: {e}"))?,
    ];
    let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(identity.key_pem.as_bytes())
        .map_err(|e| anyhow::anyhow!("Failed to parse private key: {e}"))?;

    let verifier = crate::mtls::LocalSendClientCertVerifier::try_new(&identity.cert_pem)
        .map_err(|e| anyhow::anyhow!("Failed to build client cert verifier: {e:#}"))?;

    let server_config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(Arc::new(verifier))
        .with_single_cert(certs, key)
        .map_err(|e| anyhow::anyhow!("Failed to build rustls ServerConfig: {e}"))?;

    // axum-server's `RustlsConfig::from_pem` sets ALPN to ["h2", "http/1.1"].
    // `from_config` does not, so set it ourselves to match LocalSend peers.
    let mut server_config = server_config;
    server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    Ok(RustlsConfig::from_config(Arc::new(server_config)))
}

/// Finish fallible TLS setup before the caller announces readiness.
pub(crate) fn spawn_server(
    state: ServerState,
    listener: tokio::net::TcpListener,
) -> Result<tokio::task::JoinHandle<Result<()>>> {
    let tls = if state.config.https {
        Some(prepare_tls(&state.identity)?)
    } else {
        None
    };
    Ok(tokio::spawn(async move {
        match tls {
            Some(config) => run_https_configured(state, listener, config).await,
            None => run_http(state, listener).await,
        }
    }))
}

pub async fn run_https(state: ServerState, listener: tokio::net::TcpListener) -> Result<()> {
    let config = prepare_tls(&state.identity)?;
    run_https_configured(state, listener, config).await
}

async fn run_https_configured(
    state: ServerState,
    listener: tokio::net::TcpListener,
    config: RustlsConfig,
) -> Result<()> {
    let mut shutdown = state.shutdown.subscribe();
    let app = build_router(state);
    let addr = listener.local_addr()?;
    tracing::info!("Listening on https://{addr} (mTLS: client cert required)");
    // Convert the tokio listener to std for axum-server; the port is already
    // bound so discovery can announce immediately after this returns.
    let std_listener = listener.into_std()?;
    let handle = axum_server::Handle::new();
    let server = axum_server::from_tcp_rustls(std_listener, config)
        .handle(handle.clone())
        .serve(app.into_make_service_with_connect_info::<SocketAddr>());
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result?,
        _ = async { let _ = shutdown.wait_for(|stopped| *stopped).await; } => {
            handle.graceful_shutdown(None);
            server.await?;
        }
    }
    Ok(())
}

fn build_router(state: ServerState) -> Router {
    // Upload routes handle arbitrary-size file bodies; the 10 MB limit applies
    // only to JSON metadata routes (info/register/prepare-upload/cancel).
    let upload_routes = Router::new()
        .route("/api/localsend/v1/send", post(upload_v1_handler))
        .route("/api/localsend/v2/upload", post(upload_v2_handler));

    Router::new()
        .route("/api/localsend/v1/info", get(info_v1_handler))
        .route("/api/localsend/v2/info", get(info_handler))
        .route("/api/localsend/v1/register", post(register_v1_handler))
        .route("/api/localsend/v2/register", post(register_handler))
        .route(
            "/api/localsend/v1/send-request",
            post(prepare_upload_v1_handler),
        )
        .route(
            "/api/localsend/v2/prepare-upload",
            post(prepare_upload_v2_handler),
        )
        .route("/api/localsend/v1/cancel", post(cancel_handler))
        .route("/api/localsend/v2/cancel", post(cancel_handler))
        .layer(DefaultBodyLimit::max(10 * 1024 * 1024))
        .merge(upload_routes)
        .with_state(state)
}

/// JSON shape expected by legacy v1 clients (manual IP entry uses `peerProtocolVersion = 1.0`).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct InfoResponseJson {
    alias: String,
    version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    device_model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    device_type: Option<String>,
    fingerprint: String,
    download: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct RegisterPayloadCompat {
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

impl RegisterPayloadCompat {
    fn fingerprint(&self) -> &str {
        if !self.fingerprint.is_empty() {
            &self.fingerprint
        } else {
            &self.token
        }
    }
}

async fn info_v1_handler(
    State(state): State<ServerState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<InfoResponseJson>, Response> {
    if is_self_fingerprint(&state, query.get("fingerprint")) {
        return Err(error_response(
            StatusCode::PRECONDITION_FAILED,
            "Self-discovered",
        ));
    }
    Ok(Json(info_response_json(&state)))
}

async fn register_v1_handler(
    State(state): State<ServerState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(payload): Json<RegisterPayloadCompat>,
) -> Result<Json<InfoResponseJson>, Response> {
    let fingerprint = payload.fingerprint();
    if fingerprint.is_empty() {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "Missing fingerprint",
        ));
    }
    if fingerprint == state.identity.fingerprint {
        return Err(error_response(
            StatusCode::PRECONDITION_FAILED,
            "Self-discovered",
        ));
    }
    record_registration(&state, addr, payload);
    Ok(Json(info_response_json(&state)))
}

async fn info_handler(State(state): State<ServerState>) -> Json<InfoResponseDtoV2> {
    Json(info_response(&state))
}

async fn register_handler(
    State(state): State<ServerState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(payload): Json<RegisterPayloadCompat>,
) -> Json<RegisterResponseDtoV2> {
    record_registration(&state, addr, payload);
    Json(register_response(&state))
}

fn record_registration(state: &ServerState, addr: SocketAddr, payload: RegisterPayloadCompat) {
    let fingerprint = payload.fingerprint().to_owned();
    if fingerprint.is_empty() || fingerprint == state.identity.fingerprint {
        return;
    }
    state.emit_event(ReceiveEvent::PeerDiscovered(
        crate::discovery::DiscoveredDevice {
            alias: payload.alias,
            ip: addr.ip().to_string(),
            port: payload.port.unwrap_or(state.config.port),
            fingerprint,
            https: payload
                .protocol
                .as_deref()
                .map(|p| p.eq_ignore_ascii_case("https"))
                .unwrap_or(state.config.https),
            version: payload
                .version
                .unwrap_or_else(|| crate::config::PROTOCOL_VERSION.to_owned()),
            device_type: payload
                .device_type
                .as_deref()
                .and_then(crate::util::parse_device_type),
            device_model: payload.device_model,
        },
    ));
}

async fn prepare_upload_v1_handler(
    State(state): State<ServerState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: String,
) -> Response {
    prepare_upload_handler(state, addr, query, headers, body, false).await
}

async fn prepare_upload_v2_handler(
    State(state): State<ServerState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: String,
) -> Response {
    prepare_upload_handler(state, addr, query, headers, body, true).await
}

async fn prepare_upload_handler(
    state: ServerState,
    addr: SocketAddr,
    query: HashMap<String, String>,
    headers: HeaderMap,
    body: String,
    v2_api: bool,
) -> Response {
    if let Some(response) = check_pin(&state, addr.ip(), &query, &headers).await {
        return response;
    }

    let request = match parse_prepare_upload_request(&body) {
        Ok(request) => request,
        Err(error) => {
            tracing::warn!("Prepare upload parse error: {error}");
            return error_response(StatusCode::BAD_REQUEST, "Request body malformed");
        }
    };

    if request.files.is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "Request must contain at least one file",
        );
    }

    if let Some(text) = extract_embedded_text_message(&request.files) {
        let mut guard = state.inner.lock().await;
        state.expire_idle_session(&mut guard.session);
        if guard.session.is_some() {
            return error_response(StatusCode::CONFLICT, "Blocked by another session");
        }
        drop(guard);
        return handle_embedded_text_message(state, &request.sender_alias, text).await;
    }

    let session_id = Uuid::new_v4().to_string();
    let sender_ip = addr.ip().to_string();
    let destination_dir = state.config.receive_dir.clone();

    let mut response_files = HashMap::new();
    let mut session_files = HashMap::new();

    for (id, file) in request.files {
        let token = Uuid::new_v4().to_string();
        let desired_name = file.file_name.clone();
        response_files.insert(id.clone(), token.clone());
        session_files.insert(
            id,
            ReceivingFileEntry {
                file,
                token,
                desired_name,
                path: None,
                failed: false,
                uploading: false,
            },
        );
    }

    // Check for an active session and install the new one in one critical
    // section to close the TOCTOU window between the conflict check and
    // session installation.
    let mut guard = state.inner.lock().await;
    state.expire_idle_session(&mut guard.session);
    if guard.session.is_some() {
        return error_response(StatusCode::CONFLICT, "Blocked by another session");
    }

    state.notify_human(format!(
        "Incoming transfer from {} ({} file(s))",
        request.sender_alias,
        session_files.len()
    ));
    let sender_alias_for_event = request.sender_alias.clone();
    let file_count_for_event = session_files.len();
    if state.output == OutputMode::Json {
        state.emit_json_event(ReceiveEventJson::TransferStarted {
            sender_alias: request.sender_alias,
            file_count: session_files.len(),
        });
    }
    state.emit_event(ReceiveEvent::TransferStarted {
        sender_alias: sender_alias_for_event,
        file_count: file_count_for_event,
    });

    guard.session = Some(ReceiveSession {
        last_activity: Instant::now(),
        session_id: session_id.clone(),
        sender_ip,
        sender_version: request.sender_version,
        destination_dir,
        files: session_files,
        status: SessionStatus::Sending,
    });
    drop(guard);

    if v2_api {
        Json(PrepareUploadResponseDtoV2 {
            session_id,
            files: response_files,
        })
        .into_response()
    } else {
        Json(response_files).into_response()
    }
}

#[derive(Debug)]
struct ParsedPrepareUpload {
    sender_alias: String,
    sender_version: String,
    files: HashMap<String, FileDto>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrepareUploadRequestCompat {
    info: PrepareUploadInfoCompat,
    files: HashMap<String, PrepareUploadFileCompat>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrepareUploadInfoCompat {
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
    #[serde(default)]
    download: Option<bool>,
    #[serde(default, rename = "hasWebInterface")]
    has_web_interface: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrepareUploadFileCompat {
    #[serde(default)]
    id: String,
    file_name: String,
    size: u64,
    file_type: String,
    #[serde(default)]
    hash: Option<String>,
    #[serde(default)]
    sha256: Option<String>,
    #[serde(default)]
    preview: Option<String>,
    #[serde(default)]
    metadata: Option<localsend::model::transfer::FileMetadata>,
}

impl PrepareUploadFileCompat {
    fn into_file_dto(self, map_key: &str) -> FileDto {
        FileDto {
            id: if self.id.is_empty() {
                map_key.to_string()
            } else {
                self.id
            },
            file_name: self.file_name,
            size: self.size,
            file_type: self.file_type,
            sha256: self.sha256.or(self.hash),
            preview: self.preview,
            metadata: self.metadata,
        }
    }
}

#[derive(Debug)]
enum PrepareUploadParseError {
    Json(serde_json::Error),
    MissingField(&'static str),
    InvalidField(&'static str),
}

impl std::fmt::Display for PrepareUploadParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json(error) => write!(f, "json error: {error}"),
            Self::MissingField(field) => write!(f, "missing field: {field}"),
            Self::InvalidField(field) => write!(f, "invalid field: {field}"),
        }
    }
}

fn parse_prepare_upload_request(
    body: &str,
) -> Result<ParsedPrepareUpload, PrepareUploadParseError> {
    if let Ok(dto) = serde_json::from_str::<PrepareUploadRequestCompat>(body) {
        let mut files = HashMap::with_capacity(dto.files.len());
        for (id, file) in dto.files {
            files.insert(id.clone(), file.into_file_dto(&id));
        }
        return Ok(ParsedPrepareUpload {
            sender_alias: dto.info.alias,
            sender_version: dto.info.version.unwrap_or_else(|| "1.0".to_string()),
            files,
        });
    }

    let root: serde_json::Value =
        serde_json::from_str(body).map_err(PrepareUploadParseError::Json)?;
    let info = root
        .get("info")
        .ok_or(PrepareUploadParseError::MissingField("info"))?;
    let alias = read_string_field(info, &["alias"])
        .ok_or(PrepareUploadParseError::MissingField("info.alias"))?;
    let version = read_string_field(info, &["version"]).unwrap_or_else(|| "1.0".to_string());

    let files_value = root
        .get("files")
        .and_then(|value| value.as_object())
        .ok_or(PrepareUploadParseError::MissingField("files"))?;

    let mut files = HashMap::with_capacity(files_value.len());
    for (map_key, file_value) in files_value {
        files.insert(map_key.clone(), parse_file_dto(map_key, file_value)?);
    }

    Ok(ParsedPrepareUpload {
        sender_alias: alias,
        sender_version: version,
        files,
    })
}

fn read_string_field(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(found) = value.get(*key).and_then(|v| v.as_str()) {
            return Some(found.to_string());
        }
    }
    None
}

fn read_u64_field(value: &serde_json::Value, keys: &[&str]) -> Option<u64> {
    for key in keys {
        let Some(raw) = value.get(*key) else {
            continue;
        };
        if let Some(number) = raw.as_u64() {
            return Some(number);
        }
        if let Some(text) = raw.as_str() {
            if let Ok(number) = text.parse::<u64>() {
                return Some(number);
            }
        }
    }
    None
}

fn parse_file_dto(
    map_key: &str,
    value: &serde_json::Value,
) -> Result<FileDto, PrepareUploadParseError> {
    let object = value
        .as_object()
        .ok_or(PrepareUploadParseError::InvalidField("files.*"))?;
    let id = read_string_field(value, &["id"]).unwrap_or_else(|| map_key.to_string());
    let file_name = read_string_field(value, &["fileName", "file_name"])
        .ok_or(PrepareUploadParseError::MissingField("files.*.fileName"))?;
    let size = read_u64_field(value, &["size"])
        .ok_or(PrepareUploadParseError::MissingField("files.*.size"))?;
    let file_type = read_string_field(value, &["fileType", "file_type"])
        .ok_or(PrepareUploadParseError::MissingField("files.*.fileType"))?;
    let sha256 = read_string_field(value, &["sha256", "hash"]);
    let preview = read_string_field(value, &["preview"]);
    let metadata = object.get("metadata").and_then(parse_file_metadata);

    Ok(FileDto {
        id,
        file_name,
        size,
        file_type,
        sha256,
        preview,
        metadata,
    })
}

fn parse_file_metadata(
    value: &serde_json::Value,
) -> Option<localsend::model::transfer::FileMetadata> {
    let _object = value.as_object()?;
    Some(localsend::model::transfer::FileMetadata {
        modified: read_string_field(value, &["modified", "lastModified"]),
        accessed: read_string_field(value, &["accessed", "lastAccessed"]),
    })
}

fn is_text_file_type(file_type: &str) -> bool {
    let lower = file_type.to_ascii_lowercase();
    lower == "text" || lower.starts_with("text/")
}

fn extract_embedded_text_message(files: &HashMap<String, FileDto>) -> Option<&str> {
    if files.len() != 1 {
        return None;
    }
    let file = files.values().next()?;
    if !is_text_file_type(&file.file_type) {
        return None;
    }
    file.preview.as_deref()
}

async fn handle_embedded_text_message(
    state: ServerState,
    sender_alias: &str,
    text: &str,
) -> Response {
    let bytes = text.len() as u64;
    state.notify_human(format!("Message from {sender_alias}: {text}"));
    if state.output == OutputMode::Json {
        state.emit_json_event(ReceiveEventJson::MessageReceived {
            sender_alias: sender_alias.to_string(),
            text: text.to_string(),
            size: bytes,
        });
        state.emit_json_event(ReceiveEventJson::TransferComplete);
    }
    state.emit_event(ReceiveEvent::MessageReceived {
        sender_alias: sender_alias.to_string(),
        text: text.to_string(),
        size: bytes,
    });
    state.emit_event(ReceiveEvent::TransferComplete);
    tokio::time::sleep(TEXT_MESSAGE_RESPONSE_DELAY).await;
    if state.stop_after_transfer {
        if let Some(tx) = &state.stop_tx {
            let _ = tx.send(());
        }
    }
    StatusCode::NO_CONTENT.into_response()
}

async fn upload_v1_handler(
    State(state): State<ServerState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Query(params): Query<HashMap<String, String>>,
    body: Body,
) -> Response {
    upload_handler(state, addr, params, body, false).await
}

async fn upload_v2_handler(
    State(state): State<ServerState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Query(params): Query<HashMap<String, String>>,
    body: Body,
) -> Response {
    upload_handler(state, addr, params, body, true).await
}

async fn upload_handler(
    state: ServerState,
    addr: SocketAddr,
    params: HashMap<String, String>,
    body: Body,
    require_session_id: bool,
) -> Response {
    let session_id = params.get("sessionId").cloned();
    if require_session_id && session_id.is_none() {
        return error_response(StatusCode::BAD_REQUEST, "Missing parameters");
    }
    let file_id = match params.get("fileId") {
        Some(v) => v.clone(),
        None => return error_response(StatusCode::BAD_REQUEST, "Missing parameters"),
    };
    let token = match params.get("token") {
        Some(v) => v.clone(),
        None => return error_response(StatusCode::BAD_REQUEST, "Missing parameters"),
    };

    let mut guard = state.inner.lock().await;
    state.expire_idle_session(&mut guard.session);
    let Some(session) = guard.session.as_mut() else {
        return error_response(StatusCode::CONFLICT, "No session");
    };

    if let Some(expected_session_id) = &session_id {
        if session.session_id != *expected_session_id {
            return error_response(StatusCode::FORBIDDEN, "Invalid session id");
        }
    }

    if session.sender_ip != addr.ip().to_string() {
        return error_response(
            StatusCode::FORBIDDEN,
            &format!("Invalid IP address: {}", addr.ip()),
        );
    }

    if session.status != SessionStatus::Sending
        && session.status != SessionStatus::FinishedWithErrors
    {
        return error_response(StatusCode::CONFLICT, "Recipient is in wrong state");
    }

    // Resuming a failed session: mark it active again so concurrent state checks see "sending".
    if session.status == SessionStatus::FinishedWithErrors {
        session.status = SessionStatus::Sending;
    }

    let Some(entry) = session.files.get_mut(&file_id) else {
        return error_response(StatusCode::FORBIDDEN, "Invalid token");
    };

    if entry.token != token {
        return error_response(StatusCode::FORBIDDEN, "Invalid token");
    }

    if entry.uploading || entry.path.is_some() {
        return error_response(StatusCode::CONFLICT, "File already uploaded or uploading");
    }
    entry.failed = false;
    entry.uploading = true;
    session.last_activity = Instant::now();

    let active_session_id = session.session_id.clone();
    let expected_size = entry.file.size;
    let desired_name = entry.desired_name.clone();
    let destination_dir = session.destination_dir.clone();
    drop(guard);

    // Reserve and open the destination in a blocking task. cap-std resolves
    // every path relative to a directory handle and disallows escapes.
    let reserve_result = match validate_relative_file_name(&desired_name) {
        Err(message) => Err((StatusCode::FORBIDDEN, message)),
        Ok(()) => tokio::task::spawn_blocking(move || {
            reserve_receive_file(&destination_dir, &desired_name)
        })
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))
        .and_then(|result| {
            result.map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))
        }),
    };
    let mut reserved = None;
    let save_result = match reserve_result {
        Ok(file) => {
            reserved = Some(file);
            let mut shutdown = state.shutdown.subscribe();
            tokio::select! {
                biased;
                _ = shutdown.wait_for(|stopped| *stopped) => {
                    Err((StatusCode::SERVICE_UNAVAILABLE, "Receiver shutting down".to_owned()))
                }
                result = save_stream(body, reserved.as_mut().unwrap(), expected_size) => result,
            }
        }
        Err(error) => Err(error),
    };

    let mut guard = state.inner.lock().await;
    let Some(session) = guard.session.as_mut() else {
        return error_response(StatusCode::CONFLICT, "No session");
    };
    // Cancellation may have replaced the session while the body was streaming.
    if session.session_id != active_session_id {
        return error_response(StatusCode::CONFLICT, "Session was cancelled");
    }
    let Some(entry) = session.files.get_mut(&file_id) else {
        return error_response(StatusCode::FORBIDDEN, "Invalid token");
    };
    entry.uploading = false;
    session.last_activity = Instant::now();

    // Record the outcome without returning early on error: the session
    // transition check below must run even when the last file fails,
    // otherwise the session is stuck in `Sending` forever.
    let save_error = match save_result {
        Ok(bytes) => {
            let reserved = reserved
                .as_mut()
                .expect("successful save has a reserved file");
            reserved.keep = true;
            let target_path = reserved.path.clone();
            let desired_name = entry.desired_name.clone();
            entry.path = Some(target_path.clone());
            entry.failed = false;
            state.notify_human(format!("Saved {} ({} bytes)", target_path.display(), bytes));
            if state.output == OutputMode::Json {
                state.emit_json_event(ReceiveEventJson::FileSaved {
                    path: target_path.display().to_string(),
                    file_name: desired_name.clone(),
                    size: bytes,
                });
            }
            state.emit_event(ReceiveEvent::FileSaved {
                path: target_path.clone(),
                file_name: desired_name,
                size: bytes,
            });
            None
        }
        Err(e) => {
            entry.failed = true;
            Some(e)
        }
    };

    // Transition the session (complete / finished-with-errors / keep alive).
    // Runs on both success and error paths.
    state.maybe_complete_session(&mut guard.session);

    match save_error {
        Some((status, msg)) => error_response(status, &msg),
        None => StatusCode::OK.into_response(),
    }
}

/// Outcome of evaluating a cancel request against the current session state.
/// Extracted as a pure function so it can be unit-tested without spinning up
/// an axum router.
#[derive(Debug, PartialEq, Eq)]
enum CancelDecision {
    /// Cancel is allowed; the caller should clear the session and return 200.
    Allow,
    /// Cancel is denied for the given reason; the caller should return 403.
    Deny(&'static str),
}

fn evaluate_cancel(
    session: Option<&ReceiveSession>,
    requester_ip: &str,
    requested_session_id: Option<&str>,
) -> CancelDecision {
    let v2_cancel = requested_session_id.is_some();
    let Some(session) = session else {
        return CancelDecision::Deny("no active session");
    };

    if !v2_cancel && session.sender_version != "1.0" {
        return CancelDecision::Deny("v1 cancel against v2 session");
    }
    if session.sender_ip != requester_ip {
        return CancelDecision::Deny("ip mismatch");
    }
    if v2_cancel && requested_session_id != Some(session.session_id.as_str()) {
        return CancelDecision::Deny("session id mismatch");
    }
    if session.status != SessionStatus::Sending
        && session.status != SessionStatus::FinishedWithErrors
    {
        return CancelDecision::Deny("session not cancellable in current state");
    }
    CancelDecision::Allow
}

async fn cancel_handler(
    State(state): State<ServerState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Query(params): Query<HashMap<String, String>>,
) -> StatusCode {
    let requested = params.get("sessionId").cloned();
    let mut guard = state.inner.lock().await;

    match evaluate_cancel(
        guard.session.as_ref(),
        &addr.ip().to_string(),
        requested.as_deref(),
    ) {
        CancelDecision::Allow => {
            state.notify_human("Transfer cancelled by sender.");
            if state.output == OutputMode::Json {
                state.emit_json_event(ReceiveEventJson::TransferCancelled);
            }
            state.emit_event(ReceiveEvent::TransferCancelled);
            guard.session = None;
            StatusCode::OK
        }
        CancelDecision::Deny(_) => StatusCode::FORBIDDEN,
    }
}

/// Owns the reservation until a successful upload is committed. Failed,
/// cancelled, or interrupted handlers remove their partial file on drop.
struct ReservedReceiveFile {
    file: Option<std::fs::File>,
    parent: cap_std::fs::Dir,
    name: std::ffi::OsString,
    path: PathBuf,
    keep: bool,
}

impl Drop for ReservedReceiveFile {
    fn drop(&mut self) {
        drop(self.file.take());
        if !self.keep {
            let _ = self.parent.remove_file(&self.name);
        }
    }
}

async fn save_stream(
    body: Body,
    reserved: &mut ReservedReceiveFile,
    expected_size: u64,
) -> Result<u64, (StatusCode, String)> {
    let mut file = File::from_std(reserved.file.take().expect("reserved file"));
    let mut stream = body.into_data_stream();
    let mut total = 0u64;
    let io_error = |e: std::io::Error| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string());

    while let Some(chunk) = tokio::time::timeout(SESSION_IDLE_TIMEOUT, stream.next())
        .await
        .map_err(|_| (StatusCode::REQUEST_TIMEOUT, "Upload stalled".to_owned()))?
    {
        let chunk = chunk.map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
        if chunk.len() as u64 > expected_size - total {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                "Upload exceeds advertised size".into(),
            ));
        }
        file.write_all(&chunk).await.map_err(io_error)?;
        total += chunk.len() as u64;
    }
    if total != expected_size {
        return Err((
            StatusCode::BAD_REQUEST,
            "Upload shorter than advertised size".into(),
        ));
    }
    file.flush().await.map_err(io_error)?;
    Ok(total)
}

fn reserve_receive_file(dir: &Path, file_name: &str) -> Result<ReservedReceiveFile> {
    validate_relative_file_name(file_name).map_err(anyhow::Error::msg)?;
    std::fs::create_dir_all(dir)?;
    let root = cap_std::fs::Dir::open_ambient_dir(dir, cap_std::ambient_authority())?;
    let relative = Path::new(file_name);
    let parent_path = relative
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    root.create_dir_all(parent_path)?;
    let parent = root.open_dir(parent_path)?;
    let leaf = relative
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("Invalid file name"))?;
    let leaf_path = Path::new(leaf);
    let stem = leaf_path.file_stem().unwrap_or(leaf).to_string_lossy();
    let ext = leaf_path
        .extension()
        .map(|e| format!(".{}", e.to_string_lossy()))
        .unwrap_or_default();
    let mut index = 0;
    loop {
        let name = match index {
            0 => leaf.to_os_string(),
            1..1000 => format!("{stem} ({index}){ext}").into(),
            _ => format!("{stem}-{}{ext}", Uuid::new_v4()).into(),
        };
        let mut options = cap_std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        match parent.open_with(&name, &options) {
            Ok(file) => {
                return Ok(ReservedReceiveFile {
                    file: Some(file.into_std()),
                    path: dir.join(parent_path).join(&name),
                    parent,
                    name,
                    keep: false,
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => index += 1,
            Err(error) => return Err(error.into()),
        }
    }
}

fn validate_relative_file_name(file_name: &str) -> Result<(), String> {
    let path = Path::new(file_name);
    if path.is_absolute() {
        return Err("Absolute file paths are not allowed".to_string());
    }
    if path.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err("Path traversal is not allowed".to_string());
    }
    Ok(())
}

fn info_response_json(state: &ServerState) -> InfoResponseJson {
    InfoResponseJson {
        alias: state.config.alias.clone(),
        version: crate::config::PROTOCOL_VERSION.to_string(),
        device_model: Some(crate::util::os_display_name()),
        device_type: Some("headless".to_string()),
        fingerprint: state.identity.fingerprint.clone(),
        download: false,
    }
}

fn is_self_fingerprint(state: &ServerState, sender_fingerprint: Option<&String>) -> bool {
    sender_fingerprint.is_some_and(|fp| fp == &state.identity.fingerprint)
}

fn info_response(state: &ServerState) -> InfoResponseDtoV2 {
    InfoResponseDtoV2 {
        alias: state.config.alias.clone(),
        version: crate::config::PROTOCOL_VERSION.to_string(),
        device_model: Some(crate::util::os_display_name()),
        device_type: Some(localsend::model::discovery::DeviceType::Headless),
        fingerprint: state.identity.fingerprint.clone(),
        download: false,
    }
}

fn register_response(state: &ServerState) -> RegisterResponseDtoV2 {
    RegisterResponseDtoV2 {
        alias: state.config.alias.clone(),
        version: crate::config::PROTOCOL_VERSION.to_string(),
        device_model: Some(crate::util::os_display_name()),
        device_type: Some(localsend::model::discovery::DeviceType::Headless),
        fingerprint: state.identity.fingerprint.clone(),
        download: false,
    }
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(ErrorResponse {
            message: message.to_string(),
        }),
    )
        .into_response()
}

async fn check_pin(
    state: &ServerState,
    client_ip: std::net::IpAddr,
    query: &HashMap<String, String>,
    headers: &HeaderMap,
) -> Option<Response> {
    let Some(expected) = state.receive_pin.as_ref().filter(|pin| !pin.is_empty()) else {
        return None;
    };

    let mut attempts_guard = state.pin_attempts.lock().await;
    // Evict idle entries so the map stays bounded in long-running receivers.
    let now = Instant::now();
    attempts_guard.retain(|_, entry| now.duration_since(entry.last) < PIN_ATTEMPTS_TTL);

    let entry = attempts_guard
        .entry(client_ip.to_string())
        .or_insert_with(|| PinAttempts {
            count: 0,
            last: now,
        });
    if entry.count >= 3 {
        return Some(error_response(
            StatusCode::TOO_MANY_REQUESTS,
            "Too many attempts.",
        ));
    }

    let provided = query.get("pin").cloned().or_else(|| {
        headers
            .get("pin")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    });

    match provided.as_deref() {
        None | Some("") => Some(error_response(StatusCode::UNAUTHORIZED, "PIN required")),
        Some(value) if value == expected.as_str() => None,
        Some(_) => {
            // Non-empty wrong PIN: count the attempt.
            let previous = entry.count;
            entry.count = previous + 1;
            entry.last = now;
            if previous == 2 {
                Some(error_response(
                    StatusCode::TOO_MANY_REQUESTS,
                    "Too many attempts.",
                ))
            } else {
                Some(error_response(StatusCode::UNAUTHORIZED, "Invalid PIN"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_path_preserves_nested_directories() {
        let dir = std::env::temp_dir().join(format!("lsend-nested-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();

        let resolved = reserve_receive_file(&dir, "photos/vacation/beach.jpg")
            .map(|file| file.path.clone())
            .expect("resolve");
        assert_eq!(
            resolved,
            dir.join("photos").join("vacation").join("beach.jpg")
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn unique_path_rejects_path_traversal() {
        let dir = std::env::temp_dir();
        assert!(reserve_receive_file(&dir, "../escape.txt").is_err());
        assert!(reserve_receive_file(&dir, "nested/../../escape.txt").is_err());
    }

    #[test]
    fn unique_path_deduplicates_within_nested_directory() {
        let dir = std::env::temp_dir().join(format!("lsend-dedupe-{}", Uuid::new_v4()));
        let nested = dir.join("docs");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(nested.join("readme.md"), b"existing").unwrap();

        let resolved = reserve_receive_file(&dir, "docs/readme.md")
            .map(|file| file.path.clone())
            .expect("resolve");
        assert_eq!(resolved, nested.join("readme (1).md"));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn parses_official_style_prepare_upload_with_token_field() {
        let json = r#"{
            "info": {
                "alias": "Cute Apple",
                "version": "2.1",
                "deviceModel": "iPhone",
                "deviceType": "mobile",
                "token": "sender-fingerprint",
                "port": 53317,
                "protocol": "https",
                "hasWebInterface": false
            },
            "files": {
                "abc": {
                    "id": "abc",
                    "fileName": "hello.txt",
                    "size": 5,
                    "fileType": "text/plain",
                    "preview": "hello"
                }
            }
        }"#;
        let parsed = parse_prepare_upload_request(json).expect("parse");
        assert_eq!(parsed.sender_alias, "Cute Apple");
        assert_eq!(parsed.files.len(), 1);
        assert_eq!(extract_embedded_text_message(&parsed.files), Some("hello"));
    }

    #[test]
    fn parses_prepare_upload_with_fingerprint_and_hash_fields() {
        let json = r#"{
            "info": {
                "alias": "Sender",
                "version": "2.1",
                "fingerprint": "abc123",
                "port": 53317,
                "protocol": "https",
                "download": false
            },
            "files": {
                "f1": {
                    "id": "f1",
                    "fileName": "photo.png",
                    "size": 1024,
                    "fileType": "image/png",
                    "hash": "deadbeef"
                }
            }
        }"#;
        let parsed = parse_prepare_upload_request(json).expect("parse");
        assert_eq!(
            parsed.files.get("f1").and_then(|f| f.sha256.as_deref()),
            Some("deadbeef")
        );
        assert!(extract_embedded_text_message(&parsed.files).is_none());
    }

    #[test]
    fn parses_prepare_upload_without_file_id_using_map_key() {
        let json = r#"{
            "info": {
                "alias": "Sender",
                "deviceType": "mobile"
            },
            "files": {
                "f1": {
                    "fileName": "photo.png",
                    "size": 1024,
                    "fileType": "image"
                }
            }
        }"#;
        let parsed = parse_prepare_upload_request(json).expect("parse");
        assert_eq!(parsed.files.get("f1").map(|f| f.id.as_str()), Some("f1"));
    }

    #[test]
    fn parses_prepare_upload_with_string_size() {
        let json = r#"{
            "info": { "alias": "Sender" },
            "files": {
                "f1": {
                    "id": "f1",
                    "fileName": "photo.png",
                    "size": "1024",
                    "fileType": "image/jpeg"
                }
            }
        }"#;
        let parsed = parse_prepare_upload_request(json).expect("parse");
        assert_eq!(parsed.files.get("f1").map(|f| f.size), Some(1024));
    }

    fn dummy_session(id: &str, ip: &str, version: &str, status: SessionStatus) -> ReceiveSession {
        ReceiveSession {
            last_activity: Instant::now(),
            session_id: id.to_string(),
            sender_ip: ip.to_string(),
            sender_version: version.to_string(),
            destination_dir: PathBuf::from("/tmp"),
            files: HashMap::new(),
            status,
        }
    }

    #[test]
    fn evaluate_cancel_allows_v1_against_v1_session() {
        let s = dummy_session("s1", "10.0.0.1", "1.0", SessionStatus::Sending);
        assert_eq!(
            evaluate_cancel(Some(&s), "10.0.0.1", None),
            CancelDecision::Allow
        );
    }

    #[test]
    fn evaluate_cancel_allows_v2_with_matching_session_id() {
        let s = dummy_session("abc", "10.0.0.1", "2.1", SessionStatus::Sending);
        assert_eq!(
            evaluate_cancel(Some(&s), "10.0.0.1", Some("abc")),
            CancelDecision::Allow
        );
    }

    #[test]
    fn evaluate_cancel_allows_finished_with_errors() {
        let s = dummy_session("abc", "10.0.0.1", "2.1", SessionStatus::FinishedWithErrors);
        assert_eq!(
            evaluate_cancel(Some(&s), "10.0.0.1", Some("abc")),
            CancelDecision::Allow
        );
    }

    #[test]
    fn evaluate_cancel_denies_v1_against_v2_session() {
        let s = dummy_session("abc", "10.0.0.1", "2.1", SessionStatus::Sending);
        assert!(matches!(
            evaluate_cancel(Some(&s), "10.0.0.1", None),
            CancelDecision::Deny(_)
        ));
    }

    #[test]
    fn evaluate_cancel_denies_ip_mismatch() {
        let s = dummy_session("abc", "10.0.0.1", "2.1", SessionStatus::Sending);
        assert!(matches!(
            evaluate_cancel(Some(&s), "10.0.0.2", Some("abc")),
            CancelDecision::Deny(_)
        ));
    }

    #[test]
    fn evaluate_cancel_denies_v2_with_wrong_session_id() {
        let s = dummy_session("abc", "10.0.0.1", "2.1", SessionStatus::Sending);
        assert!(matches!(
            evaluate_cancel(Some(&s), "10.0.0.1", Some("xyz")),
            CancelDecision::Deny(_)
        ));
    }

    #[test]
    fn evaluate_cancel_denies_when_no_session() {
        assert!(matches!(
            evaluate_cancel(None, "10.0.0.1", Some("abc")),
            CancelDecision::Deny(_)
        ));
    }

    #[test]
    fn evaluate_cancel_denies_in_finished_state() {
        // Once a transfer is `Finished`, it is no longer cancellable.
        let s = dummy_session("abc", "10.0.0.1", "2.1", SessionStatus::Finished);
        assert!(matches!(
            evaluate_cancel(Some(&s), "10.0.0.1", Some("abc")),
            CancelDecision::Deny(_)
        ));
    }

    #[test]
    fn is_text_file_type_matches_text_and_text_slash() {
        assert!(is_text_file_type("text"));
        assert!(is_text_file_type("text/plain"));
        assert!(is_text_file_type("TEXT/PLAIN"));
        assert!(!is_text_file_type("image/png"));
        assert!(!is_text_file_type("application/json"));
    }

    #[test]
    fn extract_embedded_text_message_only_for_single_text_file() {
        let mut files = HashMap::new();
        files.insert(
            "a".to_string(),
            FileDto {
                id: "a".to_string(),
                file_name: "a.txt".to_string(),
                size: 5,
                file_type: "text/plain".to_string(),
                sha256: None,
                preview: Some("hello".to_string()),
                metadata: None,
            },
        );
        assert_eq!(extract_embedded_text_message(&files), Some("hello"));

        // Non-text file type is not a text message.
        let mut files = HashMap::new();
        files.insert(
            "a".to_string(),
            FileDto {
                id: "a".to_string(),
                file_name: "a.png".to_string(),
                size: 5,
                file_type: "image/png".to_string(),
                sha256: None,
                preview: Some("hello".to_string()),
                metadata: None,
            },
        );
        assert_eq!(extract_embedded_text_message(&files), None);

        // Multiple files: not a text message even if all are text.
        let mut files = HashMap::new();
        for k in ["a", "b"] {
            files.insert(
                k.to_string(),
                FileDto {
                    id: k.to_string(),
                    file_name: format!("{k}.txt"),
                    size: 1,
                    file_type: "text/plain".to_string(),
                    sha256: None,
                    preview: Some("hi".to_string()),
                    metadata: None,
                },
            );
        }
        assert_eq!(extract_embedded_text_message(&files), None);
    }

    #[test]
    fn validate_relative_file_name_accepts_safe_names() {
        assert!(validate_relative_file_name("file.txt").is_ok());
        assert!(validate_relative_file_name("nested/file.txt").is_ok());
        assert!(validate_relative_file_name("a/b/c.txt").is_ok());
        assert!(validate_relative_file_name("file with spaces.txt").is_ok());
    }

    #[test]
    fn validate_relative_file_name_rejects_absolute_paths() {
        assert!(validate_relative_file_name("/etc/passwd").is_err());
        assert!(validate_relative_file_name("/file.txt").is_err());
    }

    #[test]
    fn validate_relative_file_name_rejects_parent_traversal() {
        assert!(validate_relative_file_name("../escape.txt").is_err());
        assert!(validate_relative_file_name("nested/../escape.txt").is_err());
        assert!(validate_relative_file_name("..").is_err());
    }

    #[test]
    fn parse_prepare_upload_rejects_empty_body() {
        let err = parse_prepare_upload_request("").expect_err("should fail");
        assert!(err.to_string().contains("json error"));
    }

    #[test]
    fn parse_prepare_upload_rejects_missing_info() {
        let json = r#"{"files": {}}"#;
        let err = parse_prepare_upload_request(json).expect_err("should fail");
        assert!(err.to_string().contains("info"));
    }

    #[test]
    fn parse_prepare_upload_rejects_missing_files() {
        let json = r#"{"info": {"alias": "Sender"}}"#;
        let err = parse_prepare_upload_request(json).expect_err("should fail");
        assert!(err.to_string().contains("files"));
    }

    #[test]
    fn parse_prepare_upload_rejects_file_missing_name() {
        let json = r#"{
            "info": {"alias": "Sender"},
            "files": {"f1": {"id": "f1", "size": 1, "fileType": "image"}}
        }"#;
        let err = parse_prepare_upload_request(json).expect_err("should fail");
        assert!(err.to_string().contains("fileName"));
    }

    #[test]
    fn parse_prepare_upload_rejects_file_missing_size() {
        let json = r#"{
            "info": {"alias": "Sender"},
            "files": {"f1": {"id": "f1", "fileName": "x.png", "fileType": "image"}}
        }"#;
        let err = parse_prepare_upload_request(json).expect_err("should fail");
        assert!(err.to_string().contains("size"));
    }

    #[test]
    fn parse_prepare_upload_rejects_file_missing_type() {
        let json = r#"{
            "info": {"alias": "Sender"},
            "files": {"f1": {"id": "f1", "fileName": "x.png", "size": 1}}
        }"#;
        let err = parse_prepare_upload_request(json).expect_err("should fail");
        assert!(err.to_string().contains("fileType"));
    }

    #[test]
    fn parse_prepare_upload_falls_back_to_snake_case_keys() {
        // Some clients may send snake_case keys instead of camelCase.
        let json = r#"{
            "info": {"alias": "Sender", "version": "2.1"},
            "files": {
                "f1": {
                    "id": "f1",
                    "file_name": "x.png",
                    "size": 1,
                    "file_type": "image/png"
                }
            }
        }"#;
        let parsed = parse_prepare_upload_request(json).expect("parse");
        assert_eq!(
            parsed.files.get("f1").map(|f| f.file_name.clone()),
            Some("x.png".to_string())
        );
    }

    #[test]
    fn parse_prepare_upload_defaults_sender_version_to_v1() {
        let json = r#"{
            "info": {"alias": "Sender"},
            "files": {"f1": {"id": "f1", "fileName": "x", "size": 1, "fileType": "image"}}
        }"#;
        let parsed = parse_prepare_upload_request(json).expect("parse");
        assert_eq!(parsed.sender_version, "1.0");
    }

    #[test]
    fn parse_prepare_upload_captures_explicit_sender_version() {
        let json = r#"{
            "info": {"alias": "Sender", "version": "2.1"},
            "files": {"f1": {"id": "f1", "fileName": "x", "size": 1, "fileType": "image"}}
        }"#;
        let parsed = parse_prepare_upload_request(json).expect("parse");
        assert_eq!(parsed.sender_version, "2.1");
    }

    #[test]
    fn unique_path_returns_original_when_no_collision() {
        let dir = std::env::temp_dir().join(format!("lsend-fresh-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let resolved = reserve_receive_file(&dir, "new.txt")
            .map(|file| file.path.clone())
            .expect("resolve");
        assert_eq!(resolved, dir.join("new.txt"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn unique_path_dedup_chain_within_same_dir() {
        let dir = std::env::temp_dir().join(format!("lsend-chain-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a.txt"), b"x").unwrap();
        std::fs::write(dir.join("a (1).txt"), b"x").unwrap();
        let resolved = reserve_receive_file(&dir, "a.txt")
            .map(|file| file.path.clone())
            .expect("resolve");
        assert_eq!(resolved, dir.join("a (2).txt"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn unique_path_dedup_preserves_extension() {
        let dir = std::env::temp_dir().join(format!("lsend-ext-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("photo.png"), b"x").unwrap();
        let resolved = reserve_receive_file(&dir, "photo.png")
            .map(|file| file.path.clone())
            .expect("resolve");
        assert_eq!(resolved, dir.join("photo (1).png"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn unique_path_handles_filename_without_extension() {
        let dir = std::env::temp_dir().join(format!("lsend-noext-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("README"), b"x").unwrap();
        let resolved = reserve_receive_file(&dir, "README")
            .map(|file| file.path.clone())
            .expect("resolve");
        assert_eq!(resolved, dir.join("README (1)"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn unique_path_falls_back_to_uuid_suffix_when_all_slots_taken() {
        // Saturate the (1)..(999) range and verify the UUID fallback path runs.
        let dir = std::env::temp_dir().join(format!("lsend-full-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("f.txt"), b"x").unwrap();
        for i in 1..1000 {
            std::fs::write(dir.join(format!("f ({i}).txt")), b"x").unwrap();
        }
        let resolved = reserve_receive_file(&dir, "f.txt")
            .map(|file| file.path.clone())
            .expect("resolve");
        let name = resolved.file_name().unwrap().to_string_lossy().to_string();
        assert!(name.starts_with("f-"), "got: {name}");
        assert!(name.ends_with(".txt"), "got: {name}");
        std::fs::remove_dir_all(&dir).ok();
    }
}

#[cfg(test)]
mod integration_tests {
    //! End-to-end tests that exercise the full axum router via
    //! `tower::ServiceExt::oneshot`. These complement the unit tests above by
    //! verifying the actual HTTP-level behavior (status codes, JSON shape,
    //! PIN enforcement, session lifecycle, file persistence).

    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn make_state(receive_dir: &std::path::Path, pin: Option<String>) -> ServerState {
        let cfg = AppConfig::new(
            Some("Tester".into()),
            53317,
            true,
            Some(receive_dir.to_path_buf()),
        )
        .expect("config");
        let identity = Identity {
            cert_pem: String::new(),
            key_pem: String::new(),
            fingerprint: "test-fp".to_string(),
        };
        ServerState::new(cfg, identity, pin, OutputMode::Json, false, None)
    }

    fn with_connect_info(req: Request<Body>, addr: SocketAddr) -> Request<Body> {
        let (mut parts, body) = req.into_parts();
        parts.extensions.insert(ConnectInfo(addr));
        Request::from_parts(parts, body)
    }

    async fn call(
        state: &ServerState,
        req: Request<Body>,
        addr: SocketAddr,
    ) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
        let app = build_router(state.clone());
        let resp = app
            .oneshot(with_connect_info(req, addr))
            .await
            .expect("router responds");
        let (parts, body) = resp.into_parts();
        let bytes = body.collect().await.expect("collect body").to_bytes();
        (parts.status, parts.headers, bytes.to_vec())
    }

    fn json_request(method: &str, uri: &str, body: Option<&str>) -> Request<Body> {
        let body = body.unwrap_or("{}").to_string();
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .expect("build request")
    }

    fn raw_request(method: &str, uri: &str, body: Vec<u8>) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .expect("build request")
    }

    fn fresh_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("lsend-int-{}-{}", tag, uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn peer_addr() -> SocketAddr {
        SocketAddr::from(([10, 0, 0, 1], 53317))
    }

    fn make_prepare_body(version: &str, file_count: usize) -> String {
        let files: String = (1..=file_count)
            .map(|i| {
                format!(
                    r#""f{i}": {{"id":"f{i}","fileName":"a{}.bin","size":4,"fileType":"application/octet-stream"}}"#,
                    i - 1
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        format!(
            r#"{{"info":{{"alias":"Sender","version":"{version}","fingerprint":"sfp"}},"files":{{{files}}}}}"#
        )
    }

    #[tokio::test]
    async fn info_v1_returns_alias_version_and_lowercase_device_type() {
        let dir = fresh_dir("info");
        let state = make_state(&dir, None);
        let (status, _, body) = call(
            &state,
            Request::builder()
                .uri("/api/localsend/v1/info?fingerprint=other")
                .body(Body::empty())
                .unwrap(),
            peer_addr(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["alias"], "Tester");
        assert_eq!(json["version"], "2.1");
        assert_eq!(json["deviceType"], "headless");
        assert_eq!(json["fingerprint"], "test-fp");
        assert_eq!(json["download"], false);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn info_v1_returns_412_for_self_fingerprint() {
        let dir = fresh_dir("info-self");
        let state = make_state(&dir, None);
        let (status, _, body) = call(
            &state,
            Request::builder()
                .uri("/api/localsend/v1/info?fingerprint=test-fp")
                .body(Body::empty())
                .unwrap(),
            peer_addr(),
        )
        .await;
        assert_eq!(status, StatusCode::PRECONDITION_FAILED);
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["message"], "Self-discovered");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn registrations_report_peer_endpoints_on_both_protocol_versions() {
        let dir = fresh_dir("registration-events");
        let mut state = make_state(&dir, None);
        let (tx, mut rx) = mpsc::unbounded_channel();
        state.event_tx = Some(tx);
        for version in ["v1", "v2"] {
            let body = r#"{"alias":"Peer","fingerprint":"peer-fp","port":6000,"protocol":"http","version":"2.1"}"#;
            let (status, _, _) = call(
                &state,
                json_request(
                    "POST",
                    &format!("/api/localsend/{version}/register"),
                    Some(body),
                ),
                peer_addr(),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            match rx.try_recv().unwrap() {
                ReceiveEvent::PeerDiscovered(device) => {
                    assert_eq!(device.ip, peer_addr().ip().to_string());
                    assert_eq!(device.alias, "Peer");
                    assert_eq!(device.port, 6000);
                    assert!(!device.https);
                    assert_eq!(device.fingerprint, "peer-fp");
                }
                other => panic!("unexpected event: {other:?}"),
            }
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn shutdown_drains_stalled_upload_and_removes_partial_file() {
        for https in [false, true] {
            let dir = fresh_dir("shutdown-upload");
            let mut state = make_state(&dir, None);
            state.config.https = https;
            state.identity = Identity::load_or_create(&dir.join("identity"), https).unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let server_state = state.clone();
            let server = tokio::spawn(async move {
                if https {
                    run_https(server_state, listener).await
                } else {
                    run_http(server_state, listener).await
                }
            });
            let client = crate::network::build_reqwest_client(&state.identity, https).unwrap();
            let base = format!(
                "{}://127.0.0.1:{port}/api/localsend/v2",
                if https { "https" } else { "http" }
            );
            let prepared: serde_json::Value = client.post(format!("{base}/prepare-upload"))
                .json(&serde_json::json!({
                    "info": {"alias": "Sender", "version": "2.1"},
                    "files": {"f1": {"id": "f1", "fileName": "partial.bin", "size": 1000000, "fileType": "application/octet-stream"}}
                })).send().await.unwrap().json().await.unwrap();
            let session = prepared["sessionId"].as_str().unwrap();
            let token = prepared["files"]["f1"].as_str().unwrap();
            let (chunk_tx, chunk_rx) = mpsc::unbounded_channel::<Result<Vec<u8>, std::io::Error>>();
            chunk_tx.send(Ok(vec![42; 4])).unwrap();
            let stream = futures_util::stream::unfold(chunk_rx, |mut rx| async move {
                rx.recv().await.map(|chunk| (chunk, rx))
            });
            let upload = tokio::spawn(
                client
                    .post(format!(
                        "{base}/upload?sessionId={session}&fileId=f1&token={token}"
                    ))
                    .body(localsend::reqwest::Body::wrap_stream(stream))
                    .send(),
            );
            tokio::time::timeout(Duration::from_secs(5), async {
                while !dir.join("partial.bin").exists() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            state.shutdown();
            tokio::time::timeout(Duration::from_secs(5), server)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(
                !dir.join("partial.bin").exists(),
                "shutdown left a partial file (https={https})"
            );
            drop(chunk_tx);
            let _ = upload.await;
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    #[tokio::test]
    async fn register_v1_echoes_alias_and_fingerprint() {
        let dir = fresh_dir("reg-v1");
        let state = make_state(&dir, None);
        let body = r#"{"alias":"Peer","fingerprint":"peer-fp","port":53317,"protocol":"http"}"#;
        let (status, _, resp) = call(
            &state,
            json_request("POST", "/api/localsend/v1/register", Some(body)),
            peer_addr(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let json: serde_json::Value = serde_json::from_slice(&resp).unwrap();
        assert_eq!(json["alias"], "Tester");
        assert_eq!(json["deviceType"], "headless");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn register_v1_rejects_self_fingerprint() {
        let dir = fresh_dir("reg-self");
        let state = make_state(&dir, None);
        let body = r#"{"alias":"Self","fingerprint":"test-fp"}"#;
        let (status, _, _) = call(
            &state,
            json_request("POST", "/api/localsend/v1/register", Some(body)),
            peer_addr(),
        )
        .await;
        assert_eq!(status, StatusCode::PRECONDITION_FAILED);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn register_v1_rejects_missing_fingerprint() {
        let dir = fresh_dir("reg-no-fp");
        let state = make_state(&dir, None);
        let body = r#"{"alias":"X"}"#;
        let (status, _, _) = call(
            &state,
            json_request("POST", "/api/localsend/v1/register", Some(body)),
            peer_addr(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn prepare_upload_v2_returns_session_id_and_tokens() {
        let dir = fresh_dir("prep");
        let state = make_state(&dir, None);
        let body = make_prepare_body("2.1", 2);
        let (status, _, resp) = call(
            &state,
            json_request("POST", "/api/localsend/v2/prepare-upload", Some(&body)),
            peer_addr(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let json: serde_json::Value = serde_json::from_slice(&resp).unwrap();
        // sessionId is a UUIDv4.
        let sid = json["sessionId"].as_str().expect("sessionId");
        assert_eq!(sid.len(), 36);
        // Two files accepted, both with tokens.
        let files = json["files"].as_object().expect("files");
        assert_eq!(files.len(), 2);
        for (_, token) in files {
            assert!(token.as_str().unwrap().len() == 36);
        }
        // A second prepare-upload should now be blocked (session in use).
        let (status2, _, _) = call(
            &state,
            json_request("POST", "/api/localsend/v2/prepare-upload", Some(&body)),
            peer_addr(),
        )
        .await;
        assert_eq!(status2, StatusCode::CONFLICT);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn prepare_upload_v1_returns_legacy_file_map() {
        let dir = fresh_dir("prep-v1");
        let state = make_state(&dir, None);
        let body = make_prepare_body("1.0", 1);
        let (status, _, resp) = call(
            &state,
            json_request("POST", "/api/localsend/v1/send-request", Some(&body)),
            peer_addr(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        // v1 response is a plain object {fileId: token} without sessionId.
        let json: serde_json::Value = serde_json::from_slice(&resp).unwrap();
        assert!(json.get("sessionId").is_none());
        let files = json.as_object().expect("map");
        assert_eq!(files.len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn prepare_upload_rejects_empty_files() {
        let dir = fresh_dir("prep-empty");
        let state = make_state(&dir, None);
        let body = r#"{"info":{"alias":"X"},"files":{}}"#;
        let (status, _, _) = call(
            &state,
            json_request("POST", "/api/localsend/v2/prepare-upload", Some(body)),
            peer_addr(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn prepare_upload_rejects_malformed_body() {
        let dir = fresh_dir("prep-bad");
        let state = make_state(&dir, None);
        let (status, _, _) = call(
            &state,
            raw_request(
                "POST",
                "/api/localsend/v2/prepare-upload",
                b"not json".to_vec(),
            ),
            peer_addr(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn prepare_upload_returns_204_for_embedded_text_message() {
        let dir = fresh_dir("prep-msg");
        let state = make_state(&dir, None);
        let body = r#"{
            "info": {"alias": "X", "version": "2.1", "fingerprint": "sfp"},
            "files": {
                "f1": {
                    "id": "f1",
                    "fileName": "msg.txt",
                    "size": 5,
                    "fileType": "text/plain",
                    "preview": "hello"
                }
            }
        }"#;
        let (status, _, _) = call(
            &state,
            json_request("POST", "/api/localsend/v2/prepare-upload", Some(body)),
            peer_addr(),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        // A follow-up prepare-upload from a different peer should succeed
        // because the message path closes the session immediately.
        let body2 = make_prepare_body("2.1", 1);
        let (status2, _, _) = call(
            &state,
            json_request("POST", "/api/localsend/v2/prepare-upload", Some(&body2)),
            peer_addr(),
        )
        .await;
        assert_eq!(status2, StatusCode::OK);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn prepare_upload_requires_pin_when_set() {
        let dir = fresh_dir("prep-pin");
        let state = make_state(&dir, Some("123456".into()));
        let body = make_prepare_body("2.1", 1);
        // No PIN → 401
        let (status, _, _) = call(
            &state,
            json_request("POST", "/api/localsend/v2/prepare-upload?pin=", Some(&body)),
            peer_addr(),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        // Correct PIN → 200
        let (status, _, _) = call(
            &state,
            json_request(
                "POST",
                "/api/localsend/v2/prepare-upload?pin=123456",
                Some(&body),
            ),
            peer_addr(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        // Wrong PIN → 401, then 429 after 3 attempts
        for attempt in 0..2 {
            let (status, _, _) = call(
                &state,
                json_request(
                    "POST",
                    "/api/localsend/v2/prepare-upload?pin=wrong",
                    Some(&body),
                ),
                peer_addr(),
            )
            .await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "attempt {attempt}");
        }
        let (status, _, _) = call(
            &state,
            json_request(
                "POST",
                "/api/localsend/v2/prepare-upload?pin=wrong",
                Some(&body),
            ),
            peer_addr(),
        )
        .await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn prepare_upload_pin_works_via_header_too() {
        let dir = fresh_dir("prep-pin-hdr");
        let state = make_state(&dir, Some("abcdef".into()));
        let body = make_prepare_body("2.1", 1);
        let req = Request::builder()
            .method("POST")
            .uri("/api/localsend/v2/prepare-upload")
            .header("content-type", "application/json")
            .header("pin", "abcdef")
            .body(Body::from(body))
            .unwrap();
        let (status, _, _) = call(&state, req, peer_addr()).await;
        assert_eq!(status, StatusCode::OK);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn upload_v2_writes_file_to_receive_dir() {
        let dir = fresh_dir("upload");
        let state = make_state(&dir, None);
        let body = make_prepare_body("2.1", 1);
        let (_, _, resp) = call(
            &state,
            json_request("POST", "/api/localsend/v2/prepare-upload", Some(&body)),
            peer_addr(),
        )
        .await;
        let json: serde_json::Value = serde_json::from_slice(&resp).unwrap();
        let sid = json["sessionId"].as_str().unwrap().to_string();
        let token = json["files"]["f1"].as_str().unwrap().to_string();

        // Upload the actual file bytes.
        let upload_uri = format!(
            "/api/localsend/v2/upload?sessionId={}&fileId=f1&token={}",
            sid, token
        );
        let req = Request::builder()
            .method("POST")
            .uri(&upload_uri)
            .header("content-type", "application/octet-stream")
            .body(Body::from(b"abcd".to_vec()))
            .unwrap();
        let (status, _, _) = call(&state, req, peer_addr()).await;
        assert_eq!(status, StatusCode::OK);
        // The file should now exist on disk.
        let written = std::fs::read(dir.join("a0.bin")).expect("written");
        assert_eq!(written, b"abcd");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn upload_v2_rejects_wrong_session_id() {
        let dir = fresh_dir("upload-bad-sid");
        let state = make_state(&dir, None);
        let body = make_prepare_body("2.1", 1);
        let (_, _, resp) = call(
            &state,
            json_request("POST", "/api/localsend/v2/prepare-upload", Some(&body)),
            peer_addr(),
        )
        .await;
        let json: serde_json::Value = serde_json::from_slice(&resp).unwrap();
        let token = json["files"]["f1"].as_str().unwrap().to_string();
        let upload_uri = format!(
            "/api/localsend/v2/upload?sessionId=wrong&fileId=f1&token={}",
            token
        );
        let req = Request::builder()
            .method("POST")
            .uri(&upload_uri)
            .body(Body::from(b"abcd".to_vec()))
            .unwrap();
        let (status, _, _) = call(&state, req, peer_addr()).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn upload_v2_rejects_wrong_token() {
        let dir = fresh_dir("upload-bad-tok");
        let state = make_state(&dir, None);
        let body = make_prepare_body("2.1", 1);
        let (_, _, resp) = call(
            &state,
            json_request("POST", "/api/localsend/v2/prepare-upload", Some(&body)),
            peer_addr(),
        )
        .await;
        let json: serde_json::Value = serde_json::from_slice(&resp).unwrap();
        let sid = json["sessionId"].as_str().unwrap().to_string();
        let upload_uri = format!(
            "/api/localsend/v2/upload?sessionId={}&fileId=f1&token=wrong",
            sid
        );
        let req = Request::builder()
            .method("POST")
            .uri(&upload_uri)
            .body(Body::from(b"abcd".to_vec()))
            .unwrap();
        let (status, _, _) = call(&state, req, peer_addr()).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn upload_v2_missing_parameters_returns_400() {
        let dir = fresh_dir("upload-400");
        let state = make_state(&dir, None);
        // No sessionId, no fileId, no token → 400
        let req = Request::builder()
            .method("POST")
            .uri("/api/localsend/v2/upload")
            .body(Body::from(b"abcd".to_vec()))
            .unwrap();
        let (status, _, _) = call(&state, req, peer_addr()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn cancel_v2_succeeds_for_active_session() {
        let dir = fresh_dir("cancel-ok");
        let state = make_state(&dir, None);
        // Establish a session.
        let body = make_prepare_body("2.1", 1);
        let (_, _, resp) = call(
            &state,
            json_request("POST", "/api/localsend/v2/prepare-upload", Some(&body)),
            peer_addr(),
        )
        .await;
        let json: serde_json::Value = serde_json::from_slice(&resp).unwrap();
        let sid = json["sessionId"].as_str().unwrap().to_string();
        // Cancel from the same peer IP with the matching sessionId.
        let (status, _, _) = call(
            &state,
            Request::builder()
                .method("POST")
                .uri(format!("/api/localsend/v2/cancel?sessionId={sid}"))
                .body(Body::empty())
                .unwrap(),
            peer_addr(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        // A subsequent prepare-upload from the same peer should now succeed.
        let body2 = make_prepare_body("2.1", 1);
        let (status2, _, _) = call(
            &state,
            json_request("POST", "/api/localsend/v2/prepare-upload", Some(&body2)),
            peer_addr(),
        )
        .await;
        assert_eq!(status2, StatusCode::OK);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn cancel_v2_rejects_wrong_ip() {
        let dir = fresh_dir("cancel-ip");
        let state = make_state(&dir, None);
        let body = make_prepare_body("2.1", 1);
        let (_, _, resp) = call(
            &state,
            json_request("POST", "/api/localsend/v2/prepare-upload", Some(&body)),
            peer_addr(),
        )
        .await;
        let json: serde_json::Value = serde_json::from_slice(&resp).unwrap();
        let sid = json["sessionId"].as_str().unwrap().to_string();
        // Cancel from a different IP → 403.
        let other = SocketAddr::from(([10, 0, 0, 99], 12345));
        let (status, _, _) = call(
            &state,
            Request::builder()
                .method("POST")
                .uri(format!("/api/localsend/v2/cancel?sessionId={sid}"))
                .body(Body::empty())
                .unwrap(),
            other,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn cancel_v2_rejects_wrong_session_id() {
        let dir = fresh_dir("cancel-sid");
        let state = make_state(&dir, None);
        let body = make_prepare_body("2.1", 1);
        let (_, _, _) = call(
            &state,
            json_request("POST", "/api/localsend/v2/prepare-upload", Some(&body)),
            peer_addr(),
        )
        .await;
        let (status, _, _) = call(
            &state,
            Request::builder()
                .method("POST")
                .uri("/api/localsend/v2/cancel?sessionId=wrong")
                .body(Body::empty())
                .unwrap(),
            peer_addr(),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn cancel_returns_403_when_no_active_session() {
        let dir = fresh_dir("cancel-none");
        let state = make_state(&dir, None);
        let (status, _, _) = call(
            &state,
            Request::builder()
                .method("POST")
                .uri("/api/localsend/v2/cancel?sessionId=anything")
                .body(Body::empty())
                .unwrap(),
            peer_addr(),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn pin_rate_limit_is_per_ip_and_independent() {
        let dir = fresh_dir("pin-concurrent");
        let state = make_state(&dir, Some("right".into()));
        let body = make_prepare_body("2.1", 1);
        let peer = peer_addr();
        let other = SocketAddr::from(([10, 0, 0, 99], 53317));

        // Peer A: 2 wrong attempts → both 401
        for _ in 0..2 {
            let (s, _, _) = call(
                &state,
                json_request(
                    "POST",
                    "/api/localsend/v2/prepare-upload?pin=wrong",
                    Some(&body),
                ),
                peer,
            )
            .await;
            assert_eq!(s, StatusCode::UNAUTHORIZED);
        }
        // Peer B (different IP) should NOT be affected by peer A's failures
        // and gets 401 with the wrong PIN (its own count starts at 0).
        let (s, _, _) = call(
            &state,
            json_request(
                "POST",
                "/api/localsend/v2/prepare-upload?pin=wrong",
                Some(&body),
            ),
            other,
        )
        .await;
        assert_eq!(s, StatusCode::UNAUTHORIZED);
        // Peer A: 3rd wrong attempt triggers 429.
        let (s, _, _) = call(
            &state,
            json_request(
                "POST",
                "/api/localsend/v2/prepare-upload?pin=wrong",
                Some(&body),
            ),
            peer,
        )
        .await;
        assert_eq!(s, StatusCode::TOO_MANY_REQUESTS);
        // Peer A: even a CORRECT PIN is rejected with 429 after lockout.
        let (s, _, _) = call(
            &state,
            json_request(
                "POST",
                "/api/localsend/v2/prepare-upload?pin=right",
                Some(&body),
            ),
            peer,
        )
        .await;
        assert_eq!(s, StatusCode::TOO_MANY_REQUESTS);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Regression: when the last file in a session fails to save, the session
    /// must transition to `FinishedWithErrors` and emit the event. Previously
    /// the error path returned early and the session was stuck in `Sending`
    /// forever, blocking all future transfers.
    #[cfg(unix)]
    #[tokio::test]
    async fn upload_failure_emits_transfer_finished_with_errors() {
        use std::os::unix::fs::PermissionsExt;

        let dir = fresh_dir("upload-fail");
        let cfg = AppConfig::new(Some("Tester".into()), 53317, true, Some(dir.to_path_buf()))
            .expect("config");
        let identity = Identity {
            cert_pem: String::new(),
            key_pem: String::new(),
            fingerprint: "test-fp".to_string(),
        };
        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<ReceiveEvent>();
        let state = ServerState::new_with_events(
            cfg,
            identity,
            None,
            OutputMode::Json,
            false,
            None,
            Some(event_tx),
        );

        // Establish a session with one file.
        let body = make_prepare_body("2.1", 1);
        let (_, _, resp) = call(
            &state,
            json_request("POST", "/api/localsend/v2/prepare-upload", Some(&body)),
            peer_addr(),
        )
        .await;
        let json: serde_json::Value = serde_json::from_slice(&resp).unwrap();
        let sid = json["sessionId"].as_str().unwrap().to_string();
        let token = json["files"]["f1"].as_str().unwrap().to_string();

        // Make the receive directory read-only so File::create fails inside
        // save_stream.
        let perms = std::fs::Permissions::from_mode(0o555);
        std::fs::set_permissions(&dir, perms).unwrap();

        let upload_uri = format!(
            "/api/localsend/v2/upload?sessionId={}&fileId=f1&token={}",
            sid, token
        );
        let req = Request::builder()
            .method("POST")
            .uri(&upload_uri)
            .header("content-type", "application/octet-stream")
            .body(Body::from(b"abcd".to_vec()))
            .unwrap();
        let (status, _, _) = call(&state, req, peer_addr()).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

        // Restore permissions for cleanup.
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();

        // TransferFinishedWithErrors must have been emitted. Without the fix
        // the error path returned early and this event never fired.
        let mut found = false;
        while let Ok(ev) = event_rx.try_recv() {
            if matches!(ev, ReceiveEvent::TransferFinishedWithErrors) {
                found = true;
                break;
            }
        }
        assert!(
            found,
            "expected TransferFinishedWithErrors after upload failure"
        );

        std::fs::remove_dir_all(&dir).ok();
    }
    #[tokio::test]
    async fn abandoned_sessions_allow_new_file_and_message_transfers() {
        for failed in [false, true] {
            let dir = fresh_dir("expire");
            let state = make_state(&dir, None);
            let body = make_prepare_body("2.1", 1);
            let previous = prepare_one(&state, &body).await;
            // Fresh negotiations must still block competing senders.
            let (status, _, _) = call(
                &state,
                json_request("POST", "/api/localsend/v2/prepare-upload", Some(&body)),
                peer_addr(),
            )
            .await;
            assert_eq!(status, StatusCode::CONFLICT);
            {
                let mut guard = state.inner.lock().await;
                let session = guard.session.as_mut().unwrap();
                session.last_activity = Instant::now() - SESSION_IDLE_TIMEOUT;
                if failed {
                    session.status = SessionStatus::FinishedWithErrors;
                }
            }
            let replacement = prepare_one(&state, &body).await;
            assert_ne!(previous["sessionId"], replacement["sessionId"]);
            // Old credentials cannot write into the replacement session.
            let (status, _, _) = call(
                &state,
                upload_request(&previous, "f1", Body::from("abcd")),
                peer_addr(),
            )
            .await;
            assert_eq!(status, StatusCode::FORBIDDEN);
            {
                let mut guard = state.inner.lock().await;
                guard.session.as_mut().unwrap().last_activity =
                    Instant::now() - SESSION_IDLE_TIMEOUT;
            }
            let message = serde_json::json!({
                "info": {"alias": "Sender", "version": "2.1"},
                "files": {"text": {"fileName": "text.txt", "size": 2, "fileType": "text/plain", "preview": "hi"}}
            }).to_string();
            let (status, _, _) = call(
                &state,
                json_request("POST", "/api/localsend/v2/prepare-upload", Some(&message)),
                peer_addr(),
            )
            .await;
            assert_eq!(status, StatusCode::NO_CONTENT);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    #[tokio::test]
    async fn active_upload_prevents_session_expiry() {
        let dir = fresh_dir("active-session");
        let state = make_state(&dir, None);
        let body = make_prepare_body("2.1", 1);
        let session = prepare_one(&state, &body).await;
        {
            let mut guard = state.inner.lock().await;
            let session = guard.session.as_mut().unwrap();
            session.last_activity = Instant::now() - SESSION_IDLE_TIMEOUT;
            session.files.get_mut("f1").unwrap().uploading = true;
        }
        let (status, _, _) = call(
            &state,
            json_request("POST", "/api/localsend/v2/prepare-upload", Some(&body)),
            peer_addr(),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        // Completion refreshes the idle deadline for the remaining files.
        state
            .inner
            .lock()
            .await
            .session
            .as_mut()
            .unwrap()
            .files
            .get_mut("f1")
            .unwrap()
            .uploading = false;
        state
            .inner
            .lock()
            .await
            .session
            .as_mut()
            .unwrap()
            .last_activity = Instant::now();
        let (status, _, _) = call(
            &state,
            upload_request(&session, "f1", Body::from("abcd")),
            peer_addr(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_upload_times_out_and_removes_partial_file() {
        let dir = fresh_dir("idle-stream");
        let state = make_state(&dir, None);
        let session = prepare_one(&state, &make_prepare_body("2.1", 1)).await;
        let chunks = futures_util::stream::once(async { Ok::<_, std::io::Error>("ab") })
            .chain(futures_util::stream::pending());
        let (status, _, _) = call(
            &state,
            upload_request(&session, "f1", Body::from_stream(chunks)),
            peer_addr(),
        )
        .await;
        assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        let guard = state.inner.lock().await;
        let file = guard.session.as_ref().unwrap().files.get("f1").unwrap();
        assert!(file.failed);
        assert!(!file.uploading);
        drop(guard);
        // A sender may still retry within the recovery window.
        let (status, _, _) = call(
            &state,
            upload_request(&session, "f1", Body::from("abcd")),
            peer_addr(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        std::fs::remove_dir_all(dir).unwrap();
    }

    async fn prepare_one(state: &ServerState, body: &str) -> serde_json::Value {
        let (status, _, response) = call(
            state,
            json_request("POST", "/api/localsend/v2/prepare-upload", Some(body)),
            peer_addr(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        serde_json::from_slice(&response).unwrap()
    }

    fn upload_request(session: &serde_json::Value, id: &str, body: Body) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(format!(
                "/api/localsend/v2/upload?sessionId={}&fileId={id}&token={}",
                session["sessionId"].as_str().unwrap(),
                session["files"][id].as_str().unwrap()
            ))
            .body(body)
            .unwrap()
    }

    #[tokio::test]
    async fn rejects_wrong_lengths_removes_partial_files_and_allows_retry() {
        for (bytes, expected) in [
            ("abc", StatusCode::BAD_REQUEST),
            ("abcde", StatusCode::PAYLOAD_TOO_LARGE),
        ] {
            let dir = fresh_dir("size");
            let state = make_state(&dir, None);
            let session = prepare_one(&state, &make_prepare_body("2.1", 1)).await;
            let (status, _, _) = call(
                &state,
                upload_request(&session, "f1", Body::from(bytes)),
                peer_addr(),
            )
            .await;
            assert_eq!(status, expected);
            assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
            let (status, _, _) = call(
                &state,
                upload_request(&session, "f1", Body::from("abcd")),
                peer_addr(),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(std::fs::read(dir.join("a0.bin")).unwrap(), b"abcd");
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn upload_cannot_escape_through_directory_symlink() {
        let dir = fresh_dir("symlink-inbox");
        let outside = fresh_dir("symlink-outside");
        std::os::unix::fs::symlink(&outside, dir.join("link")).unwrap();
        let state = make_state(&dir, None);
        let body = make_prepare_body("2.1", 1).replace("a0.bin", "link/escape.bin");
        let session = prepare_one(&state, &body).await;
        let (status, _, _) = call(
            &state,
            upload_request(&session, "f1", Body::from("abcd")),
            peer_addr(),
        )
        .await;
        assert!(!status.is_success());
        assert!(!outside.join("escape.bin").exists());
        std::fs::remove_dir_all(dir).unwrap();
        std::fs::remove_dir_all(outside).unwrap();
    }

    #[tokio::test]
    async fn parallel_uploads_with_same_name_preserve_both_files() {
        let dir = fresh_dir("parallel-names");
        let state = make_state(&dir, None);
        let body = make_prepare_body("2.1", 2).replace("a1.bin", "a0.bin");
        let session = prepare_one(&state, &body).await;
        let (first, second) = tokio::join!(
            call(
                &state,
                upload_request(&session, "f1", Body::from("aaaa")),
                peer_addr()
            ),
            call(
                &state,
                upload_request(&session, "f2", Body::from("bbbb")),
                peer_addr()
            ),
        );
        assert_eq!(first.0, StatusCode::OK);
        assert_eq!(second.0, StatusCode::OK);
        let mut contents: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| std::fs::read(entry.unwrap().path()).unwrap())
            .collect();
        contents.sort();
        assert_eq!(contents, vec![b"aaaa".to_vec(), b"bbbb".to_vec()]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn cancelled_upload_does_not_complete_replacement_session() {
        let dir = fresh_dir("cancel-stream");
        let state = make_state(&dir, None);
        let body = make_prepare_body("2.1", 1);
        let session = prepare_one(&state, &body).await;
        let (tx, rx) = mpsc::channel::<Vec<u8>>(1);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let stream =
            futures_util::stream::unfold((rx, Some(started_tx)), |(mut rx, started)| async move {
                if let Some(started) = started {
                    let _ = started.send(());
                }
                rx.recv()
                    .await
                    .map(|data| (Ok::<_, std::io::Error>(data), (rx, None)))
            });
        let request = upload_request(&session, "f1", Body::from_stream(stream));
        let upload_state = state.clone();
        let upload = tokio::spawn(async move { call(&upload_state, request, peer_addr()).await });
        tokio::time::timeout(Duration::from_secs(5), started_rx)
            .await
            .unwrap()
            .unwrap();
        let cancel = Request::builder()
            .method("POST")
            .uri(format!(
                "/api/localsend/v2/cancel?sessionId={}",
                session["sessionId"].as_str().unwrap()
            ))
            .body(Body::empty())
            .unwrap();
        assert_eq!(call(&state, cancel, peer_addr()).await.0, StatusCode::OK);
        let replacement = prepare_one(&state, &body).await;
        tx.send(b"abcd".to_vec()).await.unwrap();
        drop(tx);
        assert_eq!(upload.await.unwrap().0, StatusCode::CONFLICT);
        let guard = state.inner.lock().await;
        let active = guard.session.as_ref().unwrap();
        assert_eq!(
            active.session_id,
            replacement["sessionId"].as_str().unwrap()
        );
        assert!(active.files["f1"].path.is_none());
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        drop(guard);
        assert_eq!(
            call(
                &state,
                upload_request(&replacement, "f1", Body::from("abcd")),
                peer_addr()
            )
            .await
            .0,
            StatusCode::OK
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
}
