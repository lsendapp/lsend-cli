use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use indicatif::{ProgressBar, ProgressStyle};
use localsend::http::client::LsHttpClient;
use localsend::http::dto_v2::{PrepareUploadRequestDtoV2, RegisterDtoV2};
use localsend::model::discovery::ProtocolType;
use localsend::model::transfer::{FileContent, FileDto};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::config::AppConfig;
use crate::discovery::{DiscoveredDevice, resolve_target};
use crate::error::CliError;
use crate::identity::Identity;
use crate::network::build_http_client;
use crate::output::{DeviceJson, OutputOptions, SendFileResult, SendKind, SendResult, print_json};
use crate::text_send::{
    TEXT_FILE_TYPE, read_clipboard_text, read_message_text, read_stdin_text, text_file_name,
    text_preview,
};

const CHUNK_SIZE: usize = 512 * 1024;

/// Inputs for a send operation. Grouped into a struct to avoid error-prone
/// positional bool/Option arguments at call sites.
#[derive(Debug, Clone)]
pub struct SendRequest<'a> {
    pub target: &'a str,
    pub paths: &'a [String],
    pub text_stdin: bool,
    pub message: Option<&'a str>,
    pub clipboard: bool,
    pub pin: Option<&'a str>,
    pub no_scan: bool,
}

/// Outcome of a send whose negotiation with the receiver finished. Per-file
/// statuses are reported in the JSON result / human output either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendOutcome {
    /// Message delivered (204) or every accepted file uploaded.
    Delivered,
    /// At least one file failed to upload; other files may still have
    /// finished. The command should exit non-zero.
    FailedFiles(usize),
}

impl SendOutcome {
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Delivered => 0,
            Self::FailedFiles(_) => 1,
        }
    }
}

pub async fn send_files(
    config: &AppConfig,
    identity: &Identity,
    request: &SendRequest<'_>,
    output: OutputOptions,
) -> Result<SendOutcome> {
    let (device, resolved_via) =
        resolve_target(request.target, config, identity, !request.no_scan).await?;
    send_files_to_device(config, identity, request, output, &device, resolved_via).await
}

/// Send to an already discovered endpoint without discarding its transport or port.
pub async fn send_files_to_device(
    config: &AppConfig,
    identity: &Identity,
    request: &SendRequest<'_>,
    output: OutputOptions,
    device: &DiscoveredDevice,
    resolved_via: &'static str,
) -> Result<SendOutcome> {
    let is_message = is_message_send(
        request.text_stdin,
        request.message.is_some(),
        request.clipboard,
    );
    let files = collect_inputs(
        request.paths,
        request.text_stdin,
        request.message,
        request.clipboard,
    )
    .await?;
    if files.is_empty() {
        return Err(CliError::NoFiles.into());
    }

    let client = build_http_client(identity, device.https)?;
    let protocol = if device.https {
        ProtocolType::Https
    } else {
        ProtocolType::Http
    };

    let register = RegisterDtoV2 {
        alias: config.alias.clone(),
        version: crate::config::PROTOCOL_VERSION.to_string(),
        device_model: Some(crate::util::os_display_name()),
        device_type: Some(localsend::model::discovery::DeviceType::Headless),
        fingerprint: identity.fingerprint.clone(),
        port: config.port,
        protocol: if config.https {
            ProtocolType::Https
        } else {
            ProtocolType::Http
        },
        download: false,
    };

    let include_text_preview = files.len() == 1 && files[0].file_type == TEXT_FILE_TYPE;
    let file_map: HashMap<String, FileDto> = files
        .iter()
        .map(|f| {
            let preview = if include_text_preview {
                f.in_memory_preview()
            } else {
                None
            };
            (
                f.id.clone(),
                FileDto {
                    id: f.id.clone(),
                    file_name: f.file_name.clone(),
                    size: f.size,
                    file_type: f.file_type.clone(),
                    sha256: Some(f.sha256.clone()),
                    preview,
                    metadata: None,
                },
            )
        })
        .collect();
    let prepare_payload = PrepareUploadRequestDtoV2 {
        info: register,
        files: file_map,
    };

    if output.show_human_progress() {
        println!(
            "Sending {} file(s) to {} ({})",
            files.len(),
            device.alias,
            device.ip
        );
    }

    let prepare_result = match &client {
        LsHttpClient::V2(c) => {
            c.prepare_upload(
                protocol.clone(),
                &device.ip,
                device.port,
                None,
                prepare_payload,
                request.pin,
                CancellationToken::new(),
            )
            .await?
        }
        LsHttpClient::V3(_) => bail!("v3 client is not supported by this CLI"),
    };

    let mut results = Vec::with_capacity(files.len());

    if prepare_result.status_code == 204 {
        // 204 means the receiver finished the exchange out-of-band. That is
        // success for an embedded text message, but for a file send it means
        // no file was accepted — reporting success would hide the rejection.
        if !is_message {
            return Err(CliError::TransferDeclined {
                target: device.alias.clone(),
            }
            .into());
        }
        if output.show_human_progress() {
            println!("Message delivered.");
        } else if output.is_json() {
            print_json(&build_send_result(
                device,
                resolved_via,
                is_message,
                &files,
                vec![],
                "finished",
                true,
            ));
        }
        return Ok(SendOutcome::Delivered);
    }

    let response = prepare_result
        .response
        .context("Missing prepare-upload response body")?;

    // Upload every accepted file. One failure must not discard the
    // already-negotiated tokens for the remaining files (REVIEW.md #4), so
    // failures are recorded per file and the loop continues.
    let mut failed = 0usize;
    for local in &files {
        let Some(token) = response.files.get(&local.id) else {
            if output.show_human_progress() {
                println!("Skipped (not accepted): {}", local.file_name);
            }
            results.push(SendFileResult {
                name: local.file_name.clone(),
                path: local.display_source(),
                size: local.size,
                status: "skipped",
                error: None,
            });
            continue;
        };

        match upload_file(
            &client,
            &protocol,
            &device.ip,
            device.port,
            &response.session_id,
            local,
            token,
            output,
        )
        .await
        {
            Ok(()) => {
                results.push(SendFileResult {
                    name: local.file_name.clone(),
                    path: local.display_source(),
                    size: local.size,
                    status: "finished",
                    error: None,
                });
            }
            Err(e) => {
                failed += 1;
                let message = format!("{e:#}");
                if output.show_human_progress() {
                    println!("Failed: {}: {message}", local.file_name);
                }
                results.push(SendFileResult {
                    name: local.file_name.clone(),
                    path: local.display_source(),
                    size: local.size,
                    status: "failed",
                    error: Some(message),
                });
            }
        }
    }

    if failed > 0 {
        // At least one file failed; the receiver keeps the session alive for
        // retries. Release it so the receiver is free for the next sender.
        cancel_failed_transfer(
            &client,
            &protocol,
            &device.ip,
            device.port,
            &response.session_id,
        )
        .await;
    }

    let ok = failed == 0;
    if output.is_json() {
        let message_status = if results.iter().all(|result| result.status == "finished") {
            "finished"
        } else {
            "skipped"
        };
        print_json(&build_send_result(
            device,
            resolved_via,
            is_message,
            &files,
            results,
            message_status,
            ok,
        ));
    } else if output.show_human_progress() {
        if ok {
            println!("Done.");
        } else {
            println!("Done with errors: {failed} file(s) failed.");
        }
    }

    Ok(if ok {
        SendOutcome::Delivered
    } else {
        SendOutcome::FailedFiles(failed)
    })
}

async fn cancel_failed_transfer(
    client: &LsHttpClient,
    protocol: &ProtocolType,
    ip: &str,
    port: u16,
    session_id: &str,
) {
    if let LsHttpClient::V2(client) = client {
        match tokio::time::timeout(
            std::time::Duration::from_secs(3),
            client.cancel(protocol.clone(), ip, port, session_id),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!("Failed to cancel transfer: {error}"),
            Err(_) => tracing::warn!("Timed out cancelling transfer"),
        }
    }
}

async fn upload_file(
    client: &LsHttpClient,
    protocol: &ProtocolType,
    ip: &str,
    port: u16,
    session_id: &str,
    local: &LocalFile,
    token: &str,
    output: OutputOptions,
) -> Result<()> {
    let progress = output.show_human_progress().then(|| {
        let pb = ProgressBar::new(local.size);
        pb.set_style(
            ProgressStyle::with_template("{msg} [{bar:40.cyan/blue}] {bytes}/{total_bytes}")
                .unwrap()
                .progress_chars("=>-"),
        );
        pb.set_message(local.file_name.clone());
        pb
    });
    let progress_for_upload = progress.clone();

    let (tx, rx) = mpsc::channel(4);
    let source = local.source.clone();
    let reader_task = tokio::spawn(async move {
        match source {
            FileSource::Path(path) => {
                let file = File::open(&path).await?;
                let mut reader = BufReader::new(file);
                let mut buffer = vec![0u8; CHUNK_SIZE];
                loop {
                    let n = reader.read(&mut buffer).await?;
                    if n == 0 {
                        break;
                    }
                    if tx.send(Bytes::copy_from_slice(&buffer[..n])).await.is_err() {
                        break;
                    }
                }
            }
            FileSource::Memory { data, .. } => {
                for chunk in data.chunks(CHUNK_SIZE) {
                    if tx.send(Bytes::copy_from_slice(chunk)).await.is_err() {
                        break;
                    }
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    let upload_result = match client {
        LsHttpClient::V2(_) => {
            client
                .upload(
                    protocol.clone(),
                    ip,
                    port,
                    None,
                    session_id,
                    &local.id,
                    token,
                    FileContent::Stream(rx),
                    move |bytes_sent| {
                        if let Some(pb) = &progress_for_upload {
                            pb.set_position(bytes_sent);
                        }
                    },
                    CancellationToken::new(),
                )
                .await
        }
        LsHttpClient::V3(_) => bail!("v3 client is not supported by this CLI"),
    };

    reader_task.await??;
    if let Some(pb) = progress {
        pb.finish_and_clear();
    }
    upload_result.map_err(|e| anyhow::anyhow!("{e}"))
}

#[derive(Clone)]
struct LocalFile {
    id: String,
    file_name: String,
    source: FileSource,
    size: u64,
    file_type: String,
    /// SHA-256 of the payload, lowercase hex. Announced in prepare-upload so
    /// receivers can verify integrity (protocol SHOULD).
    sha256: String,
}

#[derive(Clone)]
enum FileSource {
    Path(PathBuf),
    Memory { data: Vec<u8>, label: String },
}

impl LocalFile {
    fn display_source(&self) -> String {
        match &self.source {
            FileSource::Path(path) => path.display().to_string(),
            FileSource::Memory { label, .. } => label.clone(),
        }
    }

    fn in_memory_preview(&self) -> Option<String> {
        match &self.source {
            FileSource::Memory { data, .. } => text_preview(data),
            FileSource::Path(_) => None,
        }
    }

    fn message_text(&self) -> String {
        self.in_memory_preview().unwrap_or_default()
    }
}

fn is_message_send(text_stdin: bool, has_message: bool, clipboard: bool) -> bool {
    text_stdin || has_message || clipboard
}

fn build_send_result(
    device: &crate::discovery::DiscoveredDevice,
    resolved_via: &'static str,
    is_message: bool,
    locals: &[LocalFile],
    file_results: Vec<SendFileResult>,
    message_status: &'static str,
    ok: bool,
) -> SendResult {
    let kind = if is_message {
        let local = locals
            .first()
            .expect("message send must include exactly one in-memory payload");
        SendKind::Message {
            text: local.message_text(),
            size: local.size,
            status: message_status,
        }
    } else {
        SendKind::File {
            files: file_results,
        }
    };

    SendResult {
        command: "send",
        ok,
        target: DeviceJson::from(device),
        resolved_via,
        kind,
    }
}

async fn collect_inputs(
    paths: &[String],
    text_stdin: bool,
    message: Option<&str>,
    clipboard: bool,
) -> Result<Vec<LocalFile>> {
    let text_mode = text_stdin || message.is_some() || clipboard;
    if text_mode && !paths.is_empty() {
        bail!("Cannot combine file paths with --text, --message, or --clipboard");
    }

    let mut files = Vec::new();

    if text_stdin {
        let data = read_stdin_text().await?;
        files.push(local_file_from_bytes(data, "stdin")?);
    } else if let Some(message) = message {
        let data = read_message_text(message)?;
        files.push(local_file_from_bytes(data, "inline")?);
    } else if clipboard {
        let data = read_clipboard_text()?;
        files.push(local_file_from_bytes(data, "clipboard")?);
    } else {
        files = collect_files(paths).await?;
    }

    Ok(files)
}

fn local_file_from_bytes(data: Vec<u8>, label: &str) -> Result<LocalFile> {
    let size = data.len() as u64;
    let sha256 = localsend::crypto::hash::sha256_hex(&data);
    Ok(LocalFile {
        id: Uuid::new_v4().to_string(),
        file_name: text_file_name(),
        source: FileSource::Memory {
            data,
            label: label.to_string(),
        },
        size,
        file_type: TEXT_FILE_TYPE.to_string(),
        sha256,
    })
}

async fn collect_files(paths: &[String]) -> Result<Vec<LocalFile>> {
    let mut files = Vec::new();
    for path_str in paths {
        let path = PathBuf::from(path_str);
        if path.is_dir() {
            collect_dir(&path, &path, &mut files).await?;
        } else if path.is_file() {
            files.push(
                local_file_from_path(&path, &path.file_name().unwrap().to_string_lossy()).await?,
            );
        } else {
            bail!("Path not found: {}", path.display());
        }
    }
    Ok(files)
}

async fn collect_dir(base: &Path, dir: &Path, out: &mut Vec<LocalFile>) -> Result<()> {
    let mut entries = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.is_dir() {
            Box::pin(collect_dir(base, &path, out)).await?;
        } else if path.is_file() {
            let relative = path
                .strip_prefix(base)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            out.push(local_file_from_path(&path, &relative).await?);
        }
    }
    Ok(())
}

async fn local_file_from_path(path: &Path, file_name: &str) -> Result<LocalFile> {
    let metadata = std::fs::metadata(path)?;
    let mime = mime_guess::from_path(path)
        .first_or_octet_stream()
        .essence_str()
        .to_string();
    let sha256 = localsend::crypto::hash::sha256_file_content(
        FileContent::Path(path.to_path_buf()),
        &CancellationToken::new(),
        |_| {},
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to hash {}: {e}", path.display()))?;
    Ok(LocalFile {
        id: Uuid::new_v4().to_string(),
        file_name: file_name.to_string(),
        source: FileSource::Path(path.to_path_buf()),
        size: metadata.len(),
        file_type: mime,
        sha256,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn failed_upload_cancels_the_negotiated_session() {
        use axum::http::StatusCode;
        use axum::{Json, Router, extract::Query, routing::post};
        let (cancel_tx, mut cancel_rx) = mpsc::unbounded_channel();
        let app = Router::new()
            .route(
                "/api/localsend/v2/prepare-upload",
                post(|Json(payload): Json<serde_json::Value>| async move {
                    let tokens: serde_json::Map<String, serde_json::Value> = payload["files"]
                        .as_object()
                        .unwrap()
                        .keys()
                        .map(|id| (id.clone(), serde_json::json!("token")))
                        .collect();
                    Json(serde_json::json!({"sessionId": "failed-session", "files": tokens}))
                }),
            )
            .route(
                "/api/localsend/v2/upload",
                post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
            )
            .route(
                "/api/localsend/v2/cancel",
                post(move |Query(params): Query<HashMap<String, String>>| {
                    let tx = cancel_tx.clone();
                    async move {
                        tx.send(params["sessionId"].clone()).unwrap();
                        StatusCode::OK
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let dir = std::env::temp_dir().join(format!("lsend-send-cancel-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.bin");
        std::fs::write(&path, b"abcd").unwrap();
        let config = AppConfig {
            alias: "Tester".into(),
            port,
            https: false,
            multicast_group: crate::config::DEFAULT_MULTICAST_GROUP.into(),
            config_dir: dir.clone(),
            receive_dir: dir.clone(),
        };
        let identity = Identity {
            cert_pem: String::new(),
            key_pem: String::new(),
            fingerprint: "test".into(),
        };
        let paths = vec![path.to_string_lossy().into_owned()];
        let request = SendRequest {
            target: "127.0.0.1",
            paths: &paths,
            text_stdin: false,
            message: None,
            clipboard: false,
            pin: None,
            no_scan: true,
        };
        // The local receiver uses HTTPS on a different port; sending must use
        // the selected peer's HTTP endpoint without changing our advertised identity.
        let device = DiscoveredDevice {
            alias: "Selected peer".into(),
            ip: "127.0.0.1".into(),
            port,
            https: false,
            fingerprint: "peer".into(),
            version: "2.1".into(),
            device_type: None,
            device_model: None,
        };
        let local_config = AppConfig {
            port: 1,
            https: true,
            ..config
        };
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            send_files_to_device(
                &local_config,
                &identity,
                &request,
                OutputOptions::new(true, true),
                &device,
                "scan",
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(outcome, SendOutcome::FailedFiles(1));
        assert_eq!(outcome.exit_code(), 1);
        assert_eq!(cancel_rx.try_recv().unwrap(), "failed-session");
        server.abort();
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// One failing upload must not discard the negotiated tokens for the
    /// remaining files: the batch continues, per-file results are reported,
    /// and the session is released once (REVIEW.md #4).
    #[tokio::test]
    async fn multi_file_send_continues_after_failure() {
        use axum::body::Bytes as BodyBytes;
        use axum::http::StatusCode;
        use axum::{Json, Router, extract::Query, routing::post};
        let (cancel_tx, mut cancel_rx) = mpsc::unbounded_channel();
        let uploads: std::sync::Arc<std::sync::Mutex<Vec<&'static str>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let uploads_for_route = uploads.clone();
        let app = Router::new()
            .route(
                "/api/localsend/v2/prepare-upload",
                post(|Json(payload): Json<serde_json::Value>| async move {
                    let tokens: serde_json::Map<String, serde_json::Value> = payload["files"]
                        .as_object()
                        .unwrap()
                        .keys()
                        .map(|id| (id.clone(), serde_json::json!("token")))
                        .collect();
                    Json(serde_json::json!({"sessionId": "batch-session", "files": tokens}))
                }),
            )
            .route(
                "/api/localsend/v2/upload",
                post(move |body: BodyBytes| {
                    let uploads = uploads_for_route.clone();
                    async move {
                        let label = if body.as_ref() == b"AAA" {
                            "AAA"
                        } else {
                            "BBB"
                        };
                        uploads.lock().unwrap().push(label);
                        if body.as_ref() == b"AAA" {
                            StatusCode::INTERNAL_SERVER_ERROR
                        } else {
                            StatusCode::OK
                        }
                    }
                }),
            )
            .route(
                "/api/localsend/v2/cancel",
                post(move |Query(params): Query<HashMap<String, String>>| {
                    let tx = cancel_tx.clone();
                    async move {
                        tx.send(params["sessionId"].clone()).unwrap();
                        StatusCode::OK
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let dir = std::env::temp_dir().join(format!("lsend-send-multi-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a.txt"), b"AAA").unwrap();
        std::fs::write(dir.join("b.txt"), b"BBB").unwrap();
        let config = AppConfig {
            alias: "Tester".into(),
            port,
            https: false,
            multicast_group: crate::config::DEFAULT_MULTICAST_GROUP.into(),
            config_dir: dir.clone(),
            receive_dir: dir.clone(),
        };
        let identity = Identity {
            cert_pem: String::new(),
            key_pem: String::new(),
            fingerprint: "test".into(),
        };
        let paths = vec![
            dir.join("a.txt").to_string_lossy().into_owned(),
            dir.join("b.txt").to_string_lossy().into_owned(),
        ];
        let request = SendRequest {
            target: "127.0.0.1",
            paths: &paths,
            text_stdin: false,
            message: None,
            clipboard: false,
            pin: None,
            no_scan: true,
        };
        let device = DiscoveredDevice {
            alias: "Batch peer".into(),
            ip: "127.0.0.1".into(),
            port,
            https: false,
            fingerprint: "peer".into(),
            version: "2.2".into(),
            device_type: None,
            device_model: None,
        };
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            send_files_to_device(
                &config,
                &identity,
                &request,
                OutputOptions::new(true, true),
                &device,
                "scan",
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(outcome, SendOutcome::FailedFiles(1));
        // The second file was attempted after the first failed.
        assert_eq!(*uploads.lock().unwrap(), vec!["AAA", "BBB"]);
        assert_eq!(cancel_rx.try_recv().unwrap(), "batch-session");
        server.abort();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn file_send_reports_error_when_receiver_replies_204() {
        use axum::http::StatusCode;
        use axum::{Router, routing::post};
        let app = Router::new().route(
            "/api/localsend/v2/prepare-upload",
            post(|| async { StatusCode::NO_CONTENT }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let dir = std::env::temp_dir().join(format!("lsend-send-204-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("note.txt");
        std::fs::write(&path, b"payload").unwrap();
        let config = AppConfig {
            alias: "Tester".into(),
            port,
            https: false,
            multicast_group: crate::config::DEFAULT_MULTICAST_GROUP.into(),
            config_dir: dir.clone(),
            receive_dir: dir.clone(),
        };
        let identity = Identity {
            cert_pem: String::new(),
            key_pem: String::new(),
            fingerprint: "test".into(),
        };
        let paths = vec![path.to_string_lossy().into_owned()];
        let request = SendRequest {
            target: "127.0.0.1",
            paths: &paths,
            text_stdin: false,
            message: None,
            clipboard: false,
            pin: None,
            no_scan: true,
        };
        let device = DiscoveredDevice {
            alias: "Declining peer".into(),
            ip: "127.0.0.1".into(),
            port,
            https: false,
            fingerprint: "peer".into(),
            version: "2.2".into(),
            device_type: None,
            device_model: None,
        };
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            send_files_to_device(
                &config,
                &identity,
                &request,
                OutputOptions::new(true, true),
                &device,
                "scan",
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        let cli_error = crate::error::CliError::from_anyhow(error);
        assert_eq!(cli_error.code(), "transfer_declined");
        assert_eq!(cli_error.exit_code(), 1);
        server.abort();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn rejects_mixed_paths_and_text_modes() {
        let result = collect_inputs(&["file.txt".to_string()], true, None, false).await;
        match result {
            Err(err) => assert!(err.to_string().contains("Cannot combine")),
            Ok(_) => panic!("expected error when mixing paths with --text"),
        }
    }

    #[test]
    fn text_file_uses_plain_mime_and_txt_name() {
        let file = local_file_from_bytes(b"hello".to_vec(), "stdin").unwrap();
        assert!(file.file_name.ends_with(".txt"));
        assert_eq!(file.file_type, "text/plain");
        assert_eq!(file.in_memory_preview().as_deref(), Some("hello"));
    }

    #[test]
    fn text_file_message_returns_full_text() {
        let file = local_file_from_bytes(b"hello world".to_vec(), "inline").unwrap();
        assert_eq!(file.message_text(), "hello world");
    }

    #[test]
    fn text_file_size_matches_byte_length() {
        let file = local_file_from_bytes(b"\x00\x01\x02".to_vec(), "x").unwrap();
        assert_eq!(file.size, 3);
        assert_eq!(
            file.sha256,
            localsend::crypto::hash::sha256_hex(&[0x00, 0x01, 0x02])
        );
    }

    #[tokio::test]
    async fn local_file_from_path_captures_size_mime_and_sha256() {
        let dir = std::env::temp_dir().join(format!("lsend-send-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("hello.txt");
        std::fs::write(&path, b"hello").unwrap();
        let file = local_file_from_path(&path, "hello.txt").await.unwrap();
        assert_eq!(file.file_name, "hello.txt");
        assert_eq!(file.size, 5);
        // Mime should be a string starting with "text/" for a .txt file.
        assert!(
            file.file_type.starts_with("text/"),
            "got: {}",
            file.file_type
        );
        assert_eq!(file.sha256, localsend::crypto::hash::sha256_hex(b"hello"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn collect_files_reads_nested_directory() {
        let dir = std::env::temp_dir().join(format!("lsend-collect-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(dir.join("a/b")).unwrap();
        std::fs::write(dir.join("a/top.txt"), b"x").unwrap();
        std::fs::write(dir.join("a/b/nested.txt"), b"y").unwrap();

        let files = collect_files(&[dir.to_string_lossy().to_string()])
            .await
            .unwrap();
        assert_eq!(files.len(), 2);
        let names: Vec<_> = files.iter().map(|f| f.file_name.clone()).collect();
        // Names preserve relative paths from the root argument.
        assert!(names.iter().any(|n| n == "a/top.txt"), "names: {names:?}");
        assert!(
            names.iter().any(|n| n == "a/b/nested.txt"),
            "names: {names:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn collect_files_errors_on_missing_path() {
        let result = collect_files(&["/nonexistent/lsend/path/xyz".to_string()]).await;
        assert!(result.is_err());
    }

    #[test]
    fn is_message_send_classifies_each_mode() {
        assert!(is_message_send(true, false, false));
        assert!(is_message_send(false, true, false));
        assert!(is_message_send(false, false, true));
        assert!(is_message_send(false, true, true));
        // Only "no mode" is a file send.
        assert!(!is_message_send(false, false, false));
    }

    #[test]
    fn local_file_display_source_for_memory() {
        let file = local_file_from_bytes(b"x".to_vec(), "stdin").unwrap();
        let display = file.display_source();
        assert!(display.contains("stdin") || !display.is_empty());
    }

    #[tokio::test]
    async fn local_file_display_source_for_path() {
        let dir = std::env::temp_dir().join(format!("lsend-disp-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("foo.txt");
        std::fs::write(&path, b"x").unwrap();
        let file = local_file_from_path(&path, "foo.txt").await.unwrap();
        assert_eq!(file.display_source(), path.display().to_string());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
