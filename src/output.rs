use std::io::{IsTerminal, stdout};

use localsend::model::discovery::DeviceType;
use serde::Serialize;

use crate::discovery::DiscoveredDevice;
use crate::error::CliError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputMode {
    Human,
    Json,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutputOptions {
    pub mode: OutputMode,
    pub quiet: bool,
}

impl OutputOptions {
    pub fn from_cli(json_flag: bool, quiet_flag: bool) -> Self {
        let json = json_flag || should_force_json();
        Self::new(json, quiet_flag)
    }

    pub fn new(json: bool, quiet: bool) -> Self {
        Self {
            mode: if json {
                OutputMode::Json
            } else {
                OutputMode::Human
            },
            quiet: json || quiet,
        }
    }

    pub fn is_json(&self) -> bool {
        self.mode == OutputMode::Json
    }

    pub fn show_human_progress(&self) -> bool {
        self.mode == OutputMode::Human && !self.quiet
    }
}

fn should_force_json() -> bool {
    no_tui_env() || !stdout().is_terminal()
}

fn no_tui_env() -> bool {
    matches!(
        std::env::var("LSEND_NO_TUI")
            .ok()
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref(),
        Some("1") | Some("true") | Some("yes")
    )
}

#[derive(Debug, Serialize)]
pub struct DeviceJson {
    pub alias: String,
    pub ip: String,
    pub port: u16,
    pub fingerprint: String,
    pub https: bool,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_model: Option<String>,
}

impl From<&DiscoveredDevice> for DeviceJson {
    fn from(device: &DiscoveredDevice) -> Self {
        Self {
            alias: device.alias.clone(),
            ip: device.ip.clone(),
            port: device.port,
            fingerprint: device.fingerprint.clone(),
            https: device.https,
            version: device.version.clone(),
            device_type: device.device_type.as_ref().map(device_type_label),
            device_model: device.device_model.clone(),
        }
    }
}

fn device_type_label(device_type: &DeviceType) -> String {
    match device_type {
        DeviceType::Mobile => "mobile",
        DeviceType::Desktop => "desktop",
        DeviceType::Web => "web",
        DeviceType::Headless => "headless",
        DeviceType::Server => "server",
    }
    .to_string()
}

#[derive(Debug, Serialize)]
pub struct ErrorEnvelope {
    pub command: &'static str,
    pub ok: bool,
    pub code: &'static str,
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

pub fn error_envelope(command: &'static str, err: &CliError) -> ErrorEnvelope {
    ErrorEnvelope {
        command,
        ok: false,
        code: err.code(),
        error: err.to_string(),
        hint: err.hint(),
    }
}

pub fn print_json<T: Serialize>(value: &T) {
    println!(
        "{}",
        serde_json::to_string(value).expect("JSON serialization must not fail")
    );
}

#[derive(Debug, Serialize)]
pub struct AliasShowResult {
    pub command: &'static str,
    pub action: &'static str,
    pub ok: bool,
    pub alias: String,
    pub path: String,
    pub locale: String,
    pub created: bool,
}

#[derive(Debug, Serialize)]
pub struct AliasRegenerateResult {
    pub command: &'static str,
    pub action: &'static str,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous: Option<String>,
    pub alias: String,
    pub path: String,
    pub locale: String,
}

#[derive(Debug, Serialize)]
pub struct AliasSetResult {
    pub command: &'static str,
    pub action: &'static str,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous: Option<String>,
    pub alias: String,
    pub path: String,
}

#[derive(Debug, Serialize)]
pub struct ScanResult {
    pub command: &'static str,
    pub ok: bool,
    pub timeout_ms: u64,
    pub devices: Vec<DeviceJson>,
}

#[derive(Debug, Serialize)]
pub struct SendFileResult {
    pub name: String,
    pub path: String,
    pub size: u64,
    pub status: &'static str,
    /// Failure reason when `status` is "failed".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SendKind {
    File {
        files: Vec<SendFileResult>,
    },
    Message {
        text: String,
        size: u64,
        status: &'static str,
    },
}

#[derive(Debug, Serialize)]
pub struct SendResult {
    pub command: &'static str,
    pub ok: bool,
    pub target: DeviceJson,
    pub resolved_via: &'static str,
    #[serde(flatten)]
    pub kind: SendKind,
}

#[derive(Debug, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum ReceiveEventJson {
    Ready {
        alias: String,
        port: u16,
        https: bool,
        receive_dir: String,
    },
    TransferStarted {
        sender_alias: String,
        file_count: usize,
    },
    MessageReceived {
        sender_alias: String,
        text: String,
        size: u64,
    },
    FileSaved {
        path: String,
        file_name: String,
        size: u64,
    },
    TransferComplete,
    TransferFinishedWithErrors,
    /// The receiver rejected a transfer before creating a session (e.g. the
    /// sender is not in the pairing list).
    TransferRejected {
        sender_alias: String,
        reason: &'static str,
    },
    TransferCancelled,
    Shutdown,
}

#[derive(Debug, Serialize)]
pub struct PairedDeviceJson {
    pub alias: String,
    pub fingerprint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_model: Option<String>,
    /// Unix seconds when the device was paired.
    pub paired_at: u64,
}

#[derive(Debug, Serialize)]
pub struct PairListResult {
    pub command: &'static str,
    pub action: &'static str,
    pub ok: bool,
    pub devices: Vec<PairedDeviceJson>,
}

#[derive(Debug, Serialize)]
pub struct PairAddResult {
    pub command: &'static str,
    pub action: &'static str,
    pub ok: bool,
    /// False when the device was already paired (metadata refreshed).
    pub created: bool,
    pub device: PairedDeviceJson,
}

#[derive(Debug, Serialize)]
pub struct PairRemoveResult {
    pub command: &'static str,
    pub action: &'static str,
    pub ok: bool,
    pub device: PairedDeviceJson,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CliError;

    #[test]
    fn error_envelope_includes_hint() {
        let err = CliError::PortInUse { port: 53317 };
        let envelope = error_envelope("receive", &err);
        assert_eq!(envelope.code, "port_in_use");
        assert!(envelope.hint.is_some());
    }

    #[test]
    fn error_envelope_serializes_command_before_ok() {
        let err = CliError::NoFiles;
        let envelope = error_envelope("send", &err);
        let json = serde_json::to_string(&envelope).expect("serialize");
        let command_pos = json.find("\"command\"").expect("command field");
        let ok_pos = json.find("\"ok\"").expect("ok field");
        assert!(command_pos < ok_pos);
        assert!(json.starts_with("{\"command\":"));
    }

    #[test]
    fn no_tui_env_forces_json_mode() {
        // SAFETY: this test runs single-threaded; env var mutation is isolated to this test.
        unsafe {
            std::env::set_var("LSEND_NO_TUI", "1");
        }
        let output = OutputOptions::from_cli(false, false);
        unsafe {
            std::env::remove_var("LSEND_NO_TUI");
        }
        assert!(output.is_json());
    }

    #[test]
    fn message_received_event_serializes_as_snake_case() {
        let event = ReceiveEventJson::MessageReceived {
            sender_alias: "iPhone".to_string(),
            text: "hello".to_string(),
            size: 5,
        };
        let json = serde_json::to_string(&event).expect("serialize");
        assert!(json.contains("\"event\":\"message_received\""));
        assert!(json.contains("\"sender_alias\":\"iPhone\""));
        assert!(json.contains("\"text\":\"hello\""));
    }

    #[test]
    fn send_file_result_serializes_with_kind_file() {
        let result = SendResult {
            command: "send",
            ok: true,
            target: DeviceJson {
                alias: "Peer".to_string(),
                ip: "192.168.1.10".to_string(),
                port: 53317,
                fingerprint: String::new(),
                https: true,
                version: "2.1".to_string(),
                device_type: None,
                device_model: None,
            },
            resolved_via: "ip",
            kind: SendKind::File {
                files: vec![SendFileResult {
                    name: "file.pdf".to_string(),
                    path: "/tmp/file.pdf".to_string(),
                    size: 1024,
                    status: "finished",
                    error: None,
                }],
            },
        };
        let json = serde_json::to_string(&result).expect("serialize");
        assert!(json.contains("\"kind\":\"file\""));
        assert!(json.contains("\"status\":\"finished\""));
        assert!(!json.contains("\"kind\":\"message\""));
    }

    #[test]
    fn send_message_result_serializes_with_kind_message() {
        let result = SendResult {
            command: "send",
            ok: true,
            target: DeviceJson {
                alias: "Peer".to_string(),
                ip: "192.168.1.10".to_string(),
                port: 53317,
                fingerprint: String::new(),
                https: true,
                version: "2.1".to_string(),
                device_type: None,
                device_model: None,
            },
            resolved_via: "ip",
            kind: SendKind::Message {
                text: "hello".to_string(),
                size: 5,
                status: "finished",
            },
        };
        let json = serde_json::to_string(&result).expect("serialize");
        assert!(json.contains("\"kind\":\"message\""));
        assert!(json.contains("\"text\":\"hello\""));
        assert!(!json.contains("\"files\""));
    }

    fn peer() -> DeviceJson {
        DeviceJson {
            alias: "Peer".to_string(),
            ip: "192.168.1.10".to_string(),
            port: 53317,
            fingerprint: String::new(),
            https: true,
            version: "2.1".to_string(),
            device_type: None,
            device_model: None,
        }
    }

    #[test]
    fn receive_event_ready_serializes_with_required_fields() {
        let event = ReceiveEventJson::Ready {
            alias: "Laptop".to_string(),
            port: 53317,
            https: true,
            receive_dir: "/tmp/inbox".to_string(),
        };
        let json = serde_json::to_string(&event).expect("serialize");
        assert!(json.contains("\"event\":\"ready\""));
        assert!(json.contains("\"alias\":\"Laptop\""));
        assert!(json.contains("\"port\":53317"));
        assert!(json.contains("\"https\":true"));
        assert!(json.contains("\"receive_dir\":\"/tmp/inbox\""));
    }

    #[test]
    fn receive_event_transfer_started_serializes() {
        let event = ReceiveEventJson::TransferStarted {
            sender_alias: "iPhone".to_string(),
            file_count: 3,
        };
        let json = serde_json::to_string(&event).expect("serialize");
        assert!(json.contains("\"event\":\"transfer_started\""));
        assert!(json.contains("\"sender_alias\":\"iPhone\""));
        assert!(json.contains("\"file_count\":3"));
    }

    #[test]
    fn receive_event_file_saved_serializes() {
        let event = ReceiveEventJson::FileSaved {
            path: "/tmp/inbox/foo.txt".to_string(),
            file_name: "foo.txt".to_string(),
            size: 1024,
        };
        let json = serde_json::to_string(&event).expect("serialize");
        assert!(json.contains("\"event\":\"file_saved\""));
        assert!(json.contains("\"path\":\"/tmp/inbox/foo.txt\""));
        assert!(json.contains("\"file_name\":\"foo.txt\""));
        assert!(json.contains("\"size\":1024"));
    }

    #[test]
    fn receive_event_transfer_complete_serializes() {
        let json = serde_json::to_string(&ReceiveEventJson::TransferComplete).expect("serialize");
        assert_eq!(json, "{\"event\":\"transfer_complete\"}");
    }

    #[test]
    fn receive_event_transfer_finished_with_errors_serializes() {
        let json = serde_json::to_string(&ReceiveEventJson::TransferFinishedWithErrors)
            .expect("serialize");
        assert_eq!(json, "{\"event\":\"transfer_finished_with_errors\"}");
    }

    #[test]
    fn receive_event_transfer_cancelled_serializes() {
        let json = serde_json::to_string(&ReceiveEventJson::TransferCancelled).expect("serialize");
        assert_eq!(json, "{\"event\":\"transfer_cancelled\"}");
    }

    #[test]
    fn receive_event_shutdown_serializes() {
        let json = serde_json::to_string(&ReceiveEventJson::Shutdown).expect("serialize");
        assert_eq!(json, "{\"event\":\"shutdown\"}");
    }

    #[test]
    fn device_json_omits_optional_fields_when_none() {
        let device = DeviceJson {
            alias: "x".into(),
            ip: "1.2.3.4".into(),
            port: 53317,
            fingerprint: "abc".into(),
            https: true,
            version: "2.1".into(),
            device_type: None,
            device_model: None,
        };
        let json = serde_json::to_string(&device).expect("serialize");
        assert!(!json.contains("device_type"));
        assert!(!json.contains("device_model"));
    }

    #[test]
    fn device_json_includes_optional_fields_when_set() {
        use localsend::model::discovery::DeviceType;
        let device = DeviceJson {
            alias: "x".into(),
            ip: "1.2.3.4".into(),
            port: 53317,
            fingerprint: "abc".into(),
            https: true,
            version: "2.1".into(),
            device_type: Some(device_type_label(&DeviceType::Mobile)),
            device_model: Some("iPhone".into()),
        };
        let json = serde_json::to_string(&device).expect("serialize");
        assert!(json.contains("\"device_type\":\"mobile\""));
        assert!(json.contains("\"device_model\":\"iPhone\""));
    }

    #[test]
    fn device_type_label_covers_all_variants() {
        use localsend::model::discovery::DeviceType;
        assert_eq!(device_type_label(&DeviceType::Mobile), "mobile");
        assert_eq!(device_type_label(&DeviceType::Desktop), "desktop");
        assert_eq!(device_type_label(&DeviceType::Web), "web");
        assert_eq!(device_type_label(&DeviceType::Headless), "headless");
        assert_eq!(device_type_label(&DeviceType::Server), "server");
    }

    #[test]
    fn scan_result_serializes_with_command_ok_timeout() {
        let result = ScanResult {
            command: "scan",
            ok: true,
            timeout_ms: 5000,
            devices: vec![peer()],
        };
        let json = serde_json::to_string(&result).expect("serialize");
        assert!(json.starts_with("{\"command\":\"scan\""));
        assert!(json.contains("\"ok\":true"));
        assert!(json.contains("\"timeout_ms\":5000"));
        assert!(json.contains("\"devices\""));
    }

    #[test]
    fn send_file_result_serializes_status() {
        let r = SendFileResult {
            name: "a.txt".into(),
            path: "/tmp/a.txt".into(),
            size: 10,
            status: "skipped",
            error: None,
        };
        let json = serde_json::to_string(&r).expect("serialize");
        assert!(json.contains("\"status\":\"skipped\""));
        assert!(json.contains("\"size\":10"));
    }

    #[test]
    fn send_file_result_serializes_error_only_when_present() {
        let ok = SendFileResult {
            name: "a.txt".into(),
            path: "/tmp/a.txt".into(),
            size: 10,
            status: "finished",
            error: None,
        };
        let json = serde_json::to_string(&ok).expect("serialize");
        assert!(!json.contains("\"error\""));

        let failed = SendFileResult {
            name: "a.txt".into(),
            path: "/tmp/a.txt".into(),
            size: 10,
            status: "failed",
            error: Some("500 Internal Server Error".into()),
        };
        let json = serde_json::to_string(&failed).expect("serialize");
        assert!(json.contains("\"status\":\"failed\""));
        assert!(json.contains("\"error\":\"500 Internal Server Error\""));
    }

    #[test]
    fn output_options_json_flag_forces_json() {
        let opts = OutputOptions::new(true, false);
        assert!(opts.is_json());
        // --quiet is implied by --json
        assert!(!opts.show_human_progress());
    }

    #[test]
    fn output_options_human_with_quiet() {
        let opts = OutputOptions::new(false, true);
        assert!(!opts.is_json());
        assert!(!opts.show_human_progress());
    }

    #[test]
    fn output_options_human_verbose() {
        let opts = OutputOptions::new(false, false);
        assert!(!opts.is_json());
        assert!(opts.show_human_progress());
    }
}
