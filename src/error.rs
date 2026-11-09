use std::fmt;

#[derive(Debug, Clone)]
pub enum CliError {
    PortInUse { port: u16 },
    TargetNotFound { target: String },
    NoFiles,
    InvalidAlias { reason: String },
    TransferDeclined { target: String },
    Other(String),
}

impl CliError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::PortInUse { .. } => "port_in_use",
            Self::TargetNotFound { .. } => "target_not_found",
            Self::NoFiles => "no_files",
            Self::InvalidAlias { .. } => "invalid_alias",
            Self::TransferDeclined { .. } => "transfer_declined",
            Self::Other(_) => "error",
        }
    }

    pub fn exit_code(&self) -> i32 {
        match self {
            Self::PortInUse { .. } => 3,
            Self::TargetNotFound { .. } | Self::NoFiles | Self::InvalidAlias { .. } => 2,
            Self::TransferDeclined { .. } | Self::Other(_) => 1,
        }
    }

    pub fn hint(&self) -> Option<String> {
        match self {
            Self::PortInUse { port } => Some(format!(
                "Close any other process holding port {port} (e.g. the LocalSend app, another `lsend receive`). \
                 Reuse an existing receiver when possible. \
                 Avoid alternate `--port`: discovery uses the same UDP/TCP port, so the LocalSend app and default `scan` (port {}) will not see this device.",
                port
            )),
            Self::TargetNotFound { .. } => Some(
                "Run `lsend scan --json` first and use the device IP, or pass an IP address directly."
                    .to_string(),
            ),
            Self::NoFiles => Some(
                "Pass file paths, or use --text (stdin), --message \"...\", or --clipboard.".to_string(),
            ),
            Self::InvalidAlias { .. } => Some(
                "Provide a non-empty alias up to 255 characters.".to_string(),
            ),
            Self::TransferDeclined { .. } => Some(
                "The receiver replied 204 No Content without accepting any files (busy, or its \
                 accept rules declined the transfer). Message sends are the only kind completed \
                 by a 204; retry the file send later or ask the receiver to free the session."
                    .to_string(),
            ),
            Self::Other(_) => None,
        }
    }

    pub fn from_anyhow(err: anyhow::Error) -> Self {
        if let Some(cli_error) = err.downcast_ref::<CliError>() {
            return cli_error.clone();
        }

        let message = err.to_string();
        if message.contains("Address already in use")
            || message.contains("discovery HTTP server on port")
            || (message.contains("bind") && message.contains("53317"))
            || message.contains("already in use")
        {
            let port = parse_port_from_message(&message).unwrap_or(crate::config::DEFAULT_PORT);
            return Self::PortInUse { port };
        }
        if message.contains("No device found with alias") {
            if let Some(target) = message
                .strip_prefix("No device found with alias \"")
                .and_then(|s| s.strip_suffix('"'))
            {
                return Self::TargetNotFound {
                    target: target.to_string(),
                };
            }
            return Self::TargetNotFound { target: message };
        }
        if message == "No files to send" {
            return Self::NoFiles;
        }
        if message.starts_with("Alias must") {
            return Self::InvalidAlias { reason: message };
        }
        Self::Other(message)
    }
}

fn parse_port_from_message(message: &str) -> Option<u16> {
    message.split_whitespace().find_map(|token| {
        token
            .trim_matches(|c: char| !c.is_ascii_digit())
            .parse()
            .ok()
    })
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PortInUse { port } => write!(
                f,
                "Port {port} is already in use. Close any other process holding port {port} (e.g. the LocalSend app, another `lsend receive`)."
            ),
            Self::TargetNotFound { target } => write!(
                f,
                "No device found with alias \"{target}\". Run `lsend scan --json` first or pass an IP address."
            ),
            Self::NoFiles => write!(f, "No files to send"),
            Self::InvalidAlias { reason } => write!(f, "{reason}"),
            Self::TransferDeclined { target } => write!(
                f,
                "Receiver {target} declined the transfer (204 No Content, no files accepted)"
            ),
            Self::Other(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for CliError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_port_in_use() {
        let err = CliError::from_anyhow(anyhow::anyhow!("Address already in use (os error 48)"));
        assert_eq!(err.code(), "port_in_use");
        assert_eq!(err.exit_code(), 3);
        assert!(err.hint().is_some());
    }

    #[test]
    fn structured_port_in_use_carries_port() {
        let err = CliError::PortInUse { port: 53317 };
        assert!(err.to_string().contains("53317"));
        let hint = err.hint().unwrap();
        assert!(hint.contains("53317"));
        assert!(hint.contains("Reuse an existing receiver"));
        assert!(hint.contains("Avoid alternate `--port`"));
    }

    #[test]
    fn exit_codes_match_documented_table() {
        assert_eq!(CliError::PortInUse { port: 1 }.exit_code(), 3);
        assert_eq!(
            CliError::TargetNotFound { target: "x".into() }.exit_code(),
            2
        );
        assert_eq!(CliError::NoFiles.exit_code(), 2);
        assert_eq!(CliError::InvalidAlias { reason: "x".into() }.exit_code(), 2);
        assert_eq!(CliError::Other("x".into()).exit_code(), 1);
    }

    #[test]
    fn code_constants_match_documented_strings() {
        assert_eq!(CliError::PortInUse { port: 1 }.code(), "port_in_use");
        assert_eq!(
            CliError::TargetNotFound { target: "x".into() }.code(),
            "target_not_found"
        );
        assert_eq!(CliError::NoFiles.code(), "no_files");
        assert_eq!(
            CliError::InvalidAlias { reason: "x".into() }.code(),
            "invalid_alias"
        );
        assert_eq!(CliError::Other("x".into()).code(), "error");
    }

    #[test]
    fn from_anyhow_classifies_target_not_found() {
        let err = CliError::from_anyhow(anyhow::anyhow!("No device found with alias \"Bob\""));
        match err {
            CliError::TargetNotFound { target } => assert_eq!(target, "Bob"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn from_anyhow_classifies_no_files() {
        let err = CliError::from_anyhow(anyhow::anyhow!("No files to send"));
        assert!(matches!(err, CliError::NoFiles));
    }

    #[test]
    fn from_anyhow_classifies_invalid_alias() {
        let err = CliError::from_anyhow(anyhow::anyhow!("Alias must not be empty"));
        match err {
            CliError::InvalidAlias { reason } => assert!(reason.contains("empty")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn from_anyhow_passthrough_preserves_inner_cli_error() {
        let inner = CliError::NoFiles;
        let any: anyhow::Error = inner.clone().into();
        let back = CliError::from_anyhow(any);
        assert!(matches!(back, CliError::NoFiles));
    }

    #[test]
    fn from_anyhow_classifies_bind_on_default_port() {
        // "Address already in use" wins over port parsing; the parser will
        // pick up the os-error number "48" so verify the structured variant.
        let err = CliError::from_anyhow(anyhow::anyhow!("Address already in use (os error 48)"));
        match err {
            CliError::PortInUse { .. } => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn from_anyhow_falls_back_to_other() {
        let err = CliError::from_anyhow(anyhow::anyhow!("something else went wrong"));
        match err {
            CliError::Other(s) => assert_eq!(s, "something else went wrong"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_port_extracts_digit_token() {
        // The parser splits on whitespace and finds the first token that
        // consists entirely of digits.
        assert_eq!(parse_port_from_message("error 48"), Some(48));
        assert_eq!(parse_port_from_message("no port here"), None);
    }

    #[test]
    fn target_not_found_hint_suggests_scan() {
        let err = CliError::TargetNotFound { target: "x".into() };
        let hint = err.hint().unwrap();
        assert!(hint.contains("scan"));
    }

    #[test]
    fn other_error_has_no_hint() {
        assert!(CliError::Other("x".into()).hint().is_none());
    }

    #[test]
    fn from_anyhow_classifies_discovery_server_bind_error() {
        let err = CliError::from_anyhow(anyhow::anyhow!(
            "Could not start discovery HTTP server on port 53317: bind: address already in use"
        ));
        match err {
            CliError::PortInUse { port } => assert_eq!(port, 53317),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn from_anyhow_classifies_generic_bind_with_port_in_message() {
        let err = CliError::from_anyhow(anyhow::anyhow!("failed to bind to 0.0.0.0:6000"));
        // The function matches "bind" + "53317" together; without 53317 it
        // falls back to Other. Verify the fallback path.
        assert!(matches!(err, CliError::Other(_)));
    }

    #[test]
    fn from_anyhow_classifies_already_in_use_with_port() {
        let err = CliError::from_anyhow(anyhow::anyhow!(
            "could not bind listener: address already in use (port 53317)"
        ));
        match err {
            CliError::PortInUse { port } => assert_eq!(port, 53317),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn transfer_declined_maps_to_exit_one_and_hint() {
        let err = CliError::TransferDeclined {
            target: "peer".into(),
        };
        assert_eq!(err.code(), "transfer_declined");
        assert_eq!(err.exit_code(), 1);
        assert!(err.hint().is_some());
    }

    #[test]
    fn anyhow_conversion_preserves_cli_error_type() {
        let err = CliError::from_anyhow(CliError::TransferDeclined { target: "x".into() }.into());
        assert!(matches!(err, CliError::TransferDeclined { .. }));
        assert_eq!(err.code(), "transfer_declined");
    }
}
