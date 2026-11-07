use std::net::SocketAddr;

use anyhow::{Context, Result};
use tokio::signal;
use tokio::sync::mpsc;

use crate::config::AppConfig;
use crate::discovery;
use crate::events::ReceiveEvent;
use crate::identity::Identity;
use crate::output::{OutputOptions, ReceiveEventJson, print_json};
use crate::server::{ServerState, spawn_server};

pub async fn run(
    config: AppConfig,
    identity: Identity,
    receive_pin: Option<String>,
    output: OutputOptions,
    once: bool,
) -> Result<()> {
    let (stop_tx, mut stop_rx) = mpsc::unbounded_channel::<()>();

    let state = ServerState::new(
        config.clone(),
        identity.clone(),
        receive_pin,
        output.mode,
        once,
        Some(stop_tx),
    );
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    // Bind before spawning so the port is known to be available before
    // discovery announcements go out - no 100 ms sleep guesswork.
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|_| crate::error::CliError::PortInUse { port: config.port })?;
    let shutdown_state = state.clone();
    let mut server_task = spawn_server(state, listener)?;
    let responder = match discovery::run_responder(config.clone(), identity).await {
        Ok(responder) => responder,
        Err(error) => {
            shutdown_state.shutdown();
            let _ = server_task.await;
            return Err(error);
        }
    };

    if output.show_human_progress() {
        println!(
            "{} is ready to receive on port {} ({})",
            config.alias,
            config.port,
            if config.https { "HTTPS" } else { "HTTP" }
        );
        println!("Saving files to {}", config.receive_dir.display());
        if once {
            println!("Waiting for one transfer, then exiting.");
        } else {
            println!("Press Ctrl+C to stop.");
        }
    } else if output.is_json() {
        print_json(&ReceiveEventJson::Ready {
            alias: config.alias.clone(),
            port: config.port,
            https: config.https,
            receive_dir: config.receive_dir.display().to_string(),
        });
    }

    let (server_result, signal_result) = tokio::select! {
        result = &mut server_task => (Some(result), Ok(())),
        result = signal::ctrl_c() => (None, result),
        Some(()) = stop_rx.recv() => (None, Ok(())),
    };
    shutdown_state.shutdown();
    drop(responder);
    let server_result = match server_result {
        Some(result) => result,
        None => server_task.await,
    };

    if output.show_human_progress() {
        println!("Shutting down...");
    } else if output.is_json() {
        print_json(&ReceiveEventJson::Shutdown);
    }
    server_result.context("Receiver server task failed")??;
    signal_result.context("Failed to listen for Ctrl+C")
}

/// Run the receiver with a programmatic event channel. The CLI's `receive`
/// subcommand uses [`run`]; the desktop app uses this entry point so it can
/// observe receive-lifecycle events without parsing stdout.
///
/// Returns a stop sender and an event receiver. Sending a stop message cancels
/// uploads, drains connections, and emits Shutdown after cleanup completes.
pub async fn run_with_events(
    config: AppConfig,
    identity: Identity,
    receive_pin: Option<String>,
    output_mode: crate::output::OutputMode,
) -> Result<(
    mpsc::UnboundedSender<()>,
    mpsc::UnboundedReceiver<ReceiveEvent>,
)> {
    let (stop_tx, mut stop_rx) = mpsc::unbounded_channel::<()>();
    let (event_tx, event_rx) = mpsc::unbounded_channel::<ReceiveEvent>();

    let state = ServerState::new_with_events(
        config.clone(),
        identity.clone(),
        receive_pin,
        output_mode,
        false,
        Some(stop_tx.clone()),
        Some(event_tx.clone()),
    );
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    // Bind before spawning so the port is known to be available before
    // discovery announcements go out - no 100 ms sleep guesswork.
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|_| crate::error::CliError::PortInUse { port: config.port })?;
    let shutdown_state = state.clone();
    let mut server_task = spawn_server(state, listener)?;

    // Start the multicast discovery responder so peers can find us by UDP scan.
    // Failure here is non-fatal for the desktop app: receive still works for
    // peers that already know our IP, and the tray will surface "degraded".
    let _responder = discovery::run_responder(config.clone(), identity.clone()).await;
    let responder_handle = match _responder {
        Ok(h) => Some(h),
        Err(e) => {
            tracing::warn!("discovery responder failed to start: {e}");
            None
        }
    };

    let _ = event_tx.send(ReceiveEvent::Ready {
        alias: config.alias.clone(),
        port: config.port,
        https: config.https,
        receive_dir: config.receive_dir.clone(),
    });

    // Keep the event channel open until every connection has finished cleanup.
    tokio::spawn(async move {
        let stopped = tokio::select! {
            _ = stop_rx.recv() => true,
            result = &mut server_task => {
                tracing::warn!(?result, "receiver server stopped");
                false
            }
        };
        shutdown_state.shutdown();
        drop(responder_handle);
        if stopped {
            let _ = server_task.await;
        }
        drop(shutdown_state);
        if stopped {
            let _ = event_tx.send(ReceiveEvent::Shutdown);
        }
    });

    Ok((stop_tx, event_rx))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn invalid_tls_fails_before_cli_or_event_receiver_is_ready() {
        let dir = std::env::temp_dir().join(format!("lsend-startup-{}", uuid::Uuid::new_v4()));
        let config = AppConfig {
            alias: "Test".into(),
            port: 0,
            https: true,
            multicast_group: crate::config::DEFAULT_MULTICAST_GROUP.into(),
            config_dir: dir.clone(),
            receive_dir: dir,
        };
        let identity = Identity {
            cert_pem: String::new(),
            key_pem: String::new(),
            fingerprint: "test".into(),
        };
        let error = run(
            config.clone(),
            identity.clone(),
            None,
            OutputOptions::new(true, true),
            false,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("Failed to parse certificate"));
        let error = run_with_events(config, identity, None, crate::output::OutputMode::Json)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Failed to parse certificate"));
    }
}
