//! Receive-lifecycle events for programmatic consumers (e.g. the desktop tray app).
//!
//! The CLI surfaces the same transitions via JSON NDJSON on stdout
//! (see [`crate::output::ReceiveEventJson`]). This enum is the plain-Rust twin,
//! emitted to an optional [`tokio::sync::mpsc::UnboundedSender`] plugged into
//! [`crate::server::ServerState`]. CLI callers pass `None`; the desktop app
//! passes `Some(tx)` to drive system notifications and tray state without
//! parsing stdout.

use std::path::PathBuf;

#[derive(Debug, Clone)]
pub enum ReceiveEvent {
    /// Peer observed through the resident receiver's HTTP registration endpoint.
    PeerDiscovered(crate::discovery::DiscoveredDevice),

    /// Emitted once after the receiver binds its listener and discovery responder.
    Ready {
        alias: String,
        port: u16,
        https: bool,
        receive_dir: PathBuf,
    },

    /// A sender announced an incoming transfer (the auto-accept handler has
    /// already created a session; declined sessions never emit this).
    TransferStarted {
        sender_alias: String,
        file_count: usize,
    },

    /// Embedded text message received out-of-band (no file body).
    MessageReceived {
        sender_alias: String,
        text: String,
        size: u64,
    },

    /// One file finished writing to disk.
    FileSaved {
        path: PathBuf,
        file_name: String,
        size: u64,
    },

    /// All files in a session saved successfully.
    TransferComplete,

    /// The session ended with at least one failed file; others may have saved.
    TransferFinishedWithErrors,

    /// The receiver rejected a transfer before creating a session (e.g. the
    /// sender is not in the pairing list, or the pairing store is unreadable).
    TransferRejected {
        sender_alias: String,
        reason: &'static str,
    },

    /// The sender cancelled mid-session.
    TransferCancelled,

    /// The receiver is shutting down (user stopped it or app is quitting).
    Shutdown,
}
