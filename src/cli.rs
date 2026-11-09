use clap::{Parser, Subcommand, ValueEnum};

use crate::config::AppConfig;

#[derive(Parser, Debug)]
#[command(
    name = "lsend",
    about = "Headless LocalSend CLI. Use --json or `lsend agent` for AI agents.",
    version
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Use plain HTTP instead of HTTPS.
    #[arg(long, global = true)]
    pub http: bool,

    /// Network port (default: 53317).
    #[arg(long, global = true, default_value_t = AppConfig::DEFAULT_PORT)]
    pub port: u16,

    /// Device display name.
    #[arg(long, global = true)]
    pub alias: Option<String>,

    /// Print diagnostic logs on stderr (RUST_LOG=info also works).
    #[arg(short, long, global = true)]
    pub verbose: bool,

    /// Emit structured JSON on stdout (implies --quiet).
    #[arg(long, global = true, visible_alias = "output-json")]
    pub json: bool,

    /// Suppress progress and status text on stdout (human mode only).
    #[arg(short, long, global = true)]
    pub quiet: bool,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Progressive agent documentation (offline, no network).
    Agent {
        #[command(subcommand)]
        topic: Option<AgentCommand>,
    },
    /// Manage the persisted device alias (device name).
    Alias(AliasOpts),

    /// Manage paired devices (receive trust list).
    Pair(PairOpts),

    /// Discover LocalSend devices on the local network.
    Scan {
        /// How long to wait for responses, in milliseconds.
        #[arg(long, default_value_t = AppConfig::DEFAULT_DISCOVERY_TIMEOUT_MS)]
        timeout: u64,
    },

    /// Send files to a device (IP address or alias from scan).
    Send {
        /// Target device IP or alias.
        target: String,

        /// Files or directories to send (omit when using --text, --message, or --clipboard).
        paths: Vec<String>,

        /// Read text from stdin (pipe). Example: echo "hello" | lsend send <IP> --text
        #[arg(long, conflicts_with = "message")]
        text: bool,

        /// Send an inline text message (UTF-8).
        #[arg(long, conflicts_with = "text")]
        message: Option<String>,

        /// Send plain text from the system clipboard.
        #[arg(long, conflicts_with_all = ["text", "message"])]
        clipboard: bool,

        /// PIN if the receiver requires one.
        #[arg(long)]
        pin: Option<String>,

        /// Do not scan when target is an alias; require IP or prior scan.
        #[arg(long)]
        no_scan: bool,
    },

    /// Start a receive server and accept incoming files automatically.
    Receive {
        /// Directory where received files are saved.
        #[arg(long)]
        dir: Option<String>,

        /// Exit after the first completed transfer (recommended for agents).
        #[arg(long)]
        once: bool,
        /// Accept transfers from unpaired devices even when a pair list exists.
        #[arg(long)]
        accept_unpaired: bool,

        /// Require this PIN from senders (persisted to config when set).
        #[arg(long)]
        pin: Option<String>,
    },
}

#[derive(clap::Args, Debug)]
#[command(arg_required_else_help = false)]
pub struct AliasOpts {
    #[command(subcommand)]
    pub action: Option<AliasAction>,
}

#[derive(Subcommand, Debug)]
pub enum AliasAction {
    /// Show the persisted device alias (creates one if missing).
    #[command(name = "show")]
    Show,

    /// Generate a new random alias and save it to alias.txt.
    Regenerate {
        /// Locale id for word lists (e.g. en, zh-CN). Defaults to system locale.
        #[arg(long)]
        locale: Option<String>,
    },

    /// Set a custom persisted device alias.
    Set {
        /// Device alias (device name shown to peers).
        name: String,
    },
}

#[derive(clap::Args, Debug)]
#[command(arg_required_else_help = false)]
pub struct PairOpts {
    #[command(subcommand)]
    pub action: Option<PairAction>,
}

#[derive(Subcommand, Debug)]
pub enum PairAction {
    /// List paired devices (default action).
    #[command(name = "list")]
    List,

    /// Pair a device by IP or alias (discovers it on the LAN first).
    Add {
        /// Target device IP or alias.
        target: String,
    },

    /// Remove a paired device by fingerprint or alias.
    Remove {
        /// Fingerprint or alias of the paired device.
        target: String,
    },
}

#[derive(Subcommand, Debug, Clone, Copy, ValueEnum)]
pub enum AgentCommand {
    /// Device alias management workflow.
    Alias,
    /// Device discovery workflow and JSON schema.
    Scan,
    /// File send workflow; prefer IP targets.
    Send,
    /// Receive workflow; use with --once.
    Receive,
    /// Paired-device trust list workflow.
    Pair,
    /// Exit codes and error JSON schema.
    Errors,
    /// Smoke-test checklist for agents.
    Eval,
}
