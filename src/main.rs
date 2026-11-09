mod agent;
mod alias;
mod alias_cmd;
mod cli;
mod config;
mod discovery;
mod error;
mod events;
mod identity;
mod legacy;
mod mtls;
mod network;
mod output;
mod pair_cmd;
mod pairing;
mod port;
mod receive;
mod receive_pin;
mod scan_server;
mod send;
mod server;
mod text_send;
mod util;

use crate::cli::{AliasAction, Cli, Commands, PairAction};
use crate::config::AppConfig;
use crate::error::CliError;
use crate::identity::Identity;
use crate::output::{DeviceJson, OutputOptions, ScanResult, error_envelope, print_json};
use anyhow::Result;
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    if let Err(code) = run().await {
        std::process::exit(code);
    }
}

async fn run() -> Result<(), i32> {
    let cli = Cli::parse();
    let output = OutputOptions::from_cli(cli.json, cli.quiet);

    if matches!(cli.command, Commands::Agent { .. }) {
        if let Commands::Agent { topic } = cli.command {
            agent::print(topic);
        }
        return Ok(());
    }

    if let Commands::Alias(opts) = cli.command {
        let action = opts.action.unwrap_or(AliasAction::Show);
        return alias_cmd::run(action, output);
    }

    let default_level = if cli.verbose {
        "lsend=info"
    } else {
        "lsend=warn"
    };
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    let https = !cli.http;
    let config =
        AppConfig::new(cli.alias, cli.port, https, None).map_err(|e| fail("lsend", output, e))?;
    let identity = Identity::load_or_create(&config.config_dir, https)
        .map_err(|e| fail("lsend", output, e))?;

    match cli.command {
        Commands::Agent { .. } | Commands::Alias(_) => unreachable!(),
        Commands::Pair(opts) => {
            let action = opts.action.unwrap_or(PairAction::List);
            return pair_cmd::run(action, &config, &identity, output).await;
        }
        Commands::Scan { timeout } => cmd_scan(&config, &identity, timeout, output)
            .await
            .map_err(|e| fail("scan", output, e))?,
        Commands::Send {
            target,
            paths,
            text,
            message,
            clipboard,
            pin,
            no_scan,
        } => {
            let request = send::SendRequest {
                target: &target,
                paths: &paths,
                text_stdin: text,
                message: message.as_deref(),
                clipboard,
                pin: pin.as_deref(),
                no_scan,
            };
            let outcome = send::send_files(&config, &identity, &request, output)
                .await
                .map_err(|e| fail("send", output, e))?;
            // Partial failures already printed the per-file result document;
            // only the exit status remains.
            if let send::SendOutcome::FailedFiles(_) = outcome {
                return Err(outcome.exit_code());
            }
        }
        Commands::Receive {
            dir,
            once,
            pin,
            accept_unpaired,
        } => {
            let mut config = config;
            if let Some(dir) = dir {
                config.receive_dir = dir.into();
            }
            let receive_pin = receive_pin::resolve(&config.config_dir, pin)
                .map_err(|e| fail("receive", output, e))?;
            receive::run(config, identity, receive_pin, accept_unpaired, output, once)
                .await
                .map_err(|e| fail("receive", output, e))?;
        }
    }

    Ok(())
}

fn fail(command: &'static str, output: OutputOptions, error: impl Into<anyhow::Error>) -> i32 {
    let cli_error = CliError::from_anyhow(error.into());
    let code = cli_error.exit_code();
    match output.is_json() {
        true => print_json(&error_envelope(command, &cli_error)),
        false => {
            eprintln!("Error: {cli_error}");
            if let Some(hint) = cli_error.hint() {
                eprintln!("Hint: {hint}");
            }
        }
    }
    code
}

async fn cmd_scan(
    config: &AppConfig,
    identity: &Identity,
    timeout_ms: u64,
    output: OutputOptions,
) -> Result<()> {
    if output.show_human_progress() {
        println!("Scanning for LocalSend devices ({timeout_ms} ms)...");
    }

    let devices = discovery::scan(config, identity, timeout_ms).await?;

    if output.is_json() {
        print_json(&ScanResult {
            command: "scan",
            ok: true,
            timeout_ms,
            devices: devices.iter().map(DeviceJson::from).collect(),
        });
    } else if output.show_human_progress() {
        if devices.is_empty() {
            println!("No devices found.");
        } else {
            for device in devices {
                println!("  {}", device.display_label());
            }
        }
    } else {
        for device in devices {
            println!("{}", device.display_label());
        }
    }

    Ok(())
}
