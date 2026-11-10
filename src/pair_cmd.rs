use std::path::Path;

use anyhow::{Result, bail};

use crate::cli::PairAction;
use crate::config::AppConfig;
use crate::discovery::{self, DiscoveredDevice};
use crate::error::CliError;
use crate::identity::Identity;
use crate::output::{
    OutputOptions, PairAddResult, PairListResult, PairRemoveResult, PairedDeviceJson,
    error_envelope, print_json,
};
use crate::pairing::{PairedDevice, PairedStore, paired_at_now};

pub async fn run(
    action: PairAction,
    config: &AppConfig,
    identity: &Identity,
    output: OutputOptions,
) -> Result<(), i32> {
    match action {
        PairAction::List => run_list(&config.config_dir, output),
        PairAction::Add { target } => run_add(&target, config, identity, output).await,
        PairAction::Remove { target } => run_remove(&target, &config.config_dir, output),
    }
}

fn run_list(config_dir: &Path, output: OutputOptions) -> Result<(), i32> {
    let store = PairedStore::load(config_dir).map_err(|e| fail("pair", output, e))?;
    let result = pair_list_result(&store);

    if output.is_json() {
        print_json(&result);
    } else if output.show_human_progress() {
        if result.devices.is_empty() {
            println!("No paired devices. Receive accepts any LAN peer while this list is empty.");
        } else {
            for device in &result.devices {
                println!("{} ({})", device.alias, device.fingerprint);
            }
        }
    } else {
        for device in &result.devices {
            println!("{}", device.fingerprint);
        }
    }

    Ok(())
}

/// Build the `pair list` result document shared by JSON and human output.
fn pair_list_result(store: &PairedStore) -> PairListResult {
    PairListResult {
        command: "pair",
        action: "list",
        ok: true,
        devices: store
            .devices
            .iter()
            .map(|d| PairedDeviceJson {
                alias: d.alias.clone(),
                fingerprint: d.fingerprint.clone(),
                device_model: d.device_model.clone(),
                paired_at: d.paired_at,
            })
            .collect(),
    }
}

async fn run_add(
    target: &str,
    config: &AppConfig,
    identity: &Identity,
    output: OutputOptions,
) -> Result<(), i32> {
    // Discovery is mandatory: pairing must record the peer's real announced
    // fingerprint, which an IP literal alone does not carry.
    let device = find_device(target, config, identity)
        .await
        .map_err(|e| fail("pair", output, e))?;

    let paired = PairedDevice {
        fingerprint: device.fingerprint.clone(),
        alias: device.alias.clone(),
        device_model: device.device_model.clone(),
        paired_at: paired_at_now(),
    };
    let json = PairedDeviceJson {
        alias: paired.alias.clone(),
        fingerprint: paired.fingerprint.clone(),
        device_model: paired.device_model.clone(),
        paired_at: paired.paired_at,
    };

    let mut store = PairedStore::load(&config.config_dir).map_err(|e| fail("pair", output, e))?;
    let created = store.upsert(paired);
    store
        .save(&config.config_dir)
        .map_err(|e| fail("pair", output, e))?;

    if output.is_json() {
        print_json(&PairAddResult {
            command: "pair",
            action: "add",
            ok: true,
            created,
            device: json,
        });
    } else if output.show_human_progress() {
        if created {
            println!("Paired {} ({}).", json.alias, json.fingerprint);
        } else {
            println!(
                "Refreshed pairing for {} ({}).",
                json.alias, json.fingerprint
            );
        }
        println!("Transfers from unpaired devices are now rejected while this list is non-empty.");
    } else {
        println!("{}", json.fingerprint);
    }

    Ok(())
}

fn run_remove(target: &str, config_dir: &Path, output: OutputOptions) -> Result<(), i32> {
    let mut store = PairedStore::load(config_dir).map_err(|e| fail("pair", output, e))?;
    let Some(removed) = store.remove(target) else {
        return Err(fail(
            "pair",
            output,
            CliError::TargetNotFound {
                target: target.to_string(),
            },
        ));
    };
    store
        .save(config_dir)
        .map_err(|e| fail("pair", output, e))?;

    let json = PairedDeviceJson {
        alias: removed.alias,
        fingerprint: removed.fingerprint,
        device_model: removed.device_model,
        paired_at: removed.paired_at,
    };

    if output.is_json() {
        print_json(&PairRemoveResult {
            command: "pair",
            action: "remove",
            ok: true,
            device: json,
        });
    } else if output.show_human_progress() {
        println!("Unpaired {} ({}).", json.alias, json.fingerprint);
        if store.devices.is_empty() {
            println!("The pair list is empty; receive accepts any LAN peer again.");
        }
    } else {
        println!("{}", json.fingerprint);
    }

    Ok(())
}

/// Resolve a pair target by alias or IP through a real scan so the returned
/// device carries the peer's announced fingerprint.
async fn find_device(
    target: &str,
    config: &AppConfig,
    identity: &Identity,
) -> Result<DiscoveredDevice> {
    let devices = discovery::scan(
        config,
        identity,
        AppConfig::DEFAULT_DISCOVERY_TIMEOUT_MS * 4,
    )
    .await?;
    let device = devices
        .into_iter()
        .find(|d| d.alias.eq_ignore_ascii_case(target) || d.ip == target)
        .ok_or(CliError::TargetNotFound {
            target: target.to_string(),
        })?;
    if device.fingerprint.is_empty() {
        bail!("Discovered device {target} announced no fingerprint; cannot pair");
    }
    Ok(device)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remove_unknown_target_maps_to_not_found_exit_two() {
        let dir = std::env::temp_dir().join(format!("lsend-paircmd-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let code = run_remove("ghost", &dir, OutputOptions::new(true, true)).unwrap_err();
        assert_eq!(code, 2);
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn list_json_shape_matches_agent_contract() {
        let store = PairedStore {
            devices: vec![
                PairedDevice {
                    fingerprint: "AA:11".to_string(),
                    alias: "Laptop".to_string(),
                    device_model: None,
                    paired_at: 1_730_000_000,
                },
                PairedDevice {
                    fingerprint: "BB:22".to_string(),
                    alias: "Pixel".to_string(),
                    device_model: Some("Pixel 8".to_string()),
                    paired_at: 1_730_000_001,
                },
            ],
        };

        let json = serde_json::to_value(pair_list_result(&store)).unwrap();

        assert_eq!(json["command"], "pair");
        assert_eq!(json["action"], "list");
        assert_eq!(json["ok"], true);
        let devices = json["devices"].as_array().unwrap();
        assert_eq!(devices.len(), 2);
        assert_eq!(devices[0]["alias"], "Laptop");
        assert_eq!(devices[0]["fingerprint"], "AA:11");
        assert_eq!(devices[0]["paired_at"], 1_730_000_000);
        // device_model must be absent, not null, when unknown.
        assert!(devices[0].get("device_model").is_none());
        assert_eq!(devices[1]["device_model"], "Pixel 8");
    }
}
