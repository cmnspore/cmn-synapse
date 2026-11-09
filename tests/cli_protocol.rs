#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::process::{Command, Output};

fn synapse(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_synapse"))
        .args(args)
        .output()
        .expect("run synapse")
}

fn parse_event(bytes: &[u8]) -> serde_json::Value {
    let text = std::str::from_utf8(bytes).expect("UTF-8 event");
    let line = text
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .expect("one event");
    serde_json::from_str(line).expect("JSON event")
}

#[test]
fn help_is_strict_help_v2_on_stdout() {
    let output = synapse(&["--help"]);
    assert!(output.status.success());
    assert!(output.stderr.is_empty());

    let event = parse_event(&output.stdout);
    assert!(agent_first_data::validate_protocol_event(&event, true).is_ok());
    assert_eq!(event["kind"], "result");
    assert_eq!(event["result"]["code"], "help");
    assert_eq!(event["result"]["help"]["schema"], "cli-help-v2");
    assert_eq!(
        event["result"]["help"]["shapes"].as_array().unwrap().len(),
        3
    );
}

#[test]
fn structural_error_is_strict_stderr_and_exit_two() {
    let output = synapse(&["--definitely-unknown"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());

    let event = parse_event(&output.stderr);
    assert!(agent_first_data::validate_protocol_event(&event, true).is_ok());
    assert_eq!(event["kind"], "error");
    assert_eq!(event["error"]["code"], "cli_unknown_argument");
}

#[test]
fn missing_config_is_domain_error_on_stderr_and_exit_one() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing.yml");
    let output = Command::new(env!("CARGO_BIN_EXE_synapse"))
        .arg("--config")
        .arg(&missing)
        .output()
        .expect("run synapse");

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(!missing.exists());

    let event = parse_event(&output.stderr);
    assert!(agent_first_data::validate_protocol_event(&event, true).is_ok());
    assert_eq!(event["kind"], "error");
    assert_eq!(event["error"]["code"], "config_load_failed");
}

#[test]
fn raw_service_shape_rejects_protocol_output_flags_before_io() {
    let output = synapse(&["--output", "yaml"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());

    let event = parse_event(&output.stderr);
    assert!(agent_first_data::validate_protocol_event(&event, true).is_ok());
    assert_eq!(event["kind"], "error");
    assert_eq!(event["error"]["code"], "cli_unregistered_combination");
}

#[test]
fn lifecycle_output_can_be_redirected_to_a_file() {
    let dir = tempfile::tempdir().unwrap();
    let output_path = dir.path().join("help.jsonl");
    let output = Command::new(env!("CARGO_BIN_EXE_synapse"))
        .arg("--help")
        .arg("--stdout-file")
        .arg(&output_path)
        .output()
        .expect("run synapse");

    assert!(output.status.success());
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());

    let bytes = std::fs::read(&output_path).unwrap();
    let event = parse_event(&bytes);
    assert!(agent_first_data::validate_protocol_event(&event, true).is_ok());
    assert_eq!(event["result"]["help"]["schema"], "cli-help-v2");
}

#[cfg(all(feature = "redb", feature = "nostr"))]
#[tokio::test]
async fn explicit_history_cleanup_cli_persists_through_restart() {
    use nostr_sdk::prelude::{EventBuilder, FinalizeEvent, Keys, Kind, Tag};
    use synapse::storage::Storage;

    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("data.redb");
    let storage = synapse::storage::RedbStorage::open(&database).unwrap();
    let payload = serde_json::json!({"capsule":{"uri":"cmn://cleanup.test/b3.data"}});
    storage
        .save_spore(
            "b3.data",
            "cleanup.test",
            &serde_json::to_vec(&payload).unwrap(),
            &[],
        )
        .await
        .unwrap();
    let event = EventBuilder::new(Kind::Custom(30078), payload.to_string())
        .tag(Tag::hashtag("cmn-spore"))
        .finalize(&Keys::generate())
        .unwrap();
    storage
        .save_nostr_event(&event.id.to_hex(), event.as_json().as_bytes())
        .await
        .unwrap();
    storage
        .save_nostr_event("legacy-malformed", b"{}")
        .await
        .unwrap();
    drop(storage);
    let config = serde_json::json!({"debug":false,"storage":{"backend":"redb","redb_path":database},"http":{"enabled":true,"address":"127.0.0.1:0"}});
    let config_path = directory.path().join("config.json");
    std::fs::write(&config_path, serde_json::to_vec(&config).unwrap()).unwrap();
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_synapse"))
        .arg("--prune-nostr")
        .arg("--config")
        .arg(&config_path)
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    let storage = synapse::storage::StorageBackend::Redb(
        synapse::storage::RedbStorage::open(&database).unwrap(),
    );
    assert_eq!(storage.get_nostr_events().await.unwrap().len(), 1);
    let config: synapse::config::NostrConfig = serde_json::from_value(
        serde_json::json!({"enabled":true,"key_store":"storage","max_events":10}),
    )
    .unwrap();
    let restarted = synapse::nostr::NostrBridge::from_config(&config, &storage, 1_048_576)
        .await
        .unwrap();
    assert_eq!(
        restarted.events().read().await.front().unwrap().id,
        event.id
    );
}
