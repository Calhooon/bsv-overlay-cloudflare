//! P0-2d (bsv-stack-lean #52, `docs/p0/p0-2d.md`): the Arcade status vector
//! replayed through this crate's readings of Arcade's words.
//!
//! `crates/overlay-discovery/vectors/arcade_status_verdicts.json` (carried
//! byte-identical by bsv-wallet-toolbox-rs) holds one recorded body per Arcade
//! status word on each surface, the intake refusals and the two reorg
//! payloads (arcade@1ae1208). Its `engine_*` columns are the verdicts this
//! crate must give:
//! - `engine_gate`: [`classify_arcade_status`] against the gate's target, the
//!   word every gate path (the submit echo, the poll, the probe, the witness
//!   look) reads;
//! - `engine_callback`: an `/arc-ingest` body through
//!   [`classify_arc_ingest_body`] and [`callback_action`] (a body with a
//!   `merklePath` is the proof arm and carries no callback column);
//! - `engine_submit`: [`classify_submit_response`] on the HTTP answer;
//! - `engine_proof`: [`parse_arcade_proof_look`] at `{NOW}`.
//!
//! The `engine_marker` column is replayed in overlay-discovery
//! (`tests/arcade_status_vector.rs`), where the marker reader lives.

use crate::admit_fast::{callback_action, CallbackAction};
use crate::broadcaster::{
    classify_arcade_status, classify_submit_response, GateVerdict, SubmitOutcome,
    ARCADE_GATE_STATUS,
};
use crate::proof_fetcher::{parse_arcade_proof_look, rfc3339_utc_ms, ArcadeProofLook};
use crate::routes::{classify_arc_ingest_body, ArcIngestBody};

const VECTOR: &str = include_str!("../../overlay-discovery/vectors/arcade_status_verdicts.json");
const NOW: &str = "2026-10-08T12:00:00Z";
const TXID: &str = "aa00000000000000000000000000000000000000000000000000000000000001";
const COMPETITOR: &str = "bb00000000000000000000000000000000000000000000000000000000000002";
const BLOCKHASH: &str = "0000000000000000000000000000000000000000000000000000000000000abc";
const BUMP: &str = "fed0ac0c000101020304";

/// One case's body with the vector's placeholders filled.
fn body(case: &serde_json::Value) -> String {
    serde_json::to_string(&case["body"])
        .unwrap()
        .replace("{TXID}", TXID)
        .replace("{COMPETITOR}", COMPETITOR)
        .replace("{BLOCKHASH}", BLOCKHASH)
        .replace("{BUMP}", BUMP)
        .replace("{NOW}", NOW)
}

fn gate(word: &str) -> &'static str {
    match classify_arcade_status(word, ARCADE_GATE_STATUS) {
        GateVerdict::Reached => "reached",
        GateVerdict::Fatal => "fatal",
        GateVerdict::Orphan => "orphan",
        GateVerdict::Pending => "pending",
    }
}

fn callback(action: CallbackAction) -> &'static str {
    match action {
        CallbackAction::LatchSeen => "latch_seen",
        CallbackAction::EvidenceCheck(_) => "evidence_check",
        CallbackAction::Ignore => "ignore",
    }
}

fn submit(outcome: SubmitOutcome) -> &'static str {
    match outcome {
        SubmitOutcome::Processing(_) => "processing",
        SubmitOutcome::SyncRejected(_) => "sync_rejected",
        SubmitOutcome::ViewRejected(_) => "view_rejected",
        SubmitOutcome::Transport(_) => "transport",
    }
}

fn proof(look: ArcadeProofLook) -> &'static str {
    match look {
        ArcadeProofLook::Mined(_) => "mined",
        ArcadeProofLook::KnownUnmined(_) => "known_unmined",
        ArcadeProofLook::Unknown => "unknown",
    }
}

/// Every `engine_*` column but the marker, every case: the list of wrong
/// verdicts, `case: column expected X, got Y`, and the number of checks.
fn replay() -> (Vec<String>, usize) {
    let v: serde_json::Value = serde_json::from_str(VECTOR).unwrap();
    let now_ms = rfc3339_utc_ms(NOW).unwrap();
    let mut wrong = Vec::new();
    let mut checks = 0;
    for case in v["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let expect = &case["expect"];
        let raw = body(case);
        let word = case["body"]["txStatus"].as_str();
        let mut check = |column: &str, got: &str| {
            checks += 1;
            let want = expect[column].as_str().unwrap();
            if got != want {
                wrong.push(format!("{name}: {column} expected {want}, got {got}"));
            }
        };
        if expect.get("engine_gate").is_some() {
            check("engine_gate", gate(word.unwrap()));
        }
        if expect.get("engine_callback").is_some() {
            let got = match case["surface"].as_str().unwrap() {
                // a push is read the way `/arc-ingest` reads it: the body first
                "push" => match classify_arc_ingest_body(&raw).unwrap() {
                    ArcIngestBody::StatusOnly { tx_status, .. } => {
                        callback(callback_action(&tx_status))
                    }
                    ArcIngestBody::Proof { .. } => "proof_arm",
                },
                _ => callback(callback_action(word.unwrap())),
            };
            check("engine_callback", got);
        }
        if expect.get("engine_submit").is_some() {
            let http = case["http"].as_u64().unwrap() as u16;
            check("engine_submit", submit(classify_submit_response(http, &raw)));
        }
        if expect.get("engine_proof").is_some() {
            let http = case["http"].as_u64().unwrap() as u16;
            check("engine_proof", proof(parse_arcade_proof_look(http, &raw, now_ms)));
        }
        if case["surface"] == "push" && expect.get("engine_callback").is_none() {
            // a push with a path is the proof arm, never a status-only callback
            checks += 1;
            if !matches!(
                classify_arc_ingest_body(&raw).unwrap(),
                ArcIngestBody::Proof { .. }
            ) {
                wrong.push(format!("{name}: a push with a merklePath read as status-only"));
            }
        }
    }
    (wrong, checks)
}

#[test]
fn every_engine_column_of_the_arcade_vector_holds() {
    let (wrong, checks) = replay();
    assert_eq!(checks, 75, "the vector's engine columns (marker aside) and the proof-arm pushes");
    assert!(
        wrong.is_empty(),
        "{} of {checks} Arcade verdicts wrong in overlay-cloudflare:\n  {}",
        wrong.len(),
        wrong.join("\n  ")
    );
}
