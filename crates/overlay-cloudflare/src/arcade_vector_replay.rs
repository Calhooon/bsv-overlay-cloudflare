//! P0-2d (bsv-stack-lean #52, `docs/p0/p0-2d.md`): the Arcade status vector
//! replayed through this crate's readings of Arcade's words.
//!
//! `crates/overlay-discovery/vectors/arcade_status_verdicts.json` (carried
//! byte-identical by bsv-wallet-toolbox-rs) holds one recorded body per Arcade
//! status word on each surface, the intake refusals and the two reorg
//! payloads (arcade@1ae1208). Its `engine_*` columns are the verdicts this
//! crate must give:
//! - `engine_gate`: [`classify_arcade_status`], the reading every gate path
//!   (the submit echo, the poll, the probe, the witness look) calls;
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
    match classify_arcade_status(word) {
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
        CallbackAction::HoldNotFinal(_) => "hold",
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
            check(
                "engine_submit",
                submit(classify_submit_response(http, &raw)),
            );
        }
        if expect.get("engine_proof").is_some() {
            let http = case["http"].as_u64().unwrap() as u16;
            check(
                "engine_proof",
                proof(parse_arcade_proof_look(http, &raw, now_ms)),
            );
        }
        if case["surface"] == "push" && expect.get("engine_callback").is_none() {
            // a push with a path is the proof arm, never a status-only callback
            checks += 1;
            if !matches!(
                classify_arc_ingest_body(&raw).unwrap(),
                ArcIngestBody::Proof { .. }
            ) {
                wrong.push(format!(
                    "{name}: a push with a merklePath read as status-only"
                ));
            }
        }
    }
    (wrong, checks)
}

#[test]
fn every_engine_column_of_the_arcade_vector_holds() {
    let (wrong, checks) = replay();
    assert_eq!(
        checks, 75,
        "the vector's engine columns (marker aside) and the proof-arm pushes"
    );
    assert!(
        wrong.is_empty(),
        "{} of {checks} Arcade verdicts wrong in overlay-cloudflare:\n  {}",
        wrong.len(),
        wrong.join("\n  ")
    );
}

/// P0-2d, moved here at L8 (#624) from overlay-discovery with the reading it
/// pins: every word Arcade defines reads its vector class, in any letter case,
/// through the tracker's `Verdict::from_arc_body` + `into_hint` and the door's
/// policy over the hint (`door_words`).
#[test]
fn every_arcade_word_reads_its_class_through_the_tracker() {
    use crate::door_words::{arcade_hint, network_holds, proof_class, ProofClass};
    use bsv_tracker::HintStatus;
    let v: serde_json::Value = serde_json::from_str(VECTOR).unwrap();
    let words = v["words"].as_array().unwrap();
    assert_eq!(
        words.len(),
        12,
        "arcade@1ae1208 models/transaction.go:89-126"
    );
    let mut wrong = Vec::new();
    for w in words {
        let word = w["word"].as_str().unwrap();
        let want = w["class"].as_str().unwrap();
        for spelled in [word.to_string(), word.to_ascii_lowercase()] {
            let got = match arcade_hint(&spelled, "", 0) {
                None => "invalid",
                Some(h) => match h.status {
                    HintStatus::Rejected { .. } => "rejected",
                    HintStatus::DoubleSpend { .. } => "conflict",
                    HintStatus::OrphanMempool => "orphan",
                    _ if proof_class(Some(&h)) == ProofClass::Mined => "mined",
                    _ if network_holds(&h) => "seen",
                    _ => "pending",
                },
            };
            if got != want {
                wrong.push(format!("{spelled}: expected {want}, got {got}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// bsv-low #836: the loop 44 bodies (`tests/fixtures/arcade_lock_time_vectors.json`, frozen) through every reading
/// that meets them: the gate's word, the live look, the evidence check (both indexers absent, aged and young), the
/// retire pass's verdict at 2 h, and the door's terminal judgement (absent aged, mempool aged, mined). A REJECTED whose
/// `extraInfo` names TX_LOCK_TIME (35) is a time, not a verdict: never a refusal, never an eviction, never a 422.
const LOCK_TIME_VECTOR: &str = include_str!("../tests/fixtures/arcade_lock_time_vectors.json");

fn lock_time_replay() -> (Vec<String>, usize) {
    use crate::admit_fast::evidence_verdict;
    use crate::broadcaster::terminal_at_arcade_by_presence;
    use crate::proof_fetcher::{live_arcade_look, retire_verdict, NetworkPresence};
    let v: serde_json::Value = serde_json::from_str(LOCK_TIME_VECTOR).unwrap();
    let mut wrong = Vec::new();
    let mut checks = 0;
    for case in v["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let expect = &case["expect"];
        let status = case["body"]["txStatus"].as_str().unwrap();
        let extra = case["body"]["extraInfo"].as_str().unwrap_or("");
        let mut check = |column: &str, got: String| {
            if let Some(want) = expect.get(column) {
                checks += 1;
                let want = want
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| want.to_string());
                if got != want {
                    wrong.push(format!("{name}: {column} expected {want}, got {got}"));
                }
            }
        };
        check("gate", gate(status).to_string());
        if case["surface"] == "push" {
            // the push is read as `/arc-ingest` reads it: the body, then its word and its extraInfo
            let raw = body(case);
            check(
                "callback",
                match classify_arc_ingest_body(&raw).unwrap() {
                    ArcIngestBody::StatusOnly {
                        tx_status,
                        extra_info,
                        ..
                    } => callback(crate::admit_fast::callback_action_with(
                        &tx_status,
                        extra_info.as_deref().unwrap_or_default(),
                    ))
                    .to_string(),
                    ArcIngestBody::Proof { .. } => "proof_arm".to_string(),
                },
            );
            continue;
        }
        let nf = crate::door_words::lock_time_not_final(status, extra);
        let height = |h: Option<u64>| h.map_or("none".to_string(), |h| h.to_string());
        check(
            "lock_height",
            height(nf.as_ref().and_then(|n| n.lock_height)),
        );
        check(
            "peer_height",
            height(nf.as_ref().and_then(|n| n.peer_height)),
        );
        check(
            "watch",
            match crate::broadcaster::witness_look_of(status.to_string(), extra.to_string(), None) {
                crate::broadcaster::WitnessLook::NotFinal(..) => "not_final".to_string(),
                crate::broadcaster::WitnessLook::Fatal(..) => "fatal".to_string(),
                other => format!("{other:?}"),
            },
        );
        let look = live_arcade_look(status, extra.to_string());
        let kind = format!("{look:?}");
        check(
            "live_look",
            if kind.starts_with("NotFinal") {
                "not_final".into()
            } else if kind.starts_with("Fatal") {
                "fatal".into()
            } else {
                kind.clone()
            },
        );
        let evidence = |young| {
            let e = format!(
                "{:?}",
                evidence_verdict(&look, Some(false), Some(false), young)
            );
            e.split('(').next().unwrap().to_ascii_lowercase()
        };
        check("evidence_absent_aged", evidence(false));
        check("evidence_absent_young", evidence(true));
        check(
            "retire_absent_2h",
            match retire_verdict(&look, Some(false), Some(false), 7_200) {
                Some(_) => "retire".into(),
                None => "keep".into(),
            },
        );
        let door = |presence, young| match terminal_at_arcade_by_presence(
            presence, young, TXID, status, extra,
        ) {
            Ok(crate::broadcaster::ArcOutcome::Accepted(_)) => "accepted".to_string(),
            Ok(crate::broadcaster::ArcOutcome::Rejected(_)) => "rejected".to_string(),
            Ok(other) => format!("{other:?}"),
            Err(_) => "retryable".to_string(),
        };
        check("door_absent_aged", door(NetworkPresence::Absent, false));
        check(
            "door_mempool_aged",
            door(NetworkPresence::Present { height: None }, false),
        );
        check(
            "door_mined",
            door(
                NetworkPresence::Present {
                    height: Some(972_185),
                },
                false,
            ),
        );
    }
    (wrong, checks)
}

#[test]
fn fix836_the_loop44_lock_time_bodies_read_not_now() {
    let (wrong, checks) = lock_time_replay();
    assert_eq!(checks, 33, "the vector's columns, every one read");
    assert!(
        wrong.is_empty(),
        "{} of {checks} loop 44 readings wrong (a non-final word is 'not now', never a refusal):\n  {}",
        wrong.len(),
        wrong.join("\n  ")
    );
}

/// bsv-low #836: the door's judgement and counter, the recorded (pushed) non-final word in the evidence check, and the
/// watch's line naming Arcade's peer's height as a hint beside the word.
#[test]
fn fix836_the_counters_the_recorded_word_and_the_peers_height() {
    use crate::admit_fast::{evidence_verdict, watch_not_final_line, EvidenceVerdict};
    use crate::broadcaster::TerminalJudgement;
    use crate::proof_fetcher::{fold_arcade_look, ArcadeLook, NetworkPresence};
    let v: serde_json::Value = serde_json::from_str(LOCK_TIME_VECTOR).unwrap();
    let extra = v["cases"][0]["body"]["extraInfo"].as_str().unwrap();
    // the door: a non-final word no indexer holds mined is its own judgement, counted apart, a 502 reason of its own
    for presence in [
        NetworkPresence::Absent,
        NetworkPresence::Present { height: None },
        NetworkPresence::Inconclusive,
    ] {
        for (young, capped) in [(false, false), (true, false), (false, true)] {
            let j = TerminalJudgement::of_word(presence, young, capped, "REJECTED", extra);
            assert_eq!(
                j,
                TerminalJudgement::NotFinal,
                "{presence:?} young={young} capped={capped}"
            );
            assert_eq!(j.counter(), "submit_lock_time_not_final_total");
            assert_eq!(
                crate::submit_counters::Reason::terminal(j).slug(),
                "terminal_judged_not_final"
            );
        }
    }
    assert_eq!(
        TerminalJudgement::of_word(
            NetworkPresence::Present { height: Some(1) },
            false,
            false,
            "REJECTED",
            extra
        ),
        TerminalJudgement::Present
    );
    assert_eq!(
        TerminalJudgement::of_word(
            NetworkPresence::Absent,
            false,
            false,
            "REJECTED",
            "TX_INVALID (31)"
        ),
        TerminalJudgement::Absent
    );
    // the recorded word (Arcade faulted; a push or the watch's earlier read filled it): never a refusal either
    let recorded = fold_arcade_look(ArcadeLook::Fault, Some(("REJECTED".into(), extra.into())));
    assert!(matches!(recorded, ArcadeLook::Recorded(..)));
    assert!(matches!(
        evidence_verdict(&recorded, Some(false), Some(false), false),
        EvidenceVerdict::Uncertain(ref why) if why.contains("not final") && why.contains("972172")
    ));
    // an indexer that holds it is still presence
    assert_eq!(
        evidence_verdict(
            &crate::proof_fetcher::live_arcade_look("REJECTED", extra.into()),
            Some(false),
            Some(true),
            false
        ),
        EvidenceVerdict::Present
    );
    // the watch's line: the lock height, the peer's height, no em dash, no already-known token
    let nf = crate::door_words::lock_time_not_final("REJECTED", extra).unwrap();
    let line = watch_not_final_line(TXID, "REJECTED", &nf);
    assert!(
        line.contains("lock height 972185, Arcade's peer at 972172"),
        "{line}"
    );
    assert!(line.contains("held, not final"), "{line}");
    assert!(!line.contains('\u{2014}'), "{line}");
    let answer = crate::broadcaster::lock_time_not_final_answer(TXID, "REJECTED", &nf);
    assert!(
        answer.contains("retryable") && answer.contains("Arcade's peer at 972172"),
        "{answer}"
    );
    assert!(!crate::broadcaster::already_known(&answer), "{answer}");
}
