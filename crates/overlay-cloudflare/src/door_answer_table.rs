//! bsv-low #619 (L3): the byte-identity pin over the door's answer table.
//!
//! L3 moves WHO decides the door's reading of Arcade's words (the host's
//! policy over the tracker's `Hint`, `door_words`), never WHAT the door
//! answers. This table is every pure answer the door gives on the recorded
//! inputs: each Arcade word through the gate, the `/arc-ingest` callback, the
//! fatal test, the retire pass's live look and its webhook fold, the proof
//! look, the retire verdict and the refusal's evidence verdict; every captured
//! body of `arcade_status_verdicts.json` through each surface it reaches; the
//! submit HTTP answers and their ladder rung; the #612 stored echo and its
//! freshness under an open eviction; the #519 / #602 presence shapes and the
//! terminal judgement. The fixture `tests/fixtures/door_answer_table.txt` was
//! written on the base (`fe6c83d`) before the change: a changed answer is a
//! red line by name. Regenerate only with a stated reason:
//! `DOOR_ANSWER_TABLE_WRITE=1 cargo test ... door_answer_table`.

use super::*;
use crate::admit_fast::{callback_action, evidence_verdict};
use crate::proof_fetcher::{
    classify_presence, fold_arcade_look, live_arcade_look, parse_arcade_proof_look,
    presence_response, retire_verdict, rfc3339_utc_ms, NetworkPresence,
};
use crate::routes::classify_arc_ingest_body;

const FIXTURE: &str = include_str!("../tests/fixtures/door_answer_table.txt");
const VECTOR: &str = include_str!("../../overlay-discovery/vectors/arcade_status_verdicts.json");
const NOW: &str = "2026-10-08T12:00:00Z";
const STALE: &str = "2026-10-08T10:00:00Z";
const TXID: &str = "aa00000000000000000000000000000000000000000000000000000000000001";
const COMPETITOR: &str = "bb00000000000000000000000000000000000000000000000000000000000002";
const BLOCKHASH: &str = "0000000000000000000000000000000000000000000000000000000000000abc";
const BUMP: &str = "fed0ac0c000101020304";

/// Arcade's twelve words, their spellings, and the words Arcade does not
/// define but ARC or a stranger might send.
const WORDS: &[&str] = &[
    "UNKNOWN",
    "RECEIVED",
    "SENT_TO_NETWORK",
    "ACCEPTED_BY_NETWORK",
    "SEEN_ON_NETWORK",
    "SEEN_MULTIPLE_NODES",
    "DOUBLE_SPEND_ATTEMPTED",
    "REJECTED",
    "PENDING_RETRY",
    "STUMP_PROCESSING",
    "MINED",
    "IMMUTABLE",
    "seen_on_network",
    " MINED\n",
    "rejected",
    "Double_Spend_Attempted",
    " stump_processing ",
    "pending_retry",
    "SEEN_IN_ORPHAN_MEMPOOL",
    "orphaned",
    "ORPHAN",
    "STORED",
    "QUEUED",
    "SUCCESS",
    "ANNOUNCED_TO_NETWORK",
    "REQUESTED_BY_NETWORK",
    "CONFIRMED",
    "MINED_IN_STALE_BLOCK",
    "REORG_UNMINED",
    "INVALID",
    "MALFORMED",
    "UTXO_SPENT",
    "NOT_A_STATUS",
    "",
    "  ",
];

fn fill(s: &str) -> String {
    s.replace("{TXID}", TXID)
        .replace("{COMPETITOR}", COMPETITOR)
        .replace("{BLOCKHASH}", BLOCKHASH)
        .replace("{BUMP}", BUMP)
        .replace("{NOW}", NOW)
}

fn esc(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('\t', "\\t")
}

/// The whole table, one `surface\tinput\tanswer` line per answer.
fn table() -> String {
    let mut out = Vec::<String>::new();
    let mut row = |surface: &str, input: &str, answer: String| {
        out.push(format!("{surface}\t{}\t{}", esc(input), esc(&answer)));
    };
    let now_ms = rfc3339_utc_ms(NOW).unwrap();

    for w in WORDS {
        row("gate", w, format!("{:?}", classify_arcade_status(w)));
        row("callback", w, format!("{:?}", callback_action(w)));
        row("fatal", w, format!("{:?}", is_arcade_fatal(w)));
        let live = live_arcade_look(w, "info".to_string());
        row("live_look", w, format!("{live:?}"));
        row(
            "live_look_fold_plant",
            w,
            format!(
                "{:?}",
                fold_arcade_look(live.clone(), Some(("REJECTED".into(), "planted".into())))
            ),
        );
        for (age, label) in [(7_200, "2h"), (49 * 3_600, "49h")] {
            row(
                &format!("retire_absent_{label}"),
                w,
                format!("{:?}", retire_verdict(&live, Some(false), Some(false), age)),
            );
        }
        for young in [true, false] {
            row(
                &format!("evidence_absent_young_{young}"),
                w,
                format!(
                    "{:?}",
                    evidence_verdict(&live, Some(false), Some(false), young)
                ),
            );
            row(
                &format!("evidence_woc_holds_young_{young}"),
                w,
                format!("{:?}", evidence_verdict(&live, Some(false), Some(true), young)),
            );
            row(
                &format!("evidence_fault_young_{young}"),
                w,
                format!("{:?}", evidence_verdict(&live, None, Some(false), young)),
            );
        }
        for (shape, body) in [
            (
                "path_fresh",
                serde_json::json!({"txid": TXID, "txStatus": w, "timestamp": NOW, "merklePath": BUMP}),
            ),
            (
                "nopath_fresh",
                serde_json::json!({"txid": TXID, "txStatus": w, "timestamp": NOW}),
            ),
            (
                "nopath_stale",
                serde_json::json!({"txid": TXID, "txStatus": w, "timestamp": STALE}),
            ),
            (
                "nopath_nostamp",
                serde_json::json!({"txid": TXID, "txStatus": w}),
            ),
        ] {
            row(
                &format!("proof_look_{shape}"),
                w,
                format!("{:?}", parse_arcade_proof_look(200, &body.to_string(), now_ms)),
            );
        }
        let push = serde_json::json!({"txid": TXID, "txStatus": w}).to_string();
        row(
            "arc_ingest_body",
            w,
            format!("{:?}", classify_arc_ingest_body(&push)),
        );
    }

    // every captured body of the vector, through each surface it reaches
    let v: serde_json::Value = serde_json::from_str(VECTOR).unwrap();
    for case in v["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let raw = fill(&serde_json::to_string(&case["body"]).unwrap());
        if let Some(http) = case["http"].as_u64() {
            let outcome = classify_submit_response(http as u16, &raw);
            row("vector_submit", name, format!("{outcome:?}"));
            match submit_entry(outcome) {
                SubmitEntry::Step(step) => {
                    row("vector_submit_ladder", name, format!("{:?}", ladder_step(step, TXID)))
                }
                other => row("vector_submit_entry", name, format!("{other:?}")),
            }
        }
        row(
            "vector_proof_look",
            name,
            format!("{:?}", parse_arcade_proof_look(200, &raw, now_ms)),
        );
        row(
            "vector_arc_ingest_body",
            name,
            format!("{:?}", classify_arc_ingest_body(&raw)),
        );
        if let Some(word) = case["body"]["txStatus"].as_str() {
            row("vector_gate", name, format!("{:?}", classify_arcade_status(word)));
            row("vector_callback", name, format!("{:?}", callback_action(word)));
        }
    }

    // the submit HTTP answers outside the vector (#213, #228 arcade#260)
    for (http, body) in [
        (200, r#"{"txid":"T","txStatus":"SEEN_ON_NETWORK"}"#),
        (202, r#"{"txid":"T","txStatus":"RECEIVED"}"#),
        (
            400,
            r#"{"error":"transaction failed validation","reason":"script"}"#,
        ),
        (
            400,
            r#"{"error":"transaction failed validation","reason":"x","status":466}"#,
        ),
        (
            400,
            r#"{"error":"transaction failed validation","reason":"x","status":467}"#,
        ),
        (
            400,
            r#"{"error":"transaction failed validation","reason":"x","status":476}"#,
        ),
        (400, r#"{"error":"other","reason":"x","status":499}"#),
        (400, r#"{"error":"other"}"#),
        (400, "not json"),
        (401, ""),
        (404, "nope"),
        (429, "slow down"),
        (500, "boom"),
        (503, ""),
    ] {
        let outcome = classify_submit_response(http, body);
        let input = format!("{http} {body}");
        row("submit", &input, format!("{outcome:?}"));
        if let SubmitEntry::Step(step) = submit_entry(outcome) {
            row("submit_ladder", &input, format!("{:?}", ladder_step(step, TXID)));
        }
    }
    for step in [
        GateStep::Accepted,
        GateStep::SyncRejected("r".into()),
        GateStep::AsyncRejected("r".into()),
        GateStep::AsyncRejected(format!(
            "{ARCADE_CASCADE_PREFIX} {ARCADE_CASCADE_RETRYABLE_HINT}"
        )),
        GateStep::Orphan("o".into()),
        GateStep::AcceptedUnseen,
        GateStep::SyncAccepted,
    ] {
        let input = format!("{step:?}");
        row("ladder", &input, format!("{:?}", ladder_step(step, TXID)));
    }

    // #612: the stored echo, and its freshness under an open eviction
    for fast in [true, false] {
        row(
            "stored_echo",
            &format!("fast_answer={fast}"),
            format!("{:?}", reached_submit_echo(fast)),
        );
    }
    let evicted = now_ms - 10 * 60_000;
    for (label, stamp) in [
        ("none", None),
        ("after_eviction", Some("2026-10-08T11:55:00Z")),
        ("before_eviction", Some("2026-10-08T11:45:00Z")),
        ("older_than_30m", Some("2026-10-08T11:20:00Z")),
        ("future", Some("2026-10-08T12:05:00Z")),
        ("garbage", Some("yesterday")),
    ] {
        for (ev_label, ev) in [("no_eviction", None), ("open_eviction", Some(evicted))] {
            row(
                "stored_echo_fresh",
                &format!("{label} {ev_label}"),
                format!("{:?}", submit_echo_is_fresh(stamp, now_ms, ev)),
            );
        }
    }

    // #519 / #602: the couriers' presence shapes and the terminal judgement
    let presences = [
        None,
        Some(NetworkPresence::Absent),
        Some(NetworkPresence::Inconclusive),
        Some(NetworkPresence::Present { height: None }),
        Some(NetworkPresence::Present {
            height: Some(970_710),
        }),
    ];
    for b in presences {
        for w in presences {
            row(
                "presence",
                &format!("bitails={b:?} woc={w:?}"),
                format!("{:?}", classify_presence(b, w)),
            );
        }
    }
    for (http, body) in [
        (200, r#"{"blockheight":970710}"#),
        (200, r#"{"blockHeight":970710}"#),
        (200, r#"{"txid":"t","vin":[],"vout":[]}"#),
        (200, r#"{"confirmations":0}"#),
        (200, "not json"),
        (404, ""),
        (429, ""),
        (500, ""),
    ] {
        row(
            "presence_response",
            &format!("{http} {body}"),
            format!("{:?}", presence_response(http, body)),
        );
    }
    for p in presences.into_iter().flatten() {
        for young in [true, false] {
            for (status, extra) in [
                ("REJECTED", ""),
                ("REJECTED", "already known, seen and mined"),
                (
                    "DOUBLE_SPEND_ATTEMPTED",
                    "UTXO_SPENT: input already spent by competitor",
                ),
            ] {
                row(
                    "terminal_by_presence",
                    &format!("{p:?} young={young} {status} {extra}"),
                    format!(
                        "{:?}",
                        terminal_at_arcade_by_presence(p, young, TXID, status, extra, &crate::door_words::LockBound::default())
                    ),
                );
            }
        }
    }
    let mut s = out.join("\n");
    s.push('\n');
    s
}

/// bsv-low #624 (L8): the rows whose answer moved when Arcade's words became
/// bsv-tracker's reading, recorded line then the answer now. Three inputs
/// only, none a word Arcade sends: ` MINED\n` (a control character, which
/// the tracker's metadata bound refuses) and `orphaned` / `ORPHAN` (outside
/// every broadcaster's vocabulary; the orphan view is ARC's
/// SEEN_IN_ORPHAN_MEMPOOL, unchanged). Each is now no input: never admits,
/// never refuses (D1), and a live look of it is a fault-shaped look whose
/// refusal needs the two-source absence bar and the age floor (#513, #740).
/// `docs/words/L8-ARCADE-VERDICT-TABLE.md`, the vocabulary rows. The fixture
/// stays the base's record; only these lines are read through this list.
const L8_MOVED: &[(&str, &str)] = &[
    (
        "gate\t MINED\\n\tReached",
        "gate\t MINED\\n\tPending",
    ),
    (
        "callback\t MINED\\n\tLatchSeen",
        "callback\t MINED\\n\tIgnore",
    ),
    (
        "live_look\t MINED\\n\tPresent",
        "live_look\t MINED\\n\tFault",
    ),
    (
        "live_look_fold_plant\t MINED\\n\tPresent",
        "live_look_fold_plant\t MINED\\n\tRecorded(\"REJECTED\", \"planted\")",
    ),
    (
        "evidence_absent_young_true\t MINED\\n\tPresent",
        "evidence_absent_young_true\t MINED\\n\tUncertain(\"a young or unknown admission: an absence-based refusal needs Arcade's own word or an older admission\")",
    ),
    (
        "evidence_fault_young_true\t MINED\\n\tPresent",
        "evidence_fault_young_true\t MINED\\n\tUncertain(\"a courier faulted\")",
    ),
    (
        "evidence_absent_young_false\t MINED\\n\tPresent",
        "evidence_absent_young_false\t MINED\\n\tRefused(\"arcade faulted, both indexers absent\")",
    ),
    (
        "evidence_fault_young_false\t MINED\\n\tPresent",
        "evidence_fault_young_false\t MINED\\n\tUncertain(\"a courier faulted\")",
    ),
    (
        "proof_look_path_fresh\t MINED\\n\tMined(\"fed0ac0c000101020304\")",
        "proof_look_path_fresh\t MINED\\n\tUnknown",
    ),
    (
        "gate\torphaned\tOrphan",
        "gate\torphaned\tPending",
    ),
    (
        "callback\torphaned\tEvidenceCheck(\"ORPHANED\")",
        "callback\torphaned\tIgnore",
    ),
    (
        "live_look\torphaned\tPresent",
        "live_look\torphaned\tFault",
    ),
    (
        "live_look_fold_plant\torphaned\tPresent",
        "live_look_fold_plant\torphaned\tRecorded(\"REJECTED\", \"planted\")",
    ),
    (
        "evidence_absent_young_true\torphaned\tPresent",
        "evidence_absent_young_true\torphaned\tUncertain(\"a young or unknown admission: an absence-based refusal needs Arcade's own word or an older admission\")",
    ),
    (
        "evidence_fault_young_true\torphaned\tPresent",
        "evidence_fault_young_true\torphaned\tUncertain(\"a courier faulted\")",
    ),
    (
        "evidence_absent_young_false\torphaned\tPresent",
        "evidence_absent_young_false\torphaned\tRefused(\"arcade faulted, both indexers absent\")",
    ),
    (
        "evidence_fault_young_false\torphaned\tPresent",
        "evidence_fault_young_false\torphaned\tUncertain(\"a courier faulted\")",
    ),
    (
        "gate\tORPHAN\tOrphan",
        "gate\tORPHAN\tPending",
    ),
    (
        "callback\tORPHAN\tEvidenceCheck(\"ORPHAN\")",
        "callback\tORPHAN\tIgnore",
    ),
    (
        "live_look\tORPHAN\tPresent",
        "live_look\tORPHAN\tFault",
    ),
    (
        "live_look_fold_plant\tORPHAN\tPresent",
        "live_look_fold_plant\tORPHAN\tRecorded(\"REJECTED\", \"planted\")",
    ),
    (
        "evidence_absent_young_true\tORPHAN\tPresent",
        "evidence_absent_young_true\tORPHAN\tUncertain(\"a young or unknown admission: an absence-based refusal needs Arcade's own word or an older admission\")",
    ),
    (
        "evidence_fault_young_true\tORPHAN\tPresent",
        "evidence_fault_young_true\tORPHAN\tUncertain(\"a courier faulted\")",
    ),
    (
        "evidence_absent_young_false\tORPHAN\tPresent",
        "evidence_absent_young_false\tORPHAN\tRefused(\"arcade faulted, both indexers absent\")",
    ),
    (
        "evidence_fault_young_false\tORPHAN\tPresent",
        "evidence_fault_young_false\tORPHAN\tUncertain(\"a courier faulted\")",
    ),
];

/// bsv-low #845 (bsv-wallet-toolbox-rs 0.11.2's Arcade status vectors, `toolbox_status` `unknown` for the three):
/// Arcade's RECEIVED, SENT_TO_NETWORK and ACCEPTED_BY_NETWORK say Arcade holds it, never that the network does.
/// A hint that promotes nothing: the live look is `Holding`, never `Present`; the evidence check reads the
/// indexers and, absent them, answers `Uncertain` (never presence, never a refusal); the retire pass takes its
/// absence arm (48 h, the memo `retired`), so a pot no broadcaster ever saw SEEN is not kept live by Arcade's rank.
/// Seven recorded lines per word move; every other line of the three words is unchanged.
pub(crate) const FIX845_HOLDING_WORDS: [&str; 3] =
    ["RECEIVED", "SENT_TO_NETWORK", "ACCEPTED_BY_NETWORK"];

/// bsv-low #836 fold (the lens's L3): UNKNOWN and PENDING_RETRY carry no network verdict (the toolbox 0.11.2
/// vector's `toolbox_status: unknown` for both) and read `Holding` too: the same seven lines move for each input the
/// table spells them by (`pending_retry` in lower case among them), the word upper case in the answer.
pub(crate) const FIX836_L3_INPUTS: [&str; 3] = ["UNKNOWN", "PENDING_RETRY", "pending_retry"];

fn fix845_moved() -> Vec<(String, String)> {
    let holding = crate::admit_fast::HOLDING_UNCERTAIN_REASON;
    let mut moved = Vec::new();
    for input in FIX845_HOLDING_WORDS.into_iter().chain(FIX836_L3_INPUTS) {
        let w = input.to_ascii_uppercase();
        let i = input;
        moved.push((
            format!("live_look\t{i}\tPresent"),
            format!("live_look\t{i}\tHolding(\"{w}\")"),
        ));
        moved.push((
            format!("live_look_fold_plant\t{i}\tPresent"),
            format!("live_look_fold_plant\t{i}\tHolding(\"{w}\")"),
        ));
        moved.push((
            format!("retire_absent_49h\t{i}\tNone"),
            format!(
                "retire_absent_49h\t{i}\tSome(\"network-absent 176400s: arcade holds it below SEEN ({w}, a hint) + both indexers 404\")"
            ),
        ));
        for young in [true, false] {
            moved.push((
                format!("evidence_absent_young_{young}\t{i}\tPresent"),
                format!("evidence_absent_young_{young}\t{i}\tUncertain(\"{holding}\")"),
            ));
            moved.push((
                format!("evidence_fault_young_{young}\t{i}\tPresent"),
                format!("evidence_fault_young_{young}\t{i}\tUncertain(\"a courier faulted\")"),
            ));
        }
    }
    moved
}

#[test]
fn the_doors_answers_are_the_recorded_table_save_where_the_tracker_refuses_the_word() {
    let got = table();
    if std::env::var_os("DOOR_ANSWER_TABLE_WRITE").is_some() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/door_answer_table.txt"
        );
        std::fs::write(path, &got).unwrap();
        return;
    }
    let fix845 = fix845_moved();
    let want: Vec<&str> = FIXTURE
        .lines()
        .map(|line| {
            L8_MOVED
                .iter()
                .find(|(recorded, _)| *recorded == line)
                .map(|(_, now)| *now)
                .or_else(|| {
                    fix845
                        .iter()
                        .find(|(recorded, _)| recorded == line)
                        .map(|(_, now)| now.as_str())
                })
                .unwrap_or(line)
        })
        .collect();
    assert_eq!(
        L8_MOVED
            .iter()
            .filter(|(recorded, _)| FIXTURE.lines().any(|l| l == *recorded))
            .count(),
        L8_MOVED.len(),
        "every L8 row names a recorded line"
    );
    assert_eq!(
        fix845
            .iter()
            .filter(|(recorded, _)| FIXTURE.lines().any(|l| l == recorded))
            .count(),
        42,
        "every #845 row names a recorded line: seven for each of RECEIVED, SENT_TO_NETWORK, ACCEPTED_BY_NETWORK, and (the #836 fold's L3) for UNKNOWN, PENDING_RETRY and pending_retry"
    );
    let have: Vec<&str> = got.lines().collect();
    let changed: Vec<String> = want
        .iter()
        .zip(have.iter())
        .filter(|(w, h)| w != h)
        .map(|(w, h)| format!("recorded: {w}\n     now: {h}"))
        .collect();
    assert!(
        changed.is_empty() && want.len() == have.len(),
        "the door's answer changed ({} lines recorded, {} now; the moved rows are L8's and #845's: RECEIVED, SENT_TO_NETWORK and ACCEPTED_BY_NETWORK are Holding, never Present, and the #836 fold's L3: UNKNOWN and PENDING_RETRY too):\n{}",
        want.len(),
        have.len(),
        changed.join("\n")
    );
}
