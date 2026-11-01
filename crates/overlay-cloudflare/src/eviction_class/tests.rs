//! bsv-low #1019's pins: the class of every recorded Arcade body, through each arm's own reason.

use super::*;
use crate::admit_fast::{
    conflict_eviction_reason, evidence_verdict, ladder_rerefusal_reason, EvidenceVerdict,
};
use crate::broadcaster::{classify_submit_response, terminal_refusal_reason, SubmitOutcome};
use crate::door_words::live_look;
use crate::proof_fetcher::{retire_eviction_reason, ArcadeLook};
use overlay_discovery::pot::storage::REFUSAL_CLASSES;

const VECTOR: &str = include_str!("../../../overlay-discovery/vectors/arcade_status_verdicts.json");
const LOCK_TIME: &str = include_str!("../../tests/fixtures/arcade_lock_time_vectors.json");
const TXID: &str = "aa00000000000000000000000000000000000000000000000000000000000001";
const COMPETITOR: &str = "bb00000000000000000000000000000000000000000000000000000000000002";

fn fill(s: &str) -> String {
    s.replace("{TXID}", TXID)
        .replace("{COMPETITOR}", COMPETITOR)
        .replace("{BLOCKHASH}", &"0".repeat(64))
        .replace("{BUMP}", "fed0ac0c000101020304")
        .replace("{NOW}", "2026-10-08T12:00:00Z")
}

fn cell(c: Option<&'static str>) -> &'static str {
    c.unwrap_or("null")
}

/// The evidence check's eviction on this word with both indexers absent: the refusal job's ledger reason
/// (`{status} ({reason})`) and its class; `-` when the check refuses nothing. A non-final word is judged past
/// #836's bound (`door_words::bound_look` turns it `Fatal`), the only way it is ever evicted.
fn evidence_cell(status: &str, extra: &str) -> String {
    let look = match live_look(
        crate::door_words::arcade_hint(status, extra, 0).as_ref(),
        extra.to_string(),
    ) {
        ArcadeLook::NotFinal(w, e) => ArcadeLook::Fatal(w, e),
        other => other,
    };
    match evidence_verdict(&look, Some(false), Some(false), false) {
        EvidenceVerdict::Refused(reason) => {
            cell(eviction_refusal_class(&format!("{status} ({reason})"))).to_string()
        }
        _ => "-".to_string(),
    }
}

/// One line a recorded body: `name \t live \t evidence \t ladder`. `live` is the word and its `extraInfo` through
/// the hint (`refusal_class_of`); `evidence` the evidence check's eviction ([`evidence_cell`]); `ladder` the
/// ladder's re-refusal over the text the ladder carries for that body (a 2xx body through `classify_arcade_status`'s
/// `{txStatus} {extraInfo}`, a sync refusal through `classify_submit_response`; `-` when the ladder refuses nothing).
fn table() -> String {
    let mut out = String::new();
    for (file, raw) in [("verdicts", VECTOR), ("lock_time", LOCK_TIME)] {
        let v: serde_json::Value = serde_json::from_str(raw).unwrap();
        for case in v["cases"].as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            let body = &case["body"];
            let status = body.get("txStatus").and_then(|s| s.as_str());
            let extra = body
                .get("extraInfo")
                .and_then(|s| s.as_str())
                .map(fill)
                .unwrap_or_default();
            let (live, evidence) = match status {
                Some(s) => (
                    cell(refusal_class_of(s, &extra)).to_string(),
                    evidence_cell(s, &extra),
                ),
                None => ("-".to_string(), "-".to_string()),
            };
            let ladder = match (case.get("http").and_then(|h| h.as_u64()), status) {
                (Some(http), _) if http >= 400 => {
                    match classify_submit_response(
                        http as u16,
                        &fill(&serde_json::to_string(body).unwrap()),
                    ) {
                        SubmitOutcome::SyncRejected(r) => {
                            cell(eviction_refusal_class(&ladder_rerefusal_reason(&r))).to_string()
                        }
                        _ => "-".to_string(),
                    }
                }
                (_, Some(s))
                    if crate::door_words::arcade_hint(s, &extra, 0)
                        .is_some_and(|h| crate::door_words::refusal(&h)) =>
                {
                    let text = format!("{s} {extra}");
                    cell(eviction_refusal_class(&ladder_rerefusal_reason(
                        text.trim(),
                    )))
                    .to_string()
                }
                _ => "-".to_string(),
            };
            out.push_str(&format!("{file}/{name}\t{live}\t{evidence}\t{ladder}\n"));
        }
    }
    out
}

const FROZEN: &str = "\
verdicts/submit_202_unknown\tnull\t-\t-\n\
verdicts/submit_202_received\tnull\t-\t-\n\
verdicts/submit_202_sent_to_network\tnull\t-\t-\n\
verdicts/submit_202_accepted_by_network\tnull\t-\t-\n\
verdicts/submit_202_seen_on_network\tnull\t-\t-\n\
verdicts/submit_202_seen_multiple_nodes\tnull\t-\t-\n\
verdicts/submit_202_double_spend_attempted\tdouble-spend\tdouble-spend\tdouble-spend\n\
verdicts/submit_202_rejected\tnull\tnull\tnull\n\
verdicts/submit_202_pending_retry\tnull\t-\t-\n\
verdicts/submit_202_stump_processing\tnull\t-\t-\n\
verdicts/submit_202_mined\tnull\t-\t-\n\
verdicts/submit_202_immutable\tnull\t-\t-\n\
verdicts/submit_202_rejected_lower_case\tnull\tnull\tnull\n\
verdicts/submit_202_orphan_word\tmissing-parent\t-\t-\n\
verdicts/submit_202_mined_in_stale_block\tnull\tabsence\t-\n\
verdicts/submit_202_undefined_word\tnull\tabsence\t-\n\
verdicts/submit_202_empty_word\tnull\tabsence\t-\n\
verdicts/submit_202_no_word\t-\t-\t-\n\
verdicts/submit_400_476_non_final\t-\t-\t-\n\
verdicts/submit_400_465_fee\t-\t-\tpolicy\n\
verdicts/submit_400_461_script\t-\t-\tnull\n\
verdicts/submit_400_467_generic\t-\t-\tnull\n\
verdicts/submit_503_backpressure\t-\t-\t-\n\
verdicts/status_unknown\tnull\t-\t-\n\
verdicts/status_received\tnull\t-\t-\n\
verdicts/status_sent_to_network\tnull\t-\t-\n\
verdicts/status_accepted_by_network\tnull\t-\t-\n\
verdicts/status_seen_on_network\tnull\t-\t-\n\
verdicts/status_seen_multiple_nodes\tnull\t-\t-\n\
verdicts/status_double_spend_attempted\tdouble-spend\tdouble-spend\tdouble-spend\n\
verdicts/status_rejected_466_conflict\tdouble-spend\tdouble-spend\tdouble-spend\n\
verdicts/status_pending_retry\tnull\t-\t-\n\
verdicts/status_stump_processing\tnull\t-\t-\n\
verdicts/status_mined\tnull\t-\t-\n\
verdicts/status_immutable\tnull\t-\t-\n\
verdicts/status_mined_lower_case\tnull\t-\t-\n\
verdicts/status_seen_reorg_unmined\tnull\t-\t-\n\
verdicts/status_undefined_word\tnull\tabsence\t-\n\
verdicts/push_received\tnull\t-\t-\n\
verdicts/push_sent_to_network\tnull\t-\t-\n\
verdicts/push_accepted_by_network\tnull\t-\t-\n\
verdicts/push_unknown\tnull\t-\t-\n\
verdicts/push_pending_retry\tnull\t-\t-\n\
verdicts/push_seen_on_network\tnull\t-\t-\n\
verdicts/push_seen_multiple_nodes\tnull\t-\t-\n\
verdicts/push_stump_processing\tnull\t-\t-\n\
verdicts/push_mined_without_path\tnull\t-\t-\n\
verdicts/push_immutable_without_path\tnull\t-\t-\n\
verdicts/push_mined_with_path\tnull\t-\t-\n\
verdicts/push_immutable_with_path\tnull\t-\t-\n\
verdicts/push_rejected_467\tnull\tnull\tnull\n\
verdicts/push_rejected_466_conflict\tdouble-spend\tdouble-spend\tdouble-spend\n\
verdicts/push_rejected_476_retryable\tnon-final\tnon-final\tnon-final\n\
verdicts/push_double_spend_attempted\tdouble-spend\tdouble-spend\tdouble-spend\n\
verdicts/push_rejected_lower_case\tnull\tnull\tnull\n\
verdicts/push_orphan_word\tmissing-parent\t-\t-\n\
verdicts/push_undefined_word\tnull\tabsence\t-\n\
verdicts/push_reorg_unmined\tnull\t-\t-\n\
verdicts/push_reorg_reanchor\tnull\t-\t-\n\
lock_time/loop44_status_rejected_tx_lock_time\tnon-final\tnon-final\tnon-final\n\
lock_time/loop44_status_rejected_tx_lock_time_bare\tnon-final\tnon-final\tnon-final\n\
lock_time/loop44_push_rejected_tx_lock_time\tnon-final\tnon-final\tnon-final\n\
lock_time/control_status_rejected_script\tnull\tnull\tnull\n\
lock_time/subject_final_by_its_bytes_named_tx_lock_time\tnon-final\tnon-final\tnon-final\n\
";

/// bsv-low #1019 (a): every recorded body of both vectors gets a class or null on each arm, frozen as a table;
/// every cell is a vocabulary word, `null` or `-`.
#[test]
fn fix1019_a_every_recorded_body_has_its_class_by_table() {
    let got = table();
    println!("{got}");
    for line in got.lines() {
        for c in line.split('\t').skip(1) {
            assert!(
                c == "null" || c == "-" || REFUSAL_CLASSES.contains(&c),
                "{line}"
            );
        }
    }
    assert_eq!(got, FROZEN, "the class table moved: re-read each line");
}

/// L55's give-up word (bsv-low #1005, the case the tower's late arm reads): Arcade's REJECTED after 288 retries naming
/// `TX_MISSING_PARENT`, as the overlay logged it.
const L55_GIVE_UP: &str = "no network verdict after 288 durable retry attempts: giving up: a parent this transaction spends has never reached the network; TX_MISSING_PARENT cb846b10";

/// The evidence check's Refused reason on a fatal word (both indexers absent, the arms' shared verdict).
fn fatal(status: &str, extra: &str) -> String {
    match evidence_verdict(
        &ArcadeLook::Fatal(status.into(), extra.into()),
        Some(false),
        Some(false),
        false,
    ) {
        EvidenceVerdict::Refused(r) => r,
        other => panic!("{other:?}"),
    }
}

fn absent(look: ArcadeLook) -> String {
    match evidence_verdict(&look, Some(false), Some(false), false) {
        EvidenceVerdict::Refused(r) => r,
        other => panic!("{other:?}"),
    }
}

/// Each arm's ledger reason, built as the arm builds it, and the class the OPEN marker records for it.
pub(crate) fn arm_reasons() -> Vec<(&'static str, String, Option<&'static str>)> {
    vec![
        // the pending watch: `port.evict(&format!("{status} ({reason})"))`
        ("pending watch, missing parent", format!("REJECTED ({})", fatal("REJECTED", L55_GIVE_UP)), Some("missing-parent")),
        // the callback's refusal job: `format!("{} ({reason})", webhook.0)`
        ("callback, double spend", format!("DOUBLE_SPEND_ATTEMPTED ({})", fatal("DOUBLE_SPEND_ATTEMPTED", "")), Some("double-spend")),
        ("callback, competitor named", format!("REJECTED ({})", fatal("REJECTED", &format!("UTXO_SPENT (70): utxo already spent by tx {COMPETITOR}[0]"))), Some("double-spend")),
        ("callback, non-final past the bound", format!("REJECTED ({})", fatal("REJECTED", "TX_LOCK_TIME (35): lock time (972185) as block height is not less than block height (972172)")), Some("non-final")),
        ("callback, policy", format!("REJECTED ({})", fatal("REJECTED", "TX_INVALID (31): GoBDK fail to ValidateTransaction -> TX_POLICY (39): transaction fee is too low")), Some("policy")),
        ("callback, script failed", format!("REJECTED ({})", fatal("REJECTED", "TX_INVALID (31): script failed")), None),
        // the ladder's re-refusal under an open eviction, over #519's terminal reason and a sync refusal
        ("ladder, terminal missing parent", ladder_rerefusal_reason(&terminal_refusal_reason(TXID, "REJECTED", L55_GIVE_UP)), Some("missing-parent")),
        ("ladder, sync fee", ladder_rerefusal_reason("TX_INVALID (31): insufficient-fee"), Some("policy")),
        ("ladder, nested live word", ladder_rerefusal_reason(&format!("REJECTED ({})", fatal("DOUBLE_SPEND_ATTEMPTED", ""))), Some("double-spend")),
        ("ladder, script failed", ladder_rerefusal_reason("REJECTED TX_INVALID (31): script failed"), None),
        // the conflict pass
        ("conflict pass", conflict_eviction_reason(&"aa".repeat(32), 1, COMPETITOR, 967_673_u64), Some("double-spend")),
        // absence alone and a fault-filled recorded word
        ("absence, arcade missing", format!("SEEN_ON_NETWORK ({})", absent(ArcadeLook::Missing)), Some("absence")),
        ("absence, arcade faulted", format!("REJECTED ({})", absent(ArcadeLook::Fault)), Some("absence")),
        ("absence, recorded word behind a fault", format!("REJECTED ({})", absent(ArcadeLook::Recorded("REJECTED".into(), L55_GIVE_UP.into()))), Some("absence")),
        // the retire pass ends a ghost pot: Arcade's live terminal word keeps its own class however the reason opens
        // (round 1's NOTE-1); the absence arm is absence
        ("retire pass, live terminal arm", retire_eviction_reason(&fatal("REJECTED", L55_GIVE_UP)), Some("missing-parent")),
        ("retire pass, absence arm", retire_eviction_reason(&absent(ArcadeLook::Missing)), Some("absence")),
        // a reason no producer writes
        ("no producer", "fixture".to_string(), None),
    ]
}

/// bsv-low #1019 (b): each eviction arm's own reason reads its class; the chain word beside it is unchanged.
#[test]
fn fix1019_b_each_arm_reason_reads_its_class() {
    let mut wrong = Vec::new();
    for (arm, reason, want) in arm_reasons() {
        let got = eviction_refusal_class(&reason);
        if got != want {
            wrong.push(format!("{arm}: {got:?} for {want:?} ({reason})"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}
