//! The judge's selection and hydration of one identity's chosen games (bsv-low #638's ranked view, kept by #893
//! section 6.2 when the walk's createdAt cursor over the three sources retired): the same representative, quota and
//! dense ranks as `results_sql` / `refund_view_sql` and the hops window, and the views' own outer projections over
//! the chosen rows. BLOB hydration happens only for selected pots.
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::collections::BTreeMap;

/// The hydration page: the chosen rows bound as one JSON array of at most this many.
pub const PAGE: usize = 32;

pub fn number(v: &Value, key: &str) -> Option<i64> {
    v[key]
        .as_i64()
        .or_else(|| v[key].as_f64().map(|n| n as i64))
}
fn rank(v: &Value) -> i64 {
    match number(v, "sigValid") {
        Some(1) => 2,
        None => 1,
        _ => 0,
    }
}
fn stamp(v: &Value) -> Option<i64> {
    number(v, "potCreatedAt").or_else(|| number(v, "createdAt"))
}

/// THE ERA'S ONE RULE (#375): a candidate whose stamp (its pot's `createdAt`, else its own, in seconds) is before the
/// cutoff, or that has none, is written off. Every road into the judgement reads it: the parties' representatives
/// ([`select`]), the hop markers ([`select_hops`]) and, since D2-ACTOR-B-FOLD-11 (bsv-low #957), the released-hop
/// evidence and its could-not-judge rows (`hydrate`).
pub fn before_era(v: &Value, era_ms: Option<i64>) -> bool {
    era_ms.is_some_and(|era| stamp(v).is_none_or(|s| s.saturating_mul(1000) < era))
}
fn recency(a: &Value, b: &Value) -> Ordering {
    rank(b)
        .cmp(&rank(a))
        .then_with(|| stamp(b).cmp(&stamp(a)))
        .then_with(|| number(b, "createdAt").cmp(&number(a, "createdAt")))
        .then_with(|| number(b, "markerRowid").cmp(&number(a, "markerRowid")))
}
fn order(a: &Value, b: &Value) -> Ordering {
    rank(b)
        .cmp(&rank(a))
        .then_with(|| number(a, "tier").cmp(&number(b, "tier")))
        .then_with(|| stamp(b).cmp(&stamp(a)))
        .then_with(|| number(b, "createdAt").cmp(&number(a, "createdAt")))
        .then_with(|| number(b, "markerRowid").cmp(&number(a, "markerRowid")))
}

/// Same representative, quota and dense ranks as results_sql/refund_view_sql.
/// Era filtering precedes collapse. Validity is a sort key, never a filter.
pub fn select(candidates: Vec<Value>, era_ms: Option<i64>, quota: usize) -> Vec<Value> {
    let mut pots: BTreeMap<(String, i64), Value> = BTreeMap::new();
    for v in candidates {
        if before_era(&v, era_ms) {
            continue;
        }
        let key = (
            v["potTxid"].as_str().unwrap().to_string(),
            number(&v, "potVout").unwrap(),
        );
        let replace = pots.get(&key).is_none_or(|old| {
            rank(&v) > rank(old)
                || (rank(&v) == rank(old)
                    && (number(&v, "createdAt"), number(&v, "markerRowid"))
                        < (number(old, "createdAt"), number(old, "markerRowid")))
        });
        if replace {
            pots.insert(key, v);
        }
    }
    let mut rows: Vec<Value> = pots.into_values().collect();
    rows.sort_by(recency);
    let mut unknown = 0;
    for v in &mut rows {
        let tier = if number(v, "unknownPot") == Some(1) {
            unknown += 1;
            i64::from(unknown > quota)
        } else {
            0
        };
        v["tier"] = json!(tier);
        v["potBestSigRank"] = json!(rank(v));
    }
    rows.sort_by(order);
    let mut final_rank = 0;
    for i in 0..rows.len() {
        if i == 0 || order(&rows[i - 1], &rows[i]) != Ordering::Equal {
            final_rank += 1;
        }
        rows[i]["finalRank"] = json!(final_rank);
    }
    rows
}

/// Keep the production outer projection and its proof/BEEF joins; replace only the ranked spine.
/// JSON carries the chosen representative and global tier, so page-local quotas cannot change selection.
pub fn hydrate_sql(original: &str) -> String {
    let prefix = original.split_once("FROM (SELECT").expect("view spine").0;
    let suffix = original
        .split_once(") w LEFT JOIN")
        .expect("view outer joins")
        .1;
    let columns = [
        "identity",
        "opponentIdentity",
        "gameId",
        "potTxid",
        "potVout",
        "recoveryHeight",
        "seatSettlePubkey",
        "seatSigHex",
        "sigHex",
        "createdAt",
        "markerRowid",
        "tier",
        "potBestSigRank",
        "finalRank",
    ];
    let fields = columns
        .iter()
        .map(|k| format!("json_extract(value,'$.{k}') AS {k}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("{prefix}FROM (SELECT pp.*, pp.createdAt AS markerCreatedAt, r.createdAt AS potCreatedAt, r.spent, r.spendingTxid, r.spentConfirmed, r.lockKind, r.pubA, r.pubB, r.pubTower, r.payPkhA, r.payPkhB, r.rakePkh, r.stakeA, r.stakeB, r.feeSats, r.recoveryHeight AS covRecoveryHeight, r.potSats, r.verdict, r.verdictTxid, r.spentHeight, r.spenderFinal, r.firstSpentAt FROM (SELECT {fields} FROM json_each(?1)) pp LEFT JOIN pot_records r ON r.txid=pp.potTxid AND r.outputIndex=pp.potVout) w LEFT JOIN{suffix}")
}

fn hop_rank(v: &Value) -> i64 {
    match number(v, "markerValid") {
        Some(1) => 2,
        None => 1,
        _ => 0,
    }
}
fn hop_key(v: &Value) -> (String, i64) {
    (
        v["hopTxid"].as_str().unwrap().to_string(),
        number(v, "hopVout").unwrap(),
    )
}
fn hop_order(a: &Value, b: &Value, per_row: bool) -> Ordering {
    number(b, "outpointMarkerRank")
        .cmp(&number(a, "outpointMarkerRank"))
        .then_with(|| {
            number(
                a,
                if per_row {
                    "paidTier"
                } else {
                    "outpointPaidTier"
                },
            )
            .cmp(&number(
                b,
                if per_row {
                    "paidTier"
                } else {
                    "outpointPaidTier"
                },
            ))
        })
        .then_with(|| number(a, "tier").cmp(&number(b, "tier")))
        .then_with(|| number(b, "hopSatsOnChain").cmp(&number(a, "hopSatsOnChain")))
        .then_with(|| {
            number(a, "potCreatedAt")
                .or_else(|| number(a, "firstMarkerAt"))
                .cmp(&number(b, "potCreatedAt").or_else(|| number(b, "firstMarkerAt")))
        })
        .then_with(|| hop_key(a).cmp(&hop_key(b)))
}
/// Mirror the unscoped hop window, including oldest-row cap and dense-rank ties.
pub fn select_hops(candidates: Vec<Value>, era_ms: Option<i64>, now_seconds: i64) -> Vec<Value> {
    use crate::hops_view::{
        HOPS_VIEW_ROWS_PER_OUTPOINT, HOPS_VIEW_UNKNOWN_HOP_MAX_AGE_SECS,
        HOPS_VIEW_UNKNOWN_HOP_QUOTA,
    };
    let mut groups: BTreeMap<(String, i64), Vec<Value>> = BTreeMap::new();
    for v in candidates {
        if before_era(&v, era_ms) {
            continue;
        }
        groups.entry(hop_key(&v)).or_default().push(v);
    }
    let mut rows = Vec::new();
    for mut group in groups.into_values() {
        group.sort_by(|a, b| {
            number(a, "createdAt")
                .cmp(&number(b, "createdAt"))
                .then_with(|| number(a, "markerRowid").cmp(&number(b, "markerRowid")))
        });
        let first = group.iter().filter_map(|v| number(v, "createdAt")).min();
        group.truncate(HOPS_VIEW_ROWS_PER_OUTPOINT);
        let rank = group.iter().map(hop_rank).max().unwrap();
        let paid = group
            .iter()
            .filter_map(|v| number(v, "paidTier"))
            .min()
            .unwrap();
        for mut v in group {
            v["firstMarkerAt"] = json!(first);
            v["markerRank"] = json!(hop_rank(&v));
            v["outpointMarkerRank"] = json!(rank);
            v["outpointPaidTier"] = json!(paid);
            v["freshUnknown"] = json!(i64::from(
                number(&v, "unknownHop") == Some(1)
                    && first.unwrap_or(0)
                        >= now_seconds - HOPS_VIEW_UNKNOWN_HOP_MAX_AGE_SECS as i64
            ));
            rows.push(v);
        }
    }
    rows.sort_by(|a, b| {
        number(a, "unknownHop")
            .cmp(&number(b, "unknownHop"))
            .then_with(|| number(a, "freshUnknown").cmp(&number(b, "freshUnknown")))
            .then_with(|| {
                number(a, "firstMarkerAt")
                    .unwrap_or(0)
                    .cmp(&number(b, "firstMarkerAt").unwrap_or(0))
            })
            .then_with(|| hop_key(a).cmp(&hop_key(b)))
    });
    let mut previous = None;
    let mut hop_rank = 0;
    let mut partition = None;
    for v in &mut rows {
        let part = (number(v, "unknownHop"), number(v, "freshUnknown"));
        if partition != Some(part) {
            hop_rank = 0;
            previous = None;
            partition = Some(part);
        }
        let key = (number(v, "firstMarkerAt").unwrap_or(0), hop_key(v));
        if previous.as_ref() != Some(&key) {
            hop_rank += 1;
            previous = Some(key);
        }
        v["tier"] = json!(if number(v, "unknownHop") == Some(0)
            || (number(v, "freshUnknown") == Some(1) && hop_rank <= HOPS_VIEW_UNKNOWN_HOP_QUOTA)
        {
            0
        } else {
            1
        });
    }
    rows.sort_by(|a, b| hop_order(a, b, false));
    let mut rank = 0;
    for i in 0..rows.len() {
        if i == 0 || hop_order(&rows[i - 1], &rows[i], false) != Ordering::Equal {
            rank += 1;
        }
        rows[i]["finalRank"] = json!(rank);
    }
    rows.sort_by(|a, b| {
        hop_order(a, b, true)
            .then_with(|| number(b, "markerRank").cmp(&number(a, "markerRank")))
            .then_with(|| number(a, "createdAt").cmp(&number(b, "createdAt")))
            .then_with(|| number(a, "markerRowid").cmp(&number(b, "markerRowid")))
    });
    rows
}

pub fn hydrate_hops_sql() -> String {
    let original = crate::hops_view::hops_view_sql(false, None, 0);
    let prefix = original.split_once("FROM (SELECT").unwrap().0;
    let suffix = original.split_once(") w LEFT JOIN").unwrap().1;
    let columns = [
        "identity",
        "gameId",
        "hopTxid",
        "hopVout",
        "hopSats",
        "opponentIdentity",
        "seatSettlePubkey",
        "seatSigHex",
        "identitySigHex",
        "markerTxid",
        "markerVout",
        "hopLockHex",
        "hopSatsOnChain",
        "containerOutputs",
        "markerValid",
        "spent",
        "spendingTxid",
        "spentConfirmed",
        "spenderFinal",
        "markerCreatedAt",
        "markerRowid",
        "potCreatedAt",
        "firstMarkerAt",
        "markerRank",
        "outpointMarkerRank",
        "paidTier",
        "tier",
    ];
    let fields = columns
        .iter()
        .map(|k| format!("json_extract(value,'$.{k}') AS {k}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("{prefix}FROM (SELECT {fields} FROM json_each(?1)) w LEFT JOIN{suffix}")
}

/// Results and refund projections share one bounded spine read, with separate
/// proof joins so each production mapper still receives its original proof flag.
///
/// bsv-low #831: `fundingHeld` says whether an UNSPENT pot's own funding is held: `seen` (the `network_seen` latch on
/// the pot txid), else `proof` (a verified index proof, `pot_beefs.proof_verified`), else `none`; NULL for a spent
/// pot. Each probe is a primary-key read inside this statement, made only for an unspent pot (the CASE gates it) and
/// the proof probe only when the latch is absent: no new statement, no scan.
pub const FUNDING_HELD_COLUMN: &str = "CASE WHEN w.spent = 0 THEN CASE WHEN EXISTS(SELECT 1 FROM network_seen fs WHERE fs.txid=lower(w.potTxid)) THEN 'seen' WHEN EXISTS(SELECT 1 FROM pot_beefs fb WHERE fb.txid=lower(w.potTxid) AND fb.proof_verified = 1) THEN 'proof' ELSE 'none' END END AS fundingHeld";

pub fn hydrate_party_pair_sql() -> String {
    let sql = hydrate_sql(&crate::results::results_sql(None, 0));
    sql.replacen("FROM (SELECT",&format!(", w.potCreatedAt AS potAdmittedAt, w.markerCreatedAt AS firstPartyMarkerAt, w.firstSpentAt AS firstSpentAt, EXISTS(SELECT 1 FROM potrefund_records pr WHERE pr.potTxid=w.potTxid AND pr.potVout=w.potVout) AS backupMarkerPresent, refund_sb.proof_verified AS refundSpenderProofVerified, {FUNDING_HELD_COLUMN} FROM (SELECT"),1)
        .replace("LEFT JOIN network_seen ns", "LEFT JOIN pot_beefs refund_sb ON w.spendingTxid IS NOT NULL AND refund_sb.txid=lower(w.spendingTxid) LEFT JOIN network_seen ns")
}
