//! #638: walk candidate metadata once by createdAt, then preserve the view's ranking.
//! Each source page is bounded before any join. BLOB hydration happens only for selected pots.
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::collections::BTreeMap;

pub const PAGE: usize = 32;
pub const PAGE_READ_CEILING: usize = 287;

#[derive(Clone, Copy, Debug)]
pub enum Source {
    Party,
    Link,
    Hop,
}

/// NULL stamps form a separate indexed prefix, so no sentinel can skip a legacy row.
/// Non-NULL pages seek by (createdAt,rowid); rowid breaks equal-stamp ties.
pub fn candidates_sql(source: Source, nulls: bool, seek: bool, next_stamp: bool) -> String {
    let (table, index) = match source {
        Source::Party => ("potparty_records", "idx_potparty_identity_created"),
        Source::Link => ("hop_pot_links", "idx_hop_pot_links_identity_created"),
        Source::Hop => ("hopparty_records", "idx_hopparty_identity_created"),
    };
    let predicate = match (nulls, seek) {
        (true, false) => "1=1",
        (true, true) => "createdAt IS NULL AND rowid > ?2",
        (false, false) => "createdAt IS NOT NULL",
        (false, true) if next_stamp => "createdAt > ?2",
        (false, true) => "createdAt = ?2 AND rowid > ?3",
    };
    let source_sql = format!("(SELECT *, rowid AS cursorRowid FROM {table} INDEXED BY {index} WHERE identity=?1 AND {predicate} ORDER BY createdAt,rowid LIMIT {PAGE})");
    if matches!(source, Source::Hop) {
        return format!("SELECT page_row.*, page_row.txid AS hopTxid, page_row.hopVout, page_row.txid AS markerTxid, page_row.outputIndex AS markerVout, page_row.createdAt AS markerCreatedAt, page_row.cursorRowid AS markerRowid, r.createdAt AS potCreatedAt, r.txid IS NULL AS unknownHop, r.spent, r.spendingTxid, r.spentConfirmed, r.spenderFinal, CASE WHEN page_row.hopLockHex IS NOT NULL AND page_row.hopSatsOnChain=page_row.hopSats THEN 0 ELSE 1 END AS paidTier FROM {source_sql} page_row LEFT JOIN pot_records r ON r.txid=page_row.txid AND r.outputIndex=page_row.hopVout ORDER BY page_row.createdAt,page_row.cursorRowid");
    }
    let party = match source {
        Source::Party => format!("SELECT s.*, s.cursorRowid AS markerRowid FROM {source_sql} s"),
        Source::Link => format!("SELECT hp.identity, hp.opponentIdentity, hp.gameId, s.potTxid, s.potVout, pot.recoveryHeight, hp.seatSettlePubkey, hp.seatSigHex, hp.identitySigHex AS sigHex, hp.markerValid AS sigValid, hp.createdAt, hp.rowid AS markerRowid, s.cursorRowid FROM {source_sql} s CROSS JOIN hopparty_records hp ON hp.rowid=s.markerRowid CROSS JOIN pot_records pot ON pot.txid=s.potTxid AND pot.outputIndex=s.potVout"),
        Source::Hop => unreachable!(),
    };
    format!("SELECT pp.*, r.createdAt AS potCreatedAt, r.txid IS NULL AS unknownPot FROM ({party}) pp LEFT JOIN pot_records r ON r.txid=pp.potTxid AND r.outputIndex=pp.potVout ORDER BY pp.createdAt,pp.cursorRowid")
}

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
        if era_ms.is_some_and(|era| stamp(&v).is_none_or(|s| s.saturating_mul(1000) < era)) {
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
        if era_ms.is_some_and(|era| stamp(&v).is_none_or(|s| s.saturating_mul(1000) < era)) {
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

/// Preserve the original rank windows and their probe/drop behavior at the walk cap,
/// including dense-rank ties. A createdAt cursor changes retrieval, never selection.
pub fn party_walk(rows: &[Value], pages: usize) -> (Vec<Vec<Value>>, bool) {
    let cap = crate::results::RESULTS_MAX_ROWS;
    let mut out = Vec::new();
    let mut more = false;
    for page in 0..pages {
        let after = page * cap;
        let mut selected: Vec<_> = rows
            .iter()
            .filter(|v| {
                let rank = number(v, "finalRank").unwrap() as usize;
                rank > after && rank <= after + cap + 1
            })
            .take(cap + 1)
            .cloned()
            .collect();
        more = selected.len() > cap;
        selected.truncate(cap);
        out.push(selected);
        if !more {
            break;
        }
    }
    (out, more)
}

pub fn hop_walk(rows: &[Value], pages: usize) -> (Vec<Vec<Value>>, bool) {
    let cap = crate::hops_view::HOPS_VIEW_MAX_OUTPOINTS;
    let row_cap = (cap + 1) * crate::hops_view::HOPS_VIEW_ROWS_PER_OUTPOINT;
    let mut out = Vec::new();
    let mut more = false;
    for page in 0..pages {
        let after = page * cap;
        let selected: Vec<_> = rows
            .iter()
            .filter(|v| {
                let rank = number(v, "finalRank").unwrap() as usize;
                rank > after && rank <= after + cap + 1
            })
            .take(row_cap)
            .cloned()
            .collect();
        let mut keys = Vec::new();
        for v in &selected {
            let key = hop_key(v);
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
        more = keys.len() > cap;
        keys.truncate(cap);
        out.push(
            selected
                .into_iter()
                .filter(|v| keys.contains(&hop_key(v)))
                .collect(),
        );
        if !more {
            break;
        }
    }
    (out, more)
}

/// SQLite versions that cannot seek the implicit rowid in a tuple range still
/// seek it with an equality on createdAt. Exhaust that stamp, then seek the next.
#[derive(Default)]
pub struct Cursor {
    pub last: Option<(Option<i64>, i64)>,
    pub next_stamp: bool,
}
impl Cursor {
    pub fn bind_numbers(&self, nulls: bool) -> Vec<i64> {
        match self.last {
            None => vec![],
            Some((_, rowid)) if nulls => vec![rowid],
            Some((stamp, _)) if self.next_stamp => vec![stamp.unwrap()],
            Some((stamp, rowid)) => vec![stamp.unwrap(), rowid],
        }
    }
    pub fn advance(&mut self, rows: &[Value], _nulls: bool) -> bool {
        let scoped_stamp = self.last.is_some() && !self.next_stamp;
        if let Some(last) = rows.last() {
            self.last = Some((
                number(last, "createdAt"),
                number(last, "cursorRowid").unwrap(),
            ));
        }
        if rows.len() < PAGE {
            if scoped_stamp {
                self.next_stamp = true;
                return true;
            }
            return false;
        }
        self.next_stamp = false;
        true
    }
}

impl Cursor {
    pub fn phase(&self) -> (bool, bool) {
        match self.last {
            None => (true, false),
            Some((None, _)) if self.next_stamp => (false, false),
            Some((None, _)) => (true, true),
            Some((Some(_), _)) => (false, true),
        }
    }
}

pub const INITIAL_PAGE: usize = 16;

/// One bounded initial read for the two party-shaped sources. This avoids an
/// empty-source statement on small identities, while keeping the statement below 287.
pub fn initial_party_sql() -> String {
    let columns = "identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,seatSettlePubkey,seatSigHex,sigHex,createdAt,markerRowid,sigValid,cursorRowid,potCreatedAt,unknownPot";
    [Source::Party, Source::Link]
        .into_iter()
        .enumerate()
        .map(|(i, source)| {
            let sql = candidates_sql(source, true, false, false)
                .replace(&format!("LIMIT {PAGE}"), &format!("LIMIT {INITIAL_PAGE}"));
            format!("SELECT {i} AS sourceTag,{columns} FROM ({sql})")
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

/// Results and refund projections share one bounded spine read, with separate
/// proof joins so each production mapper still receives its original proof flag.
pub fn hydrate_party_pair_sql() -> String {
    let sql = hydrate_sql(&crate::results::results_sql(None, 0));
    sql.replacen("FROM (SELECT",", w.potCreatedAt AS potAdmittedAt, w.markerCreatedAt AS firstPartyMarkerAt, w.firstSpentAt AS firstSpentAt, EXISTS(SELECT 1 FROM potrefund_records pr WHERE pr.potTxid=w.potTxid AND pr.potVout=w.potVout) AS backupMarkerPresent, refund_sb.proof_verified AS refundSpenderProofVerified FROM (SELECT",1)
        .replace("LEFT JOIN network_seen ns", "LEFT JOIN pot_beefs refund_sb ON w.spendingTxid IS NOT NULL AND refund_sb.txid=lower(w.spendingTxid) LEFT JOIN network_seen ns")
}
