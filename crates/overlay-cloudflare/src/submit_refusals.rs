//! bsv-low #486 (2026-10-06): THE DOOR'S REFUSAL LEDGER.
//!
//! A JOIN the overlay admitted and the network then refused is EVICTED, and
//! the eviction ledger names the hops it spent (`pot_evictions.releasedSpends`):
//! the app layer strands those hops at once (`low-app-layer/src/owed.rs`). A
//! JOIN refused SYNCHRONOUSLY, before any admission (the door's interpreter:
//! 400 `ERR_SCRIPT_REFUSED`; the network's definitive word: 422), wrote
//! nothing, so its hops kept the in-progress row with the felt's rejoin for
//! the whole 30-minute young window.
//!
//! This ledger is that refusal's durable word, and it is KEYED ON THE UTXO the
//! refusal would have consumed (the lens fold of 2026-10-06, MEDIUM-1): ONE
//! row per indexed hop OUTPOINT, never one per refused transaction. A refused
//! txid is a name anyone mints for free (`tm_lowfund` admits any P2PKH, and a
//! second input's unlocking script changes the txid without touching a
//! SIGHASH_ALL signature), so a ledger keyed on it grew by one row per refused
//! variant at zero sats. Keyed on the outpoint, the N-th variant finds the row
//! the first one wrote and changes nothing (`refusals_to_write`).
//!
//! UNFORGEABLE by construction, the same bar the eviction's released spends
//! meet (an evicted transaction passed admission, so its hops' keys signed
//! it). A refused transaction passed nothing, and anyone can submit bytes that
//! name a stranger's hop, so an input is recorded ONLY when its unlocking
//! script VERIFIES here against the source output the BEEF carries
//! (`overlay_discovery::pot::p2pkh_input_signed`: the hop's own key signed
//! THIS transaction) and the index holds that output UNSPENT (`pot_records`,
//! where `tm_lowfund` puts every hop). A stranger's refused bytes record
//! nothing, and neither does a corrupted copy of a JOIN that already landed.
//!
//! REVERSAL (the lens's LOW-3): a refusal says one copy was refused, never
//! that no copy can land. When a transaction spending the hop IS admitted (the
//! door's own write, or an eviction's readmission re-marking the spend), the
//! row is retired (`retire_admitted`): the index's spend pointer names the
//! outpoints, so the retirement is keyed on the UTXO too.
//!
//! NOT recorded: a 502 (transport trouble is retryable by the door's own
//! contract; the same bytes may still land), and anything that is not a
//! P2PKH source (a refused pot spend is the pot's story, never a hop's).
//!
//! Written after the answer is decided, under `wait_until`, fail-soft: the
//! door's verdict never depends on it. Read by the app layer alone, by the
//! walking identity's own hop outpoints (`owed::OWED_DOOR_REFUSALS_SQL`).
//!
//! COST of one refused submit, after this fold: one signature check per
//! judged input (at most `SUBMIT_REFUSAL_MAX_INPUTS`), one D1 read, and for a
//! hop not yet named (or named by a weaker or hour-old word) one upsert per
//! hop, one prune and one `pot_changes` flush. A repeated variant costs the
//! read alone.

use crate::d1::{QVal, Query};
use worker::*;

/// Migration 161: the ledger, ONE row per hop outpoint. `refusedTxid` is the newest refused subject that named
/// the hop (a log pointer, never a key).
pub const SUBMIT_REFUSALS_CREATE: &str = "CREATE TABLE IF NOT EXISTS submit_refusals (hopTxid TEXT NOT NULL, hopVout INTEGER NOT NULL, refusedTxid TEXT NOT NULL, reason TEXT NOT NULL, refusedAt INTEGER NOT NULL, PRIMARY KEY (hopTxid, hopVout))";
/// Migration 162: the door's own pass prunes by age.
pub const SUBMIT_REFUSALS_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_submit_refusals_at ON submit_refusals(refusedAt)";
/// Binds: hopTxid (as `pot_records` holds it), hopVout, refusedTxid (lowercase), reason, refusedAt (unix ms). A
/// later refusal REPLACES the hop's row (never a second row).
pub const SUBMIT_REFUSAL_UPSERT_SQL: &str = "INSERT INTO submit_refusals (hopTxid, hopVout, refusedTxid, reason, refusedAt) VALUES (?, ?, ?, ?, ?) \
     ON CONFLICT(hopTxid, hopVout) DO UPDATE SET refusedTxid = excluded.refusedTxid, reason = excluded.reason, refusedAt = excluded.refusedAt";
/// Bind: the cutoff (unix ms). Run with every write: the ledger only ever holds the retention window.
pub const SUBMIT_REFUSALS_GC_SQL: &str = "DELETE FROM submit_refusals WHERE refusedAt < ?";
/// Bind: the admitted (or readmitted) transaction's txid, lowercase. Retires the refusal of every hop the index
/// shows SPENT by it (`idx_pot_spending`, then the ledger's primary key; the unary plus keeps the planner off the
/// `spent` indexes, which would walk every spent row).
pub const SUBMIT_REFUSALS_RETIRE_SQL: &str =
    "DELETE FROM submit_refusals WHERE (hopTxid, hopVout) IN (SELECT txid, outputIndex FROM pot_records WHERE spendingTxid = ? AND +spent = 1)";
/// How long a refusal is kept: the app layer reads 24 hours of it, and a hop past its young window (30 minutes) is
/// stranded by age regardless.
pub const SUBMIT_REFUSALS_RETENTION_MS: i64 = 48 * 60 * 60 * 1000;
/// A hop's standing row is rewritten by an equal or weaker word only once it is this old (so the row cannot age out
/// of the reader's window under a hop still being refused, and a flood of variants writes once an hour at most).
pub const SUBMIT_REFUSAL_REFRESH_MS: i64 = 60 * 60 * 1000;
/// Inputs of one refused subject the ledger will judge (a JOIN has two; one signature check each).
pub const SUBMIT_REFUSAL_MAX_INPUTS: usize = 8;
/// The 400 arm's word: the door's interpreter refused a script of this subject (ONE copy; another may be good).
pub const REASON_SCRIPT_REFUSED: &str = "script-refused";
/// The 422 arm's word, followed by the network's status word.
pub const REASON_NETWORK_REJECTED_PREFIX: &str = "network-rejected";

/// PURE: the 422 arm's reason (`network-rejected: <status word>`).
pub fn network_rejected_reason(status_word: &str) -> String {
    format!("{REASON_NETWORK_REJECTED_PREFIX}: {status_word}")
}

/// PURE: how much a reason says. The network's definitive word outranks the door's script refusal.
fn reason_rank(reason: &str) -> u8 {
    if reason.starts_with(REASON_NETWORK_REJECTED_PREFIX) {
        2
    } else {
        1
    }
}

/// One hop outpoint whose own key signed a refused subject.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SignedSpend {
    pub txid: String,
    pub vout: u32,
}

/// PURE: the outpoints the subject would spend whose P2PKH key SIGNED the subject (lowercase txid, vout), in input
/// order. The source output is read from the BEEF's own bytes for the txid the input names (content-addressed);
/// a source the BEEF does not carry, a non-P2PKH source, or a script that does not verify contributes nothing. A
/// subject with more than `SUBMIT_REFUSAL_MAX_INPUTS` inputs is not a JOIN: nothing is judged.
pub fn signed_p2pkh_spends(beef_bytes: &[u8], subject_txid: &str) -> Vec<SignedSpend> {
    let Ok(beef) = bsv_rs::transaction::Beef::from_binary(beef_bytes) else {
        return Vec::new();
    };
    let Some(tx) = beef.find_txid(subject_txid).and_then(|t| t.tx().cloned()) else {
        return Vec::new();
    };
    if tx.inputs.len() > SUBMIT_REFUSAL_MAX_INPUTS {
        return Vec::new();
    }
    let mut out = Vec::new();
    for (vin, input) in tx.inputs.iter().enumerate() {
        let Some(src_txid) = input.source_txid.as_deref().map(str::to_ascii_lowercase) else {
            continue;
        };
        let Some(src) = beef.find_txid(&src_txid).and_then(|t| t.tx()) else {
            continue;
        };
        let Some(src_out) = src.outputs.get(input.source_output_index as usize) else {
            continue;
        };
        let Some(sats) = src_out.satoshis else { continue };
        let lock = src_out.locking_script.to_binary();
        if overlay_discovery::pot::p2pkh_input_signed(&tx, vin, &lock, sats) {
            out.push(SignedSpend {
                txid: src_txid,
                vout: input.source_output_index,
            });
        }
    }
    out
}

/// PURE: the ONE read `record` makes (binds: txid, vout per pair, in order): each signed spend's `pot_records` row
/// (presence and the index's spend word) beside the refusal the ledger already holds for it.
pub fn index_read_query(signed: &[SignedSpend]) -> Query {
    let pairs = vec!["(p.txid = ? AND p.outputIndex = ?)"; signed.len()].join(" OR ");
    let mut q = Query::new(format!(
        "SELECT p.txid AS txid, p.outputIndex AS outputIndex, p.spent AS spent, r.reason AS heldReason, r.refusedAt AS heldAt \
         FROM pot_records p LEFT JOIN submit_refusals r ON r.hopTxid = p.txid AND r.hopVout = p.outputIndex WHERE {pairs}"
    ));
    for s in signed {
        q = q.bind(s.txid.as_str()).bind(i64::from(s.vout));
    }
    q
}

/// One row of [`index_read_query`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct IndexedHop {
    pub txid: String,
    #[serde(rename = "outputIndex")]
    pub output_index: i64,
    #[serde(default)]
    pub spent: Option<i64>,
    #[serde(rename = "heldReason", default)]
    pub held_reason: Option<String>,
    #[serde(rename = "heldAt", default)]
    pub held_at: Option<i64>,
}

/// PURE, THE DOOR'S DECISION: which signed spends earn a ledger write for this refusal. A spend is written only when
/// (1) the index HOLDS its outpoint (a signature alone names nothing: anyone signs for an output of their own that
/// no seat's marker can point at), (2) the index shows it UNSPENT (a corrupted copy of a JOIN that already landed
/// names nothing), and (3) the ledger holds no row for it, or holds a WEAKER word (a script refusal under the
/// network's definitive one), or holds a row older than `SUBMIT_REFUSAL_REFRESH_MS`. (3) is the flood's bound: a
/// stranger's N-th refused variant of one outpoint writes nothing and notes nothing.
pub fn refusals_to_write(signed: &[SignedSpend], index: &[IndexedHop], reason: &str, now_ms: i64) -> Vec<SignedSpend> {
    let mut out: Vec<SignedSpend> = Vec::new();
    for s in signed {
        let Some(row) = index.iter().find(|r| r.output_index == i64::from(s.vout) && r.txid.eq_ignore_ascii_case(&s.txid)) else {
            continue;
        };
        if row.spent.unwrap_or(0) != 0 {
            continue;
        }
        let standing = match (row.held_reason.as_deref(), row.held_at) {
            (Some(held), Some(at)) => reason_rank(held) >= reason_rank(reason) && now_ms.saturating_sub(at) < SUBMIT_REFUSAL_REFRESH_MS,
            _ => false,
        };
        // the hop's txid as the INDEX holds it: the reader joins on it
        let spend = SignedSpend { txid: row.txid.clone(), vout: s.vout };
        if !standing && !out.contains(&spend) {
            out.push(spend);
        }
    }
    out
}

/// PURE: the writes of one refusal, in order: one upsert per hop, then the prune. Empty when nothing is to write.
pub fn write_queries(to_write: &[SignedSpend], subject_txid: &str, reason: &str, now_ms: i64) -> Vec<Query> {
    if to_write.is_empty() {
        return Vec::new();
    }
    let mut out: Vec<Query> = to_write
        .iter()
        .map(|s| {
            Query::new(SUBMIT_REFUSAL_UPSERT_SQL)
                .bind(s.txid.as_str())
                .bind(i64::from(s.vout))
                .bind(subject_txid.to_ascii_lowercase())
                .bind(reason)
                .bind(QVal::Int(now_ms))
        })
        .collect();
    out.push(Query::new(SUBMIT_REFUSALS_GC_SQL).bind(QVal::Int(now_ms - SUBMIT_REFUSALS_RETENTION_MS)));
    out
}

/// PURE: the retirement of every refusal whose hop the index shows spent by `admitted_txid`.
pub fn retire_query(admitted_txid: &str) -> Query {
    Query::new(SUBMIT_REFUSALS_RETIRE_SQL).bind(admitted_txid.to_ascii_lowercase())
}

/// Record one synchronous refusal of `subject_txid` (see the module docs): `signed_p2pkh_spends`, the one read
/// (`index_read_query`), the decision (`refusals_to_write`), the writes (`write_queries`); the real-SQLite pins run
/// exactly those four. Meant for `wait_until`; every fault is logged and swallowed. Notes each WRITTEN hop for the
/// app layer (`pot_changes`: its hook attributes a P2PKH outpoint through the seats' own hop markers and re-derives
/// their owed rows) and ships the notes itself.
pub async fn record(env: Env, beef: Vec<u8>, subject_txid: String, reason: String, now_ms: i64) {
    let signed = signed_p2pkh_spends(&beef, &subject_txid);
    if signed.is_empty() {
        return;
    }
    let db = match env.d1("OVERLAY_DB") {
        Ok(db) => db,
        Err(e) => {
            console_log!("[submit-refusals] OVERLAY_DB unavailable for {subject_txid}: {e}");
            return;
        }
    };
    let index = match index_read_query(&signed).fetch_all::<IndexedHop>(&db).await {
        Ok(rows) => rows,
        Err(e) => {
            console_log!("[submit-refusals] index read failed for {subject_txid}: {e}");
            return;
        }
    };
    let to_write = refusals_to_write(&signed, &index, &reason, now_ms);
    if to_write.is_empty() {
        return;
    }
    let mut queries = write_queries(&to_write, &subject_txid, &reason, now_ms);
    let prune = queries.pop();
    for q in queries {
        if let Err(e) = q.execute(&db).await {
            console_log!("[submit-refusals] ledger write failed for {subject_txid}: {e}");
            return;
        }
    }
    if let Some(prune) = prune {
        if let Err(e) = prune.execute(&db).await {
            console_log!("[submit-refusals] prune failed: {e}");
        }
    }
    console_log!(
        "[submit-refusals] {subject_txid} refused at the door ({reason}): {} signed hop spend(s) recorded",
        to_write.len()
    );
    for s in &to_write {
        crate::pot_changes::note(&s.txid, s.vout);
    }
    crate::pot_changes::flush_inline(env).await;
}

/// THE REVERSAL: a transaction spending the hop was admitted (or readmitted), so the refusal of an earlier copy is
/// stale. Fail-soft (a row that survives a faulted delete is inert: the reader skips a hop the index shows spent).
pub async fn retire_admitted(db: &D1Database, admitted_txid: &str) {
    if let Err(e) = retire_query(admitted_txid).execute(db).await {
        console_log!("[submit-refusals] retire for {admitted_txid} failed ({e}): the row stands, inert while the hop reads spent");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bsv_rs::primitives::PrivateKey;
    use bsv_rs::script::templates::P2PKH;
    use bsv_rs::script::{Script, ScriptTemplate, SignOutputs, UnlockingScript};
    use bsv_rs::transaction::{MerklePath, MerklePathLeaf, Transaction, TransactionInput, TransactionOutput};

    /// A "mined" hop: one throwaway input, `sats` to `key`'s P2PKH, carrying a one-leaf BUMP so a BEEF can hold it.
    fn hop(key: &PrivateKey, sats: u64, salt: u8) -> Transaction {
        let mut tx = Transaction::new();
        tx.inputs.push(TransactionInput {
            source_txid: Some(hex::encode([salt; 32])),
            source_output_index: 0,
            unlocking_script: Some(UnlockingScript::from_script(Script::new())),
            ..Default::default()
        });
        tx.outputs.push(TransactionOutput::new(sats, P2PKH::new().lock(&key.public_key().hash160()).unwrap()));
        let txid = tx.id();
        tx.merkle_path = Some(MerklePath::new(900_000, vec![vec![MerklePathLeaf::new_txid(0, txid)]]).unwrap());
        tx
    }

    /// A two-input "JOIN": hop A signed by `sign_a`, hop B signed by `sign_b`.
    async fn join(hop_a: Transaction, sign_a: &PrivateKey, hop_b: Transaction, sign_b: &PrivateKey) -> (Vec<u8>, String) {
        let mut tx = Transaction::new();
        tx.add_input_from_tx(hop_a, 0, P2PKH::unlock(sign_a, SignOutputs::All, false)).unwrap();
        tx.add_input_from_tx(hop_b, 0, P2PKH::unlock(sign_b, SignOutputs::All, false)).unwrap();
        tx.outputs.push(TransactionOutput::new(40_000, P2PKH::new().lock(&[0x11u8; 20]).unwrap()));
        tx.sign().await.expect("template signing");
        (tx.to_beef(false).expect("BEEF with both proven parents"), tx.id())
    }

    /// bsv-low #486: the ledger records a hop only when the hop's OWN key signed the refused transaction. A JOIN one
    /// seat signed honestly and the other seat corrupted (the door's `ERR_SCRIPT_REFUSED`) names the honest seat's
    /// hop alone; bytes a STRANGER submits naming a victim's hop (signed with the stranger's key: also refused at
    /// the door) name nothing, so no one can strand another seat's live hop by getting bytes refused.
    #[tokio::test]
    async fn only_a_hop_whose_own_key_signed_the_refused_transaction_is_recorded() {
        let (a, b, stranger) = (PrivateKey::random(), PrivateKey::random(), PrivateKey::random());
        let (hop_a, hop_b) = (hop(&a, 20_190, 0xa1), hop(&b, 20_190, 0xb2));
        let (a_txid, b_txid) = (hop_a.id(), hop_b.id());
        // both seats signed: both hops are named (a valid JOIN the NETWORK refused, the 422 arm)
        let (beef, txid) = join(hop_a.clone(), &a, hop_b.clone(), &b).await;
        assert_eq!(
            signed_p2pkh_spends(&beef, &txid),
            vec![SignedSpend { txid: a_txid.clone(), vout: 0 }, SignedSpend { txid: b_txid.clone(), vout: 0 }]
        );
        // seat B's input does not verify (the interpreter's refusal): seat A's hop alone is named
        let (beef, txid) = join(hop_a.clone(), &a, hop_b.clone(), &stranger).await;
        assert_eq!(signed_p2pkh_spends(&beef, &txid), vec![SignedSpend { txid: a_txid.clone(), vout: 0 }]);
        // a stranger's bytes naming BOTH victims' hops: nothing
        let (beef, txid) = join(hop_a.clone(), &stranger, hop_b.clone(), &stranger).await;
        assert!(signed_p2pkh_spends(&beef, &txid).is_empty());
        // a subject the BEEF does not hold, junk bytes: nothing
        assert!(signed_p2pkh_spends(&beef, &hex::encode([0x77u8; 32])).is_empty());
        assert!(signed_p2pkh_spends(b"junk", &txid).is_empty());
    }

    /// Replay a production-built [`Query`] on real SQLite: its own SQL, its own binds.
    fn binds(q: &Query) -> Vec<rusqlite::types::Value> {
        q.params()
            .iter()
            .map(|p| match p {
                QVal::Null => rusqlite::types::Value::Null,
                QVal::Int(i) => rusqlite::types::Value::Integer(*i),
                QVal::Text(s) => rusqlite::types::Value::Text(s.clone()),
                QVal::Bool(b) => rusqlite::types::Value::Integer(i64::from(*b)),
                QVal::Blob(b) => rusqlite::types::Value::Blob(b.clone()),
                QVal::Float(f) => rusqlite::types::Value::Real(*f),
            })
            .collect()
    }

    fn ledger_db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        for sql in crate::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                assert!(e.to_string().to_ascii_lowercase().contains("duplicate column"), "migration failed under real SQLite: {e}");
            }
        }
        conn
    }

    /// `record`, natively: the same four steps over real SQLite. Returns the hops written (the ones `record` notes).
    fn record_on(conn: &rusqlite::Connection, beef: &[u8], subject: &str, reason: &str, now_ms: i64) -> Vec<SignedSpend> {
        let signed = signed_p2pkh_spends(beef, subject);
        if signed.is_empty() {
            return Vec::new();
        }
        let read = index_read_query(&signed);
        let index: Vec<IndexedHop> = conn
            .prepare(read.sql())
            .unwrap()
            .query_map(rusqlite::params_from_iter(binds(&read).iter()), |r| {
                Ok(IndexedHop { txid: r.get(0)?, output_index: r.get(1)?, spent: r.get(2)?, held_reason: r.get(3)?, held_at: r.get(4)? })
            })
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        let to_write = refusals_to_write(&signed, &index, reason, now_ms);
        for q in write_queries(&to_write, subject, reason, now_ms) {
            conn.execute(q.sql(), rusqlite::params_from_iter(binds(&q).iter())).unwrap();
        }
        to_write
    }

    fn index_hop(conn: &rusqlite::Connection, txid: &str) {
        conn.execute("INSERT INTO pot_records (txid, outputIndex, spent, createdAt) VALUES (?1, 0, 0, 800)", [txid]).unwrap();
    }

    fn ledger(conn: &rusqlite::Connection) -> Vec<(String, i64, String, String, i64)> {
        conn.prepare("SELECT hopTxid, hopVout, refusedTxid, reason, refusedAt FROM submit_refusals ORDER BY hopTxid")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }

    /// The lens's MEDIUM-2, the door half, EXECUTED on the shipped schema: a refused submit writes exactly the row
    /// of each hop whose own key signed it AND that the index holds unspent, and nothing else. Both door arms (the
    /// 400's word, the 422's). To red: let `refusals_to_write` keep a spend the index does not hold, or one it shows
    /// spent, or drop the signature check in `signed_p2pkh_spends`.
    #[tokio::test]
    async fn a_refused_submit_writes_exactly_the_row_of_each_signed_and_indexed_hop_real_sqlite() {
        let conn = ledger_db();
        let (a, b, stranger) = (PrivateKey::random(), PrivateKey::random(), PrivateKey::random());
        let (hop_a, hop_b) = (hop(&a, 20_190, 0xa1), hop(&b, 20_190, 0xb2));
        let (a_txid, b_txid) = (hop_a.id(), hop_b.id());
        // seat B's input is corrupted (the 400 arm); only hop A is indexed so far
        index_hop(&conn, &a_txid);
        let (beef, bad_join) = join(hop_a.clone(), &a, hop_b.clone(), &stranger).await;
        let wrote = record_on(&conn, &beef, &bad_join, REASON_SCRIPT_REFUSED, 1_000);
        assert_eq!(wrote, vec![SignedSpend { txid: a_txid.clone(), vout: 0 }]);
        assert_eq!(ledger(&conn), vec![(a_txid.clone(), 0, bad_join.clone(), REASON_SCRIPT_REFUSED.to_string(), 1_000)]);
        // an UNSIGNED input (a stranger's bytes naming both indexed hops): no row, whatever the index holds
        index_hop(&conn, &b_txid);
        let (beef, forged) = join(hop_a.clone(), &stranger, hop_b.clone(), &stranger).await;
        assert!(record_on(&conn, &beef, &forged, REASON_SCRIPT_REFUSED, 1_100).is_empty());
        assert_eq!(ledger(&conn).len(), 1, "a stranger's bytes record nothing about a victim's hop");
        // an UNINDEXED input (signed, but no `pot_records` row): no row
        let c = PrivateKey::random();
        let hop_c = hop(&c, 5_000, 0xc3);
        let (beef, loose) = join(hop_c.clone(), &c, hop_b.clone(), &stranger).await;
        assert!(record_on(&conn, &beef, &loose, REASON_SCRIPT_REFUSED, 1_200).is_empty());
        assert_eq!(ledger(&conn).len(), 1);
        // the 422 arm: both seats signed, the network refused: hop B gains its row, hop A's word is upgraded
        let (beef, valid_join) = join(hop_a.clone(), &a, hop_b.clone(), &b).await;
        let reason = network_rejected_reason("REJECTED");
        let wrote = record_on(&conn, &beef, &valid_join, &reason, 2_000);
        assert_eq!(wrote.len(), 2);
        let mut want = vec![(a_txid.clone(), 0, valid_join.clone(), reason.clone(), 2_000), (b_txid.clone(), 0, valid_join.clone(), reason.clone(), 2_000)];
        want.sort();
        assert_eq!(ledger(&conn), want);
        // a hop the index shows SPENT (the JOIN landed; a corrupted copy arrives after): no row (the lens's LOW-3)
        conn.execute("DELETE FROM submit_refusals", []).unwrap();
        conn.execute("UPDATE pot_records SET spent = 1, spendingTxid = ?1 WHERE txid = ?2", rusqlite::params![valid_join, a_txid]).unwrap();
        let (beef, late_copy) = join(hop_a.clone(), &a, hop_b.clone(), &stranger).await;
        assert!(record_on(&conn, &beef, &late_copy, REASON_SCRIPT_REFUSED, 3_000).is_empty());
        assert!(ledger(&conn).is_empty());
    }

    /// The lens's MEDIUM-1: the row key is the hop OUTPOINT. A stranger who owns one indexed output and mints
    /// refused variants of its spend (a different second input each time: a different txid, the same signature
    /// rules) holds ONE row, and from the second variant on writes nothing and notes nothing.
    /// To red: key the upsert on the refused txid, or drop the standing-row rule of `refusals_to_write`.
    #[tokio::test]
    async fn refused_variants_of_one_outpoint_hold_one_row_and_write_once_real_sqlite() {
        let conn = ledger_db();
        let stranger = PrivateKey::random();
        let own = hop(&stranger, 1_000, 0x51);
        index_hop(&conn, &own.id());
        let mut txids = std::collections::HashSet::new();
        let mut writes = 0usize;
        for n in 0..24u8 {
            let other = PrivateKey::random();
            let (beef, variant) = join(own.clone(), &stranger, hop(&other, 1_000, n), &stranger).await;
            assert!(txids.insert(variant.clone()), "every variant is a new txid");
            writes += record_on(&conn, &beef, &variant, REASON_SCRIPT_REFUSED, 10_000 + i64::from(n)).len();
        }
        assert_eq!(txids.len(), 24);
        assert_eq!(ledger(&conn).len(), 1, "one row per outpoint, whatever the count of refused variants");
        assert_eq!(writes, 1, "the first variant wrote; the other 23 cost the read alone");
        assert_eq!(ledger(&conn)[0].4, 10_000, "the standing row was not rewritten");
        // the standing word is refreshed only once it is an hour old (it must not age out under a live refusal)
        let (beef, late) = join(own.clone(), &stranger, hop(&PrivateKey::random(), 1_000, 0x77), &stranger).await;
        assert!(record_on(&conn, &beef, &late, REASON_SCRIPT_REFUSED, 10_000 + SUBMIT_REFUSAL_REFRESH_MS - 1).is_empty());
        assert_eq!(record_on(&conn, &beef, &late, REASON_SCRIPT_REFUSED, 10_000 + SUBMIT_REFUSAL_REFRESH_MS).len(), 1);
        assert_eq!(ledger(&conn), vec![(own.id(), 0, late, REASON_SCRIPT_REFUSED.to_string(), 10_000 + SUBMIT_REFUSAL_REFRESH_MS)]);
        // a weaker word never replaces the network's
        let held = [IndexedHop { txid: own.id(), output_index: 0, spent: Some(0), held_reason: Some(network_rejected_reason("REJECTED")), held_at: Some(5) }];
        let one = [SignedSpend { txid: own.id(), vout: 0 }];
        assert!(refusals_to_write(&one, &held, REASON_SCRIPT_REFUSED, 6).is_empty());
        assert!(refusals_to_write(&one, &held, &network_rejected_reason("REJECTED"), 6).is_empty());
    }

    /// THE REVERSAL (the lens's LOW-3): the same hop's spend is admitted after a refusal (the index marks the hop
    /// spent by the admitted transaction), and the row is retired by the UTXO the admitted transaction consumed,
    /// whatever txid the refusal carried. Another hop's row stands. Then the prune.
    /// To red: make `retire_query` match on `refusedTxid`.
    #[tokio::test]
    async fn an_admission_retires_the_refusal_of_the_hop_it_spends_real_sqlite() {
        let conn = ledger_db();
        let (a, b, stranger) = (PrivateKey::random(), PrivateKey::random(), PrivateKey::random());
        let (hop_a, hop_b) = (hop(&a, 20_190, 0xa1), hop(&b, 20_190, 0xb2));
        index_hop(&conn, &hop_a.id());
        index_hop(&conn, &hop_b.id());
        let (beef, corrupted) = join(hop_a.clone(), &a, hop_b.clone(), &stranger).await;
        assert_eq!(record_on(&conn, &beef, &corrupted, REASON_SCRIPT_REFUSED, 1_000).len(), 1);
        let other = hop(&stranger, 700, 0x0f);
        index_hop(&conn, &other.id());
        let (beef, theirs) = join(other.clone(), &stranger, hop_b.clone(), &a).await;
        assert_eq!(record_on(&conn, &beef, &theirs, REASON_SCRIPT_REFUSED, 1_000).len(), 1);
        assert_eq!(ledger(&conn).len(), 2);
        // the GOOD JOIN (another txid than the refused copy) is admitted: the index marks hop A spent by it
        let (_, good) = join(hop_a.clone(), &a, hop_b.clone(), &b).await;
        assert_ne!(good, corrupted);
        let retire = retire_query(&good);
        assert_eq!(conn.execute(retire.sql(), rusqlite::params_from_iter(binds(&retire).iter())).unwrap(), 0, "nothing is spent by it yet");
        conn.execute("UPDATE pot_records SET spent = 1, spendingTxid = ?1 WHERE txid = ?2", rusqlite::params![good, hop_a.id()]).unwrap();
        assert_eq!(conn.execute(retire.sql(), rusqlite::params_from_iter(binds(&retire).iter())).unwrap(), 1);
        assert_eq!(ledger(&conn).iter().map(|r| r.0.clone()).collect::<Vec<_>>(), vec![other.id()], "the stranger's own row stands");
        // the retirement runs off the spend pointer's index and the ledger's key
        let plan: Vec<String> = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {SUBMIT_REFUSALS_RETIRE_SQL}"))
            .unwrap()
            .query_map([&good], |r| r.get::<_, String>(3))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert!(plan.iter().any(|l| l.contains("idx_pot_spending")), "{plan:?}");
        // the prune
        assert_eq!(conn.execute(SUBMIT_REFUSALS_GC_SQL, [1_000i64]).unwrap(), 0, "a row at the cutoff is kept");
        assert_eq!(conn.execute(SUBMIT_REFUSALS_GC_SQL, [1_001i64]).unwrap(), 1);
    }

    /// The door's two hooks, by source (the arms themselves need a live Worker): the 400 arm and the definitive 422
    /// arm each hand `record` their word; no other arm does (a 502 is retryable: the same bytes may still land);
    /// the admission's 200 path and the readmission retire. A source pin, comments stripped.
    #[test]
    fn the_door_records_on_its_400_and_its_definitive_422_alone_and_retires_on_admission() {
        let code_only = |s: &str| s.lines().map(|l| l.split("//").next().unwrap_or("")).collect::<Vec<_>>().join("\n");
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let routes = code_only(include_str!("routes.rs"));
        let routes = squash(&routes[..routes.find("#[cfg(test)]").unwrap()]);
        let call = "crate::submit_refusals::record(";
        assert_eq!(routes.matches(call).count(), 2, "exactly two hooks");
        let first = routes.find(call).unwrap();
        let second = first + 1 + routes[first + 1..].find(call).unwrap();
        let arm_400 = routes.find("count(crate::ops::COUNTER_SUBMIT_SCRIPT_REFUSED);").expect("the 400 arm");
        assert!(arm_400 < first && first - arm_400 < 200, "the first hook sits in the script-refused arm");
        assert!(routes[first..first + 260].contains("crate::submit_refusals::REASON_SCRIPT_REFUSED.to_string()"));
        assert!(routes[second..second + 320].contains("crate::submit_refusals::network_rejected_reason("));
        let answer_422 = routes[second..].find("json_error(&format!(\"networkrejected:{reason}\"),422)").expect("the 422 answer");
        assert!(answer_422 < 500, "the second hook sits right before the definitive 422 answer (at +{answer_422})");
        assert_eq!(routes.matches("crate::submit_refusals::retire_admitted(").count(), 1, "the door's admission retires");
        let admit = code_only(include_str!("admit_fast.rs"));
        let admit = squash(&admit[..admit.find("#[cfg(test)]").unwrap()]);
        let readmit = &admit[admit.find("pubasyncfnreadmit_if_evicted(").unwrap()..];
        let remark = readmit.find("remark_spends(db,&txid,&released,now_ms)").expect("the re-mark");
        let retire = readmit.find("crate::submit_refusals::retire_admitted(db,&txid)").expect("the readmission retires");
        assert!(remark < retire, "retired AFTER the spend pointers are back");
    }
}
