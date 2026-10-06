//! bsv-low #486 (2026-10-06): THE DOOR'S REFUSAL LEDGER.
//!
//! A JOIN the overlay admitted and the network then refused is EVICTED, and
//! the eviction ledger names the hops it spent (`pot_evictions.releasedSpends`):
//! the app layer strands those hops at once (`low-app-layer/src/owed.rs`). A
//! JOIN refused SYNCHRONOUSLY, before any admission (the door's interpreter:
//! 400 `ERR_SCRIPT_REFUSED`; the network's definitive word: 422), wrote
//! nothing, so its hops kept the in-progress row with the felt's rejoin for
//! the whole 30-minute young window, for a hand that can never start from
//! that transaction.
//!
//! This ledger is that refusal's durable word: one row per refused subject,
//! naming the outpoints it would have spent. It is keyed on the UTXO and it
//! is UNFORGEABLE by construction, the same bar the eviction's released
//! spends meet (an evicted transaction passed admission, so its hops' keys
//! signed it). A refused transaction passed nothing, and anyone can submit
//! bytes that name a stranger's hop, so an input is recorded ONLY when its
//! unlocking script VERIFIES here against the source output the BEEF carries
//! (`overlay_discovery::pot::p2pkh_input_signed`: the hop's own key signed
//! THIS transaction) and the index holds that output (`pot_records`, where
//! `tm_lowfund` puts every hop). A stranger's refused bytes record nothing.
//!
//! NOT recorded: a 502 (transport trouble is retryable by the door's own
//! contract; the same bytes may still land), and anything that is not a
//! P2PKH source (a refused pot spend is the pot's story, never a hop's).
//!
//! Written after the answer is decided, under `wait_until`, fail-soft: the
//! door's verdict never depends on it. Read by the app layer alone.

use crate::d1::Query;
use worker::*;

/// Migration 161: the ledger. `signedSpends` is JSON, `[{"txid","vout"}, ...]`, the shape of
/// `pot_evictions.releasedSpends` without its `table` (every entry is a `pot_records` row).
pub const SUBMIT_REFUSALS_CREATE: &str =
    "CREATE TABLE IF NOT EXISTS submit_refusals (txid TEXT PRIMARY KEY, reason TEXT NOT NULL, refusedAt INTEGER NOT NULL, signedSpends TEXT)";
/// Migration 162: the app layer reads a recent window, the door's own pass prunes by age.
pub const SUBMIT_REFUSALS_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_submit_refusals_at ON submit_refusals(refusedAt)";
/// Binds: txid (lowercase), reason, refusedAt (unix ms), signedSpends (JSON). A re-refusal of the same bytes
/// refreshes the row (the newest word and time).
pub const SUBMIT_REFUSAL_UPSERT_SQL: &str = "INSERT INTO submit_refusals (txid, reason, refusedAt, signedSpends) VALUES (?, ?, ?, ?) \
     ON CONFLICT(txid) DO UPDATE SET reason = excluded.reason, refusedAt = excluded.refusedAt, signedSpends = excluded.signedSpends";
/// Bind: the cutoff (unix ms). Run with every write: the ledger only ever holds the retention window.
pub const SUBMIT_REFUSALS_GC_SQL: &str = "DELETE FROM submit_refusals WHERE refusedAt < ?";
/// How long a refusal is kept: the app layer reads 24 hours of it, and a hop past its young window (30 minutes) is
/// stranded by age regardless.
pub const SUBMIT_REFUSALS_RETENTION_MS: i64 = 48 * 60 * 60 * 1000;
/// Inputs of one refused subject the ledger will judge (a JOIN has two; one signature check each).
pub const SUBMIT_REFUSAL_MAX_INPUTS: usize = 8;

/// One recorded spend, as the app layer parses it (`owed::released_hop_outpoints` reads `txid` and `vout`).
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

/// PURE: the `pot_records` presence read for the signed spends (binds: txid, vout per pair, in order).
pub fn indexed_spends_sql(n: usize) -> String {
    let pairs = vec!["(txid = ? AND outputIndex = ?)"; n].join(" OR ");
    format!("SELECT txid, outputIndex FROM pot_records WHERE {pairs}")
}

#[derive(serde::Deserialize)]
struct IndexedRow {
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: i64,
}

/// Record one synchronous refusal of `subject_txid` (see the module docs). Meant for `wait_until`; every fault is
/// logged and swallowed. Notes each recorded hop for the app layer (`pot_changes`: its hook attributes a P2PKH
/// outpoint through the seats' own hop markers and re-derives their owed rows) and ships the notes itself.
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
    let mut q = Query::new(indexed_spends_sql(signed.len()));
    for s in &signed {
        q = q.bind(s.txid.as_str()).bind(i64::from(s.vout));
    }
    let indexed: Vec<SignedSpend> = match q.fetch_all::<IndexedRow>(&db).await {
        Ok(rows) => signed
            .into_iter()
            .filter(|s| rows.iter().any(|r| r.output_index == i64::from(s.vout) && r.txid.eq_ignore_ascii_case(&s.txid)))
            .collect(),
        Err(e) => {
            console_log!("[submit-refusals] index read failed for {subject_txid}: {e}");
            return;
        }
    };
    if indexed.is_empty() {
        return;
    }
    let Ok(json) = serde_json::to_string(&indexed) else {
        return;
    };
    if let Err(e) = Query::new(SUBMIT_REFUSAL_UPSERT_SQL)
        .bind(subject_txid.to_ascii_lowercase())
        .bind(reason.as_str())
        .bind(now_ms)
        .bind(json)
        .execute(&db)
        .await
    {
        console_log!("[submit-refusals] ledger write failed for {subject_txid}: {e}");
        return;
    }
    if let Err(e) = Query::new(SUBMIT_REFUSALS_GC_SQL).bind(now_ms - SUBMIT_REFUSALS_RETENTION_MS).execute(&db).await {
        console_log!("[submit-refusals] prune failed: {e}");
    }
    console_log!(
        "[submit-refusals] {subject_txid} refused at the door ({reason}): {} signed hop spend(s) recorded",
        indexed.len()
    );
    for s in &indexed {
        crate::pot_changes::note(&s.txid, s.vout);
    }
    crate::pot_changes::flush_inline(env).await;
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
        // the JSON the app layer parses is the released-spends entry shape
        assert_eq!(
            serde_json::to_string(&[SignedSpend { txid: a_txid, vout: 0 }]).unwrap(),
            format!(r#"[{{"txid":"{}","vout":0}}]"#, hop_a.id())
        );
    }

    /// The statements run on real SQLite against the shipped migrations: the write, the re-refusal's refresh, the
    /// index presence read, the prune.
    #[test]
    fn the_ledger_statements_run_on_the_shipped_schema_real_sqlite() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        for sql in crate::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                assert!(e.to_string().to_ascii_lowercase().contains("duplicate column"), "migration failed under real SQLite: {e}");
            }
        }
        let (join, hop) = ("aa".repeat(32), "bb".repeat(32));
        let spends = format!(r#"[{{"txid":"{hop}","vout":0}}]"#);
        conn.execute(SUBMIT_REFUSAL_UPSERT_SQL, rusqlite::params![join, "script-refused", 1_000i64, spends]).unwrap();
        conn.execute(SUBMIT_REFUSAL_UPSERT_SQL, rusqlite::params![join, "network-rejected: REJECTED", 2_000i64, spends]).unwrap();
        let (reason, at): (String, i64) = conn.query_row("SELECT reason, refusedAt FROM submit_refusals WHERE txid = ?1", [&join], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
        assert_eq!((reason.as_str(), at), ("network-rejected: REJECTED", 2_000));
        conn.execute("INSERT INTO pot_records (txid, outputIndex, spent, createdAt) VALUES (?1, 0, 0, 800)", [&hop]).unwrap();
        let found: Vec<(String, i64)> = conn
            .prepare(&indexed_spends_sql(2))
            .unwrap()
            .query_map(rusqlite::params![hop, 0i64, "cc".repeat(32), 0i64], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(found, vec![(hop, 0)]);
        assert_eq!(conn.execute(SUBMIT_REFUSALS_GC_SQL, [2_000i64]).unwrap(), 0, "a row at the cutoff is kept");
        assert_eq!(conn.execute(SUBMIT_REFUSALS_GC_SQL, [2_001i64]).unwrap(), 1);
    }
}
