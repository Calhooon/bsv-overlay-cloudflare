//! Compact tracker-produced projection (K11 replacement boundary). No journal.
use super::{served, HeaderSnapshot};
use crate::d1_ledger::Counted;
use bsv_tracker::{
    Evidence, Header, Hint, HintStatus, HostAction, Input, Params, Proof, State, Word,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use worker::wasm_bindgen::JsValue;

pub const CREATE: &str = "CREATE TABLE IF NOT EXISTS tx_words (txid TEXT PRIMARY KEY, word TEXT NOT NULL, tier TEXT NOT NULL, height INTEGER, proof_root TEXT, proof_height INTEGER, proof_hex TEXT, header_hash TEXT, hint_source TEXT, hint_at INTEGER, updated_at INTEGER NOT NULL, legacy_body TEXT)";
pub const UPSERT: &str = "INSERT INTO tx_words (txid,word,tier,height,proof_root,proof_height,proof_hex,header_hash,hint_source,hint_at,updated_at,legacy_body) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12) ON CONFLICT(txid) DO UPDATE SET word=excluded.word,tier=excluded.tier,height=excluded.height,proof_root=excluded.proof_root,proof_height=excluded.proof_height,proof_hex=excluded.proof_hex,header_hash=excluded.header_hash,hint_source=CASE WHEN excluded.hint_at IS NULL OR tx_words.hint_at > excluded.hint_at THEN tx_words.hint_source ELSE COALESCE(excluded.hint_source,tx_words.hint_source) END,hint_at=CASE WHEN excluded.hint_at IS NULL THEN tx_words.hint_at ELSE MAX(COALESCE(tx_words.hint_at,0),excluded.hint_at) END,updated_at=MAX(tx_words.updated_at,excluded.updated_at),legacy_body=COALESCE(excluded.legacy_body,tx_words.legacy_body) WHERE (excluded.tier = 'chain' AND tx_words.tier <> 'chain') OR (tx_words.updated_at <= excluded.updated_at AND (tx_words.tier <> 'chain' OR excluded.tier = 'chain') AND NOT (tx_words.word = 'seen' AND excluded.word IN ('unknown','built','announced')) AND NOT (tx_words.word = 'announced' AND excluded.word IN ('unknown','built')) AND NOT (tx_words.word = 'built' AND excluded.word = 'unknown') AND (tx_words.word <> 'abandoned' OR excluded.word = 'abandoned' OR excluded.tier = 'chain') AND (tx_words.word <> 'mined' OR excluded.word IN ('mined','stale')))";
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Row {
    pub txid: String,
    pub word: String,
    pub tier: String,
    pub height: Option<f64>,
    pub proof_root: Option<String>,
    pub proof_height: Option<f64>,
    pub proof_hex: Option<String>,
    pub header_hash: Option<String>,
    pub hint_source: Option<String>,
    pub hint_at: Option<f64>,
    pub updated_at: f64,
    pub legacy_body: Option<String>,
}
impl Row {
    pub fn from_state(state: &State, now: u64, legacy: Option<String>) -> Self {
        let view = served::fields(state);
        let proof = state.evidence();
        let hint = state.hints().first();
        Self {
            txid: state.txid().into(),
            word: view["word"].as_str().unwrap().into(),
            tier: view["tier"].as_str().unwrap().into(),
            height: state.word().height().map(f64::from),
            proof_root: proof.map(|p| p.root().into()),
            proof_height: proof.map(|p| f64::from(p.height())),
            proof_hex: proof.map(|p| p.proof().path().to_hex()),
            header_hash: proof.map(|p| p.header().hash.clone()),
            hint_source: hint.map(|h| h.source.clone()),
            hint_at: hint.map(|h| h.observed as f64),
            updated_at: now as f64,
            legacy_body: legacy,
        }
    }
    pub fn view(&self) -> Value {
        let verify = if self.word == "mined" {
            json!({"root":self.proof_root,"height":self.proof_height.map(|h| h as u32),"headerHash":self.header_hash,"merklePath":self.proof_hex})
        } else {
            Value::Null
        };
        let mut view = json!({"word":self.word,"tier":self.tier,"height":self.height.map(|h| h as u32),"verifyPath":verify,"source":self.hint_source,"at":self.hint_at.map(|t| t as u64)});
        if let Some(word) = self.terminal_hint() {
            view["terminalHint"] = json!(word);
        }
        view
    }
    /// bsv-low #598 (the B-OWED-2 lens's HIGH-1): the terminal Arcade hint the row's producer marked
    /// (`runtime::mark_terminal`: `trackerTerminal`, `trackerTerminalWord`), served on [`Row::view`] as the additive
    /// `terminalHint` (absent on every unmarked row, so those views are byte for byte as before): `rejected`,
    /// `conflicted`, or `terminal` for a mark written before the word was recorded. Never on a mined row (the mark is
    /// cleared there).
    pub fn terminal_hint(&self) -> Option<String> {
        if self.word == "mined" {
            return None;
        }
        let body: Value = serde_json::from_str(self.legacy_body.as_deref()?).ok()?;
        if body["trackerTerminal"] != json!(true) {
            return None;
        }
        Some(match body["trackerTerminalWord"].as_str() {
            Some(w @ ("rejected" | "conflicted")) => w.to_string(),
            _ => "terminal".to_string(),
        })
    }
    /// The stored mined row's State: its retained proof stepped against the snapshot it recorded (its own header),
    /// so the word is `mined` with no re-ask. [`Row::rechecked`] and the chain-event intake
    /// (`chain_events::reask_of`, `chain_events::drain_one`) step it on.
    pub fn restored_state(&self) -> Result<State, String> {
        let path = overlay_engine::beef_limits::merkle_path_from_hex(
            self.proof_hex.as_deref().ok_or("missing stored proof")?,
            overlay_engine::beef_limits::COURIER_PROOF_MAX_BYTES,
        )
        .map_err(|e| e.to_string())?;
        let proof = Proof::new(&self.txid, path).map_err(|e| e.to_string())?;
        if self.proof_height != Some(f64::from(proof.height()))
            || self.height != self.proof_height
            || self.proof_root.as_deref() != Some(proof.root().map_err(|e| e.to_string())?.as_str())
        {
            return Err("stored proof projection mismatch".into());
        }
        let old = HeaderSnapshot {
            tip: Ok(proof.height()),
            headers: BTreeMap::from([(
                proof.height(),
                Ok(Some(Header {
                    hash: self.header_hash.clone().ok_or("missing stored header")?,
                    merkle_root: self.proof_root.clone().ok_or("missing stored root")?,
                })),
            )]),
        };
        let mut state = State::new(&self.txid);
        state
            .step(
                &Params { age_threshold: 60 },
                &old,
                Input::Evidence(Evidence::Proof(proof)),
            )
            .map_err(|e| e.to_string())?;
        Ok(state)
    }
    /// One retained proof seeds a fresh State against its recorded snapshot;
    /// the only current evidence input is Recheck. No historical input replay.
    pub fn rechecked(&self, current: &HeaderSnapshot) -> Result<Value, String> {
        if self.word != "mined" {
            return Ok(self.view());
        }
        let mut state = self.restored_state()?;
        let params = Params { age_threshold: 60 };
        let fault = state
            .step(&params, current, Input::Evidence(Evidence::Recheck))
            .err();
        let mut view = served::fields(&state);
        view["source"] = json!(self.hint_source);
        view["at"] = json!(self.hint_at.map(|t| t as u64));
        if let Some(e) = fault {
            view["verifyPath"] = Value::Null;
            view["cannotCheck"] = json!(e.to_string());
        }
        Ok(view)
    }
}
pub fn seed_hint(row: Option<&Row>, txid: &str) -> State {
    let mut state = State::new(txid);
    let empty = HeaderSnapshot {
        tip: Ok(0),
        headers: Default::default(),
    };
    let params = Params { age_threshold: 60 };
    if let Some(row) = row {
        if matches!(row.word.as_str(), "announced" | "seen") {
            state
                .step(&params, &empty, Input::Host(HostAction::Build))
                .expect("stored host seed");
        }
        let input = match row.word.as_str() {
            "built" => Some(Input::Host(HostAction::Build)),
            "abandoned" => Some(Input::Host(HostAction::Abandon)),
            "announced" => Some(Input::Hint(Hint::new(
                row.hint_source.as_deref().unwrap_or("stored"),
                HintStatus::Accepted,
                row.hint_at.unwrap_or(0.0) as u64,
            ))),
            "seen" => Some(Input::Hint(Hint::new(
                row.hint_source.as_deref().unwrap_or("stored"),
                HintStatus::Seen,
                row.hint_at.unwrap_or(0.0) as u64,
            ))),
            _ => None,
        };
        if let Some(input) = input {
            state.step(&params, &empty, input).expect("host/hint input");
        }
    }
    state
}
pub fn read_sql(n: usize) -> String {
    let marks = vec!["?"; n].join(",");
    format!("SELECT * FROM tx_words WHERE txid IN ({marks})")
}
/// One JSON bind, one indexed lookup statement even for a fleet-sized list.
pub const READ: &str =
    "SELECT tx_words.* FROM json_each(?1) AS wanted JOIN tx_words ON tx_words.txid = wanted.value";
pub async fn read(env: &worker::Env, ids: &[String]) -> worker::Result<BTreeMap<String, Row>> {
    let mut rows = BTreeMap::new();
    if ids.is_empty() {
        return Ok(rows);
    }
    let keys = serde_json::to_string(ids).map_err(|e| worker::Error::RustError(e.to_string()))?;
    for row in env
        .d1("OVERLAY_DB")?
        .prepare(READ)
        .bind(&[JsValue::from_str(&keys)])?
        .counted_all()
        .await?
        .results::<Row>()?
    {
        rows.insert(row.txid.clone(), row);
    }
    Ok(rows)
}
/// The [`UPSERT`]'s twelve binds of `row`, in its column order (`write`'s; a native pin binds them under real
/// SQLite).
pub fn upsert_binds(row: &Row) -> Vec<Value> {
    let value = serde_json::to_value(row).expect("a row serializes");
    UPSERT_COLUMNS.iter().map(|k| value[*k].clone()).collect()
}
const UPSERT_COLUMNS: [&str; 12] = [
    "txid",
    "word",
    "tier",
    "height",
    "proof_root",
    "proof_height",
    "proof_hex",
    "header_hash",
    "hint_source",
    "hint_at",
    "updated_at",
    "legacy_body",
];
pub async fn write(env: &worker::Env, row: &Row) -> worker::Result<()> {
    let binds: Vec<_> = upsert_binds(row)
        .iter()
        .map(|v| match v {
            Value::String(s) => JsValue::from_str(s),
            Value::Number(n) => JsValue::from_f64(n.as_f64().unwrap()),
            _ => JsValue::NULL,
        })
        .collect();
    env.d1("OVERLAY_DB")?
        .prepare(UPSERT)
        .bind(&binds)?
        .counted_run()
        .await?;
    Ok(())
}
/// Keep a chain projection on hints/absence. A checked proof is the sole
/// replacement; old Seen also survives an accepting or rejecting hint.
pub fn project(old: Option<&Row>, state: &State, now: u64, legacy: Option<String>) -> Row {
    if let Some(old) = old.filter(|r| r.tier == "chain" && !matches!(state.word(), Word::Mined(_)))
    {
        let mut row = old.clone();
        row.updated_at = now as f64;
        if legacy.is_some() {
            row.legacy_body = legacy;
        }
        if let Some(h) = state.hints().first() {
            row.hint_source = Some(h.source.clone());
            row.hint_at = Some(h.observed as f64);
        }
        row
    } else {
        Row::from_state(state, now, legacy)
    }
}

/// Durable cursor per host version: completed deploy passes do not restart on recycle.
pub const BACKFILL_VERSION: &str = "ns3-fold-v3-header-retry";
pub const BACKFILL_CREATE: &str = "CREATE TABLE IF NOT EXISTS tracker_backfill (version TEXT PRIMARY KEY, cursor TEXT NOT NULL, complete INTEGER NOT NULL)";
// One-to-one PK join; no proof/absence filter may scan beyond the 64-subject page.
pub const BACKFILL_PAGE: &str = "SELECT transactions.txid, CASE WHEN transactions.has_proof = 1 AND tx_words.txid IS NULL THEN transactions.beef END AS beef, transactions.has_proof, tx_words.txid AS tracked FROM transactions LEFT JOIN tx_words ON tx_words.txid = transactions.txid WHERE transactions.txid > ?1 ORDER BY transactions.txid LIMIT 64";
pub const BACKFILL_POT_PAGE: &str = "SELECT pot_beefs.txid, CASE WHEN pot_beefs.has_proof = 1 AND tx_words.txid IS NULL THEN pot_beefs.beef END AS beef, pot_beefs.has_proof, tx_words.txid AS tracked FROM pot_beefs LEFT JOIN tx_words ON tx_words.txid = pot_beefs.txid WHERE pot_beefs.txid > ?1 ORDER BY pot_beefs.txid LIMIT 64";
pub const BACKFILL_SAVE: &str = "INSERT INTO tracker_backfill(version,cursor,complete) VALUES (?1,?2,?3) ON CONFLICT(version) DO UPDATE SET cursor=MAX(tracker_backfill.cursor,excluded.cursor),complete=MAX(tracker_backfill.complete,excluded.complete)";

/// bsv-low #757: a live hold, the authority of a word event (`runtime::live_hold`): the engine's `transactions` row,
/// or the pot index's record of the subject as a pot or hop coin's spender (`pot_records.spendingTxid`; a settle,
/// refund or sweep admits no output and never has a `transactions` row; `pot_records` holds hop rows too, so a JOIN
/// and a courier-found hop sweep spender are held). Equal to the overlay's `tracker_events::LIVE_HOLD_SQL` (pinned);
/// [`SWEEP`] and [`DUE`] read the same two tables, the spender arm only while its word still matters.
pub const LIVE_HOLD: &str = "SELECT 1 AS held FROM transactions WHERE txid = lower(?1) UNION ALL SELECT 1 AS held FROM pot_records WHERE spendingTxid = lower(?1) LIMIT 1";

/// The sweep's statement, [`SWEEP`] and [`SWEEP_FRAMED`] (one spelling).
macro_rules! sweep_sql {
    ($tail:literal) => {
        concat!("DELETE FROM tx_words WHERE txid IN (SELECT w.txid FROM tx_words w WHERE w.updated_at < ?1 AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.txid=w.txid) AND NOT EXISTS (SELECT 1 FROM pot_records p WHERE p.spendingTxid=w.txid AND w.word IN ('unknown','built','announced','seen') AND COALESCE(json_extract(w.legacy_body,'$.trackerTerminal'),0) = 0) ORDER BY w.updated_at,w.txid LIMIT 64)", $tail)
    };
}
/// Expire stranger projections after eight days. Outside this sweep: a subject with a `transactions` row (main's arm,
/// byte for byte), and (bsv-low #757) a recorded pot or hop spender whose word is not yet mined or terminal: the
/// spender arm holds only while [`DUE`] would still refresh the row (`unknown`, `built`, `announced`, `seen`, not
/// `trackerTerminal`). Once the spender is mined (the index's own proof then decides its owed row), stale, abandoned
/// or terminal, its row ages out at eight days as on main, so `pot_records`' permanent pointers buy no permanent row
/// and no walk (the FIX-757 lens's MEDIUM-1; pinned by `tests/i757_sweep_walk.rs`).
pub const SWEEP: &str = sweep_sql!("");
/// FIX-1010 (bsv-low #1010): [`SWEEP`] as the tick runs it, answering the words it deleted (each noted `word`; the
/// row's trigger marked the stored rows that read it).
pub const SWEEP_FRAMED: &str = sweep_sql!(" RETURNING txid");

/// Only live held subjects buy scheduled refreshes (the same hold as [`SWEEP`]: its spender arm's word filter is this
/// statement's own, so a spender DUE stops refreshing is one SWEEP no longer spares). A
/// stranger's historic built projection cannot renew its own sweep age.
pub const DUE: &str = "SELECT w.txid FROM tx_words w WHERE w.word IN ('unknown','built','announced','seen') AND w.updated_at <= ?1 AND (EXISTS (SELECT 1 FROM transactions t WHERE t.txid=w.txid) OR EXISTS (SELECT 1 FROM pot_records p WHERE p.spendingTxid=w.txid)) AND COALESCE(json_extract(w.legacy_body,'$.trackerTerminal'),0) = 0 ORDER BY w.updated_at,w.txid LIMIT 2";

pub const AGE_INDEX: &str = "CREATE INDEX IF NOT EXISTS tx_words_age ON tx_words(updated_at,txid)";
