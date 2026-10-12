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
pub const UPSERT: &str = "INSERT INTO tx_words (txid,word,tier,height,proof_root,proof_height,proof_hex,header_hash,hint_source,hint_at,updated_at,legacy_body) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12) ON CONFLICT(txid) DO UPDATE SET word=excluded.word,tier=excluded.tier,height=excluded.height,proof_root=excluded.proof_root,proof_height=excluded.proof_height,proof_hex=excluded.proof_hex,header_hash=excluded.header_hash,hint_source=excluded.hint_source,hint_at=excluded.hint_at,updated_at=excluded.updated_at,legacy_body=COALESCE(excluded.legacy_body,tx_words.legacy_body) WHERE tx_words.updated_at <= excluded.updated_at";
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
            json!({"root":self.proof_root,"height":self.proof_height,"headerHash":self.header_hash,"merklePath":self.proof_hex})
        } else {
            Value::Null
        };
        json!({"word":self.word,"tier":self.tier,"height":self.height,"verifyPath":verify,"hintSource":self.hint_source,"hintAt":self.hint_at})
    }
    /// One retained proof seeds a fresh State against its recorded snapshot;
    /// the only current evidence input is Recheck. No historical input replay.
    pub fn rechecked(&self, current: &HeaderSnapshot) -> Result<Value, String> {
        if self.word != "mined" {
            return Ok(self.view());
        }
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
        let params = Params { age_threshold: 60 };
        state
            .step(&params, &old, Input::Evidence(Evidence::Proof(proof)))
            .map_err(|e| e.to_string())?;
        let fault = state
            .step(&params, current, Input::Evidence(Evidence::Recheck))
            .err();
        let mut view = served::fields(&state);
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
pub async fn read(env: &worker::Env, ids: &[String]) -> worker::Result<BTreeMap<String, Row>> {
    let mut rows = BTreeMap::new();
    if ids.is_empty() {
        return Ok(rows);
    }
    let db = env.d1("OVERLAY_DB")?;
    for chunk in ids.chunks(90) {
        let binds: Vec<_> = chunk.iter().map(|id| JsValue::from_str(id)).collect();
        for row in db
            .prepare(read_sql(chunk.len()))
            .bind(&binds)?
            .counted_all()
            .await?
            .results::<Row>()?
        {
            rows.insert(row.txid.clone(), row);
        }
    }
    Ok(rows)
}
pub async fn write(env: &worker::Env, row: &Row) -> worker::Result<()> {
    let value = serde_json::to_value(row).map_err(|e| worker::Error::RustError(e.to_string()))?;
    let binds: Vec<_> = [
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
    ]
    .iter()
    .map(|k| match &value[*k] {
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
