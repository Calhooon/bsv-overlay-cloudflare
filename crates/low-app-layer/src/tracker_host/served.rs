//! Additive rendering: the State is the only producer of the new word.
use bsv_tracker::{State, Word};
use serde_json::{json, Value};
use std::collections::BTreeMap;

pub fn fields(state: &State) -> Value {
    let (word, tier) = match state.word() {
        Word::Unknown => ("unknown", "host"),
        Word::Built => ("built", "host"),
        Word::Abandoned => ("abandoned", "host"),
        Word::Announced => ("announced", "hint"),
        Word::Seen => ("seen", "hint"),
        Word::Mined(_) => ("mined", "chain"),
        Word::Stale => ("stale", "chain"),
        Word::Rejected { .. } => ("rejected", "chain"),
        Word::Conflicted { .. } => ("conflicted", "chain"),
    };
    let verify = match state.word() {
        Word::Mined(m) if !state.suspect() => Some(
            json!({"root":m.checked().root(),"height":m.height(),"headerHash":m.checked().header().hash,"merklePath":m.checked().proof().path().to_hex()}),
        ),
        _ => None,
    };
    let hints: Vec<_> = state
        .hints()
        .iter()
        .map(|h| json!({"source":h.source,"status":format!("{:?}",h.status),"observed":h.observed}))
        .collect();
    let mut v = json!({"word":word,"tier":tier,"height":state.word().height(),"verifyPath":verify,"suspect":state.suspect(),"hints":hints});
    match state.word() {
        Word::Rejected { reason } => v["reason"] = json!(reason),
        Word::Conflicted { competitors } => v["competitors"] = json!(competitors),
        _ => {}
    }
    v
}

/// Preserve every legacy field and type. `tracker` is the complete tuple;
/// top-level word/tier/verifyPath are convenient additive projections.
pub fn transaction(value: &mut Value, state: &State) {
    let f = fields(state);
    if let Some(obj) = value.as_object_mut() {
        for key in ["word", "tier", "verifyPath"] {
            obj.insert(key.into(), f[key].clone());
        }
        obj.insert("tracker".into(), f);
    }
}

/// The subject of a spender word is the spender, never the creating txid.
pub fn spenders(value: &mut Value, states: &BTreeMap<String, State>) {
    match value {
        Value::Object(obj) => {
            let id = ["spendingTxid", "chainSpender", "spenderTxid"]
                .iter()
                .find_map(|key| obj.get(*key).and_then(Value::as_str))
                .map(str::to_ascii_lowercase);
            if let Some(id) = id {
                let state = states.get(&id).cloned().unwrap_or_else(|| State::new(&id));
                obj.insert("spenderWord".into(), fields(&state));
            }
            for child in obj.values_mut() {
                spenders(child, states);
            }
        }
        Value::Array(a) => {
            for child in a {
                spenders(child, states);
            }
        }
        _ => {}
    }
}

/// All production body wiring goes through this boundary. Missing subjects
/// have the tracker's birth word; legacy flags never synthesize evidence.
pub fn body(
    raw: &str,
    transaction_subject: Option<&str>,
    states: &BTreeMap<String, State>,
) -> String {
    let mut value: Value = serde_json::from_str(raw).expect("production JSON builder");
    if let Some(id) = transaction_subject {
        let state = states.get(id).cloned().unwrap_or_else(|| State::new(id));
        transaction(&mut value, &state);
    }
    if let Some(answers) = value.get_mut("answers").and_then(Value::as_object_mut) {
        for (id, answer) in answers {
            let state = states.get(id).cloned().unwrap_or_else(|| State::new(id));
            transaction(answer, &state);
        }
    }
    spenders(&mut value, states);
    value.to_string()
}
