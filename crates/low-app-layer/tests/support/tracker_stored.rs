//! State has no trusted deserializer. Store replayable inputs and the
//! snapshots used by its evidence transitions, then recheck a restored mined
//! word against current CHAINTRACKS before exposing its verify path.
use bsv_tracker::{
    Evidence, Header, Hint, HintStatus, HostAction, Input, Params, Proof, State, Verdict, Word,
};
use low_app_layer::tracker_host::{served, HeaderSnapshot};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const CREATE: &str = "CREATE TABLE IF NOT EXISTS tx_words (txid TEXT PRIMARY KEY, state TEXT NOT NULL, updated_at INTEGER NOT NULL)";
pub const UPSERT: &str = "INSERT INTO tx_words (txid,state,updated_at) VALUES (?1,?2,?3) ON CONFLICT(txid) DO UPDATE SET state=excluded.state,updated_at=excluded.updated_at WHERE tx_words.updated_at <= excluded.updated_at";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Event {
    Build,
    Legacy {
        body: Value,
    },
    Arc {
        body: Value,
        observed: u64,
    },
    Seen {
        source: String,
        observed: u64,
    },
    Proof {
        hex: String,
        height: u32,
        hash: String,
        root: String,
        tip: u32,
    },
    Tick {
        now: u64,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub v: u8,
    pub txid: String,
    pub events: Vec<Event>,
}
impl Record {
    pub fn new(txid: &str) -> Self {
        Self {
            v: 1,
            txid: txid.to_ascii_lowercase(),
            events: vec![],
        }
    }
    pub fn replay(&self) -> Result<State, String> {
        if self.v != 1 || !low_app_layer::logic::valid_txid(&self.txid) {
            return Err("invalid tracker record".into());
        }
        let mut state = State::new(&self.txid);
        let params = Params { age_threshold: 60 };
        let empty = HeaderSnapshot {
            tip: Ok(0),
            headers: BTreeMap::new(),
        };
        for event in &self.events {
            let input = match event {
                Event::Legacy { .. } => continue,
                Event::Build => Input::Host(HostAction::Build),
                Event::Arc { body, observed } => Input::Hint(
                    Verdict::from_arc_body(&self.txid, body.to_string().as_bytes())
                        .map_err(|e| e.to_string())?
                        .into_hint("arcade", *observed),
                ),
                Event::Seen { source, observed } => {
                    Input::Hint(Hint::new(source, HintStatus::Seen, *observed))
                }
                Event::Tick { now } => Input::Host(HostAction::Tick(*now)),
                Event::Proof {
                    hex,
                    height,
                    hash,
                    root,
                    tip,
                } => {
                    let path = overlay_engine::beef_limits::merkle_path_from_hex(
                        hex,
                        overlay_engine::beef_limits::COURIER_PROOF_MAX_BYTES,
                    )
                    .map_err(|e| e.to_string())?;
                    if path.block_height != *height {
                        return Err("record proof height mismatch".into());
                    }
                    let proof = Proof::new(&self.txid, path).map_err(|e| e.to_string())?;
                    let snapshot = HeaderSnapshot {
                        tip: Ok(*tip),
                        headers: BTreeMap::from([(
                            *height,
                            Ok(Some(Header {
                                hash: hash.clone(),
                                merkle_root: root.clone(),
                            })),
                        )]),
                    };
                    state
                        .step(&params, &snapshot, Input::Evidence(Evidence::Proof(proof)))
                        .map_err(|e| e.to_string())?;
                    continue;
                }
            };
            state
                .step(&params, &empty, input)
                .map_err(|e| e.to_string())?;
        }
        Ok(state)
    }
    pub fn append_checked(&mut self, proof: Proof, headers: &HeaderSnapshot) -> Result<(), String> {
        use bsv_tracker::Headers;
        let checked = headers.check(proof).map_err(|e| e.to_string())?;
        self.events.push(Event::Proof {
            hex: checked.proof().path().to_hex(),
            height: checked.height(),
            hash: checked.header().hash.clone(),
            root: checked.root().into(),
            tip: headers.tip_height().map_err(|e| e.to_string())?,
        });
        Ok(())
    }
}

/// A failed current check keeps the tracker's word, and removes the usable
/// badge capability from the served view. It is never turned into absence.
pub fn rechecked(record: &Record, current: &HeaderSnapshot) -> Result<(State, Value), String> {
    let mut state = record.replay()?;
    let fault = if matches!(state.word(), Word::Mined(_)) {
        state
            .step(
                &Params { age_threshold: 60 },
                current,
                Input::Evidence(Evidence::Recheck),
            )
            .err()
    } else {
        None
    };
    let mut view = served::fields(&state);
    if let Some(e) = fault {
        view["verifyPath"] = Value::Null;
        view["cannotCheck"] = Value::String(e.to_string());
    }
    Ok((state, view))
}
