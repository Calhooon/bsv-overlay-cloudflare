//! Worker orchestration: event writes and batched lookup reads.
use super::{
    stored::{self, Row},
    HeaderSnapshot,
};
use crate::d1_ledger::Counted;
use bsv_tracker::{
    Clock, Evidence, Hint, HintStatus, HostAction, Input, Params, Proof, Reask, State, Verdict,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use worker::wasm_bindgen::JsValue;

fn subjects(value: &Value, ids: &mut BTreeSet<String>) {
    match value {
        Value::Object(obj) => {
            for key in [
                "spendingTxid",
                "chainSpender",
                "spenderTxid",
                "settleTxid",
                "sweepTxid",
                "payTxid",
                "spendTxid",
            ] {
                if let Some(id) = obj
                    .get(key)
                    .and_then(Value::as_str)
                    .filter(|s| crate::logic::valid_txid(s))
                {
                    ids.insert(id.to_ascii_lowercase());
                }
            }
            if let Some(answers) = obj.get("answers").and_then(Value::as_object) {
                ids.extend(answers.keys().cloned());
            }
            for child in obj.values() {
                subjects(child, ids);
            }
        }
        Value::Array(a) => {
            for child in a {
                subjects(child, ids);
            }
        }
        _ => {}
    }
}
pub fn decorate(value: &mut Value, subject: Option<&str>, views: &BTreeMap<String, Value>) {
    fn tx(value: &mut Value, id: &str, views: &BTreeMap<String, Value>) {
        let f = views
            .get(id)
            .cloned()
            .unwrap_or_else(|| super::served::fields(&State::new(id)));
        if let Some(obj) = value.as_object_mut() {
            for k in ["word", "tier", "verifyPath"] {
                obj.insert(k.into(), f[k].clone());
            }
            obj.insert("tracker".into(), f);
        }
    }
    if let Some(id) = subject {
        tx(value, id, views);
    }
    match value {
        Value::Object(obj) => {
            if let Some(answers) = obj.get_mut("answers").and_then(Value::as_object_mut) {
                for (id, answer) in answers {
                    tx(answer, id, views);
                }
            }
            let id = [
                "spendingTxid",
                "chainSpender",
                "spenderTxid",
                "settleTxid",
                "sweepTxid",
                "payTxid",
                "spendTxid",
            ]
            .iter()
            .find_map(|k| obj.get(*k).and_then(Value::as_str))
            .map(str::to_ascii_lowercase);
            if let Some(id) = id {
                obj.insert(
                    "spenderWord".into(),
                    views
                        .get(&id)
                        .cloned()
                        .unwrap_or_else(|| super::served::fields(&State::new(&id))),
                );
            }
            for (key, child) in obj.iter_mut() {
                if key != "spenderWord" && key != "tracker" {
                    decorate(child, None, views);
                }
            }
        }
        Value::Array(a) => {
            for child in a {
                decorate(child, None, views);
            }
        }
        _ => {}
    }
}
pub async fn views(env: &worker::Env, rows: &BTreeMap<String, Row>) -> BTreeMap<String, Value> {
    let heights: BTreeSet<_> = rows
        .values()
        .filter(|r| r.word == "mined")
        .filter_map(|r| r.proof_height.map(|h| h as u32))
        .collect();
    let headers = if heights.is_empty() {
        HeaderSnapshot {
            tip: Ok(0),
            headers: Default::default(),
        }
    } else {
        super::headers(env, &heights.into_iter().collect::<Vec<_>>()).await
    };
    rows.iter()
        .map(|(id, row)| {
            let view = row.rechecked(&headers).unwrap_or_else(|e| {
                let mut v = super::served::fields(&State::new(id));
                v["cannotCheck"] = json!(e);
                v
            });
            (id.clone(), view)
        })
        .collect()
}
pub async fn body(env: &worker::Env, raw: &str, subject: Option<&str>) -> worker::Result<String> {
    let value: Value =
        serde_json::from_str(raw).map_err(|e| worker::Error::RustError(e.to_string()))?;
    let mut ids = BTreeSet::new();
    subjects(&value, &mut ids);
    if let Some(id) = subject {
        ids.insert(id.into());
    }
    let ids: Vec<_> = ids.into_iter().collect();
    let rows = stored::read(env, &ids).await?;
    body_loaded(env, value, subject, rows).await
}
pub async fn body_loaded(
    env: &worker::Env,
    mut value: Value,
    subject: Option<&str>,
    mut rows: BTreeMap<String, Row>,
) -> worker::Result<String> {
    let mut ids = BTreeSet::new();
    subjects(&value, &mut ids);
    if let Some(id) = subject {
        ids.insert(id.into());
    }

    for id in ids {
        if let std::collections::btree_map::Entry::Vacant(entry) = rows.entry(id) {
            let row = event(env, entry.key(), "fallback", None, None).await?;
            entry.insert(row);
        }
    }
    let views = views(env, &rows).await;
    decorate(&mut value, subject, &views);
    Ok(value.to_string())
}
#[derive(serde::Deserialize)]
struct BeefRow {
    beef: Option<String>,
}
/// Read held bytes only: absence is no evidence and never a terminal word.
pub async fn held_proof(env: &worker::Env, txid: &str) -> worker::Result<(bool, Option<Proof>)> {
    let db = env.d1("OVERLAY_DB")?;
    for sql in [
        "SELECT hex(beef) AS beef FROM pot_beefs WHERE txid = ?",
        "SELECT hex(beef) AS beef FROM transactions WHERE txid = ?",
    ] {
        if let Some(row) = db
            .prepare(sql)
            .bind(&[JsValue::from_str(txid)])?
            .counted_first::<BeefRow>()
            .await?
        {
            let proof = row
                .beef
                .and_then(|h| hex::decode(h).ok())
                .and_then(|bytes| {
                    bsv_rs::transaction::Transaction::from_beef(&bytes, Some(txid)).ok()
                })
                .and_then(|tx| tx.merkle_path.clone())
                .and_then(|path| Proof::new(txid, path).ok());
            return Ok((true, proof));
        }
    }
    Ok((false, None))
}
/// Each observed WRITE event feeds raw hints/proofs to the tracker. No
/// retire/refusal/absence can construct a chain rejection.
pub async fn event(
    env: &worker::Env,
    txid: &str,
    kind: &str,
    callback: Option<&Value>,
    legacy: Option<String>,
) -> worker::Result<Row> {
    let old = stored::read(env, &[txid.into()]).await?.remove(txid);
    if kind == "fallback" {
        if let Some(old) = old {
            return Ok(old);
        }
    }
    let mut legacy = legacy;
    let mut state = stored::seed_hint(old.as_ref(), txid);
    let params = Params { age_threshold: 60 };
    let empty = HeaderSnapshot {
        tip: Ok(0),
        headers: Default::default(),
    };
    let now = super::WorkerClock.now();
    if kind == "tick" || kind == "retire" {
        state
            .step(&params, &empty, Input::Host(HostAction::Tick(now)))
            .expect("clock input");
    }
    if kind == "tick"
        && matches!(
            state.word(),
            bsv_tracker::Word::Unknown | bsv_tracker::Word::Built
        )
    {
        state
            .step(&params, &empty, Input::Host(HostAction::SpendAttempt))
            .expect("owed input reask");
    }
    let (held, proof) = held_proof(env, txid).await?;
    let first_party = held
        || matches!(kind, "submit" | "accepted" | "seen")
        || old.as_ref().is_some_and(|row| {
            row.hint_source.as_deref() == Some("arcade")
                || row.word == "built"
                || row
                    .legacy_body
                    .as_ref()
                    .and_then(|body| serde_json::from_str::<Value>(body).ok())
                    .is_some_and(|body| {
                        matches!(body["source"].as_str(), Some("index" | "index+external"))
                    })
        });
    if matches!(kind, "fallback" | "tick") && !first_party && legacy.is_none() {
        let (observation, absence, claimed_height) =
            crate::routes::tx_any_external_leg(txid, "tracker-write").await;
        let mut answer = crate::txany::decide_tx_any(None, None, Some(&observation), absence);
        answer.claimed_height = claimed_height;
        legacy = Some(crate::txany::tx_any_body(txid, &answer));
    }
    if (held
        || kind == "seen"
        || kind == "accepted"
        || legacy.as_ref().is_some_and(|s| {
            serde_json::from_str::<Value>(s)
                .ok()
                .is_some_and(|v| v["rawHex"].is_string())
        }))
        && state.word() == &bsv_tracker::Word::Unknown
    {
        state
            .step(&params, &empty, Input::Host(HostAction::Build))
            .expect("host input");
    }
    if kind == "accepted" {
        state
            .step(
                &params,
                &empty,
                Input::Hint(Hint::new("arcade", HintStatus::Accepted, now)),
            )
            .expect("admission hint input");
    }
    if kind == "seen" {
        state
            .step(
                &params,
                &empty,
                Input::Hint(Hint::new("arcade", HintStatus::Seen, now)),
            )
            .expect("hint input");
    }
    let arc = crate::routes::arcade_url();
    // Public callback is a trigger; live GET owns its hint. A callback's
    // merkle path is independently checked evidence even if GET faults.
    if first_party || kind == "callback" {
        if let Some((200, bytes)) =
            crate::routes::provider_get("tracker-write", &format!("{arc}/tx/{txid}")).await
        {
            if let Ok(v) = Verdict::from_arc_body(txid, &bytes) {
                state
                    .step(&params, &empty, Input::Hint(v.into_hint("arcade", now)))
                    .expect("hint input");
            }
        }
    }
    if matches!(kind, "fallback" | "tick") {
        if let Some(value) = legacy
            .as_ref()
            .and_then(|s| serde_json::from_str::<Value>(s).ok())
        {
            if value["present"] == true && value["source"] == "external" {
                state
                    .step(
                        &params,
                        &empty,
                        Input::Hint(Hint::new("woc", HintStatus::Seen, now)),
                    )
                    .expect("hint input");
            }
        }
    }
    let pushed = callback
        .and_then(|v| v["merklePath"].as_str())
        .and_then(|h| {
            overlay_engine::beef_limits::merkle_path_from_hex(
                h,
                overlay_engine::beef_limits::COURIER_PROOF_MAX_BYTES,
            )
            .ok()
        })
        .and_then(|p| Proof::new(txid, p).ok());
    let mut checked = false;
    for proof in [pushed, proof].into_iter().flatten() {
        let headers = super::headers(env, &[proof.height()]).await;
        if state
            .step(&params, &headers, Input::Evidence(Evidence::Proof(proof)))
            .is_ok()
        {
            checked = true;
            break;
        }
    }
    if !checked && (kind == "fallback" || state.reask().is_some()) {
        let proof = super::execute_reask(
            txid,
            first_party,
            &arc,
            &Reask::Age,
            |url| async move { crate::routes::provider_get("tracker-write", &url).await },
            |height| async move { super::headers(env, &[height]).await },
        )
        .await;
        if let Ok(Some(proof)) = proof {
            let headers = super::headers(env, &[proof.height()]).await;
            if let Err(e) = state.step(&params, &headers, Input::Evidence(Evidence::Proof(proof))) {
                worker::console_warn!("[tracker-write] {txid}: {e}");
            }
        }
    }
    let row = stored::project(old.as_ref(), &state, now, legacy);
    stored::write(env, &row).await?;
    Ok(row)
}
pub async fn ingest(
    mut req: worker::Request,
    env: &worker::Env,
) -> worker::Result<worker::Response> {
    if !crate::internal_events::internal_bearer_ok(&req, env) {
        return worker::Response::error("Unauthorized", 401);
    }
    let frame: Value = req.json().await?;
    let id = frame["txid"]
        .as_str()
        .filter(|id| crate::logic::valid_txid(id))
        .ok_or_else(|| worker::Error::RustError("invalid tracker event txid".into()))?
        .to_ascii_lowercase();
    let kind = frame["kind"].as_str().unwrap_or("refresh");
    event(env, &id, kind, frame.get("body"), None).await?;
    worker::Response::from_json(&json!({"ok":true}))
}

/// Batch reads include stored foreign words even after the byte index has
/// dropped them. Unknown ids keep the existing batch fallback contract.
pub async fn batch_body(env: &worker::Env, raw: &str, ids: &[String]) -> worker::Result<String> {
    let mut value: Value = serde_json::from_str(raw)?;
    let rows = stored::read(env, ids).await?;
    for (id, row) in &rows {
        let answer = row
            .legacy_body
            .as_ref()
            .and_then(|s| serde_json::from_str::<Value>(s).ok())
            .unwrap_or_else(|| {
                crate::txany::tx_any_value(
                    id,
                    &crate::txany::TxAnyAnswer {
                        present: matches!(row.word.as_str(), "announced" | "seen" | "mined")
                            .then_some(true),
                        confirmed: Some(row.word == "mined"),
                        height: row.height.map(|h| h as u64),
                        ..Default::default()
                    },
                )
            });
        value["answers"][id] = answer;
    }
    if let Some(unknown) = value["unknown"].as_array_mut() {
        unknown.retain(|id| !id.as_str().is_some_and(|id| rows.contains_key(id)));
    }
    if let Some(reasons) = value["reasons"].as_object_mut() {
        reasons.retain(|id, _| !rows.contains_key(id));
    }
    body_loaded(env, value, None, rows).await
}

/// Host pacing for the owed WRITE event. Chain words are never age-polled.
/// The tracker decides the resulting Reask from Tick/SpendAttempt inputs.
pub fn owed_reask_due(row: &Row, now: u64) -> bool {
    matches!(
        row.word.as_str(),
        "unknown" | "built" | "announced" | "seen"
    ) && now.saturating_sub(row.updated_at as u64) >= 60
}
