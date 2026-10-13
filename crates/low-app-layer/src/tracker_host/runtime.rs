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
        let f = views.get(id).cloned().unwrap_or_else(|| {
            let mut state = State::new(id);
            if let Some(source) = value["hintSource"].as_str() {
                let observed =
                    (value["observedAtMs"].as_f64().unwrap_or(0.0).max(0.0) / 1000.0) as u64;
                let empty = HeaderSnapshot {
                    tip: Ok(0),
                    headers: Default::default(),
                };
                state
                    .step(
                        &Params { age_threshold: 60 },
                        &empty,
                        Input::Hint(Hint::new(source, HintStatus::Unknown, observed)),
                    )
                    .expect("non-promoting courier hint");
            }
            super::served::fields(&state)
        });
        if let Some(obj) = value.as_object_mut() {
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
/// Shared read boundary: lookup faults never erase an available legacy body.
pub fn lookup_result<T, E>(result: Result<T, E>) -> Result<Option<T>, E> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(_) => {
            crate::courier::note_presence("tracker_lookup_faults_total");
            Ok(None)
        }
    }
}

pub async fn read_soft(
    env: &worker::Env,
    ids: &[String],
) -> worker::Result<Option<BTreeMap<String, Row>>> {
    lookup_result(stored::read(env, ids).await)
}

pub fn decorate_lookup(
    mut value: Value,
    subject: Option<&str>,
    views: Option<&BTreeMap<String, Value>>,
) -> String {
    if let Some(views) = views {
        decorate(&mut value, subject, views);
    }
    value.to_string()
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
    let rows = read_soft(env, &ids).await?;
    body_loaded(env, value, subject, rows).await
}
pub async fn body_loaded(
    env: &worker::Env,
    value: Value,
    subject: Option<&str>,
    rows: Option<BTreeMap<String, Row>>,
) -> worker::Result<String> {
    let views = match rows {
        Some(rows) => Some(views(env, &rows).await),
        None => None,
    };
    Ok(decorate_lookup(value, subject, views.as_ref()))
}
/// Owed snapshots already persist their word tuples in facts. Recheck those
/// proofs without another D1 lookup or a read-triggered fallback write.
pub async fn owed_body(env: &worker::Env, value: &mut Value) {
    fn collect(value: &Value, rows: &mut BTreeMap<String, Row>) {
        if let Some(obj) = value.as_object() {
            if let (Some(word), Some(id)) = (
                obj.get("spenderWord"),
                obj.get("coinWord").and_then(|c| c["txid"].as_str()),
            ) {
                let p = &word["verifyPath"];
                rows.insert(
                    id.to_string(),
                    Row {
                        txid: id.into(),
                        word: word["word"].as_str().unwrap_or("unknown").into(),
                        tier: word["tier"].as_str().unwrap_or("host").into(),
                        height: word["height"].as_f64(),
                        proof_root: p["root"].as_str().map(str::to_string),
                        proof_height: p["height"].as_f64(),
                        proof_hex: p["merklePath"].as_str().map(str::to_string),
                        header_hash: p["headerHash"].as_str().map(str::to_string),
                        hint_source: word["source"].as_str().map(str::to_string),
                        hint_at: word["at"].as_f64(),
                        updated_at: 0.0,
                        legacy_body: None,
                    },
                );
            }
            for (key, child) in obj {
                if key != "spenderWord" {
                    collect(child, rows);
                }
            }
        } else if let Some(a) = value.as_array() {
            for child in a {
                collect(child, rows);
            }
        }
    }
    fn replace(value: &mut Value, views: &BTreeMap<String, Value>) {
        if let Some(obj) = value.as_object_mut() {
            let id = obj
                .get("coinWord")
                .and_then(|c| c["txid"].as_str())
                .map(str::to_string);
            if let Some(id) = id {
                if let Some(word) = views.get(&id) {
                    let old_coin = obj["coinWord"]["word"].as_str().unwrap_or("pending");
                    let coin = if word["word"] == "stale" {
                        "suspended"
                    } else if word["word"] == "mined" && !word["verifyPath"].is_null() {
                        if old_coin == "lost" {
                            "lost"
                        } else {
                            "spent"
                        }
                    } else {
                        "pending"
                    };
                    obj.insert(
                        "coinWord".into(),
                        json!({"word":coin,"txid":id,"height":word["height"]}),
                    );
                    obj.insert("spenderWord".into(), word.clone());
                }
            }
            for (key, child) in obj.iter_mut() {
                if key != "spenderWord" {
                    replace(child, views);
                }
            }
        } else if let Some(a) = value.as_array_mut() {
            for child in a {
                replace(child, views);
            }
        }
    }
    let mut rows = BTreeMap::new();
    collect(value, &mut rows);
    // A recompute with a word-store fault persists only legacy facts. Do not
    // synthesize tracker decorations when that snapshot has no word tuples.
    if rows.is_empty() {
        return;
    }
    let views = views(env, &rows).await;
    replace(value, &views);
    decorate(value, None, &views);
}

#[derive(serde::Deserialize)]
struct BeefRow {
    beef: Option<Vec<u8>>,
}
/// Read held bytes only: absence is no evidence and never a terminal word.
pub async fn held_proof(env: &worker::Env, txid: &str) -> worker::Result<(bool, Option<Proof>)> {
    let db = env.d1("OVERLAY_DB")?;
    for sql in [
        "SELECT beef FROM pot_beefs WHERE txid = ?",
        "SELECT beef FROM transactions WHERE txid = ?",
    ] {
        if let Some(row) = db
            .prepare(sql)
            .bind(&[JsValue::from_str(txid)])?
            .counted_first::<BeefRow>()
            .await?
        {
            let proof = row
                .beef
                .and_then(|bytes| {
                    overlay_engine::beef_limits::transaction_from_beef(
                        &bytes,
                        Some(txid),
                        &overlay_engine::beef_limits::STORED_BEEF_LIMITS,
                    )
                    .ok()
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
    if kind == "tick" {
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
    let (held, proof) = if kind == "proof" {
        (true, None)
    } else {
        held_proof(env, txid).await?
    };
    let first_party = held
        || matches!(kind, "submit" | "accepted" | "seen")
        || old.as_ref().is_some_and(|row| {
            row.hint_source.as_deref() == Some("arcade")
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
            crate::routes::tx_any_external_leg(txid, "tracker-write", env).await;
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
    if kind == "callback" {
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
            if let Some(hint) = courier_presence_hint(&value, now) {
                state
                    .step(&params, &empty, Input::Hint(hint))
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
    if !checked && hook_may_reask(kind, state.reask().is_some()) {
        super::pacing::begin_tick(now / 900);
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
    let value: Value = serde_json::from_str(raw)?;
    let rows = read_soft(env, ids).await?;
    body_loaded(env, value, None, rows).await
}

/// Only the scheduled pass may refresh aged words, two subjects per tick.
pub const PUBLIC_ROWS_PER_MINUTE: usize = 0;

pub async fn tick(env: &worker::Env) -> worker::Result<()> {
    #[derive(serde::Deserialize)]
    struct Subject {
        txid: String,
    }
    let now = super::WorkerClock.now();
    let db = env.d1("OVERLAY_DB")?;
    // Catch up on the scheduled path only; no read gains a schema statement.
    db.prepare(stored::AGE_INDEX).counted_run().await?;
    db.prepare(stored::SWEEP)
        .bind(&[JsValue::from_f64(now.saturating_sub(8 * 86400) as f64)])?
        .counted_run()
        .await?;
    crate::courier::note_presence("tracker_stranger_sweep_pass_total");
    let rows = db.prepare(stored::DUE)
        .bind(&[JsValue::from_f64(now.saturating_sub(900) as f64)])?
        .counted_all().await?.results::<Subject>()?;
    super::pacing::begin_tick(now / 900);
    for row in rows {
        event(env, &row.txid, "tick", None, None).await?;
    }
    Ok(())
}

/// Host pacing for the owed WRITE event. Chain words are never age-polled.
/// The tracker decides the resulting Reask from Tick/SpendAttempt inputs.
pub fn owed_reask_due(row: &Row, now: u64) -> bool {
    matches!(
        row.word.as_str(),
        "unknown" | "built" | "announced" | "seen"
    ) && now.saturating_sub(row.updated_at as u64) >= 60
}

/// The spent-any memo plan; a tracked txid does not prove an outpoint pointer.
pub fn spent_any_memo_plan(
    probe_targets: &[(String, u32)],
    memos: &[crate::hops_view::ProbeMemo],
    now_ms: i64,
    _stored_words: &BTreeMap<String, Row>,
) -> crate::hops_view::ProbePlan {
    crate::hops_view::split_probe_targets(
        probe_targets,
        memos,
        now_ms,
        crate::hops_view::PROBE_MEMO_MAX_AGE_MS,
    )
}

pub fn courier_presence_hint(value: &Value, now: u64) -> Option<Hint> {
    let source = value["hintSource"]
        .as_str()
        .or_else(|| (value["present"] == true && value["source"] == "external").then_some("woc"))?;
    // 0.2.2 has no Announced hint. Unknown retains the courier source without promotion.
    Some(Hint::new(source, HintStatus::Unknown, now))
}

#[derive(serde::Deserialize)]
pub struct BackfillSubject {
    pub txid: String,
    pub beef: Option<Vec<u8>>,
    pub has_proof: f64,
    pub tracked: Option<String>,
}

/// Production page walker; only the header and write transports are supplied.
pub async fn walk_backfill_page<H, HF, W, WF>(
    rows: Vec<BackfillSubject>,
    mut after: String,
    now: u64,
    mut headers: H,
    mut write: W,
) -> Result<String, String>
where
    H: FnMut(u32) -> HF,
    HF: std::future::Future<Output = HeaderSnapshot>,
    W: FnMut(Row) -> WF,
    WF: std::future::Future<Output = Result<(), String>>,
{
    for subject in rows {
        crate::courier::note_presence("tracker_backfill_scanned_total");
        after = subject.txid.clone();
        if subject.tracked.is_some() || subject.has_proof == 0.0 {
            continue;
        }
        let proof = subject
            .beef
            .and_then(|bytes| {
                overlay_engine::beef_limits::transaction_from_beef(
                    &bytes,
                    Some(&subject.txid),
                    &overlay_engine::beef_limits::STORED_BEEF_LIMITS,
                )
                .ok()
            })
            .and_then(|tx| tx.merkle_path.clone())
            .and_then(|path| Proof::new(&subject.txid, path).ok());
        if let Some(proof) = proof {
            let headers = headers(proof.height()).await;
            let mut state = State::new(&subject.txid);
            if let Err(error) = state
                .step(
                    &Params { age_threshold: 60 },
                    &headers,
                    Input::Evidence(Evidence::Proof(proof)),
                )
            {
                let reason = match error {
                    bsv_tracker::CheckError::RootMismatch(_) => "root_mismatch",
                    bsv_tracker::CheckError::Unavailable(_) => "unavailable",
                    bsv_tracker::CheckError::Headers(_) => "headers",
                    _ => "invalid_proof",
                };
                crate::courier::note_presence("tracker_backfill_skipped_total");
                crate::courier::note_presence(&format!("tracker_backfill_skipped_{reason}_total"));
                continue;
            }
            write(Row::from_state(&state, now, None)).await?;
            crate::courier::note_presence("tracker_backfill_written_total");
        } else {
            crate::courier::note_presence("tracker_backfill_unreadable_total");
        }
    }
    Ok(after)
}

thread_local! { static BACKFILL_DONE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
/// Proof-only deploy convergence: one 64-subject indexed page per scheduled pass.
/// The page plus its tx_words PK join reads at most 64 + 64 rows.
pub async fn backfill(env: &worker::Env) -> worker::Result<()> {
    if BACKFILL_DONE.with(std::cell::Cell::get) {
        return Ok(());
    }
    #[derive(serde::Deserialize)]
    struct Cursor {
        cursor: String,
        complete: f64,
    }
    let db = env.d1("OVERLAY_DB")?;
    let cursor = db
        .prepare("SELECT cursor, complete FROM tracker_backfill WHERE version = ?1")
        .bind(&[JsValue::from_str(stored::BACKFILL_VERSION)])?
        .counted_first::<Cursor>()
        .await?;
    if cursor.as_ref().is_some_and(|c| c.complete != 0.0) {
        BACKFILL_DONE.with(|c| c.set(true));
        return Ok(());
    }
    let saved = cursor.map_or_else(|| "0:".into(), |c| c.cursor);
    let pots = saved.starts_with("1:");
    let mut after = saved[2..].to_string();
    let rows = db
        .prepare(if pots {
            stored::BACKFILL_POT_PAGE
        } else {
            stored::BACKFILL_PAGE
        })
        .bind(&[JsValue::from_str(&after)])?
        .counted_all()
        .await?
        .results::<BackfillSubject>()?;
    let page_done = rows.len() < 64;
    let complete = pots && page_done;
    after = walk_backfill_page(
        rows, after, super::WorkerClock.now(),
        |height| async move { super::headers(env, &[height]).await },
        |row| async move { stored::write(env, &row).await.map_err(|e| e.to_string()) },
    ).await.map_err(worker::Error::RustError)?;
    let saved = if page_done && !pots {
        "1:".to_string()
    } else {
        format!("{}:{after}", if pots { 1 } else { 0 })
    };
    db.prepare(stored::BACKFILL_SAVE)
        .bind(&[
            JsValue::from_str(stored::BACKFILL_VERSION),
            JsValue::from_str(&saved),
            JsValue::from_f64(if complete { 1.0 } else { 0.0 }),
        ])?
        .counted_run()
        .await?;
    if complete {
        BACKFILL_DONE.with(|c| c.set(true));
    }
    Ok(())
}

pub fn hook_may_reask(kind: &str, has_reask: bool) -> bool {
    kind == "tick" && has_reask
}

#[cfg(test)]
mod budget_pins {
    #[test]
    fn ns3_m4_a_read_with_no_row_writes_nothing() {
        let runtime = include_str!("runtime.rs");
        let body = &runtime[runtime.find("pub async fn body_loaded(").unwrap()
            ..runtime.find("pub async fn owed_body(").unwrap()];
        assert!(
            !body.contains("event("),
            "a read with no row still writes a fallback word"
        );
        let routes = include_str!("../routes.rs");
        let tx = &routes[routes.find("pub async fn tx_any(").unwrap()
            ..routes.find("struct BeefTrustRowKeyed").unwrap()];
        assert!(
            !tx.contains("runtime::event("),
            "tx-any read still writes a word"
        );
        let owed = &routes[routes.find("async fn owed_recompute_inner(").unwrap()
            ..routes.find("let spender_words =").unwrap()];
        assert!(
            !owed.contains("runtime::event("),
            "owed read still writes a word"
        );
    }
}
