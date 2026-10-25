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
    let mut value = value;
    let Some(rows) = rows else {
        return Ok(value.to_string());
    };
    let views = views(env, &rows).await;
    decorate(&mut value, subject, &views);
    // bsv-low #862 (L7-DEPTH): the host's spend depth beside each `tracker` the decoration served.
    stamp_spend_depth(&mut value, spend_depth(env).depth);
    Ok(value.to_string())
}
/// bsv-low #617 (L1): the outpoint routes' (`/utxo-status`, `/spent-any`) txids to read words for: every entry's
/// own creating txid (its `fundingWord`) beside the spender subjects [`body`] reads.
pub fn outpoint_ids(value: &Value) -> Vec<String> {
    let mut ids = BTreeSet::new();
    subjects(value, &mut ids);
    if let Some(entries) = value.as_array() {
        ids.extend(
            entries
                .iter()
                .filter_map(|e| e["txid"].as_str())
                .filter(|s| crate::logic::valid_txid(s))
                .map(str::to_ascii_lowercase),
        );
    }
    ids.into_iter().collect()
}

/// bsv-low #617 (L1): each outpoint entry of `/utxo-status` and `/spent-any` serves two words beside every legacy
/// field (additive, the legacy keys and values untouched):
/// - `fundingWord`: the tracker's tuple `{word, tier, height, verifyPath, ...}` for the entry's own creating txid (the
///   design's `networkSeen` successor; `unknown` with no row);
/// - `lock`: the coin word of the outpoint, `{word, txid, tier, height, verifyPath}`: `spent` when the recorded
///   spender's word is a CHECKED `mined` (its verify path served) or the index has PROVEN the spend
///   ([`index_proven_spend`]: tier `chain`, `verifiedBy` naming the proof, a null path, the index's proof read
///   first so no tracker word vetoes it), `suspended` when that word is `stale`, `locked`
///   for any other pointer (a recorded spender the chain has not confirmed), `unspent` for a known outpoint with no
///   spender (`unspent_word`: the index's `free` on `/utxo-status`, the couriers' `unspentAsFarAsAsked` on
///   `/spent-any`, with `unspent_tier`), and `unknown` where the route could not say. `lost(c)` needs an intended
///   spender, which a public outpoint read does not have, so it is never served here.
pub fn decorate_outpoints(
    value: &mut Value,
    views: &BTreeMap<String, Value>,
    unspent_word: &str,
    unspent_tier: &str,
) {
    let Some(entries) = value.as_array_mut() else {
        return;
    };
    let word_of = |id: &str| {
        let id = id.to_ascii_lowercase();
        views
            .get(&id)
            .cloned()
            .unwrap_or_else(|| super::served::fields(&State::new(&id)))
    };
    for entry in entries {
        let Some(txid) = entry["txid"].as_str().map(str::to_string) else {
            continue;
        };
        let funding = word_of(&txid);
        let spender = entry["spendingTxid"].as_str().map(str::to_ascii_lowercase);
        let lock = match (&entry["known"], &entry["spent"], spender) {
            (Value::Bool(true), Value::Bool(true), Some(s)) => {
                let w = word_of(&s);
                if w["word"] == "mined" && !w["verifyPath"].is_null() {
                    crate::owed::coin_word_value("spent", &s, &w)
                } else if let Some(by) = index_proven_spend(entry) {
                    json!({"word":"spent","txid":s,"tier":"chain","height":null,"verifyPath":null,"verifiedBy":by})
                } else if w["word"] == "stale" {
                    crate::owed::coin_word_value("suspended", &s, &w)
                } else {
                    crate::owed::coin_word_value("locked", &s, &w)
                }
            }
            (Value::Bool(true), Value::Bool(false), _) => {
                json!({"word":unspent_word,"txid":null,"tier":unspent_tier,"height":null,"verifyPath":null})
            }
            _ => {
                json!({"word":"unknown","txid":null,"tier":"host","height":null,"verifyPath":null})
            }
        };
        if let Some(obj) = entry.as_object_mut() {
            obj.insert("fundingWord".into(), funding);
            obj.insert("lock".into(), lock);
        }
    }
}

/// The W2-WORDS lens's MEDIUM-1 (NS-3: the index's verified proof decides first): the checked proof an outpoint
/// entry's `spentConfirmed: true` rests on, named as `spentConfirmedSource` names it, or `None` when it rests on none.
/// `/utxo-status` serves no source: its `spentConfirmed` is `pot_records.spentConfirmed`, latched only when the
/// overlay SPV-verified the spend's bump (`index-proof`). `/spent-any` names its source: a checked proof
/// (`index-proof`, `tracker-mined`, `txany-proof`) counts; `courier` (WoC's status) and `null` do not.
fn index_proven_spend(entry: &Value) -> Option<&'static str> {
    use crate::results::{
        SPENT_CONFIRMED_SOURCE_INDEX_PROOF as INDEX,
        SPENT_CONFIRMED_SOURCE_TRACKER_MINED as TRACKER,
        SPENT_CONFIRMED_SOURCE_TXANY_PROOF as TXANY,
    };
    if entry["spentConfirmed"] != true {
        return None;
    }
    match entry.get("spentConfirmedSource") {
        None => Some(INDEX),
        Some(src) => [INDEX, TRACKER, TXANY].into_iter().find(|p| src == *p),
    }
}

/// bsv-low #617 (L1): `/utxo-status`'s body: [`body`]'s spender words plus each entry's `fundingWord` and `lock`
/// ([`decorate_outpoints`], the index's `free`). A word-store read fault serves the legacy body unchanged.
pub async fn outpoint_body(env: &worker::Env, raw: &str) -> worker::Result<String> {
    let mut value: Value =
        serde_json::from_str(raw).map_err(|e| worker::Error::RustError(e.to_string()))?;
    let ids = outpoint_ids(&value);
    let Some(rows) = read_soft(env, &ids).await? else {
        return Ok(value.to_string());
    };
    let views = views(env, &rows).await;
    decorate(&mut value, None, &views);
    decorate_outpoints(&mut value, &views, "free", "host");
    // bsv-low #862 (L7-DEPTH): the host's spend depth beside each entry's `lock`.
    stamp_spend_depth(&mut value, spend_depth(env).depth);
    Ok(value.to_string())
}

/// bsv-low #862 (L7-DEPTH, the L6 app-layer half): the confirmations this host requires before a wallet credits a
/// coin on a served chain word (a refund, a hop sweep, a concede payout read off `lock`, `spenderWord` or `tracker`;
/// R3 of the wallet charter). 1 is the floor: a checked `mined` word at any height.
pub const SPEND_DEPTH_FLOOR: u64 = 1;
/// The largest depth this host serves (the L7-DEPTH lens's MEDIUM-1): ONE bound shared with the client, whose
/// `SERVED_SPEND_DEPTH_MAX` (bsv-low `app/src/lib/chainReads.ts`, `parseServedSpendDepth`) bounds a served depth to
/// 1 ..= 144 and reads a value past it as not served, the floor. A var past it is MALFORMED here too: the floor is
/// served and counted, so the host never serves a depth the client would read as 1 while `/health` said more.
pub const SPEND_DEPTH_MAX: u64 = 144;
/// The render var the depth is read from (`[vars]` and `[env.beta.vars]` of `wrangler.toml`).
pub const SPEND_DEPTH_VAR: &str = "SPEND_DEPTH";
/// Counted once per isolate when the var is missing or malformed, past [`SPEND_DEPTH_MAX`] included (the depth served
/// is then the floor).
pub const SPEND_DEPTH_MALFORMED_COUNTER: &str = "spend_depth_var_malformed_total";

/// The depth in force and whether a well-formed var gave it (`false`: the floor, the var missing or malformed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpendDepth {
    pub depth: u64,
    pub from_var: bool,
}

/// The var's value: plain decimal digits (surrounding whitespace allowed), from the floor to [`SPEND_DEPTH_MAX`].
/// Anything else (absent, empty, `0`, a sign, a fraction, a hex form, past 144) is the floor.
pub fn spend_depth_of(raw: Option<&str>) -> SpendDepth {
    raw.map(str::trim)
        .filter(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|s| s.parse::<u32>().ok())
        .map(u64::from)
        .filter(|d| (SPEND_DEPTH_FLOOR..=SPEND_DEPTH_MAX).contains(d))
        .map(|depth| SpendDepth {
            depth,
            from_var: true,
        })
        .unwrap_or(SpendDepth {
            depth: SPEND_DEPTH_FLOOR,
            from_var: false,
        })
}

thread_local! {
    static SPEND_DEPTH_MALFORMED_NOTED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// [`spend_depth_of`], counting [`SPEND_DEPTH_MALFORMED_COUNTER`] the first time this isolate meets a missing or
/// malformed var (never again in it, whatever the read rate).
pub fn spend_depth_noted(raw: Option<&str>) -> SpendDepth {
    let d = spend_depth_of(raw);
    if !d.from_var && !SPEND_DEPTH_MALFORMED_NOTED.with(|n| n.replace(true)) {
        crate::courier::note_presence(SPEND_DEPTH_MALFORMED_COUNTER);
    }
    d
}

/// The render's depth (a missing binding of the var is the floor, counted).
pub fn spend_depth(env: &worker::Env) -> SpendDepth {
    let raw = env.var(SPEND_DEPTH_VAR).ok().map(|v| v.to_string());
    spend_depth_noted(raw.as_deref())
}

/// The additive field: `spendDepth` beside `tracker` on the subject and on every `answers[id]` that carries one
/// (`/tx-any`), and beside `lock` on every outpoint entry that carries one (`/utxo-status`, `/spent-any`). An object
/// the word decoration did not reach gains nothing, so a legacy body served on a word-store fault is byte-identical,
/// and a decorated one is its old shape plus exactly this one key.
pub fn stamp_spend_depth(value: &mut Value, depth: u64) {
    fn beside(obj: &mut serde_json::Map<String, Value>, key: &str, depth: u64) {
        if obj.contains_key(key) {
            obj.insert("spendDepth".into(), json!(depth));
        }
    }
    match value {
        Value::Object(obj) => {
            if let Some(answers) = obj.get_mut("answers").and_then(Value::as_object_mut) {
                for answer in answers.values_mut().filter_map(Value::as_object_mut) {
                    beside(answer, "tracker", depth);
                }
            }
            beside(obj, "tracker", depth);
        }
        Value::Array(entries) => {
            for entry in entries.iter_mut().filter_map(Value::as_object_mut) {
                beside(entry, "lock", depth);
            }
        }
        _ => {}
    }
}

/// `/health.spendDepth`: the depth in force, where it came from (`var` or `floor`), the floor and the var as set.
pub fn spend_depth_health_json(raw: Option<&str>) -> Value {
    let d = spend_depth_of(raw);
    json!({
        "depth": d.depth,
        "source": if d.from_var { "var" } else { "floor" },
        "floor": SPEND_DEPTH_FLOOR,
        "var": raw,
    })
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
                        crate::owed::coin_word_value(coin, &id, word),
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
/// [`held_proof`]'s reads, in its order: the first row found is the subject's held bytes.
pub const HELD_PROOF_READS: [&str; 2] = [
    "SELECT beef FROM pot_beefs WHERE txid = ?",
    "SELECT beef FROM transactions WHERE txid = ?",
];
/// PURE: the subject's own proof out of its held BEEF, if the bytes carry one.
pub fn proof_of_held_beef(txid: &str, beef: Option<Vec<u8>>) -> Option<Proof> {
    beef.and_then(|bytes| {
        overlay_engine::beef_limits::transaction_from_beef(
            &bytes,
            Some(txid),
            &overlay_engine::beef_limits::STORED_BEEF_LIMITS,
        )
        .ok()
    })
    .and_then(|tx| tx.merkle_path.clone())
    .and_then(|path| Proof::new(txid, path).ok())
}
/// Read held bytes only: absence is no evidence and never a terminal word.
pub async fn held_proof(env: &worker::Env, txid: &str) -> worker::Result<(bool, Option<Proof>)> {
    let db = env.d1("OVERLAY_DB")?;
    for sql in HELD_PROOF_READS {
        if let Some(row) = db
            .prepare(sql)
            .bind(&[JsValue::from_str(txid)])?
            .counted_first::<BeefRow>()
            .await?
        {
            return Ok((true, proof_of_held_beef(txid, row.beef)));
        }
    }
    Ok((false, None))
}
/// PURE (bsv-low #621, L5): the source and time of a `seen` event's hint. The overlay's latch ships
/// `{source, at}` (`ops::latch_network_seen`): a broadcaster's name (`arcade`, `taal`, `gorillapool`; anything else,
/// or none from an older overlay, is `arcade`, the pre-#621 attribution) and its stamp in ms, read as seconds and
/// never later than `now` (a stamp ahead of the clock is the clock's).
pub fn seen_hint_source(body: Option<&Value>, now: u64) -> (&'static str, u64) {
    let source = match body.and_then(|b| b["source"].as_str()) {
        Some("taal") => "taal",
        Some("gorillapool") => "gorillapool",
        _ => "arcade",
    };
    let at = body
        .and_then(|b| b["at"].as_u64())
        .map(|ms| (ms / 1000).min(now))
        .unwrap_or(now);
    (source, at)
}

/// PURE (bsv-low #757): the host and hint inputs of a held subject's event, before any proof: `Build` when the word
/// is `unknown` and the subject has bytes (`bytes`: held, or a legacy body carrying its raw hex) or the event is a
/// broadcaster's (`seen`, `accepted`), then the event's own hint (`accepted`: Arcade's accept; `seen`: the latch's
/// broadcaster at its time, [`seen_hint_source`]).
pub fn event_hints(state: &mut State, kind: &str, bytes: bool, callback: Option<&Value>, now: u64) {
    let params = Params { age_threshold: 60 };
    let empty = HeaderSnapshot {
        tip: Ok(0),
        headers: Default::default(),
    };
    if (bytes || kind == "seen" || kind == "accepted")
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
        // bsv-low #621 (L5): the hint names the broadcaster the overlay's latch named, at its time
        let (source, observed) = seen_hint_source(callback, now);
        state
            .step(
                &params,
                &empty,
                Input::Hint(Hint::new(source, HintStatus::Seen, observed)),
            )
            .expect("hint input");
    }
}

/// PURE (bsv-low #757): the merkle path an event carries (the overlay's `proof` event from a checked bump, an Arcade
/// callback's), bounded as a courier proof; it is evidence only once checked against our header.
pub fn pushed_proof(txid: &str, callback: Option<&Value>) -> Option<Proof> {
    callback
        .and_then(|v| v["merklePath"].as_str())
        .and_then(|h| {
            overlay_engine::beef_limits::merkle_path_from_hex(
                h,
                overlay_engine::beef_limits::COURIER_PROOF_MAX_BYTES,
            )
            .ok()
        })
        .and_then(|p| Proof::new(txid, p).ok())
}

/// The event boundary runs effects only for an authoritative subject.
pub async fn event_for_subject<F, Fut>(
    held: bool,
    admitted: bool,
    apply: F,
) -> worker::Result<Option<Row>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = worker::Result<Row>>,
{
    if !held && !admitted {
        return Ok(None);
    }
    apply().await.map(Some)
}

/// Each observed WRITE event feeds raw hints/proofs to the tracker. No
/// retire/refusal/absence can construct a chain rejection.
pub async fn event(
    env: &worker::Env,
    txid: &str,
    kind: &str,
    callback: Option<&Value>,
    legacy: Option<String>,
) -> worker::Result<Option<Row>> {
    let held = live_hold(env, txid).await?;
    event_for_subject(held, kind == "submit", || {
        event_held(env, txid, kind, callback, legacy, held)
    })
    .await
}

/// A retained pot BEEF or an Arcade source is bytes, never live authority. bsv-low #757: a live hold is the engine's
/// `transactions` row OR the pot index's record of the subject as a pot or hop coin's spender ([`stored::LIVE_HOLD`],
/// pinned equal to the overlay's `tracker_events::LIVE_HOLD_SQL`, its `note_held`): a settle never has a
/// `transactions` row. The sweep and the scheduled refresh spare a recorded pot or hop spender only while its word is
/// not yet mined or terminal ([`stored::SWEEP`], [`stored::DUE`]).
pub async fn live_hold(env: &worker::Env, txid: &str) -> worker::Result<bool> {
    #[derive(serde::Deserialize)]
    struct Held {
        held: f64,
    }
    Ok(env
        .d1("OVERLAY_DB")?
        .prepare(stored::LIVE_HOLD)
        .bind(&[JsValue::from_str(txid)])?
        .counted_first::<Held>()
        .await?
        .is_some_and(|r| r.held == 1.0))
}

async fn event_held(
    env: &worker::Env,
    txid: &str,
    kind: &str,
    callback: Option<&Value>,
    legacy: Option<String>,
    live: bool,
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
    let first_party = live || kind == "submit";
    let terminal = std::cell::Cell::new(None);
    if matches!(kind, "fallback" | "tick") && !first_party && legacy.is_none() {
        let (observation, absence, claimed_height) =
            crate::routes::tx_any_external_leg(txid, "tracker-write", env).await;
        let mut answer = crate::txany::decide_tx_any(None, None, Some(&observation), absence);
        answer.claimed_height = claimed_height;
        legacy = Some(crate::txany::tx_any_body(txid, &answer));
    }
    let raw_bytes = legacy.as_ref().is_some_and(|s| {
        serde_json::from_str::<Value>(s)
            .ok()
            .is_some_and(|v| v["rawHex"].is_string())
    });
    event_hints(&mut state, kind, held || raw_bytes, callback, now);
    let arc = crate::routes::arcade_url();
    // Public callback is a trigger; live GET owns its hint. A callback's
    // merkle path is independently checked evidence even if GET faults.
    if kind == "callback" {
        if let Some((200, bytes)) =
            crate::routes::provider_get("tracker-write", &format!("{arc}/tx/{txid}")).await
        {
            if let Ok(v) = Verdict::from_arc_body(txid, &bytes) {
                let hint = v.into_hint("arcade", now);
                terminal.set(terminal_word(&hint));
                state
                    .step(&params, &empty, Input::Hint(hint))
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
    let pushed = pushed_proof(txid, callback);
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
            |url| {
                let terminal = &terminal;
                let arc = &arc;
                async move {
                    let response = crate::routes::provider_get("tracker-write", &url).await;
                    if url.starts_with(arc) {
                        if let Some((200, bytes)) = &response {
                            if let Ok(v) = Verdict::from_arc_body(txid, bytes) {
                                terminal.set(terminal_word(&v.into_hint("arcade", now)));
                            }
                        }
                    }
                    response
                }
            },
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
    let mut row = stored::project(old.as_ref(), &state, now, legacy);
    mark_terminal(&mut row, old.as_ref(), terminal.get());
    let tip = crate::routes::chaintracks_present_height_env(env, "owed")
        .await
        .ok();
    let judge =
        crate::owed_judge::D1Judge::new(env, tip, crate::routes::written_off_before_ms_env(env))?;
    let report = crate::owed_judge::judge_word_write(&judge, &row)
        .await
        .map_err(worker::Error::RustError)?;
    for (_, error) in report.faults {
        worker::console_warn!("[owed] word judgement deferred: {error}");
    }
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
    let held = event(env, &id, kind, frame.get("body"), None)
        .await?
        .is_some();
    if !held {
        note_unheld();
    }
    worker::Response::from_json(&ingest_answer(held))
}

/// bsv-low #660 (B-OWED-3-FOLD, the lens's L2): a frame whose subject the live hold does not name (and that is not
/// the gated `submit`) writes nothing. It was answered `{"ok":true}` and counted `shipped` at the overlay, so no side
/// named the drop. Now counted here (mirrored on the overlay's `ops_counters`) and answered `held: false`, which the
/// overlay counts `tracker_events_<source>_unheld_total`.
pub const COUNTER_TRACKER_EVENT_UNHELD: &str = "applayer_tracker_event_unheld_total";

/// The mirrored counter names of the word events (the observability bar's seed list).
pub fn tracker_mirrored_counter_names() -> Vec<String> {
    vec![
        COUNTER_TRACKER_EVENT_UNHELD.to_string(),
        SPEND_DEPTH_MALFORMED_COUNTER.to_string(),
    ]
}

/// Count one unheld frame (B-OWED-3-FOLD L2).
pub fn note_unheld() {
    crate::courier::note_presence(COUNTER_TRACKER_EVENT_UNHELD);
}

/// PURE (B-OWED-3-FOLD L2): `/internal/tx-word`'s answer, `held` whether the frame was applied (written).
pub fn ingest_answer(held: bool) -> Value {
    json!({"ok": true, "held": held})
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
    let rows = db
        .prepare(stored::DUE)
        .bind(&[JsValue::from_f64(now.saturating_sub(900) as f64)])?
        .counted_all()
        .await?
        .results::<Subject>()?;
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
    // 0.4.0 still has no Announced hint. Unknown retains the courier source without promotion.
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
        let previous = after.clone();
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
            if let Err(error) = state.step(
                &Params { age_threshold: 60 },
                &headers,
                Input::Evidence(Evidence::Proof(proof)),
            ) {
                if matches!(
                    error,
                    bsv_tracker::CheckError::Unavailable(_) | bsv_tracker::CheckError::Headers(_)
                ) {
                    crate::courier::note_presence("tracker_backfill_header_faults_total");
                    return Ok(previous);
                }
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

/// bsv-low #778, #781 (B-FLEET-4): the backfill's wall inside the scheduled pass. Every CHAINTRACKS await of a page
/// is raced against it (less [`BACKFILL_SAVE_RESERVE_MS`], kept for the cursor's save), so no header answer can hold
/// the tracker task past it: before, `super::headers` was awaited with no timeout, ahead of the chain-event intake's
/// own 25 s (`chain_events::WALL_MS`), and a pass could skip the intake silently (#778, beta 14:15:47Z, wall 66 s).
/// 20 s: the backfill plus the intake's 25 s stay inside the drain task's 60 s (`pot_hook::DRAIN_WALL_MS`).
pub const BACKFILL_WALL_MS: u64 = 20_000;

/// bsv-low #778 (B-FLEET-4): what the backfill's wall leaves after its last CHAINTRACKS await, for the cursor's save.
pub const BACKFILL_SAVE_RESERVE_MS: u64 = 3_000;

/// bsv-low #778 (B-FLEET-4): the snapshot a header await the deadline dropped answers: no tip, so the page stops at
/// that subject exactly as on a CHAINTRACKS fault (`tracker_backfill_header_faults_total`) and resumes there next pass.
pub fn deadline_snapshot() -> HeaderSnapshot {
    HeaderSnapshot {
        tip: Err(bsv_tracker::HeaderError(
            "the backfill's deadline fell before CHAINTRACKS answered".into(),
        )),
        headers: std::collections::BTreeMap::new(),
    }
}

/// bsv-low #778 (B-FLEET-4): [`walk_backfill_page`] with each CHAINTRACKS await raced against `deadline` (less its
/// reserve). A dropped await is counted `tracker_backfill_header_deadline_total` and answers [`deadline_snapshot`]:
/// the page stops at that subject, its cursor is the one before it, and the save runs in the reserve.
pub async fn walk_backfill_page_within<H, HF, W, WF>(
    rows: Vec<BackfillSubject>,
    after: String,
    now: u64,
    deadline: &crate::pot_hook::Deadline,
    mut headers: H,
    write: W,
) -> Result<String, String>
where
    H: FnMut(u32) -> HF,
    HF: std::future::Future<Output = HeaderSnapshot>,
    W: FnMut(Row) -> WF,
    WF: std::future::Future<Output = Result<(), String>>,
{
    walk_backfill_page(
        rows,
        after,
        now,
        |height| {
            let answer = headers(height);
            async move {
                match deadline.run_reserving(answer).await {
                    Some(snapshot) => snapshot,
                    None => {
                        crate::courier::note_presence("tracker_backfill_header_deadline_total");
                        deadline_snapshot()
                    }
                }
            }
        },
        write,
    )
    .await
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
    let deadline = crate::routes::worker_deadline(BACKFILL_WALL_MS, BACKFILL_SAVE_RESERVE_MS);
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
    let short_page = rows.len() < 64;
    let page_end = rows
        .last()
        .map_or_else(|| after.clone(), |row| row.txid.clone());
    after = walk_backfill_page_within(
        rows,
        after,
        super::WorkerClock.now(),
        &deadline,
        |height| async move { super::headers(env, &[height]).await },
        |row| async move { stored::write(env, &row).await.map_err(|e| e.to_string()) },
    )
    .await
    .map_err(worker::Error::RustError)?;
    let page_done = short_page && after == page_end;
    let complete = pots && page_done;
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

pub fn terminal_hint(hint: &Hint) -> bool {
    terminal_word(hint).is_some()
}

/// PURE (bsv-low #598, the B-OWED-2 lens's HIGH-1): the refusal a terminal Arcade hint names, `rejected` (REJECTED,
/// INVALID, MALFORMED) or `conflicted` (DOUBLE_SPEND_ATTEMPTED, UTXO_SPENT, or a competitor named beside an accepting
/// word); `None` for every other hint. A hint, never a chain word: the tracker keeps the word (NS-3).
pub fn terminal_word(hint: &Hint) -> Option<&'static str> {
    match hint.status {
        HintStatus::Rejected { .. } => Some("rejected"),
        HintStatus::DoubleSpend { .. } => Some("conflicted"),
        _ => None,
    }
}

/// PURE (bsv-low #757; #598): the terminal mark of the stored row, the producer `/owed` reads for a latched funding
/// the network refused. A terminal Arcade hint (`terminal`) on a word that is not mined sets `trackerTerminal: true`
/// and `trackerTerminalWord` ([`terminal_word`]) in the row's legacy body (the old row's body kept otherwise, as the
/// UPSERT's `COALESCE` keeps it); a mined word clears both. A later non-terminal hint leaves the mark as it stands
/// (the body is not rewritten), so the mark ends only at a checked proof. [`stored::Row::view`] serves it.
pub fn mark_terminal(row: &mut Row, old: Option<&Row>, terminal: Option<&'static str>) {
    if terminal.is_none() && row.word != "mined" {
        return;
    }
    let mut body = row
        .legacy_body
        .as_deref()
        .or_else(|| old.and_then(|r| r.legacy_body.as_deref()))
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .unwrap_or_else(|| json!({}));
    let marked = terminal.filter(|_| row.word != "mined");
    body["trackerTerminal"] = json!(marked.is_some());
    match (marked, body.as_object_mut()) {
        (Some(word), _) => body["trackerTerminalWord"] = json!(word),
        (None, Some(obj)) => {
            obj.remove("trackerTerminalWord");
        }
        (None, None) => {}
    }
    row.legacy_body = Some(body.to_string());
}

#[cfg(test)]
mod seen_hint_pins {
    use super::seen_hint_source;
    use serde_json::json;

    /// bsv-low #621 (L5): the `seen` hint names the broadcaster the overlay's latch named, at its stamp (ms read as
    /// seconds, never after now); an older overlay's event (no body) or a name that is not a broadcaster's is
    /// Arcade, the attribution before the pair.
    #[test]
    fn i621_the_seen_hint_names_the_latchs_broadcaster_and_time() {
        let now = 1_790_000_100;
        let b = json!({"source": "taal", "at": 1_790_000_000_123_u64});
        assert_eq!(seen_hint_source(Some(&b), now), ("taal", 1_790_000_000));
        let b = json!({"source": "gorillapool", "at": 9_999_999_999_999_u64});
        assert_eq!(
            seen_hint_source(Some(&b), now),
            ("gorillapool", now),
            "a stamp ahead of the clock is the clock's"
        );
        assert_eq!(seen_hint_source(None, now), ("arcade", now));
        let b = json!({"source": "woc", "at": 1_u64});
        assert_eq!(
            seen_hint_source(Some(&b), now).0,
            "arcade",
            "a courier never names a seen hint"
        );
    }
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
            ..routes
                .find("let mut rows = derive_owed_rows(&inputs);")
                .unwrap()];
        assert!(
            !owed.contains("runtime::event("),
            "owed read still writes a word"
        );
    }
}
