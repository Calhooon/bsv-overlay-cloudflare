//! bsv-low #617 (LOW words map L1): the four routes `/tx-any`, `/spent-any`, `/utxo-status` and `/owed` serve the
//! tracker's tuple `{word, tier, height, verifyPath}` on every word they carry, through the production body builders
//! and the production decoration boundary; every legacy key keeps its value.
use bsv_rs::transaction::MerklePath;
use bsv_tracker::{
    Evidence, Header, HeaderError, Headers, Hint, HintStatus, HostAction, Input, Params, Proof,
    State,
};
use low_app_layer::tracker_host::{runtime, served};
use low_app_layer::{logic, owed, txany};
use serde_json::{json, Value};
use std::collections::BTreeMap;

struct Active;
impl Headers for Active {
    fn header_at(&self, h: u32) -> Result<Option<Header>, HeaderError> {
        Ok(Some(Header {
            hash: "02".repeat(32),
            // a coinbase path's root is its txid: the fixture mines tx(1) at 10, tx(3) at 11, tx(4) at 12
            merkle_root: tx(match h {
                10 => 1,
                11 => 3,
                _ => 4,
            }),
        }))
    }
    fn tip_height(&self) -> Result<u32, HeaderError> {
        Ok(20)
    }
}
fn tx(n: u8) -> String {
    format!("{n:02x}").repeat(32)
}
fn params() -> Params {
    Params { age_threshold: 60 }
}
fn seen(id: &str) -> Value {
    let mut s = State::new(id);
    s.step(&params(), &Active, Input::Host(HostAction::Build))
        .unwrap();
    s.step(
        &params(),
        &Active,
        Input::Hint(Hint::new("arcade", HintStatus::Seen, 7)),
    )
    .unwrap();
    served::fields(&s)
}
/// A checked mined word: the coinbase path of `id` at height `n` checks against [`Active`]'s root `tx(n)`.
fn mined(id: &str, n: u8) -> Value {
    let mut s = State::new(id);
    s.step(
        &params(),
        &Active,
        Input::Evidence(Evidence::Proof(
            Proof::new(id, MerklePath::from_coinbase_txid(id, u32::from(n))).unwrap(),
        )),
    )
    .unwrap();
    served::fields(&s)
}
fn stale(id: &str, n: u8) -> Value {
    let mut v = mined(id, n);
    v["word"] = json!("stale");
    v["verifyPath"] = Value::Null;
    v
}
fn assert_tuple(v: &Value, at: &str) {
    for key in ["word", "tier", "height", "verifyPath"] {
        assert!(
            v.as_object().is_some_and(|o| o.contains_key(key)),
            "{at}: the word carries no `{key}`: {v}"
        );
    }
}
/// Every word object a body carries, by key, wherever it sits.
fn words(v: &Value, out: &mut Vec<(String, Value)>) {
    match v {
        Value::Object(o) => {
            for (k, c) in o {
                if ["tracker", "spenderWord", "coinWord", "fundingWord", "lock"]
                    .contains(&k.as_str())
                {
                    out.push((k.clone(), c.clone()));
                } else {
                    words(c, out);
                }
            }
        }
        Value::Array(a) => a.iter().for_each(|c| words(c, out)),
        _ => {}
    }
}
fn strip(v: &mut Value) {
    match v {
        Value::Object(o) => {
            for k in ["tracker", "spenderWord", "coinWord", "fundingWord", "lock"] {
                o.remove(k);
            }
            o.values_mut().for_each(strip);
        }
        Value::Array(a) => a.iter_mut().for_each(strip),
        _ => {}
    }
}
fn status(txid: &str, spent: Option<bool>, spender: Option<&str>) -> logic::OutpointStatus {
    logic::OutpointStatus {
        txid: txid.into(),
        vout: 0,
        known: spent.is_some(),
        spent,
        spending_txid: spender.map(str::to_string),
        spent_confirmed: Some(false),
        spender_seen: Some(true),
        spender_final: Some(true),
        reason: None,
        funding_seen: Some(true),
        evicted: logic::ServedEviction::NotRead,
    }
}
fn views() -> BTreeMap<String, Value> {
    BTreeMap::from([
        (tx(1), mined(&tx(1), 10)),
        (tx(2), seen(&tx(2))),
        (tx(3), mined(&tx(3), 11)),
        (tx(4), stale(&tx(4), 12)),
    ])
}

/// The outpoint routes: each entry carries `fundingWord` (its own creating txid's tuple) and `lock` (the coin word),
/// both full tuples; the legacy body is byte-identical once they are stripped.
#[test]
fn l1_outpoint_routes_serve_funding_and_lock_tuples() {
    let entries = [
        status(&tx(1), Some(true), Some(&tx(3))),
        status(&tx(1), Some(true), Some(&tx(2))),
        status(&tx(5), Some(true), Some(&tx(4))),
        status(&tx(5), Some(false), None),
        status(&tx(6), None, None),
    ];
    for (route, raw, unspent, tier) in [
        (
            "/utxo-status",
            logic::utxo_status_body(&entries),
            "free",
            "host",
        ),
        (
            "/spent-any",
            txany::spent_any_body(&entries, &[false; 5]),
            "unspentAsFarAsAsked",
            "hint",
        ),
    ] {
        let before: Value = serde_json::from_str(&raw).unwrap();
        let mut after = before.clone();
        let v = views();
        runtime::decorate(&mut after, None, &v);
        runtime::decorate_outpoints(&mut after, &v, unspent, tier);
        let mut found = Vec::new();
        words(&after, &mut found);
        for (k, w) in &found {
            assert_tuple(w, &format!("{route} {k}"));
        }
        let rows = after.as_array().unwrap();
        assert_eq!(rows[0]["fundingWord"]["word"], "mined", "{route}");
        assert!(!rows[0]["fundingWord"]["verifyPath"].is_null(), "{route}");
        assert_eq!(rows[0]["lock"]["word"], "spent", "{route}");
        assert_eq!(rows[0]["lock"]["txid"], tx(3), "{route}");
        assert_eq!(rows[0]["lock"]["tier"], "chain", "{route}");
        assert_eq!(rows[0]["lock"]["height"], 11, "{route}");
        assert_eq!(
            rows[0]["lock"]["verifyPath"], rows[0]["spenderWord"]["verifyPath"],
            "{route}: the spent coin serves its spender's checked path"
        );
        assert_eq!(rows[1]["lock"]["word"], "locked", "{route}");
        assert_eq!(rows[1]["lock"]["tier"], "hint", "{route}");
        assert!(rows[1]["lock"]["verifyPath"].is_null(), "{route}");
        assert_eq!(rows[2]["fundingWord"]["word"], "unknown", "{route}");
        assert_eq!(rows[2]["fundingWord"]["tier"], "host", "{route}");
        assert_eq!(rows[2]["lock"]["word"], "suspended", "{route}");
        assert!(rows[2]["lock"]["verifyPath"].is_null(), "{route}");
        assert_eq!(rows[3]["lock"]["word"], unspent, "{route}");
        assert_eq!(rows[3]["lock"]["tier"], tier, "{route}");
        assert_eq!(rows[4]["lock"]["word"], "unknown", "{route}");
        strip(&mut after);
        assert_eq!(after, before, "{route}: a legacy key or value changed");
    }
}

/// `/tx-any`: the subject and every batch answer carry the tuple (NS-3 built; pinned here beside the others).
#[test]
fn l1_tx_any_serves_the_tuple_on_subject_and_answers() {
    let answer = txany::decide_tx_any(
        None,
        None,
        Some(&txany::TxObservation::FirstPartyPresent {
            confirmed: false,
            raw_hex: Some("0100".into()),
        }),
        txany::AbsenceCorroboration::Unknown,
    );
    let v = views();
    for (raw, subject) in [
        (txany::tx_any_body(&tx(1), &answer), Some(tx(1))),
        (
            txany::tx_any_batch_body(&[(tx(2), answer.clone())], &[]),
            None,
        ),
    ] {
        let mut value: Value = serde_json::from_str(&raw).unwrap();
        runtime::decorate(&mut value, subject.as_deref(), &v);
        let mut found = Vec::new();
        words(&value, &mut found);
        assert!(!found.is_empty());
        for (k, w) in &found {
            assert_tuple(w, &format!("/tx-any {k}"));
        }
    }
}

/// `/owed`: `coinWord` is the full tuple; its verify path rides only a coin the spender's CHECKED mined word spent.
#[test]
fn l1_owed_coin_word_is_a_full_tuple() {
    let checked = mined(&tx(3), 11);
    let spent = owed::spender_coin_word(&checked, &tx(3), Some(&tx(3)));
    assert_tuple(&spent, "spent");
    assert_eq!(spent["word"], "spent");
    assert_eq!(spent["tier"], "chain");
    assert_eq!(spent["verifyPath"], checked["verifyPath"]);
    let lost = owed::spender_coin_word(&checked, &tx(3), Some(&tx(9)));
    assert_eq!(lost["word"], "lost");
    assert_eq!(lost["verifyPath"], checked["verifyPath"]);
    let pending = owed::spender_coin_word(&seen(&tx(2)), &tx(2), None);
    assert_tuple(&pending, "pending");
    assert_eq!(pending["word"], "pending");
    assert_eq!(pending["tier"], "hint");
    assert!(pending["verifyPath"].is_null());
    let suspended = owed::spender_coin_word(&stale(&tx(4), 12), &tx(4), None);
    assert_eq!(suspended["word"], "suspended");
    assert!(suspended["verifyPath"].is_null());
}

/// The production wiring: `/utxo-status` answers through the outpoint body, `/spent-any` decorates its entries.
#[test]
fn l1_routes_wire_the_outpoint_words() {
    let routes = include_str!("../src/routes.rs");
    let utxo = &routes[routes
        .find("let mut entries = assemble_statuses(&outpoints, &rows);")
        .unwrap()..];
    let utxo = &utxo[..utxo.find("\n}\n").unwrap()];
    assert!(
        utxo.contains("runtime::outpoint_body("),
        "utxo-status skips the outpoint words"
    );
    let spent = &routes[routes
        .find("crate::txany::spent_any_body_sourced(")
        .unwrap()..];
    let spent = &spent[..spent.find("\n}\n").unwrap()];
    assert!(
        spent.contains("runtime::decorate_outpoints("),
        "spent-any skips the outpoint words"
    );
    let runtime_src = include_str!("../src/tracker_host/runtime.rs");
    let owed_read = &runtime_src[runtime_src.find("pub async fn owed_body(").unwrap()..];
    assert!(
        owed_read[..owed_read.find("\n}\n").unwrap()].contains("coin_word_value("),
        "the owed read rebuilds coinWord without its tuple"
    );
}
