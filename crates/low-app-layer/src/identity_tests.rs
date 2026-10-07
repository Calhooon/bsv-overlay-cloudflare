//! The identity views' pins (bsv-low #532). The handlers are driven whole
//! through a FAKE resolver binding and a FAKE kill list: the route tier
//! (`make ci-route`) mounts no app-layer worker, so this is the route test,
//! stated. `FIXTURE` is Zanaadu's own `/api/pf/identity` body
//! (`overlay/tests/fixtures/pf/pf-consumer-fixtures.json` at `2de902a`,
//! sha256 `4c3ecd47...e795`, `expected.identityBody`), the lib's bytes with no
//! `display`: what a resolver older than M29-3a's display merge answers.
//! `DISPLAY_FIXTURE` is the CAPTURED body of LOW's resolver: the same bytes
//! with `,"display":<pick>` spliced into each entry the way
//! `low-identity-resolver/src/display.rs` does, the pick being the lib's own
//! `expected.display` (18,479 bytes, the M29-3a display lens's figure); the two
//! entries quoted in the resolver's CONTRACT.md are pinned to be in it.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;
use crate::auth::{effective_mode, front_door_disposition, AuthMode, Disposition, IDENTITY_ROUTES};
use std::cell::{Cell, RefCell};

const FIXTURE: &str = include_str!("fixtures/resolver_identity_batch.fixture.json");
const DISPLAY_FIXTURE: &str = include_str!("fixtures/resolver_identity_display_batch.fixture.json");
const ORIGIN: &str = "https://low-app-layer-beta.dev-a3e.workers.dev";

/// Zanaadu's `expected.identityQuery`: the eight holders, in its order.
const QUERY: &str = "0222ca50390e660f601036dd7502bb973bdcd104b5dcb8f2e74de8edc6c292b03a,027670d1cbc190bc530ebde2669e76e7ab3c23b3ac0add9aa0902170d1280bddb4,029acb7d8ce958f7e5189676c3e7248ad79f717a672d4c80b59c425663e078810d,02ea0a856277a766272a1825aee660ef8bafcbdcfba1ca8a0878259611bf07996c,027cdcaef67e477fca8fe105bc67fab43132747330468be11bb62bdfe8734de17e,029f004a5bab5dd55346dbdf50832f100c3ce460bb2c2ace8016ade927f863bb55,027525d8e66c44ad84f6d5f785e26f6936be264a859278086502173085febc5b8b,025710158d4bc87e484ac205f67ad5c8e4438e12afa09eed71bc28381ea8e358cd";

const ALICE: &str = "0222ca50390e660f601036dd7502bb973bdcd104b5dcb8f2e74de8edc6c292b03a";
const BOB: &str = "027670d1cbc190bc530ebde2669e76e7ab3c23b3ac0add9aa0902170d1280bddb4";
const CAROL: &str = "029acb7d8ce958f7e5189676c3e7248ad79f717a672d4c80b59c425663e078810d";
const DAVE: &str = "02ea0a856277a766272a1825aee660ef8bafcbdcfba1ca8a0878259611bf07996c";
const ERIN: &str = "027cdcaef67e477fca8fe105bc67fab43132747330468be11bb62bdfe8734de17e";
const NOBODY: &str = "025710158d4bc87e484ac205f67ad5c8e4438e12afa09eed71bc28381ea8e358cd";
const ALICE_PIC: &str = "bc734b551469854ece2ca23019e01c0869c7103d070956fcfaec12a0f086f628";
const BOB_PIC: &str = "74c560ae2e6a8950103698605f7b9b451628285ab36f7b51aaa9d6582715e80c";

// -- fakes ------------------------------------------------------------------

/// The resolver binding: a path -> (status, body) table, every call recorded.
#[derive(Default)]
struct FakeResolver {
    routes: RefCell<HashMap<String, (u16, Vec<u8>)>>,
    fault: Cell<Option<UpstreamError>>,
    calls: RefCell<Vec<String>>,
}

impl FakeResolver {
    fn with_batch(body: &str, keys: &str) -> Self {
        let r = FakeResolver::default();
        r.serve(&format!("{RESOLVER_BATCH_PATH}?ik={keys}"), 200, body.as_bytes());
        r
    }

    fn serve(&self, path: &str, status: u16, body: &[u8]) {
        self.routes.borrow_mut().insert(path.to_string(), (status, body.to_vec()));
    }
}

impl Resolver for FakeResolver {
    async fn get(&self, path: &str, max_bytes: usize) -> std::result::Result<Upstream, UpstreamError> {
        self.calls.borrow_mut().push(path.to_string());
        if let Some(e) = self.fault.take() {
            return Err(e);
        }
        match self.routes.borrow().get(path) {
            // The real binding's cap: a body over `max_bytes` is never returned.
            Some((_, body)) if body.len() > max_bytes => Err(UpstreamError::TooLarge),
            Some((status, body)) => Ok(Upstream {
                status: *status,
                body: body.clone(),
            }),
            None => Ok(Upstream {
                status: 404,
                body: b"{\"error\":\"not found\"}".to_vec(),
            }),
        }
    }
}

#[derive(Default)]
struct FakeKill {
    set: RefCell<HashSet<String>>,
    broken: Cell<bool>,
    /// One entry per `killed_among` call: how many hashes it asked.
    reads: RefCell<Vec<usize>>,
}

impl KillList for FakeKill {
    async fn killed_among(&self, hashes: &[String]) -> std::result::Result<HashSet<String>, String> {
        // The KV adapter's chunking, counted as the store sees it.
        for chunk in hashes.chunks(KILL_BULK_CHUNK) {
            self.reads.borrow_mut().push(chunk.len());
        }
        if self.broken.get() {
            return Err("kv down".to_string());
        }
        Ok(hashes.iter().filter(|h| self.set.borrow().contains(*h)).cloned().collect())
    }
    async fn kill(&self, hash: &str, _reason: &str, _now: i64) -> std::result::Result<(), String> {
        if self.broken.get() {
            return Err("kv down".to_string());
        }
        self.set.borrow_mut().insert(hash.to_string());
        Ok(())
    }
    async fn unkill(&self, hash: &str) -> std::result::Result<(), String> {
        self.set.borrow_mut().remove(hash);
        Ok(())
    }
}

fn keys() -> Vec<String> {
    parse_identity_keys(QUERY).unwrap()
}

fn body_json(a: &Answer) -> serde_json::Value {
    serde_json::from_slice(&a.body).unwrap()
}

/// The fixture's raw per-key parts, as the resolver wrote them.
fn fixture_raw(key: &str, field: &str) -> String {
    raw_of(DISPLAY_FIXTURE, key, field)
}

fn raw_of(body: &str, key: &str, field: &str) -> String {
    let v: HashMap<String, Box<RawValue>> = serde_json::from_str(body).unwrap();
    let ids: HashMap<String, HashMap<String, Box<RawValue>>> =
        serde_json::from_str(v["identities"].get()).unwrap();
    ids[key][field].get().to_string()
}

fn png(len: usize) -> Vec<u8> {
    let mut b = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
    b.resize(len, 7);
    b
}

fn hash_of(b: &[u8]) -> String {
    hex::encode(bsv_rs::primitives::hash::sha256(b))
}

// -- the mapping ------------------------------------------------------------

/// Zanaadu's `expected.display` (the lib's pick over the fixtures), renamed to
/// the contract's shape, with `since` looked up in each holder's `names[]`.
fn expected_display(key: &str) -> serde_json::Value {
    let url = |h: &str| format!("{ORIGIN}/identity/pic/{h}");
    let pick = |name: &str, shown: &str, pic: &str, height: serde_json::Value, txid: &str| {
        serde_json::json!({"name": name, "nameDisplay": shown, "imageHash": pic, "pictureUrl": url(pic),
            "since": {"height": height, "headTxid": txid}})
    };
    let none = serde_json::json!({"name": null, "nameDisplay": null, "imageHash": null, "pictureUrl": null, "since": null});
    match key {
        ALICE => pick("alpha", "alpha", ALICE_PIC, 900007.into(), "bce006a0ca96065a851e12b2eac7d7ce6cc26ec2ad00fcae850d65ce5e1b7bdd"),
        BOB => pick("bob", "bob", BOB_PIC, 900017.into(), "774c1435e50476a7b07cbab062f0d33635bae30f65942ba02b5e194653cd74be"),
        CAROL => pick("carol", "carol", "6cc97d417a349ae9375a4d13948d347ee6627354ec866f41d1676363170f5ceb", serde_json::Value::Null, "0554ae9833bf69637dc8d1c0693e5edd2946e75e546b87caf90ae168643885ef"),
        DAVE => pick("d4ve", "D4ve", "d8f11aff599bd918329a74c7605ed997735bc435d7e6d8d538c9c2daa5a33246", 900011.into(), "5375126d4f4a0d7ca5ee8d0f70216075d6f7c6a6a2adc67c1ab15dd15d62b1db"),
        ERIN => pick("erin", "erin", "a144b55fc113b93b9f59f674bb0b557cba6a0f4cb4fb6f68f51e3227419fc6bd", 900013.into(), "a823c341718b6c3cc738620c28897f1639f2fe779c09e4663c208d3dbce64554"),
        _ => none,
    }
}

/// The mapping from the captured resolver answer to the contract shape: every
/// key answered; `userNumber`, `names`, `pictures`, `preference` and
/// `namespaceIds` byte for byte; `display` the resolver's pick MAPPED, equal to
/// Zanaadu's `expected.display` for every holder (carol, dave and erin
/// included: on the fixtures their pick carries the preference; on LOW's live
/// resolver it would not, LOW holding no preference records).
#[tokio::test]
async fn the_resolver_answer_maps_to_the_contract_shape() {
    let r = FakeResolver::with_batch(DISPLAY_FIXTURE, QUERY);
    let a = batch_answer(&r, &FakeKill::default(), &keys(), ORIGIN).await;
    assert_eq!(a.status, 200);
    assert_eq!(a.outcome, Outcome::Served);
    assert_eq!(a.header("Cache-Control"), Some(JSON_CACHE_CONTROL));
    let text = String::from_utf8(a.body.clone()).unwrap();
    let v = body_json(&a);

    // namespaceIds, byte for byte.
    let ns: HashMap<String, Box<RawValue>> = serde_json::from_str(DISPLAY_FIXTURE).unwrap();
    assert!(text.starts_with(&format!("{{\"namespaceIds\":{},", ns["namespaceIds"].get())));

    let ids = v["identities"].as_object().unwrap();
    assert_eq!(ids.len(), 8, "every requested key is answered");
    for key in keys() {
        let entry = ids[&key].as_object().unwrap();
        let mut fields: Vec<&str> = entry.keys().map(String::as_str).collect();
        fields.sort_unstable();
        assert_eq!(fields, vec!["display", "names", "pictures", "preference", "userNumber"], "{key}");
        for f in ["userNumber", "names", "pictures", "preference"] {
            let raw = fixture_raw(&key, f);
            assert!(
                text.contains(&format!("\"{f}\":{raw}")),
                "{key}.{f} must be the resolver's bytes"
            );
        }
        assert_eq!(entry["display"], expected_display(&key), "{key}");
        let mut shape: Vec<&str> = entry["display"].as_object().unwrap().keys().map(String::as_str).collect();
        shape.sort_unstable();
        assert_eq!(shape, vec!["imageHash", "name", "nameDisplay", "pictureUrl", "since"], "no ownerCanonical, no userNumber");
    }
    // The wire order of an entry is the contract's.
    assert!(text.contains(&format!("\"{NOBODY}\":{{\"userNumber\":null,\"names\":[],\"pictures\":[],\"preference\":{{\"name\":null,\"picture\":null}},\"display\":{{\"name\":null,\"nameDisplay\":null,\"imageHash\":null,\"pictureUrl\":null,\"since\":null}}}}")));
}

/// The two entries the resolver's CONTRACT.md quotes, verbatim, are in the
/// captured body, and each maps to the contract's shape.
#[tokio::test]
async fn the_contract_entries_map_to_the_contract_shape() {
    const BOB_ENTRY: &str = r#""027670d1cbc190bc530ebde2669e76e7ab3c23b3ac0add9aa0902170d1280bddb4":{"userNumber":2,"names":[{"name":"bob","ownerSigner":"02fc785fd6af28caf61274612a25e37c33270266fc371f80f5c946e4ee3861e713","ownerIdentity":"027670d1cbc190bc530ebde2669e76e7ab3c23b3ac0add9aa0902170d1280bddb4","minterIdentity":"027670d1cbc190bc530ebde2669e76e7ab3c23b3ac0add9aa0902170d1280bddb4","ownerCanonical":true,"minterCanonical":true,"salePrice":0,"listed":false,"headTxid":"774c1435e50476a7b07cbab062f0d33635bae30f65942ba02b5e194653cd74be","height":900017,"consented":true,"lastOp":"mint","display":"bob"},{"name":"gift_name","ownerSigner":"02fc785fd6af28caf61274612a25e37c33270266fc371f80f5c946e4ee3861e713","ownerIdentity":"027670d1cbc190bc530ebde2669e76e7ab3c23b3ac0add9aa0902170d1280bddb4","minterIdentity":"0222ca50390e660f601036dd7502bb973bdcd104b5dcb8f2e74de8edc6c292b03a","ownerCanonical":true,"minterCanonical":true,"salePrice":0,"listed":false,"headTxid":"032c0f5b9a7437275086185ce366303aa0a34eb86450a3e8f14c7129c0e21525","height":900016,"consented":false,"lastOp":"transfer","display":"gift_name"}],"pictures":[{"imageHash":"749be7996a7c91dc4f501be1e1fc23800440b5127614266253c65d25bdead80f","ownerSigner":"02fc785fd6af28caf61274612a25e37c33270266fc371f80f5c946e4ee3861e713","ownerIdentity":"027670d1cbc190bc530ebde2669e76e7ab3c23b3ac0add9aa0902170d1280bddb4","minterIdentity":"0222ca50390e660f601036dd7502bb973bdcd104b5dcb8f2e74de8edc6c292b03a","ownerCanonical":true,"minterCanonical":true,"salePrice":0,"listed":false,"headTxid":"9fa1366e2aaa0d5a91f12d7c6189508d1aed97d62ff96ba26a5778273a31c7e0","height":900036,"consented":false,"lastOp":"transfer"},{"imageHash":"74c560ae2e6a8950103698605f7b9b451628285ab36f7b51aaa9d6582715e80c","ownerSigner":"02fc785fd6af28caf61274612a25e37c33270266fc371f80f5c946e4ee3861e713","ownerIdentity":"027670d1cbc190bc530ebde2669e76e7ab3c23b3ac0add9aa0902170d1280bddb4","minterIdentity":"0222ca50390e660f601036dd7502bb973bdcd104b5dcb8f2e74de8edc6c292b03a","ownerCanonical":true,"minterCanonical":true,"salePrice":0,"listed":false,"headTxid":"93f62b9fd769ffb4f98bd75b406f06cd12a1888b2e1dd70400859b03aeaa2a83","height":900035,"consented":true,"lastOp":"touch"},{"imageHash":"e1c0738479ca0d7b7509cf0bb016871680e49a02566e2c74316ddd04fe2992f2","ownerSigner":"02fc785fd6af28caf61274612a25e37c33270266fc371f80f5c946e4ee3861e713","ownerIdentity":"027670d1cbc190bc530ebde2669e76e7ab3c23b3ac0add9aa0902170d1280bddb4","minterIdentity":"027670d1cbc190bc530ebde2669e76e7ab3c23b3ac0add9aa0902170d1280bddb4","ownerCanonical":true,"minterCanonical":true,"salePrice":0,"listed":false,"headTxid":"db974bea2f83179207f8db8cf6b822c13b922cabd342cabdaeddd197d99c1088","height":900033,"consented":true,"lastOp":"mint"}],"preference":{"name":null,"picture":null},"display":{"userNumber":2,"name":"bob","nameDisplay":"bob","picture":"74c560ae2e6a8950103698605f7b9b451628285ab36f7b51aaa9d6582715e80c","ownerCanonical":true}}"#;
    const NOBODY_ENTRY: &str = r#""025710158d4bc87e484ac205f67ad5c8e4438e12afa09eed71bc28381ea8e358cd":{"userNumber":null,"names":[],"pictures":[],"preference":{"name":null,"picture":null},"display":{"userNumber":null,"name":null,"nameDisplay":null,"picture":null,"ownerCanonical":null}}"#;
    for entry in [BOB_ENTRY, NOBODY_ENTRY] {
        assert!(DISPLAY_FIXTURE.contains(entry), "the CONTRACT.md entry is the captured body's bytes");
    }
    for (key, entry, user_number) in [(BOB, BOB_ENTRY, serde_json::json!(2)), (NOBODY, NOBODY_ENTRY, serde_json::Value::Null)] {
        let body = format!("{{\"namespaceIds\":{{\"name\":null,\"picture\":null}},\"identities\":{{{entry}}}}}");
        let r = FakeResolver::with_batch(&body, key);
        let v = body_json(&single_answer(&r, &FakeKill::default(), key, ORIGIN).await);
        assert_eq!(v["userNumber"], user_number, "userNumber passed through");
        assert_eq!(v["display"], expected_display(key));
        assert_eq!(v["identityKey"], key);
    }
}

/// A resolver entry with no `display` (an old resolver) maps to the all-null
/// display and is counted; nothing is derived from its rows.
#[tokio::test]
async fn an_entry_without_display_is_all_null_never_derived() {
    let batch = parse_resolver_batch(FIXTURE.as_bytes(), &keys()).unwrap();
    assert_eq!(batch.display_notes(), (8, 0), "eight entries without display");
    let before = health_json()["display"]["absent"].as_u64().unwrap();
    let r = FakeResolver::with_batch(FIXTURE, QUERY);
    let kill = FakeKill::default();
    let v = body_json(&batch_answer(&r, &kill, &keys(), ORIGIN).await);
    for key in keys() {
        assert_eq!(v["identities"][&key]["display"], expected_display(NOBODY), "{key}");
        assert_eq!(v["identities"][&key]["userNumber"].to_string(), raw_of(FIXTURE, &key, "userNumber"));
    }
    assert!(kill.reads.borrow().is_empty(), "no pick, nothing to ask the kill list");
    assert!(health_json()["display"]["absent"].as_u64().unwrap() >= before + 8);
}

/// The pick's name is no row of the same body's `names[]` (the two reads at the
/// resolver straddled a mirror write): `since` is null, the rest is mapped, the
/// miss is counted, and nothing fails.
#[tokio::test]
async fn a_pick_naming_no_row_has_since_null_and_is_counted() {
    let body = DISPLAY_FIXTURE.replace(
        r#""display":{"userNumber":2,"name":"bob","nameDisplay":"bob","#,
        r#""display":{"userNumber":2,"name":"bobby","nameDisplay":"Bobby","#,
    );
    assert_ne!(body, DISPLAY_FIXTURE);
    let batch = parse_resolver_batch(body.as_bytes(), &keys()).unwrap();
    assert_eq!(batch.display_notes(), (0, 1));
    let before = health_json()["display"]["sinceMiss"].as_u64().unwrap();
    let r = FakeResolver::with_batch(&body, BOB);
    let a = single_answer(&r, &FakeKill::default(), BOB, ORIGIN).await;
    assert_eq!((a.status, a.outcome), (200, Outcome::Served));
    let d = &body_json(&a)["display"];
    assert_eq!(d["name"], "bobby");
    assert_eq!(d["nameDisplay"], "Bobby");
    assert_eq!(d["since"], serde_json::Value::Null);
    assert_eq!(d["imageHash"], BOB_PIC);
    assert!(health_json()["display"]["sinceMiss"].as_u64().unwrap() > before);
}

/// A pick whose picture is not a hash is a shape fault (502), never a URL.
#[tokio::test]
async fn a_pick_picture_that_is_not_a_hash_is_refused() {
    let body = DISPLAY_FIXTURE.replace(BOB_PIC, "../../admin");
    let r = FakeResolver::with_batch(&body, BOB);
    let a = single_answer(&r, &FakeKill::default(), BOB, ORIGIN).await;
    assert_eq!((a.status, a.outcome), (502, Outcome::UpstreamFault));
}

/// No display rule can come back: the module's code reads no consent or
/// canonical flag and orders no rows (doc comments may name them).
#[test]
fn no_display_rule_lives_in_the_app_layer() {
    let code: String = include_str!("identity.rs")
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    for planted in ["consented", "ownerCanonical", "owner_canonical", ".sort", ".min_by(", ".max_by(", "listed", "preference ="] {
        assert!(!code.contains(planted), "identity.rs must map the resolver's pick, never derive it ({planted})");
    }
}

/// A key the resolver did not answer is the empty shape, never absent.
#[tokio::test]
async fn an_unanswered_key_is_the_empty_shape() {
    let body = r#"{"namespaceIds":{"name":null,"picture":null},"identities":{}}"#;
    let r = FakeResolver::with_batch(body, NOBODY);
    let a = batch_answer(&r, &FakeKill::default(), &[NOBODY.to_string()], ORIGIN).await;
    assert_eq!(
        String::from_utf8(a.body).unwrap(),
        format!(
            "{{\"namespaceIds\":{{\"name\":null,\"picture\":null}},\"identities\":{{\"{NOBODY}\":{{\"userNumber\":null,\"names\":[],\"pictures\":[],\"preference\":{{\"name\":null,\"picture\":null}},\"display\":{{\"name\":null,\"nameDisplay\":null,\"imageHash\":null,\"pictureUrl\":null,\"since\":null}}}}}}}}"
        )
    );
}

#[tokio::test]
async fn the_single_key_form() {
    let r = FakeResolver::with_batch(DISPLAY_FIXTURE, ALICE);
    let a = single_answer(&r, &FakeKill::default(), ALICE, ORIGIN).await;
    assert_eq!(a.status, 200);
    let v = body_json(&a);
    assert_eq!(v["identityKey"], ALICE);
    assert_eq!(v["userNumber"], 1);
    assert_eq!(v["display"], expected_display(ALICE));
    let text = String::from_utf8(a.body).unwrap();
    assert!(text.contains(&format!("\"names\":{}", fixture_raw(ALICE, "names"))));
    assert_eq!(*r.calls.borrow(), vec![format!("{RESOLVER_BATCH_PATH}?ik={ALICE}")]);
}

#[test]
fn the_key_list_parses_dedupes_and_caps() {
    let upper = ALICE.to_ascii_uppercase().replace("0X", "0x");
    assert_eq!(parse_identity_keys(&format!("{ALICE},{upper},,")).unwrap(), vec![ALICE.to_string()]);
    assert!(parse_identity_keys("").is_err());
    assert!(parse_identity_keys("04abc").is_err());
    let many: Vec<String> = (0..101).map(|i| format!("02{:064x}", i)).collect();
    assert!(parse_identity_keys(&many.join(",")).unwrap_err().contains("max 100"));
    assert_eq!(parse_identity_keys(&many[..100].join(",")).unwrap().len(), 100);
}

#[tokio::test]
async fn resolver_faults_are_honest_answers_never_guesses() {
    let r = FakeResolver::default();
    r.fault.set(Some(UpstreamError::Timeout));
    let a = batch_answer(&r, &FakeKill::default(), &[ALICE.to_string()], ORIGIN).await;
    assert_eq!((a.status, a.outcome), (503, Outcome::UpstreamFault));
    assert_eq!(a.header("Cache-Control"), Some(NO_STORE));

    let r = FakeResolver::with_batch("{\"not\":\"the shape\"}", ALICE);
    let a = batch_answer(&r, &FakeKill::default(), &[ALICE.to_string()], ORIGIN).await;
    assert_eq!((a.status, a.outcome), (502, Outcome::UpstreamFault));

    let r = FakeResolver::default();
    r.serve(&format!("{RESOLVER_BATCH_PATH}?ik={ALICE}"), 401, b"{}");
    let a = batch_answer(&r, &FakeKill::default(), &[ALICE.to_string()], ORIGIN).await;
    assert_eq!((a.status, a.outcome), (502, Outcome::UpstreamFault));
}

// -- the picture gate -------------------------------------------------------

#[tokio::test]
async fn a_png_passes_with_the_hardening_headers() {
    let bytes = png(1000);
    let h = hash_of(&bytes);
    let r = FakeResolver::default();
    r.serve(&format!("{RESOLVER_PICTURE_PATH}{h}"), 200, &bytes);
    let a = picture_answer(&r, &FakeKill::default(), &h).await;
    assert_eq!((a.status, a.outcome), (200, Outcome::Served));
    assert_eq!(a.body, bytes);
    assert_eq!(a.header("Content-Type"), Some("image/png"));
    assert_eq!(a.header("X-Content-Type-Options"), Some("nosniff"));
    assert_eq!(a.header("Content-Security-Policy"), Some("sandbox"));
    assert_eq!(a.header("Cache-Control"), Some("public, immutable, max-age=31536000"));
}

#[tokio::test]
async fn an_svg_with_a_png_extension_refuses() {
    let svg = br#"<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>"#.to_vec();
    let h = hash_of(&svg);
    let r = FakeResolver::default();
    r.serve(&format!("{RESOLVER_PICTURE_PATH}{h}"), 200, &svg);
    // The hash route itself: the bytes say svg, whatever was claimed.
    let a = picture_answer(&r, &FakeKill::default(), &h).await;
    assert_eq!((a.status, a.outcome), (404, Outcome::RefusedType));
    assert_eq!(a.header("Content-Type"), Some("application/json"));
    // A `.png` on the path is not a hash: the resolver is never asked.
    let a = picture_answer(&r, &FakeKill::default(), &format!("{h}.png")).await;
    assert_eq!((a.status, a.outcome), (404, Outcome::BadRequest));
    assert_eq!(r.calls.borrow().len(), 1);
    assert_eq!(picture_gate(&h, &svg), PictureGate::RefusedType);
}

#[tokio::test]
async fn a_65_kb_png_refuses() {
    let big = png(65 * 1024);
    let h = hash_of(&big);
    assert_eq!(picture_gate(&h, &big), PictureGate::Oversize);
    let r = FakeResolver::default();
    r.serve(&format!("{RESOLVER_PICTURE_PATH}{h}"), 200, &big);
    let a = picture_answer(&r, &FakeKill::default(), &h).await;
    assert_eq!((a.status, a.outcome), (404, Outcome::Oversize));
    // Exactly 64 KB is inside the cap.
    let edge = png(PICTURE_MAX_BYTES);
    assert_eq!(picture_gate(&hash_of(&edge), &edge), PictureGate::Serve("image/png"));
}

#[test]
fn the_magic_bytes_decide_the_type() {
    assert_eq!(sniff_image(&[0xFF, 0xD8, 0xFF, 0xE0, 0, 0]), Some("image/jpeg"));
    assert_eq!(sniff_image(b"GIF89a\x01\x00"), Some("image/gif"));
    assert_eq!(sniff_image(b"GIF87a\x01\x00"), Some("image/gif"));
    assert_eq!(sniff_image(b"RIFF\x10\x00\x00\x00WEBPVP8 "), Some("image/webp"));
    assert_eq!(sniff_image(b"RIFF\x10\x00\x00\x00WAVEfmt "), None);
    assert_eq!(sniff_image(b"<?xml version=\"1.0\"?><svg/>"), None);
    assert_eq!(sniff_image(b"<!doctype html><html>"), None);
    // The 4-byte prefix IS the png rule, as Zanaadu's `sniff_image_kind` and the client's sniff (M29-3b fold lens LOW-1).
    assert_eq!(sniff_image(&[0x89, b'P', b'N', b'G']), Some("image/png"));
    assert_eq!(sniff_image(b"GIF8"), Some("image/gif"), "GIF8 alone is the gif rule");
    assert_eq!(sniff_image(&[0x89, b'P', b'N']), None, "three bytes are not a png");
    // AVIF: Zanaadu's own header fixture, the one the client lane pins.
    assert_eq!(sniff_image(AVIF), Some("image/avif"));
    assert_eq!(sniff_image(b"\x00\x00\x00\x1cftypheicmif1"), None, "an ftyp box, not avif");
    assert_eq!(sniff_image(&AVIF[..11]), None);
    assert_eq!(sniff_image(b""), None);
}

/// Zanaadu's AVIF header (`pf_content.rs`), as in the client's `picture.test.ts`.
const AVIF: &[u8] = b"\x00\x00\x00\x1cftypavifmif1";

#[tokio::test]
async fn an_avif_face_is_served_as_avif() {
    let bytes = AVIF.to_vec();
    let h = hash_of(&bytes);
    let r = FakeResolver::default();
    r.serve(&format!("{RESOLVER_PICTURE_PATH}{h}"), 200, &bytes);
    let a = picture_answer(&r, &FakeKill::default(), &h).await;
    assert_eq!((a.status, a.outcome), (200, Outcome::Served));
    assert_eq!(a.header("Content-Type"), Some("image/avif"));
    assert_eq!(a.header("X-Content-Type-Options"), Some("nosniff"));
    assert_eq!(a.header("Content-Security-Policy"), Some("sandbox"));
}

#[tokio::test]
async fn bytes_that_do_not_hash_to_the_url_refuse() {
    let bytes = png(100);
    let other = "00".repeat(32);
    let r = FakeResolver::default();
    r.serve(&format!("{RESOLVER_PICTURE_PATH}{other}"), 200, &bytes);
    let a = picture_answer(&r, &FakeKill::default(), &other).await;
    assert_eq!((a.status, a.outcome), (404, Outcome::HashMismatch));
}

#[tokio::test]
async fn every_picture_refusal_is_the_same_404() {
    let bytes = png(100);
    let h = hash_of(&bytes);
    let r = FakeResolver::default();
    let kill = FakeKill::default();
    let missing = picture_answer(&r, &kill, &h).await;
    kill.set.borrow_mut().insert(h.clone());
    let killed = picture_answer(&r, &kill, &h).await;
    for a in [&missing, &killed] {
        assert_eq!(a.status, 404);
        assert_eq!(a.body, missing.body);
        assert_eq!(a.headers, missing.headers);
        assert_eq!(a.header("Cache-Control"), Some(NO_STORE));
    }
    assert_eq!((missing.outcome, killed.outcome), (Outcome::NotFound, Outcome::Killed));
}

// -- the kill list ----------------------------------------------------------

#[tokio::test]
async fn the_kill_list_stops_the_bytes_and_the_display() {
    let bytes = png(500);
    let h = hash_of(&bytes);
    let r = FakeResolver::default();
    r.serve(&format!("{RESOLVER_PICTURE_PATH}{h}"), 200, &bytes);
    let kill = FakeKill::default();
    assert_eq!(picture_answer(&r, &kill, &h).await.status, 200);

    let a = kill_answer(&kill, format!("{{\"imageHash\":\"{}\",\"reason\":\"abuse\"}}", h.to_uppercase()).as_bytes(), 1).await;
    assert_eq!(a.status, 200);
    assert_eq!(body_json(&a), serde_json::json!({"imageHash": h, "killed": true}));
    let a = picture_answer(&r, &kill, &h).await;
    assert_eq!((a.status, a.outcome), (404, Outcome::Killed));
    assert_eq!(r.calls.borrow().len(), 1, "a killed hash never reaches the resolver");

    let a = kill_answer(&kill, format!("{{\"imageHash\":\"{h}\",\"unkill\":true}}").as_bytes(), 2).await;
    assert_eq!(body_json(&a)["killed"], false);
    assert_eq!(picture_answer(&r, &kill, &h).await.status, 200);

    // A killed pick shows NO picture (no fallback to another of the holder's
    // pictures); the name, its casing and `since` are untouched.
    let kill = FakeKill::default();
    kill.set.borrow_mut().insert(ALICE_PIC.to_string());
    let r = FakeResolver::with_batch(DISPLAY_FIXTURE, ALICE);
    let a = single_answer(&r, &kill, ALICE, ORIGIN).await;
    assert_eq!((a.status, a.outcome), (200, Outcome::Served));
    assert_eq!(a.header("Cache-Control"), Some(JSON_CACHE_CONTROL));
    let mut want = expected_display(ALICE);
    want["imageHash"] = serde_json::Value::Null;
    want["pictureUrl"] = serde_json::Value::Null;
    assert_eq!(body_json(&a)["display"], want);
    assert_eq!(*kill.reads.borrow(), vec![1], "one read: the pick's picture only");
    // The kill list never touches a name: a "hash" equal to nothing in names.
    let kill = FakeKill::default();
    kill.set.borrow_mut().insert(BOB_PIC.to_string());
    let r = FakeResolver::with_batch(DISPLAY_FIXTURE, QUERY);
    let v = body_json(&batch_answer(&r, &kill, &keys(), ORIGIN).await);
    for key in keys() {
        let d = &v["identities"][&key]["display"];
        let mut want = expected_display(&key);
        if key == BOB {
            want["imageHash"] = serde_json::Value::Null;
            want["pictureUrl"] = serde_json::Value::Null;
        }
        assert_eq!(*d, want, "{key}");
    }
}

#[tokio::test]
async fn a_kill_list_fault_fails_closed() {
    let kill = FakeKill::default();
    kill.broken.set(true);
    let bytes = png(500);
    let h = hash_of(&bytes);
    let r = FakeResolver::default();
    r.serve(&format!("{RESOLVER_PICTURE_PATH}{h}"), 200, &bytes);
    let a = picture_answer(&r, &kill, &h).await;
    assert_eq!((a.status, a.outcome), (503, Outcome::KillListFault));
    assert_eq!(a.header("Cache-Control"), Some(NO_STORE));

    let r = FakeResolver::with_batch(DISPLAY_FIXTURE, ALICE);
    let a = single_answer(&r, &kill, ALICE, ORIGIN).await;
    assert_eq!((a.status, a.outcome), (200, Outcome::KillListFault));
    assert_eq!(a.header("Cache-Control"), Some(NO_STORE), "a degraded answer is never cached");
    let v = body_json(&a);
    assert_eq!(v["display"]["imageHash"], serde_json::Value::Null);
    assert_eq!(v["display"]["pictureUrl"], serde_json::Value::Null);
    assert_eq!(v["display"]["name"], "alpha", "names still render");

    assert_eq!(kill_answer(&kill, format!("{{\"imageHash\":\"{h}\"}}").as_bytes(), 1).await.status, 503);
}

#[tokio::test]
async fn a_bad_kill_body_is_refused() {
    let kill = FakeKill::default();
    for body in ["", "{}", "{\"imageHash\":\"zz\"}", "{\"imageHash\":\"00\",\"extra\":1}"] {
        assert_eq!(kill_answer(&kill, body.as_bytes(), 1).await.status, 400, "{body}");
    }
    let long = format!("{{\"imageHash\":\"{}\",\"reason\":\"{}\"}}", "00".repeat(32), "x".repeat(501));
    assert_eq!(kill_answer(&kill, long.as_bytes(), 1).await.status, 400);
    // The body itself is capped at 4 KB before it is parsed.
    let huge = format!("{{\"imageHash\":\"{}\",\"reason\":\"x\"{}}}", "00".repeat(32), " ".repeat(KILL_BODY_MAX_BYTES));
    let a = kill_answer(&kill, huge.as_bytes(), 1).await;
    assert_eq!((a.status, a.outcome), (413, Outcome::BadRequest));
    assert!(kill.set.borrow().is_empty());
}

// -- verify -----------------------------------------------------------------

#[tokio::test]
async fn verify_passes_the_proofs_through() {
    let r = FakeResolver::with_batch(DISPLAY_FIXTURE, ALICE);
    let name_proof = r#"{"name":"alpha","leafKey":"ab","namespaceId":"cd","shardId":2,"root":"ee","chainRoot":"ee","match":true,"leaf":"ff","siblings":["00"],"directions":[true],"head":{"txid":"11","vout":0,"height":900007}}"#;
    r.serve(&format!("{RESOLVER_VERIFY_NAME_PATH}alpha"), 200, name_proof.as_bytes());
    // The picture's proof is a 404 at the resolver: `null`, not an error.
    let a = verify_answer(&r, &FakeKill::default(), ALICE, ORIGIN).await;
    assert_eq!(a.status, 200);
    let text = String::from_utf8(a.body.clone()).unwrap();
    assert!(text.contains(&format!("\"name\":{name_proof}")), "byte for byte");
    let v = body_json(&a);
    assert_eq!(v["identityKey"], ALICE);
    assert_eq!(v["display"], expected_display(ALICE), "the mapped picks are the ones proven");
    assert_eq!(v["picture"], serde_json::Value::Null);
    assert_eq!(
        *r.calls.borrow(),
        vec![
            format!("{RESOLVER_BATCH_PATH}?ik={ALICE}"),
            format!("{RESOLVER_VERIFY_NAME_PATH}alpha"),
            format!("{RESOLVER_VERIFY_PICTURE_PATH}{ALICE_PIC}"),
        ]
    );

    r.serve(&format!("{RESOLVER_VERIFY_NAME_PATH}alpha"), 503, b"{}");
    let a = verify_answer(&r, &FakeKill::default(), ALICE, ORIGIN).await;
    assert_eq!((a.status, a.outcome), (503, Outcome::UpstreamFault));
    r.serve(&format!("{RESOLVER_VERIFY_NAME_PATH}alpha"), 200, b"[1,2]");
    assert_eq!(verify_answer(&r, &FakeKill::default(), ALICE, ORIGIN).await.status, 502);
}

// -- the anonymous read, the CORS, the budgets ------------------------------

/// A lobby shows names before a wallet connects: under AUTH_ENFORCE the
/// identity GETs stay on the front door's public path, and the picture and the
/// kill route are served before it.
#[test]
fn the_identity_reads_are_anonymous_under_strict() {
    for path in ["/identities", &format!("/identity/{ALICE}"), &format!("/identity/verify/{ALICE}")] {
        let mode = effective_mode(AuthMode::Strict, path);
        assert_eq!(mode, AuthMode::Lenient, "{path}");
        assert_eq!(front_door_disposition(mode, true, false), Disposition::ProceedAnonymous, "{path}");
        assert!(!IDENTITY_ROUTES.contains(&path));
        assert!(is_identity_path(path));
    }
    let pic = format!("/identity/pic/{ALICE_PIC}");
    assert!(is_front_door_exempt(&Method::Get, &pic));
    assert!(!is_front_door_exempt(&Method::Post, &pic));
    assert!(is_front_door_exempt(&Method::Post, KILL_ROUTE));
    assert!(!is_front_door_exempt(&Method::Get, KILL_ROUTE));
    assert!(!is_front_door_exempt(&Method::Get, "/identities"));
}

#[test]
fn cors_answers_the_app_origins_only_and_exposes_what_the_browser_reads() {
    let allow = parse_origins(" https://bsvarcade.com/, https://LOW-POT.pages.dev ,,");
    assert_eq!(allow, vec!["https://bsvarcade.com", "https://low-pot.pages.dev"]);
    assert_eq!(allowed_origin(Some("https://low-pot.pages.dev"), &allow).as_deref(), Some("https://low-pot.pages.dev"));
    assert_eq!(allowed_origin(Some("https://evil.example"), &allow), None);
    assert_eq!(allowed_origin(Some("https://bsvarcade.com.evil.example"), &allow), None);
    assert_eq!(allowed_origin(None, &allow), None);
    assert_eq!(allowed_origin(Some("https://bsvarcade.com"), &[]), None, "unset allowlist: no origin");

    let h: HashMap<&str, String> = cors_headers(Some("https://bsvarcade.com")).into_iter().collect();
    assert_eq!(h["Access-Control-Allow-Origin"], "https://bsvarcade.com");
    assert_eq!(h["Vary"], "Origin");
    let expose = &h["Access-Control-Expose-Headers"];
    use bsv_middleware_cloudflare::{auth_headers, session_lane as lane};
    for name in [
        auth_headers::VERSION,
        auth_headers::IDENTITY_KEY,
        auth_headers::NONCE,
        auth_headers::YOUR_NONCE,
        auth_headers::SIGNATURE,
        auth_headers::MESSAGE_TYPE,
        auth_headers::REQUEST_ID,
        lane::SESSION_COUNTER_HEADER,
        lane::SESSION_MAC_HEADER,
        lane::LANE_OFFER_HEADER,
        "Retry-After",
        "ETag",
    ] {
        assert!(expose.contains(name), "expose must carry {name}: {expose}");
    }
    assert!(h["Access-Control-Allow-Headers"].contains(lane::LANE_ASK_HEADER));

    let none: HashMap<&str, String> = cors_headers(None).into_iter().collect();
    assert_eq!(none.len(), 1, "a stranger's origin gets Vary and nothing else");
    assert_eq!(none["Vary"], "Origin");
}

#[test]
fn the_budgets_refuse_per_ip_and_per_identity() {
    let mut b = Budget::default();
    let t = 1_000_000;
    // Per IP: 30 full batches, then the 31st is refused until the window ends.
    for _ in 0..30 {
        assert!(b.charge("1.2.3.4", None, 100, t).is_ok());
    }
    assert_eq!(b.charge("1.2.3.4", None, 100, t + 10_000), Err((BudgetScope::Ip, 50_000)));
    assert!(b.charge("5.6.7.8", None, 100, t).is_ok(), "another IP has its own window");
    assert!(b.charge("1.2.3.4", None, 100, t + BUDGET_WINDOW_MS).is_ok(), "a new window");

    // Per identity, across IPs.
    let mut b = Budget::default();
    for i in 0..60 {
        assert!(b.charge(&format!("10.0.0.{i}"), Some(ALICE), 100, t).is_ok());
    }
    assert_eq!(b.charge("10.0.1.1", Some(ALICE), 1, t).unwrap_err().0, BudgetScope::Identity);
    assert!(b.charge("10.0.1.1", Some(BOB), 1, t).is_ok());

    let a = budget_refusal(BudgetScope::Ip, 1_500);
    assert_eq!(a.status, 429);
    assert_eq!(a.header("Retry-After"), Some("2"));
    assert_eq!(body_json(&a)["code"], "ERR_IDENTITY_BUDGET");
}

#[test]
fn the_budget_map_is_bounded() {
    let mut b = Budget::default();
    for i in 0..BUDGET_MAX_TRACKED {
        b.charge(&format!("ip{i}"), None, 1, 0).unwrap();
    }
    // Every window still live: the map is dropped (fail open), counted.
    b.charge("one-more", None, 1, 1).unwrap();
    assert_eq!(b.evictions, 1);
    assert!(b.windows.len() <= 2);
    // Expired windows are dropped first, without an eviction.
    let mut b = Budget::default();
    for i in 0..BUDGET_MAX_TRACKED {
        b.charge(&format!("ip{i}"), None, 1, 0).unwrap();
    }
    b.charge("late", None, 1, BUDGET_WINDOW_MS + 1).unwrap();
    assert_eq!(b.evictions, 0);
    assert_eq!(b.windows.len(), 1);
}

#[test]
fn every_route_is_counted_on_health() {
    count(RouteId::Picture, Outcome::Killed);
    let v = health_json();
    for r in ["batch", "single", "picture", "verify", "kill"] {
        for o in OUTCOME_NAMES {
            assert!(v["routes"][r][o].is_u64(), "routes.{r}.{o}");
        }
    }
    assert!(v["routes"]["picture"]["killed"].as_u64().unwrap() >= 1);
    assert_eq!(v["budget"]["ipKeysPerWindow"], IP_KEYS_PER_WINDOW);
}

// -- the rows-read ceilings (#499's shape; move into 499's ledger at merge) --

/// D1 rows read per request, every new route: ZERO. The module holds no D1
/// handle (no binding name, no statement), so a planted query reds here.
#[test]
fn rows_read_ceiling_499_no_identity_route_reads_d1() {
    let src = include_str!("identity.rs");
    for planted in ["OVERLAY_DB", ".d1(", "D1Database", ".prepare(", "SELECT ", "INSERT "] {
        assert!(!src.contains(planted), "identity.rs must not touch D1 ({planted})");
    }
}

/// The other stores per request, bounded: the resolver at most once on the
/// batch, single and picture routes and at most three times on verify; the
/// kill list in ONE bulk read of the picks (at most 100, one per key).
#[tokio::test]
async fn rows_read_ceiling_499_store_calls_per_route() {
    // A 100-key batch where every key holds 5 pictures and its pick is one.
    let keys: Vec<String> = (0..100).map(|i| format!("02{:064x}", i)).collect();
    let mut ids = serde_json::Map::new();
    for (i, k) in keys.iter().enumerate() {
        let pics: Vec<serde_json::Value> = (0..5)
            .map(|j| serde_json::json!({"imageHash": format!("{:060x}{:04x}", i, j), "height": j}))
            .collect();
        let pick = serde_json::json!({"userNumber": null, "name": null, "nameDisplay": null, "picture": format!("{:060x}{:04x}", i, 4), "ownerCanonical": true});
        ids.insert(k.clone(), serde_json::json!({"userNumber": null, "names": [], "pictures": pics, "preference": {"name": null, "picture": null}, "display": pick}));
    }
    let body = serde_json::json!({"namespaceIds": {"name": null, "picture": null}, "identities": ids}).to_string();
    let r = FakeResolver::with_batch(&body, &keys.join(","));
    let kill = FakeKill::default();
    let a = batch_answer(&r, &kill, &keys, ORIGIN).await;
    assert_eq!(a.status, 200);
    assert_eq!(r.calls.borrow().len(), 1);
    assert_eq!(*kill.reads.borrow(), vec![100], "100 picks, one bulk read");

    let r = FakeResolver::with_batch(DISPLAY_FIXTURE, ALICE);
    let kill = FakeKill::default();
    single_answer(&r, &kill, ALICE, ORIGIN).await;
    assert_eq!((r.calls.borrow().len(), kill.reads.borrow().len()), (1, 1));
    verify_answer(&r, &kill, ALICE, ORIGIN).await;
    assert_eq!((r.calls.borrow().len(), kill.reads.borrow().len()), (4, 2), "verify: three resolver calls");

    let r = FakeResolver::default();
    let kill = FakeKill::default();
    picture_answer(&r, &kill, ALICE_PIC).await;
    assert_eq!((r.calls.borrow().len(), kill.reads.borrow().clone()), (1, vec![1]));
}

// -- the origins per env (the M29-3b lens, L6) ------------------------------

/// `localhost` is an app origin on beta only (the dev server), never in the
/// default (prod) `[vars]` or anywhere outside `[env.beta]`.
#[test]
fn localhost_is_a_beta_origin_only() {
    let toml = include_str!("../wrangler.toml");
    let (prod, beta) = toml.split_at(toml.find("\n[env.beta]").expect("a beta env"));
    let code = |part: &str| -> Vec<String> {
        part.lines()
            .filter(|l| !l.trim_start().starts_with('#'))
            .map(|l| l.split(" #").next().unwrap_or(l).to_string())
            .collect()
    };
    let origins = |part: &str| -> String {
        code(part)
            .into_iter()
            .find(|l| l.trim_start().starts_with(APP_ORIGINS_VAR))
            .expect("an IDENTITY_APP_ORIGINS line")
    };
    assert!(!code(prod).iter().any(|l| l.contains("localhost")), "prod must not carry localhost");
    assert!(!origins(prod).contains("localhost"));
    assert!(origins(prod).contains("https://bsvarcade.com"));
    assert!(origins(beta).contains("http://localhost:5173"), "the pin reads the beta list too");
    assert_eq!(toml.matches("[env.").count(), toml.matches("[env.beta").count(), "beta is the only other env");
}
