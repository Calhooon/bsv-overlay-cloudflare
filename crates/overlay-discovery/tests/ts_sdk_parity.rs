//! TS-SDK parity tests: verifies overlay-discovery produces byte-exact
//! PushDrop bytes matching `@bsv/sdk 1.10.1` + `@bsv/overlay 0.6.0` (the
//! versions `nanostore.babbage.systems` / `overlay-us-1.bsvb.tech` run).
//!
//! Fixtures live in `tests/fixtures/parity/*.json` and are regenerated
//! from `bsv-storage-cloudflare/tests/parity/ts-fixtures/generate.mjs`.
//!
//! If this test flips red, our on-chain SHIP/SLAP advertisements are not
//! byte-compatible with the TS reference and peer validators will reject
//! them — a silent federation break.

#![allow(clippy::unwrap_used)]

#[allow(deprecated)]
use bsv_rs::overlay::create_overlay_admin_token;
use bsv_rs::overlay::create_signed_overlay_admin_token;
use bsv_rs::overlay::Protocol as OverlayProtocol;
use bsv_rs::primitives::ec::{PrivateKey, PublicKey};
use bsv_rs::script::templates::PushDrop;
use bsv_rs::wallet::{Counterparty, KeyDeriver, Protocol, SecurityLevel};
use serde::Deserialize;

// ---------- Fixture types ----------

#[derive(Deserialize)]
struct ShipInput {
    domain: String,
    topic: String,
}

#[derive(Deserialize)]
struct SlapInput {
    domain: String,
    service: String,
}

#[derive(Deserialize)]
struct Expected {
    locking_script_hex: String,
}

#[derive(Deserialize)]
struct ShipFixture {
    admin_priv_hex: String,
    input: ShipInput,
    expected: Expected,
}

#[derive(Deserialize)]
struct SlapFixture {
    admin_priv_hex: String,
    input: SlapInput,
    expected: Expected,
}

// ---------- Shared helpers ----------

fn admin_root(priv_hex: &str) -> (PrivateKey, PublicKey) {
    let k = PrivateKey::from_hex(priv_hex).unwrap();
    let p = k.public_key();
    (k, p)
}

/// Build a PushDrop the way `@bsv/sdk` `pushdrop.lock(fields, protocol,
/// keyID, counterparty, forSelf=true, includeSignature=true)` does —
/// 5 fields (4 data + 1 signature), BRC-42-derived locking key.
///
/// This is NOT what `bsv_rs::overlay::create_overlay_admin_token`
/// produces today (it emits 4 unsigned fields with identity-key
/// locking — a **divergence from TS we need to flag**). The function
/// here is the CORRECT reference; if our SHIP/SLAP helper doesn't
/// match this shape, the parity test below will catch it.
fn build_ts_parity_pushdrop(
    root: &PrivateKey,
    protocol_name: &str,
    data_fields: Vec<Vec<u8>>,
) -> String {
    let protocol = Protocol::new(SecurityLevel::Counterparty, protocol_name);
    let deriver = KeyDeriver::new(Some(root.clone()));

    // Locking key: BRC-42 child with counterparty="anyone", forSelf=true
    let locking_pub = deriver
        .derive_public_key(&protocol, "1", &Counterparty::Anyone, true)
        .unwrap();

    // Signing key: same derivation
    let signing_key = deriver
        .derive_private_key(&protocol, "1", &Counterparty::Anyone)
        .unwrap();

    // Sign concat(data_fields) — matches @bsv/sdk pushdrop.lock sign preimage.
    use bsv_rs::primitives::hash::sha256;
    let sign_data: Vec<u8> = data_fields.iter().flat_map(|f| f.iter().copied()).collect();
    let sig_der = signing_key.sign(&sha256(&sign_data)).unwrap().to_der();

    let mut all_fields = data_fields;
    all_fields.push(sig_der);

    let pd = PushDrop::new(locking_pub, all_fields);
    pd.lock().to_hex()
}

// ---------- SHIP parity ----------

#[test]
fn ts_sdk_ship_pushdrop_byte_exact_goldens() {
    let fixtures: &[(&str, &str)] = &[
        (
            "ship_tm_uhrp",
            include_str!("../../../tests/fixtures/parity/ship_tm_uhrp.json"),
        ),
        (
            "ship_tm_ship",
            include_str!("../../../tests/fixtures/parity/ship_tm_ship.json"),
        ),
        (
            "ship_long_topic",
            include_str!("../../../tests/fixtures/parity/ship_long_topic.json"),
        ),
    ];

    for (name, raw) in fixtures {
        let f: ShipFixture = serde_json::from_str(raw).unwrap();
        let (root, root_pub) = admin_root(&f.admin_priv_hex);

        let fields = vec![
            b"SHIP".to_vec(),
            root_pub.to_compressed().to_vec(),
            f.input.domain.as_bytes().to_vec(),
            f.input.topic.as_bytes().to_vec(),
        ];

        // Reference (TS-parity) rebuild via our in-test helper.
        let parity_hex = build_ts_parity_pushdrop(&root, "service host interconnect", fields);

        // The NEW upstream bsv-rs helper: `create_signed_overlay_admin_token`.
        // This is what `CloudflareAdvertiser` switches to (task #41).
        let upstream_hex = create_signed_overlay_admin_token(
            &root,
            OverlayProtocol::Ship,
            &f.input.domain,
            &f.input.topic,
        )
        .to_hex();

        // Primary assertion: our in-test reference matches the TS fixture.
        assert_eq!(
            parity_hex, f.expected.locking_script_hex,
            "{name}: TS-parity rebuild must match @bsv/sdk fixture byte-exact"
        );

        // THE WIN: the upstream bsv-rs helper also matches TS byte-exact.
        // Before #40 landed, this assertion would have failed (old helper
        // produced 4-field unsigned). With the #40 fix in place, it's green
        // and we can delete the canary below + switch production callers.
        assert_eq!(
            upstream_hex, f.expected.locking_script_hex,
            "{name}: create_signed_overlay_admin_token must match @bsv/sdk fixture byte-exact"
        );
    }
}

// ---------- SLAP parity ----------

#[test]
fn ts_sdk_slap_pushdrop_byte_exact_goldens() {
    let fixtures: &[(&str, &str)] = &[
        (
            "slap_ls_uhrp",
            include_str!("../../../tests/fixtures/parity/slap_ls_uhrp.json"),
        ),
        (
            "slap_ls_ship",
            include_str!("../../../tests/fixtures/parity/slap_ls_ship.json"),
        ),
    ];

    for (name, raw) in fixtures {
        let f: SlapFixture = serde_json::from_str(raw).unwrap();
        let (root, root_pub) = admin_root(&f.admin_priv_hex);

        let fields = vec![
            b"SLAP".to_vec(),
            root_pub.to_compressed().to_vec(),
            f.input.domain.as_bytes().to_vec(),
            f.input.service.as_bytes().to_vec(),
        ];
        let parity_hex = build_ts_parity_pushdrop(&root, "service lookup availability", fields);

        assert_eq!(
            parity_hex, f.expected.locking_script_hex,
            "{name}: TS-parity rebuild must match @bsv/sdk fixture byte-exact"
        );
    }
}

/// The DEPRECATED `create_overlay_admin_token` (4-field unsigned) still
/// exists for backward compatibility, but diverges from the TS reference
/// and from `create_signed_overlay_admin_token`. This canary asserts
/// that divergence on a stable fixture so nobody accidentally reroutes
/// the old helper to emit 5-field signed bytes without updating its
/// callers (which rely on the legacy shape for decoding on-chain
/// pre-fix tokens).
#[test]
#[allow(deprecated)]
fn deprecated_create_overlay_admin_token_still_emits_4_field_unsigned() {
    let raw = include_str!("../../../tests/fixtures/parity/ship_tm_uhrp.json");
    let f: ShipFixture = serde_json::from_str(raw).unwrap();
    let (_root, root_pub) = admin_root(&f.admin_priv_hex);
    let deprecated_hex = create_overlay_admin_token(
        OverlayProtocol::Ship,
        &root_pub,
        &f.input.domain,
        &f.input.topic,
    )
    .to_hex();
    assert_ne!(
        deprecated_hex, f.expected.locking_script_hex,
        "deprecated helper must remain 4-field unsigned (current TS is 5-field signed)"
    );
}

// ---------- UHRP admission transcript parity ----------
//
// Each fixture under `tests/fixtures/parity/transcripts/<label>.json`
// carries one PushDrop locking script plus the `expected_admit: bool`
// that `@bsv/sdk 1.10.1` + `@bsv/overlay 0.6.0`'s
// `UHRPTopicManager.identifyAdmissibleOutputs` returns. Our Rust
// validator `UHRPTopicManager::validate_uhrp_output` must agree on the
// admit/reject verdict for every case — the full set was pre-verified
// against the TS source in `bsv-storage-cloudflare/tests/parity/ts-fixtures/validate-transcripts.mjs`.
//
// This is the single source of truth for "does rust-overlay admit the
// same on-chain adverts bsvb.tech admits". Never skip a transcript
// without documenting exactly why the TS verdict is not reproducible
// under our validator.

use bsv_overlay_discovery::uhrp::topic_manager::UHRPTopicManager;
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::TransactionOutput;

#[derive(Deserialize)]
struct TranscriptCase {
    label: String,
    locking_script_hex: String,
    expected_admit: bool,
    #[allow(dead_code)]
    reason: String,
}

/// Rough unix-second "now" used for expiry checks. The TS reference
/// rejects only `expiry < 1`, so any positive wall-clock value works;
/// we pin one to keep Rust test results deterministic across the
/// fixture set.
const TRANSCRIPT_NOW: u64 = 1_800_000_000;

fn load_transcript(raw: &str) -> TranscriptCase {
    serde_json::from_str(raw).expect("transcript fixture parses")
}

fn validate_hex(locking_script_hex: &str) -> bool {
    let script = match LockingScript::from_hex(locking_script_hex) {
        Ok(s) => s,
        Err(_) => {
            // The TS `UHRPTopicManager` wraps decode in a try/catch and
            // returns an empty outputs list for unparsable scripts → no
            // admit. Mirror the TS "no admit" verdict when decode fails.
            return false;
        }
    };
    let output = TransactionOutput {
        satoshis: Some(1),
        locking_script: script,
        change: false,
    };
    matches!(
        UHRPTopicManager::validate_uhrp_output(&output, TRANSCRIPT_NOW),
        Ok(true)
    )
}

// Expand one `#[test]` per transcript so failures name the exact case
// and `cargo test <label>` can zero in during investigation.
macro_rules! transcript_test {
    ($name:ident, $file:expr) => {
        #[test]
        fn $name() {
            let raw = include_str!(concat!(
                "../../../tests/fixtures/parity/transcripts/",
                $file
            ));
            let c = load_transcript(raw);
            let got = validate_hex(&c.locking_script_hex);
            assert_eq!(
                got, c.expected_admit,
                "transcript {}: expected admit={} got admit={} (reason: {})",
                c.label, c.expected_admit, got, c.reason
            );
        }
    };
}

transcript_test!(transcript_valid_admit, "valid_admit.json");
transcript_test!(transcript_past_expiry_admits, "past_expiry_admits.json");
transcript_test!(transcript_zero_expiry_rejects, "zero_expiry_rejects.json");
transcript_test!(transcript_http_scheme_rejects, "http_scheme_rejects.json");
transcript_test!(transcript_ftp_scheme_rejects, "ftp_scheme_rejects.json");
transcript_test!(
    transcript_bad_hash_length_31_rejects,
    "bad_hash_length_31_rejects.json"
);
transcript_test!(
    transcript_bad_identity_key_32_bytes_rejects,
    "bad_identity_key_32_bytes_rejects.json"
);
transcript_test!(
    transcript_bad_varint_expiry_rejects,
    "bad_varint_expiry_rejects.json"
);
transcript_test!(
    transcript_empty_signature_rejects,
    "empty_signature_rejects.json"
);
transcript_test!(
    transcript_bad_signature_wrong_key_rejects,
    "bad_signature_wrong_key_rejects.json"
);
transcript_test!(transcript_bad_linkage_rejects, "bad_linkage_rejects.json");
transcript_test!(
    transcript_too_few_fields_rejects,
    "too_few_fields_rejects.json"
);
transcript_test!(
    transcript_too_many_fields_rejects,
    "too_many_fields_rejects.json"
);
transcript_test!(transcript_non_pushdrop_rejects, "non_pushdrop_rejects.json");
transcript_test!(
    transcript_zero_content_length_rejects,
    "zero_content_length_rejects.json"
);
transcript_test!(
    transcript_max_content_length_admits,
    "max_content_length_admits.json"
);

/// Bundle test: runs the transcripts_index.json with all 16 cases at
/// once, producing a single pass/fail summary. Useful as a CI gate; the
/// per-case tests above are the fast signal during investigation.
#[test]
fn transcript_index_all_cases_match_ts_verdict() {
    let index_raw = include_str!("../../../tests/fixtures/parity/transcripts_index.json");
    #[derive(Deserialize)]
    struct Index {
        cases: Vec<TranscriptCase>,
    }
    let idx: Index = serde_json::from_str(index_raw).unwrap();
    assert_eq!(idx.cases.len(), 16, "expected 16 transcripts in index");
    let mut mismatches: Vec<String> = Vec::new();
    for c in &idx.cases {
        let got = validate_hex(&c.locking_script_hex);
        if got != c.expected_admit {
            mismatches.push(format!(
                "{}: expected admit={} got admit={} (reason: {})",
                c.label, c.expected_admit, got, c.reason
            ));
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} of {} transcripts mismatched:\n{}",
        mismatches.len(),
        idx.cases.len(),
        mismatches.join("\n")
    );
}
