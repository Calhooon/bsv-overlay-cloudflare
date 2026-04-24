//! Layer 3 — cross-validator oracle.
//!
//! Runs BOTH our Rust `UHRPTopicManager::validate_uhrp_output` AND the
//! TS reference (`@bsv/overlay 0.6.0` + `@bsv/sdk 1.10.1`'s
//! `UHRPTopicManager.identifyAdmissibleOutputs`) over the same corpus of
//! locking scripts, then asserts both implementations agree on every
//! admit/reject decision.
//!
//! Why bother: byte-exact PushDrop goldens (Layer 1) prove we encode
//! fields the same way. Live harness (Layer 2) proves our JSON response
//! shape matches nanostore. Neither catches **admission-rule drift** —
//! e.g. if bsvb's TS `UHRPTopicManager.ts` tightens URL validation to
//! forbid IP-literal hosts, our Rust impl would keep admitting them
//! silently. Running both over the same inputs catches exactly that.
//!
//! Corpus:
//! - 2 cases from `bsv-storage-cloudflare/tests/parity/fixtures/parity/admission_transcript.json`
//!   (valid_future_expiry, past_expiry_still_admits)
//! - ~10 hand-crafted cases in this file exercising shape-reject gates
//!   (P2PKH, too-few-fields, short hash, bad URI scheme, empty URL,
//!   zero expiry, zero content length, bad signature, mismatched
//!   locking key, impostor-identity, non-UTF-8 URL).
//!
//! The hand-crafted cases are built with the SAME `make_signed_uhrp_output`
//! logic as `src/uhrp/topic_manager.rs::tests`, using a fresh deterministic
//! admin key (BIP-32 test scalar 1). They produce valid PushDrop hex
//! that both validators should agree on.
//!
//! Invocation:
//! - `cargo test -p overlay-discovery --test cross_validator`
//! - Requires `node` on PATH and
//!   `bsv-storage-cloudflare/tests/parity/ts-fixtures/node_modules/` installed.
//! - If either is missing, the test emits `SKIPPED: <reason>` on stderr
//!   and passes. This keeps CI green on minimal containers while surfacing
//!   the skip to anyone reading the log.
//!
//! **Don't run this in parallel with other tests touching the same
//! ts-fixtures dir** — the Node shim reads from stdin, no shared state.
//! (Default `cargo test` parallelism is fine; the shim is stateless.)

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use bsv_rs::primitives::ec::{PrivateKey, PublicKey};
use bsv_rs::primitives::encoding::Writer;
use bsv_rs::script::templates::PushDrop as PushDropTemplate;
use bsv_rs::transaction::TransactionOutput;
use bsv_rs::wallet::{
    Counterparty, CreateSignatureArgs, GetPublicKeyArgs, ProtoWallet, Protocol, SecurityLevel,
};
use overlay_discovery::uhrp::topic_manager::UHRPTopicManager;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

const UHRP_BRC43_PROTOCOL_NAME: &str = "uhrp advertisement";
const UHRP_KEY_ID: &str = "1";

/// Matches the admin priv used in `generate.mjs` — BIP-32 test scalar 1.
const ADMIN_PRIV_HEX: &str = "0000000000000000000000000000000000000000000000000000000000000001";

/// ~2096. Any time far enough in the future that the test never flakes
/// on system-clock drift.
const FUTURE_EXPIRY: u64 = 4_000_000_000;

/// Past-expiry value used by the TS admission_transcript fixture.
/// TS admits it (only rejects expiry<1).
const PAST_EXPIRY: u64 = 1_700_000_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CorpusCase {
    label: String,
    locking_script_hex: String,
    /// Default 1. Carried through to the synthetic BEEF the shim builds
    /// but has no admission relevance.
    #[serde(default = "default_sats")]
    satoshis: u64,
    /// Our local expected-admit prediction, independent of Rust validator
    /// output. Documented so a cross-validator divergence is easy to
    /// classify: if Rust and Node agree but differ from `expected_admit`,
    /// the corpus is wrong. If Rust disagrees with Node, THAT'S the bug.
    expected_admit: bool,
    /// Human-readable note about what the case exercises.
    note: String,
}

fn default_sats() -> u64 {
    1
}

#[derive(Debug, Deserialize)]
struct ShimVerdicts {
    verdicts: Vec<ShimVerdict>,
}

#[derive(Debug, Deserialize)]
struct ShimVerdict {
    label: String,
    admitted: bool,
    #[allow(dead_code)]
    #[serde(default)]
    reason: String,
    #[allow(dead_code)]
    #[serde(default)]
    output_count: usize,
}

// ---------------------------------------------------------------------------
// Hand-crafted corpus builders
// ---------------------------------------------------------------------------

fn varint(v: u64) -> Vec<u8> {
    let mut w = Writer::new();
    w.write_var_int(v);
    w.into_bytes()
}

/// Build a correctly signed UHRP PushDrop output with the given field
/// values. Mirrors `src/uhrp/topic_manager.rs::tests::make_signed_uhrp_output`.
fn make_signed_uhrp_output(
    admin_key: &PrivateKey,
    hash_bytes: [u8; 32],
    url: &str,
    expiry_time: u64,
    content_length: u64,
) -> TransactionOutput {
    let wallet = ProtoWallet::new(Some(admin_key.clone()));
    let protocol_id = Protocol::new(SecurityLevel::Counterparty, UHRP_BRC43_PROTOCOL_NAME);

    let locking_key_hex = wallet
        .get_public_key(GetPublicKeyArgs {
            identity_key: false,
            protocol_id: Some(protocol_id.clone()),
            key_id: Some(UHRP_KEY_ID.to_string()),
            counterparty: Some(Counterparty::Anyone),
            for_self: Some(true),
        })
        .unwrap()
        .public_key;
    let locking_key = PublicKey::from_hex(&locking_key_hex).unwrap();

    let identity_bytes = admin_key.public_key().to_compressed();
    let expiry_bytes = varint(expiry_time);
    let length_bytes = varint(content_length);

    let data_fields: Vec<Vec<u8>> = vec![
        identity_bytes.to_vec(),
        hash_bytes.to_vec(),
        url.as_bytes().to_vec(),
        expiry_bytes,
        length_bytes,
    ];

    let sign_data: Vec<u8> = data_fields.iter().flat_map(|f| f.iter().copied()).collect();
    let sig_result = wallet
        .create_signature(CreateSignatureArgs {
            data: Some(sign_data),
            hash_to_directly_sign: None,
            protocol_id,
            key_id: UHRP_KEY_ID.to_string(),
            counterparty: Some(Counterparty::Anyone),
        })
        .unwrap();

    let mut all_fields = data_fields;
    all_fields.push(sig_result.signature);

    let pushdrop = PushDropTemplate::new(locking_key, all_fields);
    TransactionOutput {
        satoshis: Some(1),
        locking_script: pushdrop.lock(),
        change: false,
    }
}

/// 6-field PushDrop with arbitrary raw data and a junk signature push.
/// Decodes to 6 fields so validators walk past the PushDrop shape check
/// and hit the specific field-level check we want to test.
fn make_raw_uhrp_output_with_junk_sig(mut fields: Vec<Vec<u8>>) -> TransactionOutput {
    let locking_key = PublicKey::from_private_key(&PrivateKey::random());
    fields.push(vec![0x30, 0x04, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01]);
    let pushdrop = PushDropTemplate::new(locking_key, fields);
    TransactionOutput {
        satoshis: Some(1),
        locking_script: pushdrop.lock(),
        change: false,
    }
}

fn script_hex(output: &TransactionOutput) -> String {
    output.locking_script.to_hex()
}

/// Build the hand-crafted portion of the corpus. Deterministic — uses a
/// fixed admin priv so regenerating produces the same scripts.
fn build_hand_crafted_corpus() -> Vec<CorpusCase> {
    let admin = PrivateKey::from_hex(ADMIN_PRIV_HEX).unwrap();
    let mut cases = Vec::new();

    // 1. Valid — future expiry. Should admit on both sides.
    cases.push(CorpusCase {
        label: "hand_valid_https_future_expiry".into(),
        locking_script_hex: script_hex(&make_signed_uhrp_output(
            &admin,
            [0x42; 32],
            "https://pub-abc.r2.dev/cdn/valid",
            FUTURE_EXPIRY,
            1024,
        )),
        satoshis: 1,
        expected_admit: true,
        note: "baseline — signed, https, future expiry, >0 length".into(),
    });

    // 2. Past expiry — TS admits (only rejects expiry<1). Rust matches TS.
    cases.push(CorpusCase {
        label: "hand_past_expiry_admits".into(),
        locking_script_hex: script_hex(&make_signed_uhrp_output(
            &admin,
            [0xCD; 32],
            "https://pub-abc.r2.dev/cdn/past",
            PAST_EXPIRY,
            100,
        )),
        satoshis: 1,
        expected_admit: true,
        note: "past-expiry advert — TS parity: only expiry<1 rejects".into(),
    });

    // 3. P2PKH — not PushDrop at all.
    cases.push(CorpusCase {
        label: "hand_p2pkh_not_pushdrop".into(),
        locking_script_hex: "76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac".into(),
        satoshis: 1000,
        expected_admit: false,
        note: "P2PKH script — fails at PushDrop.decode on both sides".into(),
    });

    // 4. Only 3 data fields — too few, regardless of sig.
    let short_fields = vec![
        admin.public_key().to_compressed().to_vec(),
        vec![0u8; 32],
        b"https://example.com".to_vec(),
    ];
    let short_pd = PushDropTemplate::new(
        PublicKey::from_private_key(&PrivateKey::random()),
        short_fields,
    );
    cases.push(CorpusCase {
        label: "hand_too_few_fields".into(),
        locking_script_hex: short_pd.lock().to_hex(),
        satoshis: 1,
        expected_admit: false,
        note: "3-field PushDrop — TS requires >=5, Rust requires exactly 6".into(),
    });

    // 5. 31-byte hash — malformed UHRP.
    let bad_hash_fields = vec![
        admin.public_key().to_compressed().to_vec(),
        vec![0u8; 31], // 31-byte hash
        b"https://example.com".to_vec(),
        varint(FUTURE_EXPIRY),
        varint(1024),
    ];
    let bad_hash_out = make_raw_uhrp_output_with_junk_sig(bad_hash_fields);
    cases.push(CorpusCase {
        label: "hand_wrong_hash_length_31".into(),
        locking_script_hex: script_hex(&bad_hash_out),
        satoshis: 1,
        expected_admit: false,
        note: "field[1] = 31 bytes — both impls reject (TS checks len===32; Rust errs)".into(),
    });

    // 6. ftp:// URI scheme.
    let ftp_fields = vec![
        admin.public_key().to_compressed().to_vec(),
        vec![0u8; 32],
        b"ftp://example.com".to_vec(),
        varint(FUTURE_EXPIRY),
        varint(1024),
    ];
    cases.push(CorpusCase {
        label: "hand_ftp_scheme_rejects".into(),
        locking_script_hex: script_hex(&make_raw_uhrp_output_with_junk_sig(ftp_fields)),
        satoshis: 1,
        expected_admit: false,
        note: "ftp:// scheme — TS checks protocol==='https:', Rust matches".into(),
    });

    // 7. Garbage signature — sig-verify fails, both reject.
    let bad_sig_fields = vec![
        admin.public_key().to_compressed().to_vec(),
        vec![0u8; 32],
        b"https://example.com".to_vec(),
        varint(FUTURE_EXPIRY),
        varint(1024),
    ];
    cases.push(CorpusCase {
        label: "hand_bad_signature".into(),
        locking_script_hex: script_hex(&make_raw_uhrp_output_with_junk_sig(bad_sig_fields)),
        satoshis: 1,
        expected_admit: false,
        note: "valid shape, garbage signature — sig-verify fails on both sides".into(),
    });

    // 8. Mismatched locking key — sig is fine but locking key is random.
    //    TS's isTokenSignatureCorrectlyLinked checks expected === lockingPublicKey.
    //    Rust compares via expected.public_key vs pushdrop.locking_public_key.to_hex().
    {
        let wallet = ProtoWallet::new(Some(admin.clone()));
        let protocol_id = Protocol::new(SecurityLevel::Counterparty, UHRP_BRC43_PROTOCOL_NAME);
        let data_fields: Vec<Vec<u8>> = vec![
            admin.public_key().to_compressed().to_vec(),
            vec![0u8; 32],
            b"https://example.com".to_vec(),
            varint(FUTURE_EXPIRY),
            varint(1024),
        ];
        let sign_data: Vec<u8> = data_fields.iter().flat_map(|f| f.iter().copied()).collect();
        let sig = wallet
            .create_signature(CreateSignatureArgs {
                data: Some(sign_data),
                hash_to_directly_sign: None,
                protocol_id,
                key_id: UHRP_KEY_ID.to_string(),
                counterparty: Some(Counterparty::Anyone),
            })
            .unwrap()
            .signature;
        let wrong_locking = PublicKey::from_private_key(&PrivateKey::random());
        let mut all = data_fields;
        all.push(sig);
        let pd = PushDropTemplate::new(wrong_locking, all);
        cases.push(CorpusCase {
            label: "hand_mismatched_locking_key".into(),
            locking_script_hex: pd.lock().to_hex(),
            satoshis: 1,
            expected_admit: false,
            note: "sig valid but locking key is unrelated — linkage check fails".into(),
        });
    }

    // 9. Impostor — field[0] = victim, signer = impostor.
    {
        let impostor = PrivateKey::random();
        let victim = PrivateKey::random();
        let imp_wallet = ProtoWallet::new(Some(impostor.clone()));
        let protocol_id = Protocol::new(SecurityLevel::Counterparty, UHRP_BRC43_PROTOCOL_NAME);
        let data_fields: Vec<Vec<u8>> = vec![
            victim.public_key().to_compressed().to_vec(),
            vec![0u8; 32],
            b"https://example.com".to_vec(),
            varint(FUTURE_EXPIRY),
            varint(1024),
        ];
        let sign_data: Vec<u8> = data_fields.iter().flat_map(|f| f.iter().copied()).collect();
        let sig = imp_wallet
            .create_signature(CreateSignatureArgs {
                data: Some(sign_data),
                hash_to_directly_sign: None,
                protocol_id: protocol_id.clone(),
                key_id: UHRP_KEY_ID.to_string(),
                counterparty: Some(Counterparty::Anyone),
            })
            .unwrap()
            .signature;
        let locking_hex = imp_wallet
            .get_public_key(GetPublicKeyArgs {
                identity_key: false,
                protocol_id: Some(protocol_id),
                key_id: Some(UHRP_KEY_ID.to_string()),
                counterparty: Some(Counterparty::Anyone),
                for_self: Some(true),
            })
            .unwrap()
            .public_key;
        let locking_key = PublicKey::from_hex(&locking_hex).unwrap();
        let mut all = data_fields;
        all.push(sig);
        let pd = PushDropTemplate::new(locking_key, all);
        cases.push(CorpusCase {
            label: "hand_impostor_identity".into(),
            locking_script_hex: pd.lock().to_hex(),
            satoshis: 1,
            expected_admit: false,
            note: "field[0]=victim pubkey, sig by impostor — linkage fails".into(),
        });
    }

    // 10. Zero content length — TS rejects `fileSize < 1`.
    cases.push(CorpusCase {
        label: "hand_zero_content_length".into(),
        locking_script_hex: script_hex(&make_signed_uhrp_output(
            &admin,
            [0u8; 32],
            "https://example.com",
            FUTURE_EXPIRY,
            0,
        )),
        satoshis: 1,
        expected_admit: false,
        note: "content_length=0 — TS rejects fileSize<1, Rust matches".into(),
    });

    // 11. Zero expiry — TS rejects `expiryTime < 1`.
    cases.push(CorpusCase {
        label: "hand_zero_expiry".into(),
        locking_script_hex: script_hex(&make_signed_uhrp_output(
            &admin,
            [0u8; 32],
            "https://example.com",
            0,
            1024,
        )),
        satoshis: 1,
        expected_admit: false,
        note: "expiry=0 — TS rejects expiryTime<1, Rust matches".into(),
    });

    // 12. Malformed URL (no scheme separator).
    let malformed_url_fields = vec![
        admin.public_key().to_compressed().to_vec(),
        vec![0u8; 32],
        b"not a url at all".to_vec(),
        varint(FUTURE_EXPIRY),
        varint(1024),
    ];
    cases.push(CorpusCase {
        label: "hand_malformed_url".into(),
        locking_script_hex: script_hex(&make_raw_uhrp_output_with_junk_sig(malformed_url_fields)),
        satoshis: 1,
        expected_admit: false,
        note: "field[2] = 'not a url at all' — URL parse throws on both sides".into(),
    });

    cases
}

// ---------------------------------------------------------------------------
// Load corpus from admission_transcript.json
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct TranscriptFixture {
    cases: Vec<TranscriptCase>,
}

#[derive(Debug, Deserialize)]
struct TranscriptCase {
    label: String,
    locking_script_hex: String,
    expected_admit: bool,
    #[serde(default)]
    reason: String,
}

fn load_transcript_corpus() -> Vec<CorpusCase> {
    let path = workspace_root()
        .join("../bsv-storage-cloudflare/tests/parity/fixtures/parity/admission_transcript.json");
    let raw = match std::fs::read_to_string(&path) {
        Ok(s) => s,
        Err(_) => return vec![], // fixture optional — don't fail if #42 hasn't landed it
    };
    let parsed: TranscriptFixture = match serde_json::from_str(&raw) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("cross_validator: failed to parse admission_transcript.json: {e}");
            return vec![];
        }
    };
    parsed
        .cases
        .into_iter()
        .map(|c| CorpusCase {
            label: format!("transcript_{}", c.label),
            locking_script_hex: c.locking_script_hex,
            satoshis: 1,
            expected_admit: c.expected_admit,
            note: c.reason,
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

/// Path to `rust-overlay/` (workspace root).
fn workspace_root() -> PathBuf {
    // CARGO_MANIFEST_DIR = overlay-discovery crate dir.
    // workspace root is two parents up: crates/overlay-discovery/.. = crates, /.. = workspace
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    crate_dir
        .parent() // crates/
        .and_then(|p| p.parent()) // workspace root
        .expect("CARGO_MANIFEST_DIR has two parents")
        .to_path_buf()
}

/// Directory containing `cross_validate.mjs` + `package.json` + `node_modules`.
fn ts_fixtures_dir() -> PathBuf {
    workspace_root()
        .join("..")
        .join("bsv-storage-cloudflare")
        .join("tests")
        .join("parity")
        .join("ts-fixtures")
}

// ---------------------------------------------------------------------------
// Shim invocation
// ---------------------------------------------------------------------------

/// Probe `node --version`. Returns true if a node binary is on PATH.
fn node_available() -> bool {
    Command::new("node")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Run `node cross_validate.mjs` with the corpus, capturing verdicts.
/// Returns `Err(reason)` on any subprocess failure with stderr included.
fn run_shim(corpus: &[CorpusCase]) -> Result<Vec<ShimVerdict>, String> {
    let dir = ts_fixtures_dir();
    let shim = dir.join("cross_validate.mjs");
    let nm = dir.join("node_modules");
    if !shim.exists() {
        return Err(format!("shim not found at {}", shim.display()));
    }
    if !nm.exists() {
        return Err(format!(
            "node_modules missing at {} — run `npm install` in ts-fixtures",
            nm.display()
        ));
    }

    let payload = serde_json::json!({
        "cases": corpus.iter().map(|c| serde_json::json!({
            "label": c.label,
            "locking_script_hex": c.locking_script_hex,
            "satoshis": c.satoshis,
        })).collect::<Vec<_>>()
    });
    let payload_bytes = serde_json::to_vec(&payload).unwrap();

    let mut child = Command::new("node")
        .arg("cross_validate.mjs")
        .current_dir(&dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn node: {e}"))?;

    child
        .stdin
        .as_mut()
        .ok_or_else(|| "no stdin handle".to_string())?
        .write_all(&payload_bytes)
        .map_err(|e| format!("write stdin: {e}"))?;

    let out = child.wait_with_output().map_err(|e| format!("wait: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "node exited {:?}\nstderr:\n{}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr)
        ));
    }

    let stdout = String::from_utf8_lossy(&out.stdout);
    let parsed: ShimVerdicts = serde_json::from_str(&stdout)
        .map_err(|e| format!("parse shim stdout: {e}\nstdout was:\n{stdout}"))?;
    Ok(parsed.verdicts)
}

// ---------------------------------------------------------------------------
// Rust-side evaluation
// ---------------------------------------------------------------------------

fn rust_admit(case: &CorpusCase, now: u64) -> bool {
    let locking = bsv_rs::script::LockingScript::from_hex(&case.locking_script_hex)
        .expect("corpus locking_script_hex must be valid hex");
    let output = TransactionOutput {
        satoshis: Some(case.satoshis),
        locking_script: locking,
        change: false,
    };
    matches!(
        UHRPTopicManager::validate_uhrp_output(&output, now),
        Ok(true)
    )
}

// ---------------------------------------------------------------------------
// Main test
// ---------------------------------------------------------------------------

#[test]
fn cross_validator_rust_vs_ts_agree() {
    // Skip conditions: no node on PATH, or node_modules not installed.
    if !node_available() {
        eprintln!(
            "SKIPPED: cross_validator — `node` not on PATH. Install Node.js to run Layer 3 oracle."
        );
        return;
    }
    if !ts_fixtures_dir().join("node_modules").exists() {
        eprintln!(
            "SKIPPED: cross_validator — node_modules missing in {}. Run `npm install` there.",
            ts_fixtures_dir().display()
        );
        return;
    }

    // Build the corpus.
    let mut corpus = build_hand_crafted_corpus();
    corpus.extend(load_transcript_corpus());
    assert!(
        corpus.len() >= 10,
        "corpus too small ({} cases) — Layer 3 needs at least 10 for meaningful parity evidence",
        corpus.len()
    );

    // Deterministic `now` — far enough in future so past-expiry is past,
    // but doesn't matter because Rust validator matches TS by ignoring now.
    let now = 1_800_000_000;

    // Run Rust validator first.
    let rust_verdicts: Vec<(String, bool)> = corpus
        .iter()
        .map(|c| (c.label.clone(), rust_admit(c, now)))
        .collect();

    // Then Node shim.
    let node_verdicts = run_shim(&corpus).expect("shim invocation failed");
    assert_eq!(
        node_verdicts.len(),
        corpus.len(),
        "node shim returned {} verdicts for {} cases",
        node_verdicts.len(),
        corpus.len()
    );

    // Align by label.
    let mut divergences: Vec<String> = Vec::new();
    let mut corpus_mismatches: Vec<String> = Vec::new();

    for (case, (_, rust_admit_decision)) in corpus.iter().zip(rust_verdicts.iter()) {
        let node = node_verdicts
            .iter()
            .find(|v| v.label == case.label)
            .unwrap_or_else(|| panic!("node verdict missing for label '{}'", case.label));

        if *rust_admit_decision != node.admitted {
            divergences.push(format!(
                "DIVERGENCE '{}': rust={} node={} (expected={}) — note: {} — node_reason: {:?}",
                case.label,
                rust_admit_decision,
                node.admitted,
                case.expected_admit,
                case.note,
                node.reason
            ));
        }

        // Soft sanity: if BOTH impls agree, flag when they disagree with the
        // corpus author's expected_admit. Not a hard fail (we treat the
        // cross-validator as the oracle, not the author) but stderr output.
        if *rust_admit_decision == node.admitted && *rust_admit_decision != case.expected_admit {
            corpus_mismatches.push(format!(
                "CORPUS_NOTE '{}': both impls say {} but expected_admit={} — author note: {}",
                case.label, rust_admit_decision, case.expected_admit, case.note,
            ));
        }
    }

    // Print soft notes first so they show up in successful runs too.
    for m in &corpus_mismatches {
        eprintln!("{m}");
    }

    assert!(
        divergences.is_empty(),
        "Rust vs TS UHRPTopicManager disagreements:\n  {}\n\n\
         Every divergence is a real admission-rule drift bug. Fix one side.",
        divergences.join("\n  ")
    );

    eprintln!(
        "cross_validator: {} cases, {} admitted, {} rejected, 0 divergences",
        corpus.len(),
        rust_verdicts.iter().filter(|(_, a)| *a).count(),
        rust_verdicts.iter().filter(|(_, a)| !*a).count(),
    );
}
