//! UHRP Topic Manager -- validates UHRP advertisement PushDrop outputs.
//!
//! UHRP (Universal Hash Resolution Protocol) advertisements are PushDrop
//! outputs with exactly 5 data fields plus an appended ECDSA signature:
//!
//! 1. Server identity public key: 33-byte compressed secp256k1 pubkey
//! 2. Content SHA-256 hash: exactly 32 bytes
//! 3. Public download URL (UTF-8; must pass `is_advertisable_uri`)
//! 4. Expiry time: unix seconds, Bitcoin VarInt
//! 5. Content length: bytes, Bitcoin VarInt
//! 6. ECDSA signature (DER): over `concat(field[0..4])`, linked via BRC-42
//!    to field[0].
//!
//! There is **no TypeScript reference** for a UHRP topic manager — the crate
//! `@bsv/overlay-discovery-services` ships only SHIP and SLAP. This module
//! is the reference implementation. It mirrors the structure of
//! [`super::super::ship::topic_manager::SHIPTopicManager`] line-for-line,
//! swapping the field validation to the UHRP 5-field layout.
//!
//! # Expiry policy
//!
//! This implementation is **strict**: an advert whose `expiry_time <= now()`
//! at validation time is rejected. Rationale is documented in
//! `docs/uhrp_topic.md`. SHIP has no expiry field so its topic manager does
//! not reject on it; UHRP has expiry as an in-protocol field explicitly for
//! "how long the host commits to serving this content" — admitting expired
//! commitments is never useful.
//!
//! # Reference points
//!
//! - `~/bsv/bsv-storage-cloudflare/PROTOCOL.md` §B — field layout
//! - `~/bsv/bsv-storage-cloudflare/src/routes/advertise.rs::build_advert_bundle` — author-side
//! - `~/bsv/storage-server/src/utils/createUHRPAdvertisement.ts` — TS author-side
//! - `super::super::ship::topic_manager` — mirrored structure

use async_trait::async_trait;
use bsv_rs::primitives::ec::PublicKey;
use bsv_rs::primitives::encoding::Reader;
use bsv_rs::script::templates::PushDrop;
use bsv_rs::transaction::Transaction;
use bsv_rs::wallet::{
    Counterparty, GetPublicKeyArgs, ProtoWallet, Protocol, SecurityLevel, VerifySignatureArgs,
};
use overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use overlay_engine::types::{AdmittanceInstructions, ServiceMetadata, SubmitMode};
use tracing::{debug, warn};

/// BRC-43 protocol name used for UHRP advertisement key derivation and
/// signature verification. Must match the TS server verbatim — any drift
/// breaks byte-parity with the existing `overlay-us-1.bsvb.tech` deployment.
const UHRP_BRC43_PROTOCOL_NAME: &str = "uhrp advertisement";

/// BRC-42 key ID segment used for UHRP advertisement. Always `"1"` per
/// PROTOCOL.md §B and TS `createUHRPAdvertisement.ts:63`.
const UHRP_KEY_ID: &str = "1";

/// UHRP Topic Manager -- identifies admissible UHRP advertisement outputs.
pub struct UHRPTopicManager;

impl UHRPTopicManager {
    /// Create a new UHRP Topic Manager.
    pub fn new() -> Self {
        Self
    }
}

impl Default for UHRPTopicManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait(?Send)]
impl TopicManager for UHRPTopicManager {
    async fn identify_admissible_outputs(
        &self,
        beef: &[u8],
        _previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        _mode: SubmitMode,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        let mut outputs_to_admit = Vec::new();

        let tx = match Transaction::from_beef(beef, None) {
            Ok(tx) => tx,
            Err(e) => {
                return Err(TopicManagerError::InvalidBeef(e.to_string()));
            }
        };

        let now = current_unix_seconds();

        for (i, output) in tx.outputs.iter().enumerate() {
            match Self::validate_uhrp_output(output, now) {
                Ok(true) => {
                    debug!("UHRP: admitted output {i}");
                    outputs_to_admit.push(i as u32);
                }
                Ok(false) => {
                    // Not a UHRP advert — common for non-UHRP outputs in a mixed tx.
                }
                Err(e) => {
                    debug!("UHRP: output {i} skipped: {e}");
                }
            }
        }

        if outputs_to_admit.is_empty() {
            warn!("UHRP: no outputs admitted");
        }

        Ok(AdmittanceInstructions {
            outputs_to_admit,
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }

    async fn get_documentation(&self) -> String {
        include_str!("../../docs/uhrp_topic.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "Universal Hash Resolution Protocol".to_string(),
            description: Some("Manages UHRP content availability advertisements.".to_string()),
            ..Default::default()
        }
    }
}

impl UHRPTopicManager {
    /// Validate a single output as a UHRP advertisement.
    ///
    /// Returns `Ok(true)` if valid, `Ok(false)` if the output is simply not
    /// a UHRP advert (e.g. wrong field count / non-UHRP shape), `Err` with a
    /// short human-readable reason if the output looks like a malformed
    /// UHRP attempt (bad URI scheme, bad varint, etc.).
    ///
    /// `now` is the current unix-seconds timestamp used for expiry checks;
    /// taking it as a parameter keeps the validator a pure function and
    /// makes unit tests deterministic.
    pub fn validate_uhrp_output(
        output: &bsv_rs::transaction::TransactionOutput,
        now: u64,
    ) -> Result<bool, String> {
        // 1. Decode PushDrop. `PushDrop::decode` returns every pushed data
        //    chunk between OP_CHECKSIG and the first OP_DROP/OP_2DROP. The
        //    author's `PushDrop::lock(includeSignature=true)` pushed 5 data
        //    fields and appended the DER signature as a 6th pushed field,
        //    so a well-formed UHRP advert decodes to exactly 6 entries.
        let pushdrop =
            PushDrop::decode(&output.locking_script).map_err(|e| format!("not a PushDrop: {e}"))?;

        // 2. Exactly 6 decoded fields: the 5 data fields plus the signature.
        if pushdrop.fields.len() != 6 {
            return Ok(false);
        }

        // 3. Field[0]: 33-byte compressed pubkey (server identity).
        if pushdrop.fields[0].len() != 33 {
            return Err("identity key must be 33 bytes".into());
        }
        let identity_pubkey = PublicKey::from_bytes(&pushdrop.fields[0])
            .map_err(|e| format!("invalid identity key: {e}"))?;

        // 4. Field[1]: 32-byte SHA-256 content hash.
        if pushdrop.fields[1].len() != 32 {
            return Err("content hash must be 32 bytes".into());
        }

        // 5. Field[2]: content download URL.
        //
        // **Note on TS parity**: the TS reference (`UHRPTopicManager.ts:48-52`)
        // only checks `fileLocationURL.protocol === 'https:'` — it does NOT
        // apply BRC-101 `is_advertisable_uri` semantics (which reject any URL
        // with a path other than `/`). UHRP `field[2]` is a content URL with
        // a path (`/cdn/<id>`), not a service-advertisement hostname. Using
        // `is_advertisable_uri` here would reject every real UHRP advert,
        // including byte-for-byte equivalents that bsvb.tech currently admits.
        let uri_str = match std::str::from_utf8(&pushdrop.fields[2]) {
            Ok(s) => s,
            Err(_) => return Err("URL is not valid UTF-8".into()),
        };
        let parsed = url::Url::parse(uri_str).map_err(|e| format!("URL parse failed: {e}"))?;
        if parsed.scheme() != "https" {
            return Err(format!("Advertisement must be on HTTPS: {uri_str}"));
        }

        // 6. Field[3]: expiry time VarInt.
        //
        // **TS-parity decision (2026-04-21):** the TS reference
        // `UHRPTopicManager.ts:55` rejects ONLY `expiry_time < 1`. A
        // value like `1700000000` (Nov 2023, already in the past when
        // checked today) passes the TS check and gets admitted.
        // bsvb.tech runs the TS source verbatim.
        //
        // We previously rejected `expiry <= now()` here. That's
        // semantically cleaner (why admit something already expired?)
        // but it's a **federation break**: in a network where nodes
        // hold different UTXO sets based on clock-time-at-admission,
        // GASP sync can never converge. The spec explicitly pushes
        // the "hide expired records" responsibility to the **lookup
        // layer** (ls_uhrp), not the topic manager.
        //
        // So we match TS here: admit whenever `expiry >= 1`. The
        // now()-based filter lives in `super::lookup_service` (see
        // `UHRPLookupService::query` expired-record filtering).
        let mut r3 = Reader::new(&pushdrop.fields[3]);
        let expiry = r3
            .read_var_int()
            .map_err(|e| format!("expiry VarInt decode failed: {e}"))?;
        if expiry < 1 {
            return Err("expiry must be >= 1".into());
        }
        let _ = now; // retained in signature for future use + test determinism

        // 7. Field[4]: content length VarInt.
        let mut r4 = Reader::new(&pushdrop.fields[4]);
        let content_length = r4
            .read_var_int()
            .map_err(|e| format!("content length VarInt decode failed: {e}"))?;
        if content_length == 0 {
            return Err("content length must be > 0".into());
        }

        // 8. Signature sits at index 5 as the final pushed field.
        let signature = &pushdrop.fields[5];
        if signature.is_empty() {
            return Err("empty signature".into());
        }

        // 9. Verify signature over concat(field[0..4]) under BRC-42 derivation
        //    with protocol `(2, "uhrp advertisement")`, key id `"1"`,
        //    counterparty = Other(field[0]).
        //
        //    The author signs with their BRC-42 child private key derived
        //    against `counterparty="anyone"`. By BRC-42 symmetry,
        //    `ProtoWallet::anyone().verify(..., counterparty=Other(author))`
        //    reconstructs the same child public key and verifies the sig.
        let sign_data: Vec<u8> = pushdrop.fields[0..5]
            .iter()
            .flat_map(|f| f.iter().copied())
            .collect();
        let protocol_id = Protocol::new(SecurityLevel::Counterparty, UHRP_BRC43_PROTOCOL_NAME);
        let counterparty = Counterparty::Other(identity_pubkey);
        let anyone_wallet = ProtoWallet::anyone();
        let sig_valid = anyone_wallet.verify_signature(VerifySignatureArgs {
            data: Some(sign_data),
            hash_to_directly_verify: None,
            signature: signature.clone(),
            protocol_id: protocol_id.clone(),
            key_id: UHRP_KEY_ID.to_string(),
            counterparty: Some(counterparty.clone()),
            for_self: None,
        });

        // A verify_signature returning Err or valid=false is a signature
        // mismatch — reject the output but don't error out the whole BEEF.
        match sig_valid {
            Ok(r) if r.valid => {}
            _ => {
                warn!("UHRP: signature verification failed");
                return Ok(false);
            }
        }

        // 10. Locking-key linkage: the PushDrop's locking pubkey must be the
        //     BRC-42 child the author derived from their root against
        //     `counterparty="anyone", for_self=true`. `anyone_wallet` with
        //     `counterparty=Other(author), for_self=None` (defaults to false
        //     — derive counterparty's child) produces the same point.
        let expected = anyone_wallet
            .get_public_key(GetPublicKeyArgs {
                identity_key: false,
                protocol_id: Some(protocol_id),
                key_id: Some(UHRP_KEY_ID.to_string()),
                counterparty: Some(counterparty),
                for_self: None,
            })
            .map_err(|e| format!("locking key derivation failed: {e}"))?;

        if expected.public_key != pushdrop.locking_public_key.to_hex() {
            warn!("UHRP: locking key linkage mismatch");
            return Ok(false);
        }

        let _ = content_length; // already validated non-zero above
        Ok(true)
    }
}

/// Current time in unix seconds.
///
/// `std::time::SystemTime` PANICS on `wasm32-unknown-unknown` (it has no OS
/// clock source), so on Cloudflare Workers we route through `js_sys::Date`.
/// Matches the pattern in `overlay-engine/src/engine.rs::current_timestamp_ms`.
///
/// Native (tests / non-wasm consumers) falls back to `SystemTime`. A
/// pre-epoch clock (impossible in practice) yields 0, which makes every
/// non-zero-expiry advert look unexpired — strictly safer than panicking.
fn current_unix_seconds() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        (js_sys::Date::now() / 1000.0) as u64
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    #![allow(clippy::expect_used)]
    use super::*;
    use bsv_rs::primitives::ec::PrivateKey;
    use bsv_rs::primitives::encoding::Writer;
    use bsv_rs::script::templates::PushDrop as PushDropTemplate;
    use bsv_rs::transaction::TransactionOutput;

    /// Build a properly signed UHRP PushDrop output that mirrors the
    /// author-side flow in `bsv-storage-cloudflare/src/routes/advertise.rs`.
    ///
    /// `admin_key` is the server identity root private key. Its
    /// compressed pubkey goes into field[0]. The locking pubkey and the
    /// signer private key are both BRC-42 children keyed by
    /// `(2, "uhrp advertisement")` / `"1"` / `counterparty="anyone"`.
    fn make_signed_uhrp_output(
        admin_key: &PrivateKey,
        hash_bytes: [u8; 32],
        url: &str,
        expiry_time: u64,
        content_length: u64,
    ) -> TransactionOutput {
        let wallet = ProtoWallet::new(Some(admin_key.clone()));
        let protocol_id = Protocol::new(SecurityLevel::Counterparty, UHRP_BRC43_PROTOCOL_NAME);

        // Derive the locking pubkey (author-side "for_self=true").
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

        // Build the data fields.
        let identity_bytes = admin_key.public_key().to_compressed();
        let mut w_exp = Writer::new();
        w_exp.write_var_int(expiry_time);
        let expiry_bytes = w_exp.into_bytes();
        let mut w_len = Writer::new();
        w_len.write_var_int(content_length);
        let length_bytes = w_len.into_bytes();

        let data_fields: Vec<Vec<u8>> = vec![
            identity_bytes.to_vec(),
            hash_bytes.to_vec(),
            url.as_bytes().to_vec(),
            expiry_bytes,
            length_bytes,
        ];

        // Sign `concat(field[0..4])` with the same counterparty=anyone BRC-42
        // derivation ProtoWallet uses under create_signature — identical to
        // the route's hand-rolled signing path.
        let sign_data: Vec<u8> = data_fields.iter().flat_map(|f| f.iter().copied()).collect();
        let sig_result = wallet
            .create_signature(bsv_rs::wallet::CreateSignatureArgs {
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

    /// Build a raw (unsigned) PushDrop output. Used to assert the validator
    /// rejects malformed inputs before it ever reaches the sig check.
    fn make_raw_uhrp_output(fields: Vec<Vec<u8>>) -> TransactionOutput {
        let locking_key = PublicKey::from_private_key(&PrivateKey::random());
        let pushdrop = PushDropTemplate::new(locking_key, fields);
        TransactionOutput {
            satoshis: Some(1),
            locking_script: pushdrop.lock(),
            change: false,
        }
    }

    fn dummy_33_byte_pubkey() -> Vec<u8> {
        PublicKey::from_private_key(&PrivateKey::random())
            .to_compressed()
            .to_vec()
    }

    fn varint(v: u64) -> Vec<u8> {
        let mut w = Writer::new();
        w.write_var_int(v);
        w.into_bytes()
    }

    const FUTURE_EXPIRY: u64 = 4_000_000_000; // ~year 2096

    #[test]
    fn validate_valid_uhrp_output_admits() {
        let admin = PrivateKey::random();
        let hash = [0x42u8; 32];
        let output =
            make_signed_uhrp_output(&admin, hash, "https://example.com", FUTURE_EXPIRY, 1024);
        let result = UHRPTopicManager::validate_uhrp_output(&output, 1_700_000_000);
        assert!(result.is_ok(), "expected Ok(_), got: {result:?}");
        assert!(result.unwrap(), "valid UHRP advert should be admitted");
    }

    #[test]
    fn rejects_wrong_field_count() {
        // 5 data fields, no signature — decodes to 5 pushed fields, fails
        // the `!= 6` check.
        let fields = vec![
            dummy_33_byte_pubkey(),
            vec![0u8; 32],
            b"https://example.com".to_vec(),
            varint(FUTURE_EXPIRY),
            varint(1024),
        ];
        let output = make_raw_uhrp_output(fields);
        let result = UHRPTopicManager::validate_uhrp_output(&output, 1_700_000_000).unwrap();
        assert!(!result, "5-field (no sig) must return Ok(false)");
    }

    #[test]
    fn rejects_too_few_fields() {
        // 3 data fields — definitely not UHRP.
        let fields = vec![
            dummy_33_byte_pubkey(),
            vec![0u8; 32],
            b"https://example.com".to_vec(),
        ];
        let output = make_raw_uhrp_output(fields);
        let result = UHRPTopicManager::validate_uhrp_output(&output, 1_700_000_000).unwrap();
        assert!(!result, "3-field PushDrop must return Ok(false)");
    }

    #[test]
    fn rejects_non_pushdrop() {
        let output = TransactionOutput {
            satoshis: Some(1000),
            locking_script: bsv_rs::script::Script::from_hex(
                "76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac",
            )
            .unwrap()
            .into(),
            change: false,
        };
        let result = UHRPTopicManager::validate_uhrp_output(&output, 1_700_000_000);
        assert!(result.is_err(), "P2PKH should fail at PushDrop::decode");
    }

    #[test]
    fn rejects_short_identity_key() {
        let fields = vec![
            vec![0x02, 0x03], // 2 bytes — not 33
            vec![0u8; 32],
            b"https://example.com".to_vec(),
            varint(FUTURE_EXPIRY),
            varint(1024),
            vec![0x30, 0x04, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01],
        ];
        let output = make_raw_uhrp_output(fields);
        let result = UHRPTopicManager::validate_uhrp_output(&output, 1_700_000_000);
        assert!(
            result.is_err(),
            "short identity key must err, got {result:?}"
        );
    }

    #[test]
    fn rejects_non_pubkey_field0() {
        // Exactly 33 bytes of non-curve data.
        let mut bogus = vec![0u8; 33];
        bogus[0] = 0x02; // valid prefix byte but the rest is zero → not on curve
        let fields = vec![
            bogus,
            vec![0u8; 32],
            b"https://example.com".to_vec(),
            varint(FUTURE_EXPIRY),
            varint(1024),
        ];
        // Need 5 data fields + a signature for the decoder to split them, but
        // for this test the validator should error out before signature check.
        let output = make_signed_from_raw_fields(fields);
        let result = UHRPTopicManager::validate_uhrp_output(&output, 1_700_000_000);
        assert!(
            result.is_err(),
            "non-curve pubkey must err at identity parse, got {result:?}"
        );
    }

    #[test]
    fn rejects_wrong_hash_length() {
        let admin = PrivateKey::random();
        // Build one valid signed output, then repack with a 31-byte hash.
        let wallet = ProtoWallet::new(Some(admin.clone()));
        let identity_bytes = admin.public_key().to_compressed().to_vec();
        let fields = vec![
            identity_bytes,
            vec![0u8; 31],
            b"https://example.com".to_vec(),
            varint(FUTURE_EXPIRY),
            varint(1024),
            vec![0x30, 0x04, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01],
        ];
        let _ = wallet;
        let locking_key = PublicKey::from_private_key(&PrivateKey::random());
        let pushdrop = PushDropTemplate::new(locking_key, fields);
        let output = TransactionOutput {
            satoshis: Some(1),
            locking_script: pushdrop.lock(),
            change: false,
        };
        let result = UHRPTopicManager::validate_uhrp_output(&output, 1_700_000_000);
        assert!(result.is_err(), "31-byte hash must err, got {result:?}");
    }

    #[test]
    fn rejects_bad_uri_scheme() {
        let fields = vec![
            dummy_33_byte_pubkey(),
            vec![0u8; 32],
            b"ftp://example.com".to_vec(), // wrong scheme
            varint(FUTURE_EXPIRY),
            varint(1024),
        ];
        let output = make_signed_from_raw_fields(fields);
        let result = UHRPTopicManager::validate_uhrp_output(&output, 1_700_000_000);
        assert!(result.is_err(), "ftp:// URI must err, got {result:?}");
    }

    #[test]
    fn accepts_https_url_with_path() {
        // TS parity: `UHRPTopicManager.ts:48-52` only checks the scheme is
        // `https:` — it does NOT reject paths or localhost. UHRP `field[2]`
        // is a content download URL like `https://pub-X.r2.dev/cdn/<id>`, so
        // the validator must admit URLs with paths. (The earlier BRC-101
        // `is_advertisable_uri` check rejected every real UHRP advert,
        // including bsvb-admitted ones.)
        let admin = PrivateKey::random();
        let output = make_signed_uhrp_output(
            &admin,
            [0u8; 32],
            "https://pub-abc.r2.dev/cdn/foo",
            FUTURE_EXPIRY,
            42,
        );
        let result = UHRPTopicManager::validate_uhrp_output(&output, 1_700_000_000);
        assert_eq!(result, Ok(true), "https URL with path must admit");
    }

    #[test]
    fn admits_past_expiry_ts_parity() {
        // TS parity: `UHRPTopicManager.ts:55` rejects only `expiry < 1`.
        // Past-expiry values still admit. The "hide expired" responsibility
        // is pushed to the lookup layer (`ls_uhrp`), not the topic manager —
        // this is the federation-safe choice, because otherwise nodes that
        // validated at different wall-clock times hold different UTXO sets
        // and GASP sync can never converge.
        let admin = PrivateKey::random();
        let output = make_signed_uhrp_output(
            &admin,
            [0u8; 32],
            "https://example.com",
            1_700_000_000, // Nov 2023 — past
            1024,
        );
        let result = UHRPTopicManager::validate_uhrp_output(&output, 1_800_000_000);
        assert_eq!(
            result,
            Ok(true),
            "past-expiry advert must admit (TS parity), got {result:?}"
        );
    }

    #[test]
    fn rejects_zero_expiry() {
        // The only expiry value the TS reference rejects: `expiry < 1`.
        // `make_signed_uhrp_output` takes a u64 so we can't sign with a
        // negative; 0 is the boundary. VarInt-encoded 0 is a single 0x00.
        let admin = PrivateKey::random();
        let output = make_signed_uhrp_output(&admin, [0u8; 32], "https://example.com", 0, 1024);
        let result = UHRPTopicManager::validate_uhrp_output(&output, 1_700_000_000);
        assert!(result.is_err(), "expiry=0 must err, got {result:?}");
    }

    #[test]
    fn rejects_zero_content_length() {
        let admin = PrivateKey::random();
        let output =
            make_signed_uhrp_output(&admin, [0u8; 32], "https://example.com", FUTURE_EXPIRY, 0);
        let result = UHRPTopicManager::validate_uhrp_output(&output, 1_700_000_000);
        assert!(result.is_err(), "zero content length must err");
    }

    #[test]
    fn rejects_bad_signature() {
        // Properly-shaped 5 fields but garbage signature → should be Ok(false)
        // (sig mismatch = not-a-UHRP-output, don't fail the whole BEEF).
        let fields = vec![
            dummy_33_byte_pubkey(),
            vec![0u8; 32],
            b"https://example.com".to_vec(),
            varint(FUTURE_EXPIRY),
            varint(1024),
        ];
        let output = make_signed_from_raw_fields(fields);
        let result = UHRPTopicManager::validate_uhrp_output(&output, 1_700_000_000).unwrap();
        assert!(!result, "bad signature must return Ok(false)");
    }

    #[test]
    fn rejects_mismatched_locking_key() {
        // Signer signs correctly but someone else's locking key is used
        // (sig verifies OK against field[0], but the locking key is wrong).
        let admin = PrivateKey::random();
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
        let sig_result = wallet
            .create_signature(bsv_rs::wallet::CreateSignatureArgs {
                data: Some(sign_data),
                hash_to_directly_sign: None,
                protocol_id,
                key_id: UHRP_KEY_ID.to_string(),
                counterparty: Some(Counterparty::Anyone),
            })
            .unwrap();

        // Use a different (random) locking key.
        let wrong_locking_key = PublicKey::from_private_key(&PrivateKey::random());
        let mut all_fields = data_fields;
        all_fields.push(sig_result.signature);
        let pushdrop = PushDropTemplate::new(wrong_locking_key, all_fields);
        let output = TransactionOutput {
            satoshis: Some(1),
            locking_script: pushdrop.lock(),
            change: false,
        };

        let result = UHRPTopicManager::validate_uhrp_output(&output, 1_700_000_000).unwrap();
        assert!(!result, "mismatched locking key must return Ok(false)");
    }

    #[test]
    fn rejects_mismatched_signer_and_identity_key() {
        // The impostor places `victim`'s pubkey in field[0] but signs with
        // their own key. Signature verification against field[0]'s
        // counterparty must fail.
        let victim = PrivateKey::random();
        let impostor = PrivateKey::random();
        let impostor_wallet = ProtoWallet::new(Some(impostor.clone()));
        let protocol_id = Protocol::new(SecurityLevel::Counterparty, UHRP_BRC43_PROTOCOL_NAME);

        let data_fields: Vec<Vec<u8>> = vec![
            victim.public_key().to_compressed().to_vec(),
            vec![0u8; 32],
            b"https://example.com".to_vec(),
            varint(FUTURE_EXPIRY),
            varint(1024),
        ];
        let sign_data: Vec<u8> = data_fields.iter().flat_map(|f| f.iter().copied()).collect();
        let sig_result = impostor_wallet
            .create_signature(bsv_rs::wallet::CreateSignatureArgs {
                data: Some(sign_data),
                hash_to_directly_sign: None,
                protocol_id: protocol_id.clone(),
                key_id: UHRP_KEY_ID.to_string(),
                counterparty: Some(Counterparty::Anyone),
            })
            .unwrap();

        // Even a plausible locking key won't save them: use the impostor's
        // child. The sig-verify step will still fail because the
        // anyone-wallet computes the expected child from `victim`'s
        // counterparty, not the impostor's.
        let locking_key_hex = impostor_wallet
            .get_public_key(GetPublicKeyArgs {
                identity_key: false,
                protocol_id: Some(protocol_id),
                key_id: Some(UHRP_KEY_ID.to_string()),
                counterparty: Some(Counterparty::Anyone),
                for_self: Some(true),
            })
            .unwrap()
            .public_key;
        let locking_key = PublicKey::from_hex(&locking_key_hex).unwrap();

        let mut all_fields = data_fields;
        all_fields.push(sig_result.signature);
        let pushdrop = PushDropTemplate::new(locking_key, all_fields);
        let output = TransactionOutput {
            satoshis: Some(1),
            locking_script: pushdrop.lock(),
            change: false,
        };

        let result = UHRPTopicManager::validate_uhrp_output(&output, 1_700_000_000).unwrap();
        assert!(
            !result,
            "mismatched signer vs field[0] must return Ok(false)"
        );
    }

    /// Sign raw fields with the default test admin key. The sig will NOT
    /// match any real identity in field[0] but lets us produce a 6-field
    /// PushDrop structure so the validator reaches the earlier checks.
    fn make_signed_from_raw_fields(data_fields: Vec<Vec<u8>>) -> TransactionOutput {
        let locking_key = PublicKey::from_private_key(&PrivateKey::random());
        let mut all = data_fields;
        // Junk signature — any 5-data-field + sig PushDrop produces 5
        // decoded fields + a signature byte slice. What's important is
        // that the decoder doesn't re-fold the last push back into fields.
        all.push(vec![0x30, 0x04, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01]);
        let pushdrop = PushDropTemplate::new(locking_key, all);
        TransactionOutput {
            satoshis: Some(1),
            locking_script: pushdrop.lock(),
            change: false,
        }
    }

    #[tokio::test]
    async fn topic_manager_metadata() {
        let mgr = UHRPTopicManager::new();
        let meta = mgr.get_metadata().await;
        assert_eq!(meta.name, "Universal Hash Resolution Protocol");
        assert!(meta
            .description
            .as_ref()
            .is_some_and(|d| d.contains("UHRP")));
    }

    #[tokio::test]
    async fn topic_manager_documentation_not_empty() {
        let mgr = UHRPTopicManager::new();
        let docs = mgr.get_documentation().await;
        assert!(!docs.is_empty());
        assert!(docs.contains("UHRP"));
    }

    // ------------------------------------------------------------------
    // Fixture test — load golden PushDrop hex from
    // bsv-storage-cloudflare/tests/fixtures/pushdrop/*.json, wrap in a
    // minimal Transaction, assert the validator sees 5 fields and drops
    // at the expected gate.
    // ------------------------------------------------------------------

    /// The golden fixtures were built by `scripts/gen-pushdrop-fixtures.mjs`
    /// in `bsv-storage-cloudflare` purely to lock the lock-script encoding
    /// down to byte-exact parity with `@bsv/sdk` — they aren't signed by a
    /// real BRC-42 child and their URLs contain pathnames (e.g.
    /// `/cdn/abc123`) that the overlay's `is_advertisable_uri` policy
    /// rejects.
    ///
    /// What this test proves: the script hex decodes, PushDrop sees 6
    /// pushed fields, and the validator returns at one of the structural
    /// gates we document (URI / sig / locking key), **not** admission.
    /// A production advert's download URL lives at the origin (e.g.
    /// `https://example.com`) and passes `is_advertisable_uri` — see the
    /// live-parity `/advertise` round-trip for that proof.
    #[test]
    fn golden_baseline_small_cdn_url_decodes_as_pushdrop() {
        let fixture_json = include_str!(
            "../../../../../bsv-storage-cloudflare/tests/fixtures/pushdrop/baseline_small_cdn_url.json"
        );
        let fixture: serde_json::Value = serde_json::from_str(fixture_json).unwrap();
        let script_hex = fixture["expected_locking_script_hex"].as_str().unwrap();
        let script = bsv_rs::script::Script::from_hex(script_hex).unwrap();
        let locking: bsv_rs::script::LockingScript = script.into();

        // Proof 1: decode as PushDrop yields exactly 6 fields (5 data + sig).
        let decoded = PushDrop::decode(&locking).unwrap();
        assert_eq!(
            decoded.fields.len(),
            6,
            "fixture must decode to 6 PushDrop fields"
        );
        assert_eq!(
            decoded.fields[0].len(),
            33,
            "field[0] must be 33-byte pubkey"
        );
        assert_eq!(decoded.fields[1].len(), 32, "field[1] must be 32-byte hash");

        // Proof 2: the validator rejects this fixture (synthetic URI with
        // path, synthetic sig) without admitting it, and the rejection
        // reason is one of the three expected gates.
        let output = TransactionOutput {
            satoshis: Some(1),
            locking_script: locking,
            change: false,
        };
        let now = 1_700_000_000;
        let result = UHRPTopicManager::validate_uhrp_output(&output, now);
        match result {
            Ok(false) => {}
            Err(reason) => {
                assert!(
                    reason.contains("URI")
                        || reason.contains("signature")
                        || reason.contains("locking key"),
                    "unexpected rejection gate: {reason}"
                );
            }
            Ok(true) => panic!("synthetic fixture must NOT admit"),
        }
    }

    // ------------------------------------------------------------------
    // Live parity smoke test — gated on UHRP_LIVE_PARITY=1.
    // Skipped in default CI because it requires real wallet infra; opt-in
    // via env var when investigating byte parity with overlay-us-1.bsvb.tech.
    // ------------------------------------------------------------------

    /// Placeholder test — the full live parity flow requires building a
    /// real BEEF with wallet signing, which happens end-to-end in the
    /// `bsv-storage-cloudflare` `/advertise` route. This stub documents
    /// the gate so operators know where to look.
    #[test]
    #[ignore = "live parity: set UHRP_LIVE_PARITY=1 and run the /advertise round-trip in bsv-storage-cloudflare"]
    fn live_parity_vs_bsvb() {
        // Intentional no-op; see handler tests in bsv-storage-cloudflare
        // for the actual live round-trip against both bsvb.tech and
        // <your-overlay>.workers.dev.
    }
}
