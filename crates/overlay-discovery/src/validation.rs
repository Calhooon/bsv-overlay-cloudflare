//! Validation utilities for SHIP/SLAP overlay discovery.
//!
//! Ported from `~/bsv/overlay-discovery-services/src/utils/`:
//! - `isValidTopicOrServiceName.ts` -- topic and lookup-service name validation
//! - `isAdvertisableURI.ts` -- URI scheme validation
//! - `isTokenSignatureCorrectlyLinked.ts` -- ECDSA signature linking

/// The most bytes a topic or lookup-service name may hold: the reference's 256.
pub const MAX_TOPIC_OR_SERVICE_NAME_BYTES: usize = 256;

/// Check if a topic manager or lookup service name is canonical (BRC-87 names
/// and the per-asset BRC-207 names, `tm_color_<txid>_<vout>`).
///
/// The reference's rule, `isValidTopicOrServiceName` of
/// `@bsv/overlay-discovery-services` at `bsv-blockchain/ts-stack`
/// `57c5f13055b555b83c1ef368fff3650abd21b6ef` (its head on 2026-11-28; the rule
/// as it stands since its 2026-10-22 change), is exactly
///
/// ```text
/// /^(?=.{1,256}$)(?:tm_|ls_)[a-z0-9]+(?:_[a-z0-9]+)*$/
/// ```
///
/// - the `tm_` (topic) or `ls_` (lookup service) prefix;
/// - then groups of ASCII lowercase letters and digits, joined by single
///   underscores: no empty group, so no doubled and no trailing underscore;
/// - 1 to 256 in all. The reference's `.{1,256}` counts UTF-16 code units;
///   this counts bytes. Every character the rule admits is ASCII, one byte
///   and one code unit, so for every admitted name the two counts are equal,
///   and a name with any other character is refused by the alphabet whatever
///   its length.
///
/// No allocation: one pass over the bytes.
///
/// # Examples
/// ```
/// use bsv_overlay_discovery::validation::is_valid_topic_or_service_name;
/// assert!(is_valid_topic_or_service_name("tm_ship"));
/// assert!(is_valid_topic_or_service_name("ls_slap"));
/// assert!(is_valid_topic_or_service_name("tm_my_custom_topic"));
/// assert!(is_valid_topic_or_service_name("tm_uhrp_files2"));
/// assert!(is_valid_topic_or_service_name(&format!("ls_{}", "a".repeat(253))));
/// assert!(!is_valid_topic_or_service_name(&format!("ls_{}", "a".repeat(254))));
/// assert!(!is_valid_topic_or_service_name("invalid"));
/// assert!(!is_valid_topic_or_service_name("tm_"));
/// assert!(!is_valid_topic_or_service_name("TM_UPPER"));
/// ```
pub fn is_valid_topic_or_service_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.len() > MAX_TOPIC_OR_SERVICE_NAME_BYTES {
        return false;
    }
    let rest = match bytes {
        [b't', b'm', b'_', rest @ ..] | [b'l', b's', b'_', rest @ ..] => rest,
        _ => return false,
    };

    // [a-z0-9]+(?:_[a-z0-9]+)*: an underscore only after a letter or digit,
    // and the name ends on a letter or digit.
    let mut group_open = false;
    for &b in rest {
        match b {
            b'a'..=b'z' | b'0'..=b'9' => group_open = true,
            b'_' if group_open => group_open = false,
            _ => return false,
        }
    }
    group_open
}

/// Recognized HTTPS-based URI scheme prefixes.
const HTTPS_SCHEMES: &[&str] = &[
    "https://",
    "https+bsvauth://",
    "https+bsvauth+smf://",
    "https+bsvauth+scrypt-offchain://",
    "https+rtt://",
];

/// Check if a URI is advertisable per BRC-101 overlay advertisement spec.
///
/// Supported schemes:
/// - `https://` -- standard HTTPS
/// - `https+bsvauth://` -- HTTPS with BSV auth (no payment)
/// - `https+bsvauth+smf://` -- HTTPS with BSV auth + payment
/// - `https+bsvauth+scrypt-offchain://` -- HTTPS with sCrypt off-chain context
/// - `https+rtt://` -- HTTPS for real-time transactions
/// - `wss://` -- WebSocket secure (for streaming lookups)
/// - `js8c+bsvauth+smf:` -- HF radio discovery (geo-located)
///
/// Common rules:
/// - `localhost` is disallowed as hostname
/// - No pathname other than `/`
pub fn is_advertisable_uri(uri: &str) -> bool {
    let uri = uri.trim();
    if uri.is_empty() {
        return false;
    }

    // HTTPS-based schemes
    for &scheme in HTTPS_SCHEMES {
        if uri.starts_with(scheme) {
            return validate_https_uri(uri, scheme);
        }
    }

    // WSS scheme
    if uri.starts_with("wss://") {
        return validate_wss_uri(uri);
    }

    // JS8 Call scheme
    if uri.starts_with("js8c+bsvauth+smf:") {
        return validate_js8c_uri(uri);
    }

    false
}

/// Validate an HTTPS-based URI by substituting the custom scheme with https://.
fn validate_https_uri(uri: &str, prefix: &str) -> bool {
    let normalized = format!("https://{}", &uri[prefix.len()..]);
    match url::Url::parse(&normalized) {
        Ok(parsed) => {
            if parsed
                .host_str()
                .is_none_or(|h| h.eq_ignore_ascii_case("localhost"))
            {
                return false;
            }
            parsed.path() == "/"
        }
        Err(_) => false,
    }
}

/// Validate a WSS URI.
fn validate_wss_uri(uri: &str) -> bool {
    // url crate doesn't know wss:// natively, substitute with https:// for parsing
    let normalized = format!("https://{}", &uri["wss://".len()..]);
    match url::Url::parse(&normalized) {
        Ok(parsed) => !parsed
            .host_str()
            .is_none_or(|h| h.eq_ignore_ascii_case("localhost")),
        Err(_) => false,
    }
}

/// Validate a JS8 Call URI (js8c+bsvauth+smf:?lat=X&long=Y&freq=Z&radius=R).
fn validate_js8c_uri(uri: &str) -> bool {
    let query_index = match uri.find('?') {
        Some(i) => i,
        None => return false,
    };

    let query_str = &uri[query_index + 1..];
    let params: std::collections::HashMap<String, String> = query_str
        .split('&')
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?.to_string();
            let value = parts.next()?.to_string();
            Some((key, value))
        })
        .collect();

    let lat: f64 = match params.get("lat").and_then(|v| v.parse().ok()) {
        Some(v) if (-90.0..=90.0).contains(&v) => v,
        _ => return false,
    };

    let lon: f64 = match params.get("long").and_then(|v| v.parse().ok()) {
        Some(v) if (-180.0..=180.0).contains(&v) => v,
        _ => return false,
    };

    let freq_str = match params.get("freq") {
        Some(v) => v,
        None => return false,
    };
    let freq: f64 = match extract_positive_number(freq_str) {
        Some(v) => v,
        None => return false,
    };

    let radius_str = match params.get("radius") {
        Some(v) => v,
        None => return false,
    };
    let radius: f64 = match extract_positive_number(radius_str) {
        Some(v) => v,
        None => return false,
    };

    let _ = (lat, lon, freq, radius); // all validated
    true
}

/// Extract the first positive number from a string (mirrors TS regex /(\d+(\.\d+)?)/).
fn extract_positive_number(s: &str) -> Option<f64> {
    let mut start = None;
    let mut has_dot = false;

    for (i, ch) in s.char_indices() {
        if ch.is_ascii_digit() {
            if start.is_none() {
                start = Some(i);
            }
        } else if ch == '.' && !has_dot && start.is_some() {
            has_dot = true;
        } else if start.is_some() {
            break;
        }
    }

    let start = start?;
    let end = s[start..]
        .char_indices()
        .find(|(_, ch)| !ch.is_ascii_digit() && *ch != '.')
        .map_or(s.len(), |(i, _)| start + i);

    let num: f64 = s[start..end].parse().ok()?;
    if num > 0.0 {
        Some(num)
    } else {
        None
    }
}

/// Checks that a PushDrop token's ECDSA signature is valid and that the
/// locking key matches the BRC-42 child derived from the claimed identity key.
///
/// This mirrors the TS `isTokenSignatureCorrectlyLinked()` in
/// `overlay-discovery-services/src/utils/isTokenSignatureCorrectlyLinked.ts`.
///
/// # Arguments
///
/// * `locking_key` -- the public key embedded in the PushDrop locking script
/// * `identity_key_bytes` -- 33-byte compressed pubkey from field\[1\]
/// * `fields` -- all PushDrop fields *including* the signature at the end
/// * `protocol_name` -- `"SHIP"` or `"SLAP"` (field\[0\] text)
///
/// # Returns
///
/// `Ok(true)` when the signature is valid **and** the locking key matches,
/// `Ok(false)` for any cryptographic mismatch, `Err` for malformed inputs.
pub fn is_token_signature_correctly_linked(
    locking_key: &bsv_rs::primitives::ec::PublicKey,
    identity_key_bytes: &[u8],
    fields: &[Vec<u8>],
    protocol_name: &str,
) -> Result<bool, String> {
    is_token_signature_correctly_linked_verbose(
        locking_key,
        identity_key_bytes,
        fields,
        protocol_name,
        &mut |_| {},
    )
}

/// Verbose variant — same as `is_token_signature_correctly_linked` but emits
/// step-by-step diagnostic strings to the provided callback. Used by
/// parity-harness plumbing in `overlay-cloudflare/src/routes.rs` so we can
/// surface divergence reasons via `worker::console_log!` from the wasm
/// runtime (tracing::warn! isn't wired up in the Worker environment).
pub fn is_token_signature_correctly_linked_verbose(
    locking_key: &bsv_rs::primitives::ec::PublicKey,
    identity_key_bytes: &[u8],
    fields: &[Vec<u8>],
    protocol_name: &str,
    log: &mut dyn FnMut(String),
) -> Result<bool, String> {
    use bsv_rs::wallet::{
        Counterparty, GetPublicKeyArgs, ProtoWallet, Protocol, SecurityLevel, VerifySignatureArgs,
    };

    // AGENT keeps its legacy stderr-verbose branch for native tests.
    let verbose = protocol_name == "AGENT";

    // Need at least 2 fields (data fields + signature)
    if fields.len() < 2 {
        return Err("fields must contain data fields plus a signature".into());
    }

    // The signature is the last field
    let signature = &fields[fields.len() - 1];
    // The data fields are everything before the signature
    let data_fields = &fields[..fields.len() - 1];

    // Concatenate data fields into a single byte array (matches TS reduce)
    let data: Vec<u8> = data_fields.iter().flat_map(|f| f.iter().copied()).collect();

    if verbose {
        eprintln!("=== AGENT SIG VERIFY ===");
        eprintln!("  locking_key: {}", locking_key.to_hex());
        eprintln!("  identity_key: {}", hex::encode(identity_key_bytes));
        eprintln!("  field_count: {}", fields.len());
        for (i, f) in fields.iter().enumerate() {
            if f.len() <= 66 {
                eprintln!("  field[{}]: {} ({} bytes)", i, hex::encode(f), f.len());
            } else {
                eprintln!(
                    "  field[{}]: {}...{} ({} bytes)",
                    i,
                    hex::encode(&f[..20]),
                    hex::encode(&f[f.len() - 4..]),
                    f.len()
                );
            }
        }
        eprintln!(
            "  sign_data: {} bytes, sha256={}",
            data.len(),
            hex::encode(bsv_rs::primitives::hash::sha256(&data))
        );
        eprintln!("  signature: {} bytes", signature.len());
    }

    // Determine the BRC-43 protocol name from the short protocol identifier
    let brc43_protocol = match protocol_name {
        "SHIP" => "service host interconnect",
        "SLAP" => "service lookup availability",
        "AGENT" => "agent registry",
        // LOW poker lobby tokens (tm_low): TABLE_OPEN + GAME_UTXO records.
        // BRC-43 protocol ID on the signer side is [2, "low poker lobby"].
        "LOW" => "low poker lobby",
        other => return Err(format!("unknown protocol: {other}")),
    };

    // Parse the identity key from bytes
    let identity_pubkey = bsv_rs::primitives::ec::PublicKey::from_bytes(identity_key_bytes)
        .map_err(|e| format!("invalid identity key: {e}"))?;

    // Security level 2 = Counterparty (matches TS [2, ...])
    let protocol_id = Protocol::new(SecurityLevel::Counterparty, brc43_protocol);
    let counterparty = Counterparty::Other(identity_pubkey);

    let anyone_wallet = ProtoWallet::anyone();

    // Diagnostic: surface the derived-vs-locking comparison even when
    // signature verification fails, so failures can be distinguished
    // (BRC-42 derivation drift vs ECDSA-verify drift vs a mismatched
    // identity/locking key pair on the input record itself).
    match anyone_wallet.get_public_key(GetPublicKeyArgs {
        identity_key: false,
        protocol_id: Some(protocol_id.clone()),
        key_id: Some("1".to_string()),
        counterparty: Some(counterparty.clone()),
        for_self: None,
    }) {
        Ok(r) => log(format!(
            "  derived_key={} locking_key={} match={}",
            r.public_key,
            locking_key.to_hex(),
            r.public_key == locking_key.to_hex()
        )),
        Err(e) => log(format!("  derivation failed: {e}")),
    }

    // 1. Verify the signature over the concatenated data fields
    let sig_valid = anyone_wallet.verify_signature(VerifySignatureArgs {
        data: Some(data),
        hash_to_directly_verify: None,
        signature: signature.clone(),
        protocol_id: protocol_id.clone(),
        key_id: "1".to_string(),
        counterparty: Some(counterparty.clone()),
        for_self: None,
    });

    match sig_valid {
        Ok(result) if result.valid => {
            if verbose {
                eprintln!("  CHECK 1 (signature): PASS");
            }
            log("  CHECK 1 (signature): PASS".to_string());
        }
        Ok(_) => {
            if verbose {
                eprintln!("  CHECK 1 (signature): FAIL (valid=false)");
            }
            log("  CHECK 1 (signature): FAIL (valid=false)".to_string());
            return Ok(false);
        }
        Err(ref e) => {
            if verbose {
                eprintln!("  CHECK 1 (signature): FAIL (err: {})", e);
            }
            log(format!("  CHECK 1 (signature): FAIL (err: {})", e));
            return Ok(false);
        }
    }

    // 2. Derive the expected locking key and compare with actual
    let expected = anyone_wallet
        .get_public_key(GetPublicKeyArgs {
            identity_key: false,
            protocol_id: Some(protocol_id),
            key_id: Some("1".to_string()),
            counterparty: Some(counterparty),
            for_self: None, // defaults to false -- derive counterparty's child
        })
        .map_err(|e| format!("key derivation failed: {e}"))?;

    let key_match = expected.public_key == locking_key.to_hex();
    if verbose {
        eprintln!("  CHECK 2 (locking key): expected={}", expected.public_key);
        eprintln!("  CHECK 2 (locking key): actual  ={}", locking_key.to_hex());
        eprintln!(
            "  CHECK 2 (locking key): {}",
            if key_match { "PASS" } else { "FAIL" }
        );
    }
    log(format!("  CHECK 2 (key) expected={}", expected.public_key));
    log(format!("  CHECK 2 (key) actual  ={}", locking_key.to_hex()));
    log(format!(
        "  CHECK 2 (key): {}",
        if key_match { "PASS" } else { "FAIL" }
    ));

    Ok(key_match)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- Topic and lookup-service names -----------------------------------
    //
    // The reference: `bsv-blockchain/ts-stack`
    // `57c5f13055b555b83c1ef368fff3650abd21b6ef`,
    // `packages/overlays/overlay-discovery-services/src/utils/`.

    /// `__tests/isValidTopicOrServiceName.test.ts` at that commit, every
    /// expectation in its order: `(name, expected)`.
    fn reference_vectors() -> Vec<(String, bool)> {
        let asset_id = format!("{}_0", "0123456789abcdef".repeat(4));
        let v: Vec<(String, bool)> = vec![
            // valid BRC-22 Topic Manager names
            ("tm_uhrp_files".into(), true),
            ("tm_tempo_songs".into(), true),
            ("tm_a".into(), true),
            ("tm_a_b_c".into(), true),
            // valid BRC-24 Lookup Service names
            ("ls_uhrp_files".into(), true),
            ("ls_tempo_songs_search".into(), true),
            ("ls_a".into(), true),
            ("ls_a_b".into(), true),
            // incorrect prefixes
            ("tp_uhrp_files".into(), false),
            ("um_tempo_songs".into(), false),
            // uppercase letters
            ("tm_Tempo_songs".into(), false),
            ("ls_tempo_Songs".into(), false),
            // starting or ending with an underscore
            ("_tm_uhrp_files".into(), false),
            ("tm_uhrp_files_".into(), false),
            // consecutive underscores
            ("tm__uhrp_files".into(), false),
            ("ls_tempo__songs".into(), false),
            // BRC-207 per-asset names
            (format!("tm_color_{asset_id}"), true),
            (format!("ls_color_{asset_id}"), true),
            ("tm_uhrp_files2".into(), true),
            // other characters
            ("ls_tempo_songs-search".into(), false),
            ("tm_uhrp%files".into(), false),
            ("tm_uhrp files".into(), false),
            ("tm_\u{00e9}".into(), false),
            // a bare prefix and names without a prefix
            ("tm_".into(), false),
            ("ls_".into(), false),
            ("tm__x".into(), false),
            ("tm_x_".into(), false),
            ("color_abc".into(), false),
            // exceeding 256 bytes (257, then 300)
            (format!("tm_{}", "a".repeat(254)), false),
            (format!("tm_{}", "a".repeat(297)), false),
            // exactly 256 bytes
            (format!("ls_{}", "a".repeat(253)), true),
            // the empty string
            (String::new(), false),
        ];
        assert_eq!(v[28].0.len(), 257, "the reference's toHaveLength(257)");
        assert_eq!(v[30].0.len(), 256, "the reference's toHaveLength(256)");
        v
    }

    #[test]
    fn names_the_references_own_vectors() {
        let vectors = reference_vectors();
        assert_eq!(vectors.len(), 32);
        for (name, expected) in vectors {
            assert_eq!(
                is_valid_topic_or_service_name(&name),
                expected,
                "{name:?} ({} bytes)",
                name.len()
            );
        }
    }

    /// A second spelling of
    /// `/^(?=.{1,256}$)(?:tm_|ls_)[a-z0-9]+(?:_[a-z0-9]+)*$/`, for the tests
    /// only: the lookahead as the reference counts it (UTF-16 code units, `.`
    /// matching no line terminator), then the prefix, then every
    /// underscore-separated group non-empty and of `[a-z0-9]` alone.
    fn name_rule_by_groups(name: &str) -> bool {
        let units = name.encode_utf16().count();
        let lookahead = (1..=256).contains(&units)
            && !name
                .chars()
                .any(|c| matches!(c, '\n' | '\r' | '\u{2028}' | '\u{2029}'));
        let rest = match (name.get(..3), name.get(3..)) {
            (Some("tm_"), Some(rest)) | (Some("ls_"), Some(rest)) => rest,
            _ => return false,
        };
        lookahead
            && rest.split('_').all(|group| {
                !group.is_empty() && group.chars().all(|c| matches!(c, 'a'..='z' | '0'..='9'))
            })
    }

    #[test]
    fn names_the_second_spelling_reads_the_references_vectors_too() {
        for (name, expected) in reference_vectors() {
            assert_eq!(name_rule_by_groups(&name), expected, "{name:?}");
        }
    }

    /// The differential: every length 0 to 300 in several shapes, each shape
    /// then mutated at the alphabet's edges, and generated strings over an
    /// alphabet of admitted and refused characters; the rule and its second
    /// spelling agree on every one.
    #[test]
    fn names_differential_against_the_second_spelling() {
        let mut names: Vec<String> = Vec::new();
        // One name of exactly `len` bytes whose body repeats `pattern`.
        let shaped = |prefix: &str, pattern: &str, len: usize| -> String {
            let mut s: String = prefix.chars().take(len).collect();
            let mut body = pattern.chars().cycle();
            while s.len() < len {
                s.extend(body.next());
            }
            s
        };
        for len in 0..=300usize {
            for prefix in ["tm_", "ls_", "xx_", "tm", "TM_", "_tm_"] {
                for pattern in [
                    "a", "z", "0", "9", "a1", "7b", "abc_", "0_1_", "a_", "_", "ab__", "A",
                    "\u{e9}",
                ] {
                    let base = shaped(prefix, pattern, len);
                    // The edges, as mutations of each shape.
                    names.push(format!("{base}_"));
                    names.push(format!("{base}a"));
                    names.push(format!("{base}0"));
                    names.push(format!("{base}\n"));
                    names.push(format!(" {base}"));
                    names.push(base.to_uppercase());
                    if base.is_char_boundary(base.len() / 2) {
                        let (head, tail) = base.split_at(base.len() / 2);
                        for mid in ["_", "__", "5", "Q", "\u{e9}", "-", "\u{2028}", "\u{1d7d8}"] {
                            names.push(format!("{head}{mid}{tail}"));
                        }
                    }
                    names.push(base);
                }
            }
        }
        // Named edges.
        for name in [
            "tm_0",
            "ls_9",
            "tm_0a",
            "tm_a0",
            "tm_123",
            "tm_1_2_3",
            "tm_a_",
            "tm_a__b",
            "tm_A",
            "tm_\u{e9}",
            "tm_a\u{e9}",
            "tm_\u{0660}",
            "tm_\u{ff41}",
            "tm_a\n",
            "tm_a\r",
            "tm-a",
            "tm_a b",
            "ls",
            "tm",
            "t",
            "",
        ] {
            names.push(name.to_string());
        }
        names.push(format!("tm_{}", "a".repeat(253)));
        names.push(format!("tm_{}", "a".repeat(254)));
        names.push(format!("tm_{}", "0".repeat(253)));
        names.push(format!("tm_{}", "0".repeat(254)));
        names.push(format!("tm_{}9", "a_".repeat(126)));
        names.push(format!("tm_{}9", "a_".repeat(127)));
        // Generated strings (xorshift, fixed seed), biased to the admitted alphabet.
        let alphabet: Vec<char> = "abmstlz0189___AZ -.\u{e9}\n".chars().collect();
        let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..20_000 {
            let len = (next() % 301) as usize;
            let mut s = String::from(match next() % 4 {
                0 => "tm_",
                1 => "ls_",
                2 => "",
                _ => "tm",
            });
            // Mostly letters and digits, so that long names are often valid.
            let noisy = next() % 3 == 0;
            while s.len() < len {
                let c = if noisy {
                    alphabet[(next() % alphabet.len() as u64) as usize]
                } else {
                    match next() % 12 {
                        0 => '_',
                        1..=4 => (b'0' + (next() % 10) as u8) as char,
                        _ => (b'a' + (next() % 26) as u8) as char,
                    }
                };
                s.push(c);
            }
            names.push(s);
        }

        let (mut admitted, mut refused) = (0usize, 0usize);
        let (mut longest_admitted, mut admitted_with_digit) = (0usize, 0usize);
        for name in &names {
            let got = is_valid_topic_or_service_name(name);
            assert_eq!(
                got,
                name_rule_by_groups(name),
                "{name:?} ({} bytes)",
                name.len()
            );
            if got {
                admitted += 1;
                longest_admitted = longest_admitted.max(name.len());
                admitted_with_digit += usize::from(name.bytes().any(|b| b.is_ascii_digit()));
                assert!(name.is_ascii() && name.len() <= MAX_TOPIC_OR_SERVICE_NAME_BYTES);
            } else {
                refused += 1;
            }
        }
        // The differential is not vacuous on either side.
        assert!(admitted > 2_000, "admitted {admitted}");
        assert!(refused > 2_000, "refused {refused}");
        assert!(admitted_with_digit > 1_000, "{admitted_with_digit}");
        assert_eq!(longest_admitted, 256);
    }

    #[test]
    fn names_every_length_to_the_bound_and_none_past_it() {
        for len in 0..=300usize {
            for fill in ["a", "7"] {
                let name = format!("tm_{}", fill.repeat(len));
                assert_eq!(
                    is_valid_topic_or_service_name(&name),
                    (1..=253).contains(&len),
                    "tm_ and {len} of {fill:?}"
                );
            }
        }
    }

    #[test]
    fn names_digits_are_admitted_in_any_group() {
        for name in [
            "tm_test123",
            "tm_1test",
            "tm_0",
            "ls_42",
            "tm_a_1_b_2",
            "tm_color_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef_0",
        ] {
            assert!(is_valid_topic_or_service_name(name), "{name}");
        }
    }

    #[test]
    fn names_refused_shapes() {
        for name in [
            "",
            "ship",
            "hello",
            "TM_ship",
            "LS_slap",
            "xx_test",
            "tm_",
            "ls_",
            "tm_SHIP",
            "tm_Ship",
            "tm_test_",
            "tm_test__name",
            "tm__test",
            "tm_1_",
            "tm_1__2",
            "tm_\u{661}",
            "tm_a\n",
            " tm_a",
            "tm_a ",
        ] {
            assert!(!is_valid_topic_or_service_name(name), "{name:?}");
        }
    }

    // -- URI validation -------------------------------------------------

    #[test]
    fn valid_https_uris() {
        assert!(is_advertisable_uri("https://example.com"));
        assert!(is_advertisable_uri("https://overlay.bsvb.tech"));
        assert!(is_advertisable_uri("https://8.8.8.8"));
    }

    #[test]
    fn valid_custom_https_uris() {
        assert!(is_advertisable_uri("https+bsvauth://example.com"));
        assert!(is_advertisable_uri("https+bsvauth+smf://example.com"));
        assert!(is_advertisable_uri(
            "https+bsvauth+scrypt-offchain://example.com"
        ));
        assert!(is_advertisable_uri("https+rtt://example.com"));
    }

    #[test]
    fn valid_wss_uri() {
        assert!(is_advertisable_uri("wss://example.com"));
        assert!(is_advertisable_uri("wss://overlay.example.org"));
    }

    #[test]
    fn invalid_localhost_uris() {
        assert!(!is_advertisable_uri("https://localhost"));
        assert!(!is_advertisable_uri("https://localhost:3000"));
        assert!(!is_advertisable_uri("wss://localhost"));
        assert!(!is_advertisable_uri("https+bsvauth://localhost"));
    }

    #[test]
    fn invalid_http_uri() {
        // http:// is not in the allowed schemes
        assert!(!is_advertisable_uri("http://example.com"));
    }

    #[test]
    fn invalid_path_uris() {
        assert!(!is_advertisable_uri("https://example.com/path"));
        assert!(!is_advertisable_uri("https://example.com/api/v1"));
    }

    #[test]
    fn invalid_empty_uris() {
        assert!(!is_advertisable_uri(""));
        assert!(!is_advertisable_uri("   "));
    }

    #[test]
    fn invalid_unknown_scheme() {
        assert!(!is_advertisable_uri("ftp://example.com"));
        assert!(!is_advertisable_uri("ssh://example.com"));
    }

    #[test]
    fn valid_js8c_uri() {
        assert!(is_advertisable_uri(
            "js8c+bsvauth+smf:?lat=40.7128&long=-74.0060&freq=7.078MHz&radius=500km"
        ));
    }

    #[test]
    fn invalid_js8c_missing_params() {
        assert!(!is_advertisable_uri("js8c+bsvauth+smf:?lat=40&long=-74"));
        assert!(!is_advertisable_uri("js8c+bsvauth+smf:")); // no query
    }

    #[test]
    fn invalid_js8c_out_of_range() {
        assert!(!is_advertisable_uri(
            "js8c+bsvauth+smf:?lat=100&long=-74&freq=7MHz&radius=500km"
        )); // lat > 90
        assert!(!is_advertisable_uri(
            "js8c+bsvauth+smf:?lat=40&long=200&freq=7MHz&radius=500km"
        )); // long > 180
    }

    // -- extract_positive_number ----------------------------------------

    #[test]
    fn test_extract_positive_number() {
        assert_eq!(extract_positive_number("7.078MHz"), Some(7.078));
        assert_eq!(extract_positive_number("500km"), Some(500.0));
        assert_eq!(extract_positive_number("42"), Some(42.0));
        assert_eq!(extract_positive_number("0"), None); // not positive
        assert_eq!(extract_positive_number("abc"), None);
        assert_eq!(extract_positive_number(""), None);
    }

    // -- Token signature linkage verification ---------------------------

    mod signature_linkage {
        use crate::validation::is_token_signature_correctly_linked;
        use bsv_rs::primitives::ec::{PrivateKey, PublicKey};
        use bsv_rs::wallet::{
            Counterparty, CreateSignatureArgs, GetPublicKeyArgs, ProtoWallet, Protocol,
            SecurityLevel,
        };

        /// Helper: create a properly signed set of fields + the correct locking key.
        fn make_signed_ship_token(signer_key: &PrivateKey) -> (PublicKey, Vec<Vec<u8>>) {
            let signer_wallet = ProtoWallet::new(Some(signer_key.clone()));
            let identity_hex = signer_wallet.identity_key_hex();
            let identity_bytes = hex::decode(&identity_hex).unwrap();

            let data_fields = vec![
                b"SHIP".to_vec(),
                identity_bytes,
                b"https://example.com".to_vec(),
                b"tm_meter".to_vec(),
            ];
            let data: Vec<u8> = data_fields.iter().flat_map(|f| f.iter().copied()).collect();

            let protocol_id =
                Protocol::new(SecurityLevel::Counterparty, "service host interconnect");

            let sig = signer_wallet
                .create_signature(CreateSignatureArgs {
                    data: Some(data),
                    hash_to_directly_sign: None,
                    protocol_id: protocol_id.clone(),
                    key_id: "1".to_string(),
                    counterparty: Some(Counterparty::Anyone),
                })
                .unwrap();

            let locking_hex = signer_wallet
                .get_public_key(GetPublicKeyArgs {
                    identity_key: false,
                    protocol_id: Some(protocol_id),
                    key_id: Some("1".to_string()),
                    counterparty: Some(Counterparty::Anyone),
                    for_self: Some(true),
                })
                .unwrap()
                .public_key;
            let locking_key = PublicKey::from_hex(&locking_hex).unwrap();

            let mut fields = data_fields;
            fields.push(sig.signature);

            (locking_key, fields)
        }

        #[test]
        fn valid_ship_token_passes() {
            let signer = PrivateKey::random();
            let (locking_key, fields) = make_signed_ship_token(&signer);

            let result =
                is_token_signature_correctly_linked(&locking_key, &fields[1], &fields, "SHIP");
            assert!(result.unwrap());
        }

        #[test]
        fn valid_slap_token_passes() {
            let signer = PrivateKey::random();
            let signer_wallet = ProtoWallet::new(Some(signer.clone()));
            let identity_hex = signer_wallet.identity_key_hex();
            let identity_bytes = hex::decode(&identity_hex).unwrap();

            let data_fields = vec![
                b"SLAP".to_vec(),
                identity_bytes.clone(),
                b"https://example.com".to_vec(),
                b"ls_lookup".to_vec(),
            ];
            let data: Vec<u8> = data_fields.iter().flat_map(|f| f.iter().copied()).collect();

            let protocol_id =
                Protocol::new(SecurityLevel::Counterparty, "service lookup availability");

            let sig = signer_wallet
                .create_signature(CreateSignatureArgs {
                    data: Some(data),
                    hash_to_directly_sign: None,
                    protocol_id: protocol_id.clone(),
                    key_id: "1".to_string(),
                    counterparty: Some(Counterparty::Anyone),
                })
                .unwrap();

            let locking_hex = signer_wallet
                .get_public_key(GetPublicKeyArgs {
                    identity_key: false,
                    protocol_id: Some(protocol_id),
                    key_id: Some("1".to_string()),
                    counterparty: Some(Counterparty::Anyone),
                    for_self: Some(true),
                })
                .unwrap()
                .public_key;
            let locking_key = PublicKey::from_hex(&locking_hex).unwrap();

            let mut fields = data_fields;
            fields.push(sig.signature);

            let result =
                is_token_signature_correctly_linked(&locking_key, &identity_bytes, &fields, "SLAP");
            assert!(result.unwrap());
        }

        #[test]
        fn tampered_data_fails() {
            let signer = PrivateKey::random();
            let (locking_key, mut fields) = make_signed_ship_token(&signer);

            // Tamper with the protocol field (like the TS test)
            let identity_bytes = fields[1].clone();
            fields[0] = b"SLAP".to_vec();

            let result =
                is_token_signature_correctly_linked(&locking_key, &identity_bytes, &fields, "SHIP");
            assert!(!result.unwrap());
        }

        #[test]
        fn wrong_identity_key_fails() {
            // Signer signs but claims to be someone else in field[1]
            let signer = PrivateKey::random();
            let signer_wallet = ProtoWallet::new(Some(signer.clone()));
            let impostor = PrivateKey::random();
            let impostor_id = PublicKey::from_private_key(&impostor)
                .to_compressed()
                .to_vec();

            let protocol_id =
                Protocol::new(SecurityLevel::Counterparty, "service host interconnect");

            let data_fields = vec![
                b"SHIP".to_vec(),
                impostor_id.clone(),
                b"https://example.com".to_vec(),
                b"tm_meter".to_vec(),
            ];
            let data: Vec<u8> = data_fields.iter().flat_map(|f| f.iter().copied()).collect();

            let sig = signer_wallet
                .create_signature(CreateSignatureArgs {
                    data: Some(data),
                    hash_to_directly_sign: None,
                    protocol_id: protocol_id.clone(),
                    key_id: "1".to_string(),
                    counterparty: Some(Counterparty::Anyone),
                })
                .unwrap();

            let locking_hex = signer_wallet
                .get_public_key(GetPublicKeyArgs {
                    identity_key: false,
                    protocol_id: Some(protocol_id),
                    key_id: Some("1".to_string()),
                    counterparty: Some(Counterparty::Anyone),
                    for_self: Some(true),
                })
                .unwrap()
                .public_key;
            let locking_key = PublicKey::from_hex(&locking_hex).unwrap();

            let mut fields = data_fields;
            fields.push(sig.signature);

            let result =
                is_token_signature_correctly_linked(&locking_key, &impostor_id, &fields, "SHIP");
            // Should fail: signature was made by signer, not impostor
            assert!(!result.unwrap());
        }

        #[test]
        fn wrong_locking_key_fails() {
            let signer = PrivateKey::random();
            let (_correct_locking_key, fields) = make_signed_ship_token(&signer);

            let wrong_locking_key = PublicKey::from_private_key(&PrivateKey::random());

            let result = is_token_signature_correctly_linked(
                &wrong_locking_key,
                &fields[1],
                &fields,
                "SHIP",
            );
            // Signature verification passes but locking key mismatch
            assert!(!result.unwrap());
        }

        #[test]
        fn unknown_protocol_returns_error() {
            let signer = PrivateKey::random();
            let (locking_key, fields) = make_signed_ship_token(&signer);

            let result =
                is_token_signature_correctly_linked(&locking_key, &fields[1], &fields, "UNKNOWN");
            assert!(result.is_err());
        }

        #[test]
        fn too_few_fields_returns_error() {
            let locking_key = PublicKey::from_private_key(&PrivateKey::random());
            let fields = vec![b"SHIP".to_vec()];

            let result =
                is_token_signature_correctly_linked(&locking_key, &[0x02; 33], &fields, "SHIP");
            assert!(result.is_err());
        }

        #[test]
        fn deterministic_with_known_key() {
            // Use PrivateKey from scalar 42 (matches the TS test)
            let mut key_bytes = [0u8; 32];
            key_bytes[31] = 42;
            let signer = PrivateKey::from_bytes(&key_bytes).unwrap();
            let (locking_key, fields) = make_signed_ship_token(&signer);

            let result =
                is_token_signature_correctly_linked(&locking_key, &fields[1], &fields, "SHIP");
            assert!(result.unwrap());
        }
    }

    /// Full end-to-end verification with exact bytes from a live rejected registration.
    #[test]
    fn debug_dolphin_milk_full_verification() {
        use bsv_rs::primitives::ec::PublicKey;
        use bsv_rs::primitives::hash::sha256;
        use bsv_rs::wallet::{
            Counterparty, GetPublicKeyArgs, ProtoWallet, Protocol, SecurityLevel,
            VerifySignatureArgs,
        };

        // Exact fields from the worm's registration attempt
        let field0 = hex::decode("4147454e54").unwrap(); // "AGENT"
        let field1 =
            hex::decode("0350cf02ff54ad9a255eacdf78d7266a36070b1b516e893b5d064859d0ad0dc618")
                .unwrap();
        let field2 =
            hex::decode("0350cf02ff54ad9a255eacdf78d7266a36070b1b516e893b5d064859d0ad0dc618")
                .unwrap();
        let field3 = hex::decode("687474703a2f2f6c6f63616c686f73743a39393939").unwrap(); // "http://localhost:9999"
        let field4 = hex::decode("6c6c6d2c746f6f6c732c77616c6c65742c6d656d6f72792c6d6573736167696e672c783430322c7363686564756c652c6f726368657374726174696f6e").unwrap();
        let signature = hex::decode("304402200978db62697a3995e0d90f400d479d14be583a14b473e22b7738042ac39e1d74022074b6e9ce103089ef5f9305e5d0c089b4a112f2652cb79065a7e0e102422da9fb").unwrap();
        let locking_key_hex = "02547369322f5bb5a854f53b2897f908f36135e20ca7fbebf9cd3c768a7b46622e";

        // Build the sign_data exactly as the overlay does: concat fields[0..5]
        let data: Vec<u8> = [&field0, &field1, &field2, &field3, &field4]
            .iter()
            .flat_map(|f| f.iter().copied())
            .collect();

        let hash = sha256(&data);
        let worm_hash = "a05a749cbd03574c866e0ee566f45130eb997d3099f51fda417c88ccee982eb0";

        println!("=== DATA COMPARISON ===");
        println!("Data length: {} bytes", data.len());
        println!("Overlay SHA-256: {}", hex::encode(hash));
        println!("Worm SHA-256:    {}", worm_hash);
        println!("Hashes match:    {}", hex::encode(hash) == worm_hash);

        // Now try the actual signature verification
        let identity_pubkey = PublicKey::from_bytes(&field1).unwrap();
        let protocol_id = Protocol::new(SecurityLevel::Counterparty, "agent registry");
        let counterparty = Counterparty::Other(identity_pubkey);
        let anyone_wallet = ProtoWallet::anyone();

        println!("\n=== SIGNATURE VERIFICATION ===");
        let result = anyone_wallet.verify_signature(VerifySignatureArgs {
            data: Some(data.clone()),
            hash_to_directly_verify: None,
            signature: signature.clone(),
            protocol_id: protocol_id.clone(),
            key_id: "1".to_string(),
            counterparty: Some(counterparty.clone()),
            for_self: None,
        });
        println!("verify_signature result: {:?}", result);

        // Also try with for_self=true
        let result_for_self = anyone_wallet.verify_signature(VerifySignatureArgs {
            data: Some(data.clone()),
            hash_to_directly_verify: None,
            signature: signature.clone(),
            protocol_id: protocol_id.clone(),
            key_id: "1".to_string(),
            counterparty: Some(counterparty.clone()),
            for_self: Some(true),
        });
        println!("verify_signature (for_self=true): {:?}", result_for_self);

        // Try with hash directly
        let result_direct = anyone_wallet.verify_signature(VerifySignatureArgs {
            data: None,
            hash_to_directly_verify: Some(hash),
            signature: signature.clone(),
            protocol_id: protocol_id.clone(),
            key_id: "1".to_string(),
            counterparty: Some(counterparty.clone()),
            for_self: None,
        });
        println!("verify_signature (direct hash): {:?}", result_direct);

        // Locking key check
        let expected = anyone_wallet
            .get_public_key(GetPublicKeyArgs {
                identity_key: false,
                protocol_id: Some(protocol_id.clone()),
                key_id: Some("1".to_string()),
                counterparty: Some(counterparty.clone()),
                for_self: None,
            })
            .unwrap();
        println!("\n=== LOCKING KEY ===");
        println!("Expected: {}", expected.public_key);
        println!("Actual:   {}", locking_key_hex);
        println!("Match:    {}", expected.public_key == locking_key_hex);

        // Also run the full is_token_signature_correctly_linked
        let fields = vec![field0, field1.clone(), field2, field3, field4, signature];
        let locking_key = PublicKey::from_hex(locking_key_hex).unwrap();
        let result = is_token_signature_correctly_linked(&locking_key, &field1, &fields, "AGENT");
        println!("\n=== FULL RESULT ===");
        println!("is_token_signature_correctly_linked: {:?}", result);
    }

    /// Diagnostic test: trace through verification with exact values from
    /// a live dolphin-milk agent registration that was rejected.
    #[test]
    fn debug_dolphin_milk_agent_key_derivation() {
        use bsv_rs::primitives::ec::PublicKey;
        use bsv_rs::wallet::{
            Counterparty, GetPublicKeyArgs, ProtoWallet, Protocol, SecurityLevel,
        };

        let identity_key_hex = "0350cf02ff54ad9a255eacdf78d7266a36070b1b516e893b5d064859d0ad0dc618";
        let locking_key_hex = "02547369322f5bb5a854f53b2897f908f36135e20ca7fbebf9cd3c768a7b46622e";
        let signature_hex =
            "304402200978db62697a3995e0d90f400d479d14be583a14b473e22b7738042ac39e1d74\
             022074b6e9ce103089ef5f9305e5d0c089b4a112f2652cb79065a7e0e102422da9fb";

        let identity_bytes = hex::decode(identity_key_hex).unwrap();
        let _locking_key = PublicKey::from_hex(locking_key_hex).unwrap();
        let signature_bytes = hex::decode(signature_hex).unwrap();

        // ── Step 1: Derive expected locking key via ProtoWallet::anyone() ──
        let anyone_wallet = ProtoWallet::anyone();
        let identity_pubkey = PublicKey::from_bytes(&identity_bytes).unwrap();
        let protocol_id = Protocol::new(SecurityLevel::Counterparty, "agent registry");
        let counterparty = Counterparty::Other(identity_pubkey);

        let expected = anyone_wallet
            .get_public_key(GetPublicKeyArgs {
                identity_key: false,
                protocol_id: Some(protocol_id.clone()),
                key_id: Some("1".to_string()),
                counterparty: Some(counterparty.clone()),
                for_self: None,
            })
            .unwrap();

        println!("=== AGENT KEY DERIVATION DEBUG ===");
        println!("Identity key:       {}", identity_key_hex);
        println!("Actual locking key: {}", locking_key_hex);
        println!("Expected locking key (anyone derives counterparty, for_self=None):");
        println!("  {}", expected.public_key);
        println!(
            "Locking key match: {}",
            expected.public_key == locking_key_hex
        );

        // Also try for_self=true (in case the overlay should derive differently)
        let expected_for_self = anyone_wallet
            .get_public_key(GetPublicKeyArgs {
                identity_key: false,
                protocol_id: Some(protocol_id.clone()),
                key_id: Some("1".to_string()),
                counterparty: Some(counterparty.clone()),
                for_self: Some(true),
            })
            .unwrap();
        println!("Expected locking key (anyone derives self, for_self=true):");
        println!("  {}", expected_for_self.public_key);
        println!(
            "Locking key match (for_self=true): {}",
            expected_for_self.public_key == locking_key_hex
        );

        // Also try for_self=false explicitly
        let expected_for_self_false = anyone_wallet
            .get_public_key(GetPublicKeyArgs {
                identity_key: false,
                protocol_id: Some(protocol_id.clone()),
                key_id: Some("1".to_string()),
                counterparty: Some(counterparty.clone()),
                for_self: Some(false),
            })
            .unwrap();
        println!("Expected locking key (anyone derives counterparty, for_self=false):");
        println!("  {}", expected_for_self_false.public_key);

        // ── Step 2: Check what the SIGNER would derive ──
        // The signer uses: signer_wallet.get_public_key(counterparty=Anyone, for_self=true)
        // We can't reproduce this without the signer's private key, but we can check
        // the anyone wallet's derivation from the other side should match.

        // ── Step 3: Try signature verification ──
        // Build dummy data fields to test signature (we don't have the full fields,
        // but we can at least test key derivation)
        println!("\n=== SIGNATURE VERIFICATION ===");
        println!(
            "Signature ({} bytes): {}...",
            signature_bytes.len(),
            &signature_hex[..40]
        );

        // The anyone wallet's identity key (should be PrivateKey(1)'s pubkey)
        println!(
            "Anyone wallet identity: {}",
            anyone_wallet.identity_key_hex()
        );
    }
}
