//! `tm_sonicstar` topic manager — admits SonicStar Song Source Protocol
//! (`sssp`) `OP_RETURN` outputs.
//!
//! See [`super`] for the design rationale, on-wire format, and the
//! permissive 3-path candidate decoder. This is the only plugin in the
//! `overlay-discovery` crate that uses bare `OP_RETURN` rather than
//! `PushDrop`, so the parsing helpers live alongside the topic manager
//! rather than in `validation.rs`.

use async_trait::async_trait;
use bsv_rs::script::op;
use bsv_rs::script::{LockingScript, Script, ScriptChunk};
use bsv_rs::transaction::Transaction;
use overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use overlay_engine::types::{AdmittanceInstructions, ServiceMetadata, SubmitMode};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

/// Protocol identifier in the JSON envelope.
pub const SSSP_PROTOCOL_ID: &str = "sssp";

/// Default `pricePerPlay` value applied when the JSON envelope omits it,
/// or sets it to `0`. Mirrors Ruth's TS `metadata.pricePerPlay || 1000`.
pub const DEFAULT_PRICE_PER_PLAY: u64 = 1000;

/// Default `royaltyRate` value applied when the JSON envelope omits it,
/// or sets it to `0`. Mirrors Ruth's TS `metadata.royaltyRate || 75`.
pub const DEFAULT_ROYALTY_RATE: u8 = 75;

/// Decoded `sssp` envelope.
///
/// Field shape mirrors Ruth's TS `SonicStarSongMetadata` (sonicstarProtocol
/// .ts:10-24). Optional fields drop out of the serialized form via
/// `skip_serializing_if` so JSON round-trips match Mongo's "undefined keys
/// are absent" semantics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SonicstarMetadata {
    #[serde(rename = "songTitle")]
    pub song_title: String,
    #[serde(rename = "artistName")]
    pub artist_name: String,
    /// Always the empty string today. Ruth's TS hard codes this with a TODO
    /// (sonicstarProtocol.ts:132); when she wires it up from transaction
    /// context we will mirror the lift here.
    #[serde(rename = "artistIdentityKey")]
    pub artist_identity_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub duration: u64,
    #[serde(rename = "songFileURL")]
    pub song_file_url: String,
    #[serde(rename = "artFileURL", skip_serializing_if = "Option::is_none")]
    pub art_file_url: Option<String>,
    #[serde(rename = "previewURL", skip_serializing_if = "Option::is_none")]
    pub preview_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genre: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub album: Option<String>,
    #[serde(rename = "releaseDate", skip_serializing_if = "Option::is_none")]
    pub release_date: Option<String>,
    #[serde(rename = "pricePerPlay")]
    pub price_per_play: u64,
    #[serde(rename = "royaltyRate")]
    pub royalty_rate: u8,
}

/// Topic manager for SonicStar Song Source Protocol outputs.
pub struct SonicstarTopicManager;

impl SonicstarTopicManager {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SonicstarTopicManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait(?Send)]
impl TopicManager for SonicstarTopicManager {
    async fn identify_admissible_outputs(
        &self,
        beef: &[u8],
        _previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        _mode: SubmitMode,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        let tx = Transaction::from_beef(beef, None)
            .map_err(|e| TopicManagerError::InvalidBeef(e.to_string()))?;

        let mut outputs_to_admit = Vec::new();
        for (i, output) in tx.outputs.iter().enumerate() {
            if Self::decode_song_metadata(&output.locking_script).is_some() {
                outputs_to_admit.push(i as u32);
                debug!("SONICSTAR: admitted output {i}");
            }
        }

        if outputs_to_admit.is_empty() {
            warn!("SONICSTAR: no outputs admitted");
        } else {
            debug!(
                "SONICSTAR: {} outputs in tx, {} admitted",
                tx.outputs.len(),
                outputs_to_admit.len()
            );
        }

        Ok(AdmittanceInstructions {
            outputs_to_admit,
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }

    async fn get_documentation(&self) -> String {
        include_str!("../../docs/sonicstar_topic.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "SonicStar Topic Manager".to_string(),
            description: Some(
                "Admits SonicStar song source ordinals (sssp protocol).".to_string(),
            ),
            ..Default::default()
        }
    }
}

impl SonicstarTopicManager {
    /// Build the candidate-buffer list in the same order as Ruth's TS
    /// reference (`sonicstarProtocol.ts:80-100`):
    ///
    /// 1. `chunks[i].data` for `i >= 1`, where the data is non-empty
    ///    (separate-push form, defensive — bsv-rs's parser collapses
    ///    everything after `OP_RETURN` into `chunks[0].data`, so this is
    ///    almost always empty in practice).
    /// 2. `chunks[0].data` raw (the collapsed buffer including push prefix
    ///    bytes; the JSON extractor's `find('{')..rfind('}')` skips past
    ///    the prefix).
    /// 3. `chunks[0].data` re-parsed as an inner `Script`, each non-empty
    ///    push payload.
    ///
    /// Caller already verified `chunks[0].op == OP_RETURN`.
    fn candidate_pushes(chunks: &[ScriptChunk]) -> Vec<Vec<u8>> {
        let mut candidates = Vec::new();

        // Path 1: separate-push form.
        for chunk in chunks.iter().skip(1) {
            if let Some(data) = chunk.data.as_ref() {
                if !data.is_empty() {
                    candidates.push(data.clone());
                }
            }
        }

        // Paths 2 + 3: chunks[0].data raw, then re-parsed.
        let tail = chunks.first().and_then(|c| c.data.as_ref());
        if let Some(tail_bytes) = tail {
            if !tail_bytes.is_empty() {
                candidates.push(tail_bytes.clone());
                if let Ok(inner) = Script::from_binary(tail_bytes) {
                    for chunk in inner.chunks() {
                        if let Some(data) = chunk.data {
                            if !data.is_empty() {
                                candidates.push(data);
                            }
                        }
                    }
                }
            }
        }

        candidates
    }

    /// Decode and validate an `sssp` envelope. Returns `Some` only if the
    /// output:
    ///
    /// - Starts with `OP_RETURN` (chunks[0]; `OP_FALSE OP_RETURN` is
    ///   rejected to match Ruth's reference at sonicstarProtocol.ts:75-77).
    /// - Contains a candidate buffer that round-trips as a JSON object
    ///   whose `protocol` field is `"sssp"`.
    /// - Has non-empty `songTitle`, `artistName`, and `songFileURL` after
    ///   the falsy-`||` defaulting rule.
    pub fn decode_song_metadata(locking_script: &LockingScript) -> Option<SonicstarMetadata> {
        let chunks = locking_script.chunks();
        let first = chunks.first()?;
        if first.op != op::OP_RETURN {
            return None;
        }

        for candidate in Self::candidate_pushes(&chunks) {
            if let Some(meta) = decode_metadata_from_bytes(&candidate) {
                return Some(meta);
            }
        }
        None
    }
}

/// Try to decode a candidate buffer as an `sssp` JSON envelope.
///
/// Implements the TS `text.indexOf('{')` / `text.lastIndexOf('}')` slice
/// trick so push-prefix bytes (e.g. `0x4c 0x9e` for PUSHDATA1) and any
/// trailing junk are tolerated. Lossy UTF-8 decode matches `Utils.toUTF8`
/// in the TS reference (which behaves like a permissive UTF-8 decoder).
fn decode_metadata_from_bytes(bytes: &[u8]) -> Option<SonicstarMetadata> {
    let text = String::from_utf8_lossy(bytes);
    let start = text.find('{')?;
    let end = text.rfind('}')?;
    if end <= start {
        return None;
    }
    let slice = text.get(start..=end)?;
    let value: serde_json::Value = serde_json::from_str(slice).ok()?;
    decode_metadata_from_value(&value)
}

/// Apply the falsy-`||` default rules from Ruth's `decodeSongMetadata`
/// (sonicstarProtocol.ts:129-143). Required string fields end up as the
/// empty string when the JSON value is missing/null/non-string; the topic
/// manager then rejects the empty case so admission is gated correctly.
fn decode_metadata_from_value(value: &serde_json::Value) -> Option<SonicstarMetadata> {
    let obj = value.as_object()?;
    if obj.get("protocol").and_then(|v| v.as_str()) != Some(SSSP_PROTOCOL_ID) {
        return None;
    }

    let song_title = string_or_empty(obj.get("songTitle"));
    let artist_name = string_or_empty(obj.get("artistName"));
    let song_file_url = string_or_empty(obj.get("songFileURL"));

    // Required-field admission gate. JS `!''` is true, so a missing /
    // empty / non-string field rejects.
    if song_title.is_empty() || artist_name.is_empty() || song_file_url.is_empty() {
        return None;
    }

    let duration = u64_or_zero_default(obj.get("duration"), 0);
    let price_per_play = u64_or_zero_default(obj.get("pricePerPlay"), DEFAULT_PRICE_PER_PLAY);
    let royalty_rate = u8_or_zero_default(obj.get("royaltyRate"), DEFAULT_ROYALTY_RATE);

    Some(SonicstarMetadata {
        song_title,
        artist_name,
        artist_identity_key: String::new(),
        description: optional_string(obj.get("description")),
        duration,
        song_file_url,
        art_file_url: optional_string(obj.get("artFileURL")),
        preview_url: optional_string(obj.get("previewURL")),
        genre: optional_string(obj.get("genre")),
        album: optional_string(obj.get("album")),
        release_date: optional_string(obj.get("releaseDate")),
        price_per_play,
        royalty_rate,
    })
}

/// JS `value || ''` for string fields. Non-string values (numbers, bools,
/// arrays, objects, null, missing) all coerce to empty.
fn string_or_empty(value: Option<&serde_json::Value>) -> String {
    value
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .unwrap_or_default()
}

/// JS `value || default` for numeric fields. The `0` case takes the
/// default to mirror the falsy-`||` quirk faithfully.
fn u64_or_zero_default(value: Option<&serde_json::Value>, default: u64) -> u64 {
    let raw = value.and_then(serde_json::Value::as_u64);
    match raw {
        Some(0) | None => default,
        Some(n) => n,
    }
}

fn u8_or_zero_default(value: Option<&serde_json::Value>, default: u8) -> u8 {
    let raw = value.and_then(serde_json::Value::as_u64);
    match raw {
        Some(0) | None => default,
        Some(n) => u8::try_from(n).unwrap_or(default),
    }
}

/// `Option<String>` for fields whose absence should drop out of the
/// persisted record. Non-string JSON values (numbers, bools, etc.) are
/// treated as `None` to match TS's behavior of leaving them unchanged
/// (which, since they are JS `undefined` in practice, serializes as
/// "missing" through Mongo).
fn optional_string(value: Option<&serde_json::Value>) -> Option<String> {
    value.and_then(|v| v.as_str()).map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bsv_rs::script::Script;
    use serde_json::json;

    fn make_op_return_script(payload: &[u8]) -> LockingScript {
        let mut script = Script::new();
        script.write_opcode(op::OP_RETURN);
        script.write_bin(payload);
        LockingScript::from(script)
    }

    fn make_op_false_op_return_script(payload: &[u8]) -> LockingScript {
        let mut script = Script::new();
        script.write_opcode(op::OP_FALSE);
        script.write_opcode(op::OP_RETURN);
        script.write_bin(payload);
        LockingScript::from(script)
    }

    fn well_formed_envelope() -> serde_json::Value {
        json!({
            "protocol": "sssp",
            "securityLevel": 2,
            "songTitle": "Hello",
            "artistName": "Adele",
            "duration": 240,
            "songFileURL": "uhrp://song",
            "artFileURL": "uhrp://art",
            "previewURL": "https://example.com/preview",
            "genre": "Pop",
            "album": "25",
            "releaseDate": "2025-04-25",
            "pricePerPlay": 1000,
            "royaltyRate": 75,
        })
    }

    fn json_bytes(v: &serde_json::Value) -> Vec<u8> {
        serde_json::to_vec(v).unwrap()
    }

    #[test]
    fn admits_well_formed_sssp_envelope() {
        let ls = make_op_return_script(&json_bytes(&well_formed_envelope()));
        let meta = SonicstarTopicManager::decode_song_metadata(&ls).expect("must admit");
        assert_eq!(meta.song_title, "Hello");
        assert_eq!(meta.artist_name, "Adele");
        assert_eq!(meta.song_file_url, "uhrp://song");
        assert_eq!(meta.duration, 240);
        assert_eq!(meta.price_per_play, 1000);
        assert_eq!(meta.royalty_rate, 75);
        assert_eq!(meta.genre.as_deref(), Some("Pop"));
        assert_eq!(meta.artist_identity_key, "", "always empty per TS TODO");
    }

    #[test]
    fn rejects_missing_song_title() {
        let mut env = well_formed_envelope();
        env.as_object_mut().unwrap().remove("songTitle");
        let ls = make_op_return_script(&json_bytes(&env));
        assert!(SonicstarTopicManager::decode_song_metadata(&ls).is_none());
    }

    #[test]
    fn rejects_empty_song_title() {
        let mut env = well_formed_envelope();
        env["songTitle"] = json!("");
        let ls = make_op_return_script(&json_bytes(&env));
        assert!(SonicstarTopicManager::decode_song_metadata(&ls).is_none());
    }

    #[test]
    fn rejects_missing_artist_name() {
        let mut env = well_formed_envelope();
        env.as_object_mut().unwrap().remove("artistName");
        let ls = make_op_return_script(&json_bytes(&env));
        assert!(SonicstarTopicManager::decode_song_metadata(&ls).is_none());
    }

    #[test]
    fn rejects_missing_song_file_url() {
        let mut env = well_formed_envelope();
        env.as_object_mut().unwrap().remove("songFileURL");
        let ls = make_op_return_script(&json_bytes(&env));
        assert!(SonicstarTopicManager::decode_song_metadata(&ls).is_none());
    }

    #[test]
    fn rejects_wrong_protocol() {
        let mut env = well_formed_envelope();
        env["protocol"] = json!("ttsp");
        let ls = make_op_return_script(&json_bytes(&env));
        assert!(SonicstarTopicManager::decode_song_metadata(&ls).is_none());
    }

    #[test]
    fn rejects_missing_protocol() {
        let mut env = well_formed_envelope();
        env.as_object_mut().unwrap().remove("protocol");
        let ls = make_op_return_script(&json_bytes(&env));
        assert!(SonicstarTopicManager::decode_song_metadata(&ls).is_none());
    }

    #[test]
    fn rejects_non_op_return_output() {
        // P2PKH script.
        let ls = LockingScript::from_hex(
            "76a914000000000000000000000000000000000000000088ac",
        )
        .unwrap();
        assert!(SonicstarTopicManager::decode_song_metadata(&ls).is_none());
    }

    #[test]
    fn rejects_malformed_json() {
        let ls = make_op_return_script(b"this is not json at all");
        assert!(SonicstarTopicManager::decode_song_metadata(&ls).is_none());
    }

    #[test]
    fn rejects_op_false_op_return_per_ts_parity() {
        // Per Ruth's TS reference (sonicstarProtocol.ts:75-77), chunks[0].op
        // must be OP_RETURN. The "safe data carrier" pattern starts with
        // OP_FALSE OP_RETURN, so chunks[0] is OP_FALSE and admission fails.
        // This is option A of the OP_FALSE/OP_RETURN ambiguity raised at
        // plan review time.
        let ls = make_op_false_op_return_script(&json_bytes(&well_formed_envelope()));
        assert!(SonicstarTopicManager::decode_song_metadata(&ls).is_none());
    }

    #[test]
    fn explicit_zero_price_per_play_defaults_to_1000() {
        let mut env = well_formed_envelope();
        env["pricePerPlay"] = json!(0);
        let ls = make_op_return_script(&json_bytes(&env));
        let meta = SonicstarTopicManager::decode_song_metadata(&ls).unwrap();
        assert_eq!(meta.price_per_play, DEFAULT_PRICE_PER_PLAY);
    }

    #[test]
    fn explicit_zero_royalty_rate_defaults_to_75() {
        let mut env = well_formed_envelope();
        env["royaltyRate"] = json!(0);
        let ls = make_op_return_script(&json_bytes(&env));
        let meta = SonicstarTopicManager::decode_song_metadata(&ls).unwrap();
        assert_eq!(meta.royalty_rate, DEFAULT_ROYALTY_RATE);
    }

    #[test]
    fn explicit_zero_duration_stays_zero() {
        // Per the TS `metadata.duration || 0` rule, 0 || 0 == 0 and the
        // observable value remains 0 (no special "default" behavior).
        let mut env = well_formed_envelope();
        env["duration"] = json!(0);
        let ls = make_op_return_script(&json_bytes(&env));
        let meta = SonicstarTopicManager::decode_song_metadata(&ls).unwrap();
        assert_eq!(meta.duration, 0);
    }

    #[test]
    fn missing_optional_fields_become_none() {
        let env = json!({
            "protocol": "sssp",
            "songTitle": "Hello",
            "artistName": "Adele",
            "songFileURL": "uhrp://song",
        });
        let ls = make_op_return_script(&json_bytes(&env));
        let meta = SonicstarTopicManager::decode_song_metadata(&ls).unwrap();
        assert!(meta.description.is_none());
        assert!(meta.art_file_url.is_none());
        assert!(meta.preview_url.is_none());
        assert!(meta.genre.is_none());
        assert!(meta.album.is_none());
        assert!(meta.release_date.is_none());
        // Numeric defaults apply.
        assert_eq!(meta.duration, 0);
        assert_eq!(meta.price_per_play, DEFAULT_PRICE_PER_PLAY);
        assert_eq!(meta.royalty_rate, DEFAULT_ROYALTY_RATE);
    }

    #[test]
    fn unknown_extra_fields_are_silently_dropped() {
        let mut env = well_formed_envelope();
        env["unrelatedField"] = json!("garbage");
        env["nestedExtra"] = json!({"foo": [1, 2, 3]});
        let ls = make_op_return_script(&json_bytes(&env));
        let meta = SonicstarTopicManager::decode_song_metadata(&ls).unwrap();
        let serialized = serde_json::to_string(&meta).unwrap();
        assert!(!serialized.contains("unrelatedField"));
        assert!(!serialized.contains("nestedExtra"));
    }

    #[test]
    fn leading_and_trailing_junk_is_tolerated() {
        // text.indexOf('{') / text.lastIndexOf('}') skips past noise.
        let mut payload = b"<<garbage>>".to_vec();
        payload.extend_from_slice(&json_bytes(&well_formed_envelope()));
        payload.extend_from_slice(b"%%trailing trash%%");
        let ls = make_op_return_script(&payload);
        let meta = SonicstarTopicManager::decode_song_metadata(&ls).unwrap();
        assert_eq!(meta.song_title, "Hello");
    }

    #[test]
    fn metadata_serializes_camel_case_with_optionals_dropped() {
        let env = json!({
            "protocol": "sssp",
            "songTitle": "Title",
            "artistName": "Artist",
            "songFileURL": "uhrp://x",
            "duration": 120,
            "pricePerPlay": 500,
            "royaltyRate": 60,
        });
        let ls = make_op_return_script(&json_bytes(&env));
        let meta = SonicstarTopicManager::decode_song_metadata(&ls).unwrap();
        let s = serde_json::to_string(&meta).unwrap();
        assert!(s.contains("\"songTitle\":\"Title\""));
        assert!(s.contains("\"songFileURL\":\"uhrp://x\""));
        assert!(s.contains("\"pricePerPlay\":500"));
        assert!(s.contains("\"royaltyRate\":60"));
        assert!(s.contains("\"artistIdentityKey\":\"\""));
        // Optional fields absent on the wire stay absent in the output.
        assert!(!s.contains("description"));
        assert!(!s.contains("artFileURL"));
        assert!(!s.contains("genre"));
    }

    #[tokio::test]
    async fn topic_manager_metadata_and_docs() {
        let mgr = SonicstarTopicManager::new();
        let meta = mgr.get_metadata().await;
        assert!(meta.name.contains("SonicStar"));
        let docs = mgr.get_documentation().await;
        assert!(!docs.is_empty());
        assert!(docs.contains("sssp"));
    }
}
