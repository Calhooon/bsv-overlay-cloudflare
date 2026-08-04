//! HOPPARTY Topic Manager — validates LOW hop-in-flight markers.
//!
//! See [`super`] for the full on-wire format: the `LOW/hopparty/v1`
//! `OP_RETURN` data carrier (9 pushes: tag / identity / opponentIdentity /
//! gameId / hopVout / hopSats / seatSettlePubkey / seatSig / identitySig).
//! There is no hop txid on the wire — the marker rides the hop tx, so the
//! CONTAINER supplies it.
//!
//! Like `tm_potparty` / `tm_result` / `tm_collected`, there is NO
//! server-side signature verification: the marker is admitted by BYTE
//! FORMAT ONLY (plus the structural `identity != opponentIdentity` rule) —
//! parsing succeeding is the entire gate. The signatures it carries are
//! preserved verbatim; the app-layer's `/hops-view` verifies them at READ
//! as a display filter, and clients verify before acting. The overlay is an
//! INDEX, not an authority (zanaadu invariant #3).

use async_trait::async_trait;
use bsv_rs::transaction::Transaction;
use overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use overlay_engine::types::{AdmittanceInstructions, ServiceMetadata, SubmitMode};
use tracing::{debug, warn};

use super::is_hopparty_marker_script;

/// HOPPARTY Topic Manager — identifies admissible LOW hopparty markers.
pub struct HoppartyTopicManager;

impl HoppartyTopicManager {
    /// Create a new HOPPARTY Topic Manager.
    pub fn new() -> Self {
        Self
    }

    /// Validate a single output as a LOW hopparty marker: true IFF its
    /// locking script is a well-formed `LOW/hopparty/v1` marker (exact tag +
    /// exactly [`super::HOPPARTY_FIELD_COUNT`] pushes + exact push lengths +
    /// identity != opponentIdentity). Everything else (P2PKH change — including the hop
    /// output riding the SAME tx — foreign OP_RETURNs, both potparty tags,
    /// malformed shapes) is simply not admitted.
    pub fn validate_hopparty_output(output: &bsv_rs::transaction::TransactionOutput) -> bool {
        is_hopparty_marker_script(&output.locking_script.to_binary())
    }
}

impl Default for HoppartyTopicManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait(?Send)]
impl TopicManager for HoppartyTopicManager {
    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        _previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        _mode: SubmitMode,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        let mut outputs_to_admit = Vec::new();

        for (i, output) in tx.outputs.iter().enumerate() {
            if Self::validate_hopparty_output(output) {
                debug!("HOPPARTY: admitted output {i}");
                outputs_to_admit.push(i as u32);
            }
            // Not a hopparty marker (the hop P2PKH itself, wallet change, a
            // potparty marker, a foreign OP_RETURN, …) — skip. The strict
            // byte-format parse keeps junk out of the index.
        }

        if outputs_to_admit.is_empty() {
            warn!("HOPPARTY: no outputs admitted");
        }

        Ok(AdmittanceInstructions {
            outputs_to_admit,
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }

    async fn get_documentation(&self) -> String {
        include_str!("../../docs/hopparty_topic.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "HOPPARTY Topic Manager".to_string(),
            description: Some(
                "Indexes LOW hop-in-flight markers \
                 (LOW/hopparty/v1 OP_RETURN) keyed by identity so a \
                 pre-JOIN funding hop is enumerable (bsv-low #315)."
                    .to_string(),
            ),
            ..Default::default()
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
pub(crate) mod tests {
    use super::super::tests::{
        golden_game_id, golden_identity, golden_marker, golden_opponent, golden_sats,
        golden_settle_pubkey, golden_sig, golden_vout, marker_script, push_data,
    };
    use super::super::HOPPARTY_TAG;
    use super::*;
    use bsv_rs::script::LockingScript;
    use bsv_rs::transaction::{Transaction as Tx, TransactionInput, TransactionOutput};

    /// A valid hopparty-marker `TransactionOutput` (0-sat OP_RETURN).
    pub(crate) fn make_hopparty_output(game_id: &[u8; 32]) -> TransactionOutput {
        let script = golden_marker(game_id, golden_vout());
        TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&script).unwrap(),
            change: false,
        }
    }

    // ── Valid markers ─────────────────────────────────────────────────────

    #[test]
    fn valid_marker_admitted() {
        let out = make_hopparty_output(&[0x42u8; 32]);
        assert!(HoppartyTopicManager::validate_hopparty_output(&out));
    }

    /// The cross-leak refusal at the TM level, both directions: neither
    /// potparty tag is admitted by THIS manager, and a hopparty marker is
    /// not admitted by the potparty manager. (The parser-level twin lives in
    /// `super::super::tests::wrong_tag_rejected_including_both_potparty_tags`;
    /// this cell proves the refusal through the TM entry production wires.)
    #[test]
    fn potparty_markers_not_admitted_and_vice_versa() {
        use crate::potparty::tests::{golden_marker as pp_v1, golden_marker_v2 as pp_v2};
        use crate::potparty::topic_manager::PotpartyTopicManager;
        for script in [
            pp_v1(&golden_game_id(), &[0x22u8; 32], 0),
            pp_v2(&golden_game_id(), &[0x22u8; 32], 0),
        ] {
            let out = TransactionOutput {
                satoshis: Some(0),
                locking_script: LockingScript::from_binary(&script).unwrap(),
                change: false,
            };
            assert!(
                !HoppartyTopicManager::validate_hopparty_output(&out),
                "a potparty marker must never be admitted under tm_hopparty"
            );
        }
        let hop = make_hopparty_output(&golden_game_id());
        assert!(
            !PotpartyTopicManager::validate_potparty_output(&hop),
            "a hopparty marker must never be admitted under tm_potparty"
        );
        // Positive control: the same hop output IS admitted by its own TM.
        assert!(HoppartyTopicManager::validate_hopparty_output(&hop));
    }

    // ── Not-a-marker (skip) ───────────────────────────────────────────────

    #[test]
    fn p2pkh_not_admitted() {
        // The hop output itself is a plain P2PKH — tm_lowfund's job, never
        // this manager's.
        let output = TransactionOutput {
            satoshis: Some(80_800),
            locking_script: LockingScript::from_hex(
                "76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac",
            )
            .unwrap(),
            change: false,
        };
        assert!(!HoppartyTopicManager::validate_hopparty_output(&output));
    }

    #[test]
    fn foreign_op_return_not_admitted() {
        let mut s = vec![0x00, 0x6au8];
        s.extend(push_data(b"SOMETHINGELSE"));
        s.extend(push_data(&[0x11u8; 32]));
        let output = TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&s).unwrap(),
            change: false,
        };
        assert!(!HoppartyTopicManager::validate_hopparty_output(&output));
    }

    #[test]
    fn self_paired_marker_not_admitted() {
        let script = marker_script(
            &golden_identity(),
            &golden_identity(),
            &golden_game_id(),
            golden_vout(),
            golden_sats(),
            &golden_settle_pubkey(),
            &golden_sig(),
            &golden_sig(),
        );
        let output = TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&script).unwrap(),
            change: false,
        };
        assert!(!HoppartyTopicManager::validate_hopparty_output(&output));
    }

    #[test]
    fn malformed_tagged_marker_not_admitted() {
        // hopparty-TAGGED but a 4-byte hopSats — strict widths reject it.
        let mut s = vec![0x00, 0x6au8];
        s.extend(push_data(HOPPARTY_TAG));
        s.extend(push_data(&golden_identity()));
        s.extend(push_data(&golden_opponent()));
        s.extend(push_data(&golden_game_id()));
        s.extend(push_data(&golden_vout().to_le_bytes()));
        s.extend(push_data(&(golden_sats() as u32).to_le_bytes())); // 4B, not 8B
        s.extend(push_data(&golden_settle_pubkey()));
        s.extend(push_data(&golden_sig()));
        s.extend(push_data(&golden_sig()));
        let output = TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&s).unwrap(),
            change: false,
        };
        assert!(!HoppartyTopicManager::validate_hopparty_output(&output));
    }

    // ── Whole-transaction admission via BEEF ─────────────────────────────

    /// The PRODUCTION shape: the marker rides the hop `createAction` as a
    /// SECOND OUTPUT beside the hop P2PKH. The manager must admit exactly
    /// the marker output and skip the money.
    #[tokio::test]
    async fn identify_admissible_outputs_over_beef() {
        let marker = make_hopparty_output(&golden_game_id());
        // The hop P2PKH first — must be skipped.
        let hop_p2pkh = TransactionOutput {
            satoshis: Some(80_800),
            locking_script: LockingScript::from_hex(
                "76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac",
            )
            .unwrap(),
            change: false,
        };

        let mut tx = Tx::new();
        tx.add_input(TransactionInput::new("00".repeat(32), 0))
            .unwrap();
        tx.add_output(hop_p2pkh).unwrap();
        tx.add_output(marker).unwrap();
        let beef = tx.to_beef(true).expect("BEEF serialization");

        let mgr = HoppartyTopicManager::new();
        let instructions = mgr
            .identify_admissible_outputs(
                &Tx::from_beef(&beef, None).expect("engine-side parse"),
                &[],
                None,
                SubmitMode::HistoricalTxNoSpv,
            )
            .await
            .unwrap();
        // Only the marker (index 1) is admitted.
        assert_eq!(instructions.outputs_to_admit, vec![1]);
    }

    /// bsv-low #315 gate HIGH-2 — **the SERVED DOCUMENTATION is a contract
    /// surface, and it must not be able to drift from the parser.**
    ///
    /// `get_documentation()` `include_str!`s `docs/hopparty_topic.md` and
    /// the overlay serves it publicly; since the client half does not exist
    /// yet, that markdown is what the client implementer builds against. A
    /// stale wire table there is not a cosmetic defect: a client that emits
    /// the documented shape gets its marker REFUSED by `tm_hopparty`, and
    /// because the marker rides the hop `createAction` the transaction is
    /// already on chain and can never be re-made — the hop is permanently
    /// unmarked, which is the exact invisibility this feature exists to fix.
    ///
    /// This is a REAL PARSE of the doc (Rule 9's option 2), not a substring
    /// test, and it CROSSES THE BOUNDARY (Rule 16): the row count the doc
    /// publishes is fed to the real `parse_hopparty_marker`, so a doc that
    /// disagrees with the parser fails here rather than in a client's
    /// unrepeatable funding transaction.
    #[tokio::test]
    async fn served_documentation_wire_table_matches_the_parser() {
        let doc = HoppartyTopicManager::new().get_documentation().await;

        // ── Parse the wire-format table: rows shaped `| N | name | … |`.
        // (A table parse, so PROSE mentioning a field name — e.g. the
        // paragraph explaining that hopTxid is absent — cannot satisfy or
        // break it. Rule 9's "the scanner counts prose" mode.)
        let rows: Vec<(usize, String)> = doc
            .lines()
            .filter_map(|line| {
                let cells: Vec<&str> = line.trim().split('|').map(str::trim).collect();
                // ["", idx, name, encoding, ""] for a well-formed row.
                if cells.len() < 4 {
                    return None;
                }
                cells[1].parse::<usize>().ok().map(|i| (i, cells[2].to_string()))
            })
            .collect();

        // POSITIVE count assertion: "matched nothing" must fail loudly.
        assert_eq!(
            rows.len(),
            super::super::HOPPARTY_FIELD_COUNT,
            "the served wire table must publish exactly the parser's push count; \
             parsed rows: {rows:?}"
        );

        // The ordered field names ARE the contract the client builds to.
        let names: Vec<&str> = rows.iter().map(|(_, n)| n.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "tag",
                "identity",
                "opponentIdentity",
                "gameId",
                "hopVout",
                "hopSats",
                "seatSettlePubkey",
                "seatSig",
                "identitySig",
            ],
            "the served field order drifted from the wire contract"
        );
        // Indices must be 0..n-1 in order (a renumbering is drift too).
        assert!(
            rows.iter().enumerate().all(|(i, (n, _))| i == *n),
            "the served table's push indices must be 0..n-1 in order: {rows:?}"
        );

        // ── BOUNDARY CROSSING: build a marker with the push count the DOC
        // publishes and drive it through the REAL parser. If the doc ever
        // says 10 again, this constructs a 10-push marker and the parser
        // refuses it — RED here instead of on-chain.
        let documented_push_count = rows.len();
        let mut script = vec![0x00, 0x6au8];
        script.extend(push_data(super::super::HOPPARTY_TAG));
        script.extend(push_data(&golden_identity()));
        script.extend(push_data(&golden_opponent()));
        script.extend(push_data(&golden_game_id()));
        script.extend(push_data(&golden_vout().to_le_bytes()));
        script.extend(push_data(&golden_sats().to_le_bytes()));
        script.extend(push_data(&golden_settle_pubkey()));
        script.extend(push_data(&golden_sig()));
        script.extend(push_data(&golden_sig()));
        // Pad to whatever the doc claims, so a LARGER documented count is
        // built and refused rather than silently ignored.
        for _ in super::super::HOPPARTY_FIELD_COUNT..documented_push_count {
            script.extend(push_data(&[0x99u8; 32]));
        }
        assert!(
            super::super::parse_hopparty_marker(&script).is_some(),
            "a marker built to the SERVED documentation must be admissible"
        );
        let out = TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&script).unwrap(),
            change: false,
        };
        assert!(
            HoppartyTopicManager::validate_hopparty_output(&out),
            "…and must clear the real admission gate"
        );

        // ── The digest sections must publish the CURRENT domain tags. The
        // needles are split so they can never match this assertion's own
        // source text (Rule 9, third failure mode).
        for needle in [
            ["LOW/hopparty/v1", "/seatsig|"].concat(),
            ["low", " potparty"].concat(), // the REUSED wallet protocol id
        ] {
            assert!(
                doc.contains(&needle),
                "the served doc must publish the digest recipe: {needle}"
            );
        }
    }

    #[tokio::test]
    async fn topic_manager_trait_works() {
        let mgr = HoppartyTopicManager::new();
        let meta = mgr.get_metadata().await;
        assert_eq!(meta.name, "HOPPARTY Topic Manager");
        assert!(!mgr.get_documentation().await.is_empty());
    }
}
