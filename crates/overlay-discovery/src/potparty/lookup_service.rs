//! POTPARTY Lookup Service — indexes and queries LOW pot-participation
//! markers for seed-only recovery (bsv-low #188).
//!
//! When outputs are admitted to `tm_potparty`, this service parses the
//! `LOW/potparty/v1` marker and stores one row per marker OUTPOINT
//! `(txid, outputIndex)` via [`PotpartyStorage`] — EVERY admitted marker is
//! kept (a duplicate submit of the same output is a no-op; outpoint keying
//! keeps a garbage front-run from censoring a genuine marker, the
//! `tm_result` lesson). A recovering client asks `partyFor` (its own
//! identity → every pot it is in) or `byPot` (a pot outpoint → both
//! parties); the answer is a freeform, newest-first JSON array carrying the
//! marker's bytes back VERBATIM. The overlay never verifies the `sig` — the
//! record surface is bytes in, bytes out.
//!
//! Permanence: a pot-participation fact is permanent recovery history and
//! the admitted output is a provably-unspendable OP_RETURN.
//! `spend_notification_mode` is [`SpendNotificationMode::None`], and
//! `output_spent` / `output_evicted` are deliberate no-ops — a potparty
//! record is NEVER removed (mirrors `ls_pot`'s permanence).

use async_trait::async_trait;
use overlay_engine::lookup_service::{LookupService, LookupServiceError};
use overlay_engine::types::*;
use std::rc::Rc;
use tracing::debug;

use super::parse_potparty_marker;
use super::storage::{PotpartyQuery, PotpartyRecord, PotpartyStorage};

/// Default number of records returned when a query omits `limit`.
const DEFAULT_LIMIT: usize = 100;
/// Hard cap on the number of records a single query can return.
const MAX_LIMIT: usize = 500;

/// POTPARTY Lookup Service — indexes markers and answers `partyFor` /
/// `byPot`.
pub struct PotpartyLookupService {
    storage: Rc<dyn PotpartyStorage>,
}

impl PotpartyLookupService {
    /// Create a new POTPARTY lookup service backed by the given storage.
    pub fn new(storage: Rc<dyn PotpartyStorage>) -> Self {
        Self { storage }
    }
}

#[async_trait(?Send)]
impl LookupService for PotpartyLookupService {
    fn admission_mode(&self) -> AdmissionMode {
        AdmissionMode::LockingScript
    }

    fn spend_notification_mode(&self) -> SpendNotificationMode {
        // A potparty marker is permanent recovery history; we never want a
        // spend notification (the OP_RETURN can't be spent anyway) to touch
        // the index.
        SpendNotificationMode::None
    }

    async fn output_admitted_by_topic(
        &self,
        payload: &OutputAdmittedByTopic,
    ) -> Result<(), LookupServiceError> {
        let (txid, output_index, topic, locking_script) = match payload {
            OutputAdmittedByTopic::LockingScript {
                txid,
                output_index,
                topic,
                locking_script,
                ..
            } => (txid, *output_index, topic, locking_script),
            _ => {
                return Err(LookupServiceError::Other(
                    "Expected locking-script mode".into(),
                ))
            }
        };

        // Only index tm_potparty outputs.
        if topic != "tm_potparty" {
            return Ok(());
        }

        // The topic manager already validated the marker; re-parse to
        // recover the fields for the index (defensive — the TM should never
        // admit anything this can't parse).
        let Some(marker) = parse_potparty_marker(locking_script) else {
            debug!("POTPARTY: admitted output is not a parseable marker — skipped");
            return Ok(());
        };

        let record = PotpartyRecord {
            identity: hex::encode(&marker.identity),
            opponent_identity: hex::encode(&marker.opponent),
            game_id: hex::encode(marker.game_id),
            pot_txid: hex::encode(marker.pot_txid),
            pot_vout: marker.pot_vout,
            recovery_height: marker.recovery_height,
            sig_hex: hex::encode(&marker.sig),
            // v2 (#230) seat-binding fields — None for a v1 marker.
            seat_settle_pubkey: marker.seat_settle_pubkey.as_ref().map(hex::encode),
            seat_sig_hex: marker.seat_sig.as_ref().map(hex::encode),
            txid: txid.to_string(),
            output_index,
            created_at: 0, // assigned by the storage layer at insert
        };

        // Keyed by the OUTPOINT: the storage layer's insert-if-absent makes
        // a replayed submit of the same output a harmless no-op, while
        // markers from different txs are ALL kept.
        self.storage
            .store_record(&record)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        Ok(())
    }

    async fn output_spent(&self, _payload: &OutputSpent) -> Result<(), LookupServiceError> {
        // No-op: a potparty marker is PERMANENT recovery history. The
        // admitted output is an unspendable OP_RETURN, so this never fires
        // anyway — but even if it did, we must not evict the record.
        Ok(())
    }

    async fn output_evicted(
        &self,
        _txid: &str,
        _output_index: u32,
    ) -> Result<(), LookupServiceError> {
        // No-op: potparty records are never evicted (permanence — above).
        Ok(())
    }

    async fn lookup(&self, question: &LookupQuestion) -> Result<LookupResult, LookupServiceError> {
        if question.service != "ls_potparty" {
            return Err(LookupServiceError::Unsupported(format!(
                "Expected ls_potparty, got {}",
                question.service
            )));
        }

        let query: PotpartyQuery = serde_json::from_value(question.query.clone())
            .map_err(|e| LookupServiceError::InvalidQuery(e.to_string()))?;

        let records = match query {
            PotpartyQuery::PartyFor { identity, limit } => {
                let identity = normalize_identity(&identity)?;
                self.storage
                    .list_for_identity(&identity, clamp_limit(limit))
                    .await
                    .map_err(|e| LookupServiceError::StorageError(e.to_string()))?
            }
            PotpartyQuery::ByPot {
                pot_txid,
                pot_vout,
                limit,
                offset,
            } => {
                let pot_txid = normalize_txid(&pot_txid)?;
                self.storage
                    .list_for_pot(
                        &pot_txid,
                        pot_vout,
                        clamp_limit(limit),
                        // Page start (#354/#356): absent → the head of the
                        // oldest-first order, so every deployed client keeps
                        // exactly today's answer. Unclamped — a huge offset
                        // is just an empty page, never a scan (the window is
                        // still LIMIT-bounded).
                        offset.unwrap_or(0) as usize,
                    )
                    .await
                    .map_err(|e| LookupServiceError::StorageError(e.to_string()))?
            }
        };

        // Carry the stored bytes back VERBATIM — the overlay is an index,
        // not an authority (a client verifies the sig itself if it cares).
        let entries: Vec<serde_json::Value> = records
            .into_iter()
            .map(|r| {
                serde_json::json!({
                    "identity": r.identity,
                    "opponentIdentity": r.opponent_identity,
                    "gameId": r.game_id,
                    "potTxid": r.pot_txid,
                    "potVout": r.pot_vout,
                    "recoveryHeight": r.recovery_height,
                    "sigHex": r.sig_hex,
                    // v2 (#230): ALWAYS emitted, null for a v1 row — absent
                    // and null must stay distinguishable on the wire (the
                    // cardsHex lesson, bsv-low #276).
                    "seatSettlePubkey": r.seat_settle_pubkey,
                    "seatSigHex": r.seat_sig_hex,
                    "txid": r.txid,
                    "outputIndex": r.output_index,
                    "createdAt": r.created_at,
                })
            })
            .collect();

        Ok(LookupResult::Answer(LookupAnswer::Freeform {
            result: serde_json::Value::Array(entries),
        }))
    }

    async fn get_documentation(&self) -> String {
        include_str!("../../docs/potparty_lookup.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "POTPARTY Lookup Service".to_string(),
            description: Some(
                "Answers 'which pots is this identity a party to?' (partyFor) \
                 and 'who are the two parties to this pot?' (byPot) over LOW \
                 potparty markers — the seed-only recovery index."
                    .to_string(),
            ),
            ..Default::default()
        }
    }
}

/// Clamp an optional query limit to `1..=MAX_LIMIT` (default
/// [`DEFAULT_LIMIT`] when absent). A `limit: 0` still returns one page of
/// nothing-useful rather than erroring — clamped up to 1.
fn clamp_limit(limit: Option<u32>) -> usize {
    (limit.map(|l| l as usize).unwrap_or(DEFAULT_LIMIT)).clamp(1, MAX_LIMIT)
}

/// Validate a 33-byte identity-key hex param and return it lowercased
/// (stored values are lowercase `hex::encode` output).
fn normalize_identity(value: &str) -> Result<String, LookupServiceError> {
    let lower = value.to_ascii_lowercase();
    if lower.len() != 66 || !lower.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(LookupServiceError::InvalidQuery(
            "identity must be 66 hex chars (a 33-byte compressed pubkey)".into(),
        ));
    }
    Ok(lower)
}

/// Validate a 32-byte txid hex param and return it lowercased.
fn normalize_txid(value: &str) -> Result<String, LookupServiceError> {
    let lower = value.to_ascii_lowercase();
    if lower.len() != 64 || !lower.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(LookupServiceError::InvalidQuery(
            "potTxid must be 64 hex chars (a 32-byte txid)".into(),
        ));
    }
    Ok(lower)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::storage::MemoryPotpartyStorage;
    use super::super::tests::{
        golden_game_id, golden_identity, golden_marker, golden_opponent, golden_pot_txid,
        golden_recovery_height, golden_sig, marker_script,
    };
    use super::*;

    fn make_service_with_storage() -> (PotpartyLookupService, Rc<MemoryPotpartyStorage>) {
        let storage = Rc::new(MemoryPotpartyStorage::new());
        let svc = PotpartyLookupService::new(storage.clone());
        (svc, storage)
    }

    fn make_service() -> PotpartyLookupService {
        make_service_with_storage().0
    }

    fn admit(txid: &str, output_index: u32, script: Vec<u8>) -> OutputAdmittedByTopic {
        OutputAdmittedByTopic::LockingScript {
            txid: txid.into(),
            output_index,
            topic: "tm_potparty".into(),
            satoshis: 0,
            locking_script: script,
            off_chain_values: None,
        }
    }

    async fn run_lookup(
        svc: &PotpartyLookupService,
        query: serde_json::Value,
    ) -> serde_json::Value {
        let q = LookupQuestion::new("ls_potparty", query);
        match svc.lookup(&q).await.unwrap() {
            LookupResult::Answer(LookupAnswer::Freeform { result }) => result,
            other => panic!("expected Freeform answer, got {other:?}"),
        }
    }

    async fn party_for(
        svc: &PotpartyLookupService,
        identity: &str,
        limit: Option<u32>,
    ) -> serde_json::Value {
        let mut q = serde_json::json!({"type": "partyFor", "identity": identity});
        if let Some(l) = limit {
            q["limit"] = serde_json::json!(l);
        }
        run_lookup(svc, q).await
    }

    async fn by_pot(
        svc: &PotpartyLookupService,
        pot_txid: &str,
        pot_vout: u32,
    ) -> serde_json::Value {
        let q = serde_json::json!({"type": "byPot", "potTxid": pot_txid, "potVout": pot_vout});
        run_lookup(svc, q).await
    }

    fn golden_identity_hex() -> String {
        hex::encode(golden_identity())
    }
    fn golden_opponent_hex() -> String {
        hex::encode(golden_opponent())
    }

    // ── Trait plumbing ───────────────────────────────────────────────────

    #[tokio::test]
    async fn modes_and_metadata() {
        let svc = make_service();
        assert_eq!(svc.admission_mode(), AdmissionMode::LockingScript);
        assert_eq!(svc.spend_notification_mode(), SpendNotificationMode::None);
        let meta = svc.get_metadata().await;
        assert_eq!(meta.name, "POTPARTY Lookup Service");
        assert!(!svc.get_documentation().await.is_empty());
    }

    // ── Admission + partyFor (end-to-end) ────────────────────────────────

    #[tokio::test]
    async fn marker_admitted_and_found_by_identity() {
        let (svc, storage) = make_service_with_storage();
        let script = golden_marker(&golden_game_id(), &golden_pot_txid(), 3);
        svc.output_admitted_by_topic(&admit("markerTx1", 0, script))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1);

        let arr = party_for(&svc, &golden_identity_hex(), None).await;
        let e = &arr[0];
        assert_eq!(e["identity"], golden_identity_hex());
        assert_eq!(e["opponentIdentity"], golden_opponent_hex());
        assert_eq!(e["gameId"], "11".repeat(32));
        assert_eq!(e["potTxid"], "22".repeat(32));
        assert_eq!(e["potVout"], 3);
        assert_eq!(e["recoveryHeight"], golden_recovery_height());
        assert_eq!(e["sigHex"], hex::encode(golden_sig()));
        assert_eq!(e["txid"], "markerTx1");
        assert!(e["createdAt"].is_i64());
        // A v1 row carries EXPLICIT nulls for the v2 fields (present, null —
        // never absent; the wire distinguishes them).
        let obj = e.as_object().unwrap();
        assert!(obj.contains_key("seatSettlePubkey"));
        assert_eq!(e["seatSettlePubkey"], serde_json::Value::Null);
        assert!(obj.contains_key("seatSigHex"));
        assert_eq!(e["seatSigHex"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn v2_marker_admitted_and_answer_carries_seat_binding() {
        use super::super::tests::{golden_marker_v2, golden_settle_pubkey};
        let (svc, storage) = make_service_with_storage();
        let script = golden_marker_v2(&golden_game_id(), &golden_pot_txid(), 3);
        svc.output_admitted_by_topic(&admit("markerTxV2", 0, script))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1);

        let arr = party_for(&svc, &golden_identity_hex(), None).await;
        let e = &arr[0];
        assert_eq!(e["identity"], golden_identity_hex());
        assert_eq!(e["gameId"], "11".repeat(32));
        assert_eq!(e["potTxid"], "22".repeat(32));
        // The seat-binding fields ride back VERBATIM (never verified here —
        // the app-layer reader verifies).
        assert_eq!(e["seatSettlePubkey"], hex::encode(golden_settle_pubkey()));
        assert_eq!(e["seatSigHex"], hex::encode(golden_sig()));

        // byPot surfaces them too.
        let arr = by_pot(&svc, &"22".repeat(32), 3).await;
        assert_eq!(
            arr[0]["seatSettlePubkey"],
            hex::encode(golden_settle_pubkey())
        );
    }

    #[tokio::test]
    async fn v2_replay_of_same_outpoint_is_idempotent() {
        use super::super::tests::golden_marker_v2;
        let (svc, storage) = make_service_with_storage();
        let script = golden_marker_v2(&golden_game_id(), &golden_pot_txid(), 0);
        // The SAME output submitted twice (risk register B5: a replayed v2
        // marker must be a no-op — outpoint-keyed dedup).
        svc.output_admitted_by_topic(&admit("txSAME", 0, script.clone()))
            .await
            .unwrap();
        svc.output_admitted_by_topic(&admit("txSAME", 0, script))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1);
        let arr = party_for(&svc, &golden_identity_hex(), None).await;
        assert_eq!(arr.as_array().unwrap().len(), 1);
    }

    // ── partyFor filters by identity only (not the opponent's marker) ─────

    #[tokio::test]
    async fn party_for_filters_by_identity_only() {
        let (svc, _storage) = make_service_with_storage();
        // Seat A's marker (identity = golden_identity).
        svc.output_admitted_by_topic(&admit(
            "txA",
            0,
            golden_marker(&golden_game_id(), &golden_pot_txid(), 0),
        ))
        .await
        .unwrap();
        // Seat B's OWN marker for the same pot (seats flipped).
        svc.output_admitted_by_topic(&admit(
            "txB",
            0,
            marker_script(
                &golden_opponent(),
                &golden_identity(),
                &golden_game_id(),
                &golden_pot_txid(),
                0,
                golden_recovery_height(),
                &golden_sig(),
            ),
        ))
        .await
        .unwrap();

        // Seat A sees only its own marker under partyFor.
        let arr = party_for(&svc, &golden_identity_hex(), None).await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["txid"], "txA");

        // Seat B likewise.
        let arr = party_for(&svc, &golden_opponent_hex(), None).await;
        assert_eq!(arr.as_array().unwrap().len(), 1);

        // An identity in no pot sees an empty array.
        let stranger = "02".to_string() + &"ee".repeat(32);
        let arr = party_for(&svc, &stranger, None).await;
        assert!(arr.as_array().unwrap().is_empty());
    }

    // ── byPot returns both parties ────────────────────────────────────────

    #[tokio::test]
    async fn by_pot_returns_both_parties() {
        let (svc, _storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit(
            "txA",
            0,
            golden_marker(&golden_game_id(), &golden_pot_txid(), 0),
        ))
        .await
        .unwrap();
        svc.output_admitted_by_topic(&admit(
            "txB",
            0,
            marker_script(
                &golden_opponent(),
                &golden_identity(),
                &golden_game_id(),
                &golden_pot_txid(),
                0,
                golden_recovery_height(),
                &golden_sig(),
            ),
        ))
        .await
        .unwrap();

        let arr = by_pot(&svc, &"22".repeat(32), 0).await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 2, "both parties to the pot");
        // OLDEST first (bsv-low #281) — the honest seats publish at funding,
        // so later dust naming the pot can never displace them.
        assert_eq!(arr[0]["txid"], "txA");
        assert_eq!(arr[1]["txid"], "txB");

        // A different vout matches nobody.
        let arr = by_pot(&svc, &"22".repeat(32), 9).await;
        assert!(arr.as_array().unwrap().is_empty());
    }

    /// bsv-low#354/#356 — `byPot` is PAGEABLE end to end, through the real
    /// wire shape.
    ///
    /// This window is scoped only by the pot outpoint, which is a payload
    /// CLAIM, so a stranger's markers (including ones filed before the pot was
    /// funded, which sort AHEAD of the honest seats in the server-stamped
    /// order) can fill page 0. Until this change the wire carried no cursor
    /// and page 0 was the whole reachable set.
    #[tokio::test]
    async fn by_pot_is_pageable_and_defaults_to_the_head() {
        let (svc, _storage) = make_service_with_storage();
        for (i, tx) in ["txA", "txB"].iter().enumerate() {
            let script = if i == 0 {
                golden_marker(&golden_game_id(), &golden_pot_txid(), 0)
            } else {
                marker_script(
                    &golden_opponent(),
                    &golden_identity(),
                    &golden_game_id(),
                    &golden_pot_txid(),
                    0,
                    golden_recovery_height(),
                    &golden_sig(),
                )
            };
            svc.output_admitted_by_topic(&admit(tx, 0, script))
                .await
                .unwrap();
        }
        let pot = "22".repeat(32);
        let page = |limit: u32, offset: Option<u32>| {
            let mut q = serde_json::json!({
                "type": "byPot", "potTxid": pot, "potVout": 0, "limit": limit
            });
            if let Some(o) = offset {
                q["offset"] = serde_json::json!(o);
            }
            q
        };

        // Page 0 of 1 buries the second seat; page 1 reaches it. Without the
        // cursor the second row was unreachable at this limit, forever.
        let p0 = run_lookup(&svc, page(1, Some(0))).await;
        assert_eq!(p0.as_array().unwrap().len(), 1);
        assert_eq!(p0[0]["txid"], "txA");
        let p1 = run_lookup(&svc, page(1, Some(1))).await;
        assert_eq!(p1[0]["txid"], "txB", "the buried seat is REACHABLE");
        // Past the end is an empty page, never an error.
        assert!(run_lookup(&svc, page(1, Some(99)))
            .await
            .as_array()
            .unwrap()
            .is_empty());
        // ABSENT on the wire = the head of the order: every already-deployed
        // caller keeps exactly the answer it gets today.
        let none = run_lookup(&svc, page(1, None)).await;
        assert_eq!(none[0]["txid"], "txA");
    }

    /// The SERVED contract must name the cursor (epoch Rule 16: an artifact
    /// published as the spec is a contract surface, and it drifts from the
    /// parser silently). The consumer on the far side reads this markdown,
    /// not `PotpartyQuery`.
    #[tokio::test]
    async fn the_served_documentation_names_the_by_pot_cursor() {
        let doc = make_service().get_documentation().await;
        assert_eq!(
            doc.matches("\"offset\"").count(),
            1,
            "the byPot example carries the cursor: {doc}"
        );
        assert!(
            doc.contains("byPot.offset"),
            "…and its semantics are spelled out, not left to be inferred"
        );
    }

    // ── Ordering + limit ──────────────────────────────────────────────────

    #[tokio::test]
    async fn newest_pot_first_and_limit() {
        let (svc, _storage) = make_service_with_storage();
        // FIVE DISTINCT POTS — since bsv-low #281 `partyFor`'s window counts
        // POTS, so markers must name different pots to occupy different slots.
        for i in 1u8..=5 {
            svc.output_admitted_by_topic(&admit(
                &format!("tx{i}"),
                0,
                golden_marker(&[i; 32], &[0xd0 + i; 32], 0),
            ))
            .await
            .unwrap();
        }
        let arr = party_for(&svc, &golden_identity_hex(), Some(3)).await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 3, "limit respected");
        assert_eq!(arr[0]["txid"], "tx5", "newest pot first");

        // limit clamps.
        assert_eq!(clamp_limit(None), DEFAULT_LIMIT);
        assert_eq!(clamp_limit(Some(0)), 1);
        assert_eq!(clamp_limit(Some(1_000_000)), MAX_LIMIT);
    }

    // ── Outpoint keying ──────────────────────────────────────────────────

    #[tokio::test]
    async fn same_outpoint_replay_is_a_noop() {
        let (svc, storage) = make_service_with_storage();
        let script = golden_marker(&golden_game_id(), &golden_pot_txid(), 0);
        svc.output_admitted_by_topic(&admit("txSAME", 0, script.clone()))
            .await
            .unwrap();
        svc.output_admitted_by_topic(&admit("txSAME", 0, script))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1, "same-outpoint replay is a no-op");
    }

    // ── Admission filters ────────────────────────────────────────────────

    #[tokio::test]
    async fn ignores_non_tm_potparty_topic() {
        let (svc, storage) = make_service_with_storage();
        let mut payload = admit(
            "tx1",
            0,
            golden_marker(&golden_game_id(), &golden_pot_txid(), 0),
        );
        if let OutputAdmittedByTopic::LockingScript { ref mut topic, .. } = payload {
            *topic = "tm_result".into();
        }
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn ignores_non_marker_script() {
        let (svc, storage) = make_service_with_storage();
        let p2pkh = hex::decode("76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac").unwrap();
        svc.output_admitted_by_topic(&admit("tx1", 0, p2pkh))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn rejects_whole_tx_mode() {
        let svc = make_service();
        let payload = OutputAdmittedByTopic::WholeTx {
            atomic_beef: vec![],
            output_index: 0,
            topic: "tm_potparty".into(),
            off_chain_values: None,
        };
        assert!(svc.output_admitted_by_topic(&payload).await.is_err());
    }

    // ── Permanence: spend / eviction are no-ops ──────────────────────────

    #[tokio::test]
    async fn spend_and_eviction_never_remove_a_record() {
        let (svc, storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit(
            "tx1",
            0,
            golden_marker(&golden_game_id(), &golden_pot_txid(), 0),
        ))
        .await
        .unwrap();
        assert_eq!(storage.record_count(), 1);

        let spent = OutputSpent::None {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_potparty".into(),
        };
        svc.output_spent(&spent).await.unwrap();
        assert_eq!(storage.record_count(), 1, "marker must survive a spend");
        svc.output_evicted("tx1", 0).await.unwrap();
        assert_eq!(storage.record_count(), 1, "marker must survive an eviction");
    }

    // ── Case-insensitive hex ──────────────────────────────────────────────

    #[tokio::test]
    async fn lookup_case_insensitive_hex() {
        let (svc, _storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit(
            "txA",
            0,
            golden_marker(&golden_game_id(), &golden_pot_txid(), 0),
        ))
        .await
        .unwrap();
        let arr = party_for(&svc, &golden_identity_hex().to_uppercase(), None).await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["identity"], golden_identity_hex());
    }

    // ── Query validation ─────────────────────────────────────────────────

    #[tokio::test]
    async fn lookup_wrong_service_errors() {
        let svc = make_service();
        let q = LookupQuestion::new("ls_result", serde_json::json!({"type": "partyFor"}));
        assert!(svc.lookup(&q).await.is_err());
    }

    #[tokio::test]
    async fn lookup_invalid_query_errors() {
        let svc = make_service();
        for bad in [
            serde_json::json!({"type": "unknownQuery"}),
            serde_json::json!("partyFor"),
            serde_json::json!(42),
            serde_json::json!({"type": "partyFor"}), // missing identity
            serde_json::json!({"type": "partyFor", "identity": "zz".repeat(33)}),
            serde_json::json!({"type": "partyFor", "identity": "02a1"}),
            serde_json::json!({"type": "byPot", "potVout": 0}), // missing potTxid
            serde_json::json!({"type": "byPot", "potTxid": "zz", "potVout": 0}),
        ] {
            let q = LookupQuestion::new("ls_potparty", bad.clone());
            assert!(svc.lookup(&q).await.is_err(), "expected error for {bad}");
        }
    }

    /// #335 (bsv-low) Rule-16 CROSS-REPO BOUNDARY PIN (overlay half).
    ///
    /// `fixtures/ls_potparty_partyfor.fixture.json` is the EXACT `result`
    /// array the real `lookup()` serializer emits for `partyFor` — and a
    /// byte-identical copy is asserted by the bsv-low CLIENT
    /// (`app/src/lib/fixtures/ls_potparty_partyfor.fixture.json`, driven
    /// through the real `lookupPotParty` parser + real signature
    /// verification by `overlay.potpartyLookup.test.ts`). The client THROWS
    /// on a structured row missing `identity`/`opponentIdentity`/`sigHex`
    /// and drops any row that fails verification, so the two cells together
    /// pin the wire contract that used to be held only by coincidence of
    /// spelling: rename a serialized field HERE and this cell goes red
    /// (lookup output != fixture); drift the client's parser and ITS cell
    /// goes red (rows dropped / throw). A property spanning two repos cannot
    /// be pinned inside either one — regenerate BOTH copies together or not
    /// at all.
    ///
    /// The record literals below are Rust-side ON PURPOSE (not parsed out of
    /// the fixture): deriving them from the fixture would make the
    /// comparison circular. The sigs are genuine (signPotPartyMarker /
    /// signPotPartyV2Marker under the client's fixed test key).
    #[tokio::test]
    async fn partyfor_answer_matches_cross_repo_fixture() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/ls_potparty_partyfor.fixture.json"))
                .expect("fixture parses");

        let identity = "02fe8d1eb1bcb3432b1db5833ff5f2226d9cb5e65cee430558c18ed3a3c86ce1af";
        let opponent = "03d528ecd9b696b54c907a9ed045447a79bb408ec39b68df504bb51f459bc3ffc9";
        let record = |game_id: &str,
                      pot_txid: &str,
                      recovery_height: u32,
                      sig_hex: &str,
                      seat: Option<(&str, &str)>,
                      txid: &str| PotpartyRecord {
            identity: identity.into(),
            opponent_identity: opponent.into(),
            game_id: game_id.into(),
            pot_txid: pot_txid.into(),
            pot_vout: 0,
            recovery_height,
            sig_hex: sig_hex.into(),
            seat_settle_pubkey: seat.map(|(pk, _)| pk.to_string()),
            seat_sig_hex: seat.map(|(_, s)| s.to_string()),
            txid: txid.into(),
            output_index: 0,
            created_at: 0, // ignored — storage stamps insertion order 0,1,2
        };

        let (svc, storage) = make_service_with_storage();
        // Insertion order = created_at order: pot1 v1 (0), pot2 v1 (1),
        // pot2 v2 (2). The partyFor window returns them newest-first.
        storage
            .store_record(&record(
                &"11".repeat(32),
                &"aa".repeat(32),
                900_000,
                "30450221009c63209518bc25ba20dac269f781a415a338eb910d04ee24b60a62f2f5d8431e022023185721992adae46be9e532b2d8930affce31f17d904d072ff55ae54600984e",
                None,
                &"c1".repeat(32),
            ))
            .await
            .unwrap();
        storage
            .store_record(&record(
                &"22".repeat(32),
                &"bb".repeat(32),
                900_100,
                "3045022100b40b70f6bf11a7af3f31afa6e5052aa3ba87b13639789c192270cfbcece57abc022062cfd033d3ab32ee0626171bf584b484e2e9b722ea074286fb033430021c1d0c",
                None,
                &"c2".repeat(32),
            ))
            .await
            .unwrap();
        storage
            .store_record(&record(
                &"22".repeat(32),
                &"bb".repeat(32),
                900_100,
                "3045022100fd60a6e60c19700afa75394660111c5762b2adff319f650214b907015e0d26bd02201fb0a2cbb43a4828867d8d4e3452586cd33530ce6807aa74acfdd0fc7fb7a8d1",
                Some((
                    "021012227c1931bdbcc58e96eacd1e0366642ec51277e3606343274b1639b7126d",
                    "3045022100f6805d81fa74d3ea416569de4089666f6abac1bd16f7d4aee4b35866d421621d02203f6313f04e516c7a91b919352db5c1c65de21588a1903e083d5555670598d1e9",
                )),
                &"c3".repeat(32),
            ))
            .await
            .unwrap();

        let answer = party_for(&svc, identity, None).await;
        assert_eq!(
            answer, fixture,
            "the serialized partyFor answer must match the cross-repo fixture BYTE-FOR-BYTE \
             (field names are the wire contract with bsv-low's lookupPotParty)"
        );
        // Loud-count guard: the pin must be comparing three real rows, not
        // two empty arrays agreeing with each other.
        assert_eq!(answer.as_array().map(|a| a.len()), Some(3));
    }
}
