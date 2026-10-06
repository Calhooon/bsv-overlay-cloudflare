//! The REAL replay through the REAL lookup service (bsv-low #553).
//!
//! `ProofLookupService` no longer links the replay: a deployment hands it
//! `low_proof_replay::prove_bundle` (`with_prover`). This is the cell that
//! used to live in `overlay-discovery`'s `proof/lookup_service.rs` and it
//! pins the same thing over the same bytes: a real bundle a deployed client
//! published is replayed ONCE at admission and both re-derived hands ride
//! the record, and a marker whose bundle the replay refuses is still
//! admitted, stored `bundleValid = Some(false)`.

use std::rc::Rc;

use overlay_discovery::proof::lookup_service::ProofLookupService;
use overlay_discovery::proof::storage::{MemoryProofStorage, ProofStorage};
use overlay_discovery::proof::PROOF_TAG;
use overlay_engine::lookup_service::LookupService;
use overlay_engine::types::OutputAdmittedByTopic;

const REAL: &[u8] = include_bytes!("../src/fixtures/bundle-a1081773.bin");
const REAL_GAME: &str = "a1081773673e8c7cb6093db8f4a59166495f15e9ded1fe354ee27bbda7922523";
const REAL_WINNER: &str = "03926129919f02ae2910ef7505aec13bd9aa937db5e38352f8f20028e0858218e0";

/// A minimal data push (the marker's own encoding rule).
fn push_data(blob: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let len = blob.len();
    if len < 0x4c {
        out.push(len as u8);
    } else if len <= 0xff {
        out.extend([0x4c, len as u8]);
    } else {
        assert!(len <= 0xffff, "a marker bundle is capped below 64 KiB");
        out.extend([0x4d, (len & 0xff) as u8, (len >> 8) as u8]);
    }
    out.extend_from_slice(blob);
    out
}

/// `OP_FALSE OP_RETURN <tag> <gameId> <winner> <sig> <bundle>`. The sig is
/// DER-shaped filler: admission is byte-format-only and never verifies it.
fn marker_script(game_id: &[u8; 32], winner: &[u8], bundle: &[u8]) -> Vec<u8> {
    let mut sig = vec![0x30u8, 0x45];
    sig.extend_from_slice(&[0xab; 69]);
    let mut s = vec![0x00, 0x6a];
    for field in [PROOF_TAG, game_id, winner, &sig, bundle] {
        s.extend(push_data(field));
    }
    s
}

fn admit(txid: &str, script: Vec<u8>) -> OutputAdmittedByTopic {
    OutputAdmittedByTopic::LockingScript {
        txid: txid.into(),
        output_index: 0,
        topic: "tm_proof".into(),
        satoshis: 0,
        locking_script: script,
        off_chain_values: None,
    }
}

#[tokio::test]
async fn admission_replays_the_bundle_and_stores_both_hands() {
    let game: [u8; 32] = hex::decode(REAL_GAME).unwrap().try_into().unwrap();
    let winner = hex::decode(REAL_WINNER).unwrap();
    let storage = Rc::new(MemoryProofStorage::new());
    let svc = ProofLookupService::with_prover(storage.clone(), low_proof_replay::prove_bundle);

    svc.output_admitted_by_topic(&admit("txREAL", marker_script(&game, &winner, REAL)))
        .await
        .unwrap();
    let rows = storage
        .list_for_game_winner(REAL_GAME, REAL_WINNER, 10)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    assert_eq!(r.bundle_valid, Some(true));
    assert_eq!(r.winner_seat, Some(0));
    assert_eq!(
        r.seat_a.as_deref(),
        Some("032f0bceeaf001f7d16871c9eba014004a17489c8a39aec9d4e9cccf626fe66e8d")
    );
    assert_eq!(r.winner_cards_hex.as_deref(), Some("011f232733"));
    assert_eq!(r.loser_cards_hex.as_deref(), Some("151c1d2d31"));
    assert_eq!(r.bundle, REAL, "the bytes are still retained verbatim");

    // A format-valid marker whose bundle is not a transcript: admitted,
    // refused by the replay.
    let other_game = [0x11u8; 32];
    svc.output_admitted_by_topic(&admit(
        "txGARBAGE",
        marker_script(&other_game, &winner, b"{\"v\":1}"),
    ))
    .await
    .unwrap();
    let g = storage
        .list_for_game_winner(&hex::encode(other_game), REAL_WINNER, 10)
        .await
        .unwrap();
    assert_eq!(g.len(), 1);
    assert_eq!(g[0].bundle_valid, Some(false));
    assert!(g[0].winner_cards_hex.is_none() && g[0].loser_cards_hex.is_none());

    // The same real bundle under the WRONG game id: the replay binds the
    // bundle to the marker's own pushes, so this is refused too.
    svc.output_admitted_by_topic(&admit("txWRONG", marker_script(&other_game, &winner, REAL)))
        .await
        .unwrap();
    let w = storage
        .list_for_game_winner(&hex::encode(other_game), REAL_WINNER, 10)
        .await
        .unwrap();
    let wrong = w.iter().find(|r| r.txid == "txWRONG").unwrap();
    assert_eq!(wrong.bundle_valid, Some(false));
}
