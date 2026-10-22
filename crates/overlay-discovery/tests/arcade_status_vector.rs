//! P0-2b (bsv-stack-lean `docs/p0/p0-2b.md`): Arcade's reorg-correction
//! markers replayed through this crate's reading of them.
//!
//! `vectors/arcade_status_verdicts.json` (carried byte-identical by
//! bsv-wallet-toolbox-rs) holds one recorded body per Arcade status word on
//! each surface, the intake refusals and the two reorg payloads
//! (arcade@1ae1208 `models/transaction.go:319-332`). Every push body is read
//! here: the two reorg payloads must name their marker, and no other
//! `extraInfo` (a rejection line, a retry reason) may read as one, because a
//! marker schedules a re-verify of a stored proof (`pot::reorg`).
//!
//! The vector's `engine_gate`, `engine_callback`, `engine_submit` and
//! `engine_proof` columns are the overlay-cloudflare crate's readings,
//! replayed in that crate (`src/arcade_vector_replay.rs`, P0-2d); it builds
//! only beside the bsv-low sources (`crates/low-proof-replay/Cargo.toml:21-22`).
//! Since bsv-low #624 (L8) every one of those readings is bsv-tracker's
//! `Verdict::from_arc_body` + `into_hint` under the door's policy, and the
//! vector's `words` classes are replayed there too
//! (`arcade_vector_replay::every_arcade_word_reads_its_class_through_the_tracker`).
//!
//! The copy's source (bsv-low #845, 2026-10-22): bsv-wallet-toolbox-rs 0.11.2
//! (tag `v0.11.2`, `41da5da`), `tests/vectors/arcade_status_verdicts.json`,
//! whose three `status_*` rows for RECEIVED, SENT_TO_NETWORK and
//! ACCEPTED_BY_NETWORK say `toolbox_status: unknown` (a hint that promotes
//! nothing; the toolbox's `e28acdc`); before it 0.7.4 (tag `v0.7.4`, main
//! `623f890`, sha256 `80d2ac72112d...`, align-engine-050, 2026-10-10). The rule
//! is `cmp`: this file is that file byte for byte (sha256 `4a28ff8b5cbe...`,
//! pinned whole below), never an edit here. A change is made in the toolbox's
//! copy and copied back.

use bsv_overlay_discovery::pot::reorg::{arcade_reorg_marker, ArcadeReorgMarker};

const VECTOR: &str = include_str!("../vectors/arcade_status_verdicts.json");

/// sha256 of bsv-wallet-toolbox-rs 0.11.2's `tests/vectors/arcade_status_verdicts.json`.
const TOOLBOX_0112_SHA256: &str =
    "4a28ff8b5cbe0c0c56cb36aabbf0012755efd0b1aae783204bc64f118c55b913";

#[test]
fn the_vector_is_the_toolbox_0_11_2_bytes() {
    let got = hex::encode(bsv_rs::primitives::sha256(VECTOR.as_bytes()));
    assert_eq!(
        got, TOOLBOX_0112_SHA256,
        "the vector is not bsv-wallet-toolbox-rs 0.11.2's (#845)"
    );
}

#[test]
fn every_arcade_push_body_reads_its_reorg_marker_and_no_other_extra_info_does() {
    let v: serde_json::Value = serde_json::from_str(VECTOR).unwrap();
    let pushes: Vec<&serde_json::Value> = v["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["surface"] == "push")
        .collect();
    assert!(!pushes.is_empty());
    let mut wrong = Vec::new();
    let mut named = 0;
    for case in &pushes {
        let name = case["name"].as_str().unwrap();
        let got = match arcade_reorg_marker(case["body"]["extraInfo"].as_str()) {
            Some(ArcadeReorgMarker::Reanchor) => "reanchor",
            Some(ArcadeReorgMarker::Unmined) => "unmined",
            None => "none",
        };
        let want = case["expect"]["engine_marker"].as_str().unwrap_or("none");
        if want != "none" {
            named += 1;
        }
        if got != want {
            wrong.push(format!("{name}: marker expected {want}, got {got}"));
        }
    }
    assert_eq!(named, 2, "the vector carries the two reorg payloads");
    assert!(
        wrong.is_empty(),
        "{} of {} recorded Arcade push bodies read the wrong reorg marker:\n  {}",
        wrong.len(),
        pushes.len(),
        wrong.join("\n  ")
    );
}

/// The latch sequence's middle and last bodies carry the markers too.
#[test]
fn the_latch_sequence_carries_unmined_then_reanchor() {
    let v: serde_json::Value = serde_json::from_str(VECTOR).unwrap();
    let steps = v["sequences"][0]["steps"].as_array().unwrap();
    let markers: Vec<Option<ArcadeReorgMarker>> = steps
        .iter()
        .map(|s| arcade_reorg_marker(s["body"]["extraInfo"].as_str()))
        .collect();
    assert_eq!(
        markers,
        vec![
            None,
            Some(ArcadeReorgMarker::Unmined),
            Some(ArcadeReorgMarker::Reanchor)
        ]
    );
}
