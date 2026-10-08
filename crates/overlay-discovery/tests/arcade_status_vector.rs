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
//! `engine_proof` columns are the overlay-cloudflare crate's readings; that
//! crate builds only beside the bsv-low sources (`crates/low-proof-replay/
//! Cargo.toml:21-22`) and is replayed where that build runs.

use bsv_overlay_discovery::pot::reorg::{arcade_reorg_marker, ArcadeReorgMarker};

const VECTOR: &str = include_str!("../vectors/arcade_status_verdicts.json");

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
