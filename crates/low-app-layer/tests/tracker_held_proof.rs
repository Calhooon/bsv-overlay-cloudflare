#[test]
fn ns3_l3_held_proof_reads_bounded_bytes() {
    let src = include_str!("../src/tracker_host/runtime.rs");
    let held = &src[src.find("struct BeefRow").unwrap()..src.find("pub async fn event(").unwrap()];
    assert!(
        !held.contains("hex(beef)"),
        "held proof still doubles BEEF through hex"
    );
    // PORT-0811: the engine's bounded door, reading the subject alone
    // (`beef_limits::subject_from_beef` parses through the same door as
    // `transaction_from_beef` and links nothing, so no link bound applies).
    assert!(
        held.contains("beef_limits::subject_from_beef"),
        "held proof still uses unbounded SDK parser"
    );
    assert!(held.contains("STORED_BEEF_LIMITS"));
}
