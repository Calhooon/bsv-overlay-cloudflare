#[test]
fn ns3_l3_held_proof_reads_bounded_bytes() {
    let src = include_str!("../src/tracker_host/runtime.rs");
    let held = &src[src.find("struct BeefRow").unwrap()..src.find("pub async fn event(").unwrap()];
    assert!(
        !held.contains("hex(beef)"),
        "held proof still doubles BEEF through hex"
    );
    assert!(
        held.contains("transaction_from_beef"),
        "held proof still uses unbounded SDK parser"
    );
    assert!(held.contains("STORED_BEEF_LIMITS"));
}
