//! bsv-low #625 (the observability bar, issue 1, the app layer's half): the per-reader courier outcomes, made by
//! the census's producer, read back from its batched flush (`courier::take_pending_if_due`). Each `#[test]` runs
//! on its own thread, so the isolate's pending map starts empty.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeMap;

const NOW: f64 = 1_000_000.0;

/// One courier case: the URL, the answer, the `applayer_courier_*` outcome, the per-reader outcome.
type Case<'a> = (String, Option<(u16, &'a [u8])>, &'a str, &'a str);

/// The deltas one flush would write (the head is exact, so the first takes always flush).
fn drained() -> BTreeMap<String, u64> {
    crate::courier::take_pending_if_due(NOW)
        .into_iter()
        .collect()
}

#[test]
fn obs_625_the_606_discriminator_is_counted_per_reader_and_provider() {
    use crate::courier::record;
    let txid = "ab".repeat(32);
    let mempool = format!(r#"{{"txid":"{txid}","hex":"0100","vin":[],"vout":[],"size":10}}"#);
    let cases: [Case; 9] = [
        // A Bitails 404 on an unmined subject (Bitails is pruned): notfound, never fault.
        (
            format!("https://api.bitails.io/download/tx/{txid}"),
            Some((404, b"")),
            "notfound",
            "notfound",
        ),
        (
            format!("https://api.bitails.io/tx/{txid}"),
            Some((404, b"{}")),
            "notfound",
            "notfound",
        ),
        // A WoC mempool body has no `confirmations`: a well-formed answer, ok, never shape.
        (
            format!("https://api.whatsonchain.com/v1/bsv/main/tx/hash/{txid}"),
            Some((200, mempool.as_bytes())),
            "ok",
            "ok",
        ),
        // A 200 whose body is not its route's format: shape, apart from a transport fault.
        (
            format!("https://api.whatsonchain.com/v1/bsv/main/tx/hash/{txid}"),
            Some((200, b"<html>busy</html>")),
            "ok",
            "shape",
        ),
        (
            format!("https://api.whatsonchain.com/v1/bsv/main/tx/{txid}/hex"),
            Some((200, b"zz-not-hex")),
            "ok",
            "shape",
        ),
        (
            format!("https://api.whatsonchain.com/v1/bsv/main/tx/{txid}/hex"),
            Some((200, b"0100beef")),
            "ok",
            "ok",
        ),
        (
            format!("https://api.whatsonchain.com/v1/bsv/main/tx/hash/{txid}"),
            None,
            "fault",
            "fault",
        ),
        (
            format!("https://api.whatsonchain.com/v1/bsv/main/tx/hash/{txid}"),
            Some((429, b"")),
            "ratelimited",
            "ratelimited",
        ),
        (
            format!("https://bananablocks.com/api/v1/txo/{txid}/0/spend"),
            Some((503, b"")),
            "fault",
            "fault",
        ),
    ];
    for (url, answer, outcome, shaped) in &cases {
        let (_, o, s) = record(url, "tx_any", *answer);
        assert_eq!((o, s), (*outcome, *shaped), "{url} {answer:?}");
    }
    let d = drained();
    for (name, want) in [
        ("courier_applayer_bitails_notfound_total", 2),
        ("courier_applayer_woc_ok_total", 2),
        ("courier_applayer_woc_shape_total", 2),
        ("courier_applayer_woc_fault_total", 1),
        ("courier_applayer_woc_ratelimited_total", 1),
        ("courier_applayer_bananablocks_fault_total", 1),
        // the pre-existing family is unchanged: a shape fault is still its `ok` (2xx)
        ("applayer_courier_woc_ok_total", 4),
        ("applayer_courier_bitails_notfound_total", 2),
        ("applayer_courier_calls_tx_any_total", 9),
    ] {
        assert_eq!(d.get(name).copied(), Some(want), "{name} in {d:?}");
    }
    assert!(
        !d.contains_key("courier_applayer_bitails_fault_total"),
        "a Bitails 404 is never a fault"
    );
}
