//! The COURIER-RUNG lens fold (lane COURIER-RUNG-FOLD, bsv-low #868, the lens's M1 at the overlay): BananaBlocks'
//! 404 is a LOOK only when its body is BananaBlocks' own recorded not-found (`{"error":"Transaction not found"}`);
//! any other 404 (another JSON error, plain text, an empty body, a balancer's page) is a FAULT, counted under the
//! provider's `fault` row and never its `notfound` row, and never a vote toward an absence.
use super::*;
use crate::admit_fast::{ConflictPresence, CourierConflictPresence};
use crate::courier_counters::{Counters as CourierCounters, Reader};

const TXID: &str = "e3340463baaceb6943dec5d82f612a247d2d1227f1b64f6e5c1438e5ff4e8d58";
const NOT_FOUND: &str = r#"{"error":"Transaction not found"}"#;

fn answer(url: String, status: u16, body: &str) {
    TEST_HTTP_RESPONSES.with(|r| r.borrow_mut().insert(url, (status, body.to_string())));
}

fn couriers(banana: (u16, &str)) {
    crate::courier_counters::clear_cooldowns();
    TEST_HTTP_RESPONSES.with(|r| r.borrow_mut().clear());
    answer(format!("{DEFAULT_BITAILS_BASE}/tx/{TXID}"), 404, "");
    answer(format!("{DEFAULT_WOC_BASE}/tx/hash/{TXID}"), 404, "");
    answer(
        format!("{DEFAULT_BANANABLOCKS_BASE}/tx/{TXID}"),
        banana.0,
        banana.1,
    );
}

async fn presence(counters: &CourierCounters) -> Option<NetworkPresence> {
    CourierConflictPresence {
        sources: ProofSources::parse("arcade,bananablocks,woc,bitails"),
        woc_api_key: None,
        counters: Some(counters.clone()),
    }
    .presence(TXID)
    .await
}

/// RED on `f5d2d700`: every non-standard 404 was counted `notfound` (`left: (None, Some(1))`), the row the fleet cell
/// reads as a clean look.
#[tokio::test]
async fn c868f_m1_a_non_standard_bananablocks_404_is_a_fault_never_a_look() {
    for body in [
        r#"{"error":"transaction not found"}"#,
        r#"{"error":"Not Found"}"#,
        "Transaction not found",
        "not found",
        "",
        "<html><body>404</body></html>",
    ] {
        couriers((404, body));
        let janitor = CourierCounters::new(Reader::Janitor);
        assert_eq!(
            presence(&janitor).await,
            Some(NetworkPresence::Inconclusive),
            "{body:?}: no absence"
        );
        let rows = janitor.take();
        assert_eq!(
            (
                rows.get("courier_janitor_bananablocks_fault_total")
                    .copied(),
                rows.get("courier_janitor_bananablocks_notfound_total")
                    .copied()
            ),
            (Some(1), None),
            "{body:?}: {rows:?}"
        );
    }
    // the recorded not-found is BananaBlocks' clean look, counted `notfound`, and with WoC's 404 the absence
    couriers((404, NOT_FOUND));
    let janitor = CourierCounters::new(Reader::Janitor);
    assert_eq!(presence(&janitor).await, Some(NetworkPresence::Absent));
    let rows = janitor.take();
    assert_eq!(
        (
            rows.get("courier_janitor_bananablocks_fault_total")
                .copied(),
            rows.get("courier_janitor_bananablocks_notfound_total")
                .copied()
        ),
        (None, Some(1)),
        "{rows:?}"
    );
    TEST_HTTP_RESPONSES.with(|r| r.borrow_mut().clear());
}
