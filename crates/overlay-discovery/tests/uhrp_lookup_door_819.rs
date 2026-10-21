//! bsv-low #819: the reference's door on `ls_uhrp`'s lookup, replayed.
//!
//! The thirty vectors of the stack's scenario
//! `corpus/scenarios/2026-10-21-overlay-lookup-unknown-key-answers-store.md`
//! (bsv-stack-lean main `d47cb15`, copied whole to
//! `fixtures/uhrp-lookup-door-819.md`, sha256 `85c29e05…abab6`), put through
//! `UHRPLookupService` over `MemoryUHRPStorage` on the scenario's Initial
//! chain, exactly as the stack's runner (`corpus/runners/uhrp-lookup/`) does:
//! 60 adverts by host key K, expiry 0; advert 1 at `ab`x32 output 1 for the
//! URL U, adverts 2 to 60 at `<i as 64 hex>.0` for the URL of thirty-two bytes
//! of value i. Each vector's verdict (refused, or answers n) must be the
//! reference's (ts-stack@9262cac66), and a refusal must carry the reference's
//! sentence. On c6b5bff69 (the door's base) the VERDICTS of 20 differ (the
//! stack runner's 10 PASS / 20 FAIL) and 28 differ by verdict or sentence.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use bsv_overlay_discovery::uhrp::lookup_service::UHRPLookupService;
use bsv_overlay_discovery::uhrp::storage::{MemoryUHRPStorage, UHRPStorage};
use bsv_rs::primitives::encoding::to_base58_check;
use overlay_engine::lookup_service::{LookupService, LookupServiceError};
use overlay_engine::types::LookupQuestion;
use std::rc::Rc;

const SCENARIO: &str = include_str!("fixtures/uhrp-lookup-door-819.md");
const U: &str = "XUSuDta9qiCVDQADh85uVy5J8Vgh5WX87BjssraN7jMEG4bEmV5m";
const N: u8 = 60;

fn url(byte: u8) -> String {
    to_base58_check(&[byte; 32], &[0xce, 0x00])
}

fn bare(cell: &str) -> &str {
    cell.trim().trim_matches('`')
}

/// The rows under `**The vectors.**`: id, step, service, query, verdict,
/// sentence (the runner's reading, a bar inside backticks the cell's).
fn vectors(text: &str) -> Vec<[String; 6]> {
    let start = text.find("**The vectors.**").expect("no vectors section");
    let mut rows = Vec::new();
    for line in text[start..].lines().skip(1) {
        let t = line.trim();
        if t.starts_with("**") {
            break;
        }
        if !t.starts_with("| V") {
            continue;
        }
        let (mut cells, mut cur, mut tick) = (Vec::new(), String::new(), false);
        for c in t.trim_matches('|').chars() {
            match c {
                '`' => {
                    tick = !tick;
                    cur.push(c);
                }
                '|' if !tick => cells.push(std::mem::take(&mut cur)),
                _ => cur.push(c),
            }
        }
        cells.push(cur);
        rows.push([0, 1, 2, 3, 4, 5].map(|i| bare(&cells[i]).to_string()));
    }
    rows
}

async fn seeded() -> UHRPLookupService {
    let storage = Rc::new(MemoryUHRPStorage::new());
    let k = format!("03{}", "22".repeat(32));
    storage
        .store_record(
            &"ab".repeat(32),
            1,
            U,
            &k,
            "https://example.invalid/1",
            0,
            1,
        )
        .await
        .unwrap();
    for i in 2..=N {
        storage
            .store_record(
                &format!("{i:064x}"),
                0,
                &url(i),
                &k,
                "https://example.invalid/i",
                0,
                1,
            )
            .await
            .unwrap();
    }
    assert_eq!(storage.record_count(), N as usize);
    UHRPLookupService::new(storage)
}

#[tokio::test]
async fn i819_the_thirty_vectors_answer_as_the_reference() {
    assert_eq!(url(1), U);
    let svc = seeded().await;
    let rows = vectors(SCENARIO);
    assert_eq!(rows.len(), 30, "the table holds 30 vectors");
    let (mut fails, mut verdicts) = (Vec::new(), 0);
    for [id, _step, service, query, verdict, sentence] in &rows {
        let q: serde_json::Value = serde_json::from_str(query).unwrap();
        let answer = svc.lookup(&LookupQuestion::new(service.clone(), q)).await;
        let refused = verdict == "refused";
        if answer.is_err() != refused
            || answer.as_ref().is_ok_and(|r| {
                r.clone()
                    .into_outputs()
                    .is_none_or(|o| *verdict != format!("answers {}", o.len()))
            })
        {
            verdicts += 1;
        }
        let ok = match (&answer, verdict.as_str()) {
            // The engine answers both as `InvalidQuery`, a 400.
            (
                Err(LookupServiceError::InvalidQuery(m) | LookupServiceError::Unsupported(m)),
                "refused",
            ) => sentence.is_empty() || m.contains(sentence.as_str()),
            (Err(_), _) => false,
            (Ok(r), v) => r
                .clone()
                .into_outputs()
                .is_some_and(|o| *v == format!("answers {}", o.len())),
        };
        if !ok {
            fails.push(format!(
                "{id}: expected {verdict} `{sentence}`, got {answer:?}"
            ));
        }
    }
    assert!(
        fails.is_empty(),
        "{} of 30 vectors differ from the reference ({verdicts} by verdict):\n{}",
        fails.len(),
        fails.join("\n")
    );
}

/// A VALID query's served bytes are the door's base's (c6b5bff69): the
/// output list of `{"uhrpUrl": U}`, the storage's references serialized.
#[tokio::test]
async fn i819_a_valid_querys_bytes_are_unchanged() {
    let svc = seeded().await;
    let answer = svc
        .lookup(&LookupQuestion::new(
            "ls_uhrp",
            serde_json::json!({"uhrpUrl": U}),
        ))
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_string(&answer.into_outputs().unwrap()).unwrap(),
        format!(r#"[{{"txid":"{}","outputIndex":1}}]"#, "ab".repeat(32))
    );
}
