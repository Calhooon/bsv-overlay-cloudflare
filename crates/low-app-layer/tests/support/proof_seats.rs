//! bsv-low #1154 (lane PROOF-SEATS): the public leaderboard never lists the configured harness identities, pinned
//! through the SHIPPED dispatcher (`leaderboard_page_body_hidden`, the production `*D1` row mappers) over rusqlite
//! with the production migrations, on both reads: the #674 board tables and #642's candidates.
use super::leaderboard_642::{fixture, run, SqliteBoard};
use super::*;
use rusqlite::Connection;
use std::collections::HashMap;

const NOW: i64 = 1_790_000_000;

/// The fixture's identity `i` (`tests/support/d1_board.rs`).
fn identity(i: u32) -> String {
    format!("02{:064x}", 0xb0a4_0000u64 + u64::from(i))
}

#[derive(Clone, Copy, Debug)]
enum Read {
    /// The shipped dispatcher on a stamped database: the #674 table read.
    Tables,
    /// #642's candidate read (the dispatcher on an unstamped database reads it too).
    Candidates,
}

fn body(
    db: &Connection,
    read: Read,
    page: usize,
    start: &crate::logic::PageStart,
    hidden: &[String],
) -> (serde_json::Value, String, Vec<String>) {
    let board = SqliteBoard::new(db);
    let (status, body) = match read {
        Read::Tables => run(leaderboard_page_body_hidden(
            &board,
            page,
            start,
            None,
            crate::logic::LEADERBOARD_CANDIDATE_WINS,
            NOW,
            hidden,
        )),
        Read::Candidates => run(leaderboard_candidate_body_hidden(
            &board,
            page,
            start,
            None,
            crate::logic::LEADERBOARD_CANDIDATE_WINS,
            NOW,
            hidden,
        )),
    };
    assert_eq!(status, 200, "{body}");
    let log: Vec<String> = board.log.into_inner().into_iter().map(|(s, _)| s).collect();
    if let Read::Tables = read {
        assert!(
            log.iter().any(|s| s.contains("FROM board_owners")),
            "the table read served it"
        );
    }
    (serde_json::from_str(&body).unwrap(), body, log)
}

fn owners(v: &serde_json::Value) -> Vec<String> {
    v["board"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["identity"].as_str().unwrap().to_string())
        .collect()
}

fn hand_winners(v: &serde_json::Value) -> Vec<String> {
    v["hands"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["winner"].as_str().unwrap().to_ascii_lowercase())
        .collect()
}

fn rows_by_owner(v: &serde_json::Value) -> HashMap<String, serde_json::Value> {
    v["board"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| (r["identity"].as_str().unwrap().to_string(), r.clone()))
        .collect()
}

/// Every page of the walk from the first, by `nextAfter` (offset) or by `nextCursor` (keyset).
fn walk(
    db: &Connection,
    read: Read,
    page: usize,
    by_cursor: bool,
    hidden: &[String],
) -> Vec<serde_json::Value> {
    let mut start = crate::logic::PageStart::Offset(0);
    let mut pages = Vec::new();
    loop {
        let (v, _, _) = body(db, read, page, &start, hidden);
        let next = if by_cursor {
            v.get("nextCursor")
                .and_then(|c| c.as_str())
                .map(|c| crate::logic::parse_leaderboard_cursor(c).unwrap())
        } else {
            v["nextAfter"]
                .as_u64()
                .map(|a| crate::logic::PageStart::Offset(a as usize))
        };
        pages.push(v);
        match next {
            Some(n) => start = n,
            None => break,
        }
        assert!(pages.len() < 400, "the walk ends");
    }
    pages
}

/// bsv-low #1154 pin 2: three winners, one hidden. The hidden identity has no row, no hand of its own is listed,
/// `candidates` and `resultCount` count what is listed, the other two rows and their order are unchanged, and a
/// hand whose winner is listed and whose loser is hidden stays. RED with the filter inert: the hidden winner is
/// listed.
#[test]
fn proof_seats_a_three_winners_one_hidden() {
    let db = fixture(3);
    // Games 1, 2 and 3 are won by identities 1, 2 and 3; identity 3 lost game 1 (`d1_board::rows`).
    let hidden = vec![identity(3)];
    for read in [Read::Tables, Read::Candidates] {
        let start = crate::logic::PageStart::Offset(0);
        let (all, _, _) = body(&db, read, 50, &start, &[]);
        assert_eq!(
            {
                let mut o = owners(&all);
                o.sort();
                o
            },
            vec![identity(1), identity(2), identity(3)],
            "{read:?}: the premise, three winners"
        );
        assert!(
            hand_winners(&all).contains(&identity(3)),
            "{read:?}: the premise, the hidden identity holds a hand"
        );
        let (v, _, _) = body(&db, read, 50, &start, &hidden);
        assert!(
            !owners(&v).contains(&identity(3)),
            "{read:?}: the hidden winner is listed: {:?}",
            owners(&v)
        );
        assert_eq!(
            owners(&v),
            owners(&all)
                .into_iter()
                .filter(|o| *o != identity(3))
                .collect::<Vec<_>>(),
            "{read:?}: the other rows, in their order"
        );
        let full = rows_by_owner(&all);
        for (o, row) in rows_by_owner(&v) {
            assert_eq!(row, full[&o], "{read:?}: {o}'s row unchanged");
        }
        assert!(
            !hand_winners(&v).contains(&identity(3)),
            "{read:?}: a hand of the hidden winner is listed"
        );
        let kept: Vec<&serde_json::Value> = all["hands"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|h| h["winner"].as_str().unwrap().to_ascii_lowercase() != identity(3))
            .collect();
        assert_eq!(
            v["hands"].as_array().unwrap().iter().collect::<Vec<_>>(),
            kept,
            "{read:?}: every other hand, in order"
        );
        assert!(
            v["hands"]
                .as_array()
                .unwrap()
                .iter()
                .any(|h| h["loser"].as_str().map(str::to_ascii_lowercase) == Some(identity(3))),
            "{read:?}: the hand won against the hidden identity stays"
        );
        assert_eq!(
            v["candidates"].as_u64().unwrap_or(0) + 1,
            all["candidates"].as_u64().unwrap_or(1),
            "{read:?}: candidates count the listed wins"
        );
        assert!(
            v["resultCount"].as_u64() < all["resultCount"].as_u64(),
            "{read:?}: resultCount counts the listed rows' markers"
        );
    }
}

/// bsv-low #1154 pin 2, paging: over the fleet-shaped board, hide the top owner and the owners either side of a page
/// boundary; the offset walk and the cursor walk, on both reads, list every listed owner exactly once in the
/// unhidden order, each row as the unhidden walk serves it, no hidden hand, and `candidates` the listed wins.
#[test]
fn proof_seats_b_paging_steps_over_a_hidden_owner_exactly_once() {
    let db = fixture(600);
    let page = 7;
    let unhidden = walk(&db, Read::Tables, page, false, &[]);
    let order: Vec<String> = unhidden.iter().flat_map(owners).collect();
    assert!(order.len() > 3 * page, "a walk of several pages");
    // The top owner, and the last of page 1 with the first of page 2 (the boundary a hidden row would straddle).
    let hidden: Vec<String> = vec![
        order[0].clone(),
        order[page - 1].clone(),
        order[page].clone(),
    ];
    let expected: Vec<String> = order
        .iter()
        .filter(|o| !hidden.contains(o))
        .cloned()
        .collect();
    let full: HashMap<String, serde_json::Value> =
        unhidden.iter().flat_map(rows_by_owner).collect();
    let hidden_wins: u64 = hidden
        .iter()
        .map(|o| full[o]["wins"].as_u64().unwrap())
        .sum();
    for read in [Read::Tables, Read::Candidates] {
        let (first_all, _, _) = body(&db, read, page, &crate::logic::PageStart::Offset(0), &[]);
        for by_cursor in [false, true] {
            let pages = walk(&db, read, page, by_cursor, &hidden);
            let listed: Vec<String> = pages.iter().flat_map(owners).collect();
            assert_eq!(
                listed, expected,
                "{read:?} cursor={by_cursor}: every listed owner exactly once, in order"
            );
            for p in &pages {
                for (o, row) in rows_by_owner(p) {
                    assert_eq!(row, full[&o], "{read:?} cursor={by_cursor}: {o}'s row");
                }
                for w in hand_winners(p) {
                    assert!(!hidden.contains(&w), "{read:?}: a hidden hand {w}");
                }
                // The table read says it ranked every listed win; the candidate read under its bound says nothing.
                match first_all.get("candidates").and_then(|c| c.as_u64()) {
                    Some(all) => assert_eq!(
                        p["candidates"].as_u64().unwrap() + hidden_wins,
                        all,
                        "{read:?}: candidates count the listed wins"
                    ),
                    None => assert!(p.get("candidates").is_none(), "{read:?}"),
                }
            }
            // Every page but the last is full: the hidden rows left no hole.
            for p in &pages[..pages.len() - 1] {
                assert_eq!(p["board"].as_array().unwrap().len(), page);
                assert_eq!(p["truncated"], serde_json::json!(true));
            }
        }
    }
}

/// bsv-low #1154 pin 2, past #642's bound: the candidate read of the last 200 games, the top owner hidden, still
/// says it is bounded, and `candidates` counts the listed wins of the window.
#[test]
fn proof_seats_b2_a_bounded_window_counts_its_listed_wins() {
    let db = fixture(600);
    let cap = 200;
    let read = |hidden: &[String]| {
        let board = SqliteBoard::new(&db);
        let (status, body) = run(leaderboard_candidate_body_hidden(
            &board,
            50,
            &crate::logic::PageStart::Offset(0),
            None,
            cap,
            NOW,
            hidden,
        ));
        assert_eq!(status, 200);
        serde_json::from_str::<serde_json::Value>(&body).unwrap()
    };
    let all = read(&[]);
    assert_eq!(all["candidatesBounded"], serde_json::json!(true));
    assert_eq!(all["candidates"], serde_json::json!(cap));
    let top = all["board"][0]["identity"].as_str().unwrap().to_string();
    let top_wins = all["board"][0]["wins"].as_u64().unwrap();
    let v = read(std::slice::from_ref(&top));
    assert!(!owners(&v).contains(&top));
    assert_eq!(v["candidatesBounded"], serde_json::json!(true));
    assert_eq!(v["candidates"].as_u64().unwrap(), cap as u64 - top_wins);
    // The page is still full, and its first 49 are the unhidden page past the hidden owner, in order.
    let listed = owners(&v);
    assert_eq!(listed.len(), 50);
    assert_eq!(
        listed[..49].to_vec(),
        owners(&all)
            .into_iter()
            .filter(|o| *o != top)
            .collect::<Vec<_>>()
    );
}

/// bsv-low #1154 pin 3: unset and empty hide nobody: the body is today's byte for byte on the fleet-shaped board,
/// on both reads and through the cursor; and a configured key that owns nothing (every statement then the
/// filtered one) serves the same bytes.
#[test]
fn proof_seats_c_unset_and_empty_are_todays_bytes() {
    let db = fixture(1_500);
    let none = crate::public_hidden::parse(None).keys;
    let empty = crate::public_hidden::parse(Some(" , ")).keys;
    assert!(none.is_empty() && empty.is_empty());
    let stranger = crate::public_hidden::parse(Some(&format!("02{}", "ab".repeat(32)))).keys;
    assert_eq!(stranger.len(), 1);
    for read in [Read::Tables, Read::Candidates] {
        for page in [crate::logic::LEADERBOARD_PAGE_DEFAULT, 7] {
            let mut starts = vec![
                crate::logic::PageStart::Offset(0),
                crate::logic::PageStart::Offset(51),
            ];
            let (first, _, _) = body(&db, read, page, &starts[0], &[]);
            if let Some(c) = first.get("nextCursor").and_then(|c| c.as_str()) {
                starts.push(crate::logic::parse_leaderboard_cursor(c).unwrap());
            }
            for start in &starts {
                let board = SqliteBoard::new(&db);
                let (_, today) = run(match read {
                    Read::Tables => leaderboard_page_body(
                        &board,
                        page,
                        start,
                        None,
                        crate::logic::LEADERBOARD_CANDIDATE_WINS,
                        NOW,
                    )
                    .await_boxed(),
                    Read::Candidates => leaderboard_candidate_body(
                        &board,
                        page,
                        start,
                        None,
                        crate::logic::LEADERBOARD_CANDIDATE_WINS,
                        NOW,
                    )
                    .await_boxed(),
                });
                for (what, hidden) in [("unset", &none), ("empty", &empty)] {
                    let (_, b, log) = body(&db, read, page, start, hidden);
                    assert_eq!(b, today, "{read:?} {what} page {page} {start:?}");
                    assert!(
                        !log.iter().any(|s| s.contains("owner <> '")),
                        "{read:?} {what}: no filtered statement"
                    );
                }
                let (_, b, log) = body(&db, read, page, start, &stranger);
                assert_eq!(
                    b, today,
                    "{read:?} a key owning nothing, page {page} {start:?}"
                );
                if let Read::Tables = read {
                    assert!(
                        log.iter().any(|s| s.contains("owner <> '")),
                        "the filtered statements ran"
                    );
                }
            }
        }
    }
}

/// Box a body future so the two reads' futures are one type.
trait AwaitBoxed<'a>: std::future::Future<Output = (u16, String)> + 'a + Sized {
    fn await_boxed(
        self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = (u16, String)> + 'a>> {
        Box::pin(self)
    }
}
impl<'a, F: std::future::Future<Output = (u16, String)> + 'a> AwaitBoxed<'a> for F {}

/// bsv-low #1154 pin 5: the var's reach is the public board alone. The parse is read by the leaderboard's
/// compute and by `/health` (its count), and by no identity-keyed view (`/results`, `/owed`, `/hops-view`,
/// `/refund-view`, `/live-view`, `/proof`): those answer for a hidden identity exactly as for any other.
#[test]
fn proof_seats_d_only_the_public_board_reads_the_var() {
    let mut readers = Vec::new();
    for (file, src) in [
        ("routes.rs", include_str!("../../src/routes.rs")),
        ("results.rs", include_str!("../../src/results.rs")),
        ("owed_read.rs", include_str!("../../src/owed_read.rs")),
        ("owed_judge.rs", include_str!("../../src/owed_judge.rs")),
        ("hops_view.rs", include_str!("../../src/hops_view.rs")),
        ("refund_view.rs", include_str!("../../src/refund_view.rs")),
        ("live_view.rs", include_str!("../../src/live_view.rs")),
        ("proof_post.rs", include_str!("../../src/proof_post.rs")),
        ("board_view.rs", include_str!("../../src/board_view.rs")),
        ("lib.rs", include_str!("../../src/lib.rs")),
    ] {
        let prod = src;
        let mut fn_name = String::new();
        for line in prod.lines() {
            let t = line.trim_start();
            if let Some(rest) = t
                .strip_prefix("pub async fn ")
                .or_else(|| t.strip_prefix("pub fn "))
                .or_else(|| t.strip_prefix("async fn "))
                .or_else(|| t.strip_prefix("fn "))
                .or_else(|| t.strip_prefix("pub(crate) async fn "))
                .or_else(|| t.strip_prefix("pub(crate) fn "))
            {
                fn_name = rest.split(['<', '(']).next().unwrap_or("").to_string();
            }
            if line.contains("public_hidden::") || line.contains("PUBLIC_HIDDEN_IDENTITIES") {
                readers.push(format!("{file}:{fn_name}"));
            }
        }
    }
    readers.sort();
    readers.dedup();
    assert_eq!(
        readers,
        vec![
            "routes.rs:compute_leaderboard_body_string".to_string(),
            "routes.rs:health".to_string(),
        ],
        "the var's readers"
    );
}
