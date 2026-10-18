//! bsv-low #674: the leaderboard read from the tables the overlay derives at the write, pinned through the SHIPPED
//! dispatcher (`leaderboard_page_body`, the production `*D1` row mappers) over rusqlite with the production
//! migrations, as a third reader beside #642's candidate read and the whole-board read it replaced.
use super::leaderboard_642::{
    fill, fixture, new_body, old_body, run, schema_under, served_body, without_642_fields,
    SqliteBoard,
};
use super::*;
use rusqlite::Connection;

const NOW: i64 = 1_790_000_000;

fn served(body: &str) -> serde_json::Value {
    serde_json::from_str(body).unwrap()
}

/// The body without the counting fields (`candidates`, `candidatesBounded`), which say WHICH read ranked it.
fn without_count(body: &str) -> String {
    let mut v = served(body);
    let m = v.as_object_mut().unwrap();
    m.remove("candidates");
    m.remove("candidatesBounded");
    v.to_string()
}

fn used_the_tables(log: &[(String, Vec<serde_json::Value>)]) -> bool {
    log.iter().any(|(sql, _)| sql.contains("FROM board_owners"))
        && !log.iter().any(|(sql, _)| sql.contains("WITH recent AS"))
}

fn counted(db: &Connection) -> usize {
    let cte = crate::logic::chain_wins_cte("?1", None);
    db.query_row(&format!("{cte} SELECT COUNT(*) FROM owned"), [], |r| {
        r.get::<_, i64>(0)
    })
    .unwrap() as usize
}

/// Inside #642's bound, the table read is #642's body byte for byte (the cursor and the #685 decoration
/// included), and the whole-board read's; it says it ranked the whole era.
#[test]
fn lb674_the_table_read_is_byte_identical_to_the_candidate_and_whole_board_reads() {
    let db = fixture(1_500);
    let total = counted(&db);
    let mut checked_decoration = false;
    for page in [
        crate::logic::LEADERBOARD_PAGE_DEFAULT,
        7,
        crate::logic::LEADERBOARD_PAGE_MAX,
    ] {
        for after in [0usize, 7, 51, 140, 400] {
            let start = crate::logic::PageStart::Offset(after);
            let (table, log) = served_body(&db, page, &start);
            assert!(
                used_the_tables(&log),
                "page {page} after {after}: the table read served it"
            );
            let (cands, _) = new_body(&db, page, &start, crate::logic::LEADERBOARD_CANDIDATE_WINS);
            assert_eq!(
                without_count(&table),
                without_count(&cands),
                "page {page} after {after}: the table read is the candidate read's body"
            );
            let (old, _) = old_body(&db, page, after);
            assert_eq!(
                without_642_fields(&table),
                old,
                "page {page} after {after}: and the whole-board read's"
            );
            let v = served(&table);
            assert_eq!(v["candidatesBounded"], serde_json::json!(false));
            assert_eq!(
                v["candidates"],
                serde_json::json!(total),
                "every counted win"
            );
            if v["board"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|r| r["evidence"].as_array().unwrap())
                .any(|e| e["proofPosted"] == serde_json::json!(true))
            {
                checked_decoration = true;
            }
        }
    }
    assert!(
        checked_decoration,
        "the #685 decoration is part of the compared bytes"
    );
}

/// The keyset cursor over the table's rank index is the candidate read's cursor walk, page by page, `nextAfter`
/// included.
#[test]
fn lb674_the_cursor_walk_over_the_rank_index_is_the_candidate_walk() {
    let db = fixture(1_500);
    let page = 40;
    let mut start = crate::logic::PageStart::Offset(0);
    let mut pages = 0;
    loop {
        let (table, log) = served_body(&db, page, &start);
        assert!(used_the_tables(&log));
        let (cands, _) = new_body(&db, page, &start, crate::logic::LEADERBOARD_CANDIDATE_WINS);
        assert_eq!(without_count(&table), without_count(&cands), "page {pages}");
        pages += 1;
        match served(&table).get("nextCursor").and_then(|c| c.as_str()) {
            Some(c) => start = crate::logic::parse_leaderboard_cursor(c).unwrap(),
            None => break,
        }
    }
    assert!(pages > 3, "a walk of several pages: {pages}");
}

/// Past #642's bound the table read is still the WHOLE board: at 3,000 counted wins the candidate read ranks the
/// last 2,000 and says so; the table read is the whole-board read's body, byte for byte.
#[test]
fn lb674_past_the_candidate_bound_the_table_read_is_the_whole_board() {
    let db = fixture(3_000);
    let total = counted(&db);
    assert!(total > crate::logic::LEADERBOARD_CANDIDATE_WINS);
    for after in [0usize, 51] {
        let start = crate::logic::PageStart::Offset(after);
        let (table, log) = served_body(&db, 50, &start);
        assert!(used_the_tables(&log));
        let (old, _) = old_body(&db, 50, after);
        assert_eq!(without_642_fields(&table), old, "after {after}");
        let v = served(&table);
        assert_eq!(v["candidatesBounded"], serde_json::json!(false));
        assert_eq!(v["candidates"], serde_json::json!(total));
        let (cands, _) = new_body(&db, 50, &start, crate::logic::LEADERBOARD_CANDIDATE_WINS);
        assert_eq!(served(&cands)["candidatesBounded"], serde_json::json!(true));
        if after == 0 {
            assert_ne!(
                without_count(&table),
                without_count(&cands),
                "the bounded candidate read is NOT the whole board here (the bound this lane removes)"
            );
        }
    }
}

/// The tables are served only when the overlay stamped them and no era cutoff is set; otherwise the route reads
/// the candidates (an unmigrated database never answers 503 for it).
#[test]
fn lb674_the_route_reads_the_candidates_until_the_tables_are_stamped() {
    let db = fixture(200);
    let start = crate::logic::PageStart::Offset(0);
    let (table, log) = served_body(&db, 50, &start);
    assert!(used_the_tables(&log));
    db.execute(
        "DELETE FROM view_derivation_state WHERE name = 'board-v1'",
        [],
    )
    .unwrap();
    let (unstamped, log) = served_body(&db, 50, &start);
    assert!(!used_the_tables(&log), "no stamp: the candidate read");
    assert!(served(&unstamped).get("candidatesBounded").is_none());
    assert_eq!(without_count(&unstamped), without_count(&table));
    db.execute_batch(
        "INSERT INTO view_derivation_state (name) VALUES ('board-v1'); DROP TABLE board_total;",
    )
    .unwrap();
    let (unmigrated, log) = served_body(&db, 50, &start);
    assert!(
        !used_the_tables(&log),
        "a table missing: the candidate read"
    );
    assert_eq!(without_count(&unmigrated), without_count(&table));
    let board = SqliteBoard::new(&db);
    let (status, _) = run(leaderboard_page_body(
        &board,
        50,
        &start,
        Some(1),
        crate::logic::LEADERBOARD_CANDIDATE_WINS,
        NOW,
    ));
    assert_eq!(status, 200);
    assert!(
        !used_the_tables(&board.log.into_inner()),
        "an era cutoff: the candidate read"
    );
}

/// The tables a page read may name for its OWN page (the hands pots beyond the page, at most the page's hands, are
/// read by pot): the per-win sources the #674 lens's MEDIUM-1 found the page joined, once per page-owner win.
fn per_win_reads(log: &[(String, Vec<serde_json::Value>)]) -> Vec<String> {
    log.iter()
        .filter(|(sql, _)| {
            sql.contains("FROM board_wins WHERE owner IN")
                || sql.contains("FROM result_markers_v2 WHERE potTxid IN")
                || sql.contains("FROM proof_markers WHERE")
                || sql.contains("FROM proof_posts WHERE")
        })
        .map(|(sql, rows)| format!("{} rows: {}", rows.len(), &sql[..sql.len().min(70)]))
        .collect()
}

/// bsv-low #674 fold, MEDIUM-1: the page read is FLAT in its owners' lifetime wins. The default page of the
/// 1,500 and 3,000 win boards (its 50 owners hold 945 and 1,843 counted wins) runs the same statements and reads
/// the same rows: the owner page, the page owners' cards (one `board_owners` row each), the hands and the hands
/// pots beyond the page; never the page owners' wins, markers, proof pointers or posted bundles. RED on `eaf122e`:
/// 113 statements and 213, the per-win reads every page.
#[test]
fn lb674_fold_m1_the_page_read_is_flat_in_the_page_owners_wins() {
    let mut seen = Vec::new();
    for wins in [1_500u32, 3_000] {
        let db = fixture(wins);
        let (body, log) = served_body(&db, 50, &crate::logic::PageStart::Offset(0));
        assert!(used_the_tables(&log));
        let owner_wins: u64 = served(&body)["board"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["wins"].as_u64().unwrap())
            .sum();
        let per_win = per_win_reads(&log);
        let rows: usize = log.iter().map(|(_, r)| r.len()).sum();
        println!(
            "{wins} wins: the page's owners hold {owner_wins}; {} statements, {rows} rows returned; per-win reads {per_win:?}",
            log.len()
        );
        assert!(
            per_win.is_empty(),
            "{wins} wins: the page joined its owners' per-win rows: {per_win:?}"
        );
        seen.push((owner_wins, log.len(), rows));
    }
    assert!(
        seen[1].0 > seen[0].0 * 3 / 2,
        "the 3,000 win page's owners hold more wins: {seen:?}"
    );
    assert_eq!(
        (seen[0].1, seen[0].2),
        (seen[1].1, seen[1].2),
        "the same statements and rows whatever the page owners' wins: {seen:?}"
    );
}

/// The card's bound as the shipped migrations write it (`derived_board.rs` `card_pack!`).
const CARD_BOUND_SQL: &str = "<= 1500000 END;";

/// The fixture's board with the card's bound cut to `cap` characters in every migration that packs a card.
fn fixture_under_card_bound(wins: u32, cap: usize) -> Connection {
    let cut = format!("<= {cap} END;");
    let packs = bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS
        .iter()
        .filter(|sql| sql.contains(CARD_BOUND_SQL))
        .count();
    assert!(
        packs > 0,
        "the shipped migrations bound the card at {CARD_BOUND_SQL:?} (a changed bound is this pin's to follow)"
    );
    fill(schema_under(&|sql| sql.replace(CARD_BOUND_SQL, &cut)), wins)
}

/// The page owners whose card does not hold every win, and those wins.
fn unpacked_page_owners(db: &Connection, body: &str) -> (usize, usize, u64) {
    let mut unpacked = 0;
    let mut unpacked_wins = 0;
    let board = served(body)["board"].as_array().unwrap().clone();
    for r in &board {
        let (wins, packed): (i64, i64) = db
            .query_row(
                "SELECT wins, packed FROM board_owners WHERE owner = ?1",
                [r["identity"].as_str().unwrap()],
                |q| Ok((q.get(0)?, q.get(1)?)),
            )
            .unwrap();
        if packed < wins {
            unpacked += 1;
            unpacked_wins += wins as u64;
        }
    }
    (board.len(), unpacked, unpacked_wins)
}

fn fallback_reads(log: &[(String, Vec<serde_json::Value>)]) -> Vec<usize> {
    log.iter()
        .filter(|(sql, _)| sql.contains("FROM board_wins WHERE owner IN"))
        .map(|(_, rows)| rows.len())
        .collect()
}

/// bsv-low #674, the second lens's LOW-A: an owner whose card would pass the 1,500,000 character bound is read by
/// its wins (`BoardCardRowD1::entries` answers `None`, `board_wins_owners_sql`, steps 3 to 6 from D1), merged with
/// the carded owners' wins in the owners query's order. On the 1,500 win fixture with the bound cut to 25,000 (the
/// 7 heaviest owners and those of 11 wins or a heavier card past it), every page is the SHIPPED bound's body byte
/// for byte (and #642's), a full page mixing carded and fallback owners; the fallback is one `board_wins` statement
/// a chunk returning exactly the unpacked page owners' wins, and the page reads at most 12 rows more a fallback win than the carded page. Then half the
/// top owner's pots are evicted and readmitted through `pot_records` on both boards: the same again. RED with the
/// fallback disabled (a short card served) and with the bound left above the fixture (no owner past it).
#[test]
fn lb674_fold_lowa_an_owner_past_the_card_bound_is_read_by_its_wins() {
    let shipped = fixture(1_500);
    let cut = fixture_under_card_bound(1_500, 25_000);
    let compare = |label: &str| {
        let mut fallback_wins = 0;
        for page in [
            crate::logic::LEADERBOARD_PAGE_DEFAULT,
            7,
            crate::logic::LEADERBOARD_PAGE_MAX,
        ] {
            for after in [0usize, 7, 51] {
                let start = crate::logic::PageStart::Offset(after);
                let (carded, carded_log) = served_body(&shipped, page, &start);
                let (body, log) = served_body(&cut, page, &start);
                assert!(
                    used_the_tables(&carded_log) && used_the_tables(&log),
                    "{label}: the table read"
                );
                assert!(
                    fallback_reads(&carded_log).is_empty(),
                    "{label}: under the shipped bound every card holds its wins"
                );
                let (owners, unpacked, unpacked_wins) = unpacked_page_owners(&cut, &body);
                assert!(
                    page < crate::logic::LEADERBOARD_PAGE_DEFAULT || (unpacked > 0 && unpacked < owners),
                    "{label}: page {page} after {after} mixes carded owners and owners read by their wins: \
                     {unpacked} of {owners}"
                );
                assert_eq!(
                    body, carded,
                    "{label}: page {page} after {after}: the fallback page is the carded page byte for byte"
                );
                let (cands, _) =
                    new_body(&cut, page, &start, crate::logic::LEADERBOARD_CANDIDATE_WINS);
                assert_eq!(
                    without_count(&body),
                    without_count(&cands),
                    "{label}: and #642's"
                );
                let fallback = fallback_reads(&log);
                let chunks = owners.div_ceil(crate::logic::D1_CHUNK_OUTPOINTS);
                assert!(
                    (unpacked == 0) == fallback.is_empty() && fallback.len() <= chunks,
                    "{label}: one per-win statement a chunk at most: {fallback:?}"
                );
                assert_eq!(
                    fallback.iter().sum::<usize>() as u64,
                    unpacked_wins,
                    "{label}: the per-win read returns the unpacked page owners' wins"
                );
                let rows = |l: &[(String, Vec<serde_json::Value>)]| -> u64 {
                    l.iter().map(|(_, r)| r.len() as u64).sum()
                };
                assert!(
                    rows(&log) <= rows(&carded_log) + 12 * unpacked_wins,
                    "{label}: page {page} after {after}: {} rows past the carded {} plus 12 a fallback win ({unpacked_wins})",
                    rows(&log),
                    rows(&carded_log)
                );
                fallback_wins += unpacked_wins;
            }
        }
        println!("{label}: {fallback_wins} fallback wins over the pages");
        fallback_wins
    };
    let fallback_wins = compare("cut to 25000");
    assert!(
        fallback_wins > 0,
        "the cut bound sends owners to the per-win read"
    );

    let top: String = cut
        .query_row(
            "SELECT owner FROM board_owners ORDER BY wins DESC, owner ASC LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let pots: Vec<String> = {
        let mut st = cut
            .prepare("SELECT potTxid FROM board_wins WHERE owner = ?1 ORDER BY potTxid")
            .unwrap();
        st.query_map([&top], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    };
    for db in [&shipped, &cut] {
        db.execute_batch("CREATE TEMP TABLE kept AS SELECT * FROM pot_records WHERE 0")
            .unwrap();
        for t in pots.iter().step_by(2) {
            db.execute(
                "INSERT INTO kept SELECT * FROM pot_records WHERE txid = ?1",
                [t],
            )
            .unwrap();
            db.execute("DELETE FROM pot_records WHERE txid = ?1", [t])
                .unwrap();
        }
    }
    compare("half the top owner's pots evicted");
    for db in [&shipped, &cut] {
        db.execute_batch("INSERT INTO pot_records SELECT * FROM kept")
            .unwrap();
    }
    compare("readmitted");
}
