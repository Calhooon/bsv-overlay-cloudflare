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

// bsv-low #674, the cutoff (lane W2-LB-674-CUTOFF): beta and prod both set `WRITTEN_OFF_BEFORE_MS`, so these are the
// pins of the read as it is served. The tables are derived under the cutoff the scheduled pass stamps into the
// overlay's `board_era` (`sync_board_era`), and the route reads them only under that same cutoff.

/// The era cutoff (ms) that keeps the fixture's counted wins from the `nth` oldest admission stamp on.
fn cutoff_at(db: &Connection, nth: usize) -> i64 {
    let cte = crate::logic::chain_wins_cte("?1", None);
    let stamps: Vec<i64> = db
        .prepare(&format!(
            "{cte} SELECT potCreatedAt FROM owned ORDER BY potCreatedAt ASC"
        ))
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    stamps[nth] * 1000
}

/// The counted wins of the era `era` (the from-scratch read).
fn counted_under(db: &Connection, era: Option<i64>) -> usize {
    let cte = crate::logic::chain_wins_cte("?1", era);
    let sql = format!("{cte} SELECT COUNT(*) FROM owned");
    match era {
        Some(ms) => db.query_row(&sql, [ms], |r| r.get::<_, i64>(0)),
        None => db.query_row(&sql, [], |r| r.get::<_, i64>(0)),
    }
    .unwrap() as usize
}

/// The scheduled pass's stamp (`sync_board_era`), over rusqlite.
fn stamp(db: &Connection, era: Option<i64>) {
    run(sync_board_era(&SqliteBoard::new(db), era)).unwrap();
}

/// The SHIPPED dispatcher under the era cutoff `era`.
fn served_under(
    db: &Connection,
    page: usize,
    start: &crate::logic::PageStart,
    era: Option<i64>,
) -> (String, Vec<(String, Vec<serde_json::Value>)>) {
    let board = SqliteBoard::new(db);
    let (status, body) = run(leaderboard_page_body(
        &board,
        page,
        start,
        era,
        crate::logic::LEADERBOARD_CANDIDATE_WINS,
        NOW,
    ));
    assert_eq!(status, 200, "{body}");
    (body, board.log.into_inner())
}

/// #642's candidate read under the era cutoff `era`.
fn candidates_under(
    db: &Connection,
    page: usize,
    start: &crate::logic::PageStart,
    era: Option<i64>,
) -> String {
    let board = SqliteBoard::new(db);
    let (status, body) = run(leaderboard_candidate_body(
        &board,
        page,
        start,
        era,
        crate::logic::LEADERBOARD_CANDIDATE_WINS,
        NOW,
    ));
    assert_eq!(status, 200, "{body}");
    body
}

/// The whole-board read (pre-#642) under the era cutoff `era`.
fn whole_under(db: &Connection, page: usize, after: usize, era: Option<i64>) -> String {
    let board = SqliteBoard::new(db);
    let (status, body) = run(super::leaderboard_642::whole_board_page_body(
        &board, page, after, era, NOW,
    ));
    assert_eq!(status, 200, "{body}");
    body
}

/// Every page shape and offset of the 1,500 win board under `era`: the table read served it, and its body is #642's
/// candidate read's and the whole-board read's under the same cutoff, byte for byte but the counting fields (which
/// say which read ranked it); it counts the era's wins. Returns whether a posted proof was among the compared bytes.
fn identical_under(db: &Connection, era: Option<i64>, label: &str) -> bool {
    let total = counted_under(db, era);
    let mut decorated = false;
    for page in [
        crate::logic::LEADERBOARD_PAGE_DEFAULT,
        7,
        crate::logic::LEADERBOARD_PAGE_MAX,
    ] {
        for after in [0usize, 7, 51, 140, 400] {
            let start = crate::logic::PageStart::Offset(after);
            let (table, log) = served_under(db, page, &start, era);
            assert!(
                used_the_tables(&log),
                "{label}: page {page} after {after}: the table read served it under the cutoff"
            );
            let cands = candidates_under(db, page, &start, era);
            assert_eq!(
                without_count(&table),
                without_count(&cands),
                "{label}: page {page} after {after}: the table read is the candidate read's body under the cutoff"
            );
            assert_eq!(
                without_642_fields(&table),
                whole_under(db, page, after, era),
                "{label}: page {page} after {after}: and the whole-board read's"
            );
            let v = served(&table);
            assert_eq!(v["candidatesBounded"], serde_json::json!(false));
            assert_eq!(
                v["candidates"],
                serde_json::json!(total),
                "{label}: the era's wins"
            );
            decorated |= v["board"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|r| r["evidence"].as_array().unwrap())
                .any(|e| e["proofPosted"] == serde_json::json!(true));
        }
    }
    decorated
}

/// THE PIN OF THE LANE. Under a cutoff stamped into the derivation, the table read serves every page and its body
/// is #642's candidate read's under the same cutoff (the #685 decoration included), a board that is not the
/// cutoff-free one. RED on main (`b444fc7`): under any cutoff `leaderboard_page_body` took the candidate read, "the
/// table read served it under the cutoff"; RED with the overlay's `board_win_era` guard removed alone: the tables
/// count the wins before the cutoff, "the table read is the candidate read's body under the cutoff".
#[test]
fn lb674_cut_under_the_cutoff_the_table_read_is_the_candidate_read() {
    let db = fixture(1_500);
    let era = cutoff_at(&db, 600);
    assert!(counted_under(&db, Some(era)) < counted_under(&db, None));
    stamp(&db, Some(era));
    assert!(
        identical_under(&db, Some(era), "cut at the 600th win"),
        "the #685 decoration is part of the compared bytes"
    );
    let start = crate::logic::PageStart::Offset(0);
    assert_ne!(
        without_count(&served_under(&db, 50, &start, Some(era)).0),
        without_count(&candidates_under(&db, 50, &start, None)),
        "the era's board is not the cutoff-free board"
    );
}

/// The keyset cursor walk under the cutoff, page by page (`nextCursor`, `nextAfter`), is the candidate read's: a
/// walk of at least four pages, every page served by the table read.
#[test]
fn lb674_cut_the_cursor_walk_under_the_cutoff_is_the_candidate_walk() {
    let db = fixture(1_500);
    let era = cutoff_at(&db, 600);
    stamp(&db, Some(era));
    let page = 40;
    let mut start = crate::logic::PageStart::Offset(0);
    let mut pages = 0;
    loop {
        let (table, log) = served_under(&db, page, &start, Some(era));
        assert!(used_the_tables(&log), "page {pages}: the table read");
        let cands = candidates_under(&db, page, &start, Some(era));
        assert_eq!(without_count(&table), without_count(&cands), "page {pages}");
        pages += 1;
        match served(&table).get("nextCursor").and_then(|c| c.as_str()) {
            Some(c) => start = crate::logic::parse_leaderboard_cursor(c).unwrap(),
            None => break,
        }
    }
    assert!(pages >= 4, "a walk of four pages or more: {pages}");
}

/// A moved cutoff re-derives the board behind its stamp: raised, cleared, lowered and set again, each stamp's board
/// is the candidate read's under that cutoff. A route whose cutoff is not the stamp (the var moved, the scheduled
/// pass not yet run) reads the candidates, under its own cutoff, never the tables of another.
#[test]
fn lb674_cut_a_moved_cutoff_rederives_behind_the_stamp() {
    let db = fixture(1_500);
    let (low, mid, high) = (
        cutoff_at(&db, 200),
        cutoff_at(&db, 600),
        cutoff_at(&db, 1_100),
    );
    let start = crate::logic::PageStart::Offset(0);
    for (step, era) in [
        ("set", Some(mid)),
        ("raised", Some(high)),
        ("cleared", None),
        ("lowered", Some(low)),
        ("set again", Some(mid)),
    ] {
        stamp(&db, era);
        identical_under(&db, era, step);
        for other in [None, Some(low), Some(mid), Some(high)]
            .into_iter()
            .filter(|o| *o != era)
        {
            let (body, log) = served_under(&db, 50, &start, other);
            assert!(
                !used_the_tables(&log),
                "{step}: a route under another cutoff ({other:?}) reads the candidates"
            );
            assert_eq!(body, candidates_under(&db, 50, &start, other), "{step}");
        }
    }
    // The stamp again is a no-op: the board's rows are not rewritten.
    let before: i64 = db
        .query_row("SELECT total_changes()", [], |r| r.get(0))
        .unwrap();
    stamp(&db, Some(mid));
    let after: i64 = db
        .query_row("SELECT total_changes()", [], |r| r.get(0))
        .unwrap();
    assert_eq!(before, after, "the same stamp writes nothing");
}

/// Past #642's bound under a cutoff the table read is still the era's WHOLE board: on the 3,000 win board cut after
/// its 300th win (2,700 era wins), the candidate read ranks the last 2,000 and says so; the table read is the
/// whole-board read's body under the cutoff, byte for byte.
#[test]
fn lb674_cut_past_the_candidate_bound_the_table_read_is_the_eras_whole_board() {
    let db = fixture(3_000);
    let era = cutoff_at(&db, 300);
    let total = counted_under(&db, Some(era));
    assert!(total > crate::logic::LEADERBOARD_CANDIDATE_WINS);
    stamp(&db, Some(era));
    for after in [0usize, 51] {
        let start = crate::logic::PageStart::Offset(after);
        let (table, log) = served_under(&db, 50, &start, Some(era));
        assert!(used_the_tables(&log));
        assert_eq!(
            without_642_fields(&table),
            whole_under(&db, 50, after, Some(era)),
            "after {after}"
        );
        let v = served(&table);
        assert_eq!(v["candidatesBounded"], serde_json::json!(false));
        assert_eq!(v["candidates"], serde_json::json!(total));
        let cands = candidates_under(&db, 50, &start, Some(era));
        assert_eq!(served(&cands)["candidatesBounded"], serde_json::json!(true));
    }
}

/// Beta's own cutoff (`crates/low-app-layer/wrangler.toml`, `[env.beta.vars] WRITTEN_OFF_BEFORE_MS`).
const BETA_CUTOFF_MS: i64 = 1_789_995_861_000;

/// A snapshot of beta's board sources (`LB674_BETA_ROWS` names the directory: one JSON array a table, each row its
/// columns and `_rid` its rowid, exported read-only; the bundles' bytes are not exported and load as `x''`, which no
/// board read touches), loaded into the production schema through the triggers, rowids kept.
fn beta_board(dir: &str) -> Connection {
    let db = super::leaderboard_642::schema();
    db.execute_batch("BEGIN").unwrap();
    for table in [
        "pot_records",
        "network_seen",
        "potparty_records",
        "result_markers_v2",
        "proof_markers",
        "proof_posts",
    ] {
        let local: Vec<String> = db
            .prepare(&format!("PRAGMA table_info({table})"))
            .unwrap()
            .query_map([], |r| r.get::<_, String>(1))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let rows: Vec<serde_json::Map<String, serde_json::Value>> =
            serde_json::from_str(&std::fs::read_to_string(format!("{dir}/{table}.json")).unwrap())
                .unwrap();
        let mut skipped = std::collections::BTreeSet::new();
        for row in &rows {
            let mut cols = vec!["rowid".to_string()];
            let mut vals: Vec<rusqlite::types::Value> = vec![rusqlite::types::Value::Integer(
                row["_rid"].as_i64().unwrap(),
            )];
            for (k, v) in row {
                if k == "_rid" {
                    continue;
                }
                if !local.contains(k) {
                    skipped.insert(k.clone());
                    continue;
                }
                cols.push(k.clone());
                vals.push(match v {
                    serde_json::Value::Null => rusqlite::types::Value::Null,
                    serde_json::Value::Number(n) if n.is_i64() => {
                        rusqlite::types::Value::Integer(n.as_i64().unwrap())
                    }
                    serde_json::Value::Number(n) => {
                        rusqlite::types::Value::Real(n.as_f64().unwrap())
                    }
                    serde_json::Value::String(s) => rusqlite::types::Value::Text(s.clone()),
                    other => panic!("{table}.{k}: {other}"),
                });
            }
            if local.iter().any(|c| c == "bundle") {
                cols.push("bundle".to_string());
                vals.push(rusqlite::types::Value::Blob(Vec::new()));
            }
            let marks = vec!["?"; cols.len()].join(",");
            db.execute(
                &format!("INSERT INTO {table} ({}) VALUES ({marks})", cols.join(",")),
                rusqlite::params_from_iter(vals),
            )
            .unwrap_or_else(|e| panic!("{table}: {e}"));
        }
        println!(
            "{table}: {} rows (columns not in the local schema: {skipped:?})",
            rows.len()
        );
    }
    db.execute_batch("COMMIT").unwrap();
    db
}

/// THE LANE'S PIN ON BETA'S ROWS (`LB674_BETA_ROWS=<dir> cargo test ... lb674_cut_beta -- --ignored`): the snapshot's
/// board, derived through the triggers with no cutoff, is beta's own (every counted win in `board_total`); under beta's
/// cutoff, stamped as the scheduled pass stamps it, every page shape and the cursor walk at `?limit=10` are served by
/// the table read and are #642's candidate read's bodies (and the whole-board read's) byte for byte but the counting
/// fields.
#[test]
#[ignore]
fn lb674_cut_beta_rows_under_the_beta_cutoff_the_table_read_is_the_candidate_read() {
    let Ok(dir) = std::env::var("LB674_BETA_ROWS") else {
        return;
    };
    let db = beta_board(&dir);
    let all = counted_under(&db, None);
    let total: i64 = db
        .query_row("SELECT wins FROM board_total WHERE id = 1", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(
        total as usize, all,
        "the triggers derived every counted win"
    );
    let era = Some(BETA_CUTOFF_MS);
    let era_wins = counted_under(&db, era);
    stamp(&db, era);
    let owners: i64 = db
        .query_row("SELECT COUNT(*) FROM board_owners", [], |r| r.get(0))
        .unwrap();
    println!("beta rows: {all} counted wins; under the cutoff {era_wins} wins of {owners} owners");
    let mut bodies = 0;
    for page in [
        crate::logic::LEADERBOARD_PAGE_DEFAULT,
        7,
        10,
        crate::logic::LEADERBOARD_PAGE_MAX,
    ] {
        for after in [0usize, 7, 10, 30, 51] {
            let start = crate::logic::PageStart::Offset(after);
            let (table, log) = served_under(&db, page, &start, era);
            assert!(
                used_the_tables(&log),
                "page {page} after {after}: the table read"
            );
            assert_eq!(
                without_count(&table),
                without_count(&candidates_under(&db, page, &start, era)),
                "page {page} after {after}: the candidate read's body"
            );
            assert_eq!(
                without_642_fields(&table),
                whole_under(&db, page, after, era),
                "page {page} after {after}: the whole-board read's"
            );
            assert_eq!(served(&table)["candidates"], serde_json::json!(era_wins));
            bodies += 1;
        }
    }
    let mut start = crate::logic::PageStart::Offset(0);
    let mut pages = 0;
    loop {
        let (table, log) = served_under(&db, 10, &start, era);
        assert!(used_the_tables(&log), "cursor page {pages}: the table read");
        assert_eq!(
            without_count(&table),
            without_count(&candidates_under(&db, 10, &start, era)),
            "cursor page {pages}"
        );
        pages += 1;
        match served(&table).get("nextCursor").and_then(|c| c.as_str()) {
            Some(c) => start = crate::logic::parse_leaderboard_cursor(c).unwrap(),
            None => break,
        }
    }
    println!(
        "beta rows: {bodies} page bodies and a {pages}-page cursor walk at limit 10 identical"
    );
    assert!(pages >= 4, "{pages}");
}
