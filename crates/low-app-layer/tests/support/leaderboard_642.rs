//! bsv-low #642: the paged leaderboard read, pinned through the SHIPPED
//! pipeline (`leaderboard_page_body`, the production `*D1` row mappers) over
//! rusqlite with the production migrations, against the whole-board read it
//! replaces (`whole_board_page_body` below: the pre-#642 steps 1 to 4 over the
//! #403 SQL, kept verbatim but for the port's binds and log seam).
use super::*;
use crate::board_db::{Bind, BoardDb, BoardDbError};
use rusqlite::{types::Value, types::ValueRef, Connection};
use std::cell::RefCell;

#[path = "d1_board.rs"]
mod board;

const NOW: u64 = 1_790_000_000;

/// One owner-page row of the #403 chain-wins spine (the whole-board read the
/// #642 pins hold the candidate read against).
#[derive(Deserialize)]
struct OwnerRowD1 {
    owner: String,
    #[serde(rename = "identityIsKey", default)]
    #[allow(dead_code)]
    identity_is_key: Option<f64>,
    #[allow(dead_code)]
    wins: Option<f64>,
}

/// One counted-win row of the #403 owners query (the #642 pins' whole-board
/// read).
#[derive(Deserialize)]
struct ChainWinPotRowD1 {
    owner: String,
    #[serde(rename = "identityIsKey", default)]
    identity_is_key: Option<f64>,
    #[serde(rename = "potTxid")]
    pot_txid: String,
    #[serde(rename = "settleTxid")]
    settle_txid: String,
    verdict: String,
    #[serde(rename = "pubA")]
    pub_a: String,
    #[serde(rename = "pubB")]
    pub_b: String,
    #[serde(rename = "settleSigners", default)]
    settle_signers: Option<String>,
    #[serde(rename = "identityA", default)]
    identity_a: Option<String>,
    #[serde(rename = "identityB", default)]
    identity_b: Option<String>,
}

impl ChainWinPotRowD1 {
    fn into_row(self) -> crate::logic::ChainWinPotRow {
        crate::logic::ChainWinPotRow {
            owner: self.owner,
            identity_is_key: self.identity_is_key.is_some_and(|v| v != 0.0),
            pot_txid: self.pot_txid,
            settle_txid: self.settle_txid,
            verdict: self.verdict,
            pub_a: self.pub_a,
            pub_b: self.pub_b,
            settle_signers: self.settle_signers,
            identity_a: self.identity_a,
            identity_b: self.identity_b,
        }
    }
}

/// The port over rusqlite. Every statement it runs is kept (its SQL with the
/// binds expanded, and its rows) so the D1 cell can replay the request.
pub(crate) struct SqliteBoard<'a> {
    pub db: &'a Connection,
    pub log: RefCell<Vec<(String, Vec<serde_json::Value>)>>,
}

impl<'a> SqliteBoard<'a> {
    pub fn new(db: &'a Connection) -> Self {
        Self {
            db,
            log: RefCell::new(Vec::new()),
        }
    }
}

impl BoardDb for SqliteBoard<'_> {
    async fn all<T: serde::de::DeserializeOwned>(
        &self,
        sql: String,
        binds: Vec<Bind>,
    ) -> std::result::Result<Vec<T>, BoardDbError> {
        let mut stmt = self
            .db
            .prepare(&sql)
            .map_err(|e| BoardDbError::Query(e.to_string()))?;
        for (i, b) in binds.iter().enumerate() {
            let v = match b {
                Bind::Num(n) if n.fract() == 0.0 => Value::Integer(*n as i64),
                Bind::Num(n) => Value::Real(*n),
                Bind::Text(s) => Value::Text(s.clone()),
            };
            stmt.raw_bind_parameter(i + 1, v)
                .map_err(|e| BoardDbError::Bind(e.to_string()))?;
        }
        let expanded = stmt.expanded_sql().unwrap_or_default();
        let names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        let mut rows = stmt.raw_query();
        let mut out = Vec::new();
        while let Some(r) = rows
            .next()
            .map_err(|e| BoardDbError::Query(e.to_string()))?
        {
            let mut obj = serde_json::Map::new();
            for (i, k) in names.iter().enumerate() {
                obj.insert(
                    k.clone(),
                    match r.get_ref(i).unwrap() {
                        ValueRef::Null => serde_json::Value::Null,
                        ValueRef::Integer(n) => serde_json::json!(n),
                        ValueRef::Real(n) => serde_json::json!(n),
                        ValueRef::Text(s) => serde_json::json!(std::str::from_utf8(s).unwrap()),
                        ValueRef::Blob(s) => serde_json::json!(hex::encode(s)),
                    },
                );
            }
            out.push(serde_json::Value::Object(obj));
        }
        self.log.borrow_mut().push((expanded, out.clone()));
        out.into_iter()
            .map(|v| serde_json::from_value(v).map_err(|e| BoardDbError::Query(e.to_string())))
            .collect()
    }
}

/// The futures here never wait (rusqlite answers at once).
pub(crate) fn run<F: std::future::Future>(f: F) -> F::Output {
    let mut f = std::pin::pin!(f);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    loop {
        if let std::task::Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

pub(crate) fn schema() -> Connection {
    schema_under(&|sql| sql.to_string())
}

/// The production migrations, each statement text passed through `edit` first.
pub(crate) fn schema_under(edit: &dyn Fn(&str) -> String) -> Connection {
    let db = Connection::open_in_memory().unwrap();
    for sql in bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS {
        let sql = edit(sql);
        if let Err(e) = db.execute_batch(&sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}: {sql}");
        }
    }
    db
}

/// The fleet-shaped board of `wins` counted wins.
pub(crate) fn fixture(wins: u32) -> Connection {
    fill(schema(), wins)
}

/// The fleet-shaped board of `wins` counted wins over the schema `db`.
pub(crate) fn fill(db: Connection, wins: u32) -> Connection {
    db.execute_batch("BEGIN").unwrap();
    for sql in board::rows(NOW, wins) {
        db.execute_batch(&sql)
            .unwrap_or_else(|e| panic!("{e}: {sql}"));
    }
    db.execute_batch("COMMIT").unwrap();
    db
}

/// The pre-#642 pipeline's steps 1 to 4 (the whole-board spine, the page
/// owners' pots, the era hands, the hands owners' pots), then the same steps
/// 5 and 6. The reference the candidate read is held byte-identical to.
pub(crate) async fn whole_board_page_body<B: BoardDb>(
    db: &B,
    page: usize,
    after: usize,
    era: Option<i64>,
    computed_at: i64,
) -> (u16, String) {
    let era_slot = || Bind::Num(era.map_or(0.0, |ms| ms as f64));
    // 1) The owner page (the counting spine, whole era).
    let mut binds = vec![Bind::Num((page + 1) as f64), Bind::Num(after as f64)];
    if let Some(ms) = era {
        binds.push(Bind::Num(ms as f64));
    }
    let mut owner_rows: Vec<OwnerRowD1> =
        match db.all(crate::logic::chain_wins_spine_sql(era), binds).await {
            Ok(rows) => rows,
            Err(_) => return (503, "{\"error\":\"database query failed\"}".to_string()),
        };
    let truncated = owner_rows.len() > page;
    owner_rows.truncate(page);
    let owners: Vec<String> = owner_rows
        .iter()
        .map(|r| r.owner.to_ascii_lowercase())
        .collect();
    let next_after = crate::logic::leaderboard_next_after(after, page, truncated);

    // 2) The page owners' counted pots (chunked at the D1 bind cap).
    let mut pot_rows: Vec<crate::logic::ChainWinPotRow> = Vec::new();
    for chunk in owners.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
        let mut b = vec![era_slot()];
        b.extend(chunk.iter().map(|o| Bind::Text(o.clone())));
        match db
            .all::<ChainWinPotRowD1>(crate::logic::chain_wins_owners_sql(chunk.len(), era), b)
            .await
        {
            Ok(rows) => pot_rows.extend(rows.into_iter().map(ChainWinPotRowD1::into_row)),
            Err(_) => return (503, "{\"error\":\"database query failed\"}".to_string()),
        }
    }

    // 3) Statuses, FAIL-SAFE 503.
    let mut outpoints: Vec<crate::logic::Outpoint> = Vec::new();
    {
        let mut seen = std::collections::HashSet::new();
        for r in &pot_rows {
            if seen.insert(r.pot_txid.to_ascii_lowercase()) {
                outpoints.push(crate::logic::Outpoint {
                    txid: r.pot_txid.clone(),
                    vout: crate::logic::LEADERBOARD_POT_VOUT,
                });
            }
        }
    }
    let mut status_rows: Vec<PotRecordRow> = Vec::with_capacity(outpoints.len());
    for chunk in chunk_outpoints(&outpoints) {
        let mut b: Vec<Bind> = Vec::with_capacity(chunk.len() * 2);
        for op in chunk {
            b.push(Bind::Text(op.db_txid()));
            b.push(Bind::Num(f64::from(op.vout)));
        }
        match db.all::<PotRowD1>(batch_where_sql(chunk.len()), b).await {
            Ok(rows) => status_rows.extend(rows.into_iter().map(PotRowD1::into_row)),
            Err(_) => return (503, "{\"error\":\"database query failed\"}".to_string()),
        }
    }
    let statuses = assemble_statuses(&outpoints, &status_rows);
    let (verdicts, params_by_pot, signers_by_pot) = classify_spent_pots(db, &statuses).await;
    let attributions = crate::logic::attributions_from_pot_rows(&pot_rows);

    // 4) Decoration, BEST-EFFORT.
    let mut markers: Vec<ResultMarkerRow> = Vec::new();
    let mut marker_seen: std::collections::HashSet<(String, String, String)> =
        std::collections::HashSet::new();
    let mut push_marker = |m: ResultMarkerRow| {
        let key = (
            m.txid.to_ascii_lowercase(),
            m.game_id.to_ascii_lowercase(),
            m.winner.to_ascii_lowercase(),
        );
        if marker_seen.insert(key) {
            markers.push(m);
        }
    };
    let pot_txids: Vec<String> = outpoints.iter().map(|o| o.txid.clone()).collect();
    for chunk in pot_txids.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
        let b: Vec<Bind> = chunk.iter().map(|t| Bind::Text(t.clone())).collect();
        if let Ok(rows) = db
            .all::<ResultRowD1>(crate::logic::pot_markers_sql(chunk.len()), b)
            .await
        {
            for m in rows.into_iter().filter_map(ResultRowD1::into_marker) {
                push_marker(m);
            }
        }
    }
    {
        let mut b = vec![Bind::Num(page as f64)];
        if let Some(ms) = era {
            b.push(Bind::Num(ms as f64));
        }
        if let Ok(rows) = db
            .all::<ResultRowD1>(crate::logic::era_hands_sql(era), b)
            .await
        {
            for m in rows.into_iter().filter_map(ResultRowD1::into_marker) {
                push_marker(m);
            }
        }
    }
    let extra_outpoints: Vec<crate::logic::Outpoint> = {
        let have: std::collections::HashSet<String> = outpoints
            .iter()
            .map(|o| o.txid.to_ascii_lowercase())
            .collect();
        crate::logic::leaderboard_pot_outpoints(&markers)
            .into_iter()
            .filter(|o| !have.contains(&o.txid.to_ascii_lowercase()))
            .collect()
    };
    let (statuses, verdicts, params_by_pot, signers_by_pot, attributions) = if extra_outpoints
        .is_empty()
    {
        (
            statuses,
            verdicts,
            params_by_pot,
            signers_by_pot,
            attributions,
        )
    } else {
        let mut extra_rows: Vec<PotRecordRow> = Vec::new();
        for chunk in chunk_outpoints(&extra_outpoints) {
            let mut b: Vec<Bind> = Vec::with_capacity(chunk.len() * 2);
            for op in chunk {
                b.push(Bind::Text(op.db_txid()));
                b.push(Bind::Num(f64::from(op.vout)));
            }
            if let Ok(rows) = db.all::<PotRowD1>(batch_where_sql(chunk.len()), b).await {
                extra_rows.extend(rows.into_iter().map(PotRowD1::into_row));
            }
        }
        let extra_statuses = assemble_statuses(&extra_outpoints, &extra_rows);
        let (ev, ep, es) = classify_spent_pots(db, &extra_statuses).await;
        let mut hands_attr = attributions;
        let hands_owners: Vec<String> = {
            let mut v: Vec<String> = markers
                .iter()
                .filter(|m| {
                    extra_outpoints
                        .iter()
                        .any(|o| o.txid.eq_ignore_ascii_case(&m.pot_txid))
                })
                .map(|m| m.winner.to_ascii_lowercase())
                .collect();
            v.sort();
            v.dedup();
            v
        };
        for chunk in hands_owners.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
            let mut b = vec![era_slot()];
            b.extend(chunk.iter().map(|o| Bind::Text(o.clone())));
            if let Ok(rows) = db
                .all::<ChainWinPotRowD1>(crate::logic::chain_wins_owners_sql(chunk.len(), era), b)
                .await
            {
                let rows: Vec<crate::logic::ChainWinPotRow> =
                    rows.into_iter().map(ChainWinPotRowD1::into_row).collect();
                hands_attr.extend(crate::logic::attributions_from_pot_rows(&rows));
            }
        }
        let mut statuses = statuses;
        statuses.extend(extra_statuses);
        let mut verdicts = verdicts;
        verdicts.extend(ev);
        let mut params_by_pot = params_by_pot;
        params_by_pot.extend(ep);
        let mut signers_by_pot = signers_by_pot;
        signers_by_pot.extend(es);
        (
            statuses,
            verdicts,
            params_by_pot,
            signers_by_pot,
            hands_attr,
        )
    };
    let (lb, result_count) = leaderboard_fold_and_decorate_with(
        db,
        &markers,
        &statuses,
        page,
        &verdicts,
        &attributions,
        &params_by_pot,
        &signers_by_pot,
        &owners,
        &PagePrefetch::default(),
    )
    .await;
    (
        200,
        crate::logic::leaderboard_body(&lb, computed_at, result_count, truncated, next_after),
    )
}

pub(crate) fn new_body(
    db: &Connection,
    page: usize,
    start: &crate::logic::PageStart,
    cap: usize,
) -> (String, Vec<(String, Vec<serde_json::Value>)>) {
    let board = SqliteBoard::new(db);
    let (status, body) = run(leaderboard_candidate_body(
        &board, page, start, None, cap, NOW as i64,
    ));
    assert_eq!(status, 200, "{body}");
    (body, board.log.into_inner())
}

/// The SHIPPED dispatcher (`leaderboard_page_body`): the #674 table read on a
/// stamped database.
pub(crate) fn served_body(
    db: &Connection,
    page: usize,
    start: &crate::logic::PageStart,
) -> (String, Vec<(String, Vec<serde_json::Value>)>) {
    let board = SqliteBoard::new(db);
    let (status, body) = run(leaderboard_page_body(
        &board,
        page,
        start,
        None,
        crate::logic::LEADERBOARD_CANDIDATE_WINS,
        NOW as i64,
    ));
    assert_eq!(status, 200, "{body}");
    (body, board.log.into_inner())
}

pub(crate) fn old_body(db: &Connection, page: usize, after: usize) -> (String, usize) {
    let board = SqliteBoard::new(db);
    let (status, body) = run(whole_board_page_body(&board, page, after, None, NOW as i64));
    assert_eq!(status, 200, "{body}");
    let stmts = board.log.borrow().len();
    (body, stmts)
}

/// The served body without the #642 fields: the pre-#642 bytes when the
/// candidates covered the era.
pub(crate) fn without_642_fields(body: &str) -> String {
    let mut v: serde_json::Value = serde_json::from_str(body).unwrap();
    let m = v.as_object_mut().unwrap();
    for k in ["nextCursor", "candidates", "candidatesBounded"] {
        m.remove(k);
    }
    v.to_string()
}

fn served(body: &str) -> serde_json::Value {
    serde_json::from_str(body).unwrap()
}

#[test]
fn lb642_the_first_page_is_byte_identical_to_the_whole_board_read() {
    let db = fixture(1_500);
    let mut checked_decoration = false;
    let mut checked_keys = false;
    for page in [
        crate::logic::LEADERBOARD_PAGE_DEFAULT,
        7,
        crate::logic::LEADERBOARD_PAGE_MAX,
    ] {
        for after in [0usize, 7, 51, 140, 400] {
            let (new, _) = new_body(
                &db,
                page,
                &crate::logic::PageStart::Offset(after),
                crate::logic::LEADERBOARD_CANDIDATE_WINS,
            );
            let (old, _) = old_body(&db, page, after);
            assert_eq!(
                without_642_fields(&new),
                old,
                "page {page} after {after}: the served body is the whole-board read's"
            );
            let v = served(&new);
            assert!(
                v.get("candidatesBounded").is_none(),
                "1,500 wins are inside the bound"
            );
            assert_eq!(
                v.get("nextCursor").is_some(),
                v["truncated"] == serde_json::json!(true),
                "a cursor exactly when there is a next page"
            );
            if after == 0 {
                let board = v["board"].as_array().unwrap();
                assert_eq!(board.len(), page.min(board.len()));
                assert!(
                    board[0]["wins"].as_u64().unwrap() > 10,
                    "the skewed head counts"
                );
                assert!(
                    !v["hands"].as_array().unwrap().is_empty(),
                    "the hands board"
                );
                let ev: Vec<&serde_json::Value> = board
                    .iter()
                    .flat_map(|r| r["evidence"].as_array().unwrap())
                    .collect();
                if ev
                    .iter()
                    .any(|e| e["proofPosted"] == serde_json::json!(true))
                    && ev
                        .iter()
                        .any(|e| !e["proofTxids"].as_array().unwrap().is_empty())
                {
                    checked_decoration = true;
                }
            }
            if v["board"]
                .as_array()
                .unwrap()
                .iter()
                .any(|r| r["identityIsKey"] == serde_json::json!(true))
            {
                checked_keys = true;
            }
        }
    }
    assert!(
        checked_decoration,
        "the #685 decoration is part of the compared bytes"
    );
    assert!(
        checked_keys,
        "key-owned rows are part of the compared bytes"
    );
}

#[test]
fn lb642_the_cursor_walk_is_the_offset_walk() {
    let db = fixture(1_500);
    let page = 40;
    let cap = crate::logic::LEADERBOARD_CANDIDATE_WINS;
    let mut by_cursor = Vec::new();
    let mut start = crate::logic::PageStart::Offset(0);
    let mut after = 0usize;
    loop {
        let (body, _) = new_body(&db, page, &start, cap);
        let (offset, _) = new_body(&db, page, &crate::logic::PageStart::Offset(after), cap);
        let (v, w) = (served(&body), served(&offset));
        assert_eq!(
            v["board"], w["board"],
            "the cursor page at rank {after} is the offset page"
        );
        assert_eq!(v["nextAfter"], w["nextAfter"]);
        by_cursor.extend(
            v["board"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r["identity"].clone()),
        );
        match v.get("nextCursor").and_then(|c| c.as_str()) {
            Some(c) => {
                start = crate::logic::parse_leaderboard_cursor(c).expect("a served cursor parses");
                after += page;
            }
            None => break,
        }
    }
    let unique: std::collections::HashSet<String> = by_cursor
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    assert_eq!(unique.len(), by_cursor.len(), "every owner once");
    assert!(
        by_cursor.len() > page * 3,
        "a walk of several pages: {}",
        by_cursor.len()
    );
    assert_eq!(
        crate::logic::parse_leaderboard_cursor("12.0A"),
        None,
        "upper case is not ours"
    );
    assert_eq!(crate::logic::parse_leaderboard_cursor("x.ab"), None);
    assert_eq!(crate::logic::parse_leaderboard_cursor("3"), None);
}

/// Past the bound the board is exactly the whole-board read of the last
/// `cap` counted wins, and says so.
#[test]
fn lb642_a_bounded_board_is_the_whole_board_of_the_last_n_games() {
    let db = fixture(1_500);
    let cap = 400;
    let (new, log) = new_body(&db, 50, &crate::logic::PageStart::Offset(0), cap);
    let v = served(&new);
    assert_eq!(v["candidatesBounded"], serde_json::json!(true));
    assert_eq!(v["candidates"], serde_json::json!(cap));
    // The kept candidates: the first statement's rows, in order.
    let kept: Vec<String> = log[0].1[..cap]
        .iter()
        .map(|r| r["potTxid"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(log[0].1.len(), cap + 1, "the probe row");
    // They are the era's most recent `cap` counted wins (the #403 predicates).
    let recent: Vec<String> = db
        .prepare(&format!(
            "{} SELECT o.potTxid FROM owned o JOIN pot_records p ON p.txid = o.potTxid AND p.outputIndex = 0 \
             ORDER BY p.createdAt DESC, p.rowid DESC LIMIT {cap}",
            crate::logic::chain_wins_cte("?1", None)
        ))
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(
        kept, recent,
        "the candidates are the last {cap} counted wins"
    );
    let trimmed = fixture(1_500);
    let held: std::collections::HashSet<&str> = kept.iter().map(String::as_str).collect();
    let all: Vec<String> = trimmed
        .prepare("SELECT txid FROM pot_records WHERE outputIndex = 0 AND verdict IS NOT NULL")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    for t in all.iter().filter(|t| !held.contains(t.as_str())) {
        trimmed
            .execute(
                "UPDATE pot_records SET verdict = NULL WHERE txid = ?1 AND outputIndex = 0",
                [t],
            )
            .unwrap();
    }
    for after in [0usize, 50] {
        let (new, _) = new_body(&db, 50, &crate::logic::PageStart::Offset(after), cap);
        let (old, _) = old_body(&trimmed, 50, after);
        assert_eq!(without_642_fields(&new), old, "after {after}");
    }
}

/// The D1 cell's input (`make ci-d1-board`, `tools/lane-642/board_pin.mjs`):
/// for each board size, the seed (the production schema and the board) and
/// every statement each page request runs, with its rows, from the SHIPPED
/// pipeline; plus the whole-board read's statements for the record. Writes
/// nothing unless `LB642_D1_PIN` names a directory.
#[test]
#[ignore]
fn lb642_emit_the_d1_cell() {
    let Ok(dir) = std::env::var("LB642_D1_PIN") else {
        return;
    };
    for wins in [1_500u32, 3_000] {
        let db = fixture(wins);
        let mut seed = String::new();
        let mut stmt = db
            .prepare("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%' ORDER BY rowid")
            .unwrap();
        for sql in stmt.query_map([], |r| r.get::<_, String>(0)).unwrap() {
            seed.push_str(&sql.unwrap());
            seed.push_str(";\n");
        }
        // bsv-low #674: the data rows the migrations write (the total's row, the
        // stamp), then the board, whose INSERTs fire the triggers on local D1.
        for sql in [
            bsv_overlay_cloudflare::derived_board::TOTAL_SEED,
            bsv_overlay_cloudflare::derived_board::STATE_DONE,
        ] {
            seed.push_str(sql);
            seed.push_str(";\n");
        }
        for row in board::rows(NOW, wins) {
            seed.push_str(&row);
            seed.push_str(";\n");
        }
        let cap = crate::logic::LEADERBOARD_CANDIDATE_WINS;
        let mut requests = Vec::new();
        // The SHIPPED read (#674's table read on the stamped board).
        let first = served_body(&db, 50, &crate::logic::PageStart::Offset(0));
        let cursor = served(&first.0)["nextCursor"].as_str().unwrap().to_string();
        let start = crate::logic::parse_leaderboard_cursor(&cursor).unwrap();
        let max = crate::logic::LEADERBOARD_PAGE_MAX;
        for (name, ceiling, (_, log)) in [
            (
                "page 1, limit 50",
                crate::logic::LEADERBOARD_VIEW_MAX_READS,
                first,
            ),
            (
                "page 1, limit 200",
                crate::logic::LEADERBOARD_VIEW_MAX_READS_PAGE_MAX,
                served_body(&db, max, &crate::logic::PageStart::Offset(0)),
            ),
            (
                "page 2 by cursor, limit 50",
                crate::logic::LEADERBOARD_VIEW_MAX_READS,
                served_body(&db, 50, &start),
            ),
        ] {
            assert!(
                log.iter().any(|(sql, _)| sql.contains("FROM board_owners")),
                "{name}: the table read"
            );
            requests.push(serde_json::json!({
                "name": name,
                "ceiling": ceiling,
                "statementCeiling": crate::logic::LEADERBOARD_STATEMENT_MAX_READS,
                // bsv-low #674 fold, MEDIUM-1: the default page reads the same rows on both boards.
                "flat": name == "page 1, limit 50",
                "statements": log.iter().map(|(sql, rows)| serde_json::json!({"sql": sql, "rows": rows})).collect::<Vec<_>>(),
            }));
        }
        // #642's candidate read on the same board, for the record.
        let (_, log) = new_body(&db, 50, &crate::logic::PageStart::Offset(0), cap);
        requests.push(serde_json::json!({
            "name": "the #642 candidate read, page 1, limit 50",
            "ceiling": null,
            "statements": log.iter().map(|(sql, rows)| serde_json::json!({"sql": sql, "rows": rows})).collect::<Vec<_>>(),
        }));
        let board = SqliteBoard::new(&db);
        run(whole_board_page_body(&board, 50, 0, None, NOW as i64));
        requests.push(serde_json::json!({
            "name": "the whole-board read (pre-#642), page 1, limit 50",
            "ceiling": null,
            "mustExceed": crate::logic::LEADERBOARD_VIEW_MAX_READS,
            "statements": board.log.into_inner().iter().map(|(sql, rows)| serde_json::json!({"sql": sql, "rows": rows})).collect::<Vec<_>>(),
        }));
        let out = format!("{dir}/w{wins}");
        std::fs::create_dir_all(&out).unwrap();
        std::fs::write(format!("{out}/seed.sql"), seed).unwrap();
        std::fs::write(
            format!("{out}/expected.json"),
            serde_json::json!({"wins": wins, "candidateCap": cap, "requests": requests})
                .to_string(),
        )
        .unwrap();
        println!("wrote {out}");
    }
}
