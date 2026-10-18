//! bsv-low #674: the leaderboard tables derived at the write, pinned against a from-scratch aggregation.
//!
//! THE DRIFT PIN: after every writer path of the settle (an admission, a spend record, the classifier's verdict,
//! the proof, a tie, a refund, the witness, a relatch and a poisoned slot, a claim and its relatch, the eviction's
//! shadow move and the readmission, a REPLACE), `board_wins`, `board_owners`, `board_hands` and `board_total`
//! equal what the app layer's own `chain_wins_cte`, `chain_wins_spine_sql` and `era_hands_sql` answer over the
//! whole source. The eviction and readmission run the overlay's SHIPPED passes (`shadow_move_passes`,
//! `restore_twins`) and its spend release and re-mark statements. Since the lens fold (LOW-2) the settle's own
//! steps run the overlay's writer statements too (`store_record_sql`, the seat and claim admission and re-latch
//! queries, `mark_spent_sql`, `verdict_cas_sql`, `confirm_spend_cas_sql`, `reanchor_confirmed_cas_sql`, the
//! `network_seen` latch, the proof pointer and posted bundle writes); a seat's or claim's latched tier is the one
//! value the pin substitutes (the statement binds the tier its signature check computes, and the pin holds no
//! signing wallet). Then every trigger is removed in turn and the same paths must go red: the trigger set is
//! complete and each trigger is needed.
use async_trait::async_trait;
use bsv_overlay_cloudflare::admit_fast::{self, ColumnInfo, ShadowDb};
use bsv_overlay_cloudflare::d1::{QVal, Query, OVERLAY_MIGRATIONS};
use bsv_overlay_cloudflare::d1_discovery as d1d;
use bsv_overlay_cloudflare::derived_board as board;
use overlay_discovery::potparty::storage::PotpartyRecord;
use overlay_discovery::result::storage::ResultRecord;
use rusqlite::{types::ValueRef, Connection};

#[path = "support/d1_board.rs"]
mod d1_board;

const NOW: u64 = 1_790_000_000;

fn pot(g: u32) -> String {
    format!("b0{g:062x}")
}
fn spender(g: u32) -> String {
    format!("b1{g:062x}")
}
fn game(g: u32) -> String {
    format!("b2{g:062x}")
}
/// The seeded winner of game `g` (`d1_board::rows`).
fn winner(g: u32) -> String {
    d1_board::identity(if g.is_multiple_of(3) { g % 7 } else { g % 150 })
}

fn schema_without(skip: Option<&str>) -> Connection {
    let db = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        if Some(*sql) == skip {
            continue;
        }
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}: {sql}");
        }
    }
    db
}

fn rows(db: &Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = db.prepare(sql).unwrap_or_else(|e| panic!("{e}: {sql}"));
    let n = stmt.column_count();
    stmt.query_map([], |r| {
        Ok((0..n)
            .map(|i| match r.get_ref(i).unwrap() {
                ValueRef::Null => "NULL".to_string(),
                ValueRef::Integer(v) => v.to_string(),
                ValueRef::Real(v) => v.to_string(),
                ValueRef::Text(s) => String::from_utf8_lossy(s).into_owned(),
                ValueRef::Blob(b) => hex::encode(b),
            })
            .collect())
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

/// The rows of `sql` (binds `p`) as JSON objects by column name, as D1 hands them to the app layer.
fn jrows(db: &Connection, sql: &str, p: &[&dyn rusqlite::ToSql]) -> Vec<serde_json::Value> {
    let mut stmt = db.prepare(sql).unwrap_or_else(|e| panic!("{e}: {sql}"));
    let names: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
    stmt.query_map(p, |r| {
        let mut m = serde_json::Map::new();
        for (i, n) in names.iter().enumerate() {
            let v = match r.get_ref(i).unwrap() {
                ValueRef::Null => serde_json::Value::Null,
                ValueRef::Integer(v) => serde_json::json!(v),
                ValueRef::Real(v) => serde_json::json!(v),
                ValueRef::Text(s) => serde_json::json!(String::from_utf8_lossy(s)),
                ValueRef::Blob(b) => serde_json::json!(hex::encode(b)),
            };
            m.insert(n.clone(), v);
        }
        Ok(serde_json::Value::Object(m))
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

/// bsv-low #674 fold, MEDIUM-1: every frag equals what the app layer's own page statements read for its pot from
/// scratch (`batch_where_sql`, `decoded_pots_sql`, `pot_markers_sql`, then each marker's `proof_pointers_sql` and
/// `proof_posted_sql`), and every card holds exactly its owner's wins, each its `board_wins` row and frag.
fn frag_and_card_drift(db: &Connection, label: &str) -> Result<(), String> {
    use low_app_layer::logic as l;
    let wins = jrows(db, "SELECT * FROM board_wins ORDER BY potTxid", &[]);
    for w in &wins {
        let pot = w["potTxid"].as_str().unwrap();
        let frag: serde_json::Value = match w["frag"].as_str() {
            Some(f) => serde_json::from_str(f).unwrap(),
            None => return Err(format!("{label}: {} has no frag", &pot[..8])),
        };
        let lower = pot.to_ascii_lowercase();
        let mut status = jrows(db, &l::batch_where_sql(1), &[&lower, &0]);
        status.iter_mut().for_each(|r| {
            r.as_object_mut().unwrap().remove("fundingSeen");
        });
        let decoded = jrows(
            db,
            &low_app_layer::results::decoded_pots_sql(1),
            &[&lower, &0],
        );
        let markers = jrows(db, &l::pot_markers_sql(1), &[&pot]);
        let mut held: Vec<serde_json::Value> = frag["m"].as_array().unwrap().clone();
        held.sort_by_key(|m| m["rn"].as_i64().unwrap());
        let mut want_m = Vec::new();
        let mut got_m = Vec::new();
        for (m, h) in markers.iter().zip(held.iter()) {
            let (g, wn) = (
                m["gameId"].as_str().unwrap().to_ascii_lowercase(),
                m["winner"].as_str().unwrap().to_ascii_lowercase(),
            );
            let pointers: Vec<serde_json::Value> = jrows(db, &l::proof_pointers_sql(1), &[&g, &wn])
                .into_iter()
                .map(|r| r["txid"].clone())
                .collect();
            let posted = !jrows(
                db,
                &low_app_layer::proof_post::proof_posted_sql(1),
                &[&g, &wn],
            )
            .is_empty();
            want_m.push((m.clone(), pointers, posted));
            let mut h = h.clone();
            let o = h.as_object_mut().unwrap();
            o.remove("rn");
            let mut pp: Vec<(Option<i64>, i64, serde_json::Value)> = o
                .remove("pp")
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .map(|p| (p[0].as_i64(), p[1].as_i64().unwrap(), p[2].clone()))
                .collect();
            pp.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.cmp(&a.1)));
            let po = o.remove("po").unwrap().as_i64().unwrap() != 0;
            got_m.push((h, pp.into_iter().map(|p| p.2).collect::<Vec<_>>(), po));
        }
        let checks = [
            (
                "status",
                serde_json::json!(status),
                serde_json::json!([frag["s"]]),
            ),
            (
                "decoded",
                serde_json::json!(decoded),
                serde_json::json!([frag["d"]]),
            ),
            (
                "markers",
                serde_json::json!(want_m
                    .iter()
                    .map(|m| serde_json::json!(m))
                    .collect::<Vec<_>>()),
                serde_json::json!(got_m
                    .iter()
                    .map(|m| serde_json::json!(m))
                    .collect::<Vec<_>>()),
            ),
            (
                "marker count",
                serde_json::json!(markers.len()),
                serde_json::json!(held.len()),
            ),
        ];
        for (what, want, got) in checks {
            if want != got {
                return Err(format!(
                    "{label}: the frag of {} drifted ({what}): from scratch {want}, held {got}",
                    &pot[..8]
                ));
            }
        }
    }
    for o in jrows(
        db,
        "SELECT owner, wins, packed, card FROM board_owners ORDER BY owner",
        &[],
    ) {
        let owner = o["owner"].as_str().unwrap();
        let card: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(o["card"].as_str().unwrap()).unwrap();
        let mut want = serde_json::Map::new();
        for w in wins.iter().filter(|w| w["owner"] == o["owner"]) {
            let mut e = w.clone();
            let f: serde_json::Value = serde_json::from_str(w["frag"].as_str().unwrap()).unwrap();
            let m = e.as_object_mut().unwrap();
            m.remove("potVout");
            m.remove("frag");
            m.insert("f".into(), f);
            want.insert(w["potTxid"].as_str().unwrap().to_string(), e);
        }
        if o["packed"] != o["wins"]
            || card.len() as i64 != o["packed"].as_i64().unwrap()
            || card != want
        {
            return Err(format!(
                "{label}: the card of {} drifted (wins {}, packed {}, {} held, {} from scratch)",
                &owner[..10],
                o["wins"],
                o["packed"],
                card.len(),
                want.len()
            ));
        }
    }
    Ok(())
}

/// The derived tables against the from-scratch read; `Err` names what drifted.
fn drift(db: &Connection, label: &str) -> Result<(), String> {
    let cte = low_app_layer::logic::chain_wins_cte("?1", None);
    let checks = [
        (
            "board_wins",
            format!(
                "{cte} SELECT potTxid, COALESCE(winnerIdentity, winKey), CASE WHEN winnerIdentity IS NULL THEN 1 ELSE 0 END, \
                 settleTxid, verdict, pubA, pubB, settleSigners, identityA, identityB, potCreatedAt FROM owned ORDER BY potTxid"
            ),
            "SELECT potTxid, owner, identityIsKey, settleTxid, verdict, pubA, pubB, settleSigners, identityA, identityB, \
             potCreatedAt FROM board_wins ORDER BY potTxid"
                .to_string(),
        ),
        (
            "board_owners",
            low_app_layer::logic::chain_wins_spine_sql(None)
                .replace("LIMIT ?1 OFFSET ?2", "LIMIT -1 OFFSET 0"),
            "SELECT owner, CASE WHEN keyWins > 0 THEN 1 ELSE 0 END, wins FROM board_owners ORDER BY wins DESC, owner ASC"
                .to_string(),
        ),
        (
            "board_hands",
            format!(
                "SELECT potTxid, txid FROM ({})",
                low_app_layer::logic::era_hands_sql(None).replace("LIMIT ?1", "LIMIT -1")
            ),
            "SELECT h.potTxid, m.txid FROM board_hands h LEFT JOIN result_markers_v2 m ON m.rowid = h.markerRowid \
             ORDER BY h.score, h.claimCreatedAt, h.gameId"
                .to_string(),
        ),
        (
            "board_total",
            format!("{cte} SELECT COUNT(*) FROM owned"),
            "SELECT wins FROM board_total".to_string(),
        ),
    ];
    for (table, scratch, derived) in checks {
        let (want, got) = (rows(db, &scratch), rows(db, &derived));
        if want != got {
            let extra: Vec<_> = got.iter().filter(|r| !want.contains(r)).take(3).collect();
            let missing: Vec<_> = want.iter().filter(|r| !got.contains(r)).take(3).collect();
            return Err(format!(
                "{label}: {table} drifted ({} derived, {} from scratch; extra {extra:?}; missing {missing:?})",
                got.len(),
                want.len()
            ));
        }
    }
    frag_and_card_drift(db, label)
}

/// The overlay's shadow storage over rusqlite, as its own pins run it.
struct Shadow<'a>(&'a Connection);

fn to_sql(v: &QVal) -> rusqlite::types::Value {
    match v {
        QVal::Null => rusqlite::types::Value::Null,
        QVal::Int(i) => rusqlite::types::Value::Integer(*i),
        QVal::Text(t) => rusqlite::types::Value::Text(t.clone()),
        QVal::Bool(b) => rusqlite::types::Value::Integer(i64::from(*b)),
        QVal::Blob(b) => rusqlite::types::Value::Blob(b.clone()),
        QVal::Float(f) => rusqlite::types::Value::Real(*f),
    }
}

fn cols(db: &Connection, table: &str) -> Vec<ColumnInfo> {
    let mut st = db
        .prepare(&format!("PRAGMA table_info(\"{table}\")"))
        .unwrap();
    st.query_map([], |r| {
        Ok(ColumnInfo {
            name: r.get(1)?,
            ty: r.get::<_, String>(2).unwrap_or_default(),
            notnull: r.get::<_, i64>(3).unwrap_or(0),
            dflt_value: r.get::<_, Option<String>>(4).unwrap_or(None),
        })
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

#[async_trait(?Send)]
impl ShadowDb for Shadow<'_> {
    async fn columns(&self, table: &str) -> Result<Vec<ColumnInfo>, String> {
        Ok(cols(self.0, table))
    }
    async fn count(&self, table: &str, key: &str, txid: &str) -> Result<u64, String> {
        self.0
            .query_row(
                &format!("SELECT COUNT(*) FROM \"{table}\" WHERE \"{key}\" = ?1"),
                [txid],
                |r| r.get::<_, i64>(0),
            )
            .map(|n| n.max(0) as u64)
            .map_err(|e| e.to_string())
    }
    async fn vouts(&self, table: &str, txid: &str) -> Result<Vec<u32>, String> {
        let mut st = self
            .0
            .prepare(&format!(
                "SELECT \"outputIndex\" FROM \"{table}\" WHERE \"txid\" = ?1"
            ))
            .map_err(|e| e.to_string())?;
        let v = st
            .query_map([txid], |r| r.get::<_, i64>(0))
            .map_err(|e| e.to_string())?
            .map(|r| r.map(|v| v.max(0) as u32))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        Ok(v)
    }
    async fn exec(&self, sql: &str, binds: Vec<QVal>) -> Result<(), String> {
        let p: Vec<rusqlite::types::Value> = binds.iter().map(to_sql).collect();
        self.0
            .execute(sql, rusqlite::params_from_iter(p.iter()))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    async fn query_i64(&self, sql: &str, binds: Vec<QVal>) -> Result<Option<i64>, String> {
        use rusqlite::OptionalExtension;
        let p: Vec<rusqlite::types::Value> = binds.iter().map(to_sql).collect();
        self.0
            .query_row(sql, rusqlite::params_from_iter(p.iter()), |r| {
                r.get::<_, Option<i64>>(0)
            })
            .optional()
            .map(|o| o.flatten())
            .map_err(|e| e.to_string())
    }
}

/// Run a production writer's built query on rusqlite, its latched tier (the bind at `tier.0`, the value the
/// writer's own signature check computed) replaced by `tier.1` where the pin needs a verified seat or claim.
fn run_query(db: &Connection, q: &Query, tier: Option<(usize, i64)>) {
    let mut p: Vec<rusqlite::types::Value> = q.params().iter().map(to_sql).collect();
    if let Some((at, t)) = tier {
        assert!(
            matches!(p[at], rusqlite::types::Value::Integer(_)),
            "bind {at} of `{}` is the latched tier",
            q.sql()
        );
        p[at] = rusqlite::types::Value::Integer(t);
    }
    db.execute(q.sql(), rusqlite::params_from_iter(p.iter()))
        .unwrap_or_else(|e| panic!("{e}: {}", q.sql()));
}

/// Run a production `... RETURNING` statement (a CAS whose hit is a row back); `true` on a hit.
fn run_cas(db: &Connection, sql: &str, p: impl rusqlite::Params) -> bool {
    let mut stmt = db.prepare(sql).unwrap_or_else(|e| panic!("{e}: {sql}"));
    let mut rows = stmt.query(p).unwrap();
    rows.next().unwrap().is_some()
}

/// A seat marker as the admission parses it (`gameId` and keys as the settle writes them).
fn seat_record(identity: &str, pot: &str, key: &str, txid: &str) -> PotpartyRecord {
    PotpartyRecord {
        identity: identity.to_string(),
        opponent_identity: "x".to_string(),
        game_id: "c2".to_string(),
        pot_txid: pot.to_string(),
        pot_vout: 0,
        recovery_height: 958_846,
        sig_hex: "00".to_string(),
        seat_settle_pubkey: Some(key.to_string()),
        seat_sig_hex: Some("00".to_string()),
        txid: txid.to_string(),
        output_index: 0,
        created_at: 0,
    }
}

/// A result claim as the admission parses it.
fn claim_record(
    game: &str,
    winner: &str,
    loser: &str,
    pot: &str,
    settle: &str,
    cards: Option<&str>,
    txid: &str,
) -> ResultRecord {
    ResultRecord {
        game_id: game.to_string(),
        winner: winner.to_string(),
        loser: loser.to_string(),
        pot_txid: pot.to_string(),
        settle_txid: settle.to_string(),
        winner_sig_hex: "00".to_string(),
        loser_sig_hex: None,
        cards_hex: cards.map(str::to_string),
        txid: txid.to_string(),
        output_index: 0,
        created_at: 0,
    }
}

/// The futures here never wait (rusqlite answers at once).
fn run<F: std::future::Future>(f: F) -> F::Output {
    let mut f = std::pin::pin!(f);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    loop {
        if let std::task::Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

/// The eviction's shadow move (both passes) of `txid`, then the release of the spend pointers it left (the
/// `evict_txid_everywhere` order).
fn evict(db: &Connection, txid: &str) {
    let r = run(admit_fast::shadow_move_passes(
        &Shadow(db),
        txid,
        "REJECTED",
        1_000,
        false,
    ));
    assert!(r.incomplete.is_empty(), "{txid}: {:?}", r.incomplete);
    db.execute(
        &admit_fast::release_pot_spends_sql(&cols(db, "pot_records")),
        [txid],
    )
    .unwrap();
}

/// The readmission: the twins restored, then the released spends re-marked.
fn readmit(db: &Connection, txid: &str, released: &[(String, u32)]) {
    let (_, faults) = run(admit_fast::restore_twins(&Shadow(db), txid));
    assert!(faults.is_empty(), "{txid}: {faults:?}");
    let (sql, with_at) = admit_fast::remark_pot_spend_sql(&cols(db, "pot_records"));
    for (t, v) in released {
        if with_at {
            db.execute(&sql, rusqlite::params![txid, 1_000, t, v])
                .unwrap();
        } else {
            db.execute(&sql, rusqlite::params![txid, t, v]).unwrap();
        }
    }
}

/// Every writer path, each followed by the drift check. `Err` names the first drift.
fn paths(db: &Connection) -> Result<(), String> {
    let x = |sql: &str| {
        db.execute_batch(sql)
            .unwrap_or_else(|e| panic!("{e}: {sql}"))
    };
    // The fleet-shaped board, admitted row by row (pots, witnesses, seats, claims, proofs).
    x("BEGIN");
    for sql in d1_board::rows(NOW, 160) {
        x(&sql);
    }
    x("COMMIT");
    drift(db, "the seeded board")?;
    let counted = rows(db, "SELECT COUNT(*) FROM board_wins");
    if counted[0][0] == "0" {
        return Err("the seeded board: no counted win derived".into());
    }

    // A settle, step by step, through the overlay's own writers (the lens fold, LOW-2): the admission, the seats
    // (unlatched, then latched), the spend, the verdict, the proof, then the winner's claim.
    let (p, s) = ("c0".repeat(32), "c1".repeat(32));
    let (ida, idb) = (d1_board::identity(3), d1_board::identity(4));
    assert_eq!(
        db.execute(
            d1d::store_record_sql(),
            rusqlite::params![
                p,
                0,
                0,
                None::<String>,
                0,
                NOW as i64,
                "covenant",
                "03aa",
                "03bb",
                "03cc",
                None::<String>,
                None::<String>,
                None::<String>,
                None::<i64>,
                None::<i64>,
                None::<i64>,
                958_846,
                2_000,
                1
            ],
        )
        .unwrap(),
        1,
        "the writer lands one row"
    );
    drift(db, "settle: the pot admitted")?;
    let seats: Vec<PotpartyRecord> = [(&ida, "03aa"), (&idb, "03bb")]
        .into_iter()
        .enumerate()
        .map(|(k, (id, key))| seat_record(id, &p, key, &format!("c3{k}{}", "0".repeat(61))))
        .collect();
    for seat in &seats {
        let q = d1d::potparty_write::potparty_insert_query(seat, NOW as i64);
        assert!(!q.sig_valid(), "an unsigned seat is admitted unlatched");
        run_query(db, q.query(), None);
    }
    drift(db, "settle: the seats admitted, unlatched")?;
    assert_eq!(
        db.execute(
            d1d::mark_spent_sql(false, false),
            rusqlite::params![s, s, 1, 1, p, 0],
        )
        .unwrap(),
        1,
        "the writer lands one row"
    );
    drift(db, "settle: the spend recorded")?;
    assert_eq!(
        db.execute(
            d1d::verdict_cas_sql(),
            rusqlite::params!["winner-a", s, "coop", p, 0, s],
        )
        .unwrap(),
        1,
        "the writer lands one row"
    );
    drift(db, "settle: the verdict")?;
    assert!(run_cas(
        db,
        d1d::confirm_spend_cas_sql(),
        rusqlite::params![None::<i64>, p, 0, s]
    ));
    drift(db, "settle: the proof (counted under the key)")?;
    for seat in &seats {
        let q = d1d::potparty_write::potparty_relatch_query(seat);
        run_query(db, q.query(), Some((0, 1)));
    }
    drift(db, "settle: the seats latched (counted under the identity)")?;
    let claim = claim_record(
        "c2",
        &ida,
        &idb,
        &p,
        &s,
        Some("0001020304"),
        &format!("c4{}", "0".repeat(62)),
    );
    run_query(
        db,
        d1d::result_write::result_insert_query(&claim, NOW as i64).query(),
        Some((11, 2)),
    );
    drift(db, "settle: the winner's claim (its hand)")?;
    let earlier = claim_record(
        "c2",
        &ida,
        &idb,
        &p,
        &s,
        Some("0d0e0f1011"),
        &format!("c5{}", "0".repeat(62)),
    );
    run_query(
        db,
        d1d::result_write::result_insert_query(&earlier, (NOW - 5) as i64).query(),
        Some((11, 1)),
    );
    drift(db, "settle: an earlier claim takes the hand")?;
    let relatch = d1d::result_write::result_relatch_query(&earlier);
    assert_eq!(
        relatch.claim_tier(),
        0,
        "an unsigned claim re-latches invalid"
    );
    run_query(db, relatch.query(), None);
    drift(db, "settle: the earlier claim relatched invalid")?;

    // A tie and a refund on counted wins: the classifier's verdict CAS.
    assert_eq!(
        db.execute(
            d1d::verdict_cas_sql(),
            rusqlite::params!["tie", spender(2), "coop", pot(2), 0, spender(2)],
        )
        .unwrap(),
        1,
        "the writer lands one row"
    );
    drift(db, "a tie")?;
    assert_eq!(
        db.execute(
            d1d::verdict_cas_sql(),
            rusqlite::params!["refund", spender(4), "tower-a", pot(4), 0, spender(4)],
        )
        .unwrap(),
        1,
        "the writer lands one row"
    );
    drift(db, "a refund")?;
    // The witness of an unproven final spend (g % 23 == 13), by the overlay's latch, then its removal (no production
    // writer deletes one).
    assert_eq!(
        db.execute(
            bsv_overlay_cloudflare::ops::NETWORK_SEEN_INSERT_SQL,
            [spender(13)],
        )
        .unwrap(),
        1,
        "the writer lands one row"
    );
    drift(db, "the witness")?;
    x(&format!(
        "DELETE FROM network_seen WHERE txid = '{}'",
        spender(13)
    ));
    drift(db, "the witness removed")?;
    // A relatch that unlatches the winner's seat (its re-latch query: an unsigned seat re-derives 0), and a poisoned
    // slot (two verified identities for one key; a hand-written copy of the seeded seat).
    let unlatched = seat_record("x", &pot(8), "x", &format!("b30{:061x}", 8));
    let q = d1d::potparty_write::potparty_relatch_query(&unlatched);
    assert!(!q.sig_valid());
    run_query(db, q.query(), None);
    drift(db, "a seat unlatched")?;
    x(&format!("INSERT INTO potparty_records (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, sigHex, seatSettlePubkey, seatSigHex, txid, outputIndex, createdAt, sigValid) SELECT '{}', opponentIdentity, gameId, potTxid, potVout, recoveryHeight, sigHex, seatSettlePubkey, seatSigHex, 'c6' || substr(txid, 3), 0, createdAt, 1 FROM potparty_records WHERE potTxid = '{}' AND txid LIKE 'b30%'", d1_board::identity(149), pot(10)));
    drift(db, "a poisoned slot")?;
    // A claim relatched invalid (its re-latch query).
    let relatched = claim_record(
        "x",
        "x",
        "y",
        &pot(20),
        "x",
        None,
        &format!("b4{:062x}", 20),
    );
    run_query(
        db,
        d1d::result_write::result_relatch_query(&relatched).query(),
        None,
    );
    drift(db, "a claim relatched")?;

    // bsv-low #674 fold: what a frag reads beside its pot. Four earlier markers push a counted pot's claim out of its
    // window (`RESULT_ROWS_PER_POT`); a proof pointer is admitted, moved to another key and removed; a posted bundle is
    // written, replaced invalid, invalidated and removed; a frag-only pot column (the spend height) moves.
    for k in 0..4u32 {
        let junk = claim_record(
            &game(16),
            &d1_board::identity(149),
            "x",
            &pot(16),
            &spender(16),
            None,
            &format!("c7{k}{}", "0".repeat(61)),
        );
        run_query(
            db,
            d1d::result_write::result_insert_query(&junk, 1).query(),
            None,
        );
    }
    drift(db, "four earlier markers on a counted pot")?;
    let proof_admit = |g: u32, txid: &str, at: i64| {
        db.execute(
            bsv_overlay_cloudflare::d1_discovery::PROOF_ADMIT_WRITE_SQL,
            rusqlite::params![
                game(g),
                winner(g),
                "00",
                vec![0u8],
                "AA==",
                txid,
                0,
                at,
                1,
                0,
                "x",
                "y",
                None::<String>,
                None::<String>
            ],
        )
        .unwrap();
    };
    let proof_txid = format!("c8{}", "0".repeat(62));
    proof_admit(24, &proof_txid, (NOW + 5) as i64);
    drift(db, "a proof pointer admitted")?;
    x(&format!(
        "UPDATE proof_markers SET gameId = '{}', winner = '{}' WHERE txid = '{proof_txid}'",
        game(21),
        winner(21)
    ));
    drift(
        db,
        "a proof pointer moved to another key (no production writer)",
    )?;
    // The eviction's shadow move does not name `proof_markers` (a published proof is permanent), so no production
    // writer deletes a pointer; the trigger keeps the derivation total.
    x(&format!(
        "DELETE FROM proof_markers WHERE txid = '{proof_txid}'"
    ));
    drift(db, "a proof pointer removed (no production writer)")?;
    let post = |g: u32, valid: i64| {
        db.execute(
            low_app_layer::proof_post::PROOF_POST_WRITE_SQL,
            rusqlite::params![
                game(g),
                winner(g),
                "00",
                vec![0u8],
                NOW as i64,
                valid,
                0,
                "x",
                "y",
                None::<String>,
                None::<String>
            ],
        )
        .unwrap();
    };
    post(24, 1);
    drift(db, "a posted bundle")?;
    post(24, 0);
    drift(db, "a posted bundle replaced invalid")?;
    post(24, 1);
    x(&format!(
        "UPDATE proof_posts SET bundleValid = 0 WHERE gameId = '{}'",
        game(24)
    ));
    drift(db, "a posted bundle invalidated (no production writer)")?;
    x(&format!(
        "DELETE FROM proof_posts WHERE gameId = '{}'",
        game(18)
    ));
    drift(db, "a posted bundle removed (no production writer)")?;
    assert!(run_cas(
        db,
        d1d::reanchor_confirmed_cas_sql(),
        rusqlite::params![900_000, pot(16), 0, spender(16)]
    ));
    drift(
        db,
        "a frag-only pot column (the spend height, the re-anchor CAS)",
    )?;

    // The eviction's shadow move: an identity-owned pot, a key-owned pot (g % 17 == 0), a seat marker alone, a
    // claim alone, and the witnessed final spender of a pot (its pointer released).
    let seat = format!("b30{:061x}", 26);
    let claim = format!("b4{:062x}", 28);
    for t in [pot(22), pot(34), seat.clone(), claim.clone()] {
        evict(db, &t);
        drift(db, &format!("evicted {}", &t[..8]))?;
    }
    evict(db, &spender(11));
    drift(db, "the settle spender evicted (its pointer released)")?;
    // The readmission of each.
    for t in [pot(22), pot(34), seat, claim] {
        readmit(db, &t, &[]);
        drift(db, &format!("readmitted {}", &t[..8]))?;
    }
    readmit(db, &spender(11), &[(pot(11), 0)]);
    // The spend recorded again with its verdict (unconfirmed, final), then the latch.
    assert_eq!(
        db.execute(
            d1d::mark_spent_sql(false, true),
            rusqlite::params![
                spender(11),
                "winner-b",
                spender(11),
                "coop",
                spender(11),
                1,
                1,
                pot(11),
                0
            ],
        )
        .unwrap(),
        1,
        "the writer lands one row"
    );
    // The latch again: the restore already brought the witness back, so `INSERT OR IGNORE` lands nothing.
    assert_eq!(
        db.execute(
            bsv_overlay_cloudflare::ops::NETWORK_SEEN_INSERT_SQL,
            [spender(11)]
        )
        .unwrap(),
        0
    );
    drift(db, "the settle spender readmitted, its verdict rewritten")?;

    // A REPLACE (which skips a delete trigger) still leaves the right row: the triggers recompute from the source.
    x(&format!("INSERT OR REPLACE INTO pot_records SELECT * FROM pot_records WHERE txid = '{}' AND outputIndex = 0", pot(40)));
    drift(db, "a REPLACE of a counted pot")?;
    x(&format!(
        "UPDATE pot_records SET pubB = pubA WHERE txid = '{}' AND outputIndex = 0",
        pot(44)
    ));
    drift(db, "a degenerate lock (pubA == pubB)")?;
    // A row moved to another pot (no production writer moves one; kept so the derivation is total): the OLD pot is
    // recomputed only on a move (lane W1-LB-674-NOMEM), so these are the paths that reach that branch.
    x(&format!(
        "UPDATE pot_records SET txid = '{}' WHERE txid = '{}' AND outputIndex = 0",
        "d0".repeat(32),
        pot(46)
    ));
    drift(db, "a counted pot moved to another txid")?;
    x(&format!(
        "UPDATE potparty_records SET potTxid = '{}' WHERE rowid IN (SELECT rowid FROM potparty_records WHERE potTxid = '{}' AND potVout = 0)",
        "d1".repeat(32),
        pot(47)
    ));
    drift(db, "a pot's seats moved to another pot")?;
    Ok(())
}

#[test]
fn lb674_drift_the_derived_board_equals_the_from_scratch_read_after_every_hook_path() {
    let db = schema_without(None);
    paths(&db).unwrap();
    let wins = rows(&db, "SELECT wins FROM board_total")[0][0].clone();
    println!("drift pin: every path green; {wins} counted wins at the end");
}

/// Each trigger, removed alone, turns the drift pin red: the set is complete and every trigger is needed.
#[test]
fn lb674_drift_each_trigger_removed_alone_is_red() {
    for (name, trigger) in [
        ("WIN_INSERT", board::WIN_INSERT),
        ("WIN_DELETE", board::WIN_DELETE),
        ("WIN_FRAG", board::WIN_FRAG),
        ("POT_FRAG", board::POT_FRAG),
        ("PROOF_INSERT", board::PROOF_INSERT),
        ("PROOF_UPDATE", board::PROOF_UPDATE),
        ("PROOF_DELETE", board::PROOF_DELETE),
        ("POST_INSERT", board::POST_INSERT),
        ("POST_UPDATE", board::POST_UPDATE),
        ("POST_DELETE", board::POST_DELETE),
        ("MARKER_INSERT", board::MARKER_INSERT),
        ("MARKER_UPDATE", board::MARKER_UPDATE),
        ("MARKER_DELETE", board::MARKER_DELETE),
        ("POT_INSERT", board::POT_INSERT),
        ("POT_UPDATE", board::POT_UPDATE),
        ("POT_DELETE", board::POT_DELETE),
        ("SEEN_INSERT", board::SEEN_INSERT),
        ("SEEN_DELETE", board::SEEN_DELETE),
        ("PARTY_INSERT", board::PARTY_INSERT),
        ("PARTY_UPDATE", board::PARTY_UPDATE),
        ("PARTY_DELETE", board::PARTY_DELETE),
    ] {
        let db = schema_without(Some(trigger));
        let err = paths(&db).expect_err(&format!("{name} removed: the drift pin stayed green"));
        println!("{name} removed: RED at {err}");
    }
}

/// The #674 block of the migration list (`WINS_CREATE` through `STATE_DONE`).
fn board_block() -> &'static [&'static str] {
    let start = OVERLAY_MIGRATIONS
        .iter()
        .position(|s| *s == board::WINS_CREATE)
        .unwrap();
    let end = OVERLAY_MIGRATIONS
        .iter()
        .position(|s| *s == board::STATE_DONE)
        .unwrap();
    &OVERLAY_MIGRATIONS[start..=end]
}

fn apply(db: &Connection, list: &[&str]) {
    for sql in list {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}: {sql}");
        }
    }
}

/// A database at migration 295 (every statement before the #674 block) holding the fleet-shaped board.
fn legacy_board(wins: u32) -> Connection {
    let db = Connection::open_in_memory().unwrap();
    let start = OVERLAY_MIGRATIONS
        .iter()
        .position(|s| *s == board::WINS_CREATE)
        .unwrap();
    apply(&db, &OVERLAY_MIGRATIONS[..start]);
    db.execute_batch("BEGIN").unwrap();
    for sql in d1_board::rows(NOW, wins) {
        db.execute_batch(&sql).unwrap();
    }
    db.execute_batch("COMMIT").unwrap();
    db
}

fn dump(db: &Connection) -> Vec<Vec<Vec<String>>> {
    [
        "SELECT * FROM board_wins ORDER BY potTxid",
        "SELECT * FROM board_owners ORDER BY owner",
        "SELECT h.potTxid, h.score, h.claimCreatedAt, h.gameId, m.txid FROM board_hands h LEFT JOIN result_markers_v2 m ON m.rowid = h.markerRowid ORDER BY h.potTxid",
        "SELECT * FROM board_total",
    ]
    .iter()
    .map(|q| rows(db, q))
    .collect()
}

/// The one-time backfill over a populated legacy database derives exactly what the triggers derive row by row,
/// and the stamp is written.
#[test]
fn lb674_backfill_over_a_legacy_board_equals_the_trigger_derivation() {
    let legacy = legacy_board(400);
    assert_eq!(
        rows(
            &legacy,
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'board_wins'"
        )[0][0],
        "0"
    );
    apply(&legacy, board_block());
    drift(&legacy, "the backfill").unwrap();
    let live = schema_without(None);
    live.execute_batch("BEGIN").unwrap();
    for sql in d1_board::rows(NOW, 400) {
        live.execute_batch(&sql).unwrap();
    }
    live.execute_batch("COMMIT").unwrap();
    assert_eq!(
        dump(&legacy),
        dump(&live),
        "backfill == triggers, byte for byte"
    );
    assert_eq!(
        rows(
            &legacy,
            "SELECT name FROM view_derivation_state WHERE name = 'board-v1'"
        )
        .len(),
        1
    );
    assert!(
        rows(&legacy, "SELECT wins FROM board_total")[0][0]
            .parse::<u32>()
            .unwrap()
            >= 400
    );
}

/// The deploy window: the triggers exist before the backfill runs, so writes between them are derived by the
/// triggers and the backfill (`INSERT OR IGNORE`) skips them; nothing is counted twice.
#[test]
fn lb674_writes_between_the_triggers_and_the_backfill_are_counted_once() {
    let db = legacy_board(200);
    let block = board_block();
    let backfill = block
        .iter()
        .position(|s| *s == board::WINS_BACKFILL)
        .unwrap();
    apply(&db, &block[..backfill]);
    // An old-version isolate writes meanwhile: a tie, a witness, a relatch, a new counted win.
    db.execute_batch(&format!(
        "UPDATE pot_records SET verdict = 'tie' WHERE txid = '{}' AND outputIndex = 0;
         INSERT INTO network_seen (txid, seenAt) VALUES ('{}', 1);
         UPDATE potparty_records SET sigValid = 0 WHERE potTxid = '{}';
         UPDATE pot_records SET spentConfirmed = 1 WHERE txid = '{}' AND outputIndex = 0;",
        pot(2),
        spender(13),
        pot(8),
        pot(7)
    ))
    .unwrap();
    let derived_early = rows(&db, "SELECT COUNT(*) FROM board_wins")[0][0].clone();
    assert_ne!(
        derived_early, "0",
        "the triggers derived the window's writes"
    );
    apply(&db, &block[backfill..]);
    drift(&db, "after the window").unwrap();
}

/// The stamped replay of the backfill steps no fleet row: the guard is in the OUTER derived table. The same
/// statement with the guard moved into the `WHERE` (the shape #638 refused) does step the fleet: the RED control.
#[test]
fn lb674_a_stamped_backfill_replay_steps_no_fleet_row() {
    let db = legacy_board(400);
    apply(&db, board_block());
    let steps = |sql: &str| {
        let mut stmt = db.prepare(sql).unwrap();
        assert_eq!(
            stmt.execute([]).unwrap(),
            0,
            "a stamped replay inserts nothing"
        );
        (
            stmt.get_status(rusqlite::StatementStatus::FullscanStep),
            stmt.get_status(rusqlite::StatementStatus::VmStep),
        )
    };
    let (scan, vm) = steps(board::WINS_BACKFILL);
    println!("stamped replay: fullscan steps {scan}, vm steps {vm}");
    assert_eq!(scan, 0, "a stamped replay must skip the fleet scan");
    assert!(vm < 100, "a stamped replay is a constant: {vm} vm steps");
    let guard =
        "(SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM view_derivation_state WHERE name='board-v1')) g CROSS JOIN ";
    assert!(board::WINS_BACKFILL.contains(guard));
    let in_where = board::WINS_BACKFILL.replace(guard, "").replacen(
        " WHERE p.outputIndex=0",
        " WHERE NOT EXISTS (SELECT 1 FROM view_derivation_state WHERE name='board-v1') AND p.outputIndex=0",
        1,
    );
    assert_ne!(in_where, board::WINS_BACKFILL);
    let (scan_where, vm_where) = steps(&in_where);
    println!("guard in the WHERE (RED control): fullscan steps {scan_where}, vm steps {vm_where}");
    assert!(
        vm_where > 10 * vm.max(1),
        "the control steps the fleet ({vm_where} vm steps)"
    );
    // The whole list re-run over the populated, stamped database changes nothing.
    let before = dump(&db);
    apply(&db, OVERLAY_MIGRATIONS);
    assert_eq!(dump(&db), before, "a re-run of the list is benign");
}

/// bsv-low #674 fold, LOW-1: the overlay's trigger scores a hand with `HAND_SCORE_SQL`, which must be the app
/// layer's own `sql_hand_score_expr("m.cardsHex")` byte for byte (the two crates cannot share the builder: the
/// overlay does not link the app layer), and every trigger that recomputes a hand carries it. With it the frag's
/// window constants: the markers a frag holds per pot are `RESULT_ROWS_PER_POT` and the proof pointers per key
/// `PROOF_POINTERS_PER_KEY`, as the page statements the frag stands in for read them. RED against a card scored one
/// point apart (a J/Q/K at 11 in `card_score!`): "the trigger's hand score is the app layer's".
#[test]
fn lb674_fold_l1_the_trigger_hand_score_is_the_app_layers() {
    assert_eq!(
        board::HAND_SCORE_SQL,
        low_app_layer::logic::sql_hand_score_expr("m.cardsHex"),
        "the trigger's hand score is the app layer's"
    );
    for (name, trigger) in [
        ("WIN_INSERT", board::WIN_INSERT),
        ("MARKER_INSERT", board::MARKER_INSERT),
        ("MARKER_UPDATE", board::MARKER_UPDATE),
        ("MARKER_DELETE", board::MARKER_DELETE),
    ] {
        assert!(
            trigger.contains(board::HAND_SCORE_SQL),
            "{name} scores its hand with HAND_SCORE_SQL"
        );
    }
    assert_eq!(
        board::FRAG_MARKERS,
        low_app_layer::logic::LEADERBOARD_RESULT_ROWS_PER_POT
    );
    assert_eq!(
        board::FRAG_POINTERS,
        low_app_layer::logic::PROOF_POINTERS_PER_KEY
    );
    let markers = format!(
        "FROM result_markers_v2 WHERE potTxid=p.txid ORDER BY createdAt ASC,rowid ASC LIMIT {})",
        board::FRAG_MARKERS
    );
    let pointers = format!(
        "ORDER BY q.createdAt DESC,q.rowid DESC LIMIT {})",
        board::FRAG_POINTERS
    );
    let frag_max = format!("length(fr.v) <= {} THEN", board::FRAG_MAX_BYTES);
    let card_max = format!("<= {} END;", board::CARD_MAX_BYTES);
    for (name, trigger) in [
        ("POT_INSERT", board::POT_INSERT),
        ("POT_FRAG", board::POT_FRAG),
        ("PROOF_INSERT", board::PROOF_INSERT),
        ("POST_INSERT", board::POST_INSERT),
    ] {
        for want in [&markers, &pointers, &frag_max] {
            assert!(trigger.contains(want.as_str()), "{name}: {want}");
        }
    }
    assert!(board::WIN_INSERT.contains(&card_max) && board::WIN_FRAG.contains(&card_max));
}

/// Lane W1-LB-674-NOMEM: what the board's triggers cost ONE statement, so the batched seed load of the route tiers
/// (`scripts/d1-load-chunked.py`, 2,000 statements a batch) cannot hide a heavier trigger. Two bounds, each measured
/// on a populated board whose one owner's card is at `CARD_MAX_BYTES`:
/// - the PREPARED program of a write to each source table (it carries every trigger the write fires; wrangler's local
///   `--file` holds a batch's programs together, and the whole fleet seed in one batch died `SQLITE_NOMEM` at about
///   1.1 GB of workerd RSS): at most `PREPARED_MAX` bytes (measured here, bundled SQLite: 109,040 for a
///   `network_seen` insert, 136,544 and 207,184 for a `pot_records` insert and update, 152,608 for a seat relatch);
/// - the BYTES a write costs: a write that moves no counted win is a few pages of WAL, and one that moves a packed
///   win rewrites its owner's card at most twice (`card_unpack!`, `card_pack!`: the lens's NOTE-2), so at most
///   `2 * CARD_MAX_BYTES` card bytes rewritten and that many plus `PAGES_SLACK` pages of WAL (measured 743 to 745
///   pages, about 3.05 MB, at the bound). RED on `f0c15b9`, whose `POT_UPDATE` recomputed the same pot as OLD and as
///   NEW (four rewrites): "a packed win's signers backfilled: 5991694 card bytes rewritten, past twice the card".
///   The seat relatch on a packed win's pot (the lens-3's LOW-1): 2,996,418 bytes, 742 pages; RED with
///   `PARTY_UPDATE`'s guard removed alone: "a seat on a packed win's pot relatched: 5992836 card bytes rewritten".
#[test]
fn lb674_nomem_the_triggers_prepare_and_write_within_their_bound() {
    const PREPARED_MAX: u64 = 256 * 1024;
    const PAGES_SLACK: u64 = 32;
    let path = std::env::temp_dir().join(format!("lb674-nomem-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let db = Connection::open(&path).unwrap();
    db.pragma_update(None, "journal_mode", "WAL").unwrap();
    db.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    apply(&db, OVERLAY_MIGRATIONS);
    let (pub_a, pub_b) = (
        format!("02{}", "ab".repeat(32)),
        format!("03{}", "cd".repeat(32)),
    );
    let win = |i: u32, confirmed: u32| {
        let (t, s) = (format!("b0b{i:061x}"), format!("5e7{i:061x}"));
        db.execute(
            "INSERT INTO pot_records (txid,outputIndex,spent,spendingTxid,createdAt,spentConfirmed,pubA,pubB,paramsDecoded,verdict,verdictTxid,spenderFinal,lockKind,potSats) VALUES (?1,0,1,?2,?3,?4,?5,?6,1,'winner-a',?2,1,'cov',1000)",
            rusqlite::params![t, s, 1000 + i, confirmed, pub_a, pub_b],
        )
        .unwrap();
        (t, s)
    };
    db.execute_batch("BEGIN").unwrap();
    let mut n = 0;
    loop {
        win(n, 1);
        n += 1;
        let (wins, packed): (u32, u32) = db
            .query_row(
                "SELECT wins, packed FROM board_owners WHERE owner = ?1",
                [&pub_a],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        if packed < wins {
            break;
        }
    }
    db.execute_batch("COMMIT").unwrap();
    let card: u64 = db
        .query_row(
            "SELECT length(card) FROM board_owners WHERE owner = ?1",
            [&pub_a],
            |r| r.get(0),
        )
        .unwrap();
    println!("the owner's card at the bound: {n} wins, {card} bytes");
    assert!(
        card as usize > board::CARD_MAX_BYTES * 98 / 100 && card as usize <= board::CARD_MAX_BYTES
    );

    // The prepared programs.
    for sql in [
        "INSERT INTO network_seen DEFAULT VALUES",
        "INSERT INTO pot_records DEFAULT VALUES",
        "UPDATE pot_records SET spent=1,spendingTxid='x',spentConfirmed=1,settleSigners='x',spentHeight=1,potSats=1 WHERE txid='x'",
        "DELETE FROM pot_records WHERE txid='x'",
        "INSERT INTO potparty_records DEFAULT VALUES",
        "UPDATE potparty_records SET sigValid=1 WHERE txid='x'",
        "INSERT INTO result_markers_v2 DEFAULT VALUES",
        "UPDATE result_markers_v2 SET claimValid=1 WHERE txid='x'",
        "INSERT INTO proof_markers DEFAULT VALUES",
        "INSERT INTO proof_posts DEFAULT VALUES",
    ] {
        let used = db
            .prepare(sql)
            .unwrap()
            .get_status(rusqlite::StatementStatus::MemUsed) as u64;
        println!("prepared {used:>7} bytes: {sql}");
        assert!(used <= PREPARED_MAX, "{sql}: {used} bytes prepared, past {PREPARED_MAX}");
    }

    // The bytes written.
    let page: u64 = db.query_row("PRAGMA page_size", [], |r| r.get(0)).unwrap();
    let wal = path.with_extension("db-wal");
    // A commit writes a page once however often the statement rewrote it, so the card's rewrites are counted apart.
    db.execute_batch(
        "CREATE TEMP TABLE card_writes (bytes INTEGER); CREATE TEMP TRIGGER card_audit AFTER UPDATE OF card ON main.board_owners BEGIN INSERT INTO card_writes VALUES (length(NEW.card)); END;",
    )
    .unwrap();
    let written = |label: &str, sql: &str, p: &[&dyn rusqlite::ToSql]| {
        db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
            .unwrap();
        db.execute("DELETE FROM card_writes", []).unwrap();
        let before = std::fs::metadata(&wal).unwrap().len();
        db.execute(sql, p).unwrap();
        let pages = (std::fs::metadata(&wal).unwrap().len() - before) / (page + 24);
        let rewritten: u64 = db
            .query_row("SELECT COALESCE(SUM(bytes), 0) FROM card_writes", [], |r| {
                r.get(0)
            })
            .unwrap();
        println!(
            "{label}: {pages} pages ({} bytes), card bytes rewritten {rewritten}",
            pages * page
        );
        assert!(
            rewritten <= 2 * board::CARD_MAX_BYTES as u64,
            "{label}: {rewritten} card bytes rewritten, past twice the card"
        );
        pages
    };
    let small = 64 * 1024 / page;
    let card_pages = 2 * board::CARD_MAX_BYTES as u64 / page + PAGES_SLACK;
    let (_, witnessed) = win(10_000_000, 0);
    let (packed_pot, packed_settle) = (format!("b0b{:061x}", 3), format!("5e7{:061x}", 3));
    let seen = |t: &str| t.to_string();
    for (label, sql, p, bound) in [
        (
            "a witness that moves no win",
            "INSERT INTO network_seen (txid, seenAt) VALUES (?1, 1)",
            vec![seen(&"7".repeat(64))],
            small,
        ),
        (
            "a witness that counts a win past the card",
            "INSERT INTO network_seen (txid, seenAt) VALUES (?1, 1)",
            vec![witnessed],
            small,
        ),
        (
            "an uncounted pot admitted",
            "INSERT INTO pot_records (txid, outputIndex, spent, createdAt) VALUES (?1, 0, 0, 1)",
            vec!["e".repeat(64)],
            small,
        ),
        (
            "a packed win's signers backfilled",
            "UPDATE pot_records SET settleSigners = 'ab' WHERE txid = ?1 AND outputIndex = 0",
            vec![packed_pot.clone()],
            card_pages,
        ),
        (
            "a packed win relatched to the same value",
            "UPDATE pot_records SET spentConfirmed = 1 WHERE txid = ?1 AND outputIndex = 0",
            vec![packed_pot.clone()],
            card_pages,
        ),
    ] {
        let params: Vec<&dyn rusqlite::ToSql> =
            p.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
        let pages = written(label, sql, &params);
        assert!(
            pages <= bound,
            "{label}: {pages} pages written, past {bound}"
        );
    }
    let pages = written(
        "a claim on a packed win",
        "INSERT INTO result_markers_v2 (gameId,winner,loser,potTxid,settleTxid,txid,outputIndex,createdAt,claimValid) VALUES ('g1',?1,?2,?3,?4,?5,0,5,1)",
        &[&pub_a, &pub_b, &packed_pot, &packed_settle, &"9".repeat(64)],
    );
    assert!(
        pages <= card_pages,
        "a claim on a packed win: {pages} pages written, past {card_pages}"
    );
    // The seat guard (the lens-3's LOW-1): a seat of the LOSER's key keeps the win on the packed owner's card, so its
    // relatch rewrites that card; recomputing the unmoved seat's pot as OLD and as NEW rewrote it four times.
    let seat = "5ea".repeat(21) + "0";
    let ida = d1_board::identity(3);
    for (label, sql, p) in [
        (
            "a seat filed on a packed win's pot",
            "INSERT INTO potparty_records (txid,outputIndex,potTxid,potVout,identity,opponentIdentity,gameId,recoveryHeight,seatSettlePubkey,sigValid,createdAt) VALUES (?1,0,?2,0,?3,'x','g1',1,?4,1,6)",
            vec![seat.clone(), packed_pot.clone(), ida, pub_b.clone()],
        ),
        (
            "a seat on a packed win's pot relatched",
            "UPDATE potparty_records SET sigValid = 1 WHERE txid = ?1",
            vec![seat.clone()],
        ),
    ] {
        let params: Vec<&dyn rusqlite::ToSql> =
            p.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
        let pages = written(label, sql, &params);
        assert!(
            pages <= card_pages,
            "{label}: {pages} pages written, past {card_pages}"
        );
    }
    let owner: String = db
        .query_row(
            "SELECT owner FROM board_wins WHERE potTxid = ?1",
            [&packed_pot],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(owner, pub_a, "the seated win stays on the packed card");
    drop(db);
    for ext in ["db", "db-wal", "db-shm"] {
        let _ = std::fs::remove_file(path.with_extension(ext));
    }
}

/// The shipped `POT_UPDATE` / `PARTY_UPDATE` with the OLD recompute's guard cut out, whatever it says: the
/// four-rewrite text of `f0c15b9`. Each trigger names its OLD pot in exactly two places (the delete, the insert).
fn unguarded(trigger: &str, old: &str) -> String {
    let mut out = trigger.to_string();
    for (from, to) in [
        (format!("potTxid={old} AND potVout=0"), ";"),
        (format!("p.txid={old}"), ") w) o"),
    ] {
        assert_eq!(out.matches(&from).count(), 1, "{from} in {trigger}");
        let start = out.find(&from).unwrap() + from.len();
        let end = start + out[start..].find(to).unwrap();
        assert!(
            !out[start..end].is_empty(),
            "the OLD recompute after {from} is unguarded"
        );
        out.replace_range(start..end, "");
    }
    out
}

fn schema_with(pot_update: &str, party_update: &str) -> Connection {
    let db = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        let sql = match *sql {
            s if s == board::POT_UPDATE => pot_update,
            s if s == board::PARTY_UPDATE => party_update,
            s => s,
        };
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}: {sql}");
        }
    }
    db
}

/// The lens-3's LOW-2: the two-rewrite triggers (the OLD pot recomputed only when the row moved) derive the same
/// bytes as the four-rewrite ones (`f0c15b9`'s text: the guard cut out), `board_wins` with its frags,
/// `board_owners` with its cards and `packed`, `board_hands` and `board_total`, after every write of one owner
/// whose card is past `CARD_MAX_BYTES`: a packed win backfilled and relatched; a win leaving and re-entering; a win
/// past the bound updated; a verdict flipped and back; a pot moved to a new txid, its `outputIndex` moved off 0 and
/// back, both in one update; a seat filed, unlatched and relatched, moved to another pot, moved with its key, its
/// `potVout` moved off 0 and back with its pot, its identity changed; a whole-table no-op update. RED against a
/// guard that never recomputes the OLD pot (`" AND 0"`): the moved pot's row stays where it was.
#[test]
fn lb674_fold3_old_and_new_triggers_derive_the_same_cards_at_the_bound() {
    let old = schema_with(
        &unguarded(board::POT_UPDATE, "OLD.txid"),
        &unguarded(board::PARTY_UPDATE, "OLD.potTxid"),
    );
    let new = schema_with(board::POT_UPDATE, board::PARTY_UPDATE);
    let scenario = |db: &Connection| -> Vec<(String, Vec<Vec<Vec<String>>>)> {
        let (pub_a, pub_b) = (
            format!("02{}", "ab".repeat(32)),
            format!("03{}", "cd".repeat(32)),
        );
        let t = |i: u32| format!("b0b{i:061x}");
        let ins = |i: u32| {
            db.execute(
                "INSERT INTO pot_records (txid,outputIndex,spent,spendingTxid,createdAt,spentConfirmed,pubA,pubB,paramsDecoded,verdict,verdictTxid,spenderFinal,lockKind,potSats) VALUES (?1,0,1,?2,?3,1,?4,?5,1,'winner-a',?2,1,'cov',1000)",
                rusqlite::params![t(i), format!("5e7{i:061x}"), 1000 + i, pub_a, pub_b],
            )
            .unwrap();
        };
        db.execute_batch("BEGIN").unwrap();
        let mut n = 0;
        loop {
            ins(n);
            n += 1;
            let (wins, packed): (u32, u32) = db
                .query_row(
                    "SELECT wins, packed FROM board_owners WHERE owner = ?1",
                    [&pub_a],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();
            if packed < wins {
                break;
            }
        }
        for k in 0..3 {
            ins(n + k);
        }
        db.execute_batch("COMMIT").unwrap();
        let seat = "e1".repeat(32);
        let steps = [
            format!("UPDATE pot_records SET settleSigners='ab' WHERE txid='{}' AND outputIndex=0", t(3)),
            format!("UPDATE pot_records SET spentConfirmed=1 WHERE txid='{}' AND outputIndex=0", t(4)),
            format!("UPDATE pot_records SET spentConfirmed=0 WHERE txid='{}' AND outputIndex=0", t(5)),
            format!("UPDATE pot_records SET settleSigners='cd' WHERE txid='{}' AND outputIndex=0", t(n + 1)),
            format!("UPDATE pot_records SET spentConfirmed=1 WHERE txid='{}' AND outputIndex=0", t(5)),
            format!("UPDATE pot_records SET verdict='winner-b' WHERE txid='{}' AND outputIndex=0", t(6)),
            format!("UPDATE pot_records SET verdict='winner-a' WHERE txid='{}' AND outputIndex=0", t(6)),
            format!("UPDATE pot_records SET txid='{}' WHERE txid='{}' AND outputIndex=0", "d0".repeat(32), t(7)),
            format!("UPDATE pot_records SET outputIndex=1 WHERE txid='{}' AND outputIndex=0", t(8)),
            format!("UPDATE pot_records SET outputIndex=0 WHERE txid='{}' AND outputIndex=1", t(8)),
            format!("UPDATE pot_records SET txid='{}', outputIndex=0 WHERE txid='{}' AND outputIndex=0", "d2".repeat(32), t(9)),
            format!("INSERT INTO potparty_records (txid,outputIndex,potTxid,potVout,identity,opponentIdentity,gameId,recoveryHeight,seatSettlePubkey,sigValid,createdAt) VALUES ('{seat}',0,'{}',0,'{}','x','g',1,'{pub_a}',1,1)", t(10), d1_board::identity(3)),
            format!("UPDATE potparty_records SET sigValid=0 WHERE txid='{seat}'"),
            format!("UPDATE potparty_records SET sigValid=1 WHERE txid='{seat}'"),
            format!("UPDATE potparty_records SET potTxid='{}' WHERE txid='{seat}'", t(11)),
            format!("UPDATE potparty_records SET potTxid='{}', seatSettlePubkey='{}' WHERE txid='{seat}'", t(12), pub_a.to_uppercase()),
            format!("UPDATE potparty_records SET potVout=1 WHERE txid='{seat}'"),
            format!("UPDATE potparty_records SET potVout=0, potTxid='{}' WHERE txid='{seat}'", t(13)),
            format!("UPDATE potparty_records SET identity='{}' WHERE txid='{seat}'", d1_board::identity(5)),
            "UPDATE pot_records SET spentConfirmed=spentConfirmed".to_string(),
        ];
        let mut out = vec![("the card past its bound".to_string(), dump(db))];
        for s in steps {
            db.execute_batch(&s).unwrap_or_else(|e| panic!("{e}: {s}"));
            out.push((s.clone(), dump(db)));
        }
        out
    };
    let (old, new) = (scenario(&old), scenario(&new));
    assert_eq!(old.len(), new.len());
    for ((step, a), (_, b)) in old.iter().zip(&new) {
        assert!(
            a == b,
            "{step}: the two-rewrite triggers derived other bytes"
        );
    }
    println!("{} states byte-identical", old.len());
}

/// bsv-low #470 PRE557 (the lens's NOTES, after the rebase onto #674): the stamp step's SHIPPED statement moves a
/// counted win's `createdAt` to its block's time, and `board_pot_update` moves the derived `board_wins` row the
/// same way (its `potCreatedAt`, the era's anchor on the board), so the derived board still equals the from-scratch
/// read; a stamp inside the slack moves neither.
#[test]
fn pre557_a_stamp_moves_the_derived_board_as_the_era_filter_reads_it() {
    let db = schema_without(None);
    db.execute_batch("BEGIN").unwrap();
    for sql in d1_board::rows(NOW, 160) {
        db.execute_batch(&sql).unwrap();
    }
    db.execute_batch("COMMIT").unwrap();
    drift(&db, "the seeded board").unwrap();
    let won = rows(
        &db,
        "SELECT potTxid, potCreatedAt FROM board_wins ORDER BY potTxid LIMIT 2",
    );
    assert_eq!(won.len(), 2, "two counted wins");
    let slack = overlay_discovery::pot::storage::BLOCK_TIME_STAMP_SLACK_SECS as i64;
    let (moved, kept) = (&won[0], &won[1]);
    let created = |txid: &str| -> i64 {
        rows(
            &db,
            &format!("SELECT createdAt FROM pot_records WHERE txid = '{txid}' AND outputIndex = 0"),
        )[0][0]
            .parse()
            .unwrap()
    };
    // a re-indexed win: admitted long after its block, moved to the block's time
    let block = created(&moved[0]) - 30 * 86_400;
    assert!(
        db.execute(
            d1d::POT_BLOCK_TIME_STAMP_SQL,
            rusqlite::params![block, slack, moved[0]],
        )
        .unwrap()
            >= 1,
        "every row of the pot's txid is stamped"
    );
    // a live win: admitted inside the slack of its block, kept
    let live_block = created(&kept[0]) - slack;
    db.execute(
        d1d::POT_BLOCK_TIME_STAMP_SQL,
        rusqlite::params![live_block, slack, kept[0]],
    )
    .unwrap();
    let after = |txid: &str| {
        rows(
            &db,
            &format!("SELECT potCreatedAt FROM board_wins WHERE potTxid = '{txid}'"),
        )[0][0]
            .clone()
    };
    assert_eq!(
        after(&moved[0]),
        block.to_string(),
        "the board's anchor is the block's time"
    );
    assert_eq!(
        after(&kept[0]),
        kept[1],
        "a live win keeps its admission time"
    );
    drift(&db, "after the stamps").unwrap();
}
