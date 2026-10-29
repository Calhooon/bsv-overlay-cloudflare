//! The existing views' projections and folds, with the subject bound at the source.
use super::*;
use crate::{hops_view, owed, results, routes};
use serde::de::DeserializeOwned;

/// bsv-low #937: the result pots under an OPEN eviction marker (`pot_evictions`' primary key, one probe a pot).
pub const OPEN_EVICTED_POTS_SQL: &str = "SELECT txid FROM pot_evictions WHERE readmittedAt IS NULL AND txid IN (SELECT value FROM json_each(?1))";
// The game index is an additive schema dependency, pinned with the migrations.
pub const PARTIES_SQL: &str = "SELECT pp.*, pp.rowid AS markerRowid, r.createdAt AS potCreatedAt, r.txid IS NULL AS unknownPot FROM potparty_records pp INDEXED BY idx_potparty_game_identity LEFT JOIN pot_records r ON r.txid=pp.potTxid AND r.outputIndex=pp.potVout WHERE pp.identity=?1 AND pp.gameId IN (SELECT value FROM json_each(?2))";
pub const LINKS_SQL: &str = "SELECT hp.identity,hp.opponentIdentity,hp.gameId,l.potTxid,l.potVout,p.recoveryHeight,hp.seatSettlePubkey,hp.seatSigHex,hp.identitySigHex AS sigHex,hp.markerValid AS sigValid,hp.createdAt,hp.rowid AS markerRowid,p.createdAt AS potCreatedAt,0 AS unknownPot FROM hopparty_records hp INDEXED BY idx_hopparty_game CROSS JOIN hop_pot_links l ON l.markerRowid=hp.rowid CROSS JOIN pot_records p ON p.txid=l.potTxid AND p.outputIndex=l.potVout WHERE hp.identity=?1 AND hp.gameId IN (SELECT value FROM json_each(?2))";
/// Read all competitors for only the named pots, including hop-derived party rows.
/// This is the same rank and oldest-marker rule as `owed_judge::rank::select`.
pub const REPRESENTATIVES_SQL: &str = "SELECT pp.identity,pp.opponentIdentity,pp.gameId,pp.potTxid,pp.potVout,pp.recoveryHeight,pp.seatSettlePubkey,pp.seatSigHex,pp.sigHex,pp.createdAt,pp.txid,pp.outputIndex,pp.sigValid,pp.rowid AS markerRowid,r.createdAt AS potCreatedAt,r.txid IS NULL AS unknownPot FROM json_each(?2) k CROSS JOIN potparty_records pp INDEXED BY idx_potparty_pot ON pp.potTxid=json_extract(k.value,'$[0]') AND pp.potVout=json_extract(k.value,'$[1]') LEFT JOIN pot_records r ON r.txid=pp.potTxid AND r.outputIndex=pp.potVout WHERE pp.identity=?1 UNION ALL SELECT hp.identity,hp.opponentIdentity,hp.gameId,l.potTxid,l.potVout,p.recoveryHeight,hp.seatSettlePubkey,hp.seatSigHex,hp.identitySigHex AS sigHex,hp.createdAt,hp.txid,hp.outputIndex,hp.markerValid AS sigValid,hp.rowid AS markerRowid,p.createdAt AS potCreatedAt,0 AS unknownPot FROM json_each(?2) k CROSS JOIN hop_pot_links l INDEXED BY idx_hop_pot_links_pot ON l.potTxid=json_extract(k.value,'$[0]') AND l.potVout=json_extract(k.value,'$[1]') CROSS JOIN hopparty_records hp ON hp.rowid=l.markerRowid CROSS JOIN pot_records p ON p.txid=l.potTxid AND p.outputIndex=l.potVout WHERE l.identity=?1";
pub const HOPS_SQL: &str = "SELECT hp.*,hp.txid AS hopTxid,hp.txid AS markerTxid,hp.outputIndex AS markerVout,hp.createdAt AS markerCreatedAt,hp.rowid AS markerRowid,r.createdAt AS potCreatedAt,r.txid IS NULL AS unknownHop,r.spent,r.spendingTxid,r.spentConfirmed,r.spenderFinal,CASE WHEN hp.hopLockHex IS NOT NULL AND hp.hopSatsOnChain=hp.hopSats THEN 0 ELSE 1 END AS paidTier FROM hopparty_records hp INDEXED BY idx_hopparty_game LEFT JOIN pot_records r ON r.txid=hp.txid AND r.outputIndex=hp.hopVout WHERE hp.identity=?1 AND hp.gameId IN (SELECT value FROM json_each(?2))";
/// The identity's OTHER markers naming the selected games' hop outputs (the D2-OWED lens's MEDIUM-3): a hop output
/// its author named under two games is ONE row, the walk's (`owed_judge::rank::select_hops`, the oldest marker); the
/// projection is [`HOPS_SQL`]'s.
pub const HOP_COMPETITORS_SQL: &str = "SELECT hp.*,hp.txid AS hopTxid,hp.txid AS markerTxid,hp.outputIndex AS markerVout,hp.createdAt AS markerCreatedAt,hp.rowid AS markerRowid,r.createdAt AS potCreatedAt,r.txid IS NULL AS unknownHop,r.spent,r.spendingTxid,r.spentConfirmed,r.spenderFinal,CASE WHEN hp.hopLockHex IS NOT NULL AND hp.hopSatsOnChain=hp.hopSats THEN 0 ELSE 1 END AS paidTier FROM json_each(?2) k CROSS JOIN hopparty_records hp INDEXED BY idx_hopparty_hop ON hp.txid=json_extract(k.value,'$[0]') AND hp.hopVout=json_extract(k.value,'$[1]') LEFT JOIN pot_records r ON r.txid=hp.txid AND r.outputIndex=hp.hopVout WHERE hp.identity=?1";
/// The evidence the judge honours (D2-OWED-FOLD-10, the sixth delta lens's NOTE-1): an evidence row under its OPEN
/// ledger at its own `evictedAt` (`h` the evidence row, `he` its ledger row). ONE predicate, read by [`RELEASED_SQL`]
/// and by [`MISSING_SQL`]'s exclusion, so the two cannot drift: both are built from it, each naming its tables and
/// the join in its own literal (the ownership checker reads a table where its literal names it), and the whole joined
/// predicate is pinned equal by text (`fold10_n1`).
macro_rules! honoured_evidence {
    () => {
        "he.readmittedAt IS NULL AND h.evictedAt=he.evictedAt"
    };
}
pub const HONOURED_EVIDENCE: &str = honoured_evidence!();
/// D2-OWED-FOLD-8: the identity's missing-evidence rows of its games under an open eviction, with the opponent its
/// verified twin party names (the old walk's seat), less the outpoints its own evidence names (D2-OWED-FOLD-9, the
/// fifth delta lens's NOTE-B: evidence the judge honours, as [`RELEASED_SQL`] reads it: under its open ledger at its
/// own `evictedAt`, so a readmitted eviction's row hides no later eviction's missing row).
pub const MISSING_SQL: &str = concat!(
    "SELECT m.hopTxid,m.hopVout,m.gameId,m.potTxid,m.evictedAt,m.bytesRead,(SELECT lower(pp.opponentIdentity) FROM potparty_records_evicted pp INDEXED BY idx_evicted_party_identity_game WHERE pp.identity=m.identity AND pp.gameId=m.gameId AND pp.potTxid=m.potTxid AND pp.sigValid=1 AND pp.seatSettlePubkey IS NOT NULL ORDER BY pp.createdAt,pp.rowid LIMIT 1) AS opponentIdentity FROM released_hop_evidence_missing m JOIN pot_evictions e ON e.txid=m.potTxid WHERE m.identity=?1 AND m.gameId IN (SELECT value FROM json_each(?2)) AND e.readmittedAt IS NULL AND m.evictedAt=e.evictedAt AND NOT EXISTS (SELECT 1 FROM released_hop_evidence h JOIN pot_evictions he ON he.txid=h.potTxid WHERE ",
    honoured_evidence!(),
    " AND h.hopTxid=m.hopTxid AND h.hopVout=m.hopVout AND h.identity=m.identity) AND NOT EXISTS (SELECT 1 FROM potparty_records p INDEXED BY idx_potparty_identity WHERE p.identity=m.identity AND p.potTxid=m.hopTxid AND p.potVout=m.hopVout) AND NOT EXISTS (SELECT 1 FROM hopparty_records hp INDEXED BY idx_hopparty_hop WHERE hp.txid=m.hopTxid AND hp.hopVout=m.hopVout AND hp.identity=m.identity AND hp.markerValid=1)"
);
/// The evidence rows of one hop the judge honours ([`HONOURED_EVIDENCE`]).
pub const RELEASED_SQL: &str = concat!(
    "SELECT h.* FROM released_hop_evidence h JOIN pot_evictions he ON he.txid=h.potTxid WHERE ",
    honoured_evidence!(),
    " AND h.hopTxid=?1 AND h.hopVout=?2"
);

pub(super) async fn read<T: DeserializeOwned>(
    db: &impl SqlPort,
    sql: impl Into<String>,
    binds: Vec<Bind>,
) -> Result<Vec<T>, String> {
    let mut out = db
        .run_batch(vec![Stmt {
            sql: sql.into(),
            binds,
        }])
        .await?;
    if out.len() != 1 {
        return Err("judge read returned the wrong statement count".into());
    }
    out.remove(0)
        .rows
        .into_iter()
        .map(|r| serde_json::from_value(r).map_err(|e| format!("judge row: {e}")))
        .collect()
}
fn text(v: &Value, k: &str) -> String {
    v[k].as_str().unwrap_or_default().to_ascii_lowercase()
}
fn num(v: &Value, k: &str) -> u64 {
    crate::owed_judge::rank::number(v, k)
        .unwrap_or_default()
        .max(0) as u64
}
fn flag(v: &Value, k: &str) -> Option<bool> {
    crate::owed_judge::rank::number(v, k).map(|n| n != 0)
}
fn pair_binds(pairs: &[(String, u32)]) -> Vec<Bind> {
    pairs
        .iter()
        .flat_map(|(t, v)| [Bind::Text(t.clone()), Bind::Int(i64::from(*v))])
        .collect()
}
fn strings(v: impl IntoIterator<Item = String>) -> Vec<Bind> {
    v.into_iter().map(Bind::Text).collect()
}
fn json_bind<T: serde::Serialize>(v: &T) -> Bind {
    Bind::Text(serde_json::to_string(v).expect("serializable query keys"))
}

/// Hydrate the selected games and the canonical representatives of their pots.
/// An outside representative is JOIN-retirement context, never an emitted row.
/// No cursor, identity-history walk, or courier request is used.
pub async fn hydrate_inputs(
    db: &impl JudgeDb,
    identity: &str,
    games: &[String],
) -> Result<JudgeInputs, String> {
    hydrate_filings(db, identity, games, &[], &[], 0).await
}

/// Pending markers enter the existing projections before assembly, while their INSERTs remain uncommitted.
pub(crate) async fn hydrate_filings(
    db: &impl JudgeDb,
    identity: &str,
    games: &[String],
    filings: &[crate::record_post::VerifiedRecord],
    pending_hops: &[Value],
    stamp: i64,
) -> Result<JudgeInputs, String> {
    let mut i = JudgeInputs {
        identity: identity.into(),
        tip: db.tip(),
        now_ms: db.now_ms() as i64,
        ..Default::default()
    };
    // TIP-877 (main's rule, the D2-OWED lens's MEDIUM-2): no tip is no refusal. The derivation withholds every
    // tip-dependent claim (`tipUnknown`), proof-decided rows advance, the rows store `probeTip` NULL, and the next tip
    // re-marks them (`MARK_UNTIPPED_SQL`).
    let bound = vec![Bind::Text(identity.into()), json_bind(&games)];
    let mut parties: Vec<Value> = read(db, PARTIES_SQL, bound.clone()).await?;
    parties.extend(read::<Value>(db, LINKS_SQL, bound.clone()).await?);
    let mut pending_parties = Vec::new();
    for filing in filings {
        if let crate::record_post::VerifiedRecord::Potparty(r, valid) = filing {
            if r.identity == identity && games.contains(&r.game_id) {
                pending_parties.push(json!({"identity":r.identity,"opponentIdentity":r.opponent_identity,"gameId":r.game_id,"potTxid":r.pot_txid,"potVout":r.pot_vout,"recoveryHeight":r.recovery_height,"seatSettlePubkey":r.seat_settle_pubkey,"seatSigHex":r.seat_sig_hex,"sigHex":r.sig_hex,"createdAt":stamp,"markerRowid":i64::MAX,"txid":r.txid,"outputIndex":0,"sigValid":i64::from(*valid)}));
            }
        }
    }
    parties.extend(pending_parties.clone());
    let pots: BTreeSet<_> = parties
        .iter()
        .map(|p| (text(p, "potTxid"), num(p, "potVout") as u32))
        .collect();
    if !pots.is_empty() {
        let mut representatives: Vec<Value> = read(
            db,
            REPRESENTATIVES_SQL,
            vec![Bind::Text(identity.into()), json_bind(&pots)],
        )
        .await?;
        for mut pending in pending_parties {
            if representatives
                .iter()
                .any(|r| r["txid"] == pending["txid"] && r["outputIndex"] == pending["outputIndex"])
            {
                continue;
            }
            let pot: Vec<Value> = read(
                db,
                "SELECT createdAt FROM pot_records WHERE txid=?1 AND outputIndex=?2",
                vec![
                    Bind::Text(text(&pending, "potTxid")),
                    Bind::Int(num(&pending, "potVout") as i64),
                ],
            )
            .await?;
            pending["unknownPot"] = json!(i64::from(pot.is_empty()));
            pending["potCreatedAt"] = pot.first().map_or(Value::Null, |p| p["createdAt"].clone());
            representatives.push(pending);
        }
        parties = crate::owed_judge::rank::select(representatives, db.era_ms(), usize::MAX);
        for p in &parties {
            if text(p, "identity") == identity && flag(p, "sigValid") == Some(true) {
                i.verified_pots.insert(owed::outpoint_key(
                    &text(p, "potTxid"),
                    num(p, "potVout") as u32,
                ));
            }
        }
        // Keep a representative outside the requested games as bounded pot
        // context for JOIN retirement; judge_with_write emits only its subjects.
    }
    let mut result_rows = Vec::new();
    let mut refund_rows = Vec::new();
    for chunk in parties.chunks(crate::owed_judge::rank::PAGE) {
        let values: Vec<Value> = read(
            db,
            crate::owed_judge::rank::hydrate_party_pair_sql(),
            vec![json_bind(&chunk)],
        )
        .await?;
        for mut v in values {
            if filings.iter().any(|f| matches!(f, crate::record_post::VerifiedRecord::Potrefund(r, _) if r.pot_txid == text(&v,"potTxid") && u64::from(r.pot_vout) == num(&v,"potVout"))) {
                v["backupMarkerPresent"] = json!(1);
            }
            if v["fundingHeld"] == owed::FUNDING_HELD_NONE {
                i.funding_unheld.insert(text(&v, "potTxid"));
            }
            if v["fundingHeld"] == owed::FUNDING_HELD_SEEN {
                i.funding_seen.insert(text(&v, "potTxid"));
            }
            result_rows.push(
                serde_json::from_value::<routes::ResultsRowD1>(v.clone())
                    .map_err(|e| e.to_string())?
                    .into_row(),
            );
            v["spenderProofVerified"] = v["refundSpenderProofVerified"].clone();
            refund_rows.push(
                serde_json::from_value::<routes::RefundViewRowD1>(v)
                    .map_err(|e| e.to_string())?
                    .into_row(),
            );
        }
    }
    let mut hop_candidates: Vec<Value> = read(db, HOPS_SQL, bound.clone()).await?;
    let outputs: BTreeSet<(String, u64)> = hop_candidates
        .iter()
        .map(|h| (text(h, "hopTxid"), num(h, "hopVout")))
        .collect();
    if !outputs.is_empty() {
        for c in read::<Value>(
            db,
            HOP_COMPETITORS_SQL,
            vec![Bind::Text(identity.into()), json_bind(&outputs)],
        )
        .await?
        {
            if !hop_candidates
                .iter()
                .any(|h| h["txid"] == c["txid"] && h["outputIndex"] == c["outputIndex"])
            {
                hop_candidates.push(c);
            }
        }
    }
    for hop in pending_hops
        .iter()
        .filter(|h| text(h, "identity") == identity && games.contains(&text(h, "gameId")))
    {
        if !hop_candidates
            .iter()
            .any(|r| r["txid"] == hop["txid"] && r["outputIndex"] == hop["outputIndex"])
        {
            let mut hop = hop.clone();
            let rows: Vec<Value> = read(db,"SELECT createdAt AS potCreatedAt,spent,spendingTxid,spentConfirmed,spenderFinal FROM pot_records WHERE txid=?1 AND outputIndex=?2",vec![Bind::Text(text(&hop,"hopTxid")),Bind::Int(num(&hop,"hopVout") as i64)]).await?;
            hop["unknownHop"] = json!(i64::from(rows.is_empty()));
            if let Some(row) = rows.first().and_then(Value::as_object) {
                hop.as_object_mut()
                    .ok_or("pending hop is not an object")?
                    .extend(row.clone());
            }
            hop_candidates.push(hop);
        }
    }
    let hop_candidates =
        crate::owed_judge::rank::select_hops(hop_candidates, db.era_ms(), i.now_ms / 1000);
    let mut hop_rows = Vec::new();
    for chunk in hop_candidates.chunks(crate::owed_judge::rank::PAGE) {
        let rows: Vec<routes::HopsViewRowD1> = read(
            db,
            crate::owed_judge::rank::hydrate_hops_sql(),
            vec![json_bind(&chunk)],
        )
        .await?;
        hop_rows.extend(rows.into_iter().map(routes::HopsViewRowD1::into_row));
    }
    let words: Vec<_> = result_rows
        .iter()
        .filter_map(|r| r.spending_txid.clone())
        .chain(hop_rows.iter().filter_map(|r| r.spending_txid.clone()))
        .chain(i.funding_unheld.iter().cloned())
        .chain(i.funding_seen.iter().cloned())
        .collect();
    read_words(db, &mut i, &words).await?;
    i.funding_unheld
        .extend(owed::refused_fundings(&i.funding_seen, &i.spender_words));
    owed::apply_word_arm(
        &mut result_rows,
        &mut refund_rows,
        &mut hop_rows,
        &i.spender_words,
    );
    i.results = assemble_results(db, identity, result_rows, games, filings, stamp).await?;
    i.refunds = crate::refund_view::assemble_refund_view(refund_rows, i.tip);
    // The view assembler's page limit belongs to reads, not a game's judgement.
    for chunk in hop_rows.chunks(crate::hops_view::HOPS_VIEW_MAX_OUTPOINTS) {
        i.hops
            .extend(hops_view::assemble_hops_view(chunk.to_vec()).0);
    }
    for e in &i.results {
        if let Some(pkh) = committed_home(e) {
            i.my_pkh_by_game.insert(e.game_id.clone(), pkh);
        }
    }
    // The eviction writer already verified identity, seat commitment, and the P2PKH output.
    let released: Vec<Value> = read(db,"SELECT DISTINCT hopTxid,hopVout FROM released_hop_evidence INDEXED BY idx_released_hop_evidence_identity_game WHERE identity=?1 AND gameId IN (SELECT value FROM json_each(?2))",bound.clone()).await?;
    let mut released_rows = Vec::new();
    for key in released {
        for mut r in read::<Value>(
            db,
            RELEASED_SQL,
            vec![
                Bind::Text(text(&key, "hopTxid")),
                Bind::Int(num(&key, "hopVout") as i64),
            ],
        )
        .await?
        {
            if text(&r, "identity") != identity || !games.contains(&text(&r, "gameId")) {
                continue;
            }
            // the opponent the twin party names, as the old walk's ring served it (the walk-against-judge pin)
            let context: Vec<Value> = read(db,"SELECT h.createdAt AS potCreatedAt,h.spent,h.spendingTxid,h.spentConfirmed,h.spenderFinal,p.pubA,p.pubB,p.payPkhA,p.payPkhB,(SELECT lower(pp.opponentIdentity) FROM potparty_records_evicted pp INDEXED BY idx_evicted_party_identity_game WHERE pp.identity=?5 AND pp.gameId=?6 AND pp.potTxid=?3 AND pp.sigValid=1 ORDER BY pp.createdAt,pp.rowid LIMIT 1) AS opponentIdentity FROM (SELECT 1) LEFT JOIN pot_records h ON h.txid=?1 AND h.outputIndex=?2 LEFT JOIN pot_records_evicted p ON p.txid=?3 AND p.lockKind='covenant' AND p.paramsDecoded=1 AND (p.pubA=?4 OR p.pubB=?4)",vec![Bind::Text(text(&r,"hopTxid")),Bind::Int(num(&r,"hopVout") as i64),Bind::Text(text(&r,"potTxid")),Bind::Text(text(&r,"seatSettlePubkey")),Bind::Text(text(&r,"identity")),Bind::Text(text(&r,"gameId"))]).await?;
            if let Some(context) = context.first().and_then(Value::as_object) {
                r.as_object_mut()
                    .ok_or("released hop is not an object")?
                    .extend(context.clone());
            }
            // D2-ACTOR-B-FOLD-11 (bsv-low #957): the era's rule on this road too (the hop's own pot row, else the
            // eviction's time): an old-era released hop is written off as its marker is, never a press (beta's
            // 20,190 sat `hop-stranded` row of an Oct 21 hop the marker road had written off).
            if era_stamped_before(&r, db.era_ms()) {
                continue;
            }
            released_rows.push(r);
        }
    }
    for r in released_rows {
        let hop = text(&r, "hopTxid");
        let vout = num(&r, "hopVout") as u32;
        let key = owed::outpoint_key(&hop, vout);
        i.evicted_hop_outpoints.insert(key.clone());
        i.evicted_pots.insert(text(&r, "potTxid"));
        let seat = text(&r, "seatSettlePubkey");
        let game = text(&r, "gameId");
        let pkh = if seat == text(&r, "pubA") {
            text(&r, "payPkhA")
        } else if seat == text(&r, "pubB") {
            text(&r, "payPkhB")
        } else {
            String::new()
        };
        if !pkh.is_empty() {
            i.my_pkh_by_game.entry(game.clone()).or_insert(pkh);
        }
        let (status, source) = hops_view::derive_hop_status(
            flag(&r, "spent"),
            flag(&r, "spentConfirmed"),
            None,
            None,
            None,
        );
        let h = hops_view::HopEntry {
            game_id: game,
            hop_txid: hop.clone(),
            hop_vout: vout,
            hop_sats: num(&r, "hopSats"),
            opponent_identity: text(&r, "opponentIdentity"),
            seat_settle_pubkey: seat,
            seat_sig_hex: String::new(),
            identity_sig_hex: String::new(),
            marker_txid: hop,
            marker_vout: vout,
            spent: flag(&r, "spent"),
            spending_txid: r["spendingTxid"].as_str().map(str::to_owned),
            spent_confirmed: flag(&r, "spentConfirmed"),
            status,
            status_source: source,
            marker_verified: hops_view::MarkerVerification::Verified,
            marker_created_at: crate::owed_judge::rank::number(&r, "evictedAt"),
            batch: None,
        };
        // the old walk's isolate count of the planted markers verified evidence replaced (`/health.owed`)
        owed::note_markerless_ignored_markers(owed::merge_markerless_hops(&mut i.hops, vec![h]));
    }
    // D2-OWED-FOLD-8: the released hops of an evicted JOIN whose bytes do not read, as the old walk faulted them; an
    // outpoint the identity's own evidence names (a released-hop evidence row, its pot, its verified hop marker) is
    // judged from that evidence instead.
    for r in read::<Value>(db, MISSING_SQL, bound.clone()).await? {
        // FOLD-11 (#957): an old-era eviction's could-not-judge row is written off with it
        if era_stamped_before(&r, db.era_ms()) {
            continue;
        }
        let hop = text(&r, "hopTxid");
        let vout = num(&r, "hopVout") as u32;
        let reason = if flag(&r, "bytesRead") == Some(true) {
            "evidence-missing: the evicted JOIN's stored outputs are absent or undecodable"
        } else {
            "evidence-missing: the evicted JOIN's stored bytes are absent or undecodable"
        };
        owed::note_markerless_read_fault();
        i.markerless_faults.push(owed::OwedRow {
            identity: identity.to_owned(),
            outpoint: owed::outpoint_key(&hop, vout),
            family: owed::OwedFamily::Unbound,
            game_id: text(&r, "gameId"),
            sats: None,
            opponent_identity: r["opponentIdentity"].as_str().map(str::to_owned),
            at_height: None,
            facts: json!({"markerlessEvidence":true,"markerlessJoin":text(&r,"potTxid"),
                "markerlessReadFault":true,"evidenceMissing":true,
                "hopTxid":hop,"hopVout":vout,"claim":null,"claimable":false,
                "markerCreatedAtMs":num(&r,"evictedAt") as i64}),
            reason: Some(format!("could not judge: {reason}")),
        });
    }
    // bsv-low #937: EVERY result's pot is asked of the ledger, its row present or not: a `pot_records` row written
    // after its pot's OPEN marker (the gated arm's write past its answer deadline, L52 pair 13) is not held, and
    // `owed::derive_owed_rows` drops an evicted pot whatever its row says (#518). Asked only of the row-less pots
    // before, so such a row read "unspent" until the write-side guard moved it. One statement, as before; one
    // primary-key probe per result pot.
    let unknown: Vec<_> = i.results.iter().map(|r| r.pot_txid.clone()).collect();
    if !unknown.is_empty() {
        let rows: Vec<Value> = read(db, OPEN_EVICTED_POTS_SQL, vec![json_bind(&unknown)]).await?;
        i.evicted_pots.extend(rows.iter().map(|r| text(r, "txid")));
    }
    hydrate_evidence(db, &mut i, games, filings).await?;
    Ok(i)
}

async fn read_words(db: &impl JudgeDb, i: &mut JudgeInputs, ids: &[String]) -> Result<(), String> {
    if ids.is_empty() {
        return Ok(());
    }
    let rows: Vec<crate::tracker_host::stored::Row> =
        read(db, crate::tracker_host::stored::READ, vec![json_bind(&ids)]).await?;
    let rows: BTreeMap<_, _> = rows.into_iter().map(|r| (r.txid.clone(), r)).collect();
    i.spender_words.extend(db.word_views(&rows).await?);
    i.word_rows.extend(rows);
    Ok(())
}

async fn assemble_results(
    db: &impl JudgeDb,
    identity: &str,
    mut rows: Vec<results::ResultsRow>,
    games: &[String],
    filings: &[crate::record_post::VerifiedRecord],
    stamp: i64,
) -> Result<Vec<results::ResultEntry>, String> {
    let pairs: Vec<_> = rows
        .iter()
        .map(|r| (r.pot_txid.clone(), r.pot_vout))
        .collect();
    if pairs.is_empty() {
        return Ok(Vec::new());
    }
    for chunk in pairs.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
        let overlay: Vec<results::PageOverlay> = read(
            db,
            results::page_overlay_sql(chunk.len()),
            pair_binds(chunk),
        )
        .await?;
        results::apply_page_overlay(&mut rows, &overlay);
    }
    let params = results::covenant_params_by_pot(&rows);
    let mut seats = HashMap::new();
    for chunk in results::seat_marker_chunks(&params) {
        let binds = chunk
            .iter()
            .flat_map(|c| {
                [
                    Bind::Text(c.pot_txid.clone()),
                    Bind::Int(i64::from(c.pot_vout)),
                    Bind::Text(c.pub_a_hex.clone()),
                    Bind::Text(c.pub_b_hex.clone()),
                ]
            })
            .collect();
        let fetched = read(
            db,
            results::seat_markers_sql(chunk.len(), results::SEAT_MARKERS_PER_KEY),
            binds,
        )
        .await?;
        routes::note_seat_marker_rows(&mut seats, fetched);
    }
    for filing in filings {
        if let crate::record_post::VerifiedRecord::Potparty(r, valid) = filing {
            if let (Some(key), Some(sig)) = (&r.seat_settle_pubkey, &r.seat_sig_hex) {
                seats
                    .entry((r.pot_txid.clone(), r.pot_vout))
                    .or_default()
                    .push(results::SeatMarkerRow {
                        identity: r.identity.clone(),
                        opponent_identity: r.opponent_identity.clone(),
                        game_id: r.game_id.clone(),
                        pot_txid: r.pot_txid.clone(),
                        pot_vout: r.pot_vout,
                        recovery_height: r.recovery_height,
                        seat_settle_pubkey: key.clone(),
                        seat_sig_hex: sig.clone(),
                        identity_sig_hex: r.sig_hex.clone(),
                        sig_valid: Some(*valid),
                    });
            }
        }
    }
    let mut hop_seats = HashMap::new();
    for (key, p) in &params {
        let legacy = rows.iter().any(|r| {
            r.pot_txid == key.0 && r.pot_vout == key.1 && (r.pub_a.is_none() || r.pub_b.is_none())
        });
        let (sql, binds) = if legacy {
            (
                results::legacy_hop_seat_markers_sql(1),
                vec![
                    Bind::Text(hex::encode(p.pub_a)),
                    Bind::Text(hex::encode(p.pub_b)),
                ],
            )
        } else {
            (
                results::hop_seat_markers_sql(1),
                pair_binds(std::slice::from_ref(key)),
            )
        };
        let fetched: Vec<Value> = read(db, sql, binds).await?;
        let hops: Vec<_> = fetched
            .iter()
            .map(|r| results::HopSeatRow {
                identity: text(r, "identity"),
                seat_settle_pubkey: text(r, "seatSettlePubkey"),
                marker_valid: flag(r, "markerValid"),
                txid: text(r, "txid"),
                hop_vout: num(r, "hopVout") as u32,
                hop_sats: crate::owed_judge::rank::number(r, "hopSats").map(|n| n as u64),
                size_bytes: crate::owed_judge::rank::number(r, "sizeBytes").map(|n| n as u64),
                fee_sats: crate::owed_judge::rank::number(r, "feeSats").map(|n| n as u64),
            })
            .collect();
        hop_seats.insert(key.clone(), hops);
    }
    let claim_rows: Vec<routes::ResultRowD1> = read(
        db,
        results::claims_sql(games.len()),
        strings(games.iter().cloned()),
    )
    .await?;
    let mut claim_rows: Vec<_> = claim_rows
        .into_iter()
        .filter_map(routes::ResultRowD1::into_marker)
        .collect();
    for filing in filings {
        if let crate::record_post::VerifiedRecord::Result(r, tier) = filing {
            if games.contains(&r.game_id) && !claim_rows.iter().any(|row| row.txid == r.txid) {
                claim_rows.push(crate::logic::ResultMarkerRow {
                    game_id: r.game_id.clone(),
                    winner: r.winner.clone(),
                    loser: r.loser.clone(),
                    pot_txid: r.pot_txid.clone(),
                    settle_txid: r.settle_txid.clone(),
                    winner_sig_hex: r.winner_sig_hex.clone(),
                    loser_sig_hex: r.loser_sig_hex.clone(),
                    cards_hex: r.cards_hex.clone(),
                    txid: r.txid.clone(),
                    created_at: Some(stamp),
                    claim_valid: Some(*tier),
                });
            }
        }
    }
    let claims = results::claims_by_game(&claim_rows);
    let mut hands: HashMap<String, Vec<results::HandMarkerFact>> = HashMap::new();
    for r in read::<Value>(
        db,
        results::hand_markers_sql(games.len()),
        strings(games.iter().cloned()),
    )
    .await?
    {
        hands
            .entry(text(&r, "gameId"))
            .or_default()
            .push(results::HandMarkerFact {
                game_id: text(&r, "gameId"),
                identity: text(&r, "identity"),
                pot_txid: text(&r, "potTxid"),
                cards_hex: text(&r, "cardsHex"),
                sig_hex: r["sigHex"].as_str().map(str::to_owned),
                row_valid: flag(&r, "rowValid"),
            });
    }
    let mut proofs: HashMap<String, Vec<results::ProofHandsFact>> = HashMap::new();
    let mut pending = Vec::new();
    for r in read::<Value>(
        db,
        results::proof_hands_sql(games.len()),
        strings(games.iter().cloned()),
    )
    .await?
    {
        if flag(&r, "bundleValid") == Some(true) && r["winnerCardsHex"].is_string() {
            proof_row(&mut proofs, &r);
        } else if r["bundleValid"].is_null() && pending.len() < results::PROOF_REPLAYS_PER_REQUEST {
            pending.push(Bind::Int(num(&r, "markerRowid") as i64));
        }
    }
    for r in read::<Value>(
        db,
        crate::proof_post::proof_posts_hands_sql(games.len()),
        strings(games.iter().cloned()),
    )
    .await?
    {
        proof_row(&mut proofs, &r);
    }
    if !pending.is_empty() {
        for r in read::<Value>(db, results::proof_bundle_bytes_sql(pending.len()), pending).await? {
            if let Some(f) = results::replay_proof_row(
                &text(&r, "gameId"),
                &text(&r, "winner"),
                &text(&r, "bundleHex"),
            ) {
                proofs.entry(f.game_id.clone()).or_default().push(f);
            }
        }
    }
    Ok(results::assemble_results(
        identity, rows, &claims, &seats, &params, &hop_seats, &hands, &proofs,
    ))
}
fn proof_row(out: &mut HashMap<String, Vec<results::ProofHandsFact>>, r: &Value) {
    if r["winnerCardsHex"].is_string() {
        out.entry(text(r, "gameId"))
            .or_default()
            .push(results::ProofHandsFact {
                game_id: text(r, "gameId"),
                winner: text(r, "winner"),
                winner_cards_hex: text(r, "winnerCardsHex"),
                loser_cards_hex: r["loserCardsHex"].as_str().map(str::to_ascii_lowercase),
            });
    }
}
fn committed_home(e: &results::ResultEntry) -> Option<String> {
    let keys = e.committed_keys.as_ref()?;
    Some(match e.my_seat? {
        results::SeatLetter::A => keys.pay_pkh_a.clone(),
        results::SeatLetter::B => keys.pay_pkh_b.clone(),
    })
}
fn memo(r: &Value) -> hops_view::ProbeMemo {
    hops_view::ProbeMemo {
        outpoint: text(r, "outpoint"),
        probed_at_ms: num(r, "probedAtMs") as i64,
        spent: flag(r, "spent") == Some(true),
        spending_txid: r["spendingTxid"].as_str().map(str::to_owned),
        spent_confirmed: flag(r, "spentConfirmed"),
        tip_height: r["tipHeight"].as_u64(),
    }
}

/// The door's refusal ledger row of ONE of the identity's own hops (`submit_refusals` holds one row per hop
/// outpoint, so a stranger's refused variants of a seat's input cost this read one row at most), inside the window
/// (`?3`, now less `owed::OWED_EVICTION_WINDOW_MS`); a hop the index holds SPENT is skipped (the refusal is stale).
pub const DOOR_REFUSAL_SQL: &str = "SELECT hopTxid,hopVout,reason FROM submit_refusals WHERE hopTxid=?1 AND hopVout=?2 AND refusedAt>=?3 AND NOT EXISTS(SELECT 1 FROM pot_records WHERE txid=?1 AND outputIndex=?2 AND spent=1)";

/// The identity's collected and held filings of the selected games, folded by `owed::CollectedFold::row`.
pub const COLLECTED_SQL: &str = "SELECT gameId,sigHex,txid,payTxid FROM collected_markers_v2 WHERE identity=?1 AND gameId IN (SELECT value FROM json_each(?2))";

async fn hydrate_evidence(
    db: &impl JudgeDb,
    i: &mut JudgeInputs,
    games: &[String],
    filings: &[crate::record_post::VerifiedRecord],
) -> Result<(), String> {
    let bound = vec![Bind::Text(i.identity.clone()), json_bind(&games)];
    for r in read::<Value>(db, COLLECTED_SQL, bound).await? {
        let mut fold = owed::CollectedFold::default();
        fold.row(
            &i.identity,
            &text(&r, "gameId"),
            r["sigHex"].as_str(),
            r["txid"].as_str(),
            r["payTxid"].as_str(),
        );
        i.collected_verified.extend(fold.verified);
        i.collected_present.extend(fold.present);
        i.held_verified.extend(fold.held);
    }
    let pairs: Vec<_> = i
        .results
        .iter()
        .filter(|r| r.spent == Some(false))
        .map(|r| (r.pot_txid.clone(), r.pot_vout))
        .collect();
    for chunk in pairs.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
        for r in read::<Value>(db, owed::valid_refunds_sql(chunk.len()), pair_binds(chunk)).await? {
            let key = owed::outpoint_key(&text(&r, "potTxid"), num(&r, "potVout") as u32);
            let raw = text(&r, "refundRawHex");
            if raw.is_empty() {
                continue;
            }
            let sats = i
                .results
                .iter()
                .find(|e| owed::outpoint_key(&e.pot_txid, e.pot_vout) == key)
                .and_then(committed_home)
                .and_then(|pkh| owed::refund_output_sats(&raw, &pkh));
            i.valid_refunds.entry(key).or_insert(owed::ValidRefund {
                raw_hex: raw,
                my_sats: sats,
            });
        }
    }
    let hop_pairs: Vec<_> = i
        .hops
        .iter()
        .map(|h| (h.hop_txid.clone(), h.hop_vout))
        .collect();
    let mut targets = hop_pairs.clone();
    targets.push(hops_view::reorg_mark_target());
    let mut memos = Vec::new();
    for keys in hops_view::probe_memo_read_chunks(&targets) {
        memos.extend(
            read::<Value>(
                db,
                hops_view::probe_memo_read_sql(keys.len()),
                strings(keys),
            )
            .await?
            .iter()
            .map(memo),
        );
    }
    // bsv-low #953: a hop whose memo is gone (aged out and swept) is judged on the belt's durable evidence of a spend,
    // as a memo that is not fresh; a memo, while it stands, is the newer word.
    for keys in hops_view::probe_evidence_read_chunks(&hop_pairs) {
        for r in read::<Value>(
            db,
            hops_view::probe_memo_read_sql(keys.len()),
            strings(keys),
        )
        .await?
        {
            let mut m = memo(&r);
            let Some((t, v)) = m.outpoint.rsplit_once(':') else {
                continue;
            };
            let Ok(v) = v.parse::<u32>() else {
                continue;
            };
            m.outpoint = hops_view::probe_memo_key(t, v);
            if m.spent && !memos.iter().any(|x| x.outpoint == m.outpoint) {
                memos.push(m);
            }
        }
    }
    let clear = hops_view::reorg_clear_at(&memos);
    let fresh = hops_view::split_probe_targets_at_tip(
        &hop_pairs,
        &memos,
        i.now_ms,
        hops_view::PROBE_MEMO_MAX_AGE_MS,
        hops_view::PROBE_MEMO_CONFIRMED_MAX_AGE_MS,
        clear,
        i.tip,
    );
    let fresh_keys: HashSet<_> = fresh
        .0
        .iter()
        .map(|(t, v, _)| owed::outpoint_key(t, *v))
        .collect();
    let mut refused = HashMap::new();
    for m in &memos {
        if let Some((t, v)) = m.outpoint.rsplit_once('.') {
            if let Ok(v) = v.parse::<u32>() {
                if hop_pairs.contains(&(t.to_string(), v)) {
                    let mut word = hops_view::stale_memo_word(m, i.now_ms, clear, i.tip);
                    // The memo is the most recent chain evidence. Its freshness is told, never an outward probe.
                    let fresh = fresh_keys.contains(&owed::outpoint_key(t, v));
                    word.stale = !fresh;
                    if hops_view::confirmation_refused(m, clear) {
                        refused.insert(owed::outpoint_key(t, v), m.spending_txid.clone());
                    }
                    // the walk's grace word: a fresh memo's confirmation taken inside the reorg grace
                    if fresh
                        && hops_view::confirmed_in_reorg_grace(
                            m.spent_confirmed,
                            m.probed_at_ms,
                            clear,
                        )
                    {
                        i.grace_words.insert(owed::outpoint_key(t, v));
                    }
                    i.hop_chain.insert(owed::outpoint_key(t, v), word);
                }
            }
        }
    }
    let mut all_sweeps: HashMap<String, Vec<crate::hopsweep::FiledHopSweep>> = HashMap::new();
    for (hop, vout) in &hop_pairs {
        let binds = vec![
            Bind::Text(hop.clone()),
            Bind::Int(i64::from(*vout)),
            Bind::Text(i.identity.clone()),
        ];
        for r in read::<Value>(db,"SELECT s.sweepTxid,s.sweepRawHex,t.has_proof,t.proofHeight FROM hopsweep_records s INDEXED BY idx_hopsweep_hop LEFT JOIN transactions t ON t.txid=s.sweepTxid WHERE s.hopTxid=?1 AND s.hopVout=?2 AND s.identity=?3 ORDER BY s.createdAt DESC,s.rowid DESC",binds).await? {
            let raw=text(&r,"sweepRawHex");
            all_sweeps.entry(owed::outpoint_key(hop,*vout)).or_default().push(crate::hopsweep::FiledHopSweep{sweep_txid:text(&r,"sweepTxid"),pays_sats:crate::hopsweep::sweep_output_sats(&raw),raw_hex:raw,index_proven:flag(&r,"has_proof")==Some(true),index_proof_height:r["proofHeight"].as_u64()});
        }
        // Both the refusal and the JOIN-seat fallback start at the selected hop's key.
        for r in read::<Value>(
            db,
            DOOR_REFUSAL_SQL,
            vec![
                Bind::Text(hop.clone()),
                Bind::Int(i64::from(*vout)),
                Bind::Int(i.now_ms - owed::OWED_EVICTION_WINDOW_MS),
            ],
        )
        .await?
        {
            i.door_refused_hop_outpoints.insert(
                owed::outpoint_key(hop, *vout),
                owed::DoorRefusal::of_reason(&text(&r, "reason")),
            );
        }
        for r in read::<Value>(db,"SELECT e.txid,p.outputIndex,p.pubA,p.pubB,p.payPkhA,p.payPkhB FROM pot_evictions e JOIN json_each(e.releasedSpends) s LEFT JOIN pot_records_evicted p ON p.txid=e.txid AND p.lockKind='covenant' AND p.paramsDecoded=1 WHERE e.readmittedAt IS NULL AND e.txid IN (SELECT potTxid FROM potparty_records_evicted INDEXED BY idx_evicted_party_identity_game WHERE identity=?1 AND gameId IN (SELECT value FROM json_each(?4))) AND json_extract(s.value,'$.txid')=?2 AND json_extract(s.value,'$.vout')=?3",vec![Bind::Text(i.identity.clone()),Bind::Text(hop.clone()),Bind::Int(i64::from(*vout)),json_bind(&games)]).await? {
            i.evicted_hop_outpoints.insert(owed::outpoint_key(hop,*vout));i.evicted_pots.insert(text(&r,"txid"));
            if let Some(keys)=results::CommittedKeys::from_columns(r["pubA"].as_str(),r["pubB"].as_str(),r["payPkhA"].as_str(),r["payPkhB"].as_str()) {
                let released=[(owed::outpoint_key(hop,*vout),text(&r,"txid"))];
                let twins=[(text(&r,"txid"),num(&r,"outputIndex") as u32,keys)];
                for (game,pkh) in owed::evicted_pot_homes(&released,&i.hops,&twins){i.my_pkh_by_game.entry(game).or_insert(pkh);}
            }

        }
        let joins: Vec<Value> = read(
            db,
            owed::JOIN_SEAT_POTS_SQL,
            vec![
                Bind::Text(i.identity.clone()),
                Bind::Text(hop.clone()),
                Bind::Int(i64::from(*vout)),
            ],
        )
        .await?;
        for join in joins {
            let pot = text(&join, "potTxid");
            if db
                .tx_facts(&pot, FactsFrom::PotBeefs)
                .await?
                .is_some_and(|f| f.spends(hop, *vout))
            {
                i.join_spent_hops
                    .insert(owed::outpoint_key(hop, *vout), pot);
                break;
            }
        }
    }
    for filing in filings {
        if let crate::record_post::VerifiedRecord::Hopsweep(_, r) = filing {
            if r.identity != i.identity || !hop_pairs.contains(&(r.hop_txid.clone(), r.hop_vout)) {
                continue;
            }
            let key = owed::outpoint_key(&r.hop_txid, r.hop_vout);
            let sweeps = all_sweeps.entry(key).or_default();
            if sweeps.iter().any(|s| s.sweep_txid == r.sweep_txid) {
                continue;
            }
            let proof: Vec<Value> = read(
                db,
                "SELECT has_proof,proofHeight FROM transactions WHERE txid=?1",
                vec![Bind::Text(r.sweep_txid.clone())],
            )
            .await?;
            let proof = proof.first();
            sweeps.insert(
                0,
                crate::hopsweep::FiledHopSweep {
                    sweep_txid: r.sweep_txid.clone(),
                    raw_hex: r.sweep_raw_hex.clone(),
                    pays_sats: crate::hopsweep::sweep_output_sats(&r.sweep_raw_hex),
                    index_proven: proof.is_some_and(|p| flag(p, "has_proof") == Some(true)),
                    index_proof_height: proof.and_then(|p| p["proofHeight"].as_u64()),
                },
            );
        }
    }
    for (key, mut sweeps) in all_sweeps {
        let newest = sweeps.remove(0);
        i.hop_sweeps
            .insert(key, crate::hopsweep::pick_filed_sweep(newest, sweeps));
    }
    i.proofs_set_aside =
        hops_view::set_aside_proofs_a_refused_word_contradicts(&mut i.hop_sweeps, &refused);
    i.refused_words = refused;
    let mut named: BTreeSet<String> = i
        .hops
        .iter()
        .filter_map(|h| h.spending_txid.clone())
        .chain(i.hop_chain.values().filter_map(|w| w.spending_txid.clone()))
        .chain(i.hop_sweeps.values().map(|s| s.sweep_txid.clone()))
        .collect();
    named.extend(i.results.iter().filter_map(|e| e.settle_txid.clone()));
    let named: Vec<_> = named.into_iter().collect();
    if named.is_empty() {
        return Ok(());
    }
    read_words(db, i, &named).await?;
    let pots:Vec<Value>=read(db,"SELECT DISTINCT txid FROM pot_records WHERE lockKind='covenant' AND txid IN (SELECT value FROM json_each(?1))",vec![json_bind(&named)]).await?;
    i.pot_spenders.extend(pots.iter().map(|r| text(r, "txid")));
    for r in read::<Value>(
        db,
        owed::OWED_SPENDER_BYTES_SELECT_SQL,
        vec![json_bind(&named)],
    )
    .await?
    {
        if let Some(bytes) = r["bytesJson"]
            .as_str()
            .and_then(owed::StoredSpenderBytes::from_json)
        {
            let sp = text(&r, "txid");
            i.courier_spenders.insert(sp.clone());
            i.spender_inputs.insert(sp.clone(), bytes.inputs.clone());
            i.spender_outputs.insert(sp, bytes.to_outputs());
        }
    }
    for sp in &named {
        if let Some(facts) = db.tx_facts(sp, FactsFrom::Stored).await? {
            let mut outputs = facts.outputs;
            for r in read::<Value>(
                db,
                "SELECT outputIndex,spent FROM pot_records WHERE txid=?1",
                vec![Bind::Text(sp.clone())],
            )
            .await?
            {
                if let Some(o) = outputs
                    .iter_mut()
                    .find(|o| u64::from(o.vout) == num(&r, "outputIndex"))
                {
                    o.spent = flag(&r, "spent");
                }
            }
            i.spender_inputs.insert(sp.clone(), facts.inputs);
            i.spender_outputs.insert(sp.clone(), outputs);
            i.courier_spenders.remove(sp);
        } else if !i.spender_outputs.contains_key(sp) {
            if let Some(raw) = i
                .word_rows
                .get(sp)
                .and_then(|r| r.legacy_body.as_deref())
                .and_then(|b| serde_json::from_str::<Value>(b).ok())
                .and_then(|b| b["rawHex"].as_str().and_then(|s| hex::decode(s).ok()))
            {
                if let Ok(tx) = bsv_rs::transaction::Transaction::from_binary(&raw) {
                    if tx.id().eq_ignore_ascii_case(sp) {
                        i.spender_inputs
                            .insert(sp.clone(), routes::spender_inputs_of(&tx));
                        i.spender_outputs
                            .insert(sp.clone(), routes::spender_outputs_of(&tx));
                        i.courier_spenders.insert(sp.clone());
                    }
                }
            }
        }
    }
    let mut home_memos = Vec::new();
    let mut proof_memos = Vec::new();
    for chunk in named.chunks(owed::HOME_LATCH_READ_CHUNK) {
        let binds = owed::home_latch_range_binds(chunk);
        home_memos.extend(
            read::<Value>(
                db,
                owed::home_latch_range_read_sql(binds.len() / 2),
                strings(binds),
            )
            .await?
            .iter()
            .map(memo),
        );
        let binds = owed::proof_latch_range_binds(chunk);
        proof_memos.extend(
            read::<Value>(
                db,
                owed::home_latch_range_read_sql(binds.len() / 2),
                strings(binds),
            )
            .await?
            .iter()
            .map(memo),
        );
    }
    i.retired_spenders = owed::latched_retired_spenders(&named, &home_memos);
    let latches = owed::proof_latches_of(&named, &proof_memos);
    let mut canonical = HashMap::new();
    for height in owed::proof_latch_recheck_heights(&latches, i.tip) {
        canonical.insert(height, db.canonical_root(height).await);
    }
    i.proof_mined_spenders = owed::latched_proof_mined_spenders(
        &latches,
        &canonical,
        i.tip,
        hops_view::reorg_clear_at(&proof_memos),
        i.now_ms,
    );
    i.moved_latches = owed::moved_proof_latches(&latches, &canonical);
    i.home_spends =
        owed::latched_home_words(&owed::courier_home_outputs(&i.as_inputs()), &home_memos).0;
    Ok(())
}

pub(super) async fn finish_rows(db: &impl JudgeDb, rows: &mut Vec<OwedRow>) -> Result<(), String> {
    let mut vouts = HashMap::new();
    for (txid, pkh) in owed::pay_vout_wanted(rows) {
        if let Some(vout) = db
            .tx_facts(&txid, FactsFrom::Stored)
            .await?
            .and_then(|f| f.sole_home_vout(&pkh))
        {
            vouts.insert(owed::pay_vout_key(&txid, &pkh), vout);
        }
    }
    owed::mark_pay_vouts(rows, &vouts);
    let mut spent = HashSet::new();
    for chunk in owed::payout_home_outpoints(rows).chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
        for r in read::<Value>(
            db,
            owed::payout_home_spent_sql(chunk.len()),
            pair_binds(chunk),
        )
        .await?
        {
            spent.insert(owed::outpoint_key(
                &text(&r, "txid"),
                num(&r, "outputIndex") as u32,
            ));
        }
    }
    owed::retire_index_spent_homes(rows, &spent);
    Ok(())
}

/// MEDIUM-3 (bsv-low #912): where a transaction's facts are read. `PotBeefs` is the JOIN's own stored BEEF alone
/// (`owed::JOIN_POT_BEEF_SQL`), `Stored` the pot's BEEF and then the engine's (`pot_beefs`, then `transactions`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactsFrom {
    PotBeefs,
    Stored,
}

/// The facts of one transaction the judge reads: its inputs (lowercase `txid`, vout) and its outputs as
/// `routes::spender_outputs_of` reads them (no spend word). The D1 judge decodes them from the stored BEEF at each
/// read; the actor (`owed_actor`) keeps them decoded and never holds the bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxFacts {
    pub inputs: Vec<(String, u32)>,
    pub outputs: Vec<owed::SpenderOutput>,
}
impl TxFacts {
    pub fn of_tx(tx: &bsv_rs::transaction::Transaction) -> Self {
        Self {
            inputs: routes::spender_inputs_of(tx),
            outputs: routes::spender_outputs_of(tx),
        }
    }
    /// `owed::join_spends_outpoint`'s answer over the decoded inputs.
    pub fn spends(&self, hop: &str, vout: u32) -> bool {
        self.inputs
            .iter()
            .any(|(t, v)| *v == vout && t.eq_ignore_ascii_case(hop))
    }
    /// `owed::sole_home_vout_of_raw`'s answer over the decoded outputs.
    pub fn sole_home_vout(&self, pkh: &str) -> Option<u32> {
        owed::sole_home_vout(
            self.outputs.iter().map(|o| (o.vout, o.pkh_hex.as_deref())),
            pkh,
        )
    }
    pub fn to_json(&self) -> Value {
        serde_json::json!({
            "inputs": self.inputs.iter().map(|(t, v)| serde_json::json!([t, v])).collect::<Vec<_>>(),
            "outputs": self.outputs.iter().map(|o| serde_json::json!([o.vout, o.pkh_hex, o.sats, o.pot_lock])).collect::<Vec<_>>(),
        })
    }
    pub fn from_json(v: &Value) -> Option<Self> {
        let inputs = v["inputs"]
            .as_array()?
            .iter()
            .map(|i| Some((i[0].as_str()?.to_string(), i[1].as_u64()? as u32)))
            .collect::<Option<Vec<_>>>()?;
        let outputs = v["outputs"]
            .as_array()?
            .iter()
            .map(|o| {
                Some(owed::SpenderOutput {
                    vout: o[0].as_u64()? as u32,
                    pkh_hex: o[1].as_str().map(str::to_string),
                    sats: o[2].as_u64()?,
                    spent: None,
                    pot_lock: o[3].as_bool()?,
                })
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self { inputs, outputs })
    }
}

/// The SQL a stored transaction's BEEFs are read by, in `FactsFrom`'s order.
pub const STORED_BEEFS_SQL: &str = "SELECT hex(beef) AS beef FROM pot_beefs WHERE txid=?1 UNION ALL SELECT hex(beef) AS beef FROM transactions WHERE txid=?1";

/// The first stored BEEF that parses and carries `txid`, decoded (the D1 judge's read, unchanged).
pub fn facts_of_beefs<'a>(txid: &str, beefs: impl IntoIterator<Item = &'a str>) -> Option<TxFacts> {
    beefs
        .into_iter()
        .filter_map(|h| hex::decode(h).ok())
        .filter_map(|b| {
            overlay_engine::beef_limits::parse_beef(
                &b,
                &overlay_engine::beef_limits::APP_BEEF_LIMITS,
            )
            .ok()
        })
        .find_map(|b| b.find_txid(txid).and_then(|t| t.tx().cloned()))
        .map(|tx| TxFacts::of_tx(&tx))
}

pub(super) async fn stored_tx_facts(
    db: &impl SqlPort,
    txid: &str,
    from: FactsFrom,
) -> Result<Option<TxFacts>, String> {
    let rows: Vec<Value> = read(
        db,
        match from {
            FactsFrom::PotBeefs => owed::JOIN_POT_BEEF_SQL,
            FactsFrom::Stored => STORED_BEEFS_SQL,
        },
        vec![Bind::Text(txid.to_string())],
    )
    .await?;
    let take = if from == FactsFrom::PotBeefs {
        1
    } else {
        rows.len()
    };
    Ok(facts_of_beefs(
        txid,
        rows.iter().take(take).filter_map(|r| r["beef"].as_str()),
    ))
}

/// D2-ACTOR-B-FOLD-11 (bsv-low #957): a released-hop evidence row (or its could-not-judge twin) under the era's rule
/// ([`crate::owed_judge::rank::before_era`]): its stamp the hop's own pot row (`potCreatedAt`, seconds), else the
/// eviction's time (`evictedAt`, milliseconds).
fn era_stamped_before(r: &Value, era_ms: Option<i64>) -> bool {
    let mut v = json!({"potCreatedAt": r["potCreatedAt"].clone()});
    if v["potCreatedAt"].is_null() {
        v["createdAt"] = json!(crate::owed_judge::rank::number(r, "evictedAt").map(|ms| ms / 1000));
    }
    crate::owed_judge::rank::before_era(&v, era_ms)
}
