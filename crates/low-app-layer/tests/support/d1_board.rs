// bsv-low #642: a fleet-shaped board. `wins` counted wins over 150 identities
// (seven of them win a third of the games), every 17th owned by its settle key
// (no latched seat), with the noise a board read must step past: unproven
// spends, a stale verdict, an unwitnessed final spend, vout-1 rows. Markers:
// the winner's claim (v2 cards on most, v1 on some, a later second claim on
// some), proof pointers and posted bundles on some. Rows are plain INSERTs, so
// the route seed loads the same board into the local D1.

pub fn identity(i: u32) -> String {
    format!("02{:064x}", 0xb0a4_0000u64 + u64::from(i))
}

fn seat(g: u32, side: u32) -> String {
    format!(
        "03{:064x}",
        0xc0a4_0000u64 + u64::from(g) * 2 + u64::from(side)
    )
}

fn pot(g: u32) -> String {
    format!("b0{g:062x}")
}

fn spender(g: u32) -> String {
    format!("b1{g:062x}")
}

fn game(g: u32) -> String {
    format!("b2{g:062x}")
}

/// Five distinct cards (0..=51) for game `g`, as `cardsHex`.
fn cards(g: u32) -> String {
    let mut out = Vec::new();
    let mut c = (g * 7) % 52;
    while out.len() < 5 {
        if !out.contains(&c) {
            out.push(c);
        }
        c = (c + 11) % 52;
    }
    out.iter().map(|c| format!("{c:02x}")).collect()
}

/// The board's games: `wins` counted, interleaved with uncounted noise.
pub fn rows(now: u64, wins: u32) -> Vec<String> {
    let mut rows = Vec::new();
    let mut counted = 0u32;
    let mut g = 0u32;
    while counted < wins {
        g += 1;
        // Admission stamps rise with `g`; every fifth game shares the previous
        // stamp (the rowid breaks the tie).
        let stamp = |g: u32| now - 7 * (100_000 - u64::from(g));
        let at = if g.is_multiple_of(5) {
            stamp(g - 1)
        } else {
            stamp(g)
        };
        let winner_a = g.is_multiple_of(2);
        let verdict = if winner_a { "winner-a" } else { "winner-b" };
        let (sa, sb) = (seat(g, 0), seat(g, 1));
        let winner_idx = if g.is_multiple_of(3) { g % 7 } else { g % 150 };
        let loser_idx = (winner_idx + 1 + g % 13) % 150;
        let (win_id, lose_id) = (identity(winner_idx), identity(loser_idx));
        let (win_seat, lose_seat) = if winner_a { (&sa, &sb) } else { (&sb, &sa) };
        // Noise: what the counting bar refuses.
        let (confirmed, final_, witnessed, stale) = match g % 23 {
            7 => (0, 0, 0, false),  // unproven, not final
            11 => (0, 1, 1, false), // unproven, final, witnessed: COUNTED
            13 => (0, 1, 0, false), // unproven, final, unwitnessed
            19 => (1, 1, 0, true),  // a stale verdict
            _ => (1, 1, 0, false),
        };
        let counts = (confirmed == 1 || witnessed == 1) && !stale;
        let verdict_txid = if stale { pot(g) } else { spender(g) };
        rows.push(format!(
            "INSERT INTO pot_records (txid, outputIndex, spent, spendingTxid, spentConfirmed, spenderFinal, createdAt, lockKind, paramsDecoded, pubA, pubB, pubTower, payPkhA, payPkhB, rakePkh, stakeA, stakeB, feeSats, recoveryHeight, potSats, verdict, verdictTxid, settleSigners) \
             VALUES ('{p}', 0, 1, '{s}', {confirmed}, {final_}, {at}, 'covenant', 1, '{sa}', '{sb}', '{t}', '{pa}', '{pb}', '{rk}', 1000, 1000, 10, 958846, 1990, '{verdict}', '{verdict_txid}', '{signers}')",
            p = pot(g),
            s = spender(g),
            t = seat(g, 9),
            pa = "11".repeat(20),
            pb = "22".repeat(20),
            rk = "33".repeat(20),
            signers = if g.is_multiple_of(4) { "tower-a" } else { "coop" },
        ));
        // A vout-1 output of the same transaction (never a board row).
        rows.push(format!(
            "INSERT INTO pot_records (txid, outputIndex, spent, createdAt) VALUES ('{p}', 1, 0, {at})",
            p = pot(g),
        ));
        if witnessed == 1 {
            rows.push(format!(
                "INSERT INTO network_seen (txid, seenAt) VALUES ('{s}', 1)",
                s = spender(g)
            ));
        }
        let key_owned = g.is_multiple_of(17);
        if !key_owned {
            for (k, (id, st)) in [(&win_id, win_seat), (&lose_id, lose_seat)]
                .into_iter()
                .enumerate()
            {
                let opp = if k == 0 { &lose_id } else { &win_id };
                rows.push(format!(
                    "INSERT INTO potparty_records (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, sigHex, seatSettlePubkey, seatSigHex, txid, outputIndex, createdAt, sigValid) \
                     VALUES ('{id}', '{opp}', '{gm}', '{p}', 0, 958846, '3045ab', '{st}', '00', 'b3{k}{g:061x}', 0, {at}, 1)",
                    gm = game(g),
                    p = pot(g),
                ));
            }
        }
        let claim_cards = if g % 9 == 4 {
            "NULL".to_string()
        } else {
            format!("'{}'", cards(g))
        };
        let claim = if g % 6 == 1 { 1 } else { 2 };
        let loser_sig = if claim == 2 { "'3045cd'" } else { "NULL" };
        rows.push(format!(
            "INSERT INTO result_markers_v2 (gameId, winner, loser, potTxid, settleTxid, winnerSigHex, loserSigHex, cardsHex, txid, outputIndex, createdAt, claimValid) \
             VALUES ('{gm}', '{win_id}', '{lose_id}', '{p}', '{s}', '3045ef', {loser_sig}, {claim_cards}, 'b4{g:062x}', 0, {mat}, {claim})",
            gm = game(g),
            p = pot(g),
            s = spender(g),
            mat = at + 30,
        ));
        if g % 10 == 3 {
            rows.push(format!(
                "INSERT INTO result_markers_v2 (gameId, winner, loser, potTxid, settleTxid, winnerSigHex, loserSigHex, cardsHex, txid, outputIndex, createdAt, claimValid) \
                 VALUES ('{gm}', '{win_id}', '{lose_id}', '{p}', '{s}', '3045ef', NULL, '{c}', 'b5{g:062x}', 0, {mat}, 1)",
                gm = game(g),
                p = pot(g),
                s = spender(g),
                c = cards(g + 1),
                mat = at + 90,
            ));
        }
        if g % 4 == 1 {
            rows.push(format!(
                "INSERT INTO proof_markers (gameId, winner, sigHex, bundle, txid, outputIndex, createdAt) \
                 VALUES ('{gm}', '{win_id}', '00', x'00', 'b6{g:062x}', 0, {mat})",
                gm = game(g),
                mat = at + 60,
            ));
        }
        if g % 8 == 2 {
            rows.push(format!(
                "INSERT INTO proof_posts (gameId, winner, sigHex, bundle, createdAt, bundleValid) \
                 VALUES ('{gm}', '{win_id}', '00', x'00', {mat}, 1)",
                gm = game(g),
                mat = at + 60,
            ));
        }
        if counts {
            counted += 1;
        }
    }
    rows
}
