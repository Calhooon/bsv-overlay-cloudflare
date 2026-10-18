// bsv-low #638: fleet noise and a whale, shared by the route seed and plan pins.
pub const WHALE: &str = "03dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

pub fn rows(now: u64) -> Vec<String> {
    let mut rows = Vec::new();
    for i in 0..14_719u32 {
        let tx = format!("d0{i:062x}");
        let id = if i < 1_033 {
            WHALE.to_string()
        } else {
            format!("02{:064x}", i % 120)
        };
        let seat = format!("03{i:064x}");
        rows.push(format!("INSERT INTO pot_records (txid, outputIndex, spent, createdAt, lockKind, paramsDecoded, recoveryHeight, pubA, pubB) VALUES ('{tx}', 0, {}, {now}, 'covenant', 1, 958846, '{seat}', '{seat}')", u8::from(i >= 6_622)));
        if i < 14_781 {
            rows.push(format!("INSERT INTO potparty_records (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, sigHex, txid, outputIndex, createdAt) VALUES ('{id}', '{id}', '{tx}', '{tx}', 0, 958846, '00', 'e0{i:062x}', 0, {now})"));
        }
        if i < 7_549 {
            let hop = format!("d1{i:062x}");
            let target = if (500..554).contains(&i) {
                14_000 + i
            } else {
                i
            };
            let target_tx = format!("d0{target:062x}");
            let seat = format!("03{target:064x}");
            let hid = if i < 554 {
                WHALE.to_string()
            } else {
                format!("02{:064x}", i % 120)
            };
            rows.push(format!("INSERT INTO pot_records (txid, outputIndex, spent, spendingTxid, createdAt) VALUES ('{hop}', 0, 1, '{target_tx}', {now})"));
            rows.push(format!("INSERT INTO hopparty_records (identity, opponentIdentity, gameId, hopVout, hopSats, seatSettlePubkey, seatSigHex, identitySigHex, containerOutputs, txid, outputIndex, createdAt, markerValid) VALUES ('{hid}', '{hid}', '{tx}', 0, 20190, '{seat}', '00', '00', 2, '{hop}', 1, {now}, 1)"));
        }
        if i < 7_209 {
            rows.push(format!("INSERT INTO potrefund_records (identity, gameId, potTxid, potVout, refundRawHex, sigHex, txid, outputIndex, createdAt, refundValid) VALUES ('{id}', '{tx}', '{tx}', 0, '00', '00', 'e1{i:062x}', 0, {now}, 1)"));
            rows.push(format!("INSERT INTO hopsweep_records (identity, gameId, hopTxid, hopVout, sweepTxid, sweepRawHex, txid, outputIndex, createdAt) VALUES ('{id}', '{tx}', 'd1{i:062x}', 0, 'e2{i:062x}', '00', 'filed:e2{i:062x}', 0, {now})"));
        }
        if i < 19_005 {
            rows.push(format!("INSERT INTO transactions (txid, beef, has_proof, proofHeight) VALUES ('e2{i:062x}', x'00', 1, 958845)"));
        }
        if i < 150 {
            rows.push(format!("INSERT INTO pot_evictions (txid, reason, evictedAt) VALUES ('e3{i:062x}', 'fixture', {})", now * 1000));
        }
    }
    // bsv-low #645: beta's network_seen latches (about 27,000), keyed apart from every pot and spender above.
    for i in 0..27_000u32 {
        rows.push(format!(
            "INSERT INTO network_seen (txid, seenAt) VALUES ('e4{i:062x}', {now})"
        ));
    }
    // NS-COST-BAR-1: beta's pot_beefs (20,837 rows), its retired transactions (21,724 rows with the 19,005 above)
    // and a proofless watch of 221 rows over every arm of the GC, keyed apart from everything above.
    for i in 0..20_837u32 {
        let unprovable = if i % 3 == 0 {
            "1"
        } else if i % 3 == 1 {
            "0"
        } else {
            "NULL"
        };
        rows.push(format!(
            "INSERT INTO pot_beefs (txid, beef, createdAt, has_proof, structurally_unprovable) VALUES ('e5{i:062x}', x'00', {now}, {}, {unprovable})",
            u8::from(i % 2 == 0)
        ));
    }
    for i in 0..2_719u32 {
        let reason = if i % 2 == 0 {
            "expired-unproven"
        } else {
            "utxo-spent"
        };
        rows.push(format!(
            "INSERT INTO transactions (txid, beef, has_proof, retired_ms, retired_reason) VALUES ('e6{i:062x}', x'00', 0, {}, '{reason}')",
            now * 1000
        ));
    }
    for i in 0..221u32 {
        let txid = match i % 4 {
            // Spread over pot_beefs' four latch shapes: proven, unprovable, NULL-latch and live backlog survive or go.
            0 => format!("e5{:062x}", i * 5 + 1),
            1 => format!("e6{i:062x}"),
            2 => format!("e2{i:062x}"),
            _ => format!("e7{i:062x}"),
        };
        rows.push(format!(
            "INSERT INTO proofless_watch (txid, first_seen_ms) VALUES ('{txid}', {})",
            now * 1000
        ));
    }
    rows
}

/// Replay fixture literals through cached parameterized statements. The same source writes and triggers run,
/// while fleet size does not multiply SQL/trigger compilation work in the native pins or seed generator.
pub fn execute_row(db: &rusqlite::Connection, sql: &str) {
    use rusqlite::types::Value;
    let (head, values) = sql.split_once(" VALUES (").expect("fixture INSERT shape");
    let values = values.strip_suffix(')').expect("fixture values close");
    let binds: Vec<Value> = values
        .split(',')
        .map(|v| {
            let v = v.trim();
            if v == "NULL" {
                Value::Null
            } else if let Some(hex) = v.strip_prefix("x'").and_then(|v| v.strip_suffix('\'')) {
                Value::Blob(hex::decode(hex).expect("fixture blob"))
            } else if let Some(text) = v.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')) {
                Value::Text(text.to_string())
            } else {
                Value::Integer(v.parse().expect("fixture integer"))
            }
        })
        .collect();
    let placeholders = vec!["?"; binds.len()].join(",");
    let template = format!("{head} VALUES ({placeholders})");
    db.prepare_cached(&template)
        .expect("fixture statement prepares")
        .execute(rusqlite::params_from_iter(binds))
        .unwrap_or_else(|e| panic!("{e}: {sql}"));
}
