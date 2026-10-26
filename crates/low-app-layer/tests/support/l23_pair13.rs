// The captured beta pair-13 eviction (cfce07ce..., evicted before the eviction writer existed) and its synthetic legs.
// Shared by the budget tier's seed (examples/d1_budget_seed.rs) and the fold-8 pin (old_walk_native.rs).
// L23R2 route-selection pin: captured pair-13 rows alongside #499's synthetic budget seats.
// A twin-only identity, ninth behind stranger evictions, at 25h, with a planted unverified hop marker.
pub fn l23_r2_route_rows(now_s: u64) -> Vec<String> {
    let f: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/l23_r2_pair13_captured.json"
    )))
    .unwrap();
    let mut rows = Vec::new();
    for (name, table) in [
        ("evictions", "pot_evictions"),
        ("parties", "potparty_records_evicted"),
        ("locks", "pot_records_evicted"),
        ("join_beef", "pot_beefs"),
    ] {
        for row in f["rows"][name].as_array().unwrap() {
            let fields: Vec<_> = row
                .as_object()
                .unwrap()
                .iter()
                .filter(|(k, _)| !k.contains('('))
                .collect();
            let values: Vec<_> = fields
                .iter()
                .map(|(k, v)| {
                    if matches!(k.as_str(), "evictedAt" | "af_evictedAt") {
                        return ((now_s - 25 * 3600) * 1000).to_string();
                    }
                    match v {
                        serde_json::Value::Null => "NULL".into(),
                        serde_json::Value::String(v) if k.as_str() == "beef" => format!("X'{v}'"),
                        serde_json::Value::String(v) => format!("'{}'", v.replace('\'', "''")),
                        serde_json::Value::Number(v) => v.to_string(),
                        _ => panic!("unexpected captured column"),
                    }
                })
                .collect();
            rows.push(format!(
                "INSERT INTO {table} ({}) VALUES ({})",
                fields
                    .iter()
                    .map(|(k, _)| format!("\"{k}\""))
                    .collect::<Vec<_>>()
                    .join(","),
                values.join(",")
            ));
        }
    }
    let hop = "97288e47d846066d59f0caa8a2bec22fd9040dd5af05f323c67cf010373ab064";
    let me = "02f9f80a92a92677d97fce5bb0a03d0321425b918868b3ef5773120d9cf5364e49";
    let game = "9d88aaaa348acae127efd08e133822201ecae56513831c37124837f3ef9ae17f";
    let competitor = "57d8e2907420da08795e3146fb5bd05446a51ef2f870213775a074eccae73851";
    let mut beef = bsv_rs::transaction::Beef::new();
    beef.merge_raw_tx(
        hex::decode(f["competitor_raw_hex"].as_str().unwrap()).unwrap(),
        None,
    );
    rows.push(format!(
        "INSERT INTO pot_beefs(txid,beef,createdAt) VALUES ('{competitor}',X'{}',{now_s})",
        hex::encode(beef.to_binary())
    ));
    rows.push(format!("INSERT INTO pot_records(txid,outputIndex,spent,spentConfirmed,createdAt) VALUES ('{hop}',0,0,0,{now_s})"));
    rows.push(format!("INSERT INTO hop_chain_probes(outpoint,probedAtMs,spent,spendingTxid,spentConfirmed) VALUES ('{hop}.0',{},1,'{competitor}',1)",now_s*1000));
    rows.push(format!("INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,markerValid,txid,outputIndex,createdAt) VALUES ('{me}','stranger','{game}',0,20190,'{}','00','00',3,0,'{hop}',2,{now_s})","02".repeat(33)));
    // Synthetic fault leg: a latched party row whose stored ancestor read returns no bytes.
    let fault_id = format!("02{}", "f1".repeat(32));
    let fault_join = "dd".repeat(32);
    let fresh_pot = "ee".repeat(32);
    rows.push(format!("INSERT INTO potparty_records_evicted(identity,potTxid,gameId,opponentIdentity,seatSettlePubkey,sigValid,af_evictedAt) VALUES ('{fault_id}','{fault_join}','{game}','opponent','{}',1,{})", "02".repeat(33), now_s * 1000));
    rows.push(format!("INSERT INTO pot_evictions(txid,reason,evictedAt,releasedSpends) VALUES ('{fault_join}','REJECTED',{},'[{{\"txid\":\"{hop}\",\"vout\":0}},{{\"txid\":\"{fresh_pot}\",\"vout\":0}}]')", now_s * 1000));
    // A healthy unrelated family for the same seat must remain freshly served after the missing JOIN.
    rows.push(format!("INSERT INTO pot_records(txid,outputIndex,spent,createdAt) VALUES ('{fresh_pot}',0,0,{now_s})"));
    rows.push(format!("INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt) VALUES ('{fault_id}','opponent','healthy-game','{fresh_pot}',0,958846,'3045ab','healthy-filing',0,{now_s})"));
    let cached_id = format!("02{}", "f2".repeat(32));
    rows.push(format!("INSERT INTO potparty_records_evicted(identity,potTxid,gameId,opponentIdentity,seatSettlePubkey,sigValid,af_evictedAt) VALUES ('{cached_id}','{fault_join}','{game}','opponent','{}',1,{})", "02".repeat(33), now_s * 1000));
    rows.push(format!("INSERT INTO owed_state(identity,computedAtMs,rows,stale,truncated) VALUES ('{cached_id}',{},1,1,0)", now_s*1000-900000));
    rows.push(format!("INSERT INTO owed_rows(identity,outpoint,family,gameId,facts,updatedAtMs) VALUES ('{cached_id}','{hop}:0','hop-stranded','{game}','{{\"markerlessEvidence\":true,\"claim\":\"sweep-hop\",\"claimable\":true}}',{})",now_s*1000-900000));
    // A separate captured seat has NO live marker: this leg specifically depends on the twin probe.
    // Its hop's unspent probe memo is written by the cell itself, at the read (overlay #85): an unspent memo answers
    // for five minutes (`PROBE_MEMO_MAX_AGE_MS`), and stamped here it aged past that behind the tier's three
    // `wrangler dev` builds on a loaded host, so the walk asked mainnet WoC, which knows this captured hop swept home.
    let other_hop = "9b238eed0177c714b1f9a608243e8f1bf79e35e9597110f83cefa1b658785c62";
    rows.push(format!("INSERT INTO pot_records(txid,outputIndex,spent,spentConfirmed,createdAt) VALUES ('{other_hop}',0,0,0,{now_s})"));
    // Budget noise claims p28, but every latch is INVALID; the verified-seat covering indexes must skip it.
    for i in 1..=300 {
        rows.push(format!("INSERT INTO potparty_records_evicted(identity,potTxid,gameId,opponentIdentity,seatSettlePubkey,sigValid,af_evictedAt) VALUES ('{me}','{i:064x}','{game}','opponent','{}',0,{})", "02".repeat(33), now_s*1000));
    }
    for i in 1..=8 {
        rows.push(format!("INSERT INTO pot_evictions(txid,reason,evictedAt,releasedSpends) VALUES ('{i:064x}','REJECTED',{},'[{{\"txid\":\"{}\",\"vout\":0}}]')",now_s*1000,"aa".repeat(32)));
    }
    rows
}
