//! The stored-rows reader (bsv-low #585, lane E585-land): before any deploy
//! of the streaming door, every BEEF the Worker keeps at rest is read through
//! the SAME functions the Worker reads it with, natively, from an export of
//! the D1 (and the R2 objects the queue keeps), so a stored body the door
//! would refuse is named before the door meets it. Read-only: it opens the
//! export in an in-memory SQLite and writes nothing anywhere.
//!
//! What is read (the readers NL-6 moved onto the stream, `fc019c8`):
//! - `transactions.beef` and `transactions_evicted.beef` (the engine's store:
//!   `D1Storage::beef_has_proof`, `reorg_sweep`'s own BUMP,
//!   `proof_fetcher`'s `transaction_from_beef`), keyed by `txid`;
//! - `pot_beefs.beef` (`pot_beef_has_proof`, the pot lookup's
//!   `transaction_from_beef`), keyed by `txid`;
//! - `mutation_dead_letters.message` (a `MutationMessage`): its inline
//!   `beefB64` through `queue::decode_beef_b64` under the dead letter's door,
//!   or its R2 object (`r2.beefR2Key`, else the `r2_key` column) through
//!   `queue::check_replay_blob`; a submit body, so the census's shape
//!   (`submit_census::census_reading`) is read too;
//! - `gasp_deferred_graphs.record` with its `gasp_deferred_graph_chunks`
//!   parts (door 4): the record is no BEEF, its nodes' raw transactions and
//!   proofs are read as the resume reads them (`Transaction::from_hex`,
//!   `beef_limits::proof_from_hex`).
//!
//! Each BEEF goes through the door (`beef_limits::read_beef`: a refusal
//! names its offset and kind), the sizing read (`stream_sizing::estimate`
//! under the census's charges, flagged past the census's memory budget, a
//! budget and never a refusal), and the reader of its table. One line per
//! refusal (`REFUSED table=.. key=.. offset=.. kind=..`), one per body that
//! could not be read at all (`UNREAD`, an R2 object not in the directory),
//! then a `SUMMARY` line.
//!
//! Inputs, named by `STORED_ROWS_INPUT` (comma-separated paths):
//! - `*.sql`: `wrangler d1 export <db> --remote --output <file>.sql`, loaded
//!   whole (schema and rows);
//! - `*.json`: the output of `wrangler d1 execute <db> --remote --json
//!   --command "SELECT * FROM <table>"`, one file per table, the TABLE named
//!   by the file's name up to its first dot (`pot_beefs.json`). A BLOB may
//!   arrive as an array of bytes or as a hex string (`SELECT txid,
//!   hex(beef) AS beef ...`).
//! - `STORED_ROWS_R2_DIR`: a directory of the queue bucket's objects, each
//!   saved by `wrangler r2 object get <bucket>/<key> --remote --file
//!   <dir>/<key>` (the key's `/` as directories), or with every `/` of the
//!   key replaced by `_` in one flat directory.
//!
//! Run: `STORED_ROWS_INPUT=/path/beta.sql STORED_ROWS_R2_DIR=/path/r2 cargo
//! test --manifest-path workers/Cargo.toml -p bsv-overlay-cloudflare --lib
//! stored_rows::read_the_export -- --ignored --nocapture`.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use overlay_engine::beef_limits;
use overlay_engine::stream_sizing;
use rusqlite::types::Value;
use rusqlite::Connection;

use crate::queue::{self, MutationMessage};
use crate::submit_census;

/// What one read of an export found.
#[derive(Debug, Default)]
pub(crate) struct Summary {
    pub rows: u64,
    pub beefs: u64,
    pub bytes: u64,
    pub refusals: u64,
    pub unread: u64,
    pub over_memory: u64,
    pub largest_body: (u64, String),
    pub largest_estimate: (u64, String),
    pub deferred_records: u64,
    pub deferred_nodes: u64,
    pub census: std::collections::BTreeMap<&'static str, u64>,
    /// Every line printed, in order.
    pub lines: Vec<String>,
}

impl Summary {
    fn line(&mut self, line: String) {
        println!("{line}");
        self.lines.push(line);
    }

    fn refused(&mut self, table: &str, key: &str, offset: Option<u64>, kind: &str, detail: &str) {
        self.refusals += 1;
        let offset = offset.map_or_else(|| "-".to_string(), |o| o.to_string());
        self.line(format!(
            "REFUSED table={table} key={key} offset={offset} kind={kind} detail={detail}"
        ));
    }

    fn summary_line(&self) -> String {
        let mut census = String::new();
        for (verdict, n) in &self.census {
            let _ = write!(census, " {verdict}:{n}");
        }
        format!(
            "SUMMARY rows={} beefs={} bytes={} refusals={} unread={} over_memory={} largest_body={} ({}) largest_estimate={} ({}) deferred_records={} deferred_nodes={} census=[{}]",
            self.rows,
            self.beefs,
            self.bytes,
            self.refusals,
            self.unread,
            self.over_memory,
            self.largest_body.0,
            self.largest_body.1,
            self.largest_estimate.0,
            self.largest_estimate.1,
            self.deferred_records,
            self.deferred_nodes,
            census.trim_start(),
        )
    }

    /// One BEEF through the door, the sizing read and, for a stored row, the
    /// table's own reader of its txid. `false` when the door refused it.
    fn read_beef(&mut self, table: &str, key: &str, bytes: &[u8], txid: Option<&str>) -> bool {
        self.beefs += 1;
        let len = bytes.len() as u64;
        self.bytes += len;
        let at = format!("{table} {key}");
        if len > self.largest_body.0 || self.largest_body.1.is_empty() {
            self.largest_body = (len, at.clone());
        }
        let est = stream_sizing::estimate(
            bytes,
            &submit_census::CENSUS_CHARGES,
            submit_census::CENSUS_MEMORY_BYTES,
        );
        if est.bytes > self.largest_estimate.0 || self.largest_estimate.1.is_empty() {
            self.largest_estimate = (est.bytes, at);
        }
        if let Some(over_at) = est.over_at {
            self.over_memory += 1;
            self.line(format!(
                "OVER-MEMORY table={table} key={key} offset={over_at} estimate={} budget={}",
                est.bytes,
                submit_census::CENSUS_MEMORY_BYTES
            ));
        }
        if let Err(refusal) = beef_limits::read_beef(bytes) {
            self.refused(
                table,
                key,
                Some(refusal.offset),
                &format!("{:?}", refusal.kind()),
                &format!("{:?}", refusal.reason),
            );
            return false;
        }
        if let Some(txid) = txid {
            if let Err(refusal) = beef_limits::own_proof(bytes, txid) {
                self.refused(
                    table,
                    key,
                    Some(refusal.offset),
                    &format!("{:?}", refusal.kind()),
                    "own_proof",
                );
                return false;
            }
            let _ = crate::d1_storage::D1Storage::beef_has_proof(txid, bytes);
            if let Err(e) = beef_limits::transaction_from_beef(
                bytes,
                Some(txid),
                &beef_limits::STORED_BEEF_LIMITS,
            ) {
                self.refused(table, key, None, "transaction_from_beef", &e.to_string());
                return false;
            }
        }
        true
    }
}

/// A BLOB as the export gave it: bytes, or text holding hex.
fn blob(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::Blob(b) => Some(b.clone()),
        Value::Text(t) => hex::decode(t.trim())
            .ok()
            .or_else(|| Some(t.as_bytes().to_vec())),
        _ => None,
    }
}

fn text(value: &Value) -> Option<String> {
    match value {
        Value::Text(t) => Some(t.clone()),
        Value::Integer(i) => Some(i.to_string()),
        Value::Real(r) => Some(r.to_string()),
        Value::Blob(b) => String::from_utf8(b.clone()).ok(),
        Value::Null => None,
    }
}

/// Rows of `sql`, or none when the table or a column is not in the export.
fn rows(db: &Connection, sql: &str, params: &[&dyn rusqlite::ToSql]) -> Option<Vec<Vec<Value>>> {
    let mut stmt = db.prepare(sql).ok()?;
    let n = stmt.column_count();
    let out = stmt
        .query_map(params, |row| {
            (0..n)
                .map(|i| row.get::<_, Value>(i))
                .collect::<Result<Vec<_>, _>>()
        })
        .ok()?
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    Some(out)
}

/// Load one JSON file of `wrangler d1 execute --json` as table `table`.
// `#[cfg(test)]` again on the item: the module is test-only (lib.rs), and
// the ownership checker reads an item's own attribute.
#[cfg(test)]
fn load_json(db: &Connection, table: &str, json: &str) -> Result<(), String> {
    let parsed: serde_json::Value = serde_json::from_str(json).map_err(|e| e.to_string())?;
    let results: Vec<&serde_json::Value> = match &parsed {
        serde_json::Value::Array(items) => items
            .iter()
            .flat_map(|i| {
                i.get("results")
                    .and_then(|r| r.as_array())
                    .into_iter()
                    .flatten()
            })
            .collect(),
        serde_json::Value::Object(o) => o
            .get("results")
            .and_then(|r| r.as_array())
            .into_iter()
            .flatten()
            .collect(),
        _ => Vec::new(),
    };
    let Some(first) = results.first().and_then(|r| r.as_object()) else {
        return Ok(());
    };
    let columns: Vec<String> = first.keys().cloned().collect();
    let quoted: Vec<String> = columns.iter().map(|c| format!("\"{c}\"")).collect();
    db.execute(
        &format!(
            "CREATE TABLE IF NOT EXISTS \"{table}\" ({})",
            quoted.join(", ")
        ),
        [],
    )
    .map_err(|e| e.to_string())?;
    let insert = format!(
        "INSERT INTO \"{table}\" ({}) VALUES ({})",
        quoted.join(", "),
        vec!["?"; columns.len()].join(", ")
    );
    for row in results {
        let values: Vec<Value> = columns
            .iter()
            .map(|c| match row.get(c) {
                None | Some(serde_json::Value::Null) => Value::Null,
                Some(serde_json::Value::String(s)) => Value::Text(s.clone()),
                Some(serde_json::Value::Number(n)) => n
                    .as_i64()
                    .map_or_else(|| Value::Real(n.as_f64().unwrap_or(0.0)), Value::Integer),
                Some(serde_json::Value::Bool(b)) => Value::Integer(i64::from(*b)),
                Some(serde_json::Value::Array(a)) => {
                    Value::Blob(a.iter().map(|b| b.as_u64().unwrap_or(0) as u8).collect())
                }
                Some(other) => Value::Text(other.to_string()),
            })
            .collect();
        db.execute(&insert, rusqlite::params_from_iter(values))
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// An R2 object's bytes from the directory, by its key.
fn r2_object(dir: Option<&Path>, key: &str) -> Option<Vec<u8>> {
    let dir = dir?;
    std::fs::read(dir.join(key))
        .or_else(|_| std::fs::read(dir.join(key.replace('/', "_"))))
        .ok()
}

/// Read every stored BEEF of the export `inputs` (and the R2 objects in
/// `r2_dir`) through the Worker's readers.
// `#[cfg(test)]` again on the item: the module is test-only (lib.rs), and
// the ownership checker reads an item's own attribute.
#[cfg(test)]
pub(crate) fn read_export(inputs: &[PathBuf], r2_dir: Option<&Path>) -> Result<Summary, String> {
    let db = Connection::open_in_memory().map_err(|e| e.to_string())?;
    for path in inputs {
        let body = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        if path.extension().is_some_and(|e| e == "json") {
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            let table = name.split('.').next().unwrap_or_default();
            load_json(&db, table, &body).map_err(|e| format!("{}: {e}", path.display()))?;
        } else {
            db.execute_batch(&body)
                .map_err(|e| format!("{}: {e}", path.display()))?;
        }
    }
    let mut s = Summary::default();

    for table in ["transactions", "transactions_evicted", "pot_beefs"] {
        for row in
            rows(&db, &format!("SELECT txid, beef FROM \"{table}\""), &[]).unwrap_or_default()
        {
            s.rows += 1;
            let txid = text(&row[0]).unwrap_or_default();
            let Some(bytes) = blob(&row[1]) else {
                continue;
            };
            s.read_beef(table, &txid, &bytes, Some(&txid));
        }
    }

    let letters = rows(
        &db,
        "SELECT txid, topics, message, r2_key FROM mutation_dead_letters",
        &[],
    )
    .or_else(|| {
        rows(
            &db,
            "SELECT txid, topics, message, NULL FROM mutation_dead_letters",
            &[],
        )
    })
    .unwrap_or_default();
    let table = "mutation_dead_letters";
    for row in letters {
        s.rows += 1;
        let key = format!(
            "{}[{}]",
            text(&row[0]).unwrap_or_default(),
            text(&row[1]).unwrap_or_default()
        );
        let message = text(&row[2]).unwrap_or_default();
        if message.is_empty() || message == "{}" {
            continue; // a `failing` note holds no bytes
        }
        let body: MutationMessage = match serde_json::from_str(&message) {
            Ok(b) => b,
            Err(e) => {
                s.refused(table, &key, None, "message", &e.to_string());
                continue;
            }
        };
        let bytes = if let Some(r) = &body.r2 {
            let Some(bytes) = r2_object(r2_dir, &r.key) else {
                s.unread += 1;
                s.line(format!(
                    "UNREAD table={table} key={key} r2={} why=object-not-in-dir",
                    r.key
                ));
                continue;
            };
            if let Err(e) = queue::check_replay_blob(r, &bytes) {
                if beef_limits::read_beef(&bytes).is_ok() {
                    s.refused(table, &key, None, "check_replay_blob", &e);
                    continue;
                }
            }
            bytes
        } else if !body.beef_b64.is_empty() {
            use base64::{engine::general_purpose::STANDARD, Engine as _};
            match STANDARD.decode(&body.beef_b64) {
                Ok(bytes) => {
                    let _ = queue::decode_beef_b64(
                        &body.beef_b64,
                        &beef_limits::DEAD_LETTER_BEEF_LIMITS,
                    );
                    bytes
                }
                Err(e) => {
                    s.refused(table, &key, None, "base64", &e.to_string());
                    continue;
                }
            }
        } else if let Some(r2_key) = text(&row[3]).filter(|t| !t.is_empty()) {
            let Some(bytes) = r2_object(r2_dir, &r2_key) else {
                s.unread += 1;
                s.line(format!(
                    "UNREAD table={table} key={key} r2={r2_key} why=object-not-in-dir"
                ));
                continue;
            };
            bytes
        } else {
            continue;
        };
        if s.read_beef(table, &key, &bytes, None) {
            let verdict = submit_census::census_reading(&bytes).verdict.as_str();
            *s.census.entry(verdict).or_default() += 1;
        }
    }

    let heads = rows(
        &db,
        "SELECT host, topic, outpoint, record, chunks, gen FROM gasp_deferred_graphs",
        &[],
    )
    .or_else(|| {
        rows(
            &db,
            "SELECT host, topic, outpoint, record, 0, '' FROM gasp_deferred_graphs",
            &[],
        )
    })
    .unwrap_or_default();
    let table = "gasp_deferred_graphs";
    for row in heads {
        s.rows += 1;
        let (host, topic, outpoint) = (
            text(&row[0]).unwrap_or_default(),
            text(&row[1]).unwrap_or_default(),
            text(&row[2]).unwrap_or_default(),
        );
        let key = format!("{host}|{topic}|{outpoint}");
        let mut json = text(&row[3]).unwrap_or_default();
        let chunks = match &row[4] {
            Value::Integer(i) => (*i).max(0) as u64,
            Value::Real(r) => r.max(0.0) as u64,
            _ => 0,
        };
        let gen = text(&row[5]).unwrap_or_default();
        let mut whole = true;
        for seq in 1..=chunks {
            let part = rows(
                &db,
                "SELECT part FROM gasp_deferred_graph_chunks WHERE host = ?1 AND topic = ?2 AND outpoint = ?3 AND gen = ?4 AND seq = ?5",
                &[&host, &topic, &outpoint, &gen, &(seq as i64)],
            )
            .and_then(|r| r.into_iter().next())
            .and_then(|r| text(&r[0]));
            let Some(part) = part else {
                // The Worker drops such a record (`get_deferred_graph`).
                s.unread += 1;
                s.line(format!(
                    "UNREAD table={table} key={key} why=part-{seq}-of-gen-{gen}-missing"
                ));
                whole = false;
                break;
            };
            json.push_str(&part);
        }
        if !whole {
            continue;
        }
        let record: overlay_engine::gasp::DeferredGraph = match serde_json::from_str(&json) {
            Ok(r) => r,
            Err(e) => {
                s.refused(table, &key, None, "record", &e.to_string());
                continue;
            }
        };
        s.deferred_records += 1;
        s.bytes += json.len() as u64;
        for (i, walked) in record.nodes.iter().enumerate() {
            s.deferred_nodes += 1;
            let node_key = format!("{key}#node{i}");
            if let Err(e) = bsv_rs::transaction::Transaction::from_hex(&walked.node.raw_tx) {
                s.refused(table, &node_key, None, "rawTx", &e.to_string());
            }
            if let Some(proof) = &walked.node.proof {
                if let Err(e) = beef_limits::proof_from_hex(proof) {
                    s.refused(table, &node_key, None, "proof", &e.to_string());
                }
            }
        }
    }

    let line = s.summary_line();
    s.line(line);
    Ok(s)
}

/// The captain's run: see the module doc.
#[test]
#[ignore = "reads a D1 export named by STORED_ROWS_INPUT"]
fn read_the_export() {
    let inputs: Vec<PathBuf> = std::env::var("STORED_ROWS_INPUT")
        .expect("STORED_ROWS_INPUT: comma-separated export files")
        .split(',')
        .filter(|p| !p.is_empty())
        .map(PathBuf::from)
        .collect();
    let r2 = std::env::var("STORED_ROWS_R2_DIR").ok().map(PathBuf::from);
    let s = read_export(&inputs, r2.as_deref()).unwrap();
    assert_eq!(s.refusals, 0, "{}", s.summary_line());
}

#[cfg(test)]
mod self_test {
    use super::*;
    use crate::queue::BeefRef;

    fn fixture(rel: &str) -> Vec<u8> {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(rel);
        let raw = std::fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        if rel.ends_with(".hex") {
            hex::decode(String::from_utf8(raw).unwrap().trim()).unwrap()
        } else {
            raw
        }
    }

    /// Every BEEF fixture in the tree, by name.
    fn fixtures() -> Vec<(&'static str, Vec<u8>)> {
        [
            "tests/fixtures/ef/loop2_join_5ad2764c_incomplete.beef",
            "tests/fixtures/ef/loop2_join_f9e85aab_represent.beef",
            "tests/fixtures/ef/parent_a7d76588_beef.hex",
            "../overlay-engine/tests/fixtures/zanaadu-pf-head-dc4ca9ea.beef",
            "../../tools/lane-script/fixtures/valid.beef.hex",
            "../../tools/lane-script/fixtures/corrupted.beef.hex",
        ]
        .into_iter()
        .map(|rel| (rel, fixture(rel)))
        .collect()
    }

    fn txid_of(bytes: &[u8]) -> String {
        beef_limits::read_beef(bytes)
            .unwrap()
            .subject
            .or_else(|| beef_limits::read_beef(bytes).unwrap().last_txid)
            .unwrap()
    }

    fn sql_blob(bytes: &[u8]) -> String {
        format!("X'{}'", hex::encode(bytes))
    }

    fn sql_text(t: &str) -> String {
        format!("'{}'", t.replace('\'', "''"))
    }

    /// An export as `wrangler d1 export` writes it (the schema, then one
    /// INSERT a row), every fixture in each BEEF table, a dead letter inline
    /// and one by R2 key, and a deferred-graph record cut into three parts;
    /// then one corrupted body. Every fixture reads clean; the corrupted
    /// body is named with its table, its key, its offset and its kind.
    #[test]
    fn e585_land_every_fixture_reads_clean_and_a_corrupted_one_names_its_offset() {
        use base64::{engine::general_purpose::STANDARD, Engine as _};
        let dir =
            std::env::temp_dir().join(format!("e585-land-stored-rows-{}", std::process::id()));
        let r2 = dir.join("r2");
        std::fs::create_dir_all(&r2).unwrap();
        let mut sql = String::from(
            "PRAGMA defer_foreign_keys=TRUE;\n\
             CREATE TABLE transactions (txid TEXT PRIMARY KEY, beef BLOB, has_proof INTEGER NOT NULL DEFAULT 0);\n\
             CREATE TABLE pot_beefs (txid TEXT PRIMARY KEY, beef BLOB NOT NULL, createdAt INTEGER);\n",
        );
        sql.push_str(crate::dead_letters::DEAD_LETTERS_CREATE);
        sql.push_str(";\nALTER TABLE mutation_dead_letters ADD COLUMN r2_key TEXT;\n");
        sql.push_str(crate::gasp_deferred::DEFERRED_GRAPHS_CREATE);
        sql.push_str(";\n");
        sql.push_str(crate::gasp_deferred::DEFERRED_GRAPH_CHUNKS_CREATE);
        sql.push_str(";\n");
        sql.push_str(crate::gasp_deferred::DEFERRED_GRAPHS_CHUNKS_COLUMN);
        sql.push_str(";\n");
        sql.push_str(crate::gasp_deferred::DEFERRED_GRAPHS_GEN_COLUMN);
        sql.push_str(";\n");

        let fixtures = fixtures();
        let mut total = 0u64;
        let mut largest = 0u64;
        for (_, bytes) in &fixtures {
            let txid = txid_of(bytes);
            total += 2 * bytes.len() as u64;
            largest = largest.max(bytes.len() as u64);
            for table in ["transactions", "pot_beefs"] {
                sql.push_str(&format!(
                    "INSERT INTO \"{table}\" VALUES({},{},0);\n",
                    sql_text(&txid),
                    sql_blob(bytes)
                ));
            }
        }
        // A dead letter inline, and one whose body is in R2.
        let (inline_rel, inline) = &fixtures[1];
        let message = MutationMessage {
            beef_b64: STANDARD.encode(inline),
            r2: None,
            topics: vec!["tm_test".into()],
            mode: "historical-tx".into(),
            reason: queue::REPLAY_REASON_PHASE3_FAULT.into(),
            redrive: None,
        };
        let (_, keyed) = &fixtures[3];
        let sha = hex::encode(bsv_rs::primitives::hash::sha256(keyed));
        let topics = vec!["tm_test".to_string()];
        let r = BeefRef {
            key: queue::r2_key(&sha, &topics, "historical-tx"),
            sha256: sha,
            bytes: keyed.len() as u64,
            txid: Some(txid_of(keyed)),
        };
        std::fs::create_dir_all(r2.join(Path::new(&r.key).parent().unwrap())).unwrap();
        std::fs::write(r2.join(&r.key), keyed).unwrap();
        let by_key = MutationMessage {
            beef_b64: String::new(),
            r2: Some(r.clone()),
            ..message.clone()
        };
        total += (inline.len() + keyed.len()) as u64;
        for (txid, m, key) in [
            (txid_of(inline), &message, None),
            (txid_of(keyed), &by_key, Some(r.key.clone())),
        ] {
            sql.push_str(&format!(
                "INSERT INTO \"mutation_dead_letters\" (txid, topics, message, status, first_seen_at, r2_key) VALUES({},'tm_test',{},'parked',1,{});\n",
                sql_text(&txid),
                sql_text(&serde_json::to_string(m).unwrap()),
                key.as_deref().map_or("NULL".to_string(), sql_text)
            ));
        }
        // A failing note: no bytes, nothing read.
        sql.push_str("INSERT INTO \"mutation_dead_letters\" (txid, topics, message, status, first_seen_at) VALUES('note','tm_test','','failing',1);\n");

        // A deferred-graph record of two nodes: a raw transaction and a
        // proven one with its own BUMP, cut into three parts.
        let valid = &fixtures[4].1;
        let manifest: serde_json::Value =
            serde_json::from_slice(&fixture("../../tools/lane-script/fixtures/manifest.json"))
                .unwrap();
        let funding = manifest["valid"]["funding_txid"].as_str().unwrap();
        let funding_tx = beef_limits::transaction_from_beef(
            valid,
            Some(funding),
            &beef_limits::STORED_BEEF_LIMITS,
        )
        .unwrap();
        let proof = beef_limits::own_bump(valid, funding).unwrap().to_hex();
        let node = |raw_tx: String, proof: Option<String>| {
            serde_json::json!({
                "node": {"graphID": "aa.0", "rawTx": raw_tx, "outputIndex": 0, "proof": proof},
                "spentBy": null
            })
        };
        let record = serde_json::json!({
            "peer": "https://peer.example", "topic": "tm_test", "outpoint": "aa.0", "score": 1,
            "nodes": [
                node(manifest["valid"]["subject_raw_hex"].as_str().unwrap().into(), None),
                node(funding_tx.to_hex(), Some(proof)),
            ],
            "pending": [], "calls": 3, "passes": 1, "reason": "bytes"
        })
        .to_string();
        let plan = crate::gasp_deferred::chunk_plan(&record, record.len() / 3 + 1);
        assert_eq!(plan.rest.len(), 2, "the record is cut into three parts");
        total += record.len() as u64;
        sql.push_str(&format!(
            "INSERT INTO \"gasp_deferred_graphs\" VALUES('https://peer.example','tm_test','aa.0',1,2,0,3,1,'bytes',{},{},1,1,{},{});\n",
            record.len(),
            sql_text(plan.head),
            plan.rest.len(),
            sql_text(&plan.gen)
        ));
        for (i, part) in plan.rest.iter().enumerate() {
            sql.push_str(&format!(
                "INSERT INTO \"gasp_deferred_graph_chunks\" VALUES('https://peer.example','tm_test','aa.0',{},{},{},1);\n",
                sql_text(&plan.gen),
                i + 1,
                sql_text(part)
            ));
        }
        let export = dir.join("clean.sql");
        std::fs::write(&export, &sql).unwrap();

        let s = read_export(&[export], Some(&r2)).unwrap();
        assert_eq!(s.refusals, 0, "every fixture reads clean: {:?}", s.lines);
        assert_eq!(s.unread, 0, "{:?}", s.lines);
        assert_eq!(s.beefs, 2 * fixtures.len() as u64 + 2, "{inline_rel}");
        assert_eq!(s.rows, 2 * fixtures.len() as u64 + 3 + 1);
        assert_eq!(s.bytes, total);
        assert_eq!(s.largest_body.0, largest);
        assert!(s.largest_estimate.0 > 0);
        assert_eq!((s.deferred_records, s.deferred_nodes), (1, 2));
        assert_eq!(
            s.census.values().sum::<u64>(),
            2,
            "both letters are censused"
        );
        assert!(s.lines.last().unwrap().starts_with("SUMMARY rows="));

        // One corrupted body: a fixture with a byte after its frame, stored
        // in pot_beefs, and an R2 object the directory does not hold.
        let (_, honest) = &fixtures[1];
        let mut corrupted = honest.clone();
        corrupted.push(0x00);
        let txid = txid_of(honest);
        let mut bad = sql;
        bad.push_str(&format!(
            "INSERT INTO \"pot_beefs\" VALUES('corrupted',{},0);\n",
            sql_blob(&corrupted)
        ));
        let gone = MutationMessage {
            r2: Some(BeefRef {
                key: "mutations/00/11".into(),
                ..r
            }),
            ..by_key
        };
        bad.push_str(&format!(
            "INSERT INTO \"mutation_dead_letters\" (txid, topics, message, status, first_seen_at) VALUES('gone','tm_test',{},'parked',1);\n",
            sql_text(&serde_json::to_string(&gone).unwrap())
        ));
        let export = dir.join("corrupted.sql");
        std::fs::write(&export, &bad).unwrap();
        let s = read_export(&[export], Some(&r2)).unwrap();
        assert_eq!(s.refusals, 1, "{:?}", s.lines);
        let refused = s.lines.iter().find(|l| l.starts_with("REFUSED")).unwrap();
        assert!(
            refused.starts_with(&format!(
                "REFUSED table=pot_beefs key=corrupted offset={} kind=TrailingBytes",
                honest.len()
            )),
            "{refused} ({txid})"
        );
        assert_eq!(s.unread, 1);
        assert!(s.lines.iter().any(|l| l.starts_with(
            "UNREAD table=mutation_dead_letters key=gone[tm_test] r2=mutations/00/11"
        )));

        // The JSON form: one file a table, a BLOB as bytes or as hex.
        let json = serde_json::json!([{
            "results": [
                {"txid": txid_of(honest), "beef": honest.iter().map(|b| u64::from(*b)).collect::<Vec<_>>()},
                {"txid": "corrupted", "beef": hex::encode(&corrupted)},
            ],
            "success": true
        }]);
        let path = dir.join("transactions.json");
        std::fs::write(&path, json.to_string()).unwrap();
        let s = read_export(&[path], None).unwrap();
        assert_eq!((s.beefs, s.refusals), (2, 1), "{:?}", s.lines);
        assert!(s.lines[0].contains(&format!("offset={} kind=TrailingBytes", honest.len())));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
