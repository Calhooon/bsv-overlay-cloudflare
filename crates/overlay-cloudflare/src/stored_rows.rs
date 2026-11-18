//! The stored-rows reader (bsv-low #585, lane E585-land): before any deploy
//! of the streaming door, every BEEF the Worker keeps at rest is read through
//! the SAME functions the Worker reads it with, natively, from an export of
//! the D1 (and the R2 objects the queue keeps), so a stored body the door
//! would refuse is named before the door meets it. Read-only: it opens the
//! export in an in-memory SQLite and writes nothing anywhere.
//!
//! What is read (the readers NL-6 moved onto the stream, `fc019c8`). Each
//! path CALLS the Worker's own read, never a second spelling of it (the land
//! lens E585-LAND-L3), so the reader can neither accept a row the Worker
//! refuses nor refuse one it accepts, with ONE exception, the raw-transaction
//! columns (below): their Workers' readers are the app layer's
//! (`hopsweep::sweep_output_sats`, `owed::refund_output_sats`,
//! `record_post::potrefund_chain_row_is_own`), which this crate does not link,
//! so those columns are read with bsv-rs's own readers the app layer's call
//! (`Transaction::from_binary`, `sighash::parse_transaction`): the verdict on
//! the bytes is the same, the spelling is not the app layer's.
//! - `transactions.beef` and `transactions_evicted.beef` (the engine's store:
//!   `D1Storage::beef_has_proof`, `reorg_sweep`'s own BUMP,
//!   `proof_fetcher`'s `subject_from_beef`), keyed by `txid`, and
//!   `pot_beefs.beef` (`pot_beef_has_proof`, the pot lookup's
//!   `transaction_from_beef`): read back as the Worker reads them, `hex(beef)
//!   AS beef` decoded by `d1::beef_of_hex_column`. A BEEF stored as hex TEXT
//!   is then its ASCII bytes, which the door refuses, as at the Worker.
//!   (`transactions_evicted` has no Rust reader: it is restored by SQL into
//!   `transactions`, where this read applies.)
//! - `mutation_dead_letters.message`: the Worker reads a parked letter's
//!   bytes only when the operator's lever re-drives it, so the reader builds
//!   the lever's message (`dead_letters::redrive_message`) and reads it
//!   through the main consumer's own read (`queue::read_for_replay`): an
//!   inline `beefB64` by `queue::decode_replay_beef`, a keyed one through a
//!   directory port whose object goes through `queue::replay_object`, the
//!   function the Worker's R2 read calls. The `r2_key` column is not read
//!   (the consumer reads `body.r2` alone). A submit body, so the census's
//!   verdict (`submit_census::census_verdict`) is read too.
//! - `gasp_deferred_graphs.record` with its `gasp_deferred_graph_chunks`
//!   parts (door 4), through `gasp_deferred::read_deferred_graph`, the
//!   Worker's `get_deferred_graph`'s own read, over the shipped statements
//!   and D1's column types (a part that is no TEXT is the Worker's fault). The
//!   record is no BEEF; its nodes' raw transactions and proofs are read as the
//!   resume reads them (`Transaction::from_hex`, `beef_limits::proof_from_hex`).
//!   The export is first given the deferred tables' own migration statements
//!   (`ADD COLUMN` re-runs tolerated by the Worker's own
//!   `d1::migration_error_is_benign`), as the Worker's first request would.
//!
//! - PORT-0811 (bsv-rs 0.8.11): every stored BEEF is also linked as the
//!   engine links it, under the engine's link bound (16 MiB of heap,
//!   `beef_limits::transaction_from_beef`): a trip is
//!   `REFUSED .. kind=link_bound` (the stored rows by their txid, a parked
//!   letter by the engine's subject rule, `subject::subject_txid_of`). A
//!   stored row whose UNNAMED read (`from_beef(_, None)`, the read a 0.7.2
//!   build's GASP node made) answers another transaction than its txid
//!   (GHSA-gg5p-wgff-v3hr: the body ends with an ancestor) is a `DIFFERS`
//!   line: it reads the same now that every reader names the txid, and is
//!   counted apart, never a refusal.
//! - PORT-0811: the D1 columns holding a raw transaction a Worker parses back
//!   (`hopsweep_records.sweepRawHex`, the owed judge's filed sweep and its
//!   `sweep_output_sats`; `potrefund_records.refundRawHex`, the records
//!   door's chain no-op and the owed judge's refunds), each through
//!   `Transaction::from_binary` and `sighash::parse_transaction`, the readers
//!   bsv-rs 0.8.5 and 0.8.11 made refuse a byte after the transaction
//!   (GHSA-j8hr-pjhm-6h27, GHSA-67vc-6c8g-7x48): such a row is
//!   `REFUSED .. kind=raw_tx` (or `kind=sighash_parse`); a sweep whose bytes
//!   name another txid than its `sweepTxid` is a `DIFFERS` line.
//! - PORT-0811 fold: the deferred EF jobs' bytes at rest
//!   (`ef_deferred_jobs`, the stream in `ef_deferred_chunks` or the R2 object
//!   `ef-deferred/<reference>` in `STORED_ROWS_R2_DIR`), through the job
//!   run's own read (`ef_deferred::stream_of_chunks`, `checked_at_rest`,
//!   `job_submission`: the length, then the reference over the topics, the
//!   BEEF and the off-chain values; either failing is `REFUSED ..
//!   kind=ef_job`), then the BEEF as every stored BEEF above (by the job's
//!   subject txid) and the gated arm's conversion (`ef::beef_to_ef_batch`,
//!   `REFUSED .. kind=ef_batch`). An R2 job whose object is not in the
//!   directory is `UNREAD`.
//! - PORT-0811 fold: `handfile_batches.payloadsHex`, read back by both
//!   Workers through `HandfileBatch::of_stored` (`REFUSED .. kind=payloads`
//!   when it does not read: the Worker then serves the row without its batch)
//!   and served verbatim; no Worker parses a payload back. The potrefund
//!   payload carries the refund's raw transaction after its fixed head, read
//!   as the exploded row's `refundRawHex` is (`kind=raw_tx`, `sighash_parse`).
//!
//! Not in a D1 export, so not read: the owed actors' own stores (each
//! identity's Durable Object SQLite, its `txfacts`, and the `OWED_COPY` KV).
//!
//! Each BEEF goes through the door (`beef_limits::read_beef`: a refusal
//! names its offset and kind), the sizing read (`stream_sizing::estimate`
//! under the census's charges, flagged past the census's memory budget, a
//! budget and never a refusal), and the reader of its table. One line per
//! refusal (`REFUSED table=.. key=.. offset=.. kind=..`), one per body that
//! could not be read at all (`UNREAD`: an R2 object not in the directory, a
//! record's part missing), then a `SUMMARY` line and a `VERDICT` line. THE RUN
//! FAILS on any refusal AND on any `UNREAD` row ([`Summary::verdict`]): a row
//! the reader could not read is not a row it read clean.
//!
//! Inputs, named by `STORED_ROWS_INPUT` (comma-separated paths):
//! - `*.sql`: `wrangler d1 export <db> --remote --output <file>.sql`, loaded
//!   whole (schema and rows);
//! - `*.json`: the output of `wrangler d1 execute <db> --remote --json
//!   --command "SELECT * FROM <table>"`, one file per table, the TABLE named
//!   by the file's name up to its first dot (`pot_beefs.json`). A BLOB
//!   arrives as an array of bytes (each 0 to 255, else the file is refused);
//!   a string is TEXT, as D1 holds it (so a `hex(beef) AS beef` export is
//!   TEXT holding hex, and is read as the Worker would read such a column).
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

use crate::gasp_deferred::{self, DeferredHead, DeferredRead, DeferredRows};
use crate::queue::{self, BeefRef, BlobFault, MissingVerdict, ReadStep, ReplayBytes};
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
    /// PORT-0811: the rows read the same by every named reader but otherwise
    /// than a 0.7.2 build read them (`DIFFERS` lines). Counted apart: not a
    /// refusal, never the run's failure.
    pub differs: u64,
    /// PORT-0811: the raw-transaction columns read.
    pub raw_txs: u64,
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

    fn differs(&mut self, table: &str, key: &str, kind: &str, detail: &str) {
        self.differs += 1;
        self.line(format!(
            "DIFFERS table={table} key={key} kind={kind} detail={detail}"
        ));
    }

    /// PORT-0811: the link as the engine makes it (bsv-rs 0.8.11's default
    /// bound); `false` when it trips (`REFUSED .. kind=link_bound`) or fails.
    fn read_linked(&mut self, table: &str, key: &str, bytes: &[u8], txid: Option<&str>) -> bool {
        match beef_limits::transaction_from_beef(bytes, txid, &beef_limits::STORED_BEEF_LIMITS) {
            Ok(_) => true,
            Err(e) if beef_limits::is_link_bound(&e) => {
                self.refused(table, key, None, "link_bound", &e.to_string());
                false
            }
            Err(e) => {
                self.refused(table, key, None, "transaction_from_beef", &e.to_string());
                false
            }
        }
    }

    /// PORT-0811: a raw transaction a Worker parses back, by the two readers
    /// that refuse a byte after it (bsv-rs 0.8.5 and 0.8.11). `Some(txid)` when
    /// both read it; `named` is the txid the row stores beside it.
    fn read_raw_tx(
        &mut self,
        table: &str,
        key: &str,
        hex_column: Option<String>,
        named: Option<&str>,
    ) {
        self.raw_txs += 1;
        let Some(raw) = hex_column.as_deref().and_then(|h| hex::decode(h).ok()) else {
            self.refused(table, key, None, "hex", "the column is no hex");
            return;
        };
        self.bytes += raw.len() as u64;
        let tx = match bsv_rs::transaction::Transaction::from_binary(&raw) {
            Ok(tx) => tx,
            Err(e) => {
                self.refused(table, key, None, "raw_tx", &e.to_string());
                return;
            }
        };
        if let Err(e) = bsv_rs::primitives::bsv::sighash::parse_transaction(&raw) {
            self.refused(table, key, None, "sighash_parse", &e.to_string());
            return;
        }
        if let Some(named) = named {
            if !tx.id().eq_ignore_ascii_case(named) {
                self.differs(
                    table,
                    key,
                    "raw_tx_txid",
                    &format!("the bytes are {}, the row names {named}", tx.id()),
                );
            }
        }
    }

    fn summary_line(&self) -> String {
        let mut census = String::new();
        for (verdict, n) in &self.census {
            let _ = write!(census, " {verdict}:{n}");
        }
        format!(
            "SUMMARY rows={} beefs={} raw_txs={} bytes={} refusals={} unread={} differs={} over_memory={} largest_body={} ({}) largest_estimate={} ({}) deferred_records={} deferred_nodes={} census=[{}]",
            self.rows,
            self.beefs,
            self.raw_txs,
            self.bytes,
            self.refusals,
            self.unread,
            self.differs,
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

    /// THE RUN'S VERDICT: clean only with no refusal AND no unread row.
    pub(crate) fn verdict(&self) -> Result<(), String> {
        if self.refusals == 0 && self.unread == 0 {
            Ok(())
        } else {
            Err(format!(
                "{} refused and {} unread rows: {}",
                self.refusals,
                self.unread,
                self.summary_line()
            ))
        }
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
            if !self.read_linked(table, key, bytes, Some(txid)) {
                return false;
            }
            // The read a 0.7.2 build's unnamed readers made of this row: the
            // wire-last entry (every read now names its txid or takes the
            // subject rule; this names a row such a reader would misread).
            if let Some(last) = beef_limits::parse_beef(bytes, &beef_limits::STORED_BEEF_LIMITS)
                .ok()
                .and_then(|b| b.txs.last().map(bsv_rs::transaction::BeefTx::txid))
            {
                if !last.eq_ignore_ascii_case(txid) {
                    self.differs(
                        table,
                        key,
                        "unnamed_subject",
                        &format!("a read naming no txid answered {last} (the wire-last entry)"),
                    );
                }
            }
        }
        true
    }
}

/// A deferred EF job's row by the Worker's own statement (`JOB_READ_SQL`),
/// each column as D1 hands it to the job's deserialization.
fn export_job(db: &Connection, reference: &str) -> Option<crate::ef_deferred::Job> {
    let row = rows(db, crate::ef_deferred::JOB_READ_SQL, &[&reference])?
        .into_iter()
        .next()?;
    let num = |i: usize| -> Option<f64> {
        match &row[i] {
            Value::Integer(v) => Some(*v as f64),
            Value::Real(v) => Some(*v),
            _ => None,
        }
    };
    let txt = |i: usize| text(&row[i]);
    Some(crate::ef_deferred::Job {
        reference: txt(0)?,
        subject_txid: txt(1)?,
        topics: txt(2)?,
        submit_mode: txt(3)?,
        has_off_chain: num(4)?,
        beef_len: num(5)?,
        bytes: num(6)?,
        at_rest: txt(7)?,
        chunks: num(8)?,
        subject_ef_bytes: num(9)?,
        batch_ef_bytes: num(10)?,
        state: txt(11)?,
        attempts: num(12)?,
        status: num(13),
        answer: txt(14),
        created_at: num(15)?,
        updated_at: num(16)?,
        legs_from: num(17).unwrap_or(0.0),
    })
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
            .map(|c| -> Result<Value, String> {
                Ok(match row.get(c) {
                    None | Some(serde_json::Value::Null) => Value::Null,
                    Some(serde_json::Value::String(s)) => Value::Text(s.clone()),
                    Some(serde_json::Value::Number(n)) => n
                        .as_i64()
                        .map_or_else(|| Value::Real(n.as_f64().unwrap_or(0.0)), Value::Integer),
                    Some(serde_json::Value::Bool(b)) => Value::Integer(i64::from(*b)),
                    Some(serde_json::Value::Array(a)) => Value::Blob(
                        a.iter()
                            .map(|b| {
                                b.as_u64()
                                    .and_then(|b| u8::try_from(b).ok())
                                    .ok_or_else(|| format!("column {c}: {b} is not a byte"))
                            })
                            .collect::<Result<_, _>>()?,
                    ),
                    Some(other) => Value::Text(other.to_string()),
                })
            })
            .collect::<Result<_, _>>()?;
        db.execute(&insert, rusqlite::params_from_iter(values))
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// The reader's [`ReplayBytes`]: the bucket's objects saved in a directory.
/// The object goes through `queue::replay_object`, the function the Worker's
/// R2 read calls; a MISSING one is not judged (that needs the live D1): it is
/// the replay's fault, which the reader reports `UNREAD`.
struct DirBytes<'a>(Option<&'a Path>);

impl ReplayBytes for DirBytes<'_> {
    async fn read_beef(&self, r: &BeefRef) -> Result<Vec<u8>, BlobFault> {
        let got = self.0.and_then(|dir| {
            std::fs::read(dir.join(&r.key))
                .or_else(|_| std::fs::read(dir.join(r.key.replace('/', "_"))))
                .ok()
        });
        queue::replay_object(r, got)
    }

    async fn judge_missing(&self, _topics: &[String], _r: &BeefRef) -> MissingVerdict {
        MissingVerdict::Fault("not in the directory".to_string())
    }
}

/// The reader's [`DeferredRows`]: the export, by the shipped statements, each
/// column typed as D1's deserialization types it (`record` and `part` TEXT,
/// `chunks` a number, `gen` TEXT, each but `record` and `part` nullable),
/// counting the bytes of JSON it hands out.
struct ExportRows<'a> {
    db: &'a Connection,
    json_bytes: std::cell::Cell<u64>,
}

/// A column of the type D1's row field takes, else the Worker's fault.
fn typed_text(value: &Value, nullable: bool, column: &str) -> Result<Option<String>, String> {
    match value {
        Value::Text(t) => Ok(Some(t.clone())),
        Value::Null if nullable => Ok(None),
        other => Err(format!(
            "column {column} is {:?}, not TEXT",
            other.data_type()
        )),
    }
}

impl DeferredRows for ExportRows<'_> {
    type Fault = String;

    async fn head(
        &self,
        host: &str,
        topic: &str,
        outpoint: &str,
    ) -> Result<Option<DeferredHead>, String> {
        let Some(row) = rows(
            self.db,
            gasp_deferred::DEFERRED_GRAPH_GET_SQL,
            &[&host, &topic, &outpoint],
        )
        .ok_or("the head read faulted")?
        .into_iter()
        .next() else {
            return Ok(None);
        };
        let chunks = match &row[1] {
            Value::Integer(i) => Some(*i as f64),
            Value::Real(r) => Some(*r),
            Value::Null => None,
            other => {
                return Err(format!(
                    "column chunks is {:?}, not a number",
                    other.data_type()
                ))
            }
        };
        let record = typed_text(&row[0], false, "record")?.unwrap_or_default();
        self.json_bytes
            .set(self.json_bytes.get() + record.len() as u64);
        Ok(Some(DeferredHead {
            record,
            chunks,
            gen: typed_text(&row[2], true, "gen")?,
        }))
    }

    async fn part(
        &self,
        host: &str,
        topic: &str,
        outpoint: &str,
        gen: &str,
        seq: u64,
    ) -> Result<Option<String>, String> {
        let found = rows(
            self.db,
            gasp_deferred::DEFERRED_GRAPH_CHUNK_GET_SQL,
            &[&host, &topic, &outpoint, &gen, &(seq as i64)],
        )
        .ok_or("the part read faulted")?;
        let part = match found.into_iter().next() {
            None => None,
            Some(row) => typed_text(&row[0], false, "part")?,
        };
        let n = part.as_ref().map_or(0, |p| p.len() as u64);
        self.json_bytes.set(self.json_bytes.get() + n);
        Ok(part)
    }
}

/// The reader runs the Worker's async reads to their end (no I/O awaits).
fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(f)
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

    // The deferred tables' own migrations, as the Worker's first request
    // applies them (an export taken before migrations 180-182 lacks them),
    // under the Worker's own tolerance of a re-run `ADD COLUMN`. A table the
    // export does not hold is not created: there is nothing of it to read.
    if rows(&db, "SELECT 1 FROM gasp_deferred_graphs LIMIT 0", &[]).is_some() {
        for sql in [
            gasp_deferred::DEFERRED_GRAPH_CHUNKS_CREATE,
            gasp_deferred::DEFERRED_GRAPHS_CHUNKS_COLUMN,
            gasp_deferred::DEFERRED_GRAPHS_GEN_COLUMN,
        ] {
            if let Err(e) = db.execute_batch(sql) {
                if !crate::d1::migration_error_is_benign(sql, &e.to_string()) {
                    return Err(format!("the deferred tables' migration: {e}"));
                }
            }
        }
    }

    // The Worker's read-back of a stored BEEF: `hex(beef) AS beef`, decoded
    // by `d1::beef_of_hex_column` (empty or undecodable: no bytes).
    for (table, sql) in [
        (
            "transactions",
            "SELECT txid, hex(beef) AS beef FROM transactions",
        ),
        (
            "transactions_evicted",
            "SELECT txid, hex(beef) AS beef FROM transactions_evicted",
        ),
        ("pot_beefs", "SELECT txid, hex(beef) AS beef FROM pot_beefs"),
    ] {
        for row in rows(&db, sql, &[]).unwrap_or_default() {
            s.rows += 1;
            let txid = text(&row[0]).unwrap_or_default();
            let Some(bytes) = crate::d1::beef_of_hex_column(text(&row[1])) else {
                continue;
            };
            s.read_beef(table, &txid, &bytes, Some(&txid));
        }
    }

    let table = "mutation_dead_letters";
    let letters = rows(
        &db,
        "SELECT txid, topics, message FROM mutation_dead_letters",
        &[],
    )
    .unwrap_or_default();
    for row in letters {
        s.rows += 1;
        let parked = crate::dead_letters::ParkedRow {
            txid: text(&row[0]).unwrap_or_default(),
            topics: text(&row[1]).unwrap_or_default(),
            fault: None,
            redrives: 0.0,
            redriven_at: None,
        };
        let key = format!("{}[{}]", parked.txid, parked.topics);
        let message = text(&row[2]).unwrap_or_default();
        if message.is_empty() || message == "{}" {
            continue; // a `failing` note holds no bytes
        }
        // The lever's message, then the consumer's read of it.
        let Some(body) = crate::dead_letters::redrive_message(&parked, &message, 1) else {
            s.refused(
                table,
                &key,
                None,
                "message",
                "the lever cannot re-drive it (redrive_message)",
            );
            continue;
        };
        let bytes = match block_on(queue::read_for_replay(&DirBytes(r2_dir), &body)) {
            ReadStep::Bytes(bytes) => bytes,
            ReadStep::Fault {
                fault,
                missing: true,
            } => {
                s.unread += 1;
                s.line(format!("UNREAD table={table} key={key} why={fault}"));
                continue;
            }
            ReadStep::Fault {
                fault,
                missing: false,
            } => {
                s.refused(table, &key, None, "read_for_replay", &fault);
                continue;
            }
            ReadStep::OverProof { fault } => {
                // lane INTAKE-BOUND: a parked letter the consumer would not read.
                s.refused(table, &key, None, "over_proof", &fault);
                continue;
            }
            ReadStep::Acked(verdict) => {
                // `DirBytes` judges no missing object: never reached.
                s.unread += 1;
                s.line(format!("UNREAD table={table} key={key} why={verdict:?}"));
                continue;
            }
        };
        if s.read_beef(table, &key, &bytes, None) {
            let verdict = submit_census::census_verdict(&bytes).as_str();
            *s.census.entry(verdict).or_default() += 1;
            // The replay's link, by the engine's subject rule (PORT-0811).
            let subject = beef_limits::parse_beef(&bytes, &beef_limits::ENGINE_BEEF_LIMITS)
                .ok()
                .and_then(|mut b| overlay_engine::subject::subject_txid_of(&mut b));
            s.read_linked(table, &key, &bytes, subject.as_deref());
        }
    }

    // PORT-0811: the raw transactions a Worker parses back from D1.
    for row in rows(
        &db,
        "SELECT txid, outputIndex, sweepTxid, sweepRawHex FROM hopsweep_records",
        &[],
    )
    .unwrap_or_default()
    {
        s.rows += 1;
        let key = format!(
            "{}:{}",
            text(&row[0]).unwrap_or_default(),
            text(&row[1]).unwrap_or_default()
        );
        let named = text(&row[2]);
        s.read_raw_tx("hopsweep_records", &key, text(&row[3]), named.as_deref());
    }
    for row in rows(
        &db,
        "SELECT txid, outputIndex, refundRawHex FROM potrefund_records WHERE refundRawHex IS NOT NULL",
        &[],
    )
    .unwrap_or_default()
    {
        s.rows += 1;
        let key = format!(
            "{}:{}",
            text(&row[0]).unwrap_or_default(),
            text(&row[1]).unwrap_or_default()
        );
        s.read_raw_tx("potrefund_records", &key, text(&row[2]), None);
    }

    // PORT-0811 fold: the deferred EF jobs' bytes at rest (`ef_deferred_jobs`,
    // their stream in `ef_deferred_chunks` or the R2 object
    // `ef-deferred/<reference>`), read back as the job's run reads them
    // (`ef_deferred::stream_of_chunks`, `checked_at_rest`, `job_submission`:
    // the length and the reference), then by the gated arm's readers.
    if rows(&db, "SELECT 1 FROM ef_deferred_jobs LIMIT 0", &[]).is_some() {
        if let Err(e) = db.execute_batch(crate::ef_deferred::JOBS_ADD_LEGS_FROM) {
            if !crate::d1::migration_error_is_benign(
                crate::ef_deferred::JOBS_ADD_LEGS_FROM,
                &e.to_string(),
            ) {
                return Err(format!("the deferred jobs' migration: {e}"));
            }
        }
    }
    let table = "ef_deferred_jobs";
    for row in rows(&db, "SELECT reference FROM ef_deferred_jobs", &[]).unwrap_or_default() {
        s.rows += 1;
        let reference = text(&row[0]).unwrap_or_default();
        let Some(job) = export_job(&db, &reference) else {
            s.refused(
                table,
                &reference,
                None,
                "job",
                "the job row does not read (JOB_READ_SQL)",
            );
            continue;
        };
        let at_rest = if job.at_rest == "r2" {
            let key = format!("{}{reference}", crate::ef_deferred::R2_PREFIX);
            match r2_dir.and_then(|dir| {
                std::fs::read(dir.join(&key))
                    .or_else(|_| std::fs::read(dir.join(key.replace('/', "_"))))
                    .ok()
            }) {
                Some(bytes) => Ok(bytes),
                None => {
                    s.unread += 1;
                    s.line(format!(
                        "UNREAD table={table} key={reference} why={key} not in the directory"
                    ));
                    continue;
                }
            }
        } else {
            let mut hexes = Vec::new();
            for idx in 0..job.chunks as i64 {
                let hex_chunk = rows(&db, crate::ef_deferred::CHUNK_READ_SQL, &[&reference, &idx])
                    .and_then(|r| r.into_iter().next())
                    .and_then(|r| text(&r[0]));
                let gone = hex_chunk.is_none();
                hexes.push(hex_chunk);
                if gone {
                    break;
                }
            }
            crate::ef_deferred::stream_of_chunks(&job, hexes)
        };
        let submission = match at_rest
            .and_then(|bytes| crate::ef_deferred::checked_at_rest(&job, bytes))
            .and_then(|bytes| crate::ef_deferred::job_submission(&job, bytes))
        {
            Ok(submission) => submission,
            Err(e) => {
                s.refused(table, &reference, None, "ef_job", &e);
                continue;
            }
        };
        let subject = Some(job.subject_txid.as_str()).filter(|t| !t.is_empty());
        if s.read_beef(table, &reference, &submission.beef, subject) {
            // The gated arm's own conversion: the run's EF batch.
            if let Err(e) = crate::ef::beef_to_ef_batch(&submission.beef) {
                s.refused(table, &reference, None, "ef_batch", &e.to_string());
            }
        }
    }

    // PORT-0811 fold: the handfile batches' payloads (`handfile_batches.payloadsHex`), read back by both Workers
    // through `HandfileBatch::of_stored` and served verbatim (no Worker parses a payload back; a client re-verifies
    // the batch). The potrefund payload carries the refund's raw transaction after its fixed head (the tag, the
    // identity, the game, the pot outpoint): it is read as the exploded row's `refundRawHex` is.
    let table = "handfile_batches";
    for row in rows(
        &db,
        "SELECT batchKey, hopTxid, payloadsHex, identitySigHex, settleSigHex FROM handfile_batches",
        &[],
    )
    .unwrap_or_default()
    {
        s.rows += 1;
        let col = |i: usize| text(&row[i]).unwrap_or_default();
        let key = col(0);
        let Some(batch) = overlay_discovery::handfile::HandfileBatch::of_stored(
            &key,
            &col(1),
            &col(2),
            &col(3),
            &col(4),
            0,
        ) else {
            s.refused(
                table,
                &key,
                None,
                "payloads",
                "the column does not read (HandfileBatch::of_stored)",
            );
            continue;
        };
        let head = overlay_discovery::potrefund::POTREFUND_TAG.len() + 33 + 32 + 32 + 4;
        match batch.payloads.get(3).and_then(|p| hex::decode(p).ok()) {
            Some(refund)
                if refund.len() > head
                    && refund.starts_with(overlay_discovery::potrefund::POTREFUND_TAG) =>
            {
                s.read_raw_tx(
                    table,
                    &format!("{key}#refund"),
                    Some(hex::encode(&refund[head..])),
                    None,
                );
            }
            _ => s.refused(
                table,
                &key,
                None,
                "payloads",
                "the potrefund payload does not read",
            ),
        }
    }

    let table = "gasp_deferred_graphs";
    let heads = rows(
        &db,
        "SELECT host, topic, outpoint FROM gasp_deferred_graphs",
        &[],
    )
    .unwrap_or_default();
    for row in heads {
        s.rows += 1;
        let (host, topic, outpoint) = (
            text(&row[0]).unwrap_or_default(),
            text(&row[1]).unwrap_or_default(),
            text(&row[2]).unwrap_or_default(),
        );
        let key = format!("{host}|{topic}|{outpoint}");
        let export = ExportRows {
            db: &db,
            json_bytes: std::cell::Cell::new(0),
        };
        let record = match block_on(gasp_deferred::read_deferred_graph(
            &export, &host, &topic, &outpoint,
        )) {
            Ok(DeferredRead::Record(record)) => {
                s.bytes += export.json_bytes.get();
                record
            }
            Ok(DeferredRead::Absent) => continue,
            Ok(DeferredRead::PartMissing { seq, gen }) => {
                // The Worker drops such a record (`get_deferred_graph`).
                s.unread += 1;
                s.line(format!(
                    "UNREAD table={table} key={key} why=part-{seq}-of-gen-{gen}-missing"
                ));
                continue;
            }
            Ok(DeferredRead::Unparsed(e)) => {
                s.refused(table, &key, None, "record", &e);
                continue;
            }
            Err(e) => {
                s.refused(table, &key, None, "row", &e);
                continue;
            }
        };
        s.deferred_records += 1;
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
    let verdict = match s.verdict() {
        Ok(()) => "VERDICT clean".to_string(),
        Err(e) => format!("VERDICT FAILED: {e}"),
    };
    s.line(verdict);
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
    s.verdict().unwrap();
}

#[cfg(test)]
mod self_test {
    use super::*;
    use crate::queue::MutationMessage;

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

    /// The schema as the Worker's migrations leave it, for the tables read.
    fn schema_sql() -> String {
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
        sql
    }

    /// A deferred-graph record of two nodes: a raw transaction and a proven
    /// one with its own BUMP.
    fn two_node_record() -> String {
        let valid = fixture("../../tools/lane-script/fixtures/valid.beef.hex");
        let manifest: serde_json::Value =
            serde_json::from_slice(&fixture("../../tools/lane-script/fixtures/manifest.json"))
                .unwrap();
        let funding = manifest["valid"]["funding_txid"].as_str().unwrap();
        let funding_tx = beef_limits::transaction_from_beef(
            &valid,
            Some(funding),
            &beef_limits::STORED_BEEF_LIMITS,
        )
        .unwrap();
        let proof = beef_limits::own_bump(&valid, funding).unwrap().to_hex();
        let node = |raw_tx: String, proof: Option<String>| {
            serde_json::json!({
                "node": {"graphID": "aa.0", "rawTx": raw_tx, "outputIndex": 0, "proof": proof},
                "spentBy": null
            })
        };
        serde_json::json!({
            "peer": "https://peer.example", "topic": "tm_test", "outpoint": "aa.0", "score": 1,
            "nodes": [
                node(manifest["valid"]["subject_raw_hex"].as_str().unwrap().into(), None),
                node(funding_tx.to_hex(), Some(proof)),
            ],
            "pending": [], "calls": 3, "passes": 1, "reason": "bytes"
        })
        .to_string()
    }

    /// The SQL of `record` cut into parts of `chunk` bytes, its parts
    /// written by `part_sql` (a part's value as SQL).
    fn record_sql(record: &str, chunk: usize, part_sql: impl Fn(&str) -> String) -> String {
        let plan = crate::gasp_deferred::chunk_plan(record, chunk);
        let mut sql = format!(
            "INSERT INTO \"gasp_deferred_graphs\" VALUES('https://peer.example','tm_test','aa.0',1,2,0,3,1,'bytes',{},{},1,1,{},{});\n",
            record.len(),
            sql_text(plan.head),
            plan.rest.len(),
            sql_text(&plan.gen)
        );
        for (i, part) in plan.rest.iter().enumerate() {
            sql.push_str(&format!(
                "INSERT INTO \"gasp_deferred_graph_chunks\" VALUES('https://peer.example','tm_test','aa.0',{},{},{},1);\n",
                sql_text(&plan.gen),
                i + 1,
                part_sql(part)
            ));
        }
        sql
    }

    /// The reader over one SQL export written to a fresh directory.
    fn read_sql(name: &str, sql: &str, r2: Option<&Path>) -> Summary {
        let dir = std::env::temp_dir().join(format!("e585-land-f6-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let export = dir.join("export.sql");
        std::fs::write(&export, sql).unwrap();
        let s = read_export(&[export], r2).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        s
    }

    /// The reader's own source, its tests cut off.
    fn reader_source() -> &'static str {
        let whole = include_str!("stored_rows.rs");
        &whole[..whole.find("mod self_test {").unwrap()]
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
        let mut sql = schema_sql();

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
            ef_job: None,
            gated: false,
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

        // A deferred-graph record of two nodes, cut into three parts.
        let record = two_node_record();
        let plan = crate::gasp_deferred::chunk_plan(&record, record.len() / 3 + 1);
        assert_eq!(plan.rest.len(), 2, "the record is cut into three parts");
        total += record.len() as u64;
        sql.push_str(&record_sql(&record, record.len() / 3 + 1, sql_text));
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
        let n = s.lines.len();
        assert!(s.lines[n - 2].starts_with("SUMMARY rows="));
        assert_eq!(s.lines[n - 1], "VERDICT clean");
        assert_eq!(s.verdict(), Ok(()));

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
            "UNREAD table=mutation_dead_letters key=gone[tm_test] why=the R2 object mutations/00/11 is MISSING"
        )), "{:?}", s.lines);

        // The JSON form: one file a table, a BLOB as an array of bytes.
        let json = serde_json::json!([{
            "results": [
                {"txid": txid_of(honest), "beef": honest.iter().map(|b| u64::from(*b)).collect::<Vec<_>>()},
                {"txid": "corrupted", "beef": corrupted.iter().map(|b| u64::from(*b)).collect::<Vec<_>>()},
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

    // ── The land lens E585-LAND-L3: the reader calls the Worker's reads ──

    /// THE PIN (L3, the letters). A parked letter's bytes are read as the
    /// lever re-drives them and the consumer reads them
    /// (`dead_letters::redrive_message`, then `queue::read_for_replay`):
    /// (a) an inline body that is no base64 is the consumer's own fault
    /// text; (b) a message with neither `r2` nor `beefB64` whose `r2_key`
    /// COLUMN names a valid object is refused, as the consumer (which reads
    /// `body.r2` alone) refuses it; (c) a keyed object with a flipped byte is
    /// the R2 read's own refusal (`queue::replay_object`). RED on `e2c561c`:
    /// (b) read the column's object clean (`refusals` 0, not 1).
    #[test]
    fn e585_land_f6_l3_a_letter_is_read_by_the_consumers_own_read() {
        let dir = std::env::temp_dir().join(format!("e585-land-f6-r2-{}", std::process::id()));
        let keyed = fixture("../overlay-engine/tests/fixtures/zanaadu-pf-head-dc4ca9ea.beef");
        let sha = hex::encode(bsv_rs::primitives::hash::sha256(&keyed));
        let topics = vec!["tm_test".to_string()];
        let r = BeefRef {
            key: queue::r2_key(&sha, &topics, "historical-tx"),
            sha256: sha,
            bytes: keyed.len() as u64,
            txid: Some(txid_of(&keyed)),
        };
        std::fs::create_dir_all(dir.join(Path::new(&r.key).parent().unwrap())).unwrap();
        let letter = |txid: &str, message: &serde_json::Value, column: Option<&str>| {
            format!(
                "INSERT INTO \"mutation_dead_letters\" (txid, topics, message, status, first_seen_at, r2_key) VALUES({},'tm_test',{},'parked',1,{});\n",
                sql_text(txid),
                sql_text(&message.to_string()),
                column.map_or("NULL".to_string(), sql_text)
            )
        };
        let base = serde_json::json!({"topics": ["tm_test"], "mode": "historical-tx",
            "reason": queue::REPLAY_REASON_PHASE3_FAULT});

        // (a)
        let mut a = base.clone();
        a["beef_b64"] = "%%not base64%%".into();
        let s = read_sql("a", &(schema_sql() + &letter("aa", &a, None)), None);
        let expected = match block_on(queue::read_for_replay(
            &DirBytes(None),
            &serde_json::from_value(a).unwrap(),
        )) {
            ReadStep::Fault { fault, .. } => fault,
            other => panic!("{other:?}"),
        };
        assert_eq!(s.refusals, 1, "{:?}", s.lines);
        assert!(
            s.lines[0].starts_with(&format!(
                "REFUSED table=mutation_dead_letters key=aa[tm_test] offset=- kind=read_for_replay detail={expected}"
            )),
            "{:?}",
            s.lines
        );

        // (b)
        std::fs::write(dir.join(&r.key), &keyed).unwrap();
        let s = read_sql(
            "b",
            &(schema_sql() + &letter("bb", &base, Some(&r.key))),
            Some(&dir),
        );
        assert_eq!(
            (s.refusals, s.beefs),
            (1, 0),
            "the r2_key column is not the consumer's read: {:?}",
            s.lines
        );
        assert!(s.lines[0].contains("kind=read_for_replay detail=invalid base64 BEEF"));

        // (c)
        let mut flipped = keyed.clone();
        flipped[keyed.len() / 2] ^= 0x01;
        std::fs::write(dir.join(&r.key), &flipped).unwrap();
        let mut c = base.clone();
        c["r2"] = serde_json::to_value(&r).unwrap();
        let s = read_sql(
            "c",
            &(schema_sql() + &letter("cc", &c, Some(&r.key))),
            Some(&dir),
        );
        assert_eq!(s.refusals, 1, "{:?}", s.lines);
        assert!(
            s.lines[0].contains("detail=the R2 object was refused (the object"),
            "{:?}",
            s.lines
        );
        let _ = std::fs::remove_dir_all(&dir);

        // The shape: the reader calls the Worker's reads, and has none of
        // its own.
        let source = reader_source();
        for call in [
            "crate::dead_letters::redrive_message(",
            "queue::read_for_replay(",
            "queue::replay_object(",
        ] {
            assert!(source.contains(call), "the reader calls {call}");
        }
        for own in ["STANDARD.decode(", "check_replay_blob(", "decode_beef_b64("] {
            assert!(!source.contains(own), "the reader reads by its own {own}");
        }
    }

    /// THE PIN (L3, the deferred graph). A record is read by
    /// `gasp_deferred::read_deferred_graph`, the Worker's own read, over the
    /// shipped statements and D1's column types: a part stored as a BLOB is
    /// the Worker's fault (D1 deserializes `part` as a String) and is
    /// refused; a part that is gone is UNREAD and fails the run. RED on
    /// `e2c561c`: the BLOB part was read as UTF-8 text and the record read
    /// clean (`refusals` 0, not 1).
    #[test]
    fn e585_land_f6_l3_a_deferred_record_is_read_as_the_worker_reads_it() {
        let record = two_node_record();
        let chunk = record.len() / 3 + 1;
        let s = read_sql(
            "text",
            &(schema_sql() + &record_sql(&record, chunk, sql_text)),
            None,
        );
        assert_eq!(s.verdict(), Ok(()), "{:?}", s.lines);
        assert_eq!((s.deferred_records, s.deferred_nodes), (1, 2));

        let as_blob = |part: &str| format!("X'{}'", hex::encode(part));
        let s = read_sql(
            "blob",
            &(schema_sql() + &record_sql(&record, chunk, as_blob)),
            None,
        );
        assert_eq!((s.refusals, s.deferred_records), (1, 0), "{:?}", s.lines);
        assert!(
            s.lines[0].starts_with(
                "REFUSED table=gasp_deferred_graphs key=https://peer.example|tm_test|aa.0 offset=- kind=row detail=column part is Blob, not TEXT"
            ),
            "{:?}",
            s.lines
        );

        let torn: String = record_sql(&record, chunk, sql_text)
            .lines()
            .filter(|l| !l.contains("gasp_deferred_graph_chunks") || !l.contains(",2,"))
            .map(|l| format!("{l}\n"))
            .collect();
        let s = read_sql("torn", &(schema_sql() + &torn), None);
        assert_eq!((s.refusals, s.unread), (0, 1), "{:?}", s.lines);
        assert!(s.verdict().is_err());

        let source = reader_source();
        assert!(source.contains("gasp_deferred::read_deferred_graph("));
        assert!(
            !source.contains("SELECT part FROM"),
            "the reader reads a part by the shipped statement"
        );
    }

    /// THE PIN (L3, the column type). A BEEF stored as hex TEXT is read back
    /// as the Worker reads it (`hex(beef)`, `d1::beef_of_hex_column`): the
    /// hex of its ASCII bytes, which the door refuses. RED on `e2c561c`: the
    /// reader hex-decoded the TEXT and read it clean (`refusals` 0, not 1).
    /// The link bound's shape: a proven parent of `n` `OP_1` outputs, every
    /// one spent by one child; 1,400 of each are past the engine's link bound
    /// (`beef_limits::LINK_HEAP_MAX_BYTES`).
    fn shared_parent_beef(n: u32) -> (Vec<u8>, String) {
        use bsv_rs::script::{LockingScript, Script, UnlockingScript};
        use bsv_rs::transaction::{
            Beef, MerklePath, MerklePathLeaf, Transaction, TransactionInput, TransactionOutput,
        };
        let op_1 = || LockingScript::from_script(Script::from_binary(&[0x51]).unwrap());
        let mut parent = Transaction::new();
        parent.inputs.push(TransactionInput {
            source_txid: Some("aa".repeat(32)),
            source_output_index: 0,
            unlocking_script: Some(UnlockingScript::from_script(Script::new())),
            ..Default::default()
        });
        parent.outputs = (0..n).map(|_| TransactionOutput::new(1, op_1())).collect();
        let parent_txid = parent.id();
        let mut child = Transaction::new();
        for vout in 0..n {
            child.inputs.push(TransactionInput {
                source_txid: Some(parent_txid.clone()),
                source_output_index: vout,
                unlocking_script: Some(UnlockingScript::from_script(Script::new())),
                ..Default::default()
            });
        }
        child.outputs.push(TransactionOutput::new(1, op_1()));
        let mut beef = Beef::new();
        let bump = beef.merge_bump(
            MerklePath::new(
                900_000,
                vec![vec![MerklePathLeaf::new_txid(0, parent_txid)]],
            )
            .unwrap(),
        );
        beef.merge_raw_tx(parent.to_binary(), Some(bump));
        beef.merge_raw_tx(child.to_binary(), None);
        (beef.to_binary(), child.id())
    }

    /// PORT-0811, the reader's extension: one run names every stored row the
    /// 0.8.11 readers refuse or read differently. A raw transaction column
    /// holding a byte after the transaction (`hopsweep_records.sweepRawHex`,
    /// `potrefund_records.refundRawHex`) is `REFUSED .. kind=raw_tx`; a stored
    /// BEEF the engine's link refuses for its re-materialization is `REFUSED ..
    /// kind=link_bound`; a stored body that ends with its proven ancestor (the
    /// order a 0.7.2 build could write) is a `DIFFERS` line, never a refusal;
    /// the honest rows read clean. On the base the reader read no raw-tx
    /// column, linked without a bound and named neither class.
    #[test]
    fn p0811_r_the_reader_names_the_rows_0811_refuses_or_reads_otherwise() {
        let honest_beef = fixture("tests/fixtures/ef/loop2_join_f9e85aab_represent.beef");
        let parsed =
            beef_limits::parse_beef(&honest_beef, &beef_limits::STORED_BEEF_LIMITS).unwrap();
        let honest_tx = parsed.txs.iter().find_map(|e| e.tx().cloned()).unwrap();
        let (honest_raw, honest_txid) = (honest_tx.to_binary(), honest_tx.id());
        let mut trailing = honest_raw.clone();
        trailing.push(0x00);
        let (bound_beef, bound_txid) = shared_parent_beef(1_400);
        // A stored body ending with its proven ancestor, written by hand
        // (`Beef::to_binary` sorts a source first).
        let parent_entry = parsed
            .txs
            .iter()
            .find(|e| e.bump_index().is_some() && e.tx().is_some())
            .expect("the fixture carries a proven parent");
        let parent_tx = parent_entry.tx().unwrap().clone();
        let parent_bump = parsed.bumps[parent_entry.bump_index().unwrap()].clone();
        let spender = parsed
            .txs
            .iter()
            .filter_map(|e| e.tx())
            .find(|t| {
                t.inputs
                    .iter()
                    .any(|i| i.source_txid.as_deref() == Some(parent_tx.id().as_str()))
            })
            .expect("the fixture spends its proven parent")
            .clone();
        let mut reversed = vec![0x01, 0x00, 0xbe, 0xef, 0x01];
        reversed.extend(parent_bump.to_binary());
        reversed.push(0x02);
        reversed.extend(spender.to_binary());
        reversed.push(0x00);
        reversed.extend(parent_tx.to_binary());
        reversed.extend([0x01, 0x00]);

        let mut sql = schema_sql();
        sql.push_str(
            "CREATE TABLE hopsweep_records (identity TEXT, gameId TEXT, hopTxid TEXT, hopVout INTEGER, sweepTxid TEXT, sweepRawHex TEXT, txid TEXT, outputIndex INTEGER, createdAt INTEGER);\n\
             CREATE TABLE potrefund_records (txid TEXT, outputIndex INTEGER, refundRawHex TEXT);\n",
        );
        let sweep = |key: &str, raw: &[u8]| {
            format!(
                "INSERT INTO hopsweep_records VALUES('i','g','h',0,{},{},{},0,1);\n",
                sql_text(&honest_txid),
                sql_text(&hex::encode(raw)),
                sql_text(key)
            )
        };
        sql.push_str(&sweep("sweep-honest", &honest_raw));
        sql.push_str(&sweep("sweep-trailing", &trailing));
        sql.push_str(&format!(
            "INSERT INTO potrefund_records VALUES('refund-trailing',0,{});\n",
            sql_text(&hex::encode(&trailing))
        ));
        sql.push_str(&format!(
            "INSERT INTO pot_beefs VALUES({},{},1);\n",
            sql_text(&bound_txid),
            sql_blob(&bound_beef)
        ));
        sql.push_str(&format!(
            "INSERT INTO \"transactions\" VALUES({},{},0);\n",
            sql_text(&spender.id()),
            sql_blob(&reversed)
        ));
        let s = read_sql("p0811-r", &sql, None);
        let named = |prefix: &str| s.lines.iter().any(|l| l.starts_with(prefix));
        assert!(
            named("REFUSED table=hopsweep_records key=sweep-trailing:0 offset=- kind=raw_tx"),
            "{:?}",
            s.lines
        );
        assert!(
            !named("REFUSED table=hopsweep_records key=sweep-honest"),
            "{:?}",
            s.lines
        );
        assert!(
            named("REFUSED table=potrefund_records key=refund-trailing:0 offset=- kind=raw_tx"),
            "{:?}",
            s.lines
        );
        assert!(
            named(&format!(
                "REFUSED table=pot_beefs key={bound_txid} offset=- kind=link_bound"
            )),
            "{:?}",
            s.lines
        );
        assert!(
            named(&format!(
                "DIFFERS table=transactions key={} kind=unnamed_subject",
                spender.id()
            )),
            "{:?}",
            s.lines
        );
        assert!(
            !named(&format!("REFUSED table=transactions key={}", spender.id())),
            "the reversed body is read by its txid: {:?}",
            s.lines
        );
        assert_eq!(s.refusals, 3, "{:?}", s.lines);
        assert!(s.verdict().is_err());
    }

    #[test]
    fn e585_land_f6_l3_a_beef_stored_as_hex_text_is_refused_as_at_the_worker() {
        let honest = fixture("tests/fixtures/ef/loop2_join_f9e85aab_represent.beef");
        let txid = txid_of(&honest);
        let row = |beef: String| {
            format!(
                "INSERT INTO \"transactions\" VALUES({},{beef},0);\n",
                sql_text(&txid)
            )
        };
        let s = read_sql("blob-row", &(schema_sql() + &row(sql_blob(&honest))), None);
        assert_eq!((s.beefs, s.refusals), (1, 0), "{:?}", s.lines);
        let s = read_sql(
            "text-row",
            &(schema_sql() + &row(sql_text(&hex::encode(&honest)))),
            None,
        );
        assert_eq!((s.beefs, s.refusals), (1, 1), "{:?}", s.lines);
        assert!(
            s.lines[0].starts_with(&format!("REFUSED table=transactions key={txid} offset=0")),
            "{:?}",
            s.lines
        );
        // What the Worker's read-back gives for that TEXT: its ASCII bytes.
        let db = Connection::open_in_memory().unwrap();
        let back: String = db
            .query_row("SELECT hex(?1)", [hex::encode(&honest)], |r| r.get(0))
            .unwrap();
        assert_eq!(
            crate::d1::beef_of_hex_column(Some(back)),
            Some(hex::encode(&honest).into_bytes())
        );
        assert!(
            !reader_source().contains("fn blob("),
            "no reader of its own typing"
        );
    }

    /// THE PIN (L3, the verdict). An UNREAD row FAILS the run: a letter
    /// whose object is not in the directory, and nothing refused, is a
    /// failed verdict and a `VERDICT FAILED` line. RED on `e2c561c`: the run
    /// asserted `refusals == 0` alone and passed over the unread row.
    #[test]
    fn e585_land_f6_l3_an_unread_row_fails_the_run() {
        let keyed = fixture("../overlay-engine/tests/fixtures/zanaadu-pf-head-dc4ca9ea.beef");
        let sha = hex::encode(bsv_rs::primitives::hash::sha256(&keyed));
        let r = BeefRef {
            key: queue::r2_key(&sha, &["tm_test".to_string()], "historical-tx"),
            sha256: sha,
            bytes: keyed.len() as u64,
            txid: Some(txid_of(&keyed)),
        };
        let message = serde_json::json!({"r2": r, "topics": ["tm_test"],
            "mode": "historical-tx", "reason": queue::REPLAY_REASON_PHASE3_FAULT});
        let sql = schema_sql()
            + &format!(
                "INSERT INTO \"mutation_dead_letters\" (txid, topics, message, status, first_seen_at) VALUES('dd','tm_test',{},'parked',1);\n",
                sql_text(&message.to_string())
            );
        let s = read_sql("unread", &sql, None);
        assert_eq!((s.refusals, s.unread), (0, 1), "{:?}", s.lines);
        let verdict = s.verdict().unwrap_err();
        assert!(
            verdict.starts_with("0 refused and 1 unread rows"),
            "{verdict}"
        );
        assert!(s
            .lines
            .last()
            .unwrap()
            .starts_with("VERDICT FAILED: 0 refused and 1 unread"));
        // The captain's run asserts the verdict, not the refusals alone.
        let whole = include_str!("stored_rows.rs");
        let run = &whole[whole.find("fn read_the_export()").unwrap()..];
        assert!(run[..run.find("\n}\n").unwrap()].contains("s.verdict().unwrap();"));
    }

    /// PORT-0811 fold: the deferred EF jobs' bytes at rest and the handfile
    /// payloads. A job held in two D1 chunks and one held in R2 read clean; a
    /// torn job (its chunks short of its length), a job whose bytes are not its
    /// reference's and a job whose BEEF the gated arm's conversion refuses are
    /// each named; an R2 job whose object is not in the directory is `UNREAD`.
    /// An honest handfile batch reads clean; one whose refund payload carries a
    /// byte after the transaction is `kind=raw_tx`; a payloads column that does
    /// not read is `kind=payloads`. On the base the reader read neither table.
    #[test]
    fn p0811_r_the_reader_reads_the_deferred_jobs_and_the_handfile_payloads() {
        use crate::ef_deferred::{job_reference, CHUNKS_CREATE, JOBS_ADD_LEGS_FROM, JOBS_CREATE};
        let beef = fixture("tests/fixtures/ef/loop2_join_f9e85aab_represent.beef");
        let (_, subject) = crate::ef::beef_to_ef_batch(&beef).unwrap();
        let mut sql = schema_sql();
        sql.push_str(&format!(
            "{JOBS_CREATE};\n{JOBS_ADD_LEGS_FROM};\n{CHUNKS_CREATE};\n"
        ));
        sql.push_str(crate::d1::HANDFILE_BATCHES_CREATE);
        sql.push_str(";\n");
        let job_row = |reference: &str,
                       topics: &str,
                       subject: &str,
                       beef_len: usize,
                       bytes: usize,
                       at_rest: &str,
                       chunks: usize| {
            format!(
                "INSERT INTO ef_deferred_jobs VALUES({},{},{},'broadcast-gated',0,{beef_len},{bytes},{},{chunks},1,1,'queued',0,NULL,NULL,1,1,0);\n",
                sql_text(reference),
                sql_text(subject),
                sql_text(topics),
                sql_text(at_rest)
            )
        };
        let chunk_row = |reference: &str, idx: usize, part: &[u8]| {
            format!(
                "INSERT INTO ef_deferred_chunks VALUES({},{idx},{});\n",
                sql_text(reference),
                sql_blob(part)
            )
        };
        // d1, clean: two chunks
        let t1 = r#"["tm_one"]"#;
        let r1 = job_reference(t1, &beef, None);
        sql.push_str(&job_row(&r1, t1, &subject, beef.len(), beef.len(), "d1", 2));
        let half = beef.len() / 2;
        sql.push_str(&chunk_row(&r1, 0, &beef[..half]));
        sql.push_str(&chunk_row(&r1, 1, &beef[half..]));
        // d1, torn: the second chunk is gone
        let t2 = r#"["tm_two"]"#;
        let r2 = job_reference(t2, &beef, None);
        sql.push_str(&job_row(&r2, t2, &subject, beef.len(), beef.len(), "d1", 2));
        sql.push_str(&chunk_row(&r2, 0, &beef[..half]));
        // d1, foreign: whole bytes under another submission's reference
        let t3 = r#"["tm_three"]"#;
        let r3 = job_reference(t3, b"other bytes", None);
        sql.push_str(&job_row(&r3, t3, &subject, beef.len(), beef.len(), "d1", 1));
        sql.push_str(&chunk_row(&r3, 0, &beef));
        // d1, the conversion refuses: a subject whose source the BEEF lacks
        let lone = {
            let mut tx = bsv_rs::transaction::Transaction::new();
            tx.inputs.push(bsv_rs::transaction::TransactionInput {
                source_txid: Some("ab".repeat(32)),
                unlocking_script: Some(bsv_rs::script::UnlockingScript::from_binary(&[]).unwrap()),
                ..Default::default()
            });
            tx.outputs.push(bsv_rs::transaction::TransactionOutput::new(
                1,
                bsv_rs::script::LockingScript::from_binary(&[0x51]).unwrap(),
            ));
            let mut b = bsv_rs::transaction::Beef::new();
            b.merge_transaction(tx.clone());
            (b.to_binary(), tx.id())
        };
        let t4 = r#"["tm_four"]"#;
        let r4 = job_reference(t4, &lone.0, None);
        sql.push_str(&job_row(
            &r4,
            t4,
            &lone.1,
            lone.0.len(),
            lone.0.len(),
            "d1",
            1,
        ));
        sql.push_str(&chunk_row(&r4, 0, &lone.0));
        // r2, clean, and r2, its object not in the directory
        let t5 = r#"["tm_five"]"#;
        let r5 = job_reference(t5, &beef, None);
        sql.push_str(&job_row(&r5, t5, &subject, beef.len(), beef.len(), "r2", 1));
        let t6 = r#"["tm_six"]"#;
        let r6 = job_reference(t6, &beef, None);
        sql.push_str(&job_row(&r6, t6, &subject, beef.len(), beef.len(), "r2", 1));
        // the handfile batches
        let parsed = beef_limits::parse_beef(&beef, &beef_limits::STORED_BEEF_LIMITS).unwrap();
        let raw = parsed
            .txs
            .iter()
            .find_map(|e| e.tx().cloned())
            .unwrap()
            .to_binary();
        let refund_payload = |raw: &[u8]| {
            let mut p = overlay_discovery::potrefund::POTREFUND_TAG.to_vec();
            p.extend([0x02; 33]);
            p.extend([0x11; 32]);
            p.extend([0x22; 32]);
            p.extend(0u32.to_le_bytes());
            p.extend_from_slice(raw);
            hex::encode(p)
        };
        let batch = |key: &str, payloads: &str| {
            format!(
                "INSERT INTO handfile_batches VALUES({},'i','g','h',{},'30','30',1);\n",
                sql_text(key),
                sql_text(payloads)
            )
        };
        let mut trailing = raw.clone();
        trailing.push(0x00);
        sql.push_str(&batch(
            "handfile:honest",
            &serde_json::json!(["00", "00", "00", refund_payload(&raw)]).to_string(),
        ));
        sql.push_str(&batch(
            "handfile:trailing",
            &serde_json::json!(["00", "00", "00", refund_payload(&trailing)]).to_string(),
        ));
        sql.push_str(&batch("handfile:unread", r#"["00"]"#));

        let dir = std::env::temp_dir().join(format!("p0811-fold-r2-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(format!("ef-deferred_{r5}")), &beef).unwrap();
        let s = read_sql("p0811-fold-r", &sql, Some(&dir));
        let _ = std::fs::remove_dir_all(&dir);
        let named = |prefix: &str| s.lines.iter().any(|l| l.starts_with(prefix));
        for clean in [&r1, &r5] {
            assert!(
                !s.lines
                    .iter()
                    .any(|l| (l.starts_with("REFUSED") || l.starts_with("UNREAD"))
                        && l.contains(&format!("key={clean} "))),
                "{clean} reads clean: {:?}",
                s.lines
            );
        }
        assert!(named(&format!("REFUSED table=ef_deferred_jobs key={r2} offset=- kind=ef_job detail=chunk 1 is gone")), "{:?}", s.lines);
        assert!(named(&format!("REFUSED table=ef_deferred_jobs key={r3} offset=- kind=ef_job detail=the bytes at rest are not the job's")), "{:?}", s.lines);
        assert!(
            named(&format!(
                "REFUSED table=ef_deferred_jobs key={r4} offset=- kind=ef_batch"
            )),
            "{:?}",
            s.lines
        );
        assert!(
            named(&format!("UNREAD table=ef_deferred_jobs key={r6} ")),
            "{:?}",
            s.lines
        );
        assert!(
            !named("REFUSED table=handfile_batches key=handfile:honest"),
            "{:?}",
            s.lines
        );
        assert!(
            named(
                "REFUSED table=handfile_batches key=handfile:trailing#refund offset=- kind=raw_tx"
            ),
            "{:?}",
            s.lines
        );
        assert!(
            named("REFUSED table=handfile_batches key=handfile:unread offset=- kind=payloads"),
            "{:?}",
            s.lines
        );
        assert_eq!((s.refusals, s.unread), (5, 1), "{:?}", s.lines);
        assert!(s.verdict().is_err());
    }
}
