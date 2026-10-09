//! bsv-low #555: the DEFERRED GASP graphs (measured on beta as #582).
//!
//! A graph whose walk passes its per-graph budget (`Engine::set_graph_budget`,
//! [`GASP_GRAPH_BUDGET_CALLS`] calls or [`GASP_GRAPH_BUDGET_MS`] in one pass;
//! since bsv-low #586 [`GASP_GRAPH_BUDGET_BYTES`] served or
//! [`GASP_GRAPH_BUDGET_NODES`] appended, `Engine::set_graph_budget_limbs`) is
//! deferred by the engine: its partial walk is kept as ONE record per (peer,
//! topic, root outpoint), REPLACED on every deferral, deleted when the graph
//! converges or is dropped, and resumed by the next pass that is served its
//! UTXO. A record is one row of `gasp_deferred_graphs` and, when its JSON is
//! past [`DEFERRED_GRAPH_CHUNK_BYTES`], the rows of
//! `gasp_deferred_graph_chunks` that hold the rest (bsv-low #585: a record has
//! no byte bound; D1's 2 MB row is routed around, not made a limit on a
//! graph). This module holds the tables, their statements (the `Storage`
//! methods in `d1_storage.rs` run them), the health block
//! `/health/invariants.gasp.deferredGraphs` and the counters.

use serde::Deserialize;
use worker::D1Database;

use crate::d1::Query;

/// Calls one graph may make in one pass on this worker. The engine's default
/// ([`overlay_engine::gasp::DEFAULT_GRAPH_BUDGET_CALLS`]).
pub const GASP_GRAPH_BUDGET_CALLS: u32 = overlay_engine::gasp::DEFAULT_GRAPH_BUDGET_CALLS;

/// Wall-clock one graph may take in one pass on this worker, ms: half of the
/// 30 s per-peer budget (`GASP_PEER_SYNC_BUDGET_MS`), so a deep graph leaves
/// the peer's other UTXOs the other half. Under the per-peer budget, as
/// `Engine::set_graph_budget` asks.
pub const GASP_GRAPH_BUDGET_MS: u64 = 15_000;

/// Bytes one graph may be SERVED in one pass on this worker (bsv-low #586):
/// the hex of each node's raw transaction and proof, as the peer sends it and
/// a record keeps it. Reached, the graph is DEFERRED as at the calls (reason
/// `bytes`), never dropped or refused: a budget per pass, not a limit.
///
/// The DEFAULT is the engine's
/// ([`overlay_engine::gasp::DEFAULT_GRAPH_BUDGET_BYTES`], 917,504: 18 heads
/// of 26 KB a fresh pass). It was seven eighths of a 1 MiB cap on a record;
/// a record has no cap since bsv-low #585, so it is a budget per pass and
/// nothing else. An operator names another with the var of the same name
/// ([`graph_budget_limbs`]).
pub const GASP_GRAPH_BUDGET_BYTES: u64 = overlay_engine::gasp::DEFAULT_GRAPH_BUDGET_BYTES;

/// Nodes one graph may APPEND in one pass on this worker (bsv-low #586). The
/// DEFAULT, the engine's ([`overlay_engine::gasp::DEFAULT_GRAPH_BUDGET_NODES`],
/// 64). Reached, the graph is deferred (reason `nodes`). The var of the same
/// name sets another ([`graph_budget_limbs`]).
pub const GASP_GRAPH_BUDGET_NODES: u32 = overlay_engine::gasp::DEFAULT_GRAPH_BUDGET_NODES;

/// The names of the two vars (`[vars]` of a wrangler config): the consts'.
pub const GASP_GRAPH_BUDGET_BYTES_VAR: &str = "GASP_GRAPH_BUDGET_BYTES";
pub const GASP_GRAPH_BUDGET_NODES_VAR: &str = "GASP_GRAPH_BUDGET_NODES";

/// The two limbs this worker runs with, (bytes, nodes), from the vars
/// `GASP_GRAPH_BUDGET_BYTES` and `GASP_GRAPH_BUDGET_NODES` as the operator
/// set them (the lens fold's E586-L1: they were compile-time, so the remedy
/// `CLAUDE.md` named, "set the limbs under the cap", needed a rebuild). A
/// decimal integer, CLAMPED: bytes to at least 1 (no upper bound since
/// bsv-low #585: there is no record cap for the limb to sit under; a limb
/// nobody reaches leaves the calls, the time and the nodes to cut the pass),
/// nodes to 1 ..= [`GASP_GRAPH_BUDGET_CALLS`] (a node appended is a call
/// made, so more would never be reached). Unset, empty or not a number: the
/// default const. Never 0: a limb of 0 is spent before the pass's first
/// request, and the walk would append nothing on every pass.
pub fn graph_budget_limbs(bytes: Option<&str>, nodes: Option<&str>) -> (u64, u32) {
    let named = |v: Option<&str>| v.and_then(|v| v.trim().parse::<u64>().ok());
    (
        named(bytes).map_or(GASP_GRAPH_BUDGET_BYTES, |b| b.max(1)),
        named(nodes).map_or(GASP_GRAPH_BUDGET_NODES, |n| {
            n.clamp(1, u64::from(GASP_GRAPH_BUDGET_CALLS)) as u32
        }),
    )
}

/// [`graph_budget_limbs`] over this worker's environment.
pub fn graph_budget_limbs_from_env(env: &worker::Env) -> (u64, u32) {
    let var = |name: &str| env.var(name).ok().map(|v| v.to_string());
    graph_budget_limbs(
        var(GASP_GRAPH_BUDGET_BYTES_VAR).as_deref(),
        var(GASP_GRAPH_BUDGET_NODES_VAR).as_deref(),
    )
}

/// The table: one row per deferred graph. `record` is the engine's
/// `DeferredGraph` as JSON, or its first [`DEFERRED_GRAPH_CHUNK_BYTES`] when
/// the JSON is longer (`chunks` then counts the rows of
/// [`DEFERRED_GRAPH_CHUNKS_CREATE`] that hold the rest, under `gen`); `bytes`
/// is the WHOLE record's size and the other columns are its summary for the
/// health block. Transient: a lost row costs the walk again from its root,
/// nothing else.
pub const DEFERRED_GRAPHS_CREATE: &str = "CREATE TABLE IF NOT EXISTS gasp_deferred_graphs (
        host TEXT NOT NULL,
        topic TEXT NOT NULL,
        outpoint TEXT NOT NULL,
        score REAL NOT NULL,
        nodes INTEGER NOT NULL,
        pending INTEGER NOT NULL,
        calls INTEGER NOT NULL,
        passes INTEGER NOT NULL,
        reason TEXT NOT NULL,
        bytes INTEGER NOT NULL,
        record TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (host, topic, outpoint)
    )";

/// The most one row holds of a record's JSON (bsv-low #585): 1 MiB, half of
/// D1's 2 MB row and the size a record was capped at before (so a record up
/// to it is written exactly as it was: one row, one statement). A longer
/// record is CHUNKED: its first part in its `gasp_deferred_graphs` row, each
/// further part a row of `gasp_deferred_graph_chunks`. Never a limit on a
/// record: a platform bound routed around.
pub const DEFERRED_GRAPH_CHUNK_BYTES: usize = 1 << 20;

/// The further parts of a record past [`DEFERRED_GRAPH_CHUNK_BYTES`] (bsv-low
/// #585), one row per part: `seq` from 1 (part 0 is the head row's `record`),
/// `gen` the generation the head row names. A save writes the parts of a NEW
/// generation first, then the head row (the ceiling is read there), then
/// deletes the other generations: a reader always finds the parts of the
/// generation its head names, and a save that faults or is refused midway
/// leaves the held record whole. Transient, as the head table.
pub const DEFERRED_GRAPH_CHUNKS_CREATE: &str =
    "CREATE TABLE IF NOT EXISTS gasp_deferred_graph_chunks (
        host TEXT NOT NULL,
        topic TEXT NOT NULL,
        outpoint TEXT NOT NULL,
        gen TEXT NOT NULL,
        seq INTEGER NOT NULL,
        part TEXT NOT NULL,
        written_at INTEGER NOT NULL,
        PRIMARY KEY (host, topic, outpoint, gen, seq)
    )";

/// The head row's two columns for a chunked record: how many rows of
/// [`DEFERRED_GRAPH_CHUNKS_CREATE`] hold its further parts (0: the record is
/// whole in `record`, every row written before bsv-low #585 included), and
/// their generation.
pub const DEFERRED_GRAPHS_CHUNKS_COLUMN: &str =
    "ALTER TABLE gasp_deferred_graphs ADD COLUMN chunks INTEGER NOT NULL DEFAULT 0";
pub const DEFERRED_GRAPHS_GEN_COLUMN: &str =
    "ALTER TABLE gasp_deferred_graphs ADD COLUMN gen TEXT NOT NULL DEFAULT ''";

/// A record's JSON cut for its rows (bsv-low #585).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkPlan<'a> {
    /// Part 0: the head row's `record`.
    pub head: &'a str,
    /// Parts 1.., each a row of `gasp_deferred_graph_chunks` at its `seq`.
    pub rest: Vec<&'a str>,
    /// The generation of `rest`: 16 hex of the JSON's sha256 (`''` with no
    /// further part). By content, so a save of the very JSON held rewrites
    /// its own parts with the same bytes and can never tear it.
    pub gen: String,
}

/// PURE: cut a record's JSON into parts of at most `chunk` bytes, on
/// character boundaries (the concatenation is the JSON, byte for byte). A
/// JSON of at most `chunk` bytes is one part, no generation: the row a
/// record always was.
pub fn chunk_plan(json: &str, chunk: usize) -> ChunkPlan<'_> {
    let chunk = chunk.max(4);
    let mut parts = Vec::new();
    let mut at = 0;
    while json.len() - at > chunk {
        let mut end = at + chunk;
        while !json.is_char_boundary(end) {
            end -= 1;
        }
        parts.push(&json[at..end]);
        at = end;
    }
    parts.push(&json[at..]);
    let head = parts.remove(0);
    let gen = if parts.is_empty() {
        String::new()
    } else {
        hex::encode(&bsv_rs::primitives::hash::sha256(json.as_bytes())[..8])
    };
    ChunkPlan {
        head,
        rest: parts,
        gen,
    }
}

/// One further part of a record. Binds: `?1` host, `?2` topic, `?3`
/// outpoint, `?4` gen, `?5` seq, `?6` part.
pub const DEFERRED_GRAPH_CHUNK_PUT_SQL: &str = "INSERT OR REPLACE INTO gasp_deferred_graph_chunks      (host, topic, outpoint, gen, seq, part, written_at)      VALUES (?1, ?2, ?3, ?4, ?5, ?6, unixepoch())";

/// One further part, read by the generation the head row names. Binds as
/// [`DEFERRED_GRAPH_CHUNK_PUT_SQL`]'s first five. One part a statement: a
/// result is never more than one row's bytes.
pub const DEFERRED_GRAPH_CHUNK_GET_SQL: &str = "SELECT part FROM gasp_deferred_graph_chunks      WHERE host = ?1 AND topic = ?2 AND outpoint = ?3 AND gen = ?4 AND seq = ?5";

/// After a head row was SAVED: delete every part of another generation (the
/// record it replaced). Binds: `?1` host, `?2` topic, `?3` outpoint, `?4` the
/// saved generation (`''`: every part).
pub const DEFERRED_GRAPH_CHUNKS_KEEP_SQL: &str = "DELETE FROM gasp_deferred_graph_chunks      WHERE host = ?1 AND topic = ?2 AND outpoint = ?3 AND gen != ?4";

/// After a head row was REFUSED (`AtCeiling`) or its save faulted: take back
/// the parts just written, unless the held head names that very generation
/// (a save of the JSON already held). Binds as
/// [`DEFERRED_GRAPH_CHUNKS_KEEP_SQL`].
pub const DEFERRED_GRAPH_CHUNKS_UNDO_SQL: &str = "DELETE FROM gasp_deferred_graph_chunks      WHERE host = ?1 AND topic = ?2 AND outpoint = ?3 AND gen = ?4        AND NOT EXISTS (SELECT 1 FROM gasp_deferred_graphs          WHERE host = ?1 AND topic = ?2 AND outpoint = ?3 AND gen = ?4)";

/// Delete every part of one record (converged or dropped), after its head.
pub const DEFERRED_GRAPH_CHUNKS_DELETE_SQL: &str =
    "DELETE FROM gasp_deferred_graph_chunks      WHERE host = ?1 AND topic = ?2 AND outpoint = ?3";

/// A part no head row names is left alone this long before the cron sweeps
/// it: a save in flight (its parts written, its head not yet) is never older.
pub const DEFERRED_GRAPH_ORPHAN_CHUNK_SECS: u64 = 3600;

/// The cron's sweep of parts no head row names (a swept head's, a save that
/// died between its parts and its head). Binds: `?1`
/// [`DEFERRED_GRAPH_ORPHAN_CHUNK_SECS`].
pub const DEFERRED_GRAPH_CHUNKS_SWEEP_SQL: &str = "DELETE FROM gasp_deferred_graph_chunks      WHERE written_at < unixepoch() - ?1        AND NOT EXISTS (SELECT 1 FROM gasp_deferred_graphs g          WHERE g.host = gasp_deferred_graph_chunks.host            AND g.topic = gasp_deferred_graph_chunks.topic            AND g.outpoint = gasp_deferred_graph_chunks.outpoint            AND g.gen = gasp_deferred_graph_chunks.gen)";

/// The most rows the table holds over EVERY peer and topic (bsv-low #555, the
/// lens fold's M3). The engine bounds records per (peer, topic) at 16, and SHIP
/// mode takes its peers from the permissionless `ls_ship`: a stranger who
/// advertises many hosts could fill the one D1 the overlay shares with the app
/// layer (about 1.4 GB a day by the lens's count). A NEW key past this is
/// refused in the upsert itself ([`DEFERRED_GRAPH_UPSERT_SQL`]), counted
/// `too_many`, and its walk goes on under the per-peer budget alone.
pub const DEFERRED_GRAPHS_MAX_ROWS: u32 = 256;

/// The most bytes of records the store holds over every graph (64 MiB, the
/// sum of `bytes`, each a WHOLE record, its chunk rows included): a save,
/// new or a replacement, that would take the sum past it is refused, as
/// [`DEFERRED_GRAPHS_MAX_ROWS`]. The measured picture graph is about 0.5 MiB.
/// A BUDGET of room in the D1 the overlay shares, not a limit on a graph
/// (bsv-low #585): a refused replacement leaves the record held as it was,
/// the engine keeps it, and the walk goes on under the per-peer budget.
pub const DEFERRED_GRAPHS_MAX_TOTAL_BYTES: u64 = 64 << 20;

/// The most rows ONE host holds over all its topics (bsv-low #555, the delta
/// fold's D-M1): an eighth of [`DEFERRED_GRAPHS_MAX_ROWS`]. The ceiling was
/// one pool over every peer, and four (host, topic) pairs of a stranger's
/// hosts held all of it; every honest deferral after them was refused
/// (`too_many`) and walked from its root on every tick, the case #555 was
/// built for. A NEW key past it is refused in the upsert, as the global
/// bounds. A "host" is the peer's NORMALIZED ORIGIN (`origin`,
/// `overlay_engine::gasp::peer_origin`; the delta-2 fold's D2-M2): keyed on
/// the URL string, eight adverts of one server (`/?1` .. `/?8`) were eight
/// hosts and filled the ceiling. Applies to DISCOVERED peers only: a
/// configured peer's save is checked against the global bounds alone.
pub const DEFERRED_GRAPHS_MAX_ROWS_PER_HOST: u32 = DEFERRED_GRAPHS_MAX_ROWS / 8;

/// The most rows the records of DISCOVERED peers (`ls_ship`, `configured =
/// 0`) hold together: half of [`DEFERRED_GRAPHS_MAX_ROWS`] (bsv-low #555, the
/// delta-2 fold's D2-M2). The other half is RESERVED for configured peers
/// (`SyncTarget::Peers`), whose saves are checked against the global bounds
/// only: a stranger's adverts, any number of hosts and spellings, no longer
/// refuse a configured peer's deferral (#582's picture graph back to the
/// pre-#555 walk). A record costs a FAST peer only the `calls` budget (100
/// tiny responses, milliseconds), not the 15 s.
pub const DEFERRED_GRAPHS_DISCOVERED_MAX_ROWS: u32 = DEFERRED_GRAPHS_MAX_ROWS / 2;

/// The most bytes of `record` ONE host holds over all its topics (8 MiB, an
/// eighth of [`DEFERRED_GRAPHS_MAX_TOTAL_BYTES`]; about sixteen of the
/// measured picture graph). A save past it is refused, as the global bound.
pub const DEFERRED_GRAPHS_MAX_BYTES_PER_HOST: u64 = DEFERRED_GRAPHS_MAX_TOTAL_BYTES / 8;

/// The most bytes the records of DISCOVERED peers hold together: half of
/// [`DEFERRED_GRAPHS_MAX_TOTAL_BYTES`] (32 MiB), as
/// [`DEFERRED_GRAPHS_DISCOVERED_MAX_ROWS`].
pub const DEFERRED_GRAPHS_DISCOVERED_MAX_BYTES: u64 = DEFERRED_GRAPHS_MAX_TOTAL_BYTES / 2;

/// A row not written for this long is swept by the cron
/// ([`DEFERRED_GRAPHS_SWEEP_SQL`], counted `gasp_graph_dropped_stale_total`):
/// twice `DEFERRED_GRAPH_MAX_PASSES` (60) at the `*/15` cadence of all three
/// configs, 30 h. A live record is rewritten on every pass that resumes it and
/// is dropped by the engine at 60 passes; twice that covers a record HELD BACK
/// behind another's resumes for that one's whole life (the lens fold's M1). A
/// row past it belongs to a peer that never finishes a sync again (dark,
/// quarantined, its advert revoked), which nothing else would ever delete.
pub const DEFERRED_GRAPH_STALE_SECS: u64 =
    2 * overlay_engine::gasp::DEFERRED_GRAPH_MAX_PASSES as u64 * 15 * 60;

/// Save (replace) one record, under the table's global ceiling. Binds: `?1`
/// host (the peer URL, the key), `?2` topic, `?3` outpoint, `?4` score, `?5`
/// nodes, `?6` pending, `?7` calls, `?8` passes, `?9` reason, `?10` bytes,
/// `?11` record, `?12` [`DEFERRED_GRAPHS_MAX_ROWS`], `?13`
/// [`DEFERRED_GRAPHS_MAX_TOTAL_BYTES`], `?14`
/// [`DEFERRED_GRAPHS_MAX_ROWS_PER_HOST`], `?15`
/// [`DEFERRED_GRAPHS_MAX_BYTES_PER_HOST`], `?16` origin
/// (`overlay_engine::gasp::peer_origin` of the host), `?17` configured
/// (1/0), `?18` [`DEFERRED_GRAPHS_DISCOVERED_MAX_ROWS`], `?19`
/// [`DEFERRED_GRAPHS_DISCOVERED_MAX_BYTES`], `?20` chunks (the rows of
/// `gasp_deferred_graph_chunks` holding the rest of the record, 0 for a
/// record whole in `?11`), `?21` their generation; `?10` is the WHOLE
/// record's size, `?11` its first part (bsv-low #585, [`chunk_plan`]).
/// `created_at` is kept from the
/// first deferral (the age); the backend owns the clock. The `WHERE` of the
/// `SELECT` re-reads the ceiling in the one statement (as #576's `PARK_SQL`):
/// a held key always replaces within the byte bounds, a new key only under
/// the row bounds too. A CONFIGURED peer's save is checked against the
/// global bounds only; a DISCOVERED peer's also against its origin's share
/// and the discovered half (the delta-2 fold's D2-M2). A refused save
/// returns NO row (`AtCeiling`).
pub const DEFERRED_GRAPH_UPSERT_SQL: &str = "INSERT INTO gasp_deferred_graphs \
     (host, topic, outpoint, score, nodes, pending, calls, passes, reason, bytes, record, created_at, updated_at, \
      origin, configured, chunks, gen) \
     SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, unixepoch(), unixepoch(), ?16, ?17, ?20, ?21 \
     WHERE (EXISTS (SELECT 1 FROM gasp_deferred_graphs WHERE host = ?1 AND topic = ?2 AND outpoint = ?3) \
       OR ((SELECT COUNT(*) FROM gasp_deferred_graphs) < ?12 \
         AND (?17 OR ((SELECT COUNT(*) FROM gasp_deferred_graphs WHERE origin = ?16 AND configured = 0) < ?14 \
           AND (SELECT COUNT(*) FROM gasp_deferred_graphs WHERE configured = 0) < ?18)))) \
     AND (SELECT COALESCE(SUM(bytes), 0) FROM gasp_deferred_graphs \
       WHERE NOT (host = ?1 AND topic = ?2 AND outpoint = ?3)) + ?10 <= ?13 \
     AND (?17 OR ((SELECT COALESCE(SUM(bytes), 0) FROM gasp_deferred_graphs \
         WHERE origin = ?16 AND configured = 0 AND NOT (host = ?1 AND topic = ?2 AND outpoint = ?3)) + ?10 <= ?15 \
       AND (SELECT COALESCE(SUM(bytes), 0) FROM gasp_deferred_graphs \
         WHERE configured = 0 AND NOT (host = ?1 AND topic = ?2 AND outpoint = ?3)) + ?10 <= ?19)) \
     ON CONFLICT(host, topic, outpoint) DO UPDATE SET \
       score = excluded.score, nodes = excluded.nodes, pending = excluded.pending, \
       calls = excluded.calls, passes = excluded.passes, reason = excluded.reason, \
       bytes = excluded.bytes, record = excluded.record, updated_at = unixepoch(), \
       origin = excluded.origin, configured = excluded.configured, \
       chunks = excluded.chunks, gen = excluded.gen \
     RETURNING outpoint";

/// The KEYS of one (peer, topic)'s records, lowest score first: a sync reads
/// these up front, and each record only when its UTXO is served (the lens
/// fold's L3: the whole records of a (peer, topic), up to 16 MiB, were read
/// in one result).
pub const DEFERRED_GRAPHS_SELECT_SQL: &str = "SELECT outpoint, score FROM gasp_deferred_graphs \
     WHERE host = ?1 AND topic = ?2 ORDER BY score, outpoint";

/// One record's head row: its first part, and where the rest is
/// ([`DEFERRED_GRAPH_CHUNK_GET_SQL`], `seq` 1 ..= `chunks` under `gen`).
pub const DEFERRED_GRAPH_GET_SQL: &str = "SELECT record, chunks, gen FROM gasp_deferred_graphs \
     WHERE host = ?1 AND topic = ?2 AND outpoint = ?3";

/// One record's head row as [`DEFERRED_GRAPH_GET_SQL`] gives it.
#[derive(Deserialize, Debug, Clone, PartialEq)]
pub(crate) struct DeferredHead {
    pub record: String,
    pub chunks: Option<f64>,
    pub gen: Option<String>,
}

/// The rows a deferred record is read from, each by its shipped statement
/// ([`DEFERRED_GRAPH_GET_SQL`], [`DEFERRED_GRAPH_CHUNK_GET_SQL`]): the
/// worker's are D1's (`d1_storage`), the stored-rows reader's an export's
/// (`stored_rows`, the land lens E585-LAND-L3). A column of another type than
/// the row's field is the port's `Fault`, as D1's deserialization faults.
pub(crate) trait DeferredRows {
    type Fault;
    async fn head(
        &self,
        host: &str,
        topic: &str,
        outpoint: &str,
    ) -> Result<Option<DeferredHead>, Self::Fault>;
    async fn part(
        &self,
        host: &str,
        topic: &str,
        outpoint: &str,
        gen: &str,
        seq: u64,
    ) -> Result<Option<String>, Self::Fault>;
}

/// What [`read_deferred_graph`] found.
#[derive(Debug)]
pub(crate) enum DeferredRead {
    /// No head row.
    Absent,
    /// The record, whole.
    Record(overlay_engine::gasp::DeferredGraph),
    /// Part `seq` of generation `gen` is gone: the record cannot be resumed
    /// (the worker drops it).
    PartMissing { seq: u64, gen: String },
    /// The JSON does not parse as a record.
    Unparsed(String),
}

/// THE read of one deferred record (bsv-low #585 door 4): its head, then
/// each further part of its generation, one a statement, then the parse.
/// ONE function, called by the worker's `get_deferred_graph` and by the
/// stored-rows reader, so the reader reads a record as the worker does.
pub(crate) async fn read_deferred_graph<P: DeferredRows>(
    rows: &P,
    host: &str,
    topic: &str,
    outpoint: &str,
) -> Result<DeferredRead, P::Fault> {
    let Some(head) = rows.head(host, topic, outpoint).await? else {
        return Ok(DeferredRead::Absent);
    };
    let mut json = head.record;
    let gen = head.gen.unwrap_or_default();
    for seq in 1..=head.chunks.unwrap_or(0.0).max(0.0) as u64 {
        let Some(part) = rows.part(host, topic, outpoint, &gen, seq).await? else {
            return Ok(DeferredRead::PartMissing { seq, gen });
        };
        json.push_str(&part);
    }
    Ok(match serde_json::from_str(&json) {
        Ok(record) => DeferredRead::Record(record),
        Err(e) => DeferredRead::Unparsed(e.to_string()),
    })
}

/// Delete one record (converged or dropped).
pub const DEFERRED_GRAPH_DELETE_SQL: &str = "DELETE FROM gasp_deferred_graphs \
     WHERE host = ?1 AND topic = ?2 AND outpoint = ?3";

/// The cron's sweep of stale rows. Binds: `?1` [`DEFERRED_GRAPH_STALE_SECS`].
pub const DEFERRED_GRAPHS_SWEEP_SQL: &str = "DELETE FROM gasp_deferred_graphs \
     WHERE updated_at < unixepoch() - ?1 RETURNING host, topic, outpoint";

/// Rows swept for age: also counted in [`COUNTER_GASP_GRAPH_DROPPED`].
pub const COUNTER_GASP_GRAPH_DROPPED_STALE: &str = "gasp_graph_dropped_stale_total";

/// Sweep the rows older than [`DEFERRED_GRAPH_STALE_SECS`] (the cron, before
/// its GASP sync): each logged, counted `stale` and in all drops. Best effort.
pub async fn sweep_stale(db: &D1Database) {
    #[derive(Deserialize)]
    struct Swept {
        host: String,
        topic: String,
        outpoint: String,
    }
    match Query::new(DEFERRED_GRAPHS_SWEEP_SQL)
        .bind(DEFERRED_GRAPH_STALE_SECS as f64)
        .fetch_all::<Swept>(db)
        .await
    {
        Ok(rows) if !rows.is_empty() => {
            for r in &rows {
                worker::console_log!(
                    "Scheduled: GASP deferred graph {} of {} for {} SWEPT (stale past {} s, bsv-low #555)",
                    r.outpoint,
                    r.host,
                    r.topic,
                    DEFERRED_GRAPH_STALE_SECS
                );
            }
            let n = rows.len() as u64;
            crate::ops::bump_counter(db, COUNTER_GASP_GRAPH_DROPPED_STALE, n).await;
            crate::ops::bump_counter(db, COUNTER_GASP_GRAPH_DROPPED, n).await;
        }
        Ok(_) => {}
        Err(e) => worker::console_log!("Scheduled: GASP deferred graph sweep failed: {e}"),
    }
    // The parts no head names: the swept rows' and a dead save's (#585).
    if let Err(e) = Query::new(DEFERRED_GRAPH_CHUNKS_SWEEP_SQL)
        .bind(DEFERRED_GRAPH_ORPHAN_CHUNK_SECS as f64)
        .execute(db)
        .await
    {
        worker::console_log!("Scheduled: GASP deferred graph chunk sweep failed: {e}");
    }
}

/// The health block lists at most this many graphs, oldest first.
pub const HEALTH_LIST_MAX: u32 = 20;

const HEALTH_COUNT_SQL: &str =
    "SELECT COUNT(*) AS c, COALESCE(SUM(bytes), 0) AS b FROM gasp_deferred_graphs";

const HEALTH_LIST_SQL: &str =
    "SELECT host, topic, outpoint, nodes, pending, calls, passes, reason, bytes, \
            (unixepoch() - created_at) AS ageSecs \
     FROM gasp_deferred_graphs ORDER BY created_at, outpoint LIMIT ?1";

/// Graphs deferred (a new record, or a resumed graph deferred again).
pub const COUNTER_GASP_GRAPH_DEFERRED: &str = "gasp_graph_deferred_total";
/// Graphs resumed from a record.
pub const COUNTER_GASP_GRAPH_RESUMED: &str = "gasp_graph_resumed_total";
/// Resumed graphs that completed and landed.
pub const COUNTER_GASP_GRAPH_CONVERGED: &str = "gasp_graph_converged_total";
/// Records dropped without converging, all reasons; each reason also has its
/// own counter, [`dropped_counter_name`].
pub const COUNTER_GASP_GRAPH_DROPPED: &str = "gasp_graph_dropped_total";

/// The counter of one drop reason: `gasp_graph_dropped_<reason>_total`.
pub fn dropped_counter_name(reason: &str) -> String {
    format!("gasp_graph_dropped_{reason}_total")
}

/// Every counter name this module serves, for the health block's zero seed.
pub fn counter_names() -> Vec<String> {
    let mut names = vec![
        COUNTER_GASP_GRAPH_DEFERRED.to_string(),
        COUNTER_GASP_GRAPH_RESUMED.to_string(),
        COUNTER_GASP_GRAPH_CONVERGED.to_string(),
        COUNTER_GASP_GRAPH_DROPPED.to_string(),
        COUNTER_GASP_GRAPH_DROPPED_STALE.to_string(),
    ];
    names.extend(
        overlay_engine::gasp::DropReason::ALL
            .iter()
            .map(|r| dropped_counter_name(r.as_str())),
    );
    names
}

/// PURE: the counter bumps of one sync's results, `(name, delta)`, zeros left
/// out.
pub fn counter_deltas(
    results: &std::collections::HashMap<String, overlay_engine::engine::TopicSyncResult>,
) -> Vec<(String, u64)> {
    let mut deltas: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    for r in results.values() {
        *deltas
            .entry(COUNTER_GASP_GRAPH_DEFERRED.into())
            .or_default() += r.deferred_graphs;
        *deltas.entry(COUNTER_GASP_GRAPH_RESUMED.into()).or_default() += r.resumed_graphs;
        *deltas
            .entry(COUNTER_GASP_GRAPH_CONVERGED.into())
            .or_default() += r.converged_graphs;
        for d in &r.dropped_graphs {
            *deltas.entry(COUNTER_GASP_GRAPH_DROPPED.into()).or_default() += 1;
            *deltas.entry(dropped_counter_name(&d.reason)).or_default() += 1;
        }
    }
    deltas.into_iter().filter(|(_, v)| *v > 0).collect()
}

/// Bump the counters of one sync (best effort, as every counter).
pub async fn record_counters(
    db: &D1Database,
    results: &std::collections::HashMap<String, overlay_engine::engine::TopicSyncResult>,
) {
    for (name, delta) in counter_deltas(results) {
        crate::ops::bump_counter(db, &name, delta).await;
    }
}

/// One row of the health list.
#[derive(Debug, Clone, Deserialize)]
pub struct HealthRow {
    pub host: String,
    pub topic: String,
    pub outpoint: String,
    pub nodes: f64,
    pub pending: f64,
    pub calls: f64,
    pub passes: f64,
    pub reason: String,
    pub bytes: f64,
    #[serde(rename = "ageSecs")]
    pub age_secs: Option<f64>,
}

#[derive(Deserialize)]
struct CountRow {
    c: f64,
    b: f64,
}

/// PURE: the health block from the count, the bytes held and the oldest rows.
pub fn health_view(
    count: u64,
    total_bytes: u64,
    rows: &[HealthRow],
    limbs: (u64, u32),
) -> serde_json::Value {
    let n = |v: f64| v.max(0.0) as u64;
    let graphs: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "topic": r.topic,
                "peer": r.host,
                "outpoint": r.outpoint,
                "nodes": n(r.nodes),
                "pending": n(r.pending),
                "calls": n(r.calls),
                "passes": n(r.passes),
                "reason": r.reason,
                "bytes": n(r.bytes),
                "ageSecs": r.age_secs.map(n),
            })
        })
        .collect();
    serde_json::json!({
        "readable": true,
        "count": count,
        "totalBytes": total_bytes,
        "oldest": graphs.first().cloned(),
        "graphs": graphs,
        "listed": rows.len(),
        "budget": {
            "calls": GASP_GRAPH_BUDGET_CALLS,
            "ms": GASP_GRAPH_BUDGET_MS,
            "bytesFetched": limbs.0,
            "nodes": limbs.1,
            "maxPasses": overlay_engine::gasp::DEFERRED_GRAPH_MAX_PASSES,
            "chunkBytes": DEFERRED_GRAPH_CHUNK_BYTES,
            "perPeerTopic": overlay_engine::gasp::DEFERRED_GRAPHS_PER_PEER_TOPIC,
            "maxRows": DEFERRED_GRAPHS_MAX_ROWS,
            "maxTotalBytes": DEFERRED_GRAPHS_MAX_TOTAL_BYTES,
            "maxRowsPerHost": DEFERRED_GRAPHS_MAX_ROWS_PER_HOST,
            "maxBytesPerHost": DEFERRED_GRAPHS_MAX_BYTES_PER_HOST,
            "discoveredMaxRows": DEFERRED_GRAPHS_DISCOVERED_MAX_ROWS,
            "discoveredMaxBytes": DEFERRED_GRAPHS_DISCOVERED_MAX_BYTES,
            "staleSecs": DEFERRED_GRAPH_STALE_SECS,
        },
    })
}

/// `/health/invariants.gasp.deferredGraphs`: the count, the oldest, and each
/// of the oldest [`HEALTH_LIST_MAX`] graphs {topic, peer, outpoint, nodes,
/// pending, calls, passes, reason, bytes, ageSecs}. `readable: false` when
/// the table cannot be read (a pre-migration isolate), distinct from none.
pub async fn health_json(db: &D1Database, limbs: (u64, u32)) -> serde_json::Value {
    let Ok(count) = Query::new(HEALTH_COUNT_SQL)
        .fetch_optional::<CountRow>(db)
        .await
    else {
        return serde_json::json!({"readable": false});
    };
    let (count, total_bytes) = count.map_or((0, 0), |r| (r.c.max(0.0) as u64, r.b.max(0.0) as u64));
    let rows = if count > 0 {
        match Query::new(HEALTH_LIST_SQL)
            .bind(HEALTH_LIST_MAX)
            .fetch_all::<HealthRow>(db)
            .await
        {
            Ok(rows) => rows,
            Err(_) => return serde_json::json!({"readable": false, "count": count}),
        }
    } else {
        Vec::new()
    };
    health_view(count, total_bytes, &rows, limbs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use overlay_engine::engine::{DroppedDeferral, TopicSyncResult};
    use overlay_engine::gasp::{DeferredGraph, PendingInput, WalkedNode};
    use overlay_engine::types::GASPNode;

    fn sqlite() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        for sql in crate::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                assert!(
                    e.to_string()
                        .to_ascii_lowercase()
                        .contains("duplicate column"),
                    "{e}\n{sql}"
                );
            }
        }
        conn
    }

    fn record(outpoint: &str, score: u64, nodes: usize, passes: u32) -> DeferredGraph {
        let node = GASPNode {
            graph_id: outpoint.into(),
            raw_tx: "00".into(),
            output_index: 0,
            proof: Some("ab".into()),
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };
        DeferredGraph {
            peer: "https://peer".into(),
            topic: "tm_x".into(),
            outpoint: outpoint.into(),
            score,
            nodes: vec![
                WalkedNode {
                    node,
                    spent_by: None,
                };
                nodes
            ],
            pending: vec![PendingInput {
                outpoint: "aa.0".into(),
                graph_id: outpoint.into(),
                metadata: false,
                spent_by: Some("bb.0".into()),
                parent_proven: false,
            }],
            calls: nodes as u64,
            passes,
            reason: "calls".into(),
            configured: false,
            idle_faults: 0,
        }
    }

    /// The shipped upsert as `D1Storage::put_deferred_graph` binds it, under
    /// the given ceiling: `true` = saved (a row returned).
    fn upsert_under(
        conn: &rusqlite::Connection,
        r: &DeferredGraph,
        max_rows: u32,
        max_bytes: u64,
    ) -> bool {
        upsert_shared(conn, r, max_rows, max_bytes, max_rows, max_bytes)
    }

    /// The same, with the per-host share too.
    fn upsert_shared(
        conn: &rusqlite::Connection,
        r: &DeferredGraph,
        max_rows: u32,
        max_bytes: u64,
        host_rows: u32,
        host_bytes: u64,
    ) -> bool {
        upsert_reserved(
            conn, r, max_rows, max_bytes, host_rows, host_bytes, max_rows, max_bytes,
        )
    }

    /// The same, with the discovered peers' half too (the delta-2 fold's
    /// D2-M2), bound as `D1Storage::put_deferred_graph` binds it.
    #[allow(clippy::too_many_arguments)]
    fn upsert_reserved(
        conn: &rusqlite::Connection,
        r: &DeferredGraph,
        max_rows: u32,
        max_bytes: u64,
        host_rows: u32,
        host_bytes: u64,
        discovered_rows: u32,
        discovered_bytes: u64,
    ) -> bool {
        put(
            conn,
            r,
            [
                u64::from(max_rows),
                max_bytes,
                u64::from(host_rows),
                host_bytes,
                u64::from(discovered_rows),
                discovered_bytes,
            ],
            DEFERRED_GRAPH_CHUNK_BYTES,
        )
    }

    /// `D1Storage::put_deferred_graph`, statement for statement, under the
    /// given bounds (rows, bytes, an origin's rows and bytes, the discovered
    /// rows and bytes) and chunk size: the further parts of a new
    /// generation, the head row, then the parts of the generation the head
    /// does not name. `true` = saved.
    fn put(conn: &rusqlite::Connection, r: &DeferredGraph, bounds: [u64; 6], chunk: usize) -> bool {
        let json = serde_json::to_string(r).unwrap();
        let plan = chunk_plan(&json, chunk);
        for (i, part) in plan.rest.iter().enumerate() {
            conn.execute(
                DEFERRED_GRAPH_CHUNK_PUT_SQL,
                rusqlite::params![r.peer, r.topic, r.outpoint, plan.gen, (i + 1) as i64, part],
            )
            .unwrap();
        }
        let saved = {
            let mut stmt = conn.prepare(DEFERRED_GRAPH_UPSERT_SQL).unwrap();
            let mut rows = stmt
                .query(rusqlite::params![
                    r.peer,
                    r.topic,
                    r.outpoint,
                    r.score as f64,
                    r.nodes.len() as i64,
                    r.pending.len() as i64,
                    r.calls as i64,
                    r.passes as i64,
                    r.reason,
                    json.len() as i64,
                    plan.head,
                    bounds[0] as i64,
                    bounds[1] as i64,
                    bounds[2] as i64,
                    bounds[3] as i64,
                    overlay_engine::gasp::peer_origin(&r.peer),
                    r.configured,
                    bounds[4] as i64,
                    bounds[5] as i64,
                    plan.rest.len() as i64,
                    plan.gen
                ])
                .unwrap();
            rows.next().unwrap().is_some()
        };
        if saved || !plan.rest.is_empty() {
            let tidy = if saved {
                DEFERRED_GRAPH_CHUNKS_KEEP_SQL
            } else {
                DEFERRED_GRAPH_CHUNKS_UNDO_SQL
            };
            conn.execute(
                tidy,
                rusqlite::params![r.peer, r.topic, r.outpoint, plan.gen],
            )
            .unwrap();
        }
        saved
    }

    /// `D1Storage::get_deferred_graph`, statement for statement: the head
    /// row, then each further part by the generation it names. `Err` names
    /// a missing part (the storage drops the record there).
    fn get(conn: &rusqlite::Connection, r: &DeferredGraph) -> Option<Result<DeferredGraph, u64>> {
        use rusqlite::OptionalExtension;
        let key = rusqlite::params![r.peer, r.topic, r.outpoint];
        let (mut json, chunks, gen): (String, i64, String) = conn
            .query_row(DEFERRED_GRAPH_GET_SQL, key, |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .optional()
            .unwrap()?;
        for seq in 1..=chunks {
            let part: Option<String> = conn
                .query_row(
                    DEFERRED_GRAPH_CHUNK_GET_SQL,
                    rusqlite::params![r.peer, r.topic, r.outpoint, gen, seq],
                    |row| row.get(0),
                )
                .optional()
                .unwrap();
            match part {
                Some(part) => json.push_str(&part),
                None => return Some(Err(seq as u64)),
            }
        }
        Some(Ok(serde_json::from_str(&json).unwrap()))
    }

    /// `D1Storage::delete_deferred_graph`: the head, then its parts.
    fn delete(conn: &rusqlite::Connection, r: &DeferredGraph) {
        let key = rusqlite::params![r.peer, r.topic, r.outpoint];
        conn.execute(DEFERRED_GRAPH_DELETE_SQL, key).unwrap();
        conn.execute(DEFERRED_GRAPH_CHUNKS_DELETE_SQL, key).unwrap();
    }

    /// (seq, gen, bytes) of every part row, in order.
    fn parts(conn: &rusqlite::Connection) -> Vec<(i64, String, usize)> {
        let mut stmt = conn
            .prepare(
                "SELECT seq, gen, length(CAST(part AS BLOB)) FROM gasp_deferred_graph_chunks \
                 ORDER BY outpoint, gen, seq",
            )
            .unwrap();
        stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    fn upsert(conn: &rusqlite::Connection, r: &DeferredGraph) {
        assert!(upsert_shared(
            conn,
            r,
            DEFERRED_GRAPHS_MAX_ROWS,
            DEFERRED_GRAPHS_MAX_TOTAL_BYTES,
            DEFERRED_GRAPHS_MAX_ROWS_PER_HOST,
            DEFERRED_GRAPHS_MAX_BYTES_PER_HOST
        ));
    }

    /// The keys, then each record by the get statement.
    fn select(conn: &rusqlite::Connection) -> Vec<DeferredGraph> {
        let mut stmt = conn.prepare(DEFERRED_GRAPHS_SELECT_SQL).unwrap();
        let keys: Vec<String> = stmt
            .query_map(rusqlite::params!["https://peer", "tm_x"], |row| {
                row.get::<_, String>(0)
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        keys.iter()
            .map(|k| {
                let json: String = conn
                    .query_row(
                        DEFERRED_GRAPH_GET_SQL,
                        rusqlite::params!["https://peer", "tm_x", k],
                        |row| row.get(0),
                    )
                    .unwrap();
                serde_json::from_str(&json).unwrap()
            })
            .collect()
    }

    fn rows(conn: &rusqlite::Connection) -> Vec<String> {
        let mut stmt = conn
            .prepare("SELECT outpoint FROM gasp_deferred_graphs ORDER BY outpoint")
            .unwrap();
        stmt.query_map([], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    /// bsv-low #555, the lens fold's M3: the global ceiling is read in the
    /// upsert itself. Under a 3-row bound a fourth NEW key returns no row
    /// (`AtCeiling`) and writes nothing; a held key still replaces; under a
    /// byte bound a new key and a replacement that would pass it are both
    /// refused, the held row unchanged. On 03e1e17 the upsert had no ceiling
    /// (it wrote every row and returned none).
    #[test]
    fn e555f_m3_the_upsert_refuses_past_the_global_ceiling() {
        let conn = sqlite();
        for (i, o) in ["a.0", "b.0", "c.0"].iter().enumerate() {
            assert!(upsert_under(&conn, &record(o, i as u64, 1, 1), 3, 1 << 20));
        }
        assert!(
            !upsert_under(&conn, &record("d.0", 9, 1, 1), 3, 1 << 20),
            "a 4th key"
        );
        assert_eq!(rows(&conn), ["a.0", "b.0", "c.0"]);
        assert!(
            upsert_under(&conn, &record("b.0", 1, 5, 2), 3, 1 << 20),
            "held: replaced"
        );
        assert_eq!(select(&conn)[1].passes, 2);
        // Bytes: what the three hold now, plus one more small record, is the bound.
        let held: i64 = conn
            .query_row("SELECT SUM(bytes) FROM gasp_deferred_graphs", [], |r| {
                r.get(0)
            })
            .unwrap();
        let small = serde_json::to_string(&record("e.0", 9, 1, 1))
            .unwrap()
            .len() as u64;
        let bound = held as u64 + small;
        assert!(
            !upsert_under(&conn, &record("f.0", 9, 9, 1), 9, bound),
            "too many bytes"
        );
        assert!(
            !upsert_under(&conn, &record("a.0", 0, 9, 2), 9, held as u64),
            "a growth past it"
        );
        assert_eq!(select(&conn)[0].nodes.len(), 1, "the held row unchanged");
        assert!(
            upsert_under(&conn, &record("e.0", 9, 1, 1), 9, bound),
            "exactly at the bound"
        );
        assert_eq!(rows(&conn), ["a.0", "b.0", "c.0", "e.0"]);
        // The shipped bounds.
        assert_eq!(
            (DEFERRED_GRAPHS_MAX_ROWS, DEFERRED_GRAPHS_MAX_TOTAL_BYTES),
            (256, 64 << 20)
        );
    }

    /// bsv-low #555, the delta fold's D-M1: the ceiling is SHARED by host. A
    /// stranger's host at its share (2 rows here, over two topics) is refused
    /// a new key and a growth past its byte share, and still replaces a held
    /// key within it; another host still saves, up to the global bounds. On
    /// 0974be5 the upsert had no per-host bound: one host could take the
    /// whole ceiling and every other host's deferral was refused.
    #[test]
    fn e555d_m1_the_upsert_shares_the_ceiling_by_host() {
        let conn = sqlite();
        let on = |host: &str, topic: &str, o: &str, nodes: usize, bytes: u64| {
            let mut r = record(o, 1, nodes, 1);
            r.peer = host.into();
            r.topic = topic.into();
            upsert_shared(&conn, &r, 5, 1 << 20, 2, bytes)
        };
        assert!(on("https://stranger", "tm_x", "s1.0", 1, 1 << 20));
        assert!(on("https://stranger", "tm_y", "s2.0", 1, 1 << 20));
        assert!(
            !on("https://stranger", "tm_z", "s3.0", 1, 1 << 20),
            "past its rows"
        );
        assert!(
            on("https://stranger", "tm_x", "s1.0", 2, 1 << 20),
            "held: replaced"
        );
        let one = serde_json::to_string(&record("s1.0", 1, 2, 1))
            .unwrap()
            .len() as u64;
        let two = serde_json::to_string(&record("s2.0", 1, 1, 1))
            .unwrap()
            .len() as u64;
        assert!(
            !on("https://stranger", "tm_y", "s2.0", 9, one + two),
            "a growth past its bytes"
        );
        assert!(
            on("https://honest", "tm_x", "h1.0", 1, 1 << 20),
            "another host saves"
        );
        assert!(on("https://honest", "tm_x", "h2.0", 1, 1 << 20));
        assert!(on("https://third", "tm_x", "t1.0", 1, 1 << 20));
        assert!(
            !on("https://fourth", "tm_x", "f1.0", 1, 1 << 20),
            "the global rows"
        );
        assert_eq!(rows(&conn), ["h1.0", "h2.0", "s1.0", "s2.0", "t1.0"]);
        assert_eq!(
            (
                DEFERRED_GRAPHS_MAX_ROWS_PER_HOST,
                DEFERRED_GRAPHS_MAX_BYTES_PER_HOST
            ),
            (32, 8 << 20)
        );
        // The worker binds the shares.
        let storage = include_str!("d1_storage.rs");
        let put = &storage[storage.find("async fn put_deferred_graph").unwrap()..];
        let put = &put[..put.find("async fn find_deferred_graphs").unwrap()];
        assert!(put.contains("DEFERRED_GRAPHS_MAX_ROWS_PER_HOST"));
        assert!(put.contains("DEFERRED_GRAPHS_MAX_BYTES_PER_HOST"));
    }

    /// bsv-low #555, the delta-2 fold's D2-M2 (the lens's DELTA2-4, inverted).
    /// On ef423da eight spellings of one server (`https://evil.example/?0` ..
    /// `?7`), 16 records over two topics each, took all 256 rows, and a
    /// CONFIGURED peer (`overlay-us-1.bsvb.tech`, `tm_uhrp`) was then refused.
    /// Now the spellings are ONE origin and share one share (32 rows), the
    /// discovered peers together hold at most half the rows (128, here with
    /// four subdomains), and the configured peer still saves, up to the
    /// global bound. Bytes: the discovered half refuses a discovered growth
    /// that a configured record of the same size passes.
    #[test]
    fn e555d2_m2_a_configured_peer_saves_with_the_discovered_half_full() {
        let conn = sqlite();
        let save = |host: &str, topic: &str, o: &str, configured: bool| {
            let mut r = record(o, 1, 1, 1);
            r.peer = host.into();
            r.topic = topic.into();
            r.configured = configured;
            let json = serde_json::to_string(&r).unwrap();
            let mut stmt = conn.prepare(DEFERRED_GRAPH_UPSERT_SQL).unwrap();
            let mut rows = stmt
                .query(rusqlite::params![
                    r.peer,
                    r.topic,
                    r.outpoint,
                    r.score as f64,
                    r.nodes.len() as i64,
                    r.pending.len() as i64,
                    r.calls as i64,
                    r.passes as i64,
                    r.reason,
                    json.len() as i64,
                    json,
                    DEFERRED_GRAPHS_MAX_ROWS,
                    DEFERRED_GRAPHS_MAX_TOTAL_BYTES as i64,
                    DEFERRED_GRAPHS_MAX_ROWS_PER_HOST,
                    DEFERRED_GRAPHS_MAX_BYTES_PER_HOST as i64,
                    overlay_engine::gasp::peer_origin(&r.peer),
                    r.configured,
                    DEFERRED_GRAPHS_DISCOVERED_MAX_ROWS,
                    DEFERRED_GRAPHS_DISCOVERED_MAX_BYTES as i64,
                    0,
                    ""
                ])
                .unwrap();
            rows.next().unwrap().is_some()
        };
        // DELTA2-4's flood: 8 spellings x 2 topics x 16 records.
        let mut saved = 0;
        for spelling in 0..8 {
            for topic in ["tm_ship", "tm_slap"] {
                for i in 0..16 {
                    let host = format!("https://evil.example/?{spelling}");
                    saved += u32::from(save(
                        &host,
                        topic,
                        &format!("{spelling}{topic}{i}.0"),
                        false,
                    ));
                }
            }
        }
        assert_eq!(
            saved, DEFERRED_GRAPHS_MAX_ROWS_PER_HOST,
            "eight spellings, one share"
        );
        // Three more servers by subdomain fill the discovered half.
        for sub in ["a", "b", "c", "d"] {
            for i in 0..32 {
                save(
                    &format!("https://{sub}.evil.example"),
                    "tm_ship",
                    &format!("{sub}{i}.0"),
                    false,
                );
            }
        }
        let held = rows(&conn).len() as u32;
        assert_eq!(
            held, DEFERRED_GRAPHS_DISCOVERED_MAX_ROWS,
            "the discovered half"
        );
        assert!(
            !save("https://e.evil.example", "tm_ship", "e0.0", false),
            "a 6th discovered host"
        );
        // The configured peer still saves, up to the global bound.
        for i in 0..(DEFERRED_GRAPHS_MAX_ROWS - held) {
            assert!(
                save(
                    "https://overlay-us-1.bsvb.tech",
                    "tm_uhrp",
                    &format!("u{i}.0"),
                    true
                ),
                "configured record {i}"
            );
        }
        assert!(
            !save(
                "https://overlay-us-1.bsvb.tech",
                "tm_uhrp",
                "u-last.0",
                true
            ),
            "the global bound"
        );
        assert_eq!(
            (
                DEFERRED_GRAPHS_DISCOVERED_MAX_ROWS,
                DEFERRED_GRAPHS_DISCOVERED_MAX_BYTES
            ),
            (128, 32 << 20)
        );
        // Bytes: a discovered host at the discovered byte half; a configured
        // record of the same size passes it.
        let conn2 = sqlite();
        let shaped = |host: &str, o: &str, configured: bool| {
            let mut r = record(o, 1, 1, 1);
            r.peer = host.into();
            r.configured = configured;
            r
        };
        let size = serde_json::to_string(&shaped("https://s1", "s1.0", false))
            .unwrap()
            .len() as u64;
        let bytes = |host: &str, o: &str, configured: bool| {
            upsert_reserved(
                &conn2,
                &shaped(host, o, configured),
                99,
                99 << 20,
                99,
                99 << 20,
                99,
                size,
            )
        };
        assert!(bytes("https://s1", "s1.0", false));
        assert!(
            !bytes("https://s2", "s2.0", false),
            "past the discovered bytes"
        );
        assert!(
            bytes("https://configured", "c1.0", true),
            "configured: global bytes only"
        );
        // The worker binds the origin, the class and the half.
        let storage = include_str!("d1_storage.rs");
        let put = &storage[storage.find("async fn put_deferred_graph").unwrap()..];
        let put = &put[..put.find("async fn find_deferred_graphs").unwrap()];
        assert!(put.contains("peer_origin(&record.peer)"));
        assert!(put.contains(".bind(record.configured)"));
        assert!(put.contains("DEFERRED_GRAPHS_DISCOVERED_MAX_ROWS"));
        assert!(put.contains("DEFERRED_GRAPHS_DISCOVERED_MAX_BYTES"));
    }

    /// bsv-low #555, the lens fold's M3: the cron's sweep deletes the rows not
    /// written for [`DEFERRED_GRAPH_STALE_SECS`] (30 h) and returns each, and
    /// leaves the others; the cron calls it before its GASP sync and counts
    /// it. On 03e1e17 nothing deleted a row whose peer never synced again.
    #[test]
    fn e555f_m3_the_sweep_deletes_stale_rows_only() {
        let conn = sqlite();
        upsert(&conn, &record("old.0", 1, 1, 1));
        upsert(&conn, &record("new.0", 2, 1, 1));
        conn.execute(
            "UPDATE gasp_deferred_graphs SET updated_at = unixepoch() - ?1 WHERE outpoint = 'old.0'",
            [DEFERRED_GRAPH_STALE_SECS as i64 + 1],
        )
        .unwrap();
        conn.execute(
            "UPDATE gasp_deferred_graphs SET updated_at = unixepoch() - ?1 WHERE outpoint = 'new.0'",
            [DEFERRED_GRAPH_STALE_SECS as i64 - 60],
        )
        .unwrap();
        let mut stmt = conn.prepare(DEFERRED_GRAPHS_SWEEP_SQL).unwrap();
        let swept: Vec<String> = stmt
            .query_map([DEFERRED_GRAPH_STALE_SECS as i64], |row| row.get(2))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(swept, ["old.0"]);
        assert_eq!(rows(&conn), ["new.0"]);
        assert_eq!(DEFERRED_GRAPH_STALE_SECS, 108_000);
        let lib = include_str!("lib.rs");
        let sweep = lib
            .find("crate::gasp_deferred::sweep_stale(&ops_db).await;")
            .unwrap();
        let sync = lib.find("engine.start_gasp_sync(),\n        crate::broadcaster::sleep_ms(GASP_SYNC_BUDGET_MS)").unwrap();
        assert!(sweep < sync, "the cron sweeps before its GASP sync");
        assert!(counter_names().contains(&COUNTER_GASP_GRAPH_DROPPED_STALE.to_string()));
    }

    /// bsv-low #555: the shipped statements under real SQLite over the shipped
    /// migrations: one row per graph REPLACED (never appended), its first
    /// `created_at` kept, the records read back whole lowest score first, a
    /// delete of one leaving the other, and the health list's columns.
    #[test]
    fn e555_the_shipped_statements_replace_one_row_per_graph() {
        let conn = sqlite();
        upsert(&conn, &record("b.0", 2, 1, 1));
        upsert(&conn, &record("a.0", 1, 3, 1));
        conn.execute(
            "UPDATE gasp_deferred_graphs SET created_at = created_at - 600 WHERE outpoint = 'a.0'",
            [],
        )
        .unwrap();
        upsert(&conn, &record("a.0", 1, 6, 2));
        let (count, bytes): (i64, i64) = conn
            .query_row(HEALTH_COUNT_SQL, [], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap();
        assert_eq!(count, 2, "replaced, never appended");
        let a = serde_json::to_string(&record("a.0", 1, 6, 2))
            .unwrap()
            .len();
        let b = serde_json::to_string(&record("b.0", 2, 1, 1))
            .unwrap()
            .len();
        assert_eq!(bytes as usize, a + b, "totalBytes");
        let back = select(&conn);
        assert_eq!(
            back.iter()
                .map(|r| (r.outpoint.as_str(), r.nodes.len(), r.passes))
                .collect::<Vec<_>>(),
            vec![("a.0", 6, 2), ("b.0", 1, 1)]
        );
        assert_eq!(back[0].pending[0].spent_by.as_deref(), Some("bb.0"));
        let (outpoint, nodes, passes, age): (String, i64, i64, i64) = conn
            .query_row(HEALTH_LIST_SQL, [HEALTH_LIST_MAX], |r| {
                Ok((r.get(2)?, r.get(3)?, r.get(6)?, r.get(9)?))
            })
            .unwrap();
        assert_eq!((outpoint.as_str(), nodes, passes), ("a.0", 6, 2));
        assert!(
            (600..610).contains(&age),
            "the age is the first deferral's: {age}"
        );
        conn.execute(
            DEFERRED_GRAPH_DELETE_SQL,
            rusqlite::params!["https://peer", "tm_x", "a.0"],
        )
        .unwrap();
        assert_eq!(
            select(&conn)
                .iter()
                .map(|r| r.outpoint.clone())
                .collect::<Vec<_>>(),
            vec!["b.0".to_string()]
        );
    }

    /// A record of `nodes` nodes, each a raw transaction of `hex` hex chars.
    fn fat_record(outpoint: &str, nodes: usize, hex: usize, passes: u32) -> DeferredGraph {
        let mut r = record(outpoint, 1, nodes, passes);
        for (i, w) in r.nodes.iter_mut().enumerate() {
            w.node.raw_tx = format!("{i:04x}").repeat(hex / 4);
        }
        r
    }

    /// bsv-low #585, DOOR 4: a record of ANY size is saved and read back
    /// whole. 46 nodes of 26 KB (the engine pin's graph at its end: 2.4 MB of
    /// JSON, past D1's 2 MB row and twice the old 1 MiB cap) are one head row
    /// and two part rows, none past [`DEFERRED_GRAPH_CHUNK_BYTES`]; the read
    /// is the record, byte for byte; `bytes` is the whole record's, so the
    /// byte bounds count it whole. A replacement writes a new generation and
    /// leaves no part of the old; a record back under a row's room leaves no
    /// part at all; a delete takes the parts with the head. A record of at
    /// most a row's room is written as before #585 (no part, no generation).
    /// RED on `e8ab762`: no chunk table, no `chunk_plan`; the engine refused
    /// the record before the storage saw it (`too_big`).
    #[test]
    fn e585_d4_a_record_past_a_row_is_chunked_saved_and_read_back_whole() {
        let conn = sqlite();
        let big = fat_record("big.0", 46, 52_236, 2);
        let json = serde_json::to_string(&big).unwrap();
        assert!(json.len() > 2_000_000, "past D1's row: {}", json.len());
        upsert(&conn, &big);
        let rows_of = parts(&conn);
        assert_eq!(rows_of.len(), 2, "{rows_of:?}");
        assert_eq!(
            rows_of.iter().map(|p| p.0).collect::<Vec<_>>(),
            [1, 2],
            "seq from 1"
        );
        let (head_len, bytes, chunks, gen): (i64, i64, i64, String) = conn
            .query_row(
                "SELECT length(CAST(record AS BLOB)), bytes, chunks, gen FROM gasp_deferred_graphs",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(head_len as usize, DEFERRED_GRAPH_CHUNK_BYTES);
        assert_eq!((bytes as usize, chunks), (json.len(), 2));
        assert_eq!(gen.len(), 16);
        assert!(rows_of
            .iter()
            .all(|p| p.1 == gen && p.2 <= DEFERRED_GRAPH_CHUNK_BYTES));
        assert_eq!(
            head_len as usize + rows_of.iter().map(|p| p.2).sum::<usize>(),
            json.len()
        );
        let back = get(&conn, &big).unwrap().unwrap();
        assert_eq!(serde_json::to_string(&back).unwrap(), json, "byte for byte");
        println!(
            "#585 DOOR 4 (worker): a record of {} bytes is 1 head row of {head_len} and {} part rows of {:?}",
            json.len(),
            rows_of.len(),
            rows_of.iter().map(|p| p.2).collect::<Vec<_>>()
        );

        // A replacement: a new generation, no part of the old left.
        let mut bigger = fat_record("big.0", 70, 52_236, 3);
        bigger.reason = "bytes".into();
        upsert(&conn, &bigger);
        let rows_of = parts(&conn);
        assert_eq!(rows_of.len(), 3);
        assert!(
            rows_of.iter().all(|p| p.1 != gen),
            "the old generation is gone"
        );
        assert_eq!(get(&conn, &bigger).unwrap().unwrap().nodes.len(), 70);
        assert_eq!(rows(&conn), ["big.0"], "one head row per graph");
        // The same JSON again rewrites its own generation and tears nothing.
        upsert(&conn, &bigger);
        assert_eq!(parts(&conn), rows_of);
        assert_eq!(get(&conn, &bigger).unwrap().unwrap().nodes.len(), 70);

        // Back under a row's room: one row, as before #585.
        let small = record("big.0", 1, 3, 4);
        upsert(&conn, &small);
        assert!(parts(&conn).is_empty());
        let (chunks, gen): (i64, String) = conn
            .query_row("SELECT chunks, gen FROM gasp_deferred_graphs", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!((chunks, gen.as_str()), (0, ""));
        assert_eq!(get(&conn, &small).unwrap().unwrap().passes, 4);

        // A delete takes the parts with the head.
        upsert(&conn, &big);
        assert_eq!(parts(&conn).len(), 2);
        delete(&conn, &big);
        assert!(rows(&conn).is_empty() && parts(&conn).is_empty());
        assert!(get(&conn, &big).is_none());

        // The plan cuts on character boundaries and concatenates to the JSON.
        let odd = "aé".repeat(1000);
        let plan = chunk_plan(&odd, 64);
        assert!(plan.rest.iter().all(|p| p.len() <= 64) && plan.head.len() <= 64);
        assert_eq!(format!("{}{}", plan.head, plan.rest.concat()), odd);
        assert_eq!(chunk_plan("{}", 64).rest.len(), 0);
        assert_eq!(chunk_plan("{}", 64).gen, "");

        // The storage runs these statements in this order.
        let storage = include_str!("d1_storage.rs");
        let put_src = &storage[storage.find("async fn put_deferred_graph").unwrap()..];
        let put_src = &put_src[..put_src.find("async fn find_deferred_graphs").unwrap()];
        let at = |needle: &str| put_src.find(needle).unwrap_or_else(|| panic!("{needle}"));
        assert!(at("DEFERRED_GRAPH_CHUNK_PUT_SQL") < at("DEFERRED_GRAPH_UPSERT_SQL"));
        assert!(at("DEFERRED_GRAPH_UPSERT_SQL") < at("DEFERRED_GRAPH_CHUNKS_KEEP_SQL"));
        assert!(at("DEFERRED_GRAPH_UPSERT_SQL") < at("DEFERRED_GRAPH_CHUNKS_UNDO_SQL"));
        let get_src = &storage[storage.find("async fn get_deferred_graph").unwrap()..];
        let get_src = &get_src[..get_src
            .find("async fn find_transactions_for_proof_check")
            .unwrap()];
        // The read is `read_deferred_graph` over D1's rows (the land lens
        // E585-LAND-L3: one read, the stored-rows reader's too), and a torn
        // record is dropped.
        assert!(get_src.contains("read_deferred_graph(&D1DeferredRows(&self.db)"));
        assert!(get_src.contains("DEFERRED_GRAPH_CHUNKS_DELETE_SQL"));
        let rows_src = &storage[storage
            .find("impl crate::gasp_deferred::DeferredRows for D1DeferredRows")
            .unwrap()..];
        let rows_src = &rows_src[..rows_src.find("\n}\n").unwrap()];
        assert!(rows_src.contains("DEFERRED_GRAPH_GET_SQL"));
        assert!(rows_src.contains("DEFERRED_GRAPH_CHUNK_GET_SQL"));
    }

    /// bsv-low #585, DOOR 4: the ceiling is a BUDGET of room and a refusal
    /// loses nothing held. A chunked replacement the byte bound refuses
    /// (`AtCeiling`) leaves the record held WHOLE under its own generation
    /// (the new parts are taken back, the old never touched), so the engine,
    /// which keeps a resumed walk's record at `AtCeiling`, resumes from it. A
    /// part with no head is swept by the cron once it is an hour old, and a
    /// head whose part is gone reads as a missing part (the storage then
    /// drops the record; the table is transient).
    #[test]
    fn e585_d4_a_refused_replacement_leaves_the_held_record_whole() {
        let conn = sqlite();
        let held = fat_record("big.0", 46, 52_236, 2);
        let held_len = serde_json::to_string(&held).unwrap().len() as u64;
        upsert(&conn, &held);
        let before = parts(&conn);
        // The global bytes leave room for the held record and no more.
        let bigger = fat_record("big.0", 70, 52_236, 3);
        assert!(
            !upsert_under(&conn, &bigger, 256, held_len + 1),
            "AtCeiling"
        );
        assert_eq!(parts(&conn), before, "the held generation, and only it");
        let back = get(&conn, &held).unwrap().unwrap();
        assert_eq!((back.nodes.len(), back.passes), (46, 2), "held whole");
        // A NEW key refused room leaves nothing behind.
        let other = fat_record("other.0", 46, 52_236, 1);
        assert!(!upsert_under(&conn, &other, 256, held_len + 1));
        assert_eq!(parts(&conn), before);
        assert_eq!(rows(&conn), ["big.0"]);

        // The cron's sweep: an orphan part an hour old goes; a young one (a
        // save in flight) and every part a head names stay.
        for (gen, age) in [
            ("dead", DEFERRED_GRAPH_ORPHAN_CHUNK_SECS as i64 + 1),
            ("inflight", 5),
        ] {
            conn.execute(
                "INSERT INTO gasp_deferred_graph_chunks (host, topic, outpoint, gen, seq, part, written_at) \
                 VALUES ('https://peer', 'tm_x', 'big.0', ?1, 1, 'x', unixepoch() - ?2)",
                rusqlite::params![gen, age],
            )
            .unwrap();
        }
        conn.execute(
            "UPDATE gasp_deferred_graph_chunks SET written_at = unixepoch() - 99999 WHERE gen NOT IN ('dead', 'inflight')",
            [],
        )
        .unwrap();
        let swept = conn
            .execute(
                DEFERRED_GRAPH_CHUNKS_SWEEP_SQL,
                [DEFERRED_GRAPH_ORPHAN_CHUNK_SECS as i64],
            )
            .unwrap();
        assert_eq!(swept, 1, "the dead save's part alone");
        assert_eq!(parts(&conn).len(), before.len() + 1);
        assert_eq!(get(&conn, &held).unwrap().unwrap().nodes.len(), 46);
        let lib = include_str!("gasp_deferred.rs");
        let sweep = &lib[lib.find("pub async fn sweep_stale").unwrap()..];
        assert!(sweep[..sweep.find("/// The health block lists").unwrap()]
            .contains("DEFERRED_GRAPH_CHUNKS_SWEEP_SQL"));

        // A head whose part is gone names the part; the storage drops it.
        conn.execute(
            "DELETE FROM gasp_deferred_graph_chunks WHERE seq = 2 AND gen NOT IN ('dead', 'inflight')",
            [],
        )
        .unwrap();
        assert_eq!(get(&conn, &held).unwrap().unwrap_err(), 2);
    }

    /// bsv-low #555: the health block names the count, the oldest and each
    /// graph {topic, peer, outpoint, nodes, pending, calls, passes, reason,
    /// bytes, ageSecs}, with the budget it runs under.
    // bsv-low #586, the lens fold (E586-L1), amended by bsv-low #585 (door 4).
    // The two limbs are vars an operator sets, clamped, the consts their
    // defaults. The bytes limb's default is the engine's 917,504 (18 heads of
    // 26 KB a fresh pass): it was seven eighths of a 1 MiB record cap and the
    // var was clamped to that cap; a record has no cap now, so the default is
    // a budget per pass and the var has no upper clamp. RED on `e8ab762`: the
    // var is clamped to 1,048,576.
    #[test]
    fn e586f_l1_the_limbs_default_under_the_record_cap_and_are_vars() {
        assert_eq!(GASP_GRAPH_BUDGET_BYTES, 917_504);
        assert_eq!(
            GASP_GRAPH_BUDGET_BYTES,
            overlay_engine::gasp::DEFAULT_GRAPH_BUDGET_BYTES
        );
        assert_eq!(GASP_GRAPH_BUDGET_NODES, 64);
        // A 26 KB head is 52,236 hex bytes (the #586 witness's): the limb is
        // read before a step, so a fresh pass is served 18.
        assert_eq!(GASP_GRAPH_BUDGET_BYTES.div_ceil(52_236), 18);

        // Unset, empty, not a number: the defaults.
        let defaults = (GASP_GRAPH_BUDGET_BYTES, GASP_GRAPH_BUDGET_NODES);
        assert_eq!(graph_budget_limbs(None, None), defaults);
        assert_eq!(graph_budget_limbs(Some(""), Some("  ")), defaults);
        assert_eq!(graph_budget_limbs(Some("4MiB"), Some("-3")), defaults);
        assert_eq!(graph_budget_limbs(Some("1e6"), Some("6.5")), defaults);
        // Named: taken as is inside the clamp, each on its own.
        assert_eq!(
            graph_budget_limbs(Some("400000"), Some("12")),
            (400_000, 12)
        );
        assert_eq!(graph_budget_limbs(Some(" 400000 "), None), (400_000, 64));
        assert_eq!(graph_budget_limbs(None, Some("12")), (917_504, 12));
        // Clamped: never 0 (no step would be made), never more nodes than
        // calls. The bytes have no upper clamp (no record cap, #585).
        assert_eq!(graph_budget_limbs(Some("0"), Some("0")), (1, 1));
        assert_eq!(
            graph_budget_limbs(Some("4194304"), Some("5000")),
            (4_194_304, GASP_GRAPH_BUDGET_CALLS)
        );
        assert_eq!(
            graph_budget_limbs(Some("18446744073709551615"), Some("4294967296")),
            (u64::MAX, GASP_GRAPH_BUDGET_CALLS)
        );
        assert_eq!(GASP_GRAPH_BUDGET_BYTES_VAR, "GASP_GRAPH_BUDGET_BYTES");
        assert_eq!(GASP_GRAPH_BUDGET_NODES_VAR, "GASP_GRAPH_BUDGET_NODES");

        // The wiring: the engine and the health block both take the vars.
        let lib = include_str!("lib.rs");
        assert!(lib.contains(
            "let (limb_bytes, limb_nodes) = crate::gasp_deferred::graph_budget_limbs_from_env(env);\n    engine.set_graph_budget_limbs(limb_bytes, limb_nodes);"
        ));
        assert!(!lib.contains("gasp_deferred::GASP_GRAPH_BUDGET_BYTES"));
        let ops = include_str!("ops.rs");
        assert!(ops.contains("crate::gasp_deferred::graph_budget_limbs_from_env(env)"));
    }

    #[test]
    fn e555_the_health_block_names_each_deferred_graph() {
        let row = HealthRow {
            host: "https://peer".into(),
            topic: "tm_x".into(),
            outpoint: "a.0".into(),
            nodes: 40.0,
            pending: 3.0,
            calls: 120.0,
            passes: 3.0,
            reason: "time".into(),
            bytes: 51_000.0,
            age_secs: Some(180.0),
        };
        let v = health_view(
            1,
            51_000,
            &[row],
            (GASP_GRAPH_BUDGET_BYTES, GASP_GRAPH_BUDGET_NODES),
        );
        assert_eq!(v["readable"], true);
        assert_eq!(v["count"], 1);
        assert_eq!(v["totalBytes"], 51_000);
        assert_eq!(
            (
                v["budget"]["maxRows"].clone(),
                v["budget"]["maxTotalBytes"].clone(),
                v["budget"]["staleSecs"].clone(),
                v["budget"]["maxRowsPerHost"].clone(),
                v["budget"]["maxBytesPerHost"].clone()
            ),
            (
                serde_json::json!(256),
                serde_json::json!(64u64 << 20),
                serde_json::json!(108_000),
                serde_json::json!(32),
                serde_json::json!(8u64 << 20)
            )
        );
        assert_eq!(v["oldest"]["outpoint"], "a.0");
        assert_eq!(
            v["graphs"][0],
            serde_json::json!({"topic": "tm_x", "peer": "https://peer", "outpoint": "a.0",
                "nodes": 40, "pending": 3, "calls": 120, "passes": 3, "reason": "time",
                "bytes": 51000, "ageSecs": 180})
        );
        assert_eq!(v["budget"]["calls"], GASP_GRAPH_BUDGET_CALLS);
        assert_eq!(v["budget"]["ms"], GASP_GRAPH_BUDGET_MS);
        // bsv-low #586: the two limbs, the engine's defaults (a budget per
        // pass; no record cap since bsv-low #585 door 4).
        assert_eq!(v["budget"]["bytesFetched"], 917_504);
        assert_eq!(v["budget"]["nodes"], 64);
        // The limbs served are the ones the worker runs with (the vars).
        let tuned = health_view(0, 0, &[], (400_000, 12));
        assert_eq!(tuned["budget"]["bytesFetched"], 400_000);
        assert_eq!(tuned["budget"]["nodes"], 12);
        let none = health_view(
            0,
            0,
            &[],
            (GASP_GRAPH_BUDGET_BYTES, GASP_GRAPH_BUDGET_NODES),
        );
        assert_eq!(
            (none["count"].clone(), none["oldest"].clone()),
            (serde_json::json!(0), serde_json::Value::Null)
        );
    }

    /// bsv-low #555: the counters of a sync, summed over topics, a drop
    /// counted in all and under its reason; every name seeded.
    #[test]
    fn e555_the_counters_sum_a_sync_and_count_drops_by_reason() {
        let topic = |deferred, resumed, converged, reasons: &[&str]| TopicSyncResult {
            peers: vec![],
            sync_type: "peers".into(),
            errors: vec![],
            pruned_inputs: 0,
            discarded_graphs: 0,
            finalized_graphs: 0,
            deadline_dropped_graphs: 0,
            cursor_moves: vec![],
            deferred_graphs: deferred,
            resumed_graphs: resumed,
            converged_graphs: converged,
            dropped_graphs: reasons
                .iter()
                .map(|r| DroppedDeferral {
                    peer: "p".into(),
                    outpoint: "o".into(),
                    reason: (*r).into(),
                })
                .collect(),
            stalled_graphs: 0,
            held_back_graphs: 0,
        };
        let results = std::collections::HashMap::from([
            ("tm_a".to_string(), topic(2, 1, 0, &["max_passes"])),
            (
                "tm_b".to_string(),
                topic(0, 1, 1, &["not_served", "max_passes"]),
            ),
        ]);
        assert_eq!(
            counter_deltas(&results),
            vec![
                ("gasp_graph_converged_total".to_string(), 1),
                ("gasp_graph_deferred_total".to_string(), 2),
                ("gasp_graph_dropped_max_passes_total".to_string(), 2),
                ("gasp_graph_dropped_not_served_total".to_string(), 1),
                ("gasp_graph_dropped_total".to_string(), 3),
                ("gasp_graph_resumed_total".to_string(), 2),
            ]
        );
        let names = counter_names();
        assert_eq!(names.len(), 5 + 10);
        // bsv-low #585: nothing is dropped for its size.
        assert!(!names.contains(&"gasp_graph_dropped_too_big_total".to_string()));
        assert!(names.contains(&"gasp_graph_dropped_root_proven_total".to_string()));
        // The delta-2 fold's D2-M2: a resumed walk dropped after its idle faults.
        assert!(names.contains(&"gasp_graph_dropped_idle_faults_total".to_string()));
    }
}
