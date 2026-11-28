//! Mainnet SHIP fan-out — ports `@bsv/sdk` `SHIPBroadcaster` behavior for
//! Cloudflare Workers.
//!
//! When the Engine admits a transaction, its built-in SHIP propagation loop
//! (`engine.rs:654-712`) only broadcasts to peers indexed in our own
//! `ls_ship` storage. That's fine for a mature overlay whose SHIP directory
//! is already populated, but on a fresh node or during BSVA's migration
//! windows, our local view of the tm_uhrp peer set is stale.
//!
//! This module reaches the mainnet DEFAULT_SLAP_TRACKERS directly — the
//! same way a TS-SDK `LookupResolver({networkPreset:'mainnet'})` would —
//! to pull the authoritative peer list for each topic, then POSTs the BEEF
//! to every discovered peer. By running this alongside the Engine's local
//! propagation, our fresh records reach every tm_uhrp overlay on mainnet
//! within seconds of admission.
//!
//! ## Parity references
//!
//! - `bsv-rs/src/overlay/topic_broadcaster.rs::TopicBroadcaster::broadcast_tx`
//!   (SHIPBroadcaster in @bsv/sdk) — discover via `ls_ship` + parallel POST
//!   (here: the lookups a bounded number at once, [`lookups_within`]; the
//!   POSTs a bounded number at once, [`within_budget`]).
//! - `bsv-rs/src/overlay/facilitators.rs` — HTTP facilitator shape, mirrored
//!   here with `worker::Fetch` because `reqwest` isn't available in the
//!   Workers runtime.
//! - `bsv-storage-cloudflare/src/overlay/mod.rs::fan_out_to_mainnet_peers` —
//!   same pattern at the storage-server layer; this module retires that
//!   duplication once we've verified we're covering both call sites.
//!
//! ## Correctness contract
//!
//! - **Best-effort**. Per-peer errors are logged and swallowed; the caller
//!   has already locally admitted the tx, so partial mainnet coverage is
//!   acceptable.
//! - **Self-filter**. A tracker's advert of our own origin is never a target
//!   (by [`overlay_engine::gasp::peer_origin`]), so we never submit to
//!   ourselves in a loop.
//! - **Dedup**. Different trackers, and different spellings, may name the same
//!   origin; each origin takes one place and is POSTed to once per call.
//! - **Per-topic**. Multi-topic BEEFs get one `ls_ship` query per topic to
//!   discover the union of interested peers, then the BEEF is submitted
//!   once per peer with the full topic list (mirrors SHIPBroadcaster).
//! - **Admission (FANOUT-0811, 2026-11-16)**. A tracker's answer output names
//!   a host only where this overlay's own SHIP topic manager admits it
//!   ([`SHIPTopicManager::admissible_advert`]: `SHIP`, five fields, an
//!   advertisable URI, a signature by the identity key's linked key with the
//!   output locked by that key), read from the answer BEEF's SUBJECT
//!   ([`overlay_engine::subject::subject_txid_of`]), for the topic asked.
//! - **The POST target (the fold)**. An advert names an ORIGIN, never a URL:
//!   the POST goes to `https://host[:port]/submit` built by [`post_target`]; no
//!   path, query, fragment or userinfo of the advert's survives, and an advert
//!   whose URI is not plain `https` names no target.
//! - **The fill (the fold)**. One place per origin
//!   ([`overlay_engine::gasp::peer_origin`]), at most [`FANOUT_MAX_HOSTS`],
//!   filled by [`plan_fanout`]: round-robin across the trackers, and in rounds
//!   of the identity keys (no key takes a second place while another key's
//!   advert waits for a first). The work is bounded apart from the places:
//!   [`FANOUT_MAX_OUTPUTS_PER_ANSWER`] and [`FANOUT_MAX_OUTPUTS_READ`] bound
//!   the reads, [`FANOUT_MAX_VERIFICATIONS`] the signature checks (a verdict
//!   is remembered per advert outpoint, [`VerdictMemo`], and a remembered
//!   verdict is used whether or not the checks are spent). What is dropped is
//!   counted, after the POSTs.
//! - **The lookups hold a bounded amount (FANOUT-0811-FOLD-5)**. One lookup a
//!   (topic, tracker), up to 400 for the route's 100 topics, made at most
//!   [`FANOUT_ASK_CONNECTIONS`] (6) at once in the asked order (topic, then
//!   tracker) by [`lookups_within`]. An answer is read whole, and turned into
//!   its adverts INSIDE its own request ([`adverts_of_answer`], at most
//!   [`FANOUT_MAX_OUTPUTS_PER_ANSWER`] outputs), its text dropped before the
//!   request answers; what the fan-out holds between the lookups and the plan
//!   is adverts, folded in the asked order whatever order they arrive in
//!   ([`LookupFold`]), trimmed at every hand-in to the read bound
//!   [`FANOUT_MAX_OUTPUTS_READ`], and no lookup is started once the answers
//!   before it have read that bound. So the most held at once is six answer
//!   texts in flight (each as large as its tracker serves: an answer is read
//!   whole; at most six are read at once) and at most 1,024 adverts kept (a
//!   PushDrop output each, a few hundred bytes). The plan is the one the
//!   answers give read one after another in the asked order.
//! - **Deadlines**. Each lookup is raced against
//!   [`FANOUT_ASK_EACH_DEADLINE_MS`] from its start inside the lookups' phase,
//!   [`FANOUT_ASK_DEADLINE_MS`]; then the POSTs under the phase's
//!   [`FANOUT_POST_DEADLINE_MS`] ([`within_budget`]), so the counter write
//!   follows the start within 18 s. A request cut by its deadline is counted
//!   ([`COUNTER_REQUESTS_CUT`]), a lookup never started
//!   ([`COUNTER_LOOKUPS_UNTRIED`]).
//! - **A cut request is aborted**. Every lookup and every POST is sent with an
//!   abort signal ([`worker::Fetch::send_with_signal`]) whose controller is
//!   held by an [`AbortOnDrop`] guard: the request's future dropped for any
//!   reason (its deadline, the phase's, the fan-out's own end) aborts its
//!   fetch, and a POST's response is released unread (the guard is dropped
//!   once its status is read). Dropping a future alone aborts nothing: the
//!   fetch would run on with its body and its connection. What the platform
//!   does with an aborted fetch's body and connection is not shown natively;
//!   on a deployment the bound below holds as far as an abort releases them
//!   (read [`COUNTER_REQUESTS_CUT`] beside the isolate's memory to see it).
//! - **Bytes in flight (FANOUT-0811-FOLD-3)**. Each POST holds its own copy of
//!   the BEEF (a `Uint8Array` made per request), so the fan-out sends to a
//!   bounded number of hosts at once, by the bytes in flight
//!   ([`within_budget`]): at most [`FANOUT_POST_BYTES_IN_FLIGHT`] of body and
//!   at most [`FANOUT_POST_CONNECTIONS`] requests ([`post_concurrency`]); a
//!   small BEEF goes out together, one past the budget one host at a time.
//!   The budget is PER FAN-OUT: each admission's `wait_until` runs its own, so
//!   N fan-outs at once in one isolate hold up to N times it (a budget shared
//!   by the isolate is not built). Each POST is raced against
//!   [`FANOUT_POST_EACH_DEADLINE_MS`] inside the phase's
//!   [`FANOUT_POST_DEADLINE_MS`] and aborted when cut; a target the phase ends
//!   before it was tried is counted ([`COUNTER_POSTS_UNTRIED`]).

use overlay_engine::beef_limits;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;

use bsv_rs::script::templates::PushDrop;
use bsv_rs::transaction::TransactionOutput;
use overlay_discovery::ship::topic_manager::SHIPTopicManager;
use overlay_engine::types::TaggedBEEF;
use worker::{AbortController, D1Database, Delay, Fetch, Headers, Method, Request, RequestInit};

/// Default SLAP trackers for mainnet — lifted verbatim from
/// `@bsv/sdk/src/overlay-tools/LookupResolver.ts:42` via
/// `bsv_rs::overlay::types::NetworkPreset::Mainnet.slap_trackers()`. Kept
/// inline here so the module is self-contained and doesn't require the
/// caller to thread a `NetworkPreset` through.
const DEFAULT_SLAP_TRACKERS: &[&str] = &[
    "https://overlay-us-1.bsvb.tech",
    "https://overlay-eu-1.bsvb.tech",
    "https://overlay-ap-1.bsvb.tech",
    "https://users.bapp.dev",
];

/// SHIP lookup question name — constant per BRC-95.
const LS_SHIP: &str = "ls_ship";

/// The most distinct ORIGINS one admission POSTs its BEEF to: one place per
/// origin. The outer bound is the platform's subrequest limit per invocation
/// (1,000 on the Workers Paid plan, shared by the `wait_until` task with the
/// request that scheduled it); before the POSTs the path spends the
/// admission's own broadcaster, courier and chaintracks calls and 4 tracker
/// lookups per topic here. 32 is above every SHIP topic's peer set the fleet
/// has seen (live answers name 12 to 14 hosts).
pub const FANOUT_MAX_HOSTS: usize = 32;

/// The most outputs of ONE tracker answer READ (a BEEF parse and a PushDrop
/// decode, no signature check). A WORK bound, not a bound on what is
/// accepted: a refused or duplicate advert costs a read and nothing more, so
/// it hides a valid one only past this many outputs of the same answer (live
/// answers carry 13 to 15). Outputs past it are counted
/// ([`COUNTER_OUTPUTS_OVER_BOUND`]).
pub const FANOUT_MAX_OUTPUTS_PER_ANSWER: usize = 256;

/// The most outputs READ over every answer of one fan-out, in the asked order
/// (the route admits up to 100 topics, 4 answers each), and so the most
/// adverts the fan-out holds ([`LookupFold`]). The outputs of an answer past
/// it are counted ([`COUNTER_OUTPUTS_OVER_BOUND`]); a lookup whose share of it
/// the answers before it have spent is never started, and is counted
/// ([`COUNTER_LOOKUPS_UNTRIED`]).
pub const FANOUT_MAX_OUTPUTS_READ: usize = 1_024;

/// The most signature checks ([`SHIPTopicManager::admissible_advert`]) one
/// fan-out makes. A verdict the isolate remembers ([`VerdictMemo`]) costs none
/// and is used even once the checks are spent. No check is made for a NEW
/// place once the places are full; the default-port pass may still check an
/// admitted origin's default-port advert (at most the adverts naming the
/// default-port spelling of a placed origin), inside this same budget. Chosen from the
/// measured cost (`fanout0811_fold_measure_a_read_and_a_check`, native,
/// release, aarch64, 2026-11-16): a check about 0.16 ms, a read about 10 us,
/// so 128 checks are about 20 ms and the fan-out's worst (1,024 reads and 128
/// checks) about 30 ms of CPU an admission. wasm32 was not measured; at an
/// assumed 2 to 10 times slower that is 0.06 to 0.3 s. 128 is four times the
/// places: each of the four trackers' turns can place an origin per check
/// with as many refusals to spare. An advert left unchecked is counted
/// ([`COUNTER_VERIFICATIONS_OVER_BOUND`]).
pub const FANOUT_MAX_VERIFICATIONS: usize = 128;

/// The verdicts an isolate remembers, keyed by the advert's outpoint (its
/// txid commits its outputs, so the verdict is the outpoint's for good). At
/// most this many entries; the eviction rule is [`VerdictMemo::remember`]'s
/// (a refusal never evicts an admission). About 100 bytes an entry.
pub const FANOUT_VERDICT_MEMO_MAX: usize = 4_096;

/// The deadline of the lookups' phase: started with the phase, it ends the
/// lookups' pool ([`lookups_within`]) whatever its requests do; a lookup in
/// flight then is cut (and aborted), one not yet started is untried. With
/// [`FANOUT_POST_DEADLINE_MS`] the fan-out's network legs end within 18 s,
/// inside the 30 s Cloudflare documents for a `ctx.waitUntil` task after the
/// response (the Workers Context API's `waitUntil`; the figure the house
/// already budgets against, bsv-low #649), with the counter write and the
/// admission's other background work after it. Not measured on a Worker.
pub const FANOUT_ASK_DEADLINE_MS: u64 = 8_000;

/// The deadline of each tracker lookup once it is started, inside the phase's
/// [`FANOUT_ASK_DEADLINE_MS`]: a tracker that never answers holds its slot of
/// the pool for this long at most (its fetch then aborted, [`AbortOnDrop`]),
/// so a hung tracker costs the others at most half the phase.
pub const FANOUT_ASK_EACH_DEADLINE_MS: u64 = 4_000;

/// The most tracker lookups in flight at once: the platform's six
/// connections ([`FANOUT_POST_CONNECTIONS`]). An answer is read whole into
/// its request and turned into adverts there, its text dropped before the
/// request answers, so at most this many answer texts are alive at once.
pub const FANOUT_ASK_CONNECTIONS: usize = FANOUT_POST_CONNECTIONS;

/// The deadline of the POST phase: started with the phase, it ends the pool
/// ([`within_budget`]) whatever its requests do; a POST in flight then is cut
/// (and aborted), a target not yet started is untried.
pub const FANOUT_POST_DEADLINE_MS: u64 = 10_000;

/// The body bytes ONE fan-out's POST phase holds in flight at once: a budget
/// per fan-out, not per isolate (several admissions in one isolate each run
/// their own fan-out, each with this budget).
/// The isolate's arithmetic (`CLAUDE.md`, "The script door reads the stream",
/// "The default's arithmetic"): of the 128 MB isolate, 56 MB is left to the
/// module and the runtime and 48 MiB is the door's memory limb, which leaves
/// about 24 MB for a request's own copies (its body, the completed BEEF, the
/// EF batch). The fan-out runs after the response, in `wait_until`, while the
/// isolate may serve other requests, so it takes a THIRD of those 24 MB:
/// 8 MiB of body copies at once, each request taken to hold one copy of the
/// BEEF (the runtime's own buffering of a body beside it is not measured), a
/// cut request aborted so its copy goes with it ([`AbortOnDrop`]; what the
/// platform frees on an abort is not measured).
/// A BEEF past the budget is sent one host at a time: its one copy is then the
/// most in flight.
pub const FANOUT_POST_BYTES_IN_FLIGHT: usize = 8 * 1024 * 1024;

/// The most POSTs in flight at once: the platform's simultaneous open
/// connections for one invocation (Cloudflare documents six; past them a
/// request queues, already holding its body).
pub const FANOUT_POST_CONNECTIONS: usize = 6;

/// The deadline of each POST once it is started, inside the phase's
/// [`FANOUT_POST_DEADLINE_MS`]: a host that never answers holds its slot of
/// the pool for this long at most, and its fetch is then aborted
/// ([`AbortOnDrop`]), so it costs the others at most one slot's 5 s of the
/// phase's 10 s as far as the platform releases an aborted fetch's
/// connection (not shown natively).
pub const FANOUT_POST_EACH_DEADLINE_MS: u64 = 5_000;

/// Answer outputs that named no host: not an advert this overlay's SHIP topic
/// manager admits, an advert for another topic, a URI that is not plain
/// `https`, or a BEEF that does not read.
pub const COUNTER_ADVERTS_REFUSED: &str = "mainnet_fanout_adverts_refused_total";
/// Answer outputs not read: past [`FANOUT_MAX_OUTPUTS_PER_ANSWER`] or
/// [`FANOUT_MAX_OUTPUTS_READ`].
pub const COUNTER_OUTPUTS_OVER_BOUND: &str = "mainnet_fanout_outputs_over_bound_total";
/// Distinct origins left without a place once the [`FANOUT_MAX_HOSTS`] were
/// full.
pub const COUNTER_HOSTS_OVER_BOUND: &str = "mainnet_fanout_hosts_over_bound_total";
/// Adverts left unchecked once [`FANOUT_MAX_VERIFICATIONS`] were spent.
pub const COUNTER_VERIFICATIONS_OVER_BOUND: &str = "mainnet_fanout_verifications_over_bound_total";
/// Tracker lookups and POSTs cut by their deadline once started
/// ([`FANOUT_ASK_EACH_DEADLINE_MS`], [`FANOUT_POST_EACH_DEADLINE_MS`], or a
/// phase's deadline, [`FANOUT_ASK_DEADLINE_MS`] or
/// [`FANOUT_POST_DEADLINE_MS`], falling on a request in flight).
pub const COUNTER_REQUESTS_CUT: &str = "mainnet_fanout_requests_cut_total";
/// Targets the POST phase's [`FANOUT_POST_DEADLINE_MS`] ended before they were
/// tried (FANOUT-0811-FOLD-3: the bytes-in-flight bound sends to a few hosts
/// at a time, so a slow phase leaves some untried; never silently).
pub const COUNTER_POSTS_UNTRIED: &str = "mainnet_fanout_posts_untried_total";
/// Tracker lookups never started: the lookups' phase
/// ([`FANOUT_ASK_DEADLINE_MS`]) ended first, or the fan-out's read bound
/// ([`FANOUT_MAX_OUTPUTS_READ`]) was spent by the answers before them.
pub const COUNTER_LOOKUPS_UNTRIED: &str = "mainnet_fanout_lookups_untried_total";
/// The seven counters, seeded at 0 by `ops::read_counters`.
pub const COUNTERS: [&str; 7] = [
    COUNTER_ADVERTS_REFUSED,
    COUNTER_OUTPUTS_OVER_BOUND,
    COUNTER_HOSTS_OVER_BOUND,
    COUNTER_VERIFICATIONS_OVER_BOUND,
    COUNTER_REQUESTS_CUT,
    COUNTER_POSTS_UNTRIED,
    COUNTER_LOOKUPS_UNTRIED,
];

/// PURE (bsv-low #731): whether an admission fans out (`MAINNET_FANOUT`). Unset, or any value but `false`, `0` or
/// `off`, fans out as before; the D1 budget tier sets `false`, so its admissions never ask the mainnet trackers
/// (a cell reads only what it seeded). The reference's `SHIPBroadcaster` is likewise the deployment's to configure.
pub fn enabled(value: Option<&str>) -> bool {
    !matches!(
        value.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("false" | "0" | "off")
    )
}

/// What one fan-out dropped, for its counters.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FanoutDrops {
    /// Answer outputs that named no host.
    pub adverts_refused: u64,
    /// Answer outputs not read (the read bounds).
    pub outputs_over_bound: u64,
    /// Distinct origins left without a place.
    pub hosts_over_bound: u64,
    /// Adverts left unchecked (the verification bound).
    pub verifications_over_bound: u64,
    /// Lookups and POSTs cut by their deadline once started.
    pub requests_cut: u64,
    /// Targets the POST phase ended before they were tried.
    pub posts_untried: u64,
    /// Lookups never started (the phase's end, or the read bound spent).
    pub lookups_untried: u64,
}

impl FanoutDrops {
    fn deltas(&self) -> BTreeMap<String, u64> {
        [
            (COUNTER_ADVERTS_REFUSED, self.adverts_refused),
            (COUNTER_OUTPUTS_OVER_BOUND, self.outputs_over_bound),
            (COUNTER_HOSTS_OVER_BOUND, self.hosts_over_bound),
            (
                COUNTER_VERIFICATIONS_OVER_BOUND,
                self.verifications_over_bound,
            ),
            (COUNTER_REQUESTS_CUT, self.requests_cut),
            (COUNTER_POSTS_UNTRIED, self.posts_untried),
            (COUNTER_LOOKUPS_UNTRIED, self.lookups_untried),
        ]
        .into_iter()
        .filter(|(_, n)| *n > 0)
        .map(|(name, n)| (name.to_string(), n))
        .collect()
    }
}

/// Where a POST goes: the canonical origin URL and its key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostTarget {
    /// `https://host[:port]`: the host lowercased, no trailing dot, the
    /// default port dropped; no path, query, fragment or userinfo.
    pub target: String,
    /// [`overlay_engine::gasp::peer_origin`] of the target: the place's key.
    pub origin: String,
    /// The target names no explicit port.
    pub default_port: bool,
}

/// PURE: the POST target of an advertised URI, or `None` where the URI is not
/// plain `https` with a host. The other advertisable schemes (`wss://`,
/// `https+bsvauth://` and its kin, `js8c+bsvauth+smf:`) name a transport this
/// POST does not speak, so they name no target.
pub fn post_target(uri: &str) -> Option<PostTarget> {
    let url = worker::Url::parse(uri.trim()).ok()?;
    if url.scheme() != "https" {
        return None;
    }
    let host = url.host_str()?.trim_end_matches('.').to_ascii_lowercase();
    if host.is_empty() {
        return None;
    }
    let target = match url.port() {
        Some(port) => format!("https://{host}:{port}"),
        None => format!("https://{host}"),
    };
    let origin = overlay_engine::gasp::peer_origin(&target);
    Some(PostTarget {
        target,
        origin,
        default_port: url.port().is_none(),
    })
}

/// PURE: the URL a target's POST is sent to (a target carries no path).
pub fn submit_url(target: &str) -> String {
    format!("{target}/submit")
}

/// One advert an answer offers, READ but not yet checked: the subject's
/// output, its claimed identity key and its target.
#[derive(Debug, Clone)]
pub struct Advert {
    /// Where its POST would go.
    pub target: PostTarget,
    /// field\[1\] as the token carries it (claimed until the check).
    pub identity_key: Vec<u8>,
    /// The subject's txid (lowercase) and the output's index.
    pub outpoint: (String, u32),
    output: TransactionOutput,
}

/// PURE: read an answer output for `topic` without checking its signature:
/// the advert's transaction is the BEEF's SUBJECT
/// ([`overlay_engine::subject::subject_txid_of`]), its output a five-field
/// PushDrop whose field\[0\] is `SHIP`, field\[3\] the topic asked and
/// field\[2\] a URI with a [`post_target`]. This only refuses; what it reads
/// still needs [`SHIPTopicManager::admissible_advert`] ([`advert_is_admitted`])
/// before it names a host.
pub fn read_advert(beef: &[u8], output_index: u32, topic: &str) -> Option<Advert> {
    // A tracker's answer past the proof limb is not read (lane INTAKE-BOUND): an advert's BEEF needs one path.
    crate::WORKER_WALK_BUDGET.proof_limb(beef).ok()?;
    let subject = {
        let mut parsed = beef_limits::parse_beef(beef, &beef_limits::PEER_BEEF_LIMITS).ok()?;
        overlay_engine::subject::subject_txid_of(&mut parsed)?
    };
    // The subject alone, nothing linked: the subject-only read under the same door.
    let tx = beef_limits::subject_from_beef(beef, Some(&subject), &beef_limits::PEER_BEEF_LIMITS)
        .ok()?;
    let output = tx.outputs.get(usize::try_from(output_index).ok()?)?;
    let fields = PushDrop::decode(&output.locking_script).ok()?.fields;
    if fields.len() != 5 || fields[0] != b"SHIP" || fields[3] != topic.as_bytes() {
        return None;
    }
    let target = post_target(std::str::from_utf8(&fields[2]).ok()?)?;
    Some(Advert {
        target,
        identity_key: fields[1].clone(),
        outpoint: (subject.to_ascii_lowercase(), output_index),
        output: output.clone(),
    })
}

/// PURE: whether this overlay's SHIP topic manager admits the advert's output
/// (THE rule, [`SHIPTopicManager::admissible_advert`]; one signature check).
pub fn advert_is_admitted(advert: &Advert) -> bool {
    matches!(
        SHIPTopicManager::admissible_advert(&advert.output),
        Ok(Some(_))
    )
}

/// PURE: the target one answer output names for `topic`, or `None`:
/// [`read_advert`], then [`advert_is_admitted`].
pub fn advert_host(beef: &[u8], output_index: u32, topic: &str) -> Option<String> {
    let advert = read_advert(beef, output_index, topic)?;
    advert_is_admitted(&advert).then_some(advert.target.target)
}

/// One tracker answer as its request reads it: the answer's TEXT is turned
/// into this inside the request and dropped there, so what a lookup hands back
/// is adverts, never text.
#[derive(Debug, Clone, Default)]
pub struct AnswerRead {
    /// Outputs the answer carries.
    outputs: usize,
    /// Outputs read: at most [`FANOUT_MAX_OUTPUTS_PER_ANSWER`].
    read: usize,
    /// The adverts read, each with its output's position in the answer, in
    /// answer order (every other read output was refused).
    adverts: Vec<(usize, Advert)>,
}

/// PURE: read one tracker's `/lookup` answer text for `topic`, at most
/// [`FANOUT_MAX_OUTPUTS_PER_ANSWER`] outputs. A body that is not an answer is
/// `Err`; an answer that is not an `output-list` offers none. The fan-out's
/// read bound ([`FANOUT_MAX_OUTPUTS_READ`]) is [`LookupFold`]'s.
pub fn adverts_of_answer(text: &str, topic: &str) -> Result<AnswerRead, String> {
    #[derive(serde::Deserialize)]
    struct LookupOutput {
        beef: Vec<u8>,
        #[serde(rename = "outputIndex")]
        output_index: u32,
    }
    #[derive(serde::Deserialize)]
    struct LookupAnswer {
        #[serde(rename = "type")]
        answer_type: String,
        #[serde(default)]
        outputs: Vec<LookupOutput>,
    }

    let answer: LookupAnswer = serde_json::from_str(text).map_err(|e| format!("parse: {e}"))?;
    if answer.answer_type != "output-list" {
        return Ok(AnswerRead::default());
    }
    let read = answer.outputs.len().min(FANOUT_MAX_OUTPUTS_PER_ANSWER);
    let adverts = answer
        .outputs
        .iter()
        .take(read)
        .enumerate()
        .filter_map(|(i, out)| read_advert(&out.beef, out.output_index, topic).map(|a| (i, a)))
        .collect();
    Ok(AnswerRead {
        outputs: answer.outputs.len(),
        read,
        adverts,
    })
}

/// What became of one lookup.
#[derive(Debug, Clone)]
enum Lookup {
    /// Never started: the phase ended, or the read bound was spent, first.
    Untried,
    /// Started; no answer yet (cut, if the phase ends so).
    Cut,
    /// It answered something that is not an answer.
    Fault(String),
    /// It answered (its adverts trimmed to what the fold can still read).
    Read(AnswerRead),
}

/// The fan-out's lookups, folded as they answer, in the order ASKED (topic,
/// then tracker), whatever order they answer in.
///
/// THE READ BOUND: lookup `i` reads at most what is left of
/// [`FANOUT_MAX_OUTPUTS_READ`] after the lookups asked before it, exactly as
/// if every answer were read one after another in the asked order. Each
/// hand-in trims every answer held to that share of the answers held so far
/// (a share an earlier answer arriving later can only lower), so the adverts
/// held never pass the read bound, at any instant; and once the answers held
/// have read the whole bound ([`LookupFold::spent`]) no lookup is started:
/// every lookup not yet started is asked after all of them, so its share is
/// nothing, and the plan is the same as if it had answered.
#[derive(Debug, Clone)]
pub struct LookupFold {
    lookups: Vec<Lookup>,
}

/// The lookups' outcome, for the plan and the counters.
#[derive(Debug, Clone)]
pub struct LookupsRead {
    /// One queue of adverts per tracker, its answers in topic order.
    pub queues: Vec<Vec<Advert>>,
    /// The reads' drops (refused, over the read bounds).
    pub drops: FanoutDrops,
    /// The answers that did not read: `(asked index, error)`.
    pub faults: Vec<(usize, String)>,
    /// Lookups started and cut by their deadline or the phase's.
    pub cut: u64,
    /// Lookups never started.
    pub untried: u64,
}

impl LookupFold {
    /// `asked` lookups, none started.
    pub fn new(asked: usize) -> Self {
        Self {
            lookups: vec![Lookup::Untried; asked],
        }
    }

    /// Lookup `i` is started.
    pub fn started(&mut self, i: usize) {
        if let Some(l) = self.lookups.get_mut(i) {
            *l = Lookup::Cut;
        }
    }

    /// Lookup `i` answered: its read (or its fault) is kept, and every answer
    /// held is trimmed to its share of the read bound.
    pub fn hand_in(&mut self, i: usize, answer: Result<AnswerRead, String>) {
        if let Some(l) = self.lookups.get_mut(i) {
            *l = match answer {
                Ok(read) => Lookup::Read(read),
                Err(e) => Lookup::Fault(e),
            };
        }
        let mut left = FANOUT_MAX_OUTPUTS_READ;
        for l in &mut self.lookups {
            if let Lookup::Read(r) = l {
                let share = r.read.min(left);
                left -= share;
                r.adverts.retain(|(pos, _)| *pos < share);
            }
        }
    }

    /// The outputs the answers held have read (before their shares).
    pub fn reads_held(&self) -> usize {
        self.lookups
            .iter()
            .map(|l| match l {
                Lookup::Read(r) => r.read,
                _ => 0,
            })
            .sum()
    }

    /// The read bound is spent by the answers held: no further lookup is
    /// started.
    pub fn spent(&self) -> bool {
        self.reads_held() >= FANOUT_MAX_OUTPUTS_READ
    }

    /// The answers held that were asked before lookup `i` have read the whole
    /// bound: lookup `i`'s share is nothing, so it is not started. (With the
    /// lookups started in the asked order every answer held was asked before
    /// the next, and this is [`LookupFold::spent`]; it is asked per lookup so
    /// the plan never rests on the order a pool polls in.)
    pub fn spent_before(&self, i: usize) -> bool {
        self.lookups
            .iter()
            .take(i)
            .map(|l| match l {
                Lookup::Read(r) => r.read,
                _ => 0,
            })
            .sum::<usize>()
            >= FANOUT_MAX_OUTPUTS_READ
    }

    /// The adverts held now (never more than [`FANOUT_MAX_OUTPUTS_READ`]).
    pub fn adverts_held(&self) -> usize {
        self.lookups
            .iter()
            .map(|l| match l {
                Lookup::Read(r) => r.adverts.len(),
                _ => 0,
            })
            .sum()
    }

    /// The lookups' outcome: `tracker_of(i)` is lookup `i`'s tracker, of
    /// `trackers`. The queues are filled in the asked order.
    pub fn finish(self, tracker_of: impl Fn(usize) -> usize, trackers: usize) -> LookupsRead {
        let mut out = LookupsRead {
            queues: vec![Vec::new(); trackers],
            drops: FanoutDrops::default(),
            faults: Vec::new(),
            cut: 0,
            untried: 0,
        };
        let mut left = FANOUT_MAX_OUTPUTS_READ;
        for (i, l) in self.lookups.into_iter().enumerate() {
            match l {
                Lookup::Untried => out.untried += 1,
                Lookup::Cut => out.cut += 1,
                Lookup::Fault(e) => out.faults.push((i, e)),
                Lookup::Read(r) => {
                    let share = r.read.min(left);
                    left -= share;
                    let adverts: Vec<Advert> = r
                        .adverts
                        .into_iter()
                        .filter(|(pos, _)| *pos < share)
                        .map(|(_, a)| a)
                        .collect();
                    out.drops.outputs_over_bound += (r.outputs - share) as u64;
                    out.drops.adverts_refused += (share - adverts.len()) as u64;
                    if let Some(queue) = out.queues.get_mut(tracker_of(i)) {
                        queue.extend(adverts);
                    }
                }
            }
        }
        out
    }
}

/// PURE: fold answer texts, `(tracker index, topic, answer text)` in the order
/// asked, as the lookups would hand them in ([`adverts_of_answer`],
/// [`LookupFold`]): one queue of adverts per tracker and the reads' drops.
/// Answers that do not read are returned as `(tracker index, error)`.
#[allow(clippy::type_complexity)]
pub fn read_answers<'a>(
    answers: impl IntoIterator<Item = (usize, &'a str, &'a str)>,
    trackers: usize,
) -> (Vec<Vec<Advert>>, FanoutDrops, Vec<(usize, String)>) {
    let answers: Vec<(usize, &str, &str)> = answers.into_iter().collect();
    let mut fold = LookupFold::new(answers.len());
    for (i, (_, topic, text)) in answers.iter().enumerate() {
        fold.started(i);
        fold.hand_in(i, adverts_of_answer(text, topic));
    }
    let read = fold.finish(|i| answers[i].0, trackers);
    let faults = read
        .faults
        .into_iter()
        .map(|(i, e)| (answers[i].0, e))
        .collect();
    (read.queues, read.drops, faults)
}

/// A verdict's answer: admitted or not, and whether it spent a check (a
/// remembered verdict does not).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Verdict {
    /// [`advert_is_admitted`]'s answer.
    pub admitted: bool,
    /// A signature check was made for it.
    pub checked: bool,
}

/// What [`plan_fanout`] chose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    /// The POST targets, in the order placed.
    pub targets: Vec<String>,
    /// The fan-out's drops, the reads' included.
    pub drops: FanoutDrops,
    /// Signature checks made.
    pub checks: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    Unknown,
    Admitted,
    Refused,
}

/// PURE: choose the POST targets from each tracker's queue of adverts.
///
/// THE FILL: one place per origin (our own never one), at most
/// [`FANOUT_MAX_HOSTS`]. Places are given in LEVELS of the identity key
/// (level `n` places only adverts whose key holds fewer than `n` places), and
/// inside a level in ROUNDS across the trackers (each tracker's turn places
/// its next eligible advert, and spends at most ONE signature check on the
/// way: a refusal ends the turn). So no
/// key takes a second place while an advert of another key waits for its
/// first, and no tracker takes a second place in a round while another has
/// an eligible advert. An origin's place moves to its default-port spelling
/// when an admitted advert names one (a port spelling does not displace the
/// host's own advert). Each advert's verdict is asked of `verify` at most
/// once (by outpoint), only when it would take a place. `verify(advert,
/// may_check)` answers a REMEMBERED verdict whatever `may_check` says (it
/// costs nothing), and makes a check only while `may_check`
/// (fewer than [`FANOUT_MAX_VERIFICATIONS`] made); `None` is "not known and
/// not checked". No check is made for a new place once the places are full;
/// the default-port pass may still check, inside the same budget.
///
/// What a stranger can still take, stated: k identity keys whose adverts EVERY
/// tracker serves ahead of the honest ones take up to k places (32 with 32
/// keys), as any k honest operators would; behind ONE permissive tracker
/// alone they take one place, and spend one check, a round of the four, so
/// the other three trackers' honest origins are all placed while there are
/// at most 24 of them, and adverts that fail the check spend at most a
/// quarter of the checks while the other trackers have adverts to check.
/// Adverts that fail the check and that EVERY tracker serves ahead of the
/// honest ones can spend the whole check budget on a cold isolate; each is
/// refused once per isolate (the verdict is remembered), and an honest
/// advert whose admission is remembered is placed past a spent budget.
pub fn plan_fanout(
    queues: Vec<Vec<Advert>>,
    self_host: Option<&str>,
    mut drops: FanoutDrops,
    mut verify: impl FnMut(&Advert, bool) -> Option<Verdict>,
) -> Plan {
    let ours = self_host.map(overlay_engine::gasp::peer_origin);
    let queues: Vec<Vec<Advert>> = queues
        .into_iter()
        .map(|q| {
            q.into_iter()
                .filter(|a| ours.as_deref() != Some(a.target.origin.as_str()))
                .collect()
        })
        .collect();
    let mut status: HashMap<(String, u32), Status> = HashMap::new();
    let mut placed: HashMap<String, usize> = HashMap::new();
    let mut targets: Vec<PostTarget> = Vec::new();
    let mut key_places: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut checks = 0usize;
    let mut unchecked: HashSet<(String, u32)> = HashSet::new();

    // An advert's verdict once (by outpoint): `(verdict, a check was spent)`,
    // the verdict `None` when it is not remembered and the budget is spent.
    // The memory is asked FIRST, the budget only gates a check.
    let mut judge = |a: &Advert,
                     status: &mut HashMap<(String, u32), Status>,
                     checks: &mut usize,
                     unchecked: &mut HashSet<(String, u32)>|
     -> (Option<bool>, bool) {
        match status.get(&a.outpoint).copied().unwrap_or(Status::Unknown) {
            Status::Admitted => (Some(true), false),
            Status::Refused => (Some(false), false),
            Status::Unknown => {
                let may_check = *checks < FANOUT_MAX_VERIFICATIONS;
                let Some(v) = verify(a, may_check) else {
                    unchecked.insert(a.outpoint.clone());
                    return (None, false);
                };
                if v.checked {
                    *checks += 1;
                }
                let s = if v.admitted {
                    Status::Admitted
                } else {
                    Status::Refused
                };
                status.insert(a.outpoint.clone(), s);
                (Some(v.admitted), v.checked)
            }
        }
    };

    let mut level = 1usize;
    while targets.len() < FANOUT_MAX_HOSTS {
        let mut capped = false;
        let mut cursors = vec![0usize; queues.len()];
        loop {
            let mut progress = false;
            for (t, queue) in queues.iter().enumerate() {
                if targets.len() >= FANOUT_MAX_HOSTS {
                    break;
                }
                // A turn spends at most ONE check: an advert that would need
                // a second waits for the tracker's next turn.
                let mut checked_this_turn = false;
                while let Some(a) = queue.get(cursors[t]) {
                    if placed.contains_key(&a.target.origin)
                        || status.get(&a.outpoint) == Some(&Status::Refused)
                    {
                        cursors[t] += 1;
                        continue;
                    }
                    if key_places.get(&a.identity_key).copied().unwrap_or(0) >= level {
                        capped = true;
                        cursors[t] += 1;
                        continue;
                    }
                    if checked_this_turn && !status.contains_key(&a.outpoint) {
                        break;
                    }
                    cursors[t] += 1;
                    let (verdict, spent) = judge(a, &mut status, &mut checks, &mut unchecked);
                    checked_this_turn |= spent;
                    progress |= spent;
                    if verdict == Some(true) {
                        placed.insert(a.target.origin.clone(), targets.len());
                        targets.push(a.target.clone());
                        *key_places.entry(a.identity_key.clone()).or_default() += 1;
                        progress = true;
                        break;
                    }
                }
            }
            if !progress || targets.len() >= FANOUT_MAX_HOSTS {
                break;
            }
        }
        if !capped {
            break;
        }
        level += 1;
    }

    // A placed origin with an explicit port moves to its default-port
    // spelling where an admitted advert names one.
    for a in queues.iter().flatten() {
        if !a.target.default_port {
            continue;
        }
        let Some(&i) = placed.get(&a.target.origin) else {
            continue;
        };
        if targets[i].default_port {
            continue;
        }
        if judge(a, &mut status, &mut checks, &mut unchecked).0 == Some(true) {
            targets[i] = a.target.clone();
        }
    }

    if targets.len() >= FANOUT_MAX_HOSTS {
        let left: HashSet<&str> = queues
            .iter()
            .flatten()
            .filter(|a| {
                !placed.contains_key(&a.target.origin)
                    && status.get(&a.outpoint) != Some(&Status::Refused)
            })
            .map(|a| a.target.origin.as_str())
            .collect();
        drops.hosts_over_bound = left.len() as u64;
    }
    let refused: HashSet<&(String, u32)> = status
        .iter()
        .filter(|(_, s)| **s == Status::Refused)
        .map(|(k, _)| k)
        .collect();
    drops.adverts_refused += queues
        .iter()
        .flatten()
        .filter(|a| refused.contains(&a.outpoint))
        .count() as u64;
    unchecked.retain(|o| !status.contains_key(o));
    drops.verifications_over_bound = unchecked.len() as u64;
    Plan {
        targets: targets.into_iter().map(|t| t.target).collect(),
        drops,
        checks,
    }
}

/// The verdicts an isolate remembers, by advert outpoint `(txid, vout)`.
#[derive(Debug)]
pub struct VerdictMemo {
    map: HashMap<(String, u32), bool>,
    cap: usize,
}

impl VerdictMemo {
    /// An empty memory of at most `cap` entries.
    pub fn new(cap: usize) -> Self {
        Self {
            map: HashMap::new(),
            cap,
        }
    }

    /// The remembered verdict of an outpoint.
    pub fn get(&self, outpoint: &(String, u32)) -> Option<bool> {
        self.map.get(outpoint).copied()
    }

    /// The entries held (never more than the cap).
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether nothing is remembered.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Remember a verdict. THE EVICTION RULE: under the cap an entry is added;
    /// at the cap every REFUSED entry is dropped first; a memory still full
    /// (every entry an admission) is cleared whole only for a new ADMISSION,
    /// and a new refusal is then not remembered. So a refusal never evicts an
    /// admission, and the memory never holds more than its cap.
    pub fn remember(&mut self, outpoint: (String, u32), admitted: bool) {
        if self.map.len() >= self.cap && !self.map.contains_key(&outpoint) {
            self.map.retain(|_, a| *a);
            if self.map.len() >= self.cap {
                if !admitted {
                    return;
                }
                self.map.clear();
            }
        }
        self.map.insert(outpoint, admitted);
    }
}

thread_local! {
    static VERDICTS: RefCell<VerdictMemo> = RefCell::new(VerdictMemo::new(FANOUT_VERDICT_MEMO_MAX));
}

/// PURE: an advert's verdict through `memo`: a remembered verdict first (no
/// check spent, whatever `may_check`), else `check` while `may_check` (the
/// verdict remembered), else `None`.
pub fn recall_or_check(
    memo: &mut VerdictMemo,
    advert: &Advert,
    may_check: bool,
    check: impl FnOnce(&Advert) -> bool,
) -> Option<Verdict> {
    if let Some(admitted) = memo.get(&advert.outpoint) {
        return Some(Verdict {
            admitted,
            checked: false,
        });
    }
    if !may_check {
        return None;
    }
    let admitted = check(advert);
    memo.remember(advert.outpoint.clone(), admitted);
    Some(Verdict {
        admitted,
        checked: true,
    })
}

/// The shipped verdict: [`recall_or_check`] over the isolate's memory
/// ([`FANOUT_VERDICT_MEMO_MAX`] entries) with [`advert_is_admitted`].
fn remembered_verdict(advert: &Advert, may_check: bool) -> Option<Verdict> {
    VERDICTS.with(|m| recall_or_check(&mut m.borrow_mut(), advert, may_check, advert_is_admitted))
}

/// PURE over its futures: make the lookups in the asked order, at most
/// `concurrency` in flight at once (a lookup is made, `make()`, only when a
/// slot is free and only while its share of the read bound is not spent,
/// [`LookupFold::spent_before`]: checked when it is taken from the queue and
/// again when it starts), each raced against its own deadline
/// (`each_deadline()`, called when it starts), the whole raced against
/// `phase_deadline`. Each lookup's answer is handed to `fold` the moment it
/// answers ([`LookupFold::hand_in`]); a lookup in flight when the phase ends
/// is dropped (its request aborted, [`AbortOnDrop`]) and stays cut, one never
/// started untried.
pub async fn lookups_within<G, F, D, P>(
    lookups: Vec<G>,
    concurrency: usize,
    fold: &RefCell<LookupFold>,
    mut each_deadline: impl FnMut() -> D,
    phase_deadline: P,
) where
    G: FnOnce() -> F,
    F: Future<Output = Result<AnswerRead, String>>,
    D: Future<Output = ()>,
    P: Future<Output = ()>,
{
    use futures_util::future::{ready, select, Either};
    use futures_util::StreamExt;
    let pool = futures_util::stream::iter(lookups.into_iter().enumerate())
        .take_while(|(i, _)| ready(!fold.borrow().spent_before(*i)))
        .map(|(i, make)| {
            let cut = each_deadline();
            async move {
                if fold.borrow().spent_before(i) {
                    return;
                }
                fold.borrow_mut().started(i);
                let request = std::pin::pin!(make());
                let cut = std::pin::pin!(cut);
                if let Either::Left((answer, _)) = select(request, cut).await {
                    fold.borrow_mut().hand_in(i, answer);
                }
            }
        })
        .buffer_unordered(concurrency.max(1))
        .for_each(|()| async {});
    let pool = std::pin::pin!(pool);
    let phase_deadline = std::pin::pin!(phase_deadline);
    select(pool, phase_deadline).await;
}

/// PURE: how many POSTs of a `body_bytes` BEEF are in flight at once:
/// `max(1, FANOUT_POST_BYTES_IN_FLIGHT / body_bytes)`, at most
/// [`FANOUT_POST_CONNECTIONS`].
pub fn post_concurrency(body_bytes: usize) -> usize {
    (FANOUT_POST_BYTES_IN_FLIGHT / body_bytes.max(1)).clamp(1, FANOUT_POST_CONNECTIONS)
}

/// What became of one request of [`within_budget`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sent<T> {
    /// It answered before its deadline.
    Answered(T),
    /// It was started and a deadline (its own, or the phase's) came first.
    Cut,
    /// The phase ended before it was started.
    Untried,
}

/// PURE over its futures: start the requests in order, at most `concurrency`
/// in flight at once (a request is made, `make()`, only when a slot is free,
/// so at most `concurrency` bodies are held), each raced against its own
/// deadline (`each_deadline()`, called when it starts), the whole raced
/// against `phase_deadline`. Answers each request's fate in the requests'
/// order: a request started when the phase ended is [`Sent::Cut`], one never
/// started [`Sent::Untried`].
pub async fn within_budget<T, G, F, D, P>(
    requests: Vec<G>,
    concurrency: usize,
    mut each_deadline: impl FnMut() -> D,
    phase_deadline: P,
) -> Vec<Sent<T>>
where
    G: FnOnce() -> F,
    F: Future<Output = T>,
    D: Future<Output = ()>,
    P: Future<Output = ()>,
{
    use futures_util::future::{select, Either};
    use futures_util::StreamExt;
    let fates: RefCell<Vec<Sent<T>>> =
        RefCell::new((0..requests.len()).map(|_| Sent::Untried).collect());
    {
        let fates = &fates;
        let pool = futures_util::stream::iter(requests.into_iter().enumerate())
            .map(|(i, make)| {
                let cut = each_deadline();
                async move {
                    fates.borrow_mut()[i] = Sent::Cut;
                    let request = std::pin::pin!(make());
                    let cut = std::pin::pin!(cut);
                    if let Either::Left((answer, _)) = select(request, cut).await {
                        fates.borrow_mut()[i] = Sent::Answered(answer);
                    }
                }
            })
            .buffer_unordered(concurrency.max(1))
            .for_each(|()| async {});
        let pool = std::pin::pin!(pool);
        let phase_deadline = std::pin::pin!(phase_deadline);
        select(pool, phase_deadline).await;
    }
    fates.into_inner()
}

/// What can abort a request in flight: [`worker::AbortController`] on the
/// platform, a counting fake in the pins.
pub trait Abort {
    /// Abort the request this controls (its fetch, its body, its connection).
    fn abort(self);
}

impl Abort for AbortController {
    fn abort(self) {
        AbortController::abort(self)
    }
}

/// Holds a request's abort controller and aborts it when dropped: the
/// request's future dropped by a deadline, by the phase's end or by any other
/// drop takes its fetch with it, where dropping the future alone would leave
/// the fetch running with its body and its connection. Dropped once a request
/// is done with its response, it releases that response unread.
pub struct AbortOnDrop<A: Abort>(Option<A>);

impl<A: Abort> AbortOnDrop<A> {
    /// Guard `controller`.
    pub fn new(controller: A) -> Self {
        Self(Some(controller))
    }
}

impl<A: Abort> Drop for AbortOnDrop<A> {
    fn drop(&mut self) {
        if let Some(controller) = self.0.take() {
            controller.abort();
        }
    }
}

/// PURE: the POSTs cut and the targets left untried, of [`within_budget`]'s
/// fates.
pub fn posts_cut_and_untried<T>(fates: &[Sent<T>]) -> (u64, u64) {
    fates
        .iter()
        .fold((0, 0), |(cut, untried), fate| match fate {
            Sent::Answered(_) => (cut, untried),
            Sent::Cut => (cut + 1, untried),
            Sent::Untried => (cut, untried + 1),
        })
}

fn deadline_of(ms: u64) -> impl FnMut() -> Delay {
    move || Delay::from(std::time::Duration::from_millis(ms))
}

/// Fan out a tagged BEEF to the mainnet peers that run any of its topics,
/// counting nothing (the advertiser's ad sync; its drops are logged).
pub async fn fan_out(tagged: &TaggedBEEF, self_host: Option<&str>) {
    fan_out_planned(tagged, self_host).await;
}

/// Fan out a tagged BEEF (the `/submit` path) and count its drops in `db`, in
/// ONE batch written AFTER the POSTs (best effort; the caller runs this in
/// `wait_until`, so the submit's answer never waits on it).
pub async fn fan_out_counted(tagged: &TaggedBEEF, self_host: Option<&str>, db: D1Database) {
    let drops = fan_out_planned(tagged, self_host).await;
    crate::ops::bump_counters(&db, drops.deltas()).await;
}

/// Ask `ls_ship` at each `DEFAULT_SLAP_TRACKERS` for each topic, read each
/// answer inside its own request ([`adverts_of_answer`], the text dropped
/// there), fold the adverts in the asked order ([`LookupFold`]), plan
/// ([`plan_fanout`] with the remembered check) and POST to each target
/// ([`submit_url`]). The lookups go at most [`FANOUT_ASK_CONNECTIONS`] at once,
/// in the asked order, while the read bound is not spent, each under its own
/// deadline inside the phase's ([`lookups_within`]); the POSTs at most
/// [`post_concurrency`] at once under the phase's deadline ([`within_budget`]);
/// each request aborted when cut ([`AbortOnDrop`]). A request cut by its
/// deadline, a lookup or a target left untried, is counted
/// ([`LookupFold::finish`], [`posts_cut_and_untried`]). All errors are logged;
/// none propagate: the Engine's `submit()` has already admitted the
/// transaction, and this is a best-effort fan-out. Answers the drops.
async fn fan_out_planned(tagged: &TaggedBEEF, self_host: Option<&str>) -> FanoutDrops {
    if tagged.topics.is_empty() {
        return FanoutDrops::default();
    }

    let asked: Vec<(usize, &str)> = tagged
        .topics
        .iter()
        .flat_map(|topic| (0..DEFAULT_SLAP_TRACKERS.len()).map(move |t| (t, topic.as_str())))
        .collect();
    let fold = RefCell::new(LookupFold::new(asked.len()));
    lookups_within(
        asked
            .iter()
            .map(|&(t, topic)| move || ask_and_read(DEFAULT_SLAP_TRACKERS[t], topic))
            .collect(),
        FANOUT_ASK_CONNECTIONS,
        &fold,
        deadline_of(FANOUT_ASK_EACH_DEADLINE_MS),
        Delay::from(std::time::Duration::from_millis(FANOUT_ASK_DEADLINE_MS)),
    )
    .await;
    let read = fold
        .into_inner()
        .finish(|i| asked[i].0, DEFAULT_SLAP_TRACKERS.len());
    for (i, e) in &read.faults {
        let (t, topic) = asked[*i];
        worker::console_warn!(
            "mainnet fan-out: tracker {} ls_ship({}) failed: {}",
            DEFAULT_SLAP_TRACKERS[t],
            topic,
            e
        );
    }
    if read.cut + read.untried > 0 {
        worker::console_warn!(
            "mainnet fan-out: {} lookup(s) cut, {} untried ({} at once, {} ms each, {} ms phase)",
            read.cut,
            read.untried,
            FANOUT_ASK_CONNECTIONS,
            FANOUT_ASK_EACH_DEADLINE_MS,
            FANOUT_ASK_DEADLINE_MS
        );
    }
    let mut requests_cut = read.cut;
    let queues = read.queues;
    let read_drops = FanoutDrops {
        lookups_untried: read.untried,
        ..read.drops
    };
    let plan = plan_fanout(queues, self_host, read_drops, remembered_verdict);
    worker::console_log!(
        "mainnet fan-out: submitting topics={} to {} peer(s) (checks={} refused={} outputs_over_bound={} hosts_over_bound={} verifications_over_bound={})",
        tagged.topics.join(","),
        plan.targets.len(),
        plan.checks,
        plan.drops.adverts_refused,
        plan.drops.outputs_over_bound,
        plan.drops.hosts_over_bound,
        plan.drops.verifications_over_bound
    );

    let concurrency = post_concurrency(tagged.beef.len());
    let posted = within_budget(
        plan.targets
            .iter()
            .map(|target| move || post_beef_to_peer(target, tagged))
            .collect(),
        concurrency,
        deadline_of(FANOUT_POST_EACH_DEADLINE_MS),
        Delay::from(std::time::Duration::from_millis(FANOUT_POST_DEADLINE_MS)),
    )
    .await;
    let (posts_cut, posts_untried) = posts_cut_and_untried(&posted);
    requests_cut += posts_cut;
    for (target, answer) in plan.targets.iter().zip(posted) {
        match answer {
            Sent::Answered(Ok(())) => {
                worker::console_log!("mainnet fan-out: peer {} accepted", target)
            }
            Sent::Answered(Err(e)) => {
                worker::console_warn!("mainnet fan-out: peer {} rejected: {}", target, e)
            }
            Sent::Cut => {
                worker::console_warn!("mainnet fan-out: peer {} cut by its deadline", target)
            }
            Sent::Untried => {}
        }
    }
    if posts_untried > 0 {
        worker::console_warn!(
            "mainnet fan-out: {} peer(s) untried at {} ms ({} at once, BEEF {} bytes)",
            posts_untried,
            FANOUT_POST_DEADLINE_MS,
            concurrency,
            tagged.beef.len()
        );
    }
    FanoutDrops {
        requests_cut,
        posts_untried,
        ..plan.drops
    }
}

/// Ask a tracker and read its answer in the request ([`adverts_of_answer`]):
/// the answer's text lives only here and is dropped before the adverts are
/// handed back.
async fn ask_and_read(tracker: &str, topic: &str) -> Result<AnswerRead, String> {
    let text = ask_tracker(tracker, topic).await?;
    adverts_of_answer(&text, topic)
}

/// Ask a SLAP tracker's `ls_ship` for the peers running `topic`; answers the
/// answer's text, read whole, for [`ask_and_read`].
///
/// SHIP PushDrop layout per `bsv-rs/src/overlay/overlay_admin_token_template.rs`:
///
/// - `field[0]` = "SHIP" literal
/// - `field[1]` = host identity key (33-byte compressed pubkey)
/// - `field[2]` = host HTTPS URL (UTF-8)
/// - `field[3]` = topic name (UTF-8)
/// - `field[4]` = signature (ECDSA DER + sighash flag)
async fn ask_tracker(tracker: &str, topic: &str) -> Result<String, String> {
    let url = format!("{}/lookup", tracker.trim_end_matches('/'));
    let body = serde_json::json!({
        "service": LS_SHIP,
        "query": { "topics": [topic] }
    })
    .to_string();

    let headers = Headers::new();
    headers
        .set("Content-Type", "application/json")
        .map_err(|e| e.to_string())?;

    let mut init = RequestInit::new();
    init.with_method(Method::Post)
        .with_headers(headers)
        .with_body(Some(worker::wasm_bindgen::JsValue::from_str(&body)));

    let request = Request::new_with_init(&url, &init).map_err(|e| e.to_string())?;
    // Aborted if this future is dropped (its deadline), whatever it awaits.
    let controller = AbortController::default();
    let signal = controller.signal();
    let _abort = AbortOnDrop::new(controller);
    let mut response = Fetch::Request(request)
        .send_with_signal(&signal)
        .await
        .map_err(|e| e.to_string())?;

    let status = response.status_code();
    if !(200..300).contains(&status) {
        return Err(format!("HTTP {status}"));
    }
    response.text().await.map_err(|e| e.to_string())
}

/// POST a BEEF to a target's `/submit` ([`submit_url`]) with standard
/// SHIPBroadcaster headers (`Content-Type: application/octet-stream`,
/// `X-Topics: JSON array`). Doesn't parse the Steak body — fan-out targets we
/// don't control may return non-standard shapes; a 2xx status is enough.
async fn post_beef_to_peer(target: &str, tagged: &TaggedBEEF) -> Result<(), String> {
    let url = submit_url(target);
    let topics_header = serde_json::to_string(&tagged.topics).map_err(|e| e.to_string())?;

    let headers = Headers::new();
    headers
        .set("Content-Type", "application/octet-stream")
        .map_err(|e| e.to_string())?;
    headers
        .set("X-Topics", &topics_header)
        .map_err(|e| e.to_string())?;

    let body_js = js_sys::Uint8Array::from(tagged.beef.as_slice());
    let mut init = RequestInit::new();
    init.with_method(Method::Post)
        .with_headers(headers)
        .with_body(Some(worker::wasm_bindgen::JsValue::from(body_js)));

    let request = Request::new_with_init(&url, &init).map_err(|e| e.to_string())?;
    // Aborted if this future is dropped (its deadline, the phase's end).
    let controller = AbortController::default();
    let signal = controller.signal();
    let abort = AbortOnDrop::new(controller);
    let response = Fetch::Request(request)
        .send_with_signal(&signal)
        .await
        .map_err(|e| e.to_string())?;

    let status = response.status_code();
    // The status is all that is read: the response is released unread.
    drop(response);
    drop(abort);
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(format!("HTTP {status}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fanout_knob_defaults_on_and_turns_off_by_name() {
        assert!(enabled(None));
        assert!(enabled(Some("")));
        assert!(enabled(Some("true")));
        assert!(enabled(Some("yes")));
        for off in ["false", "0", "off", " FALSE ", "Off"] {
            assert!(!enabled(Some(off)), "{off:?} must turn the fan-out off");
        }
    }

    #[test]
    fn default_trackers_are_https() {
        // Guard: an accidental `http://` slip would silently fail in the
        // Worker runtime (mixed-content rejection). Cheap compile-time-ish
        // check.
        for t in DEFAULT_SLAP_TRACKERS {
            assert!(t.starts_with("https://"), "tracker {t} must be https");
        }
    }

    #[test]
    fn tracker_list_matches_bsv_rs_mainnet() {
        // Sanity: our list should match `bsv_rs::overlay::types::
        // NetworkPreset::Mainnet.slap_trackers()` byte-for-byte. If bsv-rs
        // rotates one, we must too — otherwise fresh clients discover
        // peers we don't know about.
        use bsv_rs::overlay::NetworkPreset;
        let canonical: Vec<String> = NetworkPreset::Mainnet
            .slap_trackers()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let ours: Vec<String> = DEFAULT_SLAP_TRACKERS
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(
            ours, canonical,
            "DEFAULT_SLAP_TRACKERS drifted from bsv_rs::overlay::NetworkPreset::Mainnet"
        );
    }

    // ---- FANOUT-0811: the fan-out holds an advertisement to the SHIP admission rule ----

    use bsv_rs::primitives::ec::PrivateKey;
    use bsv_rs::script::templates::PushDrop;
    use bsv_rs::script::{LockingScript, UnlockingScript};
    use bsv_rs::transaction::{Beef, Transaction, TransactionInput, TransactionOutput};
    use overlay_discovery::advertiser::WalletAdvertiser;
    use overlay_engine::types::Protocol;

    const TOPIC: &str = "tm_uhrp";

    fn input_of(src: &str, vout: u32) -> TransactionInput {
        TransactionInput {
            source_transaction: None,
            source_txid: Some(src.to_string()),
            source_output_index: vout,
            unlocking_script: Some(UnlockingScript::new()),
            unlocking_script_template: None,
            sequence: 0xffff_ffff,
        }
    }

    fn output_of(lock: LockingScript) -> TransactionOutput {
        TransactionOutput {
            satoshis: Some(1),
            locking_script: lock,
            change: false,
        }
    }

    /// One transaction carrying `lock` at output 0, its input a txid the BEEF
    /// does not carry, as a BEEF of that one transaction.
    fn beef_of_lock(lock: LockingScript) -> Vec<u8> {
        let mut tx = Transaction::new();
        tx.inputs.push(input_of(&"cd".repeat(32), 0));
        tx.outputs.push(output_of(lock));
        let mut b = Beef::new();
        b.merge_raw_tx(tx.to_binary(), None);
        b.to_binary()
    }

    /// The repository's own signed-advert writer (`WalletAdvertiser`).
    fn signed_lock(uri: &str, topic: &str) -> LockingScript {
        let key = PrivateKey::random();
        WalletAdvertiser::new(&key, uri)
            .unwrap()
            .build_ad_script(Protocol::Ship, topic)
            .unwrap()
    }

    /// The tracker answer's JSON over `(beef, outputIndex)` pairs.
    fn answer_of(outputs: &[(Vec<u8>, u32)]) -> String {
        serde_json::json!({
            "type": "output-list",
            "outputs": outputs
                .iter()
                .map(|(beef, i)| serde_json::json!({ "beef": beef, "outputIndex": i }))
                .collect::<Vec<_>>(),
        })
        .to_string()
    }

    /// A PushDrop with a junk signature, locked by a key unrelated to the
    /// identity key it names (the proof lane's case).
    fn junk_lock(tag: &[u8], uri: &str, topic: &str) -> LockingScript {
        let locking = PrivateKey::random().public_key();
        let identity = PrivateKey::random().public_key();
        PushDrop::new(
            locking,
            vec![
                tag.to_vec(),
                identity.to_compressed().to_vec(),
                uri.as_bytes().to_vec(),
                topic.as_bytes().to_vec(),
                vec![0x30, 0x06, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01],
            ],
        )
        .lock()
    }

    /// An admitted advert for `uri` by `key` (bsv-rs's own signed-token writer,
    /// which takes any URI; the repository's writer refuses some spellings).
    fn advert_by(key: &PrivateKey, uri: &str) -> LockingScript {
        bsv_rs::overlay::create_signed_overlay_admin_token(key, Protocol::Ship, uri, TOPIC)
    }

    /// The shipped reading and plan over `(tracker, answer text)`, the check
    /// the real one (unremembered).
    fn shipped_plan(answers: &[(usize, String)], self_host: Option<&str>) -> Plan {
        let (queues, drops, faults) = read_answers(
            answers.iter().map(|(t, text)| (*t, TOPIC, text.as_str())),
            DEFAULT_SLAP_TRACKERS.len(),
        );
        assert!(faults.is_empty(), "{faults:?}");
        plan_fanout(queues, self_host, drops, |a, may| {
            may.then(|| Verdict {
                admitted: advert_is_admitted(a),
                checked: true,
            })
        })
    }

    /// An advert as read, for the fill's pins: `uri`'s target, a claimed key
    /// of 33 `key` bytes, outpoint `n`.
    fn fake(uri: &str, key: u8, n: usize) -> Advert {
        Advert {
            target: post_target(uri).unwrap(),
            identity_key: vec![key; 33],
            outpoint: (format!("{n:064x}"), 0),
            output: output_of(LockingScript::new()),
        }
    }

    fn admit_all(_: &Advert, may_check: bool) -> Option<Verdict> {
        may_check.then_some(Verdict {
            admitted: true,
            checked: true,
        })
    }

    #[test]
    fn fanout0811_a_junk_signature_and_unrelated_key_names_no_host() {
        for tag in [&b"SHIP"[..], &b"XXXX"[..]] {
            let beef = beef_of_lock(junk_lock(tag, "https://stranger.example", TOPIC));
            assert_eq!(
                advert_host(&beef, 0, TOPIC),
                None,
                "an unsigned advert (field0 {:?}) names no host",
                String::from_utf8_lossy(tag)
            );
            let plan = shipped_plan(&[(0, answer_of(&[(beef, 0)]))], None);
            assert!(plan.targets.is_empty());
            assert_eq!(plan.drops.adverts_refused, 1);
        }
    }

    #[test]
    fn fanout0811_wrong_type_wrong_topic_and_unadvertisable_uris_name_no_host() {
        // A token signed by its identity's linked key, the type field not SHIP.
        let key = PrivateKey::random();
        let slap = WalletAdvertiser::new(&key, "https://peer.example")
            .unwrap()
            .build_ad_script(Protocol::Slap, "ls_uhrp")
            .unwrap();
        assert_eq!(
            advert_host(&beef_of_lock(slap), 0, TOPIC),
            None,
            "a SLAP token"
        );
        // A valid SHIP advert for another topic than the one asked.
        let other = beef_of_lock(signed_lock("https://peer.example", "tm_other"));
        assert_eq!(advert_host(&other, 0, TOPIC), None, "another topic");
        assert_eq!(
            advert_host(&other, 0, "tm_other").as_deref(),
            Some("https://peer.example"),
            "the control: the same advert names its host for its own topic"
        );
        // URIs the engine would not advertise, the signature valid over them.
        for uri in [
            "http://peer.example",
            "httpfoo",
            "https://localhost",
            "https://peer.example/path",
        ] {
            let lock = advert_by(&PrivateKey::random(), uri);
            assert_eq!(
                advert_host(&beef_of_lock(lock), 0, TOPIC),
                None,
                "{uri} is not advertisable"
            );
        }
    }

    #[test]
    fn fanout0811_the_repositorys_own_signed_advert_names_its_host() {
        let beef = beef_of_lock(signed_lock("https://peer.example/", TOPIC));
        assert_eq!(
            advert_host(&beef, 0, TOPIC).as_deref(),
            Some("https://peer.example")
        );
        let plan = shipped_plan(&[(0, answer_of(&[(beef, 0)]))], None);
        assert_eq!(plan.targets, vec!["https://peer.example".to_string()]);
        assert_eq!(plan.drops, FanoutDrops::default());
        // An output index past the transaction's outputs names none.
        let beef = beef_of_lock(signed_lock("https://peer.example", TOPIC));
        assert_eq!(advert_host(&beef, 1, TOPIC), None);
        // A BEEF that does not parse names none.
        assert_eq!(advert_host(b"garbage", 0, TOPIC), None);
    }

    /// Lane NAMES: the fan-out's admission is the SHIP topic manager's, whose
    /// name rule is the reference's at
    /// `57c5f13055b555b83c1ef368fff3650abd21b6ef`
    /// (`/^(?=.{1,256}$)(?:tm_|ls_)[a-z0-9]+(?:_[a-z0-9]+)*$/`): a host
    /// advertising a topic whose name carries digits, or is 200 bytes long,
    /// receives the fan-out; one advertising a 257-byte topic does not.
    #[test]
    fn fanout0811_names_a_digit_topic_and_a_200_byte_topic_name_their_host() {
        let per_asset = format!("tm_color_{}_0", "0123456789abcdef".repeat(4));
        let long = format!("tm_{}", "a1".repeat(98) + "z");
        assert_eq!(long.len(), 200);
        let at_bound = format!("tm_{}", "a".repeat(253));
        for topic in [per_asset.as_str(), "tm_uhrp_files2", &long, &at_bound] {
            let key = PrivateKey::random();
            let lock = bsv_rs::overlay::create_signed_overlay_admin_token(
                &key,
                Protocol::Ship,
                "https://peer.example/",
                topic,
            );
            assert!(
                matches!(
                    SHIPTopicManager::admissible_advert(&output_of(lock.clone())),
                    Ok(Some(_))
                ),
                "{topic}: the SHIP admission admits it"
            );
            assert_eq!(
                advert_host(&beef_of_lock(lock), 0, topic).as_deref(),
                Some("https://peer.example"),
                "{topic}"
            );
            // The repository's own writer builds the same advertisement.
            let beef = beef_of_lock(signed_lock("https://peer.example/", topic));
            assert_eq!(
                advert_host(&beef, 0, topic).as_deref(),
                Some("https://peer.example"),
                "{topic}"
            );
        }
        let past = format!("tm_{}", "a".repeat(254));
        let lock = bsv_rs::overlay::create_signed_overlay_admin_token(
            &PrivateKey::random(),
            Protocol::Ship,
            "https://peer.example/",
            &past,
        );
        assert!(SHIPTopicManager::admissible_advert(&output_of(lock.clone())).is_err());
        assert_eq!(advert_host(&beef_of_lock(lock), 0, &past), None);
    }

    #[test]
    fn fanout0811_the_advert_is_read_from_the_beefs_subject_not_the_wire_last() {
        // The subject carries the valid advert at output 0 and spends the
        // parent; the parent carries a junk advert at output 0. Written with
        // the SUBJECT first, so the wire-last transaction is the parent.
        let mut parent = Transaction::new();
        parent.inputs.push(input_of(&"ab".repeat(32), 0));
        parent.outputs.push(output_of(junk_lock(
            b"SHIP",
            "https://wire-last.example",
            TOPIC,
        )));
        parent.outputs.push(output_of(LockingScript::new()));
        let parent_id = parent.id();
        let mut subject = Transaction::new();
        subject.inputs.push(input_of(&parent_id, 1));
        subject
            .outputs
            .push(output_of(signed_lock("https://subject.example", TOPIC)));
        // BEEF V1 written by hand (the SDK's writer sorts sources first):
        // version, no BUMPs, two transactions, each raw then "no BUMP".
        let mut beef = vec![0x01, 0x00, 0xbe, 0xef, 0x00, 0x02];
        for tx in [&subject, &parent] {
            beef.extend(tx.to_binary());
            beef.push(0x00);
        }
        let wire_last = Beef::from_binary(&beef)
            .unwrap()
            .txs
            .last()
            .map(|t| t.txid())
            .unwrap();
        assert_eq!(wire_last, parent_id, "the premise: the parent is wire-last");
        assert_eq!(
            advert_host(&beef, 0, TOPIC).as_deref(),
            Some("https://subject.example")
        );
    }

    #[test]
    fn fanout0811_more_valid_hosts_than_the_bound_post_to_the_bound_and_the_drop_is_counted() {
        // 37 origins of 37 keys, each served by two trackers, and our own
        // origin in another spelling.
        let hosts: Vec<String> = (0..FANOUT_MAX_HOSTS + 5)
            .map(|i| format!("https://peer{i}.example"))
            .collect();
        let queue: Vec<Advert> = hosts
            .iter()
            .enumerate()
            .map(|(i, h)| fake(h, i as u8, i))
            .chain([fake("https://US.example:443/", 200, 999)])
            .collect();
        let plan = plan_fanout(
            vec![queue.clone(), queue],
            Some("https://us.example"),
            FanoutDrops::default(),
            admit_all,
        );
        assert_eq!(plan.targets, hosts[..FANOUT_MAX_HOSTS].to_vec());
        assert_eq!(plan.drops.hosts_over_bound, 5);
        assert_eq!(
            plan.checks, FANOUT_MAX_HOSTS,
            "no check for a new place once the places are full"
        );
        assert!(!plan.targets.iter().any(|t| t.contains("us.example")));

        // The outputs of one answer past their read bound are not read, and
        // counted; and so are the answers past the fan-out's read bound.
        let outputs: Vec<(Vec<u8>, u32)> = (0..FANOUT_MAX_OUTPUTS_PER_ANSWER + 3)
            .map(|_| (b"x".to_vec(), 0))
            .collect();
        let text = answer_of(&outputs);
        let read = adverts_of_answer(&text, TOPIC).unwrap();
        assert_eq!(read.outputs - read.read, 3);
        assert_eq!(read.read, FANOUT_MAX_OUTPUTS_PER_ANSWER);
        assert!(read.adverts.is_empty(), "every output read is refused");
        let answers: Vec<(usize, &str, &str)> =
            (0..5).map(|t| (t % 4, TOPIC, text.as_str())).collect();
        let (_, drops, _) = read_answers(answers, 4);
        let per = FANOUT_MAX_OUTPUTS_PER_ANSWER + 3;
        assert_eq!(
            drops.outputs_over_bound,
            (5 * per - FANOUT_MAX_OUTPUTS_READ) as u64,
            "the fan-out reads at most FANOUT_MAX_OUTPUTS_READ"
        );

        let drops = FanoutDrops {
            adverts_refused: 1,
            outputs_over_bound: 3,
            hosts_over_bound: 5,
            verifications_over_bound: 7,
            requests_cut: 2,
            posts_untried: 4,
            lookups_untried: 6,
        };
        let deltas = drops.deltas();
        assert_eq!(deltas.get(COUNTER_HOSTS_OVER_BOUND), Some(&5));
        assert_eq!(deltas.get(COUNTER_OUTPUTS_OVER_BOUND), Some(&3));
        assert_eq!(deltas.get(COUNTER_ADVERTS_REFUSED), Some(&1));
        assert_eq!(deltas.get(COUNTER_VERIFICATIONS_OVER_BOUND), Some(&7));
        assert_eq!(deltas.get(COUNTER_REQUESTS_CUT), Some(&2));
        assert_eq!(deltas.get(COUNTER_POSTS_UNTRIED), Some(&4));
        assert_eq!(deltas.get(COUNTER_LOOKUPS_UNTRIED), Some(&6));
        assert!(FanoutDrops::default().deltas().is_empty());
    }

    /// The text of the body of `name` in `src` (to the first `\n}\n`).
    fn body_of<'a>(src: &'a str, name: &str) -> &'a str {
        let start = src.find(name).unwrap_or_else(|| panic!("{name} not found"));
        let rest = &src[start..];
        &rest[..rest.find("\n}\n").unwrap()]
    }

    #[test]
    fn fanout0811_the_shipped_loop_calls_the_pure_functions_and_the_counters_are_seeded() {
        let src = include_str!("mainnet_fanout.rs");
        let planned = body_of(src, "async fn fan_out_planned(");
        assert!(planned.contains("move || ask_and_read(DEFAULT_SLAP_TRACKERS[t], topic)"));
        assert!(
            planned.contains("RefCell::new(LookupFold::new(asked.len()))")
                && planned.contains(".finish(|i| asked[i].0, DEFAULT_SLAP_TRACKERS.len())"),
            "the answers are the pure fold"
        );
        assert!(
            planned.contains("plan_fanout(queues, self_host, read_drops, remembered_verdict)"),
            "the targets are the pure plan, under the remembered check"
        );
        assert!(planned.contains("post_beef_to_peer(target, tagged)"));
        assert!(
            !planned.contains("bump_counters"),
            "the counters are written by fan_out_counted, after the POSTs"
        );
        let ask = body_of(src, "async fn ask_tracker(");
        assert!(
            !ask.contains("PushDrop"),
            "the tracker read decodes nothing"
        );
        let reader = body_of(src, "pub fn read_advert(");
        assert!(reader.contains("subject::subject_txid_of(&mut parsed)"));
        assert!(
            reader.contains("beef_limits::subject_from_beef(beef, Some(&subject)"),
            "the advert is the subject read alone, nothing linked"
        );
        assert!(
            !reader.contains("transaction_from_beef"),
            "never the wire-last reader"
        );
        let admitted = body_of(src, "pub fn advert_is_admitted(");
        assert!(admitted.contains("SHIPTopicManager::admissible_advert(&advert.output)"));
        let remembered = body_of(src, "fn remembered_verdict(");
        assert!(remembered.contains(
            "recall_or_check(&mut m.borrow_mut(), advert, may_check, advert_is_admitted)"
        ));
        assert!(src.contains("RefCell::new(VerdictMemo::new(FANOUT_VERDICT_MEMO_MAX))"));
        let ops = include_str!("ops.rs");
        assert!(
            ops.contains("for name in crate::mainnet_fanout::COUNTERS {"),
            "seeded at 0"
        );
    }

    // ---- FANOUT-0811-FOLD: the lens's findings ----

    #[test]
    fn fanout0811_fold_m1_spellings_of_one_origin_take_one_place() {
        // 33 valid adverts of one origin in as many spellings, one key, then
        // an honest host: one place for the origin, one for the honest host.
        let stranger = PrivateKey::random();
        let mut spellings: Vec<String> = (0..28)
            .map(|i| format!("https://victim.example/?a{i}"))
            .collect();
        spellings.extend(
            [
                "https://victim.example/#x",
                "https://u:p@victim.example/",
                "https://VICTIM.example/",
                "https://victim.example./",
                "https://victim.example:443/",
            ]
            .map(str::to_string),
        );
        let mut outputs: Vec<(Vec<u8>, u32)> = spellings
            .iter()
            .map(|u| (beef_of_lock(advert_by(&stranger, u)), 0))
            .collect();
        outputs.push((
            beef_of_lock(signed_lock("https://honest.example", TOPIC)),
            0,
        ));
        let plan = shipped_plan(&[(0, answer_of(&outputs))], None);
        assert_eq!(
            plan.targets,
            vec![
                "https://victim.example".to_string(),
                "https://honest.example".to_string()
            ]
        );
        assert_eq!(plan.drops.adverts_refused, 0);
    }

    #[test]
    fn fanout0811_fold_m1_no_key_and_no_tracker_takes_every_place() {
        // (a) One key, 64 origins, served by every tracker ahead of three
        // honest operators: the key takes one place before any takes a second.
        let stranger: Vec<Advert> = (0..64)
            .map(|i| fake(&format!("https://s{i}.evil.example"), 1, i))
            .collect();
        let honest: Vec<Advert> = (0..3)
            .map(|i| fake(&format!("https://h{i}.example"), 10 + i as u8, 100 + i))
            .collect();
        let queue: Vec<Advert> = stranger.iter().chain(&honest).cloned().collect();
        let plan = plan_fanout(vec![queue; 4], None, FanoutDrops::default(), admit_all);
        assert_eq!(plan.targets.len(), FANOUT_MAX_HOSTS);
        for h in &honest {
            assert!(plan.targets.contains(&h.target.target), "{h:?} placed");
        }

        // (b) 40 keys behind ONE permissive tracker; the other three serve 24
        // honest origins: every honest origin is placed.
        let permissive: Vec<Advert> = (0..40)
            .map(|i| fake(&format!("https://k{i}.evil.example"), 50 + i as u8, 200 + i))
            .collect();
        let honest: Vec<Advert> = (0..24)
            .map(|i| fake(&format!("https://h{i}.example"), 120 + i as u8, 300 + i))
            .collect();
        let plan = plan_fanout(
            vec![permissive, honest.clone(), honest.clone(), honest.clone()],
            None,
            FanoutDrops::default(),
            admit_all,
        );
        for h in &honest {
            assert!(plan.targets.contains(&h.target.target), "{h:?} placed");
        }
        assert_eq!(plan.targets.len(), FANOUT_MAX_HOSTS);

        // (c) The same tracker's 200 adverts that fail the check spend one
        // check a turn: 30 honest origins on another tracker are all placed.
        let junk: Vec<Advert> = (0..200)
            .map(|i| fake(&format!("https://j{i}.evil.example"), 1, 400 + i))
            .collect();
        let honest: Vec<Advert> = (0..30)
            .map(|i| fake(&format!("https://h{i}.example"), 150 + i as u8, 700 + i))
            .collect();
        let plan = plan_fanout(
            vec![junk, honest.clone()],
            None,
            FanoutDrops::default(),
            |a: &Advert, may: bool| {
                may.then(|| Verdict {
                    admitted: !a.target.origin.ends_with("evil.example"),
                    checked: true,
                })
            },
        );
        assert_eq!(plan.targets.len(), 30);
        assert_eq!(plan.checks, FANOUT_MAX_VERIFICATIONS);
        // While the honest tracker had adverts the junk tracker spent one
        // check a round (30); the places not full, the rest of the budget
        // went to it after.
        assert_eq!(
            plan.drops.adverts_refused,
            (FANOUT_MAX_VERIFICATIONS - 30) as u64
        );
    }

    #[test]
    fn fanout0811_fold_m1_refused_and_duplicate_adverts_do_not_hide_a_valid_one() {
        // The lens's case: 64 valid adverts of one stranger ahead of an honest
        // one in ONE answer, then 120 refused and 40 duplicates ahead of
        // another honest one. Every honest advert is read and placed.
        let key = PrivateKey::random();
        let mut outputs: Vec<(Vec<u8>, u32)> = (0..64)
            .map(|i| {
                (
                    beef_of_lock(advert_by(&key, &format!("https://s{i}.evil.example"))),
                    0,
                )
            })
            .collect();
        outputs.push((beef_of_lock(signed_lock("https://first.example", TOPIC)), 0));
        let dup = beef_of_lock(advert_by(&key, "https://s0.evil.example"));
        outputs.extend((0..120).map(|_| (b"refused".to_vec(), 0)));
        outputs.extend((0..40).map(|_| (dup.clone(), 0)));
        outputs.push((
            beef_of_lock(signed_lock("https://second.example", TOPIC)),
            0,
        ));
        let plan = shipped_plan(&[(0, answer_of(&outputs))], None);
        for honest in ["https://first.example", "https://second.example"] {
            assert!(
                plan.targets.contains(&honest.to_string()),
                "{honest} placed"
            );
        }
        assert_eq!(plan.drops.outputs_over_bound, 0);
        assert_eq!(plan.drops.adverts_refused, 120);
    }

    #[test]
    fn fanout0811_fold_m1_a_port_spelling_does_not_displace_the_hosts_own_advert() {
        let queue = vec![
            fake("https://honest.example:8443/", 1, 1),
            fake("https://honest.example/", 2, 2),
        ];
        let plan = plan_fanout(vec![queue], None, FanoutDrops::default(), admit_all);
        assert_eq!(plan.targets, vec!["https://honest.example".to_string()]);
    }

    #[test]
    fn fanout0811_fold_m2_the_post_goes_to_the_canonical_submit() {
        for (uri, url) in [
            ("https://victim.example/?a", "https://victim.example/submit"),
            (
                "https://victim.example?a=1",
                "https://victim.example/submit",
            ),
            ("https://victim.example/#x", "https://victim.example/submit"),
            (
                "https://u:p@victim.example/",
                "https://victim.example/submit",
            ),
            ("https://VICTIM.example./", "https://victim.example/submit"),
            (
                "https://victim.example:443/",
                "https://victim.example/submit",
            ),
            (
                "https://victim.example:8443/",
                "https://victim.example:8443/submit",
            ),
            ("https://[::1]:9/", "https://[::1]:9/submit"),
        ] {
            let target = post_target(uri).unwrap_or_else(|| panic!("{uri}"));
            assert_eq!(submit_url(&target.target), url, "{uri}");
            assert!(!target.target.contains(['?', '#', '@']));
        }
        // Through the shipped reader too.
        let beef = beef_of_lock(advert_by(
            &PrivateKey::random(),
            "https://u:p@victim.example/?a#x",
        ));
        assert_eq!(
            advert_host(&beef, 0, TOPIC)
                .map(|t| submit_url(&t))
                .as_deref(),
            Some("https://victim.example/submit")
        );
        let post = body_of(
            include_str!("mainnet_fanout.rs"),
            "async fn post_beef_to_peer(",
        );
        assert!(post.contains("let url = submit_url(target);"));
    }

    #[test]
    fn fanout0811_fold_l2_other_advertisable_schemes_name_no_target() {
        // M5: the engine admits these; the POST does not speak them.
        for uri in [
            "wss://peer.example",
            "https+bsvauth://peer.example/",
            "https+rtt://peer.example/",
            "js8c+bsvauth+smf:?lat=40&long=-74&freq=7078&radius=1000",
        ] {
            let lock = advert_by(&PrivateKey::random(), uri);
            let output = output_of(lock.clone());
            assert!(
                matches!(SHIPTopicManager::admissible_advert(&output), Ok(Some(_))),
                "the premise: {uri} is admitted"
            );
            assert_eq!(advert_host(&beef_of_lock(lock), 0, TOPIC), None, "{uri}");
            assert_eq!(post_target(uri), None, "{uri}");
        }
    }

    #[test]
    fn fanout0811_fold_l2_the_fold_counts_every_answers_drops() {
        // M13: the drops of several answers add up, the plan's refusals too.
        let valid = beef_of_lock(signed_lock("https://peer.example", TOPIC));
        let junk = beef_of_lock(junk_lock(b"SHIP", "https://junk.example", TOPIC));
        let a = answer_of(&[(b"x".to_vec(), 0), (valid, 0), (junk, 0)]);
        let b = r#"{"type":"freeform","result":{}}"#.to_string();
        let c = "not json".to_string();
        let d = answer_of(
            &(0..FANOUT_MAX_OUTPUTS_PER_ANSWER + 44)
                .map(|_| (b"x".to_vec(), 0))
                .collect::<Vec<_>>(),
        );
        let (queues, drops, faults) = read_answers(
            [
                (0, TOPIC, a.as_str()),
                (1, TOPIC, b.as_str()),
                (2, TOPIC, c.as_str()),
                (3, TOPIC, d.as_str()),
            ],
            4,
        );
        assert_eq!(faults.len(), 1);
        assert_eq!(faults[0].0, 2);
        assert_eq!(
            drops.adverts_refused,
            1 + FANOUT_MAX_OUTPUTS_PER_ANSWER as u64
        );
        assert_eq!(drops.outputs_over_bound, 44);
        let plan = plan_fanout(queues, None, drops, |a, may| {
            may.then(|| Verdict {
                admitted: advert_is_admitted(a),
                checked: true,
            })
        });
        assert_eq!(plan.targets, vec!["https://peer.example".to_string()]);
        assert_eq!(
            plan.drops.adverts_refused,
            2 + FANOUT_MAX_OUTPUTS_PER_ANSWER as u64
        );
        assert_eq!(plan.drops.outputs_over_bound, 44);
    }

    #[test]
    fn fanout0811_fold_l1_the_counters_are_one_write_after_the_posts_on_the_db_the_route_has() {
        let src = include_str!("mainnet_fanout.rs");
        let counted = body_of(src, "pub async fn fan_out_counted(");
        let planned = counted
            .find("fan_out_planned(tagged, self_host).await")
            .unwrap();
        let bump = counted
            .find("bump_counters(&db, drops.deltas()).await")
            .unwrap();
        assert!(planned < bump, "the write follows the POSTs");
        // M15: the route's counted fan-out takes a database, not an Option.
        assert!(src.contains("pub async fn fan_out_counted(tagged: &TaggedBEEF, self_host: Option<&str>, db: D1Database)"));
        let routes = include_str!("routes.rs");
        assert!(routes.contains(
            "Ok(db) => crate::mainnet_fanout::fan_out_counted(&tagged_beef, host, db).await"
        ));
        assert!(routes.contains("let fanout_db = env.d1(\"OVERLAY_DB\");"));
        let advertiser = include_str!("advertiser.rs");
        assert!(advertiser
            .contains("crate::mainnet_fanout::fan_out(&tagged, Some(&self.hosting_url)).await"));
    }

    #[test]
    fn fanout0811_fold_l4_the_checks_are_bounded_and_remembered() {
        // 200 adverts that fail the check, each its own key, every tracker:
        // the budget is spent and what is left is counted.
        let junk: Vec<Advert> = (0..200)
            .map(|i| fake(&format!("https://j{i}.evil.example"), i as u8, i))
            .collect();
        let mut calls = 0usize;
        let plan = plan_fanout(
            vec![junk; 4],
            None,
            FanoutDrops::default(),
            |_: &Advert, may: bool| {
                may.then(|| {
                    calls += 1;
                    Verdict {
                        admitted: false,
                        checked: true,
                    }
                })
            },
        );
        assert_eq!(
            calls, FANOUT_MAX_VERIFICATIONS,
            "each outpoint once, at most the budget"
        );
        assert_eq!(plan.checks, FANOUT_MAX_VERIFICATIONS);
        assert_eq!(
            plan.drops.verifications_over_bound,
            (200 - FANOUT_MAX_VERIFICATIONS) as u64
        );
        assert!(plan.targets.is_empty());

        // A remembered verdict spends no check.
        let advert = read_advert(
            &beef_of_lock(signed_lock("https://peer.example", TOPIC)),
            0,
            TOPIC,
        )
        .unwrap();
        assert_eq!(
            remembered_verdict(&advert, true),
            Some(Verdict {
                admitted: true,
                checked: true
            })
        );
        assert_eq!(
            remembered_verdict(&advert, false),
            Some(Verdict {
                admitted: true,
                checked: false
            })
        );
        let memoised = plan_fanout(
            vec![vec![advert]],
            None,
            FanoutDrops::default(),
            remembered_verdict,
        );
        assert_eq!((memoised.targets.len(), memoised.checks), (1, 0));
    }

    // ---- FANOUT-0811-FOLD-2: the delta lens's findings ----

    #[test]
    fn fanout0811_fold2_l1_a_remembered_admission_is_placed_past_a_spent_budget() {
        // The lens's P1: 40 refused adverts ahead of an honest one on each of
        // the four trackers, the honest advert's admission remembered.
        let honest = fake("https://honest.example", 7, 9_999);
        let mut memo = VerdictMemo::new(FANOUT_VERDICT_MEMO_MAX);
        memo.remember(honest.outpoint.clone(), true);
        let queues: Vec<Vec<Advert>> = (0..4)
            .map(|t| {
                (0..40)
                    .map(|i| {
                        fake(
                            &format!("https://j{t}x{i}.evil.example"),
                            (t * 40 + i) as u8,
                            t * 40 + i,
                        )
                    })
                    .chain([honest.clone()])
                    .collect()
            })
            .collect();
        let plan = plan_fanout(queues, None, FanoutDrops::default(), |a, may| {
            recall_or_check(&mut memo, a, may, |a| {
                !a.target.origin.ends_with("evil.example")
            })
        });
        assert_eq!(plan.checks, FANOUT_MAX_VERIFICATIONS);
        assert_eq!(
            plan.targets,
            vec!["https://honest.example".to_string()],
            "a remembered admission is placed whatever the budget"
        );
        assert_eq!(
            plan.drops.verifications_over_bound,
            (160 - FANOUT_MAX_VERIFICATIONS) as u64
        );
    }

    #[test]
    fn fanout0811_fold2_l1_b_the_memory_is_bounded_and_a_refusal_never_evicts_an_admission() {
        let k = |n: u32| (format!("{n:064x}"), 0u32);
        let mut memo = VerdictMemo::new(4);
        memo.remember(k(1), true);
        memo.remember(k(2), true);
        memo.remember(k(3), false);
        memo.remember(k(4), false);
        // Full: a refusal drops the refusals, never the admissions.
        memo.remember(k(5), false);
        assert_eq!(memo.len(), 3);
        assert_eq!((memo.get(&k(1)), memo.get(&k(2))), (Some(true), Some(true)));
        assert_eq!((memo.get(&k(3)), memo.get(&k(5))), (None, Some(false)));
        // Full of admissions: a refusal is not remembered.
        memo.remember(k(6), true);
        memo.remember(k(7), true);
        memo.remember(k(8), false);
        assert_eq!(memo.len(), 4);
        assert_eq!(memo.get(&k(8)), None);
        assert!([1, 2, 6, 7].iter().all(|n| memo.get(&k(*n)) == Some(true)));
        // Full of admissions: an admission clears it, the bound held.
        memo.remember(k(9), true);
        assert_eq!(memo.len(), 1);
        assert_eq!(memo.get(&k(9)), Some(true));
        // The bound under any order of inserts.
        for n in 10..2_000u32 {
            memo.remember(k(n), n % 3 == 0);
            assert!(memo.len() <= 4, "at {n}: {}", memo.len());
        }
    }

    #[test]
    fn fanout0811_fold2_l3_the_memory_is_keyed_by_the_output_not_the_transaction() {
        // One transaction: a valid advert at output 0, a forged one at 1.
        let mut tx = Transaction::new();
        tx.inputs.push(input_of(&"cd".repeat(32), 0));
        tx.outputs
            .push(output_of(signed_lock("https://valid.example", TOPIC)));
        tx.outputs.push(output_of(junk_lock(
            b"SHIP",
            "https://forged.example",
            TOPIC,
        )));
        let mut b = Beef::new();
        b.merge_raw_tx(tx.to_binary(), None);
        let beef = b.to_binary();
        let valid = read_advert(&beef, 0, TOPIC).unwrap();
        let forged = read_advert(&beef, 1, TOPIC).unwrap();
        assert_eq!(valid.outpoint.0, forged.outpoint.0, "the premise: one txid");
        let mut memo = VerdictMemo::new(FANOUT_VERDICT_MEMO_MAX);
        assert_eq!(
            recall_or_check(&mut memo, &valid, true, advert_is_admitted),
            Some(Verdict {
                admitted: true,
                checked: true
            })
        );
        assert_eq!(
            recall_or_check(&mut memo, &forged, false, advert_is_admitted),
            None,
            "the valid output's verdict is not the forged output's"
        );
        assert_eq!(
            recall_or_check(&mut memo, &forged, true, advert_is_admitted),
            Some(Verdict {
                admitted: false,
                checked: true
            })
        );
    }

    #[test]
    fn fanout0811_fold2_l3_the_default_port_move_is_checked() {
        // The default-port spelling's advert fails the check: the place stays.
        let queue = vec![
            fake("https://honest.example:8443/", 1, 1),
            fake("https://honest.example/", 2, 2),
        ];
        let plan = plan_fanout(
            vec![queue],
            None,
            FanoutDrops::default(),
            |a: &Advert, may: bool| {
                may.then_some(Verdict {
                    admitted: !a.target.default_port,
                    checked: true,
                })
            },
        );
        assert_eq!(
            plan.targets,
            vec!["https://honest.example:8443".to_string()]
        );
        assert_eq!(plan.drops.adverts_refused, 1);
    }

    #[test]
    fn fanout0811_fold2_l3_the_round_robin_holds_under_remembered_verdicts() {
        // Every verdict from memory (no check spent): one tracker's 40 origins
        // of 40 keys do not take the places of another tracker's three.
        let permissive: Vec<Advert> = (0..40)
            .map(|i| fake(&format!("https://k{i}.evil.example"), i as u8, i))
            .collect();
        let honest: Vec<Advert> = (0..3)
            .map(|i| fake(&format!("https://h{i}.example"), 100 + i as u8, 100 + i))
            .collect();
        let plan = plan_fanout(
            vec![permissive, honest.clone()],
            None,
            FanoutDrops::default(),
            |_: &Advert, _: bool| {
                Some(Verdict {
                    admitted: true,
                    checked: false,
                })
            },
        );
        assert_eq!(plan.checks, 0);
        assert_eq!(plan.targets.len(), FANOUT_MAX_HOSTS);
        for h in &honest {
            assert!(plan.targets.contains(&h.target.target), "{h:?} placed");
        }
    }

    #[tokio::test]
    async fn fanout0811_fold2_l2_the_requests_are_made_together_each_under_its_deadline() {
        use std::cell::RefCell;
        use std::rc::Rc;
        // A tracker that never answers first, then two that answer: all three
        // are started before any answer is awaited (six at once), the tarpit
        // is cut by its own deadline, the others' answers are handed in.
        let started = Rc::new(RefCell::new(Vec::new()));
        let request = |i: usize, answer: Option<Result<AnswerRead, String>>| {
            let started = started.clone();
            move || -> std::pin::Pin<Box<dyn Future<Output = Result<AnswerRead, String>>>> {
                Box::pin(async move {
                    started.borrow_mut().push(i);
                    match answer {
                        Some(a) => a,
                        None => futures_util::future::pending().await,
                    }
                })
            }
        };
        let requests = vec![
            request(0, None),
            request(1, Some(Ok(AnswerRead::default()))),
            request(2, Some(Err("HTTP 500".into()))),
        ];
        // Each deadline, when it fires, notes how many requests had started.
        let fired = Rc::new(RefCell::new(Vec::new()));
        let mut deadlines = 0usize;
        let fold = RefCell::new(LookupFold::new(3));
        lookups_within(
            requests,
            FANOUT_ASK_CONNECTIONS,
            &fold,
            || {
                deadlines += 1;
                let (fired, started) = (fired.clone(), started.clone());
                async move {
                    tokio::task::yield_now().await;
                    fired.borrow_mut().push(started.borrow().len());
                }
            },
            futures_util::future::pending::<()>(),
        )
        .await;
        assert_eq!(deadlines, 3, "one deadline a request");
        assert_eq!(*started.borrow(), vec![0, 1, 2]);
        assert_eq!(
            fired.borrow()[0],
            3,
            "the tarpit is cut with every other request already started"
        );
        let read = fold.into_inner().finish(|i| i, 3);
        assert_eq!((read.cut, read.untried), (1, 0));
        assert_eq!(read.faults, vec![(2, "HTTP 500".to_string())]);

        // The shipped loop makes its lookups through the bounded pool and
        // its POSTs through theirs, then counts the cuts; the deadlines leave
        // the counter write inside 30 s.
        let src = include_str!("mainnet_fanout.rs");
        let planned = body_of(src, "async fn fan_out_planned(");
        assert!(planned.contains("deadline_of(FANOUT_ASK_EACH_DEADLINE_MS)"));
        assert!(planned.contains("from_millis(FANOUT_ASK_DEADLINE_MS)"));
        assert!(planned.contains("FANOUT_POST_DEADLINE_MS"));
        assert!(!planned.contains("within_deadline("));
        assert_eq!(planned.matches("lookups_within(").count(), 1);
        assert_eq!(planned.matches("within_budget(").count(), 1);
        assert!(planned.contains("let mut requests_cut = read.cut;"));
        const _: () = assert!(FANOUT_ASK_DEADLINE_MS + FANOUT_POST_DEADLINE_MS <= 20_000);
        const _: () = assert!(FANOUT_ASK_EACH_DEADLINE_MS < FANOUT_ASK_DEADLINE_MS);
    }

    /// Every request at once, each raced against its own deadline (every
    /// deadline started with the batch): the pins' "all at once" control.
    async fn within_deadline<T, F, D>(
        requests: Vec<F>,
        mut deadline: impl FnMut() -> D,
    ) -> Vec<Option<T>>
    where
        F: Future<Output = T>,
        D: Future<Output = ()>,
    {
        use futures_util::future::{join_all, select, Either};
        let raced: Vec<_> = requests
            .into_iter()
            .map(|request| {
                let cut = deadline();
                async move {
                    let request = std::pin::pin!(request);
                    let cut = std::pin::pin!(cut);
                    match select(request, cut).await {
                        Either::Left((answer, _)) => Some(answer),
                        Either::Right(_) => None,
                    }
                }
            })
            .collect();
        join_all(raced).await
    }

    /// `n` turns of the executor: the pins' clock (no timer runs natively).
    async fn turns(n: usize) {
        for _ in 0..n {
            tokio::task::yield_now().await;
        }
    }

    /// A boxed request maker, as the harness makes them.
    type Make = Box<dyn FnOnce() -> std::pin::Pin<Box<dyn Future<Output = ()>>>>;

    /// What the counting harness measures: the body bytes held by fetches
    /// that are still running, the most at once, and the aborts made.
    #[derive(Clone, Default)]
    struct Meter {
        held: std::rc::Rc<std::cell::Cell<usize>>,
        most: std::rc::Rc<std::cell::Cell<usize>>,
        aborts: std::rc::Rc<std::cell::Cell<usize>>,
    }

    /// A model fetch: holds its body until it ENDS, by its answer or by an
    /// abort, never by the drop of the future that awaited it (the platform's
    /// rule: a dropped future aborts nothing).
    #[derive(Clone)]
    struct ModelFetch {
        ended: std::rc::Rc<std::cell::Cell<bool>>,
        meter: Meter,
        body: usize,
    }

    impl ModelFetch {
        fn start(meter: &Meter, body: usize) -> Self {
            meter.held.set(meter.held.get() + body);
            meter.most.set(meter.most.get().max(meter.held.get()));
            Self {
                ended: Default::default(),
                meter: meter.clone(),
                body,
            }
        }
        fn end(&self) {
            if !self.ended.replace(true) {
                self.meter.held.set(self.meter.held.get() - self.body);
            }
        }
    }

    /// The model's abort controller: ends its fetch, counted.
    struct ModelAbort(ModelFetch);

    impl Abort for ModelAbort {
        fn abort(self) {
            self.0.meter.aborts.set(self.0.meter.aborts.get() + 1);
            self.0.end();
        }
    }

    /// The counting harness: each request, once MADE, starts a model fetch
    /// holding `body` bytes, which answers after `hold` turns (or never with
    /// `None`). With `guarded` the request holds its controller in an
    /// [`AbortOnDrop`], as the shipped requests do, so a cut aborts the fetch;
    /// without it a cut fetch runs on holding its body (the control).
    fn counting(
        body: usize,
        hold: Option<usize>,
        n: usize,
        meter: &Meter,
        guarded: bool,
    ) -> Vec<impl FnOnce() -> std::pin::Pin<Box<dyn Future<Output = ()>>>> {
        (0..n)
            .map(|_| {
                let meter = meter.clone();
                move || -> std::pin::Pin<Box<dyn Future<Output = ()>>> {
                    let fetch = ModelFetch::start(&meter, body);
                    Box::pin(async move {
                        let _abort = guarded.then(|| AbortOnDrop::new(ModelAbort(fetch.clone())));
                        match hold {
                            Some(k) => turns(k).await,
                            None => futures_util::future::pending().await,
                        }
                        fetch.end();
                    })
                }
            })
            .collect()
    }

    #[tokio::test]
    async fn fanout0811_fold3_the_bytes_in_flight_stay_under_the_budget() {
        const MIB: usize = 1024 * 1024;
        // A ladder of body sizes, each to the full 32 targets, every request
        // answering: the most bytes held at once is the budget (or, past it,
        // one body), and every target is answered.
        for body in [
            200,
            64 * 1024,
            MIB,
            FANOUT_POST_BYTES_IN_FLIGHT / 6 + 1,
            2 * MIB,
            4 * MIB,
            FANOUT_POST_BYTES_IN_FLIGHT,
            FANOUT_POST_BYTES_IN_FLIGHT + 1,
            10 * MIB,
            32 * MIB,
        ] {
            let meter = Meter::default();
            let (held, most) = (meter.held.clone(), meter.most.clone());
            let concurrency = post_concurrency(body);
            let fates = within_budget(
                counting(body, Some(3), FANOUT_MAX_HOSTS, &meter, true),
                concurrency,
                futures_util::future::pending::<()>,
                futures_util::future::pending::<()>(),
            )
            .await;
            assert!(
                fates.iter().all(|f| *f == Sent::Answered(())),
                "{body}: every target answered"
            );
            assert_eq!(held.get(), 0);
            assert!(
                most.get() <= FANOUT_POST_BYTES_IN_FLIGHT.max(body),
                "{body} bytes x 32 targets: {} bytes in flight at once (budget {})",
                most.get(),
                FANOUT_POST_BYTES_IN_FLIGHT
            );
            assert_eq!(
                most.get(),
                concurrency * body,
                "{body}: the pool is used to its bound"
            );
        }
        // A small body goes out together (the connections' limit); one past
        // the budget one at a time.
        assert_eq!(post_concurrency(200), FANOUT_POST_CONNECTIONS);
        assert_eq!(post_concurrency(FANOUT_POST_BYTES_IN_FLIGHT / 6), 6);
        assert_eq!(post_concurrency(FANOUT_POST_BYTES_IN_FLIGHT / 6 + 1), 5);
        assert_eq!(post_concurrency(FANOUT_POST_BYTES_IN_FLIGHT), 1);
        assert_eq!(post_concurrency(FANOUT_POST_BYTES_IN_FLIGHT + 1), 1);
        assert_eq!(post_concurrency(0), FANOUT_POST_CONNECTIONS);
        assert!((0..64 * MIB)
            .step_by(4099)
            .all(|b| post_concurrency(b) <= FANOUT_POST_CONNECTIONS));

        // The control: fold 2's rule (every target at once) holds 32 bodies.
        let meter = Meter::default();
        let most = meter.most.clone();
        let body = 4 * MIB;
        within_deadline(
            counting(body, Some(3), FANOUT_MAX_HOSTS, &meter, true)
                .into_iter()
                .map(|make| make())
                .collect(),
            futures_util::future::pending::<()>,
        )
        .await;
        assert_eq!(
            most.get(),
            FANOUT_MAX_HOSTS * body,
            "fold 2 held 32 x the body"
        );

        // The shipped loop sends through it at the body's concurrency.
        let src = include_str!("mainnet_fanout.rs");
        let planned = body_of(src, "async fn fan_out_planned(");
        assert!(planned.contains("let concurrency = post_concurrency(tagged.beef.len());"));
        assert!(planned.contains("deadline_of(FANOUT_POST_EACH_DEADLINE_MS)"));
        assert!(planned.contains("move || post_beef_to_peer(target, tagged)"));
        // The advertiser's ad sync takes the same function.
        let advertiser = include_str!("advertiser.rs");
        assert!(advertiser.contains("crate::mainnet_fanout::fan_out(&tagged"));
        const _: () = assert!(FANOUT_POST_EACH_DEADLINE_MS < FANOUT_POST_DEADLINE_MS);
        const _: () = assert!(FANOUT_ASK_DEADLINE_MS + FANOUT_POST_DEADLINE_MS <= 20_000);
    }

    #[tokio::test]
    async fn fanout0811_fold3_hung_hosts_are_cut_and_the_rest_counted_untried() {
        // 32 targets that never answer, one at a time (a body past the
        // budget): each started one is cut by its own deadline (10 turns), the
        // phase (25 turns) ends with the third in flight, and the other 29 are
        // untried, never silently dropped.
        let meter = Meter::default();
        let (held, most) = (meter.held.clone(), meter.most.clone());
        let body = FANOUT_POST_BYTES_IN_FLIGHT + 1;
        let fates = within_budget(
            counting(body, None, FANOUT_MAX_HOSTS, &meter, true),
            post_concurrency(body),
            || turns(10),
            turns(25),
        )
        .await;
        let cut = fates.iter().filter(|f| **f == Sent::Cut).count();
        let untried = fates.iter().filter(|f| **f == Sent::Untried).count();
        assert_eq!((cut, untried), (3, 29), "{fates:?}");
        assert_eq!(cut + untried, FANOUT_MAX_HOSTS);
        assert_eq!(most.get(), body, "one body at a time");
        assert_eq!(
            held.get(),
            0,
            "the abort of each cut fetch released its body"
        );
        assert_eq!(meter.aborts.get(), 3, "each cut fetch aborted once");

        // One hung host among answering ones costs them one slot: with six at
        // once the hung one is cut, every other answers inside the phase.
        let meter = Meter::default();
        let most = meter.most.clone();
        let mut makes: Vec<Make> = Vec::new();
        for (i, make) in counting(200, Some(2), FANOUT_MAX_HOSTS, &meter, true)
            .into_iter()
            .enumerate()
        {
            if i == 0 {
                makes.extend(
                    counting(200, None, 1, &meter, true)
                        .into_iter()
                        .map(|m| Box::new(m) as Make),
                );
            } else {
                makes.push(Box::new(make));
            }
        }
        let fates = within_budget(makes, post_concurrency(200), || turns(50), turns(500)).await;
        assert_eq!(fates[0], Sent::Cut);
        assert!(
            fates[1..].iter().all(|f| *f == Sent::Answered(())),
            "{fates:?}"
        );
        assert!(most.get() <= 6 * 200);
    }

    // ---- FANOUT-0811-FOLD-4: a cut request is aborted ----

    #[tokio::test]
    async fn fanout0811_fold4_m1_a_cut_request_is_aborted_and_releases_its_body() {
        // The lens's P1: 32 hosts that never answer, a body at the budget's
        // sixth (six at once), each POST cut at 10 turns, the phase at 19. With
        // the guard each cut aborts its fetch: never more than six fetches
        // alive, never more than the budget held, nothing alive at the end.
        let body = FANOUT_POST_BYTES_IN_FLIGHT / FANOUT_POST_CONNECTIONS;
        assert_eq!(post_concurrency(body), FANOUT_POST_CONNECTIONS);
        let run = |guarded: bool| async move {
            let meter = Meter::default();
            let fates = within_budget(
                counting(body, None, FANOUT_MAX_HOSTS, &meter, guarded),
                post_concurrency(body),
                || turns(10),
                turns(19),
            )
            .await;
            (meter, posts_cut_and_untried(&fates))
        };
        let (meter, (cut, untried)) = run(true).await;
        assert_eq!((cut, untried), (12, 20));
        assert_eq!(meter.aborts.get(), 12, "every cut fetch aborted, once");
        assert_eq!(meter.held.get(), 0, "no fetch alive after the phase");
        assert!(
            meter.most.get() <= FANOUT_POST_BYTES_IN_FLIGHT,
            "{} bytes in flight at once",
            meter.most.get()
        );
        assert_eq!(
            meter.most.get() / body,
            FANOUT_POST_CONNECTIONS,
            "at most six fetches alive at once"
        );

        // The control: the cut a drop of the future alone (no guard), as
        // `Fetch::send` makes it. The fetches run on with their bodies.
        let (meter, (cut, untried)) = run(false).await;
        assert_eq!((cut, untried), (12, 20));
        assert_eq!(meter.aborts.get(), 0);
        assert_eq!(
            meter.held.get() / body,
            12,
            "12 fetches alive at the phase's end"
        );
        assert_eq!(meter.most.get(), 12 * body, "twice the budget in flight");
    }

    #[tokio::test]
    async fn fanout0811_fold4_m1_the_guard_aborts_once_on_any_drop() {
        use std::cell::Cell;
        use std::rc::Rc;
        struct Counted(Rc<Cell<usize>>);
        impl Abort for Counted {
            fn abort(self) {
                self.0.set(self.0.get() + 1);
            }
        }
        // Dropped in place: one abort.
        let aborts = Rc::new(Cell::new(0));
        drop(AbortOnDrop::new(Counted(aborts.clone())));
        assert_eq!(aborts.get(), 1);
        // Held by a future dropped mid-await (a deadline's cut): one abort.
        let aborts = Rc::new(Cell::new(0));
        let guarded = {
            let aborts = aborts.clone();
            async move {
                let _abort = AbortOnDrop::new(Counted(aborts));
                futures_util::future::pending::<()>().await;
            }
        };
        let answer = within_deadline(vec![guarded], || turns(3)).await;
        assert_eq!(answer, vec![None]);
        assert_eq!(aborts.get(), 1, "the cut aborted the request");
        // A request that answers: one abort at its end (its response released).
        let aborts = Rc::new(Cell::new(0));
        let answered = {
            let aborts = aborts.clone();
            async move {
                let _abort = AbortOnDrop::new(Counted(aborts));
                7
            }
        };
        assert_eq!(
            within_deadline(vec![answered], || turns(3)).await,
            vec![Some(7)]
        );
        assert_eq!(aborts.get(), 1);
    }

    #[test]
    fn fanout0811_fold4_m1_the_shipped_requests_are_sent_with_a_signal_the_guard_aborts() {
        let src = include_str!("mainnet_fanout.rs");
        for name in ["async fn ask_tracker(", "async fn post_beef_to_peer("] {
            let body = body_of(src, name);
            assert!(
                body.contains("let controller = AbortController::default();"),
                "{name}"
            );
            assert!(body.contains("let signal = controller.signal();"), "{name}");
            assert!(body.contains("AbortOnDrop::new(controller);"), "{name}");
            assert!(body.contains(".send_with_signal(&signal)"), "{name}");
            assert!(
                !body.contains(".send()"),
                "{name}: never sent without a signal"
            );
            let guard = body.find("AbortOnDrop::new(controller)").unwrap();
            let send = body.find(".send_with_signal(&signal)").unwrap();
            assert!(guard < send, "{name}: guarded before it is sent");
        }
        // The POST reads the status and releases the response unread.
        let post = body_of(src, "async fn post_beef_to_peer(");
        let status = post.find("let status = response.status_code();").unwrap();
        let released = post.find("drop(response);\n    drop(abort);").unwrap();
        assert!(status < released, "released after its status is read");
        assert!(!post.contains("response.text()") && !post.contains("response.bytes()"));
        // The guard's drop aborts; the platform's controller is the trait's.
        let guard = body_of(src, "impl<A: Abort> Drop for AbortOnDrop<A> {");
        assert!(guard.contains("if let Some(controller) = self.0.take() {"));
        assert!(guard.contains("controller.abort();"));
        let platform = body_of(src, "impl Abort for AbortController {");
        assert!(platform.contains("AbortController::abort(self)"));
    }

    #[test]
    fn fanout0811_fold4_l1_the_shipped_loop_counts_cuts_and_untried_by_the_pure_fold() {
        let fates = [
            Sent::Answered(Ok::<(), String>(())),
            Sent::Cut,
            Sent::Untried,
            Sent::Answered(Err("HTTP 500".into())),
            Sent::Cut,
            Sent::Untried,
            Sent::Untried,
        ];
        assert_eq!(posts_cut_and_untried(&fates), (2, 3));
        assert_eq!(posts_cut_and_untried::<()>(&[]), (0, 0));
        // The shipped loop takes its counts from them, and counts nothing else.
        let src = include_str!("mainnet_fanout.rs");
        let planned = body_of(src, "async fn fan_out_planned(");
        assert!(planned.contains("let mut requests_cut = read.cut;"));
        assert!(planned.contains("lookups_untried: read.untried,"));
        assert!(
            planned.contains("let (posts_cut, posts_untried) = posts_cut_and_untried(&posted);")
        );
        assert!(planned.contains("requests_cut += posts_cut;"));
        assert_eq!(
            planned.matches("+= ").count(),
            1,
            "no other count in the loop"
        );
        assert!(planned.contains("requests_cut,\n        posts_untried,\n        ..plan.drops"));
        // The budget's figure.
        assert_eq!(FANOUT_POST_BYTES_IN_FLIGHT, 8 * 1024 * 1024);
    }

    // ---- FANOUT-0811-FOLD-5: the lookups hold a bounded amount at once ----

    /// An answer as its request reads it: `outputs` carried, `read` of them
    /// read, an advert (its own origin, of tracker `t`, lookup `i`) at every
    /// `every`-th position read.
    fn answer_read(outputs: usize, every: usize, t: usize, i: usize) -> AnswerRead {
        let read = outputs.min(FANOUT_MAX_OUTPUTS_PER_ANSWER);
        AnswerRead {
            outputs,
            read,
            adverts: (0..read)
                .step_by(every.max(1))
                .map(|pos| {
                    let n = i * 1_000 + pos;
                    (
                        pos,
                        fake(&format!("https://h{n}.t{t}.example"), (n % 251) as u8, n),
                    )
                })
                .collect(),
        }
    }

    /// A boxed lookup maker, as the model makes them.
    type LookupMake<'a> = Box<
        dyn FnOnce() -> std::pin::Pin<Box<dyn Future<Output = Result<AnswerRead, String>> + 'a>>
            + 'a,
    >;

    /// The model lookups: lookup `i`, once MADE, holds its answer's `text`
    /// bytes (a fetch the meter counts, ended by its answer or its abort)
    /// until it has read them, after `holds[i]` turns (`None`: never), then
    /// hands back `answers[i]`. Each make asserts the fold's read bound is not
    /// spent and the adverts held are within it, and notes its index.
    fn model_lookups<'a>(
        answers: &[AnswerRead],
        holds: &[Option<usize>],
        text: usize,
        meter: &Meter,
        fold: &'a RefCell<LookupFold>,
        made: &'a RefCell<Vec<usize>>,
    ) -> Vec<LookupMake<'a>> {
        answers
            .iter()
            .cloned()
            .zip(holds.iter().copied())
            .enumerate()
            .map(|(i, (answer, hold))| {
                let meter = meter.clone();
                Box::new(move || {
                    {
                        let f = fold.borrow();
                        assert!(
                            !f.spent(),
                            "lookup {i} started after the read bound was spent"
                        );
                        assert!(f.adverts_held() <= FANOUT_MAX_OUTPUTS_READ);
                    }
                    made.borrow_mut().push(i);
                    let fetch = ModelFetch::start(&meter, text);
                    Box::pin(async move {
                        let _abort = AbortOnDrop::new(ModelAbort(fetch.clone()));
                        match hold {
                            Some(k) => turns(k).await,
                            None => futures_util::future::pending().await,
                        }
                        fetch.end();
                        assert!(fold.borrow().adverts_held() <= FANOUT_MAX_OUTPUTS_READ);
                        Ok(answer)
                    })
                        as std::pin::Pin<Box<dyn Future<Output = Result<AnswerRead, String>>>>
                }) as LookupMake<'a>
            })
            .collect()
    }

    const TOPICS: usize = 100;
    const TRACKERS: usize = 4;

    #[tokio::test]
    async fn fanout0811_fold5_the_answer_text_alive_is_the_pool_times_the_largest() {
        // 100 topics and four trackers, every answer a 1 MiB text, two
        // outputs read each (800 reads: the bound is never spent, so every
        // lookup is made), answering in a scattered order: at most six texts
        // are alive at any instant.
        const MIB: usize = 1024 * 1024;
        let n = TOPICS * TRACKERS;
        let answers: Vec<AnswerRead> = (0..n).map(|i| answer_read(2, 1, i % 4, i)).collect();
        let holds: Vec<Option<usize>> = (0..n).map(|i| Some(1 + (i * 37) % 13)).collect();
        let meter = Meter::default();
        let fold = RefCell::new(LookupFold::new(n));
        let made = RefCell::new(Vec::new());
        lookups_within(
            model_lookups(&answers, &holds, MIB, &meter, &fold, &made),
            FANOUT_ASK_CONNECTIONS,
            &fold,
            futures_util::future::pending::<()>,
            futures_util::future::pending::<()>(),
        )
        .await;
        assert_eq!(
            *made.borrow(),
            (0..n).collect::<Vec<_>>(),
            "made in the asked order"
        );
        assert_eq!(meter.held.get(), 0);
        assert_eq!(
            meter.most.get(),
            FANOUT_ASK_CONNECTIONS * MIB,
            "the most answer text alive at once"
        );
        let read = fold.into_inner().finish(|i| i % 4, TRACKERS);
        assert_eq!((read.cut, read.untried), (0, 0));
        assert_eq!(read.queues.iter().map(Vec::len).sum::<usize>(), 2 * n);

        // The control: every lookup at once (fold 2's rule) holds all 400.
        let meter = Meter::default();
        let fold = RefCell::new(LookupFold::new(n));
        let made = RefCell::new(Vec::new());
        within_deadline(
            model_lookups(&answers, &holds, MIB, &meter, &fold, &made)
                .into_iter()
                .map(|make| make())
                .collect(),
            futures_util::future::pending::<()>,
        )
        .await;
        assert_eq!(meter.most.get(), n * MIB, "fold 2 held 400 texts at once");
    }

    #[tokio::test]
    async fn fanout0811_fold5_the_read_bound_stops_the_lookups_and_the_rest_is_counted() {
        // Every answer reads 256 outputs, an advert at each: four answers
        // spend the bound. Scattered arrivals: the adverts held never pass it
        // (asserted at every make and every answer), no lookup is made once
        // it is spent, and the lookups never made are counted untried.
        let n = TOPICS * TRACKERS;
        let answers: Vec<AnswerRead> = (0..n).map(|i| answer_read(300, 1, i % 4, i)).collect();
        let holds: Vec<Option<usize>> = (0..n).map(|i| Some(1 + ((n - i) * 7) % 11)).collect();
        let meter = Meter::default();
        let fold = RefCell::new(LookupFold::new(n));
        let made = RefCell::new(Vec::new());
        lookups_within(
            model_lookups(&answers, &holds, 4096, &meter, &fold, &made),
            FANOUT_ASK_CONNECTIONS,
            &fold,
            futures_util::future::pending::<()>,
            futures_util::future::pending::<()>(),
        )
        .await;
        let made = made.into_inner();
        assert!(made.len() < FANOUT_ASK_CONNECTIONS + 4, "made {made:?}");
        let fold = fold.into_inner();
        assert!(fold.spent());
        assert_eq!(fold.adverts_held(), FANOUT_MAX_OUTPUTS_READ);
        let read = fold.finish(|i| i % 4, TRACKERS);
        assert_eq!(
            read.untried as usize,
            n - made.len(),
            "every lookup not made counted"
        );
        assert_eq!(read.cut, 0);
        assert_eq!(
            read.queues.iter().map(Vec::len).sum::<usize>(),
            FANOUT_MAX_OUTPUTS_READ,
            "the first four lookups asked, whatever the order they answered in"
        );
        for (t, q) in read.queues.iter().enumerate() {
            assert!(q
                .iter()
                .all(|a| a.target.target.ends_with(&format!(".t{t}.example"))));
            assert!(q
                .iter()
                .all(|a| a.outpoint.0 < format!("{:064x}", 4 * 1_000)));
        }
        assert_eq!(
            read.drops.outputs_over_bound,
            (made.len() * 300 - FANOUT_MAX_OUTPUTS_READ) as u64
        );

        // Out of order by hand: a late answer asked earlier trims those held.
        let mut fold = LookupFold::new(6);
        for i in [5, 4, 3, 2] {
            fold.started(i);
            fold.hand_in(i, Ok(answer_read(300, 1, 0, i)));
            assert!(fold.adverts_held() <= FANOUT_MAX_OUTPUTS_READ, "after {i}");
        }
        assert!(fold.spent() && !fold.spent_before(2) && fold.spent_before(6));
        fold.hand_in(0, Ok(answer_read(300, 1, 0, 0)));
        assert_eq!(fold.adverts_held(), FANOUT_MAX_OUTPUTS_READ);
        let read = fold.finish(|_| 0, 1);
        assert_eq!(read.untried, 1, "lookup 1 never started");
        assert!(read.queues[0]
            .iter()
            .all(|a| a.outpoint.0 < format!("{:064x}", 5 * 1_000)));

        // The phase's end and a hung tracker: cut and untried, each counted.
        let answers: Vec<AnswerRead> = (0..n).map(|i| answer_read(1, 1, i % 4, i)).collect();
        let holds: Vec<Option<usize>> = (0..n)
            .map(|i| if i % 4 == 0 { None } else { Some(2) })
            .collect();
        let meter = Meter::default();
        let fold = RefCell::new(LookupFold::new(n));
        let made = RefCell::new(Vec::new());
        lookups_within(
            model_lookups(&answers, &holds, 4096, &meter, &fold, &made),
            FANOUT_ASK_CONNECTIONS,
            &fold,
            || turns(10),
            turns(60),
        )
        .await;
        let made = made.into_inner().len();
        let read = fold.into_inner().finish(|i| i % 4, TRACKERS);
        assert!(
            read.cut > 0 && read.untried > 0,
            "{} cut, {} untried",
            read.cut,
            read.untried
        );
        assert_eq!(read.untried as usize, n - made);
        assert_eq!(meter.held.get(), 0, "every cut lookup aborted");
        let drops = FanoutDrops {
            lookups_untried: read.untried,
            ..Default::default()
        };
        assert_eq!(
            drops.deltas().get(COUNTER_LOOKUPS_UNTRIED),
            Some(&read.untried)
        );
        assert!(COUNTERS.contains(&COUNTER_LOOKUPS_UNTRIED));
    }

    #[tokio::test]
    async fn fanout0811_fold5_the_plan_is_the_same_whatever_order_the_answers_arrive_in() {
        // 400 answers of 20 outputs, every fifth an advert (the bound spent
        // part way): the queues, the drops and the plan are the ones the
        // answers give read one after another in the asked order, for five
        // orders of arrival.
        let n = TOPICS * TRACKERS;
        let answers: Vec<AnswerRead> = (0..n).map(|i| answer_read(20, 5, i % 4, i)).collect();
        let mut sequential = LookupFold::new(n);
        for (i, a) in answers.iter().enumerate() {
            sequential.started(i);
            sequential.hand_in(i, Ok(a.clone()));
        }
        let want = sequential.finish(|i| i % 4, TRACKERS);
        let plan_of =
            |read: &LookupsRead| plan_fanout(read.queues.clone(), None, read.drops, admit_all);
        let want_plan = plan_of(&want);
        let orders: [&dyn Fn(usize) -> usize; 5] = [
            &|_| 1,
            &|i| 1 + (i * 37) % 13,
            &|i| 1 + (n - i) % 9,
            &|i| if i % 2 == 0 { 9 } else { 1 },
            &|i| 1 + (i * i) % 17,
        ];
        for (k, hold) in orders.iter().enumerate() {
            let holds: Vec<Option<usize>> = (0..n).map(|i| Some(hold(i))).collect();
            let meter = Meter::default();
            let fold = RefCell::new(LookupFold::new(n));
            let made = RefCell::new(Vec::new());
            lookups_within(
                model_lookups(&answers, &holds, 64, &meter, &fold, &made),
                FANOUT_ASK_CONNECTIONS,
                &fold,
                futures_util::future::pending::<()>,
                futures_util::future::pending::<()>(),
            )
            .await;
            let made = made.into_inner().len();
            let read = fold.into_inner().finish(|i| i % 4, TRACKERS);
            let targets = |r: &LookupsRead| -> Vec<Vec<String>> {
                r.queues
                    .iter()
                    .map(|q| q.iter().map(|a| a.target.target.clone()).collect())
                    .collect()
            };
            assert_eq!(targets(&read), targets(&want), "order {k}: the queues");
            assert_eq!(
                read.drops.adverts_refused, want.drops.adverts_refused,
                "order {k}"
            );
            assert_eq!(
                read.drops.outputs_over_bound + 20 * read.untried,
                want.drops.outputs_over_bound,
                "order {k}: every output past the bound is unread or untried"
            );
            assert_eq!(read.untried as usize, n - made);
            assert_eq!(
                plan_of(&read).targets,
                want_plan.targets,
                "order {k}: the plan"
            );
        }
    }

    #[test]
    fn fanout0811_fold5_the_shipped_loop_reads_each_answer_in_its_request_through_the_pool() {
        let src = include_str!("mainnet_fanout.rs");
        let planned = body_of(src, "async fn fan_out_planned(");
        assert!(planned.contains("lookups_within("));
        assert!(planned.contains("FANOUT_ASK_CONNECTIONS,\n        &fold,"));
        assert!(planned.contains("move || ask_and_read(DEFAULT_SLAP_TRACKERS[t], topic)"));
        assert!(planned.contains("lookups_untried: read.untried,"));
        assert!(!planned.contains("read_answers(") && !planned.contains("ask_tracker("));
        assert!(!planned.contains("text"), "no answer text reaches the loop");
        // The text lives in the request: read there, dropped before it answers.
        let ask = body_of(src, "async fn ask_and_read(");
        assert!(ask.contains("let text = ask_tracker(tracker, topic).await?;"));
        assert!(ask.contains("adverts_of_answer(&text, topic)"));
        // The pool takes a lookup only while its share is not spent, and
        // checks again when it starts; the fold trims at every hand-in.
        let pool = body_of(src, "pub async fn lookups_within<");
        assert!(pool.contains(".take_while(|(i, _)| ready(!fold.borrow().spent_before(*i)))"));
        assert!(pool.contains("if fold.borrow().spent_before(i) {\n                    return;"));
        assert!(pool.contains(".buffer_unordered(concurrency.max(1))"));
        let hand_in = body_of(src, "    pub fn hand_in(");
        assert!(hand_in.contains("r.adverts.retain(|(pos, _)| *pos < share);"));
        const _: () = assert!(FANOUT_ASK_CONNECTIONS <= FANOUT_POST_CONNECTIONS);
    }

    /// The measured cost of a read and a check (natively; run with
    /// `--release -- --ignored --nocapture`).
    #[test]
    #[ignore]
    fn fanout0811_fold_measure_a_read_and_a_check() {
        let beef = beef_of_lock(signed_lock("https://peer.example", TOPIC));
        let n = 500u32;
        let start = std::time::Instant::now();
        let mut advert = None;
        for _ in 0..n {
            advert = read_advert(&beef, 0, TOPIC);
        }
        let read_us = start.elapsed().as_micros() as f64 / f64::from(n);
        let advert = advert.unwrap();
        let start = std::time::Instant::now();
        for _ in 0..n {
            assert!(advert_is_admitted(&advert));
        }
        let check_us = start.elapsed().as_micros() as f64 / f64::from(n);
        println!(
            "fanout0811 measure: read {read_us:.1} us, check {check_us:.1} us (BEEF {} bytes); worst {} reads + {} checks = {:.1} ms",
            beef.len(),
            FANOUT_MAX_OUTPUTS_READ,
            FANOUT_MAX_VERIFICATIONS,
            (read_us * FANOUT_MAX_OUTPUTS_READ as f64 + check_us * FANOUT_MAX_VERIFICATIONS as f64) / 1000.0
        );
    }
}
