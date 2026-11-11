//! bsv-low #1165 (the design `docs/DESIGN-1165-CHAIN-CLOCK-SECOND-OPINION-2026-11-11.md`, R1 to R6): the chain
//! clock's SECOND OPINION. When `/tip`'s verified snapshot is past [`SECOND_OPINION_AFTER_MS`] of pass age and its lag
//! bar passes, the couriers are asked for their best block (height and hash) in parallel under one
//! [`SECOND_OPINION_DEADLINE_MS`] deadline; the stored tip is CORROBORATED when at least two of them name it, and never
//! when any one names a block above it (the store is behind) or another hash at its height (the tip is contested).
//! The height and hash served stay the verified header's: a courier's word only says "nothing newer exists", the
//! negative a header cannot prove.
//!
//! The verdict is a PURE function ([`verdict`]); the fetch ([`ask_env`]) goes through the courier census's own
//! fetch (`routes::provider_get`), so `COURIER_SINK` seals it in the tiers and every ask is counted by provider.

use crate::tracker_host::VerifiedTip;
use serde_json::{json, Value};
use std::cell::RefCell;

/// R1: a pass at most this old is answered as before, and no courier is asked (half the client's 10-minute bar; a
/// healthy header service stamps every minute).
pub const SECOND_OPINION_AFTER_MS: u64 = 5 * 60 * 1_000;
/// R2: the one deadline every source is asked under.
pub const SECOND_OPINION_DEADLINE_MS: u64 = 2_000;
/// R3: the sources that must name the stored tip.
pub const CORROBORATIONS_NEEDED: usize = 2;
/// R5: how long a corroboration is memoised per isolate.
pub const MEMO_CORROBORATED_MS: u64 = 30_000;
/// R5: how long a refusal is memoised per isolate.
pub const MEMO_REFUSED_MS: u64 = 10_000;
/// The courier census's caller label for these asks.
pub const CALLER: &str = "tip";

/// R6 (beta only, behind the test-knob gate): the verdict reads the pass as this many ms old.
pub const TIP_TEST_PASS_AGE_MS_VAR: &str = "TIP_TEST_PASS_AGE_MS";
/// R6 (beta only): one source (the first that answered) reads one block above.
pub const TIP_TEST_SOURCE_AHEAD_VAR: &str = "TIP_TEST_SOURCE_AHEAD";

/// The sources, each with its best-block endpoint (probed by hand 2026-11-11, recorded in the PR):
/// WhatsOnChain `GET /v1/bsv/main/chain/info` (`blocks`, `bestblockhash`), Bitails `GET /network/info` (`blocks`,
/// `bestBlockhash`), BananaBlocks' native `GET /api/v1/block/tip` (`height`, `hash`).
pub const SOURCES: [(&str, &str); 3] = [
    ("woc", "https://api.whatsonchain.com/v1/bsv/main/chain/info"),
    ("bitails", "https://api.bitails.io/network/info"),
    ("bananablocks", "https://bananablocks.com/api/v1/block/tip"),
];

/// The counters (the overlay seeds each at 0).
pub const COUNTER_ASKED: &str = "tip_second_opinion_asked_total";
pub const COUNTER_CORROBORATED: &str = "tip_second_opinion_corroborated_total";
pub const COUNTER_BEHIND: &str = "tip_second_opinion_behind_total";
pub const COUNTER_CONTESTED: &str = "tip_second_opinion_contested_total";
pub const COUNTER_SHORT: &str = "tip_second_opinion_short_total";
/// The fold's F1: a source (or the store's own word) one block above, inside the lag bar's own tolerance.
pub const COUNTER_WITHIN_LAG: &str = "tip_second_opinion_within_lag_total";
/// The fold's F5: the courier census's per-caller counter for these asks (`courier::CALLERS` holds `"tip"`).
pub const COUNTER_COURIER_CALLS: &str = "applayer_courier_calls_tip_total";
/// The fold's F3: an in-flight marker older than this is dead (the request that wrote it died mid-ask).
pub const ASKING_DEAD_AFTER_MS: u64 = SECOND_OPINION_DEADLINE_MS + 500;

/// The names this worker writes on the overlay's `ops_counters`.
pub fn counter_names() -> Vec<String> {
    [
        COUNTER_ASKED,
        COUNTER_CORROBORATED,
        COUNTER_BEHIND,
        COUNTER_CONTESTED,
        COUNTER_SHORT,
        COUNTER_WITHIN_LAG,
        COUNTER_COURIER_CALLS,
    ]
    .iter()
    .map(|n| n.to_string())
    .collect()
}

/// One source's answer: its best block, or why it gave none; `at` is the clock when the answer came.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceAnswer {
    pub name: String,
    pub read: Result<(u32, String), String>,
    pub at: u64,
}

/// Why a second opinion refused (R3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// A source names a best block above the stored height.
    Behind,
    /// A source names another hash at the stored height.
    Contested,
    /// Fewer than two sources name the stored tip (or the two newest answers are older than
    /// [`MEMO_CORROBORATED_MS`] at the clock read after the ask: the fold's F4).
    Short,
    /// The fold's F1 and F2: the highest height a source or the store's own word names is above the stored height,
    /// by at most the lag bar's own tolerance (`TIP_LAG_MAX_BLOCKS`). Never a corroboration; served as `Short` is.
    WithinLag,
}

impl Refusal {
    pub fn word(self) -> &'static str {
        match self {
            Refusal::Behind => "behind",
            Refusal::Contested => "contested",
            Refusal::Short => "short",
            Refusal::WithinLag => "within-lag",
        }
    }
}

/// The verdict (R3): corroborated, with the time of the OLDER of the two newest corroborating answers (the age R4
/// serves), or refused with its reason. Both carry every source's answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    Corroborated {
        checked_at: u64,
        sources: Vec<SourceAnswer>,
    },
    Refused {
        reason: Refusal,
        sources: Vec<SourceAnswer>,
    },
}

impl Verdict {
    pub fn word(&self) -> &'static str {
        match self {
            Verdict::Corroborated { .. } => "corroborated",
            Verdict::Refused { reason, .. } => reason.word(),
        }
    }
    pub fn sources(&self) -> &[SourceAnswer] {
        match self {
            Verdict::Corroborated { sources, .. } | Verdict::Refused { sources, .. } => sources,
        }
    }
}

/// One source's standing against the stored tip.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Standing {
    Matches,
    Above,
    OtherHash,
    /// Below the stored height, or not asked: counts for nothing either way.
    Nothing,
}

fn standing(tip_height: u32, tip_hash: &str, answer: &SourceAnswer) -> Standing {
    match &answer.read {
        Ok((height, _)) if *height > tip_height => Standing::Above,
        Ok((height, hash)) if *height == tip_height => {
            if hash.eq_ignore_ascii_case(tip_hash) {
                Standing::Matches
            } else {
                Standing::OtherHash
            }
        }
        _ => Standing::Nothing,
    }
}

/// PURE (R3, with the fold's F1, F2 and F4): the verdict of the sources' answers on the stored tip.
///
/// `ahead` is the highest height any source names, or the store's own word names (`store_lag`, the blocks the store
/// says it has seen past its verified tip: one more voice, F2), less the stored height. `ahead` past `lag_max` is
/// `Behind`; else any other hash at the stored height is `Contested` (stricter than the lag bar on purpose: a
/// contested tip is evidence of a reorg at the height a stake would be built on); else `ahead` of 1 up to `lag_max`
/// is `WithinLag` (never a corroboration); else at least [`CORROBORATIONS_NEEDED`] sources naming the stored hash at
/// the stored height (lowercase hex compared) corroborate, with the time of the older of the two newest matches,
/// unless that time is more than [`MEMO_CORROBORATED_MS`] before `now` (the clock read AFTER the ask), which is
/// `Short`; else `Short`.
pub fn verdict(
    tip_height: u32,
    tip_hash: &str,
    store_lag: Option<u32>,
    lag_max: u32,
    sources: Vec<SourceAnswer>,
    now: u64,
) -> Verdict {
    let standings: Vec<Standing> = sources
        .iter()
        .map(|s| standing(tip_height, tip_hash, s))
        .collect();
    let ahead = sources
        .iter()
        .filter_map(|s| s.read.as_ref().ok())
        .map(|(h, _)| h.saturating_sub(tip_height))
        .chain(store_lag)
        .max()
        .unwrap_or(0);
    let refused = |reason| Verdict::Refused {
        reason,
        sources: sources.clone(),
    };
    if ahead > lag_max {
        return refused(Refusal::Behind);
    }
    if standings.contains(&Standing::OtherHash) {
        return refused(Refusal::Contested);
    }
    if ahead >= 1 {
        return refused(Refusal::WithinLag);
    }
    let mut matched: Vec<u64> = sources
        .iter()
        .zip(&standings)
        .filter(|(_, st)| **st == Standing::Matches)
        .map(|(s, _)| s.at)
        .collect();
    if matched.len() < CORROBORATIONS_NEEDED {
        return refused(Refusal::Short);
    }
    matched.sort_unstable_by(|a, b| b.cmp(a));
    let checked_at = matched[CORROBORATIONS_NEEDED - 1];
    if now.saturating_sub(checked_at) > MEMO_CORROBORATED_MS {
        return refused(Refusal::Short);
    }
    Verdict::Corroborated {
        checked_at,
        sources,
    }
}

/// PURE: one source's best block out of its body, by its name's shape; `Err` names what did not read.
pub fn parse_best_block(name: &str, status: u16, body: &[u8]) -> Result<(u32, String), String> {
    if !(200..300).contains(&status) {
        return Err(format!("HTTP {status}"));
    }
    let v: Value = serde_json::from_slice(body).map_err(|_| "not JSON".to_string())?;
    let (h, x) = match name {
        "woc" => ("blocks", "bestblockhash"),
        "bitails" => ("blocks", "bestBlockhash"),
        "bananablocks" => ("height", "hash"),
        _ => return Err(format!("unknown source {name}")),
    };
    let height = v[h]
        .as_u64()
        .and_then(|n| u32::try_from(n).ok())
        .ok_or_else(|| format!("no {h}"))?;
    let hash = v[x]
        .as_str()
        .filter(|s| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()))
        .ok_or_else(|| format!("no {x}"))?
        .to_ascii_lowercase();
    Ok((height, hash))
}

/// The knobs of R6, read off the env through `auth::test_knob_n` (0 and false without the beta marker).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TipKnobs {
    pub pass_age_ms: u64,
    pub source_ahead: bool,
}

impl TipKnobs {
    /// PURE: the knobs a render honours.
    pub fn of(
        beta_marker: Option<&str>,
        pass_age: Option<&str>,
        source_ahead: Option<&str>,
    ) -> Self {
        TipKnobs {
            pass_age_ms: crate::auth::test_knob_n(beta_marker, pass_age),
            source_ahead: crate::auth::test_knob_n(beta_marker, source_ahead) > 0,
        }
    }

    /// `/health.testKnobs`'s entries for these knobs.
    pub fn listed(&self) -> Vec<String> {
        let mut v = Vec::new();
        if self.pass_age_ms > 0 {
            v.push(format!(
                "{TIP_TEST_PASS_AGE_MS_VAR}={} (/tip reads its pass as that old)",
                self.pass_age_ms
            ));
        }
        if self.source_ahead {
            v.push(format!(
                "{TIP_TEST_SOURCE_AHEAD_VAR}=1 (/tip's first source reads one block above)"
            ));
        }
        v
    }
}

/// The knobs off the env.
pub fn knobs_env(env: &worker::Env) -> TipKnobs {
    let var = |n: &str| env.var(n).ok().map(|v| v.to_string());
    TipKnobs::of(
        var(crate::auth::BETA_MARKER_VAR).as_deref(),
        var(TIP_TEST_PASS_AGE_MS_VAR).as_deref(),
        var(TIP_TEST_SOURCE_AHEAD_VAR).as_deref(),
    )
}

/// PURE (R6): the source-ahead knob over the answers: the first source that answered reads one block above.
pub fn apply_source_ahead(sources: &mut [SourceAnswer]) {
    if let Some((h, _)) = sources.iter_mut().find_map(|s| s.read.as_mut().ok()) {
        *h = h.saturating_add(1);
    }
}

struct Memo {
    hash: String,
    verdict: Verdict,
    at: u64,
}

thread_local! {
    /// R5: this isolate's last verdict, keyed by the tip's hash.
    static MEMO: RefCell<Option<Memo>> = const { RefCell::new(None) };
    /// `/health.tipSecondOpinion`: this isolate's last verdict.
    static LAST: RefCell<Option<Value>> = const { RefCell::new(None) };
    /// The fold's F3: "asking for <hash> since <ms>", written before the ask's first await, cleared when the verdict
    /// is noted (or when the asking request is dropped).
    static ASKING: RefCell<Option<(String, u64)>> = const { RefCell::new(None) };
}

/// The fold's F3: is an ask for this tip's hash in flight in this isolate (a marker no older than
/// [`ASKING_DEAD_AFTER_MS`])? A request that finds one does not ask and does not wait: it never awaits a future
/// another request started (in workerd a request must not await I/O another request's context began), so there is
/// no shared future, only this marker.
pub fn asking_for(hash: &str, now: u64) -> bool {
    ASKING.with(|a| {
        a.borrow().as_ref().is_some_and(|(h, since)| {
            h.eq_ignore_ascii_case(hash) && now.saturating_sub(*since) <= ASKING_DEAD_AFTER_MS
        })
    })
}

/// The marker of one ask in flight; dropping it clears the marker if it is still this ask's.
pub struct Asking {
    hash: String,
    since: u64,
}

/// Write the marker (before the ask's first await).
pub fn begin_asking(hash: &str, now: u64) -> Asking {
    ASKING.with(|a| *a.borrow_mut() = Some((hash.to_string(), now)));
    Asking {
        hash: hash.to_string(),
        since: now,
    }
}

impl Drop for Asking {
    fn drop(&mut self) {
        ASKING.with(|a| {
            let mut a = a.borrow_mut();
            if a.as_ref()
                .is_some_and(|(h, since)| *h == self.hash && *since == self.since)
            {
                *a = None;
            }
        });
    }
}

/// Tests only: plant a marker as a request that is still asking (or died asking) would have left it.
#[cfg(test)]
pub fn plant_asking(hash: &str, since: u64) {
    ASKING.with(|a| *a.borrow_mut() = Some((hash.to_string(), since)));
}

/// R5: the memoised verdict for this tip hash, while it is young (30 s corroborated, 10 s refused); a memo for another
/// tip is never served.
pub fn memo_for(hash: &str, now: u64) -> Option<Verdict> {
    MEMO.with(|m| {
        let m = m.borrow();
        let memo = m.as_ref()?;
        if !memo.hash.eq_ignore_ascii_case(hash) {
            return None;
        }
        let ttl = match memo.verdict {
            Verdict::Corroborated { .. } => MEMO_CORROBORATED_MS,
            Verdict::Refused { .. } => MEMO_REFUSED_MS,
        };
        // a memo stamped after `now` (a clock that stepped back) is never served
        (now >= memo.at && now - memo.at < ttl).then(|| memo.verdict.clone())
    })
}

/// Tests only: forget this thread's memo.
#[cfg(test)]
pub fn clear_memo() {
    MEMO.with(|m| *m.borrow_mut() = None);
    ASKING.with(|a| *a.borrow_mut() = None);
}

/// Record a LIVE verdict: the memo, the counters and `/health`'s last verdict.
pub fn note_live(tip: &VerifiedTip, verdict: &Verdict, now: u64) {
    MEMO.with(|m| {
        *m.borrow_mut() = Some(Memo {
            hash: tip.hash.clone(),
            verdict: verdict.clone(),
            at: now,
        })
    });
    crate::courier::note_presence(COUNTER_ASKED);
    crate::courier::note_presence(match verdict {
        Verdict::Corroborated { .. } => COUNTER_CORROBORATED,
        Verdict::Refused {
            reason: Refusal::Behind,
            ..
        } => COUNTER_BEHIND,
        Verdict::Refused {
            reason: Refusal::Contested,
            ..
        } => COUNTER_CONTESTED,
        Verdict::Refused {
            reason: Refusal::Short,
            ..
        } => COUNTER_SHORT,
        Verdict::Refused {
            reason: Refusal::WithinLag,
            ..
        } => COUNTER_WITHIN_LAG,
    });
    LAST.with(|l| {
        *l.borrow_mut() = Some(json!({
            "verdict": verdict.word(),
            "at": now,
            "tipHeight": tip.height,
            "tipHash": tip.hash,
            "corroboration": corroboration_json(tip, verdict),
        }))
    });
}

/// `/health.tipSecondOpinion`: this isolate's last live verdict (`null` before the first), and the bounds.
pub fn health_json() -> Value {
    json!({
        "scope": "isolate",
        "last": LAST.with(|l| l.borrow().clone()),
        "afterMs": SECOND_OPINION_AFTER_MS,
        "deadlineMs": SECOND_OPINION_DEADLINE_MS,
        "sources": SOURCES.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
    })
}

/// R4's `corroboration`: each source's name, height, whether it matched, its time (and its fault, if any).
pub fn corroboration_json(tip: &VerifiedTip, verdict: &Verdict) -> Value {
    Value::Array(
        verdict
            .sources()
            .iter()
            .map(|s| match &s.read {
                Ok((height, hash)) => json!({
                    "source": s.name,
                    "height": height,
                    "hash": hash,
                    "matched": standing(tip.height, &tip.hash, s) == Standing::Matches,
                    "at": s.at,
                }),
                Err(fault) => json!({
                    "source": s.name,
                    "height": Value::Null,
                    "matched": false,
                    "at": s.at,
                    "fault": fault,
                }),
            })
            .collect(),
    )
}

/// The verdict's text for a log line and a 503's reason.
pub fn describe(tip: &VerifiedTip, verdict: &Verdict) -> String {
    let each: Vec<String> = verdict
        .sources()
        .iter()
        .map(|s| match &s.read {
            Ok((h, hash)) => {
                let what = match standing(tip.height, &tip.hash, s) {
                    Standing::Matches => "match",
                    Standing::Above => "ahead",
                    Standing::OtherHash => "another hash",
                    Standing::Nothing => "below",
                };
                format!("{} {h} {} {what}", s.name, &hash[..hash.len().min(16)])
            }
            Err(e) => format!("{} fault: {e}", s.name),
        })
        .collect();
    let store = tip
        .last_seen_height
        .map(|seen| seen.saturating_sub(tip.height))
        .into_iter()
        .chain(tip.live_lag_blocks)
        .max()
        .filter(|lag| *lag > 0)
        .map(|lag| format!("; the store's own word {lag} ahead"))
        .unwrap_or_default();
    format!(
        "the second opinion on {} {} is {}: {}{store}",
        tip.height,
        &tip.hash[..tip.hash.len().min(16)],
        verdict.word(),
        each.join(", ")
    )
}

/// The fetch: every source asked at once through the courier census's fetch, each under the same
/// [`SECOND_OPINION_DEADLINE_MS`] (one deadline: every timer starts together).
pub async fn ask_env() -> Vec<SourceAnswer> {
    let asks = SOURCES.iter().map(|(name, url)| async move {
        let read = crate::routes::provider_get(CALLER, url);
        let deadline = worker::Delay::from(core::time::Duration::from_millis(
            SECOND_OPINION_DEADLINE_MS,
        ));
        futures_util::pin_mut!(read, deadline);
        let read = match futures_util::future::select(read, deadline).await {
            futures_util::future::Either::Left((Some((status, body)), _)) => {
                parse_best_block(name, status, &body)
            }
            futures_util::future::Either::Left((None, _)) => Err("the read failed".to_string()),
            futures_util::future::Either::Right(_) => {
                // the fold's F5: the dropped read never reaches `provider_get`'s own note, so the census is told
                // here (a fault of that provider, caller `tip`, no status, the deadline as its duration)
                crate::courier::note(url, CALLER, None, SECOND_OPINION_DEADLINE_MS as f64);
                Err(format!("no answer within {SECOND_OPINION_DEADLINE_MS} ms"))
            }
        };
        SourceAnswer {
            name: (*name).to_string(),
            read,
            at: worker::Date::now().as_millis(),
        }
    });
    futures_util::future::join_all(asks).await
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: u32 = 974_983;
    fn hash() -> String {
        "ab".repeat(32)
    }
    fn ok(name: &str, height: u32, hash: &str, at: u64) -> SourceAnswer {
        SourceAnswer {
            name: name.into(),
            read: Ok((height, hash.into())),
            at,
        }
    }
    fn fault(name: &str, at: u64) -> SourceAnswer {
        SourceAnswer {
            name: name.into(),
            read: Err("HTTP 503".into()),
            at,
        }
    }
    /// The table's verdict: the store's own word silent, the lag bar's tolerance of one block, the clock at 30.
    fn verdict(tip_height: u32, tip_hash: &str, sources: Vec<SourceAnswer>) -> Verdict {
        super::verdict(tip_height, tip_hash, None, 1, sources, 30)
    }

    #[test]
    fn tip1165_the_verdict_table() {
        let h = hash();
        let other = "cd".repeat(32);
        // two match: corroborated, the older of the two newest
        let v = verdict(H, &h, vec![ok("a", H, &h, 10), ok("b", H, &h, 30)]);
        assert_eq!(v.word(), "corroborated");
        assert!(matches!(v, Verdict::Corroborated { checked_at: 10, .. }));
        // three match: the second newest
        let v = verdict(
            H,
            &h,
            vec![ok("a", H, &h, 10), ok("b", H, &h, 30), ok("c", H, &h, 20)],
        );
        assert!(matches!(v, Verdict::Corroborated { checked_at: 20, .. }));
        // two match and one below: corroborated (below counts for nothing)
        let v = verdict(
            H,
            &h,
            vec![
                ok("a", H, &h, 1),
                ok("b", H, &h, 1),
                ok("c", H - 1, &other, 1),
            ],
        );
        assert_eq!(v.word(), "corroborated");
        // the fold's F1: two match and one above by one: within the lag bar's tolerance, never a corroboration
        let v = verdict(
            H,
            &h,
            vec![
                ok("a", H, &h, 1),
                ok("b", H, &h, 1),
                ok("c", H + 1, &other, 1),
            ],
        );
        assert_eq!(v.word(), "within-lag");
        // two match and one above by two: behind
        let v = verdict(
            H,
            &h,
            vec![
                ok("a", H, &h, 1),
                ok("b", H, &h, 1),
                ok("c", H + 2, &other, 1),
            ],
        );
        assert_eq!(v.word(), "behind");
        // another hash at the stored height: contested
        let v = verdict(
            H,
            &h,
            vec![ok("a", H, &h, 1), ok("b", H, &h, 1), ok("c", H, &other, 1)],
        );
        assert_eq!(v.word(), "contested");
        // above past the tolerance beats contested; contested beats one block within it
        let v = verdict(H, &h, vec![ok("a", H + 2, &h, 1), ok("b", H, &other, 1)]);
        assert_eq!(v.word(), "behind");
        let v = verdict(H, &h, vec![ok("a", H + 1, &h, 1), ok("b", H, &other, 1)]);
        assert_eq!(v.word(), "contested");
        // the fold's F2: the store's own word one ahead is a voice; two ahead is behind
        let two = || vec![ok("a", H, &h, 10), ok("b", H, &h, 30)];
        assert_eq!(
            super::verdict(H, &h, Some(1), 1, two(), 30).word(),
            "within-lag"
        );
        assert_eq!(
            super::verdict(H, &h, Some(2), 1, two(), 30).word(),
            "behind"
        );
        assert_eq!(
            super::verdict(H, &h, Some(0), 1, two(), 30).word(),
            "corroborated"
        );
        // the fold's F4: the older of the two newest matches more than 30 s before the clock is short
        assert_eq!(
            super::verdict(H, &h, None, 1, two(), 10 + MEMO_CORROBORATED_MS).word(),
            "corroborated"
        );
        assert_eq!(
            super::verdict(H, &h, None, 1, two(), 11 + MEMO_CORROBORATED_MS).word(),
            "short"
        );
        // one match, two faults: short; one match, one below, one fault: short
        let v = verdict(H, &h, vec![ok("a", H, &h, 1), fault("b", 1), fault("c", 1)]);
        assert_eq!(v.word(), "short");
        let v = verdict(
            H,
            &h,
            vec![ok("a", H, &h, 1), ok("b", H - 1, &h, 1), fault("c", 1)],
        );
        assert_eq!(v.word(), "short");
        // nothing answered
        assert_eq!(verdict(H, &h, vec![]).word(), "short");
        // lowercase hex compared: an upper-case source matches a lower-case store, and the reverse
        let v = verdict(
            H,
            &h,
            vec![ok("a", H, &h.to_uppercase(), 1), ok("b", H, &h, 1)],
        );
        assert_eq!(v.word(), "corroborated");
        let v = verdict(
            H,
            &h.to_uppercase(),
            vec![ok("a", H, &h, 1), ok("b", H, &h, 1)],
        );
        assert_eq!(v.word(), "corroborated");
    }

    /// The probed body shapes (2026-11-11T03:17:53Z, bodies trimmed).
    #[test]
    fn tip1165_each_sources_body_reads() {
        let hx = "00000000000000001230afea56b2ce2a08c78a6f595aa9fb6ee8fa90751a655f";
        let woc = format!(
            r#"{{"chain":"main","blocks":974983,"headers":974983,"bestblockhash":"{hx}"}}"#
        );
        let bitails = format!(
            r#"{{"blocks":974983,"bestBlockhash":"{}","prunedBeforeBlockHeight":875000}}"#,
            hx.to_uppercase()
        );
        let bb = format!(r#"{{"hash":"{hx}","height":974983,"version":771751936}}"#);
        for (name, body) in [("woc", woc), ("bitails", bitails), ("bananablocks", bb)] {
            assert_eq!(
                parse_best_block(name, 200, body.as_bytes()),
                Ok((974_983, hx.to_string())),
                "{name}"
            );
        }
        assert!(parse_best_block("woc", 429, b"{}").is_err());
        assert!(parse_best_block("woc", 200, b"nope").is_err());
        assert!(parse_best_block("woc", 200, br#"{"blocks":1,"bestblockhash":"zz"}"#).is_err());
        assert!(parse_best_block("bananablocks", 200, br#"{"blocks":1}"#).is_err());
    }

    #[test]
    fn tip1165_the_knobs_need_the_beta_marker() {
        let on = TipKnobs::of(Some("beta"), Some("2700000"), Some("1"));
        assert_eq!(
            on,
            TipKnobs {
                pass_age_ms: 2_700_000,
                source_ahead: true
            }
        );
        assert_eq!(on.listed().len(), 2);
        let off = TipKnobs::of(None, Some("2700000"), Some("1"));
        assert_eq!(off, TipKnobs::default());
        assert!(off.listed().is_empty());
        assert_eq!(
            TipKnobs::of(Some(" "), Some("5"), Some("1")),
            TipKnobs::default()
        );
    }

    #[test]
    fn tip1165_the_seeded_names() {
        assert_eq!(counter_names().len(), 7);
        assert!(counter_names()
            .iter()
            .all(|n| n.starts_with("tip_second_opinion_") || n == COUNTER_COURIER_CALLS));
        assert!(crate::courier::CALLERS.contains(&CALLER), "the fold's F5");
    }
}
