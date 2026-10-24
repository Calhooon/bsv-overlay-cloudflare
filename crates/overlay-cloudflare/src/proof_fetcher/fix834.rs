//! bsv-low #834 (FIX-834): the index janitor's courier pacing, pinned on a replay of loop 44's fault shape (WoC
//! answering 429, pruned Bitails answering "could not look", BananaBlocks answering) over the tick's several
//! fetchers sharing one pass's counters.
use super::*;
use crate::courier_counters::{self, Counters as CourierCounters, Reader};

const T0: u64 = 1_800_000_000_000;
const PRUNED: &str = r#"{"outputs":[{"index":0,"spent":""}]}"#;

fn outpoint(n: u32) -> String {
    format!("{n:064x}")
}

fn answer(url: String, status: u16, body: &str) {
    TEST_HTTP_RESPONSES.with(|r| r.borrow_mut().insert(url, (status, body.to_string())));
}

/// Loop 44's shape for one outpoint: BananaBlocks a clean "unspent", WoC 429, pruned Bitails `spent: ""`.
fn loop44_outpoint(txid: &str) {
    answer(
        bananablocks_spend_url(DEFAULT_BANANABLOCKS_BASE, txid, 0),
        200,
        r#"{"spent":false}"#,
    );
    answer(
        woc_spent_url(DEFAULT_WOC_BASE, txid, 0),
        429,
        "Too Many Requests",
    );
    answer(bitails_tx_url(DEFAULT_BITAILS_BASE, txid), 200, PRUNED);
}

fn fetcher(counts: &CourierCounters) -> ChainProofFetcher {
    ChainProofFetcher::new(None)
        .with_sources(ProofSources::parse("woc,bananablocks,bitails"))
        .with_budget(200)
        .with_courier_counters(counts.clone())
}

fn reset() {
    courier_counters::clear_cooldowns();
    TEST_HTTP_RESPONSES.with(|r| r.borrow_mut().clear());
    TEST_HTTP_CALLS.with(|c| c.set(0));
}

fn calls() -> u32 {
    TEST_HTTP_CALLS.with(Cell::get)
}

/// One tick's spend asks over 24 outpoints, split over TWO fetchers that share the pass's counters (the tick's
/// spend chaser and its missing-spend discovery each build their own).
async fn one_tick(now: u64) -> (CourierCounters, Vec<Result<Option<String>, String>>) {
    let counts = CourierCounters::paced(Reader::Janitor, now);
    let (a, b) = (fetcher(&counts), fetcher(&counts));
    let mut out = Vec::new();
    for n in 1..=24 {
        let f = if n <= 12 { &a } else { &b };
        out.push(f.resolve_spender(&outpoint(n), 0).await);
    }
    (counts, out)
}

/// FIX 1 and 2 on loop 44's shape. RED with the pacer inert (`Counters::pace` always `Ok`): each fetcher's own
/// circuit asks WoC three times (6 in the tick) and pruned Bitails' `spent: ""` steps no circuit, so it is asked for
/// every outpoint (24): 54 subrequests where the paced tick makes 30.
#[tokio::test]
async fn fix834_a_the_loop_44_fault_shape_is_paced_across_the_ticks_fetchers() {
    reset();
    for n in 1..=25 {
        loop44_outpoint(&outpoint(n));
    }
    let (counts, verdicts) = one_tick(T0).await;
    // A negative still needs a second provider: one clean "unspent", a 429 and a "could not look" decide nothing.
    assert!(verdicts.iter().all(Result::is_err), "{verdicts:?}");
    let view = counts.pacing_view();
    assert_eq!(
        view["woc"]["asked"], 3,
        "WoC backs off at its third 429, over BOTH fetchers: {view}"
    );
    assert_eq!(view["woc"]["backedOff"], true);
    assert_eq!(view["woc"]["skipped"], 21);
    assert_eq!(
        view["bananablocks"]["asked"], 24,
        "the healthy rung takes every ask: {view}"
    );
    assert_eq!(
        view["bitails"]["asked"], 3,
        "pruned Bitails is skipped by construction after 3 non-answers: {view}"
    );
    assert_eq!(view["bitails"]["couldNotLook"], 3);
    assert_eq!(counts.paced_asks(), 30);
    assert_eq!(
        calls(),
        30,
        "the tick's subrequests are the asks the pacer let go"
    );
    let rows = counts.take();
    assert_eq!(rows["courier_janitor_woc_fault_total"], 3);
    assert_eq!(rows["courier_janitor_woc_backoff_total"], 1);
    assert_eq!(rows["courier_janitor_woc_skipped_total"], 21);
    assert_eq!(rows["courier_janitor_bitails_couldnotlook_total"], 3);
    assert_eq!(rows["courier_janitor_bitails_skipped_total"], 21);
    assert!(
        !rows.contains_key("courier_janitor_bitails_fault_total"),
        "a non-answer is never a fault: {rows:?}"
    );
    assert!(
        !rows.contains_key("courier_janitor_bitails_notfound_total"),
        "nor an ask it could answer: {rows:?}"
    );
    assert_eq!(rows["courier_janitor_bananablocks_notfound_total"], 24);

    // The next tick inside the cool-down: WoC is not asked at all; the other rungs are.
    TEST_HTTP_CALLS.with(|c| c.set(0));
    let (counts, verdicts) = one_tick(T0 + 15 * 60 * 1000).await;
    assert!(verdicts.iter().all(Result::is_err));
    let view = counts.pacing_view();
    assert_eq!(view["woc"]["asked"], 0, "cooled down across ticks: {view}");
    assert_eq!(
        view["woc"]["cooledUntilMs"],
        T0 + courier_counters::COURIER_COOLDOWN_MS
    );
    assert_eq!(view["bananablocks"]["asked"], 24);
    assert_eq!(calls(), 27);
    // The next rung takes the ask: with WoC cooled, BananaBlocks' named spender still answers.
    let spender = "ee".repeat(32);
    answer(
        bananablocks_spend_url(DEFAULT_BANANABLOCKS_BASE, &outpoint(25), 0),
        200,
        &format!(r#"{{"spent":true,"spentTxid":"{spender}"}}"#),
    );
    let f = fetcher(&counts);
    assert_eq!(f.resolve_spender(&outpoint(25), 0).await, Ok(Some(spender)));

    // Past the cool-down WoC is asked again (and backs off again on the same 429s).
    TEST_HTTP_CALLS.with(|c| c.set(0));
    let (counts, _) = one_tick(T0 + courier_counters::COURIER_COOLDOWN_MS + 1).await;
    assert_eq!(counts.pacing_view()["woc"]["asked"], 3);
}

/// FIX 1's budget: a provider that has spent its per-tick share is skipped (counted) and the ladder's next rung
/// takes the ask. RED with the pacer inert: WoC asked 70 times.
#[tokio::test]
async fn fix834_b_the_per_tick_budget_hands_the_ask_to_the_next_rung() {
    reset();
    for n in 1..=70 {
        let txid = outpoint(n);
        answer(
            bananablocks_spend_url(DEFAULT_BANANABLOCKS_BASE, &txid, 0),
            200,
            r#"{"spent":false}"#,
        );
        answer(woc_spent_url(DEFAULT_WOC_BASE, &txid, 0), 404, "");
        answer(bitails_tx_url(DEFAULT_BITAILS_BASE, &txid), 200, PRUNED);
    }
    let counts = CourierCounters::paced(Reader::Janitor, T0);
    let f = fetcher(&counts);
    let mut unspent = 0;
    for n in 1..=70 {
        if f.resolve_spender(&outpoint(n), 0).await == Ok(None) {
            unspent += 1;
        }
    }
    let view = counts.pacing_view();
    let budget = courier_counters::COURIER_TICK_BUDGET
        .iter()
        .find(|(p, _)| *p == "woc")
        .unwrap()
        .1;
    assert_eq!(view["woc"]["asked"], budget, "{view}");
    assert_eq!(view["woc"]["skipped"], 70 - budget);
    assert_eq!(
        view["woc"]["backedOff"], false,
        "a spent budget is no back-off"
    );
    assert_eq!(
        unspent, budget,
        "two clean negatives decide; past the budget the second provider is missing"
    );
    assert_eq!(
        view["bitails"]["asked"], 3,
        "the next rung took the ask, then was skipped by construction"
    );
    assert_eq!(
        counts.paced_asks(),
        2 * budget + 3,
        "the pass's subrequests are bounded by the shares"
    );
    assert_eq!(
        view["bananablocks"]["asked"], budget,
        "BananaBlocks' share is spent too"
    );
}

/// FIX 2 (the convention: Bitails' 404 is "could not look", a negative needs a SECOND provider). Unpaced, so it
/// runs on the base as is: RED on `00a5e613`, where pruned Bitails' 404 was a clean negative and, beside one
/// BananaBlocks "unspent" and a WoC 503, decided the outpoint unspent (`Ok(None)`).
#[tokio::test]
async fn fix834_c_a_bitails_spend_404_is_could_not_look_never_a_clean_negative() {
    reset();
    let txid = outpoint(7);
    answer(
        bananablocks_spend_url(DEFAULT_BANANABLOCKS_BASE, &txid, 0),
        200,
        r#"{"spent":false}"#,
    );
    answer(woc_spent_url(DEFAULT_WOC_BASE, &txid, 0), 503, "busy");
    answer(bitails_tx_url(DEFAULT_BITAILS_BASE, &txid), 404, "");
    let counts = CourierCounters::new(Reader::Janitor);
    let f = fetcher(&counts);
    let verdict = f.resolve_spender(&txid, 0).await;
    assert!(
        verdict.is_err(),
        "one capable negative is no verdict: {verdict:?}"
    );
    let rows = counts.take();
    assert_eq!(
        rows.get("courier_janitor_bitails_notfound_total"),
        None,
        "{rows:?}"
    );
    assert_eq!(
        rows.get("courier_janitor_bitails_fault_total"),
        None,
        "{rows:?}"
    );
    assert_eq!(f.rung_stats()["bitails_tx"].fault, 0);
}

/// FIX 2 on the proof ladder. RED on `00a5e613`: Bitails asked for a subject Arcade holds unmined (3 asks, not 2),
/// and its 404 counted `notfound` (an ask it could answer) and a legacy fault.
#[tokio::test]
async fn fix834_d_the_proof_ladder_never_asks_bitails_for_an_unmined_subject() {
    reset();
    let txid = outpoint(9);
    answer(
        format!("{DEFAULT_ARCADE_URL}/tx/{txid}"),
        200,
        r#"{"txStatus":"SEEN_ON_NETWORK","timestamp":"1970-01-01T00:00:00Z"}"#,
    );
    let counts = CourierCounters::new(Reader::Janitor);
    let f = ChainProofFetcher::new(None)
        .with_sources(ProofSources::parse("arcade,bitails,woc"))
        .with_courier_counters(counts.clone());
    assert_eq!(f.fetch_verified_proof_exhaustive(&txid).await, Ok(None));
    assert_eq!(
        calls(),
        2,
        "Arcade, then WoC's node; pruned Bitails cannot see the mempool"
    );
    let rows = counts.take();
    assert_eq!(rows["courier_janitor_bitails_skipped_total"], 1, "{rows:?}");
    assert_eq!(f.proof_tallies.borrow()["bitails"].fault, 0);

    // No word from Arcade (a 404): Bitails is asked, and its 404 for this proofless subject is "could not look".
    reset();
    let txid = outpoint(10);
    let counts = CourierCounters::new(Reader::Janitor);
    let f = ChainProofFetcher::new(None)
        .with_sources(ProofSources::parse("arcade,bitails,woc"))
        .with_courier_counters(counts.clone());
    assert_eq!(f.fetch_verified_proof(&txid).await, Ok(None));
    assert_eq!(calls(), 3);
    let rows = counts.take();
    assert_eq!(
        rows["courier_janitor_bitails_couldnotlook_total"], 1,
        "{rows:?}"
    );
    assert_eq!(rows.get("courier_janitor_bitails_notfound_total"), None);
    assert_eq!(
        f.proof_tallies.borrow()["bitails"].fault,
        0,
        "never a legacy fault either"
    );
    assert_eq!(
        rows["courier_janitor_woc_notfound_total"], 1,
        "WoC's 404 is an answer (its node sees the mempool)"
    );
}

/// The #606 rule on the proof ladder's height read: a body naming the asked txid with no height (WoC's mempool
/// `/tx/hash`) is the subject unmined, never a malformed answer. RED on `00a5e613` (`invalid-height`, a shape fault).
#[tokio::test]
async fn fix834_e_a_mempool_body_without_a_height_is_unmined_not_a_fault() {
    reset();
    let txid = outpoint(11);
    answer(
        format!("{DEFAULT_WOC_BASE}/tx/hash/{txid}"),
        200,
        &format!(r#"{{"txid":"{txid}","hash":"{txid}","size":225,"vin":[],"vout":[]}}"#),
    );
    let counts = CourierCounters::new(Reader::Janitor);
    let f = ChainProofFetcher::new(None)
        .with_sources(ProofSources::parse("woc"))
        .with_courier_counters(counts.clone());
    let probe = f.diagnose_rung("woc", &txid).await;
    assert_eq!(probe.parse_result, "unmined-hint");
    assert!(!probe.fault);
    assert_eq!(f.fetch_verified_proof(&txid).await, Ok(None));
    assert_eq!(
        f.proof_tallies.borrow()["woc"].fault,
        0,
        "never a legacy fault"
    );
    let rows = counts.take();
    assert_eq!(rows["courier_janitor_woc_notfound_total"], 1, "{rows:?}");
    assert_eq!(rows.get("courier_janitor_woc_shape_total"), None);
    // A present height that is not a number, or a body for another txid, is still malformed.
    for body in [
        format!(r#"{{"txid":"{txid}","blockheight":"soon"}}"#),
        format!(r#"{{"txid":"{}"}}"#, outpoint(12)),
    ] {
        answer(format!("{DEFAULT_WOC_BASE}/tx/hash/{txid}"), 200, &body);
        let probe = f.diagnose_rung("woc", &txid).await;
        assert_eq!(probe.parse_result, "invalid-height", "{body}");
        assert!(probe.fault);
    }
}

/// FIX 3: the served rate is the cell's own formula (`fault + shape` over `ok + notfound + fault + shape`), and
/// pruned Bitails' non-answers and the pacer's skips are beside it, outside both terms. Loop 44's janitor figures:
/// WoC 149 of 407, Bitails 143 of 381. RED with `couldnotlook` folded into the asks (Bitails 143 of 881).
#[test]
fn fix834_f_the_served_fault_rate_excludes_could_not_look() {
    let c = serde_json::json!({
        "courier_janitor_woc_ok_total": 250, "courier_janitor_woc_notfound_total": 8,
        "courier_janitor_woc_fault_total": 149, "courier_janitor_woc_skipped_total": 40,
        "courier_janitor_woc_backoff_total": 3,
        "courier_janitor_bitails_ok_total": 238, "courier_janitor_bitails_fault_total": 140,
        "courier_janitor_bitails_shape_total": 3, "courier_janitor_bitails_couldnotlook_total": 500,
        "courier_janitor_bitails_skipped_total": 900,
    });
    let v = crate::ops::courier_pacing_view(&c);
    let p = &v["providers"];
    assert_eq!(p["woc"]["asks"], 407);
    assert_eq!(p["woc"]["faults"], 149);
    assert_eq!(p["woc"]["backoffs"], 3);
    assert_eq!(p["bitails"]["asks"], 381);
    assert_eq!(p["bitails"]["faults"], 143);
    assert_eq!(p["bitails"]["couldNotLook"], 500);
    assert_eq!(p["bitails"]["skipped"], 900);
    let rate = p["bitails"]["faultRate"].as_f64().unwrap();
    assert!((rate - 143.0 / 381.0).abs() < 1e-12, "{rate}");
    assert_eq!(
        p["bananablocks"]["faultRate"],
        serde_json::Value::Null,
        "no ask, no rate"
    );
    assert_eq!(v["backoffAfterFaults"], 3);
    assert_eq!(v["cooldownSecs"], 1200);
    assert_eq!(v["budgetPerTick"]["woc"], 64);
    // Every name the view reads is served from 0.
    let names = courier_counters::all_names();
    for n in [
        "courier_janitor_bitails_couldnotlook_total",
        "courier_janitor_woc_skipped_total",
        "courier_janitor_woc_backoff_total",
    ] {
        assert!(names.iter().any(|x| x == n), "{n}");
    }
}

/// The pacer only paces what it is asked to: unpaced counters (the door's, and every pass not built paced) and a
/// fetcher with no counters never skip, and the shipped tick and admin pass build theirs paced.
#[tokio::test]
async fn fix834_g_only_the_janitors_pass_is_paced() {
    reset();
    for n in 1..=8 {
        loop44_outpoint(&outpoint(n));
    }
    let door = CourierCounters::new(Reader::Door);
    let f = fetcher(&door);
    for n in 1..=8 {
        let _ = f.resolve_spender(&outpoint(n), 0).await;
    }
    assert_eq!(door.pacing_view(), serde_json::Value::Null);
    assert!(door
        .take()
        .keys()
        .all(|k| !k.contains("skipped") && !k.contains("backoff")));
    let tick = include_str!("../tick.rs");
    let lib = include_str!("../lib.rs");
    for (name, src) in [("tick.rs", tick), ("lib.rs", lib)] {
        assert!(
            src.contains("crate::courier_counters::Counters::paced(\n        crate::courier_counters::Reader::Janitor,"),
            "{name} builds the janitor's counters paced"
        );
        assert!(
            !src.contains("Counters::new(crate::courier_counters::Reader::Janitor)"),
            "{name} has no unpaced janitor counters left"
        );
    }
}

/// FIX-834-MERGE (the merged main's budget tier, `janitor-pass-woc-429` RED on `45b83a32`: the stand-in saw WoC 6 and
/// Bitails 24 where the pass's pacer let 3 and 3 go). The caller outside the pacer was the #602 CONFLICT PASS inside
/// the retire pass (`admit_fast::confirmed_conflict_pass`): its own 24-call fetcher and its presence reads carried no
/// counters, so once #837's JOINs gave it inputs to read its own circuit asked a WoC the pass had backed off three
/// more times and pruned Bitails on every candidate. Now the pass's fetcher (`admit_fast::conflict_pass_fetcher`) and
/// its presence (`CourierConflictPresence::counters`) ride the janitor pass's ONE pacer, as the retire pass's own
/// looks do. RED with `conflict_pass_fetcher` building the base's unpaced fetcher (counters ignored): "every
/// subrequest of the pass is one the pacer let go ... left: 37, right: 26" (WoC 3 and Bitails 8 past the pacer).
#[tokio::test]
async fn fix834m_a_the_conflict_pass_asks_through_the_janitor_pacer() {
    use crate::admit_fast::ConflictPresence;
    reset();
    for n in 1..=20 {
        loop44_outpoint(&outpoint(n));
    }
    let counts = CourierCounters::paced(Reader::Janitor, T0);
    // The tick's spend chaser first (it backs WoC off at its third 429), then the retire pass's conflict pass.
    let chaser = fetcher(&counts);
    for n in 1..=12 {
        assert!(chaser.resolve_spender(&outpoint(n), 0).await.is_err());
    }
    let sources = ProofSources::parse("woc,bananablocks,bitails");
    let conflict = crate::admit_fast::conflict_pass_fetcher(sources, None, Some(counts.clone()));
    for n in 13..=20 {
        // one clean "unspent", a skipped WoC and a "could not look" decide nothing: a negative needs a second provider
        assert!(conflict.resolve_spender(&outpoint(n), 0).await.is_err());
    }
    assert_eq!(calls(), 26, "every subrequest of the pass is one the pacer let go (WoC 3, Bitails 3, BananaBlocks 20)");
    let view = counts.pacing_view();
    assert_eq!(view["woc"]["asked"], 3, "the conflict pass asks a backed-off WoC: {view}");
    assert_eq!(view["bitails"]["asked"], 3, "pruned Bitails skipped by construction over both: {view}");
    assert_eq!(view["bananablocks"]["asked"], 20, "the healthy rung takes every ask: {view}");

    // The conflict pass's presence reads: WoC backed off is not asked; Bitails' presence is its own rung (the proof
    // ladder's could-not-look cap does not blind the both-indexer reading the verdicts take), still paced.
    let presence = crate::admit_fast::CourierConflictPresence {
        sources,
        woc_api_key: None,
        counters: Some(counts.clone()),
    };
    TEST_HTTP_CALLS.with(|c| c.set(0));
    let _ = presence.presence(&outpoint(1)).await;
    assert_eq!(calls(), 1, "Bitails asked, WoC skipped");
    let view = counts.pacing_view();
    assert_eq!(view["woc"]["asked"], 3, "{view}");
    assert_eq!(view["bitails"]["asked"], 4, "{view}");

    // The retire pass's own looks take the same pacer: WoC's presence skipped, Arcade's look paced and counted.
    TEST_HTTP_CALLS.with(|c| c.set(0));
    assert!(sources.woc_presence_counted(None, &outpoint(2), Some(&counts)).await.is_none());
    assert_eq!(calls(), 0, "a backed-off WoC presence read makes no subrequest");
    let arcade = "https://arcade.example";
    answer(format!("{arcade}/tx/{}", outpoint(2)), 404, "not found");
    let look = ProofSources::parse("arcade")
        .arcade_look_counted(arcade, &outpoint(2), None, Some(&counts))
        .await;
    assert_eq!(look, ArcadeLook::Missing);
    assert_eq!(counts.pacing_view()["arcade"]["asked"], 1);
    assert_eq!(counts.take()["courier_janitor_arcade_notfound_total"], 1);
}

/// FIX-834-MERGE (structural): every courier look of the janitor's retire pass is wired to the pass's counters, at
/// both entrances (the scheduled tick and `/admin/complete-proofs`), and the rebroadcast backstop's too; the
/// operator's `/internal/retire-pass` lever alone runs unpaced. RED on `45b83a32` (no counters argument).
#[test]
fn fix834m_b_the_retire_pass_and_its_callers_carry_the_pacer() {
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let proof = squash(include_str!("../proof_fetcher.rs"));
    let pass = &proof[proof.find("pub(crate)asyncfnrun_retire_pass(").expect("the pass")..];
    let pass = &pass[..pass.find("letmutsummary=RetireSummary").expect("its head")];
    assert!(pass.contains("counters:Option<CourierCounters>,"), "{pass}");
    assert!(pass.contains("counters:counters.clone(),"), "the retire port");
    assert!(
        pass.contains("crate::admit_fast::confirmed_conflict_pass(db,woc_api_key,sources,counters).await;"),
        "the conflict pass"
    );
    let port = &proof[proof.find("implRetirePortforWorkerRetire<'_>{").expect("the port")..];
    let port = &port[..port.find("asyncfnlatch(").expect("its looks")];
    for look in [
        ".arcade_look_counted(self.arcade_base,txid,evidence,self.counters.as_ref())",
        // bsv-low #868: the mempool witness beside WoC's node (Bitails, else BananaBlocks), paced the same way
        ".mempool_witness_presence_counted(txid,self.counters.as_ref())",
        ".woc_presence_counted(self.woc_api_key,txid,self.counters.as_ref())",
    ] {
        assert!(port.contains(look), "{look}");
    }
    let admit = squash(include_str!("../admit_fast.rs"));
    assert!(admit.contains("fetcher:conflict_pass_fetcher(sources,woc_api_key,counters.clone()),"));
    let callers = squash(include_str!("../lib.rs")) + &squash(include_str!("../tick.rs"));
    assert_eq!(
        callers.matches("crate::lookup_service_chain_tracker(env),Some(courier_counts.clone()),).await;").count(),
        2,
        "both janitor entrances pace the retire pass"
    );
    assert_eq!(
        callers.matches("crate::proof_fetcher::ProofSources::from_env(env),Some(&courier_counts),)").count(),
        2,
        "both janitor entrances pace the rebroadcast backstop"
    );
    assert!(proof.contains("crate::lookup_service_chain_tracker(env),None,),).await;"), "the lever runs unpaced");
}
