//! bsv-low #838, FIX-838-FOLD: the FIX-838 lens's folds (`docs/audit/FIX-838-lens-2026-10-22.md` in bsv-low),
//! pinned over the shipped sources. The route and its background run on wasm only (the gated arm needs Arcade), so
//! these read the code; the decisions they wire are driven natively in `ef_deferred::fix838` (`fix838f_*`). Each
//! test here compiles on 53119867 and is RED there by its own assertion.

fn code(src: &str) -> &str {
    &src[..src.find("#[cfg(test)]").unwrap_or(src.len())]
}

fn body<'a>(src: &'a str, head: &str) -> &'a str {
    let s = &src[src
        .find(head)
        .unwrap_or_else(|| panic!("`{head}` is in the source"))..];
    &s[..s.find("\n}\n").unwrap()]
}

/// MEDIUM-1: a past-deadline write whose report is not durable takes S2's decision exactly as the request does:
/// its idempotent replay is enqueued marked `gated` (the network accepted these bytes at the door), counted by the
/// request's three S2 counters; the guard runs first, as in the request; and the route hands the background the
/// bytes and the mode it needs. A write the platform cuts (no report) is the deferred job's, which the request put
/// at rest before its 202.
#[test]
fn fix838f_m1_a_past_deadline_write_that_faults_enqueues_its_s2_replay() {
    let routes = code(include_str!("routes.rs"));
    let f = body(routes, "async fn finish_past_deadline<");
    let ack = f
        .find("crate::ef_deferred::past_deadline_ack(")
        .expect("MEDIUM-1: the past-deadline write's report takes S2's decision");
    assert!(
        f.contains("crate::queue::enqueue_replay(&env, &tagged.beef, &tagged.topics, mode, true)"),
        "the replay is the request's, marked gated"
    );
    for counter in [
        "crate::ops::COUNTER_SUBMIT_MUTATION_FAULT",
        "crate::ops::COUNTER_SUBMIT_MUTATION_QUEUED",
        "crate::ops::COUNTER_SUBMIT_MUTATION_REFUSED",
    ] {
        assert!(
            f.contains(counter),
            "counted as the request counts: {counter}"
        );
    }
    let guard = f
        .find("crate::admit_fast::open_eviction(&db, &subject).await")
        .expect("the write-side guard");
    assert!(guard < ack, "the guard, then S2, the request's order");
    assert!(
        !f.contains("Nothing is enqueued here"),
        "the job is the second cover, not the only one"
    );
    let parts = body(routes, "async fn submit_parts(");
    // bsv-low #936's fold: the background is the write's own task, its after-write `finish_past_deadline`
    let hand = &parts[parts
        .find("let (after_env, after_tagged, after_subject) =")
        .unwrap()..];
    let hand = &hand[..hand.find("ctx.wait_until(task);").unwrap()];
    assert!(
        hand.contains("tagged_beef.clone(),")
            && hand.contains("finish_past_deadline(")
            && hand.contains("mode,"),
        "the background is handed the bytes and the mode: {hand}"
    );
    // the job is at rest before the 202: a dropped isolate leaves it
    let me = code(include_str!("ef_deferred.rs"));
    let d = body(me, "pub async fn defer_past_deadline<");
    assert!(d.find("put_at_rest(").unwrap() < d.find("deferral_answer(").unwrap());
}

/// MEDIUM-2: the bound the row states is the bound the route holds. The race covers the engine write's wait and the
/// put; every statement the request awaits after a write that landed inside the deadline (until the S2 decision)
/// is named in `ceilings.json` `doorAnswer.postWriteInRequest`, and the row no longer states an answer ceiling.
#[test]
fn fix838f_m2_the_rows_bound_is_what_the_route_holds() {
    let ceilings: serde_json::Value =
        serde_json::from_str(include_str!("../../../tools/lane-499/ceilings.json")).unwrap();
    let row = &ceilings["doorAnswer"];
    assert!(
        row.get("ceilingMs").is_none(),
        "MEDIUM-2: the row states no answer ceiling the route does not hold"
    );
    assert_eq!(row["raceBoundMs"], 16_000);
    let doc = row["doc"].as_str().unwrap();
    assert!(doc.contains("The race covers the ENGINE WRITE'S WAIT only"));
    assert!(row["preEngineInRequest"].as_str().is_some());
    let named: Vec<&str> = row["postWriteInRequest"]
        .as_array()
        .expect("the post-write statements are named")
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    let routes = code(include_str!("routes.rs"));
    let parts = body(routes, "async fn submit_parts(");
    let from = parts
        .find("let (steak, mutation_report) = match engine_outcome {")
        .unwrap();
    let to = parts
        .find("let mutation_queued = match crate::queue::mutation_ack(")
        .unwrap();
    let tail = &parts[from..to];
    let mut found = Vec::new();
    let mut at = 0;
    while let Some(i) = tail[at..].find("crate::") {
        let i = at + i;
        at = i + 7;
        let call = &tail[i..];
        let Some(paren) = call.find('(') else { break };
        let path = &call[..=paren];
        let fn_name = path.rsplit("::").next().unwrap();
        if !fn_name.starts_with(|c: char| c.is_ascii_lowercase())
            || path.contains(|c: char| c.is_whitespace())
        {
            continue;
        }
        let statement = &call[..call.find(';').unwrap_or(call.len())];
        let before = &tail[i.saturating_sub(120)..i];
        if statement.contains(".await") && !before.contains("wait_until(") {
            found.push(path.to_string());
        }
    }
    assert!(
        found.len() >= 8,
        "the post-write awaits are read: {found:?}"
    );
    for path in &found {
        assert!(
            named.contains(&path.as_str()),
            "{path} is awaited in the request after the write and named outside the bound"
        );
    }
    for belt in [
        "SELECT 1 AS one FROM transactions WHERE txid = lower(?)",
        "SELECT 1 AS one FROM pot_records WHERE txid = lower(?) LIMIT 1",
    ] {
        assert!(parts.contains(belt));
        assert!(named.iter().any(|n| n.ends_with(belt)), "{belt} named");
    }
}

/// LOW-1: a RESUMED run (the deferred job) that the stored echo under an OPEN eviction refuses (#612) runs the
/// write-side guard's re-evict, so rows a write the platform cut left after the eviction's table loop do not stand
/// under the open row. The request's own arm is unchanged (its write never ran).
#[test]
fn fix838f_l1_a_resumed_run_refused_by_the_stored_echo_re_evicts() {
    let broadcaster = code(include_str!("broadcaster.rs"));
    assert!(
        broadcaster.contains("return Err(STORED_ECHO_UNDER_EVICTION.into());"),
        "the stored echo's refusal has one name"
    );
    let routes = code(include_str!("routes.rs"));
    let parts = body(routes, "async fn submit_parts(");
    let arm = &parts[parts.find("Err(transport) => {").unwrap()..];
    let arm = &arm[..arm
        .find("return Ok(with_server_timing(resp, &gated_timing));")
        .unwrap()];
    let resumed = arm
        .find("matches!(budget, crate::ef_deferred::WorkBudget::Resumed { .. })")
        .expect("LOW-1: the resumed run's arm");
    let echo = arm
        .find("transport == crate::broadcaster::STORED_ECHO_UNDER_EVICTION")
        .expect("on the stored echo alone");
    let read = arm
        .find("crate::admit_fast::open_eviction(&ledger_db, &subject_txid).await")
        .expect("the open row read again");
    let evict = arm
        .find("crate::admit_fast::evict_txid_everywhere(")
        .expect("the re-evict");
    let notes = arm
        .find("crate::pot_changes::flush_inline(env.clone()).await;")
        .expect("the early 502 ships the eviction's notes");
    assert!(resumed < echo && echo < read && read < evict && evict < notes);
}

/// LOW-2: past the deadline a faulted door readmission is retried as the request retries it (the refused memo
/// read, then the second readmission on a stale open row, or the re-evict on a fresh refusal), never skipped.
#[test]
fn fix838f_l2_a_faulted_door_readmission_is_retried_past_the_deadline() {
    let routes = code(include_str!("routes.rs"));
    let f = body(routes, "async fn finish_past_deadline<");
    assert!(
        !f.contains("if !door_readmit_failed {"),
        "LOW-2: the guard is not skipped on a faulted door readmission"
    );
    let memo = f
        .find("crate::admit_fast::refused_memo_exists(&db, &subject).await")
        .expect("the refused memo read");
    let open = f
        .find("crate::admit_fast::open_eviction(&db, &subject).await")
        .unwrap();
    let again = f
        .find("Ok(Some(ev)) if door_readmit_failed && !fresh_refusal => {")
        .expect("the request's arm");
    let readmit = f
        .find("crate::admit_fast::readmit_if_evicted(")
        .expect("the second readmission");
    let evict = f.find("crate::admit_fast::evict_txid_everywhere(").unwrap();
    assert!(memo < open && open < again && again < readmit && readmit < evict);
    // the request's arm, for the same order
    let parts = body(routes, "async fn submit_parts(");
    assert!(parts.contains("Ok(Some(ev)) if door_readmit_failed && !fresh_refusal => {"));
}

/// LOW-3: the deferral's log line names its cause, and the past-deadline 202 is counted apart (seeded at 0).
#[test]
fn fix838f_l3_the_deferral_names_its_cause_and_is_counted_apart() {
    let me = code(include_str!("ef_deferred.rs"));
    let d = body(me, "pub async fn defer_past_deadline<");
    assert!(
        d.contains("DeferralCause::AnswerDeadline"),
        "LOW-3: the past-deadline deferral names its cause"
    );
    let a = body(me, "fn deferral_answer(");
    assert!(a.contains("(cause {};") && a.contains("cause.word(),"));
    let ops = code(include_str!("ops.rs"));
    assert!(ops.contains(
        "pub const COUNTER_SUBMIT_DEFERRED_PAST_DEADLINE: &str = \"submit_deferred_past_deadline_total\";"
    ));
    let seeded = body(ops, "async fn read_counters(");
    assert!(seeded.contains("COUNTER_SUBMIT_DEFERRED_PAST_DEADLINE,"));
    let routes = code(include_str!("routes.rs"));
    let parts = body(routes, "async fn submit_parts(");
    let past = &parts[parts
        .find("crate::ef_deferred::defer_past_deadline(")
        .unwrap()..];
    assert!(past[..past.find("return answer;").unwrap()]
        .contains("counters.bump(crate::ops::COUNTER_SUBMIT_DEFERRED_PAST_DEADLINE, 1);"));
}
