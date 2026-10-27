//! bsv-low #936 (READ-936, loop 52's class S4): a client that goes away during the gated arm's engine write
//! (the invocation `canceled`) must never cut that write between two of its statements. On eaaee0ec the request
//! awaited the write itself (`within_deadline(write, deadline)` on the bare write), so the platform's cancel of the
//! request dropped it after `insert_output` and before the lookup services' `output_admitted_by_topic`: the JOIN
//! `fad01028…` held a `tm_pot` coin with no `pot_records` and no `pot_beefs` row, and the refund's admission then
//! recorded the JOIN applied and deleted the coin (the pot left the index with no eviction record). The route runs
//! on wasm only, so this reads the code; the ownership it wires is driven natively in `owned_write::tests`. Each
//! test here compiles on eaaee0ec and is RED there by its own assertion.

fn code(src: &str) -> &str {
    &src[..src.find("#[cfg(test)]").unwrap_or(src.len())]
}

fn gated_write(routes: &str) -> &str {
    let s = &routes[routes
        .find("let engine_outcome = match (gated_subject.as_deref(), gated_work, budget) {")
        .expect("the gated arm's engine write")..];
    &s[..s
        .find("let (steak, mutation_report) = match engine_outcome {")
        .unwrap()]
}

/// The gated write is owned by `ctx.wait_until` from its first statement and the request only waits on its
/// answer: the request's `within_deadline` races the owned handle, never the bare write.
#[test]
fn fix936_a_the_gated_write_is_owned_by_wait_until_never_by_the_request() {
    let routes = code(include_str!("routes.rs"));
    let w = gated_write(routes);
    let owned = w
        .find("crate::owned_write::owned_write(")
        .expect("#936: the gated write is created owned, outside the request's future");
    let spawned = w
        .find("ctx.wait_until(task);")
        .expect("#936: the owned write is driven by the platform's wait_until");
    let raced = w
        .find("within_deadline(handle, deadline)")
        .expect("#936: the request races the handle, not the write");
    assert!(
        owned < spawned && spawned < raced,
        "owned, then spawned, then raced"
    );
    assert!(
        !w.contains("within_deadline(write, deadline)"),
        "#936: the request never races (and so never owns) the bare write"
    );
}

/// The FIX-936 lens's fold: the write's own task runs the after-write (`finish_past_deadline`, the guard, S2, the
/// notes) on EVERY gated write, named by the cause the task reads (never a literal `RequestGone` the request's drop
/// chose); the cover (the deferred job) is started from the request's `AbortSignal`, never from `Handle::drop`; and
/// the request past its deadline only marks the handle (no second after-write spawned by the request).
#[test]
fn fix936_b_the_task_runs_the_after_write_and_the_abort_starts_the_cover() {
    let routes = code(include_str!("routes.rs"));
    let w = gated_write(routes);
    let owned = &w[w.find("crate::owned_write::owned_write(").unwrap()..];
    let owned = &owned[..owned.find("ctx.wait_until(task);").unwrap()];
    let gone = owned
        .find("request_signal.gone(),")
        .expect("#936 fold: the going is the request's AbortSignal");
    let cover = owned
        .find("crate::ef_deferred::defer_request_gone(")
        .expect("#936: the cover is the deferred job");
    let after = owned
        .find("finish_past_deadline(")
        .expect("#936 fold: the task's own after-write");
    assert!(
        gone < cover && cover < after,
        "the signal, the cover, the after-write"
    );
    assert!(
        owned.contains("cause,") && !owned.contains("AfterAnswer::RequestGone"),
        "#936 fold: the cause is the task's reading, never chosen by a drop"
    );
    assert!(
        !w.contains("ctx.wait_until(finish_past_deadline("),
        "#936 fold: past the deadline the request spawns no second after-write"
    );
    assert!(w.contains("handle.past_deadline();"));
    let ow = code(include_str!("owned_write.rs"));
    assert!(
        !ow.contains("impl<T> Drop for Handle<T>"),
        "#936 fold: nothing hangs on a Drop the platform never makes"
    );
    let parts = &routes[routes.find("async fn submit_inner(").unwrap()..];
    assert!(parts.contains("request_signal: crate::owned_write::RequestSignal::of(&req),"));
}

/// The FIX-936 lens's LOW-2 and MEDIUM-2: every line of the after-write names its cause, and the counter L53 reads
/// is bumped by the task on a request gone.
#[test]
fn fix936_h_every_after_write_line_names_its_cause() {
    let routes = code(include_str!("routes.rs"));
    let f = &routes[routes.find("async fn finish_past_deadline<").unwrap()..];
    let f = &f[..f.find("\n}\n").unwrap()];
    assert!(
        !f.contains("past-deadline"),
        "LOW-2: no line says past-deadline for every cause"
    );
    let lines = f.matches("worker::console_log!(").count();
    assert!(lines >= 9);
    assert_eq!(
        f.matches("{word}").count(),
        lines,
        "LOW-2: one cause word on each line"
    );
    let gone = f
        .find("if cause == crate::ef_deferred::AfterAnswer::RequestGone {")
        .unwrap();
    assert!(f[gone..].starts_with(
        "if cause == crate::ef_deferred::AfterAnswer::RequestGone {\n        crate::ops::bump_counter(&db, crate::ops::COUNTER_SUBMIT_WRITE_OUTLIVED_REQUEST, 1).await;"
    ));
}

/// The `enable_request_signal` compatibility flag (no default date: Cloudflare's compatibility flags page) is set in
/// every overlay render, so the request's `AbortSignal` fires when its client goes away.
#[test]
fn fix936_i_the_request_signal_flag_is_in_every_render() {
    let flag = "compatibility_flags = [\"enable_request_signal\"]";
    let low = include_str!("../wrangler.low.toml");
    let beta = low.find("\n[env.beta]\n").expect("the beta render");
    let top = &low[..beta];
    assert!(
        top.contains(&format!("\n{flag}\n")),
        "wrangler.low.toml prod: {flag}"
    );
    let beta = &low[beta..];
    let beta = &beta[..beta[1..].find("\n[").map_or(beta.len(), |i| i + 1)];
    assert!(
        beta.contains(&format!("\n{flag}\n")),
        "wrangler.low.toml beta: {flag}"
    );
    assert!(
        include_str!("../wrangler.toml").contains(&format!("\n{flag}\n")),
        "wrangler.toml: {flag}"
    );
}

/// FIX-936-REBASE (PR 183 under PR 188): bsv-low #920's landing guard (the re-eviction stamped with the evidence's
/// start, `settle_reevicted`, the `admit_fast_reevicted_after_write_total` count) runs ONCE per landing of a gated
/// owned write, in the write's OWN task (`finish_past_deadline`, under every cause, its count not gated on the
/// cause), and the request's copy beside it under `InRequest` reads the open row only to answer its 422. Every
/// other arm (an ungated write, a deferred job's run) keeps the request's full guard. A source pin: the route runs
/// on wasm only. RED against the guard run in both copies (the request's `task_guards` branch removed) and against the
/// task's count gated on `counts` again.
#[test]
fn fix936_j_the_landing_guard_runs_once_in_the_writes_own_task() {
    let routes = code(include_str!("routes.rs"));
    let f = &routes[routes.find("async fn finish_past_deadline<").unwrap()..];
    let f = &f[..f.find("\n}\n").unwrap()];
    let arm = &f[f
        .find("evict_txid_everywhere(\n                &db,\n                &subject,\n                &ev.reason,\n                ev.evicted_at_ms,")
        .expect("the task's re-eviction, stamped with the evidence's start")..];
    let arm = &arm[..arm.find("reevicted = true;").unwrap()];
    assert!(
        arm.contains("settle_reevicted("),
        "the task's re-eviction is settled (#920)"
    );
    assert!(
        arm.contains("bump_counter(&db, crate::ops::COUNTER_ADMIT_FAST_REEVICTED_AFTER_WRITE, 1)"),
        "the task counts its re-eviction"
    );
    assert!(
        !arm.contains("if counts"),
        "the task's landing guard and its count run under every cause (the request's copy no longer counts it)"
    );

    let w = gated_write(routes);
    let spawned = w.find("ctx.wait_until(task);").unwrap();
    let set = w
        .find("task_guards = true;")
        .expect("the owned arm marks that its task guards the landing");
    assert!(
        spawned < set,
        "marked once the task is handed to the platform"
    );

    let post = &routes[routes
        .find("let (steak, mutation_report) = match engine_outcome {")
        .unwrap()..];
    let arm = post
        .find("Ok(Some(ev)) => {\n                    if task_guards {")
        .expect("the request's copy asks whether the task guards the landing");
    let split = arm + post[arm..].find("} else {").unwrap();
    let read_only = &post[arm..split];
    let full = split
        + post[split..]
            .find("everywhere(&ledger_db, subject, &ev.reason, ev.evicted_at_ms)")
            .expect("the request's full guard for every other arm");
    for needle in [
        "evict_txid_everywhere",
        "settle_reevicted",
        "COUNTER_ADMIT_FAST_REEVICTED_AFTER_WRITE",
    ] {
        assert!(
            !read_only.contains(needle),
            "the request's copy beside the task does not run the landing guard ({needle})"
        );
    }
    let answer = &post[full..];
    let answer = &answer[..answer.find("Ok(None) => {}").unwrap()];
    assert!(
        answer.contains("evicted_after_write_message(word), 422"),
        "both branches answer the refusal, never a 200 over an open eviction"
    );
}
