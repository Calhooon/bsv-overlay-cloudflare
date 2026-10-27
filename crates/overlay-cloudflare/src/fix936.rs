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
