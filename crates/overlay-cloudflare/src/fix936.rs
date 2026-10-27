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

/// The request gone: the write's own task runs what the request would have run after it (the write-side guard,
/// the S2 decision, the notes; `finish_past_deadline` under the cause `RequestGone`), and the moment the request is
/// gone with the write still running, the submission is put at rest as the deferred job (the cover of a task the
/// platform itself cuts), as #838 defers a write past its deadline.
#[test]
fn fix936_b_a_gone_request_has_its_after_write_and_its_deferred_job() {
    let routes = code(include_str!("routes.rs"));
    let w = gated_write(routes);
    assert!(
        w.contains("crate::ef_deferred::defer_request_gone("),
        "#936: the cover is the deferred job, put at rest when the request goes"
    );
    assert!(
        w.contains("crate::ef_deferred::AfterAnswer::RequestGone"),
        "#936: the orphaned write's after-write is finish_past_deadline's, named"
    );
    assert!(
        w.contains("crate::ef_deferred::AfterAnswer::PastDeadline"),
        "#838's path keeps its own cause"
    );
}
