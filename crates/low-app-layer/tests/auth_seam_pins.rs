//! #318 — structural pins on the auth seam (built per Rule 9: positive
//! counts, comments stripped, whitespace-normalized needles assembled at
//! runtime so no needle can match itself; RED-verified with
//! differently-spelled injections per Rule 12a).
//!
//! What these pin (and their modelling boundary, Rule 17): source-structure
//! properties of the identity plumbing that no native behavioral test can
//! reach (the worker router only runs on wasm). Each pin is a POSITIVE count
//! so a rotted needle fails loudly instead of passing vacuously. They are
//! deliberately about the CONSTRUCT (a call expression, a field init), never
//! a region.

use std::fs;
use std::path::Path;

/// Strip `//`/`//!`/`///` comments and string-collapse whitespace, so a pin
/// can neither be satisfied by prose (Rule 9: "the scanner counts PROSE")
/// nor broken by `cargo fmt` re-wrapping.
fn normalized_code(path: &str) -> String {
    let src = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(path))
        .unwrap_or_else(|e| panic!("read {path}: {e}"));
    let mut out = String::new();
    for line in src.lines() {
        // Strip line comments. (Block comments are not used for code-bearing
        // regions in this crate; a `/*` inside a string literal would be the
        // only false positive and none exists in these files.)
        let code = match line.find("//") {
            Some(i) => &line[..i],
            None => line,
        };
        out.push_str(code);
        out.push(' ');
    }
    // Collapse ALL whitespace so wrapping/indentation cannot rot a needle.
    out.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace(' ', "")
}

fn count(haystack: &str, needle: &str) -> usize {
    haystack.match_indices(needle).count()
}

/// Every one of the SIX identity-scoped handlers resolves identity through
/// THE seam — `view_identity(&req, &ctx)` — and nothing else in routes.rs
/// parses `?identity=` (the parse lives INSIDE the seam, so a handler cannot
/// re-choose; Rule 15).
#[test]
fn six_handlers_use_the_identity_seam_and_no_handler_parses_identity_itself() {
    let routes = normalized_code("src/routes.rs");

    // Needle assembled from halves so this test file never contains it whole
    // (Rule 9: "the needle matches ITSELF").
    let seam_call = ["view_identity(", "&req,&ctx)"].concat();
    assert_eq!(
        count(&routes, &seam_call),
        6,
        "exactly the six identity-scoped handlers (/results, /refund-view, \
         /live-view, /recovery-view, /hops-view, and since 2026-09-03 \
         /refund-backups — bsv-low W2 batch B(a)) must call the seam"
    );

    // The ?identity= query parse exists ONCE — inside view_identity itself.
    let parse = ["k==", "\"identity\""].concat();
    assert_eq!(
        count(&routes, &parse),
        1,
        "the ?identity= parse must live only inside the seam"
    );

    // And the seam derives its answer from the ONE decision function.
    let decision = ["resolve_view_identity(", "ctx.data.mode,&ctx.data.caller"].concat();
    assert_eq!(
        count(&routes, &decision),
        1,
        "the seam must derive from auth::resolve_view_identity(mode, caller, query)"
    );
}

/// Rule 8b pin: NO request header is ever read in this crate — the verified
/// identity travels in-process only (router data), so a forged
/// `X-Identity-Key` (or any header) cannot become an identity in either
/// mode. Pinned as a POSITIVE count of the one benign `.headers().get(`
/// call that exists (a RESPONSE Content-Length read in the live-view tower
/// fan-out) — any NEW header read anywhere in src/ moves the count and
/// fails this pin loudly, forcing a review.
#[test]
fn no_request_header_is_ever_read_as_an_identity() {
    let all_src = [
        "src/auth.rs",
        "src/compaction.rs",
        "src/cors.rs",
        "src/hops_view.rs",
        "src/lib.rs",
        "src/live_view.rs",
        "src/logic.rs",
        "src/refund_view.rs",
        "src/results.rs",
        "src/routes.rs",
        "src/txany.rs",
    ];
    let mut header_reads = 0usize;
    let mut identity_header_mentions = 0usize;
    for path in all_src {
        let code = normalized_code(path);
        header_reads += count(&code, &[".headers().", "get("].concat());
        // The forged-header class by NAME, any spelling that survives
        // whitespace-stripping ("X-Identity-Key", "x-identity-key", …).
        identity_header_mentions += count(&code.to_ascii_lowercase(), "identity-key\"");
    }
    assert_eq!(
        header_reads, 1,
        "exactly ONE .headers().get( call may exist in src/ (the live-view \
         response Content-Length budget); a new header read must be reviewed \
         against Rule 8b and this count updated deliberately"
    );
    assert_eq!(
        identity_header_mentions, 0,
        "no identity-key header literal may appear in code (the CORS list \
         uses the middleware's auth_headers constants, not literals)"
    );
}

/// The commit-message claim made executable (Rule 10): the middleware is
/// ALWAYS run with `allow_unauthenticated: false` — lenient-ness lives in
/// the pure disposition, so an attempted-but-invalid auth can never be
/// silently downgraded to anonymous in either mode.
#[test]
fn middleware_always_runs_with_allow_unauthenticated_false() {
    let auth = normalized_code("src/auth.rs");
    let strict_opt = ["allow_unauthenticated:", "false"].concat();
    let lenient_opt = ["allow_unauthenticated:", "true"].concat();
    assert_eq!(
        count(&auth, &strict_opt),
        1,
        "exactly one options site, false"
    );
    assert_eq!(
        count(&auth, &lenient_opt),
        0,
        "no allow_unauthenticated:true site"
    );
    // And process_auth is invoked exactly once (the single front door).
    assert_eq!(
        count(&auth, "process_auth(req,env,&opts)"),
        1,
        "one process_auth call site — the front door"
    );
}

/// The public-route exemption is wired at the REAL producer (Rule 6b): the
/// front door computes the disposition against `effective_mode(global, path)`,
/// not the raw global — so strict enforcement can only ever reach an identity
/// route. Pinned as the actual expression the front door dispatches on.
#[test]
fn front_door_dispatches_on_the_effective_per_route_mode() {
    let auth = normalized_code("src/auth.rs");
    // The front door derives the effective mode from the global + the path…
    assert_eq!(
        count(&auth, "letmode=effective_mode(global_mode,&path);"),
        1,
        "the front door must compute the effective (per-route) mode"
    );
    // …and dispatches the disposition on THAT mode, never the raw global.
    assert_eq!(
        count(
            &auth,
            "front_door_disposition(mode,auth_configured,auth_attempted)"
        ),
        1,
        "the disposition must run on the effective mode"
    );
    assert_eq!(
        count(&auth, "front_door_disposition(global_mode,"),
        0,
        "the disposition must NEVER run on the raw global mode (that would gate public routes)"
    );
    // effective_mode only widens to strict for the identity routes.
    assert_eq!(
        count(&auth, "route_requires_identity_auth(path)"),
        1,
        "effective_mode must key the exemption on route_requires_identity_auth"
    );
}

/// `CallerAuth::Verified` is constructed from the middleware's verified
/// result and NOWHERE else: one `CallerAuth::verified(` call in src/, and
/// its argument is the middleware context's identity key.
#[test]
fn verified_identity_has_exactly_one_producer_the_middleware_result() {
    // SHIPPED code only: auth.rs's own #[cfg(test)] module legitimately
    // constructs CallerAuth::verified fixtures, so cut at the test-module
    // marker — and assert the marker exists so this scoping can never rot
    // silently (the region is "everything that ships", which is exactly the
    // claim: no shipped producer other than the front door).
    let marker = ["#[cfg(", "test)]"].concat();
    let mut calls = 0usize;
    for path in ["src/auth.rs", "src/lib.rs", "src/routes.rs"] {
        let code = normalized_code(path);
        let shipped = match code.find(&marker) {
            Some(i) => &code[..i],
            None => &code[..],
        };
        calls += count(shipped, &["CallerAuth::", "verified("].concat());
    }
    assert!(
        normalized_code("src/auth.rs").contains(&marker),
        "auth.rs must still carry its test module — the scoping above depends on it"
    );
    assert_eq!(
        calls, 1,
        "exactly one CallerAuth::verified( producer may exist in shipped \
         worker code (the front door's Authenticated arm)"
    );
    let auth = normalized_code("src/auth.rs");
    assert_eq!(
        count(
            &auth,
            &["CallerAuth::verified(", "&context.identity_key)"].concat()
        ),
        1,
        "and it must be fed by the middleware's verified context identity"
    );
}

/// Rule 16 (the boundary you publish): the deploy config and the code agree
/// on the contract names — the enforcement var and the session-store binding
/// exist in BOTH `wrangler.toml` and the code that reads them. A rename on
/// either side fails here, not in production.
#[test]
fn wrangler_config_and_code_agree_on_auth_contract_names() {
    let auth = normalized_code("src/auth.rs");
    let wrangler = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("wrangler.toml"))
        .expect("read wrangler.toml");

    for name in ["AUTH_ENFORCE", "SERVER_PRIVATE_KEY", "AUTH_SESSIONS"] {
        assert!(
            wrangler.contains(name),
            "wrangler.toml must declare/document {name}"
        );
    }
    assert_eq!(count(&auth, "env.var(\"AUTH_ENFORCE\")"), 1);
    assert_eq!(count(&auth, "\"SERVER_PRIVATE_KEY\""), 1);
    assert_eq!(count(&auth, "env.kv(\"AUTH_SESSIONS\")"), 1);
    // The middleware reads the SAME binding name internally (its
    // process_auth builds KvSessionStorage from "AUTH_SESSIONS") — pinned
    // there by the middleware crate; here we pin that our configured-check
    // probes the identical name, so "configured" cannot drift from what the
    // middleware will actually find.
}

/// Rule 13 made executable: every front-door outcome COUNTS itself — the
/// lenient accept is "accepted and counted", never a silent accept, and the
/// refusal paths are observable numbers on /health. Anchored on the arm
/// construct (the match arm plus its first statement), not a region.
#[test]
fn every_front_door_outcome_is_counted() {
    // Shipped code only — auth.rs's own test module also exercises the
    // count fns (same scoping rationale + marker guard as the
    // one-producer pin above).
    let marker = ["#[cfg(", "test)]"].concat();
    let full = normalized_code("src/auth.rs");
    assert!(
        full.contains(&marker),
        "auth.rs must still carry its test module"
    );
    let auth = full[..full.find(&marker).unwrap()].to_string();
    let arms = [
        // The anonymous arm counts with the per-route split (route_idx).
        [
            "Disposition::ProceedAnonymous=>{",
            "count_anonymous_served(route_idx);",
        ]
        .concat(),
        [
            "Disposition::RefuseUnauthenticated=>{",
            "count_strict_refused_unauthenticated();",
        ]
        .concat(),
        [
            "Disposition::RefuseMisconfigured=>{",
            "count_misconfigured_refused();",
        ]
        .concat(),
    ];
    for arm in &arms {
        assert_eq!(
            count(&auth, arm),
            1,
            "front-door arm must count itself: {arm}"
        );
    }
    // The authenticated path counts once (the Authenticated+session arm)…
    assert_eq!(count(&auth, "count_authenticated_served();"), 1);
    // …and middleware refusals are counted at each refusal site (Err, the
    // contract-break 500, and the non-handshake Response refusal).
    assert_eq!(count(&auth, "count_auth_refused();"), 3);
}

/// The pinned needles genuinely match the current source (Rule 9's
/// "assert the needle count is what you expect" applied to the helper
/// itself): normalization keeps code and strips prose.
#[test]
fn normalizer_strips_comments_and_whitespace() {
    // Self-check on a synthetic sample via the real helper's core steps.
    let sample = "let x = 1; // comment with view_identity(&req, &ctx)\nlet y=2;";
    let code: String = sample
        .lines()
        .map(|l| match l.find("//") {
            Some(i) => &l[..i],
            None => l,
        })
        .collect::<Vec<_>>()
        .join(" ");
    let collapsed = code
        .split_whitespace()
        .collect::<Vec<_>>()
        .join("")
        .to_string();
    assert!(collapsed.contains("letx=1;"));
    assert!(!collapsed.contains("view_identity"));
}
