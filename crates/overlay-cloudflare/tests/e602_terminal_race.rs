use bsv_overlay_cloudflare::broadcaster::{terminal_at_arcade_by_presence, ArcOutcome};
use bsv_overlay_cloudflare::proof_fetcher::{classify_presence, NetworkPresence};

#[test]
fn e602_mempool_conflict_never_readmits() {
    let presence = classify_presence(
        Some(NetworkPresence::Absent),
        Some(NetworkPresence::Present { height: None }),
    );
    for young in [true, false] {
        let answer = terminal_at_arcade_by_presence(
            presence,
            young,
            "subject",
            "REJECTED",
            "UTXO_SPENT: input already spent by competitor; DOUBLE_SPEND_ATTEMPTED",
        );
        assert!(
            !matches!(answer, Ok(ArcOutcome::Accepted(_))),
            "mempool conflict readmitted: {answer:?}"
        );
    }
}

#[test]
fn e602_confirmation_survives_the_courier_body_and_fold() {
    use bsv_overlay_cloudflare::proof_fetcher::presence_response;
    for body in [r#"{"blockheight":970710}"#, r#"{"blockHeight":970710}"#] {
        let mined = presence_response(200, body);
        let presence = classify_presence(Some(NetworkPresence::Absent), mined);
        assert_eq!(
            presence,
            NetworkPresence::Present {
                height: Some(970710)
            }
        );
        for young in [true, false] {
            assert!(matches!(
                terminal_at_arcade_by_presence(
                    presence,
                    young,
                    "subject",
                    "REJECTED",
                    "UTXO_SPENT"
                ),
                Ok(ArcOutcome::Accepted(_))
            ));
        }
    }
    for body in [r#"{}"#, r#"{"blockheight":0}"#] {
        let presence = presence_response(200, body).unwrap();
        assert_eq!(presence, NetworkPresence::Present { height: None });
        assert!(
            terminal_at_arcade_by_presence(presence, true, "s", "REJECTED", "UTXO_SPENT").is_err()
        );
        assert!(matches!(
            terminal_at_arcade_by_presence(presence, false, "s", "REJECTED", "UTXO_SPENT"),
            Ok(ArcOutcome::Rejected(_))
        ));
    }
    assert_eq!(presence_response(200, "broken"), None);
    assert_eq!(presence_response(429, "{}"), None);
    assert_eq!(presence_response(404, ""), Some(NetworkPresence::Absent));
}

fn source(file: &str) -> String {
    let root = std::env::var("E602_BASE_SOURCE_DIR")
        .unwrap_or_else(|_| format!("{}/src", env!("CARGO_MANIFEST_DIR")));
    std::fs::read_to_string(format!("{root}/{file}"))
        .unwrap()
        .split_whitespace()
        .collect()
}

#[test]
fn e602_readmission_latch_requires_broadcaster_witness() {
    let routes = source("routes.rs");
    let start = routes.find("matcharcade_outcome{").unwrap();
    let arm = &routes[start..];
    let arm = &arm[..arm
        .find("Ok(crate::broadcaster::ArcOutcome::AcceptedPending(pending))")
        .unwrap()];
    assert_eq!(arm.matches("latch_network_seen(").count(), 1);
    assert!(arm.contains("ifarcade.admission_is_witnessed(){matchenv.d1"));
    assert!(arm.contains("ctx.wait_until(crate::admit_fast::pending_watch_job("));
}

#[test]
fn e602_retire_pass_rechecks_live_pots_for_confirmed_competitors() {
    let proof = source("proof_fetcher.rs");
    let pass = &proof[proof.find("pubasyncfnrun_retire_pass(").unwrap()..];
    let pass = &pass[..pass.find("letmutsummary=RetireSummary").unwrap()];
    assert_eq!(
        pass.matches(
            "letconflict=crate::admit_fast::confirmed_conflict_pass(db,woc_api_key,sources).await;"
        )
        .count(),
        1
    );
}

#[test]
fn e602_only_broadcaster_admissions_carry_a_witness() {
    use bsv_overlay_cloudflare::broadcaster::{
        admission_has_broadcaster_witness, TerminalJudgement,
    };
    assert!(admission_has_broadcaster_witness(None));
    for word in [
        TerminalJudgement::Present,
        TerminalJudgement::Race,
        TerminalJudgement::Absent,
        TerminalJudgement::Young,
        TerminalJudgement::Inconclusive,
        TerminalJudgement::Capped,
    ] {
        assert!(!admission_has_broadcaster_witness(Some(word)));
    }
    let race = TerminalJudgement::of(NetworkPresence::Present { height: None }, true, false);
    assert_eq!(race, TerminalJudgement::Race);
    assert_eq!(race.counter(), "submit_terminal_judged_race_total");
}

#[test]
fn e602_courier_functions_keep_the_response_body() {
    let proof = source("proof_fetcher.rs");
    for name in ["bitails_presence", "woc_presence"] {
        let start = proof.find(&format!("asyncfn{name}(base:")).unwrap();
        let body = &proof[start..];
        let body = &body[..body.find("_=>None,").unwrap()];
        assert_eq!(body.matches("presence_response(status,&body)").count(), 1);
    }
}

#[test]
fn e602_fold_retire_callers_pass_the_woc_key_and_print_the_conflict_summary() {
    // The admin caller remains in lib.rs; cron and alarm share tick.rs.
    let callers =
        (source("lib.rs") + &source("tick.rs")).replace("from_env(&env)", "from_env(env)");
    let call = "run_retire_pass(&ops_db,&arcade_base,woc_key.as_deref(),crate::proof_fetcher::ProofSources::from_env(env),).await;";
    assert_eq!(
        callers.matches(call).count(),
        2,
        "both retire-pass callers pass the key and source policy"
    );
    assert_eq!(
        callers
            .matches("run_retire_pass(&ops_db,&arcade_base,None)")
            .count(),
        0
    );
    assert_eq!(
        callers
            .matches("letwoc_key=env.secret(\"WOC_API_KEY\").ok().map(|k|k.to_string());letretire=")
            .count(),
        2
    );
    assert_eq!(
        callers
            .matches("conflict:scanned={}evicted={}faults={}")
            .count(),
        2
    );
}

#[test]
fn e602_fold_a_witnessed_accept_starts_no_watch() {
    let routes = source("routes.rs");
    assert!(routes.contains("letneeds_watch=!arcade.admission_is_witnessed();"));
}

#[test]
fn e602_delta_the_conflict_pass_bumps_its_counters_on_both_exits() {
    let admit_fast = source("admit_fast.rs");
    let start = admit_fast
        .find("pubasyncfnconfirmed_conflict_pass(")
        .unwrap();
    let end = admit_fast[start..]
        .find("asyncfnbump_conflict_counters(")
        .unwrap();
    let pass = &admit_fast[start..start + end];
    // bsv-low #649 H1: the read fault bumps here; each candidate is persisted by the shipped loop, through the
    // worker's port, before the next candidate's courier calls (never once after the loop)
    assert_eq!(
        pass.matches("bump_conflict_counters(db,&summary).await;")
            .count(),
        1,
        "the read fault bumps the conflict counters"
    );
    assert!(
        pass.contains("run_conflict_candidates(&port,"),
        "the loop is the shipped one"
    );
    let port = &admit_fast[admit_fast
        .find("implConflictPassPortforWorkerConflictPass")
        .unwrap()..start];
    assert!(
        port.contains("asyncfnpersist(&self,deltas:&ConflictPassSummary){bump_conflict_counters(self.db,deltas).await;}"),
        "the worker's port persists a candidate's deltas"
    );
}
