//! Cloudflare Workers deployment for BSV Overlay Services.
//!
//! Entry point for the overlay worker. Handles HTTP routing, D1 storage
//! initialization, and Engine setup.
//!
//! Pattern from ~/bsv/rust-wallet-infra/src/lib.rs.

pub mod admit_fast;
pub mod advert_lifecycle;
pub mod advertiser;
pub mod arcade_reorg;
pub mod ban_storage;
pub mod beef_blob_sweep;
pub mod broadcaster;
pub mod chain_tracker;
pub mod change_flush;
pub mod change_park;
pub mod d1;
pub mod d1_discovery;
pub mod d1_ledger;
pub mod d1_storage;
pub mod dead_letters;
pub mod derived_hops;
pub mod ef;
pub mod ef_deferred;
pub mod error;
mod gasp_cost;
pub mod gasp_deferred;
pub mod gasp_fault_memo;
pub mod gasp_remote;
mod gasp_speed;
pub mod health_checker;
pub mod hop_changes;
pub mod hop_probe_memos;
pub mod janitor;
pub mod lobby_changes;
pub mod mainnet_fanout;
pub mod ops;
pub mod peer_crawler;
pub mod pot_changes;
pub mod proof_fetcher;
mod proof_probe;
pub mod proof_window;
pub mod queue;
pub mod relatch;
pub mod reorg_sweep;
pub mod routes;
pub mod ship_lookup;
pub mod submit_census;
pub mod submit_gate;
pub mod submit_refusals;
mod tick;
mod tick_alarm;
mod tick_ops;
pub mod tip_pass;
pub use tick_alarm::TickAlarm;
pub mod wallet;

use overlay_engine::beef_limits;
use std::collections::HashMap;
use std::rc::Rc;

use overlay_discovery::agent::lookup_service::AgentLookupService;
use overlay_discovery::agent::storage::AgentStorage;
use overlay_discovery::agent::topic_manager::AgentTopicManager;
use overlay_discovery::collected::lookup_service::CollectedLookupService;
use overlay_discovery::collected::storage::CollectedStorage;
use overlay_discovery::collected::topic_manager::CollectedTopicManager;
use overlay_discovery::dm_delegation::lookup_service::DmDelegationLookupService;
use overlay_discovery::dm_delegation::storage::DmDelegationStorage;
use overlay_discovery::dm_delegation::topic_manager::DmDelegationTopicManager;
use overlay_discovery::hand::lookup_service::HandLookupService;
use overlay_discovery::hand::storage::HandStorage;
use overlay_discovery::hand::topic_manager::HandTopicManager;
use overlay_discovery::hopparty::lookup_service::HoppartyLookupService;
use overlay_discovery::hopparty::storage::HoppartyStorage;
use overlay_discovery::hopparty::topic_manager::HoppartyTopicManager;
use overlay_discovery::low::lookup_service::LowLookupService;
use overlay_discovery::low::storage::LowStorage;
use overlay_discovery::low::topic_manager::LowTopicManager;
use overlay_discovery::pot::lookup_service::PotLookupService;
use overlay_discovery::pot::storage::PotStorage;
use overlay_discovery::pot::topic_manager::PotTopicManager;
use overlay_discovery::potparty::lookup_service::PotpartyLookupService;
use overlay_discovery::potparty::storage::PotpartyStorage;
use overlay_discovery::potparty::topic_manager::PotpartyTopicManager;
use overlay_discovery::potrefund::lookup_service::PotrefundLookupService;
use overlay_discovery::potrefund::storage::PotrefundStorage;
use overlay_discovery::potrefund::topic_manager::PotrefundTopicManager;
use overlay_discovery::proof::lookup_service::ProofLookupService;
use overlay_discovery::proof::storage::ProofStorage;
use overlay_discovery::proof::topic_manager::ProofTopicManager;
use overlay_discovery::result::lookup_service::ResultLookupService;
use overlay_discovery::result::storage::ResultStorage;
use overlay_discovery::result::topic_manager::ResultTopicManager;
use overlay_discovery::reveal::lookup_service::RevealLookupService;
use overlay_discovery::reveal::storage::RevealStorage;
use overlay_discovery::reveal::topic_manager::RevealTopicManager;
use overlay_discovery::ship::lookup_service::SHIPLookupService;
use overlay_discovery::ship::storage::SHIPStorage;
use overlay_discovery::ship::topic_manager::SHIPTopicManager;
use overlay_discovery::slap::lookup_service::SLAPLookupService;
use overlay_discovery::slap::storage::SLAPStorage;
use overlay_discovery::slap::topic_manager::SLAPTopicManager;
use overlay_discovery::uhrp::lookup_service::UHRPLookupService;
use overlay_discovery::uhrp::storage::UHRPStorage;
use overlay_discovery::uhrp::topic_manager::UHRPTopicManager;
use overlay_engine::engine::{Engine, EngineConfig};
use overlay_engine::lookup_service::LookupService;
use overlay_engine::topic_manager::TopicManager;
use worker::{event, Context, Env, Method, Request, Response};

use crate::broadcaster::{ArcadeBroadcaster, WorkerBroadcaster};
use crate::chain_tracker::WorkerChainTracker;
use crate::d1::ensure_overlay_migrations;
use crate::d1_discovery::{
    D1AgentStorage, D1CollectedStorage, D1DmDelegationStorage, D1HandStorage, D1HoppartyStorage,
    D1LowStorage, D1PotStorage, D1PotpartyStorage, D1PotrefundStorage, D1ProofStorage,
    D1ResultStorage, D1RevealStorage, D1SHIPStorage, D1SLAPStorage, D1UHRPStorage,
};
use crate::d1_storage::D1Storage;
use crate::health_checker::WorkerHealthChecker;
use crate::routes::*;

/// Non-GASP peers the scheduled cron crawls. Each entry is
/// `(peer_url, [(lookup_service, topic_manager), ...])`. GASP-speaking
/// peers are discovered dynamically via `engine.start_gasp_sync()` and
/// not listed here — this is purely the compatibility bridge for
/// `@bsv/overlay-express` hosts that don't expose `/requestSyncResponse`.
///
/// Today: `overlay-us-1.bsvb.tech` carries UHRP advertisements
/// (ls_uhrp / tm_uhrp). Probed 2026-04-21: their `/requestSyncResponse`
/// returns `ERR_ROUTE_NOT_FOUND`, but `/lookup` + `/submit` work —
/// hence this bridge.
///
/// Adding a peer is a code change, not an env var, by design: the
/// service→topic mapping is version-controlled alongside the
/// topic-manager admission logic that re-validates their records.
fn non_gasp_peers() -> Vec<peer_crawler::PeerConfig> {
    vec![peer_crawler::PeerConfig {
        peer_url: "https://overlay-us-1.bsvb.tech".to_string(),
        service_to_topic: vec![("ls_uhrp".to_string(), "tm_uhrp".to_string())],
    }]
}

// Reference trackers: ts-stack@edf6e03
// packages/sdk/src/overlay-tools/LookupResolver.ts:216-223.
fn ship_slap_bootstrap() -> Vec<String> {
    [
        "https://overlay-us-1.bsvb.tech",
        "https://overlay-eu-1.bsvb.tech",
        "https://overlay-ap-1.bsvb.tech",
        "https://users.bapp.dev",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn gasp_sync_configuration(trackers: &[String]) -> overlay_engine::types::SyncConfiguration {
    use overlay_engine::types::SyncTarget;
    let mut config = HashMap::new();
    for topic in ["tm_ship", "tm_slap"] {
        config.insert(topic.to_string(), SyncTarget::Peers(trackers.to_vec()));
    }
    for topic in ["tm_uhrp", "tm_agent", "tm_dm_delegation"] {
        config.insert(topic.to_string(), SyncTarget::Ship);
    }
    // The LOW topics remain single-node indexes.
    for topic in [
        "tm_low",
        "tm_reveal",
        "tm_pot",
        "tm_lowfund",
        "tm_collected",
        "tm_hand",
        "tm_result",
        "tm_proof",
        "tm_potparty",
        "tm_potrefund",
    ] {
        config.insert(topic.to_string(), SyncTarget::Disabled);
    }
    config
}

fn rekey_sync_configuration(
    config: overlay_engine::types::SyncConfiguration,
    suffix: &str,
) -> overlay_engine::types::SyncConfiguration {
    config
        .into_iter()
        .map(|(topic, target)| (crate::routes::suffixed_name(&topic, suffix), target))
        .collect()
}

fn registered_sync_configuration(
    mut config: overlay_engine::types::SyncConfiguration,
    managers: &HashMap<String, Box<dyn TopicManager>>,
) -> (overlay_engine::types::SyncConfiguration, Vec<String>) {
    let mut skipped = Vec::new();
    config.retain(|topic, _| {
        let registered = managers.contains_key(topic);
        if !registered {
            skipped.push(topic.clone());
        }
        registered
    });
    skipped.sort();
    (config, skipped)
}

fn sync_topic_warning_due(topic: &str) -> bool {
    // One warning per topic per isolate, including repeated engine builds.
    static WARNED: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<String>>> =
        std::sync::OnceLock::new();
    WARNED
        .get_or_init(Default::default)
        .lock()
        .is_ok_and(|mut warned| warned.insert(topic.to_string()))
}

/// bsv-low #499: the routes the D1 rows ledger keys by name (`d1_ledger::route_of`; anything else is `other`). A
/// route with a path parameter is listed by its fixed head. `/admin` and `/internal` are one key each.
pub const D1_LEDGER_ROUTES: &[&str] = &[
    "/submit",
    "/lookup",
    "/arc-ingest",
    "/requestSyncResponse",
    "/requestForeignGASPNode",
    "/health/invariants",
    "/health",
    "/beef-any",
    "/listTopicManagers",
    "/listLookupServiceProviders",
    "/admin",
    "/internal",
];

/// The fetch entry: [`dispatch`] under the request's D1 rows ledger (bsv-low #499). What the request cost D1 rides
/// its answer as `Server-Timing: d1;desc="reads=N writes=M stmts=K"` (appended to the route's own timing) and the
/// route's running maxima ride `/health/invariants.d1Budget`.
#[event(fetch)]
async fn main(req: Request, env: Env, ctx: Context) -> worker::Result<Response> {
    let route = crate::d1_ledger::route_of(req.path().as_str(), D1_LEDGER_ROUTES);
    let (out, tally) = crate::d1_ledger::scoped(dispatch(req, env, ctx)).await;
    crate::d1_ledger::record(route, tally);
    // lens fold L5: an escaped handler error is answered as the runtime would, with the figures stamped
    let mut resp = match out {
        Ok(resp) => resp,
        Err(e) => return crate::d1_ledger::escaped(&e, tally),
    };
    crate::d1_ledger::stamp(&mut resp, tally);
    Ok(resp)
}

async fn dispatch(req: Request, env: Env, ctx: Context) -> worker::Result<Response> {
    // Install a panic hook so Rust panics surface in wrangler tail as
    // proper stack traces instead of the Worker silently returning early
    // (the default wasm behaviour). `set_once` makes re-calls across
    // request invocations cheap. Same pattern as `bsv-middleware-cloudflare`.
    bsv_middleware_cloudflare::init_panic_hook();

    // CORS preflight
    if req.method() == Method::Options {
        return cors_preflight();
    }

    // Health check routes (no DB needed — checks are configuration-level)
    if req.method() == Method::Get {
        match req.path().as_str() {
            "/health" => return health(&env).await,
            "/health/live" => return health_live(&env).await,
            "/health/ready" => return health_ready(&env).await,
            // Loop-2 hardening (2026-09-05): an on-demand, courier-backed, chaintracks-
            // verified BEEF for a MINED txid the index never admitted — the credit
            // path's proof source when `/beef` has nothing to serve.
            p if p.starts_with("/beef-any/") => return routes::beef_any(&env, p).await,
            // NL-6c: a deferred broadcast-gated submission's state and, once
            // its run ended, the arm's answer.
            p if p.starts_with(crate::ef_deferred::POLL_PREFIX) => {
                return crate::ef_deferred::poll(&env, p).await
            }
            _ => {}
        }
    }

    if req.method() == Method::Get && req.path() == "/admin/proof-probe" {
        if let Err(response) = check_admin_auth(&req, &env) {
            return response;
        }
        return proof_probe::handle(&env, &req).await;
    }

    // D1 database binding
    let db = Rc::new(env.d1("OVERLAY_DB")?);
    // Ban storage — shares the OVERLAY_DB binding via a dedicated table
    let ban_storage = Rc::new(crate::ban_storage::D1BanStorage::new(db.clone()));

    // Apply migrations once per isolate (idempotent — CREATE IF NOT EXISTS;
    // unguarded per-request execution was 63 D1 round-trips/request, #255)
    // bsv-low #499 (lens fold L2): under its own ledger key, `(boot)`, not the route that woke the isolate
    crate::d1_ledger::boot(ensure_overlay_migrations(&db))
        .await
        .map_err(|e| worker::Error::from(format!("Migration failed: {e}")))?;

    // Build Engine + discovery storage (shared for janitor)
    let ship_storage: Rc<dyn SHIPStorage> = Rc::new(D1SHIPStorage::new(db.clone()));
    let slap_storage: Rc<dyn SLAPStorage> = Rc::new(D1SLAPStorage::new(db.clone()));
    let agent_storage: Rc<dyn AgentStorage> = Rc::new(D1AgentStorage::new(db.clone()));
    let dm_delegation_storage: Rc<dyn DmDelegationStorage> =
        Rc::new(D1DmDelegationStorage::new(db.clone()));
    let uhrp_storage: Rc<dyn UHRPStorage> = Rc::new(D1UHRPStorage::new(db.clone()));
    let low_storage: Rc<dyn LowStorage> = Rc::new(D1LowStorage::new(db.clone()));
    let reveal_storage: Rc<dyn RevealStorage> = Rc::new(D1RevealStorage::new(db.clone()));
    let pot_storage: Rc<dyn PotStorage> = Rc::new(D1PotStorage::new(db.clone()));
    let collected_storage: Rc<dyn CollectedStorage> = Rc::new(D1CollectedStorage::new(db.clone()));
    let hand_storage: Rc<dyn HandStorage> = Rc::new(D1HandStorage::new(db.clone()));
    let result_storage: Rc<dyn ResultStorage> = Rc::new(D1ResultStorage::new(db.clone()));
    let proof_storage: Rc<dyn ProofStorage> = Rc::new(D1ProofStorage::new(db.clone()));
    let potparty_storage: Rc<dyn PotpartyStorage> = Rc::new(D1PotpartyStorage::new(db.clone()));
    let potrefund_storage: Rc<dyn PotrefundStorage> = Rc::new(D1PotrefundStorage::new(db.clone()));
    let hopparty_storage: Rc<dyn HoppartyStorage> = Rc::new(D1HoppartyStorage::new(db.clone()));
    // DB handle for GET /health/invariants (#192/#193, P4) — the engine build
    // below consumes `db`.
    let ops_db = db.clone();
    let engine = build_engine_with_storage(
        db,
        &env,
        ship_storage.clone(),
        slap_storage.clone(),
        agent_storage.clone(),
        dm_delegation_storage.clone(),
        uhrp_storage.clone(),
        low_storage.clone(),
        reveal_storage.clone(),
        pot_storage.clone(),
        collected_storage.clone(),
        hand_storage.clone(),
        result_storage.clone(),
        proof_storage.clone(),
        potparty_storage.clone(),
        potrefund_storage.clone(),
        hopparty_storage.clone(),
    );

    // Hosting URL for web UI
    let hosting_url = env.var("HOSTING_URL").ok().map(|v| v.to_string());

    // Route dispatch
    let result = match (req.method(), req.path().as_str()) {
        (Method::Get, "/") => web_ui(&engine, hosting_url.as_deref()).await,
        (Method::Get, "/health/invariants") => {
            // Proof-completion liveness (#192/#193, P4). strict=1 → 503 when the
            // completion pass has been dead longer than the staleness budget
            // (the alarm surface); otherwise 200 with the same verdict body.
            let strict = req
                .url()
                .ok()
                .and_then(|u| {
                    u.query_pairs()
                        .find(|(k, _)| k == "strict")
                        .map(|(_, v)| v.into_owned())
                })
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);
            crate::ops::health_invariants(&ops_db, &env, strict).await
        }
        (Method::Get, "/listTopicManagers") => list_topic_managers(&engine).await,
        (Method::Get, "/listLookupServiceProviders") => {
            list_lookup_service_providers(&engine).await
        }
        (Method::Get, "/getDocumentationForTopicManager") => {
            get_doc_for_topic_manager(&engine, &req).await
        }
        (Method::Get, "/getDocumentationForLookupServiceProvider") => {
            get_doc_for_lookup_service(&engine, &req).await
        }
        (Method::Post, "/submit") => {
            // broadcast-gated mode broadcasts through Arcade V2 (the overlay's
            // sole network broadcaster, #192/#193) — keyless. ARCADE_URL
            // overrides the default endpoint; the callback is derived from
            // HOSTING_URL inside the route.
            let arcade_url = env.var("ARCADE_URL").ok().map(|v| v.to_string());
            // #214: the TAAL key powers the corroborating broadcaster for an
            // exhausted gated ladder (Arcade async-REJECTED is never
            // authoritative uncorroborated). Absent key → keyless TAAL then
            // GorillaPool; corroboration always runs.
            let taal_api_key = env.secret("TAAL_API_KEY").ok().map(|s| s.to_string());
            // `ctx` is threaded in so the best-effort mainnet SHIP fan-out
            // runs via `wait_until` AFTER the response instead of inline
            // (it was costing the caller seconds on every submit).
            submit(
                &engine,
                req,
                hosting_url.as_deref(),
                arcade_url,
                taal_api_key,
                &ctx,
                &env,
            )
            .await
        }
        (Method::Post, "/lookup") => lookup(&engine, req).await,
        (Method::Post, "/arc-ingest") => {
            // Mainline only mounts /arc-ingest when arcApiKey is configured
            // (`OverlayExpress.ts` — gated on `typeof arcApiKey === 'string'
            // && arcApiKey.length > 0`). Mirror that here: without
            // TAAL_API_KEY we return the same 404 ERR_ROUTE_NOT_FOUND body
            // so the parity harness matches byte-for-byte.
            if env.secret("TAAL_API_KEY").is_err() {
                not_found()
            } else {
                // The callback merklePath is re-verified against chaintracks
                // before stitch (#192/#193) — a callback is a courier too.
                // #228: this push is the PRIMARY proof source — a verified
                // proof also lands in the LOW pot stores (pot_beefs compact +
                // pot_records spend-confirm latch) so the poll backstop skips
                // the tx entirely; non-MINED status callbacks
                // (X-FullStatusUpdates) are acknowledged and counted, never a
                // parse error.
                let tracker = lookup_service_chain_tracker(&env);
                let out = arc_ingest(
                    &engine,
                    req,
                    tracker.as_deref(),
                    pot_storage.as_ref(),
                    Some(&ops_db),
                    crate::admit_fast::EvidenceEnv::from_env(&env),
                    |fut| ctx.wait_until(fut),
                )
                .await;
                // bsv-low loop 6: a spend the push confirmed is a pot CHANGE —
                // ship the pot-changed webhook off the critical path (the same
                // flush `/submit` runs), so the seats' events boxes hear it.
                crate::pot_changes::flush(&env, |fut| ctx.wait_until(fut));
                out
            }
        }
        // bsv-low loop 6: the block-event spend-confirmation pass (bearer
        // INTERNAL_TOKEN, forwarded by the app-layer from chaintracks' tip).
        (Method::Post, "/internal/tip-changed") => {
            crate::tip_pass::internal_tip_changed(
                req,
                &env,
                &ctx,
                pot_storage.as_ref(),
                Some(&ops_db),
            )
            .await
        }
        // bsv-low M19 R2: the manual reorg demotion from a named height (the
        // rows a past reorg left behind; bearer INTERNAL_TOKEN; bounded).
        (Method::Post, "/internal/reorg") => {
            crate::tip_pass::internal_reorg(req, &env, &ctx, pot_storage.as_ref(), Some(&ops_db))
                .await
        }
        // bsv-low M19B-G1: one bounded pass of the Arcade reorg-event
        // consumer on demand (bearer INTERNAL_TOKEN; no body).
        (Method::Post, "/internal/arcade-reorg") => {
            crate::tip_pass::internal_arcade_reorg(
                req,
                &env,
                &ctx,
                pot_storage.as_ref(),
                Some(&ops_db),
            )
            .await
        }
        // bsv-low #576: the operator's bounded re-drive of the parked dead letters (bearer INTERNAL_TOKEN).
        (Method::Post, "/internal/redrive-dead-letters") => {
            crate::dead_letters::internal_redrive(req, &env).await
        }
        (Method::Post, "/internal/discard-dead-letters") => {
            crate::dead_letters::internal_discard(req, &env).await
        }
        // bsv-low #585 (door 3's fold-4): ONE bounded pass of the R2 orphan sweep on demand, the tick's own
        // function (bearer INTERNAL_TOKEN; body empty or {}).
        (Method::Post, "/internal/beef-blob-sweep") => {
            crate::beef_blob_sweep::internal_sweep(req, &env).await
        }
        // bsv-low #649 N3: one step of the parked set's shipped code on demand (bearer INTERNAL_TOKEN), so the
        // D1 budget tier measures it on a populated table.
        (Method::Post, "/internal/change-park") => {
            crate::change_park::internal_lever(req, &env).await
        }
        (Method::Post, "/requestSyncResponse") => request_sync_response(&engine, req).await,
        (Method::Post, "/requestForeignGASPNode") => request_foreign_gasp_node(&engine, req).await,

        // /admin/config is public (no auth) per mainline overlay-express 2.2.0
        (Method::Get, "/admin/config") => admin_config(&env).await,

        // Authed admin GETs
        (Method::Get, path) if path.starts_with("/admin/") => {
            if let Err(resp) = check_admin_auth(&req, &env) {
                return resp;
            }
            match path {
                "/admin/stats" => {
                    admin_stats(
                        &env,
                        ship_storage.as_ref(),
                        slap_storage.as_ref(),
                        ban_storage.as_ref(),
                    )
                    .await
                }
                "/admin/ship-records" => admin_ship_records(ship_storage.as_ref()).await,
                "/admin/slap-records" => admin_slap_records(slap_storage.as_ref()).await,
                "/admin/tick/status" => tick_alarm::admin_status(&env).await,
                "/admin/bans" => admin_bans(ban_storage.as_ref()).await,
                _ => not_found(),
            }
        }

        // Authed admin POSTs
        (Method::Post, path) if path.starts_with("/admin/") => {
            if let Err(resp) = check_admin_auth(&req, &env) {
                return resp;
            }
            match path {
                "/admin/tick/arm" => tick_alarm::admin_arm(&env).await,
                // #192/#193: run the BEEF proof-completion passes on demand. The
                // `*/15` scheduled event drives them (observed firing on beta, bsv-low
                // #649 D4-L1: the R2 sweep at 19:16:06Z and 19:31:06Z on 2026-10-13);
                // this authenticated admin FETCH route is an ADDITIONAL trigger that an
                // external cron (e.g. low-monitor) or an operator may POST. Same logic
                // as the scheduled block; fail-closed.
                "/admin/complete-proofs" => admin_complete_proofs(&env).await,
                // bsv-low#304 M-2: one-shot operator drain of the verified-
                // latch backlog (chaintracks-only stored-bump re-verify —
                // never a courier fetch). Drive repeatedly post-deploy until
                // `already_proven` returns 0.
                "/admin/reverifyPotBeefs" => admin_reverify_pot_beefs(&env, &req).await,
                // bsv-low#309: run the advert-lifecycle passes (expired-advert
                // reaper + advert spend-confirm) on demand — the same logic as
                // the scheduled tick's step 6, from a reliably-firing FETCH
                // route (cron completion has historically been carried by the
                // external admin poker). Fail-closed on the tip like the cron.
                "/admin/advert-lifecycle" => admin_advert_lifecycle(&env, &req).await,
                "/admin/syncAdvertisements" => admin_sync_advertisements(&engine).await,
                "/admin/startGASPSync" => admin_start_gasp_sync(&engine, &env, req).await,
                "/admin/evictOutpoint" => admin_evict_outpoint(&engine, req).await,
                // bsv-low PLAN-PRE-LOOP4 §H4: re-notify (or re-submit, subject-named) a txid the index never came to know
                "/admin/readmit" => admin_readmit(&engine, &env, req).await,
                "/admin/remove-token" => admin_remove_token(&engine, req).await,
                "/admin/crawlPeers" => admin_crawl_peers(&engine, &env, &non_gasp_peers()).await,
                "/admin/janitor" => {
                    admin_janitor(
                        ship_storage.as_ref(),
                        slap_storage.as_ref(),
                        hosting_url.as_deref(),
                    )
                    .await
                }
                "/admin/health-check" => admin_health_check(req).await,
                "/admin/ban" => {
                    admin_ban(
                        ban_storage.as_ref(),
                        ship_storage.as_ref(),
                        slap_storage.as_ref(),
                        req,
                    )
                    .await
                }
                "/admin/unban" => admin_unban(ban_storage.as_ref(), req).await,
                _ => not_found(),
            }
        }

        _ => not_found(),
    };

    // bsv-low #523 (M26): the dispatch's ONE exit ships whatever pot (and hop, via the pot flush, and lobby) rows
    // this request changed, off the critical path. The routes that already flush have drained the set (a no-op here); the
    // ones that never did (`/admin/complete-proofs` confirmed a tower-broadcast refund's spend and pushed
    // nothing, `/admin/readmit`'s discovery, a `/lookup` whose serve-time rung latches a spend) ride this one.
    crate::pot_changes::flush(&env, |fut| ctx.wait_until(fut));
    crate::lobby_changes::flush(&env, |fut| ctx.wait_until(fut));
    result
}

/// Build Engine from an `Env` binding (D1 init + migrations + engine).
///
/// Used by `wait_until` closures and the queue consumer where a fresh Engine
/// must be constructed from a cloned Env.
pub async fn build_engine_from_env(env: &Env) -> Result<Engine, String> {
    let db = Rc::new(
        env.d1("OVERLAY_DB")
            .map_err(|e| format!("D1 binding error: {e}"))?,
    );
    // bsv-low #499 (lens fold L2): under its own ledger key, `(boot)`, not the route that woke the isolate
    crate::d1_ledger::boot(ensure_overlay_migrations(&db))
        .await
        .map_err(|e| format!("Migration failed: {e}"))?;
    let ship_storage: Rc<dyn SHIPStorage> = Rc::new(D1SHIPStorage::new(db.clone()));
    let slap_storage: Rc<dyn SLAPStorage> = Rc::new(D1SLAPStorage::new(db.clone()));
    let agent_storage: Rc<dyn AgentStorage> = Rc::new(D1AgentStorage::new(db.clone()));
    let dm_delegation_storage: Rc<dyn DmDelegationStorage> =
        Rc::new(D1DmDelegationStorage::new(db.clone()));
    let uhrp_storage: Rc<dyn UHRPStorage> = Rc::new(D1UHRPStorage::new(db.clone()));
    let low_storage: Rc<dyn LowStorage> = Rc::new(D1LowStorage::new(db.clone()));
    let reveal_storage: Rc<dyn RevealStorage> = Rc::new(D1RevealStorage::new(db.clone()));
    let pot_storage: Rc<dyn PotStorage> = Rc::new(D1PotStorage::new(db.clone()));
    let collected_storage: Rc<dyn CollectedStorage> = Rc::new(D1CollectedStorage::new(db.clone()));
    let hand_storage: Rc<dyn HandStorage> = Rc::new(D1HandStorage::new(db.clone()));
    let result_storage: Rc<dyn ResultStorage> = Rc::new(D1ResultStorage::new(db.clone()));
    let proof_storage: Rc<dyn ProofStorage> = Rc::new(D1ProofStorage::new(db.clone()));
    let potparty_storage: Rc<dyn PotpartyStorage> = Rc::new(D1PotpartyStorage::new(db.clone()));
    let potrefund_storage: Rc<dyn PotrefundStorage> = Rc::new(D1PotrefundStorage::new(db.clone()));
    let hopparty_storage: Rc<dyn HoppartyStorage> = Rc::new(D1HoppartyStorage::new(db.clone()));
    Ok(build_engine_with_storage(
        db,
        env,
        ship_storage,
        slap_storage,
        agent_storage,
        dm_delegation_storage,
        uhrp_storage,
        low_storage,
        reveal_storage,
        pot_storage,
        collected_storage,
        hand_storage,
        result_storage,
        proof_storage,
        potparty_storage,
        potrefund_storage,
        hopparty_storage,
    ))
}

/// Chain tracker for the LOW lookup services (ls_low table expiry, ls_pot
/// spend-confirmation) — CHAINTRACKS service binding preferred, plain
/// `CHAIN_TRACKER_URL` fallback, `None` when neither is configured.
///
/// ChainTracks is another Worker on the SAME account, so a plain
/// `workers.dev` URL fetch loops back to THIS worker (404) and the check
/// never resolves — we route through the CHAINTRACKS service binding
/// instead, which reaches the real ChainTracks worker. The URL fallback
/// works only if ChainTracks is off-account; with no tracker at all each
/// consumer fails open/safe (ls_low: no expiry filter; ls_pot: spends record
/// as unconfirmed hints).
/// 2026-09-04: EVERY courier fetcher this worker builds — the Arcade URL from
/// the var and the WoC api key from the `WOC_API_KEY` secret (the plumbing
/// existed; nothing wired the secret, so every WoC read ran on the free tier
/// that 429s under load — the repo's most-documented outage class).
pub(crate) fn courier_fetcher(
    env: &Env,
    tracker: Option<Rc<dyn bsv_rs::transaction::ChainTracker>>,
) -> crate::proof_fetcher::ChainProofFetcher {
    courier_fetcher_unrecorded(env, tracker)
        .with_proof_fault_store(env.d1("OVERLAY_DB").ok().map(Rc::new))
}

/// The courier of [`courier_fetcher`] with no fault store: it writes
/// nothing (engine 0.1.9, the probe's walk). Its tallies are in memory.
pub(crate) fn courier_fetcher_unrecorded(
    env: &Env,
    tracker: Option<Rc<dyn bsv_rs::transaction::ChainTracker>>,
) -> crate::proof_fetcher::ChainProofFetcher {
    let mut f = crate::proof_fetcher::ChainProofFetcher::new(tracker)
        .with_sources(crate::proof_fetcher::ProofSources::from_env(env));
    if let Some(u) = env
        .var("ARCADE_URL")
        .ok()
        .map(|v| v.to_string())
        .filter(|s| !s.trim().is_empty())
    {
        f = f.with_arcade_url(u);
    }
    f.with_woc_api_key(env.secret("WOC_API_KEY").ok().map(|k| k.to_string()))
}

pub(crate) fn lookup_service_chain_tracker(
    env: &Env,
) -> Option<Rc<dyn bsv_rs::transaction::ChainTracker>> {
    let ct_url = env
        .var("CHAIN_TRACKER_URL")
        .map(|v| v.to_string())
        .unwrap_or_else(|_| "https://chaintracks.invalid".to_string());
    match env.service("CHAINTRACKS") {
        Ok(svc) => Some(Rc::new(WorkerChainTracker::with_service(ct_url, svc))),
        // No binding configured: fall back to the URL path (works only if
        // ChainTracks is off-account; otherwise fails open).
        Err(_) => env.var("CHAIN_TRACKER_URL").ok().map(|u| {
            Rc::new(WorkerChainTracker::new(u.to_string()))
                as Rc<dyn bsv_rs::transaction::ChainTracker>
        }),
    }
}

/// Build the overlay Engine with D1-backed storage and pre-built SHIP/SLAP/Agent storage.
///
/// The discovery storage references are passed in so they can be shared with
/// the Janitor service (which needs direct access to discovery records).
#[allow(clippy::too_many_arguments)] // one storage handle per registered plugin
fn build_engine_with_storage(
    db: Rc<worker::D1Database>,
    env: &Env,
    ship_storage: Rc<dyn SHIPStorage>,
    slap_storage: Rc<dyn SLAPStorage>,
    agent_storage: Rc<dyn AgentStorage>,
    dm_delegation_storage: Rc<dyn DmDelegationStorage>,
    uhrp_storage: Rc<dyn UHRPStorage>,
    low_storage: Rc<dyn LowStorage>,
    reveal_storage: Rc<dyn RevealStorage>,
    pot_storage: Rc<dyn PotStorage>,
    collected_storage: Rc<dyn CollectedStorage>,
    hand_storage: Rc<dyn HandStorage>,
    result_storage: Rc<dyn ResultStorage>,
    proof_storage: Rc<dyn ProofStorage>,
    potparty_storage: Rc<dyn PotpartyStorage>,
    potrefund_storage: Rc<dyn PotrefundStorage>,
    hopparty_storage: Rc<dyn HoppartyStorage>,
) -> Engine {
    // Storage
    let ledger = db.clone();
    let storage = Box::new(D1Storage::new(db));

    // Topic manager + lookup service registration is driven by env vars so
    // the same binary can run as a pure mainline-parity overlay (default)
    // or as a fully-loaded dolphinmilk deployment with UHRP / Agent /
    // DmDelegation extras. Matches the @bsv/overlay-express
    // library-configured-at-deploy model.
    //
    // Defaults (var unset) = the mainline set: SHIP + SLAP only.
    let topic_list = env
        .var("TOPIC_MANAGERS")
        .ok()
        .map(|v| v.to_string())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "tm_ship,tm_slap".into());
    let lookup_list = env
        .var("LOOKUP_SERVICES")
        .ok()
        .map(|v| v.to_string())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "ls_ship,ls_slap".into());

    // Keep these for the advertiser (needs read access to our own SHIP/SLAP
    // records). Rc::clone is a refcount bump, not a data copy.
    let ship_storage_for_ad = ship_storage.clone();
    let slap_storage_for_ad = slap_storage.clone();

    let mut managers: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    for t in topic_list
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        match t {
            "tm_ship" => {
                managers.insert("tm_ship".into(), Box::new(SHIPTopicManager::new()));
            }
            "tm_slap" => {
                managers.insert("tm_slap".into(), Box::new(SLAPTopicManager::new()));
            }
            "tm_uhrp" => {
                managers.insert("tm_uhrp".into(), Box::new(UHRPTopicManager::new()));
            }
            "tm_agent" => {
                managers.insert("tm_agent".into(), Box::new(AgentTopicManager::new()));
            }
            "tm_dm_delegation" => {
                managers.insert(
                    "tm_dm_delegation".into(),
                    Box::new(DmDelegationTopicManager::new()),
                );
            }
            "tm_low" => {
                managers.insert("tm_low".into(), Box::new(LowTopicManager::new()));
            }
            "tm_reveal" => {
                managers.insert("tm_reveal".into(), Box::new(RevealTopicManager::new()));
            }
            "tm_pot" => {
                managers.insert("tm_pot".into(), Box::new(PotTopicManager::new()));
            }
            "tm_lowfund" => {
                managers.insert(
                    "tm_lowfund".into(),
                    Box::new(
                        overlay_discovery::pot::lowfund_topic_manager::LowFundTopicManager::new(),
                    ),
                );
            }
            "tm_collected" => {
                managers.insert(
                    "tm_collected".into(),
                    Box::new(CollectedTopicManager::new()),
                );
            }
            "tm_hand" => {
                managers.insert("tm_hand".into(), Box::new(HandTopicManager::new()));
            }
            "tm_result" => {
                managers.insert("tm_result".into(), Box::new(ResultTopicManager::new()));
            }
            "tm_proof" => {
                managers.insert("tm_proof".into(), Box::new(ProofTopicManager::new()));
            }
            "tm_potparty" => {
                managers.insert("tm_potparty".into(), Box::new(PotpartyTopicManager::new()));
            }
            "tm_potrefund" => {
                managers.insert(
                    "tm_potrefund".into(),
                    Box::new(PotrefundTopicManager::new()),
                );
            }
            "tm_hopparty" => {
                managers.insert("tm_hopparty".into(), Box::new(HoppartyTopicManager::new()));
            }
            other => worker::console_warn!("TOPIC_MANAGERS: unknown entry '{other}' — skipped"),
        }
    }

    let mut lookup_services: HashMap<String, Box<dyn LookupService>> = HashMap::new();
    for l in lookup_list
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        match l {
            "ls_ship" => {
                lookup_services.insert(
                    "ls_ship".into(),
                    Box::new(SHIPLookupService::new(ship_storage.clone())),
                );
            }
            "ls_slap" => {
                lookup_services.insert(
                    "ls_slap".into(),
                    Box::new(SLAPLookupService::new(slap_storage.clone())),
                );
            }
            "ls_uhrp" => {
                lookup_services.insert(
                    "ls_uhrp".into(),
                    Box::new(UHRPLookupService::new(uhrp_storage.clone())),
                );
            }
            "ls_agent" => {
                lookup_services.insert(
                    "ls_agent".into(),
                    Box::new(AgentLookupService::new(agent_storage.clone())),
                );
            }
            "ls_dm_delegation" => {
                lookup_services.insert(
                    "ls_dm_delegation".into(),
                    Box::new(DmDelegationLookupService::new(
                        dm_delegation_storage.clone(),
                    )),
                );
            }
            "ls_low" => {
                // Wire the chain tip into ls_low so findOpenTables enforces
                // table expiry at query time (bsv-low #148). LOW-local: only
                // the LOW services consult the tracker.
                let mut low_svc = LowLookupService::new(low_storage.clone());
                if let Some(t) = lookup_service_chain_tracker(env) {
                    low_svc = low_svc.with_chain_tracker(t);
                }
                lookup_services.insert("ls_low".into(), Box::new(low_svc));
            }
            "ls_reveal" => {
                lookup_services.insert(
                    "ls_reveal".into(),
                    Box::new(RevealLookupService::new(reveal_storage.clone())),
                );
            }
            "ls_pot" => {
                // Wire the same SPV source into ls_pot so output_spent can
                // derive the CONFIRMED hint (prefer-confirmed /
                // never-clobber-with-unconfirmed spend pointers): a
                // bump-carrying spend the tracker validates is recorded as
                // chain truth an unconfirmed /submit can never overwrite.
                // No tracker → every spend degrades to an unconfirmed hint.
                let mut pot_svc = PotLookupService::new(pot_storage.clone());
                if let Some(t) = lookup_service_chain_tracker(env) {
                    pot_svc = pot_svc.with_chain_tracker(t);
                }
                lookup_services.insert("ls_pot".into(), Box::new(pot_svc));
            }
            "ls_collected" => {
                lookup_services.insert(
                    "ls_collected".into(),
                    Box::new(CollectedLookupService::new(collected_storage.clone())),
                );
            }
            "ls_hand" => {
                lookup_services.insert(
                    "ls_hand".into(),
                    Box::new(HandLookupService::new(hand_storage.clone())),
                );
            }
            "ls_result" => {
                lookup_services.insert(
                    "ls_result".into(),
                    Box::new(ResultLookupService::new(result_storage.clone())),
                );
            }
            "ls_proof" => {
                lookup_services.insert(
                    "ls_proof".into(),
                    Box::new(ProofLookupService::with_prover(
                        proof_storage.clone(),
                        low_proof_replay::prove_bundle,
                    )),
                );
            }
            "ls_potparty" => {
                lookup_services.insert(
                    "ls_potparty".into(),
                    Box::new(PotpartyLookupService::new(potparty_storage.clone())),
                );
            }
            "ls_potrefund" => {
                lookup_services.insert(
                    "ls_potrefund".into(),
                    Box::new(PotrefundLookupService::new(potrefund_storage.clone())),
                );
            }
            "ls_hopparty" => {
                lookup_services.insert(
                    "ls_hopparty".into(),
                    Box::new(HoppartyLookupService::new(hopparty_storage.clone())),
                );
            }
            other => worker::console_warn!("LOOKUP_SERVICES: unknown entry '{other}' — skipped"),
        }
    }

    // Config — hosting URL from env var, or default
    let hosting_url = env.var("HOSTING_URL").ok().map(|v| v.to_string());

    // SHIP discovery queries the trackers and their advertised ls_ship hosts,
    // even when local ls_ship has no records. UHRP keeps a bootstrap peer in
    // its union so tracker outages still leave a sync target.
    let ship_slap_bootstrap = ship_slap_bootstrap();
    let mut sync_configuration = gasp_sync_configuration(&ship_slap_bootstrap);

    // ── Deploy-time topic namespace (`TOPIC_SUFFIX`, bsv-low beta stack) ─────
    //
    // The zanaadu model: ONE binary serves prod (suffix unset) and a fully
    // isolated beta (`_beta`) whose rows can be wiped without touching prod.
    // `TOPIC_MANAGERS` / `LOOKUP_SERVICES` stay BASE-named in every env — the
    // suffix is applied HERE, in a single place, to the registered manager and
    // service keys AND to the GASP sync map, so a topic added later cannot be
    // forgotten by the namespacing.
    //
    // Fail-closed by construction: a suffixed deployment registers ONLY
    // suffixed names, so a client aimed at the wrong stack asks for a topic
    // this worker does not have and is refused. Neither direction can write a
    // row into the other environment's index — which is the whole point, since
    // those rows are money-recovery enumeration.
    let topic_suffix = env
        .var("TOPIC_SUFFIX")
        .ok()
        .map(|v| v.to_string())
        .unwrap_or_default();
    if !topic_suffix.is_empty() {
        // SHIP and SLAP are NEVER suffixed. The engine hardcodes those four
        // names for tracker bootstrap (`engine.rs` `tm_ship`/`tm_slap` arms),
        // for suppressing self-advertisement of the discovery topics, and for
        // peer discovery via `ls_ship`; the advertiser also submits its own
        // ads under bare `tm_ship`/`tm_slap`, so suffixing them would make the
        // ad self-admission fail `UnsupportedTopic`. Discovery is deliberately
        // a shared, global namespace — it is the LOW protocol topics that must
        // be per-environment.
        let rekey = |k: String| -> String { crate::routes::suffixed_name(&k, &topic_suffix) };
        managers = managers.into_iter().map(|(k, v)| (rekey(k), v)).collect();
        lookup_services = lookup_services
            .into_iter()
            .map(|(k, v)| (rekey(k), v))
            .collect();
        // The GASP sync map is keyed by topic too, and a MISSING key is not
        // inert: `engine.rs` defaults an unknown manager to `SyncTarget::Ship`.
        // Leaving these bare would silently turn every deliberately-Disabled
        // LOW topic into live peer discovery on the beta deploy.
        sync_configuration = rekey_sync_configuration(sync_configuration, &topic_suffix);
    }

    // Filter after both maps have their deployment namespace, before the
    // engine fills defaults. Unregistered topics never spend peer slots.
    let (sync_configuration, skipped) =
        registered_sync_configuration(sync_configuration, &managers);
    for topic in skipped {
        if sync_topic_warning_due(&topic) {
            worker::console_warn!("GASP sync topic {topic} has no registered manager; skipped");
        }
    }

    let config = EngineConfig {
        hosting_url: hosting_url.clone(),
        slap_trackers: ship_slap_bootstrap,
        sync_configuration,
        ..Default::default()
    };

    // ChainTracker — SPV verification via ChainTracks API.
    //
    // #320 defect 3b root cause: this slot used a PLAIN URL fetch while every
    // other tracker consumer routes through `lookup_service_chain_tracker`.
    // ChainTracks is a Worker on the SAME account, and a `workers.dev`
    // subrequest to a same-account Worker loops back to the CALLER (bsv-low
    // #148, documented on the sibling fn) — so the engine answered its own
    // /findHeaderHexForHeight with its own 404, and `run_validation`'s SPV
    // gate returned `BlockNotFound` for EVERY height, deterministically,
    // since initial release. Victims: the ad self-admission (every
    // sync_advertisements local submit) and any stock no-mode-header
    // /submit whose BEEF carries a BUMP. ChainTracks itself was healthy the
    // whole time. A configured service binding enables verification even
    // without a URL variable, matching lookup_service_chain_tracker.
    let tracker_url = env.var("CHAIN_TRACKER_URL").ok().map(|v| v.to_string());
    let chain_tracker: Option<Box<dyn bsv_rs::transaction::ChainTracker>> =
        match env.service("CHAINTRACKS") {
            Ok(svc) => Some(Box::new(WorkerChainTracker::with_service(
                tracker_url.unwrap_or_else(|| "https://chaintracks.invalid".to_string()),
                svc,
            ))),
            Err(_) => tracker_url.map(|url| {
                Box::new(WorkerChainTracker::new(url)) as Box<dyn bsv_rs::transaction::ChainTracker>
            }),
        };

    // Network broadcaster — Arcade V2 is the overlay's SOLE network broadcaster
    // (#192/#193): EF submit + a FREE merkle proof pushed in Arcade's MINED
    // callback. Keyless (no TAAL_API_KEY needed). The X-CallbackUrl points at
    // our own /arc-ingest so a MINED status pushes the merkle path back for
    // proof completion (the primary proof source). ARCADE_URL overrides the
    // endpoint (default: arcade-v2-us-1.bsvblockchain.tech).
    //
    // NOTE: this engine slot is only hit for generic `CurrentTx` submits; the
    // LOW money path broadcasts through the broadcast-gated /submit route
    // (`ArcadeBroadcaster::broadcast_efs_gated`), which is where the callback
    // registration actually matters.
    let arcade_url = env
        .var("ARCADE_URL")
        .ok()
        .map(|v| v.to_string())
        .unwrap_or_default();
    let mut arcade = ArcadeBroadcaster::new(arcade_url);
    if let Some(ref h) = hosting_url {
        arcade = arcade.with_callback(format!("{}/arc-ingest", h.trim_end_matches('/')));
    }
    let arc_broadcaster: Option<Box<dyn overlay_engine::broadcaster::ArcBroadcaster>> =
        Some(Box::new(arcade) as Box<dyn overlay_engine::broadcaster::ArcBroadcaster>);

    // Advertiser — issues SHIP/SLAP on-chain ads announcing what topics /
    // lookup services this overlay carries. Requires SERVER_PRIVATE_KEY +
    // HOSTING_URL. If either is missing (dev / misconfigured deploys), fall
    // back to `None` so the engine silently skips sync_advertisements rather
    // than failing startup.
    let advertiser: Option<Box<dyn overlay_engine::advertiser::Advertiser>> = (|| {
        let priv_hex = match env.secret("SERVER_PRIVATE_KEY") {
            Ok(s) => s.to_string(),
            Err(e) => {
                worker::console_log!("advertiser: SERVER_PRIVATE_KEY missing: {e}");
                return None;
            }
        };
        let priv_key = match bsv_rs::primitives::ec::PrivateKey::from_hex(&priv_hex) {
            Ok(k) => k,
            Err(e) => {
                worker::console_log!("advertiser: SERVER_PRIVATE_KEY not valid hex: {e}");
                return None;
            }
        };
        let hosting = match hosting_url.clone() {
            Some(h) => h,
            None => {
                worker::console_log!("advertiser: HOSTING_URL not set — skipping");
                return None;
            }
        };
        let wallet_url = env
            .var("WALLET_STORAGE_URL")
            .ok()
            .map(|v| v.to_string())
            .unwrap_or_else(|| crate::wallet::client::DEFAULT_WALLET_STORAGE_URL.to_string());
        match crate::advertiser::CloudflareAdvertiser::new(
            priv_key,
            hosting.clone(),
            wallet_url.clone(),
            ship_storage_for_ad,
            slap_storage_for_ad,
        ) {
            Ok(a) => {
                worker::console_log!(
                    "advertiser: initialized hosting={} wallet_url={}",
                    hosting,
                    wallet_url
                );
                Some(Box::new(a) as Box<dyn overlay_engine::advertiser::Advertiser>)
            }
            Err(e) => {
                worker::console_log!(
                    "CloudflareAdvertiser init failed — sync_advertisements will no-op: {e}"
                );
                None
            }
        }
    })();

    let mut engine = Engine::with_all(
        managers,
        lookup_services,
        storage,
        advertiser,
        Some(Box::new(WorkerBroadcaster)), // SHIP broadcaster
        arc_broadcaster,
        chain_tracker,
        config,
    );

    engine.set_ship_lookup_client(Box::new(crate::ship_lookup::WorkerShipLookupClient));
    engine.set_ship_bootstrap_peers(
        crate::routes::suffixed_name("tm_uhrp", &topic_suffix),
        vec!["https://overlay-us-1.bsvb.tech".to_string()],
    );

    // Enable GASP sync with HTTP-based peer communication
    engine.set_gasp_remote_factory(Box::new(crate::gasp_remote::WorkerGASPRemoteFactory));

    // bsv-low#302: per-peer GASP sync budget. The #257 fix bounds the whole
    // cron STEP at GASP_SYNC_BUDGET_MS, but ONE dead ephemeral peer (ngrok
    // tunnel / dead host imported via SHIP ads) could still burn the entire
    // step budget because the per-peer fetch had no timeout. Each peer now
    // gets its own slice; a peer exceeding it is dropped loudly, recorded
    // as a failed sync (feeding the quarantine), and the loop continues —
    // so one dead peer costs 30 s, not the tick. Applies to the cron AND
    // /admin/startGASPSync (same engine builder).
    engine.set_peer_sync_budget(
        std::rc::Rc::new(|ms| {
            Box::pin(crate::broadcaster::sleep_ms(ms)) as overlay_engine::engine::SleepFuture
        }),
        GASP_PEER_SYNC_BUDGET_MS,
    );
    // bsv-low #559 (the delta lens's DELTA-3): no deadline drops a finalize
    // submit between its writes, so a D1 call that never answers inside one
    // held the 30 s peer budget AND the 240 s belt. Each transaction's
    // finalize submit now has a bound of its own over every storage call
    // and hook inside it (the lens fold of 2026-10-07, F2): a call still
    // unanswered past it is that call's fault, the submit is NEVER dropped
    // between its writes, its UTXO fails and the cursor stays.
    engine.set_finalize_submit_budget(
        std::rc::Rc::new(|ms| {
            Box::pin(crate::broadcaster::sleep_ms(ms)) as overlay_engine::engine::SleepFuture
        }),
        GASP_FINALIZE_SUBMIT_BUDGET_MS,
    );
    // bsv-low #555 (measured as #582): a graph whose walk passes its OWN
    // budget (inside the per-peer one) is DEFERRED with its walk kept in
    // `gasp_deferred_graphs` and resumed next tick, so one deep graph neither
    // restarts every tick nor holds the peer's other UTXOs. Same builder for
    // the cron and /admin/startGASPSync.
    engine.set_graph_budget(
        std::rc::Rc::new(|ms| {
            Box::pin(crate::broadcaster::sleep_ms(ms)) as overlay_engine::engine::SleepFuture
        }),
        crate::gasp_deferred::GASP_GRAPH_BUDGET_CALLS,
        crate::gasp_deferred::GASP_GRAPH_BUDGET_MS,
    );
    // bsv-low #586: the same budget's bytes and nodes limbs. A graph served
    // that many bytes, or appending that many nodes, in one pass is deferred
    // and resumed like one past its calls: a budget per pass, never a limit.
    // The vars GASP_GRAPH_BUDGET_BYTES / GASP_GRAPH_BUDGET_NODES, clamped;
    // unset, the defaults (a budget per pass; no record cap since #585).
    let (limb_bytes, limb_nodes) = crate::gasp_deferred::graph_budget_limbs_from_env(env);
    engine.set_graph_budget_limbs(limb_bytes, limb_nodes);

    // Chain-backed proof fetcher (#192/#193): the courier ladder
    // (Arcade→WoC→Bitails) with a MANDATORY chaintracks re-verify before any
    // BUMP is returned. This is the proof source the cron's
    // `complete_missing_proofs` (transactions store) and the pot-store
    // completion tick call to turn a proofless stored BEEF into a proven one.
    // Without a chain tracker it degrades to a pure retry (no proof can ever be
    // verified — fail-closed).
    let proof_tracker = lookup_service_chain_tracker(env);
    let sync_fetcher =
        courier_fetcher(env, proof_tracker.clone()).with_budget(GASP_PROOF_FETCH_BUDGET);
    engine.set_gasp_ancestor_fetcher(std::rc::Rc::new(sync_fetcher));
    let proof_fetcher = courier_fetcher(env, proof_tracker);
    engine.set_ancestor_fetcher(std::rc::Rc::new(proof_fetcher));

    // bsv-low #575 (the E1D delta-2 fold, L2): a carried predecessor the engine would land first from a
    // successor's BEEF is asked of the eviction ledger BEFORE its write (`admit_fast::landing_guard`), at every
    // door this engine serves; `guard_landed` after the write stays the belt.
    engine.set_landing_guard(std::rc::Rc::new(move |txid: &str, topic: &str| {
        let (db, txid, topic) = (ledger.clone(), txid.to_string(), topic.to_string());
        Box::pin(async move { crate::admit_fast::landing_guard(&db, &txid, &topic).await })
            as overlay_engine::engine::LandingGuardFuture
    }));

    // bsv-low #592: the engine's own script walk (every submit that walks: the queue's historical-tx replay, the
    // ungated modes, `/admin/readmit`, the peer crawler's current-tx; and the GASP anchor check) runs under the
    // door's budget, the instance the gated door walks under. A breach is counted (`ops::note_engine_walk`), never
    // a refusal.
    engine.set_walk_budget(WORKER_WALK_BUDGET);

    engine
}

/// The budget of the engine's own script walk in this Worker (bsv-low #592, `Engine::set_walk_budget`): the door's
/// own, `DoorBudget::DEFAULT`, under which the gated door walks (`verify_scripts_only`); pinned equal
/// (`beef_door_replay::e592`). Three eighths of a 128 MB isolate for the memory limb.
pub const WORKER_WALK_BUDGET: overlay_engine::engine::DoorBudget =
    overlay_engine::engine::DoorBudget::DEFAULT;

/// PURE (bsv-low#257): race `fut` against `deadline`; `None` = the deadline
/// won and `fut` was DROPPED (its in-flight work cancelled). Injectable
/// deadline so the control flow is natively unit-tested; the scheduled
/// handler passes `broadcaster::sleep_ms`. Generalized into the engine
/// crate for bsv-low#302 (the per-peer GASP budget reuses it) — this is
/// the same function, re-exported to keep every existing call site.
use overlay_engine::gasp::race_or_deadline;

/// Per-step wall-clock budgets for the `*/15` scheduled tick (bsv-low#257).
///
/// ROOT CAUSE, proven live 2026-07-30: EVERY cron invocation since the
/// 2026-07-02 deploy died at the platform's 15-minute wall-clock kill
/// (workersInvocationsAdaptive: exactly one internalError per 15-min
/// bucket, wallTime ≈ 900,000,000 µs, ≈112.5 GB-s at 128 MB — the
/// metronomic #257 signature), because `engine.start_gasp_sync()` runs
/// UNBOUNDED: `/admin/startGASPSync` (the same call on the fetch path)
/// ran 114 s and 240 s live with zero progress and never returned. The
/// discovered/bootstrapped peer set includes long-dead ephemeral hosts
/// (ngrok tunnels imported via SHIP ads), the per-peer sync has no
/// per-fetch timeout, and errors continue to the NEXT peer — so the step
/// can exceed the whole 15-minute budget, and every step after it (peer
/// crawl, proof passes, janitor, ops heartbeat, the #273 backstop) never
/// ran from cron. Completion has been carried solely by the external
/// /admin/complete-proofs poke since deploy day.
///
/// The fix direction: every network-bound cron step gets a bounded slice
/// and a LOUD timeout log; a timed-out step is dropped (its work is
/// idempotent — GASP cursors persist only per completed peer sync, crawl
/// submits are dupe-checked, janitor evictions retry) and the tick moves
/// on. Budgets sum to ≤ 9 min of network steps, leaving headroom for the
/// bounded passes inside the 15-min cap.
// Discovery uses SHIP_DISCOVERY_MAX_HOSTS_PER_TICK = 4 additional providers
// plus the four ship_slap_bootstrap trackers, resolved once per invocation.
// SHIP_DISCOVERY_LOOKUP_TIMEOUT_MS = 5_000 bounds each complete lookup.
// With three SHIP topics, at most 4 SLAP + 3 * 8 SHIP calls cost 140 s.
// GASP_SYNC_BUDGET_MS remains the outer 240 s deadline; discovery calls are
// shorter than GASP_PEER_SYNC_BUDGET_MS = 30_000 so a failed host is isolated.
pub(crate) const GASP_SYNC_BUDGET_MS: u64 = 240_000;
const PEER_CRAWL_BUDGET_MS: u64 = 120_000;
const JANITOR_BUDGET_MS: u64 = 180_000;

/// bsv-low#309: wall-clock slice for the advert-lifecycle passes (reap plus
/// spend-confirm). Internally bounded already (1 tip read, ≤50 D1 deletes,
/// 16 candidates × ≤3 provider GETs), so 60 s is generous headroom for slow
/// providers; a timed-out pass is dropped (both passes are idempotent —
/// candidates are simply revisited next tick) and the tick moves on.
const ADVERT_LIFECYCLE_BUDGET_MS: u64 = 60_000;

/// bsv-low#302: wall-clock slice ONE GASP peer's sync may consume. 30 s is
/// generous for a healthy peer (a page fetch is seconds) while letting the
/// 240 s step survive several dead peers AND still reach live ones; the
/// step-level GASP_SYNC_BUDGET_MS stays as the outer belt.
const GASP_PEER_SYNC_BUDGET_MS: u64 = 30_000;

/// Sync alone gets 60 transaction attempts: a planning allowance of 500 ms
/// per transaction in the 30 s peer slice. The wall deadline remains the
/// actual time bound. At most 3 raw GETs + 5 proof GETs + 3 header reads =
/// 11 subrequests per ancestor attempt, hence 60 * 11 = 660 of the paid
/// plan's 1,000 ceiling, leaving 340 for pages, storage and other passes.
/// The 500 ms is a sizing assumption, not a measured latency or a minimum.
/// This is a courier allowance, not a guarantee for the whole tick's I/O.
const GASP_PROOF_FETCH_BUDGET: u32 = (GASP_PEER_SYNC_BUDGET_MS / 500) as u32;

/// The bound of ONE transaction's GASP finalize submit (bsv-low #559): a
/// handful of D1 calls and the lookup hooks, milliseconds when D1 answers.
/// The per-peer budget and the step belt both WAIT for a submit in flight,
/// so this is what keeps a hung D1 call from holding them.
const GASP_FINALIZE_SUBMIT_BUDGET_MS: u64 = 30_000;

/// Completion has 30 s and plans 500 ms per candidate: 60 attempts.
/// Proof-only work uses at most 5 HTTP reads + 3 header reads + 3 fault
/// counter writes: 60 * 11 = 660 of the paid 1,000 subrequests. This leaves
/// 340 for storage, setup and other work. This is an allowance
/// per pass, not a guarantee for the whole invocation's aggregate traffic.
const TRANSACTION_PROOF_COMPLETION_BUDGET_MS: u64 = 30_000;
const TRANSACTION_PROOF_FETCH_BUDGET: u32 = (TRANSACTION_PROOF_COMPLETION_BUDGET_MS / 500) as u32;
const TRANSACTION_PROOF_COMPLETION_LIMIT: u64 = TRANSACTION_PROOF_FETCH_BUDGET as u64;

fn transaction_proof_fetcher(env: &Env) -> crate::proof_fetcher::ChainProofFetcher {
    courier_fetcher(env, lookup_service_chain_tracker(env))
        .with_budget(TRANSACTION_PROOF_FETCH_BUDGET)
}

#[event(scheduled)]
async fn scheduled(_event: worker::ScheduledEvent, env: Env, _ctx: worker::ScheduleContext) {
    tick_alarm::arm_from_cron(&env).await;
    tick::run_tick(&env, tick::TickSource::Cron).await;
}

/// POST /admin/complete-proofs (#192/#193) — run the BEEF proof-completion passes
/// ON DEMAND, the SAME logic the `*/15` cron runs (observed firing on beta, bsv-low
/// #649 D4-L1), from an authenticated admin FETCH route, an ADDITIONAL trigger
/// beside the scheduled one. Self-contained (builds its own db/engine/storages from env) +
/// fail-closed: a BUMP is stitched only once chaintracks-verified; the `has_proof`
/// latch + serve-time trim trust only verified proofs. An external cron POSTs this
/// (bearer-authed via ADMIN_TOKEN, gated at the dispatch). Returns the counters.
/// POST /admin/reverifyPotBeefs[?limit=N] (bsv-low#304 gate M-2) — one-shot
/// bulk drain of the pot_beefs verified-latch backlog. Runs the SAME
/// completion pass the cron runs but with a WIDE candidate page and a
/// ZERO-budget fetcher: every structurally-bumped candidate gets its STORED
/// bump chaintracks-re-verified (one service-binding read each — never a
/// courier fetch); genuine → `mark_pot_beef_proven`, fake → honestly left
/// for the budgeted cron pass to replace. Rows without a structural bump
/// simply count `still_unconfirmed` (the zero budget refuses the courier
/// path). Bearer-authed at the dispatch like every admin POST. The operator
/// drives it repeatedly post-deploy until `already_proven` returns 0 —
/// `limit` defaults to ADMIN_REVERIFY_DEFAULT_LIMIT, capped at
/// ADMIN_REVERIFY_MAX_LIMIT (subrequest-wall math at the consts).
async fn admin_reverify_pot_beefs(env: &Env, req: &Request) -> worker::Result<Response> {
    let limit = req
        .url()
        .ok()
        .and_then(|u| {
            u.query_pairs()
                .find(|(k, _)| k == "limit")
                .and_then(|(_, v)| v.parse::<u64>().ok())
        })
        .unwrap_or(crate::proof_fetcher::ADMIN_REVERIFY_DEFAULT_LIMIT)
        .clamp(1, crate::proof_fetcher::ADMIN_REVERIFY_MAX_LIMIT);

    let db = match env.d1("OVERLAY_DB") {
        Ok(d) => Rc::new(d),
        Err(e) => return Response::error(format!("reverify-pot-beefs: D1 binding: {e}"), 500),
    };
    if let Err(e) = ensure_overlay_migrations(&db).await {
        return Response::error(format!("reverify-pot-beefs: migrations: {e}"), 500);
    }
    let pot_storage: Rc<dyn PotStorage> = Rc::new(D1PotStorage::new(db));

    // Chaintracks-only: budget 0 makes every courier attempt refuse
    // (fail-closed None → still_unconfirmed), so this route can never turn
    // into a WoC/Bitails hammer no matter how wide the page is.
    let fetcher = crate::proof_fetcher::ChainProofFetcher::new(lookup_service_chain_tracker(env))
        .with_budget(0);
    // min_age 0: the drain is operator-driven and the fast path only ever
    // LATCHES verified truth — the #228 push-primary age gate protects
    // courier polling, which budget 0 already forbids.
    let s =
        crate::proof_fetcher::complete_pot_beef_proofs(pot_storage.as_ref(), &fetcher, limit, 0)
            .await;
    Response::from_json(&serde_json::json!({
        "status": "ok",
        "limit": limit,
        "scanned": s.scanned,
        "already_proven": s.already_proven,
        "completed": s.completed,
        "still_unconfirmed": s.still_unconfirmed,
        "stitch_failed": s.stitch_failed,
    }))
}

/// POST /admin/advert-lifecycle[?spendLimit=N&reapLimit=M] (bsv-low#309) —
/// run the LOW advert-lifecycle passes ON DEMAND: the expired-advert reaper
/// (fail-CLOSED — a null tip reaps NOTHING, the #148 lesson) then the
/// advert-outpoint spend-confirm probe (delete ONLY on a raw-verified
/// spend; unknown never deletes). Same logic + shared runner as the
/// scheduled tick (`run_advert_lifecycle` — the order cannot drift apart);
/// bearer-authed at the dispatch like every admin POST. `spendLimit` /
/// `reapLimit` widen a post-deploy backlog drain, clamped at the
/// subrequest-wall caps (math at the consts).
async fn admin_advert_lifecycle(env: &Env, req: &Request) -> worker::Result<Response> {
    let query_limit = |name: &str, default: u64, max: u64| {
        req.url()
            .ok()
            .and_then(|u| {
                u.query_pairs()
                    .find(|(k, _)| k == name)
                    .and_then(|(_, v)| v.parse::<u64>().ok())
            })
            .unwrap_or(default)
            .clamp(1, max)
    };
    let spend_limit = query_limit(
        "spendLimit",
        crate::advert_lifecycle::ADVERT_SPEND_CHECK_LIMIT,
        crate::advert_lifecycle::ADVERT_SPEND_CHECK_MAX_LIMIT,
    );
    let reap_limit = query_limit(
        "reapLimit",
        crate::advert_lifecycle::ADVERT_REAP_LIMIT,
        crate::advert_lifecycle::ADVERT_REAP_MAX_LIMIT,
    );

    let db = match env.d1("OVERLAY_DB") {
        Ok(d) => Rc::new(d),
        Err(e) => return Response::error(format!("advert-lifecycle: D1 binding: {e}"), 500),
    };
    if let Err(e) = ensure_overlay_migrations(&db).await {
        return Response::error(format!("advert-lifecycle: migrations: {e}"), 500);
    }
    let low_storage: Rc<dyn LowStorage> = Rc::new(D1LowStorage::new(db));

    // The SAME tip source the ls_low expiry filter uses; unresolved →
    // the reaper refuses (fail-closed) while the spend probe still runs
    // (it needs no tip — chain truth is age-independent).
    let tracker = lookup_service_chain_tracker(env);
    let tip = crate::advert_lifecycle::resolve_tip(tracker.as_deref()).await;
    let (reap, spend) = crate::advert_lifecycle::run_advert_lifecycle(
        low_storage.as_ref(),
        tip,
        None,
        spend_limit,
        reap_limit,
        crate::proof_fetcher::ProofSources::from_env(env),
    )
    .await;

    Response::from_json(&serde_json::json!({
        "status": "ok",
        "tip": tip,
        "spend_limit": spend_limit,
        "reap_limit": reap_limit,
        "reap_tip_resolved": reap.tip_resolved,
        "reap_scanned": reap.scanned,
        "reaped": reap.reaped,
        "reap_delete_failed": reap.delete_failed,
        "spend_scanned": spend.scanned,
        "spend_deleted": spend.deleted,
        "spend_not_spent": spend.not_spent,
        "spend_unknown": spend.unknown,
        "spend_delete_failed": spend.delete_failed,
    }))
}

async fn admin_complete_proofs(env: &Env) -> worker::Result<Response> {
    let db = match env.d1("OVERLAY_DB") {
        Ok(d) => Rc::new(d),
        Err(e) => return Response::error(format!("complete-proofs: D1 binding: {e}"), 500),
    };
    if let Err(e) = ensure_overlay_migrations(&db).await {
        return Response::error(format!("complete-proofs: migrations: {e}"), 500);
    }
    let pot_storage: Rc<dyn PotStorage> = Rc::new(D1PotStorage::new(db.clone()));
    let ops_db = db.clone();
    let engine = match build_engine_from_env(env).await {
        Ok(e) => e,
        Err(e) => return Response::error(format!("complete-proofs: engine: {e}"), 500),
    };
    // 1. Transactions store (engine + ancestor fetcher). Submitted rows give
    //    /arc-ingest its callback window; GASP rows have no callback to await.
    let completion_fetcher = transaction_proof_fetcher(env);
    let backstop_age = crate::proof_fetcher::PUSH_BACKSTOP_MIN_AGE_SECS;
    let (tx_completed, tx_fetch_failed, tx_early_gasp) =
        match overlay_engine::gasp::race_or_deadline(
            engine.complete_missing_proofs_with(
                &completion_fetcher,
                TRANSACTION_PROOF_COMPLETION_LIMIT,
                backstop_age,
            ),
            crate::broadcaster::sleep_ms(TRANSACTION_PROOF_COMPLETION_BUDGET_MS),
        )
        .await
        {
            Some(Ok(s)) => (
                s.completed as u64,
                s.fetch_failed as u64,
                s.early_gasp as u64,
            ),
            Some(Err(e)) => {
                worker::console_log!("complete-proofs: transactions error: {e}");
                (0, 0, 0)
            }
            None => {
                worker::console_log!(
                    "complete-proofs: transaction deadline reached; retry next run"
                );
                (0, 0, 0)
            }
        };
    // 2+3. Pot-store maintenance — chaser BEFORE the pot-beef bulk drain,
    //    encoded once in run_pot_maintenance (bsv-low#304 gate M-5; same
    //    order as the scheduled tick). Own fetchers, own budget cells.
    let spend_fetcher = courier_fetcher(env, lookup_service_chain_tracker(env)).with_budget(20);
    let pot_tracker = lookup_service_chain_tracker(env);
    let pot_fetcher = courier_fetcher(env, pot_tracker);
    let (ss, ps) = crate::proof_fetcher::run_pot_maintenance(
        pot_storage.as_ref(),
        &spend_fetcher,
        20,
        &pot_fetcher,
        crate::proof_fetcher::POT_PROOF_PASS_LIMIT,
        backstop_age,
    )
    .await;
    // 4. pot_records decoded-params lazy backfill (#284) — no courier/tracker,
    //    pure re-reads of our own admitted pot_beefs bytes.
    let bf = crate::proof_fetcher::backfill_decoded_params(
        pot_storage.as_ref(),
        crate::proof_fetcher::PARAMS_BACKFILL_LIMIT,
    )
    .await;
    // 4a. bsv-low #406: settleSigners historic backfill (see the scheduled
    //     tick) — pokeable so the beta census can be converged on demand.
    let signers_bf = crate::proof_fetcher::backfill_settle_signers(
        pot_storage.as_ref(),
        crate::proof_fetcher::SETTLE_SIGNERS_BACKFILL_LIMIT,
    )
    .await;
    // 4a'. bsv-low #468: spender payouts historic backfill (see the scheduled
    //      tick) — pokeable so the beta census converges on demand.
    let payouts_bf = crate::proof_fetcher::backfill_spender_payouts(
        pot_storage.as_ref(),
        crate::proof_fetcher::SPENDER_PAYOUTS_BACKFILL_LIMIT,
    )
    .await;
    // 4b. the #355/#367 RE-LATCH fixpoint over the verdict columns (four
    //     arms since brain-cutover M1: sigValid, markerValid, claimValid,
    //     rowValid) — same bounds as the scheduled tick; pokeable so a
    //     predicate change can be converged without waiting out the cron
    //     (the cron-poker doctrine).
    let relatch_summaries =
        crate::relatch::run_relatch(db.clone(), crate::relatch::RELATCH_PAGE_LIMIT).await;
    for summary in &relatch_summaries {
        crate::relatch::log_relatch_summary(summary);
    }
    let relatch_json: Vec<serde_json::Value> = relatch_summaries
        .iter()
        .map(|s| {
            serde_json::json!({
                "table": s.table,
                "scanned": s.scanned,
                "changed": s.changed(),
                "latched": s.latched,
                "promoted": s.promoted,
                "demoted": s.demoted,
                "remaining": s.remaining,
                "still_null": s.still_null,
                "cursor": s.cursor,
                "sweeps": s.sweeps,
                "wrapped": s.wrapped,
                "errors": s.errors,
            })
        })
        .collect();
    // 4b. terminal-retire classifier (INCIDENT D1-CALLBACK-FLOOD 2026-09-01)
    //     — BEFORE the backstop, same as the scheduled twin: a row retired
    //     this pass must not be re-presented by the next step.
    let arcade_base = env
        .var("ARCADE_URL")
        .ok()
        .map(|v| v.to_string())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| crate::broadcaster::ARCADE_DEFAULT_URL.to_string());
    // #602 fold (MEDIUM-2): the conflict pass's GETs ride the WoC key when one is installed.
    let woc_key = env.secret("WOC_API_KEY").ok().map(|k| k.to_string());
    let retire = crate::proof_fetcher::run_retire_pass(
        &ops_db,
        &arcade_base,
        woc_key.as_deref(),
        crate::proof_fetcher::ProofSources::from_env(env),
    )
    .await;
    worker::console_log!(
        "complete-proofs: retire-pass — scanned={} retired={} kept_present={} kept_uncertain={} conflict: scanned={} evicted={} faults={}",
        retire.scanned,
        retire.retired,
        retire.kept_present,
        retire.kept_uncertain,
        retire.conflict.scanned,
        retire.conflict.evicted,
        retire.conflict.faults,
    );
    // 4b'. bsv-low #649 D3-H1: the parked pot and hop notes (a background flush withheld them), delivered by the
    //    SHARED drain the scheduled handler runs (D4-L1): the `*/15` tick is the observed trigger and this route
    //    the additional authenticated admin path, which drains when invoked. One bounded pass (128 rows a set,
    //    10 s to start its POSTs), counted `change_park_drain_complete_proofs_total`.
    let parked = crate::change_park::drain_parked(
        env,
        &ops_db,
        crate::change_park::DrainTrigger::CompleteProofs,
    )
    .await;
    let change_parked = parked.map(|d| {
        serde_json::json!({
            "read": d.read,
            "unparked": d.unparked,
            "kept": d.kept,
            "refused": d.refused,
            "exhausted": d.exhausted,
            "swept": d.swept,
        })
    });
    // 4c. admitted-but-network-absent rebroadcast backstop (bsv-low #273) —
    //     runs last, own bounds + 30min–14d candidacy bracket (gate LOW-1) +
    //     the incident's attempt cap; see the scheduled block's note.
    let tx_storage = D1Storage::new(db.clone());
    let taal_key = env.secret("TAAL_API_KEY").ok().map(|s| s.to_string());
    let rb = crate::proof_fetcher::run_rebroadcast_backstop(
        &tx_storage,
        taal_key.as_deref(),
        None,
        crate::proof_fetcher::ProofSources::from_env(env),
    )
    .await;
    // 5. observability heartbeat + counters (same as the cron would stamp).
    let proofs_completed = tx_completed + ps.completed as u64;
    let fetch_failed = tx_fetch_failed + ps.fetch_failed as u64;
    crate::ops::record_completion_tick(
        &ops_db,
        proofs_completed,
        fetch_failed,
        ps.completed as u64,
        // A displacement latches spentConfirmed the same as a confirm — the
        // ops heartbeat counts both or displaced rows go invisible.
        (ss.confirmed + ss.displaced) as u64,
    )
    .await;
    let flagged = crate::ops::refresh_proofless_watch(&ops_db).await;
    let mut answer = serde_json::json!({
        "status": "ok",
        "tx_completed": tx_completed,
        "pot_completed": ps.completed,
        "pot_scanned": ps.scanned,
        "pot_already_proven": ps.already_proven,
        "pot_still_unconfirmed": ps.still_unconfirmed,
        "fetch_failed": fetch_failed,
        "spends_confirmed": ss.confirmed,
        "spends_scanned": ss.scanned,
        "spends_still_unconfirmed": ss.still_unconfirmed,
        // bsv-low#304 gate M-5: proof/header READ faults (subrequest wall /
        // transport) — distinguishable from "not mined yet".
        "spends_tracker_faults": ss.tracker_faults,
        // bsv-low#301: confirmed-CAS misses — the pointer moved between the
        // chaser's read and its write; nothing was confirmed on the stale
        // read (the row re-chases or was competing-confirmed).
        "spends_cas_missed": ss.cas_missed,
        // bsv-low#301 gate M2: CAS write ERRORS (driver/storage fault —
        // distinct from a guard miss). A total failure of the RETURNING
        // statement self-announces as scanned>0 & confirmed=0 & this >0.
        "spends_cas_errors": ss.cas_errors,
        // 2026-08-18 reconcile: rows whose never-mined claim was DISPLACED by
        // the chaintracks-proven actual spender; attempts/faults make a dead
        // or starved reconcile leg self-announce instead of reading as "the
        // chain simply has no hint".
        "spends_displaced": ss.displaced,
        "spends_displace_attempts": ss.displace_attempts,
        "spends_displace_faults": ss.displace_faults,
        // #284 decoded-params backfill counters.
        "params_scanned": bf.scanned,
        "params_decoded": bf.decoded,
        "params_verdicts": bf.verdicts,
        "params_missing_beef": bf.missing_beef,
        // bsv-low #406 settleSigners backfill counters.
        "signers_scanned": signers_bf.scanned,
        "signers_latched": signers_bf.latched,
        "signers_unresolved": signers_bf.unresolved,
        "signers_missing_beef": signers_bf.missing_beef,
        "payouts_scanned": payouts_bf.scanned,
        "payouts_measured": payouts_bf.measured,
        "payouts_missing_beef": payouts_bf.missing_beef,
        // #355/#367 re-latch counters, per table. `changed` is the fixpoint's
        // progress AND the predicate-regression detector; `demoted` is the
        // alarm (rows the predicate now refuses that it previously accepted);
        // `still_null` is the legacy tier's remaining size.
        "relatch": relatch_json,
        // #273 rebroadcast-backstop counters.
        "rebroadcast_scanned": rb.scanned,
        "rebroadcast_present": rb.present,
        "rebroadcast_inconclusive": rb.inconclusive,
        "rebroadcast_rescued": rb.rebroadcast,
        "rebroadcast_failed": rb.rebroadcast_failed,
        "rebroadcast_budget_skipped": rb.budget_skipped,
        "rebroadcast_attempted": rb.attempted.len(),
        // INCIDENT D1-CALLBACK-FLOOD 2026-09-01 — the retire pass's tick.
        "retire_scanned": retire.scanned,
        "retire_retired": retire.retired,
        "retire_kept_present": retire.kept_present,
        "retire_kept_uncertain": retire.kept_uncertain,
        // Observability only (≤5): which spending txids were sampled, so an
        // operator can check them on a block explorer and distinguish a broken
        // chaser from a genuinely unconfirmable backlog.
        "spends_sample": ss.sample,
        "proofless_over_24h": flagged,
    });
    answer["tx_early_gasp"] = serde_json::json!(tx_early_gasp);
    // bsv-low #649 D3-H1: the shared parked drain's pass (`null`: no app layer configured, nothing read); set apart
    // from the literal above, which sits at `json!`'s recursion limit.
    answer["change_parked"] = change_parked.unwrap_or(serde_json::Value::Null);
    Response::from_json(&answer)
}

/// Queue consumer for the onSteakReady pattern — and the S2 replay
/// (queue-durable admission, bsv-low 2026-08-29).
///
/// Processes mutation messages enqueued by /submit. Each message contains a
/// BEEF + topics + mode. The consumer builds an Engine and calls
/// `engine.submit_with_report()` under the REPLAY mode
/// (`queue::replay_submit_mode` — never a re-broadcast), which includes
/// Phase 3 mutations, and acks ONLY a durable report: a replay that still
/// faults is handed back (`retry`) for the platform's backoff and
/// dead-letters after `max_retries` — never silently dropped.
///
/// Dedup safety: `applied_transactions` in D1 ensures at-least-once delivery
/// is safe — a topic whose every write landed is detected and skipped in
/// Phase 1, and a topic that faulted is never recorded as applied, so its
/// replay is re-validated and re-written (idempotent backend writes).
/// The Worker's `queue` export, written out as worker-macros 0.8.5's `event(queue)` attribute writes it
/// (`event.rs`: a synchronous wrapper returning a Promise, a panic on `Err`), so the batch arrives as the
/// platform's own object (bsv-low #576's lens fold, H1): the DLQ consumer reads each message's `attempts`, which
/// workers-rs 0.8.5's `Message` does not expose, to time its backoff and to know its LAST delivery.
mod _worker_queue {
    use ::worker::wasm_bindgen;
    use ::worker::wasm_bindgen::prelude::*;

    #[wasm_bindgen]
    pub fn queue(
        event: ::worker::worker_sys::MessageBatch,
        env: ::worker::Env,
        ctx: ::worker::worker_sys::Context,
    ) -> ::worker::js_sys::Promise {
        ::worker::js_sys::futures::future_to_promise(::std::panic::AssertUnwindSafe(async move {
            let ctx = ::worker::Context::new(ctx);
            if let Err(e) = super::queue_entry(event, env, ctx).await {
                ::worker::console_log!("{}", &e);
                panic!("{}", e);
            }
            Ok(::worker::wasm_bindgen::JsValue::UNDEFINED)
        }))
    }
}

async fn queue_entry(
    event: worker::worker_sys::MessageBatch,
    env: Env,
    ctx: worker::Context,
) -> worker::Result<()> {
    // bsv-low #576: a batch from the dead letter queue is PARKED in D1, never replayed here (the operator's lever
    // re-drives it).
    let name: String = event.queue().map(String::from).unwrap_or_default();
    if crate::dead_letters::is_dead_letter_queue(&name) {
        return crate::dead_letters::park_batch(&event, &env).await;
    }
    queue_handler(worker::MessageBatch::from(event), env, ctx).await
}

#[rustfmt::skip] // Preserve main's exact queue wiring pins.
async fn queue_handler(
    batch: worker::MessageBatch<crate::queue::MutationMessage>,
    env: Env,
    ctx: worker::Context,
) -> worker::Result<()> {
    use overlay_engine::types::{SubmitMode, TaggedBEEF};
    use worker::MessageExt;

    let engine = build_engine_from_env(&env)
        .await
        .map_err(|e| worker::Error::from(format!("Queue engine build failed: {e}")))?;

    // bsv-low #576's lens fold (N4, M1): every message keeps its handle, so nothing is dropped here unseen: a body
    // that does not decode and one whose BEEF is not base64 are handed back like any fault, dead-letter after their
    // replays and are PARKED (the undecodable one exhausted), where the platform used to ack them silently.
    let counters = env.d1("OVERLAY_DB").ok();
    // bsv-low #585 (door 3, the deletion rule): the R2 objects of the messages this batch ACKED, deleted after the
    // loop (a batch that dies midway leaves objects nothing names, never a redelivery that finds its bytes gone).
    let mut acked_objects: Vec<String> = Vec::new();
    for msg in batch.raw_iter() {
        let body: crate::queue::MutationMessage = match worker::serde_wasm_bindgen::from_value(
            msg.body(),
        ) {
            Ok(b) => b,
            Err(e) => {
                worker::console_log!("Queue: failed to deserialize message ({e}) — retrying; it dead-letters and is parked");
                if let Some(db) = &counters {
                    crate::ops::bump_counter(db, crate::ops::COUNTER_QUEUE_MUTATION_RETRIED, 1)
                        .await;
                }
                msg.retry();
                continue;
            }
        };
        let body = &body;

        // NL-6c: a deferred EF job carries its reference, never bytes; its run
        // settles its own row (the arm's answer, or queued again for the cron),
        // so the message is acked once the run returns. A run that kills the
        // isolate never reaches the ack: the bound is `ef_deferred::run_job`'s doc (N4).
        if let Some(reference) = body.ef_job.as_deref() {
            crate::ef_deferred::run_job(&env, &ctx, &engine, reference).await;
            msg.ack();
            continue;
        }

        // bsv-low #585 (door 3): a message that names an R2 object replays THAT object's bytes, read and checked
        // (length, sha256, the consumer's own BEEF policy), exactly as an inline body from here on. A read fault
        // and a mismatch are the replay's FAULT (never "not now"): handed back, dead-lettered, parked with the key.
        // A MISSING object (the d3 fold, the twin) is judged by the message's SUBJECT: a twin of the same bytes,
        // topics and mode shares the object and the first ack deleted it, so a subject under an open eviction or
        // with an applied row in every topic named is acked (a dupe, no letter, nothing to delete: the object is
        // gone); one not shown landed is the replay's FAULT, as before.
        // The d3 fold-2 (E585-D3-L3): the read and the use of the verdict are `read_for_replay`, run as is by the
        // lib's tests; this arm only acts on its step. Its MISSING fault notes KEEP the letter's class (L4).
        let ports = crate::queue::WorkerBytes {
            env: &env,
            db: counters.as_ref(),
        };
        let step = crate::queue::read_for_replay(&ports, body).await;
        if let (
            crate::queue::ReadStep::Acked(_) | crate::queue::ReadStep::Fault { missing: true, .. },
            Some(db),
        ) = (&step, &counters)
        {
            crate::ops::bump_counter(db, crate::ops::COUNTER_BEEF_BLOBS_MISSING, 1).await;
        }
        let beef = match step {
            crate::queue::ReadStep::Bytes(b) => b,
            crate::queue::ReadStep::Acked(verdict) => {
                let (why, counter) = if verdict == crate::queue::MissingVerdict::Twin {
                    (
                        crate::dead_letters::Resolved::Twin,
                        crate::ops::COUNTER_QUEUE_R2_TWIN_ACKED,
                    )
                } else {
                    (
                        crate::dead_letters::Resolved::RefusedEvicted,
                        crate::ops::COUNTER_QUEUE_REPLAY_SKIPPED_EVICTED,
                    )
                };
                let subject = body.r2.as_ref().and_then(|r| r.txid.as_deref());
                worker::console_log!(
                    "Queue: the R2 object of {} is MISSING and its replay is acked: {}",
                    subject.unwrap_or("?"),
                    why.says()
                );
                if let Some(db) = &counters {
                    crate::ops::bump_counter(db, counter, 1).await;
                    let row_object = crate::dead_letters::resolve(db, body, subject, why).await;
                    acked_objects.extend(row_object);
                }
                msg.ack();
                continue;
            }
            crate::queue::ReadStep::Fault { fault, missing } => {
                worker::console_log!("Queue: {fault} — retrying; it dead-letters and is parked");
                if let Some(db) = &counters {
                    crate::ops::bump_counter(db, crate::ops::COUNTER_QUEUE_MUTATION_RETRIED, 1)
                        .await;
                    if missing {
                        crate::ops::bump_counter(db, crate::ops::COUNTER_QUEUE_R2_MISSING_FAULT, 1)
                            .await;
                        crate::dead_letters::note_failing_keeping_class(db, body, None, &fault)
                            .await;
                    } else {
                        crate::dead_letters::note_failing(
                            db,
                            body,
                            None,
                            &fault,
                            crate::dead_letters::LetterClass::Fault,
                        )
                        .await;
                    }
                }
                msg.retry();
                continue;
            }
        };

        let tagged_beef = TaggedBEEF {
            beef,
            topics: body.topics.clone(),
            off_chain_values: None,
        };

        let mode: SubmitMode = crate::queue::replay_submit_mode(&body.mode);
        // ── bsv-low LOOP 18 (2026-09-21): a replay never resurrects an evicted pot ──
        // The subject by the ONE rule (D5, `subject_txid_of` over the enqueued bytes: no EF conversion, so a
        // JOIN carrying a txid-only hop — the F-D class — is judged too; the gate's MEDIUM-3); a body that does not
        // parse is retried, never silently written. An open eviction skips the replay (acked: the twin holds the
        // rows, a MINED proof readmits); an unreadable ledger retries the replay later (never a blind write).
        let subject: Option<String> =
            beef_limits::parse_beef(&tagged_beef.beef, &beef_limits::QUEUE_BEEF_LIMITS)
                .ok()
                .and_then(|mut named| crate::ef::subject_txid_of(&mut named));
        let Some(subject) = subject else {
            worker::console_log!("Queue: the replay's subject could not be derived (an unparsable BEEF or no unique tip) — retrying");
            if let Some(db) = &counters {
                crate::ops::bump_counter(db, crate::ops::COUNTER_QUEUE_MUTATION_RETRIED, 1).await;
                crate::dead_letters::note_failing(db, body, None, "the replay's subject could not be derived (an unparsable BEEF or no unique tip)", crate::dead_letters::LetterClass::Fault).await;
            }
            msg.retry();
            continue;
        };
        if let Some(db) = &counters {
            match crate::admit_fast::open_eviction(db, &subject).await {
                Ok(Some(ev)) => {
                    crate::ops::bump_counter(db, crate::ops::COUNTER_QUEUE_REPLAY_SKIPPED_EVICTED, 1).await;
                    worker::console_log!(
                        "Queue: replay of {subject} SKIPPED — under an open eviction (at {} ms — {}); the twin holds it, a MINED proof readmits",
                        ev.evicted_at_ms,
                        ev.reason
                    );
                    let row_object = crate::dead_letters::resolve(
                        db,
                        body,
                        Some(&subject),
                        crate::dead_letters::Resolved::RefusedEvicted,
                    )
                    .await;
                    acked_objects.extend(row_object);
                    acked_objects.extend(body.r2.as_ref().map(|r| r.key.clone()));
                    msg.ack();
                    continue;
                }
                Ok(None) => {}
                Err(e) => {
                    crate::ops::bump_counter(db, crate::ops::COUNTER_ADMIT_FAST_LEDGER_UNREADABLE, 1).await;
                    worker::console_log!(
                        "Queue: the eviction ledger could not be read for {subject} ({e}) — retrying the replay later"
                    );
                    crate::dead_letters::note_failing(db, body, Some(&subject), &format!("the eviction ledger could not be read ({e})"), crate::dead_letters::LetterClass::Fault).await;
                    msg.retry();
                    continue;
                }
            }
        }

        // bsv-low #592 and the E592 lens fold: the replay's walk runs under the engine's budget. A walk past it goes
        // on as historical-tx-no-spv ONLY on a message the gated arm's producer marked (`gated`: the network accepted
        // these bytes), acked and never dead-lettered for its size; on any other it is "not now", retried, then
        // parked as a `not_now` letter (#576's class: one per txid, 200 a day), never admitted unrun.
        let policy = crate::queue::replay_breach_policy(body);
        let replayed = engine.submit_with_report_under(&tagged_beef, mode, policy).await;
        if let (Ok((_, report)), Some(db)) = (&replayed, &counters) {
            crate::ops::note_engine_walk(db, report, "Queue").await;
        }
        if let (Err(overlay_engine::engine::EngineError::WalkCouldNotRun(stop)), Some(db)) =
            (&replayed, &counters)
        {
            crate::ops::note_engine_walk_not_now(db, stop, "Queue").await;
        }
        // lane E1D's delta fold (L2): what the engine landed first from the BEEF (written whole, whatever the
        // subject's own report) is guarded like the replay's own write; the batch's end flush ships the notes
        if let (Ok((_, report)), Some(db)) = (&replayed, &counters) {
            if !report.landed_predecessors.is_empty() {
                crate::admit_fast::guard_landed(db, &report.landed_predecessors, "Queue").await;
            }
        }
        match replayed {
            Ok((_steak, report)) if report.is_durable() => {
                // the write-side guard, AFTER the replay's write (the gate's HIGH-1): an eviction that opened
                // while this replay was landing — its table loop missed the rows landing after each step — is
                // re-run over what the replay wrote; the replay is acked either way (its bytes are refused)
                if let Some(db) = &counters {
                    match crate::admit_fast::open_eviction(db, &subject).await {
                        Ok(Some(ev)) => {
                            let now_ms = worker::Date::now().as_millis();
                            let moved =
                                crate::admit_fast::evict_txid_everywhere(db, &subject, &ev.reason, now_ms)
                                    .await
                                    .moved;
                            crate::ops::bump_counter(db, crate::ops::COUNTER_ADMIT_FAST_REEVICTED_AFTER_WRITE, 1)
                                .await;
                            crate::pot_changes::flush_inline(env.clone()).await;
                            worker::console_log!(
                                "Queue: the replay of {subject} outran an eviction (at {} ms — {}): re-evicted {moved} row(s); acked",
                                ev.evicted_at_ms,
                                ev.reason
                            );
                            let row_object = crate::dead_letters::resolve(
                                db,
                                body,
                                Some(&subject),
                                crate::dead_letters::Resolved::ReEvicted,
                            )
                            .await;
                            acked_objects.extend(row_object);
                            acked_objects.extend(body.r2.as_ref().map(|r| r.key.clone()));
                            msg.ack();
                            continue;
                        }
                        Ok(None) => {}
                        Err(e) => {
                            crate::ops::bump_counter(db, crate::ops::COUNTER_ADMIT_FAST_LEDGER_UNREADABLE, 1).await;
                            worker::console_log!(
                                "Queue: the eviction ledger could not be read for {subject} after the replay's write ({e}) — retrying (a durable write dedups)"
                            );
                            crate::dead_letters::note_failing(
                                db,
                                body,
                                Some(&subject),
                                &format!("the eviction ledger could not be read after the replay's write ({e})"),
                                crate::dead_letters::LetterClass::Fault,
                            )
                            .await;
                            msg.retry();
                            continue;
                        }
                    }
                }
                worker::console_log!(
                    "Queue: mutation applied for {} topic(s) (reason={:?}, applied={:?})",
                    body.topics.len(),
                    body.reason,
                    report.applied_topics
                );
                let row_object = if let Some(db) = &counters {
                    crate::ops::bump_counter(db, crate::ops::COUNTER_QUEUE_MUTATION_APPLIED, 1)
                        .await;
                    crate::dead_letters::resolve(
                        db,
                        body,
                        Some(&subject),
                        crate::dead_letters::Resolved::Landed,
                    )
                    .await
                } else {
                    None
                };
                // the d3 fold-3 (E585-D3-DELTA-L1): the ack's ONE decision over R2; a replay with a named topic that
                // FAILED leaves its object for a twin in flight (the sweep is the backstop)
                let ack = crate::queue::landed_ack(body, &report, row_object);
                if ack.leaves {
                    worker::console_log!(
                        "Queue: {subject} landed with a FAILED topic (applied={:?}, deduped={:?}): its R2 object stays for a twin",
                        report.applied_topics,
                        report.deduped_topics
                    );
                }
                acked_objects.extend(ack.delete);
                msg.ack();
            }
            Ok((_steak, report)) => {
                worker::console_log!(
                    "Queue: mutation replay still NOT durable ({} fault(s): {}) — retrying",
                    report.faults.len(),
                    report.summary()
                );
                if let Some(db) = &counters {
                    crate::ops::bump_counter(db, crate::ops::COUNTER_QUEUE_MUTATION_RETRIED, 1)
                        .await;
                    crate::dead_letters::note_failing(db, body, Some(&subject), &format!("not durable: {}", report.summary()), crate::dead_letters::LetterClass::of_sites(report.faults.iter().map(|f| f.site)))
                        .await;
                }
                msg.retry();
            }
            Err(e) => {
                worker::console_log!("Queue: mutation failed: {} — retrying", e);
                if let Some(db) = &counters {
                    crate::ops::bump_counter(db, crate::ops::COUNTER_QUEUE_MUTATION_RETRIED, 1)
                        .await;
                    crate::dead_letters::note_failing(db, body, Some(&subject), &format!("failed: {e}"), crate::queue::replay_error_class(&e)).await;
                }
                msg.retry();
            }
        }
    }

    crate::queue::delete_beefs(&env, &acked_objects, "its replay was acked").await;

    // W2-P4: ship the pot rows this batch changed (off the critical path).
    crate::pot_changes::flush(&env, |fut| ctx.wait_until(fut));
    crate::lobby_changes::flush(&env, |fut| ctx.wait_until(fut));
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    struct RegisteredTopicRemote;

    #[async_trait::async_trait(?Send)]
    impl overlay_engine::gasp::GASPRemote for RegisteredTopicRemote {
        async fn get_initial_response(
            &self,
            request: &overlay_engine::types::GASPInitialRequest,
        ) -> Result<overlay_engine::types::GASPInitialResponse, overlay_engine::gasp::GASPError>
        {
            Ok(overlay_engine::types::GASPInitialResponse {
                utxo_list: Vec::new(),
                since: request.since,
            })
        }
        async fn get_initial_reply(
            &self,
            _: &overlay_engine::types::GASPInitialResponse,
        ) -> Result<overlay_engine::types::GASPInitialReply, overlay_engine::gasp::GASPError>
        {
            unreachable!()
        }
        async fn request_node(
            &self,
            _: &str,
            _: &str,
            _: u32,
            _: bool,
        ) -> Result<overlay_engine::types::GASPNode, overlay_engine::gasp::GASPError> {
            unreachable!()
        }
        async fn submit_node(
            &self,
            _: &overlay_engine::types::GASPNode,
        ) -> Result<Option<overlay_engine::types::GASPNodeResponse>, overlay_engine::gasp::GASPError>
        {
            unreachable!()
        }
    }

    struct RegisteredTopicFactory(Rc<std::cell::RefCell<Vec<String>>>);
    impl overlay_engine::gasp::GASPRemoteFactory for RegisteredTopicFactory {
        fn create_remote(&self, _: &str, topic: &str) -> Box<dyn overlay_engine::gasp::GASPRemote> {
            self.0.borrow_mut().push(topic.to_string());
            Box::new(RegisteredTopicRemote)
        }
    }

    #[tokio::test]
    async fn sync_budget_unregistered_topics_are_skipped_after_suffix_rekey() {
        use overlay_engine::types::SyncTarget;
        for suffix in ["", "_beta"] {
            let trackers = vec!["https://tracker.example".to_string()];
            let mut config = gasp_sync_configuration(&trackers);
            config.insert("tm_agent".to_string(), SyncTarget::Peers(trackers.clone()));
            let config = rekey_sync_configuration(config, suffix);
            let topic = crate::routes::suffixed_name("tm_uhrp", suffix);
            let mut managers: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
            managers.insert("tm_ship".to_string(), Box::new(SHIPTopicManager::new()));
            managers.insert(topic.clone(), Box::new(UHRPTopicManager::new()));
            managers.insert(
                crate::routes::suffixed_name("tm_low", suffix),
                Box::new(LowTopicManager::new()),
            );
            let (filtered, skipped) = registered_sync_configuration(config, &managers);
            assert_eq!(filtered.len(), 3);
            assert_eq!(skipped.len(), 12);
            assert!(matches!(filtered[&topic], SyncTarget::Ship));
            assert!(matches!(
                filtered[&crate::routes::suffixed_name("tm_low", suffix)],
                SyncTarget::Disabled
            ));
            let mut engine = Engine::new(
                managers,
                HashMap::new(),
                Box::new(overlay_engine::storage::memory::MemoryStorage::new()),
                None,
                EngineConfig {
                    sync_configuration: filtered,
                    ship_trackers: trackers.clone(),
                    slap_trackers: trackers,
                    ..Default::default()
                },
            );
            engine.set_ship_bootstrap_peers(
                topic.clone(),
                vec!["https://registered.example".to_string()],
            );
            let calls = Rc::new(std::cell::RefCell::new(Vec::new()));
            engine.set_gasp_remote_factory(Box::new(RegisteredTopicFactory(calls.clone())));
            let result = engine.start_gasp_sync().await.unwrap();
            assert_eq!(result.topics_synced.len(), 2);
            assert!(calls
                .borrow()
                .iter()
                .all(|called| called == "tm_ship" || called == &topic));
            assert_eq!(calls.borrow().len(), 2);
        }
    }

    #[test]
    fn gasp_sync_map_preserves_every_topic_and_target_after_rekey() {
        use overlay_engine::types::SyncTarget;
        let trackers = ship_slap_bootstrap();
        let config = gasp_sync_configuration(&trackers);
        let topics = [
            "tm_ship",
            "tm_slap",
            "tm_uhrp",
            "tm_agent",
            "tm_dm_delegation",
            "tm_low",
            "tm_reveal",
            "tm_pot",
            "tm_lowfund",
            "tm_collected",
            "tm_hand",
            "tm_result",
            "tm_proof",
            "tm_potparty",
            "tm_potrefund",
        ];
        assert_eq!(config.len(), topics.len());
        for suffix in ["", "_beta"] {
            let rekeyed = rekey_sync_configuration(config.clone(), suffix);
            assert_eq!(rekeyed.len(), topics.len());
            for topic in topics {
                let target = &rekeyed[&crate::routes::suffixed_name(topic, suffix)];
                match topic {
                    "tm_ship" | "tm_slap" => {
                        assert!(matches!(target, SyncTarget::Peers(peers) if peers == &trackers))
                    }
                    "tm_uhrp" | "tm_agent" | "tm_dm_delegation" => {
                        assert!(matches!(target, SyncTarget::Ship))
                    }
                    _ => assert!(matches!(target, SyncTarget::Disabled)),
                }
            }
        }
        assert_eq!(
            trackers,
            overlay_engine::types::NetworkPreset::Mainnet.slap_trackers()
        );
    }

    /// Pin the set of non-GASP peers we crawl on a cron. Adding a new
    /// peer is a *policy* change — it means we trust that peer's
    /// records enough to admit them through our own tm_X validators —
    /// so it should be reviewed explicitly. This test fails on any
    /// drift from the agreed list so an accidental edit to
    /// `non_gasp_peers()` can't slip through a review unnoticed.
    #[test]
    fn non_gasp_peers_pinned() {
        let peers = non_gasp_peers();
        assert_eq!(peers.len(), 1, "only overlay-us-1.bsvb.tech today");

        let bsvb = &peers[0];
        assert_eq!(bsvb.peer_url, "https://overlay-us-1.bsvb.tech");
        assert_eq!(
            bsvb.service_to_topic,
            vec![("ls_uhrp".to_string(), "tm_uhrp".to_string())],
            "bsvb carries only UHRP records for us today; adding a \
             service is a real trust extension"
        );
    }

    /// Every configured peer's topic must be prefixed with `tm_` — the
    /// engine's admission dispatch keys on this and an unprefixed topic
    /// would silently skip. Separate from the pinned-peers test so a
    /// future peer addition gets this check for free.
    #[test]
    fn non_gasp_peer_topics_are_tm_prefixed() {
        for peer in non_gasp_peers() {
            for (svc, topic) in &peer.service_to_topic {
                assert!(
                    svc.starts_with("ls_"),
                    "{}: lookup service `{svc}` must be ls_*",
                    peer.peer_url
                );
                assert!(
                    topic.starts_with("tm_"),
                    "{}: topic manager `{topic}` must be tm_*",
                    peer.peer_url
                );
            }
        }
    }

    /// bsv-low #523 (M26): EVERY fetch route that reaches the dispatch flushes the pot-changed set on its way out,
    /// in the request that did the work. `/admin/complete-proofs` (the reliably-firing twin of the scheduled
    /// tick) confirmed a tower-broadcast refund's spend, the storage noted the pot, and the route answered
    /// without a flush: the note sat in the isolate's set and the seat's owed list heard nothing. One flush at
    /// the dispatch's single exit covers every route there is and every route added later (the proof-completion
    /// poke, `/admin/readmit`'s discovery, a `/lookup` whose serve-time rung latches a spend). To red: delete
    /// the flush between the dispatch and `result`, or add a `return` inside the dispatch that is not an
    /// auth refusal.
    #[test]
    fn every_dispatched_fetch_route_flushes_the_pot_changes_on_its_way_out() {
        let code_only = |s: &str| {
            s.lines()
                .map(|l| l.split("//").next().unwrap_or(""))
                .collect::<Vec<_>>()
                .join("\n")
        };
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let src = include_str!("lib.rs");
        let start = src
            .find("async fn main(req: Request, env: Env, ctx: Context)")
            .expect("the fetch handler");
        let end = start
            + src[start..]
                .find("\npub async fn build_engine_from_env(")
                .expect("the next item");
        let handler = code_only(&src[start..end]);
        let dispatch = handler
            .find("let result = match (req.method(), req.path().as_str()) {")
            .expect("the dispatch");
        let tail = squash(&handler[dispatch..]);
        assert!(
            tail.contains(&squash(
                "}; crate::pot_changes::flush(&env, |fut| ctx.wait_until(fut)); crate::lobby_changes::flush(&env, |fut| ctx.wait_until(fut)); result }"
            )),
            "the dispatch's single exit flushes the pot (and hop, and lobby) changes before it answers"
        );
        let returns = handler[dispatch..].matches("return ").count();
        let refusals = handler[dispatch..].matches("return resp;").count();
        assert_eq!(returns, refusals, "the only early returns inside the dispatch are the admin auth refusals (no pot write precedes them)");
    }

    // ── bsv-low#257: the cron step deadline race ─────────────────────────

    #[tokio::test]
    async fn race_returns_the_value_when_the_step_finishes_first() {
        let out = race_or_deadline(async { 42u32 }, std::future::pending::<()>()).await;
        assert_eq!(out, Some(42));
    }

    #[tokio::test]
    async fn race_drops_a_hung_step_when_the_deadline_fires() {
        // The #257 shape: a step that never completes (the unbounded GASP
        // sync against a dead peer) must NOT hold the tick — the deadline
        // wins, the step future is dropped, and the caller continues.
        let out = race_or_deadline(std::future::pending::<u32>(), async {}).await;
        assert_eq!(out, None, "a hung step must yield to the deadline");
    }
}

// P0-2d (bsv-stack-lean #52): the Arcade status vector replayed through the
// readings above. Declared last: the source pins read this file up to its
// first `#[cfg(test)]`.
#[cfg(test)]
mod arcade_vector_replay;

#[cfg(test)]
mod beef_door_replay;

// bsv-low #585 (E585-land): the stored-rows reader, a native test-only tool
// that reads a D1 export through the Worker's own readers.
#[cfg(test)]
mod stored_rows;

#[cfg(test)]
#[path = "../../worker-build-info/src/worker_tests.rs"]
mod build_info_tests;

pub mod tracker_events;
