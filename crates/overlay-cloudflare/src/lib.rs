//! Cloudflare Workers deployment for BSV Overlay Services.
//!
//! Entry point for the overlay worker. Handles HTTP routing, D1 storage
//! initialization, and Engine setup.
//!
//! Pattern from ~/bsv/rust-wallet-infra/src/lib.rs.

pub mod advert_lifecycle;
pub mod advertiser;
pub mod ban_storage;
pub mod broadcaster;
pub mod chain_tracker;
pub mod d1;
pub mod d1_discovery;
pub mod d1_storage;
pub mod ef;
pub mod error;
pub mod gasp_remote;
pub mod health_checker;
pub mod janitor;
pub mod lobby_changes;
pub mod mainnet_fanout;
pub mod ops;
pub mod peer_crawler;
pub mod pot_changes;
pub mod proof_fetcher;
pub mod queue;
pub mod relatch;
pub mod routes;
pub mod submit_census;
pub mod submit_gate;
pub mod wallet;

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

#[event(fetch)]
async fn main(req: Request, env: Env, ctx: Context) -> worker::Result<Response> {
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
            _ => {}
        }
    }

    // D1 database binding
    let db = Rc::new(env.d1("OVERLAY_DB")?);
    // Ban storage — shares the OVERLAY_DB binding via a dedicated table
    let ban_storage = Rc::new(crate::ban_storage::D1BanStorage::new(db.clone()));

    // Apply migrations once per isolate (idempotent — CREATE IF NOT EXISTS;
    // unguarded per-request execution was 63 D1 round-trips/request, #255)
    ensure_overlay_migrations(&db)
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
                arc_ingest(
                    &engine,
                    req,
                    tracker.as_deref(),
                    pot_storage.as_ref(),
                    Some(&ops_db),
                )
                .await
            }
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
                // #192/#193 — run the BEEF proof-completion passes on demand. The
                // `*/15` cron that normally drives them is not firing on this worker
                // (CF is not delivering the scheduled event — a queue+cron platform
                // quirk; the handler/config/export are all correct), so this
                // reliably-firing FETCH route is the durable trigger: an external
                // cron (e.g. low-monitor) POSTs it every ~15 min. Same logic as the
                // scheduled block; fail-closed.
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
                "/admin/startGASPSync" => admin_start_gasp_sync(&engine).await,
                "/admin/evictOutpoint" => admin_evict_outpoint(&engine, req).await,
                "/admin/remove-token" => admin_remove_token(&engine, req).await,
                "/admin/crawlPeers" => admin_crawl_peers(&engine, &non_gasp_peers()).await,
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
    ensure_overlay_migrations(&db)
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
fn lookup_service_chain_tracker(env: &Env) -> Option<Rc<dyn bsv_rs::transaction::ChainTracker>> {
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
                    Box::new(ProofLookupService::new(proof_storage.clone())),
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

    // GASP sync_configuration. Two modes per topic:
    //
    // - `SyncTarget::Ship` — discover peers dynamically via SHIP lookup
    //   at sync time. Works once we've ingested SHIP ads (our own or
    //   peers') into our own `ls_ship`. Fresh deploys start with empty
    //   `ls_ship` except for our own ads — so Ship-mode finds only us,
    //   which isn't useful.
    //
    // - `SyncTarget::Peers(urls)` — bootstrap with a hardcoded peer list.
    //   Required to break the discovery cold-start: without at least one
    //   known peer, we never learn about anyone. For `tm_uhrp` we pin
    //   `overlay-us-1.bsvb.tech`; once sync runs once it imports their
    //   SHIP records, and from then on SHIP-mode could discover further
    //   peers organically (left for a follow-up — the hardcode is
    //   sufficient for bi-directional UHRP sync today).
    let mut sync_configuration: overlay_engine::types::SyncConfiguration =
        std::collections::HashMap::new();

    // tm_ship + tm_slap bootstrap peers — must match what the mainline
    // reference's default @bsv/sdk LookupResolver seeds with, so the parity
    // harness's two sides pull from the same sources. Verified against the
    // reference container's GASP sync log:
    //   "Will attempt to sync with 4 peers" →
    //     overlay-{us,eu,ap}-1.bsvb.tech, users.bapp.dev
    // Once rust has pulled SHIP/SLAP records from these four, subsequent
    // syncs could fall back to SyncTarget::Ship and discover more organically
    // (left for a follow-up — the hardcode matches what mainline uses at cold
    // start).
    let ship_slap_bootstrap = vec![
        "https://overlay-us-1.bsvb.tech".to_string(),
        "https://overlay-eu-1.bsvb.tech".to_string(),
        "https://overlay-ap-1.bsvb.tech".to_string(),
        "https://users.bapp.dev".to_string(),
    ];
    sync_configuration.insert(
        "tm_ship".to_string(),
        overlay_engine::types::SyncTarget::Peers(ship_slap_bootstrap.clone()),
    );
    sync_configuration.insert(
        "tm_slap".to_string(),
        overlay_engine::types::SyncTarget::Peers(ship_slap_bootstrap.clone()),
    );

    sync_configuration.insert(
        "tm_uhrp".to_string(),
        overlay_engine::types::SyncTarget::Peers(
            vec!["https://overlay-us-1.bsvb.tech".to_string()],
        ),
    );
    // tm_agent + tm_dm_delegation are Calhooon-internal for now;
    // SHIP-mode is the right default (we're the only known host).
    for topic in ["tm_agent", "tm_dm_delegation"] {
        sync_configuration.insert(topic.to_string(), overlay_engine::types::SyncTarget::Ship);
    }

    // tm_low (LOW poker lobby) starts as a single-node lobby: the
    // low-overlay worker is the only host carrying the topic, tables are
    // short-lived, and clients hit this instance directly — so GASP sync
    // would only burn cron cycles discovering nobody. Explicitly Disabled
    // (rather than Ship) until a second lobby node exists.
    sync_configuration.insert(
        "tm_low".to_string(),
        overlay_engine::types::SyncTarget::Disabled,
    );

    // tm_reveal (LOW break-glass reveal index) is likewise single-node: the
    // low-overlay worker is the only host carrying it and the watchtower
    // queries this instance directly. Disabled until a second reveal node
    // exists (mirrors tm_low).
    sync_configuration.insert(
        "tm_reveal".to_string(),
        overlay_engine::types::SyncTarget::Disabled,
    );

    // tm_pot (LOW pot-spend landing-proof index) is single-node like tm_low /
    // tm_reveal: this worker is the only host and the LOW client queries it
    // directly. Disabled until a second pot-index node exists. tm_lowfund
    // (the hop-side index into the same store) mirrors it, as does
    // tm_collected (the cross-device "already collected" marker index,
    // bsv-low #161), as do tm_result (the hand-result leaderboard
    // marker index, bsv-low #38) and tm_proof (the rung-3
    // transcript-proof bundle index), as does tm_potparty (the by-identity
    // pot-participation recovery index, bsv-low #188) and tm_potrefund (the
    // pre-signed refund-backup recovery index, bsv-low #191).
    for topic in [
        "tm_pot",
        "tm_lowfund",
        "tm_collected",
        "tm_hand",
        "tm_result",
        "tm_proof",
        "tm_potparty",
        "tm_potrefund",
    ] {
        sync_configuration.insert(
            topic.to_string(),
            overlay_engine::types::SyncTarget::Disabled,
        );
    }

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
        sync_configuration = sync_configuration
            .into_iter()
            .map(|(k, v)| (rekey(k), v))
            .collect();
    }

    let config = EngineConfig {
        hosting_url: hosting_url.clone(),
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
    // whole time. Same preference order as the sibling: service binding
    // first, URL fetch only as the off-account fallback. The enablement
    // condition (CHAIN_TRACKER_URL set) is unchanged — only the transport.
    let chain_tracker: Option<Box<dyn bsv_rs::transaction::ChainTracker>> =
        env.var("CHAIN_TRACKER_URL").ok().map(|v| {
            let ct_url = v.to_string();
            match env.service("CHAINTRACKS") {
                Ok(svc) => Box::new(WorkerChainTracker::with_service(ct_url, svc))
                    as Box<dyn bsv_rs::transaction::ChainTracker>,
                Err(_) => Box::new(WorkerChainTracker::new(ct_url))
                    as Box<dyn bsv_rs::transaction::ChainTracker>,
            }
        });

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

    // Chain-backed proof fetcher (#192/#193): the courier ladder
    // (Arcade→WoC→Bitails) with a MANDATORY chaintracks re-verify before any
    // BUMP is returned. This is the proof source the cron's
    // `complete_missing_proofs` (transactions store) and the pot-store
    // completion tick call to turn a proofless stored BEEF into a proven one.
    // Without a chain tracker it degrades to a pure retry (no proof can ever be
    // verified — fail-closed).
    let proof_tracker = lookup_service_chain_tracker(env);
    let mut proof_fetcher = crate::proof_fetcher::ChainProofFetcher::new(proof_tracker);
    if let Some(u) = env
        .var("ARCADE_URL")
        .ok()
        .map(|v| v.to_string())
        .filter(|s| !s.trim().is_empty())
    {
        proof_fetcher = proof_fetcher.with_arcade_url(u);
    }
    engine.set_ancestor_fetcher(std::rc::Rc::new(proof_fetcher));

    engine
}

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
const GASP_SYNC_BUDGET_MS: u64 = 240_000;
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

#[event(scheduled)]
async fn scheduled(_event: worker::ScheduledEvent, env: Env, ctx: worker::ScheduleContext) {
    worker::console_log!("Scheduled event triggered");

    // Initialize D1 and run migrations
    let db = match env.d1("OVERLAY_DB") {
        Ok(db) => Rc::new(db),
        Err(e) => {
            worker::console_log!("Scheduled: D1 binding error: {}", e);
            return;
        }
    };

    if let Err(e) = ensure_overlay_migrations(&db).await {
        worker::console_log!("Scheduled: Migration error: {}", e);
        return;
    }

    // Build shared storage + engine
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
    // Keep a DB handle for the observability writes (#192/#193, P4) — the
    // engine build below consumes `db`.
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

    // Sync advertisements (if advertiser + hosting URL are configured).
    // Publishes any new SHIP/SLAP ads on-chain so peers can discover us.
    match engine.sync_advertisements().await {
        Ok(report) if report.effective() => {
            worker::console_log!("Scheduled: Ad sync ok: {report:?}");
        }
        Ok(report) => {
            // bsv-low #320 defect 3a/M1 — surface, never swallow (incl. the
            // zero-admit refusal shape).
            worker::console_log!("Scheduled: Ad sync completed WITH FAILURES: {report:?}");
        }
        Err(e) => {
            worker::console_log!("Scheduled: Ad sync error: {}", e);
        }
    }

    // GASP sync with discovered peers. For each topic configured in
    // `sync_configuration`, start_gasp_sync discovers peers (via SHIP
    // lookup of other overlays carrying the topic) and exchanges UTXOs
    // — we pull their records into our D1 and, symmetrically, they pull
    // ours. This is how a UHRP advert published on bsvb.tech ends up
    // queryable on rust-overlay and vice versa.
    //
    // If no `sync_configuration` is set in EngineConfig (the current
    // default), GASP sync is a near-no-op: `start_gasp_sync` iterates
    // configured topics only. That's fine — calling it keeps the wire
    // connected so adding topic peers later Just Works.
    // #257: BOUNDED — an unbounded GASP sync (dead SHIP-discovered peers, no
    // per-fetch timeout) hung every cron to the 15-min kill since deploy day;
    // see the budget consts. A timeout drops the sync (cursors persist only
    // per completed peer — idempotent redo next tick) and the tick moves on.
    match race_or_deadline(
        engine.start_gasp_sync(),
        crate::broadcaster::sleep_ms(GASP_SYNC_BUDGET_MS),
    )
    .await
    {
        None => worker::console_log!(
            "Scheduled: GASP sync EXCEEDED its {GASP_SYNC_BUDGET_MS} ms budget — dropped (bsv-low#257); continuing the tick"
        ),
        Some(Ok(r)) => {
            let total_peers: usize = r.topics_synced.values().map(|t| t.peers.len()).sum();
            let total_errors: usize = r.topics_synced.values().map(|t| t.errors.len()).sum();
            worker::console_log!(
                "Scheduled: GASP sync — topics={} peers={} errors={}",
                r.topics_synced.len(),
                total_peers,
                total_errors
            );
            for (topic, res) in &r.topics_synced {
                if !res.errors.is_empty() {
                    worker::console_log!(
                        "  Scheduled GASP topic={} sync_type={} errors={:?}",
                        topic,
                        res.sync_type,
                        res.errors
                    );
                }
            }
        }
        Some(Err(e)) => worker::console_log!("Scheduled: GASP sync error: {}", e),
    }

    // Peer crawl: bridge for non-GASP peers (bsvb today). `/lookup` +
    // `/submit` instead of `/requestSyncResponse`. Engine's tm_X
    // is_dupe check makes this idempotent — crawling the same peer
    // twice in a row costs compute but admits nothing new.
    let peers = non_gasp_peers();
    // #257: bounded like GASP sync — crawl submits are dupe-checked, so a
    // dropped crawl re-does nothing next tick.
    match race_or_deadline(
        peer_crawler::crawl_peers(&engine, &peers, "cron"),
        crate::broadcaster::sleep_ms(PEER_CRAWL_BUDGET_MS),
    )
    .await
    {
        None => worker::console_log!(
            "Scheduled: peer-crawl EXCEEDED its {PEER_CRAWL_BUDGET_MS} ms budget — dropped (bsv-low#257); continuing the tick"
        ),
        Some(crawl_result) => {
            let total_attempted: usize = crawl_result.attempted.values().sum();
            let total_admitted: usize = crawl_result.admitted_by.values().sum();
            worker::console_log!(
                "Scheduled: peer-crawl — peers={} attempted={} admitted={} peer_errors={}",
                peers.len(),
                total_attempted,
                total_admitted,
                crawl_result.peer_errors.len(),
            );
            for (k, errs) in &crawl_result.errors {
                if !errs.is_empty() {
                    worker::console_log!(
                        "  Scheduled peer-crawl {k}: {} submit-errors (first: {})",
                        errs.len(),
                        errs.first().map(String::as_str).unwrap_or("")
                    );
                }
            }
            for (k, e) in &crawl_result.peer_errors {
                worker::console_log!("  Scheduled peer-crawl {k}: lookup failed: {e}");
            }
        }
    }

    // BEEF proof completion (#192/#193). Two parallel passes, both bounded per
    // tick and fail-closed (a BUMP is stitched only once its root is verified
    // against chaintracks; an unmined/unverifiable candidate is retried, never
    // written proofless).
    //
    // 1. Engine `transactions` store — uses the ancestor fetcher set in
    //    build_engine_with_storage. A no-op if no fetcher/tracker is configured.
    //
    //    #228: the poll passes are the BACKSTOP — /arc-ingest push is the
    //    primary proof source — so each pass only touches rows older than
    //    PUSH_BACKSTOP_MIN_AGE_SECS (see its doc for the 30-min rationale).
    let engine_budget = u64::from(crate::proof_fetcher::DEFAULT_FETCH_BUDGET);
    let backstop_age = crate::proof_fetcher::PUSH_BACKSTOP_MIN_AGE_SECS;
    let (tx_completed, tx_fetch_failed) = match engine
        .complete_missing_proofs(engine_budget, backstop_age)
        .await
    {
        Ok(s) => {
            worker::console_log!(
                "Scheduled: proof-completion (transactions) — scanned={} proofless={} completed={} \
                 still_unconfirmed={} fetch_failed={} stitch_failed={} already_proven={}",
                s.scanned,
                s.proofless,
                s.completed,
                s.still_unconfirmed,
                s.fetch_failed,
                s.stitch_failed,
                s.already_proven,
            );
            (s.completed as u64, s.fetch_failed as u64)
        }
        Err(e) => {
            worker::console_log!("Scheduled: proof-completion (transactions) error: {e}");
            (0, 0)
        }
    };

    // 2+3. LOW pot-store maintenance, ORDER LOAD-BEARING (bsv-low#304 gate
    //    M-5): the #186 spend-confirmation chaser (the small, money-relevant
    //    CREDIT ANCHOR) runs BEFORE the pot-beef proof/bulk-drain pass so a
    //    subrequest-wall starvation can never queue the anchor behind the
    //    drain. The order is encoded ONCE in `run_pot_maintenance` (shared
    //    with /admin/complete-proofs). Each pass keeps its own fetcher (own
    //    budget cell). The chaser upgrades a 0-conf pot spend to
    //    spentConfirmed = 1 ONLY once the SPENDING tx's bump verifies against
    //    chaintracks — fail-closed, never downgrades. The pot-beef pass
    //    fetches → verifies → stitches → trims → compacts each
    //    not-yet-verified pot BEEF; its candidate page is
    //    POT_PROOF_PASS_LIMIT (drain + op math at the const), courier
    //    traffic independently bounded by its fetcher budget.
    let mut spend_fetcher =
        crate::proof_fetcher::ChainProofFetcher::new(lookup_service_chain_tracker(&env))
            .with_budget(20);
    if let Some(u) = env
        .var("ARCADE_URL")
        .ok()
        .map(|v| v.to_string())
        .filter(|s| !s.trim().is_empty())
    {
        spend_fetcher = spend_fetcher.with_arcade_url(u);
    }
    let pot_tracker = lookup_service_chain_tracker(&env);
    let mut pot_fetcher = crate::proof_fetcher::ChainProofFetcher::new(pot_tracker);
    if let Some(u) = env
        .var("ARCADE_URL")
        .ok()
        .map(|v| v.to_string())
        .filter(|s| !s.trim().is_empty())
    {
        pot_fetcher = pot_fetcher.with_arcade_url(u);
    }
    let (spend_summary, pot_summary) = crate::proof_fetcher::run_pot_maintenance(
        pot_storage.as_ref(),
        &spend_fetcher,
        20,
        &pot_fetcher,
        crate::proof_fetcher::POT_PROOF_PASS_LIMIT,
        backstop_age,
    )
    .await;
    worker::console_log!(
        "Scheduled: spend-confirmation (pot_records) — scanned={} confirmed={} \
         still_unconfirmed={} fetch_failed={} tracker_faults={} cas_missed={} cas_errors={} \
         displaced={} displace_attempts={} displace_faults={}",
        spend_summary.scanned,
        spend_summary.confirmed,
        spend_summary.still_unconfirmed,
        spend_summary.fetch_failed,
        spend_summary.tracker_faults,
        spend_summary.cas_missed,
        spend_summary.cas_errors,
        spend_summary.displaced,
        spend_summary.displace_attempts,
        spend_summary.displace_faults,
    );
    // bsv-low W4 (2026-09-04): spends the index never saw — bounded, oldest
    // first, on a fetcher of its OWN (the first tick on beta shared the
    // confirmation pass's 20-call budget and faulted 11 of 20 candidates on
    // "budget exhausted"): up to 4 courier calls per candidate, never a call
    // taken from the money-relevant confirmation pass.
    let mut discovery_fetcher =
        crate::proof_fetcher::ChainProofFetcher::new(lookup_service_chain_tracker(&env))
            .with_budget(crate::proof_fetcher::MISSING_SPEND_FETCH_BUDGET);
    if let Some(u) = env
        .var("ARCADE_URL")
        .ok()
        .map(|v| v.to_string())
        .filter(|s| !s.trim().is_empty())
    {
        discovery_fetcher = discovery_fetcher.with_arcade_url(u);
    }
    let missing = crate::proof_fetcher::discover_missing_spends(
        pot_storage.as_ref(),
        &discovery_fetcher,
        crate::proof_fetcher::MISSING_SPEND_PASS_LIMIT,
        crate::proof_fetcher::MISSING_SPEND_MIN_AGE_SECS,
    )
    .await;
    worker::console_log!(
        "Scheduled: missing-spend discovery (pot_records) — scanned={} discovered={} no_hint={} unbound={} faults={} write_errors={} hop_rows={} new_era={}",
        missing.scanned,
        missing.discovered,
        missing.no_hint,
        missing.unbound,
        missing.faults,
        missing.write_errors,
        missing.hop_rows,
        missing.discovered_new_era
    );
    // 2026-09-04: the pass's lifetime counters ride `/health/invariants`
    // (`counters.missing_spend_*`) so the backlog's drain, the couriers'
    // faults and — the paged one — NEW-era discoveries are observable without
    // a laptop tail. Additive upserts; a never-run counter reads 0 there.
    crate::ops::record_missing_spend_pass(
        &ops_db,
        missing.scanned as u64,
        missing.discovered as u64,
        missing.faults as u64,
        missing.discovered_new_era as u64,
    )
    .await;
    worker::console_log!(
        "Scheduled: proof-completion (pot_beefs) — scanned={} completed={} already_proven={} \
         still_unconfirmed={} fetch_failed={} stitch_failed={}",
        pot_summary.scanned,
        pot_summary.completed,
        pot_summary.already_proven,
        pot_summary.still_unconfirmed,
        pot_summary.fetch_failed,
        pot_summary.stitch_failed,
    );

    // 4. LOW `pot_records` decoded-params lazy backfill (#284): decode the
    //    covenant params / lock kind / verdict for pre-migration rows from
    //    the durable pot_beefs bytes. No courier, no tracker — pure re-reads
    //    of our own admitted bytes; a row with a missing funding BEEF stays
    //    a candidate (bounded per tick, RANDOM-sampled).
    let backfill_summary = crate::proof_fetcher::backfill_decoded_params(
        pot_storage.as_ref(),
        crate::proof_fetcher::PARAMS_BACKFILL_LIMIT,
    )
    .await;
    worker::console_log!(
        "Scheduled: params-backfill (pot_records) — scanned={} decoded={} verdicts={} \
         missing_beef={}",
        backfill_summary.scanned,
        backfill_summary.decoded,
        backfill_summary.verdicts,
        backfill_summary.missing_beef,
    );

    // 4a. bsv-low #406: settleSigners historic backfill — attach WHO SIGNED
    //     to rows classified before #406 shipped, from the durable spender
    //     bytes. Pure local re-reads + ECDSA verifies; bounded, RANDOM-
    //     sampled; converges to zero candidates and stays there.
    let signers_summary = crate::proof_fetcher::backfill_settle_signers(
        pot_storage.as_ref(),
        crate::proof_fetcher::SETTLE_SIGNERS_BACKFILL_LIMIT,
    )
    .await;
    worker::console_log!(
        "Scheduled: signers-backfill (pot_records) — scanned={} latched={} unresolved={} \
         missing_beef={}",
        signers_summary.scanned,
        signers_summary.latched,
        signers_summary.unresolved,
        signers_summary.missing_beef,
    );

    // 4b. The RE-LATCH fixpoint over the two admission-latched verdict columns
    //     (bsv-low #355 potparty.sigValid + #367 hopparty.markerValid). Pure
    //     re-reads of our own rows — no courier, no tracker, no BEEF parse —
    //     and a bounded page per table per tick. This is the ONLY repair path
    //     either column has: a hopparty marker rides a transaction already on
    //     chain, so no republish can ever re-latch it, and a row a transient
    //     predicate fault latched 0 sorts below even the legacy tier forever.
    //     `changed`/`demoted` in the log lines are the regression detector.
    for summary in
        crate::relatch::run_relatch(ops_db.clone(), crate::relatch::RELATCH_PAGE_LIMIT).await
    {
        crate::relatch::log_relatch_summary(&summary);
    }

    // 4b. Terminal-retire classifier (INCIDENT D1-CALLBACK-FLOOD 2026-09-01).
    //    Runs BEFORE the rebroadcast backstop on purpose: a row it retires
    //    this tick (corroborated network-dead — Arcade terminal verdict or
    //    48h+ absence, PLUS both indexers' definitive 404) must not be
    //    re-presented by the very next step. Bounded: ≤8 candidates per
    //    store, ≤3 courier GETs each; every uncertainty keeps (fail-safe).
    let arcade_base = env
        .var("ARCADE_URL")
        .ok()
        .map(|v| v.to_string())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| crate::broadcaster::ARCADE_DEFAULT_URL.to_string());
    let retire = crate::proof_fetcher::run_retire_pass(&ops_db, &arcade_base, None).await;
    worker::console_log!(
        "Scheduled: retire-pass — scanned={} retired={} kept_present={} kept_uncertain={}",
        retire.scanned,
        retire.retired,
        retire.kept_present,
        retire.kept_uncertain,
    );

    // 5. Admitted-but-network-absent rebroadcast backstop (bsv-low #273,
    //    #267 item c). The passes above only help txs the network HOLDS; an
    //    admitted tx the network never accepted (the #267 incident class)
    //    never self-heals — this pass presence-probes old proofless rows
    //    (Bitails + WoC) and rebroadcasts the stored BEEF ancestry-first
    //    when BOTH indexers definitively 404 it. Runs LAST, own bounds
    //    (16 candidates / 48 POSTs, 30min–14d candidacy bracket — gate
    //    LOW-1), so it can never starve proof completion. Since the incident
    //    fix the attempts are RECORDED AND CAPPED (`run_rebroadcast_backstop`
    //    — 3 lifetime attempts, spacing 1h/6h, dead-letter log at the cap).
    let tx_storage = D1Storage::new(ops_db.clone());
    let taal_key = env.secret("TAAL_API_KEY").ok().map(|s| s.to_string());
    let rb = crate::proof_fetcher::run_rebroadcast_backstop(&tx_storage, taal_key.as_deref(), None)
        .await;
    worker::console_log!(
        "Scheduled: rebroadcast-backstop (transactions) — scanned={} present={} \
         inconclusive={} rebroadcast={} failed={} budget_skipped={} attempted={}",
        rb.scanned,
        rb.present,
        rb.inconclusive,
        rb.rebroadcast,
        rb.rebroadcast_failed,
        rb.budget_skipped,
        rb.attempted.len(),
    );

    // 6. LOW advert lifecycle (bsv-low#309): reap expired-but-unspent lobby
    //    adverts (fail-CLOSED — a null tip reaps NOTHING) + spend-confirm a
    //    bounded advert-outpoint sample so a close that bypassed /submit
    //    (the direct-ARC fallback — the #256 orphan generator) still leaves
    //    ls_low. Raced like the other network-bound steps; both passes are
    //    idempotent, so a dropped tick just retries. Also poke-able via
    //    POST /admin/advert-lifecycle (the cron-poker doctrine).
    let advert_tracker = lookup_service_chain_tracker(&env);
    match race_or_deadline(
        async {
            let tip = crate::advert_lifecycle::resolve_tip(advert_tracker.as_deref()).await;
            crate::advert_lifecycle::run_advert_lifecycle(
                low_storage.as_ref(),
                tip,
                None,
                crate::advert_lifecycle::ADVERT_SPEND_CHECK_LIMIT,
                crate::advert_lifecycle::ADVERT_REAP_LIMIT,
            )
            .await
        },
        crate::broadcaster::sleep_ms(ADVERT_LIFECYCLE_BUDGET_MS),
    )
    .await
    {
        None => worker::console_log!(
            "Scheduled: advert-lifecycle EXCEEDED its {ADVERT_LIFECYCLE_BUDGET_MS} ms budget — dropped; continuing the tick"
        ),
        Some((reap, spend)) => {
            worker::console_log!(
                "Scheduled: advert-lifecycle (low_records) — tip_resolved={} reap_scanned={} \
                 reaped={} reap_delete_failed={} spend_scanned={} spend_deleted={} \
                 not_spent={} unknown={} spend_delete_failed={}",
                reap.tip_resolved,
                reap.scanned,
                reap.reaped,
                reap.delete_failed,
                spend.scanned,
                spend.deleted,
                spend.not_spent,
                spend.unknown,
                spend.delete_failed,
            );
        }
    }

    // Observability (#192/#193, P4): stamp the completion-pass heartbeat, bump
    // the persistent counters, and refresh the proofless first-seen ledger so a
    // dead pass / a proof-not-landing surfaces via GET /health/invariants
    // within a day (not weeks). Best-effort — never breaks the cron.
    let proofs_completed = tx_completed + pot_summary.completed as u64;
    let fetch_failed = tx_fetch_failed + pot_summary.fetch_failed as u64;
    let pot_beefs_compacted = pot_summary.completed as u64;
    // A displacement latches spentConfirmed the same as a confirm — the ops
    // heartbeat counts both or displaced rows go invisible (2026-08-18).
    let spends_confirmed = (spend_summary.confirmed + spend_summary.displaced) as u64;
    crate::ops::record_completion_tick(
        &ops_db,
        proofs_completed,
        fetch_failed,
        pot_beefs_compacted,
        spends_confirmed,
    )
    .await;
    let flagged = crate::ops::refresh_proofless_watch(&ops_db).await;
    worker::console_log!(
        "Scheduled: ops — proofs_completed+={proofs_completed} fetch_failed+={fetch_failed} \
         pot_beefs_compacted+={pot_beefs_compacted} spends_confirmed+={spends_confirmed} \
         proofless_over_24h={flagged}"
    );

    // Run janitor health checks. #257: bounded — the janitor serially
    // health-checks every SHIP/SLAP domain (including dead ephemeral hosts),
    // so once GASP sync stopped eating the whole budget this step would be
    // the next unbounded candidate. A dropped janitor just retries evictions
    // next tick.
    let janitor_config = overlay_engine::health_checker::JanitorConfig::default();
    let checker = WorkerHealthChecker;
    let hosting_url = env.var("HOSTING_URL").ok().map(|v| v.to_string());
    match race_or_deadline(
        janitor::run_janitor(
            ship_storage.as_ref(),
            slap_storage.as_ref(),
            &checker,
            &janitor_config,
            hosting_url.as_deref(),
        ),
        crate::broadcaster::sleep_ms(JANITOR_BUDGET_MS),
    )
    .await
    {
        None => worker::console_log!(
            "Scheduled: janitor EXCEEDED its {JANITOR_BUDGET_MS} ms budget — dropped (bsv-low#257); continuing"
        ),
        Some(Ok(result)) => {
            worker::console_log!(
                "Scheduled: Janitor completed — SHIP: {}, SLAP: {}, evicted: {}, healthy: {}, unhealthy: {}",
                result.ship_records_checked,
                result.slap_records_checked,
                result.records_evicted,
                result.domains_healthy,
                result.domains_unhealthy,
            );
        }
        Some(Err(e)) => {
            worker::console_log!("Scheduled: Janitor error: {}", e);
        }
    }

    worker::console_log!("Scheduled tasks completed");
    // W2-P4: ship the pot rows this run changed (off the critical path).
    crate::pot_changes::flush(&env, |fut| ctx.wait_until(fut));
    crate::lobby_changes::flush(&env, |fut| ctx.wait_until(fut));
}

/// POST /admin/complete-proofs (#192/#193) — run the BEEF proof-completion passes
/// ON DEMAND, the SAME logic the `*/15` cron would run, from a reliably-firing
/// FETCH route (the scheduled event is not delivered to this worker — a queue+cron
/// platform quirk). Self-contained (builds its own db/engine/storages from env) +
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
    // 1. transactions store (engine + ancestor fetcher). #228: backstop-gated —
    //    /arc-ingest push is the primary source; only rows older than
    //    PUSH_BACKSTOP_MIN_AGE_SECS are polled.
    let budget = u64::from(crate::proof_fetcher::DEFAULT_FETCH_BUDGET);
    let backstop_age = crate::proof_fetcher::PUSH_BACKSTOP_MIN_AGE_SECS;
    let (tx_completed, tx_fetch_failed) =
        match engine.complete_missing_proofs(budget, backstop_age).await {
            Ok(s) => (s.completed as u64, s.fetch_failed as u64),
            Err(e) => {
                worker::console_log!("complete-proofs: transactions error: {e}");
                (0, 0)
            }
        };
    // 2+3. Pot-store maintenance — chaser BEFORE the pot-beef bulk drain,
    //    encoded once in run_pot_maintenance (bsv-low#304 gate M-5; same
    //    order as the scheduled tick). Own fetchers, own budget cells.
    let mut spend_fetcher =
        crate::proof_fetcher::ChainProofFetcher::new(lookup_service_chain_tracker(env))
            .with_budget(20);
    if let Some(u) = env
        .var("ARCADE_URL")
        .ok()
        .map(|v| v.to_string())
        .filter(|s| !s.trim().is_empty())
    {
        spend_fetcher = spend_fetcher.with_arcade_url(u);
    }
    let pot_tracker = lookup_service_chain_tracker(env);
    let mut pot_fetcher = crate::proof_fetcher::ChainProofFetcher::new(pot_tracker);
    if let Some(u) = env
        .var("ARCADE_URL")
        .ok()
        .map(|v| v.to_string())
        .filter(|s| !s.trim().is_empty())
    {
        pot_fetcher = pot_fetcher.with_arcade_url(u);
    }
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
    let retire = crate::proof_fetcher::run_retire_pass(&ops_db, &arcade_base, None).await;
    worker::console_log!(
        "complete-proofs: retire-pass — scanned={} retired={} kept_present={} kept_uncertain={}",
        retire.scanned,
        retire.retired,
        retire.kept_present,
        retire.kept_uncertain,
    );
    // 4c. admitted-but-network-absent rebroadcast backstop (bsv-low #273) —
    //     runs last, own bounds + 30min–14d candidacy bracket (gate LOW-1) +
    //     the incident's attempt cap; see the scheduled block's note.
    let tx_storage = D1Storage::new(db.clone());
    let taal_key = env.secret("TAAL_API_KEY").ok().map(|s| s.to_string());
    let rb = crate::proof_fetcher::run_rebroadcast_backstop(&tx_storage, taal_key.as_deref(), None)
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
    Response::from_json(&serde_json::json!({
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
    }))
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
#[event(queue)]
async fn queue_handler(
    batch: worker::MessageBatch<crate::queue::MutationMessage>,
    env: Env,
    ctx: worker::Context,
) -> worker::Result<()> {
    use base64::{engine::general_purpose::STANDARD, Engine as B64Engine};
    use overlay_engine::types::{SubmitMode, TaggedBEEF};
    use worker::MessageExt;

    let engine = build_engine_from_env(&env)
        .await
        .map_err(|e| worker::Error::from(format!("Queue engine build failed: {e}")))?;

    for msg_result in batch.iter() {
        let msg = match msg_result {
            Ok(m) => m,
            Err(e) => {
                worker::console_log!("Queue: failed to deserialize message: {}", e);
                continue;
            }
        };

        let body = msg.body();

        let beef = match STANDARD.decode(&body.beef_b64) {
            Ok(b) => b,
            Err(e) => {
                worker::console_log!("Queue: invalid base64 BEEF: {}", e);
                msg.ack();
                continue;
            }
        };

        let tagged_beef = TaggedBEEF {
            beef,
            topics: body.topics.clone(),
            off_chain_values: None,
        };

        let mode: SubmitMode = crate::queue::replay_submit_mode(&body.mode);
        let counters = env.d1("OVERLAY_DB").ok();

        match engine.submit_with_report(&tagged_beef, mode).await {
            Ok((_steak, report)) if report.is_durable() => {
                worker::console_log!(
                    "Queue: mutation applied for {} topic(s) (reason={:?}, applied={:?})",
                    body.topics.len(),
                    body.reason,
                    report.applied_topics
                );
                if let Some(db) = &counters {
                    crate::ops::bump_counter(db, crate::ops::COUNTER_QUEUE_MUTATION_APPLIED, 1)
                        .await;
                }
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
                }
                msg.retry();
            }
            Err(e) => {
                worker::console_log!("Queue: mutation failed: {} — retrying", e);
                if let Some(db) = &counters {
                    crate::ops::bump_counter(db, crate::ops::COUNTER_QUEUE_MUTATION_RETRIED, 1)
                        .await;
                }
                msg.retry();
            }
        }
    }

    // W2-P4: ship the pot rows this batch changed (off the critical path).
    crate::pot_changes::flush(&env, |fut| ctx.wait_until(fut));
    crate::lobby_changes::flush(&env, |fut| ctx.wait_until(fut));
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

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
