//! Cloudflare Workers deployment for BSV Overlay Services.
//!
//! Entry point for the overlay worker. Handles HTTP routing, D1 storage
//! initialization, and Engine setup.
//!
//! Pattern from ~/bsv/rust-wallet-infra/src/lib.rs.

pub mod advertiser;
pub mod ban_storage;
pub mod broadcaster;
pub mod chain_tracker;
pub mod d1;
pub mod ef;
pub mod d1_discovery;
pub mod d1_storage;
pub mod error;
pub mod gasp_remote;
pub mod health_checker;
pub mod janitor;
pub mod mainnet_fanout;
pub mod ops;
pub mod peer_crawler;
pub mod proof_fetcher;
pub mod queue;
pub mod routes;
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
    D1AgentStorage, D1CollectedStorage, D1DmDelegationStorage, D1LowStorage, D1PotStorage,
    D1PotpartyStorage, D1PotrefundStorage, D1ProofStorage, D1ResultStorage, D1RevealStorage,
    D1SHIPStorage, D1SLAPStorage, D1UHRPStorage,
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
    let result_storage: Rc<dyn ResultStorage> = Rc::new(D1ResultStorage::new(db.clone()));
    let proof_storage: Rc<dyn ProofStorage> = Rc::new(D1ProofStorage::new(db.clone()));
    let potparty_storage: Rc<dyn PotpartyStorage> = Rc::new(D1PotpartyStorage::new(db.clone()));
    let potrefund_storage: Rc<dyn PotrefundStorage> =
        Rc::new(D1PotrefundStorage::new(db.clone()));
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
        result_storage.clone(),
        proof_storage.clone(),
        potparty_storage.clone(),
        potrefund_storage.clone(),
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
            submit(&engine, req, hosting_url.as_deref(), arcade_url, taal_api_key, &ctx).await
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
    let result_storage: Rc<dyn ResultStorage> = Rc::new(D1ResultStorage::new(db.clone()));
    let proof_storage: Rc<dyn ProofStorage> = Rc::new(D1ProofStorage::new(db.clone()));
    let potparty_storage: Rc<dyn PotpartyStorage> = Rc::new(D1PotpartyStorage::new(db.clone()));
    let potrefund_storage: Rc<dyn PotrefundStorage> =
        Rc::new(D1PotrefundStorage::new(db.clone()));
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
        result_storage,
        proof_storage,
        potparty_storage,
        potrefund_storage,
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
    result_storage: Rc<dyn ResultStorage>,
    proof_storage: Rc<dyn ProofStorage>,
    potparty_storage: Rc<dyn PotpartyStorage>,
    potrefund_storage: Rc<dyn PotrefundStorage>,
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
                    Box::new(overlay_discovery::pot::lowfund_topic_manager::LowFundTopicManager::new()),
                );
            }
            "tm_collected" => {
                managers.insert("tm_collected".into(), Box::new(CollectedTopicManager::new()));
            }
            "tm_result" => {
                managers.insert("tm_result".into(), Box::new(ResultTopicManager::new()));
            }
            "tm_proof" => {
                managers.insert("tm_proof".into(), Box::new(ProofTopicManager::new()));
            }
            "tm_potparty" => {
                managers.insert(
                    "tm_potparty".into(),
                    Box::new(PotpartyTopicManager::new()),
                );
            }
            "tm_potrefund" => {
                managers.insert(
                    "tm_potrefund".into(),
                    Box::new(PotrefundTopicManager::new()),
                );
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

    let config = EngineConfig {
        hosting_url: hosting_url.clone(),
        sync_configuration,
        ..Default::default()
    };

    // ChainTracker — SPV verification via ChainTracks API
    let chain_tracker: Option<Box<dyn bsv_rs::transaction::ChainTracker>> =
        env.var("CHAIN_TRACKER_URL").ok().map(|v| {
            Box::new(WorkerChainTracker::new(v.to_string()))
                as Box<dyn bsv_rs::transaction::ChainTracker>
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

#[event(scheduled)]
async fn scheduled(_event: worker::ScheduledEvent, env: Env, _ctx: worker::ScheduleContext) {
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
    let result_storage: Rc<dyn ResultStorage> = Rc::new(D1ResultStorage::new(db.clone()));
    let proof_storage: Rc<dyn ProofStorage> = Rc::new(D1ProofStorage::new(db.clone()));
    let potparty_storage: Rc<dyn PotpartyStorage> = Rc::new(D1PotpartyStorage::new(db.clone()));
    let potrefund_storage: Rc<dyn PotrefundStorage> =
        Rc::new(D1PotrefundStorage::new(db.clone()));
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
        result_storage.clone(),
        proof_storage.clone(),
        potparty_storage.clone(),
        potrefund_storage.clone(),
    );

    // Sync advertisements (if advertiser + hosting URL are configured).
    // Publishes any new SHIP/SLAP ads on-chain so peers can discover us.
    if let Err(e) = engine.sync_advertisements().await {
        worker::console_log!("Scheduled: Ad sync error: {}", e);
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
    match engine.start_gasp_sync().await {
        Ok(r) => {
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
        Err(e) => worker::console_log!("Scheduled: GASP sync error: {}", e),
    }

    // Peer crawl: bridge for non-GASP peers (bsvb today). `/lookup` +
    // `/submit` instead of `/requestSyncResponse`. Engine's tm_X
    // is_dupe check makes this idempotent — crawling the same peer
    // twice in a row costs compute but admits nothing new.
    let peers = non_gasp_peers();
    let crawl_result = peer_crawler::crawl_peers(&engine, &peers, "cron").await;
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

    // 2. LOW `pot_beefs` recovery store — the engine does NOT touch it, so this
    //    parallel tick fetches → verifies → stitches → trims → compacts (bypass
    //    longer-wins) each proofless pot BEEF. Its own fetcher (own per-tick
    //    subrequest budget) so the two passes don't share a budget cell.
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
    let pot_summary = crate::proof_fetcher::complete_pot_beef_proofs(
        pot_storage.as_ref(),
        &pot_fetcher,
        20,
        backstop_age,
    )
    .await;
    worker::console_log!(
        "Scheduled: proof-completion (pot_beefs) — scanned={} completed={} still_unconfirmed={} \
         fetch_failed={} stitch_failed={}",
        pot_summary.scanned,
        pot_summary.completed,
        pot_summary.still_unconfirmed,
        pot_summary.fetch_failed,
        pot_summary.stitch_failed,
    );

    // 3. LOW `pot_records` spend-confirmation chaser (#186). Own fetcher + budget
    //    cell (not shared with the pot-beef pass). Upgrades a 0-conf pot spend to
    //    spentConfirmed = 1 ONLY once the SPENDING tx's bump verifies against
    //    chaintracks — fail-closed, never downgrades.
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
    let spend_summary = crate::proof_fetcher::complete_spend_confirmations(
        pot_storage.as_ref(),
        &spend_fetcher,
        20,
        backstop_age,
    )
    .await;
    worker::console_log!(
        "Scheduled: spend-confirmation (pot_records) — scanned={} confirmed={} \
         still_unconfirmed={} fetch_failed={}",
        spend_summary.scanned,
        spend_summary.confirmed,
        spend_summary.still_unconfirmed,
        spend_summary.fetch_failed,
    );

    // Observability (#192/#193, P4): stamp the completion-pass heartbeat, bump
    // the persistent counters, and refresh the proofless first-seen ledger so a
    // dead pass / a proof-not-landing surfaces via GET /health/invariants
    // within a day (not weeks). Best-effort — never breaks the cron.
    let proofs_completed = tx_completed + pot_summary.completed as u64;
    let fetch_failed = tx_fetch_failed + pot_summary.fetch_failed as u64;
    let pot_beefs_compacted = pot_summary.completed as u64;
    let spends_confirmed = spend_summary.confirmed as u64;
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

    // Run janitor health checks
    let janitor_config = overlay_engine::health_checker::JanitorConfig::default();
    let checker = WorkerHealthChecker;
    let hosting_url = env.var("HOSTING_URL").ok().map(|v| v.to_string());
    match janitor::run_janitor(
        ship_storage.as_ref(),
        slap_storage.as_ref(),
        &checker,
        &janitor_config,
        hosting_url.as_deref(),
    )
    .await
    {
        Ok(result) => {
            worker::console_log!(
                "Scheduled: Janitor completed — SHIP: {}, SLAP: {}, evicted: {}, healthy: {}, unhealthy: {}",
                result.ship_records_checked,
                result.slap_records_checked,
                result.records_evicted,
                result.domains_healthy,
                result.domains_unhealthy,
            );
        }
        Err(e) => {
            worker::console_log!("Scheduled: Janitor error: {}", e);
        }
    }

    worker::console_log!("Scheduled tasks completed");
}

/// POST /admin/complete-proofs (#192/#193) — run the BEEF proof-completion passes
/// ON DEMAND, the SAME logic the `*/15` cron would run, from a reliably-firing
/// FETCH route (the scheduled event is not delivered to this worker — a queue+cron
/// platform quirk). Self-contained (builds its own db/engine/storages from env) +
/// fail-closed: a BUMP is stitched only once chaintracks-verified; the `has_proof`
/// latch + serve-time trim trust only verified proofs. An external cron POSTs this
/// (bearer-authed via ADMIN_TOKEN, gated at the dispatch). Returns the counters.
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
    // 2. pot_beefs recovery store (own fetcher + budget).
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
    let ps = crate::proof_fetcher::complete_pot_beef_proofs(
        pot_storage.as_ref(),
        &pot_fetcher,
        20,
        backstop_age,
    )
    .await;
    // 3. pot_records spend-confirmation chaser (#186) — own fetcher + budget.
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
    let ss = crate::proof_fetcher::complete_spend_confirmations(
        pot_storage.as_ref(),
        &spend_fetcher,
        20,
        backstop_age,
    )
    .await;
    // 4. observability heartbeat + counters (same as the cron would stamp).
    let proofs_completed = tx_completed + ps.completed as u64;
    let fetch_failed = tx_fetch_failed + ps.fetch_failed as u64;
    crate::ops::record_completion_tick(
        &ops_db,
        proofs_completed,
        fetch_failed,
        ps.completed as u64,
        ss.confirmed as u64,
    )
    .await;
    let flagged = crate::ops::refresh_proofless_watch(&ops_db).await;
    Response::from_json(&serde_json::json!({
        "status": "ok",
        "tx_completed": tx_completed,
        "pot_completed": ps.completed,
        "pot_scanned": ps.scanned,
        "pot_still_unconfirmed": ps.still_unconfirmed,
        "fetch_failed": fetch_failed,
        "spends_confirmed": ss.confirmed,
        "spends_scanned": ss.scanned,
        "spends_still_unconfirmed": ss.still_unconfirmed,
        // Observability only (≤5): which spending txids were sampled, so an
        // operator can check them on a block explorer and distinguish a broken
        // chaser from a genuinely unconfirmable backlog.
        "spends_sample": ss.sample,
        "proofless_over_24h": flagged,
    }))
}

/// Queue consumer for the onSteakReady pattern.
///
/// Processes mutation messages enqueued by /submit. Each message contains a
/// BEEF + topics + mode. The consumer builds an Engine and calls full
/// `engine.submit()` which includes Phase 3 mutations.
///
/// Dedup safety: `applied_transactions` in D1 ensures at-least-once delivery
/// is safe — duplicate messages are detected and skipped in Phase 1.
#[event(queue)]
async fn queue_handler(
    batch: worker::MessageBatch<crate::queue::MutationMessage>,
    env: Env,
    _ctx: worker::Context,
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

        let mode = match body.mode.as_str() {
            "historical-tx" => SubmitMode::HistoricalTx,
            "historical-tx-no-spv" => SubmitMode::HistoricalTxNoSpv,
            _ => SubmitMode::CurrentTx,
        };

        match engine.submit(&tagged_beef, mode).await {
            Ok(_steak) => {
                worker::console_log!("Queue: mutation applied for {} topic(s)", body.topics.len());
                msg.ack();
            }
            Err(e) => {
                worker::console_log!("Queue: mutation failed: {}", e);
                msg.retry();
            }
        }
    }

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
}
