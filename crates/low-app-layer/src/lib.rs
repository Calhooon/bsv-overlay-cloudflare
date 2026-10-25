//! low-app-layer — LOW's browser-facing READ surface for chain facts.
//!
//! The LOW game client (a browser app, `bsv-low/app`) must not read
//! WhatsOnChain: a third-party indexer in the money path is both a privacy
//! leak and an availability dependency the game can't control. Instead the
//! client reads THIS worker, which serves from the low-overlay's D1 database
//! — the same `low-overlay-db` the low-overlay worker (tm_pot / ls_pot,
//! `wrangler.low.toml`) writes — with Cloudflare edge caching in front
//! (the zanaadu "app-layer" pattern, mirrored from `~/bsv/zanaadu/app-layer`).
//!
//! Why the overlay D1 is the right source:
//! - `pot_records` is the DURABLE pot-spend landing-proof index: one row per
//!   admitted pot outpoint, updated (never deleted) when the spend is seen.
//!   It survives engine output eviction, so a spent pot remains queryable as
//!   the permanent landing proof a client checks before crediting a payout.
//! - `transactions` holds the engine's admitted BEEF bytes, which the client
//!   needs for SPV-verifiable ancestry without touching an external indexer.
//! - The chain tip comes from the same-account `rust-chaintracks` worker via
//!   a service binding (a plain workers.dev fetch to a same-account worker
//!   loops back to the caller, so the binding is required).
//!
//! NOT read-only on data (bsv-low #474): since M18-2 it files rows (the
//! filings, the hop sweeps, the owed list, the courier memos). Its write set,
//! table by table, is in `storage-ownership.json` and
//! `docs/STORAGE-OWNERSHIP.md`, and `make ci` refuses a statement the
//! manifest does not grant.
//!
//! **The DDL it issues (bsv-low #283):** the
//! overlay owns the schema and applies `OVERLAY_MIGRATIONS` on ITS cold
//! start, which is not an ordering this worker can wait for — a cold isolate
//! here can reach a column the overlay has not added yet and fail every
//! recovery query with `no such column`. So [`schema`] issues ONE additive,
//! idempotent `ALTER TABLE … ADD COLUMN` for the one column this worker reads
//! and cannot serve without, at most once per isolate, pinned byte-identical
//! to the overlay's own migration. It is not a migration runner and must not
//! become one — see `docs/DEPLOY-ORDER-AND-SCHEMA.md`.
//!
//! Routes:
//! - `GET /utxo-status?outpoints=<txid>.<vout>,…` — spent-status of up to 64
//!   pot outpoints from `pot_records` in ONE batched D1 query (fail-safe: an
//!   unknown outpoint is reported `known:false`, never asserted unspent).
//! - `GET /pots-view?outpoints=…` — the batched DERIVED view (GH
//!   bsv-low#163): the `/utxo-status` facts PLUS each recorded spender's raw
//!   tx (extracted from its stored BEEF; client hash-verifies) PLUS the
//!   chain tip, in one request / one D1 join — the query that replaces the
//!   client's per-spender `/beef` fan-out.
//! - `GET /recovery-view?identity=<66-hex>` — the seed-only BY-IDENTITY
//!   recovery view (bsv-low#189): every pot the identity is a party to
//!   (`potparty_records`, bsv-low#188) JOINed to its on-chain spend status
//!   (`pot_records`) + spender bytes (`pot_beefs`) + the chain tip, in one
//!   request / one D1 query. A missing/invalid identity is an empty result,
//!   never an error.
//! - `GET /leaderboard?limit=200` — the server-side leaderboard join + rank
//!   (bsv-low #38): the recent `result_markers_v2` markers JOINed to their
//!   `pot_records` anchor (CHUNKED, the same table `/utxo-status` reads) +
//!   `proof_markers` pointers, aggregated + ranked with the client's exact
//!   `aggregateBoard` / `lowestHands` rules — the ONE call that replaces the
//!   client's ~110-round-trip N+1. The server counts on (both sigs +
//!   anchored) and returns the sigs so the client re-verifies (see
//!   `logic`'s trust note); a D1 fault is a 5xx, never a fabricated board.
//! - `GET /results?identity=<66-hex>` — SERVER-DERIVED settle results
//!   (bsv-low #227): for every pot the identity is a party to, the chain-truth
//!   classification of the pot's spend against the four covenant-mandated
//!   templates (winner-A / winner-B / tie / height-gated refund) derived from
//!   the pot's OWN committed lock params — no client claim required. See the
//!   `results` module for the trust model + conservatism rules.
//! - `GET /refund-view?identity=<66-hex>` — the per-identity REFUND STATUS
//!   view (bsv-low #252 stage 2a): every pot the identity is a party to,
//!   shaped as a refund answer — height-gate math against the tip,
//!   refund-backup marker PRESENCE (`backupMarkerPresent` — an unverified
//!   byte-format-admitted bit, never the raw bytes; the reader verifies),
//!   and the chain-truth exit status (`armed`/`gate-open`/`landed`/
//!   `superseded`/`unknown`) with a `/results`-style `status`/`statusSource`
//!   honesty pair. See the `refund_view` module docs.
//! - `GET /refund-backups?identity=<66-hex>` — the per-identity REFUND-BACKUP
//!   BYTES view (bsv-low W2 batch B(a), 2026-09-03): every indexed
//!   `potrefund_records` row for the identity's pots, `refundRawHex`
//!   INCLUDED, bounded per pot and in all — the wiped-device seeding pass's
//!   ONE batched read (it used to ask the overlay once per pot). The reader
//!   still verifies every raw; see the `refund_backups` module docs.
//! - `GET /live-view?identity=<66-hex>` — the per-identity LIVE-HAND view
//!   (bsv-low #252 stage 2a step 3): every pot the identity is a party to
//!   with NO confirmed spend (unspent / unconfirmed-spend / never-indexed —
//!   a confirmed spend belongs to `/refund-view`/`/results`), with the
//!   `/refund-view` gate math, per-POT marker corroboration (a second
//!   bounded candidate query — the pot's OWN v2 seat-binding markers,
//!   verified under a per-request budget, since production files v1 first so
//!   the window representative is never the v2 row; `markerSource` /
//!   `opponentIdentitySource` honesty tags) and a
//!   BOUNDED tower fan-out: corroborated gameIds (quality-selected — known
//!   pots first, never raw window position) get the watchtower's public
//!   `GET /case/:gameId` (via the `TOWER` service binding, per-fetch
//!   timeout plus a pre-buffer byte budget), shaped to a validated
//!   `{status,epoch,deadlineMs,accused}` subset. Success serves the fetched
//!   `caseGameId` under the NON-VOUCHING `"tower-by-gameid-unverified"` tag
//!   (the tower's answer for that gameId — the case↔pot binding is not
//!   verified); every other state keeps `case: null` under a four-valued
//!   `caseSource` — UNKNOWN, never "no case". See the `live_view` module
//!   docs.
//! - `GET /hops-view?identity=<66-hex>` — the per-identity HOPS-IN-FLIGHT
//!   view (bsv-low #315, #252 stage 2b): every hop the identity marked
//!   (`hopparty_records`, tm_hopparty) joined to the `tm_lowfund`-indexed
//!   hop outpoint for spent/unspent status (honesty pair; an un-indexed
//!   hop is `unknown`, never asserted-unspent) plus the read-time
//!   `markerVerified` validity filter (seatSig + identitySig + the
//!   admitted hop lock re-derived from the marker's own settle pubkey —
//!   display labeling, rows never dropped). See the `hops_view` module.
//! - `GET /spent-any?outpoints=<txid>.<vout>,…` — spend status for ARBITRARY
//!   (legacy, never-indexed) outpoints via SERVER-SIDE upstream reads (WoC
//!   primary with raw hash-verification; a NEGATIVE needs Bitails
//!   corroboration; provider faults are an honest `known:false`). Short
//!   in-isolate cache; same row shape as `/utxo-status`.
//! - `GET /beef/:txid` — the admitted BEEF bytes from `transactions`.
//! - `GET /tip`: verified header height, hash and timestamp, with sync age via CHAINTRACKS; unavailable or stale is 503.
//! - `GET /health` — liveness.
//! - `GET /identities?ik=`, `GET /identity/:ik`, `GET /identity/verify/:ik`,
//!   `GET /identity/pic/:imageHash`, `POST /internal/identity/kill`: the
//!   identity views (bsv-low #532), a proxy to LOW's identity resolver through
//!   the `IDENTITY` binding; see the `identity` module.
//!
//! NO CACHING (owner call, 2026-07-14): the Cache API misbehaves on
//! workers.dev (intermittent CF 1042s observed on the first deploy), and the
//! scaling mechanism here is the QUERY COLLAPSE — one request → one D1 query
//! for a whole batch of outpoints — not a cache. Every response is
//! `no-store`.

pub mod board_view;
pub use board_view::BoardView;
/// bsv-low #441: the session lane's store (one Durable Object per BRC-104
/// session nonce AND per lane id; `bsv-middleware-cloudflare` 0.3.4).
pub use bsv_middleware_cloudflare::AuthSessionStore;
pub mod armed_pots;
pub mod auth;
pub mod beef_guard;
pub mod board_db;
pub mod compaction;
pub mod cors;
pub mod courier;
pub mod credit_beef;
/// bsv-low #499: the D1 rows ledger, the overlay's own source compiled here (this crate does not link the overlay).
#[path = "../../overlay-cloudflare/src/d1_ledger.rs"]
pub mod d1_ledger;
/// bsv-low #848 (FAST-C): the four funding-moment filings under one signature per key.
pub mod handfile;
pub mod hops_view;
pub mod hopsweep;
/// bsv-low #532: the identity views (names and pictures), a proxy to LOW's identity resolver.
pub mod identity;
pub mod internal_events;
pub mod lane_attest;
pub mod live_view;
pub mod logic;
pub mod owed;
pub mod owed_cursor;
pub mod owed_judge;
pub mod owed_read;
pub mod pass_meter;
pub mod pot_hook;
pub mod proof_post;
pub mod rate;
pub mod record_post;
pub mod refund_backups;
pub mod refund_view;
pub mod results;
mod routes;
pub mod schema;
pub mod tracker_host;
pub mod txany;

use serde_json::Value;
use worker::{event, Context, Env, Request, Response, Result, Router};

/// Worker entry — HTTP request dispatch.
///
/// OPTIONS preflight is answered before routing (it carries no body and must
/// succeed for the browser to send the real GET). Every other request passes
/// the BRC-103/104 FRONT DOOR (bsv-low #318, `auth::front_door` — lenient by
/// default, strict behind `AUTH_ENFORCE`; the tower's posture) which resolves
/// the caller's verified identity into the router data. Authenticated replies
/// are SIGNED (`sign_json_response`) so `AuthFetch` clients verify the
/// server. Every response — success, 4xx, or 5xx — gets wildcard CORS stamped
/// on the way out, so a cross-origin browser always sees the real status
/// instead of an opaque network error.
///
/// bsv-low #499 (lens fold M2): the WHOLE body runs under the request's D1 rows ledger, the internal hooks and the
/// front door's own replies included (the t=0 burst's hooks answered with no figures before), and every answer,
/// an escaped handler error's too (L5), carries `Server-Timing: d1;desc=..`, stamped last (after any signing).
/// Deploy convergence runs only on the scheduled owed-clock pass, never a fetch.
#[event(scheduled)]
async fn scheduled(_event: worker::ScheduledEvent, env: Env, ctx: worker::ScheduleContext) {
    routes::set_courier_sink(env.var("COURIER_SINK").ok().map(|v| v.to_string()));
    // bsv-low #635: the cost window this isolate holds (its last tick's statements included), written when due.
    let now = worker::Date::now().as_millis() as f64;
    if let (Some(window), Ok(db)) = (d1_ledger::take_cost_if_due(now), env.d1("OVERLAY_DB")) {
        ctx.wait_until(async move { d1_ledger::flush_cost(&db, window, now).await });
    }
    // bsv-low #781, #778 (B-FLEET-4): the pass's clock: each phase of both tasks timed, the task that ends last
    // logs one `[scheduled-pass]` line and writes the row the overlay serves on `/health/invariants`.
    let meter = std::rc::Rc::new(pass_meter::PassMeter::worker());
    let meter_drain = meter.clone();
    // E649 L4 N4: the pot hook's drain is its OWN task, handed first, under its own wall: the tracker's backfill
    // awaits CHAINTRACKS (bounded since B-FLEET-4, `runtime::BACKFILL_WALL_MS`), so it never holds the drain behind
    // it. N7: ONE deadline for the whole task: the boot, the pass (its every read, relay call, settle and counter)
    // and the courier flush run under it.
    let env_drain = env.clone();
    let drain: pot_hook::Detached = Box::pin(async move {
        let meter = meter_drain;
        let task = async {
            let deadline =
                routes::worker_deadline(pot_hook::DRAIN_WALL_MS, pot_hook::DRAIN_SETTLE_RESERVE_MS);
            if meter
                .phase(
                    "drain.boot",
                    deadline.run(d1_ledger::boot(schema::ensure_latch_columns(&env_drain))),
                )
                .await
                .is_none()
            {
                worker::console_warn!("[pot-changed] drain: boot not done at the deadline");
                return;
            }
            // E649 L4 H1: one bounded pass over the pot hook's parked wake-ups
            if let Err(e) = meter
                .phase(
                    "drain.pass",
                    routes::drain_pot_announce(&env_drain, &deadline),
                )
                .await
            {
                worker::console_warn!("[pot-changed] drain deferred: {e}");
            }
            if let Ok(db) = env_drain.d1("OVERLAY_DB") {
                // #658: the take stays inside the race; an unacknowledged batch returns on cancellation.
                let now = worker::Date::now().as_millis() as f64;
                if meter
                    .phase(
                        "drain.flush",
                        deadline.run(courier::take_and_flush(now, |pending| {
                            courier::flush_ack(Some(db), pending)
                        })),
                    )
                    .await
                    .is_none()
                {
                    worker::console_warn!(
                        "[pot-changed] drain: courier flush not done at the deadline"
                    );
                }
            }
        };
        task.await;
        pass_meter::end_task(&meter, routes::D1Sql::of(&env_drain).ok().as_ref()).await;
    });
    // bsv-low #771 (L1b), #778: the chain-event intake is its OWN task, handed second, under its own one deadline
    // (`chain_events::WALL_MS`): the feed pulled over the CHAINTRACKS binding on its durable cursor, each event's
    // affected mined rows queued in the cursor's batch, then eight queued re-asks drained. Until #778 it ran after the
    // tracker's boot, board era and backfill in one task, so any await there that never answered (the 14:15:47Z pass
    // of 2026-10-20) skipped it silently; its first statement is now the pass's durable witness
    // (`chain_events::witness`), so a pass that never reaches it is counted by the next one that does.
    let env_intake = env.clone();
    let meter_intake = meter.clone();
    let intake: pot_hook::Detached = Box::pin(async move {
        let meter = meter_intake;
        meter
            .phase(
                "tracker.chain_events",
                tracker_host::chain_events::run(&env_intake),
            )
            .await;
        pass_meter::end_task(&meter, routes::D1Sql::of(&env_intake).ok().as_ref()).await;
    });
    let env_owed = env.clone();
    let meter_owed = meter.clone();
    ctx.wait_until(async move {
        let deadline = routes::worker_deadline(20_000, 3_000);
        let work = async {
            schema::ensure_latch_columns(&env_owed).await;
            let tip = routes::chaintracks_present_height_env(&env_owed, "owed")
                .await
                .ok();
            let judge = owed_judge::D1Judge::new(
                &env_owed,
                tip,
                routes::written_off_before_ms_env(&env_owed),
            )?;
            let report = owed_judge::drain_dirty(&judge, owed_judge::OWED_DIRTY_SUBJECTS_PER_PASS)
                .await
                .map_err(worker::Error::RustError)?;
            for (_, error) in report.faults {
                worker::console_warn!("[owed] scheduled judgement deferred: {error}");
            }
            Ok::<(), worker::Error>(())
        };
        match meter_owed
            .phase("owed.drain", deadline.run_reserving(work))
            .await
        {
            Some(Ok(())) => {}
            Some(Err(error)) => worker::console_warn!("[owed] scheduled drain failed: {error}"),
            None => worker::console_warn!("[owed] scheduled drain reached its wall"),
        }
        pass_meter::end_task(&meter_owed, routes::D1Sql::of(&env_owed).ok().as_ref()).await;
    });
    let tracker: pot_hook::Detached = Box::pin(async move {
        meter
            .phase(
                "tracker.boot",
                d1_ledger::boot(schema::ensure_latch_columns(&env)),
            )
            .await;
        // bsv-low #674, the cutoff: the era cutoff stamped into the board's derivation (a no-op once it is; the
        // re-derivation once when it moves). Before the tracker's backfill.
        meter
            .phase("tracker.board_era", routes::sync_board_era_env(&env))
            .await;
        // bsv-low #778 (B-FLEET-4): the backfill's every CHAINTRACKS await is raced against its own wall
        // (`runtime::BACKFILL_WALL_MS`), so it can no longer hold the intake behind it.
        if let Err(e) = meter
            .phase("tracker.backfill", tracker_host::runtime::backfill(&env))
            .await
        {
            courier::note_presence("tracker_backfill_faulted_total");
            worker::console_warn!("[tracker-backfill] deferred: {e}");
        }
        if let Err(e) = meter
            .phase("tracker.tick", tracker_host::runtime::tick(&env))
            .await
        {
            worker::console_warn!("[tracker-tick] deferred: {e}");
        }
        // bsv-low #672 (RC-L30): the durable spender-bytes store's bounded age + row-cap sweep (the janitor's shape),
        // best-effort on the scheduled path, never on a read.
        meter
            .phase("tracker.sweep", routes::sweep_owed_spender_bytes(&env))
            .await;
        pass_meter::end_task(&meter, routes::D1Sql::of(&env).ok().as_ref()).await;
    });
    pot_hook::schedule_tick(&|f| ctx.wait_until(f), drain, intake, tracker);
}

#[event(fetch)]
pub async fn fetch(req: Request, env: Env, ctx: Context) -> Result<Response> {
    let d1_route = d1_ledger::route_of(req.path().as_str(), D1_LEDGER_ROUTES);
    let (out, d1) = d1_ledger::scoped(serve(req, env, ctx)).await;
    d1_ledger::record(d1_route, d1);
    let mut resp = match out {
        Ok(resp) => resp,
        Err(e) => {
            let mut resp = d1_ledger::escaped(&e, d1)?;
            cors::add_cors_headers(&mut resp);
            return Ok(resp);
        }
    };
    d1_ledger::stamp(&mut resp, d1);
    Ok(resp)
}

/// bsv-low #628: the census's batched flush for an answer that leaves before the router (the front door's own
/// refusals, the exempt identity routes), under the same due rule (`courier::should_flush`) and in `wait_until`.
fn flush_counters_if_due(ctx: &Context, env: &Env) {
    let pending = courier::take_pending_if_due(worker::Date::now().as_millis() as f64);
    if !pending.is_empty() {
        let db = env.d1("OVERLAY_DB").ok();
        ctx.wait_until(async move { courier::flush(db, pending).await });
    }
}

/// The fetch body, under the ledger [`fetch`] scopes.
async fn serve(req: Request, env: Env, ctx: Context) -> Result<Response> {
    let stored_owed = req.method() == worker::Method::Get && req.path() == "/owed";
    // 2026-09-04: the WoC api key for this isolate's provider reads (see routes::provider_get).
    routes::set_woc_api_key(env.secret("WOC_API_KEY").ok().map(|k| k.to_string()));
    // bsv-low #451 slice B: the Arcade endpoint for `/tx-any`'s first external witness (`routes::arcade_url`).
    routes::set_arcade_url(env.var("ARCADE_URL").ok().map(|v| v.to_string()));
    // bsv-low #731: the D1 budget tier's courier sink (a loopback origin only; unset everywhere but the tier).
    routes::set_courier_sink(env.var("COURIER_SINK").ok().map(|v| v.to_string()));
    // Readable wasm panics in `wrangler tail` (set_once → cheap on re-entry).
    console_error_panic_hook::set_once();
    // bsv-low #635 (B-COST-1): this request counted in the isolate's cost window; the window written so far goes to
    // the overlay's `worker_cost_daily` after the answer when it is due (one upsert per 64 requests or 60 s).
    if stored_owed {
        d1_ledger::note_deferred_request();
    } else if let Some(window) = d1_ledger::cost_on_request(worker::Date::now().as_millis() as f64)
    {
        if let Ok(db) = env.d1("OVERLAY_DB") {
            ctx.wait_until(async move {
                d1_ledger::flush_cost(&db, window, worker::Date::now().as_millis() as f64).await;
            });
        }
    }

    // bsv-low #532: the identity views answer CORS to the app origins only; decided before the request moves.
    let identity_cors = identity::IdentityCors::for_request(&req, &env);
    if cors::is_preflight(&req) {
        if let Some(c) = &identity_cors {
            return c.preflight();
        }
        return cors::preflight();
    }

    // bsv-low #283 / #362: this Worker never runs `OVERLAY_MIGRATIONS`, and
    // the overlay applies them on ITS cold start — so a cold isolate here can
    // reach `pp.sigValid` or `hp.markerValid` before any request has warmed
    // the overlay, and those queries fail to prepare with `no such column`.
    // One additive, idempotent statement per latch column, at most once per
    // isolate, removes the whole ordering hazard. Never fails the request;
    // see `schema`.
    //
    // The returned token is what `router` demands (gate round 2, MED-3):
    // deleting this line, or wrapping it in `if false`, is a BUILD failure —
    // there is no other way to obtain a `LatchColumnsEnsured`.
    // bsv-low #499 (lens fold L2): keyed `(boot)` in the ledger, not under the route that woke the isolate.
    let schema_ready = d1_ledger::boot(schema::ensure_latch_columns(&env)).await;

    if req.method() == worker::Method::Post && req.path() == "/internal/tx-word" {
        // bsv-low #660 (B-OWED-3-FOLD L2): the answer takes the counters, so an unheld drop reaches `ops_counters`
        let resp = tracker_host::runtime::ingest(req, &env).await;
        flush_counters_if_due(&ctx, &env);
        return resp;
    }
    // bsv-low #771 FIX-771-L1: the operator's chain-event cursor lever (POST sets, GET reads; bearer INTERNAL_TOKEN).
    if req.path() == "/internal/chain-events-cursor" {
        return tracker_host::chain_events::internal_cursor(req, &env).await;
    }

    // W2-P4: first-party internal webhooks (bearer INTERNAL_TOKEN) — served
    // BEFORE the BRC-103 front door, exactly like the relay's /broadcast:
    // the callers are our workers, not identities with a wallet.
    if req.method() == worker::Method::Post && req.path() == "/internal/tip-changed" {
        return internal_events::tip_changed(req, &env, &ctx).await;
    }
    if req.method() == worker::Method::Post && req.path() == "/internal/pot-changed" {
        return routes::internal_pot_changed(req, &env, &ctx).await;
    }
    if req.method() == worker::Method::Post && req.path() == "/internal/hop-changed" {
        return routes::internal_hop_changed(req, &env, &ctx).await;
    }
    if req.method() == worker::Method::Post && req.path() == "/internal/pot-announce-drain" {
        return routes::internal_pot_announce_drain(req, &env).await;
    }
    if req.method() == worker::Method::Post && req.path() == "/internal/lobby-changed" {
        return internal_events::lobby_changed(req, &env).await;
    }
    // bsv-low W4 (2026-09-03): the tower watchdog's population — unspent pots
    // with an admitted refund-backup marker, bearer-gated like the webhooks.
    if req.method() == worker::Method::Get && req.path() == "/internal/armed-pots" {
        return armed_pots::armed_pots(req, &env).await;
    }

    // bsv-low #532: the picture bytes (the front door's JSON signing would destroy them; an `<img>` sends no
    // auth) and the operator's kill list (bearer INTERNAL_TOKEN) are served before the front door.
    if identity::is_front_door_exempt(&req.method(), &req.path()) {
        let mut resp = identity::serve_exempt(req, &env).await?;
        // The picture gets the app-origin set; the kill route, like every `/internal/*` route, gets no CORS.
        if let Some(c) = &identity_cors {
            c.apply(&mut resp);
        }
        flush_counters_if_due(&ctx, &env);
        return Ok(resp);
    }

    // BRC-103/104 front door: handshake replies / strict-mode refusals /
    // middleware refusals return here; otherwise the request proceeds with
    // the resolved [`auth::AuthState`] as the router data (in-process only —
    // never a header a stranger could forge, see `auth`'s Rule 8b note).
    let (req, mut state) = match auth::front_door(req, &env).await? {
        auth::FrontDoor::Proceed(req, state) => (req, state),
        auth::FrontDoor::Reply(mut resp) => {
            stamp_cors(&mut resp, &identity_cors);
            if !stored_owed {
                flush_counters_if_due(&ctx, &env);
            }
            return Ok(resp);
        }
    };
    // Stored owed reads cannot schedule work, including shared counter flushes.
    let ctx = std::rc::Rc::new(ctx);
    state.wait = (!stored_owed).then(|| ctx.clone());
    // Keep the session for reply-signing; the state moves into the router.
    let session = state.session.clone();
    let lane = state.lane.clone();
    let lane_offer = state.lane_offer.clone();

    // bsv-low #451 slice B: the courier census's durable deltas ride ONE D1 batch, after the answer, when a flush is
    // due (`courier::should_flush`: the isolate's exact head, then by calls or by time).
    let counters_db = env.d1("OVERLAY_DB").ok();
    // bsv-low #499: the figures ride the answer's `Server-Timing` (stamped by `fetch`, after this body signed or
    // sealed the response) and `/health.d1Budget`.
    let mut resp = router(state, schema_ready).run(req, env).await?;
    if !stored_owed {
        let pending = courier::take_pending_if_due(worker::Date::now().as_millis() as f64);
        if !pending.is_empty() {
            ctx.wait_until(async move { courier::flush(counters_db, pending).await });
        }
    }

    // Sign the terminal JSON for an AUTHENTICATED caller (the tower's
    // posture: re-serialize the handler's JSON at its own status code, sign,
    // then stamp no-store + CORS back on — `sign_json_response` builds a
    // fresh response, so the handler's cache header must be re-applied).
    // bsv-low #441: a call that rode the session lane is answered SEALED under
    // the lane (the exact bytes sent are the bytes MAC'd), never signed.
    if let Some(lane) = lane {
        let status = resp.status_code();
        let text = resp.text().await.unwrap_or_else(|_| {
            serde_json::json!({ "error": "handler returned an unreadable response" }).to_string()
        });
        resp = bsv_middleware_cloudflare::seal_lane_response_text(text, status, &lane)
            .map_err(|e| worker::Error::from(e.to_string()))?;
        stamp_cors(&mut resp, &identity_cors);
        return Ok(resp);
    }
    if let Some(session) = session {
        let status = resp.status_code();
        let value: Value = resp.json().await.unwrap_or_else(
            |_| serde_json::json!({ "error": "handler returned a non-JSON response" }),
        );
        // bsv-low #441: the request that MINTED a lane gets the offer on its
        // signed reply (`x-bsv-lane-offer`, a signable header).
        let extra: Vec<(String, String)> = match lane_offer {
            Some(offer) => vec![(
                bsv_middleware_cloudflare::session_lane::LANE_OFFER_HEADER.to_string(),
                bsv_middleware_cloudflare::session_lane::offer_header_value(&offer),
            )],
            None => vec![],
        };
        resp = bsv_middleware_cloudflare::sign_json_response(&value, status, &extra, &session)
            .map_err(|e| worker::Error::from(e.to_string()))?;
        resp.headers_mut().set("Content-Type", "application/json")?;
        resp.headers_mut().set("Cache-Control", "no-store")?;
    }
    stamp_cors(&mut resp, &identity_cors);
    Ok(resp)
}

/// The wildcard CORS of every route, or the identity views' app-origin set (bsv-low #532).
fn stamp_cors(resp: &mut Response, identity_cors: &Option<identity::IdentityCors>) {
    match identity_cors {
        Some(c) => c.apply(resp),
        None => cors::add_cors_headers(resp),
    }
}

/// bsv-low #499: the routes the D1 rows ledger keys by name (`d1_ledger::route_of`; anything else is `other`). A
/// route with a path parameter is listed by its fixed head.
pub const D1_LEDGER_ROUTES: &[&str] = &[
    "/owed",
    "/pots-view",
    "/results",
    "/utxo-status",
    "/recovery-view",
    "/refund-view",
    "/refund-backups",
    "/live-view",
    "/hops-view",
    "/leaderboard",
    "/record",
    "/proof",
    "/lane/attest",
    "/spent-any",
    "/tx-any",
    "/beef",
    "/credit-beef",
    "/rate",
    "/tip",
    "/epoch",
    "/health",
    // bsv-low #532 (the merge over #499): the identity views, each its own key. A more specific head is listed before
    // the head it starts with (`route_of` takes the first match): the picture before `/identity`, the kill route
    // before `/internal`.
    "/identities",
    "/identity/pic",
    "/identity",
    "/internal/identity/kill",
    // lens fold M2: the first-party hooks and the tower's population, one key (the overlay keys `/internal` too)
    "/internal",
];

/// The route table. All GET, all JSON; unknown paths get a JSON 404 via the
/// `or_else_any_method` catch-alls (worker-rs' default no-match 404 is plain
/// text, so both `/` and the wildcard are registered explicitly).
///
/// `_schema` is a [`schema::LatchColumnsEnsured`] and is deliberately
/// unused: it is a CAPABILITY, not data. Routes below read `pp.sigValid`
/// and `hp.markerValid`, and this parameter is what makes the schema
/// catch-up impossible to skip without failing the build (gate round 2,
/// MED-3 — see the `schema` module doc). Do not delete it to silence a lint.
fn router(
    state: auth::AuthState,
    _schema: schema::LatchColumnsEnsured,
) -> Router<'static, auth::AuthState> {
    Router::with_data(state)
        .get_async("/utxo-status", routes::utxo_status)
        .get_async("/pots-view", routes::pots_view)
        .get_async("/recovery-view", routes::recovery_view)
        .get_async("/leaderboard", routes::leaderboard)
        .get_async("/results", routes::results)
        // bsv-low P1.1 proof-in-DB (owner GO 2026-09-02): the winner posts the
        // transcript proof bundle here instead of spending it on chain.
        .get_async("/proof", proof_post::proof_get)
        .post_async("/proof", proof_post::proof_post)
        // bsv-low M18-2 (B), W-E: every index marker FILED here instead of bought on
        // chain — the same bytes, the same verifiers, the same tables.
        .post_async("/record", record_post::record_post)
        // bsv-low #443 step 4: this door's lane from the relay's attestation (no handshake, no signed read).
        .post_async("/lane/attest", lane_attest::lane_attest)
        .get_async("/refund-view", routes::refund_view)
        // bsv-low W2 batch B(a) (owner ruling 2026-09-03): the wiped-device
        // seeding pass reads every refund backup in ONE batched call here
        // instead of one overlay lookup per pot.
        .get_async("/refund-backups", routes::refund_backups)
        .get_async("/hops-view", routes::hops_view)
        // bsv-low #469: THE OWED LIST — one served answer for the games page (computed on write, served on read).
        .get_async("/owed", routes::owed)
        .get_async("/live-view", routes::live_view)
        .get_async("/spent-any", routes::spent_any)
        .get_async("/tx-any/:txid", routes::tx_any)
        // bsv-low W-C.3 (2026-09-09/10): the same answer for up to 50 INDEX-HELD
        // txids in one round trip (the home surface's per-tx presence sweep);
        // every txid the index does not decide is listed `unknown` for the
        // per-txid route.
        .get_async("/tx-any", routes::tx_any_batch)
        .get_async("/beef/:txid", routes::beef)
        // #209 follow-up: server-assembled credit ancestry, so the client
        // never stores BEEF bytes to survive a reload (see `credit_beef`).
        .get_async("/credit-beef/:txid", routes::credit_beef)
        // bsv-low #497 (2026-09-20): the exchange rate from a courier ladder behind a per-isolate cache — the
        // browser never calls a courier (the same-origin WoC proxy is gone).
        .get_async("/rate", routes::rate)
        .get_async("/tip", routes::tip)
        // bsv-low #532: the identity views (`identity`); `/identity/*rest` is `:ik` or `verify/:ik` (the picture
        // route is served before the front door).
        .get_async(identity::BATCH_ROUTE, identity::get_batch)
        .get_async("/identity/*rest", identity::get_identity)
        .get("/epoch", routes::epoch)
        .get_async("/health", routes::health)
        .or_else_any_method("/", routes::not_found)
        .or_else_any_method("/*catchall", routes::not_found)
}

#[cfg(test)]
mod d1_ledger_door_pins {
    fn squash(s: &str) -> String {
        s.lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
            .split_whitespace()
            .collect()
    }

    /// bsv-low #499 (lens fold M2, L5): the WHOLE fetch body runs under the request's ledger, so the internal hooks
    /// (the t=0 burst's own door), the latch-column boot and the front door's replies are counted and stamped, and
    /// an escaped handler error is answered with its figures in both workers. The runtime half is the tier's (every
    /// `/internal/pot-changed` answer carries `d1;desc=`). RED on `a0db9fb`: the scope wrapped only the router, after
    /// the hook dispatch and the front door, and an escaped `Err` left through `out?` unstamped.
    #[test]
    fn the_hooks_and_the_front_door_run_inside_the_ledger_and_an_escaped_error_is_stamped() {
        let lib = squash(include_str!("lib.rs"));
        let fetch = lib
            .find(&squash(
                "pub async fn fetch(req: Request, env: Env, ctx: Context) -> Result<Response> {",
            ))
            .expect("the entry");
        let serve = lib
            .find(&squash(
                "async fn serve(req: Request, env: Env, ctx: Context) -> Result<Response> {",
            ))
            .expect("the body");
        let entry = &lib[fetch..serve];
        assert!(
            entry.contains(&squash("d1_ledger::scoped(serve(req, env, ctx)).await;")),
            "fetch runs the whole body under the ledger"
        );
        assert!(
            entry.contains(&squash(
                "Err(e) => { let mut resp = d1_ledger::escaped(&e, d1)?;"
            )),
            "an escaped error is answered stamped"
        );
        let body = &lib[serve..];
        let end = body
            .find(&squash("pub const D1_LEDGER_ROUTES"))
            .expect("the body's end");
        let body = &body[..end];
        for door in [
            "\"/internal/tip-changed\"",
            "\"/internal/pot-changed\"",
            "\"/internal/hop-changed\"",
            "\"/internal/lobby-changed\"",
            "\"/internal/armed-pots\"",
            "auth::front_door(req,&env)",
            "schema::ensure_latch_columns(&env)",
        ] {
            assert!(
                body.contains(door),
                "{door} is served inside the scoped body"
            );
        }
        assert!(
            !body.contains("d1_ledger::scoped("),
            "one scope per request, at the entry"
        );
        assert!(
            lib.contains(&squash("\"/internal\",")),
            "the hooks have their ledger key"
        );
        let overlay = squash(include_str!("../../overlay-cloudflare/src/lib.rs"));
        assert!(
            overlay.contains(&squash(
                "Err(e) => return crate::d1_ledger::escaped(&e, tally),"
            )),
            "the overlay stamps an escaped error too"
        );
        assert!(
            overlay.contains(&squash(
                "crate::d1_ledger::boot(ensure_overlay_migrations(&db))"
            )),
            "the overlay's migrations are keyed (boot)"
        );
    }
}

#[cfg(test)]
mod beef_door_replay;

#[cfg(test)]
mod obs_625_tests;

#[cfg(test)]
mod obs_counters_tests;

#[cfg(test)]
#[path = "../../worker-build-info/src/worker_tests.rs"]
mod build_info_tests;

/// bsv-low #635 (B-COST-1): the app layer's doors write the shared cost window into the overlay's
/// `worker_cost_daily` (the table the overlay migrates and serves on `/health/invariants.costBar`).
#[cfg(test)]
mod b_cost_1_wiring {
    fn squash(s: &str) -> String {
        s.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    #[test]
    fn b_cost_1_every_app_layer_door_counts_and_writes_the_window() {
        let lib = squash(include_str!("lib.rs"));
        let serve = &lib[lib
            .find("async fn serve(req: Request, env: Env, ctx: Context)")
            .unwrap()..];
        let serve = &serve[..serve.find("identity::IdentityCors::for_request").unwrap()];
        assert!(
            serve.contains("d1_ledger::cost_on_request(")
                && serve.contains("ctx.wait_until(async move { d1_ledger::flush_cost(&db, window,"),
            "the fetch door counts the request and hands a due window to wait_until, before any route answers"
        );
        let cron = &lib[lib.find("async fn scheduled(").unwrap()..];
        let cron = &cron[..cron.find("let env_drain").unwrap()];
        assert!(
            cron.contains("d1_ledger::take_cost_if_due(now)")
                && cron.contains("d1_ledger::flush_cost(&db, window, now)")
        );
        let actor = squash(include_str!("board_view.rs"));
        let flush = &actor[actor.find("fn flush_counters_if_due(&self)").unwrap()..];
        let flush = &flush[..flush.find("async fn serve_fetch(").unwrap()];
        assert!(
            flush.contains("crate::d1_ledger::take_cost_if_due(now)"),
            "the actor writes its walks' window"
        );
        // The overlay's table, created by its migration; this crate writes the same upsert through the shared source.
        assert!(bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS
            .contains(&crate::d1_ledger::COST_DAILY_CREATE));
        assert_eq!(crate::d1_ledger::COST_WORKER, "low-app-layer");
    }
}
/// FAST-C-LENS and FAST-C-FOLD: regression cases over the handfile family.
#[cfg(test)]
mod handfile_lens;
