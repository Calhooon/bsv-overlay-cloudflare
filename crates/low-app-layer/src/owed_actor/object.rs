//! `OwedActor`, the Durable Object (bsv-low #912; `DESIGN-D2-ACTOR-2026-10-26.md` section 4). One per identity
//! (`idFromName` of the lowercased 66-hex key, LOW-1), its own SQLite (`new_sqlite_classes`, tag `v4-owed-actor`),
//! reached only by binding: the consumer's delivery and the writers' direct call (`/apply`), the read (`/rows`), the
//! wipe's lever (`/reset`, LOW-2). It accepts no public input: the Worker's router never forwards a public request to
//! it (the wipe's two bearer levers below are the operator's).
//!
//! Routes (POST unless named): `/apply {identity, games, kind, seq, atMs, tip, txfacts?}` applies one event (the
//! closure, then the section; the facts of a `txfacts` event held first); `GET /rows?identity=` its stored rows and
//! its ack watermark; `GET /stored?identity=` its stored rows (the reconcile's read); `GET /bars` the object's own
//! bars; `/reset` deletes its whole storage (the alarm included), reached from outside only through the bearer lever
//! `POST /internal/owed-actor-reset` over the identity list `GET /internal/owed-actor-identities` reads from D1.
//! The alarm writes the coalesced KV copy (at most one a second, MEDIUM-9) and re-judges its gated games when the
//! block it waits for has come (MEDIUM-5; the hook's `tip` event stays the second belt).
use super::*;
use crate::pot_hook::SqlPort;
use worker::*;

/// The alarm's cadence while the actor waits for a block (a gated row or a courier-dated sweep): about a block.
pub const TIP_ALARM_MS: u64 = 600_000;
/// The meta keys.
const META_SCHEMA: &str = "schema";
const META_ACK: &str = "ackSeq";
const META_GATE: &str = "gate";
const META_KV_DIRTY: &str = "kvDirty";
const META_JUDGED_AT: &str = "judgedAtMs";
const META_IDENTITY: &str = "identity";
const META_BOOTSTRAPPED: &str = "bootstrapped";
const META_BOOT_CURSOR: &str = "bootCursor";
/// MEDIUM-4: the last tip the tip alarm judged (its next window is `(tipJudged, tip]`).
const META_TIP_JUDGED: &str = "tipJudged";
/// NEW2-L5: the tip window's carried faults ([`tip_window`]), a JSON object of game to consecutive faults.
const META_TIP_FAULTS: &str = "tipFaults";
/// LOW-6: the bootstrap's first keyset cursor (`[at, gameId]` of the last page; this one precedes every game).
pub const BOOT_CURSOR_START: &str = "[9223372036854775807,\"\"]";
/// The bootstrap's keyset from its stored cursor (anything that is not one, the HIGH-4 re-arm's `0` included, is
/// the start).
pub fn boot_keyset(cursor: Option<&str>) -> (i64, String) {
    cursor
        .and_then(|c| serde_json::from_str::<Value>(c).ok())
        .and_then(|v| Some((v.get(0)?.as_i64()?, v.get(1)?.as_str()?.to_string())))
        .unwrap_or((i64::MAX, String::new()))
}

/// The actor's SQLite as a [`SqlPort`]. Each statement runs synchronously (`ctx.storage.sql.exec`): a batch runs in
/// one turn of the object's event loop, so its writes commit together (the platform's write coalescing); a
/// statement that faults ends the batch and its earlier statements stand (the mirror is a cache of the record the
/// next closure replaces; the judge's commit is the fixpoint's alone and a fault of it restores the identity's rows,
/// `commit_fixpoint`, the D2-ACTOR-B lens's HIGH-2).
pub struct DoSql {
    sql: SqlStorage,
}
impl DoSql {
    pub fn of(state: &State) -> Self {
        Self {
            sql: state.storage().sql(),
        }
    }
    fn run(&self, st: &Stmt) -> std::result::Result<StmtOut, String> {
        let binds: Vec<SqlStorageValue> = st
            .binds
            .iter()
            .map(|b| match b {
                Bind::Text(t) => SqlStorageValue::String(t.clone()),
                Bind::Int(n) => SqlStorageValue::Integer(*n),
                Bind::Null => SqlStorageValue::Null,
            })
            .collect();
        let cursor = self.sql.exec(&st.sql, binds).map_err(|e| e.to_string())?;
        let rows: Vec<Value> = cursor.to_array().map_err(|e| e.to_string())?;
        Ok(StmtOut {
            changes: Some(cursor.rows_written() as u64),
            rows,
        })
    }
}
impl SqlPort for DoSql {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> std::result::Result<Vec<StmtOut>, String> {
        stmts.iter().map(|s| self.run(s)).collect()
    }
    async fn counters(&self, _: Vec<(String, u64)>) {}
    fn now_ms(&self) -> u64 {
        Date::now().as_millis()
    }
    fn log(&self, line: &str) {
        console_log!("{line}");
    }
}

/// The outside the judge asks from the actor: the word views' header rechecks and the canonical roots (CHAINTRACKS).
pub struct EnvOutside<'a>(pub &'a Env);
impl Outside for EnvOutside<'_> {
    async fn word_views(
        &self,
        rows: &BTreeMap<String, crate::tracker_host::stored::Row>,
    ) -> std::result::Result<BTreeMap<String, Value>, String> {
        Ok(crate::tracker_host::runtime::views(self.0, rows).await)
    }
    async fn canonical_root(&self, height: u64) -> Option<String> {
        crate::beef_guard::canonical_root(self.0, height).await
    }
}

/// The schema statements of a fresh (or a rebuilt) actor: drop what an older digest made, then create.
///
/// The D2-ACTOR-B lens's HIGH-4: a digest change ([`actor_digest`]: the mirror's generated DDL, or the actor's own
/// tables) drops the JUDGE's tables too (`owed_rows`, `owed_row_words`, `owed_state`, `owed_dirty`, their indexes and
/// triggers with them; `CREATE ... IF NOT EXISTS` keeps an old shape), and the actor's own `txfacts`, `applied` and
/// the commit's snapshot tables; its judged state in `meta` is cleared (`bootstrapped`, the ack, the gate, the
/// judgement's time) and, where it knows its identity, its bootstrap is re-armed from the first page (the object
/// sets its alarm, [`OwedActor`]'s `rebuilt`), so a read serves `pending` until the bootstrap ends (Rule 23: never
/// a short list served as complete).
pub fn schema_plan(held_digest: Option<&str>) -> Vec<String> {
    schema_plan_kept(held_digest, None, &[])
}

/// The second delta lens's NEW2-L4: runs every statement of `plan` through `exec`, and stamps the render's `digest`
/// and `shape` ([`META_SCHEMA`], `shape`) through `stamp` only when EVERY statement answered. A statement that fails
/// is logged and leaves the stamps where they were, so the object's next ensure (its next instance) computes the same
/// plan and retries it; an actor whose index `CREATE` failed is never stamped as holding the render's indexes (a
/// statement naming the index in `INDEXED BY` would fail on every apply until the next digest move). Answers whether
/// the plan applied whole.
///
/// The third delta lens's LOW-2: `shape` is stamped when every TABLE statement answered (every statement but an
/// index's `CREATE INDEX` or `DROP INDEX`), so a rebuild whose one `CREATE INDEX` failed is retried by the next
/// instance as the INDEX-ONLY move ([`schema_plan_kept`]): the rows and `bootstrapped` kept, no second rebuild and no
/// second bootstrap. The digest is stamped only when the plan is whole, as before. The caller counts a plan that did
/// not apply whole (`owed_actor_schema_partial_total`, served `schemaPartial`).
pub fn ensure_schema(
    plan: &[String],
    mut exec: impl FnMut(&str) -> std::result::Result<(), String>,
    mut log: impl FnMut(String),
    mut stamp: impl FnMut(&str, &str),
) -> bool {
    let mut whole = true;
    let mut tables_whole = true;
    for s in plan {
        if let Err(e) = exec(s) {
            whole = false;
            if !is_index_statement(s) {
                tables_whole = false;
            }
            log(format!("[owed-actor] schema: {e}: {s}"));
        }
    }
    if whole {
        stamp(META_SCHEMA, &actor_digest());
        stamp("shape", &actor_shape());
    } else if tables_whole {
        stamp("shape", &actor_shape());
        log("[owed-actor] schema: an index statement failed; shape stamped, digest NOT stamped (the next ensure retries the indexes only)".into());
    } else {
        log("[owed-actor] schema: the plan did not apply whole; digest and shape NOT stamped (the next ensure retries it)".into());
    }
    whole
}

/// LOW-2: an index's own statement (`CREATE INDEX`, `DROP INDEX`), the part of a plan [`actor_shape`] does not hold.
pub fn is_index_statement(ddl: &str) -> bool {
    is_plain_index(ddl) || ddl.trim_start().starts_with("DROP INDEX")
}

/// The delta lens's NEW-M2: [`schema_plan`] with the held SHAPE ([`actor_shape`]) and the held indexes' names. A
/// digest move whose shape is the render's is index-only: every held index dropped and the render's created, the rows
/// and `meta` kept (no rebuild, no bootstrap, no read served `pending`). Otherwise the rebuild of HIGH-4.
pub fn schema_plan_kept(
    held_digest: Option<&str>,
    held_shape: Option<&str>,
    held_indexes: &[String],
) -> Vec<String> {
    if held_digest == Some(actor_digest().as_str()) {
        return Vec::new();
    }
    if held_digest.is_some() && held_shape == Some(actor_shape().as_str()) {
        let mut out: Vec<String> = held_indexes
            .iter()
            .map(|i| format!("DROP INDEX IF EXISTS \"{}\"", i.replace('"', "")))
            .collect();
        out.extend(mirror_ddl());
        return out;
    }
    let mut out = Vec::new();
    if held_digest.is_some() {
        for t in FAMILIES
            .iter()
            .map(|f| f.table)
            .chain(JUDGE_TABLES)
            .chain(["txfacts", "applied"])
        {
            out.push(format!("DROP TABLE IF EXISTS {t}"));
        }
        for t in SNAPSHOT_TABLES {
            out.push(format!("DROP TABLE IF EXISTS {t}_snap"));
        }
    }
    out.extend(mirror_ddl());
    if held_digest.is_some() {
        out.push(format!(
            "DELETE FROM meta WHERE name IN ('{META_BOOTSTRAPPED}','{META_BOOT_CURSOR}','{META_ACK}','{META_GATE}','{META_JUDGED_AT}','{META_KV_DIRTY}','{META_TIP_JUDGED}','{META_TIP_FAULTS}')"
        ));
        out.push(format!(
            "INSERT INTO meta(name,value) SELECT '{META_BOOT_CURSOR}','0' WHERE EXISTS (SELECT 1 FROM meta WHERE name='{META_IDENTITY}')"
        ));
    }
    out
}

/// The indexes the actor's SQLite holds that a statement made (never an automatic one).
pub const HELD_INDEXES_SQL: &str =
    "SELECT name FROM sqlite_master WHERE type='index' AND sql IS NOT NULL ORDER BY name";

/// The `/apply` body.
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplyBody {
    pub identity: String,
    pub games: Vec<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub seq: Option<i64>,
    #[serde(default)]
    pub at_ms: Option<u64>,
    #[serde(default)]
    pub tip: Option<u64>,
    #[serde(default)]
    pub txfacts: Vec<Value>,
    /// A `word` event whose word is `mined`: its judgement is the collect-after-mined bar's sample.
    #[serde(default)]
    pub mined: bool,
}

#[durable_object]
pub struct OwedActor {
    state: State,
    env: Env,
    /// MEDIUM-2: the in-object serial queue. Every apply (its closure's outside reads included) takes the turn, so an
    /// event arriving meanwhile is applied after it, never interleaved at an await.
    turn: futures_util::lock::Mutex<()>,
    ready: RefCell<bool>,
    /// HIGH-4: this isolate rebuilt the actor under a new digest; its bootstrap's alarm is set at the next await.
    rebuilt: RefCell<bool>,
    /// The third delta lens's LOW-2: this isolate's schema plan did not apply whole; counted at the next await.
    schema_partial: RefCell<bool>,
    bars: RefCell<Bars>,
    last_kv_ms: RefCell<Option<u64>>,
}

impl DurableObject for OwedActor {
    fn new(state: State, env: Env) -> Self {
        Self {
            state,
            env,
            turn: futures_util::lock::Mutex::new(()),
            ready: RefCell::new(false),
            rebuilt: RefCell::new(false),
            schema_partial: RefCell::new(false),
            bars: RefCell::default(),
            last_kv_ms: RefCell::new(None),
        }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        let url = req.url()?;
        let path = url.path().to_string();
        // MEDIUM-3: a read of an object never applied writes nothing (no schema, no meta, no alarm); the read's caller
        // asked D1 first and arms a bootstrap explicitly (`/bootstrap`) only for an identity D1 knows.
        if req.method() == Method::Get && path == "/rows" && self.is_fresh() {
            return Response::from_json(
                &json!({"identity": null, "fresh": true, "bootstrapped": false, "bootstrapArmed": false}),
            );
        }
        if path != "/reset" && path != "/bars" {
            self.check_epoch().await;
        }
        self.rearm_after_rebuild().await;
        match (req.method(), path.as_str()) {
            (Method::Post, "/bootstrap") => {
                let body: Value = req.json().await.unwrap_or(Value::Null);
                let Some(me) = body["identity"].as_str().and_then(actor_name) else {
                    return Response::error("identity is not a 66-hex key", 400);
                };
                let (armed, already) = self.arm_bootstrap(&me).await;
                Response::from_json(
                    &json!({"identity": me, "armed": armed, "bootstrapped": already}),
                )
            }
            (Method::Post, "/apply") => {
                let body: ApplyBody = match req.json().await {
                    Ok(b) => b,
                    Err(e) => return Response::error(format!("bad apply body: {e}"), 400),
                };
                self.apply_route(body).await
            }
            (Method::Get, "/rows") => {
                let identity = url
                    .query_pairs()
                    .find(|(k, _)| k == "identity")
                    .map(|(_, v)| v.to_string());
                self.rows_route(identity).await
            }
            // The reconcile's read (MEDIUM-10): the stored rows, the words stripped as stored, no apply.
            (Method::Get, "/stored") => {
                let local = self.local();
                let me = url
                    .query_pairs()
                    .find(|(k, _)| k == "identity")
                    .and_then(|(_, v)| actor_name(&v));
                let rows = me
                    .as_deref()
                    .map(|m| self.rows_of(&local, m))
                    .unwrap_or_default();
                Response::from_json(&json!({"identity": me, "rows": rows}))
            }
            (Method::Get, "/bars") => {
                Response::from_json(&self.bars.borrow().served(Date::now().as_millis()))
            }
            (Method::Post, "/reset") => {
                let _turn = self.turn.lock().await;
                self.state.storage().delete_all().await?;
                let _ = self.state.storage().delete_alarm().await;
                *self.ready.borrow_mut() = false;
                Response::from_json(&json!({"reset": true}))
            }
            _ => Response::error("not found", 404),
        }
    }

    async fn alarm(&self) -> Result<Response> {
        self.check_epoch().await;
        self.rearm_after_rebuild().await;
        let local = self.local();
        if self.meta(META_BOOT_CURSOR).is_some() {
            if let Err(e) = self.bootstrap_page(&local).await {
                console_log!("[owed-actor] bootstrap page: {e}");
                let _ = self
                    .state
                    .storage()
                    .set_alarm(Date::now().as_millis() as i64 + 60_000)
                    .await;
            }
        }
        if self.meta(META_KV_DIRTY).as_deref() == Some("1") {
            self.write_kv_copy(&local).await;
        }
        // MEDIUM-4: the tip alarm judges the gates in `(tipJudged, tip]` and the courier-dated sweeps below the tip,
        // then arms at the next gate (never a re-judge of every open gate each ten minutes).
        if let (Some(_), Some(identity)) = (self.meta(META_GATE), self.meta(META_IDENTITY)) {
            let tip = crate::owed_events::tip_memoised(&self.env, true).await;
            if let Some(t) = tip {
                let last = self
                    .meta(META_TIP_JUDGED)
                    .and_then(|v| v.parse::<i64>().ok())
                    .unwrap_or(-1);
                let games = self.gated_games(&local, t, last);
                // The second delta lens's NEW2-L5 (over the delta lens's NEW-L1): `tipJudged` moves past the window;
                // a game whose apply faulted is carried in `tipFaults` and retried, parked at its third consecutive
                // fault, counted once an alarm and named in its line ([`tip_window`]).
                let held = self.tip_faults();
                let window = tip_window(&identity, games, &held, t as i64, |chunk| {
                    let body = ApplyBody {
                        identity: identity.clone(),
                        games: chunk,
                        kind: Some("tip".into()),
                        seq: None,
                        at_ms: None,
                        tip,
                        txfacts: Vec::new(),
                        mined: false,
                    };
                    async move {
                        self.apply_route(body)
                            .await
                            .is_ok_and(|r| r.status_code() == 200)
                    }
                })
                .await;
                self.put_meta(&local, META_TIP_FAULTS, &json!(window.faults).to_string());
                self.put_meta(&local, META_TIP_JUDGED, &window.tip_judged.to_string());
                if let Some(line) = &window.line {
                    console_log!("{line}");
                    if let Ok(record) = crate::routes::D1Sql::of(&self.env) {
                        record
                            .counters(vec![
                                (COUNTERS[18].into(), window.counted),
                                (COUNTERS[19].into(), window.parked_counted),
                            ])
                            .await;
                    }
                }
            }
            self.arm_tip_alarm(&local, tip).await;
        }
        Response::ok("ok")
    }
}

impl OwedActor {
    fn local(&self) -> DoSql {
        let local = DoSql::of(&self.state);
        if !*self.ready.borrow() {
            let held: Option<String> = local
                .sql
                .exec("SELECT value FROM meta WHERE name='schema'", None)
                .ok()
                .and_then(|c| c.to_array::<Value>().ok())
                .and_then(|r| {
                    r.first()
                        .and_then(|v| v["value"].as_str().map(str::to_string))
                });
            let read_one = |sql: &str| -> Option<String> {
                local
                    .sql
                    .exec(sql, None)
                    .ok()
                    .and_then(|c| c.to_array::<Value>().ok())
                    .and_then(|r| {
                        r.first()
                            .and_then(|v| v["value"].as_str().map(str::to_string))
                    })
            };
            let held_shape = read_one("SELECT value FROM meta WHERE name='shape'");
            let held_indexes: Vec<String> =
                if held.is_some() && held.as_deref() != Some(actor_digest().as_str()) {
                    local
                        .sql
                        .exec(HELD_INDEXES_SQL, None)
                        .ok()
                        .and_then(|c| c.to_array::<Value>().ok())
                        .unwrap_or_default()
                        .iter()
                        .filter_map(|r| r["name"].as_str().map(str::to_string))
                        .collect()
                } else {
                    Vec::new()
                };
            let plan = schema_plan_kept(held.as_deref(), held_shape.as_deref(), &held_indexes);
            let index_only =
                held.is_some() && held_shape.as_deref() == Some(actor_shape().as_str());
            if held.is_some() && !plan.is_empty() && !index_only {
                console_log!("[owed-actor] rebuilt under a new digest: the judge's tables dropped, the bootstrap re-armed");
                *self.rebuilt.borrow_mut() = true;
            } else if !plan.is_empty() && index_only {
                console_log!(
                    "[owed-actor] an index-only digest move: the indexes rebuilt, the rows kept"
                );
            }
            // NEW2-L4: `digest` and `shape` are stamped only when the whole plan applied; this instance does not
            // run the plan again (`ready`), its next instance does.
            let whole = ensure_schema(
                &plan,
                |s| {
                    local
                        .sql
                        .exec(s, None)
                        .map(|_| ())
                        .map_err(|e| e.to_string())
                },
                |l| console_log!("{l}"),
                |name, value| {
                    let _ = local.sql.exec(
                        META_PUT_SQL,
                        vec![
                            SqlStorageValue::from(name.to_string()),
                            SqlStorageValue::from(value.to_string()),
                        ],
                    );
                },
            );
            if !whole {
                *self.schema_partial.borrow_mut() = true;
            }
            *self.ready.borrow_mut() = true;
        }
        local
    }

    /// HIGH-4: after a rebuild the bootstrap's first page runs on the next alarm (now): the read serves `pending`
    /// until it ends.
    async fn rearm_after_rebuild(&self) {
        let _ = self.local();
        // LOW-2: a plan that did not apply whole is counted once an instance.
        if self.schema_partial.replace(false) {
            if let Ok(record) = crate::routes::D1Sql::of(&self.env) {
                record.counters(vec![(COUNTERS[20].into(), 1)]).await;
            }
        }
        if self.rebuilt.replace(false) && self.meta(META_BOOT_CURSOR).is_some() {
            let _ = self
                .state
                .storage()
                .set_alarm(Date::now().as_millis() as i64)
                .await;
        }
    }

    /// MEDIUM-3: an object no apply, bootstrap or read ever wrote (no `meta` table): a read of it writes nothing.
    fn is_fresh(&self) -> bool {
        !*self.ready.borrow()
            && self
                .state
                .storage()
                .sql()
                .exec(
                    "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='meta'",
                    None,
                )
                .ok()
                .and_then(|c| c.to_array::<Value>().ok())
                .is_none_or(|r| r.is_empty())
    }

    /// MEDIUM-5: the era the actor holds against the render's ([`actor_epoch`]): another era's actor is RESET (its
    /// storage and its alarm, inside its turn) before it answers, then stamped; counted `owed_actor_epoch_resets_total`.
    async fn check_epoch(&self) {
        let current = env_epoch(&self.env);
        let held = self.meta(META_EPOCH);
        match epoch_verdict(held.as_deref(), &current) {
            EpochVerdict::Keep => {}
            EpochVerdict::Stamp => self.put_meta(&self.local(), META_EPOCH, &current),
            EpochVerdict::Reset => {
                let _turn = self.turn.lock().await;
                let _ = self.state.storage().delete_all().await;
                let _ = self.state.storage().delete_alarm().await;
                *self.ready.borrow_mut() = false;
                *self.rebuilt.borrow_mut() = false;
                console_log!(
                    "[owed-actor] RESET on an era move: held {}, the render's {current}",
                    held.unwrap_or_default()
                );
                let local = self.local();
                self.put_meta(&local, META_EPOCH, &current);
                if let Ok(record) = crate::routes::D1Sql::of(&self.env) {
                    record.counters(vec![(COUNTERS[12].into(), 1)]).await;
                }
            }
        }
    }

    /// MEDIUM-1: arm the bootstrap of `me` (its first page on the alarm, now) unless it is done or armed. Answers
    /// (armed now, already bootstrapped).
    async fn arm_bootstrap(&self, me: &str) -> (bool, bool) {
        let local = self.local();
        if self.meta(META_BOOTSTRAPPED).is_some() {
            return (false, true);
        }
        if self.meta(META_BOOT_CURSOR).is_some() {
            return (false, false);
        }
        self.put_meta(&local, META_IDENTITY, me);
        self.put_meta(&local, META_BOOT_CURSOR, BOOT_CURSOR_START);
        let _ = self
            .state
            .storage()
            .set_alarm(Date::now().as_millis() as i64)
            .await;
        (true, false)
    }

    /// MEDIUM-4: the tip alarm at the next gate above `tip`, or the next block while a courier-dated sweep is held
    /// ([`next_tip_alarm_ms`]); none owed: `meta.gate` cleared. An alarm already set (an earlier duty) is kept: each
    /// alarm re-arms this one at its end.
    async fn arm_tip_alarm(&self, local: &DoSql, tip: Option<u64>) {
        let row = local
            .sql
            .exec(
                NEXT_GATE_SQL,
                vec![SqlStorageValue::Integer(tip.map_or(-1, |t| t as i64))],
            )
            .ok()
            .and_then(|c| c.to_array::<Value>().ok())
            .and_then(|r| r.into_iter().next())
            .unwrap_or(Value::Null);
        let now = Date::now().as_millis();
        match next_tip_alarm_ms(
            now,
            tip,
            row["gate"].as_u64(),
            row["sweeps"].as_i64() == Some(1),
        ) {
            Some(at) => {
                self.put_meta(local, META_GATE, &at.to_string());
                if self
                    .state
                    .storage()
                    .get_alarm()
                    .await
                    .ok()
                    .flatten()
                    .is_none()
                {
                    let _ = self.state.storage().set_alarm(at as i64).await;
                }
            }
            None => {
                let _ = local.sql.exec("DELETE FROM meta WHERE name='gate'", None);
            }
        }
    }

    /// Phase C (`OWED_D1_JUDGE=false`): the D1 rows are not dated, so the actor publishes its belt questions.
    fn publishes_asks(&self) -> bool {
        !d1_judge_on(self.env.var(D1_JUDGE_FLAG).ok().map(|v| v.to_string()))
    }

    fn meta(&self, name: &str) -> Option<String> {
        let local = self.local();
        local
            .sql
            .exec(META_GET_SQL, vec![SqlStorageValue::from(name.to_string())])
            .ok()
            .and_then(|c| c.to_array::<Value>().ok())
            .and_then(|r| {
                r.first()
                    .and_then(|v| v["value"].as_str().map(str::to_string))
            })
    }

    /// NEW2-L5: the tip window's carried faults (empty when absent or unreadable).
    fn tip_faults(&self) -> BTreeMap<String, u32> {
        self.meta(META_TIP_FAULTS)
            .and_then(|v| serde_json::from_str(&v).ok())
            .unwrap_or_default()
    }

    fn put_meta(&self, local: &DoSql, name: &str, value: &str) {
        let _ = local.sql.exec(
            META_PUT_SQL,
            vec![
                SqlStorageValue::from(name.to_string()),
                SqlStorageValue::from(value.to_string()),
            ],
        );
    }

    /// The games whose rows the tips since `last` have reached (MEDIUM-4, [`GATED_GAMES_SQL`]).
    fn gated_games(&self, local: &DoSql, tip: u64, last: i64) -> Vec<String> {
        local
            .sql
            .exec(
                GATED_GAMES_SQL,
                vec![
                    SqlStorageValue::Integer(tip as i64),
                    SqlStorageValue::Integer(last),
                ],
            )
            .ok()
            .and_then(|c| c.to_array::<Value>().ok())
            .unwrap_or_default()
            .iter()
            .filter_map(|r| r["gameId"].as_str().map(str::to_string))
            .collect()
    }

    async fn apply_route(&self, body: ApplyBody) -> Result<Response> {
        let Some(me) = actor_name(&body.identity) else {
            return Response::error("identity is not a 66-hex key", 400);
        };
        let started = Date::now().as_millis();
        let local = self.local();
        let seq = body.seq.unwrap_or(0);
        let facts: Vec<Stmt> = body
            .txfacts
            .iter()
            .filter_map(|f| txfacts_event_stmt(f, seq))
            .collect();
        if !facts.is_empty() {
            let _ = local.run_batch(facts).await;
        }
        let record = crate::routes::D1Sql::of(&self.env)?;
        let kind_seq = body.kind.as_deref().zip(body.seq);
        let applied = apply_and_publish_in_turn(
            &self.turn,
            &record,
            &local,
            &EnvOutside(&self.env),
            &me,
            &body.games,
            kind_seq,
            body.tip,
            crate::routes::written_off_before_ms_env(&self.env),
            self.publishes_asks(),
        )
        .await;
        let (report, pushes) = match applied {
            Ok(r) => r,
            Err(e) => return Response::error(format!("owed actor apply: {e}"), 503),
        };
        let now = Date::now().as_millis();
        {
            let mut bars = self.bars.borrow_mut();
            bars.apply.note(now.saturating_sub(started));
            if let Some(at) = body.at_ms {
                bars.visible.note(now.saturating_sub(at));
                if body.mined {
                    bars.collect_after_mined.note(now.saturating_sub(at));
                }
            }
        }
        self.put_meta(&local, META_IDENTITY, &me);
        self.put_meta(&local, META_JUDGED_AT, &now.to_string());
        // NEW2-L5: an apply that landed clears its games' carried tip faults (a parked game re-enters the window).
        let mut held = self.tip_faults();
        let before = held.len();
        held.retain(|g, _| !body.games.iter().any(|x| x.eq_ignore_ascii_case(g)));
        if held.len() != before {
            self.put_meta(&local, META_TIP_FAULTS, &json!(held).to_string());
        }
        // THE PUSH (section 4): `owed-changed` through the relay's pots room, carrying the judgement and the event's
        // write time (the harness's bar).
        for (identity, mut push) in pushes {
            push["judgedAtMs"] = json!(now);
            push["atMs"] = json!(body.at_ms);
            crate::push_coalesce::queue_seat_push(&identity, push);
        }
        if crate::push_coalesce::has_queued() {
            self.state
                .wait_until(crate::push_coalesce::flush_queued(self.env.clone()));
        }
        // MEDIUM-9: the KV copy at most once a second; the rest coalesced on the alarm.
        let last = *self.last_kv_ms.borrow();
        if kv_copy_due(last, now) {
            self.write_kv_copy(&local).await;
        } else {
            self.put_meta(&local, META_KV_DIRTY, "1");
            let _ = self
                .state
                .storage()
                .set_alarm(now as i64 + KV_COPY_MIN_INTERVAL_MS as i64)
                .await;
        }
        // MEDIUM-4: the actor's own tip alarm, at the next gate or (a courier-dated sweep held) the next block.
        self.arm_tip_alarm(&local, body.tip).await;
        // MEDIUM-1: an actor's first event bootstraps it (every other game of its identity, paged on its alarm), so
        // Phase A proves the bootstrap before any read is served from it.
        let _ = self.arm_bootstrap(&me).await;
        let mut answer = answer_of(&report, self.rows_of(&local, &me));
        answer["identity"] = json!(me);
        answer["judgedAtMs"] = json!(now);
        answer["atMs"] = json!(body.at_ms);
        answer["mined"] = json!(body.mined);
        answer["applyMs"] = json!(now.saturating_sub(started));
        answer["ackSeq"] = json!(self.meta(META_ACK));
        Response::from_json(&answer)
    }

    fn rows_of(&self, local: &DoSql, me: &str) -> Vec<Value> {
        local
            .sql
            .exec(ROWS_SQL, vec![SqlStorageValue::from(me.to_string())])
            .ok()
            .and_then(|c| c.to_array::<Value>().ok())
            .unwrap_or_default()
    }

    /// `GET /rows?identity=`: the joined rows as the D1 read joins them (`owed_read::ROWS_SQL` over the actor's own
    /// tables; the read replaces every word column with D1's, MEDIUM-4) and the state row. It WRITES NOTHING (the
    /// D2-ACTOR-B lens's MEDIUM-3): an actor not bootstrapped answers `bootstrapped: false` (and whether its bootstrap
    /// is armed); the read's caller arms it by `/bootstrap`.
    async fn rows_route(&self, identity: Option<String>) -> Result<Response> {
        let local = self.local();
        let me = self
            .meta(META_IDENTITY)
            .or(identity.and_then(|i| actor_name(&i)));
        let bootstrapped = self.meta(META_BOOTSTRAPPED).is_some();
        let armed = self.meta(META_BOOT_CURSOR).is_some();
        let read = |sql: &str| -> Vec<Value> {
            me.as_deref()
                .and_then(|m| {
                    local
                        .sql
                        .exec(sql, vec![SqlStorageValue::from(m.to_string())])
                        .ok()
                })
                .and_then(|c| c.to_array::<Value>().ok())
                .unwrap_or_default()
        };
        Response::from_json(&json!({
            "identity": me,
            "fresh": false,
            "bootstrapped": bootstrapped,
            "bootstrapArmed": armed,
            "joined": read(crate::owed_read::ROWS_SQL),
            "state": read(crate::owed_read::STATE_SQL).first(),
            "ackSeq": self.meta(META_ACK).and_then(|s| s.parse::<i64>().ok()),
            "judgedAtMs": self.meta(META_JUDGED_AT).and_then(|s| s.parse::<u64>().ok()),
        }))
    }

    /// MEDIUM-6: one bootstrap page (16 games, newest first) per alarm, resumable on its keyset cursor (LOW-6),
    /// until the page is short: then the actor is bootstrapped and its reads serve its rows, counted durably
    /// (`owed_actor_bootstraps_total`, served `/health.owedActor.bootstraps`, the D2-ACTOR-B lens's MEDIUM-1). The
    /// delta lens's NEW-M2: each page takes a token of the fleet's budget first ([`boot_pace`]), and the pages of one
    /// actor are [`BOOTSTRAP_PAGE_DELAY_MS`] apart.
    async fn bootstrap_page(&self, local: &DoSql) -> Result<()> {
        let (Some(me), Some(cursor)) = (self.meta(META_IDENTITY), self.meta(META_BOOT_CURSOR))
        else {
            return Ok(());
        };
        let (at, after) = boot_keyset(Some(&cursor));
        let record = crate::routes::D1Sql::of(&self.env)?;
        // NEW-M2: one token of the fleet's bootstrap budget, or a wait for a slot in a later minute.
        let now = Date::now().as_millis();
        // NEW2-L3: one statement; a full minute writes nothing and answers no row (the page waits).
        let taken = boot_taken(
            &record
                .run_batch(vec![Stmt::new(
                    BOOT_BUDGET_TAKE_SQL,
                    vec![Bind::Int(now as i64)],
                )])
                .await
                .map_err(Error::RustError)?
                .into_iter()
                .next()
                .map(|o| o.rows)
                .unwrap_or_default(),
        );
        if let Some(wait) = boot_pace(taken, now, &me) {
            self.state.storage().set_alarm(wait as i64).await?;
            return Ok(());
        }
        let page = record
            .run_batch(vec![Stmt::new(
                crate::owed_judge::IDENTITY_GAMES_AFTER_SQL,
                vec![Bind::Text(me.clone()), Bind::Int(at), Bind::Text(after)],
            )])
            .await
            .map_err(Error::RustError)?;
        let rows: Vec<Value> = page.into_iter().next().map(|o| o.rows).unwrap_or_default();
        let games: Vec<String> = rows
            .iter()
            .filter_map(|r| r["gameId"].as_str().map(str::to_string))
            .collect();
        if !games.is_empty() {
            let tip = crate::owed_events::tip_memoised(&self.env, false).await;
            apply_and_publish_in_turn(
                &self.turn,
                &record,
                local,
                &EnvOutside(&self.env),
                &me,
                &games,
                None,
                tip,
                crate::routes::written_off_before_ms_env(&self.env),
                self.publishes_asks(),
            )
            .await
            .map_err(Error::RustError)?;
        }
        if games.len() < 16 {
            let now = Date::now().as_millis();
            self.put_meta(local, META_BOOTSTRAPPED, &now.to_string());
            self.put_meta(local, META_JUDGED_AT, &now.to_string());
            let _ = local
                .sql
                .exec("DELETE FROM meta WHERE name='bootCursor'", None);
            self.bars.borrow_mut().bootstraps += 1;
            record.counters(vec![(COUNTERS[10].into(), 1)]).await;
            self.put_meta(local, META_KV_DIRTY, "1");
            self.write_kv_copy(local).await;
        } else {
            let last = rows.last().cloned().unwrap_or(Value::Null);
            self.put_meta(
                local,
                META_BOOT_CURSOR,
                &json!([last["at"].as_i64().unwrap_or(0), last["gameId"]]).to_string(),
            );
            self.state
                .storage()
                .set_alarm((Date::now().as_millis() + BOOTSTRAP_PAGE_DELAY_MS) as i64)
                .await?;
        }
        Ok(())
    }

    async fn write_kv_copy(&self, local: &DoSql) {
        let Some(me) = self.meta(META_IDENTITY) else {
            return;
        };
        let Ok(kv) = self.env.kv(KV_BINDING) else {
            return;
        };
        let now = Date::now().as_millis();
        let mut body = kv_copy_body(&me, &self.rows_of(local, &me), now);
        // NEW-L2: the copy names its era; a read of another era serves none of it.
        body["epoch"] = json!(env_epoch(&self.env));
        let put = kv.put(&me, body.to_string());
        if let Ok(put) = put {
            if put.execute().await.is_ok() {
                *self.last_kv_ms.borrow_mut() = Some(now);
                self.bars.borrow_mut().kv_copies += 1;
                let _ = local
                    .sql
                    .exec("DELETE FROM meta WHERE name='kvDirty'", None);
            }
        }
    }
}

/// The render's era ([`actor_epoch`] of `STORAGE_EPOCH` and `WRITTEN_OFF_BEFORE_MS`).
pub fn env_epoch(env: &Env) -> String {
    actor_epoch(
        env.var("STORAGE_EPOCH").ok().map(|v| v.to_string()),
        crate::routes::written_off_before_ms_env(env),
    )
}

/// A stub of the identity's actor, `None` without the binding or for a key that is not 66-hex.
pub fn stub_of(env: &Env, identity: &str) -> Option<Stub> {
    let name = actor_name(identity)?;
    env.durable_object(BINDING)
        .ok()?
        .id_from_name(&name)
        .ok()?
        .get_stub()
        .ok()
}

/// One actor call (the consumer's delivery, a writer's direct call), bounded by `wall_ms`.
pub async fn call_apply(
    env: &Env,
    body: &Value,
    wall_ms: u64,
) -> std::result::Result<Value, String> {
    let identity = body["identity"].as_str().unwrap_or_default();
    let stub = stub_of(env, identity).ok_or("no owed actor for this identity")?;
    let mut init = RequestInit::new();
    init.with_method(Method::Post)
        .with_body(Some(body.to_string().into()));
    let req =
        Request::new_with_init("https://owed-actor/apply", &init).map_err(|e| e.to_string())?;
    let call = async {
        let mut resp = stub
            .fetch_with_request(req)
            .await
            .map_err(|e| e.to_string())?;
        if resp.status_code() != 200 {
            return Err(format!(
                "owed actor answered {}: {}",
                resp.status_code(),
                resp.text().await.unwrap_or_default()
            ));
        }
        resp.json::<Value>().await.map_err(|e| e.to_string())
    };
    let wall = Delay::from(std::time::Duration::from_millis(wall_ms));
    futures_util::pin_mut!(call);
    futures_util::pin_mut!(wall);
    match futures_util::future::select(call, wall).await {
        futures_util::future::Either::Left((r, _)) => r,
        futures_util::future::Either::Right(_) => {
            Err(format!("owed actor: no answer in {wall_ms} ms"))
        }
    }
}

/// The read's call (`GET /rows`), bounded.
pub async fn call_rows(
    env: &Env,
    identity: &str,
    wall_ms: u64,
) -> std::result::Result<Value, String> {
    let stub = stub_of(env, identity).ok_or("no owed actor for this identity")?;
    let name = actor_name(identity).unwrap_or_default();
    let call = async {
        let mut resp = stub
            .fetch_with_str(&format!("https://owed-actor/rows?identity={name}"))
            .await
            .map_err(|e| e.to_string())?;
        resp.json::<Value>().await.map_err(|e| e.to_string())
    };
    let wall = Delay::from(std::time::Duration::from_millis(wall_ms));
    futures_util::pin_mut!(call);
    futures_util::pin_mut!(wall);
    match futures_util::future::select(call, wall).await {
        futures_util::future::Either::Left((r, _)) => r,
        futures_util::future::Either::Right(_) => {
            Err(format!("owed actor: no answer in {wall_ms} ms"))
        }
    }
}

/// The reconcile's call (`GET /stored`), bounded: the actor's stored rows.
pub async fn call_stored(
    env: &Env,
    identity: &str,
    wall_ms: u64,
) -> std::result::Result<Vec<Value>, String> {
    let stub = stub_of(env, identity).ok_or("no owed actor for this identity")?;
    let name = actor_name(identity).unwrap_or_default();
    let call = async {
        let mut resp = stub
            .fetch_with_str(&format!("https://owed-actor/stored?identity={name}"))
            .await
            .map_err(|e| e.to_string())?;
        if resp.status_code() != 200 {
            return Err(format!("owed actor answered {}", resp.status_code()));
        }
        let v = resp.json::<Value>().await.map_err(|e| e.to_string())?;
        Ok(v["rows"].as_array().cloned().unwrap_or_default())
    };
    let wall = Delay::from(std::time::Duration::from_millis(wall_ms));
    futures_util::pin_mut!(call);
    futures_util::pin_mut!(wall);
    match futures_util::future::select(call, wall).await {
        futures_util::future::Either::Left((r, _)) => r,
        futures_util::future::Either::Right(_) => {
            Err(format!("owed actor: no answer in {wall_ms} ms"))
        }
    }
}

/// The display copy the actor wrote (MEDIUM-9), read when the actor cannot be reached.
pub async fn read_kv_copy(env: &Env, identity: &str) -> Option<Value> {
    let kv = env.kv(KV_BINDING).ok()?;
    let id = actor_name(identity)?;
    kv_copy_of_era(
        kv.get(&id).json::<Value>().await.ok().flatten(),
        &env_epoch(env),
    )
}

/// The read's wall on the actor (the KV copy past it).
pub const READ_WALL_MS: u64 = 1_500;

/// THE AMENDED BAR (item 4): after a writer's own durable answer, its filings' identities' actors are called
/// directly (bounded, `wait_until`); the queue message stands as the durable backstop. Counted
/// `owed_actor_direct_calls_total` and `owed_actor_direct_faults_total`; the rows' visibility is measured from
/// `at_ms`, the write.
pub fn schedule_direct(
    ctx: &RouteContext<crate::auth::AuthState>,
    filings: Vec<crate::record_post::VerifiedRecord>,
    at_ms: u64,
) {
    let Some(wait) = &ctx.data.wait else { return };
    if ctx.env.durable_object(BINDING).is_err() {
        return;
    }
    let env = ctx.env.clone();
    wait.wait_until(async move {
        let Ok(db) = crate::routes::D1Sql::of(&env) else {
            return;
        };
        let mut by: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for f in &filings {
            if let Ok(subjects) = crate::record_post::owed_write::subjects(&db, f).await {
                for s in subjects {
                    by.entry(s.identity).or_default().push(s.game_id);
                }
            }
        }
        let tip = crate::owed_events::tip_memoised(&env, false).await;
        let (mut calls, mut faults) = (0u64, 0u64);
        for (identity, games) in by {
            let e = EventCtx {
                kind: "direct".into(),
                seq: 0,
                at_ms: at_ms as i64,
                mined: false,
            };
            // LOW-7: an apply takes at most `GAMES_PER_APPLY` games; a bigger batch is split here.
            for chunk in games.chunks(GAMES_PER_APPLY) {
                let mut body = apply_body(&identity, chunk, &e, tip);
                body["seq"] = Value::Null;
                calls += 1;
                match call_apply(&env, &body, DIRECT_CALL_WALL_MS).await {
                    Ok(answer) => note_answer_as("direct", &answer),
                    Err(e) => {
                        faults += 1;
                        console_log!("[owed-actor] direct call {identity}: {e}");
                    }
                }
            }
        }
        db.counters(vec![
            (COUNTERS[3].into(), calls),
            (COUNTERS[4].into(), faults),
        ])
        .await;
        // MEDIUM-7: the direct class's samples into the fleet's durable bars.
        flush_bars(&db, Date::now().as_millis(), false).await;
    });
}

/// The answer of Phase B's read.
#[derive(Debug)]
pub(crate) enum ActorRead {
    /// The rows to serve through `Snapshot::body` (the actor's, or D1's own for an identity D1 does not know).
    Rows(crate::owed_read::Snapshot),
    /// The actor's rows are not served as complete (out of reach, or its bootstrap unfinished).
    Fallback(Fallback),
}

/// MEDIUM-3: does D1 know the identity (a stated owed list, or a party, hop marker or evicted party row: the
/// existing indexed probe `owed_read::read_with` asks of an unstated one)? The delta lens's NEW-M1: the probe's
/// answer (`known`) is the gate, never a wanted mark: in Phase C nothing writes D1's `owed_state`, and a mark per read
/// re-judged a new player's whole identity on every sweep tick.
pub fn known_to_d1(snapshot: &crate::owed_read::Snapshot) -> bool {
    snapshot.state.is_some() || snapshot.wanted || snapshot.known
}

/// Phase B's read (`OWED_ACTOR_READ`). FIRST D1's state and pending marks (LOW-5: never D1's rows): an identity D1
/// does not know is answered from D1 as before, no actor called and none minted (MEDIUM-3, counted
/// `owed_actor_unknown_reads_total`). Then the actor's rows joined with D1's words, D1's pending marks kept. An actor
/// whose bootstrap is unfinished is served its KV copy display only, or a 503 `pending` with no copy, and its
/// bootstrap armed when it is not (MEDIUM-1); an actor out of reach the same (MEDIUM-2; Rule 23: never `rows: []` by
/// silence). `Err` is the stored read's fault (503).
pub(crate) async fn read_snapshot(
    env: &Env,
    db: &crate::routes::D1Sql,
    identity: &str,
) -> std::result::Result<ActorRead, String> {
    let mut snapshot = crate::owed_read::read_with(db, identity, false).await?;
    if !known_to_d1(&snapshot) {
        db.counters(vec![(COUNTERS[11].into(), 1)]).await;
        return Ok(ActorRead::Rows(snapshot));
    }
    let answer = match call_rows(env, identity, READ_WALL_MS).await {
        Ok(a) => a,
        Err(e) => {
            console_log!("[owed-actor] read {identity}: {e}");
            let copy = read_kv_copy(env, identity).await;
            return Ok(ActorRead::Fallback(fallback_body(
                identity,
                copy.as_ref(),
                KV_COPY_REASON,
                "kv-copy",
            )));
        }
    };
    if answer["bootstrapped"] != true {
        if answer["bootstrapArmed"] != true {
            if let Err(e) = call_bootstrap(env, identity, READ_WALL_MS).await {
                console_log!("[owed-actor] bootstrap {identity}: {e}");
            }
        }
        let copy = read_kv_copy(env, identity).await;
        return Ok(ActorRead::Fallback(fallback_body(
            identity,
            copy.as_ref(),
            BOOTSTRAP_REASON,
            "owed-actor-bootstrap",
        )));
    }
    let mut joined: Vec<Value> = answer["joined"].as_array().cloned().unwrap_or_default();
    let txids = word_txids(&joined);
    let words = if txids.is_empty() {
        Vec::new()
    } else {
        db.run_batch(vec![Stmt::new(
            READ_WORDS_SQL,
            vec![Bind::Text(json!(txids).to_string())],
        )])
        .await?
        .into_iter()
        .next()
        .map(|o| o.rows)
        .unwrap_or_default()
    };
    join_words(&mut joined, &words);
    snapshot.joined = joined;
    snapshot.state = answer["state"]
        .as_object()
        .map(|o| Value::Object(o.clone()));
    Ok(ActorRead::Rows(snapshot))
}

/// MEDIUM-1: arm one identity's bootstrap (`/bootstrap`), bounded.
pub async fn call_bootstrap(
    env: &Env,
    identity: &str,
    wall_ms: u64,
) -> std::result::Result<Value, String> {
    let stub = stub_of(env, identity).ok_or("no owed actor for this identity")?;
    let name = actor_name(identity).unwrap_or_default();
    let mut init = RequestInit::new();
    init.with_method(Method::Post)
        .with_body(Some(json!({ "identity": name }).to_string().into()));
    let req =
        Request::new_with_init("https://owed-actor/bootstrap", &init).map_err(|e| e.to_string())?;
    let call = async {
        let mut resp = stub
            .fetch_with_request(req)
            .await
            .map_err(|e| e.to_string())?;
        if resp.status_code() != 200 {
            return Err(format!("owed actor answered {}", resp.status_code()));
        }
        resp.json::<Value>().await.map_err(|e| e.to_string())
    };
    let wall = Delay::from(std::time::Duration::from_millis(wall_ms));
    futures_util::pin_mut!(call);
    futures_util::pin_mut!(wall);
    match futures_util::future::select(call, wall).await {
        futures_util::future::Either::Left((r, _)) => r,
        futures_util::future::Either::Right(_) => {
            Err(format!("owed actor: no answer in {wall_ms} ms"))
        }
    }
}

/// THE PHASE A BOOTSTRAP LEVER (the D2-ACTOR-B lens's MEDIUM-1): `POST /internal/owed-actor-bootstrap` (bearer
/// `INTERNAL_TOKEN`), body empty, `{}` or `{"after": "<identity>"}`: every identity D1 knows ([`RESET_IDENTITIES_SQL`]'s
/// union), [`BOOTSTRAP_PER_CALL`] a call past `after`, each actor's `/bootstrap` (armed now, already bootstrapped, or
/// a fault), `next` the cursor (`null` at the end). Run it in Phase A until `next` is null, then run a SECOND full pass
/// and read its `coverage` (also `/health.owedActor.coverage`): Phase B's gate is that pass `complete`, `contiguous`
/// and `uncovered` 0 (the delta lens's NEW-L3; `bootstraps` against `activeIdentities` is a hint only).
pub const BOOTSTRAP_PER_CALL: usize = 50;
pub async fn bootstrap_lever(mut req: Request, env: &Env) -> Result<Response> {
    if !crate::internal_events::internal_bearer_ok(&req, env) {
        return Response::error("unauthorized", 401);
    }
    let text = req.text().await.unwrap_or_default();
    let body: Value = if text.trim().is_empty() {
        json!({})
    } else {
        match serde_json::from_str(&text) {
            Ok(b @ Value::Object(_)) => b,
            _ => {
                return Response::error("the body is empty, {} or {\"after\": \"<identity>\"}", 400)
            }
        }
    };
    let after = body["after"].as_str().unwrap_or_default().to_string();
    let db = crate::routes::D1Sql::of(env)?;
    let rows = db
        .run_batch(vec![Stmt::new(
            RESET_IDENTITIES_SQL,
            vec![
                Bind::Text(after.clone()),
                Bind::Int(BOOTSTRAP_PER_CALL as i64),
            ],
        )])
        .await
        .map_err(Error::RustError)?
        .into_iter()
        .next()
        .map(|o| o.rows)
        .unwrap_or_default();
    let ids: Vec<String> = rows
        .iter()
        .filter_map(|r| r["identity"].as_str().map(str::to_string))
        .collect();
    let (mut armed, mut done, mut skipped, mut faults) = (0u64, 0u64, Vec::new(), Vec::new());
    for id in &ids {
        if actor_name(id).is_none() {
            skipped.push(id.clone());
            continue;
        }
        match call_bootstrap(env, id, DIRECT_CALL_WALL_MS).await {
            Ok(a) if a["armed"] == true => armed += 1,
            Ok(a) if a["bootstrapped"] == true => done += 1,
            Ok(_) => {}
            Err(e) => faults.push(json!({"identity": id, "error": e})),
        }
    }
    let next = (ids.len() == BOOTSTRAP_PER_CALL)
        .then(|| ids.last().cloned())
        .flatten();
    // NEW-L3: the pass's coverage, folded into its ledger row (an identity is covered only when it answered
    // `bootstrapped`; a key that is not 66-hex has no actor and is counted apart).
    let uncovered = ids.len() - skipped.len() - done as usize;
    let prev: Option<Value> = db
        .run_batch(vec![Stmt::new(COVERAGE_READ_SQL, vec![])])
        .await
        .ok()
        .and_then(|o| o.into_iter().next())
        .and_then(|o| o.rows.into_iter().next())
        .and_then(|r| {
            r["cursor"]
                .as_str()
                .and_then(|s| serde_json::from_str(s).ok())
        });
    let coverage = coverage_fold(
        prev.as_ref(),
        &after,
        ids.len() - skipped.len(),
        uncovered,
        next.as_deref(),
        Date::now().as_millis(),
    );
    let _ = db
        .run_batch(vec![Stmt::new(
            crate::owed_belt::STATE_SQL,
            vec![
                Bind::Text(COVERAGE_NAME.into()),
                Bind::Text(coverage.to_string()),
                Bind::Int(Date::now().as_millis() as i64),
            ],
        )])
        .await;
    Response::from_json(
        &json!({"identities": ids.len(), "armed": armed, "bootstrapped": done, "skipped": skipped, "faults": faults, "next": next, "coverage": coverage}),
    )
}

/// THE WIPE'S LEVER (LOW-2; WIPE-REHEARSAL-3 section 2, step 4a), part 1: `GET /internal/owed-actor-identities?after=`
/// (bearer `INTERNAL_TOKEN`): the identities an actor may exist for, read from D1 (every identity of a party, a hop
/// marker, a released hop's evidence or a stored owed state: each actor is minted by an event, a direct call or a read
/// of one of them), [`RESET_IDENTITIES_PAGE`] a page in key order past `after`; `next` is the cursor, `null` at the end.
/// Read it WHOLE before D1 is wiped.
pub const RESET_IDENTITIES_PAGE: usize = 500;
pub const RESET_IDENTITIES_SQL: &str = "SELECT identity FROM (SELECT identity FROM owed_state UNION SELECT identity FROM potparty_records UNION SELECT identity FROM hopparty_records UNION SELECT identity FROM released_hop_evidence) WHERE identity>?1 ORDER BY identity LIMIT ?2";
/// Part 2: `POST /internal/owed-actor-reset {"identities": [...]}` (bearer `INTERNAL_TOKEN`), 1 to
/// [`RESET_PER_CALL`] keys a call (more is a 400): each identity's actor `/reset` (`deleteAll`, its alarm deleted),
/// answered per key (`reset`, `skipped` for a key that is not 66-hex, `faults`).
pub const RESET_PER_CALL: usize = 50;

pub async fn identities_lever(req: Request, env: &Env) -> Result<Response> {
    if !crate::internal_events::internal_bearer_ok(&req, env) {
        return Response::error("unauthorized", 401);
    }
    let after = req
        .url()?
        .query_pairs()
        .find(|(k, _)| k == "after")
        .map(|(_, v)| v.to_string())
        .unwrap_or_default();
    let db = crate::routes::D1Sql::of(env)?;
    let rows = db
        .run_batch(vec![Stmt::new(
            RESET_IDENTITIES_SQL,
            vec![Bind::Text(after), Bind::Int(RESET_IDENTITIES_PAGE as i64)],
        )])
        .await
        .map_err(Error::RustError)?
        .into_iter()
        .next()
        .map(|o| o.rows)
        .unwrap_or_default();
    let ids: Vec<String> = rows
        .iter()
        .filter_map(|r| r["identity"].as_str().map(str::to_string))
        .collect();
    let next = (ids.len() == RESET_IDENTITIES_PAGE)
        .then(|| ids.last().cloned())
        .flatten();
    Response::from_json(&json!({"identities": ids, "next": next}))
}

/// The reset lever's plan: the keys it resets (lowercased, 66-hex, deduplicated) and those it skips; `Err` past the
/// bound or with no key.
pub fn reset_plan(body: &Value) -> std::result::Result<(Vec<String>, Vec<String>), String> {
    let keys = body["identities"]
        .as_array()
        .ok_or("the body names no identities")?;
    if keys.is_empty() || keys.len() > RESET_PER_CALL {
        return Err(format!("1 to {RESET_PER_CALL} identities a call"));
    }
    let (mut reset, mut skipped) = (BTreeSet::new(), Vec::new());
    for k in keys {
        match k.as_str().and_then(actor_name) {
            Some(n) => {
                reset.insert(n);
            }
            None => skipped.push(k.to_string()),
        }
    }
    Ok((reset.into_iter().collect(), skipped))
}

pub async fn reset_lever(mut req: Request, env: &Env) -> Result<Response> {
    if !crate::internal_events::internal_bearer_ok(&req, env) {
        return Response::error("unauthorized", 401);
    }
    let body: Value = match req.json().await {
        Ok(b) => b,
        Err(e) => return Response::error(format!("bad reset body: {e}"), 400),
    };
    let (keys, skipped) = match reset_plan(&body) {
        Ok(p) => p,
        Err(e) => return Response::error(e, 400),
    };
    let (mut reset, mut faults) = (Vec::new(), Vec::new());
    for k in keys {
        let Some(stub) = stub_of(env, &k) else {
            faults.push(json!({"identity": k, "error": "no owed actor binding"}));
            continue;
        };
        let mut init = RequestInit::new();
        init.with_method(Method::Post);
        let answer = match Request::new_with_init("https://owed-actor/reset", &init) {
            Ok(r) => stub.fetch_with_request(r).await,
            Err(e) => Err(e),
        };
        match answer {
            Ok(resp) if resp.status_code() == 200 => reset.push(k),
            Ok(resp) => faults.push(json!({"identity": k, "error": resp.status_code()})),
            Err(e) => faults.push(json!({"identity": k, "error": e.to_string()})),
        }
    }
    Response::from_json(&json!({"reset": reset, "skipped": skipped, "faults": faults}))
}
