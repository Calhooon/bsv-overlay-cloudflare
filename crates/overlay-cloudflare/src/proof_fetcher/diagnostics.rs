use super::*;

/// The proof ladder uses these rungs. BananaBlocks supplies raw transactions
/// and spend hints, and has no BUMP endpoint in this implementation.
pub(crate) const PROOF_RUNGS: [&str; 3] = ["arcade", "bitails", "woc"];

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProofRequest {
    pub url: String,
    pub http_status: Option<u16>,
    pub parse_result: &'static str,
    #[serde(skip)]
    observation: Option<CourierAttempt>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProofProbe {
    #[serde(skip)]
    pub(super) counters: Option<CourierCounters>,
    pub rung: &'static str,
    pub requests: Vec<ProofRequest>,
    pub parse_result: &'static str,
    pub judge: &'static str,
    #[serde(skip)]
    pub step: Option<RungStep>,
    #[serde(skip)]
    pub bump: Option<String>,
    #[serde(skip)]
    pub unmined: bool,
    #[serde(skip)]
    pub fault: bool,
}

impl ProofProbe {
    pub(crate) fn new(rung: &'static str) -> Self {
        Self {
            rung,
            counters: None,
            requests: Vec::new(),
            parse_result: "disabled",
            judge: "not-run",
            step: None,
            bump: None,
            unmined: false,
            fault: false,
        }
    }

    async fn get(&mut self, url: String, header: Option<(&str, &str)>) -> Option<String> {
        // Never return credentials or query values in diagnostics.
        let safe_url = worker::Url::parse(&url)
            .ok()
            .map(|mut u| {
                let _ = u.set_username("");
                let _ = u.set_password(None);
                u.set_query(None);
                u.set_fragment(None);
                u.to_string()
            })
            .unwrap_or_else(|| "invalid-url".into());
        self.requests.push(ProofRequest {
            url: safe_url,
            http_status: None,
            parse_result: "pending",
            observation: None,
        });
        let (response, observation) =
            http_get_counted(&url, header, self.counters.as_ref(), self.rung).await;
        let (status, body) = match response {
            Ok((status, body)) => (Some(status), Some(body)),
            Err(_) => (None, None),
        };
        let ok = status.is_some_and(|s| (200..300).contains(&s));
        if let Some(request) = self.requests.last_mut() {
            request.http_status = status;
            request.parse_result = if ok { "pending" } else { "not-run" };
            request.observation = observation;
            CourierAttempt::set(
                &mut request.observation,
                status.map_or(CourierOutcome::Fault, |s| {
                    CourierOutcome::response(s, true, true)
                }),
            );
        }
        if !ok {
            // The legacy tally, the `[proof]` line and the probe's `parseResult`
            // keep a 404/410 a fault, as on main b4ae1c6 (the design's baseline
            // rows); only the per-reader family above records it `notfound`.
            self.parse_result = if status.is_some() {
                "http-error"
            } else {
                "transport-error"
            };
            self.fault = true;
            return None;
        }
        body
    }

    fn parsed(&mut self, result: &'static str, fault: bool) {
        self.parse_result = result;
        self.fault |= fault;
        if let Some(request) = self.requests.last_mut() {
            request.parse_result = result;
            let outcome = if fault {
                CourierOutcome::Shape
            } else if matches!(result, "no-proof" | "unmined-hint") {
                CourierOutcome::NotFound
            } else {
                CourierOutcome::Ok
            };
            CourierAttempt::set(&mut request.observation, outcome);
        }
    }
}

impl ChainProofFetcher {
    /// Read one rung and judge its candidate. No store, memo, counter or event
    /// is changed here, so the admin probe exercises the production parser.
    #[cfg(test)]
    pub(crate) async fn diagnose_rung(&self, rung: &'static str, txid: &str) -> ProofProbe {
        let mut probe = ProofProbe::new(rung);
        self.diagnose_rung_into(&mut probe, txid).await;
        probe
    }

    /// Keep partial evidence with the caller if its deadline cancels this read.
    pub(crate) async fn diagnose_rung_into(&self, probe: &mut ProofProbe, txid: &str) {
        let rung = probe.rung;
        let (base, header) = match rung {
            "arcade" => (&self.arcade_url, None),
            "bitails" => (&self.bitails_base, None),
            "woc" => (
                &self.woc_base,
                self.woc_api_key.as_deref().map(|k| ("woc-api-key", k)),
            ),
            _ => return,
        };
        if base.is_empty() {
            return;
        }
        let bump = if rung == "arcade" {
            let Some(body) = probe.get(format!("{base}/tx/{txid}"), header).await else {
                return;
            };
            let Ok(value) = serde_json::from_str::<serde_json::Value>(&body) else {
                probe.parsed("invalid-json", true);
                return;
            };
            if value
                .get("txStatus")
                .and_then(serde_json::Value::as_str)
                .is_none()
            {
                probe.parsed("missing-status", true);
                return;
            }
            #[cfg(all(test, not(target_arch = "wasm32")))]
            let now = 0;
            #[cfg(not(all(test, not(target_arch = "wasm32"))))]
            let now = worker::Date::now().as_millis() as i64;
            match parse_arcade_proof_look(200, &body, now) {
                ArcadeProofLook::Mined(bump) => bump,
                ArcadeProofLook::KnownUnmined(_) => {
                    probe.parsed("unmined-hint", false);
                    probe.unmined = true;
                    return;
                }
                ArcadeProofLook::Unknown => {
                    let mined = value
                        .get("txStatus")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|s| {
                            crate::door_words::proof_class(
                                crate::door_words::arcade_hint(s, "", 0).as_ref(),
                            ) == crate::door_words::ProofClass::Mined
                        });
                    probe.parsed(if mined { "missing-bump" } else { "no-proof" }, mined);
                    return;
                }
            }
        } else {
            let path = if rung == "woc" { "tx/hash" } else { "tx" };
            let Some(body) = probe.get(format!("{base}/{path}/{txid}"), header).await else {
                return;
            };
            let height = serde_json::from_str::<serde_json::Value>(&body)
                .ok()
                .and_then(|v| {
                    v.get("blockheight")
                        .or_else(|| v.get("blockHeight"))
                        .and_then(serde_json::Value::as_u64)
                })
                .and_then(|h| u32::try_from(h).ok());
            let Some(height) = height else {
                probe.parsed("invalid-height", true);
                return;
            };
            if height == 0 {
                probe.parsed("unmined-hint", false);
                return;
            }
            probe.parsed("height", false);
            let Some(body) = probe
                .get(format!("{base}/tx/{txid}/proof/tsc"), header)
                .await
            else {
                return;
            };
            let Some(bump) = tsc_body_to_bump_hex(&body, height) else {
                probe.parsed("invalid-tsc", true);
                return;
            };
            bump
        };
        probe.parsed("bump", false);
        let verdict = verify_bump_outcome(self.tracker.as_deref(), &bump, txid).await;
        probe.step = Some(rung_step(&verdict));
        probe.judge = match verdict {
            BumpVerdict::Valid => {
                probe.bump = Some(bump);
                "valid"
            }
            BumpVerdict::Invalid => "rejected",
            BumpVerdict::NoHeader { .. } => "header-unavailable",
            BumpVerdict::Fault(_) => "header-fault",
        };
        probe.fault = probe.judge != "valid";
        if probe.judge == "rejected" {
            if let Some(request) = probe.requests.last_mut() {
                CourierAttempt::set(&mut request.observation, CourierOutcome::Shape);
            }
        }
    }

    pub(super) async fn note_proof_probe(&self, probe: &ProofProbe) {
        if probe.parse_result == "disabled" {
            self.proof_tallies
                .borrow_mut()
                .entry(probe.rung)
                .or_default()
                .skipped += 1;
            return;
        }
        if probe.fault {
            self.proof_tallies
                .borrow_mut()
                .entry(probe.rung)
                .or_default()
                .fault += 1;
            if self.proof_fault_logged.borrow_mut().insert(probe.rung) {
                let status = probe.requests.last().and_then(|r| r.http_status);
                push_log(&format!(
                    "[proof] rung={} status={status:?} parse={} judge={}",
                    probe.rung, probe.parse_result, probe.judge
                ));
            }
            if let Some(db) = &self.proof_fault_store {
                crate::ops::bump_counter(
                    db,
                    &crate::ops::courier_counter_name(probe.rung, "fault"),
                    1,
                )
                .await;
            }
        } else {
            self.proof_tallies
                .borrow_mut()
                .entry(probe.rung)
                .or_default()
                .ok += 1;
        }
    }
}
