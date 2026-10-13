//! A singleton alarm that can keep the tick alive when cron delivery stops.

use serde::{Deserialize, Serialize};
use worker::{durable_object, wasm_bindgen, DurableObject, Env, Method, Request, Response, State};

use crate::tick::{now_ms, run_tick, TickSource};

pub(crate) const TICK_INTERVAL_MS: i64 = 900_000;
const BINDING: &str = "TICK_ALARM";
const INSTANCE: &str = "tick";

pub(crate) fn next_alarm_at(now_ms: i64, last_ms: Option<i64>) -> i64 {
    now_ms
        .max(last_ms.unwrap_or(now_ms))
        .saturating_add(TICK_INTERVAL_MS)
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AlarmStatus {
    last_source: Option<String>,
    last_at_ms: Option<i64>,
    pub alarm_armed: bool,
    pub next_alarm_ms: Option<i64>,
}

#[durable_object]
pub struct TickAlarm {
    state: State,
    env: Env,
}

impl TickAlarm {
    async fn arm_at(&self, at_ms: i64) -> worker::Result<()> {
        // The SDK's i64 overload is a relative delay. Use an explicit Date
        // for the absolute timestamp returned by next_alarm_at.
        self.state
            .storage()
            .set_alarm(worker::ScheduledTime::new(js_sys::Date::new(
                &worker::wasm_bindgen::JsValue::from_f64(at_ms as f64),
            )))
            .await
    }

    async fn status(&self) -> worker::Result<AlarmStatus> {
        let next_alarm_ms = self.state.storage().get_alarm().await?;
        let db = self.env.d1("OVERLAY_DB")?;
        let last = crate::tick_ops::last_tick(&db)
            .await
            .map_err(worker::Error::RustError)?;
        Ok(AlarmStatus {
            last_source: last.tick_source,
            last_at_ms: last.tick_at_ms.map(|ms| ms as i64),
            alarm_armed: next_alarm_ms.is_some(),
            next_alarm_ms,
        })
    }
}

impl DurableObject for TickAlarm {
    fn new(state: State, env: Env) -> Self {
        Self { state, env }
    }

    async fn fetch(&self, req: Request) -> worker::Result<Response> {
        match (req.method(), req.path().as_str()) {
            (Method::Post, "/arm") => {
                if self.state.storage().get_alarm().await?.is_none() {
                    self.arm_at(next_alarm_at(now_ms(), None)).await?;
                }
                Response::from_json(&self.status().await?)
            }
            (Method::Get, "/status") => Response::from_json(&self.status().await?),
            _ => Response::error("Not found", 404),
        }
    }

    async fn alarm(&self) -> worker::Result<Response> {
        let report = run_and_rearm(
            |at| self.arm_at(at),
            run_tick(&self.env, TickSource::Alarm),
            now_ms,
        )
        .await?;
        Response::from_json(&report)
    }
}

async fn run_and_rearm<T, E, A, AF, TF>(mut arm: A, tick: TF, now: impl Fn() -> i64) -> Result<T, E>
where
    A: FnMut(i64) -> AF,
    AF: std::future::Future<Output = Result<(), E>>,
    TF: std::future::Future<Output = T>,
{
    // Persist a successor before doing work as well as on return. A
    // failed tick, including an isolate interruption, leaves a wakeup.
    let started = now();
    arm(next_alarm_at(started, None)).await?;
    let report = tick.await;
    arm(next_alarm_at(now(), Some(started))).await?;
    Ok(report)
}

async fn with_optional_binding<B, T, E, F, Fut>(
    binding: Result<B, E>,
    send: F,
) -> Result<Option<T>, E>
where
    F: FnOnce(B) -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    match binding {
        Ok(binding) => send(binding).await.map(Some),
        Err(_) => Ok(None),
    }
}

async fn call(env: &Env, arm: bool) -> worker::Result<Option<AlarmStatus>> {
    let request = with_optional_binding(env.durable_object(BINDING), |namespace| async move {
        let stub = namespace.id_from_name(INSTANCE)?.get_stub()?;
        let (url, method) = if arm {
            ("https://tick.invalid/arm", Method::Post)
        } else {
            ("https://tick.invalid/status", Method::Get)
        };
        let mut response = stub.fetch_with_request(Request::new(url, method)?).await?;
        if response.status_code() != 200 {
            return Err(worker::Error::RustError(format!(
                "tick alarm returned {}",
                response.status_code()
            )));
        }
        response.json::<AlarmStatus>().await
    });
    crate::race_or_deadline(request, crate::broadcaster::sleep_ms(5_000))
        .await
        .ok_or_else(|| {
            worker::Error::RustError("tick alarm control request timed out".to_string())
        })?
}

pub(crate) async fn arm_from_cron(env: &Env) {
    match call(env, true).await {
        Ok(Some(status)) => worker::console_log!("tick alarm armed for {:?}", status.next_alarm_ms),
        Ok(None) => worker::console_log!("tick alarm arm skipped: missing TICK_ALARM binding"),
        Err(e) => worker::console_log!("tick alarm arm failed: {e}"),
    }
}

pub(crate) async fn admin_arm(env: &Env) -> worker::Result<Response> {
    admin_response(call(env, true).await)
}

pub(crate) async fn admin_status(env: &Env) -> worker::Result<Response> {
    admin_response(call(env, false).await)
}

fn admin_response(result: worker::Result<Option<AlarmStatus>>) -> worker::Result<Response> {
    match result {
        Ok(Some(status)) => Response::from_json(&status),
        Ok(None) => Response::from_json(&serde_json::json!({
            "error": "missing TICK_ALARM binding", "alarmArmed": false, "nextAlarmMs": null,
        }))
        .map(|r| r.with_status(503)),
        Err(e) => Response::from_json(&serde_json::json!({"error": e.to_string()}))
            .map(|r| r.with_status(503)),
    }
}

pub(crate) async fn health_json(db: &worker::D1Database, env: &Env) -> serde_json::Value {
    let last = crate::tick_ops::last_tick(db).await;
    let activity = crate::tick_ops::activity(db, now_ms()).await;
    let alarm = call(env, false).await;
    let (alarm_armed, next_alarm_ms, alarm_error) = match alarm {
        Ok(Some(status)) => (Some(status.alarm_armed), status.next_alarm_ms, None),
        Ok(None) => (
            Some(false),
            None,
            Some("missing TICK_ALARM binding".to_string()),
        ),
        Err(e) => (None, None, Some(e.to_string())),
    };
    let readable = last.is_ok() && activity.is_ok();
    let last = last.ok().unwrap_or_default();
    let activity = activity.ok().flatten();
    serde_json::json!({
        "lastSource": last.tick_source,
        "lastAtMs": last.tick_at_ms.map(|ms| ms as i64),
        "alarmArmed": alarm_armed,
        "nextAlarmMs": next_alarm_ms,
        "cronSeen24h": activity.as_ref().map(|a| a.cron as u64),
        "alarmSeen24h": activity.as_ref().map(|a| a.alarm as u64),
        "leaseSkips24h": activity.as_ref().map(|a| a.skips as u64),
        "readable": readable,
        "alarmError": alarm_error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn failed_tick_still_rearms_from_completion() {
        let now = std::cell::Cell::new(100);
        let arms = std::cell::RefCell::new(Vec::new());
        let report = run_and_rearm(
            |at| {
                arms.borrow_mut().push(at);
                std::future::ready(Ok::<_, &str>(()))
            },
            async {
                assert_eq!(*arms.borrow(), vec![900_100]);
                now.set(500);
                Err::<(), _>("failed tick")
            },
            || now.get(),
        )
        .await;
        assert_eq!(report, Ok(Err("failed tick")));
        assert_eq!(*arms.borrow(), vec![900_100, 900_500]);
    }

    #[test]
    fn rearm_advances_from_completion_and_handles_a_clock_rollback() {
        assert_eq!(next_alarm_at(100, None), 900_100);
        assert_eq!(next_alarm_at(500, Some(100)), 900_500);
        assert_eq!(next_alarm_at(50, Some(100)), 900_100);
        assert_eq!(next_alarm_at(i64::MAX, None), i64::MAX);
    }

    #[tokio::test]
    async fn missing_binding_skips_the_arm_without_making_a_request() {
        let result = with_optional_binding::<(), (), _, _, _>(Err("missing binding"), |_| async {
            panic!("a missing binding cannot send a request")
        })
        .await;
        assert_eq!(result, Ok(None));
    }

    #[tokio::test]
    async fn a_bound_alarm_preserves_the_response_or_transport_error() {
        assert_eq!(
            with_optional_binding(Ok::<_, &str>(7), |v| async move { Ok(v + 1) }).await,
            Ok(Some(8))
        );
        assert_eq!(
            with_optional_binding(Ok::<_, &str>(7), |_| async { Err::<(), _>("unavailable") })
                .await,
            Err("unavailable")
        );
    }
}
