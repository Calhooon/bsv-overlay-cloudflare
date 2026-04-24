//! JSON canonicalisation — sorts keys, normalises ephemeral fields so that
//! only structural divergences surface.

use serde_json::{Map, Value};

/// Fields we treat as ephemeral (values replaced with `"<NORMALIZED>"`).
/// Matched case-insensitively against top-level OR nested keys.
const EPHEMERAL_KEYS: &[&str] = &[
    "startedat",
    "starttime",
    "uptimems",
    "uptime",
    "uptime_secs",
    "uptimesecs",
    "scheduler_last_tick_secs_ago",
    "durationms",
    "responsetimems",
    "timestamp",
    "time",
    "date",
    // Storage-assigned IDs — Mongo ObjectIDs on TS vs synthesised keys on Rust
    "_id",
    "createdat",
    "bannedat",
    // Health-check churn counters
    "down",
];

/// Canonicalise a JSON value: sort map keys recursively, replace ephemeral
/// field values with a placeholder, and apply shape-aware normalisation
/// (e.g. `/health` `checks[]` collapse to scope aggregates).
pub fn canonicalise(v: &Value) -> Value {
    let standard = canonicalise_standard(v);
    if is_health_shape(&standard) {
        canonicalise_standard(&apply_health_rules(standard))
    } else {
        standard
    }
}

fn canonicalise_standard(v: &Value) -> Value {
    match v {
        Value::Object(m) => {
            let mut sorted: Vec<(&String, &Value)> = m.iter().collect();
            sorted.sort_by(|a, b| a.0.cmp(b.0));
            let mut out = Map::new();
            for (k, val) in sorted {
                let canon_val = if is_ephemeral(k) {
                    Value::String("<NORMALIZED>".into())
                } else {
                    canonicalise_standard(val)
                };
                out.insert(k.clone(), canon_val);
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(arr.iter().map(canonicalise_standard).collect()),
        _ => v.clone(),
    }
}

/// Does this look like a `/health` response? (has top-level `checks` + `live`)
fn is_health_shape(v: &Value) -> bool {
    match v {
        Value::Object(m) => m.contains_key("checks") && m.contains_key("live"),
        _ => false,
    }
}

/// Collapse `/health`-specific fields so that:
/// - `checks[]` becomes `{live_ok, ready_ok}` (platform-appropriate check
///   names like knex/mongo vs d1/queues don't cause false diffs)
/// - deployment-specific `service.{name, advertisableFQDN, port}` are
///   normalised away (they'll always differ between TS-local and
///   Rust-local).
fn apply_health_rules(mut v: Value) -> Value {
    if let Value::Object(root) = &mut v {
        if let Some(Value::Array(checks)) = root.remove("checks") {
            let live_ok = scope_ok(&checks, "live");
            let ready_ok = scope_ok(&checks, "ready");
            root.insert(
                "checks".into(),
                serde_json::json!({
                    "live_ok": live_ok,
                    "ready_ok": ready_ok,
                }),
            );
        }
        if let Some(Value::Object(service)) = root.get_mut("service") {
            for field in ["name", "advertisableFQDN", "port"] {
                if service.contains_key(field) {
                    service.insert(field.into(), Value::String("<NORMALIZED>".into()));
                }
            }
        }
    }
    v
}

/// Are all *critical* checks in the given scope reporting "ok"?
fn scope_ok(checks: &[Value], scope: &str) -> bool {
    checks
        .iter()
        .filter(|c| c.get("scope").and_then(|s| s.as_str()) == Some(scope))
        .filter(|c| c.get("critical").and_then(|v| v.as_bool()).unwrap_or(false))
        .all(|c| c.get("status").and_then(|s| s.as_str()) == Some("ok"))
}

fn is_ephemeral(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    EPHEMERAL_KEYS.iter().any(|e| *e == lower)
}

/// Pretty-print canonical JSON (2-space indent, trailing newline) for diffing.
pub fn pretty(v: &Value) -> String {
    let mut s = serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string());
    s.push('\n');
    s
}

/// Try to parse body as JSON; on failure, return None.
pub fn try_parse_json(body: &str) -> Option<Value> {
    serde_json::from_str(body).ok()
}
