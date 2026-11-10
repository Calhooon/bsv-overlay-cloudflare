//! bsv-low #1159 (lane LAUNCH-FLOOR-APP): the launch instant pinned as the era cutoff's floor on the prod stack,
//! through the answers the routes build: `/epoch`'s body (`logic::epoch_body`), `/health`'s era fields
//! (`logic::era_health_fields`) and the owed actor's era (`owed_actor::actor_epoch`), each over the effective limbs
//! the one reader of each var serves (`logic::era_cutoff_ms`, `logic::era_cutoff_height`, which
//! `routes::written_off_before_ms_env` and `routes::written_off_before_height` call). The money view's half of (h)
//! is in `hops_view_sqlite.rs` (`launch_floor_1159_h_*`).

use low_app_layer::logic::{
    clamp_future_cutoff, epoch_body, era_cutoff_height, era_cutoff_ms, era_health_fields,
    normalize_storage_epoch, normalize_written_off_before_height, normalize_written_off_before_ms,
    LAUNCH_FLOOR_HEIGHT, LAUNCH_FLOOR_MS,
};
use low_app_layer::owed_actor::actor_epoch;
use serde_json::{json, Map, Value};

const NOW: i64 = LAUNCH_FLOOR_MS + 10 * 86_400_000;
const HOUR: i64 = 3_600_000;

/// One `[section]` of the app layer's own render, as `KEY = "value"` pairs.
fn render_vars(section: &str) -> Vec<(String, String)> {
    let toml = include_str!("../wrangler.toml");
    let mut out = Vec::new();
    let mut inside = false;
    for line in toml.lines() {
        let t = line.trim();
        if t.starts_with('[') {
            inside = t == section;
            continue;
        }
        if !inside || t.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = t.split_once('=') {
            out.push((k.trim().to_string(), v.trim().trim_matches('"').to_string()));
        }
    }
    out
}

fn var(vars: &[(String, String)], k: &str) -> Option<String> {
    vars.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone())
}

/// The three answers a render gives under this build.
fn answers(vars: &[(String, String)]) -> (String, Value, String) {
    let marker = var(vars, "LOW_BETA_MARKER");
    let ms = era_cutoff_ms(var(vars, "WRITTEN_OFF_BEFORE_MS"), NOW, marker.as_deref());
    let h = era_cutoff_height(var(vars, "WRITTEN_OFF_BEFORE_HEIGHT"), marker.as_deref());
    let epoch = normalize_storage_epoch(var(vars, "STORAGE_EPOCH"));
    let mut health = Map::new();
    for (k, v) in era_health_fields(ms, h) {
        health.insert(k.to_string(), v);
    }
    (
        epoch_body(epoch.as_deref(), ms.value, h.value),
        Value::Object(health),
        actor_epoch(var(vars, "STORAGE_EPOCH"), ms.value),
    )
}

/// The same three under main's rule (c8388bcc: the normaliser and the future belt, no floor; `/health`'s two
/// fields, `json!(v)` or null).
fn answers_on_main(vars: &[(String, String)]) -> (String, Value, String) {
    let ms = clamp_future_cutoff(
        normalize_written_off_before_ms(var(vars, "WRITTEN_OFF_BEFORE_MS")),
        NOW,
    );
    let h = normalize_written_off_before_height(var(vars, "WRITTEN_OFF_BEFORE_HEIGHT"));
    let epoch = normalize_storage_epoch(var(vars, "STORAGE_EPOCH"));
    (
        epoch_body(epoch.as_deref(), ms, h),
        json!({"writtenOffBeforeMs": ms, "writtenOffBeforeHeight": h}),
        actor_epoch(var(vars, "STORAGE_EPOCH"), ms),
    )
}

fn with(vars: &[(String, String)], k: &str, v: String) -> Vec<(String, String)> {
    let mut out: Vec<_> = vars.iter().filter(|(n, _)| n != k).cloned().collect();
    out.push((k.to_string(), v));
    out
}

/// (h) the prod render with its cutoff an hour (and its height six blocks) above the floor: `/epoch` serves the
/// floor pair and `/health` says `cutoffClamped: true` beside it.
#[test]
fn launch_floor_1159_h_the_prod_render_above_the_floor_serves_the_floor() {
    let prod = render_vars("[vars]");
    assert_eq!(
        var(&prod, "LOW_BETA_MARKER"),
        None,
        "the prod render carries no beta marker"
    );
    let above = with(
        &prod,
        "WRITTEN_OFF_BEFORE_MS",
        (LAUNCH_FLOOR_MS + HOUR).to_string(),
    );
    let above = with(
        &above,
        "WRITTEN_OFF_BEFORE_HEIGHT",
        (LAUNCH_FLOOR_HEIGHT + 6).to_string(),
    );
    let (epoch, health, _) = answers(&above);
    let e: Value = serde_json::from_str(&epoch).unwrap();
    assert_eq!(
        (
            e["writtenOffBeforeMs"].clone(),
            e["writtenOffBeforeHeight"].clone()
        ),
        (json!(LAUNCH_FLOOR_MS), json!(LAUNCH_FLOOR_HEIGHT)),
        "/epoch serves the floor pair"
    );
    assert_eq!(
        health,
        json!({
            "writtenOffBeforeMs": LAUNCH_FLOOR_MS,
            "writtenOffBeforeHeight": LAUNCH_FLOOR_HEIGHT,
            "cutoffClamped": true,
        }),
        "/health echoes the floor pair and says it clamped"
    );
    let keys: Vec<&String> = e.as_object().unwrap().keys().collect();
    assert_eq!(
        keys,
        [
            "storageEpoch",
            "writtenOffBeforeHeight",
            "writtenOffBeforeMs"
        ],
        "/epoch's shape unchanged"
    );
}

/// (i) today's exact config, both renders: the three answers are byte for byte main's.
#[test]
fn launch_floor_1159_i_todays_config_is_byte_identical_to_main() {
    for section in ["[vars]", "[env.beta.vars]"] {
        let vars = render_vars(section);
        assert!(
            var(&vars, "WRITTEN_OFF_BEFORE_MS").is_some(),
            "{section} sets a cutoff"
        );
        assert_eq!(answers(&vars), answers_on_main(&vars), "{section}");
    }
    let prod = render_vars("[vars]");
    assert_eq!(
        var(&prod, "WRITTEN_OFF_BEFORE_MS").as_deref(),
        Some("1794257400000"),
        "prod's configured cutoff is the floor"
    );
    assert_eq!(
        var(&prod, "WRITTEN_OFF_BEFORE_HEIGHT").as_deref(),
        Some("974809")
    );
    let (epoch, health, _) = answers(&prod);
    assert_eq!(
        epoch,
        "{\"storageEpoch\":\"2026-11-09-launch-1\",\"writtenOffBeforeHeight\":974809,\"writtenOffBeforeMs\":1794257400000}"
    );
    assert!(
        health.get("cutoffClamped").is_none(),
        "nothing clamped, no key"
    );
}

/// (j) the owed actor's era is the same for the render above the floor as for the floor's own render: no actor
/// is re-keyed by a forward bump.
#[test]
fn launch_floor_1159_j_the_owed_actors_era_does_not_move() {
    let prod = render_vars("[vars]");
    let floor = with(&prod, "WRITTEN_OFF_BEFORE_MS", LAUNCH_FLOOR_MS.to_string());
    let above = with(
        &prod,
        "WRITTEN_OFF_BEFORE_MS",
        (LAUNCH_FLOOR_MS + HOUR).to_string(),
    );
    assert_eq!(answers(&above).2, answers(&floor).2);
    assert_eq!(
        answers(&floor).2,
        format!("2026-11-09-launch-1|{LAUNCH_FLOOR_MS}")
    );
    // beta keeps its own era as configured
    let beta = render_vars("[env.beta.vars]");
    let beta_above = with(
        &beta,
        "WRITTEN_OFF_BEFORE_MS",
        (LAUNCH_FLOOR_MS + HOUR).to_string(),
    );
    assert_eq!(answers(&beta_above).2, answers_on_main(&beta_above).2);
}
