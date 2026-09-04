//! `GET /internal/armed-pots` (2026-09-03, bsv-low W4 — the tower watchdog's
//! POPULATION).
//!
//! `low-monitor` reads the tower `/status` of every pot it knows about every
//! five minutes, and since 2026-09-03 that read REPAIRS a lost Durable Object
//! sweep alarm (Cloudflare loses about one tick in a hundred; a lost tick
//! silently ends a parked refund's dead-man switch). The monitor's registry,
//! however, is written by the tower's OWN tick once the pot corroborates on
//! chain — so a pot that loses its very first tick never gets a registry row
//! and never gets a repairing read. This listing is the population that does
//! not depend on the tower: every pot with a refund-backup marker admitted by
//! the overlay (`potrefund_records` — the client files it at arm time, and
//! admission is network-accept gated, so the pot is chain-corroborated by
//! construction), joined against the pot index and filtered to UNSPENT, newest
//! first, bounded. Bearer-gated (`INTERNAL_TOKEN`) like the other first-party
//! internal routes; served before the BRC-103 front door.
//!
//! A pot that appears here with NO tower record is itself a finding the
//! monitor pages on (the arm never reached the tower, or was displaced).
use serde::Deserialize;
use serde_json::{json, Value};
use worker::*;

/// Default lookback: a parked refund lives at most `MAX_LOCK_AHEAD` blocks
/// (~500) past its arm — three days covers it with margin.
pub const ARMED_POTS_DEFAULT_SINCE_MS: i64 = 3 * 24 * 60 * 60 * 1000;
pub const ARMED_POTS_MAX_ROWS: usize = 400;

/// UNSPENT pots with a refund-backup marker since `?1` (UNIX SECONDS — the
/// overlay stamps `potrefund_records.createdAt` with
/// `current_unix_seconds_i64()`; the first cut compared milliseconds and
/// listed nothing, caught by the empty live answer), newest first, `?2` rows
/// at most. The LEFT JOIN keeps a pot the index has not marked yet
/// (`spent IS NULL`) — an unknown spend is not a spend.
pub fn armed_pots_sql() -> &'static str {
    "SELECT pr.potTxid AS potTxid, pr.potVout AS potVout, pr.gameId AS gameId, \
            MAX(pr.createdAt) AS createdAt \
     FROM potrefund_records pr \
     LEFT JOIN pot_records p ON p.txid = pr.potTxid AND p.outputIndex = pr.potVout \
     WHERE (p.spent IS NULL OR p.spent = 0) AND pr.createdAt >= ?1 \
     GROUP BY pr.potTxid, pr.potVout, pr.gameId \
     ORDER BY createdAt DESC \
     LIMIT ?2"
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ArmedPotRowD1 {
    #[serde(rename = "potTxid")]
    pot_txid: String,
    #[serde(rename = "potVout")]
    pot_vout: f64,
    #[serde(rename = "gameId")]
    game_id: String,
    #[serde(rename = "createdAt", default)]
    created_at: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArmedPot {
    pub pot_txid: String,
    pub pot_vout: u32,
    pub game_id: String,
    pub created_at_ms: i64,
}

impl ArmedPotRowD1 {
    pub(crate) fn into_pot(self) -> Option<ArmedPot> {
        let txid = self.pot_txid.trim().to_ascii_lowercase();
        if txid.len() != 64 || !txid.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        if !(self.pot_vout.is_finite() && self.pot_vout >= 0.0) {
            return None;
        }
        Some(ArmedPot {
            pot_txid: txid,
            pot_vout: self.pot_vout as u32,
            game_id: self.game_id.trim().to_ascii_lowercase(),
            // Stored in seconds; served in milliseconds like every other stamp.
            created_at_ms: self.created_at.map_or(0, |v| (v as i64).saturating_mul(1000)),
        })
    }
}

/// `since_ms` and `limit` from the query string, bounded: `limit` never above
/// `ARMED_POTS_MAX_ROWS`, `since_ms` never negative; absent = the defaults.
pub fn parse_armed_pots_query(query: Option<&str>, now_ms: i64) -> (i64, usize) {
    let mut since = now_ms - ARMED_POTS_DEFAULT_SINCE_MS;
    let mut limit = ARMED_POTS_MAX_ROWS;
    for pair in query.unwrap_or("").split('&') {
        let (k, v) = match pair.split_once('=') {
            Some(kv) => kv,
            None => continue,
        };
        match k {
            "since_ms" => {
                if let Ok(n) = v.parse::<i64>() {
                    since = n.max(0);
                }
            }
            "limit" => {
                if let Ok(n) = v.parse::<usize>() {
                    limit = n.clamp(1, ARMED_POTS_MAX_ROWS);
                }
            }
            _ => {}
        }
    }
    (since, limit)
}

/// The stored stamp is seconds; the API speaks milliseconds.
#[must_use]
pub fn since_seconds(since_ms: i64) -> i64 {
    (since_ms / 1000).max(0)
}

/// The body: `{ pots: [{potTxid, potVout, gameId, createdAt}], truncated, sinceMs }`.
pub fn armed_pots_body(pots: &[ArmedPot], truncated: bool, since_ms: i64) -> Value {
    json!({
        "pots": pots.iter().map(|p| json!({
            "potTxid": p.pot_txid,
            "potVout": p.pot_vout,
            "gameId": p.game_id,
            "createdAt": p.created_at_ms,
        })).collect::<Vec<_>>(),
        "truncated": truncated,
        "sinceMs": since_ms,
        "count": pots.len(),
    })
}

pub async fn armed_pots(req: Request, env: &Env) -> Result<Response> {
    if !crate::internal_events::internal_bearer_ok(&req, env) {
        return Response::error("unauthorized", 401);
    }
    let url = req.url()?;
    let (since_ms, limit) = parse_armed_pots_query(url.query(), Date::now().as_millis() as i64);
    let db = match env.d1("OVERLAY_DB") {
        Ok(db) => db,
        Err(e) => {
            console_warn!("[armed-pots] OVERLAY_DB binding unavailable: {e}");
            return Response::error("database unavailable", 503);
        }
    };
    // One row past the limit tells the caller the page is cut.
    let stmt = db.prepare(armed_pots_sql()).bind(&[
        wasm_bindgen::JsValue::from_f64(since_seconds(since_ms) as f64),
        wasm_bindgen::JsValue::from_f64((limit + 1) as f64),
    ])?;
    let rows: Vec<ArmedPotRowD1> = match stmt.all().await.and_then(|r| r.results::<ArmedPotRowD1>()) {
        Ok(rows) => rows,
        Err(e) => {
            console_warn!("[armed-pots] query failed: {e}");
            return Response::error("database query failed", 503);
        }
    };
    let mut pots: Vec<ArmedPot> = rows.into_iter().filter_map(ArmedPotRowD1::into_pot).collect();
    let truncated = pots.len() > limit;
    pots.truncate(limit);
    Response::from_json(&armed_pots_body(&pots, truncated, since_ms))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_query_is_bounded_and_defaults_to_three_days() {
        let now = 1_000_000_000_000i64;
        assert_eq!(parse_armed_pots_query(None, now), (now - ARMED_POTS_DEFAULT_SINCE_MS, ARMED_POTS_MAX_ROWS));
        assert_eq!(parse_armed_pots_query(Some("since_ms=5&limit=10"), now), (5, 10));
        assert_eq!(parse_armed_pots_query(Some("limit=9999"), now).1, ARMED_POTS_MAX_ROWS);
        assert_eq!(parse_armed_pots_query(Some("limit=0"), now).1, 1);
        assert_eq!(parse_armed_pots_query(Some("since_ms=-7"), now).0, 0);
        assert_eq!(parse_armed_pots_query(Some("since_ms=x&limit=y"), now), (now - ARMED_POTS_DEFAULT_SINCE_MS, ARMED_POTS_MAX_ROWS));
    }

    #[test]
    fn the_sql_keeps_unmarked_pots_and_drops_spent_ones() {
        let sql = armed_pots_sql();
        assert!(sql.contains("LEFT JOIN pot_records"), "an unknown spend is not a spend");
        assert!(sql.contains("p.spent IS NULL OR p.spent = 0"));
        assert!(sql.contains("ORDER BY createdAt DESC"));
        assert!(sql.contains("LIMIT ?2"));
    }

    #[test]
    fn rows_are_validated_and_lowercased() {
        let ok = ArmedPotRowD1 { pot_txid: "AB".repeat(32), pot_vout: 0.0, game_id: "G1".into(), created_at: Some(5.0) }.into_pot().unwrap();
        assert_eq!(ok.pot_txid, "ab".repeat(32));
        assert_eq!(ok.game_id, "g1");
        assert_eq!(ok.created_at_ms, 5_000, "seconds in the row, milliseconds out");
        assert_eq!(since_seconds(1_788_470_000_123), 1_788_470_000);
        assert_eq!(since_seconds(-5), 0);
        assert!(ArmedPotRowD1 { pot_txid: "zz".repeat(32), pot_vout: 0.0, game_id: "g".into(), created_at: None }.into_pot().is_none());
        assert!(ArmedPotRowD1 { pot_txid: "ab".repeat(32), pot_vout: -1.0, game_id: "g".into(), created_at: None }.into_pot().is_none());
        let body = armed_pots_body(&[ok], true, 7);
        assert_eq!(body["count"], 1);
        assert_eq!(body["truncated"], true);
        assert_eq!(body["pots"][0]["potVout"], 0);
    }
}
