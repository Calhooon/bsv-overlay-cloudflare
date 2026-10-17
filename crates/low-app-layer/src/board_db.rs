//! bsv-low #642: the leaderboard pipeline's D1 port. The route runs the SAME
//! pipeline over D1 ([`D1Board`]) that the native pins run over rusqlite, so
//! the byte-identity and rows-read pins execute the shipped orchestration, not
//! a model of it.

/// One bound parameter, as the pipeline binds it (a D1 number or text).
#[derive(Debug, Clone, PartialEq)]
pub enum Bind {
    Num(f64),
    Text(String),
}

/// A statement's fault: its bind or its query (the call sites before #642
/// told the two apart, and still do).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoardDbError {
    Bind(String),
    Query(String),
}

impl std::fmt::Display for BoardDbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BoardDbError::Bind(e) | BoardDbError::Query(e) => f.write_str(e),
        }
    }
}

/// Run one statement and map its rows (the production `*D1` row types). Each
/// call site decides whether a fault is a 503 or an omitted decoration, as
/// before #642.
#[allow(async_fn_in_trait)]
pub trait BoardDb {
    async fn all<T: serde::de::DeserializeOwned>(
        &self,
        sql: String,
        binds: Vec<Bind>,
    ) -> Result<Vec<T>, BoardDbError>;
}

/// The Worker's D1, every statement counted by the #499 ledger.
pub struct D1Board<'a>(pub &'a worker::D1Database);

impl BoardDb for D1Board<'_> {
    async fn all<T: serde::de::DeserializeOwned>(
        &self,
        sql: String,
        binds: Vec<Bind>,
    ) -> Result<Vec<T>, BoardDbError> {
        use crate::d1_ledger::Counted;
        use worker::wasm_bindgen::JsValue;
        let js: Vec<JsValue> = binds
            .iter()
            .map(|b| match b {
                Bind::Num(n) => JsValue::from_f64(*n),
                Bind::Text(s) => JsValue::from_str(s),
            })
            .collect();
        let stmt = self
            .0
            .prepare(sql)
            .bind(&js)
            .map_err(|e| BoardDbError::Bind(e.to_string()))?;
        stmt.counted_all()
            .await
            .and_then(|r| r.results::<T>())
            .map_err(|e| BoardDbError::Query(e.to_string()))
    }
}

/// A log line on a path the native pins run (`console_warn!` calls a wasm
/// import and panics off wasm32).
pub fn warn(msg: &str) {
    #[cfg(target_arch = "wasm32")]
    worker::console_warn!("{msg}");
    #[cfg(not(target_arch = "wasm32"))]
    eprintln!("{msg}");
}
