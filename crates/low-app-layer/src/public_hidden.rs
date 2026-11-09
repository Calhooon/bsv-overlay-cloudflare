//! bsv-low #1154 (lane PROOF-SEATS): the identities the PUBLIC leaderboard never lists.
//!
//! Every promotion ends with one real hand between two harness seats on a private table (bsv-low
//! `scripts/prod-hand-leg.sh`); its winner is a result like any other and would enter the public board. The
//! render var [`VAR`] names those seats: a comma separated list of compressed public keys (66 hex, `02` or `03`),
//! trimmed and lowercased at the read. Unset or empty hides nobody; an entry that is not such a key is ignored,
//! counted ([`Hidden::malformed`], served `/health.publicHiddenMalformed`) and logged once per isolate.
//!
//! The REACH is `/leaderboard` alone (`routes::leaderboard_page_body_hidden`): no board row for a hidden identity,
//! no hand whose winner is hidden, and the counts and the paging count what is listed. A view keyed by the caller's
//! own identity or a named identity (`/results`, `/owed`, `/hops-view`, `/refund-view`, `/live-view`, `/proof`)
//! never reads it: the seats are real wallets with real sats and the harness reads those views.

use std::cell::RefCell;

/// The render var.
pub const VAR: &str = "PUBLIC_HIDDEN_IDENTITIES";

/// The parsed var: the hidden keys (lowercase, sorted, distinct) and the entries refused.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Hidden {
    pub keys: Vec<String>,
    pub malformed: usize,
}

/// A compressed public key as the board names an owner: 66 lowercase hex, `02` or `03` first.
fn compressed_key(entry: &str) -> Option<String> {
    let k = entry.trim().to_ascii_lowercase();
    (k.len() == 66
        && (k.starts_with("02") || k.starts_with("03"))
        && k.bytes().all(|b| b.is_ascii_hexdigit()))
    .then_some(k)
}

/// Parse the var's raw value (`None`: unset). Empty entries (a trailing comma, spaces) are not counted.
pub fn parse(raw: Option<&str>) -> Hidden {
    let mut out = Hidden::default();
    for entry in raw.unwrap_or("").split(',') {
        if entry.trim().is_empty() {
            continue;
        }
        match compressed_key(entry) {
            Some(k) => out.keys.push(k),
            None => out.malformed += 1,
        }
    }
    out.keys.sort();
    out.keys.dedup();
    out
}

thread_local! {
    /// This isolate's parse, keyed by the raw value it parsed, and whether its refusals were logged.
    static MEMO: RefCell<Option<(Option<String>, Hidden, bool)>> = const { RefCell::new(None) };
}

/// The parse of `raw`, memoised per isolate; `log` is called once per isolate per raw value whose parse refused
/// an entry.
pub fn memoised(raw: Option<String>, log: impl FnOnce(usize)) -> Hidden {
    MEMO.with(|m| {
        let mut m = m.borrow_mut();
        if m.as_ref().map(|(r, _, _)| r) != Some(&raw) {
            let parsed = parse(raw.as_deref());
            *m = Some((raw, parsed, false));
        }
        let (_, parsed, logged) = m.as_mut().expect("set above");
        if parsed.malformed > 0 && !*logged {
            *logged = true;
            log(parsed.malformed);
        }
        parsed.clone()
    })
}

/// The var as this Worker's render sets it.
pub fn from_env(env: &worker::Env) -> Hidden {
    memoised(env.var(VAR).ok().map(|v| v.to_string()), |n| {
        worker::console_warn!(
            "[leaderboard] {VAR}: {n} entr{} not a compressed public key (66 hex, 02 or 03): ignored",
            if n == 1 { "y" } else { "ies" }
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "03c2aec25a754849410c9d27b4ec50db5d8a4fe0bd57867071a2c1dbbdbb553f6a";
    const B: &str = "0247bf525bd5d14e4783b7f7c5517db56c46d1bec1cf665d90b32d7ecc412cf0bc";

    /// bsv-low #1154 pin 1: trim, lowercase, 66-hex compressed keys only; a malformed entry ignored and counted;
    /// unset and empty the same.
    #[test]
    fn proof_seats_the_parse() {
        assert_eq!(parse(None), Hidden::default());
        assert_eq!(parse(Some("")), Hidden::default());
        assert_eq!(parse(Some(" , ,")), Hidden::default());
        let upper = A.to_ascii_uppercase();
        let raw = format!("  {upper} ,{B},{A}, ");
        assert_eq!(
            parse(Some(&raw)),
            Hidden {
                keys: vec![B.to_string(), A.to_string()],
                malformed: 0
            },
            "trimmed, lowercased, sorted, distinct"
        );
        let bad = [
            &A[..64],                      // too short
            &format!("{A}00"),             // too long
            &format!("04{}", &A[2..]),     // not a compressed prefix
            &format!("03{}zz", &A[2..64]), // not hex
            "nobody",
        ];
        let raw = format!("{},{B}", bad.join(","));
        assert_eq!(
            parse(Some(&raw)),
            Hidden {
                keys: vec![B.to_string()],
                malformed: 5
            }
        );
    }

    /// The memo logs a refusal once per raw value per isolate, and re-parses a moved value.
    #[test]
    fn proof_seats_the_memo_logs_once() {
        let mut logs = 0;
        for _ in 0..3 {
            let h = memoised(Some(format!("{A},bad")), |_| logs += 1);
            assert_eq!(h.keys, vec![A.to_string()]);
        }
        assert_eq!(logs, 1);
        let h = memoised(None, |_| logs += 1);
        assert!(h.keys.is_empty());
        assert_eq!(logs, 1);
    }

    /// bsv-low #1154 pin 6: the prod render names the six harness identities of the prod hand legs; the beta render
    /// names none.
    #[test]
    fn proof_seats_the_renders() {
        let toml = include_str!("../wrangler.toml");
        let prod = &toml[toml.find("\n[vars]\n").unwrap()..];
        let prod = &prod[..prod.find("\n[[").unwrap()];
        let beta = &toml[toml.find("\n[env.beta.vars]\n").unwrap()..];
        let beta = &beta[..beta.find("\n[[").unwrap()];
        let value = |vars: &str| {
            vars.lines()
                .find_map(|l| l.trim().strip_prefix(&format!("{VAR} = ")))
                .map(|v| v.trim_matches('"').to_string())
        };
        let mut six = vec![
            "03c2aec25a754849410c9d27b4ec50db5d8a4fe0bd57867071a2c1dbbdbb553f6a",
            "03295e29accd6a25a9c1e14582e58bc149749b4f379c11cbe64068a29a925a40d2",
            "03b01d10e77d6b16f49fd88a0f13149757bf22aa999d03b59a1bd6da460b7a6934",
            "02b2f90e11a3af178c38f303f01dd93debfb2fc76e52401155eb735dde98eadacb",
            "0247bf525bd5d14e4783b7f7c5517db56c46d1bec1cf665d90b32d7ecc412cf0bc",
            "03369a6bf7f24b00bf49742e99b4204eb5a7ad5fc6602134bf70b2230f07afe5bf",
        ];
        six.sort();
        let parsed = parse(value(prod).as_deref());
        assert_eq!(parsed.malformed, 0);
        assert_eq!(parsed.keys, six, "the prod render's hidden identities");
        assert_eq!(value(beta), None, "the beta render hides nobody");
        assert!(
            !toml[toml.find("\n[env.beta").unwrap()..].contains(VAR),
            "no beta table sets the var"
        );
    }
}
