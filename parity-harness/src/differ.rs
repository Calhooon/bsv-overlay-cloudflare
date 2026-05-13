//! Compare two Captured responses and produce a unified diff if they differ.

use crate::{canonical, client::Captured};
use similar::{ChangeTag, TextDiff};

pub struct Outcome {
    pub matches: bool,
    pub status_matches: bool,
    pub body_matches: bool,
    pub content_type_matches: bool,
    /// Unified diff of canonicalised bodies (empty if they match).
    pub body_diff: String,
    /// TS canonical body — captured for future debug introspection (e.g.,
    /// when a parity diff needs the literal pre-diff form for a bug report).
    /// Currently unused by `report.rs`; kept to avoid losing the value
    /// when `compare()` already computed it.
    #[allow(dead_code, reason = "kept for parity-harness debug introspection")]
    pub ts_canonical: String,
    #[allow(dead_code, reason = "kept for parity-harness debug introspection")]
    pub rust_canonical: String,
}

pub fn compare(ts: &Captured, rust: &Captured) -> Outcome {
    let status_matches = ts.status == rust.status;

    let ts_ct = content_type(ts);
    let rust_ct = content_type(rust);
    let content_type_matches = strip_charset(&ts_ct) == strip_charset(&rust_ct);

    let (ts_canonical, rust_canonical, body_matches) = {
        let ts_json = canonical::try_parse_json(&ts.body_text);
        let rust_json = canonical::try_parse_json(&rust.body_text);
        match (ts_json, rust_json) {
            (Some(a), Some(b)) => {
                let ca = canonical::canonicalise(&a);
                let cb = canonical::canonicalise(&b);
                let sa = canonical::pretty(&ca);
                let sb = canonical::pretty(&cb);
                let eq = sa == sb;
                (sa, sb, eq)
            }
            _ => {
                // Non-JSON: compare raw text with trailing-whitespace normalisation.
                let sa = ts.body_text.trim_end().to_string() + "\n";
                let sb = rust.body_text.trim_end().to_string() + "\n";
                let eq = sa == sb;
                (sa, sb, eq)
            }
        }
    };

    let body_diff = if body_matches {
        String::new()
    } else {
        unified_diff(&ts_canonical, &rust_canonical)
    };

    Outcome {
        matches: status_matches && body_matches && content_type_matches,
        status_matches,
        body_matches,
        content_type_matches,
        body_diff,
        ts_canonical,
        rust_canonical,
    }
}

fn content_type(c: &Captured) -> String {
    c.headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-type"))
        .map(|(_, v)| v.clone())
        .unwrap_or_default()
}

fn strip_charset(ct: &str) -> String {
    ct.split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

/// Myers diff is O((N+M)D). For intentionally divergent responses (e.g.
/// the root_dashboard HTML, where mainline and rust render entirely
/// different HTML), the edit distance approaches max and the algorithm
/// can take many minutes on kilobyte-scale inputs. Cap the wall time
/// and fall back to a degraded summary so the report generation makes
/// forward progress.
const DIFF_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
/// Bodies larger than this (characters) skip the line-diff entirely —
/// the Myers algorithm becomes pathological above ~16 KiB of divergent
/// content and the diff output becomes unreadable anyway.
const DIFF_MAX_CHARS: usize = 16 * 1024;

fn unified_diff(a: &str, b: &str) -> String {
    let mut out = String::new();
    out.push_str("--- ts\n+++ rust\n");

    if a.len() > DIFF_MAX_CHARS || b.len() > DIFF_MAX_CHARS {
        out.push_str(&format!(
            "(bodies too large to diff: ts={} chars, rust={} chars — showing first 200 chars of each)\n",
            a.len(),
            b.len()
        ));
        out.push_str("-ts: ");
        out.push_str(&a.chars().take(200).collect::<String>());
        out.push('\n');
        out.push_str("+rust: ");
        out.push_str(&b.chars().take(200).collect::<String>());
        out.push('\n');
        return out;
    }

    let deadline = std::time::Instant::now() + DIFF_TIMEOUT;
    let diff = TextDiff::configure().deadline(deadline).diff_lines(a, b);
    for change in diff.iter_all_changes() {
        let sign = match change.tag() {
            ChangeTag::Delete => "-",
            ChangeTag::Insert => "+",
            ChangeTag::Equal => " ",
        };
        out.push_str(sign);
        out.push_str(change.value());
        if !change.value().ends_with('\n') {
            out.push('\n');
        }
    }
    out
}
