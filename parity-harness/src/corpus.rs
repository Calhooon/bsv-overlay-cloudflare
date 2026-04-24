//! Corpus entry format + walker.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path};
use walkdir::WalkDir;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub name: String,
    pub method: String,
    pub path: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub body: Option<serde_json::Value>,
    /// Optional human-readable note. Rendered in PARITY_REPORT.md for
    /// expected-divergence entries (e.g. where mainline has a known bug).
    #[serde(default)]
    pub note: Option<String>,
    /// Relative path of the corpus file (for reporting)
    #[serde(skip)]
    pub source: String,
}

pub fn walk(root: &Path) -> Result<Vec<Entry>> {
    let mut out = Vec::new();
    for dirent in WalkDir::new(root).into_iter().filter_map(|e| e.ok()) {
        let path = dirent.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading {}", path.display()))?;
        let mut entry: Entry = serde_json::from_str(&raw)
            .with_context(|| format!("parsing {}", path.display()))?;
        entry.source = path
            .strip_prefix(root)
            .unwrap_or(path)
            .to_string_lossy()
            .into_owned();
        out.push(entry);
    }
    out.sort_by(|a, b| a.source.cmp(&b.source));
    Ok(out)
}
