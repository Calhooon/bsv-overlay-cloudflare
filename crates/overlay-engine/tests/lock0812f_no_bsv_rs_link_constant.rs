//! LOCK-0812-FOLD: the engine names bsv-rs's link limits by a method's result,
//! never by a constant of bsv-rs.
//!
//! This repository takes every later 0.8 patch of bsv-rs by the lock alone, so
//! no source may fail to compile because bsv-rs renamed or dropped one of its
//! associated constants. Its default heap figure is read as
//! `BeefLinkLimits::default().max_link_heap_bytes`, and the engine sets its
//! own through `BeefLinkLimits::with_max_link_heap_bytes`. A source pin: it
//! reads the text of every Rust file under the crates named below.
// bsv-stack-lean #134: test code
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::{Path, PathBuf};

/// The trees a link-limit constant of bsv-rs must not appear in.
const TREES: [&str; 5] = [
    "crates/overlay-engine/src",
    "crates/overlay-engine/tests",
    "crates/overlay-discovery/src",
    "crates/overlay-cloudflare/src",
    "crates/low-app-layer/src",
];

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|x| x == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn lock0812f_no_source_names_a_link_limit_constant_of_bsv_rs() {
    // Built in two halves so this file does not carry the needle whole.
    let needle = ["DEFAULT_MAX", "_LINK"].concat();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut found = Vec::new();
    let mut read = 0usize;
    for tree in TREES {
        let mut files = Vec::new();
        rust_files(&root.join(tree), &mut files);
        for file in files {
            read += 1;
            let text = std::fs::read_to_string(&file).unwrap();
            for (n, line) in text.lines().enumerate() {
                if line.contains(&needle) {
                    found.push(format!("{}:{}: {}", file.display(), n + 1, line.trim()));
                }
            }
        }
    }
    assert!(read > 100, "the trees were read: {read} files");
    assert!(
        found.is_empty(),
        "bsv-rs's link limits are named by a method's result, never its constant: {found:#?}"
    );
}
