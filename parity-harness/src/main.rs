//! Differential parity oracle for rust-overlay ↔ @bsv/overlay-express 2.2.0.
//!
//! Replays a corpus of HTTP requests against both implementations,
//! canonicalises JSON responses, and writes a markdown report flagging any
//! divergence. Exit code is non-zero if any entry diverges.

mod canonical;
mod client;
mod corpus;
mod differ;
mod report;

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(about = "rust-overlay ↔ overlay-express parity harness")]
struct Args {
    /// TS reference base URL (overlay-express on Docker)
    #[arg(long, default_value = "http://localhost:8090")]
    ts: String,

    /// Rust implementation base URL (wrangler dev)
    #[arg(long, default_value = "http://127.0.0.1:8787")]
    rust: String,

    /// Corpus directory (walked recursively for .json entries)
    #[arg(long, default_value = "./parity-harness/corpus")]
    corpus: PathBuf,

    /// Output markdown report path
    #[arg(long, default_value = "./PARITY_REPORT.md")]
    report: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    eprintln!(
        "parity-harness: ts={} rust={} corpus={}",
        args.ts,
        args.rust,
        args.corpus.display()
    );

    let entries = corpus::walk(&args.corpus)?;
    eprintln!("loaded {} corpus entries", entries.len());

    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    let mut results = Vec::with_capacity(entries.len());
    for entry in &entries {
        let ts = client::send(&http, &args.ts, entry).await;
        let rust = client::send(&http, &args.rust, entry).await;
        let outcome = differ::compare(&ts, &rust);
        let tag = if outcome.matches { "GREEN" } else { "RED  " };
        eprintln!("  [{tag}] {}", entry.name);
        results.push((entry.clone(), ts, rust, outcome));
    }

    report::write(&args.report, &args, &results)?;
    let reds = results.iter().filter(|(_, _, _, o)| !o.matches).count();
    eprintln!(
        "\ndone: {} entries, {} green, {} red — report at {}",
        results.len(),
        results.len() - reds,
        reds,
        args.report.display()
    );

    if reds > 0 {
        std::process::exit(1);
    }
    Ok(())
}
