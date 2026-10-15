//! Out-of-band overlay D1 migration emitter (bsv-low promotion-repair B.1,
//! the #430 shape). Renders the migration statements `from+1..=count` plus the
//! certify rows as `.sql` files for `wrangler d1 execute --remote --file`.
//!
//! Run (from the PROMOTED sha's checkout):
//!
//! ```text
//! cargo run -p bsv-overlay-cloudflare --example emit-migrations -- \
//!     --from <persisted count> [--count <target>] --out <dir> [--chunk 25]
//! ```
//!
//! `--count` defaults to the built-in `OVERLAY_MIGRATION_COUNT`; it is a
//! refusal to pass any other value (a different count means the wrong
//! checkout). See `docs/MIGRATION-EMITTER.md` for the operator steps.
//!
//! Exit codes: 0 success; 2 a refusal or a usage error (nothing written);
//! 1 an IO fault.

use std::path::PathBuf;
use std::process::ExitCode;

use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATION_COUNT;
use bsv_overlay_cloudflare::migration_emitter::{emit_to_dir, EmitFailure, DEFAULT_CHUNK};

const USAGE: &str = "\
usage: emit-migrations --from <persisted count> --out <dir> \
[--count <target, default built-in>] [--chunk <statements per file, default 25>]";

enum Fail {
    Usage(String),
    Emit(EmitFailure),
}

struct Args {
    from: usize,
    count: usize,
    out: PathBuf,
    chunk: usize,
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    match run(&argv) {
        Ok(msg) => {
            println!("{msg}");
            ExitCode::SUCCESS
        }
        Err(Fail::Usage(m)) => {
            eprintln!("emit-migrations: {m}");
            eprintln!("{USAGE}");
            ExitCode::from(2)
        }
        // A named refusal: nothing was written. Exit 2 (an operator mistake),
        // distinct from an IO fault (exit 1).
        Err(Fail::Emit(EmitFailure::Refused(e))) => {
            eprintln!("emit-migrations: {e}");
            ExitCode::from(2)
        }
        Err(Fail::Emit(e @ EmitFailure::Io { .. })) => {
            eprintln!("emit-migrations: {e}");
            ExitCode::from(1)
        }
    }
}

fn run(argv: &[String]) -> Result<String, Fail> {
    let args = parse(argv)?;
    let outcome = emit_to_dir(args.from, args.count, args.chunk, &args.out).map_err(Fail::Emit)?;
    let mut lines = vec![format!(
        "emitted {} statement(s) {}..{} at {} per file, fp {}:",
        outcome.statements,
        outcome.from + 1,
        outcome.count,
        outcome.chunk,
        outcome.fingerprint
    )];
    for path in &outcome.files {
        lines.push(format!("  {}", path.display()));
    }
    lines.push(
        "apply the parts in order, then certify.sql LAST, with \
         `wrangler d1 execute low-overlay-db --remote --file <file>`"
            .to_string(),
    );
    Ok(lines.join("\n"))
}

fn parse(argv: &[String]) -> Result<Args, Fail> {
    let mut from: Option<usize> = None;
    let mut count: Option<usize> = None;
    let mut out: Option<PathBuf> = None;
    let mut chunk: Option<usize> = None;

    let mut it = argv.iter();
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--from" => from = Some(usize_val(&mut it, "--from")?),
            "--count" => count = Some(usize_val(&mut it, "--count")?),
            "--chunk" => chunk = Some(usize_val(&mut it, "--chunk")?),
            "--out" => {
                let v = it
                    .next()
                    .ok_or_else(|| Fail::Usage("--out needs a directory".to_string()))?;
                out = Some(PathBuf::from(v));
            }
            "-h" | "--help" => return Err(Fail::Usage("help".to_string())),
            other => return Err(Fail::Usage(format!("unknown argument {other}"))),
        }
    }

    Ok(Args {
        from: from.ok_or_else(|| Fail::Usage("--from is required".to_string()))?,
        count: count.unwrap_or(OVERLAY_MIGRATION_COUNT),
        out: out.ok_or_else(|| Fail::Usage("--out is required".to_string()))?,
        chunk: chunk.unwrap_or(DEFAULT_CHUNK),
    })
}

fn usize_val(it: &mut std::slice::Iter<'_, String>, flag: &str) -> Result<usize, Fail> {
    let v = it
        .next()
        .ok_or_else(|| Fail::Usage(format!("{flag} needs a number")))?;
    v.parse::<usize>()
        .map_err(|_| Fail::Usage(format!("{flag} wants a non-negative integer, got {v}")))
}
