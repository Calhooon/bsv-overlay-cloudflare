#!/usr/bin/env python3
"""d1-load-chunked.py (bsv-low #674, lane W1-LB-674-NOMEM): load a SQL file into a LOCAL D1 a batch of statements at a time.

wrangler 4.54's `d1 execute --local --file` hands the WHOLE file to miniflare's D1 object as ONE batch: one
`transactionSync` over every statement, each through its own `sql.prepare()`, whose compiled program lives until the
isolate's GC frees its handle. A statement's program carries the programs of every trigger it fires, so the batch's
memory is the sum of its statements' programs. Measured (native SQLite 3.53, `.stats`): an `INSERT INTO network_seen`
is 8,432 bytes prepared without the board's triggers and 178,368 with them (`pot_records` 120,128 / 282,656,
`potparty_records` 22,112 / 193,488). The 102,928-line fleet seed of `ci-d1-budget` then died `SQLITE_NOMEM` at about
1.1 GB of workerd RSS on a 10,000-line stretch of `network_seen` inserts, and loads at at most 707 MB in batches of
2,000 statements. The rows and bytes written are the same either way (the fleet seed holds no counted win).

A cut is made only where SQLite's own `sqlite3_complete` ends a statement (wrangler 4.148's splitter; a trigger body is
never cut), and each batch is one `wrangler d1 execute --local --file`, so each batch is its own transaction. The
triggers' per-statement cost is pinned on its own (`board_derived_sqlite.rs`,
`lb674_nomem_the_triggers_prepare_and_write_within_their_bound`), so a heavier trigger cannot hide behind the batching.

usage: d1-load-chunked.py <crate dir> <database> <persist dir> <sql file> [statements per batch, default 2000]
"""
import os
import sqlite3
import subprocess
import sys
import tempfile
import time


def batches(path, per_batch):
    """The file's statements, `per_batch` at a time, each batch the file's own text cut at a statement's end."""
    out, pending, count = [], [], 0
    with open(path, encoding="utf-8") as f:
        for line in f:
            pending.append(line)
            if sqlite3.complete_statement("".join(pending)):
                out.extend(pending)
                pending = []
                count += 1
                if count == per_batch:
                    yield count, "".join(out)
                    out, count = [], 0
    rest = "".join(out) + "".join(pending)
    if rest.strip():
        yield count, rest


def main():
    if len(sys.argv) not in (5, 6):
        sys.exit(__doc__.strip().splitlines()[-1])
    crate, db, persist, path = sys.argv[1:5]
    per_batch = int(sys.argv[5]) if len(sys.argv) == 6 else 2000
    total, t0 = 0, time.time()
    with tempfile.TemporaryDirectory(prefix="d1-load-") as tmp:
        for i, (n, text) in enumerate(batches(path, per_batch)):
            part = os.path.join(tmp, f"batch-{i:04d}.sql")
            with open(part, "w", encoding="utf-8") as f:
                f.write(text)
            r = subprocess.run(
                ["npx", "wrangler", "d1", "execute", db, "--local", "--persist-to", persist, "--file", part],
                cwd=crate, capture_output=True, text=True,
            )
            if r.returncode != 0:
                sys.stdout.write(r.stdout)
                sys.stderr.write(r.stderr)
                sys.exit(f"d1-load-chunked: batch {i} ({n} statements after {total}) did not load")
            total += n
    print(f"d1-load-chunked: {total} statements in {i + 1} batches of at most {per_batch}, {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
