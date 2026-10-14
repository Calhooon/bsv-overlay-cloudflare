#!/usr/bin/env python3
"""No external writes, network or clock reads in repository topic managers.

The engine trait is excluded. Test modules are excluded. Zanaadu's executor
seam is outside this repository and must be scanned by its companion PR.
"""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN = re.compile(
    r"worker::Fetch|worker::Date::now|js_sys::Date::now|SystemTime::now"
    r"|env\s*\.\s*(?:d1|r2|kv|queue)\s*\("
    r"|ArcPfTxStatusChecker|HeaderPfProofVerifier"
    r"|\.(?:prepare|send_batch|send_raw|put|execute|batch)\s*\("
    r"|\b(?:INSERT\s+INTO|UPDATE\s+\w+\s+SET|DELETE\s+FROM)\b"
)


def violations(text):
    production = text.split("#[cfg(test)]", 1)[0]
    return [(i, line.strip()) for i, line in enumerate(production.splitlines(), 1)
            if FORBIDDEN.search(line) and not line.lstrip().startswith("//")]


def main():
    assert violations('worker::Fetch::Request(request).send().await')
    assert violations('env.d1("DB")?.prepare("INSERT INTO outputs VALUES (?)")')
    assert not violations('#[cfg(test)]\nworker::Fetch::Request(request)')
    paths = sorted((ROOT / 'crates').rglob('*topic_manager.rs'))
    paths = [p for p in paths if p != ROOT / 'crates/overlay-engine/src/topic_manager.rs']
    failures = [(path, line, text) for path in paths
                for line, text in violations(path.read_text())]
    for path, line, text in failures:
        print(f'{path.relative_to(ROOT)}:{line}: {text}')
    print(f'admission manager scan: {len(paths)} managers, {len(failures)} violations')
    return bool(failures)


if __name__ == '__main__':
    raise SystemExit(main())
