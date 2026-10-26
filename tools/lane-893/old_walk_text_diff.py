#!/usr/bin/env python3
"""bsv-low #893, the D2-OWED delta lens's MEDIUM-3 gap: the old owed walk's regions in `routes.rs` (the
`owed_walk_*` macro_rules bodies, expanded over `worker` there and over SQLite in `old_walk_native.rs`) against main's
`routes.rs` at a base commit. Prints every line that differs; exits 1 if any differing line is more than an alias
edit (the names the regions resolve: `WalkDb`, `WalkDate`, `WalkEnv`, `walk_read_soft`, `walk_views`,
`walk_canonical_root`, `walk_counted_batch`, and the log macro `console_log!` unqualified) or the retired-walk `#[allow(dead_code)]` marks.

Run from the repository root: `python3 tools/lane-893/old_walk_text_diff.py 8e5e73f7`.
"""
import difflib, re, subprocess, sys

BASE = sys.argv[1] if len(sys.argv) > 1 else "8e5e73f7"
PATH = "crates/low-app-layer/src/routes.rs"
here = open(PATH).read().splitlines()
main = subprocess.run(["git", "show", f"{BASE}:{PATH}"], capture_output=True, text=True, check=True).stdout.splitlines()

# The alias edits, region text -> main's spelling (each may also be main's plain name).
ALIASES = [
    (r"\bWalkDb\b", r"(?:worker::)?D1Database"),
    (r"\bWalkDate\b", r"(?:worker::)?Date"),
    (r"\bWalkEnv\b", r"(?:worker::)?Env"),
    (r"\bwalk_read_soft\b", r"(?:crate::tracker_host::runtime::)?read_soft"),
    (r"\bwalk_views\b", r"(?:crate::tracker_host::runtime::)?views"),
    (r"\bwalk_canonical_root\b", r"(?:crate::beef_guard::)?canonical_root"),
    (r"\bwalk_counted_batch\b", r"(?:crate::d1_ledger::)?counted_batch"),
    # the log macro, resolved to the shim natively and to `worker`'s (imported at the top of `routes.rs`) there
    (r"\bconsole_log\b", r"(?:worker::)?console_log"),
]

def alias_only(new, old):
    pattern = re.escape(new.strip())
    for alias, spelled in ALIASES:
        pattern = re.sub(re.escape(alias[2:-2]), spelled.replace("\\", "\\\\"), pattern)
    return re.fullmatch(pattern, old.strip()) is not None

regions = []
i = 0
while i < len(here):
    m = re.match(r"macro_rules! (owed_walk_\w+) \{", here[i])
    if m:
        name, start = m.group(1), i + 2  # past `() => {`
        j = here.index(f"{name}!();", i)
        regions.append((name, here[start : j - 2]))  # before `    };` and `}`
        i = j
    i += 1

bad = 0
total = 0
for name, body in regions:
    lines = [l for l in body if l.strip() != "#[allow(dead_code)] // bsv-low #893: the retired owed walk (the belt replaced its alarm); lane 6 deletes it"]
    first = next(l for l in lines if l.strip())
    starts = [k for k, l in enumerate(main) if l.strip() == re.sub(r"\bWalkDb\b", "D1Database", first.strip()) or l.strip() == first.strip() or alias_only(first, l)]
    if not starts:
        print(f"{name}: first line not found in {BASE}: {first!r}")
        bad += 1
        continue
    k = starts[0]
    window = main[k : k + len(lines) + 40]
    sm = difflib.SequenceMatcher(a=lines, b=window, autojunk=False)
    blocks = [b for b in sm.get_matching_blocks() if b.size]
    end = blocks[-1].b + blocks[-1].size
    window = window[:end]
    differing = 0
    for tag, a0, a1, b0, b1 in difflib.SequenceMatcher(a=lines, b=window, autojunk=False).get_opcodes():
        if tag == "equal":
            continue
        new, old = lines[a0:a1], window[b0:b1]
        if tag == "replace" and len(new) == len(old) and all(alias_only(n, o) for n, o in zip(new, old)):
            for n, o in zip(new, old):
                print(f"{name}: ALIAS  -{o.strip()}\n{name}:        +{n.strip()}")
            differing += len(new)
            continue
        for o in old:
            print(f"{name}: LOGIC  -{o.strip()}")
        for n in new:
            print(f"{name}: LOGIC  +{n.strip()}")
        bad += 1
    total += len(lines)
    print(f"{name}: {len(lines)} lines, {differing} alias edits, main {BASE} lines {k + 1}..{k + end}")
print(f"regions {len(regions)}, lines {total}, logic differences {bad}")
sys.exit(1 if bad else 0)
