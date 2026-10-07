#!/usr/bin/env python3
"""check-storage-ownership.py (bsv-low #474): every SQL statement of the LOW crates against storage-ownership.json.

The overlay and the app layer share ONE D1 per environment (`OVERLAY_DB`). Who may write and read which table is
stated in `storage-ownership.json` (prose: `docs/STORAGE-OWNERSHIP.md`). This check reads every Rust string literal
of the scanned crates (production code only: `tests/`, `examples/`, `benches/` and `#[cfg(test)]` items are
skipped), finds the SQL in it, and refuses:

  * a WRITE (INSERT / UPDATE / DELETE / REPLACE / CREATE / ALTER / DROP) to a table the crate holds no grant for;
  * a READ (FROM / JOIN) of a table the crate is not granted (a writer may read what it writes);
  * a table the manifest does not list (so a migration that adds a table must add a row);
  * a dynamic table name (`FROM {table}`, `INSERT INTO "{}"`, a keyword ending its literal) not pinned in the
    manifest's `dynamic_sites` with its count, and a pin whose count no longer matches;
  * a manifest row of the shared D1 that nothing creates, and a non-owner grant nothing exercises (stale);
  * a manifest table with no row in docs/STORAGE-OWNERSHIP.md.

Each red names the file, the line and the table. One hit may be allowed in place by a comment on the literal's first
line, or on the line above it: `// storage-ok(<table>): <reason>` (the reason is required).

LIMITS (each pinned by --self-test, so a change in what the parser sees is a red here, not a surprise):
  1. String-built SQL: only SQL inside a string literal is seen. A table name that is not in the same literal as its
     keyword is a DYNAMIC site (pinned by count, its tables listed by hand), never resolved through a const or arg.
  2. Reads are seen by the UPPERCASE keywords FROM and JOIN only (English text in messages would flood a
     case-blind match); writes are matched case-blind but need their full shape (`INSERT INTO t`, `UPDATE t SET`).
  3. A comma join is followed (`FROM a x, b y`); a table named only inside a subquery's own FROM is seen as usual.
     CTE names (`name AS (`) defined anywhere in the same file are not tables.
  4. Only `.rs` and `.sql` files under the scanned crates' trees are read; SQL a worker receives at run time
     (none today) is out of reach.
  5. `#[cfg(test)]` is recognised on an item whose body is a brace block or ends at `;`; a test helper outside
     such an item (a `pub fn` used only by tests) is scanned as production code.

  python3 scripts/check-storage-ownership.py              check the tree; exit 1 on any red
  python3 scripts/check-storage-ownership.py --self-test  fixtures only; run by `make ci`
  python3 scripts/check-storage-ownership.py --inventory  print every (crate, op, table) the tree issues
"""
import json
import os
import re
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MANIFEST = "storage-ownership.json"
DOC = os.path.join("docs", "STORAGE-OWNERSHIP.md")
WRITE_OPS = ("INSERT", "UPDATE", "DELETE", "REPLACE", "CREATE", "ALTER", "DROP")
SKIP_DIRS = {"tests", "examples", "benches", "target", "node_modules", ".wrangler"}
IDENT = r'[ \t\n"`\[]*([A-Za-z_][A-Za-z0-9_]*)?'

# (op, regex). The table token follows the match; a missing identifier there is a dynamic site.
PATTERNS = [
    ("INSERT", re.compile(r"\bINSERT(?:\s+OR\s+[A-Za-z]+)?\s+INTO\b", re.I)),
    ("REPLACE", re.compile(r"(?<!OR )\bREPLACE\s+INTO\b", re.I)),
    ("UPDATE", re.compile(r"\bUPDATE(?:\s+OR\s+[A-Za-z]+)?(?=\s+[\"`\[]?[A-Za-z_{][^\s]*\s+SET\b)", re.I)),
    ("DELETE", re.compile(r"\bDELETE\s+FROM\b", re.I)),
    ("CREATE", re.compile(r"\bCREATE\s+(?:VIRTUAL\s+)?TABLE(?:\s+IF\s+NOT\s+EXISTS)?\b", re.I)),
    ("CREATE", re.compile(r"\bCREATE\s+(?:UNIQUE\s+)?INDEX(?:\s+IF\s+NOT\s+EXISTS)?\s+[A-Za-z_][A-Za-z0-9_]*\s+ON\b", re.I)),
    ("ALTER", re.compile(r"\bALTER\s+TABLE\b", re.I)),
    ("DROP", re.compile(r"\bDROP\s+(?:TABLE|INDEX)(?:\s+IF\s+EXISTS)?\b", re.I)),
    ("SELECT", re.compile(r"\b(?:FROM|JOIN)\b")),
]
ALLOW = re.compile(r"//\s*storage-ok\(([A-Za-z0-9_]+)\):\s*\S")
CTE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s+AS\s+(?:NOT\s+)?(?:MATERIALIZED\s+)?\(", re.I)
# SQLite's own names and table-valued functions a FROM may name.
BUILTIN = {"sqlite_master", "sqlite_schema", "sqlite_sequence", "pragma_table_info", "json_each", "json_tree"}


# ---------------------------------------------------------------- the lexer

def lex(src):
    """Return (literals, code): every string literal as (start line, text with positions kept), and the source
    with comments and literal bodies blanked to spaces (newlines kept), for the cfg(test) brace walk."""
    lits, code, i, n, line = [], list(src), 0, len(src), 1

    def blank(a, b):
        for k in range(a, b):
            if code[k] != "\n":
                code[k] = " "

    while i < n:
        c = src[i]
        if c == "\n":
            line += 1
            i += 1
        elif src.startswith("//", i):
            j = src.find("\n", i)
            j = n if j < 0 else j
            blank(i, j)
            i = j
        elif src.startswith("/*", i):
            d, j = 1, i + 2
            while j < n and d:
                if src.startswith("/*", j):
                    d, j = d + 1, j + 2
                elif src.startswith("*/", j):
                    d, j = d - 1, j + 2
                else:
                    j += 1
            blank(i, j)
            line += src.count("\n", i, j)
            i = j
        elif (c in "rb") and (i == 0 or not (src[i - 1].isalnum() or src[i - 1] == "_")) and re.match(r'b?r#*"|b"', src[i:i + 64]):
            m = re.match(r'b?r(#*)"', src[i:i + 64])
            if m:
                h = m.group(1)
                s = i + m.end()
                e = src.find('"' + h, s)
                e = n if e < 0 else e
                text = src[s:e]
                nxt = e + 1 + len(h)
            else:
                s, e = scan_quoted(src, i + 2)
                text = cook(src[s:e])
                nxt = e + 1
            lits.append((line, text))
            blank(i, nxt)
            line += src.count("\n", i, nxt)
            i = nxt
        elif c == '"':
            s, e = scan_quoted(src, i + 1)
            lits.append((line, cook(src[s:e])))
            blank(i, e + 1)
            line += src.count("\n", i, e + 1)
            i = e + 1
        elif c == "'":
            m = re.match(r"'(?:\\(?:x[0-9a-fA-F]{2}|u\{[0-9a-fA-F]+\}|.)|[^\\'\n])'", src[i:i + 16])
            if m:
                blank(i, i + m.end())
                i += m.end()
            else:
                i += 1  # a lifetime
        else:
            i += 1
    return lits, "".join(code)


def scan_quoted(src, s):
    j = s
    while j < len(src) and src[j] != '"':
        j += 2 if src[j] == "\\" else 1
    return s, j


def cook(raw):
    """A plain literal's text with every position kept: an escaped quote reads as a quote, a line continuation's
    backslash as a space."""
    out = list(raw)
    for k, ch in enumerate(raw):
        if ch == "\\" and k + 1 < len(raw) and raw[k + 1] in '"\n\\':
            out[k] = " "
    return "".join(out)


def test_spans(code):
    """Byte spans of every `#[cfg(test)]` (or `#[cfg(all(test, ...))]`) item in the blanked code."""
    spans = []
    for m in re.finditer(r"#\[cfg\((?:all\(\s*)?test\b[^\]]*\]", code):
        j, depth = m.end(), 0
        while j < len(code):
            ch = code[j]
            if ch == ";" and depth == 0:
                j += 1
                break
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    j += 1
                    break
            j += 1
        spans.append((m.start(), j))
    return spans


def line_starts(src):
    starts, k = [0], 0
    while True:
        k = src.find("\n", k)
        if k < 0:
            return starts
        k += 1
        starts.append(k)


# ---------------------------------------------------------------- extraction

def hits_in(text):
    """(offset, op, table or None, token) for every SQL reference in one literal; None is a dynamic site."""
    out = []
    for op, rx in PATTERNS:
        for m in rx.finditer(text):
            rest = text[m.end():]
            t = re.match(IDENT, rest)
            name = t.group(1) if t else None
            after = rest[t.end():] if t else rest
            token = rest.strip()[:24].split("\n")[0]
            if op == "SELECT":
                if re.search(r"\bDELETE\s+$", text[:m.start()], re.I):
                    continue  # the FROM of a DELETE, a write seen above
                if name is None:
                    if rest.strip() == "" or rest.lstrip(' \t\n"`[').startswith("{"):
                        out.append((m.start(), op, None, token))
                    continue  # a subquery `FROM (`, or prose
                if after.startswith("("):
                    continue  # a table-valued function
                out.append((m.start(), op, name, token))
                # a comma join: `FROM a x, b y`
                tail = after
                while True:
                    c = re.match(r"(?:\s+(?:AS\s+)?(?!WHERE\b|ON\b|JOIN\b|LEFT\b|INNER\b|CROSS\b|GROUP\b|ORDER\b|LIMIT\b|UNION\b)[A-Za-z_][A-Za-z0-9_]*)?\s*,\s*([A-Za-z_][A-Za-z0-9_]*)\b(?!\s*\()", tail)
                    if not c:
                        break
                    out.append((m.start(), op, c.group(1), token))
                    tail = tail[c.end():]
                continue
            if name is None or after.startswith("{"):
                out.append((m.start(), op, None, token))
            else:
                out.append((m.start(), op, name, token))
    return out


def scan_crate(crate_dir, root=ROOT):
    """Every hit of one crate: dicts with file, line, op, table (None if dynamic), token, allow."""
    hits = []
    for dp, dn, fn in os.walk(crate_dir):
        dn[:] = sorted(d for d in dn if d not in SKIP_DIRS)
        for f in sorted(fn):
            if not (f.endswith(".rs") or f.endswith(".sql")):
                continue
            path = os.path.join(dp, f)
            hits.extend(scan_file(path, os.path.relpath(path, root)))
    return hits


def scan_file(path, rel=None):
    src = open(path, encoding="utf-8").read()
    rel = rel or os.path.relpath(path, ROOT)
    if path.endswith(".sql"):
        lits, spans = [(1, src)], []
    else:
        lits, code = lex(src)
        spans = test_spans(code)
    starts = line_starts(src)
    src_lines = src.split("\n")
    ctes = set()
    for _, text in lits:
        ctes.update(m.group(1) for m in CTE.finditer(text))
    out = []
    for lit_line, text in lits:
        off = starts[lit_line - 1] if lit_line - 1 < len(starts) else 0
        if any(a <= off < b for a, b in spans):
            continue
        allows = set()
        for ln in (lit_line - 1, lit_line):
            if 1 <= ln <= len(src_lines):
                allows.update(m.group(1) for m in ALLOW.finditer(src_lines[ln - 1]))
        for pos, op, table, token in hits_in(text):
            if table is not None and (table in ctes or table in BUILTIN):
                continue
            out.append({"file": rel, "line": lit_line + text.count("\n", 0, pos), "op": op,
                        "table": table, "token": token, "allow": table in allows if table else False})
    return out


# ---------------------------------------------------------------- the judgement

def judge(man, hits_by_crate):
    reds, notes = [], []
    tables = {t["name"]: t for t in man["tables"]}
    not_tables = set(man.get("not_tables", {}))
    exercised = set()  # (crate, table, kind)
    created = set()
    dyn_count = {}

    def granted(crate, table, op):
        t = tables[table]
        if crate == t["owner"]:
            return True
        if crate == man.get("schema_owner") and op in ("CREATE", "ALTER", "DROP") and t["database"] == man["shared_database"]:
            return True  # the migrations own the schema of every shared table
        g = t.get("writers", {}).get(crate, [])
        if op == "SELECT":
            return crate in t.get("readers", []) or bool(g)
        return op in g

    for crate, hits in hits_by_crate.items():
        for h in hits:
            where = f'{h["file"]}:{h["line"]}'
            if h["table"] is None:
                key = (h["file"], h["op"])
                dyn_count[key] = dyn_count.get(key, 0) + 1
                continue
            name = h["table"]
            if name in not_tables:
                continue
            if h["allow"]:
                notes.append(f"allowed in place: {where} {h['op']} {name}")
                continue
            if name not in tables:
                reds.append(f"{where}: table `{name}` ({h['op']}) is not in {MANIFEST}: add its row (owner, readers, rebuild class, never-wipe)")
                continue
            if h["op"] == "CREATE":
                created.add(name)
            kind = "read" if h["op"] == "SELECT" else h["op"]
            if not granted(crate, name, h["op"]):
                what = "reads" if h["op"] == "SELECT" else f"writes ({h['op']})"
                reds.append(f"{where}: crate `{crate}` {what} table `{name}`, owned by `{tables[name]['owner']}`, and the manifest grants it no such access")
            exercised.add((crate, name, kind))

    # dynamic sites: pinned by (file, op) count, their tables granted like any statement
    pins = {(d["file"], d["op"]): d for d in man.get("dynamic_sites", [])}
    for key, n in sorted(dyn_count.items()):
        d = pins.get(key)
        if d is None:
            reds.append(f"{key[0]}: {n} dynamic-table {key[1]} site(s) not pinned in {MANIFEST} dynamic_sites (name the tables it reaches)")
        elif d["count"] != n:
            reds.append(f"{key[0]}: {n} dynamic-table {key[1]} site(s), pinned {d['count']}: re-read the sites and re-pin")
    for key, d in pins.items():
        if key not in dyn_count:
            reds.append(f"{key[0]}: dynamic_sites pins {d['count']} {key[1]} site(s) and the file has none: drop the stale pin")
            continue
        for name in d["tables"]:
            if name not in tables:
                reds.append(f"{key[0]}: dynamic {key[1]} reaches `{name}`, not in {MANIFEST}")
                continue
            if key[1] == "CREATE":
                created.add(name)
            if not granted(d["crate"], name, key[1]):
                reds.append(f"{key[0]}: dynamic {key[1]} by `{d['crate']}` reaches `{name}`, owned by `{tables[name]['owner']}`, with no grant")
            exercised.add((d["crate"], name, "read" if key[1] == "SELECT" else key[1]))

    scanned = set(hits_by_crate)
    for name, t in tables.items():
        if t["database"] == man["shared_database"] and name not in created:
            reds.append(f"{MANIFEST}: table `{name}` is listed and nothing in the scanned crates creates it: a stale row")
        for crate, ops in t.get("writers", {}).items():
            if crate == t["owner"] or crate not in scanned:
                continue
            for op in ops:
                if (crate, name, op) not in exercised:
                    reds.append(f"{MANIFEST}: `{name}` grants `{crate}` {op} and no statement of that crate issues it: a stale grant")
        for crate in t.get("readers", []):
            if crate in scanned and not any((crate, name, k) in exercised for k in ("read",) + WRITE_OPS):
                reds.append(f"{MANIFEST}: `{name}` lists `{crate}` as a reader and no statement of that crate reads it: a stale grant")
    return reds, notes


def load_manifest(path):
    man = json.load(open(path, encoding="utf-8"))
    for t in man["tables"]:
        for k in ("name", "database", "owner", "readers", "rebuild_class", "never_wipe", "why"):
            if k not in t:
                raise SystemExit(f"{path}: table row {t.get('name')} lacks `{k}`")
    return man


def run(root=ROOT, manifest=None):
    man = load_manifest(manifest or os.path.join(root, MANIFEST))
    hits = {c: scan_crate(os.path.join(root, p), root) for c, p in man["scanned_crates"].items()}
    reds, notes = judge(man, hits)
    doc = open(os.path.join(root, DOC), encoding="utf-8").read()
    for t in man["tables"]:
        if f"| `{t['name']}` |" not in doc:
            reds.append(f"{DOC}: table `{t['name']}` has a row in {MANIFEST} and none on the page")
    return (reds, notes), hits


# ---------------------------------------------------------------- self-test

FIX_MAN = {
    "shared_database": "db",
    "scanned_crates": {"owner-crate": "own", "guest": "guest"},
    "not_tables": {},
    "tables": [
        {"name": "owned", "database": "db", "owner": "owner-crate", "writers": {}, "readers": ["guest"],
         "rebuild_class": "chain", "never_wipe": False, "why": "fixture"},
        {"name": "shared", "database": "db", "owner": "owner-crate", "writers": {"guest": ["INSERT"]}, "readers": [],
         "rebuild_class": "filings", "never_wipe": True, "why": "fixture"},
    ],
    "dynamic_sites": [],
}
OWN_SRC = 'const A: &str = "CREATE TABLE IF NOT EXISTS owned (k TEXT)";\nconst B: &str = "CREATE TABLE shared (k TEXT)";\n'
GUEST_SRC = 'fn f() { q("SELECT k FROM owned WHERE k = ?"); q("INSERT INTO shared (k) VALUES (?)"); }\n'


def self_test():
    ok, bad = 0, 0

    def check(name, cond):
        nonlocal ok, bad
        if cond:
            ok += 1
        else:
            bad += 1
            print(f"  FAIL: {name}")

    def tree(guest_extra="", man_edit=None, own_extra=""):
        d = tempfile.mkdtemp()
        for c, s in (("own", OWN_SRC + own_extra), ("guest", GUEST_SRC + guest_extra)):
            os.makedirs(os.path.join(d, c, "src"))
            open(os.path.join(d, c, "src", "lib.rs"), "w").write(s)
        m = json.loads(json.dumps(FIX_MAN))
        if man_edit:
            man_edit(m)
        json.dump(m, open(os.path.join(d, MANIFEST), "w"))
        os.makedirs(os.path.join(d, "docs"))
        open(os.path.join(d, DOC), "w").write("".join(f"| `{t['name']}` |\n" for t in m["tables"] if t["name"] != "undocumented"))
        (reds, notes), hits = run(d)
        return reds, notes, hits

    reds, _, _ = tree()
    check("the clean fixture is green", reds == [])
    # THE PLANTED VIOLATION: a guest writes into a table it only reads.
    reds, _, _ = tree('fn g() { q("UPDATE owned SET k = ? WHERE k = ?"); }\n')
    check("a write to a table the crate does not own is red, naming file, line and table",
          len(reds) == 1 and "guest/src/lib.rs:2" in reds[0] and "`owned`" in reds[0] and "UPDATE" in reds[0])
    # THE ANNOTATED ALLOW: the same write with a reasoned in-place allow is green.
    reds, notes, _ = tree('fn g() {\n    // storage-ok(owned): fixture, the planted allow\n    q("UPDATE owned SET k = ? WHERE k = ?");\n}\n')
    check("an annotated allow is green and noted", reds == [] and any("owned" in n for n in notes))
    reds, _, _ = tree('fn g() {\n    // storage-ok(owned):\n    q("UPDATE owned SET k = ?");\n}\n')
    check("an allow without a reason is not an allow", len(reds) == 1)
    reds, _, _ = tree('fn g() { q("SELECT * FROM shared"); }\n',
                      lambda m: m["tables"][1]["writers"].clear())
    check("a read the manifest does not grant is red", any("reads table `shared`" in r for r in reds))
    reds, _, _ = tree(own_extra='const C: &str = "CREATE TABLE added (k TEXT)";\n')
    check("a new table without a manifest row is red", any("`added`" in r and "not in" in r for r in reds))
    reds, _, _ = tree(man_edit=lambda m: m["tables"].append(dict(m["tables"][0], name="ghost")))
    check("a manifest row nothing creates is red (stale)", any("`ghost`" in r and "stale row" in r for r in reds))
    reds, _, _ = tree(own_extra='const D: &str = "CREATE TABLE undocumented (k TEXT)";\n',
                      man_edit=lambda m: m["tables"].append(dict(m["tables"][0], name="undocumented", readers=[])))
    check("a manifest row the page does not name is red", reds == [f"{DOC}: table `undocumented` has a row in {MANIFEST} and none on the page"])
    m2 = lambda m: (m.update(schema_owner="guest"), m["tables"][0]["readers"].clear())
    reds, _, _ = tree('fn s() { q("ALTER TABLE owned ADD COLUMN x INTEGER"); }\n', m2)
    check("the schema owner may ALTER a table it does not own, and still not read it", len(reds) == 1 and "reads table `owned`" in reds[0])
    reds, _, _ = tree(man_edit=lambda m: m["tables"][1]["writers"]["guest"].append("DELETE"))
    check("an unexercised non-owner write grant is red (stale)", any("stale grant" in r for r in reds))
    # LIMIT 1: string-built SQL is a dynamic site, pinned by count.
    dyn = 'fn h(t: &str) { q(format!("DELETE FROM {t} WHERE k = ?")); q(format!("SELECT k FROM {}", t)); }\n'
    reds, _, _ = tree(dyn)
    check("an unpinned dynamic table name is red", sum("not pinned" in r for r in reds) == 2)
    pin = lambda m: m["dynamic_sites"].extend([
        {"file": "guest/src/lib.rs", "op": "DELETE", "count": 1, "crate": "guest", "tables": ["owned"], "why": "fixture"},
        {"file": "guest/src/lib.rs", "op": "SELECT", "count": 1, "crate": "guest", "tables": ["owned"], "why": "fixture"}])
    reds, _, _ = tree(dyn, pin)
    check("a pinned dynamic site still needs its tables granted", len(reds) == 1 and "DELETE" in reds[0] and "`owned`" in reds[0])
    reds, _, _ = tree(dyn + 'fn i(t: &str) { q(format!("SELECT 1 FROM {t}")); }\n', pin)
    check("a dynamic site past its pinned count is red", any("pinned 1" in r for r in reds))
    split = 'fn j() { q(String::from("SELECT k FROM ") + T); }\n'
    _, _, hits = tree(split)
    check("a keyword ending its literal is a dynamic site", any(h["table"] is None for h in hits["guest"]))
    # LIMIT 2: lowercase reads are unseen, lowercase writes are seen.
    _, _, hits = tree('fn k() { q("select k from owned"); q("update owned set k = 1"); }\n')
    ops = sorted((h["op"], h["table"]) for h in hits["guest"])
    check("a lowercase read is unseen (stated limit), a lowercase write is seen",
          ("SELECT", "owned") in ops and ops.count(("SELECT", "owned")) == 1 and ("UPDATE", "owned") in ops)
    # LIMIT 3: comma joins followed, CTE names not tables, DO UPDATE SET not a statement.
    _, _, hits = tree('fn l() { q("WITH w AS (SELECT 1) SELECT * FROM w, owned o, shared s"); q("INSERT INTO shared (k) VALUES (?) ON CONFLICT(k) DO UPDATE SET k = excluded.k"); }\n')
    names = [(h["op"], h["table"]) for h in hits["guest"]]
    check("a comma join is followed and a CTE is not a table",
          ("SELECT", "shared") in names and ("SELECT", "w") not in names and names.count(("SELECT", "owned")) == 2)
    check("an upsert's DO UPDATE SET is not an UPDATE", ("UPDATE", "excluded") not in names and ("UPDATE", "SET") not in names
          and not any(op == "UPDATE" for op, _ in names))
    # LIMIT 5 and the comment/test skips.
    _, _, hits = tree('// "DELETE FROM owned" in a comment\n/* "DROP TABLE owned" */\n#[cfg(test)]\nmod t { fn f() { q("DELETE FROM owned"); } }\n#[cfg(test)]\nconst T: &str = "DROP TABLE owned";\n')
    check("comments and #[cfg(test)] items are not scanned", not any(h["op"] in ("DELETE", "DROP") for h in hits["guest"]))
    _, _, hits = tree("fn m() { q(r#\"DELETE FROM \"owned\" WHERE k = '}'\"#); q(\"INSERT INTO \\\"shared\\\" (k) VALUES (1)\"); let c = '\"'; }\n")
    got = sorted((h["op"], h["table"]) for h in hits["guest"])
    check("raw strings, quoted names and char literals lex right", ("DELETE", "owned") in got and ("INSERT", "shared") in got)
    print(f"check-storage-ownership self-test: {ok} ok, {bad} failed")
    return bad == 0


def main(argv):
    if argv[1:] == ["--self-test"]:
        return 0 if self_test() else 1
    if argv[1:] == ["--inventory"]:
        _, hits = run()
        rows = {}
        for crate, hs in hits.items():
            for h in hs:
                k = (crate, h["op"], h["table"] or f"<dynamic {h['file']}>")
                rows[k] = rows.get(k, 0) + 1
        for (c, op, t), n in sorted(rows.items()):
            print(f"{c}\t{op}\t{t}\t{n}")
        return 0
    if argv[1:]:
        print("usage: check-storage-ownership.py [--self-test|--inventory]", file=sys.stderr)
        return 2
    (reds, notes), hits = run()
    n = sum(len(h) for h in hits.values())
    for note in notes:
        print(f"  note: {note}")
    if reds:
        print(f"✗ check-storage-ownership: {len(reds)} red(s) over {n} SQL references:")
        for r in reds:
            print(f"  {r}")
        return 1
    print(f"check-storage-ownership: {n} SQL references in {len(hits)} crates match {MANIFEST} ✓")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
