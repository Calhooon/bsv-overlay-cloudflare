#!/usr/bin/env python3
"""bsv-low #499: the D1 census verb, "measured before every promotion".

Reads the D1 rows ledger a stack serves (the overlay's `/health/invariants.d1Budget`, the app layer's
`/health.d1Budget`: per-route running maxima of one request's `meta.rows_read` / `rows_written` / statements,
per isolate since boot) and prints the table the promotion checklist carries (docs/D1-BUDGETS.md). Each route
is set beside the CI ceiling of its scenario (`tools/lane-499/ceilings.json`), ADVISORY here: production rows are
not the fixture's, so a route over its fixture ceiling is a line to read, not a refusal. Python 3 stdlib only.

    scripts/d1-census.py --overlay https://low-overlay-beta.example --app https://low-app-layer-beta.example
    scripts/d1-census.py --app http://127.0.0.1:8800 --json
    scripts/d1-census.py --self-test        (canned bodies, no network)

A ledger is ONE isolate's: the figures are what the isolate that answered has served since it booted. Sample a
few times (`--samples N`) to see more isolates; the table keeps each route's largest maximum and sums requests.
The view actor's compute (`/results:view`, `/leaderboard:view`) is recorded by the isolate that forwarded to it.
"""

import argparse
import json
import os
import sys
import urllib.request

CEILINGS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tools", "lane-499", "ceilings.json")
SURFACES = {"overlay": "/health/invariants", "app": "/health"}


def fetch_budget(base, path, timeout):
    req = urllib.request.Request(base.rstrip("/") + path, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = json.loads(r.read().decode("utf-8"))
    budget = body.get("d1Budget")
    if not isinstance(budget, dict):
        raise ValueError(f"{base}{path} serves no d1Budget (a build before bsv-low #499?)")
    return budget


def merge(into, budget):
    """Fold one sample into the running table: per route the largest maxima, requests summed."""
    for route, b in (budget.get("routes") or {}).items():
        cur = into.setdefault(route, {"requests": 0, "max": {"reads": 0, "writes": 0, "stmts": 0}, "samples": 0})
        cur["requests"] += int(b.get("requests", 0))
        cur["samples"] += 1
        for k in ("reads", "writes", "stmts"):
            cur["max"][k] = max(cur["max"][k], int((b.get("max") or {}).get(k, 0)))
    un = budget.get("unscoped") or {}
    u = into.setdefault("(unscoped)", {"requests": 0, "max": {"reads": 0, "writes": 0, "stmts": 0}, "samples": 0})
    u["samples"] += 1
    for k in ("reads", "writes", "stmts"):
        u["max"][k] = max(u["max"][k], int(un.get(k, 0)))


def ceilings_by_route(path):
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except OSError:
        return {}
    out = {}
    for name, s in (doc.get("scenarios") or {}).items():
        key = s.get("censusKey")
        if key:
            out[key] = (name, s.get("ceiling") or {})
    return out


def table(surface, routes, ceilings):
    lines = [
        f"### {surface}",
        "",
        "| route | requests | max reads | max writes | max stmts | CI ceiling (scenario: reads/writes/stmts) | note |",
        "|---|---:|---:|---:|---:|---|---|",
    ]
    for route in sorted(routes):
        r = routes[route]
        m = r["max"]
        ceil = ceilings.get(route)
        if ceil:
            name, c = ceil
            cell = f"{name}: {c.get('reads')}/{c.get('writes')}/{c.get('stmts')}"
            over = [k for k in ("reads", "writes", "stmts") if m[k] > int(c.get(k, 0))]
            note = ("over the fixture ceiling: " + ", ".join(over)) if over else ""
        else:
            cell, note = "", ""
        if route == "(unscoped)":
            note = "after-answer work of the isolate (wait_until), not a route's; the figure is its total"
        lines.append(f"| `{route}` | {r['requests']} | {m['reads']} | {m['writes']} | {m['stmts']} | {cell} | {note} |")
    lines.append("")
    return "\n".join(lines)


def self_test():
    body = {
        "routes": {
            "/owed": {"requests": 3, "max": {"reads": 600, "writes": 18, "stmts": 26}, "last": {}, "total": {}},
            "/utxo-status": {"requests": 9, "max": {"reads": 8, "writes": 0, "stmts": 1}, "last": {}, "total": {}},
        },
        "unscoped": {"reads": 40, "writes": 2, "stmts": 5},
    }
    routes = {}
    merge(routes, body)
    merge(routes, {"routes": {"/owed": {"requests": 1, "max": {"reads": 10, "writes": 30, "stmts": 2}}}, "unscoped": {}})
    assert routes["/owed"]["requests"] == 4, routes
    assert routes["/owed"]["max"] == {"reads": 600, "writes": 30, "stmts": 26}, routes
    ceilings = {"/owed": ("owed-first-read", {"reads": 528, "writes": 20, "stmts": 28})}
    out = table("app", routes, ceilings)
    assert "| `/owed` | 4 | 600 | 30 | 26 | owed-first-read: 528/20/28 | over the fixture ceiling: reads, writes |" in out, out
    assert "| `/utxo-status` | 9 | 8 | 0 | 1 |  |  |" in out, out
    assert "(unscoped)" in out
    real = ceilings_by_route(CEILINGS)
    assert real.get("/owed", ("",))[0] == "owed-first-read", real
    print("d1-census self-test: ok")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--overlay", help="the overlay's base URL (reads /health/invariants)")
    ap.add_argument("--app", help="the app layer's base URL (reads /health)")
    ap.add_argument("--samples", type=int, default=1, help="reads per surface (each may land on another isolate)")
    ap.add_argument("--timeout", type=float, default=15.0)
    ap.add_argument("--ceilings", default=CEILINGS)
    ap.add_argument("--json", action="store_true", help="print the merged figures as JSON")
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args()
    if a.self_test:
        self_test()
        return 0
    if not a.overlay and not a.app:
        ap.error("name at least one of --overlay / --app")
    ceilings = ceilings_by_route(a.ceilings)
    merged = {}
    for surface, base in (("overlay", a.overlay), ("app", a.app)):
        if not base:
            continue
        routes = {}
        for _ in range(max(1, a.samples)):
            merge(routes, fetch_budget(base, SURFACES[surface], a.timeout))
        merged[surface] = routes
    if a.json:
        print(json.dumps(merged, indent=2, sort_keys=True))
        return 0
    print("## D1 census (bsv-low #499): per-route maxima of one request, per isolate since boot")
    print("")
    for surface, routes in merged.items():
        print(table(surface, routes, ceilings))
    return 0


if __name__ == "__main__":
    sys.exit(main())
