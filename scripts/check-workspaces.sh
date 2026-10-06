#!/usr/bin/env bash
# check-workspaces.sh (bsv-low #553, 2026-10-06). Two refusals over the two cargo workspaces, run by `make ci-deploy`:
#   1. BOUNDS: the ROOT workspace (what a consumer pins by git rev) must load from this repository alone. ANY `path`
#      value in the root `Cargo.toml` or in a member's manifest (single or double quoted, any table: dependencies,
#      dev/build/target dependencies, `[workspace.dependencies]`, `[patch]`, `[lib]`...) that resolves, from its
#      manifest's directory, to a place outside the repository root is refused. Members are read from the root
#      `[workspace] members` (globs expanded), so a new member is scanned the day it is added.
#   2. MIRROR: `workers/Cargo.toml` repeats the root `[workspace.package]` and `[workspace.dependencies]` by hand (a
#      workspace cannot inherit another's table). Every entry present in BOTH must be identical.
# The manifests are PARSED (python3 >= 3.11 `tomllib`), never grepped, and no path needs to exist on this machine.
#   scripts/check-workspaces.sh           both checks; exit 1 on a refusal
set -euo pipefail
ROOT=$(git rev-parse --show-toplevel)
ROOT="$ROOT" python3 - <<'PY'
import glob, os, sys
try:
    import tomllib
except ImportError:
    sys.exit("✗ check-workspaces: needs python3 >= 3.11 (tomllib); found " + sys.version.split()[0])

root = os.path.realpath(os.environ["ROOT"])


def load(path):
    with open(path, "rb") as f:
        return tomllib.load(f)


def paths(node, trail=()):
    """Every string value under a key named `path`, with its dotted location."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "path" and isinstance(value, str):
                yield ".".join(trail), value
            else:
                yield from paths(value, trail + (key,))
    elif isinstance(node, list):
        for item in node:
            yield from paths(item, trail)


root_manifest = os.path.join(root, "Cargo.toml")
workspace = load(root_manifest).get("workspace", {})
manifests = [root_manifest]
for pattern in workspace.get("members", []):
    hits = sorted(glob.glob(os.path.join(root, pattern)))
    if not hits:
        sys.exit(f"✗ check-workspaces: root workspace member `{pattern}` matches no directory")
    manifests += [os.path.join(hit, "Cargo.toml") for hit in hits]

leaks = []
for manifest in manifests:
    here = os.path.dirname(manifest)
    for where, value in paths(load(manifest)):
        # realpath, so a symlink out of the repository is a leak too.
        target = os.path.realpath(os.path.join(here, value))
        if os.path.commonpath([root, target]) != root:
            leaks.append((os.path.relpath(manifest, root), where, value, target))

if leaks:
    print("✗ check-workspaces: the ROOT workspace must build from this repository alone")
    print("  (bsv-low #553: consumers pin the engine crates by git rev). No manifest of it")
    print("  may name a path that leaves the repository; such a crate belongs to")
    print("  workers/Cargo.toml. Found:")
    for manifest, where, value, target in leaks:
        print(f"    {manifest}: [{where}] path = {value!r} -> {target}")
    sys.exit(1)

workers_manifest = os.path.join(root, "workers", "Cargo.toml")
theirs = load(workers_manifest).get("workspace", {})
drift = []
for table in ("package", "dependencies"):
    ours, mirror = workspace.get(table, {}), theirs.get(table, {})
    for key in sorted(set(ours) & set(mirror)):
        if ours[key] != mirror[key]:
            drift.append((table, key, ours[key], mirror[key]))

if drift:
    print("✗ check-workspaces: workers/Cargo.toml no longer mirrors the root Cargo.toml.")
    print("  An entry present in both tables must be identical (bsv-low #553: the two")
    print("  workspaces cannot inherit from each other, so change BOTH). Differs:")
    for table, key, a, b in drift:
        print(f"    [workspace.{table}] {key}: root {a!r} != workers {b!r}")
    sys.exit(1)

print(f"check-workspaces: {len(manifests)} root manifests name no path outside the repository; "
      "workers/Cargo.toml mirrors the shared [workspace.package] and [workspace.dependencies] entries ✓")
PY
