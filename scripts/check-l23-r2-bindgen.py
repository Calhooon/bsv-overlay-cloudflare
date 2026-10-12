#!/usr/bin/env python3
"""L23R2D-L2: the deployable bindgen family follows main, never a host CLI."""
import subprocess
import tomllib
from pathlib import Path

main = tomllib.loads(subprocess.check_output(
    ["git", "show", "origin/main:workers/Cargo.lock"], text=True))
held = tomllib.loads(Path("workers/Cargo.lock").read_text())
family = lambda lock: {p["name"]: p["version"] for p in lock["package"]
                       if p["name"].startswith("wasm-bindgen") or
                       p["name"] in ("js-sys", "web-sys")}
assert family(held) == family(main), "bindgen family differs from main"
print("L23R2D-L2 bindgen family equals main: GREEN")
