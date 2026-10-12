#!/usr/bin/env python3
"""Refuse a tag unless both workspaces and all release packages agree."""
import os
from pathlib import Path
import tomllib


def read(path):
    return tomllib.loads(Path(path).read_text())


version = read("Cargo.toml")["workspace"]["package"]["version"]
assert read("workers/Cargo.toml")["workspace"]["package"]["version"] == version
for crate in ("overlay-engine", "overlay-discovery", "low-proof-replay", "worker-build-info", "overlay-cloudflare"):
    package = read(f"crates/{crate}/Cargo.toml")["package"]
    value = package["version"]
    assert value == {"workspace": True} or value == version, crate
assert os.environ["GITHUB_REF_NAME"] == f"v{version}", "tag must equal workspace version"
print(f"release version: {version}; both workspaces and five packages agree")
