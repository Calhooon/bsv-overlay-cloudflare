# Releasing the overlay crates

1. Every change reaches protected `main` through a PR, with linear history and admins included.
2. Rebase-merge only with `test`, `fmt`, `clippy`, and `wasm32` green; the captain also runs the full `make ci` on the merge commit.
3. Integrate the callable L2-gate `ci.yml` before releasing 0.1.0; its baseline is `engine-e592-2026-10-10` plus #74's build object.
4. A release PR updates both workspace versions, compatible internal dependency versions and `CHANGELOG.md`; all five packages share the version. Rust 1.88 is the measured minimum (`rust-version`, checked by the gate's `msrv` job on every target); the other jobs build at 1.95.0.
5. Bootstrap the five new crates once from the captain's machine, in order: `bsv-overlay-engine`, `bsv-overlay-discovery`, `bsv-low-proof-replay`, `bsv-worker-build-info`, `bsv-overlay-cloudflare`; use `workers/Cargo.toml` for the latter three and wait for each registry index entry.
6. After each bootstrap publish, create its crates.io trusted publisher for owner `Calhooon`, repository `bsv-overlay-cloudflare`, workflow `release.yml`, no environment; hold the release workflow for the bootstrap `v0.1.0` tag push, then restore it so the version is not published twice.
7. Subsequent releases tag the merged commit `vX.Y.Z`; `.github/workflows/release.yml` checks workspace versions and main ancestry, calls the four CI jobs, dry-runs and publishes in dependency order, waiting for each index entry.
8. Subsequent publication uses OIDC trusted publishing only, with no stored repository token. Each crate job obtains its own short-lived token. Without its trusted publisher, authentication fails closed.
9. Publication intentionally omits `--locked` even though both workspace locks are committed: packaged path dependencies resolve from the registry; gates test the checkout locks and package verification tests the registry resolution.
10. Multi-crate publication is not atomic: after a partial release, preserve published versions. Rerun a failed job only if its crate version is still unpublished; if upload succeeded but indexing timed out, do not rerun that publish. Code fixes or recovery that would republish require a new release PR and version; yank a bad published version when appropriate.
