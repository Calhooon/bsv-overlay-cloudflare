# bsv-overlay-cloudflare

A BSV overlay engine, discovery plugins and Cloudflare Workers, written in Rust.
The engine handles submission, lookup and GASP synchronization through storage,
topic-manager, lookup-service and broadcaster traits. The Worker supplies HTTP,
D1 storage, queues and scheduled work. Compatibility witnesses live in
[parity-harness](parity-harness); they do not establish universal wire parity.

## Install and workspace layout

The five published crates are all **0.1.0**, on **bsv-rs 0.4.5**. Add the crate
needed by your application from its Cargo project directory:

```sh
cargo add bsv-overlay-engine@=0.1.0
cargo add bsv-overlay-discovery@=0.1.0
cargo add bsv-low-proof-replay@=0.1.0
cargo add bsv-worker-build-info@=0.1.0
cargo add bsv-overlay-cloudflare@=0.1.0
```

| Crate | Role | Source |
|---|---|---|
| bsv-overlay-engine | Platform-independent engine and traits | [engine](crates/overlay-engine) |
| bsv-overlay-discovery | SHIP, SLAP and other discovery plugins | [discovery](crates/overlay-discovery) |
| bsv-low-proof-replay | Proof replay used by the Workers | [replay](crates/low-proof-replay) |
| bsv-worker-build-info | Shared build provenance | [build info](crates/worker-build-info) |
| bsv-overlay-cloudflare | Cloudflare adapter | [Worker](crates/overlay-cloudflare) |

There are two workspaces and two lockfiles. [Cargo.toml](Cargo.toml) contains
the engine, discovery, integration tests and parity harness.
[workers/Cargo.toml](workers/Cargo.toml) contains the deployable Workers and
shared replay/build crates. This separation keeps a library consumer independent
of Worker tooling. Both now resolve without sibling checkouts; both pin bsv-rs
exactly, and the workspace consistency check keeps their shared versions equal.
A root workspace test does not test the Workers workspace.

## Build and gate

```sh
cargo build --workspace
cargo build --manifest-path workers/Cargo.toml --workspace --target wasm32-unknown-unknown
```

Use Rust with the wasm target installed. [Makefile](Makefile) defines the full
`make ci` gate: both workspaces' tests and clippy, wasm builds, deployment
checks, route witnesses and D1 budgets. The captain runs it by hand before every
landing. [.github/workflows/ci.yml](.github/workflows/ci.yml) also runs `test`,
`fmt`, `clippy` and `wasm32` on every PR and main push; root formatting is its
current formatting scope. The announced protection start is 2026-10-12 12:00 UTC;
the workflow file alone does not attest that repository settings are enabled.

## Deploy and observe

The deployable configurations live in [crates/overlay-cloudflare](crates/overlay-cloudflare)
and [crates/low-app-layer](crates/low-app-layer). Provision their bindings and
credentials privately, apply the migrations in the documented order and build
from a clean release checkout. Follow [deployment order and schema](docs/DEPLOY-ORDER-AND-SCHEMA.md)
and [storage ownership](docs/STORAGE-OWNERSHIP.md); deployment is an operator action.
Installing a library does not deploy a Worker.

Every health route of both Workers includes `build.engine_rev`, `build.lib_rev`,
`build.dirty`, `build.built_at` and `build.crates`. The crate versions come from
the Workers lockfile; a health response identifies the artifact, and is not a
claim that all downstream services are ready.

## Records and releases

The source release tag for this line is `engine-l2-repin-2026-10-12`.
[RELEASING.md](RELEASING.md) defines the multi-crate release order and trusted
publishing workflow. [CHANGELOG.md](CHANGELOG.md) records changes;
[docs/D1-BUDGETS.md](docs/D1-BUDGETS.md) records route budgets.

Licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE).
