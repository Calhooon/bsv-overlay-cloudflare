# low-proof-replay

Replays a LOW/proof/v1 transcript bundle and re-derives both hands using
`bsv-low-core` and `bsv-low-wire` from crates.io. The `prove_bundle` function
fills the overlay discovery `BundleProver` hook. It performs no network I/O.

The crate builds natively and for wasm32-unknown-unknown. It uses the same
0.1.0 release line as the engine, discovery and Cloudflare adapter.

Licensed under MIT OR Apache-2.0. Source and integration tests are in the
[bsv-overlay-cloudflare repository](https://github.com/Calhooon/bsv-overlay-cloudflare).
