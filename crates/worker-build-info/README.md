# bsv-worker-build-info

Shared build provenance for the overlay and app-layer Workers. The `build`
feature exposes `emit()` for each Worker build script; `build_json!()` captures
the calling crate's build environment without runtime I/O.

Capture records revisions, dirty status, UTC build time and exact dependency
versions from the build lockfile. Duplicate dependency entries fail closed.
`BUILD_LIB_REV` accepts a 40-character hexadecimal revision or `unknown`.

Repository builds read `workers/Cargo.lock`; packaged Worker builds read the
resolved `Cargo.lock` beside their manifest. Archives without git metadata
report an `unknown` revision. The health object shape is unchanged.
