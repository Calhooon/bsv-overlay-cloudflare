//! Shared health metadata. The macro expands in the deployable crate so its
//! own build script's environment is captured, including BUILD_LIB_REV.
// The lint bar (bsv-stack-lean #134): the workspace table cannot deny these
// for the LOW crates beside this one, so the engine's crate denies them here.
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
// The lint bar (bsv-stack-lean #135): the public crate documents every public
// item, and holds no unsafe code.
#![deny(missing_docs)]
#![forbid(unsafe_code)]
#[doc(hidden)]
pub use serde_json::json;

#[cfg(any(feature = "build", test))]
#[doc(hidden)]
// bsv-stack-lean #134: build-script code; a panic is how a build script fails
#[allow(clippy::expect_used, clippy::panic)]
pub mod capture;
#[cfg(feature = "build")]
pub use capture::emit;

/// Capture the calling Worker's build environment without runtime I/O.
/// Missing build-script variables remain null, as in Zanaadu's build object.
#[macro_export]
macro_rules! build_json {
    () => {
        $crate::json!({
            "engine_rev": option_env!("OVERLAY_ENGINE_REV"),
            "lib_rev": option_env!("OVERLAY_LIB_REV"),
            "dirty": option_env!("OVERLAY_DIRTY").map(|v| v == "1"),
            "built_at": option_env!("OVERLAY_BUILT_AT"),
            "crates": {
                "bsv-rs": option_env!("OVERLAY_BSV_RS"),
                "bsv-middleware-cloudflare": option_env!("OVERLAY_MIDDLEWARE_CLOUDFLARE"),
                "bsv-middleware-core": option_env!("OVERLAY_MIDDLEWARE_CORE"),
            },
        })
    };
}
