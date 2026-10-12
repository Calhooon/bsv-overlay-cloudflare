//! Build-host implementation; never linked into a Worker wasm binary.
use std::path::{Path, PathBuf};
use std::process::Command;

const CRATES: [(&str, &str); 3] = [
    ("bsv-rs", "OVERLAY_BSV_RS"),
    ("bsv-middleware-cloudflare", "OVERLAY_MIDDLEWARE_CLOUDFLARE"),
    ("bsv-middleware-core", "OVERLAY_MIDDLEWARE_CORE"),
];

// Cargo writes canonical [[package]] entries. Only top-level name/version
// fields count; never choose a version out of multiple package entries.
fn lock_version(lock: &str, wanted: &str) -> Result<String, String> {
    let mut versions = Vec::new();
    for block in lock.split("[[package]]").skip(1) {
        let mut name = None;
        let mut version = None;
        for line in block
            .lines()
            .take_while(|l| !l.trim_start().starts_with('['))
        {
            let line = line.trim();
            if let Some(v) = line.strip_prefix("name = ") {
                name = Some(v.trim_matches('"'));
            }
            if let Some(v) = line.strip_prefix("version = ") {
                version = Some(v.trim_matches('"'));
            }
        }
        if name == Some(wanted) {
            versions.push(
                version.ok_or_else(|| format!("workers/Cargo.lock: {wanted} has no version"))?,
            );
        }
    }
    match versions.as_slice() {
        [version] => Ok((*version).to_owned()),
        _ => Err(format!(
            "workers/Cargo.lock: expected exactly one {wanted} package entry, found {}",
            versions.len()
        )),
    }
}

fn git(dir: &Path, args: &[&str]) -> Option<String> {
    let out = Command::new("git")
        .arg("--no-optional-locks")
        .args(args)
        .current_dir(dir)
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

fn valid_rev(rev: &str) -> bool {
    rev == "unknown" || (rev.len() == 40 && rev.bytes().all(|c| c.is_ascii_hexdigit()))
}

fn revision(dir: &Path) -> (String, bool) {
    let rev = git(dir, &["rev-parse", "HEAD"]).filter(|s| valid_rev(s));
    let dirty =
        rev.is_some() && git(dir, &["status", "--porcelain"]).is_some_and(|s| !s.is_empty());
    (rev.unwrap_or_else(|| "unknown".into()), dirty)
}

// The std-only UTC conversion used by Zanaadu's build_revs model.
pub fn rfc3339_utc(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3_600,
        (rem % 3_600) / 60,
        rem % 60
    )
}

// Cargo packages include their resolved lock beside the normalized manifest.
// Repository builds retain the shared workers lock and its exact versions.
fn build_lock(manifest: &Path) -> PathBuf {
    let packaged = manifest.join("Cargo.lock");
    if packaged.is_file() {
        return packaged;
    }
    manifest
        .parent()
        .and_then(Path::parent)
        .expect("Worker lives under crates/ or carries a packaged Cargo.lock")
        .join("workers/Cargo.lock")
}

/// Run from each deployable crate's build.rs, never from the shared crate's.
pub fn emit() {
    let manifest = PathBuf::from(
        std::env::var_os("CARGO_MANIFEST_DIR").expect("Cargo sets CARGO_MANIFEST_DIR"),
    );
    let lock = build_lock(&manifest);
    println!("cargo:rerun-if-env-changed=BUILD_LIB_REV");
    println!("cargo:rerun-if-changed={}", lock.display());
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=build.rs");
    let text = std::fs::read_to_string(&lock)
        .expect("read build Cargo.lock for exact build crate versions");
    for (name, env) in CRATES {
        let version = lock_version(&text, name).unwrap_or_else(|message| panic!("{message}"));
        println!("cargo:rustc-env={env}={version}");
    }

    // git --git-path resolves linked worktrees, ordinary checkouts, packed
    // refs and Cargo's detached git checkouts, relative to this manifest.
    for name in ["HEAD", "index", "packed-refs"] {
        watch_git_path(&manifest, name);
    }
    if let Some(reference) = git(&manifest, &["symbolic-ref", "-q", "HEAD"]) {
        watch_git_path(&manifest, &reference);
    }
    let (engine, dirty) = revision(&manifest);
    let lib = std::env::var("BUILD_LIB_REV").unwrap_or_else(|_| engine.clone());
    assert!(
        valid_rev(&lib),
        "BUILD_LIB_REV must be 40 hex characters or unknown"
    );
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    println!("cargo:rustc-env=OVERLAY_ENGINE_REV={engine}");
    println!("cargo:rustc-env=OVERLAY_LIB_REV={lib}");
    println!("cargo:rustc-env=OVERLAY_DIRTY={}", u8::from(dirty));
    println!("cargo:rustc-env=OVERLAY_BUILT_AT={}", rfc3339_utc(now));
}

fn watch_git_path(manifest: &Path, name: &str) {
    if let Some(path) = git(manifest, &["rev-parse", "--git-path", name]) {
        let path = manifest.join(path);
        println!("cargo:rerun-if-changed={}", path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_versions_are_exact_and_ambiguous_entries_fail() {
        let one = "[[package]]\nname = \"bsv-rs\"\nversion = \"0.4.3\"\n";
        assert_eq!(lock_version(one, "bsv-rs").unwrap(), "0.4.3");
        for lock in [
            String::new(),
            format!("{one}{one}"),
            format!("{one}{}", one.replace("0.4.3", "0.3.35")),
        ] {
            assert!(lock_version(&lock, "bsv-rs")
                .unwrap_err()
                .contains("expected exactly one bsv-rs package entry"));
        }
        let actual = include_str!("../../../workers/Cargo.lock");
        for (name, _) in CRATES {
            assert!(lock_version(actual, name).is_ok());
        }
    }

    #[test]
    fn lock_location_supports_repository_and_packaged_worker() {
        let scratch = std::env::temp_dir().join(format!("build-lock-{}", std::process::id()));
        let manifest = scratch.join("crates/worker");
        std::fs::create_dir_all(&manifest).unwrap();
        assert_eq!(build_lock(&manifest), scratch.join("workers/Cargo.lock"));
        let packaged = manifest.join("Cargo.lock");
        std::fs::write(&packaged, "packaged lock").unwrap();
        assert_eq!(build_lock(&manifest), packaged);
        std::fs::remove_dir_all(scratch).unwrap();
    }

    #[test]
    fn utc_calendar_boundaries() {
        assert_eq!(rfc3339_utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339_utc(951_782_400), "2000-02-29T00:00:00Z");
        assert_eq!(rfc3339_utc(1_704_067_199), "2023-12-31T23:59:59Z");
    }

    #[test]
    fn revisions_follow_the_crate_checkout_and_detect_untracked_files() {
        let scratch =
            std::env::temp_dir().join(format!("worker-build-info-{}", std::process::id()));
        std::fs::create_dir(&scratch).unwrap();
        let repo = scratch.join("repo");
        let manifest = repo.join("crates/worker");
        std::fs::create_dir_all(&manifest).unwrap();
        assert_eq!(revision(&manifest), ("unknown".into(), false));
        assert!(git(&repo, &["init", "--template=", "."]).is_some());
        let file = manifest.join("Cargo.toml");
        std::fs::write(&file, "fixture").unwrap();
        assert!(git(&repo, &["add", "."]).is_some());
        assert!(git(
            &repo,
            &[
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "user.name=Build test",
                "-c",
                "user.email=build@example.invalid",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-m",
                "fixture"
            ]
        )
        .is_some());
        let rev = git(&repo, &["rev-parse", "HEAD"]).unwrap();
        assert_eq!(revision(&manifest), (rev.clone(), false));
        std::fs::write(manifest.join("untracked"), "dirty").unwrap();
        assert_eq!(revision(&manifest), (rev.clone(), true));
        std::fs::remove_file(manifest.join("untracked")).unwrap();
        std::fs::write(&file, "modified").unwrap();
        assert_eq!(revision(&manifest), (rev.clone(), true));
        std::fs::write(&file, "fixture").unwrap();
        assert!(git(&repo, &["checkout", "--detach", &rev]).is_some());
        assert_eq!(revision(&manifest), (rev.clone(), false));
        let linked = scratch.join("linked");
        assert!(git(
            &repo,
            &[
                "worktree",
                "add",
                "--detach",
                linked.to_str().unwrap(),
                &rev
            ]
        )
        .is_some());
        assert_eq!(revision(&linked.join("crates/worker")), (rev, false));
        let head = git(&linked, &["rev-parse", "--git-path", "HEAD"]).unwrap();
        assert!(linked.join(head).is_file());
        assert!(git(&repo, &["worktree", "remove", linked.to_str().unwrap()]).is_some());
        std::fs::remove_dir_all(scratch).unwrap();
    }

    #[test]
    fn revision_shape() {
        assert!(valid_rev("unknown"));
        assert!(valid_rev(&"a".repeat(40)));
        assert!(!valid_rev(&"g".repeat(40)));
        assert!(!valid_rev("abc1234"));
    }
}
