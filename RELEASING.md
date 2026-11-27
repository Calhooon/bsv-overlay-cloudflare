# Releasing the overlay crates

1. Every change reaches protected `main` through a PR, with linear history and admins included.
2. Rebase-merge only with `test`, `fmt`, `clippy`, and `wasm32` green; the captain also runs the full `make ci` on the merge commit.
3. Integrate the callable L2-gate `ci.yml` before releasing 0.1.0; its baseline is `engine-e592-2026-10-10` plus #74's build object.
4. A release PR updates both workspace versions, compatible internal dependency versions and `CHANGELOG.md`; all five packages share the version. Rust 1.88 is the measured minimum (`rust-version`, checked by the gate's `msrv` job on every target); the other jobs build at 1.95.0.
5. Bootstrap the five new crates once from the captain's machine, in order: `bsv-overlay-engine`, `bsv-overlay-discovery`, `bsv-low-proof-replay`, `bsv-worker-build-info`, `bsv-overlay-cloudflare`; use `workers/Cargo.toml` for the latter three and wait for each registry index entry.
6. After each bootstrap publish, create its crates.io trusted publisher for owner `Calhooon`, repository `bsv-overlay-cloudflare`, workflow `release.yml`, no environment; hold the release workflow for the bootstrap `v0.1.0` tag push, then restore it so the version is not published twice.
7. Subsequent releases tag the merged commit `vX.Y.Z`; `.github/workflows/release.yml` checks the tag's form, workspace versions and the tag's line (a stable tag on `main`, a prerelease tag `vX.Y.Z-beta.N` on `beta`: item 14), calls the four CI jobs, dry-runs and publishes in dependency order, waiting for each index entry.
8. Subsequent publication uses OIDC trusted publishing only, with no stored repository token. Each crate job obtains its own short-lived token. Without its trusted publisher, authentication fails closed.
9. Publication intentionally omits `--locked` even though both workspace locks are committed: packaged path dependencies resolve from the registry; gates test the checkout locks and package verification tests the registry resolution.
10. Multi-crate publication is not atomic: after a partial release, preserve published versions. Rerun a failed job only if its crate version is still unpublished; if upload succeeded but indexing timed out, do not rerun that publish. Code fixes or recovery that would republish require a new release PR and version; yank a bad published version when appropriate.

## The two lines and the direction rule

Items 1 to 10 describe one line, `main`. The repository takes a second, `beta`, cut from `main`'s tip by the owner's word. Until that branch exists on the remote nothing above changes and the `beta-born` check passes, saying so. From the day it exists:

11. Every change is born on `beta`: a pull request into `beta`, gated as item 2 gates `main` (`ci.yml` runs on both lines).
12. `main` takes only a quick fix of a NAMED CLASS, each commit a `git cherry-pick -x` of a `beta` commit with its test. The classes:
    - a security advisory;
    - a panic, or a wasm trap, on input;
    - a wrong verdict: consensus, a BEEF, or a coin credited, offered or spent wrongly;
    - a break in a live dependent;
    - the front pages: `README.md`, `SECURITY.md`, release notes;
    - a toolchain mend: what a required check on `main` demands after CI's toolchain moved, with no behaviour change;
    - an additive item a dependent's stable line needs.

    A pull request into `main` says which class it is in, in one line of its description: `Class: <one of the seven>`. A change in none of them waits on `beta` for the cut.
13. Quick fixes travel in roll-ups: one stable patch release carries the fixes that are ready, so a dependent hops once per roll-up, not once per fix.
14. A prerelease is a tag `vX.Y.Z-beta.N` on `beta`'s tip, with both workspaces at `X.Y.Z-beta.N` by a pull request into `beta`. `release.yml` admits that form only on `beta`, a plain `vX.Y.Z` only on `main`, and no other form of tag. Nothing on `beta` is reverted and landed again so that a prerelease carries a pin alone: the prerelease is what `beta` is.
15. What `main` carries that `beta` does not goes to `beta` once, by `git cherry-pick -x`. After that nothing is ported from `main`, because nothing is born there. The one commit born on `main` is a stable release's own commit (item 4: versions and `CHANGELOG.md`); a commit of `beta` that names it by a `(cherry picked from commit <sha>)` line carries what it says to `beta`.
16. `beta` merges into `main` once, at the cut, on the owner's tag.
17. A finding of a read on `beta` stops a landing only when it is one of item 12's classes. Any other finding becomes a row for the line's own work. The deep reads are spent once, at the cut.

Two checks hold the rule, so that no memory has to. Both are `scripts/check-lines.py` (Python 3, standard library), called by jobs of `ci.yml` and tested by `scripts/tests/lines.sh` (`make lines`, part of `make ci` and of the hosted `lint` job):

18. `beta-born`, on every pull request into `main`: each commit of the pull request is on `origin/beta` by sha, by patch (`git cherry`), or by a cherry-pick line in either direction (the commit names a commit of `beta`, or a commit of `beta` names it); or it is a release commit, which the check says in its output (it changes only the manifests, the two locks and `CHANGELOG.md`, every changed manifest or lock line a version). A commit that is none of these is refused by sha and subject, and so is a merge commit. A `-x` line naming a commit `beta` does not hold passes nothing. The cut's own pull request needs no flag: its head is `beta`'s tip, every commit is on `beta` by sha, and none is left to judge.
19. `main-in-beta`, on every pull request into `beta` (judged on the pull request's merge commit) and every push to `beta`: each commit of `origin/main` is on the tree by sha, by patch, or by a cherry-pick line in either direction. It has no release-commit exception (item 15).
20. Both checks read patches and links, never ancestry. `main` keeps linear history and merges by rebase (item 1), so a fix has one sha on `beta` and another on `main`, and after the cut every commit of `beta` has a second sha on `main`; the patch and the `-x` line survive the new sha. A pick resolved by hand has another patch, so it passes only by its `-x` line: add the line with `git commit --amend` when a pick was made without it.
21. A check that cannot run (a shallow clone, a remote that does not answer) exits 2 and is never read as a pass. Making `beta-born` a required check on `main` and `main-in-beta` on `beta` is the protection's own change, made by the captain when `beta` is cut.
