#!/usr/bin/env bash
# The two lines' checks (scripts/check-lines.py: beta-born, main-in-beta) on scratch repositories and a local
# bare remote under $TMPDIR. Nothing of this repository's own history is read. LINES_SCRIPT names another copy of
# the script (a mutant: each case below is RED against its rule reverted).
set -euo pipefail
cd "$(dirname "$0")/../.."
script="${LINES_SCRIPT:-$(pwd)/scripts/check-lines.py}"
scratch="$(mktemp -d "${TMPDIR:?set TMPDIR outside the checkout}/lines.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
fail() { echo "lines-test: FAIL $*" >&2; exit 1; }
export HOME="$scratch/home" GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null
unset GITHUB_ACTIONS
mkdir -p "$HOME"
git init -q --bare "$scratch/remote.git"
git init -q -b main "$scratch/repo"
cd "$scratch/repo"
git config user.email t@example.invalid; git config user.name t
git remote add origin "$scratch/remote.git"
mkdir -p crates/a workers src
printf '[workspace.package]\nversion = "0.1.0"\n' > Cargo.toml
printf '[workspace.package]\nversion = "0.1.0"\n' > workers/Cargo.toml
printf '[package]\nname = "a"\n[dependencies]\nb = { package = "bb", version = "0.1.0", path = "../b" }\nc = "1"\n' > crates/a/Cargo.toml
printf '[[package]]\nname = "a"\nversion = "0.1.0"\n' > Cargo.lock
printf '# Changelog\n\n## Unreleased\n' > CHANGELOG.md
printf 'one\ntwo\nthree\nfour\nfive\n' > src/lib.rs
git add -A; git commit -qm base
git push -q origin main
run() { local log="$scratch/run.$RANDOM$RANDOM.out"; last="$log"; set +e; python3 "$script" "$@" >"$log" 2>&1; code=$?; set -e; }
born() { run beta-born --base origin/main --head "$1"; }
carried() { run main-in-beta --tree "$1"; }
commit() { git add -A; git commit -qm "$1"; }                      # commit <subject>
short() { git rev-parse --short "$1"; }

# 1. no branch `beta` on the remote: a commit born on main passes, with the one line
git checkout -q -b fix-1 main
echo born-on-main > src/main_born.rs; commit "born on main"
born fix-1
[ "$code" -eq 0 ] || fail "with no beta branch a pull request into main was refused: $(cat "$last")"
[ "$(wc -l < "$last" | tr -d ' ')" = 1 ] && grep -q 'beta-born: origin has no branch `beta`: the line is not born yet' "$last" \
    || fail "no beta branch is not said in the one line: $(cat "$last")"

# 1b. the marker `.github/lines`, read at the BASE: `beta unborn` passes as no marker does; `beta born` with no
#     branch `beta` on the remote is a failure (a line that was born and is missing passes nothing); any other
#     marker is "could not run"
git checkout -q -b marked main; mkdir -p .github
printf '# the lines\nbeta unborn\n' > .github/lines; commit "the marker, unborn"
run beta-born --base marked --head fix-1
[ "$code" -eq 0 ] || fail "the marker `beta unborn` with no beta branch was refused: $(cat "$last")"
printf '# the lines\nbeta born\n' > .github/lines; commit "the marker, born"
run beta-born --base marked --head fix-1
[ "$code" -eq 1 ] || fail "the marker says beta born, the branch is missing, and beta-born answered $code, not 1: $(cat "$last")"
grep -q 'beta-born: REFUSED: .github/lines at the base says `beta born` and origin has no branch `beta`' "$last" \
    || fail "the missing born line is not said: $(cat "$last")"
printf 'gamma maybe\n' > .github/lines; commit "the marker, unreadable"
run beta-born --base marked --head fix-1
[ "$code" -eq 2 ] || fail "an unreadable marker answered $code, not 2: $(cat "$last")"

# the line is born: beta is cut from main's tip
git checkout -q -b beta main; git push -q origin beta

# 2. a commit born on main is refused, by sha and subject
born fix-1
[ "$code" -eq 1 ] || fail "a commit born on main passed beta-born (exit $code): $(cat "$last")"
grep -q "beta-born: REFUSED $(short fix-1) born on main" "$last" || fail "the refusal does not name the commit: $(cat "$last")"

# 3. `git cherry-pick -x` of a beta commit passes. The pick is resolved by hand (its patch differs from
#    beta's), so only its `-x` line can pass it; a plain pick (no line, the same patch) passes by patch
git checkout -q beta
printf 'one\nTWO\nthree\nfour\nfive\n' > src/lib.rs; commit "fix: two"
fix_on_beta="$(git rev-parse HEAD)"
git push -q origin beta
git checkout -q -b fix-2 main
git cherry-pick -x "$fix_on_beta" >/dev/null
echo "resolved by hand" >> src/lib.rs; git add -A; git commit -q --amend --no-edit
born fix-2
[ "$code" -eq 0 ] || fail "a cherry-pick -x of a beta commit was refused: $(cat "$last")"
git checkout -q -b fix-2b main
git cherry-pick "$fix_on_beta" >/dev/null
born fix-2b
[ "$code" -eq 0 ] || fail "a pick of the same patch was refused: $(cat "$last")"

# 3b. the `-x` line is evidence for the named commit's paths only. A commit born on main that adds another file
#     under a line naming a real beta commit is refused; so is the line QUOTED inside a sentence over a change
#     of the named commit's own path (only a whole line is the line); so is an EMPTY commit that carries the line
git checkout -q -b evil main
echo evil > src/evil.rs; git add -A
git commit -q -m "an unrelated file under a borrowed line" -m "(cherry picked from commit $fix_on_beta)"
born evil
[ "$code" -eq 1 ] || fail "a commit born on main passed on a line naming a beta commit that touches other paths (exit $code): $(cat "$last")"
grep -q "beta-born: REFUSED $(short evil) an unrelated file under a borrowed line" "$last" || fail "the borrowed line's commit is not named: $(cat "$last")"
git checkout -q -b quoted main
printf 'one\ntwo\nthree\nfour\nFIVE\n' > src/lib.rs; git add -A
git commit -q -m "five" -m "see also \"(cherry picked from commit $fix_on_beta)\" in passing"
born quoted
[ "$code" -eq 1 ] || fail "the cherry-pick line quoted inside a sentence passed a commit born on main (exit $code): $(cat "$last")"
git checkout -q -b hollow main
git commit -q --allow-empty -m "nothing at all" -m "(cherry picked from commit $fix_on_beta)"
born hollow
[ "$code" -eq 1 ] || fail "an empty commit passed on its cherry-pick line (exit $code): $(cat "$last")"
#     the link written from beta's side is bound the same way: a beta commit of another path names a main-born
#     commit, and that commit is refused
git checkout -q -b named-from-beta main
echo other > src/named.rs; commit "born on main, named by beta"
named="$(git rev-parse HEAD)"
git checkout -q -b beta-names beta
echo note > src/note.rs; git add -A; git commit -q -m "a note" -m "(cherry picked from commit $named)"
git push -q origin beta-names:beta
born named-from-beta
[ "$code" -eq 1 ] || fail "a commit born on main passed because a beta commit of other paths names it (exit $code): $(cat "$last")"
git push -q -f origin beta:beta

# 3b'. a line may not name a commit BOTH lines hold. Every commit main had before the birth is an ancestor of
#      beta, so a commit born on main that edits an existing file under a line naming such a commit would
#      pass by the named commit's paths: refused
both="$(git rev-list --max-parents=0 main)"
git checkout -q -b old-source main
printf 'one\ntwo\nthree\nfour\nfive\nEVIL\n' > src/lib.rs; git add -A
git commit -q -m "born on main under a line naming a commit both lines hold" -m "(cherry picked from commit $both)"
born old-source
[ "$code" -eq 1 ] || fail "a commit born on main passed on a line naming a commit both lines hold (exit $code): $(cat "$last")"
grep -q "beta-born: REFUSED $(short old-source) born on main under a line" "$last" || fail "the commit under the old line is not named: $(cat "$last")"

# 3c. the patch is compared verbatim: the same lines at another indentation are another patch
git checkout -q beta
printf 'if x:\n    a()\n' > src/tool.py; commit "tool"
printf 'if x:\n    a()\n    c()\n' > src/tool.py; commit "tool: c inside the branch"
git push -q origin beta
git checkout -q -b tool-base main; git cherry-pick beta~1 >/dev/null
git checkout -q -b outdented tool-base
printf 'if x:\n    a()\nc()\n' > src/tool.py; commit "tool: c outside the branch"
run beta-born --base tool-base --head outdented
[ "$code" -eq 1 ] || fail "a whitespace-only variant of a beta patch passed as the same patch (exit $code): $(cat "$last")"
git checkout -q -b indented tool-base; git cherry-pick beta >/dev/null
run beta-born --base tool-base --head indented
[ "$code" -eq 0 ] || fail "the verbatim pick of the same patch was refused: $(cat "$last")"

# 4. a `-x` line naming a commit that is NOT on beta is refused
git checkout -q -b side main
printf 'one\ntwo\nthree\nFOUR\nfive\n' > src/lib.rs; commit "fix: four, never on beta"
not_on_beta="$(git rev-parse HEAD)"
git checkout -q -b fix-3 main
git cherry-pick -x "$not_on_beta" >/dev/null
born fix-3
[ "$code" -eq 1 ] || fail "a cherry-pick line naming a commit beta lacks passed (exit $code): $(cat "$last")"
grep -q "beta-born: REFUSED $(short fix-3) fix: four, never on beta" "$last" || fail "the refusal does not name the pick: $(cat "$last")"
# a line naming no commit at all is refused the same way
git commit -q --amend -m "fix: four" -m "(cherry picked from commit 0123456789abcdef0123456789abcdef01234567)"
born fix-3
[ "$code" -eq 1 ] || fail "a cherry-pick line naming no commit passed (exit $code): $(cat "$last")"

# 5. the release commit is main-born by nature and passes, said; one that also changes a source file is refused
git checkout -q -b release main
sed -i.bak 's/0\.1\.0/0.1.1/' Cargo.toml workers/Cargo.toml crates/a/Cargo.toml Cargo.lock; rm -f ./*.bak ./*/*.bak ./*/*/*.bak
printf '# Changelog\n\n## Unreleased\n\n## [0.1.1]\n\n- the fix\n' > CHANGELOG.md
commit "0.1.1"
born release
[ "$code" -eq 0 ] || fail "a release commit was refused: $(cat "$last")"
grep -q "beta-born: a release commit, main-born by nature (versions and CHANGELOG.md alone): $(short release) 0.1.1" "$last" \
    || fail "the release commit is not said: $(cat "$last")"
git checkout -q -b release-and-more main
sed -i.bak 's/0\.1\.0/0.1.1/' Cargo.toml; rm -f ./*.bak
sed -i.bak 's/c = "1"/c = "2"/' crates/a/Cargo.toml; rm -f crates/a/*.bak
commit "0.1.1 and a dependency"
born release-and-more
[ "$code" -eq 1 ] || fail "a version bump that also moves a dependency passed as a release commit: $(cat "$last")"
#    an internal dependency's line may move its version and nothing else: a release that also repoints its path
git checkout -q -b release-and-a-path main
sed -i.bak 's/0\.1\.0/0.1.1/' Cargo.toml workers/Cargo.toml crates/a/Cargo.toml Cargo.lock; rm -f ./*.bak ./*/*.bak ./*/*/*.bak
sed -i.bak 's|path = "../b"|path = "../elsewhere"|' crates/a/Cargo.toml; rm -f crates/a/*.bak
commit "0.1.1 and a path"
born release-and-a-path
[ "$code" -eq 1 ] || fail "a release commit that repoints an internal dependency's path passed: $(cat "$last")"

# 6. the cut's own pull request (its head is beta's tip) passes: no commit is left to judge; a merge commit
#    born on a main-bound branch is refused
born beta
[ "$code" -eq 0 ] || fail "the cut's pull request was refused: $(cat "$last")"
git checkout -q -b merged main
git merge -q --no-ff -m "merge beta by hand" beta
born merged
[ "$code" -eq 1 ] || fail "a merge commit born on a main-bound branch passed (exit $code): $(cat "$last")"
grep -q "beta-born: REFUSED $(short merged) merge beta by hand" "$last" || fail "the merge is not named: $(cat "$last")"

# 7. main-in-beta. main takes the hand-resolved pick of the beta fix by rebase (another sha, another patch): beta
#    carries it by the pick's own `-x` line, green
git checkout -q main; git cherry-pick fix-2 >/dev/null; git push -q origin main
carried beta
[ "$code" -eq 0 ] || fail "main-in-beta is red though main's only new commit names its beta source: $(cat "$last")"
#    main then holds a fix beta lacks: red, the commit named
echo "hot fix" > src/hot.rs; commit "hot fix on main"
hot="$(git rev-parse HEAD)"
git push -q origin main
carried beta
[ "$code" -eq 1 ] || fail "main-in-beta is green though main holds a fix beta lacks (exit $code): $(cat "$last")"
grep -q "main-in-beta: REFUSED $(short "$hot") hot fix on main" "$last" || fail "the missing fix is not named: $(cat "$last")"
#    carried to beta with -x and resolved by hand there (another patch): green by the line a beta commit carries
git checkout -q beta
git cherry-pick -x "$hot" >/dev/null
echo "beta's own shape" >> src/hot.rs; git add -A; git commit -q --amend --no-edit
carried beta
[ "$code" -eq 0 ] || fail "main-in-beta is red after the fix was carried with -x: $(cat "$last")"
grep -q 'main-in-beta: every commit of origin/main .* by sha, by patch or by a cherry-pick line' "$last" || fail "green is not said: $(cat "$last")"
#    an EMPTY commit of beta that only carries the link line carries nothing: red
git reset -q --hard HEAD~1
git commit -q --allow-empty -m "carry the hot fix, in name only" -m "(cherry picked from commit $hot)"
carried beta
[ "$code" -eq 1 ] || fail "main-in-beta is green on an empty commit that only names the fix (exit $code): $(cat "$last")"
#    and carried by patch alone (a plain pick, as a rebase merge leaves it): green
git reset -q --hard HEAD~1; git cherry-pick "$hot" >/dev/null
carried beta
[ "$code" -eq 0 ] || fail "main-in-beta is red after the fix was carried by patch: $(cat "$last")"
#    had the commit under a line naming a commit both lines hold LANDED on main, beta does not carry it: red
git checkout -q main; echo EVIL >> src/lib.rs; git add -A
git commit -q -m "born on main under a line naming a commit both lines hold" -m "(cherry picked from commit $both)"
git push -q origin main
carried beta
[ "$code" -eq 1 ] || fail "main-in-beta is green on a main commit whose line names a commit both lines hold (exit $code): $(cat "$last")"
grep -q "main-in-beta: REFUSED $(short main) born on main under a line" "$last" || fail "the landed commit is not named: $(cat "$last")"
git checkout -q beta

# 9. a full life on a fresh repository and remote: the birth; a clean pick and a hand-resolved pick to main; a
#    release commit on main; a prerelease tag on beta; THE CUT (a branch cut from beta's tip with main merged
#    into it, a conflict resolved there), taken by main as ONE merge commit; then life after the cut, a second
#    cut, and a commit born on main after them
git init -q --bare "$scratch/life.git"
git init -q -b main "$scratch/life"
cd "$scratch/life"
git config user.email t@example.invalid; git config user.name t
git remote add origin "$scratch/life.git"
mkdir -p src
printf '[workspace.package]\nversion = "0.1.0"\n' > Cargo.toml
printf '# Changelog\n' > CHANGELOG.md
printf 'one\ntwo\nthree\nfour\nfive\n' > src/lib.rs
commit base; git push -q origin main
git checkout -q -b beta main; git push -q origin beta                           # the birth
printf 'one\nTWO\nthree\nfour\nfive\n' > src/lib.rs; commit "fix: two"; f1="$(git rev-parse HEAD)"
echo three > src/three.rs; commit "fix: three"; f2="$(git rev-parse HEAD)"
git push -q origin beta
git checkout -q main; git cherry-pick -x "$f2" >/dev/null                         # a clean pick
git cherry-pick -x "$f1" >/dev/null                                               # a pick resolved by hand
echo "resolved by hand" >> src/lib.rs; git add -A; git commit -q --amend --no-edit
run beta-born --base origin/main --head main
[ "$code" -eq 0 ] || fail "life: the two picks were refused: $(cat "$last")"
git push -q origin main
sed -i.bak 's/0\.1\.0/0.1.1/' Cargo.toml; rm -f ./*.bak; printf '# Changelog\n\n## [0.1.1]\n' > CHANGELOG.md
commit "0.1.1"; release="$(git rev-parse HEAD)"                                   # a release commit on main
run beta-born --base origin/main --head main
[ "$code" -eq 0 ] || fail "life: the release commit was refused: $(cat "$last")"
git push -q origin main
#    a branch cut from MAIN whose one commit merges main, its resolution carrying a file: not the cut, refused
git checkout -q -b sneak main~1; git merge -q --no-ff --no-edit -m "merge main, with a file" main
echo sneaked > src/sneak.rs; git add -A; git commit -q --amend --no-edit
run beta-born --base origin/main --head sneak
[ "$code" -eq 1 ] || fail "a merge of main on a branch cut from main passed as a cut (exit $code): $(cat "$last")"
grep -q "beta-born: REFUSED $(short sneak) merge main, with a file" "$last" || fail "the merge on a main branch is not named: $(cat "$last")"
git checkout -q beta
printf '# Changelog\n\n## [0.1.1]\n' > CHANGELOG.md; git add -A
git commit -q -m "carry the 0.1.1 changelog" -m "(cherry picked from commit $release)"
sed -i.bak 's/0\.1\.0/0.2.0-beta.1/' Cargo.toml; rm -f ./*.bak; commit "0.2.0-beta.1"
git tag v0.2.0-beta.1; git push -q origin beta v0.2.0-beta.1                      # a prerelease on beta
carried beta
[ "$code" -eq 0 ] || fail "life: main-in-beta is red before the cut: $(cat "$last")"
#    the cut branch: beta's tip, main merged INTO it, the version conflict resolved there (beta takes no merge)
git checkout -q -b cut beta
if git merge -q --no-edit main >/dev/null 2>&1; then fail "life: the fixture's merge of main did not conflict"; fi
git checkout -q --ours Cargo.toml; git add -A; git commit -q --no-edit
run beta-born --base origin/main --head cut
[ "$code" -eq 0 ] || fail "the cut's pull request (beta's tip with main merged in) was refused: $(cat "$last")"
grep -q "beta-born: the cut: $(short cut) is origin/beta $(short beta)'s history with 1 merge(s) of main into it" "$last" \
    || fail "the cut is not said: $(cat "$last")"
#    the same cut branch with one commit born on it: refused, by name
git checkout -q -b cut-extra cut
echo born-on-the-cut > src/cut.rs; commit "born on the cut branch"
run beta-born --base origin/main --head cut-extra
[ "$code" -eq 1 ] || fail "a commit born on the cut branch passed (exit $code): $(cat "$last")"
grep -q "beta-born: REFUSED $(short cut-extra) born on the cut branch" "$last" || fail "the commit born on the cut is not named: $(cat "$last")"
#    a cut branch that merges a third branch: refused, the merge named
git checkout -q -b third "$(git rev-list --max-parents=0 main)"; echo third > src/third.rs; commit "a third branch"
git checkout -q -b cut-third beta; git merge -q --no-ff --no-edit -m "merge a third branch" third
run beta-born --base origin/main --head cut-third
[ "$code" -eq 1 ] || fail "a cut branch that merges a third branch passed (exit $code): $(cat "$last")"
grep -q "beta-born: REFUSED $(short cut-third) merge a third branch" "$last" || fail "the third branch's merge is not named: $(cat "$last")"
#    main takes the cut by ONE merge commit, first parent main, second parent the cut branch
git checkout -q main; git merge -q --no-ff --no-edit -m "the cut" cut; git push -q origin main
[ "$(git rev-parse main^1)" = "$(git rev-parse origin/main~1)" ] || fail "life: the merge's first parent is not main"
git merge-base --is-ancestor v0.2.0-beta.1 main || fail "the prerelease tag is not an ancestor of main after the cut"
[ -z "$(git rev-list beta ^main)" ] || fail "a beta sha is not an ancestor of main after the cut"
carried beta
[ "$code" -eq 0 ] || fail "main-in-beta is red on beta after the cut: $(cat "$last")"
grep -q "main-in-beta: judged from the last cut $(short main) the cut" "$last" || fail "the last cut is not said: $(cat "$last")"
#    a change born on beta after the cut, picked to main: both checks green
git checkout -q beta; echo four > src/four.rs; commit "fix: four"; f4="$(git rev-parse HEAD)"; git push -q origin beta
git checkout -q -b pick-4 main; git cherry-pick -x "$f4" >/dev/null
run beta-born --base origin/main --head pick-4
[ "$code" -eq 0 ] || fail "a pick made after the cut was refused: $(cat "$last")"
git checkout -q main; git merge -q --ff-only pick-4; git push -q origin main
carried beta
[ "$code" -eq 0 ] || fail "main-in-beta is red after a pick made after the cut: $(cat "$last")"
#    a second cut works the same way from the last one
git checkout -q -b cut-2 beta; git merge -q --no-edit main >/dev/null
run beta-born --base origin/main --head cut-2
[ "$code" -eq 0 ] || fail "the second cut's pull request was refused: $(cat "$last")"
git checkout -q main; git merge -q --no-ff --no-edit -m "the second cut" cut-2; git push -q origin main
carried beta
[ "$code" -eq 0 ] || fail "main-in-beta is red on beta after the second cut: $(cat "$last")"
#    a commit born on main after the cut: beta-born refuses it; landed by force, main-in-beta goes red
git checkout -q -b after main; echo after > src/after.rs; commit "born on main after the cut"
run beta-born --base origin/main --head after
[ "$code" -eq 1 ] || fail "a commit born on main after the cut passed beta-born (exit $code): $(cat "$last")"
git push -q -f origin after:main
carried beta
[ "$code" -eq 1 ] || fail "main-in-beta is green though main holds a commit born after the cut (exit $code): $(cat "$last")"
grep -q "main-in-beta: REFUSED $(short after) born on main after the cut" "$last" || fail "the commit born after the cut is not named: $(cat "$last")"
cd "$scratch/repo"

# 8. a check that cannot run is never a pass: a remote that does not answer
git remote set-url origin "$scratch/gone.git"
born fix-1
[ "$code" -eq 2 ] || fail "beta-born with an unreachable remote answered $code, not 2: $(cat "$last")"

echo "lines-test: PASS (no beta branch passes with the one line; a commit born on main refused by sha; a hand-resolved cherry-pick -x of a beta commit passes, a same-patch pick passes; a line naming a commit beta lacks, or no commit, refused; a release commit passes and is said, one that moves a dependency refused; the cut's own pull request passes, a merge commit refused; the marker read at the base: unborn passes, born with no branch fails, unreadable is exit 2; a borrowed line over other paths, a quoted line, an empty commit under a line and a beta link of other paths refused; a whitespace-only variant refused, the verbatim pick passes; main-in-beta green by main's own line, red on a fix beta lacks, green once carried by -x or by patch, red on an empty commit that only names it; a line naming a commit both lines hold refused by both checks; a release that repoints an internal dependency's path refused; a full life: the cut's pull request (beta's tip with main merged in) passes, one with a commit born on it or a merge of a third branch refused, a merge of main on a branch cut from main refused, main takes it by one merge commit and holds every beta sha and the prerelease tag, main-in-beta green after it and after a second cut, judged from the last cut, a pick made after the cut green on both checks, a commit born on main after the cut refused by both; an unreachable remote is exit 2)"
