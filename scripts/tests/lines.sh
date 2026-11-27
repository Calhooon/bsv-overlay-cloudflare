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
#    and carried by patch alone (a plain pick, as a rebase merge leaves it): green
git reset -q --hard HEAD~1; git cherry-pick "$hot" >/dev/null
carried beta
[ "$code" -eq 0 ] || fail "main-in-beta is red after the fix was carried by patch: $(cat "$last")"

# 8. a check that cannot run is never a pass: a remote that does not answer
git remote set-url origin "$scratch/gone.git"
born fix-1
[ "$code" -eq 2 ] || fail "beta-born with an unreachable remote answered $code, not 2: $(cat "$last")"

echo "lines-test: PASS (no beta branch passes with the one line; a commit born on main refused by sha; a hand-resolved cherry-pick -x of a beta commit passes, a same-patch pick passes; a line naming a commit beta lacks, or no commit, refused; a release commit passes and is said, one that moves a dependency refused; the cut's own pull request passes, a merge commit refused; main-in-beta green by main's own line, red on a fix beta lacks, green once carried by -x or by patch; an unreachable remote is exit 2)"
