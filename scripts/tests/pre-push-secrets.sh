#!/usr/bin/env bash
# The pre-push secrets door (scripts/hooks/pre-push.common -> the remote default branch's scripts/check-secrets.py
# --pre-push) on scratch repositories and local bare remotes under $TMPDIR, the door installed THERE by
# scripts/install-hooks.sh, the script `make hooks` runs, and a SYNTHETIC vault
# folder (OVERLAY_VAULT_DIR; HOME pointed at the scratch so no real vault is ever read). Every planted value is built
# here at run time; none is committed.
set -euo pipefail
cd "$(dirname "$0")/../.."
here="$(pwd)"
scratch="$(mktemp -d "${TMPDIR:?set TMPDIR outside the checkout}/pre-push-secrets.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
fail() { echo "pre-push-secrets-test: FAIL $*" >&2; exit 1; }
export HOME="$scratch/home" GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null
mkdir -p "$HOME" "$scratch/vault"
# synthetic values: one recorded only as `# Enter:` under a secret-setting command, one bare in a second file
rand() { python3 -c 'import secrets,string;a=string.ascii_letters+string.digits;print("".join(secrets.choice(a) for _ in range(37))+"Q7z")'; }
entered="$(rand)"; bare="$(rand)"; shape="gh""p_$(rand | cut -c1-36)"
printf 'wrangler secret put SYNTHETIC\n# Enter: %s\n' "$entered" > "$scratch/vault/one.md"
printf 'notes\n%s\n' "$bare" > "$scratch/vault/two.txt"
export OVERLAY_VAULT_DIR="$scratch/vault"
git init -q --bare "$scratch/remote.git"
git init -q -b main "$scratch/repo"
cd "$scratch/repo"
git config user.email t@example.invalid; git config user.name t
mkdir -p scripts/hooks
cp "$here/scripts/check-secrets.py" scripts/
cp "$here/scripts/hooks/pre-push" scripts/hooks/
echo clean > README; git add -A; git commit -qm base
git remote add origin "$scratch/remote.git"
git push -q origin main                    # the remote's main holds the door (no hook installed yet)
tip() { git -C "$scratch/remote.git" rev-parse -q --verify "refs/heads/$1" || echo none; }
outputs=()
push() { local log="$scratch/push.$RANDOM.out"; outputs+=("$log"); last="$log"; set +e; git push "$@" >"$log" 2>&1; code=$?; set -e; }
install() { local log="$scratch/install.$RANDOM.out"; last="$log"; set +e; FORCE="${FORCE:-}" sh "$here/scripts/install-hooks.sh" >"$log" 2>&1; code=$?; set -e; }
common_hook() { echo "$(git rev-parse --git-common-dir)/hooks/pre-push"; }

# the installer over the old relative core.hooksPath (what `make hooks` once set); its effects are asserted below
git config core.hooksPath scripts/hooks
install; [ "$code" -eq 0 ] || fail "the installer failed: $(cat "$last")"
grep -q "FORCE=\$(FORCE)" "$here/Makefile" || fail "make hooks takes no FORCE"
grep -q 'sh scripts/install-hooks.sh' "$here/Makefile" || fail "make hooks does not run scripts/install-hooks.sh"

push origin main; [ "$code" -eq 0 ] || fail "a clean push was refused: $(tail -2 "$last")"
base="$(tip main)"

# a shape is refused, and the remote does not receive it
printf 'export X=%s\n' "$shape" > shape.sh; git add shape.sh; git commit -qm shape
push origin main; [ "$code" -ne 0 ] || fail "a planted credential shape was pushed"
[ "$(tip main)" = "$base" ] || fail "the remote received the refused commit"
grep -q 'credential at [0-9a-f]\{12\}:shape.sh:1 (a GitHub token; value withheld)' "$last" || fail "the shape refusal does not name commit, path, line and rule"
git reset -q --hard HEAD~1

# a value the vault records only as `# Enter:`, in a quoted list with no name near it, is refused
printf 'const L: [&str; 1] = ["%s"];\n' "$entered" > list.rs; git add list.rs; git commit -qm list
push origin main; [ "$code" -ne 0 ] || fail "a vault value was pushed"
grep -q ':list.rs:1 (a value the vault holds (one.md line 2); value withheld)' "$last" || fail "the vault refusal does not name the vault file and line"
git reset -q --hard HEAD~1

# a value added and removed again in one push: refused (it is in the pushed history)
printf '%s\n' "$bare" > note.txt; git add note.txt; git commit -qm add
git rm -q note.txt; git commit -qm remove
push origin main; [ "$code" -ne 0 ] || fail "a value added then removed in one push was pushed"
grep -q ':note.txt:1 (a value the vault holds (two.txt line 2)' "$last" || fail "the buried value was not named"
[ "$(tip main)" = "$base" ] || fail "the remote received the refused history"
git reset -q --hard HEAD~2

# a new branch is judged: one carrying a value is refused, a clean one passes
git checkout -q -b side; printf '%s\n' "$bare" > side.txt; git add side.txt; git commit -qm side
push origin side; [ "$code" -ne 0 ] || fail "a new branch carrying a vault value was pushed"
[ "$(tip side)" = none ] || fail "the remote received the refused branch"
git reset -q --hard HEAD~1; echo more > more; git add more; git commit -qm clean-side
push origin side; [ "$code" -eq 0 ] || fail "a clean new branch was refused: $(tail -2 "$last")"
grep -q '1 commit(s) clean' "$last" || fail "the new branch was not judged against the remote's refs alone"
# a delete carries nothing
push origin :side; [ "$code" -eq 0 ] || fail "a branch delete was refused"

# no vault: the shapes still refuse; a clean push passes with the one WARNING line
export OVERLAY_VAULT_DIR="$scratch/absent"
git checkout -q main; echo again >> README; git commit -qam again
push origin main; [ "$code" -eq 0 ] || fail "a clean push with no vault was refused"
[ "$(grep -c "the vault's values were NOT compared" "$last")" = 1 ] || fail "no vault: the one line was not printed once"
printf 'export X=%s\n' "$shape" > shape.sh; git add shape.sh; git commit -qm shape
push origin main; [ "$code" -ne 0 ] || fail "with no vault a shape was pushed"

# a worktree of this clone checked out at a commit holding NEITHER scripts/check-secrets.py NOR scripts/hooks/ (an old
# base): the door still runs (the remote default branch's script, refs/remotes/origin/main here: no origin/HEAD set)
git checkout -q main; git reset -q --hard "$base"
old_blob="$(echo old | git hash-object -w --stdin)"
old_commit="$(printf '100644 blob %s\tREADME\n' "$old_blob" | git mktree | xargs git commit-tree -m old-base)"
git worktree add -q -b old-line "$scratch/old" "$old_commit"
cd "$scratch/old"
[ ! -e scripts/check-secrets.py ] && [ ! -e scripts/hooks ] || fail "the old worktree holds the door's files"
printf 'export X=%s\n' "$shape" > shape.sh; git add shape.sh; git commit -qm shape-old
push origin old-line; [ "$code" -ne 0 ] || fail "a worktree at an old base pushed a planted credential shape"
grep -q 'credential at [0-9a-f]\{12\}:shape.sh:1 (a GitHub token; value withheld)' "$last" || fail "the old worktree's refusal is not the door's"
[ "$(tip old-line)" = none ] || fail "the remote received the old worktree's refused branch"
git reset -q --hard HEAD~1; echo fine > fine; git add fine; git commit -qm clean-old
push origin old-line; [ "$code" -eq 0 ] || fail "a clean push from the old worktree was refused: $(tail -2 "$last")"
push origin :old-line; [ "$code" -eq 0 ] || fail "the old worktree's branch delete was refused"
cd "$scratch/repo"; git worktree remove --force "$scratch/old"

# the installer never overwrites a foreign pre-push (printed, exit non-zero, nothing changed); FORCE=1 replaces it
printf '#!/bin/sh\n# someone else'"'"'s hook\nexit 0\n' > "$(common_hook)"
git config core.hooksPath scripts/hooks
install; [ "$code" -ne 0 ] || fail "the installer overwrote a foreign pre-push"
grep -q "someone else's hook" "$last" && grep -q 'FORCE=1' "$last" || fail "the foreign pre-push and the way on were not printed"
grep -q "someone else's hook" "$(common_hook)" || fail "the foreign pre-push was changed"
[ "$(git config --get core.hooksPath)" = scripts/hooks ] || fail "a refused install changed core.hooksPath"
FORCE=1 install; [ "$code" -eq 0 ] || fail "FORCE=1 did not replace the foreign pre-push: $(cat "$last")"
grep -q 'unset core.hooksPath' "$last" || fail "the installer did not say it unset the old core.hooksPath"
! git config --get core.hooksPath >/dev/null || fail "the installer left core.hooksPath set"
cmp -s "$here/scripts/hooks/pre-push.common" "$(common_hook)" || fail "FORCE=1 did not install the wrapper"
[ -x "$(common_hook)" ] || fail "the wrapper is not executable"
install; [ "$code" -eq 0 ] || fail "a second install of the same wrapper was refused"
# core.hooksPath set to anything else STOPS and changes nothing
git config core.hooksPath elsewhere; rm -f "$(common_hook)"
install; [ "$code" -ne 0 ] || fail "the installer went on over a foreign core.hooksPath"
[ "$(git config --get core.hooksPath)" = elsewhere ] && [ ! -e "$(common_hook)" ] || fail "a refused install changed something"
git config --unset core.hooksPath; install; [ "$code" -eq 0 ] || fail "the reinstall failed"

# a clone whose remote default branch holds no script: every push refused with the one line, the remote untouched
git init -q --bare -b main "$scratch/bare2.git"
git init -q -b main "$scratch/repo2"
cd "$scratch/repo2"
git config user.email t@example.invalid; git config user.name t
echo two > README; git add -A; git commit -qm base2
git remote add origin "$scratch/bare2.git"; git push -q origin main; git remote set-head origin -a >/dev/null
install; [ "$code" -eq 0 ] || fail "the installer failed in the second clone: $(cat "$last")"
echo more >> README; git commit -qam more
push origin main; [ "$code" -ne 0 ] || fail "a push was let through with no door on the remote default branch"
[ "$(grep -c 'refs/remotes/origin/main holds no scripts/check-secrets.py in this clone.*git fetch origin.*push refused' "$last")" = 1 ] \
    || fail "the refusal is not the one line: $(cat "$last")"
[ "$(git -C "$scratch/bare2.git" rev-parse main)" = "$(git rev-parse HEAD~1)" ] || fail "the remote received the refused push"
cd "$scratch/repo"

# no output of any push carries a planted value or 8 characters of one
for log in "${outputs[@]}"; do
  for value in "$entered" "$bare" "$shape"; do
    for ((i = 0; i + 8 <= ${#value}; i++)); do
      ! grep -qF -- "${value:i:8}" "$log" || fail "a push's output carries part of a planted value"
    done
  done
done
echo "pre-push-secrets-test: PASS (the installer unsets the old core.hooksPath, keeps a foreign pre-push and stops on another core.hooksPath; a worktree at a base holding no door refused a shape and passed a clean push; a remote default branch with no script refuses every push with one line; a shape and an Enter-recorded vault value refused by commit:path:line, the remote untouched; an add-then-remove history refused; a new branch judged; a delete passes; no vault: shapes refuse, one WARNING line; no output carries a value)"
