#!/bin/sh
# `make hooks`: install the secrets door (scripts/hooks/pre-push.common) in this clone's COMMON hooks folder, which
# every worktree of the clone runs, and make sure core.hooksPath is not set (while it is, git reads no hook from that
# folder). Every check runs before any change, so a refusal changes nothing:
# - core.hooksPath set to `scripts/hooks` in the clone's own config (what `make hooks` wrote before; a RELATIVE path,
#   so a worktree whose tree predates the hook ran none) is unset, and said; set to anything else, or set outside the
#   clone's own config, STOPS.
# - a pre-push already in the common folder that is not this wrapper is never overwritten: it is printed and the
#   install STOPS; move it aside and run `make hooks` again, or `make hooks FORCE=1` to replace it.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
src="$here/hooks/pre-push.common"
[ -f "$src" ] || { echo "make hooks: $src is missing" >&2; exit 1; }
common=$(cd "$(git rev-parse --git-common-dir)" && pwd)
dest="$common/hooks/pre-push"
unset_old=0
if hp=$(git config --get core.hooksPath); then
  local_hp=$(git config --local --get core.hooksPath || true)
  if [ "$hp" = "scripts/hooks" ] && [ "$local_hp" = "scripts/hooks" ]; then
    unset_old=1
  else
    echo "make hooks: core.hooksPath is set to '$hp' (not this repository's old value scripts/hooks in the clone's own config); git would run no hook from $common/hooks. Nothing changed: unset it yourself (git config --unset core.hooksPath, at the scope that sets it) and run make hooks again." >&2
    exit 1
  fi
fi
if [ -e "$dest" ] && ! cmp -s "$src" "$dest" && [ "${FORCE:-}" != 1 ]; then
  echo "make hooks: $dest already holds a pre-push that is not this repository's door; nothing changed. It begins:" >&2
  sed -n '1,12p' "$dest" | sed 's/^/  | /' >&2
  echo "make hooks: move it aside (keep it if you still need it) and run make hooks again, or run make hooks FORCE=1 to replace it." >&2
  exit 1
fi
if [ "$unset_old" = 1 ]; then
  git config --local --unset core.hooksPath
  echo "make hooks: unset core.hooksPath (it was scripts/hooks, a relative path that ran no hook in a worktree whose tree predates the door)"
fi
mkdir -p "$common/hooks"
cp "$src" "$dest.tmp.$$"
chmod 755 "$dest.tmp.$$"
mv -f "$dest.tmp.$$" "$dest"
echo "make hooks: the secrets door is installed at $dest for every worktree of this clone; it runs the remote default branch's scripts/check-secrets.py (refs/remotes/origin/HEAD, else origin/main)"
