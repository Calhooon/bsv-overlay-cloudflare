#!/usr/bin/env python3
"""The two lines' direction rule, held by a machine (RELEASING.md, "The two lines and the direction rule").

Python 3, standard library only; every fact is read from git in the current repository.

  check-lines.py beta-born --base <sha> --head <sha> [--remote origin]
      A pull request into `main`. Every commit of merge-base(base, head)..head must have been born on
      `beta`. A commit passes when ONE of these holds, read in this order:
        (1) it is reachable from <remote>/beta (by sha): this is how the CUT's own pull request is
            recognised, with no flag and no title: its head is beta's tip, so no commit is left to judge;
        (2) <remote>/beta holds a commit of the same patch (`git cherry`, the patch id): a pick whose
            sha moved and whose patch did not, with or without its `-x` line;
        (3) its message carries `(cherry picked from commit <40 hex>)`, the line `git cherry-pick -x`
            writes, and a commit so named is reachable from <remote>/beta: the pick of a `beta` commit
            resolved by hand, whose patch differs;
        (4) a commit of <remote>/beta names IT by the same line (the link written from the other side);
        (5) it is a RELEASE commit, main-born by nature, said in the output: it changes only the
            manifests (`Cargo.toml`, `workers/Cargo.toml`, `crates/*/Cargo.toml`), the two locks and
            `CHANGELOG.md`; every changed line of a manifest is a `version = "..."` line or an internal
            (`path = `) dependency's line carrying its version; every changed line of a lock is a
            `version = "..."` line; and the root manifest's version is among them.
      Anything else is refused by sha and subject. A MERGE commit not reachable from <remote>/beta is
      refused too (`git cherry` does not read merges, and `main`'s history is linear).
      While <remote> has NO branch `beta` the check passes and says so in one line: the line is not
      born yet and the repository stays mergeable.

  check-lines.py main-in-beta [--tree HEAD] [--remote origin]
      A pull request into `beta` (on its merge commit) or a push to `beta`. Every commit of
      <remote>/main is on <tree> by (1) sha, (2) patch, (3) its own `-x` line naming a commit of the
      tree, or (4) a commit of the tree naming it. There is no release-commit exception here: what a
      release commit of `main` carries reaches `beta` by a commit of `beta` that names it.
      This is patch equivalence and links, never ancestry: `main` keeps linear history and takes
      commits by rebase, so the same change has one sha on `main` and another on `beta`.

Exit 0: the rule holds. Exit 1: refused, each commit named. Exit 2: the check could not run (no such
commit, a shallow clone, a remote that did not answer): never read as a pass.
"""
import argparse
import os
import re
import subprocess
import sys

NAME = "check-lines"
PICK_LINE = re.compile(r"\(cherry picked from commit ([0-9a-f]{40})\)")
MANIFEST = re.compile(r"^(Cargo\.toml|workers/Cargo\.toml|crates/[^/]+/Cargo\.toml)$")
LOCKS = ("Cargo.lock", "workers/Cargo.lock")
VERSION_LINE = re.compile(r'^version = "[^"]*"$')
INTERNAL_DEP_LINE = re.compile(r'^[A-Za-z0-9_-]+ = \{.*\bversion = "[^"]*".*\bpath = "[^"]*".*\}$|'
                               r'^[A-Za-z0-9_-]+ = \{.*\bpath = "[^"]*".*\bversion = "[^"]*".*\}$')


class CouldNotRun(Exception):
    pass


def git(*args, check=True):
    done = subprocess.run(("git",) + args, capture_output=True, text=True)
    if check and done.returncode != 0:
        raise CouldNotRun(f"git {' '.join(args)}: {done.stderr.strip() or done.returncode}")
    return done


def out(*args):
    return git(*args).stdout.strip()


def is_ancestor(commit, of):
    """True when `commit` exists here and is reachable from `of`."""
    if git("cat-file", "-e", f"{commit}^{{commit}}", check=False).returncode != 0:
        return False
    return git("merge-base", "--is-ancestor", commit, of, check=False).returncode == 0


def remote_line(remote, branch):
    """The remote's branch as a local ref, fetched now; None when the remote has no such branch."""
    listed = git("ls-remote", "--heads", remote, f"refs/heads/{branch}")
    if not listed.stdout.strip():
        return None
    ref = f"refs/remotes/{remote}/{branch}"
    git("fetch", "--quiet", "--no-tags", remote, f"+refs/heads/{branch}:{ref}")
    return ref


def whole_history():
    if out("rev-parse", "--is-shallow-repository") == "true":
        raise CouldNotRun("a shallow clone: the lines cannot be compared (fetch the whole history)")


def subject(sha):
    return out("log", "-1", "--format=%h %s", sha)


def named_sources(sha):
    return PICK_LINE.findall(out("log", "-1", "--format=%B", sha))


def unplaced(line, tip, limit=None):
    """The commits of limit..tip that `line` holds neither by sha nor by patch (`git cherry`'s `+`)."""
    args = ["cherry", line, tip] + ([limit] if limit else [])
    return [row.split()[1] for row in out(*args).splitlines() if row.startswith("+ ")]


def linked(sha, line, names_of_line):
    """Rules (3) and (4): the commit names a commit of the line, or a commit of the line names it."""
    if any(is_ancestor(source, line) for source in named_sources(sha)):
        return True
    return sha in names_of_line


def names_on(line, other):
    """Every commit a commit of merge-base(line, other)..line names by a `-x` line."""
    base = git("merge-base", line, other, check=False).stdout.strip()
    span = f"{base}..{line}" if base else line
    return set(PICK_LINE.findall(out("log", "--format=%B", span)))


def release_commit(sha):
    """Rule (5): versions and the changelog alone, the root manifest's version among them."""
    if git("rev-parse", "--verify", "--quiet", f"{sha}^", check=False).returncode != 0:
        return False
    files = out("diff-tree", "--no-commit-id", "--name-only", "-r", sha).splitlines()
    if not files:
        return False
    bumped = False
    for path in files:
        if path == "CHANGELOG.md":
            continue
        manifest = bool(MANIFEST.match(path))
        if not manifest and path not in LOCKS:
            return False
        for row in out("diff", "-U0", f"{sha}^", sha, "--", path).splitlines():
            if row.startswith(("+++", "---")) or not row.startswith(("+", "-")):
                continue
            text = row[1:].strip()
            if VERSION_LINE.match(text):
                bumped = bumped or (path == "Cargo.toml" and row.startswith("+"))
            elif not (manifest and INTERNAL_DEP_LINE.match(text)):
                return False
    return bumped


def refuse(check, missing, why):
    for sha in missing:
        print(f"{NAME}: {check}: REFUSED {subject(sha)}")
    line = f"{NAME}: {check}: {len(missing)} commit(s) {why}"
    print(line)
    if os.environ.get("GITHUB_ACTIONS") == "true":
        print(f"::error::{line}")
    return 1


def beta_born(args):
    whole_history()
    beta = remote_line(args.remote, "beta")
    if beta is None:
        print(f"{NAME}: beta-born: {args.remote} has no branch `beta`: the line is not born yet, "
              "so every pull request into main passes")
        return 0
    base = out("merge-base", args.base, args.head)
    merges = out("rev-list", "--merges", f"{base}..{args.head}", f"^{beta}").split()
    names = names_on(beta, args.head)
    missing = list(merges)
    for sha in unplaced(beta, args.head, base):
        if linked(sha, beta, names):
            continue
        if release_commit(sha):
            print(f"{NAME}: beta-born: a release commit, main-born by nature (versions and CHANGELOG.md alone): "
                  f"{subject(sha)}")
            continue
        missing.append(sha)
    if missing:
        return refuse("beta-born", missing,
                      "of this pull request were not born on beta (no sha, no patch, no `-x` line naming a "
                      "commit of beta, not a release commit): land each on beta through a pull request, then "
                      "`git cherry-pick -x` it here")
    print(f"{NAME}: beta-born: every commit of {out('rev-parse', '--short', base)}.."
          f"{out('rev-parse', '--short', args.head)} is on {args.remote}/beta "
          f"{out('rev-parse', '--short', beta)} by sha, by patch or by a cherry-pick line, or is a release commit")
    return 0


def main_in_beta(args):
    whole_history()
    main = remote_line(args.remote, "main")
    if main is None:
        raise CouldNotRun(f"{args.remote} has no branch `main`")
    tree = out("rev-parse", "--verify", f"{args.tree}^{{commit}}")
    names = names_on(tree, main)
    missing = [sha for sha in unplaced(tree, main) if not linked(sha, tree, names)]
    if missing:
        return refuse("main-in-beta", missing,
                      "of main are not on this tree (no sha, no patch, no cherry-pick line either way): "
                      "`git cherry-pick -x` each onto beta through a pull request before this lands")
    print(f"{NAME}: main-in-beta: every commit of {args.remote}/main {out('rev-parse', '--short', main)} is on "
          f"{out('rev-parse', '--short', tree)} by sha, by patch or by a cherry-pick line")
    return 0


def main():
    parser = argparse.ArgumentParser(prog=NAME, description="The two lines' direction rule (RELEASING.md).")
    checks = parser.add_subparsers(dest="check", required=True)
    born = checks.add_parser("beta-born", help="a pull request into main: every commit was born on beta")
    born.add_argument("--base", required=True, help="the pull request's base commit (main)")
    born.add_argument("--head", required=True, help="the pull request's head commit")
    born.add_argument("--remote", default="origin")
    born.set_defaults(run=beta_born)
    carried = checks.add_parser("main-in-beta", help="beta's tree carries every commit of main")
    carried.add_argument("--tree", default="HEAD", help="the tree judged (a merge commit, beta's tip)")
    carried.add_argument("--remote", default="origin")
    carried.set_defaults(run=main_in_beta)
    args = parser.parse_args()
    try:
        return args.run(args)
    except CouldNotRun as fault:
        print(f"{NAME}: {args.check}: COULD NOT RUN: {fault}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
