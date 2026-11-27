#!/usr/bin/env python3
"""The two lines' direction rule, held by a machine (RELEASING.md, "The two lines and the direction rule").

Python 3, standard library only; every fact is read from git in the current repository.

  check-lines.py beta-born --base <sha> --head <sha> [--remote origin]
      A pull request into `main`. Every commit of merge-base(base, head)..head must have been born on
      `beta`. A commit passes when ONE of these holds, read in this order:
        (1) it is reachable from <remote>/beta (by sha): this is how the CUT's own pull request is
            recognised, with no flag and no title: its head is beta's tip, so no commit is left to judge;
        (2) <remote>/beta holds a commit of the same patch, compared VERBATIM (`git patch-id
            --verbatim`: whitespace counts, where `git cherry` strips it): a pick whose sha moved and
            whose patch did not, with or without its `-x` line;
        (3) its message carries `(cherry picked from commit <40 hex>)` as a WHOLE line, the line
            `git cherry-pick -x` writes (the same words quoted inside a sentence are not the line), a
            commit so named is reachable from <remote>/beta, the commit is not empty, and it touches
            ONLY paths the named `beta` commit(s) touch: the pick of a `beta` commit resolved by hand,
            whose patch differs. Inside those paths its content is not compared: a reviewer reads it;
        (4) a commit of <remote>/beta that is not empty names IT by the same line (the link written
            from the other side), under the same bound on paths;
        (5) it is a RELEASE commit, main-born by nature, said in the output: it changes only the
            manifests (`Cargo.toml`, `workers/Cargo.toml`, `crates/*/Cargo.toml`), the two locks and
            `CHANGELOG.md`; every changed line of a manifest is a `version = "..."` line or an internal
            (`path = `) dependency's line carrying its version; every changed line of a lock is a
            `version = "..."` line; and the root manifest's version is among them.
      Anything else is refused by sha and subject. A MERGE commit not reachable from <remote>/beta is
      refused too (`git cherry` does not read merges, and `main`'s history is linear).
      While <remote> has NO branch `beta`, the tracked marker `.github/lines`, read at <base> (never
      from the pull request), decides: `beta unborn` (or no marker) passes with one line, the line is
      not born yet and the repository stays mergeable; `beta born` is a FAILURE (exit 1): a line that
      was born and is missing passes nothing. Any other marker is exit 2.

  check-lines.py main-in-beta [--tree HEAD] [--remote origin]
      A pull request into `beta` (on its merge commit) or a push to `beta`. Every commit of
      <remote>/main is on <tree> by (1) sha, (2) patch (verbatim), (3) its own `-x` line naming a
      commit of the tree whose paths bound its own, or (4) a commit of the tree naming it. Under (4)
      the naming commit must NOT be empty (a link that carries nothing carries nothing), and its
      paths are not bounded: a release commit of `main` moves versions `beta` does not share, so the
      commit of `beta` that names it carries what it says (the changelog). No release-commit exception.
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
PICK_LINE = re.compile(r"^\(cherry picked from commit ([0-9a-f]{40})\)[ \t]*$", re.M)
MARKER = ".github/lines"
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


def paths(sha):
    """The paths a commit touches (both names of a rename); empty for an empty commit and for a merge."""
    return set(out("diff-tree", "--root", "--no-commit-id", "--name-only", "--no-renames", "-r", sha).splitlines())


def patch_ids(*revs):
    """{sha: verbatim patch id} of the non-merge commits `revs` select. A commit with no patch has none."""
    log = subprocess.run(("git", "log", "--no-merges", "-p", "--format=%H", "--no-ext-diff", "--no-textconv",
                          "--no-renames", "--full-index", "--binary") + revs, capture_output=True)
    if log.returncode != 0:
        raise CouldNotRun(f"git log -p {' '.join(revs)}: {log.stderr.decode(errors='replace').strip()}")
    done = subprocess.run(("git", "patch-id", "--verbatim"), input=log.stdout, capture_output=True)
    if done.returncode != 0:
        raise CouldNotRun(f"git patch-id --verbatim: {done.stderr.decode(errors='replace').strip()}")
    rows = (row.split() for row in done.stdout.decode().splitlines())
    return {row[1]: row[0] for row in rows if len(row) == 2}


def unplaced(line, tip, limit=None):
    """The non-merge commits of limit..tip that `line` holds neither by sha nor by verbatim patch, oldest
    first. `git cherry` would answer the same but for whitespace, which its patch id strips."""
    span = [tip, f"^{line}"] + ([f"^{limit}"] if limit else [])
    wanted = out("rev-list", "--no-merges", "--reverse", *span).split()
    if not wanted:
        return []
    mine = patch_ids(*span)
    held = set(patch_ids(line, f"^{tip}").values())
    return [sha for sha in wanted if mine.get(sha) not in held]


def linked(sha, line, names_of_line, bound_both=True):
    """Rules (3) and (4). (3): the commit names commit(s) of the line, is not empty, and touches only paths
    they touch. (4): a commit of the line that is not empty names it; `bound_both` holds its paths too."""
    mine = paths(sha)
    sources = [source for source in named_sources(sha) if is_ancestor(source, line)]
    if sources and mine and mine <= set().union(*(paths(source) for source in sources)):
        return True
    namers = names_of_line.get(sha, ())
    if not namers:
        return False
    return not bound_both or (bool(mine) and mine <= set().union(*(paths(namer) for namer in namers)))


def names_on(line, other):
    """{named sha: [the commits of merge-base(line, other)..line that name it by a `-x` line and are not empty]}."""
    base = git("merge-base", line, other, check=False).stdout.strip()
    span = f"{base}..{line}" if base else line
    names = {}
    for entry in git("log", "--format=%H%n%B%x00", span).stdout.split("\0"):
        entry = entry.strip("\n")
        if not entry:
            continue
        namer, _, body = entry.partition("\n")
        found = PICK_LINE.findall(body)
        if found and paths(namer):
            for named in found:
                names.setdefault(named, []).append(namer)
    return names


def marker_says_born(commit):
    """The tracked marker at `commit`: True for `beta born`, False for `beta unborn` or no marker."""
    shown = git("show", f"{commit}:{MARKER}", check=False)
    if shown.returncode != 0:
        return False
    rows = [row.strip() for row in shown.stdout.splitlines() if row.strip() and not row.lstrip().startswith("#")]
    if rows == ["beta born"]:
        return True
    if rows == ["beta unborn"]:
        return False
    raise CouldNotRun(f"{MARKER} at {commit} says neither `beta born` nor `beta unborn`")


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
        if marker_says_born(args.base):
            line = (f"{NAME}: beta-born: REFUSED: {MARKER} at the base says `beta born` and {args.remote} has no "
                    "branch `beta`: a line that was born and is missing passes nothing (restore the branch)")
            print(line)
            if os.environ.get("GITHUB_ACTIONS") == "true":
                print(f"::error::{line}")
            return 1
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
                      "of this pull request were not born on beta (no sha, no verbatim patch, no whole `-x` line "
                      "naming a commit of beta whose paths hold this commit's, not a release commit): land each on beta through a pull request, then "
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
    missing = [sha for sha in unplaced(tree, main) if not linked(sha, tree, names, bound_both=False)]
    if missing:
        return refuse("main-in-beta", missing,
                      "of main are not on this tree (no sha, no verbatim patch, no cherry-pick line either way from a "
                      "commit that is not empty): "
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
