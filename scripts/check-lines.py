#!/usr/bin/env python3
"""The two lines' direction rule, held by a machine (RELEASING.md, "The two lines and the direction rule").

Python 3, standard library only; every fact is read from git in the current repository.

  check-lines.py beta-born --base <sha> --head <sha> [--remote origin]
      A pull request into `main`. Every commit of merge-base(base, head)..head must have been born on
      `beta`. A commit passes when ONE of these holds, read in this order:
        (1) it is reachable from <remote>/beta (by sha);
        (2) <remote>/beta holds a commit of the same patch, compared VERBATIM (`git patch-id
            --verbatim`: whitespace counts, where `git cherry` strips it): a pick whose sha moved and
            whose patch did not, with or without its `-x` line;
        (3) its message carries `(cherry picked from commit <40 hex>)` as a WHOLE line, the line
            `git cherry-pick -x` writes (the same words quoted inside a sentence are not the line), a
            commit so named is reachable from <remote>/beta, the commit is not empty, and it touches
            ONLY paths the named `beta` commit(s) touch: the pick of a `beta` commit resolved by hand,
            whose patch differs. Inside those paths its content is not compared: a reviewer reads it.
            A named commit counts only when the BASE does not already hold it: a commit both lines
            hold (every commit `main` had before the birth is one) is no source of a pick, so a line
            naming it passes nothing;
        (4) a commit of <remote>/beta that is not empty names IT by the same line (the link written
            from the other side), under the same bound on paths;
        (5) it is a RELEASE commit, main-born by nature, said in the output: it changes only the
            manifests (`Cargo.toml`, `workers/Cargo.toml`, `crates/*/Cargo.toml`), the two locks and
            `CHANGELOG.md`; every changed line of a manifest is a `version = "..."` line or an internal
            (`path = `) dependency's line carrying its version; every changed line of a lock is a
            `version = "..."` line; an internal dependency's lines differ in the version alone (the
            lines removed and the lines added are the same once the version is taken out); and the root
            manifest's version is among them.
      Anything else is refused by sha and subject. A MERGE commit not reachable from <remote>/beta is
      refused too (`git cherry` does not read merges, and `main`'s history is linear), except on the cut.
      THE CUT'S PULL REQUEST (RELEASING.md item 16), recognised by its shape, with no flag and no title.
      Walk the head's FIRST-PARENT chain from the head while the commit is reachable neither from
      <remote>/beta nor from <base>; the pull request is the cut when that chain holds a merge and the
      commit the walk stops at is reachable from <remote>/beta and not from <base> (the branch was cut
      from `beta`, not from `main`). On the cut, every commit of <base>..<head> that <remote>/beta does
      not hold must be a commit of that chain with exactly two parents whose SECOND parent is reachable
      from <base> (a merge of `main` into the cut branch; its first parent is, by the walk, the beta
      commit the chain stops at or an earlier such merge). Anything else is refused by sha and subject:
      a non-merge commit (nothing is born on the cut branch), a merge of anything that is not `main`, a
      commit such a merge brought in. The chain must stop at <remote>/beta's TIP itself: a branch cut
      from an older commit of `beta` is refused, naming the commit and the tip. For each merge of the
      chain the paths `git show --remerge-diff` names (what its resolution wrote beyond a clean
      re-merge of its parents; git 2.36 or later, an older git exits 2) are PRINTED, and a path present
      in the merge and absent from BOTH parents (a file the resolution created) is REFUSED. The lines a
      resolution changes inside files its parents hold are printed, not judged: a reviewer reads them.
      A pull request that is not the cut is judged by (1) to (5) above, its merges refused, and a COPY
      of one of beta's own version commits (rule (5)'s shape on beta since merge-base(beta, head),
      matched by verbatim patch or by author, author date and subject, which a rebase keeps) is refused:
      beta's versions reach main only by the cut, so a cut branch rebased onto main is refused. A head
      that IS beta's tip (nothing of `main` left to merge) has nothing to judge.
      After a cut nothing more is needed here: the check judges only the pull request's own commits,
      and a pick made after the cut names a `beta` commit born after it, which the base does not hold.
      While <remote> has NO branch `beta`, the tracked marker `.github/lines`, read at <base> (never
      from the pull request), decides: `beta unborn` (or no marker) passes with one line, the line is
      not born yet and the repository stays mergeable; `beta born` is a FAILURE (exit 1): a line that
      was born and is missing passes nothing. Any other marker is exit 2.

  check-lines.py main-in-beta [--tree HEAD] [--remote origin]
      A pull request into `beta` (on its merge commit) or a push to `beta`. Every commit of
      <remote>/main is on <tree> by (1) sha, (2) patch (verbatim), (3) its own `-x` line naming a
      commit of the tree that <remote>/main does not hold, whose paths bound its own, or (4) a
      commit of the tree naming it. Under (4)
      the naming commit must NOT be empty (a link that carries nothing carries nothing), and its
      paths are not bounded: a release commit of `main` moves versions `beta` does not share, so the
      commit of `beta` that names it carries what it says (the changelog). No release-commit exception.
      Only the commits of <remote>/main AFTER THE LAST CUT are judged: those not reachable from the
      newest commit of main's first-parent chain (`git rev-list --first-parent --merges`) that has
      exactly two parents and whose SECOND parent holds a commit of <tree> its FIRST parent does not
      (some `git merge-base --all <second parent> <tree>` is not an ancestor of the first parent): the
      merge by which `main` took `beta`. Everything at or before it was reconciled by the cut itself,
      and every commit of `beta` up to the cut is then reachable from `main`, so an older pick's line
      names a commit both lines hold and could no longer place it. With no cut, every commit of main is
      judged. A merge from before the birth reads as a cut by this rule too; every commit at or before
      it is on <tree> by sha, so the answer is the same.
      This is patch equivalence and links, never ancestry: `main` keeps linear history and takes
      commits by rebase, so the same change has one sha on `main` and another on `beta`.

  check-lines.py cut-landed [--remote origin] [--cut <the cut branch>]
      Item 16's last step, right after the cut's merge and before anything else lands on main: main's
      tip is a merge commit with two parents whose second parent is a cut branch (the shape above, over
      the merge's first parent) and, with --cut, is that branch's tip. Red, naming what it found, when
      the tip is not a merge: REBASED when the tip's patch is a commit of beta's under a new sha,
      SQUASHED otherwise.

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
DEP_VERSION = re.compile(r'\bversion = "[^"]*"')
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


def linked(sha, line, other, names_of_line, bound_both=True):
    """Rules (3) and (4). (3): the commit names commit(s) of the line that `other` (the line the commit is
    on) does not hold, is not empty, and touches only paths they touch. (4): a commit of the line that is
    not empty names it; `bound_both` holds its paths too."""
    mine = paths(sha)
    sources = [source for source in named_sources(sha)
               if is_ancestor(source, line) and not is_ancestor(source, other)]
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
    """Rule (5): versions and the changelog alone, the root manifest's version among them. An internal
    dependency's line may move its version and nothing else on the line."""
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
        deps = {"-": [], "+": []}
        for row in out("diff", "-U0", f"{sha}^", sha, "--", path).splitlines():
            if row.startswith(("+++", "---")) or not row.startswith(("+", "-")):
                continue
            text = row[1:].strip()
            if VERSION_LINE.match(text):
                bumped = bumped or (path == "Cargo.toml" and row.startswith("+"))
            elif manifest and INTERNAL_DEP_LINE.match(text):
                deps[row[0]].append(DEP_VERSION.sub('version = ""', text))
            else:
                return False
        if sorted(deps["-"]) != sorted(deps["+"]):
            return False
    return bumped


def parents(sha):
    return out("rev-list", "--parents", "-n1", sha).split()[1:]


def cut_chain(head, beta, base):
    """The cut's pull request: (the head's first-parent chain of commits neither <beta> nor <base> holds, the
    commit the walk stops at), when the chain holds a merge and stops at a commit of <beta> that <base>
    lacks; None otherwise."""
    chain, sha = [], out("rev-parse", "--verify", f"{head}^{{commit}}")
    while not is_ancestor(sha, beta) and not is_ancestor(sha, base):
        chain.append(sha)
        up = parents(sha)
        if not up:
            return None
        sha = up[0]
    if not any(len(parents(link)) > 1 for link in chain):
        return None
    if is_ancestor(sha, base):
        return None
    return chain, sha


REMERGE_DIFF_GIT = (2, 36)


def remerge_diff_paths(merge):
    """The paths a merge's tree differs on from a clean automatic re-merge of its parents (`git show
    --remerge-diff`, git 2.36 or later): what its conflict resolution wrote. A git without it cannot run."""
    found = re.match(r"git version (\d+)\.(\d+)", out("version"))
    if not found or tuple(int(part) for part in found.groups()) < REMERGE_DIFF_GIT:
        raise CouldNotRun(f"`{out('version')}` has no `git show --remerge-diff` (git "
                          f"{'.'.join(map(str, REMERGE_DIFF_GIT))} or later): a cut's merges cannot be read")
    shown = out("show", "--remerge-diff", "--format=", "--name-only", "--no-renames", merge)
    return sorted(set(row for row in shown.splitlines() if row.strip()))


def created_by_merge(merge):
    """The paths present in a merge's tree and absent from BOTH its parents: files its resolution created."""
    added = None
    for parent in parents(merge):
        rows = set(out("diff-tree", "-r", "--no-commit-id", "--name-only", "--no-renames", "--diff-filter=A",
                       parent, merge).splitlines())
        added = rows if added is None else added & rows
    return sorted(added or ())


def judge_cut(args, beta, chain, stop):
    """Every commit of the cut's pull request that beta lacks is a merge of the base into the cut branch, the
    branch was cut from beta's TIP, and no merge's resolution creates a file neither parent has. What each
    merge's resolution wrote beyond a clean re-merge is printed: a reviewer reads it."""
    tip = out("rev-parse", "--verify", f"{beta}^{{commit}}")
    if stop != tip:
        line = (f"{NAME}: beta-born: REFUSED: the cut branch was cut from {subject(stop)}, a commit of beta that is "
                f"not its tip {subject(tip)}: cut it again from {args.remote}/beta's tip (item 16, step 2)")
        print(line)
        if os.environ.get("GITHUB_ACTIONS") == "true":
            print(f"::error::{line}")
        return 1
    links = set(chain)
    missing = []
    for sha in out("rev-list", "--reverse", args.head, f"^{args.base}", f"^{beta}").split():
        up = parents(sha)
        if sha in links and len(up) == 2 and is_ancestor(up[1], args.base):
            continue
        missing.append(sha)
    if missing:
        return refuse("beta-born", missing,
                      "of the cut's pull request are neither on beta nor a merge of main into the cut branch "
                      "(nothing is born on the cut branch, and it merges main alone): land each on beta first")
    merges = [sha for sha in reversed(chain) if len(parents(sha)) > 1]
    created = []
    for merge in merges:
        wrote = remerge_diff_paths(merge)
        said = ", ".join(wrote) if wrote else "nothing"
        print(f"{NAME}: beta-born: the cut's merge {subject(merge)} writes, beyond a clean re-merge of its parents: "
              f"{said} (read it: git show --remerge-diff {out('rev-parse', '--short', merge)})")
        created += [(merge, path) for path in created_by_merge(merge)]
    if created:
        for merge, path in created:
            print(f"{NAME}: beta-born: REFUSED {subject(merge)}: its resolution creates {path}, which neither "
                  "parent has")
        line = (f"{NAME}: beta-born: {len(created)} file(s) created by a merge of the cut branch: a cut's merge "
                "resolves what its two parents hold and creates nothing (land the file on beta first)")
        print(line)
        if os.environ.get("GITHUB_ACTIONS") == "true":
            print(f"::error::{line}")
        return 1
    print(f"{NAME}: beta-born: the cut: {out('rev-parse', '--short', args.head)} is {args.remote}/beta "
          f"{out('rev-parse', '--short', beta)}'s history with {len(merges)} merge(s) of main into it; the lines "
          "each merge changes inside files its parents hold are printed above, not judged: a reviewer reads them")
    return 0


def version_commits(beta, head):
    """{verbatim patch id or identity: sha} of beta's own version commits (rule (5)'s shape: the root
    manifest's version moved) since merge-base(beta, head). Identity is the author, the author date and the
    subject, which a rebase keeps."""
    base = git("merge-base", beta, head, check=False).stdout.strip()
    span = [beta] + ([f"^{base}"] if base else [])
    found = {}
    wanted = [sha for sha in out("rev-list", "--no-merges", *span).split() if release_commit(sha)]
    if wanted:
        ids = patch_ids(*span)
        for sha in wanted:
            if sha in ids:
                found[ids[sha]] = sha
            found[identity(sha)] = sha
    return found


def identity(sha):
    return "identity " + out("log", "-1", "--format=%ae%x00%ad%x00%s", "--date=raw", sha)


def last_cut(main, tree):
    """The newest merge of main's first-parent chain whose second parent holds a commit of <tree> its first
    parent does not: the merge by which main took beta. None when main has taken no cut."""
    for sha in out("rev-list", "--first-parent", "--merges", main).split():
        up = parents(sha)
        if len(up) != 2:
            continue
        bases = git("merge-base", "--all", up[1], tree, check=False).stdout.split()
        if any(not is_ancestor(found, up[0]) for found in bases):
            return sha
    return None


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
    found = cut_chain(args.head, beta, args.base)
    if found is not None:
        return judge_cut(args, beta, *found)
    merges = out("rev-list", "--merges", f"{base}..{args.head}", f"^{beta}").split()
    names = names_on(beta, args.head)
    missing = list(merges)
    versions = version_commits(beta, args.head)
    if versions:
        mine = patch_ids(args.head, f"^{base}", f"^{beta}")
        copies = [sha for sha in out("rev-list", "--no-merges", "--reverse", args.head, f"^{base}", f"^{beta}").split()
                  if mine.get(sha) in versions or identity(sha) in versions]
        if copies:
            return refuse("beta-born", copies,
                          "of this pull request are copies of beta's own version commits, which reach main only by "
                          "the cut's merge commit (a rebased cut branch? cut it again from beta's tip: item 16)")
    for sha in unplaced(beta, args.head, base):
        if linked(sha, beta, args.base, names):
            continue
        if release_commit(sha):
            print(f"{NAME}: beta-born: a release commit, main-born by nature (versions and CHANGELOG.md alone): "
                  f"{subject(sha)}")
            continue
        missing.append(sha)
    if missing:
        return refuse("beta-born", missing,
                      "of this pull request were not born on beta (no sha, no verbatim patch, no whole `-x` line "
                      "naming a commit of beta that main lacks and whose paths hold this commit's, not a release commit): land each on beta through a pull request, then "
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
    cut = last_cut(main, tree)
    if cut:
        print(f"{NAME}: main-in-beta: judged from the last cut {subject(cut)}")
    missing = [sha for sha in unplaced(tree, main, cut) if not linked(sha, tree, main, names, bound_both=False)]
    if missing:
        return refuse("main-in-beta", missing,
                      "of main are not on this tree (no sha, no verbatim patch, no cherry-pick line either way from a "
                      "commit that is not empty): "
                      "`git cherry-pick -x` each onto beta through a pull request before this lands")
    print(f"{NAME}: main-in-beta: every commit of {args.remote}/main {out('rev-parse', '--short', main)} is on "
          f"{out('rev-parse', '--short', tree)} by sha, by patch or by a cherry-pick line")
    return 0


def cut_landed(args):
    """Item 16's last step, run by the captain right after the cut's merge and before anything else lands on
    main: main's tip is a merge commit with two parents, its first parent main as it was, its second the cut
    branch (cut_chain's shape over beta). Red, naming what it found, when the cut was squashed or rebased."""
    whole_history()
    main = remote_line(args.remote, "main")
    beta = remote_line(args.remote, "beta")
    if main is None or beta is None:
        raise CouldNotRun(f"{args.remote} has no branch `{'main' if main is None else 'beta'}`")
    tip = out("rev-parse", "--verify", f"{main}^{{commit}}")
    up = parents(tip)

    def red(why):
        line = f"{NAME}: cut-landed: REFUSED {subject(tip)}: {why}"
        print(line)
        if os.environ.get("GITHUB_ACTIONS") == "true":
            print(f"::error::{line}")
        return 1

    if len(up) != 2:
        held = set(patch_ids(beta, f"^{up[0]}" if up else beta).values()) if up else set()
        mine = patch_ids(tip, f"^{up[0]}") if up else {}
        if mine.get(tip) in held:
            how = "REBASED (its patch is a commit of beta's under a new sha; the merges are gone)"
        else:
            how = "SQUASHED (one commit, not a commit of beta's)"
        return red(f"main's tip has {len(up)} parent(s), not a merge commit: the cut was {how}; every beta sha and "
                   "prerelease tag stays off main (item 16)")
    found = cut_chain(up[1], beta, up[0])
    if found is None:
        return red(f"main's tip is a merge whose second parent {subject(up[1])} is not a cut branch (no merge of main "
                   "on a first-parent chain cut from beta)")
    chain, stop = found
    strays = [sha for sha in chain if len(parents(sha)) != 2 or not is_ancestor(parents(sha)[1], up[0])]
    if strays:
        return red(f"its second parent carries {len(strays)} commit(s) that are not merges of main: "
                   + "; ".join(subject(sha) for sha in strays))
    if args.cut and out("rev-parse", "--verify", f"{args.cut}^{{commit}}") != up[1]:
        return red(f"its second parent {subject(up[1])} is not the cut branch's tip {subject(args.cut)}")
    beta_tip = out("rev-parse", "--verify", f"{beta}^{{commit}}")
    held = "is" if is_ancestor(beta_tip, tip) else "is NOT (beta moved after the cut was made)"
    print(f"{NAME}: cut-landed: main's tip {out('rev-parse', '--short', tip)} is a merge commit: first parent "
          f"{out('rev-parse', '--short', up[0])}, second the cut branch {out('rev-parse', '--short', up[1])} cut from "
          f"beta {out('rev-parse', '--short', stop)}; beta's tip {out('rev-parse', '--short', beta_tip)} {held} on main")
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
    landed = checks.add_parser("cut-landed", help="after the cut: main's tip is the cut's merge commit")
    landed.add_argument("--remote", default="origin")
    landed.add_argument("--cut", help="the cut branch's tip, compared with the merge's second parent")
    landed.set_defaults(run=cut_landed)
    args = parser.parse_args()
    try:
        return args.run(args)
    except CouldNotRun as fault:
        print(f"{NAME}: {args.check}: COULD NOT RUN: {fault}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
