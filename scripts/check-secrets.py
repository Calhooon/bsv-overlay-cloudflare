#!/usr/bin/env python3
"""check-secrets.py: refuse a value that grants authority before it reaches this public repository.

A value is known two ways, and the check needs both:

  1. BY ITS SHAPE (no vault needed; runs anywhere, the hosted CI included): a credential format a provider
     documents exactly (SHAPES below), or a Bitcoin private key whose Base58Check checksum verifies.
  2. BY BEING ONE OF THE OPERATOR'S OWN SECRETS, whatever it looks like: the values the operator's vault folder
     holds, READ AT RUN TIME on the operator's machine and compared as fixed strings with the scanned bytes. No
     value is ever stored in, printed by, hashed into or abbreviated by this repository: a diagnostic names the
     commit (or worktree / index), the path, the line, the rule, and for a vault value the vault FILE'S NAME and
     LINE NUMBER where it was recorded, then "value withheld".

THE VAULT FOLDER: `$OVERLAY_VAULT_DIR` if set, else `~/vault`. Every regular file under it (recursively, dot-files
and dot-directories skipped) whose suffix is one of TEXT_SUFFIXES (or none) and whose size is at most
VAULT_FILE_MAX_BYTES (1 MiB); a larger file, a file holding a NUL byte in its first 8 KiB (binary), or any other
suffix (an encrypted store, an image) is not read and is NAMED on one line. JSON files (`.json`): a vault's JSON is
mostly STATE (wallet and run state: txids, outpoints, hashes, heights), whose hex would otherwise be read as keys,
so a JSON file that parses gives only the string values whose KEY names a secret (SECRET_NAME), at any depth; one
that does not parse is read as text.

THE EXTRACTION RULE (a candidate is a value that can grant authority, with or without a name beside it). A TOKEN is
a maximal run of credential characters [A-Za-z0-9_+/=-].

  TAKEN:
  (a) NAMED values: the value of `NAME=value`, `NAME: value` or a table row's Value cell (by its header) whose name
      says secret (SECRET_NAME: key, token, secret, password, passphrase, wif, seed, mnemonic, credential, bearer,
      auth, xprv, private): the whole value, quotes and backticks stripped, at least 8 characters, whatever its
      characters (a password may hold punctuation).
  (b) MARKED values: the rest of a line after `Enter:`, `Paste:`, `Value:`, `Secret:` or `Password:` (a leading `#`
      or `>` allowed), at least 8 characters: the value a `wrangler secret put` prompt was given.
  (c) Values AFTER A SECRET-SETTING COMMAND (`secret put`, `secret set`, `secrets set`, `secret bulk`, `set-secret`):
      on each of the next three non-blank lines, every token of at least 12 characters that is not a NAME (below):
      a value pasted bare into a code block under the command.
  (d) UNNAMED tokens anywhere: hex of at least 32 digits (keys, tokens, account ids, seeds); a UUID (database and
      namespace ids, which this repository's config check already forbids in public text: chosen to be taken); a
      token of at least 20 characters holding an upper-case letter, a lower-case letter and a digit (Base64 and
      Base58 secrets: API tokens, WIFs, extended keys); a token of at least 24 characters holding letters and
      digits with no `-` or `_` (lower-case API keys).

  NOT TAKEN (each is public by construction, or names a thing rather than granting authority):
  - a compressed public key (66 hex, `02`/`03`) or an uncompressed one (130 hex, `04`): public by definition;
  - a Base58 address (25 to 35 Base58 characters starting `1`, `3`, `m`, `n` or `2` whose Base58Check checksum
    verifies over 21 bytes of a P2PKH or P2SH version): public (an unverified token of that look is taken);
  - a NAME: a hostname or URL (they hold `.` or `://`, which no token holds; a URL's query value whose parameter
    name says secret is taken by (a)), a Worker, database, bucket or queue name (lower-case words joined by `-` or
    `_`, which (d) never takes);
  - an unnamed token on a line that LABELS it public: txid, transaction, hash, sha, digest, commit, block, merkle,
    root, outpoint, address, public key, pubkey, identity key; unless the same line also says secret
    (SECRET_NAME), when the label does not apply;
  - a value shorter than its rule's minimum, or one of fewer than 6 distinct characters (a placeholder: `xxxx...`,
    `000...`, `<your-token>`).
  Hex values are compared in lower and upper case. Every comparison is a fixed-string search of the scanned bytes,
  so a value is found inside a longer line, a quoted list, a URL or a JSON string.

  A COLLISION (a public value the vault records unlabelled, then found in the repository) is fixed in the VAULT
  (label its line as a txid, a hash, an address...) or by a derived exclusion above, never by a list of strings in
  this repository: such a list would itself be secret-shaped strings in a public place.

NO VAULT (the hosted CI; a host whose vault is a stub; `--no-vault`): the shapes are still judged, and ONE line says
the vault's values were not compared and why (on stderr, as a WARNING, in the pre-push door). It never fails a scan.

MODES:
  (none)                 the tree: every tracked file, its working-tree bytes and, where staged, its index bytes;
  --pre-push [REMOTE]    git's pre-push stdin: every blob any pushed commit adds or modifies (a merge against each
                         parent), the commits not already on the remote (a new branch judged against the remote's
                         refs; a delete carries nothing);
  --commits REVSPEC...   every blob the commits of `git rev-list REVSPEC...` add or modify (a hosted job scanning a
                         pull request's commits: `--commits BASE..HEAD`);
  --no-vault             with any of the above: shapes only, the vault not read;
  --self-test            synthetic vault folders and synthetic repositories only, never the real vault.

Exit status: 0 clean, 1 a finding (or an unreadable tracked path), 2 a usage error.
"""
import hashlib
import json
import os
import pathlib
import random
import re
import subprocess
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent

# -- 1. the shapes ------------------------------------------------------------------------------------------------
# Each is a provider's documented format (prefix and alphabet), so a match is a credential, not a hash. The PEM
# header is assembled so this file never carries it whole.
_B58 = rb"1-9A-HJ-NP-Za-km-z"
SHAPES = (
    ("a GitHub token", re.compile(rb"gh[pousr]_[A-Za-z0-9]{36,}")),
    ("a GitHub fine-grained token", re.compile(rb"github_pat_[A-Za-z0-9_]{40,}")),
    ("an AWS access key id", re.compile(rb"(?<![A-Z0-9])(?:AKIA|ASIA)[A-Z0-9]{16}(?![A-Z0-9])")),
    ("a PEM private key", re.compile(rb"-----BEGIN (?:RSA |EC |DSA |OPENSSH |ENCRYPTED |PGP )?PRIVATE" rb" KEY(?: BLOCK)?-----")),
    ("a secret-named assignment", re.compile(
        rb"\b[A-Z0-9_]*(?:API_TOKEN|API_KEY|AUTH_TOKEN|PASSWORD)[\"']?\s*[:=]\s*[\"']?[A-Za-z0-9_+/=-]{16,}", re.I)),
    ("a Slack token", re.compile(rb"xox[abeprs]-[0-9A-Za-z-]{10,}")),
    ("a Stripe live secret key", re.compile(rb"(?:sk|rk)_live_[0-9A-Za-z]{24,}")),
    ("a Google API key", re.compile(rb"AIza[0-9A-Za-z_-]{35}")),
    ("a GitLab token", re.compile(rb"glpat-[0-9A-Za-z_-]{20,}")),
    ("an npm token", re.compile(rb"npm_[A-Za-z0-9]{36}")),
    ("an Anthropic API key", re.compile(rb"sk-ant-(?:api|admin)[0-9]{2}-[A-Za-z0-9_-]{20,}")),
)
# Bitcoin private keys: a candidate by its alphabet and length, a finding only when its Base58Check checksum
# verifies and its version byte is a private key's (WIF 0x80 mainnet / 0xef testnet; BIP32 xprv / tprv).
WIF_CANDIDATE = re.compile(rb"(?<![" + _B58 + rb"])[5KLc9][" + _B58 + rb"]{50,51}(?![" + _B58 + rb"])")
XPRV_CANDIDATE = re.compile(rb"(?<![" + _B58 + rb"])[xt]prv[" + _B58 + rb"]{107,108}(?![" + _B58 + rb"])")
B58_ALPHABET = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58check(token):
    """The payload of a Base58Check string whose checksum verifies, else None."""
    number = 0
    for char in token:
        number = number * 58 + B58_ALPHABET.index(char)
    raw = number.to_bytes((number.bit_length() + 7) // 8, "big")
    raw = b"\0" * (len(token) - len(token.lstrip(b"1"))) + raw
    if len(raw) < 5:
        return None
    body, check = raw[:-4], raw[-4:]
    return body if hashlib.sha256(hashlib.sha256(body).digest()).digest()[:4] == check else None


def bitcoin_key_hits(data):
    hits = []
    for match in WIF_CANDIDATE.finditer(data):
        body = b58check(match.group())
        if body and body[0] in (0x80, 0xEF) and (len(body) == 33 or (len(body) == 34 and body[-1] == 1)):
            hits.append((match.start(), "a Bitcoin WIF private key"))
    for match in XPRV_CANDIDATE.finditer(data):
        body = b58check(match.group())
        if body and len(body) == 78 and body[:4] in (bytes.fromhex("0488ade4"), bytes.fromhex("04358394")):
            hits.append((match.start(), "a BIP32 extended private key"))
    return hits


# -- 2. the vault's values ----------------------------------------------------------------------------------------
TEXT_SUFFIXES = {"", ".md", ".txt", ".env", ".toml", ".yaml", ".yml", ".ini", ".conf", ".cfg", ".sh", ".json"}
VAULT_FILE_MAX_BYTES = 1 << 20
SECRET_NAME = re.compile(
    r"key|token|secret|passw|passphrase|wif|seed|mnemonic|credential|bearer|auth|xprv|private", re.I)
PUBLIC_LABEL = re.compile(
    r"txid|tx[ _-]?id|transaction|hash|\bsha|digest|commit|block|merkle|\broot\b|outpoint|address"
    r"|public[ _-]?key|pubkey|identity[ _-]?key", re.I)
MARKED = re.compile(r"^\s*(?:[#>]\s*)?(?:enter|paste|value|secret|password)\s*:\s*(\S.*?)\s*$", re.I)
SETTER = re.compile(r"secrets?[ :-](?:put|set|bulk)|set-secret", re.I)
TOKEN = re.compile(r"[A-Za-z0-9_+/-]+={0,2}")  # `=` only as Base64 padding: `NAME=value` is two tokens
NAME_LIKE = re.compile(r"[a-z0-9]+(?:[-_][a-z0-9]+)+")
UUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
B58_ADDRESS = re.compile(r"[13mn2][1-9A-HJ-NP-Za-km-z]{24,34}")
HEX = re.compile(r"[0-9a-fA-F]+")


def is_public(value):
    if HEX.fullmatch(value):
        if len(value) == 66 and value[:2] in ("02", "03"):
            return True
        if len(value) == 130 and value[:2] == "04":
            return True
    if B58_ADDRESS.fullmatch(value) and not HEX.fullmatch(value):
        # an address only when it IS one: 21 bytes under a P2PKH or P2SH version, its checksum verifying
        body = b58check(value.encode())
        return bool(body) and len(body) == 21 and body[0] in (0x00, 0x05, 0x6F, 0xC4)
    return False


def placeholder(value):
    return len(set(value)) < 6 or value.startswith("<") or value.lower().startswith(("your-", "your_"))


def unnamed_candidate(token):
    """Rule (d): an unnamed token that can grant authority."""
    if is_public(token) or placeholder(token):
        return False
    if UUID.fullmatch(token):
        return True
    if HEX.fullmatch(token):
        return len(token) >= 32
    upper, lower, digit = (any(c.isupper() for c in token), any(c.islower() for c in token),
                           any(c.isdigit() for c in token))
    if len(token) >= 20 and upper and lower and digit:
        return True
    return len(token) >= 24 and (upper or lower) and digit and "-" not in token and "_" not in token


def named_candidate(value, minimum=8):
    """Rules (a) and (b): an explicitly recorded value."""
    value = value.strip().strip("`\"' ")
    if len(value) < minimum or placeholder(value) or is_public(value):
        return None
    if "://" in value or " " in value or (TOKEN.fullmatch(value) and NAME_LIKE.fullmatch(value)):
        return None
    if re.fullmatch(r"[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+", value):  # a hostname
        return None
    if re.fullmatch(r"[0-9]+|true|false|yes|no|null|none", value, re.I):
        return None
    return value


def text_values(text):
    """Yield (line number, value) for every value the extraction rule takes from a text file."""
    columns = None
    after_setter = 0
    for number, line in enumerate(text.splitlines(), 1):
        stripped = line.strip()
        # (a) a table's Value cell, named by its header
        if stripped.startswith("|"):
            cells = [cell.strip().strip("`\"' ") for cell in stripped.strip("|").split("|")]
            headers = [cell.lower() for cell in cells]
            value_column = next((i for i, cell in enumerate(headers) if cell.startswith("value")), None)
            if value_column is not None:
                name_column = next((i for i, cell in enumerate(headers)
                                    if cell in ("secret", "name", "key", "variable", "field", "item")), None)
                columns = (name_column, value_column) if name_column is not None else None
            elif columns and max(columns) < len(cells) and SECRET_NAME.search(cells[columns[0]]):
                value = named_candidate(cells[columns[1]])
                if value:
                    yield number, value
        else:
            columns = None
        # (a) NAME=value, NAME: value (and a URL's secret-named query parameter)
        for match in re.finditer(r"([A-Za-z][A-Za-z0-9_.-]*)[\"'`]?\s*(?:=|:\s)\s*[\"'`]?([^\s\"'`|&]+)", line):
            if SECRET_NAME.search(match.group(1)):
                value = named_candidate(match.group(2))
                if value:
                    yield number, value
        # (b) a marked value
        marked = MARKED.match(line)
        if marked:
            value = named_candidate(marked.group(1))
            if value:
                yield number, value
        # (c) the lines after a secret-setting command
        if after_setter and stripped:
            after_setter -= 1
            for token in TOKEN.findall(line):
                if len(token) >= 12 and not NAME_LIKE.fullmatch(token) and not placeholder(token) \
                        and not is_public(token) and not token.isalpha():
                    yield number, token
        if SETTER.search(line):
            after_setter = 3
        # (d) unnamed tokens, unless the line labels them public and says nothing secret
        labelled = PUBLIC_LABEL.search(line) and not SECRET_NAME.search(PUBLIC_LABEL.sub("", line))
        for token in TOKEN.findall(line):
            if unnamed_candidate(token) and not labelled:
                yield number, token


def json_values(document):
    """Yield every string value whose key names a secret, at any depth (a JSON file is mostly public state)."""
    stack = [(None, document)]
    while stack:
        key, node = stack.pop()
        if isinstance(node, dict):
            stack.extend(node.items())
        elif isinstance(node, list):
            stack.extend((key, item) for item in node)
        elif isinstance(node, str) and key is not None and SECRET_NAME.search(str(key)):
            value = named_candidate(node)
            if value:
                yield value


class Vault:
    """The vault's values, read at run time. Holds each value with the vault file name and line it came from."""

    def __init__(self, folder=None, disabled=None):
        self.values = {}  # bytes -> (file name, line number or None)
        self.skipped = []
        self.reason = disabled
        if disabled:
            return
        folder = pathlib.Path(folder) if folder else pathlib.Path(
            os.environ.get("OVERLAY_VAULT_DIR") or pathlib.Path.home() / "vault")
        self.folder = folder
        if not folder.is_dir():
            self.reason = f"no vault folder at {self.shown(folder)}"
            return
        for path in sorted(folder.rglob("*")):
            relative = path.relative_to(folder)
            if any(part.startswith(".") for part in relative.parts) or not path.is_file() or path.is_symlink():
                continue
            if path.suffix.lower() not in TEXT_SUFFIXES or path.stat().st_size > VAULT_FILE_MAX_BYTES:
                self.skipped.append(str(relative))
                continue
            raw = path.read_bytes()
            if b"\0" in raw[:8192]:
                self.skipped.append(str(relative))
                continue
            text = raw.decode("utf-8", "replace")
            found = None
            if path.suffix.lower() == ".json":
                try:
                    found = [(None, value) for value in json_values(json.loads(text))]
                except ValueError:
                    found = None
            for number, value in found if found is not None else text_values(text):
                for variant in ({value.lower(), value.upper()} if HEX.fullmatch(value) else {value}):
                    self.values.setdefault(variant.encode(), (str(relative), number))
        if not self.values:
            self.reason = f"the vault folder {self.shown(folder)} holds no value this rule takes (a stub?)"

    @staticmethod
    def shown(folder):
        home = str(pathlib.Path.home())
        text = str(folder)
        return "~" + text[len(home):] if text.startswith(home + os.sep) or text == home else text

    def notice(self):
        """The ONE line when the vault's values were not compared, else None."""
        if self.reason is None:
            return None
        return (f"check-secrets: WARNING: the vault's values were NOT compared ({self.reason}); "
                "only credential shapes were judged")

    def skipped_notice(self):
        if not self.skipped:
            return None
        return (f"check-secrets: {len(self.skipped)} vault file(s) not read (binary, too large or not text): "
                + ", ".join(self.skipped))


class Matcher:
    """A fixed-string search for many values at once (no third-party Aho-Corasick in the standard library).

    Every value of credential characters lies inside one maximal run of them; a value of length m >= 2K - 1 that
    occurs in a run covers one window of K bytes starting at a multiple of K from the run's start, and that window
    is one of the value's own K-grams. So each run is sampled every K bytes against an index of every value's
    K-grams, and only a sampled hit is confirmed with a substring search. Any other value (short, or holding
    punctuation) is searched for directly.
    """

    K = 6
    RUN = re.compile(rb"[A-Za-z0-9_+/=-]{11,}")

    def __init__(self, values):
        self.grams = {}
        self.direct = []
        for value in values:
            if len(value) >= 2 * self.K - 1 and re.fullmatch(rb"[A-Za-z0-9_+/=-]+", value):
                for i in range(len(value) - self.K + 1):
                    self.grams.setdefault(value[i:i + self.K], set()).add(value)
            else:
                self.direct.append(value)

    def find(self, data):
        """Yield (offset, value) for every occurrence of every value."""
        seen = set()
        if self.grams:
            k = self.K
            for run in self.RUN.finditer(data):
                text, start = run.group(), run.start()
                for i in range(0, len(text) - k + 1, k):
                    for value in self.grams.get(text[i:i + k], ()):
                        offset = text.find(value)
                        while offset >= 0:
                            if (start + offset, value) not in seen:
                                seen.add((start + offset, value))
                                yield start + offset, value
                            offset = text.find(value, offset + 1)
        for value in self.direct:
            offset = data.find(value)
            while offset >= 0:
                yield offset, value
                offset = data.find(value, offset + 1)


def findings(data, vault, matcher):
    """(line, rule) for every hit in `data`; the rule names a shape or the vault file and line, never a value."""
    hits = set()
    for name, pattern in SHAPES:
        for match in pattern.finditer(data):
            hits.add((match.start(), name))
    hits.update(bitcoin_key_hits(data))
    for offset, value in matcher.find(data):
        source, number = vault.values[value]
        where = f"{source} line {number}" if number else source
        hits.add((offset, f"a value the vault holds ({where})"))
    return sorted({(data.count(b"\n", 0, offset) + 1, rule) for offset, rule in hits})


def diagnostic(where, path, line, rule):
    return f"check-secrets: credential at {where}:{path}:{line} ({rule}; value withheld)"


# -- the scans ----------------------------------------------------------------------------------------------------
ZERO = re.compile(r"0{40}(?:0{24})?")


def git(*args, cwd=None):
    """git in `cwd`, else in the repository the process runs in (a hook runs at its worktree's top level)."""
    return subprocess.run(["git", *args], cwd=cwd, check=True, stdout=subprocess.PIPE).stdout


def scan_tree(vault, out, cwd=None):
    cwd = pathlib.Path(cwd or git("rev-parse", "--show-toplevel").decode().strip())
    matcher = Matcher(vault.values)
    names = sorted(set(git("ls-files", "-z", "--cached", cwd=cwd).split(b"\0")) - {b""})
    staged = set(git("diff", "--cached", "--name-only", "-z", "--diff-filter=ACMR", cwd=cwd).split(b"\0"))
    failed = False
    for raw in names:
        name = raw.decode()
        path = cwd / name
        sources = []
        if path.is_symlink():
            sources.append(("worktree", os.readlink(path).encode()))
        elif path.is_file():
            sources.append(("worktree", path.read_bytes()))
        if raw in staged:
            sources.append(("index", git("show", ":" + name, cwd=cwd)))
        for where, data in sources:
            for line, rule in findings(data, vault, matcher):
                out(diagnostic(where, name, line, rule))
                failed = True
    out("check-secrets: the tree is clean (every tracked file, working tree and index)" if not failed else
        "check-secrets: REFUSED: a tracked file carries a credential; remove it, and rewrite any commit that "
        "carries it (never push it)")
    return failed


def scan_commits(commits, vault, out, cwd=None, push=False):
    """Every blob the commits add or modify (a merge against each parent), each blob read once."""
    matcher = Matcher(vault.values)
    seen = set()
    failed = False
    for commit in commits:
        raw = git("diff-tree", "-r", "-m", "--root", "--no-commit-id", "-z", "--raw", "--diff-filter=ACMRT",
                  commit, cwd=cwd).split(b"\0")
        for meta, name in zip(raw[0::2], raw[1::2]):
            if not meta.startswith(b":"):
                continue
            blob = meta.split()[3].decode()
            if blob in seen or ZERO.fullmatch(blob):
                continue
            seen.add(blob)
            data = git("cat-file", "blob", blob, cwd=cwd)
            for line, rule in findings(data, vault, matcher):
                out(diagnostic(commit[:12], name.decode(), line, rule))
                failed = True
    if failed:
        out("check-secrets: push REFUSED: a pushed commit carries a credential; rewrite the commit (drop the value "
            "from every commit that carries it), never push it" if push else
            "check-secrets: REFUSED: a commit carries a credential; rewrite the commit, never push it")
    else:
        out(f"check-secrets: {len(commits)} commit(s) clean ({len(seen)} blob(s) added or modified)")
    return failed


def remote_shas(remote, cwd=None):
    """The commits the remote already holds, as far as this clone knows: its remote-tracking refs."""
    if not remote:
        return []
    refs = git("for-each-ref", "--format=%(objectname)", f"refs/remotes/{remote}/", cwd=cwd).decode().split()
    return refs


def pre_push(lines, remote, vault, out, cwd=None):
    commits = []
    known = remote_shas(remote, cwd)
    for line in lines:
        fields = line.split()
        if len(fields) != 4:
            continue
        _, local, _, theirs = fields
        if ZERO.fullmatch(local):
            continue  # a delete carries no bytes
        exclude = [] if ZERO.fullmatch(theirs) else [theirs]
        if exclude and subprocess.run(["git", "cat-file", "-e", theirs + "^{commit}"], cwd=cwd,
                                      stderr=subprocess.DEVNULL).returncode != 0:
            exclude = []  # the remote's tip is not here: judge against its tracked refs alone
        exclude += known
        listed = git("rev-list", local, *(["--not", *exclude] if exclude else []), cwd=cwd).decode().split()
        commits += [commit for commit in listed if commit not in commits]
    return scan_commits(commits, vault, out, cwd=cwd, push=True)


# -- the self-test --------------------------------------------------------------------------------------------------
def synthetic(n, length=40, rng=random.Random(711)):
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789"
    return ["".join(rng.choice(alphabet) for _ in range(length - 3)) + "Q7z" for _ in range(n)]


def self_test():
    captured = []
    out = captured.append
    planted = []

    def plant(value):
        planted.append(value)
        return value

    with tempfile.TemporaryDirectory() as scratch:
        scratch = pathlib.Path(scratch)
        folder = scratch / "vault"
        folder.mkdir()
        values = synthetic(5)
        bare, entered, second, after, jsonval = (plant(v) for v in values)
        rng = random.Random(1)
        # recorded only as `# Enter:`, and with punctuation, so no unnamed-token rule could take it
        entered = planted[1] = "".join(rng.choice("abcdefghijk") for _ in range(6)) + "!" + entered[:10] + "#x"
        hexkey = plant("".join(rng.choice("0123456789abcdef") for _ in range(64)))
        password = plant("Pw!synthetic#v4lue")
        pub = "02" + "".join(rng.choice("0123456789abcdef") for _ in range(64))
        address = base58check(b"\x00" + bytes(rng.randrange(256) for _ in range(20)))
        txid = "".join(rng.choice("0123456789abcdef") for _ in range(64))
        lookalike = plant("m" + synthetic(1, 34)[0][:31] + "7Qz")  # an address's look, no address's checksum
        host = "synthetic-worker.example.dev"
        (folder / "first.md").write_text(
            "# synthetic project\n"
            f"- deployed at {host}, worker low-synthetic-beta, database synthetic-db\n"
            f"- operator {pub}\n"  # unlabelled: public by its own shape
            f"- funds go to {address}\n"
            f"- the funding txid {txid}\n"
            "```\nwrangler secret put SYNTHETIC_THING\n"
            f"# Enter: {entered}\n"
            "```\n"
            "notes:\n"
            f"{bare}\n"
            f"wrangler secret put OTHER\n\n{after}\n"
            f"HEX_PRIVATE_KEY={hexkey}\n"
            f"ADMIN_PASSWORD: {password}\n")
        (folder / "second.txt").write_text(f"misc\n{second}\n{lookalike}\n")
        (folder / "state.json").write_text(json.dumps({"txids": [txid], "wallet": {"rootKeyWif": jsonval}}))
        (folder / "blob.bin").write_bytes(b"\0\1\2")
        vault = Vault(folder)
        assert vault.notice() is None, vault.notice()

        def hit(text, v=vault):
            return findings(text.encode(), v, Matcher(v.values))

        # a bare vault value, quoted in a list, no name near it: refused (a names-only rule would miss it)
        assert hit(f'const LIST: [&str; 2] = ["{bare}", "other"];'), "a bare vault value in a quoted list is RED"
        # a value recorded only as `# Enter: <value>`: known
        assert hit(f"x = '{entered}'"), "a value recorded only as # Enter: is RED"
        # a value in a second vault file: known
        assert hit(f"see {second}"), "a value in a second vault file is RED"
        assert hit(f"// {after}"), "a value pasted bare after a secret put is RED"
        assert hit(f'{{"k":"{jsonval}"}}'), "a JSON vault file's secret-named value is RED"
        assert hit(f"url = https://x.example/?q={hexkey.upper()}"), "a hex vault value in upper case is RED"
        assert hit(f"p = {password};"), "a named password with punctuation is RED"
        assert hit(f"[{lookalike}]"), "a token with an address's look and no address's checksum is RED"
        # public things the vault records are not refused
        for public in (pub, address, host, "low-synthetic-beta", "synthetic-db", txid):
            assert not hit(f"fixture {public}"), "a public value the vault records is GREEN"
        # the vault's non-text files are named, never read
        assert vault.skipped == ["blob.bin"], vault.skipped
        # the shapes refuse with or without a vault; no vault prints the one line
        no_vault = Vault(scratch / "absent")
        assert no_vault.notice() and "NOT compared" in no_vault.notice(), "no vault folder prints the one line"
        stub = scratch / "stub"
        stub.mkdir()
        (stub / "notes.md").write_text("# nothing here yet\n")
        assert "holds no value" in Vault(stub).notice()
        github = "gh" + "p_" + "A1b2" * 9
        pem = "-----BEGIN " + "PRIVATE KEY-----"
        assert hit(github, no_vault) == [(1, "a GitHub token")]
        assert hit("x\n" + pem, no_vault) == [(2, "a PEM private key")]
        assert not hit("02" + "11" * 32, no_vault) and not hit("deadbeef" * 8, no_vault)
        # a real Base58Check WIF (of a synthetic key) is refused; the same shape with a bad checksum is not
        wif = to_wif(bytes(range(1, 33)))
        assert hit(f'"{wif}"', no_vault) == [(1, "a Bitcoin WIF private key")]
        assert not hit(wif[:-1] + ("2" if wif[-1] != "2" else "3"), no_vault), "a bad-checksum WIF shape is GREEN"

        # the tree scan, in a synthetic repository
        repo = scratch / "repo"
        repo.mkdir()
        env_git = ["-c", "user.email=t@example.invalid", "-c", "user.name=t", "-c", "commit.gpgsign=false",
                   "-c", "core.hooksPath=/dev/null"]
        git("init", "-q", "-b", "main", cwd=repo)
        (repo / "clean.rs").write_text(f"// {pub} {address} {txid} {host}\n")
        git("add", "-A", cwd=repo)
        git(*env_git, "commit", "-qm", "clean", cwd=repo)
        assert not scan_tree(vault, out, cwd=repo), "a clean tree is GREEN"
        assert not scan_tree(no_vault, out, cwd=repo), "a clean tree with no vault exits 0"
        (repo / "leak.rs").write_text(f'static S: &[&str] = &["{bare}"];\n')
        git("add", "-A", cwd=repo)
        assert scan_tree(vault, out, cwd=repo), "a staged vault value is RED"
        assert any(":leak.rs:1 (a value the vault holds (first.md line " in line for line in captured)
        # a commit range: the add and the later remove are both in the pushed history
        git(*env_git, "commit", "-qm", "leak", cwd=repo)
        git("rm", "-q", "leak.rs", cwd=repo)
        git(*env_git, "commit", "-qm", "unleak", cwd=repo)
        history = git("rev-list", "HEAD~2..HEAD", cwd=repo).decode().split()
        assert scan_commits(history, vault, out, cwd=repo), "a value added then removed in one push is RED"

        # 1,000 synthetic values: this file and every file of the lane pass
        thousand = scratch / "thousand"
        thousand.mkdir()
        (thousand / "many.md").write_text("".join(f"TOKEN_{i}={v}\n" for i, v in enumerate(synthetic(1000, 44))))
        big = Vault(thousand)
        assert len(big.values) == 1000, len(big.values)
        # the lane's new files pass whole; the files it edits are judged here on the vault's values (their
        # shapes are the tree scan's, which judges every tracked line)
        matcher = Matcher(big.values)
        for name in ("scripts/check-secrets.py", "scripts/hooks/pre-push", "scripts/tests/pre-push-secrets.sh",
                     "Makefile", ".github/workflows/ci.yml", "CLAUDE.md", "CHANGELOG.md"):
            path = ROOT / name
            if not path.is_file():
                continue
            hits = findings(path.read_bytes(), big, matcher)
            if name not in ("scripts/check-secrets.py", "scripts/hooks/pre-push", "scripts/tests/pre-push-secrets.sh"):
                hits = [hit for hit in hits if hit[1].startswith("a value the vault holds")]
            assert not hits, f"{name} passes its own scan under 1,000 vault values"

        # no diagnostic ever carries a planted value or 8 characters of one
        planted += [github, wif]
        text = "\n".join(captured)
        for value in planted:
            for i in range(len(value) - 7):
                assert value[i:i + 8] not in text, "a diagnostic carries a planted value"
    print("check-secrets self-test: a bare, an Enter-recorded, a second-file, an after-command, a JSON and a "
          "punctuated vault value RED; public keys, addresses, hosts, names, a labelled txid GREEN")
    print("check-secrets self-test: shapes RED without a vault (one line printed), a bad-checksum WIF GREEN; an "
          "add-then-remove history RED; 1,000 values: the lane's files clean; no diagnostic carries a value")


def to_wif(secret):
    return base58check(b"\x80" + secret + b"\x01")


def base58check(body):
    raw = body + hashlib.sha256(hashlib.sha256(body).digest()).digest()[:4]
    number = int.from_bytes(raw, "big")
    text = b""
    while number:
        number, rest = divmod(number, 58)
        text = B58_ALPHABET[rest:rest + 1] + text
    return "1" * (len(raw) - len(raw.lstrip(b"\0"))) + text.decode()


def main():
    args = sys.argv[1:]
    no_vault = "--no-vault" in args
    args = [arg for arg in args if arg != "--no-vault"]
    if args == ["--self-test"]:
        self_test()
        return 0
    vault = Vault(disabled="--no-vault: shapes only" if no_vault else None)
    pushing = bool(args) and args[0] == "--pre-push"
    notice = vault.notice()
    if notice:
        print(notice, file=sys.stderr if pushing else sys.stdout, flush=True)
    if vault.skipped_notice():
        print(vault.skipped_notice(), file=sys.stderr if pushing else sys.stdout, flush=True)
    out = (lambda line: print(line, file=sys.stderr, flush=True)) if pushing else print
    started = time.monotonic()
    if not args:
        failed = scan_tree(vault, out)
    elif pushing and len(args) <= 3:
        failed = pre_push(sys.stdin.read().splitlines(), args[1] if len(args) > 1 else None, vault, out)
    elif args[0] == "--commits" and len(args) > 1:
        failed = scan_commits(git("rev-list", *args[1:]).decode().split(), vault, out)
    else:
        print("usage: check-secrets.py [--no-vault] [--self-test | --pre-push [REMOTE [URL]] | --commits REVSPEC...]",
              file=sys.stderr)
        return 2
    if os.environ.get("CHECK_SECRETS_TIMING"):
        print(f"check-secrets: {time.monotonic() - started:.2f} s, {len(vault.values)} vault value(s)",
              file=sys.stderr)
    return int(failed)


if __name__ == "__main__":
    sys.exit(main())
