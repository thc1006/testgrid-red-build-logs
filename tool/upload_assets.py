#!/usr/bin/env python3
"""Upload the assets of node e2e builds' artifacts/ that the fetch packed (fetch_red_logs.py --assets DIR) to a
release of their month, and write what it uploaded and what the releases hold.

Of the tool's code, only this script gets the workflow's token. It reads nothing the fetch read from TestGrid or
GCS but DIR/manifest.json, which it checks entry by entry, and it decides the names, tags and endpoints itself. It
imports only the standard library, none of the fetch's code, and runs as python3 -I -S: no installed package, .pth
file or PYTHON* variable is read with the token present.

  GITHUB_TOKEN=... GITHUB_REPOSITORY=owner/repo GITHUB_REF_NAME=main python3 -I -S tool/upload_assets.py DIR

The release of a month is node-logs-<YYYY-MM> on the default branch, and a prerelease node-logs-<branch>-<YYYY-MM>
on another (see branch_prefix); a full one (MAX_ASSETS) continues in <tag>-2, <tag>-3, ... DIR/uploaded.json gets
each asset as it is uploaded (or found there already), so a step cut off keeps those; DIR/releases.json, at the end,
the start of the branch's tags and every asset of its node-logs releases. A wait GitHub asks for that would outlast
the step (SECONDS), a rate limit that outlasts a request's tries, or the step's time running out, stops the run; an
upload that could outlast the step (at RATE) is left for the next, unless it is the run's first.
"""
import argparse
import hashlib
import http.client
import json
import os
import random
import re
import stat
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

API = "https://api.github.com"
UPLOADS = "https://uploads.github.com"
MAX_ASSETS = 1000  # GitHub: "Up to 1000 release assets may be associated with a single release."
MAX_BYTES = (2 << 30) - 1  # GitHub: "Each file included in a release must be under 2 GiB."
MAX_PAGES = 20  # of a listing, of 100 each
RETRIES = 4  # tries of a request (as the fetch's)
BACKOFF = 2.0  # seconds before the first retry, doubled after each, with jitter
SECONDS = 18 * 60  # of the step's 20 minutes, the most it waits until: then it writes its files and stops
RATE = 2 << 20  # bytes a second an upload is sized at (see put), far below what a hosted runner sends
GAP = 1.0  # seconds between two requests that change something (see GitHub.once)
DIGEST_READS = 3  # reads of an asset GitHub's upload answer gave no digest of (see with_digest)
NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*_[0-9]{1,20}\.tar\.xz")  # <job>_<build>.tar.xz
MONTH_RE = re.compile(r"[0-9]{4}-(?:0[1-9]|1[0-2])")
SHA256_RE = re.compile(r"[0-9a-f]{64}")
REPO_RE = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
TAG_RE = re.compile(r"node-logs-(?:[a-z0-9-]+-)?[0-9]{4}-[0-9]{2}(?:-[0-9]+)?")  # every branch's (see branch_prefix)


class Failed(Exception):
    """An entry that cannot be uploaded now (the next run tries again)."""


class Later(Exception):
    """An entry left for the next run: printed, not an error."""


class OutOfTime(Exception):
    """A wait that would outlast the step, a rate limit that outlasted a request's tries, or the step's time up: the
    run stops, and the next one goes on."""


class Retry(Exception):
    """A request GitHub may take if tried again (a timeout, a 5xx, or a rate limit), and the seconds it asks to
    wait first (None: not said)."""

    def __init__(self, message, wait=None, limited=False):
        super().__init__(message)
        self.wait, self.limited = wait, limited

    def seconds(self, attempt):
        if self.wait is not None:
            return self.wait
        if self.limited:  # (GitHub: "at least one minute", then "an exponentially increasing amount of time")
            return 60.0 * 2 ** attempt
        return BACKOFF * 2 ** attempt * (0.5 + random.random())


class NoRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None  # (a redirect is an error: the token goes only to the hosts given)


def limit_wait(headers):
    """The seconds a rate limit error asks to wait, as GitHub's best practices read them (docs.github.com/en/rest/
    using-the-rest-api/best-practices-for-using-the-rest-api, "Handle rate limit errors appropriately"):
    retry-after; else, with x-ratelimit-remaining 0, until x-ratelimit-reset (UTC epoch seconds); else None."""
    after, reset = headers.get("Retry-After", "").strip(), headers.get("x-ratelimit-reset", "").strip()
    if after.isascii() and after.isdigit() and len(after) <= 12:
        return float(after)
    if headers.get("x-ratelimit-remaining", "").strip() == "0" and reset.isascii() and reset.isdigit() and len(reset) <= 12:
        return max(0.0, int(reset) - time.time()) + 1
    return None


class GitHub:
    def __init__(self, api, uploads, repo, token, deadline):
        self.api, self.uploads, self.repo, self.token, self.deadline = api, uploads, repo, token, deadline
        self.opener = urllib.request.build_opener(NoRedirects)
        self.changed = -float("inf")  # (when the last request that changes something ended)
        self.sent = False  # (whether an upload was started)

    def sleep(self, seconds):
        seconds = max(seconds, 0)  # (a gap already past)
        if time.monotonic() + seconds > self.deadline:
            self.in_time()  # (if it is up already)
            raise OutOfTime(f"a wait of {seconds:.0f} s would pass the step's time; the next run goes on")
        if seconds > 0:
            time.sleep(seconds)

    def in_time(self):
        if time.monotonic() > self.deadline:
            raise OutOfTime("the step's time is up; the next run goes on")

    def once(self, method, url, data=None, headers=None, timeout=60):
        """(status, parsed JSON body or None); a 404 is (404, None), another 4xx Failed, and what may pass if
        tried again Retry. One that changes something waits GAP after the last (GitHub: "wait at least one
        second between each request")."""
        req = urllib.request.Request(url, data=data, method=method, headers={
            "Authorization": f"Bearer {self.token}", "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "testgrid-red-build-logs", **(headers or {})})
        if method != "GET":
            self.sleep(self.changed + GAP - time.monotonic())
        try:
            with self.opener.open(req, timeout=timeout) as r:
                body = r.read()
                return r.status, json.loads(body) if body else None
        except urllib.error.HTTPError as e:
            try:
                text = e.read(4096).decode("utf-8", "replace")
            except (http.client.HTTPException, OSError, ValueError):
                text = ""
            if e.code == 404:
                return 404, None
            # (GitHub: a rate limit is a 403 or a 429, with x-ratelimit-remaining 0 or a message that says so)
            if e.code == 429 or e.code == 403 and (e.headers.get("Retry-After") or "rate limit" in text.lower()
                                                   or e.headers.get("x-ratelimit-remaining", "").strip() == "0"):
                raise Retry(f"{method} {url}: HTTP {e.code} (a rate limit)", limit_wait(e.headers), True) from None
            if e.code >= 500:
                raise Retry(f"{method} {url}: HTTP {e.code}", limit_wait(e.headers)) from None
            raise Failed(f"{method} {url}: HTTP {e.code}: {text[:300]}") from None
        except (urllib.error.URLError, http.client.HTTPException, OSError, ValueError) as e:
            raise Retry(f"{method} {url}: {type(e).__name__}: {e}") from None
        finally:
            if method != "GET":
                self.changed = time.monotonic()

    def call(self, method, path, body=None, ok=(200,)):
        """A request of the API at path, tried up to RETRIES times: its parsed JSON (None for a 404)."""
        data = json.dumps(body).encode() if body is not None else None
        for attempt in range(RETRIES):
            try:
                status, got = self.once(method, self.api + path, data,
                                        {"Content-Type": "application/json"} if data else None)
            except Retry as e:
                if attempt == RETRIES - 1:
                    raise (OutOfTime if e.limited else Failed)(str(e)) from None
                self.sleep(e.seconds(attempt))
                continue
            if status == 404:
                return None
            if status not in ok:
                raise Failed(f"{method} {path}: HTTP {status}")
            return got
        raise AssertionError("not reached")

    def listing(self, path):
        """Every item of a paged listing."""
        items = []
        for page in range(1, MAX_PAGES + 1):
            got = self.call("GET", f"{path}?per_page=100&page={page}")
            if not isinstance(got, list):
                raise Failed(f"GET {path}: not a list")
            items += got
            if len(got) < 100:
                return items
        raise Failed(f"GET {path}: more than {MAX_PAGES} pages")


def shaped(got, what, **types):
    """got, if it is an object whose keys have those types (an int no bool), else Failed: GitHub answered what
    this code does not know."""
    if not isinstance(got, dict) or any(not isinstance(got.get(k), t) or t is int and isinstance(got.get(k), bool)
                                        for k, t in types.items()):
        raise Failed(f"{what}: GitHub answered {str(got)[:100]!r}")
    return got


def a_release(got):
    return shaped(got, "a release", id=int, tag_name=str)


def an_asset(got):
    return dict(shaped(got, "an asset", id=int, name=str, size=int, digest=(str, type(None))), digest=got.get("digest"))


def asset_info(a):
    return dict({k: a.get(k) for k in ("id", "name", "size", "digest", "state")}, url=a.get("browser_download_url"))


class Releases:
    """The repository's node-logs releases, every branch's ({tag: release}), listed once, and the assets of each
    ({name: asset}) as they are read."""

    def __init__(self, gh, prefix, prerelease, target):
        self.gh, self.prefix, self.prerelease, self.target, self.assets = gh, prefix, prerelease, target, {}
        self.all = {r["tag_name"]: r for r in map(a_release, gh.listing(f"/repos/{gh.repo}/releases"))
                    if TAG_RE.fullmatch(r["tag_name"])}

    def assets_of(self, tag):
        if tag not in self.assets:
            self.assets[tag] = release_assets(self.gh, self.all[tag])
        return self.assets[tag]

    def numbered(self, month):
        """[(n, tag)] of the branch's releases of a month, as listed: <tag> is 1, <tag>-2 is 2, ..."""
        tag_re = re.compile(re.escape(self.prefix + month) + r"(?:-([1-9][0-9]*))?")
        return sorted((int(m.group(1) or 1), t) for t in self.all if (m := tag_re.fullmatch(t)))

    def of(self, month):
        """[(release, {name: asset})] of the branch's releases of a month; Failed if one is a draft (GitHub makes
        one of a release whose tag was deleted), whose assets are not public."""
        tags = [t for _, t in self.numbered(month)]
        if drafts := [t for t in tags if self.all[t].get("draft")]:
            raise Failed(f"{drafts[0]} is a draft (was its tag deleted?): nothing is uploaded to it or after it")
        return [(self.all[t], self.assets_of(t)) for t in tags]

    def with_room(self, month):
        """The month's release that has room for one more asset, made after the last if none has."""
        for release, assets in self.of(month):
            if len(assets) < MAX_ASSETS:
                return release, assets
        n = max((n for n, _ in self.numbered(month)), default=0) + 1
        tag = f"{self.prefix}{month}" + (f"-{n}" if n > 1 else "")
        try:
            release = self.gh.call("POST", f"/repos/{self.gh.repo}/releases", ok=(201,), body={
                "tag_name": tag, "target_commitish": self.target, "name": tag, "draft": False,
                "prerelease": self.prerelease, "make_latest": "false",
                "body": "The artifacts/ of red node e2e builds the archive took this month, one .tar.xz a build "
                        "(see the README's layout of the archive)."})
        except Failed:  # (a try GitHub took, though its answer was lost, makes the retry a 422)
            release = self.gh.call("GET", f"/repos/{self.gh.repo}/releases/tags/{urllib.parse.quote(tag)}")
            if release is None:
                raise
        self.all[tag] = a_release(release)
        return self.all[tag], self.assets_of(tag)


def checked(entry, folder, seen):
    """The entry of the manifest as it is to be uploaded, checked, with its file open ("file"), which the upload
    reads; Failed if it is not one to upload."""
    if not isinstance(entry, dict):
        raise Failed(f"not an entry: {str(entry)[:100]!r}")
    name = entry.get("name")
    if not isinstance(name, str) or not NAME_RE.fullmatch(name) or len(name) > 255:
        raise Failed(f"not an asset's name: {str(name)[:100]!r}")
    if name in seen:
        raise Failed(f"{name}: listed twice")
    size, sha, month = entry.get("bytes"), entry.get("sha256"), entry.get("month")
    if not isinstance(month, str) or not MONTH_RE.fullmatch(month):
        raise Failed(f"{name}: not a month: {str(month)[:20]!r}")
    if type(size) is not int or not 0 < size <= MAX_BYTES:
        raise Failed(f"{name}: not a size GitHub takes: {str(size)[:20]!r}")
    if not isinstance(sha, str) or not SHA256_RE.fullmatch(sha):
        raise Failed(f"{name}: not a sha256: {str(sha)[:80]!r}")
    try:
        fd = os.open(os.path.join(folder, name), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)  # (nor waits on a FIFO)
    except OSError as e:
        raise Failed(f"{name}: cannot be read: {e}") from None
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode) or info.st_size != size:
        os.close(fd)
        raise Failed(f"{name}: not a file of {size} bytes")
    f = open(fd, "rb")
    if sha256(f) != sha:
        f.close()
        raise Failed(f"{name}: its sha256 is not the manifest's")
    seen.add(name)
    return {"name": name, "month": month, "bytes": size, "sha256": sha, "file": f}


def sha256(f):
    f.seek(0)
    digest = hashlib.sha256()
    for chunk in iter(lambda: f.read(1 << 20), b""):
        digest.update(chunk)
    return digest.hexdigest()


def put(gh, releases, entry):
    """Upload a checked entry to its month's release: what uploaded.json records of it. One whose upload could
    outlast the step at RATE is Later, unless it is the run's first."""
    name, want = entry["name"], "sha256:" + entry["sha256"]
    held = next(((r, a) for r, a in releases.of(entry["month"]) if name in a), None)
    if held is not None:
        release, assets = held
        found = assets[name]
        if found.get("state") == "uploaded" and found["digest"] == want:
            return recorded(release, found)  # (uploaded by a run whose commit was lost)
    if gh.sent and time.monotonic() + 60 + entry["bytes"] / RATE > gh.deadline:
        raise Later(f"{name}: its upload could outlast the step's time; the next run uploads it")
    gh.sent = True
    if held is not None:
        delete(gh, found)  # (one a run uploaded whose commit was lost, or one a 502 left)
        del assets[name]
    else:
        release, assets = releases.with_room(entry["month"])
    asset, sent = upload(gh, release, entry)
    if asset["digest"] is None:
        asset = with_digest(gh, asset)
    if asset["name"] != name or asset["size"] != entry["bytes"] or asset["digest"] not in (want, None) \
            or sent != entry["sha256"]:
        delete(gh, asset)
        raise Failed(f"{name}: GitHub took {asset['size']} bytes of digest {asset['digest']} as {asset['name'][:100]!r}"
                     f" (sent sha256:{sent}), not {entry['bytes']} of {want}; deleted it")
    assets[name] = asset
    return recorded(release, asset, entry["sha256"])


def with_digest(gh, asset):
    """The asset, read again up to DIGEST_READS times (BACKOFF, doubled, apart) until GitHub says its digest."""
    for attempt in range(DIGEST_READS):
        gh.sleep(BACKOFF * 2 ** attempt)
        got = gh.call("GET", f"/repos/{gh.repo}/releases/assets/{asset['id']}")
        if got is None:
            raise Failed(f"{asset['name'][:100]}: GitHub answered its upload, then had no asset {asset['id']}")
        asset = an_asset(got)
        if asset["digest"] is not None:
            break
    return asset


def recorded(release, asset, sha=None):
    """What uploaded.json records of an asset; without GitHub's digest, the sha256 of the file it was sent from."""
    return {"release": release["tag_name"], "id": asset["id"], "size": asset["size"],
            "digest": asset["digest"] or f"sha256:{sha}", "url": asset.get("browser_download_url"),
            "uploaded_at": asset.get("updated_at"),
            **({} if asset["digest"] else {"note": "GitHub said no digest: this is the sha256 of the file sent"})}


def delete(gh, asset):
    gh.call("DELETE", f"/repos/{gh.repo}/releases/assets/{asset['id']}", ok=(204,))


class Sent:
    """An entry's open file as the upload reads it, from its start, hashed as it goes."""

    def __init__(self, f):
        f.seek(0)
        self.f, self.digest = f, hashlib.sha256()

    def read(self, n=-1):
        data = self.f.read(n)
        self.digest.update(data)
        return data


def upload(gh, release, entry):
    """Upload an entry's file to release, tried up to RETRIES times: (the asset, the sha256 of what was sent).
    A retry that could outlast the step at RATE is Failed. Before each retry, an asset of its name the failed try
    left (GitHub: a 502 "may leave an empty asset with a state of starter") is deleted."""
    url = (f"{gh.uploads}/repos/{gh.repo}/releases/{release['id']}/assets?"
           + urllib.parse.urlencode({"name": entry["name"]}))
    need = 60 + entry["bytes"] / RATE
    for attempt in range(RETRIES):
        try:
            sent = Sent(entry["file"])
            status, asset = gh.once("POST", url, sent, {"Content-Type": "application/x-xz",
                                                        "Content-Length": str(entry["bytes"])})
            if status == 201:
                return an_asset(asset), sent.digest.hexdigest()
            raise Failed(f"{entry['name']}: upload answered HTTP {status}")
        except Retry as e:
            if attempt == RETRIES - 1:
                raise (OutOfTime if e.limited else Failed)(f"{entry['name']}: {e}") from None
            gh.sleep(e.seconds(attempt))
            if time.monotonic() + need > gh.deadline:
                raise Failed(f"{entry['name']}: {e}; no time left to try again") from None
            left = release_assets(gh, release).get(entry["name"])
            if left is not None:
                delete(gh, left)
    raise AssertionError("not reached")


def release_assets(gh, release):
    return {a["name"]: a for a in map(an_asset, gh.listing(f"/repos/{gh.repo}/releases/{release['id']}/assets"))}


def write_json(path, data):
    """Write data to path whole: a step cut off leaves the old file or the new one."""
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=".", suffix=".part")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(json.dumps(data, indent=2, sort_keys=True) + "\n")
    os.replace(tmp, path)


def branch_prefix(branch, default):
    """The start of the tags of a branch's releases: node-logs- on the default branch; on another, node-logs-, the
    branch's name in lowercase letters, digits and hyphens, and the start of its sha256, so no two branches share
    one."""
    if branch == default:
        return "node-logs-"
    name = re.sub(r"[^a-z0-9]+", "-", branch.lower()).strip("-")
    digest = hashlib.sha256(branch.encode("utf-8", "surrogateescape")).hexdigest()
    return f"node-logs-{name}{'-' if name else ''}{digest[:8]}-"


def main(argv=None, env=None):
    env = os.environ if env is None else env
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("folder", help="the --assets folder of the fetch: manifest.json and the assets it lists")
    p.add_argument("--api", default=API)
    p.add_argument("--uploads", default=UPLOADS)
    args = p.parse_args(argv)
    token, repo, branch = env.get("GITHUB_TOKEN", ""), env.get("GITHUB_REPOSITORY", ""), env.get("GITHUB_REF_NAME", "")
    if not token or not REPO_RE.fullmatch(repo) or not branch:
        print("ERROR GITHUB_TOKEN, GITHUB_REPOSITORY and GITHUB_REF_NAME must be set", file=sys.stderr)
        return 1
    if not os.path.isdir(args.folder):
        print(f"no {args.folder}: the fetch packed nothing")
        return 0
    gh = GitHub(args.api.rstrip("/"), args.uploads.rstrip("/"), repo, token, time.monotonic() + SECONDS)
    done, errors, out = {"run": None, "uploaded": {}}, [], os.path.join(args.folder, "uploaded.json")
    try:  # (one there already is not this run's, and the record step reads it)
        os.remove(os.path.join(args.folder, "releases.json"))
    except FileNotFoundError:
        pass
    write_json(out, done)
    try:
        with open(os.path.join(args.folder, "manifest.json"), encoding="utf-8") as f:
            manifest = json.load(f)
    except FileNotFoundError:
        manifest = {"assets": []}
        print("no manifest.json: the fetch packed nothing")
    except (OSError, ValueError) as e:
        print(f"ERROR manifest.json cannot be read: {e}", file=sys.stderr)
        return 1
    entries = manifest.get("assets") if isinstance(manifest, dict) else None
    if not isinstance(entries, list):
        print("ERROR manifest.json lists no assets", file=sys.stderr)
        return 1
    done["run"] = manifest.get("run")
    try:
        write_json(out, done)
        default = shaped(gh.call("GET", f"/repos/{repo}"), f"GET /repos/{repo}", default_branch=str)["default_branch"]
        prefix = branch_prefix(branch, default)
        # (its tags go on the default branch: a branch's run may push nothing)
        releases, seen = Releases(gh, prefix, branch != default, default), set()
        for entry in entries:
            gh.in_time()
            try:
                entry = checked(entry, args.folder, seen)
                try:
                    done["uploaded"][entry["name"]] = put(gh, releases, entry)
                finally:
                    entry["file"].close()
                print(f"{entry['name']}: in {done['uploaded'][entry['name']]['release']}")
            except Later as e:
                print(e)
                continue
            except Failed as e:
                errors.append(str(e))
                continue
            write_json(out, done)
        tag_re = re.compile(re.escape(prefix) + r"[0-9]{4}-[0-9]{2}(?:-[0-9]+)?")
        now = Releases(gh, prefix, branch != default, default)  # (listed again: what they hold at the end)
        write_json(os.path.join(args.folder, "releases.json"), {"prefix": prefix, "releases": {
            tag: {"id": r["id"], "draft": r.get("draft"), "prerelease": r.get("prerelease"),
                  "assets": sorted((asset_info(a) for a in now.assets_of(tag).values()), key=lambda a: a["name"])}
            for tag, r in sorted(now.all.items()) if tag_re.fullmatch(tag)}})
    except (Failed, OutOfTime) as e:
        errors.append(str(e))
    for e in errors:
        print("ERROR", e, file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
